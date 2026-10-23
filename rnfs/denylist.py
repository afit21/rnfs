"""The deny list (specification section 8, "Deny lists"): CIDs and whole-file SHA-256 hashes this node refuses.

A denied CID isn't fetched, read, stored, served to /blocks (answered as not held) or provided, whether it is a
root or anywhere inside the DAG being fetched. A file-hash entry (`sha256:<hex>`) matches a file whose content
hashes to it; that is only known once the content is read, so it is checked where content is assembled (get,
cat, the gateway, add, archive import), after the blocks have been fetched. A file of one block has the hash as its
raw CID's digest, so that CID is denied too (`cids` holds it): such a file isn't fetched, stored, served or provided
either.

Entries come from the local list (`rnfs deny add` / `rm`; the deny_entries table) and from the deny lists named
in the deny_lists setting, which rnfs/features/deny.py fetches and follows. The deny_lists table records the
version (CID) each source last accepted; its blocks stay pinned with source "deny" while it is in use, so at
startup reload() reads every list back from storage before anything is fetched or provided.

Checks are set lookups: `cids` (binary CIDs) and `hashes` (32-byte digests) are frozensets swapped whole and
read without a lock, so /blocks serving and each block a fetch stores pay one lookup. A list is untrusted input:
parse_list refuses one over MAX_LIST_BYTES or MAX_LIST_ENTRIES, not UTF-8, or with a line that isn't an entry,
and its source keeps the version it had. Listeners (the node) are told after every change, outside the lock.

Hashing reads every occurrence of repeated content, so a few small blocks can declare a file of 2^56 bytes. While
there are file hashes, a file declaring more than DENY_HASH_RATIO times the bytes of its distinct chunks, plus
DENY_HASH_SLACK, is refused (Denied) unhashed: it can't be checked (the node's _check_file).

So a file isn't read and hashed again before each send, the SHA-256 of the files checked is remembered by CID
(at most MAX_KNOWN_HASHES, least recently used forgotten first). A CID's content never changes, so a hash
remembered never goes stale, and they are kept across restarts in the deny_hashes table: memory is what checks
read, and save() (the node's periodic saves and its stop) writes the rows changed since the last one, NORMAL.
"""
import itertools
import re
import threading
import time
from collections import OrderedDict
from collections.abc import Callable

from rnfs import statedb
from rnfs.constants import KIB, MIB
from rnfs.content.cid import Cid
from rnfs.errors import NotFound
from rnfs.log import LOG_WARNING, log, plural

HASH_PREFIX = "sha256:"
SOURCE = "deny"  # the pin source of the deny lists' own blocks
MAX_LIST_BYTES = 4096 * KIB  # a list file (about 60 000 CIDs)
MAX_LIST_ENTRIES = 100_000  # entries in one list
MAX_LOCAL_ENTRIES = 100_000  # entries in the local list
MAX_KNOWN_HASHES = 10_000  # files whose content hash is remembered (a CID's content never changes)
# A file is hashed only if it declares at most DENY_HASH_RATIO × the bytes of its distinct chunks + DENY_HASH_SLACK:
# hashing then costs at most 16 times what its blocks cost to store, and any file of up to 64 MiB is hashed
DENY_HASH_RATIO = 16
DENY_HASH_SLACK = 64 * MIB
_HEX_DIGEST = re.compile(r"[0-9a-f]{64}")


def parse_entry(text) -> tuple[str, bytes | None, bytes | None]:
    """(canonical text, binary CID or None, digest or None) of one entry: a CID, or "sha256:" and 64 hex digits
    (a file's content hash). Raises ValueError."""
    if not isinstance(text, str):
        raise ValueError("a deny-list entry must be a CID or sha256:<64 hex digits>")
    text = text.strip()
    if text[:len(HASH_PREFIX)].lower() == HASH_PREFIX:
        digest = text[len(HASH_PREFIX):].lower()
        if not _HEX_DIGEST.fullmatch(digest):
            raise ValueError(f"invalid file hash {text[:80]!r}: must be sha256:<64 hex digits>")
        return HASH_PREFIX + digest, None, bytes.fromhex(digest)
    try:
        cid = Cid.parse(text)
    except ValueError:
        raise ValueError(f"invalid deny-list entry {text[:80]!r}: not a CID or sha256:<64 hex digits>") from None
    return str(cid), cid.binary, None


def parse_list(data: bytes) -> tuple[frozenset[bytes], frozenset[bytes]]:
    """(binary CIDs, digests) of a deny list: UTF-8 text, one entry per line; blank lines and everything after a
    "#" are ignored. Raises ValueError (saying which line) for a list over MAX_LIST_BYTES or MAX_LIST_ENTRIES
    entries, that isn't UTF-8, or with a line that isn't an entry."""
    if len(data) > MAX_LIST_BYTES:
        raise ValueError(f"it is over {MAX_LIST_BYTES // KIB} KiB")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        raise ValueError("it isn't UTF-8 text") from None
    cids: set[bytes] = set()
    digests: set[bytes] = set()
    for number, line in enumerate(re.split(r"\r\n|\r|\n", text), 1):  # not splitlines: \x0b, \x85, U+2028 ... aren't line ends
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        try:
            _, binary, digest = parse_entry(line)
        except ValueError as e:
            raise ValueError(f"line {number}: {e}") from None
        (cids if binary is not None else digests).add(binary if binary is not None else digest)
        if len(cids) + len(digests) > MAX_LIST_ENTRIES:
            raise ValueError(f"it has over {MAX_LIST_ENTRIES} entries")
    return frozenset(cids), frozenset(digests)


class NoDenyList:
    """Denies nothing: what storage, the fetcher and the publisher check until the node gives them its list."""

    cids: frozenset[bytes] = frozenset()
    hashes: frozenset[bytes] = frozenset()

    def denies(self, cid: Cid) -> bool:
        """False."""
        return False

    def denies_hash(self, digest: bytes) -> bool:
        """False."""
        return False


NONE = NoDenyList()


class DenyList(NoDenyList):
    """The node's deny list: the local entries and every source's accepted list, in memory, saved in the state
    database (deny_entries, deny_lists), and the files' hashes remembered (deny_hashes). Thread-safe; checks take
    no lock."""

    def __init__(self, state_dir: str, clock: Callable[[], float] = time.time):
        """Load the local entries, the sources' accepted versions (their entries come with reload()) and the
        files' hashes remembered."""
        self.clock = clock
        self.cids: frozenset[bytes] = frozenset()
        self.hashes: frozenset[bytes] = frozenset()
        self._lock = threading.Lock()
        self._local: dict[str, tuple[bytes | None, bytes | None]] = {}  # canonical text -> (binary CID, digest)
        self._accepted: dict[str, Cid] = {}  # source -> the version it last accepted (the deny_lists table)
        self._lists: dict[str, tuple[frozenset[bytes], frozenset[bytes]]] = {}  # source -> its entries, read
        self._known: OrderedDict[bytes, bytes] = OrderedDict()  # binary file CID -> its content's SHA-256, LRU
        # Hashes remembered, used or forgotten since the last save(); every one still known is among the most
        # recently used
        self._known_dirty: set[bytes] = set()
        self._known_used = 0  # the deny_hashes table's next used value: larger than every one saved
        self._save_lock = threading.Lock()  # keeps saves in order without holding _lock while writing
        self._listeners: list[Callable[[], None]] = []
        self._db = statedb.connect(state_dir)
        try:
            self._load()
        except BaseException:
            self._db.close()
            raise

    def _load(self):
        """Read the tables; a row that doesn't parse is logged and deleted, as are hashes past MAX_KNOWN_HASHES
        (the least recently used)."""
        bad_entries, bad_lists, bad_hashes = [], [], []
        for (text,) in self._db.query("SELECT entry FROM deny_entries"):
            try:
                canonical, binary, digest = parse_entry(text)
                if canonical != text:
                    raise ValueError("not canonical")
            except ValueError:
                bad_entries.append((text,))
                continue
            self._local[canonical] = (binary, digest)
        for dumped, binary in self._db.query("SELECT source, cid FROM deny_lists"):
            try:
                source = statedb.load(dumped)
                if not isinstance(source, str):
                    raise ValueError("not a string")
                self._accepted[source] = Cid.from_bytes(binary)
            except (ValueError, TypeError):
                bad_lists.append((dumped,))
        for binary, digest, used in self._db.query("SELECT cid, digest, used FROM deny_hashes ORDER BY used"):
            try:
                Cid.from_bytes(binary)
                if not isinstance(digest, bytes) or len(digest) != 32 or not isinstance(used, int):
                    raise ValueError("not a SHA-256 digest and a position")
            except ValueError:
                bad_hashes.append((binary,))
                continue
            self._known[binary] = digest
            self._known_used = max(self._known_used, used + 1)
        excess = list(itertools.islice(self._known, max(0, len(self._known) - MAX_KNOWN_HASHES)))
        for binary in excess:
            del self._known[binary]
        bad = len(bad_entries) + len(bad_lists) + len(bad_hashes)
        if bad:
            log(f"deny list: dropped {plural(bad, 'invalid row')} from the state database", LOG_WARNING)
        if bad or excess:
            with self._db.transaction() as tx:
                tx.executemany("DELETE FROM deny_entries WHERE entry = ?", bad_entries)
                tx.executemany("DELETE FROM deny_lists WHERE source = ?", bad_lists)
                tx.executemany("DELETE FROM deny_hashes WHERE cid = ?", bad_hashes + [(binary,) for binary in excess])
        self._rebuild()

    def reload(self, read: Callable[[Cid], bytes | None]):
        """Read every source's accepted version back with read(cid) (its bytes from storage, or None) and use
        its entries; one that can't be read or parsed is left to the feature to fetch again (logged)."""
        with self._lock:
            accepted = dict(self._accepted)
        lists = {}
        for source, cid in accepted.items():
            try:
                data = read(cid)
                if data is None:
                    raise NotFound("not stored")
                lists[source] = parse_list(data)
            except (ValueError, NotFound) as e:
                log(f"deny list {source}: can't read its version {cid} ({e}); fetching it again", LOG_WARNING)
        with self._lock:
            for source, entries in lists.items():
                if self._accepted.get(source) == accepted[source]:
                    self._lists[source] = entries
            self._rebuild()
        self._notify()

    # Checks (no lock: the sets are swapped whole)

    def denies(self, cid: Cid) -> bool:
        """True if cid is denied."""
        return cid.binary in self.cids

    def denies_hash(self, digest: bytes) -> bool:
        """True if a file whose content has this SHA-256 is denied."""
        return digest in self.hashes

    def known_hash(self, cid: Cid) -> bytes | None:
        """The SHA-256 of file cid's content, if worked out before (here or before a restart: it never changes)."""
        with self._lock:
            digest = self._known.get(cid.binary)
            if digest is not None:
                self._known.move_to_end(cid.binary)
                self._known_dirty.add(cid.binary)
            return digest

    def remember_hash(self, cid: Cid, digest: bytes):
        """Remember the SHA-256 of file cid's content (at most MAX_KNOWN_HASHES files, least recently used
        forgotten first); saved with the next save()."""
        with self._lock:
            self._known[cid.binary] = digest
            self._known.move_to_end(cid.binary)
            self._known_dirty.add(cid.binary)
            while len(self._known) > MAX_KNOWN_HASHES:
                self._known_dirty.add(self._known.popitem(last=False)[0])

    # The local list

    def entries(self) -> list[str]:
        """The local list's entries, sorted."""
        with self._lock:
            return sorted(self._local)

    def add(self, texts) -> list[str]:
        """Add entries to the local list; those new, canonical. Raises ValueError (adding none) if one isn't an
        entry or the list would hold over MAX_LOCAL_ENTRIES."""
        parsed = {}
        for text in texts:
            canonical, binary, digest = parse_entry(text)
            parsed[canonical] = (binary, digest)
        with self._lock:
            new = {text: value for text, value in parsed.items() if text not in self._local}
            if len(self._local) + len(new) > MAX_LOCAL_ENTRIES:
                raise ValueError(f"the local deny list holds at most {MAX_LOCAL_ENTRIES} entries")
            if new:
                now = self.clock()
                with self._db.transaction(durable=True) as tx:
                    tx.executemany("INSERT INTO deny_entries (entry, added) VALUES (?, ?)",
                                   [(text, now) for text in new])
                self._local.update(new)
                self._rebuild()
        if new:
            self._notify()
        return sorted(new)

    def remove(self, texts) -> list[str]:
        """Remove entries from the local list; those removed, canonical. Raises ValueError if one isn't an entry,
        NotFound (removing none) if one isn't on the local list."""
        canonical = {parse_entry(text)[0] for text in texts}
        with self._lock:
            missing = sorted(text for text in canonical if text not in self._local)
            if missing:
                raise NotFound(f"not on the local deny list: {', '.join(missing)}")
            if canonical:
                with self._db.transaction(durable=True) as tx:
                    tx.executemany("DELETE FROM deny_entries WHERE entry = ?", [(text,) for text in canonical])
                for text in canonical:
                    del self._local[text]
                self._rebuild()
        if canonical:
            self._notify()
        return sorted(canonical)

    # Sources (rnfs/features/deny.py)

    def accepted(self) -> dict[str, Cid]:
        """source -> the version (CID) it last accepted."""
        with self._lock:
            return dict(self._accepted)

    def in_use(self, source: str) -> bool:
        """True if source's accepted version is read and its entries apply."""
        with self._lock:
            return source in self._lists

    def entry_count(self, source: str) -> int:
        """How many entries source's accepted version has (0 if it isn't read)."""
        with self._lock:
            cids, digests = self._lists.get(source, (frozenset(), frozenset()))
            return len(cids) + len(digests)

    def accept(self, source: str, cid: Cid, entries: tuple[frozenset[bytes], frozenset[bytes]]):
        """Make version cid, with entries (parse_list's), source's list; saved before it applies."""
        with self._lock:
            with self._db.transaction(durable=True) as tx:
                tx.execute("INSERT OR REPLACE INTO deny_lists (source, cid, accepted) VALUES (?, ?, ?)",
                           (statedb.dump(source), cid.binary, self.clock()))
            self._accepted[source] = cid
            self._lists[source] = entries
            self._rebuild()
        self._notify()

    def forget(self, source: str):
        """Stop using source (no longer in deny_lists): its entries no longer apply."""
        with self._lock:
            if source not in self._accepted and source not in self._lists:
                return
            with self._db.transaction(durable=True) as tx:
                tx.execute("DELETE FROM deny_lists WHERE source = ?", (statedb.dump(source),))
            self._accepted.pop(source, None)
            self._lists.pop(source, None)
            self._rebuild()
        self._notify()

    # Changes

    def add_listener(self, listener: Callable[[], None]):
        """Call listener() after every change (from the changing thread, outside the lock)."""
        self._listeners.append(listener)

    def _rebuild(self):
        """Swap in the sets of every entry in use (lock held)."""
        cids = {binary for binary, _ in self._local.values() if binary is not None}
        digests = {digest for _, digest in self._local.values() if digest is not None}
        for list_cids, list_digests in self._lists.values():
            cids |= list_cids
            digests |= list_digests
        # A file of one block is its CID's own content: its hash names the raw CID too, which every CID check (fetch,
        # store, /blocks, providing) then refuses, so the hash entry stops it as a CID entry does (spec section 8)
        cids |= {Cid.raw_of_digest(digest).binary for digest in digests}
        if cids != self.cids:
            self.cids = frozenset(cids)
        if digests != self.hashes:
            self.hashes = frozenset(digests)

    def _notify(self):
        """Tell every listener; one failing is logged."""
        for listener in list(self._listeners):
            try:
                listener()
            except Exception as e:  # noqa: BLE001 - one listener mustn't stop the others
                log(f"deny list: applying a change failed: {e!r}", LOG_WARNING)

    def save(self):
        """Persist the hashes remembered, used or forgotten since the last save, least recently used first
        (NORMAL: the node's periodic saves and its stop). Raises OSError (StateDbError) if the write fails; the
        rows are then saved by the next one."""
        with self._save_lock:
            with self._lock:
                if not self._known_dirty:
                    return
                dirty, self._known_dirty = self._known_dirty, set()
                # Each change moved its hash to the end, so those still known are the last ones, in order
                kept = sum(1 for binary in dirty if binary in self._known)
                changed = list(itertools.islice(reversed(self._known.items()), kept))[::-1]
                rows = [(binary, digest, self._known_used + n) for n, (binary, digest) in enumerate(changed)]
                self._known_used += len(rows)
                gone = [(binary,) for binary in dirty if binary not in self._known]
            try:
                with self._db.transaction() as tx:
                    tx.executemany("DELETE FROM deny_hashes WHERE cid = ?", gone)
                    tx.executemany("INSERT OR REPLACE INTO deny_hashes (cid, digest, used) VALUES (?, ?, ?)", rows)
            except BaseException:
                with self._lock:
                    self._known_dirty |= dirty
                raise

    def close(self):
        """Release the state database (idempotent)."""
        self._db.close()
