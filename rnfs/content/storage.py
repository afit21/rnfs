"""Storage: blocks, pins, cached fetches, protection and eviction (specification section 8). No RNS dependency.

- Pins are recursive and held by sources: "user", "folder", "name:<alias>", "remote:<identity hash>", "deny" (the
  deny lists' own blocks), "app:<app>" and "app:<app>:<key>" (pins an app made with its API token, rnfs.apps),
  and the features' "follow:". The LXMF feature of rnfs before 0.2.0 pinned with "lxmf:<sender>", which 0.2.0's
  upgrade rewrote as "app:lxmf:<sender>": an app called lxmf holds them. A pin lasts while any source holds it.
  Pins are never evicted.
- Cache is everything else. Each cached fetch is recorded as root + path + target, which keeps the path's
  nodes and the target's DAG; fetches are evicted least recently used first when storage exceeds
  max_storage, and gc() evicts them all.
- Blocks of fetches, adds and imports in progress are protected as if pinned, by a protection session.
- A block the deny list (`deny`, rnfs.denylist) denies is never stored: put() raises Denied. drop_denied() drops
  the cached fetches that reach one, and denied_roots() says which pins do.
- A block is deleted only if no pin, session or remaining cached fetch reaches it.
- max_storage is soft: no fetch is refused for it, and storage may exceed it while fetches run or when
  pins alone fill it. The hard bound is the disk: check_disk() refuses blocks that would leave less than
  DISK_RESERVE free on the filesystem holding the blocks.
- max_storage counts what the blocks take on disk (allocated_bytes): each block's size rounded up to the block
  store's allocation unit (fsutil.allocation_unit, read once at the start), since each is a file of its own and
  a small one takes a whole cluster (16-32 KiB on FAT32). used_bytes, the quotas and stat count block bytes.

Pins and cache records are persisted in the state database (rnfs.statedb), and only read from it at startup:
each pin and release at once, as one row inserted or deleted and committed durably (after the blocks it keeps
are synced), and cache records (which change on every fetch) by save(), which the node calls periodically and
at shutdown, writing only the records changed since the last save. The block index (every block's size) is kept
there too, written with both, and loaded at the next start only if close() saved it whole (_index): otherwise every
block is listed. Thread-safe: one lock guards the
indexes; long walks run outside it: completeness walks are discarded if blocks were deleted meanwhile, and
eviction, gc and the size queries read the DAG nodes they will walk beforehand (_warm), deciding what to delete
(or counting) under the lock, where a DAG memoised as complete is taken whole rather than walked again.
"""
import itertools
import math
import os
import re
import shutil
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Callable, Iterator

from rnfs import denylist, fsutil, ids, statedb
from rnfs.constants import MAX_BLOCK_SIZE, MIB
from rnfs.content import blockstore, dag, walker
from rnfs.content.blockstore import BlockStore
from rnfs.content.cid import Cid
from rnfs.content.dag import DirectoryNode, IndexNode
from rnfs.errors import BlockMismatch, Closed, Denied, InvalidDag, NotFound, StorageFull
from rnfs.log import LOG_ERROR, LOG_NOTICE, LOG_VERBOSE, SlowStep, log, plural, size

USER, FOLDER = "user", "folder"
SIZES_STORE = "block_sizes"  # its meta value "clean": 1 once close() saved the block_sizes table whole
DENY = denylist.SOURCE  # the deny lists' own blocks (rnfs/features/deny.py)
NAME_PREFIX, REMOTE_PREFIX = "name:", "remote:"
APP_PREFIX = "app:"
# The pins the LXMF feature of rnfs before 0.2.0 made: lxmf:<sender address> (LEGACY_LXMF_PREFIX), which 0.2.0,
# whose LXMF moved out of rnfs into an app of its own, made app:lxmf:<sender address>: an app called lxmf holds them
# (rnfs app add lxmf; rnfs app rm lxmf releases them). Its schema step rewrote the pins table's, and import_pins
# rewrites those of an older pins.json, imported after that step
LXMF_PREFIX = APP_PREFIX + "lxmf:"
LEGACY_LXMF_PREFIX = "lxmf:"
# An app's name (rnfs app add): an alias's characters (names.check_alias) in lowercase, so it never holds ":" and
# two apps never differ only by case. Pin rows are checked against these and one that fails is deleted when
# loaded, so what they accept mustn't narrow
APP_NAME = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}")
# The key an app may give its pins (app:<app>:<key>), so it can hold and release groups of its own; ":" allowed, so
# a key may nest (the name before it holds none)
PIN_KEY = re.compile(r"[A-Za-z0-9._:-]{1,128}")
MAX_CACHE_RECORDS = 10_000  # cached fetches remembered; past this the least recently used record is dropped
MISSING_MEMO = 10_000  # incomplete DAGs whose missing block dag_blocks remembers; past this the oldest is forgotten
# Blocks get(remember=True) checked against their CIDs, kept in memory (at most 1 MiB of them): the node serves one
# again, a /range answer of a few bytes at a time, without reading and hashing the whole block each time (AUD-L8)
VERIFIED_MEMORY = 8
EVICT_MARGIN = 32  # eviction frees max_storage / EVICT_MARGIN more than it must, so it runs rarely
DISK_RESERVE_MIN = 256 * MIB  # free disk space fetches always leave on the blocks' filesystem...
DISK_RESERVE_FRACTION = 0.02  # ...or this fraction of the filesystem, if larger
# Blocks a Session.writer() writes at once: each is synced to disk before its rename, and syncs that overlap
# share the filesystem's journal commits (about 9x the speed of one at a time on an SSD)
WRITE_THREADS = 16
WRITE_QUEUE = 32  # blocks a writer holds waiting to be written, at most (2 MiB of 64 KiB chunks)

DAMAGED = object()  # Storage._node's answer for a stored DAG node that is unreadable or no longer matches its CID
MISSING = object()  # Storage._read_node's answer for a block that isn't stored (or was deleted meanwhile)
UNREADABLE = "can't be read"  # how _block_problem starts its answer for a block whose read raised an OSError
_NO_MEMO = object()  # a memo's answer for a key it has no entry for (None is a memoised answer)
_NO_ENTRIES: frozenset = frozenset()  # dag_entries of a file, shared by every one memoised

# The ID in a remote: or follow: pin source, as bytes.hex() writes it: exactly what loaded before rnfs.ids, as a pin
# row that fails is deleted
_IDENTITY_HASH = ids.HEX16_LOWER


def disk_reserve(total: int) -> int:
    """The free space a filesystem of total bytes must keep: max(DISK_RESERVE_MIN, DISK_RESERVE_FRACTION of it)."""
    return max(DISK_RESERVE_MIN, int(total * DISK_RESERVE_FRACTION))


def check_source(source: str):
    """Raise ValueError unless source is a valid pin source."""
    if source in (USER, FOLDER, DENY):
        return
    if isinstance(source, str) and source.startswith(NAME_PREFIX) and len(source) > len(NAME_PREFIX):
        return
    if isinstance(source, str) and source.startswith(REMOTE_PREFIX) \
            and _IDENTITY_HASH.fullmatch(source[len(REMOTE_PREFIX):]):
        return
    # follow:<name>: the latest target of a followed name (rnfs/features/follow.py)
    if isinstance(source, str) and source.startswith("follow:") and _IDENTITY_HASH.fullmatch(source[len("follow:"):]):
        return
    # app:<app>[:<key>]: pins an app made through the API with its token (rnfs/apps.py), and the app:lxmf:<sender>
    # pins the LXMF feature of rnfs before 0.2.0 left (LXMF_PREFIX)
    if isinstance(source, str) and app_of(source) is not None:
        return
    raise ValueError(f"invalid pin source {source!r}")


def check_app_name(name) -> str:
    """name if it is a valid app name (APP_NAME), else ValueError."""
    if not isinstance(name, str) or not APP_NAME.fullmatch(name):
        raise ValueError(f"invalid app name {name!r}: 1-64 characters of a-z, 0-9, '.', '_' and '-', starting with a "
                         f"letter or digit")
    return name


def app_source(name: str, key: str | None = None) -> str:
    """The pin source of app name's pins, of its group key if given: app:<name>[:<key>]. Raises ValueError for a
    name or key that isn't valid."""
    check_app_name(name)
    if key is None:
        return APP_PREFIX + name
    if not isinstance(key, str) or not PIN_KEY.fullmatch(key):
        raise ValueError(f"invalid pin key {key!r}: 1-128 characters of A-Z, a-z, 0-9, '.', '_', ':' and '-'")
    return f"{APP_PREFIX}{name}:{key}"


def is_own_source(source) -> bool:
    """True if source pins the user's own content: the user's pins (user), the shared folder's (folder), those of a
    name this node publishes (name:<alias>) and an app's (app:<app>[:<key>]); not those another node asked for
    (remote:<identity>), a followed name's (follow:<name>) or the deny lists' own blocks (deny)."""
    return isinstance(source, str) and (source in (USER, FOLDER) or source.startswith(NAME_PREFIX)
                                        or app_of(source) is not None)


def app_of(source) -> str | None:
    """The app whose pin source this is (app:<app> or app:<app>:<key>), or None if it is no valid app source."""
    if not isinstance(source, str) or not source.startswith(APP_PREFIX):
        return None
    name, colon, key = source[len(APP_PREFIX):].partition(":")
    if not APP_NAME.fullmatch(name) or (colon and not PIN_KEY.fullmatch(key)):
        return None
    return name


def name_source(alias: str) -> str:
    """The pin source for a name's alias."""
    return NAME_PREFIX + alias


def remote_source(identity_hash: bytes | str) -> str:
    """The pin source for a remote identity (bytes, or 32 hex characters)."""
    text = identity_hash.hex() if isinstance(identity_hash, (bytes, bytearray)) else identity_hash.lower()
    return REMOTE_PREFIX + text


def upgraded_source(source):
    """source as rnfs 0.2.0 writes it: a valid lxmf:<address> (LEGACY_LXMF_PREFIX) becomes app:lxmf:<address>, as
    the schema step "lxmf_app_pins" rewrote the pins table's; anything else is returned as it is."""
    if isinstance(source, str) and source.startswith(LEGACY_LXMF_PREFIX) \
            and _IDENTITY_HASH.fullmatch(source[len(LEGACY_LXMF_PREFIX):]):
        return LXMF_PREFIX + source[len(LEGACY_LXMF_PREFIX):]
    return source


def import_pins(value: dict, tx) -> int:
    """Import pins.json (statedb.migrate): its pins replace the pins table, each record checked as rnfs 0.1.4
    loaded it (an invalid one is logged and skipped), lxmf: sources rewritten as the schema did those of the pins
    table (upgraded_source: the files are imported after the schema steps). Returns the number of pins imported."""
    tx.execute("DELETE FROM pins")
    count = 0
    for text, sources in value.get("pins", {}).items():
        try:
            cid = Cid.parse(text)
            if not isinstance(sources, list):
                raise TypeError("sources aren't a list")
            sources = [upgraded_source(source) for source in sources]
            for source in sources:
                check_source(source)
            # Inside the try: a source SQLite can't store (a lone surrogate) is an invalid record too
            tx.executemany("INSERT OR IGNORE INTO pins (cid, source) VALUES (?, ?)",
                           [(cid.binary, source) for source in sources])
        except (ValueError, TypeError):
            log(f"storage: ignoring invalid pin record {text!r}", LOG_ERROR)
            continue
        count += bool(sources)
    return count


CacheKey = tuple[Cid, tuple[str, ...]]  # a cached fetch's root and path names


def _cache_names(names) -> tuple[str, ...]:
    """A cache record's path names, checked: a list of valid names. Raises TypeError or ValueError."""
    if not isinstance(names, list):
        raise TypeError("names aren't a list")
    for name in names:
        dag.check_name(name)
    return tuple(names)


def _cache_row(key: CacheKey) -> tuple[bytes, str]:
    """The cache table's key columns (root, path) for a record's key."""
    root, names = key
    return root.binary, statedb.dump(list(names))


def _parse_cache_row(root: bytes, path: str) -> CacheKey:
    """A record's key from the cache table's key columns, checked. Raises TypeError or ValueError."""
    return Cid.from_bytes(root), _cache_names(statedb.load(path))


def import_cache(value: dict, tx) -> int:
    """Import cache.json (statedb.migrate): its records, least recently used first, replace the cache table, each
    checked as rnfs 0.1.4 loaded it. Returns the number of records imported."""
    tx.execute("DELETE FROM cache")
    records: OrderedDict[CacheKey, Cid] = OrderedDict()
    for record in value.get("fetches", []):
        try:
            root, names, target = record
            names = _cache_names(names)
            records[(Cid.parse(root), names)] = Cid.parse(target)  # a repeated key keeps its first place
        except (ValueError, TypeError):
            log(f"storage: ignoring invalid cache record {record!r}", LOG_ERROR)
    tx.executemany("INSERT INTO cache (root, path, target, used) VALUES (?, ?, ?, ?)",
                   [(*_cache_row(key), target.binary, used) for used, (key, target) in enumerate(records.items())])
    return len(records)


def _walk_path(root: Cid, names: tuple[str, ...], step: Callable[[Cid, str], tuple[Cid, bool] | None]) \
        -> Iterator[Cid]:
    """The DAG nodes a walk along <root>/<names> passes through (not the target it reaches), in order: the walk
    that keeps a cached fetch's path, written once for Storage._path_cids (under the lock) and Storage._warm
    (without it), so warming covers exactly what the locked walk reads.

    step(cid, name) is where name leads from the node cid (Storage._step_in's answer), or None if the walk ends
    there: no such name, not a directory, or cid can't be read. The walk also ends at a raw block, unyielded.
    """
    current = root
    for name in names:
        while True:
            if current.is_raw:
                return
            yield current
            answer = step(current, name)
            if answer is None:
                return
            current, through_directory = answer
            if through_directory:
                break  # the next name


class Session:
    """A protection session: blocks it stores or lists are kept as if pinned until it ends.

    A listed CID protects every stored block below it too, so listing a root protects its whole DAG.
    """

    def __init__(self, storage: "Storage"):
        """A session over storage; use Storage.protect() rather than making one directly."""
        self._storage = storage
        self.cids: set[Cid] = set()

    def add(self, cid: Cid):
        """Protect cid (and what is stored below it) for the rest of the session."""
        with self._storage._lock:
            self.cids.add(cid)

    def put(self, cid: Cid, block: bytes):
        """Store a block and protect it. Raises BlockMismatch (a ValueError) if it doesn't match its CID."""
        self.add(cid)  # first, so an eviction between storing and protecting can't take it
        self._storage._put(cid, block, protected=True)

    @contextmanager
    def writer(self) -> Iterator[Callable[[Cid, bytes], None]]:
        """A put for adds and imports: `with session.writer() as put:`. Each block is protected at once, as by
        put(), and stored on one of WRITE_THREADS threads; every one is stored (or the first error raised)
        before the with block ends, so nothing is pinned before its blocks are. Callers hand it only blocks
        that match their CIDs (a mismatch is raised, but later): a block just built from, or checked."""
        writer = _Writer(self)
        try:
            yield writer.put
        except BaseException:
            writer.close(raise_error=False)  # the body's error is the one raised; what it queued is still stored
            raise
        writer.close()


class _Writer:
    """Session.writer()'s threads: at most WRITE_QUEUE blocks waiting, the first error kept for the caller."""

    def __init__(self, session: Session):
        """A writer storing blocks for session."""
        self._session = session
        self._pool = ThreadPoolExecutor(WRITE_THREADS, thread_name_prefix="rnfs-write")
        self._room = threading.Semaphore(WRITE_QUEUE)
        self._error: BaseException | None = None

    def put(self, cid: Cid, block: bytes):
        """Protect cid and queue its block (waiting while WRITE_QUEUE are queued); raises an earlier block's
        error, so an add stops at the first one."""
        self._raise()
        self._session.add(cid)  # first, as in Session.put
        self._room.acquire()
        try:
            self._pool.submit(self._write, cid, block)
        except BaseException:
            self._room.release()
            raise

    def _write(self, cid: Cid, block: bytes):
        """Store one block on a writer thread, keeping the first error."""
        try:
            self._session._storage._put(cid, block, protected=True)
        except BaseException as e:  # noqa: BLE001 - kept and raised on the caller's thread
            if self._error is None:
                self._error = e
        finally:
            self._room.release()

    def _raise(self):
        """Raise the first error a block met, if any."""
        if self._error is not None:
            raise self._error

    def close(self, raise_error: bool = True):
        """Wait for every block queued to be stored, then raise the first error (if raise_error)."""
        self._pool.shutdown(wait=True)
        if raise_error:
            self._raise()


class Storage:
    """Blocks plus pins, cached fetches, protection and eviction."""

    def __init__(self, blocks_dir: str, state_dir: str, max_storage: int):
        """Open storage, indexing the blocks on disk and loading pins and cache records from state_dir."""
        self._lock = threading.RLock()
        self.state_dir = os.path.expanduser(state_dir)
        self._db = statedb.connect(self.state_dir)
        self.max_storage = max_storage
        self._save_lock = threading.Lock()  # keeps cache saves in order without holding _lock while writing

        # The index of stored blocks: CID -> size. Kept in the block_sizes table too, rows changed but not yet
        # written in _sizes_dirty (a size, or None for a deleted block), written whenever pins or cache records are
        # saved; the table is trusted at the next start only if close() saved it whole (_index)
        self._sizes: dict[Cid, int] = {}
        self._sizes_dirty: dict[Cid, int | None] = {}
        self._writing = 0  # blocks put() is writing to disk outside the lock
        self._closed = False  # close() ran: nothing is stored or deleted any more
        try:
            self._sizes = self._index(blocks_dir)
        except BaseException:
            self._db.close()
            raise
        self._used = sum(self._sizes.values())
        self._allocated = sum(map(self._allocation, self._sizes.values()))
        self._pins: dict[Cid, set[str]] = {}
        # When each pinned CID was first pinned by any source (Unix time; pin_times); one pinned before rnfs 0.2.0
        # has none
        self._added: dict[Cid, float] = {}
        self._pinned: frozenset[Cid] | None = None  # pinned_cids(), built when asked; None again once pins change
        self._own_pinned: frozenset[Cid] | None = None  # own_pinned_cids(), the same way, once any source changes
        # Called as pin_listener(change, cids, source) once pins of source were added ("added") or released
        # ("removed") and saved, and as cache_listener(change, cids) with the targets of cache records added
        # ("added"), dropped ("removed") or retargeted (both), each with the lock held: the node's "pin" and "cache"
        # events. Must only queue (rnfs.events)
        self.pin_listener = None
        self.cache_listener = None
        self._cache: OrderedDict[CacheKey, Cid] = OrderedDict()  # oldest first
        self._sessions: list[Session] = []
        self._links: dict[Cid, tuple[Cid, ...]] = {}  # children of stored DAG nodes; CIDs never change content
        # Where a path step leads from a stored DAG node: node -> {name: _step_in's answer}; memoised like _links
        self._steps: dict[Cid, dict[str, tuple[Cid, bool] | None]] = {}
        self._complete: dict[Cid, frozenset[Cid]] = {}  # dag_blocks memo, complete DAGs only
        # dag_entries memo: the entries of each of _complete's DAGs, found by the same walk (_NO_ENTRIES for a
        # file); kept and forgotten with _complete
        self._complete_entries: dict[Cid, frozenset[Cid]] = {}
        self._complete_bytes: dict[Cid, int] = {}  # dag_bytes memo: the bytes of _complete's DAGs, as asked for
        # dag_blocks memo for incomplete DAGs: root -> a block of its DAG found missing. A CID's content never
        # changes, so the DAG is incomplete for as long as that block isn't stored (checked at each lookup)
        self._missing: dict[Cid, Cid] = {}
        self._generation = 0  # incremented whenever blocks are deleted, invalidating walks in progress
        self._cache_generation = 0  # incremented whenever cache records are removed or retargeted
        self._changes = 0  # incremented with each block stored, pin, release and new cache record (changes())
        # Cache records changed (or removed) since the last save(); every one is among the most recently used
        self._cache_dirty: set[CacheKey] = set()
        self._cache_used = 0  # the cache table's next used value: larger than every one saved
        # False only when nothing has happened since the last collection that could leave a block unreached,
        # so eviction over max_storage with nothing to evict needn't walk everything again
        self._maybe_garbage = True
        self._reported_damaged: set[Cid] = set()  # damaged DAG nodes already logged as stopping collection
        # The last VERIFIED_MEMORY blocks get(remember=True) verified, with their files' signatures, least recently used
        # first; forgotten as each is deleted (_delete), under the lock get checks a block is stored with
        self._verified: OrderedDict[Cid, tuple[bytes, tuple[int, int, int]]] = OrderedDict()
        self.deny = denylist.NONE  # what put() refuses; the node sets its DenyList
        try:
            self._load()
        except BaseException:
            self._db.close()
            raise

    # Persistence

    def _index(self, blocks_dir: str) -> dict[Cid, int]:
        """Open the block store and index its blocks: from the block_sizes table if the last stop was clean (so
        startup needn't list every block), else by listing them all (also at the first start of rnfs 0.2.0), the
        table then written again. The clean mark is cleared, durably, before anything changes on disk, and set
        again only by close()."""
        clean = self._db.meta(SIZES_STORE, "clean")
        if clean == 1:
            with self._db.transaction(durable=True) as tx:
                statedb.set_meta(tx, SIZES_STORE, "clean", 0)
        self.blocks = BlockStore(blocks_dir, report=lambda text: log(f"storage: {text}", LOG_NOTICE))
        # What the blocks' filesystem allocates a file in, read once: the filesystem under a directory rnfs keeps
        # open doesn't change while it runs, and a stat per block would cost a syscall each
        self.allocation_unit = fsutil.allocation_unit(self.blocks.directory)
        sizes = self._load_sizes() if clean == 1 else None
        if sizes is not None:
            # Blocks found directly in blocks/: those a failed move left there are in the table, but an older rnfs
            # run since may have stored more
            for cid, length in self.blocks.found_flat.items():
                if cid not in sizes:
                    sizes[cid] = self._sizes_dirty[cid] = length
            return sizes
        if clean == 0:
            log("storage: listing every stored block, since rnfs didn't stop cleanly last time", LOG_NOTICE)
        # At the first start of 0.2.0 (no mark yet) this lists every block quietly, unless it takes a while: on a
        # large store, minutes, right after the move out of the flat layout
        with SlowStep("storage: listing every stored block to record its size", "storage: listed every stored block"):
            sizes = dict(self.blocks.scan(tidy=True))
            # Only the rows that differ are written: after a crash most are right, and reading them back and comparing
            # costs less than rewriting them all (a third less with a million blocks)
            saved = self._load_sizes(quiet=True)
            with self._db.transaction() as tx:
                if saved is None:
                    tx.execute("DELETE FROM block_sizes")
                    saved = {}
                tx.executemany("DELETE FROM block_sizes WHERE cid = ?",
                               [(cid.binary,) for cid in saved.keys() - sizes])
                tx.executemany("INSERT OR REPLACE INTO block_sizes (cid, size) VALUES (?, ?)",
                               [(cid.binary, length) for cid, length in sizes.items() if saved.get(cid) != length])
                statedb.set_meta(tx, SIZES_STORE, "clean", 0)
        return sizes

    def _load_sizes(self, quiet: bool = False) -> dict[Cid, int] | None:
        """The block_sizes table, or None if a row doesn't parse (a database edited by hand): then every block is
        listed instead, and the table written again whole. quiet: the blocks are being listed anyway, so a bad row
        isn't logged."""
        sizes = {}
        for binary, length in self._db.query("SELECT cid, size FROM block_sizes"):
            try:
                cid = Cid.from_bytes(binary)
            except (ValueError, TypeError):
                cid = None
            if cid is None or type(length) is not int or not 0 <= length <= MAX_BLOCK_SIZE:
                if not quiet:
                    log(f"storage: ignoring the saved block sizes (invalid row {binary!r} {length!r}); listing "
                        "every stored block", LOG_ERROR)
                return None
            sizes[cid] = length
        return sizes

    def _take_sizes(self) -> dict[Cid, int | None]:
        """The block_sizes rows changed since they were last written, to be written now (lock held)."""
        dirty, self._sizes_dirty = self._sizes_dirty, {}
        return dirty

    def _restore_sizes(self, dirty: dict[Cid, int | None]):
        """Keep the rows a failed write didn't write for the next one, unless changed since (lock held)."""
        for cid, length in dirty.items():
            self._sizes_dirty.setdefault(cid, length)

    @staticmethod
    def _write_sizes(tx, dirty: dict[Cid, int | None]):
        """Write the block_sizes rows changed (dirty, from _take_sizes) in transaction tx."""
        tx.executemany("DELETE FROM block_sizes WHERE cid = ?",
                       [(cid.binary,) for cid, length in dirty.items() if length is None])
        tx.executemany("INSERT OR REPLACE INTO block_sizes (cid, size) VALUES (?, ?)",
                       [(cid.binary, length) for cid, length in dirty.items() if length is not None])

    def _load(self):
        """Load pins and cache records; rows that don't parse are logged and deleted, as are cache records past
        MAX_CACHE_RECORDS."""
        bad_pins = []
        for binary, source in self._db.query("SELECT cid, source FROM pins"):
            try:
                cid = Cid.from_bytes(binary)
                check_source(source)
            except (ValueError, TypeError):
                log(f"storage: ignoring invalid pin record {binary!r} {source!r}", LOG_ERROR)
                bad_pins.append((binary, source))
                continue
            self._pins.setdefault(cid, set()).add(source)
        stale_times = []
        for binary, added in self._db.query("SELECT cid, added FROM pin_times"):
            try:
                cid = Cid.from_bytes(binary)
            except (ValueError, TypeError):
                cid = None
            # A time without its pin is deleted: they are written in one transaction, so only a pin row refused
            # above (or a database edited by hand) leaves one
            if cid in self._pins and isinstance(added, (int, float)) and math.isfinite(added):
                self._added[cid] = float(added)
            else:
                stale_times.append((binary,))
        bad_cache = []
        for root, path, target, used in self._db.query("SELECT root, path, target, used FROM cache ORDER BY used"):
            try:
                self._cache[_parse_cache_row(root, path)] = Cid.from_bytes(target)
            except (ValueError, TypeError):
                log(f"storage: ignoring invalid cache record {root!r} {path!r}", LOG_ERROR)
                bad_cache.append((root, path))
                continue
            if isinstance(used, int):
                self._cache_used = max(self._cache_used, used + 1)
        capped = [_cache_row(key) for key in list(self._cache)[:max(0, len(self._cache) - MAX_CACHE_RECORDS)]]
        self._cap_cache()
        self._cache_dirty.clear()
        if bad_pins or bad_cache or capped or stale_times:
            with self._db.transaction() as tx:
                tx.executemany("DELETE FROM pins WHERE cid = ? AND source = ?", bad_pins)
                tx.executemany("DELETE FROM cache WHERE root = ? AND path = ?", bad_cache + capped)
                tx.executemany("DELETE FROM pin_times WHERE cid = ?", stale_times)

    def _save_pins(self, source: str, added=(), removed=(), first=(), at: float = 0.0, gone=()):
        """Persist source's new pins of added and releases of removed, durably, with the CIDs first pinned at
        all now (first, at time at) and no longer pinned at all (gone) (called with the lock held). The block
        sizes changed since they were last written are written with them."""
        sizes = self._take_sizes()
        try:
            self.blocks.sync()  # so pins saved never outlast the blocks they keep
            with self._db.transaction(durable=True) as tx:
                tx.executemany("INSERT OR IGNORE INTO pins (cid, source) VALUES (?, ?)",
                               [(cid.binary, source) for cid in added])
                tx.executemany("INSERT OR IGNORE INTO pin_times (cid, added) VALUES (?, ?)",
                               [(cid.binary, at) for cid in first])
                tx.executemany("DELETE FROM pin_times WHERE cid = ?", [(cid.binary,) for cid in gone])
                tx.executemany("DELETE FROM pins WHERE cid = ? AND source = ?",
                               [(cid.binary, source) for cid in removed])
                self._write_sizes(tx, sizes)
        except BaseException:
            self._restore_sizes(sizes)
            raise

    def save(self):
        """Persist the cache records changed since the last save, least recently used first, and the block sizes
        changed."""
        with self._save_lock:
            with self._lock:
                if not self._cache_dirty and not self._sizes_dirty:
                    return
                dirty, self._cache_dirty = self._cache_dirty, set()
                # The records changed since the last save are the most recently used (each change moved its
                # record to the end), so those still cached are the last ones, in order
                kept = sum(1 for key in dirty if key in self._cache)
                changed = list(itertools.islice(reversed(self._cache.items()), kept))[::-1]
                rows = [(*_cache_row(key), target.binary, self._cache_used + n)
                        for n, (key, target) in enumerate(changed)]
                self._cache_used += len(rows)
                gone = [_cache_row(key) for key in dirty if key not in self._cache]
            try:
                self.blocks.sync()
                # The sizes are taken and written under the lock, so a pin saved meanwhile (which writes them too)
                # can't have its newer rows overwritten by older ones. A row may be written before its block's
                # name is synced: the table is only trusted after a clean stop, which syncs first
                with self._lock:
                    sizes = self._take_sizes()
                    try:
                        with self._db.transaction() as tx:
                            tx.executemany("DELETE FROM cache WHERE root = ? AND path = ?", gone)
                            tx.executemany("INSERT OR REPLACE INTO cache (root, path, target, used) "
                                           "VALUES (?, ?, ?, ?)", rows)
                            self._write_sizes(tx, sizes)
                    except BaseException:
                        self._restore_sizes(sizes)
                        raise
            except BaseException:
                with self._lock:
                    self._cache_dirty |= dirty
                raise

    def close(self):
        """Release the state database (the node calls this once it has saved everything at shutdown), first saving
        the block sizes whole and marking them clean, so the next start loads them rather than listing every
        block. Not marked if a put is still writing (a thread that outlived the node's stop) or a partial file
        may be left: the next start lists every block. Nothing is stored or deleted afterwards."""
        try:
            with self._lock:
                if self._closed:
                    return
                self._closed = True
                if self._writing or self.blocks.untidy:
                    return
                sizes = self._take_sizes()
                try:
                    self.blocks.sync()
                    with self._db.transaction(durable=True) as tx:
                        self._write_sizes(tx, sizes)
                        statedb.set_meta(tx, SIZES_STORE, "clean", 1)
                except OSError as e:
                    log(f"storage: couldn't save the block sizes ({e}); the next start lists every block", LOG_ERROR)
        finally:
            self._db.close()

    # Blocks

    def has(self, cid: Cid) -> bool:
        """True if the block is stored."""
        with self._lock:
            return cid in self._sizes

    def get(self, cid: Cid, verify: bool = False, remember: bool = False) -> bytes | None:
        """The stored block, or None. With verify, a block that no longer matches its CID is deleted, giving None.

        With remember too, the block is kept in memory once verified, among the last VERIFIED_MEMORY, with its file's
        signature (BlockStore.signature), and given from there, unread and unhashed, while it stays stored and its
        file keeps that signature: a file rewritten or replaced since is read and checked again.
        """
        signature = None
        with self._lock:
            if cid not in self._sizes:
                return None
            remembered = self._verified.get(cid) if verify and remember else None
        if remembered is not None:
            block, kept = remembered
            try:
                signature = self.blocks.signature(cid)
            except OSError:
                signature = None
            if signature is not None and signature == kept:
                with self._lock:
                    if cid in self._verified:
                        self._verified.move_to_end(cid)
                return block
        if verify and remember and signature is None:
            try:
                signature = self.blocks.signature(cid)  # before the read: a change after it is seen at the next one
            except OSError:
                signature = None
        try:
            block = self.blocks.get(cid)
        except OSError as e:
            # Unreadable (a disk error, a file this user can't open): damaged, as _read_node takes it, but kept until
            # verify() removes it, as a damaged node is: a file a run as root left is whole, and may be the only copy
            log(f"storage: block {cid} {UNREADABLE} ({e}); `rnfs verify` removes it", LOG_ERROR)
            return None
        if block is None or (verify and not blockstore.matches(cid, block)):
            with self._lock:
                problem = self._block_problem(cid) if cid in self._sizes else None
                if problem is not None:
                    log(f"storage: block {cid} {problem}; removing it", LOG_ERROR)
                    self._delete({cid})  # if Windows defers it, verify() or the next read tries again
            return None
        if verify and remember and signature is not None:
            with self._lock:
                if cid in self._sizes:  # not deleted while it was read: _delete forgets it under this lock
                    self._verified[cid] = (block, signature)
                    self._verified.move_to_end(cid)
                    while len(self._verified) > VERIFIED_MEMORY:
                        self._verified.popitem(last=False)
        return block

    def _block_problem(self, cid: Cid) -> str | None:
        """Why the stored block cid is bad, for the log, or None if it can be read and matches its CID. An unreadable
        one says why (the OSError: a disk error, or a file this user can't open, such as one a run as root left)."""
        try:
            block = self.blocks.get(cid)
        except OSError as e:
            return f"{UNREADABLE} ({e})"
        if block is None:
            return "is missing"
        return None if blockstore.matches(cid, block) else "is damaged (it doesn't match its CID)"

    def block_size(self, cid: Cid) -> int | None:
        """Length of a stored block, or None."""
        with self._lock:
            return self._sizes.get(cid)

    def put(self, cid: Cid, block: bytes):
        """Store a block after verifying it (atomic, durable). Raises BlockMismatch (a ValueError) if it doesn't
        match or is too large, even when the block is already stored, and Denied if the deny list denies it.

        A block stored outside a protection session is cache that no record reaches: the first to go.
        """
        self._put(cid, block, protected=False)

    def _put(self, cid: Cid, block: bytes, protected: bool):
        """Store a block; protected if a session already lists it, so it can't be garbage. Raises Closed once
        close() has run."""
        if self.deny.denies(cid):
            raise Denied(f"{cid} is on this node's deny list")
        with self._lock:
            if self._closed:
                raise Closed("storage is closed")  # a block stored now would be missing from the saved sizes
            stored = cid in self._sizes
            if not stored:
                self._writing += 1
        if stored:
            # Verified all the same, so callers (archive import) never count unchecked bytes as supplied
            if not blockstore.matches(cid, block):
                raise BlockMismatch(f"block does not match {cid}")
            return
        try:
            self.blocks.put(cid, block)  # verifies; outside the lock, since it writes to disk
        finally:
            with self._lock:
                self._writing -= 1
        with self._lock:
            if cid not in self._sizes:
                self._sizes[cid] = self._sizes_dirty[cid] = len(block)
                self._used += len(block)
                self._allocated += self._allocation(len(block))
                self._changes += 1
                if not protected:
                    self._maybe_garbage = True

    def used_bytes(self) -> int:
        """Bytes of every stored block."""
        with self._lock:
            return self._used

    def allocated_bytes(self) -> int:
        """What every stored block takes on disk (_allocation of each one's size): what max_storage bounds."""
        with self._lock:
            return self._allocated

    def _allocation(self, length: int) -> int:
        """What a block of length bytes takes on disk: a file of its own, so rounded up to the allocation unit."""
        return fsutil.allocated(length, self.allocation_unit)

    def block_count(self) -> int:
        """Number of stored blocks."""
        with self._lock:
            return len(self._sizes)

    def getter(self) -> walker.Get:
        """A walker get(expect) over stored blocks, raising NotFound for a block that isn't stored."""

        def get(expect: walker.Expect) -> bytes:
            """The stored block for expect.cid."""
            block = self.get(expect.cid)
            if block is None:
                raise NotFound(f"block {expect.cid} is not stored")
            return block

        return get

    # Pins

    def pin(self, cid: Cid, source: str):
        """Pin cid with source (it lasts while any source holds it). Fetching it is the caller's job."""
        self.pin_many([cid], source)

    def pin_many(self, cids, source: str):
        """Pin each of cids with source, as pin() does, saving pins once for them all. If the save fails none of
        them is newly pinned, and the error is raised."""
        check_source(source)
        with self._lock:
            added = []
            for cid in cids:
                sources = self._pins.setdefault(cid, set())
                if source not in sources:
                    sources.add(source)
                    added.append(cid)
            if not added:
                return
            self._changes += 1
            self._pinned = self._own_pinned = None
            now = time.time()
            first = [cid for cid in added if self._pins[cid] == {source}]  # pinned by nothing else before
            try:
                self._save_pins(source, added=added, first=first, at=now)
            except BaseException:
                # Not saved (a full or failing disk): not pinned, so memory says what the database does
                for cid in added:
                    sources = self._pins[cid]
                    sources.discard(source)
                    if not sources:
                        del self._pins[cid]
                self._pinned = self._own_pinned = None
                raise
            for cid in first:  # a pin another source held already keeps its first time (or its lack of one)
                self._added[cid] = now
            self._tell(self.pin_listener, "added", added, source)

    def unpin(self, cid: Cid, source: str) -> bool:
        """Release source's hold on cid; True if it held it. Blocks go at the next evict() or gc()."""
        with self._lock:
            sources = self._pins.get(cid)
            if sources is None or source not in sources:
                return False
            sources.discard(source)
            self._changes += 1
            self._own_pinned = None
            if not sources:
                del self._pins[cid]
                self._maybe_garbage = True
                self._pinned = None
            gone = [cid] if cid not in self._pins else []
            try:
                self._save_pins(source, removed=[cid], gone=gone)
            except BaseException:
                self._pins.setdefault(cid, set()).add(source)  # still pinned, as the database says
                self._pinned = self._own_pinned = None
                raise
            if gone:
                self._added.pop(cid, None)
            self._tell(self.pin_listener, "removed", [cid], source)
            return True

    def _tell(self, listener, *args):
        """Call a pin or cache listener, if set (lock held); one that fails is logged, never raised: the change
        it reports is made and saved already."""
        if listener is None:
            return
        try:
            listener(*args)
        except Exception as e:  # noqa: BLE001 - see above
            log(f"storage: a change listener failed: {e!r}", LOG_ERROR)

    def pin_added(self, cid: Cid) -> float | None:
        """When cid was first pinned (Unix time; None if not pinned, or pinned before rnfs 0.2.0 recorded it)."""
        with self._lock:
            return self._added.get(cid) if cid in self._pins else None

    def pin_order(self, cids) -> list[Cid]:
        """cids (pinned CIDs) oldest pin first: by when each was first pinned, those pinned before rnfs 0.2.0
        recorded it (and any not pinned) first, then by CID, so the order is the same at every call."""
        with self._lock:
            added = self._added
            return sorted(cids, key=lambda cid: (added.get(cid, 0.0) if cid in self._pins else 0.0, cid.binary))

    def pins(self) -> dict[Cid, set[str]]:
        """Every pin and its sources (a copy)."""
        with self._lock:
            return {cid: set(sources) for cid, sources in self._pins.items()}

    def pinned_cids(self) -> frozenset[Cid]:
        """Every pinned CID: the same frozenset until pins change, so asking after every add costs nothing where
        pins() would copy every pin and its sources."""
        with self._lock:
            if self._pinned is None:
                self._pinned = frozenset(self._pins)
            return self._pinned

    def own_pinned_cids(self) -> frozenset[Cid]:
        """Every CID pinned by a source of the user's own content (is_own_source), whatever else pins it: the same
        frozenset until a pin's sources change."""
        with self._lock:
            if self._own_pinned is None:
                self._own_pinned = frozenset(cid for cid, sources in self._pins.items()
                                             if any(is_own_source(source) for source in sources))
            return self._own_pinned

    def pinned_by(self, source: str) -> set[Cid]:
        """CIDs held by source."""
        with self._lock:
            return {cid for cid, sources in self._pins.items() if source in sources}

    def is_pinned(self, cid: Cid) -> bool:
        """True if any source pins cid."""
        with self._lock:
            return cid in self._pins

    def has_pin(self, cid: Cid, source: str) -> bool:
        """True if source pins cid (a lookup, where pins() would copy every pin)."""
        with self._lock:
            sources = self._pins.get(cid)
            return sources is not None and source in sources

    def pin_count(self) -> int:
        """Number of pinned CIDs."""
        with self._lock:
            return len(self._pins)

    # Cache

    def record_fetch(self, root: Cid, names: list[str] | tuple[str, ...], target: Cid) -> set[Cid]:
        """Record (or touch, as most recently used) a cached fetch of <root>/<names> leading to target.

        Returns the targets of records dropped to stay within MAX_CACHE_RECORDS, no longer cached.
        """
        with self._lock:
            self._set_record((root, tuple(names)), target)
            dropped = self._cap_cache()
            return dropped - set(self._cache.values())

    def _set_record(self, key: CacheKey, target: Cid):
        """Record key -> target as the most recently used, with the bookkeeping a new or retargeted record needs:
        a retargeted one may leave its old target's blocks unreached (lock held)."""
        previous = self._cache.get(key)
        if previous is None:
            self._changes += 1
        elif previous != target:
            self._maybe_garbage = True
            self._cache_generation += 1
        self._cache[key] = target
        self._cache.move_to_end(key)
        self._cache_dirty.add(key)
        if previous != target:
            if previous is not None:  # a retargeted record (rare: a path whose name now leads elsewhere)
                self._tell(self.cache_listener, "removed", [previous])
            self._tell(self.cache_listener, "added", [target])

    def _drop_records(self, keys) -> list[Cid]:
        """Remove the records of keys, with the bookkeeping every removal needs: the save deletes their rows
        (_cache_dirty), _prune_fetched runs (_cache_generation), and the next collection looks for what they
        left unreached (_maybe_garbage); their targets, in order, a target once per record (lock held)."""
        targets = []
        for key in keys:
            targets.append(self._cache.pop(key))
            self._cache_dirty.add(key)
        if targets:
            self._maybe_garbage = True
            self._cache_generation += 1
            # One per target of the records dropped, as cache_ls lists records: another record may still lead to
            # the same target, which a client reloading the list then sees
            self._tell(self.cache_listener, "removed", list(dict.fromkeys(targets)))
        return targets

    def _cap_cache(self) -> set[Cid]:
        """Drop the least recently used records past MAX_CACHE_RECORDS; their targets (lock held)."""
        excess = max(0, len(self._cache) - MAX_CACHE_RECORDS)
        return set(self._drop_records(list(itertools.islice(self._cache, excess))))

    def touch(self, root: Cid, names: list[str] | tuple[str, ...]) -> bool:
        """Mark a cached fetch as used now; False if there is no such record."""
        key = (root, tuple(names))
        with self._lock:
            if key not in self._cache:
                return False
            self._cache.move_to_end(key)
            self._cache_dirty.add(key)
            return True

    def cached_fetches(self) -> list[tuple[Cid, tuple[str, ...], Cid]]:
        """Every cached fetch as (root, names, target), least recently used first."""
        with self._lock:
            return [(root, names, target) for (root, names), target in self._cache.items()]

    def cached_targets(self) -> set[Cid]:
        """Targets of every cached fetch."""
        with self._lock:
            return set(self._cache.values())

    def cache_generation(self) -> int:
        """A count that grows whenever cache records are removed or lead somewhere new (eviction, gc, the
        MAX_CACHE_RECORDS cap, a fetch recorded again with another target); while it is unchanged,
        cached_targets() has only gained members."""
        with self._lock:
            return self._cache_generation

    def changes(self) -> int:
        """A count that grows with every block stored or deleted, pin or release, and cache record added, removed
        or retargeted: while it is unchanged, so are pins(), cached_targets() and every dag_blocks() answer."""
        with self._lock:
            return self._changes + self._generation + self._cache_generation

    # Protection

    @contextmanager
    def protect(self) -> Iterator[Session]:
        """A protection session for a fetch, add or import in progress: `with storage.protect() as session:`."""
        session = Session(self)
        with self._lock:
            self._sessions.append(session)
        try:
            yield session
        finally:
            with self._lock:
                self._sessions.remove(session)
                if session.cids:
                    self._maybe_garbage = True

    # Reachability

    def _node(self, cid: Cid):
        """The stored DAG node cid (lock held): None if it's missing or raw, or invalid (it matches its CID but
        doesn't decode, so it lists nothing); DAMAGED if it's unreadable or no longer matches its CID, after
        a power loss or a disk error, so what it lists is unknown. Checked against the CID, since a node's
        children decide what collection keeps."""
        if cid.is_raw or cid not in self._sizes:
            return None
        node = self._read_node(cid)
        return DAMAGED if node is MISSING else node

    def _read_node(self, cid: Cid):
        """The DAG node cid read from disk, needing no lock: as _node, but MISSING if it isn't stored."""
        if cid.is_raw:
            return None
        try:
            block = self.blocks.get(cid)
        except OSError:
            return DAMAGED
        if block is None:
            return MISSING
        if not blockstore.matches(cid, block):
            return DAMAGED
        try:
            return dag.decode(block)
        except InvalidDag:
            return None

    @staticmethod
    def _listed(node) -> tuple[Cid, ...]:
        """The CIDs a decoded DAG node lists; none for None (raw, missing or invalid)."""
        return () if node is None else node.child_cids

    @staticmethod
    def _step_in(node, name: str) -> tuple[Cid, bool] | None:
        """Where the path step name leads from a decoded node: (entry, True) through a directory, (child, False)
        to the index node child that may hold it (walker.index_child), or None (no such entry, or not a
        directory)."""
        if isinstance(node, DirectoryNode):
            entry = next((e for e in node.entries if e.name == name), None)
            return None if entry is None else (entry.cid, True)
        if isinstance(node, IndexNode):
            position = walker.index_child(node, name)
            return None if position is None else (node.children[position].cid, False)
        return None

    def _children(self, cid: Cid) -> tuple[Cid, ...] | None:
        """CIDs listed by a stored DAG node; none for raw, missing or invalid blocks; None if the node is
        damaged, as its children are unknown (lock held)."""
        listed = self._links.get(cid)
        if listed is None:
            if cid.is_raw or cid not in self._sizes:
                return ()  # not memoised: a raw block never lists anything, and a missing node may be stored later
            node = self._node(cid)
            if node is DAMAGED:
                return None  # not memoised: verify() removes it, or a fetch replaces it
            listed = self._links[cid] = self._listed(node)
        return listed

    def _reach(self, roots, known: set[Cid] | frozenset[Cid] = frozenset(),
               damaged: set[Cid] | None = None, partial: set[Cid] | frozenset[Cid] = frozenset()) -> set[Cid]:
        """Stored blocks reachable from roots through stored DAG nodes, not passing through known; damaged
        nodes reached are added to damaged (lock held).

        A DAG memoised as complete (_complete) is taken whole, less known, rather than walked: every block of it
        is stored, and the walk would reach exactly those not in known, since known holds the whole DAG below
        each of its blocks, except partial ones: blocks of known whose stored children may not all be in known
        (a kept record's path nodes, in _evict_to). A complete DAG holding one of those is walked as before.
        """
        reached: set[Cid] = set()
        stack = [cid for cid in roots if cid in self._sizes and cid not in known]
        while stack:
            cid = stack.pop()
            if cid in reached:
                continue
            blocks = self._complete.get(cid)
            if blocks is not None and partial.isdisjoint(blocks):
                reached |= blocks - known if known else blocks
                continue
            reached.add(cid)
            listed = self._children(cid)
            if listed is None:
                if damaged is not None:
                    damaged.add(cid)
                continue
            stack.extend(child for child in listed
                         if child in self._sizes and child not in reached and child not in known)
        return reached

    def _path_cids(self, root: Cid, names: tuple[str, ...], damaged: set[Cid] | None = None) -> set[Cid]:
        """The stored nodes along <root>/<names> (not their other children), as far as they're stored; damaged
        nodes met are added to damaged (lock held)."""

        def step(cid: Cid, name: str) -> tuple[Cid, bool] | None:
            """_step_in's answer at cid, memoised (_steps); None if cid isn't stored, or is damaged."""
            if cid not in self._sizes:
                return None
            answer = self._steps.get(cid, {}).get(name, _NO_MEMO)
            if answer is _NO_MEMO:
                node = self._node(cid)
                if node is DAMAGED:
                    if damaged is not None:
                        damaged.add(cid)
                    return None  # not memoised, as in _children
                answer = self._steps.setdefault(cid, {})[name] = self._step_in(node, name)
            return answer

        return {cid for cid in _walk_path(root, names, step) if cid in self._sizes}

    def _warm(self, roots: set[Cid], paths: list[tuple[Cid, tuple[str, ...]]]):
        """Read and decode, without the lock, the DAG nodes a collection will walk from roots and along paths,
        and memoise what they list (_links, _steps), so the walk under the lock reads (almost) nothing from disk
        and /blocks serving isn't held up behind it.

        Only a memo is filled here, never a decision: a CID's content never changes, and only nodes read whole
        and matching their CID are memoised (a damaged or missing one is left for the walk under the lock). What
        to delete is still decided under the lock, from the pins, sessions and records current then, so a block
        that became reachable meanwhile is kept; nodes stored meanwhile are read under the lock.
        """
        with self._lock:
            links = dict(self._links)
            steps = {cid: dict(names) for cid, names in self._steps.items()} if paths else {}
            # A DAG memoised as complete is taken whole by the walk (_reach): nothing of it needs reading. After a
            # start, what is provided has been (the first refresh's dag_blocks), so the first collection reads
            # (almost) nothing: on a node with 857k blocks a sealed folder's unpack waited 3 minutes for it
            complete = set(self._complete)
        found_links: dict[Cid, tuple[Cid, ...]] = {}
        found_steps: dict[tuple[Cid, str], tuple[Cid, bool] | None] = {}
        seen: set[Cid] = set()
        stack = list(roots)
        while stack:
            cid = stack.pop()
            if cid in seen or cid.is_raw or cid in complete:
                continue
            seen.add(cid)
            listed = links.get(cid)
            if listed is None:
                node = self._read_node(cid)
                if node is MISSING or node is DAMAGED:
                    continue
                listed = found_links[cid] = self._listed(node)
            stack.extend(child for child in listed if child not in seen and not child.is_raw)

        def step(cid: Cid, name: str) -> tuple[Cid, bool] | None:
            """_step_in's answer at cid, memoised or read; None if cid can't be read (the walk under the lock
            reads it)."""
            known = steps.get(cid, {}).get(name, found_steps.get((cid, name), _NO_MEMO))
            if known is _NO_MEMO:
                node = self._read_node(cid)
                if node is MISSING or node is DAMAGED:
                    return None
                known = found_steps[(cid, name)] = self._step_in(node, name)
            return known

        for root, names in paths:
            for _ in _walk_path(root, names, step):
                pass  # step memoises as the walk goes: the same walk _path_cids makes under the lock
        with self._lock:
            for cid, listed in found_links.items():
                if cid in self._sizes:
                    self._links.setdefault(cid, listed)
            for (cid, name), step in found_steps.items():
                if cid in self._sizes:
                    self._steps.setdefault(cid, {}).setdefault(name, step)

    def _live(self, damaged: set[Cid] | None = None) -> set[Cid]:
        """Every stored block a pin, a session or a cached fetch reaches (lock held)."""
        roots = self._roots() | set(self._cache.values())
        live = self._reach(roots, damaged=damaged)
        for root, names in self._cache:
            live |= self._path_cids(root, names, damaged)
        return live

    def _held_back(self, damaged: set[Cid]) -> bool:
        """True, after reporting it, if damaged nodes that something reaches stop a collection (lock held).

        Blocks below a damaged node can't be told from garbage, so nothing is deleted while one is reached:
        the next collection tries again once verify() has removed it or a fetch has replaced it.
        """
        if not damaged:
            return False
        self._maybe_garbage = True
        for cid in damaged - self._reported_damaged:
            log(f"storage: DAG node {cid} is damaged, so the blocks below it are unknown; deleting nothing until "
                f"`rnfs verify` removes it", LOG_ERROR)
        self._reported_damaged |= damaged
        return True

    def _delete(self, cids: set[Cid]) -> tuple[set[Cid], int]:
        """Delete stored blocks; returns (deleted, bytes freed) (lock held). Invalidates completeness memos.

        A block Windows keeps in use (a reader has it open) stays stored and indexed, so the index always
        matches the disk; it is left out of deleted, and the next collection tries again. So does one whose delete
        fails (an OSError: a disk error, a filesystem remounted read-only), which is logged; the blocks deleted
        around it are accounted for all the same, and their DAGs' completeness memos forgotten.
        """
        deleted: set[Cid] = set()
        freed = released = 0  # bytes, and what they took on disk
        deferred = False
        failed, error = 0, None  # deletes that raised, and the first error
        if self._closed:
            return deleted, freed  # the sizes are saved: the blocks stay, and are collected after the next start
        for cid in cids:
            if cid in self._sizes:
                try:
                    removed = self.blocks.delete(cid)
                except OSError as e:
                    failed += 1
                    error = error or e  # one line for them all: a read-only filesystem fails every one
                    deferred = True
                    continue
                if removed is None:
                    log(f"storage: block {cid} is in use; deleting it later", LOG_VERBOSE)
                    deferred = True
                    continue
                deleted.add(cid)
                self._verified.pop(cid, None)
                length = self._sizes.pop(cid)
                freed += length
                released += self._allocation(length)
                self._sizes_dirty[cid] = None
                self._links.pop(cid, None)
                self._steps.pop(cid, None)
                self._reported_damaged.discard(cid)
        if deleted:
            self._used -= freed
            self._allocated -= released
            self._generation += 1
            self._complete = {root: blocks for root, blocks in self._complete.items() if not blocks & deleted}
            self._complete_entries = {root: entries for root, entries in self._complete_entries.items()
                                      if root in self._complete}
            self._complete_bytes = {root: n for root, n in self._complete_bytes.items() if root in self._complete}
        if failed:
            log(f"storage: couldn't delete {plural(failed, 'block')} ({error}); trying again later", LOG_ERROR)
        if deleted or deferred:
            # A deleted DAG node no longer reaches its children; a deferred block is still garbage
            self._maybe_garbage = True
        return deleted, freed

    def _collect(self) -> tuple[set[Cid], int]:
        """Delete every stored block nothing reaches; returns (deleted, bytes freed) (lock held)."""
        damaged: set[Cid] = set()
        live = self._live(damaged)
        if self._held_back(damaged):
            return set(), 0
        dead = {cid for cid in self._sizes if cid not in live}
        deleted, freed = self._delete(dead)
        self._maybe_garbage = deleted != dead  # anything deferred is collected on a later pass
        return deleted, freed

    def _evict_to(self, limit: int) -> tuple[set[Cid], int, int]:
        """Delete unreached blocks, then cached fetches least recently used first, until the blocks take at most
        limit bytes on disk (allocated_bytes) or nothing evictable is left; returns (deleted, bytes freed, records
        evicted) (lock held).

        One walk: what pins and sessions reach, then each record's own blocks from the most recently used
        down, keeping records while everything kept fits; the older ones are evicted.
        """
        if not self._cache and not self._maybe_garbage:
            return set(), 0, 0  # only pins and sessions hold blocks: there is nothing to evict
        damaged: set[Cid] = set()
        live = self._reach(self._roots(), damaged=damaged)
        # What pins and sessions don't reach, rather than what they do: the bytes kept follow from it with one
        # pass over the stored blocks, and the blocks to delete are what is left of it once records are kept
        dead = self._sizes.keys() - live
        allocation = self._allocation
        kept = self._allocated - sum(allocation(self._sizes[cid]) for cid in dead)
        partial: set[Cid] = set()  # path nodes kept without the rest of their DAG: see _reach
        records = list(self._cache.items())  # oldest first
        keep = 0
        for (root, names), target in reversed(records):
            reached = self._reach({target}, live, damaged, partial)
            path = self._path_cids(root, names, damaged) - live
            more = reached | path
            more_bytes = sum(allocation(self._sizes[cid]) for cid in more)
            if kept + more_bytes > limit:
                break
            live |= more
            dead -= more
            partial |= path - reached
            kept += more_bytes
            keep += 1
        if self._held_back(damaged):
            return set(), 0, 0
        evicted = len(records) - keep
        self._drop_records([key for key, _ in records[:evicted]])
        deleted, freed = self._delete(dead)
        self._maybe_garbage = deleted != dead  # anything deferred is collected on a later pass
        return deleted, freed, evicted

    # Completeness

    def dag_blocks(self, cid: Cid) -> frozenset[Cid]:
        """The blocks of cid's DAG, which must be completely stored and valid (memoised).

        Raises NotFound if a block is missing, InvalidDag if the DAG breaks a reader check. A missing block is
        remembered (_missing), so asking again while it is still missing walks nothing.
        """
        return self._complete_dag(cid)[0]

    def dag_entries(self, cid: Cid) -> frozenset[Cid]:
        """The CIDs directory entries name anywhere in cid's DAG, which must be completely stored and valid: every
        file and folder below it (not chunks, file nodes or index nodes); none for a file. Found by dag_blocks' walk
        and memoised with it, so a DAG dag_blocks walked isn't read again. Raises as dag_blocks."""
        return self._complete_dag(cid)[1]

    def _complete_dag(self, cid: Cid) -> tuple[frozenset[Cid], frozenset[Cid]]:
        """(dag_blocks, dag_entries) of cid: the DAG walked once, every occurrence of each block checked, and
        memoised unless a block was deleted meanwhile."""
        with self._lock:
            if cid in self._complete:
                return self._complete[cid], self._complete_entries[cid]
            missing = self._missing.get(cid)
            if missing is not None:
                if missing not in self._sizes:
                    raise NotFound(f"block {missing} is not stored")
                del self._missing[cid]  # stored since: walk again
            generation = self._generation
        found: set[Cid] = set()
        entries: set[Cid] = set()  # what the directory nodes met list (each directory's entries, through its index)
        # The DAG node expectations checked. Raw blocks' aren't kept: checking one again (its stored length against
        # the expectation) costs no more than looking it up, and they are most of a large DAG's blocks
        seen: set[walker.Expect] = set()
        stack = [walker.root_expect(cid)]
        while stack:
            expect = stack.pop()
            if expect.cid.is_raw:
                length = self.block_size(expect.cid)
                if length is None:
                    self._lacks(cid, expect.cid)
                walker.check_raw(expect, length)  # stored blocks were verified, so their length is enough
            else:
                if expect in seen:
                    continue
                seen.add(expect)
                block = self.get(expect.cid)
                if block is None:
                    self._lacks(cid, expect.cid)
                node = walker.decode_checked(expect, block)  # a DAG node, as its CID isn't raw
                if isinstance(node, DirectoryNode):
                    entries.update(entry.cid for entry in node.entries)
                stack.extend(walker.children(expect, node))
            found.add(expect.cid)
        result = frozenset(found), frozenset(entries) if entries else _NO_ENTRIES
        with self._lock:
            # A deletion during the walk may have removed a block already counted
            if self._generation == generation:
                self._complete[cid], self._complete_entries[cid] = result
            else:
                gone = next((block for block in result[0] if block not in self._sizes), None)
                if gone is not None:
                    self._lacks(cid, gone, f"a block of {cid} was deleted")
        return result

    def _lacks(self, root: Cid, block: Cid, message: str | None = None):
        """Remember that block, part of root's DAG, isn't stored (_missing), and raise NotFound (with message,
        if given)."""
        with self._lock:
            if block not in self._sizes:  # else stored meanwhile: nothing to remember
                if root not in self._missing and len(self._missing) >= MISSING_MEMO:
                    del self._missing[next(iter(self._missing))]  # the oldest
                self._missing[root] = block
        raise NotFound(message or f"block {block} is not stored")

    def collect(self) -> int:
        """Delete every block nothing reaches (what an add or import refused left behind); how many."""
        with self._lock:
            roots = self._roots() | set(self._cache.values())
            paths = [key for key in self._cache if key[1]]
        self._warm(roots, paths)
        with self._lock:
            deleted, _ = self._collect()
            return len(deleted)

    def denied_roots(self, roots, denied: frozenset[bytes]) -> set[Cid]:
        """Those of roots that are, or reach through stored DAG nodes, a block whose binary CID is in denied."""
        roots = set(roots)
        if not denied or not roots:
            return set()
        self._warm(roots, [])
        with self._lock:
            return {root for root in roots
                    if root.binary in denied or any(cid.binary in denied for cid in self._reach([root]))}

    def drop_denied(self, denied: frozenset[bytes]) -> dict:
        """Drop every cached fetch whose root, path or target's stored DAG holds a block whose binary CID is in
        denied, then delete every block nothing reaches, as gc() does for every fetch.

        Returns {"uncached": set[Cid] targets no longer cached, "removed_blocks", "freed_bytes",
        "incomplete": set[Cid] no longer complete}.
        """
        with self._lock:
            records = dict(self._cache)
            targets = self._targets()
        if not denied or not records:
            return {"uncached": set(), "removed_blocks": 0, "freed_bytes": 0, "incomplete": set()}
        stats = {}

        def act(before: dict[Cid, frozenset[Cid]]) -> set[Cid]:
            """Drop the records reaching a denied block and collect; the blocks deleted."""
            self._warm(set(records.values()) | {root for root, _ in records}, [key for key in records if key[1]])
            with self._lock:
                self._add_memoised(before)
                dropped = [key for key, target in self._cache.items()
                           if any(cid.binary in denied
                                  for cid in {key[0], target} | self._path_cids(*key) | self._reach([target]))]
                uncached = set(self._drop_records(dropped)) - set(self._cache.values())
                deleted, freed = self._collect() if dropped else (set(), 0)
                stats.update(uncached=uncached, removed_blocks=len(deleted), freed_bytes=freed)
                return deleted

        stats["incomplete"] = self._newly_incomplete(targets, act)
        return stats

    def uncache(self, cids: set[Cid]) -> dict:
        """Drop every cached fetch leading to one of cids (cache rm), then delete every block nothing reaches, as
        gc() does for every fetch. A pin of the same content keeps its blocks.

        Returns {"uncached": set[Cid] targets no longer cached, "removed_records", "removed_blocks", "freed_bytes"
        (the records' own blocks), "unreferenced_blocks", "unreferenced_bytes" (blocks nothing reached before, an
        unpinned add's say, collected with them), "incomplete": set[Cid] no longer complete}.
        """
        with self._lock:
            records = {key: target for key, target in self._cache.items() if target in cids}
            targets = self._targets()
        if not records:
            return {"uncached": set(), "removed_records": 0, "removed_blocks": 0, "freed_bytes": 0,
                    "unreferenced_blocks": 0, "unreferenced_bytes": 0, "incomplete": set()}
        stats = {}

        def act(before: dict[Cid, frozenset[Cid]]) -> set[Cid]:
            """Drop the records still leading to one of cids and collect; the blocks deleted."""
            with self._lock:
                rest = dict(self._cache)
                roots = self._roots()
            # What stays is walked without the lock first, as in gc(), so the collection under it reads little
            self._warm(roots | {target for target in rest.values() if target not in cids},
                       [key for key, target in rest.items() if key[1] and target not in cids])
            with self._lock:
                self._add_memoised(before)
                # Blocks nothing reached before the records were dropped are collected with them, but aren't theirs:
                # counted apart, or the content just unpinned was reported as the first CID's own blocks
                orphans = {cid: self._sizes[cid] for cid in self._sizes.keys() - self._live()}
                dropped = [key for key, target in self._cache.items() if target in cids]
                uncached = set(self._drop_records(dropped)) - set(self._cache.values())
                deleted, freed = self._collect() if dropped else (set(), 0)
                stray = deleted & orphans.keys()
                stray_bytes = sum(orphans[cid] for cid in stray)
                stats.update(uncached=uncached, removed_records=len(dropped), removed_blocks=len(deleted) - len(stray),
                             freed_bytes=freed - stray_bytes, unreferenced_blocks=len(stray),
                             unreferenced_bytes=stray_bytes)
                return deleted

        stats["incomplete"] = self._newly_incomplete(targets, act)
        return stats

    def is_complete(self, cid: Cid) -> bool:
        """True if every block of cid's DAG is stored and the DAG is valid."""
        try:
            self.dag_blocks(cid)
            return True
        except (NotFound, InvalidDag):
            return False

    def _newly_incomplete(self, targets: set[Cid], act: Callable[[dict[Cid, frozenset[Cid]]], set[Cid]]) -> set[Cid]:
        """Those of targets complete before act and not after: the one sequence for every operation that deletes
        blocks a pin or cached fetch may need (eviction, gc, drop_denied, verify), so the node can withdraw them.

        Which targets are complete is found first, without the lock (dag_blocks walks their DAGs, reading blocks);
        then act(before) runs, deletes blocks and returns those it deleted. An act that deletes under one hold of
        the lock adds the targets memoised as complete by then (_add_memoised(before)) just before deleting.
        """
        before = self._complete_sets(targets)
        deleted = act(before)
        return {cid for cid, blocks in before.items() if blocks & deleted}

    def _complete_sets(self, cids) -> dict[Cid, frozenset[Cid]]:
        """dag_blocks for those of cids that are complete."""
        result = {}
        for cid in cids:
            try:
                result[cid] = self.dag_blocks(cid)
            except (NotFound, InvalidDag):
                pass
        return result

    def _targets(self) -> set[Cid]:
        """Everything the node might be providing: pins and cached targets (lock held)."""
        return set(self._pins) | set(self._cache.values())

    # Space, eviction and GC

    def pinned_bytes(self) -> int:
        """Bytes of stored blocks reachable from pins."""
        with self._lock:
            pins = set(self._pins)
        self._warm(pins, [])  # the DAG nodes are read without the lock, as in gc()
        with self._lock:
            return sum(self._sizes[cid] for cid in self._reach(self._pins))

    def check_disk(self, nbytes: int):
        """Raise StorageFull if storing nbytes more would leave less than the disk reserve free on the
        filesystem holding the blocks: max(DISK_RESERVE_MIN, DISK_RESERVE_FRACTION of it).

        This, not max_storage, is what bounds a fetch: a full disk breaks every write, not just rnfs's.
        """
        if nbytes <= 0:
            return
        try:
            usage = shutil.disk_usage(self.blocks.directory)
        except OSError:
            return  # can't tell; writing the blocks reports a real failure
        reserve = disk_reserve(usage.total)
        if nbytes > usage.free - reserve:
            # No path: the message can travel beyond the local user (job errors, companion pages relaying daemon errors)
            raise StorageFull(f"not enough free disk space: {size(nbytes)} more would leave less than the "
                              f"{size(reserve)} reserve free ({size(usage.free)} free)")

    def evict(self, needed: int = 0) -> set[Cid]:
        """Enforce max_storage over what the blocks take on disk (allocated_bytes), leaving room for needed more
        bytes: delete unreached blocks, then cached fetches least recently used first. Evicts a margin more, so it
        needn't run again at once.

        Returns pins and cached targets that were complete and no longer are, for withdrawal.
        """
        def wanted() -> bool:
            """Whether eviction has anything to do (lock held)."""
            # With nothing evictable, don't walk everything to find that out again
            return self._allocated + needed > self.max_storage and (bool(self._cache) or self._maybe_garbage)

        with self._lock:
            if not wanted():
                return set()
            targets = self._targets()
            roots = self._roots() | set(self._cache.values())
            paths = [key for key in self._cache if key[1]]

        def act(before: dict[Cid, frozenset[Cid]]) -> set[Cid]:
            """Evict; the blocks deleted."""
            self._warm(roots, paths)  # the long part, reading DAG nodes from disk, runs without the lock
            with self._lock:
                if not wanted():
                    return set()  # another eviction did it meanwhile
                self._add_memoised(before)
                limit = self.max_storage - needed - self.max_storage // EVICT_MARGIN
                deleted, freed, evicted = self._evict_to(max(0, limit))
                if deleted:
                    log(f"storage: evicted {plural(evicted, 'cached fetch')}, freeing {size(freed)} "
                        f"in {plural(len(deleted), 'block')}", LOG_VERBOSE)
                return deleted

        return self._newly_incomplete(targets, act)

    def _roots(self) -> set[Cid]:
        """What pins and sessions hold (lock held)."""
        roots = set(self._pins)
        for session in self._sessions:
            roots |= session.cids
        return roots

    def _add_memoised(self, before: dict[Cid, frozenset[Cid]]):
        """Add to before (complete DAGs found without the lock) the targets memoised as complete now, which a
        deletion about to happen may make incomplete (lock held)."""
        for cid in self._targets():
            if cid in self._complete:
                before[cid] = self._complete[cid]

    def gc(self) -> dict:
        """Evict every cached fetch and delete every block no pin or session reaches.

        Returns {"removed_blocks", "freed_bytes", "incomplete": set[Cid] no longer complete,
        "uncached": set[Cid] targets of the evicted fetches}.
        """
        with self._lock:
            targets = self._targets()
            roots = self._roots()
        stats = {}

        def act(before: dict[Cid, frozenset[Cid]]) -> set[Cid]:
            """Drop every record and collect; the blocks deleted."""
            # The long part without the lock, as in evict(); every cached fetch goes, so only pins and sessions count
            self._warm(roots, [])
            with self._lock:
                self._add_memoised(before)
                stats["uncached"] = set(self._drop_records(list(self._cache)))
                deleted, freed = self._collect()
                stats.update(removed_blocks=len(deleted), freed_bytes=freed)
                return deleted

        incomplete = self._newly_incomplete(targets, act)
        return {"removed_blocks": stats["removed_blocks"], "freed_bytes": stats["freed_bytes"],
                "incomplete": incomplete, "uncached": stats["uncached"]}

    def gc_preview(self) -> dict:
        """What gc() would delete now, deleting nothing, and what the stored blocks are kept for (0.2.1: POST /api/gc
        with dry_run): {"removed_blocks", "freed_bytes" (block bytes, as gc() counts them), "allocated_bytes" (what
        those take on disk), "held_back" (a damaged DAG node something reaches: gc would delete nothing),
        "cached_fetches" (the records gc would evict), "pinned_bytes" (blocks a pin reaches), "cached_bytes" (blocks
        only a cached fetch reaches), "protected_bytes" (blocks only an operation in progress holds: a fetch, an
        add, a job) and "unreferenced_bytes" (blocks nothing reaches)}. Walks every pin's DAG, as gc does: the nodes
        are read without the lock first (_warm)."""
        with self._lock:
            roots = self._roots() | set(self._cache.values())
        self._warm(roots, [])
        with self._lock:
            damaged: set[Cid] = set()
            pinned = self._reach(self._pins, damaged=damaged)
            held = pinned | self._reach(self._roots(), known=pinned, damaged=damaged)
            live = self._live(damaged)
            dead = self._sizes.keys() - held
            held_back = bool(damaged)
            removed = set() if held_back else dead

            def total(cids) -> int:
                """The block bytes of cids."""
                return sum(self._sizes[cid] for cid in cids if cid in self._sizes)

            return {"removed_blocks": len(removed), "freed_bytes": total(removed),
                    "allocated_bytes": sum(self._allocation(self._sizes[cid]) for cid in removed),
                    "held_back": held_back, "cached_fetches": len(self._cache),
                    "pinned_bytes": total(pinned), "cached_bytes": total(live - held),
                    "protected_bytes": total(held - pinned), "unreferenced_bytes": total(self._sizes.keys() - live)}

    def verify(self) -> dict:
        """Re-check every stored block against its CID, deleting bad ones.

        Returns {"checked", "bad": [Cid], "incomplete": set[Cid] pins and cached targets no longer complete}.
        """
        with self._lock:
            cids = list(self._sizes)
            targets = self._targets()
        bad = []

        def act(before: dict[Cid, frozenset[Cid]]) -> set[Cid]:
            """Re-check every block, deleting bad ones, each under its own hold of the lock; the bad ones."""
            for cid in cids:
                if self.blocks.verify(cid):
                    continue
                with self._lock:
                    # Checked again under the lock: it may have been deleted (or rewritten) meanwhile
                    problem = self._block_problem(cid) if cid in self._sizes else None
                    if problem is not None:
                        if problem.startswith(UNREADABLE):  # why, so a disk error or a file's owner can be told
                            log(f"storage: block {cid} {problem}", LOG_ERROR)
                        self._delete({cid})  # reported either way; one Windows defers stays until a later try
                        bad.append(cid)
            return set(bad)

        incomplete = self._newly_incomplete(targets, act)  # completeness without the lock, as in evict()
        if bad:
            log(f"storage: verify removed {plural(len(bad), 'damaged block')}", LOG_ERROR)
        return {"checked": len(cids), "bad": bad, "incomplete": incomplete}

    # Sizes, for stat and the remote-pin quota

    def content_size(self, cid: Cid) -> int | None:
        """Content bytes of cid's DAG as its root block states it, or None if the root isn't stored or valid.

        Readers verify sizes as blocks arrive, so a DAG that lies about its size is invalid and fails.
        """
        block = self.get(cid)
        if block is None:
            return None
        try:
            return walker.size_of(walker.root_expect(cid), block)
        except InvalidDag:
            return None

    def dag_bytes(self, cid: Cid) -> int:
        """Bytes of the stored blocks of cid's DAG (each block once), as far as its stored nodes reach."""
        with self._lock:
            # A DAG memoised as complete is all stored: its total, summed once (_delete forgets it with the DAG)
            if cid in self._complete:
                if cid not in self._complete_bytes:
                    self._complete_bytes[cid] = sum(map(self._sizes.__getitem__, self._complete[cid]))
                return self._complete_bytes[cid]
        self._warm({cid}, [])  # the DAG nodes are read without the lock, as in gc()
        with self._lock:
            return sum(self._sizes[block] for block in self._reach({cid}))

    def source_usage(self, source: str) -> int:
        """What every CID source pins counts against its quota: the bytes of its DAG's blocks, or its content
        size while that is larger (not fetched yet); 0 for those whose root isn't stored."""
        return sum(max(self.content_size(cid) or 0, self.dag_bytes(cid)) for cid in self.pinned_by(source))

    def stat(self, cid: Cid) -> dict:
        """{"size", "blocks", "local_blocks", "local_bytes"} for cid's DAG, as far as its stored nodes show it.

        "blocks" counts distinct blocks known from stored nodes, so it is exact only once all DAG nodes are
        stored; "size" is None until the root is.
        """
        self._warm({cid}, [])  # the DAG nodes are read without the lock, as in gc()
        with self._lock:
            known = {cid}
            stack = [cid]
            while stack:
                for child in self._children(stack.pop()) or ():
                    if child not in known:
                        known.add(child)
                        stack.append(child)
            local = [c for c in known if c in self._sizes]
            local_bytes = sum(self._sizes[c] for c in local)
        return {"size": self.content_size(cid), "blocks": len(known), "local_blocks": len(local),
                "local_bytes": local_bytes}

