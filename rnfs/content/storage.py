"""Storage: blocks, pins, cached fetches, protection and eviction (specification section 8). No RNS dependency.

- Pins are recursive and held by sources: "user", "folder", "name:<alias>", "remote:<identity hash>". A pin
  lasts while any source holds it. Pins are never evicted.
- Cache is everything else. Each cached fetch is recorded as root + path + target, which keeps the path's
  nodes and the target's DAG; fetches are evicted least recently used first when storage exceeds
  max_storage, and gc() evicts them all.
- Blocks of fetches, adds and imports in progress are protected as if pinned, by a protection session.
- A block is deleted only if no pin, session or remaining cached fetch reaches it.
- max_storage is soft: no fetch is refused for it, and storage may exceed it while fetches run or when
  pins alone fill it. The hard bound is the disk: check_disk() refuses blocks that would leave less than
  DISK_RESERVE free on the filesystem holding the blocks.

Pins and cache records are persisted as JSON under state_dir: pins at once, cache records (which change on
every fetch) by save(), which the node calls periodically and at shutdown. Thread-safe: one lock guards the
indexes; long walks (completeness) run outside it and are discarded if blocks were deleted meanwhile.
"""
import json
import os
import re
import shutil
import threading
from collections import OrderedDict
from contextlib import contextmanager
from typing import Callable, Iterator

from rnfs import fsutil
from rnfs.constants import MAX_BLOCK_SIZE, MIB
from rnfs.content import dag, walker
from rnfs.content.blockstore import BlockStore
from rnfs.content.cid import Cid
from rnfs.content.dag import DirectoryNode, IndexNode
from rnfs.errors import InvalidDag, NotFound, StorageFull
from rnfs.log import LOG_ERROR, LOG_VERBOSE, log, plural, size

USER, FOLDER = "user", "folder"
NAME_PREFIX, REMOTE_PREFIX = "name:", "remote:"
LXMF_PREFIX = "lxmf:"
PINS_FILE = "pins.json"
CACHE_FILE = "cache.json"
MAX_CACHE_RECORDS = 10_000  # cached fetches remembered; past this the least recently used record is dropped
EVICT_MARGIN = 32  # eviction frees max_storage / EVICT_MARGIN more than it must, so it runs rarely
DISK_RESERVE_MIN = 256 * MIB  # free disk space fetches always leave on the blocks' filesystem...
DISK_RESERVE_FRACTION = 0.02  # ...or this fraction of the filesystem, if larger

DAMAGED = object()  # Storage._node's answer for a stored DAG node that is unreadable or no longer matches its CID

_IDENTITY_HASH = re.compile(r"[0-9a-f]{32}")


def disk_reserve(total: int) -> int:
    """The free space a filesystem of total bytes must keep: max(DISK_RESERVE_MIN, DISK_RESERVE_FRACTION of it)."""
    return max(DISK_RESERVE_MIN, int(total * DISK_RESERVE_FRACTION))


def check_source(source: str):
    """Raise ValueError unless source is a valid pin source."""
    if source in (USER, FOLDER):
        return
    if isinstance(source, str) and source.startswith(NAME_PREFIX) and len(source) > len(NAME_PREFIX):
        return
    if isinstance(source, str) and source.startswith(REMOTE_PREFIX) \
            and _IDENTITY_HASH.fullmatch(source[len(REMOTE_PREFIX):]):
        return
    # follow:<name>: the latest target of a followed name (rnfs/features/follow.py)
    if isinstance(source, str) and source.startswith("follow:") and _IDENTITY_HASH.fullmatch(source[len("follow:"):]):
        return
    # lxmf:<LXMF address>: links auto-fetched from that sender (rnfs/features/lxmf.py)
    if isinstance(source, str) and source.startswith(LXMF_PREFIX) and _IDENTITY_HASH.fullmatch(source[len(LXMF_PREFIX):]):
        return
    raise ValueError(f"invalid pin source {source!r}")


def name_source(alias: str) -> str:
    """The pin source for a name's alias."""
    return NAME_PREFIX + alias


def remote_source(identity_hash: bytes | str) -> str:
    """The pin source for a remote identity (bytes, or 32 hex characters)."""
    text = identity_hash.hex() if isinstance(identity_hash, (bytes, bytearray)) else identity_hash.lower()
    return REMOTE_PREFIX + text


def write_json(path: str, value, ensure_ascii: bool = True):
    """Write value as JSON to path atomically and durably (temporary file synced to disk, then renamed), readable
    by its owner only. With ensure_ascii False, non-ASCII characters are written as UTF-8 rather than escaped."""
    # backslashreplace: a lone surrogate, which ensure_ascii False leaves as is, is written as its JSON escape
    data = json.dumps(value, ensure_ascii=ensure_ascii).encode("utf-8", "backslashreplace")
    fsutil.write_atomic(path, data, mode=0o600)  # the rename is retried on Windows while read_json has path open


def read_json(path: str, shape: Callable[[object], bool] | None = None):
    """The JSON value in path, or None if it doesn't exist. A damaged file (not JSON, failing shape if given,
    or a directory) is moved aside, not lost, and gives None. Raises OSError if path can't be read: a transient
    or permission error isn't damage, and starting without the file could lose what it records (pins)."""
    try:
        with open(path, encoding="utf-8") as f:
            value = json.load(f)  # an empty file, as a crash can leave, is a ValueError too
    except FileNotFoundError:
        return None
    except ValueError as e:
        problem = str(e)
    except OSError:
        if not os.path.isdir(path):  # Windows reports opening a directory as PermissionError
            raise
        problem = "it is a directory"
    else:
        if shape is None or shape(value):
            return value
        problem = "unexpected structure"
    try:
        aside = _move_aside(path)
    except OSError as moving:
        # A read-only state dir: start without it rather than not at all
        log(f"storage: {path} is damaged ({problem}) and couldn't be moved aside ({moving}); starting without it",
            LOG_ERROR)
        return None
    log(f"storage: {path} is damaged ({problem}); moved to {aside} and starting without it", LOG_ERROR)
    return None


def _move_aside(path: str) -> str:
    """Rename path to path.damaged, or path.damaged.<n> so an earlier damaged copy is kept; the new path."""
    for n in range(1000):
        aside = path + ".damaged" + (f".{n}" if n else "")
        try:
            fsutil.rename_new(path, aside)
            return aside
        except FileExistsError:
            continue
    raise FileExistsError(f"too many damaged copies of {path}")


def shaped(key: str, kind: type) -> Callable[[object], bool]:
    """A read_json shape: an object whose key, if present, holds a kind."""
    return lambda value: isinstance(value, dict) and isinstance(value.get(key, kind()), kind)


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
        """Store a block and protect it. Raises ValueError if it doesn't match its CID."""
        self.add(cid)  # first, so an eviction between storing and protecting can't take it
        self._storage._put(cid, block, protected=True)


class Storage:
    """Blocks plus pins, cached fetches, protection and eviction."""

    def __init__(self, blocks_dir: str, state_dir: str, max_storage: int):
        """Open storage, indexing the blocks on disk and loading pins and cache records from state_dir."""
        self._lock = threading.RLock()
        self.blocks = BlockStore(blocks_dir)
        self.state_dir = os.path.expanduser(state_dir)
        os.makedirs(self.state_dir, mode=0o700, exist_ok=True)
        self.max_storage = max_storage
        self._save_lock = threading.Lock()  # keeps cache saves in order without holding _lock while writing

        self._sizes: dict[Cid, int] = dict(self.blocks.scan())
        self._used = sum(self._sizes.values())
        self._pins: dict[Cid, set[str]] = {}
        self._cache: OrderedDict[tuple[Cid, tuple[str, ...]], Cid] = OrderedDict()  # oldest first
        self._sessions: list[Session] = []
        self._links: dict[Cid, tuple[Cid, ...]] = {}  # children of stored DAG nodes; CIDs never change content
        self._complete: dict[Cid, frozenset[Cid]] = {}  # dag_blocks memo, complete DAGs only
        self._generation = 0  # incremented whenever blocks are deleted, invalidating walks in progress
        self._cache_dirty = False  # cache records changed since the last save()
        # False only when nothing has happened since the last collection that could leave a block unreached,
        # so eviction over max_storage with nothing to evict needn't walk everything again
        self._maybe_garbage = True
        self._reported_damaged: set[Cid] = set()  # damaged DAG nodes already logged as stopping collection
        self._load()

    # Persistence

    def _load(self):
        """Load pins and cache records, skipping entries that don't parse."""
        pins = read_json(os.path.join(self.state_dir, PINS_FILE), shaped("pins", dict)) or {}
        for text, sources in pins.get("pins", {}).items():
            try:
                cid = Cid.parse(text)
                if not isinstance(sources, list):
                    raise TypeError("sources aren't a list")
                for source in sources:
                    check_source(source)
                if sources:
                    self._pins[cid] = set(sources)
            except (ValueError, TypeError):
                log(f"storage: ignoring invalid pin record {text!r}", LOG_ERROR)
        cache = read_json(os.path.join(self.state_dir, CACHE_FILE), shaped("fetches", list)) or {}
        for record in cache.get("fetches", []):
            try:
                root, names, target = record
                if not isinstance(names, list):
                    raise TypeError("names aren't a list")
                for name in names:
                    dag.check_name(name)
                self._cache[(Cid.parse(root), tuple(names))] = Cid.parse(target)
            except (ValueError, TypeError):
                log(f"storage: ignoring invalid cache record {record!r}", LOG_ERROR)
        self._cap_cache()

    def _save_pins(self):
        """Persist pins (called with the lock held)."""
        value = {"pins": {str(cid): sorted(sources) for cid, sources in self._pins.items()}}
        self.blocks.sync()  # so pins saved never outlast the blocks they keep
        write_json(os.path.join(self.state_dir, PINS_FILE), value)

    def save(self):
        """Persist cache records, least recently used first, if they changed since the last save."""
        with self._save_lock:
            with self._lock:
                if not self._cache_dirty:
                    return
                value = {"fetches": [[str(root), list(names), str(target)]
                                     for (root, names), target in self._cache.items()]}
                self._cache_dirty = False
            try:
                self.blocks.sync()
                write_json(os.path.join(self.state_dir, CACHE_FILE), value)
            except BaseException:
                with self._lock:
                    self._cache_dirty = True
                raise

    # Blocks

    def has(self, cid: Cid) -> bool:
        """True if the block is stored."""
        with self._lock:
            return cid in self._sizes

    def get(self, cid: Cid, verify: bool = False) -> bytes | None:
        """The stored block, or None. With verify, a block that no longer matches its CID is deleted, giving None."""
        with self._lock:
            if cid not in self._sizes:
                return None
        block = self.blocks.get(cid)
        if block is None or (verify and not (len(block) <= MAX_BLOCK_SIZE and cid.verify(block))):
            with self._lock:
                if cid in self._sizes and not self.blocks.verify(cid):
                    log(f"storage: block {cid} is damaged or missing; removing it", LOG_ERROR)
                    self._delete({cid})  # if Windows defers it, verify() or the next read tries again
            return None
        return block

    def block_size(self, cid: Cid) -> int | None:
        """Length of a stored block, or None."""
        with self._lock:
            return self._sizes.get(cid)

    def put(self, cid: Cid, block: bytes):
        """Store a block after verifying it (atomic, durable). Raises ValueError if it doesn't match or is too
        large, even when the block is already stored.

        A block stored outside a protection session is cache that no record reaches: the first to go.
        """
        self._put(cid, block, protected=False)

    def _put(self, cid: Cid, block: bytes, protected: bool):
        """Store a block; protected if a session already lists it, so it can't be garbage."""
        with self._lock:
            stored = cid in self._sizes
        if stored:
            # Verified all the same, so callers (archive import) never count unchecked bytes as supplied
            if len(block) > MAX_BLOCK_SIZE or not cid.verify(block):
                raise ValueError(f"block does not match {cid}")
            return
        self.blocks.put(cid, block)  # verifies; outside the lock, since it writes to disk
        with self._lock:
            if cid not in self._sizes:
                self._sizes[cid] = len(block)
                self._used += len(block)
                if not protected:
                    self._maybe_garbage = True

    def used_bytes(self) -> int:
        """Bytes of every stored block."""
        with self._lock:
            return self._used

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
        check_source(source)
        with self._lock:
            sources = self._pins.setdefault(cid, set())
            if source not in sources:
                sources.add(source)
                self._save_pins()

    def unpin(self, cid: Cid, source: str) -> bool:
        """Release source's hold on cid; True if it held it. Blocks go at the next evict() or gc()."""
        with self._lock:
            sources = self._pins.get(cid)
            if sources is None or source not in sources:
                return False
            sources.discard(source)
            if not sources:
                del self._pins[cid]
                self._maybe_garbage = True
            self._save_pins()
            return True

    def pins(self) -> dict[Cid, set[str]]:
        """Every pin and its sources (a copy)."""
        with self._lock:
            return {cid: set(sources) for cid, sources in self._pins.items()}

    def pinned_by(self, source: str) -> set[Cid]:
        """CIDs held by source."""
        with self._lock:
            return {cid for cid, sources in self._pins.items() if source in sources}

    def is_pinned(self, cid: Cid) -> bool:
        """True if any source pins cid."""
        with self._lock:
            return cid in self._pins

    # Cache

    def record_fetch(self, root: Cid, names: list[str] | tuple[str, ...], target: Cid) -> set[Cid]:
        """Record (or touch, as most recently used) a cached fetch of <root>/<names> leading to target.

        Returns the targets of records dropped to stay within MAX_CACHE_RECORDS, no longer cached.
        """
        key = (root, tuple(names))
        with self._lock:
            previous = self._cache.get(key)
            if previous is not None and previous != target:
                self._maybe_garbage = True
            self._cache[key] = target
            self._cache.move_to_end(key)
            self._cache_dirty = True
            dropped = self._cap_cache()
            return dropped - set(self._cache.values())

    def _cap_cache(self) -> set[Cid]:
        """Drop the least recently used records past MAX_CACHE_RECORDS; their targets (lock held)."""
        dropped = set()
        while len(self._cache) > MAX_CACHE_RECORDS:
            dropped.add(self._cache.popitem(last=False)[1])
        if dropped:
            self._maybe_garbage = True
            self._cache_dirty = True
        return dropped

    def touch(self, root: Cid, names: list[str] | tuple[str, ...]) -> bool:
        """Mark a cached fetch as used now; False if there is no such record."""
        key = (root, tuple(names))
        with self._lock:
            if key not in self._cache:
                return False
            self._cache.move_to_end(key)
            self._cache_dirty = True
            return True

    def cached_fetches(self) -> list[tuple[Cid, tuple[str, ...], Cid]]:
        """Every cached fetch as (root, names, target), least recently used first."""
        with self._lock:
            return [(root, names, target) for (root, names), target in self._cache.items()]

    def cached_targets(self) -> set[Cid]:
        """Targets of every cached fetch."""
        with self._lock:
            return set(self._cache.values())

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
        try:
            block = self.blocks.get(cid)
        except OSError:
            block = None
        if block is None or len(block) > MAX_BLOCK_SIZE or not cid.verify(block):
            return DAMAGED
        try:
            return dag.decode(block)
        except InvalidDag:
            return None

    def _children(self, cid: Cid) -> tuple[Cid, ...] | None:
        """CIDs listed by a stored DAG node; none for raw, missing or invalid blocks; None if the node is
        damaged, as its children are unknown (lock held)."""
        if cid not in self._links:
            node = self._node(cid)
            if node is DAMAGED:
                return None  # not memoised: verify() removes it, or a fetch replaces it
            if node is None:
                listed: tuple[Cid, ...] = ()
            elif isinstance(node, DirectoryNode):
                listed = tuple(entry.cid for entry in node.entries)
            else:
                listed = tuple(child.cid for child in node.children)
            self._links[cid] = listed
        return self._links[cid]

    def _reach(self, roots, known: set[Cid] | frozenset[Cid] = frozenset(),
               damaged: set[Cid] | None = None) -> set[Cid]:
        """Stored blocks reachable from roots through stored DAG nodes, not passing through known; damaged
        nodes reached are added to damaged (lock held)."""
        reached: set[Cid] = set()
        stack = [cid for cid in roots if cid in self._sizes and cid not in known]
        while stack:
            cid = stack.pop()
            if cid in reached:
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
        found: set[Cid] = set()
        current = root
        for name in names:
            while True:
                if current.is_raw or current not in self._sizes:
                    return found
                found.add(current)
                node = self._node(current)
                if node is DAMAGED and damaged is not None:
                    damaged.add(current)
                if isinstance(node, DirectoryNode):
                    entry = next((e for e in node.entries if e.name == name), None)
                    if entry is None:
                        return found
                    current = entry.cid
                    break
                if isinstance(node, IndexNode):
                    candidates = [c for c in node.children if c.first_name <= name]
                    if not candidates:
                        return found
                    current = candidates[-1].cid
                    continue
                return found
        return found

    def _live(self, damaged: set[Cid] | None = None) -> set[Cid]:
        """Every stored block a pin, a session or a cached fetch reaches (lock held)."""
        roots = set(self._pins)
        for session in self._sessions:
            roots |= session.cids
        roots |= set(self._cache.values())
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
        matches the disk; it is left out of deleted, and the next collection tries again.
        """
        deleted: set[Cid] = set()
        freed = 0
        deferred = False
        for cid in cids:
            if cid in self._sizes:
                if self.blocks.delete(cid) is None:
                    log(f"storage: block {cid} is in use; deleting it later", LOG_VERBOSE)
                    deferred = True
                    continue
                deleted.add(cid)
                freed += self._sizes.pop(cid)
                self._links.pop(cid, None)
                self._reported_damaged.discard(cid)
        if deleted:
            self._used -= freed
            self._generation += 1
            self._complete = {root: blocks for root, blocks in self._complete.items() if not blocks & deleted}
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
        """Delete unreached blocks, then cached fetches least recently used first, until at most limit bytes
        are stored or nothing evictable is left; returns (deleted, bytes freed, records evicted) (lock held).

        One walk: what pins and sessions reach, then each record's own blocks from the most recently used
        down, keeping records while everything kept fits; the older ones are evicted.
        """
        if not self._cache and not self._maybe_garbage:
            return set(), 0, 0  # only pins and sessions hold blocks: there is nothing to evict
        roots = set(self._pins)
        for session in self._sessions:
            roots |= session.cids
        damaged: set[Cid] = set()
        live = self._reach(roots, damaged=damaged)
        kept = sum(self._sizes[cid] for cid in live)
        records = list(self._cache.items())  # oldest first
        keep = 0
        for (root, names), target in reversed(records):
            more = self._reach({target}, live, damaged) | (self._path_cids(root, names, damaged) - live)
            more_bytes = sum(self._sizes[cid] for cid in more)
            if kept + more_bytes > limit:
                break
            live |= more
            kept += more_bytes
            keep += 1
        if self._held_back(damaged):
            return set(), 0, 0
        evicted = len(records) - keep
        for _ in range(evicted):
            self._cache.popitem(last=False)
        if evicted:
            self._cache_dirty = True
        dead = {cid for cid in self._sizes if cid not in live}
        deleted, freed = self._delete(dead)
        self._maybe_garbage = deleted != dead  # anything deferred is collected on a later pass
        return deleted, freed, evicted

    # Completeness

    def dag_blocks(self, cid: Cid) -> frozenset[Cid]:
        """The blocks of cid's DAG, which must be completely stored and valid (memoised).

        Raises NotFound if a block is missing, InvalidDag if the DAG breaks a reader check.
        """
        with self._lock:
            if cid in self._complete:
                return self._complete[cid]
            generation = self._generation
        found: set[Cid] = set()
        seen: set[walker.Expect] = set()
        stack = [walker.root_expect(cid)]
        while stack:
            expect = stack.pop()
            if expect in seen:
                continue
            seen.add(expect)
            if expect.cid.is_raw:
                length = self.block_size(expect.cid)
                if length is None:
                    raise NotFound(f"block {expect.cid} is not stored")
                walker.check_raw(expect, length)  # stored blocks were verified, so their length is enough
            else:
                block = self.get(expect.cid)
                if block is None:
                    raise NotFound(f"block {expect.cid} is not stored")
                stack.extend(walker.expand(expect, block))
            found.add(expect.cid)
        result = frozenset(found)
        with self._lock:
            # A deletion during the walk may have removed a block already counted
            if self._generation == generation:
                self._complete[cid] = result
            elif not all(block in self._sizes for block in result):
                raise NotFound(f"a block of {cid} was deleted")
        return result

    def is_complete(self, cid: Cid) -> bool:
        """True if every block of cid's DAG is stored and the DAG is valid."""
        try:
            self.dag_blocks(cid)
            return True
        except (NotFound, InvalidDag):
            return False

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
        """Enforce max_storage, leaving room for needed more bytes: delete unreached blocks, then cached
        fetches least recently used first. Evicts a margin more, so it needn't run again at once.

        Returns pins and cached targets that were complete and no longer are, for withdrawal.
        """
        with self._lock:
            if self._used + needed <= self.max_storage:
                return set()
            if not self._cache and not self._maybe_garbage:
                return set()  # nothing is evictable: don't walk everything to find that out again
            before = self._complete_sets(self._targets())
            limit = self.max_storage - needed - self.max_storage // EVICT_MARGIN
            deleted, freed, evicted = self._evict_to(max(0, limit))
            if deleted:
                log(f"storage: evicted {plural(evicted, 'cached fetch')}, freeing {size(freed)} "
                    f"in {plural(len(deleted), 'block')}", LOG_VERBOSE)
            return {cid for cid, blocks in before.items() if blocks & deleted}

    def gc(self) -> dict:
        """Evict every cached fetch and delete every block no pin or session reaches.

        Returns {"removed_blocks", "freed_bytes", "incomplete": set[Cid] no longer complete,
        "uncached": set[Cid] targets of the evicted fetches}.
        """
        with self._lock:
            before = self._complete_sets(self._targets())
            uncached = set(self._cache.values())
            if self._cache:
                self._cache.clear()
                self._cache_dirty = True
            deleted, freed = self._collect()
            incomplete = {cid for cid, blocks in before.items() if blocks & deleted}
            return {"removed_blocks": len(deleted), "freed_bytes": freed, "incomplete": incomplete,
                    "uncached": uncached}

    def verify(self) -> dict:
        """Re-check every stored block against its CID, deleting bad ones.

        Returns {"checked", "bad": [Cid], "incomplete": set[Cid] pins and cached targets no longer complete}.
        """
        with self._lock:
            cids = list(self._sizes)
            before = self._complete_sets(self._targets())
        bad = []
        for cid in cids:
            if self.blocks.verify(cid):
                continue
            with self._lock:
                # Checked again under the lock: it may have been deleted (or rewritten) meanwhile
                if cid in self._sizes and not self.blocks.verify(cid):
                    self._delete({cid})  # reported either way; one Windows defers stays until a later try
                    bad.append(cid)
        if bad:
            log(f"storage: verify removed {plural(len(bad), 'damaged block')}", LOG_ERROR)
        bad_set = set(bad)
        return {"checked": len(cids), "bad": bad,
                "incomplete": {cid for cid, blocks in before.items() if blocks & bad_set}}

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
