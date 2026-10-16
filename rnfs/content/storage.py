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

Pins and cache records are persisted in the state database (rnfs.statedb), and only read from it at startup:
each pin and release at once, as one row inserted or deleted and committed durably (after the blocks it keeps
are synced), and cache records (which change on every fetch) by save(), which the node calls periodically and
at shutdown, writing only the records changed since the last save. Thread-safe: one lock guards the
indexes; long walks run outside it: completeness walks are discarded if blocks were deleted meanwhile, and
eviction, gc and the size queries read the DAG nodes they will walk beforehand (_warm), deciding what to delete
(or counting) under the lock.
"""
import itertools
import os
import re
import shutil
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Callable, Iterator

from rnfs import statedb
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
MAX_CACHE_RECORDS = 10_000  # cached fetches remembered; past this the least recently used record is dropped
EVICT_MARGIN = 32  # eviction frees max_storage / EVICT_MARGIN more than it must, so it runs rarely
DISK_RESERVE_MIN = 256 * MIB  # free disk space fetches always leave on the blocks' filesystem...
DISK_RESERVE_FRACTION = 0.02  # ...or this fraction of the filesystem, if larger
# Blocks a Session.writer() writes at once: each is synced to disk before its rename, and syncs that overlap
# share the filesystem's journal commits (about 9x the speed of one at a time on an SSD)
WRITE_THREADS = 16
WRITE_QUEUE = 32  # blocks a writer holds waiting to be written, at most (2 MiB of 64 KiB chunks)

DAMAGED = object()  # Storage._node's answer for a stored DAG node that is unreadable or no longer matches its CID
MISSING = object()  # Storage._read_node's answer for a block that isn't stored (or was deleted meanwhile)

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


def import_pins(value: dict, tx) -> int:
    """Import pins.json (statedb.migrate): its pins replace the pins table, each record checked as rnfs 0.1.4
    loaded it (an invalid one is logged and skipped). Returns the number of pins imported."""
    tx.execute("DELETE FROM pins")
    count = 0
    for text, sources in value.get("pins", {}).items():
        try:
            cid = Cid.parse(text)
            if not isinstance(sources, list):
                raise TypeError("sources aren't a list")
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


def import_cache(value: dict, tx) -> int:
    """Import cache.json (statedb.migrate): its records, least recently used first, replace the cache table, each
    checked as rnfs 0.1.4 loaded it. Returns the number of records imported."""
    tx.execute("DELETE FROM cache")
    records: OrderedDict[tuple[Cid, tuple[str, ...]], Cid] = OrderedDict()
    for record in value.get("fetches", []):
        try:
            root, names, target = record
            if not isinstance(names, list):
                raise TypeError("names aren't a list")
            for name in names:
                dag.check_name(name)
            records[(Cid.parse(root), tuple(names))] = Cid.parse(target)  # a repeated key keeps its first place
        except (ValueError, TypeError):
            log(f"storage: ignoring invalid cache record {record!r}", LOG_ERROR)
    tx.executemany("INSERT INTO cache (root, path, target, used) VALUES (?, ?, ?, ?)",
                   [(root.binary, statedb.dump(list(names)), target.binary, used)
                    for used, ((root, names), target) in enumerate(records.items())])
    return len(records)


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
        except BaseException as e:
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
        self.blocks = BlockStore(blocks_dir)
        self.state_dir = os.path.expanduser(state_dir)
        self._db = statedb.connect(self.state_dir)
        self.max_storage = max_storage
        self._save_lock = threading.Lock()  # keeps cache saves in order without holding _lock while writing

        self._sizes: dict[Cid, int] = dict(self.blocks.scan())
        self._used = sum(self._sizes.values())
        self._pins: dict[Cid, set[str]] = {}
        self._cache: OrderedDict[tuple[Cid, tuple[str, ...]], Cid] = OrderedDict()  # oldest first
        self._sessions: list[Session] = []
        self._links: dict[Cid, tuple[Cid, ...]] = {}  # children of stored DAG nodes; CIDs never change content
        # Where a path step leads from a stored DAG node: node -> {name: _step_in's answer}; memoised like _links
        self._steps: dict[Cid, dict[str, tuple[Cid, bool] | None]] = {}
        self._complete: dict[Cid, frozenset[Cid]] = {}  # dag_blocks memo, complete DAGs only
        self._generation = 0  # incremented whenever blocks are deleted, invalidating walks in progress
        self._cache_generation = 0  # incremented whenever cache records are removed or retargeted
        # Cache records changed (or removed) since the last save(); every one is among the most recently used
        self._cache_dirty: set[tuple[Cid, tuple[str, ...]]] = set()
        self._cache_used = 0  # the cache table's next used value: larger than every one saved
        # False only when nothing has happened since the last collection that could leave a block unreached,
        # so eviction over max_storage with nothing to evict needn't walk everything again
        self._maybe_garbage = True
        self._reported_damaged: set[Cid] = set()  # damaged DAG nodes already logged as stopping collection
        try:
            self._load()
        except BaseException:
            self._db.close()
            raise

    # Persistence

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
        bad_cache = []
        for root, path, target, used in self._db.query("SELECT root, path, target, used FROM cache ORDER BY used"):
            try:
                names = statedb.load(path)
                if not isinstance(names, list):
                    raise TypeError("names aren't a list")
                for name in names:
                    dag.check_name(name)
                key = (Cid.from_bytes(root), tuple(names))
                self._cache[key] = Cid.from_bytes(target)
            except (ValueError, TypeError):
                log(f"storage: ignoring invalid cache record {root!r} {path!r}", LOG_ERROR)
                bad_cache.append((root, path))
                continue
            if isinstance(used, int):
                self._cache_used = max(self._cache_used, used + 1)
        capped = [(root.binary, statedb.dump(list(names))) for root, names in
                  list(self._cache)[:max(0, len(self._cache) - MAX_CACHE_RECORDS)]]
        self._cap_cache()
        self._cache_dirty.clear()
        if bad_pins or bad_cache or capped:
            with self._db.transaction() as tx:
                tx.executemany("DELETE FROM pins WHERE cid = ? AND source = ?", bad_pins)
                tx.executemany("DELETE FROM cache WHERE root = ? AND path = ?", bad_cache + capped)

    def _save_pins(self, source: str, added=(), removed=()):
        """Persist source's new pins of added and releases of removed, durably (called with the lock held)."""
        self.blocks.sync()  # so pins saved never outlast the blocks they keep
        with self._db.transaction(durable=True) as tx:
            tx.executemany("INSERT OR IGNORE INTO pins (cid, source) VALUES (?, ?)",
                           [(cid.binary, source) for cid in added])
            tx.executemany("DELETE FROM pins WHERE cid = ? AND source = ?", [(cid.binary, source) for cid in removed])

    def save(self):
        """Persist the cache records changed since the last save, least recently used first."""
        with self._save_lock:
            with self._lock:
                if not self._cache_dirty:
                    return
                dirty, self._cache_dirty = self._cache_dirty, set()
                # The records changed since the last save are the most recently used (each change moved its
                # record to the end), so those still cached are the last ones, in order
                kept = sum(1 for key in dirty if key in self._cache)
                changed = list(itertools.islice(reversed(self._cache.items()), kept))[::-1]
                rows = [(root.binary, statedb.dump(list(names)), target.binary, self._cache_used + n)
                        for n, ((root, names), target) in enumerate(changed)]
                self._cache_used += len(rows)
                gone = [(root.binary, statedb.dump(list(names))) for root, names in dirty if
                        (root, names) not in self._cache]
            try:
                self.blocks.sync()
                with self._db.transaction() as tx:
                    tx.executemany("DELETE FROM cache WHERE root = ? AND path = ?", gone)
                    tx.executemany("INSERT OR REPLACE INTO cache (root, path, target, used) VALUES (?, ?, ?, ?)",
                                   rows)
            except BaseException:
                with self._lock:
                    self._cache_dirty |= dirty
                raise

    def close(self):
        """Release the state database (the node calls this once it has saved everything at shutdown)."""
        self._db.close()

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
            try:
                self._save_pins(source, added=added)
            except BaseException:
                # Not saved (a full or failing disk): not pinned, so memory says what the database does
                for cid in added:
                    sources = self._pins[cid]
                    sources.discard(source)
                    if not sources:
                        del self._pins[cid]
                raise

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
            try:
                self._save_pins(source, removed=[cid])
            except BaseException:
                self._pins.setdefault(cid, set()).add(source)  # still pinned, as the database says
                raise
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

    def pin_count(self) -> int:
        """Number of pinned CIDs."""
        with self._lock:
            return len(self._pins)

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
                self._cache_generation += 1
            self._cache[key] = target
            self._cache.move_to_end(key)
            self._cache_dirty.add(key)
            dropped = self._cap_cache()
            return dropped - set(self._cache.values())

    def _cap_cache(self) -> set[Cid]:
        """Drop the least recently used records past MAX_CACHE_RECORDS; their targets (lock held)."""
        dropped = set()
        while len(self._cache) > MAX_CACHE_RECORDS:
            key, target = self._cache.popitem(last=False)
            dropped.add(target)
            self._cache_dirty.add(key)
        if dropped:
            self._maybe_garbage = True
            self._cache_generation += 1
        return dropped

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
        if len(block) > MAX_BLOCK_SIZE or not cid.verify(block):
            return DAMAGED
        try:
            return dag.decode(block)
        except InvalidDag:
            return None

    @staticmethod
    def _listed(node) -> tuple[Cid, ...]:
        """The CIDs a decoded DAG node lists; none for None (raw, missing or invalid)."""
        if node is None:
            return ()
        if isinstance(node, DirectoryNode):
            return tuple(entry.cid for entry in node.entries)
        return tuple(child.cid for child in node.children)

    @staticmethod
    def _step_in(node, name: str) -> tuple[Cid, bool] | None:
        """Where the path step name leads from a decoded node: (entry, True) through a directory, (child, False)
        to the index node child that may hold it, or None (no such entry, or not a directory)."""
        if isinstance(node, DirectoryNode):
            entry = next((e for e in node.entries if e.name == name), None)
            return None if entry is None else (entry.cid, True)
        if isinstance(node, IndexNode):
            candidates = [c for c in node.children if c.first_name <= name]
            return (candidates[-1].cid, False) if candidates else None
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
                step = self._steps.get(current, {}).get(name, MISSING)
                if step is MISSING:
                    node = self._node(current)
                    if node is DAMAGED:
                        if damaged is not None:
                            damaged.add(current)
                        return found  # not memoised, as in _children
                    step = self._steps.setdefault(current, {})[name] = self._step_in(node, name)
                if step is None:
                    return found
                current, through_directory = step
                if through_directory:
                    break  # the next name
        return found

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
        found_links: dict[Cid, tuple[Cid, ...]] = {}
        found_steps: dict[tuple[Cid, str], tuple[Cid, bool] | None] = {}
        seen: set[Cid] = set()
        stack = list(roots)
        while stack:
            cid = stack.pop()
            if cid in seen or cid.is_raw:
                continue
            seen.add(cid)
            listed = links.get(cid)
            if listed is None:
                node = self._read_node(cid)
                if node is MISSING or node is DAMAGED:
                    continue
                listed = found_links[cid] = self._listed(node)
            stack.extend(child for child in listed if child not in seen and not child.is_raw)

        def step(cid: Cid, name: str):
            """_step_in's answer for cid, memoised or read; MISSING if cid can't be read."""
            known = steps.get(cid, {}).get(name, found_steps.get((cid, name), MISSING))
            if known is MISSING:
                node = self._read_node(cid)
                if node is MISSING or node is DAMAGED:
                    return MISSING
                known = found_steps[(cid, name)] = self._step_in(node, name)
            return known

        for root, names in paths:
            current, ended = root, False
            for name in names:
                while not ended:
                    result = None if current.is_raw else step(current, name)
                    if result is None or result is MISSING:
                        ended = True  # the path ends here, or the walk under the lock reads it
                    else:
                        current, through_directory = result
                        if through_directory:
                            break  # the next name
                if ended:
                    break
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
                self._steps.pop(cid, None)
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
        damaged: set[Cid] = set()
        live = self._reach(self._roots(), damaged=damaged)
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
            self._cache_dirty.add(self._cache.popitem(last=False)[0])
        if evicted:
            self._cache_generation += 1
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
        """Enforce max_storage, leaving room for needed more bytes: delete unreached blocks, then cached
        fetches least recently used first. Evicts a margin more, so it needn't run again at once.

        Returns pins and cached targets that were complete and no longer are, for withdrawal.
        """
        def wanted() -> bool:
            """Whether eviction has anything to do (lock held)."""
            # With nothing evictable, don't walk everything to find that out again
            return self._used + needed > self.max_storage and (bool(self._cache) or self._maybe_garbage)

        with self._lock:
            if not wanted():
                return set()
            targets = self._targets()
            roots = self._roots() | set(self._cache.values())
            paths = [key for key in self._cache if key[1]]
        # The long part, reading DAG nodes from disk, runs without the lock (_warm)
        before = self._complete_sets(targets)
        self._warm(roots, paths)
        with self._lock:
            if not wanted():
                return set()  # another eviction did it meanwhile
            before = self._with_memoised(before)
            limit = self.max_storage - needed - self.max_storage // EVICT_MARGIN
            deleted, freed, evicted = self._evict_to(max(0, limit))
            if deleted:
                log(f"storage: evicted {plural(evicted, 'cached fetch')}, freeing {size(freed)} "
                    f"in {plural(len(deleted), 'block')}", LOG_VERBOSE)
            return {cid for cid, blocks in before.items() if blocks & deleted}

    def _roots(self) -> set[Cid]:
        """What pins and sessions hold (lock held)."""
        roots = set(self._pins)
        for session in self._sessions:
            roots |= session.cids
        return roots

    def _with_memoised(self, before: dict[Cid, frozenset[Cid]]) -> dict[Cid, frozenset[Cid]]:
        """before (complete DAGs found without the lock) plus the targets memoised as complete now, which a
        deletion about to happen may make incomplete (lock held)."""
        result = dict(before)
        for cid in self._targets():
            if cid in self._complete:
                result[cid] = self._complete[cid]
        return result

    def gc(self) -> dict:
        """Evict every cached fetch and delete every block no pin or session reaches.

        Returns {"removed_blocks", "freed_bytes", "incomplete": set[Cid] no longer complete,
        "uncached": set[Cid] targets of the evicted fetches}.
        """
        with self._lock:
            targets = self._targets()
            roots = self._roots()
        # The long part without the lock, as in evict(); every cached fetch goes, so only pins and sessions count
        before = self._complete_sets(targets)
        self._warm(roots, [])
        with self._lock:
            before = self._with_memoised(before)
            uncached = set(self._cache.values())
            if self._cache:
                self._cache_dirty.update(self._cache)
                self._cache.clear()
                self._cache_generation += 1
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
            targets = self._targets()
        before = self._complete_sets(targets)  # without the lock, as in evict(); dag_blocks checks for deletions
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

