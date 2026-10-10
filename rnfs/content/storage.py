"""Storage: blocks, pins, cached fetches, protection and eviction (specification section 8). No RNS dependency.

- Pins are recursive and held by sources: "user", "folder", "name:<alias>", "remote:<identity hash>". A pin
  lasts while any source holds it. Pins are never evicted.
- Cache is everything else. Each cached fetch is recorded as root + path + target, which keeps the path's
  nodes and the target's DAG; fetches are evicted least recently used first when storage exceeds
  max_storage, and gc() evicts them all.
- Blocks of fetches, adds and imports in progress are protected as if pinned, by a protection session.
- A block is deleted only if no pin, session or remaining cached fetch reaches it.

Pins and cache records are persisted as JSON under state_dir. Thread-safe: one lock guards the indexes;
long walks (completeness) run outside it and are discarded if blocks were deleted meanwhile.
"""
import json
import os
import re
import tempfile
import threading
from collections import OrderedDict
from contextlib import contextmanager
from typing import Iterator

from rnfs.constants import MAX_BLOCK_SIZE
from rnfs.content import dag, walker
from rnfs.content.blockstore import BlockStore
from rnfs.content.cid import Cid
from rnfs.content.dag import DirectoryNode, IndexNode
from rnfs.errors import InvalidDag, NotFound, StorageFull
from rnfs.log import LOG_ERROR, LOG_VERBOSE, log, plural, size

USER, FOLDER = "user", "folder"
NAME_PREFIX, REMOTE_PREFIX = "name:", "remote:"
PINS_FILE = "pins.json"
CACHE_FILE = "cache.json"

_IDENTITY_HASH = re.compile(r"[0-9a-f]{32}")


def check_source(source: str):
    """Raise ValueError unless source is a valid pin source."""
    if source in (USER, FOLDER):
        return
    if isinstance(source, str) and source.startswith(NAME_PREFIX) and len(source) > len(NAME_PREFIX):
        return
    if isinstance(source, str) and source.startswith(REMOTE_PREFIX) \
            and _IDENTITY_HASH.fullmatch(source[len(REMOTE_PREFIX):]):
        return
    raise ValueError(f"invalid pin source {source!r}")


def name_source(alias: str) -> str:
    """The pin source for a name's alias."""
    return NAME_PREFIX + alias


def remote_source(identity_hash: bytes | str) -> str:
    """The pin source for a remote identity (bytes, or 32 hex characters)."""
    text = identity_hash.hex() if isinstance(identity_hash, (bytes, bytearray)) else identity_hash.lower()
    return REMOTE_PREFIX + text


def write_json(path: str, value):
    """Write value as JSON to path atomically (temporary file, then rename)."""
    directory = os.path.dirname(path)
    fd, partial = tempfile.mkstemp(prefix=".partial-", dir=directory)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(value, f)
        os.replace(partial, path)
    except BaseException:
        try:
            os.remove(partial)
        except FileNotFoundError:
            pass
        raise


def read_json(path: str):
    """The JSON value in path, or None if it doesn't exist. A damaged file is moved aside, not lost."""
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except (ValueError, OSError) as e:
        aside = path + ".damaged"
        os.replace(path, aside)
        log(f"storage: {path} is damaged ({e}); moved to {aside} and starting without it", LOG_ERROR)
        return None


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
        self._storage.put(cid, block)


class Storage:
    """Blocks plus pins, cached fetches, protection and eviction."""

    def __init__(self, blocks_dir: str, state_dir: str, max_storage: int):
        """Open storage, indexing the blocks on disk and loading pins and cache records from state_dir."""
        self._lock = threading.RLock()
        self.blocks = BlockStore(blocks_dir)
        self.state_dir = os.path.expanduser(state_dir)
        os.makedirs(self.state_dir, exist_ok=True)
        self.max_storage = max_storage

        self._sizes: dict[Cid, int] = dict(self.blocks.scan())
        self._used = sum(self._sizes.values())
        self._pins: dict[Cid, set[str]] = {}
        self._cache: OrderedDict[tuple[Cid, tuple[str, ...]], Cid] = OrderedDict()  # oldest first
        self._sessions: list[Session] = []
        self._links: dict[Cid, tuple[Cid, ...]] = {}  # children of stored DAG nodes; CIDs never change content
        self._complete: dict[Cid, frozenset[Cid]] = {}  # dag_blocks memo, complete DAGs only
        self._generation = 0  # incremented whenever blocks are deleted, invalidating walks in progress
        self._load()

    # Persistence

    def _load(self):
        """Load pins and cache records, skipping entries that don't parse."""
        pins = read_json(os.path.join(self.state_dir, PINS_FILE)) or {}
        for text, sources in pins.get("pins", {}).items():
            try:
                cid = Cid.parse(text)
                for source in sources:
                    check_source(source)
                if sources:
                    self._pins[cid] = set(sources)
            except (ValueError, TypeError):
                log(f"storage: ignoring invalid pin record {text!r}", LOG_ERROR)
        cache = read_json(os.path.join(self.state_dir, CACHE_FILE)) or {}
        for record in cache.get("fetches", []):
            try:
                root, names, target = record
                for name in names:
                    dag.check_name(name)
                self._cache[(Cid.parse(root), tuple(names))] = Cid.parse(target)
            except (ValueError, TypeError):
                log(f"storage: ignoring invalid cache record {record!r}", LOG_ERROR)

    def _save_pins(self):
        """Persist pins (called with the lock held)."""
        value = {"pins": {str(cid): sorted(sources) for cid, sources in self._pins.items()}}
        write_json(os.path.join(self.state_dir, PINS_FILE), value)

    def _save_cache(self):
        """Persist cache records, least recently used first (called with the lock held)."""
        value = {"fetches": [[str(root), list(names), str(target)] for (root, names), target in self._cache.items()]}
        write_json(os.path.join(self.state_dir, CACHE_FILE), value)

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
                    self._delete({cid})
            return None
        return block

    def block_size(self, cid: Cid) -> int | None:
        """Length of a stored block, or None."""
        with self._lock:
            return self._sizes.get(cid)

    def put(self, cid: Cid, block: bytes):
        """Store a block after verifying it (atomic). Raises ValueError if it doesn't match or is too large.

        A block stored outside a protection session is cache that no record reaches: the first to go.
        """
        with self._lock:
            if cid in self._sizes:
                return
        self.blocks.put(cid, block)  # verifies; outside the lock, since it writes to disk
        with self._lock:
            if cid not in self._sizes:
                self._sizes[cid] = len(block)
                self._used += len(block)

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

    def record_fetch(self, root: Cid, names: list[str] | tuple[str, ...], target: Cid):
        """Record (or touch, as most recently used) a cached fetch of <root>/<names> leading to target."""
        key = (root, tuple(names))
        with self._lock:
            self._cache[key] = target
            self._cache.move_to_end(key)
            self._save_cache()

    def touch(self, root: Cid, names: list[str] | tuple[str, ...]) -> bool:
        """Mark a cached fetch as used now; False if there is no such record."""
        key = (root, tuple(names))
        with self._lock:
            if key not in self._cache:
                return False
            self._cache.move_to_end(key)
            self._save_cache()
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

    # Reachability

    def _children(self, cid: Cid) -> tuple[Cid, ...]:
        """CIDs listed by a stored DAG node; none for raw, missing or undecodable blocks (lock held)."""
        if cid.is_raw or cid not in self._sizes:
            return ()
        if cid not in self._links:
            block = self.blocks.get(cid)
            try:
                node = dag.decode(block) if block is not None else None
            except InvalidDag:
                node = None
            if node is None:
                listed: tuple[Cid, ...] = ()
            elif isinstance(node, DirectoryNode):
                listed = tuple(entry.cid for entry in node.entries)
            else:
                listed = tuple(child.cid for child in node.children)
            self._links[cid] = listed
        return self._links[cid]

    def _reach(self, roots) -> set[Cid]:
        """Stored blocks reachable from roots through stored DAG nodes (lock held)."""
        reached: set[Cid] = set()
        stack = [cid for cid in roots if cid in self._sizes]
        while stack:
            cid = stack.pop()
            if cid in reached:
                continue
            reached.add(cid)
            stack.extend(child for child in self._children(cid) if child in self._sizes and child not in reached)
        return reached

    def _path_cids(self, root: Cid, names: tuple[str, ...]) -> set[Cid]:
        """The stored nodes along <root>/<names> (not their other children), as far as they're stored (lock held)."""
        found: set[Cid] = set()
        current = root
        for name in names:
            while True:
                if current.is_raw or current not in self._sizes:
                    return found
                found.add(current)
                try:
                    node = dag.decode(self.blocks.get(current) or b"")
                except InvalidDag:
                    return found
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

    def _live(self) -> set[Cid]:
        """Every stored block a pin, a session or a cached fetch reaches (lock held)."""
        roots = set(self._pins)
        for session in self._sessions:
            roots |= session.cids
        roots |= set(self._cache.values())
        live = self._reach(roots)
        for root, names in self._cache:
            live |= self._path_cids(root, names)
        return live

    def _delete(self, cids: set[Cid]) -> int:
        """Delete stored blocks; returns bytes freed (lock held). Invalidates completeness memos."""
        freed = 0
        for cid in cids:
            if cid in self._sizes:
                self.blocks.delete(cid)
                freed += self._sizes.pop(cid)
                self._links.pop(cid, None)
        if cids:
            self._used -= freed
            self._generation += 1
            self._complete = {root: blocks for root, blocks in self._complete.items() if not blocks & cids}
        return freed

    def _collect(self) -> tuple[set[Cid], int]:
        """Delete every stored block nothing reaches; returns (deleted, bytes freed) (lock held)."""
        live = self._live()
        dead = {cid for cid in self._sizes if cid not in live}
        return dead, self._delete(dead)

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

    def check_space(self, pinned: bool):
        """Raise StorageFull if pins alone fill max_storage and this fetch isn't being pinned."""
        if pinned:
            return
        used = self.pinned_bytes()
        if used >= self.max_storage:
            raise StorageFull(f"pins alone use {size(used)} of max_storage ({size(self.max_storage)})")

    def evict(self) -> set[Cid]:
        """Enforce max_storage: delete unreached blocks, then cached fetches least recently used first.

        Returns pins and cached targets that were complete and no longer are, for withdrawal.
        """
        with self._lock:
            if self._used <= self.max_storage:
                return set()
            before = self._complete_sets(self._targets())
            deleted, freed = self._collect()
            evicted = 0
            while self._used > self.max_storage and self._cache:
                self._cache.popitem(last=False)
                evicted += 1
                more, more_freed = self._collect()
                deleted |= more
                freed += more_freed
            if evicted:
                self._save_cache()
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
                self._save_cache()
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
                    self._delete({cid})
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

    def source_usage(self, source: str) -> int:
        """Total content size of every CID source pins, counting those whose size isn't known yet as 0."""
        return sum(self.content_size(cid) or 0 for cid in self.pinned_by(source))

    def stat(self, cid: Cid) -> dict:
        """{"size", "blocks", "local_blocks", "local_bytes"} for cid's DAG, as far as its stored nodes show it.

        "blocks" counts distinct blocks known from stored nodes, so it is exact only once all DAG nodes are
        stored; "size" is None until the root is.
        """
        with self._lock:
            known = {cid}
            stack = [cid]
            while stack:
                for child in self._children(stack.pop()):
                    if child not in known:
                        known.add(child)
                        stack.append(child)
            local = [c for c in known if c in self._sizes]
            local_bytes = sum(self._sizes[c] for c in local)
        return {"size": self.content_size(cid), "blocks": len(known), "local_blocks": len(local),
                "local_bytes": local_bytes}
