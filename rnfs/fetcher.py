"""Fetching a target from providers (specification sections 6 and 7).

A Fetch is one fetch of `<root>/<path>`, used as a context manager that holds a storage protection session,
so every block it stores is kept until it ends and an interrupted fetch resumes where it stopped. The node
drives it step by step: resolve() fetches the root block and the path, then fetch_dag(), read_range() or
fetch_listing() fetch what the operation needs. Blocks already stored are never fetched again, and each
CID is fetched once however often it appears.

Providers, in the order they are tried:
1. up to max_parallel_providers nodes with an open outgoing link, asked for the first missing block;
2. if none has it, providers found by a Lookup running in the background, so downloading starts with
   the first one found; the search ends once max_parallel_providers providers are known and the root
   block has arrived, when the lookup is complete, or at lookup_timeout. Providers in the routing table
   for over provider_seniority are tried first, the rest after them; within each group, fewer hops first,
   then a faster next-hop interface (rnfs.mesh.provider_rank), then reply order. Open links are ranked
   the same way. Hops and bitrates come from path_info (the node passes RNS's; by default none are known).
`get --from` uses only the given node.

A provider fails, and isn't used again during the fetch, when its link fails, it answers None for a block
of the root it advertised, it is busy max_failures times in a row (each retried after BUSY_DELAY), or it
stalls twice. When no providers are left, the lookup runs again for a fresh random half of providers,
within the same lookup_timeout. A block not matching its CID bans the provider; a correctly hashed block
that isn't a valid DAG node marks the root invalid locally and fails the fetch (a stored block that no
longer matches its CID is local damage: it is deleted and fetched again). A fetch that gets no verified
block and no response progress for lookup_timeout + stall_timeout fails, so a hung one ends.

Upload pacing (section 3): a responder starts a slow link's next /blocks response no sooner than its size at
the upload cap (by default half the rate estimate) after the previous one started, and refuses a request it
would have to hold back for longer than a few seconds. So after a response on a slow link, a provider isn't
asked again until as long as that request took has passed again. Refusals within four times that of a
served response aren't counted towards max_failures (the estimates at the two ends differ, and a responder
may set a lower cap): they are retried every eighth of that time, or BUSY_DELAY if longer.

Limits (sections 7 and 8): max_download_size bounds both the target's verified size and the DAG node bytes
a fetch downloads (a DAG of empty files has size 0 but can hold gigabytes of directory nodes); listings and
write-outs stop at max_output_entries entries met while fetching. fetch_dag also bounds the bytes of the
DAG's distinct blocks by what its declared size can hold (dag_bound: the size, DAG_GRANULE_OVERHEAD per
DAG_GRANULE of it and DAG_SLACK), plus what its directory and index nodes bring (node_credit: their own
bytes and DAG_ENTRY_OVERHEAD per entry or index child, less INDEX_NODE_COST for an index node); a
caller's max_dag_bytes (a quota) can lower it. fetch_listing has the same bound for a size of 0, and each
read_range call bounds the bytes of the file nodes it meets by range_bound (what the range can hold, plus two
full file nodes per level a file of the declared size can have). A file node may list children of size 0,
and index nodes can be chained, so without these a file declaring 1 byte, or a directory of one entry, could
hold any number of distinct nodes. What the builder writes never comes near them (design.md, "DAG byte
bound"). While one applies, requests ask for no more than it leaves, counting a DAG node as MAX_BLOCK_SIZE
until it arrives (a directory or index node as nothing under the size-derived bound: see _reserve), so a
fetch downloads at most about one block beyond it before failing with TooLarge. max_storage never refuses a
fetch: one
that isn't being pinned evicts other cached fetches to make room for each response, and goes over
max_storage if nothing is evictable (the node evicts again once it ends). What refuses a fetch, pinned or
not, with StorageFull is the disk: its remaining size, and then each response, must leave the disk reserve
free (Storage.check_disk).

Requests are spread over up to max_parallel_providers providers (1 on slow nodes), one /blocks request in
flight per provider. Request threads only call the client; all fetch state is changed by the thread that
runs the fetch, from the events they queue.

Within `with observing(observer):`, every fetch the thread opens reports its total size and each block it
meets to the Observer, and raises Cancelled once observer.cancelled() is true (background jobs use this).
"""
import queue
import threading
import time
from collections import OrderedDict, deque
from contextlib import contextmanager
from itertools import islice
from typing import Callable, Iterator

from rnfs import statedb
from rnfs.config import Config
from rnfs.constants import (CHUNK_SIZE, KIB, MAX_BLOCK_SIZE, MAX_CHILDREN, MAX_DEPTH, MAX_REQUEST_SIZE,
                            MAX_RESPONSE_SIZE, RESPONSE_OVERHEAD)
from rnfs.content import walker
from rnfs.content.cid import Cid
from rnfs.content.storage import Storage
from rnfs.content.walker import Expect
from rnfs.errors import InvalidDag, NotFound, RnfsError, TooLarge
from rnfs.log import LOG_VERBOSE, LOG_WARNING, log, node_name, plural
from rnfs.network import client as peer_client
from rnfs.network import protocol, rates
from rnfs import mesh

BUSY_DELAY = 2.0  # seconds before a busy provider is asked again
MAX_STALLS = 2  # a provider that stalls this often fails
WINDOW = 32  # chunks fetched ahead of what a byte-range read has written out
POLL = 0.5  # seconds between checks while waiting for responses
DAG_NODE_GUESS = 8 * KIB  # assumed size of a DAG node block when reserving a request's batch
MAX_BATCH = 32  # CIDs reserved for one request at most; the client sends as many as fit one packet
MAX_INVALID = 10_000  # roots remembered as invalid; past this the oldest is forgotten


def import_invalid(value: list, tx) -> int:
    """Import invalid.json (statedb.migrate): its roots, oldest first, replace the invalid table; one that doesn't
    parse is skipped, as rnfs 0.1.4 did, and only the newest MAX_INVALID are kept. Returns the number imported."""
    tx.execute("DELETE FROM invalid")
    roots: OrderedDict[Cid, None] = OrderedDict()
    for text in value:
        try:
            roots[Cid.parse(text)] = None
        except (ValueError, TypeError):
            continue
    kept = list(roots)[-MAX_INVALID:]
    tx.executemany("INSERT INTO invalid (cid, position) VALUES (?, ?)",
                   [(cid.binary, position) for position, cid in enumerate(kept)])
    return len(kept)
# The DAG byte bound (dag_bound and fetch_dag; design.md, "DAG byte bound"). Writers cut chunks of CHUNK_SIZE,
# or with FastCDC of over 16 KiB, so a file has at most one chunk per 16 KiB of content beyond its last; each
# costs at most 51 bytes of file node (a [cid, size] child), plus about 1/255 of that for the levels above
DAG_GRANULE = CHUNK_SIZE // 16  # 4 KiB of declared content, the smallest chunk SmallLeaves counts as large...
DAG_GRANULE_OVERHEAD = 64  # ...may bring this many bytes of file nodes (an honest file needs under 13)
# The target's own node header and the file node levels above its chunks (under 600), or the index nodes of a
# directory met before its entries (under 336)
DAG_SLACK = 1 * KIB
# A directory or index node pays for itself and brings this much per entry or child: room for the index nodes'
# own cost below and an empty directory's 3 bytes. An index child leads to entries of its own, which an index
# node is met before, so it brings its share in advance
DAG_ENTRY_OVERHEAD = 16
# What an index node takes from that: a chain of single-child index nodes costs 48 bytes a level, and nodes of 4
# children or more pay for themselves. The tree rule fills every index node but the last of its level, so an
# honest directory meets at most one that costs anything per level (7 levels for 2^64 entries) before its entries
INDEX_NODE_COST = 64
# The largest file node the tree rule writes: its header (array, type, array16) and MAX_CHILDREN children of at
# most 51 bytes ([cid, size]: a 39-byte CID and a 9-byte size, with their headers)
FILE_NODE_MAX = 5 + MAX_CHILDREN * 51


def dag_bound(size: int) -> int:
    """The most bytes of distinct blocks the DAG of a target declaring size content bytes may hold, before
    the allowance its directory and index nodes bring (Fetch.fetch_dag): size, DAG_GRANULE_OVERHEAD per
    DAG_GRANULE of it, and DAG_SLACK."""
    return size + DAG_GRANULE_OVERHEAD * -(-size // DAG_GRANULE) + DAG_SLACK


def node_credit(length: int, children: list[Expect]) -> int:
    """What a counted DAG node of length bytes with these children brings to the DAG byte bound: a directory
    node its own bytes plus DAG_ENTRY_OVERHEAD per entry, an index node the same per child less INDEX_NODE_COST
    (it costs at most that), a file node nothing (the content it holds pays for it)."""
    entries = sum(1 for child in children if child.kind is not None)
    if entries:
        return length + DAG_ENTRY_OVERHEAD * entries
    index_children = sum(1 for child in children if child.first_name is not None)
    if index_children:
        return max(0, length + DAG_ENTRY_OVERHEAD * index_children - INDEX_NODE_COST)
    return 0


def file_levels(size: int) -> int:
    """The most file node levels the tree rule gives a file of size bytes whose chunks, but the last, hold at
    least DAG_GRANULE: enough MAX_CHILDREN-wide levels for size // DAG_GRANULE + 1 chunks (7 at most)."""
    chunks = size // DAG_GRANULE + 1
    levels = 1
    while MAX_CHILDREN ** levels < chunks and levels < MAX_DEPTH:
        levels += 1
    return levels


def range_bound(size: int, length: int) -> int:
    """The most bytes of file nodes reading length bytes of a file declaring size bytes may meet (read_range).

    The nodes wholly inside the range cost what dag_bound(length) allows, as in a whole file; on each level
    the range can only end inside two more, and an honest file of size bytes has at most file_levels(size)
    levels of nodes of at most FILE_NODE_MAX bytes. Raw chunks aren't counted: a range's chunks hold its bytes
    and at most two chunks' worth more.
    """
    return dag_bound(length) + 2 * file_levels(size) * FILE_NODE_MAX


class Cancelled(RnfsError):
    """A fetch stopped because its observer asked it to (a job cancelled, or the node stopping)."""


class Observer:
    """Watches the fetches opened on one thread (see observing()): progress reports and a cancel check.

    Called on the fetching thread only; the methods must be quick.
    """

    def total(self, size: int):
        """The verified content size of what a fetch is about to fetch in full (fetch_dag, read_range)."""

    def block(self, cid: Cid, length: int, downloaded: bool):
        """A block was met that is now stored: downloaded just now, or already stored (downloaded False)."""

    def cancelled(self) -> bool:
        """True makes the fetch raise Cancelled at its next step (checked at least every POLL seconds)."""
        return False


_observers = threading.local()


@contextmanager
def observing(observer: Observer):
    """Report every fetch opened on this thread to observer until the block exits (rnfs.features.jobs)."""
    previous = getattr(_observers, "current", None)
    _observers.current = observer
    try:
        yield observer
    finally:
        _observers.current = previous


class _RangeBudget:
    """The file node bytes one read_range call may still meet (range_bound), and the nodes it has met."""

    __slots__ = ("limit", "length", "used", "counted")

    def __init__(self, limit: int, length: int):
        """A budget of limit bytes for reading length bytes."""
        self.limit = limit
        self.length = length
        self.used = 0
        self.counted: set[Cid] = set()

    def count(self, root: Cid, cid: Cid, length: int):
        """Count a DAG node of length bytes once. Raises TooLarge once over the limit."""
        if cid in self.counted:
            return
        self.counted.add(cid)
        self.used += length
        if self.used > self.limit:
            raise TooLarge(f"reading {plural(self.length, 'byte')} of {root} would take more than {self.limit} "
                           "bytes of file nodes, more than such a range can need: refusing it")


class _Provider:
    """What a fetch knows about one provider."""

    __slots__ = ("node_id", "order", "busy", "busy_until", "stalls", "failed", "path", "pacing_until",
                 "pacing_retry")

    def __init__(self, node_id: bytes, order: int, path: mesh.PathInfo = mesh.NO_PATH):
        """A provider found order-th, path hops away (looked up once, when found)."""
        self.node_id = node_id
        self.order = order
        self.path = path
        self.busy = 0  # busy refusals in a row
        self.busy_until = 0.0
        self.stalls = 0
        self.failed = False
        self.pacing_until = 0.0  # until then, a refusal is most likely the responder's upload pacing...
        self.pacing_retry = 0.0  # ...and is retried after this many seconds


class Fetcher:
    """Creates fetches and remembers roots found invalid. Thread-safe."""

    def __init__(self, storage: Storage, client, config: Config, own_id: bytes, routing,
                 make_lookup: Callable[[bytes], object], slow_node: bool = False, state_dir: str | None = None,
                 after_store: Callable[[], None] | None = None, clock: Callable[[], float] = time.monotonic,
                 busy_delay: float = BUSY_DELAY, make_room: Callable[[int], None] | None = None):
        """Fetch into storage with client (PeerClient); routing is the RoutingTable (bans, seniority).

        make_lookup(key) returns a dht.lookup.Lookup for key; after_store() runs after each batch of stored
        blocks, so the node can enforce max_storage; make_room(nbytes) evicts cache so an unpinned fetch can
        store nbytes more within max_storage, as far as it can (by default storage.evict, without withdrawing
        what it made incomplete).
        """
        self.storage = storage
        self.client = client
        self.config = config
        self.own_id = own_id
        self.routing = routing
        self.make_lookup = make_lookup
        self.slow_node = slow_node
        self.after_store = after_store or (lambda: None)
        self.make_room = make_room or (lambda nbytes: storage.evict(nbytes))
        self.clock = clock
        self.busy_delay = busy_delay
        # node ID -> mesh.PathInfo (hops, next-hop interface bitrate) for ranking providers; the node sets
        # RNS's (mesh.RnsPaths.info), and without it nothing is known and ranking falls back to reply order
        self.path_info: Callable[[bytes], mesh.PathInfo] = mesh.no_path_info
        self._lock = threading.Lock()
        self._db = None if state_dir is None else statedb.connect(state_dir)
        self._invalid: OrderedDict[Cid, None] = OrderedDict()  # oldest first
        self._invalid_saved: set = set()  # the binary CIDs the invalid table holds
        self._invalid_position = 0  # the next root's position in the table: after every one saved
        if self._db is not None:
            for binary, position in self._db.query("SELECT cid, position FROM invalid ORDER BY position"):
                self._invalid_saved.add(binary)
                if isinstance(position, int):
                    self._invalid_position = max(self._invalid_position, position + 1)
                try:
                    self._invalid[Cid.from_bytes(binary)] = None
                except ValueError:
                    continue
            while len(self._invalid) > MAX_INVALID:
                self._invalid.popitem(last=False)
        # Changed since the last save; a row that didn't parse, or past MAX_INVALID, is deleted by the next
        self._invalid_dirty = len(self._invalid) != len(self._invalid_saved)

    @property
    def max_parallel(self) -> int:
        """Providers used at a time: max_parallel_providers, 1 on slow nodes by default."""
        return max(1, self.config.effective_max_parallel_providers(self.slow_node))

    def open(self, root: Cid, names: list[str] | tuple[str, ...] = (), from_node: bytes | None = None,
             pinned: bool = False) -> "Fetch":
        """A fetch of <root>/<names>, to use as `with fetcher.open(...) as fetch:`.

        Raises InvalidDag if root was found invalid before. Its traversals raise StorageFull if what they
        would store doesn't fit on the disk (never because of max_storage).
        """
        if self.is_invalid(root):
            error = InvalidDag(f"{root} was found to be an invalid DAG")
            error.root = root
            raise error
        return Fetch(self, root, list(names), from_node, pinned)

    def is_invalid(self, root: Cid) -> bool:
        """True if root was marked invalid locally."""
        with self._lock:
            return root in self._invalid

    def mark_invalid(self, root: Cid):
        """Remember that root's DAG broke a reader check, so it isn't fetched again (saved by save())."""
        with self._lock:
            if root in self._invalid:
                return
            self._invalid[root] = None
            while len(self._invalid) > MAX_INVALID:
                self._invalid.popitem(last=False)
            self._invalid_dirty = True
        log(f"marked {root} invalid locally", LOG_WARNING)

    def save(self):
        """Persist the invalid roots added or forgotten since the last save, if any (the node calls this
        periodically and at shutdown)."""
        if self._db is None:
            return
        with self._lock:
            if not self._invalid_dirty:
                return
            # Written under the lock, so saves can't overtake each other; it rarely changes
            current = [cid.binary for cid in self._invalid]
            added = [binary for binary in current if binary not in self._invalid_saved]
            gone = self._invalid_saved.difference(current)
            with self._db.transaction() as tx:
                tx.executemany("DELETE FROM invalid WHERE cid = ?", [(binary,) for binary in gone])
                tx.executemany("INSERT OR REPLACE INTO invalid (cid, position) VALUES (?, ?)",
                               [(binary, self._invalid_position + n) for n, binary in enumerate(added)])
            self._invalid_position += len(added)
            self._invalid_saved = set(current)
            self._invalid_dirty = False

    def close(self):
        """Release the state database (the node calls this once it has saved everything at shutdown)."""
        if self._db is not None:
            self._db.close()


class Fetch:
    """One fetch of a target: its protection session, providers, lookup and the blocks it has stored."""

    def __init__(self, fetcher: Fetcher, root: Cid, names: list[str], from_node: bytes | None, pinned: bool):
        """Use Fetcher.open."""
        self.fetcher = fetcher
        self.storage = fetcher.storage
        self.client = fetcher.client
        self.config = fetcher.config
        self.root = root
        self.names = names
        self.from_node = from_node
        self.pinned = pinned
        self.target: Expect | None = None  # set by resolve()
        self.observer: Observer | None = getattr(_observers, "current", None)  # progress and cancel hook
        self.downloaded_blocks = 0
        self.downloaded_bytes = 0
        self.node_bytes = 0  # DAG node bytes downloaded, which max_download_size also bounds
        # Set by the caller to bound the bytes of the distinct blocks of the DAG met, stored or downloaded
        # (a remote pin's quota); TooLarge once over. fetch_dag adds the bound its target's size implies
        self.max_dag_bytes: int | None = None
        self.dag_bytes = 0
        # Content bytes the targets of fetch_dag declare, once it runs (fetch_listing's count as 0)
        self._declared: int | None = None
        self._credit = 0  # what the counted directory and index nodes bring to the bound (node_credit)
        self._counted: set[Cid] = set()
        self._range: _RangeBudget | None = None  # the budget of the read_range call ensuring blocks, if any
        self._progress_at = fetcher.clock()  # last verified block or response progress, for the no-progress deadline
        self.root_delivered = False
        self.closed = False
        self.session = None
        self._protection = None
        self._providers: dict[bytes, _Provider] = {}
        self._live = 0  # providers not failed; read by the lookup thread, so kept as a plain counter
        self._probed = from_node is not None  # open links are only asked when the DHT may be used
        self._lookup = None
        self._lookup_running = False
        self._lookup_found = 0  # providers the last lookup run reported
        self._events: queue.Queue = queue.Queue()
        # provider -> (CIDs asked for, is a probe); kept across _download calls, since a request may outlive one
        self._in_flight: dict[bytes, tuple[list[Cid], bool]] = {}
        if from_node is not None:
            self._new_provider(from_node)

    # Context

    def __enter__(self) -> "Fetch":
        """Start the protection session; the root, and whatever is stored below it, is protected at once."""
        self._protection = self.storage.protect()
        self.session = self._protection.__enter__()
        self.session.add(self.root)
        return self

    def __exit__(self, exc_type, exc, tb):
        """Stop the lookup and end the session; an InvalidDag raised by this fetch marks the root invalid.

        A fetch ended by an error (or cancelled) closes the links of its requests still in flight, so their
        responses don't keep using the link and hold up the next /blocks to those nodes.
        """
        self.closed = True
        if self._lookup is not None:
            self._lookup.stop()
        if exc_type is not None:
            while True:  # requests already answered aren't in flight any more
                try:
                    event = self._events.get_nowait()
                except queue.Empty:
                    break
                if event[0] == "result":
                    self._in_flight.pop(event[1], None)
            for node_id in list(self._in_flight):
                try:
                    self.client.close(node_id)
                except Exception as e:
                    log(f"could not close the link to {node_name(node_id)}: {e}", LOG_WARNING)
        # Only this fetch's own error: one from another fetch nested in the same `with` is about another root
        if isinstance(exc, InvalidDag) and getattr(exc, "root", None) == self.root:
            self.fetcher.mark_invalid(self.root)
        self._protection.__exit__(exc_type, exc, tb)
        return False

    @contextmanager
    def _checking(self):
        """Tag an InvalidDag raised within with this fetch's root, so only this fetch marks it invalid."""
        try:
            yield
        except InvalidDag as e:
            if getattr(e, "root", None) is None:
                e.root = self.root
            raise

    def _check_cancelled(self):
        """Raise Cancelled if the observer asks the fetch to stop."""
        if self.observer is not None and self.observer.cancelled():
            raise Cancelled(f"fetching {self.root} was cancelled")

    def get(self, expect: Expect) -> bytes:
        """A walker get over stored blocks (protected by this fetch), raising NotFound for a missing block."""
        block = self.storage.get(expect.cid)
        if block is None:
            raise NotFound(f"block {expect.cid} is not stored")
        return block

    # Traversals

    def resolve(self) -> Expect:
        """Fetch the root block and the blocks along the path; the target's expectation (its block stored)."""
        with self._checking():
            return self._resolve()

    def _resolve(self) -> Expect:
        """resolve(), untagged."""
        expect = walker.root_expect(self.root)
        self.ensure([expect])
        for i, name in enumerate(self.names):
            while True:
                child = walker.step(expect, self.get(expect), name)
                if child is None:
                    raise NotFound(f"{self.root}/{'/'.join(self.names[:i + 1])} does not exist")
                self.ensure([child])
                expect = child
                if not walker.is_index_step(expect):
                    break
        self.target = expect
        return expect

    def size_of(self, expect: Expect) -> int:
        """Content bytes below a stored, checked block."""
        with self._checking():
            return walker.size_of(expect, self.get(expect))

    def check_size(self, size: int):
        """Raise TooLarge if a fetch of size content bytes would exceed max_download_size."""
        limit = self.config.max_download_size
        if limit is not None and size > limit:
            raise TooLarge(f"{size} bytes would exceed max_download_size ({limit})")

    def check_disk(self, target: Expect, size: int):
        """Raise StorageFull if the size bytes still to fetch below target wouldn't fit on the disk."""
        if not self.storage.is_complete(target.cid):
            self.storage.check_disk(size)

    def fetch_dag(self, target: Expect, max_entries: int | None = None):
        """Fetch every block of target's DAG not already stored, level by level, checking each.

        Raises TooLarge once its distinct blocks hold more than dag_limit() bytes (what target's declared
        size and the directory entries met allow, or max_dag_bytes if lower), and with max_entries once more
        directory entries than that are met (for write-outs).
        """
        with self._checking():
            self.ensure([target])
            size = self.size_of(target)
            self.check_size(size)
            # From here on the DAG's blocks are counted (the target's again) against what its size can hold
            self._declared = (self._declared or 0) + size
            if self.observer is not None:
                self.observer.total(size)
            # The root's own DAG, already complete, was checked from the same expectation when it was memoised
            if target == walker.root_expect(self.root) and self.storage.is_complete(target.cid):
                return
            # What is stored already needn't fit again (its DAG nodes make this an estimate)
            self.check_disk(target, size - self.storage.dag_bytes(target.cid))
            seen = {target}
            frontier = [target]
            entries = 0
            while frontier:
                children = self.ensure(frontier)
                next_frontier = []
                for expect in frontier:
                    for child in children[expect]:
                        if child not in seen:
                            seen.add(child)
                            next_frontier.append(child)
                entries = self._count_entries(entries, frontier, children, next_frontier, max_entries)
                frontier = next_frontier

    def fetch_listing(self, target: Expect, max_entries: int | None = None):
        """Fetch the directory and index nodes of the directory target (not its entries' contents).

        Raises TooLarge once more than max_entries (default max_output_entries) entries are met, or once its
        blocks hold more than dag_limit() bytes: a listing's blocks are bounded as fetch_dag's are for a
        target of size 0 (its directory nodes pay for themselves, a chain of index nodes doesn't).
        """
        if max_entries is None:
            max_entries = self.config.max_output_entries
        with self._checking():
            if self._declared is None:
                self._declared = 0  # from here on its blocks are counted, the target's again
            frontier = [target]
            seen = {target}
            entries = 0
            while frontier:
                children = self.ensure(frontier)
                # Only an index's children carry first_name; a directory's entries and a file's chunks don't
                next_frontier = [child for expect in frontier for child in children[expect]
                                 if walker.is_index_step(child) and child not in seen]
                entries = self._count_entries(entries, frontier, children, next_frontier, max_entries)
                seen.update(next_frontier)
                frontier = next_frontier

    @staticmethod
    def _count_entries(entries: int, frontier: list[Expect], children: dict[Expect, list[Expect]],
                       next_frontier: list[Expect], max_entries: int | None) -> int:
        """Entries met so far, after a level: raises TooLarge past max_entries, before the next level is fetched.

        Entries of distinct directory nodes are counted, plus the index children about to be fetched: each
        holds at least one entry below it (its first_name), so an index tree can't grow unchecked either.
        """
        entries += sum(1 for expect in frontier for child in children[expect] if child.kind is not None)
        if max_entries is not None:
            pending = sum(1 for child in next_frontier if walker.is_index_step(child))
            if entries + pending > max_entries:
                raise TooLarge(f"more than max_output_entries ({max_entries}) entries")
        return entries

    def read_range(self, target: Expect, offset: int = 0, length: int | None = None) -> Iterator[bytes]:
        """The bytes of the file target in [offset, offset + length), fetching only the chunks covering them
        and the nodes above, WINDOW chunks at a time. Raises ValueError for a directory.

        Lazy all the way down: the range is walked depth-first as it is read, so a DAG claiming billions of
        chunks costs nothing until they are read, and small chunks count against max_output_entries. The file
        nodes it meets, stored or downloaded, may hold range_bound(size, range length) bytes (TooLarge past
        it): a node can be padded with children of size 0, which the walk skips but must download.
        """
        if offset < 0 or (length is not None and length < 0):
            raise ValueError("offset and length must be non-negative")
        with self._checking():
            self.ensure([target])
            if walker.kind_of(target, self.get(target)) != "file":
                raise ValueError(f"{target.cid} is a directory")
            size = self.size_of(target)
            end = size if length is None else min(size, offset + length)
            if offset >= end:
                return
            self.check_size(end - offset)
            self.check_disk(target, end - offset)
            budget = _RangeBudget(range_bound(size, end - offset), end - offset)
            if not target.cid.is_raw:
                budget.count(self.root, target.cid, len(self.get(target)))
            small = walker.SmallLeaves(self.config.max_output_entries)
            window: list[tuple[Expect, int, int]] = []
            for leaf in self._range_leaves(target, offset, end - offset, budget):
                small.meet(leaf[2])
                window.append(leaf)
                if len(window) >= WINDOW:
                    yield from self._read_window(window)
                    window = []
            yield from self._read_window(window)

    def _read_window(self, window: list[tuple[Expect, int, int]]) -> Iterator[bytes]:
        """Fetch a window of chunks in one go, then yield the range within each, in order."""
        self.ensure([expect for expect, _, _ in window])
        for expect, child_offset, child_length in window:
            block = self.get(expect)
            try:
                walker.check_raw(expect, len(block))
            except InvalidDag:
                # Checked by its recorded length when ensured; a chunk damaged on disk since is fetched again
                if self.storage.get(expect.cid, verify=True) is not None:
                    raise
                log(f"stored block {expect.cid} of {self.root} is damaged; fetching it again", LOG_WARNING)
                self.ensure([expect])
                block = self.get(expect)
                walker.check_raw(expect, len(block))
            yield block[child_offset:child_offset + child_length]

    def _range_leaves(self, target: Expect, offset: int, length: int,
                      budget: _RangeBudget) -> Iterator[tuple[Expect, int, int]]:
        """The raw chunks covering a range, in order, each with the range within it, depth-first; the DAG
        nodes below each node are fetched together when it is reached, counted against budget (the stack
        holds at most MAX_DEPTH × MAX_CHILDREN items)."""
        stack: list[tuple[Expect, int, int]] = [(target, offset, length)]
        while stack:
            expect, child_offset, child_length = stack.pop()
            if expect.cid.is_raw:
                yield expect, child_offset, child_length
                continue
            parts = walker.range_children(expect, self.get(expect), child_offset, child_length)
            # Only while ensuring: another read_range of this fetch may run between this generator's steps
            self._range = budget
            try:
                self.ensure([child for child, _, _ in parts if not child.cid.is_raw])
            finally:
                self._range = None
            stack.extend(reversed(parts))

    # Getting blocks

    def ensure(self, expects: list[Expect]) -> dict[Expect, list[Expect]]:
        """Make sure every block of expects is stored and passes its reader checks; their children's expects.

        Raises InvalidDag, NotFound when no provider can supply a block, StorageFull, TooLarge.
        """
        with self._checking():
            return self._ensure(expects)

    def _ensure(self, expects: list[Expect]) -> dict[Expect, list[Expect]]:
        """ensure(), untagged."""
        self._check_cancelled()
        children: dict[Expect, list[Expect]] = {}
        wanted: dict[Cid, list[Expect]] = {}
        for expect in dict.fromkeys(expects):
            walker.check_cid(expect)  # before fetching a block that can't be right
            checked = self._check_stored(expect)
            if checked is None:
                wanted.setdefault(expect.cid, []).append(expect)
            else:
                children[expect] = checked
                if self.observer is not None:
                    self.observer.block(expect.cid, self.storage.block_size(expect.cid) or 0, False)
        if wanted:
            self._download(wanted, children)
            self.fetcher.after_store()
        return children

    def _check_stored(self, expect: Expect) -> list[Expect] | None:
        """Children of a stored block after checking it against expect, or None if it isn't stored.

        A stored block failing the checks is verified against its CID: if it no longer matches (damaged on
        disk), it is deleted and None returned, so it is fetched again rather than the root marked invalid.
        """
        try:
            if expect.cid.is_raw:
                length = self.storage.block_size(expect.cid)
                if length is None:
                    return None
                walker.check_raw(expect, length)  # stored blocks were verified, so their length is enough
                self._count_dag(expect.cid, length)
                return []
            block = self.storage.get(expect.cid)
            if block is None:
                return None
            result = walker.expand(expect, block)
        except InvalidDag:
            if self.storage.get(expect.cid, verify=True) is None:
                log(f"stored block {expect.cid} of {self.root} is damaged; fetching it again", LOG_WARNING)
                return None
            raise
        self._count_dag(expect.cid, len(block), result)
        return result

    def dag_limit(self) -> int | None:
        """Bytes the distinct blocks of the DAG may hold: max_dag_bytes, and once fetch_dag (or fetch_listing)
        runs, dag_bound of the size its target declares plus what the directory and index nodes counted bring
        (node_credit), whichever is lower. None if neither applies."""
        limits = [] if self.max_dag_bytes is None else [self.max_dag_bytes]
        if self._declared is not None:
            limits.append(dag_bound(self._declared) + self._credit)
        return min(limits, default=None)

    def _count_dag(self, cid: Cid, length: int, children: list[Expect] = ()):
        """Count a block of the DAG once against dag_limit(), if there is one, with what it brings to it
        (node_credit), and a DAG node against the budget of the read_range call ensuring it, if any. Raises
        TooLarge once over either."""
        if self._range is not None and not cid.is_raw:
            self._range.count(self.root, cid, length)
        if (self.max_dag_bytes is None and self._declared is None) or cid in self._counted:
            return
        self._counted.add(cid)
        self.dag_bytes += length
        self._credit += node_credit(length, children)
        limit = self.dag_limit()
        if self.dag_bytes > limit:
            if limit == self.max_dag_bytes:
                raise TooLarge(f"{self.root} holds more than {limit} bytes of blocks")
            content = f"its {plural(self._declared, 'byte')} of content and " if self._declared else ""
            raise TooLarge(f"{self.root} holds more than {limit} bytes of blocks, more than {content}its "
                           "directory entries can take: refusing it")

    def _room(self, wanted: dict[Cid, list[Expect]]) -> tuple | None:
        """What the limits leave for blocks not yet asked for, after the most the requests in flight can bring:
        (bytes under the size-derived bound, bytes under max_dag_bytes, bytes under the budget of the
        read_range call ensuring blocks), each None if that limit doesn't apply; None if none does."""
        if self._declared is None and self.max_dag_bytes is None and self._range is None:
            return None
        bound = None if self._declared is None else dag_bound(self._declared) + self._credit - self.dag_bytes
        quota = None if self.max_dag_bytes is None else self.max_dag_bytes - self.dag_bytes
        budget = None if self._range is None else self._range.limit - self._range.used
        room = (bound, quota, budget)
        for cids, _ in self._in_flight.values():
            for cid in cids:
                room = self._reserve(cid, wanted, room)
        return room

    def _reserve(self, cid: Cid, wanted: dict[Cid, list[Expect]], room: tuple) -> tuple:
        """room (as _room) after the most cid can add to the bytes counted: a raw chunk's declared size,
        otherwise MAX_BLOCK_SIZE, and nothing for a raw chunk under a read_range budget.

        Under the size-derived bound a directory or index node takes nothing: a directory node brings more
        than it takes, and an index node takes at most INDEX_NODE_COST, so a request of MAX_BATCH of them
        overshoots by at most 2 KiB. Holding them to INDEX_NODE_COST each would ask for a directory's
        subdirectories a few at a time.
        """
        bound, quota, budget = room
        if budget is not None and not cid.is_raw and cid not in self._range.counted:
            budget -= MAX_BLOCK_SIZE
        if cid in self._counted:
            return bound, quota, budget
        expects = wanted.get(cid)
        worst = MAX_BLOCK_SIZE
        if expects and cid.is_raw and expects[0].size is not None:
            worst = expects[0].size
        if bound is not None and not (expects and not cid.is_raw
                                      and not any(walker.FILE_TYPE in expect.types for expect in expects)):
            bound -= worst
        return bound, None if quota is None else quota - worst, budget

    @staticmethod
    def _fits(room: tuple) -> bool:
        """True if room (as _room) leaves nothing below zero."""
        return all(left is None or left >= 0 for left in room)

    def _accept(self, cid: Cid, block: bytes, wanted: dict[Cid, list[Expect]], children: dict):
        """Check a verified block against every expectation of it and the limits, then store it.

        Raises InvalidDag, TooLarge.
        """
        for expect in wanted[cid]:
            children[expect] = walker.expand(expect, block)
        if not cid.is_raw:
            limit = self.config.max_download_size
            if limit is not None and self.node_bytes + len(block) > limit:
                raise TooLarge(f"{self.root}'s DAG nodes would exceed max_download_size ({limit})")
            self.node_bytes += len(block)
        self._count_dag(cid, len(block), children[wanted[cid][0]])
        self.session.put(cid, block)
        self._progress_at = self.fetcher.clock()
        self.downloaded_blocks += 1
        self.downloaded_bytes += len(block)
        if self.observer is not None:
            self.observer.block(cid, len(block), True)
        if cid == self.root and not self.root_delivered:
            self.root_delivered = True
            if self._lookup is not None:
                self._lookup.stop()  # it may now have enough providers to stop searching

    def _download(self, wanted: dict[Cid, list[Expect]], children: dict):
        """Fetch the wanted blocks from providers until all are stored. Raises NotFound if none can."""
        outstanding = set(wanted)
        pending = deque(wanted)
        in_flight = self._in_flight
        self._progress_at = max(self._progress_at, self.fetcher.clock())  # it may be after a pacing wait
        give_up = self.config.lookup_timeout + self.config.stall_timeout
        while outstanding:
            self._check_cancelled()  # each pass waits at most POLL, so a cancel takes effect promptly
            # Requests and lookups have deadlines of their own; this catches whatever hangs regardless
            if self.fetcher.clock() - self._progress_at > give_up:
                raise NotFound(f"fetching {self.root} made no progress for {give_up:g}s")
            self._assign(pending, in_flight, outstanding, wanted)
            if not in_flight and not self._lookup_running and not self._waiting_for_busy():
                if not self._rerun_lookup():
                    raise NotFound(self._not_found_message(outstanding))
                continue
            try:
                events = [self._events.get(timeout=self._wait_time())]
            except queue.Empty:
                continue
            # Everything already queued is handled before assigning, so providers found together are ranked
            while True:
                try:
                    events.append(self._events.get_nowait())
                except queue.Empty:
                    break
            for event in events:
                self._handle_event(event, pending, in_flight, outstanding, wanted, children)

    def _handle_event(self, event: tuple, pending: deque, in_flight: dict, outstanding: set[Cid],
                      wanted: dict[Cid, list[Expect]], children: dict):
        """Apply one event from a request thread or the lookup thread."""
        kind = event[0]
        if kind == "provider":
            self._add_provider(event[1])
        elif kind == "lookup_done":
            self._lookup_running = False
            self._lookup_found = event[1]
        elif kind == "result":
            _, provider, cids, result, sent_at, rate = event
            _, probe = in_flight.pop(provider, (cids, False))
            self._handle(provider, cids, result, probe, wanted, children, outstanding, sent_at, rate)
            requeue = [cid for cid in cids if cid in outstanding and cid not in pending
                       and not any(cid in asked for asked, _ in in_flight.values())]
            pending.extendleft(reversed(requeue))  # back at the front, keeping the walk's order

    def _make_room(self, nbytes: int):
        """Before storing nbytes: evict other cached fetches over max_storage if not pinned (going over it if
        nothing is evictable), and check the disk. Raises StorageFull if the disk can't take them."""
        if nbytes == 0:
            return
        if not self.pinned and self.storage.used_bytes() + nbytes > self.storage.max_storage:
            self.fetcher.make_room(nbytes)
        self.storage.check_disk(nbytes)  # each response, so a DAG whose nodes alone are large stops too

    def _not_found_message(self, outstanding: set[Cid]) -> str:
        """Why the fetch failed, for the user."""
        what = self.root if self.root in outstanding or not self._providers else f"{len(outstanding)} blocks of {self.root}"
        if self.from_node is not None:
            return f"{node_name(self.from_node)} could not supply {what}"
        if not self._providers:
            return f"no provider found for {self.root}"
        return f"no provider could supply {what} ({plural(len(self._providers), 'provider')} tried)"

    def _usable(self) -> list[_Provider]:
        """Providers not failed, in the order they're tried: long-known ones first, then the rest; within
        each, nearest and fastest first, then as found (section 6)."""
        return sorted((p for p in self._providers.values() if not p.failed),
                      key=lambda p: self._rank(p.node_id, p.path, p.order))

    def _rank(self, node_id: bytes, path: mesh.PathInfo, order: int) -> tuple:
        """Sort key for a provider: seniority stays first (an attacker's fresh identities can't jump the
        queue by being near), then hops and next-hop bitrate, then order."""
        senior = self.fetcher.routing.is_senior(node_id, self.config.provider_seniority)
        return mesh.provider_rank(senior, path, order)

    def _path(self, node_id: bytes) -> mesh.PathInfo:
        """What RNS knows about the path to node_id (nothing if the lookup fails)."""
        try:
            return self.fetcher.path_info(node_id)
        except Exception:
            return mesh.NO_PATH

    def _waiting_for_busy(self) -> bool:
        """True if a usable provider is only waiting out a busy delay."""
        now = self.fetcher.clock()
        return any(p.busy_until > now for p in self._providers.values() if not p.failed)

    def _wait_time(self) -> float:
        """How long to wait for an event: until the next busy provider may be asked again, at most POLL."""
        now = self.fetcher.clock()
        waits = [p.busy_until - now for p in self._providers.values() if not p.failed and p.busy_until > now]
        return max(0.01, min([POLL, *waits]))

    def _assign(self, pending: deque, in_flight: dict, outstanding: set[Cid], wanted: dict[Cid, list[Expect]]):
        """Send requests to idle providers, after finding providers if there are none."""
        now = self.fetcher.clock()
        usable = self._usable()
        if not usable and not in_flight:
            self._discover(pending, in_flight)
            return
        busy_providers = sum(1 for _, probe in in_flight.values() if not probe)
        for provider in usable:
            if not pending or busy_providers >= self.fetcher.max_parallel:
                break
            if provider.node_id in in_flight or provider.busy_until > now:
                continue
            batch = self._take_batch(pending, provider.node_id, wanted)
            if batch:
                in_flight[provider.node_id] = (batch, False)
                self._send(provider.node_id, batch)
                busy_providers += 1

    def _take_batch(self, pending: deque, node_id: bytes, wanted: dict[Cid, list[Expect]]) -> list[Cid]:
        """The next CIDs to ask node_id for: what one response can carry and one request packet on its link can
        hold, at least one.

        Under a DAG limit, only what the room it leaves can take for sure (_room), but at least one CID when no
        request is in flight: so a fetch downloads at most about one block beyond the limit.
        """
        budget = rates.requester_max_bytes(self.client.rate_estimate(node_id), self.config)
        # However large max_bytes is, the responder's budget is at most this (server.blocks_budget, section 7)
        carried = min(budget, MAX_RESPONSE_SIZE - RESPONSE_OVERHEAD)
        # The client sends what fits its link's packet and returns the rest: the open link's MDU, else the
        # largest request any link allows (test fakes may not know links). Whether a prefix fits only gets
        # worse as it grows, so one count covers every batch taken from the front of pending
        link_mdu = getattr(self.client, "link_mdu", None)
        mdu = (link_mdu(node_id) if link_mdu is not None else None) or MAX_REQUEST_SIZE
        fit = max(1, protocol.blocks_batch_size(budget, list(islice(pending, MAX_BATCH)), mdu))
        room = self._room(wanted)
        batch: list[Cid] = []
        total = 0
        while pending and len(batch) < fit:
            cid = pending[0]
            known = wanted[cid][0].size if cid.is_raw else None
            guess = known if known is not None else (CHUNK_SIZE if cid.is_raw else DAG_NODE_GUESS)
            if batch and total + guess > carried:
                break
            if room is not None:
                room = self._reserve(cid, wanted, room)
                if not self._fits(room) and (batch or self._in_flight):
                    break
            batch.append(pending.popleft())
            total += guess
        return batch

    def _send(self, node_id: bytes, cids: list[Cid]):
        """Ask node_id for cids on a request thread, which queues the result."""
        def progress(fraction: float):
            """The response is arriving: the fetch isn't hung."""
            self._progress_at = self.fetcher.clock()

        def work():
            """Run the blocking request and queue its result, with when it was sent and the rate estimate
            it was sent with (the responder's upload cap derives from the same response)."""
            sent_at = self.fetcher.clock()
            rate = None
            try:
                rate = self.client.rate_estimate(node_id)
                result = self.client.blocks(node_id, cids, on_progress=progress)
            except Exception as e:
                result = peer_client.BlocksResult(peer_client.FAILED, unsent=list(cids), reason=str(e))
            if result.elapsed is not None:
                # When it actually went out: opening the link or waiting for its /blocks slot isn't its time
                sent_at = max(sent_at, self.fetcher.clock() - result.elapsed)
            self._events.put(("result", node_id, cids, result, sent_at, rate))

        threading.Thread(target=work, name="rnfs-fetch", daemon=True).start()

    # Finding providers

    def _discover(self, pending: deque, in_flight: dict):
        """No usable provider: ask nodes with open links, else start the lookup."""
        if not self._probed:
            self._probed = True
            banned = self.fetcher.routing.is_banned
            linked = [n for n in self.client.open_links() if n != self.fetcher.own_id and not banned(n)]
            # Ranked like providers; most recently used breaks ties
            ranked = sorted(enumerate(linked), key=lambda item: self._rank(item[1], self._path(item[1]), item[0]))
            candidates = [node_id for _, node_id in ranked][:self.fetcher.max_parallel]
            if candidates and pending:
                # Each is asked for just the first block missing, which is the root at the start
                first = pending[0]
                log(f"asking {plural(len(candidates), 'node')} with an open link for {first}", LOG_VERBOSE)
                for node_id in candidates:
                    in_flight[node_id] = ([first], True)
                    self._send(node_id, [first])
                return
        if self._lookup is None and self.from_node is None:
            self._lookup = self.fetcher.make_lookup(self.root.key)
            log(f"looking for providers of {self.root}", LOG_VERBOSE)
            self._start_lookup()

    def _start_lookup(self):
        """Run find_providers on a thread of its own; providers and its end arrive as events."""
        self._lookup_running = True
        lookup = self._lookup

        def should_stop() -> bool:
            """Stop once enough providers are known and one delivered the root, or the fetch is over."""
            return self.closed or (self.root_delivered and self._live >= self.fetcher.max_parallel)

        def work():
            """Search, reporting each provider as its reply arrives."""
            found = []
            try:
                found = lookup.find_providers(lambda node_id: self._events.put(("provider", node_id)), should_stop)
            except Exception as e:
                log(f"provider lookup for {self.root} failed: {e}", LOG_WARNING)
            self._events.put(("lookup_done", len(found)))

        threading.Thread(target=work, name="rnfs-find-providers", daemon=True).start()

    def _rerun_lookup(self) -> bool:
        """Every provider failed: search again for a fresh random half, within lookup_timeout. False if not."""
        if self._lookup is None or self.closed:
            return False
        # A run that found nothing new would most likely find nothing new again
        if self._lookup_found == 0 or self._lookup.remaining() <= 0:
            return False
        log(f"every provider of {self.root} failed; searching again", LOG_VERBOSE)
        self._lookup_found = 0
        self._start_lookup()
        return True

    def _add_provider(self, node_id: bytes):
        """Use a provider the lookup found, unless it is us, banned, known, or over the unknown-ID budget."""
        if node_id == self.fetcher.own_id or node_id in self._providers or self.fetcher.routing.is_banned(node_id):
            return
        if self._lookup is not None and not self._lookup.allow_unknown(node_id):
            log(f"skipping provider {node_name(node_id)}: identity unknown and the path request budget is spent",
                LOG_VERBOSE)
            return
        self._new_provider(node_id)
        log(f"found provider {node_name(node_id)} for {self.root}", LOG_VERBOSE)

    # Results

    def _new_provider(self, node_id: bytes) -> _Provider:
        """Start using node_id as a provider, after those found before it."""
        provider = self._providers[node_id] = _Provider(node_id, len(self._providers), self._path(node_id))
        self._live += 1
        return provider

    def _fail(self, provider: _Provider, why: str):
        """A provider failed: it isn't used again during this fetch."""
        if not provider.failed:
            provider.failed = True
            self._live -= 1
            log(f"provider {node_name(provider.node_id)} failed for {self.root}: {why}", LOG_VERBOSE)

    def _handle(self, node_id: bytes, cids: list[Cid], result, probe: bool, wanted: dict, children: dict,
                outstanding: set[Cid], sent_at: float | None = None, rate: float | None = None):
        """Store a response's blocks and apply the provider failure and pacing rules. Raises InvalidDag.

        sent_at is when the request was sent and rate the link's rate estimate then (None: unknown).
        """
        provider = self._providers.get(node_id)
        if result.bad:
            # Not a lossy link: the node sent something other than what was asked for (section 10)
            log(f"banning {node_name(node_id)}: it sent {plural(len(result.bad), 'block')} not matching "
                "the CID requested", LOG_WARNING)
            self.fetcher.routing.ban(node_id)
            if provider is None:
                provider = self._new_provider(node_id)
            self._fail(provider, "sent a block not matching its CID")
        new = {cid: block for cid, block in result.blocks.items() if cid in outstanding}
        self._make_room(sum(len(block) for cid, block in new.items() if not self.storage.has(cid)))
        for cid, block in new.items():
            self._accept(cid, block, wanted, children)
            outstanding.discard(cid)
        if probe:
            # A node with an open link that has the block becomes a provider; one that hasn't isn't penalised
            if result.blocks and provider is None:
                provider = self._new_provider(node_id)
                log(f"{node_name(node_id)} (open link) has {self.root}", LOG_VERBOSE)
                self._pace(provider, sent_at, rate)
            return
        if provider is None or provider.failed:
            return
        now = self.fetcher.clock()
        if result.status == peer_client.OK:
            provider.busy = 0
            if result.missing:
                self._fail(provider, f"doesn't have {plural(len(result.missing), 'block')} of the root it advertised")
            elif result.blocks:
                self._pace(provider, sent_at, rate)
        elif result.status == peer_client.BUSY and result.reason != "cancelled" and now < provider.pacing_until:
            # Refused soon after it served us: most likely its upload pacing, which isn't a failure (a response
            # it cancelled, displaced or past its deadline, counts as busy as usual)
            provider.busy_until = now + provider.pacing_retry
        elif result.status == peer_client.BUSY:
            provider.busy += 1
            if provider.busy >= self.config.max_failures:
                self._fail(provider, f"busy {provider.busy} times in a row")
            else:
                provider.busy_until = now + self.fetcher.busy_delay
        elif result.status == peer_client.STALLED:
            provider.stalls += 1
            if provider.stalls >= MAX_STALLS:
                self._fail(provider, f"stalled {provider.stalls} times")
        else:
            self._fail(provider, result.reason or "request failed")

    def _pace(self, provider: _Provider, sent_at: float | None, rate: float | None):
        """After provider served a response to a request sent at sent_at: on a slow link (by the rate
        estimate the request was sent with, as the responder judges it), wait before asking again, so the
        responder's upload pacing doesn't refuse the next request; and take refusals for a while as pacing."""
        if sent_at is None:
            return
        now = self.fetcher.clock()
        took = max(0.0, now - sent_at)  # about the response's transfer time at the link's rate, plus its RTT
        provider.pacing_until = sent_at + 4 * took
        provider.pacing_retry = max(self.fetcher.busy_delay, took / 8)
        if rate is not None and rate < self.config.slow_bitrate:
            # At the default cap (half the rate estimate) the next response may start once the last one's
            # transfer time has passed twice since it started
            provider.busy_until = max(provider.busy_until, now + took)
            self._progress_at = max(self._progress_at, provider.busy_until)  # waiting isn't hanging


def describe(fetch: Fetch) -> str:
    """A log line for a finished fetch."""
    return (f"fetched {plural(fetch.downloaded_blocks, 'block')} ({fetch.downloaded_bytes} bytes) of {fetch.root}"
            + (f" from {plural(len(fetch._providers), 'provider')}" if fetch._providers else ""))

