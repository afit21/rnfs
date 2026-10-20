"""Fetching a target from providers (specification sections 6 and 7).

A Fetch is one fetch of `<root>/<path>`, used as a context manager that holds a storage protection session,
so every block it stores is kept until it ends and an interrupted fetch resumes where it stopped. The node
drives it step by step: resolve() fetches the root block and the path, then fetch_dag(), read_range() or
fetch_listing() fetch what the operation needs. Blocks already stored are never fetched again, and each
CID is fetched once however often it appears.

Providers, in the order they are tried:
1. up to max_parallel_providers nodes with an open outgoing link (on a node that isn't slow, none over a slow
   link, below), asked for the first missing block; a fetch's hint first, link or not and whatever its link's
   speed (the node that asked for a remote pin, which may not have published its records yet);
2. if none has it, providers found by a Lookup running in the background, so downloading starts with
   the first one found; the search ends once max_parallel_providers providers are known and the root
   block has arrived, when the lookup is complete, or at lookup_timeout. Providers in the routing table
   for over provider_seniority are tried first, the rest after them; within each group, a better local score
   first (by band), then fewer hops, then a faster next-hop interface (rnfs.mesh.provider_rank), then reply
   order. Open links are ranked the same way. Hops and bitrates come from path_info (the node passes RNS's;
   by default none are known).
`get --from` uses only the given node.

A provider fails, and isn't used again during the fetch, when its link fails (no path, closed by the provider, a
malformed answer), it answers None for a block of the root it advertised, it is busy max_failures times in a row (each
retried after BUSY_DELAY), its requests go unanswered max_failures times in a row the way a lossy link loses them
(peer_client.LOST: each has its link closed and is retried on a new one after BUSY_DELAY), or it stalls twice. When no
providers are left, the lookup runs again for a fresh random half of providers, within the same lookup_timeout. A block
not matching its CID bans the provider; a correctly hashed block that isn't a valid DAG node marks the root invalid
locally and fails the fetch (a stored block that no longer matches its CID is local damage: it is deleted and fetched
again). A fetch that gets no verified block and no response progress for lookup_timeout + stall_timeout fails, so a hung
one ends.

Local scores (rnfs.dht.scores, section 6): a response with verified blocks raises its provider's score, and, once
per fetch, the scores of the nodes whose /find answers listed it; a provider that fails falls, one sending a block
not matching its CID much further.

End-game duplication (NET-4; section 7): once nothing is left to ask for, an idle provider is also asked for a
batch another has had in flight for longer than deadline_slack (and, on a link with a rate estimate, DUPLICATE_AFTER
times its blocks' transfer time at it), so a response dripping in (never stalling, its deadline hours away at
unmeasured_bitrate) doesn't hold the fetch up. Each request is duplicated at most once, only while fewer than
max_parallel_providers requests are in flight, and never over a slow link (so never on a slow node). The first
verified copy wins: the request still sending what has arrived has its link closed (unless another fetch has a
request on it: then it is left to finish), and its outcome isn't counted against its provider.

Upload pacing (section 3): a responder starts a slow link's next /blocks response no sooner than its size at the upload
cap (by default half the rate estimate) after the previous one started, and refuses a request it would have to hold back
for longer than a few seconds. So after a response on a slow link, a provider isn't asked again until as long as that
request took has passed again, and its link counts as in use meanwhile (PeerClient.hold_link), so the idle sweep doesn't
close it and lose the rate estimate. Refusals within four times that of a served response aren't counted towards
max_failures (the estimates at the two ends differ, and a responder may set a lower cap): they are retried every eighth
of that time, or BUSY_DELAY if longer.

Limits (sections 7 and 8): max_download_size bounds both the target's verified size and the DAG node bytes a
fetch downloads (a DAG of empty files has size 0 but can hold gigabytes of directory nodes); listings and
write-outs stop at max_output_entries entries met while fetching. fetch_dag also bounds the bytes of the DAG's
distinct blocks by what its declared size can hold (dag_bound: the size, DAG_GRANULE_OVERHEAD per DAG_GRANULE
of it and DAG_SLACK), plus what its directory and index nodes bring (node_credit: their own bytes and
DAG_ENTRY_OVERHEAD per entry or index child, less INDEX_NODE_COST for an index node); a caller's max_dag_bytes
(a quota) can lower it. fetch_listing has the same bound for a size of 0, and each read_range call bounds the
bytes of the file nodes it meets by range_bound (what the range can hold, plus two full file nodes per level a
file of the declared size can have). A file node may list children of size 0, and index nodes can be chained,
so without these a file declaring 1 byte, or a directory of one entry, could hold any number of distinct nodes.
What the builder writes never comes near them (design.md, "DAG byte bound"). While one applies, requests ask
for no more than it leaves, counting a DAG node as MAX_BLOCK_SIZE until it arrives (a directory or index node
as nothing under the size-derived bound: see _DagBudget.reserve), so a fetch downloads at most about one block beyond it
before failing with TooLarge. max_storage never refuses a fetch: one that isn't being pinned evicts other
cached fetches to make room for each response, and goes over max_storage if nothing is evictable (the node
evicts again once it ends). What refuses a fetch, pinned or not, with StorageFull is the disk: its remaining
size, and then each response, must leave the disk reserve free (Storage.check_disk). A CID the deny list denies
(rnfs.denylist; the storage's) ends a fetch with Denied where it is met, root or child, stored or not, before
it is asked for or read; a complete stored DAG is checked whole, since fetch_dag doesn't walk it (and its
blocks against a quota).

Requests are spread over up to max_parallel_providers providers (1 on slow nodes), one /blocks request in
flight per provider. A provider is reached over a slow link (Fetch._slow) when its link's rate estimate is below
slow_bitrate, or, with none, when the next-hop interface to it is or it is behind a slow path (fetcher.slow_path,
mesh.SlowPaths); the providers over slow links behind one next-hop interface share one request in flight
(Fetch._slot), so a node with a fast and a slow interface, which isn't slow, runs parallel requests over the fast
one only. Request threads only call the client and note when a response last made progress
(Fetch._heard_at, which the no-progress deadline reads with the fetch thread's own _progress_at); all other
fetch state is changed by the thread that runs the fetch, from the events they queue (ResultEvent, and the
lookup thread's ProviderEvent and LookupDone).

fetch_dag and fetch_listing walk the DAG inside one _download: a block's children still to fetch are queued as
soon as it is met, stored or downloaded, rather than once its whole tree level has arrived, so no provider waits
for a level's last response. A parent is always stored before its children are asked for, so whatever an
interrupted fetch stored hangs together from the target, and the next fetch resumes from it.

read_range fetches WINDOW chunks at a time (SLOW_WINDOW while no provider known is reached over a fast link: 32 chunks
take about 5 hours on LoRa before the first byte), and asks for the next window's before handing the reader this one's
bytes, so the providers don't sit idle between windows; a block that a request brings after the call that asked for it
has returned is stored all the same. A fetch that ends by an error, is cancelled, or ends with requests it no longer
needs (a reader that stopped early, which has had at most one window fetched that it didn't read) closes the links of
its requests still in flight, unless another fetch has a request on the same (shared) link: then its request is left to
finish, rather than fail the other's. It never cancels a response's Resource: a responder would take that for a
rejection and cool the requester down (section 3).

Within `with observing(observer):`, every fetch the thread opens reports its total size and each block it
meets to the Observer, and raises Cancelled once observer.cancelled() is true (background jobs use this).
"""
import queue
import threading
import time
from collections import OrderedDict, deque
from contextlib import contextmanager
from itertools import islice
from typing import Callable, Iterator, NamedTuple

from rnfs import statedb
from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE, KIB, MAX_BLOCK_SIZE, MAX_CHILDREN, MAX_DEPTH, MAX_REQUEST_SIZE
from rnfs.content import walker
from rnfs.content.cid import Cid
from rnfs.content.storage import Storage
from rnfs.content.walker import Expect
from rnfs.dht.scores import Scores
from rnfs.errors import Cancelled, Denied, InvalidDag, NotFound, TooLarge
from rnfs.log import LOG_VERBOSE, LOG_WARNING, log, node_name, plural
from rnfs.network import client as peer_client
from rnfs.network import protocol, rates
from rnfs import mesh

BUSY_DELAY = 2.0  # seconds before a busy provider is asked again
MAX_STALLS = 2  # a provider that stalls this often fails
WINDOW = 32  # chunks fetched ahead of what a byte-range read has written out
SLOW_WINDOW = 1  # the same while no provider known is over a fast link (Fetch._window): one chunk, one ahead
POLL = 0.5  # seconds between checks while waiting for responses
DAG_NODE_GUESS = 8 * KIB  # assumed size of a DAG node block when reserving a request's batch
MAX_BATCH = 32  # CIDs reserved for one request at most; the client sends as many as fit one packet
MAX_INVALID = 10_000  # roots remembered as invalid; past this the oldest is forgotten
# End-game duplication: a batch on a link with a rate estimate is asked of another provider too only once it has
# been in flight this many times as long as its bytes take at that rate (and deadline_slack at least)
DUPLICATE_AFTER = 1.5


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


class _DagBudget:
    """The byte limits on the distinct blocks a fetch meets, and what it has counted against them (the module
    docstring's "Limits"). Pure bookkeeping: no storage, network or clock, and only the fetch thread uses it.

    A fetch's own budget (Fetch.budget) bounds the distinct blocks of its DAG, each counted once: by max_dag_bytes
    (a quota its caller sets) and, once fetch_dag or fetch_listing runs, by dag_bound of the size their targets
    declare plus what the directory and index nodes counted bring (node_credit). A read_range call passes
    Fetch.ensure a budget of its own (for_range), which also bounds the file nodes that call meets by range_bound,
    each counted once per call, and counts every block against the fetch's own budget as well.

    The room a budget leaves is a tuple (bytes under the size-derived bound, under max_dag_bytes, under the
    read_range call's bound), each None if that limit doesn't apply: room() computes it, reserve() takes from it
    what a block not yet received may add, and fits() checks it.
    """

    __slots__ = ("root", "whole", "max_dag_bytes", "declared", "credit", "used", "counted", "range_limit",
                 "range_length", "range_used", "range_counted")

    def __init__(self, root: Cid):
        """A fetch's own budget for root's DAG, with no limit until one is set (max_dag_bytes) or declared."""
        self.root = root
        self.whole = self  # the fetch's own budget, which a read_range call's counts against too
        self.max_dag_bytes: int | None = None
        self.declared: int | None = None  # content bytes fetch_dag's targets declare (fetch_listing's count as 0)
        self.credit = 0  # what the counted directory and index nodes bring to the bound (node_credit)
        self.used = 0  # bytes of the distinct blocks counted
        self.counted: set[Cid] = set()
        self.range_limit: int | None = None  # a read_range call's: range_bound for its range_length bytes
        self.range_length = 0
        self.range_used = 0
        self.range_counted: set[Cid] = set()

    def for_range(self, limit: int, length: int) -> "_DagBudget":
        """The budget of a read_range call reading length bytes: limit bytes of the file nodes it meets, and this
        fetch's own budget."""
        budget = _DagBudget(self.root)
        budget.whole = self.whole
        budget.range_limit = limit
        budget.range_length = length
        return budget

    def declare(self, size: int):
        """Bound the DAG by what size more content bytes can hold (fetch_dag; fetch_listing declares 0)."""
        whole = self.whole
        whole.declared = (whole.declared or 0) + size

    def limit(self) -> int | None:
        """Bytes the distinct blocks of the DAG may hold: max_dag_bytes, and once declared, dag_bound of the size
        declared plus what the directory and index nodes counted bring (node_credit), whichever is lower. None
        if neither applies."""
        whole = self.whole
        limits = [] if whole.max_dag_bytes is None else [whole.max_dag_bytes]
        if whole.declared is not None:
            limits.append(dag_bound(whole.declared) + whole.credit)
        return min(limits, default=None)

    def count(self, cid: Cid, length: int, children: list[Expect] = ()):
        """Count a DAG node of length bytes against a read_range call's bound, once per call, then any block of the
        DAG against the fetch's limits, once, with what it brings to them (node_credit). Raises TooLarge once over
        either."""
        if self.range_limit is not None and not cid.is_raw and cid not in self.range_counted:
            self.range_counted.add(cid)
            self.range_used += length
            if self.range_used > self.range_limit:
                raise TooLarge(f"reading {plural(self.range_length, 'byte')} of {self.root} would take more than "
                               f"{self.range_limit} bytes of file nodes, more than such a range can need: refusing it")
        whole = self.whole
        if (whole.max_dag_bytes is None and whole.declared is None) or cid in whole.counted:
            return
        whole.counted.add(cid)
        whole.used += length
        whole.credit += node_credit(length, children)
        limit = self.limit()
        if whole.used > limit:
            if limit == whole.max_dag_bytes:
                raise TooLarge(f"{self.root} holds more than {limit} bytes of blocks")
            content = f"its {plural(whole.declared, 'byte')} of content and " if whole.declared else ""
            raise TooLarge(f"{self.root} holds more than {limit} bytes of blocks, more than {content}its "
                           "directory entries can take: refusing it")

    def room(self, in_flight, wanted: dict[Cid, list[Expect]]) -> tuple | None:
        """What the limits leave for blocks not yet asked for, after the most the CIDs in_flight (asked for, not
        yet received) can bring; None if no limit applies."""
        whole = self.whole
        if whole.declared is None and whole.max_dag_bytes is None and self.range_limit is None:
            return None
        bound = None if whole.declared is None else dag_bound(whole.declared) + whole.credit - whole.used
        quota = None if whole.max_dag_bytes is None else whole.max_dag_bytes - whole.used
        budget = None if self.range_limit is None else self.range_limit - self.range_used
        room = (bound, quota, budget)
        for cid in in_flight:
            room = self.reserve(cid, wanted, room)
        return room

    def reserve(self, cid: Cid, wanted: dict[Cid, list[Expect]], room: tuple) -> tuple:
        """room after the most cid can add to the bytes counted: a raw chunk's declared size, otherwise
        MAX_BLOCK_SIZE, and nothing for a raw chunk under a read_range call's bound.

        Under the size-derived bound a directory or index node takes nothing: a directory node brings more
        than it takes, and an index node takes at most INDEX_NODE_COST, so a request of MAX_BATCH of them
        overshoots by at most 2 KiB. Holding them to INDEX_NODE_COST each would ask for a directory's
        subdirectories a few at a time.
        """
        bound, quota, budget = room
        if budget is not None and not cid.is_raw and cid not in self.range_counted:
            budget -= MAX_BLOCK_SIZE
        if cid in self.whole.counted:
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
    def fits(room: tuple) -> bool:
        """True if room leaves nothing below zero."""
        return all(left is None or left >= 0 for left in room)


class ResultEvent(NamedTuple):
    """A /blocks request's result, queued by its request thread."""

    node_id: bytes
    cids: list[Cid]  # what it asked for
    result: peer_client.BlocksResult
    sent_at: float | None  # when it went out (None: unknown)
    rate: float | None  # the link's rate estimate it was sent with (the responder's upload cap derives from it)


class ProviderEvent(NamedTuple):
    """A provider the lookup found, queued by the lookup thread as its reply arrives."""

    node_id: bytes


class LookupDone(NamedTuple):
    """A lookup run ended, queued by the lookup thread."""

    found: int  # providers it reported


class _Pending:
    """The CIDs a _download has still to ask for, in the walk's order, each at most once.

    A deque with a set alongside: requeueing a result asks whether each of its CIDs is already pending, and
    scanning a deque of a large file's chunks for every CID held up the fetch thread (16k pending, 20 ms a result).
    """

    __slots__ = ("_order", "_members")

    def __init__(self, cids):
        """Pending cids, in order (distinct, as a dict's keys are)."""
        self._order: deque[Cid] = deque(cids)
        self._members: set[Cid] = set(self._order)

    def __len__(self) -> int:
        """How many CIDs are pending."""
        return len(self._order)

    def __contains__(self, cid: Cid) -> bool:
        """True if cid is pending."""
        return cid in self._members

    def __iter__(self) -> Iterator[Cid]:
        """The pending CIDs, front first."""
        return iter(self._order)

    def __getitem__(self, index: int) -> Cid:
        """The CID at index (0 is the next one asked for)."""
        return self._order[index]

    def popleft(self) -> Cid:
        """Take the next CID to ask for."""
        cid = self._order.popleft()
        self._members.discard(cid)
        return cid

    def push_front(self, cids: list[Cid]):
        """Put cids (none of them pending) back at the front, keeping their order."""
        self._order.extendleft(reversed(cids))
        self._members.update(cids)

    def extend(self, cids: list[Cid]):
        """Add cids (none of them pending) at the back, keeping their order."""
        self._order.extend(cids)
        self._members.update(cids)


class _Walk:
    """The walk of fetch_dag or fetch_listing below a target, met block by block (A4): what each block met adds to
    fetch, and the directory entries met so far. Pure bookkeeping, used by the fetch thread only."""

    __slots__ = ("seen", "listing", "max_entries", "entries", "index_queued")

    def __init__(self, target: Expect, listing: bool, max_entries: int | None):
        """A walk from target: of its whole DAG, or for a listing (listing=True) of its index nodes only."""
        self.seen = {target}
        self.listing = listing
        self.max_entries = max_entries
        self.entries = 0  # entries of the distinct directory nodes met
        self.index_queued: set[Expect] = set()  # index children queued, not met yet

    def met(self, expect: Expect, children: list[Expect]) -> list[Expect]:
        """The children of expect, a block just met (stored or downloaded), to fetch next: each not seen before,
        and for a listing only an index's (a directory's entries and a file's chunks carry no first_name).

        Raises TooLarge once more than max_entries entries are met, counted per block before its children are
        fetched: the entries of the distinct directory nodes met, plus one for each index child queued and not
        met yet, which holds at least one entry below it (its first_name), so an index tree can't grow unchecked
        either.
        """
        self.index_queued.discard(expect)
        self.entries += sum(1 for child in children if child.kind is not None)
        follow = []
        for child in children:
            if child not in self.seen and (not self.listing or walker.is_index_step(child)):
                self.seen.add(child)
                follow.append(child)
                if walker.is_index_step(child):
                    self.index_queued.add(child)
        if self.max_entries is not None and self.entries + len(self.index_queued) > self.max_entries:
            raise TooLarge(f"more than max_output_entries ({self.max_entries}) entries")
        return follow


class _Provider:
    """What a fetch knows about one provider."""

    __slots__ = ("node_id", "order", "busy", "busy_until", "stalls", "lost", "failed", "path", "pacing_until",
                 "pacing_retry")

    def __init__(self, node_id: bytes, order: int, path: mesh.PathInfo = mesh.NO_PATH):
        """A provider found order-th, path hops away (looked up once, when found)."""
        self.node_id = node_id
        self.order = order
        self.path = path
        self.busy = 0  # busy refusals in a row
        self.busy_until = 0.0
        self.stalls = 0
        self.lost = 0  # requests unanswered in a row the way a lossy link loses them (peer_client.LOST)
        self.failed = False
        self.pacing_until = 0.0  # until then, a refusal is most likely the responder's upload pacing...
        self.pacing_retry = 0.0  # ...and is retried after this many seconds


class Fetcher:
    """Creates fetches and remembers roots found invalid. Thread-safe."""

    def __init__(self, storage: Storage, client, config: Config, own_id: bytes, routing,
                 make_lookup: Callable[[bytes], object], slow_node: bool = False, state_dir: str | None = None,
                 after_store: Callable[[], None] | None = None, clock: Callable[[], float] = time.monotonic,
                 busy_delay: float = BUSY_DELAY, make_room: Callable[[int], None] | None = None,
                 scores: Scores | None = None):
        """Fetch into storage with client (PeerClient); routing is the RoutingTable (bans, seniority), and scores the
        local scores, raised by verified blocks and the answers that led to them, lowered by failures and bad blocks
        (in memory only unless given).

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
        self.scores = scores if scores is not None else Scores()
        self.make_lookup = make_lookup
        self.slow_node = slow_node
        self.after_store = after_store or (lambda: None)
        self.make_room = make_room or (lambda nbytes: storage.evict(nbytes))
        self.clock = clock
        self.busy_delay = busy_delay
        # node ID -> mesh.PathInfo (hops, next-hop interface bitrate) for ranking providers; the node sets
        # RNS's (mesh.RnsPaths.info), and without it nothing is known and ranking falls back to reply order
        self.path_info: Callable[[bytes], mesh.PathInfo] = mesh.no_path_info
        # path_info of several nodes at once (mesh.RnsPaths.infos: one RPC to a shared instance, not one each)
        self.path_infos: Callable[[list[bytes]], dict[bytes, mesh.PathInfo]] = lambda node_ids: {
            node_id: self.path_info(node_id) for node_id in node_ids}
        # Whether a node is behind a slow path (mesh.SlowPaths: more than SLOW_PATH_HOPS hops, or this node's last link
        # to it was slow); the node sets its own. Without it, rate estimates and next-hop bitrates alone tell slow links
        self.slow_path: Callable[[bytes], bool] = lambda node_id: False
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
             pinned: bool = False, hint: bytes | None = None) -> "Fetch":
        """A fetch of <root>/<names>, to use as `with fetcher.open(...) as fetch:`.

        hint is a node likely to hold it (a remote pin's asker), asked first along with the nodes with an open
        link, as they are: one that hasn't it isn't penalised, and the lookup follows. Ignored with from_node.

        Raises InvalidDag if root was found invalid before. Its traversals raise StorageFull if what they
        would store doesn't fit on the disk (never because of max_storage).
        Raises Denied if the deny list denies root; its traversals raise Denied at the first denied CID they meet,
        stored or not, before fetching or reading it.
        """
        if self.is_invalid(root):
            error = InvalidDag(f"{root} was found to be an invalid DAG")
            error.root = root
            raise error
        self.check_denied(root)
        return Fetch(self, root, list(names), from_node, pinned, hint)

    def check_denied(self, cid: Cid):
        """Raise Denied if the deny list (the storage's) denies cid."""
        if self.storage.deny.denies(cid):
            raise Denied(f"{cid} is on this node's deny list")

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

    def __init__(self, fetcher: Fetcher, root: Cid, names: list[str], from_node: bytes | None, pinned: bool,
                 hint: bytes | None = None):
        """Use Fetcher.open."""
        self.fetcher = fetcher
        self.storage = fetcher.storage
        self.client = fetcher.client
        self.config = fetcher.config
        self.root = root
        self.names = names
        self.from_node = from_node
        self.pinned = pinned
        self.hint = hint  # asked first, with the open links (_discover)
        self.target: Expect | None = None  # set by resolve()
        self.observer: Observer | None = getattr(_observers, "current", None)  # progress and cancel hook
        self.downloaded_blocks = 0
        self.downloaded_bytes = 0
        self.node_bytes = 0  # DAG node bytes downloaded, which max_download_size also bounds
        self.budget = _DagBudget(root)  # the DAG byte limits (max_dag_bytes, dag_limit())
        # For the no-progress deadline: the fetch thread's last verified block (or pacing wait), and the last
        # response progress a request thread heard; the deadline counts from the later of the two
        self._progress_at = fetcher.clock()
        self._heard_at = self._progress_at
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
        # What each CID of a request in flight (not a probe) was asked for as, so a block arriving after the
        # _download call that asked for it has returned is checked and stored rather than dropped
        self._expects: dict[Cid, list[Expect]] = {}
        # End-game duplication (_duplicate): when each request in flight (not a probe) was sent, by the fetch
        # thread's clock; a request duplicated -> its duplicate, and back, while each is in flight; and the
        # requests whose links were closed because the other copy won, whose outcome isn't counted
        self._sent_at: dict[bytes, float] = {}
        self._expected: dict[bytes, float | None] = {}  # its bytes' transfer time at its link's rate estimate, if any
        self._duplicated: dict[bytes, bytes] = {}
        self._duplicates: dict[bytes, bytes] = {}
        self._abandoned: set[bytes] = set()
        # The idents of this fetch's request threads while they run, and the providers whose links it closed:
        # a link another fetch has a request on isn't closed (PeerClient.close_unless_used)
        self._threads: set[int] = set()
        self._links_closed: set[bytes] = set()
        self._credited: set[bytes] = set()  # nodes whose /find answers led to a provider that served this fetch
        if from_node is not None:
            self._new_provider(from_node)

    @property
    def max_dag_bytes(self) -> int | None:
        """Set by the caller to bound the bytes of the distinct blocks of the DAG met, stored or downloaded (a
        remote pin's quota); TooLarge once over. fetch_dag adds the bound its target's size implies."""
        return self.budget.max_dag_bytes

    @max_dag_bytes.setter
    def max_dag_bytes(self, value: int | None):
        """Set the quota."""
        self.budget.max_dag_bytes = value

    @property
    def dag_bytes(self) -> int:
        """Bytes of the distinct blocks of the DAG counted against its limits so far (none while none applies)."""
        return self.budget.used

    # Context

    def __enter__(self) -> "Fetch":
        """Start the protection session; the root, and whatever is stored below it, is protected at once."""
        self._protection = self.storage.protect()
        self.session = self._protection.__enter__()
        self.session.add(self.root)
        return self

    def __exit__(self, exc_type, exc, tb):
        """Stop the lookup and end the session; an InvalidDag raised by this fetch marks the root invalid.

        A fetch ended by an error (or cancelled, or by its reader closing a read_range early) closes the links of
        its requests still in flight, so their responses don't keep using the link and hold up the next /blocks
        to those nodes. One that ends normally does so for requests it no longer needs (a read_range's next window,
        left unread), but leaves a probe for the first block, which another node sent, to finish. A link another
        fetch has a request on (in progress or waiting for the link's /blocks slot) isn't closed: this fetch's
        request is left to finish, and its blocks dropped, rather than fail the other fetch's and make it drop
        the provider. Closing the link is how a requester stops a response: it never cancels the response's
        Resource, which a responder would take for a rejection and cool the requester down for (section 3).
        """
        self.closed = True
        if self._lookup is not None:
            self._lookup.stop()
        while True:  # requests already answered aren't in flight any more
            try:
                event = self._events.get_nowait()
            except queue.Empty:
                break
            if isinstance(event, ResultEvent):
                self._in_flight.pop(event.node_id, None)
        for node_id, (_, probe) in list(self._in_flight.items()):
            if (exc_type is None and probe) or node_id in self._links_closed:
                continue
            self._close_link(node_id)
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
        """Fetch every block of target's DAG not already stored, checking each; a block's children are asked for
        as soon as it is met (_Walk).

        Raises TooLarge once its distinct blocks hold more than dag_limit() bytes (what target's declared
        size and the directory entries met allow, or max_dag_bytes if lower), and with max_entries once more
        directory entries than that are met (for write-outs).
        """
        with self._checking():
            self.ensure([target])
            size = self.size_of(target)
            self.check_size(size)
            # From here on the DAG's blocks are counted (the target's again) against what its size can hold
            self.budget.declare(size)
            if self.observer is not None:
                self.observer.total(size)
            # The root's own DAG, already complete, was checked from the same expectation when it was memoised;
            # its blocks aren't met again, so the deny list is checked against them here
            if target == walker.root_expect(self.root) and self.storage.is_complete(target.cid):
                denied = self.storage.deny.cids
                if denied and any(cid.binary in denied for cid in self.storage.dag_blocks(target.cid)):
                    raise Denied(f"{target.cid} holds a block on this node's deny list")
                # A quota counts the blocks stored as well as downloaded: a remote pin or auto-fetch of a DAG held
                # already (a user pin, a cached fetch) gets the same answer as one fetched now
                if self.max_dag_bytes is not None and self.storage.dag_bytes(target.cid) > self.max_dag_bytes:
                    raise TooLarge(f"{self.root} holds more than {self.max_dag_bytes} bytes of blocks")
                return
            # What is stored already needn't fit again (its DAG nodes make this an estimate)
            self.check_disk(target, size - self.storage.dag_bytes(target.cid))
            self.ensure([target], walk=_Walk(target, False, max_entries))

    def fetch_listing(self, target: Expect, max_entries: int | None = None):
        """Fetch the directory and index nodes of the directory target (not its entries' contents).

        Raises TooLarge once more than max_entries (default max_output_entries) entries are met, or once its
        blocks hold more than dag_limit() bytes: a listing's blocks are bounded as fetch_dag's are for a
        target of size 0 (its directory nodes pay for themselves, a chain of index nodes doesn't).
        """
        if max_entries is None:
            max_entries = self.config.max_output_entries
        with self._checking():
            self.budget.declare(0)  # from here on its blocks are counted, the target's again
            self.ensure([target], walk=_Walk(target, True, max_entries))

    def read_range(self, target: Expect, offset: int = 0, length: int | None = None) -> Iterator[bytes]:
        """The bytes of the file target in [offset, offset + length), fetching only the chunks covering them
        and the nodes above, a window of chunks at a time (_window: WINDOW, or SLOW_WINDOW while no provider known
        is over a fast link). Raises ValueError for a directory.

        The next window's chunks are asked for before this one's bytes are handed to the reader, so the
        providers keep working while it reads; a reader that stops early has fetched at most one window more
        than it read, and ending the fetch closes the links still bringing it (but not one another fetch has a
        request on: that request is left to finish).

        Lazy all the way down: the range is walked depth-first as it is read, so a DAG claiming billions of
        chunks costs nothing until they are read, and small chunks count against max_output_entries. The file
        nodes it meets, stored or downloaded, may hold range_bound(size, range length) bytes (TooLarge past
        it): a node can be padded with children of size 0, which the walk skips but must download.
        """
        if offset < 0 or (length is not None and length < 0):
            raise ValueError("offset and length must be non-negative")
        with self._checking():
            self.ensure([target])
            if walker.kind_of(target, self.get(target)) != walker.FILE_TYPE:
                raise ValueError(f"{target.cid} is a directory")
            size = self.size_of(target)
            end = size if length is None else min(size, offset + length)
            if offset >= end:
                return
            self.check_size(end - offset)
            self.check_disk(target, end - offset)
            budget = self.budget.for_range(range_bound(size, end - offset), end - offset)
            if not target.cid.is_raw:
                budget.count(target.cid, len(self.get(target)))  # the fetch's own budget has counted it already
            small = walker.SmallLeaves(self.config.max_output_entries)
            leaves = self._range_leaves(target, offset, end - offset, budget)
            window = self._next_window(leaves, small)
            while window:
                ahead = self._next_window(leaves, small)
                yield from self._read_window(window, ahead)
                window = ahead

    def _next_window(self, leaves: Iterator[tuple[Expect, int, int]],
                     small: walker.SmallLeaves) -> list[tuple[Expect, int, int]]:
        """The next window of chunks of a range's walk (fewer at its end, none past it), each met by small: sized by
        _window once its first chunk is reached, so the file nodes above it, fetched on the way, have brought
        their providers."""
        first = next(leaves, None)
        if first is None:
            return []
        small.meet(first[2])
        window = [first]
        for leaf in islice(leaves, self._window() - 1):
            small.meet(leaf[2])  # each as it is reached, before the walk fetches the nodes above the next
            window.append(leaf)
        return window

    def _window(self) -> int:
        """Chunks a byte-range read fetches at a time: WINDOW if a usable provider is reached over a fast link
        (not _slow), else SLOW_WINDOW, with as many asked for ahead. With every provider over a slow link (or none
        known yet: say, the root and file nodes were stored already), a large window would hold the reader's first
        byte back for hours, while the providers can't send any faster anyway (they share one request per slow
        interface, _slot)."""
        if any(not self._slow(provider.node_id, provider.path) for provider in self._usable()):
            return WINDOW
        return SLOW_WINDOW

    def _read_window(self, window: list[tuple[Expect, int, int]],
                     ahead: list[tuple[Expect, int, int]]) -> Iterator[bytes]:
        """Fetch a window of chunks in one go, asking for ahead's (the next window's) once nothing of it is left to
        ask for, then yield the range within each, in order."""
        self.ensure([expect for expect, _, _ in window], ahead=[expect for expect, _, _ in ahead])
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
                      budget: _DagBudget) -> Iterator[tuple[Expect, int, int]]:
        """The raw chunks covering a range, in order, each with the range within it, depth-first; the DAG
        nodes below each node are fetched together when it is reached, counted against budget, the read_range
        call's (the stack holds at most MAX_DEPTH × MAX_CHILDREN items)."""
        stack: list[tuple[Expect, int, int]] = [(target, offset, length)]
        while stack:
            expect, child_offset, child_length = stack.pop()
            if expect.cid.is_raw:
                yield expect, child_offset, child_length
                continue
            parts = walker.range_children(expect, self.get(expect), child_offset, child_length)
            # Passed, not kept on the fetch: another read_range of this fetch may run between this generator's steps
            self.ensure([child for child, _, _ in parts if not child.cid.is_raw], budget=budget)
            stack.extend(reversed(parts))

    # Getting blocks

    def ensure(self, expects: list[Expect], budget: _DagBudget | None = None, ahead: list[Expect] = (),
               walk: _Walk | None = None) -> dict[Expect, list[Expect]]:
        """Make sure every block of expects is stored and passes its reader checks; their children's expects.

        Their blocks are counted against budget (a read_range call's), by default the fetch's own. The blocks of
        ahead (a read_range's next window) are asked for once nothing of expects is left to ask for, and stored
        as they arrive, during this call or a later one, but not waited for; one stored already, denied, or not
        passing its CID check is left out, to be met when it is ensured. Under walk (fetch_dag's or
        fetch_listing's; not with ahead), each block met adds the children walk.met returns, ensured in the same
        call, and nothing is returned.
        Raises InvalidDag, NotFound when no provider can supply a block, StorageFull, TooLarge.
        """
        with self._checking():
            return self._ensure(expects, self.budget if budget is None else budget, ahead, walk)

    def _ensure(self, expects: list[Expect], budget: _DagBudget, ahead: list[Expect] = (),
                walk: _Walk | None = None) -> dict[Expect, list[Expect]]:
        """ensure(), untagged."""
        self._check_cancelled()
        children: dict[Expect, list[Expect]] = {}
        wanted: dict[Cid, list[Expect]] = {}
        self._meet(expects, wanted, children, budget, walk)
        prefetch: dict[Cid, list[Expect]] = {}
        for expect in ahead:
            cid = expect.cid
            if cid in wanted or self.storage.has(cid) or self.storage.deny.denies(cid):
                continue
            try:
                walker.check_cid(expect)
            except InvalidDag:
                continue
            prefetch.setdefault(cid, []).append(expect)
        if wanted or prefetch:
            self._download(wanted, children, budget, prefetch, walk)
            self.fetcher.after_store()
        return children

    def _meet(self, expects, wanted: dict[Cid, list[Expect]], children: dict, budget: _DagBudget,
              walk: _Walk | None) -> list[Cid]:
        """Check each of expects before fetching or reading it (the deny list, its CID) and, if it is stored,
        against its reader checks (counted against budget), its children going into children; the rest go into
        wanted. Under walk, a stored block's children walk.met returns are met in turn, and children isn't kept.
        Returns the CIDs newly wanted, in the order met."""
        queue = deque(dict.fromkeys(expects))
        new = []
        while queue:
            expect = queue.popleft()
            self.fetcher.check_denied(expect.cid)  # before fetching or reading it, stored or not
            walker.check_cid(expect)  # before fetching a block that can't be right
            checked = self._check_stored(expect, budget)
            if checked is None:
                if expect.cid not in wanted:
                    new.append(expect.cid)
                wanted.setdefault(expect.cid, []).append(expect)
                continue
            if self.observer is not None:
                self.observer.block(expect.cid, self.storage.block_size(expect.cid) or 0, False)
            if walk is None:
                children[expect] = checked
            else:
                queue.extend(walk.met(expect, checked))
        return new

    def _check_stored(self, expect: Expect, budget: _DagBudget) -> list[Expect] | None:
        """Children of a stored block after checking it against expect (counted against budget), or None if it
        isn't stored.

        A stored block failing the checks is verified against its CID: if it no longer matches (damaged on
        disk), it is deleted and None returned, so it is fetched again rather than the root marked invalid.
        """
        try:
            if expect.cid.is_raw:
                length = self.storage.block_size(expect.cid)
                if length is None:
                    return None
                walker.check_raw(expect, length)  # stored blocks were verified, so their length is enough
                budget.count(expect.cid, length)
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
        budget.count(expect.cid, len(block), result)
        return result

    def dag_limit(self) -> int | None:
        """Bytes the distinct blocks of the DAG may hold (_DagBudget.limit), None if no limit applies."""
        return self.budget.limit()

    def _accept(self, cid: Cid, block: bytes, wanted: dict[Cid, list[Expect]], children: dict, budget: _DagBudget):
        """Check a verified block against every expectation of it and the limits (budget's among them), then
        store it.

        Raises InvalidDag, TooLarge.
        """
        for expect in wanted[cid]:
            children[expect] = walker.expand(expect, block)
        if not cid.is_raw:
            limit = self.config.max_download_size
            if limit is not None and self.node_bytes + len(block) > limit:
                raise TooLarge(f"{self.root}'s DAG nodes would exceed max_download_size ({limit})")
            self.node_bytes += len(block)
        budget.count(cid, len(block), children[wanted[cid][0]])
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

    def _download(self, wanted: dict[Cid, list[Expect]], children: dict, budget: _DagBudget,
                  ahead: dict[Cid, list[Expect]] | None = None, walk: _Walk | None = None):
        """Fetch the wanted blocks from providers until all are stored, counted against budget. Raises NotFound if
        none can.

        ahead's blocks (ensure's) are asked for after wanted's and stored as they arrive, but not waited for:
        whatever of them is still to ask for once wanted's are stored goes out to the providers idle by then,
        before returning, so they keep working while the caller uses wanted's. Under walk, each block stored adds
        its children still to fetch (_meet) to wanted's, at the back of the queue.
        """
        outstanding = set(wanted)  # still to store: wanted's, then ahead's
        needed = outstanding  # what the call waits for: wanted's
        if ahead:
            needed = set(outstanding)
            for cid, expects in ahead.items():
                wanted[cid] = expects
                outstanding.add(cid)
        in_flight = self._in_flight
        # A CID that a request still in flight (an earlier call's ahead) asked for isn't asked for again: its
        # result is waited for, and requeued if it doesn't bring the block
        asked = {cid for cids, _ in in_flight.values() for cid in cids}
        pending = _Pending(cid for cid in wanted if cid not in asked)
        self._progress_at = max(self._progress_at, self.fetcher.clock())  # it may be after a pacing wait
        give_up = self.config.lookup_timeout + self.config.stall_timeout
        while needed:
            self._check_cancelled()  # each pass waits at most POLL, so a cancel takes effect promptly
            # Requests and lookups have deadlines of their own; this catches whatever hangs regardless
            if self.fetcher.clock() - max(self._progress_at, self._heard_at) > give_up:
                raise NotFound(f"fetching {self.root} made no progress for {give_up:g}s")
            self._assign(pending, in_flight, outstanding, wanted, budget)
            if not in_flight and not self._lookup_running and not self._waiting_for_busy():
                if not self._rerun_lookup():
                    raise NotFound(self._not_found_message(needed))
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
                self._handle_event(event, pending, in_flight, outstanding, wanted, children, budget, needed, walk)
        if pending and self._usable():
            # ahead's requests go out before the caller is handed wanted's blocks (never a lookup just for them)
            self._assign(pending, in_flight, outstanding, wanted, budget)

    def _handle_event(self, event: ResultEvent | ProviderEvent | LookupDone, pending: _Pending, in_flight: dict,
                      outstanding: set[Cid], wanted: dict[Cid, list[Expect]], children: dict, budget: _DagBudget,
                      needed: set[Cid] | None = None, walk: _Walk | None = None):
        """Apply one event from a request thread or the lookup thread. needed is _download's, if it isn't
        outstanding itself: the blocks stored are taken out of it too. Under walk, the children of the blocks
        stored are met, and those still to fetch queued."""
        if isinstance(event, ProviderEvent):
            self._add_provider(event.node_id)
        elif isinstance(event, LookupDone):
            self._lookup_running = False
            self._lookup_found = event.found
        elif isinstance(event, ResultEvent):
            cids = event.cids
            _, probe = in_flight.pop(event.node_id, (cids, False))
            self._sent_at.pop(event.node_id, None)
            self._expected.pop(event.node_id, None)
            abandoned = event.node_id in self._abandoned
            self._abandoned.discard(event.node_id)
            asked_now = [cid for cid in cids if cid in outstanding]
            self._handle(event.node_id, cids, event.result, probe, wanted, children, outstanding, budget,
                         event.sent_at, event.rate, counted=not abandoned)
            self._settle_duplicate(event.node_id, in_flight)
            still_asked = {cid for asked, _ in in_flight.values() for cid in asked}
            requeue = [cid for cid in cids if cid in outstanding and cid not in pending and cid not in still_asked]
            pending.push_front(requeue)  # back at the front, keeping the walk's order
            for cid in cids:
                if cid not in still_asked:
                    self._expects.pop(cid, None)
            if needed is not None and needed is not outstanding:
                needed.difference_update(cid for cid in cids if cid not in outstanding)
            if walk is not None:
                # In the order asked for, so the queue keeps the walk's order (breadth first, as met)
                met = [child for cid in asked_now if cid not in outstanding
                       for expect in wanted[cid] for child in walk.met(expect, children.pop(expect))]
                new = self._meet(met, wanted, children, budget, walk)
                outstanding.update(new)
                if needed is not None and needed is not outstanding:
                    needed.update(new)
                pending.extend([cid for cid in new if cid not in still_asked])

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
        each, best scored, then nearest and fastest first, then as found (section 6)."""
        return sorted((p for p in self._providers.values() if not p.failed),
                      key=lambda p: self._rank(p.node_id, p.path, p.order))

    def _rank(self, node_id: bytes, path: mesh.PathInfo, order: int) -> tuple:
        """Sort key for a provider: seniority stays first (an attacker's fresh identities can't jump the
        queue by being near, or by serving popular blocks), then local score band, then hops and next-hop
        bitrate, then order."""
        senior = self.fetcher.routing.is_senior(node_id, self.config.provider_seniority)
        return mesh.provider_rank(senior, path, order, self.fetcher.scores.band(node_id))

    def _path(self, node_id: bytes) -> mesh.PathInfo:
        """What RNS knows about the path to node_id (nothing if the lookup fails)."""
        try:
            return self.fetcher.path_info(node_id)
        except Exception:  # noqa: BLE001 - RNS internals: no path known if they fail
            return mesh.NO_PATH

    def _paths(self, node_ids: list[bytes]) -> dict[bytes, mesh.PathInfo]:
        """_path of each of node_ids, looked up together (fetcher.path_infos)."""
        try:
            found = self.fetcher.path_infos(node_ids)
        except Exception:  # noqa: BLE001 - RNS internals: no path known if they fail
            found = {}
        return {node_id: found.get(node_id) or self._path(node_id) for node_id in node_ids}

    def _waiting_for_busy(self) -> bool:
        """True if a usable provider is only waiting out a busy delay."""
        now = self.fetcher.clock()
        return any(p.busy_until > now for p in self._providers.values() if not p.failed)

    def _wait_time(self) -> float:
        """How long to wait for an event: until the next busy provider may be asked again, at most POLL."""
        now = self.fetcher.clock()
        waits = [p.busy_until - now for p in self._providers.values() if not p.failed and p.busy_until > now]
        return max(0.01, min([POLL, *waits]))

    def _assign(self, pending: _Pending, in_flight: dict, outstanding: set[Cid], wanted: dict[Cid, list[Expect]],
                budget: _DagBudget):
        """Send requests to idle providers, after finding providers if there are none."""
        now = self.fetcher.clock()
        usable = self._usable()
        if not usable and not in_flight:
            self._discover(pending, in_flight)
            return
        busy_providers = sum(1 for _, probe in in_flight.values() if not probe)
        # The slow interfaces a request is in flight over: providers behind one share it, one request at a time
        slots = {self._slot(self._providers[node_id]) for node_id, (_, probe) in in_flight.items()
                 if not probe and node_id in self._providers}
        for provider in usable:
            if not pending or busy_providers >= self.fetcher.max_parallel:
                break
            if provider.node_id in in_flight or provider.busy_until > now:
                continue
            slot = self._slot(provider)
            if slot is not None and slot in slots:
                continue
            batch = self._take_batch(pending, provider.node_id, wanted, budget)
            if batch:
                in_flight[provider.node_id] = (batch, False)
                for cid in batch:
                    self._expects[cid] = wanted[cid]
                self._sent_at[provider.node_id] = now
                self._expected[provider.node_id] = self._expected_time(provider.node_id, batch, wanted)
                self._send(provider.node_id, batch)
                busy_providers += 1
                slots.add(slot)
        if not pending and busy_providers < self.fetcher.max_parallel:
            self._duplicate(usable, in_flight, busy_providers, now)

    def _duplicate(self, usable: list[_Provider], in_flight: dict, busy_providers: int, now: float):
        """End-game duplication (NET-4; section 7, item 7): with nothing left to ask for, ask idle providers on
        fast links too for the batches others have had in flight for longer than deadline_slack and DUPLICATE_AFTER
        times their expected time (_expected_time, if their link has a rate estimate), oldest first, each at most
        once, and within max_parallel requests in flight. Nothing is duplicated over a slow link (_slow): it would
        spend the radio's airtime twice. A duplicate is sized for its own provider's link and what one response
        carries (_take_batch)."""
        idle = [provider for provider in usable if provider.node_id not in in_flight and provider.busy_until <= now
                and not self._slow(provider.node_id, provider.path)]
        if not idle:
            return
        slow = sorted((sent_at, node_id) for node_id, sent_at in self._sent_at.items()
                      if node_id in in_flight and now - sent_at > self._duplicate_after(node_id)
                      and node_id not in self._duplicated and node_id not in self._duplicates
                      and node_id not in self._abandoned)
        for sent_at, node_id in slow:
            if not idle or busy_providers >= self.fetcher.max_parallel:
                return
            # Its blocks not stored yet, as they were asked for (every CID in flight has its expectations here)
            left = [cid for cid in in_flight[node_id][0] if cid in self._expects and not self.storage.has(cid)]
            if not left:
                continue
            provider = idle[0]
            # Not reserved against the DAG limits again: a block is counted once, whichever copy brings it
            batch = self._take_batch(_Pending(left), provider.node_id, self._expects, None)
            if not batch:
                continue
            idle.pop(0)
            log(f"{node_name(node_id)} has been sending {plural(len(left), 'block')} of {self.root} for "
                f"{now - sent_at:.0f}s; asking {node_name(provider.node_id)} too", LOG_VERBOSE)
            in_flight[provider.node_id] = (batch, False)
            self._sent_at[provider.node_id] = now
            self._expected[provider.node_id] = self._expected_time(provider.node_id, batch, self._expects)
            self._duplicated[node_id] = provider.node_id
            self._duplicates[provider.node_id] = node_id
            self._send(provider.node_id, batch)
            busy_providers += 1

    def _duplicate_after(self, node_id: bytes) -> float:
        """Seconds node_id's request in flight must have taken before it is duplicated: deadline_slack, or
        DUPLICATE_AFTER times its expected time if longer."""
        expected = self._expected.get(node_id)
        return max(self.config.deadline_slack, DUPLICATE_AFTER * expected if expected is not None else 0.0)

    def _slow(self, node_id: bytes, path: mesh.PathInfo) -> bool:
        """True if node_id is reached over a slow link: by the rate estimate of the link to it, if it has one (below
        slow_bitrate); else if the next-hop interface on the path to it runs below slow_bitrate, or it is behind a
        slow path (fetcher.slow_path). On a slow node, every link is."""
        if self.fetcher.slow_node:
            return True
        rate = self.client.rate_estimate(node_id)
        if rate is not None:
            return rate < self.config.slow_bitrate
        if path.bitrate is not None and path.bitrate < self.config.slow_bitrate:
            return True
        try:
            return bool(self.fetcher.slow_path(node_id))
        except Exception:  # noqa: BLE001 - RNS internals and traffic counters: not known to be slow if they fail
            return False

    def _slot(self, provider: _Provider) -> tuple | None:
        """What provider's requests share, one at a time, with those of other providers: its next-hop interface if
        its link is slow (_slow; an unknown interface counts as one), so providers behind one slow interface don't
        contend for its channel; None for a fast link (a slot of its own)."""
        if not self._slow(provider.node_id, provider.path):
            return None
        return ("slow", provider.path.interface)

    def _settle_duplicate(self, node_id: bytes, in_flight: dict):
        """node_id's request has been answered: if it was one of a duplicated pair and the other copy, still in
        flight, now carries nothing not stored, close that one's link (the first verified copy won), unless another
        fetch has a request on it: then it is left to finish. Either way its outcome isn't counted against its
        provider (_handle's counted). Closing the link stops the response without cancelling its Resource
        (section 3)."""
        other = self._duplicated.pop(node_id, None)
        if other is not None:
            self._duplicates.pop(other, None)
        else:
            other = self._duplicates.pop(node_id, None)  # the original stays marked as duplicated until answered
        if other is None or other not in in_flight or other in self._abandoned:
            return
        if any(not self.storage.has(cid) for cid in in_flight[other][0]):
            return  # it still carries blocks nobody else has sent
        self._abandoned.add(other)
        log(f"{node_name(node_id)} sent what {node_name(other)} was still sending; closing that link unless "
            "another fetch uses it", LOG_VERBOSE)
        self._close_link(other)

    def _close_link(self, node_id: bytes):
        """Close the link of this fetch's request to node_id, unless another fetch has a request on it."""
        try:
            if self.client.close_unless_used(node_id, set(self._threads)):
                self._links_closed.add(node_id)
            else:
                log(f"leaving the request to {node_name(node_id)} to finish: another fetch uses its link", LOG_VERBOSE)
        except Exception as e:  # noqa: BLE001 - closing is best effort; logged
            log(f"could not close the link to {node_name(node_id)}: {e}", LOG_WARNING)

    def _take_batch(self, pending: _Pending, node_id: bytes, wanted: dict[Cid, list[Expect]],
                    budget: _DagBudget | None) -> list[Cid]:
        """The next CIDs to ask node_id for: what one response can carry and one request packet on its link can
        hold, at least one.

        Under a DAG limit, only what the room budget leaves can take for sure (_DagBudget.room), but at least one
        CID when no request is in flight: so a fetch downloads at most about one block beyond the limit. With no
        budget (a duplicate's, whose blocks are reserved already), no limit is applied.
        """
        rate = self.client.rate_estimate(node_id)
        max_bytes = rates.requester_max_bytes(rate, self.config)
        # However large max_bytes is, the responder's budget is at most this (section 7); its upload cap isn't
        # known here, and a lower one only cuts the response short (the rest comes back unsent)
        carried = rates.response_capacity(max_bytes, rate, None, self.config.response_target)
        # The client sends what fits its link's packet and returns the rest: the open link's MDU, else (no link
        # open yet) the largest request any link allows. Whether a prefix fits only gets worse as it grows, so
        # one count covers every batch taken from the front of pending
        mdu = self.client.link_mdu(node_id) or MAX_REQUEST_SIZE
        fit = max(1, protocol.blocks_batch_size(max_bytes, list(islice(pending, MAX_BATCH)), mdu))
        room = None if budget is None else budget.room((cid for cids, _ in self._in_flight.values() for cid in cids),
                                                       wanted)
        batch: list[Cid] = []
        total = 0
        while pending and len(batch) < fit:
            cid = pending[0]
            guess = self._guess(cid, wanted)
            if batch and total + guess > carried:
                break
            if room is not None:
                room = budget.reserve(cid, wanted, room)
                if not budget.fits(room) and (batch or self._in_flight):
                    break
            batch.append(pending.popleft())
            total += guess
        return batch

    @staticmethod
    def _guess(cid: Cid, wanted: dict[Cid, list[Expect]]) -> int:
        """The bytes a block not yet received is taken to have when sizing a batch: a raw chunk's declared size,
        else CHUNK_SIZE for a raw chunk and DAG_NODE_GUESS for a DAG node."""
        known = wanted[cid][0].size if cid.is_raw and cid in wanted else None
        return known if known is not None else (CHUNK_SIZE if cid.is_raw else DAG_NODE_GUESS)

    def _expected_time(self, node_id: bytes, batch: list[Cid], wanted: dict[Cid, list[Expect]]) -> float | None:
        """Seconds batch's blocks (as _guess takes them, at most what one response carries beyond its first) take at
        the rate estimate of the link to node_id; None if it has none."""
        rate = self.client.rate_estimate(node_id)
        if not rate:
            return None
        carried = rates.response_capacity(rates.requester_max_bytes(rate, self.config), rate, None,
                                          self.config.response_target)
        size = sum(self._guess(cid, wanted) for cid in batch)
        return rates.transfer_seconds(min(size, max(carried, self._guess(batch[0], wanted))), rate)

    def _send(self, node_id: bytes, cids: list[Cid]):
        """Ask node_id for cids on a request thread, which queues the result."""
        self._links_closed.discard(node_id)  # a link closed before (a lost request's) isn't this request's
        def progress(fraction: float):
            """The response is arriving: the fetch isn't hung (on the request thread, so only _heard_at is written)."""
            self._heard_at = self.fetcher.clock()

        def work():
            """Run the blocking request and queue its result, with when it was sent and the rate estimate
            it was sent with (the responder's upload cap derives from the same response)."""
            sent_at = self.fetcher.clock()
            rate = None
            ident = threading.get_ident()
            self._threads.add(ident)  # until its request is over: the link's users that are this fetch's
            try:
                rate = self.client.rate_estimate(node_id)
                result = self.client.blocks(node_id, cids, on_progress=progress)
            except Exception as e:  # noqa: BLE001 - any client failure is a failed request, retried elsewhere; logged
                # The client reports network failures in its result, so this is a bug: logged here, inside the
                # except, so the debug log has its traceback
                log(f"/blocks to {node_name(node_id)} raised {e!r}", LOG_WARNING)
                result = peer_client.BlocksResult(peer_client.FAILED, unsent=list(cids), reason=str(e))
            finally:
                self._threads.discard(ident)
            if result.elapsed is not None:
                # When it actually went out: opening the link or waiting for its /blocks slot isn't its time
                sent_at = max(sent_at, self.fetcher.clock() - result.elapsed)
            self._events.put(ResultEvent(node_id, cids, result, sent_at, rate))

        threading.Thread(target=work, name="rnfs-fetch", daemon=True).start()

    # Finding providers

    def _discover(self, pending: _Pending, in_flight: dict):
        """No usable provider: ask nodes with open links, else start the lookup."""
        if not self._probed:
            self._probed = True
            banned = self.fetcher.routing.is_banned
            hinted = [n for n in [self.hint] if n is not None and n != self.fetcher.own_id and not banned(n)]
            linked = [n for n in self.client.open_links() if n != self.fetcher.own_id and not banned(n)
                      and n not in hinted]
            # Ranked like providers; most recently used breaks ties. Their paths in one go: attached to a shared
            # instance, that is one RPC rather than one per link before the first request (plan C1). A node that
            # isn't slow probes none over a slow link: each would hold the lookup up by a radio round trip, and
            # several would each send the root. A slow node, whose every link is slow, keeps its one probe (it
            # probes max_parallel links): one that hits saves a whole lookup over the radio
            paths = self._paths(linked)
            if not self.fetcher.slow_node:
                linked = [node_id for node_id in linked if not self._slow(node_id, paths[node_id])]
            # The hint goes first, taking one of the probe slots, over any link: one request that saves a lookup,
            # to the one node known to hold the content before its records are out (a remote pin's asker)
            ranked = sorted(enumerate(linked), key=lambda item: self._rank(item[1], paths[item[1]], item[0]))
            candidates = (hinted + [node_id for _, node_id in ranked])[:self.fetcher.max_parallel]
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
                found = lookup.find_providers(lambda node_id: self._events.put(ProviderEvent(node_id)), should_stop)
            except Exception as e:  # noqa: BLE001 - a failed lookup ends with the providers found so far
                log(f"provider lookup for {self.root} failed: {e}", LOG_WARNING)
            self._events.put(LookupDone(len(found)))

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

    def _served(self, node_id: bytes):
        """node_id delivered verified blocks: its score rises, and so, once per fetch, do those of the nodes whose
        /find answers listed it (local scores, section 6)."""
        scores = self.fetcher.scores
        scores.served(node_id)
        if self._lookup is None:
            return
        for lister in self._lookup.listers(node_id) - self._credited:
            self._credited.add(lister)
            scores.useful_answer(lister)

    def _fail(self, provider: _Provider, why: str):
        """A provider failed: it isn't used again during this fetch, and its score falls."""
        if not provider.failed:
            provider.failed = True
            self.fetcher.scores.failed(provider.node_id)
            self._live -= 1
            log(f"provider {node_name(provider.node_id)} failed for {self.root}: {why}", LOG_VERBOSE)

    def _handle(self, node_id: bytes, cids: list[Cid], result, probe: bool, wanted: dict, children: dict,
                outstanding: set[Cid], budget: _DagBudget, sent_at: float | None = None, rate: float | None = None,
                counted: bool = True):
        """Store a response's blocks (counted against budget) and apply the provider failure and pacing rules.
        Raises InvalidDag.

        sent_at is when the request was sent and rate the link's rate estimate then (None: unknown). A request
        not counted (one whose link the fetch closed, as its duplicate won) has its blocks stored, but its outcome
        applies none of the rules: closing the link isn't the provider's stall or failure.
        """
        provider = self._providers.get(node_id)
        if result.bad:
            # Not a lossy link: the node sent something other than what was asked for (section 10)
            log(f"banning {node_name(node_id)}: it sent {plural(len(result.bad), 'block')} not matching "
                "the CID requested", LOG_WARNING)
            self.fetcher.routing.ban(node_id)
            self.fetcher.scores.bad_block(node_id)
            if provider is None:
                provider = self._new_provider(node_id)
            self._fail(provider, "sent a block not matching its CID")
        new = {cid: block for cid, block in result.blocks.items() if cid in outstanding}
        # Asked for by an earlier _download call (a read_range's next window, while its walk fetched a file node):
        # checked against what it was asked for as and stored, not dropped and asked for again
        late = {cid: block for cid, block in result.blocks.items()
                if cid not in outstanding and cid in self._expects and not self.storage.has(cid)}
        self._make_room(sum(len(block) for cid, block in (new | late).items() if not self.storage.has(cid)))
        for cid, block in new.items():
            self._accept(cid, block, wanted, children, budget)
            outstanding.discard(cid)
        for cid, block in late.items():
            self._accept(cid, block, self._expects, {}, self.budget)
        if result.blocks:
            self._served(node_id)
        if probe:
            # A node with an open link that has the block becomes a provider; one that hasn't isn't penalised
            if result.blocks and provider is None:
                provider = self._new_provider(node_id)
                log(f"{node_name(node_id)} (open link) has {self.root}", LOG_VERBOSE)
                self._pace(provider, sent_at, rate)
            return
        if provider is None or provider.failed or not counted:
            return
        now = self.fetcher.clock()
        if result.status != peer_client.LOST:
            provider.lost = 0  # it answered (or failed outright)
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
        elif result.status == peer_client.LOST:
            # A lost packet, most likely (at 5% packet loss, more than half of 1 MiB fetches lose a request): asked
            # again on a new link after the busy delay. The link is closed first, so a response still being
            # sent on it stops (a link closing starts no cooldown, section 3) and the next isn't refused for it
            provider.lost += 1
            self._close_link(node_id)
            if provider.lost >= self.config.max_failures:
                self._fail(provider, f"{result.reason or 'no answer'} {provider.lost} times in a row")
            else:
                provider.busy_until = now + self.fetcher.busy_delay
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
        # Refusals until 4 × took after sending count as pacing: at the default cap the next response may start
        # 2 × took after the last one did (rates.UPLOAD_CAP_DIVISOR), and twice that leaves room for a responder
        # whose rate estimate or cap is lower than ours and the default. Retrying them every eighth of took loses
        # at most that much once the responder is ready, for one refusal (a request packet) each time
        provider.pacing_until = sent_at + 4 * took
        provider.pacing_retry = max(self.fetcher.busy_delay, took / 8)
        if rate is not None and rate < self.config.slow_bitrate:
            # At the default cap (rates.default_upload_cap) the next response may start once the last one's
            # transfer time has passed UPLOAD_CAP_DIVISOR times since it started: took has passed once already
            provider.busy_until = max(provider.busy_until, now + (rates.UPLOAD_CAP_DIVISOR - 1) * took)
            self._progress_at = max(self._progress_at, provider.busy_until)  # waiting isn't hanging
            # Waiting is using the link: on LoRa a 64 KiB chunk takes about 9 minutes, and so does the wait, longer
            # than link_idle_timeout; a link swept meanwhile would take its rate estimate, and the pacing, with it
            self.client.hold_link(provider.node_id, provider.busy_until - now)


def describe(fetch: Fetch) -> str:
    """A log line for a finished fetch."""
    return (f"fetched {plural(fetch.downloaded_blocks, 'block')} ({fetch.downloaded_bytes} bytes) of {fetch.root}"
            + (f" from {plural(len(fetch._providers), 'provider')}" if fetch._providers else ""))

