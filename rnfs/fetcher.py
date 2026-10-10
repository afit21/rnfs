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
   for over provider_seniority are tried first, the rest in reply order.
`get --from` uses only the given node.

A provider fails, and isn't used again during the fetch, when its link fails, it answers None for a block
of the root it advertised, it is busy max_failures times in a row (each retried after BUSY_DELAY), or it
stalls twice. When no providers are left, the lookup runs again for a fresh random half of providers,
within the same lookup_timeout. A block not matching its CID bans the provider; a correctly hashed block
that isn't a valid DAG node marks the root invalid locally and fails the fetch.

Requests are spread over up to max_parallel_providers providers (1 on slow nodes), one /blocks request in
flight per provider. Request threads only call the client; all fetch state is changed by the thread that
runs the fetch, from the events they queue.
"""
import queue
import threading
import time
from collections import deque
from typing import Callable, Iterator

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE, KIB, MAX_REQUEST_SIZE
from rnfs.content import walker
from rnfs.content.cid import Cid
from rnfs.content.storage import Storage, read_json, write_json
from rnfs.content.walker import Expect
from rnfs.errors import InvalidDag, NotFound, TooLarge
from rnfs.log import LOG_VERBOSE, LOG_WARNING, log, node_name, plural
from rnfs.network import client as peer_client
from rnfs.network import protocol, rates

BUSY_DELAY = 2.0  # seconds before a busy provider is asked again
MAX_STALLS = 2  # a provider that stalls this often fails
WINDOW = 32  # chunks fetched ahead of what a byte-range read has written out
POLL = 0.5  # seconds between checks while waiting for responses
DAG_NODE_GUESS = 8 * KIB  # assumed size of a DAG node block when reserving a request's batch
MAX_BATCH = 32  # CIDs reserved for one request at most; the client sends as many as fit one packet
INVALID_FILE = "invalid.json"


class _Provider:
    """What a fetch knows about one provider."""

    __slots__ = ("node_id", "order", "busy", "busy_until", "stalls", "failed")

    def __init__(self, node_id: bytes, order: int):
        """A provider found order-th."""
        self.node_id = node_id
        self.order = order
        self.busy = 0  # busy refusals in a row
        self.busy_until = 0.0
        self.stalls = 0
        self.failed = False


class Fetcher:
    """Creates fetches and remembers roots found invalid. Thread-safe."""

    def __init__(self, storage: Storage, client, config: Config, own_id: bytes, routing,
                 make_lookup: Callable[[bytes], object], slow_node: bool = False, state_dir: str | None = None,
                 after_store: Callable[[], None] | None = None, clock: Callable[[], float] = time.monotonic,
                 busy_delay: float = BUSY_DELAY):
        """Fetch into storage with client (PeerClient); routing is the RoutingTable (bans, seniority).

        make_lookup(key) returns a dht.lookup.Lookup for key; after_store() runs after each batch of stored
        blocks, so the node can enforce max_storage.
        """
        self.storage = storage
        self.client = client
        self.config = config
        self.own_id = own_id
        self.routing = routing
        self.make_lookup = make_lookup
        self.slow_node = slow_node
        self.after_store = after_store or (lambda: None)
        self.clock = clock
        self.busy_delay = busy_delay
        self._lock = threading.Lock()
        self._invalid_path = None if state_dir is None else f"{state_dir.rstrip('/')}/{INVALID_FILE}"
        self._invalid: set[Cid] = set()
        if self._invalid_path is not None:
            for text in read_json(self._invalid_path) or []:
                try:
                    self._invalid.add(Cid.parse(text))
                except (ValueError, TypeError):
                    continue

    @property
    def max_parallel(self) -> int:
        """Providers used at a time: max_parallel_providers, 1 on slow nodes by default."""
        return max(1, self.config.effective_max_parallel_providers(self.slow_node))

    def open(self, root: Cid, names: list[str] | tuple[str, ...] = (), from_node: bytes | None = None,
             pinned: bool = False) -> "Fetch":
        """A fetch of <root>/<names>, to use as `with fetcher.open(...) as fetch:`.

        Raises InvalidDag if root was found invalid before. A fetch that isn't being pinned raises
        StorageFull before its first download if pins alone fill max_storage.
        """
        if self.is_invalid(root):
            raise InvalidDag(f"{root} was found to be an invalid DAG")
        return Fetch(self, root, list(names), from_node, pinned)

    def is_invalid(self, root: Cid) -> bool:
        """True if root was marked invalid locally."""
        with self._lock:
            return root in self._invalid

    def mark_invalid(self, root: Cid):
        """Remember that root's DAG broke a reader check, so it isn't fetched again."""
        with self._lock:
            if root in self._invalid:
                return
            self._invalid.add(root)
            if self._invalid_path is not None:
                write_json(self._invalid_path, sorted(str(cid) for cid in self._invalid))
        log(f"marked {root} invalid locally", LOG_WARNING)


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
        self.downloaded_blocks = 0
        self.downloaded_bytes = 0
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
        """Stop the lookup and end the session; an InvalidDag marks the root invalid."""
        self.closed = True
        if self._lookup is not None:
            self._lookup.stop()
        if exc_type is not None and issubclass(exc_type, InvalidDag):
            self.fetcher.mark_invalid(self.root)
        self._protection.__exit__(exc_type, exc, tb)
        return False

    def get(self, expect: Expect) -> bytes:
        """A walker get over stored blocks (protected by this fetch), raising NotFound for a missing block."""
        block = self.storage.get(expect.cid)
        if block is None:
            raise NotFound(f"block {expect.cid} is not stored")
        return block

    # Traversals

    def resolve(self) -> Expect:
        """Fetch the root block and the blocks along the path; the target's expectation (its block stored)."""
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
        return walker.size_of(expect, self.get(expect))

    def check_size(self, size: int):
        """Raise TooLarge if a fetch of size content bytes would exceed max_download_size."""
        limit = self.config.max_download_size
        if limit is not None and size > limit:
            raise TooLarge(f"{size} bytes would exceed max_download_size ({limit})")

    def fetch_dag(self, target: Expect):
        """Fetch every block of target's DAG not already stored, level by level, checking each."""
        self.ensure([target])
        self.check_size(self.size_of(target))
        # The root's own DAG, already complete, was checked from the same expectation when it was memoised
        if target == walker.root_expect(self.root) and self.storage.is_complete(target.cid):
            return
        seen = {target}
        frontier = [target]
        while frontier:
            children = self.ensure(frontier)
            next_frontier = []
            for expect in frontier:
                for child in children[expect]:
                    if child not in seen:
                        seen.add(child)
                        next_frontier.append(child)
            frontier = next_frontier

    def fetch_listing(self, target: Expect):
        """Fetch the directory and index nodes of the directory target (not its entries' contents)."""
        frontier = [target]
        seen = {target}
        while frontier:
            children = self.ensure(frontier)
            # Only an index's children carry first_name; a directory's entries and a file's chunks don't
            frontier = [child for expect in frontier for child in children[expect]
                        if walker.is_index_step(child) and child not in seen]
            seen.update(frontier)

    def read_range(self, target: Expect, offset: int = 0, length: int | None = None) -> Iterator[bytes]:
        """The bytes of the file target in [offset, offset + length), fetching only the chunks covering them
        and the nodes above, WINDOW chunks at a time. Raises ValueError for a directory."""
        if offset < 0 or (length is not None and length < 0):
            raise ValueError("offset and length must be non-negative")
        self.ensure([target])
        if walker.kind_of(target, self.get(target)) != "file":
            raise ValueError(f"{target.cid} is a directory")
        size = self.size_of(target)
        end = size if length is None else min(size, offset + length)
        if offset >= end:
            return
        self.check_size(end - offset)
        leaves = self._range_leaves(target, offset, end - offset)
        for start in range(0, len(leaves), WINDOW):
            window = leaves[start:start + WINDOW]
            self.ensure([expect for expect, _, _ in window])
            for expect, child_offset, child_length in window:
                block = self.get(expect)
                walker.check_raw(expect, len(block))
                yield block[child_offset:child_offset + child_length]

    def _range_leaves(self, target: Expect, offset: int, length: int) -> list[tuple[Expect, int, int]]:
        """The raw chunks covering a range, in order, each with the range within it; fetches the nodes above."""
        items: list[tuple[Expect, int, int]] = [(target, offset, length)]
        while any(not expect.cid.is_raw for expect, _, _ in items):
            self.ensure([expect for expect, _, _ in items if not expect.cid.is_raw])
            expanded = []
            for expect, child_offset, child_length in items:
                if expect.cid.is_raw:
                    expanded.append((expect, child_offset, child_length))
                else:
                    expanded.extend(walker.range_children(expect, self.get(expect), child_offset, child_length))
            items = expanded
        return items

    # Getting blocks

    def ensure(self, expects: list[Expect]) -> dict[Expect, list[Expect]]:
        """Make sure every block of expects is stored and passes its reader checks; their children's expects.

        Raises InvalidDag, NotFound when no provider can supply a block, StorageFull, TooLarge.
        """
        children: dict[Expect, list[Expect]] = {}
        wanted: dict[Cid, list[Expect]] = {}
        for expect in dict.fromkeys(expects):
            walker.check_cid(expect)  # before fetching a block that can't be right
            checked = self._check_stored(expect)
            if checked is None:
                wanted.setdefault(expect.cid, []).append(expect)
            else:
                children[expect] = checked
        if wanted:
            if self.downloaded_blocks == 0:
                self.storage.check_space(self.pinned)  # only now: content already stored is always served
            self._download(wanted, children)
            self.fetcher.after_store()
        return children

    def _check_stored(self, expect: Expect) -> list[Expect] | None:
        """Children of a stored block after checking it against expect, or None if it isn't stored."""
        if expect.cid.is_raw:
            length = self.storage.block_size(expect.cid)
            if length is None:
                return None
            walker.check_raw(expect, length)  # stored blocks were verified, so their length is enough
            return []
        block = self.storage.get(expect.cid)
        if block is None:
            return None
        return walker.expand(expect, block)

    def _accept(self, cid: Cid, block: bytes, wanted: dict[Cid, list[Expect]], children: dict):
        """Check a verified block against every expectation of it, then store it. Raises InvalidDag."""
        for expect in wanted[cid]:
            children[expect] = walker.expand(expect, block)
        self.session.put(cid, block)
        self.downloaded_blocks += 1
        self.downloaded_bytes += len(block)
        if cid == self.root and not self.root_delivered:
            self.root_delivered = True
            if self._lookup is not None:
                self._lookup.stop()  # it may now have enough providers to stop searching

    def _download(self, wanted: dict[Cid, list[Expect]], children: dict):
        """Fetch the wanted blocks from providers until all are stored. Raises NotFound if none can."""
        outstanding = set(wanted)
        pending = deque(wanted)
        in_flight: dict[bytes, tuple[list[Cid], bool]] = {}  # provider -> (CIDs asked for, is a probe)
        while outstanding:
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
            _, provider, cids, result = event
            _, probe = in_flight.pop(provider, (cids, False))
            self._handle(provider, cids, result, probe, wanted, children, outstanding)
            requeue = [cid for cid in cids if cid in outstanding and cid not in pending
                       and not any(cid in asked for asked, _ in in_flight.values())]
            pending.extendleft(reversed(requeue))  # back at the front, keeping the walk's order

    def _not_found_message(self, outstanding: set[Cid]) -> str:
        """Why the fetch failed, for the user."""
        what = self.root if self.root in outstanding or not self._providers else f"{len(outstanding)} blocks of {self.root}"
        if self.from_node is not None:
            return f"{node_name(self.from_node)} could not supply {what}"
        if not self._providers:
            return f"no provider found for {self.root}"
        return f"no provider could supply {what} ({plural(len(self._providers), 'provider')} tried)"

    def _usable(self) -> list[_Provider]:
        """Providers not failed, in the order they're tried: long-known ones first, then the rest as found."""
        seniority = self.config.provider_seniority

        def rank(provider: _Provider):
            """Sort key: senior first, then discovery order."""
            known_for = self.fetcher.routing.seniority(provider.node_id)
            return (0 if known_for is not None and known_for >= seniority else 1, provider.order)

        return sorted((p for p in self._providers.values() if not p.failed), key=rank)

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
        """The next CIDs to ask node_id for: what its budget and one request packet can hold, at least one."""
        budget = rates.requester_max_bytes(self.client.rate_estimate(node_id), self.config)
        batch: list[Cid] = []
        total = 0
        while pending and len(batch) < MAX_BATCH:
            cid = pending[0]
            known = wanted[cid][0].size if cid.is_raw else None
            guess = known if known is not None else (CHUNK_SIZE if cid.is_raw else DAG_NODE_GUESS)
            if batch and total + guess > budget:
                break
            # The largest request any link allows; the client sends what fits its link and returns the rest
            if batch and protocol.blocks_batch_size(budget, batch + [cid], MAX_REQUEST_SIZE) <= len(batch):
                break
            batch.append(pending.popleft())
            total += guess
        return batch

    def _send(self, node_id: bytes, cids: list[Cid]):
        """Ask node_id for cids on a request thread, which queues the result."""
        def work():
            """Run the blocking request and queue its result."""
            try:
                result = self.client.blocks(node_id, cids)
            except Exception as e:
                result = peer_client.BlocksResult(peer_client.FAILED, unsent=list(cids), reason=str(e))
            self._events.put(("result", node_id, cids, result))

        threading.Thread(target=work, name="rnfs-fetch", daemon=True).start()

    # Finding providers

    def _discover(self, pending: deque, in_flight: dict):
        """No usable provider: ask nodes with open links, else start the lookup."""
        if not self._probed:
            self._probed = True
            banned = self.fetcher.routing.is_banned
            candidates = [n for n in self.client.open_links()
                          if n != self.fetcher.own_id and not banned(n)][:self.fetcher.max_parallel]
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
        provider = self._providers[node_id] = _Provider(node_id, len(self._providers))
        self._live += 1
        return provider

    def _fail(self, provider: _Provider, why: str):
        """A provider failed: it isn't used again during this fetch."""
        if not provider.failed:
            provider.failed = True
            self._live -= 1
            log(f"provider {node_name(provider.node_id)} failed for {self.root}: {why}", LOG_VERBOSE)

    def _handle(self, node_id: bytes, cids: list[Cid], result, probe: bool, wanted: dict, children: dict,
                outstanding: set[Cid]):
        """Store a response's blocks and apply the provider failure rules. Raises InvalidDag."""
        provider = self._providers.get(node_id)
        if result.bad:
            # Not a lossy link: the node sent something other than what was asked for (section 10)
            log(f"banning {node_name(node_id)}: it sent {plural(len(result.bad), 'block')} not matching "
                "the CID requested", LOG_WARNING)
            self.fetcher.routing.ban(node_id)
            if provider is None:
                provider = self._new_provider(node_id)
            self._fail(provider, "sent a block not matching its CID")
        for cid, block in result.blocks.items():
            if cid in outstanding:
                self._accept(cid, block, wanted, children)
                outstanding.discard(cid)
        if probe:
            # A node with an open link that has the block becomes a provider; one that hasn't isn't penalised
            if result.blocks and provider is None:
                self._new_provider(node_id)
                log(f"{node_name(node_id)} (open link) has {self.root}", LOG_VERBOSE)
            return
        if provider is None or provider.failed:
            return
        if result.status == peer_client.OK:
            provider.busy = 0
            if result.missing:
                self._fail(provider, f"doesn't have {plural(len(result.missing), 'block')} of the root it advertised")
        elif result.status == peer_client.BUSY:
            provider.busy += 1
            if provider.busy >= self.config.max_failures:
                self._fail(provider, f"busy {provider.busy} times in a row")
            else:
                provider.busy_until = self.fetcher.clock() + self.fetcher.busy_delay
        elif result.status == peer_client.STALLED:
            provider.stalls += 1
            if provider.stalls >= MAX_STALLS:
                self._fail(provider, f"stalled {provider.stalls} times")
        else:
            self._fail(provider, result.reason or "request failed")


def describe(fetch: Fetch) -> str:
    """A log line for a finished fetch."""
    return (f"fetched {plural(fetch.downloaded_blocks, 'block')} ({fetch.downloaded_bytes} bytes) of {fetch.root}"
            + (f" from {plural(len(fetch._providers), 'provider')}" if fetch._providers else ""))

