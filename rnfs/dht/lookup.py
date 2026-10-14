"""Iterative DHT lookups (specification section 6). No RNS dependency: network access, identity knowledge
and time are injected, so lookups run over in-memory networks in tests.

A lookup keeps up to `alpha` /find queries in flight, always to the closest known nodes not yet queried,
and merges the nodes each reply returns. It is complete once the `beta` closest nodes it knows of have all
answered, or once every known node has been queried without finding new ones; it never runs past its
`lookup_timeout` budget. Queries are blocking calls run on worker threads, so a slow or hung node never
holds the caller past the deadline.
"""
import queue
import threading
import time
from collections.abc import Callable, Iterable

from rnfs.constants import ID_LENGTH, K
from rnfs.dht.routing import distance

# query(node_id, key) -> (provider IDs, node IDs), or None if the node didn't answer, refused, or replied
# with garbage. May block for a long time (path request, link setup, RNS's request timeout).
FindQuery = Callable[[bytes, bytes], tuple[list[bytes], list[bytes]] | None]
NodeCallback = Callable[[bytes], object]  # return value ignored

_POLL_INTERVAL = 0.5  # seconds between should_stop() checks while waiting for replies
_STOP = object()  # queued by stop() to wake the waiting caller


def _ignore(node_id: bytes) -> None:
    """Default node callback: does nothing."""


def _always(node_id: bytes) -> bool:
    """Default is_known: every identity is known."""
    return True


def _never(node_id: bytes) -> bool:
    """Default exclude: nothing is excluded."""
    return False


def _valid_id(node_id) -> bool:
    """True if node_id has the shape of a node ID."""
    return isinstance(node_id, bytes) and len(node_id) == ID_LENGTH


class Lookup:
    """One lookup for key: find_closest() for publishing, find_providers() for fetching.

    - `start_nodes`: usually the routing table's closest nodes to key.
    - `query`: a FindQuery; exceptions count as no answer.
    - `is_known(id)`: whether RNS knows id's identity. Querying an unknown ID costs a path request, so at
      most `alpha` unknown IDs are tried per lookup (allow_unknown, shared with the fetcher's providers).
    - `on_answer(id)` / `on_failure(id)`: called from worker threads when a query is answered or fails,
      even after the run that sent it has returned, so the node can update its routing table.
    - `exclude(id)`: IDs to ignore in replies, as nodes and as providers (banned nodes).
    - `timeout`: the time budget, starting at the first run; later runs share it.

    A Lookup may be run again (find_providers after every provider failed): each run re-queries the nodes,
    so /find replies give a fresh random half of providers, within the same deadline and unknown-ID budget.
    Runs of one Lookup must not overlap.
    """

    def __init__(self, key: bytes, start_nodes: Iterable[bytes], query: FindQuery, own_id: bytes,
                 alpha: int = 3, beta: int = 3, k: int = K, timeout: float = 600,
                 is_known: Callable[[bytes], bool] = _always, on_answer: NodeCallback = _ignore,
                 on_failure: NodeCallback = _ignore, exclude: Callable[[bytes], bool] = _never,
                 clock: Callable[[], float] = time.monotonic, poll_interval: float = _POLL_INTERVAL):
        """A lookup for key; nothing is sent until find_closest() or find_providers() runs it."""
        self.key = key
        self.query = query
        self.own_id = own_id
        self.alpha = max(1, alpha)
        self.beta = max(1, beta)
        self.k = k
        self.timeout = timeout
        self.is_known = is_known
        self.on_answer = on_answer
        self.on_failure = on_failure
        self.exclude = exclude
        self.clock = clock
        self.poll_interval = poll_interval
        self.deadline: float | None = None
        self._failed: set[bytes] = set()  # didn't answer: dropped for the rest of the lookup
        self._answered: set[bytes] = set()  # answered in any run
        self._allowed_unknown: set[bytes] = set()  # unknown IDs that were given a path request
        self._refused_unknown: set[bytes] = set()  # unknown IDs over the budget
        self._reported: set[bytes] = set()  # providers already passed to on_provider, in any run
        self._listing: set[bytes] = set()  # nodes whose reply listed a provider, in any run
        self._last_known: dict[bytes, int] = {}  # the nodes the last run knew of and hadn't seen fail
        self._last_queried: set[bytes] = set()  # the nodes the last run queried
        self._wake: queue.Queue | None = None
        self._lock = threading.Lock()
        self._start_nodes = [n for n in dict.fromkeys(start_nodes) if self._usable(n)]

    # Public interface

    def find_closest(self) -> list[bytes]:
        """Run until complete or timed out; the k closest nodes known, closest first.

        As the spec says, these are the closest nodes known, not only those that answered: termination
        guarantees just the beta closest answered, and the stores sent to the rest show which are
        reachable. Left out: nodes that failed or were still not answering at the deadline, and
        unqueried IDs whose identity isn't known (contacting them costs a path request).
        """
        self._run(None, lambda: False)
        candidates = [n for n in self._last_known
                      if n in self._answered or (n not in self._last_queried and self.is_known(n))]
        return sorted(candidates, key=self._distance)[:self.k]

    def find_providers(self, on_provider: NodeCallback, should_stop: Callable[[], bool] = lambda: False
                       ) -> list[bytes]:
        """Run until should_stop() is True, the lookup is complete, or the deadline; returns the providers
        found by this run, in the order found.

        Each provider not reported by an earlier run is passed to on_provider as soon as its reply arrives,
        so downloading can start with the first; a reply listing providers doesn't end the search, so one
        node near the key can't hide content. on_provider and should_stop are called in the caller's thread
        and must not block. Call stop() from another thread to make the run re-check should_stop at once.
        Provider IDs are not checked against is_known: the fetcher calls allow_unknown before using one.
        """
        return self._run(on_provider, should_stop)

    def stop(self):
        """Wake a running find_providers so it checks should_stop() now rather than at its next poll."""
        wake = self._wake
        if wake is not None:
            wake.put(_STOP)

    def allow_unknown(self, node_id: bytes) -> bool:
        """True if node_id may be contacted: its identity is known, or the lookup's budget of alpha unknown
        IDs (path requests) has room. Each unknown ID is charged once; later calls for it return True."""
        if self.is_known(node_id):
            return True
        with self._lock:
            if node_id in self._allowed_unknown:
                return True
            if len(self._allowed_unknown) >= self.alpha:
                self._refused_unknown.add(node_id)
                return False
            self._allowed_unknown.add(node_id)
            return True

    def remaining(self) -> float:
        """Seconds left in the time budget (the full timeout before the first run)."""
        if self.deadline is None:
            return self.timeout
        return max(0.0, self.deadline - self.clock())

    def listing_count(self) -> int:
        """How many nodes have answered with at least one provider (in any run), so a caller can tell
        providers vouched for by several nodes from those one node listed."""
        with self._lock:
            return len(self._listing)

    def closest_answered(self) -> list[bytes]:
        """The k closest nodes that answered in any run so far, closest first."""
        with self._lock:
            answered = list(self._answered)
        return sorted(answered, key=self._distance)[:self.k]

    # The lookup itself

    def _distance(self, node_id: bytes) -> int:
        """node_id's distance to the key."""
        return distance(node_id, self.key)

    def _usable(self, node_id) -> bool:
        """True if node_id may be queried in this lookup."""
        return (_valid_id(node_id) and node_id != self.own_id and node_id not in self._failed
                and node_id not in self._refused_unknown and not self.exclude(node_id))

    def _run(self, on_provider: NodeCallback | None, should_stop: Callable[[], bool]) -> list[bytes]:
        """One pass of the iterative lookup; returns the providers it newly reported."""
        if self.deadline is None:
            self.deadline = self.clock() + self.timeout
        results: queue.Queue = queue.Queue()
        self._wake = results
        try:
            return self._iterate(results, on_provider, should_stop)
        finally:
            self._wake = None

    def _iterate(self, results: queue.Queue, on_provider: NodeCallback | None,
                 should_stop: Callable[[], bool]) -> list[bytes]:
        """The run's loop: launch queries, merge replies, check termination. Runs in the caller's thread."""
        # Nodes known to this run: the start nodes plus those that answered before, since they are
        # usually closer. Every run re-queries them, which is what gives a fresh random half of providers.
        known: dict[bytes, int] = {}  # node ID -> distance, for usable nodes
        for node_id in list(self._start_nodes) + self.closest_answered():
            if self._usable(node_id):
                known[node_id] = self._distance(node_id)
        queried: set[bytes] = set()
        answered: set[bytes] = set()
        found: list[bytes] = []
        in_flight = 0
        window = max(self.alpha, self.beta)
        self._last_known = known  # live: find_closest reads them once the run ends
        self._last_queried = queried

        while True:
            if on_provider is not None and should_stop():
                break
            if self.clock() >= self.deadline:
                break
            ranked = sorted(known, key=known.__getitem__)
            if ranked and all(node_id in answered for node_id in ranked[:self.beta]):
                break  # the beta closest known nodes have all answered: complete

            # Keep up to alpha queries in flight, to the closest nodes not yet queried. Only nodes ranked
            # within the closest max(alpha, beta) known can affect completion, so farther ones aren't
            # queried while nearer queries are pending: on slow networks every query costs airtime.
            while in_flight < self.alpha:
                ranked = sorted(known, key=known.__getitem__)[:window]
                node_id = next((n for n in ranked if n not in queried), None)
                if node_id is None:
                    break
                queried.add(node_id)
                if not self.allow_unknown(node_id):
                    del known[node_id]  # over the unknown-ID budget: dropped, but not a failure
                    continue
                self._send(node_id, results)
                in_flight += 1
            if in_flight == 0:
                break  # every known node was queried and nothing new turned up

            wait = min(self.deadline - self.clock(), self.poll_interval)
            if wait <= 0:
                break
            try:
                item = results.get(timeout=wait)
            except queue.Empty:
                continue
            if item is _STOP:
                continue
            in_flight -= 1
            node_id, reply = item
            if reply is None:
                with self._lock:
                    self._failed.add(node_id)
                known.pop(node_id, None)
                continue

            answered.add(node_id)
            with self._lock:
                self._answered.add(node_id)
            providers, nodes = reply
            if providers:
                with self._lock:
                    self._listing.add(node_id)
            for new_id in nodes:
                if new_id not in known and new_id not in queried and self._usable(new_id):
                    known[new_id] = self._distance(new_id)
            if on_provider is not None:
                for provider in providers:
                    if provider in self._reported or provider == self.own_id or self.exclude(provider):
                        continue
                    self._reported.add(provider)
                    found.append(provider)
                    on_provider(provider)
        return found

    def _send(self, node_id: bytes, results: queue.Queue):
        """Query node_id on a worker thread, which reports the outcome and queues it for the run."""
        def work():
            """Run the blocking query, report it, and hand the cleaned reply to the run."""
            try:
                reply = self._clean(self.query(node_id, self.key))
            except Exception:
                reply = None  # one broken node must not abort the lookup
            try:
                (self.on_failure if reply is None else self.on_answer)(node_id)
            except Exception:
                pass
            results.put((node_id, reply))

        # Daemon threads rather than a ThreadPoolExecutor: a query hung past the deadline is abandoned
        # and must not delay process exit. At most alpha run at a time per run.
        threading.Thread(target=work, name="rnfs-lookup", daemon=True).start()

    def _clean(self, reply) -> tuple[list[bytes], list[bytes]] | None:
        """reply as (providers, nodes) of valid IDs, at most k each; None if it isn't a reply at all."""
        if not isinstance(reply, (tuple, list)) or len(reply) != 2:
            return None
        providers, nodes = reply
        if not isinstance(providers, (tuple, list)) or not isinstance(nodes, (tuple, list)):
            return None
        # A responder sends at most K of each; ignore any excess rather than let a reply bloat the lookup
        providers = list(dict.fromkeys(p for p in providers if _valid_id(p)))[:self.k]
        nodes = list(dict.fromkeys(n for n in nodes if _valid_id(n)))[:self.k]
        return providers, nodes
