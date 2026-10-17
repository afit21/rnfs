"""Iterative DHT lookups (specification section 6). No RNS dependency: network access, identity knowledge
and time are injected, so lookups run over in-memory networks in tests.

A lookup keeps up to `alpha` /find queries in flight, always to the closest known nodes not yet queried,
and merges the nodes each reply returns. It is complete once the `beta` closest nodes it knows of have all
answered, or once every known node has been queried without finding new ones; it never runs past its
`lookup_timeout` budget. Queries are blocking calls run on worker threads, so a slow or hung node never
holds the caller past the deadline.

Nodes near the key can keep answering with ever closer IDs (made up, or ground for the key), so the beta closest
never all answer. A run therefore also ends, incomplete, once it has sent `max_queries` queries and their answers
are in, or after STALL_WINDOWS × max(alpha, beta) answers in a row that leave the bit length of the beta-th
closest known distance unchanged. Over a simulated network of 2000 honest nodes, lookups complete within both.

Nodes whose recent queries all failed (the routing table's `unreliable`) rank after every other node, so a
lookup asks and waits for them only when it has too few others. One whose retry is due (`may_retry`) is asked
again on top of the `alpha` queries if it would otherwise rank among the closest, and nothing waits for that
answer: a node that never establishes a link costs a lookup no time, yet one that comes back is found.
"""
import heapq
import queue
import threading
import time
from collections.abc import Callable, Iterable

from rnfs.constants import ID_LENGTH, K
from rnfs.dht.routing import distance, is_id

# query(node_id, key) -> (provider IDs, node IDs), or None if the node didn't answer, refused, or replied
# with garbage. May block for a long time (path request, link setup, RNS's request timeout).
FindQuery = Callable[[bytes, bytes], tuple[list[bytes], list[bytes]] | None]
NodeCallback = Callable[[bytes], object]  # return value ignored

_POLL_INTERVAL = 0.5  # seconds between should_stop() checks while waiting for replies
MAX_QUERIES = 8 * K  # queries a run sends at most (section 6)
STALL_WINDOWS = 3  # a run ends after this many query windows of answers in a row that bring the key no nearer
_FAR = ID_LENGTH * 8 + 1  # _reach with fewer than beta nodes known: farther than any distance
_STOP = object()  # queued by stop() to wake the waiting caller


def _ignore(node_id: bytes) -> None:
    """Default node callback: does nothing."""


def _always(node_id: bytes) -> bool:
    """Default is_known: every identity is known."""
    return True


def _never(node_id: bytes) -> bool:
    """Default exclude: nothing is excluded."""
    return False


def _quiet(text: str) -> None:
    """Default report: says nothing."""


def _name(node_id: bytes) -> str:
    """A node ID as the log writes it (RNS.prettyhexrep's form, without loading RNS)."""
    return f"<{node_id.hex()}>"


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
    - `report(text)`: told each run's start and end, and each query's outcome (the node logs them at
      LOG_DEBUG, for the debug log); called from the running thread and the query threads.
    - `unreliable(id)`: nodes to avoid, checked when a run learns of them; `may_retry(id)`: True if an
      unreliable node is due to be asked again, claiming that retry (both the routing table's).
    - `max_queries`: queries one run sends at most, retries included.

    A Lookup may be run again (find_providers after every provider failed): each run re-queries the nodes,
    so /find replies give a fresh random half of providers, within the same deadline and unknown-ID budget.
    Runs of one Lookup must not overlap.
    """

    def __init__(self, key: bytes, start_nodes: Iterable[bytes], query: FindQuery, own_id: bytes,
                 alpha: int = 3, beta: int = 3, k: int = K, timeout: float = 600,
                 is_known: Callable[[bytes], bool] = _always, on_answer: NodeCallback = _ignore,
                 on_failure: NodeCallback = _ignore, exclude: Callable[[bytes], bool] = _never,
                 clock: Callable[[], float] = time.monotonic, poll_interval: float = _POLL_INTERVAL,
                 report: Callable[[str], None] = _quiet, unreliable: Callable[[bytes], bool] = _never,
                 may_retry: Callable[[bytes], bool] = _never, max_queries: int = MAX_QUERIES):
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
        self.report = report
        self.unreliable = unreliable
        self.may_retry = may_retry
        self.max_queries = max(1, max_queries)
        self.deadline: float | None = None
        self._failed: set[bytes] = set()  # didn't answer: dropped for the rest of the lookup
        self._answered: set[bytes] = set()  # answered in any run
        self._allowed_unknown: set[bytes] = set()  # unknown IDs that were given a path request
        self._refused_unknown: set[bytes] = set()  # unknown IDs over the budget
        self._reported: set[bytes] = set()  # providers already passed to on_provider, in any run
        self._listing: set[bytes] = set()  # nodes whose reply listed a provider, in any run
        self._listed_by: dict[bytes, set[bytes]] = {}  # provider -> nodes whose reply listed it, in any run
        self._last_known: dict[bytes, int] = {}  # the nodes the last run knew of and hadn't seen fail
        self._last_queried: set[bytes] = set()  # the nodes the last run queried
        self._last_doubtful: set[bytes] = set()  # the nodes the last run knew of as unreliable and not answered
        self._wake: queue.Queue | None = None
        self._lock = threading.Lock()
        self._start_nodes = [n for n in dict.fromkeys(start_nodes) if self._usable(n)]

    # Public interface

    def find_closest(self) -> list[bytes]:
        """Run until complete or timed out; the k closest nodes known, closest first.

        As the spec says, these are the closest nodes known, not only those that answered: termination
        guarantees just the beta closest answered, and the stores sent to the rest show which are
        reachable. Left out: nodes that failed or were still not answering at the deadline, and
        unqueried IDs whose identity isn't known (contacting them costs a path request). Unreliable nodes
        come after every other, so stores are sent to them only to make up k.
        """
        self._run(None, lambda: False)
        candidates = [n for n in self._last_known
                      if n in self._answered or (n not in self._last_queried and self.is_known(n))]
        doubtful = self._last_doubtful
        return sorted(candidates, key=lambda n: (n in doubtful, self._distance(n)))[:self.k]

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

    def listing_nodes(self) -> list[bytes]:
        """The nodes that have answered with at least one provider (in any run)."""
        with self._lock:
            return list(self._listing)

    def listers(self, provider: bytes) -> set[bytes]:
        """The nodes whose replies listed provider (in any run), so a caller can tell who vouches for it."""
        with self._lock:
            return set(self._listed_by.get(provider, ()))

    def closest_answered(self) -> list[bytes]:
        """The k closest nodes that answered in any run so far, closest first."""
        with self._lock:
            answered = list(self._answered)
        return sorted(answered, key=self._distance)[:self.k]

    # The lookup itself

    def _distance(self, node_id: bytes) -> int:
        """node_id's distance to the key."""
        return distance(node_id, self.key)

    def _reach(self, known: dict[bytes, int]) -> int:
        """The bit length of the beta-th closest known node's distance to the key (_FAR if fewer are known):
        what an answer must shorten to count as progress."""
        if len(known) < self.beta:
            return _FAR
        return heapq.nsmallest(self.beta, known.values())[-1].bit_length()

    def _usable(self, node_id) -> bool:
        """True if node_id may be queried in this lookup."""
        return (is_id(node_id) and node_id != self.own_id and node_id not in self._failed
                and node_id not in self._refused_unknown and not self.exclude(node_id))

    def _run(self, on_provider: NodeCallback | None, should_stop: Callable[[], bool]) -> list[bytes]:
        """One pass of the iterative lookup; returns the providers it newly reported."""
        if self.deadline is None:
            self.deadline = self.clock() + self.timeout
        results: queue.Queue = queue.Queue()
        self._wake = results
        self.report(f"lookup for {self.key.hex()}" + (" (providers)" if on_provider is not None else "")
                    + f": {len(self._start_nodes)} start nodes, {max(0.0, self.deadline - self.clock()):.0f}s left")
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
        # Known nodes that were unreliable when found and haven't answered: ranked after every other
        doubtful = {node_id for node_id in known if self.unreliable(node_id)}
        queried: set[bytes] = set()
        answered: set[bytes] = set()
        retrying: set[bytes] = set()  # unreliable nodes asked again on top of alpha, which nothing waits for
        passed: set[bytes] = set()  # unreliable nodes whose retry wasn't due (or needs a path request)
        found: list[bytes] = []
        in_flight = 0
        retried = 0
        sent = 0  # queries sent by this run, retries included: at most max_queries
        window = max(self.alpha, self.beta)
        # Answers in a row that didn't bring the beta-th closest known node a bit nearer the key
        reach = self._reach(known)
        stalled = 0
        self._last_known = known  # live: find_closest reads them once the run ends
        self._last_queried = queried
        self._last_doubtful = doubtful

        def rank(node_id: bytes) -> tuple[bool, int]:
            """Sort key: reliable nodes (and those that answered) first, then by distance."""
            return node_id in doubtful, known[node_id]

        started = self.clock()
        why = "stopped"

        while True:
            if on_provider is not None and should_stop():
                why = "enough providers"
                break
            if self.clock() >= self.deadline:
                why = "out of time"
                break
            ranked = sorted(known, key=rank)
            if ranked and all(node_id in answered for node_id in ranked[:self.beta]):
                why = "complete"
                break  # the beta closest known nodes have all answered: complete
            if stalled >= STALL_WINDOWS * window:
                why = "no progress"
                break  # nodes keep answering, but the closest known come no nearer
            for node_id in ranked[:window]:
                if node_id in retrying:  # a retry the lookup now needs: waited for like any query
                    retrying.discard(node_id)
                    in_flight += 1

            # Keep up to alpha queries in flight, to the closest nodes not yet queried. Only nodes ranked
            # within the closest max(alpha, beta) known can affect completion, so farther ones aren't
            # queried while nearer queries are pending: on slow networks every query costs airtime.
            while in_flight < self.alpha and sent < self.max_queries:
                ranked = sorted(known, key=rank)[:window]
                node_id = next((n for n in ranked if n not in queried), None)
                if node_id is None:
                    break
                queried.add(node_id)
                if not self.allow_unknown(node_id):
                    del known[node_id]  # over the unknown-ID budget: dropped, but not a failure
                    continue
                self._send(node_id, results)
                sent += 1
                in_flight += 1
            # An unreliable node that distance alone would rank among them is asked again once its retry is
            # due: only an answer shows it is back. On top of alpha, since nothing waits for it
            for node_id in sorted(known, key=known.__getitem__)[:window]:
                if node_id not in doubtful or node_id in queried or node_id in passed or sent >= self.max_queries:
                    continue
                if self.is_known(node_id) and self.may_retry(node_id):
                    queried.add(node_id)
                    retrying.add(node_id)
                    retried += 1
                    sent += 1
                    self._send(node_id, results)
                else:
                    passed.add(node_id)
            if in_flight == 0:
                if sent >= self.max_queries:
                    why = "query cap"
                    break  # sent all it may, and every answer waited for is in
                why = "no node left to ask"
                break  # every known node was queried and nothing new turned up

            wait = min(self.deadline - self.clock(), self.poll_interval)
            if wait <= 0:
                why = "out of time"
                break
            try:
                item = results.get(timeout=wait)
            except queue.Empty:
                continue
            if item is _STOP:
                continue
            node_id, reply = item
            if node_id in retrying:
                retrying.discard(node_id)
            else:
                in_flight -= 1
            if reply is None:
                with self._lock:
                    self._failed.add(node_id)
                known.pop(node_id, None)
                continue

            answered.add(node_id)
            doubtful.discard(node_id)
            with self._lock:
                self._answered.add(node_id)
            providers, nodes = reply
            if providers:
                with self._lock:
                    self._listing.add(node_id)
                    for provider in providers:  # at most k per reply, from nodes that answered
                        self._listed_by.setdefault(provider, set()).add(node_id)
            for new_id in nodes:
                if new_id not in known and new_id not in queried and self._usable(new_id):
                    known[new_id] = self._distance(new_id)
                    if self.unreliable(new_id):
                        doubtful.add(new_id)
            nearer = self._reach(known)
            if nearer < reach:
                reach, stalled = nearer, 0
            else:
                stalled += 1
            if on_provider is not None:
                for provider in providers:
                    if provider in self._reported or provider == self.own_id or self.exclude(provider):
                        continue
                    self._reported.add(provider)
                    found.append(provider)
                    on_provider(provider)
        failed = len(queried & self._failed)
        self.report(f"lookup for {self.key.hex()}: {why} after {self.clock() - started:.1f}s; asked {len(queried)}, "
                    f"{len(answered)} answered, {failed} failed, {in_flight} still out"
                    + (f", retried {retried} unreliable ({len(retrying)} still out)" if retried else "")
                    + (f"; {len(found)} new providers" if on_provider is not None else ""))
        return found

    def _send(self, node_id: bytes, results: queue.Queue):
        """Query node_id on a worker thread, which reports the outcome and queues it for the run."""
        def work():
            """Run the blocking query, report it, and hand the cleaned reply to the run."""
            started = self.clock()
            try:
                reply = self._clean(self.query(node_id, self.key))
            except Exception as e:  # noqa: BLE001 - one broken node must not abort the lookup
                reply = None  # one broken node must not abort the lookup
                self.report(f"lookup for {self.key.hex()}: asking {_name(node_id)} raised {e!r}")
            else:
                took = self.clock() - started
                self.report(f"lookup for {self.key.hex()}: {_name(node_id)} " + (
                    f"failed after {took:.1f}s" if reply is None
                    else f"answered in {took:.1f}s: {len(reply[0])} providers, {len(reply[1])} nodes"))
            try:
                (self.on_failure if reply is None else self.on_answer)(node_id)
            except Exception:  # noqa: BLE001 - a caller's callback must not end the lookup
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
        providers = list(dict.fromkeys(p for p in providers if is_id(p)))[:self.k]
        nodes = list(dict.fromkeys(n for n in nodes if is_id(n)))[:self.k]
        return providers, nodes
