"""Iterative DHT lookups (specification section 6). No RNS dependency: network access, identity knowledge
and time are injected, so lookups run over in-memory networks in tests. (Only a callback raising, a bug, is
logged through rnfs.log, which loads RNS, imported then.)

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

Nodes behind a slow path (`slow_path`, specification section 6: on nodes that aren't slow, servers on LoRa islands)
rank after every other but the unreliable ones, so a lookup leaves LoRa airtime alone while it has enough others; it
still asks them, and stores to them, when it hasn't. Ordering only: they are asked like any node then, nothing counts
against them, and one that answers ranks first for the rest of the lookup. A node whose path was unknown when it was
ranked is checked again once the query's path request is answered (`check_path`): one found slow is ranked again
before its link is opened, and asked at once only if it still ranks among the nodes to query.

Identities generated next to a key (specification section 6) can answer for it with each other only. Two local
rules make a run wait for more than the beta closest, so they can't hide what honest servers near the key hold:
- **Senior servers** (`seniors`: the servers closest to the key that the announce first-seen log heard at least
  provider_seniority ago, which such identities can't be): the beta closest of them known that haven't failed must
  answer too. Without an attack they are the beta closest anyway; at most len(seniors) extra queries.
- **The density check** (`size`: the network size estimate, rnfs.dht.density): when the nodes known near the key are
  packed more than DENSITY_THRESHOLD times denser than the estimate allows, RING_ANSWERS servers of each ring beyond
  the dense core must answer too.
Their extra queries go out within alpha like any other and count towards max_queries; their answers count as
progress when they bring the key nearer, but never against the stall rule (they aren't meant to come nearer).

Disjoint lookups (rnfs.dht.disjoint) run several Lookups that share no node: `claim(id)` is asked before each query
(False: another path owns the node, which this one drops) and `taken(id)` leaves out nodes another path owns when a
reply names them.

Local servers (specification section 6, "LoRa islands"): `first` names nodes the first run asks before any other (a
search asks the closest local servers first), until one of them lists a provider; their answers seed the rest of the
run like any other. `allowed(id)` says which nodes the lookup may query at all (in partition mode, local servers only):
asked as nodes are learned and again before each is queried, so a node that stops being allowed mid-lookup is dropped
when its turn comes, and never in the result. Neither touches providers: a reply's providers are reported whoever they
are.
"""
import heapq
import queue
import threading
import time
from collections.abc import Callable, Iterable

from rnfs.constants import ID_LENGTH, K
from rnfs.dht import density
from rnfs.dht.routing import distance, is_id

# query(node_id, key) -> (provider IDs, node IDs), or None if the node didn't answer, refused, or replied
# with garbage. May block for a long time (path request, link setup, RNS's request timeout).
FindQuery = Callable[[bytes, bytes], tuple[list[bytes], list[bytes]] | None]
NodeCallback = Callable[[bytes], object]  # return value ignored

# Rank tiers: a run queries, and find_closest and candidates() rank, the nodes of a lower tier first, by distance within
# each. Nodes that answered are always FIRST
FIRST = 0  # answered, or nothing against it
SLOW_PATH = 1  # behind a slow path (slow_path) and not answered
UNRELIABLE = 2  # unreliable (the routing table's) and not answered
_POLL_INTERVAL = 0.5  # seconds between should_stop() checks while waiting for replies
MAX_QUERIES = 8 * K  # queries a run sends at most (section 6)
STALL_WINDOWS = 3  # a run ends after this many query windows of answers in a row that bring the key no nearer
# A search's query unanswered this long gives up its place among the alpha in flight, and ranks with the unreliable
# until it answers (its answer still counts): a server RNS has a long path to holds a slot for its link's whole establishment
# timeout, 217 s for one 34 hops away, and two such servers held a LoRa island's search 6 minutes (Lookup.patience)
PATIENCE_FAST = 15.0  # seconds, on a node that isn't slow: an honest answer takes 0.1-5 s between cloud nodes
PATIENCE_SLOW = 45.0  # ... and on a slow node, where a link and an answer over the radio take 10-40 s
HOLD_PATIENCE = 3  # a search that found nobody waits this many times patience for a server slow to answer
_FAR = ID_LENGTH * 8 + 1  # _reach with fewer than beta nodes known: farther than any distance
_STOP = object()  # queued by stop() to wake the waiting caller
_SLOWER = object()  # a worker's outcome: check_path found the node's path slow, so it wasn't asked


def _ignore(node_id: bytes) -> None:
    """Default node callback: does nothing."""


def _always(node_id: bytes) -> bool:
    """Default is_known: every identity is known."""
    return True


def _never(node_id: bytes) -> bool:
    """Default exclude: nothing is excluded."""
    return False


def _unnamed(node_id: bytes, by: bytes) -> None:
    """Default named: does nothing."""


def _quiet(text: str) -> None:
    """Default report: says nothing."""


def _unsampled(key: bytes, kth: int) -> None:
    """Default on_complete: keeps nothing."""


def _name(node_id: bytes) -> str:
    """A node ID as the log writes it (RNS.prettyhexrep's form, without loading RNS)."""
    return f"<{node_id.hex()}>"


class Lookup:
    """One lookup for key: find_closest() for publishing, find_providers() for fetching.

    - `start_nodes`: usually the routing table's closest nodes to key.
    - `query`: a FindQuery; exceptions count as no answer.
    - `is_known(id)`: whether RNS knows id's identity. Querying an unknown ID costs a path request, so at
      most `unknowns` unknown IDs are tried per lookup (allow_unknown, shared with the fetcher's providers):
      `alpha` if None, never fewer (slow nodes give publishing and provider lookups more, rnfs.dht.lookups).
    - `on_answer(id)` / `on_failure(id)`: called from worker threads when a query is answered or fails,
      even after the run that sent it has returned, so the node can update its routing table.
    - `exclude(id)`: IDs to ignore in replies, as nodes and as providers (banned nodes).
    - `timeout`: the time budget, starting at the first run; later runs share it.
    - `report(text)`: told each run's start and end, and each query's outcome (the node logs them at
      LOG_DEBUG, for the debug log); called from the running thread and the query threads.
    - `unreliable(id)`: nodes to avoid, checked when a run learns of them; `may_retry(id)`: True if an
      unreliable node is due to be asked again, claiming that retry (both the routing table's).
    - `max_queries`: queries one run sends at most, retries included.
    - `on_useful(id)`: a node whose answer proved useful: it named a node closer to the key than itself that then
      answered (local scores, section 6). Called from the running thread, at most once per node per lookup.
    - `size`: the network size estimate (rnfs.dht.density), which turns the density check on; `seniors`: the
      announce first-seen log's servers closest to the key heard at least provider_seniority ago (see above).
    - `claim(id)`, `taken(id)`: a disjoint lookup's paths' shared node ownership (see above).
    - `on_complete(key, kth)`: told, at the end of each run that completed with at least k nodes known, the
      distance from the key to the k-th closest (the network size estimate's sample).
    - `slow_path(id)`: nodes behind a slow path (see above), asked once per node per lookup; `check_path(id)`: called
      on the query's thread before a node of the first tier that hasn't answered is asked (once per node per lookup):
      None to ask it, True if its path turned out slow (it is ranked again instead), False if it can't be reached
      (a failed query, not sent). Nodes that aren't slow give both; slow nodes neither.
    - `first`: nodes the first run asks before any other, until a reply lists a provider (see above); `allowed(id)`:
      whether a node may be queried at all (see above).
    - `named(id, by)`: told, just before a query goes to a node whose identity isn't known (a path request), which node's
      answer first named it (the node scopes that path request to the path to `by`, rnfs.network.paths).

    After find_closest, candidates() gives every node it chose from and ask() sends one more /find about a nearby
    key, so one lookup serves the keys of a region (sweep providing, rnfs.publisher).

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
                 may_retry: Callable[[bytes], bool] = _never, max_queries: int = MAX_QUERIES,
                 on_useful: NodeCallback = _ignore, size: float | None = None, seniors: Iterable[bytes] = (),
                 claim: Callable[[bytes], bool] = _always, taken: Callable[[bytes], bool] = _never,
                 on_complete: Callable[[bytes, int], object] = _unsampled, unknowns: int | None = None,
                 slow_path: Callable[[bytes], bool] = _never,
                 check_path: Callable[[bytes], bool | None] | None = None, first: Iterable[bytes] = (),
                 allowed: Callable[[bytes], bool] = _always,
                 named: Callable[[bytes, bytes], object] = _unnamed, patience: float | None = None):
        """A lookup for key; nothing is sent until find_closest() or find_providers() runs it. patience: seconds after
        which a find_providers query unanswered stops holding one of the alpha places (PATIENCE_FAST, PATIENCE_SLOW;
        None: never)."""
        self.key = key
        self.query = query
        self.own_id = own_id
        self.alpha = max(1, alpha)
        self.unknowns = max(self.alpha, unknowns or 0)
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
        self.on_useful = on_useful
        self.size = size
        self.claim = claim
        self.taken = taken
        self.on_complete = on_complete
        self.slow_path = slow_path
        self.check_path = check_path
        self.allowed = allowed
        self.named = named
        self.patience = patience
        self._named_by: dict[bytes, bytes] = {}  # node -> the node whose answer first named it, in any run
        self._slow: dict[bytes, bool] = {}  # slow_path's answers, asked once per node (and check_path's True)
        self._checked: set[bytes] = set()  # nodes check_path was asked about
        self.ended = ""  # how the last run ended ("complete", "no progress", ...; "" before the first)
        self.sent = 0  # queries the runs sent, in all (retries included)
        self.dense = False  # the density check fired in a run
        self._useful: set[bytes] = set()  # nodes passed to on_useful, in any run
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
        self._last_slow: set[bytes] = set()  # the nodes the last run knew of as behind a slow path and not answered
        self._out: set[bytes] = set()  # nodes ask() sent to that haven't answered (still out at its deadline)
        # node -> the nodes it named, for every answer about the key, also those that came after the run that asked
        # stopped waiting (it completes with queries still out)
        self._replies: dict[bytes, list[bytes]] = {}
        self._provided: dict[bytes, list[bytes]] = {}  # node -> the providers its answer about the key listed, likewise
        self._wake: queue.Queue | None = None
        self._lock = threading.Lock()
        self._start_nodes = [n for n in dict.fromkeys(start_nodes) if self._usable(n)]
        self._seniors = [n for n in dict.fromkeys(seniors) if self._usable(n)]
        self._first = [n for n in dict.fromkeys(first) if self._usable(n)]  # asked first by the first run only

    # Public interface

    def find_closest(self) -> list[bytes]:
        """Run until complete or timed out; the k closest nodes known, closest first.

        As the spec says, these are the closest nodes known, not only those that answered: termination
        guarantees just the beta closest answered, and the stores sent to the rest show which are
        reachable. Left out: nodes that failed or were still not answering at the deadline, and
        unqueried IDs whose identity isn't known (contacting them costs a path request). Ranked by tier, then
        distance: unreliable nodes come after every other, so stores are sent to them only to make up k.
        """
        self._run(None, lambda: False)
        candidates = self._choices(late=False)
        return sorted(candidates, key=lambda n: (candidates[n], self._distance(n)))[:self.k]

    def candidates(self) -> dict[bytes, int]:
        """Every node find_closest chooses from, after a run (and ask()), with its rank tier: UNRELIABLE for an
        unreliable node that hasn't answered, SLOW_PATH for one behind a slow path that hasn't, else FIRST. Sweep
        providing ranks them for other keys near this one.

        The nodes the last run knew of that answered, or that it didn't query and whose identity is known; not
        those that failed or were still not answering when it ended. Unlike find_closest, also the answers that came
        after the run ended (a run that completes leaves queries out, usually to nodes that are fine): their nodes
        answered, and the nodes they named are known as a run's replies' are.
        """
        return self._choices(late=True)

    def _choices(self, late: bool) -> dict[bytes, int]:
        """candidates(), with (late) or without the answers that came after the last run ended."""
        with self._lock:
            answered = set(self._answered)
            replies = dict(self._replies) if late else {}
        known = dict(self._last_known)
        slow = set(self._last_slow)
        for node_id, nodes in replies.items():
            if node_id in self._failed:
                continue
            answered.add(node_id)
            for n in [node_id] + nodes:
                if n not in known and self._usable(n):
                    known[n] = self._distance(n)
                    if self._is_slow(n):  # named after the run ended: slow paths still rank after the rest
                        slow.add(n)
        doubtful, queried, out = self._last_doubtful, self._last_queried, self._out

        def tier(n: bytes) -> int:
            """n's rank tier: nodes that answered first."""
            if n in answered:
                return FIRST
            return UNRELIABLE if n in doubtful else SLOW_PATH if n in slow else FIRST

        return {n: tier(n) for n in known
                if n in answered or (n not in out and n not in queried and self.is_known(n) and self.allowed(n))}

    def ask(self, node_id: bytes, key: bytes) -> list[bytes] | None:
        """Send node_id one /find about key, a key near this lookup's (sweep providing asks about the far half of
        a region), within the lookup's deadline; None if it didn't answer by then. Its answer or failure is counted
        as a run's (on_answer, on_failure), and the nodes it names join the nodes candidates() gives: returned as
        it named them (valid IDs, at most k), so the caller can tell whether it named k.

        Call it only between runs. node_id must be usable (not this node, failed, excluded) and is charged to the
        budget of unknown IDs as a run's queries are.
        """
        if self.deadline is None:
            self.deadline = self.clock() + self.timeout
        if self.remaining() <= 0 or not self._usable(node_id) or not self.allow_unknown(node_id):
            return None  # nothing sent
        results: queue.Queue = queue.Queue()
        self._out.add(node_id)
        self._send(node_id, results, key)
        reply = None
        while True:
            wait = min(self.deadline - self.clock(), self.poll_interval)
            if wait <= 0:
                return None  # still out: its outcome is counted when it comes
            try:
                answered_id, reply = results.get(timeout=wait)
            except queue.Empty:
                continue
            if answered_id == node_id:
                break
        known = self._last_known
        self._out.discard(node_id)
        if reply is None:
            with self._lock:
                self._failed.add(node_id)
            known.pop(node_id, None)
            return None
        with self._lock:
            self._answered.add(node_id)
        self._last_doubtful.discard(node_id)
        self._last_slow.discard(node_id)
        known.setdefault(node_id, self._distance(node_id))
        nodes = reply[1]
        for new_id in nodes:
            if new_id not in known and new_id not in self._last_queried and self._usable(new_id):
                known[new_id] = self._distance(new_id)
                with self._lock:
                    self._named_by.setdefault(new_id, node_id)
                if self.unreliable(new_id):
                    self._last_doubtful.add(new_id)
                if self._is_slow(new_id):
                    self._last_slow.add(new_id)
        return list(nodes)

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
        """True if node_id may be contacted: its identity is known, or the lookup's budget of `unknowns` unknown
        IDs (path requests) has room. Each unknown ID is charged once; later calls for it return True."""
        if self.is_known(node_id):
            return True
        with self._lock:
            if node_id in self._allowed_unknown:
                return True
            if len(self._allowed_unknown) >= self.unknowns:
                self._refused_unknown.add(node_id)
                return False
            self._allowed_unknown.add(node_id)
            return True

    def remaining(self) -> float:
        """Seconds left in the time budget (the full timeout before the first run)."""
        if self.deadline is None:
            return self.timeout
        return max(0.0, self.deadline - self.clock())

    def late_providers(self) -> list[bytes]:
        """The providers listed by answers that came after the run that asked had ended (a run completes with queries
        still out, often to nodes behind a slow path, which rank last), not reported before: reported now, as a run
        reports them (not this node, nor one exclude names), and their listers counted as a run's are. Call it
        between runs, in the caller's thread."""
        with self._lock:
            provided = list(self._provided.items())
        late: list[bytes] = []
        for node_id, providers in provided:
            fresh = [provider for provider in providers if provider not in self._reported and provider not in late
                     and provider != self.own_id and not self.exclude(provider)]
            if not fresh:
                continue
            with self._lock:
                self._listing.add(node_id)
                for provider in providers:
                    self._listed_by.setdefault(provider, set()).add(node_id)
            late += fresh
        self._reported.update(late)
        return late

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

    def _is_slow(self, node_id: bytes) -> bool:
        """slow_path(node_id), asked once per lookup (it reads RNS's path table and the traffic counters)."""
        with self._lock:
            cached = self._slow.get(node_id)
        if cached is None:
            cached = bool(self.slow_path(node_id))
            with self._lock:
                cached = self._slow.setdefault(node_id, cached)
        return cached

    def _usable(self, node_id) -> bool:
        """True if node_id may be queried in this lookup."""
        return (is_id(node_id) and node_id != self.own_id and node_id not in self._failed
                and node_id not in self._refused_unknown and not self.exclude(node_id) and self.allowed(node_id))

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
        # The nodes asked before any other (local servers), in this run only; emptied once a reply lists a provider
        first = [node_id for node_id in self._first if not self.taken(node_id)]
        self._first = []
        for node_id in list(self._start_nodes) + self.closest_answered() + self._seniors + first:
            if self._usable(node_id) and not self.taken(node_id):
                known[node_id] = self._distance(node_id)
        seniors = [node_id for node_id in self._seniors if node_id in known]
        # Known nodes that were unreliable when found and haven't answered: ranked after every other; and those behind
        # a slow path that haven't answered in this lookup, ranked after every other but those
        doubtful = {node_id for node_id in known if self.unreliable(node_id)}
        slow = {node_id for node_id in known if node_id not in self._answered and self._is_slow(node_id)}
        queried: set[bytes] = set()
        answered: set[bytes] = set()
        retrying: set[bytes] = set()  # unreliable nodes asked again on top of alpha, which nothing waits for
        asked_at: dict[bytes, float] = {}  # node -> when its query (one of the alpha) was sent, until it is answered
        # A search's (find_providers): finding the closest, for publishing, waits for the slow as before, as where
        # records are stored matters more than how soon; a search's late answers still give their providers
        # (late_providers)
        patience = self.patience if on_provider is not None else None
        lagged = 0  # queries that stopped holding a place for want of patience (they joined retrying)
        lagging: dict[bytes, float] = {}  # those still unanswered -> when they were sent
        extras: set[bytes] = set()  # nodes asked only because the senior rule or the density check required them
        passed: set[bytes] = set()  # unreliable nodes whose retry wasn't due (or needs a path request)
        found: list[bytes] = []
        introduced: dict[bytes, bytes] = {}  # node -> the node whose answer first named it, in this run
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
        self._last_slow = slow
        self._out = set()

        def tier(node_id: bytes) -> int:
            """node_id's rank tier: reliable nodes on fast paths (and those that answered) first, then slow paths."""
            return UNRELIABLE if node_id in doubtful else SLOW_PATH if node_id in slow else FIRST

        def rank(node_id: bytes) -> tuple[int, int]:
            """Sort key: by tier, then by distance."""
            return tier(node_id), known[node_id]

        def required() -> list[bytes]:
            """The nodes that must answer besides the beta closest known: the beta closest senior servers known, and
            beyond a dense core RING_ANSWERS of each ring (ranked as rank ranks them)."""
            needed = sorted((node_id for node_id in seniors if node_id in known), key=rank)[:self.beta]
            if self.size is not None:
                by_distance = sorted(known, key=known.__getitem__)
                ring = density.rings(by_distance, [known[node_id] for node_id in by_distance], self.size,
                                     density.RING_ANSWERS, later=tier)
                if ring:
                    self.dense = True
                    needed += [node_id for node_id in ring if node_id not in needed]
            return needed

        checks = bool(seniors) or self.size is not None
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
            needed = required() if checks else []
            # A search that has found nobody waits for its servers slow to answer before it ends, up to HOLD_PATIENCE
            # times patience after each was asked: they no longer hold its other queries back, but one may hold the
            # only record. One that answers later still gives its providers (late_providers)
            holding = (not found and not self._reported and patience is not None
                       and any(self.clock() - at < HOLD_PATIENCE * patience for at in lagging.values()))
            if ranked and all(node_id in answered for node_id in ranked[:self.beta] + needed) and not holding:
                why = "complete"
                break  # the beta closest known nodes, and those the checks need, have all answered: complete
            if stalled >= STALL_WINDOWS * window:
                why = "no progress"
                break  # nodes keep answering, but the closest known come no nearer
            if patience is not None:
                now = self.clock()
                for node_id in [n for n, at in asked_at.items() if now - at >= patience]:
                    # Unanswered past patience: on top of alpha now, as a retry is, and ranked with the unreliable, so
                    # it is waited for again (just below) only while the lookup knows no better nodes to ask
                    lagging[node_id] = asked_at.pop(node_id)
                    retrying.add(node_id)
                    doubtful.add(node_id)
                    in_flight -= 1
                    lagged += 1
                if lagged:
                    ranked = sorted(known, key=rank)
            for node_id in ranked[:window]:
                if node_id in retrying:  # a retry the lookup now needs: waited for like any query
                    retrying.discard(node_id)
                    in_flight += 1

            # Keep up to alpha queries in flight, to the closest nodes not yet queried. Only nodes ranked
            # within the closest max(alpha, beta) known can affect completion, so farther ones aren't
            # queried while nearer queries are pending: on slow networks every query costs airtime.
            # The nodes the checks need are queried the same way, in rank order among the window's.
            while in_flight < self.alpha and sent < self.max_queries:
                ranked = sorted(known, key=rank)[:window]
                node_id = next((n for n in first if n in known and n not in queried), None)
                if node_id is None:
                    targets = sorted(set(ranked).union(n for n in needed if n in known), key=rank) if needed else ranked
                    node_id = next((n for n in targets if n not in queried), None)
                if node_id is None:
                    break
                queried.add(node_id)
                if not self.allowed(node_id) or not self.claim(node_id) or not self.allow_unknown(node_id):
                    # No longer allowed (partition mode began), another path's (a disjoint lookup), or over the
                    # unknown-ID budget: dropped, but not a failure
                    del known[node_id]
                    continue
                if node_id not in ranked:
                    extras.add(node_id)
                # A first-tier node whose path is unknown may turn out slow once its path request is answered (a node
                # asked first is asked whatever its path)
                check = (self.check_path is not None and tier(node_id) == FIRST and node_id not in self._answered
                         and node_id not in self._checked and node_id not in first)
                if check:
                    self._checked.add(node_id)
                self._send(node_id, results, check=check)
                asked_at[node_id] = self.clock()
                sent += 1
                self.sent += 1
                in_flight += 1
            # An unreliable node that distance alone would rank among them is asked again once its retry is
            # due: only an answer shows it is back. On top of alpha, since nothing waits for it
            for node_id in sorted(known, key=known.__getitem__)[:window]:
                if node_id not in doubtful or node_id in queried or node_id in passed or sent >= self.max_queries:
                    continue
                if not self.is_known(node_id):
                    passed.add(node_id)
                elif not self.allowed(node_id) or not self.claim(node_id):
                    known.pop(node_id, None)  # no longer allowed (partition mode), or another path's (disjoint)
                elif self.may_retry(node_id):
                    queried.add(node_id)
                    retrying.add(node_id)
                    retried += 1
                    sent += 1
                    self.sent += 1
                    self._send(node_id, results)
                else:
                    passed.add(node_id)
            if in_flight == 0 and not holding:
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
            asked_at.pop(node_id, None)
            lagging.pop(node_id, None)
            if node_id in retrying:
                retrying.discard(node_id)
            else:
                in_flight -= 1
            if reply is _SLOWER:
                # Not asked: ranked again, now behind a slow path, and queried again (without checking) only if it
                # still ranks among the nodes to query. Nothing was sent, so nothing counts
                queried.discard(node_id)
                extras.discard(node_id)
                sent -= 1
                self.sent -= 1
                if node_id in known:
                    slow.add(node_id)
                continue
            if reply is None:
                with self._lock:
                    self._failed.add(node_id)
                known.pop(node_id, None)
                continue

            answered.add(node_id)
            doubtful.discard(node_id)
            slow.discard(node_id)
            with self._lock:
                self._answered.add(node_id)
            self._credit(introduced.get(node_id), node_id)
            providers, nodes = reply
            if providers:
                first.clear()  # the nodes asked first found something: the rest are asked as any other
                with self._lock:
                    self._listing.add(node_id)
                    for provider in providers:  # at most k per reply, from nodes that answered
                        self._listed_by.setdefault(provider, set()).add(node_id)
            for new_id in nodes:
                if new_id not in known and new_id not in queried and self._usable(new_id) and not self.taken(new_id):
                    known[new_id] = self._distance(new_id)
                    introduced.setdefault(new_id, node_id)
                    with self._lock:
                        self._named_by.setdefault(new_id, node_id)
                    if self.unreliable(new_id):
                        doubtful.add(new_id)
                    if self._is_slow(new_id):
                        slow.add(new_id)
            nearer = self._reach(known)
            if nearer < reach:
                reach, stalled = nearer, 0
            elif node_id not in extras:  # the checks' nodes aren't asked to come nearer: not stalling
                stalled += 1
            if on_provider is not None:
                for provider in providers:
                    if provider in self._reported or provider == self.own_id or self.exclude(provider):
                        continue
                    self._reported.add(provider)
                    found.append(provider)
                    on_provider(provider)
        failed = len(queried & self._failed)
        self.ended = why
        self.report(f"lookup for {self.key.hex()}: {why} after {self.clock() - started:.1f}s; asked {len(queried)}, "
                    f"{len(answered)} answered, {failed} failed, {in_flight} still out"
                    + (f", retried {retried} unreliable ({len(retrying)} still out)" if retried else "")
                    + (f", {lagged} slow to answer let others be asked" if lagged else "")
                    + (f", {len(extras)} for the senior rule and the density check" if extras else "")
                    + ("; dense near the key" if self.dense else "")
                    + (f"; {len(found)} new providers" if on_provider is not None else ""))
        kth = density.verified_sample(dict(known), self.verified()) if why == "complete" else None
        if kth is not None:
            try:
                self.on_complete(self.key, kth)
            except Exception as e:  # noqa: BLE001 - a caller's callback must not end the lookup; logged
                from rnfs.log import LOG_WARNING, log
                log(f"lookup for {self.key.hex()}: the completion callback raised {e!r}", LOG_WARNING)
        return found

    def verified(self) -> Callable[[bytes], bool]:
        """Whether a node a run knew of (and hadn't seen fail) is verified for the size estimate's sample
        (density.verified_sample, told to on_complete): it answered, in any run, or its identity is known. A reply can
        name made-up IDs just beyond the beta closest, which a run completes without querying; a node still out when
        the run completed counts if its identity is known, as a real server's slow answer is common."""
        with self._lock:
            answered = set(self._answered)
        return lambda node_id: node_id in answered or self.is_known(node_id)

    def known_distances(self) -> dict[bytes, int]:
        """The nodes the last run knew of and hadn't seen fail, with their distances to the key."""
        return dict(self._last_known)

    def heard(self) -> set[bytes]:
        """Every node the runs and ask() heard of that may be queried and hasn't failed: the nodes they knew of and
        those any answer named (also one that came after its run ended), whether or not their identity is known.
        candidates() is the part of it a store can reach without a path request; sweep providing compares the two."""
        with self._lock:
            heard = set(self._named_by) | self._answered
            for nodes in self._replies.values():
                heard.update(nodes)
        heard.update(self._last_known)
        # Not _usable: an unknown ID over the budget of path requests was still heard of
        return {node_id for node_id in heard
                if is_id(node_id) and node_id != self.own_id and node_id not in self._failed
                and not self.exclude(node_id) and self.allowed(node_id)}

    def _credit(self, introducer: bytes | None, node_id: bytes):
        """node_id answered: if introducer named it and it is closer to the key, introducer's answer was useful
        (an answer of made-up IDs never is, as they don't answer). Once per introducer per lookup."""
        if introducer is None or introducer in self._useful or self._distance(node_id) >= self._distance(introducer):
            return
        self._useful.add(introducer)
        try:
            self.on_useful(introducer)
        except Exception as e:  # noqa: BLE001 - a caller's callback must not end the lookup; logged
            from rnfs.log import LOG_WARNING, log
            log(f"lookup for {self.key.hex()}: the useful-answer callback for {_name(introducer)} raised {e!r}",
                LOG_WARNING)

    def _send(self, node_id: bytes, results: queue.Queue, key: bytes | None = None, check: bool = False):
        """Query node_id about key (the lookup's own by default) on a worker thread, which reports the outcome
        and queues it for the run. check: ask check_path first, and queue _SLOWER instead if the path is slow."""
        key = self.key if key is None else key
        about = "" if key == self.key else f" about {key.hex()}"
        with self._lock:
            # Charged to the budget of unknown IDs (allow_unknown): its query starts with a path request
            by = self._named_by.get(node_id) if node_id in self._allowed_unknown else None
        if by is not None:
            try:
                self.named(node_id, by)
            except Exception as e:  # noqa: BLE001 - a caller's callback must not end the lookup; logged
                from rnfs.log import LOG_WARNING, log
                log(f"lookup for {self.key.hex()}: the named callback for {_name(node_id)} raised {e!r}", LOG_WARNING)

        def work():
            """Run the blocking query, report it, and hand the cleaned reply to the run."""
            started = self.clock()
            try:
                path = self.check_path(node_id) if check else None
                if path:
                    with self._lock:
                        self._slow[node_id] = True
                    self.report(f"lookup for {self.key.hex()}: the path to {_name(node_id)} is slow; ranked again")
                    results.put((node_id, _SLOWER))
                    return
                reply = None if path is False else self._clean(self.query(node_id, key))
            except Exception as e:  # noqa: BLE001 - one broken node must not abort the lookup
                reply = None  # one broken node must not abort the lookup
                self.report(f"lookup for {self.key.hex()}: asking {_name(node_id)}{about} raised {e!r}")
            else:
                took = self.clock() - started
                self.report(f"lookup for {self.key.hex()}: {_name(node_id)}{about} " + (
                    f"failed after {took:.1f}s" if reply is None
                    else f"answered in {took:.1f}s: {len(reply[0])} providers, {len(reply[1])} nodes"))
            if reply is not None and key == self.key:
                with self._lock:
                    self._replies[node_id] = reply[1]
                    if reply[0]:
                        self._provided[node_id] = list(reply[0])
            try:
                (self.on_failure if reply is None else self.on_answer)(node_id)
            except Exception as e:  # noqa: BLE001 - a caller's callback must not end the lookup; logged
                # A bug in the routing table (private mode's admit, say): logged inside the except, so the debug
                # log has its traceback. rnfs.log loads RNS, which this module doesn't at import
                from rnfs.log import LOG_WARNING, log
                log(f"lookup for {self.key.hex()}: the callback for {_name(node_id)} raised {e!r}", LOG_WARNING)
            results.put((node_id, reply))

        # Daemon threads rather than a ThreadPoolExecutor: a query hung past the deadline is abandoned
        # and must not delay process exit. A run waits for at most alpha at a time; retries of unreliable nodes
        # run on top of those (each node once per run, every query within max_queries).
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
