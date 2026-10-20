"""Disjoint lookup paths (specification section 6; S/Kademlia): one lookup for a key run as several Lookups that share
no node. No RNS dependency, like rnfs.dht.lookup.

Identities placed next to a key answer for it with each other only, and a lookup that queries them can be steered
among them. Paths that share no node can't all be: once one path has queried an identity, the others never do, so
they reach the key through the honest servers around it. The start nodes (and the senior servers, if given) are
dealt to the paths in turn; a path claims each node before querying it (Claims), drops a node another path claimed
first, and ignores such nodes when replies name them. Each path is an ordinary Lookup run (its own max_queries and
termination), and every path shares one time budget: they start together.

The result merges the paths: find_closest() gives the k closest any path found, path_candidates() each path's own
(a publisher also stores to each path's k closest), find_providers() the providers any path finds, reported in the
caller's thread as they arrive. In the eclipse simulation, publishing over 3 paths (and searching over them when the
density-checked lookup finds nothing) found content 98.9% of the time under attack on fast nodes, for about 25 more
messages per publish.
"""
import queue
import threading
from collections.abc import Callable, Iterable

from rnfs.dht import density
from rnfs.dht.lookup import FIRST, Lookup, NodeCallback
from rnfs.dht.routing import distance

# make_path(start nodes, seniors, claim, taken) -> the Lookup of one path
MakePath = Callable[[list[bytes], list[bytes], Callable[[bytes], bool], Callable[[bytes], bool]], Lookup]


def _unsampled(key: bytes, kth: int) -> None:
    """Default on_complete: keeps nothing."""


class Claims:
    """Which path of a disjoint lookup owns each node it queried. Thread-safe."""

    def __init__(self):
        """No node claimed yet."""
        self._owner: dict[bytes, int] = {}
        self._lock = threading.Lock()

    def claim(self, path: int, node_id: bytes) -> bool:
        """path is about to query node_id: True if the node is now (or already was) path's, False if another's."""
        with self._lock:
            return self._owner.setdefault(node_id, path) == path

    def taken(self, path: int, node_id: bytes) -> bool:
        """True if another path than path owns node_id."""
        with self._lock:
            owner = self._owner.get(node_id)
        return owner is not None and owner != path


class DisjointLookup:
    """One lookup for key over paths Lookups that share no node, made by make_path; quacks like a Lookup where its
    callers need it (find_closest, candidates, ask, remaining, find_providers, stop and the listings).

    on_complete(key, kth) is told the distance to the k-th closest node the paths found together, once a call in
    which every path completed ends (the network size estimate's sample, rnfs.dht.density)."""

    def __init__(self, key: bytes, start_nodes: Iterable[bytes], make_path: MakePath, paths: int,
                 seniors: Iterable[bytes] = (), on_complete: Callable[[bytes, int], object] = _unsampled):
        """The paths, each with every paths-th start node and senior server; nothing is sent until a run."""
        self.key = key
        self.on_complete = on_complete
        count = max(1, paths)
        start, senior = list(dict.fromkeys(start_nodes)), list(dict.fromkeys(seniors))
        self.claims = Claims()
        self.paths: list[Lookup] = [
            make_path(start[index::count], senior[index::count],
                      lambda node_id, index=index: self.claims.claim(index, node_id),
                      lambda node_id, index=index: self.claims.taken(index, node_id))
            for index in range(count)]
        self.k = self.paths[0].k
        self.poll_interval = self.paths[0].poll_interval
        self._reported: set[bytes] = set()  # providers passed to on_provider, in any run
        self._events: queue.Queue | None = None  # the running find_providers' queue (stop() wakes it)

    @property
    def dense(self) -> bool:
        """True if the density check fired on a path."""
        return any(path.dense for path in self.paths)

    @property
    def sent(self) -> int:
        """Queries the paths sent, in all."""
        return sum(path.sent for path in self.paths)

    @property
    def ended(self) -> str:
        """How the last runs ended: "complete" if every path completed, else the first other way one ended."""
        return next((path.ended for path in self.paths if path.ended != "complete"), "complete")

    # Running the paths

    def _run_all(self, work: Callable[[Lookup], object]):
        """Run work(path) for every path at once, the first in the caller's thread; raises the first exception one
        raised, once all have ended."""
        failures: list[BaseException] = []

        def run(path: Lookup):
            """One path's work, its exception kept."""
            try:
                work(path)
            except BaseException as e:  # noqa: BLE001 - raised in the caller's thread once every path has ended
                failures.append(e)

        threads = [threading.Thread(target=run, args=(path,), name="rnfs-lookup-path", daemon=True)
                   for path in self.paths[1:]]
        for thread in threads:
            thread.start()
        run(self.paths[0])
        for thread in threads:
            thread.join()
        if failures:
            raise failures[0]

    def _sample(self):
        """Tell on_complete the size estimate's sample (density.verified_sample) over the nodes the paths knew of
        together, if every path completed: verified if a path verified it."""
        if self.ended != "complete":
            return
        known: dict[bytes, int] = {}
        checks = []
        for path in self.paths:
            known.update(path.known_distances())
            checks.append(path.verified())
        kth = density.verified_sample(known, lambda node_id: any(check(node_id) for check in checks))
        if kth is not None:
            self.on_complete(self.key, kth)

    def find_closest(self) -> list[bytes]:
        """Run every path until complete or out of time; the k closest nodes they know of together, ranked as a
        Lookup's find_closest ranks them (by tier: unreliable nodes that haven't answered last)."""
        results: list[list[bytes]] = []
        self._run_all(lambda path: results.append(path.find_closest()))
        status = self.candidates()
        merged = {node_id for result in results for node_id in result}
        self._sample()
        return sorted(merged, key=lambda n: (status.get(n, FIRST), distance(n, self.key)))[:self.k]

    def find_providers(self, on_provider: NodeCallback, should_stop: Callable[[], bool] = lambda: False
                       ) -> list[bytes]:
        """Run every path until should_stop() is True or each is complete or out of time; returns the providers this
        call found, in the order found. Each provider not reported before is passed to on_provider as soon as a path
        finds it; on_provider and should_stop are called in the caller's thread."""
        events: queue.Queue = queue.Queue()
        self._events = events
        stopping = threading.Event()
        failures: list[BaseException] = []

        def run(path: Lookup):
            """One path's search, its providers queued for the caller (and its exception kept)."""
            try:
                path.find_providers(lambda provider: events.put(provider), stopping.is_set)
            except BaseException as e:  # noqa: BLE001 - raised in the caller's thread once every path has ended
                failures.append(e)
            finally:
                events.put(None)

        threads = [threading.Thread(target=run, args=(path,), name="rnfs-lookup-path", daemon=True)
                   for path in self.paths]
        for thread in threads:
            thread.start()
        found: list[bytes] = []
        running = len(threads)
        try:
            while running:
                if not stopping.is_set() and should_stop():
                    stopping.set()
                    for path in self.paths:
                        path.stop()
                try:
                    item = events.get(timeout=self.poll_interval)
                except queue.Empty:
                    continue
                if item is None:
                    running -= 1
                elif isinstance(item, bytes) and item not in self._reported:
                    self._reported.add(item)
                    found.append(item)
                    on_provider(item)
        finally:
            stopping.set()
            self._events = None
        for thread in threads:
            thread.join()
        if failures:
            raise failures[0]
        self._sample()
        return found

    def stop(self):
        """Wake a running find_providers so it checks should_stop() now."""
        events = self._events
        if events is not None:
            events.put(False)

    # What the paths learned

    def candidates(self) -> dict[bytes, int]:
        """Every node a path's find_closest chooses from (Lookup.candidates), with its rank tier: the lowest any path
        that knows it gives it."""
        merged: dict[bytes, int] = {}
        for path in self.paths:
            for node_id, tier in path.candidates().items():
                merged[node_id] = min(merged.get(node_id, tier), tier)
        return merged

    def path_candidates(self) -> list[dict[bytes, int]]:
        """Each path's own candidates()."""
        return [path.candidates() for path in self.paths]

    def ask(self, node_id: bytes, key: bytes) -> list[bytes] | None:
        """Lookup.ask, through the path that knows node_id (the first if none does)."""
        path = next((path for path in self.paths if node_id in path.candidates()), self.paths[0])
        return path.ask(node_id, key)

    def remaining(self) -> float:
        """Seconds left in the time budget the paths share."""
        return max(path.remaining() for path in self.paths)

    def allow_unknown(self, node_id: bytes) -> bool:
        """Lookup.allow_unknown, from the first path's budget (the fetcher's providers use it)."""
        return self.paths[0].allow_unknown(node_id)

    def listing_nodes(self) -> list[bytes]:
        """The nodes that answered a path with at least one provider."""
        return list(dict.fromkeys(node_id for path in self.paths for node_id in path.listing_nodes()))

    def listers(self, provider: bytes) -> set[bytes]:
        """The nodes whose replies to any path listed provider."""
        return set().union(*(path.listers(provider) for path in self.paths))

    def closest_answered(self) -> list[bytes]:
        """The k closest nodes that answered any path, closest first."""
        answered = {node_id for path in self.paths for node_id in path.closest_answered()}
        return sorted(answered, key=lambda n: distance(n, self.key))[:self.k]
