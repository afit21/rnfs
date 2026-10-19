"""Which lookups a node runs (specification section 6, "Identities placed next to a key").
No RNS dependency: the node gives Lookups (its routing table, client and callbacks) through new_lookup.

- **Searching** (Lookups.search: providers for the fetcher, keeper queries, `rnfs providers`): a Lookup with the
  density check (the size estimate leaving out the key's own sample) and the senior rule (the announce first-seen
  log's servers closest to the key, heard at least provider_seniority ago). Only if it finds no provider at all,
  the search falls back to disjoint paths (DISJOINT_PATHS, SLOW_DISJOINT_PATHS on slow nodes) with the same checks,
  in what is left of the same time budget and query cap (Search: so a search sends at most MAX_QUERIES /find in all,
  however its lookups end; once identities that keep naming closer ones have used them up there is no fallback). A
  slow node paces the fallback like its publish lookups: it
  waits for the pacing (pace) if that fits in the time left, and is skipped otherwise. In the eclipse simulation
  this kept a normal search's cost at +0.36 /find while content was found 98.9% of the time under attack; a search
  for content nobody provides pays the fallback, about 2.5-3 times a normal lookup.
"""
import threading
from collections.abc import Callable

from rnfs.dht import density
from rnfs.dht.disjoint import DisjointLookup
from rnfs.dht.lookup import Lookup, NodeCallback
from rnfs.dht.routing import distance

DISJOINT_PATHS = 3  # paths of a search's fallback (eclipse simulation: 4 added under 1 point)
SLOW_DISJOINT_PATHS = 2  # on slow nodes, whose every /find costs seconds of airtime

# new_lookup(key, start nodes, **Lookup options) -> a Lookup wired to the node (routing table, client, callbacks)
NewLookup = Callable[..., Lookup]


class Search:
    """A provider search: the first lookup, then, only if no run of it found a provider, the fallback (made by
    fallback(time left, queries left) when needed) in the rest of the same time budget and of the first lookup's
    max_queries, so the search sends at most that many /find in all (a security bound: DHT-1). Quacks like a Lookup
    where its callers need it. pace() (slow nodes): 0.0 if the fallback may start now (and is counted), else the
    seconds to wait."""

    def __init__(self, first: Lookup, fallback: Callable[[float, int], Lookup | DisjointLookup | None] | None = None,
                 pace: Callable[[], float] | None = None):
        """A search that starts with first."""
        self.first = first
        self.second: Lookup | DisjointLookup | None = None  # the fallback, once escalated
        self.key = first.key
        self._fallback = fallback
        self._pace = pace
        self._reported: set[bytes] = set()  # providers passed to on_provider, in any run
        self._wake = threading.Event()

    @property
    def escalated(self) -> bool:
        """True once the search fell back to disjoint paths."""
        return self.second is not None

    def find_providers(self, on_provider: NodeCallback, should_stop: Callable[[], bool] = lambda: False
                       ) -> list[bytes]:
        """Lookup.find_providers: a run of the first lookup (of the fallback, once escalated), then the fallback if no
        provider has been found yet, time is left and should_stop() isn't True. Returns the providers this call
        found."""
        found: list[bytes] = []

        def report(provider: bytes):
            """Pass on a provider not reported before."""
            if provider not in self._reported:
                self._reported.add(provider)
                found.append(provider)
                on_provider(provider)

        (self.second or self.first).find_providers(report, should_stop)
        if self.second is None and not self._reported and self._fallback is not None and not should_stop():
            self.second = self._escalate(should_stop)
            if self.second is not None:
                self.second.find_providers(report, should_stop)
        return found

    def _escalate(self, should_stop: Callable[[], bool]) -> Lookup | DisjointLookup | None:
        """The fallback, once the pacing allows it; None if no time or queries are left (fallback gives None when too
        few are), the pacing would take longer than the time left, or should_stop() turns True while waiting."""
        queries = self.first.max_queries - self.first.sent
        while True:
            left = self.first.remaining()
            if left <= 0 or queries <= 0 or should_stop():
                return None
            wait = self._pace() if self._pace is not None else 0.0
            if wait <= 0:
                break
            if wait >= left:
                self.first.report(f"lookup for {self.key.hex()}: no fallback over disjoint paths, paced for "
                                  f"{wait:.0f}s more")
                return None
            self._wake.wait(min(wait, self.first.poll_interval))
            self._wake.clear()
        fallback = self._fallback(self.first.remaining(), queries)
        if fallback is not None:
            self.first.report(f"lookup for {self.key.hex()}: no provider found, falling back to disjoint paths with "
                              f"{queries} queries left")
        return fallback

    def stop(self):
        """Wake a running find_providers so it checks should_stop() now."""
        self._wake.set()
        self.first.stop()
        if self.second is not None:
            self.second.stop()

    def find_closest(self) -> list[bytes]:
        """The first lookup's find_closest (closest-nodes lookups never fall back)."""
        return self.first.find_closest()

    def candidates(self) -> dict[bytes, bool]:
        """The first lookup's candidates."""
        return self.first.candidates()

    def remaining(self) -> float:
        """Seconds left in the time budget (the fallback shares the first lookup's)."""
        return self.first.remaining()

    def allow_unknown(self, node_id: bytes) -> bool:
        """Lookup.allow_unknown, from the first lookup's budget."""
        return self.first.allow_unknown(node_id)

    def _lookups(self) -> list:
        """The first lookup and the fallback if any."""
        return [self.first] + ([self.second] if self.second is not None else [])

    def listing_nodes(self) -> list[bytes]:
        """The nodes that answered with at least one provider, in either."""
        return list(dict.fromkeys(node_id for lookup in self._lookups() for node_id in lookup.listing_nodes()))

    def listers(self, provider: bytes) -> set[bytes]:
        """The nodes whose replies listed provider, in either."""
        return set().union(*(lookup.listers(provider) for lookup in self._lookups()))

    def closest_answered(self) -> list[bytes]:
        """The k closest nodes that answered either, closest first."""
        answered = {node_id for lookup in self._lookups() for node_id in lookup.closest_answered()}
        return sorted(answered, key=lambda n: distance(n, self.key))[:self.first.k]


class Lookups:
    """The node's recipe for lookups: new_lookup makes a Lookup wired to it, start(key) gives the start nodes,
    seniors(key) the senior servers (the announce first-seen log's), sizes the size estimate (fed by every complete
    lookup); paths disjoint paths per fallback; pace (slow nodes) paces the fallback."""

    def __init__(self, new_lookup: NewLookup, start: Callable[[bytes], list[bytes]], sizes: density.SizeEstimate,
                 seniors: Callable[[bytes], list[bytes]], paths: int = DISJOINT_PATHS,
                 pace: Callable[[], float] | None = None):
        """Lookups made with new_lookup."""
        self.new_lookup = new_lookup
        self.start = start
        self.sizes = sizes
        self.seniors = seniors
        self.paths = max(1, paths)
        self.pace = pace

    def search(self, key: bytes) -> Search:
        """A provider search for key: density-checked, the senior rule, and disjoint paths only as a fallback."""
        size, seniors = self.sizes.estimate(key), self.seniors(key)
        first = self.new_lookup(key, self.start(key), size=size, seniors=seniors, on_complete=self.sizes.note)
        return Search(first, lambda left, queries: self._disjoint(key, size, seniors, left, queries), self.pace)

    def _disjoint(self, key: bytes, size: float | None = None, seniors: list[bytes] | tuple = (),
                  timeout: float | None = None, queries: int | None = None) -> Lookup | DisjointLookup | None:
        """A lookup for key over self.paths disjoint paths (one Lookup for a single path), each with the checks given,
        within timeout (the node's lookup_timeout if None) and sending queries in all, split among the paths (each
        path's run its own max_queries if None); None if that leaves a path none."""
        options = {} if timeout is None else {"timeout": timeout}
        if queries is not None:
            if queries < self.paths:
                return None
            options["max_queries"] = queries // self.paths
        if self.paths == 1:
            return self.new_lookup(key, self.start(key), size=size, seniors=seniors, on_complete=self.sizes.note,
                                   **options)

        def path(start: list[bytes], senior: list[bytes], claim, taken) -> Lookup:
            """One path's Lookup."""
            return self.new_lookup(key, start, size=size, seniors=senior, claim=claim, taken=taken, **options)

        return DisjointLookup(key, self.start(key), path, self.paths, seniors, on_complete=self.sizes.note)
