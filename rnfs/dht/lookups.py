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
- **Publishing** (Lookups.publish: a region's lookup, rnfs.dht.sweep): always disjoint paths, without the checks;
  then each key of the region is checked (flags) over the servers the paths learned, and a key flagged gets a
  widened lookup of its own (widen: one Lookup with both checks), whose ring servers it is stored on too.
- **Path requests.** A lookup tries at most its budget of unknown IDs (Lookup.unknowns, alpha by default). A slow
  node, whose announce-starved routing table often knows only some of a key's closest servers, gives a publishing
  lookup SLOW_PUBLISH_UNKNOWNS and a search SLOW_SEARCH_UNKNOWNS (the topology simulation's strategy UB: an island
  publisher's records reached 3.7 of their 10 closest servers instead of 1.3, and backbone searchers found them 99% of
  the time instead of 68%). Disjoint paths split their lookup's budget (split_budget), each path never below alpha: a
  slow node's publish lookup sends at most 10 path requests over its 2 paths, and a search's fallback 3 (besides the
  first lookup's 3). The widened lookup of a flagged key is a publishing lookup of its own.
- **Local first** (specification section 6, "LoRa islands"): a search's first lookup asks the closest local servers
  (local_first(key), none on a node without them) before any other, and their answers seed the rest of it; its
  fallback doesn't.
"""
import threading
from collections.abc import Callable

from rnfs.constants import K
from rnfs.dht import density
from rnfs.dht.disjoint import DisjointLookup
from rnfs.dht.lookup import Lookup, NodeCallback
from rnfs.dht.routing import distance

DISJOINT_PATHS = 3  # paths of a publisher's lookups and a search's fallback (eclipse simulation: 4 added under 1 point)
SLOW_DISJOINT_PATHS = 2  # on slow nodes, whose every /find costs seconds of airtime
SLOW_PUBLISH_UNKNOWNS = 10  # unknown IDs (path requests) a slow node's publishing lookup tries at most (alpha: 1)
SLOW_SEARCH_UNKNOWNS = 3  # unknown IDs a slow node's search (and its fallback) tries at most

# new_lookup(key, start nodes, **Lookup options) -> a Lookup wired to the node (routing table, client, callbacks)
NewLookup = Callable[..., Lookup]


def split_budget(total: int | None, paths: int) -> list[int | None]:
    """total unknown IDs dealt to paths disjoint paths, the first ones taking what doesn't divide evenly (None: each
    path the Lookup's default, alpha). A Lookup never takes fewer than alpha, so a share may round up to it."""
    if total is None:
        return [None] * paths
    return [total // paths + (1 if index < total % paths else 0) for index in range(paths)]


def _budget(unknowns: int | None) -> dict:
    """The Lookup option for a budget of unknowns unknown IDs (none: the Lookup's default)."""
    return {} if unknowns is None else {"unknowns": unknowns}


class Search:
    """A provider search: the first lookup, then, only if no run of it found a provider, the fallback (made by
    fallback(time left, queries left) when needed) in the rest of the same time budget and of the first lookup's
    max_queries, so the search sends at most that many /find in all (a security bound: DHT-1). Quacks like a Lookup
    where its callers need it. pace() (slow nodes): 0.0 if the fallback may start now (and is counted), else the
    seconds to wait. checks (the node's density.CheckCounts) counts the search once if its first lookup has the
    density check (a size estimate), and once more if the check fired in any of its lookups."""

    def __init__(self, first: Lookup, fallback: Callable[[float, int], Lookup | DisjointLookup | None] | None = None,
                 pace: Callable[[], float] | None = None, checks: density.CheckCounts | None = None):
        """A search that starts with first."""
        self.first = first
        self.second: Lookup | DisjointLookup | None = None  # the fallback, once escalated
        self.key = first.key
        self._fallback = fallback
        self._pace = pace
        self._checks = checks
        self._counted = self._counted_dense = False  # whether checks has this search, and that it fired
        self._reported: set[bytes] = set()  # providers passed to on_provider, in any run
        self._wake = threading.Event()

    @property
    def dense(self) -> bool:
        """True once the density check fired in any of the search's lookups."""
        return any(lookup.dense for lookup in self._lookups())

    def _count(self):
        """Count the search's density check in checks, once, and that it fired, once (a later run may make it)."""
        if self._checks is None or self.first.size is None:
            return
        if not self._counted:
            self._counted = True
            self._checks.checked.add()
        if not self._counted_dense and self.dense:
            self._counted_dense = True
            self._checks.flagged.add()

    @property
    def escalated(self) -> bool:
        """True once the search fell back to disjoint paths."""
        return self.second is not None

    def find_providers(self, on_provider: NodeCallback, should_stop: Callable[[], bool] = lambda: False
                       ) -> list[bytes]:
        """Lookup.find_providers: a run of the first lookup (of the fallback, once escalated), then the fallback if no
        provider has been found yet, time is left and should_stop() isn't True, then the providers of answers that came
        after their run ended (Lookup.late_providers). Returns the providers this call found."""
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
        # Answers that came after their run ended: over LoRa the provider itself, a slow path's node ranked last, may
        # answer just after the first lookup completed, and the fetch would fail with its record in hand
        for lookup in [self.first] + ([self.second] if self.second is not None else []):
            for provider in lookup.late_providers():
                report(provider)
        self._count()
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
        closest = self.first.find_closest()
        self._count()
        return closest

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
    lookup); paths disjoint paths per publish lookup and fallback; pace (slow nodes) paces the fallback;
    publish_unknowns and search_unknowns the budgets of unknown IDs of a publishing lookup and of a search (and of its
    fallback), split among disjoint paths (None: alpha per Lookup, as on nodes that aren't slow); local_first(key) the
    local servers a search asks first. checks counts how often the density check ran and fired, for the status (a
    density.CheckCounts of its own if None)."""

    def __init__(self, new_lookup: NewLookup, start: Callable[[bytes], list[bytes]], sizes: density.SizeEstimate,
                 seniors: Callable[[bytes], list[bytes]], paths: int = DISJOINT_PATHS,
                 pace: Callable[[], float] | None = None, publish_unknowns: int | None = None,
                 search_unknowns: int | None = None, local_first: Callable[[bytes], list[bytes]] = lambda key: [],
                 checks: density.CheckCounts | None = None):
        """Lookups made with new_lookup."""
        self.checks = checks if checks is not None else density.CheckCounts()
        self.new_lookup = new_lookup
        self.start = start
        self.sizes = sizes
        self.seniors = seniors
        self.paths = max(1, paths)
        self.pace = pace
        self.publish_unknowns = publish_unknowns
        self.search_unknowns = search_unknowns
        self.local_first = local_first

    def search(self, key: bytes) -> Search:
        """A provider search for key: the closest local servers asked first, density-checked, the senior rule, and
        disjoint paths only as a fallback."""
        size, seniors = self.sizes.estimate(key), self.seniors(key)
        local = self.local_first(key)
        first = self.new_lookup(key, self.start(key), size=size, seniors=seniors, on_complete=self.sizes.note,
                                **({"first": local} if local else {}), **_budget(self.search_unknowns))
        return Search(first, lambda left, queries: self._disjoint(key, size, seniors, left, queries,
                                                                  self.search_unknowns), self.pace, self.checks)

    def publish(self, key: bytes) -> Lookup | DisjointLookup:
        """A region's closest-nodes lookup for key, over disjoint paths."""
        return self._disjoint(key, unknowns=self.publish_unknowns)

    def flags(self, key: bytes, region) -> bool:
        """The density check for key over what its region's lookup learned (a sweep.Region), counted in checks."""
        size = self.sizes.estimate(key)
        if size is None:
            return False
        distances = sorted(distance(node_id, key) for node_id in region.nodes)
        flagged = density.dense_core(distances, size) > 0
        self.checks.note(flagged)
        return flagged

    def widen(self, key: bytes, region) -> list[bytes]:
        """The widened lookup of a key that flags() flagged: a Lookup with both checks, so it finds and asks the
        servers beyond the dense core. Returns K servers of each ring, from what it and the region learned."""
        size = self.sizes.estimate(key)
        lookup = self.new_lookup(key, self.start(key), size=size, seniors=self.seniors(key),
                                 on_complete=self.sizes.note, **_budget(self.publish_unknowns))
        lookup.find_closest()
        nodes = dict(region.nodes)
        for node_id, tier in lookup.candidates().items():
            nodes[node_id] = min(nodes.get(node_id, tier), tier)
        ranked = sorted(nodes, key=lambda n: distance(n, key))
        return density.rings(ranked, [distance(n, key) for n in ranked], size, K, later=nodes.__getitem__)

    def _disjoint(self, key: bytes, size: float | None = None, seniors: list[bytes] | tuple = (),
                  timeout: float | None = None, queries: int | None = None,
                  unknowns: int | None = None) -> Lookup | DisjointLookup | None:
        """A lookup for key over self.paths disjoint paths (one Lookup for a single path), each with the checks given,
        within timeout (the node's lookup_timeout if None) and sending queries in all, split among the paths (each
        path's run its own max_queries if None); None if that leaves a path none. unknowns unknown IDs are split among
        the paths too (split_budget)."""
        options = {} if timeout is None else {"timeout": timeout}
        if queries is not None:
            if queries < self.paths:
                return None
            options["max_queries"] = queries // self.paths
        if self.paths == 1:
            return self.new_lookup(key, self.start(key), size=size, seniors=seniors, on_complete=self.sizes.note,
                                   **options, **_budget(unknowns))
        # DisjointLookup makes the paths in order, one call each: path i takes the i-th share
        shares = iter(split_budget(unknowns, self.paths))

        def path(start: list[bytes], senior: list[bytes], claim, taken) -> Lookup:
            """One path's Lookup, with its share of the unknown IDs."""
            return self.new_lookup(key, start, size=size, seniors=senior, claim=claim, taken=taken, **options,
                                   **_budget(next(shares)))

        return DisjointLookup(key, self.start(key), path, self.paths, seniors, on_complete=self.sizes.note)
