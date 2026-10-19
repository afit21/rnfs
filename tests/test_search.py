"""Tests for the lookups a node runs (rnfs/dht/lookups.py): the provider search's fallback to disjoint paths."""
import os
import random
import tempfile
import unittest

from rnfs.config import Config
from rnfs.constants import ID_LENGTH, K
from rnfs.content.storage import Storage
from rnfs.dht.density import SIZE_SAMPLES, SizeEstimate
from rnfs.dht.lookup import Lookup
from rnfs.dht.lookups import Lookups, Search
from rnfs.dht.providers import ProviderStore
from rnfs.dht.routing import RoutingTable
from rnfs.publisher import SLOW_LOOKUP_PACE, Publisher
from tests.fakes.client import FakeClient
from tests.fakes.log import quiet_log
from tests.support import FakeClock
from tests.test_density import AttackedNetwork
from tests.test_lookup import CONFIG, LookupTestCase, ident

setUpModule, tearDownModule = quiet_log()


class FakeSearchLookup:
    """A first lookup for Search: reports given providers, with a settable time budget."""

    def __init__(self, providers=(), left: float = 100.0):
        """A lookup that will report providers."""
        self.key = ident(1)
        self.k = K
        self.poll_interval = 0.01
        self.providers = list(providers)
        self.left = left
        self.max_queries = 80
        self.sent = 7
        self.runs = 0
        self.reports: list[str] = []

    def report(self, text):
        """Note a report."""
        self.reports.append(text)

    def find_providers(self, on_provider, should_stop=lambda: False):
        """Report the providers."""
        self.runs += 1
        for provider in self.providers:
            on_provider(provider)
        return list(self.providers)

    def remaining(self):
        """The time left."""
        return self.left

    def stop(self):
        """Nothing to wake."""

    def listing_nodes(self):
        """None."""
        return []

    def listers(self, provider):
        """None."""
        return set()

    def closest_answered(self):
        """None."""
        return []


class SearchTest(unittest.TestCase):
    """The searcher's escalation to disjoint paths."""

    def test_falls_back_only_when_nothing_was_found(self):
        made = []
        first = FakeSearchLookup([ident(5)])
        search = Search(first, lambda left, queries: made.append(left))
        self.assertEqual(search.find_providers(lambda p: None), [ident(5)])
        self.assertEqual(made, [])
        self.assertFalse(search.escalated)

        second = FakeSearchLookup([ident(6), ident(5)])
        first = FakeSearchLookup([], left=42.0)
        search = Search(first, lambda left, queries: (made.append((left, queries)), second)[1])
        reported = []
        self.assertEqual(search.find_providers(reported.append), [ident(6), ident(5)])
        self.assertEqual(made, [(42.0, 80 - 7)])  # in what is left of the same budgets
        self.assertTrue(search.escalated)
        # A run again (every provider failed) runs the fallback again, reporting nothing twice
        self.assertEqual(search.find_providers(reported.append), [])
        self.assertEqual((first.runs, second.runs), (1, 2))
        self.assertEqual(reported, [ident(6), ident(5)])

    def test_no_fallback_out_of_time_or_when_stopped(self):
        made = []
        search = Search(FakeSearchLookup([], left=0.0), lambda left, queries: made.append(left))
        self.assertEqual(search.find_providers(lambda p: None), [])
        search = Search(FakeSearchLookup([]), lambda left, queries: made.append(left))
        self.assertEqual(search.find_providers(lambda p: None, should_stop=lambda: True), [])
        spent = FakeSearchLookup([])
        spent.sent = spent.max_queries  # identities kept it busy to its query cap: nothing left for a fallback
        search = Search(spent, lambda left, queries: made.append(left))
        self.assertEqual(search.find_providers(lambda p: None), [])
        self.assertEqual(made, [])

    def test_paced_fallback_waits_or_is_skipped(self):
        made = []
        waits = [0.05, 0.0]
        search = Search(FakeSearchLookup([]),
                        lambda left, queries: (made.append(left), FakeSearchLookup([ident(3)]))[1],
                        pace=lambda: waits.pop(0))
        self.assertEqual(search.find_providers(lambda p: None), [ident(3)])
        self.assertEqual(waits, [])  # asked again once the wait was over
        first = FakeSearchLookup([], left=10.0)
        search = Search(first, lambda left, queries: made.append(left), pace=lambda: 30.0)
        self.assertEqual(search.find_providers(lambda p: None), [])
        self.assertEqual(len(made), 1)
        self.assertIn("paced", first.reports[-1])


class LookupsTest(LookupTestCase):
    """Lookups: what a node runs for searching."""

    def setUp(self):
        """An attacked network and a node's table and size estimate over it."""
        super().setUp()
        self.key = random.Random(40).randbytes(ID_LENGTH)
        self.net = AttackedNetwork(300, 41, self.key, 12)
        self.networks.append(self.net)
        self.own = random.Random(42).randbytes(ID_LENGTH)
        self.table = RoutingTable(self.own, CONFIG, clock=self.net.clock)
        for n in self.net.nodes:
            self.table.heard(n)
        self.sizes = SizeEstimate()
        self.lookups = Lookups(self.new_lookup, lambda key: self.table.closest(key, K), self.sizes,
                               lambda key: [], paths=3)
        rng = random.Random(43)
        for _ in range(SIZE_SAMPLES):
            self.lookups.search(rng.randbytes(ID_LENGTH)).find_closest()

    def new_lookup(self, key, start, **options):
        """A Lookup over the network."""
        options.setdefault("timeout", 10)
        return Lookup(key, start, self.net.query, self.own, poll_interval=0.02, **options)

    def test_complete_lookups_feed_the_size_estimate(self):
        self.assertEqual(len(self.sizes), SIZE_SAMPLES)
        self.assertLess(abs(self.sizes.estimate() / 312 - 1), 0.6)

    def test_search_is_density_checked_and_falls_back_to_disjoint_paths(self):
        search = self.lookups.search(self.key)
        self.assertIsNotNone(search.first.size)
        self.assertEqual(search.find_providers(lambda p: None), [])  # nobody provides it
        self.assertTrue(search.escalated)
        self.assertEqual(len(search.second.paths), 3)
        self.assertTrue(search.second.dense)
        # The search's /find in all stay within one run's cap: the paths share what the first lookup left
        self.assertLessEqual(search.first.sent + search.second.sent, search.first.max_queries)
        self.assertEqual(search.second.paths[0].max_queries, (search.first.max_queries - search.first.sent) // 3)

    def test_no_check_before_the_estimate_warms_up(self):
        lookups = Lookups(self.new_lookup, lambda key: self.table.closest(key, K), SizeEstimate(), lambda key: [])
        self.assertIsNone(lookups.search(self.key).first.size)


class PacingTest(unittest.TestCase):
    """Publisher.pace_lookups: a slow node's search fallback shares the publish rounds' pacing."""

    def test_pace_lookups(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        storage = Storage(os.path.join(tmp.name, "blocks"), os.path.join(tmp.name, "state"), 10**9)
        self.addCleanup(storage.close)
        clock = FakeClock(1_000_000.0)
        publisher = Publisher(ident(0), Config(rnfs_dir=tmp.name), storage, ProviderStore(None), FakeClient(),
                              lambda key: None, True, clock=clock)
        self.assertEqual(publisher.pace_lookups(2), 0.0)
        self.assertEqual(publisher.pace_lookups(2), 2 * SLOW_LOOKUP_PACE)  # waits for the two counted
        clock.now += 2 * SLOW_LOOKUP_PACE
        self.assertEqual(publisher.pace_lookups(1), 0.0)
        # The next publish round waits for it as for its own lookups
        self.assertGreater(publisher._next_lookup, clock.now)

if __name__ == "__main__":
    unittest.main()
