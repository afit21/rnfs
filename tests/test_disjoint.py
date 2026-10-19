"""Tests for disjoint lookup paths (rnfs/dht/disjoint.py) over simulated networks."""
import random
import threading
import time
import unittest

from rnfs.constants import ID_LENGTH, K
from rnfs.dht.disjoint import Claims, DisjointLookup
from rnfs.dht.lookup import Lookup
from rnfs.dht.routing import distance
from tests.fakes.log import quiet_log
from tests.test_lookup import FakeNetwork, LookupTestCase, ident

setUpModule, tearDownModule = quiet_log()


class DisjointTest(LookupTestCase):
    """Disjoint lookup paths over a simulated network."""

    def make(self, net: FakeNetwork, key: bytes, start: list[bytes], paths: int = 3, **kwargs) -> DisjointLookup:
        """A disjoint lookup over net."""
        def path(start_nodes, seniors, claim, taken):
            """One path's Lookup."""
            return self.lookup(net, key, start_nodes, seniors=seniors, claim=claim, taken=taken, **kwargs)
        return DisjointLookup(key, start, path, paths)

    def test_claims(self):
        claims = Claims()
        self.assertTrue(claims.claim(0, ident(1)))
        self.assertTrue(claims.claim(0, ident(1)))
        self.assertFalse(claims.claim(1, ident(1)))
        self.assertTrue(claims.taken(1, ident(1)))
        self.assertFalse(claims.taken(0, ident(1)))
        self.assertFalse(claims.taken(1, ident(2)))

    def test_paths_share_no_node_and_find_the_closest(self):
        net = self.network(400, seed=20)
        rng = random.Random(21)
        for _ in range(5):
            key = rng.randbytes(ID_LENGTH)
            net.queried.clear()
            lookup = self.make(net, key, rng.sample(list(net.nodes), K))
            result = lookup.find_closest()
            self.assertEqual(len(net.queried), len(set(net.queried)))  # no node asked twice: by no two paths
            self.assertEqual(result[:3], net.true_closest(key, 3))
            self.assertEqual(len(result), K)
            self.assertEqual(lookup.ended, "complete")
            per_path = lookup.path_candidates()
            self.assertEqual(len(per_path), 3)
            self.assertEqual(set(lookup.candidates()), set().union(*per_path))

    def test_start_nodes_dealt_to_the_paths(self):
        start = [ident(n) for n in range(1, 8)]
        made = []

        def path(start_nodes, seniors, claim, taken):
            """Note what each path was given."""
            made.append(start_nodes)
            return Lookup(ident(0), start_nodes, lambda node_id, key: None, ident(99))
        DisjointLookup(ident(0), start, path, 3)
        self.assertEqual(made, [start[0::3], start[1::3], start[2::3]])

    def test_find_providers_reports_in_the_callers_thread(self):
        net = self.network(300, seed=22)
        key = random.Random(23).randbytes(ID_LENGTH)
        net.publish(key, ident(5))
        threads = []
        lookup = self.make(net, key, random.Random(24).sample(list(net.nodes), K))
        found = lookup.find_providers(lambda provider: threads.append(threading.current_thread()))
        self.assertEqual(found, [ident(5)])
        self.assertEqual(threads, [threading.current_thread()])
        self.assertTrue(lookup.listing_nodes())
        self.assertTrue(lookup.listers(ident(5)))
        self.assertEqual(lookup.closest_answered()[:3], net.true_closest(key, 3))
        self.assertEqual(lookup.find_providers(lambda provider: None), [])  # reported once only

    def test_should_stop_ends_every_path(self):
        net = self.network(200, seed=25)
        key = random.Random(26).randbytes(ID_LENGTH)
        lookup = self.make(net, key, random.Random(27).sample(list(net.nodes), K))
        started = time.monotonic()
        self.assertEqual(lookup.find_providers(lambda provider: None, should_stop=lambda: True), [])
        self.assertLess(time.monotonic() - started, 2)

    def test_paths_share_the_samples_of_the_size_estimate(self):
        net = self.network(300, seed=28)
        key = random.Random(29).randbytes(ID_LENGTH)
        samples = []

        def path(start_nodes, seniors, claim, taken):
            """A path that would sample on its own (it mustn't)."""
            return self.lookup(net, key, start_nodes, claim=claim, taken=taken)
        lookup = DisjointLookup(key, random.Random(30).sample(list(net.nodes), K), path, 2,
                                on_complete=lambda k, kth: samples.append(kth))
        lookup.find_closest()
        self.assertEqual(len(samples), 1)  # one for the paths together
        self.assertLess(abs(samples[0] / distance(net.true_closest(key)[-1], key) - 1), 0.5)


if __name__ == "__main__":
    unittest.main()
