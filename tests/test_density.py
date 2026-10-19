"""Tests for the network size estimate and the density check (rnfs/dht/density.py), and the checks a Lookup runs
with them (rnfs/dht/lookup.py: the density check's rings and the senior rule). How well the defences work together
under attack is simulated in tests/test_eclipse.py."""
import random
import tempfile
import unittest

from rnfs.constants import ID_LENGTH, K
from rnfs.dht import density
from rnfs.dht.density import (MIN_SIZE_SAMPLES, RING_ANSWERS, SIZE_SAMPLES, SPACE, SizeEstimate, dense_core,
                              kth_distance, radius, rings)
from rnfs.dht.lookup import Lookup
from rnfs.dht.routing import distance
from tests.fakes.log import quiet_log
from tests.support import state_execute, state_rows
from tests.test_lookup import FakeNetwork, LookupTestCase, ident

setUpModule, tearDownModule = quiet_log()


def spaced(count: int, size: float, start: int = 1) -> list[int]:
    """count distances as servers spread evenly over a network of size servers would lie from a key, rank start on."""
    return [rank * SPACE // int(size) for rank in range(start, start + count)]


class SizeEstimateTest(unittest.TestCase):
    """The pooled size estimate."""

    def test_median_of_the_samples_of_other_keys(self):
        sizes = SizeEstimate()
        self.assertIsNone(sizes.estimate())
        size = 1000
        kth = K * SPACE // size
        keys = [ident(n) for n in range(1, MIN_SIZE_SAMPLES + 1)]
        for key in keys[:-1]:
            sizes.note(key, kth)
        self.assertIsNone(sizes.estimate())  # one sample short
        sizes.note(keys[-1], kth)
        self.assertAlmostEqual(sizes.estimate(), (K - 1 / 3) / K * size, delta=1)
        # A key's own sample is left out: identities placed next to it can't shrink its yardstick
        self.assertIsNone(sizes.estimate(keys[0]))
        sizes.note(ident(99), 1)  # an outlier: the median barely moves
        self.assertAlmostEqual(sizes.estimate(), (K - 1 / 3) / K * size, delta=1)
        self.assertIsNotNone(sizes.estimate(keys[0]))

    def test_latest_sample_per_key_and_at_most_the_last_samples(self):
        sizes = SizeEstimate(minimum=1)
        sizes.note(ident(1), 100)
        sizes.note(ident(1), 200)  # the same key again: replaced
        self.assertEqual(len(sizes), 1)
        self.assertAlmostEqual(sizes.estimate(), (K - 1 / 3) * SPACE / 200)
        for n in range(2, 2 * SIZE_SAMPLES):
            sizes.note(ident(n), 1000)
        self.assertEqual(len(sizes), SIZE_SAMPLES)
        self.assertAlmostEqual(sizes.estimate(), (K - 1 / 3) * SPACE / 1000)  # the first ones forgotten
        sizes.note(b"short", 5)
        sizes.note(ident(500), 0)
        sizes.note(ident(501), SPACE)
        self.assertEqual(len(sizes), SIZE_SAMPLES)

    def test_kept_across_restarts(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        sizes = SizeEstimate(tmp.name)
        for n in range(1, MIN_SIZE_SAMPLES + 1):
            sizes.note(ident(n), n * 1000)
        sizes.save()
        before = sizes.estimate()
        sizes.close()
        again = SizeEstimate(tmp.name)
        self.addCleanup(again.close)
        self.assertEqual(again.estimate(), before)
        self.assertEqual(len(again), MIN_SIZE_SAMPLES)
        again.save()  # nothing changed: nothing written
        self.assertEqual(len(state_rows(tmp.name, "SELECT value FROM meta WHERE store = ?", (density.META_STORE,))),
                         1)

    def test_malformed_saved_samples_are_ignored(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        SizeEstimate(tmp.name).close()  # creates the database
        state_execute(tmp.name, "INSERT OR REPLACE INTO meta (store, key, value) VALUES (?, 'samples', ?)",
                      (density.META_STORE, b"not a multiple of 32 bytes"))
        sizes = SizeEstimate(tmp.name)
        self.addCleanup(sizes.close)
        self.assertEqual(len(sizes), 0)
        sizes.save()  # rewritten as it is now
        self.assertEqual(state_rows(tmp.name, "SELECT value FROM meta WHERE store = ?", (density.META_STORE,)),
                         [(b"",)])

    def test_kth_distance(self):
        self.assertIsNone(kth_distance(list(range(K - 1))))
        self.assertEqual(kth_distance(list(reversed(range(1, 2 * K)))), K)


class DensityCheckTest(unittest.TestCase):
    """dense_core and rings."""

    SIZE = 1000.0

    def test_natural_spacing_does_not_fire(self):
        self.assertEqual(dense_core(spaced(60, self.SIZE), self.SIZE), 0)
        # Twice as dense as estimated is still natural enough
        self.assertEqual(dense_core(spaced(60, 1.9 * self.SIZE), self.SIZE), 0)

    def test_packed_identities_fire(self):
        placed = [n + 1 for n in range(K)]  # K identities closer than any honest server
        honest = spaced(50, self.SIZE)
        core = dense_core(sorted(placed + honest), self.SIZE)
        self.assertGreaterEqual(core, K)
        self.assertLessEqual(core, 2 * K)  # the honest ranks after them are natural again

    def test_ranks_below_k_never_fire(self):
        placed = [n + 1 for n in range(K - 1)]
        honest = spaced(50, self.SIZE, start=K)  # natural from rank K on
        self.assertEqual(dense_core(sorted(placed + honest), self.SIZE), 0)

    def test_rings_after_the_core_and_beyond_the_radius(self):
        nodes = [ident(n) for n in range(80)]
        placed = [n + 1 for n in range(20)]
        distances = sorted(placed + spaced(60, self.SIZE))
        core = dense_core(distances, self.SIZE)
        found = rings(nodes, distances, self.SIZE, 3)
        beyond = next(i for i, d in enumerate(distances) if d >= radius(self.SIZE))
        self.assertEqual(found[:3], nodes[core:core + 3])
        self.assertTrue(set(nodes[beyond:beyond + 3]) <= set(found))
        self.assertEqual(len(found), len(set(found)))
        # Unreliable nodes come last within a ring
        later = {nodes[core]}
        self.assertNotIn(nodes[core], rings(nodes, distances, self.SIZE, 3, later=later.__contains__)[:3])

    def test_no_rings_without_a_core_or_an_estimate(self):
        nodes = [ident(n) for n in range(60)]
        distances = spaced(60, self.SIZE)
        self.assertEqual(rings(nodes, distances, self.SIZE, 5), [])
        self.assertEqual(rings(nodes, sorted([1] * 20 + distances[:40]), None, 5), [])


class AttackedNetwork(FakeNetwork):
    """A FakeNetwork of honest servers and count identities placed closer to key than any of them (liars: they answer
    with each other only), which the honest servers whose buckets have room have heard."""

    def __init__(self, servers: int, seed: int, key: bytes, count: int):
        """The honest network, then the identities."""
        super().__init__(servers, seed=seed)
        nearest = min(distance(n, key) for n in self.nodes)
        rng = random.Random(seed + 1)
        honest = list(self.nodes)
        self.placed = [(int.from_bytes(key, "big") ^ rng.randrange(1, nearest)).to_bytes(ID_LENGTH, "big")
                       for _ in range(count)]
        for n in self.placed:
            self.add(n)
            self.liars.add(n)
        for n in honest:
            for placed in self.placed:
                self.nodes[n].table.heard(placed)  # only where the bucket has room ("fresh" identities)

    def honest_closest(self, key: bytes, count: int = K) -> list[bytes]:
        """The count honest servers closest to key."""
        return sorted((n for n in self.nodes if n not in self.liars), key=lambda n: distance(n, key))[:count]


class LookupChecksTest(LookupTestCase):
    """The density check and the senior rule inside a Lookup."""

    def setUp(self):
        """300 honest servers, 12 identities next to the key, a publisher's record on the 10 honest servers closest."""
        super().setUp()
        self.key = random.Random(5).randbytes(ID_LENGTH)
        self.net = AttackedNetwork(300, 6, self.key, 12)
        self.networks.append(self.net)
        self.publisher = ident(7)
        for n in self.net.honest_closest(self.key, 2 * K):
            self.net.nodes[n].store.store(self.key, self.publisher)
        self.start = random.Random(8).sample(list(set(self.net.nodes) - self.net.liars), K)
        rng = random.Random(9)
        self.size = SizeEstimate()
        for _ in range(SIZE_SAMPLES):
            other = rng.randbytes(ID_LENGTH)
            self.lookup(self.net, other, rng.sample(self.start, 3), on_complete=self.size.note).find_closest()

    def search(self, **kwargs) -> tuple[list[bytes], Lookup]:
        """find_providers for the key from the start nodes: the providers found, and the lookup."""
        lookup = self.lookup(self.net, self.key, self.start, **kwargs)
        self.net.queried.clear()
        found: list[bytes] = []
        lookup.find_providers(found.append)
        return found, lookup

    def test_size_estimate_from_complete_lookups(self):
        estimate = self.size.estimate()
        self.assertIsNotNone(estimate)
        self.assertGreater(estimate, 300 / 2)
        self.assertLess(estimate, 300 * 2)

    def test_identities_next_to_the_key_are_a_plain_lookups_closest(self):
        lookup = self.lookup(self.net, self.key, self.start)
        self.assertTrue(set(lookup.find_closest()) <= self.net.liars)  # a publisher would store only on them
        self.assertFalse(lookup.dense)

    def test_the_density_check_reaches_past_them(self):
        found, lookup = self.search(size=self.size.estimate(self.key))
        self.assertEqual(found, [self.publisher])
        self.assertTrue(lookup.dense)
        self.assertEqual(lookup.ended, "complete")

    def test_the_senior_rule_asks_the_old_servers_near_the_key(self):
        seniors = self.net.honest_closest(self.key)
        found, lookup = self.search(seniors=seniors)
        self.assertEqual(found, [self.publisher])
        self.assertFalse(lookup.dense)
        # The beta closest of them answered
        self.assertTrue(set(seniors[:3]) <= set(self.net.queried))

    def test_senior_rule_costs_nothing_when_they_are_the_closest(self):
        """Without identities next to the key the senior servers are the beta closest: no query more."""
        net = self.network(300, seed=10)
        key = random.Random(11).randbytes(ID_LENGTH)
        start = random.Random(12).sample(list(net.nodes), K)
        plain = Lookup(key, start, net.query, ident(0), timeout=5, poll_interval=0.05)
        plain.find_closest()
        asked = len(net.queried)
        net.queried.clear()
        checked = Lookup(key, start, net.query, ident(0), timeout=5, poll_interval=0.05,
                         seniors=net.true_closest(key))
        checked.find_closest()
        self.assertLessEqual(len(net.queried), asked + 2)  # replies arrive in another order: a query or two
        self.assertTrue(set(net.true_closest(key, 3)) <= set(net.queried))

    def test_ring_answers_do_not_count_against_the_stall_rule(self):
        """The ring servers asked don't bring the key nearer, yet the run waits for RING_ANSWERS of each ring."""
        found, lookup = self.search(size=self.size.estimate(self.key))
        honest_asked = [n for n in dict.fromkeys(self.net.queried) if n not in self.net.liars]
        far = [n for n in honest_asked if distance(n, self.key) > distance(self.net.honest_closest(self.key)[0],
                                                                            self.key)]
        self.assertGreaterEqual(len(far), RING_ANSWERS)
        self.assertEqual(lookup.ended, "complete")

    def test_checks_stay_within_the_query_cap(self):
        found, lookup = self.search(size=self.size.estimate(self.key), max_queries=6)
        self.assertLessEqual(len(self.net.queried), 6)
        self.assertEqual(lookup.ended, "query cap")


if __name__ == "__main__":
    unittest.main()
