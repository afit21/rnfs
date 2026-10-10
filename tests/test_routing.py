"""Tests for the DHT routing table: buckets, replacement, failures, seniority, bans and reply filtering."""
import random
import threading
import unittest

from rnfs.config import Config
from rnfs.constants import BUCKET_COUNT, ID_LENGTH, K
from rnfs.dht.routing import RoutingTable, distance

OWN_ID = bytes(ID_LENGTH)
ANNOUNCE = 6 * 3600


def node_id(n: int) -> bytes:
    """The node ID whose big-endian value is n."""
    return n.to_bytes(ID_LENGTH, "big")


class FakeClock:
    """A manually advanced clock."""

    def __init__(self):
        """Clock at an arbitrary start time; tests advance it by setting now."""
        self.now = 1000.0

    def __call__(self) -> float:
        """The current fake time."""
        return self.now


def bucket_ids(bucket: int, count: int) -> list[bytes]:
    """count distinct IDs that fall in the given bucket of OWN_ID's table."""
    return [node_id((1 << bucket) + i) for i in range(count)]


class RoutingTestCase(unittest.TestCase):
    """Base: a table for OWN_ID with a fake clock and default settings."""

    def setUp(self):
        """Create the table."""
        self.clock = FakeClock()
        self.config = Config(announce_interval=ANNOUNCE, max_failures=3, ban_time=3600)
        self.table = RoutingTable(OWN_ID, self.config, clock=self.clock)


class BucketTest(RoutingTestCase):
    """Bucket placement and basic membership."""

    def test_distance_is_big_endian_xor(self):
        self.assertEqual(distance(node_id(0b1100), node_id(0b1010)), 0b0110)
        self.assertEqual(distance(node_id(1 << 127), OWN_ID), 1 << 127)

    def test_bucket_index_is_highest_differing_bit(self):
        self.assertEqual(self.table.bucket_index(node_id(1)), 0)
        self.assertEqual(self.table.bucket_index(node_id(0b1010)), 3)
        self.assertEqual(self.table.bucket_index(node_id(1 << 127)), BUCKET_COUNT - 1)
        with self.assertRaises(ValueError):
            self.table.bucket_index(OWN_ID)

    def test_rejects_malformed_ids(self):
        with self.assertRaises(ValueError):
            self.table.heard(b"short")
        with self.assertRaises(ValueError):
            RoutingTable(b"x", self.config)

    def test_heard_adds_once(self):
        self.assertTrue(self.table.heard(node_id(5)))
        self.assertFalse(self.table.heard(node_id(5)))  # already there: refreshed
        self.assertTrue(self.table.contains(node_id(5)))
        self.assertEqual(self.table.node_count(), 1)

    def test_ignores_own_id(self):
        self.assertFalse(self.table.heard(OWN_ID))
        self.assertEqual(self.table.node_count(), 0)
        self.assertFalse(self.table.failed(OWN_ID))

    def test_admit_predicate_filters_nodes(self):
        table = RoutingTable(OWN_ID, self.config, clock=self.clock, admit=lambda n: n == node_id(1))
        self.assertTrue(table.heard(node_id(1)))
        self.assertFalse(table.heard(node_id(2)))
        self.assertEqual(table.nodes(), [node_id(1)])

    def test_closest_ranks_every_bucket(self):
        rng = random.Random(1)
        ids = [rng.randbytes(ID_LENGTH) for _ in range(300)]
        for n in ids:
            self.table.heard(n)
        key = rng.randbytes(ID_LENGTH)
        stored = self.table.nodes()
        expected = sorted(stored, key=lambda n: distance(n, key))[:K]
        self.assertEqual(self.table.closest(key), expected)
        self.assertEqual(len(self.table.closest(key, count=3)), 3)


class ReplacementTest(RoutingTestCase):
    """A full bucket keeps its nodes unless the least recently heard has stopped answering or gone silent."""

    def setUp(self):
        """Fill bucket 10 with K nodes heard one second apart."""
        super().setUp()
        self.members = bucket_ids(10, K)
        for n in self.members:
            self.table.heard(n)
            self.clock.now += 1
        self.newcomer = bucket_ids(10, K + 1)[-1]

    def test_full_bucket_keeps_healthy_nodes(self):
        self.assertFalse(self.table.heard(self.newcomer))
        self.assertFalse(self.table.contains(self.newcomer))
        self.assertEqual(sorted(self.table.nodes()), sorted(self.members))

    def test_a_flood_of_newcomers_cannot_displace_healthy_nodes(self):
        for i in range(500):
            self.table.heard(node_id((1 << 10) + K + i))
        self.assertEqual(sorted(self.table.nodes()), sorted(self.members))

    def test_newcomer_replaces_least_recently_heard_if_it_failed_since(self):
        self.table.failed(self.members[0])  # members[0] is the least recently heard
        self.assertTrue(self.table.heard(self.newcomer))
        self.assertFalse(self.table.contains(self.members[0]))
        self.assertTrue(self.table.contains(self.newcomer))

    def test_hearing_again_clears_the_failure(self):
        self.table.failed(self.members[0])
        self.table.heard(self.members[0])  # now heard most recently, and healthy
        self.assertFalse(self.table.heard(self.newcomer))

    def test_newcomer_replaces_node_silent_for_two_announce_intervals(self):
        # Every node but the first is heard again; the first stays silent past the limit
        self.clock.now += 2 * ANNOUNCE - 20  # members[0], heard 10 s before the loop ended, is silent 2A - 10
        for n in self.members[1:]:
            self.table.heard(n)
        self.assertFalse(self.table.heard(self.newcomer))  # not yet silent long enough
        self.clock.now += 15
        self.assertTrue(self.table.heard(self.newcomer))
        self.assertFalse(self.table.contains(self.members[0]))

    def test_stale_node_that_is_not_least_recently_heard_is_kept(self):
        # Only the least recently heard node is considered; it is healthy, so the newcomer is refused
        self.table.failed(self.members[4])
        self.assertFalse(self.table.heard(self.newcomer))
        self.assertTrue(self.table.contains(self.members[4]))
        self.assertFalse(self.table.contains(self.newcomer))


class FailureAndSeniorityTest(RoutingTestCase):
    """Dropping after max_failures, and seniority kept for a node that comes back in time."""

    def test_dropped_after_max_failures_in_a_row(self):
        n = node_id(42)
        self.table.heard(n)
        self.assertFalse(self.table.failed(n))
        self.assertFalse(self.table.failed(n))
        self.assertTrue(self.table.failed(n))
        self.assertFalse(self.table.contains(n))
        self.assertFalse(self.table.failed(n))  # not in the table any more

    def test_hearing_resets_the_failure_count(self):
        n = node_id(42)
        self.table.heard(n)
        self.table.failed(n)
        self.table.failed(n)
        self.table.heard(n)
        self.assertFalse(self.table.failed(n))
        self.assertFalse(self.table.failed(n))
        self.assertTrue(self.table.contains(n))

    def test_seniority_counts_from_first_seen(self):
        n = node_id(42)
        self.assertIsNone(self.table.seniority(n))
        self.table.heard(n)
        self.clock.now += 500
        self.table.heard(n)
        self.assertEqual(self.table.seniority(n), 500)

    def drop(self, n: bytes):
        """Fail n until it is dropped."""
        while not self.table.failed(n):
            pass

    def test_dropped_node_regains_seniority_within_two_announce_intervals(self):
        n = node_id(42)
        self.table.heard(n)
        self.clock.now += 10 * ANNOUNCE
        self.drop(n)
        self.clock.now += 2 * ANNOUNCE - 1
        self.assertTrue(self.table.heard(n))
        self.assertEqual(self.table.seniority(n), 12 * ANNOUNCE - 1)

    def test_dropped_node_starts_over_after_two_announce_intervals(self):
        n = node_id(42)
        self.table.heard(n)
        self.clock.now += 10 * ANNOUNCE
        self.drop(n)
        self.clock.now += 2 * ANNOUNCE + 1
        self.table.heard(n)
        self.assertEqual(self.table.seniority(n), 0)

    def test_remove_forgets_seniority(self):
        n = node_id(42)
        self.table.heard(n)
        self.clock.now += 1000
        self.table.remove(n)
        self.assertFalse(self.table.contains(n))
        self.table.heard(n)
        self.assertEqual(self.table.seniority(n), 0)


class ReplyFilterTest(RoutingTestCase):
    """closest(for_reply=True) leaves out nodes that failed since heard or have gone silent."""

    def test_reply_leaves_out_failed_and_silent_nodes(self):
        healthy, failing, silent = node_id(1), node_id(2), node_id(3)
        self.table.heard(silent)
        self.clock.now += 2 * ANNOUNCE + 1
        self.table.heard(healthy)
        self.table.heard(failing)
        self.table.failed(failing)
        key = node_id(0)
        self.assertEqual(self.table.closest(key, for_reply=True), [healthy])
        # Our own lookups still try them
        self.assertEqual(self.table.closest(key), [healthy, failing, silent])


class BanTest(RoutingTestCase):
    """Bans drop a node and ignore it for ban_time."""

    def test_ban_drops_and_ignores_until_it_ends(self):
        n = node_id(42)
        self.table.heard(n)
        self.table.ban(n)
        self.assertFalse(self.table.contains(n))
        self.assertTrue(self.table.is_banned(n))
        self.assertFalse(self.table.heard(n))
        self.clock.now += 3599
        self.assertTrue(self.table.is_banned(n))
        self.assertFalse(self.table.heard(n))
        self.clock.now += 2
        self.assertFalse(self.table.is_banned(n))
        self.assertTrue(self.table.heard(n))
        self.assertEqual(self.table.seniority(n), 0)  # a ban forfeits seniority

    def test_ban_of_unknown_node(self):
        n = node_id(42)
        self.table.ban(n)
        self.assertTrue(self.table.is_banned(n))
        self.assertFalse(self.table.is_banned(node_id(43)))


class ThreadSafetyTest(RoutingTestCase):
    """Concurrent use from announce, request and lookup threads."""

    def test_concurrent_heard_failed_closest(self):
        rng = random.Random(2)
        ids = [rng.randbytes(ID_LENGTH) for _ in range(400)]
        errors = []

        def worker(seed: int):
            """Hammer the table with a mix of calls."""
            r = random.Random(seed)
            try:
                for _ in range(2000):
                    n = r.choice(ids)
                    action = r.random()
                    if action < 0.5:
                        self.table.heard(n)
                    elif action < 0.8:
                        self.table.failed(n)
                    else:
                        self.table.closest(n, for_reply=True)
            except Exception as e:  # pragma: no cover - reported below
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(self.table.node_count(), len(self.table.nodes()))


if __name__ == "__main__":
    unittest.main()
