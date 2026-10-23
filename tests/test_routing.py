"""Tests for the DHT routing table: buckets, replacement, failures, seniority, bans and reply filtering."""
import random
import tempfile
import threading
import unittest
from unittest import mock

from rnfs.config import Config
from rnfs.constants import BUCKET_COUNT, ID_LENGTH, K
from rnfs.dht import routing
from rnfs import statedb
from rnfs.dht.routing import AVOID_AFTER, AVOID_MAX, RoutingTable, distance
from rnfs.dht.routing_state import RoutingState
from tests.support import FakeClock, state_execute, state_rows

OWN_ID = bytes(ID_LENGTH)
ANNOUNCE = 6 * 3600


def node_id(n: int) -> bytes:
    """The node ID whose big-endian value is n."""
    return n.to_bytes(ID_LENGTH, "big")


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

    def test_admit_is_asked_without_the_lock(self):
        """admit may block on RNS, so heard() and answered() ask it with the table's lock free for other
        threads (those answering /find, and lookups), and only for nodes not already in the table."""
        asked, free = [], []

        def admit(n: bytes) -> bool:
            """Note whether another thread could take the table's lock now."""
            def probe():
                """Take and release the lock without waiting."""
                if table._lock.acquire(blocking=False):
                    table._lock.release()
                    free.append(n)
            thread = threading.Thread(target=probe)
            thread.start()
            thread.join()
            asked.append(n)
            return True

        table = RoutingTable(OWN_ID, self.config, clock=self.clock, admit=admit)
        self.assertTrue(table.heard(node_id(1)))
        self.assertTrue(table.answered(node_id(2)))
        self.assertFalse(table.heard(node_id(1)))  # in the table: not asked again
        self.assertEqual(asked, [node_id(1), node_id(2)])
        self.assertEqual(free, asked)

    def test_changes_while_admit_is_asked_are_honoured(self):
        """The table is checked again once admit answers: a node banned meanwhile isn't added, and one another
        thread added meanwhile is only refreshed, whatever admit said."""
        during = []

        def admit(n: bytes) -> bool:
            """Run the next change queued in during, as another thread could, then answer as it says."""
            return during.pop()(n) if during else True

        table = RoutingTable(OWN_ID, self.config, clock=self.clock, admit=admit)
        during.append(lambda n: table.ban(n) or True)
        self.assertFalse(table.heard(node_id(1)))
        self.assertFalse(table.contains(node_id(1)))
        during.append(lambda n: table.heard(n) and False)
        self.assertFalse(table.answered(node_id(2)))
        self.assertEqual(table.nodes(), [node_id(2)])

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

    def test_closest_matches_ranking_every_node(self):
        """Walking buckets from the key's gives exactly what sorting the whole table by distance gives."""
        rng = random.Random(3)
        for _ in range(60):
            own = rng.randbytes(ID_LENGTH)
            own_value = int.from_bytes(own, "big")
            table = RoutingTable(own, self.config, clock=self.clock)
            # Random IDs fill the far buckets; IDs sharing a prefix with own reach the near ones, some left empty
            ids = [rng.randbytes(ID_LENGTH) for _ in range(rng.randrange(0, 300))]
            for bucket in rng.sample(range(BUCKET_COUNT), rng.randrange(0, BUCKET_COUNT)):
                for _ in range(rng.randrange(1, K + 3)):
                    ids.append((own_value ^ (1 << bucket) ^ rng.getrandbits(bucket)).to_bytes(ID_LENGTH, "big"))
            for n in ids:
                table.heard(n)
            stored = table.nodes()
            # Some go silent or fail a query, so replies leave them out
            self.clock.now += 2 * ANNOUNCE + 1
            for n in stored:
                if rng.random() < 0.5:
                    table.heard(n)
                    if rng.random() < 0.3:
                        table.failed(n)
            live = [n for n in stored if table.standing(n, 0) < 2]
            keys = [own, rng.randbytes(ID_LENGTH)] + rng.sample(stored, min(3, len(stored)))
            keys += [(own_value ^ rng.getrandbits(rng.randrange(1, 129))).to_bytes(ID_LENGTH, "big") for _ in range(3)]
            for key in keys:
                for count in (0, 1, 3, K, len(stored) + 5):
                    expected = sorted(stored, key=lambda n: distance(n, key))[:count]
                    self.assertEqual(table.closest(key, count), expected)
                    expected = sorted(live, key=lambda n: distance(n, key))[:count]
                    self.assertEqual(table.closest(key, count, for_reply=True), expected)


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


class JoinOrderTest(RoutingTestCase):
    """join_order(): the servers a join asks before bootstrap nodes."""

    def test_longest_known_first_leaving_out_those_that_failed_since_heard(self):
        young, old, failing, silent = node_id(1), node_id(2), node_id(3), node_id(4)
        self.table.heard(old)
        self.table.heard(silent)
        self.clock.now += 10
        for n in (young, failing):
            self.table.heard(n)
        self.table.failed(failing)
        self.clock.now += 2 * ANNOUNCE + 1  # silent ones are still asked: after a long downtime, all are
        self.table.heard(young)
        self.assertEqual(self.table.join_order(), [old, silent, young])


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

    def test_live_count_leaves_out_the_same_nodes(self):
        healthy, failing, silent = node_id(1), node_id(2), node_id(3)
        self.table.heard(silent)
        self.clock.now += 2 * ANNOUNCE + 1
        self.table.heard(healthy)
        self.table.heard(failing)
        self.table.failed(failing)
        self.assertEqual((self.table.live_count(), self.table.node_count()), (1, 3))
        self.table.heard(failing)  # hearing it again makes it live
        self.assertEqual(self.table.live_count(), 2)


class UnreliableTest(RoutingTestCase):
    """Nodes whose queries keep failing are unreliable however often they announce, until one is answered."""

    def fail_queries(self, n: bytes, times: int):
        """Count times failed queries for n, hearing its announce before each, as a far node that keeps
        announcing but never connects is heard."""
        for _ in range(times):
            self.table.heard(n)
            self.table.failed(n)

    def test_announces_do_not_reset_the_count(self):
        """A node heard before every failure is unreliable after max_failures of them, and stays in the table."""
        n = node_id(42)
        self.fail_queries(n, 2)
        self.assertFalse(self.table.unreliable(n))
        self.fail_queries(n, 1)
        self.assertTrue(self.table.unreliable(n))
        self.table.heard(n)
        self.assertTrue(self.table.unreliable(n))
        self.assertTrue(self.table.contains(n))  # the table's own failure count was reset by the announce

    def test_nodes_outside_the_table_count_too(self):
        """A node only named in replies (never in the table) is remembered as unreliable."""
        n = node_id(42)
        for _ in range(3):
            self.assertFalse(self.table.failed(n))
        self.assertFalse(self.table.contains(n))
        self.assertTrue(self.table.unreliable(n))

    def test_retried_after_a_doubling_backoff(self):
        """may_retry is True once the backoff has passed, for one caller; each failure doubles it up to AVOID_MAX."""
        n = node_id(42)
        self.fail_queries(n, 3)
        self.assertFalse(self.table.may_retry(n))
        self.clock.now += AVOID_AFTER
        self.assertTrue(self.table.may_retry(n))
        self.assertFalse(self.table.may_retry(n))  # claimed: another lookup doesn't retry it too
        self.fail_queries(n, 1)
        self.clock.now += 2 * AVOID_AFTER - 1
        self.assertFalse(self.table.may_retry(n))
        self.clock.now += 1
        self.assertTrue(self.table.may_retry(n))
        self.fail_queries(n, 20)
        self.clock.now += AVOID_MAX - 1
        self.assertFalse(self.table.may_retry(n))
        self.clock.now += 1
        self.assertTrue(self.table.may_retry(n))

    def test_an_answer_restores_it(self):
        """answered() makes the node reliable again: no retry needed, and failures count from zero."""
        n = node_id(42)
        self.fail_queries(n, 5)
        self.assertFalse(self.table.answered(n))  # already in the table
        self.assertFalse(self.table.unreliable(n))
        self.clock.now += AVOID_MAX
        self.assertFalse(self.table.may_retry(n))
        self.fail_queries(n, 2)
        self.assertFalse(self.table.unreliable(n))

    def test_answered_adds_like_heard(self):
        """answered() adds a node not yet in the table, as hearing it would."""
        n = node_id(42)
        self.assertTrue(self.table.answered(n))
        self.assertTrue(self.table.contains(n))
        self.assertFalse(self.table.answered(OWN_ID))

    def test_reliable_nodes_are_unaffected(self):
        """Fewer than max_failures failures in a row leave a node reliable, with no retry to claim."""
        n = node_id(42)
        self.fail_queries(n, 2)
        self.assertFalse(self.table.unreliable(n))
        self.assertFalse(self.table.may_retry(n))
        self.assertFalse(self.table.unreliable(node_id(43)))

    def test_memory_is_bounded(self):
        """At most MAX_UNRELIABLE nodes are remembered; the least recently failed is forgotten first."""
        old = routing.MAX_UNRELIABLE
        routing.MAX_UNRELIABLE = 4
        self.addCleanup(setattr, routing, "MAX_UNRELIABLE", old)
        ids = [node_id(100 + i) for i in range(5)]
        for n in ids:
            self.fail_queries(n, 3)
        self.assertFalse(self.table.unreliable(ids[0]))
        self.assertTrue(all(self.table.unreliable(n) for n in ids[1:]))


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


class SnapshotTest(RoutingTestCase):
    """snapshot() (GET /api/routing, 0.2.1): the buckets holding nodes, unreliable nodes and bans, as wall time."""

    def test_snapshot(self):
        self.table.wall_clock = lambda: self.clock.now + 1_000_000  # the wall clock, a fixed offset ahead
        old, newer = bucket_ids(3, 2)
        self.table.heard(old)
        self.clock.now += 10
        self.table.heard(newer)
        self.table.failed(newer)
        far = node_id(1 << 100)
        self.table.heard(far)
        for _ in range(3):
            self.table.failed(node_id(7))  # never in the table: unreliable all the same
        self.table.ban(node_id(99), "it sent a block not matching the CID requested")
        self.table.ban(node_id(98))
        snap = self.table.snapshot()
        self.assertEqual((snap["k"], snap["nodes"], snap["dropped"]), (K, 3, 0))
        self.assertEqual([bucket["index"] for bucket in snap["buckets"]], [3, 100])
        first, second = snap["buckets"][0]["nodes"]
        self.assertEqual((first["node_id"], first["failures"], first["stale"]), (old.hex(), 0, False))
        self.assertEqual((second["node_id"], second["failures"], second["stale"]), (newer.hex(), 1, True))
        self.assertEqual(first["first_seen"], self.clock.now - 10 + 1_000_000)
        self.assertEqual([(entry["node_id"], entry["failures"]) for entry in snap["unreliable"]],
                         [(node_id(7).hex(), 3)])
        self.assertEqual([(ban["node_id"], ban["reason"]) for ban in snap["bans"]],
                         [(node_id(99).hex(), "it sent a block not matching the CID requested"), (node_id(98).hex(), None)])
        self.assertEqual(snap["bans"][0]["until"], self.clock.now + 3600 + 1_000_000)
        self.assertEqual(self.table.snapshot(max_listed=1)["bans_total"], 2)
        self.assertEqual(len(self.table.snapshot(max_listed=1)["bans"]), 1)
        self.clock.now += 3601
        self.assertEqual(self.table.snapshot()["bans"], [])  # ended, and their reasons forgotten
        self.assertEqual(self.table._ban_reasons, {})



class PruneTest(RoutingTestCase):
    """Expired bans and dropped nodes are forgotten from the front of their expiry order (S4), and the outcome is
    the same when an entry is out of order."""

    def drop(self, n: bytes):
        """Add n and fail it until it is dropped."""
        self.table.heard(n)
        self.assertTrue(self.table.contains(n))
        while not self.table.failed(n):
            pass

    def test_expired_entries_are_forgotten_when_a_node_is_heard(self):
        dropped, banned = [node_id(100 + i) for i in range(3)], [node_id(200 + i) for i in range(3)]
        for d, b in zip(dropped, banned):
            self.drop(d)
            self.table.ban(b)
            self.clock.now += 10
        self.clock.now = 1000 + 3600 + 15  # the first two bans have ended
        self.table.heard(node_id(5))
        self.assertEqual(list(self.table._bans), banned[2:])
        self.assertEqual(list(self.table._dropped), dropped)
        self.clock.now = 1000 + 2 * ANNOUNCE + 5  # the first drop has expired, and every ban
        self.table.heard(node_id(5))
        self.assertEqual(list(self.table._bans), [])
        self.assertEqual(list(self.table._dropped), dropped[1:])

    def test_setting_again_moves_an_entry_to_the_end(self):
        first, second = node_id(42), node_id(43)
        self.table.ban(first)
        self.clock.now += 10
        self.table.ban(second)
        self.clock.now += 10
        self.table.ban(first)  # ends after second's ban now
        self.assertEqual(list(self.table._bans), [second, first])
        self.clock.now += 3600 - 5  # second's ban has ended, not first's
        self.assertTrue(self.table.heard(second))
        self.assertNotIn(second, self.table._bans)
        self.assertFalse(self.table.heard(first))
        first, second = node_id(44), node_id(45)
        self.drop(first)
        self.clock.now += 10
        self.drop(second)
        self.clock.now += 10
        self.drop(first)  # back, then dropped again: after second
        self.assertEqual(list(self.table._dropped), [second, first])

    def test_out_of_order_entries_still_expire(self):
        # A changed ban_time (or a clock step) can put a later-ending entry first: it then holds the ones behind it
        # in memory, but they no longer count
        long_ban, short_ban = node_id(42), node_id(43)
        self.table.ban(long_ban)
        self.config.ban_time = 10
        self.table.ban(short_ban)
        self.clock.now += 11
        self.assertTrue(self.table.heard(short_ban))
        self.assertFalse(self.table.heard(long_ban))
        late, early = node_id(44), node_id(45)
        self.clock.now = 5000
        self.drop(late)
        self.clock.now = 1000  # stepped back
        self.drop(early)
        self.clock.now = 1000 + 2 * ANNOUNCE + 1  # early's seniority has expired, late's hasn't
        self.table.heard(node_id(5))
        self.assertEqual(list(self.table._dropped), [late, early])  # held behind late
        self.assertTrue(self.table.heard(early))
        self.assertEqual(self.table.seniority(early), 0)
        self.assertTrue(self.table.heard(late))
        self.assertEqual(self.table.seniority(late), 1000 + 2 * ANNOUNCE + 1 - 5000)

class PersistenceTest(RoutingTestCase):
    """row() and restore(): what a restart keeps, with times saved as wall clock and the monotonic clock restarted."""

    WALL = 1_700_000_000.0

    def setUp(self):
        """The table of RoutingTestCase, with a wall clock WALL - 1000 seconds ahead of its monotonic one."""
        super().setUp()
        self.wall = FakeClock(self.WALL)
        self.table = RoutingTable(OWN_ID, self.config, clock=self.clock, wall_clock=self.wall)

    def tick(self, seconds: float):
        """Move both clocks on."""
        self.clock.now += seconds
        self.wall.now += seconds

    def saved(self, table: RoutingTable | None = None) -> list[tuple]:
        """Every row the table would save now."""
        table = table or self.table
        candidates = [node_id(n) for n in range(1, 400)]
        return [row for row in map(table.row, candidates) if row is not None]

    def restarted(self, downtime: float, rows: list[tuple] | None = None, **kwargs) -> RoutingTable:
        """A new table loading the saved rows after downtime, on a monotonic clock started anew (at 5)."""
        rows = self.saved() if rows is None else rows
        self.wall.now += downtime
        self.clock = FakeClock(5.0)
        table = RoutingTable(OWN_ID, self.config, clock=self.clock, wall_clock=self.wall, **kwargs)
        self.changed = table.restore(rows)
        return table

    def test_round_trip_keeps_seniority_last_heard_failures_drops_and_bans(self):
        old, fresh, failing, dropped, banned = (node_id(n) for n in (10, 11, 12, 13, 14))
        self.table.heard(old)
        self.tick(5000)
        for n in (fresh, failing, dropped):
            self.table.heard(n)
        self.table.failed(failing)
        while not self.table.failed(dropped):
            pass
        self.table.ban(banned)
        self.tick(100)
        rows = self.saved()
        self.assertEqual({row[0] for row in rows}, {old, fresh, failing, dropped, banned})
        self.assertIn((old, routing.IN_TABLE, self.WALL, self.WALL, 0), rows)
        table = self.restarted(600, rows)
        self.assertEqual(self.changed, [])
        self.assertEqual(sorted(table.nodes()), [old, fresh, failing])
        self.assertEqual(table.seniority(old), 5000 + 100 + 600)
        self.assertEqual(table.seniority(fresh), 100 + 600)
        self.assertFalse(table.unreliable(failing))  # failed queries in a row aren't saved ...
        self.assertNotIn(failing, table.closest(failing, for_reply=True))  # ... but it failed since last heard
        self.assertTrue(table.is_banned(banned))
        self.clock.now += 3600 - 700
        self.assertFalse(table.is_banned(banned))  # the ban still ends ban_time after it began
        self.assertTrue(table.heard(dropped))  # back within 2 × announce_interval of its drop: seniority regained
        self.assertEqual(table.seniority(dropped), 5000 + 100 + 600 + 2900 - 5000)
        self.assertEqual(self.saved(table), [table.row(n) for n in sorted(r[0] for r in self.saved(table))])

    def test_find_replies_still_leave_out_nodes_silent_since_before_the_restart(self):
        quiet, recent = node_id(20), node_id(21)
        self.table.heard(quiet)
        self.tick(ANNOUNCE)
        self.table.heard(recent)
        table = self.restarted(ANNOUNCE + 10)  # quiet: silent 2 × ANNOUNCE + 10 by now
        self.assertEqual(table.closest(quiet, for_reply=True), [recent])
        self.assertEqual(table.live_count(), 1)
        self.assertEqual(sorted(table.closest(quiet)), [quiet, recent])  # kept, for joins and lookups
        table.heard(quiet)
        self.assertEqual(sorted(table.closest(quiet, for_reply=True)), [quiet, recent])

    def test_expired_drops_and_bans_are_left_out_and_their_rows_deleted(self):
        dropped, banned = node_id(30), node_id(31)
        self.table.heard(dropped)
        while not self.table.failed(dropped):
            pass
        self.table.ban(banned)
        table = self.restarted(2 * ANNOUNCE + 1)
        self.assertEqual(sorted(self.changed), [dropped, banned])
        self.assertIsNone(table.row(dropped))
        self.assertFalse(table.is_banned(banned))
        self.assertTrue(table.heard(dropped))
        self.assertEqual(table.seniority(dropped), 0)  # new again

    def test_times_in_the_future_count_as_now(self):
        ahead = self.WALL + 10 * ANNOUNCE  # saved while the clock was ahead, since corrected
        rows = [(node_id(40), routing.IN_TABLE, ahead, ahead, 0), (node_id(41), routing.DROPPED, ahead, ahead, 0),
                (node_id(42), routing.BANNED, None, ahead, 0)]
        table = self.restarted(0, rows)
        self.assertEqual(sorted(self.changed), [node_id(40), node_id(41), node_id(42)])  # written again, clamped
        self.assertEqual(table.seniority(node_id(40)), 0)
        self.assertEqual(table.row(node_id(40)), (node_id(40), routing.IN_TABLE, self.WALL, self.WALL, 0))
        self.assertEqual(table.row(node_id(41))[2:4], (self.WALL, self.WALL))
        self.assertEqual(table.row(node_id(42))[3], self.WALL + 3600)  # at most ban_time from now
        self.clock.now += 3600
        self.assertFalse(table.is_banned(node_id(42)))

    def test_malformed_rows_and_our_own_id_are_left_out(self):
        good = (node_id(50), routing.IN_TABLE, self.WALL, self.WALL, 0)
        bad = [(b"short", routing.IN_TABLE, self.WALL, self.WALL, 0), (node_id(51), 7, self.WALL, self.WALL, 0),
               (node_id(52), routing.IN_TABLE, None, self.WALL, 0), (node_id(53), routing.IN_TABLE, "x", 1.0, 0),
               (node_id(54), routing.DROPPED, self.WALL, float("nan"), 0), (node_id(55), True, 1.0, 1.0, 0),
               (node_id(56), routing.IN_TABLE, 1.0, 1.0, -1), (node_id(57), routing.IN_TABLE, 1.0, 1.0, 1.5),
               (OWN_ID, routing.IN_TABLE, self.WALL, self.WALL, 0)]
        table = self.restarted(0, [good, *bad])
        self.assertEqual(table.nodes(), [node_id(50)])
        self.assertEqual(self.changed, [row[0] for row in bad])

    def test_admit_is_asked_again(self):
        """Private mode: a node no longer a member isn't loaded."""
        rows = [(node_id(n), routing.IN_TABLE, self.WALL, self.WALL, 0) for n in (60, 61)]
        table = self.restarted(0, rows, admit=lambda n: n == node_id(60))
        self.assertEqual(table.nodes(), [node_id(60)])
        self.assertEqual(self.changed, [node_id(61)])

    def test_a_full_bucket_keeps_the_nodes_first_seen_first(self):
        # Only after our own ID changed can saved rows overfill a bucket
        ids = bucket_ids(8, K + 2)
        rows = [(n, routing.IN_TABLE, self.WALL - i, self.WALL, 0) for i, n in enumerate(ids)]
        table = self.restarted(0, rows)
        self.assertEqual(sorted(table.nodes()), sorted(ids[2:]))
        self.assertEqual(sorted(self.changed), sorted(ids[:2]))

    def test_every_change_is_reported_with_the_lock_held(self):
        changes = []
        self.table.track(lambda n: changes.append((n, self.table._lock._is_owned())))
        a, b = node_id(70), node_id(71)
        self.table.heard(a)
        self.table.heard(a)
        self.table.failed(a)
        self.table.answered(a)
        self.table.ban(b)
        self.table.remove(a)
        self.tick(3601)
        self.assertFalse(self.table.is_banned(b))
        self.assertEqual(changes, [(a, True)] * 4 + [(b, True)] * 2 + [(a, True), (b, True)])  # ban() removes first
        for n in (72, 73):
            self.table.heard(node_id(n))
            while not self.table.failed(node_id(n)):
                pass
        changes.clear()
        self.tick(2 * ANNOUNCE + 1)
        self.table.heard(node_id(74))  # prunes the expired seniority of both
        self.assertEqual([n for n, _ in changes], [node_id(72), node_id(73), node_id(74)])


class RoutingStateTest(RoutingTestCase):
    """The table saved in the state database (routing_state.RoutingState): changed rows only, read back at start."""

    def setUp(self):
        """A state directory, and the table of RoutingTestCase saved in it."""
        super().setUp()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.state_dir = tmp.name
        self.wall = FakeClock(1_700_000_000.0)
        self.table = RoutingTable(OWN_ID, self.config, clock=self.clock, wall_clock=self.wall)
        self.state = self.open(self.table)

    def open(self, table: RoutingTable) -> RoutingState:
        """table's saved state, closed at the end of the test."""
        state = RoutingState(table, self.state_dir)
        self.addCleanup(state.close)
        return state

    def rows(self) -> list[tuple]:
        """The saved rows, by node."""
        return state_rows(self.state_dir, "SELECT node, state, first_seen, at, failures FROM routing ORDER BY node")

    def test_saves_changed_rows_and_loads_them_at_start(self):
        for n in (1, 2, 3):
            self.table.heard(node_id(n))
        self.table.ban(node_id(4))
        self.assertEqual(self.rows(), [])  # nothing written until the save
        self.state.save()
        self.assertEqual([row[:2] for row in self.rows()], [(node_id(n), routing.IN_TABLE) for n in (1, 2, 3)]
                         + [(node_id(4), routing.BANNED)])
        written = []
        real = statedb.RowStore.write_rows
        with mock.patch.object(statedb.RowStore, "write_rows",
                               lambda store, tx, snapshot: (written.append(snapshot), real(store, tx, snapshot))):
            self.state.save()  # nothing changed: nothing written
            self.clock.now += 60
            self.wall.now += 60
            self.table.heard(node_id(2))
            self.table.remove(node_id(3))
            self.state.save()
        self.assertEqual(len(written), 1)  # one save, of the two rows that changed
        self.assertEqual(dict(written[0]), {node_id(2): self.table.row(node_id(2)), node_id(3): None})
        self.state.close()
        table = RoutingTable(OWN_ID, self.config, clock=FakeClock(7.0), wall_clock=self.wall)
        self.open(table)
        self.assertEqual(sorted(table.nodes()), [node_id(1), node_id(2)])
        self.assertTrue(table.is_banned(node_id(4)))
        self.assertEqual(table.seniority(node_id(1)), 60)

    def test_rows_not_loaded_are_deleted_by_the_next_save(self):
        state_execute(self.state_dir, "INSERT INTO routing VALUES (?, ?, ?, ?, ?)", (b"short", 0, 1.0, 1.0, 0))
        state_execute(self.state_dir, "INSERT INTO routing VALUES (?, ?, ?, ?, ?)",
                      (node_id(9), routing.BANNED, None, 1.0, 0))  # ended long ago
        state_execute(self.state_dir, "INSERT INTO routing VALUES (?, ?, ?, ?, ?)",
                      (node_id(8), routing.IN_TABLE, 1.0, 2.0, 0))
        self.state.close()
        table = RoutingTable(OWN_ID, self.config, clock=self.clock, wall_clock=self.wall)
        state = self.open(table)
        self.assertEqual(table.nodes(), [node_id(8)])
        state.save()
        self.assertEqual(self.rows(), [(node_id(8), routing.IN_TABLE, 1.0, 2.0, 0)])


class ThreadSafetyTest(RoutingTestCase):
    """Concurrent use from announce, request and lookup threads."""

    def test_concurrent_heard_failed_closest(self):
        """Four threads hearing, failing and ranking nodes leave a consistent table: every node once, in its own
        bucket, no bucket over K, and never our own ID."""
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
        nodes = self.table.nodes()
        self.assertEqual(len(set(nodes)), self.table.node_count())  # each node once
        self.assertNotIn(OWN_ID, nodes)
        for n in nodes:
            self.assertIn(n, self.table._buckets[self.table.bucket_index(n)])
        self.assertTrue(all(len(bucket) <= K for bucket in self.table._buckets))
        self.assertGreater(len(nodes), 0)


if __name__ == "__main__":
    unittest.main()
