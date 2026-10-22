"""Tests for the announce first-seen log (rnfs/dht/firstseen.py): first-heard times, the closest-old-servers query,
the cap and its eviction order, and the saved table."""
import os
import random
import tempfile
import unittest

from rnfs.constants import K
from rnfs.dht.firstseen import FirstSeenLog
from rnfs.dht.routing import distance
from tests.support import FakeClock, state_execute, state_rows

WALL = 1_700_000_000.0
DAY = 86400.0


def node_id(n: int) -> bytes:
    """The node ID whose big-endian value is n."""
    return n.to_bytes(16, "big")


class LogTest(unittest.TestCase):
    """The log in memory."""

    def setUp(self):
        """An empty log on a fake wall clock."""
        self.clock = FakeClock(WALL)
        self.log = FirstSeenLog(clock=self.clock)

    def test_first_heard_is_kept_when_heard_again(self):
        a = node_id(1)
        self.assertIsNone(self.log.first_heard(a))
        self.log.heard(a)
        self.clock.now += 100
        self.log.heard(a)
        self.assertEqual(self.log.first_heard(a), WALL)
        self.assertEqual(len(self.log), 1)
        self.log.heard(b"short")  # ignored
        self.assertEqual(len(self.log), 1)

    def test_client_mode_forgets(self):
        a = node_id(1)
        self.log.heard(a)
        self.log.forget(a)
        self.log.forget(node_id(2))  # not in the log: nothing to do
        self.assertIsNone(self.log.first_heard(a))
        self.assertEqual(self.log.closest(a, min_age=0), [])
        self.clock.now += 100
        self.log.heard(a)
        self.assertEqual(self.log.first_heard(a), WALL + 100)  # new again

    def test_closest_leaves_out_servers_heard_too_recently(self):
        key = node_id(0)
        old = [node_id(n) for n in (5, 9, 1000)]
        for n in old:
            self.log.heard(n)
        self.clock.now += DAY
        new = [node_id(n) for n in (1, 2, 3)]  # closer to the key, but first heard a moment ago
        for n in new:
            self.log.heard(n)
        self.assertEqual(self.log.closest(key, count=3, min_age=DAY), old)
        self.assertEqual(self.log.closest(key, count=3, min_age=0), new)
        self.assertEqual(self.log.closest(key, count=2, min_age=DAY, exclude=lambda n: n == old[0]), old[1:])
        self.assertEqual(self.log.closest(key, count=0), [])

    def test_closest_leaves_out_servers_gone_silent(self):
        """heard_within leaves out servers not heard for that long; hearing one again brings it back."""
        key = node_id(0)
        silent, live = [node_id(n) for n in (1, 2)], [node_id(n) for n in (5, 9, 1000)]
        for n in silent + live:
            self.log.heard(n)
        self.clock.now += 2 * DAY
        for n in live:
            self.log.heard(n)
        self.clock.now += DAY
        self.assertEqual(self.log.closest(key, count=3, min_age=DAY), silent + live[:1])
        self.assertEqual(self.log.closest(key, count=3, min_age=DAY, heard_within=2 * DAY), live)
        self.log.heard(silent[1])
        self.assertEqual(self.log.closest(key, count=3, min_age=DAY, heard_within=2 * DAY), [silent[1]] + live[:2])

    def test_closest_matches_ranking_every_server(self):
        rng = random.Random(4)
        for size in (0, 1, 5, 40, 3000):
            log = FirstSeenLog(clock=self.clock)
            ids = [rng.randbytes(16) for _ in range(size)]
            # Clustered IDs too, so some prefix runs are dense and others empty
            base = rng.getrandbits(128)
            ids += [(base ^ rng.getrandbits(rng.randrange(1, 40))).to_bytes(16, "big") for _ in range(size // 4)]
            ages = {}
            for n in ids:
                self.clock.now = WALL + rng.randrange(0, 10) * DAY
                log.heard(n)
                ages.setdefault(n, self.clock.now)
            self.clock.now = WALL + 10 * DAY
            for _ in range(20):
                key = rng.choice([rng.randbytes(16), base.to_bytes(16, "big")])
                min_age = rng.choice([0, 3 * DAY, 9 * DAY, 11 * DAY])
                count = rng.choice([1, K, 50])
                eligible = [n for n in ages if ages[n] <= self.clock.now - min_age]
                expected = sorted(eligible, key=lambda n: distance(n, key))[:count]
                with self.subTest(size=size, min_age=min_age, count=count):
                    self.assertEqual(log.closest(key, count=count, min_age=min_age), expected)

    def test_the_newest_not_yet_senior_is_forgotten_first(self):
        """Full, and none silent: the server first heard most recently goes, if not yet senior (a newcomer itself),
        never a senior one."""
        log = FirstSeenLog(clock=self.clock, max_rows=3, seniority=DAY, silence=12 * 3600)
        a, b, c, d, e = (node_id(n) for n in (1, 2, 3, 4, 5))
        log.heard(a)
        self.clock.now += 1
        for n in (b, c):
            log.heard(n)
        log.heard(a)  # heard again: b is now the least recently heard, but that doesn't matter while it isn't silent
        log.heard(d)
        self.assertIsNone(log.first_heard(d))  # the newcomer
        self.assertEqual(sorted(log.closest(node_id(0), count=10)), [a, b, c])
        self.clock.now += DAY - 1  # a is senior, b and c not
        for n in (a, b, c):
            log.heard(n)
        self.clock.now += 1  # b and c senior too: only newcomers go
        log.heard(e)
        self.assertIsNone(log.first_heard(e))
        self.assertEqual(log.first_heard(a), WALL)  # kept its first-heard time

    def test_a_silent_server_is_forgotten_first(self):
        log = FirstSeenLog(clock=self.clock, max_rows=2, seniority=DAY, silence=12 * 3600)
        a, b, c = (node_id(n) for n in (1, 2, 3))
        log.heard(a)
        log.heard(b)
        self.clock.now += 2 * DAY  # both senior
        log.heard(a)
        log.heard(b)
        self.clock.now += 6 * 3600
        log.heard(b)
        self.clock.now += 6 * 3600
        log.heard(c)  # a was silent for exactly 12 h: not yet forgotten
        self.assertIsNone(log.first_heard(c))
        self.clock.now += 1
        log.heard(c)
        self.assertIsNone(log.first_heard(a))  # silent too long, senior or not
        self.assertEqual(sorted(log.closest(node_id(0), count=10)), [b, c])

    def test_with_no_seniority_the_least_recently_heard_goes(self):
        log = FirstSeenLog(clock=self.clock, max_rows=2, seniority=0)
        a, b, c = (node_id(n) for n in (1, 2, 3))
        for n in (a, b, a, c):
            log.heard(n)
        self.assertEqual(sorted(log.closest(node_id(0), count=10)), [a, c])


class SavedTest(unittest.TestCase):
    """The log in the state database."""

    def setUp(self):
        """A state directory and a fake wall clock."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.state_dir = os.path.join(tmp.name, "state")
        self.clock = FakeClock(WALL)

    def open(self, **kwargs) -> FirstSeenLog:
        """The log saved in the state directory, closed at the end of the test."""
        log = FirstSeenLog(self.state_dir, clock=self.clock, **kwargs)
        self.addCleanup(log.close)
        return log

    def rows(self) -> list[tuple]:
        """The saved rows, by node."""
        return state_rows(self.state_dir, "SELECT node, first_heard, last_heard FROM announce_first_seen ORDER BY node")

    def test_announces_are_written_together_and_only_when_changed(self):
        log = self.open()
        a, b = node_id(1), node_id(2)
        log.heard(a)
        log.heard(b)
        log.heard(a)
        self.assertEqual(self.rows(), [])  # nothing written per announce
        self.clock.now += 30
        log.save()
        self.assertEqual(self.rows(), [(a, WALL, WALL + 30), (b, WALL, WALL + 30)])
        self.assertEqual(log._store.pending, frozenset())
        log.save()  # nothing changed: nothing written
        self.clock.now += 30
        log.heard(b)
        self.assertEqual(log._store.pending, {b})
        log.save()
        self.assertEqual(self.rows(), [(a, WALL, WALL + 30), (b, WALL, WALL + 60)])
        log.forget(a)
        log.save()
        self.assertEqual(self.rows(), [(b, WALL, WALL + 60)])

    def test_restart_keeps_first_heard_times_and_the_eviction_order(self):
        log = self.open()
        a, b, c = node_id(1), node_id(2), node_id(3)
        log.heard(b)
        log.heard(a)
        log.save()
        self.clock.now += 60
        log.heard(b)
        log.save()
        log.close()
        self.clock.now += 5 * DAY
        log = self.open(max_rows=2)
        self.assertEqual(log.first_heard(a), WALL)
        self.assertEqual(log.closest(node_id(0), count=5, min_age=5 * DAY), [a, b])
        # Last heard as of the save that wrote each row: b's, 60 s after a's
        self.assertEqual(log.closest(node_id(0), count=5, heard_within=5 * DAY), [b])
        log.heard(c)  # a and b are senior, and silence counts from the restart: the newcomer goes
        self.assertIsNone(log.first_heard(c))
        self.clock.now += 12 * 3600 + 1
        log.heard(b)
        log.heard(c)  # a, heard least recently, has now been silent too long since the restart
        self.assertIsNone(log.first_heard(a))
        log.save()
        self.assertEqual([row[0] for row in self.rows()], [b, c])

    def test_times_in_the_future_count_as_now_and_malformed_rows_go(self):
        self.open().close()
        ahead = WALL + 30 * DAY
        insert = "INSERT INTO announce_first_seen VALUES (?, ?, ?)"
        state_execute(self.state_dir, insert, (node_id(1), ahead, ahead))
        state_execute(self.state_dir, insert, (b"short", WALL, WALL))
        state_execute(self.state_dir, insert, (node_id(2), "x", WALL))
        log = self.open()
        self.assertEqual(log.first_heard(node_id(1)), WALL)
        self.assertEqual(len(log), 1)
        log.save()
        self.assertEqual(self.rows(), [(node_id(1), WALL, WALL)])

    def test_a_smaller_cap_forgets_at_load(self):
        log = self.open()
        for n in range(1, 6):
            self.clock.now += 1
            log.heard(node_id(n))
            log.save()
        log.close()
        log = self.open(max_rows=2)
        self.assertEqual(len(log), 2)
        log.save()
        # None is senior or silent: those first heard most recently go
        self.assertEqual([row[0] for row in self.rows()], [node_id(1), node_id(2)])


if __name__ == "__main__":
    unittest.main()
