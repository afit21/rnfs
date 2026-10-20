"""Tests for local scores (rnfs/dht/scores.py): what raises and lowers them, decay, bands, the cap and its eviction
order, bootstrap seeds, and the saved table."""
import os
import tempfile
import unittest

from rnfs.constants import DAY
from rnfs.dht.scores import BAD_BLOCK, BOOTSTRAP_SEED, FAILED, SCORE_HALF_LIFE, SCORE_LIMIT, SERVED, USEFUL_ANSWER, \
    Scores, band
from tests.support import FakeClock, state_execute, state_rows

WALL = 1_700_000_000.0


def node_id(n: int) -> bytes:
    """The node ID whose big-endian value is n."""
    return n.to_bytes(16, "big")


class ScoreTest(unittest.TestCase):
    """Scores in memory."""

    def setUp(self):
        """Empty scores on a fake wall clock."""
        self.clock = FakeClock(WALL)
        self.scores = Scores(clock=self.clock)

    def test_a_peer_without_a_row_scores_zero(self):
        self.assertEqual(self.scores.score(node_id(1)), 0)
        self.assertEqual(self.scores.band(node_id(1)), 0)

    def test_useful_work_raises_and_failures_lower(self):
        a = node_id(1)
        self.scores.served(a)
        self.scores.served(a)
        self.scores.useful_answer(a)
        self.assertEqual(self.scores.score(a), 2 * SERVED + USEFUL_ANSWER)
        self.scores.failed(a)
        self.assertEqual(self.scores.score(a), 2 * SERVED + USEFUL_ANSWER + FAILED)
        self.scores.bad_block(a)
        self.assertEqual(self.scores.score(a), 2 * SERVED + USEFUL_ANSWER + FAILED + BAD_BLOCK)

    def test_scores_stay_within_the_limit(self):
        a, b = node_id(1), node_id(2)
        for _ in range(int(SCORE_LIMIT) + 10):
            self.scores.served(a)
            self.scores.bad_block(b)
        self.assertEqual(self.scores.score(a), SCORE_LIMIT)
        self.assertEqual(self.scores.score(b), -SCORE_LIMIT)

    def test_silence_halves_a_score_each_half_life_towards_zero(self):
        a, b = node_id(1), node_id(2)
        for _ in range(8):
            self.scores.served(a)
        self.scores.bad_block(b)
        self.clock.now += SCORE_HALF_LIFE
        self.assertAlmostEqual(self.scores.score(a), 4)
        self.assertAlmostEqual(self.scores.score(b), BAD_BLOCK / 2)
        self.clock.now += 2 * SCORE_HALF_LIFE
        self.assertAlmostEqual(self.scores.score(a), 1)
        self.scores.served(a)  # decayed first, then raised
        self.assertAlmostEqual(self.scores.score(a), 2)
        self.clock.now -= 10 * DAY  # a clock stepped back: no decay, never growth
        self.assertAlmostEqual(self.scores.score(a), 2)

    def test_bands_are_a_power_of_two_wide(self):
        self.assertEqual([band(s) for s in (0, 0.9, -0.9, 1, 1.9, 2, 3.9, 4, 64, -1, -2, -32)],
                         [0, 0, 0, 1, 1, 2, 2, 3, 7, -1, -2, -6])
        a = node_id(1)
        for _ in range(5):
            self.scores.served(a)
        self.assertEqual(self.scores.bands(), {a: 3})

    def test_bad_ids_are_ignored(self):
        self.scores.served(b"short")
        self.assertEqual(self.scores.bands(), {})


class CapTest(unittest.TestCase):
    """The capped table forgets the least recently useful peer first."""

    def setUp(self):
        """Scores holding at most 3 peers."""
        self.clock = FakeClock(WALL)
        self.scores = Scores(clock=self.clock, max_rows=3)

    def test_least_recently_useful_is_forgotten_first(self):
        a, b, c, d = (node_id(n) for n in (1, 2, 3, 4))
        for n in (a, b, c):
            self.clock.now += 1
            self.scores.served(n)
        self.scores.failed(a)  # lowering isn't useful: a stays least recently useful
        self.scores.served(d)
        self.assertEqual(set(self.scores.bands()), {b, c, d})
        self.scores.served(b)
        self.scores.served(a)  # c is now the least recently useful
        self.assertEqual(set(self.scores.bands()), {a, b, d})

    def test_rows_only_lowered_go_first(self):
        a, b, c, bad = (node_id(n) for n in (1, 2, 3, 9))
        self.scores.served(a)
        self.scores.served(b)
        self.scores.failed(bad)
        self.assertEqual(set(self.scores.bands()), {a, b, bad})
        self.scores.served(c)
        self.assertEqual(set(self.scores.bands()), {a, b, c})
        self.scores.failed(bad)  # the table is full of useful peers: a new lowered row is forgotten at once
        self.assertEqual(set(self.scores.bands()), {a, b, c})
        self.assertEqual(self.scores.score(bad), 0)


class SeedTest(unittest.TestCase):
    """Bootstrap nodes start with a seed, which they keep only by earning it."""

    def test_bootstrap_nodes_start_seeded_and_keep_it_only_by_earning_it(self):
        clock = FakeClock(WALL)
        boot, other = node_id(1), node_id(2)
        s = Scores(seeded=[boot], clock=clock)
        self.assertEqual(s.score(boot), BOOTSTRAP_SEED)
        self.assertEqual(s.score(other), 0)
        s.failed(boot)
        self.assertEqual(s.score(boot), BOOTSTRAP_SEED + FAILED)
        clock.now += 3 * SCORE_HALF_LIFE
        self.assertAlmostEqual(s.score(boot), (BOOTSTRAP_SEED + FAILED) / 8)
        s.bad_block(boot)
        self.assertLess(s.score(boot), 0)

    def test_seeded_once_per_row(self):
        with tempfile.TemporaryDirectory() as state_dir:
            clock = FakeClock(WALL)
            boot = node_id(1)
            s = Scores(state_dir, seeded=[boot], clock=clock)
            s.failed(boot)
            s.save()
            s.close()
            clock.now += SCORE_HALF_LIFE
            s = Scores(state_dir, seeded=[boot], clock=clock)  # a restart: not seeded again
            self.assertAlmostEqual(s.score(boot), (BOOTSTRAP_SEED + FAILED) / 2)
            s.close()

    def test_no_longer_a_bootstrap_node_loses_what_is_left_of_its_seed(self):
        with tempfile.TemporaryDirectory() as state_dir:
            clock = FakeClock(WALL)
            boot = node_id(1)
            s = Scores(state_dir, seeded=[boot], clock=clock)
            for _ in range(4):
                s.served(boot)
            s.save()
            s.close()
            clock.now += SCORE_HALF_LIFE
            s = Scores(state_dir, seeded=[], clock=clock)  # default_bootstrap = false, say
            self.assertAlmostEqual(s.score(boot), 4 * SERVED / 2)  # what it earned, decayed alike
            s.save()
            s.close()
            self.assertEqual(state_rows(state_dir, "SELECT seeded FROM peer_scores"), [(None,)])

    def test_losing_the_seed_keeps_the_score_within_the_limit(self):
        # A bootstrap node whose bad blocks took it to -SCORE_LIMIT (the seed's share lost in the clamp) mustn't go
        # below it when it stops being one: found by tests/security/test_fuzz_parsers.py (SavedRowsFuzzTest)
        with tempfile.TemporaryDirectory() as state_dir:
            clock = FakeClock(WALL)
            boot = node_id(1)
            s = Scores(state_dir, seeded=[boot], clock=clock)
            for _ in range(3):
                s.bad_block(boot)
            self.assertEqual(s.score(boot), -SCORE_LIMIT)
            s.save()
            s.close()
            s = Scores(state_dir, seeded=[], clock=clock)
            self.assertEqual(s.score(boot), -SCORE_LIMIT)
            s.close()


class SavedTest(unittest.TestCase):
    """Scores in the state database."""

    def setUp(self):
        """A state directory and a fake wall clock."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.state_dir = os.path.join(tmp.name, "state")
        self.clock = FakeClock(WALL)

    def open(self, **kwargs) -> Scores:
        """Scores saved in the state directory, closed at the end of the test."""
        s = Scores(self.state_dir, clock=self.clock, **kwargs)
        self.addCleanup(s.close)
        return s

    def rows(self) -> list[tuple]:
        """The saved rows, by node."""
        return state_rows(self.state_dir, "SELECT node, score, updated, useful, seeded FROM peer_scores ORDER BY node")

    def test_round_trip_writes_only_changed_rows(self):
        s = self.open()
        a, b = node_id(1), node_id(2)
        s.served(a)
        s.failed(b)
        self.assertEqual(self.rows(), [])
        s.save()
        self.assertEqual(self.rows(), [(a, SERVED, WALL, WALL, None), (b, FAILED, WALL, None, None)])
        self.assertEqual(s._store.pending, frozenset())
        self.clock.now += 10
        s.served(a)
        self.assertEqual(s._store.pending, {a})
        s.save()
        s.close()
        self.clock.now += SCORE_HALF_LIFE
        s = self.open()
        self.assertAlmostEqual(s.score(a), (decayed := SERVED * 0.5 ** (10 / SCORE_HALF_LIFE) + SERVED) / 2)
        self.assertAlmostEqual(s.score(b), FAILED * 0.5 ** ((SCORE_HALF_LIFE + 10) / SCORE_HALF_LIFE))
        self.assertGreater(decayed, 1)

    def test_eviction_order_is_kept_across_a_restart(self):
        s = self.open(max_rows=10)
        a, b, lowered = node_id(1), node_id(2), node_id(3)
        s.served(b)
        self.clock.now += 1
        s.served(a)
        s.failed(lowered)
        s.save()
        s.close()
        s = self.open(max_rows=2)  # a smaller cap: the least recently useful go at load, and their rows next save
        self.assertEqual(set(s.bands()), {a, b})
        s.served(node_id(4))
        self.assertEqual(set(s.bands()), {a, node_id(4)})
        s.save()
        self.assertEqual([row[0] for row in self.rows()], [a, node_id(4)])

    def test_times_in_the_future_count_as_now_and_malformed_rows_go(self):
        self.open().close()
        ahead = WALL + 30 * DAY
        node = node_id(1)
        insert = "INSERT INTO peer_scores VALUES (?, ?, ?, ?, ?)"
        state_execute(self.state_dir, insert, (node, 4.0, ahead, ahead, None))
        state_execute(self.state_dir, insert, (b"short", 1.0, WALL, WALL, None))
        state_execute(self.state_dir, insert, (node_id(2), "x", WALL, WALL, None))
        state_execute(self.state_dir, insert, (node_id(3), 1000.0, WALL, WALL, None))
        s = self.open()
        self.assertEqual(s.score(node_id(3)), SCORE_LIMIT)
        s.save()
        self.clock.now += SCORE_HALF_LIFE
        self.assertAlmostEqual(s.score(node), 2.0)  # decaying from now, not from the future
        self.assertEqual(self.rows(), [(node, 4.0, WALL, WALL, None), (node_id(3), SCORE_LIMIT, WALL, WALL, None)])


if __name__ == "__main__":
    unittest.main()
