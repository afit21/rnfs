"""Tests for the counts of recent events the node's status reports (rnfs/dht/recent.py)."""
import unittest

from rnfs.constants import DAY, HOUR
from rnfs.dht.recent import RecentCounts
from tests.support import FakeClock


class RecentCountsTest(unittest.TestCase):
    """Events counted per minute over the last day."""

    def setUp(self):
        """A counter on a fake clock."""
        self.clock = FakeClock(10 * DAY)
        self.counts = RecentCounts(self.clock)

    def test_windows_and_ageing_out(self):
        self.assertEqual(self.counts.hour_and_day(), (0, 0))
        self.counts.add()
        self.counts.add(2)
        self.counts.add(0)  # nothing
        self.assertEqual(self.counts.hour_and_day(), (3, 3))
        self.clock.now += HOUR - 60  # still within the hour, to the minute
        self.counts.add()
        self.assertEqual(self.counts.hour_and_day(), (4, 4))
        self.clock.now += 60  # the first three are over an hour old now
        self.assertEqual(self.counts.hour_and_day(), (1, 4))
        self.clock.now += DAY - HOUR  # the first three are over a day old
        self.assertEqual(self.counts.hour_and_day(), (0, 1))
        self.clock.now += HOUR
        self.assertEqual(self.counts.hour_and_day(), (0, 0))
        self.assertEqual(len(self.counts._slots), 0)  # dropped, not only left out

    def test_memory_is_bounded_by_the_horizon(self):
        """However many events, at most a slot a minute over the day is kept."""
        for minute in range(3 * 24 * 60):
            self.clock.now += 60
            self.counts.add(5)
        self.assertLessEqual(len(self.counts._slots), 24 * 60)
        self.assertEqual(self.counts.count(DAY), 5 * 24 * 60)
        self.assertEqual(self.counts.count(HOUR), 5 * 60)

    def test_a_clock_that_goes_back_counts_with_the_latest_slot(self):
        self.counts.add()
        self.clock.now -= 600
        self.counts.add()
        self.clock.now += 600
        self.assertEqual(self.counts.hour_and_day(), (2, 2))
        self.assertEqual(len(self.counts._slots), 1)


if __name__ == "__main__":
    unittest.main()
