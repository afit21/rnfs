"""REL-15 (0.2.0 release audit): a flood of fresh identities set the clock-offset estimate.

The attack: the estimate is the median of the senders heard recently, at most MAX_SENDERS (128) of them, the least
recently heard forgotten first. 128 announces of fresh rnfs.dht identities, emitted 2 h ahead and sent within a few
seconds, pushed every honest sender out: the node then warned that its correct clock was 2 h behind and reported a
false clock_offset in its status, which a user could act on by setting the clock wrong.

The test hears 60 senior servers with correct clocks, then 128 fresh identities 2 h ahead: the estimate stays the
seniors'. The fix (rnfs/clockskew.py; rnfs/node.py, _announced): once MIN_SENDERS senior servers (first heard
provider_seniority ago, by the announce first-seen log) were heard within the window the estimate is theirs alone, and
a full table forgets the senders that aren't senior first. Without seniors it works as before.
"""
import unittest

from rnfs.clockskew import MIN_SENDERS, ClockSkew
from tests.support import FakeClock

WINDOW = 12 * 3600.0


def sender(i: int) -> bytes:
    """A sender's 16-byte hash."""
    return i.to_bytes(16, "big")


class ClockSkewFloodTest(unittest.TestCase):

    def setUp(self):
        self.wall = FakeClock(2_000_000_000.0)
        self.mono = FakeClock(10_000.0)
        self.reports = []
        self.skew = ClockSkew(WINDOW, self.wall, self.mono, report=lambda text, warning: self.reports.append(warning))

    def test_fresh_identities_dont_move_the_seniors_estimate(self):
        for i in range(60):
            self.skew.node_announced(sender(i), self.wall.now - 2, senior=True)
        for i in range(128):
            self.mono.now += 0.01
            self.skew.node_announced(sender(1000 + i), self.wall.now + 7200)
        offset, count = self.skew.estimate()
        self.assertLess(abs(offset), 5)
        self.assertEqual(count, 60)
        self.assertEqual(self.reports, [])  # no warning that the clock is behind
        self.assertEqual(self.skew.status(), {"clock_offset": 0})

    def test_without_seniors_the_median_of_all_as_before(self):
        for i in range(MIN_SENDERS):
            self.skew.node_announced(sender(i), self.wall.now + 400)
        self.assertEqual(self.skew.estimate(), (400, MIN_SENDERS))
        for i in range(MIN_SENDERS - 1):  # too few seniors yet to be the estimate alone
            self.skew.node_announced(sender(100 + i), self.wall.now, senior=True)
        self.assertEqual(self.skew.estimate()[1], 2 * MIN_SENDERS - 1)


if __name__ == "__main__":
    unittest.main()
