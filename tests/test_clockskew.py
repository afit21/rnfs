"""Tests for the clock offset estimate from announces' emission times (rnfs.clockskew; specification section 9,
"Announces and the clock")."""
import unittest

from rnfs import clockskew
from rnfs.clockskew import MIN_SENDERS, ClockSkew, duration_text, offset_text
from tests.support import FakeClock

WINDOW = 12 * 3600.0


def sender(i: int) -> bytes:
    """A sender's 16-byte hash."""
    return i.to_bytes(16, "big")


class ClockSkewTest(unittest.TestCase):
    """The median of distinct senders' least delayed announces in the window."""

    def setUp(self):
        """An estimator on fake wall and monotonic clocks, recording what it reports."""
        self.wall = FakeClock(2_000_000_000.0)
        self.mono = FakeClock(10_000.0)
        self.reports = []
        self.skew = ClockSkew(WINDOW, self.wall, self.mono, report=lambda text, warning: self.reports.append(warning))

    def nodes(self, offsets, start: int = 0):
        """A live rnfs.dht announce from one node per offset, each emitted offset seconds after this clock's now."""
        for i, offset in enumerate(offsets, start):
            self.skew.node_announced(sender(i), self.wall.now + offset)

    def test_no_estimate_before_enough_senders(self):
        """MIN_SENDERS distinct senders are needed; one sender heard many times is one."""
        self.nodes([400] * (MIN_SENDERS - 1))
        for _ in range(10):
            self.skew.node_announced(sender(0), self.wall.now + 400)
        self.assertEqual(self.skew.estimate(), (None, MIN_SENDERS - 1))
        self.assertEqual(self.skew.status(), {"clock_offset": None})
        self.nodes([400], MIN_SENDERS - 1)
        self.assertEqual(self.skew.estimate(), (400, MIN_SENDERS))

    def test_a_few_liars_dont_move_it(self):
        """Fewer than half of the senders, however far off, leave the median among the honest ones."""
        self.nodes([0, 2, -3, 1, -1, 5, 3])
        self.nodes([10 ** 6] * 3, start=7)  # three clocks far ahead, or lying
        offset, count = self.skew.estimate()
        self.assertEqual(count, 10)
        self.assertLessEqual(abs(offset), 5)
        self.assertEqual(self.reports, [])

    def test_bad_emission_times_are_ignored(self):
        """An unknown or impossible emission time is no sample."""
        for emitted in (None, float("nan"), float("inf"), 0, -5, True, "1"):
            self.skew.node_announced(sender(1), emitted)
        self.assertEqual(self.skew.estimate(), (None, 0))

    def test_behind_is_warned_once_and_cleared(self):
        """Past WARN_OFFSET it is reported once, not at each sample, and reported again once back within
        CLEAR_OFFSET; a step of the wall clock (an NTP sync) moves every sample with it."""
        self.nodes([600] * 6)
        self.assertEqual(self.skew.estimate()[0], 600)
        self.nodes([600] * 6)
        self.assertEqual(self.reports, [True])
        self.wall.now += 600  # the clock is set right
        self.assertEqual(self.skew.estimate()[0], 0)
        self.nodes([0])
        self.assertEqual(self.reports, [True, False])

    def test_ahead_is_never_reported(self):
        """Announces RNS delivered late (held after an interface came up, or queued past a transport node's cap) make
        the estimate say ahead; that can't be told from a clock ahead, so it is neither warned of nor shown: status
        reads 0 and behind() 0, while the estimate itself stays as measured."""
        self.nodes([-7200] * 8)  # every sender's only announce heard two hours after it was emitted
        self.assertEqual(self.skew.estimate()[0], -7200)
        self.assertEqual(self.skew.behind(), 0)
        self.assertEqual(self.skew.status(), {"clock_offset": 0})
        self.assertEqual(self.reports, [])
        self.nodes([600] * 9, start=8)  # a clock really behind is still told, once most senders say so
        self.assertEqual(self.skew.status(), {"clock_offset": 600})
        self.assertEqual(self.reports, [True])

    def test_late_announces_count_as_their_least_delayed(self):
        """A sender's least delayed announce in the window counts: an announce held in a queue (emitted long before
        it arrived) doesn't replace a timely one."""
        self.nodes([0] * 5)
        self.nodes([-7200] * 5)  # the same senders, their announces two hours late
        self.assertEqual(self.skew.estimate()[0], 0)

    def test_samples_expire_with_the_window(self):
        """Samples older than the window don't count; a sender heard again counts with its new sample."""
        self.nodes([100] * 5)
        self.wall.now += WINDOW + 1
        self.mono.now += WINDOW + 1
        self.assertEqual(self.skew.estimate(), (None, 0))
        self.nodes([-50] * 5)  # older than the expired ones' value, but those no longer count
        self.assertEqual(self.skew.estimate()[0], -50)

    def test_names_count_for_fewer_senders_than_nodes(self):
        """One node may announce many names with one clock: names make up MIN_SENDERS, else stay fewer than the
        nodes, so they can't outvote them."""
        for i in range(MIN_SENDERS):
            self.skew.name_announced(sender(100 + i), self.wall.now + 900)
        self.assertEqual(self.skew.estimate(), (900, MIN_SENDERS))
        for i in range(50):
            self.skew.name_announced(sender(200 + i), self.wall.now + 900)
        self.nodes([0] * 6)
        offset, count = self.skew.estimate()
        self.assertEqual((offset, count), (0, 11))

    def test_senders_are_bounded(self):
        """At most MAX_SENDERS of each kind are kept, the least recently heard forgotten first."""
        self.nodes([0] * (clockskew.MAX_SENDERS + 10))
        self.assertEqual(self.skew.estimate()[1], clockskew.MAX_SENDERS)
        self.assertNotIn(sender(0), self.skew._nodes)


class TextTest(unittest.TestCase):
    """The texts the warnings use."""

    def test_texts(self):
        """Durations round to the units that matter; an offset says which way."""
        self.assertEqual(duration_text(45.4), "45 s")
        self.assertEqual(duration_text(-720), "12 min")
        self.assertEqual(duration_text(3 * 3600 + 300), "3 h 5 min")
        self.assertEqual(duration_text(2 * 86400 + 4 * 3600 + 59), "2 d 4 h")
        self.assertEqual(offset_text(600), "10 min behind")
        self.assertEqual(offset_text(-7200), "2 h ahead")


if __name__ == "__main__":
    unittest.main()
