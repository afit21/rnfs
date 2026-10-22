"""Pacing by the radio's measured load (rnfs/network/channel.py, specification section 3, "Measured airtime"): the
figures read from fake interface stats, 0.0 as unknown, at most one reading every 15 s, the stretch of a busy channel
and its bound, the own-airtime budget, and how a slow node's path requests, publishing rounds and /blocks use them."""
import types
import unittest

from rnfs.network import channel, paths
from tests.fakes.log import quiet_log
from tests.support import FakeClock
from tests.test_paths import FakeTransport

setUpModule, tearDownModule = quiet_log()


def rnode(**figures) -> dict:
    """An online RNode's entry in Reticulum.get_interface_stats(), with figures (percentages)."""
    return {"type": "RNodeInterface", "status": True, **figures}


def stats(*interfaces) -> dict:
    """Reticulum.get_interface_stats() of interfaces given as dicts."""
    return {"interfaces": list(interfaces)}


class MeasuredLoadTest(unittest.TestCase):
    """measured_load over get_interface_stats()."""

    def test_the_busiest_radio_of_each_figure(self):
        load = channel.measured_load(stats(
            rnode(airtime_short=3.5, airtime_long=12.0, channel_load_short=20.0, channel_load_long=31.0),
            rnode(airtime_short=9.0, airtime_long=2.0, channel_load_short=45.5, channel_load_long=0.0),
            {"type": "TCPClientInterface", "status": True, "bitrate": 10_000_000}))
        self.assertEqual(load, channel.Load(9.0, 12.0, 45.5, 31.0))
        self.assertEqual(load.channel, 45.5)  # the short figure: the load now

    def test_zero_and_nonsense_are_unknown(self):
        # An RNode reports 0.0 before its first measurement
        self.assertEqual(channel.measured_load(stats(rnode(airtime_short=0.0, airtime_long=0.0,
                                                           channel_load_short=0.0, channel_load_long=0.0))),
                         channel.UNKNOWN)
        for value in (-1.0, 100.5, float("nan"), float("inf"), True, "50", None):
            with self.subTest(value=value):
                self.assertEqual(channel.measured_load(stats(rnode(channel_load_short=value))), channel.UNKNOWN)
        self.assertEqual(channel.measured_load(stats(rnode(channel_load_short=100))).channel, 100.0)

    def test_long_figure_while_the_short_is_unknown(self):
        self.assertEqual(channel.measured_load(stats(rnode(channel_load_short=0.0, channel_load_long=40.0))).channel,
                         40.0)

    def test_offline_and_local_interfaces_are_left_out(self):
        for entry in ({**rnode(channel_load_short=80.0), "status": False},
                      {"type": "LocalClientInterface", "status": True, "channel_load_short": 80.0},
                      {"type": "LocalServerInterface", "status": True, "channel_load_short": 80.0}):
            with self.subTest(entry=entry):
                self.assertEqual(channel.measured_load(stats(entry)), channel.UNKNOWN)
        for broken in (None, {}, {"interfaces": None}, {"interfaces": ["x", 3]}):
            self.assertEqual(channel.measured_load(broken), channel.UNKNOWN)

    def test_stretch(self):
        self.assertEqual([channel.stretch_for(load) for load in (None, 10.0, 25.0, 50.0, 75.0, 100.0)],
                         [1.0, 1.0, 1.0, 2.0, 3.0, 4.0])


class ChannelLoadTest(unittest.TestCase):
    """ChannelLoad over a fake Reticulum, in virtual time."""

    def setUp(self):
        """A reading of one RNode, its figures in self.figures."""
        self.clock = FakeClock()
        self.figures = {}
        self.calls = 0

        def get_interface_stats():
            """The RNode's figures now (as through a shared instance's RPC too)."""
            self.calls += 1
            return stats(rnode(**self.figures))
        self.reticulum = types.SimpleNamespace(get_interface_stats=get_interface_stats)
        self.load = channel.ChannelLoad(lambda: self.reticulum, clock=self.clock)

    def test_read_at_most_every_15_s(self):
        self.figures = {"channel_load_short": 50.0}
        self.assertEqual(self.load.stretch(), 2.0)
        self.figures = {"channel_load_short": 10.0}
        self.clock.now += 14.9
        self.assertEqual(self.load.stretch(), 2.0)  # the last reading
        self.assertEqual(self.calls, 1)
        self.clock.now += 0.1
        self.assertEqual(self.load.stretch(), 1.0)
        self.assertEqual((self.calls, self.load.reads), (2, 2))

    def test_nothing_asked_nothing_read(self):
        # Polled only while paced work asks: a node with nothing queued reads nothing
        self.clock.now += 3600
        self.assertEqual(self.calls, 0)

    def test_stretch_wait_is_bounded(self):
        self.figures = {"channel_load_short": 100.0}
        self.assertEqual(self.load.stretch_wait(300.0, 10**6), 1200.0)
        self.assertEqual(self.load.stretch_wait(300.0, 500.0), 500.0)  # slack: never past PROVIDER_TTL / 2
        self.assertEqual(self.load.stretch_wait(300.0, 100.0), 300.0)  # never shorter than unstretched
        self.figures = {"channel_load_short": 20.0}
        self.clock.now += 15
        self.assertEqual(self.load.stretch_wait(300.0, 10**6), 300.0)

    def test_own_airtime_budget(self):
        self.figures = {"airtime_long": 50.0}
        self.assertFalse(self.load.over_budget())
        self.assertEqual(self.load.airtime_share(), 0.5)
        self.figures = {"airtime_long": 50.1, "channel_load_short": 10.0}
        self.clock.now += 15
        self.assertTrue(self.load.over_budget())
        self.figures = {"airtime_short": 90.0, "airtime_long": 0.0}  # the hour's figure unknown: not over
        self.clock.now += 15
        self.assertFalse(self.load.over_budget())
        self.assertIsNone(self.load.airtime_share())

    def test_unreadable_stats_are_unknown(self):
        self.reticulum.get_interface_stats = lambda: 1 / 0
        self.assertEqual(self.load.load(), channel.UNKNOWN)
        self.assertEqual((self.load.stretch(), self.load.over_budget(), self.load.airtime_share()), (1.0, False, None))
        self.assertEqual(channel.ChannelLoad(lambda: None, clock=self.clock).load(), channel.UNKNOWN)

    def test_a_shared_instance_restarting_is_logged_without_a_warning(self):
        """Its rnsd restarting refuses the RPC for a while: expected, so not a warning (with a traceback); anything
        else is."""
        logged = []
        self.load._failure_log = lambda message, level: logged.append(level)

        def refused():
            raise ConnectionRefusedError(111, "Connection refused")
        for failure, level in ((refused, channel.LOG_VERBOSE), (lambda: 1 / 0, channel.LOG_WARNING)):
            self.reticulum.get_interface_stats = failure
            self.clock.now += channel.POLL
            self.assertEqual(self.load.load(), channel.UNKNOWN)
            self.assertEqual(logged.pop(), level)


class PathSpacingTest(unittest.TestCase):
    """A slow node's path requests are spaced further apart while its channel is busy."""

    def test_busy_channel_stretches_the_spacing(self):
        clock = FakeClock()
        transport = FakeTransport(clock)
        figures = {"channel_load_short": 75.0}
        reticulum = types.SimpleNamespace(get_interface_stats=lambda: stats(rnode(**figures)),
                                          is_connected_to_shared_instance=False)

        def sleep(seconds):
            """Advance virtual time."""
            clock.now += seconds
        budget = paths.PathRequests(transport=lambda: transport, reticulum=lambda: reticulum, clock=clock,
                                    sleep=sleep)
        budget.stretch = channel.ChannelLoad(lambda: reticulum, clock=clock).stretch
        for n in range(1, 4):
            budget.await_path(bytes([n]) * 16, 30)
        budget.charge(bytes([9]) * 16)  # a failed link's turn is stretched too
        figures["channel_load_short"] = 5.0
        clock.now += 15  # the next reading finds the channel quiet
        for n in range(4, 6):
            budget.await_path(bytes([n]) * 16, 30)
        start = transport.requests[0][0]
        self.assertEqual([round(when - start, 6) for when, _ in transport.requests], [0, 3, 6, 21, 22])

    def test_a_broken_stretch_spaces_as_ever(self):
        clock = FakeClock()
        transport = FakeTransport(clock)
        budget = paths.PathRequests(transport=lambda: transport, reticulum=lambda: None, clock=clock,
                                    sleep=lambda seconds: setattr(clock, "now", clock.now + seconds))
        for broken in (lambda: 1 / 0, lambda: float("nan"), lambda: 0.2):
            budget.stretch = broken
            budget.await_path(bytes([len(transport.requests) + 1]) * 16, 30)
        start = transport.requests[0][0]
        self.assertEqual([round(when - start, 6) for when, _ in transport.requests], [0, 1, 2])


if __name__ == "__main__":
    unittest.main()
