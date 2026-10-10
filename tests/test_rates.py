"""Tests for rate estimates, slow links, slow nodes, deadlines and requester max_bytes (section 2)."""
import types
import unittest

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE, KIB
from rnfs.network import rates

LINK = b"L" * 16


def interface(type_name="TCPClientInterface", status=True, bitrate=10_000_000):
    """A Reticulum.get_interface_stats() interface entry with the keys rates reads."""
    return {"type": type_name, "status": status, "bitrate": bitrate, "name": f"{type_name}[x]"}


class RateEstimatesTest(unittest.TestCase):
    """Estimates come only from responses of at least min_rate_sample."""

    def setUp(self):
        """Estimates with the default 16 KiB minimum sample."""
        self.estimates = rates.RateEstimates(16 * KIB)

    def test_small_responses_are_ignored(self):
        self.assertFalse(self.estimates.record(LINK, 16 * KIB - 1, 5000))
        self.assertIsNone(self.estimates.get(LINK))

    def test_large_response_is_recorded(self):
        self.assertTrue(self.estimates.record(LINK, 16 * KIB, 5000))
        self.assertEqual(self.estimates.get(LINK), 5000)
        self.estimates.record(LINK, 64 * KIB, 7000)
        self.assertEqual(self.estimates.get(LINK), 7000)

    def test_missing_or_zero_rate_is_ignored(self):
        self.assertFalse(self.estimates.record(LINK, 64 * KIB, None))
        self.assertFalse(self.estimates.record(LINK, 64 * KIB, 0))
        self.assertFalse(self.estimates.record(LINK, None, 1000))

    def test_forget(self):
        self.estimates.record(LINK, 64 * KIB, 7000)
        self.estimates.forget(LINK)
        self.assertIsNone(self.estimates.get(LINK))


class SlowLinkTest(unittest.TestCase):
    """Slow links: estimate below slow_bitrate, or no estimate and MTU of 500 or less."""

    def test_by_estimate(self):
        self.assertTrue(rates.is_slow_link(99_999, 1500, 100_000))
        self.assertFalse(rates.is_slow_link(100_000, 500, 100_000))

    def test_by_mtu_without_estimate(self):
        self.assertTrue(rates.is_slow_link(None, 500, 100_000))
        self.assertFalse(rates.is_slow_link(None, 501, 100_000))
        self.assertTrue(rates.is_slow_link(None, None, 100_000))

    def test_link_is_slow_reads_mtu(self):
        link = types.SimpleNamespace(get_mtu=lambda: 262144)
        self.assertFalse(rates.link_is_slow(link, None, Config()))
        self.assertTrue(rates.link_is_slow(link, 1000, Config()))


class SlowNodeTest(unittest.TestCase):
    """Slow nodes from interface stats, ignoring local, offline and bitrate-less interfaces."""

    def test_all_slow(self):
        stats = {"interfaces": [interface("RNodeInterface", bitrate=1200), interface("KISSInterface", bitrate=9600)]}
        self.assertTrue(rates.slow_from_stats(stats, 100_000))

    def test_one_fast_interface(self):
        stats = {"interfaces": [interface("RNodeInterface", bitrate=1200), interface()]}
        self.assertFalse(rates.slow_from_stats(stats, 100_000))

    def test_ignored_interfaces(self):
        stats = {"interfaces": [
            interface("RNodeInterface", bitrate=1200),
            interface("LocalServerInterface", bitrate=1_000_000_000),
            interface("LocalClientInterface", bitrate=1_000_000_000),
            interface("TCPServerInterface", status=False),
            interface("PipeInterface", bitrate=None),
            interface("UDPInterface", bitrate=0),
        ]}
        self.assertEqual(rates.counted_bitrates(stats), [1200.0])
        self.assertTrue(rates.slow_from_stats(stats, 100_000))

    def test_none_counted_is_not_slow(self):
        stats = {"interfaces": [interface("LocalClientInterface", bitrate=1_000_000_000)]}
        self.assertFalse(rates.slow_from_stats(stats, 100_000))
        self.assertFalse(rates.slow_from_stats({}, 100_000))

    def test_setting_overrides(self):
        slow_stats = types.SimpleNamespace(get_interface_stats=lambda: {"interfaces": [interface(bitrate=1200)]})
        self.assertTrue(rates.is_slow_node(Config(slow="auto"), slow_stats))
        self.assertFalse(rates.is_slow_node(Config(slow="no"), slow_stats))
        self.assertTrue(rates.is_slow_node(Config(slow="yes"), types.SimpleNamespace()))

    def test_stats_failure_is_not_slow(self):
        def broken():
            """Stats that can't be read."""
            raise OSError("no shared instance")
        self.assertFalse(rates.is_slow_node(Config(), types.SimpleNamespace(get_interface_stats=broken)))


class DeadlineAndBudgetTest(unittest.TestCase):
    """Response deadlines and requester max_bytes."""

    def test_deadline_at_rate_estimate(self):
        config = Config()
        # 3 × (10 000 bytes at 8 000 bit/s = 10 s) + 30 s slack
        self.assertAlmostEqual(rates.response_deadline(10_000, 8_000, config), 60)

    def test_deadline_unmeasured(self):
        config = Config(unmeasured_bitrate=100, deadline_slack=30)
        self.assertAlmostEqual(rates.response_deadline(1000, None, config), 3 * 80 + 30)

    def test_requester_max_bytes(self):
        config = Config(response_target=60)
        self.assertEqual(rates.requester_max_bytes(None, config), CHUNK_SIZE)
        self.assertEqual(rates.requester_max_bytes(8000, config), 60_000)


if __name__ == "__main__":
    unittest.main()
