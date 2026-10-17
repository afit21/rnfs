"""NET-2: a /blocks response that fits one packet frees its slot at once and never produces a rate estimate, and
the upload cap applied only once a link had one. So on a large-MTU path (RNS's TCP interfaces negotiate 16 KiB
and more) max_upload_rate never applied to a requester that only asked for blocks fitting one packet: the PoC
served 50 such responses (1 MB) on a 32000-byte MDU under max_upload_rate = 1000 bit/s with no pacing at all.

Fixed (decision Q5): an explicit max_upload_rate applies to every /blocks response, with or without a rate
estimate; the default cap (half the rate estimate on slow links) still needs an estimate. Waits up to
MAX_PACING_WAIT are held, longer ones refused at once, as for multi-packet responses.
"""
import unittest

from rnfs.config import Config
from rnfs.constants import KIB
from rnfs.network import server

from tests.security._server_helpers import TCP_BITRATE, FakeInterface, ServerHarness
from tests.test_server import block_of

MTU, MDU = 32768, 32000  # a TCP path (RNS: 16 KiB at 10 Mbit/s, 32 KiB at 100 Mbit/s)


class SinglePacketPacingTest(unittest.TestCase):
    """Responses that fit one packet on a large-MTU link, with no rate estimate."""

    def setUp(self):
        self.small = [block_of(20 * KIB, i) for i in range(60)]  # 20 KiB: one packet each on this link
        self.interface = FakeInterface("TCP", TCP_BITRATE)

    def harness(self, config: Config) -> tuple[ServerHarness, object, list]:
        """A server under config, a large-MTU link to it, and the waits it holds responses back for."""
        harness = ServerHarness(dict(self.small), config)
        link = harness.connect(self.interface, mtu=MTU, mdu=MDU)
        waits = []

        def pace(response, wait):
            """Hold the response back in virtual time."""
            waits.append(wait)
            harness.clock.now += wait
        harness.server._pace = pace
        return harness, link, waits

    def ask(self, harness, link, i: int):
        """Request one small block as request i; the handler's answer."""
        return harness.blocks(link, [self.small[i][0]], request_id=i.to_bytes(16, "big"))

    def test_poc_responses_refused_under_a_low_explicit_cap(self):
        harness, link, waits = self.harness(Config(max_upload_rate=1000))
        served = 0
        for i in range(50):
            body = self.ask(harness, link, i)
            if body is not False:
                served += 1
        self.assertIsNone(harness.server.rates.get(link.link_id))  # still no rate estimate
        # One 20 KiB response takes ~164 s at 1000 bit/s: every later request is refused at once
        self.assertEqual(served, 1)
        self.assertEqual(waits, [])
        self.assertGreater(harness.server.pacer.wait_time(link.link_id), 160)

    def test_waits_within_the_bound_are_held(self):
        cap = 1_000_000  # one 20 KiB response every ~0.16 s
        harness, link, waits = self.harness(Config(max_upload_rate=cap))
        start = harness.clock.now
        for i in range(20):
            self.assertIsNot(self.ask(harness, link, i), False)
        self.assertEqual(len(waits), 19)
        self.assertTrue(all(0 < wait <= server.MAX_PACING_WAIT for wait in waits))
        # 19 responses before the last had to have started by now: their bytes at the cap
        self.assertGreaterEqual(harness.clock.now - start, 19 * 20 * KIB * 8 / cap)

    def test_default_cap_still_needs_a_rate_estimate(self):
        # No explicit cap: the default (half the rate estimate on slow links) has nothing to halve
        harness, link, waits = self.harness(Config())
        for i in range(20):
            self.assertIsNot(self.ask(harness, link, i), False)
        self.assertEqual(waits, [])
        self.assertEqual(harness.server.pacer.wait_time(link.link_id), 0)

    def test_multi_packet_responses_also_paced_without_an_estimate(self):
        harness, link, waits = self.harness(Config(max_upload_rate=1000))
        big = block_of(100 * KIB, 99)
        harness.backend.blocks[big[0]] = big[1]
        self.assertIsNot(harness.blocks(link, [big[0]], request_id=b"b" * 16), False)
        self.assertGreater(harness.server.pacer.wait_time(link.link_id), server.MAX_PACING_WAIT)


if __name__ == "__main__":
    unittest.main()
