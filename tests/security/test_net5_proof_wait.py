"""NET-5: on the responder, link.rtt is whatever the requester claims (RNS takes the larger of what it measured
and the value in the requester's RTT packet, decoded with msgpack), so it can be huge, infinite, NaN, negative or
not a number at all. The server used it once, for how long a new /blocks waits for the previous response's
proof, as min(PROOF_GRACE, 2 × rtt + 1) on the real monotonic clock: a negative rtt gave no grace at all, a
string raised in the request handler, and the wait ignored the server's injected clock.

Fixed: RequestServer._proof_wait(link) is the only server-side use of link.rtt. It takes a finite, non-bool
rtt ≥ 0 (anything else counts as none), caps the wait at PROOF_GRACE, and _previous_done measures it on
self.clock.
"""
import ast
import math
import threading
import time
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import KIB
from rnfs.network import server

from tests.security._server_helpers import ServerHarness
from tests.support import wait_for
from tests.test_server import block_of

NO_RTT_WAIT = 1.0  # 2 × 0 + 1: what a link without a usable rtt gets


class ProofWaitTest(unittest.TestCase):
    """_proof_wait for every rtt a requester can claim."""

    def setUp(self):
        self.harness = ServerHarness({})

    def wait(self, rtt) -> float:
        """_proof_wait for a link whose rtt is rtt."""
        link = self.harness.connect()
        link.rtt = rtt
        return self.harness.server._proof_wait(link)

    def test_honest_rtts(self):
        self.assertAlmostEqual(self.wait(0.05), 1.1)
        self.assertAlmostEqual(self.wait(0), NO_RTT_WAIT)
        self.assertAlmostEqual(self.wait(1), 3.0)
        self.assertEqual(self.wait(2), server.PROOF_GRACE)

    def test_inflated_rtts_capped(self):
        for rtt in (1e9, 1e308, 10**400):
            with self.subTest(rtt=rtt):
                self.assertEqual(self.wait(rtt), server.PROOF_GRACE)

    def test_unusable_rtts_count_as_none(self):
        for rtt in (math.inf, -math.inf, math.nan, True, False, -1.0, -1e9, None, "5", b"\x05", [1.0], 1j):
            with self.subTest(rtt=rtt):
                wait = self.wait(rtt)
                self.assertEqual(wait, NO_RTT_WAIT)
                self.assertTrue(math.isfinite(wait))

    def test_no_rtt_attribute(self):
        link = self.harness.connect()
        del link.rtt
        self.assertEqual(self.harness.server._proof_wait(link), NO_RTT_WAIT)

    def test_link_rtt_used_only_through_the_helper(self):
        # Any future server-side use of link.rtt must go through _proof_wait, which caps it
        with open(server.__file__, encoding="utf-8") as f:
            tree = ast.parse(f.read())
        uses = []
        for function in ast.walk(tree):
            if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for node in ast.walk(function):
                attribute = isinstance(node, ast.Attribute) and node.attr == "rtt"
                looked_up = isinstance(node, ast.Constant) and node.value == "rtt"
                if attribute or looked_up:
                    uses.append(function.name)
        self.assertEqual(set(uses), {"_proof_wait"})


class ProofGraceClockTest(unittest.TestCase):
    """The grace a new /blocks waits for a withheld proof, on the server's clock, for claimed rtts."""

    def setUp(self):
        self.block = block_of(100 * KIB)
        self.harness = ServerHarness(dict([self.block]), Config())

    def withhold(self, rtt):
        """A link whose first response has all its parts sent but its proof withheld."""
        link = self.harness.connect()
        link.rtt = rtt
        self.assertIsNot(self.harness.blocks(link, [self.block[0]], request_id=b"a" * 16), False)
        self.harness.advertise(link, b"a" * 16).status = RNS.Resource.AWAITING_PROOF
        return link

    def ask_in_background(self, link) -> list:
        """A second /blocks on link, from another thread; its answer lands in the list returned."""
        results = []
        thread = threading.Thread(target=lambda: results.append(
            self.harness.blocks(link, [self.block[0]], request_id=b"b" * 16)), daemon=True)
        thread.start()
        self.addCleanup(thread.join, 10)
        return results

    def test_grace_ends_on_the_server_clock(self):
        for rtt in (1e9, math.inf, math.nan, True, -5.0, "x"):
            with self.subTest(rtt=rtt):
                self.harness = ServerHarness(dict([self.block]), Config())
                link = self.withhold(rtt)
                results = self.ask_in_background(link)
                time.sleep(0.05)
                self.assertEqual(results, [])  # waiting out the grace
                start = time.monotonic()
                self.harness.clock.now += server.PROOF_GRACE  # past any grace: the wait ends at once
                wait_for(lambda: results, timeout=3, what="the waiting request to be refused")
                self.assertLess(time.monotonic() - start, 2)
                self.assertEqual(results, [False])
                self.assertEqual(len([r for r in self.harness.server.pools.responses() if r.link_id == link.link_id]),
                                 1)

    def test_proof_within_the_grace_accepted(self):
        link = self.withhold(1e9)
        resource = link.outgoing_resources[0]
        results = self.ask_in_background(link)
        time.sleep(0.05)
        resource.complete()
        wait_for(lambda: results, timeout=3, what="the waiting request to be answered")
        self.assertIsNot(results[0], False)


if __name__ == "__main__":
    unittest.main()
