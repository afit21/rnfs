"""REL-2 (0.2.0 release audit): a request for a block the node doesn't serve displaced a response in progress.

The attack: _serve reserved a response slot, choosing a response to displace when the pool was full, before the
backend said whether anything asked for is served. A ~100-byte /blocks (or /range) for a made-up CID, sent from a
fresh link with a small MTU (so it lands in the 2-slot slow pool), cancelled the slowest honest response older than
deadline_slack and was then answered [None] ([] for /range) as one packet. Repeated, it broke every slow download.

The test fills the slow pool with two honest LoRa downloads making steady progress for 80 s, then asks for an unserved
CID from a new link: the answers are today's ([None], []), and neither download is cancelled. The fix
(rnfs/network/server.py, RequestServer._serve; Backend.serves): when nothing asked for is served, the answer is sent at
once without a slot.
"""
import unittest

import RNS

from rnfs.constants import RANGE_PATH
from rnfs.content.cid import RAW, Cid
from rnfs.network import protocol
from tests.security._server_helpers import LORA_BITRATE, FakeInterface, ServerHarness

BLOCK = bytes(range(256)) * 256  # 64 KiB
SERVED = Cid.for_block(RAW, BLOCK)
NOT_SERVED = Cid.for_block(RAW, b"nothing this node holds")


class UnservedDisplacementTest(unittest.TestCase):
    def setUp(self):
        RNS.loglevel = RNS.LOG_CRITICAL
        self.h = ServerHarness({SERVED: BLOCK})
        self.radio = FakeInterface("lora", LORA_BITRATE)
        self.other = FakeInterface("tcp", 10_000_000)

    def fill_slow_pool(self):
        """Two honest slow-link downloads, moving steadily, older than deadline_slack."""
        honest = []
        for _ in range(self.h.server.pools.capacity["slow"]):
            link = self.h.connect(self.radio, hops=1, mtu=500)
            self.assertIsInstance(self.h.blocks(link, [SERVED]), list)
            resource = self.h.advertise(link)
            honest.append((link, resource))
        for step in range(1, 9):  # 80 s of steady progress
            for _, resource in honest:
                resource.progress = 0.05 * step
            self.h.tick(10)
        return honest

    def assert_none_cancelled(self, honest):
        cancelled = [resource.cancelled for _, resource in honest]
        self.assertEqual(cancelled, [False] * len(honest),
                         "an honest response was cancelled for a request that served nothing")

    def test_blocks_for_unserved_cid(self):
        honest = self.fill_slow_pool()
        newcomer = self.h.connect(self.other, hops=1, mtu=500)  # small MTU, no estimate: slow pool
        answer = self.h.blocks(newcomer, [NOT_SERVED])
        self.assertEqual(answer, [None])  # the answer an older node gets too
        self.assert_none_cancelled(honest)

    def test_range_for_unserved_cid(self):
        honest = self.fill_slow_pool()
        newcomer = self.h.connect(self.other, hops=1, mtu=500)
        handler = self.h.destination.handlers[RANGE_PATH][0]
        answer = handler(RANGE_PATH, protocol.encode_range(NOT_SERVED, 0, 1), newcomer.link_id, newcomer.link_id,
                         None, 0.0)
        self.assertEqual(answer, [])
        self.assert_none_cancelled(honest)


if __name__ == "__main__":
    unittest.main()
