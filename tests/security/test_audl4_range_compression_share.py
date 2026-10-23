"""AUD-L4 (0.2.1 audit): one requester could spend the node's whole /range compression budget.

The attack: /range compresses with bz2 when asked, within one node-wide budget (RequestServer.compression, a
CompressionBudget of COMPRESS_RATE, 256 KiB of input a second, in bursts of COMPRESS_BURST, 1 MiB) so a flood of
compressed ranges can't spend the node's CPU. Any link could ask, and nothing divided the budget: one requester asking
for compressed 128 KiB ranges back to back took all of it, and every other link's ranges, slow LoRa ones included
(which are what compression is for), were sent uncompressed until it refilled, at up to four times their airtime.

The test has one link take the burst's worth of compressed ranges of a compressible block, then asks for a compressed
range from another slow link: it must come back compressed.

The fix (RequestServer._handle_range, CompressionBudget): each link may spend at most a COMPRESS_LINK_SHARE-th (a
quarter) of the budget, in a bucket of its own (refilled at that share of COMPRESS_RATE), forgotten when it closes.
Ranges are still compressed for any link that asks, as in 0.2.0: whether a link is slow is partly the requester's to
declare (AUD-M4), so refusing links the node classes fast would add little, and tests/test_client.py checks compressed
ranges over a real (fast, local TCP) link.
"""
import unittest

import RNS

from rnfs.constants import MAX_BLOCK_SIZE, RANGE_BZ2, RANGE_PATH
from rnfs.content.cid import RAW, Cid
from rnfs.network import protocol, server
from tests.security._server_helpers import LORA_BITRATE, TCP_BITRATE, FakeInterface, ServerHarness

BLOCK = (b"It is a truth universally acknowledged, that a single man in possession of a good fortune, must be in want "
         b"of a wife. ") * 1200
BLOCK = BLOCK[:MAX_BLOCK_SIZE]
CID = Cid.for_block(RAW, BLOCK)


class RangeCompressionShareTest(unittest.TestCase):
    def setUp(self):
        RNS.loglevel = RNS.LOG_CRITICAL
        self.h = ServerHarness({CID: BLOCK})
        self.count = 0

    def compressed_range(self, link) -> int:
        """Ask link for the whole block compressed, let the response complete; the answer's encoding."""
        self.count += 1
        request_id = self.count.to_bytes(16, "big")
        handler = self.h.destination.handlers[RANGE_PATH][0]
        answer = handler(RANGE_PATH, protocol.encode_range(CID, 0, MAX_BLOCK_SIZE, RANGE_BZ2), request_id,
                         link.link_id, None, 0.0)
        self.assertIsInstance(answer, list)
        resource = self.h.advertise(link, request_id)
        resource.complete()
        self.h.tick()
        return answer[1]

    def test_one_link_cannot_spend_the_whole_budget(self):
        greedy = self.h.connect(FakeInterface("tcp", TCP_BITRATE), mtu=500, mdu=431)  # slow: small MTU, no estimate
        for _ in range(server.COMPRESS_BURST // MAX_BLOCK_SIZE):
            self.compressed_range(greedy)
        honest = self.h.connect(FakeInterface("lora", LORA_BITRATE), mtu=500, mdu=431)
        self.assertEqual(self.compressed_range(honest), RANGE_BZ2,
                         "another link's range went uncompressed: one link had spent the node's budget")


if __name__ == "__main__":
    unittest.main()
