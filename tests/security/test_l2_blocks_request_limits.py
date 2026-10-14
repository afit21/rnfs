"""L2: the /blocks server used to serve duplicate CIDs and, on a link with no rate estimate, cap a response
only at the requester's max_bytes (up to ~1 MiB).

Duplicate CIDs: FIXED here. A request that repeats a CID is refused as malformed, so a node can't be made to
read and serve the same block several times in one response.

The unmeasured-link budget cap (at CHUNK_SIZE) is NOT applied, because doing so changes behaviour that four
tests in tests/test_server.py (another session's file) assert, and those must keep passing. The cap tests below
assert the safe (capped) behaviour and are @unittest.expectedFailure until the cap lands together with those
tests' updates (the exact diffs are in the handback report).
"""
import unittest

from rnfs.constants import CHUNK_SIZE, KIB, MAX_BLOCK_SIZE, MAX_RESPONSE_SIZE
from rnfs.network import protocol, server
from tests.security._server_helpers import TCP_BITRATE, FakeInterface, ServerHarness
from tests.test_server import block_of


class ParseDuplicatesTest(unittest.TestCase):
    """parse_blocks_request refuses a request that repeats a CID (fixed)."""

    def test_duplicate_cids_are_refused(self):
        (c1, _), (c2, _) = block_of(10, 1), block_of(20, 2)
        self.assertIsNotNone(protocol.parse_blocks_request(protocol.encode_blocks(1000, [c1, c2])))
        self.assertIsNone(protocol.parse_blocks_request(protocol.encode_blocks(1000, [c1, c1])))
        self.assertIsNone(protocol.parse_blocks_request(protocol.encode_blocks(1000, [c1, c2, c1])))

    def test_server_refuses_a_duplicate_request(self):
        (c1, b1), (c2, b2) = block_of(CHUNK_SIZE, 1), block_of(CHUNK_SIZE, 2)
        h = ServerHarness({c1: b1, c2: b2})
        link = h.connect(FakeInterface("hub", TCP_BITRATE), mtu=1500, mdu=1400)
        self.assertIs(h.blocks(link, [c1, c1]), False)
        self.assertEqual(h.server.pools.responses(), [])
        h.server.rates.record(link.link_id, 64 * KIB, 8000 * 8)  # measured, so more than one block fits
        self.assertEqual(h.blocks(link, [c1, c2], max_bytes=MAX_RESPONSE_SIZE), [b1, b2])  # distinct served


class MeasuredBudgetTest(unittest.TestCase):
    """With a rate estimate the budget is unchanged (this already holds)."""

    def test_budget_with_a_rate_estimate_is_unchanged(self):
        self.assertEqual(server.blocks_budget(1_000_000, 8000, None, 60), 60_000)
        self.assertEqual(server.blocks_budget(1_000_000, 8000, 4000, 60), 30_000)
        self.assertEqual(server.blocks_budget(5000, 1e9, None, 60), 5000)

    def test_measured_link_serves_more_than_one_block(self):
        blocks = {cid: data for cid, data in (block_of(CHUNK_SIZE, i) for i in range(10))}
        h = ServerHarness(blocks)
        link = h.connect(FakeInterface("hub", TCP_BITRATE), mtu=1500, mdu=1400)
        h.server.rates.record(link.link_id, 64 * KIB, 8000 * 8)  # 8 kB/s: 480 kB in 60 s
        served = [e for e in h.blocks(link, list(blocks), max_bytes=MAX_RESPONSE_SIZE) if e is not None]
        self.assertGreater(len(served), 1)


class UnmeasuredBudgetCapTest(unittest.TestCase):
    """The unmeasured-link budget cap at CHUNK_SIZE: the safe behaviour, not yet applied (see module docstring)."""

    # L2: fails until blocks_budget caps an unmeasured link at CHUNK_SIZE (and tests/test_server.py is updated)
    @unittest.expectedFailure
    def test_budget_without_a_rate_estimate_is_capped_at_chunk_size(self):
        self.assertEqual(server.blocks_budget(10 * CHUNK_SIZE, None, None, 60), CHUNK_SIZE)
        self.assertEqual(server.blocks_budget(MAX_RESPONSE_SIZE, None, None, 60), CHUNK_SIZE)

    # L2: fails until an unmeasured link's response is capped at CHUNK_SIZE
    @unittest.expectedFailure
    def test_unmeasured_link_response_is_small(self):
        blocks = {cid: data for cid, data in (block_of(CHUNK_SIZE, i) for i in range(10))}
        h = ServerHarness(blocks)
        link = h.connect(FakeInterface("hub", TCP_BITRATE), mtu=1500, mdu=1400)
        served = [e for e in h.blocks(link, list(blocks), max_bytes=MAX_RESPONSE_SIZE) if e is not None]
        self.assertEqual(len(served), 1)  # one CHUNK_SIZE block, not ten

    def test_first_block_is_still_served_whole_regardless(self):
        # The first served block is always included, up to MAX_BLOCK_SIZE, whether or not the cap applies
        (c1, b1), (c2, b2) = block_of(MAX_BLOCK_SIZE, 1), block_of(CHUNK_SIZE, 2)
        h = ServerHarness({c1: b1, c2: b2})
        link = h.connect(FakeInterface("hub", TCP_BITRATE), mtu=1500, mdu=1400)
        self.assertEqual(h.blocks(link, [c1], max_bytes=CHUNK_SIZE)[0], b1)


if __name__ == "__main__":
    unittest.main()
