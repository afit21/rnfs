"""/range (0.2.0): a new request is a new way in, so it must cost a node no more than /blocks already can.

The attacks: a requester sends /range with fields out of range (offsets and lengths past the largest block, booleans
for integers, a length of 0, a missing or extra field), hoping for an exception or an oversized read; holds a /blocks
response in flight and sends /range on the same link (or the reverse) to get two responses per link where /blocks
allows one; fills the response pools, spends upload_budget, or asks during quiet hours or on an interface not served,
hoping /range isn't counted where /blocks is; asks for a range past the block's end; or asks for compression on every
range of an incompressible block to spend the node's CPU. The test sends each through RequestServer's /range handler
over tests/security/_server_helpers.py's fakes on a FakeClock.

The fix: parse_range_request refuses every malformed shape (answered False); the /range handler goes through the same
path as /blocks (RequestServer._serve): the link's one /blocks slot, the pools, cooldowns, upload pacing, the airtime
controls and upload_budget, charged by packed size; the range is never longer than max_bytes or what is left of the
block; compression is done only when asked, only if smaller, and only within a node-wide compression budget
(CompressionBudget), past which the bytes go as they are.
"""
import bz2
import os
import unittest

from rnfs.config import Config
from rnfs.constants import BLOCKS_PATH, KIB, MAX_BLOCK_SIZE, RANGE_BZ2, RANGE_PATH
from rnfs.content.cid import RAW, Cid
from rnfs.network import protocol, server
from tests.security._server_helpers import TCP_BITRATE, FakeInterface, ServerHarness
from tests.test_server import block_of


def range_request(h: ServerHarness, link, data, request_id=None, identity=None):
    """Send a /range request with this data on link as RNS would; the handler's answer."""
    handler = h.destination.handlers[RANGE_PATH][0]
    return handler(RANGE_PATH, data, request_id or link.link_id, link.link_id, identity, 0.0)


def ask(h: ServerHarness, link, cid, offset, max_bytes, flags=0, request_id=None):
    """A well-formed /range request; the handler's answer."""
    return range_request(h, link, protocol.encode_range(cid, offset, max_bytes, flags), request_id)


def complete(h: ServerHarness, link, request_id=None):
    """RNS advertising the response to request_id on link and the requester receiving all of it."""
    resource = h.advertise(link, request_id)
    resource.progress = 1.0
    resource.complete()


def text_block(size: int) -> tuple[Cid, bytes]:
    """A compressible raw block of size bytes and its CID."""
    data = (b"It is a truth universally acknowledged, that a single man in possession of a good fortune "
            * (size // 90 + 1))[:size]
    return Cid.for_block(RAW, data), data


class MalformedRequestTest(unittest.TestCase):
    """Every malformed /range is answered False at once, taking no slot."""

    def test_malformed_shapes(self):
        cid, block = block_of(1000, 1)
        h = ServerHarness({cid: block})
        link = h.connect(FakeInterface("hub", TCP_BITRATE), mtu=1500, mdu=1400)
        bad = [
            None, [], [cid.binary], [cid.binary, 0, 10], [cid.binary, 0, 10, 0, 0],
            [b"not a cid", 0, 10, 0], [cid.binary, -1, 10, 0], [cid.binary, MAX_BLOCK_SIZE + 1, 10, 0],
            [cid.binary, 0, 0, 0], [cid.binary, 0, MAX_BLOCK_SIZE + 1, 0], [cid.binary, 0, 10, -1],
            [cid.binary, False, 10, 0], [cid.binary, 0, True, 0], [cid.binary, 0, 10, True],
            [cid.binary, 0.0, 10, 0], (cid.binary, 0, 10, 0), {"cid": cid.binary},
        ]
        for data in bad:
            with self.subTest(data=data):
                self.assertIsNone(protocol.parse_range_request(data))
                self.assertIs(range_request(h, link, data), False)
                self.assertEqual(h.server.pools.responses(), [])
                self.assertEqual(h.server.incoming.in_progress(link.link_id), (0, 0))

    def test_well_formed_edges(self):
        cid = block_of(10, 1)[0]
        self.assertEqual(protocol.parse_range_request([cid.binary, MAX_BLOCK_SIZE, MAX_BLOCK_SIZE, 0]),
                         (cid, MAX_BLOCK_SIZE, MAX_BLOCK_SIZE, 0))
        self.assertEqual(protocol.parse_range_request([cid.binary, 0, 1, 255]), (cid, 0, 1, 255))  # reserved bits

    def test_offset_past_the_end_is_refused_and_unserved_is_empty(self):
        cid, block = block_of(1000, 1)
        h = ServerHarness({cid: block})
        link = h.connect(FakeInterface("hub", TCP_BITRATE), mtu=1500, mdu=1400)
        self.assertIs(ask(h, link, cid, 1001, 10), False)
        self.assertEqual(ask(h, link, block_of(5, 9)[0], 0, 10), [])  # not served: an answer RNS sends
        self.assertEqual(ask(h, link, cid, 1000, 10), [1000, 0, b""])  # at the very end: nothing left
        self.assertEqual(h.server.pools.responses(), [])


class SameSlotsAsBlocksTest(unittest.TestCase):
    """/range takes the link's one /blocks slot and a pool slot, as /blocks does."""

    def setUp(self):
        self.cid, self.block = block_of(MAX_BLOCK_SIZE, 1)
        self.h = ServerHarness({self.cid: self.block}, Config(max_blocks_responses=2, fast_blocks_responses=1))
        self.link = self.h.connect(FakeInterface("hub", TCP_BITRATE), mtu=1500, mdu=1400)

    def test_range_while_blocks_is_in_flight_is_refused(self):
        self.assertEqual(self.h.blocks(self.link, [self.cid], request_id=b"b" * 16), [self.block])
        self.h.advertise(self.link, b"b" * 16)
        self.assertIs(ask(self.h, self.link, self.cid, 0, 1000, request_id=b"r" * 16), False)

    def test_blocks_while_range_is_in_flight_is_refused(self):
        answer = ask(self.h, self.link, self.cid, 0, 64 * KIB, request_id=b"r" * 16)
        self.assertEqual(answer, [MAX_BLOCK_SIZE, 0, self.block[:64 * KIB]])
        self.h.advertise(self.link, b"r" * 16)
        self.assertIsNotNone(self.h.response(self.link))  # holds a pool slot
        self.assertEqual(self.h.server.incoming.in_progress(self.link.link_id), (1, 1))
        self.assertIs(self.h.blocks(self.link, [self.cid], request_id=b"b" * 16), False)

    def test_full_pool_refuses_range(self):
        # max_blocks_responses=2: one fast slot. Another link's /blocks holds it, young
        other = self.h.connect(FakeInterface("hub", TCP_BITRATE), mtu=1500, mdu=1400)
        self.assertEqual(self.h.blocks(other, [self.cid]), [self.block])
        self.h.advertise(other)
        self.assertIs(ask(self.h, self.link, self.cid, 0, 64 * KIB), False)

    def test_range_is_never_longer_than_asked_or_left(self):
        self.assertEqual(ask(self.h, self.link, self.cid, MAX_BLOCK_SIZE - 10, 1000), [MAX_BLOCK_SIZE, 0,
                                                                                      self.block[-10:]])


class AirtimeControlsTest(unittest.TestCase):
    """Interface rules, quiet hours, cooldowns and upload_budget refuse or charge /range as /blocks."""

    def test_interface_rules_refuse_range(self):
        cid, block = block_of(1000, 1)
        h = ServerHarness({cid: block}, Config(no_serve_interfaces=["lora"]))
        link = h.connect(FakeInterface("lora0", 1000))
        self.assertIs(ask(h, link, cid, 0, 100), False)
        self.assertIs(h.blocks(link, [cid]), False)

    def test_own_airtime_over_budget_refuses_range(self):
        cid, block = block_of(1000, 1)
        h = ServerHarness({cid: block})
        h.server.airtime.over_airtime = lambda: True  # the radio measures this node over its airtime budget
        link = h.connect(FakeInterface("lora0", 1000))
        self.assertIs(ask(h, link, cid, 0, 100), False)
        self.assertIs(h.blocks(link, [cid]), False)

    def test_cooldown_refuses_range(self):
        cid, block = block_of(1000, 1)
        h = ServerHarness({cid: block})
        link = h.connect(FakeInterface("hub", TCP_BITRATE), mtu=1500, mdu=1400)
        h.server.cooldowns.add(link.link_id)
        self.assertIs(ask(h, link, cid, 0, 100), False)

    def test_upload_budget_charges_range_by_packed_size(self):
        cid, block = block_of(MAX_BLOCK_SIZE, 1)
        h = ServerHarness({cid: block}, Config(upload_budget=200 * KIB))
        link = h.connect(FakeInterface("hub", TCP_BITRATE), mtu=1500, mdu=1400)
        before = h.server.airtime.bucket.tokens
        answer = ask(h, link, cid, 0, 64 * KIB)
        self.assertEqual(len(answer[2]), 64 * KIB)
        complete(h, link)
        packed = protocol.packed_response_size(link.link_id, answer)
        self.assertAlmostEqual(before - h.server.airtime.bucket.tokens, packed, delta=64)
        # An empty bucket refuses /range before building anything
        h.server.airtime.bucket.tokens = -1
        self.assertIs(ask(h, link, cid, 0, 64 * KIB, request_id=b"x" * 16), False)


class CompressionTest(unittest.TestCase):
    """Compression only when asked, only if smaller, within the node's compression budget."""

    def test_only_when_asked_and_smaller(self):
        (tcid, text), (rcid, noise) = text_block(64 * KIB), (lambda d: (Cid.for_block(RAW, d), d))(os.urandom(4096))
        h = ServerHarness({tcid: text, rcid: noise})
        link = h.connect(FakeInterface("hub", TCP_BITRATE), mtu=1500, mdu=1400)
        self.assertEqual(ask(h, link, tcid, 0, 32 * KIB), [64 * KIB, 0, text[:32 * KIB]])  # not asked
        complete(h, link)
        size, encoding, data = ask(h, link, tcid, 0, 32 * KIB, RANGE_BZ2, request_id=b"t" * 16)
        self.assertEqual((size, encoding), (64 * KIB, RANGE_BZ2))
        self.assertEqual(bz2.decompress(data), text[:32 * KIB])
        self.assertLess(len(data), 32 * KIB // 2)
        complete(h, link, b"t" * 16)
        # Random bytes don't get smaller: sent as they are
        self.assertEqual(ask(h, link, rcid, 0, 4096, RANGE_BZ2, request_id=b"n" * 16), [4096, 0, noise])

    def test_budget_spent_sends_raw(self):
        tcid, text = text_block(MAX_BLOCK_SIZE)
        h = ServerHarness({tcid: text})
        h.server.compression.tokens = 0  # spent: the next compression waits for it to refill
        link = h.connect(FakeInterface("hub", TCP_BITRATE), mtu=1500, mdu=1400)
        self.assertEqual(ask(h, link, tcid, 0, MAX_BLOCK_SIZE, RANGE_BZ2), [MAX_BLOCK_SIZE, 0, text])

    def test_budget_bounds_bytes_compressed_per_second(self):
        budget = server.CompressionBudget(server.COMPRESS_RATE, server.COMPRESS_BURST, clock=lambda: 0.0)
        taken = 0
        while budget.take(MAX_BLOCK_SIZE):
            taken += MAX_BLOCK_SIZE
        self.assertLessEqual(taken, server.COMPRESS_BURST)
        self.assertLessEqual(server.COMPRESS_RATE, 256 * KIB)


class BlocksStillAnsweredTest(unittest.TestCase):
    """The handler is additive: /blocks is registered and answered exactly as before."""

    def test_both_paths_registered(self):
        cid, block = block_of(1000, 1)
        h = ServerHarness({cid: block})
        self.assertIn(BLOCKS_PATH, h.destination.handlers)
        self.assertIn(RANGE_PATH, h.destination.handlers)
        self.assertFalse(h.destination.handlers[RANGE_PATH][2])  # auto_compress=False: never RNS's compression


if __name__ == "__main__":
    unittest.main()
