"""Tests for PeerClient over a fake RNS, plus a two-process integration test over real RNS (RNFS_E2E=1).

The integration test runs this file as a script in two subprocesses, each with its own Reticulum instance
(RNS.Reticulum is a per-process singleton), connected over local TCP: one serves /find, /provide and
/blocks through RequestServer with a trivial backend, the other drives PeerClient against it.
"""
import json
import os
import random
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from rnfs.config import Config  # noqa: E402
from rnfs.constants import (BLOCKS_PATH, CHUNK_SIZE, FIND_PATH, KIB, MAX_BLOCK_SIZE, PIN_PATH,  # noqa: E402
                            PROVIDE_PATH, RANGE_BZ2, RANGE_PATH)
from rnfs.content.cid import RAW, Cid  # noqa: E402
from rnfs.network import client, links, paths, protocol  # noqa: E402

import tests.test_links as fakes  # noqa: E402  (a module, so its test cases don't run twice)
from tests.support import free_port  # noqa: E402

NODE = bytes(16)
KEY = bytes(range(16))


def block_of(size: int, seed: int) -> tuple[Cid, bytes]:
    """A pseudo-random raw block of size bytes and its CID."""
    data = random.Random(seed).randbytes(size)
    return Cid.for_block(RAW, data), data


class PeerClientTest(unittest.TestCase):
    """PeerClient with RNS replaced by the fakes from test_links."""

    def setUp(self):
        """A client over a LinkPool whose links are FakeLinks."""
        patcher = mock.patch.object(links, "RNS", fakes.fake_rns())
        patcher.start()
        self.addCleanup(patcher.stop)
        fakes.FakeLink.created = []
        fakes.FakeLink.establish_delay = 0.0
        fakes.FakeLink.response_size = 10
        self.identity = object()
        self.config = Config(stall_timeout=0.5)
        self.pool = links.LinkPool(self.identity, self.config)
        self.addCleanup(self.pool.close_all)
        self.client = client.PeerClient(self.pool, self.config)

    def respond(self, function):
        """Answer every request with function(link, path, data)."""
        fakes.FakeLink.responder = staticmethod(function)

    def call(self, function):
        """Run function on a thread, failing on deadlock."""
        return fakes.run(self, function)

    # /find

    def test_find(self):
        self.respond(lambda link, path, data: [[KEY], [NODE]])
        self.assertEqual(self.call(lambda: self.client.find(NODE, KEY)), ([KEY], [NODE]))
        self.assertTrue(self.client.has_link(NODE))
        self.assertEqual(self.client.open_links(), [NODE])

    def test_find_refused_or_malformed(self):
        for answer in (False, [[KEY]], [[b"short"], []], fakes.FAIL):
            with self.subTest(answer=answer):
                self.respond(lambda link, path, data: answer)
                self.assertIsNone(self.call(lambda: self.client.find(NODE, KEY)))

    def test_small_requests_cap_their_response_size(self):
        self.respond(lambda link, path, data: [[KEY] * 10, [NODE] * 10] if path == FIND_PATH else True)
        self.call(lambda: self.client.find(NODE, KEY))
        self.call(lambda: self.client.provide(NODE, [KEY], []))
        self.call(lambda: self.client.pin(NODE, block_of(10, 1)[0], True))
        sizes = {path: max_size for link in fakes.FakeLink.created for path, _, max_size in link.requests}
        self.assertEqual(sizes, {FIND_PATH: protocol.MAX_FIND_RESPONSE_SIZE,
                                 PROVIDE_PATH: protocol.MAX_ACCEPT_RESPONSE_SIZE,
                                 PIN_PATH: protocol.MAX_ACCEPT_RESPONSE_SIZE})
        # Exactly the largest honest answers, which fit one packet
        self.assertEqual(protocol.MAX_FIND_RESPONSE_SIZE,
                         protocol.packed_response_size(bytes(16), [[KEY] * 10, [NODE] * 10]))
        self.assertLessEqual(protocol.MAX_FIND_RESPONSE_SIZE, protocol.DEFAULT_MDU)

    def test_trickled_answers_give_up_at_the_deadline(self):
        # A response Resource that starts and never finishes stops RNS's own timeout; the request must
        # still give up and free its slot
        self.respond(lambda link, path, data: fakes.Stall(300))
        with mock.patch.object(links, "RESPONSE_GRACE", 0.1):
            self.assertIsNone(self.call(lambda: self.client.find(NODE, KEY)))
            peer = self.call(lambda: self.pool.get(NODE))
            self.assertEqual(peer.requests, 0)
            # Unanswered, so it may have been recorded: the key counts as sent there, for a later withdrawal
            self.assertEqual(self.call(lambda: self.client.provide(NODE, [KEY], [])), {KEY})
            self.assertFalse(self.call(lambda: self.client.pin(NODE, block_of(10, 1)[0], True)))

    def test_find_waits_for_a_slot_no_longer_than_the_deadline(self):
        self.respond(lambda link, path, data: [[KEY], [NODE]])
        peer = self.call(lambda: self.pool.get(NODE))
        self.assertTrue(peer.acquire(False) and peer.acquire(False))  # both slots held, e.g. by trickled answers
        with mock.patch.object(links, "RESPONSE_GRACE", 0.1):
            self.assertIsNone(self.call(lambda: self.client.find(NODE, KEY)))
        self.assertEqual(peer.link.requests, [])
        peer.release(False)
        self.assertEqual(self.call(lambda: self.client.find(NODE, KEY)), ([KEY], [NODE]))

    def test_find_unreachable(self):
        links.RNS.Transport.has_path = lambda node_id: False
        links.RNS.Transport.await_path = lambda node_id, timeout: False
        self.assertIsNone(self.call(lambda: self.client.find(NODE, KEY)))

    # /provide and /pin

    def test_provide_batches_over_one_identified_link(self):
        self.respond(lambda link, path, data: True)
        keys = [i.to_bytes(16, "big") for i in range(30)]
        self.assertEqual(self.call(lambda: self.client.provide(NODE, keys, [])), set(keys))
        (link,) = fakes.FakeLink.created
        self.assertEqual(link.identities, [self.identity])
        self.assertEqual([len(data[0]) for path, data, _ in link.requests], [22, 8])
        self.assertTrue(all(path == PROVIDE_PATH for path, _, _ in link.requests))
        self.assertEqual(link.status, fakes.FakeLink.CLOSED)
        self.assertFalse(self.client.has_link(NODE))

    def test_provide_refused_is_retried_once_then_stops(self):
        self.respond(lambda link, path, data: False)
        keys = [i.to_bytes(16, "big") for i in range(30)]
        # The refused batch may have been recorded in part; the batch after it was never sent
        self.assertEqual(self.call(lambda: self.client.provide(NODE, keys, [KEY])), set(keys[:21]))
        (link,) = fakes.FakeLink.created
        self.assertEqual(len(link.requests), 2)  # first batch and its one retry
        self.assertEqual(link.status, fakes.FakeLink.CLOSED)

    def test_provide_reports_the_keys_sent_up_to_a_refusal(self):
        keys = [i.to_bytes(16, "big") for i in range(50)]
        self.respond(lambda link, path, data: keys[0] in data[0])  # only the first batch is recorded
        self.assertEqual(self.call(lambda: self.client.provide(NODE, keys, [])), set(keys[:44]))
        (link,) = fakes.FakeLink.created
        self.assertEqual([len(data[0]) for path, data, _ in link.requests], [22, 22, 22])  # one retry

    def test_provide_without_a_link_reports_nothing_sent(self):
        links.RNS.Transport.has_path = lambda node_id: False
        links.RNS.Transport.await_path = lambda node_id, timeout: False
        self.assertEqual(self.call(lambda: self.client.provide(NODE, [KEY], [])), set())

    def test_pin(self):
        self.respond(lambda link, path, data: path == PIN_PATH and data[1] is True)
        cid = block_of(10, 1)[0]
        self.assertTrue(self.call(lambda: self.client.pin(NODE, cid, True)))
        self.assertEqual(fakes.FakeLink.created[0].identities, [self.identity])
        self.assertEqual(fakes.FakeLink.created[0].status, fakes.FakeLink.CLOSED)

    def test_ping(self):
        self.assertEqual(self.call(lambda: self.client.ping(NODE)), 0.01)
        self.assertEqual(fakes.FakeLink.created[0].status, fakes.FakeLink.CLOSED)

    # /blocks

    def test_blocks_sorted_and_verified(self):
        (c1, b1), (c2, _), (c3, _), (c4, _) = (block_of(100, i) for i in range(4))
        self.respond(lambda link, path, data: [b1, None, b"forged"])
        result = self.call(lambda: self.client.blocks(NODE, [c1, c2, c3, c4, c1]))
        self.assertEqual(result.status, client.OK)
        self.assertEqual(result.blocks, {c1: b1})
        self.assertEqual(result.missing, [c2])
        self.assertEqual(result.bad, [c3])
        self.assertEqual(result.unsent, [c4])

    def test_blocks_request_without_rate_estimate(self):
        self.respond(lambda link, path, data: [])
        cid = block_of(10, 1)[0]
        self.call(lambda: self.client.blocks(NODE, [cid]))
        path, data, max_response_size = fakes.FakeLink.created[0].requests[0]
        self.assertEqual((path, data), (BLOCKS_PATH, [CHUNK_SIZE, [cid.binary]]))
        self.assertEqual(max_response_size, MAX_BLOCK_SIZE + KIB)

    def test_blocks_records_rate_and_uses_it(self):
        cid, block = block_of(20_000, 1)
        self.respond(lambda link, path, data: [block])
        fakes.FakeLink.response_size = 20_100
        peer = self.call(lambda: self.pool.get(NODE))
        peer.link.expected_rate = 80_000
        self.call(lambda: self.client.blocks(NODE, [cid]))
        self.assertEqual(self.client.rate_estimate(NODE), 80_000)
        self.assertEqual(self.client.traffic.peer(NODE)["link_rate"], 80_000)  # the last link's, for slow paths
        self.call(lambda: self.client.blocks(NODE, [cid]))
        # response_target (60 s) at 80 kbit/s
        self.assertEqual(peer.link.requests[1][1][0], 600_000)

    def test_small_response_gives_no_rate(self):
        cid, block = block_of(100, 1)
        self.respond(lambda link, path, data: [block])
        peer = self.call(lambda: self.pool.get(NODE))
        peer.link.expected_rate = 80_000
        self.call(lambda: self.client.blocks(NODE, [cid]))
        self.assertIsNone(self.client.rate_estimate(NODE))
        self.assertIsNone(self.client.traffic.peer(NODE)["link_rate"])

    def test_blocks_elapsed_counts_from_sending(self):
        """A /blocks result's elapsed time runs from sending the request, not from opening the link."""
        cid, block = block_of(100, 1)

        def answer(link, path, data):
            """Answer after 0.1 s."""
            time.sleep(0.1)
            return [block]
        self.respond(answer)
        fakes.FakeLink.establish_delay = 0.5
        start = time.monotonic()
        result = self.call(lambda: self.client.blocks(NODE, [cid]))
        # A tick short of 0.6 s and 0.1 s: before Python 3.13, time.monotonic on Windows ticks every 15.6 ms
        self.assertGreaterEqual(time.monotonic() - start, 0.58)
        self.assertGreaterEqual(result.elapsed, 0.08)
        self.assertLess(result.elapsed, 0.5)  # opening the link isn't counted
        self.respond(lambda link, path, data: False)
        self.assertIsNone(self.call(lambda: self.client.blocks(NODE, [cid])).elapsed)

    # /range (rnfs 0.2.0)

    def serve_ranges(self, block: bytes, compress_answers: bool = False):
        """Answer /range as a 0.2.0 node would from block (bz2 when asked and compress_answers)."""
        def answer(link, path, data):
            """[size, encoding, bytes] for the range asked."""
            self.assertEqual(path, RANGE_PATH)
            _, offset, max_bytes, flags = data
            part = block[offset:offset + max_bytes]
            if flags & RANGE_BZ2 and compress_answers:
                return protocol.encode_range_response(len(block), RANGE_BZ2, protocol.compress_range(part))
            return protocol.encode_range_response(len(block), 0, part)
        self.respond(answer)

    def test_range_brings_part_then_the_whole_verified_block(self):
        cid, block = block_of(40 * KIB, 1)
        self.serve_ranges(block)
        first = self.call(lambda: self.client.block_range(NODE, cid, b"", 16 * KIB))
        self.assertEqual((first.status, first.size, first.data, first.block), (client.OK, 40 * KIB, block[:16 * KIB],
                                                                               None))
        path, data, max_response_size = fakes.FakeLink.created[0].requests[0]
        self.assertEqual((path, data), (RANGE_PATH, [cid.binary, 0, 16 * KIB, 0]))
        self.assertEqual(max_response_size, 17 * KIB)
        last = self.call(lambda: self.client.block_range(NODE, cid, block[:32 * KIB], 16 * KIB, block_size=40 * KIB))
        self.assertEqual((last.status, last.data, last.block, last.bad), (client.OK, block[32 * KIB:], block, False))
        self.assertEqual(fakes.FakeLink.created[0].requests[1][1], [cid.binary, 32 * KIB, 16 * KIB, 0])
        self.assertEqual(self.client.traffic.peer(NODE)["received"], 40 * KIB)  # counted once verified

    def test_range_compressed(self):
        data = b"a line of text that repeats itself, as text does\n" * 400
        cid = Cid.for_block(RAW, data)
        self.serve_ranges(data, compress_answers=True)
        result = self.call(lambda: self.client.block_range(NODE, cid, b"", len(data), compress=True))
        self.assertEqual((result.block, result.compressed), (data, True))
        self.assertEqual(fakes.FakeLink.created[0].requests[0][1][3], RANGE_BZ2)

    def test_range_mismatch_is_bad(self):
        cid, block = block_of(1000, 1)
        self.serve_ranges(b"x" + block[1:])
        result = self.call(lambda: self.client.block_range(NODE, cid, b"", 1000))
        self.assertEqual((result.status, result.bad, result.block), (client.OK, True, None))
        self.assertEqual(self.client.traffic.peer(NODE)["bad"], 1)

    def test_range_unserved_busy_and_malformed(self):
        cid, block = block_of(1000, 1)
        for answer, status, missing in (([], client.OK, True), (False, client.BUSY, False),
                                        ([1000, 0, b"short"], client.FAILED, False),
                                        ([999, 0, block[:100]], client.FAILED, False),  # not the size known
                                        ([1000, RANGE_BZ2, block[:100]], client.FAILED, False),  # not asked for
                                        ([b"a", b"b"], client.FAILED, False), ("x", client.FAILED, False)):
            with self.subTest(answer=answer):
                self.respond(lambda link, path, data: answer)
                result = self.call(lambda: self.client.block_range(NODE, cid, b"", 100, block_size=1000))
                self.assertEqual((result.status, result.missing, result.data), (status, missing, b""))

    def test_range_left_unanswered_is_silent(self):
        # What a node before 0.2.0 does: RNS times the request out with the link up
        self.respond(lambda link, path, data: fakes.FAIL)
        cid = block_of(10, 1)[0]
        result = self.call(lambda: self.client.block_range(NODE, cid, b"", 10))
        self.assertEqual((result.status, result.silent), (client.LOST, True))
        self.assertTrue(self.client.has_link(NODE))  # the link is fine: /blocks goes on it next
        self.assertEqual(self.client.traffic.peer(NODE)["failed"], 0)  # not the node's failure
        # A link the node closed under it isn't silence: a refusal, as for /blocks
        self.close_under_request(links.DESTINATION_CLOSED)
        result = self.call(lambda: self.client.block_range(NODE, cid, b"", 10))
        self.assertEqual((result.status, result.reason, result.silent), (client.BUSY, "closed", False))

    def test_range_stall_closes_link(self):
        self.respond(lambda link, path, data: fakes.Stall(50_000))
        cid = block_of(10, 1)[0]
        result = self.call(lambda: self.client.block_range(NODE, cid, b"", 10))
        self.assertEqual(result.status, client.STALLED)
        self.assertFalse(self.client.has_link(NODE))

    def test_hold_link(self):
        self.assertFalse(self.client.hold_link(NODE, 60))  # no link to hold
        peer = self.call(lambda: self.pool.get(NODE))
        self.assertTrue(self.client.hold_link(NODE, 60))
        self.assertIsNotNone(peer.held_until)

    def test_link_mdu(self):
        self.assertIsNone(self.client.link_mdu(NODE))
        peer = self.call(lambda: self.pool.get(NODE))
        self.assertEqual(self.client.link_mdu(NODE), 431)
        self.pool.close_link(peer)
        self.assertIsNone(self.client.link_mdu(NODE))

    def test_blocks_batch_cut(self):
        cids = [block_of(10, i)[0] for i in range(15)]
        self.respond(lambda link, path, data: [None] * len(data[1]))
        result = self.call(lambda: self.client.blocks(NODE, cids))
        self.assertEqual(len(result.missing), 10)
        self.assertEqual(result.unsent, cids[10:])

    def test_blocks_busy(self):
        self.respond(lambda link, path, data: False)
        cid = block_of(10, 1)[0]
        result = self.call(lambda: self.client.blocks(NODE, [cid]))
        self.assertEqual((result.status, result.unsent), (client.BUSY, [cid]))

    def test_blocks_malformed_fails(self):
        cid = block_of(10, 1)[0]
        for answer in ([b"a", b"b"], "x", []):
            with self.subTest(answer=answer):
                self.respond(lambda link, path, data: answer)
                self.assertEqual(self.call(lambda: self.client.blocks(NODE, [cid])).status, client.FAILED)

    def test_blocks_timed_out_is_lost(self):
        # RNS gave up on the request with the link up: its packet or its answer's was lost, most likely
        self.respond(lambda link, path, data: fakes.FAIL)
        cid = block_of(10, 1)[0]
        result = self.call(lambda: self.client.blocks(NODE, [cid]))
        self.assertEqual((result.status, result.unsent), (client.LOST, [cid]))
        self.assertTrue(self.client.has_link(NODE))  # the fetcher decides whether to close it

    def close_under_request(self, reason):
        """Respond by closing the link with RNS's teardown reason (None: closed by this node's teardown())."""
        def answer(link, path, data):
            """Close the link instead of answering."""
            link.teardown_reason = reason
            link.teardown()
            return fakes.SILENT
        self.respond(answer)

    def test_blocks_on_a_link_that_timed_out_is_lost(self):
        self.close_under_request(links.LINK_TIMEOUT)
        result = self.call(lambda: self.client.blocks(NODE, [block_of(10, 1)[0]]))
        self.assertEqual(result.status, client.LOST)

    def test_blocks_on_a_link_closed_by_this_node_fails(self):
        self.close_under_request(links.INITIATOR_CLOSED)
        result = self.call(lambda: self.client.blocks(NODE, [block_of(10, 1)[0]]))
        self.assertEqual(result.status, client.FAILED)

    def test_blocks_on_a_link_the_provider_closed_is_busy(self):
        # A server with no room for another incoming link closes it (RNS: DESTINATION_CLOSED at the opener): a
        # refusal, retried on a new link after the busy delay, as a False is
        self.close_under_request(links.DESTINATION_CLOSED)
        result = self.call(lambda: self.client.blocks(NODE, [block_of(10, 1)[0]]))
        self.assertEqual((result.status, result.reason), (client.BUSY, "closed"))
        self.assertEqual(self.client.traffic.snapshot()["totals"]["refused"], 1)

    def find_closed(self, node_id: bytes = NODE) -> tuple:
        """/find to NODE on a thread: (its answer, closed_by_node(node_id) on that thread afterwards)."""
        return self.call(lambda: (self.client.find(NODE, KEY), self.client.closed_by_node(node_id)))

    def test_find_on_a_link_the_server_closed_is_a_refusal(self):
        """closed_by_node tells the lookup's failure callback, on the querying thread, that the server closed the link:
        not counted against it (specification section 6)."""
        self.close_under_request(links.DESTINATION_CLOSED)
        self.assertEqual(self.find_closed(), (None, True))
        self.assertEqual(self.find_closed(KEY), (None, False))  # asked about another node
        self.assertFalse(self.client.closed_by_node(NODE))  # another thread
        self.assertEqual(self.client.traffic.snapshot()["totals"]["refused"], 2)
        for reason in (links.LINK_TIMEOUT, links.INITIATOR_CLOSED):
            with self.subTest(reason=reason):
                self.close_under_request(reason)
                self.assertEqual(self.find_closed(), (None, False))
        self.respond(lambda link, path, data: [[KEY], [NODE]])
        self.assertEqual(self.find_closed(), (([KEY], [NODE]), False))

    def test_link_closed_as_it_is_established_is_a_refusal(self):
        def refuse(link):
            """The server tears the link down as it establishes it."""
            link.teardown_reason = links.DESTINATION_CLOSED
            link.teardown()
            return True
        fakes.FakeLink.establishes = staticmethod(refuse)
        self.addCleanup(setattr, fakes.FakeLink, "establishes", None)
        charged = self.pool.paths.charged
        self.assertEqual(self.find_closed(), (None, True))
        self.assertTrue(self.pool.refused(NODE) and not self.pool.unestablished(NODE))
        self.assertEqual(self.pool.paths.charged, charged)  # RNS doesn't ask for the path again
        result = self.call(lambda: self.client.blocks(NODE, [block_of(10, 1)[0]]))
        self.assertEqual((result.status, result.reason), (client.BUSY, "closed"))
        fakes.FakeLink.establishes = None
        self.respond(lambda link, path, data: [[KEY], [NODE]])
        self.assertIsNotNone(self.call(lambda: self.client.find(NODE, KEY)))
        self.assertFalse(self.pool.refused(NODE))

    def test_blocks_link_not_established_is_lost_no_path_fails(self):
        cid = block_of(10, 1)[0]
        self.respond(lambda link, path, data: [None])
        fakes.FakeLink.establishes = staticmethod(lambda link: False)
        self.addCleanup(setattr, fakes.FakeLink, "establishes", None)
        result = self.call(lambda: self.client.blocks(NODE, [cid]))
        self.assertEqual((result.status, result.reason), (client.LOST, "link not established"))
        fakes.FakeLink.establishes = None  # established now: what get() last saw is forgotten
        self.assertEqual(self.call(lambda: self.client.blocks(NODE, [cid])).status, client.OK)
        self.pool.close(NODE)
        # A budget that hasn't seen the failed link: after one, the pool waits path_timeout for the path RNS requests
        # again by itself (network/paths.py) rather than failing at once
        self.pool.paths = paths.PathRequests(transport=lambda: links.RNS.Transport, reticulum=lambda: None)
        links.RNS.Transport.has_path = lambda node_id: False
        links.RNS.Transport.await_path = lambda node_id, timeout: False
        result = self.call(lambda: self.client.blocks(NODE, [cid]))
        self.assertEqual((result.status, result.reason), (client.FAILED, "unreachable"))

    def test_blocks_empty_answer_is_a_failure(self):
        # An honest node always answers at least the first CID, so an empty answer is malformed: as OK it
        # would put every CID back as unsent, and the fetcher would ask the same node again forever
        cids = [block_of(10, i)[0] for i in range(3)]
        for answer in ([], [None]):
            with self.subTest(answer=answer):
                self.respond(lambda link, path, data: answer)
                result = self.call(lambda: self.client.blocks(NODE, cids))
                self.assertEqual((result.status, result.unsent), (client.FAILED, cids))

    def test_blocks_stall_closes_link(self):
        self.respond(lambda link, path, data: fakes.Stall(10_000))
        cid = block_of(10, 1)[0]
        result = self.call(lambda: self.client.blocks(NODE, [cid]))
        self.assertEqual(result.status, client.STALLED)
        self.assertIn("no progress", result.reason)
        self.assertEqual(fakes.FakeLink.created[0].status, fakes.FakeLink.CLOSED)
        self.assertFalse(self.client.has_link(NODE))

    def peer_with_incoming(self, incoming):
        """Open the shared link, giving it an incoming_resources list like RNS's."""
        peer = self.call(lambda: self.pool.get(NODE))
        peer.link.incoming_resources = incoming
        return peer

    def test_blocks_cancelled_by_responder_is_busy(self):
        self.config.stall_timeout = 60
        self.peer_with_incoming([])  # the response Resource started, then the responder cancelled it
        self.respond(lambda link, path, data: fakes.Stall(10_000))
        with mock.patch.object(links, "CANCEL_GRACE", 0.3):
            result = self.call(lambda: self.client.blocks(NODE, [block_of(10, 1)[0]]))
        self.assertEqual((result.status, result.reason), (client.BUSY, "cancelled"))
        self.assertTrue(self.client.has_link(NODE))  # not a stall: the link stays open

    def test_response_still_arriving_is_not_cancelled(self):
        peer = self.peer_with_incoming(None)
        peer.link.incoming_resources = [mock.Mock(request_id=bytes([0]) * 16)]  # the first request's response
        self.respond(lambda link, path, data: fakes.Stall(10_000))
        with mock.patch.object(links, "CANCEL_GRACE", 0.1):
            result = self.call(lambda: self.client.blocks(NODE, [block_of(10, 1)[0]]))
        self.assertEqual(result.status, client.STALLED)

    def test_blocks_missed_deadline(self):
        self.config.stall_timeout = 60
        self.config.deadline_slack = 0
        self.config.unmeasured_bitrate = 1_000_000  # 3 × 10 kB at 1 Mbit/s: 0.24 s
        self.respond(lambda link, path, data: fakes.Stall(10_000))
        result = self.call(lambda: self.client.blocks(NODE, [block_of(10, 1)[0]]))
        self.assertEqual(result.status, client.STALLED)
        self.assertIn("deadline", result.reason)

    def test_blocks_progress_reported(self):
        cid, block = block_of(100, 1)
        self.respond(lambda link, path, data: [block])
        seen = []
        self.call(lambda: self.client.blocks(NODE, [cid], on_progress=seen.append))
        self.assertEqual(seen[-1], 1.0)

    def test_no_cids(self):
        self.assertEqual(self.client.blocks(NODE, []).status, client.OK)


# Integration over real RNS, in two subprocesses

SERVER_CONFIG = """
[reticulum]
  enable_transport = No
  share_instance = No
[logging]
  loglevel = 2
[interfaces]
  [[TCP Server]]
    type = TCPServerInterface
    enabled = yes
    listen_ip = 127.0.0.1
    listen_port = {port}
"""

CLIENT_CONFIG = """
[reticulum]
  enable_transport = No
  share_instance = No
[logging]
  loglevel = 2
[interfaces]
  [[TCP Client]]
    type = TCPClientInterface
    enabled = yes
    target_host = 127.0.0.1
    target_port = {port}
"""

E2E_BLOCKS = [(60_000, seed) for seed in range(4)] + [(500, 99)]  # sizes and seeds of the served blocks
E2E_MISSING = 1234  # seed of a block nobody serves
# A compressible block the server serves too, which the client fetches in compressed /range requests (rnfs 0.2.0)
E2E_TEXT = b"".join(b"line %d of a text that compresses well, as text does\n" % i for i in range(2000))[:100_000]


def start_reticulum(configdir: str):
    """Start this process's Reticulum instance from configdir."""
    import RNS
    return RNS.Reticulum(configdir=configdir)


class E2EBackend:
    """A trivial server backend: serves E2E_BLOCKS and keeps provider records in memory."""

    def __init__(self):
        """Serve the integration test's blocks."""
        self.blocks = dict(block_of(size, seed) for size, seed in E2E_BLOCKS)
        self.blocks[Cid.for_block(RAW, E2E_TEXT)] = E2E_TEXT
        self.records: dict[bytes, list[bytes]] = {}
        self.lock = threading.Lock()

    def is_dht_server(self):
        """Whether this node is a DHT server."""
        return True

    def find(self, key):
        """Providers and nodes for key."""
        with self.lock:
            return list(self.records.get(key, [])), []

    def provide(self, publisher_id, keys, withdrawn):
        """Record publisher_id for keys."""
        with self.lock:
            for key in keys:
                self.records.setdefault(key, []).append(publisher_id)
            for key in withdrawn:
                if publisher_id in self.records.get(key, []):
                    self.records[key].remove(publisher_id)
        return True

    def served_block(self, cid):
        """The served block for cid, if any."""
        return self.blocks.get(cid)

    def remote_pin(self, identity_hash, cid, pin):
        """Accept or refuse a remote pin."""
        return False

    def is_member(self, identity_hash):
        """Whether the identity is a member."""
        return False

    def served_block_size(self, cid):
        """Not known: blocks are read to size them."""
        return None

    def feature_requests(self):
        """No feature paths."""
        return {}


def run_e2e_server(configdir: str):
    """Serve rnfs.dht until stdin closes, printing the node ID first."""
    import RNS
    from rnfs.network.announces import DhtAnnouncer
    from rnfs.network.server import RequestServer

    start_reticulum(configdir)
    identity = RNS.Identity()
    destination = RNS.Destination(identity, RNS.Destination.IN, RNS.Destination.SINGLE, "rnfs", "dht")
    config = Config(rnfs_dir=configdir, startup_announce_delay=2)
    server = RequestServer(destination, config, E2EBackend())
    DhtAnnouncer(destination, config).start()
    print("NODE", destination.hash.hex(), flush=True)
    sys.stdin.read()
    # What the server tracked: rate estimates from completed responses, and slots still held
    print("SERVER", json.dumps({"rates": server.rates.samples, "responses": len(server.pools.responses())}),
          flush=True)
    os._exit(0)


def run_e2e_client(configdir: str, server_hex: str):
    """Exercise /find, /provide and /blocks against the server and print the results as JSON."""
    import RNS
    from rnfs.network.announces import DhtAnnounceHandler, read_mode
    from rnfs.network.identity import node_id_for_identity

    start_reticulum(configdir)
    server_id = bytes.fromhex(server_hex)
    heard = []
    handler = DhtAnnounceHandler(on_server=heard.append, on_client=lambda node_id: None)
    handler.register()
    identity = RNS.Identity()
    config = Config(rnfs_dir=configdir, path_timeout=30)
    pool = links.LinkPool(identity, config)
    peers = client.PeerClient(pool, config)
    results = {"own_id": node_id_for_identity(identity).hex()}

    results["find_before"] = peers.find(server_id, KEY)
    results["provide"] = peers.provide(server_id, [KEY], []) == {KEY}
    found = peers.find(server_id, KEY)
    results["find_after"] = [[node.hex() for node in ids] for ids in found] if found else None

    wanted = [block_of(size, seed)[0] for size, seed in E2E_BLOCKS] + [block_of(10, E2E_MISSING)[0]]
    received, missing, statuses, max_bytes = {}, [], [], []
    for _ in range(10):
        if not wanted:
            break
        max_bytes.append(peers.rate_estimate(server_id))
        result = peers.blocks(server_id, wanted)
        statuses.append(result.status)
        received.update(result.blocks)
        missing += result.missing
        wanted = result.unsent
    results["statuses"] = statuses
    results["rates"] = max_bytes
    results["received"] = sorted(str(cid) for cid in received)
    results["verified"] = all(cid.verify(block) for cid, block in received.items())
    results["missing"] = [str(cid) for cid in missing]
    results["rate"] = peers.rate_estimate(server_id)
    # /range (rnfs 0.2.0): the text block in compressed ranges of 16 KiB, each checked and the whole verified
    text_cid, held, ranges = Cid.for_block(RAW, E2E_TEXT), b"", []
    for _ in range(20):
        answer = peers.block_range(server_id, text_cid, held, 16 * KIB, compress=True)
        ranges.append([answer.status, len(answer.data), answer.wire, answer.compressed])
        if answer.status != client.OK or answer.block is not None:
            break
        held += answer.data
    results["ranges"] = ranges
    results["range_block"] = answer.block == E2E_TEXT
    results["ping"] = peers.ping(server_id)
    deadline = time.monotonic() + 15
    while server_id not in heard and time.monotonic() < deadline:
        time.sleep(0.2)
    results["heard_server"] = server_id in heard
    results["mode"] = read_mode(server_id, 5, refresh=False)
    pool.close_all()
    print("RESULT", json.dumps(results), flush=True)
    os._exit(0)


@unittest.skipUnless(os.environ.get("RNFS_E2E") == "1", "set RNFS_E2E=1 to run")
class TwoNodeTest(unittest.TestCase):
    """RequestServer and PeerClient in two processes over real RNS and local TCP."""

    @staticmethod
    def stop(proc: subprocess.Popen):
        """Stop the server subprocess (it exits when its stdin closes) and close its pipes."""
        try:
            if not proc.stdin.closed:
                proc.stdin.close()
            proc.wait(timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            proc.kill()
            proc.wait()
        proc.stdout.close()

    def test_find_provide_blocks(self):
        # A server killed on Windows may not have released its files yet: don't fail the test over cleanup
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            port = free_port()
            dirs = {}
            for name, template in (("server", SERVER_CONFIG), ("client", CLIENT_CONFIG)):
                dirs[name] = os.path.join(tmp, name)
                os.makedirs(dirs[name])
                with open(os.path.join(dirs[name], "config"), "w", encoding="utf-8") as f:
                    f.write(template.format(port=port))

            script = os.path.abspath(__file__)
            env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONPATH": ROOT}
            server_proc = subprocess.Popen([sys.executable, script, "--e2e-server", dirs["server"]],
                                           stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                           encoding="utf-8", errors="replace", env=env)
            self.addCleanup(self.stop, server_proc)
            server_id = None
            server_output = []
            deadline = time.monotonic() + 30
            while server_id is None and time.monotonic() < deadline:
                line = server_proc.stdout.readline()
                if not line:
                    break
                server_output.append(line)
                if line.startswith("NODE "):
                    server_id = line.split()[1]
            self.assertIsNotNone(server_id, "".join(server_output))

            client_proc = subprocess.run([sys.executable, script, "--e2e-client", dirs["client"], server_id],
                                         capture_output=True, encoding="utf-8", errors="replace", timeout=180,
                                         env=env)
            lines = [line for line in client_proc.stdout.splitlines() if line.startswith("RESULT ")]
            self.assertTrue(lines, client_proc.stdout + client_proc.stderr)
            results = json.loads(lines[0][len("RESULT "):])
            server_proc.stdin.close()
            server_lines = [line for line in server_proc.stdout.read().splitlines() if line.startswith("SERVER ")]
            self.assertTrue(server_lines)
            server_state = json.loads(server_lines[0][len("SERVER "):])

        self.assertEqual(results["find_before"], [[], []])
        self.assertTrue(results["provide"])
        self.assertEqual(results["find_after"], [[results["own_id"]], []])
        served = sorted(str(block_of(size, seed)[0]) for size, seed in E2E_BLOCKS)
        self.assertEqual(results["received"], served)
        self.assertTrue(results["verified"])
        self.assertEqual(results["missing"], [str(block_of(10, E2E_MISSING)[0])])
        self.assertTrue(all(status == client.OK for status in results["statuses"]), results["statuses"])
        # The first response (CHUNK_SIZE budget: one 60 kB block) gives the link a rate estimate,
        # and the larger budget it allows fetches the rest in one more request
        self.assertGreater(len(results["statuses"]), 1)
        self.assertIsNone(results["rates"][0])
        self.assertIsNotNone(results["rate"])
        self.assertIsInstance(results["ping"], float)
        self.assertTrue(results["heard_server"])
        self.assertEqual(results["mode"], 1)
        # /range over real RNS: compressed ranges (single packets and Resources, both past the guards' precheck)
        # rebuild the block, verified; each range's bytes on the wire well under its bytes
        self.assertTrue(results["range_block"], results["ranges"])
        self.assertTrue(all(status == client.OK and compressed for status, _, _, compressed in results["ranges"]),
                        results["ranges"])
        self.assertLess(sum(wire for _, _, wire, _ in results["ranges"]), len(E2E_TEXT) // 3)
        # The server found its response Resources and recorded the link's rate when they completed
        self.assertGreaterEqual(server_state["rates"], 1)
        self.assertEqual(server_state["responses"], 0)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--e2e-server"]:
        run_e2e_server(sys.argv[2])
    elif sys.argv[1:2] == ["--e2e-client"]:
        run_e2e_client(sys.argv[2], sys.argv[3])
    else:
        unittest.main()
