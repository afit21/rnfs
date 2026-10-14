"""M2: upload_budget was charged a response's full size up front and refunded only when it ended, so responses a
requester never downloaded (on links that each count as their own requester) held the bucket's capacity.

Fixed: the bucket is charged as bytes are sent, from the Resource's progress. Up front a response is charged
only a reservation of about one packet (its advertisement); while it is within its grace (or once it has sent
something) the rest of it is counted as committed when admitting other responses, so responses admitted
together can't overrun the bucket; past its grace with nothing sent it commits nothing. When it concludes its
charge is made final: its size if it completed, what it sent if not. Driven with the fakes and a FakeClock.
"""
import unittest

import RNS

from rnfs import mesh
from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE, KIB, MAX_RESPONSE_SIZE
from tests.security._server_helpers import TCP_BITRATE, FakeInterface, ServerHarness
from tests.support import FakeClock
from tests.test_server import block_of

MDU = 1400  # the fake TCP links' MDU: the most a reservation is
BUDGET = 500_000  # the audit's example: a LoRa-like hourly budget
REFILL = 0.25 * BUDGET / 3600 + 2  # what the bucket refills over one 0.25 s monitor tick, and rounding


class TokenBucketReserveTest(unittest.TestCase):
    """TokenBucket.reserve and charge: admission counts committed bytes; only the reservation is taken."""

    def test_reserve_takes_only_the_reservation(self):
        bucket = mesh.TokenBucket(10_000, clock=FakeClock())
        self.assertTrue(bucket.reserve(8000, 500))
        self.assertEqual(bucket.available(), 9500)
        self.assertFalse(bucket.reserve(2000, 500, committed=7600))  # 9500 - 7600 < 2000
        self.assertEqual(bucket.available(), 9500)
        self.assertTrue(bucket.reserve(2000, 500, committed=7000))
        bucket.charge(12_000)  # bytes sent are charged, into debt if need be
        self.assertEqual(bucket.available(), -3000)

    def test_full_bucket_allows_one_oversized_response_only_when_nothing_is_committed(self):
        bucket = mesh.TokenBucket(10_000, clock=FakeClock())
        self.assertFalse(bucket.reserve(20_000, 500, committed=1))
        self.assertTrue(bucket.reserve(20_000, 500))


class UploadBudgetTest(unittest.TestCase):
    """upload_budget is charged for bytes actually sent."""

    def setUp(self):
        self.big = [block_of(CHUNK_SIZE, i) for i in range(6)]
        self.cids = [cid for cid, _ in self.big]
        self.hub = FakeInterface("TCP hub", TCP_BITRATE)
        self.h = ServerHarness(dict(self.big), Config(upload_budget=BUDGET, max_blocks_responses=8))
        self.airtime = self.h.server.airtime

    def link(self):
        """A fast link over the TCP hub that already has a rate estimate, so responses of several blocks fit."""
        link = self.h.connect(self.hub, mtu=1500, mdu=MDU)
        self.h.server.rates.record(link.link_id, 64 * KIB, TCP_BITRATE)
        return link

    def request(self, link, count=3):
        """/blocks for count blocks on link, accepted, and its Resource advertised: (response, resource)."""
        answer = self.h.blocks(link, self.cids[:count], max_bytes=MAX_RESPONSE_SIZE)
        self.assertIsNot(answer, False)
        return self.h.response(link), self.h.advertise(link)

    def spent(self) -> float:
        """Bytes taken from the bucket so far, less what it has refilled since (about REFILL a monitor tick)."""
        return BUDGET - self.airtime.bucket.available()

    def test_undownloaded_response_holds_at_most_the_reservation(self):
        response, resource = self.request(self.link())
        self.assertGreater(response.cost, 3 * CHUNK_SIZE)
        self.assertLessEqual(self.spent(), MDU)
        self.h.tick(response.grace + 1)  # its grace is over and it has sent nothing
        self.assertLessEqual(self.spent(), MDU)
        self.assertEqual(self.h.server._committed(), 0)
        self.assertFalse(resource.cancelled)  # still in flight, still holding no more than the reservation

    def test_the_m2_attack_no_longer_shrinks_the_bucket(self):
        """Two unidentified links each ask for a large response and never download it; an honest requester
        arriving once their grace is over is served in full (before the fix: cut to ~107 kB of 500 kB)."""
        held = [self.request(self.link()) for _ in range(2)]
        self.h.tick(max(response.grace for response, _ in held) + 1)
        self.assertLessEqual(self.spent(), 2 * MDU)
        honest = self.link()
        answer = self.h.blocks(honest, self.cids[3:6], max_bytes=MAX_RESPONSE_SIZE)
        self.assertIsNot(answer, False)
        self.assertEqual(len([entry for entry in answer if entry is not None]), 3)  # not cut by the held ones
        self.assertEqual(self.h.server.traffic.snapshot()["refused"].get(mesh.REFUSED_BUDGET, 0), 0)

    def test_completed_response_is_charged_its_size(self):
        response, resource = self.request(self.link())
        resource.progress = 1.0
        resource.complete()
        self.assertTrue(response.finished)
        self.assertAlmostEqual(self.spent(), response.cost, delta=1)

    def test_completed_between_monitor_passes_is_charged_its_size(self):
        response, resource = self.request(self.link())
        resource.status = RNS.Resource.COMPLETE  # no progress seen, then complete
        self.h.tick(0.25)
        self.assertTrue(response.finished)
        self.assertAlmostEqual(self.spent(), response.cost, delta=REFILL)

    def test_charged_as_progress_arrives(self):
        response, resource = self.request(self.link())
        resource.status = RNS.Resource.TRANSFERRING
        for fraction in (0.1, 0.5, 0.8):
            resource.progress = fraction
            self.h.tick(0.25)
            self.assertAlmostEqual(self.spent(), fraction * response.cost, delta=REFILL * 3)

    def test_partly_sent_then_cancelled_is_charged_what_was_sent(self):
        response, resource = self.request(self.link())
        resource.status = RNS.Resource.TRANSFERRING
        resource.progress = 0.2
        self.h.tick(0.25)
        resource.progress = 0.4
        resource.cancel()  # RNS: the requester rejected it, or it timed out
        self.assertTrue(response.finished)
        self.assertAlmostEqual(self.spent(), 0.4 * response.cost, delta=REFILL)

    def test_link_closed_mid_transfer_is_charged_what_was_sent(self):
        link = self.link()
        response, resource = self.request(link)
        resource.status = RNS.Resource.TRANSFERRING
        resource.progress = 0.25
        link.teardown()
        self.assertAlmostEqual(self.spent(), 0.25 * response.cost, delta=2)

    def test_undownloaded_and_cancelled_costs_nothing(self):
        link = self.link()
        response, resource = self.request(link)
        self.h.tick(response.grace + self.h.config.deadline_slack + 1)  # cancelled for making no progress
        self.assertTrue(resource.cancelled)
        self.assertAlmostEqual(self.spent(), 0, delta=1)

    def test_responses_admitted_together_cannot_overrun_the_bucket(self):
        """Responses within their grace commit their size, so one admitted meanwhile is cut to what is left."""
        held = [self.request(self.link(), count=3)[0] for _ in range(2)]
        third = self.link()
        answer = self.h.blocks(third, self.cids[3:6], max_bytes=MAX_RESPONSE_SIZE)
        self.assertIsNot(answer, False)
        self.assertEqual(len([entry for entry in answer if entry is not None]), 1)  # ~107 kB left: one block
        costs = sum(response.cost for response in held) + self.h.response(third).cost
        self.assertLessEqual(costs, BUDGET)

    def test_bucket_refills_as_before(self):
        response, resource = self.request(self.link())
        resource.progress = 1.0
        resource.complete()
        self.assertGreater(self.spent(), 3 * CHUNK_SIZE)
        self.h.clock.now += 1800  # half an hour: half the budget back
        self.assertAlmostEqual(self.spent(), max(0, response.cost - BUDGET / 2), delta=2)
        self.h.clock.now += 1800
        self.assertEqual(self.airtime.remaining(), BUDGET)

    def test_half_share_still_bounds_one_requester(self):
        """One identity's responses in flight cost at most half the bucket: its own undownloaded responses
        use up its share, while another requester is still served in full."""
        greedy = RNS.Identity()
        answers = []
        for _ in range(3):
            link = self.link()
            answers.append(self.h.blocks(link, self.cids[:3], max_bytes=MAX_RESPONSE_SIZE, identity=greedy))
            if answers[-1] is not False:
                self.h.advertise(link)
        self.assertIsNot(answers[0], False)
        in_flight = sum(r.cost for r in self.h.server.pools.responses() if r.identity == bytes(greedy.hash))
        self.assertLessEqual(in_flight, BUDGET // 2 + CHUNK_SIZE)
        self.assertIs(answers[-1], False)
        self.h.tick(max(r.grace for r in self.h.server.pools.responses()) + 1)
        other = self.link()
        answer = self.h.blocks(other, self.cids[3:6], max_bytes=MAX_RESPONSE_SIZE, identity=RNS.Identity())
        self.assertEqual(len([entry for entry in answer if entry is not None]), 3)

    def test_single_packet_and_untracked_responses_are_charged_in_full(self):
        small = block_of(300, 99)
        h = ServerHarness({small[0]: small[1], **dict(self.big)}, Config(upload_budget=BUDGET))
        link = h.connect(self.hub, mtu=1500, mdu=MDU)
        self.assertIsNot(h.blocks(link, [small[0]]), False)  # fits one packet: sent as we return
        packet = BUDGET - h.server.airtime.bucket.available()
        self.assertGreater(packet, 300)
        blind = h.connect(self.hub, mtu=1500, mdu=MDU)
        h.server.watched.discard(blind.link_id)  # RNS internals unavailable: progress can't be followed
        self.assertIsNot(h.blocks(blind, [self.cids[0]]), False)
        response = h.response(blind)
        self.assertTrue(response.untracked)
        self.assertAlmostEqual(BUDGET - h.server.airtime.bucket.available(), packet + response.cost, delta=1)


if __name__ == "__main__":
    unittest.main()
