"""M2 follow-up: a response in flight counted its whole unsent rest as committed upload_budget once it had sent
anything, so a few responses crawling along (one part, then next to nothing) could hold admission for every other
requester while spending nothing.

Now, after its grace, a response commits at most what it sends at its progress rate before its deadline, and
nothing once it is below its progress floor. Driven with the fakes and a FakeClock.
"""
import unittest

import RNS

from rnfs import mesh
from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE, KIB, MAX_RESPONSE_SIZE
from tests.security._server_helpers import TCP_BITRATE, FakeInterface, ServerHarness
from tests.test_server import block_of

MDU = 1400
BUDGET = 500_000  # a LoRa-like hourly budget, as in test_m2_upload_budget


class CommitmentBoundTest(unittest.TestCase):

    def setUp(self):
        self.big = [block_of(CHUNK_SIZE, i) for i in range(6)]
        self.cids = [cid for cid, _ in self.big]
        self.hub = FakeInterface("TCP hub", TCP_BITRATE)
        # Plenty of free slots, so a newcomer displaces nothing: only the budget decides
        self.h = ServerHarness(dict(self.big), Config(upload_budget=BUDGET, max_blocks_responses=40))

    def request(self, count=3):
        """/blocks for count blocks on a new directly connected TCP link with a rate estimate (1 Mbit/s), accepted
        and advertised, its Resource transferring: (response, resource)."""
        link = self.h.connect(self.hub, mtu=1500, mdu=MDU)
        self.h.server.rates.record(link.link_id, 64 * KIB, 1_000_000)
        self.assertIsNot(self.h.blocks(link, self.cids[:count], max_bytes=MAX_RESPONSE_SIZE), False)
        resource = self.h.advertise(link)
        resource.status = RNS.Resource.TRANSFERRING
        return self.h.response(link), resource

    def crawl(self, *pairs):
        """Each response sends one part, then nothing, until well past its grace (below its floor)."""
        for response, resource in pairs:
            resource.progress = MDU / response.size
        self.h.tick(0.25)
        self.h.tick(max(response.grace for response, _ in pairs) + 2)
        for response, _ in pairs:
            self.assertFalse(response.finished)
            self.assertFalse(response.moving(self.h.clock.now))

    def test_crawling_response_below_its_floor_commits_nothing(self):
        response, resource = self.request()
        self.crawl((response, resource))
        # M2: fails until a response below its floor stops counting its unsent rest as committed
        self.assertEqual(self.h.server._committed(), 0, "a crawling response holds admission")

    def test_moving_response_commits_at_most_what_it_sends_by_its_deadline(self):
        response, resource = self.request()
        rate = 2000  # bytes/s: above the 1.25 kB/s floor, far below what it needs
        for _ in range(int(response.grace) + 6):
            resource.progress = min(1.0, resource.progress + rate / response.size)
            self.h.tick(1)
        now = self.h.clock.now
        self.assertTrue(response.moving(now))
        rest = response.cost - response.charged
        bound = response.progress_rate(now) * (response.deadline - now)
        self.assertLess(bound, rest / 2)
        # M2: fails until the commitment is bounded by what the response's rate sends before its deadline
        self.assertLessEqual(self.h.server._committed(), bound + 1)

    def test_crawling_responses_do_not_cut_an_honest_response(self):
        held = [self.request() for _ in range(2)]
        self.crawl(*held)
        honest = self.h.connect(self.hub, mtu=1500, mdu=MDU)
        self.h.server.rates.record(honest.link_id, 64 * KIB, 1_000_000)
        answer = self.h.blocks(honest, self.cids[3:6], max_bytes=MAX_RESPONSE_SIZE)
        self.assertIsNot(answer, False)
        # M2: fails until crawling responses stop committing (before: cut to one block of three)
        self.assertEqual(len([entry for entry in answer if entry is not None]), 3)
        self.assertEqual(self.h.server.traffic.snapshot()["refused"].get(mesh.REFUSED_BUDGET, 0), 0)

    def test_responses_within_their_grace_still_commit_their_rest(self):
        """The counterpart: responses admitted together can't overrun the bucket."""
        response, _ = self.request()
        self.assertEqual(self.h.server._committed(), response.cost - response.charged)


if __name__ == "__main__":
    unittest.main()
