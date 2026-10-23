"""AUD-H1 (0.2.1 audit, a 0.2.1 regression): paced one-packet answers held their links' /blocks slots outside the pools.

The attack: 0.2.1 lets an answer that surely fits one packet (fits_packet) skip the response pools, but such an answer
still waits out upload pacing (RequestServer._pace, up to MAX_PACING_WAIT) holding its link's /blocks slot.
IncomingLinks._choose_victim never closes a link with a /blocks request in progress, so an attacker with
max_incoming_links links, each given a slow rate estimate (the default cap is then half of it) and looping on tiny
blocks paced about 5 s apart, kept every link busy at about 8 KB/s, and the node refused every new incoming link,
honest ones included. 0.2.0 reserved a pool slot before pacing, so at most a pool's worth of links (2 slow, 16 fast)
could be in that wait at once.

The test rebuilds it on ServerHarness with a smaller max_incoming_links: every attacker link gets a slow rate estimate
and a first one-packet answer, then asks again. Each request that enters the pacing wait sends the next link's from
inside it (so every wait overlaps, without threads), and once all have been sent a newcomer connects. A second test
closes a link while its one-packet answer is being built.

The fix (RequestServer._serve): an answer that must wait for pacing takes a pool slot (slotted = wait > 0 or not
fits_packet(mdu)), so links in a pacing wait are bounded by the pools again and the others stay closable; and a
response outside the pools is tracked per link (RequestServer.unslotted) and finished when its link closes
(_link_closed), so nothing is sent for a closed link.
"""
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import KIB
from rnfs.content.cid import RAW, Cid
from rnfs.network import server
from tests.security._server_helpers import TCP_BITRATE, FakeInterface, ServerHarness

BLOCK = b"x" * 330  # one packet at the default MTU
CID = Cid.for_block(RAW, BLOCK)
ATTACKERS = 8


class PacedAnswerSlotTest(unittest.TestCase):
    def setUp(self):
        RNS.loglevel = RNS.LOG_CRITICAL
        self.h = ServerHarness({CID: BLOCK}, Config(max_incoming_links=ATTACKERS))
        backend = self.h.backend
        backend.served_block_size = lambda cid: len(backend.blocks[cid]) if cid in backend.blocks else None
        self.tcp = FakeInterface("tcp", TCP_BITRATE)

    def test_paced_one_packet_answers_leave_room_for_a_newcomer(self):
        links = []
        for n in range(ATTACKERS):
            link = self.h.connect(self.tcp)
            # The link once downloaded a response slowly: ~1.1 kbit/s, so slow, and paced at half that
            self.h.server.rates.record(link.link_id, 64 * KIB, 1100.0)
            self.assertEqual(self.h.blocks(link, [CID], request_id=bytes([n]) * 16), [BLOCK])
            links.append(link)
        self.assertGreater(self.h.server.pacer.wait_time(links[0].link_id), 1)

        pending = list(links)
        seen = {}

        def ask_next():
            """Send the next attacker link's request; once every one is sent, while their waits all overlap, look."""
            while pending:
                link = pending.pop(0)
                self.h.blocks(link, [CID], request_id=b"\x80" + bytes(15))
            if not seen:
                seen["busy"] = sum(1 for link in links if self.h.server.incoming.in_progress(link.link_id)[1])
                seen["newcomer"] = self.h.connect(self.tcp)

        def pace(response, wait):
            """The pacing wait: the attack goes on from inside it."""
            ask_next()
        self.h.server._pace = pace
        ask_next()

        self.assertLessEqual(seen["busy"], self.h.server.pools.capacity[server.SLOW_POOL],
                             "more links held a /blocks request in a pacing wait than the slow pool has slots")
        self.assertEqual(seen["newcomer"].status, RNS.Link.ACTIVE, "a newcomer was refused: every link was busy")

    def test_an_unslotted_answer_is_finished_when_its_link_closes(self):
        link = self.h.connect(self.tcp)
        served = self.h.backend.served_block

        def closing(cid):
            """The link closes while its answer is being built."""
            link.teardown()
            return served(cid)
        self.h.backend.served_block = closing
        self.assertIs(self.h.blocks(link, [CID]), False)
        self.assertEqual(self.h.server.unslotted, {})
        self.assertEqual(self.h.server.pacer.wait_time(link.link_id), 0)


if __name__ == "__main__":
    unittest.main()
