"""AUD-H4 (0.2.1 audit): a node asked first (a probe) that drips its answer held off the provider search, and later
fetches failed without searching.

The attack: a remembered provider of a root, or a node with an open link, answers the probe for the root's block
slowly, keeping the response moving so it never stalls. Its deadline at unmeasured_bitrate is about 8.8 hours. The
fetch started its lookup only once every probe had answered, and a probe was never duplicated (NET-4 covered only
providers), so the first fetch didn't search. Each later fetch's probe to that node waited, with no deadline, for its
link's one /blocks slot, which the drip held, so it failed "made no progress" without ever searching. A remembered
node whose probe ended cancelled stayed remembered, so the next fetch of the root asked it first again.

The tests rebuild it over tests.test_fetcher's FakeClient, with deadline_slack at 0.3 s. P1 holds an open link and
drips its answer for DRIP seconds; P2, which a lookup finds, holds the content. SlotClient adds each node's one /blocks
slot, which a probe waits for at most its slot_wait, as LinkPool does; a client test checks PeerClient's side.

The fix (rnfs/fetcher.py: Fetch._assign, _discover, _duplicate, _handle; rnfs/network/client.py and links.py):
- once only probes are in flight and one has run past deadline_slack (or 1.5 times its block's transfer time at the
  link's rate estimate or next-hop bitrate, if longer), the lookup starts while the probe goes on;
- a probe is duplicated like any request (end-game duplication), so a provider the lookup found sends the block;
- a probe waits for its link's /blocks slot no longer than that, and comes back busy ("no free slot"), a miss;
- a remembered node whose probe was cancelled, or ran that long without sending anything, is forgotten.
"""
import os
import threading
import time
import unittest
from collections import defaultdict
from unittest import mock

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.content.cid import RAW, Cid
from rnfs.errors import NotFound
from rnfs.network import client, links
from rnfs.network.client import BUSY, OK, BlocksResult
from tests.fakes.client import FakeClient
from tests.support import wait_for
from tests.test_fetcher import P1, P2, FetcherTest
import tests.test_links as fakes  # a module, so its test cases don't run twice

DRIP = 8.0  # seconds P1 keeps its answer moving before cancelling it
SLACK = 0.3  # deadline_slack in these tests


class SlotClient(FakeClient):
    """FakeClient with each node's one /blocks slot, as on a LinkPool link: a request waits for it, a probe at most its
    slot_wait (then it comes back busy, unsent). Nodes in drips hold their slot that many seconds, reporting progress,
    then cancel the answer."""

    def __init__(self):
        """No drips yet."""
        super().__init__()
        self.slots: dict[bytes, threading.Lock] = defaultdict(threading.Lock)
        self.drips: dict[bytes, float] = {}
        self.slot_waits: list[tuple[bytes, float | None]] = []  # (node, slot_wait) of every /blocks call
        self.stop = threading.Event()

    def blocks(self, node_id, cids, on_progress=None, slot_wait=None):
        """FakeClient.blocks, once node_id's slot is free; a drip if drips says so."""
        with self.lock:
            self.slot_waits.append((node_id, slot_wait))
        slot = self.slots[node_id]
        if not slot.acquire(timeout=-1 if slot_wait is None else slot_wait):
            return BlocksResult(BUSY, unsent=list(cids), reason="no free slot")
        try:
            if node_id in self.drips:
                with self.lock:
                    self.requests.append((node_id, list(cids)))
                end = time.monotonic() + self.drips[node_id]
                while time.monotonic() < end and not self.stop.is_set():
                    if on_progress is not None:
                        on_progress(0.01)  # always moving: it never stalls
                    time.sleep(0.05)
                return BlocksResult(BUSY, unsent=list(cids), reason="cancelled")
            return super().blocks(node_id, cids, on_progress)
        finally:
            slot.release()


class DrippingProbeTest(FetcherTest):
    """P1 (an open link, or remembered) drips its answer to the probe; a lookup would find P2, which has the file."""

    def setUp(self):
        """A SlotClient, deadline_slack SLACK, and a fetcher that notes when each lookup starts."""
        super().setUp()
        self.client = SlotClient()
        self.addCleanup(self.client.stop.set)
        self.config.deadline_slack = SLACK
        # A fetch whose probe waits for a slot hears nothing: it fails "made no progress" after 3 s rather than hang
        self.config.lookup_timeout, self.config.stall_timeout = 2, 1
        self.fetcher = self.make_fetcher()
        self.searched: dict[bytes, float] = {}  # key -> when its first lookup started
        make_lookup = self.fetcher.make_lookup

        def noted(key):
            """make_lookup, noted."""
            self.searched.setdefault(key, time.monotonic())
            return make_lookup(key)
        self.fetcher.make_lookup = noted
        self.client.drips[P1] = DRIP

    def timed_fetch(self, root: Cid) -> float:
        """Fetch root's whole DAG; the seconds it took."""
        started = time.monotonic()
        self.fetch_all(root)
        return time.monotonic() - started

    def test_the_search_starts_while_a_probe_drips(self):
        built = self.file(os.urandom(4 * CHUNK_SIZE))
        self.hold(P2)
        self.client.links = [P1]
        self.runs.extend([[P2]] * 3)
        started = time.monotonic()
        took = self.timed_fetch(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        # AUD-H4: fails until the lookup starts once the probe is past deadline_slack (it started when the drip ended)
        self.assertLess(self.searched[built.cid.key] - started, DRIP / 2)
        # ... and the probe's block is asked of the provider found meanwhile (it waited for the drip)
        self.assertLess(took, DRIP - 1, f"the fetch took {took:.1f}s, as long as the drip")
        self.assertEqual(self.client.asked(P2)[0], built.cid)

    def test_a_probe_waits_for_its_slot_no_longer_than_deadline_slack(self):
        first, second = self.file(os.urandom(2 * CHUNK_SIZE)), self.file(os.urandom(2 * CHUNK_SIZE))
        self.hold(P2)
        self.client.links = [P1]
        self.runs.extend([[P2]] * 6)
        errors = []

        def fetch_first():
            """The fetch whose probe drips, holding P1's slot."""
            try:
                self.fetch_all(first.cid)
            except Exception as e:  # reported by the test
                errors.append(e)
        thread = threading.Thread(target=fetch_first, daemon=True)
        thread.start()
        self.addCleanup(thread.join, DRIP + 5)
        wait_for(self.client.slots[P1].locked, what="the first fetch's probe to start dripping")
        # AUD-H4: fails until a probe's slot wait is bounded: this one waited for the slot, never searched, and failed
        # NotFound at the no-progress deadline (or got the slot only once the drip ended)
        took = self.timed_fetch(second.cid)
        self.assertIn(second.cid.key, self.searched)
        self.assertLess(took, DRIP - 1, f"the second fetch took {took:.1f}s, as long as the drip")
        thread.join(DRIP + 5)
        self.assertEqual(errors, [])
        self.assertTrue(self.storage.is_complete(first.cid))
        # Only probes wait for their slot so long at most: a provider's request waits as long as it takes
        self.assertIn((P1, SLACK), self.client.slot_waits)
        self.assertTrue(all(wait is None for node, wait in self.client.slot_waits if node == P2))

    def test_a_remembered_node_whose_probe_drips_is_forgotten(self):
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        self.hold(P2)
        self.fetcher.remember(built.cid, P1)
        self.client.drips[P1] = 1.0  # past deadline_slack, then cancelled
        self.runs.extend([[P2]] * 3)
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        # AUD-H4: fails until a remembered node whose probe was cancelled, or ran long, is forgotten
        self.assertNotIn(P1, self.fetcher.remembered(built.cid))
        self.assertIn(P2, self.fetcher.remembered(built.cid))

    def test_a_remembered_node_whose_probe_ran_long_is_forgotten(self):
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        self.fetcher.remember(built.cid, P1)
        del self.client.drips[P1]

        def slow_refusal(cids):
            """A refusal, after keeping the probe waiting past deadline_slack."""
            time.sleep(4 * SLACK)
            return BlocksResult(BUSY, unsent=list(cids), reason="busy")
        self.client.behaviour[P1] = slow_refusal
        self.runs.extend([[]] * 3)  # nobody else provides it: the fetch waits for P1's answer, then fails
        with self.assertRaises(NotFound):
            self.fetch_all(built.cid)
        # AUD-H4: fails until a remembered node whose probe ran past deadline_slack without sending anything is
        # forgotten (a refusal kept it)
        self.assertNotIn(P1, self.fetcher.remembered(built.cid))

    def test_a_remembered_node_busy_at_once_or_whose_slot_was_taken_is_kept(self):
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        self.hold(P2)
        self.fetcher.remember(built.cid, P1)
        del self.client.drips[P1]
        self.client.behaviour[P1] = lambda cids: BlocksResult(BUSY, unsent=list(cids), reason="busy")
        self.runs.extend([[P2]] * 3)
        self.fetch_all(built.cid)
        self.assertIn(P1, self.fetcher.remembered(built.cid))  # it may still have it
        # Its slot held by another request of this node: nothing was asked of it
        other = self.file(os.urandom(2 * CHUNK_SIZE))
        self.hold(P2)
        self.fetcher.remember(other.cid, P1)
        with self.client.slots[P1]:
            self.fetch_all(other.cid)
        self.assertIn(P1, self.fetcher.remembered(other.cid))


class PeerClientProbeSlotTest(unittest.TestCase):
    """PeerClient over LinkPool (tests.test_links' fake RNS): a probe gives up waiting for its link's /blocks slot."""

    def setUp(self):
        """A client over a LinkPool whose links are FakeLinks, and the link to P1 open, its /blocks slot taken."""
        patcher = mock.patch.object(links, "RNS", fakes.fake_rns())
        patcher.start()
        self.addCleanup(patcher.stop)
        fakes.FakeLink.created = []
        fakes.FakeLink.establish_delay = 0.0
        fakes.FakeLink.responder = staticmethod(lambda link, path, data: [b"x"])
        self.pool = links.LinkPool(object(), Config(stall_timeout=0.5))
        self.addCleanup(self.pool.close_all)
        self.client = client.PeerClient(self.pool, self.pool.config)
        self.peer = fakes.run(self, lambda: self.pool.get(P1))
        self.assertTrue(self.peer.acquire(True))  # another fetch's response holds it
        self.cid = Cid.for_block(RAW, b"x")

    def test_blocks_and_range_give_up_on_the_slot(self):
        result = fakes.run(self, lambda: self.client.blocks(P1, [self.cid], slot_wait=0.2))
        self.assertEqual((result.status, result.reason, result.unsent), (BUSY, "no free slot", [self.cid]))
        ranged = fakes.run(self, lambda: self.client.block_range(P1, self.cid, max_bytes=1, slot_wait=0.2))
        self.assertEqual((ranged.status, ranged.reason), (BUSY, "no free slot"))
        self.assertEqual(self.peer.link.requests, [])  # neither was sent
        self.assertTrue(self.peer.is_active())  # nor was the link closed
        self.assertEqual((self.client.traffic.peer(P1) or {}).get("requests", 0), 0)  # not counted as P1's refusal
        self.peer.release(True)
        result = fakes.run(self, lambda: self.client.blocks(P1, [self.cid], slot_wait=0.2))
        self.assertEqual((result.status, list(result.blocks)), (OK, [self.cid]))


if __name__ == "__main__":
    unittest.main()
