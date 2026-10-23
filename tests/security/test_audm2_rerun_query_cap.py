"""AUD-M2 (0.2.1 audit): a fetch's searches again reset the /find cap, so one fetch sent 66,426 /find in 15 s against a
cap of 80 (a DHT-1 bypass).

The attack: Sybil DHT servers near a key answer every /find listing K made-up provider IDs, fresh each time. When
every provider of a fetch has failed, the fetch runs its lookup again (Fetch._rerun_lookup) if the last run found any
provider, bounded only by lookup_timeout, and each run of a Lookup had max_queries (MAX_QUERIES, 80) of its own. The
made-up IDs are unknown to RNS, so the fetch skipped them (the unknown-ID budget was spent on the first three), but
LookupDone.found counted them, so the fetch searched again at once, again and again until lookup_timeout.

The tests rebuild it with offline nodes over a SybilNet (tests/security/_dhtr2b_helpers.py, as the audit's repro did),
and with tests.test_fetcher's fakes for the fetcher's side.

The fix: a Lookup's runs together send at most max_queries /find (rnfs/dht/lookup.py, Lookup._iterate: a run sends
what the earlier ones left), as specification section 6 says a search does however its lookups end; and a run counts
as having found providers only if the fetch took one up (rnfs/fetcher.py, Fetch._add_provider and _handle_event), so a
run that only names providers the fetch can't use isn't followed by another.
"""
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE, K
from rnfs.dht import lookup
from rnfs.errors import NotFound
from rnfs.node import RnfsNode
from tests.security._dhtr2b_helpers import B_ID, C_ID, D_ID, SybilNet
from tests.test_fetcher import P1, P2, P3, FakeLookup, FetcherTest
from tests.test_names import FakeNetwork

LOOKUP_TIMEOUT = 5.0


class FreshProvidersNet(SybilNet):
    """SybilNet whose Sybils list K made-up provider IDs, fresh for every /find (unknown to RNS), and count every
    /find and /blocks."""

    def __init__(self):
        """Nothing counted yet."""
        super().__init__()
        self.count_lock = threading.Lock()
        self.finds = 0

    def find(self, node_id, key):
        """A Sybil's answer: fresh made-up providers, and the Sybils as nodes."""
        with self.count_lock:
            self.finds += 1
        if node_id in self.sybils:
            return [os.urandom(16) for _ in range(K)], list(self.sybils[:K])
        return super().find(node_id, key)


class RerunQueryCapTest(unittest.TestCase):
    """B fetches a CID nobody provides; the K Sybils near every key are in the routing tables of B, C and D."""

    def setUp(self):
        """Three offline nodes over a FreshProvidersNet, and K Sybils they know."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        self.tmp = self.enterContext(tempfile.TemporaryDirectory())
        self.net = FreshProvidersNet()
        self.b, c, d = (self.make_node(label, node_id) for label, node_id in (("b", B_ID), ("c", C_ID), ("d", D_ID)))
        for node, others in ((self.b, (C_ID, D_ID)), (c, (B_ID, D_ID)), (d, (B_ID, C_ID))):
            for other in others:
                node.routing.heard(other)
        self.net.sybils = [bytes([0x77, i]) + os.urandom(14) for i in range(K)]
        for sybil in self.net.sybils:
            for node in (self.b, c, d):
                node.routing.heard(sybil)
        # As in real RNS, the made-up providers are unknown: only alpha of them get a path request per search
        self.b._is_known = lambda node_id: node_id in self.net.nodes or node_id in self.net.sybils

    def make_node(self, label: str, node_id: bytes) -> RnfsNode:
        """An offline node with its own directory."""
        config = Config(rnfs_dir=os.path.join(self.tmp, label), slow="no", lookup_timeout=LOOKUP_TIMEOUT,
                        path_timeout=0.2)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def test_one_fetch_sends_at_most_max_queries_find(self):
        cid = "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy"
        started = time.monotonic()
        with self.assertRaises(NotFound):
            self.b.stat(cid)
        took = time.monotonic() - started
        # AUD-M2: fails until a search's runs share one /find cap (tens of thousands were sent)
        self.assertLessEqual(self.net.finds, lookup.MAX_QUERIES, f"{self.net.finds} /find in {took:.1f}s")


class AcceptedProvidersTest(FetcherTest):
    """A run of the fetch's lookup that names only providers the fetch skips isn't followed by another."""

    def test_skipped_providers_dont_count_as_found(self):
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        self.hold(P3)
        unknown = {P2}  # identities the fetch can't contact (the unknown-ID budget is spent)
        patcher = mock.patch.object(FakeLookup, "allow_unknown", lambda lookup, node_id: node_id not in unknown)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.runs.extend([[P1], [P2], [P3]])  # P1 holds nothing; the second run names only P2, which is skipped
        with self.assertRaises(NotFound):
            self.fetch_all(built.cid)
        # AUD-M2: fails until LookupDone counts only providers the fetch took up (it searched a third time)
        self.assertEqual(self.lookups[0].count, 2)

    def test_a_run_that_found_a_usable_provider_is_still_followed_by_another(self):
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        self.hold(P2)
        self.runs.extend([[P1], [P2]])  # P1 fails, the second run finds P2
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual(self.lookups[0].count, 2)


class LookupRunsShareTheCapTest(unittest.TestCase):
    """A Lookup run again sends what its earlier runs left of max_queries."""

    def test_runs_share_max_queries(self):
        key = bytes(16)
        sent = []

        def grinding(node_id, asked):
            """Each answer names K nodes nearer the key than any before, and a fresh provider."""
            sent.append(node_id)
            nearer = len(sent) * K
            return [os.urandom(16)], [(10**30 // nearer + i).to_bytes(16, "big") for i in range(K)]
        start = [(1 << 127 | i).to_bytes(16, "big") for i in range(K)]
        search = lookup.Lookup(key, start, grinding, bytes([0xFF]) * 16, timeout=30, max_queries=12,
                               poll_interval=0.01)
        for _ in range(5):
            search.find_providers(lambda provider: None)
        # AUD-M2: fails until the runs share the cap (each run sent its own 12)
        self.assertLessEqual(len(sent), 12)
        self.assertEqual(search.sent, len(sent))


if __name__ == "__main__":
    unittest.main()
