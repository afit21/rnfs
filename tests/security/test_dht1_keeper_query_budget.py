"""DHT-1: a stalled lookup left keeper /name queries 1 s, censoring offline owners' names.

Sybil server IDs ground close to a name's keeper key answer /find slowly, each time with fresh Sybil IDs closer to the
key, so the beta closest nodes never all answer. With fewer than 3 × alpha keepers the provider search never had
enough candidates to stop, so it used the whole lookup_timeout, and the /name queries then got
max(1 s, time left): a keeper whose link takes seconds to set up was asked, but its answer was dropped. The fix gives
/name its own budget, stops the search once candidates exist and lookup_timeout / 5 has passed (resuming it if their
answers don't settle the name), and bounds each lookup run (a query cap and a no-progress stop).

Offline nodes over GrindingNet (a SybilNet, tests/security/_dhtr2b_helpers.py): the name is at sequence 2, kept only
by D. Resolver B (alpha 3) knows two grinding Sybils, and holds D's provider record unless a test says otherwise.
"""
import os
import threading
import time
import unittest
from unittest import mock

from rnfs.config import Config
from rnfs.constants import K
from rnfs.dht import lookup
from rnfs.features import keepers
from rnfs.node import RnfsNode
from tests.security import _dhtr2b_helpers
from tests.security._dhtr2b_helpers import B_ID, D_ID, KeeperScenario, SybilNet
from tests.test_names import FakeNetwork

S_ID = bytes([0x55] * 16)  # an honest DHT server, slow to answer
X_ID = bytes([0x66] * 16)  # a keeper that has left: its /name request fails
SYBIL_DELAY = 0.3  # seconds each grinding Sybil takes to answer /find
KEEPER_DELAY = 3.0  # seconds keeper D takes to answer /name (link setup over a slow path)


class GrindingNet(SybilNet):
    """SybilNet with grinding Sybils: each answers /find after SYBIL_DELAY with K fresh Sybil IDs and no providers,
    every second answer's a bit nearer the key (about 250 answers before they run out of bits). Nodes in find_delay
    / name_delay answer late."""

    def __init__(self):
        """No grinding Sybils yet."""
        super().__init__()
        self.grinders: set[bytes] = set()
        self.closeness = 1 << 126  # the distance of the next IDs handed out, halved by every second answer
        self.answers = 0
        self.find_delay: dict[bytes, float] = {}
        self.name_delay: dict[bytes, float] = {}
        self.name_times: dict[bytes, float] = {}  # when each node was first asked /name (time.monotonic)
        self.grind_lock = threading.Lock()

    def find(self, node_id, key):
        """A grinding Sybil's /find answer, or the node's (late if find_delay says so)."""
        if node_id in self.grinders:
            with self.grind_lock:
                self.find_requests.append(node_id)
            time.sleep(SYBIL_DELAY)
            with self.grind_lock:
                self.answers += 1
                if self.answers % 2 == 0:
                    self.closeness = max(1 << 16, self.closeness // 2)
                fresh = self.closeness + (self.answers % 2) * K  # the two answers at one distance differ
                closer = [(int.from_bytes(key, "big") ^ (fresh + i)).to_bytes(16, "big") for i in range(K)]
                self.grinders.update(closer)
            return [], closer
        time.sleep(self.find_delay.get(node_id, 0))
        return super().find(node_id, key)

    def name_record(self, node_id, name):
        """The node's /name answer, late if name_delay says so."""
        self.name_times.setdefault(node_id, time.monotonic())
        time.sleep(self.name_delay.get(node_id, 0))
        return super().name_record(node_id, name)


class KeeperQueryBudgetTest(KeeperScenario):
    """The name at sequence 2 kept only by D; resolver B is made by each test."""

    def setUp(self):
        """The scenario over a GrindingNet."""
        with mock.patch.object(_dhtr2b_helpers, "SybilNet", GrindingNet):
            super().setUp()

    def make_node(self, label: str, node_id: bytes, slow: bool = False, lookup_timeout: float = 5) -> RnfsNode:
        """An offline node with its own directory and the given lookup_timeout."""
        config = Config(rnfs_dir=os.path.join(self.dir, label), slow="no", lookup_timeout=lookup_timeout,
                        path_timeout=0.2)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork(), slow=slow)
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def resolver(self, lookup_timeout: float, keeper_recorded: bool = True) -> RnfsNode:
        """B, knowing two grinding Sybils near the key (and holding D's provider record if keeper_recorded)."""
        b = self.make_node("b", B_ID, lookup_timeout=lookup_timeout)
        seeds = [self.near_key(1 << 125), self.near_key((1 << 125) + 1)]
        self.net.grinders.update(seeds)
        for seed in seeds:
            b.routing.heard(seed)
        if keeper_recorded:
            self.assertTrue(b.records.store(self.key, D_ID))
        self.net.name_delay[D_ID] = KEEPER_DELAY
        return b

    def test_name_wait_at_the_defaults(self):
        """path_timeout + link setup over 5 hops + twice RNS's request timeout: 87.5 s with the default settings."""
        self.assertEqual(keepers.name_wait(Config()), 87.5)

    def test_keeper_answer_survives_a_stalled_lookup(self):
        b = self.resolver(lookup_timeout=10)
        started = time.monotonic()
        records = b.feature("keepers").ask_keepers(self.name)
        took = time.monotonic() - started
        asked = self.net.name_times.get(D_ID)
        self.assertIsNotNone(asked, "keeper D was never asked")
        # DHT-1: fails until /name has a budget of its own: D was asked with 1 s left, and its answer dropped
        self.assertEqual(self.best(records), 2, f"D asked at {asked - started:.1f}s, the query took {took:.1f}s")
        # The search stopped at lookup_timeout / 5 with D in hand; D was asked then, not when the lookup gave up
        self.assertLess(asked - started, 10 / 5 + 1.5)
        # Its answers didn't settle the name (D isn't established), so the search resumed for the rest of the budget
        self.assertLess(took, 10 + KEEPER_DELAY)

    def test_stalled_lookup_is_bounded(self):
        """With no keeper in sight the search can't stop early: the lookup's query cap ends it, long before
        lookup_timeout, having sent at most MAX_QUERIES /find requests."""
        b = self.resolver(lookup_timeout=40, keeper_recorded=False)
        started = time.monotonic()
        records = b.feature("keepers").ask_keepers(self.name)
        took = time.monotonic() - started
        self.assertEqual(records, [])
        # DHT-1: fails until a lookup run is bounded (it ran to lookup_timeout)
        self.assertLess(took, 16, f"the lookup ran {took:.1f}s with {len(self.net.find_requests)} /find requests")
        self.assertLessEqual(len(self.net.find_requests), lookup.MAX_QUERIES)

    def test_resumed_search_finds_a_later_keeper(self):
        """B holds a record of X, which has left; slow server S lists D, but answers after the search's soft budget.
        X is asked when the budget runs out; its answer doesn't settle the name, so the search resumes and finds D."""
        s = self.make_node("s", S_ID)
        b = self.make_node("b", B_ID, lookup_timeout=10)
        b.routing.heard(S_ID)
        self.net.find_delay[S_ID] = 4.0
        self.assertTrue(s.records.store(self.key, D_ID))
        self.assertTrue(b.records.store(self.key, X_ID))
        started = time.monotonic()
        records = b.feature("keepers").ask_keepers(self.name)
        self.assertEqual(self.best(records), 2)
        self.assertIn(X_ID, self.net.name_times)
        # DHT-1: fails until the search stops at lookup_timeout / 5 once candidates exist (not once S answered)
        self.assertLess(self.net.name_times[X_ID] - started, 10 / 5 + 1.5)
        self.assertGreater(self.net.name_times[D_ID], self.net.name_times[X_ID])


if __name__ == "__main__":
    unittest.main()
