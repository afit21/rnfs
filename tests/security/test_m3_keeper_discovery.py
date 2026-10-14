"""M3: identities that stored provider records for a name's keeper key before the real keepers did take every slot
a keeper-fallback resolve asks, so the real keepers are never reached.

Offline nodes over KeeperNet: owner A publishes a name and advertises it, B resolves it, C keeps it and advertises
that, D is one more DHT server. Squatter identities (never online, so they answer nothing) store provider records for
name_key(N) after A's but before C's, either on B itself or on the servers B's lookup queries (B then holds none).
A goes away and B resolves: the path request hears nothing, so B falls back to keepers. The tests check the outcome
only: B gets C's record, and C was asked. With alpha 3 two squatters were enough; on a slow node (alpha 1) one was.
Further tests pin down the fix: local records don't skip the search, keepers are ranked by routing-table standing
(a departed owner after a live keeper), and further batches are asked while answers are older than the sequence
already known or only one keeper answered, within a bound (two /name requests on a slow node).
"""
import io
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.dht.routing import RoutingTable
from rnfs.errors import NotFound
from rnfs.features.keepers import name_key
from rnfs.node import RnfsNode
from tests.support import FakeClock
from tests.test_names import FakeNetwork
from tests.test_names_keep import KeeperNet

A_ID, B_ID, C_ID, D_ID = (bytes([x] * 16) for x in (0xA, 0xB, 0xC, 0xD))
SQUATTERS = [bytes([0xE0 + i] * 16) for i in range(3)]  # identities with no node behind them


class KeeperDiscoveryTest(unittest.TestCase):
    """Owner A, resolver B, keeper C and server D as offline nodes that all know each other."""

    def setUp(self):
        """A temporary directory and the network; nodes are made by build()."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name
        self.net = KeeperNet()

    def make_node(self, label: str, node_id: bytes, slow: bool = False) -> RnfsNode:
        """An offline node with its own directory."""
        config = Config(rnfs_dir=os.path.join(self.dir, label), slow="no", lookup_timeout=5, path_timeout=0.2)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork(), slow=slow)
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def build(self, slow_resolver: bool = False):
        """The four nodes, and A's name published and advertised (A's record is the oldest for its key)."""
        self.a = self.make_node("a", A_ID)
        self.b = self.make_node("b", B_ID, slow=slow_resolver)
        self.c = self.make_node("c", C_ID)
        self.d = self.make_node("d", D_ID)
        ids = (A_ID, B_ID, C_ID, D_ID)
        for node, node_id in zip((self.a, self.b, self.c, self.d), ids):
            for other in ids:
                if other != node_id:
                    node.routing.heard(other)
        self.name = bytes.fromhex(self.a.name_create("site")["name"])
        self.key = name_key(self.name)
        self.cid = self.a.add_stream(io.BytesIO(b"v1"))["cid"]
        self.a.name_publish("site", self.cid)
        self.net.sender = A_ID
        self.a.publisher.publish_due()

    def squat(self, nodes, squatters):
        """Each squatter stores a provider record for the name's keeper key on each of nodes, as /provide would."""
        for node in nodes:
            for squatter in squatters:
                self.assertTrue(node.records.store(self.key, squatter))

    def keep_on_c(self):
        """C keeps the name (fetching its record from A) and advertises it."""
        keeper = self.c.feature("keepers")
        keeper.keep(self.name)
        keeper.run_pending()
        self.assertEqual(str(keeper.store.get(self.name).cid), self.cid)
        self.net.sender = C_ID
        self.c.publisher.publish_due()

    def forget_owner_record(self):
        """A's provider records for the key are gone everywhere, as once A has been away longer than their TTL."""
        for node in (self.b, self.c, self.d):
            node.records.withdraw(self.key, A_ID)

    def forget_at_resolver(self):
        """B holds no provider records for the key, so its resolve has to look them up on the servers."""
        for provider in self.b.records.providers(self.key):
            self.b.records.withdraw(self.key, provider)

    def resolve_without_owner(self):
        """A goes away; B resolves its name. The result, or None if NotFound."""
        del self.net.nodes[A_ID]
        self.net.name_requests.clear()
        try:
            return self.b.name_resolve(self.name.hex())
        except NotFound:
            return None

    def assert_reached_keeper(self, result):
        """B got C's record, and asked C for it."""
        asked = [node_id.hex()[:4] for node_id in self.net.name_requests]
        self.assertTrue(C_ID in self.net.name_requests, f"keeper C never asked; asked {asked}")
        self.assertIsNotNone(result, f"resolve failed; asked {asked}")
        self.assertEqual((result["cid"], result["sequence"]), (self.cid, 1))

    def test_control_keeper_asked_without_squatters(self):
        self.build()
        self.keep_on_c()
        self.assert_reached_keeper(self.resolve_without_owner())

    def test_control_slow_resolver_asks_keeper_without_squatters(self):
        self.build(slow_resolver=True)
        self.assertEqual(self.b.config.effective_alpha(self.b.slow), 1)
        self.keep_on_c()
        self.forget_owner_record()
        self.assert_reached_keeper(self.resolve_without_owner())

    def test_squatters_at_resolver_do_not_hide_keeper(self):
        self.build()
        self.squat([self.b], SQUATTERS[:2])
        self.keep_on_c()
        self.assert_reached_keeper(self.resolve_without_owner())

    def test_squatters_at_servers_do_not_hide_keeper(self):
        self.build()
        self.squat([self.c, self.d], SQUATTERS[:2])
        self.keep_on_c()
        self.forget_at_resolver()
        self.assert_reached_keeper(self.resolve_without_owner())

    def test_one_squatter_at_slow_resolver_does_not_hide_keeper(self):
        self.build(slow_resolver=True)
        self.assertEqual(self.b.config.effective_alpha(self.b.slow), 1)
        self.squat([self.b], SQUATTERS[:1])
        self.keep_on_c()
        self.forget_owner_record()
        self.assert_reached_keeper(self.resolve_without_owner())

    def test_one_squatter_at_servers_does_not_hide_keeper_from_slow_resolver(self):
        self.build(slow_resolver=True)
        self.assertEqual(self.b.config.effective_alpha(self.b.slow), 1)
        self.squat([self.c, self.d], SQUATTERS[:1])
        self.keep_on_c()
        self.forget_owner_record()
        self.forget_at_resolver()
        self.assert_reached_keeper(self.resolve_without_owner())

    def test_owners_stale_record_does_not_hide_keeper_from_slow_resolver(self):
        """No squatter at all: at alpha 1 the gone owner's own record, the oldest, takes the only slot."""
        self.build(slow_resolver=True)
        self.keep_on_c()
        self.assert_reached_keeper(self.resolve_without_owner())

    def test_local_records_do_not_skip_the_search(self):
        """alpha squatter records at the resolver, and none of the keeper's: the search still runs and finds C."""
        self.build()
        self.squat([self.b], SQUATTERS[:3])
        self.keep_on_c()
        self.b.records.withdraw(self.key, C_ID)
        self.forget_owner_record()
        self.assert_reached_keeper(self.resolve_without_owner())

    def test_departed_owner_ranked_after_live_keeper(self):
        """The owner's lookup query failed, so its own stale record is asked after the live keeper's."""
        self.build(slow_resolver=True)
        self.keep_on_c()
        result = self.resolve_without_owner()
        self.assertEqual(self.net.name_requests[0], C_ID)
        self.assert_reached_keeper(result)

    def test_slow_resolver_asks_at_most_two_keepers(self):
        """On a slow node (alpha 1) a query is two batches of one at most, however many candidates there are."""
        self.build(slow_resolver=True)
        self.squat([self.b], SQUATTERS)
        self.assertEqual(self.b.feature("keepers").keeper_rounds(), 2)
        self.resolve_without_owner()
        self.assertLessEqual(len(self.net.name_requests), 2)

    def test_fast_resolver_asks_one_batch_when_two_keepers_answer(self):
        """alpha 3: the first batch reaches A's replacement keepers C and D, which both answer; nothing more."""
        self.build()
        self.keep_on_c()
        keeper = self.d.feature("keepers")
        keeper.keep(self.name)
        keeper.run_pending()
        self.net.sender = D_ID
        self.d.publisher.publish_due()
        self.squat([self.b], SQUATTERS)
        with mock.patch.object(self.b.feature("keepers"), "rank_keepers",
                               lambda candidates: sorted(candidates, key=lambda p: (p not in (C_ID, D_ID), p))):
            result = self.resolve_without_owner()
        self.assertEqual(sorted(self.net.name_requests[:2]), [C_ID, D_ID])
        self.assertEqual(len(self.net.name_requests), 3)  # one batch of alpha
        self.assertEqual(result["sequence"], 1)

    def two_keepers_one_behind(self):
        """C keeps sequence 1; A then publishes sequence 2, which D keeps and B resolves (so B knows 2)."""
        self.keep_on_c()
        cid2 = self.a.add_stream(io.BytesIO(b"v2"))["cid"]
        self.a.name_publish("site", cid2)
        keeper = self.d.feature("keepers")
        keeper.keep(self.name)
        keeper.run_pending()
        self.assertEqual(keeper.store.get(self.name).sequence, 2)
        self.net.sender = D_ID
        self.d.publisher.publish_due()
        self.assertEqual(self.b.name_resolve(self.name.hex())["sequence"], 2)
        return cid2

    def test_answer_older_than_known_asks_a_further_batch(self):
        """Slow resolver: C, asked first, answers sequence 1 while B knows 2, so D is asked too."""
        self.build(slow_resolver=True)
        cid2 = self.two_keepers_one_behind()
        order = [C_ID, D_ID]
        with mock.patch.object(self.b.feature("keepers"), "rank_keepers",
                               lambda candidates: sorted(candidates, key=lambda p: order.index(p) if p in order
                                                         else len(order))):
            result = self.resolve_without_owner()
        self.assertEqual(self.net.name_requests, [C_ID, D_ID])
        self.assertEqual((result["sequence"], result["cid"]), (2, cid2))

    def test_current_answer_on_slow_resolver_asks_one(self):
        """Slow resolver: D, asked first, answers the sequence B knows; no second /name is spent."""
        self.build(slow_resolver=True)
        self.two_keepers_one_behind()
        order = [D_ID, C_ID]
        with mock.patch.object(self.b.feature("keepers"), "rank_keepers",
                               lambda candidates: sorted(candidates, key=lambda p: order.index(p) if p in order
                                                         else len(order))):
            result = self.resolve_without_owner()
        self.assertEqual(self.net.name_requests, [D_ID])
        self.assertEqual(result["sequence"], 2)


class StandingTest(unittest.TestCase):
    """RoutingTable.standing, which ranks keepers: senior and healthy, healthy, stale, absent."""

    def test_order(self):
        clock = FakeClock(1000.0)
        config = Config(provider_seniority=100, announce_interval=3600)
        table = RoutingTable(bytes(16), config, clock=clock)
        old, new, failing, absent = (bytes([x] * 16) for x in (1, 2, 3, 4))
        table.heard(old)
        table.heard(failing)
        clock.now += 200
        table.heard(new)
        table.heard(old)
        table.failed(failing)
        self.assertEqual([table.standing(n, 100) for n in (old, new, failing, absent)], [0, 1, 2, 3])
        clock.now += 2 * 3600 + 1  # silent for two announce intervals
        self.assertEqual(table.standing(old, 100), 2)


if __name__ == "__main__":
    unittest.main()
