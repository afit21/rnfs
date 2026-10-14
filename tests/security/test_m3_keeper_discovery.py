"""M3: identities that stored provider records for a name's keeper key before the real keepers did take every slot
a keeper-fallback resolve asks, so the real keepers are never reached.

Offline nodes over KeeperNet: owner A publishes a name and advertises it, B resolves it, C keeps it and advertises
that, D is one more DHT server. Squatter identities (never online, so they answer nothing) store provider records for
name_key(N) after A's but before C's, either on B itself or on the servers B's lookup queries (B then holds none).
A goes away and B resolves: the path request hears nothing, so B falls back to keepers. The tests check the outcome
only: B gets C's record, and C was asked. With alpha 3 two squatters are enough; on a slow node (alpha 1) one is.
"""
import io
import os
import tempfile
import unittest

import RNS

from rnfs.config import Config
from rnfs.errors import NotFound
from rnfs.features.keepers import name_key
from rnfs.node import RnfsNode
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

    # M3: fails until ask_keepers treats local records as candidates and keeps looking past non-answering ones
    @unittest.expectedFailure
    def test_squatters_at_resolver_do_not_hide_keeper(self):
        self.build()
        self.squat([self.b], SQUATTERS[:2])
        self.keep_on_c()
        self.assert_reached_keeper(self.resolve_without_owner())

    # M3: fails until the provider search runs past the first alpha providers found and ranks them by more than age
    @unittest.expectedFailure
    def test_squatters_at_servers_do_not_hide_keeper(self):
        self.build()
        self.squat([self.c, self.d], SQUATTERS[:2])
        self.keep_on_c()
        self.forget_at_resolver()
        self.assert_reached_keeper(self.resolve_without_owner())

    # M3: fails until ask_keepers treats local records as candidates and keeps looking past non-answering ones
    @unittest.expectedFailure
    def test_one_squatter_at_slow_resolver_does_not_hide_keeper(self):
        self.build(slow_resolver=True)
        self.assertEqual(self.b.config.effective_alpha(self.b.slow), 1)
        self.squat([self.b], SQUATTERS[:1])
        self.keep_on_c()
        self.forget_owner_record()
        self.assert_reached_keeper(self.resolve_without_owner())

    # M3: fails until the provider search runs past the first alpha providers found and ranks them by more than age
    @unittest.expectedFailure
    def test_one_squatter_at_servers_does_not_hide_keeper_from_slow_resolver(self):
        self.build(slow_resolver=True)
        self.assertEqual(self.b.config.effective_alpha(self.b.slow), 1)
        self.squat([self.c, self.d], SQUATTERS[:1])
        self.keep_on_c()
        self.forget_owner_record()
        self.forget_at_resolver()
        self.assert_reached_keeper(self.resolve_without_owner())

    # M3: fails until a resolve whose keepers all answer nothing asks a further batch
    @unittest.expectedFailure
    def test_owners_stale_record_does_not_hide_keeper_from_slow_resolver(self):
        """No squatter at all: at alpha 1 the gone owner's own record, the oldest, takes the only slot."""
        self.build(slow_resolver=True)
        self.keep_on_c()
        self.assert_reached_keeper(self.resolve_without_owner())


if __name__ == "__main__":
    unittest.main()
