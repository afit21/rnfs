"""Shared setup for the keeper discovery tests of the second DHT round (test_m3r1_*, test_m3r2_*).

Offline nodes over SybilNet, a KeeperNet with Sybil identities. Owner A publishes a name at sequence 1, which keeper
C keeps (its /name answer is kept as the replay an attacker would send), then at sequence 2, which keeper D keeps.
A and C then leave, so D is the only keeper holding the current record. Each test adds resolver B (and other
servers) itself, with the routing tables and provider records its case needs.
"""
import io
import os
import tempfile
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import K
from rnfs.features.keepers import name_key
from rnfs.node import RnfsNode
from tests.test_names import FakeNetwork
from tests.test_names_keep import KeeperNet

A_ID, B_ID, C_ID, D_ID = (bytes([x] * 16) for x in (0xA, 0xB, 0xC, 0xD))


class SybilNet(KeeperNet):
    """KeeperNet with Sybil identities: no node behind them, yet live DHT servers. Each answers /find for any key
    listing the first K Sybils as providers and as nodes, and /name with sybil_answer (a replayed old record, or [])."""

    def __init__(self):
        """No Sybils yet."""
        super().__init__()
        self.sybils: list[bytes] = []
        self.sybil_answer = []
        self.find_requests: list[bytes] = []

    def find(self, node_id, key):
        """A Sybil's /find answer, or the node's."""
        self.find_requests.append(node_id)
        if node_id in self.sybils:
            return list(self.sybils[:K]), list(self.sybils[:K])
        return super().find(node_id, key)

    def name_record(self, node_id, name):
        """A Sybil's /name answer, or the node's."""
        if node_id in self.sybils:
            self.name_requests.append(node_id)
            return self.sybil_answer
        return super().name_record(node_id, name)


class KeeperScenario(unittest.TestCase):
    """A name at sequence 2 kept only by D, with a validly signed replay of sequence 1 (self.old)."""

    def setUp(self):
        """The network, the name's two versions, and its keepers."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name
        self.net = SybilNet()
        a, c, self.d = self.make_node("a", A_ID), self.make_node("c", C_ID), self.make_node("d", D_ID)
        for node, node_id in ((a, A_ID), (c, C_ID), (self.d, D_ID)):
            for other in (A_ID, C_ID, D_ID):
                if other != node_id:
                    node.routing.heard(other)
        self.name = bytes.fromhex(a.name_create("site")["name"])
        self.key = name_key(self.name)
        a.name_publish("site", a.add_stream(io.BytesIO(b"v1"))["cid"])
        self.net.sender = A_ID
        a.publisher.publish_due()
        keeper = c.feature("keepers")
        keeper.keep(self.name)
        keeper.run_pending()
        self.assertEqual(keeper.store.get(self.name).sequence, 1)
        self.old = self.net.name_record(C_ID, self.name)  # sequence 1, validly signed by the name's key
        a.name_publish("site", a.add_stream(io.BytesIO(b"v2"))["cid"])
        keeper = self.d.feature("keepers")
        keeper.keep(self.name)
        keeper.run_pending()
        self.assertEqual(keeper.store.get(self.name).sequence, 2)
        self.d.records.store(self.key, D_ID)  # D lists itself in its /find answers
        del self.net.nodes[A_ID], self.net.nodes[C_ID]
        for gone in (A_ID, C_ID):  # long gone: their provider records have expired
            self.d.records.withdraw(self.key, gone)
        self.net.name_requests.clear()
        self.net.find_requests.clear()

    def make_node(self, label: str, node_id: bytes, slow: bool = False) -> RnfsNode:
        """An offline node with its own directory."""
        config = Config(rnfs_dir=os.path.join(self.dir, label), slow="no", lookup_timeout=5, path_timeout=0.2)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork(), slow=slow)
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def near_key(self, distance: int) -> bytes:
        """The ID at XOR distance `distance` from the name's keeper key (1 is the closest there is)."""
        return (int.from_bytes(self.key, "big") ^ distance).to_bytes(16, "big")

    def best(self, records) -> int | None:
        """The highest sequence among records from ask_keepers."""
        return max((record.sequence for record in records), default=None)
