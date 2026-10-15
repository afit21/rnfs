"""L5: when a followed name moves, the new target is fetched and pinned with no size cap, so a hostile or compromised
name owner could point it at a DAG of any size.

B follows A's name. A first publishes an honest file, then points the name at a root that declares 1 byte but holds
about 2 MB of empty file nodes (the L3 bomb). The follow fetch now runs under the same DAG byte bound as every
fetch_dag (rnfs.fetcher.dag_bound of the new target's declared size), so the check fails with "refusing it" after
the root, the old target stays pinned and the bomb is not. Content size itself stays bounded by max_download_size
(no default) and the disk reserve, as before: a per-follow size limit was not added.
"""
import io
import os
import tempfile
import unittest

import RNS

from rnfs.config import Config
from rnfs.content.cid import Cid
from rnfs.content.storage import USER
from rnfs.features.follow import follow_source
from rnfs.node import RnfsNode
from tests.security._content_helpers import SLACK, Bomb
from tests.test_names import FakeNetwork
from tests.test_names_keep import KeeperNet

A_ID, B_ID = bytes([0xA] * 16), bytes([0xB] * 16)


class FollowCapTest(unittest.TestCase):
    """Owner A and follower B as offline nodes."""

    def setUp(self):
        """Two nodes that know each other, and A's name."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = KeeperNet()
        self.net.sender = B_ID
        self.a = self.make_node("a", A_ID)
        self.b = self.make_node("b", B_ID)
        self.a.routing.heard(B_ID)
        self.b.routing.heard(A_ID)
        self.name = bytes.fromhex(self.a.name_create("site")["name"])
        self.downloaded = 0
        real = self.net.blocks

        def counting(node_id, cids, on_progress=None):
            """KeeperNet.blocks, adding up the bytes returned."""
            result = real(node_id, cids, on_progress)
            self.downloaded += sum(len(block) for block in result.blocks.values())
            return result

        self.net.blocks = counting

    def make_node(self, label: str, node_id: bytes) -> RnfsNode:
        """An offline node with its own directory."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, label), slow="no", lookup_timeout=5, path_timeout=0.2)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def test_follow_refuses_a_target_its_size_cant_hold(self):
        honest = Cid.parse(self.a.add_stream(io.BytesIO(b"version one"))["cid"])
        self.a.name_publish("site", str(honest))
        self.a.records.store(honest.key, A_ID)
        follow = self.b.feature("follow")
        follow.follow(self.name)
        self.b.names.offer(self.name, 1, honest, 0.0)  # as if B heard A's announce
        self.assertTrue(follow.check(self.name, network=False))
        source = follow_source(self.name)
        self.assertEqual(self.b.storage.pins().get(honest), {source})
        # The name moves to the bomb
        bomb = Bomb()
        with self.a.storage.protect() as session:
            for cid, block in bomb.blocks.items():
                session.put(cid, block)
        self.a.storage.pin(bomb.root, USER)
        self.a.publisher.refresh()
        self.a.records.store(bomb.root.key, A_ID)
        self.a.name_publish("site", str(bomb.root))
        self.b.names.offer(self.name, 2, bomb.root, 0.0)
        self.downloaded = 0
        self.assertFalse(follow.check(self.name, network=False))
        self.assertIn("refusing it", follow.following()[0]["state"])
        self.assertLessEqual(self.downloaded, bomb.size + SLACK)
        pins = self.b.storage.pins()
        self.assertEqual(pins.get(honest), {source})
        self.assertNotIn(bomb.root, pins)
        self.assertEqual(follow.following()[0]["pinned"], str(honest))


if __name__ == "__main__":
    unittest.main()
