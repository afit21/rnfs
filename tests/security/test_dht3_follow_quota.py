"""DHT-3: a followed name's owner could fill the follower's disk. The follow fetch (follow.py, `fetcher.open(target,
pinned=True)`) had no size limit beyond max_download_size (no default) and the disk reserve, and a pinned fetch never
stops at, or evicts for, max_storage: pointing the name at a large target pinned all of it.

B follows A's name with a small follow_quota. A first publishes a small file, which B pins, then points the name at
a target over the quota: a file larger than it, or a directory of empty files (size 0) whose directory nodes hold
more bytes than it. Each check now fails with the quota in its state, having downloaded little more than the root;
the old target stays pinned and the new one isn't. follow_quota = 0 fetches nothing at all, and a target pinned
before the quota was lowered stays pinned (no migration).
"""
import io
import os
import tempfile
import unittest

import RNS

from rnfs import fetcher
from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE, GIB
from rnfs.content.cid import Cid
from rnfs.features.follow import follow_source
from rnfs.node import RnfsNode
from tests.security._content_helpers import SLACK
from tests.test_names import FakeNetwork
from tests.test_names_keep import KeeperNet
from tests.test_node import _CancelAfter

A_ID, B_ID = bytes([0xA] * 16), bytes([0xB] * 16)
QUOTA = 50_000


class FollowQuotaTest(unittest.TestCase):
    """Owner A and follower B as offline nodes; B follows A's name, at a small file pinned already."""

    def setUp(self):
        """Two nodes that know each other, and A's name pointing at a small file B follows and pins."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = KeeperNet()
        self.net.sender = B_ID
        self.a = self.make_node("a", A_ID)
        self.b = self.make_node("b", B_ID, follow_quota=QUOTA)
        self.a.routing.heard(B_ID)
        self.b.routing.heard(A_ID)
        self.name = bytes.fromhex(self.a.name_create("site")["name"])
        self.downloaded = 0
        real = self.net.blocks

        def counting(node_id, cids, on_progress=None, slot_wait=None):
            """KeeperNet.blocks, adding up the bytes returned."""
            result = real(node_id, cids, on_progress)
            self.downloaded += sum(len(block) for block in result.blocks.values())
            return result

        self.net.blocks = counting
        self.follow = self.b.feature("follow")
        self.source = follow_source(self.name)
        self.honest = self.move(Cid.parse(self.a.add_stream(io.BytesIO(b"version one"))["cid"]), 1)
        self.follow.follow(self.name)
        self.assertTrue(self.follow.check(self.name, network=False))
        self.assertEqual(self.b.storage.pins().get(self.honest), {self.source})
        self.downloaded = 0

    def make_node(self, label: str, node_id: bytes, **settings) -> RnfsNode:
        """An offline node with its own directory."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, label), slow="no", lookup_timeout=5, path_timeout=0.2,
                        **settings)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def move(self, target: Cid, sequence: int) -> Cid:
        """Point A's name at target (which A provides), as if B heard the announce."""
        self.a.records.store(target.key, A_ID)
        self.a.name_publish("site", str(target))
        self.b.names.offer(self.name, sequence, target, 0.0)
        return target

    def empty_files(self, count: int) -> str:
        """A directory on disk of count empty files with long names: size 0, but kilobytes of directory nodes."""
        root = os.path.join(self.tmp.name, "empties")
        os.mkdir(root)
        for i in range(count):
            open(os.path.join(root, f"{i:04d}" + "n" * 120), "wb").close()
        return root

    def assert_refused(self, target: Cid):
        """The check of the moved name failed on the quota, downloading little; the old target is still pinned."""
        self.assertFalse(self.follow.check(self.name, network=False))
        entry = self.follow.following()[0]
        self.assertIn("follow_quota", entry["state"])
        self.assertLessEqual(self.downloaded, SLACK)
        pins = self.b.storage.pins()
        self.assertEqual(pins.get(self.honest), {self.source})
        self.assertNotIn(target, pins)
        self.assertEqual(entry["pinned"], str(self.honest))
        self.assertEqual(self.b.storage.cached_fetches(), [])  # refused: what it got isn't kept for a retry

    def test_default_is_one_gib(self):
        self.assertEqual(Config().follow_quota, 1 * GIB)

    def test_target_larger_than_the_quota_is_refused(self):
        big = Cid.parse(self.a.add_stream(io.BytesIO(os.urandom(4 * QUOTA)))["cid"])
        self.assert_refused(self.move(big, 2))

    def test_target_whose_blocks_exceed_the_quota_is_refused(self):
        """Size 0, but more than QUOTA bytes of directory nodes: max_dag_bytes counts them."""
        empties = Cid.parse(self.a.add_path(self.empty_files(400))["cid"])
        self.assertGreater(self.a.storage.dag_bytes(empties), QUOTA)
        self.move(empties, 2)
        self.assertFalse(self.follow.check(self.name, network=False))
        entry = self.follow.following()[0]
        self.assertIn("follow_quota", entry["state"])
        # The quota stops the fetch at about one request beyond it
        self.assertLessEqual(self.downloaded, QUOTA + SLACK)
        self.assertNotIn(empties, self.b.storage.pins())
        self.assertEqual(self.b.storage.pins().get(self.honest), {self.source})
        self.assertEqual(self.b.storage.cached_fetches(), [])

    def test_target_within_the_quota_is_pinned(self):
        small = self.move(Cid.parse(self.a.add_stream(io.BytesIO(os.urandom(QUOTA // 2)))["cid"]), 2)
        self.assertTrue(self.follow.check(self.name, network=False))
        self.assertEqual(self.b.storage.pins().get(small), {self.source})
        self.assertNotIn(self.honest, self.b.storage.pins())

    def test_a_fetch_cut_short_keeps_its_progress(self):
        """A new target's fetch that fails (here cut short) keeps what it got as a cached fetch, so the next check
        resumes from there; the old target stays pinned meanwhile."""
        self.b.config.follow_quota = 8 * CHUNK_SIZE
        target = self.move(Cid.parse(self.a.add_stream(io.BytesIO(os.urandom(4 * CHUNK_SIZE)))["cid"]), 2)
        with fetcher.observing(_CancelAfter(2)):
            self.assertFalse(self.follow.check(self.name, network=False))
        self.assertEqual(self.b.storage.cached_fetches(), [(target, (), target)])
        self.assertEqual(self.b.storage.pins().get(self.honest), {self.source})
        kept = self.b.storage.stat(target)["local_bytes"]
        self.assertGreater(kept, 0)
        self.downloaded = 0
        self.assertTrue(self.follow.check(self.name, network=False))
        self.assertEqual(self.b.storage.pins().get(target), {self.source})
        self.assertEqual(self.downloaded, self.b.storage.dag_bytes(target) - kept)  # only what it didn't have

    def test_zero_quota_fetches_nothing(self):
        self.b.config.follow_quota = 0
        small = self.move(Cid.parse(self.a.add_stream(io.BytesIO(b"version two"))["cid"]), 2)
        self.assertFalse(self.follow.check(self.name, network=False))
        self.assertIn("follow_quota", self.follow.following()[0]["state"])
        self.assertEqual(self.downloaded, 0)
        self.assertNotIn(small, self.b.storage.pins())
        self.assertEqual(self.b.storage.pins().get(self.honest), {self.source})

    def test_target_pinned_before_the_quota_was_lowered_stays(self):
        self.b.config.follow_quota = 1
        self.assertTrue(self.follow.check(self.name, network=False))
        self.assertEqual(self.follow.following()[0]["state"], "current")
        self.assertEqual(self.b.storage.pins().get(self.honest), {self.source})


if __name__ == "__main__":
    unittest.main()
