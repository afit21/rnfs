"""AUD-M6 (follow and deny part): the follow and deny-list fetches had no directory entry cap (an N1 variant).

FollowFeature.check and DenyFeature.check fetched the new target with `fetch.fetch_dag(expect)`, without the
max_entries that get, export and listings pass (max_output_entries). Directory nodes pay for themselves in the
fetch budget, so a tree of empty files declares size 0 whatever it holds: follow_quota (1 GiB by default) was then
the only bound, and a followed name's owner could make the follower fetch and pin far more entries than any `get`
of the same target would accept (the audit pinned 200,000, about 1.4 KB of memory each while fetching).

B follows A's name with max_output_entries lowered to ENTRIES_CAP, and A points the name at a directory of more
empty files than that, whose blocks fit easily within follow_quota. The fixed check fails with the entry cap in its
state, the old target stays pinned and the new one isn't. A deny list source is always a file, which holds no
directory entries (DenyFeature.check refuses a directory before fetching, and caps the file at MAX_LIST_BYTES), so
passing the cap there changes nothing an attacker can reach and no test of it can fail; it is passed for the same
rule everywhere.
"""
import io
import os
import tempfile
import unittest

import RNS

from rnfs.config import Config
from rnfs.content.cid import Cid
from rnfs.errors import TooLarge
from rnfs.features.follow import follow_source
from rnfs.node import RnfsNode
from tests.test_names import FakeNetwork
from tests.test_names_keep import KeeperNet

A_ID, B_ID = bytes([0xA] * 16), bytes([0xB] * 16)
ENTRIES_CAP = 50  # B's max_output_entries
FILES = 4 * ENTRIES_CAP  # empty files in the tree A's name moves to


class FollowEntryCapTest(unittest.TestCase):
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
        self.b = self.make_node("b", B_ID, max_output_entries=ENTRIES_CAP)
        self.a.routing.heard(B_ID)
        self.b.routing.heard(A_ID)
        self.name = bytes.fromhex(self.a.name_create("site")["name"])
        self.follow = self.b.feature("follow")
        self.source = follow_source(self.name)
        self.honest = self.move(Cid.parse(self.a.add_stream(io.BytesIO(b"version one"))["cid"]), 1)
        self.follow.follow(self.name)
        self.assertTrue(self.follow.check(self.name, network=False))
        self.assertEqual(self.b.storage.pins().get(self.honest), {self.source})

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
        """A directory on disk of count empty files: size 0, whatever it holds."""
        root = os.path.join(self.tmp.name, "empties")
        os.mkdir(root)
        for i in range(count):
            open(os.path.join(root, f"{i:04d}"), "wb").close()
        return root

    def test_a_target_with_more_entries_than_the_cap_is_refused(self):
        """A size-0 tree of FILES entries, well within follow_quota, is refused at max_output_entries."""
        tree = Cid.parse(self.a.add_path(self.empty_files(FILES))["cid"])
        self.assertLess(self.a.storage.dag_bytes(tree), self.b.config.follow_quota)
        with self.assertRaises(TooLarge, msg="a get of the same target is refused"):
            self.b.get(str(tree), os.path.join(self.tmp.name, "out"))
        self.move(tree, 2)
        # AUD-M6: fails until the follow fetch passes max_output_entries to fetch_dag
        self.assertFalse(self.follow.check(self.name, network=False), "the follow pinned more entries than the cap")
        entry = self.follow.following()[0]
        self.assertIn("max_output_entries", entry["state"])
        self.assertNotIn(tree, self.b.storage.pins())
        self.assertEqual(self.b.storage.pins().get(self.honest), {self.source})
        self.assertEqual(entry["pinned"], str(self.honest))

    def test_a_target_within_the_cap_is_pinned(self):
        """A tree of fewer entries than the cap is followed as before."""
        small = os.path.join(self.tmp.name, "small")
        os.mkdir(small)
        for i in range(ENTRIES_CAP // 2):
            open(os.path.join(small, f"{i:04d}"), "wb").close()
        tree = self.move(Cid.parse(self.a.add_path(small)["cid"]), 2)
        self.assertTrue(self.follow.check(self.name, network=False))
        self.assertEqual(self.b.storage.pins().get(tree), {self.source})


if __name__ == "__main__":
    unittest.main()
