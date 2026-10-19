"""NEW-2 (the same pattern in other pin sources): follow: and lxmf: pins that a crash left unrecorded were never released.

- follow: unfollow saves the followed names before releasing the pin, and check() pins the new target before
  saving the name's and only then releases the target recorded there. A crash in between left a
  `follow:<name>` pin that no later check, unfollow or `pin rm` released.
- lxmf: an auto-fetched pin is saved at once, the inbox only every few seconds, so a crash could leave an
  `lxmf:<sender>` pin whose message the saved inbox doesn't have; inbox removal could never release it.

The crash is a BaseException from a wrapped call, and the node or feature is then built again on the same
directory without stop(), as in a real crash (each is stopped and closed only when the test is done).
"""
import io
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs import statedb
from rnfs.config import Config
from rnfs.content.cid import Cid
from rnfs.content.storage import USER
from rnfs.features import lxmf
from rnfs.features.follow import follow_source
from rnfs.node import RnfsNode
from tests.support import state_rows
from tests.test_lxmf import SENDER, FeatureTestCase
from tests.test_names import FakeNetwork
from tests.test_names_keep import KeeperNet

A_ID, B_ID = bytes([0xA] * 16), bytes([0xB] * 16)

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the nodes' log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class Crash(BaseException):
    """Stands in for the process dying."""


class FollowCrashTest(unittest.TestCase):
    """Owner A publishes a name that follower B follows; offline nodes."""

    def setUp(self):
        """Two nodes that know each other, and A's name."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = KeeperNet()
        self.net.sender = B_ID
        self.a = self.make_node("a", A_ID)
        self.addCleanup(self.a.stop)
        self.b = self.make_node("b", B_ID)
        self.addCleanup(self.b.stop)
        self.a.routing.heard(B_ID)
        self.b.routing.heard(A_ID)
        self.name = bytes.fromhex(self.a.name_create("site")["name"])
        self.source = follow_source(self.name)

    def make_node(self, label: str, node_id: bytes) -> RnfsNode:
        """An offline node on its own directory (a restart if it was used before)."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, label), slow="no", lookup_timeout=5, path_timeout=0.2)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        return node

    def publish(self, data: bytes, sequence: int, follower: RnfsNode) -> Cid:
        """Point A's name at new content and tell follower of the record."""
        cid = Cid.parse(self.a.add_stream(io.BytesIO(data))["cid"])
        self.a.name_publish("site", str(cid))
        self.a.records.store(cid.key, A_ID)
        follower.names.offer(self.name, sequence, cid, 0.0)
        return cid

    def test_crash_inside_unfollow_leaves_no_pin_after_restart(self):
        """Crash after the name's row is deleted, before its pin is released."""
        follow = self.b.feature("follow")
        cid1 = self.publish(b"version one", 1, self.b)
        follow.follow(self.name)
        self.assertTrue(follow.check(self.name, network=False))
        with mock.patch.object(self.b.storage, "unpin", side_effect=Crash()):
            with self.assertRaises(Crash):
                follow.unfollow(self.name)
        b = self.make_node("b", B_ID)
        self.addCleanup(b.stop)
        # NEW-2: fails until follow pins of names no longer followed are released at start
        self.assertEqual(b.storage.pinned_by(self.source), set(), f"{cid1} stays pinned for a name not followed")

    def test_crash_between_pinning_and_saving_is_cleaned_up_by_the_next_move(self):
        """Crash after pinning the new target, before the name's row records it; the name then moves again."""
        follow = self.b.feature("follow")
        self.publish(b"version one", 1, self.b)
        follow.follow(self.name)
        self.assertTrue(follow.check(self.name, network=False))
        self.publish(b"version two", 2, self.b)
        with mock.patch.object(follow, "_save", side_effect=Crash()):
            with self.assertRaises(Crash):
                follow.check(self.name, network=False)
        b = self.make_node("b", B_ID)
        self.addCleanup(b.stop)
        b.routing.heard(A_ID)
        cid3 = self.publish(b"version three", 3, b)
        self.assertTrue(b.feature("follow").check(self.name, network=False))
        # NEW-2: fails until a check releases every other target its follow source holds, not only the recorded one
        self.assertEqual(b.storage.pinned_by(self.source), {cid3})
        b.feature("follow").unfollow(self.name)
        self.assertEqual(b.storage.pinned_by(self.source), set())

    def test_crash_inside_the_commit_recording_the_new_target(self):
        """The process dies inside the commit of the name's new target: nothing of it is saved, and the next
        start and move clean up as above."""
        follow = self.b.feature("follow")
        cid1 = self.publish(b"version one", 1, self.b)
        follow.follow(self.name)
        self.assertTrue(follow.check(self.name, network=False))
        cid2 = self.publish(b"version two", 2, self.b)
        commit = statedb._Database._commit

        def crashing(database, conn):
            """Die in the commit that records cid2 as the name's target."""
            if conn.execute("SELECT count(*) FROM follow WHERE pinned = ?", (cid2.binary,)).fetchone()[0]:
                raise Crash()
            commit(database, conn)

        with mock.patch.object(statedb._Database, "_commit", crashing):
            with self.assertRaises(Crash):
                follow.check(self.name, network=False)
        state = self.b.config.state_dir
        self.assertEqual(state_rows(state, "SELECT pinned FROM follow"), [(cid1.binary,)])
        b = self.make_node("b", B_ID)
        self.addCleanup(b.stop)
        b.routing.heard(A_ID)
        self.assertEqual(b.storage.pinned_by(self.source), {cid1, cid2})  # a followed name's: kept
        cid3 = self.publish(b"version three", 3, b)
        self.assertTrue(b.feature("follow").check(self.name, network=False))
        self.assertEqual(b.storage.pinned_by(self.source), {cid3})


class LxmfCrashTest(FeatureTestCase):
    """B auto-fetches a link from a trusted sender, then crashes before its inbox is saved."""

    def test_pin_of_a_message_the_saved_inbox_lacks_is_released_at_start(self):
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        kept = feature.received(self.link_message(number=1))
        feature.process_auto_fetches()
        feature.save()  # the first message and its pin are saved
        target = Cid.parse(feature.inbox.get(kept)["auto"][0]["cid"])
        other = Cid.parse(self.a.add_stream(io.BytesIO(os.urandom(2000)))["cid"])
        lost = feature.received(self.link_message(lxmf.Link(other, from_node=A_ID), number=2))
        feature.process_auto_fetches()
        self.assertEqual(feature.inbox.get(lost)["auto"][0]["state"], "pinned")
        self.b.storage.pin(other, USER)  # another source's pin is never touched
        # Crash before the inbox is saved: the feature is built again from what is on disk
        again = self.reloaded()
        source = f"app:lxmf:{SENDER.hex()}"
        # NEW-2: fails until lxmf: pins the saved inbox doesn't record are released at start
        self.assertEqual(self.b.storage.pinned_by(source), {target})
        self.assertEqual(self.b.storage.pins()[other], {USER})
        self.assertEqual(again.inbox_rm(kept), 1)
        self.assertEqual(self.b.storage.pinned_by(source), set())


if __name__ == "__main__":
    unittest.main()
