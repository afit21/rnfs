"""R-1: the start-up reconcile released follow: pins when the followed names were missing, damaged or had a
malformed entry.

NEW-2 made the node release, at start, follow pins of names the follow state doesn't list. But a follow state that
is missing, damaged or holds a malformed entry lists nothing for the names it lost, so a restart released pins of
names still followed. The fixed reconcile releases only when the state was read whole (clean); otherwise it keeps
the pins and records them again (the name is followed again), so they also survive later starts and can still be
released by unfollowing. (The LXMF feature's inbox had the same fix; its tests left rnfs with the feature in 0.2.0.)

The state lives in the state database; follow.json is what rnfs 0.1.4 wrote, imported at the first start. Each test
takes B's saved follow state away between two starts and puts back a version of it: none (missing), the 0.1.4 file
truncated (moved aside as damaged when imported) or with the entry malformed (imported, but not clean), or the
database's own rows with the entry malformed or the clean value gone.

The tests use two offline nodes (A owns a name B follows) on KeeperNet, as test_new2_follow_lxmf_pins does.
"""
import io
import json
import os
import tempfile
import unittest

import RNS

from rnfs.config import Config
from rnfs.content.cid import Cid
from rnfs.features.follow import follow_source
from rnfs.node import RnfsNode
from tests.support import state_execute, write_legacy
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


def damage(state_dir: str, how: str, store: str, name: str, legacy, malformed_row: tuple[str, tuple]):
    """Take store's saved state (its rows and meta values) away and put back a version that isn't whole: none
    ("missing"), the legacy file `name` holding legacy truncated ("damaged") or with an entry malformed
    ("malformed": legacy is that), the database's rows with an entry malformed ("malformed row": malformed_row's
    statement and parameters are run instead), or its meta values without the clean one ("unclean")."""
    if how == "malformed row":
        state_execute(state_dir, *malformed_row)
        return
    state_execute(state_dir, f"DELETE FROM {store}")
    state_execute(state_dir, "DELETE FROM meta WHERE store = ?" + (" AND key = 'clean'" if how == "unclean" else ""),
                  (store,))
    if how == "damaged":
        text = json.dumps(legacy)
        write_legacy(state_dir, name, text=text[:len(text) // 2])
    elif how == "malformed":
        write_legacy(state_dir, name, legacy)


class FollowDamagedStateTest(unittest.TestCase):
    """B follows A's name; its saved follow state is then lost or damaged and B restarts."""

    def setUp(self):
        """Two nodes that know each other, A's name, and B following it at its first target."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = KeeperNet()
        self.net.sender = B_ID
        self.a = self.make_node("a", A_ID)
        self.addCleanup(self.a.stop)
        self.b = self.make_node("b", B_ID)
        self.addCleanup(lambda: self.b.stop())  # whichever B is open last
        self.a.routing.heard(B_ID)
        self.b.routing.heard(A_ID)
        self.name = bytes.fromhex(self.a.name_create("site")["name"])
        self.source = follow_source(self.name)
        self.cid = Cid.parse(self.a.add_stream(io.BytesIO(b"version one"))["cid"])
        self.a.name_publish("site", str(self.cid))
        self.a.records.store(self.cid.key, A_ID)
        self.b.names.offer(self.name, 1, self.cid, 0.0)
        follow = self.b.feature("follow")
        follow.follow(self.name)
        self.assertTrue(follow.check(self.name, network=False))
        self.assertEqual(self.b.storage.pinned_by(self.source), {self.cid})
        self.state = self.b.config.state_dir

    def make_node(self, label: str, node_id: bytes) -> RnfsNode:
        """An offline node on its own directory (a restart if it was used before)."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, label), slow="no", lookup_timeout=5, path_timeout=0.2)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        return node

    def restart(self) -> RnfsNode:
        """Stop B and open it again on the same directory."""
        self.b.stop()
        self.b = self.make_node("b", B_ID)
        self.b.routing.heard(A_ID)
        return self.b

    def check_kept(self, how: str):
        """After the follow state is made `how`, restarts keep the pin and follow the name again; unfollow
        releases."""
        self.b.stop()
        # follow.json as rnfs 0.1.4 wrote it, with the name's entry malformed
        legacy = {"version": 1, "names": {self.name.hex(): {"pinned": "not-a-cid" if how == "malformed" else
                                                             str(self.cid)}}}
        damage(self.state, how, "follow", "follow.json", legacy,
               ("UPDATE follow SET pinned = ? WHERE name = ?", (b"\x01\x02", self.name)))
        b = self.restart()
        # R-1: fails until follow pins are released at start only when the follow state was read whole
        self.assertEqual(b.storage.pinned_by(self.source), {self.cid},
                         f"a {how} follow state released the pin of a name still followed")
        self.assertEqual([entry["name"] for entry in b.feature("follow").following()], [self.name.hex()])
        if how == "damaged":
            self.assertTrue(os.path.exists(os.path.join(self.state, "follow.json.damaged")))
        elif how == "malformed":
            self.assertTrue(os.path.exists(os.path.join(self.state, "follow.json.migrated")))
        b = self.restart()  # the name was saved again: the next start keeps the pin too
        self.assertEqual(b.storage.pinned_by(self.source), {self.cid})
        self.assertEqual(b.feature("follow").following()[0]["pinned"], str(self.cid))
        b.feature("follow").unfollow(self.name)
        self.assertEqual(b.storage.pinned_by(self.source), set())

    def test_missing_follow_state_keeps_pins(self):
        """No follow state saved."""
        self.check_kept("missing")

    def test_damaged_follow_file_keeps_pins(self):
        """follow.json truncated (moved aside as damaged when imported)."""
        self.check_kept("damaged")

    def test_malformed_follow_entry_keeps_pins(self):
        """follow.json with the name's entry malformed (skipped when imported)."""
        self.check_kept("malformed")

    def test_malformed_follow_row_keeps_pins(self):
        """The name's row with a pinned value that isn't a CID (dropped at load)."""
        self.check_kept("malformed row")

    def test_follow_state_without_its_clean_value_keeps_pins(self):
        """The name's row lost with the value saying the state was saved whole (with it, the pin is released:
        test_new2_follow_lxmf_pins)."""
        self.check_kept("unclean")


if __name__ == "__main__":
    unittest.main()
