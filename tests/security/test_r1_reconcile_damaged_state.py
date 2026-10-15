"""R-1: the start-up reconcile released follow: and lxmf: pins when follow.json or the LXMF inbox was missing,
damaged or had a malformed entry.

NEW-2 made the node release, at start, follow pins of names follow.json doesn't list and lxmf pins no inbox
message records. But a follow.json or lxmf_inbox.json that is missing, damaged (read_json moves it aside) or
holds a malformed entry lists nothing for the names and messages it lost, so a restart released pins of names
still followed and of messages still wanted. The fixed reconcile releases only when the file was read whole;
otherwise it keeps the pins and records them again (the name is followed again; the sender's pins get a new
inbox message), so they also survive later starts and can still be released by unfollowing or removing it.

The follow tests use two offline nodes (A owns a name B follows) on KeeperNet, as test_new2_follow_lxmf_pins
does; the lxmf tests use tests.test_lxmf's FeatureTestCase. Each damages the state file between two starts.
"""
import io
import json
import os
import tempfile
import unittest

import RNS

from rnfs.config import Config
from rnfs.content.cid import Cid
from rnfs.features import lxmf
from rnfs.features.follow import FOLLOW_FILE, follow_source
from rnfs.node import RnfsNode
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


def damage(path: str, how: str, malformed):
    """Make the state file at path missing, damaged (truncated JSON) or hold one malformed entry: malformed(state)
    changes the loaded state in place."""
    if how == "missing":
        os.remove(path)
    elif how == "damaged":
        with open(path, "r+", encoding="utf-8") as f:
            text = f.read()
            f.seek(0)
            f.truncate()
            f.write(text[:len(text) // 2])
    else:
        with open(path, encoding="utf-8") as f:
            state = json.load(f)
        malformed(state)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(state, f)


class FollowDamagedStateTest(unittest.TestCase):
    """B follows A's name; follow.json is then lost or damaged and B restarts."""

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
        self.path = os.path.join(self.b.config.state_dir, FOLLOW_FILE)

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
        """After follow.json is made `how`, restarts keep the pin and follow the name again; unfollow releases."""
        self.b.stop()
        damage(self.path, how, lambda state: state["names"][self.name.hex()].update(pinned="not-a-cid"))
        b = self.restart()
        # R-1: fails until follow pins are released at start only when follow.json was read whole
        self.assertEqual(b.storage.pinned_by(self.source), {self.cid},
                         f"a {how} {FOLLOW_FILE} released the pin of a name still followed")
        self.assertEqual([entry["name"] for entry in b.feature("follow").following()], [self.name.hex()])
        b = self.restart()  # follow.json was written again: the next start keeps the pin too
        self.assertEqual(b.storage.pinned_by(self.source), {self.cid})
        self.assertEqual(b.feature("follow").following()[0]["pinned"], str(self.cid))
        b.feature("follow").unfollow(self.name)
        self.assertEqual(b.storage.pinned_by(self.source), set())

    def test_missing_follow_file_keeps_pins(self):
        """follow.json deleted."""
        self.check_kept("missing")

    def test_damaged_follow_file_keeps_pins(self):
        """follow.json truncated (moved aside as damaged at start)."""
        self.check_kept("damaged")

    def test_malformed_follow_entry_keeps_pins(self):
        """follow.json with the name's entry malformed (skipped at load)."""
        self.check_kept("malformed")


class LxmfDamagedStateTest(FeatureTestCase):
    """B auto-fetches and pins a link from a trusted sender; its inbox file is then lost or damaged."""

    def check_kept(self, how: str):
        """After lxmf_inbox.json is made `how`, the pin is kept and recorded; removing that record releases it."""
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        kept = feature.received(self.link_message(number=1))
        feature.process_auto_fetches()
        feature.save()
        target = Cid.parse(feature.inbox.get(kept)["auto"][0]["cid"])
        source = f"lxmf:{SENDER.hex()}"
        path = os.path.join(self.b.config.state_dir, "lxmf_inbox.json")
        damage(path, how, lambda state: state["messages"][0].update(links="not a list"))
        again = lxmf.LxmfFeature(self.b)
        # R-1: fails until lxmf pins are released at start only when the inbox file was read whole
        self.assertEqual(self.b.storage.pinned_by(source), {target},
                         f"a {how} inbox file released the pin of a message the inbox lost")
        records = [entry for entry in again.inbox_list()
                   if {"state": "pinned", "cid": str(target)} in (entry.get("auto") or [])]
        self.assertEqual([entry["from"] for entry in records], [SENDER.hex()])
        self.assertEqual([lxmf.Link.from_json(link).cid for link in records[0]["links"]], [target])
        again = lxmf.LxmfFeature(self.b)  # the record was saved: the next start keeps the pin too
        self.assertEqual(self.b.storage.pinned_by(source), {target})
        self.assertEqual(again.inbox_rm(records[0]["id"]), 1)
        self.assertEqual(self.b.storage.pinned_by(source), set())

    def test_missing_inbox_keeps_pins(self):
        """lxmf_inbox.json deleted."""
        self.check_kept("missing")

    def test_damaged_inbox_keeps_pins(self):
        """lxmf_inbox.json truncated (moved aside as damaged at start)."""
        self.check_kept("damaged")

    def test_malformed_inbox_record_keeps_pins(self):
        """lxmf_inbox.json with the message's record malformed (dropped at load)."""
        self.check_kept("malformed")


if __name__ == "__main__":
    unittest.main()
