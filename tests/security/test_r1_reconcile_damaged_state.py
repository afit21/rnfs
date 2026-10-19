"""R-1: the start-up reconcile released follow: and lxmf: pins when the followed names or the LXMF inbox were
missing, damaged or had a malformed entry.

NEW-2 made the node release, at start, follow pins of names the follow state doesn't list and lxmf pins no inbox
message records. But a follow state or inbox that is missing, damaged or holds a malformed entry lists nothing
for the names and messages it lost, so a restart released pins of names still followed and of messages still
wanted. The fixed reconcile releases only when the state was read whole (clean); otherwise it keeps the pins and
records them again (the name is followed again; the sender's pins get a new inbox message), so they also survive
later starts and can still be released by unfollowing or removing it.

The state lives in the state database; follow.json and lxmf_inbox.json are what rnfs 0.1.4 wrote, imported at
the first start. Each test takes B's saved follow or inbox state away between two starts and puts back a version
of it: none (missing), the 0.1.4 file truncated (moved aside as damaged when imported) or with the entry
malformed (imported, but not clean), or the database's own rows with the entry malformed or the clean value gone.

The follow tests use two offline nodes (A owns a name B follows) on KeeperNet, as test_new2_follow_lxmf_pins
does; the lxmf tests use tests.test_lxmf's FeatureTestCase.
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
from rnfs.features.follow import follow_source
from rnfs.node import RnfsNode
from tests.support import state_execute, write_legacy
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


def damage(state_dir: str, how: str, store: str, name: str, legacy, malformed_row: tuple[str, tuple]):
    """Take store's saved state (its rows and meta values) away and put back a version that isn't whole: none
    ("missing"), the legacy file `name` holding legacy truncated ("damaged") or with an entry malformed
    ("malformed": legacy is that), the database's rows with an entry malformed ("malformed row": malformed_row's
    statement and parameters are run instead), or its meta values without the clean one ("unclean")."""
    if how == "malformed row":
        state_execute(state_dir, *malformed_row)
        return
    table, where = ("lxmf_messages", " WHERE box = 'inbox'") if store == lxmf.INBOX else (store, "")
    state_execute(state_dir, f"DELETE FROM {table}{where}")
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


class LxmfDamagedStateTest(FeatureTestCase):
    """B auto-fetches and pins a link from a trusted sender; its saved inbox is then lost or damaged."""

    def check_kept(self, how: str):
        """After the inbox is made `how`, the pin is kept and recorded; removing that record releases it."""
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        kept = feature.received(self.link_message(number=1))
        feature.process_auto_fetches()
        feature.save()
        target = Cid.parse(feature.inbox.get(kept)["auto"][0]["cid"])
        source = f"app:lxmf:{SENDER.hex()}"
        # lxmf_inbox.json as rnfs 0.1.4 wrote it, with the message's record malformed: the stored records, not
        # inbox_list(), which adds computed fields (trusted, contact) 0.1.4 never wrote
        messages = feature.inbox.entries()
        legacy = {"next_id": 2, "messages": [{**messages[0], "links": "not a list"} if how == "malformed" else
                                             messages[0]]}
        self.b.stop()
        state = self.b.config.state_dir
        damage(state, how, lxmf.INBOX, "lxmf_inbox.json", legacy,
               ("UPDATE lxmf_messages SET record = '{not json' WHERE box = 'inbox' AND id = ?", (kept,)))
        self.b = self.make_node("b", B_ID, lxmf_auto_fetch_from=[SENDER.hex()])
        again = self.b.feature("lxmf")
        # R-1: fails until lxmf pins are released at start only when the inbox was read whole
        self.assertEqual(self.b.storage.pinned_by(source), {target},
                         f"a {how} inbox released the pin of a message the inbox lost")
        records = [entry for entry in again.inbox_list()
                   if {"state": "pinned", "cid": str(target)} in (entry.get("auto") or [])]
        self.assertEqual([entry["from"] for entry in records], [SENDER.hex()])
        self.assertEqual([lxmf.Link.from_json(link).cid for link in records[0]["links"]], [target])
        if how == "damaged":
            self.assertTrue(os.path.exists(os.path.join(state, "lxmf_inbox.json.damaged")))
        elif how == "malformed":
            self.assertTrue(os.path.exists(os.path.join(state, "lxmf_inbox.json.migrated")))
        again = self.reloaded()  # the record was saved: the next start keeps the pin too
        self.assertEqual(self.b.storage.pinned_by(source), {target})
        self.assertEqual(again.inbox_rm(records[0]["id"]), 1)
        self.assertEqual(self.b.storage.pinned_by(source), set())

    def test_missing_inbox_keeps_pins(self):
        """No inbox saved."""
        self.check_kept("missing")

    def test_damaged_inbox_keeps_pins(self):
        """lxmf_inbox.json truncated (moved aside as damaged when imported)."""
        self.check_kept("damaged")

    def test_malformed_inbox_record_keeps_pins(self):
        """lxmf_inbox.json with the message's record malformed (dropped when imported)."""
        self.check_kept("malformed")

    def test_malformed_inbox_row_keeps_pins(self):
        """The message's row isn't JSON (dropped at load)."""
        self.check_kept("malformed row")

    def test_inbox_without_its_clean_value_keeps_pins(self):
        """The message's row lost with the value saying the inbox was saved whole (with it, the pin is released:
        test_new2_follow_lxmf_pins)."""
        self.check_kept("unclean")


if __name__ == "__main__":
    unittest.main()
