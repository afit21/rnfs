"""NEW-6 follow-up: one malformed entry in state/remote_pins.json stopped the node from starting.

_load_remote_pins unpacked each saved [source, cid] pair inside a try, but looked the source up in a set
outside it: a source that is a JSON list or object is unhashable, so the TypeError escaped _resume_remote_pins
and _start_threads, and the node failed to start. The fixed node skips such entries (with a log line) and
resumes the rest.

The pinning node is an offline node; a provider joined to it by tests.test_node's NetClient holds the content.
The file is written as a damaged or hand-edited one might be, and the node is started the way RnfsNode.__init__
starts it (_start_threads).
"""
import io
import json
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs import node as node_module
from rnfs.config import Config
from rnfs.content.cid import Cid
from rnfs.content.storage import remote_source
from rnfs.node import REMOTE_PINS_FILE, RnfsNode
from tests.support import wait_for
from tests.test_names import FakeNetwork
from tests.test_node import A_ID, B_ID, NetClient

IDENTITY = bytes(range(16))
SOURCE = remote_source(IDENTITY)

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class RemotePinsBadEntryTest(unittest.TestCase):
    """Provider A holds a file; node B starts with a remote_pins.json holding malformed entries and a good one."""

    def setUp(self):
        """The provider, a file it provides, and the pinning node's config (accepting /pin from IDENTITY)."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = NetClient()
        self.net.sender = B_ID
        self.provider = self.open("a", A_ID)
        self.cid = Cid.parse(self.provider.add_stream(io.BytesIO(os.urandom(3000)))["cid"])
        self.provider.records.store(self.cid.key, A_ID)

    def open(self, name, node_id) -> RnfsNode:
        """An offline node over <tmp>/name that accepts /pin from IDENTITY."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, name), slow="no", lookup_timeout=5, path_timeout=0.5,
                        default_bootstrap=False, remote_pin_identities=[IDENTITY.hex()])
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def start_with(self, entries) -> RnfsNode:
        """Write entries as B's remote_pins.json, then open B and start it as RnfsNode.__init__ does."""
        state_dir = Config(rnfs_dir=os.path.join(self.tmp.name, "b")).state_dir
        os.makedirs(state_dir, exist_ok=True)
        with open(os.path.join(state_dir, REMOTE_PINS_FILE), "w", encoding="utf-8") as f:
            json.dump(entries, f)
        node = self.open("b", B_ID)
        node.routing.heard(A_ID)
        try:
            node._start_threads()
        except TypeError as error:
            # NEW-6: fails until each saved entry's types are checked inside the try, so a bad one is skipped
            self.fail(f"the node failed to start: {type(error).__name__}: {error}")
        return node

    def test_unhashable_sources_are_skipped(self):
        """Sources that are a JSON list or object are skipped and logged; the good entry is still fetched."""
        logged = []
        with mock.patch.object(node_module, "log", side_effect=lambda message, *args: logged.append(message)):
            node = self.start_with([[[SOURCE], str(self.cid)], [{"s": SOURCE}, str(self.cid)],
                                    [SOURCE, [str(self.cid)]], [SOURCE, {"cid": str(self.cid)}],
                                    [SOURCE, str(self.cid)]])
        self.assertTrue(wait_for(lambda: SOURCE in node.storage.pins().get(self.cid, set()), 15),
                        "the good remote pin wasn't resumed")
        self.assertTrue(any("4 malformed entries" in message and REMOTE_PINS_FILE in message
                            for message in logged), logged)


if __name__ == "__main__":
    unittest.main()
