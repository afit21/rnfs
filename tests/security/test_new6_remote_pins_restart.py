"""NEW-6 (remote-pin half): remote pins accepted but not yet fetched were lost when the node restarted.

RnfsNode kept accepted /pin requests only in memory (_remote_reserved). The requester got True, but if the
node stopped before fetching them, nothing fetched them after the restart, and nothing said so. The fixed
node saves the pins waiting (the state database's remote_pins table, in the order accepted) and resumes them at
start; pins released before the stop, or from identities no longer on remote_pin_identities, are not resumed.

The tests use two offline nodes joined by tests.test_node's NetClient. The provider's /blocks answers are held
while the pinning node stops, so its pins are still waiting then; the node is then reopened over the same
directory and started the way RnfsNode.__init__ starts it (_resume_remote_pins, which _start_network runs before
it accepts links since AUD-L12, then _start_threads).
"""
import io
import os
import tempfile
import threading
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.content.cid import Cid
from rnfs.content.storage import remote_source
from rnfs.network.client import BlocksResult
from rnfs.node import RnfsNode
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


class RemotePinsRestartTest(unittest.TestCase):
    """Provider A holds files; node B accepts /pin for them from IDENTITY, then restarts."""

    def setUp(self):
        """The provider, the pinning node, and files the pinning node can find on the provider."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = NetClient()
        self.net.sender = B_ID
        self.provider = self.open("a", A_ID)
        self.node = self.open("b", B_ID)
        self.cids = [Cid.parse(self.provider.add_stream(io.BytesIO(os.urandom(2 * CHUNK_SIZE)))["cid"])
                     for _ in range(2)]
        for cid in self.cids:
            self.provider.records.store(cid.key, A_ID)
        self.real_blocks = self.net.blocks

    def open(self, name, node_id, **settings) -> RnfsNode:
        """An offline node over <tmp>/name that accepts /pin from IDENTITY (unless settings say otherwise)."""
        settings.setdefault("remote_pin_identities", [IDENTITY.hex()])
        config = Config(rnfs_dir=os.path.join(self.tmp.name, name), slow="no", lookup_timeout=5, path_timeout=0.5,
                        default_bootstrap=False, **settings)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        if node_id == B_ID:
            node.routing.heard(A_ID)
        return node

    def stop_while_waiting(self, *pins):
        """Accept /pin for pins while the provider's answers are held, then stop the node with them waiting."""
        asked, go = threading.Event(), threading.Event()

        def held(node_id, cids, on_progress=None, slot_wait=None):
            """Hold every /blocks until the test lets it go, then fail it."""
            asked.set()
            go.wait(10)
            return BlocksResult("failed", unsent=list(cids))

        self.net.blocks = held
        for cid in pins:
            self.assertTrue(self.node.backend.remote_pin(IDENTITY, cid, True))
        self.assertTrue(asked.wait(10))
        self.node.stop()
        go.set()
        self.net.blocks = self.real_blocks

    def restart(self, **settings) -> RnfsNode:
        """Reopen the pinning node over the same directory and start it as RnfsNode.__init__ does."""
        self.node = self.open("b", B_ID, **settings)
        self.node._resume_remote_pins()
        self.node._start_threads()
        return self.node

    def pinned(self, node, cid) -> bool:
        """True if node holds cid with the remote source."""
        return SOURCE in node.storage.pins().get(cid, set())

    def test_waiting_pins_are_resumed_after_a_restart(self):
        """Pins accepted and still waiting (one being fetched, one queued) are fetched after the restart."""
        self.stop_while_waiting(*self.cids[:2])
        node = self.restart()
        # NEW-6: fails until accepted remote pins are saved and re-queued at start
        self.assertTrue(wait_for(lambda: all(self.pinned(node, cid) for cid in self.cids[:2]), 15),
                        "remote pins accepted before the restart were never fetched")

    def test_release_before_the_stop_is_kept(self):
        """A pin released before the stop isn't fetched after the restart."""
        asked, go = threading.Event(), threading.Event()

        def held(node_id, cids, on_progress=None, slot_wait=None):
            """Hold every /blocks until the test lets it go, then fail it."""
            asked.set()
            go.wait(10)
            return BlocksResult("failed", unsent=list(cids))

        self.net.blocks = held
        for cid in self.cids[:2]:
            self.assertTrue(self.node.backend.remote_pin(IDENTITY, cid, True))
        self.assertTrue(asked.wait(10))
        self.assertTrue(self.node.backend.remote_pin(IDENTITY, self.cids[1], False))  # released while queued
        self.node.stop()
        go.set()
        self.net.blocks = self.real_blocks
        node = self.restart()
        self.assertTrue(wait_for(lambda: self.pinned(node, self.cids[0]), 15))
        self.assertTrue(wait_for(lambda: not node._remote_reserved.get(SOURCE), 15))
        self.assertFalse(self.pinned(node, self.cids[1]))

    def test_identity_removed_from_the_list_is_not_resumed(self):
        """Pins of an identity taken off remote_pin_identities before the restart are dropped."""
        self.stop_while_waiting(self.cids[0])
        node = self.restart(remote_pin_identities=[])
        self.assertEqual(node._remote_reserved.get(SOURCE, {}), {})  # the resume queues synchronously
        self.assertNotIn(SOURCE, node._remote_workers)
        self.assertFalse(self.pinned(node, self.cids[0]))


if __name__ == "__main__":
    unittest.main()
