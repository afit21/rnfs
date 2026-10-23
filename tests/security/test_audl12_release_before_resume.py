"""AUD-L12: a remote-pin release arriving while the node started was lost, and the pin resumed.

RnfsNode._start_network accepted links (destination.accepts_links(True)) before _start_threads queued the remote
pins saved as waiting at the last stop (_resume_remote_pins). A /pin [cid, false] from the identity that asked for
one of them, arriving in between, found nothing reserved and nothing pinned, so it changed nothing; the resume then
queued the pin from the database and fetched and pinned it, against its owner's last word. (A pin accepted in that
window could also have its save, by the maintenance thread, replace the saved ones before they were read.)

The test stops an offline node with a remote pin waiting, reopens it over the same directory, and starts its network
the way RnfsNode.__init__ does (_start_network, with RNS's parts stood in for), delivering the release at the moment
links are accepted. The fixed node loads the saved remote pins before it accepts links, so the release finds the pin
reserved and drops it: nothing is pinned, and the pin isn't saved as waiting.
"""
import io
import os
import tempfile
import threading
import types
import unittest
from unittest import mock

import RNS

from rnfs import node as node_module
from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.content.cid import Cid
from rnfs.content.storage import remote_source
from rnfs.mesh import no_path_info
from rnfs.network.client import BlocksResult
from rnfs.node import RnfsNode
from rnfs.remote_pins import RELEASED
from tests.support import state_rows, wait_for
from tests.test_names import FakeNetwork
from tests.test_node import A_ID, B_ID, NetClient

IDENTITY = bytes(range(16))
SOURCE = remote_source(IDENTITY)


class ReleaseBeforeResumeTest(unittest.TestCase):
    """Provider A holds a file; node B accepted /pin of it from IDENTITY, stopped before fetching it, and starts."""

    def setUp(self):
        """The provider, the pinning node with the pin saved as waiting, and RNS's log silenced."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = NetClient()
        self.net.sender = B_ID
        self.net.traffic = None  # what RnfsNode._start_network hands the request server
        self.provider = self.open("a", A_ID)
        self.cid = Cid.parse(self.provider.add_stream(io.BytesIO(os.urandom(2 * CHUNK_SIZE)))["cid"])
        self.provider.records.store(self.cid.key, A_ID)
        node = self.open("b", B_ID)
        real_blocks, asked, go = self.net.blocks, threading.Event(), threading.Event()

        def held(node_id, cids, on_progress=None, slot_wait=None):
            """Hold every /blocks until the node has stopped, then fail it."""
            asked.set()
            go.wait(10)
            return BlocksResult("failed", unsent=list(cids))

        self.net.blocks = held
        self.assertTrue(node.backend.remote_pin(IDENTITY, self.cid, True))
        self.assertTrue(asked.wait(10))
        node.stop()
        go.set()
        self.net.blocks = real_blocks
        self.assertEqual(self.waiting(), [(self.cid.binary,)])

    def open(self, name, node_id) -> RnfsNode:
        """An offline node over <tmp>/name that accepts /pin from IDENTITY."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, name), slow="no", lookup_timeout=5, path_timeout=0.5,
                        default_bootstrap=False, remote_pin_identities=[IDENTITY.hex()])
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        if node_id == B_ID:
            node.routing.heard(A_ID)
        return node

    def waiting(self) -> list:
        """The remote pins B's state database holds as waiting."""
        return state_rows(os.path.join(self.tmp.name, "b", "state"), "SELECT cid FROM remote_pins")

    def start_network(self, node: RnfsNode, on_accepting):
        """node._start_network as RnfsNode.__init__ runs it, RNS's parts stood in for; on_accepting() runs as the
        destination starts accepting links."""

        def accepts_links(accepting):
            """RNS.Destination.accepts_links: a request may arrive as soon as links are accepted."""
            if accepting:
                on_accepting()

        node.destination = types.SimpleNamespace(accepts_links=accepts_links)
        node.links = types.SimpleNamespace(paths=types.SimpleNamespace(lacked=lambda node_id: False,
                                                                       named=lambda node_id, by: None),
                                           close_all=lambda: None)
        paths = types.SimpleNamespace(info=no_path_info, infos=lambda node_ids: {}, hops=lambda node_id: 0,
                                      scan=lambda max_hops: {})
        with mock.patch.object(node_module, "RnsPaths", return_value=paths), \
                mock.patch.object(node_module, "RequestServer"), mock.patch.object(node_module, "DhtAnnouncer"), \
                mock.patch.object(node_module, "DhtAnnounceHandler"), mock.patch.object(node.names, "start"):
            node._start_network(node.config, False)

    def test_a_release_as_links_are_accepted_is_kept(self):
        node = self.open("b", B_ID)
        self.start_network(node, lambda: node.backend.remote_pin(IDENTITY, self.cid, False))
        self.assertTrue(wait_for(lambda: not node._remote_workers, 15), "the remote-pin worker didn't end")
        # AUD-L12: fails until the saved remote pins are loaded before links are accepted
        self.assertNotIn(SOURCE, node.storage.pins().get(self.cid, set()), "the released pin was resumed and pinned")
        self.assertEqual(node.pin_remote_served()["pins"][0]["state"], RELEASED)
        node._save_remote_pins()
        self.assertEqual(self.waiting(), [])

    def test_a_waiting_pin_is_still_resumed(self):
        """With no release, the saved pin is fetched and pinned after the start, as before."""
        node = self.open("b", B_ID)
        self.start_network(node, lambda: None)
        self.assertTrue(wait_for(lambda: SOURCE in node.storage.pins().get(self.cid, set()), 15))


if __name__ == "__main__":
    unittest.main()
