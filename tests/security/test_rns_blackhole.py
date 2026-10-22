"""No ID (RNS blackhole): a node whose identity the RNS instance blackholes kept its place in rnfs.

RNS 1.5.6 lets an instance's operator, or the blackhole lists it subscribes to, blackhole an identity
(Transport.blackholed_identities, Reticulum.get_blackholed_identities through a shared instance too). RNS then drops
its announces, removes the paths to its destinations, and tears down a link that identifies as it, but rnfs went on
using what it already held: the node stayed in the routing table and was asked in lookups, its provider records were
handed out in /find replies and used by fetches, and a link it had identified before being listed was still served.

The tests rebuild that with offline nodes (tests/test_node.py's NetClient) and a fake Reticulum whose
get_blackholed_identities lists the attacker's identity hash, in the shape RNS returns ({identity hash: {"source",
"until", "reason"}}), and a RequestServer over tests/test_server.py's fakes. The fix (rnfs/network/blackhole.py): the
node reads the list every few minutes, treats a listed identity's node ID as banned while it is listed
(RoutingTable.blackholed, so is_banned: routing table, lookups, provider records, fetches), drops it from the routing
table when it is newly listed, and its request server answers False on a link identified as a listed identity.
"""
import io
import math
import os
import tempfile
import time
import types
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import APP_NAME, DHT_ASPECT, FIND_PATH
from rnfs.content.cid import Cid
from rnfs.network import protocol
from rnfs.network.blackhole import Blackhole
from rnfs.node import RnfsNode
from tests.security._server_helpers import ServerHarness
from tests.test_names import FakeNetwork
from tests.test_node import NetClient

ATTACKER_IDENTITY = bytes([0x66] * 16)
ATTACKER = RNS.Destination.hash(ATTACKER_IDENTITY, APP_NAME, DHT_ASPECT)  # its node ID: rnfs.dht of that identity
HONEST, VICTIM = bytes([0x0C] * 16), bytes([0x0B] * 16)

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class FakeReticulum:
    """Reticulum.get_blackholed_identities as RNS answers it (locally or through a shared instance's RPC)."""

    def __init__(self):
        """Nothing blackholed."""
        self.blackholed: dict = {}

    def get_blackholed_identities(self):
        """The list, {identity hash: {"source", "until", "reason"}}."""
        return dict(self.blackholed)

    def blackhole(self, identity_hash: bytes, until=None):
        """List identity_hash, as `rnpath -B` or a blackhole source would."""
        self.blackholed[identity_hash] = {"source": bytes(16), "until": until, "reason": "spam"}


class BlackholedNodeTest(unittest.TestCase):
    """The victim's view of a blackholed node: routing, lookups, provider records and fetches."""

    def setUp(self):
        """Three offline nodes over a NetClient; the victim knows the attacker and the honest node, both providing the
        same file, and the attacker's record is stored at both."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = NetClient()
        self.net.sender = ATTACKER  # /provide calls come from the attacker
        self.attacker, self.honest, self.victim = (self.node(name, node_id) for name, node_id in
                                                   (("a", ATTACKER), ("c", HONEST), ("b", VICTIM)))
        self.cid = self.attacker.add_stream(io.BytesIO(b"provided by both"))["cid"]
        self.honest.add_stream(io.BytesIO(b"provided by both"))
        key = Cid.parse(self.cid).key
        for node in (self.attacker, self.honest):
            node.records.store(key, node.node_id)
        for node in (self.honest, self.victim):
            node.backend.provide(ATTACKER, [key], [])
        self.key = key
        for node_id in (ATTACKER, HONEST):
            self.victim.routing.heard(node_id)
        self.reticulum = FakeReticulum()
        self.victim.reticulum = self.reticulum
        self.victim.blackhole.interval = 0  # read at every maintenance pass
        self.asked: list[bytes] = []
        blocks = self.net.blocks
        self.net.blocks = lambda node_id, cids, on_progress=None: (self.asked.append(node_id),
                                                                   blocks(node_id, cids, on_progress))[1]

    def node(self, name: str, node_id: bytes) -> RnfsNode:
        """An offline node with its own directory."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, name), slow="no", lookup_timeout=5, path_timeout=0.5)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def maintain(self):
        """One maintenance pass of the victim (which reads the blackhole)."""
        self.victim.maintain(time.monotonic(), {})

    def test_blackholed_node_is_treated_as_banned(self):
        self.reticulum.blackhole(ATTACKER_IDENTITY)
        self.maintain()
        self.assertTrue(self.victim.routing.is_banned(ATTACKER), "a blackholed node isn't treated as banned")
        self.assertFalse(self.victim.routing.contains(ATTACKER), "a blackholed node stays in the routing table")
        self.assertFalse(self.victim.routing.heard(ATTACKER), "a blackholed node's announce is taken")
        providers, nodes = self.victim.backend.find(self.key)
        self.assertNotIn(ATTACKER, providers, "a /find reply hands out a blackholed node's record")
        self.assertNotIn(ATTACKER, nodes)
        self.assertNotIn(ATTACKER, self.victim.providers(self.cid))
        self.assertEqual(b"".join(self.victim.read(self.cid)), b"provided by both")
        self.assertNotIn(ATTACKER, self.asked, "a fetch asked a blackholed node")
        self.assertIn(HONEST, self.asked)

    def test_lifted_blackhole_ends_the_ban(self):
        self.reticulum.blackhole(ATTACKER_IDENTITY)
        self.maintain()
        self.reticulum.blackholed.clear()
        self.maintain()
        self.assertFalse(self.victim.routing.is_banned(ATTACKER))
        self.assertTrue(self.victim.routing.heard(ATTACKER))
        self.assertIn(ATTACKER, self.victim.backend.find(self.key)[0])

    def test_expired_entry_counts_as_lifted(self):
        self.reticulum.blackhole(ATTACKER_IDENTITY, until=time.time() - 1)
        self.maintain()
        self.assertFalse(self.victim.routing.is_banned(ATTACKER))
        self.reticulum.blackhole(bytes([0x67] * 16), until=time.time() + 3600)  # someone else, still listed
        self.reticulum.blackholed[ATTACKER_IDENTITY]["until"] = time.time() + 3600
        self.maintain()
        self.assertTrue(self.victim.routing.is_banned(ATTACKER))

    def test_unreadable_list_keeps_the_last(self):
        self.reticulum.blackhole(ATTACKER_IDENTITY)
        self.maintain()
        self.reticulum.get_blackholed_identities = lambda: 1 / 0  # the shared instance's RPC failing, say
        self.maintain()
        self.assertTrue(self.victim.routing.is_banned(ATTACKER))
        for broken in (None, [], {b"short": {}}, {ATTACKER_IDENTITY.hex(): {}}):
            self.assertEqual(Blackhole(lambda: types.SimpleNamespace(
                get_blackholed_identities=lambda broken=broken: broken)).refresh(), set())


class BlackholedLinkTest(unittest.TestCase):
    """The server: a link identified as a blackholed identity is answered False."""

    def test_identified_link_of_a_blackholed_identity_is_refused(self):
        harness = ServerHarness({})
        reticulum = FakeReticulum()
        blackhole = Blackhole(lambda: reticulum)
        harness.server.blackholed = blackhole.identity_listed
        link = harness.connect()
        handler = harness.destination.handlers[FIND_PATH][0]

        def find(identity_hash: bytes):
            """/find on the link, identified as identity_hash."""
            return handler(FIND_PATH, protocol.encode_find(bytes(16)), os.urandom(16), link.link_id,
                           types.SimpleNamespace(hash=identity_hash), 0.0)
        self.assertIsNot(find(ATTACKER_IDENTITY), False)  # identified before it was listed: RNS lets the link be
        reticulum.blackhole(ATTACKER_IDENTITY)
        blackhole.refresh()
        self.assertIs(find(ATTACKER_IDENTITY), False, "a blackholed identity's identified link is still served")
        self.assertIsNot(find(bytes([0x67] * 16)), False)
        self.assertEqual(blackhole.node_listed(ATTACKER), True)
        self.assertTrue(math.isinf(Blackhole(lambda: None)._read_at))


if __name__ == "__main__":
    unittest.main()
