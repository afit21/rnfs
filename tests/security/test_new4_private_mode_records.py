"""NEW-4: in private mode, /find replies listed provider records of non-members stored before the switch.

NodeBackend.find filtered providers only by bans, and ProviderStore._load reloaded saved records with no
membership check. The request server refuses non-members' /provide once private mode is on, but records
already held (stored while the node was public, or from a member removed since) kept being handed to members
for up to PROVIDER_TTL, and a member's client then opened identified links to those outsiders.

The tests run offline nodes over the same state directory, first public (or with a longer member list), then
private, with RNS.Identity.recall patched to know which node IDs belong to member identities (as RNS would
from their announces). They check /find replies, the facade's providers() and the records kept, and that a
private node's LinkPool doesn't link to (and so identify to) a non-member.
"""
import os
import tempfile
import types
import unittest
from unittest import mock

import RNS

from rnfs import node as node_module
from rnfs.config import Config
from rnfs.content.cid import RAW, Cid
from rnfs.network import links
from rnfs.node import RnfsNode
from tests.test_links import fake_rns, run
from tests.test_names import FakeNetwork
from tests.test_node import NetClient

OWN = bytes([0xA] * 16)
OUTSIDER = bytes([0x66] * 16)  # node ID of a non-member that provided while the node was public
MEMBER = bytes([0x77] * 16)  # node ID of a member
FORMER = bytes([0x88] * 16)  # node ID of a member removed from the list later
IDENTITIES = {OUTSIDER: bytes([0x16] * 16), MEMBER: bytes([0x17] * 16), FORMER: bytes([0x18] * 16)}
CID = Cid.for_block(RAW, b"provided by everyone")
KEY = CID.key

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


def recall(node_id, _no_use=False):
    """RNS.Identity.recall as RNS would answer it for the test's node IDs (their announces were heard)."""
    identity_hash = IDENTITIES.get(node_id)
    return None if identity_hash is None else types.SimpleNamespace(hash=identity_hash)


class PrivateModeRecordsTest(unittest.TestCase):
    """One node's state directory, opened public then private."""

    def setUp(self):
        """A temporary rnfs dir, and RNS.Identity.recall answering for the test's node IDs."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = os.path.join(self.tmp.name, "a")
        patcher = mock.patch.object(node_module.RNS.Identity, "recall", side_effect=recall)
        patcher.start()
        self.addCleanup(patcher.stop)

    def open(self, **settings) -> RnfsNode:
        """The node over self.root, offline, with these settings."""
        config = Config(rnfs_dir=self.root, slow="no", lookup_timeout=1, path_timeout=0.2, **settings)
        node = RnfsNode.offline(config, OWN, NetClient(), FakeNetwork())
        self.addCleanup(node.stop)
        return node

    def members(self, *node_ids):
        """Settings for private mode with these node IDs' identities as members."""
        return {"private": True, "members": [IDENTITIES[node_id].hex() for node_id in node_ids]}

    def test_records_from_before_private_mode_are_not_handed_out(self):
        """Records an outsider stored while the node was public are dropped when it starts in private mode."""
        public = self.open()
        for publisher in (OUTSIDER, MEMBER):
            self.assertTrue(public.backend.provide(publisher, [KEY], []))
        public.records.store(KEY, OWN)
        public.stop()  # saves the provider records

        private = self.open(**self.members(MEMBER))
        providers, _ = private.backend.find(KEY)
        # NEW-4: fails until private mode leaves non-members out of /find replies and purges their records
        self.assertNotIn(OUTSIDER, providers, "a /find reply in private mode lists a non-member's record")
        self.assertEqual(sorted(providers), sorted([MEMBER, OWN]))
        self.assertNotIn(OUTSIDER, private.records.providers(KEY))  # gone, not just hidden
        with mock.patch.object(private, "_lookup") as lookup:
            lookup.return_value.find_providers = lambda add: None
            self.assertNotIn(OUTSIDER, private.providers(str(CID)))

    def test_records_of_a_removed_member_are_dropped(self):
        """A member taken off the list loses its records at the next start."""
        before = self.open(**self.members(MEMBER, FORMER))
        for publisher in (MEMBER, FORMER):
            self.assertTrue(before.backend.provide(publisher, [KEY], []))
        before.stop()

        after = self.open(**self.members(MEMBER))
        providers, _ = after.backend.find(KEY)
        # NEW-4: fails until records of identities no longer on the members list are dropped
        self.assertEqual(providers, [MEMBER], "a removed member's record is still handed out")

    def test_reply_leaves_out_a_non_member_record_stored_while_private(self):
        """Whatever put it there, a non-member's record isn't handed out in private mode; a publisher the
        server accepted as a member is, even if RNS doesn't recall its identity."""
        private = self.open(**self.members(MEMBER))
        unknown_member = bytes([0x99] * 16)  # a member's node ID that RNS hasn't heard an announce for
        private.records.store(KEY, OUTSIDER)
        self.assertTrue(private.backend.provide(unknown_member, [KEY], []))  # the server checked membership
        providers, _ = private.backend.find(KEY)
        # NEW-4: fails until /find replies in private mode are filtered by membership
        self.assertEqual(providers, [unknown_member], "a non-member's record was handed out in private mode")

    def test_public_mode_is_unchanged(self):
        """Outside private mode every record is handed out as before."""
        public = self.open()
        public.backend.provide(OUTSIDER, [KEY], [])
        self.assertEqual(public.backend.find(KEY)[0], [OUTSIDER])


class PrivateModeLinksTest(unittest.TestCase):
    """The client side: a private node's LinkPool links (and identifies) only to members."""

    def setUp(self):
        """RNS in the links module replaced by tests.test_links' fake, recalling identities with hashes."""
        rns = fake_rns()
        rns.Identity = types.SimpleNamespace(
            recall=lambda node_id: types.SimpleNamespace(node_id=node_id, hash=IDENTITIES.get(node_id, bytes(16))))
        patcher = mock.patch.object(links, "RNS", rns)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.identity = object()

    def pool(self, **settings) -> links.LinkPool:
        """A LinkPool with these settings."""
        pool = links.LinkPool(self.identity, Config(**settings))
        self.addCleanup(pool.close_all)
        return pool

    def test_private_node_does_not_link_to_a_non_member(self):
        """An outsider listed as a provider (by a stale record anywhere) gets no link and no identification."""
        pool = self.pool(private=True, members=[IDENTITIES[MEMBER].hex()])
        # NEW-4 (client side): fails until private mode opens links only to members
        self.assertIsNone(run(self, lambda: pool.get(OUTSIDER)), "a private node linked to a non-member")
        self.assertIsNone(run(self, lambda: pool.open_identified(OUTSIDER)))
        member = run(self, lambda: pool.get(MEMBER))
        self.assertIsNotNone(member)
        self.assertEqual(member.link.identities, [self.identity])

    def test_public_node_links_to_anyone(self):
        """Outside private mode, links go to any node, unidentified."""
        peer = run(self, lambda: self.pool().get(OUTSIDER))
        self.assertIsNotNone(peer)
        self.assertEqual(peer.link.identities, [])


if __name__ == "__main__":
    unittest.main()
