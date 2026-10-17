"""Direct tests of a keeper query (rnfs/features/keepers.py, _KeeperQuery): the search for a name's keepers, the
choice of each batch, the answers kept, and when they settle the name.

The keepers resolution as a whole (resolve, kept records, forgeries) is tested through offline nodes in
tests/test_names_keep.py and tests/security/test_m3*.py; here resolver B is an offline node over KeeperNet, whose
/name answers each test sets per keeper.
"""
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.features.keepers import _UNVOUCHED, _KeeperQuery, name_key
from rnfs.names import name_for_identity
from rnfs.node import RnfsNode
from tests.test_names import FakeNetwork
from tests.test_names_keep import CID1, CID2, KeeperNet, signed

B_ID, S_ID = bytes([0xB] * 16), bytes([0x5] * 16)
K1, K2, K3, K4 = (bytes([0x10 + i] * 16) for i in range(4))  # keepers: /name answers only (no node behind them)
SOURCE1, SOURCE2 = bytes([0x21] * 16), bytes([0x22] * 16)


class KeeperQueryTest(unittest.TestCase):
    """Resolver B (alpha 3) and the records of a name at sequences 1 and 2, signed by its key."""

    def setUp(self):
        """The network, B, and the name's records."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name
        self.net = KeeperNet()
        self.b = self.make_node("b", B_ID)
        identity = RNS.Identity()
        self.name = name_for_identity(identity)
        self.key = name_key(self.name)
        self.old, self.new = signed(identity, 1, CID1).wire(), signed(identity, 2, CID2).wire()

    def make_node(self, label: str, node_id: bytes, slow: bool = False, **settings) -> RnfsNode:
        """An offline node with its own directory; heard servers are established at once unless settings say."""
        settings.setdefault("provider_seniority", 0)
        config = Config(rnfs_dir=os.path.join(self.dir, label), slow="no", lookup_timeout=5, path_timeout=0.2,
                        **settings)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork(), slow=slow)
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def query(self, node: RnfsNode | None = None) -> _KeeperQuery:
        """A keeper query of node (B by default) for the name."""
        return _KeeperQuery((node or self.b).feature("keepers"), self.name)

    def with_candidates(self, vouched: dict[bytes, set[bytes]], node: RnfsNode | None = None) -> _KeeperQuery:
        """A query whose search found vouched's keys, ranked in that order, each listed by the given sources."""
        query = self.query(node)
        query.candidates = list(vouched)
        query.vouched = dict(vouched)
        return query

    # The search

    def test_search_ranks_providers_and_records_who_vouches(self):
        """A record stored here vouches with this node; one an established server lists, with that server."""
        s = self.make_node("s", S_ID)
        self.b.routing.heard(S_ID)
        self.assertTrue(s.records.store(self.key, K1))
        self.assertTrue(self.b.records.store(self.key, K2))
        self.b.routing.heard(K2)  # in the table: ranks before K1, which isn't
        query = self.query()
        self.assertEqual(query.search(), [K2, K1])
        self.assertEqual(query.vouched, {K1: {S_ID}, K2: {B_ID}})
        self.assertIsNone(query.floor)
        self.assertEqual(self.net.name_requests, [])  # searching asks no keeper

    def test_listing_by_a_server_without_standing_vouches_as_nobody(self):
        b = self.make_node("b2", bytes([0xB2] * 16), provider_seniority=86400)
        s = self.make_node("s", S_ID)
        b.routing.heard(S_ID)  # healthy, but not in the table long enough to be established
        self.assertTrue(s.records.store(self.key, K1))
        query = self.query(b)
        self.assertEqual(query.search(), [K1])
        self.assertEqual(query.vouched, {K1: {_UNVOUCHED}})

    def test_search_finds_nothing(self):
        query = self.query()
        self.assertEqual(query.search(), [])
        self.assertEqual(query.candidates, [])

    def test_search_takes_known_sequence_as_floor(self):
        self.assertTrue(self.b.records.store(self.key, K1))
        self.b.names.offer(self.name, 2, CID2, 0.0)
        query = self.query()
        query.search()
        self.assertEqual(query.floor, 2)

    # Batches

    def test_batch_takes_keepers_of_unused_sources_first_then_rank_order(self):
        query = self.with_candidates({K1: {SOURCE1}, K2: {SOURCE1}, K3: {SOURCE2}, K4: {SOURCE1, SOURCE2}})
        self.assertEqual(query.next_batch(), [K1, K3, K2])
        self.assertEqual(query.next_batch(), [K4])
        self.assertEqual(query.next_batch(), [])
        self.assertEqual(query.asked, 4)

    def test_slow_node_asks_one_at_a_time(self):
        b = self.make_node("slow", bytes([0xB3] * 16), slow=True)
        query = self.with_candidates({K1: {SOURCE1}, K2: {SOURCE2}}, node=b)
        self.assertEqual((query.next_batch(), query.next_batch(), query.next_batch()), ([K1], [K2], []))

    def test_unknown_keepers_get_a_budget_of_alpha(self):
        """Each unknown keeper costs a path request: past alpha of them, they are passed over (and dropped)."""
        unknown = {K1, K2, K3, K4}
        with mock.patch.object(self.b, "_is_known", lambda node_id: node_id not in unknown):
            query = self.with_candidates({K1: {SOURCE1}, K2: {SOURCE1}, K3: {SOURCE1}, K4: {SOURCE1}, S_ID: {SOURCE1}})
            self.assertEqual(query.next_batch(), [K1, K2, K3])
            self.assertEqual(query.next_batch(), [S_ID])  # K4 is over the budget
            self.assertEqual(query.candidates, [])
            self.assertEqual(query.asked, 4)

    # Answers

    def test_only_verifying_answers_are_kept(self):
        other = signed(RNS.Identity(), 3, CID2).wire()  # validly signed, but by another name's key
        self.net.forged.update({K1: self.old, K2: other, K3: False, K4: None})
        query = self.with_candidates({K1: {SOURCE1}, K2: {SOURCE1}, K3: {SOURCE1}, K4: {SOURCE1}})
        for keeper in (K1, K2, K3, K4):
            query.ask(keeper)
        self.assertEqual([(record.sequence, record.cid) for record in query.records()], [(1, CID1)])

    def test_failed_request_is_no_answer(self):
        query = self.with_candidates({K1: {SOURCE1}})
        with mock.patch.object(self.net, "name_record", side_effect=OSError("link failed")):
            query.ask(K1)
        self.assertEqual(query.records(), [])

    # Settling

    def test_nothing_known_settles_on_two_established_keepers(self):
        """With no sequence known, only established keepers confirm each other."""
        self.b.routing.heard(K1)
        self.b.routing.heard(K2)
        self.net.forged.update({K1: self.old, K2: self.old, K3: self.old})
        query = self.with_candidates({K1: {SOURCE1}, K2: {SOURCE1}, K3: {SOURCE1}})
        self.assertFalse(query.settled())
        query.ask(K1)
        query.ask(K3)  # not in B's table: no standing
        self.assertFalse(query.settled())
        query.ask(K2)
        self.assertTrue(query.settled())

    def test_known_sequence_settles_on_an_answer_reaching_it_and_a_second_keeper(self):
        self.net.forged.update({K1: self.old, K2: self.old, K3: self.new})
        query = self.with_candidates({K1: {SOURCE1}, K2: {SOURCE1}, K3: {SOURCE1}})
        query.floor = 2
        query.ask(K1)
        query.ask(K2)
        self.assertFalse(query.settled())  # two answers, both older than what is known
        query.ask(K3)
        self.assertTrue(query.settled())  # any keepers count once an answer reaches the known sequence

    def test_slow_node_settles_on_one_answer_reaching_known_sequence(self):
        b = self.make_node("slow", bytes([0xB3] * 16), slow=True)
        self.net.forged[K1] = self.new
        query = self.with_candidates({K1: {SOURCE1}}, node=b)
        query.floor = 2
        query.ask(K1)
        self.assertTrue(query.settled())


if __name__ == "__main__":
    unittest.main()
