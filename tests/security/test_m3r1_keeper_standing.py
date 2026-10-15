"""M3-R1: after the first M3 fix, identities that answer /find could still steer a keeper-fallback resolve: those
the lookup met joined the routing table and outranked keepers recorded here, records stored here by any identity
counted as a source that could end the search, and two answers from such identities "confirmed" a replayed old
record when the resolver knew no sequence for the name.

Offline nodes over SybilNet (tests/security/_dhtr2b_helpers.py): the name is at sequence 2, kept only by D; Sybils
replay its validly signed sequence 1. Resolver B has never seen the name, so it has no sequence to compare against.
Each test runs B's keeper query (KeepersFeature.ask_keepers) and checks that D was asked and sequence 2 came back.
"""
import random
import unittest
from unittest import mock

from tests.security._dhtr2b_helpers import B_ID, D_ID, KeeperScenario

X_ID = bytes([0x55] * 16)  # an honest DHT server that keeps nothing
SQUATTERS = [bytes([0xE0 + i] * 16) for i in range(3)]  # identities with no node behind them


class KeeperStandingTest(KeeperScenario):
    """The name at sequence 2 kept only by D; resolver B is added by each test."""

    def test_sybils_met_during_the_resolve_do_not_outrank_a_recorded_keeper(self):
        """B (a slow node) knows only X, whose table holds 10 Sybils close to the key; D's record is stored at B.
        Sybils that answer B's lookup join B's table, but must not rank above D for it."""
        x = self.make_node("x", X_ID)
        b = self.make_node("b", B_ID, slow=True)
        b.routing.heard(X_ID)
        self.net.sybils = [self.near_key(i + 1) for i in range(10)]
        self.net.sybil_answer = self.old
        for sybil in self.net.sybils:
            x.routing.heard(sybil)
        self.assertTrue(b.records.store(self.key, D_ID))
        keepers = b.feature("keepers")
        for trial in range(5):
            for sybil in self.net.sybils:
                b.routing.remove(sybil)  # met afresh by each resolve
            keepers._rng = random.Random(trial)
            self.net.name_requests.clear()
            records = keepers.ask_keepers(self.name)
            asked = [node_id.hex()[:6] for node_id in self.net.name_requests]
            # M3-R1: fails until keepers are ranked on the routing table as it stood before the lookup
            self.assertIn(D_ID, self.net.name_requests, f"trial {trial}: keeper D never asked; asked {asked}")
            self.assertEqual(self.best(records), 2, f"trial {trial}: asked {asked}")

    def test_answers_without_standing_do_not_settle_a_resolve_with_nothing_to_compare(self):
        """B (alpha 3) holds records of three Sybils and of D, and asks the Sybils first (ranking forced). Their three
        replays of sequence 1 agree, but none is an established server, so B must ask further and reach D."""
        b = self.make_node("b", B_ID)
        b.routing.heard(D_ID)
        self.net.sybils = list(SQUATTERS)
        self.net.sybil_answer = self.old
        for provider in SQUATTERS + [D_ID]:
            self.assertTrue(b.records.store(self.key, provider))
        order = SQUATTERS + [D_ID]
        keepers = b.feature("keepers")
        with mock.patch.object(keepers, "rank_keepers",
                               lambda candidates, before=None: sorted(
                                   candidates, key=lambda p: order.index(p) if p in order else len(order))):
            records = keepers.ask_keepers(self.name)
        asked = [node_id.hex()[:6] for node_id in self.net.name_requests]
        self.assertEqual(self.net.name_requests[:3], SQUATTERS)
        # M3-R1: fails until only established keepers' answers confirm each other when no sequence is known
        self.assertIn(D_ID, self.net.name_requests, f"three replayed answers settled the resolve; asked {asked}")
        self.assertEqual(self.best(records), 2)

    def test_records_stored_here_by_unknown_identities_do_not_end_the_search(self):
        """B (a slow node, so 3 candidates are wanted) holds records of three squatters; X, the server closest to the
        key, lists one of them. Those must not end the search before D, also in B's table, is asked by the lookup."""
        x_id = self.near_key(1)
        x = self.make_node("x", x_id)
        b = self.make_node("b", B_ID, slow=True)
        b.routing.heard(x_id)
        b.routing.heard(D_ID)
        for squatter in SQUATTERS:
            self.assertTrue(b.records.store(self.key, squatter))
        self.assertTrue(x.records.store(self.key, SQUATTERS[0]))
        records = b.feature("keepers").ask_keepers(self.name)
        found = [node_id.hex()[:6] for node_id in self.net.find_requests]
        # M3-R1: fails until records from publishers without standing don't count as a source in enough()
        self.assertIn(D_ID, self.net.find_requests, f"the search stopped before reaching D; /find sent to {found}")
        self.assertEqual(self.net.name_requests[0], D_ID)
        self.assertEqual(self.best(records), 2)


if __name__ == "__main__":
    unittest.main()
