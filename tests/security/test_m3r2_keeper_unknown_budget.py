"""M3-R2: a keeper query asked keepers whose identity RNS doesn't know (each costs a path request) only within the
lookup's budget of alpha unknown IDs, which the lookup itself spends on nodes /find replies name, so replies naming a
few unknown nodes kept every unknown keeper from being asked.

Offline nodes over SybilNet (tests/security/_dhtr2b_helpers.py): the name is at sequence 2, kept only by D. Resolver
B (alpha 3) knows only X, whose /find answer lists D as a provider and three unknown, unreachable IDs as the nodes
closest to the key. B's identity knowledge is narrowed to X, so D and those nodes are unknown. The lookup spends its
budget on the three nodes; D must still be asked, from a budget of the keeper query's own.
"""
import unittest

from tests.security._dhtr2b_helpers import B_ID, D_ID, KeeperScenario

X_ID = bytes([0x55] * 16)


class UnknownKeeperBudgetTest(KeeperScenario):
    """The name at sequence 2 kept only by D, which resolver B doesn't know."""

    def test_unknown_nodes_in_replies_do_not_use_up_the_keepers_budget(self):
        x = self.make_node("x", X_ID)
        b = self.make_node("b", B_ID)
        b._is_known = lambda node_id: node_id == X_ID  # RNS knows X's identity only
        b.routing.heard(X_ID)
        unknown = [self.near_key(i + 1) for i in range(b.config.effective_alpha(b.slow))]
        for node_id in unknown:
            x.routing.heard(node_id)  # no node behind them: B's /find to each fails
        self.assertTrue(x.records.store(self.key, D_ID))
        records = b.feature("keepers").ask_keepers(self.name)
        self.assertTrue(set(unknown) <= set(self.net.find_requests))  # the lookup spent its budget on them
        # M3-R2: fails until unknown keepers have a budget of their own, apart from the lookup's
        self.assertIn(D_ID, self.net.name_requests, "the unknown keeper D was never asked")
        self.assertEqual(self.best(records), 2)

    def test_unknown_keepers_have_a_budget_of_alpha(self):
        """However many unknown keepers there are, alpha are asked per query: none were once the lookup's budget was
        spent, and no more than alpha now."""
        x = self.make_node("x", X_ID)
        b = self.make_node("b", B_ID)
        b._is_known = lambda node_id: node_id == X_ID
        b.routing.heard(X_ID)
        alpha = b.config.effective_alpha(b.slow)
        for i in range(alpha):
            x.routing.heard(self.near_key(i + 1))
        strangers = [bytes([0xE0 + i] * 16) for i in range(2 * alpha)]  # unknown, never answer
        for stranger in strangers:
            self.assertTrue(x.records.store(self.key, stranger))
        b.feature("keepers").ask_keepers(self.name)
        # M3-R2: fails until unknown keepers have a budget of their own, apart from the lookup's
        self.assertEqual(len(self.net.name_requests), alpha)

if __name__ == "__main__":
    unittest.main()
