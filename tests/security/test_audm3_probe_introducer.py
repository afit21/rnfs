"""AUD-M3: the server that named the failing identities was the first probe target (a REV4 bypass).

After REV4, failures of off-island servers a node knows only ask for a probe (`PartitionMode.failed`), and partition
mode (specification section 6, "LoRa islands") starts only if that probe fails. The probe went to the off-island server
that answered most recently (`PartitionMode.probe_target`). An attacker off the island announces 3 identities that
never accept a link, answers a slow node's `/find` naming them, and is then the most recent off-island server to have
answered: the probe goes to it, it drops the probe, and the node enters partition mode for 15 minutes to 2 hours (its
lookups and publishes keep to the island), then heal-republishes everything. A probe also counted as failed when
other off-island servers answered while it was out.

Rebuilt here on PartitionMode alone, as the audit did, and on a slow island node whose search meets the attacker,
the probe sent at once as the local-servers thread sends it (on_probe).

Now a failure is told which servers named the failing identity (`Lookup.introducers`, through the node's
`_query_failed`), and those aren't probe targets while any other is; senior servers (heard for provider_seniority, so
not identities made for the attack) are preferred; and a probe that goes unanswered after an off-island server
answered or announced itself while it was out doesn't enter partition mode.
"""
import unittest

from rnfs.dht.local import FAILURES_TO_ENTER, PartitionMode
from tests.fakes.log import quiet_log
from tests.support import FakeClock
from tests.test_islands import HUB, LORA, IslandCase, ident

KEY = ident(0x4444)
ATTACKER = ident(0x4445)  # off the island, next to KEY
ANNOUNCED = [ident(0x4440 + n) for n in range(FAILURES_TO_ENTER)]  # announced by the attacker: RNS knows them
HONEST = [ident(0x80 + n) for n in range(4)]  # off-island servers that answered earlier lookups

setUpModule, tearDownModule = quiet_log()


class ProbeTargetTest(unittest.TestCase):
    """PartitionMode on its own."""

    def mode(self, **options) -> PartitionMode:
        """A slow node's partition mode that every server is known to, the honest servers having answered."""
        mode = PartitionMode(lambda n: False, slow=True, clock=FakeClock(), known=lambda n: True, **options)
        for n in HONEST:
            mode.answered(n)
        return mode

    def test_the_server_that_named_the_failing_ids_isnt_probed(self):
        mode = self.mode()
        for _ in range(10):
            mode.answered(ATTACKER)  # it answers a /find, naming its identities
            for n in ANNOUNCED:
                mode.failed(n, named_by={ATTACKER})
            self.assertTrue(mode.probe_due())
            target = mode.probe_target()
            self.assertNotEqual(target, ATTACKER)
            mode.probed(target, answered=target != ATTACKER)
            self.assertFalse(mode.active)

    def test_senior_servers_are_probed_first(self):
        """A server heard for long can't be an identity made for the attack, unlike one that just answered."""
        fresh = ident(0x90)
        mode = self.mode(senior=lambda n: n in HONEST)
        mode.answered(fresh)
        for n in ANNOUNCED:
            mode.failed(n)
        self.assertEqual({mode.probe_target() for _ in range(8)}, set(HONEST))

    def test_contact_while_the_probe_was_out_counts(self):
        mode = self.mode()
        for n in ANNOUNCED:
            mode.failed(n)
        target = mode.probe_target()
        mode.answered(HONEST[0] if target != HONEST[0] else HONEST[1])  # another off-island server answers meanwhile
        mode.probed(target, answered=False)
        self.assertFalse(mode.active)
        # Without contact meanwhile, an unanswered probe still enters partition mode
        for n in ANNOUNCED:
            mode.failed(n)
        mode.probed(mode.probe_target(), answered=False)
        self.assertTrue(mode.active)

    def test_only_suspects_left_are_probed_all_the_same(self):
        """With no other off-island server to probe, the one that named the IDs is (failures alone don't enter)."""
        mode = PartitionMode(lambda n: False, slow=True, clock=FakeClock(), known=lambda n: True)
        mode.answered(ATTACKER)
        for n in ANNOUNCED:
            mode.failed(n, named_by={ATTACKER})
        self.assertFalse(mode.active)
        self.assertEqual(mode.probe_target(), ATTACKER)


class ForcedPartitionTest(IslandCase):
    """A slow island node, connected to the backbone, whose search reaches the attacker."""

    def test_the_attacker_drops_the_probe(self):
        node = self.make_node(slow="yes", local_interfaces=["LoRa"])
        islanders = [self.server(n, LORA) for n in range(1, 5)]
        backbone = [self.server(n, HUB) for n in range(0x80, 0x88)]
        self.server(int.from_bytes(ATTACKER, "big"), HUB)
        for n in [ATTACKER] + islanders + backbone:
            node._heard_server(n)
            node.routing.heard(n)
        for n in backbone + [ATTACKER]:
            node._answered(n)
        node._is_known = lambda n: n in self.net.nodes or n in ANNOUNCED
        find = self.net.find
        probed: list[bytes] = []

        def find_as_attacked(node_id, key):
            """/find: the attacker names its identities, and drops the probe."""
            if key == node.node_id:
                probed.append(node_id)
            if node_id == ATTACKER:
                return None if key == node.node_id else ([], list(ANNOUNCED))
            return find(node_id, key)

        self.net.find = find_as_attacked
        node.partition.on_probe = node.probe_off_island  # sent at once, as the local-servers thread is woken to
        node.lookups.search(KEY).find_providers(lambda provider: None)
        self.assertTrue(probed)
        self.assertNotIn(ATTACKER, probed)
        self.assertFalse(node.partition.active)


if __name__ == "__main__":
    unittest.main()
