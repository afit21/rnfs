"""REV4: a server beyond the island could force an island node into partition mode.

Partition mode (specification section 6, "LoRa islands") was entered after FAILURES_TO_ENTER (3) failures in a row
reaching off-island servers (`PartitionMode.failed`). A server off the island that a slow node asks answers `/find`
with 3 made-up IDs closest to the key: the node's search tries them (a slow node's search may try 3 unknown IDs), each
fails, and the node entered partition mode at once, so its lookups and publishes kept to its island until a probe
(15 minutes to 2 hours on a slow node) answered. The probe targets also began with the IDs that had failed, made-up
ones included, so the first probe failed too.

Now only failures of servers the node knows (in its routing table or announce first-seen log, or whose identity or
path RNS knows) count; 3 in a row ask for a probe of a server that answered (or announced itself) recently, as
silence does, and partition mode starts only if that probe fails; an ID that only ever failed is never a probe target.
"""
import unittest

from rnfs.dht.local import FAILURES_TO_ENTER, PartitionMode
from rnfs.dht.routing import distance
from tests.fakes.log import quiet_log
from tests.support import FakeClock
from tests.test_islands import HUB, LORA, IslandCase, ident

KEY = ident(0x4444)
ATTACKER = ident(0x4445)  # off the island, next to KEY: asked first
MADE_UP = [ident(0x4440 + n) for n in range(FAILURES_TO_ENTER)]  # closer to KEY than any server but the attacker

setUpModule, tearDownModule = quiet_log()


class ForcedPartitionTest(IslandCase):
    """A slow island node, connected to the backbone, whose search reaches an off-island server naming made-up IDs."""

    def setUp(self):
        """4 island servers and 8 backbone servers that have answered the node, and the attacker, all in its routing
        table; RNS knows the identities of real servers only."""
        super().setUp()
        self.node = self.make_node(slow="yes", local_interfaces=["LoRa"])
        self.islanders = [self.server(n, LORA) for n in range(1, 5)]
        self.backbone = [self.server(n, HUB) for n in range(0x80, 0x88)]
        self.server(int.from_bytes(ATTACKER, "big"), HUB)
        for n in [ATTACKER] + self.islanders + self.backbone:  # the attacker first: the bucket holds K
            self.node._heard_server(n)
            self.node.routing.heard(n)
        for n in self.backbone + [ATTACKER]:
            self.node._answered(n)  # off-island servers answered earlier lookups
        self.node._is_known = lambda node_id: node_id in self.net.nodes
        self.asked: list[bytes] = []
        find = self.net.find

        def find_as_attacked(node_id, key):
            """/find, recorded; the attacker names the made-up IDs."""
            self.asked.append(node_id)
            if node_id == ATTACKER:
                return [], list(MADE_UP)
            return find(node_id, key)

        self.net.find = find_as_attacked
        self.assertTrue(all(distance(n, KEY) < distance(b, KEY) for n in MADE_UP for b in self.backbone))

    def test_made_up_ids_dont_enter_partition_mode(self):
        self.node.lookups.search(KEY).find_providers(lambda provider: None)
        self.assertLessEqual(set(MADE_UP), set(self.asked))  # each was tried, and failed
        self.assertFalse(self.node.partition.active)
        self.assertFalse(self.node.partition.probe_due())  # they don't even ask for a probe
        self.node.check_local()
        self.assertFalse(self.node.partition.active)
        # The node still uses servers beyond the island
        self.asked.clear()
        self.node.lookups.search(ident(0x9999)).find_providers(lambda provider: None)
        self.assertTrue(set(self.asked) & set(self.backbone))

    def test_known_failures_ask_for_a_probe_of_a_server_that_answered(self):
        """Three backbone servers the node knows go down: a probe, not partition mode; the probe goes to a server that
        answered and is still up, so the node stays connected."""
        down = self.backbone[:FAILURES_TO_ENTER]
        for n in down:
            self.net.nodes.pop(n)
            self.node._query_failed(n)
        self.assertFalse(self.node.partition.active)
        self.assertTrue(self.node.partition.probe_due())
        self.asked.clear()
        self.node.check_local()
        self.assertEqual(len(self.asked), 1)
        self.assertNotIn(self.asked[0], down)
        self.assertFalse(self.node.partition.active)
        self.assertFalse(self.node.partition.probe_due())


class ProbeTargetsTest(unittest.TestCase):
    """PartitionMode on its own: IDs that never answered aren't probe targets."""

    def test_failed_ids_are_never_probed(self):
        clock = FakeClock()
        mode = PartitionMode(lambda n: False, slow=True, clock=clock, known=lambda n: n not in MADE_UP)
        mode.answered(ATTACKER)
        for n in MADE_UP:
            mode.failed(n)
        self.assertFalse(mode.active or mode.probe_due())
        # Known servers that failed: a probe, of one that answered
        mode.answered(ident(0x80))
        for n in range(FAILURES_TO_ENTER):
            mode.failed(ident(0x90 + n))
        self.assertFalse(mode.active)
        self.assertTrue(mode.probe_due())
        targets = {mode.probe_target() for _ in range(10)}
        self.assertEqual(targets, {ident(0x80), ATTACKER})
        mode.probed(ident(0x80), False)
        self.assertTrue(mode.active)  # the probe failed: cut off after all
        clock.now += 7200
        targets = {mode.probe_target() for _ in range(10)}
        self.assertEqual(targets, {ident(0x80), ATTACKER})  # the failed ones are probed only if they once answered


if __name__ == "__main__":
    unittest.main()
