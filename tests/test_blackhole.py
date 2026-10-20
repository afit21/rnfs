"""RNS's identity blackhole as rnfs reads it (rnfs/network/blackhole.py): the list's shape, expired and malformed
entries, the node IDs derived, reads every few minutes, and the routing table's hook. The attack it stops is
tests/security/test_rns_blackhole.py."""
import unittest

import RNS

from rnfs.config import Config
from rnfs.dht.routing import RoutingTable
from rnfs.network import blackhole
from tests.fakes.log import quiet_log
from tests.support import FakeClock

LISTED, OTHER = bytes([0x66] * 16), bytes([0x67] * 16)
setUpModule, tearDownModule = quiet_log()


def node_id(identity_hash: bytes) -> bytes:
    """The rnfs.dht destination of the identity with this hash."""
    return RNS.Destination.hash(identity_hash, "rnfs", "dht")


class ListedTest(unittest.TestCase):
    """listed() over what get_blackholed_identities returns."""

    def test_shape(self):
        now = 1000.0
        entries = {LISTED: {"source": bytes(16), "until": None, "reason": None},
                   OTHER: {"source": bytes(16), "until": now + 1, "reason": "spam"},
                   bytes([1] * 16): {"until": now},  # expired: RNS lifts it within a minute
                   bytes([2] * 16): "not a dict",  # kept: only an expiry can lift an entry
                   bytes([3] * 15): {}, "text": {}}
        self.assertEqual(blackhole.listed(entries, now), {LISTED, OTHER, bytes([2] * 16)})
        for broken in (None, [], "x"):
            self.assertEqual(blackhole.listed(broken, now), set())


class BlackholeTest(unittest.TestCase):
    """Blackhole over a fake Reticulum, in virtual time."""

    def setUp(self):
        """A list read through self.entries."""
        self.clock = FakeClock()
        self.entries = {}
        self.reticulum = type("R", (), {"get_blackholed_identities": lambda _: dict(self.entries)})()
        self.list = blackhole.Blackhole(lambda: self.reticulum, clock=self.clock, wall_clock=lambda: 0.0)

    def test_node_ids_newly_listed(self):
        self.entries[LISTED] = {"until": None}
        self.assertEqual(self.list.refresh(), {node_id(LISTED)})
        self.assertTrue(self.list.node_listed(node_id(LISTED)) and self.list.identity_listed(LISTED))
        self.assertFalse(self.list.node_listed(LISTED))  # an identity hash isn't a node ID
        self.entries[OTHER] = {"until": None}
        self.assertEqual(self.list.refresh(), {node_id(OTHER)})  # only the new one
        self.entries.clear()
        self.assertEqual(self.list.refresh(), set())
        self.assertFalse(self.list.node_listed(node_id(LISTED)))

    def test_read_every_few_minutes(self):
        self.entries[LISTED] = {"until": None}
        self.assertEqual(self.list.refresh_if_due(), {node_id(LISTED)})
        self.entries[OTHER] = {"until": None}
        self.clock.now += blackhole.REFRESH - 1
        self.assertEqual(self.list.refresh_if_due(), set())
        self.assertEqual(self.list.reads, 1)
        self.clock.now += 1
        self.assertEqual(self.list.refresh_if_due(), {node_id(OTHER)})

    def test_without_reticulum_nothing_is_listed(self):
        self.assertEqual(blackhole.Blackhole(lambda: None).refresh(), set())


class RoutingHookTest(unittest.TestCase):
    """RoutingTable.blackholed: banned while listed, not saved as a ban."""

    def test_banned_while_listed(self):
        table = RoutingTable(bytes(16), Config())
        listed = set()
        table.blackholed = listed.__contains__
        self.assertTrue(table.heard(LISTED))
        listed.add(OTHER)
        self.assertTrue(table.is_banned(OTHER))
        self.assertFalse(table.heard(OTHER))
        self.assertFalse(table.answered(OTHER))
        self.assertIsNone(table.row(OTHER))  # nothing persisted: lifted with the list
        listed.clear()
        self.assertFalse(table.is_banned(OTHER))
        self.assertTrue(table.heard(OTHER))


if __name__ == "__main__":
    unittest.main()
