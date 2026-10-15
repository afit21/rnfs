"""L12-R: after the L12 fix, new inbox messages from senders not trusted came only from a node-wide token bucket (32,
then one a minute), so a single identity sending one message with an rnfs link a minute kept it empty: no other
stranger's message was kept (0 of 144 in a day, simulated).

Node B runs the lxmf feature on a stand-in network (tests/test_lxmf.py's FeatureTestCase), with a contact and a
settable clock. One sender floods; other strangers, each a new identity, send one message each. Theirs must be kept:
each sender first spends a small allowance of its own (SENDER_BURST, then one per SENDER_INTERVAL), and only then
the node-wide bucket.
"""
import os
import unittest

from rnfs.features import lxmf
from rnfs.features.lxmf import Incoming, Link
from tests import test_lxmf
from tests.support import FakeClock

ATTACKER = bytes([0xAA] * 16)
CONTACT = bytes([0xC0] * 16)
SENDER_BURST = 4  # rnfs.features.lxmf.SENDER_BURST: one sender's new records at once


def setUpModule():
    """Silence the node's log."""
    test_lxmf.setUpModule()


def tearDownModule():
    """Restore RNS's loglevel."""
    test_lxmf.tearDownModule()


class SenderAllowanceTest(test_lxmf.FeatureTestCase):
    """Node B's feature with one LXMF contact and a clock the tests move."""

    def setUp(self):
        """B's feature, its contact, and the clock."""
        super().setUp()
        self.feature = self.feature_node()
        self.b.feature("contacts").add("carol", CONTACT.hex(), kind="lxmf")
        self.clock = FakeClock(1000.0)
        self.feature.clock = self.clock
        self.uri = Link(self.cid).uri().encode()

    def send(self, source: bytes):
        """A new message with an rnfs link from source; its inbox id, or None if it wasn't kept."""
        return self.feature.received(Incoming(source, os.urandom(32), 1.7e9, b"", self.uri, {}, "direct"))

    def test_one_sender_cannot_empty_the_bucket_at_once(self):
        for _ in range(lxmf.UNTRUSTED_BURST):
            self.send(ATTACKER)
        kept = sum(self.send(os.urandom(16)) is not None for _ in range(10))
        # L12-R: fails until a sender spends an allowance of its own before the node-wide bucket
        self.assertEqual(kept, 10, f"one sender's burst left room for {kept} of 10 other strangers' messages")

    def test_steady_sender_does_not_starve_strangers(self):
        """The attacker sends one a minute, just as the bucket refills, for six hours; a new stranger writes every
        10 minutes, half a minute after the attacker."""
        for _ in range(lxmf.UNTRUSTED_BURST):
            self.send(ATTACKER)
        sent = kept = 0
        for minute in range(6 * 60):
            if minute % 10 == 5:
                self.clock.now += lxmf.UNTRUSTED_INTERVAL / 2
                sent += 1
                kept += self.send(os.urandom(16)) is not None
                self.clock.now += lxmf.UNTRUSTED_INTERVAL / 2
            else:
                self.clock.now += lxmf.UNTRUSTED_INTERVAL
            self.send(ATTACKER)
        # L12-R: fails until a sender spends an allowance of its own before the node-wide bucket
        self.assertEqual(kept, sent, f"kept {kept} of {sent} strangers' messages")
        self.assertIsNotNone(self.send(CONTACT))  # trusted senders aren't limited at all

    def test_allowance_per_sender(self):
        """SENDER_BURST at once, then one per SENDER_INTERVAL; a message refused by it takes nothing from the bucket."""
        accepted = sum(self.send(ATTACKER) is not None for _ in range(SENDER_BURST + 3))
        # L12-R: fails until a sender spends an allowance of its own before the node-wide bucket
        self.assertEqual(accepted, SENDER_BURST)
        self.assertEqual(lxmf.SENDER_BURST, SENDER_BURST)
        self.assertEqual(self.feature._untrusted_tokens, lxmf.UNTRUSTED_BURST - lxmf.SENDER_BURST)
        self.clock.now += lxmf.SENDER_INTERVAL
        self.assertIsNotNone(self.send(ATTACKER))
        self.assertIsNone(self.send(ATTACKER))
        # Senders' allowances are remembered for the most recent SENDER_ALLOWANCES senders only
        for _ in range(2 * lxmf.SENDER_ALLOWANCES):
            self.feature._untrusted_token(os.urandom(16))
        self.assertEqual(len(self.feature._sender_tokens), lxmf.SENDER_ALLOWANCES)


if __name__ == "__main__":
    unittest.main()
