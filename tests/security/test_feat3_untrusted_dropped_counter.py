"""FEAT-3: many identities starve every stranger's messages.

The L12-R allowance limits each sender not trusted, but strangers share one node-wide bucket (32 at once, then one a
minute), and an LXMF identity costs one announce. One message a minute, each from a fresh identity, keeps the
bucket empty, so every other stranger's message is dropped; trusted senders (contacts, lxmf_accept_from,
lxmf_auto_fetch_from) are unaffected. Stopping it would take LXMF stamps (proof of work), which change the announce
and refuse senders that haven't heard it: deferred, as an interop break. The limit is documented (spec section 10)
and counted: `status()["untrusted_dropped"]` is the number of untrusted senders' messages the rate limits dropped
since the start, so an app can suggest adding the people the user expects messages from as contacts.

Node B runs the lxmf feature on a stand-in network (tests/test_lxmf.py's FeatureTestCase) with one LXMF contact
and a clock the test moves.
"""
import os
import unittest

from rnfs.features import lxmf
from rnfs.features.lxmf import Link
from tests import test_lxmf
from tests.support import FakeClock
from tests.test_lxmf import message

CONTACT = bytes([0xC0] * 16)
STRANGER = bytes([0x5E] * 16)  # a sender not trusted, with its own allowance left


def setUpModule():
    """Silence the node's log."""
    test_lxmf.setUpModule()


def tearDownModule():
    """Restore RNS's loglevel."""
    test_lxmf.tearDownModule()


class StrangerBucketTest(test_lxmf.FeatureTestCase):
    """B with contact CONTACT; fresh identities keep the strangers' bucket empty."""

    def setUp(self):
        """B's feature, its contact and its clock."""
        super().setUp()
        self.feature = self.feature_node()
        self.b.feature("contacts").add("carol", CONTACT.hex(), kind="lxmf")
        self.clock = FakeClock(1000.0)
        self.feature.clock = self.clock
        self.junk = Link(self.cid).uri().encode()
        self.number = 0

    def send(self, source: bytes) -> int | None:
        """A message with an rnfs link from source; its inbox id, or None if it wasn't kept."""
        self.number += 1
        return self.feature.received(message(source, self.junk, number=self.number % 256))

    def test_drops_are_counted_and_contacts_are_unaffected(self):
        # FEAT-3: fails until the status has the counter
        self.assertEqual(self.b.status()["untrusted_dropped"], 0)
        for _ in range(lxmf.UNTRUSTED_BURST):
            self.assertIsNotNone(self.send(os.urandom(16)))
        dropped = 0
        for _ in range(60):  # an hour: a fresh identity takes each token as it comes back
            self.clock.now += lxmf.UNTRUSTED_INTERVAL
            self.assertIsNotNone(self.send(os.urandom(16)))
            dropped += self.send(STRANGER) is None
        self.assertEqual(dropped, 60, "a stranger got a message through the emptied bucket")
        self.assertEqual(self.b.status()["untrusted_dropped"], 60)
        self.assertEqual(self.feature.status()["untrusted_dropped"], 60)
        # The per-sender allowance counts too
        self.clock.now += 10 * lxmf.UNTRUSTED_INTERVAL
        sender = os.urandom(16)
        kept = [self.send(sender) is not None for _ in range(lxmf.SENDER_BURST + 1)]
        self.assertEqual(kept, [True] * lxmf.SENDER_BURST + [False])
        self.assertEqual(self.b.status()["untrusted_dropped"], 61)
        # Contacts are unaffected, and not counted
        self.assertIsNotNone(self.send(CONTACT))
        self.assertEqual(self.b.status()["untrusted_dropped"], 61)


if __name__ == "__main__":
    unittest.main()
