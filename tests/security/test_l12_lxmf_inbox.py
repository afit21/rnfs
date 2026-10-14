"""L12: with lxmf_accept_from empty (the default), many cheap LXMF identities could push every unprotected record
out of the 500-message inbox: each new sender got its own 50 records.

Node B runs the lxmf feature on a stand-in network (tests/test_lxmf.py's FeatureTestCase and FakeLxmf). A contact
(an LXMF contact in the contacts feature) and an auto-fetch sender whose fetch failed each leave a record; then
messages with rnfs links arrive from more than INBOX_LIMIT fresh identities, slowly (the clock moves an hour
between them) so a rate limit can't be what saves the records. Both records must still be there: records from
trusted senders (contacts, lxmf_auto_fetch_from, lxmf_accept_from) are dropped only for each other's. Further tests
show a fast flood of new identities is rate limited (a token bucket shared by senders not trusted), so it can't
even cycle out older records from senders not trusted, while trusted senders aren't limited.
"""
import os
import unittest

from rnfs.features import lxmf
from rnfs.features.lxmf import Link
from tests import test_lxmf
from tests.support import FakeClock
from tests.test_lxmf import SENDER, message

CONTACT = bytes([0xC0] * 16)
EARLY = bytes([0xE0] * 16)  # a sender not trusted, heard before the flood


def setUpModule():
    """Silence the node's log."""
    test_lxmf.setUpModule()


def tearDownModule():
    """Restore RNS's loglevel."""
    test_lxmf.tearDownModule()


class InboxFloodTest(test_lxmf.FeatureTestCase):
    """Node B with one LXMF contact, SENDER in lxmf_auto_fetch_from, and a settable clock."""

    def setUp(self):
        """B's feature, its contact, and a clock the tests move."""
        super().setUp()
        self.feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        self.b.feature("contacts").add("carol", CONTACT.hex(), kind="lxmf")
        self.clock = FakeClock(1000.0)
        self.feature.clock = self.clock  # the fixed code's rate limit reads it; the old code ignores it
        self.junk = Link(self.cid).uri().encode()
        self.number = 0

    def from_new_identity(self, step: float = 0.0):
        """A message with an rnfs link from a fresh identity, step seconds after the last."""
        self.clock.now += step
        self.number += 1
        return self.feature.received(message(os.urandom(16), self.junk, number=self.number % 256))

    def test_slow_flood_of_new_identities_keeps_trusted_records(self):
        contact = self.feature.received(self.link_message(source=CONTACT, number=1))
        auto = self.feature.received(self.link_message(source=SENDER, number=2))
        self.feature._set_auto(auto, 0, {"state": "failed", "error": "unreachable"})  # no longer holds pins
        for _ in range(lxmf.INBOX_LIMIT + 10):
            self.from_new_identity(step=3600.0)
        self.assertEqual(len(self.feature.inbox), lxmf.INBOX_LIMIT)
        # L12: fails until records from contacts and auto-fetch senders are dropped only for each other's
        self.assertIsNotNone(self.feature.inbox.get(contact),
                             "a flood of new identities pushed out a contact's message")
        self.assertIsNotNone(self.feature.inbox.get(auto),
                             "a flood of new identities pushed out an auto-fetch sender's failed message")

    def test_trusted_records_are_dropped_only_for_each_other(self):
        self.feature.inbox.limit = 3
        u1 = self.from_new_identity()
        c1 = self.feature.received(self.link_message(source=CONTACT, number=1))
        u2 = self.from_new_identity()
        # A contact's message makes room by dropping records from senders not trusted, oldest first
        c2 = self.feature.received(self.link_message(source=CONTACT, number=2))
        c3 = self.feature.received(self.link_message(source=CONTACT, number=3))
        self.assertEqual([e["id"] for e in self.feature.inbox_list()], [c1, c2, c3],
                         f"a contact's message dropped another contact's (unknown senders' were {u1}, {u2})")
        # The inbox is all contacts' now: a sender not trusted gets no room; the next contact's drops the oldest
        self.assertIsNone(self.from_new_identity(step=3600.0))
        c4 = self.feature.received(self.link_message(source=CONTACT, number=4))
        self.assertEqual([e["id"] for e in self.feature.inbox_list()], [c2, c3, c4])

    def test_fast_flood_of_new_identities_is_rate_limited(self):
        early = self.feature.received(self.link_message(source=EARLY, number=1))
        accepted = sum(self.from_new_identity() is not None for _ in range(lxmf.INBOX_LIMIT * 2))
        # L12: fails until records from senders not trusted are rate limited (a shared token bucket)
        self.assertLess(accepted, 100, f"{accepted} records from new identities were kept in one instant")
        self.assertIsNotNone(self.feature.inbox.get(early), "the flood cycled out an earlier sender's message")
        # Trusted senders aren't limited, and tokens come back with time
        self.assertIsNotNone(self.feature.received(self.link_message(source=CONTACT, number=1)))
        self.assertIsNotNone(self.feature.received(self.link_message(source=SENDER, number=2)))
        self.assertIsNone(self.from_new_identity())
        self.assertIsNotNone(self.from_new_identity(step=lxmf.UNTRUSTED_INTERVAL))
        self.assertIsNone(self.from_new_identity())

    def test_accept_from_senders_are_not_rate_limited(self):
        # A user who lists senders in lxmf_accept_from named them: they aren't limited like strangers
        self.feature.accept_from = {CONTACT.hex(), EARLY.hex()}
        for number in range(lxmf.INBOX_PER_SENDER + 5):
            self.feature.received(self.link_message(source=EARLY, number=number))
        senders = [e["from"] for e in self.feature.inbox_list()]
        self.assertEqual(senders.count(EARLY.hex()), lxmf.INBOX_PER_SENDER)  # the per-sender cap still applies
        self.assertIsNone(self.from_new_identity())  # not in lxmf_accept_from: ignored, as before

    def test_trust_follows_the_contacts_feature(self):
        # A contact added later protects what is already in the inbox; removing the contact ends that
        self.feature.inbox.limit = 2
        early = self.feature.received(self.link_message(source=EARLY, number=1))
        contacts = self.b.feature("contacts")
        contacts.add("erin", EARLY.hex(), kind="lxmf")
        for _ in range(3):
            self.assertIsNotNone(self.from_new_identity(step=3600.0))  # each drops the last stranger's record
        self.assertIsNotNone(self.feature.inbox.get(early))
        contacts.remove("erin")
        self.assertIsNotNone(self.from_new_identity(step=3600.0))
        self.assertIsNone(self.feature.inbox.get(early))

    def test_contacts_lookup_by_address(self):
        # ContactsFeature.has, which the inbox asks about every record it may drop
        from rnfs.features.contacts import ContactsFeature
        book = self.b.feature("contacts")
        book.add("carol2", CONTACT.hex(), kind="lxmf")
        book.add("node", EARLY.hex(), kind="node")
        self.assertTrue(book.has(CONTACT, "lxmf"))
        self.assertFalse(book.has(EARLY, "lxmf"))  # a node contact isn't an LXMF one
        book.remove("carol")
        self.assertTrue(book.has(CONTACT, "lxmf"))  # carol2 still names it
        book.add("carol2", EARLY.hex(), kind="lxmf")  # replaced
        self.assertFalse(book.has(CONTACT, "lxmf"))
        self.assertTrue(ContactsFeature(self.b).has(EARLY, "lxmf"))  # loaded from contacts.json


if __name__ == "__main__":
    unittest.main()
