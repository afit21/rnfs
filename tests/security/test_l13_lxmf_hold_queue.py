"""L13: messages from senders whose identity isn't known yet waited in one global queue (32 messages, 120 s each), so
32 forged messages claiming random senders crowded out a real first message, which LXMF has already recorded as
delivered and will not deliver again.

An LxmfNetwork with a stand-in router and feature (tests/test_lxmf.py's NetworkBoundsTest.network), Reticulum's path
requests and identity recall patched, and time.monotonic set by hand. The tests fill the queue with forgeries and
check what the fix must do: a message claiming a trusted sender (a contact or an auto-fetch sender) takes a slot
held for an unknown one; one claimed sender gets only HOLD_PER_SENDER slots; a message whose path request got no
answer within HOLD_PATH_WAIT is dropped early; and the path-request tokens go to trusted senders first.
"""
import os
import unittest
from unittest import mock

import RNS

from rnfs.features import lxmf
from tests import test_lxmf

CONTACT = bytes([0xC0] * 16)
T0 = 1000.0


def setUpModule():
    """Silence the node's log."""
    test_lxmf.setUpModule()


def tearDownModule():
    """Restore RNS's loglevel."""
    test_lxmf.tearDownModule()


class HoldQueueTest(unittest.TestCase):
    """The hold queue of an LxmfNetwork whose feature trusts CONTACT and accepts everyone."""

    def setUp(self):
        """The network, its path requests recorded, and the time at T0."""
        self.network = test_lxmf.NetworkBoundsTest.network(self)
        self.network.feature.accepts.return_value = True
        self.trusted = {CONTACT}
        self.network.feature.trusted.side_effect = lambda source: source in self.trusted
        self.requests = []
        self.now = T0
        for patch in (mock.patch.object(RNS.Transport, "request_path", side_effect=self.requests.append),
                      mock.patch.object(RNS.Identity, "recall", return_value=None),
                      mock.patch.object(lxmf.time, "monotonic", side_effect=lambda: self.now)):
            patch.start()
            self.addCleanup(patch.stop)

    def deliver(self, source: bytes, tag: bytes):
        """LXMF hands over a message claiming source, whose identity isn't known."""
        self.network._delivered(mock.Mock(signature_validated=False,
                                          unverified_reason=self.network.LXMF.LXMessage.SOURCE_UNKNOWN,
                                          source_hash=source, packed=os.urandom(16) + source + tag, method=2))

    def tags(self) -> list[bytes]:
        """The tags of the held messages, in queue order."""
        return [packed[2 * lxmf.ID_LENGTH:] for _, packed, _ in self.network._held]

    def fill_with_forgeries(self, count: int = lxmf.HOLD_LIMIT):
        """count forged messages claiming random senders."""
        for _ in range(count):
            self.deliver(os.urandom(16), b"forged")
        self.assertEqual(len(self.network._held), count)

    def test_contact_takes_a_slot_held_for_an_unknown_sender(self):
        self.fill_with_forgeries()
        self.deliver(CONTACT, b"REAL")
        # L13: fails until a contact or auto-fetch sender can take a slot held by an unknown sender
        self.assertIn(b"REAL", self.tags(), "a full queue of forgeries dropped a contact's first message")
        self.assertEqual(len(self.network._held), lxmf.HOLD_LIMIT)
        # Trusted senders push out every forgery but never each other, and a full queue of them refuses strangers
        for i in range(lxmf.HOLD_LIMIT):
            self.trusted.add(bytes([i]) * 16)
            self.deliver(bytes([i]) * 16, b"trusted")
        self.assertEqual(self.tags(), [b"REAL"] + [b"trusted"] * (lxmf.HOLD_LIMIT - 1))
        self.deliver(os.urandom(16), b"late")
        self.assertNotIn(b"late", self.tags())

    def test_one_claimed_sender_gets_only_a_few_slots(self):
        spoofed = os.urandom(16)
        for _ in range(lxmf.HOLD_LIMIT + 8):
            self.deliver(spoofed, b"forged")
        self.deliver(os.urandom(16), b"REAL")
        # L13: fails until the queue limits held messages per claimed sender as well as in total
        self.assertIn(b"REAL", self.tags(), "one claimed sender filled the whole queue")
        self.assertEqual(self.tags().count(b"forged"), lxmf.HOLD_PER_SENDER)

    def test_unanswered_path_request_frees_the_slot_early(self):
        self.fill_with_forgeries()
        asked = list(self.requests)
        self.assertEqual(len(asked), lxmf.PATH_REQUEST_BURST)
        self.now = T0 + 60.0  # well before HOLD_TIME, long after any path response would have come
        self.network._retry_held()
        held = {packed[lxmf.ID_LENGTH:2 * lxmf.ID_LENGTH] for _, packed, _ in self.network._held}
        # L13: fails until a held message whose path request got no answer is dropped before HOLD_TIME
        self.assertFalse(held & set(asked), "held messages whose path requests went unanswered still hold slots")
        # The senders asked only now (tokens came back) keep their slots until their own wait is over
        self.assertTrue(set(self.requests[len(asked):]) <= held)
        self.assertEqual(len(held), lxmf.HOLD_LIMIT - len(asked))
        self.deliver(os.urandom(16), b"REAL")
        self.assertIn(b"REAL", self.tags())

    def test_answered_path_request_validates_after_the_early_wait(self):
        # The early drop is only for unanswered requests: a sender that becomes known is validated as before
        self.deliver(CONTACT, b"REAL")
        self.now = T0 + lxmf.HOLD_PATH_WAIT + 1
        message = mock.Mock(signature_validated=True)
        with mock.patch.object(RNS.Identity, "recall", return_value=object()), \
                mock.patch.object(self.network.LXMF.LXMessage, "unpack_from_bytes", return_value=message), \
                mock.patch.object(self.network, "_incoming", return_value="incoming"):
            self.network._retry_held()
        self.network.feature.received.assert_called_once_with("incoming")
        self.assertEqual(self.network._held, [])

    def test_path_request_tokens_go_to_trusted_senders_first(self):
        self.fill_with_forgeries(lxmf.HOLD_LIMIT - 1)
        self.deliver(CONTACT, b"REAL")
        self.assertNotIn(CONTACT, self.requests)  # the token bucket was empty
        self.now = T0 + lxmf.PATH_REQUEST_INTERVAL  # one token back
        self.network._retry_held()
        # L13: fails until held messages from trusted senders get the path-request tokens before unknown ones
        self.assertEqual(self.requests[-1], CONTACT, "the token went to a forged sender ahead of a contact")


if __name__ == "__main__":
    unittest.main()
