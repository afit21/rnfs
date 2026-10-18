"""FEAT-2: display names could fake a trusted address in `rnfs inbox`.

A sender's display name comes from its own announce. `rnfs inbox` printed it first and unmarked, followed by the
address: up to 64 printable characters fit `alice <TRUSTED_ADDRESS>` plus padding, pushing the real address off
to the right, and nothing said whether the sender was a contact or trusted at all:

    #7  2023-11-15 08:43  alice <51515151515151515151515151515151>                         <deaddead…>  direct

Node B runs the lxmf feature on a stand-in network (tests/test_lxmf.py's FeatureTestCase). Fixed: `inbox_list()`
(GET /api/lxmf/inbox) adds computed `trusted` and `contact` (the LXMF contact's nick) fields, and each message's
line gives the sender's address first, then `untrusted`, `trusted` or `contact <nick>` (`?` from an older daemon
that doesn't say), then the display name last, quoted and escaped with fsutil.printable_label (FEAT-6).
"""
import io
import re
import unittest
from types import SimpleNamespace
from unittest import mock

from rnfs.features import lxmf
from tests import test_lxmf
from tests.test_lxmf import OTHER, SENDER, message

ATTACKER = bytes([0xDE, 0xAD] * 8)
CONTACT = bytes([0xC0] * 16)
HEADER = re.compile(r"#\d+  \S+ \S+  <([0-9a-f]{32})>  (untrusted|trusted|contact \S+|\?)  (\S+)(?:  (\".*\"))?$")


def setUpModule():
    """Silence the node's log."""
    test_lxmf.setUpModule()


def tearDownModule():
    """Restore RNS's loglevel."""
    test_lxmf.tearDownModule()


def listing(entries: list[dict]) -> list[str]:
    """The message lines (not links or text) `rnfs inbox` prints for entries, as the API returned them."""
    client = mock.Mock()
    client.get.return_value = {"messages": entries}
    out = io.StringIO()
    with mock.patch("rnfs.cli.client", return_value=client), mock.patch("sys.stdout", out):
        lxmf._run_inbox(SimpleNamespace())
    return [line for line in out.getvalue().splitlines() if line.startswith("#")]


class InboxSenderLineTest(test_lxmf.FeatureTestCase):
    """B with SENDER in lxmf_auto_fetch_from and CONTACT as LXMF contact carol."""

    def setUp(self):
        """B's feature and its contact."""
        super().setUp()
        self.feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        self.b.feature("contacts").add("carol", CONTACT.hex(), kind="lxmf")
        self.uri = lxmf.Link(self.cid).uri().encode()

    def receive(self, source: bytes, display_name: str | None, number: int = 1) -> int:
        """A message with an rnfs link from source, announcing display_name; its inbox id."""
        return self.feature.received(message(source, self.uri, number=number, display_name=display_name))

    def test_display_name_cannot_pose_as_a_trusted_address(self):
        fake = f"alice <{SENDER.hex()}>" + " " * 20
        self.receive(ATTACKER, fake[:lxmf.MAX_DISPLAY_NAME_CHARS])
        [line] = listing(self.feature.inbox_list())
        # FEAT-2: fails until the line gives the real address first and marks the sender
        found = HEADER.fullmatch(line)
        self.assertIsNotNone(found, f"the sender's address doesn't come first, marked: {line!r}")
        self.assertEqual(found[1], ATTACKER.hex())
        self.assertEqual(found[2], "untrusted")
        self.assertEqual(found[4], f'"alice <{SENDER.hex()}>' + " " * 20 + '"')
        self.assertLess(line.index(ATTACKER.hex()), line.index(SENDER.hex()))

    def test_api_says_who_is_trusted_or_a_contact(self):
        stranger = self.receive(ATTACKER, "bob", 1)
        trusted = self.receive(SENDER, None, 2)
        contact = self.receive(CONTACT, "Carol", 3)
        by_id = {e["id"]: e for e in self.feature.inbox_list()}
        self.assertEqual((by_id[stranger]["trusted"], by_id[stranger]["contact"]), (False, None))
        self.assertEqual((by_id[trusted]["trusted"], by_id[trusted]["contact"]), (True, None))
        self.assertEqual((by_id[contact]["trusted"], by_id[contact]["contact"]), (True, "carol"))
        lines = listing(self.feature.inbox_list())
        self.assertEqual([HEADER.fullmatch(line)[2] for line in lines], ["untrusted", "trusted", "contact carol"])
        self.assertIsNone(HEADER.fullmatch(lines[1])[4])  # no display name: nothing quoted
        self.assertEqual(HEADER.fullmatch(lines[2])[4], '"Carol"')
        # Computed when listed: trust follows the contacts and settings now, and isn't saved in the record
        self.b.feature("contacts").remove("carol")
        self.assertEqual(self.feature.inbox_list()[2]["trusted"], False)
        self.assertNotIn("trusted", self.feature.inbox.get(contact))

    def test_older_daemon_shows_a_question_mark(self):
        self.receive(ATTACKER, "bob")
        entries = self.feature.inbox_list()
        for entry in entries:
            del entry["trusted"], entry["contact"]
        self.assertEqual(HEADER.fullmatch(listing(entries)[0])[2], "?")

    def test_display_name_is_escaped_as_a_label(self):
        # A blank filler, a combining mark after it, quotes and a spelled-out escape
        self.receive(ATTACKER, 'a\u3164\u0301"  trusted  "\\x1b')
        [line] = listing(self.feature.inbox_list())
        self.assertEqual(HEADER.fullmatch(line)[4], '"a\\u3164\\u0301\\"  trusted  \\"\\\\x1b"')

    def test_untrusted_sender_with_an_address_for_a_name(self):
        # A name that is nothing but another address still comes after the real one, quoted
        self.receive(OTHER, f"<{CONTACT.hex()}>")
        [line] = listing(self.feature.inbox_list())
        found = HEADER.fullmatch(line)
        self.assertEqual((found[1], found[2], found[4]), (OTHER.hex(), "untrusted", f'"<{CONTACT.hex()}>"'))


class NickOfTest(test_lxmf.FeatureTestCase):
    """ContactsFeature.nick_of, which the inbox listing asks for each sender."""

    def test_nick_of(self):
        self.feature_node()
        book = self.b.feature("contacts")
        book.add("zed", CONTACT.hex(), kind="lxmf")
        book.add("carol", CONTACT.hex(), kind="lxmf")
        book.add("node", OTHER.hex(), kind="node")
        self.assertEqual(book.nick_of(CONTACT, "lxmf"), "carol")  # the first by nick when several name it
        self.assertIsNone(book.nick_of(OTHER, "lxmf"))  # a node contact isn't an LXMF one
        self.assertEqual(book.nick_of(OTHER, "node"), "node")
        book.remove("carol")
        self.assertEqual(book.nick_of(CONTACT, "lxmf"), "zed")
        book.add("zed", OTHER.hex(), kind="lxmf")  # replaced
        self.assertIsNone(book.nick_of(CONTACT, "lxmf"))
        self.assertFalse(book.has(CONTACT, "lxmf"))
        self.assertTrue(book.has(OTHER, "lxmf"))


if __name__ == "__main__":
    unittest.main()
