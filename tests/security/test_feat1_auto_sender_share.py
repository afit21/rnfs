"""FEAT-1: one auto-fetch sender could push out another's pinned messages.

A record holding auto-fetched pins (or queued to) is protected: it is dropped for other senders' messages only when
every record is protected, and then the oldest record of anyone went. Senders in lxmf_auto_fetch_from had no
per-sender cap, and re-linking a CID the sender already pins records "pinned" without using quota. So an auto-fetch
sender sending about 500 messages re-linking one tiny CID filled the inbox with protected records, and each further
message dropped the oldest record overall: another auto-fetch sender's message, whose `lxmf:` pin `_release` then
released. The spec's threat table promises "auto-fetched pins never pushed out by others' messages".

Node B runs the lxmf feature on a stand-in network (tests/test_lxmf.py's FeatureTestCase) with VICTIM and ATTACKER
both in lxmf_auto_fetch_from. VICTIM sends one link, which B fetches and pins; ATTACKER sends 600 re-links of one
small file, each fetched (pinned at no quota) before the next. Fixed: each auto-fetch sender's records, protected
ones included, are limited to 1/n of the inbox's record and byte limits (n auto-fetch senders); over its share a
sender's own records go, unprotected first, then those whose pins another of its records also holds, then the
oldest; and when every other record is protected, only the incoming sender's own record may go (or the message is
refused), never another sender's.
"""
import hashlib
import io
import unittest

from rnfs.content.cid import Cid
from rnfs.features import lxmf
from rnfs.features.lxmf import Incoming, Link, MessageLog
from tests import test_lxmf
from tests.test_lxmf import A_ID, OTHER, SENDER

VICTIM, ATTACKER = OTHER, SENDER
RELINKS = 600


def setUpModule():
    """Silence the node's log."""
    test_lxmf.setUpModule()


def tearDownModule():
    """Restore RNS's loglevel."""
    test_lxmf.tearDownModule()


class AutoSenderShareTest(test_lxmf.FeatureTestCase):
    """VICTIM and ATTACKER in lxmf_auto_fetch_from; VICTIM's link fetched and pinned first."""

    def setUp(self):
        """B's feature, VICTIM's pinned message, and the small file ATTACKER re-links."""
        super().setUp()
        self.feature = self.feature_node(lxmf_auto_fetch_from=[VICTIM.hex(), ATTACKER.hex()])
        self.victim = self.feature.received(self.link_message(source=VICTIM, number=1))
        self.feature.process_auto_fetches()
        self.victim_cid = Cid.parse(self.a.info(f"{self.cid}/file.txt")["cid"])
        self.assertEqual(self.feature.inbox.get(self.victim)["auto"][0]["state"], "pinned")
        small = Cid.parse(self.a.add_stream(io.BytesIO(b"x" * 47))["cid"])
        self.small = Link(small, from_node=A_ID)
        self.sent = 0

    def relink(self, text: bytes = b"") -> int | None:
        """ATTACKER sends the small file's link again (with text), and B auto-fetches it; the inbox id."""
        self.sent += 1
        content = text + b"\n" + self.small.uri().encode()
        message = Incoming(ATTACKER, hashlib.sha256(b"relink %d" % self.sent).digest(), 1.7e9, b"", content,
                           lxmf.encode_field([self.small]), "direct")
        entry_id = self.feature.received(message)
        self.feature.process_auto_fetches()
        return entry_id

    def attacker_records(self) -> list[dict]:
        """ATTACKER's inbox records."""
        return [e for e in self.feature.inbox_list() if e["from"] == ATTACKER.hex()]

    def test_relinks_keep_the_victims_record_and_pin(self):
        for _ in range(RELINKS):
            self.relink()
        # FEAT-1: fails until one auto-fetch sender can't push out another's protected records
        self.assertIsNotNone(self.feature.inbox.get(self.victim),
                             "another auto-fetch sender's re-links pushed out the victim's pinned message")
        self.assertIn(f"lxmf:{VICTIM.hex()}", self.b.storage.pins().get(self.victim_cid, set()),
                      "the victim's auto-fetched pin was released")
        # The attacker keeps its share of the records, and its pin
        self.assertEqual(len(self.attacker_records()), lxmf.INBOX_LIMIT // 2)
        self.assertIn(f"lxmf:{ATTACKER.hex()}", self.b.storage.pins()[self.small.cid])
        self.assertTrue(all(e["auto"][0]["state"] == "pinned" for e in self.attacker_records()))

    def test_share_holds_by_bytes(self):
        self.feature.inbox.max_bytes = 200_000
        for _ in range(100):
            self.relink(text=b"y" * 3000)
        share = self.feature.inbox.max_bytes // 2
        used = sum(lxmf._json_bytes(e) for e in self.attacker_records())
        self.assertLessEqual(used, share)
        self.assertGreater(used, share - 2 * lxmf.MAX_ENTRY_BYTES)  # it does get its share
        self.assertIsNotNone(self.feature.inbox.get(self.victim))

    def test_share_counts_the_auto_fetch_senders(self):
        # With a third auto-fetch sender, each gets a third
        self.feature.auto_fetch_from.add(bytes([0x54] * 16).hex())
        self.feature.inbox.limit = 30
        for _ in range(40):
            self.relink()
        self.assertEqual(len(self.attacker_records()), 10)
        self.assertIsNotNone(self.feature.inbox.get(self.victim))


class SharedMessageLogTest(unittest.TestCase):
    """MessageLog.add's share and the choice of victim when every other record is protected."""

    def setUp(self):
        """An inbox log whose records with "cids" hold those CIDs (protected)."""
        import tempfile
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.log = MessageLog(tmp.name, lxmf.INBOX, 100, protect=lambda e: bool(e.get("cids")),
                              holds=lambda e: set(e.get("cids") or ()))
        self.addCleanup(self.log.close)

    def add(self, sender: str, cids=(), share=None) -> int | None:
        """Add a record from sender holding cids; its id, or None if refused."""
        return self.log.add({"from": sender, "cids": list(cids)}, share=share)[0]

    def senders(self) -> list[tuple[str, tuple]]:
        """(sender, cids) of each record, oldest first."""
        return [(e["from"], tuple(e["cids"])) for e in self.log.entries()]

    def test_over_its_share_a_sender_drops_its_own_in_order(self):
        self.add("v", ["v1"])
        self.add("a", ["c1"])  # protected, the only holder of c1
        self.add("a", ["c2"])
        self.add("a", ["c2"])  # c2 again: the older c2 record holds nothing the other doesn't
        self.add("a")  # unprotected
        self.add("a", ["c3"], share=(4, 10 ** 6))  # five records of "a": the unprotected one goes first
        self.assertEqual(self.senders(), [("v", ("v1",)), ("a", ("c1",)), ("a", ("c2",)), ("a", ("c2",)),
                                          ("a", ("c3",))])
        self.add("a", ["c4"], share=(4, 10 ** 6))  # then the oldest whose CIDs another of its records holds
        self.assertEqual(self.senders(), [("v", ("v1",)), ("a", ("c1",)), ("a", ("c2",)), ("a", ("c3",)),
                                          ("a", ("c4",))])
        self.add("a", ["c5"], share=(4, 10 ** 6))  # then its oldest
        self.assertEqual(self.senders(), [("v", ("v1",)), ("a", ("c2",)), ("a", ("c3",)), ("a", ("c4",)),
                                          ("a", ("c5",))])

    def test_when_all_are_protected_only_the_senders_own_may_go(self):
        self.log.limit = 2
        self.add("v", ["v1"])
        self.add("v", ["v2"])
        # Another sender's protected record is never chosen: the new record is refused
        self.assertIsNone(self.add("a", ["c1"]))
        self.assertEqual(self.senders(), [("v", ("v1",)), ("v", ("v2",))])
        # The same sender's new record pushes out its own oldest
        self.assertIsNotNone(self.add("v", ["v3"]))
        self.assertEqual(self.senders(), [("v", ("v2",)), ("v", ("v3",))])


if __name__ == "__main__":
    unittest.main()
