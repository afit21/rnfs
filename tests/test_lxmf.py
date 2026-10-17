"""Tests for the LXMF feature (rnfs/features/lxmf.py): rnfs: URIs, the message field, the inbox and outbox,
receiving and auto-fetch policy, sending with fallback, and running without LXMF. No Reticulum: offline nodes
and a stand-in for LxmfNetwork."""
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

import RNS

from rnfs import statedb
from rnfs.config import Config
from rnfs.content import storage as storage_module
from rnfs.content.cid import Cid
from rnfs.content.storage import USER
from rnfs.errors import NotFound
from rnfs.features import lxmf
from rnfs.features.lxmf import FIELD_CUSTOM_DATA, FIELD_CUSTOM_TYPE, Incoming, Link, MessageLog
from rnfs.node import RnfsNode

from tests.support import FakeClock, failing_commits, state_execute, state_rows, wait_for, write_legacy
from tests.test_names import FakeNetwork
from tests.test_node import NetClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
A_ID, B_ID = bytes([0xA] * 16), bytes([0xB] * 16)
SENDER, OTHER, RECIPIENT = bytes([0x51] * 16), bytes([0x52] * 16), bytes([0x53] * 16)
CID = Cid.for_block("raw", b"hello")
NAME = bytes(range(16))

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node's log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class UriTest(unittest.TestCase):
    """rnfs: URIs: formatting, parsing and rejection."""

    def test_round_trips(self):
        links = [Link(CID), Link(CID, path=("dir", "file.txt")), Link(CID, from_node=A_ID),
                 Link(CID, path=("a b", "100%", "é?#&=+/x"[:-2], "~._-"), from_node=B_ID),
                 Link(name=NAME), Link(name=NAME, path=("docs", "ünïcode ✓.pdf"), from_node=A_ID)]
        for link in links:
            uri = lxmf.format_uri(link)
            self.assertTrue(uri.isascii() and " " not in uri, uri)
            self.assertEqual(lxmf.parse_uri(uri), link, uri)
            self.assertEqual(lxmf.format_uri(lxmf.parse_uri(uri)), uri)

    def test_canonical_form(self):
        self.assertEqual(lxmf.format_uri(Link(CID, path=("a b", "é"), from_node=A_ID)),
                         f"rnfs:{CID}/a%20b/%C3%A9?from={A_ID.hex()}")
        self.assertEqual(lxmf.format_uri(Link(name=NAME, path=("x",))), f"rnfs:name/{NAME.hex()}/x")

    def test_lenient_but_equivalent_forms(self):
        expected = Link(CID, path=("dir", "a b"), from_node=A_ID)
        for text in (f"RNFS:{CID}/dir/a%20b?from={A_ID.hex()}", f"rnfs:{CID}/dir/a%20b?from={A_ID.hex().upper()}",
                     f"rnfs:{CID}/%64ir/a%20b?from={A_ID.hex()}", f"rnfs:{CID}/dir/a%20b/?from={A_ID.hex()}",
                     f"rnfs:{CID}/dir/a%20b?x=1&from={A_ID.hex()}&y="):
            self.assertEqual(lxmf.parse_uri(text), expected, text)
        self.assertEqual(lxmf.parse_uri(f"rnfs:name/{NAME.hex().upper()}").name, NAME)
        self.assertEqual(lxmf.parse_uri(f"rnfs:{CID}/a:b@c!$&'()*+,;=").path, ("a:b@c!$&'()*+,;=",))

    def test_rejects_malformed(self):
        bad = ["", "rnfs:", "http://x", f"ipfs:{CID}", "rnfs:notacid", f"rnfs:{str(CID).upper()}",
               f"rnfs:{CID}x", f"rnfs:{CID}//a", f"rnfs:/{CID}", f"rnfs:{CID}/a%2Fb", f"rnfs:{CID}/a%00",
               f"rnfs:{CID}/..", f"rnfs:{CID}/.", f"rnfs:{CID}/%2E%2E", f"rnfs:{CID}/a b", f"rnfs:{CID}/é",
               f"rnfs:{CID}/%FF", f"rnfs:{CID}/%C3", f"rnfs:{CID}/%zz", f"rnfs:{CID}/a%", f"rnfs:{CID}#frag",
               f"rnfs:{CID}/a\x1b", f"rnfs:{CID}?", f"rnfs:{CID}?from", f"rnfs:{CID}?=x", f"rnfs:{CID}?from=zz",
               f"rnfs:{CID}?from={A_ID.hex()[:-2]}", f"rnfs:{CID}?from={A_ID.hex()}&from={B_ID.hex()}",
               f"rnfs:{CID}?from={A_ID.hex()}&&", f"rnfs:{CID}?a=b?c", "rnfs:name", "rnfs:name/", "rnfs:name/abc",
               f"rnfs:name/{NAME.hex()}x", f"rnfs:{CID}/" + "a/" * 65 + "a", f"rnfs:{CID}/" + "a" * 256,
               f"rnfs:{CID}/" + "%41" * 1400, None, 42, b"rnfs:x"]
        for text in bad:
            with self.assertRaises(ValueError, msg=repr(text)):
                lxmf.parse_uri(text)

    def test_parse_target(self):
        self.assertEqual(lxmf.parse_target(f"{CID}/dir/a b.txt"), Link(CID, path=("dir", "a b.txt")))
        self.assertEqual(lxmf.parse_target(f"name/{NAME.hex()}/x"), Link(name=NAME, path=("x",)))
        self.assertEqual(lxmf.parse_target(f"rnfs:{CID}?from={A_ID.hex()}"), Link(CID, from_node=A_ID))
        for text in ("", f"{CID}/a?b", f"{CID}/50%", f"{CID}//x", "nonsense", None):
            with self.assertRaises(ValueError, msg=repr(text)):
                lxmf.parse_target(text)

    def test_find_links_in_text(self):
        first = Link(CID, path=("a.txt",), from_node=A_ID)
        second = Link(name=NAME)
        text = (f"Here: {first.uri()}. Also ({second.uri()}), again <{first.uri()}> and a broken rnfs:bafy "
                f"and notrnfs:{CID} and xrnfs:{CID}")
        self.assertEqual(lxmf.find_links(text), [first, second])
        self.assertEqual(lxmf.find_links(None), [])
        many = " ".join(Link(CID, path=(f"f{i}",)).uri() for i in range(20))
        self.assertEqual(len(lxmf.find_links(many)), lxmf.MAX_LINKS)

    def test_query_values_may_hold_equals(self):
        self.assertEqual(lxmf.parse_uri(f"rnfs:{CID}?x=a=b&from={A_ID.hex()}"), Link(CID, from_node=A_ID))
        with self.assertRaises(ValueError):
            lxmf.parse_uri(f"rnfs:{CID}?from={A_ID.hex()}=x")

    def test_links_on_lines_of_their_own_are_read_whole(self):
        # A trailing "." is written unencoded (and "." or "..." is a valid name): only a link sharing its line
        # loses trailing punctuation
        for name in ("file.", "...", "a!"):
            link = Link(CID, path=("dir", name), from_node=A_ID)
            self.assertEqual(lxmf.find_links(lxmf.message_text("look", [link])), [link], name)
            self.assertEqual(lxmf.find_links(f"look:\r\n  {link.uri()}  \nbye"), [link], name)
        self.assertEqual(lxmf.find_links(f"rnfs:{CID}/dir/it's"), [Link(CID, path=("dir", "it's"))])
        self.assertEqual(lxmf.find_links(f"see rnfs:{CID}/a.txt."), [Link(CID, path=("a.txt",))])
        self.assertEqual(lxmf.find_links(f"rnfs:{CID}/a.txt. and more"), [Link(CID, path=("a.txt",))])
        self.assertEqual(lxmf.find_links(f"rnfs:{CID}/a b."), [Link(CID, path=("a",))])

    def test_message_text(self):
        link = Link(CID, from_node=A_ID)
        self.assertEqual(lxmf.message_text("  hi there ", [link]), f"hi there\n\n{link.uri()}")
        self.assertEqual(lxmf.message_text(None, [link]), link.uri())
        self.assertEqual(lxmf.find_links(lxmf.message_text("x", [link])), [link])

    def test_link_json(self):
        link = Link(CID, NAME, ("p",), 12, A_ID)
        self.assertEqual(Link.from_json(json.loads(json.dumps(link.to_json()))), link)
        with self.assertRaises(ValueError):
            Link.from_json({"uri": link.uri(), "size": -1})


class FieldTest(unittest.TestCase):
    """The FIELD_CUSTOM_TYPE / FIELD_CUSTOM_DATA structure: encoding and strict decoding."""

    def test_field_ids_are_lxmf_custom_fields(self):
        try:
            import LXMF
        except ImportError:
            self.skipTest("LXMF is not installed")
        self.assertEqual((FIELD_CUSTOM_TYPE, FIELD_CUSTOM_DATA), (LXMF.FIELD_CUSTOM_TYPE, LXMF.FIELD_CUSTOM_DATA))

    def test_round_trip_through_msgpack(self):
        from RNS.vendor import umsgpack
        links = [Link(CID, path=("dir", "f.txt"), size=1234, from_node=A_ID), Link(name=NAME), Link(CID, NAME)]
        fields = umsgpack.unpackb(umsgpack.packb(lxmf.encode_field(links)))
        self.assertEqual(fields[FIELD_CUSTOM_TYPE], "rnfs")
        self.assertEqual(lxmf.decode_field(fields), links)
        self.assertEqual(set(fields[FIELD_CUSTOM_DATA][1]), {"name"})  # only what is set

    def test_not_ours(self):
        for fields in (None, {}, [], {FIELD_CUSTOM_TYPE: "other", FIELD_CUSTOM_DATA: []},
                       {FIELD_CUSTOM_TYPE: b"rnfs"}, {0x01: "x"}):
            self.assertIsNone(lxmf.decode_field(fields))

    def test_strict(self):
        good = {"cid": CID.binary, "path": ["a"], "size": 1, "from": A_ID}

        def field(*entries):
            return {FIELD_CUSTOM_TYPE: "rnfs", FIELD_CUSTOM_DATA: list(entries)}

        bad = [field(), {FIELD_CUSTOM_TYPE: "rnfs"},
               {FIELD_CUSTOM_TYPE: "rnfs", FIELD_CUSTOM_DATA: {"cid": CID.binary}},
               field(*[good] * 9), field("x"), field({}), field({1: CID.binary}),
               field({"cid": str(CID)}), field({"cid": b"\x01\x55"}), field({"cid": bytearray(CID.binary)}),
               field({"name": NAME[:15]}), field({"name": NAME.hex()}), field({**good, "path": "a"}),
               field({**good, "path": [".."]}), field({**good, "path": ["a/b"]}), field({**good, "path": [b"a"]}),
               field({**good, "path": [""]}), field({**good, "path": ["x"] * 65}), field({**good, "size": True}),
               field({**good, "size": -1}), field({**good, "size": 2 ** 64}), field({**good, "size": 1.5}),
               field({**good, "from": A_ID + b"\x00"}), field({**good, "from": None}),
               field({**good, **{f"k{i}": 0 for i in range(13)}})]
        for fields in bad:
            with self.assertRaises(ValueError, msg=repr(fields)):
                lxmf.decode_field(fields)

    def test_unknown_keys_are_ignored(self):
        fields = {FIELD_CUSTOM_TYPE: "rnfs", FIELD_CUSTOM_DATA: [{"cid": CID.binary, "future": [1, 2]}]}
        self.assertEqual(lxmf.decode_field(fields), [Link(CID)])

    def test_encode_limits(self):
        with self.assertRaises(ValueError):
            lxmf.encode_field([])
        with self.assertRaises(ValueError):
            lxmf.encode_field([Link(CID)] * 9)
        with self.assertRaises(ValueError):
            lxmf.encode_field([Link()])


class MessageLogTest(unittest.TestCase):
    """The bounded, persistent message log behind the inbox and outbox."""

    def setUp(self):
        """A temporary state directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = self.tmp.name

    def open(self, limit: int = 10, box: str = lxmf.INBOX, **settings) -> MessageLog:
        """A log of box in the state directory (closed before the directory is removed)."""
        log = MessageLog(self.state, box, limit, **settings)
        self.addCleanup(log.close)
        return log

    def rows(self, box: str = lxmf.INBOX) -> dict:
        """The saved records of box: id -> record text."""
        return dict(state_rows(self.state, "SELECT id, record FROM lxmf_messages WHERE box = ?", (box,)))

    def test_bounded_oldest_dropped(self):
        """Past the limit the oldest record is dropped."""
        log = self.open(3)
        dropped = [log.add({"text": f"m{i}"})[1] for i in range(5)]
        self.assertEqual([e["id"] for e in log.entries()], [3, 4, 5])
        self.assertEqual([[e["text"] for e in d] for d in dropped], [[], [], [], ["m0"], ["m1"]])

    def test_persistence(self):
        """Records are saved as UTF-8 JSON, one row each; ids are never reused; a lower limit keeps the newest."""
        log = self.open()
        log.add({"text": "héllo ✓"})
        second, _ = log.add({"text": "two"})
        log.remove(second)
        log.save()
        self.assertEqual(self.rows(), {1: '{"text": "héllo ✓", "id": 1}'})
        again = self.open()
        self.assertEqual([e["text"] for e in again.entries()], ["héllo ✓"])
        self.assertTrue(again.clean)
        self.assertEqual(again.add({})[0], 3)  # ids are never reused
        self.assertEqual(self.open(1).entries()[0]["id"], 1)  # a lower limit keeps the newest

    def test_records_round_trip_exactly(self):
        """Lone surrogates, non-BMP and other non-ASCII characters, key order and nested values come back as
        they were saved."""
        log = self.open()
        records = [{"title": "\udc80 lone", "text": "emoji \U0001F600 and \ud83d alone", "display_name": "名前 \udfff",
                    "links": [{"uri": "rnfs:x", "size": 3}], "auto": [{"state": "pinned", "cid": "c"}], "sent": 1.5},
                   {"z": None, "a": [1, [2, {"b": "\x00\n"}]], "text": "\U0010FFFF"}]
        for record in records:
            log.add(dict(record))
        log.save()
        loaded = self.open().entries()
        self.assertEqual(loaded, [{**record, "id": n} for n, record in enumerate(records, 1)])
        self.assertEqual([list(e) for e in loaded], [list(record) + ["id"] for record in records])
        self.assertEqual([lxmf._json_bytes(e) for e in loaded], [lxmf._json_bytes(e) for e in log.entries()])

    def test_update_find_remove_all(self):
        """Records are updated, found by a field and removed (all of them for None)."""
        log = self.open()
        first, _ = log.add({"hash": "aa"})
        log.add({"hash": "bb"})
        self.assertTrue(log.update(first, state="x"))
        self.assertFalse(log.update(99, state="x"))
        self.assertEqual(log.find("hash", "aa")["state"], "x")
        self.assertIsNone(log.find("hash", "cc"))
        self.assertEqual(len(log.remove(None)), 2)
        self.assertEqual(len(log), 0)

    def test_a_save_writes_only_the_records_changed(self):
        """A save writes the records added, changed or removed since the last, with the next id."""
        log = self.open()
        for n in range(3):
            log.add({"n": n})
        log.save()
        state_execute(self.state, "UPDATE lxmf_messages SET record = '{\"n\": 0, \"id\": 1, \"mark\": 1}' "
                                  "WHERE box = 'inbox' AND id = 1")
        log.update(2, n=20)
        log.remove(3)
        log.add({"n": 4})
        log.save()
        self.assertEqual(self.rows(), {1: '{"n": 0, "id": 1, "mark": 1}', 2: '{"n": 20, "id": 2}',
                                       4: '{"n": 4, "id": 4}'})
        self.assertEqual(state_rows(self.state, "SELECT key, value FROM meta WHERE store = 'inbox' ORDER BY key"),
                         [("clean", 1), ("next_id", 5)])
        log.save()  # nothing changed: nothing written
        self.assertEqual(state_rows(self.state, "SELECT value FROM meta WHERE store = 'inbox' AND key = 'next_id'"),
                         [(5,)])

    def test_boxes_are_apart(self):
        """The inbox and outbox share the table, each with its rows and next id."""
        inbox, outbox = self.open(), self.open(box=lxmf.OUTBOX)
        inbox.add({"to": "in"})
        inbox.add({"to": "in"})
        outbox.add({"to": "out"})
        inbox.save()
        outbox.save()
        self.assertEqual([e["to"] for e in self.open(box=lxmf.OUTBOX).entries()], ["out"])
        self.assertEqual(self.open(box=lxmf.OUTBOX).add({})[0], 2)
        self.assertEqual(len(self.open().entries()), 2)

    def test_saves_never_land_out_of_order(self):
        """L13: a save that took its snapshot first can't be written after a later one; adding doesn't wait for
        a slow save."""
        import threading
        log = self.open()
        log.add({"n": 1})
        commit = statedb._Database._commit
        entered, gate = threading.Event(), threading.Event()

        def slow(database, conn):
            """The first save stalls in its commit until released."""
            if threading.current_thread().name == "first":
                entered.set()
                gate.wait(5)
            commit(database, conn)

        with mock.patch.object(statedb._Database, "_commit", slow):
            first = threading.Thread(target=log.save, name="first")
            first.start()
            entered.wait(5)
            log.add({"n": 2})  # doesn't wait for the save
            threading.Timer(0.2, gate.set).start()
            log.save()  # waits for the first save, then writes the newer records
            first.join(5)
        self.assertEqual([e["n"] for e in self.open().entries()], [1, 2])

    def test_a_failed_save_is_tried_again(self):
        """L18: a save is one transaction: if it fails nothing of it is written (the records saved before stay
        whole), and the next save writes what it would have."""
        log = self.open()
        log.add({"n": 1})
        log.save()
        log.update(1, n=10)
        log.add({"n": 2})
        with failing_commits():
            with self.assertRaises(OSError):
                log.save()
        self.assertEqual(self.rows(), {1: '{"n": 1, "id": 1}'})
        self.assertEqual([e["n"] for e in log.entries()], [10, 2])  # memory keeps them
        log.save()
        self.assertEqual([e["n"] for e in self.open().entries()], [10, 2])

    def test_malformed_rows_are_dropped_and_deleted(self):
        """Rows that aren't JSON, a dict with a positive integer id (up to MAX_ID) equal to the row's, or that
        valid() refuses are dropped and deleted; the box is then not clean until the next save."""
        log = self.open()
        log.add({"n": 1})
        log.save()
        for row_id, text in ((5, '{"id": "5"}'), (6, '{"id": 7}'), (8, "{not json"), (9, '"junk"'),
                             (10, '{"id": true}'), (11, '{"id": 11, "bad": 1}'), ("x12", '{"id": 12}')):
            state_execute(self.state, "INSERT INTO lxmf_messages (box, id, record) VALUES ('inbox', ?, ?)",
                          (row_id, text))
        state_execute(self.state, "INSERT INTO lxmf_messages (box, id, record) VALUES ('inbox', ?, ?)",
                      (lxmf.MAX_ID + 1, '{"id": %d}' % (lxmf.MAX_ID + 1)))
        state_execute(self.state, "UPDATE meta SET value = 'x' WHERE store = 'inbox' AND key = 'next_id'")
        log = self.open(valid=lambda entry: "bad" not in entry)
        self.assertEqual([e["id"] for e in log.entries()], [1])
        self.assertFalse(log.clean)
        self.assertEqual(list(self.rows()), [1])
        self.assertEqual(log.add({})[0], 2)  # a next id that isn't an integer is ignored
        again = self.open()
        self.assertFalse(again.clean)  # not saved since
        log.save()
        self.assertTrue(self.open().clean)

    def test_unsaved_state_is_not_clean(self):
        """A box never saved, or whose clean value is missing, isn't clean."""
        self.assertFalse(self.open().clean)
        log = self.open()
        log.add({"n": 1})
        log.save()
        self.assertTrue(self.open().clean)
        state_execute(self.state, "DELETE FROM meta WHERE store = 'inbox' AND key = 'clean'")
        self.assertFalse(self.open().clean)
        self.assertEqual(len(self.open().entries()), 1)

    def test_bounds_at_load_delete_rows(self):
        """Rows past the limit or the byte bound at load are deleted; that doesn't make the box unclean."""
        log = self.open()
        for n in range(5):
            log.add({"text": "x" * 100})
        log.save()
        bounded = self.open(3, max_bytes=250)
        self.assertEqual([e["id"] for e in bounded.entries()], [4, 5])
        self.assertTrue(bounded.clean)
        self.assertEqual(sorted(self.rows()), [4, 5])
        self.assertEqual(self.open().add({})[0], 6)


class FakeLxmf:
    """LxmfNetwork stand-in: records sends; identities known and propagation node as the test sets them."""

    def __init__(self, address: bytes = RECIPIENT, known: bool = True, propagation_node: bytes | None = None,
                 heard: bytes | None = None):
        """No messages sent yet; propagation_node is the configured one, heard the nearest heard."""
        self.address = address
        self.display_name = "test node"
        self.known = known
        self.pn = propagation_node
        self.heard = heard
        self.sent: list[tuple[bytes, str, dict, bool]] = []

    def nearest_propagation_node(self):
        """The heard propagation node the test set."""
        return self.heard

    def propagation_node(self, nearest=False):
        """The configured propagation node, else (nearest) the heard one, as the test set them."""
        return self.pn if self.pn is not None else (self.heard if nearest else None)

    def wait_identity(self, address, timeout):
        """Known or not, as the test set."""
        return self.known

    def send(self, address, content, fields, propagated, nearest=False):
        """Record and return a hash."""
        if propagated and self.propagation_node(nearest) is None:
            raise ValueError("no propagation node")
        self.sent.append((address, content, fields, propagated))
        return hashlib.sha256(f"{len(self.sent)}{content}".encode()).digest()

    def poll(self):
        """Nothing changes by itself."""
        return []

    def idle(self):
        """Nothing to poll."""
        return True

    def sync(self, nearest=False):
        """Started if there is a propagation node to use."""
        return self.propagation_node(nearest) is not None

    def announce(self):
        """Nothing to do."""

    def stop(self):
        """Nothing to do."""


def message(source=SENDER, content=b"", fields=None, number=1, title=b"", display_name=None, timestamp=1.7e9):
    """An Incoming message."""
    return Incoming(source, hashlib.sha256(bytes([number]) + source).digest(), timestamp, title, content,
                    fields or {}, "direct", display_name)


class FeatureTestCase(unittest.TestCase):
    """Offline nodes A (holding content) and B (running the feature), joined by a NetClient."""

    def setUp(self):
        """Fresh nodes; A holds a directory with one file."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = NetClient()
        self.net.sender = B_ID
        self.a = self.make_node("a", A_ID)
        self.data = os.urandom(3000)
        self.cid = Cid.parse(self.a.add_stream(io.BytesIO(self.data), name="file.txt")["cid"])
        self.link = Link(self.cid, path=("file.txt",), from_node=A_ID)

    def make_node(self, name: str, node_id: bytes, **settings) -> RnfsNode:
        """An offline node with its own directory."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, name), slow="no", lookup_timeout=2, path_timeout=0.5,
                        **settings)
        config.validate()
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def feature_node(self, network=None, **settings):
        """Node B with the lxmf feature attached to a stand-in network (jobs left out unless a test adds it)."""
        self.b = self.make_node("b", B_ID, **settings)
        jobs = self.b.features.pop("jobs", None)
        if jobs is not None:
            jobs.close()  # the node no longer closes it at stop
        self.feature = self.b.feature("lxmf")
        self.network = network or FakeLxmf()
        self.feature.attach(self.network)
        return self.feature

    def reloaded(self) -> lxmf.LxmfFeature:
        """B's lxmf feature built again from the saved state, as at a restart (closed before the directory is
        removed)."""
        again = lxmf.LxmfFeature(self.b)
        self.addCleanup(again.close)
        return again

    def link_message(self, link=None, number=1, source=SENDER, text=b"have a look"):
        """A message carrying link in its field and its text."""
        link = link or self.link
        return message(source, text + b"\n\n" + link.uri().encode(), lxmf.encode_field([link]), number)


class ReceiveTest(FeatureTestCase):
    """What gets into the inbox."""

    def test_field_links_are_kept(self):
        feature = self.feature_node()
        link = Link(self.cid, path=("file.txt",), size=3000, from_node=A_ID)
        entry_id = feature.received(self.link_message(link))
        entry = feature.inbox.get(entry_id)
        self.assertEqual(entry["from"], SENDER.hex())
        self.assertEqual([Link.from_json(l) for l in entry["links"]], [link])
        self.assertIsNone(entry["auto"])
        self.assertEqual(entry["sent"], 1.7e9)
        self.assertEqual(self.b.status()["lxmf_inbox"], 1)
        self.assertEqual(self.b.status()["lxmf_address"], RECIPIENT.hex())

    def test_text_links_when_there_is_no_field(self):
        feature = self.feature_node()
        entry_id = feature.received(message(content=f"look: {self.link.uri()}!".encode()))
        self.assertEqual(feature.inbox.get(entry_id)["links"], [self.link.to_json()])

    def test_malformed_field_falls_back_to_text(self):
        feature = self.feature_node()
        fields = {FIELD_CUSTOM_TYPE: "rnfs", FIELD_CUSTOM_DATA: [{"cid": "not binary"}]}
        entry_id = feature.received(message(content=self.link.uri().encode(), fields=fields))
        self.assertEqual(feature.inbox.get(entry_id)["links"], [self.link.to_json()])
        self.assertIsNone(feature.received(message(content=b"no links", fields=fields, number=2)))

    def test_messages_without_links_duplicates_and_own_are_ignored(self):
        feature = self.feature_node()
        self.assertIsNone(feature.received(message(content=b"just chatting")))
        self.assertIsNotNone(feature.received(self.link_message()))
        self.assertIsNone(feature.received(self.link_message()))  # same hash
        self.assertIsNone(feature.received(self.link_message(source=RECIPIENT, number=5)))  # from ourselves
        self.assertEqual(len(feature.inbox), 1)

    def test_accept_from(self):
        feature = self.feature_node(lxmf_accept_from=[SENDER.hex()], lxmf_auto_fetch_from=[f"<{OTHER.hex().upper()}>"])
        self.assertIsNotNone(feature.received(self.link_message(number=1)))
        self.assertIsNotNone(feature.received(self.link_message(source=OTHER, number=2)))  # auto-fetch senders too
        self.assertIsNone(feature.received(self.link_message(source=bytes(16), number=3)))
        self.assertEqual(len(feature.inbox), 2)

    def test_untrusted_text_is_capped(self):
        feature = self.feature_node()
        content = b"\x00evil\x1b[2J" + b"x" * 10000 + b"\xff " + self.link.uri().encode()
        entry_id = feature.received(message(content=content, fields=lxmf.encode_field([self.link]),
                                            title=b"t" * 1000, display_name="n" * 500, timestamp=True))
        entry = feature.inbox.get(entry_id)
        self.assertEqual(len(entry["text"]), lxmf.MAX_TEXT_CHARS)
        self.assertNotIn("\x00", entry["text"])
        self.assertEqual(len(entry["title"]), lxmf.MAX_TITLE_CHARS)
        self.assertEqual(len(entry["display_name"]), lxmf.MAX_DISPLAY_NAME_CHARS)
        self.assertIsNone(entry["sent"])

    def test_inbox_is_bounded_and_persists(self):
        feature = self.feature_node()
        feature.inbox.limit = 3
        feature.clock = FakeClock()
        for number in range(5):
            feature.clock.now += lxmf.SENDER_INTERVAL  # within the limits on senders not trusted (L12)
            feature.received(self.link_message(number=number))
        self.assertEqual([e["id"] for e in feature.inbox_list()], [3, 4, 5])
        feature.save()
        again = self.reloaded()
        self.assertEqual([e["id"] for e in again.inbox_list()], [3, 4, 5])

    def test_inbox_rm(self):
        feature = self.feature_node()
        for number in range(3):
            feature.received(self.link_message(number=number))
        self.assertEqual(feature.inbox_rm(2), 1)
        with self.assertRaises(NotFound):
            feature.inbox_rm(2)
        self.assertEqual(feature.inbox_rm(None), 2)


class AutoFetchTest(FeatureTestCase):
    """Links from lxmf_auto_fetch_from senders are fetched and pinned within their quota."""

    def source(self, sender=SENDER) -> str:
        """The pin source for sender."""
        return f"lxmf:{sender.hex()}"

    def target_cid(self) -> Cid:
        """The CID of the linked file inside A's directory."""
        return Cid.parse(self.a.info(f"{self.cid}/file.txt")["cid"])

    def test_pins_with_lxmf_source_through_the_hint(self):
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        entry_id = feature.received(self.link_message())
        self.assertEqual(feature.inbox.get(entry_id)["auto"], [{"state": "queued"}])
        feature.process_auto_fetches()
        target = self.target_cid()
        self.assertEqual(feature.inbox.get(entry_id)["auto"], [{"state": "pinned", "cid": str(target)}])
        self.assertEqual(self.b.storage.pins()[target], {self.source()})
        self.assertTrue(self.b.storage.is_complete(target))
        self.assertIn(target, self.b.publisher.provided())  # pinned content is provided like any pin

    def test_other_senders_are_not_fetched(self):
        feature = self.feature_node(lxmf_auto_fetch_from=[OTHER.hex()])
        entry_id = feature.received(self.link_message())
        feature.process_auto_fetches()
        self.assertIsNone(feature.inbox.get(entry_id)["auto"])
        self.assertEqual(self.b.storage.pins(), {})

    def test_quota(self):
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()], lxmf_auto_fetch_quota=4000)
        first = feature.received(self.link_message(number=1))
        feature.process_auto_fetches()
        self.assertEqual(feature.inbox.get(first)["auto"][0]["state"], "pinned")
        # A second file from the same sender would take it past 4000 bytes
        other = Cid.parse(self.a.add_stream(io.BytesIO(os.urandom(2000)))["cid"])
        second = feature.received(self.link_message(Link(other, from_node=A_ID), number=2))
        feature.process_auto_fetches()
        result = feature.inbox.get(second)["auto"][0]
        self.assertEqual(result["state"], "refused")
        self.assertIn("quota", result["error"])
        self.assertNotIn(other, self.b.storage.pins())

    def test_max_download_size(self):
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()], max_download_size=100)
        entry_id = feature.received(self.link_message())
        feature.process_auto_fetches()
        self.assertEqual(feature.inbox.get(entry_id)["auto"][0]["state"], "refused")
        self.assertEqual(self.b.storage.pins(), {})

    def test_removal_releases_pins_others_still_hold(self):
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        first = feature.received(self.link_message(number=1))
        second = feature.received(self.link_message(number=2))
        feature.process_auto_fetches()
        target = self.target_cid()
        self.b.storage.pin(target, USER)
        feature.inbox_rm(first)
        self.assertIn(self.source(), self.b.storage.pins()[target])  # the second message still holds it
        feature.inbox_rm(second)
        self.assertEqual(self.b.storage.pins()[target], {USER})

    def test_full_inbox_keeps_pin_holders(self):
        # RM-M1: messages from others never push out one holding auto-fetched pins
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        feature.inbox.limit = 1
        first = feature.received(self.link_message(number=1))
        feature.process_auto_fetches()
        target = self.target_cid()
        self.assertIn(target, self.b.storage.pins())
        self.assertIsNone(feature.received(message(content=b"see " + Link(self.cid, from_node=A_ID).uri().encode(),
                                                   source=OTHER, number=2)))
        self.assertIn(target, self.b.storage.pins())
        self.assertEqual([e["id"] for e in feature.inbox_list()], [first])
        feature.inbox_rm(first)  # an explicit removal releases them
        self.assertNotIn(target, self.b.storage.pins())

    def test_flood_from_many_senders_keeps_trusted_pins(self):
        # RM-M1 (PoC inbox_flood.py): INBOX_LIMIT messages from fresh identities, slowly enough to pass the rate
        # limit on senders not trusted (L12)
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        feature.clock = FakeClock()
        trusted = feature.received(self.link_message())
        feature.process_auto_fetches()
        target = self.target_cid()
        junk = Link(self.cid).uri().encode()
        for i in range(lxmf.INBOX_LIMIT + 10):
            feature.clock.now += lxmf.UNTRUSTED_INTERVAL
            feature.received(message(os.urandom(16), junk, number=i % 256))
        self.assertIn(self.source(), self.b.storage.pins()[target])
        self.assertIsNotNone(feature.inbox.get(trusted))
        self.assertEqual(len(feature.inbox), lxmf.INBOX_LIMIT)

    def test_one_sender_is_capped(self):
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        feature.clock = FakeClock()
        junk = Link(self.cid).uri().encode()
        for i in range(lxmf.INBOX_PER_SENDER + 5):
            feature.clock.now += lxmf.SENDER_INTERVAL  # within the rate limits on senders not trusted (L12)
            feature.received(message(OTHER, junk, number=i))
        feature.received(self.link_message(source=bytes([0x54] * 16), number=1))
        senders = [e["from"] for e in feature.inbox_list()]
        self.assertEqual(senders.count(OTHER.hex()), lxmf.INBOX_PER_SENDER)
        self.assertEqual(senders[-1], "54" * 16)

    def test_removal_racing_the_pin_releases_it(self):
        # RM-L6 (PoC pin_leak_race.py): a removal right after the pin must see it recorded
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        entry_id = feature.received(self.link_message())
        original = self.b.publisher.refresh

        def refresh():
            """Remove the message while the pin is being published."""
            self.b.publisher.refresh = original
            feature.inbox_rm(entry_id)
            original()

        self.b.publisher.refresh = refresh
        feature.process_auto_fetches()
        self.assertEqual(len(feature.inbox), 0)
        self.assertNotIn(self.target_cid(), self.b.storage.pins())

    def test_name_links_resolve_without_persisting(self):
        # BG-L9: names arriving unasked go only into the bounded table
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        calls = []

        def resolve(name, persist, max_age=0.0):
            """Record how the name was resolved."""
            calls.append((name, persist))
            return {"cid": str(self.cid)}

        with mock.patch.object(self.b.names, "resolve", side_effect=resolve):
            entry_id = feature.received(self.link_message(Link(name=NAME, path=("file.txt",), from_node=A_ID)))
            feature.process_auto_fetches()
        self.assertEqual(calls, [(NAME, False)])
        self.assertEqual(feature.inbox.get(entry_id)["auto"][0]["state"], "pinned")

    def test_removed_while_queued_pins_nothing(self):
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        entry_id = feature.received(self.link_message())
        feature.inbox_rm(entry_id)
        feature.process_auto_fetches()
        self.assertEqual(self.b.storage.pins(), {})

    def test_unreachable_hint_falls_back_to_the_dht(self):
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        self.b.routing.heard(A_ID)  # the DHT can find A, the hint is a node that doesn't exist
        self.a.records.provide(A_ID, [self.cid.key], [])
        entry_id = feature.received(self.link_message(Link(self.cid, path=("file.txt",), from_node=bytes(16))))
        feature.process_auto_fetches()
        self.assertEqual(feature.inbox.get(entry_id)["auto"][0]["state"], "pinned")

    def test_queue_is_bounded(self):
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        with mock.patch.object(lxmf, "AUTO_QUEUE_LIMIT", 1):
            feature.received(self.link_message(number=1))
            second = feature.received(self.link_message(number=2))
        self.assertEqual(feature.inbox.get(second)["auto"][0]["state"], "skipped")


class InboxGetTest(FeatureTestCase):
    """`inbox get`: fetching what a message links to."""

    def test_get_into_directory_through_the_hint(self):
        feature = self.feature_node()
        entry_id = feature.received(self.link_message())
        out = os.path.join(self.tmp.name, "out")
        os.makedirs(out)
        results = feature.inbox_get(entry_id, directory=out)
        self.assertEqual(results[0]["output"], os.path.join(out, "file.txt"))
        with open(results[0]["output"], "rb") as f:
            self.assertEqual(f.read(), self.data)
        with self.assertRaises(ValueError):
            feature.inbox_get(entry_id, directory=out)  # it exists now

    def test_choices(self):
        feature = self.feature_node()
        both = f"{self.link.uri()} {Link(self.cid, from_node=A_ID).uri()}"
        entry_id = feature.received(message(content=both.encode()))
        with self.assertRaises(ValueError):
            feature.inbox_get(entry_id, output=os.path.join(self.tmp.name, "x"))  # two links, one output
        with self.assertRaises(ValueError):
            feature.inbox_get(entry_id, 3, directory=self.tmp.name)
        with self.assertRaises(NotFound):
            feature.inbox_get(99, directory=self.tmp.name)
        output = os.path.join(self.tmp.name, "dir-copy")
        feature.inbox_get(entry_id, 2, output=output)
        self.assertEqual(os.listdir(output), ["file.txt"])

    def test_jobs_hook(self):
        feature = self.feature_node()
        jobs = mock.Mock()
        jobs.submit.return_value = {"id": "0123abcd"}
        self.b.features["jobs"] = jobs
        entry_id = feature.received(self.link_message())
        output = os.path.join(self.tmp.name, "f")
        results = feature.inbox_get(entry_id, output=output)
        self.assertEqual(results[0]["job"], {"id": "0123abcd"})
        # The sender's node first, then the DHT (BG-M4: also once the routing table isn't empty)
        jobs.submit.assert_called_once_with("get", f"{self.cid}/file.txt", output, from_node=A_ID.hex(),
                                            fallback_dht=True)
        self.b.routing.heard(bytes([9] * 16))
        feature.inbox_get(entry_id, output=output + "2")
        self.assertEqual(jobs.submit.call_args.kwargs, {"from_node": A_ID.hex(), "fallback_dht": True})
        no_hint = feature.received(self.link_message(Link(self.cid), number=2))
        feature.inbox_get(no_hint, output=output + "3")
        self.assertEqual(jobs.submit.call_args.kwargs, {})


class SendTest(FeatureTestCase):
    """`send` and the outbox."""

    def test_send_direct(self):
        feature = self.feature_node()
        result = feature.send(f"<{SENDER.hex()}>", f"{self.cid}/file.txt", "a file for you")
        self.assertEqual(result["method"], "direct")
        address, content, fields, propagated = self.network.sent[0]
        self.assertEqual((address, propagated), (SENDER, False))
        self.assertTrue(content.startswith("a file for you\n\n"))
        self.assertEqual(lxmf.find_links(content), [Link(self.cid, path=("file.txt",), from_node=B_ID)])
        self.assertEqual(lxmf.decode_field(fields), [Link(self.cid, path=("file.txt",), from_node=B_ID)])
        entry = feature.outbox_list()[0]
        self.assertEqual((entry["hash"], entry["state"], entry["to"]), (result["hash"], "outbound", SENDER.hex()))

    def test_size_is_included_when_local(self):
        feature = self.feature_node()
        self.b.add_stream(io.BytesIO(self.data), name="file.txt")
        feature.send(SENDER.hex(), f"rnfs:{self.cid}/file.txt")
        self.assertEqual(lxmf.decode_field(self.network.sent[0][2])[0].size, 3000)

    def test_states_and_fallback_to_propagation(self):
        feature = self.feature_node(FakeLxmf(propagation_node=OTHER))
        result = feature.send(SENDER.hex(), str(self.cid))
        first = bytes.fromhex(result["hash"])
        feature.outbound_changed(first, "sending", 0.5)
        self.assertEqual(feature.outbox_list()[0]["progress"], 0.5)
        feature.outbound_changed(first, "failed", 0.0)
        entry = feature.outbox_list()[0]
        self.assertEqual((entry["method"], entry["state"], entry["direct_hash"]),
                         ("propagated", "outbound", first.hex()))
        self.assertTrue(self.network.sent[1][3])
        self.assertEqual(self.network.sent[1][1], self.network.sent[0][1])
        feature.outbound_changed(bytes.fromhex(entry["hash"]), "sent", 1.0)
        self.assertEqual(feature.outbox_list()[0]["state"], "sent")

    def test_failure_without_propagation_node(self):
        feature = self.feature_node()
        first = bytes.fromhex(feature.send(SENDER.hex(), str(self.cid))["hash"])
        feature.outbound_changed(first, "failed", 0.0)
        self.assertEqual(feature.outbox_list()[0]["state"], "failed")
        self.assertEqual(len(self.network.sent), 1)

    def test_propagate(self):
        feature = self.feature_node()
        with self.assertRaises(ValueError):
            feature.send(SENDER.hex(), str(self.cid), propagate=True)
        self.network.pn = OTHER
        self.assertEqual(feature.send(SENDER.hex(), str(self.cid), propagate=True)["method"], "propagated")
        self.assertTrue(self.network.sent[0][3])

    def test_refusals(self):
        feature = self.feature_node(FakeLxmf(known=False))
        with self.assertRaises(NotFound):
            feature.send(SENDER.hex(), str(self.cid))
        self.network.known = True
        for to, target, text in ((SENDER.hex(), "nonsense", None), ("", str(self.cid), None),
                                 ("not-a-contact", str(self.cid), None),
                                 (SENDER.hex(), str(self.cid), "x" * (lxmf.MAX_TEXT_CHARS + 1))):
            with self.assertRaises(ValueError, msg=(to, target)):
                feature.send(to, target, text)
        self.assertEqual(self.network.sent, [])

    def test_contact_nick(self):
        feature = self.feature_node()
        contacts = mock.Mock()
        contacts.lookup.side_effect = lambda nick, kind=None: SENDER if nick == "alice" and kind in ("lxmf", None) \
            else (OTHER if nick == "bob" else None)
        self.addCleanup(self.b.features["contacts"].close)  # the node closes the stand-in instead
        self.b.features["contacts"] = contacts
        feature.send("alice", str(self.cid))
        self.assertEqual(self.network.sent[0][0], SENDER)
        contacts.lookup.side_effect = lambda nick, kind=None: OTHER if kind is None else None
        with self.assertRaisesRegex(ValueError, "not an LXMF address"):
            feature.send("bob", str(self.cid))
        # A name contact as the target: the link follows the name
        contacts.lookup.side_effect = lambda nick, kind=None: {("alice", "lxmf"): SENDER,
                                                               ("site", "name"): NAME}.get((nick, kind))
        feature.send("alice", "site/docs/a.txt")
        self.assertEqual(lxmf.decode_field(self.network.sent[1][2]),
                         [Link(name=NAME, path=("docs", "a.txt"), from_node=B_ID)])

    def test_unfinished_messages_fail_after_restart(self):
        feature = self.feature_node()
        feature.send(SENDER.hex(), str(self.cid))
        feature.save()
        again = self.reloaded()
        self.assertEqual(again.outbox_list()[0]["state"], "failed")
        again.save()
        self.assertEqual(self.reloaded().outbox_list()[0]["state"], "failed")


class LoopNetwork(FakeLxmf):
    """FakeLxmf counting the loop's announces, downloads and polls; idle unless the test queues a message."""

    def __init__(self):
        """Nothing queued; feature is set by the test, for wake()."""
        super().__init__()
        self.announces = self.syncs = self.polls = 0
        self.pending = 0  # polls until the queued message is done with
        self.feature = None

    def announce(self):
        """Count it."""
        self.announces += 1

    def sync(self, nearest=False):
        """Count it."""
        self.syncs += 1
        return super().sync(nearest)

    def poll(self):
        """Count it; the queued message is done with after its polls."""
        self.polls += 1
        self.pending = max(0, self.pending - 1)
        return []

    def idle(self):
        """Idle once the queued message is done with."""
        return not self.pending

    def queue(self, polls: int):
        """Queue a message followed for polls polls, waking the loop as LxmfNetwork.send does."""
        self.pending = polls
        self.feature.wake()


class LoopTest(FeatureTestCase):
    """S7: the announce/sync/poll loop polls each TICK only while the network has something to poll."""

    def test_idle_loop_waits_until_the_next_announce_or_download(self):
        network = LoopNetwork()
        feature = self.feature_node(network, startup_announce_delay=30, lxmf_sync_interval=3600)
        timeouts = []

        class Recording(threading.Event):
            """Records each wait's timeout and returns at once; queues a message at the third, stops at the sixth."""

            def wait(self, timeout=None):
                timeouts.append(timeout)
                if len(timeouts) == 3:
                    network.pending = 2
                if len(timeouts) == 6:
                    feature._stop.set()
                return super().wait(0)

        feature._wake = Recording()
        feature._run()  # here, until the sixth wait stops it
        self.assertEqual((network.announces, network.syncs, network.polls), (1, 0, 6))
        # Idle: until the second announce (startup_announce_delay); the queued message: each TICK
        self.assertEqual(timeouts[3], lxmf.TICK)
        for timeout in timeouts[:3] + timeouts[4:]:
            self.assertTrue(29 < timeout <= 30, timeouts)

    def test_a_queued_message_wakes_the_idle_loop(self):
        network = LoopNetwork()
        feature = self.feature_node(network)  # the default startup_announce_delay: idle, it waits 30 s
        network.feature = feature
        thread = threading.Thread(target=feature._run, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(feature.wake)
        self.addCleanup(feature._stop.set)
        wait_for(lambda: network.polls, 10, "the first poll")
        time.sleep(1.2)
        self.assertEqual(network.polls, 1)  # no polls each second while idle
        network.queue(2)
        # Polled at once, then each TICK until it is done with; then idle again
        wait_for(lambda: network.polls >= 3, 10, "the queued message's polls")
        time.sleep(1.2)
        self.assertEqual(network.polls, 3)
        self.assertEqual(network.announces, 1)
        # stop() wakes the idle loop
        feature._stop.set()
        feature.wake()
        thread.join(5)
        self.assertFalse(thread.is_alive())


class DisabledTest(FeatureTestCase):
    """Without the LXMF package, or switched off, the feature stays off cleanly."""

    def test_lxmf_missing(self):
        self.b = self.make_node("b", B_ID)
        feature = self.b.feature("lxmf")
        with mock.patch.dict(sys.modules, {"LXMF": None}):  # `import LXMF` raises ImportError
            feature.start()
        self.assertIsNone(feature.network)
        self.assertIn("not installed", feature.off_reason)
        self.assertEqual(feature._threads, [])
        self.assertIsNone(self.b.status()["lxmf_address"])
        with self.assertRaisesRegex(ValueError, "not installed"):
            feature.send(SENDER.hex(), str(self.cid))
        with self.assertRaisesRegex(ValueError, "not installed"):
            feature.address()
        self.assertEqual(feature.inbox_list(), [])  # stored messages stay readable
        feature.stop()

    def test_switched_off(self):
        self.b = self.make_node("b", B_ID, lxmf_enabled=False)
        feature = self.b.feature("lxmf")
        with mock.patch.object(lxmf, "LxmfNetwork", side_effect=AssertionError("must not start")):
            feature.start()
        self.assertIsNone(feature.network)
        self.assertIn("lxmf_enabled", feature.off_reason)

    def test_failure_to_start_does_not_raise(self):
        self.b = self.make_node("b", B_ID)
        feature = self.b.feature("lxmf")
        with mock.patch.object(lxmf, "LxmfNetwork", side_effect=RuntimeError("boom")):
            feature.start()
        self.assertIn("boom", feature.off_reason)


class SettingsTest(unittest.TestCase):
    """lxmf_* settings validation and the lxmf: pin source."""

    def test_validation(self):
        Config(lxmf_accept_from=[SENDER.hex(), f"<{OTHER.hex()}>"], lxmf_propagation_node=OTHER.hex()).validate()
        for settings in ({"lxmf_accept_from": ["xyz"]}, {"lxmf_auto_fetch_from": SENDER.hex()},
                         {"lxmf_propagation_node": "00"}, {"lxmf_auto_fetch_quota": -1},
                         {"lxmf_auto_fetch_quota": True}, {"lxmf_sync_interval": -5},
                         {"lxmf_display_name": ""}, {"lxmf_display_name": "a\nb"}, {"lxmf_enabled": "yes"}):
            with self.assertRaises(ValueError, msg=settings):
                Config(**settings).validate()

    def test_pin_source(self):
        storage_module.check_source(f"lxmf:{SENDER.hex()}")
        for source in ("lxmf:", "lxmf:xyz", f"lxmf:{'ab' * 16}".upper(), f"lxmf:{SENDER.hex()}0"):
            with self.assertRaises(ValueError, msg=source):
                storage_module.check_source(source)

    def test_identity_path(self):
        config = Config(rnfs_dir="/x/rnfs")
        self.assertEqual(lxmf.lxmf_identity_path(config), os.path.join("/x/rnfs", "storage", "lxmf_identity"))


class NetworkPartsTest(unittest.TestCase):
    """The parts of LxmfNetwork that need no running Reticulum."""

    def network(self) -> lxmf.LxmfNetwork:
        """An LxmfNetwork without a router or RNS instance."""
        try:
            import LXMF
        except ImportError:
            self.skipTest("LXMF is not installed")
        from RNS.vendor import umsgpack
        network = lxmf.LxmfNetwork.__new__(lxmf.LxmfNetwork)
        network.RNS, network.LXMF, network.msgpack = RNS, LXMF, umsgpack
        network.router = mock.Mock()
        network._lock = __import__("threading").Lock()
        network._propagation_nodes = {}
        network._configured_pn = network._current_pn = None
        return network

    def test_announce_data_has_no_compression(self):
        network = self.network()
        destination = mock.Mock(display_name="rnfs 1234", stamp_cost=None)
        network.router.delivery_destinations = {RECIPIENT: destination}
        app_data = network._announce_app_data(RECIPIENT)
        self.assertEqual(network.LXMF.display_name_from_app_data(app_data), "rnfs 1234")
        self.assertFalse(network.LXMF.compression_support_from_app_data(app_data))
        self.assertIsNone(network._announce_app_data(SENDER))

    def test_nearest_propagation_node(self):
        network = self.network()
        from RNS.vendor import umsgpack

        def announce(enabled: bool) -> bytes:
            """Propagation node announce data as LXMRouter makes it."""
            return umsgpack.packb([False, 1, enabled, 256, 10240, [16, 3, 18], {}])

        self.assertIsNone(network.propagation_node())
        network.heard_propagation_node(SENDER, announce(True))
        network.heard_propagation_node(OTHER, announce(True))
        network.heard_propagation_node(A_ID, b"\x93garbage")
        hops = {SENDER: 3, OTHER: 1}
        with mock.patch.object(RNS.Transport, "hops_to", side_effect=hops.get):
            # RM-ML4: one only heard is used only when asked for
            self.assertIsNone(network.propagation_node())
            self.assertEqual(network.nearest_propagation_node(), OTHER)
            network.router.set_outbound_propagation_node.assert_not_called()
            self.assertEqual(network.propagation_node(nearest=True), OTHER)
            network.router.set_outbound_propagation_node.assert_called_with(OTHER)
            network.heard_propagation_node(OTHER, announce(False))  # it stopped propagating
            self.assertEqual(network.propagation_node(nearest=True), SENDER)

    def test_configured_propagation_node_wins(self):
        network = self.network()
        network._configured_pn = OTHER
        self.assertEqual(network.propagation_node(), OTHER)
        self.assertEqual(network.propagation_node(nearest=True), OTHER)

    def test_signal_handlers_are_kept(self):
        import signal
        before = signal.getsignal(signal.SIGINT)
        with lxmf._keeping_signal_handlers():
            signal.signal(signal.SIGINT, lambda *_: None)
        self.assertIs(signal.getsignal(signal.SIGINT), before)


class CliTest(unittest.TestCase):
    """The commands parse, and loading them doesn't load RNS or LXMF."""

    def test_parser(self):
        from rnfs import cli
        parser = cli.build_parser()
        args = parser.parse_args(["send", SENDER.hex(), str(CID), "-m", "hi", "--propagate"])
        self.assertEqual((args.to, args.target, args.message, args.propagate), (SENDER.hex(), str(CID), "hi", True))
        self.assertEqual(parser.parse_args(["inbox"]).run.__name__, "_run_inbox")
        args = parser.parse_args(["inbox", "get", "4", "--link", "2", "-o", "x"])
        self.assertEqual((args.run.__name__, args.n, args.link, args.output), ("_run_inbox_get", 4, 2, "x"))
        self.assertFalse(args.use_sent_cid)
        self.assertTrue(parser.parse_args(["inbox", "get", "4", "--use-sent-cid"]).use_sent_cid)
        self.assertTrue(parser.parse_args(["inbox", "rm", "--all"]).all)
        self.assertEqual(parser.parse_args(["outbox"]).run.__name__, "_run_outbox")
        self.assertEqual(parser.parse_args(["lxmf", "address"]).run.__name__, "_run_address")
        self.assertTrue(parser.parse_args(["lxmf", "sync", "--nearest"]).nearest)
        self.assertFalse(parser.parse_args(["lxmf", "sync"]).nearest)
        self.assertTrue(parser.parse_args(["send", SENDER.hex(), str(CID), "--propagate", "--nearest"]).nearest)

    def test_no_rns_at_import(self):
        code = ("import sys; from rnfs import cli; from rnfs.features import lxmf; cli.build_parser(); "
                "lxmf.parse_uri(lxmf.format_uri(lxmf.Link(name=bytes(16)))); "
                "print('RNS' in sys.modules, 'LXMF' in sys.modules)")
        result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, timeout=60,
                                encoding="utf-8", errors="replace")
        self.assertEqual(result.stdout.strip(), "False False", result.stderr)

    def test_human_size(self):
        self.assertEqual(lxmf.human_size(999), "999 B")
        self.assertEqual(lxmf.human_size(12_345_678), "12.3 MB")


class AuditFixesTest(FeatureTestCase):
    """Regressions for the second security audit's LXMF findings."""

    def test_heard_propagation_node_needs_nearest(self):
        # RM-ML4: no fallback or propagation through a node only heard announcing itself, unless asked
        feature = self.feature_node(FakeLxmf(heard=OTHER))
        first = bytes.fromhex(feature.send(SENDER.hex(), str(self.cid))["hash"])
        feature.outbound_changed(first, "failed", 0.0)
        self.assertEqual(feature.outbox_list()[0]["state"], "failed")
        self.assertEqual(len(self.network.sent), 1)
        with self.assertRaisesRegex(ValueError, "--nearest"):
            feature.send(SENDER.hex(), str(self.cid), propagate=True)
        self.assertEqual(feature.send(SENDER.hex(), str(self.cid), propagate=True, nearest=True)["method"],
                         "propagated")
        second = bytes.fromhex(feature.send(SENDER.hex(), str(self.cid), nearest=True)["hash"])
        feature.outbound_changed(second, "failed", 0.0)
        self.assertEqual(feature.outbox_list()[-1]["method"], "propagated")  # this one asked for the nearest
        self.assertEqual(feature.address()["nearest_propagation_node"], OTHER.hex())
        self.assertIsNone(feature.address()["propagation_node"])

    def test_field_links_longer_than_a_uri_are_refused(self):
        # DS-M3 (PoC inbox_bloat.py): 64 names of 252 bytes can't be a URI, so the field is malformed
        name = "\U0001F600" * 63
        fields = {FIELD_CUSTOM_TYPE: "rnfs", FIELD_CUSTOM_DATA: [{"cid": CID.binary, "path": [name] * 64}] * 8}
        with self.assertRaisesRegex(ValueError, "longer than"):
            lxmf.decode_field(fields)
        feature = self.feature_node()
        self.assertIsNone(feature.received(message(content=b"x" * 4000, fields=fields)))

    def test_entries_are_bounded(self):
        # DS-M3: 8 near-4096-character links and 4000 4-byte characters of text fit MAX_ENTRY_BYTES
        feature = self.feature_node()
        links = [Link(CID, path=("\u00e9" * 120,) * 5 + (f"f{i}",)) for i in range(8)]
        self.assertTrue(all(len(link.uri()) < lxmf.MAX_URI_LENGTH for link in links))
        entry_id = feature.received(message(content=("\U0001F600" * 4000).encode(), fields=lxmf.encode_field(links),
                                            title=b"\x01" * 300))
        entry = feature.inbox.get(entry_id)
        self.assertLessEqual(lxmf._json_bytes(entry), lxmf.MAX_ENTRY_BYTES)
        self.assertGreaterEqual(len(entry["links"]), 1)
        self.assertEqual(entry["links"][0], links[0].to_json())

    def test_inbox_bytes_are_bounded_and_saved_as_utf8(self):
        feature = self.feature_node()
        feature.inbox.max_bytes = 20_000
        for number in range(20):
            feature.received(self.link_message(number=number, source=bytes([number]) * 16,
                                               text=("é" * 3000).encode()))
        sizes = [lxmf._json_bytes(e) for e in feature.inbox.entries()]
        self.assertLessEqual(sum(sizes), 20_000)
        self.assertEqual(feature.inbox_list()[-1]["id"], 20)
        feature.save()
        raw = "".join(text for text, in state_rows(self.b.config.state_dir, "SELECT record FROM lxmf_messages"))
        self.assertIn("é", raw)
        self.assertNotIn("\\u00e9", raw)

    def test_default_output_name_from_the_sender_is_sanitized(self):
        # LP-L1 / CC-M1: a dotfile or a name with escapes becomes the root CID
        feature = self.feature_node()
        out = os.path.join(self.tmp.name, "cwd")
        os.makedirs(out)
        for number, name in enumerate((".bash_login", "\x1b]0;x\x07evil.txt")):
            cid = Cid.parse(self.a.add_stream(io.BytesIO(b"payload %d" % number), name=name)["cid"])
            entry_id = feature.received(self.link_message(Link(cid, path=(name,), from_node=A_ID), number=number))
            result = feature.inbox_get(entry_id, directory=out)[0]
            self.assertEqual(result["output"], os.path.join(out, str(cid)))
        self.assertFalse([n for n in os.listdir(out) if n.startswith(".") or not n.isprintable()])

    def test_inbox_get_prints_escaped(self):
        from types import SimpleNamespace
        results = {"results": [{"output": "/x/\x1b[2Jevil", "warnings": []},
                               {"output": "/x/\x1b]0;t\x07", "job": {"id": "0123abcd"}}]}
        client = mock.Mock()
        client.post.return_value = results
        out = io.StringIO()
        with mock.patch("rnfs.cli.client", return_value=client), mock.patch("sys.stdout", out):
            lxmf._run_inbox_get(SimpleNamespace(output=None, n=1, link=None, use_sent_cid=False))
        self.assertNotIn("\x1b", out.getvalue())
        self.assertNotIn("\x07", out.getvalue())

    GOOD_IN = {"id": 2, "hash": "aa", "from": SENDER.hex(), "links": [], "auto": None, "text": "t"}
    GOOD_OUT = {"id": 3, "hash": "bb", "to": SENDER.hex(), "state": "delivered", "method": "direct", "links": [],
                "text": ""}
    BAD = {"id": 1, "links": "x", "auto": 5, "from": 3, "state": []}

    def test_malformed_records_are_dropped_at_load(self):
        """BG-L8 (PoC poc_state_shapes.py): records of the wrong shape in the files of rnfs 0.1.4 are dropped
        when they are imported."""
        state = Config(rnfs_dir=os.path.join(self.tmp.name, "b")).state_dir
        for name, good, wrong in (("lxmf_inbox.json", self.GOOD_IN, {"auto": [1]}),
                                  ("lxmf_outbox.json", self.GOOD_OUT, {"state": ["x"]})):
            write_legacy(state, name, {"messages": [self.BAD, {**good, **wrong, "id": 7}, good, good], "next_id": "x"})
        self.b = self.make_node("b", B_ID)
        feature = self.b.feature("lxmf")
        self.assertEqual([e["id"] for e in feature.inbox_list()], [2])
        self.assertEqual([e["id"] for e in feature.outbox_list()], [3])
        self.assertFalse(feature.inbox.clean)
        self.assertEqual(feature.inbox_rm(None), 1)

    def test_malformed_rows_are_dropped_at_load(self):
        """BG-L8 for the database: rows of the wrong shape are dropped and deleted."""
        self.b = self.make_node("b", B_ID)
        state = self.b.config.state_dir
        for box, good, wrong in ((lxmf.INBOX, self.GOOD_IN, {"auto": [1]}),
                                 (lxmf.OUTBOX, self.GOOD_OUT, {"state": ["x"]})):
            for record in (self.BAD, {**good, **wrong, "id": 7}, good):
                state_execute(state, "INSERT INTO lxmf_messages (box, id, record) VALUES (?, ?, ?)",
                              (box, record["id"], json.dumps(record)))
        feature = self.reloaded()
        self.assertEqual([e["id"] for e in feature.inbox_list()], [2])
        self.assertEqual([e["id"] for e in feature.outbox_list()], [3])
        self.assertEqual(state_rows(state, "SELECT box, id FROM lxmf_messages ORDER BY box, id"),
                         [(lxmf.INBOX, 2), (lxmf.OUTBOX, 3)])

    def test_privacy_defaults(self):
        feature = self.feature_node(lxmf_send_from_hint=False)
        feature.send(SENDER.hex(), f"{self.cid}/file.txt")
        _, content, fields, _ = self.network.sent[0]
        self.assertEqual(lxmf.decode_field(fields), [Link(self.cid, path=("file.txt",))])
        self.assertNotIn(B_ID.hex(), content)
        self.assertEqual(lxmf.DEFAULT_DISPLAY_NAME, "rnfs node")


class RestartAndCancelTest(FeatureTestCase):
    """Auto-fetches across restarts and cancels, `inbox get` on several links, outbox records dropped early."""

    def restarted(self) -> lxmf.LxmfFeature:
        """B's lxmf feature saved, then loaded again and started on a stand-in network (no threads run)."""
        self.feature.save()
        again = self.reloaded()
        with mock.patch.object(lxmf, "LxmfNetwork", return_value=FakeLxmf()), \
                mock.patch.object(again, "_run"), mock.patch.object(again, "_run_auto_fetch"):
            again.start()
        self.addCleanup(again.stop)  # before it is closed
        return again

    def test_queued_auto_fetches_survive_a_restart(self):
        # M3: the queue isn't saved; start() queues what the inbox still has as queued, within the limit
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        ids = [feature.received(self.link_message(number=i)) for i in range(1, 4)]
        with mock.patch.object(lxmf, "AUTO_QUEUE_LIMIT", 2):
            again = self.restarted()
        self.assertEqual(list(again._auto_queue), [(ids[0], 0), (ids[1], 0)])
        self.assertEqual(again.inbox.get(ids[2])["auto"][0]["state"], "skipped")
        again.process_auto_fetches()
        self.assertEqual([again.inbox.get(i)["auto"][0]["state"] for i in ids], ["pinned", "pinned", "skipped"])
        self.assertIn(Cid.parse(self.a.info(f"{self.cid}/file.txt")["cid"]), self.b.storage.pins())

    def test_stopping_cancels_an_auto_fetch_and_leaves_it_queued(self):
        # L27 + M3: the fetch running at shutdown stops at once, and runs again after the restart
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        entry_id = feature.received(self.link_message())
        opened = []
        original = self.b.fetcher.open

        def open_(*args, **kwargs):
            """The node stops as the fetch starts."""
            opened.append(args)
            feature._stop.set()
            return original(*args, **kwargs)

        with mock.patch.object(self.b.fetcher, "open", side_effect=open_):
            feature.process_auto_fetches()
        self.assertEqual(len(opened), 1)  # no fallback to the DHT after the cancel
        target = Cid.parse(self.a.info(f"{self.cid}/file.txt")["cid"])
        self.assertFalse(self.b.storage.is_complete(target))
        self.assertEqual(feature.inbox.get(entry_id)["auto"], [{"state": "queued"}])
        again = self.restarted()
        again.process_auto_fetches()
        self.assertEqual(again.inbox.get(entry_id)["auto"][0]["state"], "pinned")

    def test_removing_the_message_cancels_its_auto_fetch(self):
        # L27: inbox rm stops the fetch instead of letting it download everything first
        feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex()])
        entry_id = feature.received(self.link_message())
        original = self.b.fetcher.open

        def open_(*args, **kwargs):
            """The message is removed as the fetch starts."""
            feature.inbox_rm(entry_id)
            return original(*args, **kwargs)

        with mock.patch.object(self.b.fetcher, "open", side_effect=open_):
            feature.process_auto_fetches()
        self.assertFalse(self.b.storage.is_complete(Cid.parse(self.a.info(f"{self.cid}/file.txt")["cid"])))
        self.assertEqual(self.b.storage.pins(), {})

    def test_links_past_the_text_kept_are_found(self):
        feature = self.feature_node()
        entry_id = feature.received(message(content=b"x" * 5000 + b" " + self.link.uri().encode()))
        entry = feature.inbox.get(entry_id)
        self.assertEqual([Link.from_json(l) for l in entry["links"]], [self.link])
        self.assertEqual(len(entry["text"]), lxmf.MAX_TEXT_CHARS)

    def test_inbox_get_checks_every_output_first(self):
        # L29: two links with the same last name fail before anything starts
        feature = self.feature_node()
        jobs = mock.Mock()
        jobs.submit.return_value = {"id": "0123abcd"}
        self.b.features["jobs"] = jobs
        other = Cid.parse(self.a.add_stream(io.BytesIO(b"other"), name="file.txt")["cid"])
        both = f"{self.link.uri()}\n{Link(other, path=('file.txt',), from_node=A_ID).uri()}"
        entry_id = feature.received(message(content=both.encode()))
        with self.assertRaisesRegex(ValueError, "links 1 and 2"):
            feature.inbox_get(entry_id, directory=self.tmp.name)
        jobs.submit.assert_not_called()
        # Once one has started, a later failure is reported with it rather than raised
        jobs.submit.side_effect = [{"id": "0123abcd"}, ValueError("too many jobs")]
        third = Cid.parse(self.a.add_stream(io.BytesIO(b"third"), name="b.txt")["cid"])
        two = f"{self.link.uri()}\n{Link(third, path=('b.txt',), from_node=A_ID).uri()}"
        results = feature.inbox_get(feature.received(message(content=two.encode(), number=2)),
                                    directory=self.tmp.name)
        self.assertEqual(results[0]["job"], {"id": "0123abcd"})
        self.assertEqual((results[1]["output"], results[1]["error"]),
                         (os.path.join(self.tmp.name, "b.txt"), "too many jobs"))

    def test_inbox_get_prints_errors_after_the_jobs(self):
        from types import SimpleNamespace
        results = {"results": [{"output": "/x/a", "job": {"id": "0123abcd"}},
                               {"output": "/x/b", "error": "too many jobs\x1b[2J"}]}
        client = mock.Mock()
        client.post.return_value = results
        out, err = io.StringIO(), io.StringIO()
        with mock.patch("rnfs.cli.client", return_value=client), mock.patch("sys.stdout", out), \
                mock.patch("sys.stderr", err), self.assertRaises(SystemExit):
            lxmf._run_inbox_get(SimpleNamespace(output=None, n=1, link=None, use_sent_cid=False))
        self.assertIn("job 0123abcd: /x/a", out.getvalue())
        self.assertIn("/x/b: too many jobs", err.getvalue())
        self.assertNotIn("\x1b", err.getvalue())

    def test_send_prints_escaped(self):
        from types import SimpleNamespace
        client = mock.Mock()
        client.post.return_value = {"hash": "ab\x1b[2J"}
        out = io.StringIO()
        with mock.patch("rnfs.cli.client", return_value=client), mock.patch("sys.stdout", out):
            lxmf._run_send(SimpleNamespace(to="x", target="y", message=None, propagate=False, nearest=False))
        self.assertNotIn("\x1b", out.getvalue())

    def test_fallback_after_the_outbox_record_was_dropped(self):
        feature = self.feature_node(FakeLxmf(propagation_node=OTHER))
        result = feature.send(SENDER.hex(), str(self.cid))
        feature.outbox.remove(result["id"])
        feature.outbound_changed(bytes.fromhex(result["hash"]), "failed", 0.0)
        self.assertEqual(len(self.network.sent), 2)
        self.assertTrue(self.network.sent[1][3])
        self.assertEqual(feature._resend, {})

    def test_failed_construction_releases_the_state_database(self):
        """A constructor that raises after opening the inbox and outbox closes both: the node never closes a
        feature it didn't get."""
        self.feature_node()
        database = statedb._open[statedb._key(self.b.config.state_dir)]
        before = database.references
        with mock.patch.object(lxmf.LxmfFeature, "_release_unrecorded", side_effect=OSError("read-only")), \
                self.assertRaises(OSError):
            lxmf.LxmfFeature(self.b)
        self.assertEqual(database.references, before)

    def test_constructor_sets_every_attribute_before_its_start_up_steps(self):
        """FT1: building the feature over an inbox that isn't clean records an unrecorded lxmf: pin in a new
        message (_release_unrecorded -> _record_kept, which takes the lock and adds to the inbox); every attribute
        the finished feature has is already set when that runs."""
        self.feature_node()
        target = Cid.parse(self.a.info(f"{self.cid}/file.txt")["cid"])
        self.b.storage.pin(target, f"lxmf:{SENDER.hex()}")  # the inbox was never saved, so it isn't clean
        seen = []
        original = lxmf.LxmfFeature._release_unrecorded

        def release_unrecorded(feature):
            """Note the attributes set so far, then run the real start-up step."""
            seen.append(set(vars(feature)))
            original(feature)

        with mock.patch.object(lxmf.LxmfFeature, "_release_unrecorded", release_unrecorded):
            again = self.reloaded()
        self.assertEqual(len(seen), 1)
        self.assertEqual(set(vars(again)) - seen[0], set(), "attributes set only after the start-up steps")
        records = again.inbox_list()
        self.assertEqual([(e["from"], e["auto"]) for e in records],
                         [(SENDER.hex(), [{"state": "pinned", "cid": str(target)}])])
        self.assertEqual(self.b.storage.pins()[target], {f"lxmf:{SENDER.hex()}"})


class MessageLogBoundsTest(unittest.TestCase):
    """MessageLog's protection of pin holders and its byte bound."""

    def setUp(self):
        """A temporary state directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def open(self, limit: int, **settings) -> MessageLog:
        """An inbox log in the state directory (closed before the directory is removed)."""
        log = MessageLog(self.tmp.name, lxmf.INBOX, limit, **settings)
        self.addCleanup(log.close)
        return log

    def test_protected_entries_stay(self):
        log = self.open(3, protect=lambda e: e.get("keep", False))
        log.add({"keep": True})
        for i in range(5):
            log.add({"text": f"m{i}"})
        self.assertEqual([e.get("keep", False) for e in log.entries()], [True, False, False])
        log.add({"keep": True})
        log.add({"keep": True})
        self.assertEqual([e.get("keep", False) for e in log.entries()], [True, True, True])
        entry_id, dropped = log.add({"text": "refused"})
        self.assertIsNone(entry_id)
        self.assertEqual(dropped, [])
        entry_id, dropped = log.add({"keep": True})  # all protected: the oldest goes
        self.assertEqual((entry_id, [e["id"] for e in dropped]), (10, [1]))

    def test_per_from_and_bytes(self):
        log = self.open(100, max_bytes=1000)
        for i in range(5):
            log.add({"from": "a", "text": str(i)}, per_from=2)
        self.assertEqual([e["text"] for e in log.entries()], ["3", "4"])
        for i in range(20):
            log.add({"from": f"s{i}", "text": "x" * 100})
        self.assertLessEqual(sum(lxmf._json_bytes(e) for e in log.entries()), 1000)
        self.assertEqual(log.entries()[-1]["from"], "s19")


class NetworkBoundsTest(unittest.TestCase):
    """LxmfNetwork's held messages, path requests and propagation node downloads (no running Reticulum)."""

    def network(self) -> lxmf.LxmfNetwork:
        """An LxmfNetwork with a stand-in router and feature."""
        try:
            import LXMF
        except ImportError:
            self.skipTest("LXMF is not installed")
        import threading
        network = lxmf.LxmfNetwork.__new__(lxmf.LxmfNetwork)
        network.RNS, network.LXMF = RNS, LXMF
        network.router = mock.Mock()
        network.feature = mock.Mock()
        network._lock = threading.Lock()
        network._held, network._path_requested = [], {}
        network._path_tokens, network._path_tokens_at = float(lxmf.PATH_REQUEST_BURST), 0.0
        network._propagation_nodes = {}
        network._configured_pn = network._current_pn = None
        network._sync_started = float("-inf")
        network._outbound = {}
        network.identity = object()
        return network

    def forged(self, source: bytes, tag: bytes):
        """A message claiming an unknown source."""
        return mock.Mock(signature_validated=False, unverified_reason=self.network_lxmf.LXMessage.SOURCE_UNKNOWN,
                         source_hash=source, packed=os.urandom(16) + source + tag, method=2)

    def test_forged_sources_are_rate_limited_and_keep_held(self):
        # RM-M3 / DS-L5 (PoC held_pathreq.py)
        network = self.network()
        self.network_lxmf = network.LXMF
        calls = []
        legit = self.forged(os.urandom(16), b"LEGIT")
        with mock.patch.object(RNS.Transport, "request_path", side_effect=calls.append), \
                mock.patch.object(lxmf.time, "monotonic", return_value=1000.0):
            network._delivered(legit)
            network._delivered(legit)  # the same message again isn't held twice
            for _ in range(1000):
                network._delivered(self.forged(os.urandom(16), b"junk"))
        self.assertEqual(len(calls), lxmf.PATH_REQUEST_BURST)
        self.assertEqual(len(network._held), lxmf.HOLD_LIMIT)
        self.assertTrue(network._held[0][1].endswith(b"LEGIT"))
        # Tokens come back with time, and the held senders not asked yet get their path requests then
        with mock.patch.object(RNS.Transport, "request_path", side_effect=calls.append), \
                mock.patch.object(RNS.Identity, "recall", return_value=None), \
                mock.patch.object(lxmf.time, "monotonic", return_value=1000.0 + lxmf.PATH_REQUEST_INTERVAL):
            network._retry_held()
        self.assertEqual(len(calls), lxmf.PATH_REQUEST_BURST + 1)
        self.assertTrue(network._held[0][1].endswith(b"LEGIT"))

    def test_senders_outside_accept_from_are_not_held(self):
        # L25: with lxmf_accept_from set, forged sources can't fill the queue ahead of an allowed sender
        network = self.network()
        self.network_lxmf = network.LXMF
        allowed = os.urandom(16)
        network.feature.accepts.side_effect = lambda source: source == allowed
        calls = []
        with mock.patch.object(RNS.Transport, "request_path", side_effect=calls.append):
            for _ in range(lxmf.HOLD_LIMIT + 5):
                network._delivered(self.forged(os.urandom(16), b"junk"))
            network._delivered(self.forged(allowed, b"LEGIT"))
        self.assertEqual([p[-5:] for _, p, _ in network._held], [b"LEGIT"])
        self.assertEqual(calls, [allowed])

    def test_messages_arriving_during_a_retry_count_against_the_limit(self):
        # Info: the queue isn't emptied while held messages are retried, so none is accepted, then cut silently
        network = self.network()
        self.network_lxmf = network.LXMF
        network._delivered(self.forged(os.urandom(16), b"FIRST"))
        arrivals = []

        def recall(source):
            """LXMF delivers many messages while the retry runs."""
            if not arrivals:
                for i in range(lxmf.HOLD_LIMIT + 8):
                    arrivals.append(i)
                    network._delivered(self.forged(os.urandom(16), b"new%02d" % i))
            return None

        with mock.patch.object(network, "_request_path") as request_path, \
                mock.patch.object(RNS.Identity, "recall", side_effect=recall):
            network._retry_held()
        tags = [p[-5:] for _, p, _ in network._held]
        self.assertEqual(tags, [b"FIRST"] + [b"new%02d" % i for i in range(lxmf.HOLD_LIMIT - 1)])
        # Every message accepted (its sender's path asked for) is still held
        self.assertEqual({call.args[0] for call in request_path.call_args_list},
                         {p[lxmf.ID_LENGTH:2 * lxmf.ID_LENGTH] for _, p, _ in network._held})
        # A held message is dropped from the queue once done with: here, when it waited too long
        with mock.patch.object(RNS.Identity, "recall", return_value=None), \
                mock.patch.object(lxmf.time, "monotonic", return_value=float("inf")):
            network._retry_held()
        self.assertEqual(network._held, [])

    def test_propagation_node_resources_are_bounded(self):
        # RM-ML4: a response too large, or of several segments, stops the download
        network = self.network()
        for size, segments, refused in ((1000, 1, False), (lxmf.MAX_PN_RESOURCE + 1, 1, True), (1000, 2, True)):
            resource = mock.Mock()
            resource.get_data_size.return_value = size
            resource.get_transfer_size.return_value = size
            resource.get_segments.return_value = segments
            network._pn_resource(resource)
            self.assertEqual(resource.max_decompressed_size, lxmf.MAX_PN_RESOURCE)
            self.assertEqual(resource.cancel.called, refused)
            self.assertEqual(resource.link.teardown.called, refused)
        link = mock.Mock(rnfs_bounded=False)
        network.router.outbound_propagation_link = link
        network._bound_propagation_downloads()
        link.set_resource_started_callback.assert_called_once_with(network._pn_resource)

    def test_delivery_resources_of_several_segments_are_refused(self):
        # Each segment states its own data size, so segments within the delivery limit added up without limit
        original = mock.Mock(return_value=True)
        advertised = lxmf.LxmfNetwork._delivery_advertised(original)
        for segments, index, accepted in ((1, 1, True), (2, 1, False), (100000, 1, False), (1, 2, False)):
            advertisement = mock.Mock(i=index)
            advertisement.get_segments.return_value = segments
            original.reset_mock()
            self.assertEqual(advertised(advertisement), accepted)
            self.assertEqual(original.called, accepted)
        original.return_value = False  # LXMF's own size check still applies to one segment
        advertisement = mock.Mock(i=1)
        advertisement.get_segments.return_value = 1
        self.assertFalse(advertised(advertisement))

    def test_sync_only_with_a_configured_node_unless_nearest(self):
        network = self.network()
        network.router.PR_PATH_REQUESTED, network.router.PR_COMPLETE = 1, 7
        network.router.propagation_transfer_state = 0
        network._propagation_nodes[OTHER] = __import__("time").time()
        with mock.patch.object(RNS.Transport, "hops_to", return_value=1):
            self.assertFalse(network.sync())
            network.router.request_messages_from_propagation_node.assert_not_called()
            self.assertTrue(network.sync(nearest=True))
        network.router.request_messages_from_propagation_node.assert_called_once_with(
            network.identity, lxmf.SYNC_MAX_MESSAGES)

    def test_idle_and_wakes(self):
        # S7: the feature's loop polls only while a message is followed or held or a download runs, and the
        # network wakes it when one starts
        network = self.network()
        self.network_lxmf = LXMF = network.LXMF
        router, wake = network.router, network.feature.wake
        router.PR_PATH_REQUESTED, router.PR_COMPLETE = LXMF.LXMRouter.PR_PATH_REQUESTED, LXMF.LXMRouter.PR_COMPLETE
        router.propagation_transfer_state = LXMF.LXMRouter.PR_IDLE
        self.assertTrue(network.idle())
        with mock.patch.object(RNS.Transport, "request_path"):
            network._delivered(self.forged(os.urandom(16), b"HELD"))
        wake.assert_called_once_with()
        self.assertFalse(network.idle())
        network._held = []
        self.assertTrue(network.idle())
        for state, idle in ((LXMF.LXMRouter.PR_PATH_REQUESTED, False), (LXMF.LXMRouter.PR_RECEIVING, False),
                            (LXMF.LXMRouter.PR_COMPLETE, True), (LXMF.LXMRouter.PR_FAILED, True)):
            router.propagation_transfer_state = state
            self.assertEqual(network.idle(), idle, state)
        router.propagation_transfer_state = LXMF.LXMRouter.PR_IDLE
        network._configured_pn = OTHER
        wake.reset_mock()
        self.assertTrue(network.sync())
        wake.assert_called_once_with()
        # Sending: followed (and the loop woken) from when it is queued
        wake.reset_mock()
        network.LXMF, network.destination = mock.Mock(), mock.Mock()
        network.LXMF.LXMessage.return_value = mock.Mock(hash=b"m" * 32)
        with mock.patch.object(RNS.Identity, "recall", return_value=mock.Mock()), \
                mock.patch.object(RNS, "Destination"):
            self.assertEqual(network.send(SENDER, "hi", {}, False), b"m" * 32)
        wake.assert_called_once_with()
        self.assertFalse(network.idle())

    def test_settings(self):
        # BG-L7 and the new lxmf_send_from_hint
        for settings in ({"lxmf_enabled": 0}, {"lxmf_enabled": 1}, {"lxmf_send_from_hint": "no"},
                         {"lxmf_send_from_hint": 0}):
            with self.assertRaises(ValueError, msg=settings):
                Config(**settings).validate()
        Config(lxmf_enabled=False, lxmf_send_from_hint=False).validate()


if __name__ == "__main__":
    unittest.main()
