"""Migration of the follow, keepers and LXMF state files of rnfs 0.1.3 and 0.1.4 into the state database:
follow.json, keepers.json, lxmf_inbox.json and lxmf_outbox.json (both versions wrote them alike).

For each: a valid file imports exactly, a damaged one is handled as those versions handled it (follow and the
inbox then aren't clean, so the reconcile at start keeps their pins), an import interrupted after its commit
isn't repeated, a second start changes nothing, a .migrated file left behind is ignored, and an import whose
commit fails leaves the file to import at the next start."""
import json
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs import statedb
from rnfs.config import Config
from rnfs.content.cid import RAW, Cid
from rnfs.features import lxmf
from rnfs.features.keepers import FOLLOW, OWN, USER, KeeperStore, sign_record
from rnfs.names import encode_record, name_for_identity
from rnfs.node import RnfsNode
from tests.support import failing_commits, state_execute, state_rows, write_legacy
from tests.test_names import FakeNetwork
from tests.test_node import NetClient

A, B, C = (Cid.for_block(RAW, bytes([i])) for i in range(3))
N1, N2, N3 = bytes(range(16)), bytes([0xEE] * 16), bytes([0x33] * 16)
SENDER = "51" * 16

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


def written(value) -> str:
    """value as rnfs 0.1.4 wrote the LXMF logs: UTF-8 JSON, non-ASCII as is, a lone surrogate as its escape."""
    return json.dumps(value, ensure_ascii=False).encode("utf-8", "backslashreplace").decode("utf-8")


class MigrateTest(unittest.TestCase):
    """An rnfs directory under a temporary directory, with the state files written into <rnfs dir>/state."""

    FILE = ""

    def setUp(self):
        """An empty rnfs directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = Config(rnfs_dir=os.path.join(self.tmp.name, "node"))
        self.state = self.config.state_dir

    def start(self):
        """Open and close the state database, as every start does first: the files there are imported."""
        statedb.connect(self.state).close()

    def node(self) -> RnfsNode:
        """An offline node over the directory (stopped before the directory is removed)."""
        node = RnfsNode.offline(self.config, b"\x07" * 16, NetClient(), FakeNetwork())
        self.addCleanup(node.stop)
        return node

    def rows(self) -> list:
        """The store's rows and meta values, as the database holds them."""
        raise NotImplementedError

    def records(self) -> int:
        """The records the migrated table says the file had."""
        return state_rows(self.state, "SELECT records FROM migrated WHERE file = ?", (self.FILE,))[0][0]


class Common:
    """Tests every file passes (mixed into a MigrateTest giving FILE, VALUE, CHANGED and COUNT)."""

    def test_interrupted_migration_isnt_repeated(self):
        """A crash between the commit and the rename: the next start only renames the file, whatever it holds
        by then, and the rows stay as imported."""
        path = write_legacy(self.state, self.FILE, text=written(self.VALUE))
        with mock.patch.object(statedb, "_keep_original", return_value=None):
            self.start()
        self.assertTrue(os.path.exists(path))
        imported = self.rows()
        write_legacy(self.state, self.FILE, self.CHANGED)
        self.start()
        self.assertEqual(self.rows(), imported)
        self.assertFalse(os.path.exists(path))
        with open(path + statedb.MIGRATED_SUFFIX, encoding="utf-8") as f:
            self.assertEqual(json.load(f), self.CHANGED)  # the file as it was, kept
        self.assertEqual(self.records(), self.COUNT)

    def test_second_start_and_a_left_migrated_file_change_nothing(self):
        """Once imported, another start, or a .migrated file left behind, changes nothing."""
        write_legacy(self.state, self.FILE, text=written(self.VALUE))
        self.start()
        imported = self.rows()
        self.assertTrue(imported)
        self.start()
        write_legacy(self.state, self.FILE + statedb.MIGRATED_SUFFIX, self.CHANGED)
        self.start()
        self.assertEqual(self.rows(), imported)
        self.assertEqual(state_rows(self.state, "SELECT count(*) FROM migrated"), [(1,)])

    def test_failed_commit_leaves_the_file(self):
        """An import whose commit fails stops the start, imports nothing and leaves the file, imported by the
        next start."""
        self.start()  # the database exists: only the import's commit fails
        path = write_legacy(self.state, self.FILE, text=written(self.VALUE))
        with failing_commits():
            with self.assertRaises(OSError):
                self.start()
        self.assertTrue(os.path.exists(path))
        self.assertEqual(state_rows(self.state, "SELECT count(*) FROM migrated"), [(0,)])
        self.start()
        self.assertFalse(os.path.exists(path))
        self.assertEqual(self.records(), self.COUNT)


class FollowTest(Common, MigrateTest):
    """follow.json: {"version": 1, "names": {name: {"pinned": cid or null}}}."""

    FILE = "follow.json"
    VALUE = {"version": 1, "names": {N1.hex(): {"pinned": str(A)}, N2.hex(): {"pinned": None}}}
    CHANGED = {"version": 1, "names": {N3.hex(): {"pinned": str(C)}}}
    COUNT = 2

    def rows(self) -> list:
        """The follow table and its meta values."""
        return (state_rows(self.state, "SELECT name, pinned FROM follow ORDER BY name")
                + state_rows(self.state, "SELECT key, value FROM meta WHERE store = 'follow'"))

    def clean(self):
        """The follow state's clean value (None if missing)."""
        rows = state_rows(self.state, "SELECT value FROM meta WHERE store = 'follow' AND key = 'clean'")
        return rows[0][0] if rows else None

    def test_valid_file_imports_exactly(self):
        """Every name and its pin is imported, and the state is clean; a node follows them."""
        write_legacy(self.state, self.FILE, self.VALUE)
        node = self.node()
        self.assertEqual({(e["name"], e["pinned"]) for e in node.feature("follow").following()},
                         {(N1.hex(), str(A)), (N2.hex(), None)})
        self.assertEqual(self.rows(), [(N1, A.binary), (N2, None), ("clean", 1)])
        self.assertTrue(os.path.exists(os.path.join(self.state, "follow.json.migrated")))

    def test_malformed_entries_are_skipped_and_not_clean(self):
        """Entries that don't parse, or aren't names, are skipped as rnfs 0.1.4 skipped them; the state isn't
        clean then (the reconcile at start keeps follow pins it doesn't list)."""
        write_legacy(self.state, self.FILE, {"names": {N1.hex(): {"pinned": str(A)}, "zz": {"pinned": None},
                                                       "abcd": {"pinned": None}, N2.hex(): {"pinned": "bad"},
                                                       N3.hex(): "not a dict"}})
        self.start()
        self.assertEqual(self.rows(), [(N1, A.binary), ("clean", 0)])
        self.assertEqual(self.records(), 1)

    def test_damaged_file_is_moved_aside_and_not_clean(self):
        """A file that isn't JSON is moved to .damaged and nothing is imported: the clean value stays missing.
        One holding no names is imported as not clean, as 0.1.4 loaded it."""
        write_legacy(self.state, self.FILE, text="{not json")
        self.start()
        self.assertTrue(os.path.exists(os.path.join(self.state, "follow.json.damaged")))
        self.assertIsNone(self.clean())
        for value in ([], {"names": []}):
            with self.subTest(value=value):
                write_legacy(self.state, self.FILE, value)
                forget_import(self.state, self.FILE)
                self.start()
                self.assertEqual(self.rows(), [("clean", 0)])

    def test_reconcile_after_the_upgrade(self):
        """Upgrading with a damaged follow.json, the pin of a name it lost is kept and the name followed again;
        with a whole one, a follow pin of a name it doesn't list is released."""
        source = "follow:" + N3.hex()
        for follow, kept in (("{not json", True), (json.dumps(self.VALUE), False)):
            with self.subTest(follow=follow):
                self.config = Config(rnfs_dir=os.path.join(self.tmp.name, "node" + str(kept)))
                self.state = self.config.state_dir
                write_legacy(self.state, "pins.json", {"pins": {str(C): [source]}})
                write_legacy(self.state, self.FILE, text=follow)
                node = self.node()
                self.assertEqual(node.storage.pinned_by(source), {C} if kept else set())
                self.assertEqual(N3.hex() in [e["name"] for e in node.feature("follow").following()], kept)


class KeepersTest(Common, MigrateTest):
    """keepers.json: {"version": 1, "names": {name: {"reasons": [...], "record": [field hex, ...] or null}}}."""

    FILE = "keepers.json"
    COUNT = 2

    @classmethod
    def setUpClass(cls):
        """A name key and a record it signed."""
        cls.identity = RNS.Identity()
        cls.name = name_for_identity(cls.identity)
        cls.record = sign_record(cls.identity, cls.name, encode_record(7, A), 1_700_000_000)
        cls.fields = [field.hex() for field in cls.record.wire()]
        cls.VALUE = {"version": 1, "names": {cls.name.hex(): {"reasons": [FOLLOW, USER], "record": cls.fields},
                                             N1.hex(): {"reasons": [OWN], "record": None}}}
        cls.CHANGED = {"version": 1, "names": {N2.hex(): {"reasons": [USER], "record": None}}}

    def rows(self) -> list:
        """The keepers table."""
        return state_rows(self.state, "SELECT name, reasons, record FROM keepers ORDER BY name")

    def load(self) -> dict:
        """Name -> (reasons, the record's sequence or None), as a store loads them."""
        store = KeeperStore(self.state, 10)
        try:
            return {name: (store.reasons(name), store.get(name).sequence if store.get(name) else None)
                    for name in store.names()}
        finally:
            store.close()

    def test_valid_file_imports_exactly(self):
        """Every name, its reasons and its record are imported; the record verifies at load."""
        write_legacy(self.state, self.FILE, self.VALUE)
        self.assertEqual(self.load(), {self.name: ({FOLLOW, USER}, 7), N1: ({OWN}, None)})
        self.assertEqual(sorted(self.rows()), sorted([(self.name, '["follow", "user"]', json.dumps(self.fields)),
                                                      (N1, '["own"]', None)]))

    def test_malformed_entries_are_skipped(self):
        """Entries that don't parse, aren't names or have no known reason are skipped; a record that doesn't
        verify is dropped and its name kept, as rnfs 0.1.4 loaded them."""
        forged = list(self.fields)
        forged[4] = "00" * 64  # the signature
        write_legacy(self.state, self.FILE, {"names": {
            self.name.hex(): {"reasons": [USER, "nobody"], "record": forged}, "zz": {"reasons": [USER]},
            "abcd": {"reasons": [USER]}, N1.hex(): {"reasons": ["nobody"]}, N2.hex(): {"record": None},
            N3.hex(): {"reasons": [USER], "record": ["zz"]}, bytes([4] * 16).hex(): {"reasons": 5}}})
        self.assertEqual(self.load(), {self.name: ({USER}, None)})
        self.assertEqual(self.rows(), [(self.name, '["user"]', None)])
        self.assertEqual(self.records(), 1)

    def test_damaged_file_is_moved_aside(self):
        """A file that isn't JSON is moved to .damaged and nothing is kept; any other value without names
        imports nothing, as rnfs 0.1.4 loaded it."""
        write_legacy(self.state, self.FILE, text="{not json")
        self.assertEqual(self.load(), {})
        self.assertTrue(os.path.exists(os.path.join(self.state, "keepers.json.damaged")))
        write_legacy(self.state, self.FILE, [1, 2])
        self.assertEqual(self.load(), {})
        self.assertEqual(self.records(), 0)


INBOX_RECORDS = [
    {"hash": "aa" * 32, "from": SENDER, "display_name": "Ünïcode \udc80 \U0001F600", "sent": 1.7e9,
     "received": 1.7e9 + 1, "title": "tïtle \ud83d", "text": "rnfs:x ✓ \U0010FFFF", "method": "direct",
     "links": [{"uri": f"rnfs:{A}", "size": 3}], "auto": [{"state": "pinned", "cid": str(A)}], "id": 3},
    {"hash": "bb" * 32, "from": SENDER, "display_name": None, "sent": None, "received": 1.7e9 + 2, "title": "",
     "text": "", "method": "propagated", "links": [{"uri": f"rnfs:{B}"}], "auto": None, "id": 5},
]
OUTBOX_RECORDS = [
    {"hash": "cc" * 32, "to": SENDER, "time": 1.7e9, "links": [{"uri": f"rnfs:{A}"}], "text": "héllo \udc80",
     "method": "direct", "state": "delivered", "progress": 1.0, "id": 1},
    {"hash": "dd" * 32, "to": SENDER, "time": 1.7e9, "links": [{"uri": f"rnfs:{B}"}], "text": "",
     "method": "direct", "state": "outbound", "progress": 0.0, "id": 2},
]


class LogTests:
    """Tests of a message log file, {"next_id": n, "messages": [record, ...]} oldest first (mixed into a
    MigrateTest giving the box and its bounds)."""

    BOX = ""
    LIMIT = 0
    MAX_BYTES = None
    VALID = None
    RECORDS: list = []

    def rows(self) -> list:
        """The box's rows and meta values."""
        return (state_rows(self.state, "SELECT id, record FROM lxmf_messages WHERE box = ? ORDER BY id", (self.BOX,))
                + state_rows(self.state, "SELECT key, value FROM meta WHERE store = ? ORDER BY key", (self.BOX,)))

    def load(self) -> tuple[list[dict], bool, int]:
        """The records a log of the box loads, whether it is clean, and the next id."""
        log = lxmf.MessageLog(self.state, self.BOX, self.LIMIT, self.MAX_BYTES, valid=self.VALID)
        try:
            return log.entries(), log.clean, log._next_id
        finally:
            log.close()

    def test_valid_file_imports_exactly(self):
        """Every record is imported exactly (lone surrogates and other non-ASCII text included), in order,
        with the next id; the box is clean."""
        write_legacy(self.state, self.FILE, text=written({"next_id": 9, "messages": self.RECORDS}))
        self.assertEqual(self.load(), (self.RECORDS, True, 9))
        self.assertEqual([list(e) for e in self.load()[0]], [list(e) for e in self.RECORDS])  # key order too
        self.assertEqual(self.records(), len(self.RECORDS))

    def test_malformed_records_are_dropped_and_not_clean(self):
        """Records that aren't dicts with a new positive integer id, that the box's check refuses, or whose id
        SQLite can't store are dropped (with a warning), as rnfs 0.1.4 dropped them; the box isn't clean."""
        good = self.RECORDS[0]
        messages = [good, {**good, "id": "4"}, {**good, "id": 0}, {**good, "id": True}, "junk", {**good},
                    {**good, "id": 2 ** 63}, {**good, "id": 2 ** 70}, {**good, "id": 8, "links": "x"}]
        write_legacy(self.state, self.FILE, text=written({"next_id": "x", "messages": messages}))
        with mock.patch.object(lxmf, "_log") as warned:
            entries, clean, next_id = self.load()
        self.assertEqual((entries, clean, next_id), ([good], False, good["id"] + 1))
        self.assertEqual(warned.call_args.args[0], f"dropped 8 malformed record(s) from {self.FILE}")
        self.assertEqual(self.records(), 1)

    def test_exhausted_ids_refuse_new_records(self):
        """A next id at SQLite's largest integer (only a hand-edited file holds one) is refused; one just below
        it is the last id given out, and saves keep working: no id or next id past 64 bits is ever stored."""
        statedb.connect(self.state).close()
        for saved, expected in ((2 ** 63 - 1, 1), (lxmf.MAX_ID, lxmf.MAX_ID)):
            with self.subTest(saved=saved):
                forget_import(self.state, self.FILE)
                write_legacy(self.state, self.FILE, text=written({"next_id": saved, "messages": []}))
                log = lxmf.MessageLog(self.state, self.BOX, self.LIMIT, self.MAX_BYTES, valid=self.VALID)
                try:
                    self.assertEqual(log._next_id, expected)
                    log._next_id = lxmf.MAX_ID + 1  # as if every id up to MAX_ID were used
                    self.assertEqual(log.add(dict(self.RECORDS[0]))[0], None)
                    log.save()
                finally:
                    log.close()

    def test_limits_apply(self):
        """Past the box's limit (and the inbox's byte bound) the oldest records are dropped, as rnfs 0.1.4 did
        at load; that leaves the box clean."""
        messages = [{**self.RECORDS[0], "id": n, "text": "x" * 1000} for n in range(1, 6)]
        write_legacy(self.state, self.FILE, text=written({"next_id": 2, "messages": messages}))
        with mock.patch.object(lxmf, self.LIMIT_NAME, 3):
            self.start()
        self.assertEqual([row_id for row_id, _ in self.rows()[:-2]], [3, 4, 5])
        self.assertEqual(self.rows()[-2:], [("clean", 1), ("next_id", 6)])

    def test_damaged_file_is_moved_aside(self):
        """A file that isn't JSON is moved to .damaged and nothing is imported (the box isn't clean: it was
        never saved); one that isn't an object, or whose messages aren't a list, imports nothing and isn't
        clean."""
        write_legacy(self.state, self.FILE, text="{not json")
        self.assertEqual(self.load(), ([], False, 1))
        self.assertTrue(os.path.exists(os.path.join(self.state, self.FILE + ".damaged")))
        for value in ([self.RECORDS[0]], {"messages": {"1": self.RECORDS[0]}, "next_id": 4}):
            with self.subTest(value=value):
                forget_import(self.state, self.FILE)
                write_legacy(self.state, self.FILE, value)
                self.assertEqual(self.load()[:2], ([], False))


class InboxTest(Common, LogTests, MigrateTest):
    """lxmf_inbox.json."""

    FILE = "lxmf_inbox.json"
    BOX = lxmf.INBOX
    LIMIT = lxmf.INBOX_LIMIT
    LIMIT_NAME = "INBOX_LIMIT"
    MAX_BYTES = lxmf.MAX_INBOX_BYTES
    VALID = staticmethod(lxmf._valid_inbox_entry)
    RECORDS = INBOX_RECORDS
    VALUE = {"next_id": 6, "messages": INBOX_RECORDS}
    CHANGED = {"next_id": 2, "messages": [{**INBOX_RECORDS[1], "id": 1}]}
    COUNT = 2

    def test_byte_bound_applies(self):
        """Past MAX_INBOX_BYTES the oldest records are dropped, one kept whatever its size."""
        messages = [{**self.RECORDS[1], "id": n, "text": "é" * 1000} for n in range(1, 4)]
        write_legacy(self.state, self.FILE, text=written({"messages": messages}))
        with mock.patch.object(lxmf, "MAX_INBOX_BYTES", 5000):
            self.start()
        self.assertEqual([row_id for row_id, _ in self.rows()[:-2]], [2, 3])

    def test_reconcile_after_the_upgrade(self):
        """Upgrading with the inbox, a node's LXMF feature lists its messages and keeps the pins they record;
        with a damaged inbox, a pin no message records is kept and recorded in a new message; with a whole one,
        it is released."""
        source = "lxmf:" + SENDER
        for inbox, kept in (("{not json", True), (written(self.VALUE), False)):
            with self.subTest(inbox=inbox):
                self.config = Config(rnfs_dir=os.path.join(self.tmp.name, "node" + str(kept)))
                self.state = self.config.state_dir
                write_legacy(self.state, "pins.json", {"pins": {str(A): [source], str(C): [source]}})
                write_legacy(self.state, self.FILE, text=inbox)
                node = self.node()
                messages = node.feature("lxmf").inbox_list()
                self.assertEqual(node.storage.pinned_by(source), {A, C} if kept else {A})
                if kept:
                    self.assertEqual([m["auto"] for m in messages], [[{"state": "pinned", "cid": str(A)},
                                                                      {"state": "pinned", "cid": str(C)}]])
                else:
                    self.assertEqual(messages, self.RECORDS)


class OutboxTest(Common, LogTests, MigrateTest):
    """lxmf_outbox.json."""

    FILE = "lxmf_outbox.json"
    BOX = lxmf.OUTBOX
    LIMIT = lxmf.OUTBOX_LIMIT
    LIMIT_NAME = "OUTBOX_LIMIT"
    VALID = staticmethod(lxmf._valid_outbox_entry)
    RECORDS = OUTBOX_RECORDS
    VALUE = {"next_id": 3, "messages": OUTBOX_RECORDS}
    CHANGED = {"next_id": 2, "messages": [OUTBOX_RECORDS[0]]}
    COUNT = 2

    def test_unfinished_messages_fail_at_the_first_start(self):
        """A message still unfinished in the file is marked failed when the node starts, as at any restart."""
        write_legacy(self.state, self.FILE, text=written(self.VALUE))
        node = self.node()
        self.assertEqual([e["state"] for e in node.feature("lxmf").outbox_list()], ["delivered", "failed"])


def forget_import(state_dir: str, name: str):
    """Forget that name was imported (and its .migrated copy), so another version of it is imported."""
    state_execute(state_dir, "DELETE FROM migrated WHERE file = ?", (name,))
    path = os.path.join(state_dir, name + statedb.MIGRATED_SUFFIX)
    if os.path.exists(path):
        os.remove(path)


if __name__ == "__main__":
    unittest.main()
