"""Migration of the small state files of rnfs 0.1.3 and 0.1.4 into the state database: pin_names.json,
shared_folder.json, jobs.json and contacts.json.

For each: a valid file imports exactly, a damaged one is handled as those versions handled it, an import
interrupted after its commit isn't repeated, a second start changes nothing, a .migrated file left behind is
ignored, and an import whose commit fails is left for the next start."""
import json
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs import statedb
from rnfs.config import Config
from rnfs.content.cid import RAW, Cid
from rnfs.content.shared_folder import SharedFolder
from rnfs.content.storage import Storage
from rnfs.features.contacts import ContactsFeature
from rnfs.features.jobs import JobsFeature
from rnfs.pin_names import MAX_NAME_LENGTH, PinNames
from tests.support import failing_commits, state_rows, write_legacy

A, B, C = (Cid.for_block(RAW, bytes([i])) for i in range(3))
NAME_ID, NODE_ID = "0123456789abcdef0123456789abcdef", "fedcba9876543210fedcba9876543210"

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class FeatureNode:
    """What JobsFeature and ContactsFeature use of a node: its config and storage."""

    def __init__(self, config: Config, storage: Storage):
        """A node with config and storage, no features and not slow."""
        self.config = config
        self.storage = storage
        self.slow = False
        self.features = {}


class MigrateCases:
    """The cases every file is checked for (mixed into a TestCase per file, which sets FILE, VALUE, CHANGED,
    COUNT, DAMAGED and load)."""

    FILE = ""
    VALUE = None  # the file as rnfs 0.1.3 and 0.1.4 wrote it
    CHANGED = None  # another valid file
    COUNT = 0  # records VALUE imports
    DAMAGED = ("{not json", "")  # files moved aside to .damaged

    def setUp(self):
        """An empty rnfs directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(self.assert_closed)  # after every store's close, before the directory goes
        self.config = Config(rnfs_dir=os.path.join(self.tmp.name, "node"))
        self.state = self.config.state_dir
        self.path = os.path.join(self.state, self.FILE)

    def assert_closed(self):
        """No state database under the temporary directory is left open (Windows can't delete open files)."""
        root = os.path.realpath(self.tmp.name)
        self.assertEqual([path for path in statedb.open_databases() if os.path.realpath(path).startswith(root)], [])

    def storage(self) -> Storage:
        """Storage over the node's directories (closed before the directory is removed)."""
        storage = Storage(self.config.blocks_dir, self.state, 10**9)
        self.addCleanup(storage.close)
        return storage

    def closing(self, store):
        """store, closed before the directory is removed."""
        self.addCleanup(store.close)
        return store

    def load(self):
        """What the store loads (starting it afresh)."""
        raise NotImplementedError

    def migrated(self) -> list[tuple]:
        """The migrated table's record of this file."""
        return state_rows(self.state, "SELECT file, records FROM migrated WHERE file = ?", (self.FILE,))

    def test_valid_file_imports_exactly(self):
        """Every record is imported, the import recorded and the file kept as <name>.migrated; a second start
        changes nothing."""
        write_legacy(self.state, self.FILE, self.VALUE)
        loaded = self.load()
        self.assertEqual(loaded, self.EXPECTED)
        self.assertEqual(self.migrated(), [(self.FILE, self.COUNT)])
        self.assertFalse(os.path.exists(self.path))
        with open(self.path + statedb.MIGRATED_SUFFIX, encoding="utf-8") as f:
            self.assertEqual(json.load(f), self.VALUE)
        self.assertEqual(self.load(), loaded)
        self.assertEqual(self.migrated(), [(self.FILE, self.COUNT)])
        self.assertFalse(os.path.exists(self.path + statedb.MIGRATED_SUFFIX + ".1"))

    def test_damaged_file_is_moved_aside(self):
        """A file that isn't JSON (or has the wrong shape) is moved to .damaged and the store starts without it."""
        for n, text in enumerate(self.DAMAGED):
            with self.subTest(text=text):
                write_legacy(self.state, self.FILE, text=text)
                self.assertEqual(self.load(), self.EMPTY)
                self.assertFalse(os.path.exists(self.path))
                self.assertTrue(os.path.exists(self.path + ".damaged" + (f".{n}" if n else "")))
        self.assertEqual(self.migrated(), [])

    def test_interrupted_migration_is_not_repeated(self):
        """A crash between the import's commit and the rename leaves the file: the next start only renames it,
        whatever it holds by then."""
        write_legacy(self.state, self.FILE, self.VALUE)
        with mock.patch.object(statedb, "_keep_original", return_value=None):
            first = self.load()
        self.assertTrue(os.path.exists(self.path))
        write_legacy(self.state, self.FILE, self.CHANGED)
        self.assertEqual(self.load(), first)
        self.assertEqual(first, self.EXPECTED)
        self.assertFalse(os.path.exists(self.path))
        with open(self.path + statedb.MIGRATED_SUFFIX, encoding="utf-8") as f:
            self.assertEqual(json.load(f), self.CHANGED)  # the file as it was, kept
        self.assertEqual(self.migrated(), [(self.FILE, self.COUNT)])

    def test_migrated_file_left_behind_is_ignored(self):
        """A <name>.migrated file is never imported."""
        write_legacy(self.state, self.FILE + statedb.MIGRATED_SUFFIX, self.VALUE)
        self.assertEqual(self.load(), self.EMPTY)
        self.assertEqual(self.migrated(), [])

    def test_failed_commit_leaves_the_file_for_the_next_start(self):
        """An import whose commit fails stops the start and changes nothing; the next start imports the file."""
        self.load()  # the database exists, so only the import's commit fails
        write_legacy(self.state, self.FILE, self.VALUE)
        with failing_commits(), self.assertRaises(OSError):
            self.load()
        self.assertTrue(os.path.exists(self.path))
        self.assertEqual(self.migrated(), [])
        self.assertEqual(self.load(), self.EXPECTED)


class PinNamesTest(MigrateCases, unittest.TestCase):
    """pin_names.json: {cid: name}."""

    FILE = "pin_names.json"
    VALUE = {str(A): "/home/u/caf\udce9.txt", str(B): "(stdin)", str(C): "x" * (MAX_NAME_LENGTH + 1)}
    CHANGED = {str(A): "elsewhere"}
    COUNT = 3
    EXPECTED = {A: "/home/u/caf\udce9.txt", B: "(stdin)", C: "x" * MAX_NAME_LENGTH}
    EMPTY: dict = {}
    DAMAGED = ("{not json", "", "[]", '"names"')

    def load(self) -> dict:
        """The names PinNames loads."""
        return self.closing(PinNames(self.state))._names

    def test_invalid_records_are_skipped(self):
        """Records that don't parse or whose name isn't a string are skipped, as rnfs 0.1.4 skipped them."""
        write_legacy(self.state, self.FILE, {"not a cid": "x", str(A): 5, str(B): ["x"], str(C): "ok"})
        self.assertEqual(self.load(), {C: "ok"})
        self.assertEqual(self.migrated(), [(self.FILE, 1)])


class SharedFolderTest(MigrateCases, unittest.TestCase):
    """shared_folder.json: {"entries": {name: [signature, cid]}}."""

    FILE = "shared_folder.json"
    VALUE = {"entries": {"a.txt": ["5f" * 32, str(A)], "caf\udce9": ["6e" * 32, str(B)]}}
    CHANGED = {"entries": {"other": ["00" * 32, str(C)]}}
    COUNT = 2
    EXPECTED = {"a.txt": ("5f" * 32, A), "caf\udce9": ("6e" * 32, B)}
    EMPTY: dict = {}
    DAMAGED = ("{not json", "", "[]", '{"entries": ["a"]}')

    def load(self) -> dict:
        """The signature cache SharedFolder loads."""
        return self.closing(SharedFolder(self.config.files_dir, self.storage(), self.state))._known

    def test_invalid_records_are_skipped(self):
        """Records that don't parse (or that SQLite can't store) are skipped, as rnfs 0.1.4 skipped them; a
        signature that isn't a string is kept as its text, as it was."""
        write_legacy(self.state, self.FILE, {"entries": {"a": {"0": 1}, "b": 5, "c": ["sig", "not a cid"],
                                                         "d": [1], "e": ["\udc80", str(A)], "f": [7, str(B)]}})
        self.assertEqual(self.load(), {"f": ("7", B)})
        self.assertEqual(self.migrated(), [(self.FILE, 1)])


class JobsTest(MigrateCases, unittest.TestCase):
    """jobs.json: {"jobs": [record, ...]}, in creation order."""

    FILE = "jobs.json"
    DONE = {"id": "0000000a", "kind": "pin", "target": str(A), "output": None, "from": None, "sealed_name": False,
            "fallback_dht": False, "state": "done", "created": 1.0, "started": 2.0, "finished": 3.0, "error": None,
            "result": {"cid": str(A)}, "runs": 1, "total_bytes": 1, "done_bytes": 1, "blocks": 1,
            "blocks_total": 1, "downloaded_bytes": 1}
    # Interrupted, and with only the fields every version wrote
    RUNNING = {"id": "0000000b", "kind": "get", "target": str(B) + "/docs", "output": os.path.abspath("out"),
               "state": "running", "created": 4.0}
    FAILED = {**DONE, "id": "0000000c", "kind": "export", "target": str(C), "output": os.path.abspath("x.rnfs"),
              "state": "failed", "error": "no provider", "result": None}
    VALUE = {"jobs": [RUNNING, DONE, FAILED]}
    CHANGED = {"jobs": [{**DONE, "id": "0000000d"}]}
    COUNT = 3
    EXPECTED = [("0000000b", "queued", None, 4.0), ("0000000a", "done", {"cid": str(A)}, 1.0),
                ("0000000c", "failed", None, 1.0)]
    EMPTY: list = []
    DAMAGED = ("{not json", "", "[]", '{"jobs": {}}')

    def feature(self) -> JobsFeature:
        """A job manager over a fresh storage (closed before the directory is removed)."""
        return self.closing(JobsFeature(FeatureNode(self.config, self.storage())))

    def load(self) -> list[tuple]:
        """(id, state, result, created) of each job the manager loads, oldest first."""
        return [(job["id"], job["state"], job["result"], job["created"]) for job in self.feature().list()]

    def test_invalid_records_are_skipped(self):
        """Malformed records are skipped, as rnfs 0.1.4 skipped them, and a repeated id keeps the first."""
        write_legacy(self.state, self.FILE, {"jobs": [
            "junk", {**self.DONE, "id": "nothex!!"}, {**self.DONE, "kind": "copy"}, self.DONE,
            {**self.DONE, "state": "failed"}, {**self.RUNNING, "output": "relative"}, self.RUNNING]})
        self.assertEqual([job[:2] for job in self.load()], [("0000000a", "done"), ("0000000b", "queued")])
        self.assertEqual(self.migrated(), [(self.FILE, 2)])
        self.assertEqual(state_rows(self.state, "SELECT id, position FROM jobs ORDER BY position"),
                         [("0000000a", 0), ("0000000b", 1)])

    def test_failed_save_is_written_by_the_next(self):
        """An imported running job loads as queued; if saving that fails it is still saved by the next save."""
        write_legacy(self.state, self.FILE, self.VALUE)
        feature = self.feature()
        with failing_commits():
            feature._save_quietly()  # logged, not raised
        self.assertEqual(statedb.load(state_rows(self.state, "SELECT record FROM jobs WHERE id = ?",
                                                 ("0000000b",))[0][0])["state"], "running")
        feature.save()
        self.assertEqual(statedb.load(state_rows(self.state, "SELECT record FROM jobs WHERE id = ?",
                                                 ("0000000b",))[0][0])["state"], "queued")


class ContactsTest(MigrateCases, unittest.TestCase):
    """contacts.json: {"version": 1, "contacts": {nick: {"kind", "id"}}}."""

    FILE = "contacts.json"
    VALUE = {"version": 1, "contacts": {"alice": {"kind": "name", "id": NAME_ID},
                                        "relay": {"kind": "node", "id": NODE_ID}}}
    CHANGED = {"version": 1, "contacts": {"bob": {"kind": "lxmf", "id": NODE_ID}}}
    COUNT = 2
    EXPECTED = [{"nick": "alice", "kind": "name", "id": NAME_ID}, {"nick": "relay", "kind": "node", "id": NODE_ID}]
    EMPTY: list = []

    def load(self) -> list[dict]:
        """The contacts the address book loads."""
        return self.closing(ContactsFeature(FeatureNode(self.config, None))).list()

    def test_invalid_records_are_skipped(self):
        """Malformed entries are skipped, as rnfs 0.1.4 skipped them, and at most MAX_CONTACTS are imported."""
        write_legacy(self.state, self.FILE, {"contacts": {
            "BAD": {"kind": "node", "id": NODE_ID}, "worse": {"kind": "spaceship", "id": NODE_ID}, "worst": "nope",
            "short": {"kind": "node", "id": "ab"}, "ok": {"kind": "node", "id": "<" + NODE_ID.upper() + ">"},
            "late": {"kind": "node", "id": NODE_ID}}})
        with mock.patch("rnfs.features.contacts.MAX_CONTACTS", 1):
            self.assertEqual(self.load(), [{"nick": "ok", "kind": "node", "id": NODE_ID}])
        self.assertEqual(self.migrated(), [(self.FILE, 1)])

    def test_other_values_import_nothing(self):
        """A file holding anything but an object with a "contacts" object imports nothing, as rnfs 0.1.4 loaded
        nothing from it, and is kept as migrated."""
        for n, value in enumerate(([], {"version": 1}, {"contacts": []}, 5)):
            with self.subTest(value=value):
                self.config = Config(rnfs_dir=os.path.join(self.tmp.name, f"other{n}"))
                self.state = self.config.state_dir
                path = write_legacy(self.state, self.FILE, value)
                self.assertEqual(self.load(), [])
                self.assertEqual(self.migrated(), [(self.FILE, 0)])
                self.assertTrue(os.path.exists(path + statedb.MIGRATED_SUFFIX))


if __name__ == "__main__":
    unittest.main()
