"""Migration of the storage, node and fetcher state files of rnfs 0.1.3 and 0.1.4 into the state database:
pins.json, cache.json (storage), fetched.json, remote_pins.json (node) and invalid.json (fetcher).

For each: a valid file imports exactly, a damaged one is handled as those versions handled it, an import
interrupted after its commit isn't repeated, a second start changes nothing, and a .migrated file left behind
is ignored."""
import json
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs import statedb
from rnfs.config import Config
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.storage import Storage, remote_source
from rnfs.errors import RnfsError
from rnfs.fetcher import Fetcher
from rnfs.node import RnfsNode
from tests.support import PERMISSION_CHECKS, state_rows, write_legacy
from tests.test_names import FakeNetwork
from tests.test_node import NetClient

A, B, C = (Cid.for_block(RAW, bytes([i])) for i in range(3))
D = Cid.for_block(DAG, b"\x93\x00\x90\x90")
IDENTITY = "0123456789abcdef0123456789abcdef"
OTHER = "fedcba9876543210fedcba9876543210"

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class MigrateTest(unittest.TestCase):
    """An rnfs directory under a temporary directory, with the state files written into <rnfs dir>/state."""

    def setUp(self):
        """An empty rnfs directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = Config(rnfs_dir=os.path.join(self.tmp.name, "node"), remote_pin_identities=[IDENTITY])
        self.state = self.config.state_dir

    def storage(self) -> Storage:
        """Storage over the node's directories (closed before the directory is removed)."""
        storage = Storage(self.config.blocks_dir, self.state, 10**9)
        self.addCleanup(storage.close)
        return storage

    def node(self) -> RnfsNode:
        """An offline node over the directory (stopped before the directory is removed)."""
        node = RnfsNode.offline(self.config, b"\x07" * 16, NetClient(), FakeNetwork())
        self.addCleanup(node.stop)
        return node

    def fetcher(self) -> Fetcher:
        """A fetcher remembering invalid roots in the state directory."""
        fetcher = Fetcher(self.storage(), None, self.config, b"\x07" * 16, None, None, state_dir=self.state)
        self.addCleanup(fetcher.close)
        return fetcher

    def interrupted(self, name: str, value, changed, load):
        """Import name holding value, as if the process died between the commit and the rename; write changed
        into the file left behind and start again: load() must show value's state, and the file be renamed."""
        path = write_legacy(self.state, name, value)
        with mock.patch.object(statedb, "_keep_original", return_value=None):
            first = load()
        self.assertTrue(os.path.exists(path))
        write_legacy(self.state, name, changed)
        self.assertEqual(load(), first)
        self.assertFalse(os.path.exists(path))
        with open(path + ".migrated", encoding="utf-8") as f:
            self.assertEqual(json.load(f), changed)  # the file as it was, kept
        self.assertEqual(state_rows(self.state, "SELECT records FROM migrated WHERE file = ?", (name,)), [(len(
            value) if isinstance(value, list) else len(next(iter(value.values()))),)])
        return first

    def migrated_again(self, name: str, changed, load, expected):
        """A <name>.migrated left behind (and the database's state) survive another start unchanged."""
        write_legacy(self.state, name + ".migrated", changed)
        self.assertEqual(load(), expected)


class PinsTest(MigrateTest):
    """pins.json: {"pins": {cid: [source, ...]}}."""

    VALUE = {"pins": {str(A): ["user", "folder"], str(B): ["name:photos", "remote:" + IDENTITY],
                      str(D): ["follow:" + IDENTITY, "lxmf:" + OTHER]}}

    def load(self) -> dict:
        """The pins storage loads."""
        return self.storage().pins()

    def test_valid_file_imports_exactly(self):
        """Every pin and source is imported, an lxmf: source as rnfs 0.2.0's app:lxmf: (the schema step rewrote the
        pins table's before this import); the file is kept as pins.json.migrated."""
        write_legacy(self.state, "pins.json", self.VALUE)
        self.assertEqual(self.load(), {A: {"user", "folder"}, B: {"name:photos", "remote:" + IDENTITY},
                                       D: {"follow:" + IDENTITY, "app:lxmf:" + OTHER}})
        self.assertEqual(len(state_rows(self.state, "SELECT * FROM pins")), 6)
        self.assertTrue(os.path.exists(os.path.join(self.state, "pins.json.migrated")))
        self.assertEqual(self.load(), {A: {"user", "folder"}, B: {"name:photos", "remote:" + IDENTITY},
                                       D: {"follow:" + IDENTITY, "app:lxmf:" + OTHER}})  # a second start: no change

    def test_invalid_records_are_skipped(self):
        """Records that don't parse, with sources that aren't a list or aren't valid (or that SQLite can't
        store), are skipped one by one, as rnfs 0.1.4 skipped them; an empty source list pins nothing."""
        write_legacy(self.state, "pins.json", {"pins": {
            "not a cid": ["user"], str(A): "user", str(B): ["users"], str(C): ["name:\udc80"], str(D): [],
            Cid.for_block(RAW, b"ok").__str__(): ["user"]}})
        self.assertEqual(self.load(), {Cid.for_block(RAW, b"ok"): {"user"}})

    def test_invalid_lxmf_sources_stay_invalid(self):
        """Only a valid lxmf: source is rewritten as app:lxmf:: one rnfs 0.1.4 skipped is skipped still, never
        turned into a valid app: source."""
        write_legacy(self.state, "pins.json", {"pins": {str(A): ["lxmf:xyz"], str(B): ["lxmf:" + OTHER.upper()],
                                                        str(C): ["lxmf:" + OTHER]}})
        self.assertEqual(self.load(), {C: {"app:lxmf:" + OTHER}})

    def test_damaged_file_is_moved_aside(self):
        """A file that isn't JSON or has the wrong shape is moved to .damaged and storage starts without it."""
        for text in ("{not json", "", "[]", '{"pins": []}'):
            with self.subTest(text=text):
                path = write_legacy(self.state, "pins.json", text=text)
                self.assertEqual(self.load(), {})
                self.assertFalse(os.path.exists(path))
        self.assertTrue(os.path.exists(os.path.join(self.state, "pins.json.damaged.3")))

    @unittest.skipUnless(PERMISSION_CHECKS, "needs permission checks")
    def test_unreadable_file_stops_startup(self):
        """A pins.json that can't be read stops startup: starting without it would let gc delete the pins."""
        path = write_legacy(self.state, "pins.json", self.VALUE)
        os.chmod(path, 0)
        try:
            with self.assertRaises(PermissionError):
                self.storage()
        finally:
            os.chmod(path, 0o600)
        self.assertEqual(len(self.load()), 3)

    def test_interrupted_migration_isnt_repeated(self):
        """A crash between the commit and the rename: the next start only renames the file."""
        pins = self.interrupted("pins.json", self.VALUE, {"pins": {str(C): ["user"]}}, self.load)
        self.assertEqual(set(pins), {A, B, D})
        self.migrated_again("pins.json", {"pins": {str(C): ["user"]}}, self.load, pins)


class CacheTest(MigrateTest):
    """cache.json: {"fetches": [[root, [name, ...], target], ...]}, least recently used first."""

    VALUE = {"fetches": [[str(D), ["docs", "é"], str(A)], [str(B), [], str(B)], [str(D), ["x"], str(C)]]}

    def load(self) -> list:
        """The cached fetches storage loads, least recently used first."""
        return self.storage().cached_fetches()

    def test_valid_file_imports_exactly_in_order(self):
        """Every record is imported in least recently used order, which later saves keep."""
        write_legacy(self.state, "cache.json", self.VALUE)
        storage = self.storage()
        self.assertEqual(storage.cached_fetches(), [(D, ("docs", "é"), A), (B, (), B), (D, ("x",), C)])
        storage.touch(D, ["docs", "é"])
        storage.save()
        self.assertEqual(self.load(), [(B, (), B), (D, ("x",), C), (D, ("docs", "é"), A)])

    def test_invalid_records_are_skipped_and_the_cap_kept(self):
        """Records that don't parse or name invalid DAG names are skipped; a repeated record keeps its first
        place with its last target; past MAX_CACHE_RECORDS the oldest are dropped."""
        write_legacy(self.state, "cache.json", {"fetches": [
            [str(A), ["a/b"], str(A)], [str(A), "ab", str(A)], ["bad", [], str(A)], [str(A), [], str(A), 1],
            [str(B), [], str(A)], [str(C), [], str(C)], [str(B), [], str(B)]]})
        with mock.patch("rnfs.content.storage.MAX_CACHE_RECORDS", 1):
            self.assertEqual(self.load(), [(C, (), C)])
        self.assertEqual(state_rows(self.state, "SELECT target FROM cache"), [(C.binary,)])

    def test_damaged_file_is_moved_aside(self):
        """A file that isn't JSON or has the wrong shape is moved to .damaged and storage starts without it."""
        for text in ("{not json", '{"fetches": 5}', '"text"'):
            with self.subTest(text=text):
                write_legacy(self.state, "cache.json", text=text)
                self.assertEqual(self.load(), [])
        self.assertTrue(os.path.exists(os.path.join(self.state, "cache.json.damaged.2")))

    def test_interrupted_migration_isnt_repeated(self):
        """A crash between the commit and the rename: the next start only renames the file."""
        changed = {"fetches": [[str(C), [], str(C)]]}
        records = self.interrupted("cache.json", self.VALUE, changed, self.load)
        self.assertEqual(len(records), 3)
        self.migrated_again("cache.json", changed, self.load, records)


class FetchedTest(MigrateTest):
    """fetched.json: [cid, ...], the targets fetched for the user."""

    def load(self) -> set:
        """The fetched targets a node loads."""
        node = self.node()
        targets = node._eligible_targets()
        node.stop()
        return targets

    def test_valid_file_imports_exactly(self):
        """Every target is imported; one that doesn't parse is skipped, as before."""
        write_legacy(self.state, "fetched.json", [str(A), str(D), "bad", 5])
        self.assertEqual(self.load(), {A, D})

    def test_damaged_file_is_moved_aside(self):
        """A file that isn't JSON or isn't a list is moved to .damaged and the node starts without it."""
        for text in ("{not json", '{"a": 1}'):
            with self.subTest(text=text):
                write_legacy(self.state, "fetched.json", text=text)
                self.assertEqual(self.load(), set())
        self.assertTrue(os.path.exists(os.path.join(self.state, "fetched.json.damaged.1")))

    def test_interrupted_migration_isnt_repeated(self):
        """A crash between the commit and the rename: the next start only renames the file."""
        targets = self.interrupted("fetched.json", [str(A), str(B)], [str(C)], self.load)
        self.assertEqual(targets, {A, B})
        self.migrated_again("fetched.json", [str(C)], self.load, targets)


class RemotePinsTest(MigrateTest):
    """remote_pins.json (0.1.4 only): [[source, cid], ...], in the order accepted."""

    VALUE = [[remote_source(IDENTITY), str(B)], [remote_source(OTHER), str(C)], [remote_source(IDENTITY), str(A)]]

    def load(self) -> list:
        """The remote pins a node would resume, in order."""
        node = self.node()
        pins = node._load_remote_pins()
        node.stop()
        return pins

    def test_valid_file_imports_exactly_in_order(self):
        """Every pair is imported in order; at load, only sources still allowed are resumed, as before."""
        write_legacy(self.state, "remote_pins.json", self.VALUE)
        self.assertEqual(self.load(), [(remote_source(IDENTITY), B), (remote_source(IDENTITY), A)])
        self.assertEqual(len(state_rows(self.state, "SELECT * FROM remote_pins")), 3)
        self.config.remote_pin_identities = [IDENTITY, OTHER]
        self.assertEqual(self.load(), [(remote_source(IDENTITY), B), (remote_source(OTHER), C),
                                       (remote_source(IDENTITY), A)])

    def test_malformed_entries_are_skipped(self):
        """Entries that aren't a pair of strings, or whose CID doesn't parse, are skipped and counted in a
        warning."""
        write_legacy(self.state, "remote_pins.json", [[[remote_source(IDENTITY)], str(A)], [remote_source(IDENTITY)],
                                                      [remote_source(IDENTITY), "bad"], self.VALUE[0]])
        with mock.patch("rnfs.node.log") as log:
            self.assertEqual(self.load(), [(remote_source(IDENTITY), B)])
        self.assertTrue(any("3 malformed entries" in call.args[0] for call in log.call_args_list))

    def test_damaged_file_is_moved_aside(self):
        """A file that isn't JSON or isn't a list is moved to .damaged and the node starts without it."""
        write_legacy(self.state, "remote_pins.json", text="{not json")
        self.assertEqual(self.load(), [])
        self.assertTrue(os.path.exists(os.path.join(self.state, "remote_pins.json.damaged")))

    @unittest.skipUnless(PERMISSION_CHECKS, "needs permission checks")
    def test_unreadable_file_is_left_for_the_next_start(self):
        """A file that can't be read is logged and skipped (the pins are best effort), and imported once it can
        be read."""
        path = write_legacy(self.state, "remote_pins.json", self.VALUE)
        os.chmod(path, 0)
        try:
            self.assertEqual(self.load(), [])
        finally:
            os.chmod(path, 0o600)
        self.assertEqual(len(self.load()), 2)

    def test_interrupted_migration_isnt_repeated(self):
        """A crash between the commit and the rename: the next start only renames the file."""
        changed = [[remote_source(IDENTITY), str(C)]]
        pins = self.interrupted("remote_pins.json", self.VALUE, changed, self.load)
        self.assertEqual(len(pins), 2)
        self.migrated_again("remote_pins.json", changed, self.load, pins)


class InvalidTest(MigrateTest):
    """invalid.json: [cid, ...], oldest first."""

    def load(self) -> list:
        """Which of A, B, C, D a fetcher remembers as invalid."""
        fetcher = self.fetcher()
        return [fetcher.is_invalid(cid) for cid in (A, B, C, D)]

    def test_valid_file_imports_exactly(self):
        """Every root is imported, oldest first; past MAX_INVALID only the newest are kept."""
        write_legacy(self.state, "invalid.json", [str(A), "bad", str(D), str(C)])
        with mock.patch("rnfs.fetcher.MAX_INVALID", 2):
            self.assertEqual(self.load(), [False, False, True, True])
        self.assertEqual(state_rows(self.state, "SELECT cid FROM invalid ORDER BY position"),
                         [(D.binary,), (C.binary,)])

    def test_damaged_file_is_moved_aside(self):
        """A file that isn't JSON (or, now, isn't a list) is moved to .damaged and the fetcher starts without
        it."""
        for text in ("{not json", "5"):
            write_legacy(self.state, "invalid.json", text=text)
            self.assertEqual(self.load(), [False] * 4)
        self.assertTrue(os.path.exists(os.path.join(self.state, "invalid.json.damaged.1")))

    def test_interrupted_migration_isnt_repeated(self):
        """A crash between the commit and the rename: the next start only renames the file."""
        roots = self.interrupted("invalid.json", [str(A), str(B)], [str(C)], self.load)
        self.assertEqual(roots, [True, True, False, False])
        self.migrated_again("invalid.json", [str(C)], self.load, roots)



class FailedStartTest(MigrateTest):
    """A node that can't start releases the state database."""

    def test_failed_setup_closes_the_database(self):
        """A store refusing to start (a malformed names.json stops startup) leaves no state database open, and
        the file in place for the next start."""
        path = write_legacy(self.state, "names.json", text="null")
        with self.assertRaises(RnfsError):
            RnfsNode.offline(self.config, b"\x07" * 16, NetClient(), FakeNetwork())
        self.assertNotIn(os.path.join(self.state, statedb.DB_FILE), statedb.open_databases())
        self.assertTrue(os.path.exists(path))
        os.remove(path)
        self.assertEqual(self.load_pins(), {})

    def load_pins(self) -> dict:
        """The pins a node starting now holds."""
        return self.node().storage.pins()


if __name__ == "__main__":
    unittest.main()
