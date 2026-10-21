"""Migration of names.json and name-floor.json, the names state of rnfs 0.1.4 and earlier, into the state database
(names_published, names_held and name_floor).

For each: a valid file imports exactly, a damaged one is handled as those versions handled it (a damaged names.json
stops startup and is left in place; a damaged name-floor.json is moved aside), an import interrupted after its
commit isn't repeated, a second start changes nothing, a .migrated file left behind is ignored, and the formats
older versions wrote load as they did. A failed commit changes nothing."""
import json
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs import names, statedb
from rnfs.config import Config
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.errors import RnfsError
from rnfs.names import NameManager, Record
from tests.support import PERMISSION_CHECKS, FakeClock, failing_commits, state_rows, write_legacy
from tests.test_names import FakeNetwork, FakePins

A, B, C = Cid.for_block(RAW, b"a"), Cid.for_block(RAW, b"b"), Cid.for_block(DAG, b"c")
MAX = names.MAX_SEQUENCE
N = [bytes([i]) * 16 for i in range(16)]  # names that aren't local
NOW = 1_000_000.0

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class MigrateNamesTest(unittest.TestCase):
    """An rnfs directory under a temporary directory, with the state files written into <rnfs dir>/state."""

    def setUp(self):
        """An empty rnfs directory, and fakes for the managers."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = Config(rnfs_dir=os.path.join(self.tmp.name, "node"), path_timeout=0.05, name_table_size=3)
        self.state = self.config.state_dir
        self.clock = FakeClock(NOW)
        self.pins = FakePins()

    def manager(self) -> NameManager:
        """A manager over the directory (closed before the directory is removed)."""
        manager = NameManager(self.config, self.pins, network=FakeNetwork(), clock=self.clock, monotonic=self.clock)
        self.addCleanup(manager.close)
        return manager

    def key(self, alias: str) -> bytes:
        """Write a new name key file for alias, as `name create` did; its name."""
        identity = RNS.Identity()
        os.makedirs(self.config.names_dir, exist_ok=True)
        with open(os.path.join(self.config.names_dir, alias), "wb") as f:
            f.write(identity.get_private_key())
        return names.name_for_identity(identity)

    def rows(self, sql: str, params=()) -> list[tuple]:
        """The rows sql reads from the state database."""
        return state_rows(self.state, sql, params)


class FileTests:
    """The tests every legacy file shares; a subclass gives FILE, COUNT (its records), value(), changed() (value
    with a record changed) and snapshot() (a new manager's state and the rows)."""

    def test_interrupted_import_is_only_finished(self):
        """An import whose rename never happened (the process died after the commit) isn't done again at the next
        start, even if the file changed meanwhile: the file is only renamed."""
        path = write_legacy(self.state, self.FILE, self.value())
        with mock.patch.object(statedb, "_keep_original", return_value=None):
            first = self.snapshot()
        self.assertTrue(os.path.exists(path))
        write_legacy(self.state, self.FILE, self.changed())
        self.assertEqual(self.snapshot(), first)
        self.assertFalse(os.path.exists(path))
        with open(path + statedb.MIGRATED_SUFFIX, encoding="utf-8") as f:
            self.assertEqual(json.load(f), self.changed())  # the file as it was, kept
        self.assertEqual(self.rows("SELECT records FROM migrated WHERE file = ?", (self.FILE,)), [(self.COUNT,)])

    def test_later_starts_change_nothing(self):
        """A second start imports nothing, nor does a .migrated copy left behind, whatever it holds."""
        write_legacy(self.state, self.FILE, self.value())
        first = self.snapshot()
        self.assertEqual(self.snapshot(), first)
        write_legacy(self.state, self.FILE + statedb.MIGRATED_SUFFIX, self.changed())
        self.assertEqual(self.snapshot(), first)
        self.assertEqual(len(self.rows("SELECT * FROM migrated WHERE file = ?", (self.FILE,))), 1)

    def test_failed_commit_leaves_the_file(self):
        """An import that can't be committed (a failing disk) stops startup and imports nothing; the next start
        imports the file."""
        statedb.connect(self.state).close()  # the database of an earlier start
        path = write_legacy(self.state, self.FILE, self.value())
        with failing_commits(), self.assertRaises(OSError):
            self.manager()
        self.assertTrue(os.path.exists(path))
        self.assertEqual(self.rows("SELECT * FROM migrated"), [])
        self.assertTrue(self.snapshot()[-1])  # rows imported
        self.assertFalse(os.path.exists(path))
        self.assertEqual(self.rows("SELECT records FROM migrated WHERE file = ?", (self.FILE,)), [(self.COUNT,)])


class NamesFileTest(FileTests, MigrateNamesTest):
    """names.json: {"version": 1, "published": {alias: {sequence, cid, imported}}, "held": {name: {sequence, cid,
    heard}}}, written with indent=1 by rnfs 0.1.0 to 0.1.4."""

    FILE = names.STATE_FILE
    COUNT = 5

    def setUp(self):
        """Key files for "site" and "moved"; "gone" has a record but no key file (deleted by hand)."""
        super().setUp()
        self.site = self.key("site")
        self.moved = self.key("moved")

    def value(self) -> dict:
        """names.json as rnfs 0.1.4 wrote it, with the highest sequence and a name not yet published."""
        return {"version": 1,
                "published": {"gone": {"sequence": MAX, "cid": str(B), "imported": False},
                              "moved": {"sequence": 0, "cid": None, "imported": True},
                              "site": {"sequence": 5, "cid": str(A), "imported": False}},
                "held": {N[1].hex(): {"sequence": MAX, "cid": str(C), "heard": 99.5},
                         self.site.hex(): {"sequence": 5, "cid": str(A), "heard": 1234.0}}}

    def changed(self) -> dict:
        """value() with site published again."""
        value = self.value()
        value["published"]["site"] = {"sequence": 6, "cid": str(B), "imported": False}
        return value

    def snapshot(self) -> tuple:
        """What a new manager loads (published, held, gone) and the rows."""
        manager = self.manager()
        return ({alias: (p.sequence, p.cid, p.imported) for alias, p in manager._published.items()},
                dict(manager._held), list(manager._gone),
                self.rows("SELECT * FROM names_published ORDER BY alias"),
                self.rows("SELECT * FROM names_held ORDER BY name"))

    def test_valid_file_imports_exactly(self):
        """Every published and held record is imported, field for field, whether or not its key file is there; the
        file is kept as names.json.migrated."""
        path = write_legacy(self.state, self.FILE, text=json.dumps(self.value(), indent=1))
        manager = self.manager()
        self.assertEqual(self.rows("SELECT alias, sequence, cid, imported FROM names_published ORDER BY alias"),
                         [("gone", str(MAX), B.binary, 0), ("moved", "0", None, 1), ("site", "5", A.binary, 0)])
        self.assertEqual(self.rows("SELECT name, sequence, cid, heard FROM names_held ORDER BY name"),
                         sorted([(N[1], str(MAX), C.binary, 99.5), (self.site, "5", A.binary, 1234.0)]))
        self.assertEqual(manager.list(), [
            # Keys from before 0.2.0 count as never exported (name_exports has no row for them)
            {"alias": "moved", "name": self.moved.hex(), "cid": None, "sequence": 0, "exported": False},
            {"alias": "site", "name": self.site.hex(), "cid": str(A), "sequence": 5, "exported": False}])
        self.assertEqual((manager._published["moved"].imported, manager._published["site"].imported), (True, False))
        self.assertEqual(manager.record(N[1]), Record(MAX, C, 99.5))
        self.assertEqual(manager._gone, [("gone", B)])  # its pin is released once the node is set up
        self.assertEqual(self.rows("SELECT file, records FROM migrated"), [(self.FILE, self.COUNT)])
        self.assertFalse(os.path.exists(path))
        self.assertTrue(os.path.exists(path + statedb.MIGRATED_SUFFIX))
        manager.release_gone()
        self.assertEqual([alias for (alias,) in self.rows("SELECT alias FROM names_published ORDER BY alias")],
                         ["moved", "site"])

    def test_entries_without_imported_flag(self):
        """An entry without "imported" (every version wrote it; one edited by hand) loads as not imported, and a key
        without an entry as imported, as rnfs 0.1.4 loaded them."""
        write_legacy(self.state, self.FILE, {"published": {"site": {"sequence": 2, "cid": str(A)}}})
        manager = self.manager()
        self.assertEqual(self.rows("SELECT alias, sequence, cid, imported FROM names_published"),
                         [("site", "2", A.binary, 0)])
        self.assertEqual((manager._published["site"], manager._published["moved"]),
                         (names._Published(2, A, False), names._Published(0, None, True)))

    def test_lenient_fields_load_as_before(self):
        """Fields rnfs 0.1.4 converted (int() of the sequence, float() of the heard time, any truth value as
        the flag) are converted the same way; an alias no key file can have is skipped."""
        write_legacy(self.state, self.FILE, {
            "published": {"site": {"sequence": "7", "cid": None, "imported": 1}, "../x": {"sequence": 1, "cid": None}},
            "held": {f"<{N[2].hex().upper()}>": {"sequence": 3.0, "cid": str(A), "heard": "12.5"},
                     N[3].hex(): {"sequence": 1, "cid": str(A), "heard": float("nan")}}})
        manager = self.manager()
        self.assertEqual(manager._published["site"], names._Published(7, None, True))
        self.assertEqual(manager.record(N[2]), Record(3, A, 12.5))
        self.assertEqual(manager.record(N[3]), Record(1, A, 0.0))  # SQLite has no NaN: heard long ago
        self.assertEqual(self.rows("SELECT alias FROM names_published"), [("site",)])

    def test_damaged_file_stops_startup(self):
        """A names.json that isn't JSON, or has a malformed record, stops startup and is left as it is (starting
        without it would allow rollback); nothing is imported."""
        damaged = ["{not json", "", "null", "[1, 2]", '{"published": []}', '{"published": {"site": {"sequence": 1}}}',
                   '{"published": {"site": {"sequence": "one", "cid": null}}}',
                   '{"published": {"site": {"sequence": 1, "cid": "not a cid"}}}',
                   '{"held": {"zz": {"sequence": 1, "cid": "%s", "heard": 1}}}' % A,
                   '{"held": {"%s": {"sequence": 1, "cid": "%s"}}}' % (N[1].hex(), A),
                   '{"held": {"%s": {"sequence": 1e999, "cid": "%s", "heard": 1}}}' % (N[1].hex(), A)]
        path = os.path.join(self.state, self.FILE)
        for text in damaged:
            with self.subTest(text=text):
                write_legacy(self.state, self.FILE, text=text)
                with self.assertRaises(RnfsError):
                    self.manager()
                with open(path, encoding="utf-8") as f:
                    self.assertEqual(f.read(), text)
        self.assertEqual(sorted(os.listdir(self.state)), sorted([self.FILE, *self.database_files()]))
        os.remove(path)
        os.mkdir(path)  # can't be read at all
        with self.assertRaisesRegex(RnfsError, "could not read"):
            self.manager()
        os.rmdir(path)
        for table in ("names_published", "names_held", "migrated"):
            self.assertEqual(self.rows(f"SELECT * FROM {table}"), [])

    def test_file_nested_too_deeply_stops_startup(self):
        """A names.json nested deeper than json can parse is refused as one that isn't JSON is (RnfsError, the file
        left as it is), not with a RecursionError."""
        text = "[" * 100000
        path = write_legacy(self.state, self.FILE, text=text)
        with self.assertRaisesRegex(RnfsError, "nested too deeply"):
            self.manager()
        with open(path, encoding="utf-8") as f:
            self.assertTrue(f.read() == text)  # assertEqual would print all of it on a failure

    def database_files(self) -> list[str]:
        """The state database's files in the state directory."""
        return [entry for entry in os.listdir(self.state) if entry.startswith(statedb.DB_FILE)]

    def test_failed_publish_commit_rolls_back(self):
        """After the import, a publish whose save can't be committed leaves memory, pins and rows as they were."""
        write_legacy(self.state, self.FILE, self.value())
        manager = self.manager()
        rows = self.rows("SELECT * FROM names_published ORDER BY alias"), self.rows("SELECT * FROM names_held")
        with failing_commits(), self.assertRaises(OSError):
            manager.publish("site", B)
        self.assertEqual(manager.list()[1]["sequence"], 5)
        self.assertEqual(manager.record(self.site), Record(5, A, 1234.0))
        self.assertEqual(self.pins.sources, {})
        self.assertEqual((self.rows("SELECT * FROM names_published ORDER BY alias"),
                          self.rows("SELECT * FROM names_held")), rows)
        self.assertEqual(manager.publish("site", B)["sequence"], 6)


class FloorFileTest(FileTests, MigrateNamesTest):
    """name-floor.json: {"version": 1, "floor": [[name, sequence, resolved], ...]}, least recently resolved
    first, written compactly by rnfs 0.1.4."""

    FILE = names.FLOOR_FILE
    COUNT = 3

    def value(self) -> dict:
        """name-floor.json as rnfs 0.1.4 wrote it, with the highest sequence."""
        return {"version": 1, "floor": [[N[1].hex(), 5, 100], [N[2].hex(), MAX, 200], [N[3].hex(), 0, 300]]}

    def changed(self) -> dict:
        """value() with the first name's floor raised."""
        value = self.value()
        value["floor"][0][1] = 6
        return value

    def snapshot(self) -> tuple:
        """The floor a new manager loads, and the rows."""
        return list(self.manager()._floor.items()), self.rows("SELECT * FROM name_floor ORDER BY used")

    def floor(self) -> list[tuple]:
        """The name_floor rows, least recently resolved first."""
        return self.rows("SELECT name, sequence, resolved, used FROM name_floor ORDER BY used")

    def test_valid_file_imports_exactly(self):
        """Every entry is imported, in order, with its sequence and resolve time; the file is kept as
        name-floor.json.migrated."""
        path = write_legacy(self.state, self.FILE, text=json.dumps(self.value(), separators=(",", ":")))
        manager = self.manager()
        self.assertEqual(self.floor(), [(N[1], "5", 100.0, 0), (N[2], str(MAX), 200.0, 1), (N[3], "0", 300.0, 2)])
        self.assertEqual(list(manager._floor.items()), [(N[1], (5, 100.0)), (N[2], (MAX, 200.0)), (N[3], (0, 300.0))])
        self.assertEqual(self.rows("SELECT file, records FROM migrated"), [(self.FILE, self.COUNT)])
        self.assertTrue(os.path.exists(path + statedb.MIGRATED_SUFFIX))
        # Saved later as any other floor: the new entry after the imported ones
        manager.offer(N[4], 1, A, NOW)
        self.assertEqual(manager.resolve(N[4], persist=False)["sequence"], 1)
        self.assertEqual([name for name, *_ in self.floor()], [N[1], N[2], N[3], N[4]])

    def test_entries_checked_as_before(self):
        """Entries that don't parse are skipped, as rnfs 0.1.4 skipped them; a name listed twice keeps the higher
        sequence at its later place; an entry without one valid resolve time counts as resolved long ago."""
        write_legacy(self.state, self.FILE, text=json.dumps({"version": 1, "floor": [
            [N[1].hex(), 5, 100], [N[2].hex(), 1, 150], [N[1].hex(), 3, 400],
            ["zz", 1, 1], [None, 1, 1], "a string entry", [N[4].hex()], 7,
            [N[5].hex(), -1, 1], [N[5].hex(), MAX + 1, 1], [N[5].hex(), True, 1], [N[5].hex(), 1.5, 1],
            [N[6].hex(), 2, 1, 2], [N[7].hex(), 3, "noon"], [N[8].hex(), 4, 10**400], [N[9].hex(), 5, float("nan")],
            [f"<{N[10].hex().upper()}>", 6, 7]]}))
        with mock.patch.object(names, "log") as log:
            manager = self.manager()
        expected = [(N[2], (1, 150.0)), (N[1], (5, 400.0)), (N[6], (2, 0.0)), (N[7], (3, 0.0)), (N[8], (4, 0.0)),
                    (N[9], (5, 0.0)), (N[10], (6, 7.0))]
        self.assertEqual(list(manager._floor.items()), expected)
        self.assertEqual(self.floor(), [(name, str(sequence), at, used)
                                        for used, (name, (sequence, at)) in enumerate(expected)])
        self.assertTrue(any("skipped 9 malformed entries" in call.args[0] for call in log.call_args_list))

    def test_entries_without_resolve_time(self):
        """[name, sequence] entries, as rnfs wrote before 0.1.4 kept resolve times, count as resolved long ago."""
        write_legacy(self.state, self.FILE, {"version": 1, "floor": [[N[1].hex(), 5], [N[2].hex(), 6]]})
        manager = self.manager()
        self.assertEqual(list(manager._floor.items()), [(N[1], (5, 0.0)), (N[2], (6, 0.0))])
        self.assertEqual(self.floor(), [(N[1], "5", 0.0, 0), (N[2], "6", 0.0, 1)])

    def test_future_times_count_as_now_at_load(self):
        """A resolve time ahead of the clock loads as now, as rnfs 0.1.4 loaded it."""
        write_legacy(self.state, self.FILE, {"version": 1, "floor": [[N[1].hex(), 5, NOW + 1000]]})
        self.assertEqual(self.manager()._floor[N[1]], (5, NOW))

    def test_held_names_and_the_bound_apply_at_load(self):
        """A name names.json holds has no floor entry, and the floor is cut to its bound, least recently resolved
        first; the rows dropped are deleted."""
        write_legacy(self.state, names.STATE_FILE, {"held": {N[1].hex(): {"sequence": 9, "cid": str(A),
                                                                          "heard": 1.0}}})
        size = names.FLOOR_FACTOR * self.config.name_table_size
        others = [bytes([0x80, i]) * 8 for i in range(size + 2)]
        write_legacy(self.state, self.FILE, {"version": 1, "floor": [[N[1].hex(), 5]]
                                             + [[name.hex(), 1] for name in others]})
        manager = self.manager()
        self.assertEqual(list(manager._floor), others[2:])
        self.assertEqual([name for name, *_ in self.floor()], others[2:])
        self.assertEqual(manager.record(N[1]).sequence, 9)

    def test_damaged_file_is_moved_aside(self):
        """A file that isn't JSON, or isn't a floor of this version, is moved to .damaged and the node starts
        without it."""
        damaged = ["{not json", "", "[]", '{"floor": []}', '{"version": 2, "floor": []}', '{"version": 1, "floor": {}}']
        for text in damaged:
            with self.subTest(text=text):
                path = write_legacy(self.state, self.FILE, text=text)
                self.assertEqual(len(self.manager()._floor), 0)
                self.assertFalse(os.path.exists(path))
        self.assertTrue(os.path.exists(os.path.join(self.state, f"{self.FILE}.damaged.{len(damaged) - 1}")))
        self.assertEqual(self.rows("SELECT * FROM name_floor"), [])
        self.assertEqual(self.rows("SELECT * FROM migrated"), [])

    @unittest.skipUnless(PERMISSION_CHECKS, "needs permission checks")
    def test_unreadable_file_stops_startup(self):
        """A name-floor.json that can't be read stops startup, as it did: a transient error isn't damage."""
        path = write_legacy(self.state, self.FILE, self.value())
        os.chmod(path, 0)
        try:
            with self.assertRaises(PermissionError):
                self.manager()
        finally:
            os.chmod(path, 0o600)
        self.assertEqual(len(self.manager()._floor), 3)


if __name__ == "__main__":
    unittest.main()
