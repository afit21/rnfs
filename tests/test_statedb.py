"""Tests for the state database (rnfs.statedb): opening and checking it, transactions, references, and the
migration of the JSON state files earlier versions wrote."""
import builtins
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

import RNS

from rnfs import statedb
from rnfs.errors import RnfsError
from tests.support import POSIX_MODES, failing_commits, state_execute, state_rows, write_legacy

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class StateDbTest(unittest.TestCase):
    """A state directory under a temporary directory."""

    def setUp(self):
        """An empty temporary directory; state goes in <tmp>/state."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = os.path.join(self.tmp.name, "state")
        self.path = os.path.join(self.state, statedb.DB_FILE)

    def connect(self, legacy=None) -> statedb.StateDb:
        """A reference to the state database (closed before the directory is removed); with legacy, only those
        files are migrated."""
        if legacy is None:
            db = statedb.connect(self.state)
        else:
            with mock.patch.object(statedb, "LEGACY", legacy):
                db = statedb.connect(self.state)
        self.addCleanup(db.close)
        return db


class OpenTest(StateDbTest):
    """Opening, checking and upgrading the database."""

    def test_new_database_has_the_whole_schema_in_wal_mode(self):
        """A new database is created with every schema step applied and journal_mode WAL."""
        db = self.connect()
        self.assertEqual(db.query("PRAGMA user_version"), [(len(statedb.SCHEMA),)])
        self.assertEqual(db.query("PRAGMA journal_mode"), [("wal",)])
        tables = {name for (name,) in db.query("SELECT name FROM sqlite_master WHERE type = 'table'")}
        self.assertLessEqual({"migrated", "meta", "pins", "cache", "lxmf_messages", "name_floor"}, tables)

    @unittest.skipUnless(POSIX_MODES, "Windows has no POSIX permission bits")
    def test_files_are_private(self):
        """rnfs.db and its -wal and -shm files are readable by their owner only, even under a lax umask; an
        existing database others could read is tightened."""
        old = os.umask(0o022)
        try:
            db = self.connect()
            with db.transaction() as tx:
                tx.execute("INSERT INTO pins (cid, source) VALUES (?, ?)", (b"x", "user"))
        finally:
            os.umask(old)
        for name in ("", "-wal", "-shm"):
            self.assertEqual(os.stat(self.path + name).st_mode & 0o777, 0o600, name)
        db.close()
        os.chmod(self.path, 0o644)
        self.connect()
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)

    def test_newer_schema_stops_startup(self):
        """A database written by a newer rnfs (more schema steps than this code knows) is refused and kept."""
        self.connect().close()
        conn = sqlite3.connect(self.path)
        conn.execute(f"PRAGMA user_version = {len(statedb.SCHEMA) + 1}")
        conn.close()
        with self.assertRaises(RnfsError) as caught:
            statedb.connect(self.state)
        self.assertIn("newer version of rnfs", str(caught.exception))
        self.assertEqual(state_rows(self.state, "PRAGMA user_version"), [(len(statedb.SCHEMA) + 1,)])
        self.assertNotIn(self.path, statedb.open_databases())

    def test_older_schema_is_upgraded_step_by_step(self):
        """A database at an earlier schema step gets the steps it lacks, and keeps its rows."""
        with mock.patch.object(statedb, "SCHEMA", statedb.SCHEMA[:2]):
            db = statedb.connect(self.state)
            with db.transaction() as tx:
                tx.execute("INSERT INTO pins (cid, source) VALUES (?, ?)", (b"x", "user"))
            db.close()
        db = self.connect()
        self.assertEqual(db.query("PRAGMA user_version"), [(len(statedb.SCHEMA),)])
        self.assertEqual(db.query("SELECT cid, source FROM pins"), [(b"x", "user")])
        self.assertEqual(db.query("SELECT count(*) FROM lxmf_messages"), [(0,)])

    def test_file_that_isnt_a_database_stops_startup(self):
        """A file that isn't an SQLite database is refused with what to do, and left as it was."""
        os.makedirs(self.state)
        garbage = b"not a database " * 1000
        with open(self.path, "wb") as f:
            f.write(garbage)
        with self.assertRaises(RnfsError) as caught:
            statedb.connect(self.state)
        self.assertIn("is damaged", str(caught.exception))
        self.assertIn(".migrated", str(caught.exception))
        with open(self.path, "rb") as f:
            self.assertEqual(f.read(), garbage)
        self.assertNotIn(self.path, statedb.open_databases())

    def test_damaged_pages_stop_startup(self):
        """A database whose pages were damaged (quick_check fails, or SQLite finds it malformed) is refused."""
        db = self.connect()
        with db.transaction() as tx:
            tx.executemany("INSERT INTO pins (cid, source) VALUES (?, ?)",
                           [(os.urandom(34), "user") for _ in range(2000)])
        db.close()
        size = os.path.getsize(self.path)
        with open(self.path, "r+b") as f:
            for offset in range(4096 * 3, size - 4096, 4096 * 2):
                f.seek(offset)
                f.write(b"\xff" * 64)  # b-tree page headers of the pins table
        with self.assertRaises(RnfsError) as caught:
            statedb.connect(self.state)
        self.assertIn("is damaged", str(caught.exception))
        self.assertNotIn(self.path, statedb.open_databases())

    @unittest.skipUnless(POSIX_MODES and hasattr(os, "getuid") and os.getuid() != 0, "needs permission checks")
    def test_unwritable_directory_isnt_called_damage(self):
        """A database that can't be opened for writing (a read-only state directory) stops startup with what
        failed, not as damaged with advice to start over, and is left as it was."""
        db = self.connect()
        with db.transaction() as tx:
            tx.execute("INSERT INTO pins (cid, source) VALUES (?, 'user')", (b"kept",))
        db.close()
        os.chmod(self.state, 0o500)
        try:
            with self.assertRaises(RnfsError) as caught:
                statedb.connect(self.state)
        finally:
            os.chmod(self.state, 0o700)
        self.assertNotIn("damaged", str(caught.exception))
        self.assertIn("could not open the state database", str(caught.exception))
        self.assertEqual(state_rows(self.state, "SELECT cid FROM pins"), [(b"kept",)])

    def test_python_without_sqlite_gets_a_clear_error(self):
        """A Python built without _sqlite3 stops startup with a message saying so."""
        real_import = builtins.__import__

        def no_sqlite(name, *args, **kwargs):
            """Fail to import sqlite3, as such a Python does."""
            if name == "sqlite3":
                raise ModuleNotFoundError("No module named '_sqlite3'")
            return real_import(name, *args, **kwargs)

        with mock.patch.object(builtins, "__import__", no_sqlite):
            with self.assertRaises(RnfsError) as caught:
                statedb.connect(self.state)
        self.assertIn("sqlite3", str(caught.exception))


class TransactionTest(StateDbTest):
    """Transactions and references."""

    def test_durable_transactions_commit_with_synchronous_full(self):
        """durable=True commits with synchronous=FULL, else NORMAL."""
        db = self.connect()
        for durable, level in ((True, 2), (False, 1), (True, 2)):
            with db.transaction(durable=durable) as tx:
                self.assertEqual(tx.execute("PRAGMA synchronous").fetchone(), (level,))

    def test_failed_commit_changes_nothing(self):
        """A commit that fails (a full disk) raises StateDbError, an OSError, and rolls everything back."""
        db = self.connect()
        with failing_commits("database or disk is full"):
            with self.assertRaises(OSError) as caught:
                with db.transaction(durable=True) as tx:
                    tx.execute("INSERT INTO pins (cid, source) VALUES (?, ?)", (b"x", "user"))
        self.assertIsInstance(caught.exception, statedb.StateDbError)
        self.assertEqual(db.query("SELECT * FROM pins"), [])
        with db.transaction() as tx:  # usable again afterwards
            tx.execute("INSERT INTO pins (cid, source) VALUES (?, ?)", (b"y", "user"))
        self.assertEqual(state_rows(self.state, "SELECT cid FROM pins"), [(b"y",)])

    def test_error_in_the_body_rolls_back_and_is_raised_as_is(self):
        """An exception inside the with block rolls the transaction back and propagates unchanged."""
        db = self.connect()
        with self.assertRaises(KeyError):
            with db.transaction() as tx:
                tx.execute("INSERT INTO pins (cid, source) VALUES (?, ?)", (b"x", "user"))
                raise KeyError("stop")
        with self.assertRaises(statedb.StateDbError):
            with db.transaction() as tx:
                tx.execute("INSERT INTO no_such_table VALUES (1)")
        self.assertEqual(db.query("SELECT * FROM pins"), [])

    def test_values_sqlite_cant_store_are_refused_without_a_change(self):
        """A lone surrogate in plain text, or an integer past 64 bits, is refused as a failed save is, with
        StateDbError (stores dump() such text); inside the transaction it is the binding error."""
        db = self.connect()
        for row, error in (((b"x", "name:\udc80"), UnicodeEncodeError), ((b"x", 2 ** 64), OverflowError)):
            with self.subTest(error=error.__name__), self.assertRaises(statedb.StateDbError) as caught:
                with db.transaction() as tx:
                    tx.execute("INSERT INTO pins (cid, source) VALUES (?, ?)", row)
            self.assertIsInstance(caught.exception.__cause__, error)
        self.assertEqual(db.query("SELECT * FROM pins"), [])

    def test_transaction_inside_a_transaction_is_an_error(self):
        """The lock isn't reentrant: a transaction or read inside one raises rather than deadlocking."""
        db = self.connect()
        with self.assertRaises(RuntimeError):
            with db.transaction():
                db.query("SELECT 1")
        with self.assertRaises(RuntimeError):
            with db.transaction():
                with db.transaction():
                    pass
        self.assertEqual(db.query("SELECT 1"), [(1,)])

    def test_threads_write_one_at_a_time(self):
        """Transactions from many threads all commit, one at a time."""
        db = self.connect()

        def write(n):
            """Insert 50 rows, one transaction each."""
            for i in range(50):
                with db.transaction() as tx:
                    tx.execute("INSERT INTO pins (cid, source) VALUES (?, 'user')", (bytes([n, i]),))

        threads = [threading.Thread(target=write, args=(n,)) for n in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(db.query("SELECT count(*) FROM pins"), [(400,)])

    def test_references_share_one_connection_until_the_last_closes(self):
        """connect() hands out references to one database per directory; the last close() closes it, and a
        closed reference raises StateDbError."""
        first, second = statedb.connect(self.state), statedb.connect(self.state)
        self.assertIn(self.path, statedb.open_databases())
        first.close()
        first.close()  # idempotent: it doesn't release the other's reference
        self.assertEqual(second.query("SELECT 1"), [(1,)])
        with self.assertRaises(statedb.StateDbError):
            first.query("SELECT 1")
        second.close()
        self.assertNotIn(self.path, statedb.open_databases())

    def test_meta(self):
        """Meta values are per store and key, with a default when unset."""
        db = self.connect()
        self.assertEqual(db.meta("follow", "clean", "unset"), "unset")
        with db.transaction() as tx:
            statedb.set_meta(tx, "follow", "clean", 1)
            statedb.set_meta(tx, "inbox", "clean", 0)
        self.assertEqual((db.meta("follow", "clean"), db.meta("inbox", "clean")), (1, 0))

    def test_dump_round_trips_any_text(self):
        """dump() gives text SQLite stores, non-ASCII as is, and load() returns exactly what was dumped."""
        db = self.connect()
        value = {"title": "é \U0001f600 \udc80 \ud800x", "n": [1, None]}
        with db.transaction() as tx:
            tx.execute("INSERT INTO pin_names (cid, name) VALUES (?, ?)", (b"x", statedb.dump(value)))
        (text,), = db.query("SELECT name FROM pin_names")
        self.assertEqual(statedb.load(text), value)
        self.assertIn("é \U0001f600", text)


def _import_rows(value, tx) -> int:
    """A test importer: value's strings become pins of source "user"; returns how many."""
    tx.execute("DELETE FROM pins")
    tx.executemany("INSERT INTO pins (cid, source) VALUES (?, 'user')", [(text.encode(),) for text in value])
    return len(value)


TEST_LEGACY = (statedb.Legacy("test.json", f"{__name__}:_import_rows", lambda value: isinstance(value, list)),)


class MigrateTest(StateDbTest):
    """migrate(), with a test importer."""

    def rows(self) -> list[bytes]:
        """The test importer's rows."""
        return sorted(cid for (cid,) in state_rows(self.state, "SELECT cid FROM pins"))

    def test_file_is_imported_once_and_kept_as_migrated(self):
        """A legacy file is imported in one transaction, recorded, renamed to .migrated, and logged."""
        write_legacy(self.state, "test.json", ["a", "b"])
        with mock.patch.object(statedb, "log") as log:
            self.connect(TEST_LEGACY).close()
        self.assertEqual(self.rows(), [b"a", b"b"])
        self.assertFalse(os.path.exists(os.path.join(self.state, "test.json")))
        with open(os.path.join(self.state, "test.json.migrated"), encoding="utf-8") as f:
            self.assertEqual(json.load(f), ["a", "b"])
        self.assertEqual(state_rows(self.state, "SELECT file, records FROM migrated"), [("test.json", 2)])
        self.assertTrue(any("imported test.json" in call.args[0] and "2 records" in call.args[0]
                            and call.args[1] == statedb.LOG_NOTICE for call in log.call_args_list))
        # A second start is a no-op; the .migrated file left behind is ignored
        with mock.patch.object(statedb, "_resolve", side_effect=AssertionError("imported again")):
            self.connect(TEST_LEGACY)
        self.assertEqual(self.rows(), [b"a", b"b"])

    def test_crash_between_the_commit_and_the_rename(self):
        """If the rename never happened, the next start only renames the file: nothing is imported twice."""
        path = write_legacy(self.state, "test.json", ["a"])
        with mock.patch.object(statedb, "_keep_original", return_value=None):
            self.connect(TEST_LEGACY).close()
        self.assertTrue(os.path.exists(path))
        write_legacy(self.state, "test.json", ["a", "changed"])  # whatever it holds now isn't imported
        self.connect(TEST_LEGACY).close()
        self.assertEqual(self.rows(), [b"a"])
        self.assertFalse(os.path.exists(path))
        self.assertTrue(os.path.exists(path + ".migrated"))

    def test_crash_inside_the_import_imports_nothing(self):
        """An importer that fails leaves no rows, no record and the file in place, so the next start imports it
        whole."""
        path = write_legacy(self.state, "test.json", ["a", "b"])
        with mock.patch(f"{__name__}._import_rows", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.connect(TEST_LEGACY)
        self.assertEqual((self.rows(), state_rows(self.state, "SELECT * FROM migrated")), ([], []))
        self.assertTrue(os.path.exists(path))
        self.connect(TEST_LEGACY)
        self.assertEqual(self.rows(), [b"a", b"b"])

    def test_failed_commit_imports_nothing(self):
        """A commit that fails stops startup with the file in place and nothing recorded."""
        self.connect().close()
        path = write_legacy(self.state, "test.json", ["a"])
        with failing_commits():
            with self.assertRaises(OSError):
                self.connect(TEST_LEGACY)
        self.assertTrue(os.path.exists(path))
        self.assertEqual(state_rows(self.state, "SELECT * FROM migrated"), [])

    def test_damaged_file_is_moved_aside_and_not_recorded(self):
        """A damaged file (not JSON, or the wrong shape) is moved to .damaged, as read_json always did."""
        for text in ("{not json", '{"a": 1}', ""):
            with self.subTest(text=text):
                path = write_legacy(self.state, "test.json", text=text)
                self.connect(TEST_LEGACY).close()
                self.assertFalse(os.path.exists(path))
                self.assertEqual(state_rows(self.state, "SELECT * FROM migrated"), [])
        self.assertTrue(os.path.exists(os.path.join(self.state, "test.json.damaged.2")))

    @unittest.skipUnless(POSIX_MODES and hasattr(os, "getuid") and os.getuid() != 0, "needs permission checks")
    def test_unreadable_file_stops_startup_unless_allowed(self):
        """A file that can't be read (not damage) stops startup and is left for the next start; one marked
        unreadable_ok is logged and skipped instead."""
        path = write_legacy(self.state, "test.json", ["a"])
        os.chmod(path, 0)
        try:
            with self.assertRaises(PermissionError):
                self.connect(TEST_LEGACY)
            allowed = (statedb.Legacy("test.json", TEST_LEGACY[0].importer, TEST_LEGACY[0].shape,
                                      unreadable_ok=True),)
            self.connect(allowed).close()
            self.assertTrue(os.path.exists(path))
        finally:
            os.chmod(path, 0o600)
        self.connect(TEST_LEGACY)
        self.assertEqual(self.rows(), [b"a"])

    def test_earlier_migrated_copy_is_kept(self):
        """If <name>.migrated is taken, the file is kept as <name>.migrated.1."""
        write_legacy(self.state, "test.json.migrated", ["earlier"])
        write_legacy(self.state, "test.json", ["a"])
        self.connect(TEST_LEGACY)
        with open(os.path.join(self.state, "test.json.migrated"), encoding="utf-8") as f:
            self.assertEqual(json.load(f), ["earlier"])
        with open(os.path.join(self.state, "test.json.migrated.1"), encoding="utf-8") as f:
            self.assertEqual(json.load(f), ["a"])

    def test_concurrent_connects_import_once(self):
        """Stores connecting from several threads at once import each file once."""
        write_legacy(self.state, "test.json", ["a", "b"])
        calls, dbs, errors = [], [], []
        real = _import_rows

        def importer(value, tx):
            """Count the import, then import."""
            calls.append(1)
            return real(value, tx)

        def connect():
            """Connect, keeping the reference or the error."""
            try:
                dbs.append(statedb.connect(self.state))
            except BaseException as e:  # noqa: BLE001 - reported below
                errors.append(e)

        with mock.patch.object(statedb, "LEGACY", TEST_LEGACY), \
                mock.patch(f"{__name__}._import_rows", side_effect=importer):
            threads = [threading.Thread(target=connect) for _ in range(6)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        for db in dbs:
            self.addCleanup(db.close)
        self.assertEqual((len(calls), errors), (1, []))
        self.assertEqual(self.rows(), [b"a", b"b"])

    def test_every_legacy_file_names_an_importer(self):
        """Every LEGACY entry's importer (and reader) exists."""
        for item in statedb.LEGACY:
            with self.subTest(file=item.file):
                self.assertTrue(callable(statedb._resolve(item.importer)))
                if item.read:
                    self.assertTrue(callable(statedb._resolve(item.read)))
        self.assertEqual(len({item.file for item in statedb.LEGACY}), 17)

    def test_state_planted_in_the_database_is_kept(self):
        """A database already holding rows isn't touched by a start without legacy files."""
        self.connect().close()
        state_execute(self.state, "INSERT INTO pins (cid, source) VALUES (?, 'user')", (b"kept",))
        self.connect()
        self.assertEqual(self.rows(), [b"kept"])


if __name__ == "__main__":
    unittest.main()
