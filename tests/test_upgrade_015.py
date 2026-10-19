"""Upgrading from rnfs 0.1.5: the current code started on an rnfs directory exactly as 0.1.5 left it
(tests/fixtures/rnfs-0.1.5, its state database holding rows in every table; see its README) keeps every row.

The expectation is the fixture's expected.json, every table's rows as 0.1.5 left them. The current code may add
rows and tables (its own schema steps), but none of 0.1.5's rows may change or go, except those a step rewrites,
whose new form is stated here (upgraded): the two `lxmf:<address>` pins become `app:lxmf:<address>` (0.2.0's
"lxmf_app_pins", the LXMF wrapper app's source). The database upgrades, passes quick_check, sets nothing aside,
and a second start changes nothing. The tables 0.1.5 created must also be
exactly those a fresh database gets, so a released schema step edited in place (which a database 0.1.5 wrote
would never see) fails here as well as in test_statedb's digest.

The clock is the fixture's (expected.json's "now"), so no record has expired."""
import functools
import importlib.util
import io
import json
import os
import shutil
import sqlite3
import tempfile
import time
import unittest
from unittest import mock

import RNS

from rnfs import node as node_module
from rnfs import statedb
from rnfs.config import Config
from rnfs.content import builder
from rnfs.content.blockstore import BlockStore
from rnfs.node import RnfsNode
from tests.support import FakeClock, state_rows
from tests.test_names import FakeNetwork
from tests.test_node import NetClient
from tests.test_statedb import RELEASED_0_1_5_STEPS
from tests.test_upgrade_014 import NODE_ID, make_blocks

FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "rnfs-0.1.5")

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


def _expected() -> dict:
    """expected.json."""
    with open(os.path.join(FIXTURE, "expected.json"), encoding="utf-8") as f:
        return json.load(f)


EXPECTED = _expected()


def _content():
    """The fixture's fixture_content module: the files 0.1.5 added on top of the 0.1.4 fixture's."""
    spec = importlib.util.spec_from_file_location("fixture_content_015", os.path.join(FIXTURE, "fixture_content.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def row(values: list) -> tuple:
    """A row of expected.json as sqlite3 gives it."""
    return tuple(bytes.fromhex(v["blob"]) if isinstance(v, dict) else v for v in values)


def upgraded(table: str, values: tuple) -> tuple:
    """A row of expected.json as the current schema leaves it: 0.2.0's step "lxmf_app_pins" rewrites a pin source
    lxmf:<address> as app:lxmf:<address> (storage.LXMF_PREFIX); every other row is kept as it was."""
    if table == "pins" and values[1].startswith("lxmf:"):
        return (values[0], "app:" + values[1])
    return values


def loadable(table: str, values: tuple) -> bool:
    """Whether this platform keeps the row: a get job's output must be absolute, and /home/... isn't on Windows
    (Python 3.13 and later), so the jobs store drops that row there (as test_upgrade_014's loadable_jobs)."""
    if table != "jobs":
        return True
    output = statedb.load(values[2]).get("output")
    return output is None or os.path.isabs(output)


def tables(state_dir: str) -> dict[str, set[tuple]]:
    """Every row of every table of the state database."""
    names = [r[0] for r in state_rows(state_dir, "SELECT name FROM sqlite_master WHERE type = 'table'")]
    return {name: set(state_rows(state_dir, f"SELECT * FROM {name}")) for name in names}


def schema_sql(path: str) -> dict[str, str]:
    """{name: sql} of every table and index in the database at path."""
    conn = sqlite3.connect(path)
    try:
        return dict(conn.execute("SELECT name, sql FROM sqlite_master WHERE sql IS NOT NULL"))
    finally:
        conn.close()


class UpgradeTest(unittest.TestCase):
    """A copy of the fixture under a temporary directory, with its blocks, started on with the clock at "now"."""

    def setUp(self):
        """Copy the fixture and make its blocks; fake the wall clock, also for the stores that take it as an
        argument."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = os.path.join(self.tmp.name, "rnfs")
        for part in ("state", "names", "files"):
            shutil.copytree(os.path.join(FIXTURE, part), os.path.join(self.root, part))
        scratch = os.path.join(self.tmp.name, "scratch")
        os.makedirs(scratch)
        blocks = os.path.join(self.root, "blocks")
        self.built = make_blocks(blocks, scratch)
        store = BlockStore(blocks)
        for data, name in _content().STREAMS:
            self.built.add(builder.add_stream(io.BytesIO(data), store.put, len(data), name).cid)
        self.state = os.path.join(self.root, "state")
        self.clock = FakeClock(EXPECTED["now"])
        for patch in (mock.patch.object(time, "time", self.clock),
                      *(mock.patch.object(node_module, cls, functools.partial(getattr(node_module, cls),
                                                                              clock=self.clock))
                        for cls in ("ProviderStore", "Publisher", "NameManager"))):
            patch.start()
            self.addCleanup(patch.stop)

    def start(self) -> RnfsNode:
        """An offline node on the copy (stopped before the directory is removed)."""
        config = Config(rnfs_dir=self.root, **EXPECTED["settings"])
        config.validate()
        node = RnfsNode.offline(config, NODE_ID, NetClient(), FakeNetwork())
        self.addCleanup(node.stop)
        return node

    def test_fixture_is_complete(self):
        """The fixture is what its README says: 0.1.5's schema, rows in every table, the 0.1.5-only ones included,
        and the deny list's blocks made again by the current code (no CID changed) and pinned "deny"."""
        self.assertEqual(EXPECTED["user_version"], RELEASED_0_1_5_STEPS)
        self.assertEqual(state_rows(self.state, "PRAGMA user_version"), [(RELEASED_0_1_5_STEPS,)])
        self.assertEqual([name for name, rows in EXPECTED["tables"].items() if not rows], [])
        for table in ("deny_entries", "deny_lists", "deny_hashes", "meta"):
            self.assertIn(table, EXPECTED["tables"])
        deny_list = row(EXPECTED["tables"]["deny_lists"][0])[1]
        self.assertIn(deny_list, {cid.binary for cid in self.built})
        self.assertIn((deny_list, "deny"), {row(values) for values in EXPECTED["tables"]["pins"]})

    def test_schema_is_upgraded(self):
        """user_version is the current schema's, and the upgraded database passes quick_check."""
        self.start().stop()
        self.assertEqual(state_rows(self.state, "PRAGMA user_version"), [(len(statedb.SCHEMA),)])
        self.assertEqual(state_rows(self.state, "PRAGMA quick_check"), [("ok",)])
        # 0.1.6's steps were applied: the shared folder's per-file cache (D1) has its table
        self.assertEqual(state_rows(self.state, "SELECT name FROM sqlite_master WHERE name = 'shared_folder_files'"),
                         [("shared_folder_files",)])

    def test_every_row_is_kept(self):
        """Every row 0.1.5 left is still there, unchanged but for those a step rewrites (upgraded), in every
        table."""
        node = self.start()
        node.stop()
        found = tables(self.state)
        for table, rows in EXPECTED["tables"].items():
            with self.subTest(table=table):
                self.assertIn(table, found)
                expected = {upgraded(table, row(values)) for values in rows if loadable(table, row(values))}
                self.assertEqual(expected - found[table], set())

    def test_lxmf_pins_become_app_pins(self):
        """The fixture's two lxmf:<address> pins (an auto-fetch sender's) are app:lxmf:<address> pins after the
        upgrade, of the same CIDs, and no lxmf: source is left; the LXMF feature still counts them as its own (its
        inbox records them), so they are kept."""
        legacy = {row(values) for values in EXPECTED["tables"]["pins"] if values[1].startswith("lxmf:")}
        self.assertEqual(len(legacy), 2)
        node = self.start()
        node.stop()
        pins = tables(self.state)["pins"]
        self.assertEqual({(cid, "app:lxmf:" + source[len("lxmf:"):]) for cid, source in legacy} - pins, set())
        self.assertEqual({source for _, source in pins if source.startswith("lxmf:")}, set())

    def test_every_pin_is_complete(self):
        """Every pin, the deny list's own included, is complete: the stores agree with the blocks."""
        node = self.start()
        pins = node.storage.pins()
        self.assertEqual({cid.binary for cid in pins}, {row(values)[0] for values in EXPECTED["tables"]["pins"]})
        for cid in pins:
            self.assertTrue(node.storage.is_complete(cid), cid)

    def test_nothing_is_set_aside(self):
        """No file in the state directory is set aside as damaged, and the .migrated files are left alone."""
        listing = sorted(os.listdir(self.state))
        self.start().stop()
        self.assertEqual([name for name in os.listdir(self.state) if ".damaged" in name], [])
        migrated = [name for name in listing if name.endswith(statedb.MIGRATED_SUFFIX)]
        self.assertEqual(len(migrated), len(statedb.LEGACY))
        for name in migrated:
            with open(os.path.join(self.state, name), "rb") as kept, \
                    open(os.path.join(FIXTURE, "state", name), "rb") as original:
                self.assertEqual(kept.read(), original.read(), name)

    def test_second_start_changes_nothing(self):
        """A second start changes no row, nor anything in the state directory."""
        self.start().stop()
        before, listing = tables(self.state), sorted(os.listdir(self.state))
        self.start().stop()
        self.assertEqual(tables(self.state), before)
        self.assertEqual(sorted(os.listdir(self.state)), listing)

    def test_released_tables_match_a_fresh_database(self):
        """The tables and indexes 0.1.5 created are exactly those the current schema creates in a fresh database:
        a released step edited in place would make them differ."""
        released = schema_sql(os.path.join(FIXTURE, "state", statedb.DB_FILE))
        db = statedb.connect(os.path.join(self.tmp.name, "fresh"))
        db.close()
        fresh = schema_sql(os.path.join(self.tmp.name, "fresh", statedb.DB_FILE))
        self.assertEqual({name: fresh.get(name) for name in released}, released)


if __name__ == "__main__":
    unittest.main()
