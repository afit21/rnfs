"""Upgrading from rnfs 0.1.5: the current code started on an rnfs directory exactly as 0.1.5 left it
(tests/fixtures/rnfs-0.1.5, its state database holding rows in every table; see its README) keeps every row.

The expectation is the fixture's expected.json, every table's rows as 0.1.5 left them. The current code may add
rows and tables (its own schema steps), but none of 0.1.5's rows may change or go: the database upgrades, passes
quick_check, sets nothing aside, and a second start changes nothing. The tables 0.1.5 created must also be
exactly those a fresh database gets, so a released schema step edited in place (which a database 0.1.5 wrote
would never see) fails here as well as in test_statedb's digest.

The blocks are written in the flat layout 0.1.6 left (blocks/<cid>); BlockLayoutTest checks they are moved into
their subdirectories from that, from a move an earlier start left half done, with partial files left over, and
when a move fails.

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

from rnfs import fsutil, statedb
from rnfs import node as node_module
from rnfs.config import Config
from rnfs.content import builder
from rnfs.content.blockstore import PARTIAL_PREFIX, BlockStore, shard_of
from rnfs.content.cid import Cid
from rnfs.node import RnfsNode
from tests.support import FakeClock, state_rows
from tests.test_names import FakeNetwork
from tests.test_node import NetClient
from tests.test_statedb import RELEASED_0_1_5_STEPS
from tests.test_upgrade_014 import NODE_ID, FlatBlockStore, make_blocks

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


class FixtureCase(unittest.TestCase):
    """A copy of the fixture under a temporary directory, with its blocks, started on with the clock at "now"."""

    def setUp(self):
        """Copy the fixture and make its blocks, in 0.1.6's flat layout; fake the wall clock, also for the stores
        that take it as an argument."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = os.path.join(self.tmp.name, "rnfs")
        for part in ("state", "names", "files"):
            shutil.copytree(os.path.join(FIXTURE, part), os.path.join(self.root, part))
        scratch = os.path.join(self.tmp.name, "scratch")
        os.makedirs(scratch)
        blocks = os.path.join(self.root, "blocks")
        self.built = make_blocks(blocks, scratch)
        store = FlatBlockStore(blocks)
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


class UpgradeTest(FixtureCase):
    """Every row 0.1.5 left is kept."""

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
        """Every row 0.1.5 left is still there, unchanged, in every table."""
        node = self.start()
        node.stop()
        found = tables(self.state)
        for table, rows in EXPECTED["tables"].items():
            with self.subTest(table=table):
                self.assertIn(table, found)
                expected = {row(values) for values in rows if loadable(table, row(values))}
                self.assertEqual(expected - found[table], set())

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


class BlockLayoutTest(FixtureCase):
    """The blocks, written in 0.1.6's flat layout, are moved into their subdirectories by the start, whatever state an
    earlier start left the move in, and every pin stays complete."""

    def flat_blocks(self) -> list[str]:
        """The block files directly in blocks/."""
        return sorted(name for name in os.listdir(self.blocks) if os.path.isfile(os.path.join(self.blocks, name))
                      and not name.startswith(PARTIAL_PREFIX))

    def check_complete(self, node: RnfsNode):
        """Every pin is complete, and every block made is stored, in its subdirectory unless still flat."""
        pins = node.storage.pins()
        self.assertTrue(pins)
        for cid in pins:
            self.assertTrue(node.storage.is_complete(cid), cid)
        for cid in self.built:
            self.assertTrue(node.storage.has(cid), cid)

    def in_shard(self, name: str) -> str:
        """Where the block named name goes."""
        return os.path.join(self.blocks, shard_of(name), name)

    def setUp(self):
        """As FixtureCase, noting the blocks directory and its flat blocks."""
        super().setUp()
        self.blocks = os.path.join(self.root, "blocks")
        self.names = self.flat_blocks()
        self.assertGreater(len(self.names), 10)

    def test_flat_store(self):
        node = self.start()
        self.check_complete(node)
        self.assertEqual(self.flat_blocks(), [])
        for name in self.names:
            self.assertTrue(os.path.isfile(self.in_shard(name)), name)

    def test_sizes_are_saved_and_read_by_the_next_start(self):
        # The first start lists every block (0.1.5 left no sizes); after its clean stop, the next one reads them
        self.start().stop()
        self.assertEqual(state_rows(self.state, "SELECT value FROM meta WHERE store = 'block_sizes'"), [(1,)])
        self.assertEqual(len(state_rows(self.state, "SELECT cid FROM block_sizes")), len(self.names))
        with mock.patch.object(BlockStore, "scan", side_effect=AssertionError("listed every block")):
            node = self.start()
        self.check_complete(node)

    def test_half_moved_store(self):
        # An earlier start stopped part way through the move; one block is in both places
        for name in self.names[::2]:
            os.makedirs(os.path.dirname(self.in_shard(name)), mode=0o700, exist_ok=True)
            os.replace(os.path.join(self.blocks, name), self.in_shard(name))
        shutil.copy(self.in_shard(self.names[0]), os.path.join(self.blocks, self.names[0]))
        node = self.start()
        self.check_complete(node)
        self.assertEqual(self.flat_blocks(), [])

    def test_leftover_partial_files(self):
        # 0.1.6 left one directly in blocks/; a 0.2.0 start that crashed, one in a subdirectory
        shard = os.path.dirname(self.in_shard(self.names[0]))
        os.makedirs(shard, mode=0o700)
        for partial in (os.path.join(self.blocks, PARTIAL_PREFIX + "old"), os.path.join(shard, PARTIAL_PREFIX + "new")):
            with open(partial, "wb") as f:
                f.write(b"half a block")
        node = self.start()
        self.check_complete(node)
        self.assertEqual([name for name in os.listdir(self.blocks) if name.startswith(PARTIAL_PREFIX)], [])
        self.assertEqual([name for name in os.listdir(shard) if name.startswith(PARTIAL_PREFIX)], [])

    def test_failed_move(self):
        # On Windows a scanner may hold a block open: it stays where it is, readable, until a later start moves it
        stuck = self.names[0]
        real_replace = fsutil.replace

        def replace(src, dst):
            """Refuse to move stuck."""
            if os.path.basename(src) == stuck:
                raise PermissionError(13, "in use", src)
            real_replace(src, dst)

        with mock.patch.object(fsutil, "replace", replace):
            node = self.start()
        self.check_complete(node)
        self.assertEqual(self.flat_blocks(), [stuck])
        self.assertIsNotNone(node.storage.get(Cid.parse(stuck)))
        node.stop()
        node = self.start()
        self.check_complete(node)
        self.assertEqual(self.flat_blocks(), [])
        self.assertTrue(os.path.isfile(self.in_shard(stuck)))


if __name__ == "__main__":
    unittest.main()
