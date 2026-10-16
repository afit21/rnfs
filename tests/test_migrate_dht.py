"""Migration of the DHT state files of rnfs 0.1.3 and 0.1.4 into the state database: providers.json (provider
records) and published.json (the publisher's last rounds, earlier rounds' nodes and pending withdrawals).

For each: a valid file imports exactly (0.1.3's providers.json too, written before the newcomer pool), a
damaged one is moved aside, an import interrupted after its commit isn't repeated, a second start changes
nothing, a .migrated file left behind is ignored, and a failed commit imports nothing."""
import json
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs import statedb
from rnfs.config import Config
from rnfs.constants import DAY, ID_LENGTH
from rnfs.content.cid import RAW, Cid
from rnfs.dht.providers import ProviderStore
from rnfs.publisher import DhtKey, Publisher
from tests.support import POSIX_MODES, FakeClock, failing_commits, state_rows, write_legacy
from tests.test_providers import held, saved

T = 1_700_000_000.0
P1, P2, P3, P4, P5 = (bytes([n]) * ID_LENGTH for n in range(1, 6))
K1, K2, K3 = (bytes([0xA0 + n]) * ID_LENGTH for n in range(1, 4))
N1, N2, N3 = (bytes([0xB0 + n]) * ID_LENGTH for n in range(1, 4))
A, B = (Cid.for_block(RAW, bytes([n])) for n in range(2))
NO_ROWS = {"publishers": {}, "records": {}, "departed": {}, "evicted": {}}

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class MigrateTest(unittest.TestCase):
    """A state directory under a temporary directory, with the state files written into it."""

    NAME = ""  # the state file

    def setUp(self):
        """An empty state directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = os.path.join(self.tmp.name, "state")
        self.path = os.path.join(self.state, self.NAME)

    def migrated(self) -> list[tuple]:
        """The migrated table's record count for the state file, if it lists it."""
        return state_rows(self.state, "SELECT records FROM migrated WHERE file = ?", (self.NAME,))

    def interrupted(self, value, changed, load):
        """Import the file holding value, as if the process died between the commit and the rename; write changed
        into the file left behind and start again: load() must show value's state, and the file be renamed and
        kept as it was left."""
        write_legacy(self.state, self.NAME, value)
        with mock.patch.object(statedb, "_keep_original", return_value=None):
            first = load()
        self.assertTrue(os.path.exists(self.path))
        write_legacy(self.state, self.NAME, changed)
        self.assertEqual(load(), first)
        self.assertFalse(os.path.exists(self.path))
        with open(self.path + ".migrated", encoding="utf-8") as f:
            self.assertEqual(json.load(f), changed)
        return first

    def migrated_again(self, changed, load, expected):
        """A <name>.migrated left behind (and the database's state) survive another start unchanged."""
        write_legacy(self.state, self.NAME + ".migrated", changed)
        self.assertEqual(load(), expected)


class ProvidersTest(MigrateTest):
    """providers.json: {"version": 1, "publishers": {id: first seen}, "records": [[key, publisher, first stored,
    last stored, pooled], ...], "departed": [[publisher, first seen, when], ...], "evicted": [...]}."""

    NAME = "providers.json"
    VALUE = {"version": 1,
             "publishers": {P1.hex(): T - 3 * DAY, P2.hex(): T - 2 * DAY, P3.hex(): T},
             "records": [[K1.hex(), P1.hex(), T - 3 * DAY, T - 1, 0], [K2.hex(), P2.hex(), T - 2 * DAY, T - 2, 1],
                         [K1.hex(), P3.hex(), T, T, 0]],
             "departed": [[P4.hex(), T - 5 * DAY, T - DAY]],
             "evicted": [[P5.hex(), T - 60, T - 30]]}
    ROWS = {"publishers": {P1: T - 3 * DAY, P2: T - 2 * DAY, P3: T},
            "records": {(K1, P1): (T - 3 * DAY, T - 1, 0), (K2, P2): (T - 2 * DAY, T - 2, 1), (K1, P3): (T, T, 0)},
            "departed": {P4: (T - 5 * DAY, T - DAY)},
            "evicted": {P5: (T - 60, T - 30)}}

    def store(self, **settings) -> ProviderStore:
        """A store on the state directory at T + 10 (closed before the directory is removed)."""
        store = ProviderStore(100, self.state, clock=FakeClock(T + 10), seniority=DAY, **settings)
        self.addCleanup(store.close)
        return store

    def load(self) -> dict:
        """What a store on the state directory loads."""
        return held(self.store())

    def test_valid_file_imports_exactly(self):
        """Every publisher, record (pooled or not), departure and eviction is imported as written and loaded;
        the file is kept as providers.json.migrated, and a second start changes nothing."""
        write_legacy(self.state, self.NAME, self.VALUE)
        self.assertEqual(self.load(), self.ROWS)
        self.assertEqual(saved(self.state), self.ROWS)
        self.assertEqual(self.migrated(), [(3,)])
        self.assertFalse(os.path.exists(self.path))
        self.assertTrue(os.path.exists(self.path + ".migrated"))
        self.assertEqual(self.load(), self.ROWS)
        self.assertEqual(saved(self.state), self.ROWS)
        self.assertFalse(os.path.exists(self.path + ".migrated.1"))

    def test_file_from_before_the_newcomer_pool_imports(self):
        """rnfs 0.1.3's file, four fields per record and no departures or evictions, imports every record outside
        the pool."""
        value = {"version": 1, "publishers": self.VALUE["publishers"],
                 "records": [record[:4] for record in self.VALUE["records"]]}
        write_legacy(self.state, self.NAME, value)
        expected = {"publishers": self.ROWS["publishers"],
                    "records": {pair: (first, last, 0) for pair, (first, last, _) in self.ROWS["records"].items()},
                    "departed": {}, "evicted": {}}
        self.assertEqual(self.load(), expected)
        self.assertEqual(saved(self.state), expected)

    def test_what_any_load_drops_is_left_out(self):
        """NaN times, IDs of the wrong length and a repeated record (the first stored is kept) aren't imported;
        expired records and those of a publisher without a first-seen time are, and the load drops them (the
        next save deletes them). Numbers written as text convert as rnfs 0.1.4 converted them."""
        nan = float("nan")
        value = {"version": 1,
                 "publishers": {P1.hex(): "1699990000", P2.hex(): nan, "abcd": T},
                 "records": [[K1.hex(), P1.hex(), T, T, 0], [K1.hex(), P1.hex(), T - 1, T - 1, 1],
                             [K2.hex(), P1.hex(), nan, T, 0], ["abcd", P1.hex(), T, T, 0],
                             [K3.hex(), P1.hex(), T - 30 * DAY, T - 30 * DAY, 0],  # expired
                             [K3.hex(), P3.hex(), T, T, 0]],  # P3 has no first-seen time
                 "departed": [[P4.hex(), nan, T], [P5.hex(), T, T], [P5.hex(), T - 9 * DAY, T - 1]]}
        write_legacy(self.state, self.NAME, value)
        rows = {"publishers": {P1: 1699990000.0},
                "records": {(K1, P1): (T - 1, T - 1, 1), (K3, P1): (T - 30 * DAY, T - 30 * DAY, 0),
                            (K3, P3): (T, T, 0)},
                "departed": {P5: (T, T)}, "evicted": {}}  # the one that went last
        store = self.store()
        self.assertEqual(saved(self.state), rows)
        self.assertEqual(held(store), {"publishers": {P1: 1699990000.0}, "records": {(K1, P1): (T - 1, T - 1, 1)},
                                       "departed": {}, "evicted": {}})  # P5 departed younger than seniority
        store.save()
        self.assertEqual(saved(self.state), held(store))

    def test_damaged_file_is_moved_aside(self):
        """A file that isn't JSON, is of another version or holds a malformed record (rnfs 0.1.4 started empty
        on each) is moved to .damaged, and the store starts empty."""
        for text in ("{not json", "", "[]", json.dumps(dict(self.VALUE, version=2)),
                     json.dumps(dict(self.VALUE, records=[["zz", P1.hex(), T, T]])),
                     json.dumps(dict(self.VALUE, records=[[K1.hex(), P1.hex(), T]])),
                     json.dumps(dict(self.VALUE, departed=[[P4.hex(), "soon", T]])),
                     json.dumps({"version": 1, "records": []})):
            with self.subTest(text=text):
                write_legacy(self.state, self.NAME, text=text)
                self.assertEqual(self.load(), NO_ROWS)
                self.assertFalse(os.path.exists(self.path))
        self.assertTrue(os.path.exists(self.path + ".damaged.7"))
        self.assertEqual(saved(self.state), NO_ROWS)
        self.assertEqual(self.migrated(), [])

    def test_directory_is_moved_aside(self):
        """A directory where the file should be is moved aside too."""
        os.makedirs(self.path)
        self.assertEqual(self.load(), NO_ROWS)
        self.assertTrue(os.path.isdir(self.path + ".damaged"))

    @unittest.skipUnless(POSIX_MODES and hasattr(os, "getuid") and os.getuid() != 0, "needs permission checks")
    def test_unreadable_file_is_left_for_the_next_start(self):
        """A file that can't be read is logged and left (the store starts empty: records are republished), and
        imported once it can be read."""
        write_legacy(self.state, self.NAME, self.VALUE)
        os.chmod(self.path, 0)
        try:
            self.assertEqual(self.load(), NO_ROWS)
            self.assertTrue(os.path.exists(self.path))
        finally:
            os.chmod(self.path, 0o600)
        self.assertEqual(self.load(), self.ROWS)

    def test_interrupted_migration_isnt_repeated(self):
        """A crash between the commit and the rename: the next start only renames the file."""
        changed = dict(self.VALUE, records=[])
        state = self.interrupted(self.VALUE, changed, self.load)
        self.assertEqual(state, self.ROWS)
        self.assertEqual(saved(self.state), self.ROWS)
        self.migrated_again(changed, self.load, self.ROWS)

    def test_failed_commit_imports_nothing(self):
        """An import whose commit fails stops the start and leaves the file, imported at the next."""
        write_legacy(self.state, self.NAME, self.VALUE)
        with failing_commits():
            with self.assertRaises(OSError):
                self.store()
        self.assertTrue(os.path.exists(self.path))
        self.assertEqual(self.load(), self.ROWS)

    def test_failed_save_after_the_import_is_written_next_time(self):
        """Changes after the import whose save fails stay to be saved: the next save writes them."""
        write_legacy(self.state, self.NAME, self.VALUE)
        store = self.store()
        self.assertTrue(store.store(K3, P1))
        self.assertTrue(store.withdraw(K1, P3))
        with failing_commits():
            with self.assertRaises(OSError):
                store.save()
        self.assertEqual(saved(self.state), self.ROWS)
        store.save()
        self.assertEqual(saved(self.state), held(store))
        self.assertIn((K3, P1), saved(self.state)["records"])


class PublishedTest(MigrateTest):
    """published.json: {"published": {item: [when, [node, ...]]}, "earlier": {item: {node: when}},
    "withdraw": {node: [key, ...]}}, items being CIDs or (feature keys) "key:" and the key in hex, as rnfs
    0.1.3 and 0.1.4 both wrote it."""

    NAME = "published.json"
    KEY = DhtKey(K3)
    VALUE = {"published": {str(A): [T - 100, [N1.hex(), N2.hex()]], str(KEY): [T - 50, []]},
             "earlier": {str(A): {N3.hex(): T - 200}, str(B): {N1.hex(): T - 300, N2.hex(): T - 400}},
             "withdraw": {N1.hex(): [K1.hex(), K2.hex()], N3.hex(): []}}
    STATE = ({A: (T - 100, [N1, N2]), KEY: (T - 50, [])},
             {A: {N3: T - 200}, B: {N1: T - 300, N2: T - 400}},
             {N1: {K1, K2}, N3: set()})

    def publisher(self) -> Publisher:
        """A publisher on the state directory at T (closed before the directory is removed)."""
        publisher = Publisher(bytes(ID_LENGTH), Config(), None, None, None, None, state_dir=self.state,
                              clock=FakeClock(T))
        self.addCleanup(publisher.close)
        return publisher

    def load(self) -> tuple:
        """(published, earlier, withdrawals) as a publisher on the state directory loads them."""
        publisher = self.publisher()
        return publisher._published, publisher._earlier, publisher._withdrawals

    def rows(self) -> tuple:
        """The publisher's tables, sorted."""
        return tuple(sorted(state_rows(self.state, f"SELECT * FROM {table}"))
                     for table in ("published", "published_earlier", "withdrawals"))

    def test_valid_file_imports_exactly(self):
        """Every last round (CIDs and feature keys), earlier node and withdrawal is imported as written and
        loaded; the file is kept as published.json.migrated, and a second start changes nothing."""
        write_legacy(self.state, self.NAME, self.VALUE)
        expected = (sorted([(str(A), T - 100, N1 + N2), (str(self.KEY), T - 50, b"")]),
                    sorted([(str(A), N3, T - 200), (str(B), N1, T - 300), (str(B), N2, T - 400)]),
                    sorted([(N1, K1), (N1, K2)]))
        loaded = self.load()
        self.assertEqual(loaded[:2], self.STATE[:2])
        self.assertEqual(loaded[2], {N1: {K1, K2}})  # an empty list queues nothing
        self.assertEqual(self.rows(), expected)
        self.assertEqual(self.migrated(), [(7,)])
        self.assertTrue(os.path.exists(self.path + ".migrated"))
        self.assertEqual(self.load(), loaded)
        self.assertEqual(self.rows(), expected)

    def test_entries_of_the_wrong_shape_are_skipped(self):
        """Entries rnfs 0.1.4 skipped are skipped one by one, and so are those with node IDs, keys or feature keys
        not ID_LENGTH bytes (which it accepted); a feature key in capitals is written as str() writes it."""
        value = {"published": {"not an item": [T, []], str(A): [T, "0101"], str(B): [float("inf"), []],
                               "key:abcd": [T, []], "key:" + K1.hex().upper(): [T - 1, [N1.hex()]],
                               str(DhtKey(K2)): [T - 2, ["01"]]},
                 "earlier": {str(A): [], str(B): {N1.hex(): "now"}, str(self.KEY): {"zz": T},
                             str(DhtKey(K2)): {N2.hex(): T - 3}},
                 "withdraw": {N1.hex(): "ab", N2.hex(): ["ab"], "01": [K1.hex()], N3.hex(): [K3.hex()]}}
        write_legacy(self.state, self.NAME, value)
        self.assertEqual(self.load(), ({DhtKey(K1): (T - 1, [N1])}, {DhtKey(K2): {N2: T - 3}}, {N3: {K3}}))
        self.assertEqual(self.rows()[0], [(str(DhtKey(K1)), T - 1, N1)])

    def test_damaged_file_is_moved_aside(self):
        """A file that isn't JSON or has the wrong shape is moved to .damaged and the publisher starts without
        it."""
        for text in ("{not json", "", "[]", '{"published": []}', '{"earlier": {}, "withdraw": []}'):
            with self.subTest(text=text):
                write_legacy(self.state, self.NAME, text=text)
                self.assertEqual(self.load(), ({}, {}, {}))
                self.assertFalse(os.path.exists(self.path))
        self.assertTrue(os.path.exists(self.path + ".damaged.4"))
        self.assertEqual(self.migrated(), [])

    @unittest.skipUnless(POSIX_MODES and hasattr(os, "getuid") and os.getuid() != 0, "needs permission checks")
    def test_unreadable_file_stops_startup(self):
        """A file that can't be read stops the start, as it did (read_json), and is imported once it can be."""
        write_legacy(self.state, self.NAME, self.VALUE)
        os.chmod(self.path, 0)
        try:
            with self.assertRaises(PermissionError):
                self.publisher()
        finally:
            os.chmod(self.path, 0o600)
        self.assertEqual(self.load()[:2], self.STATE[:2])

    def test_interrupted_migration_isnt_repeated(self):
        """A crash between the commit and the rename: the next start only renames the file."""
        changed = {"published": {}, "earlier": {}, "withdraw": {N2.hex(): [K3.hex()]}}
        state = self.interrupted(self.VALUE, changed, self.load)
        self.assertEqual(state[:2], self.STATE[:2])
        self.migrated_again(changed, self.load, state)

    def test_failed_commit_imports_nothing(self):
        """An import whose commit fails stops the start and leaves the file, imported at the next."""
        write_legacy(self.state, self.NAME, self.VALUE)
        with failing_commits():
            with self.assertRaises(OSError):
                self.publisher()
        self.assertTrue(os.path.exists(self.path))
        self.assertEqual(self.load()[:2], self.STATE[:2])

    def test_failed_save_after_the_import_is_written_next_time(self):
        """Changes after the import whose save fails stay to be saved: the next save writes them."""
        write_legacy(self.state, self.NAME, self.VALUE)
        publisher = self.publisher()
        with publisher._lock:
            publisher._withdrawals[N2] = {K3}
            publisher._touch_node(N2)
            del publisher._published[A]
            publisher._touch_item(A)
        with failing_commits():
            with self.assertRaises(OSError):
                publisher.save()
        publisher.save()
        self.assertEqual(self.load(), ({self.KEY: (T - 50, [])}, self.STATE[1], {N1: {K1, K2}, N2: {K3}}))


if __name__ == "__main__":
    unittest.main()
