"""Tests for mutable names (specification section 9), with RNS's network side replaced by fakes."""
import os
import stat
import tempfile
import threading
import unittest
from unittest import mock

import RNS
from RNS.vendor import umsgpack

from rnfs import fsutil, names, statedb
from rnfs.config import Config
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.errors import NotFound, RnfsError
from rnfs.names import NameManager, encode_record, parse_record
from tests.support import CAN_SYMLINK, POSIX_MODES, FakeClock, failing_commits, state_execute, state_rows, wait_for

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence RNS's log, which names and RNS's key loading write to."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


CID_A = Cid.for_block(RAW, b"a")
CID_B = Cid.for_block(RAW, b"b")
CID_C = Cid.for_block(DAG, b"c")


class FakePins:
    """Storage stand-in recording pin sources per CID."""

    def __init__(self):
        """No pins."""
        self.sources: dict[Cid, set[str]] = {}

    def pin(self, cid, source):
        """Add source to cid's pins."""
        self.sources.setdefault(cid, set()).add(source)

    def unpin(self, cid, source) -> bool:
        """Remove source from cid's pins; True if it was there."""
        sources = self.sources.get(cid, set())
        found = source in sources
        sources.discard(source)
        if not sources:
            self.sources.pop(cid, None)
        return found


class FakeDestination:
    """An RNS destination that records its default app_data and announces."""

    def __init__(self, identity):
        """Destination of identity's name."""
        self.hash = names.name_for_identity(identity)
        self.app_data = None
        self.announced = []

    def set_default_app_data(self, app_data):
        """Record app_data."""
        self.app_data = app_data

    def announce(self):
        """Record an announce carrying the current app_data."""
        self.announced.append(self.app_data)


class FakeNetwork:
    """RnsNetwork stand-in: what RNS 'heard' is set by the test, path requests can answer via a callback."""

    def __init__(self):
        """Nothing heard yet."""
        self.destinations: dict[bytes, FakeDestination] = {}
        self.closed = []
        self.handlers = []
        self.app_data: dict[bytes, bytes] = {}
        self.heard: dict[bytes, float] = {}
        self.emitted: dict[bytes, float] = {}
        self.stored_emitted: dict[bytes, float] = {}  # when the announce RNS 'stored' last was emitted, if known
        self.path_requests = []
        self.on_request_path = None

    def name_destination(self, identity):
        """A fake destination; KeyError if one is registered for the same name, like RNS."""
        destination = FakeDestination(identity)
        if destination.hash in self.destinations:
            raise KeyError("Attempt to register an already registered destination.")
        self.destinations[destination.hash] = destination
        return destination

    def close_destination(self, destination):
        """Forget destination."""
        self.closed.append(destination)
        self.destinations.pop(destination.hash, None)

    def register_announce_handler(self, handler):
        """Keep handler."""
        self.handlers.append(handler)

    def deregister_announce_handler(self, handler):
        """Drop handler."""
        self.handlers.remove(handler)

    def request_path(self, name):
        """Record the request and run the test's answer, if any."""
        self.path_requests.append(name)
        if self.on_request_path:
            self.on_request_path(name)

    def recall_app_data(self, name):
        """What RNS 'stored' for name."""
        return self.app_data.get(name)

    def heard_time(self, name):
        """When RNS 'stored' it."""
        return self.heard.get(name)

    def emitted_time(self, name):
        """When the newest announce RNS 'took a path from' was emitted (None: tests not setting emitted)."""
        return self.emitted.get(name)

    def recalled_emission(self, name, app_data):
        """When the announce RNS 'stored' last was emitted (None if the test didn't say), and that nothing shows it
        isn't the path's record: an older one is a suspected replay all the same (DHT-2)."""
        return self.stored_emitted.get(name), True

    def store(self, name, app_data, heard, emitted=None):
        """Make RNS 'remember' an announce, as validate_announce does, without calling handlers. An announce
        emitted at emitted joins the path's, which keeps the newest emission: repeating an old one changes nothing."""
        self.app_data[name] = app_data
        self.heard[name] = heard
        self.stored_emitted.pop(name, None)
        if emitted is not None:
            self.stored_emitted[name] = emitted
            self.emitted[name] = max(emitted, self.emitted.get(name, emitted))

    def announce(self, name, app_data, heard, emitted=None):
        """Deliver an announce as RNS does: remembered, then passed to every matching handler."""
        self.store(name, app_data, heard, emitted)
        for handler in self.handlers:
            handler.received_announce(destination_hash=name, announced_identity=None, app_data=app_data)


def other_name(i: int) -> bytes:
    """A name that isn't local."""
    return bytes([i]) * 16


class FakeKeepers:
    """The keepers feature as NameManager uses it: a local record, and records other keepers answer."""

    def __init__(self):
        """Nothing kept, nobody answering."""
        self.local: dict[bytes, tuple[int, Cid, float]] = {}
        self.answers: dict[bytes, list[tuple[int, Cid, float]]] = {}
        self.queried: list[bytes] = []

    def local_record(self, name):
        """The record kept here for name."""
        return self.local.get(name)

    def query(self, name):
        """Records other keepers answer for name."""
        self.queried.append(name)
        return self.answers.get(name, [])

    def always_ask(self):
        """Keepers are asked only when the network gives nothing."""
        return False

    def published(self, name, identity, app_data):
        """A local name was published."""

    def heard(self, name, sequence, cid, packet_hash):
        """A record was heard."""


class NamesTestCase(unittest.TestCase):
    """A NameManager in a temporary rnfs dir with fake pins, network and clock."""

    def setUp(self):
        """Fresh directory and fakes."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = Config(rnfs_dir=self.tmp.name, path_timeout=0.2, name_table_size=3,
                             announce_interval=3600, startup_announce_delay=30)
        self.pins = FakePins()
        self.network = FakeNetwork()
        self.clock = FakeClock(1_000_000.0)
        self.manager = self.make_manager()

    def make_manager(self) -> NameManager:
        """A manager over the same directory (as after a restart), sharing the fakes; closed after the test. Its
        refreshes wait in virtual time: path_timeout passes at once unless an answer has already arrived."""
        manager = NameManager(self.config, self.pins, network=self.network, clock=self.clock, monotonic=self.clock,
                              wait=lambda event, timeout: event.is_set())
        self.addCleanup(manager.close)
        return manager

    def rows(self, sql: str, params=()) -> list[tuple]:
        """The rows sql reads from the state database."""
        return state_rows(self.config.state_dir, sql, params)


class RecordParsingTest(unittest.TestCase):
    """Strict parsing of heard app_data."""

    def test_valid_record(self):
        """A canonical [sequence, CID] parses, for both codecs and sequence 0."""
        self.assertEqual(parse_record(encode_record(7, CID_A)), (7, CID_A))
        self.assertEqual(parse_record(encode_record(0, CID_C)), (0, CID_C))
        self.assertEqual(parse_record(encode_record(names.MAX_SEQUENCE, CID_C)), (names.MAX_SEQUENCE, CID_C))

    def test_malformed_records_rejected(self):
        """Anything not exactly a canonical record is None."""
        malformed = [
            None, b"", "text", 5, b"\xc1",
            umsgpack.packb(5),
            umsgpack.packb([5]),
            umsgpack.packb([5, CID_A.binary, 1]),
            umsgpack.packb({"sequence": 5, "cid": CID_A.binary}),
            umsgpack.packb([True, CID_A.binary]),  # bool isn't a sequence
            umsgpack.packb([-1, CID_A.binary]),
            umsgpack.packb([1.0, CID_A.binary]),
            umsgpack.packb([None, CID_A.binary]),
            umsgpack.packb([1, str(CID_A)]),  # written form instead of binary
            umsgpack.packb([1, CID_A.binary[:-1]]),  # truncated CID
            umsgpack.packb([1, b"\x00" * 36]),  # unknown prefix
            umsgpack.packb([1, None]),
            encode_record(1, CID_A) + b"\x00",  # trailing bytes
            b"\x92\xcc\x05" + umsgpack.packb(CID_A.binary),  # 5 not in shortest form
            b"\x92\xcf" + (2**64 - 1).to_bytes(8, "big") + b"\xc4" + bytes([len(CID_A.binary) + 30])
            + CID_A.binary + b"\x00" * 30,  # too long
        ]
        for app_data in malformed:
            with self.subTest(app_data=app_data):
                self.assertIsNone(parse_record(app_data))

    def test_parse_name(self):
        """Names parse from 32 hex characters, with or without angle brackets."""
        name = bytes(range(16))
        self.assertEqual(names.parse_name(name.hex()), name)
        self.assertEqual(names.parse_name(f"<{name.hex()}>"), name)
        for bad in ["", "00" * 15, "zz" * 16, "00" * 17]:
            with self.assertRaises(ValueError):
                names.parse_name(bad)


class KeyTest(NamesTestCase):
    """Name keys: aliases, files, export and import."""

    def test_alias_validation(self):
        """Aliases that aren't safe file names are refused."""
        for bad in ["", ".", "..", ".hidden", "a/b", "../x", "a b", "-x", "x" * 65, None, "é"]:
            with self.subTest(alias=bad), self.assertRaises(ValueError):
                self.manager.create(bad)
        self.manager.create("my-site_1.0")

    def test_windows_alias_rules(self):
        """On Windows, aliases that are device names or lose a trailing dot are refused too."""
        with mock.patch.object(fsutil, "IS_WINDOWS", True):
            for bad in ["con", "NUL.txt", "com1", "site."]:
                with self.subTest(alias=bad), self.assertRaises(ValueError):
                    names.check_alias(bad)
            self.assertEqual(names.check_alias("console"), "console")
        if not fsutil.IS_WINDOWS:
            self.assertEqual(names.check_alias("con"), "con")

    def test_key_file_in_use_on_windows_is_kept_with_a_warning(self):
        """rm still forgets the name when Windows won't delete its key file yet."""
        self.manager.create("site")
        with mock.patch.object(fsutil, "remove", return_value=False), \
                mock.patch.object(names, "log") as log:
            self.manager.remove("site")
        self.assertEqual(self.manager.list(), [])
        self.assertTrue(any("in use" in call.args[0] for call in log.call_args_list))

    def test_create_stores_private_key_file(self):
        """create writes the identity at names_dir/alias, readable only by the owner, and lists it."""
        name = self.manager.create("site")
        path = os.path.join(self.config.names_dir, "site")
        if POSIX_MODES:  # Windows has no Unix permission bits
            self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
        with open(path, "rb") as f:
            self.assertEqual(len(f.read()), 64)  # RNS's raw private key, unchanged by newline translation
        self.assertEqual(len(name), 16)
        self.assertEqual(self.manager.list(), [{"alias": "site", "name": name.hex(), "cid": None, "sequence": 0,
                                                "exported": False}])
        with self.assertRaises(ValueError):
            self.manager.create("site")

    def test_keys_never_exported_are_listed_until_exported(self):
        """A created key is unexported until the user exports it; an imported one never is (the file it came from
        is a copy); reading a key for a feature (export_key) doesn't count; it all survives a restart, and rm
        forgets it."""
        self.manager.create("made")
        self.manager.create("other")
        key = self.manager.export_key("other")  # what keepers and the user's export both call
        self.assertEqual(self.manager.unexported(), ["made", "other"])
        self.manager.mark_exported("other")
        self.assertEqual(self.manager.unexported(), ["made"])
        self.manager.remove("other")
        self.manager.import_key("again", key)
        self.assertEqual(self.manager.unexported(), ["made"])
        self.assertEqual({entry["alias"]: entry["exported"] for entry in self.manager.list()},
                         {"made": False, "again": True})
        with self.assertRaises(NotFound):
            self.manager.mark_exported("missing")
        self.manager.close()
        self.manager = self.make_manager()
        self.assertEqual(self.manager.unexported(), ["made"])
        self.manager.remove("again")
        self.assertEqual(self.rows("SELECT how FROM name_exports"), [])

    def test_backup_warning_names_the_export_command(self):
        """create's and import's reminder gives the command that backs the key up."""
        self.assertIn("rnfs name export site -o site.key", names.key_backup_warning("site"))

    @unittest.skipUnless(CAN_SYMLINK, "creating symlinks needs Developer Mode or an admin on Windows")
    def test_create_refuses_an_alias_whose_key_is_a_dangling_symlink(self):
        """An alias whose key file is a dangling symlink can't be read but isn't free: create and import refuse it,
        and its saved record stays as it is."""
        self.manager.create("site")
        self.manager.publish("site", CID_A)
        self.manager.close()
        row = self.rows("SELECT * FROM names_published")
        path = os.path.join(self.config.names_dir, "site")
        os.remove(path)
        os.symlink(os.path.join(self.config.names_dir, "elsewhere"), path)
        restarted = self.make_manager()
        self.assertEqual(restarted._unreadable, {"site"})
        with self.assertRaisesRegex(ValueError, "alias site already exists"):
            restarted.create("site")
        with self.assertRaisesRegex(ValueError, "alias site already exists"):
            restarted.import_key("site", RNS.Identity().get_private_key())
        self.assertTrue(os.path.islink(path))
        self.assertEqual(self.rows("SELECT * FROM names_published"), row)

    def test_create_survives_a_remove_right_after_it(self):
        """A remove landing as soon as create lets go of the lock doesn't make create fail: it returns the name."""
        lock, depth, removed = self.manager._lock, [0], []
        manager = self.manager

        class RemovingLock:
            """The manager's lock, removing "site" (as another thread might) once create releases it."""

            def __enter__(self):
                """Take the lock."""
                lock.acquire()
                depth[0] += 1

            def __exit__(self, *exc):
                """Release it, then remove "site" the first time it is free."""
                depth[0] -= 1
                lock.release()
                if depth[0] == 0 and not removed:
                    removed.append(True)
                    manager.remove("site")

        manager._lock = RemovingLock()
        name = manager.create("site")
        self.assertEqual(len(name), 16)
        self.assertEqual((removed, manager.list()), ([True], []))

    def test_keys_survive_restart(self):
        """A new manager over the same directory loads the same names."""
        name = self.manager.create("site")
        self.assertEqual(self.make_manager().name("site"), name)

    def test_old_temp_files_removed_at_start(self):
        """Temp files interrupted key writes left are deleted (0.1.1's under fixed names, and write_atomic's,
        which hold a private key); nothing else is."""
        self.manager.create("site")
        old = [os.path.join(self.config.state_dir, "names.json.tmp"), os.path.join(self.config.names_dir, ".gone.tmp"),
               os.path.join(self.config.names_dir, fsutil.PARTIAL_PREFIX + "0123456789abcdef")]
        kept = [os.path.join(self.config.names_dir, name)
                for name in (".notes", "x.tmp", fsutil.PARTIAL_PREFIX + "short", fsutil.PARTIAL_PREFIX + "0" * 17)]
        for path in old + kept:
            with open(path, "wb") as f:
                f.write(b"partial")
        self.assertEqual(self.make_manager().list()[0]["alias"], "site")
        self.assertEqual([os.path.exists(path) for path in old + kept], [False] * len(old) + [True] * len(kept))
        # No temp file of its own: only the state database is there
        self.assertEqual([entry for entry in os.listdir(self.config.state_dir)
                          if not entry.startswith(statedb.DB_FILE)], [])

    def test_export_import_round_trip(self):
        """An exported key imported elsewhere gives the same name."""
        name = self.manager.create("site")
        key = self.manager.export_key("site")
        self.assertEqual(len(key), names.KEY_LENGTH)

        other = NameManager(Config(rnfs_dir=os.path.join(self.tmp.name, "other")), FakePins(),
                            network=FakeNetwork())
        self.addCleanup(other.close)
        self.assertEqual(other.import_key("copy", key), name)
        self.assertEqual(other.export_key("copy"), key)
        self.assertEqual(other.list()[0]["name"], name.hex())

    def test_import_rejects_bad_keys_and_duplicates(self):
        """Wrong-length keys, taken aliases and a key already held are refused."""
        self.manager.create("site")
        key = self.manager.export_key("site")
        with self.assertRaises(ValueError):
            self.manager.import_key("x", key[:-1])
        with self.assertRaises(ValueError):
            self.manager.import_key("x", "not bytes")
        with self.assertRaises(ValueError):
            self.manager.import_key("site", RNS.Identity().get_private_key())
        with self.assertRaises(ValueError):
            self.manager.import_key("again", key)

    def test_unknown_alias(self):
        """Operations on a missing alias raise NotFound."""
        for call in (lambda: self.manager.export_key("nope"), lambda: self.manager.remove("nope"),
                     lambda: self.manager.publish("nope", CID_A)):
            with self.assertRaises(NotFound):
                call()


class PublishTest(NamesTestCase):
    """Publishing: pins, sequences, persistence, announces."""

    def test_pin_source_swapped_on_republish(self):
        """The target is pinned name:<alias>, and the previous target released."""
        self.manager.create("site")
        self.manager.publish("site", CID_A)
        self.assertEqual(self.pins.sources, {CID_A: {"name:site"}})
        self.manager.publish("site", CID_A)
        self.assertEqual(self.pins.sources, {CID_A: {"name:site"}})
        self.manager.publish("site", CID_B)
        self.assertEqual(self.pins.sources, {CID_B: {"name:site"}})

    def test_pin_source_swap_keeps_other_sources(self):
        """Releasing name:<alias> leaves the old target's other pins alone."""
        self.pins.pin(CID_A, "user")
        self.manager.create("site")
        self.manager.publish("site", CID_A)
        self.manager.publish("site", CID_B)
        self.assertEqual(self.pins.sources[CID_A], {"user"})

    def test_remove_releases_pin(self):
        """Removing a name releases its pin, deletes its key and closes its destination."""
        self.manager.start()
        self.manager.create("site")
        self.manager.publish("site", CID_A)
        self.manager.remove("site")
        self.assertEqual(self.pins.sources, {})
        self.assertFalse(os.path.exists(os.path.join(self.config.names_dir, "site")))
        self.assertEqual(self.manager.list(), [])
        self.assertEqual(len(self.network.closed), 1)
        self.assertEqual(self.make_manager().list(), [])

    def test_deleted_key_file_releases_pin_once_asked(self):
        """A name whose key file was deleted by hand has its pin released by release_gone(), not while loading."""
        self.manager.create("site")
        self.manager.publish("site", CID_A)
        os.remove(os.path.join(self.config.names_dir, "site"))
        restarted = self.make_manager()  # the node isn't set up yet: releasing now would recompute provides
        self.assertEqual(self.pins.sources, {CID_A: {"name:site"}})
        restarted.release_gone()
        self.assertEqual(self.pins.sources, {})
        self.assertEqual(restarted.list(), [])
        self.make_manager().release_gone()  # its entry is gone from the state: nothing left to release
        self.assertEqual(self.pins.sources, {})

    def test_deleted_key_file_pin_released_once_at_reconcile(self):
        """reconcile_pins releases a deleted key's recorded pin once, with one warning; a pin nothing records is
        still released."""
        self.manager.create("site")
        self.manager.publish("site", CID_A)
        self.pins.pin(CID_B, "name:site")  # left by a crash
        os.remove(os.path.join(self.config.names_dir, "site"))
        restarted = self.make_manager()
        with mock.patch.object(names, "log") as log:
            restarted.reconcile_pins({cid: set(sources) for cid, sources in self.pins.sources.items()})
        self.assertEqual(self.pins.sources, {})
        self.assertEqual([call.args[0] for call in log.call_args_list],
                         ["name key site is gone; releasing its pin",
                          "releasing the pins of name site, which has no key"])
        log.reset_mock()
        self.pins.pin(CID_A, "name:site")
        self.pins.unpin(CID_B, "name:site")
        self.manager.close()
        state_execute(self.config.state_dir, "INSERT INTO names_published VALUES ('site', '1', ?, 0)",
                      (CID_A.binary,))
        restarted = self.make_manager()
        with mock.patch.object(names, "log") as log:
            restarted.reconcile_pins({cid: set(sources) for cid, sources in self.pins.sources.items()})
        self.assertEqual(self.pins.sources, {})
        self.assertEqual([call.args[0] for call in log.call_args_list], ["name key site is gone; releasing its pin"])

    def test_alias_replaced_while_refreshing_isnt_published(self):
        """An imported key removed and its alias made again while its first publish refreshes: the publish fails,
        rather than record the target under the old name and announce it under the new one."""
        self.manager.start()
        old = self.manager.import_key("site", RNS.Identity().get_private_key())

        def replace(name):
            """Remove "site" and create it again, with another key."""
            self.network.on_request_path = None
            self.manager.remove("site")
            self.manager.create("site")

        self.network.on_request_path = replace
        with self.assertRaisesRegex(NotFound, "replaced while publishing"):
            self.manager.publish("site", CID_A)
        new = self.manager.name("site")
        self.assertNotEqual(new, old)
        self.assertEqual((self.manager.record(old), self.manager.record(new)), (None, None))
        self.assertEqual(self.rows("SELECT * FROM names_held"), [])
        self.assertEqual(self.pins.sources, {})

    def test_sequence_increments(self):
        """Each publish is one higher than the last, and that survives a restart."""
        self.manager.create("site")
        self.assertEqual(self.manager.publish("site", CID_A)["sequence"], 1)
        self.assertEqual(self.manager.publish("site", CID_B)["sequence"], 2)
        restarted = self.make_manager()
        self.assertEqual(restarted.list()[0]["cid"], str(CID_B))
        self.assertEqual(restarted.publish("site", CID_A)["sequence"], 3)

    def test_publish_increments_past_resolvable_sequence(self):
        """A higher sequence RNS holds for the name (the key used elsewhere) is exceeded."""
        name = self.manager.create("site")
        self.manager.publish("site", CID_A)
        self.network.store(name, encode_record(41, CID_B), self.clock.now)
        self.assertEqual(self.manager.publish("site", CID_C)["sequence"], 42)

    def test_first_publish_of_imported_key_refreshes(self):
        """An imported key's first publish asks the network, and goes past what it answers."""
        key = RNS.Identity().get_private_key()
        name = self.manager.import_key("moved", key)

        def answer(requested):
            """The key's previous home announced sequence 9."""
            self.network.store(requested, encode_record(9, CID_A), self.clock.now + 1)

        self.network.on_request_path = answer
        self.assertEqual(self.manager.publish("moved", CID_B)["sequence"], 10)
        self.assertEqual(self.network.path_requests, [name])
        # Later publishes don't need the network
        self.manager.publish("moved", CID_C)
        self.assertEqual(self.network.path_requests, [name])

    def test_first_publish_of_imported_key_asks_keepers(self):
        """Its old announces may have expired while keepers still hold its highest sequence (section 9)."""
        keepers = self.manager.keepers = FakeKeepers()
        name = self.manager.import_key("moved", RNS.Identity().get_private_key())
        keepers.answers[name] = [(7, CID_A, self.clock.now - 10)]
        self.assertEqual(self.manager.publish("moved", CID_B)["sequence"], 8)
        self.assertEqual(keepers.queried, [name])
        # The record this node keeps itself counts too
        other = self.manager.import_key("other", RNS.Identity().get_private_key())
        keepers.local[other] = (4, CID_A, self.clock.now - 10)
        self.assertEqual(self.manager.publish("other", CID_B)["sequence"], 5)

    def test_key_without_state_is_published_above_the_network(self):
        """With its records lost, a key's first publish asks the network and keepers, as for an imported key."""
        keepers = self.manager.keepers = FakeKeepers()
        name = self.manager.create("site")
        for cid in (CID_A, CID_B, CID_C):
            self.manager.publish("site", cid)
        self.assertEqual(keepers.queried, [])  # a key made here needs no network
        for table in ("names_published", "names_held"):
            state_execute(self.config.state_dir, f"DELETE FROM {table}")
        keepers.answers[name] = [(3, CID_C, self.clock.now - 10)]
        restarted = self.make_manager()
        restarted.keepers = keepers
        self.assertEqual(restarted.publish("site", CID_A)["sequence"], 4)
        self.assertEqual(keepers.queried, [name])
        self.assertIn(name, self.network.path_requests)

    def test_announces(self):
        """start announces published names with their record; publish re-announces; the loop repeats."""
        self.manager.create("site")
        self.manager.create("empty")
        self.manager.publish("site", CID_A)
        name = self.manager.name("site")
        self.manager.start()
        destination = self.network.destinations[name]
        self.assertEqual(destination.announced, [encode_record(1, CID_A)])
        self.assertEqual(destination.app_data, encode_record(1, CID_A))
        empty = self.network.destinations[self.manager.name("empty")]
        self.assertEqual(empty.announced, [])

        # Again after startup_announce_delay, then every announce_interval
        self.assertEqual(self.manager.announce_due(), 30)
        self.clock.now += 30
        self.assertEqual(self.manager.announce_due(), 3600)
        self.assertEqual(len(destination.announced), 2)
        self.clock.now += 1000
        self.assertEqual(self.manager.announce_due(), 2600)
        self.assertEqual(len(destination.announced), 2)

        self.manager.publish("site", CID_B)
        self.assertEqual(destination.announced[-1], encode_record(2, CID_B))
        self.assertEqual(destination.app_data, encode_record(2, CID_B))

    def test_run_loop_stops(self):
        """run returns promptly once its stop event is set."""
        stop = threading.Event()
        thread = threading.Thread(target=self.manager.run, args=(stop,))
        thread.start()
        stop.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())

    def test_stop_deregisters(self):
        """stop removes the handler and every destination."""
        self.manager.create("site")
        self.manager.start()
        self.assertEqual(len(self.network.handlers), 1)
        self.assertTrue(self.network.handlers[0].receive_path_responses)
        self.assertEqual(self.network.handlers[0].aspect_filter, "rnfs.name")
        self.manager.stop()
        self.assertEqual(self.network.handlers, [])
        self.assertEqual(self.network.destinations, {})


class ResolveTest(NamesTestCase):
    """Resolving and the sequence rules."""

    def setUp(self):
        """A started manager, so announces reach its handler."""
        super().setUp()
        self.manager.start()

    def test_resolve_after_path_response(self):
        """A record arriving in answer to the path request is returned with its age."""
        name = other_name(1)
        self.network.on_request_path = lambda n: self.network.announce(n, encode_record(3, CID_A), self.clock.now)
        result = self.manager.resolve(name, persist=True)
        self.assertEqual(result, {"name": name.hex(), "cid": str(CID_A), "sequence": 3, "age": 0.0})

    def test_unresolvable(self):
        """A name nobody answers for resolves to None after path_timeout."""
        self.assertIsNone(self.manager.resolve(other_name(1), persist=True))

    def test_resolve_without_handler_call(self):
        """A path response RNS stores without calling handlers (path unchanged) still ends the wait."""
        name = other_name(1)
        self.network.on_request_path = lambda n: self.network.store(n, encode_record(2, CID_A), self.clock.now)
        result = self.manager.resolve(name, persist=False)
        self.assertEqual(result["sequence"], 2)

    def test_age(self):
        """age is how long ago the record was last heard."""
        name = other_name(1)
        self.network.announce(name, encode_record(1, CID_A), self.clock.now)
        self.clock.now += 100
        self.assertEqual(self.manager.resolve(name, persist=False, max_age=1000)["age"], 100)

    def test_sequence_rollback_rejected(self):
        """Once seen, a lower sequence is never accepted, from a handler, RNS's store, or after a restart."""
        name = other_name(1)
        self.network.announce(name, encode_record(5, CID_A), self.clock.now)
        self.assertEqual(self.manager.resolve(name, persist=True)["sequence"], 5)

        self.network.announce(name, encode_record(3, CID_B), self.clock.now)  # replayed old announce
        self.assertEqual(self.manager.resolve(name, persist=True)["sequence"], 5)

        restarted = self.make_manager()
        self.network.store(name, encode_record(4, CID_B), self.clock.now)
        result = restarted.resolve(name, persist=True)
        self.assertEqual((result["sequence"], result["cid"]), (5, str(CID_A)))

    def test_higher_sequence_replaces(self):
        """A higher sequence replaces the held record and is persisted."""
        name = other_name(1)
        self.network.announce(name, encode_record(5, CID_A), self.clock.now)
        self.manager.resolve(name, persist=True)
        self.network.announce(name, encode_record(6, CID_B), self.clock.now)
        self.assertEqual(self.make_manager().resolve(name, persist=True, max_age=10)["cid"], str(CID_B))

    def test_equal_sequence_keeps_held_record(self):
        """A different record with the same sequence doesn't replace the one held."""
        name = other_name(1)
        self.network.announce(name, encode_record(5, CID_A), self.clock.now)
        self.network.announce(name, encode_record(5, CID_B), self.clock.now + 1)
        result = self.manager.resolve(name, persist=False)
        self.assertEqual((result["sequence"], result["cid"]), (5, str(CID_A)))

    def test_same_record_heard_again_is_younger(self):
        """Hearing the held record again resets its age."""
        name = other_name(1)
        self.network.announce(name, encode_record(5, CID_A), self.clock.now)
        self.clock.now += 50
        self.network.announce(name, encode_record(5, CID_A), self.clock.now)
        self.assertEqual(self.manager.resolve(name, persist=False, max_age=10)["age"], 0)

    def test_replayed_announce_shows_its_record_current_for_a_bounded_time(self):
        """An old announce repeated (RNS remembers it as just heard; its emission stays) is heard no later than
        ANNOUNCE_VALIDITY announce intervals after it was emitted: a resolve past that asks keepers, which have
        the newer record, and a follow's or deny list's check, reusing a record for that long, does too after it."""
        name = other_name(1)
        emitted = self.clock.now
        window = names.ANNOUNCE_VALIDITY * self.config.announce_interval

        def replayed(manager, keepers):
            """The old announce, repeated by a neighbour, also in answer to path requests."""
            self.network.announce(name, encode_record(1, CID_A), self.clock.now, emitted=emitted)
            self.network.on_request_path = lambda n: self.network.store(n, encode_record(1, CID_A), self.clock.now,
                                                                        emitted=emitted)
            keepers.answers[name] = [(2, CID_B, emitted + 10)]  # the owner published again; this node missed it

        keepers = self.manager.keepers = FakeKeepers()
        replayed(self.manager, keepers)
        self.clock.now = emitted + window - 100
        replayed(self.manager, keepers)
        self.assertEqual(self.manager.resolve(name, persist=False)["sequence"], 1)
        self.clock.now = emitted + window + 100
        replayed(self.manager, keepers)
        self.assertEqual(self.manager.resolve(name, persist=False)["sequence"], 2)
        self.assertEqual(keepers.queried, [name])

        # A follow's check (max_age = window) on another node holding the old record
        self.network = FakeNetwork()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = Config(rnfs_dir=self.tmp.name, path_timeout=0.2, name_table_size=3, announce_interval=3600,
                             startup_announce_delay=30)
        self.clock.now = emitted
        follower = self.make_manager()
        follower.start()
        keepers = follower.keepers = FakeKeepers()
        replayed(follower, keepers)
        sequences = []
        while self.clock.now < emitted + 3 * window:
            self.clock.now += window / 4
            replayed(follower, keepers)
            sequences.append(follower.resolve(name, persist=True, max_age=window)["sequence"])
        self.assertEqual(sequences[:7], [1] * 7)  # up to 2 windows after it was emitted
        self.assertEqual(set(sequences[8:]), {2})

    def test_announces_of_an_owner_still_announcing_stay_current(self):
        """Each new announce has a new emission time, so the record stays current, and keepers aren't asked."""
        keepers = self.manager.keepers = FakeKeepers()
        name = other_name(1)
        window = names.ANNOUNCE_VALIDITY * self.config.announce_interval
        for _ in range(6):
            self.network.announce(name, encode_record(1, CID_A), self.clock.now, emitted=self.clock.now)
            self.assertEqual(self.manager.resolve(name, persist=True, max_age=window)["age"], 0)
            self.clock.now += self.config.announce_interval
        self.assertEqual((keepers.queried, self.network.path_requests), ([], []))

    def test_malformed_announce_ignored(self):
        """A malformed record is ignored and leaves the held one."""
        name = other_name(1)
        self.network.announce(name, encode_record(5, CID_A), self.clock.now)
        self.network.announce(name, umsgpack.packb([True, CID_B.binary]), self.clock.now)
        self.assertEqual(self.manager.resolve(name, persist=False)["cid"], str(CID_A))

    def test_gateway_resolves_are_not_persisted(self):
        """persist=False records live only in memory; persist=True ones survive a restart."""
        gateway_name, api_name = other_name(1), other_name(2)
        self.network.announce(gateway_name, encode_record(1, CID_A), self.clock.now)
        self.network.announce(api_name, encode_record(1, CID_B), self.clock.now)
        self.manager.resolve(gateway_name, persist=False)
        self.manager.resolve(api_name, persist=True)

        self.network.app_data.clear()
        self.network.heard.clear()
        restarted = self.make_manager()
        self.assertIsNone(restarted.resolve(gateway_name, persist=False))
        self.assertEqual(restarted.resolve(api_name, persist=False)["cid"], str(CID_B))

    def test_table_bounded_lru(self):
        """Heard records for other names stay within name_table_size, least recently used evicted."""
        for i in range(1, 4):
            self.network.announce(other_name(i), encode_record(1, CID_A), self.clock.now)
        self.network.app_data.clear()  # only what the manager holds counts
        self.manager.resolve(other_name(1), persist=False, max_age=10)  # 1 is now most recently used
        self.network.announce(other_name(4), encode_record(1, CID_A), self.clock.now)
        self.network.announce(other_name(5), encode_record(1, CID_A), self.clock.now)
        self.network.app_data.clear()

        self.assertEqual(len(self.manager._table), 3)
        held = {i for i in range(1, 6) if self.manager.resolve(other_name(i), persist=False, max_age=10)}
        self.assertEqual(held, {1, 4, 5})

    def test_persisted_names_not_evicted(self):
        """Names resolved for the user are outside the table, so a flood of announces can't evict them."""
        mine = other_name(100)
        self.network.announce(mine, encode_record(1, CID_A), self.clock.now)
        self.manager.resolve(mine, persist=True)
        for i in range(1, 20):
            self.network.announce(other_name(i), encode_record(1, CID_A), self.clock.now)
        self.network.app_data.clear()
        self.assertEqual(len(self.manager._table), 3)
        self.assertEqual(self.manager.resolve(mine, persist=False, max_age=10)["sequence"], 1)

    def test_local_name_resolves_without_network(self):
        """This node's own names resolve to their record with no path request."""
        name = self.manager.create("site")
        self.manager.publish("site", CID_A)
        result = self.manager.resolve(name, persist=True)
        self.assertEqual((result["sequence"], result["cid"]), (1, str(CID_A)))
        self.assertEqual(self.network.path_requests, [])

    def test_imported_name_resolves_from_network_until_published(self):
        """An imported key not yet published here may have records elsewhere, so resolving it asks."""
        name = self.manager.import_key("moved", RNS.Identity().get_private_key())
        self.network.on_request_path = lambda n: self.network.announce(n, encode_record(6, CID_A), self.clock.now)
        self.assertEqual(self.manager.resolve(name, persist=True)["sequence"], 6)
        self.assertEqual(self.network.path_requests, [name])

    def test_heard_time_in_the_future_isnt_fresh(self):
        """A record heard while the clock ran ahead is refreshed once the clock is corrected."""
        name = other_name(1)
        self.network.announce(name, encode_record(1, CID_A), self.clock.now)
        self.manager.resolve(name, persist=True)
        self.clock.now += 60  # past the first resolve's refresh, which a resolve reuses for max_age (E1)
        with self.manager._lock:  # as loaded from a record saved while the clock ran ahead
            self.manager._held[name] = names.Record(1, CID_A, self.clock.now + 10**6)
        self.network.path_requests.clear()
        self.manager.resolve(name, persist=True, max_age=60)
        self.assertEqual(self.network.path_requests, [name])

    def test_watched_name_first_heard_unasked_is_kept(self):
        """A name heard unasked and then watched counts as asked: a flood of new names doesn't evict it."""
        watched = other_name(1)
        self.network.announce(watched, encode_record(1, CID_A), self.clock.now)
        self.manager.watch(watched)
        for i in range(2, 10):
            self.network.announce(other_name(i), encode_record(1, CID_B), self.clock.now)
        self.assertEqual(self.manager.record(watched).sequence, 1)

    def test_resolve_rejects_bad_names(self):
        """Names must be 16 bytes."""
        with self.assertRaises(ValueError):
            self.manager.resolve(b"short", persist=False)


class RefreshCacheTest(NamesTestCase):
    """A resolve's network refresh is reused for max_age and shared by the resolves running alongside it (E1): a
    name whose owner is offline, with keepers answering."""

    AGE = 60.0  # the gateway's max_age (node.GATEWAY_NAME_AGE)

    def setUp(self):
        """A started manager with keepers holding sequence 4; RNS remembers the owner's last announce, of 3,
        emitted two days ago (it never shows current)."""
        super().setUp()
        self.manager.start()
        self.name = other_name(1)
        emitted = self.clock.now - 2 * 24 * 3600
        self.network.store(self.name, encode_record(3, CID_A), emitted, emitted=emitted)
        self.keepers = self.manager.keepers = FakeKeepers()
        self.keepers.answers[self.name] = [(4, CID_B, emitted + 10)]

    def gateway(self) -> dict | None:
        """A gateway resolve."""
        return self.manager.resolve(self.name, persist=False, max_age=self.AGE)

    def work(self) -> tuple[int, int]:
        """(path requests, keeper queries) so far."""
        return len(self.network.path_requests), len(self.keepers.queried)

    def test_gateway_resolves_reuse_a_refresh_for_max_age(self):
        """Gateway resolves within max_age of a refresh go to neither the network nor keepers; after it they do."""
        for _ in range(5):
            self.assertEqual(self.gateway()["sequence"], 4)
            self.clock.now += self.AGE / 5 - 1
        self.assertEqual(self.work(), (1, 1))
        self.clock.now += 5
        self.assertEqual(self.gateway()["sequence"], 4)
        self.assertEqual(self.work(), (2, 2))

    def test_cli_resolves_always_refresh(self):
        """A resolve with max_age 0 (the CLI's) never reuses a refresh, nor does it leave one the gateway can't."""
        for _ in range(3):
            self.assertEqual(self.manager.resolve(self.name, persist=True)["sequence"], 4)
        self.assertEqual(self.work(), (3, 3))
        self.assertEqual(self.gateway()["sequence"], 4)
        self.assertEqual(self.work(), (3, 3))

    def test_nothing_found_is_reused_too(self):
        """A name nothing answers for resolves to None, and again to None without the network for max_age."""
        self.keepers.answers.clear()
        self.network.app_data.clear()
        self.assertIsNone(self.gateway())
        self.assertIsNone(self.gateway())
        self.assertEqual(self.work(), (1, 1))

    def test_a_record_evicted_since_is_looked_for_again(self):
        """A refresh that found a record isn't reused once the bounded table has dropped it."""
        self.assertEqual(self.gateway()["sequence"], 4)
        for i in range(2, 2 + self.config.name_table_size):
            self.manager.offer(other_name(i), 1, CID_A, self.clock.now)
        self.assertIsNone(self.manager.record(self.name))
        self.assertEqual(self.gateway()["sequence"], 4)
        self.assertEqual(self.work(), (2, 2))

    def test_a_suspected_replay_is_not_reused(self):
        """A refresh leaving a record that is a suspected replay (DHT-2: older than the path's announce) isn't
        reused, so each resolve asks keepers again."""
        self.keepers.answers.clear()
        self.network.store(self.name, encode_record(3, CID_A), self.clock.now, emitted=self.clock.now - 3600)
        self.network.emitted[self.name] = self.clock.now - 60  # the path holds a newer announce
        for _ in range(3):
            self.assertEqual(self.gateway()["sequence"], 3)
        self.assertEqual(self.work(), (3, 3))

    def test_simultaneous_resolves_share_one_refresh(self):
        """Resolves of a name being refreshed, the CLI's included, wait for that refresh and take its record: one
        path request and one keeper query for all of them. Each persists as it asked."""
        gate = threading.Lock()
        gate.acquire()
        keepers = self.keepers
        real_query = keepers.query

        def query(name):
            """The keepers' answer, once the test opens the gate."""
            with gate:
                return real_query(name)

        keepers.query = query
        joined, real_wait = [], threading.Event.wait

        def wait(event, timeout=None):
            """Event.wait, noting a resolve waiting for a refresh running."""
            if event in self.manager._refreshing.values():
                joined.append(event)
            return real_wait(event, timeout)

        results = {}
        persist = {i: i == 0 for i in range(8)}  # resolve 0 is the CLI's; the rest the gateway's
        threads = [threading.Thread(target=lambda i=i: results.__setitem__(i, self.manager.resolve(
            self.name, persist=persist[i], max_age=0.0 if persist[i] else self.AGE))) for i in range(8)]
        with mock.patch.object(threading.Event, "wait", wait):
            for thread in threads:
                thread.start()
            wait_for(lambda: len(joined) == 7, what="seven resolves waiting for the eighth's refresh")
            gate.release()
            for thread in threads:
                thread.join(10)
        self.assertEqual(self.work(), (1, 1))
        self.assertEqual({i: result["sequence"] for i, result in results.items()}, {i: 4 for i in range(8)})
        self.assertEqual(self.manager._held[self.name].sequence, 4)  # the CLI's resolve persisted it
        self.assertEqual(self.manager._refreshing, {})


class AnnounceHoldTest(NamesTestCase):
    """A name's announces are held while the clock is behind its last announce, which RNS would take them as replays
    of (section 9, "Announces and the clock"). The wall clock and the monotonic one are separate here."""

    def setUp(self):
        """A manager whose monotonic clock is its own, with a published name announced at the start."""
        super().setUp()
        self.mono = FakeClock(50_000.0)
        self.manager = self.make_manager()
        self.name = self.manager.create("site")
        self.manager.publish("site", CID_A)
        self.manager.start()
        self.last = int(self.clock.now)
        self.assertEqual(self.announced(), [encode_record(1, CID_A)])

    def make_manager(self) -> NameManager:
        """A manager over the same directory, its wall clock self.clock and its monotonic clock self.mono."""
        mono = getattr(self, "mono", None) or self.clock
        manager = NameManager(self.config, self.pins, network=self.network, clock=self.clock, monotonic=mono,
                              wait=lambda event, timeout: event.is_set())
        self.addCleanup(manager.close)
        return manager

    def announced(self) -> list:
        """The records announced for the name so far."""
        return self.network.destinations[self.name].announced

    def restart_behind(self, seconds: float) -> NameManager:
        """Stop the manager and start another with the clock set seconds before the last announce."""
        self.manager.stop()
        self.manager.close()
        self.clock.now = self.last - seconds
        self.manager = self.make_manager()
        self.manager.start()
        return self.manager

    def test_last_announce_is_persisted(self):
        """Each announce's emission time is saved, in whole seconds as RNS signs it."""
        self.assertEqual(self.rows("SELECT name, emitted, held FROM name_announces"), [(self.name, self.last, 0.0)])
        self.clock.now += 3600.5
        self.mono.now += 3600
        self.manager.announce_due()
        self.assertEqual(self.rows("SELECT emitted FROM name_announces"), [(self.last + 3600,)])

    def test_publish_held_while_the_clock_is_behind(self):
        """A Pi without a clock comes back 10 minutes behind its last announce: the start's announce is held, and a
        publish is committed but held too, its result and status saying so, until the clock passes it."""
        manager = self.restart_behind(600)
        self.assertEqual(len(self.announced()), 0)  # a new destination: nothing announced through it
        self.assertEqual(manager.announce_status()["announces_held"], ["site"])
        result = manager.publish("site", CID_B)
        self.assertEqual(result["sequence"], 2)
        self.assertIn("10 min behind the name's last announce", result["announce_held"])
        self.assertEqual(self.announced(), [])
        # Committed: resolved and kept here, and pinned, like any publish
        self.assertEqual(manager.resolve(self.name, persist=True)["cid"], str(CID_B))
        self.assertIn(CID_B, self.pins.sources)

        # Rechecked every minute; still behind
        self.clock.now += 300
        self.mono.now += 300
        self.assertLessEqual(manager.announce_due(), names.HOLD_RECHECK)
        self.assertEqual(self.announced(), [])
        # The clock passes the last announce: the new record goes out, and the hold ends
        self.clock.now = self.last + 1
        self.mono.now += 60
        manager.announce_due()
        self.assertEqual(self.announced(), [encode_record(2, CID_B)])
        self.assertEqual(manager.announce_status()["announces_held"], [])
        self.assertNotIn("announce_held", manager.publish("site", CID_C))
        self.assertEqual(self.announced()[-1], encode_record(3, CID_C))

    def test_a_held_reason_leaves_out_an_estimate_under_a_second(self):
        manager = self.restart_behind(600)
        with mock.patch.object(manager.clock_skew, "behind", return_value=0.4):
            reason = manager.publish("site", CID_B)["announce_held"]
        self.assertNotIn("going by the announces heard", reason)  # not "it is 0 s behind"
        with mock.patch.object(manager.clock_skew, "behind", return_value=600.0):
            reason = manager.publish("site", CID_C)["announce_held"]
        self.assertIn("going by the announces heard, it is 10 min behind", reason)

    def test_a_clock_at_the_last_announce_is_not_held(self):
        """Only a clock behind holds: one in the same second as the last announce announces (RNS may drop that one,
        as it did before 0.2.0, until the next re-announce)."""
        manager = self.restart_behind(0)
        self.assertEqual(self.announced(), [encode_record(1, CID_A)])
        self.assertNotIn("announce_held", manager.publish("site", CID_B))

    def test_a_path_heard_from_the_key_elsewhere_holds_too(self):
        """A key used on another machine: the newest emission RNS holds a path to the name from counts as its last
        announce, though this node never announced it."""
        self.network.emitted[self.name] = self.clock.now + 120
        self.assertIn("2 min behind", self.manager.publish("site", CID_B)["announce_held"])
        self.assertEqual(self.announced(), [encode_record(1, CID_A)])

    def test_held_at_most_a_week_in_all(self):
        """A clock far behind (no saved time at all) holds the announces for HOLD_LIMIT, counted across restarts: by
        then every path taken from the last announce has lapsed, and the announces go out though still behind."""
        manager = self.restart_behind(365 * 24 * 3600)
        self.assertEqual(manager.announce_status()["announces_held"], ["site"])
        for _ in range(4):  # 4 days, a minute at a time being too slow: one recheck a day
            self.mono.now += 24 * 3600
            manager.announce_due()
        self.assertEqual(self.announced(), [])
        manager = self.restart_behind(365 * 24 * 3600)  # the 4 days held are saved at stop
        self.assertEqual(self.rows("SELECT held FROM name_announces"), [(4 * 24 * 3600.0,)])
        self.mono.now += 3 * 24 * 3600 - 1
        manager.announce_due()
        self.assertEqual(self.announced(), [])
        self.mono.now += names.HOLD_RECHECK
        manager.announce_due()
        self.assertEqual(self.announced(), [encode_record(1, CID_A)])
        self.assertEqual(manager.announce_status()["announces_held"], [])
        # Announced at the clock as it is: the next announces aren't held
        self.assertEqual(self.rows("SELECT emitted, held FROM name_announces"), [(int(self.clock.now), 0.0)])
        self.assertNotIn("announce_held", manager.publish("site", CID_B))

    def test_held_time_saved_while_held(self):
        """The time held is saved within HOLD_SAVE while held, not only at stop, so a crash loses little of it."""
        manager = self.restart_behind(600)
        self.mono.now += names.HOLD_SAVE - 1
        manager.announce_due()
        self.assertEqual(self.rows("SELECT held FROM name_announces"), [(0.0,)])
        self.mono.now += names.HOLD_RECHECK
        manager.announce_due()
        self.assertEqual(self.rows("SELECT held FROM name_announces"), [(names.HOLD_SAVE - 1 + names.HOLD_RECHECK,)])

    def test_removed_name_drops_its_hold(self):
        """Removing a held name ends its hold; its last announce stays saved, for when the key comes back."""
        manager = self.restart_behind(600)
        manager.remove("site")
        self.assertEqual(manager.announce_status()["announces_held"], [])
        self.assertEqual(self.rows("SELECT name FROM name_announces"), [(self.name,)])

    def test_malformed_rows_are_dropped(self):
        """A damaged name_announces row only loses a reason to hold: the node starts and announces."""
        self.manager.stop()
        self.manager.close()
        state_execute(self.config.state_dir, "UPDATE name_announces SET emitted = 'soon', held = 'x'")
        self.clock.now -= 600
        manager = self.make_manager()
        manager.start()
        self.assertEqual(self.announced(), [encode_record(1, CID_A)])

    def test_status_reports_the_clock_estimate(self):
        """announce_status gives the clock estimate from the name announces heard: none until enough senders."""
        self.assertEqual(self.manager.announce_status(), {"announces_held": [], "clock_offset": None})
        for i in range(1, 6):
            self.network.emitted[other_name(i)] = self.clock.now + 400
            self.network.announce(other_name(i), encode_record(1, CID_A), self.clock.now)
        self.assertEqual(self.manager.announce_status()["clock_offset"], 400)

    def test_path_responses_and_asked_names_are_not_samples(self):
        """A path response, or an announce of a name a resolve is waiting for (a shared instance passes path answers
        on as announces), says nothing of its owner's clock now."""
        for i in range(1, 6):
            self.network.emitted[other_name(i)] = self.clock.now - 3600
            self.network.handlers[0].received_announce(other_name(i), None, encode_record(1, CID_A), None, True)
        self.assertEqual(self.manager.clock_skew.estimate(), (None, 0))
        self.manager._waiters[other_name(1)] = [threading.Event()]
        self.manager._heard(other_name(1), encode_record(2, CID_A))
        self.assertEqual(self.manager.clock_skew.estimate(), (None, 0))
        self.manager._heard(other_name(2), encode_record(2, CID_A))
        self.assertEqual(self.manager.clock_skew.estimate(), (None, 1))


class StateTest(NamesTestCase):
    """The published and held records in the state database: what a save writes, failed commits, damaged rows."""

    def test_rows(self):
        """Published and held records are rows, sequences as decimal text; a key not yet published has no CID."""
        name = self.manager.create("site")
        self.manager.create("empty")
        self.manager.publish("site", CID_A)
        self.assertEqual(self.rows("SELECT alias, sequence, cid, imported FROM names_published ORDER BY alias"),
                         [("empty", "0", None, 0), ("site", "1", CID_A.binary, 0)])
        self.assertEqual(self.rows("SELECT name, sequence, cid, heard FROM names_held"),
                         [(name, "1", CID_A.binary, self.clock.now)])

    def test_sequences_past_sqlite_integers(self):
        """The highest sequence, past SQLite's signed integers, is saved and loaded as it is."""
        name = self.manager.create("site")
        self.network.store(name, encode_record(names.MAX_SEQUENCE - 1, CID_A), self.clock.now)
        self.assertEqual(self.manager.publish("site", CID_B)["sequence"], names.MAX_SEQUENCE)
        self.assertEqual(self.rows("SELECT sequence FROM names_published"), [(str(names.MAX_SEQUENCE),)])
        restarted = self.make_manager()
        self.assertEqual(restarted.list()[0]["sequence"], names.MAX_SEQUENCE)
        with self.assertRaises(RnfsError):
            restarted.publish("site", CID_C)

    def test_save_writes_only_changed_rows(self):
        """A publish writes its name's two rows, whatever else is saved; a save with nothing changed writes none."""
        for alias in ("a", "b", "c"):
            self.manager.create(alias)
            self.manager.publish(alias, CID_A)
        conn = self.manager._db._db().conn
        before = conn.total_changes
        self.manager.publish("b", CID_B)
        self.assertEqual(conn.total_changes - before, 2)  # b's published row and its held record
        before = conn.total_changes
        self.manager.stop()
        self.assertEqual(conn.total_changes, before)

    def test_failed_commit_rolls_the_publish_back(self):
        """A publish whose save can't be committed (a failing disk) fails, and memory, pins and the database
        say what they said before it; trying again works."""
        name = self.manager.create("site")
        self.manager.publish("site", CID_A)
        before = self.rows("SELECT * FROM names_published"), self.rows("SELECT * FROM names_held")
        with failing_commits(), self.assertRaises(OSError):
            self.manager.publish("site", CID_B)
        self.assertEqual(self.manager.list()[0]["cid"], str(CID_A))
        self.assertEqual(self.manager.record(name).cid, CID_A)
        self.assertEqual(self.pins.sources, {CID_A: {"name:site"}})
        self.assertEqual((self.rows("SELECT * FROM names_published"), self.rows("SELECT * FROM names_held")), before)
        self.assertEqual(self.manager.publish("site", CID_B)["sequence"], 2)
        self.assertEqual(self.make_manager().list()[0]["cid"], str(CID_B))

    def test_failed_resolve_save_is_retried(self):
        """A record resolved for the user whose save failed is still held, and saved by the next save."""
        name = other_name(1)
        self.network.store(name, encode_record(4, CID_A), self.clock.now)
        with failing_commits(), self.assertRaises(OSError):
            self.manager.resolve(name, persist=True)
        self.assertEqual(self.rows("SELECT * FROM names_held"), [])
        self.manager.stop()
        self.assertEqual(self.rows("SELECT name, sequence FROM names_held"), [(name, "4")])

    def test_failed_save_of_a_heard_record_is_retried(self):
        """An announce raising a held name's sequence whose save fails still wakes resolves and tells keepers and
        listeners; announce_due saves it."""
        name = other_name(1)
        self.network.store(name, encode_record(4, CID_A), self.clock.now)
        self.manager.resolve(name, persist=True)
        self.manager.start()
        keepers = self.manager.keepers = FakeKeepers()
        heard = []
        self.manager.watch(name)
        self.manager.add_record_listener(lambda n, record: heard.append(record.sequence))
        waiting = threading.Event()
        self.manager._waiters[name] = [waiting]
        with failing_commits(), mock.patch.object(keepers, "heard") as told:
            self.network.announce(name, encode_record(5, CID_B), self.clock.now)
        self.assertTrue(waiting.is_set())
        self.assertEqual((heard, told.call_args.args[1]), ([5], 5))
        self.assertEqual(self.manager.record(name).sequence, 5)
        self.assertEqual(self.rows("SELECT sequence FROM names_held"), [("4",)])
        self.manager.announce_due()
        self.assertEqual(self.rows("SELECT sequence FROM names_held"), [("5",)])

    def test_malformed_rows_stop_startup(self):
        """A published or held row that doesn't parse stops startup, as a malformed names.json did: starting
        without it would allow rollback."""
        self.manager.create("site")
        self.manager.publish("site", CID_A)
        self.manager.close()
        published, held = self.rows("SELECT * FROM names_published"), self.rows("SELECT * FROM names_held")
        damage = [
            ("UPDATE names_published SET sequence = 'one'", ()),
            ("UPDATE names_published SET sequence = ?", (b"1",)),
            ("UPDATE names_published SET cid = ?", (b"\x00" * 36,)),
            ("UPDATE names_published SET cid = 'text'", ()),
            ("UPDATE names_published SET imported = 2", ()),
            ("INSERT INTO names_published VALUES ('../x', '1', NULL, 0)", ()),
            ("UPDATE names_held SET name = ?", (b"short",)),
            ("UPDATE names_held SET sequence = '1.5'", ()),
            ("UPDATE names_held SET cid = ?", (CID_A.binary[:-1],)),
            ("UPDATE names_held SET heard = 'soon'", ()),
        ]
        for sql, params in damage:
            with self.subTest(sql=sql):
                for table, rows in (("names_published", published), ("names_held", held)):
                    state_execute(self.config.state_dir, f"DELETE FROM {table}")
                    for row in rows:
                        state_execute(self.config.state_dir, f"INSERT INTO {table} VALUES (?, ?, ?, ?)", row)
                state_execute(self.config.state_dir, sql, params)
                with self.assertRaisesRegex(RnfsError, "malformed name record"):
                    self.make_manager()

    def test_unreadable_key_row_left_as_it_is(self):
        """The row of an alias whose key file can't be read survives every save, unchanged."""
        self.manager.create("site")
        self.manager.publish("site", CID_A)
        self.manager.close()
        row = self.rows("SELECT * FROM names_published")
        with open(os.path.join(self.config.names_dir, "site"), "wb") as f:
            f.write(b"\x00" * 7)  # not a key
        restarted = self.make_manager()
        restarted.create("other")
        restarted.stop()
        self.assertEqual(self.rows("SELECT * FROM names_published WHERE alias = 'site'"), row)

    def test_close_is_idempotent(self):
        """close() releases the database once; again it does nothing."""
        self.manager.close()
        self.manager.close()
        self.assertNotIn(self.manager._db.path, statedb.open_databases())


class FloorSaveTest(NamesTestCase):
    """The name_floor rows are written outside the manager's lock, in order, only those changed, and a failed write
    is tried again."""

    def setUp(self):
        """A started manager."""
        super().setUp()
        self.manager.start()
        self.addCleanup(self.manager.stop)

    def resolve_for_gateway(self, i: int, sequence: int = 1):
        """Resolve other_name(i), just announced at sequence, without persist (so it goes in the floor)."""
        self.network.announce(other_name(i), encode_record(sequence, CID_A), self.clock.now)
        self.assertEqual(self.manager.resolve(other_name(i), persist=False, max_age=60)["sequence"], sequence)

    def saved(self) -> list:
        """The names the name_floor table holds, least recently resolved first."""
        return [name for (name,) in self.rows("SELECT name FROM name_floor ORDER BY used")]

    def plant(self, *rows):
        """Close the manager and plant (name, sequence, resolved, used) rows in name_floor."""
        self.manager.stop()
        self.manager.close()
        for row in rows:
            state_execute(self.config.state_dir, "INSERT INTO name_floor VALUES (?, ?, ?, ?)", row)

    def test_rows(self):
        """Each entry is a row: its name, sequence (decimal text), resolve time and place."""
        self.resolve_for_gateway(1, sequence=7)
        self.clock.now += 0.5
        self.resolve_for_gateway(2)
        self.manager.stop()
        self.assertEqual(self.rows("SELECT name, sequence, resolved FROM name_floor ORDER BY used"),
                         [(other_name(1), "7", 1_000_000.0), (other_name(2), "1", 1_000_000.5)])

    def test_order_and_raises_survive_a_restart(self):
        """A name resolved again moves last; a sequence raised by an announce keeps its place; both are loaded so."""
        for i in (1, 2, 3):
            self.clock.now += names.FLOOR_SAVE_INTERVAL
            self.resolve_for_gateway(i)
        self.clock.now += names.FLOOR_SAVE_INTERVAL
        self.resolve_for_gateway(1, sequence=2)
        self.network.announce(other_name(2), encode_record(9, CID_B), self.clock.now)
        self.manager.stop()
        self.assertEqual(self.saved(), [other_name(2), other_name(3), other_name(1)])
        restarted = self.make_manager()
        self.assertEqual([(name, sequence) for name, (sequence, _) in restarted._floor.items()],
                         [(other_name(2), 9), (other_name(3), 1), (other_name(1), 2)])

    def test_round_trip(self):
        """A restart loads the floor as it was saved, each name with its (sequence, resolved, used) value in the
        same order, after resolves, raises, a name held for the user and the highest sequence; a resolve after it
        goes last."""
        for i in (1, 2, 3, 4):
            self.clock.now += 1
            self.resolve_for_gateway(i, sequence=i)
        self.resolve_for_gateway(2, sequence=5)  # resolved again: last, raised
        self.network.announce(other_name(3), encode_record(9, CID_B), self.clock.now)  # raised in place
        self.manager.offer(other_name(1), names.MAX_SEQUENCE, CID_B, self.clock.now)  # a keeper's answer
        self.manager.resolve(other_name(4), persist=True, max_age=60)  # held: leaves the floor
        self.manager.stop()
        saved = list(self.manager._floor._entries.items())
        self.assertEqual([(name, sequence) for name, (sequence, _, _) in saved],
                         [(other_name(1), names.MAX_SEQUENCE), (other_name(3), 9), (other_name(2), 5)])
        restarted = self.make_manager()
        self.assertEqual(list(restarted._floor._entries.items()), saved)
        self.assertEqual(dict(restarted._floor), {name: (sequence, at) for name, (sequence, at, _) in saved})
        self.network.announce(other_name(5), encode_record(1, CID_A), self.clock.now)
        restarted.resolve(other_name(5), persist=False, max_age=60)
        self.assertEqual(list(restarted._floor), [other_name(1), other_name(3), other_name(2), other_name(5)])
        restarted.stop()
        self.assertEqual(list(self.make_manager()._floor), [other_name(1), other_name(3), other_name(2), other_name(5)])

    def test_save_writes_only_changed_rows(self):
        """A resolve adding a name to the floor writes its row alone, as a save after an announce raised one does."""
        for i in range(1, 6):
            self.clock.now += names.FLOOR_SAVE_INTERVAL
            self.resolve_for_gateway(i)
        conn = self.manager._db._db().conn
        before = conn.total_changes
        self.clock.now += names.FLOOR_SAVE_INTERVAL
        self.resolve_for_gateway(6)
        self.assertEqual(conn.total_changes - before, 1)
        before = conn.total_changes
        self.network.announce(other_name(6), encode_record(2, CID_B), self.clock.now)
        self.manager.announce_due()
        self.assertEqual(conn.total_changes - before, 1)
        self.assertEqual(self.rows("SELECT sequence FROM name_floor WHERE name = ?", (other_name(6),)), [("2",)])

    def saved_sequence(self, i: int) -> list[tuple]:
        """The sequence column of other_name(i)'s name_floor row (empty if it has none)."""
        return self.rows("SELECT sequence FROM name_floor WHERE name = ?", (other_name(i),))

    def test_a_raise_found_by_a_resolve_is_saved_soon(self):
        """A name in the table resolved again for the gateway, whose refresh hears a higher sequence, has the raise
        saved within FLOOR_SAVE_INTERVAL (the announce raised the floor before the resolve compared with it)."""
        self.resolve_for_gateway(1)
        self.assertEqual(self.saved_sequence(1), [("1",)])
        self.clock.now += names.FLOOR_SAVE_INTERVAL
        self.network.on_request_path = lambda name: self.network.announce(name, encode_record(2, CID_B),
                                                                          self.clock.now)
        self.assertEqual(self.manager.resolve(other_name(1), persist=False)["sequence"], 2)
        wait_for(lambda: self.saved_sequence(1) == [("2",)], timeout=names.FLOOR_SAVE_INTERVAL,
                 what="the raised floor to be saved")

    def test_a_raise_by_an_announce_is_saved_soon(self):
        """An announce raising the floor of a name in the table, with no resolve, is saved within
        FLOOR_SAVE_INTERVAL too, not at the next re-announce pass."""
        self.resolve_for_gateway(1)
        self.clock.now += names.FLOOR_SAVE_INTERVAL
        self.network.announce(other_name(1), encode_record(3, CID_B), self.clock.now)
        wait_for(lambda: self.saved_sequence(1) == [("3",)], timeout=names.FLOOR_SAVE_INTERVAL,
                 what="the raised floor to be saved")

    def test_a_raise_soon_after_a_save_waits_for_the_interval(self):
        """A raise within FLOOR_SAVE_INTERVAL of the last save is saved by the timer once the interval has passed."""
        with mock.patch.object(names, "FLOOR_SAVE_INTERVAL", 0.5):
            self.resolve_for_gateway(1)
            self.network.announce(other_name(1), encode_record(4, CID_B), self.clock.now)
            self.assertEqual(self.saved_sequence(1), [("1",)])
            self.assertIsNotNone(self.manager._floor._timer)
            wait_for(lambda: self.saved_sequence(1) == [("4",)], timeout=5, what="the timer's save")

    def test_no_timer_after_close(self):
        """A raise offered once the manager is closed (a keeper's late answer) starts no save."""
        self.resolve_for_gateway(1)
        self.manager.stop()
        self.manager.close()
        self.clock.now += names.FLOOR_SAVE_INTERVAL
        self.manager.offer(other_name(1), 5, CID_B, self.clock.now)
        self.assertIsNone(self.manager._floor._timer)

    def test_written_without_the_lock(self):
        """The floor's commit runs without the manager's lock: resolves and announces needn't wait for it."""
        real_commit = statedb._Database._commit
        free = []

        def commit(database, conn):
            """The floor's commit: another thread can take the manager's lock meanwhile."""
            lock = self.manager._lock
            thread = threading.Thread(target=lambda: free.append(lock.acquire(timeout=5)) or lock.release())
            thread.start()
            thread.join()
            return real_commit(database, conn)

        with mock.patch.object(statedb._Database, "_commit", commit):
            self.resolve_for_gateway(1)
        self.assertEqual(free, [True])
        self.assertEqual(self.saved(), [other_name(1)])

    def test_saves_never_land_out_of_order(self):
        """A save that copies the floor while another is still writing it isn't overwritten by the older copy."""
        real_commit = statedb._Database._commit
        second = []

        def commit(database, conn):
            """The first save's commit: meanwhile another resolve raises the floor and saves it."""
            if not second:
                self.clock.now += names.FLOOR_SAVE_INTERVAL
                second.append(threading.Thread(target=self.resolve_for_gateway, args=(2,)))
                second[0].start()
                second[0].join(0.2)
            return real_commit(database, conn)

        with mock.patch.object(statedb._Database, "_commit", commit):
            self.resolve_for_gateway(1)
            second[0].join(5)
        self.assertEqual(self.saved(), [other_name(1), other_name(2)])

    def test_failed_save_is_tried_again_at_once(self):
        """A save that couldn't be committed doesn't count for FLOOR_SAVE_INTERVAL: the next resolve saves again."""
        with failing_commits():
            self.resolve_for_gateway(1)
        self.assertEqual(self.saved(), [])
        self.resolve_for_gateway(2)  # within FLOOR_SAVE_INTERVAL of the failed save, which doesn't count
        self.assertEqual(self.saved(), [other_name(1), other_name(2)])
        self.resolve_for_gateway(3)  # but the save that worked does
        self.assertEqual(len(self.saved()), 2)

    def test_name_resolved_for_the_user_leaves_the_floor(self):
        """A floor name resolved for the user is held: its floor entry goes, and the next floor save deletes its row,
        so it no longer counts toward the floor's bound."""
        self.resolve_for_gateway(1)
        self.resolve_for_gateway(2)
        self.manager.announce_due()
        self.assertEqual(self.saved(), [other_name(1), other_name(2)])
        self.manager.resolve(other_name(1), persist=True, max_age=60)
        self.assertNotIn(other_name(1), self.manager._floor)
        self.assertNotIn(other_name(1), self.manager._floor._entries)
        self.manager.announce_due()
        self.assertEqual(self.saved(), [other_name(2)])

    def test_invalid_rows_are_dropped(self):
        """A floor row that doesn't parse is logged and deleted; the node starts without it."""
        good = (other_name(1), "5", 1.0, 0)
        self.plant(good, (b"short", "5", 1.0, 1), (other_name(2), str(names.MAX_SEQUENCE + 1), 1.0, 2),
                   (other_name(3), "-1", 1.0, 3), (other_name(4), "five", 1.0, 4), (other_name(5), "6", "soon", 5),
                   (other_name(6), "7", 2.0, "late"), (other_name(7), "8", 3.0, 2**63 - 1))
        with mock.patch.object(names, "log") as log:
            restarted = self.make_manager()
        self.assertEqual(dict(restarted._floor), {other_name(1): (5, 1.0)})
        self.assertEqual(self.rows("SELECT * FROM name_floor"), [good])
        self.assertEqual(sum("invalid name floor entry" in call.args[0] for call in log.call_args_list), 7)

    def test_rows_dropped_at_load_are_deleted(self):
        """The floor rows of names held for the user, and those past the floor's bound, are deleted at load."""
        state_execute(self.config.state_dir, "INSERT INTO names_held VALUES (?, '1', ?, 0.0)",
                      (other_name(100), CID_A.binary))
        size = names.FLOOR_FACTOR * self.config.name_table_size
        self.plant((other_name(100), "1", 0.0, 0), *[(other_name(i), "1", 0.0, i) for i in range(1, size + 3)])
        restarted = self.make_manager()
        kept = [other_name(i) for i in range(3, size + 3)]  # the two least recently resolved went
        self.assertEqual(list(restarted._floor), kept)
        self.assertEqual(self.saved(), kept)

    def test_future_resolve_time_counts_as_now(self):
        """A resolve time ahead of the clock (corrected since) loads as now, and is saved so with the next change."""
        self.plant((other_name(1), "5", self.clock.now + 1000, 0))
        restarted = self.make_manager()
        self.assertEqual(restarted._floor[other_name(1)], (5, self.clock.now))
        self.network.store(other_name(2), encode_record(1, CID_A), self.clock.now)
        restarted.resolve(other_name(2), persist=False)
        self.assertEqual(self.rows("SELECT resolved FROM name_floor ORDER BY used"), [(self.clock.now,)] * 2)


if __name__ == "__main__":
    unittest.main()
