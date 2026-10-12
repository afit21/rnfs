"""Tests for mutable names (specification section 9), with RNS's network side replaced by fakes."""
import os
import stat
import tempfile
import threading
import unittest
from unittest import mock

import RNS
from RNS.vendor import umsgpack

from rnfs import fsutil, names
from rnfs.config import Config
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.errors import NotFound
from rnfs.names import NameManager, encode_record, parse_record
from tests.support import POSIX_MODES

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

    def store(self, name, app_data, heard):
        """Make RNS 'remember' an announce, as validate_announce does, without calling handlers."""
        self.app_data[name] = app_data
        self.heard[name] = heard

    def announce(self, name, app_data, heard):
        """Deliver an announce as RNS does: remembered, then passed to every matching handler."""
        self.store(name, app_data, heard)
        for handler in self.handlers:
            handler.received_announce(destination_hash=name, announced_identity=None, app_data=app_data)


class Clock:
    """A settable clock, used for both wall and monotonic time."""

    def __init__(self, now=1_000_000.0):
        """Start at now."""
        self.now = now

    def __call__(self) -> float:
        """The current time."""
        return self.now


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
        self.clock = Clock()
        self.manager = self.make_manager()

    def make_manager(self) -> NameManager:
        """A manager over the same directory (as after a restart), sharing the fakes."""
        return NameManager(self.config, self.pins, network=self.network, clock=self.clock, monotonic=self.clock)


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
        self.assertEqual(self.manager.list(), [{"alias": "site", "name": name.hex(), "cid": None, "sequence": 0}])
        with self.assertRaises(ValueError):
            self.manager.create("site")

    def test_keys_survive_restart(self):
        """A new manager over the same directory loads the same names."""
        name = self.manager.create("site")
        self.assertEqual(self.make_manager().name("site"), name)

    def test_export_import_round_trip(self):
        """An exported key imported elsewhere gives the same name."""
        name = self.manager.create("site")
        key = self.manager.export_key("site")
        self.assertEqual(len(key), names.KEY_LENGTH)

        other = NameManager(Config(rnfs_dir=os.path.join(self.tmp.name, "other")), FakePins(),
                            network=FakeNetwork())
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
        """With names.json lost, a key's first publish asks the network and keepers, as for an imported key."""
        keepers = self.manager.keepers = FakeKeepers()
        name = self.manager.create("site")
        for cid in (CID_A, CID_B, CID_C):
            self.manager.publish("site", cid)
        self.assertEqual(keepers.queried, [])  # a key made here needs no network
        os.remove(os.path.join(self.config.state_dir, names.STATE_FILE))
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
        with self.manager._lock:  # as loaded from names.json saved while the clock ran ahead
            self.manager._held[name] = names.Record(1, CID_A, self.clock.now + 10**6)
        self.network.path_requests.clear()
        self.manager.resolve(name, persist=True, max_age=60)
        self.assertEqual(self.network.path_requests, [name])

    def test_resolve_rejects_bad_names(self):
        """Names must be 16 bytes."""
        with self.assertRaises(ValueError):
            self.manager.resolve(b"short", persist=False)


if __name__ == "__main__":
    unittest.main()
