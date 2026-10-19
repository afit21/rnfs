"""Regression tests for the names fixes of the second audit: kept records as a resolve floor (RM-L5), follow
retry backoff (DS-H1), republish cadence, jitter and pacing of kept names' keys (DS-M1), nick expansion
caching (DS-L1), round robin keeper retries (DS-L3), keys surviving a restart (BG-M1), keep_max_names
validation (BG-L7) and nicks for `name resolve`.
"""
import io
import os
import random
import tempfile
import time
import types
import unittest
from unittest import mock

import RNS

from rnfs import api
from rnfs.config import Config
from rnfs.constants import DAY, PROVIDER_TTL
from rnfs.content.cid import RAW, Cid
from rnfs.dht.providers import ProviderStore
from rnfs.features import contacts as contacts_module
from rnfs.features import follow as follow_module
from rnfs.features.follow import RETRY_INTERVAL, retry_delay
from rnfs.features.keepers import USER, name_key, sign_record
from rnfs.names import encode_record, name_for_identity
from rnfs.network import protocol
from rnfs.node import RnfsNode
from rnfs.publisher import KEY_MIN_INTERVAL, SLOW_LOOKUP_PACE, SLOW_ROUND_LOOKUPS, DhtKey, Publisher, key_interval

from tests.support import FakeClock
from tests.test_names import FakeNetwork
from tests.test_names_keep import KeeperNet
from tests.test_publisher import FakeClient, FakeLookup, near

A_ID, B_ID = bytes([0xA] * 16), bytes([0xB] * 16)
N1, N2 = bytes([1]) * 16, bytes([2]) * 16
OLD, NEW = Cid.for_block(RAW, b"old"), Cid.for_block(RAW, b"new")

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the nodes' log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class NodeTestCase(unittest.TestCase):
    """Offline nodes in a temporary directory."""

    def setUp(self):
        """A temporary directory and a network joining offline nodes."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = KeeperNet()

    def make_node(self, label: str, node_id: bytes, names_network=None) -> RnfsNode:
        """An offline node under label."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, label), slow="no", lookup_timeout=1, path_timeout=0.01)
        node = RnfsNode.offline(config, node_id, self.net, names_network or FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node


class KeptFloorTest(NodeTestCase):
    """RM-L5: a kept name's record is the floor for resolving, across restarts."""

    def test_replayed_old_announce_refused_after_restart(self):
        owner = RNS.Identity()
        name = name_for_identity(owner)
        node = self.make_node("b", B_ID)
        keepers = node.feature("keepers")
        keepers.store.keep(name, USER)
        keepers._store(sign_record(owner, name, encode_record(5, NEW)))
        node.stop()
        # After a restart, a path request is answered with the owner's older, validly signed announce
        fake = FakeNetwork()
        fake.on_request_path = lambda n: fake.store(n, encode_record(3, OLD), time.time())
        again = self.make_node("b", B_ID, fake)
        self.assertEqual(again.names.record(name).sequence, 5)  # offered at startup
        for for_gateway in (True, False):
            result = again.name_resolve(name.hex(), for_gateway=for_gateway)
            self.assertEqual((result["sequence"], result["cid"]), (5, str(NEW)))

    def test_floor_survives_table_eviction(self):
        owner = RNS.Identity()
        name = name_for_identity(owner)
        fake = FakeNetwork()
        node = self.make_node("b", B_ID, fake)
        keepers = node.feature("keepers")
        keepers.store.keep(name, USER)
        keepers._store(sign_record(owner, name, encode_record(5, NEW)))
        node.names._table.clear()  # evicted by other names
        fake.store(name, encode_record(3, OLD), time.time())
        self.assertEqual(node.name_resolve(name.hex(), for_gateway=True)["sequence"], 5)


class FakeMonotonic:
    """follow's time module with a settable monotonic clock."""

    def __init__(self):
        """Start at an arbitrary time."""
        self.now = 1000.0

    def monotonic(self) -> float:
        """The fake monotonic time."""
        return self.now


class FollowBackoffTest(NodeTestCase):
    """DS-H1: a followed name that can't be resolved or fetched doesn't flood path requests."""

    def setUp(self):
        """One node whose follow feature runs on a fake clock."""
        super().setUp()
        self.clock = FakeMonotonic()
        patcher = mock.patch.object(follow_module, "time",
                                    types.SimpleNamespace(monotonic=self.clock.monotonic, time=time.time))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.fake = FakeNetwork()
        self.node = self.make_node("b", B_ID, self.fake)
        self.follow = self.node.feature("follow")

    def simulate(self, seconds: float):
        """Run due checks for seconds of fake time."""
        end = self.clock.now + seconds
        while self.clock.now < end:
            self.clock.now += max(self.follow.run_due(), 1.0)

    def test_retry_delay_doubles_up_to_announce_interval(self):
        interval = 6 * 3600
        self.assertEqual([retry_delay(n, interval) for n in range(1, 9)],
                         [300, 600, 1200, 2400, 4800, 9600, 19200, interval])
        self.assertEqual(retry_delay(10_000, interval), interval)
        self.assertEqual(retry_delay(5, 60), RETRY_INTERVAL)  # never below the first delay

    def test_unresolvable_name_backs_off(self):
        self.follow.follow(name_for_identity(RNS.Identity()))
        self.simulate(DAY)
        # 288 a day at a fixed 5 minutes before; 300 s doubling up to 6 h is about ten
        self.assertLessEqual(len(self.fake.path_requests), 12)

    def test_failed_fetch_retries_without_path_requests(self):
        target = Cid.for_block(RAW, b"nobody has this")
        self.fake.on_request_path = lambda n: self.fake.store(n, encode_record(1, target), time.time())
        self.follow.follow(name_for_identity(RNS.Identity()))
        self.simulate(DAY)
        # The first check resolves; fetch retries use the record held. One more path request at most, once
        # the record is older than 2 x announce_interval
        self.assertLessEqual(len(self.fake.path_requests), 2)
        self.assertIn("failed", self.follow.following()[0]["state"])

    def test_periodic_check_is_local_while_record_is_fresh(self):
        fresh, stale = name_for_identity(RNS.Identity()), name_for_identity(RNS.Identity())
        interval = self.node.config.announce_interval
        for name, age in ((fresh, interval), (stale, 2 * interval + 60)):
            self.follow.follow(name)
            self.node.names.offer(name, 1, OLD, time.time() - age)
        self.follow.check(fresh)
        self.assertEqual(self.fake.path_requests, [])
        self.follow.check(stale)
        self.assertEqual(self.fake.path_requests, [stale])


class KeyCadenceTest(unittest.TestCase):
    """DS-M1: kept names' keys are republished at most every 3 days, desynchronised, and paced on slow nodes."""

    def make_publisher(self, keys: set[bytes], slow: bool = False, regions: bool = False, **settings) -> Publisher:
        """A publisher providing only keys, with fake network and clock; with regions, each key's lookup finds
        servers that make it a region of its own (sweep providing), else every lookup finds N1 and N2."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        from rnfs.content.storage import Storage
        config = Config(rnfs_dir=self.tmp.name, **settings)
        storage = Storage(os.path.join(self.tmp.name, "blocks"), os.path.join(self.tmp.name, "state"), 10**9)
        self.addCleanup(storage.close)  # its state database, before the directory goes (Windows)
        self.clock = FakeClock(1_000_000.0)
        self.client = FakeClient()
        publisher = Publisher(bytes(16), config, storage, ProviderStore(None), self.client,
                              lambda key: FakeLookup(near(key) if regions else [N1, N2]), slow_node=slow,
                              clock=self.clock)
        publisher.add_key_source(lambda: set(keys))
        publisher.refresh()
        return publisher

    def test_key_interval_bounds(self):
        keys = [os.urandom(16) for _ in range(200)]
        for base in (1000.0, DAY, 3 * DAY, 3.45 * DAY, PROVIDER_TTL / 2 - 1):
            intervals = [key_interval(key, base) for key in keys]
            floor = max(base, KEY_MIN_INTERVAL)
            self.assertTrue(all(0.9 * floor <= i < PROVIDER_TTL / 2 for i in intervals), base)
            self.assertTrue(all(i <= 1.1 * floor for i in intervals), base)
        intervals = [key_interval(key, DAY) for key in keys]
        self.assertGreater(max(intervals) - min(intervals), 0.15 * KEY_MIN_INTERVAL)  # spread out
        self.assertEqual(key_interval(keys[0], DAY), key_interval(keys[0], DAY))  # fixed per key

    def test_jitter_comes_from_the_first_byte(self):
        """Keys sharing their first byte (as a region's keys share their leading bits) come due together; the 256
        first bytes give 256 slots, spread over the whole +-10%."""
        rng = random.Random(5)
        for first in (0, 0x7F, 0xFF):
            keys = [bytes([first]) + rng.randbytes(15) for _ in range(20)]
            self.assertEqual(len({key_interval(key, DAY) for key in keys}), 1)
        slots = sorted({key_interval(bytes([first]) + bytes(15), DAY) for first in range(256)})
        self.assertEqual(len(slots), 256)
        self.assertLess(slots[0], 0.91 * KEY_MIN_INTERVAL)
        self.assertGreater(slots[-1], 1.09 * KEY_MIN_INTERVAL)

    def test_keys_republished_every_three_days_not_daily(self):
        key = os.urandom(16)
        publisher = self.make_publisher({key})
        self.assertEqual(publisher.publish_due()["published"], [DhtKey(key)])
        self.clock.now += DAY + 1
        self.assertEqual(publisher.publish_due()["published"], [])
        self.clock.now += key_interval(key, DAY) - DAY
        self.assertEqual(publisher.publish_due()["published"], [DhtKey(key)])

    def test_slow_node_paces_lookups(self):
        """At most SLOW_ROUND_LOOKUPS region lookups a round (each key a region here), and SLOW_LOOKUP_PACE per
        lookup before the next round."""
        keys = {os.urandom(16) for _ in range(25)}
        publisher = self.make_publisher(keys, slow=True, regions=True)
        self.assertEqual(len(publisher.publish_due()["published"]), SLOW_ROUND_LOOKUPS)
        self.assertEqual(publisher.publish_due()["published"], [])
        self.assertGreaterEqual(publisher.next_due(), SLOW_LOOKUP_PACE * SLOW_ROUND_LOOKUPS - 1)
        self.clock.now += SLOW_LOOKUP_PACE * SLOW_ROUND_LOOKUPS
        self.assertEqual(len(publisher.publish_due()["published"]), SLOW_ROUND_LOOKUPS)

    def test_withdrawal_of_a_key_also_provided_is_dropped(self):
        key = os.urandom(16)
        publisher = self.make_publisher({key})
        publisher._withdrawals[N1] = {key, b"\1" * 16}  # what a restart used to queue
        publisher.publish_due()
        sent = {node: (keys, withdrawn) for node, keys, withdrawn in self.client.calls}
        self.assertEqual(sent[N1], ([key], [b"\1" * 16]))
        self.assertEqual(publisher._withdrawals, {})


class RestartKeysTest(NodeTestCase):
    """BG-M1: a restart neither withdraws kept names' keys nor lets a batch delete what another stored."""

    def test_restart_queues_no_withdrawals(self):
        a = self.make_node("a", A_ID)
        b = self.make_node("b", B_ID)
        a.routing.heard(B_ID)
        b.routing.heard(A_ID)
        name = bytes.fromhex(a.name_create("site")["name"])
        a.name_publish("site", a.add_stream(io.BytesIO(b"v1"))["cid"])
        self.net.sender = A_ID
        a.publisher.publish_due()
        self.assertIn(A_ID, b.records.providers(name_key(name)))
        a.stop()
        again = self.make_node("a", A_ID)
        self.assertEqual(again.publisher._withdrawals, {})
        self.assertIn(name_key(name), again.publisher.provided_keys())

    def test_multi_batch_provide_keeps_stored_keys(self):
        keys = [os.urandom(16) for _ in range(40)]
        gone = [os.urandom(16) for _ in range(20)]
        batches = protocol.provide_batches(keys, keys + gone, 400)  # ~ a link MDU: several batches
        self.assertGreater(len(batches), 1)
        store = ProviderStore(None)
        store.provide(A_ID, gone)
        for batch_keys, batch_withdrawn in batches:
            store.provide(A_ID, batch_keys, batch_withdrawn)
        self.assertTrue(all(A_ID in store.providers(key) for key in keys))
        self.assertFalse(any(A_ID in store.providers(key) for key in gone))


class KeeperRetryTest(NodeTestCase):
    """DS-L3: past MAX_PENDING recordless names, the periodic retry asks every one in turn."""

    def test_round_robin(self):
        node = self.make_node("b", B_ID)
        keepers = node.feature("keepers")
        names = sorted(os.urandom(16) for _ in range(100))
        asked = []
        keepers.query = lambda name: asked.append(name) or []
        for name in names:
            keepers.store.keep(name, USER)
        for _ in range(2):
            keepers.retry_missing()
            keepers.run_pending()
        self.assertEqual(set(asked), set(names))


class ContactsCacheTest(NodeTestCase):
    """DS-L1: a nick's CID is reused for EXPAND_AGE from the resolve, whatever the record's age."""

    def test_expansion_cached_from_resolve_time(self):
        node = self.make_node("b", B_ID)
        contacts = node.feature("contacts")
        name = name_for_identity(RNS.Identity())
        contacts.add("alice", name.hex(), "name")
        calls = []

        def resolve(ident, persist, max_age=0.0):
            """A keeper's record: emitted long ago."""
            calls.append(ident)
            return {"name": ident.hex(), "cid": str(OLD), "sequence": 1, "age": 10 * DAY}

        clock = FakeMonotonic()
        with mock.patch.object(node.names, "resolve", resolve), \
                mock.patch.object(contacts_module, "time", types.SimpleNamespace(monotonic=clock.monotonic)):
            self.assertEqual(contacts.expand_target("alice/x"), f"{OLD}/x")
            self.assertEqual(contacts.expand_target("alice"), str(OLD))
            self.assertEqual(len(calls), 1)
            clock.now += contacts_module.EXPAND_AGE
            contacts.expand_target("alice")
            self.assertEqual(len(calls), 2)


class FakeHandler:
    """The parts of ApiHandler api_name_resolve uses."""

    def __init__(self, node, name: str):
        """A request for name."""
        self.node = node
        self.name = name
        self.sent = None

    def param(self, key: str, required: bool = True):
        """The name parameter."""
        return self.name

    def send_json(self, status: int, value):
        """Keep the response."""
        self.sent = (status, value)


class NameArgumentTest(NodeTestCase):
    """`rnfs name resolve` takes a name contact's nick, as keep and follow do; keep_max_names is checked."""

    def test_resolve_accepts_nick(self):
        node = self.make_node("b", B_ID)
        owner = RNS.Identity()
        name = name_for_identity(owner)
        node.feature("keepers").store.keep(name, USER)
        node.feature("keepers")._store(sign_record(owner, name, encode_record(2, NEW)))
        node.feature("contacts").add("alice", name.hex(), "name")
        for text in ("alice", name.hex()):
            handler = FakeHandler(node, text)
            api.ApiHandler.api_name_resolve(handler)
            self.assertEqual(handler.sent[1]["cid"], str(NEW))
        for bad in ("bob", "xyz!", "00"):
            with self.assertRaises(ValueError):
                api.ApiHandler.api_name_resolve(FakeHandler(node, bad))

    def test_keep_max_names_validation(self):
        self.assertEqual(Config().keep_max_names, 100)
        for bad in (True, False, 1.5, "10", -1):
            with self.assertRaises(ValueError):
                Config(keep_max_names=bad).validate()
        Config(keep_max_names=0).validate()


if __name__ == "__main__":
    unittest.main()
