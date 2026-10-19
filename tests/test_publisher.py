"""Tests for the provide strategy, batched publishing, republishing and withdrawal (rnfs/publisher.py)."""
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

from rnfs import publisher as publisher_module
from rnfs.config import Config
from rnfs.constants import K, PROVIDER_TTL
from rnfs.content import builder
from rnfs.content.cid import RAW, Cid
from rnfs.content.storage import USER, Storage
from rnfs.denylist import DenyList
from rnfs.dht.providers import ProviderStore
from rnfs.publisher import (MAX_WAIT, PROVIDE_PARALLEL, PUBLISH_LOOKUPS, SAVE_EVERY_NODES, SLOW_LOOKUP_PACE,
                            SLOW_ROUND_LOOKUPS, DhtKey, Publisher)
import tests.test_links as fake_links  # a module, so its test cases don't run twice
from tests.fakes.client import FakeClient
from tests.fakes.log import quiet_log
from tests.support import FakeClock, failing_commits, state_execute, state_rows, write_legacy
from tests.test_providers import recorded_writes, rows_written

OWN = bytes(16)
N1, N2, N3 = (bytes([i]) * 16 for i in (1, 2, 3))

setUpModule, tearDownModule = quiet_log()


class FakeLookup:
    """find_closest returns fixed nodes, the only ones it knows; no /find a sweep adds is answered."""

    def __init__(self, nodes):
        """A lookup answering nodes."""
        self.nodes = nodes
        self.asked: list[tuple[bytes, bytes]] = []

    def find_closest(self):
        """The fixed nodes."""
        return list(self.nodes)

    def candidates(self):
        """The fixed nodes, none unreliable."""
        return {node: False for node in self.nodes}

    def remaining(self):
        """Time left in its budget: plenty."""
        return 600.0

    def ask(self, node_id, key):
        """No answer (noted)."""
        self.asked.append((node_id, key))
        return None


def near(key: bytes, count: int = K) -> list[bytes]:
    """count server IDs sharing key's first 15 bytes: a lookup finding them makes key a region of its own (sweep
    providing), as random keys share far fewer bits."""
    return [key[:15] + bytes([(key[15] + 1 + i) % 256]) for i in range(count)]


class Running:
    """Counts the lookups running at once (the most ever in peak), the threads they ran on and those finished."""

    def __init__(self):
        """None run yet."""
        self.lock = threading.Lock()
        self.now = self.peak = self.finished = 0
        self.threads: set[str] = set()

    def __enter__(self):
        """A lookup starts."""
        with self.lock:
            self.now += 1
            self.peak = max(self.peak, self.now)
            self.threads.add(threading.current_thread().name)

    def __exit__(self, *exc):
        """It ends."""
        with self.lock:
            self.now -= 1
            self.finished += 1


class SleepingLookup(FakeLookup):
    """find_closest takes delay seconds, counted in running."""

    def __init__(self, nodes, delay: float, running: Running):
        """A lookup answering nodes after delay seconds."""
        super().__init__(nodes)
        self.delay, self.running = delay, running

    def find_closest(self):
        """The fixed nodes, once delay has passed."""
        with self.running:
            time.sleep(self.delay)
        return list(self.nodes)


class SlowClient(FakeClient):
    """A FakeClient whose /provide takes delay seconds, counting the requests in flight at once (the most in peak)."""

    def __init__(self, delay: float):
        """Each request takes delay seconds."""
        super().__init__()
        self.delay = delay
        self.now = self.peak = 0

    def provide(self, node_id, keys, withdrawn):
        """Record the request after delay seconds."""
        with self.lock:
            self.now += 1
            self.peak = max(self.peak, self.now)
        time.sleep(self.delay)
        with self.lock:
            self.now -= 1
        return super().provide(node_id, keys, withdrawn)


class GatedClient(FakeClient):
    """A FakeClient whose /provide requests wait for gate, once PROVIDE_PARALLEL of them are waiting (waiting)."""

    def __init__(self):
        """Nothing waiting yet."""
        super().__init__()
        self.gate, self.waiting = threading.Event(), threading.Event()
        self.count = 0
        self.results: list[dict] = []

    def provide(self, node_id, keys, withdrawn):
        """Wait for the gate, then record the request."""
        with self.lock:
            self.count += 1
            if self.count == PROVIDE_PARALLEL:
                self.waiting.set()
        self.gate.wait(5)
        with self.lock:
            return super().provide(node_id, keys, withdrawn)


class Crash(BaseException):
    """Stands in for the process dying in the middle of a round."""


class CrashingClient(FakeClient):
    """A FakeClient that dies at every request once crash_at have been recorded."""

    def __init__(self, crash_at: int):
        """Crash from the crash_at'th request (0-based) on."""
        super().__init__()
        self.crash_at = crash_at

    def provide(self, node_id, keys, withdrawn):
        """Record the request, or crash."""
        with self.lock:
            if len(self.calls) >= self.crash_at:
                raise Crash()
            return super().provide(node_id, keys, withdrawn)


class PublisherTest(unittest.TestCase):
    """A Publisher over temporary storage with fakes for the network."""

    def setUp(self):
        """Storage, an in-memory provider store, and a publisher whose lookups find self.nodes."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = os.path.join(self.tmp.name, "state")
        self.config = Config(rnfs_dir=self.tmp.name, republish_interval=1000)
        self.storage = Storage(os.path.join(self.tmp.name, "blocks"), self.state, 10**9)
        self.addCleanup(self.storage.close)
        self.records = ProviderStore(None)
        self.client = FakeClient()
        self.clock = FakeClock(1_000_000.0)
        self.nodes = [N1, N2]
        self.eligible: set[Cid] = set()
        self.lookups: list[bytes] = []
        self.on_lookup = lambda key: None
        self.nodes_of = lambda key: self.nodes  # the nodes a key's lookup finds
        self.lookup_delay = None  # or key -> seconds its lookup takes (counted in self.running)
        self.running = Running()
        self.fetched_changes = 0  # what the node's fetched-targets change count would be (generation)
        self.publisher = self.make_publisher()

    def make_publisher(self, slow: bool = False, state: str | None = None, generation: bool = False) -> Publisher:
        """A publisher sharing this test's state directory (or on state), closed when the test ends; with
        generation, one as the node makes it: storage.changes() and fetched_changes."""
        def make_lookup(key):
            """Remember the key; find self.nodes_of(key), after self.lookup_delay(key) seconds if set."""
            self.lookups.append(key)
            self.on_lookup(key)
            if self.lookup_delay is None:
                return FakeLookup(self.nodes_of(key))
            return SleepingLookup(self.nodes_of(key), self.lookup_delay(key), self.running)
        publisher = Publisher(OWN, self.config, self.storage, self.records, self.client, make_lookup, slow,
                              state or self.state, cached_eligible=lambda: self.eligible, clock=self.clock,
                              retry_interval=30,
                              generation=(lambda: (self.storage.changes(), self.fetched_changes)) if generation
                              else None)
        self.addCleanup(publisher.close)
        return publisher

    def saved(self) -> dict:
        """The publisher's tables: {"published": {item: (at, nodes)}, "earlier": {(item, node): at},
        "withdraw": {node: {key, ...}}}."""
        withdraw: dict[bytes, set[bytes]] = {}
        for node, key in state_rows(self.state, "SELECT node, key FROM withdrawals"):
            withdraw.setdefault(node, set()).add(key)
        return {"published": {item: (at, nodes) for item, at, nodes in
                              state_rows(self.state, "SELECT item, at, nodes FROM published")},
                "earlier": {(item, node): at for item, node, at in
                            state_rows(self.state, "SELECT item, node, at FROM published_earlier")},
                "withdraw": withdraw}

    def held(self, publisher: Publisher) -> dict:
        """What publisher holds in memory, shaped as saved() reads the tables."""
        with publisher._lock:
            return {"published": {str(item): (at, b"".join(nodes))
                                  for item, (at, nodes) in publisher._published.items()},
                    "earlier": {(str(item), node): at for item, nodes in publisher._earlier.items()
                                for node, at in nodes.items()},
                    "withdraw": {node: set(keys) for node, keys in publisher._withdrawals.items() if keys}}

    def withdrawn(self) -> dict[bytes, set[bytes]]:
        """The keys withdrawn from each node by the calls made so far."""
        result: dict[bytes, set[bytes]] = {}
        for node_id, _, keys in self.client.calls:
            if keys:
                result.setdefault(node_id, set()).update(keys)
        return result

    def pinned(self, data: bytes) -> Cid:
        """A complete pinned file."""
        built = builder.build_bytes(data, self.storage.put)
        self.storage.pin(built.cid, USER)
        return built.cid

    def test_roots_is_the_default(self):
        from rnfs.config import Config
        self.assertEqual(Config().provide_strategy, "roots")

    def test_pinned_strategy(self):
        self.config.provide_strategy = "pinned"
        complete = self.pinned(b"complete" * 10000)
        missing = Cid.for_block(RAW, b"not stored")
        self.storage.pin(missing, USER)
        cached = builder.build_bytes(b"cached", self.storage.put)
        self.storage.record_fetch(cached.cid, [], cached.cid)
        self.eligible.add(cached.cid)
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {complete})
        self.assertTrue(self.publisher.serves(complete))
        self.assertFalse(self.publisher.serves(cached.cid))

    def test_roots_strategy_provides_eligible_cached_targets(self):
        self.config.provide_strategy = "roots"
        user_fetch = builder.build_bytes(b"fetched by the user", self.storage.put)
        gateway_fetch = builder.build_bytes(b"fetched by the gateway", self.storage.put)
        for built in (user_fetch, gateway_fetch):
            self.storage.record_fetch(built.cid, [], built.cid)
        self.eligible.add(user_fetch.cid)
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {user_fetch.cid})
        # Evicted from the cache: no longer provided
        self.storage.gc()
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), set())

    def test_keys_batched_per_node_and_self_recorded(self):
        a, b = self.pinned(b"a"), self.pinned(b"b")
        self.publisher.refresh()
        result = self.publisher.publish_due()
        self.assertEqual(set(result["published"]), {a, b})
        self.assertEqual(len(self.client.calls), 2)  # one request per node, both keys in it
        for node_id, keys, withdrawn in self.client.calls:
            self.assertEqual(set(keys), {a.key, b.key})
            self.assertEqual(withdrawn, [])
        self.assertEqual(self.records.providers(a.key), [OWN])

    def test_not_republished_until_due(self):
        self.pinned(b"x")
        self.publisher.refresh()
        self.publisher.publish_due()
        self.client.calls.clear()
        self.clock.now += 999
        self.assertEqual(self.publisher.publish_due()["published"], [])
        self.assertEqual(self.client.calls, [])
        self.assertAlmostEqual(self.publisher.next_due(), 1)
        self.clock.now += 1
        self.assertEqual(len(self.publisher.publish_due()["published"]), 1)

    def test_state_survives_restart(self):
        self.pinned(b"y")
        self.publisher.refresh()
        self.publisher.publish_due()
        self.client.calls.clear()
        restarted = self.make_publisher()
        restarted.refresh()
        self.assertEqual(restarted.publish_due()["published"], [])
        self.assertEqual(self.client.calls, [])

    def test_unpin_withdraws_from_last_published_nodes(self):
        cid = self.pinned(b"z")
        self.nodes = [N1, N2, N3]
        self.client.refuse.add(N3)
        self.publisher.refresh()
        self.publisher.publish_due()
        self.client.calls.clear()
        self.storage.unpin(cid, USER)
        self.publisher.refresh()
        self.assertEqual(self.records.providers(cid.key), [])  # local record withdrawn at once
        self.assertEqual(self.publisher.next_due(), 0)  # the withdrawal is due now
        self.publisher.publish_due()
        self.assertEqual(sorted(self.client.calls), [(N1, [], [cid.key]), (N2, [], [cid.key])])
        self.client.calls.clear()
        self.publisher.publish_due()
        self.assertEqual(self.client.calls, [])  # sent once

    def test_withdrawal_rides_with_provides(self):
        old = self.pinned(b"old")
        self.publisher.refresh()
        self.publisher.publish_due()
        self.client.calls.clear()
        self.storage.unpin(old, USER)
        new = self.pinned(b"new")
        self.publisher.refresh()
        self.publisher.publish_due()
        self.assertEqual(sorted(self.client.calls), [(N1, [new.key], [old.key]), (N2, [new.key], [old.key])])

    def test_no_nodes_retried_soon(self):
        self.nodes = []
        cid = self.pinned(b"lonely")
        self.publisher.refresh()
        self.publisher.publish_due()
        self.assertEqual(self.client.calls, [])
        self.assertAlmostEqual(self.publisher.next_due(), 30)
        self.nodes = [N1]
        self.clock.now += 30
        self.publisher.publish_due()
        self.assertEqual(self.client.calls, [(N1, [cid.key], [])])

    def test_refused_store_not_retried_until_republish(self):
        self.pinned(b"refused")
        self.client.refuse = {N1, N2}
        self.publisher.refresh()
        self.publisher.publish_due()
        self.client.calls.clear()
        self.clock.now += 10
        self.publisher.publish_due()
        self.assertEqual(self.client.calls, [])

    def test_keys_a_node_recorded_before_refusing_are_withdrawn_later(self):
        cids = [self.pinned(b"key %d" % i) for i in range(3)]
        self.client.limit[N1] = 1  # e.g. N1's per-publisher limit was reached after one key
        self.publisher.refresh()
        self.publisher.publish_due()
        first = next(keys for node_id, keys, _ in self.client.calls if node_id == N1)[0]
        self.client.calls.clear()
        for cid in cids:
            self.storage.unpin(cid, USER)
        self.publisher.refresh()
        self.publisher.publish_due()
        self.assertEqual(self.withdrawn(), {N1: {first}, N2: {cid.key for cid in cids}})

    @mock.patch.object(publisher_module, "PROVIDE_PARALLEL", 1)  # one at a time: the stop point is exact
    def test_round_stopped_midway_leaves_unsent_cids_due(self):
        cid = self.pinned(b"interrupted")
        self.publisher.refresh()
        result = self.publisher.publish_due(should_stop=lambda: len(self.client.calls) >= 1)
        self.assertEqual([node_id for node_id, _, _ in self.client.calls], [N1])  # N2 never sent
        self.assertEqual(result["published"], [])
        self.assertEqual(self.publisher.next_due(), 0)
        # Still due after a restart; and N1, which recorded it, is withdrawn from on unpin
        self.publisher = self.make_publisher()
        self.publisher.refresh()
        self.assertEqual(self.publisher.next_due(), 0)
        self.client.calls.clear()
        self.storage.unpin(cid, USER)
        self.publisher.refresh()
        self.publisher.publish_due()
        self.assertEqual(self.withdrawn(), {N1: {cid.key}})

    def test_unpin_withdraws_from_earlier_rounds_nodes(self):
        cid = self.pinned(b"moving")
        self.publisher.refresh()
        self.publisher.publish_due()  # N1, N2
        self.nodes = [N2, N3]  # N1 left the closest set; its record lives on for PROVIDER_TTL
        self.clock.now += 1000
        self.publisher.publish_due()
        self.publisher = self.make_publisher()  # remembered across a restart
        self.publisher.refresh()
        self.client.calls.clear()
        self.storage.unpin(cid, USER)
        self.publisher.refresh()
        self.publisher.publish_due()
        self.assertEqual(self.withdrawn(), {N1: {cid.key}, N2: {cid.key}, N3: {cid.key}})

    def test_earlier_nodes_forgotten_once_their_record_expired(self):
        cid = self.pinned(b"moved")
        self.publisher.refresh()
        self.publisher.publish_due()  # N1, N2
        self.nodes = [N2, N3]
        self.clock.now += 1000
        self.publisher.publish_due()
        self.clock.now += PROVIDER_TTL
        self.publisher.publish_due()  # N1's record has expired by now
        self.client.calls.clear()
        self.storage.unpin(cid, USER)
        self.publisher.refresh()
        self.publisher.publish_due()
        self.assertEqual(self.withdrawn(), {N2: {cid.key}, N3: {cid.key}})

    def test_withdrawals_queued_during_lookups_go_in_the_same_round(self):
        old = self.pinned(b"old")
        self.publisher.refresh()
        self.publisher.publish_due()
        new = self.pinned(b"new")
        self.publisher.refresh()
        self.client.calls.clear()

        def unpin_old(key):
            """Unpin old while new's lookup runs."""
            if key == new.key:
                self.storage.unpin(old, USER)
                self.publisher.refresh()

        self.on_lookup = unpin_old
        self.publisher.publish_due()
        self.assertEqual(sorted(self.client.calls), [(N1, [new.key], [old.key]), (N2, [new.key], [old.key])])

    def test_cid_unpinned_during_its_lookup_isnt_stored(self):
        cid = self.pinned(b"short-lived")
        self.publisher.refresh()

        def unpin(key):
            """Unpin it while its lookup runs."""
            self.storage.unpin(cid, USER)
            self.publisher.refresh()

        self.on_lookup = unpin
        self.assertEqual(self.publisher.publish_due()["published"], [])
        self.assertEqual(self.client.calls, [])
        self.assertEqual(self.records.providers(cid.key), [])  # nor recorded locally

    def test_times_in_the_future_are_clamped(self):
        # Published while the clock ran ahead: once corrected, it's republished an interval from now
        self.pinned(b"ahead")
        self.publisher.refresh()
        self.clock.now += 10**6
        self.publisher.publish_due()
        self.clock.now -= 10**6
        self.assertAlmostEqual(self.publisher.next_due(), 1000)
        self.publisher.save()
        self.clock.now += 10**6
        self.publisher.publish_due()
        self.publisher.save()
        self.clock.now -= 10**6
        restarted = self.make_publisher()
        restarted.refresh()
        self.assertAlmostEqual(restarted.next_due(), 1000)

    def test_slow_pacing_clamped_after_the_clock_goes_back(self):
        publisher = self.make_publisher(slow=True)
        self.pinned(b"paced")
        publisher.refresh()
        self.clock.now += 10**6
        publisher.publish_due()
        self.clock.now -= 10**6
        self.pinned(b"new")  # due now, but lookups are paced
        publisher.refresh()
        self.assertEqual(publisher.next_due(), SLOW_LOOKUP_PACE * SLOW_ROUND_LOOKUPS)  # at most a full round's wait

    def test_legacy_state_of_the_wrong_shape_is_ignored(self):
        """published.json of the wrong shape is moved aside, and entries of the wrong shape (or with node IDs or
        keys not ID_LENGTH bytes) are skipped: nothing is imported, so the CID is due."""
        cid = self.pinned(b"shape")
        for n, value in enumerate(([], {"published": []}, {"published": {str(cid): 5}, "withdraw": []},
                                   {"published": {str(cid): [10**400, []]}}, {"published": {str(cid): [1, "0101"]}},
                                   {"published": {str(cid): [1, ["0101"]]}},
                                   {"earlier": {str(cid): []}, "withdraw": {N1.hex(): "ab"}},
                                   {"earlier": {str(cid): {"01": 1}}, "withdraw": {N1.hex(): ["ab"]}})):
            with self.subTest(value=value):
                state = os.path.join(self.tmp.name, f"legacy {n}")  # each its own: a file is imported once
                path = write_legacy(state, "published.json", value)
                publisher = self.make_publisher(state=state)
                publisher.refresh()
                self.assertEqual(publisher.next_due(), 0)  # nothing loaded: due
                self.assertFalse(os.path.exists(path))
                for table in ("published", "published_earlier", "withdrawals"):
                    self.assertEqual(state_rows(state, f"SELECT * FROM {table}"), [])
                publisher.close()

    def test_malformed_rows_are_skipped_and_deleted(self):
        """Rows that don't parse are skipped, the rest loaded; the load deletes those whose item doesn't parse,
        and the next save the others, so the tables hold what memory does."""
        a, b = self.pinned(b"a"), self.pinned(b"b")
        self.publisher.refresh()
        self.publisher.publish_due()
        self.publisher.close()
        rows = [("INSERT INTO published (item, at, nodes) VALUES (?, ?, ?)", ("not an item", 1, N1)),
                ("INSERT OR REPLACE INTO published (item, at, nodes) VALUES (?, ?, ?)", (str(b), "yesterday", N1)),
                ("INSERT INTO published (item, at, nodes) VALUES (?, ?, ?)", ("key:abcd", 1, N1)),  # 2 bytes
                ("INSERT INTO published_earlier (item, node, at) VALUES (?, ?, ?)", (str(a), b"short", 1)),
                ("INSERT INTO published_earlier (item, node, at) VALUES (?, ?, ?)", (str(a), N3, "x")),
                ("INSERT INTO published_earlier (item, node, at) VALUES (?, ?, ?)", ("bad", N3, 1)),
                ("INSERT INTO withdrawals (node, key) VALUES (?, ?)", (b"short", a.key)),
                ("INSERT INTO withdrawals (node, key) VALUES (?, ?)", (N3, "text"))]
        for sql, row in rows:
            state_execute(self.state, sql, row)
        publisher = self.make_publisher()
        publisher.refresh()
        self.assertEqual(set(publisher._published), {a})  # b's row was replaced by the malformed one
        self.assertEqual(publisher._withdrawals, {})
        self.assertEqual(self.saved()["published"].keys(), {str(a), str(b)})  # parsed items wait for the save
        publisher.save()
        self.assertEqual(self.saved(), self.held(publisher))

    def test_times_clamped_and_earlier_nodes_expired_at_load_are_saved(self):
        """A last-published time in the future loads as now, and earlier nodes past PROVIDER_TTL are dropped;
        the next save writes both."""
        cid = self.pinned(b"ahead")
        self.publisher.refresh()
        self.clock.now += 10**6
        self.publisher.publish_due()
        self.nodes = [N3]
        self.clock.now += 1000
        self.publisher.publish_due()  # N1, N2 earlier now
        self.clock.now -= 10**6
        restarted = self.make_publisher()
        self.assertEqual(restarted._published[cid][0], self.clock.now)
        restarted.save()
        self.assertEqual(self.saved()["published"], {str(cid): (self.clock.now, N3)})
        self.clock.now += 10**6 + PROVIDER_TTL
        restarted = self.make_publisher()
        self.assertEqual(restarted._earlier, {})
        restarted.save()
        self.assertEqual(self.saved()["earlier"], {})

    def test_provided_again_cancels_only_its_queued_withdrawals(self):
        """A CID provided (again) cancels the withdrawals queued for its key at every node, whose nodes join its
        earlier ones; the other keys' stay queued, and a CID with none queued cancels nothing."""
        a, b = self.pinned(b"a"), self.pinned(b"b")
        other = bytes([7]) * 16
        with self.publisher._lock:  # as if queued earlier, and saved
            self.publisher._withdrawals = {N1: {a.key, other}, N2: {a.key}, N3: {other}}
            for node in (N1, N2, N3):
                self.publisher._touch_node(node)
        self.publisher.save()
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {a, b})
        self.assertEqual(self.publisher._withdrawals, {N1: {other}, N3: {other}})
        self.assertEqual(set(self.publisher._earlier[a]), {N1, N2})
        self.assertNotIn(b, self.publisher._earlier)
        self.assertEqual(self.saved()["withdraw"], {N1: {other}, N3: {other}})
        self.assertEqual(self.records.providers(a.key), [OWN])
        self.assertEqual(self.records.providers(b.key), [OWN])  # recorded locally, nothing to cancel

    def test_served_blocks_are_the_provided_dags(self):
        cid = self.pinned(os.urandom(200_000))
        self.publisher.refresh()
        for block in self.storage.dag_blocks(cid):
            self.assertTrue(self.publisher.serves(block))
        self.assertFalse(self.publisher.serves(Cid.for_block(RAW, b"other")))

    def test_refresh_builds_the_sets_again_only_when_they_change(self):
        """Each refresh provides and serves exactly the complete candidates' DAGs and the sources' keys; when
        none of them changed, the same served set is kept rather than built again."""
        keys: set[bytes] = set()
        self.publisher.add_key_source(lambda: keys)
        self.eligible = set()

        def check():
            """The publisher's sets are those of storage and the key source now."""
            self.publisher.refresh()
            candidates = set(self.storage.pins()) | (self.storage.cached_targets() & self.eligible)
            dags = {cid: self.storage.dag_blocks(cid) for cid in candidates if self.storage.is_complete(cid)}
            self.assertEqual(self.publisher.provided(), set(dags))
            self.assertEqual(self.publisher.provided_keys(), keys)
            self.assertEqual(self.publisher._served, frozenset().union(*dags.values()))
            return self.publisher._served

        a = self.pinned(os.urandom(100_000))
        served = check()
        self.assertIs(check(), served)  # nothing changed
        b = self.pinned(os.urandom(100_000))  # added
        self.assertIsNot(check(), served)
        keys.add(bytes([9]) * 16)
        served = check()
        self.assertIn(DhtKey(bytes([9]) * 16), self.publisher._provided)
        self.assertIs(check(), served)
        # A pin whose DAG is missing a block isn't provided until the block is stored
        blocks: dict[Cid, bytes] = {}
        partial = builder.build_bytes(os.urandom(100_000), lambda cid, block: blocks.setdefault(cid, block)).cid
        missing = next(cid for cid in blocks if cid.is_raw)
        for cid, block in blocks.items():
            if cid != missing:
                self.storage.put(cid, block)
        self.storage.pin(partial, USER)
        self.assertIs(check(), served)
        self.storage.put(missing, blocks[missing])
        self.assertIsNot(check(), served)
        self.storage.unpin(a, USER)  # gone
        served = check()
        self.assertFalse(self.publisher.serves(a))
        # A cached target the roots strategy provides, until it is evicted
        self.config.provide_strategy = "roots"
        fetched = builder.build_bytes(b"fetched", self.storage.put)
        self.storage.record_fetch(fetched.cid, [], fetched.cid)
        self.eligible.add(fetched.cid)
        self.assertIsNot(check(), served)
        self.storage.gc()
        check()
        keys.clear()
        check()
        self.assertEqual(self.publisher.provided(), {b, partial})

    def test_refresh_withdraws_what_isnt_provided_when_the_sets_are_unchanged(self):
        """State loaded at startup is withdrawn even when the provided set is the same as before (empty)."""
        cid = self.pinned(b"once provided")
        self.publisher.refresh()
        self.publisher.publish_due()
        self.storage.unpin(cid, USER)
        restarted = self.make_publisher()
        restarted.refresh()
        self.assertEqual(restarted.provided(), set())
        restarted.publish_due()
        self.assertEqual(self.withdrawn(), {N1: {cid.key}, N2: {cid.key}})

    def test_saved_without_the_lock_and_in_order(self):
        """The publish state is written outside the lock, and a save that snapshots meanwhile lands last."""
        a, b = self.pinned(b"a"), self.pinned(b"b")
        self.publisher.refresh()
        free, second = [], []

        def unpin_and_save():
            """Before the first save writes: another thread takes the lock, then unpins b and saves."""
            if not second:
                lock = self.publisher._lock
                thread = threading.Thread(target=lambda: free.append(lock.acquire(timeout=5)) or lock.release())
                thread.start()
                thread.join()
                self.storage.unpin(b, USER)
                second.append(threading.Thread(target=self.publisher.refresh))
                second[0].start()
                second[0].join(0.2)

        with recorded_writes(self.publisher._db, before=unpin_and_save):
            self.publisher.publish_due()
            second[0].join(5)
        self.assertEqual(free, [True])
        saved = self.saved()
        self.assertEqual(set(saved["published"]), {str(a)})
        self.assertEqual(saved["withdraw"], {N1: {b.key}, N2: {b.key}})

    def test_save_writes_only_what_changed(self):
        """With many CIDs published, unpinning one writes its rows and its nodes' withdrawals only."""
        cids = [self.pinned(b"cid %d" % i) for i in range(50)]
        self.publisher.refresh()
        self.publisher.publish_due()
        self.assertEqual(self.saved(), self.held(self.publisher))
        with recorded_writes(self.publisher._db) as transactions:
            self.storage.unpin(cids[0], USER)
            self.publisher.refresh()
        self.assertEqual(len(transactions), 1)
        # Its published row and earlier rows deleted (one item: 2), N1's and N2's withdrawals deleted and written (4)
        self.assertEqual(rows_written(transactions), 6)
        self.assertEqual(self.saved(), self.held(self.publisher))
        with recorded_writes(self.publisher._db) as transactions:
            self.publisher.save()
        self.assertEqual(transactions, [])  # nothing changed since

    # Republish lookups run at once

    def due_keys(self, count: int) -> list[Cid]:
        """count complete pins, refreshed into the provided set: all due."""
        cids = [self.pinned(b"due %d" % i) for i in range(count)]
        self.publisher.refresh()
        return cids

    def test_lookups_run_at_once(self):
        """A round's lookups run PUBLISH_LOOKUPS at once, on threads of their own, so a round of many keys whose
        lookups take time finishes in a fraction of the time one at a time takes (the peak count shows it). Each key
        is a region of its own here (its K closest share 120 bits with it); the first lookup runs alone, as the
        regions' size isn't known before it."""
        self.due_keys(20)
        self.nodes_of = lambda key: near(key) + [N1, N2]
        self.lookup_delay = lambda key: 0.1  # 2 s one at a time
        result = self.publisher.publish_due()
        self.assertEqual(len(result["published"]), 20)
        self.assertEqual(result["lookups"], 20)
        self.assertEqual(self.running.peak, PUBLISH_LOOKUPS)
        self.assertEqual(self.running.threads, {"rnfs-publish-lookup"})
        shared = [keys for node_id, keys, _ in self.client.calls if node_id in (N1, N2)]
        self.assertEqual(len(shared), 2)  # still one /provide per node, every key in it
        self.assertTrue(all(len(keys) == 20 for keys in shared))

    def test_lookups_at_once_send_what_one_at_a_time_sends(self):
        """Lookups finishing in any order, each finding its own nodes: every node gets the same /provide
        requests, keys and withdrawals in the same order, as when the lookups run one at a time."""
        cids = self.due_keys(20)
        self.nodes_of = lambda key: near(key) + ([node for i, node in enumerate((N1, N2, N3)) if key[i] % 2] or [N1])
        self.lookup_delay = lambda key: 0.005 + key[3] % 5 / 200  # 5-25 ms, so they finish out of order
        gone = bytes([7]) * 16
        rounds = []
        for lookups in (PUBLISH_LOOKUPS, 1):
            with self.subTest(lookups=lookups), mock.patch.object(publisher_module, "PUBLISH_LOOKUPS", lookups):
                self.client = FakeClient()
                self.running = Running()
                publisher = self.make_publisher(state=os.path.join(self.tmp.name, f"state {lookups}"))
                publisher._withdrawals = {N1: {gone}, N3: {gone}}  # as if queued earlier
                publisher.refresh()
                result = publisher.publish_due()
                self.assertEqual(set(result["published"]), set(cids))
                if lookups == 1:
                    self.assertEqual(self.running.peak, 1)
                else:
                    self.assertGreater(self.running.peak, 1)
                rounds.append((result, self.client.calls))
        self.assertEqual(rounds[0], rounds[1])

    def test_slow_nodes_look_up_one_at_a_time(self):
        """Slow nodes keep their pacing: one lookup at a time, on the publishing thread, at most
        SLOW_ROUND_LOOKUPS a round, and SLOW_LOOKUP_PACE per lookup before the next round."""
        publisher = self.make_publisher(slow=True)
        self.due_keys(20)
        publisher.refresh()
        self.nodes_of = lambda key: near(key) + [N1, N2]  # each key a region of its own
        self.lookup_delay = lambda key: 0.01
        self.assertEqual(len(publisher.publish_due()["published"]), SLOW_ROUND_LOOKUPS)
        self.assertEqual(self.running.peak, 1)
        self.assertEqual(self.running.threads, {threading.current_thread().name})
        self.assertEqual(publisher.next_due(), SLOW_LOOKUP_PACE * SLOW_ROUND_LOOKUPS)
        self.assertEqual(publisher.publish_due()["published"], [])
        self.clock.now += SLOW_LOOKUP_PACE * SLOW_ROUND_LOOKUPS
        self.assertEqual(len(publisher.publish_due()["published"]), SLOW_ROUND_LOOKUPS)

    def queue_withdrawals(self, publisher: Publisher, servers: list[bytes], keys: int = 1):
        """Queue withdrawals of keys keys at each of servers, as if their CIDs had been unpinned, and saved."""
        with publisher._lock:
            for n, node_id in enumerate(servers):
                publisher._withdrawals[node_id] = {bytes([n + 1, k]) * 8 for k in range(keys)}
                publisher._touch_node(node_id)
        publisher.save()

    def test_slow_nodes_pace_withdrawals(self):
        """On a slow node each server only withdrawals reach costs a lookup's worth: SLOW_ROUND_LOOKUPS servers a
        round, those with the most keys queued first, SLOW_LOOKUP_PACE each before the next round."""
        publisher = self.make_publisher(slow=True)
        servers = [bytes([0x80, n]) * 8 for n in range(25)]
        self.queue_withdrawals(publisher, servers)
        with publisher._lock:
            publisher._withdrawals[servers[24]] |= {bytes([99]) * 16}  # two keys: first
        self.assertEqual(publisher.next_due(), 0)
        result = publisher.publish_due()
        self.assertEqual(result["lookups"], 0)
        self.assertEqual(result["nodes"], sorted([servers[24]] + servers[:SLOW_ROUND_LOOKUPS - 1]))
        self.assertEqual(publisher.next_due(), SLOW_LOOKUP_PACE * SLOW_ROUND_LOOKUPS)
        self.assertEqual(publisher.publish_due()["nodes"], [])  # paced: nothing sent
        self.clock.now += SLOW_LOOKUP_PACE * SLOW_ROUND_LOOKUPS
        self.assertEqual(len(publisher.publish_due()["nodes"]), SLOW_ROUND_LOOKUPS)
        self.clock.now += SLOW_LOOKUP_PACE * SLOW_ROUND_LOOKUPS
        self.assertEqual(len(publisher.publish_due()["nodes"]), 5)
        self.assertEqual(publisher._next_lookup, self.clock.now + SLOW_LOOKUP_PACE * 5)  # paced by the 5 sent
        self.assertEqual(publisher.next_due(), MAX_WAIT)  # nothing left
        self.assertEqual(self.saved()["withdraw"], {})
        self.assertEqual({node_id for node_id, _, _ in self.client.calls}, set(servers))

    def test_slow_nodes_keep_room_for_withdrawals(self):
        """While withdrawal-only servers are queued, up to half a slow round is kept for them; withdrawals to servers
        the round stores to anyway cost nothing more."""
        publisher = self.make_publisher(slow=True)
        self.due_keys(20)
        publisher.refresh()
        self.nodes_of = lambda key: near(key) + [N1]  # each key a region of its own
        alone = [bytes([0x80, n]) * 8 for n in range(3)]
        self.queue_withdrawals(publisher, alone + [N1])
        result = publisher.publish_due()
        self.assertEqual(result["lookups"], SLOW_ROUND_LOOKUPS - 4)  # 4 queued, at most 5 kept
        self.assertEqual(len(result["published"]), SLOW_ROUND_LOOKUPS - 4)
        sent = {node_id: withdrawn for node_id, _, withdrawn in self.client.calls if withdrawn}
        self.assertEqual(set(sent), set(alone) | {N1})
        self.assertEqual(publisher.next_due(), SLOW_LOOKUP_PACE * (SLOW_ROUND_LOOKUPS - 4 + 3))
        # Many queued: still half the round for lookups
        self.queue_withdrawals(publisher, [bytes([0x90, n]) * 8 for n in range(30)])
        self.clock.now += SLOW_LOOKUP_PACE * SLOW_ROUND_LOOKUPS
        result = publisher.publish_due()
        self.assertEqual(result["lookups"], SLOW_ROUND_LOOKUPS // 2)
        self.assertEqual(len(result["nodes"]) - len(set(result["nodes"]) & {N1}),
                         (SLOW_ROUND_LOOKUPS // 2) * K + SLOW_ROUND_LOOKUPS // 2)

    def test_fast_nodes_send_withdrawals_at_once(self):
        servers = [bytes([0x80, n]) * 8 for n in range(25)]
        self.queue_withdrawals(self.publisher, servers)
        self.assertEqual(self.publisher.publish_due()["nodes"], sorted(servers))
        self.assertEqual(self.publisher.next_due(), MAX_WAIT)

    def test_stopped_round_keeps_only_finished_lookups(self):
        """should_stop is checked before each lookup starts: those started finish and are published, in due
        order, and the rest stay due."""
        self.due_keys(20)
        self.nodes_of = lambda key: near(key) + [N1, N2]  # each key a region of its own
        self.lookup_delay = lambda key: 0.02
        # Only the lookup threads are told to stop, so the round still sends what was looked up
        result = self.publisher.publish_due(
            should_stop=lambda: len(self.lookups) >= 6 and threading.current_thread().name == "rnfs-publish-lookup")
        self.assertGreaterEqual(len(self.lookups), 6)
        self.assertLess(len(self.lookups), 6 + PUBLISH_LOOKUPS)
        self.assertEqual(self.running.finished, len(self.lookups))
        self.assertEqual({cid.key for cid in result["published"]}, set(self.lookups))
        self.assertEqual(sorted(result["published"], key=lambda cid: cid.key), result["published"])
        for node_id, keys, _ in self.client.calls:
            if node_id in (N1, N2):
                self.assertEqual(keys, [cid.key for cid in result["published"]])
        self.assertEqual(self.publisher.next_due(), 0)  # the others are still due

    def test_failed_lookup_ends_the_round_once_the_others_finish(self):
        """A lookup that raises starts no more; publish_due raises it once those running have finished, having
        sent nothing, and everything stays due."""
        cids = self.due_keys(20)
        first = min(cid.key for cid in cids)  # due first: never published, then by key
        self.lookup_delay = lambda key: 0.05
        finished = []

        def fail(key):
            """The first key's lookup raises."""
            if key == first:
                raise RuntimeError("lookup failed")

        self.on_lookup = fail
        with self.assertRaises(RuntimeError):
            try:
                self.publisher.publish_due()
            finally:
                finished.append(self.running.finished)
        self.assertLessEqual(len(self.lookups), PUBLISH_LOOKUPS)
        self.assertEqual(finished, [len(self.lookups) - 1])  # every other lookup started had finished
        self.assertEqual(self.client.calls, [])
        self.assertEqual(self.publisher.next_due(), 0)

    # /provide sent to many servers at once

    def test_provides_sent_at_once(self):
        """Other nodes send /provide to up to PROVIDE_PARALLEL servers at once; each server still gets one call
        holding all its keys and the withdrawals queued for it, and the round sends and records exactly what one
        server at a time does."""
        cids = self.due_keys(5)
        self.nodes_of = lambda key: near(key) + [N1]  # 5 regions, 51 servers
        servers = sorted({node for cid in cids for node in near(cid.key)} | {N1})
        gone = bytes([7]) * 16
        rounds = []
        for parallel in (PROVIDE_PARALLEL, 1):
            with self.subTest(parallel=parallel), mock.patch.object(publisher_module, "PROVIDE_PARALLEL", parallel):
                self.client = SlowClient(0.02)
                publisher = self.make_publisher(state=os.path.join(self.tmp.name, f"state {parallel}"))
                publisher._withdrawals = {N1: {gone}, servers[3]: {gone}, N3: {gone}}  # as if queued earlier
                publisher.refresh()
                result = publisher.publish_due()
                self.assertEqual(self.client.peak, parallel)
                self.assertEqual(sorted(node for node, _, _ in self.client.calls), sorted(servers + [N3]))
                calls = {node: (keys, withdrawn) for node, keys, withdrawn in self.client.calls}
                self.assertEqual(calls[N1], ([cid.key for cid in result["published"]], [gone]))
                self.assertEqual(calls[N3], ([], [gone]))
                self.assertEqual(calls[servers[3]][1], [gone])
                rounds.append((result, sorted(self.client.calls), self.held(publisher)))
        self.assertEqual(rounds[0], rounds[1])

    def test_slow_nodes_send_one_at_a_time(self):
        publisher = self.make_publisher(slow=True)
        self.due_keys(3)
        publisher.refresh()
        self.nodes_of = lambda key: near(key)
        self.client = SlowClient(0.01)
        publisher.client = self.client
        publisher.publish_due()
        self.assertEqual(self.client.peak, 1)
        self.assertEqual(len(self.client.calls), 3 * K)

    def test_stopped_while_sending_in_parallel(self):
        """A stop lets the requests in flight finish and starts no more; keys some of whose servers weren't sent to
        stay due, and are withdrawn from those that recorded them once unpinned."""
        cids = self.due_keys(4)
        self.nodes_of = lambda key: near(key)
        stop = threading.Event()
        self.client = GatedClient()
        publisher = self.make_publisher()
        publisher.refresh()
        thread = threading.Thread(target=lambda: self.client.results.append(publisher.publish_due(stop.is_set)))
        thread.start()
        self.assertTrue(self.client.waiting.wait(5))
        stop.set()
        self.client.gate.set()
        thread.join(5)
        (result,) = self.client.results
        sent = {node for node, _, _ in self.client.calls}
        self.assertEqual(len(sent), PROVIDE_PARALLEL)  # those in flight when stopped, and no more
        complete = [cid for cid in cids if set(near(cid.key)) <= sent]
        self.assertEqual(result["published"], complete)
        self.assertEqual(publisher.next_due(), 0)  # the others are still due
        for cid in cids:
            self.storage.unpin(cid, USER)
        publisher.refresh()
        self.client = FakeClient()
        publisher.client = self.client
        publisher.publish_due()
        self.assertEqual(set(self.withdrawn()), sent)

    def test_crash_while_sending_in_parallel(self):
        """A round crashing part way through parallel sends: once restarted, every node a save listed is withdrawn
        from when the CIDs stop being provided, at least SAVE_EVERY_NODES of those that stored, and none other."""
        cids = self.due_keys(3)
        self.nodes_of = lambda key: near(key)
        self.client = CrashingClient(crash_at=SAVE_EVERY_NODES + 5)
        publisher = self.make_publisher()
        publisher.refresh()
        with self.assertRaises(Crash):
            publisher.publish_due()
        stored = {node for node, _, _ in self.client.calls}
        self.assertEqual(len(stored), SAVE_EVERY_NODES + 5)
        self.client = FakeClient()
        restarted = self.make_publisher()
        restarted.refresh()
        for cid in cids:
            self.storage.unpin(cid, USER)
        restarted.refresh()
        restarted.publish_due()
        withdrawn = set(self.withdrawn())
        self.assertLessEqual(withdrawn, stored)
        self.assertGreaterEqual(len(withdrawn), SAVE_EVERY_NODES)

    # Refreshes that change nothing

    def counting(self) -> dict[str, int]:
        """Count the calls of storage.pinned_cids and dag_blocks from now until the test ends: {name: calls}."""
        counts = {"pinned_cids": 0, "dag_blocks": 0}
        for name in counts:
            original = getattr(self.storage, name)

            def counted(*args, name=name, original=original):
                """Count the call, then make it."""
                counts[name] += 1
                return original(*args)

            patcher = mock.patch.object(self.storage, name, counted)
            patcher.start()
            self.addCleanup(patcher.stop)
        return counts

    def test_refresh_skipped_when_nothing_changed(self):
        """With a generation, a refresh after which nothing it is computed from changed reads no pins or DAGs
        and leaves the sets as they were; without one, every refresh computes."""
        self.publisher = self.make_publisher(generation=True)
        keys = {bytes([9]) * 16}
        self.publisher.add_key_source(lambda: keys)
        cid = self.pinned(os.urandom(100_000))
        self.publisher.refresh()
        provided, served = self.publisher._provided, self.publisher._served
        counts = self.counting()
        for _ in range(3):
            self.publisher.refresh()
        self.assertEqual(counts, {"pinned_cids": 0, "dag_blocks": 0})
        self.assertIs(self.publisher._provided, provided)
        self.assertIs(self.publisher._served, served)
        self.assertEqual(self.publisher.provided(), {cid})
        self.assertEqual(self.publisher.provided_keys(), keys)
        publisher = self.make_publisher()
        publisher.refresh()
        counts["pinned_cids"] = 0
        publisher.refresh()
        self.assertEqual(counts["pinned_cids"], 1)

    def test_refresh_computes_after_each_kind_of_change(self):
        """With a generation, each change a refresh is computed from makes the next refresh compute, and what
        stops being provided is withdrawn at once."""
        self.publisher = self.make_publisher(generation=True)
        self.config.provide_strategy = "roots"
        deny = DenyList(self.state)
        self.addCleanup(deny.close)
        self.storage.deny = deny
        keys: set[bytes] = set()
        self.publisher.add_key_source(lambda: keys)
        self.publisher.refresh()
        counts = self.counting()

        def refresh(provided: set, withdrawn: Cid | bytes | None = None):
            """Refresh: it computes, provides provided (CIDs and keys) and has queued, at once, the withdrawal
            of withdrawn (a CID or key published to N1 and N2). Then publish, and a refresh computes nothing."""
            counts["pinned_cids"] = 0
            self.publisher.refresh()
            self.assertEqual(counts["pinned_cids"], 1)
            self.assertEqual(self.publisher.provided() | self.publisher.provided_keys(), provided)
            if withdrawn is not None:
                key = withdrawn if isinstance(withdrawn, bytes) else withdrawn.key
                self.assertEqual(self.records.providers(key), [])
                self.assertEqual(self.held(self.publisher)["withdraw"], {N1: {key}, N2: {key}})
            self.publisher.publish_due()  # also published, so a later withdrawal has nodes to go to
            self.assertEqual(self.held(self.publisher)["withdraw"], {})
            counts["pinned_cids"] = 0
            self.publisher.refresh()
            self.assertEqual(counts["pinned_cids"], 0)

        with self.subTest("pin"):
            a = self.pinned(b"a")
            refresh({a})
        with self.subTest("unpin"):
            b = self.pinned(b"b")
            refresh({a, b})
            self.storage.unpin(b, USER)
            refresh({a}, b)
        fetched = builder.build_bytes(b"fetched", self.storage.put).cid
        self.eligible.add(fetched)
        self.publisher.refresh()
        with self.subTest("cache record added"):
            self.storage.record_fetch(fetched, [], fetched)
            refresh({a, fetched})
        with self.subTest("cache record dropped"):
            self.storage.gc()
            refresh({a}, fetched)
        with self.subTest("fetched targets changed"):
            builder.build_bytes(b"fetched", self.storage.put)
            self.storage.record_fetch(fetched, [], fetched)
            self.eligible.clear()
            refresh({a})
            self.eligible.add(fetched)
            self.fetched_changes += 1
            refresh({a, fetched})
            self.eligible.clear()
            self.fetched_changes += 1
            refresh({a}, fetched)
        big = builder.build_bytes(os.urandom(100_000), self.storage.put).cid
        self.storage.pin(big, USER)
        refresh({a, big})
        damaged = next(cid for cid in self.storage.dag_blocks(big) if cid.is_raw)
        block = self.storage.get(damaged)
        with self.subTest("block deleted"):
            with open(self.storage.blocks.path(damaged), "wb") as f:
                f.write(b"damaged")
            self.assertEqual(self.storage.verify()["bad"], [damaged])
            refresh({a}, big)
        with self.subTest("block stored"):
            self.storage.put(damaged, block)
            refresh({a, big})
        with self.subTest("deny list"):
            deny.add([str(damaged)])
            refresh({a}, big)
            deny.remove([str(damaged)])
            refresh({a, big})
        key = bytes([9]) * 16
        with self.subTest("key source's keys"):
            keys.add(key)
            refresh({a, big, key})
            keys.clear()
            refresh({a, big}, key)
        with self.subTest("new key source"):
            keys.add(key)
            refresh({a, big, key})
            self.publisher.add_key_source(lambda: {key})  # the same key: computed again all the same
            refresh({a, big, key})

    def test_change_made_while_refreshing_is_seen_by_the_next(self):
        """The generation is read before computing: a pin made after the compute read the pins is provided by
        the next refresh, though nothing changed after that."""
        self.publisher = self.make_publisher(generation=True)
        a = self.pinned(b"a")
        built = builder.build_bytes(b"pinned meanwhile", self.storage.put)
        pinned_cids = self.storage.pinned_cids

        def pins_then_pin():
            """The pinned CIDs as they were, then (once) another pin."""
            result = pinned_cids()
            if not self.storage.is_pinned(built.cid):
                self.storage.pin(built.cid, USER)
            return result

        with mock.patch.object(self.storage, "pinned_cids", pins_then_pin):
            self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {a})
        self.publisher.refresh()
        self.assertEqual(self.publisher.provided(), {a, built.cid})

    def test_first_refresh_with_a_generation_withdraws_loaded_state(self):
        """The first refresh after a restart computes, so what was published but is no longer provided is
        withdrawn though the provided set is the same as before (empty)."""
        cid = self.pinned(b"once provided")
        self.publisher.refresh()
        self.publisher.publish_due()
        self.storage.unpin(cid, USER)
        restarted = self.make_publisher(generation=True)
        restarted.refresh()
        self.assertEqual(self.held(restarted)["withdraw"], {N1: {cid.key}, N2: {cid.key}})

    def test_failed_save_is_written_next_time(self):
        """A round whose save fails raises (the loop logs it); the next save writes what it held."""
        cid = self.pinned(b"failing disk")
        self.publisher.refresh()
        with failing_commits():
            with self.assertRaises(OSError):
                self.publisher.publish_due()
        self.assertEqual(self.saved()["published"], {})
        self.publisher.save()
        self.assertEqual(set(self.saved()["published"]), {str(cid)})
        restarted = self.make_publisher()
        restarted.refresh()
        self.assertEqual(restarted.publish_due()["published"], [])

    def test_saves_are_one_transaction_not_durable(self):
        """A save writes published, published_earlier and withdrawals in one transaction, not durably (NORMAL)."""
        cid = self.pinned(b"normal")
        self.publisher.refresh()
        self.publisher.publish_due()
        self.storage.unpin(cid, USER)
        real = self.publisher._db.transaction
        with mock.patch.object(self.publisher._db, "transaction", side_effect=real) as opened:
            self.publisher.refresh()  # the item's rows deleted and its withdrawals queued, in one save
        self.assertEqual([call.args for call in opened.call_args_list], [(False,)])
        self.assertEqual(self.saved(), self.held(self.publisher))


class ParallelSendsTest(unittest.TestCase):
    """The real PeerClient, over fake RNS links, sending a round's /provide to many servers at once: each server
    gets its batches in order over its own identified link, withdrawals first, and one refusing stops only its own."""

    def setUp(self):
        """A publisher whose PeerClient's links are FakeLinks recording which server they go to."""
        from rnfs.network import client as client_module
        from rnfs.network import links
        rns = fake_links.fake_rns()
        rns.Link = RecordingLink
        patcher = mock.patch.object(links, "RNS", rns)
        patcher.start()
        self.addCleanup(patcher.stop)
        fake_links.FakeLink.created = []
        fake_links.FakeLink.establish_delay = 0.05  # so links overlap
        self.addCleanup(setattr, fake_links.FakeLink, "establish_delay", 0.0)
        RecordingLink.open_now = RecordingLink.peak = 0
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = Config(rnfs_dir=self.tmp.name)
        self.storage = Storage(os.path.join(self.tmp.name, "blocks"), os.path.join(self.tmp.name, "state"), 10**9)
        self.addCleanup(self.storage.close)
        pool = links.LinkPool(object(), self.config)
        self.addCleanup(pool.close_all)
        self.client = client_module.PeerClient(pool, self.config)
        self.servers = [bytes([0x40, n]) * 8 for n in range(K)]
        self.publisher = Publisher(OWN, self.config, self.storage, ProviderStore(None), self.client,
                                   lambda key: FakeLookup(self.servers), clock=FakeClock(1_000_000.0))

    def test_batches_in_order_withdrawals_first_and_stop_at_a_refusal(self):
        from rnfs.network import protocol
        keys = sorted(bytes([0x41, n]) * 8 for n in range(60))  # every server gets every key: 3 batches or more
        self.publisher.add_key_source(lambda: set(keys))
        gone = [bytes([0x42, n]) * 8 for n in range(5)]
        refusing = self.servers[4]
        self.publisher._withdrawals = {server: set(gone) for server in self.servers}
        self.publisher.refresh()
        first = {}

        def respond(link, path, data):
            """Accept every batch but the refusing server's after its first."""
            first.setdefault(link.node_id, data)
            return link.node_id != refusing or data == first[link.node_id]

        fake_links.FakeLink.responder = staticmethod(respond)
        result = fake_links.run(self, self.publisher.publish_due, timeout=20)
        self.assertGreater(RecordingLink.peak, 1)  # links to several servers open at once
        by_server = {}
        for link in fake_links.FakeLink.created:
            by_server.setdefault(link.node_id, []).append(link)
        self.assertEqual(set(by_server), set(self.servers))
        expected = protocol.provide_batches(keys, sorted(gone), 431)  # the fake links' MDU
        self.assertGreaterEqual(len(expected), 3)
        self.assertEqual(expected[0][1], sorted(gone))  # withdrawals ride first
        for server, made in by_server.items():
            self.assertEqual(len(made), 1)  # one identified link each
            sent = [data for _, data, _ in made[0].requests]
            if server == refusing:
                # The first batch, then the second, refused, and its one retry: nothing after
                self.assertEqual(sent, [list(expected[0]), list(expected[1]), list(expected[1])])
            else:
                self.assertEqual(sent, [list(batch) for batch in expected])
        self.assertEqual(len(result["published"]), len(keys))
        # The refusing server may hold the keys of the batches it was sent, the refused one included (withdrawn
        # from later), not those after it
        maybe_held = set(expected[0][0]) | set(expected[1][0])
        for item, (_, nodes) in self.publisher._published.items():
            self.assertEqual(set(nodes), set(self.servers) - ({refusing} if item.key not in maybe_held else set()))


class RecordingLink(fake_links.FakeLink):
    """A FakeLink that knows the server it goes to, and counts the links open at once (the most in peak)."""

    open_now = peak = 0

    def __init__(self, destination, established_callback, closed_callback):
        """A link to destination's server."""
        self.node_id = destination.hash
        with _LINKS_LOCK:
            RecordingLink.open_now += 1
            RecordingLink.peak = max(RecordingLink.peak, RecordingLink.open_now)
        super().__init__(destination, established_callback, closed_callback)

    def teardown(self):
        """Close it (counted once)."""
        if self.status != fake_links.FakeLink.CLOSED:
            with _LINKS_LOCK:
                RecordingLink.open_now -= 1
        super().teardown()


_LINKS_LOCK = threading.Lock()


if __name__ == "__main__":
    unittest.main()
