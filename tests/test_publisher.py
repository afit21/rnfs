"""Tests for the provide strategy, batched publishing, republishing and withdrawal (rnfs/publisher.py)."""
import json
import os
import tempfile
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import PROVIDER_TTL
from rnfs.content import builder
from rnfs.content.cid import RAW, Cid
from rnfs.content.storage import USER, Storage
from rnfs.dht.providers import ProviderStore
from rnfs.publisher import SLOW_LOOKUP_PACE, SLOW_ROUND_LOOKUPS, STATE_FILE, Publisher

OWN = bytes(16)
N1, N2, N3 = (bytes([i]) * 16 for i in (1, 2, 3))

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the publisher's log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class FakeClient:
    """Records /provide requests; nodes in refuse record nothing, a node in limit records only its first keys."""

    def __init__(self):
        """No requests yet."""
        self.calls: list[tuple[bytes, list[bytes], list[bytes]]] = []
        self.refuse: set[bytes] = set()
        self.limit: dict[bytes, int] = {}

    def provide(self, node_id, keys, withdrawn):
        """Record one batched /provide; the keys the node may have recorded, as PeerClient.provide returns."""
        self.calls.append((node_id, list(keys), list(withdrawn)))
        if node_id in self.refuse:
            return set()
        return set(list(keys)[:self.limit.get(node_id, len(keys))])


class FakeLookup:
    """find_closest returns fixed nodes."""

    def __init__(self, nodes):
        """A lookup answering nodes."""
        self.nodes = nodes

    def find_closest(self):
        """The fixed nodes."""
        return list(self.nodes)


class Clock:
    """A settable wall clock."""

    def __init__(self):
        """Start at an arbitrary time."""
        self.now = 1_000_000.0

    def __call__(self):
        """The current time."""
        return self.now


class PublisherTest(unittest.TestCase):
    """A Publisher over temporary storage with fakes for the network."""

    def setUp(self):
        """Storage, an in-memory provider store, and a publisher whose lookups find self.nodes."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = os.path.join(self.tmp.name, "state")
        self.config = Config(rnfs_dir=self.tmp.name, republish_interval=1000)
        self.storage = Storage(os.path.join(self.tmp.name, "blocks"), self.state, 10**9)
        self.records = ProviderStore(1000, None)
        self.client = FakeClient()
        self.clock = Clock()
        self.nodes = [N1, N2]
        self.eligible: set[Cid] = set()
        self.lookups: list[bytes] = []
        self.on_lookup = lambda key: None
        self.publisher = self.make_publisher()

    def make_publisher(self, slow: bool = False) -> Publisher:
        """A publisher sharing this test's state directory."""
        def make_lookup(key):
            """Remember the key; find self.nodes."""
            self.lookups.append(key)
            self.on_lookup(key)
            return FakeLookup(self.nodes)
        return Publisher(OWN, self.config, self.storage, self.records, self.client, make_lookup, slow, self.state,
                         cached_eligible=lambda: self.eligible, clock=self.clock, retry_interval=30)

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

    def test_state_of_the_wrong_shape_is_ignored(self):
        cid = self.pinned(b"shape")
        for value in ([], {"published": []}, {"published": {str(cid): 5}, "withdraw": []},
                      {"published": {str(cid): [10**400, []]}}, {"published": {str(cid): [1, "0101"]}},
                      {"earlier": {str(cid): []}, "withdraw": {N1.hex(): "ab"}}):
            with self.subTest(value=value):
                with open(os.path.join(self.state, STATE_FILE), "w", encoding="utf-8") as f:
                    json.dump(value, f)
                publisher = self.make_publisher()
                publisher.refresh()
                self.assertEqual(publisher.next_due(), 0)  # nothing loaded: due

    def test_served_blocks_are_the_provided_dags(self):
        cid = self.pinned(os.urandom(200_000))
        self.publisher.refresh()
        for block in self.storage.dag_blocks(cid):
            self.assertTrue(self.publisher.serves(block))
        self.assertFalse(self.publisher.serves(Cid.for_block(RAW, b"other")))


if __name__ == "__main__":
    unittest.main()
