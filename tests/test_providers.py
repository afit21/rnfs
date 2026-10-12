"""Tests for the provider record store: TTL, republishing, withdrawal, eviction, replies and persistence."""
import json
import os
import random
import tempfile
import threading
import unittest
from unittest import mock

from rnfs import fsutil
from rnfs.constants import DAY, ID_LENGTH, K, PROVIDER_TTL
from rnfs.dht.providers import MIN_PER_PUBLISHER, ProviderStore


def ident(n: int) -> bytes:
    """The 16-byte ID whose big-endian value is n."""
    return n.to_bytes(ID_LENGTH, "big")


KEY = ident(0xABC)


class FakeClock:
    """A manually advanced wall clock."""

    def __init__(self):
        """Clock at an arbitrary start time; tests advance it by setting now."""
        self.now = 1_700_000_000.0

    def __call__(self) -> float:
        """The current fake time."""
        return self.now


class StoreTestCase(unittest.TestCase):
    """Base: an in-memory store with a fake clock and seeded rng."""

    max_records = 1000

    def setUp(self):
        """Create the store."""
        self.clock = FakeClock()
        self.store = ProviderStore(self.max_records, None, clock=self.clock, rng=random.Random(1))


class RecordTest(StoreTestCase):
    """Storing, refreshing, expiring and withdrawing records."""

    def test_store_and_list(self):
        self.assertTrue(self.store.store(KEY, ident(1)))
        self.assertEqual(self.store.providers(KEY), [ident(1)])
        self.assertEqual(self.store.record_count(), 1)
        self.assertEqual(self.store.providers(ident(5)), [])

    def test_rejects_malformed_ids(self):
        with self.assertRaises(ValueError):
            self.store.store(b"short", ident(1))
        with self.assertRaises(ValueError):
            self.store.store(KEY, b"short")

    def test_future_times_clamped_at_expiry(self):
        # Stored while the clock ran a day ahead: once corrected, the record lives a TTL from now, not a day more
        self.clock.now += DAY
        self.store.store(KEY, ident(1))
        self.clock.now -= DAY
        self.assertEqual(self.store.expire(), 0)
        self.assertEqual(self.store.first_seen(ident(1)), self.clock.now)
        self.clock.now += PROVIDER_TTL
        self.assertEqual(self.store.expire(), 1)

    def test_expires_ttl_after_last_stored(self):
        self.store.store(KEY, ident(1))
        self.clock.now += PROVIDER_TTL - 1
        self.assertEqual(self.store.providers(KEY), [ident(1)])
        self.clock.now += 1
        self.assertEqual(self.store.providers(KEY), [])
        self.assertEqual(self.store.for_reply(KEY), [])
        self.assertEqual(self.store.expire(), 1)
        self.assertEqual(self.store.record_count(), 0)
        self.assertEqual(self.store.publisher_count(), 0)

    def test_republish_extends_expiry_and_keeps_first_stored(self):
        self.store.store(KEY, ident(1))
        self.clock.now += 10
        self.store.store(KEY, ident(2))
        self.clock.now += DAY
        self.store.store(KEY, ident(1))  # republished, but still the older record
        self.assertEqual(self.store.providers(KEY), [ident(1), ident(2)])
        self.clock.now += PROVIDER_TTL - 1
        self.assertEqual(self.store.providers(KEY), [ident(1)])
        self.assertEqual(self.store.record_count(), 2)

    def test_withdraw_removes_record_and_publisher(self):
        self.store.store(KEY, ident(1))
        self.assertIsNotNone(self.store.first_seen(ident(1)))
        self.assertTrue(self.store.withdraw(KEY, ident(1)))
        self.assertFalse(self.store.withdraw(KEY, ident(1)))
        self.assertEqual(self.store.providers(KEY), [])
        self.assertIsNone(self.store.first_seen(ident(1)))

    def test_provide_batch(self):
        keys = [ident(100 + i) for i in range(5)]
        self.assertEqual(self.store.provide(ident(1), keys), 5)
        self.assertEqual(self.store.provide(ident(1), [ident(200)], withdrawn=keys[:3]), 1)
        self.assertEqual(self.store.record_count(), 3)
        self.assertEqual(self.store.providers(keys[0]), [])
        self.assertEqual(self.store.providers(keys[4]), [ident(1)])

    def test_providers_exclude(self):
        self.store.store(KEY, ident(1))
        self.store.store(KEY, ident(2))
        self.assertEqual(self.store.providers(KEY, exclude=lambda p: p == ident(1)), [ident(2)])


class ReplyTest(StoreTestCase):
    """for_reply: the K/2 oldest records first, then K/2 at random from the rest."""

    def fill(self, count: int):
        """Store count providers of KEY, one second apart: ident(1) is the oldest."""
        for i in range(1, count + 1):
            self.store.store(KEY, ident(i))
            self.clock.now += 1

    def test_few_providers_all_listed_oldest_first(self):
        self.fill(4)
        self.assertEqual(self.store.for_reply(KEY), [ident(i) for i in range(1, 5)])

    def test_oldest_half_then_random_half(self):
        self.fill(50)
        replies = [self.store.for_reply(KEY) for _ in range(30)]
        for reply in replies:
            self.assertEqual(len(reply), K)
            self.assertEqual(reply[:K // 2], [ident(i) for i in range(1, K // 2 + 1)])
            self.assertEqual(len(set(reply)), K)
            self.assertTrue(all(p not in reply[:K // 2] for p in reply[K // 2:]))
        # The random half varies, so later providers get listed too
        listed = {p for reply in replies for p in reply[K // 2:]}
        self.assertGreater(len(listed), 20)

    def test_random_half_fills_up_to_k(self):
        self.fill(K - 2)
        self.assertEqual(sorted(self.store.for_reply(KEY)), [ident(i) for i in range(1, K - 1)])

    def test_banned_and_expired_left_out(self):
        self.fill(3)
        self.clock.now += PROVIDER_TTL - 1.5  # ident(1) and ident(2) expired, ident(3) not
        self.store.store(KEY, ident(4))
        reply = self.store.for_reply(KEY, exclude=lambda p: p == ident(4))
        self.assertEqual(reply, [ident(3)])

    def test_squatters_cannot_hold_every_slot(self):
        # Early squatters take the oldest slots, but honest later providers still appear in the random half
        for i in range(K // 2):
            self.store.store(KEY, ident(1000 + i))
        self.clock.now += 1
        honest = ident(1)
        self.store.store(KEY, honest)
        for i in range(30):
            self.store.store(KEY, ident(2000 + i))
        seen = sum(honest in self.store.for_reply(KEY) for _ in range(50))
        self.assertGreater(seen, 0)


class EvictionTest(StoreTestCase):
    """When full, records of the publisher first seen most recently are evicted."""

    max_records = 10

    def test_new_publisher_refused_when_full(self):
        for i in range(10):
            self.store.store(ident(100 + i), ident(1))
        self.assertFalse(self.store.store(KEY, ident(2)))
        self.assertEqual(self.store.record_count(), 10)
        self.assertIsNone(self.store.first_seen(ident(2)))

    def test_newest_publisher_cannot_churn_its_own_records(self):
        self.store.store(ident(100), ident(1))
        self.clock.now += 1
        for i in range(9):
            self.store.store(ident(200 + i), ident(2))
        self.assertFalse(self.store.store(ident(300), ident(2)))
        self.assertEqual(self.store.providers(ident(300)), [])

    def test_older_publisher_displaces_newest(self):
        self.store.store(ident(100), ident(1))  # oldest publisher
        self.clock.now += 1
        for i in range(4):
            self.store.store(ident(200 + i), ident(2))
        self.clock.now += 1
        for i in range(5):
            self.store.store(ident(300 + i), ident(3))  # newest publisher
            self.clock.now += 1
        self.assertTrue(self.store.store(ident(101), ident(1)))
        self.assertEqual(self.store.record_count(), 10)
        # The newest publisher's least recently stored record went
        self.assertEqual(self.store.providers(ident(300)), [])
        # And again: the newest publisher keeps losing its least recently stored records
        self.assertTrue(self.store.store(ident(201), ident(1)))
        self.assertEqual(self.store.providers(ident(301)), [])

    def test_flood_of_new_identities_displaces_only_itself(self):
        for i in range(5):
            self.store.store(ident(100 + i), ident(1))
            self.store.store(ident(100 + i), ident(2))
        self.clock.now += 1
        accepted = sum(self.store.store(ident(500 + i), ident(10_000 + i)) for i in range(1000))
        self.assertEqual(accepted, 0)
        for i in range(5):
            self.assertEqual(sorted(self.store.providers(ident(100 + i))), [ident(1), ident(2)])

    def test_flood_after_honest_records_only_churns_flooders(self):
        for i in range(6):
            self.store.store(ident(100 + i), ident(1))
        self.clock.now += 1
        # Flooders fill the remaining room, then established publishers still get in by evicting them
        for i in range(4):
            self.store.store(ident(500 + i), ident(10_000 + i))
            self.clock.now += 1
        self.assertEqual(self.store.record_count(), 10)
        for i in range(4):
            self.assertTrue(self.store.store(ident(200 + i), ident(1)))
        self.assertEqual(self.store.record_count(), 10)
        self.assertEqual(self.store.publisher_count(), 1)
        self.assertEqual(len(self.store.providers(ident(103))), 1)

    def test_expired_records_make_room(self):
        for i in range(10):
            self.store.store(ident(100 + i), ident(1))
        self.clock.now += PROVIDER_TTL
        self.assertTrue(self.store.store(KEY, ident(2)))
        self.assertEqual(self.store.record_count(), 1)


class PublisherLimitTest(StoreTestCase):
    """One publisher can't take over the store; one key can't gather unbounded publishers."""

    max_records = 100

    def test_default_share(self):
        self.assertEqual(ProviderStore(100_000, None).max_per_publisher, 1000)
        self.assertEqual(ProviderStore(10, None).max_per_publisher, MIN_PER_PUBLISHER)

    def test_aged_publisher_cannot_take_over_the_store(self):
        attacker = ident(1)
        self.store.store(ident(100), attacker)  # seen first, so it outranks everyone in eviction
        self.clock.now += DAY
        honest = [ident(10 + n) for n in range(5)]
        for n, publisher in enumerate(honest):
            for i in range(self.store.max_per_publisher):
                self.store.store(ident(200 + 100 * n + i), publisher)
        # Before: each of these evicted a record of the newest publisher, until the attacker held all 100
        accepted = sum(self.store.store(ident(10_000 + i), attacker) for i in range(1000))
        self.assertEqual(accepted, self.store.max_per_publisher - 1)
        for n, publisher in enumerate(honest):
            held = sum(len(self.store.providers(ident(200 + 100 * n + i))) for i in range(self.store.max_per_publisher))
            self.assertEqual(held, self.store.max_per_publisher)
        # Room is left for others
        self.assertTrue(self.store.store(KEY, ident(3)))

    def test_refresh_at_the_limit(self):
        for i in range(self.store.max_per_publisher):
            self.assertTrue(self.store.store(ident(100 + i), ident(1)))
        self.assertFalse(self.store.store(KEY, ident(1)))
        self.clock.now += DAY
        self.assertTrue(self.store.store(ident(100), ident(1)))  # republishing is never refused

    def test_own_records_are_not_limited(self):
        store = ProviderStore(self.max_records, None, clock=self.clock, own_id=ident(1))
        for i in range(3 * store.max_per_publisher):
            self.assertTrue(store.store(ident(100 + i), ident(1)))
        self.assertTrue(store.store(KEY, ident(1)))
        self.assertEqual(store.record_count(), 3 * store.max_per_publisher + 1)
        # Others still are
        for i in range(store.max_per_publisher):
            store.store(ident(1000 + i), ident(2))
        self.assertFalse(store.store(KEY, ident(2)))

    def test_expired_records_do_not_count_toward_the_limit(self):
        for i in range(self.store.max_per_publisher):
            self.store.store(ident(100 + i), ident(1))
        self.clock.now += PROVIDER_TTL
        self.assertTrue(self.store.store(KEY, ident(1)))
        self.assertEqual(self.store.record_count(), 1)

    def test_full_key_evicts_a_random_non_senior_publisher(self):
        store = ProviderStore(1000, None, clock=self.clock, rng=random.Random(1), max_per_key=20)
        accepted = [store.store(KEY, ident(i)) for i in range(1, 31)]
        self.assertEqual(accepted, [True] * 30)  # newcomers are never locked out
        providers = store.providers(KEY)
        self.assertEqual(len(providers), 20)
        self.assertEqual(providers[:K // 2], [ident(i) for i in range(1, K // 2 + 1)])  # the oldest half stays
        self.assertIn(ident(30), providers)
        self.assertEqual(store.record_count(), 20)
        self.assertTrue(store.store(KEY, providers[-1]))  # a recorded publisher may still republish
        self.assertEqual(store.providers(KEY), providers)
        self.clock.now += PROVIDER_TTL
        self.assertTrue(store.store(KEY, ident(31)))  # expired records make room, evicting none
        self.assertEqual(store.providers(KEY), [ident(31)])

    def test_sybils_holding_a_key_cannot_lock_out_later_providers(self):
        store = ProviderStore(100_000, None, clock=self.clock, rng=random.Random(1))
        early = ident(1)
        store.store(KEY, early)
        for i in range(store.max_per_key - 1):
            self.assertTrue(store.store(KEY, ident(1000 + i)))
        late = ident(2)
        self.assertTrue(store.store(KEY, late))
        self.assertTrue(any(late in store.for_reply(KEY) for _ in range(1000)))  # in the random half
        self.assertIn(early, store.for_reply(KEY)[:K // 2])  # long-standing providers are never evicted

    def test_reply_asks_exclude_only_about_chosen_candidates(self):
        store = ProviderStore(10_000, None, clock=self.clock, rng=random.Random(1))
        for i in range(store.max_per_key):
            store.store(KEY, ident(i))
        self.assertEqual(len(store.providers(KEY)), store.max_per_key)
        asked = []
        banned = {ident(i) for i in range(0, store.max_per_key, 2)}  # every other one, oldest included
        reply = store.for_reply(KEY, exclude=lambda p: asked.append(p) or p in banned)
        self.assertEqual(len(reply), K)
        self.assertFalse(banned & set(reply))
        self.assertEqual(reply[:K // 2], [ident(i) for i in range(1, K, 2)])
        self.assertLess(len(asked), 100)


class PersistenceTest(unittest.TestCase):
    """Records and first-seen times survive a restart; the file is replaced atomically."""

    def setUp(self):
        """A temporary state directory."""
        self.directory = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.directory.name, "state", "providers.json")
        self.clock = FakeClock()

    def tearDown(self):
        """Remove the state directory."""
        self.directory.cleanup()

    def open(self) -> ProviderStore:
        """A store at self.path."""
        return ProviderStore(100, self.path, clock=self.clock)

    def test_round_trip(self):
        store = self.open()
        store.store(KEY, ident(1))
        self.clock.now += 5
        store.store(KEY, ident(2))
        store.store(ident(7), ident(2))
        store.save()
        self.assertEqual(os.listdir(os.path.dirname(self.path)), ["providers.json"])  # no temp files left

        self.clock.now += 100
        loaded = self.open()
        self.assertEqual(loaded.providers(KEY), [ident(1), ident(2)])
        self.assertEqual(loaded.record_count(), 3)
        self.assertEqual(loaded.first_seen(ident(1)), store.first_seen(ident(1)))
        self.assertEqual(loaded.first_seen(ident(2)), store.first_seen(ident(2)))

    def test_first_seen_order_survives_restart(self):
        store = ProviderStore(3, self.path, clock=self.clock)
        store.store(ident(100), ident(1))
        self.clock.now += 1
        store.store(ident(101), ident(2))
        store.store(ident(102), ident(2))
        store.save()
        loaded = ProviderStore(3, self.path, clock=self.clock)
        self.assertTrue(loaded.store(ident(103), ident(1)))  # evicts a record of ident(2), the newest
        self.assertEqual(loaded.record_count(), 3)
        self.assertEqual(len(loaded.providers(ident(101)) + loaded.providers(ident(102))), 1)

    def test_limits_applied_on_load_keeping_oldest(self):
        store = ProviderStore(100, self.path, clock=self.clock, max_per_publisher=100)
        for i in range(30):
            store.store(ident(100 + i), ident(1))
            self.clock.now += 1
        store.save()
        loaded = ProviderStore(100, self.path, clock=self.clock, max_per_publisher=10)
        self.assertEqual(loaded.record_count(), 10)
        self.assertEqual([len(loaded.providers(ident(100 + i))) for i in range(30)], [1] * 10 + [0] * 20)

    def test_expired_records_dropped_on_load(self):
        store = self.open()
        store.store(KEY, ident(1))
        store.save()
        self.clock.now += PROVIDER_TTL
        loaded = self.open()
        self.assertEqual(loaded.record_count(), 0)
        self.assertIsNone(loaded.first_seen(ident(1)))

    def test_future_times_clamped_on_load(self):
        # Saved while the clock ran a year ahead; once corrected, records still expire a TTL from now
        self.clock.now += 365 * DAY
        store = self.open()
        store.store(KEY, ident(1))
        store.save()
        self.clock.now -= 365 * DAY
        loaded = self.open()
        self.assertEqual(loaded.first_seen(ident(1)), self.clock.now)
        self.assertEqual(loaded.providers(KEY), [ident(1)])
        self.clock.now += PROVIDER_TTL
        self.assertEqual(loaded.expire(), 1)

    def test_out_of_range_numbers_start_empty(self):
        os.makedirs(os.path.dirname(self.path))
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump({"version": 1, "publishers": {ident(1).hex(): 10 ** 400}, "records": []}, f)
        self.assertEqual(self.open().record_count(), 0)

    def test_saves_never_land_out_of_order(self):
        # A save that snapshots while another is still writing must not be overwritten by the older snapshot
        store = self.open()
        store.store(KEY, ident(1))
        real_replace = fsutil.replace
        second = []

        def slow_replace(source, target):
            """The first save's rename: meanwhile another thread stores and saves."""
            if not second:
                store.store(ident(7), ident(2))
                second.append(threading.Thread(target=store.save))
                second[0].start()
                second[0].join(0.2)
            real_replace(source, target)

        with mock.patch.object(fsutil, "replace", slow_replace):
            store.save()
            second[0].join(5)
        with open(self.path, encoding="utf-8") as f:
            self.assertEqual(len(json.load(f)["records"]), 2)

    def test_corrupt_file_starts_empty(self):
        os.makedirs(os.path.dirname(self.path))
        with open(self.path, "w", encoding="utf-8") as f:
            f.write("{not json")
        self.assertEqual(self.open().record_count(), 0)

    def test_save_only_when_changed(self):
        store = self.open()
        store.save()
        self.assertFalse(os.path.exists(self.path))
        store.store(KEY, ident(1))
        store.save()
        with open(self.path, encoding="utf-8") as f:
            self.assertEqual(len(json.load(f)["records"]), 1)


class ThreadSafetyTest(StoreTestCase):
    """Concurrent stores, withdrawals and replies."""

    max_records = 200

    def test_concurrent_use_keeps_counts_consistent(self):
        errors = []

        def worker(seed: int):
            """A mix of operations on a small key and publisher space."""
            r = random.Random(seed)
            try:
                for _ in range(3000):
                    key, publisher = ident(r.randrange(50)), ident(1000 + r.randrange(30))
                    action = r.random()
                    if action < 0.6:
                        self.store.store(key, publisher)
                    elif action < 0.8:
                        self.store.withdraw(key, publisher)
                    else:
                        self.store.for_reply(key)
            except Exception as e:  # pragma: no cover - reported below
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        total = sum(len(self.store.providers(ident(k))) for k in range(50))
        self.assertEqual(total, self.store.record_count())
        self.assertLessEqual(total, 200)


if __name__ == "__main__":
    unittest.main()
