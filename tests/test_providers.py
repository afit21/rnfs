"""Tests for the provider record store: TTL, republishing, withdrawal, eviction, replies and persistence (the
state database's provider tables)."""
import os
import random
import tempfile
import threading
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from unittest import mock

from rnfs.constants import DAY, ID_LENGTH, K, PROVIDER_TTL
from rnfs.dht.providers import MIN_PER_PUBLISHER, ProviderStore
from tests.support import FakeClock, failing_commits, state_execute, state_rows, write_legacy


def ident(n: int) -> bytes:
    """The 16-byte ID whose big-endian value is n."""
    return n.to_bytes(ID_LENGTH, "big")


KEY = ident(0xABC)


class StoreTestCase(unittest.TestCase):
    """Base: an in-memory store with a fake clock and seeded rng; every store's indexes are checked after each test."""

    max_records = 1000

    def setUp(self):
        """Create the store."""
        self.clock = FakeClock(1_700_000_000.0)
        self.stores: list[ProviderStore] = []
        self.store = self.new_store(self.max_records, rng=random.Random(1))

    def new_store(self, max_records: int, **settings) -> ProviderStore:
        """An in-memory store on the fake clock, checked for consistency after the test."""
        store = ProviderStore(max_records, None, clock=self.clock, **settings)
        self.stores.append(store)
        return store

    def tearDown(self):
        """Every index of every store agrees with its records."""
        for store in self.stores:
            store._check_consistency()


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

    def test_new_publisher_evicts_a_pooled_record_when_full(self):
        for i in range(10):
            self.store.store(ident(100 + i), ident(1))
        # ident(1)'s last record went into the newcomer pool (a tenth of the store); a new publisher evicts a
        # random pooled record rather than being refused, and the records outside the pool stay
        self.assertTrue(self.store.store(KEY, ident(2)))
        self.assertEqual(self.store.record_count(), 10)
        self.assertEqual(self.store.providers(ident(109)), [])
        self.assertEqual([len(self.store.providers(ident(100 + i))) for i in range(9)], [1] * 9)

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

    def test_flood_of_new_identities_displaces_only_pooled_records(self):
        for i in range(5):
            self.store.store(ident(100 + i), ident(1))
            self.store.store(ident(100 + i), ident(2))
        self.clock.now += 1
        accepted = sum(self.store.store(ident(500 + i), ident(10_000 + i)) for i in range(1000))
        # Each new identity lands in the newcomer pool (one record here), evicting the one there
        self.assertEqual(accepted, 1000)
        self.assertEqual(self.store.record_count(), 10)
        for i in range(4):
            self.assertEqual(sorted(self.store.providers(ident(100 + i))), [ident(1), ident(2)])
        # ident(2)'s last record, stored once the rest of the store was full, was the one in the pool
        self.assertEqual(self.store.providers(ident(104)), [ident(1)])

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
        store = self.new_store(self.max_records, own_id=ident(1))
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
        store = self.new_store(1000, rng=random.Random(1), max_per_key=20)
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
        store = self.new_store(100_000, rng=random.Random(1))
        early = ident(1)
        store.store(KEY, early)
        for i in range(store.max_per_key - 1):
            self.assertTrue(store.store(KEY, ident(1000 + i)))
        late = ident(2)
        self.assertTrue(store.store(KEY, late))
        self.assertTrue(any(late in store.for_reply(KEY) for _ in range(1000)))  # in the random half
        self.assertIn(early, store.for_reply(KEY)[:K // 2])  # long-standing providers are never evicted

    def test_reply_asks_exclude_only_about_chosen_candidates(self):
        store = self.new_store(10_000, rng=random.Random(1))
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


class Recorder:
    """A transaction's connection that records what each executemany writes or deletes."""

    def __init__(self, tx, writes: list):
        """Forward to tx, recording into writes."""
        self.tx = tx
        self.writes = writes

    def execute(self, sql, params=()):
        """Run one statement, recording it with its one row."""
        self.writes.append((sql, [params]))
        return self.tx.execute(sql, params)

    def executemany(self, sql, rows):
        """Run a statement for each row, recording them."""
        rows = list(rows)
        self.writes.append((sql, rows))
        return self.tx.executemany(sql, rows)


@contextmanager
def recorded_writes(db, before=None) -> Iterator[list]:
    """Within the with block, each transaction of the StateDb reference db appends the list of its statements, as
    (sql, rows), to the list yielded; before(), if given, is called as each one is about to open."""
    transactions: list[list] = []
    real = db.transaction

    @contextmanager
    def transaction(durable: bool = False):
        """The real transaction, recorded."""
        if before is not None:
            before()
        with real(durable) as tx:
            transactions.append([])
            yield Recorder(tx, transactions[-1])

    with mock.patch.object(db, "transaction", transaction):
        yield transactions


def rows_written(transactions: list[list]) -> int:
    """How many rows the statements recorded by recorded_writes wrote or deleted."""
    return sum(len(rows) for statements in transactions for _, rows in statements)


def saved(state_dir: str) -> dict:
    """The provider tables in state_dir, shaped as held() gives a store's state."""
    return {
        "publishers": dict(state_rows(state_dir, "SELECT publisher, first_seen FROM provider_publishers")),
        "records": {(key, publisher): (first, last, pooled) for key, publisher, first, last, pooled in state_rows(
            state_dir, "SELECT key, publisher, first_stored, last_stored, pooled FROM provider_records")},
        "departed": {publisher: (seen, at) for publisher, seen, at in state_rows(
            state_dir, "SELECT publisher, first_seen, at FROM provider_departed")},
        "evicted": {publisher: (seen, at) for publisher, seen, at in state_rows(
            state_dir, "SELECT publisher, first_seen, at FROM provider_evicted")},
    }


def held(store: ProviderStore) -> dict:
    """What store holds in memory: publishers' first-seen times, records with every field, departures and
    evictions."""
    with store._lock:
        return {
            "publishers": {publisher: entry.first_seen for publisher, entry in store._publishers.items()},
            "records": {(key, publisher): (record.first_stored, record.last_stored, int(record.pooled))
                        for key, records in store._records.items() for publisher, record in records.items()},
            "departed": dict(store._departed),
            "evicted": dict(store._evicted),
        }


class PersistenceTest(unittest.TestCase):
    """Records and first-seen times survive a restart; saves write only what changed, in order."""

    def setUp(self):
        """A temporary state directory (removed after the stores on it are closed)."""
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.state = os.path.join(self.directory.name, "state")
        self.clock = FakeClock(1_700_000_000.0)
        self.stores: list[ProviderStore] = []

    def open(self, max_records: int = 100, **settings) -> ProviderStore:
        """A store on self.state, checked for consistency and closed when the test ends."""
        store = ProviderStore(max_records, self.state, clock=self.clock, **settings)
        self.addCleanup(store.close)
        self.stores.append(store)
        return store

    def tearDown(self):
        """Every index of every store opened agrees with its records (before cleanups close them)."""
        for store in self.stores:
            store._check_consistency()

    def test_round_trip(self):
        """Records and first-seen times are saved, and loaded by the next store."""
        store = self.open()
        store.store(KEY, ident(1))
        self.clock.now += 5
        store.store(KEY, ident(2))
        store.store(ident(7), ident(2))
        store.save()
        self.assertEqual(len(state_rows(self.state, "SELECT * FROM provider_records")), 3)

        self.clock.now += 100
        loaded = self.open()
        self.assertEqual(loaded.providers(KEY), [ident(1), ident(2)])
        self.assertEqual(loaded.record_count(), 3)
        self.assertEqual(loaded.first_seen(ident(1)), store.first_seen(ident(1)))
        self.assertEqual(loaded.first_seen(ident(2)), store.first_seen(ident(2)))

    def test_own_record_beyond_a_full_key_survives_restart(self):
        """The node's own record doesn't count toward max_per_key on load either (DHT-5)."""
        own = ident(1)
        store = self.open(1000, max_per_key=5, own_id=own)
        for i in range(5):
            store.store(KEY, ident(100 + i))
        self.clock.now += 1
        store.store(KEY, own)
        store.save()
        loaded = self.open(1000, max_per_key=5, own_id=own)
        self.assertEqual(loaded.providers(KEY), [ident(100 + i) for i in range(5)] + [own])

    def test_first_seen_order_survives_restart(self):
        """A publisher seen first still evicts the newest one's records after a restart."""
        store = self.open(3)
        store.store(ident(100), ident(1))
        self.clock.now += 1
        store.store(ident(101), ident(2))
        store.store(ident(102), ident(2))
        store.save()
        loaded = self.open(3)
        self.assertTrue(loaded.store(ident(103), ident(1)))  # evicts a record of ident(2), the newest
        self.assertEqual(loaded.record_count(), 3)
        self.assertEqual(len(loaded.providers(ident(101)) + loaded.providers(ident(102))), 1)

    def test_limits_applied_on_load_keeping_oldest(self):
        """A smaller per-publisher limit at load keeps the oldest records; the next save deletes the rest."""
        store = self.open(max_per_publisher=100)
        for i in range(30):
            store.store(ident(100 + i), ident(1))
            self.clock.now += 1
        store.save()
        loaded = self.open(max_per_publisher=10)
        self.assertEqual(loaded.record_count(), 10)
        self.assertEqual([len(loaded.providers(ident(100 + i))) for i in range(30)], [1] * 10 + [0] * 20)
        loaded.save()
        self.assertEqual(saved(self.state), held(loaded))
        self.assertEqual(len(saved(self.state)["records"]), 10)

    def test_expired_records_dropped_on_load(self):
        """Records expired by the time of the load aren't loaded, and the next save deletes their rows."""
        store = self.open()
        store.store(KEY, ident(1))
        store.save()
        self.clock.now += PROVIDER_TTL
        loaded = self.open()
        self.assertEqual(loaded.record_count(), 0)
        self.assertIsNone(loaded.first_seen(ident(1)))
        loaded.save()
        self.assertEqual(saved(self.state), {"publishers": {}, "records": {}, "departed": {}, "evicted": {}})

    def test_future_times_clamped_on_load(self):
        """Times saved while the clock ran ahead load as now, and are saved so."""
        # Saved while the clock ran a year ahead; once corrected, records still expire a TTL from now
        self.clock.now += 365 * DAY
        store = self.open()
        store.store(KEY, ident(1))
        store.save()
        self.clock.now -= 365 * DAY
        loaded = self.open()
        self.assertEqual(loaded.first_seen(ident(1)), self.clock.now)
        self.assertEqual(loaded.providers(KEY), [ident(1)])
        loaded.save()
        self.assertEqual(saved(self.state)["records"], {(KEY, ident(1)): (self.clock.now, self.clock.now, 0)})
        self.clock.now += PROVIDER_TTL
        self.assertEqual(loaded.expire(), 1)

    def test_times_clamped_at_expiry_are_saved(self):
        """Times expire() brings back to now (records, first-seen times, departures) are saved so."""
        store = self.open(seniority=DAY)
        store.store(KEY, ident(1))
        self.clock.now += 2 * DAY
        store.store(ident(7), ident(2))
        store.withdraw(KEY, ident(1))  # departs, remembered
        store.save()
        self.clock.now -= 3 * DAY
        store.expire()
        with recorded_writes(store._db) as transactions:
            store.save()
        self.assertEqual(saved(self.state), held(store))
        now = self.clock.now
        self.assertEqual(saved(self.state)["records"], {(ident(7), ident(2)): (now, now, 0)})
        self.assertEqual(saved(self.state)["departed"], {ident(1): (now, now)})
        # The record, publisher and departure, the departure deleted from both its tables first
        self.assertEqual(rows_written(transactions), 5)

    def test_malformed_rows_are_ignored_and_deleted(self):
        """Rows of the wrong type or length, or of a publisher without a row, load as nothing; the next save
        deletes them, keeping the rest."""
        store = self.open(seniority=DAY)
        store.store(KEY, ident(1))
        store.store(KEY, ident(2))
        self.clock.now += 2 * DAY
        store.withdraw(KEY, ident(2))  # departs, remembered
        store.save()
        store.close()
        records = ("INSERT INTO provider_records (key, publisher, first_stored, last_stored, pooled)"
                   " VALUES (?, ?, ?, ?, ?)")
        now = self.clock.now
        for sql, row in ((records, (b"short", ident(1), now, now, 0)),
                         (records, ("text key 16 long", ident(1), now, now, 0)),
                         (records, (ident(8), ident(1), "yesterday", now, 0)),
                         (records, (ident(9), ident(1), now, now, "pooled")),
                         (records, (ident(10), ident(3), now, now, 0)),  # ident(3) has no publisher row
                         ("INSERT INTO provider_publishers (publisher, first_seen) VALUES (?, ?)", (ident(4), "x")),
                         ("INSERT INTO provider_publishers (publisher, first_seen) VALUES (?, ?)", (b"x", now)),
                         ("INSERT INTO provider_departed (publisher, first_seen, at) VALUES (?, ?, ?)", (b"y", 1, now)),
                         ("INSERT INTO provider_evicted (publisher, first_seen, at) VALUES (?, ?, ?)",
                          (ident(5), now, "x"))):
            state_execute(self.state, sql, row)
        loaded = self.open(seniority=DAY)
        self.assertEqual(loaded.providers(KEY), [ident(1)])
        self.assertEqual(loaded.record_count(), 1)
        self.assertEqual(loaded.allowance(ident(2)), loaded.max_per_publisher)
        loaded.save()
        self.assertEqual(saved(self.state), held(loaded))
        self.assertEqual(len(saved(self.state)["records"]), 1)

    def test_legacy_file_with_out_of_range_numbers_is_set_aside(self):
        """providers.json with a number no float holds (which made rnfs 0.1.4 start empty) is moved aside."""
        write_legacy(self.state, "providers.json",
                     {"version": 1, "publishers": {ident(1).hex(): 10 ** 400}, "records": []})
        self.assertEqual(self.open().record_count(), 0)
        self.assertTrue(os.path.exists(os.path.join(self.state, "providers.json.damaged")))

    def test_saves_never_land_out_of_order(self):
        """A save that snapshots while another is still writing doesn't land first, overwritten by the older."""
        store = self.open()
        store.store(KEY, ident(1))
        second = []

        def refresh_and_save():
            """Before the first save writes: refresh its record, and save from another thread."""
            if not second:
                self.clock.now += 5
                store.store(KEY, ident(1))
                second.append(threading.Thread(target=store.save))
                second[0].start()
                second[0].join(0.2)

        with recorded_writes(store._db, before=refresh_and_save):
            store.save()
            second[0].join(5)
        self.assertEqual(saved(self.state)["records"], {(KEY, ident(1)): (self.clock.now - 5, self.clock.now, 0)})

    def test_saved_tables_are_the_whole_state(self):
        """The tables hold every publisher, record (pooled or not), departure and eviction, as memory does."""
        store = self.open(20, seniority=DAY)
        for publisher in (1, 2):
            for i in range(9):
                store.store(ident(100 * publisher + i), ident(publisher))
                self.clock.now += 1
        for publisher in (3, 4, 5):  # the main part is full: newcomers share the pool, and 5 evicts 3 or 4
            store.store(ident(1000 + publisher), ident(publisher))
            self.clock.now += 1
        store.store(ident(101), ident(1))  # refreshed in place
        store.withdraw(ident(100), ident(1))
        self.clock.now += DAY
        store.withdraw(ident(1005), ident(5))  # 5 departs, seen for long enough to be remembered
        expected = held(store)
        self.assertTrue(expected["departed"] and expected["evicted"])
        self.assertIn(1, [pooled for _, _, pooled in expected["records"].values()])
        store.save()
        self.assertEqual(saved(self.state), expected)

    def test_save_writes_only_what_changed(self):
        """With a thousand records saved, refreshing one and withdrawing another writes two rows."""
        store = self.open(2000)
        for i in range(1000):
            store.store(ident(100 + i), ident(1 + i % 10))
        store.save()
        self.clock.now += 10
        store.store(ident(100), ident(1))
        store.withdraw(ident(101), ident(2))
        with recorded_writes(store._db) as transactions:
            store.save()
        self.assertEqual(len(transactions), 1)
        self.assertEqual(rows_written(transactions), 2)
        self.assertEqual(saved(self.state), held(store))

    def test_failed_save_is_tried_again(self):
        """A save whose commit fails raises, and the next save writes what it held."""
        store = self.open()
        store.store(KEY, ident(1))
        with failing_commits():
            with self.assertRaises(OSError):
                store.save()
        store.save()
        self.assertEqual(self.open().providers(KEY), [ident(1)])

    def test_corrupt_legacy_file_starts_empty(self):
        """providers.json that isn't JSON is moved aside, and the store starts empty."""
        write_legacy(self.state, "providers.json", text="{not json")
        self.assertEqual(self.open().record_count(), 0)
        self.assertTrue(os.path.exists(os.path.join(self.state, "providers.json.damaged")))

    def test_save_only_when_changed(self):
        """A save with nothing changed opens no transaction."""
        store = self.open()
        with recorded_writes(store._db) as transactions:
            store.save()
            self.assertEqual(transactions, [])
            store.store(KEY, ident(1))
            store.save()
            store.save()
        self.assertEqual(len(transactions), 1)
        self.assertEqual(len(saved(self.state)["records"]), 1)

    def test_random_operations_are_saved_and_loaded_exactly(self):
        """Many random stores, withdrawals, batches, expiries, retains and saves (some failing) with every limit
        reached: a store loaded from the tables holds exactly what the first one holds, and so do the tables."""
        rng = random.Random(7)
        own = ident(1)
        settings = {"max_per_key": 6, "own_id": own, "seniority": DAY, "rng": random.Random(1)}
        store = self.open(60, **settings)
        publishers = [ident(1 + n) for n in range(40)]
        keys = [ident(1000 + n) for n in range(30)]

        def check():
            """Expire, save, and load another store from the tables: it, and they, hold what store does."""
            store.expire()
            store.save()
            loaded = ProviderStore(60, self.state, clock=self.clock, **settings)
            try:
                store._check_consistency()
                loaded._check_consistency()
                self.assertEqual(held(loaded), held(store))
                self.assertEqual(saved(self.state), held(store))
                self.assertFalse(loaded._dirty_records or loaded._dirty_publishers or loaded._dirty_gone)
            finally:
                loaded.close()

        failed = 0
        for _ in range(4000):
            action = rng.random()
            publisher = rng.choice(publishers)
            if action < 0.4:
                store.store(rng.choice(keys), publisher)
            elif action < 0.55:
                store.withdraw(rng.choice(keys), publisher)
            elif action < 0.65:
                store.provide(publisher, rng.sample(keys, rng.randrange(1, 12)), rng.sample(keys, rng.randrange(3)))
            elif action < 0.8:
                self.clock.now += rng.choice((1, 60, 3600))
            elif action < 0.83:
                self.clock.now += rng.choice((DAY, 3 * DAY, PROVIDER_TTL))
                store.expire()
            elif action < 0.85:
                banned = set(rng.sample(publishers, 3))
                store.retain(lambda p: p not in banned)
            elif action < 0.95:
                store.save()
            elif action < 0.97:
                with failing_commits():
                    try:
                        store.save()
                    except OSError:
                        failed += 1
            elif action < 0.98:
                check()
        self.assertGreater(failed, 10)
        self.assertGreater(store._count, 0)
        self.assertTrue(store._departed or store._evicted)
        check()

    def test_random_operations_keep_indexes_consistent(self):
        """Random stores, withdrawals, expiries, clock corrections and reloads (some with smaller limits) in a store
        kept full: every index agrees with the records after each step, and after expire() no time is later than
        now."""
        rng = random.Random(11)
        own = ident(1)
        publishers = [ident(1 + n) for n in range(25)]
        keys = [ident(1000 + n) for n in range(20)]
        limits = [{"max_records": 40}, {"max_records": 40}, {"max_records": 30, "max_per_publisher": 5},
                  {"max_records": 20, "max_per_key": 3}]
        settings = {"max_per_key": 5, "own_id": own, "seniority": DAY, "rng": random.Random(2)}
        store = self.open(**(settings | limits[0]))
        counts = {"pooled": 0, "reloads": 0, "clamped": 0}

        def no_future_times(store: ProviderStore):
            """Nothing store holds is later than now."""
            now = self.clock.now
            state = held(store)
            self.assertTrue(all(seen <= now for seen in state["publishers"].values()))
            self.assertTrue(all(first <= now and last <= now for first, last, _ in state["records"].values()))
            for memory in ("departed", "evicted"):
                self.assertTrue(all(seen <= now and at <= now for seen, at in state[memory].values()))

        for _ in range(3000):
            action = rng.random()
            publisher = rng.choice(publishers)
            if action < 0.45:
                store.store(rng.choice(keys), publisher)
            elif action < 0.6:
                store.withdraw(rng.choice(keys), publisher)
            elif action < 0.75:
                self.clock.now += rng.choice((1, 600, 3 * 3600, DAY, 3 * DAY))
            elif action < 0.8:
                self.clock.now -= rng.choice((1, 600, DAY))  # corrected backwards: times are now in the future
                counts["clamped"] += 1
            elif action < 0.88:
                store.expire()
                no_future_times(store)
            elif action < 0.9:
                store.retain(lambda p: p not in {publisher})
            elif action < 0.95:
                # A restart, sometimes with expired records and future times still held (the load drops and clamps
                # them): after an expire(), the reloaded store holds what this one did unless its limits are smaller
                expired = rng.random() < 0.5
                if expired:
                    store.expire()
                store.save()
                store.close()
                setting = rng.choice(limits)
                reloaded = self.open(**(settings | setting))
                if expired and setting == limits[0] and store.max_records == 40:
                    self.assertEqual(held(reloaded), held(store))
                no_future_times(reloaded)
                store = reloaded
                counts["reloads"] += 1
            counts["pooled"] = max(counts["pooled"], store._pool_count)
            store._check_consistency()
        self.assertTrue(counts["pooled"] and counts["reloads"] > 50 and counts["clamped"] > 50)


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
