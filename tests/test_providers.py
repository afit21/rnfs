"""Tests for the provider record store (rnfs/dht/providers.py): TTL, republishing, withdrawal, /find replies, the
cap of publishers per key, the record budget and its storage tiers, and the records on disk (the state database's
provider tables): batched writes, restarts and the upgrade of 0.1.x's tables."""
import os
import random
import tempfile
import threading
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from unittest import mock

import RNS

from rnfs import statedb
from rnfs.constants import DAY, HOUR, ID_LENGTH, K, PROVIDER_TTL
from rnfs.dht import providers
from rnfs.dht.providers import EARNED_DAYS, NEWCOMER_TTL_FULL, ProviderStore
from tests.support import FakeClock, failing_commits, state_execute, state_rows, write_legacy

T0 = 19676 * DAY + HOUR  # 01:00 UTC, so a day's refreshes fall on one UTC day
SIZE = 100  # bytes a record takes in these tests (record_bytes): a store of capacity n has a budget of n * SIZE


def ident(n: int) -> bytes:
    """The 16-byte ID whose big-endian value is n."""
    return n.to_bytes(ID_LENGTH, "big")


KEY = ident(0xABC)
OWN = ident(0xFFFF)
_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the log while these tests run (failed saves and damaged files are logged)."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


def keys(start: int, count: int) -> list[bytes]:
    """count keys from ident(start) on."""
    return [ident(start + i) for i in range(count)]


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
    """The provider tables in state_dir: first-seen times, records with every column, standings (records, tier,
    days, last day), and the memories of departed and evicted publishers 0.1.x kept (0.2.0 keeps none)."""
    return {
        "publishers": dict(state_rows(state_dir, "SELECT publisher, first_seen FROM provider_publishers")),
        "records": {(key, publisher): (first, last, pooled) for key, publisher, first, last, pooled in state_rows(
            state_dir, "SELECT key, publisher, first_stored, last_stored, pooled FROM provider_records")},
        "standing": {publisher: tuple(row) for publisher, *row in state_rows(
            state_dir, "SELECT publisher, records, tier, days, last_day FROM provider_standing")},
        "departed": {publisher: (seen, at) for publisher, seen, at in state_rows(
            state_dir, "SELECT publisher, first_seen, at FROM provider_departed")},
        "evicted": {publisher: (seen, at) for publisher, seen, at in state_rows(
            state_dir, "SELECT publisher, first_seen, at FROM provider_evicted")},
    }


def held(store: ProviderStore) -> dict:
    """What store holds, as its tables and the changes it hasn't written yet make it, shaped as saved() gives it."""
    with store._lock:
        records, standings = store._merged()
    return {"publishers": {publisher: standing.first_seen for publisher, standing in standings.items()},
            "records": {pair: (first, last, 0) for pair, (first, last) in records.items()},
            "standing": {publisher: standing.row() for publisher, standing in standings.items()},
            "departed": {}, "evicted": {}}


class StoreTestCase(unittest.TestCase):
    """Base: memory-only stores on a fake clock, with record_bytes SIZE, seeded picks and a set of Known publishers;
    every store's caches and counts are checked against its records after each test."""

    capacity = 10_000  # records the default store holds

    def setUp(self):
        """The clock, the Known set and the default store."""
        self.clock = FakeClock(T0)
        self.known: set[bytes] = set()
        self.stores: list[ProviderStore] = []
        self.store = self.new_store()

    def new_store(self, capacity: int | None = None, state_dir: str | None = None, **settings) -> ProviderStore:
        """A store holding capacity records (default: the class's) on state_dir (None: memory only), checked after
        the test (and closed then, if on disk)."""
        settings.setdefault("known", self.known.__contains__)
        settings.setdefault("rng", random.Random(1))
        settings.setdefault("record_bytes", SIZE)
        store = ProviderStore(state_dir, (self.capacity if capacity is None else capacity) * SIZE, clock=self.clock,
                              **settings)
        self.stores.append(store)
        if state_dir is not None:
            self.addCleanup(store.close)
        return store

    def tearDown(self):
        """Every store's caches and counts agree with its records (those not closed yet)."""
        for store in self.stores:
            if store._db._database is not None:
                store._check_consistency()

    def fill(self, store: ProviderStore, publisher: bytes, count: int, start: int) -> list[bytes]:
        """Store count records of publisher, for keys from ident(start) on, each accepted; the keys."""
        stored = keys(start, count)
        for key in stored:
            self.assertTrue(store.store(key, publisher), f"{publisher.hex()}'s record refused")
        return stored

    def earn(self, store: ProviderStore, publisher: bytes, count: int, start: int) -> list[bytes]:
        """Make publisher Earned holding count records: stored, then one refreshed on each of EARNED_DAYS later UTC
        days (the clock moves EARNED_DAYS days on). The keys."""
        stored = self.fill(store, publisher, count, start)
        for _ in range(EARNED_DAYS):
            self.clock.now += DAY
            self.assertTrue(store.store(stored[0], publisher))
        self.assertEqual(store.standing(publisher)["tier"], "earned")
        return stored

    def held_by(self, store: ProviderStore, publisher: bytes) -> int:
        """How many records publisher holds."""
        standing = store.standing(publisher)
        return 0 if standing is None else standing["records"]


class RecordTest(StoreTestCase):
    """Storing, refreshing, expiring and withdrawing records."""

    def test_store_and_list(self):
        self.assertTrue(self.store.store(KEY, ident(1)))
        self.assertEqual(self.store.providers(KEY), [ident(1)])
        self.assertEqual(self.store.record_count(), 1)
        self.assertEqual(self.store.publisher_count(), 1)
        self.assertEqual(self.store.first_seen(ident(1)), T0)
        self.assertEqual(self.store.providers(ident(5)), [])

    def test_rejects_malformed_ids(self):
        for call in (lambda: self.store.store(b"short", ident(1)), lambda: self.store.store(KEY, b"short"),
                     lambda: self.store.provide(ident(1), [b"short"]), lambda: self.store.withdraw(KEY, b"x")):
            with self.assertRaises(ValueError):
                call()

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
        self.assertIsNone(self.store.standing(ident(1)))  # no refresh days to keep

    def test_republish_extends_expiry_and_keeps_first_stored(self):
        self.store.store(KEY, ident(1))
        self.clock.now += 10
        self.store.store(KEY, ident(2))
        self.clock.now += DAY
        self.store.store(KEY, ident(1))  # republished, but still the older record
        self.assertEqual(self.store.providers(KEY), [ident(1), ident(2)])
        self.assertEqual(self.store.record(KEY, ident(1)), (T0, self.clock.now))
        self.clock.now += PROVIDER_TTL - 1
        self.assertEqual(self.store.providers(KEY), [ident(1)])

    def test_a_refresh_within_six_hours_moves_nothing(self):
        """A refresh is recorded only once it moves the last-stored time by more than REFRESH_WRITE (6 h)."""
        self.store.store(KEY, ident(1))
        self.clock.now += 6 * HOUR
        self.store.store(KEY, ident(1))
        self.assertEqual(self.store.record(KEY, ident(1)), (T0, T0))
        self.clock.now += 1
        self.store.store(KEY, ident(1))
        self.assertEqual(self.store.record(KEY, ident(1)), (T0, T0 + 6 * HOUR + 1))

    def test_expired_record_stored_again_is_new(self):
        self.store.store(KEY, ident(1))
        self.clock.now += PROVIDER_TTL
        self.assertTrue(self.store.store(KEY, ident(1)))
        self.assertEqual(self.store.record(KEY, ident(1)), (self.clock.now, self.clock.now))
        self.assertEqual(self.store.record_count(), 1)

    def test_withdraw_removes_record_and_publisher(self):
        self.store.store(KEY, ident(1))
        self.assertTrue(self.store.withdraw(KEY, ident(1)))
        self.assertFalse(self.store.withdraw(KEY, ident(1)))
        self.assertEqual(self.store.providers(KEY), [])
        self.assertEqual((self.store.record_count(), self.store.publisher_count()), (0, 0))
        self.assertIsNone(self.store.first_seen(ident(1)))

    def test_provide_batch(self):
        """Withdrawals first, then stores: a key in both lists is stored."""
        self.assertEqual(self.store.provide(ident(1), [KEY, ident(2)]), 2)
        self.assertEqual(self.store.provide(ident(1), [KEY], withdrawn=[KEY, ident(2)]), 1)
        self.assertEqual(self.store.providers(KEY), [ident(1)])
        self.assertEqual(self.store.providers(ident(2)), [])

    def test_providers_exclude(self):
        self.store.store(KEY, ident(1))
        self.store.store(KEY, ident(2))
        self.assertEqual(self.store.providers(KEY, exclude=lambda p: p == ident(1)), [ident(2)])

    def test_future_times_clamped_at_expiry(self):
        # Stored while the clock ran a day ahead: once corrected, the record lives a TTL from now, not a day more
        self.clock.now += DAY
        self.store.store(KEY, ident(1))
        self.clock.now -= DAY
        self.assertEqual(self.store.expire(), 0)
        self.assertEqual(self.store.first_seen(ident(1)), self.clock.now)
        self.assertEqual(self.store.record(KEY, ident(1)), (self.clock.now, self.clock.now))
        self.clock.now += PROVIDER_TTL
        self.assertEqual(self.store.expire(), 1)

    def test_expiry_clamps_only_after_the_clock_went_back(self):
        """The clamping pass runs only when the clock reads earlier than a time it gave before (S5)."""
        with mock.patch.object(self.store, "_clamp", wraps=self.store._clamp) as clamp:
            self.store.expire()
            self.store.store(KEY, ident(1))
            self.store.expire()
            self.clock.now += 60
            self.store.expire()
            self.assertEqual(clamp.call_count, 0)
            self.clock.now -= 100  # corrected backwards: the record was stored in the future
            self.store.expire()
            self.assertEqual(clamp.call_count, 1)
            self.assertEqual(self.store.first_seen(ident(1)), self.clock.now)
            self.store.expire()  # every time is now at most the clock's: nothing more to clamp
            self.clock.now += 5
            self.store.expire()
            self.assertEqual(clamp.call_count, 1)

    def test_retain_drops_records_and_standing(self):
        """retain() removes the records of publishers keep rejects, with their standing; this node's stay."""
        store = self.new_store(own_id=OWN)
        self.earn(store, ident(1), 3, 100)
        self.fill(store, ident(2), 2, 200)
        self.fill(store, OWN, 2, 300)
        asked = []

        def keep(publisher):
            """Keeps ident(2) only."""
            asked.append(publisher)
            return publisher == ident(2)

        self.assertEqual(store.retain(keep), 3)
        self.assertEqual(sorted(asked), [ident(1), ident(2)])
        self.assertIsNone(store.standing(ident(1)))  # its refresh days too
        self.assertEqual(store.record_count(), 4)
        self.assertEqual(store.providers(ident(300)), [OWN])


class ReplyTest(StoreTestCase):
    """The /find reply: the K/2 oldest records, then random ones."""

    def setUp(self):
        """20 providers of KEY, stored a second apart."""
        super().setUp()
        for i in range(20):
            self.store.store(KEY, ident(100 + i))
            self.clock.now += 1

    def test_few_providers_all_listed_oldest_first(self):
        self.store.store(ident(1), ident(2))
        self.clock.now += 1
        self.store.store(ident(1), ident(1))
        self.assertEqual(self.store.for_reply(ident(1)), [ident(2), ident(1)])

    def test_oldest_half_then_random_half(self):
        reply = self.store.for_reply(KEY)
        self.assertEqual(len(reply), K)
        self.assertEqual(reply[:K // 2], [ident(100 + i) for i in range(K // 2)])
        self.assertEqual(len(set(reply)), K)
        replies = {tuple(self.store.for_reply(KEY)[K // 2:]) for _ in range(20)}
        self.assertGreater(len(replies), 1)  # the random half varies

    def test_banned_and_expired_left_out(self):
        self.clock.now += PROVIDER_TTL - 10  # the first 10 have expired
        reply = self.store.for_reply(KEY, exclude=lambda p: p == ident(110))
        self.assertEqual(reply[:K // 2], [ident(111 + i) for i in range(K // 2)])
        self.assertEqual(len(reply), 9)

    def test_squatters_cannot_hold_every_slot(self):
        """Later providers still get into the random half of replies."""
        late = ident(999)
        self.clock.now += DAY
        self.store.store(KEY, late)
        self.assertTrue(any(late in self.store.for_reply(KEY) for _ in range(50)))

    def test_reply_asks_exclude_only_about_chosen_candidates(self):
        asked = []
        self.store.for_reply(KEY, exclude=lambda p: asked.append(p) or False)
        self.assertEqual(len(asked), K)


class FreeStoreTest(StoreTestCase):
    """While there is room, anyone stores freely: no per-publisher caps, no newcomer pool."""

    capacity = 100

    def test_one_publisher_may_fill_the_store(self):
        self.fill(self.store, ident(1), 100, 1000)
        self.assertTrue(self.store.is_full())
        self.assertEqual(self.store.tier_counts(), {"newcomer": 100, "earned": 0, "known": 0, "own": 0})

    def test_a_flood_of_identities_stores_while_there_is_room(self):
        for i in range(99):
            self.assertTrue(self.store.store(ident(1000 + i), ident(5000 + i)))
        self.assertFalse(self.store.is_full())
        self.assertEqual(self.store.publisher_count(), 99)

    def test_newcomers_live_the_whole_ttl_while_not_full(self):
        """The 4.5-day expiry of Newcomers' records applies only while the store is full."""
        self.fill(self.store, ident(1), 10, 1000)
        self.clock.now += NEWCOMER_TTL_FULL + DAY
        self.assertEqual(self.store.expire(), 0)
        self.assertEqual(len(self.store.providers(ident(1000))), 1)
        self.store.maintain()
        self.assertEqual(self.store.record_count(), 10)


class KeyCapTest(StoreTestCase):
    """A key keeps at most max_per_key publishers: a newcomer to a full key evicts one of its lowest tier, never one
    of the K/2 oldest records, and never this node's own record."""

    def setUp(self):
        """A store of 20 publishers per key, recording OWN as this node."""
        super().setUp()
        self.store = self.new_store(max_per_key=20, own_id=OWN)

    def full_key(self, tiers: dict[int, str]) -> list[bytes]:
        """KEY held by 20 publishers, ident(100 + i) for i in tiers made "earned" or "known" (the rest Newcomers),
        the earned ones first; the publishers, oldest record first."""
        publishers = [ident(100 + i) for i in range(20)]
        earned = [p for i, p in enumerate(publishers) if tiers.get(i) == "earned"]
        for n, publisher in enumerate(earned):
            self.fill(self.store, publisher, 1, 1000 + 10 * n)  # their own keys, refreshed to earn
        for _ in range(EARNED_DAYS):
            self.clock.now += DAY
            for n, publisher in enumerate(earned):
                self.store.store(ident(1000 + 10 * n), publisher)
        self.known.update(p for i, p in enumerate(publishers) if tiers.get(i) == "known")
        for publisher in publishers:
            self.assertTrue(self.store.store(KEY, publisher))
            self.clock.now += 1
        return publishers

    def test_newcomer_to_a_key_of_newcomers_evicts_a_random_non_senior_one(self):
        publishers = self.full_key({})
        evicted = set()
        for n in range(30):
            self.assertTrue(self.store.store(KEY, ident(500 + n)))
            evicted |= set(publishers) - set(self.store.providers(KEY))
        self.assertEqual(len(self.store.providers(KEY)), 20)
        self.assertEqual(self.store.providers(KEY)[:K // 2], publishers[:K // 2])  # the senior half stays
        self.assertGreater(len(evicted), 1)

    def test_lowest_tier_gives_way_first(self):
        """Earned and Known publishers of a full key stay while Newcomers are there to give way, and the senior half
        (here Newcomers) stays too."""
        publishers = self.full_key({i: "earned" for i in range(10, 15)} | {i: "known" for i in range(15, 20)})
        for n in range(30):
            self.assertTrue(self.store.store(KEY, ident(500 + n)))
        held = self.store.providers(KEY)
        self.assertEqual(len(held), 20)
        self.assertEqual(held[:K // 2], publishers[:K // 2])
        self.assertLessEqual(set(publishers[10:]), set(held))
        self.assertIn(ident(529), held)  # the last newcomer got in

    def test_earned_publishers_of_a_full_key_never_give_way_to_each_other(self):
        """A key full of Earned publishers (past its senior half) refuses Newcomers and Earned ones alike; a Known
        publisher evicts one."""
        publishers = self.full_key({i: "earned" for i in range(5, 20)})
        self.assertFalse(self.store.store(KEY, ident(500)))  # a Newcomer
        newcomer_earned = ident(501)
        self.earn(self.store, newcomer_earned, 1, 2000)
        self.assertFalse(self.store.store(KEY, newcomer_earned))
        self.known.add(ident(502))
        self.assertTrue(self.store.store(KEY, ident(502)))
        self.assertEqual(len(set(publishers[5:]) - set(self.store.providers(KEY))), 1)

    def test_own_record_is_not_counted_and_never_evicted(self):
        self.full_key({})
        self.assertTrue(self.store.store(KEY, OWN))
        self.assertEqual(len(self.store.providers(KEY)), 21)
        for n in range(100):
            self.assertTrue(self.store.store(KEY, ident(500 + n)))
        self.assertIn(OWN, self.store.providers(KEY))
        self.assertEqual(len(self.store.providers(KEY)), 21)

    def test_expired_records_make_room(self):
        self.full_key({})
        self.clock.now += PROVIDER_TTL
        self.assertTrue(self.store.store(KEY, ident(500)))
        self.assertEqual(self.store.providers(KEY), [ident(500)])


class TierTest(StoreTestCase):
    """Once the budget is full, the tiers decide who makes room (spec section 6; the module's docstring). The
    store holds 100 records: floors of 10 (Newcomer), 60 (Earned) and 30 (Known)."""

    capacity = 100

    def test_refresh_days_make_a_publisher_earned(self):
        """Refreshes on EARNED_DAYS distinct UTC days after the first store's; refreshes on one day count once."""
        publisher = ident(1)
        self.fill(self.store, publisher, 2, 100)
        for _ in range(5):
            self.clock.now += 60
            self.store.store(ident(100), publisher)  # the day of the first store doesn't count
        self.assertEqual(self.store.standing(publisher)["days"], 0)
        for day in range(1, EARNED_DAYS + 1):
            self.clock.now += DAY
            self.store.store(ident(100), publisher)
            self.store.store(ident(101), publisher)
            self.assertEqual(self.store.standing(publisher)["days"], day)
        self.assertEqual(self.store.standing(publisher)["tier"], "earned")
        self.assertEqual(self.store.tier_counts()["earned"], 2)
        # A new key isn't a refresh: it holds no record of it to refresh
        self.assertEqual(self.store.standing(ident(2)), None)

    def test_known_publishers(self):
        self.known.add(ident(1))
        self.fill(self.store, ident(1), 3, 100)
        self.assertEqual(self.store.standing(ident(1))["tier"], "known")
        self.assertEqual(self.store.tier_counts()["known"], 3)

    def test_newcomers_evict_the_largest_newcomer(self):
        """A full store of Newcomers: a new one evicts a record of the one holding the most; one holding as many as
        the largest is refused."""
        self.fill(self.store, ident(1), 60, 1000)
        self.fill(self.store, ident(2), 40, 2000)
        self.assertTrue(self.store.store(ident(3000), ident(3)))
        self.assertEqual((self.held_by(self.store, ident(1)), self.held_by(self.store, ident(2))), (59, 40))
        self.assertFalse(self.store.store(ident(1999), ident(1)))  # the largest: no churning its own records
        self.assertTrue(self.store.store(ident(2999), ident(2)))  # 40 < 59
        self.assertEqual(self.held_by(self.store, ident(1)), 58)
        # The record it evicts is the one refreshed least recently
        self.assertNotIn(ident(1), self.store.providers(ident(1000)))
        self.assertIn(ident(1), self.store.providers(ident(1059)))

    def test_a_flood_of_newcomers_evens_out_the_newcomers(self):
        """Fresh identities storing into a full store of Newcomers churn the largest holders down to their own
        level, a record at a time: an honest newcomer keeps as many as any of them."""
        honest = ident(1)
        self.fill(self.store, honest, 50, 1000)
        self.fill(self.store, ident(2), 50, 2000)
        for n in range(500):
            self.store.store(ident(10_000 + n), ident(5000 + n % 50))
        self.assertTrue(self.store.is_full())
        held = [self.held_by(self.store, ident(5000 + n)) for n in range(50)]
        self.assertGreaterEqual(self.held_by(self.store, honest), min(held))

    def test_earned_storer_takes_from_newcomers_above_their_floor(self):
        earned = ident(1)
        self.earn(self.store, earned, 50, 1000)
        self.fill(self.store, ident(2), 50, 2000)  # Newcomers at 50, over their floor of 10
        for n in range(40):
            self.assertTrue(self.store.store(ident(3000 + n), earned))
        self.assertEqual(self.store.tier_counts(), {"newcomer": 10, "earned": 90, "known": 0, "own": 0})
        # At their floor: Earned can't take more from them, nor from each other
        self.assertFalse(self.store.store(ident(3999), earned))

    def test_earned_publishers_never_evict_each_other(self):
        """However many Earned identities a flood has, they take no records from other Earned publishers."""
        honest = ident(1)
        self.earn(self.store, honest, 60, 1000)
        flooders = [ident(5000 + n) for n in range(30)]
        for n, flooder in enumerate(flooders):
            self.store.store(ident(10_000 + n), flooder)
        for _ in range(EARNED_DAYS):
            self.clock.now += DAY
            for n, flooder in enumerate(flooders):
                self.store.store(ident(10_000 + n), flooder)
        self.fill(self.store, ident(2), 10, 2000)  # Newcomers, at their floor
        self.assertTrue(self.store.is_full())
        refused = sum(not self.store.store(ident(20_000 + n), flooders[n % 30]) for n in range(300))
        self.assertEqual(refused, 300)
        self.assertEqual(self.held_by(self.store, honest), 60)

    def test_newcomers_below_their_floor_take_from_earned_above_its_own(self):
        """Floors hold in both directions: Earned growth can't take the Newcomers' floor for good."""
        earned = ident(1)
        self.earn(self.store, earned, 95, 1000)
        self.fill(self.store, ident(2), 5, 2000)
        self.assertTrue(self.store.is_full())
        for n in range(5):
            self.assertTrue(self.store.store(ident(3000 + n), ident(3)))
        self.assertEqual(self.store.tier_counts()["earned"], 90)
        self.assertEqual(self.store.tier_counts()["newcomer"], 10)
        # At its floor now: further Newcomers take from the largest Newcomer only
        self.assertTrue(self.store.store(ident(3999), ident(4)))
        self.assertEqual(self.store.tier_counts()["earned"], 90)
        self.assertEqual(self.held_by(self.store, ident(2)), 4)

    def test_earned_below_its_floor_takes_from_known_above_its_own(self):
        self.known.add(ident(9))
        earned = ident(1)
        self.earn(self.store, earned, 45, 1000)
        self.fill(self.store, ident(9), 50, 9000)
        self.fill(self.store, ident(2), 5, 2000)  # Newcomers below their floor: not taken from
        self.assertTrue(self.store.store(ident(3000), earned))
        self.assertEqual(self.store.tier_counts(), {"newcomer": 5, "earned": 46, "known": 49, "own": 0})
        for n in range(1, 30):
            self.store.store(ident(3000 + n), earned)
        # Up to Earned's floor only: then within Earned, which never evicts
        self.assertEqual(self.store.tier_counts(), {"newcomer": 5, "earned": 60, "known": 35, "own": 0})
        self.assertFalse(self.store.store(ident(3999), earned))

    def test_known_storer_takes_from_the_lowest_tier_above_its_floor(self):
        known = ident(9)
        self.known.add(known)
        self.earn(self.store, ident(1), 70, 1000)
        self.fill(self.store, ident(2), 30, 2000)
        for n in range(20):
            self.assertTrue(self.store.store(ident(9000 + n), known))
        self.assertEqual(self.store.tier_counts(), {"newcomer": 10, "earned": 70, "known": 20, "own": 0})
        for n in range(20, 30):
            self.assertTrue(self.store.store(ident(9000 + n), known))
        self.assertEqual(self.store.tier_counts(), {"newcomer": 10, "earned": 60, "known": 30, "own": 0})
        # Every tier at its floor: within Known, from its largest holder, which may not churn its own records
        self.assertFalse(self.store.store(ident(9999), known))
        other = ident(8)
        self.known.add(other)
        self.assertTrue(self.store.store(ident(8000), other))
        self.assertEqual(self.held_by(self.store, known), 29)

    def test_a_tier_changed_since_written_is_found_when_evicting(self):
        """A publisher that became Known (a contact added) after its records were counted as a Newcomer's isn't
        evicted as a Newcomer: it is moved to Known when picked."""
        self.fill(self.store, ident(1), 60, 1000)
        self.fill(self.store, ident(2), 40, 2000)
        self.known.add(ident(1))
        self.assertTrue(self.store.store(ident(3000), ident(3)))
        self.assertEqual(self.held_by(self.store, ident(1)), 60)
        self.assertEqual(self.held_by(self.store, ident(2)), 39)
        self.assertEqual(self.store.tier_counts(), {"newcomer": 40, "earned": 0, "known": 60, "own": 0})

    def test_a_refused_batch_looks_for_room_once(self):
        """Once the full store refuses one key of a /provide, the rest are refused at once; refreshes still count."""
        self.fill(self.store, ident(1), 100, 1000)
        with mock.patch.object(self.store, "_reclaim", wraps=self.store._reclaim) as reclaim:
            stored = self.store.provide(ident(1), keys(5000, 20) + [ident(1000)])
        self.assertEqual((stored, reclaim.call_count), (1, 1))


class NewcomerExpiryTest(StoreTestCase):
    """While the budget is full, a Newcomer's record last stored NEWCOMER_TTL_FULL (4.5 days) ago counts as
    expired; other tiers' records live PROVIDER_TTL."""

    capacity = 20

    def test_only_while_full_and_only_newcomers(self):
        earned = ident(1)
        for key in self.earn(self.store, earned, 5, 1000):
            self.store.store(key, earned)  # all refreshed now
        self.fill(self.store, ident(2), 5, 2000)
        self.clock.now += 2 * DAY
        self.assertEqual(self.store.expire(), 0)  # not full
        self.clock.now += NEWCOMER_TTL_FULL - 2 * DAY
        self.fill(self.store, ident(3), 10, 3000)  # fresh; now full
        self.assertTrue(self.store.is_full())
        self.assertEqual(self.store.expire(), 5)  # ident(2)'s: 4.5 days old
        self.assertEqual(self.held_by(self.store, ident(2)), 0)
        self.assertEqual(self.held_by(self.store, earned), 5)  # as old, but Earned

    def test_a_full_store_expires_before_it_evicts(self):
        """A store that finds itself full sweeps for expired records first: the stale Newcomer goes, not the
        fresh one."""
        self.fill(self.store, ident(2), 10, 2000)
        self.clock.now += NEWCOMER_TTL_FULL
        self.fill(self.store, ident(3), 10, 3000)
        self.assertTrue(self.store.store(ident(4000), ident(4)))
        self.assertEqual(self.held_by(self.store, ident(2)), 0)
        self.assertEqual(self.held_by(self.store, ident(3)), 10)


class OwnRecordsTest(StoreTestCase):
    """This node's records: always accepted, never evicted, in a tier of their own."""

    capacity = 20

    def setUp(self):
        """A store recording OWN as this node."""
        super().setUp()
        self.store = self.new_store(own_id=OWN)

    def test_always_accepted_and_never_evicted(self):
        own = self.fill(self.store, OWN, 30, 1000)  # beyond the budget
        self.assertEqual(self.store.tier_counts()["own"], 30)
        for n in range(100):
            self.store.store(ident(5000 + n), ident(100 + n))
        self.assertEqual([key for key in own if OWN not in self.store.providers(key)], [])
        self.clock.now += NEWCOMER_TTL_FULL
        self.store.expire()
        self.assertEqual(self.store.tier_counts()["own"], 30)

    def test_own_records_take_room_from_others(self):
        self.fill(self.store, OWN, 10, 1000)
        self.fill(self.store, ident(1), 10, 2000)
        self.assertTrue(self.store.is_full())
        self.assertFalse(self.store.store(ident(2999), ident(1)))

    def test_budget_of_nothing_keeps_only_own_records(self):
        store = self.new_store(capacity=0, own_id=OWN)
        self.assertFalse(store.store(KEY, ident(1)))
        self.assertTrue(store.store(KEY, OWN))


class UsageTest(StoreTestCase):
    """usage(), the node's status of the store: the budget's fill, each tier's against its floor, and the records
    refused and evicted over the last hour and day, by tier. The store holds 100 records."""

    capacity = 100

    def setUp(self):
        """A store recording OWN as this node, its recent counts on a monotonic clock of their own."""
        super().setUp()
        self.monotonic = FakeClock(1000.0)
        self.store = self.new_store(own_id=OWN, monotonic=self.monotonic)

    def tier(self, name: str) -> dict:
        """usage()'s entry for the tier name."""
        return self.store.usage()["tiers"][name]

    def recent(self, what: str) -> dict[str, tuple[int, int]]:
        """what ("refused", "evicted") per tier: (last hour, last day)."""
        return {name: (tier[f"{what}_hour"], tier[f"{what}_day"])
                for name, tier in self.store.usage()["tiers"].items()}

    def test_fill_against_the_budget_and_the_floors(self):
        empty = self.store.usage()
        self.assertEqual({key: value for key, value in empty.items() if key != "tiers"},
                         {"bytes": 0, "budget": 100 * SIZE, "capacity": 100, "full": False, "publishers": 0,
                          "own_records": 0})
        self.assertEqual(list(empty["tiers"]), ["known", "earned", "newcomer"])
        self.earn(self.store, ident(2), 50, 2000)
        self.known.add(ident(1))
        self.fill(self.store, ident(1), 30, 1000)
        self.fill(self.store, ident(3), 15, 3000)
        self.fill(self.store, OWN, 5, 4000)
        usage = self.store.usage()
        self.assertEqual({key: value for key, value in usage.items() if key != "tiers"},
                         {"bytes": 100 * SIZE, "budget": 100 * SIZE, "capacity": 100, "full": True, "publishers": 4,
                          "own_records": 5})
        fill = {name: (tier["records"], tier["share"], tier["floor"]) for name, tier in usage["tiers"].items()}
        self.assertEqual(fill, {"known": (30, 0.3, 0.3), "earned": (50, 0.5, 0.6), "newcomer": (15, 0.15, 0.1)})
        self.assertEqual(self.recent("refused"), {"known": (0, 0), "earned": (0, 0), "newcomer": (0, 0)})

    def test_refusals_and_evictions_by_tier_age_out(self):
        self.earn(self.store, ident(2), 50, 2000)
        self.known.add(ident(1))
        self.fill(self.store, ident(1), 30, 1000)
        self.fill(self.store, ident(3), 15, 3000)
        self.fill(self.store, OWN, 5, 4000)
        self.assertTrue(self.store.store(ident(5000), ident(4)))  # a Newcomer evicts the largest Newcomer's record
        for n in range(5):  # Earned takes from the Newcomers down to their floor ...
            self.assertTrue(self.store.store(ident(6000 + n), ident(2)))
        self.assertFalse(self.store.store(ident(6999), ident(2)))  # ... and then may not: refused
        self.assertFalse(self.store.store(ident(3999), ident(3)))  # the largest Newcomer can't churn: refused
        self.assertEqual(self.tier("newcomer")["records"], 10)
        self.assertEqual(self.recent("evicted"), {"known": (0, 0), "earned": (0, 0), "newcomer": (6, 6)})
        self.assertEqual(self.recent("refused"), {"known": (0, 0), "earned": (1, 1), "newcomer": (1, 1)})
        # provide() counts each key of a batch refused for room, also those refused without looking again
        self.assertEqual(self.store.provide(ident(3), keys(7000, 3)), 0)
        self.assertEqual(self.recent("refused")["newcomer"], (4, 4))
        # Refreshes and this node's records are never refused
        self.assertTrue(self.store.store(ident(3010), ident(3)))
        self.assertTrue(self.store.store(ident(8000), OWN))
        self.assertEqual(self.recent("refused")["newcomer"], (4, 4))
        self.monotonic.now += 2 * HOUR
        self.assertEqual(self.recent("evicted")["newcomer"], (0, 6))
        self.assertEqual(self.recent("refused"), {"known": (0, 0), "earned": (0, 1), "newcomer": (0, 4)})
        self.monotonic.now += DAY
        self.assertEqual(self.recent("evicted")["newcomer"], (0, 0))
        self.assertEqual(self.recent("refused")["newcomer"], (0, 0))

    def test_evictions_from_a_full_key_count(self):
        store = self.store = self.new_store(max_per_key=2, monotonic=self.monotonic)
        self.assertTrue(store.store(KEY, ident(1)))
        self.assertTrue(store.store(KEY, ident(2)))
        self.assertTrue(store.store(KEY, ident(3)))  # evicts ident(2), the oldest record staying
        self.assertEqual(self.recent("evicted")["newcomer"], (1, 1))
        self.assertFalse(store.is_full())  # not the budget: the key's cap

    def test_usage_reads_nothing_from_the_database(self):
        """The status is called often by apps: it reports counts kept in memory, with no read of the records."""
        self.fill(self.store, ident(1), 100, 1000)
        self.store.save()
        with mock.patch.object(self.store._db, "query", side_effect=AssertionError("read the database")):
            self.assertTrue(self.store.usage()["full"])


class BudgetTest(StoreTestCase):
    """The budget's measure: bytes per record from the database, and the free disk."""

    def test_bytes_per_record_are_measured(self):
        """Without record_bytes, a record's size is the database's pages over its records (at least
        MEASURE_RECORDS of them), measured at each save: a store whose records outgrow the budget is full."""
        store = self.new_store(record_bytes=None)
        store.record_budget = 1 << 30
        self.assertLess(store._record_bytes, 50)  # an empty database over MEASURE_RECORDS records
        for n in range(12_000):
            store.store(ident(10_000 + n), ident(1 + n % 50))
        store.save()
        size = store._record_bytes
        self.assertGreater(size, 60)
        self.assertLess(size, 400)
        self.assertFalse(store.is_full())
        store.record_budget = int(size * 11_999)
        self.assertTrue(store.is_full())
        self.assertTrue(store.store(ident(1), ident(5000)))
        self.assertEqual(store.record_count(), 12_000)  # it made room rather than grow

    def test_free_disk_bounds_the_store(self):
        """Records never take the disk the blocks' reserve needs: free_space() is the room left beyond it."""
        room = [50 * SIZE]
        store = self.new_store(free_space=lambda: room[0])
        self.fill(store, ident(1), 49, 1000)
        self.assertFalse(store.is_full())
        self.fill(store, ident(2), 1, 2000)
        self.assertTrue(store.is_full())  # measured at the start: 50 records' room
        store.save()  # measured again: 50 more records' room (they took none, here)
        self.assertFalse(store.is_full())
        room[0] = 0
        store.save()
        self.assertTrue(store.is_full())
        room[0] = -SIZE  # below the reserve already: full however few records there are
        store.withdraw(ident(1000), ident(1))
        store.save()
        self.assertTrue(store.is_full())

    def test_a_free_space_error_keeps_the_last_measure(self):
        def failing():
            """The disk can't be asked."""
            raise OSError("gone")

        store = self.new_store(free_space=failing)
        self.assertTrue(store.store(KEY, ident(1)))
        store.save()
        self.assertFalse(store.is_full())


class WriteBudgetTest(StoreTestCase):
    """The write budget (tests/security/test_rev2_provider_write_budget.py has the attack): rows a window by pool,
    here (2, 1, 1), so WRITE_CARRY windows carry 1440 for all and 720 each kept for Earned and Known."""

    def setUp(self):
        """A store with a small budget, on the test's clock for monotonic time too."""
        super().setUp()
        self.store = self.new_store(own_id=OWN, monotonic=self.clock, write_budget=(2, 1, 1))
        self.carried = providers.WRITE_CARRY * 2

    def spend(self, publisher: bytes = ident(0xA)) -> int:
        """publisher stores new keys until one is refused; how many it stored."""
        stored = 0
        while self.store.store(ident((1 << 100) + self.store.record_count()), publisher):
            stored += 1
        return stored

    def test_budget_refills_per_window_up_to_its_carry(self):
        self.assertEqual(self.spend(), self.carried)
        self.clock.now += providers.WRITE_WINDOW
        self.assertEqual(self.spend(), 2)
        self.clock.now += 10 * DAY
        self.assertEqual(self.spend(), self.carried)
        self.assertEqual(self.store.usage()["tiers"]["newcomer"]["refused_hour"], 1)

    def test_own_records_and_refreshes_are_never_refused(self):
        self.spend()
        self.assertTrue(self.store.store(KEY, OWN))
        self.store.withdraw(KEY, OWN)
        self.assertTrue(self.store.store(KEY, OWN))
        self.assertTrue(self.store.store(ident(1 << 100), ident(0xA)))  # its first record, refreshed

    def test_withdrawals_spend_but_are_never_refused(self):
        stored = keys(1 << 100, 100)
        for key in stored:
            self.assertTrue(self.store.store(key, ident(0xB)))
        self.assertEqual(sum(self.store.withdraw(key, ident(0xB)) for key in stored), 100)
        self.assertEqual(self.spend(), self.carried - 200)
        self.store.save()  # stored again after a save, they are new records: refused
        for key in stored:
            self.assertFalse(self.store.store(key, ident(0xB)))
        self.assertTrue(self.store.withdraw(ident(1 << 100), ident(0xA)))

    def test_earned_and_known_spend_their_own_first(self):
        earned = ident(0xE)
        self.earn(self.store, earned, 1, 5000)
        self.known.add(ident(0xC))
        self.assertEqual(self.spend(earned), self.carried + providers.WRITE_CARRY)
        self.assertEqual(self.spend(ident(0xC)), providers.WRITE_CARRY)
        self.assertEqual(self.spend(), 0)

    def test_no_budget(self):
        store = self.new_store(monotonic=self.clock, write_budget=None)
        self.assertEqual(sum(store.store(ident((1 << 100) + n), ident(0xA)) for n in range(3000)), 3000)


class CacheTest(StoreTestCase):
    """Memory doesn't grow with the records or publishers: the caches are bounded, the rest is read per key."""

    def test_caches_stay_bounded(self):
        with mock.patch.multiple(providers, CACHE_KEYS=8, CACHE_ROWS=40, STANDING_CACHE=8):
            store = self.new_store()
            for n in range(400):
                store.store(ident(10_000 + n % 100), ident(1 + n // 4))
            store.save()
            for n in range(100):
                self.assertEqual(len(store.providers(ident(10_000 + n))), 4)
            self.assertLessEqual(len(store._cache), 8)
            self.assertLessEqual(store._cache_rows, 40)
            self.assertLessEqual(len(store._standings), 8)
            self.assertEqual((store._dirty, store._pinned_standings), ({}, {}))
            store._check_consistency()

    def test_changes_waiting_to_be_written_are_bounded(self):
        """Past FLUSH_CHANGES rows waiting, a store writes them itself."""
        with mock.patch.object(providers, "FLUSH_CHANGES", 50):
            store = self.new_store()
            with recorded_writes(store._db) as transactions:
                for n in range(100):
                    store.store(ident(10_000 + n), ident(1))
            self.assertGreaterEqual(len(transactions), 1)
            self.assertLess(store._changes(), 50)


class PersistenceTest(StoreTestCase):
    """Records and standings on disk: batched writes, restarts, failures."""

    def setUp(self):
        """A temporary state directory (removed after the stores on it are closed)."""
        super().setUp()
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.state = os.path.join(directory.name, "state")

    def open(self, **settings) -> ProviderStore:
        """A store on self.state."""
        return self.new_store(state_dir=self.state, **settings)

    def test_round_trip(self):
        """Records, first-seen times and refresh days are saved, and read by the next store."""
        store = self.open(own_id=OWN)
        self.earn(store, ident(1), 2, 100)
        self.fill(store, ident(2), 1, 200)
        self.fill(store, OWN, 1, 300)
        store.save()
        expected = held(store)
        self.assertEqual(saved(self.state), expected)
        store.close()
        loaded = self.open(own_id=OWN)
        self.assertEqual(held(loaded), expected)
        self.assertEqual(loaded.standing(ident(1)), store.standing(ident(1)))
        self.assertEqual(loaded.tier_counts(), {"newcomer": 1, "earned": 2, "known": 0, "own": 1})
        self.assertEqual(loaded.providers(ident(100)), [ident(1)])
        self.assertEqual((loaded.record_count(), loaded.publisher_count()), (4, 3))

    def test_stores_write_nothing_until_saved(self):
        """Stores and withdrawals change memory only; a save writes them in one transaction: each record, and each
        publisher's standing and first-seen time."""
        store = self.open()
        with recorded_writes(store._db) as transactions:
            for n in range(10):
                store.store(ident(100 + n), ident(1 + n % 2))
            store.withdraw(ident(100), ident(1))
            self.assertEqual(transactions, [])
            store.save()
        self.assertEqual(len(transactions), 1)
        self.assertEqual(rows_written(transactions), 10 + 2 + 2)
        self.assertEqual(saved(self.state), held(store))

    def test_refreshes_are_written_only_past_six_hours(self):
        store = self.open()
        for n in range(100):
            store.store(ident(100 + n), ident(1))
        store.save()
        with recorded_writes(store._db) as transactions:
            self.clock.now += 6 * HOUR
            for n in range(100):
                store.store(ident(100 + n), ident(1))
            store.save()
            self.assertEqual(transactions, [])
            self.clock.now += 60
            store.store(ident(100), ident(1))
            store.save()
        self.assertEqual(rows_written(transactions), 1)

    def test_a_new_refresh_day_writes_the_standing(self):
        store = self.open()
        store.store(KEY, ident(1))
        store.save()
        with recorded_writes(store._db) as transactions:
            self.clock.now += DAY
            store.store(KEY, ident(1))
            store.save()
        self.assertEqual(rows_written(transactions), 2)  # the record and the standing
        self.assertEqual(saved(self.state)["standing"][ident(1)][2], 1)

    def test_a_refusal_writes_nothing(self):
        store = self.open()
        store.record_budget = 10 * SIZE
        self.fill(store, ident(1), 10, 100)
        store.save()
        with recorded_writes(store._db) as transactions:
            for n in range(50):
                self.assertFalse(store.store(ident(1000 + n), ident(1)))  # the largest Newcomer
            self.assertEqual(store.provide(ident(1), keys(2000, 20)), 0)
            store.save()
        self.assertEqual(transactions, [])

    def test_save_only_when_changed(self):
        store = self.open()
        with recorded_writes(store._db) as transactions:
            store.save()
            self.assertEqual(transactions, [])
            store.store(KEY, ident(1))
            store.save()
            store.save()
        self.assertEqual(len(transactions), 1)

    def test_failed_save_is_tried_again(self):
        store = self.open()
        store.store(KEY, ident(1))
        with failing_commits():
            with self.assertRaises(OSError):
                store.save()
        self.assertEqual(store.providers(KEY), [ident(1)])
        store.save()
        store.close()
        self.assertEqual(self.open().providers(KEY), [ident(1)])

    def test_failing_writes_bound_the_changes_held(self):
        """While saves fail, others' new records are refused past MAX_CHANGES (memory stays bounded); this node's and
        refreshes still go through."""
        store = self.open(own_id=OWN)
        with mock.patch.multiple(providers, FLUSH_CHANGES=20, MAX_CHANGES=40), failing_commits():
            results = [store.store(ident(100 + n), ident(1 + n)) for n in range(40)]
            self.assertFalse(all(results))
            self.assertTrue(store.store(ident(9), OWN))
            self.assertTrue(store.store(ident(100), ident(1)))
        store.save()
        self.assertEqual(saved(self.state), held(store))

    def test_future_times_clamped_on_load(self):
        store = self.open()
        self.clock.now += DAY
        store.store(KEY, ident(1))
        store.save()
        store.close()
        self.clock.now -= DAY
        loaded = self.open()
        self.assertEqual(loaded.record(KEY, ident(1)), (self.clock.now, self.clock.now))
        self.assertEqual(loaded.first_seen(ident(1)), self.clock.now)
        loaded.save()
        self.assertEqual(saved(self.state)["records"][(KEY, ident(1))], (self.clock.now, self.clock.now, 0))

    def test_malformed_rows_are_ignored_and_deleted(self):
        """Records of the wrong type or length, or of a publisher with no standing, read as nothing; the next save
        deletes them, keeping the rest and the counts right."""
        store = self.open()
        store.store(KEY, ident(1))
        store.store(KEY, ident(2))
        store.save()
        store.close()
        records = ("INSERT INTO provider_records (key, publisher, first_stored, last_stored, pooled)"
                   " VALUES (?, ?, ?, ?, ?)")
        now = self.clock.now
        for row in ((KEY, b"short", now, now, 0), (KEY, "text publisher 1", now, now, 0),
                    (KEY, ident(3), now, now, 0)):  # ident(3) has no standing
            state_execute(self.state, records, row)
        state_execute(self.state, "UPDATE provider_records SET first_stored = 'yesterday' WHERE publisher = ?",
                      (ident(2),))
        loaded = self.open()
        self.assertEqual(loaded.providers(KEY), [ident(1)])
        self.assertEqual((loaded.record_count(), loaded.publisher_count()), (1, 1))
        loaded.save()
        self.assertEqual(saved(self.state), held(loaded))
        self.assertEqual(list(saved(self.state)["records"]), [(KEY, ident(1))])

    def test_corrupt_legacy_file_starts_empty(self):
        """providers.json that isn't JSON is moved aside, and the store starts empty."""
        write_legacy(self.state, "providers.json", text="{not json")
        self.assertEqual(self.open().record_count(), 0)
        self.assertTrue(os.path.exists(os.path.join(self.state, "providers.json.damaged")))

    def test_random_operations_are_saved_and_loaded_exactly(self):
        """Many random stores, refreshes over days, withdrawals, batches, expiries, retains and saves (some failing)
        with the budget, its tiers and the key cap at work: a store loaded from the tables holds exactly what the
        first one holds, and answers the same."""
        rng = random.Random(7)
        settings = {"max_per_key": 6, "own_id": OWN}
        with mock.patch.multiple(providers, CACHE_KEYS=5, CACHE_ROWS=20, STANDING_CACHE=5):
            store = self.open(**settings)
            store.record_budget = 60 * SIZE
            publishers = [ident(1 + n) for n in range(40)] + [OWN]
            self.known.update(publishers[:4])
            all_keys = keys(1000, 30)
            for step in range(3000):
                action = rng.random()
                publisher = rng.choice(publishers)
                if action < 0.6:
                    store.store(rng.choice(all_keys), publisher)
                elif action < 0.7:
                    store.withdraw(rng.choice(all_keys), publisher)
                elif action < 0.8:
                    store.provide(publisher, rng.sample(all_keys, 4), rng.sample(all_keys, 2))
                elif action < 0.9:
                    self.clock.now += rng.choice((60, HOUR, DAY / 2, DAY))
                elif action < 0.93:
                    store.expire()
                elif action < 0.94:
                    store.retain(lambda p: p != rng.choice(publishers))
                elif action < 0.97:
                    if rng.random() < 0.2 and store._changes():
                        with failing_commits(), self.assertRaises(OSError):
                            store.save()
                    else:
                        store.save()
                else:
                    store.maintain()
                if step % 500 == 0:
                    store._check_consistency()
            store.save()
            expected = held(store)
            self.assertEqual(saved(self.state), expected)
            loaded = ProviderStore(self.state, store.record_budget, clock=self.clock, record_bytes=SIZE,
                                   known=self.known.__contains__, **settings)
            self.addCleanup(loaded.close)
            self.stores.append(loaded)
            self.assertEqual(held(loaded), expected)
            for key in all_keys:
                self.assertEqual(loaded.providers(key), store.providers(key))
            self.assertEqual(loaded.tier_counts(), store.tier_counts())
            self.assertGreater(store.tier_counts()["earned"] + store.tier_counts()["known"], 0)


class UpgradeTest(unittest.TestCase):
    """A state database of 0.1.6 (its schema steps only) holding provider rows, opened by this version: the schema
    step builds each publisher's standing, and the store reads them as it left them."""

    NOW = T0

    def setUp(self):
        """A database with 0.1.6's schema and provider rows, as 0.1.6 saved them."""
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.state = os.path.join(directory.name, "state")
        from tests.test_statedb import RELEASED_0_1_6_STEPS
        with mock.patch.object(statedb, "SCHEMA", statedb.SCHEMA[:RELEASED_0_1_6_STEPS]):
            db = statedb.connect(self.state)
        now = self.NOW
        with db.transaction() as tx:
            tx.executemany("INSERT INTO provider_publishers (publisher, first_seen) VALUES (?, ?)", [
                (ident(1), now - 10 * DAY),  # refreshing for days: Earned
                (ident(2), now - 10 * DAY),  # stored long ago, never refreshed
                (ident(3), now - DAY),  # refreshed, but first seen a day ago
                (ident(4), now - 10 * DAY),  # refreshed for days, but in 0.1.x's newcomer pool
                (OWN, now - 20 * DAY)])
            tx.executemany("INSERT INTO provider_records (key, publisher, first_stored, last_stored, pooled)"
                           " VALUES (?, ?, ?, ?, ?)", [
                               (ident(100), ident(1), now - 10 * DAY, now - HOUR, 0),
                               (ident(101), ident(1), now - HOUR, now - HOUR, 0),
                               (ident(100), ident(2), now - 6 * DAY, now - 6 * DAY, 0),
                               (ident(102), ident(3), now - DAY, now - 60, 0),
                               (ident(103), ident(4), now - 9 * DAY, now - 60, 1),
                               (ident(104), OWN, now - 20 * DAY, now - 60, 0)])
            tx.execute("INSERT INTO provider_departed (publisher, first_seen, at) VALUES (?, ?, ?)",
                       (ident(5), now - 9 * DAY, now - DAY))
            tx.execute("INSERT INTO provider_evicted (publisher, first_seen, at) VALUES (?, ?, ?)",
                       (ident(6), now - DAY, now - HOUR))
        db.close()

    def test_standing_is_built_and_old_memories_go(self):
        store = ProviderStore(self.state, clock=FakeClock(self.NOW), own_id=OWN)
        self.addCleanup(store.close)
        tables = saved(self.state)
        today = int((self.NOW - HOUR) // DAY)
        self.assertEqual(tables["standing"], {ident(1): (2, 1, 3, today), ident(2): (1, 0, 0, today - 6),
                                              ident(3): (1, 0, 0, today), ident(4): (1, 0, 0, today),
                                              OWN: (1, 1, 3, today)})  # this node's tier is set by its store
        self.assertEqual({pooled for _, _, pooled in tables["records"].values()}, {0})
        self.assertEqual((tables["departed"], tables["evicted"]), ({}, {}))
        self.assertEqual(store.standing(ident(1))["tier"], "earned")
        self.assertEqual(store.tier_counts(), {"newcomer": 3, "earned": 2, "known": 0, "own": 1})
        self.assertEqual(store.first_seen(ident(2)), self.NOW - 10 * DAY)
        self.assertEqual(store.providers(ident(100)), [ident(1), ident(2)])
        store.save()
        self.assertEqual(saved(self.state)["standing"][OWN][1], providers.OWN)
        store._check_consistency()


class ThreadSafetyTest(StoreTestCase):
    """Concurrent stores, withdrawals, replies and saves."""

    capacity = 200

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
                    elif action < 0.98:
                        self.store.for_reply(key)
                    else:
                        self.store.save()
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
