"""REV2: with no per-publisher cap left, one identity could store and withdraw provider records in a loop.

ProviderStore.provide recorded every new key of a /provide while the store had room, and applied every withdrawal,
so one identity sending batches of new keys, withdrawing those it had stored a save earlier, made the store write
two rows (an insert, then a delete) per key it sent: measured in-process about 100,000 row changes a second (22 MB/s
of writes), and at 100 requests a second, a rate RNS carries, about 8,400 row changes and 9.5 MB a second into a
store of 165,000 records. On a Raspberry Pi's SD card that wears the card out in days.

Rebuilt here with a memory-only store on a fake clock (wall and monotonic time together), the node's save every
30 s (maintain) and the rows each save writes to provider_records counted (test_providers.recorded_writes): one
Newcomer sending up to 10 requests a second for two hours, each filled with the withdrawals of keys it stored in
earlier windows and then new keys. The fix: new records get a write budget per save interval, with shares kept for
Earned and Known publishers, and past it new records are refused (the existing False); withdrawals count against it
but are never refused. So the rows written stay within twice the budget (each record inserted is deleted at most
once), whatever the requests. Further tests: the shares kept for Earned and Known publishers; a withdrawal followed
by a store of the same key and publisher before the next save is a refresh (no new record, first-stored time kept);
a refresh is written only once its last-stored time moved 6 h; and honest 0.1.x publishers (republishing every 24 h
or every 3 days, adding and withdrawing some keys, a fresh one publishing 20,000 keys at a 0.1.x node's top rate)
are never refused while the store isn't flooded.
"""
import random
import unittest

from rnfs.constants import DAY, HOUR, ID_LENGTH
from rnfs.dht.providers import EARNED_DAYS, ProviderStore
from tests.support import FakeClock
from tests.test_providers import recorded_writes

T0 = 19676 * DAY + HOUR  # 01:00 UTC
SIZE = 100  # bytes a record takes here
CAPACITY = 10_000_000  # records: the store never fills in these tests
WINDOW = 30.0  # seconds between the node's saves (RECORDS_INTERVAL)
BATCH = 55  # keys and withdrawals one /provide carries at most
ATTACKER = bytes([0xA7] * ID_LENGTH)


def ident(n: int) -> bytes:
    """The 16-byte ID whose big-endian value is n."""
    return n.to_bytes(ID_LENGTH, "big")


def record_rows(transactions: list[list]) -> int:
    """Rows of provider_records the recorded transactions inserted, replaced or deleted."""
    return sum(len(rows) for statements in transactions for sql, rows in statements if "provider_records" in sql)


class WriteBudgetTest(unittest.TestCase):
    """A store far from full, on one fake clock for wall and monotonic time."""

    def setUp(self):
        """The clock and the store."""
        self.clock = FakeClock(T0)
        self.known: set[bytes] = set()
        self.store = ProviderStore(None, CAPACITY * SIZE, clock=self.clock, monotonic=self.clock,
                                   rng=random.Random(1), record_bytes=SIZE, known=self.known.__contains__)
        self.addCleanup(self.store.close)
        self.next_key = 1 << 100

    def new_keys(self, count: int) -> list[bytes]:
        """count keys never used before."""
        start = self.next_key
        self.next_key += count
        return [ident(start + i) for i in range(count)]

    def earn(self, publisher: bytes) -> bytes:
        """Make publisher Earned (a record stored, then refreshed on EARNED_DAYS later days); its key."""
        key = self.new_keys(1)[0]
        self.assertTrue(self.store.store(key, publisher))
        for _ in range(EARNED_DAYS):
            self.clock.now += DAY
            self.assertTrue(self.store.store(key, publisher))
        self.assertEqual(self.store.standing(publisher)["tier"], "earned")
        return key

    def drain(self, publisher: bytes = ATTACKER) -> int:
        """publisher sends batches of new keys at once until one is refused outright; how many it stored."""
        stored = 0
        while True:
            recorded = self.store.provide(publisher, self.new_keys(BATCH))
            stored += recorded
            if recorded == 0:
                return stored
            self.assertLess(stored, 200_000, "no write budget: the store records every new key")

    def test_store_and_withdraw_loop_is_bounded(self):
        """One identity, up to 10 requests a second for two hours, each withdrawing keys it stored in an earlier
        window and storing new ones: the rows written to provider_records stay within twice a small budget."""
        withdrawable: list[bytes] = []  # keys the attacker stored in earlier windows (saved since)
        written = 0
        late = 0  # rows written in the second hour
        windows = int(2 * HOUR / WINDOW)
        with recorded_writes(self.store._db) as transactions:
            for window in range(windows):
                stored_now: list[bytes] = []
                for _ in range(int(10 * WINDOW)):
                    withdrawn = withdrawable[:BATCH]
                    del withdrawable[:BATCH]
                    keys = self.new_keys(BATCH - len(withdrawn))
                    recorded = self.store.provide(ATTACKER, keys, withdrawn)
                    stored_now += keys[:recorded]  # a batch is stored in order, up to the first refusal
                    if recorded < len(keys):
                        break  # refused: an attacker gains nothing by repeating it at once
                withdrawable += stored_now
                self.clock.now += WINDOW
                self.store.maintain()
                rows = record_rows(transactions)
                transactions.clear()
                written += rows
                if window >= windows // 2:
                    late += rows
                elapsed = (window + 1) * WINDOW
                # REV2: fails if one identity's loop isn't bounded by a write budget
                self.assertLessEqual(written, 2 * (50_000 + 3 * elapsed),
                                     f"{written} record rows written in {elapsed:.0f} s by one identity")
        # Once any budget carried over is spent, the sustained rate: under 3 new records a second, and their deletes
        self.assertLessEqual(late, 2 * 3 * HOUR, f"{late} record rows written in the second hour")

    def test_earned_and_known_keep_their_shares(self):
        """A Newcomer flood spends what every tier may use; Earned and Known publishers still store new records
        from the shares kept for them, while another Newcomer is refused."""
        earned, known = ident(0xE), ident(0xC)
        self.earn(earned)
        self.known.add(known)
        self.drain()
        # REV2: fails if a Newcomer flood leaves room for other Newcomers' new records (no budget)
        self.assertEqual(self.store.provide(ident(0x5), self.new_keys(1)), 0, "a Newcomer stored past the budget")
        self.assertEqual(self.store.provide(earned, self.new_keys(10)), 10, "the Earned share was spent")
        self.assertEqual(self.store.provide(known, self.new_keys(10)), 10, "the Known share was spent")
        # Refreshes of records held are never refused
        key = self.new_keys(1)[0]
        self.clock.now += DAY  # every share refilled
        self.assertTrue(self.store.store(key, ident(0x6)))
        self.drain()
        self.clock.now += DAY / 2
        self.assertTrue(self.store.store(key, ident(0x6)))

    def test_withdraw_then_store_before_a_save_is_a_refresh(self):
        """A publisher that withdraws a key and stores it again before the next save refreshes its record: it keeps
        its first-stored time and isn't a new record, so a spent budget doesn't refuse it."""
        honest = ident(0x48)
        key = self.new_keys(1)[0]
        self.assertTrue(self.store.store(key, honest))
        self.store.save()
        self.clock.now += DAY
        self.drain()
        self.assertEqual(self.store.provide(honest, [key], [key]), 1)
        # REV2: fails if a withdrawal and a store before the next save make a new record
        self.assertEqual(self.store.record(key, honest), (T0, self.clock.now))
        self.store.withdraw(key, honest)
        self.assertTrue(self.store.store(key, honest))
        self.assertEqual(self.store.record(key, honest)[0], T0)

    def test_refresh_written_once_moved_six_hours(self):
        """A refresh writes its record only once it moves the last-stored time by more than 6 h."""
        publisher = ident(0x50)
        key = self.new_keys(1)[0]
        self.assertTrue(self.store.store(key, publisher))
        self.store.save()
        with recorded_writes(self.store._db) as transactions:
            self.clock.now += 2 * HOUR
            self.assertTrue(self.store.store(key, publisher))
            self.store.save()
            # REV2: fails if a refresh is written once it moved an hour
            self.assertEqual(record_rows(transactions), 0, "a refresh 2 h later was written")
            self.assertEqual(self.store.record(key, publisher), (T0, T0))
            self.clock.now = T0 + 6 * HOUR + 1
            self.assertTrue(self.store.store(key, publisher))
            self.store.save()
            self.assertEqual(record_rows(transactions), 1)
        self.assertEqual(self.store.record(key, publisher), (T0, T0 + 6 * HOUR + 1))

    def test_honest_01x_publishers_are_never_refused(self):
        """0.1.x publishers new to this server, unflooded: a fast one first publishing 20,000 keys one a request at
        3.3 keys a second (a 0.1.x round's top rate), then republishing every 24 h and a slow one every 3 days,
        each adding keys and withdrawing some at each republish, over three weeks."""
        fast, slow = ident(0xF), ident(0x51)
        held = {fast: self.new_keys(20_000), slow: self.new_keys(5_000)}
        for key in held[fast]:
            self.assertTrue(self.store.store(key, fast), "the fast publisher's first round was refused")
            self.clock.now += 0.3
        for key in held[slow]:
            self.assertTrue(self.store.store(key, slow), "the slow publisher's first round was refused")
        rng = random.Random(2)
        for day in range(1, 22):
            self.clock.now = T0 + day * DAY
            for publisher, interval in ((fast, 1), (slow, 3)):
                if day % interval:
                    continue
                withdrawn = rng.sample(held[publisher], 50)
                for key in withdrawn:
                    self.store.withdraw(key, publisher)
                held[publisher] = [key for key in held[publisher] if key not in set(withdrawn)]
                held[publisher] += self.new_keys(500)
                for key in held[publisher]:
                    self.assertTrue(self.store.store(key, publisher), f"refused on day {day}")
            self.store.maintain()
        self.assertEqual(self.store.standing(fast)["tier"], "earned")


if __name__ == "__main__":
    unittest.main()
