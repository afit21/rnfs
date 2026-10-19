"""M4-R: after the first M4 fix, a flood of fresh identities churned the newcomer pool at random, so an honest
newcomer kept almost none of its records (1 of 100 against 600 identities), and once its last record was evicted
its first-seen time went with it: it was new again, and so could never age into a full share.

Rebuilt against the 0.2.0 store, where the newcomer pool became the Newcomer tier of a full store: a store whose
budget is full of established (Earned) publishers down to the Newcomers' floor, then an honest newcomer H, then a
flood of new identities. A full Newcomer tier evicts from its largest holder, so H keeps about a fair share of it
rather than losing its records at random; and the refresh days H earned (what now ages a publisher) outlast the
eviction of its last record, across a restart too, while fresh identities that never refreshed leave nothing.
"""
import os
import random
import tempfile
import unittest

from rnfs.constants import DAY, ID_LENGTH
from rnfs.dht.providers import EARNED_DAYS, FLOORS, NEWCOMER, NEWCOMER_TTL_FULL, ProviderStore
from tests.support import FakeClock

H = bytes([0x48] * ID_LENGTH)
SIZE = 100  # bytes a record takes here


def ident(n: int) -> bytes:
    """The 16-byte ID whose big-endian value is n."""
    return n.to_bytes(ID_LENGTH, "big")


class NewcomerTierTest(unittest.TestCase):
    """Stores of `capacity` records, their Earned publishers holding all but the Newcomers' floor."""

    def setUp(self):
        """The clock."""
        self.clock = FakeClock(1_700_000_000.0)

    def make_store(self, capacity: int, state: str | None = None) -> ProviderStore:
        """A store of capacity records, with ten Earned publishers holding all but the Newcomers' floor (persisted
        in state, if given, and closed when the test ends)."""
        store = ProviderStore(state, capacity * SIZE, clock=self.clock, rng=random.Random(1), record_bytes=SIZE)
        self.addCleanup(store.close)
        established = [ident((0x0F << 120) + i) for i in range(10)]
        share = int(capacity * (1 - FLOORS[NEWCOMER])) // len(established)
        for day in range(EARNED_DAYS + 1):  # stored, then refreshed on EARNED_DAYS days
            for i, publisher in enumerate(established):
                for n in range(share):
                    store.store(ident((1 << 100) + i * 100_000 + n), publisher)
            if day < EARNED_DAYS:
                self.clock.now += DAY
        self.assertEqual(store.tier_counts()["earned"], share * len(established))
        return store

    def test_newcomer_keeps_a_fair_share_of_its_tier(self):
        store = self.make_store(1000)
        keys = [ident((2 << 100) + i) for i in range(100)]
        self.assertEqual(sum(store.store(key, H) for key in keys), len(keys))
        identities = 30
        for i in range(identities):
            for n in range(10):
                store.store(ident((3 << 100) + i * 100 + n), ident((0x5 << 120) + i))
        self.assertTrue(store.is_full())
        kept = sum(H in store.providers(key) for key in keys)
        fair = store.tier_counts()["newcomer"] // (identities + 1)
        # M4-R: fails if a full Newcomer tier evicts at random rather than from its largest holder
        self.assertGreaterEqual(kept, fair, f"H kept {kept} of {len(keys)} records; a fair share is {fair}")
        self.assertEqual(store.tier_counts()["earned"], 900)  # the established publishers lost nothing

    def test_refresh_days_outlast_the_last_record(self):
        """A newcomer whose last record a flood pushed out keeps the refresh days it earned, across a restart. (In
        a tier of single records the flood's own, changed most recently, give way first; H's goes as the full
        store's Newcomer records do, NEWCOMER_TTL_FULL after it was last stored.)"""
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)  # after the stores are closed (cleanups run last first)
        state = os.path.join(directory.name, "state")
        store = self.make_store(1000, state)
        self.assertTrue(store.store(ident(2 << 100), H))
        self.clock.now += DAY
        self.assertTrue(store.store(ident(2 << 100), H))  # refreshed on a second day: one day counted
        self.assertEqual(store.standing(H)["days"], 1)
        for i in range(3 * 100):  # fresh identities, one record each
            self.clock.now += 1
            store.store(ident((3 << 100) + i), ident((0x5 << 120) + i))
        self.assertTrue(store.is_full())
        self.clock.now += NEWCOMER_TTL_FULL
        store.maintain()
        self.assertIsNone(store.first_seen(H), "H still holds a record")
        store.save()
        store.close()
        store = ProviderStore(state, 1000 * SIZE, clock=self.clock, record_bytes=SIZE)
        self.addCleanup(store.close)
        # M4-R: fails if a publisher's standing goes with its last record
        self.assertEqual(store.standing(H)["days"], 1, "H's refresh days went with its last record")
        self.assertTrue(store.store(ident(4 << 100), H))
        for _ in range(EARNED_DAYS - 1):
            self.clock.now += DAY
            store.store(ident(4 << 100), H)
        self.assertEqual(store.standing(H)["tier"], "earned")

    def test_fresh_identities_leave_no_standing(self):
        """The memory of evicted publishers is bounded by having earned something: identities that never refreshed
        leave no standing behind, so a flood of them grows nothing."""
        store = self.make_store(1000)
        for i in range(20 * 100):
            self.clock.now += 1
            store.store(ident((3 << 100) + i), ident((0x5 << 120) + i))
        store.save()
        self.assertEqual(store.publisher_count(), 10 + 100)  # the established ones, and as many fresh as there's room
        standings = store._db.query("SELECT COUNT(*) FROM provider_standing")[0][0]
        self.assertEqual(standings, 110)


if __name__ == "__main__":
    unittest.main()
