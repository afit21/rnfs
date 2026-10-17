"""M4-R: after the first M4 fix, a flood of fresh identities churned the newcomer pool at random, so an honest
newcomer kept almost none of its records (1 of 100 against 600 identities), and once its last record was evicted
its first-seen time went with it: it was new again, and so could never age into a full share.

A ProviderStore whose space outside the pool is full of older publishers (as tests/security/test_m4_* do), then an
honest newcomer H, then a flood of new identities storing into the pool. H must keep about a fair share of the pool,
and its first-seen time must outlast the eviction of its last record (across a restart too), while that memory
stays bounded.
"""
import os
import random
import tempfile
import unittest

from rnfs.constants import DAY, ID_LENGTH
from rnfs.dht.providers import ProviderStore
from tests.support import FakeClock

H = bytes([0x48] * ID_LENGTH)


def ident(n: int) -> bytes:
    """The 16-byte ID whose big-endian value is n."""
    return n.to_bytes(ID_LENGTH, "big")


def fill_main(store: ProviderStore, clock: FakeClock):
    """Fill the store outside the pool with publishers seen two days ago (so at their full share)."""
    publishers = [ident((0x0F << 120) + i) for i in range(store.main_size // store.max_per_publisher)]
    for i, publisher in enumerate(publishers):
        store.store(ident((1 << 100) + i), publisher)
    clock.now += 2 * DAY
    for i, publisher in enumerate(publishers):
        for n in range(store.max_per_publisher):
            store.store(ident((1 << 100) + i * 10_000 + n), publisher)
    assert store.record_count() == store.main_size, store.record_count()


class NewcomerPoolTest(unittest.TestCase):
    """Stores with provider_seniority one day and seeded eviction picks."""

    def make_store(self, max_records: int, state: str | None = None) -> ProviderStore:
        """A store on self.clock, its newcomer allowance a tenth of the full one when large enough (persisted in
        state, if given, and closed when the test ends)."""
        store = ProviderStore(max_records, state, clock=self.clock, rng=random.Random(1), seniority=DAY)
        self.addCleanup(store.close)
        return store

    def setUp(self):
        """The clock."""
        self.clock = FakeClock(1_700_000_000.0)

    def test_newcomer_keeps_a_fair_share_of_the_pool(self):
        store = self.make_store(1000)
        fill_main(store, self.clock)
        keys = [ident((2 << 100) + i) for i in range(store.newcomer_per_publisher)]
        self.assertEqual(sum(store.store(key, H) for key in keys), len(keys))
        identities = 30
        for i in range(identities):
            for n in range(store.newcomer_per_publisher):
                store.store(ident((3 << 100) + i * 100 + n), ident((0x5 << 120) + i))
        kept = sum(H in store.providers(key) for key in keys)
        fair = store.pool_size // (identities + 1)
        # M4-R: fails until a full pool evicts from the publisher holding the most pooled records
        self.assertGreaterEqual(kept, fair, f"H kept {kept} of {len(keys)} records; a fair share of the pool is {fair}")

    def test_first_seen_outlasts_eviction_from_the_pool(self):
        """A newcomer whose last record a flood evicted from the pool keeps its first-seen time, across a restart."""
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)  # after the stores are closed (cleanups run last first)
        state = os.path.join(directory.name, "state")
        store = self.make_store(10_000, state)
        fill_main(store, self.clock)
        self.assertTrue(store.store(ident(2 << 100), H))
        first_seen = store.first_seen(H)
        self.clock.now += 60
        for i in range(3 * store.pool_size):  # fresh identities, one record each
            self.clock.now += 1
            store.store(ident((3 << 100) + i), ident((0x5 << 120) + i))
        self.assertIsNone(store.first_seen(H), "H still holds a record: the flood didn't evict it")
        store.save()
        store = self.make_store(10_000, state)  # and a restart
        self.clock.now += DAY
        # M4-R: fails until first-seen times of newcomers evicted from the pool are kept
        self.assertEqual(store.allowance(H), store.max_per_publisher,
                         "H, first seen over a day ago, is new again after the flood")
        self.assertTrue(store.store(ident(4 << 100), H))
        self.assertEqual(store.first_seen(H), first_seen)

    def test_memory_of_evicted_newcomers_is_bounded(self):
        """Bounded, and the newcomers first seen most recently are forgotten first, so a flood can't push out H."""
        store = self.make_store(1000)
        fill_main(store, self.clock)
        self.assertTrue(store.store(ident(2 << 100), H))
        for i in range(20 * store.pool_size):
            self.clock.now += 1
            store.store(ident((3 << 100) + i), ident((0x5 << 120) + i))
        self.assertIsNone(store.first_seen(H))
        # M4-R: fails until first-seen times of newcomers evicted from the pool are kept
        self.assertIsNotNone(store._departed_first_seen(H, self.clock.now), "H's first-seen time was forgotten")
        self.assertLessEqual(len(store._evicted), store._evicted_size)


if __name__ == "__main__":
    unittest.main()
