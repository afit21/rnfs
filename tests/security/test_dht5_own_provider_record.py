"""DHT-5: a node's own provider record could be evicted from its own store by Sybil stores to the key.

Records are kept per key up to max_per_key publishers; a newcomer to a full key evicted a random publisher outside
the K/2 oldest records, and the node's own record (own_id, which the node records for every key it provides) was
one of the candidates. A handful of Sybil identities storing first take the senior places; after that about 1000
stores to the key evicted the node's own record in 50 of 50 trials. The node's own records could likewise be the
victim of a full newcomer pool. Now own_id's record doesn't count toward max_per_key and is never the victim, of
either limit; everyone else's limits are unchanged. Since 0.2.0 the second limit is the record budget and its tiers
(no newcomer pool): the node's own records are in a tier of their own, never evicted to make room.
"""
import random
import unittest

from rnfs.constants import ID_LENGTH, K
from rnfs.dht.providers import ProviderStore
from tests.support import FakeClock

OWN = bytes([0x0E] * ID_LENGTH)
KEY = bytes([0xAB] * ID_LENGTH)
SEEDS = 50
SYBIL_STORES = 1000
MAX_PER_KEY = 20


def sybil(n: int) -> bytes:
    """The n-th Sybil identity: a 16-byte ID unlike OWN."""
    return (0xF << 124 | n).to_bytes(ID_LENGTH, "big")


class OwnProviderRecordTest(unittest.TestCase):
    """Stores with own_id set, flooded by fresh identities, over 50 eviction seeds."""

    def setUp(self):
        """The clock."""
        self.clock = FakeClock(1_700_000_000.0)

    def make_store(self, seed: int, records: int = 100_000) -> ProviderStore:
        """A memory-only store of `records` records recording OWN as this node, its eviction picks seeded."""
        return ProviderStore(None, records, clock=self.clock, rng=random.Random(seed), max_per_key=MAX_PER_KEY,
                             own_id=OWN, record_bytes=1)

    def test_sybil_stores_to_a_key_never_evict_the_own_record(self):
        evicted = []
        for seed in range(SEEDS):
            store = self.make_store(seed)
            for n in range(K // 2):  # the senior places, taken before the node records itself
                self.assertTrue(store.store(KEY, sybil(n)))
            self.assertTrue(store.store(KEY, OWN))
            for n in range(K // 2, K // 2 + SYBIL_STORES):
                self.assertTrue(store.store(KEY, sybil(n)))  # newcomers are still never locked out
            providers = store.providers(KEY)
            if OWN not in providers:
                evicted.append(seed)
                continue
            # The own record doesn't count: the key still holds max_per_key others, the oldest K/2 of them kept
            self.assertEqual(len(providers), MAX_PER_KEY + 1)
            others = [p for p in providers if p != OWN]
            self.assertEqual(others[:K // 2], [sybil(n) for n in range(K // 2)])
        self.assertEqual(evicted, [], f"own record evicted in {len(evicted)} of {SEEDS} trials")

    def test_own_store_to_a_full_key_evicts_no_one(self):
        store = self.make_store(1)
        for n in range(MAX_PER_KEY):
            store.store(KEY, sybil(n))
        self.assertTrue(store.store(KEY, OWN))
        self.assertEqual(set(store.providers(KEY)), {sybil(n) for n in range(MAX_PER_KEY)} | {OWN})

    def test_a_full_store_never_evicts_own_records(self):
        """Own records fill most of the budget; a flood of identities then fills the rest and churns it, taking
        only each other's records; and own records stored past the budget evict no one."""
        for seed in range(SEEDS):
            store = self.make_store(seed, records=100)
            own_keys = [(1 << 100 | n).to_bytes(ID_LENGTH, "big") for n in range(90)]
            for key in own_keys:
                self.assertTrue(store.store(key, OWN))
            for n in range(SYBIL_STORES):
                store.store((2 << 100 | n).to_bytes(ID_LENGTH, "big"), sybil(n))
            self.assertTrue(store.is_full())
            for n in range(90, 95):
                own_keys.append((1 << 100 | n).to_bytes(ID_LENGTH, "big"))
                self.assertTrue(store.store(own_keys[-1], OWN))
            missing = [key for key in own_keys if OWN not in store.providers(key)]
            self.assertEqual(missing, [], f"seed {seed}: {len(missing)} own records evicted")
            self.assertEqual(store.record_count(), 105)  # own records over the budget, the Sybils' within it


if __name__ == "__main__":
    unittest.main()
