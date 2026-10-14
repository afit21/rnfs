"""NEW-8: a /provide from a publisher at its record limit cost thousands of expiry checks per refused key.

ProviderStore.provide stored each key of the batch in turn; for a publisher already holding its allowance,
_within_limits then scanned all of that publisher's records for expired ones before refusing, for every key.
At the default max_records (100 000: 1000 records per publisher) a refused 1 KiB batch cost tens of
thousands of checks (about 5 ms) under the store's lock, which /find replies take too. The fixed store sweeps
a publisher's expired records at most once per batch, skips the sweep while none of its records can have
expired yet, and refuses each further key at once.

The work is counted, not timed: the store's ttl is a float that counts how often a record's age is compared
with it, which is what each expiry check does. The clock is fixed (a FakeClock-style callable).
"""
import os
import unittest

from rnfs.constants import PROVIDE_PATH
from rnfs.dht.providers import ProviderStore
from rnfs.network import protocol

TTL = 1000.0


class CountingTtl(float):
    """A ttl that counts the comparisons `age >= ttl` make (Python asks the float subclass on the right)."""

    checks = 0

    def __le__(self, other):
        """other >= self: one expiry check."""
        CountingTtl.checks += 1
        return float.__le__(self, other)

    def __gt__(self, other):
        """other < self: one liveness check."""
        CountingTtl.checks += 1
        return float.__gt__(self, other)


def batch_keys() -> list[bytes]:
    """As many new keys as fit in one /provide request on a large-MTU link (the most one request can carry)."""
    keys = []
    while protocol.fits(PROVIDE_PATH, protocol.encode_provide(keys + [os.urandom(16)], []), 10_000):
        keys.append(os.urandom(16))
    return keys


class ProvideAtLimitTest(unittest.TestCase):
    """A publisher holding its whole allowance in a store of the default size."""

    def setUp(self):
        """A store at the default max_records, with one publisher at its limit."""
        self.now = [10_000.0]
        self.store = ProviderStore(100_000, None, ttl=CountingTtl(TTL), clock=lambda: self.now[0])
        self.publisher = os.urandom(16)
        self.allowance = self.store.max_per_publisher
        for _ in range(self.allowance):
            self.assertTrue(self.store.store(os.urandom(16), self.publisher))
        self.keys = batch_keys()
        self.assertGreater(len(self.keys), 10)

    def checks_for(self, keys) -> tuple[int, int]:
        """(keys stored, expiry checks made) for one /provide batch of keys from the publisher."""
        CountingTtl.checks = 0
        stored = self.store.provide(self.publisher, keys)
        return stored, CountingTtl.checks

    def test_refused_batch_scans_the_publishers_records_at_most_once(self):
        """A refused batch costs at most one pass over the publisher's records plus a check per key."""
        stored, checks = self.checks_for(self.keys)
        self.assertEqual(stored, 0)
        # NEW-8: fails until the per-publisher limit is checked once per batch, not once per key
        self.assertLessEqual(checks, self.allowance + 2 * len(self.keys),
                             f"a refused batch of {len(self.keys)} keys made {checks} expiry checks")

    def test_next_refused_batch_skips_the_sweep(self):
        """While none of its records can have expired, a refused batch doesn't scan them at all."""
        self.checks_for(self.keys)
        self.now[0] += TTL / 2  # nothing stored at the start has expired yet
        stored, checks = self.checks_for([os.urandom(16) for _ in self.keys])
        self.assertEqual(stored, 0)
        # NEW-8: fails until a sweep that can't find anything expired is skipped
        self.assertLessEqual(checks, 2 * len(self.keys), f"{checks} expiry checks with nothing expired")

    def test_expired_records_still_make_room(self):
        """Once records have expired, the publisher can store again, in the same batch that finds them."""
        self.checks_for(self.keys)
        self.now[0] += TTL  # every record has expired
        stored, _ = self.checks_for(self.keys)
        self.assertEqual(stored, len(self.keys))

    def test_refreshes_are_still_accepted_at_the_limit(self):
        """A publisher at its limit may still republish keys it holds."""
        held = next(iter(self.store._publishers[self.publisher].keys))
        self.assertEqual(self.store.provide(self.publisher, [held] + self.keys), 1)


if __name__ == "__main__":
    unittest.main()
