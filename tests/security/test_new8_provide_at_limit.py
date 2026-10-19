"""NEW-8: a /provide from a publisher at its record limit cost thousands of expiry checks per refused key.

ProviderStore.provide stored each key of the batch in turn; for a publisher already holding its allowance,
_within_limits then scanned all of that publisher's records for expired ones before refusing, for every key.
At the default max_records (100 000: 1000 records per publisher) a refused 1 KiB batch cost tens of
thousands of checks (about 5 ms) under the store's lock, which /find replies take too.

Rebuilt against the 0.2.0 store, where the limit a publisher meets is a full budget (no per-publisher allowance) and
records are read from the state database per key: a publisher holding most of a full store's Newcomer tier, so its
new keys are refused (it may not churn its own records). Its refused batch must cost a bounded number of database
reads per key (one for the key; room is looked for once per batch, not per key), write nothing, and still accept
the refreshes of records it holds. The work is counted, not timed: the store's database reads.
"""
import os
import unittest
from unittest import mock

from rnfs.constants import PROVIDE_PATH
from rnfs.dht.providers import ProviderStore
from rnfs.network import protocol

TTL = 1000.0
RECORDS = 10_000  # the store's capacity here


def batch_keys() -> list[bytes]:
    """As many new keys as fit in one /provide request on a large-MTU link (the most one request can carry)."""
    keys = []
    while protocol.fits(PROVIDE_PATH, protocol.encode_provide(keys + [os.urandom(16)], []), 10_000):
        keys.append(os.urandom(16))
    return keys


class ProvideAtLimitTest(unittest.TestCase):
    """A full store, one publisher holding all of it."""

    def setUp(self):
        """A full store whose records are all one Newcomer's."""
        self.now = [10_000.0]
        self.store = ProviderStore(None, RECORDS, ttl=TTL, clock=lambda: self.now[0], record_bytes=1)
        self.publisher = os.urandom(16)
        self.held = [os.urandom(16) for _ in range(RECORDS)]
        for key in self.held:
            self.assertTrue(self.store.store(key, self.publisher))
        self.store.save()
        self.assertTrue(self.store.is_full())
        self.keys = batch_keys()
        self.assertGreater(len(self.keys), 10)

    def reads_for(self, keys) -> tuple[int, int, int]:
        """(keys stored, database rows read, transactions opened) for one /provide batch of keys."""
        db = self.store._db
        real = db.query
        rows = []

        def query(sql, params=()):
            """The real read, its rows counted."""
            result = real(sql, params)
            rows.append(len(result))
            return result

        with mock.patch.object(db, "query", query), \
                mock.patch.object(db, "transaction", wraps=db.transaction) as transaction:
            stored = self.store.provide(self.publisher, keys)
        return stored, sum(rows) + len(rows), transaction.call_count

    def test_refused_batch_costs_a_read_per_key(self):
        """A refused batch reads about one row per key (and one query each), and writes nothing."""
        stored, reads, transactions = self.reads_for(self.keys)
        self.assertEqual((stored, transactions), (0, 0))
        # NEW-8: fails if a refused key reads the publisher's records (10 000 of them) to look for room
        self.assertLessEqual(reads, 2 * len(self.keys) + 200,
                             f"a refused batch of {len(self.keys)} keys read {reads} queries and rows")

    def test_next_refused_batch_costs_the_same(self):
        self.reads_for(self.keys)
        self.now[0] += TTL / 2  # nothing stored at the start has expired yet
        stored, reads, transactions = self.reads_for([os.urandom(16) for _ in self.keys])
        self.assertEqual((stored, transactions), (0, 0))
        self.assertLessEqual(reads, 2 * len(self.keys) + 200, f"{reads} queries and rows with nothing expired")

    def test_expired_records_still_make_room(self):
        """Once records have expired, the publisher can store again, in the same batch that finds them."""
        self.reads_for(self.keys)
        self.now[0] += TTL  # every record has expired
        stored, _, _ = self.reads_for(self.keys)
        self.assertEqual(stored, len(self.keys))

    def test_refreshes_are_still_accepted_at_the_limit(self):
        """A publisher at the limit may still republish keys it holds, also after a refused key."""
        self.assertEqual(self.store.provide(self.publisher, self.keys + [self.held[0]]), 1)


if __name__ == "__main__":
    unittest.main()
