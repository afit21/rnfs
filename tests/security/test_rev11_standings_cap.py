"""REV11: the standings of publishers left without records were bounded only by PROVIDER_TTL.

A publisher that refreshed a record on a later UTC day keeps its standing (first-seen time and refresh days) a TTL
after the last of those days once its last record is gone (ProviderStore._remove, _forget_departed), so one that was
away for a while is still Earned. That memory had no cap: each identity that stored a record, refreshed it a day
later and withdrew it left a row behind for a week, so a flood of them grew provider_standing and
provider_publishers without bound (and the state database's size, which the record budget measures, with them).

Rebuilt with a memory-only store and the cap lowered for the test: honest Earned publishers that left, then far
more identities each refreshing one record on one later day and withdrawing it. The fix caps the standings kept
without records by count (MAX_DEPARTED); past it, the hourly pass forgets those with the fewest refresh days first,
the longest unrefreshed first among equals, so honest Earned publishers that left outlast a flood of identities
with a day or two of refreshes each.
"""
import random
import unittest
from unittest import mock

from rnfs.constants import DAY, HOUR, ID_LENGTH
from rnfs.dht import providers
from rnfs.dht.providers import EARNED_DAYS, ProviderStore
from tests.support import FakeClock

T0 = 19676 * DAY + HOUR  # 01:00 UTC
SIZE = 100  # bytes a record takes here
CAP = 200  # the cap of standings without records, lowered for the test
HONEST = 50
FLOOD = 1000


def ident(n: int) -> bytes:
    """The 16-byte ID whose big-endian value is n."""
    return n.to_bytes(ID_LENGTH, "big")


class DepartedStandingsTest(unittest.TestCase):
    """A memory-only store far from full."""

    def setUp(self):
        """The clock and the store, and the cap lowered."""
        patch = mock.patch.object(providers, "MAX_DEPARTED", CAP)
        patch.start()
        self.addCleanup(patch.stop)
        self.clock = FakeClock(T0)
        self.store = ProviderStore(None, 10_000_000 * SIZE, clock=self.clock, monotonic=self.clock,
                                   rng=random.Random(1), record_bytes=SIZE)
        self.addCleanup(self.store.close)

    def leave(self, publishers: list[bytes], days: int, first_key: int):
        """Each of publishers stores a key, refreshes it on `days` later UTC days, then withdraws it."""
        for i, publisher in enumerate(publishers):
            self.assertTrue(self.store.store(ident(first_key + i), publisher))
        for _ in range(days):
            self.clock.now += DAY
            for i, publisher in enumerate(publishers):
                self.assertTrue(self.store.store(ident(first_key + i), publisher))
        for i, publisher in enumerate(publishers):
            self.assertTrue(self.store.withdraw(ident(first_key + i), publisher))

    def departed(self) -> int:
        """Standings kept for publishers holding no records, as the database has them."""
        return self.store._db.query("SELECT COUNT(*) FROM provider_standing WHERE records = 0")[0][0]

    def test_standings_without_records_are_capped(self):
        honest = [ident((0x4 << 120) + i) for i in range(HONEST)]
        self.leave(honest, EARNED_DAYS, 1 << 100)
        flood = [ident((0xF << 120) + i) for i in range(FLOOD)]
        self.leave(flood, 1, 2 << 100)
        self.clock.now += HOUR
        self.store.maintain()
        # REV11: fails if standings without records are capped only by PROVIDER_TTL
        self.assertLessEqual(self.departed(), CAP, "standings without records aren't capped")
        for publisher in honest:
            standing = self.store.standing(publisher)
            self.assertIsNotNone(standing, "an honest Earned publisher that left was forgotten before the flood")
            self.assertEqual(standing["tier"], "earned")
        self.assertEqual(self.store.publisher_count(), 0)
        # The cap holds as the flood goes on
        self.leave([ident((0xE << 120) + i) for i in range(FLOOD)], 1, 3 << 100)
        self.clock.now += HOUR
        self.store.maintain()
        self.assertLessEqual(self.departed(), CAP)
        self.assertTrue(all(self.store.standing(publisher) is not None for publisher in honest))


if __name__ == "__main__":
    unittest.main()
