"""REL-13 (0.2.0 release audit): withdrawing and restoring the same records spent the provider write budget for free.

The attack: REV2 gave new records and withdrawals a write budget per save interval. A withdrawal spent a row; a store
of the same key and publisher before the next save is a refresh, which spends nothing, but nothing was given back
either, though neither row is then written. One identity holding 27 records, sending /provide with those 27 keys
both withdrawn and stored 4 times a second, spent 108 rows a second while writing almost nothing, and once the
carried-over budget was gone every other Newcomer's new record was refused (an Earned attacker refused Earned
publishers too), for as long as it kept on.

The test runs that loop for a while on a fake clock, the node's save every 30 s, and checks that an honest Newcomer's
new records are still stored and that the attacker still holds its 27. The fix (rnfs/dht/providers.py,
ProviderStore._withdraw and _store, _WriteBudget.refund): the pool a withdrawal spent from is given its row back when
the record is restored before the save.
"""
import random
import unittest

from rnfs.constants import DAY, HOUR, ID_LENGTH
from rnfs.dht.providers import ProviderStore
from tests.support import FakeClock

T0 = 19676 * DAY + HOUR
SIZE = 100
CAPACITY = 10_000_000
WINDOW = 30.0
ATTACKER = bytes([0xA7] * ID_LENGTH)
HONEST = bytes([0x11] * ID_LENGTH)


def ident(n: int) -> bytes:
    """The 16-byte ID whose big-endian value is n."""
    return n.to_bytes(ID_LENGTH, "big")


class WithdrawRestoreBudgetTest(unittest.TestCase):

    def test_a_withdraw_and_restore_loop_spends_nothing(self):
        clock = FakeClock(T0)
        store = ProviderStore(None, CAPACITY * SIZE, clock=clock, monotonic=clock, rng=random.Random(1),
                              record_bytes=SIZE, known=lambda publisher: False)
        self.addCleanup(store.close)
        held = [ident((1 << 100) + i) for i in range(27)]
        self.assertEqual(store.provide(ATTACKER, held), 27)
        honest, tried, next_key = 0, 0, 1 << 110
        for second in range(int(40 * 60)):  # 40 minutes, past what the budget carries over
            for _ in range(4):
                store.provide(ATTACKER, held, held)
            if second % 60 == 0:  # an honest Newcomer publishing a new record a minute
                tried += 1
                honest += store.store(ident(next_key), HONEST)
                next_key += 1
            clock.now += 1
            if second % int(WINDOW) == 0:
                store.maintain()
        self.assertEqual(honest, tried)
        self.assertEqual(store.provide(ATTACKER, held), 27)  # its records are all still there, as refreshes


if __name__ == "__main__":
    unittest.main()
