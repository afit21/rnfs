"""REV3: a full key could be taken over by Earned identities.

A key keeps at most 1000 publishers. ProviderStore._key_room let a newcomer to a full key evict a random record of
the lowest tier there only if that tier was below its own, or was its own and not Earned: Earned publishers never
gave way to each other. So about 1000 identities that each stored the key and refreshed it on 3 UTC days (from about
48 h) filled its 1000 publishers with Earned records, after which every new provider of the key was refused,
Newcomer and Earned alike (0.1.6 let a newcomer evict a random record). Only Known publishers still got in.

Rebuilt with a memory-only store and the default 1000 publishers a key: 1000 identities store one key and refresh
it on EARNED_DAYS later days, then an honest Newcomer and an honest Earned publisher store it. The fix: at a full
key an Earned or Known storer evicts a random Earned record outside the K/2 oldest (a Newcomer one first only while
Newcomers hold more than their share of the key), and a Newcomer evicts a random Earned record while Newcomers hold
less than a share of the key (a tenth: 100 of its 1000 publishers), else a random Newcomer one. The K/2 oldest
records still never give way, and a flood of fresh identities can't take more than the Newcomers' share.
"""
import random
import unittest

from rnfs.constants import DAY, HOUR, ID_LENGTH, K
from rnfs.dht.providers import EARNED_DAYS, ProviderStore
from tests.support import FakeClock

T0 = 19676 * DAY + HOUR  # 01:00 UTC
SIZE = 100  # bytes a record takes here
KEY = bytes([0x4B] * ID_LENGTH)
PUBLISHERS = 1000  # a key's publishers at most (the default)
NEWCOMER_SHARE = PUBLISHERS // 10  # the Newcomers' share of a full key


def ident(n: int) -> bytes:
    """The 16-byte ID whose big-endian value is n."""
    return n.to_bytes(ID_LENGTH, "big")


class FullKeyTest(unittest.TestCase):
    """KEY held by 1000 Earned identities (the attack), in a store far from full."""

    def setUp(self):
        """The store, and the attack: each identity stores KEY, then refreshes it on EARNED_DAYS later days."""
        self.clock = FakeClock(T0)
        self.known: set[bytes] = set()
        self.store = ProviderStore(None, 10_000_000 * SIZE, clock=self.clock, monotonic=self.clock,
                                   rng=random.Random(1), record_bytes=SIZE, known=self.known.__contains__)
        self.addCleanup(self.store.close)
        self.attackers = [ident((0xA << 120) + i) for i in range(PUBLISHERS)]
        for publisher in self.attackers:
            self.assertTrue(self.store.store(KEY, publisher))
        for _ in range(EARNED_DAYS):
            self.clock.now += DAY
            for publisher in self.attackers:
                self.assertTrue(self.store.store(KEY, publisher))
        self.assertEqual(self.store.tier_counts()["earned"], PUBLISHERS)
        self.assertEqual(len(self.store.providers(KEY)), PUBLISHERS)
        self.senior = self.store.providers(KEY)[:K // 2]

    def earn(self, *publishers: bytes):
        """Make publishers Earned, each on a key of its own, while the attack keeps its records refreshed."""
        for publisher in publishers:
            self.assertTrue(self.store.store(ident(publisher[0] << 8), publisher))
        for _ in range(EARNED_DAYS):
            self.clock.now += DAY
            for publisher in publishers:
                self.assertTrue(self.store.store(ident(publisher[0] << 8), publisher))
            for attacker in self.attackers:
                self.store.store(KEY, attacker)
        for publisher in publishers:
            self.assertEqual(self.store.standing(publisher)["tier"], "earned")

    def test_new_providers_still_get_in(self):
        newcomer = bytes([0x4E] * ID_LENGTH)
        earned = bytes([0x45] * ID_LENGTH)
        self.earn(earned)
        # REV3: fail if a key full of Earned identities refuses every new provider
        self.assertTrue(self.store.store(KEY, newcomer), "a Newcomer was refused at a key full of Earned identities")
        self.assertTrue(self.store.store(KEY, earned), "an Earned publisher was refused at a key full of Earned ones")
        held = self.store.providers(KEY)
        self.assertEqual(len(held), PUBLISHERS)
        self.assertIn(newcomer, held)
        self.assertIn(earned, held)
        self.assertEqual(held[:K // 2], self.senior, "one of the K/2 oldest records gave way")
        self.known.add(bytes([0x4F] * ID_LENGTH))
        self.assertTrue(self.store.store(KEY, bytes([0x4F] * ID_LENGTH)))  # Known publishers still get in

    def test_newcomers_keep_to_their_share(self):
        """A flood of fresh identities at the key gets them in, churning among themselves, but never more than the
        Newcomers' share of the key; the senior half stays."""
        flood = [ident((0xF << 120) + i) for i in range(3 * PUBLISHERS)]
        stored = sum(self.store.store(KEY, publisher) for publisher in flood)
        # REV3: fails if Newcomers are refused at a key full of Earned identities
        self.assertEqual(stored, len(flood))
        held = self.store.providers(KEY)
        newcomers = sum(self.store.standing(publisher)["tier"] == "newcomer" for publisher in held)
        self.assertEqual(newcomers, NEWCOMER_SHARE, "the Newcomers at the key aren't held to their share")
        self.assertEqual(held[:K // 2], self.senior)

    def test_earned_storers_spare_the_newcomers_share(self):
        """Earned storers evict Earned records while Newcomers hold no more than their share: the share stays."""
        for i in range(NEWCOMER_SHARE):
            self.assertTrue(self.store.store(KEY, ident((0xF << 120) + i)))
        others = [bytes([0x60 + i] * ID_LENGTH) for i in range(20)]
        self.earn(*others)
        # REV3: fails if an Earned publisher is refused at a key full of Earned ones
        for publisher in others:
            self.assertTrue(self.store.store(KEY, publisher))
        held = self.store.providers(KEY)
        newcomers = sum(self.store.standing(publisher)["tier"] == "newcomer" for publisher in held)
        self.assertEqual(newcomers, NEWCOMER_SHARE)
        self.assertLessEqual(set(others), set(held))


if __name__ == "__main__":
    unittest.main()
