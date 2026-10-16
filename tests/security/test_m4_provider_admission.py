"""M4: a provider store filled by about 100 publishers refused every publisher it hadn't seen yet, this node included.

A ProviderStore of the default size (Config().max_records) is filled to the brim by 100 identities storing their
per-publisher maximum each. A publisher that stored before the flood is still accepted; a new one, and the node's
own ID with no records yet, must be too (the newcomer pool, and own records always admitted). Further tests pin
down the limits: the per-publisher cap and its ramp-up with age, the first-seen grace after a publisher's last
record goes, and how many keys one /provide request may carry on the server side.
"""
import os
import random
import tempfile
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import DAY, ID_LENGTH, MAX_PROVIDE_KEYS, MAX_REQUEST_SIZE, PROVIDE_PATH, PROVIDER_TTL
from rnfs.dht.providers import ProviderStore
from rnfs.network import protocol, server
from rnfs.network.identity import node_id_for_identity
from tests.support import FakeClock
from tests.test_server import FakeBackend, FakeDestination, FakeLink

OWN = bytes([0x01] * ID_LENGTH)
HONEST_OLD = bytes([0x02] * ID_LENGTH)
NEWCOMER = bytes([0x03] * ID_LENGTH)
FLOODERS = 100


def ident(n: int) -> bytes:
    """The 16-byte ID whose big-endian value is n."""
    return n.to_bytes(ID_LENGTH, "big")


class FullStoreTest(unittest.TestCase):
    """A default-sized store: one honest publisher, then FLOODERS identities filling it."""

    def setUp(self):
        """The store, filled; every flooder stores its maximum, a second after the one before."""
        self.clock = FakeClock(1_700_000_000.0)
        self.store = ProviderStore(Config().max_records, None, clock=self.clock, own_id=OWN)
        self.assertTrue(self.store.store(ident(1), HONEST_OLD))
        self.clock.now += 1
        key = 1 << 64
        for flooder in range(FLOODERS):
            publisher = ident((0xF << 120) + flooder)
            for _ in range(self.store.max_per_publisher):
                key += 1
                self.store.store(ident(key), publisher)
            self.clock.now += 1
        self.assertEqual(self.store.record_count(), self.store.max_records)

    def test_flooders_needed_to_fill(self):
        """Today 100 publishers at 1% each fill the default store of 100,000 records."""
        self.assertEqual((self.store.max_records, self.store.max_per_publisher), (100_000, 1000))
        self.assertEqual(self.store.publisher_count(), FLOODERS + 1)

    def test_established_publisher_accepted(self):
        self.assertTrue(self.store.store(ident(2), HONEST_OLD))
        self.assertEqual(self.store.record_count(), self.store.max_records)

    def test_new_publisher_can_store(self):
        accepted = sum(self.store.store(ident(10 + i), NEWCOMER) for i in range(10))
        self.assertGreater(accepted, 0, "a new honest publisher stored none of 10 records in the full store")

    def test_own_records_always_accepted(self):
        self.assertIsNone(self.store.first_seen(OWN))
        self.assertTrue(self.store.store(ident(3), OWN), "the node's own record was refused by its full store")

    def test_own_records_never_refused(self):
        """However many: each evicts a record of the newest publisher outside the pool, never the honest one."""
        self.assertEqual(sum(self.store.store(ident(10_000 + i), OWN) for i in range(2000)), 2000)
        self.assertEqual(self.store.record_count(), self.store.max_records)
        self.assertEqual(self.store.providers(ident(1)), [HONEST_OLD])

    def test_newcomers_only_churn_the_pool(self):
        """A flood of new identities displaces pooled records only: the publishers seen first keep everything."""
        self.assertEqual(sum(self.store.store(ident(10_000 + i), ident((0xE << 120) + i)) for i in range(2000)), 2000)
        self.assertEqual(self.store.record_count(), self.store.max_records)
        self.assertEqual(self.store.providers(ident(1)), [HONEST_OLD])
        first_flooder = ident(0xF << 120)
        held = sum(first_flooder in self.store.providers(ident((1 << 64) + 1 + i)) for i in range(1000))
        self.assertEqual(held, 1000)


class AllowanceTest(unittest.TestCase):
    """The per-publisher allowance grows with age: a tenth until a publisher was seen provider_seniority ago."""

    def setUp(self):
        """A default-sized store ramping up over a day, as the node makes it."""
        self.clock = FakeClock(1_700_000_000.0)
        self.store = ProviderStore(Config().max_records, None, clock=self.clock, own_id=OWN,
                                   seniority=Config().provider_seniority)
        self.assertEqual(Config().provider_seniority, DAY)

    def test_new_publisher_gets_a_tenth(self):
        self.assertEqual((self.store.max_per_publisher, self.store.newcomer_per_publisher), (1000, 100))
        publisher = ident(7)
        self.assertEqual(self.store.allowance(publisher), 100)
        self.assertEqual(sum(self.store.store(ident(100 + i), publisher) for i in range(1000)), 100)
        self.clock.now += DAY
        self.assertEqual(self.store.allowance(publisher), 1000)
        self.assertEqual(sum(self.store.store(ident(100 + i), publisher) for i in range(1001)), 1000)

    def test_own_id_unlimited_from_the_start(self):
        self.assertIsNone(self.store.allowance(OWN))
        self.assertEqual(sum(self.store.store(ident(100 + i), OWN) for i in range(1500)), 1500)

    def test_first_seen_kept_for_a_ttl_after_the_last_record_goes(self):
        publisher = ident(7)
        self.assertTrue(self.store.store(ident(100), publisher))
        first_seen = self.store.first_seen(publisher)
        self.clock.now += 2 * DAY
        self.store.withdraw(ident(100), publisher)
        self.assertIsNone(self.store.first_seen(publisher))
        # Back within a TTL: its age, and so its full allowance, comes back with it
        self.clock.now += PROVIDER_TTL - 1
        self.assertEqual(self.store.allowance(publisher), 1000)
        self.assertTrue(self.store.store(ident(101), publisher))
        self.assertEqual(self.store.first_seen(publisher), first_seen)
        # Records expiring count as leaving too; after a TTL more it is new again
        self.clock.now += PROVIDER_TTL
        self.store.expire()
        self.clock.now += PROVIDER_TTL
        self.store.expire()
        self.assertEqual(self.store.allowance(publisher), 100)
        self.assertTrue(self.store.store(ident(102), publisher))
        self.assertEqual(self.store.first_seen(publisher), self.clock.now)

    def test_young_publishers_are_not_remembered(self):
        """A publisher seen for less than provider_seniority has no standing to keep, so a flood of fresh
        identities can't push older publishers' first-seen times out of the bounded memory."""
        self.assertTrue(self.store.store(ident(100), ident(7)))
        self.clock.now += 60
        self.store.withdraw(ident(100), ident(7))
        self.clock.now += 60
        self.assertTrue(self.store.store(ident(100), ident(7)))
        self.assertEqual(self.store.first_seen(ident(7)), self.clock.now)


class ReturningPublisherTest(unittest.TestCase):
    """A publisher back within the grace counts as seen when it was first seen, also for eviction."""

    def test_returning_publisher_evicts_the_newest(self):
        clock = FakeClock(1_700_000_000.0)
        store = ProviderStore(100, None, clock=clock, seniority=DAY)
        old, newer = ident(1), ident(2)
        store.store(ident(100), old)
        clock.now += 2 * DAY
        store.withdraw(ident(100), old)  # away, with its first-seen time remembered
        for i in range(store.max_per_publisher):
            store.store(ident(200 + i), newer)
        for n in range(8):  # fill the store outside the pool with others seen after it
            for i in range(store.max_per_publisher):
                store.store(ident(1000 * (n + 1) + i), ident(10 + n))
            clock.now += 1
        self.assertEqual(store.record_count(), store.main_size)
        # Seen first, so it displaces the newest publisher outside the pool rather than going into the pool
        self.assertTrue(store.store(ident(100), old))
        self.assertEqual(store.record_count(), store.main_size)
        self.assertEqual(store.providers(ident(100)), [old])


class PersistenceTest(unittest.TestCase):
    """The pool and remembered first-seen times survive a restart."""

    def test_round_trip(self):
        """A restarted store keeps which records are pooled and a departed publisher's first-seen time."""
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        state = os.path.join(directory.name, "state")
        clock = FakeClock(1_700_000_000.0)
        store = ProviderStore(100, state, clock=clock, rng=random.Random(1), seniority=DAY)
        self.addCleanup(store.close)
        gone = ident(9)
        store.store(ident(1), gone)
        clock.now += 2 * DAY
        store.withdraw(ident(1), gone)
        for n in range(10):  # 90 outside the pool, then the last 10 pooled
            for i in range(10):
                store.store(ident(100 * (n + 1) + i), ident(10 + n))
        store.save()
        loaded = ProviderStore(100, state, clock=clock, rng=random.Random(1), seniority=DAY)
        self.addCleanup(loaded.close)
        self.assertEqual(loaded.record_count(), 100)
        self.assertEqual(loaded.allowance(gone), loaded.max_per_publisher)
        # Pooled records stay pooled: a newcomer evicts one of them, not a record outside the pool
        self.assertTrue(loaded.store(ident(5), ident(99)))
        self.assertEqual(sum(len(loaded.providers(ident(100 * (n + 1) + i))) for n in range(9) for i in range(10)),
                         90)


class PerPublisherCapTest(unittest.TestCase):
    """Today's per-publisher cap: max(10, max_records // 100)."""

    def test_cap(self):
        for max_records, cap in ((100, 10), (999, 10), (1000, 10), (5000, 50), (100_000, 1000)):
            with self.subTest(max_records=max_records):
                self.assertEqual(ProviderStore(max_records, None).max_per_publisher, cap)

    def test_cap_enforced(self):
        store = ProviderStore(100_000, None, clock=FakeClock(1_700_000_000.0))
        publisher = ident(7)
        self.assertEqual(sum(store.store(ident(100 + i), publisher) for i in range(1001)), 1000)


class ProvideRequestSizeTest(unittest.TestCase):
    """How many keys one /provide may carry: only RNS's request size limit bounds it, not the parser."""

    def test_keys_per_request(self):
        """55 keys fit a 1 KiB request; the server takes them all in one /provide. rnfs's client sends 22."""
        keys = [ident(1000 + i) for i in range(1000)]
        most = max(n for n in range(1, 100)
                   if protocol.request_size(PROVIDE_PATH, protocol.encode_provide(keys[:n], [])) <= MAX_REQUEST_SIZE)
        self.assertEqual(most, 55)
        # The parser itself has a count limit now (M4 fix; it used to take any number): MAX_PROVIDE_KEYS in all,
        # which is what fits MAX_REQUEST_SIZE
        self.assertEqual(MAX_PROVIDE_KEYS, most)
        self.assertIsNone(protocol.parse_provide_request(protocol.encode_provide(keys, [])))
        self.assertIsNone(protocol.parse_provide_request(protocol.encode_provide(keys[:30], keys[30:56])))
        self.assertEqual(protocol.parse_provide_request(protocol.encode_provide(keys[:30], keys[30:55])),
                         (keys[:30], keys[30:55]))
        # The request server lets RNS accept up to MAX_REQUEST_SIZE and records every key of such a request
        backend = FakeBackend()
        destination = FakeDestination()
        server.RequestServer(destination, Config(), backend, clock=FakeClock(), start_monitor=False)
        self.assertEqual(destination.max_request_size, MAX_REQUEST_SIZE)
        link = FakeLink(b"l" * 16)
        destination.links.append(link)
        destination.established(link)
        identity = RNS.Identity()
        handler = destination.handlers[PROVIDE_PATH][0]
        self.assertIs(handler(PROVIDE_PATH, protocol.encode_provide(keys[:most], []), bytes(16), link.link_id,
                              identity, 0.0), True)
        self.assertEqual(backend.provided, [(node_id_for_identity(identity), keys[:most], [])])
        # rnfs's own client batches to one packet at the default link MDU, and never sends more than the server takes
        self.assertEqual(len(protocol.provide_batches(keys, [], None)[0][0]), 22)
        batches = protocol.provide_batches(keys[:600], keys[600:], 1_000_000)
        self.assertEqual(max(len(stored) + len(withdrawn) for stored, withdrawn in batches), MAX_PROVIDE_KEYS)
        # One key more than that is refused as malformed, recording nothing
        backend.provided.clear()
        self.assertIs(handler(PROVIDE_PATH, protocol.encode_provide(keys[:most + 1], []), bytes(16), link.link_id,
                              identity, 0.0), False)
        self.assertEqual(backend.provided, [])


if __name__ == "__main__":
    unittest.main()
