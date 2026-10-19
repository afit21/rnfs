"""M4: a provider store filled by about 100 publishers refused every publisher it hadn't seen yet, this node included.

Rebuilt against the 0.2.0 store (a byte budget and storage tiers instead of max_records and per-publisher caps): a
store at its budget, holding an established (Earned) publisher's records and filled to the brim by 100 fresh
identities. The established publisher is still accepted, and so are a new publisher and the node's own records (the
Newcomer tier makes room from its largest holders; own records are always admitted). Further tests pin down what
replaced the per-publisher caps: no cap while there is room, refresh days kept a TTL past a publisher's last record,
and across a restart; and how many keys one /provide request may carry on the server side.
"""
import os
import tempfile
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import DAY, ID_LENGTH, MAX_PROVIDE_KEYS, MAX_REQUEST_SIZE, PROVIDE_PATH, PROVIDER_TTL
from rnfs.dht.providers import EARNED_DAYS, ProviderStore
from rnfs.network import protocol, server
from rnfs.network.identity import node_id_for_identity
from tests.support import FakeClock
from tests.test_server import FakeBackend, FakeDestination, FakeLink

OWN = bytes([0x01] * ID_LENGTH)
HONEST_OLD = bytes([0x02] * ID_LENGTH)
NEWCOMER = bytes([0x03] * ID_LENGTH)
FLOODERS = 100
RECORDS = 100_000  # the store's capacity in these tests (the default budget holds millions)
SIZE = 117  # bytes a record takes (fixed here; the node measures it)


def ident(n: int) -> bytes:
    """The 16-byte ID whose big-endian value is n."""
    return n.to_bytes(ID_LENGTH, "big")


def earn(store: ProviderStore, clock: FakeClock, publisher: bytes, key: bytes):
    """Make publisher Earned: it stores key, and refreshes it on EARNED_DAYS later days."""
    assert store.store(key, publisher)
    for _ in range(EARNED_DAYS):
        clock.now += DAY
        assert store.store(key, publisher)


class FullStoreTest(unittest.TestCase):
    """A store of RECORDS records: one established publisher, then FLOODERS fresh identities filling it."""

    def setUp(self):
        """The store, filled; every flooder stores a hundredth of it, a second after the one before."""
        self.clock = FakeClock(1_700_000_000.0)
        self.store = ProviderStore(None, RECORDS * SIZE, clock=self.clock, own_id=OWN, record_bytes=SIZE)
        earn(self.store, self.clock, HONEST_OLD, ident(1))
        key = 1 << 64
        for flooder in range(FLOODERS):
            publisher = ident((0xF << 120) + flooder)
            for _ in range(RECORDS // FLOODERS):
                key += 1
                self.store.store(ident(key), publisher)
            self.clock.now += 1
        self.assertTrue(self.store.is_full())

    def test_flooders_fill_the_store(self):
        """100 publishers at 1% each fill it: no per-publisher cap holds them back while there is room."""
        self.assertEqual(self.store.publisher_count(), FLOODERS + 1)
        self.assertEqual(self.store.record_count(), RECORDS)

    def test_established_publisher_accepted(self):
        self.assertTrue(self.store.store(ident(2), HONEST_OLD))
        self.assertEqual(self.store.record_count(), RECORDS)

    def test_new_publisher_can_store(self):
        accepted = sum(self.store.store(ident(10 + i), NEWCOMER) for i in range(10))
        self.assertEqual(accepted, 10, "a new honest publisher was refused by the full store")

    def test_own_records_always_accepted(self):
        self.assertIsNone(self.store.first_seen(OWN))
        self.assertTrue(self.store.store(ident(3), OWN), "the node's own record was refused by its full store")

    def test_own_records_never_refused(self):
        """However many: the node's own go over the budget rather than be refused, and evict no one."""
        self.assertEqual(sum(self.store.store(ident(10_000 + i), OWN) for i in range(2000)), 2000)
        self.assertEqual(self.store.record_count(), RECORDS + 2000)
        self.assertEqual(self.store.providers(ident(1)), [HONEST_OLD])

    def test_newcomers_only_churn_newcomers(self):
        """A flood of new identities displaces only the Newcomers' records: the established publisher keeps
        everything, and each new one gets in."""
        self.assertEqual(sum(self.store.store(ident(10_000 + i), ident((0xE << 120) + i)) for i in range(2000)), 2000)
        self.assertEqual(self.store.record_count(), RECORDS)
        self.assertEqual(self.store.providers(ident(1)), [HONEST_OLD])


class NoCapTest(unittest.TestCase):
    """While there is room, no per-publisher cap: a large publisher (a mirror) stores all it has."""

    def test_a_publisher_may_hold_far_more_than_a_hundredth(self):
        store = ProviderStore(None, RECORDS * SIZE, clock=FakeClock(1_700_000_000.0), record_bytes=SIZE)
        publisher = ident(7)
        self.assertEqual(sum(store.store(ident(100 + i), publisher) for i in range(5000)), 5000)

    def test_the_default_budget(self):
        """1 GiB, millions of records at the size a record takes on disk."""
        self.assertEqual(Config().record_budget, 1 << 30)
        self.assertEqual(ProviderStore(None).record_budget, 1 << 30)


class StandingTest(unittest.TestCase):
    """What took the place of the first-seen grace: refresh days, kept a TTL past a publisher's last record."""

    def setUp(self):
        """A store on a temporary state directory."""
        self.clock = FakeClock(1_700_000_000.0)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.state = os.path.join(directory.name, "state")

    def open(self) -> ProviderStore:
        """A store on self.state, closed when the test ends."""
        store = ProviderStore(self.state, 100 * SIZE, clock=self.clock, record_bytes=SIZE)
        self.addCleanup(store.close)
        return store

    def test_refresh_days_kept_a_ttl_after_the_last_record_goes(self):
        store = self.open()
        publisher = ident(7)
        earn(store, self.clock, publisher, ident(100))
        store.withdraw(ident(100), publisher)
        self.assertIsNone(store.first_seen(publisher))
        # Back within a TTL: still Earned
        self.clock.now += PROVIDER_TTL - DAY
        store.maintain()
        self.assertTrue(store.store(ident(101), publisher))
        self.assertEqual(store.standing(publisher)["tier"], "earned")
        store.withdraw(ident(101), publisher)
        # A TTL after its last refresh day, forgotten: new again
        self.clock.now += PROVIDER_TTL + DAY
        store._last_hourly = float("-inf")
        store.maintain()
        self.assertIsNone(store.standing(publisher))
        self.assertTrue(store.store(ident(102), publisher))
        self.assertEqual(store.standing(publisher)["tier"], "newcomer")

    def test_fresh_identities_leave_nothing_behind(self):
        """A publisher that never refreshed a record has no standing to keep once its records go: a flood of fresh
        identities leaves no rows."""
        store = self.open()
        for n in range(50):
            store.store(ident(100 + n), ident(1000 + n))
            store.withdraw(ident(100 + n), ident(1000 + n))
        store.save()
        self.assertEqual(store.publisher_count(), 0)
        self.assertEqual([store.standing(ident(1000 + n)) for n in range(50)], [None] * 50)

    def test_standing_survives_a_restart(self):
        store = self.open()
        earn(store, self.clock, ident(9), ident(1))
        store.save()
        store.close()
        loaded = self.open()
        self.assertEqual(loaded.standing(ident(9))["tier"], "earned")
        self.assertEqual(loaded.standing(ident(9))["days"], EARNED_DAYS)


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
