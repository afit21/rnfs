"""M4: a provider store filled by about 100 publishers refuses every publisher it hasn't seen yet, this node included.

A ProviderStore of the default size (Config().max_records) is filled to the brim by 100 identities storing their
per-publisher maximum each. A publisher that stored before the flood is still accepted; a new one, and the node's
own ID with no records yet, are refused. Passing tests pin down today's limits (the per-publisher cap, and how many
keys one /provide request may carry on the server side), which the fix may change.
"""
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import ID_LENGTH, MAX_REQUEST_SIZE, PROVIDE_PATH
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

    # M4: fails until a full store keeps room for newcomers (a newcomer pool, or an allowance that grows with age)
    @unittest.expectedFailure
    def test_new_publisher_can_store(self):
        accepted = sum(self.store.store(ident(10 + i), NEWCOMER) for i in range(10))
        self.assertGreater(accepted, 0, "a new honest publisher stored none of 10 records in the full store")

    # M4: fails until the node's own records are admitted to its full store whatever its first-seen time
    @unittest.expectedFailure
    def test_own_records_always_accepted(self):
        self.assertIsNone(self.store.first_seen(OWN))
        self.assertTrue(self.store.store(ident(3), OWN), "the node's own record was refused by its full store")


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
        """Today 55 keys fit a 1 KiB request; the server takes them all in one /provide. rnfs's client sends 22."""
        keys = [ident(1000 + i) for i in range(1000)]
        most = max(n for n in range(1, 100)
                   if protocol.request_size(PROVIDE_PATH, protocol.encode_provide(keys[:n], [])) <= MAX_REQUEST_SIZE)
        self.assertEqual(most, 55)
        # The parser itself has no count limit
        self.assertEqual(protocol.parse_provide_request(protocol.encode_provide(keys, [])), (keys, []))
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
        # rnfs's own client batches to one packet at the default link MDU
        self.assertEqual(len(protocol.provide_batches(keys, [], None)[0][0]), 22)


if __name__ == "__main__":
    unittest.main()
