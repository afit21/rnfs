"""Tests for keepers and following (rnfs/features/keepers.py, follow.py): signed name records checked against
real RNS signatures made in-process, the /name handler, keeper store and DHT resolution, follow pin swaps.

Offline nodes are joined by a NetClient that also answers /name from the other node's handler.
"""
import io
import os
import tempfile
import unittest
from unittest import mock

import RNS
from RNS.vendor import umsgpack

from rnfs.config import Config
from rnfs.content.cid import RAW, Cid
from rnfs.features import keepers
from rnfs.features.follow import follow_source
from rnfs.features.keepers import (NAME_HASH, NAME_PATH, OWN, USER, KeeperStore, from_announce_data, name_key,
                                   parse_response, parse_signed, sign_record)
from rnfs.names import encode_record, name_for_identity
from rnfs.network import server
from rnfs.node import RnfsNode
from rnfs.publisher import DhtKey

try:
    from tests.test_names import FakeNetwork
    from tests.test_node import NetClient
    from tests.test_server import Clock, FakeBackend, FakeDestination, FakeLink
except ImportError:  # run from inside tests/
    from test_names import FakeNetwork
    from test_node import NetClient
    from test_server import Clock, FakeBackend, FakeDestination, FakeLink

A_ID, B_ID, C_ID = bytes([0xA] * 16), bytes([0xB] * 16), bytes([0xC] * 16)
CID1 = Cid.for_block(RAW, b"one")
CID2 = Cid.for_block(RAW, b"two")

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the nodes' log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


def signed(identity, sequence: int, cid: Cid, now: float = 1_700_000_000) -> keepers.SignedRecord:
    """A record of identity's name, signed in-process."""
    return sign_record(identity, name_for_identity(identity), encode_record(sequence, cid), now)


class SignedRecordTest(unittest.TestCase):
    """Building and checking signed records exactly as RNS signs announces."""

    def setUp(self):
        """A name key."""
        self.identity = RNS.Identity()
        self.name = name_for_identity(self.identity)

    def test_name_hash_is_rns_name_hash(self):
        expected = RNS.Identity.full_hash(RNS.Destination.expand_name(None, "rnfs", "name").encode())
        self.assertEqual(NAME_HASH, expected[:RNS.Identity.NAME_HASH_LENGTH // 8])

    def test_name_key_is_domain_separated(self):
        key = name_key(self.name)
        self.assertEqual(len(key), 16)
        self.assertNotEqual(key, Cid.for_block(RAW, self.name).key)
        self.assertNotEqual(name_key(bytes(16)), name_key(bytes([1]) + bytes(15)))

    def test_round_trip(self):
        record = signed(self.identity, 7, CID1)
        parsed = parse_signed(self.name, record.wire())
        self.assertEqual((parsed.sequence, parsed.cid, parsed.emitted), (7, CID1, 1_700_000_000))
        self.assertEqual(parse_response(self.name, umsgpack.unpackb(umsgpack.packb(record.wire()))), parsed)

    def test_rns_validates_our_signature(self):
        """A record signed here is a valid RNS announce: RNS's own check accepts it."""
        record = signed(self.identity, 1, CID1)
        out = RNS.Destination(self.identity, RNS.Destination.OUT, RNS.Destination.SINGLE, "rnfs", "name")
        data = record.public_key + NAME_HASH + record.random_hash + record.signature + record.app_data
        packet = RNS.Packet(out, data, RNS.Packet.ANNOUNCE)
        packet.pack()
        received = RNS.Packet(None, packet.raw)
        received.unpack()
        self.assertTrue(RNS.Identity.validate_announce(received, only_validate_signature=True))

    def test_real_rns_announce_verifies(self):
        """The data of an announce RNS itself builds is parsed and verified (the RNS packet cache path)."""
        # Without a running Reticulum, registering the destination with Transport is skipped
        with mock.patch.object(RNS.Transport, "register_destination", lambda destination: None):
            destination = RNS.Destination(self.identity, RNS.Destination.IN, RNS.Destination.SINGLE, "rnfs",
                                          "name")
        destination.set_default_app_data(encode_record(3, CID2))
        packet = destination.announce(send=False)
        record = from_announce_data(self.name, packet.data, packet.context_flag == RNS.Packet.FLAG_SET)
        self.assertIsNotNone(record)
        self.assertEqual((record.sequence, record.cid), (3, CID2))
        self.assertIsNone(from_announce_data(self.name, packet.data[:-1], False))
        self.assertIsNone(from_announce_data(self.name, packet.data, True))  # no ratchet in it

    def test_forgeries_rejected(self):
        record = signed(self.identity, 2, CID1)
        good = record.wire()
        other = RNS.Identity()

        def changed(index, value):
            """good with one field replaced."""
            fields = list(good)
            fields[index] = value
            return fields

        forged = [
            changed(5, encode_record(3, CID1)),  # app_data changed after signing
            changed(5, encode_record(2, CID2)),
            changed(4, bytes(64)),  # signature
            changed(4, record.signature[:-1] + bytes([record.signature[-1] ^ 1])),
            changed(2, bytes(10)),  # random hash (the emission time is signed)
            changed(0, other.get_public_key()),  # someone else's key
            changed(1, bytes(10)),  # another aspect's name hash
            changed(3, bytes(32)),  # a ratchet that wasn't signed
        ]
        for fields in forged:
            self.assertIsNone(parse_signed(self.name, fields), fields)
        # A valid record of another name, offered for this one
        self.assertIsNone(parse_signed(self.name, signed(other, 9, CID1).wire()))

    def test_malformed_rejected(self):
        good = signed(self.identity, 2, CID1).wire()
        for data in (None, False, True, 0, b"", "", {}, [], good[:5], good + [b""], [*good[:5], "text"],
                     [*good[:5], None], [*good[:5], 5], [bytearray(f) for f in good], (tuple(good)),
                     [good[0][:-1], *good[1:]], [good[0] + b"\0", *good[1:]], [*good[:3], b"\0" * 31, *good[4:]],
                     [*good[:5], good[5] + b"\0"], [*good[:5], b"\x00" * 65]):
            self.assertIsNone(parse_response(self.name, data), data)
        self.assertIsNone(parse_signed(b"\0" * 15, good))
        self.assertIsNone(parse_signed("x" * 16, good))

    def test_non_canonical_record_rejected(self):
        """app_data that umsgpack would accept but isn't the one canonical encoding is refused, even if signed."""
        app_data = encode_record(2, CID1) + b"\0"  # trailing byte
        name = self.name
        random_hash = bytes(5) + (1_700_000_000).to_bytes(5, "big")
        public_key = self.identity.get_public_key()
        signature = self.identity.sign(keepers.signed_data(name, public_key, random_hash, b"", app_data))
        self.assertIsNone(parse_signed(name, [public_key, NAME_HASH, random_hash, b"", signature, app_data]))

    def test_response_fits_one_packet(self):
        self.assertLessEqual(keepers.max_response_size(), RNS.Link.MDU)
        packed = umsgpack.packb([bytes(16), signed(self.identity, 2**64 - 1, Cid.for_block("dag", b"x")).wire()])
        self.assertLessEqual(len(packed), keepers.max_response_size())

    def test_newer_than(self):
        old = signed(self.identity, 2, CID1, now=1000)
        self.assertTrue(signed(self.identity, 3, CID1, now=900).newer_than(old))
        self.assertTrue(signed(self.identity, 2, CID1, now=1001).newer_than(old))
        self.assertFalse(signed(self.identity, 2, CID2, now=2000).newer_than(old))  # same sequence, other CID
        self.assertFalse(signed(self.identity, 1, CID1, now=2000).newer_than(old))


class KeeperStoreTest(unittest.TestCase):
    """Kept names, reasons, the bound and persistence."""

    def setUp(self):
        """A store in a temporary directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "keepers.json")
        self.identity = RNS.Identity()
        self.name = name_for_identity(self.identity)

    def test_only_kept_names_store_newer_records(self):
        store = KeeperStore(self.path, 10)
        self.assertFalse(store.offer(signed(self.identity, 1, CID1)))  # not kept
        store.keep(self.name, USER)
        self.assertTrue(store.offer(signed(self.identity, 2, CID1)))
        self.assertFalse(store.offer(signed(self.identity, 1, CID2)))  # rollback
        self.assertEqual(store.get(self.name).sequence, 2)
        self.assertEqual(store.provided_keys(), {name_key(self.name)})

    def test_persisted_and_reverified(self):
        store = KeeperStore(self.path, 10)
        store.keep(self.name, USER)
        store.offer(signed(self.identity, 5, CID2))
        store.save()
        again = KeeperStore(self.path, 10)
        self.assertEqual((again.get(self.name).sequence, again.reasons(self.name)), (5, {USER}))
        # A record damaged on disk is dropped rather than served
        with open(self.path, encoding="utf-8") as f:
            text = f.read()
        record_hex = again.get(self.name).signature.hex()
        with open(self.path, "w", encoding="utf-8") as f:
            f.write(text.replace(record_hex, "00" * 64))
        self.assertIsNone(KeeperStore(self.path, 10).get(self.name))

    def test_bound_excludes_own_names(self):
        store = KeeperStore(self.path, 1)
        store.keep(bytes([1] * 16), USER)
        with self.assertRaises(ValueError):
            store.keep(bytes([2] * 16), USER)
        store.keep(bytes([3] * 16), OWN)  # own names don't count
        store.keep(bytes([1] * 16), "follow")  # another reason for a kept name doesn't count either
        self.assertEqual(store.count(), 2)

    def test_release_drops_record_with_last_reason(self):
        store = KeeperStore(self.path, 10)
        store.keep(self.name, USER)
        store.keep(self.name, "follow")
        store.offer(signed(self.identity, 1, CID1))
        self.assertTrue(store.release(self.name, USER))
        self.assertIsNotNone(store.get(self.name))
        self.assertFalse(store.release(self.name, USER))
        store.release(self.name, "follow")
        self.assertIsNone(store.get(self.name))
        self.assertEqual(store.provided_keys(), set())


class KeeperNet(NetClient):
    """NetClient that also answers /name from the other node's handler, and can stand in a forger."""

    def __init__(self):
        """No forged answers."""
        super().__init__()
        self.forged: dict[bytes, object] = {}
        self.name_requests: list[bytes] = []

    def name_record(self, node_id, name):
        """The node's /name answer as the request server would send it (after an RNS msgpack round trip)."""
        self.name_requests.append(node_id)
        if node_id in self.forged:
            return self.forged[node_id]
        node = self.nodes.get(node_id)
        if node is None:
            return None
        handler = node.backend.feature_requests()[NAME_PATH]
        return umsgpack.unpackb(umsgpack.packb(handler(name, None)))


class KeeperNodesTest(unittest.TestCase):
    """Owner A, resolver B and keeper C as offline nodes."""

    def setUp(self):
        """Three nodes; B and C know each other as DHT servers."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = KeeperNet()
        self.net.sender = C_ID
        self.a = self.make_node("a", A_ID)
        self.b = self.make_node("b", B_ID)
        self.c = self.make_node("c", C_ID)
        for node, others in ((self.a, (B_ID, C_ID)), (self.b, (A_ID, C_ID)), (self.c, (A_ID, B_ID))):
            for other in others:
                node.routing.heard(other)
        self.name = bytes.fromhex(self.a.name_create("site")["name"])

    def make_node(self, label: str, node_id: bytes, **settings) -> RnfsNode:
        """An offline node with its own directory."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, label), slow="no", lookup_timeout=5,
                        path_timeout=0.2, **settings)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def publish(self, data: bytes) -> Cid:
        """Add data on A and point A's name at it."""
        cid = self.a.add_stream(io.BytesIO(data))["cid"]
        self.a.name_publish("site", cid)
        return Cid.parse(cid)

    def advertise(self, keeper: RnfsNode, keeper_id: bytes):
        """keeper publishes what it provides, storing provider records on the other nodes."""
        self.net.sender = keeper_id
        keeper.publisher.publish_due()

    def test_owner_keeps_and_advertises_its_own_names(self):
        cid = self.publish(b"v1")
        kept = self.a.feature("keepers").kept()
        self.assertEqual([(k["name"], k["reasons"], k["cid"]) for k in kept], [(self.name.hex(), [OWN], str(cid))])
        self.assertIn(name_key(self.name), self.a.publisher.provided_keys())
        self.assertNotIn(DhtKey(name_key(self.name)), self.a.publisher.provided())  # provided() stays CIDs
        self.advertise(self.a, A_ID)
        self.assertIn(A_ID, self.b.records.providers(name_key(self.name)))
        # Removing the name stops keeping it
        self.a.name_rm("site")
        self.a.feature("keepers").sync_own()
        self.assertEqual(self.a.feature("keepers").kept(), [])
        self.assertNotIn(name_key(self.name), self.a.publisher.provided_keys())

    def test_keeper_serves_after_owner_is_gone(self):
        cid = self.publish(b"v1")
        self.advertise(self.a, A_ID)
        keeper = self.c.feature("keepers")
        keeper.keep(self.name)
        keeper.run_pending()  # asks the keepers it finds: A, the owner
        self.assertEqual(keeper.store.get(self.name).cid, cid)
        self.advertise(self.c, C_ID)
        # A goes away; B resolves: the path request hears nothing, so B asks keepers and finds C
        del self.net.nodes[A_ID]
        result = self.b.name_resolve(self.name.hex())
        self.assertEqual((result["cid"], result["sequence"]), (str(cid), 1))
        self.assertIn(C_ID, self.net.name_requests)

    def test_no_rollback_from_keepers(self):
        self.publish(b"v1")
        self.advertise(self.a, A_ID)
        keeper = self.c.feature("keepers")
        keeper.keep(self.name)
        keeper.run_pending()
        self.advertise(self.c, C_ID)
        # B already persisted sequence 2 (heard while A was online)
        cid2 = self.publish(b"v2")
        self.b.names.offer(self.name, 2, cid2, 0.0)
        self.b.name_resolve(self.name.hex())
        del self.net.nodes[A_ID]
        self.net.forged[A_ID] = None
        result = self.b.name_resolve(self.name.hex())  # C still serves sequence 1
        self.assertEqual((result["sequence"], result["cid"]), (2, str(cid2)))

    def test_forged_answers_rejected(self):
        cid = self.publish(b"v1")
        self.advertise(self.a, A_ID)
        good = self.a.feature("keepers").store.get(self.name)
        attacker = RNS.Identity()
        forged_record = signed(attacker, 99, CID2)
        tampered = good.wire()
        tampered[5] = encode_record(99, CID2)
        for answer in (forged_record.wire(), tampered, False, None, [b"x"], {"a": 1}):
            self.net.forged[A_ID] = answer
            self.assertEqual(self.b.feature("keepers").ask_keepers(self.name), [], answer)
        self.assertIsNone(self.b.names.record(self.name))
        del self.net.forged[A_ID]
        self.assertEqual([r.cid for r in self.b.feature("keepers").ask_keepers(self.name)], [cid])

    def test_heard_newer_announce_refreshes_kept_record(self):
        self.publish(b"v1")
        self.advertise(self.a, A_ID)
        keeper = self.c.feature("keepers")
        keeper.keep(self.name)
        keeper.run_pending()
        self.c.names.start()  # registers the announce handler with the fake network
        cid2 = self.publish(b"v2")
        self.c.names._network.announce(self.name, encode_record(2, cid2), 1.0)  # as RNS delivers it
        self.assertEqual(keeper.store.get(self.name).sequence, 1)  # no packet cache offline: asked later
        keeper.run_pending()
        self.assertEqual((keeper.store.get(self.name).sequence, keeper.store.get(self.name).cid), (2, cid2))

    def test_unkeep_withdraws(self):
        self.publish(b"v1")
        self.advertise(self.a, A_ID)
        keeper = self.c.feature("keepers")
        keeper.keep(self.name)
        keeper.run_pending()
        self.assertIn(name_key(self.name), self.c.publisher.provided_keys())
        keeper.unkeep(self.name)
        self.assertNotIn(name_key(self.name), self.c.publisher.provided_keys())
        with self.assertRaises(Exception):
            keeper.unkeep(self.name)

    def test_keep_bound(self):
        node = self.make_node("small", bytes([0xD] * 16), keep_max_names=1)
        node.feature("keepers").keep(bytes([1] * 16))
        with self.assertRaises(ValueError):
            node.feature("keepers").keep(bytes([2] * 16))

    def test_handler_answers_strictly(self):
        handler = self.a.backend.feature_requests()[NAME_PATH]
        self.assertEqual(handler(self.name, None), [])  # not published yet: nothing kept
        for data in (None, b"", b"\0" * 15, b"\0" * 17, "x" * 16, [self.name], 5, True):
            self.assertIs(handler(data, None), False)
        self.publish(b"v1")
        self.assertIsNotNone(parse_response(self.name, handler(self.name, None)))

    def test_follow_swaps_pins(self):
        cid1 = self.publish(b"version one")
        self.advertise(self.a, A_ID)
        self.a.records.store(cid1.key, A_ID)
        follow = self.b.feature("follow")
        follow.follow(self.name)
        self.assertIn("follow", self.b.feature("keepers").store.reasons(self.name))
        self.assertTrue(follow.check(self.name))
        source = follow_source(self.name)
        self.assertEqual(self.b.storage.pins().get(cid1), {source})
        self.assertTrue(self.b.storage.is_complete(cid1))
        # The name moves: the new target is pinned, then the old one released
        cid2 = self.publish(b"version two")
        self.a.records.store(cid2.key, A_ID)
        self.b.names.offer(self.name, 2, cid2, 0.0)
        self.assertTrue(follow.check(self.name, network=False))
        pins = self.b.storage.pins()
        self.assertEqual(pins.get(cid2), {source})
        self.assertNotIn(cid1, pins)
        self.assertEqual(follow.following()[0]["pinned"], str(cid2))
        # A target that can't be fetched keeps the old pin
        missing = Cid.for_block(RAW, b"nobody has this")
        self.b.names.offer(self.name, 3, missing, 0.0)
        self.assertFalse(follow.check(self.name, network=False))
        self.assertEqual(self.b.storage.pins().get(cid2), {source})
        # Unfollowing releases the pin and the keeper reason
        follow.unfollow(self.name)
        self.assertNotIn(cid2, self.b.storage.pins())
        self.assertNotIn("follow", self.b.feature("keepers").store.reasons(self.name))
        self.assertEqual(follow.following(), [])

    def test_follow_persists(self):
        cid1 = self.publish(b"persisted")
        self.a.records.store(cid1.key, A_ID)
        self.advertise(self.a, A_ID)
        self.b.feature("follow").follow(self.name)
        self.b.feature("follow").check(self.name)
        self.b.stop()
        again = RnfsNode.offline(self.b.config, B_ID, self.net, FakeNetwork())
        self.addCleanup(again.stop)
        self.assertEqual(again.feature("follow").following()[0]["pinned"], str(cid1))
        self.assertIn("follow", again.feature("keepers").store.reasons(self.name))


class NameRequestServerTest(unittest.TestCase):
    """/name registered on the request server through Feature.requests: limits and private mode apply."""

    def make_server(self, config, handler):
        """A server whose backend adds /name."""
        backend = FakeBackend(members={b"m" * 16})
        backend.feature_requests = lambda: {NAME_PATH: handler}
        destination = FakeDestination()
        server.RequestServer(destination, config, backend, clock=Clock(), start_monitor=False)
        link = FakeLink(b"l" * 16)
        destination.links.append(link)
        destination.established(link)
        return destination, link

    def ask(self, destination, link, data, identity=None):
        """Call /name's handler as RNS would."""
        return destination.handlers[NAME_PATH][0](NAME_PATH, data, bytes(16), link.link_id, identity, 0.0)

    def test_registered_and_answers(self):
        destination, link = self.make_server(Config(), lambda data, identity: [] if data == b"n" * 16 else None)
        self.assertEqual(destination.handlers[NAME_PATH][1:], (RNS.Destination.ALLOW_ALL, False))
        self.assertEqual(self.ask(destination, link, b"n" * 16), [])
        self.assertIs(self.ask(destination, link, b"x"), False)  # None would leave the requester waiting

    def test_errors_answer_false(self):
        def broken(data, identity):
            raise RuntimeError("bug")
        destination, link = self.make_server(Config(), broken)
        self.assertIs(self.ask(destination, link, b"n" * 16), False)

    def test_private_mode(self):
        destination, link = self.make_server(Config(private=True), lambda data, identity: [])
        self.assertIs(self.ask(destination, link, b"n" * 16), False)
        member = RNS.Identity(create_keys=False)
        member.hash = b"m" * 16
        self.assertEqual(self.ask(destination, link, b"n" * 16, member), [])


if __name__ == "__main__":
    unittest.main()
