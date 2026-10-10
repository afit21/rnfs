"""Tests for encoding and strictly validating rnfs.dht messages, and for request batch sizes."""
import unittest

import RNS
from RNS.vendor import umsgpack

from rnfs.constants import (BLOCKS_PATH, CHUNK_SIZE, FIND_PATH, ID_LENGTH, K, KIB, MAX_BLOCK_SIZE, MAX_REQUEST_SIZE,
                            MAX_RESPONSE_SIZE, PROVIDE_PATH)
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.network import protocol

KEY = bytes(range(ID_LENGTH))
RAW_CID = Cid.for_block(RAW, b"raw block")
DAG_CID = Cid.for_block(DAG, b"dag block")


def keys(count: int) -> list[bytes]:
    """count distinct 16-byte keys."""
    return [i.to_bytes(ID_LENGTH, "big") for i in range(count)]


class RequestSizeTest(unittest.TestCase):
    """Request sizes are computed exactly as Link.request packs them."""

    def test_matches_link_request_packing(self):
        data = [[KEY] * 3, []]
        path_hash = RNS.Identity.truncated_hash(PROVIDE_PATH.encode("utf-8"))
        expected = len(umsgpack.packb([1700000000.123, path_hash, data]))
        self.assertEqual(protocol.request_size(PROVIDE_PATH, data), expected)

    def test_framing_is_28_bytes(self):
        # Array header, 9-byte float timestamp, 18-byte bin path hash; data packs separately
        self.assertEqual(protocol.request_size(FIND_PATH, None) - 1, 28)

    def test_limit_is_mdu_capped_by_max_request_size(self):
        self.assertEqual(protocol.request_limit(None), RNS.Link.MDU)
        self.assertEqual(protocol.request_limit(300), 300)
        self.assertEqual(protocol.request_limit(8000), MAX_REQUEST_SIZE)

    def test_default_mdu(self):
        self.assertEqual(protocol.DEFAULT_MDU, 431)


class ProvideBatchTest(unittest.TestCase):
    """/provide batching: as few requests as fit in one packet each."""

    def test_22_keys_per_request_at_default_mtu(self):
        batches = protocol.provide_batches(keys(100), [], None)
        self.assertEqual([len(k) for k, _ in batches], [22, 22, 22, 22, 12])

    def test_counts_both_lists(self):
        batches = protocol.provide_batches(keys(15), keys(30)[15:], None)
        self.assertEqual(len(batches[0][0]) + len(batches[0][1]), 22)
        self.assertEqual(sum(len(k) + len(w) for k, w in batches), 30)

    def test_every_batch_fits_and_order_is_kept(self):
        stored, withdrawn = keys(40), keys(80)[40:]
        for mdu in (None, 200, 1000):
            with self.subTest(mdu=mdu):
                batches = protocol.provide_batches(stored, withdrawn, mdu)
                for k, w in batches:
                    self.assertTrue(protocol.fits(PROVIDE_PATH, protocol.encode_provide(k, w), mdu))
                self.assertEqual([x for k, _ in batches for x in k], stored)
                self.assertEqual([x for _, w in batches for x in w], withdrawn)

    def test_larger_mtu_is_capped_at_max_request_size(self):
        batches = protocol.provide_batches(keys(200), [], 8000)
        size = protocol.request_size(PROVIDE_PATH, protocol.encode_provide(batches[0][0], []))
        self.assertLessEqual(size, MAX_REQUEST_SIZE)
        self.assertGreater(len(batches[0][0]), 22)

    def test_empty(self):
        self.assertEqual(protocol.provide_batches([], [], None), [])


class FindTest(unittest.TestCase):
    """/find requests and responses."""

    def test_request_round_trip(self):
        self.assertEqual(protocol.parse_find_request(protocol.encode_find(KEY)), KEY)

    def test_request_rejects_malformed(self):
        for data in (KEY[:-1], KEY + b"\0", "a" * ID_LENGTH, [KEY], None, bytearray(KEY)):
            with self.subTest(data=data):
                self.assertIsNone(protocol.parse_find_request(data))

    def test_response_round_trip_and_truncation(self):
        data = protocol.encode_find_response([KEY] * (K + 3), keys(K + 5))
        self.assertEqual(len(data[0]), K)
        self.assertEqual(protocol.parse_find_response(data), ([KEY] * K, keys(K)))

    def test_response_rejects_malformed(self):
        bad = {
            "refusal": False,
            "not a list": ([KEY], [KEY]),
            "one list": [[KEY]],
            "three lists": [[KEY], [KEY], []],
            "short id": [[KEY[:-1]], []],
            "too many providers": [[KEY] * (K + 1), []],
            "too many nodes": [[], [KEY] * (K + 1)],
            "id not bytes": [["x" * 16], []],
        }
        for reason, data in bad.items():
            with self.subTest(reason):
                self.assertIsNone(protocol.parse_find_response(data))

    def test_full_response_fits_one_packet(self):
        response = protocol.encode_find_response([KEY] * K, [KEY] * K)
        self.assertLessEqual(protocol.packed_response_size(bytes(16), response), RNS.Link.MDU)


class ProvideTest(unittest.TestCase):
    """/provide requests and responses."""

    def test_round_trip(self):
        self.assertEqual(protocol.parse_provide_request(protocol.encode_provide([KEY], [])), ([KEY], []))

    def test_rejects_malformed(self):
        bad = {"not a list": KEY, "one list": [[KEY]], "short key": [[KEY[:-1]], []],
               "withdrawn not list": [[KEY], KEY], "extra": [[KEY], [], []]}
        for reason, data in bad.items():
            with self.subTest(reason):
                self.assertIsNone(protocol.parse_provide_request(data))

    def test_only_true_is_accepted(self):
        self.assertTrue(protocol.is_accepted(True))
        for data in (False, None, 1, [True], b"\x01"):
            with self.subTest(data=data):
                self.assertFalse(protocol.is_accepted(data))


class BlocksTest(unittest.TestCase):
    """/blocks requests, responses and sizes."""

    def test_request_round_trip(self):
        data = protocol.encode_blocks(CHUNK_SIZE, [RAW_CID, DAG_CID])
        self.assertEqual(protocol.parse_blocks_request(data), (CHUNK_SIZE, [RAW_CID, DAG_CID]))

    def test_request_rejects_malformed(self):
        bad = {
            "bool max_bytes": [True, [RAW_CID.binary]],
            "negative max_bytes": [-1, [RAW_CID.binary]],
            "float max_bytes": [1.0, [RAW_CID.binary]],
            "cids not list": [10, RAW_CID.binary],
            "invalid cid": [10, [RAW_CID.binary[:-1]]],
            "cid as string": [10, [str(RAW_CID)]],
            "extra field": [10, [], None],
            "not a list": 10,
        }
        for reason, data in bad.items():
            with self.subTest(reason):
                self.assertIsNone(protocol.parse_blocks_request(data))

    def test_zero_max_bytes_is_valid(self):
        self.assertEqual(protocol.parse_blocks_request([0, []]), (0, []))

    def test_response(self):
        self.assertEqual(protocol.parse_blocks_response([b"a", None], 2), [b"a", None])
        self.assertEqual(protocol.parse_blocks_response([], 2), [])

    def test_response_rejects_malformed(self):
        bad = {
            "refusal": (False, 1),
            "longer than request": ([b"a", b"b"], 1),
            "entry not bytes": (["a"], 1),
            "entry a list": ([[b"a"]], 1),
            "oversized block": ([bytes(MAX_BLOCK_SIZE + 1)], 1),
        }
        for reason, (data, requested) in bad.items():
            with self.subTest(reason):
                self.assertIsNone(protocol.parse_blocks_response(data, requested))

    def test_batch_size_fits(self):
        cids = [Cid.for_block(RAW, bytes([i])) for i in range(30)]
        for max_bytes in (0, CHUNK_SIZE, MAX_RESPONSE_SIZE):
            with self.subTest(max_bytes=max_bytes):
                count = protocol.blocks_batch_size(max_bytes, cids, None)
                self.assertTrue(protocol.fits(BLOCKS_PATH, protocol.encode_blocks(max_bytes, cids[:count]), None))
                self.assertFalse(protocol.fits(BLOCKS_PATH, protocol.encode_blocks(max_bytes, cids[:count + 1]), None))
        # 28 framing + 1 list + 3 max_bytes (uint16) + 3 list header... ~10 raw CIDs of 38 bytes in 431
        self.assertEqual(protocol.blocks_batch_size(CHUNK_SIZE, cids, None), 10)

    def test_max_response_size(self):
        self.assertEqual(protocol.max_response_size(CHUNK_SIZE), MAX_BLOCK_SIZE + KIB)
        self.assertEqual(protocol.max_response_size(500 * KIB), 501 * KIB)
        self.assertEqual(protocol.max_response_size(10 * 1024 * KIB), MAX_RESPONSE_SIZE)


class PinTest(unittest.TestCase):
    """/pin requests."""

    def test_round_trip(self):
        for pin in (True, False):
            with self.subTest(pin=pin):
                self.assertEqual(protocol.parse_pin_request(protocol.encode_pin(DAG_CID, pin)), (DAG_CID, pin))

    def test_rejects_malformed(self):
        bad = {"int pin": [RAW_CID.binary, 1], "bad cid": [b"x", True], "missing pin": [RAW_CID.binary],
               "not a list": RAW_CID.binary, "string cid": [str(RAW_CID), True]}
        for reason, data in bad.items():
            with self.subTest(reason):
                self.assertIsNone(protocol.parse_pin_request(data))


if __name__ == "__main__":
    unittest.main()
