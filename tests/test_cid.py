"""Tests for CIDs (specification section 4): binary and written forms, validation, DHT keys."""
import base64
import copy
import hashlib
import pickle
import unittest

from RNS.vendor import umsgpack

from rnfs.constants import ID_LENGTH
from rnfs.content import cid as cid_module
from rnfs.content.cid import DAG, EMPTY_DIRECTORY, EMPTY_DIRECTORY_BLOCK, EMPTY_FILE, RAW, Cid

HELLO_WORLD = "bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e"
EMPTY = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"


def written(binary: bytes) -> str:
    """binary as "b" + lowercase unpadded base32, without checking it."""
    return "b" + base64.b32encode(binary).decode().lower().rstrip("=")


class CidTest(unittest.TestCase):
    """Building, parsing and rejecting CIDs."""

    def test_raw_cid_matches_ipfs(self):
        # `ipfs add --cid-version 1 --raw-leaves` of "hello world" and of an empty file
        self.assertEqual(str(Cid.for_block(RAW, b"hello world")), HELLO_WORLD)
        self.assertEqual(str(EMPTY_FILE), EMPTY)

    def test_binary_forms(self):
        digest = hashlib.sha256(b"x").digest()
        raw = Cid.for_block(RAW, b"x")
        node = Cid.for_block(DAG, b"x")
        self.assertEqual(raw.binary, bytes.fromhex("01551220") + digest)
        self.assertEqual(node.binary, bytes.fromhex("018080c0011220") + digest)
        self.assertEqual((len(raw.binary), len(node.binary)), (36, 39))
        self.assertEqual((raw.codec, node.codec), (RAW, DAG))
        self.assertTrue(raw.is_raw)
        self.assertFalse(node.is_raw)
        self.assertEqual(raw.digest, digest)

    def test_round_trips(self):
        for value in (Cid.for_block(RAW, b"a"), Cid.for_block(DAG, b"a")):
            self.assertEqual(Cid.parse(str(value)), value)
            self.assertEqual(Cid.from_bytes(value.binary), value)
            self.assertEqual(hash(Cid.from_bytes(value.binary)), hash(value))

    def test_key(self):
        raw = Cid.for_block(RAW, b"same")
        node = Cid.for_block(DAG, b"same")
        self.assertEqual(len(raw.key), ID_LENGTH)
        self.assertEqual(raw.key, hashlib.sha256(raw.binary).digest()[:16])
        self.assertNotEqual(raw.key, node.key)  # identical bytes, different codecs, different keys

    def test_written_form_and_key_are_memoised(self):
        value = Cid.for_block(RAW, b"hello world")
        self.assertEqual(str(value), HELLO_WORLD)
        self.assertIs(str(value), str(value))  # computed once
        self.assertEqual(value.key, hashlib.sha256(value.binary).digest()[:ID_LENGTH])
        self.assertIs(value.key, value.key)
        # Memoising changes nothing a CID is compared, hashed or copied by
        fresh = Cid.from_bytes(value.binary)
        self.assertEqual(fresh, value)
        self.assertEqual(hash(fresh), hash(value))
        self.assertEqual({fresh: 1}[value], 1)
        self.assertEqual(repr(fresh), repr(value))
        self.assertEqual(repr(value), f"Cid({HELLO_WORLD})")
        self.assertEqual(Cid.parse(HELLO_WORLD), value)
        for copied in (copy.copy(value), copy.deepcopy(fresh), pickle.loads(pickle.dumps(value))):
            self.assertEqual(copied, value)
            self.assertEqual(hash(copied), hash(value))
            self.assertEqual(str(copied), HELLO_WORLD)
            self.assertEqual(copied.key, value.key)
            self.assertEqual(copied.codec, RAW)

    def test_verify(self):
        value = Cid.for_block(RAW, b"real")
        self.assertTrue(value.verify(b"real"))
        self.assertFalse(value.verify(b"fake"))

    def test_empty_directory(self):
        self.assertEqual(EMPTY_DIRECTORY_BLOCK, umsgpack.packb([2, []]))
        self.assertEqual(EMPTY_DIRECTORY, Cid.for_block(DAG, EMPTY_DIRECTORY_BLOCK))

    def test_rejects_malformed(self):
        digest = bytes(32)
        bad = {
            "not a string": b"bafk",
            "wrong multibase": "z" + HELLO_WORLD[1:],
            "empty": "",
            "too short": "bae",
            "truncated": HELLO_WORLD[:-4],
            "not base32": "b!!!!",
            "non-ascii": "bafkreié",
            "padded": HELLO_WORLD + "====",
            "uppercase": HELLO_WORLD.upper(),
            "uppercase body": "b" + HELLO_WORLD[1:].upper(),
            "unused bits set": HELLO_WORLD[:-1] + chr(ord(HELLO_WORLD[-1]) + 1),
            "CIDv0-like version": written(bytes([0, 0x55, 0x12, 0x20]) + digest),
            "dag-pb codec": written(bytes([1, 0x70, 0x12, 0x20]) + digest),
            "wrong hash": written(bytes([1, 0x55, 0x13, 0x20]) + digest),
            "short digest": written(bytes([1, 0x55, 0x12, 0x20]) + digest[:31]),
            "long digest": written(bytes([1, 0x55, 0x12, 0x20]) + digest + b"\0"),
            "dag codec, raw length": written(bytes.fromhex("018080c0011220") + digest[:29]),
        }
        for reason, value in bad.items():
            with self.subTest(reason):
                with self.assertRaises(ValueError):
                    Cid.parse(value)
                self.assertFalse(cid_module.is_valid(value))
        invalid = {"too short": b"\x01\x55", "not bytes": "abc", "dag-pb": bytes([1, 0x70, 0x12, 0x20]) + digest}
        for reason, binary in invalid.items():
            with self.subTest(reason):
                with self.assertRaises(ValueError):
                    Cid.from_bytes(binary)

    def test_ordering(self):
        a, b = sorted([Cid.for_block(RAW, b"1"), Cid.for_block(RAW, b"2")])
        self.assertLess(a.binary, b.binary)


if __name__ == "__main__":
    unittest.main()
