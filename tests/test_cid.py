"""Tests for CIDs (specification section 4): binary and written forms, validation, DHT keys."""
import base64
import copy
import hashlib
import pickle
import random
import unittest

from RNS.vendor import umsgpack

from rnfs.constants import ID_LENGTH
from rnfs.content import cid as cid_module
from rnfs.content.cid import DAG, EMPTY_DIRECTORY, EMPTY_DIRECTORY_BLOCK, EMPTY_FILE, RAW, Cid, from_base36, to_base36

HELLO_WORLD = "bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e"
EMPTY = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"


def written(binary: bytes) -> str:
    """binary as "b" + lowercase unpadded base32, without checking it."""
    return "b" + base64.b32encode(binary).decode().lower().rstrip("=")


def malformed() -> dict[str, object]:
    """Reason -> a value that is not a valid written CID."""
    digest = bytes(32)
    return {
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
        bad = malformed()
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

    def test_from_block_name_accepts_exactly_what_parse_accepts(self):
        # The block-directory scan's cheaper check (S2) must agree with parse() on every name, valid or not
        rng = random.Random(4)
        valid = [str(Cid.for_block(codec, bytes([i]))) for codec in (RAW, DAG) for i in range(4)]
        names = list(valid) + list(malformed().values()) + [None, 1, HELLO_WORLD.encode()]
        # One digit replaced: by base32 digits (most of which set spare bits in the final digit), by other ASCII,
        # and by non-ASCII whose upper() is ASCII ("ı" -> "I", "ſ" -> "S")
        pool = cid_module._BASE32 + cid_module._BASE32.upper() + "0189=_ -!\n" + "ıſéK"
        for name in valid[::3]:
            for i in range(len(name)):
                names += [name[:i] + c + name[i + 1:] for c in pool]
        for name in valid:
            names += [name[:-1], name + "a", name + "=", name + "\n", " " + name, name[:-1] + "=", name[1:]]
        # Random binaries of every length near a CID's, most with a wrong prefix, and the prefixes one bit off
        for length in range(30, 45):
            names += [written(rng.randbytes(length)) for _ in range(20)]
        for prefix in cid_module._PREFIXES.values():
            for bit in range(len(prefix) * 8):
                flipped = (int.from_bytes(prefix, "big") ^ (1 << bit)).to_bytes(len(prefix), "big")
                names.append(written(flipped + rng.randbytes(32)))
            names += [written(prefix + rng.randbytes(32)) for _ in range(50)]
        accepted = 0
        for name in names:
            with self.subTest(name=name):
                try:
                    expected = Cid.parse(name)
                except ValueError:
                    with self.assertRaises(ValueError):
                        Cid.from_block_name(name)
                    continue
                cid = Cid.from_block_name(name)
                accepted += 1
                self.assertEqual(cid, expected)
                self.assertEqual((cid.binary, cid.codec, str(cid), cid.key), (expected.binary, expected.codec,
                                                                              str(expected), expected.key))
                self.assertIs(str(cid), name)  # kept as the written form, not encoded again
        self.assertGreater(accepted, 100)  # both outcomes were exercised
        self.assertGreater(len(names) - accepted, 1000)

    def test_ordering(self):
        a, b = sorted([Cid.for_block(RAW, b"1"), Cid.for_block(RAW, b"2")])
        self.assertLess(a.binary, b.binary)


def base36(binary: bytes) -> str:
    """binary as "k" + lowercase base36, as IPFS's multibase base36 writes it (base-x: one "0" per leading zero
    byte), computed digit by digit from the bytes, independently of rnfs.content.cid."""
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    digits = []  # least significant first
    for byte in binary:
        carry = byte
        for i, digit in enumerate(digits):
            carry += digit * 256
            digits[i], carry = carry % 36, carry // 36
        while carry:
            digits.append(carry % 36)
            carry //= 36
    zeros = len(binary) - len(binary.lstrip(b"\0"))
    return "k" + "0" * zeros + "".join(alphabet[d] for d in reversed(digits))


class Base36Test(unittest.TestCase):
    """The gateway's host-only form: "k" + base36, exact length and canonical, never accepted by Cid.parse."""

    def test_round_trips_and_lengths(self):
        rng = random.Random(36)
        for codec, length in ((RAW, 56), (DAG, 60)):
            for block in [b"", b"a"] + [rng.randbytes(20) for _ in range(200)]:
                value = Cid.for_block(codec, block)
                text = to_base36(value)
                self.assertEqual(text, base36(value.binary))
                self.assertEqual(len(text), length)
                self.assertLessEqual(len(text), 63)  # one DNS label
                parsed = from_base36(text)
                self.assertEqual(parsed, value)
                self.assertEqual(str(parsed), str(value))  # the written form is base32, as always

    def test_every_digest_takes_the_same_length(self):
        # The smallest and largest digest of each prefix: the prefix alone fixes the number of digits
        for prefix, length in ((bytes.fromhex("01551220"), 56), (bytes.fromhex("018080c0011220"), 60)):
            for digest in (bytes(32), b"\xff" * 32):
                self.assertEqual(len(to_base36(Cid.from_bytes(prefix + digest))), length)
                self.assertEqual(from_base36(base36(prefix + digest)).binary, prefix + digest)

    def test_parse_never_takes_the_host_form(self):
        for codec in (RAW, DAG):
            text = to_base36(Cid.for_block(codec, b"x"))
            with self.assertRaises(ValueError):
                Cid.parse(text)
            self.assertFalse(cid_module.is_valid(text))
            with self.assertRaises(ValueError):
                Cid.from_block_name(text)

    def test_rejects_everything_else(self):
        raw = to_base36(Cid.for_block(RAW, b"x"))
        node = to_base36(Cid.for_block(DAG, b"x"))
        digest = bytes(32)
        bad = {
            "not a string": raw.encode(), "none": None, "empty": "", "k alone": "k",
            "base32 CID": HELLO_WORLD, "uppercase": raw.upper(), "uppercase body": "k" + raw[1:].upper(),
            "uppercase K": "K" + raw[1:], "one uppercase digit": raw[:10] + raw[10].upper() + raw[11:],
            "wrong multibase": "b" + raw[1:], "no multibase": raw[1:],
            "one short": raw[:-1], "one long": raw + "0", "dag one short": node[:-1], "dag one long": node + "0",
            "leading zero": "k0" + raw[1:-1], "leading zeros, dag": "k00" + node[1:-2],
            "underscore": raw[:20] + "_" + raw[21:], "plus sign": "k+" + raw[2:], "minus sign": "k-" + raw[2:],
            "space": raw[:-1] + " ", "leading space": " " + raw, "trailing newline": raw + "\n",
            "non-ascii digit": raw[:-1] + "\u0663", "fullwidth digit": raw[:-1] + "\uff11",
            "dot": raw[:-1] + ".", "too large": "k" + "z" * 55, "too large, dag": "k" + "z" * 59,
            "dag-pb codec": base36(bytes([1, 0x70, 0x12, 0x20]) + digest),
            "wrong hash": base36(bytes([1, 0x55, 0x13, 0x20]) + digest),
            "short digest": base36(bytes([1, 0x55, 0x12, 0x20]) + digest[:31]),
            "long digest": base36(bytes([1, 0x55, 0x12, 0x20]) + digest + b"\0"),
            "CIDv0-like": base36(bytes([0, 0x55, 0x12, 0x20]) + digest),
            "leading zero byte": base36(b"\0" + bytes([1, 0x55, 0x12, 0x20]) + digest[:31]),
        }
        for reason, value in bad.items():
            with self.subTest(reason):
                with self.assertRaises(ValueError):
                    from_base36(value)
        # Every one-digit change of a valid form: accepted only if it is canonical and another CID's form
        rng = random.Random(5)
        for text in (raw, node):
            for _ in range(300):
                i = rng.randrange(1, len(text))
                changed = text[:i] + rng.choice("0123456789abcdefghijklmnopqrstuvwxyzK_ .") + text[i + 1:]
                try:
                    value = from_base36(changed)
                except ValueError:
                    continue
                self.assertEqual(to_base36(value), changed)


if __name__ == "__main__":
    unittest.main()
