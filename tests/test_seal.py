"""Tests for sealed files (rnfs/content/seal.py) and their use by the node: add --to, get/cat decryption, --raw."""
import io
import os
import shutil
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.content import seal
from rnfs.content.seal import SEGMENT_SIZE, NotRecipient, SealError
from rnfs.errors import TooLarge
from rnfs.node import RnfsNode

from tests.test_builder import ShortReads
from tests.test_names import FakeNetwork
from tests.test_node import NetClient

ALICE, BOB, CAROL = RNS.Identity(), RNS.Identity(), RNS.Identity()


def reader(blob: bytes, log: list | None = None):
    """A RawReader over bytes in memory, yielding small pieces; records each (offset, length) asked for."""
    def raw(offset, length):
        if log is not None:
            log.append((offset, length))
        data = blob[offset:offset + length]
        for i in range(0, len(data), 5000):
            yield data[i:i + 5000]
    return raw


def open_all(blob: bytes, identity=BOB) -> tuple[bytes, int, str | None]:
    """(plaintext, size, name) of a sealed blob opened with identity; raises what opening raises."""
    sealed = seal.open_sealed(reader(blob), len(blob))
    if sealed is None:
        raise SealError("not sealed")
    sealed.unlock(identity)
    return b"".join(sealed.read()), sealed.size, sealed.name


def craft(segments: list[bytes], name: bytes | None = None, identity=BOB) -> bytes:
    """A sealed file built by hand as a malicious sealer could, each segment's plaintext given explicitly."""
    import hashlib
    from RNS.vendor import umsgpack

    key = os.urandom(32)
    public = RNS.Identity(create_keys=False)
    public.load_public_key(identity.get_public_key())
    sealed_name = None if name is None else seal._encrypt_part(*seal._keys(key, seal.NAME_INFO), name, b"")
    header = umsgpack.packb([SEGMENT_SIZE, [[bytes(identity.hash), public.encrypt(key)]], sealed_name])
    prefix = seal.MAGIC + bytes([seal.VERSION]) + len(header).to_bytes(4, "big") + header
    header_hash = hashlib.sha256(prefix).digest()
    mac_key, enc_key = seal._keys(key, seal.SEGMENT_INFO)
    return prefix + b"".join(
        seal._encrypt_part(mac_key, enc_key, plain, seal._segment_bound(header_hash, i, i == len(segments) - 1))
        for i, plain in enumerate(segments))


class FormatTest(unittest.TestCase):

    def setUp(self):
        self.keys = [ALICE.get_public_key(), BOB.get_public_key()]

    def test_round_trip_sizes(self):
        for size in (0, 1, 15, 16, SEGMENT_SIZE - 1, SEGMENT_SIZE, SEGMENT_SIZE + 1, 2 * SEGMENT_SIZE,
                     3 * SEGMENT_SIZE + 17, 2_500_000):
            data = os.urandom(size)
            blob = seal.seal_bytes(data, self.keys, "report.pdf")
            self.assertTrue(seal.is_sealed(blob))
            for identity in (ALICE, BOB):
                plain, plain_size, name = open_all(blob, identity)
                self.assertEqual(plain, data, size)
                self.assertEqual(plain_size, size)
                self.assertEqual(name, "report.pdf")

    def test_oversized_final_segment_is_rejected(self):
        """PKCS7 fits up to 65551 bytes in a full segment's size; the final one may hold SEGMENT_SIZE at most."""
        for segments in ([b"C" * (SEGMENT_SIZE + 15)], [b"A" * SEGMENT_SIZE, b"B" * (SEGMENT_SIZE + 1)]):
            blob = craft(segments)
            sealed = seal.open_sealed(reader(blob), len(blob))
            sealed.unlock(BOB)
            with self.assertRaises(SealError):
                sealed.size
            with self.assertRaises(SealError):
                b"".join(sealed.read())
        self.assertEqual(open_all(craft([b"A" * SEGMENT_SIZE, b"B" * SEGMENT_SIZE]))[1], 2 * SEGMENT_SIZE)

    def test_invalid_sealed_name_is_ignored(self):
        """An authenticated but invalid name (or one failing its tag) reads as no name; the content still opens."""
        for name in (b"..", b"a/b", b"\xff\xfe", b""):
            plain, size, got = open_all(craft([b"hello"], name))
            self.assertEqual((plain, size, got), (b"hello", 5, None), name)
        blob = bytearray(craft([b"hello"], b"fine.txt"))
        sealed = seal.open_sealed(reader(bytes(blob)), len(blob))
        sealed.unlock(BOB)
        self.assertEqual(sealed.name, "fine.txt")
        sealed._sealed_name = sealed._sealed_name[:-1] + bytes([sealed._sealed_name[-1] ^ 1])
        self.assertIsNone(sealed.name)

    def test_layout(self):
        blob = seal.seal_bytes(b"x" * (SEGMENT_SIZE + 5), self.keys)
        self.assertEqual(blob[:12], b"rnfs-sealed\n")
        self.assertEqual(blob[12], 1)
        sealed = seal.open_sealed(reader(blob), len(blob))
        self.assertEqual(sealed.segments, 2)
        self.assertEqual(len(blob) - sealed.body_start, seal.FULL_SEGMENT + 16 + 16 + 32)
        self.assertEqual(sorted(sealed.recipients), sorted(seal.identity_hash(k) for k in self.keys))
        self.assertEqual(set(sealed.recipients), {bytes(ALICE.hash), bytes(BOB.hash)})
        sealed.unlock(ALICE)
        self.assertIsNone(sealed.name)

    def test_streaming_reader(self):
        """SealingReader holds a segment at a time and gives the same format whatever the read sizes."""
        data = os.urandom(5 * SEGMENT_SIZE + 3)
        sealing = seal.SealingReader(io.BytesIO(data), self.keys, size=len(data))
        pieces = []
        while True:
            piece = sealing.read(10_000)
            if not piece:
                break
            self.assertLessEqual(len(piece), 10_000)
            pieces.append(piece)
        self.assertEqual(open_all(b"".join(pieces))[0], data)
        with self.assertRaises(ValueError):  # a sized stream that ends early
            seal.SealingReader(io.BytesIO(b"abc"), self.keys, size=10).read()

    def test_streaming_reader_reads_whole_segments(self):
        """SealingReader fills each segment through short reads, reads no more than a size allows, and says how
        many bytes a sized stream lacked."""
        data = os.urandom(2 * SEGMENT_SIZE)
        stream = ShortReads(data + b"extra", 1000)
        self.assertEqual(open_all(seal.SealingReader(stream, self.keys, size=len(data)).read())[0], data)
        self.assertEqual((stream.pos, stream.largest_request), (len(data), SEGMENT_SIZE))
        self.assertEqual(open_all(seal.SealingReader(ShortReads(data, 1000), self.keys).read())[0], data)
        with self.assertRaisesRegex(ValueError, "^stream ended 7 bytes early$"):
            seal.SealingReader(io.BytesIO(b"abc"), self.keys, size=10)
        sealing = seal.SealingReader(ShortReads(data[:SEGMENT_SIZE + 3], 1000), self.keys, size=SEGMENT_SIZE + 10)
        with self.assertRaisesRegex(ValueError, "^stream ended 7 bytes early$"):
            sealing.read()

    def test_recipients(self):
        blob = seal.seal_bytes(b"secret", self.keys)
        with self.assertRaises(NotRecipient):
            open_all(blob, CAROL)
        with self.assertRaises(ValueError):
            seal.seal_bytes(b"x", [])
        with self.assertRaises(ValueError):
            seal.seal_bytes(b"x", [b"short"])
        # Duplicates collapse into one entry
        blob = seal.seal_bytes(b"x", self.keys + self.keys)
        self.assertEqual(len(seal.open_sealed(reader(blob), len(blob)).recipients), 2)

    def test_parse_recipient(self):
        known = {bytes(BOB.hash): BOB.get_public_key()}
        recall = known.get
        self.assertEqual(seal.parse_recipient(BOB.hash.hex(), recall), BOB.get_public_key())
        self.assertEqual(seal.parse_recipient(f"<{BOB.hash.hex()}>", recall), BOB.get_public_key())
        self.assertEqual(seal.parse_recipient(CAROL.get_public_key().hex(), recall), CAROL.get_public_key())
        with self.assertRaises(ValueError):
            seal.parse_recipient(CAROL.hash.hex(), recall)  # RNS doesn't know it
        with self.assertRaises(ValueError):
            seal.parse_recipient("bob", recall)
        self.assertEqual(seal.parse_recipient("bob", recall, {"bob": bytes(BOB.hash)}.get), BOB.get_public_key())

    def test_every_byte_of_the_prefix_is_authenticated(self):
        data = os.urandom(SEGMENT_SIZE + 1000)
        blob = seal.seal_bytes(data, self.keys, "name.txt")
        sealed = seal.open_sealed(reader(blob), len(blob))
        for position in range(len(MAGIC := seal.MAGIC), sealed.body_start):
            tampered = bytearray(blob)
            tampered[position] ^= 0x01
            with self.assertRaises((SealError, NotRecipient), msg=f"byte {position}"):
                open_all(bytes(tampered))
        # A damaged magic makes it an ordinary (unsealed) file
        tampered = bytearray(blob)
        tampered[0] ^= 1
        self.assertIsNone(seal.open_sealed(reader(bytes(tampered)), len(tampered)))
        self.assertEqual(len(MAGIC), 12)

    def test_every_part_of_every_segment_is_authenticated(self):
        data = os.urandom(2 * SEGMENT_SIZE + 500)
        blob = seal.seal_bytes(data, self.keys)
        sealed = seal.open_sealed(reader(blob), len(blob))
        start = sealed.body_start
        positions = set(range(start, len(blob), 997)) | {start, start + 15, start + 16, start + seal.FULL_SEGMENT - 1,
                                                         start + seal.FULL_SEGMENT, len(blob) - 1, len(blob) - 33}
        for position in sorted(positions):
            tampered = bytearray(blob)
            tampered[position] ^= 0x80
            with self.assertRaises(SealError, msg=f"byte {position}"):
                open_all(bytes(tampered))

    def test_truncation_reordering_splicing(self):
        data = os.urandom(3 * SEGMENT_SIZE)
        blob = seal.seal_bytes(data, self.keys)
        sealed = seal.open_sealed(reader(blob), len(blob))
        start, full = sealed.body_start, seal.FULL_SEGMENT
        segments = [blob[start + i * full:start + (i + 1) * full] for i in range(3)]
        prefix = blob[:start]
        cases = {
            "last segment dropped": prefix + segments[0] + segments[1],
            "first segment dropped": prefix + segments[1] + segments[2],
            "two segments swapped": prefix + segments[1] + segments[0] + segments[2],
            "cut mid-segment": blob[:-100],
            "no segments": prefix,
            "segment repeated": prefix + segments[0] + segments[0] + segments[1] + segments[2],
        }
        other = seal.seal_bytes(os.urandom(3 * SEGMENT_SIZE), self.keys)
        other_start = seal.open_sealed(reader(other), len(other)).body_start
        cases["segment from another file"] = (prefix + segments[0] + other[other_start + full:other_start + 2 * full]
                                              + segments[2])
        cases["header from another file"] = other[:other_start] + blob[start:]
        for what, tampered in cases.items():
            with self.assertRaises(SealError, msg=what):
                open_all(tampered)

    def test_range_reads_decrypt_only_covering_segments(self):
        data = os.urandom(6 * SEGMENT_SIZE + 1234)
        blob = seal.seal_bytes(data, self.keys)
        for offset, length in ((0, 10), (SEGMENT_SIZE - 5, 10), (2 * SEGMENT_SIZE, SEGMENT_SIZE),
                               (3 * SEGMENT_SIZE + 7, None), (len(data) - 3, 100), (len(data), 5), (10**9, 5),
                               (5, 0), (0, None)):
            log: list = []
            sealed = seal.open_sealed(reader(blob, log), len(blob))
            sealed.unlock(BOB)
            log.clear()
            got = b"".join(sealed.read(offset, length))
            end = len(data) if length is None else offset + length
            self.assertEqual(got, data[offset:end], (offset, length))
            if length and offset < len(data):
                first = offset // SEGMENT_SIZE
                last = min((end - 1) // SEGMENT_SIZE, sealed.segments - 1)
                (read_offset, read_length), = log
                self.assertEqual(read_offset, sealed.body_start + first * seal.FULL_SEGMENT)
                self.assertLessEqual(read_length, (last - first + 1) * seal.FULL_SEGMENT)
        with self.assertRaises(ValueError):
            list(sealed.read(-1, 5))


class NodeSealTest(unittest.TestCase):
    """Sealing on offline nodes: A adds for B, B fetches over a NetClient and decrypts."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        self.net = NetClient()
        self.net.sender = bytes([0xB] * 16)
        self.a = self.make_node("a", bytes([0xA] * 16), ALICE)
        self.b = self.make_node("b", bytes([0xB] * 16), BOB)
        self.c = self.make_node("c", bytes([0xC] * 16), CAROL)
        for node in (self.b, self.c):
            node.routing.heard(self.a.node_id)

    def make_node(self, name, node_id, identity) -> RnfsNode:
        """An offline node with an identity (the daemon's, which opens sealed files)."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, name), slow="no", lookup_timeout=5, path_timeout=0.5)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        node.identity = identity
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def advertise(self, cid: str):
        """A provides cid."""
        self.a.publisher.publish_due()

    def test_add_get_cat(self):
        data = os.urandom(3 * SEGMENT_SIZE + 99)
        path = os.path.join(self.tmp.name, "secret.bin")
        with open(path, "wb") as f:
            f.write(data)
        result = self.a.add_path(path, to=[BOB.get_public_key().hex()], to_self=True)
        self.assertTrue(result["sealed"])
        cid = result["cid"]
        self.assertEqual(b"".join(self.a.read(cid)), data)  # to_self: the sender reads it too
        self.advertise(cid)
        info = self.b.info(cid)
        self.assertEqual((info["sealed"], info["recipient"], info["size"], info["name"]), (True, True, len(data), "secret.bin"))
        self.assertGreater(info["stored_size"], len(data))
        self.assertEqual(b"".join(self.b.read(cid, offset=SEGMENT_SIZE - 3, length=10)), data[SEGMENT_SIZE - 3:SEGMENT_SIZE + 7])
        out = os.path.join(self.tmp.name, "out")
        result = self.b.get(cid, out)
        self.assertEqual((result["sealed"], result["name"]), (True, "secret.bin"))
        with open(out, "rb") as f:
            self.assertEqual(f.read(), data)
        raw_out = os.path.join(self.tmp.name, "raw")
        self.b.get(cid, raw_out, raw=True)
        with open(raw_out, "rb") as f:
            ciphertext = f.read()
        self.assertTrue(seal.is_sealed(ciphertext))
        self.assertEqual(b"".join(self.b.read(cid, raw=True)), ciphertext)
        self.assertEqual(b"".join(self.b.read(cid, for_gateway=True)), ciphertext)  # the gateway never decrypts
        self.assertNotIn("sealed", self.b.info(cid, for_gateway=True))

    def test_read_file_of_a_sealed_file(self):
        """read_file gives a recipient the plaintext size and bytes, raw the ciphertext, and anyone else the
        stored size, then NotRecipient at the first chunk (GET /api/cat)."""
        data = os.urandom(SEGMENT_SIZE + 99)
        cid = self.a.add_stream(io.BytesIO(data), to=[BOB.get_public_key().hex()], seal_name="note")["cid"]
        self.advertise(cid)
        info, chunks = self.b.read_file(cid, offset=SEGMENT_SIZE - 3, length=10)
        self.assertEqual((info["sealed"], info["size"]), (True, len(data)))
        self.assertEqual(b"".join(chunks), data[SEGMENT_SIZE - 3:SEGMENT_SIZE + 7])
        info, chunks = self.b.read_file(cid, raw=True)
        self.assertEqual(b"".join(chunks), b"".join(self.b.read(cid, raw=True)))
        self.assertEqual(len(b"".join(self.b.read(cid, raw=True))), info["size"])
        info, chunks = self.c.read_file(cid)
        self.assertEqual((info["sealed"], info["recipient"], info["size"]), (True, False, info["stored_size"]))
        with self.assertRaises(NotRecipient):
            next(chunks)

    def test_non_recipient(self):
        cid = self.a.add_stream(io.BytesIO(b"for bob only"), to=[BOB.get_public_key().hex()], seal_name="note")["cid"]
        self.advertise(cid)
        info = self.c.info(cid)
        self.assertEqual((info["sealed"], info["recipient"]), (True, False))
        with self.assertRaises(NotRecipient):
            b"".join(self.c.read(cid))
        out = os.path.join(self.tmp.name, "out")
        with self.assertRaises(NotRecipient):
            self.c.get(cid, out)
        self.assertFalse(os.path.lexists(out))
        self.assertEqual(b"".join(self.b.read(cid)), b"for bob only")
        self.assertEqual(self.b.info(cid)["name"], "note")
        with self.assertRaises(NotRecipient):  # the sender didn't seal for itself
            b"".join(self.a.read(cid))

    def test_recipient_by_hash_and_unknown(self):
        def recall(hash_, from_identity_hash=False, _no_use=False):
            """RNS knows Bob's destination only."""
            return BOB if hash_ == bytes([0xB] * 16) and not from_identity_hash else None

        with mock.patch.object(RNS.Identity, "recall", side_effect=recall):
            cid = self.a.add_stream(io.BytesIO(b"hi"), to=[(bytes([0xB] * 16)).hex()])["cid"]
            with self.assertRaises(ValueError):
                self.a.add_stream(io.BytesIO(b"hi"), to=[(bytes([0xD] * 16)).hex()])
        self.advertise(cid)
        self.assertEqual(b"".join(self.b.read(cid)), b"hi")
        # The node's own ID names its own identity
        cid = self.a.add_stream(io.BytesIO(b"me"), to=[self.a.node_id.hex()])["cid"]
        self.assertEqual(b"".join(self.a.read(cid)), b"me")

    def test_refusals(self):
        directory = os.path.join(self.tmp.name, "dir")
        os.mkdir(directory)
        with self.assertRaisesRegex(ValueError, "single files"):
            self.a.add_path(directory, to_self=True)
        with self.assertRaisesRegex(ValueError, "only-hash"):
            self.a.add_stream(io.BytesIO(b"x"), to_self=True, only_hash=True)

    def test_wrap_shows_the_name(self):
        path = os.path.join(self.tmp.name, "visible.txt")
        with open(path, "wb") as f:
            f.write(b"content")
        root = self.a.add_path(path, wrap=True, to_self=True)["cid"]
        self.assertEqual([e["name"] for e in self.a.ls(root)], ["visible.txt"])
        self.assertEqual(b"".join(self.a.read(f"{root}/visible.txt")), b"content")
        # With seal_name off, nothing inside names it
        cid = self.a.add_path(path, to_self=True, seal_name=False)["cid"]
        self.assertIsNone(self.a.info(cid)["name"])

    def test_hidden_or_invalid_sealed_names_are_not_offered(self):
        """A sealer-chosen name that is hidden or unprintable isn't returned as the default output name, and
        an invalid one doesn't stop get (nor leave a partial output)."""
        for i, name in enumerate((".bash_login", "bad\x1b[2Jname")):
            cid = self.a.add_stream(io.BytesIO(b"payload"), to=[BOB.get_public_key().hex()], seal_name=name)["cid"]
            self.advertise(cid)
            self.assertIsNone(self.b.info(cid)["name"], name)
            out = os.path.join(self.tmp.name, f"out{i}")
            self.assertIsNone(self.b.get(cid, out)["name"])
            with open(out, "rb") as f:
                self.assertEqual(f.read(), b"payload")
        cid = self.a.add_stream(io.BytesIO(craft([b"hello"], b"..")))["cid"]
        self.advertise(cid)
        self.assertIsNone(self.b.info(cid)["name"])
        out = os.path.join(self.tmp.name, "invalid-name")
        self.assertEqual(self.b.get(cid, out)["name"], None)
        with open(out, "rb") as f:
            self.assertEqual(f.read(), b"hello")

    def test_sealed_get_reports_its_total_to_the_observer(self):
        """A job decrypting a sealed file knows its total (the stored size), not just the bytes done."""
        from rnfs import fetcher

        cid = self.a.add_stream(io.BytesIO(os.urandom(2 * SEGMENT_SIZE)), to=[BOB.get_public_key().hex()])["cid"]
        self.advertise(cid)
        totals = []

        class Watch(fetcher.Observer):
            def total(self, size):
                """Record the total."""
                totals.append(size)

        with fetcher.observing(Watch()):
            self.b.get(cid, os.path.join(self.tmp.name, "watched"))
        self.assertEqual(totals, [self.b.info(cid)["stored_size"]])

    def test_tampered_blocks_are_reported(self):
        data = os.urandom(2 * SEGMENT_SIZE)
        cid = self.a.add_stream(io.BytesIO(seal.seal_bytes(data, [ALICE.get_public_key()])))["cid"]
        self.assertEqual(b"".join(self.a.read(cid)), data)
        tampered = bytearray(seal.seal_bytes(data, [ALICE.get_public_key()]))
        tampered[-40] ^= 1
        bad = self.a.add_stream(io.BytesIO(bytes(tampered)))["cid"]
        with self.assertRaises(SealError):
            b"".join(self.a.read(bad))
        out = os.path.join(self.tmp.name, "bad")
        with self.assertRaises(SealError):
            self.a.get(bad, out)
        self.assertFalse(os.path.lexists(out))  # the partial output is removed

    def test_get_refuses_what_wont_fit(self):
        cid = self.a.add_stream(io.BytesIO(os.urandom(3 * SEGMENT_SIZE)), to_self=True)["cid"]
        out = os.path.join(self.tmp.name, "out")
        real = shutil.disk_usage

        def full(path):
            """Little free space where the output goes."""
            usage = real(path)
            return usage._replace(free=SEGMENT_SIZE) if os.path.abspath(path) == self.tmp.name else usage

        with mock.patch("shutil.disk_usage", full):
            with self.assertRaises(TooLarge):
                self.a.get(cid, out)
        self.assertFalse(os.path.lexists(out))


if __name__ == "__main__":
    unittest.main()
