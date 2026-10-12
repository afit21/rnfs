"""Tests for content-defined chunking (rnfs/content/chunker.py) and add --chunker on the builder and node."""
import hashlib
import io
import os
import random
import tempfile
import time
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE, MAX_BLOCK_SIZE
from rnfs.content import builder, chunker, dag, walker
from rnfs.content.cid import Cid
from rnfs.node import RnfsNode

from tests.test_names import FakeNetwork

# Fixed by the specification text: every implementation must produce these
VECTOR_LENGTHS = [68012, 67020, 76641, 76118, 65772, 99649, 18749, 68089, 81228, 74064, 66952, 86699, 75467,
                  67594, 56522]
VECTOR_CID = "bagaibqabciqmb3zqfyjuzmxwezantnxahoiwklo2i7b7lamhpv4psa3lhbzg6da"
FIXED_CID = "bagaibqabciqalfywj6ff5m6wfytxzirtijmhx6licv3utbiq2d5wzyzzcide3kq"


def pseudo(n: int, seed: bytes = b"rnfs-chunker-test") -> bytes:
    """n deterministic pseudo-random bytes: SHA-256 of the seed and a 4-byte counter, concatenated."""
    return b"".join(hashlib.sha256(seed + i.to_bytes(4, "big")).digest() for i in range(n // 32 + 1))[:n]


def simple_cuts(data: bytes) -> list[int]:
    """Chunk lengths by the byte-by-byte reference definition."""
    lengths, start = [], 0
    while start < len(data):
        end = chunker.cut_point_simple(data, start, len(data))
        lengths.append(end - start)
        start = end
    return lengths


class ChunkerTest(unittest.TestCase):

    def test_parameters(self):
        self.assertEqual((chunker.MIN_SIZE, chunker.AVG_SIZE, chunker.MAX_SIZE), (16384, 65536, 131072))
        self.assertEqual(chunker.MAX_SIZE, MAX_BLOCK_SIZE)
        self.assertEqual(chunker.GEAR[0], 0x61304b5b3811834c)
        self.assertEqual(chunker.GEAR[255], 0x7989ca9814accbdb)
        self.assertEqual(chunker.GEAR[7], int.from_bytes(hashlib.sha256(b"rnfs-fastcdc-gear\x07").digest()[:8], "big"))
        self.assertEqual(chunker.MASK_S, 0xFFFFC00000000000)
        self.assertEqual(chunker.MASK_L, 0xFFFC000000000000)

    def test_determinism_vector(self):
        data = pseudo(1 << 20)
        self.assertEqual(chunker.fastcdc_cuts(data), VECTOR_LENGTHS)
        self.assertEqual(simple_cuts(data), VECTOR_LENGTHS)  # the fast version is the definition
        with chunker.using(chunker.FASTCDC):
            self.assertEqual(str(builder.build_bytes(data, builder.NO_PUT).cid), VECTOR_CID)
        self.assertEqual(str(builder.build_bytes(data, builder.NO_PUT).cid), FIXED_CID)  # default unchanged

    def test_fast_matches_reference(self):
        rng = random.Random(7)
        cases = [os.urandom(700_000), bytes(400_000), b"\xff" * 300_000, bytes(range(256)) * 1500,
                 bytes(rng.getrandbits(8) & 0x0F for _ in range(300_000)), os.urandom(16385), os.urandom(16384),
                 os.urandom(65537), os.urandom(131073), b""]
        for data in cases:
            self.assertEqual(chunker.fastcdc_cuts(data), simple_cuts(data), len(data))

    def test_bounds(self):
        data = os.urandom(8 << 20)
        lengths = chunker.fastcdc_cuts(data)
        self.assertEqual(sum(lengths), len(data))
        for length in lengths[:-1]:
            self.assertGreaterEqual(length, chunker.MIN_SIZE)
            self.assertLessEqual(length, chunker.MAX_SIZE)
        self.assertLessEqual(lengths[-1], chunker.MAX_SIZE)
        average = len(data) / len(lengths)
        self.assertTrue(48 * 1024 < average < 96 * 1024, average)  # normalized chunking stays near 64 KiB
        # No cut point at all: every chunk is MAX_SIZE
        self.assertEqual(set(chunker.fastcdc_cuts(bytes(1 << 20))), {chunker.MAX_SIZE})
        self.assertEqual(chunker.fastcdc_cuts(b"x" * 100), [100])

    def test_boundaries_survive_an_insertion(self):
        data = pseudo(4 << 20, b"insertion")
        edited = data[:2_000_000] + b"inserted bytes" * 7 + data[2_000_000:]

        def chunk_ids(blob):
            """Digests of a blob's chunks."""
            return [hashlib.sha256(chunk).digest() for chunk in chunker.fastcdc_stream(io.BytesIO(blob))]

        before, after = chunk_ids(data), chunk_ids(edited)
        unchanged = len(set(before) & set(after))
        self.assertGreaterEqual(unchanged, len(before) - 3)  # only the chunks around the edit change
        # Fixed chunking loses every chunk after the edit
        fixed_before = {hashlib.sha256(c).digest() for c in chunker.fixed_chunks(io.BytesIO(data))}
        fixed_after = {hashlib.sha256(c).digest() for c in chunker.fixed_chunks(io.BytesIO(edited))}
        self.assertLess(len(fixed_before & fixed_after), len(fixed_before) * 0.6)

    def test_stream_matches_memory_and_reads_in_pieces(self):
        data = os.urandom(3 * chunker.READ_SIZE + 12345)

        class Trickle(io.BytesIO):
            """Returns short reads, as pipes do."""

            def read(self, size=-1):
                return super().read(min(size, 1000) if size and size > 0 else size)

        expected = chunker.fastcdc_cuts(data)
        self.assertEqual([len(c) for c in chunker.fastcdc_stream(io.BytesIO(data))], expected)
        self.assertEqual([len(c) for c in chunker.fastcdc_stream(Trickle(data))], expected)
        self.assertEqual(b"".join(chunker.fastcdc_stream(io.BytesIO(data), len(data))), data)

    def test_sized_streams(self):
        for name in chunker.CHUNKERS:
            with chunker.using(name):
                self.assertEqual(b"".join(chunker.chunks(io.BytesIO(b"abcdef"), 4)), b"abcd")
                with self.assertRaises(ValueError):
                    list(chunker.chunks(io.BytesIO(b"abc"), 4))
                self.assertEqual(list(chunker.chunks(io.BytesIO(b""))), [])

    def test_using(self):
        self.assertEqual(chunker.active(), chunker.FIXED)
        with chunker.using("fastcdc"):
            self.assertEqual(chunker.active(), chunker.FASTCDC)
        self.assertEqual(chunker.active(), chunker.FIXED)
        with self.assertRaises(ValueError):
            with chunker.using("rabin"):
                pass

    def test_readers_accept_variable_chunks(self):
        """The walker's reader checks take any chunk size up to MAX_BLOCK_SIZE: a FastCDC DAG reads back."""
        data = os.urandom(70 * CHUNK_SIZE + 99)  # > MAX_CHILDREN chunks would need 256 · ~70 KiB; use a smaller one
        blocks: dict[Cid, bytes] = {}
        with chunker.using(chunker.FASTCDC):
            built = builder.build_bytes(data, lambda cid, block: blocks.setdefault(cid, block))

        def get(expect: walker.Expect) -> bytes:
            """A walker get over the built blocks."""
            return blocks[expect.cid]

        root = walker.root_expect(built.cid)
        self.assertEqual(b"".join(walker.read_range(root, get)), data)
        self.assertEqual(b"".join(walker.read_range(root, get, 100_000, 300_000)), data[100_000:400_000])
        sizes = {len(block) for cid, block in blocks.items() if cid.is_raw}
        self.assertGreater(len(sizes), 3)  # chunks of many sizes
        for _ in walker.iter_dag(root, get):
            pass  # every reader check passes

    def test_tree_rule_over_many_chunks(self):
        """More than MAX_CHILDREN chunks are grouped by the tree rule, as for fixed chunks."""
        data = bytes(300 * chunker.MAX_SIZE)  # zeros: every chunk is MAX_SIZE, so 300 chunks
        blocks: dict[Cid, bytes] = {}
        with chunker.using(chunker.FASTCDC):
            built = builder.build_bytes(data, lambda cid, block: blocks.setdefault(cid, block))
        root = dag.decode(blocks[built.cid])
        self.assertEqual(len(root.children), 2)
        self.assertEqual([c.size for c in root.children], [256 * chunker.MAX_SIZE, 44 * chunker.MAX_SIZE])

    def test_throughput(self):
        """Report MB/s (not a pass/fail threshold beyond a sanity floor)."""
        data = os.urandom(16 << 20)
        start = time.perf_counter()
        total = sum(len(c) for c in chunker.fastcdc_stream(io.BytesIO(data)))
        elapsed = time.perf_counter() - start
        self.assertEqual(total, len(data))
        rate = len(data) / elapsed / 1e6
        if os.environ.get("RNFS_BENCH"):
            print(f"\nFastCDC chunking: {rate:.1f} MB/s")
        self.assertGreater(rate, 2)


class NodeChunkerTest(unittest.TestCase):
    """add --chunker on an offline node."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", self.saved)
        config = Config(rnfs_dir=os.path.join(self.tmp.name, "n"), slow="no")
        self.node = RnfsNode.offline(config, bytes([3] * 16), None, FakeNetwork())
        self.addCleanup(self.node.stop)

    def test_add_with_fastcdc(self):
        data = pseudo(1 << 20)
        result = self.node.add_stream(io.BytesIO(data), chunker="fastcdc")
        self.assertEqual(result["cid"], VECTOR_CID)
        self.assertEqual(b"".join(self.node.read(result["cid"])), data)
        path = os.path.join(self.tmp.name, "file")
        with open(path, "wb") as f:
            f.write(data)
        self.assertEqual(self.node.add_path(path, chunker="fastcdc")["cid"], VECTOR_CID)
        self.assertEqual(self.node.add_path(path)["cid"], FIXED_CID)
        self.assertEqual(self.node.add_path(path, only_hash=True, chunker="fastcdc")["cid"], VECTOR_CID)
        with self.assertRaises(ValueError):
            self.node.add_path(path, chunker="nope")


if __name__ == "__main__":
    unittest.main()
