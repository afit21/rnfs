"""N1-R1: Fetch.read_range had no byte bound beyond max_download_size, so a small read of a file whose nodes are
padded with empty (size 0) children downloaded whole padded nodes all the way down: `cat` of a file declaring 1 byte
took 7.3 MB, and `get` of a file sealed to the node (decrypted through read_range, never fetch_dag) 29 MB for 12 KB.

A serves files whose path to each chunk runs through padded file nodes; B reads them every way that goes through
read_range: cat (node.read), a gateway read, info's sealed-header read and get of a file sealed to B. Each must be
refused with TooLarge having downloaded at most the root block, the read's bound (dag_bound of the range plus two
full file nodes per level an honest file of the declared size can have; _fetchr2_helpers.range_allowance) and
about one block more. Honest files (both chunkers, a file of two node levels, sealed files) are read at many
offsets and lengths, downloading the same blocks as with no bound (in at most one more request) and using under
half of it.
"""
import io
import os
import random
import unittest
from unittest import mock

import RNS

from rnfs import fetcher
from rnfs.constants import CHUNK_SIZE, MAX_BLOCK_SIZE, MAX_CHILDREN
from rnfs.content import dag, seal
from rnfs.content.cid import Cid
from rnfs.errors import TooLarge
from tests.security._content_helpers import TwoNodeTest
from tests.security._fetchr2_helpers import FULL_FILE_NODE, Blocks, file_levels, range_allowance
from tests.test_node import A_ID

LEVELS = 20  # padded levels below each root: about 2.4 MB of nodes per chain


class RangeBoundFormulaTest(unittest.TestCase):
    """The bound read_range uses is the one derived in _fetchr2_helpers from the constants."""

    def test_formula(self):
        range_bound = getattr(fetcher, "range_bound", None)
        self.assertIsNotNone(range_bound, "rnfs.fetcher has no range_bound")
        for size, length in [(1, 1), (CHUNK_SIZE, 10), (MAX_CHILDREN * CHUNK_SIZE + 1, 2), (dag.MAX_SIZE, 1),
                             (dag.MAX_SIZE, 10 ** 9)]:
            self.assertEqual(range_bound(size, length), range_allowance(size, length))
        self.assertEqual(fetcher.FILE_NODE_MAX, FULL_FILE_NODE)

    def test_levels(self):
        # 4 KiB granules: a file of up to 1 MiB has at most 256 chunks, so one node level; any size, at most 7
        self.assertEqual(file_levels(1), 1)
        self.assertEqual(file_levels(MAX_CHILDREN * 4096 - 1), 1)
        self.assertEqual(file_levels(MAX_CHILDREN * 4096), 2)
        self.assertEqual(file_levels(dag.MAX_SIZE), 7)


class PaddedChainTest(TwoNodeTest):
    """Reads of files whose chunks sit below chains of padded file nodes, refused within the bound."""

    def serve_chained(self, pieces: list[bytes], filler_chunks: int = 0) -> tuple[Cid, int]:
        """A file of pieces, each below LEVELS padded nodes, under a small root; (root, the root's block size).
        filler_chunks adds, after them, an unpadded node of that many 4 KiB chunks, to declare a larger file."""
        blocks = Blocks()
        children = [dag.Child(blocks.chain(blocks.raw(piece), len(piece), LEVELS), len(piece)) for piece in pieces]
        if filler_chunks:
            filler = blocks.raw(b"y" * 4096)
            node = blocks.node(dag.FileNode([dag.Child(filler, 4096)] * filler_chunks))
            children.append(dag.Child(node, 4096 * filler_chunks))
        root = blocks.node(dag.FileNode(children))
        self.serve(blocks.blocks, root)
        return root, len(blocks.blocks[root])

    def assert_refused_within(self, read, root_bytes: int, size: int, length: int):
        """read() raises TooLarge after B downloaded at most the root, the bound for the read and a block."""
        self.net.reset()
        self.b.gc()
        with self.assertRaises(TooLarge, msg="the read finished: nothing bounds it") as caught:
            read()
        self.assertIn("refusing it", str(caught.exception))
        allowed = root_bytes + range_allowance(size, length) + MAX_BLOCK_SIZE
        self.assertLessEqual(self.net.downloaded_bytes, allowed,
                             f"{self.net.downloaded_bytes} bytes downloaded in {self.net.downloaded_blocks} blocks")

    def test_cat_of_a_1_byte_file(self):
        root, root_bytes = self.serve_chained([b"x"])
        self.assertEqual(self.b.info(str(root))["size"], 1)
        self.assert_refused_within(lambda: b"".join(self.b.read(str(root))), root_bytes, 1, 1)

    def test_gateway_and_offset_reads(self):
        pieces = [bytes([i]) * 4096 for i in range(4)]
        root, root_bytes = self.serve_chained(pieces)
        size = 4 * 4096
        for offset, length in [(0, None), (5000, 1), (4095, 2), (3 * 4096, 4096)]:
            wanted = size - offset if length is None else length
            def read(o=offset, n=length):
                """A gateway read of the range."""
                return b"".join(self.b.read(str(root), o, n, for_gateway=True))
            self.assert_refused_within(read, root_bytes, size, wanted)

    def test_sealed_info_and_get(self):
        identity = RNS.Identity()
        self.b.identity = identity
        sealed = seal.seal_bytes(b"secret" * 2000, [identity.get_public_key()], "f.txt")
        pieces = [sealed[i:i + 4096] for i in range(0, len(sealed), 4096)]
        root, root_bytes = self.serve_chained(pieces)
        # Its header is read first, through the first piece's chain: refused within that small read's bound
        self.assert_refused_within(lambda: self.b.info(str(root)), root_bytes, len(sealed), len(seal.MAGIC))
        out = os.path.join(self.tmp.name, "sealed-out")
        self.assert_refused_within(lambda: self.b.get(str(root), out), root_bytes, len(sealed), len(seal.MAGIC))
        self.assertFalse(os.path.lexists(out))

    def test_a_large_declared_size_buys_a_few_levels_only(self):
        # A root declaring 2.4 MB: an honest file that size has two node levels, so the bound for reading its
        # first byte allows four full file nodes, still far below the chain's 2.4 MB
        root, root_bytes = self.serve_chained([b"x"], filler_chunks=600)
        size = self.b.info(str(root), raw=True)["size"]  # without raw, info reads the start to look for a seal
        self.assert_refused_within(lambda: b"".join(self.b.read(str(root), 0, 1)), root_bytes, size, 1)


class HonestRangeTest(TwoNodeTest):
    """Honest files read at many offsets: the same blocks as without the bound, well inside it."""

    def setUp(self):
        """Count /blocks requests too."""
        super().setUp()
        self.requests = 0
        real = self.net.blocks

        def counting(*args, **kwargs):
            """NetClient.blocks, counted."""
            self.requests += 1
            return real(*args, **kwargs)

        self.net.blocks = counting
        self.random = random.Random(2)

    def add(self, data: bytes, chunker: str | None = None) -> Cid:
        """data added on A and advertised."""
        cid = Cid.parse(self.a.add_stream(io.BytesIO(data), chunker=chunker)["cid"])
        self.a.records.store(cid.key, A_ID)
        return cid

    def read(self, node, cid: Cid, offset: int, length: int | None) -> tuple[bytes, int, int, int]:
        """(bytes read, file node bytes downloaded, blocks downloaded, requests) for one read by node, from an
        empty store."""
        node.gc()
        self.net.reset()
        self.requests = 0
        with node.fetcher.open(cid) as fetch:
            data = b"".join(fetch.read_range(fetch.resolve(), offset, length))
            node_bytes = fetch.node_bytes
        return data, node_bytes, self.net.downloaded_blocks, self.requests

    def assert_reads(self, cid: Cid, data: bytes, ranges: list[tuple[int, int | None]]):
        """Each range reads right, within half its bound, downloading what an unbounded fetch downloads."""
        unbounded = self.make_node("c", bytes([0xC] * 16))
        unbounded.routing.heard(A_ID)
        for offset, length in ranges:
            end = len(data) if length is None else min(len(data), offset + length)
            got, node_bytes, blocks, requests = self.read(self.b, cid, offset, length)
            self.assertEqual(got, data[offset:end])
            allowed = range_allowance(len(data), end - offset)
            self.assertLessEqual(node_bytes, allowed / 2, f"range {offset}+{length}: {node_bytes} of {allowed}")
            with mock.patch.object(fetcher, "range_bound", lambda size, length: 1 << 62, create=True):
                _, free_bytes, free_blocks, free_requests = self.read(unbounded, cid, offset, length)
            self.assertEqual((node_bytes, blocks), (free_bytes, free_blocks))
            # A small range ending in two file nodes of a level asks for them one at a time: the bound leaves
            # room for one MAX_BLOCK_SIZE node, not two
            self.assertLessEqual(requests, free_requests + 1, f"range {offset}+{length}")

    def test_file_of_two_node_levels(self):
        data = self.random.randbytes((MAX_CHILDREN + 3) * CHUNK_SIZE + 7)
        boundary = MAX_CHILDREN * CHUNK_SIZE  # where the root's first child ends
        self.assert_reads(self.add(data), data, [
            (0, 1), (boundary - 1, 2), (boundary - 3, CHUNK_SIZE + 6), (CHUNK_SIZE - 1, 2), (len(data) - 1, None),
            (len(data) - 10, 10), (12345, 3 * CHUNK_SIZE), (boundary // 2, None), (0, None)])

    def test_fastcdc_file(self):
        data = self.random.randbytes(3 * 1024 * 1024 + 7)
        self.assert_reads(self.add(data, chunker="fastcdc"), data, [
            (0, 1), (17_000, 100_000), (1_000_000, 1), (len(data) - 1, 1), (0, None), (2 * 1024 * 1024, None)])

    def test_media_seeks_through_the_gateway(self):
        data = self.random.randbytes(40 * CHUNK_SIZE + 1)
        cid = self.add(data)
        for offset, length in [(0, 2), (CHUNK_SIZE * 7 + 3, 1 << 20), (len(data) - 2, None), (CHUNK_SIZE, 0)]:
            self.b.gc()
            got = b"".join(self.b.read(str(cid), offset, length, for_gateway=True))
            self.assertEqual(got, data[offset:None if length is None else offset + length])

    def test_sealed_files(self):
        identity = RNS.Identity()
        self.b.identity = identity
        for size, chunker in [(5, None), (3 * CHUNK_SIZE + 11, None), (2 * 1024 * 1024 + 3, "fastcdc")]:
            plain = self.random.randbytes(size)
            sealed = seal.seal_bytes(plain, [identity.get_public_key()], "f.bin")
            cid = self.add(sealed, chunker)
            self.b.gc()
            info = self.b.info(str(cid))
            self.assertEqual((info["sealed"], info["size"]), (True, size))
            out = os.path.join(self.tmp.name, f"sealed-{size}")
            self.assertTrue(self.b.get(str(cid), out)["sealed"])
            with open(out, "rb") as f:
                self.assertEqual(f.read(), plain)
            self.b.gc()
            for offset, length in [(0, 1), (size // 2, 70_000), (max(0, size - 3), None)]:
                got = b"".join(self.b.read(str(cid), offset, length))
                self.assertEqual(got, plain[offset:None if length is None else offset + length])


if __name__ == "__main__":
    unittest.main()
