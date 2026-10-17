"""CONT-1: while the deny list held a `sha256:` entry, a file was hashed whole before any of it was sent or
imported, and the hash read every occurrence of repeated content. Six blocks (about 128 KB: one 64 KiB chunk and
five file nodes, each listing the node below it 256 times) declare 2^56 bytes, so a 1-byte `cat`, a gateway
`Range: bytes=0-0` request, `?format=archive` or `import` of the 128 KB archive hashed for about a year, holding a
gateway slot all along.

Now a file whose declared size is more than DENY_HASH_RATIO times the bytes of its distinct chunks, plus
DENY_HASH_SLACK, is refused as Denied (451) before any chunk is read: B holds the PoC whole (as an attacker can make
it, by having B fetch every block with ?format=raw), and its storage counts every raw block read, failing the read
past a limit so the old behaviour fails at once rather than hanging. A repetitive file inside the slack is still
served, an honest denied file still gets 451, and with no hash entries nothing is walked.
"""
import hashlib
import io
import os
import time
import unittest
from unittest import mock

from rnfs import denylist
from rnfs.constants import CHUNK_SIZE, MAX_CHILDREN
from rnfs.content import archive, dag, walker
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.storage import USER
from rnfs.errors import Denied, StorageFull
from rnfs.gateway import GatewayServer
from tests.security._content_helpers import TwoNodeTest
from tests.test_api import raw_request
from tests.test_node import A_ID

DENIED_ELSEWHERE = "sha256:" + hashlib.sha256(b"some other file").hexdigest()  # makes deny.hashes non-empty
QUICK = 10.0  # seconds a refusal may take, with room for a loaded machine


class RawRead(Exception):
    """A raw block was read past the test's limit: the content is being hashed."""


def repeated(chunk: bytes, levels: int, width: int = MAX_CHILDREN) -> tuple[dict[Cid, bytes], Cid, int]:
    """A file of chunk repeated: levels file nodes, each listing the one below it width times.

    Returns (its blocks, its root, the size the root declares)."""
    below = Cid.for_block(RAW, chunk)
    blocks = {below: chunk}
    size = len(chunk)
    for _ in range(levels):
        block = dag.encode(dag.FileNode([dag.Child(below, size)] * width))
        below = Cid.for_block(DAG, block)
        blocks[below] = block
        size *= width
    return blocks, below, size


def joined(blocks: dict[Cid, bytes], parts: list[tuple[Cid, int]]) -> tuple[Cid, int]:
    """A file node over parts (CID, size), added to blocks; (its CID, its size)."""
    block = dag.encode(dag.FileNode([dag.Child(cid, size) for cid, size in parts]))
    cid = Cid.for_block(DAG, block)
    blocks[cid] = block
    return cid, sum(size for _, size in parts)


class DenyHashAmplificationTest(TwoNodeTest):
    """The 6-block PoC and its neighbours, read by B every way that checks file hashes."""

    def setUp(self):
        super().setUp()
        self.chunk = os.urandom(CHUNK_SIZE)
        self.blocks, self.root, self.size = repeated(self.chunk, 5)
        self.assertEqual(self.size, 2 ** 56)
        self.assertEqual(len(self.blocks), 6)
        self.raw_reads = 0
        self.raw_limit = 0  # raw block reads allowed before RawRead
        get = self.b.storage.get

        def counting_get(cid, verify=False):
            """Storage.get, counting raw blocks read."""
            if cid.is_raw:
                self.raw_reads += 1
                if self.raw_reads > self.raw_limit:
                    raise RawRead(f"raw block {cid} read {self.raw_reads} times")
            return get(cid, verify)

        patcher = mock.patch.object(self.b.storage, "get", counting_get)
        patcher.start()
        self.addCleanup(patcher.stop)

    def deny(self, *entries):
        """Add entries to B's local list, its warnings silenced."""
        with mock.patch("rnfs.node.log"):
            self.b.deny.add(list(entries))

    def store_on_b(self, blocks: dict[Cid, bytes], root: Cid):
        """B holds root whole, pinned."""
        with self.b.storage.protect() as session:
            for cid, block in blocks.items():
                session.put(cid, block)
        self.b.storage.pin(root, USER)
        self.assertTrue(self.b.storage.is_complete(root))

    def gateway(self) -> int:
        """B's gateway's port."""
        server = GatewayServer(self.b, self.b.config, 0)
        server.start()
        self.addCleanup(server.stop)
        return server.port

    def assert_quick_and_unread(self, started: float):
        """Refused within QUICK, without reading a raw block."""
        self.assertLess(time.monotonic() - started, QUICK)
        self.assertEqual(self.raw_reads, 0)

    def test_constants(self):
        self.assertEqual(denylist.DENY_HASH_RATIO, 16)
        self.assertEqual(denylist.DENY_HASH_SLACK, 64 * 1024 * 1024)

    def test_cat(self):
        """cat of one byte of the stored PoC is refused before any chunk is read."""
        self.store_on_b(self.blocks, self.root)
        self.deny(DENIED_ELSEWHERE)
        started = time.monotonic()
        with self.assertRaisesRegex(Denied, "repeats its content more than 16"):
            next(self.b.read(str(self.root), offset=0, length=1))
        self.assert_quick_and_unread(started)
        # /api/cat: its info reads the first chunk (is it sealed?), and nothing more
        self.raw_limit = 1
        info, chunks = self.b.read_file(str(self.root), length=1)
        self.assertEqual(info["size"], 2 ** 56)
        with self.assertRaises(Denied):
            next(chunks)
        self.assertLess(time.monotonic() - started, QUICK)

    def test_gateway_range(self):
        """A gateway Range: bytes=0-0 request (and the type sniffing it implies) answers 451 at once."""
        self.store_on_b(self.blocks, self.root)
        self.deny(DENIED_ELSEWHERE)
        port = self.gateway()
        started = time.monotonic()
        status, _, body = raw_request(port, "GET", f"/rnfs/{self.root}", None, {"Range": "bytes=0-0"})
        self.assertEqual(status, 451)
        self.assertEqual(body, b"unavailable: this node's deny list blocks this content\n")
        self.assert_quick_and_unread(started)

    def test_gateway_archive(self):
        """?format=archive answers 451 before anything is sent, and export_stream never starts."""
        self.store_on_b(self.blocks, self.root)
        self.deny(DENIED_ELSEWHERE)
        port = self.gateway()
        started = time.monotonic()
        status, _, _ = raw_request(port, "GET", f"/rnfs/{self.root}?format=archive")
        self.assertEqual(status, 451)
        begun = []
        with self.assertRaises(Denied):
            self.b.export_stream(str(self.root), begun.append, for_gateway=True)
        self.assertEqual(begun, [])
        self.assert_quick_and_unread(started)

    def test_import(self):
        """Importing the 128 KB archive is refused as Denied, pins nothing and leaves none of its blocks."""
        path = os.path.join(self.tmp.name, "poc.rnfs")
        with open(path, "wb") as out:
            archive.export_archive([self.root], lambda expect: self.blocks[expect.cid], out)
        self.assertLess(os.path.getsize(path), 200 * 1024)
        self.deny(DENIED_ELSEWHERE)
        before = self.b.storage.block_count()
        started = time.monotonic()
        with self.assertRaisesRegex(Denied, "repeats its content"):
            self.b.import_archive(path)
        self.assert_quick_and_unread(started)
        self.assertEqual(self.b.storage.pins(), {})
        self.assertEqual(self.b.storage.block_count(), before)

    def test_not_stored_is_refused_before_downloading_content(self):
        """Fetched from A, the PoC is refused (its declared size can't fit on the disk, or by the ratio) without B
        reading a chunk."""
        self.serve(self.blocks, self.root)
        self.deny(DENIED_ELSEWHERE)
        started = time.monotonic()
        with self.assertRaises((Denied, StorageFull)):
            next(self.b.read(str(self.root), offset=0, length=1))
        self.assert_quick_and_unread(started)

    def test_over_the_ratio_is_refused(self):
        """A repeated-chunk file just past 16 × its chunk + 64 MiB (80 MiB of one chunk) is refused unread; one
        inside it is served."""
        blocks, sixteen, sixteen_size = repeated(self.chunk, 1)  # 16 MiB
        over, over_size = joined(blocks, [(sixteen, sixteen_size)] * 5)
        self.assertGreater(over_size, denylist.DENY_HASH_RATIO * CHUNK_SIZE + denylist.DENY_HASH_SLACK)
        self.store_on_b(blocks, over)
        self.deny(DENIED_ELSEWHERE)
        with self.assertRaises(Denied):
            next(self.b.read(str(over), offset=0, length=1))
        self.assertEqual(self.raw_reads, 0)

    def test_repetitive_file_inside_the_slack_is_served(self):
        """A 32 MiB file of one repeated chunk is hashed (once: then remembered) and served, by cat and the
        gateway."""
        blocks, sixteen, sixteen_size = repeated(self.chunk, 1)
        root, size = joined(blocks, [(sixteen, sixteen_size)] * 2)
        self.assertEqual(size, 32 * 1024 * 1024)
        self.store_on_b(blocks, root)
        self.deny(DENIED_ELSEWHERE)
        self.raw_limit = 10 ** 6
        self.assertEqual(b"".join(self.b.read(str(root), offset=5, length=10)), self.chunk[5:15])
        self.assertEqual(self.b.deny.known_hash(root), hashlib.sha256(self.chunk * 512).digest())
        status, _, body = raw_request(self.gateway(), "GET", f"/rnfs/{root}", None, {"Range": "bytes=0-0"})
        self.assertEqual((status, body), (206, self.chunk[:1]))

    def test_honest_denied_file(self):
        """A multi-chunk file whose hash is denied still gets Denied and 451, stored or fetched."""
        data = os.urandom(3 * CHUNK_SIZE + 7)
        cid = Cid.parse(self.a.add_stream(io.BytesIO(data))["cid"])
        self.a.records.store(cid.key, A_ID)
        self.deny("sha256:" + hashlib.sha256(data).hexdigest())
        self.raw_limit = 10 ** 6
        with self.assertRaises(Denied):
            next(self.b.read(str(cid), offset=CHUNK_SIZE, length=1))  # fetched from A, then denied
        status, _, _ = raw_request(self.gateway(), "GET", f"/rnfs/{cid}", None, {"Range": "bytes=0-0"})
        self.assertEqual(status, 451)

    def test_no_hash_entries_nothing_walked(self):
        """With no sha256: entries, nothing is walked or hashed: a 1-byte read of the PoC reads one chunk."""
        self.store_on_b(self.blocks, self.root)
        self.deny(str(Cid.for_block(RAW, b"denied by CID")))
        self.raw_limit = 8  # the read, and the gateway's sniff and range: a chunk each
        with mock.patch.object(walker, "leaf_bytes", side_effect=AssertionError("walked")) as walk:
            self.assertEqual(b"".join(self.b.read(str(self.root), offset=0, length=1)), self.chunk[:1])
            status, _, body = raw_request(self.gateway(), "GET", f"/rnfs/{self.root}", None,
                                          {"Range": "bytes=0-0"})
        self.assertEqual((status, body), (206, self.chunk[:1]))
        walk.assert_not_called()


if __name__ == "__main__":
    unittest.main()
