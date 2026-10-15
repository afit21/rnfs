"""DAG byte bound (fix for L3, N1 and L5): fetch_dag bounds the bytes of a DAG's distinct blocks by what its declared
size can hold (rnfs.fetcher.dag_bound) plus what its directory and index nodes bring (rnfs.fetcher.node_credit: their
own bytes and DAG_ENTRY_OVERHEAD per entry or index child, less INDEX_NODE_COST for an index node).

The tests check the bound's formula; that everything the builder writes (a file of hundreds of chunks over two node
levels, FastCDC chunks, deep and wide directories with the longest names, empty files and directories) is fetched
for real by get, pin add, dir set, dir rm and export without coming near it, and without smaller requests; and that
DAGs made to break it are refused with TooLarge, every way a user can fetch one, after downloading at most about one
block more than it allows.
"""
import io
import os
import random
import unittest
from unittest import mock

from rnfs import fetcher
from rnfs.constants import CHUNK_SIZE, MAX_BLOCK_SIZE, MAX_CHILDREN
from rnfs.content import builder, dag, walker
from rnfs.content.cid import DAG, EMPTY_FILE, RAW, Cid
from rnfs.content.storage import USER
from rnfs.errors import TooLarge
from rnfs.fetcher import (DAG_ENTRY_OVERHEAD, DAG_GRANULE, DAG_GRANULE_OVERHEAD, DAG_SLACK, INDEX_NODE_COST, dag_bound,
                          node_credit)
from tests.security._content_helpers import TwoNodeTest
from tests.test_node import A_ID

LONG = 255  # the longest entry name, in bytes


def name(i: int, length: int = LONG) -> str:
    """A distinct entry name of length bytes."""
    return f"{i:05d}".ljust(length, "n")


class DagBoundTest(unittest.TestCase):
    """The formula."""

    def test_formula(self):
        self.assertEqual(dag_bound(0), DAG_SLACK)
        self.assertEqual(dag_bound(1), 1 + DAG_GRANULE_OVERHEAD + DAG_SLACK)
        self.assertEqual(dag_bound(DAG_GRANULE), DAG_GRANULE + DAG_GRANULE_OVERHEAD + DAG_SLACK)
        self.assertEqual(dag_bound(DAG_GRANULE + 1), DAG_GRANULE + 1 + 2 * DAG_GRANULE_OVERHEAD + DAG_SLACK)
        self.assertEqual(dag_bound(dag.MAX_SIZE) - dag.MAX_SIZE,
                         DAG_GRANULE_OVERHEAD * (dag.MAX_SIZE // DAG_GRANULE + 1) + DAG_SLACK)

    def test_room_for_the_largest_honest_nodes(self):
        # A file node child is at most 51 bytes: a full file node of MAX_CHILDREN chunks fits the allowance of
        # its chunks many times over, even at FastCDC's smallest chunk (16 KiB)
        child = len(dag.encode(dag.FileNode([dag.Child(Cid.for_block(DAG, b"x"), dag.MAX_SIZE)])))
        self.assertLessEqual(child - 3, 51)
        full = len(dag.encode(dag.FileNode([dag.Child(Cid.for_block(DAG, b"%d" % i), dag.MAX_SIZE)
                                            for i in range(MAX_CHILDREN)])))
        self.assertLess(full, MAX_CHILDREN * (16 * 1024 // DAG_GRANULE) * DAG_GRANULE_OVERHEAD / 4)
        # A directory node pays for itself; so does an index node of a few children, but one of a single child
        # costs a little, so a chain of them can't pay for itself
        index = dag.IndexNode([dag.IndexChild(name(0), Cid.for_block(DAG, b"x"), dag.MAX_SIZE)])
        expect = walker.Expect(Cid.for_block(DAG, dag.encode(index)), dag.MAX_SIZE, walker.DIRECTORY_TYPES, 1)
        block = dag.encode(index)
        self.assertLess(node_credit(len(block), walker.expand(expect, block)), len(block))
        self.assertGreater(INDEX_NODE_COST, 3 * DAG_ENTRY_OVERHEAD)
        # An honest index tree has at most one node that costs anything per level, met before the entries below
        # it bring their share: seven levels (2^64 entries) fit DAG_SLACK
        self.assertLess(7 * (INDEX_NODE_COST - DAG_ENTRY_OVERHEAD), DAG_SLACK)


class CapTest(TwoNodeTest):
    """B fetches from A what A built or was given, counting B's /blocks requests."""

    def setUp(self):
        """Count requests as well as blocks."""
        super().setUp()
        self.requests = 0
        real = self.net.blocks

        def counting(*args, **kwargs):
            """NetClient.blocks, counted."""
            self.requests += 1
            return real(*args, **kwargs)

        self.net.blocks = counting
        self.random = random.Random(1)

    def data(self, length: int) -> bytes:
        """length reproducible random bytes."""
        return self.random.randbytes(length)

    def add_file(self, length: int, chunker: str | None = None) -> Cid:
        """A file of length random bytes added on A and advertised."""
        cid = Cid.parse(self.a.add_stream(io.BytesIO(self.data(length)), chunker=chunker)["cid"])
        self.a.records.store(cid.key, A_ID)
        return cid

    def add_tree(self, make) -> Cid:
        """make(path) builds a tree on disk; it is added on A and advertised."""
        path = os.path.join(self.tmp.name, f"tree{self.random.random()}")
        make(path)
        cid = Cid.parse(self.a.add_path(path)["cid"])
        self.a.records.store(cid.key, A_ID)
        return cid

    def usage(self, node, cid: Cid) -> tuple[int, int, int, int]:
        """(declared size, bytes of distinct blocks, what its nodes bring to the bound, directory entries) of a
        stored DAG."""
        get = node.storage.getter()
        expect = walker.root_expect(cid)
        total = credit = entries = 0
        for current, block in walker.iter_dag(expect, get):
            total += len(block)
            if not current.cid.is_raw:
                children = walker.expand(current, block)
                credit += node_credit(len(block), children)
                entries += sum(1 for child in children if child.kind is not None)
        return walker.size_of(expect, get(expect)), total, credit, entries

    def assert_well_inside(self, cid: Cid):
        """B fetched cid's DAG with get, pin add and export; of the bound's margin (what neither the content nor
        nodes paying for themselves take: dag_bound beyond the size, and DAG_ENTRY_OVERHEAD per entry), its
        nodes take under half."""
        out = os.path.join(self.tmp.name, f"out{self.random.random()}")
        self.b.pin_add(str(cid))
        self.assertIn(USER, self.b.storage.pins()[cid])
        self.assertTrue(self.b.storage.is_complete(cid))
        self.b.get(str(cid), out)
        self.b.export(str(cid), out + ".archive")
        size, total, credit, entries = self.usage(self.b, cid)
        allowed = dag_bound(size) + credit
        margin = dag_bound(size) - size + DAG_ENTRY_OVERHEAD * entries
        self.assertGreaterEqual(allowed - total, margin / 2,
                                f"{total} bytes of blocks for {size} bytes and {entries} entries: {allowed} allowed")
        return out

    # Honest content

    def test_file_of_two_node_levels(self):
        cid = self.add_file((MAX_CHILDREN + 4) * CHUNK_SIZE + 5)
        self.assertEqual(self.b.info(str(cid))["size"], (MAX_CHILDREN + 4) * CHUNK_SIZE + 5)
        out = self.assert_well_inside(cid)
        self.assertEqual(os.path.getsize(out), (MAX_CHILDREN + 4) * CHUNK_SIZE + 5)

    def test_fastcdc_file(self):
        self.assert_well_inside(self.add_file(3 * 1024 * 1024 + 7, chunker="fastcdc"))

    def test_files_just_over_a_chunk(self):
        # Two chunks each: a file node per file, the most per-file overhead the fixed chunker writes
        def make(path):
            """Thirty such files with the longest names."""
            os.makedirs(path)
            for i in range(30):
                with open(os.path.join(path, name(i)), "wb") as f:
                    f.write(self.data(CHUNK_SIZE + 1))
        self.assert_well_inside(self.add_tree(make))

    def test_deep_directories_with_long_names(self):
        # Built in memory: 60 directories with the longest names don't fit a local path
        with self.a.storage.protect() as session:
            built = builder.build_bytes(self.data(CHUNK_SIZE + 1), session.put)
            for depth in range(60):
                entries = [dag.Entry(name(1), builder.build_bytes(b"", session.put).cid, 0, dag.KIND_FILE),
                           dag.Entry(name(2), builder.build_bytes(self.data(1), session.put).cid, 1, dag.KIND_FILE),
                           dag.Entry(name(3), built.cid, built.size, built.kind)]
                built = builder.build_directory(entries, session.put, built.height)
            self.a.storage.pin(built.cid, USER)
        self.a.publisher.refresh()
        self.assertEqual(built.height, 61)
        self.a.records.store(built.cid.key, A_ID)
        self.assert_well_inside(built.cid)

    def test_wide_directory_of_empty_files_and_directories(self):
        def make(path):
            """Over MAX_CHILDREN empty files (so index nodes) and some empty directories, longest names."""
            os.makedirs(path)
            for i in range(2 * MAX_CHILDREN + 3):
                with open(os.path.join(path, name(i)), "wb"):
                    pass
            for i in range(10):
                os.makedirs(os.path.join(path, name(10_000 + i)))
        cid = self.add_tree(make)
        self.assertIsInstance(dag.decode(self.a.storage.get(cid)), dag.IndexNode)
        self.assertEqual(self.b.info(str(cid))["size"], 0)
        self.assert_well_inside(cid)

    def test_empty_file_and_empty_directory(self):
        empty = self.add_file(0)
        self.assertEqual(empty, EMPTY_FILE)
        self.assert_well_inside(empty)
        directory = self.add_tree(os.makedirs)
        self.assertEqual(self.a.storage.get(directory), dag.encode(dag.DirectoryNode([])))
        self.assert_well_inside(directory)

    def test_dir_set_and_dir_rm(self):
        def make(path):
            """A directory of a few small files."""
            os.makedirs(path)
            for i in range(5):
                with open(os.path.join(path, name(i, 8)), "wb") as f:
                    f.write(self.data(100 * i))
        directory = self.add_tree(make)
        child = self.add_file(MAX_CHILDREN * CHUNK_SIZE + 1)
        new = Cid.parse(self.b.dir_set(str(directory), "big", str(child))["cid"])
        self.assertTrue(self.b.storage.is_complete(new))
        removed = Cid.parse(self.b.dir_rm(f"{new}/{name(0, 8)}")["cid"])
        self.assertTrue(self.b.storage.is_complete(removed))

    def test_honest_directories_are_fetched_in_as_few_requests(self):
        def make(path):
            """Forty small subdirectories of a few tiny files: many DAG nodes, little content."""
            for i in range(40):
                os.makedirs(os.path.join(path, name(i, 10)))
                for j in range(3):
                    with open(os.path.join(path, name(i, 10), name(j, 6)), "wb") as f:
                        f.write(self.data(10 + j))
        cid = self.add_tree(make)
        self.b.pin_add(str(cid))
        bounded = self.requests
        other = self.make_node("c", bytes([0xC] * 16))
        other.routing.heard(A_ID)
        self.requests = 0
        with mock.patch.object(fetcher, "dag_bound", lambda size: 1 << 62):
            other.pin_add(str(cid))
        self.assertEqual(bounded, self.requests)

    # Content made to break the bound

    def serve_nodes(self, nodes: list, raw: bytes = b"") -> Cid:
        """Encode nodes (the last is the root), store them and the raw block on A, pin and advertise the root."""
        blocks: dict[Cid, bytes] = {EMPTY_FILE: b"", Cid.for_block(RAW, raw): raw}
        cid = None
        for node in nodes:
            block = dag.encode(node)
            cid = Cid.for_block(DAG, block)
            blocks[cid] = block
        self.serve(blocks, cid)
        return cid

    def empty_file_nodes(self, count: int, children: int) -> list:
        """count distinct file nodes of children empty chunks (about 41 bytes each), each of size 0."""
        return [dag.FileNode([dag.Child(EMPTY_FILE, 0)] * (children + i)) for i in range(count)]

    def cids(self, nodes: list) -> list[Cid]:
        """The CIDs of nodes."""
        return [Cid.for_block(DAG, dag.encode(node)) for node in nodes]

    def test_small_root_over_large_empty_nodes(self):
        # The root fits the bound of its 1 byte; each node below it is 80 KB of empty chunks
        empties = self.empty_file_nodes(15, 2000)
        x = Cid.for_block(RAW, b"x")
        root = dag.FileNode([dag.Child(cid, 0) for cid in self.cids(empties)] + [dag.Child(x, 1)])
        self.assertLess(len(dag.encode(root)), dag_bound(1))
        cid = self.serve_nodes(empties + [root], b"x")
        for fetch in (lambda: self.b.pin_add(str(cid)), lambda: self.b.get(str(cid), os.path.join(self.tmp.name, "o")),
                      lambda: self.b.export(str(cid), os.path.join(self.tmp.name, "o.archive"))):
            self.net.reset()
            self.b.gc()
            with self.assertRaises(TooLarge) as caught:
                fetch()
            self.assertIn("refusing it", str(caught.exception))
            # Without holding requests to the room left, a request for 8 nodes would bring 640 KB
            self.assertLessEqual(self.net.downloaded_bytes, dag_bound(1) + MAX_BLOCK_SIZE)
        self.assertNotIn(cid, self.b.storage.pins())

    def test_directory_of_empty_nodes(self):
        # Each entry brings DAG_ENTRY_OVERHEAD; each points to 40 KB of empty chunks
        empties = self.empty_file_nodes(200, 1000)
        directory = dag.DirectoryNode([dag.Entry(name(i, 8), cid, 0, 0) for i, cid in enumerate(self.cids(empties))])
        cid = self.serve_nodes(empties + [directory])
        with self.assertRaises(TooLarge):
            self.b.pin_add(str(cid))
        limit = dag_bound(0) + len(empties) * DAG_ENTRY_OVERHEAD
        self.assertLessEqual(self.net.downloaded_bytes, len(dag.encode(directory)) + limit + MAX_BLOCK_SIZE)
        self.assertNotIn(cid, self.b.storage.pins())

    def test_dir_set_with_a_child_that_breaks_the_bound(self):
        directory = self.add_tree(os.makedirs)
        bomb = self.serve_bomb()
        with self.assertRaises(TooLarge):
            self.b.dir_set(str(directory), "child", str(bomb.root))
        self.assertEqual(self.b.storage.pins(), {})

    def test_a_quota_is_overshot_by_one_block_at_most(self):
        # Directory nodes bring room under the size-derived bound, but not under a quota: they count in full there
        with self.a.storage.protect() as session:
            empty = builder.build_bytes(b"", session.put).cid
            subdirectories = []
            for i in range(20):
                built = builder.build_directory([dag.Entry(name(j, 100) + str(i), empty, 0, 0) for j in range(250)],
                                                session.put)
                subdirectories.append(dag.Entry(name(i, 8), built.cid, 0, dag.KIND_DIRECTORY))
            root = builder.build_directory(subdirectories, session.put).cid
            self.a.storage.pin(root, USER)
        self.a.publisher.refresh()
        self.a.records.store(root.key, A_ID)
        with self.assertRaises(TooLarge), self.b.fetcher.open(root, pinned=True) as fetch:
            fetch.max_dag_bytes = 50_000
            fetch.fetch_dag(fetch.resolve())
        self.assertLessEqual(self.net.downloaded_bytes, 50_000 + MAX_BLOCK_SIZE)

    def test_a_lower_quota_still_applies(self):
        cid = self.add_file(4 * CHUNK_SIZE)
        with self.assertRaises(TooLarge) as caught, self.b.fetcher.open(cid, pinned=True) as fetch:
            fetch.max_dag_bytes = 2 * CHUNK_SIZE
            fetch.fetch_dag(fetch.resolve())
        self.assertNotIn("refusing it", str(caught.exception))  # the quota's message: it was lower
        with self.b.fetcher.open(cid, pinned=True) as fetch:
            fetch.max_dag_bytes = 10 * CHUNK_SIZE
            fetch.fetch_dag(fetch.resolve())
            self.assertEqual(fetch.dag_limit(), dag_bound(4 * CHUNK_SIZE))


if __name__ == "__main__":
    unittest.main()
