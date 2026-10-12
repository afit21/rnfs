"""Tests for walking DAGs (specification sections 5 and 7): reader checks, paths, ranges, listing, writing out."""
import errno
import os
import stat
import tempfile
import unittest
from unittest import mock

from rnfs import constants, fsutil
from rnfs.content import builder, dag, walker
from rnfs.content.cid import DAG, EMPTY_DIRECTORY, RAW, Cid
from rnfs.content.dag import Child, DirectoryNode, Entry, FileNode, IndexChild, IndexNode
from rnfs.content.walker import Expect, root_expect
from rnfs.errors import InvalidDag, NotFound, TooLarge
from tests.support import POSIX_MODES


class Blocks(dict):
    """In-memory blocks with a walker get() that counts reads."""

    def __init__(self):
        """No blocks, no reads."""
        super().__init__()
        self.reads: dict[Cid, int] = {}

    def put(self, cid: Cid, block: bytes):
        """Store a block."""
        self[cid] = block

    def get(self, expect: Expect) -> bytes:
        """The block for expect, or NotFound."""
        self.reads[expect.cid] = self.reads.get(expect.cid, 0) + 1
        if expect.cid not in self:
            raise NotFound(str(expect.cid))
        return self[expect.cid]

    def raw(self, data: bytes) -> Cid:
        """Store a raw block."""
        cid = Cid.for_block(RAW, data)
        self[cid] = data
        return cid

    def node(self, node) -> Cid:
        """Store a DAG node, valid or not (encode doesn't check)."""
        block = dag.encode(node)
        cid = Cid.for_block(DAG, block)
        self[cid] = block
        return cid


def small_tree():
    """Small CHUNK_SIZE and MAX_CHILDREN, for multi-level trees."""
    return mock.patch.multiple(constants, CHUNK_SIZE=4, MAX_CHILDREN=3)


def read_all(expect: Expect, blocks: Blocks, offset: int = 0, length: int | None = None) -> bytes:
    """A file's bytes in a range."""
    return b"".join(walker.read_range(expect, blocks.get, offset, length))


class ReaderCheckTest(unittest.TestCase):
    """Every reader check of section 5 rejects a crafted malicious DAG."""

    def setUp(self):
        """Some valid pieces to build malicious DAGs from."""
        self.blocks = Blocks()
        self.chunk = self.blocks.raw(b"abcd")
        self.file = self.blocks.node(FileNode([Child(self.chunk, 4), Child(self.chunk, 4)]))
        self.directory = self.blocks.node(DirectoryNode([Entry("x", self.chunk, 4, 0)]))
        self.index = self.blocks.node(IndexNode([IndexChild("x", self.directory, 4)]))

    def assertInvalid(self, root: Cid, names=()):
        """Walking root (or resolving a path in it) must fail as InvalidDag."""
        with self.assertRaises(InvalidDag):
            if names:
                walker.resolve(root, list(names), self.blocks.get)
            else:
                list(walker.iter_dag(root_expect(root), self.blocks.get))

    def test_valid_pieces(self):
        for root in (self.chunk, self.file, self.directory, self.index):
            list(walker.iter_dag(root_expect(root), self.blocks.get))

    def test_child_types(self):
        b = self.blocks
        cases = {
            "file child is a directory": FileNode([Child(self.directory, 4)]),
            "file child is an index": FileNode([Child(self.index, 4)]),
            "kind 0 entry is a directory": DirectoryNode([Entry("a", self.directory, 4, 0)]),
            "kind 1 entry is an index": DirectoryNode([Entry("a", self.index, 4, 1)]),
            "kind 2 entry is raw": DirectoryNode([Entry("a", self.chunk, 4, 2)]),
            "kind 2 entry is a file": DirectoryNode([Entry("a", self.file, 8, 2)]),
            "index child is raw": IndexNode([IndexChild("a", self.chunk, 4)]),
            "index child is a file": IndexNode([IndexChild("a", self.file, 8)]),
        }
        for reason, node in cases.items():
            with self.subTest(reason):
                self.assertInvalid(b.node(node))

    def test_sizes(self):
        b = self.blocks
        cases = {
            "chunk size too big": FileNode([Child(self.chunk, 5)]),
            "chunk size too small": FileNode([Child(self.chunk, 3)]),
            "file node size": FileNode([Child(self.file, 7)]),
            "entry size": DirectoryNode([Entry("a", self.file, 9, 0)]),
            "directory entry size": DirectoryNode([Entry("a", self.directory, 0, 2)]),
            "index child size": IndexNode([IndexChild("x", self.directory, 5)]),
        }
        for reason, node in cases.items():
            with self.subTest(reason):
                self.assertInvalid(b.node(node))

    def test_size_overflow(self):
        # Two children whose sizes sum past 2^64 - 1: the node doesn't even decode
        half = 2**63
        block = dag.encode(FileNode([Child(self.chunk, half), Child(self.chunk, half)]))
        cid = Cid.for_block(DAG, block)
        self.blocks[cid] = block
        self.assertInvalid(cid)
        # A size claimed at the maximum is caught against the child
        self.assertInvalid(self.blocks.node(FileNode([Child(self.chunk, 2**64 - 1)])))

    def test_index_first_names(self):
        b = self.blocks
        dir_ab = b.node(DirectoryNode([Entry("a", self.chunk, 4, 0), Entry("b", self.chunk, 4, 0)]))
        dir_cd = b.node(DirectoryNode([Entry("c", self.chunk, 4, 0), Entry("d", self.chunk, 4, 0)]))
        empty = EMPTY_DIRECTORY
        b[empty] = dag.encode(DirectoryNode([]))
        valid = b.node(IndexNode([IndexChild("a", dir_ab, 8), IndexChild("c", dir_cd, 8)]))
        list(walker.iter_dag(root_expect(valid), b.get))
        cases = {
            "first_name isn't the first name below": IndexNode([IndexChild("0", dir_ab, 8), IndexChild("c", dir_cd, 8)]),
            "first_name after the first name below": IndexNode([IndexChild("b", dir_ab, 8), IndexChild("c", dir_cd, 8)]),
            "names below reach the next first_name": IndexNode([IndexChild("a", dir_ab, 8), IndexChild("b", dir_cd, 8)]),
            "overlapping children": IndexNode([IndexChild("a", dir_ab, 8), IndexChild("ab", dir_ab, 8)]),
            "empty directory under an index": IndexNode([IndexChild("a", empty, 0)]),
        }
        for reason, node in cases.items():
            with self.subTest(reason):
                self.assertInvalid(b.node(node))

    def test_nested_index_bounds(self):
        # The upper bound from a higher index applies to everything below its child
        b = self.blocks
        dir_ab = b.node(DirectoryNode([Entry("a", self.chunk, 4, 0), Entry("b", self.chunk, 4, 0)]))
        dir_ez = b.node(DirectoryNode([Entry("e", self.chunk, 4, 0), Entry("z", self.chunk, 4, 0)]))
        dir_m = b.node(DirectoryNode([Entry("m", self.chunk, 4, 0)]))
        lower = b.node(IndexNode([IndexChild("a", dir_ab, 8), IndexChild("e", dir_ez, 8)]))
        bad = b.node(IndexNode([IndexChild("a", lower, 16), IndexChild("m", dir_m, 4)]))  # "z" >= "m"
        self.assertInvalid(bad)

    def test_unsorted_or_duplicate_names(self):
        b = self.blocks
        self.assertInvalid(b.node(DirectoryNode([Entry("b", self.chunk, 4, 0), Entry("a", self.chunk, 4, 0)])))
        self.assertInvalid(b.node(DirectoryNode([Entry("a", self.chunk, 4, 0), Entry("a", self.chunk, 4, 0)])))
        self.assertInvalid(b.node(IndexNode([IndexChild("x", self.directory, 4), IndexChild("x", self.directory, 4)])))

    def test_invalid_names(self):
        for name in (".", "..", "a/b", ""):
            with self.subTest(name=name):
                self.assertInvalid(self.blocks.node(DirectoryNode([Entry(name, self.chunk, 4, 0)])))

    def test_depth(self):
        with mock.patch.object(constants, "MAX_DEPTH", 4):
            cid = self.directory  # depth counts DAG nodes; the raw chunk below isn't one
            for _ in range(3):
                cid = self.blocks.node(DirectoryNode([Entry("d", cid, 4, 2)]))
            list(walker.iter_dag(root_expect(cid), self.blocks.get))  # 4 DAG nodes deep
            too_deep = self.blocks.node(DirectoryNode([Entry("d", cid, 4, 2)]))
            self.assertInvalid(too_deep)
            self.assertInvalid(too_deep, ["d", "d", "d", "d", "x"])

    def test_root_types(self):
        expect = Expect(self.chunk, None, walker.DIRECTORY_TYPES, 1)
        with self.assertRaises(InvalidDag):
            walker.expand(expect, self.blocks[self.chunk])
        with self.assertRaises(InvalidDag):
            walker.check_cid(expect)
        with self.assertRaises(InvalidDag):
            walker.expand(Expect(self.file, None, frozenset({"raw"}), 1), self.blocks[self.file])

    def test_oversized_raw_block(self):
        with self.assertRaises(InvalidDag):
            walker.check_raw(root_expect(self.chunk), constants.MAX_BLOCK_SIZE + 1)

    def test_not_a_dag_node(self):
        block = b"\x93\x01\x02\x03"
        cid = Cid.for_block(DAG, block)
        self.blocks[cid] = block
        self.assertInvalid(cid)

    def test_repeated_cid_checked_per_occurrence(self):
        # The same chunk listed correctly, then with a wrong size: the second occurrence must still fail
        b = self.blocks
        good = b.node(FileNode([Child(self.chunk, 4)]))
        lying = b.node(FileNode([Child(self.chunk, 5)]))
        root = b.node(DirectoryNode([Entry("a", good, 4, 0), Entry("b", lying, 5, 0)]))
        self.assertInvalid(root)
        # And the same subtree at an allowed and a too-great depth
        with mock.patch.object(constants, "MAX_DEPTH", 3):
            deep = b.node(DirectoryNode([Entry("x", self.directory, 4, 2)]))
            deeper = b.node(DirectoryNode([Entry("y", deep, 4, 2)]))
            root = b.node(DirectoryNode([Entry("a", deep, 4, 2), Entry("b", deeper, 4, 2)]))
            self.assertInvalid(root)


class WalkTest(unittest.TestCase):
    """Paths, listing, byte ranges and full walks over built trees."""

    def test_iter_dag_each_block_once(self):
        blocks = Blocks()
        chunk = blocks.raw(b"same")
        sub = blocks.node(FileNode([Child(chunk, 4)] * 5))
        root = blocks.node(DirectoryNode([Entry(n, sub, 20, 0) for n in "abc"] + [Entry("d", chunk, 4, 1)]))
        walked = list(walker.iter_dag(root_expect(root), blocks.get))
        self.assertEqual([e.cid for e, _ in walked], [root, sub, chunk])
        self.assertTrue(all(count == 1 for count in blocks.reads.values()))
        self.assertEqual(walked[0][1], blocks[root])

    def test_iter_dag_rereads_a_node_met_under_another_expectation(self):
        # Node bytes aren't kept for the whole walk, but every occurrence is still checked
        blocks = Blocks()
        chunk = blocks.raw(b"same")
        sub = blocks.node(FileNode([Child(chunk, 4)]))
        root = blocks.node(DirectoryNode([Entry("a", sub, 4, 0), Entry("b", sub, 4, 1)]))
        walked = list(walker.iter_dag(root_expect(root), blocks.get))
        self.assertEqual([e.cid for e, _ in walked], [root, sub, chunk])
        self.assertEqual(blocks.reads[sub], 2)
        bad = blocks.node(DirectoryNode([Entry("a", sub, 4, 0), Entry("b", sub, 5, 1)]))
        with self.assertRaises(InvalidDag):
            list(walker.iter_dag(root_expect(bad), blocks.get))
        bad = blocks.node(DirectoryNode([Entry("a", sub, 4, 0), Entry("b", chunk, 4, 0), Entry("c", chunk, 3, 0)]))
        with self.assertRaises(InvalidDag):  # a raw block met again is checked by its length
            list(walker.iter_dag(root_expect(bad), blocks.get))

    def test_iter_dags_yields_each_block_once_over_every_root(self):
        blocks = Blocks()
        chunk = blocks.raw(b"same")
        sub = blocks.node(FileNode([Child(chunk, 4)]))
        root = blocks.node(DirectoryNode([Entry("a", sub, 4, 0)]))
        walked = [e.cid for e, _ in walker.iter_dags([root_expect(sub), root_expect(root), root_expect(chunk)],
                                                        blocks.get)]
        self.assertEqual(walked, [sub, chunk, root])

    def test_iter_dag_depth_first(self):
        with small_tree():
            blocks = Blocks()
            built = builder.build_bytes(bytes(range(40)), blocks.put)
            order = [e.cid for e, _ in walker.iter_dag(root_expect(built.cid), blocks.get)]
            self.assertEqual(set(order), set(blocks))
            self.assertEqual(order[0], built.cid)
            # Raw chunks appear in content order
            chunks = [blocks[c] for c in order if c.is_raw]
            self.assertEqual(b"".join(chunks), bytes(range(40)))

    def test_missing_block(self):
        blocks = Blocks()
        root = blocks.node(FileNode([Child(Cid.for_block(RAW, b"gone"), 4)]))
        with self.assertRaises(NotFound):
            list(walker.iter_dag(root_expect(root), blocks.get))

    def test_read_range(self):
        with small_tree():
            blocks = Blocks()
            data = bytes(i % 256 for i in range(103))
            built = builder.build_bytes(data, blocks.put)
            expect = root_expect(built.cid)
            self.assertEqual(read_all(expect, blocks), data)
            for offset, length in [(0, 1), (3, 2), (4, 4), (5, 50), (100, 10), (103, 5), (200, 1), (0, 0), (37, None)]:
                with self.subTest(offset=offset, length=length):
                    end = None if length is None else offset + length
                    self.assertEqual(read_all(expect, blocks, offset, length), data[offset:end])

    def test_read_range_fetches_only_covering_chunks(self):
        with small_tree():
            blocks = Blocks()
            built = builder.build_bytes(bytes(100), blocks.put)
            blocks.reads.clear()
            read_all(root_expect(built.cid), blocks, 10, 3)
            raw_reads = [c for c in blocks.reads if c.is_raw]
            self.assertEqual(len(raw_reads), 1)

    def test_read_raw_and_directory(self):
        blocks = Blocks()
        raw = blocks.raw(b"hello")
        self.assertEqual(read_all(root_expect(raw), blocks, 1, 3), b"ell")
        directory = blocks.node(DirectoryNode([]))
        with self.assertRaises(ValueError):
            read_all(root_expect(directory), blocks)

    def test_range_children(self):
        blocks = Blocks()
        a, b = blocks.raw(b"aaaa"), blocks.raw(b"bb")
        node = blocks.node(FileNode([Child(a, 4), Child(b, 2), Child(a, 4)]))
        parts = walker.range_children(root_expect(node), blocks[node], 3, 5)
        self.assertEqual([(e.cid, o, n) for e, o, n in parts], [(a, 3, 1), (b, 0, 2), (a, 0, 2)])

    def test_range_children_skip_empty_children(self):
        blocks = Blocks()
        a, empty = blocks.raw(b"aaaa"), blocks.raw(b"")
        node = blocks.node(FileNode([Child(a, 4), Child(empty, 0), Child(empty, 0), Child(a, 4)]))
        parts = walker.range_children(root_expect(node), blocks[node], 0, 8)
        self.assertEqual([(e.cid, o, n) for e, o, n in parts], [(a, 0, 4), (a, 0, 4)])

    def test_small_chunks_count_against_max_entries(self):
        # A file of 256 × 256 one-byte chunks: tiny on disk, but each byte costs a whole chunk's work
        blocks = Blocks()
        cid, size = blocks.raw(b"x"), 1
        for _ in range(2):
            cid, size = blocks.node(FileNode([Child(cid, size)] * 256)), size * 256
        with self.assertRaises(TooLarge):
            b"".join(walker.read_range(root_expect(cid), blocks.get, small=walker.SmallLeaves(1000)))
        # Full chunks are free, and a range has at most two short pieces at its ends
        built = builder.build_bytes(os.urandom(5 * constants.CHUNK_SIZE), blocks.put)
        data = b"".join(walker.read_range(root_expect(built.cid), blocks.get, 10, 3 * constants.CHUNK_SIZE,
                                          small=walker.SmallLeaves(0)))
        self.assertEqual(len(data), 3 * constants.CHUNK_SIZE)

    def test_resolve_through_index(self):
        with small_tree():
            blocks = Blocks()
            names = [f"f{i:02d}" for i in range(40)]
            entries = [Entry(n, blocks.raw(n.encode()), 3, 0) for n in names]
            root = builder.build_directory(entries, blocks.put).cid
            self.assertIsInstance(dag.decode(blocks[root]), IndexNode)
            for name in names:
                with self.subTest(name=name):
                    expect = walker.resolve(root, [name], blocks.get)
                    self.assertEqual(blocks[expect.cid], name.encode())
                    self.assertEqual(expect.kind, dag.KIND_FILE)
            for missing in ("a", "f00a", "f39z", "zz", "f1"):
                with self.subTest(missing=missing):
                    with self.assertRaises(NotFound):
                        walker.resolve(root, [missing], blocks.get)

    def test_resolve_nested_and_into_file(self):
        blocks = Blocks()
        leaf = builder.build_bytes(b"leaf", blocks.put)
        inner = builder.wrap("leaf", leaf, blocks.put)
        outer = builder.wrap("inner", inner, blocks.put)
        self.assertEqual(walker.resolve(outer.cid, ["inner", "leaf"], blocks.get).cid, leaf.cid)
        self.assertEqual(walker.resolve(outer.cid, [], blocks.get), root_expect(outer.cid))
        with self.assertRaises(NotFound):
            walker.resolve(outer.cid, ["inner", "leaf", "deeper"], blocks.get)

    def test_list_directory(self):
        with small_tree():
            blocks = Blocks()
            entries = [Entry(f"e{i:02d}", blocks.raw(b"x"), 1, i % 2) for i in range(20)]
            root = builder.build_directory(entries, blocks.put).cid
            listed = list(walker.list_directory(root_expect(root), blocks.get))
            self.assertEqual(listed, sorted(entries, key=lambda e: e.name))
            with self.assertRaises(TooLarge):
                list(walker.list_directory(root_expect(root), blocks.get, max_entries=19))
            file = blocks.raw(b"f")
            with self.assertRaises(ValueError):
                list(walker.list_directory(root_expect(file), blocks.get))

    def test_count_entries_repeated_empty_subtrees(self):
        blocks = Blocks()
        cid = EMPTY_DIRECTORY
        blocks[cid] = dag.encode(DirectoryNode([]))
        for _ in range(3):  # 10 + 100 + 1000 entries, all of size 0, in four blocks
            cid = blocks.node(DirectoryNode([Entry(f"d{i}", cid, 0, 2) for i in range(10)]))
        self.assertEqual(walker.count_entries(root_expect(cid), blocks.get, 10_000), 1110)
        with self.assertRaises(TooLarge):
            walker.count_entries(root_expect(cid), blocks.get, 1109)
        self.assertEqual(len(blocks.reads), 4)

    def test_height(self):
        with small_tree():
            blocks = Blocks()
            built = builder.build_bytes(bytes(50), blocks.put)
            self.assertEqual(walker.height(root_expect(built.cid), blocks.get), built.height)

    def test_parse_target(self):
        cid = Cid.for_block(RAW, b"x")
        self.assertEqual(walker.parse_target(str(cid)), (cid, []))
        self.assertEqual(walker.parse_target(f"{cid}/a/b"), (cid, ["a", "b"]))
        self.assertEqual(walker.parse_target(f"{cid}/a/"), (cid, ["a"]))
        for bad in ("", "nope", f"{cid}//a", f"{cid}/../a", f"{cid}/./a", "/" + str(cid), str(cid).upper()):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    walker.parse_target(bad)

    def test_kind_and_size(self):
        blocks = Blocks()
        built = builder.build_bytes(bytes(constants.CHUNK_SIZE + 1), blocks.put)
        wrapped = builder.wrap("a", built, blocks.put)
        self.assertEqual(walker.kind_of(root_expect(built.cid), blocks[built.cid]), "file")
        self.assertEqual(walker.kind_of(root_expect(wrapped.cid), blocks[wrapped.cid]), "directory")
        self.assertEqual(walker.size_of(root_expect(wrapped.cid), blocks[wrapped.cid]), constants.CHUNK_SIZE + 1)


class WriteOutTest(unittest.TestCase):
    """Writing files and directories to the local filesystem."""

    def setUp(self):
        """A source tree and an output directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.blocks = Blocks()

    def path(self, *parts: str) -> str:
        """A path in the temporary directory."""
        return os.path.join(self.tmp.name, *parts)

    def test_round_trip_directory(self):
        source = self.path("src")
        os.makedirs(os.path.join(source, "sub", "empty"))
        files = {"a.txt": b"a", "sub/b.bin": os.urandom(3 * constants.CHUNK_SIZE + 5), ".h": b"", "run": b"#!/bin/sh"}
        for name, data in files.items():
            with open(os.path.join(source, name), "wb") as f:
                f.write(data)
        os.chmod(os.path.join(source, "run"), 0o755)
        built, _ = builder.add_path(source, self.blocks.put)
        dest = self.path("out")
        warnings = walker.write_out(root_expect(built.cid), self.blocks.get, dest, 100)
        self.assertEqual(warnings, [])
        for name, data in files.items():
            with open(os.path.join(dest, name), "rb") as f:
                self.assertEqual(f.read(), data)
        self.assertTrue(os.path.isdir(os.path.join(dest, "sub", "empty")))
        if POSIX_MODES:  # Windows has no execute bit to keep
            self.assertTrue(os.stat(os.path.join(dest, "run")).st_mode & stat.S_IXUSR)
            self.assertFalse(os.stat(os.path.join(dest, "a.txt")).st_mode & stat.S_IXUSR)
        self.assertEqual(builder.add_path(dest, builder.NO_PUT)[0].cid, built.cid)

    def test_write_file_and_executable_target(self):
        built = builder.build_bytes(b"#!", self.blocks.put, kind=dag.KIND_EXECUTABLE)
        wrapped = builder.wrap("x", built, self.blocks.put)
        target = walker.resolve(wrapped.cid, ["x"], self.blocks.get)
        walker.write_out(target, self.blocks.get, self.path("x"), 10)
        if POSIX_MODES:
            self.assertTrue(os.stat(self.path("x")).st_mode & stat.S_IXUSR)
        walker.write_out(root_expect(built.cid), self.blocks.get, self.path("plain"), 10)
        with open(self.path("plain"), "rb") as f:
            self.assertEqual(f.read(), b"#!")

    def test_windows_writes_executables_without_chmod(self):
        # os.chmod on Windows only sets or clears read-only; write_out mustn't rely on it there
        built = builder.build_bytes(b"#!", self.blocks.put, kind=dag.KIND_EXECUTABLE)
        wrapped = builder.wrap("x", built, self.blocks.put)
        target = walker.resolve(wrapped.cid, ["x"], self.blocks.get)
        with mock.patch.object(fsutil, "IS_WINDOWS", True), mock.patch("os.chmod") as chmod, \
                mock.patch("os.fchmod", create=True) as fchmod:
            walker.write_out(target, self.blocks.get, self.path("x"), 10)
        chmod.assert_not_called()
        fchmod.assert_not_called()
        with open(self.path("x"), "rb") as f:
            self.assertEqual(f.read(), b"#!")

    def test_refuses_existing_destination(self):
        built = builder.build_bytes(b"x", self.blocks.put)
        os.mkdir(self.path("exists"))
        with self.assertRaises(ValueError):
            walker.write_out(root_expect(built.cid), self.blocks.get, self.path("exists"), 10)

    def test_max_output_entries_counts_every_occurrence(self):
        cid = EMPTY_DIRECTORY
        self.blocks[cid] = dag.encode(DirectoryNode([]))
        for _ in range(3):
            cid = self.blocks.node(DirectoryNode([Entry(f"d{i}", cid, 0, 2) for i in range(10)]))
        with self.assertRaises(TooLarge):
            walker.write_out(root_expect(cid), self.blocks.get, self.path("bomb"), 1000)
        self.blocks.reads.clear()
        self.assertEqual(walker.write_out(root_expect(cid), self.blocks.get, self.path("ok"), 1110), [])
        self.assertTrue(os.path.isdir(self.path("ok", "d9", "d9", "d9")))
        self.assertEqual(list(self.blocks.reads.values()), [1, 1, 1, 1])  # each block read once

    def test_small_chunk_bomb_stops_write(self):
        cid, size = self.blocks.raw(b"x"), 1
        for _ in range(2):
            cid, size = self.blocks.node(FileNode([Child(cid, size)] * 256)), size * 256
        root = self.blocks.node(DirectoryNode([Entry("bomb", cid, size, 0)]))
        with self.assertRaises(TooLarge):
            walker.write_out(root_expect(root), self.blocks.get, self.path("out"), 1000)
        # Many small honest files are fine: each has one short chunk, and entries are within max_entries
        small = [Entry(f"f{i}", self.blocks.raw(b"%d" % i), len(b"%d" % i), 0) for i in range(50)]
        honest = builder.build_directory(small, self.blocks.put)
        self.assertEqual(walker.write_out(root_expect(honest.cid), self.blocks.get, self.path("ok"), 50), [])

    def test_skips_names_the_filesystem_would_misread(self):
        a = builder.build_bytes(b"a", self.blocks.put)
        root = builder.build_directory([Entry("bad", a.cid, 1, 0), Entry("good", a.cid, 1, 0)], self.blocks.put)
        real = walker.unsafe_name_reason
        with mock.patch.object(walker, "unsafe_name_reason", lambda n: "test reason" if n == "bad" else real(n)):
            warnings = walker.write_out(root_expect(root.cid), self.blocks.get, self.path("out"), 10)
        self.assertEqual(os.listdir(self.path("out")), ["good"])
        self.assertEqual(len(warnings), 1)
        self.assertIn("bad", warnings[0])

    def test_skips_names_this_filesystem_refuses(self):
        # Names ext4 takes but FAT or eCryptfs refuse: skipped and reported, the rest still written
        a = builder.build_bytes(b"a", self.blocks.put)
        sub = builder.build_directory([Entry("x", a.cid, 1, 0)], self.blocks.put)
        root = builder.build_directory([Entry("a?b", a.cid, 1, 0), Entry("d?", sub.cid, 1, 2),
                                        Entry("good", a.cid, 1, 0), Entry("long", a.cid, 1, 0)], self.blocks.put)

        def refusing(real, *names_and_errors):
            """real(path, ...), but OSError(error) for the names given."""
            refused = dict(names_and_errors)

            def operation(path, *args):
                """Refuse some names as the filesystem would."""
                error = refused.get(os.path.basename(path))
                if error is not None:
                    raise OSError(error, os.strerror(error), path)
                return real(path, *args)

            return operation

        with mock.patch.object(walker, "open", refusing(open, ("a?b", errno.EINVAL), ("long", errno.ENAMETOOLONG)),
                               create=True), \
                mock.patch("os.mkdir", refusing(os.mkdir, ("d?", errno.EINVAL))):
            warnings = walker.write_out(root_expect(root.cid), self.blocks.get, self.path("out"), 10)
        self.assertEqual(os.listdir(self.path("out")), ["good"])
        self.assertEqual(len(warnings), 3)
        self.assertTrue(all("can't represent" in warning for warning in warnings))
        # Any other error (a full disk) still fails the whole get
        with mock.patch.object(walker, "open", refusing(open, ("good", errno.ENOSPC)), create=True), \
                self.assertRaises(OSError):
            walker.write_out(root_expect(root.cid), self.blocks.get, self.path("out2"), 10)

    @unittest.skipUnless(POSIX_MODES, "Windows has no execute bit")
    def test_execute_bit_is_set_through_the_descriptor(self):
        # Not by path, which a symlink swapped in meanwhile could redirect
        built = builder.build_bytes(b"#!", self.blocks.put, kind=dag.KIND_EXECUTABLE)
        root = builder.build_directory([Entry("x", built.cid, 2, 1)], self.blocks.put)
        with mock.patch("os.chmod", side_effect=AssertionError("by path")):
            walker.write_out(root_expect(root.cid), self.blocks.get, self.path("out"), 10)
        self.assertTrue(os.stat(self.path("out", "x")).st_mode & stat.S_IXUSR)

    def test_unsafe_name_reason(self):
        self.assertIsNone(walker.unsafe_name_reason("fine name.txt"))
        for name in ("a/b", "..", "."):
            self.assertIsNotNone(walker.unsafe_name_reason(name))
        with mock.patch.object(os, "altsep", "\\"):
            self.assertIsNotNone(walker.unsafe_name_reason("a\\b"))
        windows_only = ("a:b", "con", "NUL.txt", "lpt1.tar.gz", "com\u00b9", "trailing.", "trailing ", "a?b", "a\\b")
        with mock.patch.object(fsutil, "IS_WINDOWS", True):
            self.assertIsNone(walker.unsafe_name_reason("fine name.txt"))
            self.assertIsNone(walker.unsafe_name_reason("console.log"))
            for name in windows_only:
                self.assertIsNotNone(walker.unsafe_name_reason(name), name)
        if not fsutil.IS_WINDOWS and os.sep == "/":
            for name in windows_only:
                self.assertIsNone(walker.unsafe_name_reason(name), name)

    def test_invalid_dag_stops_write(self):
        chunk = self.blocks.raw(b"abcd")
        root = self.blocks.node(DirectoryNode([Entry("a", chunk, 5, 0)]))
        with self.assertRaises(InvalidDag):
            walker.write_out(root_expect(root), self.blocks.get, self.path("out"), 10)


if __name__ == "__main__":
    unittest.main()
