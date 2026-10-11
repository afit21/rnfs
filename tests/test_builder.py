"""Tests for building trees (specification section 5): the tree rule, files, directories, wrap, only-hash."""
import io
import os
import tempfile
import threading
import unittest
from unittest import mock

from rnfs import constants, fsutil
from rnfs.content import builder, dag
from rnfs.content.builder import NO_PUT
from rnfs.content.cid import DAG, EMPTY_DIRECTORY, EMPTY_FILE, RAW, Cid
from rnfs.content.dag import Child, DirectoryNode, Entry, FileNode, IndexChild, IndexNode
from rnfs.errors import TooLarge
from tests.support import CAN_SYMLINK, HAS_FIFO, POSIX_MODES

HELLO_WORLD = "bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e"
# What add records for a file with the execute bit: Windows has none, so there every file is kind 0
EXEC_KIND = dag.KIND_EXECUTABLE if POSIX_MODES else dag.KIND_FILE


def within(seconds: float, function, *args):
    """function(*args)'s result, or what it raised; fails the test if it doesn't return within seconds."""
    outcome: list = []

    def run():
        """Call function, keeping its result or exception."""
        try:
            outcome.append(("result", function(*args)))
        except BaseException as e:  # noqa: B902 - handed back to the test
            outcome.append(("error", e))

    thread = threading.Thread(target=run, daemon=True)  # a hung call mustn't keep the test run alive
    thread.start()
    thread.join(seconds)
    if not outcome:
        raise AssertionError(f"still running after {seconds} s (blocked?)")
    kind, value = outcome[0]
    if kind == "error":
        raise value
    return value


def swap_on_open(name: str, swap):
    """Patch os.open so that swap() runs just before name is first opened: a race between listing and opening."""
    real_open = os.open
    swapped = []

    def fake_open(path, flags, mode=0o777, *, dir_fd=None):
        """os.open, swapping the entry first."""
        if not swapped and os.path.basename(os.fsdecode(path)) == name:
            swapped.append(True)
            swap()
        return real_open(path, flags, mode, dir_fd=dir_fd)

    return mock.patch("os.open", side_effect=fake_open)


class Blocks(dict):
    """An in-memory block store: put(cid, block) checks and keeps every block."""

    def put(self, cid: Cid, block: bytes):
        """Store a block, checking it matches its CID."""
        assert cid.verify(block), "builder produced a block that doesn't match its CID"
        self[cid] = block


def small_tree(chunk_size: int = 4, max_children: int = 3):
    """Patch CHUNK_SIZE and MAX_CHILDREN so multi-level trees are small."""
    return mock.patch.multiple(constants, CHUNK_SIZE=chunk_size, MAX_CHILDREN=max_children)


def reference_file(data: bytes, blocks: Blocks) -> Cid:
    """The tree rule for files, applied literally to the whole chunk list (no streaming)."""
    size = constants.CHUNK_SIZE
    chunks = [data[i:i + size] for i in range(0, len(data), size)]
    if not chunks:
        blocks.put(EMPTY_FILE, b"")
        return EMPTY_FILE
    children = []
    for chunk in chunks:
        cid = Cid.for_block(RAW, chunk)
        blocks.put(cid, chunk)
        children.append(Child(cid, len(chunk)))
    if len(children) == 1:
        return children[0].cid
    while len(children) > constants.MAX_CHILDREN:
        groups = [children[i:i + constants.MAX_CHILDREN] for i in range(0, len(children), constants.MAX_CHILDREN)]
        children = [Child(put_node(FileNode(group), blocks), FileNode(group).size) for group in groups]
    return put_node(FileNode(children), blocks)


def reference_directory(entries: list[Entry], blocks: Blocks) -> Cid:
    """The tree rule for directories, applied literally."""
    items = sorted(entries, key=lambda e: e.name)
    level = 0
    while len(items) > constants.MAX_CHILDREN:
        groups = [items[i:i + constants.MAX_CHILDREN] for i in range(0, len(items), constants.MAX_CHILDREN)]
        new_items = []
        for group in groups:
            node = DirectoryNode(group) if level == 0 else IndexNode(group)
            first = group[0].name if level == 0 else group[0].first_name
            new_items.append(IndexChild(first, put_node(node, blocks), node.size))
        items = new_items
        level += 1
    return put_node(DirectoryNode(items) if level == 0 else IndexNode(items), blocks)


def put_node(node, blocks: Blocks) -> Cid:
    """Encode and store a node."""
    block = dag.encode(node)
    cid = Cid.for_block(DAG, block)
    blocks.put(cid, block)
    return cid


class ShortReads(io.RawIOBase):
    """A stream that returns at most `most` bytes per read and records the largest read asked for."""

    def __init__(self, data: bytes, most: int):
        """Serve data in pieces of at most most bytes."""
        self.data = data
        self.pos = 0
        self.most = most
        self.largest_request = 0

    def read(self, n=-1):
        """At most self.most bytes."""
        self.largest_request = max(self.largest_request, n)
        piece = self.data[self.pos:self.pos + min(n, self.most)]
        self.pos += len(piece)
        return piece


class FileTest(unittest.TestCase):
    """Files from streams."""

    def test_small_file_is_one_raw_block(self):
        blocks = Blocks()
        built = builder.build_bytes(b"hello world", blocks.put)
        self.assertEqual(str(built.cid), HELLO_WORLD)
        self.assertEqual((built.size, built.kind, built.height), (11, dag.KIND_FILE, 0))
        self.assertEqual(dict(blocks), {built.cid: b"hello world"})

    def test_empty_file(self):
        blocks = Blocks()
        built = builder.build_bytes(b"", blocks.put)
        self.assertEqual(built.cid, EMPTY_FILE)
        self.assertEqual(blocks, {EMPTY_FILE: b""})

    def test_chunk_boundaries(self):
        size = constants.CHUNK_SIZE
        exact = builder.build_bytes(b"x" * size, NO_PUT)
        self.assertTrue(exact.cid.is_raw)
        blocks = Blocks()
        over = builder.build_bytes(b"x" * size + b"y", blocks.put)
        node = dag.decode(blocks[over.cid])
        self.assertEqual(node, FileNode([Child(exact.cid, size), Child(Cid.for_block(RAW, b"y"), 1)]))
        self.assertEqual((over.size, over.height), (size + 1, 1))

    def test_matches_tree_rule(self):
        with small_tree():
            for length in list(range(0, 60)) + [4 * 27, 4 * 27 + 1, 4 * 81 + 3]:
                with self.subTest(length=length):
                    data = bytes(i % 251 for i in range(length))
                    expected = Blocks()
                    built_blocks = Blocks()
                    built = builder.build_bytes(data, built_blocks.put)
                    self.assertEqual(built.cid, reference_file(data, expected))
                    self.assertEqual(built_blocks, expected)
                    self.assertEqual(built.size, length)

    def test_multi_level_shape(self):
        with small_tree():
            blocks = Blocks()
            built = builder.build_bytes(bytes(50), blocks.put)  # 13 chunks -> 5 groups -> 2 -> root
            root = dag.decode(blocks[built.cid])
            self.assertEqual([c.size for c in root.children], [36, 14])
            self.assertEqual(built.height, 3)

    def test_streams_short_reads_and_bounded_memory(self):
        data = os.urandom(3 * constants.CHUNK_SIZE + 17)
        stream = ShortReads(data, 1000)
        self.assertEqual(builder.build_file(stream, NO_PUT).cid, builder.build_bytes(data, NO_PUT).cid)
        self.assertLessEqual(stream.largest_request, constants.CHUNK_SIZE)

    def test_size_reads_exactly_size(self):
        stream = io.BytesIO(b"abcdef")
        built = builder.build_file(stream, NO_PUT, size=3)
        self.assertEqual(built.cid, Cid.for_block(RAW, b"abc"))
        self.assertEqual(stream.read(), b"def")

    def test_short_stream_with_size_fails(self):
        with self.assertRaises(ValueError):
            builder.build_file(io.BytesIO(b"abc"), NO_PUT, size=10)

    def test_only_hash_matches(self):
        data = os.urandom(2 * constants.CHUNK_SIZE)
        self.assertEqual(builder.build_bytes(data, NO_PUT), builder.build_bytes(data, Blocks().put))

    def test_add_stream_wraps(self):
        blocks = Blocks()
        built = builder.add_stream(io.BytesIO(b"hi"), blocks.put, name="a.txt", executable=True)
        node = dag.decode(blocks[built.cid])
        self.assertEqual(node, DirectoryNode([Entry("a.txt", Cid.for_block(RAW, b"hi"), 2, dag.KIND_EXECUTABLE)]))
        with self.assertRaises(ValueError):
            builder.add_stream(io.BytesIO(b"hi"), NO_PUT, name="a/b")


class DirectoryTest(unittest.TestCase):
    """Directories from entries."""

    def entries(self, count: int) -> list[Entry]:
        """count file entries with distinct names, in scrambled order."""
        names = [f"n{i:04d}" for i in range(count)]
        names = names[1::2] + names[::2]
        return [Entry(name, Cid.for_block(RAW, name.encode()), len(name), 0) for name in names]

    def test_empty_directory(self):
        blocks = Blocks()
        built = builder.build_directory([], blocks.put)
        self.assertEqual(built.cid, EMPTY_DIRECTORY)
        self.assertEqual((built.size, built.kind, built.height), (0, dag.KIND_DIRECTORY, 1))

    def test_sorted_bytewise(self):
        blocks = Blocks()
        entries = [Entry(n, EMPTY_FILE, 0, 0) for n in ("é", "b", "B", "a")]
        built = builder.build_directory(entries, blocks.put)
        self.assertEqual([e.name for e in dag.decode(blocks[built.cid]).entries], ["B", "a", "b", "é"])

    def test_rejects_duplicates_and_bad_names(self):
        with self.assertRaises(ValueError):
            builder.build_directory([Entry("a", EMPTY_FILE, 0, 0)] * 2, NO_PUT)
        for name in ("", ".", "..", "a/b", "x" * 256):
            with self.assertRaises(ValueError):
                builder.build_directory([Entry(name, EMPTY_FILE, 0, 0)], NO_PUT)

    def test_matches_tree_rule(self):
        with small_tree():
            for count in list(range(0, 30)) + [81, 82, 250]:
                with self.subTest(count=count):
                    entries = self.entries(count)
                    expected, built_blocks = Blocks(), Blocks()
                    built = builder.build_directory(entries, built_blocks.put)
                    self.assertEqual(built.cid, reference_directory(entries, expected))
                    self.assertEqual(built_blocks, expected)

    def test_index_levels(self):
        with small_tree():
            blocks = Blocks()
            built = builder.build_directory(self.entries(10), blocks.put)  # 4 groups of entries -> 2 -> root
            root = dag.decode(blocks[built.cid])
            self.assertIsInstance(root, IndexNode)
            self.assertEqual([c.first_name for c in root.children], ["n0000", "n0009"])
            lower = dag.decode(blocks[root.children[0].cid])
            self.assertIsInstance(lower, IndexNode)
            self.assertEqual([c.first_name for c in lower.children], ["n0000", "n0003", "n0006"])
            group = dag.decode(blocks[lower.children[1].cid])
            self.assertIsInstance(group, DirectoryNode)
            self.assertEqual([e.name for e in group.entries], ["n0003", "n0004", "n0005"])
            self.assertEqual(built.height, 3)
            self.assertEqual(built.size, 50)

    def test_wrap(self):
        blocks = Blocks()
        inner = builder.build_bytes(b"data", blocks.put)
        wrapped = builder.wrap("f", inner, blocks.put)
        self.assertEqual(blocks[wrapped.cid], dag.encode(DirectoryNode([Entry("f", inner.cid, 4, 0)])))
        self.assertEqual(wrapped.height, 1)


class PathTest(unittest.TestCase):
    """Files and directories from the filesystem."""

    def setUp(self):
        """A temporary directory to build trees in."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = os.path.join(self.tmp.name, "root")
        os.mkdir(self.root)

    def write(self, relative: str, data: bytes, mode: int | None = None) -> str:
        """Write a file under the root, creating directories."""
        path = os.path.join(self.root, relative)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
        if mode is not None:
            os.chmod(path, mode)
        return path

    def test_directory_tree(self):
        self.write("a.txt", b"a")
        self.write(".hidden", b"h")
        self.write("run.sh", b"#!", mode=0o755)
        self.write("sub/b.txt", b"bb")
        os.mkdir(os.path.join(self.root, "empty"))
        blocks = Blocks()
        built, warnings = builder.add_path(self.root, blocks.put)
        self.assertEqual(warnings, [])
        node = dag.decode(blocks[built.cid])
        sub = builder.build_directory([Entry("b.txt", Cid.for_block(RAW, b"bb"), 2, 0)], NO_PUT)
        self.assertEqual(node.entries, (
            Entry(".hidden", Cid.for_block(RAW, b"h"), 1, dag.KIND_FILE),
            Entry("a.txt", Cid.for_block(RAW, b"a"), 1, dag.KIND_FILE),
            Entry("empty", EMPTY_DIRECTORY, 0, dag.KIND_DIRECTORY),
            Entry("run.sh", Cid.for_block(RAW, b"#!"), 2, EXEC_KIND),
            Entry("sub", sub.cid, 2, dag.KIND_DIRECTORY),
        ))
        self.assertEqual(built.size, 6)
        self.assertIn(EMPTY_DIRECTORY, blocks)

    def test_same_content_same_cid(self):
        self.write("a", b"same")
        first = builder.add_path(self.root, NO_PUT)[0]
        os.utime(os.path.join(self.root, "a"), ns=(0, 0))
        os.chmod(os.path.join(self.root, "a"), 0o600)
        self.assertEqual(builder.add_path(self.root, NO_PUT)[0], first)

    @unittest.skipUnless(HAS_FIFO and CAN_SYMLINK and POSIX_MODES, "FIFOs, symlinks and non-UTF-8 names (POSIX)")
    def test_skips_symlinks_specials_and_bad_names(self):
        self.write("real", b"r")
        os.symlink("real", os.path.join(self.root, "link"))
        os.symlink(self.tmp.name, os.path.join(self.root, "dirlink"))
        os.mkfifo(os.path.join(self.root, "fifo"))
        with open(os.path.join(os.fsencode(self.root), b"bad\xff"), "wb") as f:
            f.write(b"x")
        blocks = Blocks()
        built, warnings = builder.add_path(self.root, blocks.put)
        self.assertEqual([e.name for e in dag.decode(blocks[built.cid]).entries], ["real"])
        self.assertEqual(len(warnings), 4)
        self.assertTrue(any("symlink" in w and "link" in w for w in warnings))
        self.assertTrue(any("fifo" in w for w in warnings))
        self.assertTrue(any("bad\\xff" in w or "bad\\udcff" in w for w in warnings))

    def test_single_file_and_wrap(self):
        path = self.write("f.bin", b"content", mode=0o700)
        built, _ = builder.add_path(path, NO_PUT)
        self.assertEqual(built.cid, Cid.for_block(RAW, b"content"))
        self.assertEqual(built.kind, EXEC_KIND)
        blocks = Blocks()
        wrapped, _ = builder.add_path(path, blocks.put, wrap_it=True)
        self.assertEqual(dag.decode(blocks[wrapped.cid]).entries, (Entry("f.bin", built.cid, 7, EXEC_KIND),))

    def test_windows_records_every_file_as_kind_0(self):
        # Windows' stat reports an execute bit for .exe, .bat, .cmd and .com files, which add must ignore
        self.write("run.sh", b"#!", mode=0o755)
        self.write("tool.exe", b"MZ")
        with mock.patch.object(fsutil, "IS_WINDOWS", True):
            self.assertFalse(builder.is_executable(0o100755))
            built, _ = builder.add_path(self.root, NO_PUT)
        plain = builder.build_directory([Entry("run.sh", Cid.for_block(RAW, b"#!"), 2, dag.KIND_FILE),
                                         Entry("tool.exe", Cid.for_block(RAW, b"MZ"), 2, dag.KIND_FILE)], NO_PUT)
        self.assertEqual(built.cid, plain.cid)
        self.assertEqual(builder.is_executable(0o100755), POSIX_MODES)

    def test_walk_by_path_without_fd_walk(self):
        # How Windows walks: directories by path, each entry checked by stat, a symlinked root followed
        self.write("a", b"a")
        self.write("sub/b", b"b")
        expected = builder.add_path(self.root, NO_PUT)[0]
        link = os.path.join(self.tmp.name, "root-link")
        with mock.patch.object(builder, "FD_WALK", False):
            self.assertEqual(builder.add_path(self.root, NO_PUT)[0], expected)
            if CAN_SYMLINK:
                os.symlink(self.root, link, target_is_directory=True)
                self.assertEqual(builder.add_path(link, NO_PUT)[0], expected)

    def test_listed_stat_fills_in_windows_inode(self):
        path = self.write("a", b"a")
        real = os.stat(path, follow_symlinks=False)
        # On Windows a DirEntry's stat has st_ino and st_dev zero; opening compares them, so stat the path
        zeroed = os.stat_result((real.st_mode, 0, 0) + tuple(real)[3:])
        entry = mock.Mock(**{"stat.return_value": zeroed})
        self.assertEqual((builder.listed_stat(entry, path).st_ino, builder.listed_stat(entry, path).st_dev),
                         (real.st_ino, real.st_dev))
        entry.stat.return_value = real
        with mock.patch("os.stat", side_effect=AssertionError("stat by path")):
            self.assertIs(builder.listed_stat(entry, path), real)

    def test_junction_is_a_link(self):
        st = os.stat(self.root)
        self.assertFalse(builder.is_link(st))
        junction = mock.Mock(st_mode=st.st_mode, st_reparse_tag=0xA0000003)  # IO_REPARSE_TAG_MOUNT_POINT
        self.assertTrue(builder.is_link(junction))

    def test_large_file_from_path_streams(self):
        data = os.urandom(5 * constants.CHUNK_SIZE + 1)
        path = self.write("big", data)
        self.assertEqual(builder.add_path(path, NO_PUT)[0].cid, builder.build_bytes(data, NO_PUT).cid)

    def test_max_depth(self):
        with mock.patch.object(constants, "MAX_DEPTH", 3):
            os.makedirs(os.path.join(self.root, "a", "b"))
            built, _ = builder.add_path(self.root, NO_PUT)  # root, a, b: 3 DAG nodes deep
            self.assertEqual(built.height, 3)
            with self.assertRaises(TooLarge):
                builder.add_path(self.root, NO_PUT, wrap_it=True)
            os.mkdir(os.path.join(self.root, "a", "b", "c"))
            with self.assertRaises(TooLarge):
                builder.add_path(self.root, NO_PUT)

    def test_max_depth_counts_index_levels(self):
        with mock.patch.multiple(constants, MAX_DEPTH=2, MAX_CHILDREN=2):
            for name in "abc":
                self.write(name, b"")
            with self.assertRaises(TooLarge):  # index + directory group + nothing else: 2; inside a dir: 3
                builder.add_path(self.root, NO_PUT, wrap_it=True)
            self.assertEqual(builder.add_path(self.root, NO_PUT)[0].height, 2)

    @unittest.skipUnless(HAS_FIFO, "FIFOs (POSIX)")
    def test_not_a_file_or_directory(self):
        fifo = os.path.join(self.root, "fifo")
        os.mkfifo(fifo)
        with self.assertRaises(ValueError):
            builder.add_path(fifo, NO_PUT)


    @unittest.skipUnless(HAS_FIFO, "FIFOs (POSIX)")
    def test_fifo_named_directly_never_blocks(self):
        fifo = os.path.join(self.root, "fifo")
        os.mkfifo(fifo)
        with self.assertRaises(ValueError):
            within(5, builder.add_path, fifo, NO_PUT)
        # Swapped in after the type check: opened without blocking, then refused
        regular = self.write("regular", b"r")
        real_stat = os.stat
        with mock.patch("os.stat", side_effect=lambda path, **kw: real_stat(regular if path == fifo else path, **kw)):
            with self.assertRaises(builder.Changed):
                within(5, builder.add_path, fifo, NO_PUT)

    @unittest.skipUnless(CAN_SYMLINK, "creating symlinks isn't allowed here")
    def test_symlink_named_directly_is_followed(self):
        path = self.write("sub/f", b"content")
        os.symlink(path, os.path.join(self.tmp.name, "file-link"))
        os.symlink(os.path.dirname(path), os.path.join(self.tmp.name, "dir-link"), target_is_directory=True)
        self.assertEqual(builder.add_path(os.path.join(self.tmp.name, "file-link"), NO_PUT)[0].cid,
                         Cid.for_block(RAW, b"content"))
        self.assertEqual(builder.add_path(os.path.join(self.tmp.name, "dir-link"), NO_PUT)[0].cid,
                         builder.add_path(os.path.dirname(path), NO_PUT)[0].cid)

    def secret(self) -> tuple[str, bytes]:
        """A file outside the tree being added, which no swap may get into it."""
        data = b"private key"
        path = os.path.join(self.tmp.name, "secret")
        with open(path, "wb") as f:
            f.write(data)
        return path, data

    @unittest.skipUnless(CAN_SYMLINK, "creating symlinks isn't allowed here")
    def test_file_swapped_for_symlink_during_walk_is_skipped(self):
        secret, data = self.secret()
        victim = self.write("sub/victim", b"harmless")
        self.write("sub/other", b"o")

        def swap():
            """Replace the file with a symlink to the secret."""
            os.remove(victim)
            os.symlink(secret, victim)

        blocks = Blocks()
        with swap_on_open("victim", swap):
            built, warnings = builder.add_path(self.root, blocks.put)
        self.assertNotIn(Cid.for_block(RAW, data), blocks)
        self.assertTrue(any("victim" in w and "changed" in w for w in warnings))
        sub = dag.decode(blocks[dag.decode(blocks[built.cid]).entries[0].cid])
        self.assertEqual([e.name for e in sub.entries], ["other"])

    @unittest.skipUnless(CAN_SYMLINK and builder.FD_WALK, "directories opened by descriptor (POSIX), symlinks")
    def test_directory_swapped_for_symlink_during_walk_is_skipped(self):
        secret, data = self.secret()
        outside = os.path.join(self.tmp.name, "outside")
        os.mkdir(outside)
        os.symlink(secret, os.path.join(outside, "key"))
        os.link(secret, os.path.join(outside, "key-copy"))
        victim = os.path.join(self.root, "victim")
        os.mkdir(victim)

        def swap():
            """Replace the directory with a symlink to one outside the tree."""
            os.rmdir(victim)
            os.symlink(outside, victim)

        blocks = Blocks()
        with swap_on_open("victim", swap):
            built, warnings = builder.add_path(self.root, blocks.put)
        self.assertNotIn(Cid.for_block(RAW, data), blocks)
        self.assertEqual(dag.decode(blocks[built.cid]).entries, ())
        self.assertTrue(any("victim" in w and "changed" in w for w in warnings))

    @unittest.skipUnless(HAS_FIFO, "FIFOs (POSIX)")
    def test_fifo_swapped_in_during_walk_never_blocks(self):
        victim = self.write("victim", b"harmless")

        def swap():
            """Replace the file with a FIFO nobody writes to."""
            os.remove(victim)
            os.mkfifo(victim)

        with swap_on_open("victim", swap):
            built, warnings = within(5, builder.add_path, self.root, NO_PUT)
        self.assertEqual(built.cid, builder.build_directory([], NO_PUT).cid)
        self.assertTrue(any("victim" in w and "changed" in w for w in warnings))


if __name__ == "__main__":
    unittest.main()
