"""Tests for directories as tar streams (rnfs/content/tarstream.py), their API routes (POST /api/add/stream?format=tar,
GET /api/get/stream, GET /api/export/stream, POST /api/import/stream) and their ApiConnection methods."""
import io
import os
import shutil
import stat
import subprocess
import tarfile
import tempfile
import unittest

from rnfs import cli, log
from rnfs.apiclient import ApiConnection, BadRequest, NotFound, TooLarge
from rnfs.constants import CHUNK_SIZE, MAX_DEPTH
from rnfs.content import builder, dag, tarstream, walker
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.dag import Entry
from rnfs.content.storage import USER
from rnfs.errors import TooLarge as NodeTooLarge
from tests.test_jobs import ApiBase


class Blocks:
    """A dict of blocks with a put callback and a get for walker."""

    def __init__(self):
        """No blocks."""
        self.blocks: dict[Cid, bytes] = {}

    def put(self, cid: Cid, block: bytes):
        """Store a block."""
        self.blocks[cid] = block

    def get(self, expect: walker.Expect) -> bytes:
        """The block expect names."""
        return self.blocks[expect.cid]


def tree(top: str) -> str:
    """Make a directory of files (one executable, one large, one empty), an empty directory and a symlink; top."""
    os.makedirs(os.path.join(top, "sub", "empty"))
    for relative, data in (("a.txt", b"hello"), ("sub/big.bin", os.urandom(2 * CHUNK_SIZE + 3)), ("run.sh", b"#!"),
                           ("sub/nothing", b""), ("ünï cödé", b"name")):
        with open(os.path.join(top, relative), "wb") as f:
            f.write(data)
    os.chmod(os.path.join(top, "run.sh"), 0o755)
    if os.name != "nt":
        os.symlink("a.txt", os.path.join(top, "link"))
    return top


def tar_of(top: str, arcname: str = ".") -> bytes:
    """A plain tar of top's contents, as `tar -C top -c .` makes it."""
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w", format=tarfile.PAX_FORMAT) as archive:
        archive.add(top, arcname=arcname)
    return out.getvalue()


def member(name: str, data: bytes = b"", **attributes) -> bytes:
    """One member's header and data, written by tarfile (PAX format), with attributes set on its TarInfo."""
    info = tarfile.TarInfo(name)
    info.size = len(data)
    for key, value in attributes.items():
        setattr(info, key, value)
    return info.tobuf(tarfile.PAX_FORMAT, "utf-8", "surrogateescape") + data + bytes(-len(data) % 512)


def raw_header(name: str, kind: bytes, size: int, fmt=tarfile.GNU_FORMAT) -> bytes:
    """A bare header block of type kind announcing size bytes (no extension headers of its own)."""
    info = tarfile.TarInfo(name)
    info.type = kind
    info.size = size
    return info.tobuf(fmt, "utf-8", "strict")


END = bytes(1024)


def files_of(top: str) -> dict:
    """{relative path: (bytes, executable)} of the regular files below top, and {path/: None} of its directories.

    Paths are written with "/" on every OS. Windows keeps no execute bit (st_mode reports one only by file
    extension), so an expected True there is support.POSIX_MODES."""
    found = {}
    for directory, dirs, names in os.walk(top):
        for name in dirs:
            found[os.path.relpath(os.path.join(directory, name), top).replace(os.sep, "/") + "/"] = None
        for name in names:
            path = os.path.join(directory, name)
            if os.path.islink(path):
                continue
            with open(path, "rb") as f:
                found[os.path.relpath(path, top).replace(os.sep, "/")] = (f.read(),
                                                                          bool(os.stat(path).st_mode & stat.S_IXUSR))
    return found


class BuildTarTest(unittest.TestCase):
    """build_tar: a tar stream into a directory DAG, as add builds one."""

    def setUp(self):
        """A temporary directory and an empty block store."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.store = Blocks()

    def build(self, data: bytes, max_entries: int = 1000, **kwargs) -> tuple[builder.Built, dict]:
        """(the DAG built from data, its warnings' result)."""
        warnings = tarstream.Warnings()
        built = tarstream.build_tar(io.BytesIO(data), self.store.put, warnings, max_entries, **kwargs)
        return built, warnings.result()

    def test_same_cid_as_add(self):
        top = tree(os.path.join(self.tmp, "top"))
        added, _ = builder.add_path(top, self.store.put)
        built, warnings = self.build(tar_of(top))
        self.assertEqual(built.cid, added.cid)
        self.assertEqual(warnings["warnings"], ["skipped symlink ./link"] if os.name != "nt" else [])
        wrapped, _ = self.build(tar_of(top), wrap="top")
        self.assertEqual(wrapped.cid, builder.add_path(top, self.store.put, wrap_it=True)[0].cid)
        # Paths as `tar -c top` writes them, without ./: the same tree under its own name
        self.assertEqual(self.build(tar_of(top, "top"))[0].cid, wrapped.cid)

    def test_unsafe_and_unsupported_members_are_skipped(self):
        evil = b"".join([
            member("/etc/passwd", b"root"), member("../escape", b"x"), member("a/../../b", b"x"),
            member("ok/../../c", b"x"), member("link", linkname="/etc", type=tarfile.SYMTYPE),
            member("hard", linkname="ok.txt", type=tarfile.LNKTYPE), member("dev", type=tarfile.CHRTYPE),
            member("fifo", type=tarfile.FIFOTYPE), member("bad\udcff", b"x"), member("x" * 256, b"x"),
            member("ok.txt", b"fine", mode=0o644), member("./tool", b"#!", mode=0o700)]) + END
        built, warnings = self.build(evil)
        entries = {entry.name: entry for entry in walker.list_directory(walker.root_expect(built.cid), self.store.get)}
        self.assertEqual(sorted(entries), ["ok.txt", "tool"])
        self.assertEqual(entries["tool"].kind, dag.KIND_EXECUTABLE)
        self.assertEqual(entries["ok.txt"].kind, dag.KIND_FILE)
        self.assertEqual(len(warnings["warnings"]), 10, warnings)
        self.assertIn("skipped /etc/passwd: an absolute path", warnings["warnings"])
        self.assertIn("skipped ../escape: a path with a .. component", warnings["warnings"])
        self.assertIn("skipped symlink link", warnings["warnings"])
        self.assertIn("skipped hard link hard", warnings["warnings"])
        self.assertIn("skipped bad\\udcff: name is not valid UTF-8 or breaks the name rules", warnings["warnings"])

    def test_conflicting_and_repeated_paths(self):
        data = b"".join([member("f", b"first"), member("f", b"second"), member("f/inside", b"x"),
                         member("d", type=tarfile.DIRTYPE), member("d", b"file over a directory"),
                         member("e/deep/file", b"implied directories")]) + END
        built, warnings = self.build(data)
        root = walker.root_expect(built.cid)
        f = walker.step_name(root, "f", self.store.get)
        self.assertEqual(b"".join(walker.read_range(f, self.store.get)), b"second")  # the last one wins, as in tar
        self.assertEqual(walker.kind_of(walker.step_name(root, "d", self.store.get), self.store.get(
            walker.step_name(root, "d", self.store.get))), walker.DIRECTORY_TYPE)
        self.assertEqual(len(warnings["warnings"]), 2, warnings)
        self.assertIsNotNone(walker.resolve(built.cid, ["e", "deep", "file"], self.store.get))

    def test_entry_and_depth_limits(self):
        many = b"".join(member(f"f{i}", b"x") for i in range(11)) + END
        self.build(many, max_entries=11)
        with self.assertRaises(NodeTooLarge):
            self.build(many, max_entries=10)
        with self.assertRaises(NodeTooLarge):  # implied directories count too
            self.build(member("a/b/c/d/e", b"x") + END, max_entries=4)
        with self.assertRaises(NodeTooLarge):
            self.build(member("/".join(["d"] * (MAX_DEPTH + 1)), b"x") + END)

    def test_warnings_are_capped(self):
        data = b"".join(member(f"l{i}", linkname="x", type=tarfile.SYMTYPE) for i in range(300)) + END
        _, warnings = self.build(data)
        self.assertEqual(len(warnings["warnings"]), log.MAX_WARNINGS + 1)
        self.assertEqual(warnings["warnings"][-1], "... and 200 more warnings")
        self.assertEqual(warnings["warnings_total"], 300)

    def test_unbounded_headers_are_refused(self):
        long_link = raw_header("././@LongLink", tarfile.GNUTYPE_LONGNAME, 10) + b"long-name\0" + bytes(502)
        sparse_10 = member("s", b"", pax_headers={"GNU.sparse.major": "1", "GNU.sparse.minor": "0",
                                                  "GNU.sparse.name": "s", "GNU.sparse.realsize": "10"})
        for name, data, reason in (
                ("a huge pax header", raw_header("h", tarfile.XHDTYPE, 2 * 1024 * 1024, tarfile.USTAR_FORMAT)
                 + bytes(4096), "extended header of 2097152 bytes"),
                ("a chain of long names", long_link * (tarstream.MAX_EXTENSIONS + 1) + member("f", b"x") + END,
                 "extended headers in a row"),
                ("a pax header over 64 KiB", raw_header("h", tarfile.XHDTYPE, 100 * 1024, tarfile.USTAR_FORMAT)
                 + bytes(100 * 1024) + END, "extended header of 102400 bytes"),
                ("one large global header", raw_header("g", tarfile.XGLTYPE, 5000, tarfile.USTAR_FORMAT)
                 + bytes(5120) + END, "global pax headers of more than"),
                ("many small global headers", (raw_header("g", tarfile.XGLTYPE, 11, tarfile.USTAR_FORMAT)
                                               + b"11 a=bcdef\n" + bytes(501) + member("f", b"x")) * 400 + END,
                 "global pax headers of more than"),
                ("an old GNU sparse file", raw_header("s", tarfile.GNUTYPE_SPARSE, 0) + END, "sparse"),
                ("a pax sparse file", sparse_10 + bytes(512) + END, "sparse")):
            with self.subTest(name), self.assertRaisesRegex(ValueError, reason):
                self.build(data)
        # Long names, as many in a row as allowed, and a global header as git archive writes one, are fine
        built, _ = self.build(long_link * tarstream.MAX_EXTENSIONS + member("f", b"x") + END)
        self.assertEqual([entry.name for entry in walker.list_directory(walker.root_expect(built.cid),
                                                                        self.store.get)], ["long-name"])
        record = b"52 comment=0123456789abcdef0123456789abcdef01234567\n"
        git = raw_header("pax_global_header", tarfile.XGLTYPE, len(record), tarfile.USTAR_FORMAT) + record
        self.build(git + bytes(-len(record) % 512) + member("f", b"x") + END)

    def test_warnings_show_a_bounded_name(self):
        """A member's name may be 64 KiB (and six times that once its invalid bytes are escaped): a warning shows
        MAX_SHOWN characters of it."""
        data = member("\udcff" * 20000, b"x") + member("x" * 30000, linkname="y", type=tarfile.SYMTYPE) + END
        _, warnings = self.build(data)
        self.assertEqual(len(warnings["warnings"]), 2)
        for warning in warnings["warnings"]:
            self.assertLess(len(warning), tarstream.MAX_SHOWN + 80, warning[:100])

    def test_tarfile_still_has_the_internals_overridden(self):
        """The header class overrides these tarfile methods; a Python renaming one must be noticed."""
        for name in tarstream.TARFILE_INTERNALS:
            self.assertTrue(callable(getattr(tarfile.TarInfo, name, None)), name)

    def test_damaged_or_cut_streams_are_errors(self):
        good = member("a", b"x" * 1000) + member("b", b"y") + END
        garbage = member("a", b"x") + b"A" * 512 + member("b", b"y") + END
        for name, data in (("garbage between members", garbage), ("cut in a file", good[:900]),
                           ("cut in a header", good[:1536 + 1024 + 100]), ("empty", b""), ("not a tar", b"x" * 2048)):
            with self.subTest(name), self.assertRaises(ValueError):
                self.build(data)
        # A tar without its end blocks (ended at a header boundary) is accepted, as tar accepts it, and so is one
        # padded with zeros to its record
        self.assertEqual(len(self.build(member("a", b"x"))[1]["warnings"]), 0)
        self.build(member("a", b"x") + bytes(tarstream.RECORD * 3))

    def test_a_zeroed_block_in_the_middle_is_refused(self):
        """tarfile stops at the first zero block: a header zeroed by damage would end the tar there, quietly."""
        whole = member("a", b"1") + member("b", b"2") + member("c", b"3") + END
        damaged = whole[:1024] + bytes(512) + whole[1536:]
        with self.assertRaisesRegex(ValueError, "data after its end"):
            self.build(damaged)
        with self.assertRaisesRegex(ValueError, "data after its end"):
            self.build(whole + b"trailing")
        stream = io.BytesIO(whole + bytes(20000))
        tarstream.build_tar(stream, self.store.put, tarstream.Warnings(), 1000)
        self.assertEqual(stream.read(), b"")  # read to its end


class WriteTarTest(unittest.TestCase):
    """plan, length and chunks: a DAG into a tar."""

    def setUp(self):
        """A temporary directory and an empty block store."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.store = Blocks()

    def tar(self, cid: Cid, name: str = "name", max_entries: int = 1000) -> tuple[bytes, list[str]]:
        """(the tar of cid's DAG, the paths skipped), checking its length is the one announced."""
        skipped = tarstream.Warnings()
        members = tarstream.plan(walker.root_expect(cid), self.store.get, name, max_entries, skipped)
        data = b"".join(tarstream.chunks(members, self.store.get, 1_700_000_000, max_entries))
        self.assertEqual(len(data), tarstream.length(members, 1_700_000_000))
        self.assertEqual(len(data) % tarstream.RECORD, 0)
        return data, skipped.kept

    def extract(self, data: bytes, where: str) -> str:
        """Extract data with tarfile's data filter (which refuses unsafe members) into where."""
        os.makedirs(where)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
            for info in archive.getmembers():
                self.assertFalse(info.name.startswith("/") or ".." in info.name.split("/"), info.name)
            archive.extractall(where, filter="data")
        return where

    def test_round_trip(self):
        top = tree(os.path.join(self.tmp, "top"))
        added, _ = builder.add_path(top, self.store.put)
        data, skipped = self.tar(added.cid)
        self.assertEqual(skipped, [])
        out = self.extract(data, os.path.join(self.tmp, "out"))
        self.assertEqual(files_of(out), files_of(top))
        warnings = tarstream.Warnings()
        self.assertEqual(tarstream.build_tar(io.BytesIO(data), self.store.put, warnings, 1000).cid, added.cid)

    def test_a_file_is_one_member(self):
        built = builder.build_bytes(b"content", self.store.put, dag.KIND_EXECUTABLE)
        root = builder.wrap("tool", built, self.store.put)
        expect = walker.step_name(walker.root_expect(root.cid), "tool", self.store.get)
        members = tarstream.plan(expect, self.store.get, "tool", 10, tarstream.Warnings())
        self.assertEqual([(m.path, m.kind, m.size) for m in members], [("tool", dag.KIND_EXECUTABLE, 7)])
        data = b"".join(tarstream.chunks(members, self.store.get, 0, 10))
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            info = archive.getmember("tool")
            self.assertEqual((info.mode, archive.extractfile(info).read()), (0o755, b"content"))

    def test_names_unsafe_in_a_tar_are_skipped(self):
        """Another node's DAG may name entries with characters some extractors read as separators, or as a Windows
        drive (c:, C:evil): skipped. A colon elsewhere is an ordinary character."""
        x = builder.build_bytes(b"x", self.store.put)
        inner = builder.build_directory([Entry("f", x.cid, 1, dag.KIND_FILE), Entry("c:", x.cid, 1, dag.KIND_FILE)],
                                        self.store.put)
        root = builder.build_directory([Entry("..\\..\\evil", inner.cid, inner.size, dag.KIND_DIRECTORY),
                                        Entry("sub", inner.cid, inner.size, dag.KIND_DIRECTORY),
                                        Entry("..＼full", x.cid, 1, dag.KIND_FILE),
                                        Entry("C:evil", x.cid, 1, dag.KIND_FILE),
                                        Entry("ok", builder.build_bytes(b"y", self.store.put).cid, 1, dag.KIND_FILE)],
                                       self.store.put)
        data, skipped = self.tar(root.cid)
        self.assertEqual(sorted(skipped), sorted(["..\\..\\evil", "..＼full", "C:evil", "sub/c:"]))
        with tarfile.open(fileobj=io.BytesIO(data)) as archive:
            self.assertEqual(archive.getnames(), ["ok", "sub", "sub/f"])

    def test_skipped_paths_are_counted_past_the_limit(self):
        """Every skipped entry is counted, but only the first few paths are kept (and each is cut short)."""
        x = builder.build_bytes(b"x", self.store.put)
        level = builder.build_directory([Entry(f"bad\\{i}", x.cid, 1, dag.KIND_FILE) for i in range(50)],
                                        self.store.put)
        for _ in range(3):
            level = builder.build_directory([Entry("d" * 250, level.cid, level.size, dag.KIND_DIRECTORY)],
                                            self.store.put)
        skipped = tarstream.Warnings(5)
        members = tarstream.plan(walker.root_expect(level.cid), self.store.get, "x", 1000, skipped)
        self.assertEqual((len(members), skipped.total, len(skipped.kept)), (3, 50, 5))
        self.assertTrue(all(len(path) <= tarstream.MAX_SHOWN + 3 for path in skipped.kept))
        self.assertEqual(members[-1].path, "/".join(["d" * 250] * 3))

    def test_small_chunks_are_counted_before_anything_is_sent(self):
        """A file of many tiny chunks is refused by plan (before a response's headers), not midway through chunks."""
        chunks = []
        for i in range(200):
            data = bytes([i])
            chunks.append(dag.Child(Cid.for_block(RAW, data), 1))
            self.store.put(chunks[-1].cid, data)
        node = dag.FileNode(chunks)
        block = dag.encode(node)
        file_cid = Cid.for_block(DAG, block)
        self.store.put(file_cid, block)
        root = builder.build_directory([Entry("f", file_cid, 200, dag.KIND_FILE)], self.store.put)
        with self.assertRaisesRegex(NodeTooLarge, "small chunks"):
            tarstream.plan(walker.root_expect(root.cid), self.store.get, "x", 100, tarstream.Warnings())
        self.assertEqual(len(self.tar(root.cid, max_entries=198)[0]) % tarstream.RECORD, 0)

    def test_long_and_repeated_names(self):
        file = builder.build_bytes(b"same", self.store.put)
        level = builder.build_directory([Entry("n" * 255, file.cid, 4, dag.KIND_FILE)], self.store.put)
        for _ in range(5):  # a path far beyond ustar's 255 bytes: a pax header carries it
            level = builder.build_directory([Entry("d" * 200, level.cid, level.size, dag.KIND_DIRECTORY),
                                             Entry("again", level.cid, level.size, dag.KIND_DIRECTORY)], self.store.put)
        data, _ = self.tar(level.cid, max_entries=200)
        # Checked in memory, not extracted: the deepest path (~1300 bytes) is past macOS's PATH_MAX of 1024, and from
        # Python 3.13 tarfile's data filter resolves the whole path too, so it can't judge it there. What it would
        # refuse is checked by hand: an absolute path, "..", a link, anything but a file or a directory
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
            files = []
            for info in archive.getmembers():
                parts = info.name.split("/")
                self.assertFalse(info.name.startswith("/") or ".." in parts or "" in parts, info.name)
                self.assertTrue(info.isfile() or info.isdir(), info.name)
                if info.isfile():
                    self.assertEqual(archive.extractfile(info).read(), b"same")
                    files.append(info.name)
        self.assertEqual(len(files), 32)
        self.assertGreater(max(map(len, files)), 1024)  # past ustar's 255 bytes and macOS's PATH_MAX alike
        self.assertTrue(all(name.endswith("/" + "n" * 255) for name in files))
        with self.assertRaises(NodeTooLarge):
            self.tar(level.cid, max_entries=50)


class FolderTarTest(unittest.TestCase):
    """folder_chunks: a folder on this machine into a tar under one top directory (a sealed folder's plaintext)."""

    def setUp(self):
        """A temporary directory and an empty block store."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.store = Blocks()

    def pack(self, path: str, top: str = "top", max_entries: int = 1000, **kwargs) -> tuple[bytes, list[str]]:
        """(the tar of the folder at path, its warnings)."""
        warnings: list[str] = []
        handle, _ = builder.open_path(path)
        try:
            data = b"".join(tarstream.folder_chunks(handle, path, top, max_entries, warnings.append, **kwargs))
        finally:
            builder.close(handle)
        self.assertEqual(len(data) % tarstream.RECORD, 0)
        return data, warnings

    def test_everything_is_under_one_top_directory(self):
        """Every member is below top/ (so `tar xf` spills nothing), each directory before its contents, and the tar
        builds into what `add --wrap` of the folder builds."""
        top = tree(os.path.join(self.tmp, "folder"))
        data, warnings = self.pack(top, "folder", mtime=1_700_000_000)
        self.assertEqual(warnings, [f"skipped symlink {os.path.join(top, 'link')}"] if os.name != "nt" else [])
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
            names = archive.getnames()
            infos = archive.getmembers()
        self.assertEqual(names[0], "folder")
        self.assertTrue(all(name == "folder" or name.startswith("folder/") for name in names), names)
        for info in infos:
            self.assertTrue(info.isfile() or info.isdir(), info.name)
            self.assertEqual((info.uid, info.gid, info.uname, info.gname, info.mtime), (0, 0, "", "", 1_700_000_000))
        seen: set[str] = set()
        for name in names:  # each directory before what it holds
            self.assertIn(name.rpartition("/")[0] or "folder", seen | {"folder"})
            seen.add(name)
        out = os.path.join(self.tmp, "out")
        os.makedirs(out)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
            archive.extractall(out, filter="data")
        self.assertEqual(os.listdir(out), ["folder"])
        self.assertEqual(files_of(os.path.join(out, "folder")), files_of(top))
        built = tarstream.build_tar(io.BytesIO(data), self.store.put, tarstream.Warnings(), 1000)
        self.assertEqual(built.cid, builder.add_path(top, self.store.put, wrap_it=True)[0].cid)

    def test_the_tar_starts_with_the_sealed_folder_marker(self):
        """The first header is the marker (a pax global header of one comment), which tarfile, GNU tar and
        build_tar take without a warning, an extra member or an extra file."""
        top = tree(os.path.join(self.tmp, "folder"))
        data, _ = self.pack(top, "folder")
        self.assertTrue(data.startswith(tarstream.folder_marker()))
        self.assertTrue(tarstream.is_marked(data[:tarstream.MARKER_PEEK]))
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
            self.assertEqual(archive.getnames()[0], "folder")
            self.assertEqual(archive.pax_headers, {"comment": tarstream.FOLDER_MARKER})
        warnings = tarstream.Warnings()
        tarstream.build_tar(io.BytesIO(data), self.store.put, warnings, 1000)  # within MAX_GLOBAL_BYTES
        self.assertEqual(warnings.total, 0)
        tar = shutil.which("tar")
        if tar is not None and os.name != "nt":
            path = os.path.join(self.tmp, "folder.tar")
            with open(path, "wb") as f:
                f.write(data)
            out = os.path.join(self.tmp, "gnu")
            os.makedirs(out)
            run = subprocess.run([tar, "xf", path], cwd=out, capture_output=True, text=True, check=False)
            self.assertEqual((run.returncode, run.stderr, os.listdir(out)), (0, "", ["folder"]))
            self.assertEqual(files_of(os.path.join(out, "folder")), files_of(top))

    def test_is_marked(self):
        marker = tarstream.folder_marker()
        good = member("f", b"x") + END
        self.assertTrue(tarstream.is_marked((marker + good)[:tarstream.MARKER_PEEK]))
        for head in (good, member("f", b"x") + marker + END, marker[:520], b"", bytes(2048),
                     tarfile.TarInfo.create_pax_global_header({"comment": tarstream.FOLDER_MARKER, "a": "b"}) + good,
                     tarfile.TarInfo.create_pax_global_header({"comment": "other"}) + good,
                     member("x", b"30 comment=rnfs sealed folder\n", type=tarfile.XHDTYPE) + good):
            self.assertFalse(tarstream.is_marked(head[:tarstream.MARKER_PEEK]), head[:600])

    def test_skipped_entries(self):
        """add's skips (invalid names, links, FIFOs), and names a tar shouldn't hold, are warnings; a file with other
        hard links is an ordinary file (never a hard-link member, which an unpacker would drop)."""
        top = os.path.join(self.tmp, "top")
        os.makedirs(top)
        # Windows reads "back\\slash" as a path and "C:drive" as a stream of a file C, so they exist only elsewhere
        for name in ("plain",) + (("back\\slash", "C:drive") if os.name != "nt" else ()):
            with open(os.path.join(top, name), "wb") as f:
                f.write(name.encode())
        bad_name = False
        if os.name != "nt":
            os.link(os.path.join(top, "plain"), os.path.join(top, "linked"))
            os.mkfifo(os.path.join(top, "fifo"))  # noqa: TID251 - not on Windows (os.name checked above)
            os.symlink("/etc/passwd", os.path.join(top, "escape"))
            try:
                with open(os.path.join(top.encode(), b"bad\xff"), "wb") as f:
                    f.write(b"x")
                bad_name = True
            except OSError:
                pass  # macOS (APFS) refuses names that aren't UTF-8 itself, so there is nothing to skip
        data, warnings = self.pack(top)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
            members = {info.name: info for info in archive.getmembers()}
            if os.name != "nt":
                self.assertEqual(archive.extractfile(members["top/linked"]).read(), b"plain")
        self.assertTrue(all(info.isfile() or info.isdir() for info in members.values()))
        expected = {"top", "top/plain"} | ({"top/linked"} if os.name != "nt" else set())
        self.assertEqual(set(members), expected)
        self.assertEqual(len(warnings), 4 + bad_name if os.name != "nt" else 0, warnings)
        if os.name != "nt":
            self.assertTrue(any("back\\slash" in text and "separator" in text for text in warnings), warnings)

    def test_top_names_that_cant_be_used_are_refused(self):
        top = os.path.join(self.tmp, "top")
        os.makedirs(top)
        for name in ("..", "a/b", "back\\slash", "C:x", "n" * 256):
            with self.assertRaises(ValueError, msg=name):
                self.pack(top, name)

    def test_entry_limit(self):
        top = os.path.join(self.tmp, "top")
        os.makedirs(os.path.join(top, "sub"))
        for i in range(3):
            with open(os.path.join(top, "sub", f"f{i}"), "wb") as f:
                f.write(b"x")
        data, _ = self.pack(top, max_entries=5)  # top, sub and three files
        tarstream.build_tar(io.BytesIO(data), self.store.put, tarstream.Warnings(), 5)
        with self.assertRaisesRegex(NodeTooLarge, "max_output_entries"):
            self.pack(top, max_entries=4)

    def test_heights_match_the_builder(self):
        """fixed_height and directory_height give what builder builds, so the packer's depth check is exact."""
        for size in (0, 1, CHUNK_SIZE, CHUNK_SIZE + 1, 256 * CHUNK_SIZE, 256 * CHUNK_SIZE + 1):
            built = builder.build_file(io.BytesIO(bytes(size)), builder.NO_PUT, size)
            self.assertEqual(tarstream.fixed_height(size), built.height, size)
        file = builder.build_bytes(b"x", builder.NO_PUT)
        for count in (0, 1, 256, 257, 256 * 256 + 1):
            for child_height in (0, 3):
                entries = [Entry(f"{i:06}", file.cid, 1, dag.KIND_FILE) for i in range(count)]
                built = builder.build_directory(entries, builder.NO_PUT, child_height)
                self.assertEqual(tarstream.directory_height(count, child_height), built.height, (count, child_height))

    def test_depth_limit_is_what_add_wrap_allows(self):
        """A folder as deep as `add --wrap` takes packs; one level more, or a file whose own nodes make the tree too
        deep, is refused, as add --wrap refuses it."""
        top = os.path.join(self.tmp, "top")
        deepest = os.path.join(top, *["d"] * (MAX_DEPTH - 2))  # top's DAG: MAX_DEPTH - 1 directory levels
        os.makedirs(deepest)
        data, _ = self.pack(top)
        built = tarstream.build_tar(io.BytesIO(data), self.store.put, tarstream.Warnings(), 1000)
        self.assertEqual(built.cid, builder.add_path(top, self.store.put, wrap_it=True)[0].cid)
        with open(os.path.join(deepest, "big"), "wb") as f:
            f.write(bytes(CHUNK_SIZE + 1))  # a file node: one level more
        with self.assertRaises(NodeTooLarge):
            builder.add_path(top, self.store.put, wrap_it=True)
        with self.assertRaises(NodeTooLarge):
            self.pack(top)
        os.remove(os.path.join(deepest, "big"))
        os.mkdir(os.path.join(deepest, "d"))
        with self.assertRaises(NodeTooLarge):
            self.pack(top)

    def test_a_file_that_shrinks_is_an_error(self):
        """Its header already gave its size, so a shorter file can't be written as a valid member."""
        top = os.path.join(self.tmp, "top")
        os.makedirs(top)
        path = os.path.join(top, "shrinking")
        with open(path, "wb") as f:
            f.write(bytes(3 * tarstream.COPY_SIZE))
        handle, _ = builder.open_path(top)
        self.addCleanup(builder.close, handle)
        chunks = tarstream.folder_chunks(handle, top, "top", 10, lambda text: None)
        next(chunks)  # top's header
        next(chunks)  # the file's header
        next(chunks)  # its first read
        os.truncate(path, 10)
        with self.assertRaisesRegex(ValueError, "got shorter"):
            list(chunks)

    def test_progress_counts_the_bytes_read(self):
        top = tree(os.path.join(self.tmp, "top"))
        read: list[int] = []
        self.pack(top, progress=read.append)
        self.assertEqual(sum(read), builder.tree_size(top))
        self.assertIn(0, read)  # every entry met is a cancel check

    def test_iterator_reader(self):
        chunks = [b"abc", b"", b"defgh", b"i"]
        reader = tarstream.IteratorReader(iter(chunks))
        self.assertEqual([reader.read(2), reader.read(4), reader.read(0), reader.read(), reader.read(5)],
                         [b"ab", b"cdef", b"", b"ghi", b""])
        closed = []

        def generate():
            """Yields once, noting when it is closed."""
            try:
                yield b"x"
            finally:
                closed.append(True)

        reader = tarstream.IteratorReader(generate())
        self.assertEqual(reader.read(1), b"x")
        reader.close()
        self.assertEqual(closed, [True])


class TarApiTest(ApiBase):
    """The streaming routes on B, an offline node, through ApiConnection."""

    def setUp(self):
        """B's API and a client of it."""
        super().setUp()
        rnfs_dir = os.path.join(self.tmp.name, "client")
        os.makedirs(rnfs_dir)
        cli.write_token(cli.token_path(rnfs_dir), self.server.token)
        self.api = ApiConnection(rnfs_dir, port=self.server.port)

    def test_tar_round_trip(self):
        top = tree(self.out("top"))
        expected = self.b.add_path(top, only_hash=True)["cid"]
        data = tar_of(top)
        result = self.api.add_tar(data)
        self.assertEqual(result["cid"], expected)
        self.assertIn(USER, self.b.storage.pins()[Cid.parse(expected)])
        chunked = self.api.add_tar(io.BytesIO(data), name="top", pin=False)  # no length: sent chunked
        self.assertEqual(chunked["cid"], self.b.add_path(top, wrap=True, only_hash=True)["cid"])
        with self.api.get_tar(expected) as stream:
            downloaded = stream.read()
            self.assertEqual(len(downloaded), stream.length)
        with tarfile.open(fileobj=io.BytesIO(downloaded)) as archive:
            archive.extractall(self.out("back"), filter="data")
        self.assertEqual(files_of(self.out("back")), files_of(top))
        self.assertEqual(self.api.add_tar(downloaded, only_hash=True)["cid"], expected)
        with self.api.get_tar(f"{expected}/a.txt") as stream:
            with tarfile.open(fileobj=io.BytesIO(stream.read())) as archive:
                self.assertEqual(archive.extractfile("a.txt").read(), b"hello")

    def test_tar_refusals(self):
        data = tar_of(tree(self.out("top")))
        for params in ({"format": "zip"}, {"format": "tar", "to_self": 1}, {"format": "tar", "seal_name": "x"}):
            with self.subTest(params), self.assertRaises(BadRequest):
                self.api.call("POST", "/api/add/stream", params=params, body=data, length=len(data))
        with self.assertRaises(BadRequest):
            self.api.add_tar(b"not a tar" * 100)
        cid = self.api.add_tar(data)["cid"]
        with self.assertRaises(BadRequest):
            self.api.request("GET", "/api/get/stream", params={"target": cid, "format": "zip"})
        with self.assertRaises(NotFound):
            self.api.get_tar(f"{cid}/missing")
        self.b.config.max_output_entries = 3
        with self.assertRaises(TooLarge):
            self.api.get_tar(cid)
        with self.assertRaises(TooLarge):
            self.api.add_tar(data)

    def test_export_and_import_streams(self):
        cid = self.b.add_path(tree(self.out("top")))["cid"]
        self.b.export(cid, self.out("archive"))
        with open(self.out("archive"), "rb") as f:
            exported = f.read()
        with self.api.export_stream(cid) as stream:
            self.assertEqual(stream.read(), exported)  # byte-identical to rnfs export
        self.b.pin_rm(cid)
        self.b.gc()
        self.assertFalse(self.b.storage.has(Cid.parse(cid)))
        result = self.api.import_stream(io.BytesIO(exported))
        self.assertEqual(result, self.b.import_archive(self.out("archive")))
        self.assertEqual(result["roots"], [cid])
        self.assertIn(USER, self.b.storage.pins()[Cid.parse(cid)])
        with self.assertRaises(BadRequest):
            self.api.import_stream(b"not an archive")
        with self.assertRaises(NotFound):
            self.api.export_stream(str(Cid.for_block(RAW, b"nobody has this")))


if __name__ == "__main__":
    unittest.main()
