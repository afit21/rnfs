"""Tests for sealed folders: `add --to <folder>` packs the folder as one tar and seals it as `<folder>.tar`
(node_content.add_sealed_path, tarstream.folder_chunks), and a recipient's `get` unpacks it (node_content.get_sealed,
unpack_sealed), including what an unpacked tar may not do."""
import contextlib
import io
import os
import shutil
import tarfile
import unittest
from unittest import mock

from rnfs import cli, statedb
from rnfs.api import ApiServer
from rnfs.constants import CHUNK_SIZE, MAX_DEPTH
from rnfs.content import builder, seal, tarstream, walker
from rnfs.content.cid import RAW, Cid
from rnfs.content.seal import SEGMENT_SIZE, NotRecipient
from rnfs.content.storage import disk_reserve
from rnfs.errors import TooLarge
from rnfs.features import jobs

from tests import test_jobs, test_seal
from tests.support import real, state_rows, wait_for
from tests.test_seal import ALICE, BOB, open_all
from tests.test_tar_stream import END, Blocks, files_of, member

MARK = tarstream.folder_marker()  # what makes a sealed .tar a sealed folder that get unpacks


def make_folder(top: str) -> str:
    """A nested folder: files in three levels (a hidden one, an executable, an empty one, one of several segments)
    and an empty directory; top."""
    os.makedirs(os.path.join(top, "sub", "deeper"))
    os.makedirs(os.path.join(top, "empty"))
    for relative, data in (("a.txt", b"hello"), (".hidden", b"dot"), ("run.sh", b"#!/bin/sh\n"),
                           ("sub/big.bin", os.urandom(2 * SEGMENT_SIZE + 5)), ("sub/deeper/nothing", b"")):
        with open(os.path.join(top, relative), "wb") as f:
            f.write(data)
    os.chmod(os.path.join(top, "run.sh"), 0o755)
    return top


class SealedFolderBase(unittest.TestCase):
    """Offline nodes as NodeSealTest's: A adds for B, B fetches over a NetClient, C is no recipient."""

    setUp = test_seal.NodeSealTest.setUp
    make_node = test_seal.NodeSealTest.make_node
    advertise = test_seal.NodeSealTest.advertise

    def seal_folder(self, top: str, **options) -> str:
        """Seal the folder at top for Bob on A, provided; its CID."""
        cid = self.a.add_path(top, to=[BOB.get_public_key().hex()], **options)["cid"]
        self.advertise(cid)
        return cid

    def plaintext(self, cid: str) -> bytes:
        """The decrypted bytes of a sealed file, as any recipient (0.1.x too) reads them: decrypting only."""
        stored = b"".join(self.b.read(cid, raw=True))
        return open_all(stored, BOB)[0]


class AddSealedFolderTest(SealedFolderBase):
    """add --to of a folder."""

    def test_a_recipient_that_only_decrypts_gets_a_tar(self):
        """What a 0.1.x recipient does (get or cat, decrypting) gives a plain tar named <folder>.tar, everything in
        it under <folder>/; the sealed bytes show neither the names nor the structure."""
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        result = self.a.add_path(top, to=[BOB.get_public_key().hex()])
        self.assertEqual((result["sealed"], result["warnings"]), (True, []))
        cid = result["cid"]
        self.advertise(cid)
        info = self.b.info(cid)
        self.assertEqual((info["type"], info["sealed"], info["recipient"], info["name"]),
                         ("file", True, True, "photos.tar"))
        stored = b"".join(self.b.read(cid, raw=True))
        plain, size, name = open_all(stored, BOB)
        self.assertEqual((name, size, info["size"]), ("photos.tar", len(plain), len(plain)))
        self.assertEqual(b"".join(self.b.read(cid)), plain)  # cat decrypts it, unchanged
        for secret in (b"photos", b"a.txt", b"big.bin", b"deeper", b"hello"):
            self.assertNotIn(secret, stored)
        out = os.path.join(self.tmp.name, "extracted")
        os.makedirs(out)
        with tarfile.open(fileobj=io.BytesIO(plain), mode="r:") as archive:
            names = archive.getnames()
            self.assertTrue(all(info.isfile() or info.isdir() for info in archive.getmembers()))
            archive.extractall(out, filter="data")
        self.assertEqual(names[0], "photos")
        self.assertTrue(all(name == "photos" or name.startswith("photos/") for name in names), names)
        self.assertEqual(os.listdir(out), ["photos"])
        self.assertEqual(files_of(os.path.join(out, "photos")), files_of(top))

    def test_skipped_entries_are_warnings(self):
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        if os.name == "nt":
            self.skipTest("no symlinks without privileges")
        os.symlink("/etc/passwd", os.path.join(top, "escape"))
        result = self.a.add_path(top, to_self=True)
        self.assertEqual(len(result["warnings"]), 1)
        self.assertIn("skipped symlink", result["warnings"][0])

    def test_long_folder_names(self):
        """A name of 251 bytes is sealed with .tar (255 bytes); one of 252 is refused, not sealed unnamed."""
        fits = make_folder(os.path.join(self.tmp.name, "é" * 125 + "f"))  # 251 bytes
        cid = self.seal_folder(fits)
        self.assertEqual(self.b.info(cid)["name"], "é" * 125 + "f.tar")
        too_long = os.path.join(self.tmp.name, "é" * 126)  # 252 bytes
        os.makedirs(too_long)
        blocks = self.a.storage.block_count()
        with self.assertRaisesRegex(ValueError, "251"):
            self.a.add_path(too_long, to_self=True)
        self.assertEqual(self.a.storage.block_count(), blocks)  # refused before anything was read or stored

    def test_refusals(self):
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        with self.assertRaisesRegex(ValueError, "no-sealed-name"):
            self.a.add_path(top, to_self=True, seal_name=False)
        with self.assertRaisesRegex(ValueError, "only-hash"):
            self.a.add_path(top, to_self=True, only_hash=True)
        with self.assertRaisesRegex(TooLarge, "max_output_entries"):
            self.a.config.max_output_entries = 5
            self.a.add_path(top, to_self=True)

    def test_wrap_shows_the_sealed_name(self):
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        root = self.a.add_path(top, wrap=True, to_self=True)["cid"]
        self.assertEqual([entry["name"] for entry in self.a.ls(root)], ["photos.tar"])
        self.assertEqual(self.a.info(f"{root}/photos.tar")["name"], "photos.tar")

    def test_sealing_is_randomised_but_the_format_unchanged(self):
        """Two seals of one folder differ (random keys), and each is a version 1 sealed file any reader opens."""
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        first = self.a.add_path(top, to=[BOB.get_public_key().hex()], to_self=True)["cid"]
        second = self.a.add_path(top, to=[BOB.get_public_key().hex()], to_self=True)["cid"]
        self.assertNotEqual(first, second)
        stored = b"".join(self.a.read(first, raw=True))
        self.assertTrue(stored.startswith(seal.MAGIC + bytes([seal.VERSION])))
        self.assertEqual(open_all(stored, ALICE)[0], open_all(stored, BOB)[0])

    def test_add_job(self):
        """An add job (add --background) seals a folder as add does; its progress counts the folder's bytes."""
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        manager = self.a.feature("jobs")
        manager.start()
        job_id = manager.submit("add", path=top, options={"to": [BOB.get_public_key().hex()]})["id"]
        self.assertTrue(wait_for(lambda: manager.get(job_id)["state"] in jobs.FINISHED, 10))
        job = manager.get(job_id)
        self.assertEqual((job["state"], job["result"]["sealed"], job["result"]["warnings"]), ("done", True, []), job)
        size = builder.tree_size(top)
        self.assertEqual((job["total_bytes"], job["done_bytes"]), (size, size))
        self.advertise(job["result"]["cid"])
        self.assertEqual(self.b.info(job["result"]["cid"])["name"], "photos.tar")


class GetSealedFolderTest(SealedFolderBase):
    """get of a sealed folder by a recipient: decrypted, built by build_tar, written out by write_out."""

    def seal_tar(self, data: bytes, name: str = "x.tar") -> str:
        """Seal data (a tar, or not) for Bob on A under name, provided; its CID."""
        cid = self.a.add_stream(io.BytesIO(data), to=[BOB.get_public_key().hex()], seal_name=name)["cid"]
        self.advertise(cid)
        return cid

    def assert_plaintext_gone(self, cid: str):
        """None of the blocks the plaintext's tar builds into is still stored on B (so none is pinned, cached or
        provided), and B's only cached fetch is the sealed file itself."""
        plain = self.plaintext(cid)
        store = Blocks()
        try:
            tarstream.build_tar(io.BytesIO(plain), store.put, tarstream.Warnings(), 10_000)
        except (ValueError, TooLarge):
            store.blocks.update((Cid.for_block(RAW, plain[i:i + CHUNK_SIZE]), b"")
                                for i in range(0, len(plain), CHUNK_SIZE))  # as written: its chunks
        for block in store.blocks:
            self.assertFalse(self.b.storage.has(block), block)
        self.assertEqual(self.b.storage.pinned_cids(), frozenset())
        cached = self.b.storage.cached_targets()
        self.assertIn(Cid.parse(cid), cached)
        self.assertEqual(cached & store.blocks.keys(), set())

    def test_round_trip(self):
        """A nested folder comes back as its top directory (or -o), every file and empty directory, executable
        bits included; the plaintext's blocks are gone afterwards."""
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        cid = self.seal_folder(top)
        out = os.path.join(self.tmp.name, "out")
        result = self.b.get(cid, out)
        self.assertEqual((result["cid"], result["sealed"], result["extracted"], result["name"], result["warnings"]),
                         (cid, True, True, "photos", []))
        self.assertEqual(files_of(out), files_of(top))
        self.assertEqual(sorted(os.listdir(self.tmp.name)), sorted(["a", "b", "c", "photos", "out"]))
        self.assert_plaintext_gone(cid)

    def test_collection_during_the_write_out_takes_nothing(self):
        """The plaintext's blocks are protected while they are written out: a gc meanwhile leaves them."""
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        cid = self.seal_folder(top)
        original = walker.write_out

        def collecting(*args, **kwargs):
            """gc on B, then the write-out."""
            self.b.gc()
            return original(*args, **kwargs)

        with mock.patch.object(walker, "write_out", collecting):
            self.b.get(cid, os.path.join(self.tmp.name, "out"))
        self.assertEqual(files_of(os.path.join(self.tmp.name, "out")), files_of(top))
        self.assert_plaintext_gone(cid)

    def test_no_extract_writes_the_tar(self):
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        cid = self.seal_folder(top)
        out = os.path.join(self.tmp.name, "photos.tar")
        result = self.b.get(cid, out, no_extract=True)
        self.assertEqual((result["sealed"], result["name"], result.get("extracted")), (True, "photos.tar", None))
        with open(out, "rb") as f:
            self.assertEqual(f.read(), self.plaintext(cid))
        self.assert_plaintext_gone(cid)
        raw = os.path.join(self.tmp.name, "raw")
        self.b.get(cid, raw, raw=True)  # --raw is unchanged: the sealed bytes
        with open(raw, "rb") as f:
            self.assertTrue(seal.is_sealed(f.read()))

    def assert_written_as_the_file(self, data: bytes, name: str = "x.tar", warned: bool = False) -> str:
        """Seal data under name, get it on B: written as the file, byte-identical to what was sealed (with a warning
        only if warned); the blocks of anything built meanwhile are gone. Its CID."""
        cid = self.seal_tar(data, name)
        out = os.path.join(self.tmp.name, f"out-{cid[-12:]}")
        result = self.b.get(cid, out)
        self.assertNotIn("extracted", result)
        self.assertEqual(len(result["warnings"]), int(warned), result)
        if warned:
            self.assertIn("isn't a valid tar", result["warnings"][0])
        with open(out, "rb") as f:
            self.assertEqual(f.read(), data)
        self.assert_plaintext_gone(cid)
        return cid

    def test_a_tar_sealed_as_a_file_stays_a_file(self):
        """A valid tar sealed under x.tar without the sealed-folder marker (as `add --to backup.tar` made one with
        0.1.x, or makes one now) is written as the file, byte-identical, with no warning: the normal case."""
        good = member("top/f", b"inside") + END
        cid = self.assert_written_as_the_file(good)
        self.assertFalse(self.b.info(cid)["sealed_folder"])
        self.assert_written_as_the_file(b"".join([member("a", b"1"), member("b", b"2")]) + bytes(20 * 512))
        self.assert_written_as_the_file(b"just some text")

    def test_only_a_first_member_marker_counts(self):
        """The marker must be the tar's first header, holding only its comment: one later in the tar, one with other
        records, or another comment, leaves the tar a file."""
        good = member("top/f", b"inside") + END
        self.assert_written_as_the_file(member("top/f", b"inside") + MARK + END)
        for records in ({"comment": tarstream.FOLDER_MARKER, "path": "x"}, {"comment": "rnfs sealed folder!"},
                        {"path": "rnfs sealed folder"}):
            self.assert_written_as_the_file(tarfile.TarInfo.create_pax_global_header(records) + good)
        cid = self.seal_tar(MARK + good)
        self.assertTrue(self.b.info(cid)["sealed_folder"])
        result = self.b.get(cid, os.path.join(self.tmp.name, "marked"))
        self.assertEqual((result["extracted"], files_of(os.path.join(self.tmp.name, "marked"))),
                         (True, {"f": (b"inside", False)}))

    def test_a_marked_tar_that_isnt_valid_is_written_as_the_file(self):
        """A marked .tar-named sealed file whose plaintext isn't a valid tar (data after its end, cut short,
        garbage) is written as the file, with a warning; whatever was built meanwhile is collected."""
        good = member("top/f", b"inside") + END
        for data in (MARK + good + b"trailing junk", MARK + good[:-700], MARK + os.urandom(3 * SEGMENT_SIZE)):
            self.assert_written_as_the_file(data, warned=True)
            self.assertFalse(self.b.storage.has(Cid.for_block(RAW, b"inside")))  # built before the junk was met
        # Unpacked only for a name ending in .tar, after at least one character
        self.assert_written_as_the_file(MARK + good, "x.tgz")
        self.assert_written_as_the_file(MARK + good, ".tar")

    def test_unsafe_members_are_skipped(self):
        """Traversal, absolute paths, links, devices and FIFOs never reach the disk (build_tar's rules), and names
        the local filesystem would read as a path are skipped by the write-out (get's rules)."""
        members = [member("top/good", b"good"), member("../evil", b"x"), member("/abs", b"x"),
                   member("top/../../evil2", b"x"), member("top/sym", type=tarfile.SYMTYPE, linkname="/etc/passwd"),
                   member("top/hard", type=tarfile.LNKTYPE, linkname="top/good"),
                   member("top/dev", type=tarfile.CHRTYPE, devmajor=1, devminor=3),
                   member("top/fifo", type=tarfile.FIFOTYPE), member("top/sub/deep", b"deep", mode=0o4755)]
        if os.name == "nt":
            members.append(member("top/con", b"x"))
        cid = self.seal_tar(MARK + b"".join(members) + END)
        out = os.path.join(self.tmp.name, "out")
        result = self.b.get(cid, out)
        self.assertEqual((result["extracted"], result["name"]), (True, "top"))
        self.assertEqual(len(result["warnings"]), 7 + (os.name == "nt"), result["warnings"])
        self.assertEqual(files_of(out), {"good": (b"good", False), "sub/": None, "sub/deep": (b"deep", True)})
        self.assertFalse(os.stat(os.path.join(out, "sub", "deep")).st_mode & 0o4000)  # no setuid from a tar
        self.assertEqual(sorted(os.listdir(self.tmp.name)), sorted(["a", "b", "c", "out"]))
        self.assertFalse(os.path.lexists(os.path.join(os.path.dirname(self.tmp.name), "evil")))
        self.assert_plaintext_gone(cid)

    def test_duplicate_entries(self):
        """A path given twice is the later member (tar's own rule, as build_tar has it); a member conflicting with
        what is there as the other type is skipped."""
        data = MARK + b"".join([member("top/f", b"first"), member("top/f", b"second"),
                                member("top/d", type=tarfile.DIRTYPE), member("top/d", b"a file over a directory")]) + END
        out = os.path.join(self.tmp.name, "out")
        result = self.b.get(self.seal_tar(data), out)
        self.assertEqual(len(result["warnings"]), 1)
        self.assertEqual(files_of(out), {"f": (b"second", False), "d/": None})

    def test_a_tar_without_one_top_directory(self):
        """A tar holding several entries (or a file alone) is unpacked whole, as a directory named as the sealed
        name without .tar."""
        out = os.path.join(self.tmp.name, "out")
        result = self.b.get(self.seal_tar(MARK + member("a", b"1") + member("b/c", b"2") + END, "several.tar"), out)
        self.assertEqual((result["extracted"], result["name"]), (True, "several"))
        self.assertEqual(files_of(out), {"a": (b"1", False), "b/": None, "b/c": (b"2", False)})
        result = self.b.get(self.seal_tar(MARK + member("only", b"1") + END, ".hidden.tar"),
                            os.path.join(self.tmp.name, "o"))
        self.assertEqual((result["extracted"], result["name"]), (True, None))  # not a name to write without asking

    def test_limits(self):
        """Too many entries or too deep: refused (pointing at --no-extract), nothing written, the blocks
        collected; --no-extract still writes the tar."""
        deep = MARK + member("/".join(["d"] * (MAX_DEPTH + 1)), b"x") + END
        many = MARK + b"".join(member(f"top/f{i}", b"x") for i in range(6)) + END
        self.b.config.max_output_entries = 5
        for i, data in enumerate((deep, many)):
            cid = self.seal_tar(data)
            out = os.path.join(self.tmp.name, f"out{i}")
            with self.assertRaisesRegex(TooLarge, "--no-extract"):
                self.b.get(cid, out)
            self.assertFalse(os.path.lexists(out))
            self.assertEqual([name for name in os.listdir(self.tmp.name) if name.startswith(".")], [])
            self.assert_plaintext_gone(cid)
            self.b.get(cid, out, no_extract=True)
            with open(out, "rb") as f:
                self.assertEqual(f.read(), data)

    def test_free_space_counts_blocks_and_files(self):
        """Unpacking needs the plaintext's size twice where the blocks and the output share a disk (its blocks, then
        its files); --no-extract needs it once."""
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        cid = self.seal_folder(top)
        self.b.pin_add(cid)  # stored first: nothing left to fetch
        plain = self.b.info(cid)["size"]
        real = shutil.disk_usage

        def tight(path):
            """Room for one and a half plaintexts beyond the reserve."""
            usage = real(path)
            return usage._replace(free=disk_reserve(usage.total) + plain * 3 // 2)

        out = os.path.join(self.tmp.name, "out")
        with mock.patch("shutil.disk_usage", tight):
            with self.assertRaisesRegex(TooLarge, "won't fit"):
                self.b.get(cid, out)
            self.assertFalse(os.path.lexists(out))
            self.b.get(cid, out, no_extract=True)
        self.assertTrue(tarfile.is_tarfile(out))

    def test_cat_and_the_gateway(self):
        """cat gives the decrypted tar, as for any sealed file; the gateway the sealed bytes."""
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        cid = self.seal_folder(top)
        self.assertEqual(b"".join(self.b.read(cid)), self.plaintext(cid))
        self.assertTrue(seal.is_sealed(b"".join(self.b.read(cid, for_gateway=True))))

    def test_non_recipients_get_nothing(self):
        cid = self.seal_folder(make_folder(os.path.join(self.tmp.name, "photos")))
        out = os.path.join(self.tmp.name, "out")
        with self.assertRaises(NotRecipient):
            self.c.get(cid, out)
        self.assertFalse(os.path.lexists(out))

    def test_get_jobs(self):
        """A get job unpacks under the top directory's name (sealed_name), or with no_extract writes the tar,
        persisting no_extract in its record."""
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        cid = self.seal_folder(top)
        manager = self.b.feature("jobs")
        manager.start()
        downloads = os.path.join(self.tmp.name, "downloads")
        os.makedirs(downloads)

        def run(**options) -> dict:
            """Submit a get job of cid into downloads; the finished job."""
            job_id = manager.submit("get", cid, os.path.join(downloads, cid), **options)["id"]
            self.assertTrue(wait_for(lambda: manager.get(job_id)["state"] in jobs.FINISHED, 10))
            return manager.get(job_id)

        job = run(sealed_name=True)
        self.assertEqual((job["state"], job["output"], job["result"]["extracted"]),
                         ("done", os.path.join(downloads, "photos"), True), job)
        self.assertEqual(files_of(job["output"]), files_of(top))
        job = run(sealed_name=True, no_extract=True)
        self.assertEqual((job["state"], job["output"], job["no_extract"]),
                         ("done", os.path.join(downloads, "photos.tar"), True), job)
        self.assertTrue(tarfile.is_tarfile(job["output"]))
        records = [statedb.load(record) for (record,) in state_rows(
            os.path.join(self.tmp.name, "b", "state"), "SELECT record FROM jobs ORDER BY position")]
        self.assertEqual([record.get("no_extract") for record in records], [None, True])
        self.assertEqual(jobs.Job.from_record(records[1]).no_extract, True)
        with self.assertRaises(ValueError):
            manager.submit("pin", cid, no_extract=True)
        with self.assertRaises(ValueError):
            manager.submit("get", cid, os.path.join(downloads, "x"), no_extract="yes")


class SealedFolderCliTest(SealedFolderBase):
    """`rnfs add --to-self <folder>` and `rnfs get [--no-extract]` against B's API."""

    run_cli = test_jobs.CliTest.run_cli

    def setUp(self):
        """B's API (with its job manager), its token where the CLI looks for it, and a downloads folder."""
        super().setUp()
        self.b.feature("jobs").start()
        self.server = ApiServer(self.b, 0, token="secret")
        self.server.start()
        self.addCleanup(self.server.stop)
        self.cli_dir = os.path.join(self.tmp.name, "cli")
        os.makedirs(self.cli_dir)
        cli.write_token(cli.token_path(self.cli_dir), self.server.token)
        self.downloads = os.path.join(self.tmp.name, "downloads")
        os.makedirs(self.downloads)

    def get(self, *argv) -> str:
        """`rnfs get *argv` run in downloads, which must succeed; the output path it prints."""
        with contextlib.chdir(self.downloads):
            code, out, err = self.run_cli("get", *argv)
        self.assertEqual(code, 0, err)
        return out.strip()

    def test_add_and_get(self):
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        code, out, err = self.run_cli("add", top, "--to-self")
        self.assertEqual(code, 0, err)
        cid = out.strip()
        self.assertEqual(real(self.get(cid)), real(os.path.join(self.downloads, "photos")))
        self.assertEqual(files_of(os.path.join(self.downloads, "photos")), files_of(top))
        self.assertEqual(real(self.get(cid, "--no-extract")), real(os.path.join(self.downloads, "photos.tar")))
        self.assertTrue(tarfile.is_tarfile(os.path.join(self.downloads, "photos.tar")))
        self.assertEqual(real(self.get(cid, "-o", "chosen")), real(os.path.join(self.downloads, "chosen")))
        self.assertEqual(files_of(os.path.join(self.downloads, "chosen")), files_of(top))
        code, _, err = self.run_cli("add", top, "--to-self", "--no-sealed-name")
        self.assertEqual(code, 1)
        self.assertIn("no-sealed-name", err)

    def test_a_wrapped_sealed_folder_is_named_by_its_directory(self):
        """get <root>/photos.tar unpacks to photos/, not to a directory called photos.tar."""
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        code, out, err = self.run_cli("add", top, "--to-self", "--wrap")
        self.assertEqual(code, 0, err)
        self.assertEqual(real(self.get(f"{out.strip()}/photos.tar")), real(os.path.join(self.downloads, "photos")))
        self.assertEqual(files_of(os.path.join(self.downloads, "photos")), files_of(top))

    def test_a_tar_sealed_as_a_file_keeps_its_name(self):
        """A real tar file sealed (no marker) comes back as that file, under the name it has, by CID or by path."""
        backup = os.path.join(self.tmp.name, "backup.tar")
        with open(backup, "wb") as f:
            f.write(member("top/f", b"inside") + END)
        code, out, err = self.run_cli("add", backup, "--to-self", "--wrap")
        self.assertEqual(code, 0, err)
        self.assertEqual(real(self.get(f"{out.strip()}/backup.tar")), real(os.path.join(self.downloads, "backup.tar")))
        os.remove(os.path.join(self.downloads, "backup.tar"))
        cid = self.b.add_path(backup, to_self=True)["cid"]
        self.assertEqual(real(self.get(cid)), real(os.path.join(self.downloads, "backup.tar")))
        with open(os.path.join(self.downloads, "backup.tar"), "rb") as f, open(backup, "rb") as g:
            self.assertEqual(f.read(), g.read())

    def test_a_daemon_without_jobs(self):
        """Without the job manager the CLI asks for the sealed name first and drops .tar for a sealed folder it
        unpacks, not for a tar sealed as a file."""
        cid = self.b.add_path(make_folder(os.path.join(self.tmp.name, "photos")), to_self=True)["cid"]
        backup = os.path.join(self.tmp.name, "backup.tar")
        with open(backup, "wb") as f:
            f.write(member("top/f", b"inside") + END)
        tar_cid = self.b.add_path(backup, to_self=True)["cid"]
        with mock.patch.object(jobs, "has_jobs", return_value=False):
            self.assertEqual(real(self.get(cid)), real(os.path.join(self.downloads, "photos")))
            self.assertEqual(real(self.get(cid, "--no-extract")), real(os.path.join(self.downloads, "photos.tar")))
            self.assertEqual(real(self.get(tar_cid)), real(os.path.join(self.downloads, "backup.tar")))
        self.assertTrue(os.path.isdir(os.path.join(self.downloads, "photos")))
        self.assertTrue(tarfile.is_tarfile(os.path.join(self.downloads, "photos.tar")))
        self.assertTrue(os.path.isfile(os.path.join(self.downloads, "backup.tar")))


if __name__ == "__main__":
    unittest.main()
