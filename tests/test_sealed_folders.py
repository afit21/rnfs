"""Tests for sealed folders: `add --to <folder>` packs the folder as one tar and seals it as `<folder>.tar`
(node_content.add_sealed_path, tarstream.folder_chunks)."""
import io
import os
import tarfile
import unittest

from rnfs.content import builder, seal
from rnfs.content.seal import SEGMENT_SIZE
from rnfs.errors import TooLarge
from rnfs.features import jobs

from tests.support import wait_for
from tests import test_seal
from tests.test_seal import ALICE, BOB, open_all
from tests.test_tar_stream import files_of


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


if __name__ == "__main__":
    unittest.main()
