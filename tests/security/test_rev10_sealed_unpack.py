"""REV10: unpacking a sealed folder (node_content.unpack_sealed) checked the free space against the plaintext's
bytes, counted twice (its blocks, then its files), and an unpack the process died in left the decrypted blocks in
the block store.

1. Bytes aren't what the disk spends. Every entry costs at least one filesystem allocation unit in the block store
   (a block file per entry) and again when written out (a file or directory), while a tar spends 512 bytes on an
   empty file or directory and 1 KiB on a file of a few bytes. A sender can seal a folder of many tiny distinct
   files (up to max_output_entries) that passes the check and then takes 4 to 8 times what was checked, twice,
   filling the recipient's disk past the reserve. The test seals 300 one-to-three-byte files, gives the disk (by a
   mock of shutil.disk_usage) room for three plaintexts beyond the reserve, which the old check passed, and expects
   the get refused before any block is stored. The tar's headers are now read first (tarstream.scan_tar, only the
   segments holding them decrypted) and each entry counted as max(its size rounded up to the allocation unit, one
   unit) on each side; --no-extract still writes the tar, and with room for the entries it unpacks.
2. The plaintext's blocks are protected by an in-memory session and collected when the unpack ends. A process that
   dies mid-unpack (killed, a power cut, an out-of-memory kill) never collects them: the plaintext of encrypted
   content stays in the block store, unpinned, counted against max_storage, until a gc or an eviction happens to
   run. The test copies B's rnfs dir while the unpack writes out (what a crash at that moment leaves), starts a
   node on the copy and expects none of the plaintext's blocks stored, the sealed file itself still pinned. An
   unpack now marks the state database (meta sealed_unpack/open, durably, before its first block) and clears the
   mark once its blocks are collected; a start that finds the mark collects every block nothing reaches.
"""
import io
import os
import shutil
import tarfile
import unittest
from unittest import mock

from rnfs import config as config_module, fsutil
from rnfs.content import blockstore, tarstream, walker
from rnfs.content.cid import Cid
from rnfs.content.storage import disk_reserve
from rnfs.errors import TooLarge
from tests.test_seal import BOB
from tests.test_sealed_folders import MARK, SealedFolderBase, make_folder
from tests.test_tar_stream import END, Blocks, files_of, member

TINY_FILES = 300


def plaintext_blocks(plain: bytes) -> set[Cid]:
    """The blocks a plaintext tar builds into (as unpack_sealed builds it)."""
    store = Blocks()
    tarstream.build_tar(io.BytesIO(plain), store.put, tarstream.Warnings(), 10_000)
    return set(store.blocks)


class SealedUnpackTest(SealedFolderBase):
    """A seals for B; B gets and unpacks."""

    def seal_tar(self, data: bytes, name: str = "x.tar") -> str:
        """Seal data for Bob on A under name, provided, and pin it on B (stored first: nothing left to fetch)."""
        cid = self.a.add_stream(io.BytesIO(data), to=[BOB.get_public_key().hex()], seal_name=name)["cid"]
        self.advertise(cid)
        self.b.pin_add(cid)
        return cid

    def test_each_entry_counts_its_disk_allocation(self):
        """300 tiny files fit three plaintexts' bytes but not their allocation: refused before any block is stored;
        --no-extract and an unpack with room still work."""
        data = MARK + b"".join(member(f"top/f{i}", str(i).encode()) for i in range(TINY_FILES)) + END
        cid = self.seal_tar(data)
        real = shutil.disk_usage

        def tight(path):
            """Room for three plaintexts beyond the reserve."""
            usage = real(path)
            return usage._replace(free=disk_reserve(usage.total) + 3 * len(data))

        blocks = self.b.storage.block_count()
        out = os.path.join(self.tmp.name, "out")
        with mock.patch("shutil.disk_usage", tight):
            with self.assertRaisesRegex(TooLarge, "won't fit"):
                self.b.get(cid, out)
            self.assertFalse(os.path.lexists(out))
            self.assertEqual(self.b.storage.block_count(), blocks)  # refused before the first block was built
            self.b.get(cid, out, no_extract=True)  # the tar itself: its bytes once
        self.assertTrue(tarfile.is_tarfile(out))
        result = self.b.get(cid, os.path.join(self.tmp.name, "room"))
        self.assertTrue(result["extracted"])
        self.assertEqual(len(files_of(os.path.join(self.tmp.name, "room"))), TINY_FILES)
        for block in plaintext_blocks(data):
            self.assertFalse(self.b.storage.has(block), block)
        # What the count rounds each entry up to: at least 4 KiB, which every filesystem CI runs on allocates
        self.assertGreaterEqual(fsutil.allocation_unit(self.tmp.name), 4096)

    def test_a_start_after_a_crash_mid_unpack_collects_the_plaintext(self):
        """A node started on B's rnfs dir as a crash during the write-out left it holds none of the plaintext's
        blocks, and still the sealed file; after an unpack that ended, nothing is marked."""
        top = make_folder(os.path.join(self.tmp.name, "photos"))
        cid = self.seal_folder(top)
        self.b.pin_add(cid)
        crash = os.path.join(self.tmp.name, "crash")
        original = walker.write_out

        def crashing(*args, **kwargs):
            """Copy B's rnfs dir as the process dying now would leave it, then write out."""
            shutil.copytree(self.b.config.root, crash, ignore=shutil.ignore_patterns(config_module.LOCK_FILE))
            return original(*args, **kwargs)

        with mock.patch.object(walker, "write_out", crashing):
            self.b.get(cid, os.path.join(self.tmp.name, "out"))
        self.assertEqual(files_of(os.path.join(self.tmp.name, "out")), files_of(top))
        self.assertEqual(self.b._db.meta("sealed_unpack", "open", 0), 0)  # ended: its blocks were collected

        plain = plaintext_blocks(self.plaintext(cid))
        left = {block for block in plain if os.path.exists(
            os.path.join(crash, "blocks", blockstore.shard_of(str(block)), str(block)))}
        self.assertTrue(left, "the copy holds the plaintext's blocks, as the crash left them")
        node = self.make_node("crash", bytes([0xD] * 16), BOB)
        for block in plain:
            self.assertFalse(node.storage.has(block), block)
            self.assertFalse(os.path.exists(os.path.join(crash, "blocks", blockstore.shard_of(str(block)),
                                                         str(block))), block)
        self.assertIn(Cid.parse(cid), node.storage.pinned_cids())
        self.assertTrue(node.storage.is_complete(Cid.parse(cid)))
        self.assertEqual(node._db.meta("sealed_unpack", "open", 0), 0)


if __name__ == "__main__":
    unittest.main()
