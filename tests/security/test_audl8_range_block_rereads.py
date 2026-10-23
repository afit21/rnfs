"""AUD-L8 (0.2.1 audit): a /range with a tiny max_bytes had the node read and hash the whole block for each answer.

The attack: the node checks a block against its CID every time it serves it (NodeBackend.served_block, through
Storage.get(verify=True)), so a damaged block is never sent. A /range answer carries only max_bytes of the block, and
fits one packet if that is small, so a requester asking for one byte at a time (or the same byte again and again) had
the node read and hash up to MAX_BLOCK_SIZE (128 KiB) per tiny answer, about as much work as the RNS side of the
exchange, many times over for one block.

The test serves one 128 KiB block through NodeBackend over a real Storage, counting reads and hashes, and asks for it
50 times; then checks the block isn't served once deleted, and that what is remembered is bounded.

The fix (Storage.get(remember=True), used by NodeBackend.served_block): the last VERIFIED_MEMORY (8) blocks served are
remembered as verified, in memory, and served again from there unread and unhashed while they stay stored. Storage's
_delete forgets them with the block, under the same lock as the check that the block is stored, so a deleted block is
never served from memory. Each is kept with its file's signature (inode, size, modification time), checked with one
stat per answer, so a file rewritten or replaced since (damaged on disk) is read and checked again, and not served.
"""
import os
import tempfile
import types
import unittest
from unittest import mock

from rnfs.constants import MAX_BLOCK_SIZE
from rnfs.content import blockstore, storage as storage_module
from rnfs.content.cid import RAW, Cid
from rnfs.content.storage import Storage
from rnfs.node import NodeBackend


def block(seed: int) -> tuple[Cid, bytes]:
    """A full-size raw block, and its CID."""
    data = bytes([seed]) * MAX_BLOCK_SIZE
    return Cid.for_block(RAW, data), data


class BlockRereadTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.storage = Storage(os.path.join(self.tmp.name, "blocks"), os.path.join(self.tmp.name, "state"), 10**9)
        self.addCleanup(self.storage.close)
        node = types.SimpleNamespace(deny=types.SimpleNamespace(denies=lambda cid: False),
                                     publisher=types.SimpleNamespace(serves=lambda cid: True),
                                     storage=self.storage, refresh_soon=lambda: None)
        self.backend = NodeBackend(node)
        self.hashes = mock.Mock(wraps=blockstore.matches)
        patcher = mock.patch.object(storage_module.blockstore, "matches", self.hashes)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_block_served_again_is_not_read_and_hashed_again(self):
        cid, data = block(1)
        self.storage.put(cid, data)
        self.hashes.reset_mock()
        reads = mock.Mock(wraps=self.storage.blocks.get)
        with mock.patch.object(self.storage.blocks, "get", reads):
            for _ in range(50):
                self.assertEqual(self.backend.served_block(cid), data)
        self.assertLessEqual(self.hashes.call_count, 1, "the whole block was hashed again for each answer")
        self.assertLessEqual(reads.call_count, 1, "the whole block was read again for each answer")

    def test_a_deleted_block_is_not_served_from_memory(self):
        cid, data = block(2)
        self.storage.put(cid, data)
        self.assertEqual(self.backend.served_block(cid), data)
        self.assertEqual(self.storage.gc()["removed_blocks"], 1)  # unpinned: collected
        self.assertIsNone(self.backend.served_block(cid))

    def test_a_block_damaged_on_disk_since_is_checked_again(self):
        # As tests/test_e2e.py's failover does: a block served, then overwritten on disk, isn't served (from memory)
        cid, data = block(3)
        self.storage.put(cid, data)
        self.assertEqual(self.backend.served_block(cid), data)
        with open(self.storage.blocks.path(cid), "wb") as f:
            f.write(b"damaged on disk")
        self.assertIsNone(self.backend.served_block(cid))
        self.assertFalse(self.storage.has(cid))

    def test_memory_is_bounded(self):
        for seed in range(storage_module.VERIFIED_MEMORY * 3):
            cid, data = block(seed)
            self.storage.put(cid, data)
            self.assertEqual(self.backend.served_block(cid), data)
        self.assertEqual(len(self.storage._verified), storage_module.VERIFIED_MEMORY)


if __name__ == "__main__":
    unittest.main()
