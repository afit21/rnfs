"""Tests for BlockStore: one verified, atomically written file per CID (specification section 8)."""
import os
import tempfile
import unittest
from unittest import mock

from rnfs import fsutil
from rnfs.constants import MAX_BLOCK_SIZE
from rnfs.content.blockstore import PARTIAL_PREFIX, BlockStore
from rnfs.content.cid import DAG, RAW, Cid

HELLO_WORLD = "bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e"


class BlockStoreTest(unittest.TestCase):
    """Storing, reading and deleting blocks."""

    def setUp(self):
        """An empty store in a temporary directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = BlockStore(self.tmp.name)

    def test_put_stores_under_cid(self):
        cid = Cid.for_block(RAW, b"hello world")
        self.store.put(cid, b"hello world")
        self.assertEqual(os.listdir(self.tmp.name), [HELLO_WORLD])
        self.assertTrue(self.store.has(cid))
        self.assertEqual(self.store.get(cid), b"hello world")
        self.assertEqual(self.store.size(cid), 11)
        self.assertTrue(self.store.verify(cid))
        self.assertEqual(list(self.store.scan()), [(cid, 11)])

    def test_mismatch_is_not_stored(self):
        with self.assertRaises(ValueError):
            self.store.put(Cid.for_block(RAW, b"real"), b"fake")
        with self.assertRaises(ValueError):
            self.store.put(Cid.for_block(DAG, b"x"), b"y")
        self.assertEqual(os.listdir(self.tmp.name), [])

    def test_too_large(self):
        block = bytes(MAX_BLOCK_SIZE + 1)
        with self.assertRaises(ValueError):
            self.store.put(Cid.for_block(RAW, block), block)
        self.assertEqual(os.listdir(self.tmp.name), [])

    def test_missing(self):
        cid = Cid.for_block(RAW, b"absent")
        self.assertFalse(self.store.has(cid))
        self.assertIsNone(self.store.get(cid))
        self.assertIsNone(self.store.size(cid))
        self.assertEqual(self.store.delete(cid), 0)
        self.assertFalse(self.store.verify(cid))

    def test_delete(self):
        cid = Cid.for_block(RAW, b"abc")
        self.store.put(cid, b"abc")
        self.assertEqual(self.store.delete(cid), 3)
        self.assertFalse(self.store.has(cid))

    def test_put_of_a_stored_block_writes_nothing(self):
        # Content-addressed: the bytes are the same, and on Windows replacing a block a reader has open fails
        cid = Cid.for_block(RAW, b"abc")
        self.store.put(cid, b"abc")
        with mock.patch("tempfile.mkstemp", side_effect=AssertionError("rewritten")), \
                mock.patch("os.replace", side_effect=AssertionError("replaced")):
            self.store.put(cid, b"abc")
        self.assertEqual(self.store.get(cid), b"abc")

    def test_put_racing_another_put_on_windows(self):
        # A concurrent put stored the block first and a reader holds it, so the replace is refused: that's fine
        cid = Cid.for_block(RAW, b"abc")

        def replace(src, dst):
            """Store the block as the other put would, then refuse like Windows."""
            with open(dst, "wb") as f:
                f.write(b"abc")
            raise PermissionError(13, "Access is denied", dst)

        with mock.patch.object(fsutil, "IS_WINDOWS", True), mock.patch.object(fsutil, "RETRY_DELAYS", ()), \
                mock.patch("os.replace", side_effect=replace):
            self.store.put(cid, b"abc")
        self.assertEqual(os.listdir(self.tmp.name), [str(cid)])  # no partial file left
        self.assertEqual(self.store.get(cid), b"abc")

    def test_delete_in_use_on_windows_is_deferred(self):
        cid = Cid.for_block(RAW, b"abc")
        self.store.put(cid, b"abc")
        with mock.patch.object(fsutil, "IS_WINDOWS", True), mock.patch.object(fsutil, "RETRY_DELAYS", (0,)), \
                mock.patch("os.remove", side_effect=PermissionError(13, "in use")):
            self.assertIsNone(self.store.delete(cid))
        self.assertTrue(self.store.has(cid))
        self.assertEqual(self.store.delete(cid), 3)

    def test_verify_detects_damage(self):
        cid = Cid.for_block(RAW, b"abc")
        self.store.put(cid, b"abc")
        with open(self.store.path(cid), "wb") as f:
            f.write(b"abd")
        self.assertFalse(self.store.verify(cid))

    def test_ignores_foreign_files_and_deletes_partial_files(self):
        open(os.path.join(self.tmp.name, "notes.txt"), "wb").close()
        open(os.path.join(self.tmp.name, HELLO_WORLD.upper()), "wb").close()
        open(os.path.join(self.tmp.name, PARTIAL_PREFIX + "x"), "wb").close()
        store = BlockStore(self.tmp.name)
        self.assertEqual(list(store.scan()), [])
        self.assertEqual(sorted(os.listdir(self.tmp.name)), sorted(["notes.txt", HELLO_WORLD.upper()]))


if __name__ == "__main__":
    unittest.main()
