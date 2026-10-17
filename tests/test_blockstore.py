"""Tests for BlockStore: one verified, atomically written file per CID (specification section 8)."""
import base64
import os
import tempfile
import unittest
from unittest import mock

from rnfs import fsutil
from rnfs.constants import MAX_BLOCK_SIZE
from rnfs.content.blockstore import PARTIAL_PREFIX, BlockStore, matches
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

    @unittest.skipIf(fsutil.IS_WINDOWS, "Windows can't sync a directory")
    def test_put_syncs_the_block_before_renaming_and_sync_the_directory(self):
        # Else a power loss can leave an empty or short file under the CID name, trusted at the next start
        events = []
        real_fsync, real_replace = os.fsync, fsutil.replace
        with mock.patch("os.fsync", side_effect=lambda fd: (events.append("fsync"), real_fsync(fd))[1]), \
                mock.patch.object(fsutil, "replace", lambda *a: (events.append("rename"), real_replace(*a))[1]):
            self.store.put(Cid.for_block(RAW, b"durable"), b"durable")
            self.store.put(Cid.for_block(RAW, b"more"), b"more")
            self.assertEqual(events, ["fsync", "rename"] * 2)
            self.store.sync()  # the directory, once for both
            self.store.sync()  # nothing new since
        self.assertEqual(events, ["fsync", "rename"] * 2 + ["fsync"])

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


    def test_scan_names_blocks_by_their_canonical_cids_only(self):
        raw, node = Cid.for_block(RAW, b"leaf"), Cid.for_block(DAG, b"node")
        self.store.put(raw, b"leaf")
        self.store.put(node, b"node")
        alphabet = "abcdefghijklmnopqrstuvwxyz234567"
        spare_bits_set = str(raw)[:-1] + alphabet[alphabet.index(str(raw)[-1]) + 1]  # parse() refuses it
        dag_pb = "b" + base64.b32encode(bytes([1, 0x70, 0x12, 0x20]) + bytes(32)).decode().lower().rstrip("=")
        for name in (spare_bits_set, dag_pb, str(raw) + "=", "b" + str(node)[1:].upper()):
            open(os.path.join(self.tmp.name, name), "wb").close()
        scanned = sorted(BlockStore(self.tmp.name).scan())
        self.assertEqual(scanned, sorted([(raw, 4), (node, 4)]))
        self.assertEqual(sorted(str(cid) for cid, _ in scanned), sorted([str(raw), str(node)]))
        self.assertEqual({cid.codec for cid, _ in scanned}, {RAW, DAG})


class MatchesTest(unittest.TestCase):
    """matches(): the one block validity check (size limit and hash) that storing, reading and recovery share."""

    def test_raw_and_dag_cids_match_their_blocks(self):
        for codec in (RAW, DAG):
            with self.subTest(codec=codec):
                self.assertTrue(matches(Cid.for_block(codec, b"block"), b"block"))

    def test_wrong_block(self):
        for codec in (RAW, DAG):
            with self.subTest(codec=codec):
                self.assertFalse(matches(Cid.for_block(codec, b"real"), b"fake"))
                self.assertFalse(matches(Cid.for_block(codec, b"real"), b"real "))

    def test_empty_block(self):
        empty = Cid.for_block(RAW, b"")
        self.assertTrue(matches(empty, b""))
        self.assertFalse(matches(empty, b"x"))
        self.assertFalse(matches(Cid.for_block(RAW, b"x"), b""))

    def test_size_limit(self):
        largest = bytes(MAX_BLOCK_SIZE)
        self.assertTrue(matches(Cid.for_block(RAW, largest), largest))
        too_large = bytes(MAX_BLOCK_SIZE + 1)
        cid = Cid.for_block(RAW, too_large)
        with mock.patch.object(Cid, "verify", wraps=cid.verify) as verify:
            self.assertFalse(matches(cid, too_large))
        verify.assert_not_called()  # refused on its size, without hashing it


if __name__ == "__main__":
    unittest.main()
