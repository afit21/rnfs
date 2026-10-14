"""NEW-5: evict() and gc() walked DAGs and read blocks from disk while holding the storage lock.

The first eviction after a restart read and hashed every DAG node under Storage._lock, so /blocks serving
(served_block -> Storage.get) waited behind it (~0.4 s with 20k blocks in the reviewer's run). The tests count the
block reads made while the evicting thread holds the lock, on a storage just reopened (nothing memoised), and
check that what the walk outside the lock finds can't make eviction delete a block that became reachable
meanwhile: a pin and a protection session added from inside that walk are kept.
"""
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.content import builder
from rnfs.content.dag import KIND_FILE, Entry
from rnfs.content.storage import USER, Storage

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence storage's log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class LockedWalkTest(unittest.TestCase):
    """Directories of small files, half pinned and half cached fetches (some through a path), reopened cold."""

    def setUp(self):
        """12 directories of 12 files; storage reopened so nothing is memoised, with room for about half."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.blocks = os.path.join(self.tmp.name, "blocks")
        self.state = os.path.join(self.tmp.name, "state")
        storage = Storage(self.blocks, self.state, 10**12)
        self.roots = []
        for d in range(12):
            entries = []
            for f in range(12):
                built = builder.build_bytes(os.urandom(64) + bytes([d, f]), storage.put)
                entries.append(Entry(f"f{f:02d}", built.cid, built.size, KIND_FILE))
            self.roots.append(builder.build_directory(entries, storage.put).cid)
        for i, root in enumerate(self.roots):
            if i % 2:
                storage.pin(root, USER)
            elif i % 4:
                storage.record_fetch(root, [], root)
            else:
                file = self.entry(storage, root, "f00")
                storage.record_fetch(root, ["f00"], file)
                storage.record_fetch(root, [], root)
        storage.save()
        self.used = storage.used_bytes()
        self.storage = Storage(self.blocks, self.state, self.used // 2)

    @staticmethod
    def entry(storage: Storage, root, name: str):
        """The CID of root's entry name."""
        from rnfs.content import dag
        return next(e.cid for e in dag.decode(storage.get(root)).entries if e.name == name)

    def reads(self, operation) -> tuple[int, int]:
        """(block reads with the storage lock held, reads without it) while operation runs."""
        counts = [0, 0]
        real = self.storage.blocks.get

        def get(cid):
            """BlockStore.get, counted."""
            counts[0 if self.storage._lock._is_owned() else 1] += 1
            return real(cid)

        with mock.patch.object(self.storage.blocks, "get", get):
            operation()
        return counts[0], counts[1]

    def test_cold_eviction_reads_no_block_under_the_lock(self):
        locked, unlocked = self.reads(self.storage.evict)
        self.assertLessEqual(self.storage.used_bytes(), self.storage.max_storage)
        # NEW-5: fails until eviction reads the DAG nodes it walks without holding the storage lock
        self.assertEqual(locked, 0, f"{locked} block reads with the storage lock held ({unlocked} without)")

    def test_cold_gc_reads_no_block_under_the_lock(self):
        locked, unlocked = self.reads(self.storage.gc)
        for i, root in enumerate(self.roots):
            self.assertEqual(self.storage.is_complete(root), bool(i % 2))
        # NEW-5: fails until gc reads the DAG nodes it walks without holding the storage lock
        self.assertEqual(locked, 0, f"{locked} block reads with the storage lock held ({unlocked} without)")

    def test_what_becomes_reachable_during_the_walk_is_kept(self):
        """A cached root pinned, and a new block protected by a session, while eviction reads without the lock."""
        cached = self.roots[2]  # an evictable cached fetch, among the least recently used
        real = self.storage.blocks.get
        fired = []
        with self.storage.protect() as session:
            def get(cid):
                """BlockStore.get; the first read made without the lock pins and stores, as other threads could."""
                if not fired and not self.storage._lock._is_owned():
                    fired.append(cid)
                    self.storage.pin(cached, USER)
                    session.put(*block)
                return real(cid)

            block = (builder.build_bytes(b"fetched meanwhile", lambda cid, data: None).cid, b"fetched meanwhile")
            with mock.patch.object(self.storage.blocks, "get", get):
                self.storage.evict()
            # NEW-5: fails until eviction walks without the lock (on the old code nothing reads without it)
            self.assertTrue(fired, "no block was read without the storage lock")
            self.assertTrue(self.storage.is_complete(cached), "a DAG pinned during the walk lost blocks")
            self.assertTrue(self.storage.has(block[0]), "a block protected during the walk was deleted")
            self.storage.gc()  # the session still protects its block; the new pin keeps its DAG
            self.assertTrue(self.storage.has(block[0]))
        self.assertTrue(self.storage.is_complete(cached))


if __name__ == "__main__":
    unittest.main()
