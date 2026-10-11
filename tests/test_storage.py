"""Tests for Storage (specification section 8): pins and sources, cached fetches, protection, eviction, GC."""
import os
import shutil
import tempfile
import threading
import unittest
from unittest import mock

from rnfs import fsutil
from rnfs.content import builder, dag, storage as storage_module, walker
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.dag import Child, Entry, FileNode
from rnfs.content.storage import FOLDER, USER, Storage, check_source, name_source, remote_source
from rnfs.errors import NotFound, StorageFull
from tests.support import POSIX_MODES

IDENTITY = "0123456789abcdef0123456789abcdef"


class StorageTest(unittest.TestCase):
    """Storage over a temporary directory."""

    def setUp(self):
        """Empty storage with plenty of room."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.storage = self.open()

    def open(self, max_storage: int = 10**9) -> Storage:
        """(Re)open storage over the same directories."""
        return Storage(os.path.join(self.tmp.name, "blocks"), os.path.join(self.tmp.name, "state"), max_storage)

    def add(self, data: bytes) -> builder.Built:
        """Store a file."""
        return builder.build_bytes(data, self.storage.put)

    def tree(self) -> tuple[builder.Built, builder.Built, builder.Built]:
        """A directory {a: file, b: file} of two-chunk files; returns (root, a, b)."""
        a = self.add(b"A" * 70000)
        b = self.add(b"B" * 70000)
        root = builder.build_directory([Entry("a", a.cid, a.size, 0), Entry("b", b.cid, b.size, 0)], self.storage.put)
        return root, a, b

    def blocks_of(self, cid: Cid) -> set[Cid]:
        """Every block of a stored DAG."""
        return {e.cid for e, _ in walker.iter_dag(walker.root_expect(cid), self.storage.getter())}


class BlockTest(StorageTest):
    """Blocks."""

    def test_put_get(self):
        cid = Cid.for_block(RAW, b"abc")
        self.storage.put(cid, b"abc")
        self.storage.put(cid, b"abc")  # idempotent
        self.assertTrue(self.storage.has(cid))
        self.assertEqual(self.storage.get(cid), b"abc")
        self.assertEqual(self.storage.used_bytes(), 3)
        self.assertEqual(self.storage.block_size(cid), 3)
        self.assertEqual(self.open().used_bytes(), 3)

    def test_put_rejects_mismatch(self):
        with self.assertRaises(ValueError):
            self.storage.put(Cid.for_block(RAW, b"abc"), b"abd")
        self.assertEqual(self.storage.used_bytes(), 0)

    def test_getter(self):
        get = self.storage.getter()
        with self.assertRaises(NotFound):
            get(walker.root_expect(Cid.for_block(RAW, b"x")))

    def test_get_verify_removes_damaged_block(self):
        built = self.add(b"abc")
        with open(self.storage.blocks.path(built.cid), "wb") as f:
            f.write(b"xyz")
        self.assertIsNone(self.storage.get(built.cid, verify=True))
        self.assertFalse(self.storage.has(built.cid))

    def test_concurrent_puts(self):
        datas = [os.urandom(100) for _ in range(200)]

        def worker(chunk):
            """Store some blocks."""
            for data in chunk:
                self.storage.put(Cid.for_block(RAW, data), data)

        threads = [threading.Thread(target=worker, args=(datas[i::4],)) for i in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(self.storage.used_bytes(), 20000)
        self.assertEqual(self.storage.block_count(), 200)


class PinTest(StorageTest):
    """Pins and their sources."""

    def test_sources(self):
        cid = self.add(b"x").cid
        self.storage.pin(cid, USER)
        self.storage.pin(cid, name_source("blog"))
        self.storage.pin(cid, remote_source(bytes.fromhex(IDENTITY)))
        self.assertEqual(self.storage.pins(), {cid: {"user", "name:blog", "remote:" + IDENTITY}})
        self.assertTrue(self.storage.unpin(cid, USER))
        self.assertFalse(self.storage.unpin(cid, USER))
        self.assertTrue(self.storage.is_pinned(cid))
        self.assertEqual(self.storage.pinned_by("name:blog"), {cid})
        self.storage.unpin(cid, "name:blog")
        self.storage.unpin(cid, "remote:" + IDENTITY)
        self.assertEqual(self.storage.pins(), {})

    def test_invalid_sources(self):
        for source in ("", "users", "name:", "remote:xyz", "remote:" + IDENTITY.upper(), "remote:" + IDENTITY + "0"):
            with self.subTest(source=source):
                with self.assertRaises(ValueError):
                    check_source(source)
                with self.assertRaises(ValueError):
                    self.storage.pin(Cid.for_block(RAW, b"x"), source)

    def test_persisted(self):
        cid = self.add(b"x").cid
        self.storage.pin(cid, FOLDER)
        self.storage.pin(cid, USER)
        self.assertEqual(self.open().pins(), {cid: {"folder", "user"}})

    def test_damaged_state_is_moved_aside(self):
        with open(os.path.join(self.tmp.name, "state", "pins.json"), "w", encoding="utf-8") as f:
            f.write("{not json")
        self.assertEqual(self.open().pins(), {})
        self.assertTrue(os.path.exists(os.path.join(self.tmp.name, "state", "pins.json.damaged")))

    def test_unmovable_damaged_state_doesnt_stop_startup(self):
        state = os.path.join(self.tmp.name, "state")
        os.mkdir(os.path.join(state, "pins.json"))  # unreadable as a file
        os.makedirs(os.path.join(state, "pins.json.damaged", "in-the-way"))  # and it can't be moved aside
        self.assertEqual(self.open().pins(), {})

    @unittest.skipUnless(POSIX_MODES, "Windows has no POSIX permission bits")
    def test_state_dir_is_private(self):
        self.assertEqual(os.stat(os.path.join(self.tmp.name, "state")).st_mode & 0o777, 0o700)

    def test_is_complete(self):
        root, a, _ = self.tree()
        self.assertTrue(self.storage.is_complete(root.cid))
        self.assertEqual(self.storage.dag_blocks(root.cid), frozenset(self.blocks_of(root.cid)))
        chunk = dag.decode(self.storage.get(a.cid)).children[0].cid
        self.storage.blocks.delete(chunk)  # behind storage's back, then through it
        self.storage._delete({chunk})
        self.assertFalse(self.storage.is_complete(root.cid))
        self.assertTrue(self.storage.is_complete(dag.decode(self.storage.get(root.cid)).entries[1].cid))
        with self.assertRaises(NotFound):
            self.storage.dag_blocks(root.cid)

    def test_invalid_dag_is_not_complete(self):
        chunk = self.add(b"abcd").cid
        block = dag.encode(FileNode([Child(chunk, 5)]))
        cid = Cid.for_block(DAG, block)
        self.storage.put(cid, block)
        self.assertFalse(self.storage.is_complete(cid))


class EvictionTest(StorageTest):
    """GC, eviction and protection."""

    def test_gc_keeps_pins_and_sessions(self):
        pinned = self.add(b"P" * 100000)
        cached = self.add(b"C" * 100000)
        protected = self.add(b"S" * 100000)
        orphan = self.add(b"O" * 10)
        self.storage.pin(pinned.cid, USER)
        self.storage.record_fetch(cached.cid, [], cached.cid)
        with self.storage.protect() as session:
            session.add(protected.cid)
            new = Cid.for_block(RAW, b"new")
            session.put(new, b"new")
            used = self.storage.used_bytes()
            stats = self.storage.gc()
            self.assertTrue(self.storage.has(new))
            self.assertTrue(self.storage.is_complete(protected.cid))
        self.assertTrue(self.storage.is_complete(pinned.cid))
        self.assertFalse(self.storage.has(cached.cid))
        self.assertFalse(self.storage.has(orphan.cid))
        self.assertEqual(stats["removed_blocks"], 4)  # cached: 2 chunks + node; orphan
        self.assertEqual(stats["freed_bytes"], used - self.storage.used_bytes())
        self.assertGreater(stats["freed_bytes"], 100010)
        self.assertEqual(stats["incomplete"], {cached.cid})
        self.assertEqual(stats["uncached"], {cached.cid})
        self.assertEqual(self.storage.cached_fetches(), [])
        # The session has ended, so its blocks are ordinary cache now
        self.storage.gc()
        self.assertFalse(self.storage.has(protected.cid))

    def test_cached_fetch_keeps_path_nodes_and_target(self):
        root, a, b = self.tree()
        self.storage.record_fetch(root.cid, ["a"], a.cid)
        orphan = self.add(b"orphan")
        self.storage.max_storage = self.storage.used_bytes() - 1
        lost = self.storage.evict()
        self.assertTrue(self.storage.has(root.cid))
        self.assertTrue(self.storage.is_complete(a.cid))
        self.assertFalse(self.storage.has(b.cid))  # a sibling of the path, not reached by the record
        self.assertFalse(self.storage.has(orphan.cid))
        self.assertEqual(lost, set())  # a is still complete; b was never a target
        self.assertEqual(self.storage.cached_fetches(), [(root.cid, ("a",), a.cid)])

    def test_lru_eviction(self):
        first = self.add(b"1" * 1000)
        second = self.add(b"2" * 1000)
        third = self.add(b"3" * 1000)
        for built in (first, second, third):
            self.storage.record_fetch(built.cid, [], built.cid)
        self.assertTrue(self.storage.touch(first.cid, []))  # now second is least recently used
        self.storage.max_storage = 2500
        lost = self.storage.evict()
        self.assertEqual(lost, {second.cid})
        self.assertEqual(self.storage.cached_targets(), {first.cid, third.cid})
        self.assertFalse(self.storage.has(second.cid))
        # The LRU order is persisted by save()
        self.storage.save()
        self.assertEqual([t for _, _, t in self.open().cached_fetches()], [third.cid, first.cid])

    def test_under_max_storage_evicts_nothing(self):
        orphan = self.add(b"orphan")
        self.assertEqual(self.storage.evict(), set())
        self.assertTrue(self.storage.has(orphan.cid))  # kept for an interrupted fetch to resume from

    def test_pins_never_evicted(self):
        pinned = self.add(b"P" * 5000)
        cached = self.add(b"C" * 5000)
        self.storage.pin(pinned.cid, USER)
        self.storage.record_fetch(cached.cid, [], cached.cid)
        self.storage.max_storage = 1000
        self.assertEqual(self.storage.evict(), {cached.cid})
        self.assertTrue(self.storage.is_complete(pinned.cid))
        self.assertGreater(self.storage.used_bytes(), self.storage.max_storage)

    def test_shared_blocks_survive(self):
        shared = self.add(b"shared")
        root = builder.build_directory([Entry("s", shared.cid, shared.size, 0)], self.storage.put)
        self.storage.pin(shared.cid, USER)
        self.storage.record_fetch(root.cid, [], root.cid)
        self.storage.gc()
        self.assertTrue(self.storage.has(shared.cid))
        self.assertFalse(self.storage.has(root.cid))

    def test_unpinned_blocks_go_at_gc(self):
        built = self.add(b"x" * 100)
        self.storage.pin(built.cid, USER)
        self.storage.gc()
        self.assertTrue(self.storage.has(built.cid))
        self.storage.unpin(built.cid, USER)
        self.assertTrue(self.storage.has(built.cid))
        stats = self.storage.gc()
        self.assertFalse(self.storage.has(built.cid))
        self.assertEqual(stats["incomplete"], set())  # no longer a pin, so not reported: unpinning withdraws it

    def test_check_disk_keeps_a_reserve_free(self):
        pinned = self.add(b"P" * 2000)
        self.storage.pin(pinned.cid, USER)
        self.assertEqual(self.storage.pinned_bytes(), 2000)
        self.storage.max_storage = 1000  # max_storage doesn't matter here
        gib = 1024 ** 3
        usage = shutil.disk_usage(self.tmp.name)._replace(total=100 * gib, free=3 * gib)
        with mock.patch("shutil.disk_usage", return_value=usage):
            self.storage.check_disk(gib - 1)  # leaves more than the 2 GiB (2%) reserve
            with self.assertRaises(StorageFull) as caught:
                self.storage.check_disk(gib + 1)
            self.assertIn("not enough free disk space", str(caught.exception))
        small = usage._replace(total=gib, free=300 * 1024 * 1024)  # 2% is below DISK_RESERVE_MIN (256 MiB)
        with mock.patch("shutil.disk_usage", return_value=small):
            self.storage.check_disk(40 * 1024 * 1024)
            with self.assertRaises(StorageFull):
                self.storage.check_disk(50 * 1024 * 1024)

    def test_evict_leaves_room_for_what_a_fetch_needs(self):
        old = self.add(b"O" * 1000)
        new = self.add(b"N" * 1000)
        self.storage.record_fetch(old.cid, [], old.cid)
        self.storage.record_fetch(new.cid, [], new.cid)
        self.storage.max_storage = 2500
        self.assertEqual(self.storage.evict(400), set())  # 2000 stored + 400 fit
        self.assertEqual(self.storage.evict(1000), {old.cid})  # least recently used first
        self.assertTrue(self.storage.has(new.cid))
        self.assertLessEqual(self.storage.used_bytes() + 1000, self.storage.max_storage)

    def test_eviction_walks_once_however_many_records_go(self):
        files = [self.add(b"pinned %d" % i) for i in range(500)]
        pinned = builder.build_directory([Entry(f"{i:03d}", f.cid, f.size, 0) for i, f in enumerate(files)],
                                         self.storage.put)  # hundreds of blocks
        self.storage.pin(pinned.cid, USER)
        for i in range(200):
            built = self.add(b"%d." % i * 100)
            self.storage.record_fetch(built.cid, [], built.cid)
        self.storage.max_storage = self.storage.used_bytes() - 10_000  # about half the records must go
        visits = []
        real = Storage._children
        with mock.patch.object(Storage, "_children", lambda s, cid: (visits.append(cid), real(s, cid))[1]):
            self.storage.evict()
        self.assertLess(len(visits), 2000)  # once per block, not once per block per evicted record
        self.assertLessEqual(self.storage.used_bytes(), self.storage.max_storage)
        self.assertTrue(self.storage.is_complete(pinned.cid))
        self.assertGreater(len(self.storage.cached_fetches()), 50)

    def test_eviction_with_nothing_evictable_doesnt_walk_again(self):
        self.storage.max_storage = 1
        with self.storage.protect() as session:
            builder.build_bytes(os.urandom(200 * 1000), session.put)
            self.storage.evict()
            with mock.patch.object(Storage, "_reach", side_effect=AssertionError("walked")):
                self.assertEqual(self.storage.evict(), set())
                builder.build_bytes(b"more", session.put)
                self.assertEqual(self.storage.evict(), set())
        self.storage.evict()  # the session ended: its blocks are garbage now
        self.assertEqual(self.storage.used_bytes(), 0)

    def test_cache_records_saved_in_batches_and_capped(self):
        path = os.path.join(self.tmp.name, "state", "cache.json")
        with mock.patch.object(storage_module, "MAX_CACHE_RECORDS", 3):
            built = [self.add(b"%d" % i) for i in range(5)]
            dropped = set()
            for b in built:
                dropped |= self.storage.record_fetch(b.cid, [], b.cid)
            self.storage.touch(built[2].cid, [])
            self.assertFalse(os.path.exists(path))  # not rewritten on every fetch
            self.assertEqual(dropped, {built[0].cid, built[1].cid})
            self.storage.save()
            self.assertEqual([t for _, _, t in self.open().cached_fetches()], [built[3].cid, built[4].cid, built[2].cid])

    def test_verify(self):
        root, a, b = self.tree()
        self.storage.pin(root.cid, USER)
        self.assertTrue(self.storage.is_complete(root.cid))  # memoised now
        with open(self.storage.blocks.path(b.cid), "wb") as f:
            f.write(b"damaged")
        stats = self.storage.verify()
        self.assertEqual(stats["checked"], 7)
        self.assertEqual(stats["bad"], [b.cid])
        self.assertEqual(stats["incomplete"], {root.cid})
        self.assertFalse(self.storage.is_complete(root.cid))  # the memo was invalidated

    def test_block_in_use_on_windows_is_collected_later(self):
        # On Windows a block a reader holds open can't be deleted: gc keeps it, indexed and counted, for next time
        kept = self.add(b"K" * 10)
        busy = self.add(b"B" * 10)
        self.storage.pin(kept.cid, USER)
        busy_path = self.storage.blocks.path(busy.cid)
        real_remove = os.remove

        def remove(path):
            """os.remove, failing as Windows does for a file another handle has open."""
            if path == busy_path:
                raise PermissionError(13, "The process cannot access the file", path)
            real_remove(path)

        used = self.storage.used_bytes()
        with mock.patch.object(fsutil, "IS_WINDOWS", True), mock.patch.object(fsutil, "RETRY_DELAYS", (0, 0)), \
                mock.patch("os.remove", side_effect=remove) as patched:
            stats = self.storage.gc()
            self.assertEqual(patched.call_count, 3)  # tried, retried twice, then deferred
        self.assertEqual((stats["removed_blocks"], stats["freed_bytes"]), (0, 0))
        self.assertTrue(self.storage.has(busy.cid))
        self.assertEqual(self.storage.used_bytes(), used)
        self.assertEqual(self.storage.used_bytes(), sum(size for _, size in self.storage.blocks.scan()))
        self.assertEqual(self.storage.evict(), set())
        # Once the reader has closed it, the next pass collects it
        stats = self.storage.gc()
        self.assertEqual((stats["removed_blocks"], stats["freed_bytes"]), (1, 10))
        self.assertFalse(self.storage.has(busy.cid))
        self.assertFalse(os.path.exists(busy_path))
        self.assertEqual(self.storage.used_bytes(), used - 10)
        self.assertTrue(self.storage.is_complete(kept.cid))

    def test_eviction_retries_a_deferred_delete(self):
        busy = self.add(b"B" * 10)
        with mock.patch.object(self.storage.blocks, "delete", return_value=None):
            self.storage.gc()
        self.assertTrue(self.storage._maybe_garbage)  # still garbage, so eviction won't skip the walk
        self.storage.max_storage = 1
        self.storage.evict()
        self.assertFalse(self.storage.has(busy.cid))
        self.assertEqual(self.storage.used_bytes(), 0)


class SizeTest(StorageTest):
    """Content sizes, for stat and the remote-pin quota."""

    def test_content_size_and_usage(self):
        root, a, b = self.tree()
        self.assertEqual(self.storage.content_size(root.cid), 140000)
        self.assertEqual(self.storage.content_size(a.cid), 70000)
        self.assertIsNone(self.storage.content_size(Cid.for_block(RAW, b"absent")))
        source = remote_source(IDENTITY)
        self.storage.pin(root.cid, source)
        self.storage.pin(a.cid, source)
        self.storage.pin(Cid.for_block(RAW, b"not yet fetched"), source)
        # The bytes of each DAG's blocks: content plus DAG nodes
        usage = sum(sum(map(self.storage.block_size, self.blocks_of(cid))) for cid in (root.cid, a.cid))
        self.assertGreater(usage, 210000)
        self.assertEqual(self.storage.source_usage(source), usage)

    def test_usage_counts_blocks_of_zero_size_dags(self):
        empty = self.add(b"")
        names = [f"{i:04d}" + "n" * 200 for i in range(300)]
        root = builder.build_directory([Entry(n, empty.cid, 0, 0) for n in names], self.storage.put)
        self.assertEqual(root.size, 0)
        source = remote_source(IDENTITY)
        self.storage.pin(root.cid, source)
        self.assertGreater(self.storage.source_usage(source), 300 * 200)

    def test_stat(self):
        root, a, b = self.tree()
        self.storage._delete({b.cid})
        stat = self.storage.stat(root.cid)
        self.assertEqual(stat["size"], 140000)
        self.assertEqual(stat["blocks"], 5)  # root, a's node and 2 chunks, b's node (its chunks are unknown)
        self.assertEqual(stat["local_blocks"], 4)


if __name__ == "__main__":
    unittest.main()
