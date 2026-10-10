"""Tests for Storage (specification section 8): pins and sources, cached fetches, protection, eviction, GC."""
import os
import tempfile
import threading
import unittest

from rnfs.content import builder, dag, walker
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.dag import Child, Entry, FileNode
from rnfs.content.storage import FOLDER, USER, Storage, check_source, name_source, remote_source
from rnfs.errors import NotFound, StorageFull

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
        with open(os.path.join(self.tmp.name, "state", "pins.json"), "w") as f:
            f.write("{not json")
        self.assertEqual(self.open().pins(), {})
        self.assertTrue(os.path.exists(os.path.join(self.tmp.name, "state", "pins.json.damaged")))

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
        # The LRU order is persisted
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

    def test_check_space(self):
        pinned = self.add(b"P" * 2000)
        self.storage.pin(pinned.cid, USER)
        self.storage.max_storage = 3000
        self.storage.check_space(pinned=False)
        self.storage.max_storage = 2000
        with self.assertRaises(StorageFull):
            self.storage.check_space(pinned=False)
        self.storage.check_space(pinned=True)
        self.assertEqual(self.storage.pinned_bytes(), 2000)

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
        self.assertEqual(self.storage.source_usage(source), 210000)

    def test_stat(self):
        root, a, b = self.tree()
        self.storage._delete({b.cid})
        stat = self.storage.stat(root.cid)
        self.assertEqual(stat["size"], 140000)
        self.assertEqual(stat["blocks"], 5)  # root, a's node and 2 chunks, b's node (its chunks are unknown)
        self.assertEqual(stat["local_blocks"], 4)


if __name__ == "__main__":
    unittest.main()
