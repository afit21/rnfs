"""Tests for Storage (specification section 8): pins and sources, cached fetches, protection, eviction, GC."""
import os
import random
import shutil
import tempfile
import threading
import unittest
from unittest import mock

from rnfs import constants, fsutil, statedb
from rnfs.content import builder, dag, storage as storage_module, walker
from rnfs.content.blockstore import PARTIAL_PREFIX, BlockStore
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.dag import Child, Entry, FileNode
from rnfs.content.storage import (FOLDER, USER, Storage, app_of, app_source, check_app_name, check_source, name_source,
                                  remote_source)
from rnfs.errors import Closed, NotFound, StorageFull
from tests.support import PERMISSION_CHECKS, POSIX_MODES, failing_commits, state_execute, state_rows, write_legacy

IDENTITY = "0123456789abcdef0123456789abcdef"


class StorageTest(unittest.TestCase):
    """Storage over a temporary directory."""

    def setUp(self):
        """Empty storage with plenty of room."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.storage = self.open()

    def open(self, max_storage: int = 10**9) -> Storage:
        """(Re)open storage over the same directories (closed before the directory is removed)."""
        storage = Storage(os.path.join(self.tmp.name, "blocks"), os.path.join(self.tmp.name, "state"), max_storage)
        self.addCleanup(storage.close)
        return storage

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

    def test_put_of_a_stored_block_still_verifies(self):
        # Archive import counts what put accepts as supplied, so junk for a block already held must fail too
        cid = Cid.for_block(RAW, b"abc")
        self.storage.put(cid, b"abc")
        with self.assertRaises(ValueError):
            self.storage.put(cid, b"junk")
        with self.storage.protect() as session, self.assertRaises(ValueError):
            session.put(cid, b"junk")
        self.assertEqual(self.storage.get(cid), b"abc")
        self.assertEqual(self.storage.used_bytes(), 3)

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


class BlockSizesTest(StorageTest):
    """The block_sizes table: loaded at a start after a clean stop, instead of listing every block."""

    def rows(self) -> dict[Cid, int]:
        """The block_sizes table as committed."""
        return {Cid.from_bytes(binary): size for binary, size in state_rows(self.state, "SELECT cid, size FROM "
                                                                                       "block_sizes")}

    def clean(self):
        """The clean mark as committed (None if never set)."""
        rows = state_rows(self.state, "SELECT value FROM meta WHERE store = 'block_sizes' AND key = 'clean'")
        return rows[0][0] if rows else None

    def setUp(self):
        """As StorageTest, with a few blocks stored."""
        super().setUp()
        self.state = os.path.join(self.tmp.name, "state")
        self.blocks = {}
        for n in range(5):
            data = b"block %d" % n * (n + 1)
            self.blocks[Cid.for_block(RAW, data)] = data
            self.storage.put(Cid.for_block(RAW, data), data)

    def sizes(self, storage: Storage) -> dict[Cid, int]:
        """What storage holds: CID -> size."""
        return {cid: storage.block_size(cid) for cid in self.blocks if storage.has(cid)}

    def test_loaded_after_a_clean_stop(self):
        self.assertEqual(self.clean(), 0)  # a running store's table isn't trusted
        self.storage.close()
        self.assertEqual(self.clean(), 1)
        self.assertEqual(self.rows(), {cid: len(data) for cid, data in self.blocks.items()})
        with mock.patch.object(BlockStore, "scan", side_effect=AssertionError("listed every block")):
            storage = self.open()
        self.assertEqual(self.sizes(storage), {cid: len(data) for cid, data in self.blocks.items()})
        self.assertEqual(storage.used_bytes(), sum(map(len, self.blocks.values())))
        self.assertEqual(self.clean(), 0)  # cleared at once, until this one stops cleanly
        storage.close()
        self.assertEqual(self.clean(), 1)

    def test_rows_follow_blocks_stored_and_deleted(self):
        # Written whenever pins or cache records are saved, so a clean stop has little left to write
        pinned = self.add(b"pinned").cid
        self.storage.pin(pinned, USER)
        self.assertEqual(set(self.rows()), set(self.blocks) | {pinned})
        self.storage.gc()  # the other blocks are cache that nothing reaches
        self.storage.save()
        self.assertEqual(self.rows(), {pinned: 6})
        self.storage.close()
        self.assertEqual(self.sizes(self.open()), {})
        self.assertEqual(self.rows(), {pinned: 6})

    def test_an_unclean_stop_lists_every_block(self):
        # The process ended without close(): the table may miss blocks stored since, or list deleted ones
        self.storage.save()
        extra = Cid.for_block(RAW, b"stored after the last save")
        self.storage.put(extra, b"stored after the last save")
        gone = next(iter(self.blocks))
        os.remove(self.storage.blocks.path(gone))  # as a delete since the last save would
        partial = os.path.join(os.path.dirname(self.storage.blocks.path(extra)), PARTIAL_PREFIX + "x")
        open(partial, "wb").close()
        with mock.patch("rnfs.content.storage.log") as logged:
            storage = self.open()  # the first is still open, so its stop was never recorded clean
        self.assertIn("didn't stop cleanly", logged.call_args_list[0].args[0])
        expected = {cid: len(data) for cid, data in self.blocks.items() if cid != gone}
        self.assertEqual(self.sizes(storage), expected)
        self.assertTrue(storage.has(extra))
        self.assertEqual(self.rows(), {**expected, extra: len(b"stored after the last save")})
        self.assertFalse(os.path.exists(partial))  # a full listing deletes partial files a crash left

    def test_a_missing_file_counts_as_a_damaged_block(self):
        cid = next(iter(self.blocks))
        self.storage.close()
        os.remove(self.storage.blocks.path(cid))
        storage = self.open()
        self.assertTrue(storage.has(cid))  # the table says so, until it is read
        self.assertIsNone(storage.get(cid))
        self.assertFalse(storage.has(cid))
        self.assertEqual(storage.used_bytes(), sum(len(data) for other, data in self.blocks.items() if other != cid))
        storage.close()
        self.assertNotIn(cid, self.rows())

    def test_flat_blocks_are_indexed_after_a_clean_stop(self):
        # An older rnfs run since the clean stop stored a block directly in blocks/
        self.storage.close()
        data = b"stored by rnfs 0.1.6"
        cid = Cid.for_block(RAW, data)
        with open(os.path.join(self.tmp.name, "blocks", str(cid)), "wb") as f:
            f.write(data)
        storage = self.open()
        self.assertEqual(storage.get(cid), data)
        self.assertEqual(storage.block_size(cid), len(data))
        storage.close()
        self.assertEqual(self.rows()[cid], len(data))

    def test_an_invalid_row_lists_every_block(self):
        self.storage.close()
        state_execute(self.state, "INSERT INTO block_sizes (cid, size) VALUES (?, ?)", (b"\x01\x02", 3))
        storage = self.open()
        self.assertEqual(self.sizes(storage), {cid: len(data) for cid, data in self.blocks.items()})
        self.assertNotIn(b"\x01\x02", [binary for binary, in state_rows(self.state, "SELECT cid FROM block_sizes")])

    def test_a_put_still_writing_at_close_isnt_a_clean_stop(self):
        started, release = threading.Event(), threading.Event()
        real_put = BlockStore.put

        def slow_put(store, cid, block):
            """Write the block once the test lets it."""
            started.set()
            release.wait(10)
            real_put(store, cid, block)

        with mock.patch.object(BlockStore, "put", slow_put):
            writer = threading.Thread(target=self.storage.put, args=(Cid.for_block(RAW, b"late"), b"late"))
            writer.start()
            self.assertTrue(started.wait(10))
            self.storage.close()
            release.set()
            writer.join()
        self.assertEqual(self.clean(), 0)
        storage = self.open()
        self.assertTrue(storage.has(Cid.for_block(RAW, b"late")))  # found by listing every block

    def test_nothing_is_stored_or_deleted_after_close(self):
        self.storage.close()
        with self.assertRaises(Closed):
            self.storage.put(Cid.for_block(RAW, b"late"), b"late")
        with self.storage._lock:
            self.assertEqual(self.storage._delete(set(self.blocks)), (set(), 0))
        for cid in self.blocks:
            self.assertTrue(os.path.exists(self.storage.blocks.path(cid)))

    def test_a_partial_file_left_isnt_a_clean_stop(self):
        # On Windows one may be in use: only a full listing at the next start finds it again
        cid = Cid.for_block(RAW, b"new")
        with mock.patch.object(fsutil, "replace", side_effect=OSError(5, "I/O error")), \
                mock.patch.object(fsutil, "remove", return_value=False):
            with self.assertRaises(OSError):
                self.storage.put(cid, b"new")
        self.storage.close()
        self.assertEqual(self.clean(), 0)
        for name in os.listdir(os.path.dirname(self.storage.blocks.path(cid))):
            if name.startswith(PARTIAL_PREFIX):
                break
        else:
            self.fail("no partial file left")
        self.open()
        self.assertEqual([name for name in os.listdir(os.path.dirname(self.storage.blocks.path(cid)))
                          if name.startswith(PARTIAL_PREFIX)], [])

    def test_a_failed_save_writes_the_rows_next_time(self):
        cid = self.add(b"x").cid
        with failing_commits():
            with self.assertRaises(OSError):
                self.storage.pin(cid, USER)
        self.assertNotIn(cid, self.rows())
        self.storage.save()
        self.assertIn(cid, self.rows())


class WriterTest(StorageTest):
    """Session.writer(): blocks stored on several threads, all of them before the with block ends."""

    def test_every_block_is_stored_and_protected_when_it_ends(self):
        data = os.urandom(40 * 65536 + 5)
        with self.storage.protect() as session:
            with session.writer() as put:
                built = builder.build_bytes(data, put)
            self.assertTrue(self.storage.is_complete(built.cid))
            self.assertEqual(self.storage.gc()["removed_blocks"], 0)  # protected while the session lasts
            self.storage.pin(built.cid, USER)
        self.assertEqual(built.cid, builder.build_bytes(data, builder.NO_PUT).cid)
        self.assertEqual(self.storage.used_bytes(), sum(self.storage.blocks.size(c) for c in self.blocks_of(built.cid)))
        reopened = self.open()
        self.assertTrue(reopened.is_complete(built.cid))

    def test_writes_overlap(self):
        # Each put waits for its sync to disk; one after another they take as long as all of those together
        writing, most, started = 0, 0, 0
        lock = threading.Lock()
        real = self.storage.blocks.put
        both = threading.Barrier(2, timeout=5)

        def put(cid, block):
            """Count the puts in progress, and hold the first two until both are."""
            nonlocal writing, most, started
            with lock:
                writing += 1
                started += 1
                most = max(most, writing)
                first_two = started <= 2
            try:
                if first_two:
                    both.wait()
                real(cid, block)
            finally:
                with lock:
                    writing -= 1

        with mock.patch.object(self.storage.blocks, "put", put), self.storage.protect() as session:
            with session.writer() as write:
                builder.build_bytes(os.urandom(8 * 65536), write)
        self.assertGreaterEqual(most, 2)

    def test_a_failed_write_is_raised_and_the_rest_still_finish(self):
        real = self.storage.blocks.put
        calls = []

        def put(cid, block):
            """Fail the third block, as a full disk would."""
            calls.append(cid)
            if len(calls) == 3:
                raise OSError(28, "No space left on device")
            real(cid, block)

        with mock.patch.object(self.storage.blocks, "put", put), self.storage.protect() as session:
            with self.assertRaises(OSError):
                with session.writer() as write:
                    builder.build_bytes(os.urandom(20 * 65536), write)
        self.assertEqual(self.storage.pins(), {})
        # Every block written is indexed, and nothing else: a write in progress at the error isn't lost track of
        self.assertEqual(set(self.storage._sizes), {c for c in calls if self.storage.blocks.has(c)})

    def test_an_error_in_the_body_waits_for_what_it_queued(self):
        blocks = [os.urandom(1000) for _ in range(10)]
        with self.storage.protect() as session:
            with self.assertRaises(RuntimeError):
                with session.writer() as put:
                    for block in blocks:
                        put(Cid.for_block(RAW, block), block)
                    raise RuntimeError("the add failed")
        self.assertEqual(self.storage.block_count(), 10)  # kept as cache, as before


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

    def test_app_sources(self):
        """app:<app> and app:<app>:<key> are sources (an app's pins), kept across a reopen; app_source makes them
        and app_of reads the app back."""
        cid = self.add(b"x").cid
        sources = {app_source("wiki"), app_source("wiki", "page:42"), app_source("lxmf", IDENTITY)}
        self.assertEqual(sources, {"app:wiki", "app:wiki:page:42", "app:lxmf:" + IDENTITY})
        for source in sources:
            check_source(source)
            self.storage.pin(cid, source)
        self.assertEqual(self.open().pins(), {cid: sources})
        self.assertEqual({app_of(source) for source in sources}, {"wiki", "lxmf"})
        for source in ("user", "name:wiki", "app:", "app:Wiki", "app:wiki:", "app:wiki:a b", "remote:" + IDENTITY):
            self.assertIsNone(app_of(source), source)

    def test_invalid_app_sources(self):
        """App names are lowercase alias characters (no ":"), keys 1-128 of [A-Za-z0-9._:-]: anything else is
        refused when made and when pinned."""
        for name in ("", "Wiki", "-wiki", "wi:ki", "wi ki", "w" * 65, None):
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    check_app_name(name)
                with self.assertRaises(ValueError):
                    app_source(name)
        for key in ("", "a b", "a/b", "k" * 129, "é", 7):
            with self.subTest(key=key):
                with self.assertRaises(ValueError):
                    app_source("wiki", key)
        check_app_name("w" * 64)
        app_source("wiki", "k" * 128)
        for source in ("app:", "app:Wiki", "app:wiki:", "app:wiki:a b", "app:-x", "app::key"):
            with self.subTest(source=source):
                with self.assertRaises(ValueError):
                    check_source(source)

    def test_invalid_sources(self):
        for source in ("", "users", "name:", "remote:xyz", "remote:" + IDENTITY.upper(), "remote:" + IDENTITY + "0",
                       "app:", "app:Wiki", "app:wiki:"):
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

    def test_pin_many_saves_once(self):
        a, b, c = (self.add(b"%d" % i).cid for i in range(3))
        self.storage.pin(a, USER)
        with mock.patch.object(self.storage, "_save_pins", wraps=self.storage._save_pins) as save:
            self.storage.pin_many([a, b, c, b], USER)
            self.assertEqual(save.call_count, 1)
            self.storage.pin_many([a, c], USER)  # nothing new: nothing to save
            self.storage.pin_many([], USER)
            self.assertEqual(save.call_count, 1)
        self.assertEqual(self.storage.pins(), {a: {"user"}, b: {"user"}, c: {"user"}})
        self.assertEqual(self.open().pins(), self.storage.pins())
        with self.assertRaises(ValueError):
            self.storage.pin_many([a], "users")

    def test_failed_save_pins_nothing(self):
        a, b, c = (self.add(b"%d" % i).cid for i in range(3))
        self.storage.pin(a, USER)
        self.storage.pin(b, FOLDER)
        for pin in (lambda: self.storage.pin_many([a, b, c], USER), lambda: self.storage.pin(c, USER)):
            with failing_commits("database or disk is full"):
                with self.assertRaises(OSError):
                    pin()
            # Memory says what the database does
            self.assertEqual(self.storage.pins(), {a: {"user"}, b: {"folder"}})
            self.assertEqual(self.open().pins(), self.storage.pins())
            self.assertEqual(self.storage.pin_count(), 2)

    def test_failed_save_keeps_the_pin(self):
        cid = self.add(b"x").cid
        self.storage.pin(cid, USER)
        with failing_commits():
            with self.assertRaises(OSError):
                self.storage.unpin(cid, USER)
        self.assertEqual(self.storage.pins(), {cid: {"user"}})  # still pinned, as the database says
        self.assertEqual(self.open().pins(), {cid: {"user"}})

    def test_a_pin_is_one_row(self):
        a, b = self.add(b"a").cid, self.add(b"b").cid
        self.storage.pin(a, USER)
        self.storage.pin(a, FOLDER)
        self.storage.pin(b, USER)
        state = os.path.join(self.tmp.name, "state")
        self.assertEqual(sorted(state_rows(state, "SELECT cid, source FROM pins")),
                         sorted([(a.binary, "user"), (a.binary, "folder"), (b.binary, "user")]))
        self.storage.unpin(a, USER)
        self.assertEqual(sorted(state_rows(state, "SELECT cid, source FROM pins")),
                         sorted([(a.binary, "folder"), (b.binary, "user")]))

    def test_invalid_rows_are_skipped_and_deleted(self):
        cid = self.add(b"x").cid
        self.storage.pin(cid, USER)
        self.storage.record_fetch(cid, [], cid)
        self.storage.save()
        state = os.path.join(self.tmp.name, "state")
        # A truncated CID, a CID in text form, an unknown source; a path that isn't a list of DAG names
        for row in ((cid.binary[:-1], "user"), (str(cid), "user"), (cid.binary, "users")):
            state_execute(state, "INSERT INTO pins (cid, source) VALUES (?, ?)", row)
        for path in ('"ab"', '["a/b"]', "{not json"):
            state_execute(state, "INSERT INTO cache (root, path, target, used) VALUES (?, ?, ?, 99)",
                          (cid.binary, path, cid.binary))
        state_execute(state, "INSERT INTO cache (root, path, target, used) VALUES (?, '[]', ?, 98)",
                      (b"\x01\x02", cid.binary))
        storage = self.open()
        self.assertEqual(storage.pins(), {cid: {"user"}})
        self.assertEqual(storage.cached_fetches(), [(cid, (), cid)])
        self.assertEqual(state_rows(state, "SELECT cid, source FROM pins"), [(cid.binary, "user")])
        self.assertEqual(state_rows(state, "SELECT root, path FROM cache"), [(cid.binary, "[]")])

    def test_pin_count(self):
        self.assertEqual(self.storage.pin_count(), 0)
        a, b = self.add(b"a").cid, self.add(b"b").cid
        self.storage.pin(a, USER)
        self.storage.pin(a, FOLDER)
        self.storage.pin(b, USER)
        self.assertEqual(self.storage.pin_count(), 2)  # CIDs, not sources
        self.storage.unpin(a, USER)
        self.assertEqual(self.storage.pin_count(), 2)
        self.storage.unpin(a, FOLDER)
        self.assertEqual(self.storage.pin_count(), 1)

    def test_has_pin(self):
        """has_pin(cid, source) agrees with pins() through pins, unpins, a failed unpin and a reopen."""
        a, b = self.add(b"a").cid, self.add(b"b").cid
        lxmf = "app:lxmf:" + IDENTITY

        def check(storage):
            pins = storage.pins()
            for cid in (a, b):
                for source in (USER, FOLDER, lxmf):
                    self.assertEqual(storage.has_pin(cid, source), source in pins.get(cid, set()), (cid, source))

        check(self.storage)
        self.assertFalse(self.storage.has_pin(a, USER))
        self.storage.pin(a, USER)
        self.storage.pin_many([a, b], lxmf)
        self.assertTrue(self.storage.has_pin(a, lxmf))
        self.assertFalse(self.storage.has_pin(b, USER))
        check(self.storage)
        with failing_commits():
            with self.assertRaises(OSError):
                self.storage.unpin(b, lxmf)
        self.assertTrue(self.storage.has_pin(b, lxmf))  # still pinned, as the database says
        self.storage.unpin(a, USER)
        self.assertFalse(self.storage.has_pin(a, USER))
        self.assertTrue(self.storage.has_pin(a, lxmf))
        self.storage.unpin(b, lxmf)  # its last source: b is no longer pinned at all
        self.assertFalse(self.storage.has_pin(b, lxmf))
        check(self.storage)
        check(self.open())

    def test_pinned_cids(self):
        """pinned_cids() is pins()'s CIDs, the same frozenset while pins don't change, through pins, a second
        source, unpins, failed saves (asked for while saving, too) and a reopen."""
        a, b, c = (self.add(b"%d" % i).cid for i in range(3))

        def check(storage):
            self.assertEqual(storage.pinned_cids(), frozenset(storage.pins()))

        empty = self.storage.pinned_cids()
        self.assertEqual(empty, frozenset())
        self.assertIs(self.storage.pinned_cids(), empty)
        self.storage.pin(a, USER)
        pinned = self.storage.pinned_cids()
        self.assertEqual(pinned, {a})
        self.assertIs(self.storage.pinned_cids(), pinned)
        self.storage.pin_many([a, b], FOLDER)
        self.assertEqual(self.storage.pinned_cids(), {a, b})
        check(self.storage)
        self.storage.pin(a, FOLDER)  # nothing changes
        self.assertIs(self.storage.pinned_cids(), self.storage.pinned_cids())

        def failing_save(*args, **kwargs):
            """Ask for the set while the change is in memory but not saved, then fail as a full disk does."""
            self.storage.pinned_cids()
            raise OSError("database or disk is full")

        with mock.patch.object(self.storage, "_save_pins", side_effect=failing_save):
            with self.assertRaises(OSError):
                self.storage.pin(c, USER)
            self.assertEqual(self.storage.pinned_cids(), {a, b})
            with self.assertRaises(OSError):
                self.storage.unpin(b, FOLDER)
            self.assertEqual(self.storage.pinned_cids(), {a, b})
        with failing_commits():
            with self.assertRaises(OSError):
                self.storage.pin(c, USER)
        check(self.storage)
        self.storage.unpin(a, USER)
        self.assertEqual(self.storage.pinned_cids(), {a, b})  # still pinned by the folder
        self.storage.unpin(a, FOLDER)
        self.assertEqual(self.storage.pinned_cids(), {b})
        check(self.storage)
        check(self.open())

    def test_damaged_state_is_moved_aside(self):
        with open(os.path.join(self.tmp.name, "state", "pins.json"), "w", encoding="utf-8") as f:
            f.write("{not json")
        self.assertEqual(self.open().pins(), {})
        self.assertTrue(os.path.exists(os.path.join(self.tmp.name, "state", "pins.json.damaged")))

    def test_empty_state_file_is_damaged(self):
        # What a power loss can leave of a file written without fsync
        open(os.path.join(self.tmp.name, "state", "pins.json"), "wb").close()
        self.assertEqual(self.open().pins(), {})
        self.assertTrue(os.path.exists(os.path.join(self.tmp.name, "state", "pins.json.damaged")))

    def test_state_of_the_wrong_shape_is_moved_aside(self):
        state = os.path.join(self.tmp.name, "state")
        for name, content in (("pins.json", "[]"), ("pins.json", '{"pins": []}'), ("cache.json", '{"fetches": 5}'),
                              ("cache.json", '"text"')):
            with open(os.path.join(state, name), "w", encoding="utf-8") as f:
                f.write(content)
            storage = self.open()
            self.assertEqual((storage.pins(), storage.cached_fetches()), ({}, []), content)
            self.assertFalse(os.path.exists(os.path.join(state, name)), content)
        # Wrongly shaped records within a well-shaped file are skipped one by one
        cid = self.add(b"x").cid
        with open(os.path.join(state, "pins.json"), "w", encoding="utf-8") as f:
            f.write('{"pins": {"%s": "user"}}' % cid)
        with open(os.path.join(state, "cache.json"), "w", encoding="utf-8") as f:
            f.write('{"fetches": [["%s", "ab", "%s"], ["%s", [], "%s"]]}' % (cid, cid, cid, cid))
        storage = self.open()
        self.assertEqual(storage.pins(), {})
        self.assertEqual(storage.cached_fetches(), [(cid, (), cid)])

    def test_earlier_damaged_copies_are_kept(self):
        state = os.path.join(self.tmp.name, "state")
        for content in ("{first", "{second"):
            with open(os.path.join(state, "pins.json"), "w", encoding="utf-8") as f:
                f.write(content)
            self.open()
        with open(os.path.join(state, "pins.json.damaged"), encoding="utf-8") as f:
            self.assertEqual(f.read(), "{first")
        with open(os.path.join(state, "pins.json.damaged.1"), encoding="utf-8") as f:
            self.assertEqual(f.read(), "{second")

    def test_directory_in_place_of_state_is_moved_aside(self):
        state = os.path.join(self.tmp.name, "state")
        os.mkdir(os.path.join(state, "pins.json"))  # unreadable as a file
        os.makedirs(os.path.join(state, "pins.json.damaged", "in-the-way"))
        self.assertEqual(self.open().pins(), {})
        self.assertTrue(os.path.isdir(os.path.join(state, "pins.json.damaged.1")))

    @unittest.skipUnless(PERMISSION_CHECKS, "needs permission checks")
    def test_unmovable_damaged_state_doesnt_stop_startup(self):
        # The database is open already (setUp's storage), so only moving the damaged file aside fails; a state
        # directory that is read-only from the start stops startup (test_statedb), since nothing could be saved
        state = os.path.join(self.tmp.name, "state")
        with open(os.path.join(state, "pins.json"), "w", encoding="utf-8") as f:
            f.write("{not json")
        os.chmod(state, 0o500)  # read-only: it can't be moved aside
        self.addCleanup(os.chmod, state, 0o700)
        self.assertEqual(self.open().pins(), {})

    @unittest.skipUnless(PERMISSION_CHECKS, "needs permission checks")
    def test_unreadable_state_stops_startup_rather_than_losing_pins(self):
        cid = self.add(b"precious").cid
        state = os.path.join(self.tmp.name, "state")
        path = write_legacy(state, "pins.json", {"pins": {str(cid): ["user"]}})
        os.chmod(path, 0)  # or EIO, EMFILE: not damage, and starting without pins would let gc delete them
        try:
            with self.assertRaises(PermissionError):
                self.open()
            self.assertTrue(os.path.exists(path))  # not imported, not moved: read at the next start
        finally:
            os.chmod(path, 0o600)
        self.assertEqual(self.open().pins(), {cid: {"user"}})

    def test_block_names_are_synced_before_pins_and_cache_records(self):
        calls = []
        real_commit = statedb._Database._commit

        def commit(database, conn):
            """Note the commit and which rows it writes, then commit."""
            calls.append("commit " + ",".join(table for table in ("pins", "cache") if conn.execute(
                f"SELECT count(*) FROM {table}").fetchone()[0]))
            real_commit(database, conn)

        with mock.patch.object(self.storage.blocks, "sync", lambda: calls.append("blocks")), \
                mock.patch.object(statedb._Database, "_commit", commit):
            cid = self.add(b"x").cid
            self.storage.pin(cid, USER)
            self.storage.record_fetch(cid, [], cid)
            self.storage.save()
        self.assertEqual(calls, ["blocks", "commit pins", "blocks", "commit pins,cache"])

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

    def test_gc_preview_changes_nothing_and_tells_what_gc_does(self):
        """gc_preview (0.2.1) deletes nothing, gives what gc then removes, and splits the stored bytes by what keeps
        them: pins, cached fetches only, sessions only, nothing."""
        pinned = self.add(b"P" * 100000)
        cached = self.add(b"C" * 100000)
        protected = self.add(b"S" * 100000)
        orphan = self.add(b"O" * 10)
        self.storage.pin(pinned.cid, USER)
        self.storage.record_fetch(cached.cid, [], cached.cid)

        def total(cid: Cid) -> int:
            """The block bytes of a stored DAG."""
            return sum(self.storage.block_size(block) for block in self.blocks_of(cid))

        with self.storage.protect() as session:
            session.add(protected.cid)
            used, blocks = self.storage.used_bytes(), self.storage.block_count()
            preview = self.storage.gc_preview()
            self.assertEqual((self.storage.used_bytes(), self.storage.block_count()), (used, blocks))
            self.assertEqual(len(self.storage.cached_fetches()), 1)
            self.assertEqual(
                {key: preview[key] for key in ("pinned_bytes", "cached_bytes", "protected_bytes",
                                               "unreferenced_bytes", "cached_fetches", "held_back")},
                {"pinned_bytes": total(pinned.cid), "cached_bytes": total(cached.cid),
                 "protected_bytes": total(protected.cid), "unreferenced_bytes": total(orphan.cid),
                 "cached_fetches": 1, "held_back": False})
            self.assertGreaterEqual(preview["allocated_bytes"], preview["freed_bytes"])
            stats = self.storage.gc()
        self.assertEqual((preview["removed_blocks"], preview["freed_bytes"]),
                         (stats["removed_blocks"], stats["freed_bytes"]))

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

    def test_collect_reads_nothing_of_a_dag_memoised_complete(self):
        """dag_blocks (the first refresh's, at every start) memoises a pin's DAG as complete; a collection then takes
        it whole without reading it again (a sealed folder's unpack, collecting at its end, waited 3 minutes on a node
        with 857k blocks while every node was read), and still deletes what nothing reaches."""
        root, a, b = self.tree()
        self.storage.pin(root.cid, "user")
        self.storage.dag_blocks(root.cid)
        orphan = self.add(b"nothing reaches this")
        read = set()
        real = Storage._read_node
        with mock.patch.object(Storage, "_read_node", lambda s, cid: (read.add(cid), real(s, cid))[1]):
            self.assertEqual(self.storage.collect(), 1)
        self.assertEqual(read, set())
        self.assertFalse(self.storage.has(orphan.cid))
        self.assertTrue(self.storage.is_complete(root.cid))

    def test_warm_reads_what_the_path_walk_reaches(self):
        """_warm and _path_cids share one path walk: warming reads exactly the stored nodes a record's path
        reaches, through index nodes, missing names, files and nodes not stored, and the locked walk then reads
        nothing."""
        with mock.patch.multiple(constants, MAX_CHILDREN=3):
            files = [self.add(b"%02d" % i * 20) for i in range(10)] + [self.add(b"G" * 70000)]  # raw blocks; a node
            names = [f"f{i:02d}" for i in range(10)] + ["g"]
            big = builder.build_directory([Entry(n, f.cid, f.size, 0) for n, f in zip(names, files)],
                                          self.storage.put)  # index nodes over groups of 3 entries
            inner = builder.wrap("big", big, self.storage.put)
            root = builder.wrap("inner", inner, self.storage.put)
        absent = builder.wrap("x", builder.build_bytes(b"absent", builder.NO_PUT), builder.NO_PUT)
        missing = builder.wrap("absent", absent, self.storage.put)  # its one entry isn't stored
        paths = [(root.cid, ("inner", "big", "f05")), (root.cid, ("inner", "big", "g")),
                 (root.cid, ("inner", "big", "a")), (root.cid, ("inner", "big", "zz")),
                 (root.cid, ("inner", "nothing")), (root.cid, ("inner", "big", "f00", "below a raw block")),
                 (root.cid, ("inner", "big", "g", "below a file node")), (missing.cid, ("absent", "x")),
                 (root.cid, ()), (files[0].cid, ("x",))]
        for path in paths:
            with self.subTest(path=path):
                self.storage = self.open()  # nothing memoised
                read = set()
                real = Storage._read_node
                with mock.patch.object(Storage, "_read_node", lambda s, cid: (read.add(cid), real(s, cid))[1]):
                    self.storage._warm(set(), [path])
                    stored = {cid for cid in read if self.storage.has(cid)}
                    read.clear()
                    with self.storage._lock:
                        reached = self.storage._path_cids(*path)
                self.assertEqual(reached, stored)
                self.assertEqual(read, set())  # warmed: the walk under the lock read nothing
        # What the walk reaches is what walker.step passes through
        expected, expect = set(), walker.root_expect(root.cid)
        for name in ("inner", "big", "f05"):
            while True:
                expected.add(expect.cid)
                expect = walker.step(expect, self.storage.get(expect.cid), name)
                if not walker.is_index_step(expect):
                    break
        self.assertGreater(len(expected), 4)  # through index nodes
        with self.storage._lock:
            self.assertEqual(self.storage._path_cids(root.cid, ("inner", "big", "f05")), expected)

    def test_lru_eviction(self):
        first = self.add(b"1" * 1000)
        second = self.add(b"2" * 1000)
        third = self.add(b"3" * 1000)
        for built in (first, second, third):
            self.storage.record_fetch(built.cid, [], built.cid)
        self.assertTrue(self.storage.touch(first.cid, []))  # now second is least recently used
        self.storage.max_storage = self.storage.allocation_unit * 5 // 2  # each block takes a unit on disk
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
        unit = self.storage.allocation_unit  # what each block takes on disk
        self.storage.max_storage = unit * 5 // 2
        self.assertEqual(self.storage.evict(400), set())  # 2 units stored + 400 fit
        self.assertEqual(self.storage.evict(unit), {old.cid})  # least recently used first
        self.assertTrue(self.storage.has(new.cid))
        self.assertLessEqual(self.storage.allocated_bytes() + unit, self.storage.max_storage)

    def test_eviction_walks_once_however_many_records_go(self):
        files = [self.add(b"pinned %d" % i) for i in range(500)]
        pinned = builder.build_directory([Entry(f"{i:03d}", f.cid, f.size, 0) for i, f in enumerate(files)],
                                         self.storage.put)  # hundreds of blocks
        self.storage.pin(pinned.cid, USER)
        for i in range(200):
            built = self.add(b"%d." % i * 100)
            self.storage.record_fetch(built.cid, [], built.cid)
        # About half the records must go: each is a block of its own, taking a unit
        self.storage.max_storage = self.storage.allocated_bytes() - 100 * self.storage.allocation_unit
        visits = []
        real = Storage._children
        with mock.patch.object(Storage, "_children", lambda s, cid: (visits.append(cid), real(s, cid))[1]):
            self.storage.evict()
        self.assertLess(len(visits), 2000)  # once per block, not once per block per evicted record
        self.assertLessEqual(self.storage.allocated_bytes(), self.storage.max_storage)
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
        state = os.path.join(self.tmp.name, "state")
        with mock.patch.object(storage_module, "MAX_CACHE_RECORDS", 3):
            built = [self.add(b"%d" % i) for i in range(5)]
            dropped = set()
            for b in built:
                dropped |= self.storage.record_fetch(b.cid, [], b.cid)
            self.storage.touch(built[2].cid, [])
            self.assertEqual(state_rows(state, "SELECT * FROM cache"), [])  # not written on every fetch
            self.assertEqual(dropped, {built[0].cid, built[1].cid})
            self.storage.save()
            self.assertEqual([t for _, _, t in self.open().cached_fetches()], [built[3].cid, built[4].cid, built[2].cid])
            # A later save writes only the records changed since: here the one touched
            query = "SELECT root, path, used FROM cache"
            before = {(root, path): used for root, path, used in state_rows(state, query)}
            self.storage.touch(built[3].cid, [])
            self.storage.save()
            after = {(root, path): used for root, path, used in state_rows(state, query)}
            self.assertEqual({key for key in after if after[key] != before[key]}, {(built[3].cid.binary, "[]")})
            self.assertEqual([t for _, _, t in self.open().cached_fetches()],
                             [built[4].cid, built[2].cid, built[3].cid])
            # Evicted and cleared records are deleted
            self.storage.gc()
            self.storage.save()
            self.assertEqual(state_rows(state, "SELECT * FROM cache"), [])

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

    def test_verify_checks_completeness_without_the_lock(self):
        pinned, cached, partial = (self.add(byte * 70000) for byte in (b"A", b"B", b"C"))
        self.storage.pin(pinned.cid, USER)
        self.storage.record_fetch(cached.cid, [], cached.cid)
        self.storage.record_fetch(partial.cid, [], partial.cid)
        chunks = {built.cid: [child.cid for child in dag.decode(self.storage.get(built.cid)).children]
                  for built in (pinned, partial)}
        self.storage._delete({chunks[partial.cid][0]})
        self.storage = self.open()  # nothing memoised as complete
        damaged = {chunks[pinned.cid][1], chunks[partial.cid][1]}  # chunks: the completeness walk doesn't read them
        for cid in damaged:
            with open(self.storage.blocks.path(cid), "wb") as f:
                f.write(b"damaged")
        held = []
        real = Storage._complete_sets
        with mock.patch.object(Storage, "_complete_sets",
                               lambda s, cids: (held.append(s._lock._is_owned()), real(s, cids))[1]):
            stats = self.storage.verify()
        self.assertEqual(held, [False])
        self.assertEqual(set(stats["bad"]), damaged)
        self.assertEqual(stats["incomplete"], {pinned.cid})  # not partial: it was incomplete before

    def test_every_record_removal_is_saved_counted_and_collected(self):
        """The cap, eviction, drop_denied and gc each remove records with the same bookkeeping (_drop_records):
        the cache generation moves, the next save deletes their rows, and their blocks are garbage."""
        files = [self.add(b"%d" % i * 1000) for i in range(6)]
        for built in files:
            self.storage.record_fetch(built.cid, [], built.cid)

        def removed(operation) -> set[Cid]:
            """The targets operation removed from the records, checked against a reopened storage."""
            before = self.storage.cached_targets()
            generation = self.storage.cache_generation()
            operation()
            gone = before - self.storage.cached_targets()
            self.assertGreater(self.storage.cache_generation(), generation)
            self.storage.save()
            self.assertEqual(self.open().cached_fetches(), self.storage.cached_fetches())
            if any(self.storage.has(cid) for cid in gone):
                self.assertTrue(self.storage._maybe_garbage)  # the next collection takes them
                self.storage.collect()
            self.assertFalse(any(self.storage.has(cid) for cid in gone))
            return gone

        with mock.patch.object(storage_module, "MAX_CACHE_RECORDS", 5):
            # Touching files[0] leaves files[1] the oldest, past the cap
            self.assertEqual(removed(lambda: self.storage.record_fetch(files[0].cid, [], files[0].cid)),
                             {files[1].cid})
        self.storage.max_storage = self.storage.allocated_bytes() - 500
        self.assertEqual(removed(self.storage.evict), {files[2].cid})
        stats = {}
        self.assertEqual(removed(lambda: stats.update(self.storage.drop_denied(frozenset({files[3].cid.binary})))),
                         {files[3].cid})
        self.assertEqual((stats["uncached"], stats["incomplete"]), ({files[3].cid}, {files[3].cid}))
        self.assertEqual(removed(self.storage.gc), {files[0].cid, files[4].cid, files[5].cid})

    def test_cache_generation(self):
        files = [self.add(b"%d" % i * 1000) for i in range(4)]
        generation = self.storage.cache_generation()

        def changed() -> bool:
            """True if the cache generation moved since the last call."""
            nonlocal generation
            moved, generation = self.storage.cache_generation() != generation, self.storage.cache_generation()
            return moved

        self.storage.record_fetch(files[0].cid, [], files[0].cid)
        self.storage.record_fetch(files[1].cid, [], files[1].cid)
        self.storage.record_fetch(files[1].cid, [], files[1].cid)  # touched again, same target
        self.storage.touch(files[0].cid, [])
        self.assertFalse(changed())  # records added or touched only
        self.storage.record_fetch(files[1].cid, [], files[2].cid)
        self.assertTrue(changed())  # files[1] is no longer a cached target
        self.storage.evict()
        self.assertFalse(changed())  # under max_storage
        unit = self.storage.allocation_unit  # what each block takes on disk
        self.storage.max_storage = 3 * unit
        self.storage.evict()
        self.assertFalse(changed())  # deleting unreached blocks was enough
        self.storage.max_storage = unit * 3 // 2
        self.storage.evict()
        self.assertTrue(changed())
        self.assertEqual(len(self.storage.cached_fetches()), 1)
        self.storage.gc()
        self.assertTrue(changed())
        self.storage.gc()
        self.assertFalse(changed())  # no records left to remove
        with mock.patch.object(storage_module, "MAX_CACHE_RECORDS", 1):
            self.storage.record_fetch(files[0].cid, [], files[0].cid)
            self.assertFalse(changed())
            self.storage.record_fetch(files[3].cid, [], files[3].cid)
            self.assertTrue(changed())  # the cap dropped a record

    def big_pinned_file(self) -> tuple[Cid, Cid]:
        """A pinned file of 300 chunks (a root over two index nodes); returns (root, its last child node)."""
        built = self.add(os.urandom(300 * 64 * 1024))
        self.storage.pin(built.cid, USER)
        return built.cid, dag.decode(self.storage.get(built.cid)).children[-1].cid

    def test_damaged_interior_node_stops_collection(self):
        # A node emptied by a power loss lists nothing readable: the pinned blocks below it aren't garbage
        root, node = self.big_pinned_file()
        garbage = self.add(b"garbage").cid
        self.storage.record_fetch(garbage, [], garbage)
        count = self.storage.block_count()
        open(self.storage.blocks.path(node), "wb").close()
        self.storage = self.open(max_storage=1)
        self.assertEqual(self.storage.evict(), set())
        self.assertEqual(self.storage.gc()["removed_blocks"], 0)
        self.assertEqual(self.storage.block_count(), count)
        self.assertTrue(self.storage.has(garbage))
        # A node that matches its CID but isn't valid lists nothing, and doesn't stop collection
        self.storage.verify()  # removes the damaged node
        invalid = Cid.for_block(DAG, b"not a node")
        self.storage.put(invalid, b"not a node")
        self.storage.pin(invalid, USER)
        self.storage.unpin(root, USER)
        self.storage.gc()
        self.assertEqual(self.storage.block_count(), 1)
        self.assertTrue(self.storage.has(invalid))

    def test_node_damaged_after_it_was_read_still_lists_its_children(self):
        root, node = self.big_pinned_file()
        count = self.storage.block_count()
        self.storage.gc()  # reads every node's children
        with open(self.storage.blocks.path(node), "wb") as f:
            f.write(b"x")
        self.storage.gc()
        self.assertEqual(self.storage.block_count(), count)

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

    def test_an_unreadable_block_is_damaged_not_an_error(self):
        # A read that raises (a disk error, or a file a run as root left that this user can't open) makes the block
        # damaged, said why, rather than breaking every walk, collection and verify that meets it. It is kept, as a
        # damaged node is, until verify removes it: a file left by root is whole, and may be the only copy
        built = self.add(os.urandom(200_000))  # chunks under one file node
        self.storage.pin(built.cid, USER)
        real_get = self.storage.blocks.get

        def get(cid):
            """BlockStore.get, failing for the file node as an unreadable file does."""
            if cid == built.cid:
                raise PermissionError(13, "Permission denied", self.storage.blocks.path(cid))
            return real_get(cid)

        with mock.patch.object(self.storage.blocks, "get", side_effect=get), \
                mock.patch("rnfs.content.storage.log") as logged:
            self.assertFalse(self.storage.is_complete(built.cid))
            self.assertIsNone(self.storage.get(built.cid))
            self.assertTrue(self.storage.has(built.cid))  # kept
            self.storage.evict()
            self.storage.gc()
            self.assertTrue(self.storage.has(built.cid))
            self.storage.verify()
            self.assertFalse(self.storage.has(built.cid))
        self.assertTrue(any("can't be read ([Errno 13] Permission denied" in call.args[0]
                            for call in logged.call_args_list))

    def test_verify_removes_an_unreadable_block_and_says_why(self):
        built = self.add(b"abc")
        real_get = self.storage.blocks.get

        def get(cid):
            """BlockStore.get, failing for built as a bad sector does."""
            if cid == built.cid:
                raise OSError(5, "Input/output error")
            return real_get(cid)

        with mock.patch.object(self.storage.blocks, "get", side_effect=get), \
                mock.patch("rnfs.content.storage.log") as logged:
            stats = self.storage.verify()
        self.assertEqual(stats["bad"], [built.cid])
        self.assertFalse(self.storage.has(built.cid))
        self.assertTrue(any("can't be read ([Errno 5] Input/output error)" in call.args[0]
                            for call in logged.call_args_list))

    def test_a_delete_that_fails_keeps_the_index_and_memos_right(self):
        # One delete raising (a disk error, a filesystem remounted read-only) mustn't leave the blocks deleted before
        # it counted as stored, nor their DAG memoised as complete
        built = self.add(os.urandom(200_000))
        self.storage.record_fetch(built.cid, [], built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))  # memoised
        real_delete = self.storage.blocks.delete
        calls = []

        def delete(cid):
            """BlockStore.delete, the second one failing."""
            calls.append(cid)
            if len(calls) == 2:
                raise OSError(30, "Read-only file system")
            return real_delete(cid)

        with mock.patch.object(self.storage.blocks, "delete", side_effect=delete), \
                mock.patch("rnfs.content.storage.log") as logged:
            stats = self.storage.gc()
        self.assertEqual(stats["removed_blocks"], len(calls) - 1)
        self.assertTrue(self.storage.has(calls[1]))  # still on disk, so still stored and counted
        self.assertFalse(self.storage.is_complete(built.cid))
        self.assertEqual(self.storage.used_bytes(), sum(size for _, size in self.storage.blocks.scan()))
        self.assertEqual(self.storage.block_count(), 1)
        self.assertTrue(any("Read-only file system" in call.args[0] for call in logged.call_args_list))
        self.assertTrue(self.storage._maybe_garbage)  # tried again by the next collection
        self.storage.gc()
        self.assertEqual(self.storage.block_count(), 0)


class AllocationTest(StorageTest):
    """max_storage counts what the blocks take on disk: each block's size rounded up to the allocation unit."""

    UNIT = 32 * 1024  # a FAT32 cluster

    def setUp(self):
        """As StorageTest, on a filesystem allocating UNIT bytes at a time."""
        self.unit = mock.Mock(return_value=self.UNIT)
        patcher = mock.patch.object(fsutil, "allocation_unit", self.unit)
        patcher.start()
        self.addCleanup(patcher.stop)
        super().setUp()

    def test_each_block_takes_whole_units(self):
        lengths = {b"": 0, b"x" * 100: 100, b"y" * self.UNIT: self.UNIT, b"z" * (self.UNIT + 1): self.UNIT + 1,
                   b"c" * constants.CHUNK_SIZE: constants.CHUNK_SIZE}
        for data in lengths:
            self.storage.put(Cid.for_block(RAW, data), data)
        self.assertEqual(self.storage.allocation_unit, self.UNIT)
        self.assertEqual(self.storage.used_bytes(), sum(lengths.values()))  # still the blocks' bytes
        # An empty block is a file too; a 64 KiB chunk takes two clusters exactly
        expected = (1 + 1 + 1 + 2 + constants.CHUNK_SIZE // self.UNIT) * self.UNIT
        self.assertEqual(self.storage.allocated_bytes(), expected)
        # Derived from the sizes kept, after a clean stop and after one that wasn't (every block listed): nothing
        # is stored for it, so an upgraded store needs no migration
        self.storage.close()
        self.assertEqual(self.open().allocated_bytes(), expected)
        self.assertEqual(self.open().allocated_bytes(), expected)  # the one before was never closed
        self.assertEqual(self.unit.call_count, 3)  # once a start, never a stat per block
        self.storage = self.open()
        self.storage.gc()
        self.assertEqual((self.storage.used_bytes(), self.storage.allocated_bytes()), (0, 0))

    def test_max_storage_counts_allocation(self):
        # Ten cached files of 100 bytes: 1,000 bytes, but ten clusters on disk
        files = [self.add(b"%d" % i * 100) for i in range(10)]
        for built in files:
            self.storage.record_fetch(built.cid, [], built.cid)
        self.storage.max_storage = 100_000  # a hundred times the bytes, three clusters
        self.storage.evict()
        self.assertLessEqual(self.storage.allocated_bytes(), self.storage.max_storage)
        self.assertEqual(self.storage.cached_targets(), {files[8].cid, files[9].cid})  # a third would cross the margin
        self.assertEqual(self.storage.used_bytes(), 200)


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

    def test_stat_of_a_partial_dag_doesnt_hide_nodes_stored_later(self):
        root, a, b = self.tree()
        blocks = {cid: self.storage.get(cid) for cid in self.blocks_of(root.cid)}
        self.storage = Storage(os.path.join(self.tmp.name, "blocks2"), os.path.join(self.tmp.name, "state2"), 10**9)
        self.addCleanup(self.storage.close)
        self.storage.put(root.cid, blocks[root.cid])
        self.storage.pin(root.cid, USER)
        self.assertEqual(self.storage.stat(root.cid)["local_blocks"], 1)  # a's and b's nodes aren't stored yet
        for cid, block in blocks.items():
            self.storage.put(cid, block)
        self.assertEqual(self.storage.stat(root.cid)["local_blocks"], 7)
        self.assertEqual(self.storage.gc()["removed_blocks"], 0)  # a's and b's chunks are reached through them
        self.assertTrue(self.storage.is_complete(root.cid))

    def test_size_queries_read_nodes_without_the_lock(self):
        root, a, b = self.tree()
        self.storage.pin(root.cid, remote_source(IDENTITY))
        expected = {"pinned": self.storage.pinned_bytes(), "dag": self.storage.dag_bytes(root.cid),
                    "usage": self.storage.source_usage(remote_source(IDENTITY)), "stat": self.storage.stat(root.cid)}
        queries = {"pinned": lambda s: s.pinned_bytes(), "dag": lambda s: s.dag_bytes(root.cid),
                   "usage": lambda s: s.source_usage(remote_source(IDENTITY)), "stat": lambda s: s.stat(root.cid)}
        for name, query in queries.items():
            with self.subTest(name):
                self.storage = self.open()  # nothing memoised
                held = []
                real = Storage._read_node
                with mock.patch.object(Storage, "_read_node",
                                       lambda s, cid: (held.append(s._lock._is_owned()), real(s, cid))[1]):
                    self.assertEqual(query(self.storage), expected[name])
                self.assertEqual(len(held), 3)  # root, a's node and b's node, once each
                self.assertNotIn(True, held)
                self.assertFalse(any(cid.is_raw for cid in self.storage._links))  # raw blocks list nothing


def walked_reach(storage: Storage, roots, known=frozenset(), damaged=None, partial=frozenset()) -> set[Cid]:
    """Storage._reach as it was before D3: always a walk, never a complete DAG taken whole (the reference)."""
    reached: set[Cid] = set()
    stack = [cid for cid in roots if cid in storage._sizes and cid not in known]
    while stack:
        cid = stack.pop()
        if cid in reached:
            continue
        reached.add(cid)
        listed = storage._children(cid)
        if listed is None:
            if damaged is not None:
                damaged.add(cid)
            continue
        stack.extend(child for child in listed if child in storage._sizes and child not in reached and child not in known)
    return reached


class CompletenessMemoTest(StorageTest):
    """D3: complete DAGs taken whole, their byte totals and incomplete DAGs' missing blocks remembered; each memo
    exact through every way blocks go (deletion, verify, eviction, unpin and gc) and come back."""

    def scenario(self, seed: int) -> list[builder.Built]:
        """Files shared between pinned and cached directories, records through paths, a block missing; the files."""
        rng = random.Random(seed)
        files = [self.add(b"%d.%d." % (seed, i) * rng.choice((10, 2000, 30000))) for i in range(12)]
        dirs = []
        for _ in range(5):
            chosen = rng.sample(range(len(files)), rng.randint(1, 5))
            dirs.append(builder.build_directory([Entry(f"f{i:02d}", files[i].cid, files[i].size, 0) for i in chosen],
                                                self.storage.put))
        top = builder.build_directory([Entry(f"d{i}", d.cid, d.size, 2) for i, d in enumerate(dirs)], self.storage.put)
        # Older, another directory over some of the same: its DAG holds the path nodes of newer records
        other = builder.build_directory([Entry(f"x{i}", dirs[i].cid, dirs[i].size, 2) for i in rng.sample(range(5), 3)],
                                        self.storage.put)
        self.storage.record_fetch(other.cid, [], other.cid)
        self.storage.pin(dirs[0].cid, USER)
        self.storage.pin(files[rng.randrange(len(files))].cid, USER)
        self.storage.record_fetch(top.cid, [], top.cid)
        for d in rng.sample(dirs[1:], 2):
            self.storage.record_fetch(d.cid, [], d.cid)
        for i, d in enumerate(dirs):
            entries = dag.decode(self.storage.get(d.cid)).entries
            entry = entries[rng.randrange(len(entries))]
            self.storage.record_fetch(top.cid, [f"d{i}", entry.name], entry.cid)
        for root, names, _ in rng.sample(self.storage.cached_fetches(), 3):
            self.storage.touch(root, names)
        self.storage._delete({files[rng.randrange(len(files))].cid})  # a file incomplete or gone
        return files

    def copy(self, name: str) -> Storage:
        """A storage over a copy of this one's blocks and state."""
        self.storage.save()
        for part in ("blocks", "state"):
            shutil.copytree(os.path.join(self.tmp.name, part), os.path.join(self.tmp.name, name, part))
        storage = Storage(os.path.join(self.tmp.name, name, "blocks"), os.path.join(self.tmp.name, name, "state"),
                          self.storage.max_storage)
        self.addCleanup(storage.close)
        return storage

    def test_collections_decide_as_the_walk_does(self):
        """Eviction, gc and drop_denied delete, keep and report exactly what they did when every DAG was walked,
        including records whose target holds a newer record's path node (kept without its other children)."""
        for seed in range(16):
            for operation in ("evict", "gc", "drop_denied"):
                with self.subTest(seed=seed, operation=operation):
                    self.tmp = tempfile.TemporaryDirectory()
                    self.addCleanup(self.tmp.cleanup)
                    self.storage = self.open()
                    files = self.scenario(seed)
                    results = []
                    for name, reach in (("memo", Storage._reach), ("walk", walked_reach)):
                        storage = self.copy(name)
                        for cid in storage.cached_targets() | set(storage.pins()):
                            storage.is_complete(cid)  # memoised, as the publisher and earlier evictions leave them
                        storage.max_storage = storage.used_bytes() * (seed + 2) // 18
                        with mock.patch.object(Storage, "_reach", reach):
                            if operation == "evict":
                                result = storage.evict()
                            elif operation == "gc":
                                result = storage.gc()
                            else:
                                result = storage.drop_denied(frozenset({files[seed % 12].cid.binary}))
                        results.append((result, storage.cached_fetches(), sorted(cid for cid, _ in storage.blocks.scan())))
                    self.assertEqual(results[0], results[1])

    def test_complete_dags_are_taken_whole(self):
        root, a, b = self.tree()
        self.storage.pin(root.cid, USER)
        expected = self.storage.pinned_bytes()
        self.storage = self.open()
        self.assertTrue(self.storage.is_complete(root.cid))
        visits = []
        real = Storage._children
        with mock.patch.object(Storage, "_children", lambda s, cid: (visits.append(cid), real(s, cid))[1]):
            self.assertEqual(self.storage.pinned_bytes(), expected)
            with self.storage._lock:
                self.assertEqual(self.storage._reach([root.cid]), self.blocks_of(root.cid))
                self.assertEqual(self.storage._reach([root.cid], {a.cid} | self.blocks_of(a.cid)),
                                 self.blocks_of(root.cid) - self.blocks_of(a.cid))
        self.assertEqual(visits, [])
        # A partial block of known (a path node kept without its other children) stops the DAG being taken whole
        with self.storage._lock:
            self.assertEqual(self.storage._reach([root.cid], {root.cid}), set())
            self.assertEqual(self.storage._reach([root.cid], {a.cid}, partial={a.cid}),
                             walked_reach(self.storage, [root.cid], {a.cid}))
            self.assertEqual(self.storage._reach([root.cid], {a.cid}, partial={a.cid}),
                             self.blocks_of(root.cid) - self.blocks_of(a.cid))

    def test_dag_bytes_memo_follows_every_deletion(self):
        """dag_bytes of a complete DAG is summed once, and forgotten whenever one of its blocks goes: deleted,
        found bad by verify, evicted with a cached fetch, or collected after an unpin."""
        def check(cid: Cid, remembered: bool):
            """dag_bytes is what a walk on a storage that has memoised nothing finds, from the memo or not."""
            self.storage.is_complete(cid)
            self.assertEqual(self.storage.dag_bytes(cid), self.open().dag_bytes(cid))
            self.assertEqual(cid in self.storage._complete_bytes, remembered)

        root, a, b = self.tree()
        self.storage.pin(root.cid, USER)
        check(root.cid, True)
        self.storage._delete({dag.decode(self.storage.get(a.cid)).children[0].cid})
        check(root.cid, False)
        self.storage.unpin(root.cid, USER)

        root, a, b = self.tree()  # a's lost chunk is back
        self.storage.pin(root.cid, USER)
        check(root.cid, True)
        with open(self.storage.blocks.path(b.cid), "wb") as f:
            f.write(b"damaged")
        self.storage.verify()
        check(root.cid, False)
        self.storage.unpin(root.cid, USER)

        cached = self.add(b"C" * 70000)
        self.storage.record_fetch(cached.cid, [], cached.cid)
        check(cached.cid, True)
        self.storage.max_storage = 1
        self.assertEqual(self.storage.evict(), {cached.cid})
        check(cached.cid, False)
        self.storage.max_storage = 10**9

        pinned = self.add(b"P" * 70000)
        self.storage.pin(pinned.cid, USER)
        check(pinned.cid, True)
        self.storage.unpin(pinned.cid, USER)
        self.storage.gc()
        check(pinned.cid, False)
        self.assertEqual(self.storage.dag_bytes(pinned.cid), 0)

    def test_entries_come_from_the_completeness_walk(self):
        """dag_entries: the CIDs directory entries name anywhere in a complete DAG (through index nodes, a repeated
        directory's once), found by dag_blocks' walk and memoised with it; none for a file; forgotten with it."""
        def entries_walked(cid: Cid) -> set[Cid]:
            """Every entry's CID below cid, by walking its directories."""
            found, stack, get = set(), [walker.root_expect(cid)], self.storage.getter()
            while stack:
                for entry, child in walker.directory_entries(stack.pop(), get):
                    found.add(entry.cid)
                    if entry.kind == dag.KIND_DIRECTORY:
                        stack.append(child)
            return found

        root, a, b = self.tree()
        files = [self.add(b"w%d" % i) for i in range(300)]
        wide = builder.build_directory([Entry(f"w{i:03d}", built.cid, built.size, 0) for i, built in enumerate(files)],
                                       self.storage.put)  # an index node's root
        top = builder.build_directory([Entry("tree", root.cid, root.size, 2), Entry("again", root.cid, root.size, 2),
                                       Entry("wide", wide.cid, wide.size, 2), Entry("a", a.cid, a.size, 0)],
                                      self.storage.put)
        expected, blocks = entries_walked(top.cid), frozenset(self.blocks_of(top.cid))
        self.assertEqual(len(expected), 3 + 1 + 300)  # tree (twice) wide a, then b below tree, then wide's
        self.assertEqual(self.storage.dag_entries(top.cid), expected)
        self.assertIn(top.cid, self.storage._complete)  # the walk memoised dag_blocks too
        with mock.patch.object(self.storage.blocks, "get", side_effect=AssertionError("read")):
            self.assertEqual(self.storage.dag_entries(top.cid), expected)
            self.assertEqual(self.storage.dag_blocks(top.cid), blocks)
        self.assertEqual(self.storage.dag_entries(root.cid), entries_walked(root.cid))
        self.assertEqual(self.storage.dag_entries(a.cid), frozenset())  # a file
        self.assertEqual(self.storage.dag_entries(self.add(b"raw").cid), frozenset())
        # A block deleted: forgotten with the DAG's blocks, and NotFound as dag_blocks
        self.storage._delete({dag.decode(self.storage.get(a.cid)).children[0].cid})
        self.assertNotIn(top.cid, self.storage._complete_entries)
        with self.assertRaises(NotFound):
            self.storage.dag_entries(top.cid)

    def test_a_repeated_chunk_is_checked_at_each_occurrence(self):
        """Raw blocks' expectations aren't remembered by the walk, but each occurrence is still checked."""
        chunk = self.add(b"abcde").cid
        good = dag.encode(FileNode([Child(chunk, 5), Child(chunk, 5)]))
        bad = dag.encode(FileNode([Child(chunk, 5), Child(chunk, 6)]))
        for block, complete in ((good, True), (bad, False)):
            cid = Cid.for_block(DAG, block)
            self.storage.put(cid, block)
            self.assertEqual(self.storage.is_complete(cid), complete)

    def test_missing_block_is_remembered(self):
        """An incomplete DAG stays incomplete while the block found missing is: asked again it reads nothing,
        and it is walked again once that block is back."""
        built = self.add(os.urandom(300 * 64 * 1024))  # a root over two file nodes
        nodes = dag.decode(self.storage.get(built.cid)).children
        first = dag.decode(self.storage.get(nodes[0].cid)).children[0].cid  # the walk meets it last
        last = dag.decode(self.storage.get(nodes[1].cid)).children[-1].cid
        blocks = {cid: self.storage.get(cid) for cid in (first, last)}
        self.storage._delete({first})
        self.assertFalse(self.storage.is_complete(built.cid))
        self.assertEqual(self.storage._missing, {built.cid: first})
        with mock.patch.object(self.storage.blocks, "get", side_effect=AssertionError("read")), \
                mock.patch.object(Storage, "block_size", side_effect=AssertionError("sized")):
            self.assertFalse(self.storage.is_complete(built.cid))
            with self.assertRaises(NotFound):
                self.storage.dag_blocks(built.cid)
        self.storage._delete({last})  # another block goes too: still incomplete, still remembered
        self.assertFalse(self.storage.is_complete(built.cid))
        self.storage.put(first, blocks[first])  # back: walked again, finding the other
        self.assertFalse(self.storage.is_complete(built.cid))
        self.assertEqual(self.storage._missing, {built.cid: last})
        self.storage.put(last, blocks[last])
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual(self.storage._missing, {})
        # A root that isn't stored
        absent = Cid.for_block(RAW, b"absent")
        self.assertFalse(self.storage.is_complete(absent))
        self.storage.put(absent, b"absent")
        self.assertTrue(self.storage.is_complete(absent))

    def test_missing_block_memo_is_bounded_and_notes_deletions_during_the_walk(self):
        files = [self.add(b"%d" % i * 70000) for i in range(4)]
        for f in files:
            self.storage._delete({dag.decode(self.storage.get(f.cid)).children[0].cid})
        with mock.patch.object(storage_module, "MISSING_MEMO", 2):
            for f in files:
                self.assertFalse(self.storage.is_complete(f.cid))
            self.assertEqual(list(self.storage._missing), [files[2].cid, files[3].cid])
            self.assertFalse(self.storage.is_complete(files[0].cid))  # forgotten: walked again
        # A block of the DAG deleted while the walk runs, after the walk counted it, is remembered as missing
        whole = self.add(b"W" * 70000)
        chunks = dag.decode(self.storage.get(whole.cid)).children
        real = self.storage.block_size

        def block_size(cid):
            """Storage.block_size; the last chunk's (met first) deletes it once counted."""
            size = real(cid)
            if cid == chunks[-1].cid and size is not None:
                self.storage._delete({cid})
            return size

        with mock.patch.object(self.storage, "block_size", block_size):
            with self.assertRaises(NotFound) as caught:
                self.storage.dag_blocks(whole.cid)
        self.assertEqual(str(caught.exception), f"a block of {whole.cid} was deleted")
        self.assertEqual(self.storage._missing[whole.cid], chunks[-1].cid)
        self.assertFalse(self.storage.is_complete(whole.cid))

if __name__ == "__main__":
    unittest.main()
