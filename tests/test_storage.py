"""Tests for Storage (specification section 8): pins and sources, cached fetches, protection, eviction, GC."""
import os
import shutil
import tempfile
import threading
import unittest
from unittest import mock

from rnfs import constants, fsutil, statedb
from rnfs.content import builder, dag, storage as storage_module, walker
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.dag import Child, Entry, FileNode
from rnfs.content.storage import FOLDER, USER, Storage, check_source, name_source, remote_source
from rnfs.errors import NotFound, StorageFull
from tests.support import POSIX_MODES, failing_commits, state_execute, state_rows, write_legacy

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
        lxmf = "lxmf:" + IDENTITY

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

    @unittest.skipUnless(POSIX_MODES and hasattr(os, "getuid") and os.getuid() != 0, "needs permission checks")
    def test_unmovable_damaged_state_doesnt_stop_startup(self):
        # The database is open already (setUp's storage), so only moving the damaged file aside fails; a state
        # directory that is read-only from the start stops startup (test_statedb), since nothing could be saved
        state = os.path.join(self.tmp.name, "state")
        with open(os.path.join(state, "pins.json"), "w", encoding="utf-8") as f:
            f.write("{not json")
        os.chmod(state, 0o500)  # read-only: it can't be moved aside
        self.addCleanup(os.chmod, state, 0o700)
        self.assertEqual(self.open().pins(), {})

    @unittest.skipUnless(POSIX_MODES and hasattr(os, "getuid") and os.getuid() != 0, "needs permission checks")
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
        self.storage.max_storage = 3000
        self.storage.evict()
        self.assertFalse(changed())  # deleting unreached blocks was enough
        self.storage.max_storage = 1500
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


if __name__ == "__main__":
    unittest.main()
