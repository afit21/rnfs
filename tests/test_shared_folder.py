"""Tests for the shared folder (specification section 8): entries imported and pinned with source "folder"."""
import hashlib
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

from rnfs import fsutil, statedb
from rnfs.config import Config
from rnfs.content import builder, chunker, shared_folder
from rnfs.content.builder import Built
from rnfs.content.cid import RAW, Cid
from rnfs.content.shared_folder import FolderFiles, SharedFolder
from rnfs.content.storage import FOLDER, USER, Storage
from rnfs.errors import BlockMismatch, Denied, TooLarge
from rnfs.node import RnfsNode
from tests.support import CAN_SYMLINK, HAS_FIFO, POSIX_MODES, state_execute, state_rows, write_legacy
from tests.test_builder import swap_on_open, within
from tests.test_names import FakeNetwork
from tests.test_node import A_ID, NetClient

SECRET = b"private key"


class SharedFolderTest(unittest.TestCase):
    """Scanning a temporary shared folder as entries are added, changed and removed."""

    def setUp(self):
        """Storage and a shared folder over a temporary directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.files = os.path.join(self.tmp.name, "files")
        self.state = os.path.join(self.tmp.name, "state")
        self.storage = Storage(os.path.join(self.tmp.name, "blocks"), self.state, 10**9)
        self.addCleanup(self.storage.close)
        self.folder = self.open()

    def open(self) -> SharedFolder:
        """A SharedFolder with its state under the temporary directory, closed when the test ends."""
        folder = SharedFolder(self.files, self.storage, self.state)
        self.addCleanup(folder.close)
        return folder

    def rows(self) -> dict:
        """What the shared_folder table holds: entry name -> (signature, CID)."""
        return {statedb.load(name): (sig, Cid.from_bytes(cid))
                for name, sig, cid in state_rows(self.state, "SELECT name, signature, cid FROM shared_folder")}

    def write(self, relative: str, data: bytes) -> str:
        """Write a file in the folder, creating directories."""
        path = os.path.join(self.files, relative)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
        return path

    def cid_of(self, relative: str):
        """The CID add would give an entry."""
        return builder.add_path(os.path.join(self.files, relative), builder.NO_PUT)[0].cid

    def test_state_of_the_wrong_shape_is_moved_aside(self):
        """A shared_folder.json of the wrong shape is moved aside when imported; records that don't parse are
        skipped."""
        for content, damaged in (('["a"]', True), ('{"entries": ["a"]}', True),
                                 ('{"entries": {"a": {"0": 1}, "b": 5, "c": ["sig", "not a cid"]}}', False)):
            path = write_legacy(self.state, "shared_folder.json", text=content)
            self.assertEqual(self.open().names(), {}, content)
            self.assertEqual(os.path.exists(path + ".damaged"), damaged, content)
            if damaged:
                os.remove(path + ".damaged")

    def test_entries_are_saved_as_they_change(self):
        """A scan writes the entries it changed and deletes the removed ones; a store without a state
        directory keeps nothing."""
        path = self.write("a.txt", b"a")
        self.write("b.txt", b"b")
        self.folder.scan()
        self.assertEqual(self.rows(), self.folder._known)
        self.assertEqual(set(self.rows()), {"a.txt", "b.txt"})
        os.remove(path)
        self.folder.scan()
        self.assertEqual(set(self.rows()), {"b.txt"})
        unsaved = SharedFolder(os.path.join(self.tmp.name, "other"), self.storage)
        self.addCleanup(unsaved.close)
        self.write(os.path.join("..", "other", "c.txt"), b"c")
        self.assertEqual(len(unsaved.scan()), 1)
        self.assertEqual(set(self.rows()), {"b.txt"})

    def test_names_that_are_not_unicode_round_trip(self):
        """An entry name with a lone surrogate (a file name that isn't UTF-8) is saved and loaded as it was."""
        odd = "caf\udce9"
        cid = Cid.for_block(RAW, b"x")
        with self.folder._lock:
            self.folder._known = {odd: ("sig", cid)}
            self.folder._save()
        self.assertEqual(self.open().names(), {cid: odd})

    def test_damaged_rows_are_deleted(self):
        """Rows that don't parse are skipped at load and deleted; the others are kept."""
        self.write("a.txt", b"a")
        self.folder.scan()
        cid = self.cid_of("a.txt").binary
        for name, sig, binary in (("not json", "sig", cid), ('"b"', b"sig", cid), ('"c"', "sig", b"not a cid"),
                                  ("7", "sig", cid)):
            state_execute(self.state, "INSERT INTO shared_folder (name, signature, cid) VALUES (?, ?, ?)",
                          (name, sig, binary))
        self.assertEqual(self.open().names(), {self.cid_of("a.txt"): "a.txt"})
        self.assertEqual(set(self.rows()), {"a.txt"})

    def test_failed_save_is_written_with_the_next_change(self):
        """A scan whose commit failed raises, keeps what it found in memory, as before, and the next scan that
        changes something saves it too."""
        self.write("a.txt", b"a")
        with mock.patch.object(self.folder._db, "transaction", side_effect=statedb.StateDbError("disk I/O error")):
            with self.assertRaises(OSError):
                self.folder.scan()
        self.assertEqual(self.folder.names(), {self.cid_of("a.txt"): "a.txt"})
        self.assertEqual(self.rows(), {})
        self.write("b.txt", b"b")
        self.folder.scan()
        self.assertEqual(set(self.rows()), {"a.txt", "b.txt"})

    def test_import_failing_for_a_reason_that_may_pass_is_tried_again(self):
        """A pin that couldn't be saved (a full or failing disk) fails the import for RETRY_AFTER, not until the
        file changes; the entry is imported once it is tried again."""
        clock = [100.0]
        self.folder.clock = lambda: clock[0]
        self.write("a.txt", b"a")
        with mock.patch.object(self.storage, "pin", side_effect=statedb.StateDbError("disk full")), \
                mock.patch("rnfs.content.shared_folder.log"):
            self.assertEqual(self.folder.scan(), set())
        self.assertEqual(self.folder.scan(), set())  # not yet: no retry before RETRY_AFTER
        self.assertEqual(self.storage.pinned_by(FOLDER), set())
        clock[0] += shared_folder.RETRY_AFTER
        self.assertEqual(self.folder.scan(), {self.cid_of("a.txt")})
        self.assertEqual(self.storage.pinned_by(FOLDER), {self.cid_of("a.txt")})

    def test_import_failing_for_its_content_waits_for_a_change(self):
        """A failure of the content itself (an RnfsError: here TooLarge) isn't tried again, however long, until it
        changes."""
        clock = [100.0]
        self.folder.clock = lambda: clock[0]
        self.write("a.txt", b"a")
        with mock.patch.object(builder, "build_open", side_effect=TooLarge("too deep")), \
                mock.patch("rnfs.content.shared_folder.log"):
            self.folder.scan()
        clock[0] += 100 * shared_folder.RETRY_AFTER
        with mock.patch.object(builder, "build_open", side_effect=AssertionError("tried again")):
            self.assertEqual(self.folder.scan(), set())
        self.write("a.txt", b"changed")
        self.assertEqual(self.folder.scan(), {self.cid_of("a.txt")})

    def test_unexpected_value_error_is_tried_again(self):
        """A ValueError isn't the content's (invalid names and special files inside are skipped): a bug, such as a
        BlockMismatch, so the entry is logged with the error's type and tried again RETRY_AFTER later, not parked
        until it changes (XC4)."""
        for error in (ValueError("bug"), BlockMismatch("block does not match")):
            with self.subTest(type(error).__name__):
                clock = [100.0]
                self.folder.clock = lambda: clock[0]
                name = f"{type(error).__name__}.txt"
                self.write(name, name.encode())
                with mock.patch.object(builder, "build_open", side_effect=error), \
                        mock.patch("rnfs.content.shared_folder.log") as log:
                    self.folder.scan()
                self.assertIn(type(error).__name__, log.call_args_list[0].args[0])
                self.assertEqual(log.call_args_list[0].args[1], shared_folder.LOG_WARNING)
                with mock.patch.object(builder, "build_open", side_effect=AssertionError("tried again")):
                    self.folder.scan()  # not yet: no retry before RETRY_AFTER
                clock[0] += shared_folder.RETRY_AFTER
                self.assertIn(self.cid_of(name), self.folder.scan())

    def test_unchanged_entry_whose_pin_fails_doesnt_stop_the_scan(self):
        """A failed pin of an unchanged entry is logged; the other entries are scanned and it is kept."""
        self.write("a.txt", b"a")
        self.folder.scan()
        self.storage.unpin(self.cid_of("a.txt"), FOLDER)
        self.write("b.txt", b"b")
        pin = self.storage.pin

        def failing(cid, source):
            """Fail a.txt's pin as a full disk would."""
            if cid == self.cid_of("a.txt"):
                raise statedb.StateDbError("disk full")
            return pin(cid, source)
        with mock.patch.object(self.storage, "pin", failing), mock.patch("rnfs.content.shared_folder.log") as log:
            self.assertEqual(self.folder.scan(), {self.cid_of("a.txt"), self.cid_of("b.txt")})
        self.assertIn("couldn't pin", log.call_args_list[0].args[0])
        self.assertEqual(self.storage.pinned_by(FOLDER), {self.cid_of("b.txt")})
        self.folder.scan()  # pinned again by the next scan
        self.assertEqual(self.storage.pinned_by(FOLDER), {self.cid_of("a.txt"), self.cid_of("b.txt")})

    def test_names_answers_during_a_scan(self):
        """names() doesn't wait for a scan's imports (`pin ls` during a long import): it answers with the entries
        of the last scan to finish."""
        self.write("a.txt", b"a")
        self.folder.scan()
        self.write("b.txt", b"b")
        a, b = self.cid_of("a.txt"), self.cid_of("b.txt")  # before the patch: cid_of goes through build_open
        importing, release = threading.Event(), threading.Event()
        build_open = builder.build_open

        def slow_build_open(*args, **kwargs):
            """builder.build_open, held until the test releases it."""
            importing.set()
            release.wait(10)
            return build_open(*args, **kwargs)

        with mock.patch.object(builder, "build_open", side_effect=slow_build_open):
            scan = threading.Thread(target=self.folder.scan)
            scan.start()
            try:
                self.assertTrue(importing.wait(10))
                answers = []
                reader = threading.Thread(target=lambda: answers.append(self.folder.names()))
                reader.start()
                reader.join(5)
                self.assertEqual(answers, [{a: "a.txt"}])
            finally:
                release.set()
                scan.join(10)
        self.assertEqual(self.folder.names(), {a: "a.txt", b: "b.txt"})

    # The per-file cache (audit D1): inside an entry that changed, files that didn't aren't read again

    def tree(self, count: int, entry: str = "dir") -> list[str]:
        """entry/f0 ... with content "file 0" ...: paths."""
        return [self.write(os.path.join(entry, f"f{i}"), f"file {i}".encode()) for i in range(count)]

    def rewrite(self, path: str, data: bytes):
        """Write data over path (the same inode), a second later by its modification time (a rewrite within the
        filesystem's time resolution would keep it)."""
        st = os.stat(path)
        with open(path, "wb") as f:
            f.write(data)
        os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 10**9))

    def scan_counting(self, options: builder.BuildOptions | None = None) -> tuple[set[Cid], int]:
        """A scan's pins, and how many files it read."""
        with mock.patch.object(builder, "build_file", wraps=builder.build_file) as build_file:
            pins = self.folder.scan(options)
        return pins, build_file.call_count

    def test_changing_one_file_reads_only_that_file(self):
        paths = self.tree(5)
        self.assertEqual(self.scan_counting(), ({self.cid_of("dir")}, 5))
        self.rewrite(paths[2], b"changed, and longer")
        pins, read = self.scan_counting()
        self.assertEqual(read, 1)
        self.assertEqual(pins, {self.cid_of("dir")})
        self.assertEqual(self.storage.pinned_by(FOLDER), pins)
        self.assertTrue(self.storage.is_complete(self.cid_of("dir")))

    def test_same_size_rewrite_and_touch_are_read_again(self):
        """A file rewritten with content of the same size, or only touched, has a new modification time: read."""
        paths = self.tree(3)
        self.folder.scan()
        self.rewrite(paths[0], b"FILE 0")
        pins, read = self.scan_counting()
        self.assertEqual((pins, read), ({self.cid_of("dir")}, 1))
        st = os.stat(paths[1])
        os.utime(paths[1], ns=(st.st_atime_ns, st.st_mtime_ns + 10**9))
        self.assertEqual(self.scan_counting(), (pins, 1))  # read again; the same content, so the same CID

    def test_renamed_files_are_not_read_again(self):
        """A file renamed in place, over another, or into another entry is the same inode, size and time."""
        paths = self.tree(3)
        self.tree(1, "other")
        self.write("top.txt", b"a top-level file")
        self.folder.scan()
        os.rename(paths[0], os.path.join(self.files, "dir", "renamed"))
        os.replace(paths[1], paths[2])
        os.rename(os.path.join(self.files, "top.txt"), os.path.join(self.files, "moved.txt"))
        pins, read = self.scan_counting()
        self.assertEqual(read, 0)
        self.assertEqual(pins, {self.cid_of("dir"), self.cid_of("other"), self.cid_of("moved.txt")})
        os.rename(os.path.join(self.files, "dir", "renamed"), os.path.join(self.files, "other", "from dir"))
        pins, read = self.scan_counting()
        self.assertEqual(read, 0)
        self.assertEqual(pins, {self.cid_of("dir"), self.cid_of("other"), self.cid_of("moved.txt")})
        self.assertEqual(self.storage.pinned_by(FOLDER), pins)

    def test_a_newly_denied_hash_is_still_refused(self):
        """A remembered file's content hash goes through the deny check as if it were read."""
        paths = self.tree(3)
        self.folder.scan(builder.BuildOptions(check=lambda digest: None))  # hashed: the deny list has hashes
        denied = hashlib.sha256(b"file 1").digest()

        def deny(digest: bytes):
            """The deny list's check, denying file 1's content."""
            if digest == denied:
                raise Denied("denied")

        self.rewrite(paths[0], b"changed")
        with mock.patch("rnfs.content.shared_folder.log") as log:
            pins, read = self.scan_counting(builder.BuildOptions(check=deny))
        self.assertEqual(read, 1)  # only the changed file: file 1's remembered hash was checked
        self.assertEqual(pins, set())
        self.assertEqual(self.storage.pinned_by(FOLDER), set())
        self.assertIn("couldn't import", log.call_args.args[0])

    def test_a_file_remembered_without_its_hash_is_read_when_there_is_a_check(self):
        """Files built while the deny list had no hashes were never hashed: with a check, they are read again."""
        paths = self.tree(3)
        self.folder.scan()
        checked = []
        self.rewrite(paths[0], b"changed")
        pins, read = self.scan_counting(builder.BuildOptions(check=checked.append))
        self.assertEqual((pins, read), ({self.cid_of("dir")}, 3))
        self.assertEqual(len(checked), 3)
        self.rewrite(paths[0], b"changed again")
        checked.clear()
        self.assertEqual(self.scan_counting(builder.BuildOptions(check=checked.append)), ({self.cid_of("dir")}, 1))
        self.assertEqual(sorted(checked), sorted(hashlib.sha256(data).digest()
                                                 for data in (b"changed again", b"file 1", b"file 2")))

    def test_a_denied_block_under_a_remembered_file_is_refused(self):
        """A file one of whose blocks is denied since is read again, so storing that block refuses the import."""
        paths = self.tree(3)
        self.folder.scan()
        denied = Cid.for_block(RAW, b"file 1")
        self.storage.deny = mock.Mock(cids=frozenset({denied.binary}), denies=lambda cid: cid == denied)
        self.rewrite(paths[0], b"changed")
        with mock.patch("rnfs.content.shared_folder.log"):
            pins, read = self.scan_counting()
        self.assertEqual(read, 2)
        self.assertEqual(pins, set())

    def test_a_block_gone_under_a_remembered_file_is_read_again(self):
        """A remembered file whose block was deleted (damaged), or collected once its entry's pin went, is read
        again and stored."""
        paths = self.tree(3)
        (first,) = self.folder.scan()
        self.storage._delete({Cid.for_block(RAW, b"file 1")})
        self.rewrite(paths[0], b"changed")
        pins, read = self.scan_counting()
        self.assertEqual((pins, read), ({self.cid_of("dir")}, 2))
        self.assertTrue(self.storage.is_complete(self.cid_of("dir")))
        (second,) = pins
        self.storage.unpin(second, FOLDER)  # as `rnfs gc` after the pin went: every block of it is collected
        self.storage.gc()
        self.assertFalse(self.storage.is_complete(Cid.for_block(RAW, b"file 2")))
        self.rewrite(paths[0], b"changed again")
        pins, read = self.scan_counting()
        self.assertEqual((pins, read), ({self.cid_of("dir")}, 3))
        self.assertTrue(self.storage.is_complete(self.cid_of("dir")))
        self.assertNotEqual(first, second)

    def test_a_file_written_while_read_is_not_remembered(self):
        """A file that changed while it was read may have given neither version: it is read again next time."""
        paths = self.tree(2)
        build_file = builder.build_file
        before = builder.file_state(os.stat(paths[0]))

        def writing(stream, *args, **kwargs):
            """build_file, appending to f0 once it is read."""
            built = build_file(stream, *args, **kwargs)
            if built.cid == Cid.for_block(RAW, b"file 0"):
                with open(paths[0], "ab") as f:
                    f.write(b" and more")
            return built

        with mock.patch.object(builder, "build_file", side_effect=writing):
            self.folder.scan()
        self.assertIsNone(self.folder.files.get(before))
        self.assertIsNotNone(self.folder.files.get(builder.file_state(os.stat(paths[1]))))
        self.assertEqual(self.scan_counting(), ({self.cid_of("dir")}, 1))

    def test_removed_entries_are_forgotten(self):
        """The files of an entry removed, or of one changed, that it no longer holds, are forgotten."""
        paths = self.tree(2)
        other = self.tree(1, "other")
        self.folder.scan()
        states = [builder.file_state(os.stat(path)) for path in [*paths, *other]]
        os.remove(paths[0])
        os.remove(other[0])
        os.rmdir(os.path.join(self.files, "other"))
        self.folder.scan()
        self.assertEqual([self.folder.files.get(state) is not None for state in states], [False, True, False])

    def test_only_fixed_chunks_are_remembered(self):
        """What files were built as stands only for the folder's chunker (fixed): a scan with another reads all."""
        paths = self.tree(2)
        self.folder.scan(builder.BuildOptions(chunker.FASTCDC))
        self.assertIsNone(self.folder.files.get(builder.file_state(os.stat(paths[0]))))

    def file_rows(self) -> dict:
        """What the shared_folder_files table holds: (entry name, state) -> (CID, height, digest)."""
        return {(statedb.load(name), tuple(statedb.load(state))): (Cid.from_bytes(cid), height, digest)
                for name, state, cid, height, digest in state_rows(
                    self.state, "SELECT name, state, cid, height, digest FROM shared_folder_files")}

    def test_the_file_cache_survives_a_restart(self):
        """What files were built as is saved (by save(), with the node's batched stores), and a restart keeps it;
        rows follow the entries: a file replaced or removed, and an entry removed, take their rows with them."""
        paths = self.tree(3)
        hashing = builder.BuildOptions(check=lambda digest: None)
        self.folder.scan(hashing)
        self.assertEqual(self.file_rows(), {})  # saved by save(), not by the scan
        self.folder.files.save()
        states = [builder.file_state(os.stat(path)) for path in paths]
        self.assertEqual(self.file_rows(), {("dir", state): (Cid.for_block(RAW, f"file {i}".encode()), 0,
                                                             hashlib.sha256(f"file {i}".encode()).digest())
                                            for i, state in enumerate(states)})
        self.folder.close()
        self.folder = self.open()
        self.rewrite(paths[0], b"changed")
        self.assertEqual(self.scan_counting(hashing), ({self.cid_of("dir")}, 1))
        self.folder.files.save()
        rows = self.file_rows()
        self.assertEqual(len(rows), 3)
        self.assertNotIn(("dir", states[0]), rows)
        self.assertIn(("dir", builder.file_state(os.stat(paths[0]))), rows)
        os.remove(paths[1])
        self.folder.scan(hashing)
        self.folder.files.save()
        self.assertEqual(len(self.file_rows()), 2)
        for path in (paths[0], paths[2]):
            os.remove(path)
        os.rmdir(os.path.dirname(paths[0]))
        self.folder.scan(hashing)
        self.folder.files.save()
        self.assertEqual(self.file_rows(), {})

    def test_damaged_file_rows_are_deleted(self):
        """File rows that don't parse are skipped at load and deleted; the others are kept."""
        (path,) = self.tree(1)
        self.folder.scan()
        self.folder.files.save()
        ((name, state), (cid, _, _)), = self.file_rows().items()
        good = statedb.dump(list(state))
        bad = [("not json", good, cid.binary, 0, None), ("7", good, cid.binary, 0, None),
               ('"x"', "not json", cid.binary, 0, None), ('"x"', "[1, 2]", cid.binary, 0, None),
               ('"x"', "[1, 2, -3, 4, false]", cid.binary, 0, None), ('"x"', "[true, 2, 3, 4, false]", cid.binary, 0, None),
               ('"x"', "[1, 2, 3, 4, 1]", cid.binary, 0, None), ('"a"', good, b"not a cid", 0, None),
               ('"b"', good, cid.binary, -1, None), ('"c"', good, cid.binary, "zero", None),
               ('"d"', good, cid.binary, 0, b"short")]
        for row in bad:
            state_execute(self.state, "INSERT INTO shared_folder_files (name, state, cid, height, digest) "
                                      "VALUES (?, ?, ?, ?, ?)", row)
        with mock.patch("rnfs.content.shared_folder.log"):
            folder = self.open()
        self.assertEqual(set(self.file_rows()), {(name, state)})
        self.assertEqual(folder.files.get(builder.file_state(os.stat(path))), Built(cid, 6, 0, 0))

    def test_failed_file_save_is_written_by_the_next(self):
        """A save whose commit failed raises, keeps the changes, and the next save writes them."""
        self.tree(2)
        self.folder.scan()
        with mock.patch.object(self.folder.files._rows.db, "transaction",
                               side_effect=statedb.StateDbError("disk I/O error")):
            with self.assertRaises(OSError):
                self.folder.files.save()
        self.assertEqual(self.file_rows(), {})
        self.folder.files.save()
        self.assertEqual(len(self.file_rows()), 2)

    def test_file_entry_names_that_are_not_unicode_round_trip(self):
        """An entry name with a lone surrogate is saved and loaded as it was."""
        files = FolderFiles(self.state)
        self.addCleanup(files.close)
        state = (1, 2, 1, 4, False)
        built = Built(Cid.for_block(RAW, b"x"), 1, 0, 0, hashlib.sha256(b"x").digest())
        files.update("caf\udce9", {state: built}, complete=True)
        files.save()
        loaded = FolderFiles(self.state)
        self.addCleanup(loaded.close)
        self.assertEqual(loaded._entries, {"caf\udce9": {state: built}})
        self.assertEqual(loaded.get(state).digest, built.digest)

    def test_creates_missing_directory(self):
        self.assertEqual(self.folder.scan(), set())
        self.assertTrue(os.path.isdir(self.files))

    def test_imports_and_pins_top_level_entries(self):
        self.write("a.txt", b"a")
        self.write("dir/b.txt", b"b")
        self.write("dir/sub/.c", b"c")
        pins = self.folder.scan()
        self.assertEqual(pins, {self.cid_of("a.txt"), self.cid_of("dir")})
        self.assertEqual(self.storage.pins(), {cid: {FOLDER} for cid in pins})
        for cid in pins:
            self.assertTrue(self.storage.is_complete(cid))  # copied into the block store

    def test_unchanged_entries_are_not_rehashed(self):
        self.write("a.txt", b"a")
        self.write("dir/b.txt", b"b")
        first = self.folder.scan()
        rehashed = AssertionError("re-hashed")
        with mock.patch.object(builder, "build_open", side_effect=rehashed), \
                mock.patch.object(builder, "build_file", side_effect=rehashed):
            self.assertEqual(self.folder.scan(), first)
            self.assertEqual(self.open().scan(), first)  # the cache survives a restart

    def test_changed_entry_releases_old_pin(self):
        path = self.write("dir/b.txt", b"b")
        old = self.folder.scan()
        self.write("dir/b.txt", b"changed")
        os.utime(path, ns=(0, 0))  # mtime alone could tie within the filesystem's resolution
        new = self.folder.scan()
        self.assertEqual(new, {self.cid_of("dir")})
        self.assertNotEqual(new, old)
        self.assertEqual(set(self.storage.pins()), new)

    def test_removed_entry_releases_pin_but_not_other_sources(self):
        path = self.write("a.txt", b"a")
        (cid,) = self.folder.scan()
        self.storage.pin(cid, USER)
        os.remove(path)
        self.assertEqual(self.folder.scan(), set())
        self.assertEqual(self.storage.pins(), {cid: {USER}})

    def test_identical_entries_share_a_pin(self):
        a = self.write("a", b"same")
        self.write("b", b"same")
        (cid,) = self.folder.scan()
        os.remove(a)
        self.assertEqual(self.folder.scan(), {cid})
        self.assertEqual(self.storage.pins(), {cid: {FOLDER}})

    @unittest.skipUnless(CAN_SYMLINK, "creating symlinks isn't allowed here")
    def test_skips_symlinks(self):
        target = self.write("real", b"r")
        os.symlink(target, os.path.join(self.files, "link"))
        self.assertEqual(self.folder.scan(), {self.cid_of("real")})

    def test_reimports_when_blocks_are_gone(self):
        self.write("a.txt", b"a")
        (cid,) = self.folder.scan()
        self.storage._delete({cid})
        self.assertEqual(self.folder.scan(), {cid})
        self.assertTrue(self.storage.is_complete(cid))

    def test_stale_folder_pins_released_at_startup(self):
        stale = builder.build_bytes(b"gone", self.storage.put).cid
        self.storage.pin(stale, FOLDER)
        self.storage.pin(stale, USER)
        self.write("a.txt", b"a")
        self.assertEqual(self.open().scan(), {self.cid_of("a.txt")})
        self.assertEqual(self.storage.pins()[stale], {USER})

    @unittest.skipUnless(POSIX_MODES, "the execute bit (POSIX)")
    def test_signature_changes_with_content_metadata(self):
        path = self.write("dir/x", b"1")
        before = shared_folder.signature(os.path.join(self.files, "dir"))
        os.chmod(path, 0o755)
        self.assertNotEqual(shared_folder.signature(os.path.join(self.files, "dir")), before)

    def test_windows_signature_ignores_execute_bit(self):
        # Windows' stat shows an execute bit for .exe files; add ignores it there, so the signature must too
        path = self.write("dir/x", b"1")
        with mock.patch.object(fsutil, "IS_WINDOWS", True):
            before = shared_folder.signature(os.path.join(self.files, "dir"))
            os.chmod(path, 0o755)
            self.assertEqual(shared_folder.signature(os.path.join(self.files, "dir")), before)

    def test_scan_by_path_without_fd_walk(self):
        # How Windows scans: directories walked by path, every entry's stat checked before it is used
        self.write("a.txt", b"a")
        self.write("dir/sub/b.txt", b"b")
        with mock.patch.object(builder, "FD_WALK", False):
            pins = self.folder.scan()
        self.assertEqual(pins, {self.cid_of("a.txt"), self.cid_of("dir")})
        self.assertEqual(self.open().scan(), pins)  # the same signatures either way


    def secret(self) -> str:
        """A file outside the shared folder that no swap may get imported."""
        path = os.path.join(self.tmp.name, "secret")
        with open(path, "wb") as f:
            f.write(SECRET)
        return path

    def assert_secret_not_imported(self):
        """The secret's block is nowhere in storage."""
        self.assertFalse(self.storage.has(Cid.for_block(RAW, SECRET)))

    @unittest.skipUnless(CAN_SYMLINK, "creating symlinks isn't allowed here")
    def test_top_level_file_swapped_for_symlink_is_not_imported(self):
        secret = self.secret()
        victim = self.write("victim", b"harmless")

        def swap():
            """Replace the entry with a symlink to the secret, after the scan checked it."""
            os.remove(victim)
            os.symlink(secret, victim)

        with swap_on_open("victim", swap):
            self.assertEqual(self.folder.scan(), set())
        self.assert_secret_not_imported()
        self.assertEqual(self.folder.scan(), set())  # now a plain symlink: skipped
        self.assert_secret_not_imported()

    @unittest.skipUnless(CAN_SYMLINK and builder.FD_WALK, "directories opened by descriptor (POSIX), symlinks")
    def test_nested_swaps_are_not_imported(self):
        secret = self.secret()
        outside = os.path.join(self.tmp.name, "outside")
        os.mkdir(outside)
        os.link(secret, os.path.join(outside, "key"))
        file_victim = self.write("dir/file", b"harmless")
        dir_victim = os.path.join(self.files, "dir", "sub")
        os.mkdir(dir_victim)

        def swap_file():
            """Replace a nested file with a symlink to the secret."""
            os.remove(file_victim)
            os.symlink(secret, file_victim)

        def swap_dir():
            """Replace a nested directory with a symlink to one outside the folder."""
            os.rmdir(dir_victim)
            os.symlink(outside, dir_victim)

        with swap_on_open("file", swap_file), swap_on_open("sub", swap_dir):
            self.assertEqual(self.folder.scan(), set())  # sub swapped while the signature was read: skipped
            self.assert_secret_not_imported()
            self.folder.scan()  # file swapped while being imported: left out
            self.assert_secret_not_imported()
        self.assertTrue(os.path.islink(file_victim) and os.path.islink(dir_victim))
        self.assertEqual(self.folder.scan(), {self.cid_of("dir")})  # both are symlinks now, skipped
        self.assert_secret_not_imported()

    @unittest.skipUnless(HAS_FIFO, "FIFOs (POSIX)")
    def test_fifos_never_block_the_scan(self):
        os.makedirs(os.path.join(self.files, "dir"))
        os.mkfifo(os.path.join(self.files, "fifo"))  # noqa: TID251 - skipped without HAS_FIFO
        os.mkfifo(os.path.join(self.files, "dir", "fifo"))  # noqa: TID251 - skipped without HAS_FIFO
        victim = self.write("victim", b"harmless")

        def swap():
            """Replace a file with a FIFO nobody writes to, after the scan checked it."""
            os.remove(victim)
            os.mkfifo(victim)  # noqa: TID251 - skipped without HAS_FIFO

        with swap_on_open("victim", swap):
            pins = within(5, self.folder.scan)
        self.assertEqual(pins, {self.cid_of("dir")})
        self.assertEqual(within(5, self.folder.scan), pins)


class NodeFileCacheTest(unittest.TestCase):
    """The node saves the shared folder's per-file cache with its other stores (D1, XC5)."""

    def test_saved_at_stop_and_used_after_a_restart(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        config = Config(rnfs_dir=os.path.join(tmp.name, "rnfs"), slow="no")
        paths = []
        for i in range(3):
            path = os.path.join(config.files_dir, "dir", f"f{i}")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(f"file {i}".encode())
            paths.append(path)
        node = RnfsNode.offline(config, A_ID, NetClient(), FakeNetwork())
        self.addCleanup(node.stop)
        self.assertIn(node.shared_folder.files, node._every_store(batched=True))
        node.maintain(time.monotonic(), {})
        self.assertEqual(state_rows(config.state_dir, "SELECT COUNT(*) FROM shared_folder_files"), [(0,)])
        node.stop()
        self.assertEqual(state_rows(config.state_dir, "SELECT COUNT(*) FROM shared_folder_files"), [(3,)])
        st = os.stat(paths[0])
        with open(paths[0], "wb") as f:
            f.write(b"changed")
        os.utime(paths[0], ns=(st.st_atime_ns, st.st_mtime_ns + 10**9))
        node = RnfsNode.offline(config, A_ID, NetClient(), FakeNetwork())
        self.addCleanup(node.stop)
        with mock.patch.object(builder, "build_file", wraps=builder.build_file) as build_file:
            node.maintain(time.monotonic(), {})
        self.assertEqual(build_file.call_count, 1)
        self.assertEqual(node.storage.pinned_by(FOLDER),
                         {builder.add_path(os.path.join(config.files_dir, "dir"), builder.NO_PUT)[0].cid})


if __name__ == "__main__":
    unittest.main()
