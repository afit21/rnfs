"""Tests for the shared folder (specification section 8): entries imported and pinned with source "folder"."""
import os
import tempfile
import threading
import unittest
from unittest import mock

from rnfs import fsutil, statedb
from rnfs.content import builder, shared_folder
from rnfs.content.cid import RAW, Cid
from rnfs.content.shared_folder import SharedFolder
from rnfs.content.storage import FOLDER, USER, Storage
from tests.support import CAN_SYMLINK, HAS_FIFO, POSIX_MODES, state_execute, state_rows, write_legacy
from tests.test_builder import swap_on_open, within

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
        """A failure of the content itself (here ValueError) isn't tried again, however long, until it changes."""
        clock = [100.0]
        self.folder.clock = lambda: clock[0]
        self.write("a.txt", b"a")
        with mock.patch.object(builder, "build_open", side_effect=ValueError("bad")), \
                mock.patch("rnfs.content.shared_folder.log"):
            self.folder.scan()
        clock[0] += 100 * shared_folder.RETRY_AFTER
        with mock.patch.object(builder, "build_open", side_effect=AssertionError("tried again")):
            self.assertEqual(self.folder.scan(), set())
        self.write("a.txt", b"changed")
        self.assertEqual(self.folder.scan(), {self.cid_of("a.txt")})

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
        os.mkfifo(os.path.join(self.files, "fifo"))
        os.mkfifo(os.path.join(self.files, "dir", "fifo"))
        victim = self.write("victim", b"harmless")

        def swap():
            """Replace a file with a FIFO nobody writes to, after the scan checked it."""
            os.remove(victim)
            os.mkfifo(victim)

        with swap_on_open("victim", swap):
            pins = within(5, self.folder.scan)
        self.assertEqual(pins, {self.cid_of("dir")})
        self.assertEqual(within(5, self.folder.scan), pins)


if __name__ == "__main__":
    unittest.main()
