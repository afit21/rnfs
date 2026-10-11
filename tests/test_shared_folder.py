"""Tests for the shared folder (specification section 8): entries imported and pinned with source "folder"."""
import os
import tempfile
import unittest
from unittest import mock

from rnfs import fsutil
from rnfs.content import builder, shared_folder
from rnfs.content.cid import RAW, Cid
from rnfs.content.shared_folder import SharedFolder
from rnfs.content.storage import FOLDER, USER, Storage
from tests.support import CAN_SYMLINK, HAS_FIFO, POSIX_MODES
from tests.test_builder import swap_on_open, within

SECRET = b"private key"


class SharedFolderTest(unittest.TestCase):
    """Scanning a temporary shared folder as entries are added, changed and removed."""

    def setUp(self):
        """Storage and a shared folder over a temporary directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.files = os.path.join(self.tmp.name, "files")
        self.storage = Storage(os.path.join(self.tmp.name, "blocks"), os.path.join(self.tmp.name, "state"), 10**9)
        self.folder = self.open()

    def open(self) -> SharedFolder:
        """A SharedFolder with its state under the temporary directory."""
        return SharedFolder(self.files, self.storage, os.path.join(self.tmp.name, "state"))

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
