"""Tests for the shared folder (specification section 8): entries imported and pinned with source "folder"."""
import os
import tempfile
import unittest
from unittest import mock

from rnfs.content import builder, shared_folder
from rnfs.content.shared_folder import SharedFolder
from rnfs.content.storage import FOLDER, USER, Storage


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
        with mock.patch.object(builder, "add_path", side_effect=AssertionError("re-hashed")):
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

    def test_signature_changes_with_content_metadata(self):
        path = self.write("dir/x", b"1")
        before = shared_folder.signature(os.path.join(self.files, "dir"))
        os.chmod(path, 0o755)
        self.assertNotEqual(shared_folder.signature(os.path.join(self.files, "dir")), before)


if __name__ == "__main__":
    unittest.main()
