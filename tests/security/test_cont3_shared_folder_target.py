"""CONT-3: the shared folder's scan follows a symlink at files/ (the owner's setting), and imported whatever it led
to. Pointed at (or, through a parent another user can write, retargeted to) the rnfs dir's storage/ or names/, it
pinned and provided the node's identity or the name keys (anyone who knew their CIDs could fetch them); pointed at
the rnfs dir or a directory above it, everything in it.

Now the scan resolves the folder it opened (fsutil.final_path of its descriptor, or the real path the path walk
checks) and refuses, with a warning shown once, a folder that is the rnfs dir, a directory holding it, or one inside
storage/, names/, state/, blocks/ or lxmf/. It is scanned as an empty folder: nothing is imported, and every pin
with source "folder" is released, including those a 0.1.5 node made from such a folder before the upgrade. A folder
below a directory other users can change gets a warning, once, and is scanned.

Reproduced with real symlinks in a temporary rnfs dir laid out as the node lays it out; the retarget is the
attacker's rename-and-link done between two scans.
"""
import os
import tempfile
import time
import unittest
from unittest import mock

from rnfs import fsutil
from rnfs.config import Config
from rnfs.content import builder, shared_folder
from rnfs.content.cid import RAW, Cid
from rnfs.content.shared_folder import SharedFolder
from rnfs.content.storage import FOLDER, Storage
from rnfs.node import RnfsNode
from tests.support import CAN_SYMLINK, POSIX_MODES, state_rows
from tests.test_node import A_ID, FakeNetwork, NetClient

IDENTITY = b"the node's private RNS identity\n"
NAME_KEY = b"a name's private signing key\n"
PRIVATE = ("storage", "names", "state", "blocks", "lxmf")


@unittest.skipUnless(CAN_SYMLINK, "creating symlinks isn't allowed here")
class SharedFolderTargetTest(unittest.TestCase):
    """An rnfs dir holding an identity and a name key, its storage, and files/ as a symlink the test points."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = os.path.join(self.tmp.name, "rnfs")
        for name in PRIVATE:
            os.makedirs(os.path.join(self.root, name))
        with open(os.path.join(self.root, "storage", "identity"), "wb") as f:
            f.write(IDENTITY)
        with open(os.path.join(self.root, "names", "0" * 32 + ".key"), "wb") as f:
            f.write(NAME_KEY)
        self.files = os.path.join(self.root, "files")
        self.state = os.path.join(self.root, "state")
        self.storage = Storage(os.path.join(self.root, "blocks"), self.state, 10**9)
        self.addCleanup(self.storage.close)
        self.logged: list[str] = []
        patcher = mock.patch.object(shared_folder, "log", lambda message, level=None: self.logged.append(message))
        patcher.start()
        self.addCleanup(patcher.stop)

    def open(self) -> SharedFolder:
        """A SharedFolder over files/, closed when the test ends."""
        folder = SharedFolder(self.files, self.storage, self.state)
        self.addCleanup(folder.close)
        return folder

    def point_at(self, target: str):
        """Make files/ a symlink to target (replacing the one there)."""
        if os.path.lexists(self.files):
            os.remove(self.files)
        os.symlink(target, self.files, target_is_directory=True)

    def directory(self, *parts: str, content: bytes = b"shared") -> str:
        """A directory below the temporary one holding a.txt with content."""
        path = os.path.join(self.tmp.name, *parts)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, "a.txt"), "wb") as f:
            f.write(content)
        return path

    def assert_refused(self, folder: SharedFolder, keys_stored: bool = False):
        """A scan imports nothing and leaves no folder pin and no saved entry (as an empty folder's scan does), the
        keys stay unstored (unless keys_stored), and one warning names the rnfs dir and the released pins."""
        before = len(self.logged)
        self.assertEqual(folder.scan(), set())
        self.assertEqual(self.storage.pinned_by(FOLDER), set())
        self.assertEqual(folder.names(), {})
        self.assertEqual(state_rows(self.state, "SELECT name FROM shared_folder"), [])
        if not keys_stored:
            for secret in (IDENTITY, NAME_KEY):
                self.assertFalse(self.storage.is_complete(Cid.for_block(RAW, secret)))
        warnings = self.logged[before:]
        self.assertEqual(len(warnings), 1, warnings)
        self.assertIn("not scanning", warnings[0])
        self.assertIn("released", warnings[0])
        self.assertIn(os.path.realpath(self.root), warnings[0])

    def test_private_subdirectories_are_refused(self):
        """files/ leading to storage/, names/, state/, blocks/ or lxmf/, or a directory inside one, is refused."""
        os.makedirs(os.path.join(self.root, "names", "deeper"))
        for target in [*PRIVATE, os.path.join("names", "deeper")]:
            with self.subTest(target):
                self.point_at(os.path.join(self.root, target))
                self.assert_refused(self.open())

    def test_rnfs_dir_and_ancestors_are_refused(self):
        """files/ leading to the rnfs dir itself or to a directory above it is refused."""
        for target in (self.root, self.tmp.name):
            with self.subTest(target):
                self.point_at(target)
                self.assert_refused(self.open())

    def test_warned_once(self):
        """Scans of a refused folder warn once while it stays refused, and again if it is refused after a good
        scan."""
        self.point_at(os.path.join(self.root, "storage"))
        folder = self.open()
        self.assert_refused(folder)
        self.assertEqual(folder.scan(), set())
        self.assertEqual(len(self.logged), 1)
        self.point_at(self.directory("elsewhere"))
        self.assertEqual(len(folder.scan()), 1)
        self.point_at(os.path.join(self.root, "names"))
        self.assert_refused(folder)

    def test_retargeted_folder_releases_its_pins_and_imports_nothing(self):
        """The audit's attack: files/ leads to shared/x, which someone who can write to shared/ swaps for a link to
        names/. The next scan imports nothing and releases what the folder shared, as for an empty folder."""
        target = self.directory("shared", "x", content=b"the owner's shared file")
        with open(os.path.join(target, "b.txt"), "wb") as f:
            f.write(b"another")
        self.point_at(target)
        folder = self.open()
        pins = folder.scan()
        self.assertEqual(pins, {Cid.for_block(RAW, b"the owner's shared file"), Cid.for_block(RAW, b"another")})
        self.assertEqual(self.storage.pinned_by(FOLDER), pins)
        os.rename(target, target + ".moved")
        os.symlink(os.path.join(self.root, "names"), target, target_is_directory=True)
        self.logged.clear()
        self.assert_refused(folder)

    def test_file_cache_sits_behind_the_guard(self):
        """The per-file cache (audit D1) is consulted only after the folder passed the check: a retargeted folder's
        scan looks nothing up in it, and forgets what it held, so going back reads every file again."""
        target = self.directory("shared", "x", content=b"the owner's shared file")
        os.makedirs(os.path.join(target, "sub"))
        with open(os.path.join(target, "sub", "b.txt"), "wb") as f:
            f.write(b"another")
        self.point_at(target)
        folder = self.open()
        self.assertEqual(len(folder.scan()), 2)
        moved = target + ".moved"
        os.rename(target, moved)
        os.symlink(os.path.join(self.root, "names"), target, target_is_directory=True)
        self.logged.clear()
        with mock.patch.object(shared_folder.FolderFiles, "get", side_effect=AssertionError("cache consulted")):
            self.assert_refused(folder)
        self.assertEqual(folder.files._index, {})
        os.remove(target)
        os.rename(moved, target)
        with mock.patch.object(builder, "build_file", wraps=builder.build_file) as build_file:
            self.assertEqual(len(folder.scan()), 2)
        self.assertEqual(build_file.call_count, 2)

    def test_upgrade_releases_keys_an_earlier_scan_imported(self):
        """A 0.1.5 node whose files/ already led into names/ imported, pinned and provided the name key. The first
        0.1.6 scan (over the same state) releases that pin and empties the shared_folder table."""
        self.point_at(os.path.join(self.root, "names"))
        earlier = self.open()
        # As 0.1.5 scanned: neither the path check nor (REV1) the device-and-inode one
        with mock.patch.object(shared_folder, "refused_reason", lambda real, rnfs_dir: None), \
                mock.patch.object(shared_folder, "private_dirs", lambda rnfs_dir: frozenset()):
            imported = earlier.scan()
        key = Cid.for_block(RAW, NAME_KEY)
        self.assertEqual(imported, {key})
        self.assertEqual(self.storage.pinned_by(FOLDER), {key})
        earlier.close()
        upgraded = self.open()
        self.assertEqual(set(upgraded.names()), {key})  # the saved entry, as 0.1.5 left it
        self.assert_refused(upgraded, keys_stored=True)
        self.assertNotIn(key, self.storage.pins())

    def test_elsewhere_is_scanned(self):
        """A folder outside the rnfs dir (or files/ as a real directory in it) is scanned as before."""
        self.point_at(self.directory("elsewhere"))
        self.assertEqual(self.open().scan(), {Cid.for_block(RAW, b"shared")})
        os.remove(self.files)
        os.mkdir(self.files)
        with open(os.path.join(self.files, "b.txt"), "wb") as f:
            f.write(b"in files")
        self.assertEqual(self.open().scan(), {Cid.for_block(RAW, b"in files")})

    def test_refused_without_the_descriptor_path(self):
        """Where the system can't name an open directory's path (fsutil.final_path gives None), the folder's real
        path is used, checked to be the directory opened."""
        with mock.patch.object(fsutil, "final_path", lambda fd: None):
            self.point_at(os.path.join(self.root, "names"))
            self.assert_refused(self.open())
            self.logged.clear()
            self.point_at(self.directory("elsewhere"))
            self.assertEqual(len(self.open().scan()), 1)

    def test_refused_by_the_path_walk(self):
        """Where directories are walked by path (FD_WALK False, as on Windows), the same folders are refused."""
        with mock.patch.object(builder, "FD_WALK", False):
            self.point_at(os.path.join(self.root, "storage"))
            self.assert_refused(self.open())
            self.logged.clear()
            self.point_at(self.directory("elsewhere"))
            self.assertEqual(len(self.open().scan()), 1)

    @unittest.skipUnless(POSIX_MODES, "Unix permission bits")
    def test_loose_ancestor_is_warned_about_once(self):
        """A folder below a directory others can write to is scanned, with one warning naming that directory; a
        sticky one (as /tmp) holding a directory of the user's own isn't."""
        loose = os.path.join(self.tmp.name, "loose")
        target = self.directory("loose", "x")
        os.chmod(loose, 0o777)
        self.point_at(target)
        folder = self.open()
        self.assertEqual(len(folder.scan()), 1)
        self.assertEqual(len(folder.scan()), 1)
        warnings = [line for line in self.logged if "other users" in line]
        self.assertEqual(len(warnings), 1, self.logged)
        self.assertIn(os.path.realpath(loose), warnings[0])
        self.logged.clear()
        sticky = os.path.join(self.tmp.name, "sticky")
        target = self.directory("sticky", "x")
        os.chmod(sticky, 0o1777)
        self.point_at(target)
        self.assertEqual(len(self.open().scan()), 1)
        self.assertEqual([line for line in self.logged if "other users" in line], [])


@unittest.skipUnless(CAN_SYMLINK, "creating symlinks isn't allowed here")
class NodeSharedFolderTargetTest(unittest.TestCase):
    """The node's maintenance scan refuses a files/ leading into its own rnfs dir."""

    def test_names_dir_not_shared(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        config = Config(rnfs_dir=os.path.join(tmp.name, "node"), slow="no")
        node = RnfsNode.offline(config, A_ID, NetClient(), FakeNetwork())
        self.addCleanup(node.stop)
        with open(os.path.join(config.names_dir, "0" * 32 + ".key"), "wb") as f:
            f.write(NAME_KEY)
        if os.path.lexists(config.files_dir):
            os.rmdir(config.files_dir)
        os.symlink(config.names_dir, config.files_dir, target_is_directory=True)
        with mock.patch.object(shared_folder, "log"):
            node.maintain(time.monotonic(), {})
        self.assertEqual(node.storage.pinned_by(FOLDER), set())
        self.assertFalse(node.storage.is_complete(Cid.for_block(RAW, NAME_KEY)))


if __name__ == "__main__":
    unittest.main()
