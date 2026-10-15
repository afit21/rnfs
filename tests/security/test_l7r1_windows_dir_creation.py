"""L7-R1 (fix): on Windows a new --dir outside the user's profile was created with its parent's access list and only
then given the private one, so in between another local account could put a file in it (an rnfs.toml whose
reticulum_dir points at a Reticulum config with a PipeInterface, which runs a program, or an identity), and the
settings and identity files were used without checking who owns them. Now the rnfs dir and its subdirectories are
created with the private DACL in the CreateDirectoryW call itself, a new root that already holds anything is
refused, rnfs.toml and the identity are checked (owner and DACL, through the handle they are read from) before
use, and a DACL that can't be tightened stops rnfs instead of being a warning.

Simulated with fakes on any platform, as test_l7_windows_acl does: fsutil.IS_WINDOWS is patched and winacl.backend
is its FakeAcl (a DACL per file; create_private_dir makes the directory with the private DACL). The race is
stood in for by an os.mkdir that, right after making the root, writes an rnfs.toml into it, as another account
would. The security descriptor given to CreateDirectoryW is built in Python and checked byte for byte here; the
real CreateDirectoryW call through ctypes is not run: none of this has been run on Windows.
"""
import os
import struct
import unittest
from unittest import mock

from rnfs import node as node_module, winacl
from rnfs.config import SETTINGS_FILE, Config
from rnfs.errors import RnfsError
from rnfs.winacl import Dacl
from tests.security.test_l7_windows_acl import OTHER, USER, WindowsTestCase, allow

PLANTED = 'reticulum_dir = "C:/Users/Public/evil-reticulum"\n'


class NewRnfsDirTest(WindowsTestCase):
    """node._make_private_dirs for a --dir that doesn't exist yet."""

    def setUp(self):
        super().setUp()
        self.root = os.path.join(self.tmp.name, "rnfs")
        for patch in (mock.patch("rnfs.log.log"), mock.patch.dict(os.environ, {"USERPROFILE": self.tmp.name})):
            patch.start()
            self.addCleanup(patch.stop)

    def test_root_and_subdirectories_are_created_private(self):
        """Each directory has the private DACL from the call that creates it: none is ever set afterwards
        (the old code made the root with os.makedirs, then set its DACL)."""
        node_module._make_private_dirs(Config(rnfs_dir=self.root))
        expected = [self.root] + [os.path.join(self.root, name) for name in ("state", "blocks", "storage", "names")]
        self.assertEqual(self.acl.set_calls, [], "a DACL was set after the directory was created with its "
                                                 "parent's access list")
        self.assertEqual(self.acl.created, expected)
        for directory in expected:
            self.assertTrue(self.private(directory), directory)

    def test_new_root_that_already_holds_a_file_is_refused(self):
        """Someone who wins the race (here: an os.mkdir that plants rnfs.toml once the root exists) gets the
        start refused, not their settings used."""
        real_mkdir = os.mkdir

        def mkdir_then_plant(path, *args, **kwargs):
            real_mkdir(path, *args, **kwargs)
            if os.path.abspath(path) == os.path.abspath(self.root):
                with open(os.path.join(path, SETTINGS_FILE), "w") as f:
                    f.write(PLANTED)

        with mock.patch("os.mkdir", mkdir_then_plant):
            with self.assertRaisesRegex(RnfsError, "was just created, yet it already holds 1 entries"):
                node_module._make_private_dirs(Config(rnfs_dir=self.root))
        self.assertEqual(sorted(os.listdir(self.root)), [SETTINGS_FILE], "rnfs went on to use the planted dir")

    def test_trailing_separator(self):
        """The default dir is given as "~/.rnfs/": its parent is the profile, not the dir itself."""
        node_module._make_private_dirs(Config(rnfs_dir=self.root + os.sep))
        self.assertEqual(self.acl.created[0], self.root)


class DescriptorTest(unittest.TestCase):
    """The self-relative security descriptor CreateDirectoryW gets."""

    def test_private_descriptor(self):
        descriptor = winacl.private_descriptor(USER, True)
        revision, _, control, owner, group, sacl, dacl = struct.unpack_from("<BBHIIII", descriptor)
        self.assertEqual(revision, 1)
        self.assertEqual(control, winacl.SE_SELF_RELATIVE | winacl.SE_DACL_PRESENT | winacl.SE_DACL_PROTECTED)
        self.assertEqual((owner, group, sacl), (0, 0, 0))  # the creator's default owner, no group or SACL
        self.assertEqual(dacl, 20)
        self.assertEqual(descriptor[dacl:], winacl.private_acl(USER, True))


class SettingsFileTest(WindowsTestCase):
    """Config.load on Windows checks rnfs.toml's owner and DACL through the handle it reads."""

    def setUp(self):
        super().setUp()
        self.root = os.path.join(self.tmp.name, "rnfs")
        os.mkdir(self.root)
        self.path = os.path.join(self.root, SETTINGS_FILE)
        with open(self.path, "w") as f:
            f.write(PLANTED)

    def test_settings_of_another_account_are_refused(self):
        self.acl.set(self.path, Dacl(OTHER, [allow(OTHER), allow(USER)]))
        with self.assertRaisesRegex(PermissionError, f"can't trust the settings file: .*belongs to {OTHER}"):
            Config.load(self.root)

    def test_settings_others_may_access_are_tightened_with_a_warning(self):
        with mock.patch("rnfs.log.log") as log:
            config = Config.load(self.root)
        self.assertEqual(config.reticulum_dir, "C:/Users/Public/evil-reticulum")  # read, after the warning
        self.assertTrue(self.private(self.path), "the settings file was used without making it private")
        self.assertIn("Check that its settings are yours", log.call_args.args[0])
        self.assertIsNotNone(self.acl.read_fds[0], "the DACL checked must be the opened file's")

    def test_settings_that_cant_be_tightened_are_refused(self):
        with mock.patch.object(self.acl, "set_private", side_effect=PermissionError(5, "Access is denied")):
            with self.assertRaisesRegex(PermissionError, "can't trust the settings file"):
                Config.load(self.root)

    def test_private_settings_load_quietly(self):
        self.acl.set(self.path, Dacl(USER, [allow(USER), allow(winacl.SYSTEM)]))
        with mock.patch("rnfs.log.log") as log:
            Config.load(self.root)
        log.assert_not_called()
        self.assertEqual(self.acl.set_calls, [])


if __name__ == "__main__":
    unittest.main()
