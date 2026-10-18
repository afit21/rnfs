"""L7-R2 (fix): on Windows an rnfs dir that was a junction or symlink was accepted: its access list was checked and
tightened on the target, wherever that is, and whoever could change the link could point the node elsewhere. Now
the daemon refuses a root that is a junction, symlink or other link-like reparse point, before setting anything.

Simulated with fakes on any platform: fsutil.IS_WINDOWS is patched and winacl.backend is test_l7_windows_acl's
FakeAcl. A symlink root is a real symlink (where the platform can make one); a junction is an os.lstat result
carrying IO_REPARSE_TAG_MOUNT_POINT, as Python reports one on Windows. Not run on Windows.
"""
import os
import stat
import unittest
from unittest import mock

from rnfs import node as node_module
from rnfs.errors import RnfsError
from tests.security.test_l7_windows_acl import WindowsTestCase
from tests.support import CAN_SYMLINK

JUNCTION = 0xA0000003  # IO_REPARSE_TAG_MOUNT_POINT


class LinkedRootTest(WindowsTestCase):
    """node._make_private_dirs on Windows with a root that links elsewhere."""

    def setUp(self):
        super().setUp()
        self.target = os.path.join(self.tmp.name, "elsewhere")
        os.mkdir(self.target)
        self.root = os.path.join(self.tmp.name, "rnfs")
        for patch in (mock.patch("rnfs.log.log"), mock.patch.dict(os.environ, {"USERPROFILE": self.tmp.name})):
            patch.start()
            self.addCleanup(patch.stop)

    def assert_refused(self):
        """The start is refused, and nothing is set or created through the link."""
        with self.assertRaisesRegex(RnfsError, "is a junction or symlink"):
            node_module._make_private_dirs(self.root)
        self.assertEqual(self.acl.set_calls, [], "the target's DACL was changed through the link")
        self.assertEqual(os.listdir(self.target), [], "rnfs created its directories in the link's target")

    @unittest.skipUnless(CAN_SYMLINK, "symlinks")
    def test_symlink_root_is_refused(self):
        os.symlink(self.target, self.root, target_is_directory=True)
        self.assert_refused()

    def test_junction_root_is_refused(self):
        os.mkdir(self.root)  # stands for the junction; lstat says what it is
        real_lstat = os.lstat

        def lstat(path, *args, **kwargs):
            result = real_lstat(path, *args, **kwargs)
            if os.path.abspath(path) != self.root:
                return result
            return mock.Mock(st_mode=stat.S_IFDIR | 0o777, st_reparse_tag=JUNCTION, st_dev=result.st_dev,
                             st_ino=result.st_ino)

        with mock.patch("os.lstat", lstat):
            with self.assertRaisesRegex(RnfsError, "is a junction or symlink"):
                node_module._make_private_dirs(self.root)
        self.assertEqual(self.acl.set_calls, [], "the target's DACL was changed through the junction")
        self.assertEqual(os.listdir(self.root), [], "rnfs created its directories through the junction")

    def test_onedrive_placeholder_is_not_a_link(self):
        """Reparse points that hold the directory itself (no name-surrogate bit, e.g. cloud files) are fine."""
        os.mkdir(self.root)
        real_lstat = os.lstat

        def lstat(path, *args, **kwargs):
            result = real_lstat(path, *args, **kwargs)
            if os.path.abspath(path) != self.root:
                return result
            return mock.Mock(st_mode=stat.S_IFDIR | 0o777, st_reparse_tag=0x9000001A)  # IO_REPARSE_TAG_CLOUD

        with mock.patch("os.lstat", lstat):
            node_module._make_private_dirs(self.root)
        self.assertTrue(self.private(self.root))


if __name__ == "__main__":
    unittest.main()
