"""I2 (fix): the identity file's permission tightening followed symlinks, so a symlink put at the identity path got
another file's permissions changed. Now a symlink there is left alone (with a warning) and a regular file is changed
through a descriptor opened with O_NOFOLLOW.

load_or_create_identity is run on a symlink to a loose file holding a key (it still loads, the target's mode is
unchanged), on a loose regular file (still tightened), and with the path swapped for a symlink between the lstat
and the open (the open refuses it, nothing is changed).
"""
import os
import stat
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.network import identity as identity_module
from rnfs.network.identity import load_or_create_identity
from tests.support import CAN_SYMLINK, POSIX_MODES


def mode_of(path: str) -> int:
    """The permission bits of path (following a symlink)."""
    return stat.S_IMODE(os.stat(path).st_mode)


@unittest.skipUnless(POSIX_MODES and CAN_SYMLINK, "Unix permission bits and symlinks")
class IdentitySymlinkTest(unittest.TestCase):
    """storage/identity under a temporary directory, and another file of the user's outside it."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        os.mkdir(os.path.join(self.tmp.name, "storage"))
        self.path = os.path.join(self.tmp.name, "storage", "identity")
        self.other = os.path.join(self.tmp.name, "elsewhere")
        self.identity = RNS.Identity()
        self.identity.to_file(self.other)
        os.chmod(self.other, 0o644)

    def test_symlink_target_is_not_changed(self):
        os.symlink(self.other, self.path)
        with mock.patch.object(identity_module, "log") as log:
            loaded = load_or_create_identity(self.path)
        self.assertEqual(loaded.hash, self.identity.hash)  # still the user's choice to link it
        self.assertEqual(mode_of(self.other), 0o644, "the permission fix followed the symlink")
        self.assertIn("is a symlink", log.call_args.args[0])

    def test_loose_regular_file_is_still_tightened(self):
        os.rename(self.other, self.path)
        with mock.patch.object(identity_module, "log"):
            load_or_create_identity(self.path)
        self.assertEqual(mode_of(self.path), 0o600)

    def test_symlink_swapped_in_after_the_check_is_not_followed(self):
        os.symlink(self.other, self.path)
        regular = os.stat(self.other)  # what lstat would have seen before the swap
        with mock.patch("os.lstat", return_value=regular), mock.patch.object(identity_module, "log") as log:
            identity_module._restrict_permissions(self.path)
        self.assertEqual(mode_of(self.other), 0o644, "the permission fix followed the symlink")
        self.assertIn("could not restrict", log.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
