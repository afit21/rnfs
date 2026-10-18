"""I2-R1 (fix): an identity file that was a symlink, belonged to another user or wasn't a regular file was still
used: a symlink (or one belonging to someone else) was loaded with at most a warning, so whoever could put it there
chose the node's key, and a FIFO made the start hang, as RNS read it by path with a blocking open. The key was
also read by path again after the permission check, so a file swapped in between was the one loaded. Now each
of these stops the node with a clear error (IdentityFileError, an RnfsError, so the daemon reports it without a
traceback), and the key is read from the O_NOFOLLOW descriptor that was checked.

Reproduced with real files, symlinks and a FIFO in a temporary directory (POSIX); "another user" is an
os.geteuid that answers a different uid, and the swap happens in the os.fchmod that tightens a loose file.
"""
import os
import tempfile
import threading
import unittest
from unittest import mock

import RNS

from rnfs.errors import RnfsError
from rnfs.network import identity as identity_module
from rnfs.network.identity import load_or_create_identity
from tests.support import CAN_SYMLINK, HAS_FIFO, POSIX_MODES


@unittest.skipUnless(POSIX_MODES, "Unix permission bits and owners")
class IdentityFileTest(unittest.TestCase):
    """storage/identity under a temporary directory, and a key file of someone else's next to it."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        os.mkdir(os.path.join(self.tmp.name, "storage"))
        self.path = os.path.join(self.tmp.name, "storage", "identity")
        self.theirs = RNS.Identity()
        self.their_file = os.path.join(self.tmp.name, "their-key")
        self.theirs.to_file(self.their_file)
        log = mock.patch.object(identity_module, "log")
        self.log = log.start()
        self.addCleanup(log.stop)

    def load(self):
        """load_or_create_identity, which must stop the node with an RnfsError (the daemon reports it as a
        message, not a traceback); its message."""
        with self.assertRaises(RnfsError) as caught:
            load_or_create_identity(self.path)
        self.assertIsInstance(caught.exception, identity_module.IdentityFileError)
        return str(caught.exception)

    @unittest.skipUnless(CAN_SYMLINK, "symlinks")
    def test_symlink_stops_the_node(self):
        os.symlink(self.their_file, self.path)
        self.assertIn("is a symlink", self.load())

    @unittest.skipUnless(CAN_SYMLINK, "symlinks")
    def test_dangling_symlink_stops_the_node(self):
        """A symlink to nothing is refused as a symlink too (it used to end in "Could not load identity")."""
        os.symlink(os.path.join(self.tmp.name, "nothing"), self.path)
        self.assertIn("is a symlink", self.load())
        self.assertFalse(os.path.exists(os.path.join(self.tmp.name, "nothing")))

    def test_identity_of_another_user_stops_the_node(self):
        os.replace(self.their_file, self.path)
        os.chmod(self.path, 0o600)
        with mock.patch("os.geteuid", return_value=os.geteuid() + 1):
            self.assertIn("belongs to another user", self.load())

    @unittest.skipUnless(HAS_FIFO, "FIFOs")
    def test_fifo_stops_the_node_without_hanging(self):
        os.mkfifo(self.path, 0o600)
        outcome = []

        def start():
            try:
                load_or_create_identity(self.path)
                outcome.append("loaded")
            except Exception as e:  # whatever it is, the test reports it
                outcome.append(e)

        thread = threading.Thread(target=start, daemon=True)
        thread.start()
        thread.join(5)
        if thread.is_alive():
            # Unblock the old code's blocking open: a writer that closes at once gives it an empty read
            os.close(os.open(self.path, os.O_WRONLY | os.O_NONBLOCK))
            thread.join(5)
            self.fail("the start hung opening the FIFO at the identity path")
        [result] = outcome
        self.assertIsInstance(result, RnfsError)
        self.assertIn("not a regular file", str(result))

    def test_key_is_read_from_the_checked_descriptor(self):
        """A file swapped in at the path right after the check (here: as the loose file is tightened) is not
        the one loaded."""
        mine = RNS.Identity()
        mine.to_file(self.path)
        os.chmod(self.path, 0o644)  # loose, so the check tightens it
        real_fchmod = os.fchmod

        def fchmod_then_swap(fd, mode):
            real_fchmod(fd, mode)
            os.replace(self.their_file, self.path)

        with mock.patch("os.fchmod", fchmod_then_swap):
            loaded = load_or_create_identity(self.path)
        self.assertEqual(loaded.hash, mine.hash, "loaded the key swapped in after the check")

    def test_failed_tighten_stops_the_node(self):
        RNS.Identity().to_file(self.path)
        os.chmod(self.path, 0o644)
        with mock.patch("os.fchmod", side_effect=PermissionError(1, "Operation not permitted")):
            self.assertIn("can't be restricted", self.load())

    def test_own_private_identity_still_loads(self):
        created = load_or_create_identity(self.path)
        self.assertEqual(load_or_create_identity(self.path).hash, created.hash)
        self.log.assert_not_called()


if __name__ == "__main__":
    unittest.main()
