"""Tests for the node identity file: created owner-only and atomically, kept across restarts, loose permissions fixed."""
import os
import stat
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs import fsutil
from rnfs.network import identity as identity_module
from rnfs.network.identity import load_or_create_identity, node_id_for_identity
from tests.support import POSIX_MODES


def mode_of(path: str) -> int:
    """The permission bits of path."""
    return stat.S_IMODE(os.stat(path).st_mode)


class IdentityFileTest(unittest.TestCase):
    """load_or_create_identity under a temporary directory."""

    def setUp(self):
        """A path under a fresh directory that doesn't exist yet, and a permissive umask."""
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = os.path.join(directory.name, "storage")
        self.path = os.path.join(self.directory, "identity")
        # A umask that lets group and others read, like the common 002 / 022 defaults
        previous = os.umask(0o002)
        self.addCleanup(os.umask, previous)

    def test_created_owner_only(self):
        load_or_create_identity(self.path)
        if POSIX_MODES:
            self.assertEqual(mode_of(self.path), 0o600)
        self.assertEqual(os.listdir(self.directory), ["identity"])  # no temp file left behind

    def test_saved_without_fchmod(self):
        # os.fchmod doesn't exist on Windows before Python 3.13; mkstemp's 0600 needs no chmod anyway
        with mock.patch("os.fchmod", create=True, side_effect=AssertionError("fchmod called")):
            created = load_or_create_identity(self.path)
        self.assertEqual(load_or_create_identity(self.path).hash, created.hash)

    def test_same_identity_after_restart(self):
        created = load_or_create_identity(self.path)
        loaded = load_or_create_identity(self.path)
        self.assertEqual(node_id_for_identity(created), node_id_for_identity(loaded))
        # Still RNS's raw format
        self.assertEqual(RNS.Identity.from_file(self.path).hash, created.hash)

    @unittest.skipUnless(POSIX_MODES, "Unix permission bits")
    def test_loose_permissions_restricted_on_load(self):
        os.makedirs(self.directory)
        identity = RNS.Identity()
        identity.to_file(self.path)  # as older versions saved it
        os.chmod(self.path, 0o664)
        loaded = load_or_create_identity(self.path)
        self.assertEqual(loaded.hash, identity.hash)
        self.assertEqual(mode_of(self.path), 0o600)

    def test_no_permission_fix_on_windows(self):
        # Windows' st_mode always shows group and others allowed: no chmod or warning at every start
        load_or_create_identity(self.path)
        with mock.patch.object(fsutil, "IS_WINDOWS", True), mock.patch("os.chmod") as chmod, \
                mock.patch.object(identity_module, "log") as log, \
                mock.patch("os.stat", return_value=mock.Mock(st_mode=0o100666)):
            identity_module._restrict_permissions(self.path)
        chmod.assert_not_called()
        log.assert_not_called()

    def test_identity_created_meanwhile_is_kept(self):
        # Another process starting on the same directory saves its identity between our check and our save
        os.makedirs(self.directory)
        theirs = RNS.Identity()
        identity_module.save_identity(theirs, self.path)
        real_isfile = os.path.isfile
        calls = []

        def isfile(path):
            """Not there yet at the first check, as when the other process saves just after it."""
            calls.append(path)
            return False if len(calls) == 1 else real_isfile(path)
        with mock.patch("os.path.isfile", isfile):
            loaded = load_or_create_identity(self.path)
        self.assertEqual(loaded.hash, theirs.hash)
        self.assertEqual(RNS.Identity.from_file(self.path).hash, theirs.hash)
        self.assertEqual(os.listdir(self.directory), ["identity"])  # our temporary file is gone

    def test_new_identity_is_durable(self):
        with mock.patch.object(fsutil, "sync_directory") as sync:
            load_or_create_identity(self.path)
        sync.assert_called_once_with(os.path.abspath(self.directory))

    def test_unreadable_identity_not_overwritten(self):
        os.makedirs(self.directory)
        with open(self.path, "wb") as f:
            f.write(b"not a key")
        with self.assertRaises(RuntimeError):
            load_or_create_identity(self.path)
        with open(self.path, "rb") as f:
            self.assertEqual(f.read(), b"not a key")


if __name__ == "__main__":
    unittest.main()
