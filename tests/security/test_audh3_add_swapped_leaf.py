"""AUD-H3 (a REV1 residual): `add` checked the path it was given against the rnfs dir (shared_folder.add_refused_reason,
on its real path), then opened it again (builder.open_path, following a link at its end). Whoever can write the
directory being added from, such as an app holding a scoped token that asks for an add of a folder of its own, could
swap the path's last component for a symlink into the rnfs dir between the two: the CLI's api_token, the node identity
or a name key was then added, pinned and provided, and the app could cat it. A thread flipping the name between a file
and a symlink won the race in a few dozen tries.

The race is rebuilt deterministically: builder.open_path, the open that follows the check, is wrapped so that the
first call swaps the path for a symlink just before opening it. A file swapped for a link to api_token (plain, wrap,
only-hash and sealed adds) and a directory swapped for a link to a directory inside a private subdirectory, or to one
holding the rnfs dir, used to be added.

Now the check is made again on what was opened, by the code holding it (builder.check_root calls the
BuildOptions.root_check add_path gives: shared_folder.opened_refused_reason, on the real path of the open file or
directory, its own final path where the system can name it, shared_folder.opened_path), for file and directory roots,
plain, sealed and only-hash adds alike: the add is refused with the same reasons, and nothing of the rnfs dir is
stored.
"""
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs import fsutil
from rnfs.content import builder
from rnfs.content.cid import RAW, Cid
from rnfs.node import RnfsNode
from tests.support import CAN_SYMLINK
from tests.test_node import A_ID, FakeNetwork, NetClient
from tests.test_seal import ALICE

TOKEN = b"the CLI's api_token, 64 hex characters of it\n"
SECRET = b"a key kept in a subdirectory of the node's storage\n"
SHARED = b"a harmless file the app asked to add\n"


@unittest.skipUnless(CAN_SYMLINK, "creating symlinks isn't allowed here")
class AddSwappedLeafTest(unittest.TestCase):
    """An offline node whose rnfs dir is <home>/.rnfs, with an api_token and a secret in storage/keys/; an app's
    folder <tmp>/app holding x, which it swaps for a symlink between the check and the open."""

    def setUp(self):
        """The node, its secrets and the app's folder."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        self.home = os.path.join(self.tmp.name, "home")
        config = Config(rnfs_dir=os.path.join(self.home, ".rnfs"), slow="no")
        self.node = RnfsNode.offline(config, A_ID, NetClient(), FakeNetwork())
        self.node.identity = ALICE
        self.addCleanup(self.node.stop)
        self.root = config.root
        self.token = self.write(os.path.join(self.root, "api_token"), TOKEN)
        self.keys = os.path.dirname(self.write(os.path.join(self.root, "storage", "keys", "secret"), SECRET))
        self.app = os.path.join(self.tmp.name, "app")
        os.makedirs(self.app)
        self.x = os.path.join(self.app, "x")

    @staticmethod
    def write(path: str, data: bytes) -> str:
        """Write data to path, making its directories; path."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
        return path

    def remove_link(self):
        """Remove the symlink at self.x (one to a directory is removed with os.rmdir on Windows)."""
        if os.name == "nt" and os.path.isdir(self.x):
            os.rmdir(self.x)
        else:
            os.remove(self.x)

    def swapping(self, target: str):
        """builder.open_path, but its first call first swaps self.x (a file or directory) for a symlink to target:
        what the app does once the check has passed."""
        real = builder.open_path
        swapped = []

        def open_path(path, *args, **kwargs):
            """Swap, once, then open as builder.open_path does."""
            if not swapped:
                swapped.append(path)
                if os.path.isdir(self.x):
                    os.rmdir(self.x)
                else:
                    os.remove(self.x)
                os.symlink(target, self.x, target_is_directory=os.path.isdir(target))
            return real(path, *args, **kwargs)

        return mock.patch.object(builder, "open_path", open_path)

    def assert_refused(self, target: str, **options):
        """add_path(self.x), x swapped for a link to target after the check, is refused (ValueError) and stores
        neither secret. The link is removed afterwards."""
        try:
            with self.swapping(target), self.assertRaises(ValueError) as caught:
                self.node.add_path(self.x, **options)
        finally:
            self.remove_link()
        self.assertIn("rnfs", str(caught.exception))
        for data in (TOKEN, SECRET):
            self.assertFalse(self.node.storage.is_complete(Cid.for_block(RAW, data)), data)

    def test_a_file_swapped_for_a_link_to_the_api_token_is_refused(self):
        for options in ({}, {"wrap": True}, {"only_hash": True}, {"to_self": True}):
            with self.subTest(**options):
                self.write(self.x, SHARED)
                self.assert_refused(self.token, **options)

    def test_a_directory_swapped_for_a_link_inside_a_private_subdirectory_is_refused(self):
        """storage/keys isn't one of the directories a walk skips (private_dirs): only the path check knows it."""
        for options in ({}, {"only_hash": True}, {"to_self": True}):
            with self.subTest(**options):
                os.makedirs(self.x)
                self.assert_refused(self.keys, **options)

    def test_a_directory_swapped_for_a_link_to_one_holding_the_rnfs_dir_is_refused(self):
        """As `add ~` is: refused as a whole, not added without the rnfs dir."""
        self.write(os.path.join(self.home, "notes.txt"), SHARED)
        os.makedirs(self.x)
        self.assert_refused(self.home)
        self.assertFalse(self.node.storage.is_complete(Cid.for_block(RAW, SHARED)))

    def test_refused_where_the_system_cant_name_an_open_file(self):
        """Without fsutil.final_path (None: no /proc, no F_GETPATH), the open file's real path comes from its path,
        checked to lead to the file opened still (shared_folder.opened_path)."""
        with mock.patch.object(fsutil, "final_path", lambda fd: None):
            self.write(self.x, SHARED)
            self.assert_refused(self.token)
            os.makedirs(self.x)
            self.assert_refused(self.keys)

    def test_a_file_not_swapped_is_added(self):
        self.write(self.x, SHARED)
        self.assertEqual(self.node.add_path(self.x)["cid"], str(Cid.for_block(RAW, SHARED)))
        self.assertTrue(self.node.storage.is_complete(Cid.for_block(RAW, SHARED)))

    def test_a_link_into_the_shared_folder_may_still_be_added(self):
        """What is in files/ is shared anyway, however the add reached it."""
        shared = self.write(os.path.join(self.root, "files", "doc.txt"), SHARED)
        self.write(self.x, b"something else")
        with self.swapping(shared):
            self.assertEqual(self.node.add_path(self.x)["cid"], str(Cid.for_block(RAW, SHARED)))


if __name__ == "__main__":
    unittest.main()
