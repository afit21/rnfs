"""REV1: `add` had no check against the rnfs dir. The shared folder and `name sync` refuse a folder leading into it
(shared_folder.refused_reason, CONT-3), but RnfsNode.add_path took any path: `rnfs add ~` pinned and provided the
node identity, the name keys, api_token and rnfs.db, each findable by its own CID with provide_entries, and an app
token could read the CLI's token through it (add <rnfs dir>/api_token, then cat it).

Now `add` (the node's add_path, so the CLI and the API; add jobs, before they are queued; sealed folders) refuses a
path refused_reason rejects (the rnfs dir, a directory holding it, anything inside its private subdirectories) and
anything else inside the rnfs dir but outside its shared folder (files/): api_token, the Reticulum config, rnfs.toml.
And every walk (add, the shared folder, name sync, the packing of a sealed folder) skips, with a warning, a directory
that is the rnfs dir or one of its private subdirectories by (st_dev, st_ino), taken when the walk starts: that also
catches another way to them, such as a bind mount, which the path check can't see. An add whose root is such a
directory is refused.

Bind mounts need root (or a user namespace) to make, so one is simulated: while the add runs, os.stat of the rnfs
dir (or of names/) reports the stat of an ordinary directory elsewhere, which is what a bind mount of it there looks
like (the same device and inode at both paths).
"""
import contextlib
import os
import tempfile
import time
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.content import shared_folder
from rnfs.content.cid import RAW, Cid
from rnfs.content.storage import FOLDER
from rnfs.name_sync import SETTLE_TIME
from rnfs.node import RnfsNode
from tests.support import CAN_SYMLINK, FakeClock
from tests.test_node import A_ID, FakeNetwork, NetClient
from tests.test_seal import ALICE

TOKEN = b"the CLI's api_token, 64 hex characters of it\n"
NAME_KEY = b"a name's private signing key\n"
IDENTITY = b"the node's private RNS identity\n"
SHARED = b"a file meant for sharing\n"


class AddRnfsDirTest(unittest.TestCase):
    """An offline node whose rnfs dir is <home>/.rnfs, holding an api_token, a name key and an identity file."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        self.home = os.path.join(self.tmp.name, "home")
        self.config = Config(rnfs_dir=os.path.join(self.home, ".rnfs"), slow="no")
        self.node = RnfsNode.offline(self.config, A_ID, NetClient(), FakeNetwork())
        self.node.identity = ALICE
        self.addCleanup(self.node.stop)
        self.root = self.config.root
        self.write(os.path.join(self.root, "api_token"), TOKEN)
        self.write(os.path.join(self.config.names_dir, "0" * 32 + ".key"), NAME_KEY)
        self.write(os.path.join(self.root, "storage", "secret"), IDENTITY)
        self.write(os.path.join(self.home, "notes", "a.txt"), SHARED)

    @staticmethod
    def write(path: str, data: bytes) -> str:
        """Write data to path, making its directories; path."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
        return path

    def assert_not_stored(self, *contents: bytes):
        """None of contents is in the block store."""
        for data in contents:
            self.assertFalse(self.node.storage.is_complete(Cid.for_block(RAW, data)), data)

    def assert_refused(self, path: str, **options):
        """add_path(path) is refused (ValueError) and stores none of the rnfs dir's secrets."""
        with self.assertRaises(ValueError) as caught:
            self.node.add_path(path, **options)
        self.assertIn("rnfs", str(caught.exception))
        self.assert_not_stored(TOKEN, NAME_KEY, IDENTITY)

    @contextlib.contextmanager
    def bound(self, bind: str, target: str):
        """While in the block, the directory bind is the same directory as target, as `mount --bind target bind`
        makes it: os.stat of target reports bind's stat (device and inode), wherever it is asked."""
        real_stat = os.stat
        targets = {os.path.normcase(os.path.abspath(target)), os.path.normcase(os.path.realpath(target))}

        def stat(path, *args, **kwargs):
            """os.stat, but bind's for target."""
            if (isinstance(path, str) and kwargs.get("dir_fd") is None
                    and os.path.normcase(os.path.abspath(path)) in targets):
                return real_stat(bind)
            return real_stat(path, *args, **kwargs)

        with mock.patch("os.stat", stat):
            yield

    # The path check: the rnfs dir, what holds it, what is inside it

    def test_the_rnfs_dir_is_refused(self):
        self.assert_refused(self.root)

    def test_a_directory_holding_it_is_refused(self):
        """`rnfs add ~`: refused as a whole (as a shared folder at ~ is), not added without the rnfs dir."""
        self.assert_refused(self.home)
        self.assert_refused(self.tmp.name)
        self.assert_not_stored(SHARED)

    def test_only_hash_and_wrap_are_refused_too(self):
        self.assert_refused(self.home, only_hash=True)
        self.assert_refused(self.home, wrap=True)

    def test_inside_a_private_subdirectory_is_refused(self):
        self.assert_refused(self.config.names_dir)
        self.assert_refused(os.path.join(self.config.names_dir, "0" * 32 + ".key"))
        self.assert_refused(os.path.join(self.root, "storage"))

    def test_the_api_token_is_refused(self):
        """What an app token could do to read the CLI's token: add it, then cat it."""
        self.assert_refused(os.path.join(self.root, "api_token"))

    def test_the_shared_folder_may_be_added(self):
        """What is in files/ is shared anyway: adding it is allowed."""
        path = self.write(os.path.join(self.config.files_dir, "doc.txt"), SHARED)
        self.assertEqual(self.node.add_path(path)["cid"], str(Cid.for_block(RAW, SHARED)))

    def test_a_sealed_folder_holding_it_is_refused(self):
        self.assert_refused(self.home, to_self=True)

    @unittest.skipUnless(CAN_SYMLINK, "creating symlinks isn't allowed here")
    def test_a_symlink_to_it_is_refused(self):
        link = os.path.join(self.tmp.name, "link")
        os.symlink(self.root, link, target_is_directory=True)
        self.assert_refused(link)
        above = os.path.join(self.tmp.name, "above")
        os.symlink(self.home, above, target_is_directory=True)
        self.assert_refused(above)

    def test_an_add_job_is_refused_before_it_is_queued(self):
        jobs = self.node.feature("jobs")
        with self.assertRaises(ValueError):
            jobs.submit("add", path=self.home)
        self.assertEqual(jobs.list(), [])

    # The walk: another way to the rnfs dir (a bind mount)

    def bind_of(self, target: str, *where: str) -> str:
        """A directory at where (below the test's directory) standing in for a bind mount of target: what target
        holds that the test checks for (api_token, or the name key) is there too."""
        bind = os.path.join(self.tmp.name, *where)
        os.makedirs(bind, exist_ok=True)
        if target == self.root:
            self.write(os.path.join(bind, "api_token"), TOKEN)
        else:
            self.write(os.path.join(bind, "0" * 32 + ".key"), NAME_KEY)
        return bind

    def test_add_skips_a_bind_mount_of_the_rnfs_dir(self):
        """`rnfs add /srv` where /srv/mnt is the rnfs dir mounted again: the add goes on, without it."""
        bind = self.bind_of(self.root, "srv", "mnt")
        self.write(os.path.join(self.tmp.name, "srv", "b.txt"), SHARED)
        with self.bound(bind, self.root):
            result = self.node.add_path(os.path.join(self.tmp.name, "srv"))
        self.assert_not_stored(TOKEN)
        self.assertTrue(self.node.storage.is_complete(Cid.for_block(RAW, SHARED)))
        self.assertTrue(any("mnt" in warning and "rnfs" in warning for warning in result["warnings"]),
                        result["warnings"])

    def test_add_skips_a_bind_mount_of_a_private_subdirectory(self):
        bind = self.bind_of(self.config.names_dir, "srv", "keys")
        with self.bound(bind, self.config.names_dir):
            result = self.node.add_path(os.path.join(self.tmp.name, "srv"))
        self.assert_not_stored(NAME_KEY)
        self.assertTrue(any("keys" in warning for warning in result["warnings"]), result["warnings"])

    def test_add_of_a_bind_mount_itself_is_refused(self):
        bind = self.bind_of(self.root, "srv", "mnt")
        with self.bound(bind, self.root):
            self.assert_refused(bind)
            self.assert_refused(bind, to_self=True)

    def test_a_file_in_another_name_for_the_rnfs_dir_is_refused(self):
        """REL-8: a case variant of the rnfs dir on a case-insensitive disk (macOS: ~/.RNFS/api_token) or a bind mount of
        it passed the path check, and a file added as the add's root isn't walked, so an app could add the CLI's token
        (or the node identity, or a name key) and cat it. Its directories are compared by inode too."""
        bind = self.bind_of(self.root, "srv", "RNFS")
        with self.bound(bind, self.root):
            self.assert_refused(os.path.join(bind, "api_token"))
        names = self.bind_of(self.config.names_dir, "srv", "NAMES")
        with self.bound(names, self.config.names_dir):
            self.assert_refused(os.path.join(names, "0" * 32 + ".key"))

    def test_a_file_in_another_name_for_files_may_be_added(self):
        shared = os.path.join(self.tmp.name, "srv", "FILES")
        self.write(os.path.join(shared, "a.txt"), SHARED)
        with self.bound(shared, os.path.join(self.root, "files")):
            self.node.add_path(os.path.join(shared, "a.txt"))

    def test_a_sealed_folder_skips_a_bind_mount(self):
        bind = self.bind_of(self.root, "srv", "mnt")
        self.write(os.path.join(self.tmp.name, "srv", "b.txt"), SHARED)
        with self.bound(bind, self.root):
            result = self.node.add_path(os.path.join(self.tmp.name, "srv"), to_self=True)
        tar = b"".join(self.node.read(result["cid"]))
        self.assertNotIn(TOKEN, tar)
        self.assertIn(SHARED, tar)
        self.assertTrue(any("mnt" in warning for warning in result["warnings"]), result["warnings"])

    def test_the_shared_folder_skips_a_bind_mount(self):
        """A bind mount in files/, at its top level or deeper, isn't imported; the rest is."""
        files = self.config.files_dir
        os.makedirs(files, exist_ok=True)
        top = self.bind_of(self.root, files, "mnt")
        deeper = self.bind_of(self.config.names_dir, files, "docs", "keys")
        self.write(os.path.join(files, "docs", "b.txt"), SHARED)
        logged: list[str] = []
        with self.bound(top, self.root), self.bound(deeper, self.config.names_dir), \
                mock.patch.object(shared_folder, "log", lambda message, level=None: logged.append(message)):
            self.node.maintain(time.monotonic(), {})
        self.assert_not_stored(TOKEN, NAME_KEY)
        self.assertTrue(self.node.storage.is_complete(Cid.for_block(RAW, SHARED)))
        self.assertEqual(len(self.node.storage.pinned_by(FOLDER)), 1)  # docs/, without keys/
        self.assertTrue(any("mnt" in line for line in logged), logged)
        self.assertTrue(any("keys" in line for line in logged), logged)

    def test_name_sync_skips_a_bind_mount(self):
        bind = self.bind_of(self.root, "site", "mnt")
        self.write(os.path.join(self.tmp.name, "site", "index.html"), SHARED)
        wall, mono = FakeClock(1_700_000_000.0), FakeClock(1000.0)
        self.node.folder_syncs.clock, self.node.folder_syncs.monotonic = wall, mono
        self.node.name_create("site")
        self.node.name_sync("site", os.path.join(self.tmp.name, "site"))
        with self.bound(bind, self.root), mock.patch.object(shared_folder, "log"):
            for step in (0, SETTLE_TIME):
                wall.now += step
                mono.now += step
                self.node.folder_syncs.tick()
        self.assert_not_stored(TOKEN)
        self.assertTrue(self.node.storage.is_complete(Cid.for_block(RAW, SHARED)))

    def test_name_sync_of_a_bind_mount_itself_is_refused(self):
        bind = self.bind_of(self.root, "site")
        wall, mono = FakeClock(1_700_000_000.0), FakeClock(1000.0)
        self.node.folder_syncs.clock, self.node.folder_syncs.monotonic = wall, mono
        self.node.name_create("site")
        with self.bound(bind, self.root), mock.patch.object(shared_folder, "log"):
            try:
                self.node.name_sync("site", bind)
            except ValueError:
                pass  # refused at once: as good
            else:
                self.node.folder_syncs.tick()
        self.assert_not_stored(TOKEN)


if __name__ == "__main__":
    unittest.main()
