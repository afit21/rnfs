"""API-1 (subdirectories): on POSIX, the rnfs dir's subdirectories that already existed (state/, blocks/, storage/,
names/, lxmf/) were never checked: makedirs(exist_ok=True) took a symlink to someone else's directory, kept a 0777
blocks/ as it was and used one another user owned. Tightening the root to 0700 doesn't take back access to them: a
user who could once write in a loose rnfs dir can keep a directory descriptor open, or own a subdirectory they
created, and plant raw blocks (trusted by their length) or swap name keys.

Now each is opened with O_DIRECTORY | O_NOFOLLOW (created 0700 if missing) and checked through that descriptor: a
symlink, a non-directory or one of another user's stops the start with an RnfsError naming chown, before Reticulum
starts; a loose one is made 0700 with a notice, and if others could write to it, its entries owned by another user
are reported (a warning only). The start runs in the order root, settings, subdirectories, lock, in the daemon and
in rnfs.Node. On Windows an existing subdirectory gets the private access list too.

Reproduced with real directories and symlinks in a temporary directory (POSIX); "another user" is an os.fstat or
os.lstat that answers a different uid for one directory or file. The Windows case runs on any platform through the
fake winacl backend of test_l7_windows_acl.
"""
import contextlib
import io
import os
import stat
import tempfile
import unittest
from unittest import mock

from rnfs import cli, embedded, node as node_module
from rnfs.config import LOCK_FILE, SETTINGS_FILE, Config
from rnfs.errors import RnfsError
from rnfs.node import RnfsNode, lock_rnfs_dir
from tests.security.test_l7_windows_acl import SHARED, WindowsTestCase
from tests.support import CAN_SYMLINK, POSIX_MODES

# Not lxmf/ since rnfs 0.2.0: LXMF's router state is left as it is for the LXMF app (test_lxmf_dir_is_left_alone)
SUBDIRS = ("state", "blocks", "storage", "names")


def foreign(path: str, real):
    """A stat function (real: os.fstat or os.lstat) that says another user owns the file at path."""
    target = os.lstat(path)

    def fake(*args, **kwargs):
        info = real(*args, **kwargs)
        if (info.st_dev, info.st_ino) != (target.st_dev, target.st_ino):
            return info
        fields = list(info[:10])
        fields[4] = os.geteuid() + 1  # noqa: TID251 - st_uid; used only by classes skipped without POSIX_MODES
        return os.stat_result(fields)
    return fake


@unittest.skipUnless(POSIX_MODES, "Unix permission bits and owners")
class SubdirectoryTest(unittest.TestCase):
    """node._make_private_dirs and the node's start over a temporary rnfs dir, and an attacker's directory."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = os.path.join(tmp.name, "rnfs")
        os.mkdir(self.root, 0o700)
        self.attacker = os.path.join(tmp.name, "attacker")
        os.mkdir(self.attacker, 0o777)
        os.chmod(self.attacker, 0o777)
        log = mock.patch.object(node_module, "log")
        self.log = log.start()
        self.addCleanup(log.stop)

    def mode(self, path: str) -> int:
        """path's permission bits."""
        return stat.S_IMODE(os.lstat(path).st_mode)

    def messages(self) -> str:
        """Everything logged."""
        return "\n".join(call.args[0] for call in self.log.call_args_list)

    def start_refused(self) -> str:
        """Starting a node on the rnfs dir, which must stop with an RnfsError before Reticulum starts or
        daemon.lock is made; its message."""
        with mock.patch.object(node_module.RNS, "Reticulum") as reticulum, \
                mock.patch.object(node_module.guards, "install") as guards:
            with self.assertRaises(RnfsError) as caught:
                RnfsNode(Config(rnfs_dir=self.root))
            with self.assertRaises(RnfsError):
                lock_rnfs_dir(Config(rnfs_dir=self.root))
        reticulum.assert_not_called()
        guards.assert_not_called()
        self.assertFalse(os.path.exists(os.path.join(self.root, LOCK_FILE)))
        return str(caught.exception)

    def test_missing_subdirectories_are_created_private(self):
        node_module._make_private_dirs(self.root)
        for name in SUBDIRS:
            self.assertEqual(self.mode(os.path.join(self.root, name)), 0o700, name)
        self.assertEqual(self.messages(), "")

    def test_loose_subdirectories_are_made_private(self):
        for name in SUBDIRS:
            os.mkdir(os.path.join(self.root, name))
            os.chmod(os.path.join(self.root, name), 0o777)
        node_module._make_private_dirs(self.root)
        for name in SUBDIRS:
            self.assertEqual(self.mode(os.path.join(self.root, name)), 0o700, name)
            self.assertIn(f"made {os.path.join(self.root, name)} private to its owner (it had mode 777)",
                          self.messages())

    def test_group_readable_subdirectory_is_made_private_without_a_scan(self):
        """The 0755 of a umask 022: tightened, and as nobody else could write to it, not scanned."""
        blocks = os.path.join(self.root, "blocks")
        os.mkdir(blocks)
        os.chmod(blocks, 0o755)
        with mock.patch("os.walk", side_effect=AssertionError("scanned")):
            node_module._make_private_dirs(self.root)
        self.assertEqual(self.mode(blocks), 0o700)

    def test_lxmf_dir_is_left_alone(self):
        """lxmf/, LXMF's router state before rnfs 0.2.0, is neither created nor changed: an LXMF app adopts it."""
        node_module._make_private_dirs(self.root)
        self.assertFalse(os.path.exists(os.path.join(self.root, "lxmf")))
        lxmf = os.path.join(self.root, "lxmf")
        os.mkdir(lxmf)
        os.chmod(lxmf, 0o750)
        node_module._make_private_dirs(self.root)
        self.assertEqual(self.mode(lxmf), 0o750)
        self.assertNotIn(lxmf, self.messages())

    def test_private_subdirectories_are_left_alone(self):
        node_module._make_private_dirs(self.root)
        self.log.reset_mock()
        node_module._make_private_dirs(self.root)
        self.assertEqual(self.messages(), "")

    @unittest.skipUnless(CAN_SYMLINK, "symlinks")
    def test_symlinked_subdirectory_is_refused(self):
        for name in SUBDIRS:
            with self.subTest(name):
                link = os.path.join(self.root, name)
                os.symlink(self.attacker, link)
                try:
                    self.assertIn("symlink", self.start_refused())
                finally:
                    os.remove(link)
        self.assertEqual(self.mode(self.attacker), 0o777)  # not chmodded through the link
        self.assertEqual(os.listdir(self.attacker), [])

    def test_file_in_place_of_a_subdirectory_is_refused(self):
        with open(os.path.join(self.root, "blocks"), "wb"):
            pass
        message = self.start_refused()
        self.assertIn("not a directory", message)

    def test_subdirectory_of_another_user_is_refused(self):
        names = os.path.join(self.root, "names")
        os.mkdir(names, 0o700)
        with mock.patch("os.fstat", foreign(names, os.fstat)):
            message = self.start_refused()
        self.assertIn("belongs to another user", message)
        self.assertIn("chown", message)

    def test_foreign_entries_of_a_once_writable_subdirectory_are_reported(self):
        blocks = os.path.join(self.root, "blocks")
        os.makedirs(os.path.join(blocks, "ab"))
        planted = os.path.join(blocks, "ab", "planted")
        with open(planted, "wb") as f:
            f.write(b"x")
        with open(os.path.join(blocks, "ab", "mine"), "wb") as f:
            f.write(b"y")
        os.chmod(blocks, 0o775)
        with mock.patch("os.lstat", foreign(planted, os.lstat)):
            node_module._make_private_dirs(self.root)
        self.assertEqual(self.mode(blocks), 0o700)
        warnings = [call.args[0] for call in self.log.call_args_list if call.args[1] == node_module.LOG_WARNING]
        [warning] = warnings
        self.assertIn("1 entry", warning)
        self.assertIn(planted, warning)
        self.assertTrue(os.path.exists(planted))  # reported, not removed

    def test_daemon_checks_in_order(self):
        """The daemon: root, then settings, then subdirectories, then the lock, each before the next."""
        order = []
        real_root, real_load = node_module.make_private_root, Config.load

        def root(path):
            order.append("root")
            real_root(path)

        def load(*args, **kwargs):
            order.append("settings")
            return real_load(*args, **kwargs)

        def lock(config):
            order.append("lock")
            raise RnfsError("stop here")

        def subdirs(path):
            order.append("subdirectories")
            raise RnfsError("stop here")

        with open(os.path.join(self.root, SETTINGS_FILE), "w", encoding="utf-8") as f:
            f.write("api_port = 9999\n")
        os.chmod(os.path.join(self.root, SETTINGS_FILE), 0o666)
        with mock.patch.object(node_module, "make_private_root", root), \
                mock.patch.object(Config, "load", load), \
                mock.patch.object(node_module, "lock_rnfs_dir", lock), \
                contextlib.redirect_stderr(io.StringIO()) as stderr, self.assertRaises(SystemExit):
            cli.main(["--dir", self.root, "daemon"])
        self.assertEqual(order, ["root", "settings", "lock"])
        self.assertIn("rnfs: warning: the settings file", stderr.getvalue())  # printed before the lock
        order.clear()
        with mock.patch.object(node_module, "_make_private_dirs", subdirs), \
                mock.patch.object(node_module, "make_private_root", root), \
                contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            cli.main(["--dir", self.root, "daemon"])
        self.assertEqual(order, ["root", "subdirectories"])
        self.assertFalse(os.path.exists(os.path.join(self.root, LOCK_FILE)))

    def test_embedded_node_checks_in_order_and_logs_warnings(self):
        """rnfs.Node: the same order, and the settings' warnings logged."""
        order = []
        real_root, real_load = node_module.make_private_root, Config.load

        def root(path):
            order.append("root")
            real_root(path)

        def load(*args, **kwargs):
            order.append("settings")
            return real_load(*args, **kwargs)

        def lock(config):
            order.append("lock")
            raise RnfsError("stop here")

        with open(os.path.join(self.root, SETTINGS_FILE), "w", encoding="utf-8") as f:
            f.write("api_port = 9999\n")
        os.chmod(os.path.join(self.root, SETTINGS_FILE), 0o666)
        with mock.patch.object(embedded, "make_private_root", root), mock.patch.object(Config, "load", load), \
                mock.patch.object(embedded, "lock_rnfs_dir", lock), mock.patch.object(embedded, "log") as log:
            with self.assertRaisesRegex(RnfsError, "stop here"):
                embedded.Node(self.root)
        self.assertEqual(order, ["root", "settings", "lock"])
        self.assertIn("the settings file", log.call_args.args[0])
        self.assertEqual(log.call_args.args[1], embedded.LOG_WARNING)


class WindowsSubdirectoryTest(WindowsTestCase):
    """node._make_private_dirs on Windows: an existing subdirectory others may access is made private too."""

    def setUp(self):
        super().setUp()
        self.root = os.path.join(self.tmp.name, "rnfs")
        log = mock.patch("rnfs.log.log")
        self.log = log.start()
        self.addCleanup(log.stop)
        profile = mock.patch.dict(os.environ, {"USERPROFILE": self.tmp.name})
        profile.start()
        self.addCleanup(profile.stop)

    def test_existing_subdirectory_others_can_read_is_tightened(self):
        node_module._make_private_dirs(self.root)
        blocks = os.path.join(self.root, "blocks")
        self.acl.set(blocks, SHARED)  # as an older rnfs left it, inheriting a folder's access list
        node_module._make_private_dirs(self.root)
        self.assertEqual(self.acl.set_calls, [(blocks, True)])
        self.assertTrue(self.private(blocks))

    @unittest.skipUnless(CAN_SYMLINK, "symlinks")
    def test_linked_subdirectory_is_refused(self):
        os.mkdir(self.root)
        elsewhere = os.path.join(self.tmp.name, "elsewhere")
        os.mkdir(elsewhere)
        os.symlink(elsewhere, os.path.join(self.root, "state"))
        with self.assertRaisesRegex(RnfsError, "junction or symlink"):
            node_module._make_private_dirs(self.root)


if __name__ == "__main__":
    unittest.main()
