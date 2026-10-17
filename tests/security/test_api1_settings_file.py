"""API-1 (settings file): on POSIX, rnfs.toml was read by path with no check at all: a 0666 file loaded with no
warning, a symlink was followed, a FIFO blocked the start and one owned by another user was used. Its settings pick
the Reticulum config (and with it programs rnfs starts, such as a PipeInterface's command) and the API port the CLI
sends its token to, so whoever could write it chose them. Now it is opened with O_NOFOLLOW | O_NONBLOCK and checked
through that descriptor: a symlink, anything but a regular file and a file of another user's are refused, a file
others may access is made 0600 with a warning, and at most 1 MiB is read. RNS's own `config` gets the same check
when reticulum_dir is the rnfs dir.

ND6: the warnings are returned to the caller (config.warnings) instead of being logged through rnfs.log, which
imports RNS. The daemon prints them, rnfs.Node logs them, the CLI client prints them and rnfs.Client raises them
as RuntimeWarnings; a client loading a loose settings file never loads RNS (checked in a `python -I` subprocess,
on the POSIX path and on the Windows one through a fake access-list backend).

Reproduced with real files, symlinks and a FIFO in a temporary directory (POSIX); "another user" is an os.geteuid
that answers a different uid.
"""
import contextlib
import io
import os
import stat
import subprocess
import sys
import tempfile
import threading
import types
import unittest
from unittest import mock

from rnfs import apiclient, cli
from rnfs.config import MAX_SETTINGS_SIZE, SETTINGS_FILE, Config
from tests.support import CAN_SYMLINK, HAS_FIFO, POSIX_MODES

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SETTINGS = 'api_port = 9999\nreticulum_dir = "/tmp/planted-reticulum"\n'

# Run with `python -I`: loads a client over a loose rnfs.toml with every import of RNS refused and recorded, on the
# POSIX path or (argv[3] == "windows") on the Windows one, through a fake access-list backend that reports the file
# open to Users until it is made private. Prints whether RNS was imported, then each warning raised
CLIENT_SCRIPT = r"""
import importlib.abc, sys, warnings
sys.path.insert(0, sys.argv[1])
attempts = []

class NoRns(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name == "RNS" or name.startswith("RNS."):
            attempts.append(name)
            raise ImportError("RNS imported")
        return None

sys.meta_path.insert(0, NoRns())
if sys.argv[3] == "windows":
    from rnfs import fsutil, winacl
    USER = "S-1-5-21-1-2-3-1001"
    loose = winacl.Dacl(USER, [winacl.Ace(winacl.ACCESS_ALLOWED, 0, winacl.FILE_ALL_ACCESS, USER),
                               winacl.Ace(winacl.ACCESS_ALLOWED, 0, 0x120089, "S-1-5-32-545")])
    private = winacl.Dacl(USER, winacl.parse_acl(winacl.private_acl(USER, False)))

    class Acl:
        made_private = False
        def user_sid(self):
            return USER
        def read(self, path, fd=None):
            return private if self.made_private else loose
        def set_private(self, path, directory):
            self.made_private = True

    fsutil.IS_WINDOWS = True
    winacl.backend = Acl()
import rnfs
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    client = rnfs.Client(sys.argv[2])
print(client.api.port)
print(bool(attempts) or any(m == "RNS" or m.startswith("RNS.") for m in sys.modules))
for warning in caught:
    print(warning.category.__name__, warning.message)
"""


@unittest.skipUnless(POSIX_MODES, "Unix permission bits and owners")
class SettingsFileTest(unittest.TestCase):
    """Config.load over a temporary rnfs dir, with rnfs.toml and a file of someone else's next to it."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name
        self.path = os.path.join(self.dir, SETTINGS_FILE)
        self.theirs = os.path.join(self.dir, "their-settings")
        with open(self.theirs, "w", encoding="utf-8") as f:
            f.write(SETTINGS)
        os.chmod(self.theirs, 0o644)

    def write(self, text: str = SETTINGS, mode: int = 0o600):
        """rnfs.toml holding text, with mode."""
        with open(self.path, "w", encoding="utf-8") as f:
            f.write(text)
        os.chmod(self.path, mode)

    def mode(self, path: str) -> int:
        """path's permission bits."""
        return stat.S_IMODE(os.lstat(path).st_mode)

    def refused(self, error=PermissionError) -> str:
        """Config.load, which must refuse the settings; its message."""
        with self.assertRaises(error) as caught:
            Config.load(self.dir)
        return str(caught.exception)

    def test_loose_settings_are_tightened_with_a_warning(self):
        self.write(mode=0o666)
        config = Config.load(self.dir)
        self.assertEqual(self.mode(self.path), 0o600)
        self.assertEqual(config.api_port, 9999)  # still used, after the warning
        [warning] = config.warnings
        self.assertIn(self.path, warning)
        self.assertIn("accessible to other users (mode 666)", warning)
        self.assertIn("Check that its settings are yours", warning)

    def test_group_writable_settings_are_tightened(self):
        """The 0664 a umask of 002 gives: made private, not refused."""
        self.write(mode=0o664)
        self.assertEqual(len(Config.load(self.dir).warnings), 1)
        self.assertEqual(self.mode(self.path), 0o600)

    def test_private_settings_load_quietly(self):
        self.write()
        self.assertEqual(list(Config.load(self.dir).warnings), [])
        self.assertEqual(list(Config.load(os.path.join(self.dir, "missing")).warnings), [])  # no file at all

    @unittest.skipUnless(CAN_SYMLINK, "symlinks")
    def test_symlinked_settings_are_refused(self):
        os.symlink(self.theirs, self.path)
        self.assertIn("symlink", self.refused())
        self.assertEqual(self.mode(self.theirs), 0o644)  # the target is left alone

    @unittest.skipUnless(CAN_SYMLINK, "symlinks")
    def test_dangling_symlink_is_refused(self):
        """A symlink to nothing is refused too, not taken for no settings."""
        os.symlink(os.path.join(self.dir, "nothing"), self.path)
        self.assertIn("symlink", self.refused())

    @unittest.skipUnless(HAS_FIFO, "FIFOs")
    def test_fifo_is_refused_without_blocking(self):
        os.mkfifo(self.path, 0o600)
        outcome = []

        def load():
            try:
                Config.load(self.dir)
                outcome.append("loaded")
            except Exception as e:  # whatever it is, the test reports it
                outcome.append(e)

        thread = threading.Thread(target=load, daemon=True)
        thread.start()
        thread.join(5)
        if thread.is_alive():
            # Unblock the old code's blocking open: a writer that closes at once gives it an empty read
            os.close(os.open(self.path, os.O_WRONLY | os.O_NONBLOCK))
            thread.join(5)
            self.fail("loading the settings hung opening the FIFO at rnfs.toml")
        [result] = outcome
        self.assertIsInstance(result, PermissionError)
        self.assertIn("not a regular file", str(result))

    def test_directory_is_refused(self):
        os.mkdir(self.path)
        self.assertIn("not a regular file", self.refused())

    def test_settings_of_another_user_are_refused(self):
        self.write(mode=0o644)
        with mock.patch("os.geteuid", return_value=os.geteuid() + 1):
            self.assertIn("belongs to another user", self.refused())
        self.assertEqual(self.mode(self.path), 0o644)  # not someone else's file to change

    def test_settings_that_cant_be_tightened_are_refused(self):
        self.write(mode=0o666)
        with mock.patch("os.fchmod", side_effect=PermissionError(1, "Operation not permitted")):
            self.assertIn("can't be restricted", self.refused())

    def test_oversized_settings_are_refused(self):
        self.write("# " + "x" * MAX_SETTINGS_SIZE + "\n")
        self.assertIn("larger than", self.refused(ValueError))

    def test_warnings_is_not_a_setting(self):
        """config.warnings is a plain attribute: rnfs.toml can't set it."""
        self.write('warnings = ["all is well"]\n')
        self.assertIn("unknown settings", self.refused(ValueError))
        self.assertEqual(list(Config().warnings), [])


@unittest.skipUnless(POSIX_MODES, "Unix permission bits and owners")
class ReticulumConfigTest(unittest.TestCase):
    """RNS's `config` in the rnfs dir, checked when reticulum_dir is the rnfs dir (as earlier versions used it)."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name
        self.path = os.path.join(self.dir, "config")
        with open(self.path, "w", encoding="utf-8") as f:
            f.write("[reticulum]\n")
        os.chmod(self.path, 0o644)

    def mode(self) -> int:
        """The config file's permission bits."""
        return stat.S_IMODE(os.lstat(self.path).st_mode)

    def test_loose_config_is_tightened_with_a_warning(self):
        config = Config.load(self.dir, reticulum_dir=self.dir + os.sep)
        self.assertEqual(self.mode(), 0o600)
        [warning] = config.warnings
        self.assertIn(self.path, warning)

    def test_reticulum_dir_set_in_rnfs_toml_counts_too(self):
        with open(os.path.join(self.dir, SETTINGS_FILE), "w", encoding="utf-8") as f:
            f.write(f'reticulum_dir = "{self.dir}"\n')
        os.chmod(os.path.join(self.dir, SETTINGS_FILE), 0o600)
        Config.load(self.dir)
        self.assertEqual(self.mode(), 0o600)

    def test_config_of_another_user_is_refused(self):
        with mock.patch("os.geteuid", return_value=os.geteuid() + 1):
            with self.assertRaisesRegex(PermissionError, "belongs to another user"):
                Config.load(self.dir, reticulum_dir=self.dir)

    @unittest.skipUnless(CAN_SYMLINK, "symlinks")
    def test_symlinked_config_is_refused(self):
        target = os.path.join(self.dir, "elsewhere")
        os.replace(self.path, target)
        os.symlink(target, self.path)
        with self.assertRaisesRegex(PermissionError, "symlink"):
            Config.load(self.dir, reticulum_dir=self.dir)

    def test_config_elsewhere_is_left_alone(self):
        """With reticulum_dir elsewhere (or RNS's default), a leftover config here isn't Reticulum's to check."""
        for reticulum_dir in (None, os.path.join(self.dir, "reticulum")):
            self.assertEqual(list(Config.load(self.dir, reticulum_dir=reticulum_dir).warnings), [])
        self.assertEqual(self.mode(), 0o644)

    def test_missing_config_is_fine(self):
        os.remove(self.path)
        self.assertEqual(list(Config.load(self.dir, reticulum_dir=self.dir).warnings), [])


@unittest.skipUnless(POSIX_MODES, "Unix permission bits")
class WarningsShownTest(unittest.TestCase):
    """Where the warnings go: the CLI client prints them, rnfs.Client raises RuntimeWarnings, without RNS."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name
        self.path = os.path.join(self.dir, SETTINGS_FILE)
        with open(self.path, "w", encoding="utf-8") as f:
            f.write("api_port = 9999\n")

    def loosen(self):
        """Make rnfs.toml readable and writable by everyone."""
        os.chmod(self.path, 0o666)

    def test_cli_client_prints_the_warning(self):
        self.loosen()
        args = types.SimpleNamespace(api_port=None, dir=self.dir)
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            client = cli.client(args)
        self.assertEqual(client.port, 9999)
        self.assertIn("rnfs: warning: the settings file", stderr.getvalue())
        self.assertIn("accessible to other users", stderr.getvalue())

    def test_library_raises_a_runtime_warning(self):
        self.loosen()
        with self.assertWarnsRegex(RuntimeWarning, "accessible to other users"):
            self.assertEqual(apiclient.ApiConnection(self.dir).port, 9999)

    def run_client(self, platform: str) -> list[str]:
        """CLIENT_SCRIPT in a `python -I` subprocess; its output lines, once it says RNS wasn't imported."""
        result = subprocess.run([sys.executable, "-I", "-c", CLIENT_SCRIPT, ROOT, self.dir, platform],
                                cwd=self.dir, capture_output=True, timeout=60, encoding="utf-8", errors="replace")
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = result.stdout.splitlines()
        self.assertEqual(lines[:2], ["9999", "False"], result.stdout + result.stderr)
        return lines[2:]

    def test_client_with_loose_settings_does_not_import_rns(self):
        self.loosen()
        [warning] = self.run_client("posix")
        self.assertTrue(warning.startswith("RuntimeWarning the settings file"), warning)
        self.assertEqual(stat.S_IMODE(os.stat(self.path).st_mode), 0o600)

    def test_client_on_windows_with_loose_settings_does_not_import_rns(self):
        """winacl reported a loose settings file through rnfs.log, which imports RNS (ND6)."""
        os.chmod(self.path, 0o600)  # the fake's access list, not these bits, says who may access it
        [warning] = self.run_client("windows")
        self.assertTrue(warning.startswith("RuntimeWarning the settings file"), warning)
        self.assertIn("Users (S-1-5-32-545) may access it", warning)


if __name__ == "__main__":
    unittest.main()
