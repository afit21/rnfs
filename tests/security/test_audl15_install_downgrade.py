"""AUD-L15, install.sh downgraded silently: after `--main` (or a newer RNFS_SOURCE), running it again installed the
older latest release, reported success, and the daemon then refused the state database the newer rnfs had
upgraded, restarting until systemd gave up. Now it reads the installed version first and refuses an older one,
and a release of the same version over an install from the main branch or RNFS_SOURCE (which may be ahead of it,
since __version__ is bumped only at the release), unless --force.

The end-to-end cases run install.sh in a scratch HOME with fake wheels, offline (see test_audm10_install_venv); the
same-version case runs install.sh's check_downgrade function by itself against a fake venv.
"""
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

from tests.security.test_audm10_install_venv import InstallerHome, make_wheel
from tests.test_packaging import INSTALL_SH, read, shell_function

PRELUDE = r"""
say() { printf '%s\n' "$*"; }
warn() { printf 'rnfs installer: warning: %s\n' "$*" >&2; }
die() { printf 'rnfs installer: %s\n' "$*" >&2; exit 1; }
"""


class DowngradeRefusedTest(InstallerHome):
    """rnfs 9.9.9 is installed; installing 9.9.8 over it."""

    def test_older_version_is_refused(self):
        older = make_wheel(tempfile.mkdtemp(dir=self.tmp.name), "9.9.8")
        result = self.install(older)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn("rnfs 9.9.9 is installed", result.stderr)
        self.assertIn("--force", result.stderr)
        self.assertEqual(self.rnfs_version(), "rnfs 9.9.9")

    def test_force_installs_the_older_version(self):
        older = make_wheel(tempfile.mkdtemp(dir=self.tmp.name), "9.9.8")
        result = self.install(older, "--force")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.rnfs_version(), "rnfs 9.9.8")

    def test_same_or_newer_version_installs(self):
        for version in ("9.9.9", "9.10.0"):
            with self.subTest(version):
                wheel = make_wheel(tempfile.mkdtemp(dir=self.tmp.name), version)
                result = self.install(wheel)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(self.rnfs_version(), f"rnfs {version}")


@unittest.skipUnless(shutil.which("sh"), "needs a POSIX shell")
@unittest.skipIf(os.name == "nt", "install.sh is the Linux and macOS route")
class CheckDowngradeTest(unittest.TestCase):
    """check_downgrade by itself, against a fake venv holding rnfs 0.2.0 installed from origin."""

    def check(self, installed: str | None, origin: str | None, wheel: str, release_tag: str = "",
              force: str = "no") -> subprocess.CompletedProcess:
        with tempfile.TemporaryDirectory() as tmp:
            venv = os.path.join(tmp, "venv")
            os.makedirs(os.path.join(venv, "lib", "python3.12", "site-packages"))
            if installed:
                os.mkdir(os.path.join(venv, "lib", "python3.12", "site-packages", f"rnfs-{installed}.dist-info"))
            if origin:
                with open(os.path.join(venv, "rnfs-install-origin"), "w") as f:
                    f.write(origin + "\n")
            script = "\n".join([PRELUDE, shell_function("installed_version"), shell_function("check_downgrade"),
                                'check_downgrade "$1"', "echo passed"])
            env = {"PATH": os.environ.get("PATH", ""), "PYTHON": sys.executable, "VENV": venv,
                   "RELEASE_TAG": release_tag, "FORCE": force, "ORIGIN_FILE": "rnfs-install-origin"}
            return subprocess.run(["sh", "-c", script, "sh", os.path.join(tmp, "w", wheel)], capture_output=True,
                                  text=True, env=env)

    def assert_passes(self, result):
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("passed", result.stdout)

    def assert_refused(self, result, message):
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(message, result.stderr)

    def test_release_over_the_main_branch_of_the_same_version(self):
        """The finding's case: main still says 0.2.0 (it is bumped at the release) but may have upgraded the
        state database; the 0.2.0 release over it is refused."""
        for origin in ("main", "source"):
            with self.subTest(origin):
                result = self.check("0.2.0", origin, "rnfs-0.2.0-py3-none-any.whl", release_tag="v0.2.0")
                self.assert_refused(result, "may be newer than")
                self.assertIn("--main", result.stderr)
                self.assert_passes(self.check("0.2.0", origin, "rnfs-0.2.0-py3-none-any.whl",
                                              release_tag="v0.2.0", force="yes"))
        # A newer release over main is an update
        self.assert_passes(self.check("0.2.0", "main", "rnfs-0.2.1-py3-none-any.whl", release_tag="v0.2.1"))

    def test_same_version_otherwise_passes(self):
        # Re-running the release, the main branch over the release, or an install from before the origin file
        self.assert_passes(self.check("0.2.0", "release", "rnfs-0.2.0-py3-none-any.whl", release_tag="v0.2.0"))
        self.assert_passes(self.check("0.2.0", "release", "rnfs-0.2.0-py3-none-any.whl"))
        self.assert_passes(self.check("0.2.0", None, "rnfs-0.2.0-py3-none-any.whl", release_tag="v0.2.0"))

    def test_older_version_refused_whatever_the_origin(self):
        for origin in (None, "release", "main", "source"):
            for release_tag in ("", "v0.1.6"):
                with self.subTest(origin=origin, release_tag=release_tag):
                    self.assert_refused(self.check("0.2.0", origin, "rnfs-0.1.6-py3-none-any.whl", release_tag),
                                        "rnfs 0.2.0 is installed")
        self.assert_refused(self.check("0.10.0", None, "rnfs-0.9.9-py3-none-any.whl"), "is installed")
        self.assert_passes(self.check("0.2", None, "rnfs-0.2.0-py3-none-any.whl"))

    def test_nothing_installed_passes(self):
        self.assert_passes(self.check(None, None, "rnfs-0.1.6-py3-none-any.whl", release_tag="v0.1.6"))

    def test_usage_documents_force(self):
        usage = read(INSTALL_SH)[:read(INSTALL_SH).index("\nset -eu\n")]
        self.assertRegex(usage, r"--force .*\n(#  .*\n)*#.*older")


if __name__ == "__main__":
    unittest.main()
