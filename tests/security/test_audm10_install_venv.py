"""AUD-M10 (a, b), install.sh's venv: (a) venv_ok passed a venv that a distro Python upgrade broke (bin/python now
the new Python, the packages still under lib/python3.<old>), so every update failed with "No module named pip" and
never repaired it; (b) a venv that had to be re-created was deleted (rm -rf) before anything was downloaded, checked
or built, so any later failure left no rnfs and the service crash-looping. Now a venv whose Python can't import pip
is re-created, and the old venv is only moved aside, after the new rnfs is built, and put back if the install fails.

install.sh runs here for real, as `sh install.sh`, in a scratch HOME, with RNFS_SOURCE a small fake rnfs wheel
(no dependencies) and PIP_NO_INDEX=1, so nothing is fetched from the network, nothing outside the scratch
directory is touched, and no systemd unit is involved (no --service, and none in the scratch HOME).
"""
import base64
import hashlib
import os
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import unittest
import zipfile

from tests.test_packaging import INSTALL_SH


def make_wheel(directory: str, version: str, requires: tuple[str, ...] = ()) -> str:
    """A minimal rnfs wheel of version whose `rnfs` command prints "rnfs <version>"; requires are Requires-Dist
    lines. Returns its path."""
    info = f"rnfs-{version}.dist-info"
    files = {
        "rnfs/__init__.py": f'def main():\n    print("rnfs {version}")\n',
        f"{info}/METADATA": "Metadata-Version: 2.1\nName: rnfs\nVersion: %s\n%s" % (
            version, "".join(f"Requires-Dist: {r}\n" for r in requires)),
        f"{info}/WHEEL": "Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        f"{info}/entry_points.txt": "[console_scripts]\nrnfs = rnfs:main\n",
    }
    record = []
    for name, text in files.items():
        digest = base64.urlsafe_b64encode(hashlib.sha256(text.encode()).digest()).rstrip(b"=").decode()
        record.append(f"{name},sha256={digest},{len(text.encode())}")
    record.append(f"{info}/RECORD,,")
    files[f"{info}/RECORD"] = "\n".join(record) + "\n"
    path = os.path.join(directory, f"rnfs-{version}-py3-none-any.whl")
    with zipfile.ZipFile(path, "w") as z:
        for name, text in files.items():
            z.writestr(name, text)
    return path


@unittest.skipUnless(shutil.which("sh"), "needs a POSIX shell")
@unittest.skipIf(os.name == "nt", "install.sh is the Linux and macOS route")
class InstallerHome(unittest.TestCase):
    """A scratch HOME with fake rnfs 9.9.9 installed by install.sh (made once, restored before each test)."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.home = os.path.join(cls.tmp.name, "home")
        cls.snapshot = os.path.join(cls.tmp.name, "snapshot")
        cls.wheels = os.path.join(cls.tmp.name, "wheels")
        os.makedirs(os.path.join(cls.tmp.name, "tmp"))
        os.mkdir(cls.wheels)
        cls.wheel = make_wheel(cls.wheels, "9.9.9")
        result = cls.install(cls.wheel)
        if result.returncode != 0:
            cls.tmp.cleanup()
            raise AssertionError(f"the first install failed:\n{result.stdout}\n{result.stderr}")
        shutil.copytree(cls.home, cls.snapshot, symlinks=True)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def setUp(self):
        # The same path as the snapshot was made at, so the venv's scripts (absolute shebangs) still work
        shutil.rmtree(self.home)
        shutil.copytree(self.snapshot, self.home, symlinks=True)

    @classmethod
    def install(cls, source: str, *args: str) -> subprocess.CompletedProcess:
        """Run install.sh with RNFS_SOURCE=source in the scratch HOME, offline."""
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("XDG_", "PIP_", "PYTHON", "RNFS_"))}
        env.update(HOME=cls.home, TMPDIR=os.path.join(cls.tmp.name, "tmp"), RNFS_SOURCE=source,
                   RNFS_PYTHON=sys.executable, PIP_NO_INDEX="1", PIP_DISABLE_PIP_VERSION_CHECK="1")
        return subprocess.run(["sh", INSTALL_SH, *args], capture_output=True, text=True, env=env, timeout=600)

    @property
    def venv(self) -> str:
        return os.path.join(self.home, ".local", "share", "rnfs", "venv")

    def rnfs_version(self) -> str:
        """What the installed command prints for --version."""
        return subprocess.run([os.path.join(self.home, ".local", "bin", "rnfs"), "--version"], capture_output=True,
                              text=True, timeout=60).stdout.strip()

    def site_lib(self) -> str:
        """The venv's lib/pythonX.Y directory."""
        return os.path.join(self.venv, "lib", "python" + sysconfig.get_python_version())


class BrokenVenvRepairTest(InstallerHome):
    """M10a: a venv that can't run pip is re-created."""

    def test_venv_left_by_a_python_upgrade_is_recreated(self):
        # A distro Python upgrade: bin/python still runs (the new Python), but the packages, pip among them, are
        # under the old version's directory, which the new Python doesn't look in
        os.rename(self.site_lib(), os.path.join(self.venv, "lib", "python3.0"))
        result = self.install(self.wheel)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("No module named pip", result.stderr)
        self.assertIn("Re-creating the venv", result.stdout)
        self.assertTrue(os.path.isdir(self.site_lib()))
        self.assertEqual(self.rnfs_version(), "rnfs 9.9.9")
        self.assertFalse(os.path.exists(self.venv + ".old"))

    def test_working_venv_is_updated_in_place(self):
        marker = os.path.join(self.venv, "kept")
        open(marker, "w").close()
        result = self.install(self.wheel)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Updating rnfs in", result.stdout)
        self.assertTrue(os.path.exists(marker))


class OldVenvKeptTest(InstallerHome):
    """M10b: a failed re-create leaves the old venv where it was."""

    def break_python(self):
        """The venv's Python is gone (e.g. a removed python3.X), so it must be re-created; a marker file tells
        whether the old venv survives."""
        python = os.path.join(self.venv, "bin", "python")
        os.remove(python)
        os.symlink(os.path.join(self.tmp.name, "no-such-python"), python)
        with open(os.path.join(self.venv, "old-venv"), "w") as f:
            f.write("the venv before this install")

    def assert_old_venv_in_place(self, result: subprocess.CompletedProcess):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertTrue(os.path.exists(os.path.join(self.venv, "old-venv")), result.stdout + result.stderr)
        self.assertFalse(os.path.exists(self.venv + ".old"))
        # The old rnfs's own files are still there
        self.assertTrue(os.path.isdir(os.path.join(self.site_lib(), "site-packages", "rnfs-9.9.9.dist-info")))

    def test_build_failure_doesnt_touch_the_old_venv(self):
        self.break_python()
        broken = os.path.join(self.wheels, "rnfs-9.9.10-py3-none-any.whl")
        with open(broken, "w") as f:
            f.write("not a zip file")
        self.assert_old_venv_in_place(self.install(broken))

    def test_install_failure_puts_the_old_venv_back(self):
        """The new venv is made and the install into it fails (a dependency PyPI can't give): the old one is put
        back."""
        self.break_python()
        needy = make_wheel(tempfile.mkdtemp(dir=self.tmp.name), "9.9.10", requires=("rnfs-no-such-dependency",))
        result = self.install(needy)
        self.assert_old_venv_in_place(result)
        self.assertIn("put back", result.stderr)

    def test_successful_recreate_removes_the_old_venv(self):
        self.break_python()
        result = self.install(self.wheel)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(os.path.exists(os.path.join(self.venv, "old-venv")))
        self.assertFalse(os.path.exists(self.venv + ".old"))
        self.assertEqual(self.rnfs_version(), "rnfs 9.9.9")

    def test_venv_left_aside_by_a_killed_install_is_recovered(self):
        """An install killed (SIGKILL, power loss) between moving the venv aside and putting it back left only
        venv.old: the next run starts from it."""
        os.rename(self.venv, self.venv + ".old")
        result = self.install(self.wheel)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Updating rnfs in", result.stdout)
        self.assertFalse(os.path.exists(self.venv + ".old"))


if __name__ == "__main__":
    unittest.main()
