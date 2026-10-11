"""Static checks of the packaging files: install.sh's isolation and unit quoting, and the dependency lock."""
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INSTALL_SH = os.path.join(REPO, "install.sh")
LOCK = os.path.join(REPO, "packaging", "requirements.lock")
FLATPAK_DEPS = os.path.join(REPO, "packaging", "flatpak", "python3-deps.json")


def read(path: str) -> str:
    """The text of a repository file."""
    with open(path, encoding="utf-8") as f:
        return f.read()


def read_lock() -> dict[str, tuple[str, set[str]]]:
    """{name: (version, {sha256, ...})} from packaging/requirements.lock."""
    lock = {}
    for line in read(LOCK).replace("\\\n", " ").splitlines():
        words = line.split("#", 1)[0].split()
        if words:
            name, _, version = words[0].partition("==")
            lock[name.lower()] = (version, {w.split(":", 1)[1] for w in words[1:] if w.startswith("--hash=sha256:")})
    return lock


def shell_function(name: str) -> str:
    """The source of one shell function from install.sh."""
    match = re.search(r"^%s\(\) \{.*?^\}$" % re.escape(name), read(INSTALL_SH), re.M | re.S)
    assert match, name
    return match.group(0)


class InstallScriptTest(unittest.TestCase):
    """install.sh: no Python import from the caller's directory, a systemd unit that survives odd paths."""

    def test_every_python_call_is_isolated(self):
        """Every interpreter call uses -I, so a venv.py or pip/ in the current directory isn't imported (LP-M1)."""
        calls = re.findall(r'("\$PYTHON"|"\$1"|python3|"\$VENV/bin/python")( -[A-Za-z]+)', read(INSTALL_SH))
        self.assertTrue(calls)
        for interpreter, flag in calls:
            self.assertEqual(flag, " -I", f"{interpreter}{flag} in install.sh isn't isolated")

    def test_leaves_the_callers_directory(self):
        """The script resolves RNFS_SOURCE to an absolute path and then changes to / before running Python."""
        text = read(INSTALL_SH)
        self.assertRegex(text, r'(?m)^if \[ -e "\$SOURCE" \]; then SOURCE=\$\(abs_path "\$SOURCE"\)')
        self.assertLess(text.index("\ncd /\n"), text.index("\ndo_install() {"))

    @unittest.skipUnless(shutil.which("sh"), "needs a POSIX shell")
    def test_systemd_quote(self):
        """The launcher path is one double-quoted ExecStart word with % doubled (LP-L5)."""
        script = shell_function("systemd_quote") + '\nsystemd_quote "$1"'
        out = subprocess.run(["sh", "-c", script, "sh", "/home/a b%h/.local/bin/rnfs"],
                             capture_output=True, text=True, check=True).stdout
        self.assertEqual(out, '"/home/a b%%h/.local/bin/rnfs"')

    def test_unit_uses_systemd_quote(self):
        """The unit install.sh writes runs the quoted launcher path."""
        self.assertIn('ExecStart=$(systemd_quote "$LAUNCHER") daemon', read(INSTALL_SH))


class DependencyLockTest(unittest.TestCase):
    """packaging/requirements.lock pins every dependency, with hashes, and the Flatpak uses only those."""

    def test_lock_covers_pyproject_dependencies(self):
        """Each project dependency, lxmf included, is pinned to one version with sha256 hashes."""
        with open(os.path.join(REPO, "pyproject.toml"), "rb") as f:
            dependencies = tomllib.load(f)["project"]["dependencies"]
        lock = read_lock()
        names = [re.match(r"[A-Za-z0-9_.-]+", d).group(0).lower() for d in dependencies]
        self.assertIn("lxmf", names)
        for name in names + ["cryptography", "cffi", "pyserial"]:
            self.assertIn(name, lock)
        for name, (version, hashes) in lock.items():
            self.assertRegex(version, r"^\d[\w.]*$", name)
            self.assertTrue(hashes and all(re.fullmatch(r"[0-9a-f]{64}", h) for h in hashes), name)

    def test_flatpak_wheels_are_locked(self):
        """Every wheel in the Flatpak's python3-deps.json is a version and file the lock pins."""
        lock = read_lock()
        with open(FLATPAK_DEPS, encoding="utf-8") as f:
            sources = json.load(f)["sources"]
        self.assertTrue(sources)
        for source in sources:
            filename = source["url"].rsplit("/", 1)[-1]
            name, version = filename.split("-")[:2]
            name = name.lower().replace("_", "-")
            self.assertIn(name, lock, filename)
            self.assertEqual(version, lock[name][0], filename)
            self.assertIn(source["sha256"], lock[name][1], filename)


if __name__ == "__main__":
    sys.exit(unittest.main())
