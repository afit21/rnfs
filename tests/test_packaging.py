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
BACKEND_LOCK = os.path.join(REPO, "packaging", "build-backend.lock")
TOOLS_LOCK = os.path.join(REPO, "packaging", "build-tools.lock")
BUILD_DEB = os.path.join(REPO, "packaging", "build-deb.sh")
WORKFLOWS = os.path.join(REPO, ".github", "workflows")
UNIT = os.path.join(REPO, "contrib", "rnfs.service")
FLATPAK_DEPS = os.path.join(REPO, "packaging", "flatpak", "python3-deps.json")


def read(path: str) -> str:
    """The text of a repository file."""
    with open(path, encoding="utf-8") as f:
        return f.read()


def read_lock(path: str = LOCK) -> dict[str, tuple[str, set[str]]]:
    """{name: (version, {sha256, ...})} from a hash lock (packaging/requirements.lock by default)."""
    lock = {}
    for line in read(path).replace("\\\n", " ").splitlines():
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
        calls = re.findall(r'("\$PYTHON"|"\$1"|"\$py"|python3|"\$VENV/bin/python")( -[A-Za-z]+)', read(INSTALL_SH))
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


    def test_builds_with_the_locked_backend(self):
        """rnfs is built with build-backend.lock's setuptools, without build isolation; dependencies are wheels."""
        build = shell_function("build_wheel")
        self.assertIn('--require-hashes --only-binary=:all: -r "$backend_lock"', build)
        self.assertIn("--no-build-isolation", build)
        self.assertIn("--require-hashes --only-binary=", shell_function("install_locked"))
        install = shell_function("do_install")
        self.assertIn("vpip check", install)

    def test_system_mode_umask(self):
        """System mode makes everything world-readable whatever root's umask is."""
        text = read(INSTALL_SH)
        system = text[text.index('if [ "$MODE" = system ]; then\n    [ "$(id -u)" = 0 ]'):]
        self.assertIn("umask 022", system[:system.index("\nelse\n")])

    def test_only_its_own_unit_is_replaced_or_removed(self):
        """The unit is rewritten or deleted only if this installer wrote it (or with --force)."""
        self.assertIn('if [ "$SERVICE" = yes ] || unit_ours; then', shell_function("do_install"))
        uninstall = shell_function("do_uninstall")
        self.assertLess(uninstall.index("if unit_ours; then"), uninstall.index('rm -f "$UNIT"'))
        write = shell_function("write_unit")
        self.assertIn('$UNIT_MARK', write)

    def test_unit_matches_contrib(self):
        """install.sh's unit has the same [Unit] limits and [Service] settings as contrib/rnfs.service."""
        def settings(text):
            return [line for line in text.splitlines()
                    if re.match(r"(StartLimit|Restart|NoNewPrivileges|Restrict|LockPersonality|KillSignal)", line)]
        contrib = settings(read(UNIT))
        self.assertIn("StartLimitBurst=5", contrib)
        self.assertIn("NoNewPrivileges=yes", contrib)
        self.assertEqual(settings(shell_function("write_unit")), contrib)


class BuildScriptsTest(unittest.TestCase):
    """build-deb.sh and the workflows install nothing unhashed and fail on a missing cffi backend."""

    def test_deb_cffi_download_errors_are_fatal(self):
        """A failed cffi download stops the build unless pip found no wheel; every backend is checked."""
        text = read(BUILD_DEB)
        self.assertNotIn("2>/dev/null; then", text)
        self.assertIn("No matching distribution", text)
        self.assertIn('_cffi_backend.cpython-"$nodot"-*.so', text)
        self.assertIn("--no-build-isolation", text)
        self.assertIn('python3 (<< $PYTHON_BELOW)', text)

    def test_workflow_pip_installs_are_hashed(self):
        """Every pip install in the workflows uses a hash lock or --no-deps (with no build isolation)."""
        for name in os.listdir(WORKFLOWS):
            for line in read(os.path.join(WORKFLOWS, name)).splitlines():
                if re.search(r"\bpip install\b", line) and not line.lstrip().startswith("#"):
                    self.assertTrue("--require-hashes" in line or "--no-deps" in line, f"{name}: {line.strip()}")
                    if "--no-deps ." in line:
                        self.assertIn("--no-build-isolation", line, f"{name}: {line.strip()}")
                if "python -m build" in line:
                    self.assertIn("--no-isolation", line, f"{name}: {line.strip()}")

    def test_release_publishes_only_on_a_pushed_tag(self):
        """A manual run doesn't publish, and existing release files aren't replaced."""
        text = read(os.path.join(WORKFLOWS, "release.yml"))
        self.assertIn("if: github.event_name == 'push' && github.ref_type == 'tag'", text)
        uploads = [line for line in text.splitlines() if "gh release upload" in line]
        self.assertTrue(uploads)
        self.assertFalse([line for line in uploads if "--clobber" in line])
        self.assertRegex(text, r"actions/attest-build-provenance@[0-9a-f]{40}")


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

    def test_build_locks(self):
        """The build locks are hashed, agree with each other and with requirements.lock, and fit pyproject.toml."""
        backend, tools, runtime = read_lock(BACKEND_LOCK), read_lock(TOOLS_LOCK), read_lock()
        self.assertEqual(list(backend), ["setuptools"])
        for name in ("setuptools", "build", "twine", "pyinstaller", "pyinstaller-hooks-contrib"):
            self.assertIn(name, tools)
        self.assertEqual(backend["setuptools"], tools["setuptools"])
        for name, (version, hashes) in [*backend.items(), *tools.items()]:
            self.assertRegex(version, r"^\d[\w.]*$", name)
            self.assertTrue(hashes and all(re.fullmatch(r"[0-9a-f]{64}", h) for h in hashes), name)
        for name in set(tools) & set(runtime):
            self.assertEqual(tools[name][0], runtime[name][0], name)
        with open(os.path.join(REPO, "pyproject.toml"), "rb") as f:
            requires = tomllib.load(f)["build-system"]["requires"]
        bounds = re.fullmatch(r"setuptools>=([\d.]+),<([\d.]+)", requires[0].replace(" ", ""))
        self.assertTrue(bounds, requires)

        def version(text):
            return tuple(int(part) for part in text.split("."))
        self.assertTrue(version(bounds.group(1)) <= version(backend["setuptools"][0]) < version(bounds.group(2)))

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
