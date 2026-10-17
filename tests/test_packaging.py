"""Static checks of the packaging files: install.sh's isolation and unit quoting, and the dependency lock."""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
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

    def test_installs_the_latest_release_by_default(self):
        """Without RNFS_SOURCE it installs the latest release's sdist, or the main branch with --main."""
        choose = shell_function("choose_source")
        self.assertEqual(choose.splitlines()[1].strip(), '[ -z "$SOURCE" ] || return 0')  # RNFS_SOURCE wins
        self.assertIn("SOURCE=$REPO_URL/releases/download/$tag/rnfs-${tag#v}.tar.gz", choose)
        self.assertIn("SOURCE=$REPO_URL/archive/refs/heads/main.zip", choose)
        self.assertIn('"/releases/latest"', shell_function("latest_release"))
        self.assertIn("    choose_source\n", shell_function("do_install"))
        self.assertIn("--main) MAIN=yes ;;", read(INSTALL_SH))

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

    @unittest.skipUnless(shutil.which("sh"), "needs a POSIX shell")
    def test_release_wheel_is_of_the_tag(self):
        """A release install takes only a wheel of the tag's version: an older release's sdist renamed to this
        release's file name builds an older one (SUP-2). Without a release (RNFS_SOURCE, --main) any rnfs wheel."""
        def find(directory, tag):
            script = shell_function("find_wheel") + '\nfind_wheel "$1"'
            return subprocess.run(["sh", "-c", script, "sh", directory], capture_output=True, text=True, check=True,
                                  env={**os.environ, "RELEASE_TAG": tag}).stdout
        with tempfile.TemporaryDirectory() as wheels:
            older = os.path.join(wheels, "rnfs-0.1.4-py3-none-any.whl")
            open(older, "w").close()
            self.assertEqual(find(wheels, "v0.1.6"), "")
            self.assertEqual(find(wheels, ""), older + "\n")
            wheel = os.path.join(wheels, "rnfs-0.1.6-py3-none-any.whl")
            open(wheel, "w").close()
            self.assertEqual(find(wheels, "v0.1.6"), wheel + "\n")
        install = shell_function("do_install")
        self.assertIn('wheel=$(find_wheel "$wheels")', install)
        self.assertLess(install.index('build_wheel "$wheels"'), install.index('wheel=$(find_wheel "$wheels")'))

    def test_provenance_names_the_tag(self):
        """The provenance check asks for this tag's build on a GitHub-hosted runner, where gh can (SUP-2; the
        check itself is run in tests/security/test_l15_install_release_check.py)."""
        verify = shell_function("verify_release")
        self.assertIn('*--source-ref*) options="$options --source-ref=refs/tags/$RELEASE_TAG"', verify)
        self.assertIn('*--deny-self-hosted-runners*) options="$options --deny-self-hosted-runners"', verify)

    def test_unit_matches_contrib(self):
        """install.sh's unit has every [Unit] and [Service] line of contrib/rnfs.service, in order, except
        ExecStart (the launcher's path) and comments (SUP-6)."""
        def settings(text):
            section, lines = None, []
            for line in text.splitlines():
                line = line.strip()
                if re.fullmatch(r"\[\w+\]", line):
                    section = line
                elif section in ("[Unit]", "[Service]") and line and not line.startswith(("#", "ExecStart=")):
                    lines.append((section, line))
            return lines
        contrib = settings(read(UNIT))
        for setting in ("StartLimitBurst=5", "NoNewPrivileges=yes", "After=rnsd.service", "TimeoutStopSec=30",
                        "SystemCallFilter=@system-service"):
            self.assertIn(setting, [line for _, line in contrib])
        self.assertEqual(settings(shell_function("write_unit")), contrib)

    def test_unit_hardening_is_an_opt_in_drop_in(self):
        """ProtectSystem and PrivateTmp, which need unprivileged user namespaces in a user unit, are in a documented
        drop-in only; no unit sets UMask, since what `get` writes takes the user's umask (SUP-6, Q16)."""
        drop_in = read(os.path.join(REPO, "contrib", "rnfs.service.d", "hardening.conf"))
        self.assertIn("\n[Service]\n", drop_in)
        self.assertRegex(drop_in, r"(?m)^ProtectSystem=full$")
        self.assertRegex(drop_in, r"(?m)^PrivateTmp=yes$")
        for unit in (read(UNIT), shell_function("write_unit")):
            self.assertNotRegex(unit, r"(?m)^(ProtectSystem|PrivateTmp|UMask)=")
            self.assertIn("contrib/rnfs.service.d/hardening.conf", unit)
        self.assertNotRegex(drop_in, r"(?m)^UMask=")

    @unittest.skipUnless(shutil.which("sh"), "needs a POSIX shell")
    def test_plain_http_source_warns(self):
        """An RNFS_SOURCE fetched over plain http:// (a URL, or a requirement naming one) gets a warning (SUP-6)."""
        script = "\n".join([re.search(r"(?m)^warn\(\) \{.*\}$", read(INSTALL_SH)).group(0),
                            shell_function("warn_plain_http"), 'SOURCE="$1"', "warn_plain_http"])

        def warning(source):
            return subprocess.run(["sh", "-c", script, "sh", source], capture_output=True, text=True,
                                  check=True).stderr
        for source in ("http://example.org/rnfs.zip", "HTTP://example.org/rnfs.zip",
                       "rnfs @ http://example.org/rnfs-0.1.6-py3-none-any.whl"):
            self.assertIn("plain http://", warning(source), source)
        for source in ("https://example.org/rnfs.zip", "/home/a/rnfs", "", "rnfs==0.1.6"):
            self.assertEqual(warning(source), "", source)
        install = shell_function("do_install")
        self.assertLess(install.index("    choose_source\n"), install.index("    warn_plain_http\n"))


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

    def test_deb_postinst_python_is_isolated(self):
        """The .deb's postinst, run as root, byte-compiles with python3 -I (SUP-6)."""
        postinst = re.search(r"(?ms)^cat > \"\$ROOT/DEBIAN/postinst\" <<'EOF'\n(.*?)^EOF$", read(BUILD_DEB))
        self.assertTrue(postinst)
        calls = re.findall(r"python3( -\S+)", postinst.group(1))
        self.assertTrue(calls)
        self.assertEqual(set(calls), {" -I"})

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

    def test_e2e_step_runs_every_gated_module(self):
        """Every test module gated on RNFS_E2E is run by the workflow's end-to-end step (else it never runs in CI)."""
        step = re.search(r"(?m)^      - name: End-to-end tests.*\n(?:        .*\n)*?        run: >-\n((?:          .*\n)+)",
                         read(os.path.join(WORKFLOWS, "tests.yml")))
        self.assertTrue(step)
        listed = set(step.group(1).split())
        gated = set()
        for top, _dirs, files in os.walk(os.path.join(REPO, "tests")):
            for name in files:
                path = os.path.join(top, name)
                if name.endswith(".py") and path != os.path.abspath(__file__) and 'environ.get("RNFS_E2E")' in read(path):
                    gated.add(os.path.relpath(path, REPO)[:-3].replace(os.sep, "."))
        self.assertIn("tests.test_e2e", gated)
        self.assertEqual(gated - listed, set())

    def test_flatpak_builds_skip_the_actions_cache(self):
        """Every flatpak-builder step turns the Actions cache off: a cache any workflow run can write must not
        reach a release bundle."""
        steps = 0
        for name in os.listdir(WORKFLOWS):
            text = read(os.path.join(WORKFLOWS, name))
            for step in re.finditer(r"(?m)^( *)- uses: [^\n]*/flatpak-builder@.*\n((?:\1  .*\n)*)", text):
                steps += 1
                self.assertRegex(step.group(2), r"(?m)^ +cache: false$", f"{name}: {step.group(0)}")
        self.assertTrue(steps)

    def test_flatpak_sources_are_checked_by_the_tests(self):
        """The check that the Flatpak's wheels match the lock runs in tests.yml (on every push, not only at a
        release, TS5), and the release builds the committed files rather than an artifact of its own."""
        tests = read(os.path.join(WORKFLOWS, "tests.yml"))
        job = re.search(r"(?m)^  flatpak-sources:\n((?:    .*\n|\n)+?)(?=^  \S|\Z)", tests)
        self.assertTrue(job)
        self.assertIn("python -I packaging/flatpak/generate-sources.py\n", job.group(1))
        self.assertIn("git diff --exit-code -- packaging/flatpak ||", job.group(1))
        release = read(os.path.join(WORKFLOWS, "release.yml"))
        self.assertNotIn("flatpak-sources", release.replace("tests.yml's flatpak-sources job", ""))

    def test_flatpak_sandbox_is_narrow(self):
        """The Flatpak reaches only rnfs's and Reticulum's directories and Downloads, and no device (SUP-5)."""
        manifest = read(os.path.join(REPO, "packaging", "flatpak", "io.github.afit21.rnfs.yml"))
        block = re.search(r"(?m)^finish-args:\n((?: +.*\n)+)", manifest)
        self.assertTrue(block)
        args = re.findall(r"(?m)^ +- (--\S+)$", block.group(1))
        self.assertEqual(args, ["--share=network", "--filesystem=~/.rnfs:create", "--filesystem=~/.reticulum:create",
                                "--filesystem=xdg-download"])
        # The opt-ins are documented in the manifest and the README
        for text in (block.group(1), read(os.path.join(REPO, "README.md"))):
            for override in ("--device=all", "--filesystem="):
                self.assertIn(f"flatpak override --user {override}", text)

    def test_release_waits_for_the_tests(self):
        """The release job needs the whole tests workflow, called from release.yml."""
        release = read(os.path.join(WORKFLOWS, "release.yml"))
        self.assertRegex(release, r"(?m)^  tests:\n    uses: \./\.github/workflows/tests\.yml$")
        needs = re.search(r"(?m)^  release:\n(?:    .*\n)*?    needs: \[(.*)\]$", release)
        self.assertTrue(needs)
        self.assertIn("tests", [n.strip() for n in needs.group(1).split(",")])
        self.assertRegex(read(os.path.join(WORKFLOWS, "tests.yml")), r"(?m)^  workflow_call:")


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
        for name in ("setuptools", "build", "twine", "pyinstaller", "pyinstaller-hooks-contrib", "ruff", "coverage"):
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


def version_tuple(text: str) -> tuple[int, ...]:
    """(1, 2, 3) from "1.2.3"."""
    return tuple(int(part) for part in text.split("."))


class ChangelogTest(unittest.TestCase):
    """CHANGELOG.md's first section is the current version's, or the next one's while it is unreleased, and the
    release workflow publishes that section as the release notes (TS6)."""

    def notes_step(self) -> str:
        """The run: script of release.yml's step that checks CHANGELOG.md and writes release-notes.md."""
        step = re.search(r"(?m)^      - name: Release notes from CHANGELOG\.md.*\n(?:        .*\n)*?        run: \|\n"
                         r"((?:          .*\n|\n)+?)(?=^      - )", read(os.path.join(WORKFLOWS, "release.yml")))
        self.assertTrue(step)
        return "\n".join(line[10:] for line in step.group(1).splitlines())

    def test_first_section_is_the_version(self):
        """The first `## ` heading is `## <__version__>`, or `## X.Y.Z (unreleased)` for a later X.Y.Z while
        __version__ still has a section of its own (it is bumped at the release)."""
        from rnfs import __version__
        headings = re.findall(r"(?m)^## (.*)$", read(os.path.join(REPO, "CHANGELOG.md")))
        self.assertTrue(headings)
        first = re.fullmatch(r"(\d+\.\d+\.\d+)( \(unreleased\))?", headings[0])
        self.assertTrue(first, headings[0])
        if first.group(2):
            self.assertGreater(version_tuple(first.group(1)), version_tuple(__version__), headings[0])
            self.assertIn(__version__, headings[1:])
        else:
            self.assertEqual(first.group(1), __version__)
        self.assertEqual(len(headings), len(set(headings)), "a version has two sections")

    def test_release_notes_are_the_changelog_section(self):
        """The release is created with --notes-file, from the notes the version job extracted, not
        --generate-notes."""
        release = read(os.path.join(WORKFLOWS, "release.yml"))
        self.assertNotIn("--generate-notes", release)
        self.assertIn("--notes-file notes/release-notes.md", release)
        self.assertIn('[ "${GITHUB_REF_TYPE}" = tag ] && [ "$first" != "## $VERSION" ]', self.notes_step())

    @unittest.skipUnless(shutil.which("sh") and shutil.which("awk"), "needs a POSIX shell and awk")
    def test_release_notes_step(self):
        """The step refuses a tag whose section is still unreleased or not first, and writes the version's section
        (without its heading or the next one) as the notes."""
        changelog = ("# Changelog\n\nIntro.\n\n## 0.2.0 (unreleased)\n\nNext.\n\n## 0.1.9\n\nNotes of 0.1.9.\n\n"
                     "### Security\n- A fix.\n\n## 0.1.8\n\nOlder.\n")

        def run(text, version, ref_type):
            with tempfile.TemporaryDirectory() as tmp:
                with open(os.path.join(tmp, "CHANGELOG.md"), "w", encoding="utf-8") as f:
                    f.write(text)
                done = subprocess.run(["sh", "-ec", self.notes_step()], cwd=tmp, capture_output=True, text=True,
                                      env={**os.environ, "VERSION": version, "GITHUB_REF_TYPE": ref_type})
                notes = read(os.path.join(tmp, "release-notes.md")) if done.returncode == 0 else None
                return done.returncode, notes
        section = "\nNotes of 0.1.9.\n\n### Security\n- A fix.\n\n"
        self.assertEqual(run(changelog, "0.1.9", "branch"), (0, section))
        self.assertNotEqual(run(changelog, "0.1.9", "tag")[0], 0)  # 0.2.0 is still above it
        self.assertNotEqual(run(changelog, "0.2.0", "tag")[0], 0)  # unreleased
        self.assertEqual(run(changelog, "0.2.0", "branch"), (0, "\nNext.\n\n"))
        released = changelog.replace("## 0.2.0 (unreleased)\n\nNext.\n\n", "")
        self.assertEqual(run(released, "0.1.9", "tag"), (0, section))
        self.assertNotEqual(run(released, "0.1.7", "branch")[0], 0)  # no section


class ReadmeTest(unittest.TestCase):
    """README.md's release links and file names are for the current version, so a version bump updates them."""

    def test_release_links_match_version(self):
        """Every releases/download/vX.Y.Z/ link and rnfs-X.Y.Z / rnfs_X.Y.Z file name is __version__."""
        from rnfs import __version__
        text = read(os.path.join(REPO, "README.md"))
        version = r"(\d+\.\d+\.\d+[\w.]*?)"
        found = (re.findall(r"releases/download/v%s/" % version, text)
                 + re.findall(r"\brnfs[-_]%s(?:[-_]|\.tar\.gz)" % version, text)
                 + re.findall(r"These links are for %s;" % version, text))
        self.assertGreaterEqual(len(found), 10)
        self.assertEqual(sorted(set(found)), [__version__])


if __name__ == "__main__":
    sys.exit(unittest.main())
