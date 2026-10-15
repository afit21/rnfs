"""L15 (fix): install.sh installed GitHub's auto-generated archive of the release tag, which neither SHA256SUMS nor the
signed provenance covers and which a moved tag changes. Now it installs the release's sdist, checked against the
release's SHA256SUMS and, where the GitHub CLI can, its signed provenance (gh attestation verify), failing closed.

install.sh's download, verify_release and fetch_release functions are run with sh against a fake release (file://
URLs) and a fake gh on a PATH holding only the tools they need, so the real network and any real gh are never
used. Whether the real gh accepts the real release's attestation is not tested here.
"""
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

from tests.test_packaging import INSTALL_SH, read, shell_function

PRELUDE = r"""
say() { printf '%s\n' "$*"; }
warn() { printf 'rnfs installer: warning: %s\n' "$*" >&2; }
die() { printf 'rnfs installer: %s\n' "$*" >&2; exit 1; }
REPO=afit21/rnfs
"""
# A gh that logs its arguments and answers as FAKE_GH says: "old" (no attestation command), "logged-out", "fail"
# (verification fails) or "ok"
FAKE_GH = r"""#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_GH_LOG"
case "$1 $2" in
    "auth status") [ "$FAKE_GH" != logged-out ] ;;
    "attestation verify")
        [ "$FAKE_GH" != old ] || { echo 'unknown command "attestation" for "gh"' >&2; exit 1; }
        case " $* " in *" --help "*) echo "  --signer-workflow string"; exit 0 ;; esac
        [ "$FAKE_GH" = ok ] || { echo "Error: no attestations found" >&2; exit 1; } ;;
    *) exit 2 ;;
esac
"""
TOOLS = ("basename", "cat", "mkdir", "grep", "printf")
SDIST = b"rnfs-9.9.9 source archive"


@unittest.skipUnless(shutil.which("sh"), "needs a POSIX shell")
@unittest.skipIf(os.name == "nt", "install.sh is the Linux and macOS route, and its file:// release needs POSIX paths")
class ReleaseCheckTest(unittest.TestCase):
    """A fake release v9.9.9 (its sdist and SHA256SUMS) under a file:// REPO_URL."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.release = os.path.join(self.tmp.name, "repo", "releases", "download", "v9.9.9")
        os.makedirs(self.release)
        self.work = os.path.join(self.tmp.name, "work")
        os.mkdir(self.work)
        self.publish(SDIST, SDIST)
        # Only the tools the functions use: no real gh, whatever this machine has
        self.bin = os.path.join(self.tmp.name, "bin")
        os.mkdir(self.bin)
        for tool in TOOLS:
            found = shutil.which(tool)
            if found:
                os.symlink(found, os.path.join(self.bin, tool))
        self.gh_log = os.path.join(self.tmp.name, "gh.log")

    def publish(self, sdist: bytes, summed: bytes, name: str = "rnfs-9.9.9.tar.gz"):
        """Put sdist in the release, with a SHA256SUMS line for summed under name (and one for another file)."""
        with open(os.path.join(self.release, "rnfs-9.9.9.tar.gz"), "wb") as f:
            f.write(sdist)
        with open(os.path.join(self.release, "SHA256SUMS"), "w") as f:
            f.write(f"{hashlib.sha256(b'other').hexdigest()}  rnfs-9.9.9-py3-none-any.whl\n")
            f.write(f"{hashlib.sha256(summed).hexdigest()}  {name}\n")

    def install_gh(self, behaviour: str):
        """Put the fake gh on the PATH, answering as behaviour."""
        gh = os.path.join(self.bin, "gh")
        with open(gh, "w") as f:
            f.write(FAKE_GH)
        os.chmod(gh, 0o755)
        self.gh = behaviour

    def fetch(self, **env) -> subprocess.CompletedProcess:
        """Run fetch_release for v9.9.9 as do_install would; prints the SOURCE it leaves."""
        repo_url = "file://" + os.path.join(self.tmp.name, "repo")
        script = "\n".join([PRELUDE, shell_function("download"), shell_function("verify_release"),
                            shell_function("gh_can_verify"), shell_function("fetch_release"),
                            'fetch_release "$1"', 'printf "SOURCE=%s\\n" "$SOURCE"'])
        environment = {"PATH": self.bin, "PYTHON": sys.executable, "REPO_URL": repo_url, "RELEASE_TAG": "v9.9.9",
                       "SOURCE": repo_url + "/releases/download/v9.9.9/rnfs-9.9.9.tar.gz",
                       "FAKE_GH": getattr(self, "gh", ""), "FAKE_GH_LOG": self.gh_log, **env}
        return subprocess.run([shutil.which("sh"), "-c", script, "sh", self.work], capture_output=True, text=True,
                              env=environment)

    def assert_installs_checked_file(self, result: subprocess.CompletedProcess):
        """fetch_release succeeded and left SOURCE at the downloaded, checked sdist."""
        self.assertEqual(result.returncode, 0, result.stderr)
        local = os.path.join(self.work, "release", "rnfs-9.9.9.tar.gz")
        self.assertIn(f"SOURCE={local}\n", result.stdout)
        with open(local, "rb") as f:
            self.assertEqual(f.read(), SDIST)

    def assert_refused(self, result: subprocess.CompletedProcess, message: str):
        """fetch_release stopped the install with message."""
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn(message, result.stderr)
        self.assertNotIn("SOURCE=", result.stdout)

    def gh_calls(self) -> list[str]:
        """The fake gh's command lines."""
        if not os.path.exists(self.gh_log):
            return []
        with open(self.gh_log) as f:
            return f.read().splitlines()

    def test_matching_sdist_without_gh_installs_with_a_warning(self):
        result = self.fetch()
        self.assert_installs_checked_file(result)
        self.assertIn("against the release's SHA256SUMS only", result.stderr)

    def test_without_gh_attestation_can_be_required(self):
        self.assert_refused(self.fetch(RNFS_REQUIRE_ATTESTATION="1"), "RNFS_REQUIRE_ATTESTATION=1, but")

    def test_replaced_sdist_is_refused(self):
        self.publish(b"backdoored", SDIST)
        self.assert_refused(self.fetch(), "doesn't match the release's SHA256SUMS")
        self.install_gh("ok")  # a good attestation answer doesn't matter: the checksum is checked first
        self.assert_refused(self.fetch(), "has sha256")

    def test_sdist_missing_from_sha256sums_is_refused(self):
        self.publish(SDIST, SDIST, name="rnfs-9.9.8.tar.gz")
        self.assert_refused(self.fetch(), "SHA256SUMS has 0 lines for rnfs-9.9.9.tar.gz")

    def test_missing_sha256sums_is_refused(self):
        os.remove(os.path.join(self.release, "SHA256SUMS"))
        self.assert_refused(self.fetch(), "couldn't download the SHA256SUMS of release v9.9.9")

    def test_attestation_checked_with_gh(self):
        self.install_gh("ok")
        result = self.fetch(RNFS_REQUIRE_ATTESTATION="1")
        self.assert_installs_checked_file(result)
        self.assertIn("signed build provenance (gh attestation verify)", result.stdout)
        local = os.path.join(self.work, "release", "rnfs-9.9.9.tar.gz")
        self.assertIn(f"attestation verify {local} --repo afit21/rnfs "
                      f"--signer-workflow=afit21/rnfs/.github/workflows/release.yml", self.gh_calls())

    def test_failed_attestation_is_refused(self):
        self.install_gh("fail")
        result = self.fetch()
        self.assert_refused(result, "no valid signed provenance")
        self.assertIn("no attestations found", result.stderr)  # gh's own explanation is shown

    def test_gh_that_cant_verify_counts_as_absent(self):
        # Ubuntu 24.04's gh 2.45 has no attestation command; a gh not logged in can't query attestations
        for behaviour in ("old", "logged-out"):
            with self.subTest(behaviour):
                self.install_gh(behaviour)
                self.assert_installs_checked_file(self.fetch())
                self.assertFalse([call for call in self.gh_calls() if " --repo " in call])
                self.assert_refused(self.fetch(RNFS_REQUIRE_ATTESTATION="1"), "RNFS_REQUIRE_ATTESTATION=1")


class DefaultSourceTest(unittest.TestCase):
    """What install.sh installs by default."""

    def test_installs_the_release_sdist_not_the_tag_archive(self):
        text = read(INSTALL_SH)
        self.assertNotIn("archive/refs/tags", re.sub(r"(?m)^\s*#.*$", "", text))
        choose = shell_function("choose_source")
        self.assertIn("RELEASE_TAG=$tag", choose)
        self.assertIn("SOURCE=$REPO_URL/releases/download/$tag/rnfs-${tag#v}.tar.gz", choose)
        install = shell_function("do_install")
        self.assertLess(install.index('fetch_release "$wheels"'), install.index('fetch_source "$wheels"'))
        self.assertIn('verify_release "$sdist"', shell_function("fetch_release"))


if __name__ == "__main__":
    unittest.main()
