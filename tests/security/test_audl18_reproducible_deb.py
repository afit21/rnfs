"""AUD-L18, reproducible release builds: nothing set SOURCE_DATE_EPOCH, and the .deb carried a direct_url.json naming
the build's temporary path and a wheel built again by build-deb.sh, different from the released one; so nobody
could rebuild a release and get the same files to compare. Now the release's python and deb jobs export the
commit's time as SOURCE_DATE_EPOCH, build-deb.sh dates every file with it (taking it from the checkout's last
commit when it isn't set) and drops direct_url.json, and the release's .deb is built from the released wheel.

Static checks of packaging/build-deb.sh and release.yml. The builds themselves were checked locally: two
build-deb.sh runs, and one from a separately built wheel, gave byte-identical .debs (see the report).
"""
import os
import re
import unittest

from tests.test_packaging import BUILD_DEB, WORKFLOWS, read

EPOCH_STEP = 'echo "SOURCE_DATE_EPOCH=$(git log -1 --format=%ct)" >> "$GITHUB_ENV"'


def job(text: str, name: str) -> str:
    """One job's lines from a workflow."""
    match = re.search(r"(?m)^  %s:\n((?:    .*\n|\s*\n)+?)(?=^  \S|\Z)" % re.escape(name), text)
    assert match, name
    return match.group(1)


class BuildDebTest(unittest.TestCase):

    def test_source_date_epoch(self):
        text = read(BUILD_DEB)
        self.assertIn('git -C "$REPO" log -1 --format=%ct', text)
        self.assertIn("export SOURCE_DATE_EPOCH", text)
        # Every file dated with it, after the last file is written and before the archive is made
        clamp = text.index('find "$ROOT" -exec touch -h -d "@$SOURCE_DATE_EPOCH" {} +')
        self.assertGreater(clamp, text.index('> "$ROOT/DEBIAN/md5sums"'))
        self.assertLess(clamp, text.index("dpkg-deb --root-owner-group --build"))
        # Set before the wheel is built, which takes its timestamps from it
        self.assertLess(text.index("export SOURCE_DATE_EPOCH"), text.index("pip wheel"))

    def test_direct_url_is_dropped(self):
        text = read(BUILD_DEB)
        self.assertIn('for direct_url in "$LIB"/*.dist-info/direct_url.json; do', text)
        self.assertIn('rm -f "$direct_url"', text)
        self.assertIn('direct_url.json," "$info/RECORD"', text)  # and its RECORD line


class ReleaseWorkflowTest(unittest.TestCase):

    def test_build_jobs_export_the_commit_time(self):
        release = read(os.path.join(WORKFLOWS, "release.yml"))
        for name in ("python", "deb"):
            with self.subTest(name):
                self.assertIn(EPOCH_STEP, job(release, name))
        python = job(release, "python")
        self.assertLess(python.index(EPOCH_STEP), python.index("python -m build"))

    def test_deb_is_built_from_the_released_wheel(self):
        deb = job(read(os.path.join(WORKFLOWS, "release.yml")), "deb")
        self.assertRegex(deb, r"(?m)^    needs: \[.*\bpython\b.*\]$")
        self.assertRegex(deb, r"(?m)^          name: release-python$")
        self.assertIn('--wheel "wheel/rnfs-$VERSION-py3-none-any.whl"', deb)
        self.assertLess(deb.index(EPOCH_STEP), deb.index("sh packaging/build-deb.sh"))


if __name__ == "__main__":
    unittest.main()
