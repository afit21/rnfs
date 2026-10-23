"""AUD-L14, the pinned dependencies: packaging/build-tools.lock pinned certifi 2026.7.22, which has
GHSA-6q48-6c7w-9gw2 / PYSEC-2026-4320 (emSign roots with compromised intermediates; fixed in 2026.10.19), and
nothing would have said so: Dependabot doesn't read *.lock files, and dependabot.yml claimed a vulnerable pin "is
still reported". Now certifi is 2026.10.19 and .github/workflows/audit.yml checks every lock against OSV daily and
whenever one changes.

Static checks of the lock and the workflow (whose scan was run locally with osv-scanner 2.7.0: see the report).
"""
import glob
import os
import re
import unittest

from tests.test_packaging import REPO, TOOLS_LOCK, WORKFLOWS, read, read_lock, version_tuple

AUDIT = os.path.join(WORKFLOWS, "audit.yml")


class LockAuditTest(unittest.TestCase):

    def test_certifi_is_fixed(self):
        version, _hashes = read_lock(TOOLS_LOCK)["certifi"]
        self.assertGreaterEqual(version_tuple(version), (2026, 10, 19))

    def test_every_lock_is_scanned(self):
        text = read(AUDIT)
        scanned = re.findall(r"(?m)^ +--lockfile=requirements\.txt:(\S+)$", text)
        locks = sorted(os.path.relpath(path, REPO).replace(os.sep, "/")
                       for path in glob.glob(os.path.join(REPO, "packaging", "*.lock")))
        self.assertEqual(len(locks), 3)
        self.assertEqual(sorted(scanned), locks)

    def test_runs_daily_and_when_a_lock_changes(self):
        text = read(AUDIT)
        self.assertRegex(text, r'(?m)^  schedule:\n    - cron: "\d+ \d+ \* \* \*"')
        for trigger in ("push", "pull_request"):
            block = re.search(r"(?m)^  %s:\n((?:    .*\n)+)" % trigger, text)
            self.assertTrue(block, trigger)
            self.assertIn('"packaging/*.lock"', block.group(1))

    def test_least_privilege_and_pinned(self):
        text = read(AUDIT)
        self.assertRegex(text, r"(?m)^permissions:\n  contents: read\n")
        self.assertNotRegex(text, r"(?m)^\s+\w[\w-]*: write")
        uses = re.findall(r"(?m)^\s+- uses: (\S+)(.*)$", text)
        self.assertTrue(uses)
        for action, comment in uses:
            self.assertRegex(action, r"@[0-9a-f]{40}$")
            self.assertRegex(comment, r"#\s*v\d")
        self.assertIn("persist-credentials: false", text)

    def test_dependabot_comment_is_accurate(self):
        text = read(os.path.join(REPO, ".github", "dependabot.yml"))
        self.assertNotIn("still reported", text)
        self.assertIn("audit.yml", text)


if __name__ == "__main__":
    unittest.main()
