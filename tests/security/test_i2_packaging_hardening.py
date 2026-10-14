"""I2 (fix), packaging and CI: the release job named no environment, so no protection rules could gate publishing; the
systemd user unit had no system call filter; and the README's "Checking a download" didn't mention the signed
provenance.

Static checks of the workflow, both systemd units (contrib/rnfs.service and the one install.sh writes) and the
README. The environment's protection rules live in the repository's settings, which no test here can see. The
unit's filter was tried locally with systemd-run (see the report).
"""
import os
import re
import unittest

from tests.test_packaging import REPO, UNIT, WORKFLOWS, read, shell_function

FILTER = ("SystemCallFilter=@system-service", "SystemCallArchitectures=native")


class ReleaseEnvironmentTest(unittest.TestCase):

    def test_release_job_has_an_environment(self):
        release = read(os.path.join(WORKFLOWS, "release.yml"))
        job = re.search(r"(?m)^  release:\n((?:    .*\n|\s*\n)*)", release)
        self.assertTrue(job)
        self.assertRegex(job.group(1), r"(?m)^    environment: release$")


class UnitHardeningTest(unittest.TestCase):

    def test_units_filter_system_calls(self):
        for name, text in (("contrib/rnfs.service", read(UNIT)), ("install.sh", shell_function("write_unit"))):
            lines = text.splitlines()
            with self.subTest(name):
                for setting in FILTER:
                    self.assertIn(setting, lines)
                self.assertIn("NoNewPrivileges=yes", lines)  # what lets a user unit use the filter
                self.assertFalse([line for line in lines if line.startswith("MemoryDenyWriteExecute")])


class ReadmeTest(unittest.TestCase):

    def test_checking_a_download_mentions_attestations(self):
        text = read(os.path.join(REPO, "README.md"))
        section = text[text.index("### Checking a download"):]
        section = section[:section.index("\n### ", 1)]
        self.assertIn("gh attestation verify", section)
        self.assertIn("--repo afit21/rnfs", section)


if __name__ == "__main__":
    unittest.main()
