"""AUD-L17, the systemd user unit's sandbox: the opt-in drop-in (hardening.conf: ProtectSystem=full, PrivateTmp) adds
little in a user unit, since the daemon runs as the user and could never write what ProtectSystem protects, while
a compromised daemon could still write ~/.bashrc or a user unit, or ask the user's systemd to start an unsandboxed
process; and the comments said otherwise. Now an opt-in contrib/rnfs.service.d/strict.conf makes the home read-only
but for rnfs's, Reticulum's and the Downloads directories and hides the runtime directory (the user manager, the
session bus, the agents), and the comments say what each drop-in does and doesn't do.

Static checks of the drop-ins and both units (contrib/rnfs.service and install.sh's), and, where systemd-analyze is
installed, `systemd-analyze --user verify` of the unit with the drop-in, from a scratch copy (no unit is installed
or started).
"""
import os
import re
import shutil
import subprocess
import tempfile
import unittest

from tests.test_packaging import REPO, UNIT, read, shell_function

DROP_INS = os.path.join(REPO, "contrib", "rnfs.service.d")
STRICT = os.path.join(DROP_INS, "strict.conf")


def settings(text: str) -> dict[str, str]:
    """{key: value} of a drop-in's [Service] lines."""
    return dict(line.split("=", 1) for line in text.splitlines() if re.match(r"^[A-Z]\w*=", line))


class StrictDropInTest(unittest.TestCase):

    def test_home_is_read_only_but_rnfs_reticulum_and_downloads(self):
        values = settings(read(STRICT))
        self.assertEqual(values["ProtectHome"], "read-only")
        writable = values["ReadWritePaths"].split()
        self.assertEqual(sorted(writable), sorted(["-%h/.rnfs", "-%h/.reticulum", "-%h/Downloads"]))
        # The user manager and session bus (systemd-run --user) and the agents live in the runtime directory
        hidden = values["InaccessiblePaths"].split()
        self.assertIn("-%t", hidden)
        for secret in ("-%h/.ssh", "-%h/.gnupg"):
            self.assertIn(secret, hidden)
        # hardening.conf's settings too, so either drop-in can be used alone
        for key, value in settings(read(os.path.join(DROP_INS, "hardening.conf"))).items():
            self.assertEqual(values[key], value, key)

    def test_documents_what_it_costs_and_leaves_open(self):
        text = read(STRICT)
        self.assertIn("rnfs get -o", text)
        self.assertIn("~/.reticulum", text)  # still writable: what other RNS programs run
        self.assertIn("systemctl --user edit rnfs", text)

    def test_units_and_hardening_point_to_it_without_overstating(self):
        for name, text in (("contrib/rnfs.service", read(UNIT)), ("install.sh", shell_function("write_unit")),
                           ("hardening.conf", read(os.path.join(DROP_INS, "hardening.conf")))):
            with self.subTest(name):
                self.assertIn("strict.conf", text)
                self.assertRegex(text, r"start (an unsandboxed process|processes outside the sandbox)")

    @unittest.skipUnless(shutil.which("systemd-analyze"), "needs systemd-analyze")
    def test_systemd_accepts_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            unit = os.path.join(tmp, "rnfs.service")
            with open(unit, "w", encoding="utf-8") as f:
                f.write(re.sub(r"(?m)^ExecStart=.*$", f"ExecStart={shutil.which('true') or '/bin/true'} daemon",
                               read(UNIT)))
            os.mkdir(unit + ".d")
            shutil.copy(STRICT, unit + ".d")
            result = subprocess.run(["systemd-analyze", "--user", "verify", unit], capture_output=True, text=True,
                                    timeout=60)
            # It loads the drop-in from rnfs.service.d next to the unit, and names any line it ignores
            self.assertNotIn("strict.conf", result.stdout + result.stderr)
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
