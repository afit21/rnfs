"""AUD-L16, contrib/install-windows-task.ps1: the scheduled task was named "rnfs daemon" for every user, in Task
Scheduler's machine-wide root folder, so a second user's install failed with "Access is denied", and an
administrator's -Stop or -Uninstall found and stopped the first user's task and daemon processes. Now the task is
named for the user's SID, the processes stopped are only the user's own, and the old shared name is stopped or
removed only when that task runs this user's own wrapper (the upgrade path).

Static checks of the script: no PowerShell runs here (the script needs Windows' Task Scheduler and CIM anyway).
"""
import os
import re
import unittest

from tests.test_packaging import REPO, read

SCRIPT = os.path.join(REPO, "contrib", "install-windows-task.ps1")


def code(text: str) -> str:
    """The script without comment lines."""
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


class WindowsTaskTest(unittest.TestCase):

    def setUp(self):
        self.text = read(SCRIPT)
        self.code = code(self.text)

    def test_task_is_named_for_the_user(self):
        self.assertIn("$Sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value", self.code)
        self.assertRegex(self.code, r'(?m)^\$TaskName = "rnfs daemon \(\$Sid\)"$')
        # Every task operation names a variable, never the old name as a literal
        for call in re.findall(r"(?:Get|Stop|Start|Register|Unregister)-ScheduledTask[^\n]*", self.code):
            self.assertNotIn('"rnfs daemon"', call)

    def test_old_shared_task_only_if_its_ours(self):
        self.assertRegex(self.code, r'(?m)^\$OldTaskName = "rnfs daemon"')
        own = re.search(r"(?ms)^function Test-OwnOldTask \{\n(.*?)^\}", self.code)
        self.assertTrue(own)
        self.assertIn("$Wrapper", own.group(1))
        # Every use of the old name outside that check is guarded by it
        lines = self.code.splitlines()
        for number, line in enumerate(lines):
            if "$OldTaskName" in line and not line.startswith(("$OldTaskName =", "    $task = Get-ScheduledTask")):
                context = "\n".join(lines[max(0, number - 3):number + 1])
                self.assertIn("Test-OwnOldTask", context, line)

    def test_only_the_users_processes_are_stopped(self):
        self.assertIn("-MethodName GetOwnerSid", self.code)
        stop = re.search(r"(?ms)^function Stop-RnfsTask\(\[string\]\$Name\) \{\n(.*?)^\}", self.code)
        self.assertTrue(stop)
        self.assertEqual(stop.group(1).count("(Get-OwnerSid $_) -eq $Sid"), 1)  # the task's own PowerShell
        self.assertEqual(stop.group(1).count("(Get-OwnerSid $child) -eq $Sid"), 1)  # its descendants
        self.assertNotIn("$TaskName", stop.group(1))

    def test_brackets_balance(self):
        """A cheap stand-in for a parse, which needs PowerShell: (), {} and [] balance outside strings."""
        stripped = re.sub(r"(?ms)@'\n.*?\n'@", "", self.code)  # the wrapper's here-string
        stripped = re.sub(r"'[^'\n]*'|\"(?:[^\"`\n]|`.)*\"", "", stripped)
        for pair in ("()", "{}", "[]"):
            self.assertEqual(stripped.count(pair[0]), stripped.count(pair[1]), pair)


if __name__ == "__main__":
    unittest.main()
