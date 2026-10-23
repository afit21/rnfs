"""Tests for rnfs/launch.py: what daemon.json says started the daemon, and the command line it restarts with."""
import sys
import unittest
from unittest import mock

from rnfs import launch

SERVICE_CGROUP = "0::/user.slice/user-1000.slice/user@1000.service/app.slice/rnfs.service\n"
TERMINAL_CGROUP = "0::/user.slice/user-1000.slice/user@1000.service/app.slice/vte-spawn-1234.scope\n"


class HowStartedTest(unittest.TestCase):

    def started(self, environ=None, platform="linux", parent=4321, cgroup="", interactive=False) -> dict:
        """how_started with everything given."""
        return launch.how_started(environ or {}, platform, parent, cgroup, interactive)

    def test_systemd_needs_a_service_cgroup(self):
        """A systemd service is told by its cgroup: INVOCATION_ID alone may be a terminal's, inherited."""
        self.assertEqual(self.started({"INVOCATION_ID": "ab12"}, cgroup=SERVICE_CGROUP),
                         {"started_by": "systemd", "service": "rnfs.service", "sandbox": None, "can_restart": True})
        self.assertEqual(self.started({"INVOCATION_ID": "ab12"}, cgroup=TERMINAL_CGROUP, interactive=True)
                         ["started_by"], "terminal")
        self.assertEqual(self.started({}, cgroup=SERVICE_CGROUP)["started_by"], "unknown")  # no INVOCATION_ID

    def test_launchd_and_terminals(self):
        self.assertEqual(self.started({"XPC_SERVICE_NAME": "io.github.afit21.rnfs"}, platform="darwin", parent=1),
                         {"started_by": "launchd", "service": "io.github.afit21.rnfs", "sandbox": None,
                          "can_restart": True})
        self.assertEqual(self.started({}, platform="darwin", parent=500, interactive=True)["started_by"], "terminal")

    def test_given_by_whoever_started_it(self):
        """RNFS_STARTED_BY wins when short and printable; Flatpak is a sandbox, whoever started it."""
        self.assertEqual(self.started({"RNFS_STARTED_BY": "app:rnfs-desktop", "FLATPAK_ID": "io.github.afit21.rnfs",
                                       "INVOCATION_ID": "ab12"}, cgroup=SERVICE_CGROUP),
                         {"started_by": "app:rnfs-desktop", "service": None, "sandbox": "flatpak",
                          "can_restart": True})
        for bad in ("x" * 65, "line\nbreak", "  "):
            with self.subTest(bad=bad):
                self.assertEqual(self.started({"RNFS_STARTED_BY": bad})["started_by"], "unknown")

    def test_windows_restarts_only_under_the_task_wrapper(self):
        """Windows has no exec: only the wrapper's daemon can restart (by exiting with EXIT_RESTART)."""
        self.assertEqual(self.started({"RNFS_STARTED_BY": "windows_task"}, platform="win32")["can_restart"], True)
        self.assertEqual(self.started({}, platform="win32", interactive=True),
                         {"started_by": "terminal", "service": None, "sandbox": None, "can_restart": False})


class RestartArgvTest(unittest.TestCase):

    def test_the_command_line_it_was_started_with(self):
        """The interpreter with its original command line; a frozen build's own executable and argv."""
        with mock.patch.object(sys, "orig_argv", ["python3", "-m", "rnfs", "daemon"], create=True):
            self.assertEqual(launch.restart_argv(), (sys.executable, ["python3", "-m", "rnfs", "daemon"]))
        with mock.patch.object(sys, "frozen", True, create=True), \
                mock.patch.object(sys, "argv", ["/opt/rnfs/rnfs", "daemon", "--slow"]):
            self.assertEqual(launch.restart_argv(), (sys.executable, ["/opt/rnfs/rnfs", "daemon", "--slow"]))


if __name__ == "__main__":
    unittest.main()
