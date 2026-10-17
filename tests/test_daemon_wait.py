"""The daemon's main thread waits for a stop signal without waking each second on POSIX (S7, rnfs/cli.py)."""
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from unittest import mock

from rnfs import cli, fsutil
from tests.support import free_port, wait_for

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
POSIX = os.name == "posix"

# A Reticulum of its own with no interfaces: the daemon starts without touching any network
RNS_CONFIG = """
[reticulum]
  enable_transport = No
  share_instance = No
[logging]
  loglevel = 2
[interfaces]
"""


class RecordingEvent(threading.Event):
    """An Event recording each wait's timeout; it is set once stop_after waits returned unset (never blocking)."""

    def __init__(self, stop_after: int):
        """Unset, nothing recorded."""
        super().__init__()
        self.timeouts = []
        self.stop_after = stop_after

    def wait(self, timeout=None):
        """Record timeout and return at once, set from the stop_after-th wait on."""
        self.timeouts.append(timeout)
        if len(self.timeouts) >= self.stop_after:
            self.set()
        return super().wait(0)


class WaitUntilStoppingTest(unittest.TestCase):
    """cli.wait_until_stopping, as run_daemon uses it."""

    def test_waits_without_a_timeout_on_posix(self):
        stopping = RecordingEvent(stop_after=1)
        with mock.patch.object(fsutil, "IS_WINDOWS", False):
            cli.wait_until_stopping(stopping)
        self.assertEqual(stopping.timeouts, [None])

    def test_wakes_each_second_on_windows(self):
        # A wait isn't interrupted for signal handlers there
        stopping = RecordingEvent(stop_after=3)
        with mock.patch.object(fsutil, "IS_WINDOWS", True):
            cli.wait_until_stopping(stopping)
        self.assertEqual(stopping.timeouts, [1.0, 1.0, 1.0])
        stopping = RecordingEvent(stop_after=2)
        with mock.patch.object(fsutil, "IS_WINDOWS", True):
            cli.wait_until_stopping(stopping, 0.25, lambda: None)
        self.assertTrue(all(0 <= timeout <= 0.25 for timeout in stopping.timeouts), stopping.timeouts)

    def test_task_runs_every_interval(self):
        # The debug log's status line: the wait lasts until it is due
        calls = []
        stopping = threading.Event()

        def task():
            """Count, and stop at the third."""
            calls.append(time.monotonic())
            if len(calls) == 3:
                stopping.set()

        start = time.monotonic()
        cli.wait_until_stopping(stopping, 0.1, task)
        self.assertEqual(len(calls), 3)
        self.assertGreaterEqual(calls[0] - start, 0.1)
        self.assertTrue(all(b - a >= 0.1 for a, b in zip(calls, calls[1:])), calls)

    @unittest.skipUnless(POSIX, "a signal interrupts a wait on POSIX only")
    def test_a_signal_ends_a_wait_without_timeout(self):
        # Other threads are running, as in the daemon, and the signal is sent to the process (as kill does)
        for signum in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signal=signal.Signals(signum).name):
                stopping = threading.Event()
                handled = []

                def handler(number, frame):
                    """As run_daemon's: set stopping."""
                    handled.append(number)
                    stopping.set()

                previous = signal.signal(signum, handler)
                idle = threading.Event()
                busy = [threading.Thread(target=idle.wait, daemon=True) for _ in range(4)]
                for thread in busy:
                    thread.start()
                sender = threading.Timer(0.3, os.kill, (os.getpid(), signum))
                # If the signal never ended the wait, end it anyway so the suite goes on (and the test fails)
                watchdog = threading.Timer(20, stopping.set)
                start = time.monotonic()
                sender.start()
                watchdog.start()
                try:
                    cli.wait_until_stopping(stopping)
                    elapsed = time.monotonic() - start
                finally:
                    watchdog.cancel()
                    idle.set()
                    signal.signal(signum, previous)
                self.assertEqual(handled, [signum])
                self.assertLess(elapsed, 10)


@unittest.skipUnless(POSIX, "Windows stops the daemon with CTRL_BREAK_EVENT, sent to a process group")
class DaemonSignalTest(unittest.TestCase):
    """A real daemon (no interfaces) stops cleanly on SIGTERM and on SIGINT while its main thread waits unbounded."""

    def test_daemon_stops_on_sigterm_and_sigint(self):
        for signum in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signal=signal.Signals(signum).name):
                tmp = tempfile.TemporaryDirectory()
                self.addCleanup(tmp.cleanup)
                rnfs_dir = os.path.join(tmp.name, "rnfs")
                os.makedirs(rnfs_dir, mode=0o700)
                with open(os.path.join(rnfs_dir, "config"), "w", encoding="utf-8") as f:
                    f.write(RNS_CONFIG)
                with open(os.path.join(rnfs_dir, "rnfs.toml"), "w", encoding="utf-8") as f:
                    f.write("default_bootstrap = false\nlxmf_enabled = false\n")
                api_port = free_port()
                log_path = os.path.join(tmp.name, "daemon.log")
                with open(log_path, "wb") as log:
                    proc = subprocess.Popen(
                        [sys.executable, "rnfs.py", "--dir", rnfs_dir, "--api-port", str(api_port), "daemon",
                         "--rnsconfig", rnfs_dir, "--gateway-port", str(free_port())],
                        cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, env={**os.environ, "PYTHONUNBUFFERED": "1"})
                try:
                    wait_for(lambda: self.status(rnfs_dir, api_port, proc), 60, "the daemon to start")
                    time.sleep(1.5)  # past the first waits of every loop
                    start = time.monotonic()
                    proc.send_signal(signum)
                    code = proc.wait(30)
                    elapsed = time.monotonic() - start
                finally:
                    if proc.poll() is None:
                        proc.kill()
                        proc.wait(30)
                with open(log_path, "rb") as f:
                    output = f.read().decode("utf-8", "replace")
                self.assertEqual(code, 0, output)
                self.assertLess(elapsed, 20, output)
                self.assertFalse(os.path.exists(os.path.join(rnfs_dir, cli.TOKEN_FILE)), output)  # a clean stop

    @staticmethod
    def status(rnfs_dir: str, api_port: int, proc) -> dict | None:
        """The daemon's status once its API answers, else None; fails if the daemon exited."""
        if proc.poll() is not None:
            raise AssertionError("the daemon exited")
        try:
            with open(os.path.join(rnfs_dir, cli.TOKEN_FILE), encoding="ascii") as f:
                token = f.read().strip()
            request = urllib.request.Request(f"http://127.0.0.1:{api_port}/api/status",
                                             headers={"X-Rnfs-Api": token})
            with urllib.request.urlopen(request, timeout=2) as response:
                return json.load(response)
        except OSError:
            return None


if __name__ == "__main__":
    unittest.main()
