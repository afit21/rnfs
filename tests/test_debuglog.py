"""Tests for the debug log file (rnfs/debuglog.py), the hooks that feed it, and what gateway requests write there."""
import contextlib
import io
import os
import stat
import tempfile
import threading
import unittest
from unittest import mock

import RNS

from rnfs import __version__, debuglog, log
from rnfs.config import Config
from rnfs.gateway import GatewayServer
from tests.support import POSIX_MODES, wait_for
from tests.test_api import FakeNode, raw_request


class DebugLogTestCase(unittest.TestCase):
    """Base: a debug log started over a temporary rnfs dir, stopped (hooks undone) after each test."""

    def setUp(self):
        """Start the debug log with RNS printing to the console, as the daemon does."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = Config(rnfs_dir=self.tmp.name, debug_log=True, gateway_fetches=3)
        self.addCleanup(self.restore_rns, RNS.logdest, RNS.loglevel)
        RNS.logdest = RNS.LOG_STDOUT
        self.addCleanup(debuglog.stop)
        self.debug = debuglog.start(self.config)

    @staticmethod
    def restore_rns(logdest, loglevel):
        """Put RNS's logging back as it was."""
        RNS.logdest, RNS.loglevel = logdest, loglevel

    def lines(self, path: str | None = None) -> list[str]:
        """The file's lines."""
        with open(path or self.debug.path, encoding="utf-8") as f:
            return f.read().splitlines()

    def logged(self, needle: str) -> list[str]:
        """The lines containing needle."""
        return [line for line in self.lines() if needle in line]


class FileTest(DebugLogTestCase):

    def test_begins_with_the_version_then_versions_and_settings(self):
        first, second, third = self.lines()[:3]
        self.assertTrue(first.startswith(f"rnfs {__version__} debug log, started "), first)
        self.assertIn(f"(pid {os.getpid()})", first)
        self.assertTrue(second.startswith("Python "), second)
        self.assertIn("RNS ", second)
        self.assertIn("'debug_log': True", third)
        self.assertIn("'gateway_fetches': 3", third)
        self.assertNotIn("'max_storage'", third)  # only settings that differ from the defaults
        self.assertEqual(self.debug.path, os.path.join(self.tmp.name, "debug.log"))

    @unittest.skipUnless(POSIX_MODES, "Windows has no POSIX permission bits")
    def test_readable_by_its_owner_only(self):
        """The debug log is readable by its owner only."""
        self.assertEqual(stat.S_IMODE(os.stat(self.debug.path).st_mode) & 0o077, 0)

    def test_each_start_keeps_the_last_run_as_dot_1(self):
        log.log("from the first run", log.LOG_NOTICE)
        debuglog.stop()
        self.debug = debuglog.start(self.config)
        self.assertTrue(self.lines(self.debug.path + ".1")[0].startswith(f"rnfs {__version__} debug log"))
        self.assertTrue(any("from the first run" in line for line in self.lines(self.debug.path + ".1")))
        self.assertFalse(self.logged("from the first run"))

    def test_a_full_file_moves_aside_and_the_next_begins_with_the_version_too(self):
        small = debuglog.DebugLog(os.path.join(self.tmp.name, "small.log"), {}, max_size=2000)
        self.addCleanup(small.close)
        for i in range(60):
            small.note(f"line {i} " + "x" * 40)
        current = self.lines(small.path)
        self.assertTrue(current[0].startswith(f"rnfs {__version__} debug log, continued "), current[0])
        self.assertLessEqual(os.path.getsize(small.path), 2000)
        self.assertTrue(os.path.exists(small.path + ".1"))
        self.assertIn("line 59", current[-1])

    def test_a_failing_disk_stops_it_quietly(self):
        stderr = io.StringIO()
        with mock.patch("os.write", side_effect=OSError(28, "No space left on device")), \
                contextlib.redirect_stderr(stderr):
            log.log("lost", log.LOG_NOTICE)
            log.log("lost too", log.LOG_NOTICE)
        self.assertEqual(stderr.getvalue().count("debug log"), 1)
        log.log("after", log.LOG_NOTICE)
        self.assertFalse(self.logged("after"))


class MessagesTest(DebugLogTestCase):

    def test_every_rnfs_message_whatever_the_console_shows(self):
        RNS.loglevel = RNS.LOG_ERROR  # the console shows errors only
        log.log("per-query detail", log.LOG_VERBOSE)
        log.log("debug detail", log.LOG_DEBUG)
        verbose, = self.logged("per-query detail")
        self.assertIn("[Verbose]", verbose)
        self.assertIn(f"[{threading.current_thread().name}] rnfs: per-query detail", verbose)
        self.assertIn("[Debug]", self.logged("debug detail")[0])

    def test_lines_are_escaped(self):
        log.log("name '\x1b]0;owned\x07'\nrnfs fake line", log.LOG_WARNING)
        line, = self.logged("owned")
        self.assertTrue(line.isprintable())
        self.assertEqual(self.logged("rnfs fake line"), [line])  # one line: the newline is escaped

    def test_an_expected_outcome_logged_in_an_except_block_has_no_traceback(self):
        try:
            raise ValueError("a refusal")
        except ValueError:
            log.log("refused, as expected", log.LOG_DEBUG)
        self.assertTrue(self.lines()[-1].endswith("rnfs: refused, as expected"))

    def test_a_message_logged_while_handling_an_exception_has_its_traceback(self):
        try:
            raise ValueError("the cause")
        except ValueError as e:
            log.log(f"it failed: {e!r}", log.LOG_ERROR)
        lines = self.lines()
        at = next(i for i, line in enumerate(lines) if "it failed" in line)
        trace = lines[at + 1:at + 6]
        self.assertTrue(all(line.startswith("    | ") for line in trace[:2]), trace)
        self.assertTrue(any("ValueError: the cause" in line for line in lines[at + 1:]))
        log.log("no exception here", log.LOG_ERROR)
        self.assertTrue(self.lines()[-1].endswith("rnfs: no exception here"))  # nothing below it

    def test_rns_messages_still_print_and_are_copied_once(self):
        RNS.loglevel = RNS.LOG_INFO
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            RNS.log("TCPClientInterface[backbone] could not connect", RNS.LOG_ERROR)
            log.log("an rnfs line", log.LOG_NOTICE)
        printed = out.getvalue()
        self.assertIn("could not connect", printed)
        self.assertIn("rnfs an rnfs line", printed)
        rns, = self.logged("could not connect")
        self.assertIn("[Error]", rns)
        self.assertIn("RNS: TCPClientInterface[backbone] could not connect", rns)
        self.assertEqual(len(self.logged("an rnfs line")), 1)  # by rnfs.log's sink, not again as RNS output
        self.assertIn("rnfs: an rnfs line", self.logged("an rnfs line")[0])

    def test_rns_detail_below_info_isnt_copied(self):
        RNS.loglevel = RNS.LOG_DEBUG
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            RNS.log("Cleaning known destinations at background priority...", RNS.LOG_DEBUG)
            RNS.log("Link establishment timed out", RNS.LOG_VERBOSE)
            RNS.log("Reconnected socket for TCPInterface[backbone]", RNS.LOG_INFO)
        self.assertIn("Cleaning known destinations", out.getvalue())  # the console is as before
        self.assertFalse(self.logged("Cleaning known destinations"))
        self.assertFalse(self.logged("Link establishment timed out"))
        self.assertTrue(self.logged("RNS: Reconnected socket"))

    def test_a_thread_that_dies_is_logged(self):
        def die():
            """Raise in a thread."""
            raise RuntimeError("boom")

        with contextlib.redirect_stderr(io.StringIO()):  # the usual report still goes to stderr
            thread = threading.Thread(target=die, name="rnfs-test-thread")
            thread.start()
            thread.join()
        died, = self.logged("rnfs-test-thread died of RuntimeError")
        self.assertIn("[Error]", died)
        self.assertTrue(self.logged("RuntimeError: boom"))

    def test_stop_undoes_every_hook(self):
        hook = threading.excepthook
        debuglog.stop()
        self.assertIsNone(debuglog.current())
        self.assertEqual(RNS.logdest, RNS.LOG_STDOUT)
        self.assertIsNot(threading.excepthook, hook)
        log.log("after stop", log.LOG_NOTICE)
        self.assertFalse(self.logged("after stop"))

    def test_status_line(self):
        node = mock.Mock(links=None)
        node.status.return_value = {"known_nodes": 4, "pins": 2}
        line = debuglog.status_line(node)
        self.assertIn("'known_nodes': 4", line)
        self.assertIn("interfaces:", line)


class GatewayRequestsTest(DebugLogTestCase):
    """What the gateway writes for each request."""

    def setUp(self):
        """A gateway over a FakeNode, with the debug log on."""
        super().setUp()
        self.node = FakeNode()
        self.server = GatewayServer(self.node, self.config, 0)
        self.server.start()
        self.addCleanup(self.server.stop)
        self.text = self.node.put_file(b"hello gateway")

    def test_a_request_and_its_outcome(self):
        status, _, body = raw_request(self.server.port, "GET", f"/rnfs/{self.text}", None,
                                      {"Range": "bytes=0-4"}, "default")
        self.assertEqual((status, body), (206, b"hello"))
        start, = self.logged(f"gateway GET /rnfs/{self.text} (")
        self.assertIn("Range: bytes=0-4", start)
        number = start.split("gateway #")[1].split("]")[0]
        # The outcome is written once the response is sent, which the client may have read whole already
        wait_for(lambda: any("gateway answered" in line for line in self.logged(f"gateway #{number}]")),
                 what="the request's outcome in the debug log")
        tagged = self.logged(f"gateway #{number}]")
        self.assertIn("is stored here", "\n".join(tagged))
        self.assertIn("5 bytes from byte 0 of 13", "\n".join(tagged))
        self.assertIn("gateway answered 206", tagged[-1])
        self.assertIn("bytes written (headers included) in", tagged[-1])

    def test_refusals_are_logged_with_their_reason(self):
        status, _, _ = raw_request(self.server.port, "GET", "/nowhere", None, None, "default")
        self.assertEqual(status, 404)
        self.assertTrue(self.logged("gateway refused /nowhere with 404"))
        self.assertIn("gateway answered 404", self.logged("gateway answered")[-1])


if __name__ == "__main__":
    unittest.main()
