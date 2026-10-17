"""Tests for rnfs.log: every log line escapes non-printable characters (remote names reach the log); ThrottledLog."""
import threading
import types
import unittest
from unittest import mock

from rnfs import api, cli, fsutil, log
from tests.support import FakeClock


class LogTest(unittest.TestCase):
    """log.log escapes centrally; the old helpers are the same function."""

    def test_log_escapes_control_characters(self):
        with mock.patch("RNS.log") as rns_log:
            log.log("got '\x1b]0;owned\x07' then\nrnfs fake line\r", log.LOG_WARNING)
        message, level = rns_log.call_args.args
        self.assertEqual(level, log.LOG_WARNING)
        self.assertTrue(message.isprintable(), message)
        self.assertEqual(message, "rnfs got '\\x1b]0;owned\\x07' then\\nrnfs fake line\\r")
        with mock.patch("RNS.log") as rns_log:
            log.log("plain été")
        self.assertEqual(rns_log.call_args.args[0], "rnfs plain été")

    def test_helpers_share_one_implementation(self):
        self.assertIs(api.loggable, fsutil.printable)
        self.assertIs(cli.printable, fsutil.printable)

    def test_feature_log_levels_are_rns_levels(self):
        """rnfs.features._common names RNS's loglevels without importing RNS: they must stay the same."""
        from rnfs.features import _common
        for level in ("LOG_ERROR", "LOG_WARNING", "LOG_NOTICE", "LOG_INFO", "LOG_VERBOSE"):
            self.assertEqual(getattr(_common, level), getattr(log, level), level)

    def test_cap_warnings(self):
        self.assertEqual(log.cap_warnings(["a", "b"]), {"warnings": ["a", "b"]})
        capped = log.cap_warnings([str(i) for i in range(log.MAX_WARNINGS + 1)])
        self.assertEqual(capped["warnings_total"], log.MAX_WARNINGS + 1)
        self.assertEqual(capped["warnings"][-1], "... and 1 more warning")
        self.assertEqual(len(capped["warnings"]), log.MAX_WARNINGS + 1)


class ThrottledLogTest(unittest.TestCase):
    """log.ThrottledLog: a line at most every interval, counting the ones left out."""

    def setUp(self):
        """A throttled log on a fake clock, its lines recorded."""
        self.clock = FakeClock()
        self.throttled = log.ThrottledLog(60.0, self.clock)
        self.lines = []
        patcher = mock.patch.object(log, "log", lambda message, level: self.lines.append((message, level)))
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_one_line_per_interval_counting_the_rest(self):
        self.throttled("refused one", log.LOG_WARNING)
        for _ in range(5):
            self.clock.now += 10
            self.throttled("refused another", log.LOG_WARNING)
        self.assertEqual(self.lines, [("refused one", log.LOG_WARNING)])
        self.clock.now += 10  # 60 s after the first line
        self.throttled("refused a seventh", log.LOG_VERBOSE)
        self.assertEqual(self.lines[1], ("refused a seventh (5 more since the last)", log.LOG_VERBOSE))
        self.clock.now += 59
        self.throttled("not yet", log.LOG_WARNING)
        self.clock.now += 120
        self.throttled("again", log.LOG_WARNING)
        self.assertEqual(self.lines[2:], [("again (1 more since the last)", log.LOG_WARNING)])

    def test_due(self):
        """due() is the check alone, for a caller that words its own line (RequestServer._over_rate)."""
        self.assertEqual(self.throttled.due(), 0)
        self.assertIsNone(self.throttled.due())
        self.assertIsNone(self.throttled.due())
        self.clock.now += 60
        self.assertEqual(self.throttled.due(), 2)
        self.assertEqual(self.lines, [])

    def test_thread_safe(self):
        throttled = log.ThrottledLog(3600.0)
        threads = [threading.Thread(target=lambda: [throttled("x", log.LOG_WARNING) for _ in range(2000)])
                   for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(self.lines), 1)
        self.assertEqual(throttled.skipped, 7999)



class ThrottledTracebackTest(unittest.TestCase):
    """What the unexpected-exception sites (XC4) rely on."""

    def test_traceback_reaches_the_debug_log_inside_an_except(self):
        """Logged inside an except at LOG_WARNING, a throttled line carries the traceback to the debug log, as
        rnfs.log does for any line; the lines it leaves out write nothing."""
        from rnfs import debuglog
        throttled = log.ThrottledLog(60.0, FakeClock())
        written = []
        debug = types.SimpleNamespace(write=lambda level, source, text, trace: written.append((text, trace)))
        with mock.patch.object(debuglog, "_active", debug), mock.patch("RNS.log"):
            self.addCleanup(log.set_debug_sink, log._debug_sink)
            log.set_debug_sink(debuglog._rnfs_message)
            for _ in range(2):
                try:
                    raise TypeError("a bug")
                except TypeError:
                    throttled("unexpected", log.LOG_WARNING)
        self.assertEqual(len(written), 1)
        text, trace = written[0]
        self.assertEqual(text, "unexpected")
        self.assertIn("TypeError: a bug", trace)


if __name__ == "__main__":
    unittest.main()
