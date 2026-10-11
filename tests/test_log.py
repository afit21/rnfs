"""Tests for rnfs.log: every log line escapes non-printable characters (remote names reach the log)."""
import unittest
from unittest import mock

from rnfs import api, cli, fsutil, log


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

    def test_cap_warnings(self):
        self.assertEqual(log.cap_warnings(["a", "b"]), {"warnings": ["a", "b"]})
        capped = log.cap_warnings([str(i) for i in range(log.MAX_WARNINGS + 1)])
        self.assertEqual(capped["warnings_total"], log.MAX_WARNINGS + 1)
        self.assertEqual(capped["warnings"][-1], "... and 1 more warning")
        self.assertEqual(len(capped["warnings"]), log.MAX_WARNINGS + 1)


if __name__ == "__main__":
    unittest.main()
