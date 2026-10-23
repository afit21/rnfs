"""Tests for rnfs/recentlog.py and GET /api/log: the daemon's recent log lines, kept in memory."""
import contextlib
import io
import unittest
from unittest import mock

import RNS

from rnfs import log, recentlog
from rnfs.recentlog import RecentLines
from tests.test_api import ApiTestCase


class RecentLinesTest(unittest.TestCase):

    def test_since_limit_level_and_dropped(self):
        lines = RecentLines(max_lines=3, clock=lambda: 100.0)
        for n, level in enumerate((RNS.LOG_NOTICE, RNS.LOG_ERROR, RNS.LOG_INFO, RNS.LOG_WARNING), 1):
            lines.add(level, "rnfs", f"line {n}")
        got = lines.since()
        self.assertEqual(([line["text"] for line in got["lines"]], got["next"], got["dropped"]),
                         (["line 2", "line 3", "line 4"], 4, True))  # line 1 is no longer held
        self.assertEqual(got["lines"][0], {"seq": 2, "time": 100.0, "level": RNS.LOG_ERROR, "level_name": "Error",
                                           "source": "rnfs", "text": "line 2"})
        self.assertEqual(lines.since(after=2), {"lines": got["lines"][1:], "next": 4, "dropped": False})
        self.assertEqual([line["seq"] for line in lines.since(limit=1)["lines"]], [4])
        self.assertEqual([line["seq"] for line in lines.since(max_level=RNS.LOG_WARNING)["lines"]], [2, 4])
        self.assertEqual(lines.since(after=4), {"lines": [], "next": 4, "dropped": False})
        lines.add(RNS.LOG_INFO, "rnfs", "x" * (recentlog.MAX_TEXT + 10))
        self.assertEqual(len(lines.since(after=4)["lines"][0]["text"]), recentlog.MAX_TEXT)

    def test_rnfs_messages_at_the_console_level(self):
        """rnfs.log keeps what the console shows: messages at RNS's loglevel or more severe, escaped."""
        fresh = RecentLines()
        with mock.patch.object(recentlog, "LINES", fresh), mock.patch.object(RNS, "loglevel", RNS.LOG_NOTICE), \
                mock.patch.object(RNS, "log"):
            log.log("shown\x1b[31m", RNS.LOG_NOTICE)
            log.log("too detailed", RNS.LOG_VERBOSE)
        self.assertEqual([(line["source"], line["text"]) for line in fresh.since()["lines"]],
                         [("rnfs", "shown\\x1b[31m")])

    def test_capture_keeps_reticulums_output(self):
        """capture() keeps RNS's own lines (level read from them), not rnfs's twice, and still prints them."""
        fresh = RecentLines()
        saved = (RNS.logdest, RNS.logcall, RNS.loglevel)
        self.addCleanup(lambda: (setattr(RNS, "logdest", saved[0]), setattr(RNS, "logcall", saved[1]),
                                 setattr(RNS, "loglevel", saved[2])))
        RNS.logdest, RNS.loglevel = RNS.LOG_STDOUT, RNS.LOG_NOTICE
        out = io.StringIO()
        with mock.patch.object(recentlog, "LINES", fresh), contextlib.redirect_stdout(out):
            recentlog.capture()
            recentlog.capture()  # idempotent
            RNS.log("TCPInterface failed to connect", RNS.LOG_ERROR)
            log.log("an rnfs message", RNS.LOG_NOTICE)
        self.assertEqual([(line["source"], line["level"], line["text"]) for line in fresh.since()["lines"]],
                         [("RNS", RNS.LOG_ERROR, "TCPInterface failed to connect"),
                          ("rnfs", RNS.LOG_NOTICE, "an rnfs message")])
        self.assertIn("TCPInterface failed to connect", out.getvalue())
        self.assertEqual(out.getvalue().count("an rnfs message"), 1)


class LogRouteTest(ApiTestCase):

    def test_route(self):
        fresh = RecentLines()
        fresh.add(RNS.LOG_WARNING, "RNS", "interface down")
        fresh.add(RNS.LOG_INFO, "rnfs", "fetched")
        with mock.patch.object(recentlog, "LINES", fresh):
            got = self.ok("GET", "/api/log")
            self.assertEqual([line["text"] for line in got["lines"]], ["interface down", "fetched"])
            self.assertEqual([line["text"] for line in self.ok("GET", "/api/log?level=warning")["lines"]],
                             ["interface down"])
            self.assertEqual(self.ok("GET", "/api/log?after=1&level=6")["lines"][0]["text"], "fetched")
            for bad in ("level=loud", "level=9", "limit=1001", "after=x"):
                with self.subTest(bad=bad):
                    self.assertEqual(self.call("GET", f"/api/log?{bad}")[0], 400)


if __name__ == "__main__":
    unittest.main()
