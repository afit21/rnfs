"""Tests for fsutil: replace and remove retried while Windows reports a file in use, Windows name rules."""
import os
import tempfile
import unittest
from unittest import mock

from rnfs import fsutil


def in_use(failures: int, real):
    """A side effect for os.replace or os.remove failing as Windows does failures times, then calling real."""
    calls = []

    def operation(*args):
        """Fail while another handle has the file open, then succeed."""
        calls.append(args)
        if len(calls) <= failures:
            raise PermissionError(32, "The process cannot access the file because it is being used", args[0])
        return real(*args)

    return operation


class RetryTest(unittest.TestCase):
    """fsutil.replace and fsutil.remove, with Windows behaviour simulated on any platform."""

    def setUp(self):
        """A temporary directory with a file in it; no real waiting between retries."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "file")
        with open(self.path, "wb") as f:
            f.write(b"x")
        patcher = mock.patch.object(fsutil, "RETRY_DELAYS", (0, 0, 0))
        patcher.start()
        self.addCleanup(patcher.stop)

    def windows(self):
        """Pretend to be on Windows."""
        return mock.patch.object(fsutil, "IS_WINDOWS", True)

    def test_replace_retries_on_windows(self):
        target = os.path.join(self.tmp.name, "target")
        with self.windows(), mock.patch("os.replace", side_effect=in_use(2, os.replace)) as replace:
            fsutil.replace(self.path, target)
        self.assertEqual(replace.call_count, 3)
        self.assertTrue(os.path.exists(target))

    def test_replace_raises_once_retries_run_out(self):
        with self.windows(), mock.patch("os.replace", side_effect=in_use(10, os.replace)) as replace:
            with self.assertRaises(PermissionError):
                fsutil.replace(self.path, os.path.join(self.tmp.name, "target"))
        self.assertEqual(replace.call_count, 4)  # one attempt per delay, then a last one

    def test_remove_retries_then_defers_on_windows(self):
        with self.windows(), mock.patch("os.remove", side_effect=in_use(1, os.remove)):
            self.assertTrue(fsutil.remove(self.path))
        self.assertFalse(os.path.exists(self.path))
        with open(self.path, "wb"):
            pass
        with self.windows(), mock.patch("os.remove", side_effect=in_use(10, os.remove)):
            self.assertFalse(fsutil.remove(self.path))  # still in use: the caller tries again later
        self.assertTrue(os.path.exists(self.path))

    def test_missing_file_is_removed(self):
        self.assertTrue(fsutil.remove(os.path.join(self.tmp.name, "missing")))
        with self.windows():
            self.assertTrue(fsutil.remove(os.path.join(self.tmp.name, "missing")))

    def test_no_retry_elsewhere(self):
        with mock.patch.object(fsutil, "IS_WINDOWS", False):
            with mock.patch("os.remove", side_effect=in_use(1, os.remove)) as remove, \
                    self.assertRaises(PermissionError):
                fsutil.remove(self.path)
            self.assertEqual(remove.call_count, 1)
            with mock.patch("os.replace", side_effect=in_use(1, os.replace)) as replace, \
                    self.assertRaises(PermissionError):
                fsutil.replace(self.path, self.path + ".new")
            self.assertEqual(replace.call_count, 1)


class WindowsNameTest(unittest.TestCase):
    """Names Windows can't take as file names, and the retry budget."""

    def test_retries_take_about_a_second(self):
        self.assertTrue(0.5 <= sum(fsutil.RETRY_DELAYS) <= 1.5)

    def test_reasons(self):
        for name in ("CON", "con.txt", "Nul", "aux.tar.gz", "COM1", "lpt9.log", "com²", "nul .txt",
                     "a:b", "a<b", "a|b", 'a"b', "a?b", "a*b", "a\\b", "tab\there", "dot.", "space "):
            self.assertIsNotNone(fsutil.windows_name_reason(name), name)
        for name in ("console", "COM10", "lpt", "file.txt", ".hidden", "a b", "été", "nul_file"):
            self.assertIsNone(fsutil.windows_name_reason(name), name)


class SafeDefaultNameTest(unittest.TestCase):
    """Remote-chosen names are output names only when printable, not hidden and one plain path component."""

    def test_normal_names_kept(self):
        for name in ("report.pdf", "a b", "été.txt", "日本語", "file.tar.gz", "x"):
            self.assertEqual(fsutil.safe_default_name(name, "CID"), name)

    def test_control_characters_fall_back(self):
        for name in ("\x1b]0;pwned\x07", "a\x07b", "\x1b[2Jclear", "\x9b31m", "tab\there", "new\nline", "a\x00b"):
            self.assertEqual(fsutil.safe_default_name(name, "CID"), "CID", repr(name))

    def test_hidden_and_path_names_fall_back(self):
        for name in (".bash_login", ".npmrc", ".", "..", "a/b", "../x", "a\\b", "", None, 42, b"bytes"):
            self.assertEqual(fsutil.safe_default_name(name, "CID"), "CID", repr(name))

    def test_windows_names_fall_back_on_windows(self):
        with mock.patch.object(fsutil, "IS_WINDOWS", True):
            for name in ("CON", "nul.txt", "COM1", "a:b", "dot.", "a*b"):
                self.assertEqual(fsutil.safe_default_name(name, "CID"), "CID", name)
            self.assertEqual(fsutil.safe_default_name("console.log", "CID"), "console.log")
        with mock.patch.object(fsutil, "IS_WINDOWS", False):
            self.assertEqual(fsutil.safe_default_name("CON", "CID"), "CON")

    def test_printable_escapes(self):
        self.assertEqual(fsutil.printable("plain été"), "plain été")
        self.assertEqual(fsutil.printable("a\x1b[31mb\x07"), "a\\x1b[31mb\\x07")
        self.assertEqual(fsutil.printable(5), "5")


if __name__ == "__main__":
    unittest.main()
