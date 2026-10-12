"""Tests for fsutil: replace and remove retried while Windows reports a file in use, atomic writes, Windows
name rules."""
import errno
import os
import stat
import sys
import tempfile
import unittest
from unittest import mock

from rnfs import fsutil
from tests.support import CAN_SYMLINK, POSIX_MODES


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


class RenameNewTest(unittest.TestCase):
    """rename_new never replaces what is at the destination, by every route it may take."""

    def setUp(self):
        """A temporary directory with a file and a directory to rename."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.file = self.make("file", b"new")
        self.directory = os.path.join(self.tmp.name, "dir")
        os.mkdir(self.directory)

    def make(self, name: str, data: bytes) -> str:
        """A file in the temporary directory."""
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as f:
            f.write(data)
        return path

    def routes(self):
        """Each way rename_new can work here: renameat2, hard link (files) and check then rename."""
        no_renameat2 = mock.patch.object(fsutil, "_rename_noreplace", return_value=False)
        no_links = mock.patch("os.link", side_effect=PermissionError(1, "Operation not permitted"))
        yield "default", []
        yield "link", [no_renameat2]
        yield "check", [no_renameat2, no_links]

    def test_moves_files_and_directories(self):
        for route, patches in self.routes():
            with self.subTest(route):
                for patch in patches:
                    patch.start()
                try:
                    for src in (self.file, self.directory):
                        fsutil.rename_new(src, src + "." + route)
                        self.assertFalse(os.path.lexists(src))
                        os.rename(src + "." + route, src)
                finally:
                    mock.patch.stopall()
        with open(self.file, "rb") as f:
            self.assertEqual(f.read(), b"new")

    def test_never_replaces(self):
        existing = self.make("existing", b"old")
        for route, patches in self.routes():
            with self.subTest(route):
                for patch in patches:
                    patch.start()
                try:
                    for src in (self.file, self.directory):
                        with self.assertRaisesRegex(FileExistsError, "already exists"):
                            fsutil.rename_new(src, existing)
                        self.assertTrue(os.path.lexists(src))
                finally:
                    mock.patch.stopall()
        with open(existing, "rb") as f:
            self.assertEqual(f.read(), b"old")

    @unittest.skipUnless(sys.platform.startswith("linux"), "renameat2 is Linux's")
    def test_atomic_on_linux(self):
        # Even a destination created after any check would have run isn't replaced
        existing = self.make("existing", b"old")
        if not fsutil._rename_noreplace(self.make("probe", b""), os.path.join(self.tmp.name, "probed")):
            self.skipTest("no renameat2(RENAME_NOREPLACE) here")
        with mock.patch("os.path.lexists", return_value=False), \
                mock.patch("os.link", side_effect=AssertionError("not needed")):
            with self.assertRaises(FileExistsError):
                fsutil.rename_new(self.file, existing)
        with open(existing, "rb") as f:
            self.assertEqual(f.read(), b"old")


class WriteAtomicTest(unittest.TestCase):
    """write_atomic replaces a file whole or not at all, never through a symlink, and leaves no temporary file."""

    def setUp(self):
        """A temporary directory with an existing file in it."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "file")
        with open(self.path, "wb") as f:
            f.write(b"old")

    def contents(self) -> bytes:
        """What the file at self.path holds."""
        with open(self.path, "rb") as f:
            return f.read()

    def assert_only_file(self, data: bytes):
        """The directory holds the file with data, and no temporary file."""
        self.assertEqual(os.listdir(self.tmp.name), ["file"])
        self.assertEqual(self.contents(), data)

    def test_replaces_an_existing_file(self):
        fsutil.write_atomic(self.path, b"new\r\n\x00")
        self.assert_only_file(b"new\r\n\x00")  # binary: no newline translation on Windows
        os.remove(self.path)
        fsutil.write_atomic(self.path, b"")
        self.assert_only_file(b"")

    @unittest.skipUnless(POSIX_MODES, "Windows has no POSIX permission bits")
    def test_mode(self):
        umask = os.umask(0)
        try:
            for mode in (0o600, 0o644, 0o640):
                with self.subTest(mode=oct(mode)):
                    fsutil.write_atomic(self.path, b"x", mode=mode)
                    self.assertEqual(stat.S_IMODE(os.stat(self.path).st_mode), mode)
            os.umask(0o077)  # the umask narrows the mode, as it does for any new file
            fsutil.write_atomic(self.path, b"x")
            self.assertEqual(stat.S_IMODE(os.stat(self.path).st_mode), 0o600)
        finally:
            os.umask(umask)

    @unittest.skipUnless(CAN_SYMLINK, "symlinks")
    def test_symlink_at_path_is_replaced_not_followed(self):
        elsewhere = os.path.join(self.tmp.name, "elsewhere")
        os.remove(self.path)
        os.symlink(elsewhere, self.path)
        fsutil.write_atomic(self.path, b"new")
        self.assertFalse(os.path.islink(self.path))
        self.assert_only_file(b"new")

    # Windows has no O_NOFOLLOW, and O_EXCL there creates a dangling symlink's target (as mkstemp would)
    @unittest.skipUnless(CAN_SYMLINK and fsutil.O_NOFOLLOW, "symlinks and O_NOFOLLOW")
    def test_refuses_a_symlink_at_the_temporary_name(self):
        # Someone able to guess the name plants a symlink there: it is neither followed nor removed
        temp = os.path.join(self.tmp.name, fsutil.PARTIAL_PREFIX + "planted")
        target = os.path.join(self.tmp.name, "target")
        os.symlink(target, temp)
        with mock.patch.object(fsutil, "_temp_path", return_value=temp):
            with self.assertRaises(FileExistsError):
                fsutil.write_atomic(self.path, b"new")
        self.assertFalse(os.path.lexists(target))
        self.assertTrue(os.path.islink(temp))
        self.assertEqual(self.contents(), b"old")

    def test_temporary_file_is_in_the_same_directory_and_prefixed(self):
        temps = []
        real_replace = fsutil.replace
        with mock.patch.object(fsutil, "replace", lambda src, dst: (temps.append(src), real_replace(src, dst))):
            fsutil.write_atomic(self.path, b"new")
        self.assertEqual(os.path.dirname(temps[0]), self.tmp.name)
        self.assertTrue(os.path.basename(temps[0]).startswith(fsutil.PARTIAL_PREFIX))
        self.assertNotEqual(fsutil._temp_path(self.tmp.name), fsutil._temp_path(self.tmp.name))

    def test_failure_mid_write_removes_the_temporary_file(self):
        failures = [
            ("fsync", mock.patch("os.fsync", side_effect=OSError(errno.EIO, "I/O error")), OSError),
            ("interrupt", mock.patch("os.fsync", side_effect=KeyboardInterrupt), KeyboardInterrupt),
            ("rename", mock.patch.object(fsutil, "replace", side_effect=PermissionError(13, "in use")),
             PermissionError),
        ]
        for stage, patch, error in failures:
            with self.subTest(stage):
                with patch, self.assertRaises(error):
                    fsutil.write_atomic(self.path, b"new")
                self.assert_only_file(b"old")  # the original is left as it was

    def test_exclusive_keeps_an_existing_file(self):
        with self.assertRaises(FileExistsError):
            fsutil.write_atomic(self.path, b"new", exclusive=True)
        self.assert_only_file(b"old")
        os.remove(self.path)
        fsutil.write_atomic(self.path, b"new", exclusive=True)
        self.assert_only_file(b"new")

    def test_directory_synced_only_when_durable(self):
        with mock.patch.object(fsutil, "sync_directory") as sync:
            fsutil.write_atomic(self.path, b"new")
        sync.assert_called_once_with(self.tmp.name)
        with mock.patch.object(fsutil, "sync_directory") as sync:
            fsutil.write_atomic(self.path, b"newer", durable=False)
        sync.assert_not_called()
        self.assert_only_file(b"newer")

    @unittest.skipIf(fsutil.IS_WINDOWS, "Windows can't sync a directory")
    def test_synced_before_and_after_the_rename(self):
        events = []
        real_fsync, real_replace = os.fsync, fsutil.replace
        with mock.patch("os.fsync", side_effect=lambda fd: (events.append("fsync"), real_fsync(fd))[1]), \
                mock.patch.object(fsutil, "replace", lambda *a: (events.append("rename"), real_replace(*a))[1]):
            fsutil.write_atomic(self.path, b"new")
        self.assertEqual(events, ["fsync", "rename", "fsync"])


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
        for name in (".bash_login", ".npmrc", ".", "..", "a/b", "../x", "a\\b", "", None, 42, b"bytes",
                     "-rf", "--help"):
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
