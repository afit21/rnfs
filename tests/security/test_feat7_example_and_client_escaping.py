"""FEAT-7: examples/rnfs_share.py printed names and errors unescaped, and rnfs.Client logged the daemon's messages
unescaped.

The example prints entry names from a DAG (whoever made it chose them), warnings and errors that quote them, so a
name holding terminal controls (ESC sequences, BEL) drove the terminal it ran in. rnfs.Client's _report passed a
daemon message, which can quote such a name, to Python's logging as it was.

The example's share() runs against a stand-in node whose add, ls, open, names and jobs answer with such names;
Client._report is called directly. Fixed: both go through rnfs.fsutil.printable.
"""
import contextlib
import importlib.util
import io
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from rnfs import remote

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EVIL = "evil\x1b]0;owned\x07\x1b[2J"  # sets the terminal's title and clears the screen


def load_example():
    """examples/rnfs_share.py as a module."""
    spec = importlib.util.spec_from_file_location("rnfs_share", os.path.join(ROOT, "examples", "rnfs_share.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class StandInNode:
    """Just what share() calls, answering with EVIL in every name, warning and error it can."""

    def __init__(self):
        """Names and jobs (a CID is base32 text, so it stays plain)."""
        self.cid = "bafyexample"
        record = SimpleNamespace(name=b"\x01" * 16, cid=self.cid, sequence=1)
        name = SimpleNamespace(alias="example", hex="01" * 16, publish=lambda cid: record)

        @contextlib.contextmanager
        def watch(_name, on_record):
            """Deliver the published record at once."""
            on_record(record)
            yield

        self.names = SimpleNamespace(list=lambda: [name], create=lambda alias: name, watch=watch)
        job = SimpleNamespace(state="done", done_bytes=1, total_bytes=1, error=None)
        job.wait = lambda: job

        def get(_target, _output, on_progress):
            """A job that is done at once."""
            on_progress(job)
            return job

        self.jobs = SimpleNamespace(get=get)

    def add(self, _path):
        """An added directory with a warning quoting a name."""
        return SimpleNamespace(cid=self.cid, size=1, warnings=[f"skipped {EVIL}"])

    def ls(self, _cid):
        """One file named EVIL."""
        return [SimpleNamespace(kind="file", size=1, name=EVIL, is_dir=False)]

    def open(self, _target):
        """A one-byte file."""
        return contextlib.nullcontext(SimpleNamespace(size=1, seek=lambda offset: None, read=lambda n: b"x"))


class ExampleTest(unittest.TestCase):
    """examples/rnfs_share.py escapes what it prints."""

    def test_share_escapes_names(self):
        """Entry names, warnings and targets are printed escaped."""
        example = load_example()
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as folder, contextlib.redirect_stdout(out):
            example.share(StandInNode(), folder)
        printed = out.getvalue()
        self.assertIn(EVIL.encode("unicode_escape").decode("ascii"), printed)
        self.assertNotIn("\x1b", printed)
        self.assertNotIn("\x07", printed)

    def test_errors_are_escaped(self):
        """An rnfs error (whose text can quote a name) is printed escaped when the example exits on it."""
        import rnfs
        example = load_example()

        def fail(*_args, **_kwargs):
            """Fail as a daemon reporting a name would."""
            raise rnfs.RnfsError(f"not found: {EVIL}")
        with mock.patch.object(rnfs, "Client", fail), mock.patch("sys.argv", ["rnfs_share", "--daemon", "x"]):
            with self.assertRaises(SystemExit) as raised:
                example.main()
        self.assertIn("not found: evil\\x1b", str(raised.exception.code))
        self.assertNotIn("\x1b", str(raised.exception.code))


class ClientReportTest(unittest.TestCase):
    """rnfs.Client logs the daemon's messages escaped."""

    def test_report_escapes(self):
        """A message quoting a name reaches logging with its controls escaped."""
        with self.assertLogs("rnfs", "ERROR") as logged:
            remote.Client._report(None, f"couldn't write {EVIL}")
        self.assertEqual(len(logged.output), 1)
        self.assertNotIn("\x1b", logged.output[0])
        self.assertIn("couldn't write evil\\x1b]0;owned\\x07", logged.output[0])


if __name__ == "__main__":
    unittest.main()
