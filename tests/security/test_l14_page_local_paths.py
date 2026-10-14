"""L14: the pages show daemon errors and CLI stderr to remote visitors, which can carry local filesystem
paths (for example "no API token at <rnfs_dir>/...: is the daemon running"). The display-mu-file page echoed
the CLI's stderr verbatim on every error path (index.mu:54, 66, 86, 90, 94).

This runs the page's real script with a CLI that fails exactly as rnfs does when the daemon is down, and
checks the visitor is not shown the local path the error carries. The fixed pages show a generic message and
write the detail to page.log in their data dir; the tests check that too.
"""
import os
import tempfile
import unittest

import RNS

from tests.security import _pages_helpers as helpers

_saved_loglevel = RNS.loglevel
# A stderr line the real CLI prints when it can't reach the daemon (cli.py:84-88), carrying a local path.
RNFS_DIR = os.path.join(os.path.expanduser("~"), ".config", "rnfs")
DAEMON_DOWN = f"rnfs: no API token at {RNFS_DIR}/apitoken: is the daemon running?"
CID = "bafkreiezrecqkty6ertxgtpi4mt6ns733iqnb2bfqwbiv6bk4b3lky4n4e"


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class LocalPathLeakTest(unittest.TestCase):
    """The display page run against a CLI whose stderr carries a local path."""

    def setUp(self):
        """A data dir for the page (its log goes there)."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.data_dir = tmp.name

    def log(self) -> str:
        """The page's log."""
        with open(os.path.join(self.data_dir, "page.log"), encoding="utf-8") as f:
            return f.read()

    def test_display_page_hides_cli_stderr_paths(self):
        """A failing rnfs command (`rnfs pin ls`, the page's first) must not leak its local path to the visitor."""
        def run(argv, *args, **kwargs):
            """Fail as the CLI does when the daemon is down."""
            return helpers._Completed(1, stderr=DAEMON_DOWN)

        output = helpers.exec_display_page(run, CID, self.data_dir)
        self.assertNotIn(RNFS_DIR, output)
        self.assertIn("can't be shown right now", output)
        self.assertIn(DAEMON_DOWN, self.log())  # the operator still gets the detail

    def test_display_page_hides_exceptions(self):
        """An rnfs that isn't installed (the OSError names its path) gives the same generic message."""
        missing = os.path.join(os.path.expanduser("~"), ".local", "bin", "rnfs")

        def run(argv, *args, **kwargs):
            """Fail as subprocess.run does for a missing executable."""
            raise FileNotFoundError(2, "No such file or directory", missing)

        output = helpers.exec_display_page(run, CID, self.data_dir)
        self.assertNotIn(missing, output)
        self.assertIn("can't be shown right now", output)
        self.assertIn(missing, self.log())

    def test_log_is_kept_small(self):
        """Past LOG_MAX bytes the log moves to page.log.1, so visitors causing errors can't fill the disk."""
        log_max = helpers.display_page_namespace(self.data_dir)["LOG_MAX"]
        with open(os.path.join(self.data_dir, "page.log"), "w", encoding="utf-8") as f:
            f.write("x" * (log_max + 1))

        def run(argv, *args, **kwargs):
            """Fail as the CLI does when the daemon is down."""
            return helpers._Completed(1, stderr=DAEMON_DOWN)

        helpers.exec_display_page(run, CID, self.data_dir)
        self.assertEqual(os.path.getsize(os.path.join(self.data_dir, "page.log.1")), log_max + 1)
        self.assertLess(len(self.log()), 1000)
        self.assertIn(DAEMON_DOWN, self.log())


if __name__ == "__main__":
    unittest.main()
