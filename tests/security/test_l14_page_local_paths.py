"""L14: the pages show daemon errors and CLI stderr to remote visitors, which can carry local filesystem
paths (for example "no API token at <rnfs_dir>/...: is the daemon running"). The display-mu-file page echoes
the CLI's stderr verbatim on every error path (index.mu:54, 66, 86, 90, 94).

This runs the page's real script with a CLI that fails exactly as rnfs does when the daemon is down, and
checks the visitor is not shown the local path the error carries.
"""
import os
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

    @unittest.expectedFailure  # L14: fails until the page shows a generic message and logs the detail locally
    def test_display_page_hides_cli_stderr_paths(self):
        """A failing `rnfs stat` must not leak its local path to the visitor."""
        def run(argv, *args, **kwargs):
            """Fail as the CLI does when the daemon is down."""
            return helpers._Completed(1, stderr=DAEMON_DOWN)

        output = helpers.exec_display_page(run, CID)
        self.assertNotIn(RNFS_DIR, output)


if __name__ == "__main__":
    unittest.main()
