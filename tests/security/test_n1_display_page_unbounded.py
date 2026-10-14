"""N1 (the H2 + L3 chain): one anonymous view of the display-mu-file page makes the node download and
permanently pin an unbounded DAG, bypassing the page's 200 KB MAX_SIZE check. A file root may truthfully
declare 1 byte while carrying many zero-size children (L3: no reader rejects size-0 file-node children). The
page's size_of() reads that declared 1 byte, so the MAX_SIZE check passes (index.mu:76-80); `pin add` then
fetches the whole DAG with no max_dag_bytes and pins it USER (node.py:821-826), bounded only by the disk
reserve.

The test runs the page's real script against an offline victim that fetches the crafted DAG from an offline
provider, and asserts the node did not store far more than MAX_SIZE. The real-daemon variant is gated behind
RNFS_E2E.
"""
import tempfile
import unittest

import RNS

from rnfs.constants import MAX_BLOCK_SIZE
from rnfs.content.storage import USER
from tests.security import _pages_helpers as helpers

MAX_SIZE = 200 * 1024  # display-mu-file index.mu:18
OVERHEAD = MAX_BLOCK_SIZE  # generous per-view slack: even one extra full block stays well under the bomb

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class UnboundedPinTest(unittest.TestCase):
    """A '1-byte' file root that carries far more than MAX_SIZE of DAG blocks."""

    def setUp(self):
        """A provider serving the bomb DAG and a victim that serves the page."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        _, self.provider, self.victim = helpers.make_nodes(self.tmp.name)
        self.addCleanup(self.provider.stop)
        self.addCleanup(self.victim.stop)
        self.root, blocks = helpers.bomb_dag(fanout=5, children=3000)
        helpers.serve_blocks(self.provider, self.victim, self.root, blocks)

    @unittest.expectedFailure  # N1: fails until a page view can't store more than MAX_SIZE (+ overhead) of DAG
    def test_one_page_view_cannot_store_more_than_max_size(self):
        """The page admits the 1-byte-declared root, then pins a DAG many times larger than MAX_SIZE."""
        declared = self.victim.stat(str(self.root))["size"]
        self.assertEqual(declared, 1)  # what the page's MAX_SIZE check sees

        _, calls = helpers.run_display_page(self.victim, str(self.root))
        self.assertIn(["pin", "add", str(self.root)], calls)  # the MAX_SIZE check did not stop it

        stored = self.victim.storage.used_bytes()
        self.assertLessEqual(
            stored, MAX_SIZE + OVERHEAD,
            f"a single page view of a 1-byte-declared root stored {stored} bytes "
            f"in {self.victim.storage.block_count()} blocks")

    @unittest.expectedFailure  # N1: fails until the node does not permanently pin an understated DAG
    def test_bomb_is_not_permanently_pinned(self):
        """The whole oversized DAG is pinned with source user (permanent), as the page's pin add asks."""
        helpers.run_display_page(self.victim, str(self.root))
        self.assertNotIn(USER, self.victim.storage.pins().get(self.root, set()))


if __name__ == "__main__":
    unittest.main()
