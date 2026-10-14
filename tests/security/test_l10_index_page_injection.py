"""L10: Micron line injection through DAG entry names on the file-index-node page. The page's escape() only
escapes backslash and backtick (index.mu:28-30), but a directory entry name may contain newlines and other
control characters (dag.check_name allows them). A name with a newline therefore injects whole Micron lines
into the page: a fake ">" heading, or a fake entry line.

A directory whose single entry name carries such a payload is built directly (decode accepts the name) and
pinned, then rendered through the page's own entry(). The attacker needs a trusted route to get the directory
pinned (an auto-fetch sender, a follow, or a remote pin); the injection itself is what is shown here.
"""
import tempfile
import unittest

import RNS

from rnfs.content import dag
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.storage import USER
from tests.security import _pages_helpers as helpers

INJECTED_HEADING = ">Official downloads"


_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class LineInjectionTest(unittest.TestCase):
    """A directory entry name that tries to add Micron lines to the page."""

    def setUp(self):
        """One offline node holding the crafted directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        _, self.node, _ = helpers.make_nodes(self.tmp.name)
        self.addCleanup(self.node.stop)

    def pin_crafted_directory(self, entry_name: str) -> str:
        """Store and user-pin a one-entry directory whose entry is named entry_name; return its CID."""
        leaf = b"payload"
        leaf_cid = Cid.for_block(RAW, leaf)
        block = dag.encode(dag.DirectoryNode([dag.Entry(entry_name, leaf_cid, len(leaf), dag.KIND_FILE)]))
        cid = Cid.for_block(DAG, block)
        with self.node.storage.protect() as session:
            session.put(leaf_cid, leaf)
            session.put(cid, block)
        self.node.storage.pin(cid, USER)
        return str(cid)

    @unittest.expectedFailure  # L10: fails until escape() also strips newlines / control characters
    def test_entry_name_cannot_add_lines(self):
        """An entry name must render as a single page line, never inject a heading or a fake entry."""
        self.pin_crafted_directory(f"readme.txt\n{INJECTED_HEADING}\n  a second line")
        rendered = [line for line in helpers.render_index(self.node) if line]
        printed_lines = "\n".join(rendered).splitlines()
        # The page must not have produced the attacker's heading as a line of its own
        self.assertNotIn(INJECTED_HEADING, printed_lines)


if __name__ == "__main__":
    unittest.main()
