"""M6: the file-index-node page publishes sealed files' plaintext names and pins that didn't come from the
operator. Spec section 5 promises a sealed file's name stays confidential, yet a sealed add records its
plaintext path/seal_name (node.py:650, 669) and the page prints the basename to every visitor; a directory
pinned from a contact (remote:/lxmf:/follow:) always gets a name from _pin_shape, so it is shown too.

The page's own entry() is run over the real pin_ls() of an offline node (JSON round-tripped, as the page
does). The recheck notes SHOW_UNNAMED=False hides unnamed file pins, so the assertions are precise: they
target sealed-file names and a named directory pin from a non-operator source.
"""
import io
import os
import tempfile
import unittest

import RNS

from rnfs.content.cid import Cid
from tests.security import _pages_helpers as helpers


_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class SealedNamesTest(unittest.TestCase):
    """Sealed adds and contact-sourced directory pins, rendered through the page's entry()."""

    def setUp(self):
        """One offline node with an identity (so it can seal to itself)."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        _, self.node, _ = helpers.make_nodes(self.tmp.name)
        self.addCleanup(self.node.stop)
        helpers.give_identity(self.node)

    def page(self) -> str:
        """The page body the file-index-node script would print for this node's pins."""
        return "\n".join(line for line in helpers.render_index(self.node) if line)

    @unittest.expectedFailure  # M6: fails until the page omits (or redacts) the names of sealed files
    def test_sealed_file_names_are_not_published(self):
        """A sealed file's plaintext name must never reach the page."""
        path = os.path.join(self.tmp.name, "Q3-layoffs.xlsx")
        with open(path, "wb") as f:
            f.write(b"secret data" * 10)
        self.node.add_path(path, to_self=True)
        self.node.add_stream(io.BytesIO(b"minutes"), to_self=True, seal_name="board-minutes.txt")
        page = self.page()
        self.assertNotIn("Q3-layoffs.xlsx", page)
        self.assertNotIn("board-minutes.txt", page)

    @unittest.expectedFailure  # M6: fails until the page lists only operator (user/folder) pins
    def test_pins_from_contacts_are_not_published(self):
        """A directory pinned from a contact (a remote: source), not by the operator, must not be listed."""
        folder = os.path.join(self.tmp.name, "from-a-contact")
        os.makedirs(folder)
        with open(os.path.join(folder, "offer-letters"), "wb") as f:
            f.write(b"x")
        cid = self.node.add_path(folder, pin=False)["cid"]  # cached, not a user pin, no recorded name
        self.node.storage.pin(Cid.parse(cid), "remote:" + "ab" * 16)  # a contact asked us to pin it
        self.assertNotIn("offer-letters", self.page())


if __name__ == "__main__":
    unittest.main()
