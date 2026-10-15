"""M6: the file-index-node page publishes sealed files' plaintext names and pins that didn't come from the
operator. Spec section 5 promises a sealed file's name stays confidential, yet a sealed add records its
plaintext path/seal_name (node.py:650, 669) and the page prints the basename to every visitor; a directory
pinned from a contact (remote:/lxmf:/follow:) always gets a name from _pin_shape, so it is shown too.

The page's own entry() is run over the real pin_ls() of an offline node (JSON round-tripped, as the page
does). The recheck notes SHOW_UNNAMED=False hides unnamed file pins, so the assertions are precise: they
target sealed-file names and a named directory pin from a non-operator source. The fixed page lists only
user/folder pins and skips sealed files, which it tells by their first bytes (`rnfs cat --raw`, a local read
of a complete pin), remembering the answer per CID in its data dir.
"""
import io
import json
import os
import subprocess
import tempfile
import unittest
from unittest import mock

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

    def test_pins_from_contacts_are_not_published(self):
        """A directory pinned from a contact (a remote: source), not by the operator, must not be listed."""
        folder = os.path.join(self.tmp.name, "from-a-contact")
        os.makedirs(folder)
        with open(os.path.join(folder, "offer-letters"), "wb") as f:
            f.write(b"x")
        cid = self.node.add_path(folder, pin=False)["cid"]  # cached, not a user pin, no recorded name
        self.node.storage.pin(Cid.parse(cid), "remote:" + "ab" * 16)  # a contact asked us to pin it
        self.assertNotIn("offer-letters", self.page())

    def add_file(self, name, content=b"shared data", **kwargs):
        """Add a file named name (user pin); its CID."""
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as f:
            f.write(content)
        return self.node.add_path(path, **kwargs)["cid"]

    def test_operator_files_are_still_listed(self):
        """Plain user pins, and a pin the operator also holds for a contact, are listed by name."""
        self.add_file("field-guide.pdf")
        folder = os.path.join(self.tmp.name, "maps")
        os.makedirs(folder)
        with open(os.path.join(folder, "north.png"), "wb") as f:
            f.write(b"png")
        cid = self.node.add_path(folder, wrap=True)["cid"]
        self.node.storage.pin(Cid.parse(cid), "remote:" + "cd" * 16)  # a contact asked for it too
        page = self.page()
        self.assertIn("field-guide.pdf", page)
        self.assertIn("maps", page)

    def test_sealed_check_is_remembered(self):
        """Each file pin is checked once (one raw cat of its first bytes); the answer is kept in the data dir."""
        self.add_file("field-guide.pdf")
        self.add_file("Q3-layoffs.xlsx", b"secret", to_self=True)
        data_dir = os.path.join(self.tmp.name, "page-data")
        calls = []
        helpers.render_index(self.node, calls, data_dir)
        self.assertEqual(sorted(call[0] for call in calls), ["cat", "cat"])
        self.assertTrue(all(call[2:] == ["--raw", "--length", "12"] for call in calls))
        with open(os.path.join(data_dir, "sealed.json"), encoding="utf-8") as f:
            self.assertEqual(sorted(json.load(f).values()), [False, True])
        calls.clear()
        page = "\n".join(line for line in helpers.render_index(self.node, calls, data_dir) if line)
        self.assertEqual(calls, [])
        self.assertIn("field-guide.pdf", page)
        self.assertNotIn("Q3-layoffs.xlsx", page)

    def test_a_file_that_cannot_be_checked_is_left_off(self):
        """If the sealed check fails (here the CLI errors), the file is left off and the reason logged."""
        self.add_file("field-guide.pdf")
        data_dir = os.path.join(self.tmp.name, "page-data")
        pin = json.loads(json.dumps(self.node.pin_ls()))[0]

        def run(argv, *args, **kwargs):
            """Fail as the CLI does when the daemon is down."""
            return helpers._Completed(1, stderr="rnfs: can't reach the daemon")

        with helpers.page_dir(data_dir), mock.patch.object(subprocess, "run", run):
            namespace = helpers.index_page_namespace()
            self.assertIsNone(namespace["entry"](pin))
        with open(os.path.join(data_dir, "page.log"), encoding="utf-8") as f:
            self.assertIn("can't reach the daemon", f.read())
        self.assertFalse(os.path.exists(os.path.join(data_dir, "sealed.json")))  # asked again next time


if __name__ == "__main__":
    unittest.main()
