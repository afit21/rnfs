"""M6 follow-up: the fixed file-index-node page never rendered its Files section.

The M6 fix added a function shown(pin), which entry() calls, but the page body later bound a module-level
variable of the same name to section()'s result. By the time the Files section ran, shown was a bool, so
entry() raised TypeError ("'bool' object is not callable") and the page printed no file at all. The other page
tests exec only the page's definitions (index_page_namespace), so none of them ran the body.

These tests run the whole page script over an offline node's real named pins (the CLI served in-process by
fake_rnfs) and check what it prints.
"""
import os
import tempfile
import unittest

import RNS

from tests.security import _pages_helpers as helpers

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class IndexPageFullRunTest(unittest.TestCase):
    """The whole file-index-node script, run against one offline node's pins."""

    def setUp(self):
        """One offline node and a page data dir."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        _, self.node, _ = helpers.make_nodes(self.tmp.name, test=self)
        self.addCleanup(self.node.stop)
        self.data_dir = os.path.join(self.tmp.name, "page-data")

    def add_file(self, name, content=b"shared data"):
        """Add a file named name (a user pin); its CID."""
        path = os.path.join(self.tmp.name, name)
        with open(path, "wb") as f:
            f.write(content)
        return self.node.add_path(path)["cid"]

    def add_folder(self, name):
        """Add a folder named name holding one file (a user pin); its CID."""
        folder = os.path.join(self.tmp.name, name)
        os.makedirs(folder)
        with open(os.path.join(folder, "inside.txt"), "wb") as f:
            f.write(b"inside")
        return self.node.add_path(folder, wrap=True)["cid"]

    def page(self) -> str:
        """What the whole page script prints for this node's pins."""
        try:
            return helpers.exec_index_page(helpers.fake_rnfs(self.node, []), self.data_dir)
        except TypeError as error:
            # fails until the page body stops rebinding shown(), which entry() calls
            self.fail(f"the page crashed: {type(error).__name__}: {error}")

    def test_files_section_lists_named_files(self):
        """With only file pins, the Files section lists each by name and CID."""
        guide = self.add_file("field-guide.pdf")
        notes = self.add_file("notes.txt", b"notes")
        page = self.page()
        self.assertIn(">Files", page)
        for name, cid in (("field-guide.pdf", guide), ("notes.txt", notes)):
            self.assertIn(f"`!{name}`!", page)
            self.assertIn(cid, page)
        self.assertNotIn("Nothing is shared yet.", page)

    def test_folders_and_files_are_both_listed(self):
        """With a folder pin as well, both sections render, Folders first."""
        self.add_folder("maps")
        self.add_file("field-guide.pdf")
        page = self.page()
        self.assertLess(page.index(">Folders"), page.index("`!maps`!"))
        self.assertLess(page.index("`!maps`!"), page.index(">Files"))
        self.assertLess(page.index(">Files"), page.index("`!field-guide.pdf`!"))

    def test_nothing_shared(self):
        """With no pins, the page says nothing is shared."""
        page = self.page()
        self.assertIn("Nothing is shared yet.", page)
        self.assertNotIn(">Files", page)


if __name__ == "__main__":
    unittest.main()
