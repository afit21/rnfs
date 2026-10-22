"""L11: the display-mu-file page printed whatever the file held as Micron, so content someone else wrote
(phishing forms, links, pages imitating the operator's own) appeared under the node's address.

The fixed page renders Micron only for CIDs in the operator's allowlist file. Anything else it shows (the
operator's own complete pins) is printed as plain text: a banner saying it is third-party content, then one
literal block (`=) holding the text, in which no line is markup. The tests run the page's real script against
an offline node holding a file full of Micron.
"""
import io
import os
import tempfile
import unittest

import RNS

from tests.security import _pages_helpers as helpers

_saved_loglevel = RNS.loglevel

# A page someone else wrote: a fake heading, a link, a password field, colour headers, an early end of the
# literal block (plainly, and hidden behind a combining mark NomadNet drops), controls and a bidi override.
HOSTILE = "\n".join([
    ">Official downloads",
    "`[Click to verify your identity`:/page/phish.mu]",
    "Password: `<!|password`>",
    "#!bg=fff",
    "#!fg=fff",
    "`=",
    ">After an early end of the block",
    "`\u0301=",
    "`{f64a846313b874ee4a357040807f8c77:/page/partial.mu",
    "\x1b[2Jcleared\tscreen",
    "\u202eevil",
    "-",
    "# a comment hiding text",
]) + "\n"


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class PlainTextTest(unittest.TestCase):
    """A file full of Micron, pinned by the operator, shown through the page."""

    def setUp(self):
        """An offline node that has the hostile file pinned (source user), and the page's data dir."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        _, self.node, _ = helpers.make_nodes(self.tmp.name, test=self)
        self.addCleanup(self.node.stop)
        self.cid = self.node.add_stream(io.BytesIO(HOSTILE.encode("utf-8")))["cid"]
        self.data_dir = os.path.join(self.tmp.name, "page-data")
        os.makedirs(self.data_dir)

    def view(self):
        """The page's output lines for the hostile file."""
        output, _ = helpers.run_display_page(self.node, self.cid, data_dir=self.data_dir)
        return output.splitlines()

    def test_content_is_one_literal_block_under_a_banner(self):
        """The banner comes first; then exactly one literal block, opened and closed by the page itself,
        holds every line of the file, so the file's own "`=" lines can't end it early."""
        lines = self.view()
        self.assertEqual(lines[0], "#!c=0")
        start = lines.index("`=")
        self.assertIn("Third-party content", "\n".join(lines[:start]))
        self.assertEqual(lines[-1], "`=")
        self.assertEqual([i for i, line in enumerate(lines) if line == "`="], [start, len(lines) - 1])
        self.assertEqual(len(lines) - start - 2, len(HOSTILE.splitlines()))  # one block line per file line
        self.assertIn(">Official downloads", lines[start + 1:])  # shown, but inside the block

    def test_content_cannot_restyle_the_page(self):
        """NomadNet takes "#!bg=" / "#!fg=" from anywhere in a page; the file's can't reach it, so it can't
        hide the banner by painting the page's text in its background colour."""
        page = "\n".join(self.view())
        self.assertNotIn("#!bg=", page)
        self.assertNotIn("#!fg=", page)

    def test_content_is_printable(self):
        """Control characters (a terminal escape) and bidi overrides don't reach the visitor's terminal. The
        only non-printable character is the page's own word joiner splitting "#!" (NomadNet drops it before
        parsing, after its search for "#!bg=")."""
        for line in self.view():
            self.assertTrue(line.replace("#\u2060!", "#!").isprintable(), repr(line))

    def test_allowlisted_content_is_micron(self):
        """The same file, once the operator allowlists it, is rendered as written, without the banner."""
        helpers.allow(self.data_dir, self.cid)
        output, _ = helpers.run_display_page(self.node, self.cid, data_dir=self.data_dir)
        self.assertEqual(output, HOSTILE)
        self.assertNotIn("Third-party content", output)


if __name__ == "__main__":
    unittest.main()
