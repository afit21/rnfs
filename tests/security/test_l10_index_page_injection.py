"""L10: Micron line injection through DAG entry names on the file-index-node page. The page's escape() only
escapes backslash and backtick (index.mu:28-30), but a directory entry name may contain newlines and other
control characters (dag.check_name allows them). A name with a newline therefore injects whole Micron lines
into the page: a fake ">" heading, or a fake entry line.

A directory whose single entry name carries such a payload is built directly (decode accepts the name) and
pinned, then rendered through the page's own entry(). The attacker needs a trusted route to get the directory
pinned (an auto-fetch sender, a follow, or a remote pin); the injection itself is what is shown here.

The fixed escape() (the same in both pages) makes every value one printable line with nothing Micron reads as
markup, even at the start of a line. The checks below follow NomadNet's parser (MicronParser.parse_line,
Browser's "#!bg="/"#!fg=" search and util.strip_modifiers); the archive's micron_check.py ran the same
payloads through NomadNet's own parser.
"""
import re
import tempfile
import unicodedata
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
        _, self.node, _ = helpers.make_nodes(self.tmp.name, test=self)
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

    def test_entry_name_cannot_add_lines(self):
        """An entry name must render as a single page line, never inject a heading or a fake entry."""
        self.pin_crafted_directory(f"readme.txt\n{INJECTED_HEADING}\n  a second line")
        rendered = [line for line in helpers.render_index(self.node) if line]
        printed_lines = "\n".join(rendered).splitlines()
        # The page must not have produced the attacker's heading as a line of its own
        self.assertNotIn(INJECTED_HEADING, printed_lines)


# Names and descriptions that try every kind of Micron markup: line-start commands, inline formatting, links,
# fields, page headers, literal blocks, partials and tables, control and bidi characters, and characters
# NomadNet drops before parsing (so they could hide markup behind them).
PAYLOADS = [">Official downloads", ">>sub", "<reset", "#hidden comment", "-", "-=", "`=", "`tl80", "\\`t",
            "`{f64a846313b874ee4a357040807f8c77:/page/x.mu", "x`!bold`!", "`[link`:/page/x.mu]", "`<field`value>",
            "#!bg=f00", "a #!fg=000", "\\", "end\\", "a\nb", "a\r\n>b", "\x1b[31mred", "\u202eevil",
            "line\u2028sep", "tab\there", "\u0301>combining", "\u0301`tl", "`\u0301=", "\U0001f3fb-", "\x00",
            "e\u0301t\u00e9"]


def unescaped(text: str) -> str:
    """text with Micron's escapes (a backslash and the character after it) removed."""
    return re.sub(r"\\.", "", text, flags=re.DOTALL)


def as_parsed(text: str) -> str:
    """text as NomadNet parses it: without the combining marks, emoji modifiers and format characters that
    util.strip_modifiers drops first."""
    return "".join(char for char in text if not unicodedata.category(char).startswith(("M", "Cf"))
                   and not "\U0001f3fb" <= char <= "\U0001f3ff")


class EscapeTest(unittest.TestCase):
    """Both pages' escape() over every payload."""

    def escapes(self):
        """(page, escape) for the two pages."""
        index = helpers.index_page_namespace()["escape"]
        with tempfile.TemporaryDirectory() as data_dir:
            display = helpers.display_page_namespace(data_dir)["escape"]
        return [("file-index-node", index), ("display-mu-file", display)]

    def test_escaped_values_are_one_printable_line(self):
        """No newline, control, bidi or other non-printable character survives."""
        for page, escape in self.escapes():
            for payload in PAYLOADS:
                with self.subTest(page=page, payload=payload):
                    text = escape(payload)
                    self.assertTrue(text.isprintable(), repr(text))
                    self.assertEqual(len(text.splitlines()), 1, repr(text))

    def test_escaped_values_have_no_markup_even_at_line_start(self):
        """Every backtick is escaped, "#!" can't form a header, and no line-start command (>, <, #, -, a table,
        a partial or a literal block) can begin the value, as NomadNet will parse it."""
        for page, escape in self.escapes():
            for payload in PAYLOADS:
                with self.subTest(page=page, payload=payload):
                    text = escape(payload)
                    self.assertNotIn("`", unescaped(text), repr(text))
                    self.assertNotIn("#!", text)
                    parsed = as_parsed(text)
                    self.assertFalse(parsed[:1] in (">", "<", "#", "-", "`"), repr(parsed))
                    self.assertFalse(parsed.startswith("\\`"), repr(parsed))  # read as `t / `{ at line start

    def test_ordinary_names_are_unchanged(self):
        """Plain names keep their text (an accent composed, not dropped)."""
        for page, escape in self.escapes():
            with self.subTest(page=page):
                self.assertEqual(escape("Alpine Linux 3.24.2 ISOs (x86_64).iso"),
                                 "Alpine Linux 3.24.2 ISOs (x86_64).iso")
                self.assertEqual(escape("cafe\u0301 menu"), "caf\u00e9 menu")

    def test_whole_entry_stays_on_its_lines(self):
        """A directory pin named with every payload at once (no path separators, so display_name keeps it
        whole) still renders as exactly the page's own two lines: the bold name, then the CID."""
        index = helpers.index_page_namespace()
        pin = {"cid": "bafkreiailvfecuuvt6tctx3owax2jic7y5af2ndriwo6xymcq2uwyut34a", "sources": ["user"],
               "complete": True, "type": "directory", "name": "\n".join(PAYLOADS).replace("/", "")}
        lines = index["entry"](pin).split("\n")
        self.assertEqual(len(lines), 2)
        self.assertTrue(lines[0].startswith("`!") and lines[0].endswith("`!"))
        self.assertNotIn("`", unescaped(lines[0][2:-2]))


if __name__ == "__main__":
    unittest.main()
