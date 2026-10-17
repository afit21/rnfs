"""FEAT-6: `fsutil.printable()` escapes terminal controls, BiDi overrides and zero-width characters, but lets through
characters that are printable yet show nothing or change their neighbours: the blank Hangul and braille fillers
(U+115F, U+1160, U+3164, U+FFA0, U+2800), combining marks (a stack of them spills over the text around it, and
one at the start joins whatever was printed before) and literal backslashes (so a label can spell out an escape
that printable() would print, and look like a different label). A display name made of fillers looks blank, and
one with a filler inside looks like another name.

printable() stays as it is: it prints paths, and a Windows path is full of backslashes. Labels chosen by someone
else (an LXMF display name) go through the new `fsutil.printable_label()`, which escapes all of these too, and
escapes a label that would print as nothing but spaces.
"""
import unittest

from rnfs import fsutil

FILLERS = ("\u115f", "\u1160", "\u3164", "\uffa0", "\u2800")
ACUTE, GRAVE, TILDE = "\u0301", "\u0300", "\u0303"  # combining marks


class PrintableLabelTest(unittest.TestCase):
    """What printable_label() escapes beyond printable()."""

    def test_printable_lets_the_lookalikes_through(self):
        # The finding: printable() returns these unchanged (it is kept that way for paths)
        for filler in FILLERS:
            self.assertEqual(fsutil.printable(f"al{filler}ice"), f"al{filler}ice")
        self.assertEqual(fsutil.printable(f"{ACUTE}bob"), f"{ACUTE}bob")
        self.assertEqual(fsutil.printable("C:\\Users\\bob"), "C:\\Users\\bob")

    def test_blank_fillers_are_escaped(self):
        # FEAT-6: fails until printable_label() exists and escapes them
        for filler in FILLERS:
            label = fsutil.printable_label(f"al{filler}ice")
            self.assertNotIn(filler, label)
            self.assertEqual(label, f"al\\u{ord(filler):04x}ice")
        self.assertEqual(fsutil.printable_label(FILLERS[2] * 3), "\\u3164" * 3)  # a name that looked blank

    def test_backslashes_are_escaped(self):
        # A label can't spell out an escape: "\x1b" typed out prints differently from an escaped ESC
        self.assertEqual(fsutil.printable_label("a\\x1bb"), "a\\\\x1bb")
        self.assertEqual(fsutil.printable_label("a\x1bb"), "a\\x1bb")
        self.assertNotEqual(fsutil.printable_label("a\\x1bb"), fsutil.printable_label("a\x1bb"))

    def test_leading_and_stacked_combining_marks_are_escaped(self):
        self.assertEqual(fsutil.printable_label(f"{ACUTE}bob"), "\\u0301bob")
        # Up to two in a row stay (Vietnamese and others use two); the third and later are escaped
        self.assertEqual(fsutil.printable_label(f"e{ACUTE}{GRAVE}"), f"e{ACUTE}{GRAVE}")
        self.assertEqual(fsutil.printable_label(f"e{ACUTE}{GRAVE}{TILDE}{ACUTE}x{ACUTE}"),
                         f"e{ACUTE}{GRAVE}\\u0303\\u0301x{ACUTE}")
        # A mark after an escape would join the escape's text: escaped too
        self.assertEqual(fsutil.printable_label(f"a\x07{ACUTE}"), "a\\x07\\u0301")
        # Spacing marks (Mc) and enclosing marks (Me) count as combining marks
        self.assertEqual(fsutil.printable_label("\u20dd"), "\\u20dd")

    def test_blank_labels_are_escaped(self):
        self.assertEqual(fsutil.printable_label("   "), "\\x20\\x20\\x20")
        self.assertEqual(fsutil.printable_label(""), "")
        self.assertEqual(fsutil.printable_label("a b"), "a b")  # spaces between visible characters stay

    def test_everything_printable_escapes_stays_escaped(self):
        for text in ("a\x1b[31mb\x07", "\u202eevil", "zero\u200bwidth", "nul\x00", "line\nbreak"):
            self.assertEqual(fsutil.printable_label(text), fsutil.printable(text))
        self.assertEqual(fsutil.printable_label("plain été 名前"), "plain été 名前")
        self.assertEqual(fsutil.printable_label(5), "5")


if __name__ == "__main__":
    unittest.main()
