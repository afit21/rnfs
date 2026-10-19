"""Tests for the 32-hex-digit ID parser (rnfs/ids.py) and the parsers built on it, which must agree."""
import unittest

from rnfs import ids
from rnfs.api import parse_hex_id
from rnfs.features.contacts import parse_hex
from rnfs.names import parse_name
from rnfs.network.identity import parse_node_id

VALUE = bytes(range(16))
HEX = VALUE.hex()
# Text holding 32 hex digits that isn't one ID: bytes.fromhex alone would skip the whitespace between them
INNER_WHITESPACE = ["0011 2233 4455 6677 8899 aabb ccdd eeff", HEX[:14] + " " + HEX[14:], HEX[:2] + "\t" + HEX[2:],
                    f"< {HEX}>", f"<{HEX[:10]}\n{HEX[10:]}>"]
NEVER = ["", HEX[:-1], HEX + "0", "zz" * 16, "0x" + HEX[2:], f"{HEX}{HEX}", "٠" * 32, None, 5, VALUE]


class ParseHex16Test(unittest.TestCase):

    def test_plain_forms(self):
        for text in (HEX, HEX.upper(), f"  {HEX}\n", f"\t{HEX.upper()} "):
            self.assertEqual(ids.parse_hex16(text, "ID"), VALUE)
            self.assertTrue(ids.is_hex16(text))

    def test_brackets_only_when_allowed(self):
        for text in (f"<{HEX}>", f" <{HEX.upper()}> ", f"<{HEX}", f"{HEX}>"):
            self.assertEqual(ids.parse_hex16(text, "ID", brackets=True), VALUE)
            with self.assertRaises(ValueError):
                ids.parse_hex16(text, "ID")

    def test_refused(self):
        for text in INNER_WHITESPACE + NEVER:
            for brackets in (False, True):
                self.assertFalse(ids.is_hex16(text, brackets=brackets), text)
                with self.assertRaises(ValueError, msg=text):
                    ids.parse_hex16(text, "ID", brackets=brackets)

    def test_lowercase_only(self):
        self.assertEqual(ids.parse_hex16(f" {HEX} ", "ID", upper=False), VALUE)
        with self.assertRaises(ValueError):
            ids.parse_hex16(HEX.upper(), "ID", upper=False)
        self.assertFalse(ids.is_hex16("A" + HEX[1:], upper=False))

    def test_error_names_what(self):
        with self.assertRaisesRegex(ValueError, r"invalid LXMF address 'x': must be 32 hex characters"):
            ids.parse_hex16("x", "LXMF address")

    def test_normal_form(self):
        self.assertEqual(ids.normal_hex16(f" <{HEX.upper()}>\n"), HEX)

    def test_patterns_are_unstripped(self):
        """Persisted rows are checked against the patterns as they are: what loaded before must load, nothing more."""
        self.assertTrue(ids.HEX16.fullmatch(HEX.upper()))
        self.assertIsNone(ids.HEX16_LOWER.fullmatch(HEX.upper()))
        for pattern in (ids.HEX16, ids.HEX16_LOWER):
            self.assertIsNone(pattern.fullmatch(f" {HEX}"))
            self.assertIsNone(pattern.fullmatch(f"<{HEX}>"))


class CallersAgreeTest(unittest.TestCase):
    """Every 32-hex parser takes the same text, differing only in whether <angle brackets> are allowed."""

    BRACKETED = (parse_node_id, parse_name, parse_hex)

    def test_bracketed_parsers(self):
        for parse in self.BRACKETED:
            for text in (HEX, HEX.upper(), f" {HEX}\n", f"<{HEX}>", f" <{HEX.upper()}> "):
                self.assertEqual(parse(text), VALUE, (parse.__name__, text))
            for text in INNER_WHITESPACE + NEVER:
                with self.assertRaises(ValueError, msg=(parse.__name__, text)):
                    parse(text)

    def test_api_parser_refuses_brackets(self):
        self.assertEqual(parse_hex_id(f" {HEX.upper()} "), VALUE)
        for text in [f"<{HEX}>"] + INNER_WHITESPACE + NEVER:
            with self.assertRaises(ValueError, msg=text):
                parse_hex_id(text)

    def test_node_id_inner_whitespace_narrowed(self):
        """parse_node_id (bootstrap_nodes) used bytes.fromhex, which took whitespace between byte pairs; now the
        same 32 digits as every other ID. Such a bootstrap node is skipped with a warning, as malformed ones were."""
        with self.assertRaisesRegex(ValueError, "invalid node ID"):
            parse_node_id("0011 2233 4455 6677 8899 aabb ccdd eeff")


if __name__ == "__main__":
    unittest.main()
