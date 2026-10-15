"""T1: names.parse_name accepted whitespace between the hex digits, returning a name shorter than 16 bytes.

parse_name checked that the text is 32 characters long, then decoded it with bytes.fromhex, which skips
whitespace between byte pairs, so "00 00 ... 00" (32 characters, 11 bytes) passed as a name. Found by
tests/security/test_fuzz_parsers.py. parse_name now requires exactly 32 hex digits (config's _HEX_ID); the test
gives it such text and asserts a ValueError, as for any other text that isn't 32 hex characters.
"""
import unittest

from rnfs.names import parse_name


class ParseNameWhitespaceTest(unittest.TestCase):
    """parse_name must reject anything but 32 hex digits (with optional <angle brackets>)."""

    def test_inner_whitespace_is_rejected(self):
        texts = ["00 " * 10 + "00", "00112233445566 77 8899aabbccddee", "<00112233445566\t77\n8899aabbccddee>"]
        self.assertTrue(all(len(text.strip("<>")) == 32 for text in texts))
        accepted = {text: self.parsed(text).hex() for text in texts if not isinstance(self.parsed(text), ValueError)}
        self.assertEqual(accepted, {}, "parse_name accepted text that isn't 32 hex digits")

    def test_canonical_forms_still_parse(self):
        name = bytes(range(16))
        for text in (name.hex(), name.hex().upper(), f"<{name.hex()}>", f"  {name.hex()}\n"):
            self.assertEqual(parse_name(text), name)

    @staticmethod
    def parsed(text):
        """What parse_name returns for text, for the failure message."""
        try:
            return parse_name(text)
        except ValueError as e:
            return e


if __name__ == "__main__":
    unittest.main()
