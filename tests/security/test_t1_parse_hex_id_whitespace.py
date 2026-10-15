"""T1 residual: api.parse_hex_id (node IDs in /api/info, /api/ping, /api/get, /api/pin/remote, jobs, and names in
gateway URLs) decoded with bytes.fromhex, which skips whitespace between byte pairs, so text that isn't 32 hex
digits passed as long as it held 32 of them. names.parse_name was made strict for the same reason.

The test gives parse_hex_id such texts and asserts a ValueError, and checks the plain forms still parse.
"""
import unittest

from rnfs.api import parse_hex_id


class ParseHexIdWhitespaceTest(unittest.TestCase):
    """parse_hex_id must take exactly 32 hex digits (surrounding whitespace aside)."""

    def test_inner_whitespace_is_rejected(self):
        texts = ["0011 2233 4455 6677 8899 aabb ccdd eeff", "00112233445566 77 8899aabbccddeeff",
                 "00112233445566\t77\n8899aabbccddeeff"]
        accepted = {}
        for text in texts:
            try:
                accepted[text] = parse_hex_id(text).hex()
            except ValueError:
                pass
        self.assertEqual(accepted, {}, "parse_hex_id accepted text that isn't 32 hex digits")

    def test_plain_forms_still_parse(self):
        value = bytes(range(16))
        for text in (value.hex(), value.hex().upper(), f"  {value.hex()}\n"):
            self.assertEqual(parse_hex_id(text), value)
        for text in ("", value.hex()[:-1], value.hex() + "0", "zz" * 16, None, 5):
            with self.assertRaises(ValueError):
                parse_hex_id(text)


if __name__ == "__main__":
    unittest.main()
