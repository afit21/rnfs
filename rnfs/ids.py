"""16-byte IDs written as 32 hex digits: node IDs, names, RNS identity hashes and LXMF addresses.

The one parser for these, so a rule about case, brackets or whitespace holds everywhere. Text may have whitespace
around it and, where brackets=True, RNS's <angle brackets> (as RNS prints hashes); nothing between the digits:
bytes.fromhex alone would skip whitespace there, giving fewer than 16 bytes from 32 characters.

Stdlib only: rnfs.config imports it, and the CLI and rnfs.Client load config without RNS.
"""
import re

HEX16 = re.compile(r"[0-9a-fA-F]{32}")  # exactly 32 hex digits, either case
# As rnfs writes them (bytes.hex()). Persisted rows are checked against this, unstripped: a row that fails is
# deleted when loaded, so what they accept mustn't change
HEX16_LOWER = re.compile(r"[0-9a-f]{32}")


def _digits(text: str, brackets: bool) -> str:
    """text without the whitespace around it and, if brackets, then the < and > around that."""
    stripped = text.strip()
    return stripped.strip("<>") if brackets else stripped


def is_hex16(text, *, brackets: bool = False, upper: bool = True) -> bool:
    """True if parse_hex16 would accept text."""
    if not isinstance(text, str):
        return False
    return (HEX16 if upper else HEX16_LOWER).fullmatch(_digits(text, brackets)) is not None


def parse_hex16(text, what: str, *, brackets: bool = False, upper: bool = True) -> bytes:
    """16 bytes from 32 hex digits, with whitespace around allowed and, if brackets, <angle brackets> inside that;
    upper=False refuses uppercase digits. Raises ValueError naming what (e.g. "node ID") for anything else."""
    if not is_hex16(text, brackets=brackets, upper=upper):
        raise ValueError(f"invalid {what} {text!r}: must be 32 hex characters")
    return bytes.fromhex(_digits(text, brackets))


def normal_hex16(text: str) -> str:
    """text as rnfs writes an ID (bytes.hex()): whitespace and <angle brackets> around it removed, lowercase. For
    settings Config.validate has checked; it doesn't check text is hex."""
    return _digits(text, True).lower()
