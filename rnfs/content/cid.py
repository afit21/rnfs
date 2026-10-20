"""Content identifiers (specification section 4). No RNS dependency.

| Codec    | Binary CID                        | Block contents                          |
| raw      | 01 55 12 20 + digest (36 bytes)   | a whole file, or one chunk of a file    |
| rnfs-dag | 01 80 80 c0 01 12 20 + digest (39)| a DAG node (section 5)                  |

The digest is the SHA-256 of the block. Written for people as "b" + lowercase unpadded base32 of the
binary CID; on the wire CIDs are always binary. Nothing else is a valid CID, and every CID has exactly
one written form. The one exception is the gateway's subdomain hosts, where a CID is "k" + lowercase base36
(to_base36, from_base36: a host-only form, which Cid.parse never accepts).
"""
import base64
import hashlib
import re

from rnfs.constants import ID_LENGTH

RAW = "raw"
DAG = "dag"

_PREFIXES = {
    RAW: bytes([0x01, 0x55, 0x12, 0x20]),
    DAG: bytes([0x01, 0x80, 0x80, 0xC0, 0x01, 0x12, 0x20]),  # codec 0x300000, multicodec private use
}
_DIGEST_LENGTH = 32
_BASE32 = "abcdefghijklmnopqrstuvwxyz234567"  # RFC 4648, lowercase as written
_TO_INT_DIGITS = str.maketrans(_BASE32, "0123456789abcdefghijklmnopqrstuv")  # the same values as int(..., 32) reads
_BASE36 = "0123456789abcdefghijklmnopqrstuvwxyz"
# Digits after the "k" -> binary length: 55 for raw CIDs (56 characters), 59 for rnfs-dag (60), both within DNS's
# 63-character label limit. Each codec's prefix fixes the count for every digest (tests/test_cid.py checks)
_HOST_LENGTHS = {55: 36, 59: 39}
_HOST_FORM = re.compile("k(?:" + "|".join(f"[0-9a-z]{{{n}}}" for n in sorted(_HOST_LENGTHS)) + ")")


def _written_form(prefix: bytes) -> tuple[int, int, str]:
    """(length, spare bits, regular expression) of the canonical written form of the CIDs with prefix: "b", then
    lowercase base32 digits, the final one's spare bits (past the binary's last) zero, as encoding leaves them."""
    bits = (len(prefix) + _DIGEST_LENGTH) * 8
    digits = -(-bits // 5)
    spare_bits = digits * 5 - bits
    last = "".join(c for value, c in enumerate(_BASE32) if value % (1 << spare_bits) == 0)
    return 1 + digits, spare_bits, f"b[a-z2-7]{{{digits - 1}}}[{last}]"


# For from_block_name(): every canonical written form (but for the prefix), and written length -> (prefix, spare bits)
_FORMS = {prefix: _written_form(prefix) for prefix in _PREFIXES.values()}
_CANONICAL = re.compile("|".join(pattern for _, _, pattern in _FORMS.values()))
_WRITTEN = {length: (prefix, spare_bits) for prefix, (length, spare_bits, _) in _FORMS.items()}


class Cid:
    """A valid CID. Immutable and hashable; compare and use as dict keys freely.

    Build one with Cid.for_block(codec, block), Cid.from_bytes(binary) or Cid.parse(text) (or from_block_name).
    Every constructor raises ValueError for anything that isn't a valid CID.
    """

    # _text and _key memoise str() and .key, computed on first use (a CID never changes)
    __slots__ = ("binary", "codec", "_text", "_key")

    def __init__(self, binary: bytes):
        """Use from_bytes; this assumes binary is already known to be valid."""
        self.binary = binary
        self.codec = RAW if len(binary) == len(_PREFIXES[RAW]) + _DIGEST_LENGTH else DAG
        self._text = None
        self._key = None

    @classmethod
    def from_bytes(cls, binary) -> "Cid":
        """The CID with this binary form. Raises ValueError unless it is exactly a raw or rnfs-dag CID."""
        if not isinstance(binary, (bytes, bytearray)):
            raise ValueError("binary CID must be bytes")
        binary = bytes(binary)
        for prefix in _PREFIXES.values():
            if len(binary) == len(prefix) + _DIGEST_LENGTH and binary.startswith(prefix):
                return cls(binary)
        raise ValueError("not a raw or rnfs-dag CID")

    @classmethod
    def for_block(cls, codec: str, block: bytes) -> "Cid":
        """The CID naming block under codec (RAW or DAG)."""
        return cls(_PREFIXES[codec] + hashlib.sha256(block).digest())

    @classmethod
    def raw_of_digest(cls, digest: bytes) -> "Cid":
        """The raw CID of the block whose SHA-256 is digest: a file of one block has its content's hash as its CID's
        digest."""
        if len(digest) != _DIGEST_LENGTH:
            raise ValueError(f"a SHA-256 digest is {_DIGEST_LENGTH} bytes, not {len(digest)}")
        return cls(_PREFIXES[RAW] + digest)

    @classmethod
    def parse(cls, text) -> "Cid":
        """The CID written as text ("b" + base32). Raises ValueError unless text is its one canonical form."""
        if not isinstance(text, str) or not text.startswith("b") or not text.isascii():
            raise ValueError(f"invalid CID {text!r}: must start with 'b' (base32)")
        body = text[1:]
        try:
            binary = base64.b32decode(body.upper() + "=" * (-len(body) % 8))
        except ValueError:
            raise ValueError(f"invalid CID {text!r}: not base32") from None
        cid = cls.from_bytes(binary)
        if str(cid) != text:
            raise ValueError(f"invalid CID {text!r}: not in canonical form (lowercase, unpadded)")
        return cid

    @classmethod
    def from_block_name(cls, name: str) -> "Cid":
        """The CID whose written form is name, a file name read from the block directory. Accepts exactly what
        parse() accepts and raises ValueError for the rest, but checks the canonical form without re-encoding,
        and keeps name as str(): opening a store scans every block's name. API and wire input use parse()."""
        if not isinstance(name, str) or not _CANONICAL.fullmatch(name):
            raise ValueError(f"invalid CID {name!r}: not in canonical form")
        prefix, spare_bits = _WRITTEN[len(name)]
        # The digits are known to be base32 here, so int() decodes them (far faster than b32decode)
        value = int(name[1:].translate(_TO_INT_DIGITS), 32) >> spare_bits
        binary = value.to_bytes(len(prefix) + _DIGEST_LENGTH, "big")
        if not binary.startswith(prefix):
            raise ValueError(f"invalid CID {name!r}: not a raw or rnfs-dag CID")
        cid = cls(binary)
        cid._text = name
        return cid

    @property
    def digest(self) -> bytes:
        """The block's SHA-256."""
        return self.binary[-_DIGEST_LENGTH:]

    @property
    def key(self) -> bytes:
        """DHT key: SHA-256 of the whole binary CID, truncated to the 128-bit node ID space."""
        if self._key is None:
            self._key = hashlib.sha256(self.binary).digest()[:ID_LENGTH]
        return self._key

    @property
    def is_raw(self) -> bool:
        """True for a raw block's CID."""
        return self.codec == RAW

    def verify(self, block: bytes) -> bool:
        """True if block's SHA-256 is this CID's digest."""
        return hashlib.sha256(block).digest() == self.digest

    def __str__(self) -> str:
        if self._text is None:
            self._text = "b" + base64.b32encode(self.binary).decode("ascii").lower().rstrip("=")
        return self._text

    def __repr__(self) -> str:
        return f"Cid({self})"

    def __eq__(self, other) -> bool:
        return isinstance(other, Cid) and other.binary == self.binary

    def __hash__(self) -> int:
        return hash(self.binary)

    def __lt__(self, other: "Cid") -> bool:
        return self.binary < other.binary


def to_base36(cid: Cid) -> str:
    """cid's gateway host form: "k" + lowercase base36 of the binary CID (specification section 4). Only the
    gateway's subdomain hosts use it; everywhere else a CID is written as str(cid)."""
    value = int.from_bytes(cid.binary, "big")  # never 0, and no leading zero byte: every prefix starts 01
    digits = []
    while value:
        value, digit = divmod(value, 36)
        digits.append(_BASE36[digit])
    return "k" + "".join(reversed(digits))


def from_base36(text) -> Cid:
    """The CID whose gateway host form is text, as to_base36 writes it, and nothing else: the exact length for
    a raw or rnfs-dag CID, lowercase base36 digits only, and canonical (to_base36 of the result is text).
    Raises ValueError. Cid.parse never accepts this form; callers turn the result into str() before any other
    use, so the written form stays the one everywhere else."""
    if not isinstance(text, str) or not _HOST_FORM.fullmatch(text):
        raise ValueError(f"invalid CID {text!r} in host: must be k + {' or '.join(map(str, _HOST_LENGTHS))} "
                         "lowercase base36 digits")
    length = _HOST_LENGTHS[len(text) - 1]
    # int() would also take "_", signs, whitespace and non-ASCII digits: the regular expression has ruled them
    # out. 36 ** digits < 256 ** length for both lengths, so to_bytes can't overflow
    value = int(text[1:], 36)
    cid = Cid.from_bytes(value.to_bytes(length, "big"))
    if to_base36(cid) != text:
        raise ValueError(f"invalid CID {text!r} in host: not in canonical form")
    return cid


def is_valid(text) -> bool:
    """True if text is a valid written CID."""
    try:
        Cid.parse(text)
        return True
    except ValueError:
        return False


EMPTY_FILE = Cid.for_block(RAW, b"")
EMPTY_DIRECTORY_BLOCK = b"\x92\x02\x90"  # msgpack [2, []]
EMPTY_DIRECTORY = Cid.for_block(DAG, EMPTY_DIRECTORY_BLOCK)
