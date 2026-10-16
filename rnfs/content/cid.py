"""Content identifiers (specification section 4). No RNS dependency.

| Codec    | Binary CID                        | Block contents                          |
| raw      | 01 55 12 20 + digest (36 bytes)   | a whole file, or one chunk of a file    |
| rnfs-dag | 01 80 80 c0 01 12 20 + digest (39)| a DAG node (section 5)                  |

The digest is the SHA-256 of the block. Written for people as "b" + lowercase unpadded base32 of the
binary CID; on the wire CIDs are always binary. Nothing else is a valid CID, and every CID has exactly
one written form.
"""
import base64
import hashlib

from rnfs.constants import ID_LENGTH

RAW = "raw"
DAG = "dag"

_PREFIXES = {
    RAW: bytes([0x01, 0x55, 0x12, 0x20]),
    DAG: bytes([0x01, 0x80, 0x80, 0xC0, 0x01, 0x12, 0x20]),  # codec 0x300000, multicodec private use
}
_DIGEST_LENGTH = 32


class Cid:
    """A valid CID. Immutable and hashable; compare and use as dict keys freely.

    Build one with Cid.for_block(codec, block), Cid.from_bytes(binary) or Cid.parse(text).
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
