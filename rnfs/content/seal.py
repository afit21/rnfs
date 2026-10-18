"""Sealed files: content encrypted for chosen RNS identities (`rnfs add --to`), a content convention on top of
rnfs. A sealed file is an ordinary file to the rest of rnfs (CIDs, the DHT, transfer and storage are
unchanged); readers recognise it by its magic prefix. Format version 1:

    prefix   = MAGIC ("rnfs-sealed\\n", 12 bytes) || VERSION (1 byte, 0x01) || header length (4 bytes, big-endian)
               || header
    header   = msgpack [segment_size, [[identity_hash, wrapped_key], ...], sealed_name or nil]
    segments = segment 0 || segment 1 || ... || segment n-1     (n >= 1)
    segment  = iv (16) || AES-256-CBC(enc_key, iv, PKCS7(plaintext)) || tag (32)
    tag      = HMAC-SHA256(mac_key, SHA-256(prefix) || index (8 bytes, big-endian) || final (1 byte) || iv || ciphertext)

- The content key K is 32 random bytes. mac_key || enc_key = HKDF-SHA256(K, salt none, info
  "rnfs-seal-v1 segments", 64 bytes); the name's keys likewise with info "rnfs-seal-v1 name".
- wrapped_key = RNS.Identity.encrypt(K) to the recipient's public key (ephemeral X25519, HKDF salted with the
  recipient's identity hash, AES-256-CBC + HMAC-SHA256 token): 128 bytes. identity_hash is the recipient's
  16-byte RNS identity hash. Entries are sorted by identity hash, unique, 1 to MAX_RECIPIENTS.
- Every segment but the last holds exactly segment_size (65536) plaintext bytes; the last holds 0 to
  segment_size (0 only when the whole file is empty), and only it has final = 1. Binding the index, the final
  flag and the prefix's hash into every tag detects reordering, truncation, splicing and header changes.
- sealed_name = iv || AES-256-CBC(PKCS7(UTF-8 name)) || HMAC-SHA256(name mac key, iv || ciphertext).

Readers find the segment count from the ciphertext size, so the plaintext size needs the last segment
(decrypting it shows its padding). Sealing gives confidentiality only: anyone may seal a file for you, and
any recipient could re-seal different content for the others, so it says nothing about who wrote it.
Crypto comes from RNS.Cryptography and RNS.Identity, already loaded with RNS's bundled msgpack (importing
RNS.vendor.umsgpack runs RNS/__init__).
"""
import hashlib
import hmac
import os
from collections.abc import Callable, Iterator
from typing import BinaryIO

from RNS.vendor import umsgpack

from rnfs.constants import KIB
from rnfs.content import chunker, dag
from rnfs.errors import NotRecipient, SealError  # defined there, so rnfs.<name> exports them without RNS

MAGIC = b"rnfs-sealed\n"
VERSION = 1
PREFIX_SIZE = len(MAGIC) + 1 + 4  # magic, version, header length
SEGMENT_SIZE = 64 * KIB
TAG_SIZE = 32
IV_SIZE = 16
FULL_SEGMENT = IV_SIZE + SEGMENT_SIZE + 16 + TAG_SIZE  # PKCS7 pads a full segment by a whole block
MIN_SEGMENT = IV_SIZE + 16 + TAG_SIZE  # an empty plaintext: one block of padding
MAX_HEADER_SIZE = 64 * KIB
MAX_RECIPIENTS = 256
KEY_SIZE = 32
IDENTITY_HASH_SIZE = 16
PUBLIC_KEY_SIZE = 64
WRAPPED_KEY_SIZE = 32 + IV_SIZE + 48 + TAG_SIZE  # ephemeral key, then an RNS token of the padded 32-byte key
SEGMENT_INFO = b"rnfs-seal-v1 segments"
NAME_INFO = b"rnfs-seal-v1 name"

RawReader = Callable[[int, int], Iterator[bytes]]  # (offset, length) -> the stored bytes of that range


def is_sealed(head: bytes) -> bool:
    """True if a file's first bytes carry the sealed-file magic."""
    return head.startswith(MAGIC)


def _crypto():
    """RNS's AES and HKDF (already loaded with RNS, which the module's umsgpack import loads)."""
    from RNS.Cryptography import hkdf
    from RNS.Cryptography.AES import AES_256_CBC
    return AES_256_CBC, hkdf


def _keys(content_key: bytes, info: bytes) -> tuple[bytes, bytes]:
    """(mac_key, enc_key) derived from the content key for one purpose."""
    _, hkdf = _crypto()
    derived = hkdf(length=64, derive_from=content_key, salt=None, context=info)
    return derived[:32], derived[32:]


def _pad(data: bytes) -> bytes:
    """PKCS#7 padding to 16 bytes (always adding 1 to 16 bytes)."""
    n = 16 - len(data) % 16
    return data + bytes([n]) * n


def _unpad(data: bytes) -> bytes:
    """Strip PKCS#7 padding, checking every padding byte. Raises SealError."""
    n = data[-1] if data else 0
    if not 1 <= n <= 16 or data[-n:] != bytes([n]) * n:
        raise SealError("sealed file has invalid padding")
    return data[:-n]


def _encrypt_part(mac_key: bytes, enc_key: bytes, plaintext: bytes, bound: bytes) -> bytes:
    """iv || ciphertext || tag, the tag over bound || iv || ciphertext."""
    aes, _ = _crypto()
    iv = os.urandom(IV_SIZE)
    ciphertext = aes.encrypt(_pad(plaintext), enc_key, iv)
    tag = hmac.new(mac_key, bound + iv + ciphertext, hashlib.sha256).digest()
    return iv + ciphertext + tag


def _decrypt_part(mac_key: bytes, enc_key: bytes, part: bytes, bound: bytes, what: str) -> bytes:
    """The plaintext of iv || ciphertext || tag after checking the tag. Raises SealError."""
    if len(part) < MIN_SEGMENT or (len(part) - IV_SIZE - TAG_SIZE) % 16:
        raise SealError(f"sealed file is truncated or malformed ({what})")
    iv, ciphertext, tag = part[:IV_SIZE], part[IV_SIZE:-TAG_SIZE], part[-TAG_SIZE:]
    expected = hmac.new(mac_key, bound + iv + ciphertext, hashlib.sha256).digest()
    if not hmac.compare_digest(tag, expected):
        raise SealError(f"sealed file failed authentication ({what}): it was tampered with, truncated or reordered")
    aes, _ = _crypto()
    return _unpad(aes.decrypt(ciphertext, enc_key, iv))


def _segment_bound(header_hash: bytes, index: int, final: bool) -> bytes:
    """What a segment's tag binds besides its own bytes."""
    return header_hash + index.to_bytes(8, "big") + (b"\x01" if final else b"\x00")


# Recipients


def identity_hash(public_key: bytes) -> bytes:
    """The RNS identity hash of a 64-byte public key: SHA-256 truncated to 16 bytes."""
    return hashlib.sha256(public_key).digest()[:IDENTITY_HASH_SIZE]


def parse_recipient(text: str, recall: Callable[[bytes], bytes | None],
                    lookup_name: Callable[[str], bytes | None] | None = None) -> bytes:
    """The public key of a recipient given as a destination or identity hash (32 hex characters, which RNS
    must know: recall(hash) returns its public key), a public key (128 hex characters), or a name that
    lookup_name (contacts) turns into a hash. Raises ValueError."""
    if not isinstance(text, str) or not text.strip():
        raise ValueError("empty recipient")
    cleaned = text.strip().strip("<>").lower()
    try:
        value = bytes.fromhex(cleaned)
    except ValueError:
        value = None
    if value is not None and len(value) == PUBLIC_KEY_SIZE:
        return value
    if value is None or len(value) != IDENTITY_HASH_SIZE:
        resolved = lookup_name(text.strip()) if lookup_name is not None else None
        if resolved is None:
            raise ValueError(f"recipient {text!r} is neither a 32-hex-character destination or identity hash "
                             f"nor a 128-hex-character public key" + (" nor a known contact" if lookup_name else ""))
        value = resolved
    public_key = recall(value)
    if public_key is None:
        raise ValueError(f"RNS doesn't know the identity of recipient {text}: it must have announced (an rnfs "
                         f"node ID or an LXMF address heard recently), or give its 128-hex-character public key")
    return public_key


# Sealing


class SealingReader:
    """A file-like reader giving the sealed form of everything read from stream, segment by segment.

    Holds one segment of plaintext and one of ciphertext at a time, so files of any size stream through.
    """

    def __init__(self, stream: BinaryIO, recipients: list[bytes], name: str | None = None, size: int | None = None):
        """Seal stream for recipients (public keys); name, if given, is sealed into the header. With size,
        exactly size bytes are read (ValueError if the stream ends early, so a cut-off upload isn't sealed)."""
        import RNS  # RNS.Identity wraps the content key

        keys = {}
        for public_key in recipients:
            if len(public_key) != PUBLIC_KEY_SIZE:
                raise ValueError("a recipient's public key must be 64 bytes")
            keys[identity_hash(public_key)] = public_key
        if not keys:
            raise ValueError("sealing needs at least one recipient")
        if len(keys) > MAX_RECIPIENTS:
            raise ValueError(f"a sealed file has at most {MAX_RECIPIENTS} recipients")
        content_key = os.urandom(KEY_SIZE)
        entries = []
        for hash_, public_key in sorted(keys.items()):
            identity = RNS.Identity(create_keys=False)
            identity.load_public_key(public_key)
            entries.append([hash_, identity.encrypt(content_key)])
        sealed_name = None
        if name is not None:
            dag.check_name(name)
            sealed_name = _encrypt_part(*_keys(content_key, NAME_INFO), name.encode("utf-8"), b"")
        header = umsgpack.packb([SEGMENT_SIZE, entries, sealed_name])
        prefix = MAGIC + bytes([VERSION]) + len(header).to_bytes(4, "big") + header
        if len(header) > MAX_HEADER_SIZE:
            raise ValueError("sealed header too large")
        self._header_hash = hashlib.sha256(prefix).digest()
        self._mac_key, self._enc_key = _keys(content_key, SEGMENT_INFO)
        self._stream = stream
        self._remaining = size
        self._out = bytearray(prefix)
        self._next = self._read_segment()
        self._index = 0
        self._done = False

    def _read_segment(self) -> bytes:
        """Up to SEGMENT_SIZE plaintext bytes (fewer only at the end; ValueError if a sized stream ends early)."""
        data, self._remaining = chunker.read_limited(self._stream, SEGMENT_SIZE, self._remaining)
        return data

    def _produce(self):
        """Seal the next segment, reading one ahead to know whether it is the last."""
        current = self._next
        self._next = self._read_segment() if len(current) == SEGMENT_SIZE else b""
        final = not self._next
        self._out += _encrypt_part(self._mac_key, self._enc_key, current,
                                   _segment_bound(self._header_hash, self._index, final))
        self._index += 1
        self._done = final

    def read(self, size: int = -1) -> bytes:
        """Up to size bytes of the sealed file (everything left if size < 0); b"" at the end."""
        while (size < 0 or len(self._out) < size) and not self._done:
            self._produce()
        if size < 0 or size >= len(self._out):
            data = bytes(self._out)
            self._out.clear()
        else:
            data = bytes(self._out[:size])
            del self._out[:size]
        return data


def seal_bytes(data: bytes, recipients: list[bytes], name: str | None = None) -> bytes:
    """The sealed form of data held in memory (tests and small uses)."""
    import io
    return SealingReader(io.BytesIO(data), recipients, name).read()


# Opening


class SealedFile:
    """A sealed file being read through raw(offset, length), its stored (ciphertext) size total."""

    def __init__(self, raw: RawReader, total: int):
        """Read and check the prefix. Raises SealError if it is malformed (use open_sealed to test the magic)."""
        self._raw = raw
        self.total = total
        fixed = self._read(0, PREFIX_SIZE)
        if not is_sealed(fixed):
            raise SealError("not a sealed file")
        if fixed[len(MAGIC)] != VERSION:
            raise SealError(f"sealed file format version {fixed[len(MAGIC)]} isn't supported (only {VERSION})")
        header_size = int.from_bytes(fixed[len(MAGIC) + 1:PREFIX_SIZE], "big")
        if not 0 < header_size <= MAX_HEADER_SIZE or PREFIX_SIZE + header_size > total:
            raise SealError("sealed file has an invalid header length")
        header = self._read(PREFIX_SIZE, header_size)
        self.header_hash = hashlib.sha256(fixed + header).digest()
        self.recipients, self._sealed_name = _decode_header(header)
        self.body_start = PREFIX_SIZE + header_size
        body = total - self.body_start
        if body < MIN_SEGMENT:
            raise SealError("sealed file is truncated: it has no segments")
        self.segments = -(-body // FULL_SEGMENT)
        last = body - (self.segments - 1) * FULL_SEGMENT
        if last < MIN_SEGMENT or (last - IV_SIZE - TAG_SIZE) % 16:
            raise SealError("sealed file is truncated or malformed (its size doesn't end on a segment)")
        self._keys: tuple[bytes, bytes] | None = None
        self._content_key: bytes | None = None
        self._size: int | None = None

    def _read(self, offset: int, length: int) -> bytes:
        """Exactly length stored bytes at offset. Raises SealError if the file is shorter."""
        data = b"".join(self._raw(offset, length))
        if len(data) != length:
            raise SealError("sealed file is truncated")
        return data

    def unlock(self, identity) -> None:
        """Unwrap the content key with identity (an RNS.Identity with its private key).

        Raises NotRecipient if the identity isn't a recipient, SealError if its wrapped key doesn't open.
        """
        own_hash = bytes(identity.hash)
        wrapped = self.recipients.get(own_hash)
        if wrapped is None:
            raise NotRecipient(f"this file is sealed for {len(self.recipients)} other "
                               f"recipient{'s' if len(self.recipients) != 1 else ''}, not this node's identity "
                               f"{own_hash.hex()} (use --raw for the ciphertext)")
        key = identity.decrypt(wrapped)
        if key is None or len(key) != KEY_SIZE:
            raise SealError("the content key sealed for this node's identity doesn't decrypt: it was tampered with")
        self._content_key = key
        self._keys = _keys(key, SEGMENT_INFO)

    @property
    def name(self) -> str | None:
        """The sealed file name, if it has a valid one (needs unlock); None if it is absent or invalid."""
        if self._sealed_name is None or self._content_key is None:
            return None
        # The name is advisory (spec 5a): one that fails its tag or isn't a valid name is ignored rather
        # than making the whole file unreadable (SealError is a ValueError, as check_name raises)
        try:
            text = _decrypt_part(*_keys(self._content_key, NAME_INFO), self._sealed_name, b"", "name")
            name = text.decode("utf-8")
            dag.check_name(name)
        except (UnicodeDecodeError, ValueError):
            return None
        return name

    def _segment(self, index: int, data: bytes) -> bytes:
        """Decrypt segment index from its stored bytes."""
        if self._keys is None:
            raise NotRecipient("sealed file isn't unlocked")
        final = index == self.segments - 1
        plaintext = _decrypt_part(*self._keys, data, _segment_bound(self.header_hash, index, final), f"segment {index}")
        # The final segment may hold up to SEGMENT_SIZE bytes too: PKCS7 lets a sealer pad 65551 bytes into
        # the size of a full segment, which would make size disagree with what reads return
        if (len(plaintext) > SEGMENT_SIZE or (not final and len(plaintext) != SEGMENT_SIZE)
                or (final and index > 0 and not plaintext)):
            raise SealError(f"sealed file segment {index} has the wrong size")
        return plaintext

    def _segment_range(self, index: int) -> tuple[int, int]:
        """(offset, length) of segment index in the stored file."""
        offset = self.body_start + index * FULL_SEGMENT
        return offset, min(FULL_SEGMENT, self.total - offset)

    @property
    def size(self) -> int:
        """The plaintext size (decrypting the last segment the first time, which also checks it)."""
        if self._size is None:
            last = self.segments - 1
            self._size = last * SEGMENT_SIZE + len(self._segment(last, self._read(*self._segment_range(last))))
        return self._size

    def read(self, offset: int = 0, length: int | None = None) -> Iterator[bytes]:
        """The plaintext in [offset, offset + length) (to the end if length is None), decrypting only the
        segments covering it, streamed from one raw read of their stored bytes."""
        if offset < 0 or (length is not None and length < 0):
            raise ValueError("offset and length must be non-negative")
        if length == 0:
            return
        first = offset // SEGMENT_SIZE
        if first >= self.segments:
            return
        last = self.segments - 1 if length is None else min(self.segments - 1, (offset + length - 1) // SEGMENT_SIZE)
        start = self._segment_range(first)[0]
        end_offset, end_length = self._segment_range(last)
        position = first * SEGMENT_SIZE  # plaintext offset of the segment being assembled
        end = None if length is None else offset + length
        pending = bytearray()
        index = first
        for data in self._raw(start, end_offset + end_length - start):
            pending += data
            while index <= last and len(pending) >= self._segment_range(index)[1]:
                size = self._segment_range(index)[1]
                plaintext = self._segment(index, bytes(pending[:size]))
                del pending[:size]
                low = max(0, offset - position)
                high = len(plaintext) if end is None else min(len(plaintext), end - position)
                if high > low:
                    yield plaintext[low:high]
                position += len(plaintext)
                index += 1
        if index <= last:
            raise SealError("sealed file is truncated")


def _decode_header(header: bytes) -> tuple[dict[bytes, bytes], bytes | None]:
    """{identity hash: wrapped key} and the sealed name of a strictly encoded header. Raises SealError."""
    try:
        value = umsgpack.unpackb(header)
    except Exception:  # noqa: BLE001 - umsgpack raises several types for hostile bytes
        raise SealError("sealed file header isn't valid msgpack") from None
    if not (isinstance(value, list) and len(value) == 3):
        raise SealError("sealed file header must be [segment_size, recipients, name]")
    segment_size, entries, sealed_name = value
    if type(segment_size) is not int or segment_size != SEGMENT_SIZE:
        raise SealError(f"sealed file segment size must be {SEGMENT_SIZE}")
    if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_RECIPIENTS:
        raise SealError(f"sealed file must list 1 to {MAX_RECIPIENTS} recipients")
    recipients: dict[bytes, bytes] = {}
    previous = b""
    for entry in entries:
        if not (isinstance(entry, list) and len(entry) == 2 and isinstance(entry[0], bytes)
                and isinstance(entry[1], bytes) and len(entry[0]) == IDENTITY_HASH_SIZE
                and len(entry[1]) == WRAPPED_KEY_SIZE):
            raise SealError("sealed file has a malformed recipient entry")
        if entry[0] <= previous:
            raise SealError("sealed file recipients must be sorted and unique")
        previous = entry[0]
        recipients[entry[0]] = entry[1]
    if sealed_name is not None and not isinstance(sealed_name, bytes):
        raise SealError("sealed file name must be bin or nil")
    if umsgpack.packb(value) != header:
        raise SealError("sealed file header isn't in its shortest encoding")
    return recipients, sealed_name


def open_sealed(raw: RawReader, total: int) -> SealedFile | None:
    """The sealed file stored behind raw, or None if it isn't one (no magic). Raises SealError if malformed."""
    if total < PREFIX_SIZE:
        return None
    head = b"".join(raw(0, len(MAGIC)))
    if not is_sealed(head):
        return None
    return SealedFile(raw, total)
