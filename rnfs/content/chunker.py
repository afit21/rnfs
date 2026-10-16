"""Cutting a file's bytes into chunks: the spec's fixed CHUNK_SIZE pieces (the default) or FastCDC
content-defined chunks (`add --chunker fastcdc`). No RNS dependency.

Readers accept any valid DAG, so the chunker only decides CIDs: the same content added with the same
chunker always gets the same CID, and every implementation of FastCDC as specified here cuts identically.

FastCDC (Xia et al., "FastCDC: a Fast and Efficient Content-Defined Chunking Approach", 2016), with
normalized chunking, as fixed by the specification:
- MIN_SIZE 16 KiB, AVG_SIZE 64 KiB, MAX_SIZE 128 KiB (= MAX_BLOCK_SIZE).
- GEAR[i] (i = 0..255) is the first 8 bytes, big-endian, of SHA-256("rnfs-fastcdc-gear" || byte i).
- Gear hash: h = 0 at offset MIN_SIZE of each chunk, then for every byte b: h = ((h << 1) + GEAR[b]) mod 2^64.
- At chunk offset i (0-based, MIN_SIZE <= i < MAX_SIZE), after hashing byte i: cut after it (the chunk is
  bytes 0..i) if h & MASK_S == 0 while i < AVG_SIZE, or h & MASK_L == 0 from AVG_SIZE on. MASK_S is the top
  18 bits of 64 (harder to meet before the average), MASK_L the top 14 bits (easier after it).
- A chunk with no cut point ends at MAX_SIZE; data left of at most MIN_SIZE bytes is the last chunk.

Speed: a byte-by-byte loop in CPython manages about 15 MB/s. The hash after byte i only depends on the 64
bytes before it: h_i = sum over k < 64 of GEAR[b_(i-k)] << k (mod 2^64), with nothing before the chunk's
MIN_SIZE offset. So the hashes of a whole block of bytes are computed at once on one big integer holding a
9-byte lane per position (a guard byte, then the 64-bit value): the lanes are filled by bytes.translate
into each byte plane and strided slice assignment, the sum is formed by six shift-and-add doublings
(S_2m(i) = S_m(i) + S_m(i - m) << m), and lanes meeting the mask are found with strided slices and
bytes.find. Everything runs in C loops inside CPython, about 2.5 times as fast as the plain loop.
"""
import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import BinaryIO

from rnfs import constants

FIXED = "fixed"
FASTCDC = "fastcdc"
CHUNKERS = (FIXED, FASTCDC)

MIN_SIZE = 16 * constants.KIB
AVG_SIZE = 64 * constants.KIB
MAX_SIZE = 128 * constants.KIB
GEAR_SEED = b"rnfs-fastcdc-gear"
GEAR = tuple(int.from_bytes(hashlib.sha256(GEAR_SEED + bytes([i])).digest()[:8], "big") for i in range(256))
MASK_S = ((1 << 18) - 1) << 46
MASK_L = ((1 << 14) - 1) << 50
_M64 = (1 << 64) - 1

READ_SIZE = 1 * constants.MIB  # bytes read from the stream at a time
_LANE = 9  # bytes per position in the lane integer: a guard byte for carries, then the hash, big-endian
_BLOCK = 8 * constants.KIB  # positions hashed per lane integer (measured fastest: masks stay small)
_CONTEXT = 63  # bytes before a block that still affect its hashes
# GEAR's byte planes: _PLANES[k][b] is byte k (0 = most significant) of GEAR[b]
_PLANES = tuple(bytes((GEAR[b] >> (8 * (7 - k))) & 0xFF for b in range(256)) for k in range(8))
# Hash bytes 3 (for MASK_S: 16 + 2 bits) and 2 (for MASK_L: 8 + 6 bits) reduced to 0 where the mask is met
_PARTIAL_S = bytes(0 if b & 0xC0 == 0 else 1 for b in range(256))
_PARTIAL_L = bytes(0 if b & 0xFC == 0 else 1 for b in range(256))

_active: ContextVar[str] = ContextVar("rnfs_chunker", default=FIXED)


def check(name: str) -> str:
    """name if it is a known chunker, else ValueError."""
    if name not in CHUNKERS:
        raise ValueError(f"unknown chunker {name!r}: use {' or '.join(CHUNKERS)}")
    return name


@contextmanager
def using(name: str | None):
    """Make name (default fixed) the chunker builder.build_file uses in this thread/context."""
    token = _active.set(check(name or FIXED))
    try:
        yield
    finally:
        _active.reset(token)


def active() -> str:
    """The chunker in use in this context."""
    return _active.get()


def chunks(stream: BinaryIO, size: int | None = None) -> Iterator[bytes]:
    """A file's chunks, by the active chunker; never an empty chunk. With size, exactly size bytes are read
    (ValueError if the stream ends early)."""
    if _active.get() == FASTCDC:
        return fastcdc_stream(stream, size)
    return fixed_chunks(stream, size)


def read_full(stream: BinaryIO, want: int) -> bytes:
    """Up to want bytes from stream, fewer only at end of stream (pipes and sockets return short reads)."""
    parts = []
    got = 0
    while got < want:
        data = stream.read(want - got)
        if not data:
            break
        parts.append(data)
        got += len(data)
    return b"".join(parts)


def read_limited(stream: BinaryIO, want: int, remaining: int | None) -> tuple[bytes, int | None]:
    """Up to want bytes (within remaining, if set) and the new remaining. ValueError if a sized stream ends early."""
    if remaining is not None:
        want = min(want, remaining)
    data = read_full(stream, want)
    if remaining is not None:
        if len(data) < want:
            raise ValueError(f"stream ended {remaining - len(data)} bytes early")
        remaining -= len(data)
    return data, remaining


def fixed_chunks(stream: BinaryIO, size: int | None = None) -> Iterator[bytes]:
    """Consecutive CHUNK_SIZE pieces (the last may be shorter, never empty): the spec's tree rule."""
    remaining = size
    while remaining is None or remaining > 0:
        chunk, remaining = read_limited(stream, constants.CHUNK_SIZE, remaining)
        if not chunk:
            return
        yield chunk
        if len(chunk) < constants.CHUNK_SIZE and remaining is None:
            return  # end of stream


def fastcdc_stream(stream: BinaryIO, size: int | None = None) -> Iterator[bytes]:
    """FastCDC chunks of a stream, holding at most READ_SIZE + MAX_SIZE bytes."""
    remaining = size
    buffer = b""
    start = 0
    eof = False
    while True:
        if not eof and len(buffer) - start < MAX_SIZE:
            data, remaining = read_limited(stream, READ_SIZE, remaining)
            eof = len(data) < READ_SIZE or remaining == 0
            buffer = buffer[start:] + data
            start = 0
        if start >= len(buffer):
            return
        end = cut_point(buffer, start, len(buffer)) if eof or len(buffer) - start >= MAX_SIZE else None
        if end is None:
            continue
        yield buffer[start:end]
        start = end


def fastcdc_cuts(data: bytes) -> list[int]:
    """The chunk lengths FastCDC gives data held in memory."""
    lengths = []
    start = 0
    while start < len(data):
        end = cut_point(data, start, len(data))
        lengths.append(end - start)
        start = end
    return lengths


def cut_point(data: bytes, start: int, end: int) -> int:
    """Where the chunk starting at data[start] ends, given that the data (as far as known) ends at end.

    The caller must provide at least MAX_SIZE bytes from start unless end is the end of the file.
    """
    if end - start <= MIN_SIZE:
        return end
    limit = min(end, start + MAX_SIZE)
    normal = min(limit, start + AVG_SIZE)
    base = start + MIN_SIZE
    cut = _scan(data, base, normal, base, False)
    if cut < 0:
        cut = _scan(data, normal, limit, base, True)
    return limit if cut < 0 else cut


def cut_point_simple(data: bytes, start: int, end: int) -> int:
    """cut_point by the byte-by-byte definition: the reference the fast version is tested against."""
    if end - start <= MIN_SIZE:
        return end
    limit = min(end, start + MAX_SIZE)
    normal = min(limit, start + AVG_SIZE)
    h = 0
    for i in range(start + MIN_SIZE, limit):
        h = ((h << 1) + GEAR[data[i]]) & _M64
        if not h & (MASK_S if i < normal else MASK_L):
            return i + 1
    return limit


_mask_cache: dict[int, tuple[int, dict[int, int]]] = {}


def _masks(count: int) -> tuple[int, dict[int, int]]:
    """For count lanes: the mask keeping each lane's 64-bit value, and per shift s the mask keeping its low
    64 - s bits (so shifting a lane up by s can't spill into its neighbour)."""
    masks = _mask_cache.get(count)
    if masks is None:
        full = int.from_bytes((b"\x00" + b"\xff" * 8) * count, "big")
        low = {s: int.from_bytes(((1 << (64 - s)) - 1).to_bytes(_LANE, "big") * count, "big") for s in (8, 16, 32)}
        masks = (full, low)
        if len(_mask_cache) >= 8:
            _mask_cache.clear()  # only the full block size and a few tails recur
        _mask_cache[count] = masks
    return masks


def _hashes(segment: bytes) -> bytes:
    """The gear hash after each byte of segment (hash 0 before it) as 9-byte big-endian lanes."""
    count = len(segment)
    lanes = bytearray(_LANE * count)
    for k in range(8):
        lanes[1 + k::_LANE] = segment.translate(_PLANES[k])
    x = int.from_bytes(lanes, "big")
    full, low = _masks(count)
    # Lane i becomes the sum over k < 2m of GEAR[b_(i-k)] << k; the guard byte absorbs carries and shifts
    # below 8 bits, larger shifts are pre-masked
    for m in (1, 2, 4):
        x = (x + (x >> (_LANE * 8 * m - m))) & full
    for m in (8, 16, 32):
        x = (x + ((x & low[m]) >> (_LANE * 8 * m - m))) & full
    return x.to_bytes(_LANE * count, "big")


def _first_match(hashes: bytes, count: int, long: bool) -> int:
    """Index of the first lane whose hash meets MASK_L (long) or MASK_S, or -1."""
    top = int.from_bytes(hashes[1::_LANE], "big")
    if long:
        combined = top | int.from_bytes(hashes[2::_LANE].translate(_PARTIAL_L), "big")
    else:
        combined = (top | int.from_bytes(hashes[2::_LANE], "big")
                    | int.from_bytes(hashes[3::_LANE].translate(_PARTIAL_S), "big"))
    return combined.to_bytes(count, "big").find(b"\x00")


def _scan(data: bytes, low: int, high: int, base: int, long: bool) -> int:
    """The first cut point in data[low:high] (the position after the matching byte), or -1; base is where
    the chunk's hash starts at 0."""
    position = low
    while position < high:
        stop = min(high, position + _BLOCK)
        context = min(_CONTEXT, position - base)
        hashes = _hashes(data[position - context:stop])
        if context:
            hashes = hashes[_LANE * context:]
        found = _first_match(hashes, stop - position, long)
        if found >= 0:
            return position + found + 1
        position = stop
    return -1
