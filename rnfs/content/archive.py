"""Archives: moving content without a network, like IPFS CAR files (specification section 8). No RNS dependency
beyond its bundled msgpack.

An archive is a msgpack stream: ["rnfs-archive", 1, [root CID, ...]], then [cid, block] for every block of
every root's DAG, depth-first from each root, each block once. CIDs are binary.
"""
import struct
from typing import BinaryIO

from RNS.vendor import umsgpack

from rnfs.constants import ARCHIVE_MAGIC, ARCHIVE_VERSION, MAX_BLOCK_SIZE
from rnfs.content import walker
from rnfs.content.cid import Cid
from rnfs.content.storage import USER, Storage
from rnfs.log import LOG_WARNING, log

_MAX_CID_LENGTH = 64  # longer than any valid binary CID; read no further before rejecting one


def export_archive(roots: list, get: walker.Get, out: BinaryIO, max_entries: int | None = None) -> dict:
    """Write an archive of roots (CIDs, or Expects for targets inside a DAG) to out. Returns {"roots", "blocks"}.

    Every block comes from get and is checked as the DAG is walked; with max_entries, the DAGs are first
    checked against max_output_entries, counting every occurrence (TooLarge).
    """
    expects = [root if isinstance(root, walker.Expect) else walker.root_expect(root) for root in roots]
    if max_entries is not None:
        for expect in expects:
            walker.count_entries(expect, get, max_entries)
    out.write(umsgpack.packb([ARCHIVE_MAGIC, ARCHIVE_VERSION, [expect.cid.binary for expect in expects]]))
    written: set[Cid] = set()
    for expect in expects:
        for block_expect, block in walker.iter_dag(expect, get):
            if block_expect.cid not in written:
                written.add(block_expect.cid)
                out.write(umsgpack.packb([block_expect.cid.binary, block]))
    return {"roots": [expect.cid for expect in expects], "blocks": len(written)}


class _StreamReader:
    """Reads the msgpack forms an archive contains from a stream, with limits checked before reading."""

    def __init__(self, stream: BinaryIO):
        """Read from stream's current position."""
        self.stream = stream
        self._pending: int | None = None  # a type byte read by at_end()

    def _take(self, count: int) -> bytes:
        """Exactly count bytes. Raises ValueError if the stream ends first."""
        parts = []
        remaining = count
        while remaining > 0:
            data = self.stream.read(remaining)
            if not data:
                raise ValueError("archive is truncated")
            parts.append(data)
            remaining -= len(data)
        return b"".join(parts)

    def _unpack(self, size_format: str) -> int:
        """A big-endian unsigned integer of a struct format."""
        return struct.unpack(size_format, self._take(struct.calcsize(size_format)))[0]

    def at_end(self) -> bool:
        """True at the end of the stream; otherwise the next byte is kept for the next read."""
        data = self.stream.read(1)
        if not data:
            return True
        self._pending = data[0]
        return False

    def _tag(self) -> int:
        """The next type byte."""
        if self._pending is not None:
            tag, self._pending = self._pending, None
            return tag
        return self._take(1)[0]

    def array(self) -> int:
        """An array header; returns its length."""
        tag = self._tag()
        if 0x90 <= tag <= 0x9F:
            return tag & 0x0F
        if tag == 0xDC:
            return self._unpack(">H")
        if tag == 0xDD:
            return self._unpack(">I")
        raise ValueError("malformed archive: expected an array")

    def uint(self) -> int:
        """A non-negative integer."""
        tag = self._tag()
        if tag <= 0x7F:
            return tag
        formats = {0xCC: ">B", 0xCD: ">H", 0xCE: ">I", 0xCF: ">Q"}
        if tag not in formats:
            raise ValueError("malformed archive: expected an integer")
        return self._unpack(formats[tag])

    def text(self, limit: int) -> str:
        """A str of at most limit bytes."""
        tag = self._tag()
        if 0xA0 <= tag <= 0xBF:
            length = tag & 0x1F
        elif tag in (0xD9, 0xDA, 0xDB):
            length = self._unpack({0xD9: ">B", 0xDA: ">H", 0xDB: ">I"}[tag])
        else:
            raise ValueError("malformed archive: expected a string")
        if length > limit:
            raise ValueError("malformed archive: string too long")
        return self._take(length).decode("utf-8", "replace")

    def bin(self, limit: int) -> bytes:
        """A bin of at most limit bytes."""
        tag = self._tag()
        formats = {0xC4: ">B", 0xC5: ">H", 0xC6: ">I"}
        if tag not in formats:
            raise ValueError("malformed archive: expected bin")
        length = self._unpack(formats[tag])
        if length > limit:
            raise ValueError("malformed archive: field too long")
        return self._take(length)

    def cid(self) -> Cid:
        """A binary CID."""
        return Cid.from_bytes(self.bin(_MAX_CID_LENGTH))


def read_archive(stream: BinaryIO):
    """The archive's roots, and an iterator of its (cid, block) records as read, unverified.

    Raises ValueError if the stream isn't an rnfs archive or is malformed (the iterator raises it when reached).
    """
    reader = _StreamReader(stream)
    if reader.array() != 3 or reader.text(len(ARCHIVE_MAGIC)) != ARCHIVE_MAGIC:
        raise ValueError("not an rnfs archive")
    version = reader.uint()
    if version != ARCHIVE_VERSION:
        raise ValueError(f"unsupported archive version {version}")
    roots = [reader.cid() for _ in range(reader.array())]

    def records():
        """Each [cid, block] record until the end of the stream."""
        while not reader.at_end():
            if reader.array() != 2:
                raise ValueError("malformed archive: expected [cid, block]")
            cid = reader.cid()
            yield cid, reader.bin(MAX_BLOCK_SIZE)

    return roots, records()


def import_archive(stream: BinaryIO, storage: Storage, pin_source: str | None = USER) -> dict:
    """Import an archive into storage, verifying every block, and pin each complete root with pin_source.

    Returns {"roots": [Cid], "complete": [Cid], "incomplete": [Cid], "blocks": stored count, "bad": count of
    blocks that didn't match their CID (skipped)}. Raises ValueError for a stream that isn't an archive; blocks
    read before a malformed record are kept as cache.
    """
    roots, records = read_archive(stream)
    stored = bad = 0
    with storage.protect() as session:
        for root in roots:
            session.add(root)  # keeps blocks already stored below the roots until they're pinned
        for cid, block in records:
            try:
                session.put(cid, block)  # verifies
            except ValueError:
                bad += 1
                continue
            stored += 1
        complete, incomplete = [], []
        for root in roots:
            if storage.is_complete(root):
                complete.append(root)
                if pin_source is not None:
                    storage.pin(root, pin_source)
            else:
                incomplete.append(root)
    if bad:
        log(f"import: skipped {bad} blocks that didn't match their CIDs", LOG_WARNING)
    for root in incomplete:
        log(f"import: {root} is not complete in the archive", LOG_WARNING)
    return {"roots": roots, "complete": complete, "incomplete": incomplete, "blocks": stored, "bad": bad}
