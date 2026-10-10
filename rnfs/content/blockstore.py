"""Blocks on disk: <blocks dir>/<cid>, one file per CID (specification section 8). No RNS dependency.

A block is verified against its CID before it's stored, and written to a temporary file that is then
renamed, so a file under a CID name is never half-written. Partial files left by a crash are deleted
when the store is opened. Not locked: Storage serialises access.
"""
import os
import tempfile
from typing import Iterator

from rnfs.constants import MAX_BLOCK_SIZE
from rnfs.content.cid import Cid

PARTIAL_PREFIX = ".partial-"


class BlockStore:
    """A directory of blocks named by their CIDs."""

    def __init__(self, directory: str):
        """Open (creating if needed) the store at directory and delete partial files left by a crash."""
        self.directory = os.path.expanduser(directory)
        os.makedirs(self.directory, exist_ok=True)
        for name in os.listdir(self.directory):
            if name.startswith(PARTIAL_PREFIX):
                try:
                    os.remove(os.path.join(self.directory, name))
                except FileNotFoundError:
                    pass

    def path(self, cid: Cid) -> str:
        """Where the block for cid is (or would be) stored."""
        return os.path.join(self.directory, str(cid))

    def has(self, cid: Cid) -> bool:
        """True if the block is stored."""
        return os.path.isfile(self.path(cid))

    def get(self, cid: Cid) -> bytes | None:
        """The stored block, or None if it isn't stored. Not re-verified: see verify()."""
        try:
            with open(self.path(cid), "rb") as f:
                return f.read(MAX_BLOCK_SIZE + 1)
        except FileNotFoundError:
            return None

    def size(self, cid: Cid) -> int | None:
        """Length of the stored block, or None if it isn't stored."""
        try:
            return os.path.getsize(self.path(cid))
        except FileNotFoundError:
            return None

    def put(self, cid: Cid, block: bytes):
        """Store block under cid. Raises ValueError, storing nothing, if it doesn't match or is too large."""
        if len(block) > MAX_BLOCK_SIZE:
            raise ValueError(f"block for {cid} exceeds MAX_BLOCK_SIZE")
        if not cid.verify(block):
            raise ValueError(f"block does not match {cid}")
        fd, partial_path = tempfile.mkstemp(prefix=PARTIAL_PREFIX, dir=self.directory)
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(block)
            # Atomic, so readers never see a half-written block under a CID name
            os.replace(partial_path, self.path(cid))
        except BaseException:
            try:
                os.remove(partial_path)
            except FileNotFoundError:
                pass
            raise

    def delete(self, cid: Cid) -> int:
        """Delete the block; returns the bytes freed (0 if it wasn't stored)."""
        path = self.path(cid)
        try:
            freed = os.path.getsize(path)
            os.remove(path)
            return freed
        except FileNotFoundError:
            return 0

    def verify(self, cid: Cid) -> bool:
        """True if the stored block exists and matches its CID."""
        block = self.get(cid)
        return block is not None and len(block) <= MAX_BLOCK_SIZE and cid.verify(block)

    def scan(self) -> Iterator[tuple[Cid, int]]:
        """(CID, size) of every stored block. Files whose names aren't canonical CIDs are ignored."""
        with os.scandir(self.directory) as listing:
            for entry in listing:
                if entry.name.startswith(PARTIAL_PREFIX):
                    continue
                try:
                    cid = Cid.parse(entry.name)
                    if entry.is_file(follow_symlinks=False):
                        yield cid, entry.stat(follow_symlinks=False).st_size
                except (ValueError, OSError):
                    continue
