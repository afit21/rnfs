"""Blocks on disk: <blocks dir>/<cid>, one file per CID (specification section 8). No RNS dependency.

A block is verified against its CID before it's stored, and written to a temporary file that is synced to
disk and then renamed, so a file under a CID name is never half-written, not even after a power loss. The
directory is synced by sync(), which Storage calls before saving state that refers to blocks: syncing it on
every put would cost more than half of what remains of write speed, and a lost name is just a block not
stored. Partial files left by a crash are deleted when the store is opened. Not locked: Storage serialises
access.

On Windows a file can't be deleted or replaced while a reader has it open, so a block already stored is
never rewritten (its content can't differ), and a delete that keeps meeting an open handle is deferred.
"""
import os
import tempfile
from typing import Iterator

from rnfs import fsutil
from rnfs.constants import MAX_BLOCK_SIZE
from rnfs.content.cid import Cid

PARTIAL_PREFIX = ".partial-"


class BlockStore:
    """A directory of blocks named by their CIDs."""

    def __init__(self, directory: str):
        """Open (creating if needed) the store at directory and delete partial files left by a crash."""
        self.directory = os.path.expanduser(directory)
        self._unsynced = False  # blocks were renamed into the directory since the last sync()
        os.makedirs(self.directory, exist_ok=True)
        for name in os.listdir(self.directory):
            if name.startswith(PARTIAL_PREFIX):
                fsutil.remove(os.path.join(self.directory, name))  # one still in use is tried at the next start

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
        """Store block under cid unless already stored. Raises ValueError, storing nothing, if it doesn't match."""
        if len(block) > MAX_BLOCK_SIZE:
            raise ValueError(f"block for {cid} exceeds MAX_BLOCK_SIZE")
        if not cid.verify(block):
            raise ValueError(f"block does not match {cid}")
        path = self.path(cid)
        if os.path.isfile(path):
            return  # content-addressed, so the same bytes; replacing them would fail on Windows while read
        fd, partial_path = tempfile.mkstemp(prefix=PARTIAL_PREFIX, dir=self.directory)
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(block)
                out.flush()
                os.fsync(out.fileno())  # else a power loss can leave the renamed file empty or short
            # Atomic, so readers never see a half-written block under a CID name
            try:
                fsutil.replace(partial_path, path)
            except PermissionError:
                # On Windows: a concurrent put stored it first and a reader holds it open. That one is as good
                if not os.path.isfile(path):
                    raise
                fsutil.remove(partial_path)
                return
            self._unsynced = True
        except BaseException:
            try:
                fsutil.remove(partial_path)
            except OSError:
                pass
            raise

    def sync(self):
        """Make the names of the blocks stored so far survive a power loss (sync the directory), if any are new."""
        if self._unsynced:
            self._unsynced = False  # first, so a put racing this is synced now or next time
            fsutil.sync_directory(self.directory)

    def delete(self, cid: Cid) -> int | None:
        """Delete the block; returns the bytes freed (0 if not stored), or None if Windows kept it in use."""
        path = self.path(cid)
        try:
            freed = os.path.getsize(path)
        except FileNotFoundError:
            return 0
        return freed if fsutil.remove(path) else None

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
