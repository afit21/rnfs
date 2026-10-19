"""Blocks on disk: <blocks dir>/<xy>/<cid>, one file per CID (specification section 8). No RNS dependency.

Each block is in the subdirectory named by its CID's third- and second-to-last characters (shard_of): 1,024 of
them, lowercase base32 digits for both codecs. A CID's leading characters are the same for every CID of its codec,
and its last carries only 2 or 3 bits of the digest, so these two spread blocks evenly. One flat directory of
millions of files is slow to list, and FAT32 holds only about 10,900 such names per directory.

A block is verified against its CID before it's stored, and written to a temporary file in its subdirectory that
is synced to disk and then renamed, so a file under a CID name is never half-written, not even after a power loss.
The directories are synced by sync(), which Storage calls before saving state that refers to blocks: syncing on
every put would cost more than half of what remains of write speed, and a lost name is just a block not stored.
Not locked for reading: Storage serialises what matters, and a small lock guards the subdirectories' bookkeeping
for the writers that store blocks in parallel.

Opening the store lists blocks/ itself (a thousand subdirectories, not the blocks): partial files left there by
a crash are deleted, and blocks rnfs 0.1.6 and earlier stored directly in blocks/ are moved into their
subdirectories, then the directories synced. The move is resumable: a rename is atomic, every start moves what
is left, and a block already in its subdirectory makes the flat copy (the same content, by its CID) redundant,
so it is deleted. A block whose move fails (on Windows a scanner may hold it open) stays readable where it is
(flat) until a later start moves it. Partial files inside the subdirectories are deleted by scan(tidy=True),
which Storage runs when it lists every block after an unclean stop.

A subdirectory must be a real directory of this user's: one that is a symlink (or a Windows junction), not a
directory, or another user's stops the start (RnfsError), as node._make_private_dirs does for blocks/ itself, so
a link planted while blocks/ was writable by others can't redirect writes. A loose one is made 0700.

On Windows a file can't be deleted or replaced while a reader has it open, so a block already stored is
never rewritten (its content can't differ), and a delete that keeps meeting an open handle is deferred.
"""
import os
import stat
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterator

from rnfs import fsutil
from rnfs.constants import MAX_BLOCK_SIZE
from rnfs.content.cid import Cid
from rnfs.errors import BlockMismatch, RnfsError

PARTIAL_PREFIX = fsutil.PARTIAL_PREFIX  # also the prefix of every other atomic write's temporary file
SHARD_DIGITS = "abcdefghijklmnopqrstuvwxyz234567"  # the lowercase base32 digits a subdirectory's name is made of
# Subdirectories sync() syncs at once: a large add writes into most of the 1,024, and directory syncs that overlap
# share the filesystem's journal commits (one at a time took 3 ms each on an NVMe SSD, 16 at once a fifth of that)
SYNC_THREADS = 16


def matches(cid: Cid, block: bytes) -> bool:
    """True if block is a valid block for cid: at most MAX_BLOCK_SIZE bytes and hashing to cid's digest."""
    return len(block) <= MAX_BLOCK_SIZE and cid.verify(block)  # the size first, so an oversized block isn't hashed


def shard_of(name: str) -> str:
    """The subdirectory holding the block whose CID is written name: its third- and second-to-last characters."""
    return name[-3:-1]


def is_shard_name(name: str) -> bool:
    """True if name is a subdirectory's name: two base32 digits."""
    return len(name) == 2 and name[0] in SHARD_DIGITS and name[1] in SHARD_DIGITS


class BlockStore:
    """A directory of blocks named by their CIDs, in subdirectories."""

    def __init__(self, directory: str, report: Callable[[str], None] | None = None):
        """Open (creating if needed) the store at directory: delete the partial files a crash left in it, and move
        the blocks stored directly in it into their subdirectories (report(text) says how many, if any). Raises
        RnfsError for a subdirectory that is unsafe to use, OSError if the directory can't be listed."""
        self.directory = os.path.expanduser(directory)
        self._report = report
        self._lock = threading.Lock()  # guards _shards, _dirty and _top_dirty
        self._shards: set[str] = set()  # subdirectories known to be real directories of ours (checked or made here)
        self._dirty: set[str] = set()  # subdirectories blocks were renamed into since the last sync()
        # blocks/ itself changed since the last sync() (a subdirectory was made, or blocks moved out of it)
        self._top_dirty = False
        # Blocks left directly in blocks/ because their move failed: path() finds them there until a later start
        self._flat: set[Cid] = set()
        os.makedirs(self.directory, exist_ok=True)
        self._open()

    def _open(self):
        """List blocks/ itself: delete partial files, check the subdirectories, and move flat blocks into theirs."""
        flat = []
        with os.scandir(self.directory) as listing:
            for entry in listing:
                name = entry.name
                if name.startswith(PARTIAL_PREFIX):
                    fsutil.remove(entry.path)  # one still in use is tried at the next start
                elif is_shard_name(name):
                    self._check_shard(entry.path)
                    self._shards.add(name)
                else:
                    try:
                        cid = Cid.from_block_name(name)
                        if entry.is_file(follow_symlinks=False):
                            flat.append(cid)
                    except ValueError:
                        continue
        if not flat:
            return
        if self._report is not None:
            self._report(f"moving {len(flat)} block{'' if len(flat) == 1 else 's'} into subdirectories of "
                         f"{self.directory} (on a large store this takes a while, once)")
        for cid in flat:
            if not self._move_flat(cid):
                self._flat.add(cid)
        with self._lock:
            self._top_dirty = True
        self.sync()
        if self._flat and self._report is not None:
            self._report(f"{len(self._flat)} block{'' if len(self._flat) == 1 else 's'} couldn't be moved into "
                         f"subdirectories; they stay readable, and the next start tries again")

    def _move_flat(self, cid: Cid) -> bool:
        """Move the block for cid from blocks/ into its subdirectory, or delete it if one is there already (the same
        content); False if it stays where it is."""
        name = str(cid)
        source = os.path.join(self.directory, name)
        try:
            target = os.path.join(self._shard_dir(name), name)
            if os.path.lexists(target):
                if not os.path.isfile(target):
                    return False  # not a block: leave the flat one in use
                fsutil.remove(source)  # one in use stays, unused, and is deleted at a later start
                return True
            # Within one filesystem: atomic, so the block is in one place or the other whenever this stops. On
            # Windows a file in use can't be moved: tried for about a second, then left for a later start
            fsutil.replace(source, target)
        except OSError:
            return False
        with self._lock:
            self._dirty.add(shard_of(name))
        return True

    @staticmethod
    def _check_shard(path: str):
        """Check that the subdirectory at path is a real directory of this user's, made private (0700) if it is
        loose. Raises RnfsError otherwise."""
        try:
            info = os.lstat(path)
        except OSError as e:
            raise RnfsError(f"can't use {path}: {e}") from None
        if fsutil.is_link(info) or not stat.S_ISDIR(info.st_mode):
            what = "a link" if fsutil.is_link(info) else "not a directory"
            raise RnfsError(f"{path} is {what}; rnfs stores blocks only in real directories of its own there. Move "
                            f"it away (rnfs creates a new one)")
        if fsutil.IS_WINDOWS:
            return  # mode bits and owners mean nothing there; blocks/'s private access list is inherited below it
        # Checked again through a descriptor opened without following a link, so what is checked is what is changed
        try:
            fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | fsutil.O_NOFOLLOW)
        except OSError as e:
            raise RnfsError(f"can't use {path}: {e}") from None
        try:
            info = os.fstat(fd)
            if info.st_uid != os.geteuid():
                raise RnfsError(f"{path} belongs to another user (uid {info.st_uid}); rnfs stores blocks only in "
                                f"directories of its own there. Move it away, or as root give it to yourself "
                                f"(chown -R {os.geteuid()} {path})")
            if stat.S_IMODE(info.st_mode) & 0o077:
                os.fchmod(fd, 0o700)
        except OSError as e:
            raise RnfsError(f"can't make {path} private: {e}") from None
        finally:
            os.close(fd)

    def _shard_dir(self, name: str) -> str:
        """The subdirectory for the block whose CID is written name, made (0700) if missing. Raises RnfsError if
        something else is there, OSError if it can't be made."""
        shard = shard_of(name)
        path = os.path.join(self.directory, shard)
        if shard in self._shards:
            return path
        with self._lock:
            if shard not in self._shards:
                try:
                    os.mkdir(path, 0o700)
                    self._top_dirty = True
                except FileExistsError:
                    self._check_shard(path)  # not there when the store was opened: checked like those
                self._shards.add(shard)
        return path

    def path(self, cid: Cid) -> str:
        """Where the block for cid is (or would be) stored."""
        name = str(cid)
        if self._flat and cid in self._flat:
            return os.path.join(self.directory, name)
        return os.path.join(self.directory, shard_of(name), name)

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
        """Store block under cid unless already stored. Raises BlockMismatch (a ValueError), storing nothing, if
        it doesn't match."""
        if not matches(cid, block):
            if len(block) > MAX_BLOCK_SIZE:
                raise BlockMismatch(f"block for {cid} exceeds MAX_BLOCK_SIZE")
            raise BlockMismatch(f"block does not match {cid}")
        path = self.path(cid)
        if os.path.isfile(path):
            return  # content-addressed, so the same bytes; replacing them would fail on Windows while read
        name = str(cid)
        shard = self._shard_dir(name)
        fd, partial_path = tempfile.mkstemp(prefix=PARTIAL_PREFIX, dir=shard)
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
                self._remove_partial(partial_path)
                return
            with self._lock:
                self._dirty.add(shard_of(name))
        except BaseException:
            try:
                self._remove_partial(partial_path)
            except OSError:
                pass
            raise

    @staticmethod
    def _remove_partial(path: str):
        """Delete a partial file put() leaves; one Windows keeps in use is left for a later start."""
        fsutil.remove(path)

    def sync(self):
        """Make the names of the blocks stored so far survive a power loss: sync each subdirectory blocks were
        renamed into since the last sync, and blocks/ itself if it changed (a subdirectory made, blocks moved)."""
        with self._lock:
            # Taken first, so a put racing this is synced now or next time
            dirty, self._dirty = self._dirty, set()
            top, self._top_dirty = self._top_dirty, False
        try:
            paths = [os.path.join(self.directory, shard) for shard in dirty]
            if len(paths) > 1:
                with ThreadPoolExecutor(min(SYNC_THREADS, len(paths)), thread_name_prefix="rnfs-sync") as pool:
                    for _ in pool.map(fsutil.sync_directory, paths):  # raises the first error
                        pass
            elif paths:
                fsutil.sync_directory(paths[0])
            if top:
                fsutil.sync_directory(self.directory)
        except BaseException:
            with self._lock:  # synced again next time
                self._dirty |= dirty
                self._top_dirty |= top
            raise

    def delete(self, cid: Cid) -> int | None:
        """Delete the block; returns the bytes freed (0 if not stored), or None if Windows kept it in use."""
        path = self.path(cid)
        try:
            freed = os.path.getsize(path)
        except FileNotFoundError:
            return 0
        if not fsutil.remove(path):
            return None
        self._flat.discard(cid)  # stored again, it goes into its subdirectory
        return freed

    def verify(self, cid: Cid) -> bool:
        """True if the stored block exists and matches its CID."""
        block = self.get(cid)
        return block is not None and matches(cid, block)

    def scan(self, tidy: bool = False) -> Iterator[tuple[Cid, int]]:
        """(CID, size) of every stored block, listing every subdirectory. Files whose names aren't canonical CIDs,
        or that are in another block's subdirectory, are ignored, and links are never followed. With tidy (only
        while nothing is being stored: at the start), partial files found are deleted."""
        for cid in list(self._flat):
            size = self.size(cid)
            if size is not None:
                yield cid, size
        for shard in sorted(self._shards):
            directory = os.path.join(self.directory, shard)
            try:
                listing = os.scandir(directory)
            except OSError:
                continue  # gone meanwhile
            with listing:
                for entry in listing:
                    name = entry.name
                    if name.startswith(PARTIAL_PREFIX):
                        if tidy:
                            try:
                                self._remove_partial(entry.path)
                            except OSError:
                                pass  # left for a later start; the blocks are still listed
                        continue
                    try:
                        cid = Cid.from_block_name(name)
                        if shard_of(name) == shard and entry.is_file(follow_symlinks=False):
                            yield cid, entry.stat(follow_symlinks=False).st_size
                    except (ValueError, OSError):
                        continue
