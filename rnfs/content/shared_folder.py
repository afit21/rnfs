"""The shared folder, <rnfs dir>/files/ (specification section 8). No RNS dependency.

Every top-level file and directory in it is imported as with `add` (copied into the block store) and
pinned with source "folder"; a changed or removed entry releases its old pin. Entries are recognised as
unchanged by a signature of path, size, modification time and execute bit (recursively for directories),
remembered across restarts, so unchanged entries aren't hashed again.
"""
import hashlib
import json
import os
import stat
import threading

from rnfs import constants
from rnfs.content import builder
from rnfs.content.builder import display_path
from rnfs.content.cid import Cid
from rnfs.content.storage import FOLDER, Storage, read_json, write_json
from rnfs.errors import RnfsError
from rnfs.log import LOG_INFO, LOG_WARNING, log

STATE_FILE = "shared_folder.json"


def signature(path: str) -> str:
    """A digest of what decides path's content: names, types, sizes, mtimes and execute bits, recursively.

    Raises OSError if path can't be read.
    """
    digest = hashlib.sha256()

    def add(item_path: str, relative: str, st: os.stat_result, depth: int):
        """Fold one item (and, for a directory, everything in it) into the digest."""
        mode = st.st_mode
        digest.update(json.dumps([relative.encode("utf-8", "surrogateescape").hex(), stat.S_IFMT(mode),
                                  st.st_size, st.st_mtime_ns, bool(mode & stat.S_IXUSR)]).encode())
        # Deeper than MAX_DEPTH can't be added anyway; don't recurse without bound
        if stat.S_ISDIR(mode) and depth <= constants.MAX_DEPTH + 1:
            with os.scandir(item_path) as listing:
                children = sorted(listing, key=lambda entry: entry.name)
            for child in children:
                add(child.path, relative + "/" + child.name, child.stat(follow_symlinks=False), depth + 1)

    add(path, "", os.stat(path, follow_symlinks=False), 1)
    return digest.hexdigest()


class SharedFolder:
    """Keeps the shared folder's entries imported and pinned with source "folder"."""

    def __init__(self, directory: str, storage: Storage, state_dir: str | None = None):
        """Track directory (created if missing at the first scan); the signature cache lives in state_dir."""
        self.directory = os.path.expanduser(directory)
        self.storage = storage
        self._state_path = os.path.join(os.path.expanduser(state_dir), STATE_FILE) if state_dir else None
        self._lock = threading.Lock()
        self._known: dict[str, tuple[str, Cid]] = {}  # entry name -> (signature, CID) of its last import
        self._failed: dict[str, str] = {}  # entry name -> signature that failed to import, not retried until it changes
        self._warned: set[str] = set()  # skipped entries already warned about
        self._load()

    def _load(self):
        """Load the signature cache, ignoring anything that doesn't parse."""
        if self._state_path is None:
            return
        value = read_json(self._state_path) or {}
        for name, record in value.get("entries", {}).items():
            try:
                self._known[name] = (str(record[0]), Cid.parse(record[1]))
            except (ValueError, TypeError, IndexError):
                continue

    def _save(self):
        """Persist the signature cache."""
        if self._state_path is None:
            return
        os.makedirs(os.path.dirname(self._state_path), exist_ok=True)
        write_json(self._state_path, {"entries": {name: [sig, str(cid)] for name, (sig, cid) in self._known.items()}})

    def scan(self) -> set[Cid]:
        """Import new and changed entries, release the pins of changed and removed ones; returns the folder's pins."""
        with self._lock:
            os.makedirs(self.directory, exist_ok=True)
            current: dict[str, tuple[str, Cid]] = {}
            with os.scandir(self.directory) as listing:
                items = sorted(listing, key=lambda entry: entry.name)
            for item in items:
                result = self._scan_item(item)
                if result is not None:
                    current[item.name] = result
            pins = {cid for _, cid in current.values()}
            for cid in self.storage.pinned_by(FOLDER) - pins:
                self.storage.unpin(cid, FOLDER)
            if current != self._known:
                self._known = current
                self._save()
            names = {item.name for item in items}
            self._failed = {name: sig for name, sig in self._failed.items() if name in names}
            return pins

    def _warn_once(self, key: str, message: str):
        """Log a warning about a skipped entry, once until it changes."""
        if key not in self._warned:
            self._warned.add(key)
            log(f"shared folder: {message}", LOG_WARNING)

    def _scan_item(self, item: os.DirEntry) -> tuple[str, Cid] | None:
        """(signature, CID) of a top-level entry, importing and pinning it if new or changed; None if skipped."""
        shown = display_path(item.path)
        try:
            if item.is_symlink():
                self._warn_once(f"symlink:{item.name}", f"skipped symlink {shown}")
                return None
            if not (item.is_dir(follow_symlinks=False) or item.is_file(follow_symlinks=False)):
                self._warn_once(f"special:{item.name}", f"skipped {shown}: not a regular file or directory")
                return None
            sig = signature(item.path)
        except OSError:
            return None  # deleted or unreadable mid-scan; it'll be picked up next time if it comes back
        known = self._known.get(item.name)
        if known is not None and known[0] == sig and self.storage.is_complete(known[1]):
            self.storage.pin(known[1], FOLDER)  # no-op if already pinned
            return known
        if self._failed.get(item.name) == sig:
            return None
        try:
            with self.storage.protect() as session:
                built, warnings = builder.add_path(item.path, session.put)
                self.storage.pin(built.cid, FOLDER)
        except (OSError, ValueError, RnfsError) as e:
            self._failed[item.name] = sig
            log(f"shared folder: couldn't import {shown}: {e}", LOG_WARNING)
            return None
        for warning in warnings:
            log(f"shared folder: {warning}", LOG_WARNING)
        log(f"shared folder: imported {shown} as {built.cid}", LOG_INFO)
        return sig, built.cid
