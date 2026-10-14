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
from rnfs.content.storage import FOLDER, Storage, read_json, shaped, write_json
from rnfs.errors import RnfsError
from rnfs.log import LOG_INFO, LOG_WARNING, log

STATE_FILE = "shared_folder.json"


def signature(path: str) -> str:
    """A digest of what decides the content of the entry at path (not following a symlink there).

    Raises OSError if path can't be read, ValueError if it is neither a regular file nor a directory.
    """
    handle, st = builder.open_path(path, follow_symlinks=False)
    try:
        return open_signature(handle, st, path)
    finally:
        builder.close(handle)


def open_signature(handle: builder.Handle, st: os.stat_result, path: str) -> str:
    """A digest of names, types, sizes, mtimes and execute bits, recursively, of the entry open as handle.

    handle and st come from builder.open_path or open_child; directories are walked as builder walks them, so
    nothing outside the entry is read. Raises OSError if something can't be read or changes while it is.
    """
    digest = hashlib.sha256()

    def add(item: builder.Handle | None, item_path: str, relative: str, st: os.stat_result, depth: int):
        """Fold one item (and, for a directory, open as item, everything in it) into the digest."""
        mode = st.st_mode
        digest.update(json.dumps([relative.encode("utf-8", "surrogateescape").hex(), stat.S_IFMT(mode),
                                  st.st_size, st.st_mtime_ns, builder.is_executable(mode)]).encode())
        # Deeper than MAX_DEPTH can't be added anyway; don't recurse without bound
        if item is None or not stat.S_ISDIR(mode) or depth > constants.MAX_DEPTH + 1:
            return
        for child in builder.list_directory(item):
            child_path = os.path.join(item_path, child.name)
            child_st = builder.listed_stat(child, child_path)
            # A Windows junction stats as a directory, but add skips it like a symlink: don't walk into it
            if not stat.S_ISDIR(child_st.st_mode) or builder.is_link(child_st):
                add(None, child_path, relative + "/" + child.name, child_st, depth + 1)  # its stat is enough
                continue
            child_handle = builder.open_child(item, child.name, child_path, child_st)
            try:
                add(child_handle, child_path, relative + "/" + child.name, child_st, depth + 1)
            finally:
                builder.close(child_handle)

    add(handle, path, "", st, 1)
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
        value = read_json(self._state_path, shaped("entries", dict)) or {}
        for name, record in value.get("entries", {}).items():
            try:
                self._known[name] = (str(record[0]), Cid.parse(record[1]))
            except (ValueError, TypeError, LookupError):
                continue

    def names(self) -> dict[Cid, str]:
        """CID → the shared-folder entry name it was imported from, for `pin ls`."""
        with self._lock:
            return {cid: name for name, (_, cid) in self._known.items()}

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
            # The folder itself is the owner's setting, so a symlink to it is followed; nothing inside it is
            folder, st = builder.open_path(self.directory)
            try:
                if not stat.S_ISDIR(st.st_mode):
                    raise NotADirectoryError(f"{display_path(self.directory)} is not a directory")
                items = builder.list_directory(folder)
                for item in items:
                    result = self._scan_item(folder, item)
                    if result is not None:
                        current[item.name] = result
            finally:
                builder.close(folder)
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

    def _scan_item(self, folder: builder.Handle, item: os.DirEntry) -> tuple[str, Cid] | None:
        """(signature, CID) of a top-level entry, importing and pinning it if new or changed; None if skipped."""
        path = os.path.join(self.directory, item.name)
        shown = display_path(path)
        try:
            st = builder.listed_stat(item, path)
            if builder.is_link(st):
                self._warn_once(f"symlink:{item.name}", f"skipped symlink {shown}")
                return None
            if not (stat.S_ISDIR(st.st_mode) or stat.S_ISREG(st.st_mode)):
                self._warn_once(f"special:{item.name}", f"skipped {shown}: not a regular file or directory")
                return None
            # Signature and import both read this one open entry, so a swap after the check above can't
            # make either follow a symlink out of the folder or block on a FIFO
            handle = builder.open_child(folder, item.name, path, st)
        except OSError:
            return None  # deleted, swapped or unreadable mid-scan; it'll be picked up next time if it comes back
        try:
            return self._import(item.name, handle, st, path)
        finally:
            builder.close(handle)

    def _import(self, name: str, handle: builder.Handle, st: os.stat_result, path: str) -> tuple[str, Cid] | None:
        """_scan_item for an entry open as handle."""
        shown = display_path(path)
        try:
            sig = open_signature(handle, st, path)
        except OSError:
            return None
        known = self._known.get(name)
        if known is not None and known[0] == sig and self.storage.is_complete(known[1]):
            self.storage.pin(known[1], FOLDER)  # no-op if already pinned
            return known
        if self._failed.get(name) == sig:
            return None
        warnings: list[str] = []
        try:
            with self.storage.protect() as session:
                built = builder.build_open(handle, st, path, session.put, warnings)
                self.storage.pin(built.cid, FOLDER)
        except (OSError, ValueError, RnfsError) as e:
            self._failed[name] = sig
            log(f"shared folder: couldn't import {shown}: {e}", LOG_WARNING)
            return None
        for warning in warnings:
            log(f"shared folder: {warning}", LOG_WARNING)
        log(f"shared folder: imported {shown} as {built.cid}", LOG_INFO)
        return sig, built.cid
