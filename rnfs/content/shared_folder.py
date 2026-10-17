"""The shared folder, <rnfs dir>/files/ (specification section 8). No RNS dependency.

Every top-level file and directory in it is imported as with `add` (copied into the block store) and
pinned with source "folder"; a changed or removed entry releases its old pin. Entries are recognised as
unchanged by a signature of path, size, modification time and execute bit (recursively for directories),
remembered across restarts (the state database's shared_folder table, rnfs.statedb: after a scan that changed
any, the entries changed are written durably), so unchanged entries aren't hashed again. Inside an entry that
changed, a regular file is not read again either if it was imported before and its device, inode, size,
modification time and execute bit are as they were (FolderFiles, audit D1): its CID is reused while every block of
it is still stored and none is denied, and its remembered content hash still goes through the deny list's check.
What the files were built as is kept in the shared_folder_files table, saved with the node's periodic saves and at
stop, not durably: a power loss costs at most reading some files again.

An entry that fails to import because of its content (the deny list, too large or too deep: an RnfsError) isn't
tried again until it changes; one that fails for a reason that may pass (a full or failing disk, a pin that couldn't
be saved, a file unreadable for now) is tried again RETRY_AFTER later, and so is one failing with a ValueError,
which the content doesn't cause (invalid names and special files inside an entry are skipped): a bug, such as a
BlockMismatch, logged with its traceback in the debug log rather than leaving the entry unshared until it changes.
A failure to pin an unchanged entry again is logged and the scan goes on.

The folder itself may be a symlink (the owner's setting), but it must not lead into the rnfs dir: a folder that is
the rnfs dir, a directory holding it, or one inside the subdirectories holding its keys and state (PRIVATE_SUBDIRS)
is refused, with a warning: it counts as empty, so nothing in it is imported and every folder pin is released
(audit CONT-3). The check is made on the folder as opened, which the scan then walks, so a link retargeted after
it can't redirect the scan. A folder below a directory other users can change, who could retarget it, is scanned
with a warning.
"""
import errno
import hashlib
import json
import os
import stat
import threading
import time
from collections.abc import Callable

from rnfs import constants, fsutil, statedb
from rnfs.content import builder, chunker, dag
from rnfs.content.builder import Built, FileState, display_path
from rnfs.content.cid import Cid
from rnfs.content.storage import FOLDER, Session, Storage
from rnfs.errors import RnfsError, StorageFull
from rnfs.log import LOG_INFO, LOG_WARNING, log


def import_entries(value: dict, tx) -> int:
    """Import shared_folder.json (statedb.migrate): its entries replace the shared_folder table; one that doesn't
    parse is skipped, as rnfs 0.1.4 did. Returns the number imported."""
    tx.execute("DELETE FROM shared_folder")
    count = 0
    for name, record in value.get("entries", {}).items():
        try:
            # Inside the try: a signature SQLite can't store (a lone surrogate) is an invalid record too
            tx.execute("INSERT OR REPLACE INTO shared_folder (name, signature, cid) VALUES (?, ?, ?)",
                       (statedb.dump(name), str(record[0]), Cid.parse(record[1]).binary))
        except (ValueError, TypeError, LookupError):
            continue
        count += 1
    return count


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
        fields = [relative.encode("utf-8", "surrogateescape").hex(), stat.S_IFMT(mode), st.st_size,
                  st.st_mtime_ns, builder.is_executable(mode)]
        if stat.S_ISREG(mode) and st.st_nlink > 1:
            fields.append("linked")  # skipped while linked elsewhere: imported once that link is gone
        digest.update(json.dumps(fields).encode())
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


RETRY_AFTER = 600.0  # seconds before an entry whose import failed for a reason that may pass is tried again
# The rnfs dir's subdirectories holding its keys (storage/identity, names/) and state: never scanned as the folder
PRIVATE_SUBDIRS = ("storage", "names", "state", "blocks", "lxmf")


def opened_path(handle: builder.Handle, st: os.stat_result, path: str) -> str:
    """The real path of the directory open as handle (from builder.open_path(path), whose stat is st): what the
    walk of handle reads, wherever path leads by now. Raises builder.Changed if path no longer leads to it where
    the system can't name an open directory, OSError if it can't be resolved.
    """
    if not isinstance(handle, int):
        return handle.root  # a path walk's root: its real path, checked against st as it was opened
    final = fsutil.final_path(handle)
    if final is not None:
        return final
    real = os.path.realpath(path)
    found = os.stat(real)
    if (found.st_dev, found.st_ino) != (st.st_dev, st.st_ino):
        raise builder.Changed(errno.ESTALE, "changed while being read", display_path(path))
    return real


def refused_reason(real: str, rnfs_dir: str) -> str | None:
    """Why a shared folder whose real path is real may not be scanned, or None: it is rnfs_dir, a directory
    holding it, or inside one of its PRIVATE_SUBDIRS (each resolved, as one may be a link to another disk)."""
    root = os.path.realpath(rnfs_dir)
    if fsutil.is_below(root, real):
        what = "the rnfs dir" if os.path.normcase(real) == os.path.normcase(root) else "a directory holding the rnfs dir"
        return f"it is {what}, {root}"
    for name in PRIVATE_SUBDIRS:
        private = os.path.realpath(os.path.join(root, name))
        if fsutil.is_below(real, private):
            return f"it is inside {private}, where rnfs keeps its keys and state"
    return None


def loose_ancestor(real: str) -> str | None:
    """The first directory above real (a real path) that other users could change, so as to swap what real is
    for something else, or None. POSIX only (None on Windows): one owned by another user than this one or root,
    or writable by group or others, unless it is sticky (as /tmp) and what is below it there is this user's or
    root's. Best effort: a directory that can't be read is passed over."""
    if fsutil.IS_WINDOWS:
        return None
    trusted = (0, os.geteuid())
    child, parent = real, os.path.dirname(real)
    while parent != child:
        try:
            st, below = os.lstat(parent), os.lstat(child)
        except OSError:
            return None
        sticky_safe = st.st_mode & stat.S_ISVTX and below.st_uid in trusted
        if st.st_uid not in trusted or (st.st_mode & 0o022 and not sticky_safe):
            return parent
        child, parent = parent, os.path.dirname(parent)
    return None


FILES_TABLE = "shared_folder_files"


def _parse_state(value) -> FileState:
    """A builder.file_state saved as a JSON list. Raises ValueError (or TypeError) if it isn't one."""
    if not (isinstance(value, list) and len(value) == 5 and isinstance(value[4], bool)
            and all(isinstance(n, int) and not isinstance(n, bool) for n in value[:4]) and value[2] >= 0):
        raise ValueError(f"not a file state: {value!r}")
    return value[0], value[1], value[2], value[3], value[4]


def _record(built: Built | None) -> tuple | None:
    """What a FolderFiles row holds of built (its size and kind come from the state), to tell a change."""
    return None if built is None else (built.cid, built.height, built.digest)


class FolderFiles:
    """The shared folder's per-file cache (audit D1): what each regular file imported from the folder was built as,
    by its builder.file_state, so the files of a changed entry that didn't change aren't read again.

    Kept per top-level entry: an entry imported again keeps only the files it holds now, and an entry gone takes its
    files with it. A file is found by its state whatever entry holds it, so one moved to another entry is reused
    too. Kept in state_dir's state database (FILES_TABLE; only in memory without one), as a node.Store: save()
    writes the rows changed since the last save (not durably), close() releases the database.
    """

    def __init__(self, state_dir: str | None = None):
        """The cache saved in state_dir (empty without one); a row that doesn't parse is logged and deleted."""
        self._lock = threading.Lock()  # held briefly, never during a build
        self._entries: dict[str, dict[FileState, Built]] = {}  # entry name -> its files' states and what they built
        self._index: dict[FileState, Built] = {}  # every entry's files, by state
        self._stale = False  # _index may hold files no entry holds any more (retain rebuilds it)
        self._rows: statedb.RowStore | None = None
        self._db = statedb.open_store(os.path.expanduser(state_dir), self._load) if state_dir else None

    def _load(self, db: statedb.StateDb):
        """Read FILES_TABLE."""
        # Saved with the node's batched saves and at stop, outside the lock, not durably: what a power loss takes
        # is read again
        self._rows = statedb.RowStore(db, FILES_TABLE, ("name", "state"), durable=False, mode=statedb.SNAPSHOT,
                                      lock=self._lock, row=self._row, key_values=self._key_values)
        bad = []
        for name_text, state_text, binary, height, digest in db.query(
                f"SELECT name, state, cid, height, digest FROM {FILES_TABLE}"):
            try:
                name, state = statedb.load(name_text), _parse_state(statedb.load(state_text))
                if not isinstance(name, str):
                    raise TypeError("not a string")
                if not (isinstance(height, int) and 0 <= height <= constants.MAX_DEPTH):
                    raise ValueError(f"height {height!r}")
                if digest is not None and not (isinstance(digest, bytes) and len(digest) == 32):
                    raise ValueError("not a SHA-256 digest")
                kind = dag.KIND_EXECUTABLE if state[4] else dag.KIND_FILE
                built = Built(Cid.from_bytes(binary), state[2], kind, height, digest)
            except (ValueError, TypeError):
                log(f"shared folder: ignoring invalid saved file {name_text!r} {state_text!r}", LOG_WARNING)
                bad.append((name_text, state_text))
                continue
            self._entries.setdefault(name, {})[state] = built
            self._index[state] = built
        statedb.delete_rows(db, FILES_TABLE, ("name", "state"), bad)

    @staticmethod
    def _key_values(key: tuple[str, FileState]) -> tuple[str, str]:
        """The name and state columns of a row's key (entry name, state)."""
        name, state = key
        return statedb.dump(name), statedb.dump(list(state))

    def _row(self, key: tuple[str, FileState]) -> tuple | None:
        """The row of key (entry name, state), or None if it is gone (lock held)."""
        name, state = key
        built = self._entries.get(name, {}).get(state)
        if built is None:
            return None
        return (*self._key_values(key), built.cid.binary, built.height, built.digest)

    def save(self):
        """Write the files changed since the last save. Raises OSError (StateDbError) if the database fails; they
        are written by the next save."""
        if self._rows is not None:
            self._rows.save()

    def close(self):
        """Release the state database (idempotent)."""
        if self._db is not None:
            self._db.close()

    def get(self, state: FileState) -> Built | None:
        """What the file in state was built as, if known."""
        with self._lock:
            return self._index.get(state)

    def update(self, name: str, found: dict[FileState, Built], complete: bool):
        """Record what an import of entry name found: every file it holds if complete (the import succeeded), else
        those the import got to, added to what was known (it is tried again, and they are still what they were)."""
        with self._lock:
            old = self._entries.get(name, {})
            self._set(name, dict(found) if complete else {**old, **found})
            self._index.update(found)

    def retain(self, names: set[str]):
        """Forget the files of the entries not in names (gone from the folder, or a folder refused), at the end of a
        scan."""
        with self._lock:
            for name in [name for name in self._entries if name not in names]:
                self._set(name, {})
            if self._stale:
                self._index = {state: built for files in self._entries.values() for state, built in files.items()}
                self._stale = False

    def _set(self, name: str, files: dict[FileState, Built]):
        """Make files entry name's (none: forget it), noting the rows that change. Lock held."""
        old = self._entries.get(name, {})
        if old.keys() - files.keys():
            self._stale = True
        if files:
            self._entries[name] = files
        else:
            self._entries.pop(name, None)
        if self._rows is not None:
            self._rows.changed(*((name, state) for state in old.keys() | files.keys()
                                 if _record(old.get(state)) != _record(files.get(state))))


class _EntryFiles:
    """One import's builder.FileCache, over the folder's FolderFiles.

    A file known by its state stands for reading it only while every block of it is stored (protected first, in the
    import's session, so a collection can't take them before the pin) and none is denied: such a file is read again,
    and the deny list refuses its blocks' put, failing the import as before. found collects what the import's files
    were built as, for FolderFiles.update.
    """

    def __init__(self, files: FolderFiles, storage: Storage, session: Session):
        """A cache for one import storing through session."""
        self.files = files
        self.storage = storage
        self.session = session
        self.found: dict[FileState, Built] = {}

    def lookup(self, st: os.stat_result, kind: int) -> Built | None:
        """builder.FileCache.lookup."""
        state = builder.file_state(st)
        known = self.files.get(state)
        if known is None or known.kind != kind:
            return None
        self.session.add(known.cid)
        if not self.storage.is_complete(known.cid):
            return None  # a block was evicted, collected or deleted as damaged: read again, which stores it
        denied = self.storage.deny.cids
        if denied and self.storage.denied_roots([known.cid], denied):
            return None
        self.found[state] = known
        return known

    def remember(self, st: os.stat_result, built: Built):
        """builder.FileCache.remember (a file whose execute bit and kind disagree, changed between listing and
        opening, isn't remembered)."""
        state = builder.file_state(st)
        if built.kind == (dag.KIND_EXECUTABLE if state[4] else dag.KIND_FILE):
            self.found[state] = built


class SharedFolder:
    """Keeps the shared folder's entries imported and pinned with source "folder"."""

    def __init__(self, directory: str, storage: Storage, state_dir: str | None = None,
                 clock: Callable[[], float] = time.monotonic, rnfs_dir: str | None = None):
        """Track directory (created if missing at the first scan); the signature cache lives in state_dir's
        state database (not kept without one). clock times the retries of failed imports. rnfs_dir is the rnfs
        dir the folder must not lead into (by default directory's parent: the folder is <rnfs dir>/files)."""
        self.directory = os.path.expanduser(directory)
        self.rnfs_dir = (os.path.expanduser(rnfs_dir) if rnfs_dir is not None
                         else os.path.dirname(os.path.normpath(os.path.abspath(self.directory))))
        self.storage = storage
        self.clock = clock
        self._db = statedb.connect(os.path.expanduser(state_dir)) if state_dir else None
        self._lock = threading.Lock()
        self._known: dict[str, tuple[str, Cid]] = {}  # entry name -> (signature, CID) of its last import
        self._saved: dict[str, tuple[str, Cid]] = {}  # what the shared_folder table holds
        # Entry name -> (signature that failed to import, when to try it again: None for not until it changes)
        self._failed: dict[str, tuple[str, float | None]] = {}
        self._warned: set[str] = set()  # skipped entries already warned about
        try:
            # What the files inside entries were built as (D1): a store of its own, which the node saves
            self.files = FolderFiles(state_dir)
            self._load()
        except BaseException:
            self.close()
            raise

    def _load(self):
        """Load the signature cache; a row that doesn't parse is logged and deleted."""
        if self._db is None:
            return
        bad = []
        for text, sig, binary in self._db.query("SELECT name, signature, cid FROM shared_folder"):
            try:
                name = statedb.load(text)
                if not isinstance(name, str) or not isinstance(sig, str):
                    raise TypeError("not a string")
                self._known[name] = (sig, Cid.from_bytes(binary))
            except (ValueError, TypeError):
                log(f"shared folder: ignoring invalid saved entry {text!r}", LOG_WARNING)
                bad.append((text,))
        self._saved = dict(self._known)
        if bad:
            with self._db.transaction() as tx:
                tx.executemany("DELETE FROM shared_folder WHERE name = ?", bad)

    def names(self) -> dict[Cid, str]:
        """CID → the shared-folder entry name it was imported from, for `pin ls`. Not locked, so it never waits
        for a scan's imports: after __init__, _known is only ever replaced whole, never changed in place."""
        known = self._known
        return {cid: name for name, (_, cid) in known.items()}

    def _save(self):
        """Persist the entries of the signature cache that differ from what the table holds, durably (lock held)."""
        if self._db is None:
            return
        changed = [(statedb.dump(name), sig, cid.binary) for name, (sig, cid) in self._known.items()
                   if self._saved.get(name) != (sig, cid)]
        gone = [(statedb.dump(name),) for name in self._saved if name not in self._known]
        with self._db.transaction(durable=True) as tx:
            tx.executemany("DELETE FROM shared_folder WHERE name = ?", gone)
            tx.executemany("INSERT OR REPLACE INTO shared_folder (name, signature, cid) VALUES (?, ?, ?)", changed)
        self._saved = dict(self._known)

    def close(self):
        """Release the state database, the per-file cache's too (the node calls this once it has saved everything
        at shutdown; the cache's changes are saved by files.save(), not here)."""
        files = getattr(self, "files", None)  # not made if __init__ failed first
        if files is not None:
            files.close()
        if self._db is not None:
            self._db.close()

    def scan(self, options: builder.BuildOptions | None = None) -> set[Cid]:
        """Import new and changed entries, release the pins of changed and removed ones; returns the folder's pins.

        options: the chunker and the deny list's file-hash check every import is built with (builder.BuildOptions;
        by default those of this context, builder.current_options)."""
        options = options if options is not None else builder.current_options()
        with self._lock:
            os.makedirs(self.directory, exist_ok=True)
            current: dict[str, tuple[str, Cid]] = {}
            # The folder itself is the owner's setting, so a symlink to it is followed; nothing inside it is
            folder, st = builder.open_path(self.directory)
            try:
                if not stat.S_ISDIR(st.st_mode):
                    raise NotADirectoryError(f"{display_path(self.directory)} is not a directory")
                # A refused folder counts as empty: every folder pin is released, so a node whose folder already
                # led into the rnfs dir (as 0.1.5 allowed) stops providing what it imported from there
                items = builder.list_directory(folder) if self._check_target(folder, st) else []
                for item in items:
                    result = self._scan_item(folder, item, options)
                    if result is not None:
                        current[item.name] = result
            finally:
                builder.close(folder)
            names = {item.name for item in items}
            self.files.retain(names)
            pins = {cid for _, cid in current.values()}
            for cid in self.storage.pinned_by(FOLDER) - pins:
                self.storage.unpin(cid, FOLDER)
            if current != self._known:
                self._known = current
                self._save()
            self._failed = {name: sig for name, sig in self._failed.items() if name in names}
            return pins

    def _check_target(self, folder: builder.Handle, st: os.stat_result) -> bool:
        """False, with a warning while it stays so, if the folder open as folder (whose stat is st) may not be
        scanned (refused_reason; scan then treats it as empty); a warning, once per directory, about a loose ancestor (loose_ancestor)."""
        real = opened_path(folder, st, self.directory)
        reason = refused_reason(real, self.rnfs_dir)
        if reason is not None:
            self._warn_once("target", f"not scanning {display_path(self.directory)}: {reason}. Its pins with "
                                      f"source folder were released; point it at another directory")
            return False
        self._warned.discard("target")  # so a folder refused again warns again
        loose = loose_ancestor(real)
        if loose is not None:
            self._warn_once(f"loose:{loose}", f"other users can change {display_path(loose)}, above "
                                              f"{display_path(real)}: they could replace the folder with another "
                                              f"directory, to have it shared. Keep every directory above it yours")
        return True

    def _warn_once(self, key: str, message: str):
        """Log a warning about a skipped entry (or the folder), once per entry name and reason while the daemon
        runs."""
        if key not in self._warned:
            self._warned.add(key)
            log(f"shared folder: {message}", LOG_WARNING)

    def _scan_item(self, folder: builder.Handle, item: os.DirEntry,
                   options: builder.BuildOptions) -> tuple[str, Cid] | None:
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
            # A file with another hard link is also a file elsewhere, which someone who may write here could
            # have linked in (audit L8-R2): skipped, so its pin goes too
            links = builder.hard_links(handle)
            if links:
                self._warn_once(f"hardlink:{item.name}", f"skipped {shown}: {builder.hard_link_reason(links)}")
                return None
            return self._import(item.name, handle, st, path, options)
        finally:
            builder.close(handle)

    def _import(self, name: str, handle: builder.Handle, st: os.stat_result, path: str,
                options: builder.BuildOptions) -> tuple[str, Cid] | None:
        """_scan_item for an entry open as handle, built with options."""
        shown = display_path(path)
        try:
            sig = open_signature(handle, st, path)
        except OSError:
            return None
        known = self._known.get(name)
        if known is not None and known[0] == sig and self.storage.is_complete(known[1]):
            try:
                self.storage.pin(known[1], FOLDER)  # no-op if already pinned
            except OSError as e:  # not saved (a full or failing disk): pinned again by the next scan
                log(f"shared folder: couldn't pin {shown}: {e}", LOG_WARNING)
            return known
        failed = self._failed.get(name)
        if failed is not None and failed[0] == sig and (failed[1] is None or self.clock() < failed[1]):
            return None
        warnings: list[str] = []
        files: _EntryFiles | None = None
        try:
            with self.storage.protect() as session:
                # What files were built as is reused only for the chunker it was built with (the folder's: fixed)
                if options.chunker == chunker.FIXED:
                    files = _EntryFiles(self.files, self.storage, session)
                with session.writer() as put:
                    built = builder.build_open(handle, st, path, put, warnings, skip_hard_links=True, options=options,
                                               cache=files)
                self.storage.pin(built.cid, FOLDER)
        except (OSError, StorageFull, ValueError) as e:
            # May pass (a full or failing disk, a pin not saved): tried again later. So is a ValueError (a
            # BlockMismatch too), which the content doesn't cause (see the module docstring): a bug, shown by type
            self._failed[name] = (sig, self.clock() + RETRY_AFTER)
            log(f"shared folder: couldn't import {shown}: {repr(e) if isinstance(e, ValueError) else e}", LOG_WARNING)
            self._keep_files(name, files, complete=False)
            return None
        except RnfsError as e:  # the content's: not until it changes
            self._failed[name] = (sig, None)
            log(f"shared folder: couldn't import {shown}: {e}", LOG_WARNING)
            self._keep_files(name, files, complete=False)
            return None
        self._keep_files(name, files, complete=True)
        for warning in warnings:
            log(f"shared folder: {warning}", LOG_WARNING)
        log(f"shared folder: imported {shown} as {built.cid}", LOG_INFO)
        return sig, built.cid

    def _keep_files(self, name: str, files: _EntryFiles | None, complete: bool):
        """Record in self.files what an import of entry name found its files built as (FolderFiles.update)."""
        if files is not None:
            self.files.update(name, files.found, complete)
