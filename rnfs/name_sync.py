"""Name sync: keeping a name pointing at a folder's current content (`rnfs name sync`, specification section 9).

A synced folder is imported as the shared folder imports its entries (rnfs.content.shared_folder.FolderImporter),
the whole folder as one entry: an unchanged folder is recognised by its signature (names, sizes, modification times
and execute bits) and not read, and inside a changed one a file known by its state isn't read either (FolderFiles);
links, special files and files with other hard links are skipped; a folder that leads into the rnfs dir is refused.
Files are cut with FastCDC, so an edited file shares most of its blocks with the version before.

When the folder's root CID differs from the one the sync last published (and from the name's target), the name is
published to it with NameManager.publish, as `name publish` does: the root pinned with source name:<alias>, the
previous target's pin released, so only the latest version is kept. A publish waits until:
- a scan SETTLE_TIME or more after the new root was first seen finds it unchanged (the sync scans again then, if
  scan_interval is longer), so a folder still being copied into isn't published half-copied; and
- PUBLISH_INTERVAL has passed since the sync last published the name. RNS transport nodes count announces of a
  destination arriving less than an hour apart (RNS's announce_rate_target, by default 3600 s on transport
  instances), and after a burst of more than announce_rate_grace (5) of them stop rebroadcasting it until an hour
  after the last one let through: publishing faster would only spend airtime on announces that don't get through.

A folder that is missing, unreadable, refused, not a folder, empty (as an unmounted drive's mount point is) or
whose import fails pauses the sync: nothing is published, the reason is logged once and shown in the sync's state,
and the sync resumes by itself once the folder is back. A version that was imported but never published is pinned
by nothing: it is protected while its scan runs, and goes with the next collection (read again if needed).

State: the name_syncs table (alias, folder, the last import's signature and root, the root the sync last published
and when), written durably as it changes, so syncs resume after a restart; name_sync_files, the per-file cache
(FolderFiles, keyed by alias), saved with the node's batched saves. Thread-safe; scans run on the node's
rnfs-name-sync thread (run), one at a time.
"""
import os
import stat
import threading
import time
from dataclasses import dataclass

from rnfs import statedb
from rnfs.content import builder, chunker
from rnfs.content import shared_folder
from rnfs.content.builder import display_path
from rnfs.content.cid import Cid
from rnfs.content.shared_folder import FolderFiles, FolderImporter
from rnfs.errors import NotFound, RnfsError
from rnfs.log import LOG_NOTICE, LOG_WARNING, log
from rnfs.names import PRIVATE_NAME_WARNING, check_alias

SETTLE_TIME = 60.0  # seconds a changed folder's new root must stay the same before it is published
PUBLISH_INTERVAL = 3600.0  # seconds between two publishes of a name by its sync (RNS's announce_rate_target)
MAX_WAIT = 60.0  # the longest the sync thread sleeps, so a changed scan_interval is picked up
TABLE = "name_syncs"
FILES_TABLE = "name_sync_files"
LABEL = "name sync"

# A sync's state: not scanned yet; a new root waiting to settle; settled, waiting for PUBLISH_INTERVAL; the name
# points at what the folder holds; paused (reason says why)
NEW, SETTLING, WAITING, SYNCED, PAUSED = "new", "settling", "waiting", "synced", "paused"

# The root of an empty folder, which is never published: an unmounted drive's mount point looks like this
EMPTY_ROOT = builder.build_directory([], builder.NO_PUT).cid


@dataclass(eq=False)
class _Sync:
    """One synced name: what name_syncs holds, and how its last scans went (in memory only)."""

    path: str
    added: float
    signature: str | None = None  # the last import's
    root: Cid | None = None  # the last import's
    published: Cid | None = None  # the root this sync last published
    published_at: float | None = None  # when (Unix time)
    state: str = NEW
    reason: str | None = None  # why it is paused
    pending: Cid | None = None  # a root not yet published, and since when it was seen (monotonic)
    pending_since: float = 0.0
    scanned: float | None = None  # when it was last scanned (monotonic)
    unread: bool = False  # the last scan couldn't read the folder whole


def _check_folder(path) -> str:
    """path normalised, if it is an absolute path (a str); ValueError otherwise."""
    if not isinstance(path, str) or not os.path.isabs(path):
        raise ValueError(f"the folder must be an absolute path: {path!r}")
    return os.path.normpath(path)


class NameSync:
    """The node's synced names (see the module docstring)."""

    def __init__(self, names, storage, config, check=lambda: None, clock=time.time, monotonic=time.monotonic):
        """Syncs publishing through names (a NameManager), importing into storage; config gives scan_interval,
        the rnfs dir (root) the folders may not lead into and the state directory. check() gives the deny list's
        file-hash check for a scan's builds (node._hash_check). clock is the wall clock (publish times are
        persisted), monotonic times scans and settling."""
        self._names = names
        self._storage = storage
        self._config = config
        self._check = check
        self.clock = clock
        self.monotonic = monotonic
        self._lock = threading.Lock()  # the syncs and their rows; taken after _publish_lock, held briefly
        # Held from a scan's decision to publish to the publish's end, and by sync/unsync, so a sync stopped or
        # changed is never published after the call stopping it returns. Before the names lock
        self._publish_lock = threading.Lock()
        self._syncs: dict[str, _Sync] = {}
        self._warned_private: set[str] = set()  # aliases whose private-mode warning was logged this run
        self._wake = threading.Event()
        self._db = None
        self.files: FolderFiles | None = None
        try:
            # What the synced folders' files were built as (FastCDC), by alias: a store of its own
            self.files = FolderFiles(config.state_dir, table=FILES_TABLE, label=LABEL)
            self._importer = FolderImporter(storage, self.files, lambda: self.monotonic(), label=LABEL,
                                            files_chunker=chunker.FASTCDC)
            self._db = statedb.open_store(config.state_dir, self._load)
        except BaseException:
            self.close()
            raise

    def _load(self, db: statedb.StateDb):
        """Read the name_syncs table; a row that doesn't parse is logged and deleted."""
        bad = []
        for alias, path_text, signature, root, published, published_at, added in db.query(
                f"SELECT alias, path, signature, root, published, published_at, added FROM {TABLE}"):
            try:
                check_alias(alias)
                path = _check_folder(statedb.load(path_text))
                if signature is not None and not isinstance(signature, str):
                    raise TypeError("signature")
                if published_at is not None and not isinstance(published_at, (int, float)):
                    raise TypeError("published_at")
                self._syncs[alias] = _Sync(path, float(added), signature,
                                           Cid.from_bytes(root) if root is not None else None,
                                           Cid.from_bytes(published) if published is not None else None,
                                           None if published_at is None else float(published_at))
            except (ValueError, TypeError) as e:
                log(f"{LABEL}: ignoring invalid saved sync {alias!r}: {e}", LOG_WARNING)
                bad.append((alias,))
        statedb.delete_rows(db, TABLE, ("alias",), bad)

    def _write(self, alias: str):
        """Write alias's row as it is now (deleted if it is no longer synced), durably. Lock held."""
        entry = self._syncs.get(alias)
        with self._db.transaction(durable=True) as tx:
            if entry is None:
                tx.execute(f"DELETE FROM {TABLE} WHERE alias = ?", (alias,))
                return
            tx.execute(f"INSERT OR REPLACE INTO {TABLE} (alias, path, signature, root, published, published_at, added)"
                       " VALUES (?, ?, ?, ?, ?, ?, ?)",
                       (alias, statedb.dump(entry.path), entry.signature,
                        entry.root.binary if entry.root is not None else None,
                        entry.published.binary if entry.published is not None else None,
                        entry.published_at, entry.added))

    def save(self):
        """Write the per-file cache's changes (the syncs themselves are written as they change)."""
        if self.files is not None:
            self.files.save()

    def close(self):
        """Release the state database (idempotent), once save() ran."""
        if self.files is not None:
            self.files.close()
            self.files = None
        if self._db is not None:
            self._db.close()
            self._db = None

    # Commands

    def sync(self, alias: str, path: str) -> dict:
        """Keep alias's name pointing at the folder at path (absolute), from its next scan, which starts now; the
        sync's entry (entry()). Syncing a name again to another folder replaces its folder. NotFound if there is
        no such name or folder, ValueError if path isn't an absolute path to a folder that may be synced."""
        path = _check_folder(path)
        self._names.name(alias)  # NotFound if there is no such name
        reason = self._folder_problem(path)
        if reason is not None:
            raise (NotFound if reason == "missing" else ValueError)(f"can't sync {path}: {self._describe(reason)}")
        with self._publish_lock, self._lock:
            entry = self._syncs.get(alias)
            if entry is None or entry.path != path:
                # A new folder starts afresh, keeping what the sync published and when, for PUBLISH_INTERVAL
                previous = entry
                entry = _Sync(path, self.clock())
                if previous is not None:
                    entry.published, entry.published_at = previous.published, previous.published_at
                self._syncs[alias] = entry
                self._write(alias)
                log(f"{LABEL}: syncing {display_path(path)} to name {alias}", LOG_NOTICE)
            result = self._entry(alias, entry)
        if self._config.private:
            result["warning"] = PRIVATE_NAME_WARNING  # each publish's announce reaches outsiders (section 3)
        self._wake.set()
        return result

    def unsync(self, alias: str):
        """Stop syncing alias's name, which keeps pointing where it last did. NotFound if it isn't synced."""
        if not self.remove(alias):
            raise NotFound(f"name {alias} isn't synced to a folder")

    def remove(self, alias: str) -> bool:
        """Stop syncing alias's name if it is synced (`name unsync`, and `name rm`); True if it was."""
        with self._publish_lock, self._lock:
            entry = self._syncs.pop(alias, None)
            if entry is None:
                return False
            self._write(alias)
            self._warned_private.discard(alias)
        log(f"{LABEL}: stopped syncing {display_path(entry.path)} to name {alias}", LOG_NOTICE)
        self._wake.set()  # the thread forgets its files
        return True

    def list(self) -> list[dict]:
        """Every sync's entry, by alias."""
        with self._lock:
            return [self._entry(alias, entry) for alias, entry in sorted(self._syncs.items())]

    def _entry(self, alias: str, entry: _Sync) -> dict:
        """The API's form of a sync (lock held): {"alias", "path", "state", "reason", "root" (the folder's root as
        last imported), "published" (the root the sync last published), "published_at", "next_publish" (when the
        hourly limit next lets it publish, null if it doesn't hold it back)}."""
        next_publish = None
        if entry.published_at is not None and self.clock() - entry.published_at < PUBLISH_INTERVAL:
            next_publish = entry.published_at + PUBLISH_INTERVAL
        return {"alias": alias, "path": entry.path, "state": entry.state, "reason": entry.reason,
                "root": str(entry.root) if entry.root is not None else None,
                "published": str(entry.published) if entry.published is not None else None,
                "published_at": entry.published_at, "next_publish": next_publish}

    # Scanning

    def run(self, stop_event: threading.Event):
        """The sync loop, for a thread of its own: scans each sync when due, until stop_event is set."""
        while not stop_event.is_set():
            try:
                delay = self.tick()
            except Exception as e:  # noqa: BLE001 - the sync thread keeps running; logged
                log(f"{LABEL} failed: {e!r}", LOG_WARNING)
                delay = MAX_WAIT
            self._wake.wait(min(max(delay, 0.0), MAX_WAIT))
            self._wake.clear()

    def wake(self):
        """Make run() look at the syncs now (a sync started or stopped, or the node stopping)."""
        self._wake.set()

    def tick(self) -> float:
        """Scan every sync that is due (scan_interval after its last scan, or sooner to settle or once the hourly
        limit allows a publish); returns the seconds until the next is due."""
        with self._lock:
            aliases = set(self._syncs)
        self._importer.retain(aliases)
        if self.files is not None:
            self.files.retain(aliases)
        for alias in sorted(aliases):
            with self._lock:
                entry = self._syncs.get(alias)
                due = entry is not None and self._due(entry) <= self.monotonic()
            if due:
                self._scan(alias, entry)
        with self._lock:
            now = self.monotonic()
            return min((self._due(entry) - now for entry in self._syncs.values()), default=MAX_WAIT)

    def _due(self, entry: _Sync) -> float:
        """When entry is next to be scanned (monotonic; lock held)."""
        if entry.scanned is None:
            return float("-inf")
        due = entry.scanned + self._config.scan_interval
        if entry.state == SETTLING and entry.pending is not None:
            due = min(due, entry.pending_since + SETTLE_TIME)
        elif entry.state == WAITING and entry.published_at is not None:
            due = min(due, self.monotonic() + entry.published_at + PUBLISH_INTERVAL - self.clock())
        return due

    def _scan(self, alias: str, entry: _Sync):
        """Import entry's folder and publish alias's name to it if it is time to."""
        options = builder.BuildOptions(chunker.FASTCDC, check=self._check())
        with self._storage.protect() as session:  # the import's blocks, until it is published
            root, reason = self._import(alias, entry, options, session)
            target = self._target(alias)
            with self._publish_lock:
                with self._lock:
                    if self._syncs.get(alias) is not entry:
                        return  # stopped or changed meanwhile
                    entry.scanned = self.monotonic()
                    if target is False:
                        root, reason = None, f"there is no name {alias} (its key is gone, or can't be read)"
                    publish = self._decide(alias, entry, root, reason, target or None)
                if publish is not None:
                    self._publish(alias, entry, publish)

    def _target(self, alias: str) -> Cid | None | bool:
        """What alias's name points at now (None: nothing yet), or False if there is no such name (its key was
        deleted by hand, or can't be read)."""
        for name in self._names.list():
            if name["alias"] == alias:
                return Cid.parse(name["cid"]) if name["cid"] else None
        return False

    def _folder_problem(self, path: str) -> str | None:
        """Why the folder at path can't be synced now ("missing", or a reason for _describe), or None."""
        try:
            handle, st = builder.open_path(path)  # a link at the folder itself is followed, as the shared folder's
        except FileNotFoundError:
            return "missing"
        except OSError as e:
            return f"it can't be read ({e.strerror or e})"
        try:
            return self._opened_problem(handle, st, path)[0]
        finally:
            builder.close(handle)

    def _opened_problem(self, handle: builder.Handle, st, path: str) -> tuple[str | None, str | None]:
        """(why the folder open as handle can't be synced, or None; its real path, if found)."""
        if not stat.S_ISDIR(st.st_mode):
            return "it isn't a folder", None
        try:
            real = shared_folder.opened_path(handle, st, path)
        except OSError as e:
            return f"it can't be read ({e.strerror or e})", None
        reason = shared_folder.refused_reason(real, self._config.root)
        if reason is not None:
            return f"{reason}, where nothing may be shared from", real
        return None, real

    @staticmethod
    def _describe(reason: str) -> str:
        """reason as a log line or an error says it."""
        return "it doesn't exist" if reason == "missing" else reason

    def _import(self, alias: str, entry: _Sync, options: builder.BuildOptions,
                session) -> tuple[Cid | None, str | None]:
        """(root, None) of entry's folder, imported and protected in session; (None, why) if it can't be."""
        path = entry.path
        try:
            handle, st = builder.open_path(path)
        except FileNotFoundError:
            return None, "the folder is missing"
        except OSError as e:
            return None, f"the folder can't be read ({e.strerror or e})"
        try:
            problem, real = self._opened_problem(handle, st, path)
            if problem is not None:
                return None, f"the folder can't be synced: {problem}"
            loose = shared_folder.loose_ancestor(real)
            if loose is not None:
                self._importer.warn_once(f"loose:{alias}:{loose}", f"other users can change {display_path(loose)}, "
                                         f"above {display_path(real)}: they could replace the folder synced to "
                                         f"name {alias} with another. Keep every directory above it yours")
            known = (entry.signature, entry.root) if entry.signature is not None and entry.root is not None else None
            result = self._importer.import_entry(alias, handle, st, path, options, known, session.add)
        finally:
            builder.close(handle)
        if result is None:
            unread = self._importer.unread.get(alias)
            if unread is None:
                failed = self._importer.failed.get(alias)
                return None, f"the folder couldn't be imported: {failed[2] if failed is not None else 'unknown'}"
            # Changing while it was read (being copied into) is in flux, not paused: paused only if the next scan
            # can't read it whole either (part of it is unreadable)
            with self._lock:
                again, entry.unread = entry.unread, True
            return None, (f"part of the folder can't be read ({unread})" if again else None)
        entry.unread = False
        entry_signature, root = result
        with self._lock:
            if self._syncs.get(alias) is entry and (entry.signature, entry.root) != result:
                entry.signature, entry.root = entry_signature, root
                self._write(alias)
        return root, None

    def _decide(self, alias: str, entry: _Sync, root: Cid | None, reason: str | None,
                target: Cid | None) -> Cid | None:
        """After a scan found root (or why not; neither: the folder changed while it was read), entry's new state,
        and the root to publish now, if any (lock held)."""
        if root is None and reason is None:
            entry.state, entry.pending = SETTLING, None
            return None
        if root == EMPTY_ROOT:
            reason = "the folder is empty (as an unmounted drive's mount point is); an empty folder isn't published"
        if reason is not None:
            self._pause(alias, entry, reason)
            return None
        if entry.state == PAUSED:
            log(f"{LABEL}: {display_path(entry.path)} can be synced to name {alias} again", LOG_NOTICE)
        entry.reason = None
        if root in (entry.published, target):
            if entry.published != root:
                entry.published = root  # the name points at it already
                self._write(alias)
            entry.state, entry.pending = SYNCED, None
            return None
        now = self.monotonic()
        if entry.pending != root:
            entry.pending, entry.pending_since, entry.state = root, now, SETTLING
            return None
        if now - entry.pending_since < SETTLE_TIME:
            entry.state = SETTLING
            return None
        if entry.published_at is not None and 0 <= self.clock() - entry.published_at < PUBLISH_INTERVAL:
            entry.state = WAITING
            return None
        return root

    def _pause(self, alias: str, entry: _Sync, reason: str):
        """Pause entry for reason, logging it once while it stays the same (lock held)."""
        if entry.state != PAUSED or entry.reason != reason:
            log(f"{LABEL}: not publishing {display_path(entry.path)} to name {alias}: {reason}", LOG_WARNING)
        entry.state, entry.reason, entry.pending = PAUSED, reason, None

    def _publish(self, alias: str, entry: _Sync, root: Cid):
        """Publish alias's name to root, as `name publish` does (_publish_lock held)."""
        published_at = self.clock()
        try:
            result = self._names.publish(alias, root)
        except (OSError, RnfsError, ValueError) as e:  # a full or failing disk, the highest sequence: next scan
            with self._lock:
                self._pause(alias, entry, f"publishing failed: {e}")
            return
        with self._lock:
            entry.published, entry.published_at = root, published_at
            entry.state, entry.pending = SYNCED, None
            self._write(alias)
        log(f"{LABEL}: published {display_path(entry.path)} to name {alias} as {root}", LOG_NOTICE)
        if result.get("warning") and alias not in self._warned_private:
            # Private mode: once per sync and run, not at every publish
            self._warned_private.add(alias)
            log(f"{LABEL} {alias}: {result['warning']}", LOG_WARNING)
