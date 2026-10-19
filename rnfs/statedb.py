"""The state database: everything rnfs persists under <rnfs dir>/state/, in one SQLite file, rnfs.db.

- Opened through the standard sqlite3 module, in WAL mode, readable by its owner only (rnfs.db is created 0600, and
  SQLite gives its -wal and -shm files the database's mode; on Windows the state directory's access list,
  winacl, covers all three).
- One connection per directory per process, shared by every store through connect(), which hands each caller its
  own StateDb reference; the last close() closes the connection. One writer at a time is all SQLite allows
  anyway, and every load happens at startup, so more connections would buy nothing but SQLITE_BUSY waits.
- transaction() is the only way to write. Its lock is the innermost lock of all (design.md, Threads): a store
  takes every lock of its own first, prepares its rows, then writes them, and takes no other lock and does no
  network I/O inside. durable=True commits with synchronous=FULL (on disk when the call returns: pins, names,
  jobs and the other stores saved on each change), False with NORMAL (stores saved periodically: a power loss
  may lose the last commits, never part of one, and never a commit without every earlier one).
- The schema is a list of steps, SCHEMA; PRAGMA user_version counts those applied. A database from a newer rnfs
  (more steps than this code knows) or one failing PRAGMA quick_check stops startup, and is left as it is.
- At every connect(), migrate() imports the JSON state files earlier versions wrote (LEGACY), each in one
  transaction recorded in the migrated table, then renames it to <name>.migrated. A crash between the commit and
  the rename leaves the record, so the next start only renames the file: nothing is imported twice or by half.

Stores keep their state in memory and read the database only when they load (open_store), and write what changed
through a RowStore: it keeps the keys changed and not yet written, writes them at once (WRITE_THROUGH) or at the
store's saves (SNAPSHOT, outside the store's lock, one save at a time), and keeps them for the next write when one
fails. reconcile_pins makes a feature's pins match its saved state at start. Text that may hold lone
surrogates (file names, remote strings) is stored through dump() and load(); sequences, which may not fit
SQLite's signed 64-bit integers, as decimal text.
"""
import importlib
import json
import os
import stat
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from rnfs import fsutil
from rnfs.errors import RnfsError
from rnfs.log import LOG_ERROR, LOG_NOTICE, log, plural

DB_FILE = "rnfs.db"
BUSY_TIMEOUT = 10.0  # seconds a commit waits for another process's (two daemons on one directory)
JOURNAL_SIZE_LIMIT = 4 * 1024 * 1024  # bytes the WAL file is truncated to after a checkpoint
MIGRATED_SUFFIX = ".migrated"

# Each step brings PRAGMA user_version from its index to its index + 1, in one transaction. Steps are only ever
# appended: a store whose layout changes gets a new step (ALTER TABLE, or a new table and an INSERT ... SELECT).
# The steps released in 0.1.5 (the first 16) are pinned by a digest in tests/test_statedb.py: a database 0.1.5
# wrote has them already, so an edit there would never reach it. A step's item is an SQL statement, or a callable
# taking the transaction's connection, for a migration SQL alone can't express.
Step = str | Callable[[object], None]
SCHEMA: tuple[tuple[str, tuple[Step, ...]], ...] = (
    ("statedb", (
        # JSON state files imported, so each is imported once (migrate)
        "CREATE TABLE migrated (file TEXT PRIMARY KEY, records INTEGER NOT NULL, at REAL NOT NULL)",
        # Small per-store values: (store, key) -> value
        "CREATE TABLE meta (store TEXT NOT NULL, key TEXT NOT NULL, value, PRIMARY KEY (store, key)) WITHOUT ROWID",
    )),
    ("storage", (
        # One row per pin source: pinning is an INSERT, releasing a DELETE
        "CREATE TABLE pins (cid BLOB NOT NULL, source TEXT NOT NULL, PRIMARY KEY (cid, source)) WITHOUT ROWID",
        # Cached fetches; path is the dump()ed list of names, used orders them (least recently used lowest)
        "CREATE TABLE cache (root BLOB NOT NULL, path TEXT NOT NULL, target BLOB NOT NULL, used INTEGER NOT NULL,"
        " PRIMARY KEY (root, path)) WITHOUT ROWID",
    )),
    ("pin_names", (
        "CREATE TABLE pin_names (cid BLOB PRIMARY KEY, name TEXT NOT NULL) WITHOUT ROWID",  # name: dump()ed
    )),
    ("node", (
        # Targets fetched for the user (the roots strategy)
        "CREATE TABLE fetched (cid BLOB PRIMARY KEY) WITHOUT ROWID",
        # Remote pins accepted and not yet fetched, in the order accepted (position)
        "CREATE TABLE remote_pins (position INTEGER PRIMARY KEY, source TEXT NOT NULL, cid BLOB NOT NULL)",
    )),
    ("fetcher", (
        # Roots found invalid, oldest (lowest position) first
        "CREATE TABLE invalid (cid BLOB PRIMARY KEY, position INTEGER NOT NULL) WITHOUT ROWID",
    )),
    ("publisher", (
        # item: str() of a provided Cid or DhtKey, as published.json keyed it; nodes: their IDs concatenated
        "CREATE TABLE published (item TEXT PRIMARY KEY, at REAL NOT NULL, nodes BLOB NOT NULL) WITHOUT ROWID",
        "CREATE TABLE published_earlier (item TEXT NOT NULL, node BLOB NOT NULL, at REAL NOT NULL,"
        " PRIMARY KEY (item, node)) WITHOUT ROWID",
        "CREATE TABLE withdrawals (node BLOB NOT NULL, key BLOB NOT NULL, PRIMARY KEY (node, key)) WITHOUT ROWID",
    )),
    ("providers", (
        "CREATE TABLE provider_publishers (publisher BLOB PRIMARY KEY, first_seen REAL NOT NULL) WITHOUT ROWID",
        "CREATE TABLE provider_records (key BLOB NOT NULL, publisher BLOB NOT NULL, first_stored REAL NOT NULL,"
        " last_stored REAL NOT NULL, pooled INTEGER NOT NULL, PRIMARY KEY (key, publisher)) WITHOUT ROWID",
        "CREATE TABLE provider_departed (publisher BLOB PRIMARY KEY, first_seen REAL NOT NULL, at REAL NOT NULL)"
        " WITHOUT ROWID",
        "CREATE TABLE provider_evicted (publisher BLOB PRIMARY KEY, first_seen REAL NOT NULL, at REAL NOT NULL)"
        " WITHOUT ROWID",
    )),
    ("names", (
        # Names published here, by alias; sequences are decimal text (up to 2^64 - 1, past SQLite's integers)
        "CREATE TABLE names_published (alias TEXT PRIMARY KEY, sequence TEXT NOT NULL, cid BLOB,"
        " imported INTEGER NOT NULL) WITHOUT ROWID",
        "CREATE TABLE names_held (name BLOB PRIMARY KEY, sequence TEXT NOT NULL, cid BLOB NOT NULL,"
        " heard REAL NOT NULL) WITHOUT ROWID",
        # Highest sequence of names resolved without persist; used orders them (least recently resolved lowest)
        "CREATE TABLE name_floor (name BLOB PRIMARY KEY, sequence TEXT NOT NULL, resolved REAL NOT NULL,"
        " used INTEGER NOT NULL) WITHOUT ROWID",
    )),
    ("shared_folder", (
        # name: the dump()ed entry name (a file name, which may not be valid Unicode)
        "CREATE TABLE shared_folder (name TEXT PRIMARY KEY, signature TEXT NOT NULL, cid BLOB NOT NULL)"
        " WITHOUT ROWID",
    )),
    ("jobs", (
        # record: the dump()ed Job.to_record(); position keeps creation order
        "CREATE TABLE jobs (id TEXT PRIMARY KEY, position INTEGER NOT NULL, record TEXT NOT NULL) WITHOUT ROWID",
    )),
    ("keepers", (
        # reasons: the dump()ed sorted list; record: the dump()ed list of the record's fields in hex, or NULL
        "CREATE TABLE keepers (name BLOB PRIMARY KEY, reasons TEXT NOT NULL, record TEXT) WITHOUT ROWID",
    )),
    ("follow", (
        "CREATE TABLE follow (name BLOB PRIMARY KEY, pinned BLOB) WITHOUT ROWID",
    )),
    ("contacts", (
        "CREATE TABLE contacts (nick TEXT PRIMARY KEY, kind TEXT NOT NULL, id BLOB NOT NULL) WITHOUT ROWID",
    )),
    ("lxmf", (
        # box: "inbox" or "outbox"; record: the dump()ed message record (its id is the id column)
        "CREATE TABLE lxmf_messages (box TEXT NOT NULL, id INTEGER NOT NULL, record TEXT NOT NULL,"
        " PRIMARY KEY (box, id)) WITHOUT ROWID",
    )),
    ("denylist", (
        # entry: canonical text (a CID's written form, or "sha256:" and 64 lowercase hex digits); ASCII
        "CREATE TABLE deny_entries (entry TEXT PRIMARY KEY, added REAL NOT NULL) WITHOUT ROWID",
        # source: a deny_lists setting as written (dump()ed); cid: the list version it last accepted
        "CREATE TABLE deny_lists (source TEXT PRIMARY KEY, cid BLOB NOT NULL, accepted REAL NOT NULL) WITHOUT ROWID",
    )),
    ("denylist_hashes", (
        # Files' content SHA-256 (digest) by binary CID; used orders them (least recently used lowest)
        "CREATE TABLE deny_hashes (cid BLOB PRIMARY KEY, digest BLOB NOT NULL, used INTEGER NOT NULL) WITHOUT ROWID",
    )),
    ("shared_folder_files", (
        # What each file in the shared folder was built as (rnfs 0.1.6). name: the dump()ed top-level entry holding
        # it; state: the dump()ed [device, inode, size, mtime_ns, execute bit] (text: an inode may not fit SQLite's
        # integers); digest: its content SHA-256, NULL if it was built without one
        "CREATE TABLE shared_folder_files (name TEXT NOT NULL, state TEXT NOT NULL, cid BLOB NOT NULL,"
        " height INTEGER NOT NULL, digest BLOB, PRIMARY KEY (name, state)) WITHOUT ROWID",
    )),
    ("pin_details", (
        # When each pinned CID was first pinned (Unix time; rnfs 0.2.0), by any source; a pin made by an earlier
        # version has no row. A table of its own: the released tables stay exactly as 0.1.5 made them
        "CREATE TABLE pin_times (cid BLOB PRIMARY KEY, added REAL NOT NULL) WITHOUT ROWID",
        # A label the user gave a pin (pin label), kept apart from where it was added from (pin_names); dump()ed
        "CREATE TABLE pin_labels (cid BLOB PRIMARY KEY, label TEXT NOT NULL) WITHOUT ROWID",
    )),
    ("routing", (
        # The routing table (rnfs 0.2.0; rnfs.dht.routing_state), one row per node: state 0 in the table (first_seen,
        # at = last heard, failures = failed queries since), 1 dropped with its seniority kept (first_seen, at = when
        # dropped), 2 banned (at = when the ban ends). Times are Unix time
        "CREATE TABLE routing (node BLOB PRIMARY KEY, state INTEGER NOT NULL, first_seen REAL, at REAL NOT NULL,"
        " failures INTEGER NOT NULL) WITHOUT ROWID",
    )),
    ("scores", (
        # Local scores (rnfs 0.2.0; rnfs.dht.scores): a peer's score as of updated, when it was last raised (NULL:
        # never) and when it was seeded as a bootstrap node (NULL: not). Times are Unix time
        "CREATE TABLE peer_scores (node BLOB PRIMARY KEY, score REAL NOT NULL, updated REAL NOT NULL, useful REAL,"
        " seeded REAL) WITHOUT ROWID",
    )),
)


class StateDbError(OSError):
    """A state database read or write failed (a full or failing disk, a closed database). An OSError, so callers
    that handle a failed save of a state file handle this the same way."""


# Text columns


def dump(value) -> str:
    """value as JSON text for a TEXT column: non-ASCII characters as is, a lone surrogate (from a file name that
    isn't UTF-8, or a remote string) as its JSON escape, which SQLite can store and load() turns back into it."""
    return json.dumps(value, ensure_ascii=False).encode("utf-8", "backslashreplace").decode("utf-8")


def load(text: str):
    """The value dump() wrote. Raises ValueError (or TypeError) if text isn't JSON."""
    return json.loads(text)


def set_meta(tx, store: str, key: str, value):
    """Set store's meta value key inside transaction tx (an integer, real, text or bytes)."""
    tx.execute("INSERT OR REPLACE INTO meta (store, key, value) VALUES (?, ?, ?)", (store, key, value))


# The database


def _sqlite3():
    """The sqlite3 module. Raises RnfsError if this Python was built without it."""
    try:
        import sqlite3
    except ImportError as e:
        raise RnfsError(f"this Python has no SQLite support ({e}); rnfs keeps its state in an SQLite database, "
                        "so it needs a Python built with the sqlite3 module (_sqlite3)") from None
    return sqlite3


class _Database:
    """One open state database, shared by every StateDb reference to it in this process."""

    def __init__(self, state_dir: str):
        """Open (creating it if missing) and check the database in state_dir, and bring its schema up to date.
        Raises RnfsError if it can't be opened, is damaged or is newer than this code."""
        self.sqlite3 = _sqlite3()
        self.state_dir = state_dir
        self.path = os.path.join(state_dir, DB_FILE)
        self.lock = threading.Lock()  # innermost: held for each transaction and read, never while taking another
        self._owner: int | None = None  # the thread inside a transaction
        self.migrate_lock = threading.Lock()  # one migrate() at a time, so no file is imported twice
        self.references = 0
        self.synchronous: str | None = None
        _create_private(self.path)
        try:
            self.conn = self.sqlite3.connect(self.path, timeout=BUSY_TIMEOUT, isolation_level=None,
                                             check_same_thread=False)
        except self.sqlite3.Error as e:
            raise RnfsError(f"could not open the state database {self.path}: {e}") from None
        try:
            self._check()
            self._upgrade()
        except BaseException:
            self.conn.close()
            raise

    def _check(self):
        """Switch to WAL mode and run quick_check. Raises RnfsError if the file isn't a sound database."""
        try:
            self.conn.execute(f"PRAGMA busy_timeout = {int(BUSY_TIMEOUT * 1000)}")
            mode = self.conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                raise RnfsError(f"could not switch the state database {self.path} to WAL mode (it is in {mode})")
            self.conn.execute(f"PRAGMA journal_size_limit = {JOURNAL_SIZE_LIMIT}")
            problems = [row[0] for row in self.conn.execute("PRAGMA quick_check").fetchall()]
        except self.sqlite3.DatabaseError as e:
            self._raise_open_error(e)
        if problems != ["ok"]:
            raise RnfsError(damaged_message(self.path, "; ".join(str(p) for p in problems[:5])))

    def _raise_open_error(self, e):
        """Raise RnfsError for a failure opening the database: damaged if SQLite found it corrupt or not a
        database; otherwise what failed (a read-only directory, a disk error, a lock), which isn't damage."""
        code = getattr(e, "sqlite_errorcode", None)
        if code is not None and code & 0xFF in (self.sqlite3.SQLITE_CORRUPT, self.sqlite3.SQLITE_NOTADB):
            raise RnfsError(damaged_message(self.path, str(e))) from None
        raise RnfsError(f"could not open the state database {self.path}: {e}. It was left as it is: check that the "
                        "state directory is writable by this user and the disk works, then start again") from None

    def _upgrade(self):
        """Apply the schema steps this database lacks, each in its own transaction. Raises RnfsError if it was
        written by a newer rnfs."""
        try:
            version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        except self.sqlite3.DatabaseError as e:
            self._raise_open_error(e)
        if version > len(SCHEMA):
            raise RnfsError(f"the state database {self.path} was written by a newer version of rnfs (schema "
                            f"{version}; this version knows up to {len(SCHEMA)}); run that version or a later "
                            "one (rnfs never moves state back to an older format)")
        for number in range(version, len(SCHEMA)):
            with self.transaction(durable=True) as tx:
                for step in SCHEMA[number][1]:
                    if callable(step):
                        step(tx)
                    else:
                        tx.execute(step)
                tx.execute(f"PRAGMA user_version = {number + 1}")

    @contextmanager
    def transaction(self, durable: bool) -> Iterator:
        """A write transaction (BEGIN IMMEDIATE ... COMMIT), yielding the connection to execute statements on.
        Rolled back if the body raises. sqlite3 errors are raised as StateDbError."""
        self._not_inside()
        with self.lock:
            self._owner = threading.get_ident()
            try:
                conn = self._conn()
                try:
                    level = "FULL" if durable else "NORMAL"
                    if self.synchronous != level:
                        conn.execute(f"PRAGMA synchronous = {level}")  # takes effect from the next commit
                        self.synchronous = level
                    conn.execute("BEGIN IMMEDIATE")
                except self.sqlite3.Error as e:
                    raise StateDbError(f"state database {self.path}: {e}") from e
                try:
                    yield conn
                    self._commit(conn)
                except BaseException as e:
                    if conn.in_transaction:
                        try:
                            conn.execute("ROLLBACK")
                        except self.sqlite3.Error:
                            pass  # a failed COMMIT may have rolled back already
                    # A value SQLite can't take (an integer past 64 bits, text with a lone surrogate) fails as the
                    # disk would, so callers that survive a failed save survive it too
                    if isinstance(e, (self.sqlite3.Error, OverflowError, UnicodeEncodeError)):
                        raise StateDbError(f"state database {self.path}: {e}") from e
                    raise
            finally:
                self._owner = None

    def _not_inside(self):
        """Raise RuntimeError if this thread is inside a transaction already (the lock isn't reentrant)."""
        if self._owner == threading.get_ident():
            raise RuntimeError("the state database was used inside one of its own transactions")

    def _commit(self, conn):
        """Commit the open transaction (tests replace this to make a commit fail)."""
        conn.execute("COMMIT")

    def query(self, sql: str, params=()) -> list[tuple]:
        """Every row sql gives. sqlite3 errors are raised as StateDbError."""
        self._not_inside()
        with self.lock:
            try:
                return self._conn().execute(sql, params).fetchall()
            except self.sqlite3.Error as e:
                raise StateDbError(f"state database {self.path}: {e}") from e

    def _conn(self):
        """The connection; StateDbError once closed."""
        if self.conn is None:
            raise StateDbError(f"the state database {self.path} is closed")
        return self.conn

    def close(self):
        """Close the connection (SQLite checkpoints the WAL into the database file)."""
        with self.lock:
            if self.conn is not None:
                try:
                    self.conn.close()
                finally:
                    self.conn = None


def damaged_message(path: str, problem: str) -> str:
    """The startup error for a damaged state database: what is wrong and how to recover."""
    directory = os.path.dirname(path)
    return (f"the state database {path} is damaged ({problem}). rnfs has left it as it is and won't start on it. "
            f"To recover, stop rnfs and move {DB_FILE} (with {DB_FILE}-wal and {DB_FILE}-shm, if there) out of "
            f"{directory}: rnfs then starts with fresh state. The JSON files of the version you upgraded from are "
            f"kept there as <name>{MIGRATED_SUFFIX}; renamed back to <name>, they are imported again at the next "
            "start (everything changed since the upgrade is lost). Blocks and keys are kept in any case.")


def _create_private(path: str):
    """Create the database file readable by its owner only, before SQLite opens it, and tighten an existing
    one (and its -wal and -shm files, whose mode SQLite copies from it) that others could read."""
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | fsutil.O_BINARY, 0o600)
    except OSError as e:
        raise RnfsError(f"could not create the state database {path}: {e}") from None
    os.close(fd)
    if fsutil.IS_WINDOWS:
        return  # no mode bits there: the state directory's access list (winacl) covers what is created in it
    for name in (path, path + "-wal", path + "-shm"):
        try:
            info = os.stat(name)
            if stat.S_IMODE(info.st_mode) & 0o077:
                os.chmod(name, 0o600)
        except FileNotFoundError:
            continue
        except OSError as e:
            raise RnfsError(f"could not make {name} private to its owner: {e}") from None


_open: dict[str, _Database] = {}  # by normalised state directory
_open_lock = threading.Lock()


class StateDb:
    """One store's reference to the state database of a state directory (see connect)."""

    def __init__(self, database: _Database):
        """A reference to database; use connect() rather than making one directly."""
        self._database: _Database | None = database
        self.path = database.path

    def _db(self) -> _Database:
        """The shared database; StateDbError once this reference is closed."""
        database = self._database
        if database is None:
            raise StateDbError(f"the state database {self.path} is closed")
        return database

    def transaction(self, durable: bool = False):
        """A write transaction: `with db.transaction(durable) as tx: tx.execute(...)`, committed when the block
        ends, rolled back if it raises. durable: synchronous=FULL, on disk when the block ends; else NORMAL.

        Take it only with every lock the change needs already held, and inside it take no other lock and do no
        network I/O. Raises StateDbError (an OSError) if the database fails; the change is then not made."""
        return self._db().transaction(durable)

    def query(self, sql: str, params=()) -> list[tuple]:
        """Every row of a read (stores read only when they load). Raises StateDbError."""
        return self._db().query(sql, params)

    def meta(self, store: str, key: str, default=None):
        """store's meta value key, or default if it was never set."""
        rows = self.query("SELECT value FROM meta WHERE store = ? AND key = ?", (store, key))
        return rows[0][0] if rows else default

    def close(self):
        """Release this reference (idempotent); the last one closes the database."""
        with _open_lock:
            database, self._database = self._database, None
            if database is None:
                return
            database.references -= 1
            if database.references == 0:
                _open.pop(_key(database.state_dir), None)
                database.close()


def _key(state_dir: str) -> str:
    """The registry key for a state directory."""
    return os.path.normcase(os.path.realpath(state_dir))


def connect(state_dir: str) -> StateDb:
    """A reference to the state database in state_dir (created 0700 if missing), opening it if this process
    hasn't, and importing any JSON state files found there first (migrate). Close it when done.

    Raises RnfsError if the database can't be opened, is damaged or is newer than this code, or if migrating a
    file must stop startup (an unreadable pins.json, a damaged names.json), and OSError if a file can't be read."""
    state_dir = os.path.expanduser(state_dir)
    os.makedirs(state_dir, mode=0o700, exist_ok=True)
    with _open_lock:
        key = _key(state_dir)
        database = _open.get(key)
        if database is None:
            database = _Database(state_dir)
            _open[key] = database
        database.references += 1
        db = StateDb(database)
    try:
        with database.migrate_lock:
            migrate(db, state_dir)
    except BaseException:
        db.close()
        raise
    return db


def open_databases() -> list[str]:
    """Paths of the state databases this process has open (tests check none is left open)."""
    with _open_lock:
        return [database.path for database in _open.values()]


# Stores: what every store does the same way


WRITE_THROUGH = "write_through"
SNAPSHOT = "snapshot"
_UNKNOWN = object()  # RowStore's last-written memo has no entry for a key


def open_store(state_dir: str, load: Callable[[StateDb], object]) -> StateDb:
    """A reference to the state database in state_dir (connect) once load(db) has read a store's rows from it
    (making the store's RowStore first, so the load can mark rows it changed). If load raises, the reference is
    released and the exception raised."""
    db = connect(state_dir)
    try:
        load(db)
    except BaseException:
        db.close()
        raise
    return db


def delete_rows(db: StateDb, table: str, key_cols: tuple[str, ...], keys: list[tuple]):
    """Delete the rows of table whose key_cols hold each of keys (tuples of their values), in one transaction: the
    rows a store's load found malformed. Not durable: a row a power loss brings back is only dropped again."""
    if keys:
        with db.transaction() as tx:
            tx.executemany(f"DELETE FROM {table} WHERE {_where(key_cols)}", keys)


def _where(key_cols: tuple[str, ...]) -> str:
    """The WHERE clause matching one row's key_cols."""
    return " AND ".join(f"{column} = ?" for column in key_cols)


class RowStore:
    """Writes a store's changed rows: one table whose key_cols are its primary key, one row per key.

    The store keeps its state in memory under its own lock, and tells changed(key) of each key it changes (lock
    held). row(key) gives the row key has now, a tuple of the table's columns in the table's order, or None once
    it is gone (lock held); key_values(key) gives its key_cols' values (by default (key,)). A write deletes the
    rows of the keys gone and inserts or replaces the others, in one transaction, durable as given.

    This owns what each store used to do by hand: the set of keys changed and not yet written, writing them in
    order, and keeping them for the next write when one fails (the change itself stays in memory):
    - WRITE_THROUGH: changed() writes at once, under the store's lock (each change is on disk, as durable says,
      when it returns); it raises OSError (StateDbError) if the write fails, and the keys are written with the
      next change or save().
    - SNAPSHOT: changed() only notes the keys; save() takes them and their rows under the lock and writes them
      outside it, so a slow disk never holds the store up. One save at a time, so an older snapshot never lands
      last; if the write fails the keys are noted again and save() raises.

    skip_unchanged: remember each row as last written, and leave out a key whose row is as it was (for a store that
    marks keys that may not have changed, as jobs mark the running ones at each save). A store whose rows span
    several tables (the publisher) subclasses this, overriding snapshot() and write_rows().
    """

    def __init__(self, db: StateDb, table: str, key_cols: tuple[str, ...], *, durable: bool,
                 mode: str = WRITE_THROUGH, lock, row: Callable[[object], tuple | None] | None = None,
                 key_values: Callable[[object], tuple] = lambda key: (key,), skip_unchanged: bool = False):
        """Rows of table for a store whose state lock guards; see the class. Raises ValueError for an unknown
        mode."""
        if mode not in (WRITE_THROUGH, SNAPSHOT):
            raise ValueError(f"unknown RowStore mode {mode!r}")
        self.db = db
        self.table = table
        self.durable = durable
        self.mode = mode
        self.lock = lock
        self._row = row
        self._key_values = key_values
        self._dirty: set = set()  # keys changed and not yet written (under lock)
        self._save_lock = threading.Lock()  # SNAPSHOT: one save at a time, so an older snapshot never lands last
        # skip_unchanged: key -> its row as last committed (under _save_lock in SNAPSHOT mode, lock otherwise)
        self._written: dict | None = {} if skip_unchanged else None
        self._delete = f"DELETE FROM {table} WHERE {_where(key_cols)}"
        columns = [info[1] for info in db.query(f"PRAGMA table_info({table})")]
        self._insert = (f"INSERT OR REPLACE INTO {table} ({', '.join(columns)}) "
                        f"VALUES ({', '.join('?' * len(columns))})")

    @property
    def pending(self) -> frozenset:
        """The keys changed and not yet written."""
        with self.lock:
            return frozenset(self._dirty)

    def changed(self, *keys):
        """Note that keys changed (lock held); in WRITE_THROUGH mode, write them, and any a failed write left, at
        once. Raises OSError (StateDbError) if that write fails; the keys are then written by the next one."""
        self._dirty.update(keys)
        if self.mode == WRITE_THROUGH:
            self._write_dirty()

    def save(self):
        """Write the keys changed and not yet written, if any: in SNAPSHOT mode the store's changes since the last
        save, in WRITE_THROUGH mode those a failed write left. Raises OSError (StateDbError) if the write fails;
        they are then written by the next save."""
        if self.mode == WRITE_THROUGH:
            with self.lock:
                if self._dirty:
                    self._write_dirty()
            return
        with self._save_lock:
            with self.lock:
                keys, self._dirty = self._dirty, set()
                if not keys:
                    return
                snapshot = self.snapshot(keys)
            try:
                self._commit(snapshot)
            except BaseException:
                with self.lock:  # written by the next save
                    self._dirty |= keys
                raise

    def _write_dirty(self):
        """WRITE_THROUGH: write every key not yet written (lock held), then forget them; kept if it fails."""
        self._commit(self.snapshot(self._dirty))
        self._dirty.clear()

    def _commit(self, snapshot):
        """Write snapshot in one transaction, if it holds anything, then remember what was written."""
        if snapshot:
            with self.db.transaction(self.durable) as tx:
                self.write_rows(tx, snapshot)
            self.written(snapshot)

    def snapshot(self, keys) -> list:
        """What a write of keys writes, taken under the lock: [(key, row or None)], leaving out (skip_unchanged)
        keys whose row is as last written. Empty (false) when there is nothing to write."""
        rows = [(key, self._row(key)) for key in keys]
        if self._written is not None:
            rows = [(key, row) for key, row in rows if self._written.get(key, _UNKNOWN) != row]
        return rows

    def write_rows(self, tx, snapshot):
        """Write snapshot inside transaction tx: delete the keys gone, insert or replace the others' rows."""
        tx.executemany(self._delete, [self._key_values(key) for key, row in snapshot if row is None])
        tx.executemany(self._insert, [row for _, row in snapshot if row is not None])

    def written(self, snapshot):
        """snapshot was committed (skip_unchanged: remember its rows; a key gone is forgotten)."""
        if self._written is None:
            return
        for key, row in snapshot:
            if row is None:
                self._written.pop(key, None)
            else:
                self._written[key] = row


def reconcile_pins(pins: dict, prefix: str, recorded: Callable[[str, object], bool], clean: bool,
                   adopt: Callable[[dict[str, list]], None], release: Callable[[str, object], None]):
    """At start, make a feature's pins match its saved state. Its pins are those whose source is prefix and an
    owner ("lxmf:<sender>", "follow:<name>"); pins is storage.pins() (CID -> sources). A pin recorded(owner, cid)
    says the state doesn't record was left by a crash between the pin and the save, or by state that was lost.

    If the state is clean (saved whole, and every row of it read), release(owner, cid) releases each such pin:
    nothing else would (`pin rm` releases only the user's pins). An OSError it raises (a read-only state
    directory) is logged; the pin is tried again at the next start. Otherwise such a pin may be all that is left
    of the state that was lost: adopt({owner: [cid, ...]}) records them again (owners and CIDs in order), so they
    are kept, and the feature can release them later."""
    unrecorded: dict[str, list] = {}
    for cid, sources in pins.items():
        for source in sources:
            if source.startswith(prefix) and not recorded(source[len(prefix):], cid):
                unrecorded.setdefault(source[len(prefix):], []).append(cid)
    if not unrecorded:
        return
    if not clean:
        adopt({owner: sorted(unrecorded[owner], key=str) for owner in sorted(unrecorded)})
        return
    for owner, cids in unrecorded.items():
        for cid in cids:
            try:
                release(owner, cid)
            except OSError as e:
                log(f"state: could not release {cid}, pinned for {prefix}{owner} ({e}); trying again at the next "
                    "start", LOG_ERROR)


# Migration of the JSON state files of rnfs 0.1.4 and earlier


@dataclass(frozen=True)
class Legacy:
    """A JSON state file and how it is imported.

    read(path, shape) gives its value (None: nothing to import; damaged files are handled as read_json does),
    by default read_json. importer(value, tx) checks each record as the store's loader always has (dropping
    malformed ones with a warning), replaces the store's rows with them inside transaction tx and returns how
    many it imported. Both are "module:function", imported when needed."""

    file: str
    importer: str
    shape: Callable[[object], bool] | None = None
    read: str | None = None
    unreadable_ok: bool = False  # an unreadable file (OSError) is logged and left for the next start


def shaped(key: str, kind: type) -> Callable[[object], bool]:
    """A read_json shape: an object whose key, if present, holds a kind."""
    return lambda value: isinstance(value, dict) and isinstance(value.get(key, kind()), kind)


def _is(kind: type) -> Callable[[object], bool]:
    """A read_json shape: a value of kind."""
    return lambda value: isinstance(value, kind)


LEGACY: tuple[Legacy, ...] = (
    Legacy("pins.json", "rnfs.content.storage:import_pins", shaped("pins", dict)),
    Legacy("cache.json", "rnfs.content.storage:import_cache", shaped("fetches", list)),
    Legacy("pin_names.json", "rnfs.pin_names:import_names", _is(dict)),
    Legacy("fetched.json", "rnfs.node:import_fetched", _is(list)),
    Legacy("remote_pins.json", "rnfs.node:import_remote_pins", _is(list), unreadable_ok=True),
    Legacy("invalid.json", "rnfs.fetcher:import_invalid", _is(list)),
    Legacy("published.json", "rnfs.publisher:import_published",
           lambda value: all(shaped(key, dict)(value) for key in ("published", "earlier", "withdraw"))),
    Legacy("providers.json", "rnfs.dht.providers:import_records", read="rnfs.dht.providers:read_records"),
    Legacy("names.json", "rnfs.names:import_names", read="rnfs.names:read_names"),
    Legacy("name-floor.json", "rnfs.names:import_floor", read="rnfs.names:read_floor"),
    Legacy("shared_folder.json", "rnfs.content.shared_folder:import_entries", shaped("entries", dict)),
    Legacy("jobs.json", "rnfs.features.jobs:import_jobs", shaped("jobs", list)),
    Legacy("keepers.json", "rnfs.features.keepers:import_keepers"),
    Legacy("follow.json", "rnfs.features.follow:import_follow"),
    Legacy("contacts.json", "rnfs.features.contacts:import_contacts"),
    Legacy("lxmf_inbox.json", "rnfs.features.lxmf:import_inbox"),
    Legacy("lxmf_outbox.json", "rnfs.features.lxmf:import_outbox"),
)


def _resolve(target: str) -> Callable:
    """The function a "module:function" string names."""
    module, _, name = target.partition(":")
    return getattr(importlib.import_module(module), name)


def migrate(db: StateDb, state_dir: str):
    """Import every legacy JSON state file in state_dir not imported yet, one transaction each, recording it in
    the migrated table in the same transaction; then rename it to <name>.migrated and sync the directory. A file
    the table already lists (a crash came between the commit and the rename) is only renamed."""
    present = [item for item in LEGACY if os.path.lexists(os.path.join(state_dir, item.file))]
    if not present:
        return
    done = {row[0] for row in db.query("SELECT file FROM migrated")}
    for item in present:
        path = os.path.join(state_dir, item.file)
        if item.file in done:
            log(f"state: {item.file} was imported into {DB_FILE} before; finishing its migration", LOG_NOTICE)
            _keep_original(path, state_dir)
            continue
        try:
            value = (_resolve(item.read) if item.read else read_json)(path, item.shape)
        except OSError as e:
            if not item.unreadable_ok:
                raise
            log(f"state: could not read {path} ({e}); leaving it to import at the next start", LOG_ERROR)
            continue
        if value is None:
            continue  # damaged and moved aside (or gone meanwhile): the store starts without it, as before
        importer = _resolve(item.importer)
        with db.transaction(durable=True) as tx:
            count = importer(value, tx)
            tx.execute("INSERT INTO migrated (file, records, at) VALUES (?, ?, ?)", (item.file, count, time.time()))
        kept = _keep_original(path, state_dir)
        log(f"state: imported {item.file} into {DB_FILE} ({plural(count, 'record')})"
            + (f"; the original is kept as {os.path.basename(kept)}" if kept else ""), LOG_NOTICE)


def _keep_original(path: str, state_dir: str) -> str | None:
    """Rename an imported file to <name>.migrated (.migrated.1, ... if taken) and sync the directory; the new path,
    or None if it couldn't be renamed (logged: the next start tries again, and won't import it twice)."""
    try:
        kept = _move_aside(path, MIGRATED_SUFFIX)
        fsutil.sync_directory(state_dir)
    except OSError as e:
        log(f"state: could not rename {path} after importing it ({e}); it is renamed at the next start", LOG_ERROR)
        return None
    return kept


# Reading the JSON state files


def read_json(path: str, shape: Callable[[object], bool] | None = None):
    """The JSON value in path, or None if it doesn't exist. A damaged file (not JSON, failing shape if given,
    or a directory) is moved aside, not lost, and gives None. Raises OSError if path can't be read: a transient
    or permission error isn't damage, and starting without the file could lose what it records (pins)."""
    try:
        with open(path, encoding="utf-8") as f:
            value = json.load(f)  # an empty file, as a crash can leave, is a ValueError too
    except FileNotFoundError:
        return None
    except ValueError as e:
        problem = str(e)
    except OSError:
        if not os.path.isdir(path):  # Windows reports opening a directory as PermissionError
            raise
        problem = "it is a directory"
    else:
        if shape is None or shape(value):
            return value
        problem = "unexpected structure"
    set_aside(path, problem)
    return None


def set_aside(path: str, problem: str):
    """Move a damaged state file aside to <name>.damaged (logged with problem), so it isn't lost; if that fails
    (a read-only state directory), log that the node starts without it."""
    try:
        aside = _move_aside(path, ".damaged")
    except OSError as moving:
        log(f"storage: {path} is damaged ({problem}) and couldn't be moved aside ({moving}); starting without it",
            LOG_ERROR)
        return
    log(f"storage: {path} is damaged ({problem}); moved to {aside} and starting without it", LOG_ERROR)


def _move_aside(path: str, suffix: str) -> str:
    """Rename path to path + suffix, or path + suffix + .<n> so an earlier copy is kept; the new path."""
    for n in range(1000):
        aside = path + suffix + (f".{n}" if n else "")
        try:
            fsutil.rename_new(path, aside)
            return aside
        except FileExistsError:
            continue
    raise FileExistsError(f"too many {suffix} copies of {path}")
