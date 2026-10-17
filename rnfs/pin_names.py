"""Local names for pins: where each pin came from on this machine, for `rnfs pin ls`. No RNS dependency.

A CID names only content, so `add photo.jpg` keeps no file name. This records, for this node's own user,
what each pin was added from (an absolute path, "(stdin)", the target given to `pin add`, an archive) so
listings can show it. It is local only: never sent anywhere, and it doesn't change any CID. Kept in the state
database's pin_names table (rnfs.statedb): each change is one row written at once, durably.
"""
import threading

from rnfs import statedb
from rnfs.content.cid import Cid
from rnfs.log import LOG_WARNING, log

MAX_NAME_LENGTH = 4096  # an origin longer than this (a very deep path) is cut, not refused


def import_names(value: dict, tx) -> int:
    """Import pin_names.json (statedb.migrate): its names replace the pin_names table, each checked as rnfs 0.1.4
    loaded it (one that doesn't parse is skipped, a long one cut to MAX_NAME_LENGTH). Returns the number
    imported."""
    tx.execute("DELETE FROM pin_names")
    imported = set()
    for text, name in value.items():
        try:
            if not isinstance(name, str):
                continue
            cid = Cid.parse(text)
            tx.execute("INSERT OR REPLACE INTO pin_names (cid, name) VALUES (?, ?)",
                       (cid.binary, statedb.dump(name[:MAX_NAME_LENGTH])))
        except (ValueError, TypeError):
            continue
        imported.add(cid)
    return len(imported)


class PinNames:
    """Pin → where it was added from. Thread-safe; entries for CIDs no longer pinned are pruned."""

    def __init__(self, state_dir: str):
        """Load the names saved in state_dir; a row that doesn't parse is logged and deleted."""
        self._db = statedb.connect(state_dir)
        self._lock = threading.Lock()
        self._names: dict[Cid, str] = {}
        self._dirty: set[Cid] = set()  # CIDs whose name changed (or went) and whose save failed
        try:
            self._load()
        except BaseException:
            self._db.close()
            raise

    def _load(self):
        """Read the pin_names table."""
        bad = []
        for binary, text in self._db.query("SELECT cid, name FROM pin_names"):
            try:
                name = statedb.load(text)
                if not isinstance(name, str):
                    raise TypeError("not a string")
                self._names[Cid.from_bytes(binary)] = name[:MAX_NAME_LENGTH]
            except (ValueError, TypeError):
                log(f"pin names: ignoring invalid record {binary!r} {text!r}", LOG_WARNING)
                bad.append((binary,))
        if bad:
            with self._db.transaction() as tx:
                tx.executemany("DELETE FROM pin_names WHERE cid = ?", bad)

    def record(self, cid: Cid, name: str):
        """Record that cid was added from name; a later add of the same content replaces it."""
        name = name[:MAX_NAME_LENGTH]
        with self._lock:
            if self._names.get(cid) == name:
                return
            self._names[cid] = name
            self._save([cid])

    def record_if_unknown(self, cid: Cid, name: str):
        """Record name for cid unless it already has one (a pin add of something added here keeps its path)."""
        with self._lock:  # held throughout, so a concurrent record() isn't overwritten
            if cid in self._names:
                return
            self._names[cid] = name[:MAX_NAME_LENGTH]
            self._save([cid])

    def get(self, cid: Cid) -> str | None:
        """Where cid was added from, or None if unknown."""
        with self._lock:
            return self._names.get(cid)

    def prune(self, pinned: set[Cid]):
        """Forget the names of CIDs that are no longer pinned, so the table stays as small as the pin list."""
        with self._lock:
            gone = [cid for cid in self._names if cid not in pinned]
            if not gone:
                return
            for cid in gone:
                del self._names[cid]
            self._save(gone)

    def _save(self, changed: list[Cid]):
        """Write the rows of changed, and of earlier changes whose save failed, durably (lock held). If it fails
        they are written with the next change."""
        self._dirty.update(changed)
        names = [(cid.binary, statedb.dump(self._names[cid])) for cid in self._dirty if cid in self._names]
        gone = [(cid.binary,) for cid in self._dirty if cid not in self._names]
        with self._db.transaction(durable=True) as tx:
            tx.executemany("DELETE FROM pin_names WHERE cid = ?", gone)
            tx.executemany("INSERT OR REPLACE INTO pin_names (cid, name) VALUES (?, ?)", names)
        self._dirty.clear()

    def close(self):
        """Release the state database (the node calls this once it has saved everything at shutdown)."""
        self._db.close()
