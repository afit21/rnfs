"""Local names for pins: where each pin came from on this machine, and the label its user gave it, for `rnfs pin
ls`. No RNS dependency.

A CID names only content, so `add photo.jpg` keeps no file name. This records, for this node's own user,
what each pin was added from (an absolute path, "(stdin)", the target given to `pin add`, an archive) so
listings can show it. It is local only: never sent anywhere, and it doesn't change any CID. Kept in the state
database's pin_names table (rnfs.statedb): each change is one row written at once, but not durably
(synchronous=NORMAL): the pin it names was committed durably before it, so a power loss can lose at most the
latest names, never a pin, and an add costs one durable commit rather than two.

A label (`rnfs pin label`, POST /api/pin/label; rnfs 0.2.0) is the name the user chose for a pin, kept apart from
where it was added from, which stays as recorded: the pin_labels table, each change written at once and durably
(the user asked for it). Names are pruned with the pins they name; a label goes as its CID stops being pinned
(forget_label, from the node's pin listener), and at start for one a stop left behind (prune_labels).
"""
import threading

from rnfs import statedb
from rnfs.content.cid import Cid
from rnfs.log import LOG_WARNING, log

MAX_NAME_LENGTH = 4096  # an origin longer than this (a very deep path) is cut, not refused
MAX_LABEL_LENGTH = 1024  # characters a label may have; a longer one is refused


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


def check_label(label) -> str | None:
    """label as stored: None (or "") clears it, else a string of 1 to MAX_LABEL_LENGTH characters, surrounding
    whitespace removed. Raises ValueError for anything else."""
    if label is None:
        return None
    if not isinstance(label, str):
        raise ValueError("a label must be a string (or null to clear it)")
    label = label.strip()
    if len(label) > MAX_LABEL_LENGTH:
        raise ValueError(f"a label may have at most {MAX_LABEL_LENGTH} characters")
    return label or None


class PinNames:
    """Pin → where it was added from, and its label. Thread-safe; entries for CIDs no longer pinned are pruned."""

    def __init__(self, state_dir: str):
        """Load the names and labels saved in state_dir; a row that doesn't parse is logged and deleted."""
        self._lock = threading.Lock()
        self._names: dict[Cid, str] = {}
        self._labels: dict[Cid, str] = {}
        self._db = statedb.open_store(state_dir, self._load)

    def _load(self, db: statedb.StateDb):
        """Read the pin_names table."""
        # Written through: a change is on disk (not durably) when it returns; one whose write failed is written
        # with the next change, or by save()
        self._store = statedb.RowStore(db, "pin_names", ("cid",), durable=False, lock=self._lock, row=self._row,
                                       key_values=lambda cid: (cid.binary,))
        bad = []
        for binary, text in db.query("SELECT cid, name FROM pin_names"):
            try:
                name = statedb.load(text)
                if not isinstance(name, str):
                    raise TypeError("not a string")
                self._names[Cid.from_bytes(binary)] = name[:MAX_NAME_LENGTH]
            except (ValueError, TypeError):
                log(f"pin names: ignoring invalid record {binary!r} {text!r}", LOG_WARNING)
                bad.append((binary,))
        statedb.delete_rows(db, "pin_names", ("cid",), bad)
        self._label_store = statedb.RowStore(db, "pin_labels", ("cid",), durable=True, lock=self._lock,
                                             row=self._label_row, key_values=lambda cid: (cid.binary,))
        bad = []
        for binary, text in db.query("SELECT cid, label FROM pin_labels"):
            try:
                label = check_label(statedb.load(text))
                if label is None:
                    raise ValueError("empty")
                self._labels[Cid.from_bytes(binary)] = label
            except (ValueError, TypeError):
                log(f"pin names: ignoring invalid label {binary!r} {text!r}", LOG_WARNING)
                bad.append((binary,))
        statedb.delete_rows(db, "pin_labels", ("cid",), bad)

    def _row(self, cid: Cid) -> tuple | None:
        """cid's pin_names row, or None if it has no name (lock held)."""
        name = self._names.get(cid)
        return None if name is None else (cid.binary, statedb.dump(name))

    def _label_row(self, cid: Cid) -> tuple | None:
        """cid's pin_labels row, or None if it has no label (lock held)."""
        label = self._labels.get(cid)
        return None if label is None else (cid.binary, statedb.dump(label))

    def set_label(self, cid: Cid, label: str | None) -> bool:
        """Label cid (None clears its label), saved durably; True if that changed it. Raises ValueError for a
        label check_label refuses, OSError if it can't be saved (kept in memory, saved with the next change)."""
        label = check_label(label)
        with self._lock:
            previous = self._labels.get(cid)
            if previous == label:
                return False
            self._put_label(cid, label)
            try:
                self._label_store.changed(cid)
            except BaseException:
                self._put_label(cid, previous)  # not saved: memory says what the database does
                raise
            return True

    def _put_label(self, cid: Cid, label: str | None):
        """Set or (None) remove cid's label in memory (lock held)."""
        if label is None:
            self._labels.pop(cid, None)
        else:
            self._labels[cid] = label

    def forget_label(self, cid: Cid):
        """Drop cid's label, once it is no longer pinned by anything (the node's pin listener). Raises OSError if
        that can't be saved (dropped in memory all the same, and saved with the next change)."""
        with self._lock:
            if self._labels.pop(cid, None) is not None:
                self._label_store.changed(cid)

    def prune_labels(self, pinned):
        """Drop the labels of CIDs not in pinned, at start: a label is dropped as its pin goes (forget_label), so
        only a stop between the two leaves one."""
        with self._lock:
            gone = [cid for cid in self._labels if cid not in pinned]
            for cid in gone:
                del self._labels[cid]
            if gone:
                self._label_store.changed(*gone)

    def label(self, cid: Cid) -> str | None:
        """cid's label, or None."""
        with self._lock:
            return self._labels.get(cid)

    def record(self, cid: Cid, name: str):
        """Record that cid was added from name; a later add of the same content replaces it. Raises OSError if it
        can't be saved (kept in memory, and saved with the next change)."""
        name = name[:MAX_NAME_LENGTH]
        with self._lock:
            if self._names.get(cid) == name:
                return
            self._names[cid] = name
            self._store.changed(cid)

    def record_if_unknown(self, cid: Cid, name: str):
        """Record name for cid unless it already has one (a pin add of something added here keeps its path)."""
        with self._lock:  # held throughout, so a concurrent record() isn't overwritten
            if cid in self._names:
                return
            self._names[cid] = name[:MAX_NAME_LENGTH]
            self._store.changed(cid)

    def get(self, cid: Cid) -> str | None:
        """Where cid was added from, or None if unknown."""
        with self._lock:
            return self._names.get(cid)

    def prune(self, pinned: set[Cid]):
        """Forget the names of CIDs that are no longer pinned, so the table stays as small as the pin list. Not the
        labels: a label is the user's own choice, dropped exactly when its pin goes (forget_label), never by a
        listing working from a snapshot that a new pin and label may have overtaken."""
        with self._lock:
            gone = [cid for cid in self._names if cid not in pinned]
            if not gone:
                return
            for cid in gone:
                del self._names[cid]
            self._store.changed(*gone)

    def save(self):
        """Write the names and labels whose write failed, if any (each change is written as it is made)."""
        self._store.save()
        self._label_store.save()

    def close(self):
        """Release the state database (the node calls this once it has saved everything at shutdown)."""
        self._db.close()
