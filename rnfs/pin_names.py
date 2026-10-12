"""Local names for pins: where each pin came from on this machine, for `rnfs pin ls`. No RNS dependency.

A CID names only content, so `add photo.jpg` keeps no file name. This records, for this node's own user,
what each pin was added from (an absolute path, "(stdin)", the target given to `pin add`, an archive) so
listings can show it. It is local only: never sent anywhere, and it doesn't change any CID.
"""
import os
import threading

from rnfs.content.cid import Cid
from rnfs.content.storage import read_json, write_json

NAMES_FILE = "pin_names.json"
MAX_NAME_LENGTH = 4096  # an origin longer than this (a very deep path) is cut, not refused


class PinNames:
    """Pin → where it was added from. Thread-safe; entries for CIDs no longer pinned are pruned."""

    def __init__(self, state_dir: str):
        """Load the names saved in state_dir, skipping anything that doesn't parse."""
        self.path = os.path.join(state_dir, NAMES_FILE)
        self._lock = threading.Lock()
        self._names: dict[Cid, str] = {}
        value = read_json(self.path, shape=lambda value: isinstance(value, dict)) or {}
        for text, name in value.items():
            try:
                if isinstance(name, str):
                    self._names[Cid.parse(text)] = name[:MAX_NAME_LENGTH]
            except (ValueError, TypeError):
                continue

    def record(self, cid: Cid, name: str):
        """Record that cid was added from name; a later add of the same content replaces it."""
        name = name[:MAX_NAME_LENGTH]
        with self._lock:
            if self._names.get(cid) == name:
                return
            self._names[cid] = name
            self._save()

    def record_if_unknown(self, cid: Cid, name: str):
        """Record name for cid unless it already has one (a pin add of something added here keeps its path)."""
        with self._lock:  # held throughout, so a concurrent record() isn't overwritten
            if cid in self._names:
                return
            self._names[cid] = name[:MAX_NAME_LENGTH]
            self._save()

    def get(self, cid: Cid) -> str | None:
        """Where cid was added from, or None if unknown."""
        with self._lock:
            return self._names.get(cid)

    def prune(self, pinned: set[Cid]):
        """Forget the names of CIDs that are no longer pinned, so the file stays as small as the pin list."""
        with self._lock:
            gone = [cid for cid in self._names if cid not in pinned]
            if not gone:
                return
            for cid in gone:
                del self._names[cid]
            self._save()

    def _save(self):
        """Write the names atomically (called with the lock held)."""
        write_json(self.path, {str(cid): name for cid, name in sorted(self._names.items(), key=lambda i: str(i[0]))})
