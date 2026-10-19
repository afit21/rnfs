"""The announce first-seen log (specification section 6, "Announce first-seen log"): when this node first heard
each DHT server's rnfs.dht announce in server mode, whether or not the server is in the routing table (a full
bucket keeps its nodes, and the log isn't limited by buckets).

Identities generated next to a key can't be old ones: of the servers near a key, those this node has heard for at
least provider_seniority were there before any such identity was made for it. closest(key, count, min_age) gives
the count closest of them, for publishers to store to as well and searchers to ask (both to come; this module is
only the log and its query). It is local state: nothing about it is sent.

- At most MAX_FIRST_SEEN servers (about 12 MB of memory when full); the least recently heard is forgotten first, so
  a flood of new identities pushes out only servers silent longer than every one of them. A server that announces
  client mode is forgotten (it is no longer a server; if it comes back, it is new again).
- Each announce costs a dictionary update, and a sorted insert (a memory move) for a server not heard before; the
  rows changed are written together with the node's batched saves (every 30 s) and at stop, not durably, so a flood
  of announces costs one transaction per save, not one per announce. A row's last-heard time is the time of the
  save that writes it (within the save interval of the last announce): it only orders forgetting.
- Times are wall clock (seniority spans restarts); one in the future when loaded (a clock since corrected
  backwards) counts as now.
- closest() walks prefix ranges of the IDs kept sorted: the IDs sharing the key's first p bits are a contiguous run,
  all closer to the key than any outside it, so the run is widened one bit at a time until it holds count old
  enough, and only that run is ranked by distance.
"""
import bisect
import math
import os
import threading
import time
from collections import OrderedDict
from collections.abc import Callable

from rnfs import statedb
from rnfs.constants import ID_LENGTH, K
from rnfs.dht.routing import check_id, distance, is_id

MAX_FIRST_SEEN = 65_536  # servers kept; the least recently heard is forgotten first
TABLE = "announce_first_seen"
_ID_BITS = ID_LENGTH * 8


def _never(node_id: bytes) -> bool:
    """Default exclude: excludes nothing."""
    return False


class FirstSeenLog:
    """When each server's announce was first heard. Thread-safe; a node.Store (save, close) given a state_dir."""

    def __init__(self, state_dir: str | None = None, clock: Callable[[], float] = time.time,
                 max_rows: int = MAX_FIRST_SEEN):
        """The log saved in state_dir (only in memory without one), on clock (Unix time)."""
        self.clock = clock
        self.max_rows = max_rows
        self._lock = threading.Lock()
        self._first: OrderedDict[bytes, float] = OrderedDict()  # node ID -> first heard; least recently heard first
        self._sorted: list[bytes] = []  # the same node IDs in ascending order (as 128-bit big-endian integers)
        self._store: statedb.RowStore | None = None
        self._db = statedb.open_store(os.path.expanduser(state_dir), self._load) if state_dir else None

    # Persistence

    def _load(self, db: statedb.StateDb):
        """Read the saved log, least recently heard first."""
        self._store = statedb.RowStore(db, TABLE, ("node",), durable=False, mode=statedb.SNAPSHOT, lock=self._lock,
                                       row=self._row)
        now = self.clock()
        changed = []
        for node, first, last in db.query(f"SELECT node, first_heard, last_heard FROM {TABLE} ORDER BY last_heard"):
            first, last_heard = _time(first), _time(last)
            if not is_id(node) or first is None or last_heard is None:
                changed.append(node)
                continue
            if first > now or last_heard > now:
                changed.append(node)
            self._first[bytes(node)] = min(first, now)
        self._sorted = sorted(self._first)
        with self._lock:
            changed += self._evict()
            self._store.changed(*changed)

    def _row(self, node_id) -> tuple | None:
        """node_id's row as written now, or None once it has none (lock held)."""
        first = self._first.get(node_id)
        if first is None:
            return None
        return node_id, first, max(first, self.clock())

    def save(self):
        """Write the rows changed since the last save. Raises OSError (StateDbError) if the database fails; they are
        written by the next save."""
        if self._store is not None:
            self._store.save()

    def close(self):
        """Release the state database (idempotent)."""
        if self._db is not None:
            self._db.close()

    def _changed(self, node_id: bytes):
        """node_id's row changed or went (lock held)."""
        if self._store is not None:
            self._store.changed(node_id)

    def _remove(self, node_id: bytes):
        """Take node_id, in the log, out of the sorted IDs (lock held)."""
        index = bisect.bisect_left(self._sorted, node_id)
        del self._sorted[index]

    def _evict(self) -> list[bytes]:
        """Forget the least recently heard servers past max_rows (lock held); the nodes forgotten."""
        gone = []
        while len(self._first) > self.max_rows:
            node_id, _ = self._first.popitem(last=False)
            self._remove(node_id)
            self._changed(node_id)
            gone.append(node_id)
        return gone

    # The log

    def heard(self, node_id: bytes):
        """node_id's rnfs.dht announce in server mode arrived: noted as first heard now if new, and as the most
        recently heard either way."""
        if not is_id(node_id):
            return
        with self._lock:
            if node_id in self._first:
                self._first.move_to_end(node_id)
            else:
                self._first[node_id] = self.clock()
                bisect.insort(self._sorted, node_id)
                self._evict()
            self._changed(node_id)

    def forget(self, node_id: bytes):
        """node_id announces client mode: it isn't a server any more."""
        with self._lock:
            if self._first.pop(node_id, None) is not None:
                self._remove(node_id)
                self._changed(node_id)

    def first_heard(self, node_id: bytes) -> float | None:
        """When node_id's announce was first heard (Unix time), or None if it isn't in the log."""
        with self._lock:
            return self._first.get(node_id)

    def __len__(self) -> int:
        """Servers in the log."""
        with self._lock:
            return len(self._first)

    def closest(self, key: bytes, count: int = K, min_age: float = 0.0,
                exclude: Callable[[bytes], bool] = _never) -> list[bytes]:
        """The count servers closest to key, closest first, among those first heard at least min_age seconds ago
        (provider_seniority, for publishing and searching) and not excluded (banned, say). exclude is called with
        the log's lock held, so it must not use the log."""
        check_id(key)
        if count <= 0:
            return []
        value = int.from_bytes(key, "big")
        with self._lock:
            horizon = self.clock() - min_age
            first = self._first
            # The narrowest run holding count IDs at all (a run only grows as it widens), then widened until it
            # holds count old enough
            low, high = 0, _ID_BITS
            while low < high:
                middle = (low + high) // 2
                start, end = self._run(value, middle)
                low, high = (low, middle) if end - start >= count else (middle + 1, high)
            for shift in range(low, _ID_BITS + 1):
                start, end = self._run(value, shift)
                old = [n for n in self._sorted[start:end] if first[n] <= horizon and not exclude(n)]
                if len(old) >= count:
                    break
        return sorted(old, key=lambda n: distance(n, key))[:count]

    def _run(self, value: int, shift: int) -> tuple[int, int]:
        """The slice of the sorted IDs sharing value's first (_ID_BITS - shift) bits: every one of them is closer to
        value than any ID outside it (lock held)."""
        low = (value >> shift) << shift
        high = low + (1 << shift)
        start = bisect.bisect_left(self._sorted, low.to_bytes(ID_LENGTH, "big"))
        end = len(self._sorted) if high >> _ID_BITS else bisect.bisect_left(self._sorted,
                                                                              high.to_bytes(ID_LENGTH, "big"))
        return start, end


def _time(value) -> float | None:
    """A saved time as a float, or None if it isn't a finite number."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value)
