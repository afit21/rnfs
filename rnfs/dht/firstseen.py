"""The announce first-seen log (specification section 6, "Announce first-seen log"): when this node first heard
each DHT server's rnfs.dht announce in server mode, whether or not the server is in the routing table (a full
bucket keeps its nodes, and the log isn't limited by buckets).

Identities generated next to a key can't be old ones: of the servers near a key, those this node has heard for at
least provider_seniority were there before any such identity was made for it. closest(key, count, min_age) gives
the count closest of them, for publishers to store to as well (rnfs.node's _senior_servers, a store part of the
publisher) and searchers to ask (to come). It is local state: nothing about it is sent.

- At most MAX_FIRST_SEEN servers (about 12 MB of memory when full). When full, it forgets first a server silent for
  `silence` (2 × announce_interval: /find replies and closest(heard_within) leave such servers out anyway), counted
  from the later of its last announce and the log's load, so a restart doesn't make every server silent; then the
  server not yet senior (first heard less than `seniority`, provider_seniority, ago) that was first heard most
  recently; and only if every server is senior and heard lately, the least recently heard. A flood of new identities
  therefore pushes out only its own newest, and servers gone silent: forgetting the least recently heard first, as
  0.2.0's first version did, let about 3 fresh identities a second (65,536 in 6 hours) push out every honest server
  between its 6-hourly announces, so none ever became senior (REV7). A server that announces client mode is forgotten
  (it is no longer a server; if it comes back, it is new again), but only for an announce emitted after its last
  server announce here: the node judges that (rnfs.node's _heard_client), as a replayed old client announce would
  make an honest server new again (AUD-L9).
- Each announce costs a dictionary update, and a sorted insert (a memory move) for a server not heard before; the
  rows changed are written together with the node's batched saves (every 30 s) and at stop, not durably, so a flood
  of announces costs one transaction per save, not one per announce. A row's last-heard time is the time of the
  save that writes it (within the save interval of the last announce): it orders forgetting, and is the last-heard
  time after a restart.
- Times are wall clock (seniority spans restarts); one in the future when loaded (a clock since corrected
  backwards) counts as now.
- The last time each server was heard is kept in memory too (loaded as the time of the save that wrote its row), so
  closest() can leave out servers that have gone silent.
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

MAX_FIRST_SEEN = 65_536  # servers kept (see the module docstring for which go first)
DEFAULT_SENIORITY = 86_400.0  # seconds: the default provider_seniority
DEFAULT_SILENCE = 2 * 6 * 3600.0  # seconds: twice the default announce_interval
TABLE = "announce_first_seen"
_ID_BITS = ID_LENGTH * 8


def _never(node_id: bytes) -> bool:
    """Default exclude: excludes nothing."""
    return False


class FirstSeenLog:
    """When each server's announce was first heard. Thread-safe; a node.Store (save, close) given a state_dir."""

    def __init__(self, state_dir: str | None = None, clock: Callable[[], float] = time.time,
                 max_rows: int = MAX_FIRST_SEEN, seniority: float = DEFAULT_SENIORITY,
                 silence: float = DEFAULT_SILENCE):
        """The log saved in state_dir (only in memory without one), on clock (Unix time). When full, it keeps servers
        first heard at least seniority seconds ago (provider_seniority) unless silent for silence seconds (2 ×
        announce_interval)."""
        self.clock = clock
        self.max_rows = max_rows
        self.seniority = seniority
        self.silence = silence
        self._lock = threading.Lock()
        self._loaded = clock()  # silence counts from here at the earliest
        self._first: dict[bytes, float] = {}  # node ID -> first heard; in the order first heard
        # node ID -> last heard (as loaded: the save that wrote it); least recently heard first
        self._last: OrderedDict[bytes, float] = OrderedDict()
        self._sorted: list[bytes] = []  # the same node IDs in ascending order (as 128-bit big-endian integers)
        self._store: statedb.RowStore | None = None
        self._db = statedb.open_store(os.path.expanduser(state_dir), self._load) if state_dir else None

    # Persistence

    def _load(self, db: statedb.StateDb):
        """Read the saved log, in the order first heard."""
        self._store = statedb.RowStore(db, TABLE, ("node",), durable=False, mode=statedb.SNAPSHOT, lock=self._lock,
                                       row=self._row)
        now = self.clock()
        changed = []
        last = {}
        for node, first, last_heard in db.query(f"SELECT node, first_heard, last_heard FROM {TABLE} "
                                                "ORDER BY first_heard, node"):
            first, last_heard = _time(first), _time(last_heard)
            if not is_id(node) or first is None or last_heard is None:
                changed.append(node)
                continue
            if first > now or last_heard > now:
                changed.append(node)
            self._first[bytes(node)] = min(first, now)
            last[bytes(node)] = min(max(first, last_heard), now)
        self._last = OrderedDict(sorted(last.items(), key=lambda item: item[1]))
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
        """Forget servers past max_rows, as the module docstring says (lock held); the nodes forgotten."""
        gone = []
        now = self.clock()
        while len(self._first) > self.max_rows:
            node_id = self._victim(now)
            del self._first[node_id]
            del self._last[node_id]
            self._remove(node_id)
            self._changed(node_id)
            gone.append(node_id)
        return gone

    def _victim(self, now: float) -> bytes:
        """The server a full log forgets next (lock held): the least recently heard if silent for self.silence (since
        the load at the earliest), else the most recently first heard if not yet senior, else the least recently
        heard. First-heard order is insertion order, so if the newest is senior every other is too."""
        oldest = next(iter(self._last))
        if now - max(self._last[oldest], self._loaded) > self.silence:
            return oldest
        newest = next(reversed(self._first))
        if self._first[newest] > now - self.seniority:
            return newest
        return oldest

    # The log

    def heard(self, node_id: bytes):
        """node_id's rnfs.dht announce in server mode arrived: noted as first heard now if new, and as the most
        recently heard either way."""
        if not is_id(node_id):
            return
        with self._lock:
            now = self.clock()
            if node_id in self._first:
                self._last.move_to_end(node_id)
            else:
                self._first[node_id] = now
                bisect.insort(self._sorted, node_id)
            self._last[node_id] = now
            self._changed(node_id)
            if len(self._first) > self.max_rows:
                self._evict()  # possibly node_id itself, if new

    def forget(self, node_id: bytes):
        """node_id announces client mode: it isn't a server any more."""
        with self._lock:
            if self._first.pop(node_id, None) is not None:
                self._last.pop(node_id, None)
                self._remove(node_id)
                self._changed(node_id)

    def first_heard(self, node_id: bytes) -> float | None:
        """When node_id's announce was first heard (Unix time), or None if it isn't in the log."""
        with self._lock:
            return self._first.get(node_id)

    def last_heard(self, node_id: bytes) -> float | None:
        """When node_id's announce was last heard (Unix time; as loaded, the save that wrote it), or None if it isn't in
        the log."""
        with self._lock:
            return self._last.get(node_id)

    def __len__(self) -> int:
        """Servers in the log."""
        with self._lock:
            return len(self._first)

    def closest(self, key: bytes, count: int = K, min_age: float = 0.0,
                exclude: Callable[[bytes], bool] = _never, heard_within: float | None = None) -> list[bytes]:
        """The count servers closest to key, closest first, among those first heard at least min_age seconds ago
        (provider_seniority, for publishing and searching), heard within heard_within seconds if given (a publisher
        leaves out servers that have gone silent, as /find replies do) and not excluded (banned, say). exclude is
        called with the log's lock held, so it must not use the log."""
        check_id(key)
        if count <= 0:
            return []
        value = int.from_bytes(key, "big")
        with self._lock:
            now = self.clock()
            horizon = now - min_age
            fresh = float("-inf") if heard_within is None else now - heard_within
            first, last = self._first, self._last
            # The narrowest run holding count IDs at all (a run only grows as it widens), then widened until it
            # holds count old enough
            low, high = 0, _ID_BITS
            while low < high:
                middle = (low + high) // 2
                start, end = self._run(value, middle)
                low, high = (low, middle) if end - start >= count else (middle + 1, high)
            for shift in range(low, _ID_BITS + 1):
                start, end = self._run(value, shift)
                old = [n for n in self._sorted[start:end]
                       if first[n] <= horizon and last[n] >= fresh and not exclude(n)]
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
