"""Kademlia routing table of DHT servers (specification section 6). No RNS dependency.

Only servers that proved reachable are stored. Full buckets keep their long-lived nodes, so an attacker
with fresh identities can't take a table over; nodes that stop answering are dropped, and a dropped node
that comes back soon regains its seniority. Bans (section 10) also live here.
"""
import threading
import time
from collections.abc import Callable

from rnfs.config import Config
from rnfs.constants import BUCKET_COUNT, ID_LENGTH, K


def distance(a: bytes, b: bytes) -> int:
    """Kademlia distance between two IDs: a XOR b as a 128-bit big-endian integer."""
    return int.from_bytes(a, "big") ^ int.from_bytes(b, "big")


def check_id(node_id: bytes):
    """Raise ValueError unless node_id is an ID_LENGTH-byte bytes (node IDs and DHT keys)."""
    if not isinstance(node_id, bytes) or len(node_id) != ID_LENGTH:
        raise ValueError(f"node IDs and keys must be {ID_LENGTH} bytes")


class _Entry:
    """A server in the table: when it was first seen and last heard, and its unanswered queries in a row."""

    __slots__ = ("node_id", "first_seen", "last_heard", "failures")

    def __init__(self, node_id: bytes, first_seen: float, now: float):
        """Entry for node_id, heard now."""
        self.node_id = node_id
        self.first_seen = first_seen
        self.last_heard = now
        self.failures = 0  # hearing resets this, so > 0 means "failed a query since last heard"


class RoutingTable:
    """BUCKET_COUNT buckets of up to K servers each, by distance from own_id. Thread-safe.

    `clock` is monotonic seconds (nothing here is persisted). `admit(node_id)`, if given, must be True
    for a node to be added: private mode passes a member check (section 3).
    """

    def __init__(self, own_id: bytes, config: Config, clock: Callable[[], float] = time.monotonic,
                 admit: Callable[[bytes], bool] | None = None):
        """Empty table for the local node own_id, using config's announce_interval, max_failures, ban_time."""
        check_id(own_id)
        self.own_id = own_id
        self.config = config
        self.clock = clock
        self.admit = admit
        self._buckets: list[dict[bytes, _Entry]] = [{} for _ in range(BUCKET_COUNT)]
        self._dropped: dict[bytes, tuple[float, float]] = {}  # node ID -> (first seen, when dropped)
        self._bans: dict[bytes, float] = {}  # node ID -> ban end
        self._lock = threading.RLock()

    @property
    def _silence_limit(self) -> float:
        """Silence after which a node is presumed gone: it missed two announces."""
        return 2 * self.config.announce_interval

    def bucket_index(self, node_id: bytes) -> int:
        """Index of the bucket node_id belongs in: i for distance in [2^i, 2^(i+1)). ValueError for own_id."""
        check_id(node_id)
        if node_id == self.own_id:
            raise ValueError("the local node has no bucket")
        return distance(self.own_id, node_id).bit_length() - 1

    def _stale(self, entry: _Entry, now: float) -> bool:
        """True if entry failed a query since it was last heard, or has been silent too long."""
        return entry.failures > 0 or now - entry.last_heard > self._silence_limit

    def _prune(self, now: float):
        """Forget expired bans, and dropped nodes whose seniority can no longer be regained."""
        for node_id in [n for n, until in self._bans.items() if until <= now]:
            del self._bans[node_id]
        for node_id in [n for n, (_, dropped) in self._dropped.items() if now - dropped > self._silence_limit]:
            del self._dropped[node_id]

    def heard(self, node_id: bytes) -> bool:
        """Record that node_id proved to be a reachable server (server-mode announce, or answered /find).

        True if it was newly added; False if it was already in the table (now refreshed) or wasn't added
        (our own ID, banned, not admitted, or its bucket is full of healthy nodes).
        """
        check_id(node_id)
        if node_id == self.own_id:  # hearing our own announce is normal
            return False
        with self._lock:
            now = self.clock()
            self._prune(now)
            if node_id in self._bans:
                return False
            bucket = self._buckets[self.bucket_index(node_id)]
            entry = bucket.get(node_id)
            if entry is not None:
                entry.last_heard = now
                entry.failures = 0
                return False
            if self.admit is not None and not self.admit(node_id):
                return False

            if len(bucket) >= K:
                # Only the least recently heard node can give up its place, and only if it has stopped
                # answering or gone silent. Healthy long-lived nodes are never displaced by newcomers.
                victim = min(bucket.values(), key=lambda e: e.last_heard)
                if not self._stale(victim, now):
                    return False
                del bucket[victim.node_id]
                # Displaced, not dropped for failures: it may come back with its seniority like a dropped node
                self._dropped[victim.node_id] = (victim.first_seen, now)

            # A node dropped recently (e.g. by a lossy link) regains its seniority
            first_seen, _ = self._dropped.pop(node_id, (now, now))
            bucket[node_id] = _Entry(node_id, first_seen, now)
            return True

    def failed(self, node_id: bytes) -> bool:
        """Count a query node_id didn't answer; after max_failures in a row it is dropped. True if dropped."""
        check_id(node_id)
        if node_id == self.own_id:
            return False
        with self._lock:
            now = self.clock()
            bucket = self._buckets[self.bucket_index(node_id)]
            entry = bucket.get(node_id)
            if entry is None:
                return False
            entry.failures += 1
            if entry.failures < self.config.max_failures:
                return False
            del bucket[node_id]
            self._dropped[node_id] = (entry.first_seen, now)
            return True

    def remove(self, node_id: bytes):
        """Remove node_id (e.g. it now announces client mode), forgetting its seniority."""
        check_id(node_id)
        if node_id == self.own_id:
            return
        with self._lock:
            self._buckets[self.bucket_index(node_id)].pop(node_id, None)
            self._dropped.pop(node_id, None)

    def ban(self, node_id: bytes):
        """Ban node_id for ban_time: dropped with its seniority, and ignored by heard() until the ban ends."""
        check_id(node_id)
        if node_id == self.own_id:
            return
        with self._lock:
            self.remove(node_id)
            self._bans[node_id] = self.clock() + self.config.ban_time

    def is_banned(self, node_id: bytes) -> bool:
        """True while node_id is banned; its announces and provider records are then ignored too."""
        with self._lock:
            until = self._bans.get(node_id)
            if until is None:
                return False
            if until <= self.clock():
                del self._bans[node_id]
                return False
            return True

    def closest(self, key: bytes, count: int = K, for_reply: bool = False) -> list[bytes]:
        """The count servers in the table closest to key, closest first.

        for_reply (answering /find) leaves out nodes that failed a query since last heard or have been
        silent over 2 × announce_interval, so departed nodes stay out of replies without a table refresh.
        """
        check_id(key)
        with self._lock:
            now = self.clock()
            # The key's bucket may be sparse or empty, so rank every node rather than one bucket
            entries = [e for bucket in self._buckets for e in bucket.values()
                       if not (for_reply and self._stale(e, now))]
        entries.sort(key=lambda e: distance(e.node_id, key))
        return [e.node_id for e in entries[:count]]

    def seniority(self, node_id: bytes) -> float | None:
        """Seconds since node_id was first seen (kept across a short absence), or None if not in the table."""
        with self._lock:
            entry = self._find(node_id)
            return None if entry is None else self.clock() - entry.first_seen

    def contains(self, node_id: bytes) -> bool:
        """True if node_id is in the table."""
        with self._lock:
            return self._find(node_id) is not None

    def nodes(self) -> list[bytes]:
        """Every node ID in the table, nearest buckets first."""
        with self._lock:
            return [node_id for bucket in self._buckets for node_id in bucket]

    def node_count(self) -> int:
        """Number of servers in the table."""
        with self._lock:
            return sum(len(bucket) for bucket in self._buckets)

    def live_count(self) -> int:
        """Number of servers in the table that /find replies would include: none failed a query since last
        heard, or has been silent over 2 × announce_interval."""
        with self._lock:
            now = self.clock()
            return sum(1 for bucket in self._buckets for e in bucket.values() if not self._stale(e, now))

    def _find(self, node_id: bytes) -> _Entry | None:
        """The entry for node_id, or None (also for malformed IDs and our own). Caller holds the lock."""
        if not isinstance(node_id, bytes) or len(node_id) != ID_LENGTH or node_id == self.own_id:
            return None
        return self._buckets[self.bucket_index(node_id)].get(node_id)
