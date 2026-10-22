"""Kademlia routing table of DHT servers (specification section 6). No RNS dependency.

Only servers that proved reachable are stored. Full buckets keep their long-lived nodes, so an attacker
with fresh identities can't take a table over; nodes that stop answering are dropped, and a dropped node
that comes back soon regains its seniority. Bans (section 10) also live here, and the node IDs of identities RNS
blackholes count as banned while listed (blackholed).

Lookups also ask the table which nodes are unreliable: those whose last max_failures queries or more all
failed, in the table or not (a node dropped, or only named in other nodes' replies). Announces don't clear
this, only an answer does, so a far node that keeps announcing but never accepts a link stops costing every
lookup its connection timeout. Such a node may be queried again once its backoff has passed (AVOID_AFTER,
doubling with each further failure up to AVOID_MAX), by one lookup at a time (may_retry).

The table works on the monotonic clock, and is persisted through rnfs.dht.routing_state (which owns the state
database, so this module stays free of it): every change to a node's entry, dropped seniority or ban is passed to
the listener set with track(), and row() gives what is saved for a node, its times as wall clock; restore() loads
saved rows at start, clamping times in the future to now. The nodes' failed queries in a row (unreliable) aren't
saved.
"""
import math
import threading
import time
from collections import OrderedDict
from collections.abc import Callable

from rnfs.config import Config
from rnfs.constants import BUCKET_COUNT, ID_LENGTH, K, MISSED_ANNOUNCES

AVOID_AFTER = 300.0  # seconds an unreliable node isn't retried after max_failures failed queries in a row ...
AVOID_MAX = 3600.0  # ... doubling with each further failure up to this
MAX_UNRELIABLE = 4096  # nodes whose failed queries are remembered; the least recently failed is forgotten first

# What a saved row (row(), restore()) holds for a node
IN_TABLE = 0  # in the table: first seen, last heard, failed queries since last heard
DROPPED = 1  # dropped, its seniority kept: first seen, when dropped
BANNED = 2  # banned: when the ban ends


def distance(a: bytes, b: bytes) -> int:
    """Kademlia distance between two IDs: a XOR b as a 128-bit big-endian integer."""
    return int.from_bytes(a, "big") ^ int.from_bytes(b, "big")


def is_id(value) -> bool:
    """True if value has the shape of a node ID or DHT key (ID_LENGTH bytes)."""
    return isinstance(value, bytes) and len(value) == ID_LENGTH


def check_id(node_id: bytes):
    """Raise ValueError unless node_id is an ID_LENGTH-byte bytes (node IDs and DHT keys)."""
    if not is_id(node_id):
        raise ValueError(f"node IDs and keys must be {ID_LENGTH} bytes")


def _ignore(node_id: bytes):
    """The default change listener: does nothing."""


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

    `clock` is monotonic seconds, and `wall_clock` Unix time, which only saved rows use (row(), restore()).
    `admit(node_id)`, if given, must be True for a node to be added: private mode passes a member check
    (section 3). It is asked without the table's lock held, since it may block (on RNS).
    """

    def __init__(self, own_id: bytes, config: Config, clock: Callable[[], float] = time.monotonic,
                 admit: Callable[[bytes], bool] | None = None, wall_clock: Callable[[], float] = time.time):
        """Empty table for the local node own_id, using config's announce_interval, max_failures, ban_time."""
        check_id(own_id)
        self.own_id = own_id
        self.config = config
        self.clock = clock
        self.wall_clock = wall_clock
        self.admit = admit
        # Told the node ID of each change to what row() gives, with the lock held (routing_state's RowStore)
        self._changed: Callable[[bytes], object] = _ignore
        self._buckets: list[dict[bytes, _Entry]] = [{} for _ in range(BUCKET_COUNT)]
        # Both in the order their entries expire (when dropped, ban end), each popped and re-inserted when set, so
        # _prune only looks at the front of each
        self._dropped: dict[bytes, tuple[float, float]] = {}  # node ID -> (first seen, when dropped)
        self._bans: dict[bytes, float] = {}  # node ID -> ban end
        # Node ID -> [failed queries in a row, when it may be retried], for any node; only an answer clears it
        self._unanswered: OrderedDict[bytes, list] = OrderedDict()
        self._lock = threading.RLock()
        # Node IDs banned for as long as RNS blackholes their identity (rnfs.network.blackhole.Blackhole.node_listed,
        # set by the node): never blocks, and isn't saved
        self.blackholed: Callable[[bytes], bool] = lambda node_id: False

    @property
    def lock(self) -> threading.RLock:
        """The table's lock (reentrant): its saved state's RowStore takes it to read rows (row())."""
        return self._lock

    @property
    def _silence_limit(self) -> float:
        """Silence after which a node is presumed gone: it missed MISSED_ANNOUNCES (two) announces."""
        return MISSED_ANNOUNCES * self.config.announce_interval

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
        """Forget expired bans, and dropped nodes whose seniority can no longer be regained. Caller holds the lock.

        Runs on every announce heard, so it stops at the first entry still current: both dicts are in expiry order.
        Their readers (_hear, _add, is_banned) check expiry themselves, so an entry left out of order by a clock step
        or a changed setting only costs memory until those before it expire."""
        expired = []
        for node_id, until in self._bans.items():
            if until > now:
                break
            expired.append(node_id)
        for node_id in expired:
            del self._bans[node_id]
            self._changed(node_id)
        expired = []
        for node_id, (_, dropped) in self._dropped.items():
            if now - dropped <= self._silence_limit:
                break
            expired.append(node_id)
        for node_id in expired:
            del self._dropped[node_id]
            self._changed(node_id)

    def _set_dropped(self, node_id: bytes, first_seen: float, now: float):
        """Remember node_id's seniority as dropped now, at the end of _dropped's expiry order. Caller holds the lock."""
        self._dropped.pop(node_id, None)
        self._dropped[node_id] = (first_seen, now)
        self._changed(node_id)

    def heard(self, node_id: bytes) -> bool:
        """Record that node_id proved to be a reachable server (server-mode announce, or answered /find; for an
        answer, call answered(), which also makes it reliable again).

        True if it was newly added; False if it was already in the table (now refreshed) or wasn't added
        (our own ID, banned, not admitted, or its bucket is full of healthy nodes).
        """
        return self._hear(node_id, False)

    def answered(self, node_id: bytes) -> bool:
        """Record that node_id answered a query: heard(), and it is reliable again. True if newly added."""
        return self._hear(node_id, True)

    def _hear(self, node_id: bytes, answered: bool) -> bool:
        """heard(), and for answered() also forget node_id's failed queries, in the same locked step.

        admit is asked outside the lock (it may block on RNS), and only for a node found not in the table and
        not banned: the table is then checked again, so the outcome is as if admit had been asked under it.
        """
        check_id(node_id)
        if node_id == self.own_id:  # hearing our own announce is normal
            return False
        admitted = True if self.admit is None else None  # None: not asked yet
        while True:
            with self._lock:
                if answered:
                    self._unanswered.pop(node_id, None)
                now = self.clock()
                self._prune(now)
                # Banned (an expired ban may be left: see _prune), or its identity blackholed by RNS
                if self._bans.get(node_id, now) > now or self.blackholed(node_id):
                    return False
                bucket = self._buckets[self.bucket_index(node_id)]
                entry = bucket.get(node_id)
                if entry is not None:
                    entry.last_heard = now
                    entry.failures = 0
                    self._changed(node_id)
                    return False
                if admitted:
                    return self._add(bucket, node_id, now)
                if admitted is False:
                    return False
            admitted = bool(self.admit(node_id))

    def _add(self, bucket: dict[bytes, _Entry], node_id: bytes, now: float) -> bool:
        """Add node_id, not in the table, to its bucket unless that is full of healthy nodes. True if added.
        Caller holds the lock."""
        if len(bucket) >= K:
            # Only the least recently heard node can give up its place, and only if it has stopped
            # answering or gone silent. Healthy long-lived nodes are never displaced by newcomers.
            victim = min(bucket.values(), key=lambda e: e.last_heard)
            if not self._stale(victim, now):
                return False
            del bucket[victim.node_id]
            # Displaced, not dropped for failures: it may come back with its seniority like a dropped node
            self._set_dropped(victim.node_id, victim.first_seen, now)

        # A node dropped recently (e.g. by a lossy link) regains its seniority
        first_seen, dropped = self._dropped.pop(node_id, (now, now))
        if now - dropped > self._silence_limit:  # expired, but not yet pruned (see _prune)
            first_seen = now
        bucket[node_id] = _Entry(node_id, first_seen, now)
        self._changed(node_id)
        return True

    def failed(self, node_id: bytes) -> bool:
        """Count a query node_id didn't answer; after max_failures in a row it is dropped. True if dropped.

        Also counted towards unreliable(), whether or not node_id is in the table; hearing it doesn't reset that.
        """
        check_id(node_id)
        if node_id == self.own_id:
            return False
        with self._lock:
            now = self.clock()
            self._count_unanswered(node_id, now)
            bucket = self._buckets[self.bucket_index(node_id)]
            entry = bucket.get(node_id)
            if entry is None:
                return False
            entry.failures += 1
            if entry.failures < self.config.max_failures:
                self._changed(node_id)
                return False
            del bucket[node_id]
            self._set_dropped(node_id, entry.first_seen, now)
            return True

    def _count_unanswered(self, node_id: bytes, now: float):
        """One more failed query for node_id; past max_failures it isn't retried for a doubling backoff."""
        entry = self._unanswered.pop(node_id, None) or [0, now]
        entry[0] += 1
        if entry[0] >= self.config.max_failures:
            entry[1] = now + self._backoff(entry[0])
        self._unanswered[node_id] = entry  # last: the most recently failed
        while len(self._unanswered) > MAX_UNRELIABLE:
            self._unanswered.popitem(last=False)

    def _backoff(self, count: int) -> float:
        """Seconds before retrying a node whose last count queries failed (at least max_failures)."""
        return min(AVOID_MAX, AVOID_AFTER * 2 ** min(count - self.config.max_failures, 32))

    def unreliable(self, node_id: bytes) -> bool:
        """True if node_id's last max_failures queries or more all failed, however recently it was heard.

        Lookups avoid such nodes unless they need them: they are queried last, and only queried before others
        as a retry (may_retry), which no lookup waits for."""
        with self._lock:
            entry = self._unanswered.get(node_id)
            return entry is not None and entry[0] >= self.config.max_failures

    def may_retry(self, node_id: bytes) -> bool:
        """True if unreliable node_id is due to be queried again, claiming that retry: other lookups see it as
        not due until the query fails (backing off further) or is answered (reliable again)."""
        with self._lock:
            entry = self._unanswered.get(node_id)
            if entry is None or entry[0] < self.config.max_failures:
                return False
            now = self.clock()
            if now < entry[1]:
                return False
            entry[1] = now + self._backoff(entry[0] + 1)
            return True

    def remove(self, node_id: bytes):
        """Remove node_id (e.g. it now announces client mode), forgetting its seniority."""
        check_id(node_id)
        if node_id == self.own_id:
            return
        with self._lock:
            self._buckets[self.bucket_index(node_id)].pop(node_id, None)
            self._dropped.pop(node_id, None)
            self._changed(node_id)

    def ban(self, node_id: bytes):
        """Ban node_id for ban_time: dropped with its seniority, and ignored by heard() until the ban ends."""
        check_id(node_id)
        if node_id == self.own_id:
            return
        with self._lock:
            self.remove(node_id)
            self._bans.pop(node_id, None)  # re-inserted last, keeping _bans in expiry order
            self._bans[node_id] = self.clock() + self.config.ban_time
            self._changed(node_id)

    def is_banned(self, node_id: bytes) -> bool:
        """True while node_id is banned, or RNS blackholes its identity (blackholed); its announces and provider
        records are then ignored too."""
        if self.blackholed(node_id):
            return True
        with self._lock:
            until = self._bans.get(node_id)
            if until is None:
                return False
            if until <= self.clock():
                del self._bans[node_id]
                self._changed(node_id)
                return False
            return True

    def closest(self, key: bytes, count: int = K, for_reply: bool = False) -> list[bytes]:
        """The count servers in the table closest to key, closest first.

        for_reply (answering /find) leaves out nodes that failed a query since last heard or have been
        silent over 2 × announce_interval, so departed nodes stay out of replies without a table refresh.
        """
        check_id(key)
        # Distances to key rank whole buckets: those in key's own bucket b are under 2^b, those in every bucket
        # below it in [2^b, 2^(b+1)), and those in a bucket i above it in [2^i, 2^(i+1)). So the buckets are
        # walked in that order and only those needed for count are sorted, not the whole table (this answers
        # /find requests). key is own_id: b is -1, and the buckets rank in order from 0
        b = distance(self.own_id, key).bit_length() - 1
        walk = ([[b]] if b >= 0 else []) + [range(max(b, 0))] + [[i] for i in range(b + 1, BUCKET_COUNT)]
        groups: list[list[bytes]] = []
        found = 0
        with self._lock:
            now = self.clock()
            for indices in walk:
                group = [e.node_id for i in indices for e in self._buckets[i].values()
                         if not (for_reply and self._stale(e, now))]
                groups.append(group)
                found += len(group)
                if 0 < count <= found:
                    break
        return [n for group in groups for n in sorted(group, key=lambda node_id: distance(node_id, key))][:count]

    def seniority(self, node_id: bytes) -> float | None:
        """Seconds since node_id was first seen (kept across a short absence), or None if not in the table."""
        with self._lock:
            entry = self._find(node_id)
            return None if entry is None else self.clock() - entry.first_seen

    def is_senior(self, node_id: bytes, min_age: float) -> bool:
        """True if node_id has been in the table (kept across a short absence) for at least min_age seconds."""
        known_for = self.seniority(node_id)
        return known_for is not None and known_for >= min_age

    def standing(self, node_id: bytes, min_age: float) -> int:
        """How much node_id is trusted to be a live, long-standing server, best first: 0 in the table for at
        least min_age and healthy, 1 in the table and healthy, 2 in the table but failed a query since last heard
        or silent over 2 × announce_interval, 3 not in the table. Ranks keepers (section 9)."""
        with self._lock:
            entry = self._find(node_id)
            return 3 if entry is None else self._standing(entry, min_age, self.clock())

    def standings(self, min_age: float) -> dict[bytes, int]:
        """standing() of every node in the table now (those left out stand at 3), so a caller can rank nodes
        on the table as it was before a lookup: nodes the lookup adds by answering don't gain rank from it."""
        with self._lock:
            now = self.clock()
            return {entry.node_id: self._standing(entry, min_age, now)
                    for bucket in self._buckets for entry in bucket.values()}

    def _standing(self, entry: _Entry, min_age: float, now: float) -> int:
        """standing() of a node in the table. Caller holds the lock."""
        if self._stale(entry, now):
            return 2
        return 0 if now - entry.first_seen >= min_age else 1

    def join_order(self) -> list[bytes]:
        """The servers in the table that haven't failed a query since last heard, longest known first: those a join
        asks before bootstrap nodes (the node puts the better scored first, keeping this order between equals)."""
        with self._lock:
            entries = [e for bucket in self._buckets for e in bucket.values() if e.failures == 0]
        return [e.node_id for e in sorted(entries, key=lambda e: (e.first_seen, e.node_id))]

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
        if not is_id(node_id) or node_id == self.own_id:
            return None
        return self._buckets[self.bucket_index(node_id)].get(node_id)

    # Persistence (rnfs.dht.routing_state)

    def track(self, changed: Callable[[bytes], object]):
        """Tell changed(node_id), with the table's lock held, whenever what row(node_id) gives changes."""
        with self._lock:
            self._changed = changed

    def row(self, node_id) -> tuple | None:
        """What is saved for node_id: (node_id, IN_TABLE, first seen, last heard, failed queries since),
        (node_id, DROPPED, first seen, when dropped, 0) or (node_id, BANNED, None, ban end, 0), times as wall clock;
        None if the table holds nothing for it (or it isn't a node ID)."""
        if not is_id(node_id) or node_id == self.own_id:
            return None
        with self._lock:
            offset = self.wall_clock() - self.clock()
            entry = self._buckets[self.bucket_index(node_id)].get(node_id)
            if entry is not None:
                return node_id, IN_TABLE, entry.first_seen + offset, entry.last_heard + offset, entry.failures
            if node_id in self._dropped:
                first_seen, dropped = self._dropped[node_id]
                return node_id, DROPPED, first_seen + offset, dropped + offset, 0
            if node_id in self._bans:
                return node_id, BANNED, None, self._bans[node_id] + offset, 0
            return None

    def restore(self, rows: list[tuple]) -> list:
        """Load rows row() gave (from the state database) into a table nothing has used yet; returns the node values
        of the rows that no longer match what the table holds, so they are written again or deleted.

        A time later than now (a clock since corrected backwards) counts as now, and a ban ends no later than
        ban_time from now. Left out: rows that are malformed or of our own ID, dropped nodes whose seniority has
        expired, bans that have ended, nodes admit() rejects (private mode: no longer members) and, in a full
        bucket (after our identity changed), the nodes first seen last. Silent nodes are kept as they were saved:
        /find replies leave them out until they are heard again, as before the restart."""
        changed = []
        parsed = []
        for row in rows:
            fields = _parse_row(row)
            if fields is None or fields[0] == self.own_id:
                changed.append(row[0] if row else None)
            else:
                parsed.append(fields)
        if self.admit is not None:  # asked without the lock, as heard() does: it may block
            for fields in [f for f in parsed if f[1] == IN_TABLE and not self.admit(f[0])]:
                parsed.remove(fields)
                changed.append(fields[0])
        with self._lock:
            wall_now, now = self.wall_clock(), self.clock()
            offset = wall_now - now
            dropped, bans = [], []
            for node_id, state, first_seen, at, failures in sorted(parsed, key=lambda f: (f[2] or 0.0, f[0])):
                clamped = min(at, wall_now), None if first_seen is None else min(first_seen, wall_now)
                if state == BANNED:
                    until = min(at, wall_now + self.config.ban_time)
                    if until <= wall_now:
                        changed.append(node_id)
                        continue
                    bans.append((until - offset, node_id))
                    clamped = (until, None)
                elif state == DROPPED:
                    if wall_now - clamped[0] > self._silence_limit:
                        changed.append(node_id)
                        continue
                    dropped.append((clamped[0] - offset, clamped[1] - offset, node_id))
                else:
                    bucket = self._buckets[self.bucket_index(node_id)]
                    if len(bucket) >= K:
                        changed.append(node_id)
                        continue
                    entry = bucket[node_id] = _Entry(node_id, clamped[1] - offset, clamped[0] - offset)
                    entry.failures = failures
                if clamped != (at, first_seen):
                    changed.append(node_id)
            for until, node_id in sorted(bans):  # in expiry order, as _prune expects
                self._bans[node_id] = until
            for dropped_at, first_seen, node_id in sorted(dropped):
                self._dropped[node_id] = (first_seen, dropped_at)
        return changed


def _number(value) -> float | None:
    """A saved time as a float, or None if it isn't a finite number."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value)


def _parse_row(row) -> tuple | None:
    """A saved row as (node ID, state, first seen or None, at, failures), or None if it is malformed."""
    if not isinstance(row, (tuple, list)) or len(row) != 5 or not is_id(row[0]):
        return None
    node_id, state, first_seen, at, failures = row
    at, first = _number(at), _number(first_seen)
    if at is None or isinstance(state, bool) or state not in (IN_TABLE, DROPPED, BANNED):
        return None
    if state == BANNED:
        return bytes(node_id), state, None, at, 0
    if first is None or isinstance(failures, bool) or not isinstance(failures, int) or failures < 0:
        return None
    return bytes(node_id), state, first, at, failures
