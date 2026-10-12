"""Provider records stored for other nodes (specification section 6). No RNS dependency.

A record says "publisher provides key". Records expire PROVIDER_TTL after they were last stored and keep
their first-stored time across republishes. At most max_records are kept: when full, records of the
publisher first seen most recently go first, so a flood of new identities displaces only itself.

Two local limits, not part of the protocol, bound what one identity can take: a publisher holds at most
max_records // PUBLISHER_SHARE records (so one long-lived identity can't fill the store and lock everyone
else out; this node's own records are exempt), and a key has at most MAX_PUBLISHERS_PER_KEY publishers (so
/find replies stay cheap however many identities advertise one key). A newcomer to a full key evicts one of
its publishers at random, never one of the K/2 oldest records: refusing newcomers would let early Sybil
identities hold the key, and the random half of /find replies relies on newcomers getting stored.

Times are wall clock and persisted, so a time in the future (the clock ran ahead, then was corrected) is
clamped to now on load and at each expire(); otherwise such records would outlive their TTL.
"""
import json
import os
import random
import tempfile
import threading
import time
from collections.abc import Callable, Iterable

from rnfs import fsutil
from rnfs.constants import ID_LENGTH, K, PROVIDER_TTL
from rnfs.dht.routing import check_id

_STATE_VERSION = 1
_EXPIRE_INTERVAL = 60  # when full, scan for expired records at most this often (seconds), not on every store
PUBLISHER_SHARE = 100  # a publisher may hold at most max_records // PUBLISHER_SHARE records ...
MIN_PER_PUBLISHER = 10  # ... but at least this many, so small stores still work
MAX_PUBLISHERS_PER_KEY = 1000  # publishers recorded per key; a newcomer evicts a random non-senior one


def _never(node_id: bytes) -> bool:
    """Default exclude predicate: excludes nothing."""
    return False


class _Record:
    """When a record was first stored and last stored (republished)."""

    __slots__ = ("first_stored", "last_stored")

    def __init__(self, first_stored: float, last_stored: float):
        """Record stored at first_stored, last refreshed at last_stored."""
        self.first_stored = first_stored
        self.last_stored = last_stored


class _Publisher:
    """A publisher with records here: when it was first seen and which keys it provides."""

    __slots__ = ("first_seen", "keys")

    def __init__(self, first_seen: float):
        """Publisher first seen at first_seen, with no keys yet."""
        self.first_seen = first_seen
        self.keys: set[bytes] = set()


class ProviderStore:
    """Provider records, publishers' first-seen times, and /find reply selection. Thread-safe.

    `clock` is wall-clock seconds, since times are persisted. `state_path` (None: memory only) is where
    save() writes records and first-seen times as JSON; it is loaded on construction. `rng` picks the
    random half of /find replies and the publisher a full key evicts.
    """

    def __init__(self, max_records: int, state_path: str | None, ttl: float = PROVIDER_TTL,
                 clock: Callable[[], float] = time.time, rng: random.Random = random,
                 max_per_publisher: int | None = None, max_per_key: int = MAX_PUBLISHERS_PER_KEY,
                 own_id: bytes | None = None):
        """Store of at most max_records records, loaded from state_path if it exists.

        max_per_publisher defaults to max(MIN_PER_PUBLISHER, max_records // PUBLISHER_SHARE). own_id's
        records (the node recording itself) aren't limited by it: they are as many as the node provides.
        """
        self.max_records = max_records
        self.max_per_publisher = (max_per_publisher if max_per_publisher is not None
                                  else max(MIN_PER_PUBLISHER, max_records // PUBLISHER_SHARE))
        self.max_per_key = max_per_key
        self.own_id = own_id
        self.state_path = state_path
        self.ttl = ttl
        self.clock = clock
        self.rng = rng
        self._records: dict[bytes, dict[bytes, _Record]] = {}  # key -> publisher -> record
        # Insertion order is first-seen order, so the publisher seen most recently is the last one
        self._publishers: dict[bytes, _Publisher] = {}
        self._count = 0
        self._dirty = False
        self._last_expire = float("-inf")
        self._lock = threading.RLock()
        self._save_lock = threading.Lock()  # one save at a time, so an older snapshot never lands last
        if state_path is not None:
            self._load()

    # Storing and withdrawing

    def store(self, key: bytes, publisher: bytes) -> bool:
        """Record that publisher provides key, or refresh its record. False if refused.

        Refused if the publisher already holds max_per_publisher records (expired records don't count;
        own_id is exempt). A key with max_per_key publishers evicts one at random, never one of the K/2
        oldest records (those /find replies list first). Then a full store evicts a record of the
        publisher first seen most recently, unless that is the newcomer itself (or the newcomer is a new
        publisher, hence the newest): then it is refused, so new identities can neither displace older
        publishers nor churn their own records.
        """
        check_id(key)
        check_id(publisher)
        with self._lock:
            now = self.clock()
            record = self._live_record(key, publisher, now)
            if record is not None:
                record.last_stored = now
                self._dirty = True
                return True
            # An expired record still counted is replaced, starting a new first-stored time
            self._remove(key, publisher)
            if not self._within_limits(key, publisher, now):
                return False

            if self._count >= self.max_records and now - self._last_expire >= _EXPIRE_INTERVAL:
                self._expire(now)
            if self._count >= self.max_records:
                if not self._evict_for(publisher):
                    return False

            entry = self._publishers.get(publisher)
            if entry is None:
                entry = self._publishers[publisher] = _Publisher(now)
            entry.keys.add(key)
            self._records.setdefault(key, {})[publisher] = _Record(now, now)
            self._count += 1
            self._dirty = True
            return True

    def withdraw(self, key: bytes, publisher: bytes) -> bool:
        """Remove publisher's record for key. True if there was one."""
        with self._lock:
            removed = self._remove(key, publisher)
            self._dirty = self._dirty or removed
            return removed

    def provide(self, publisher: bytes, keys: Iterable[bytes], withdrawn: Iterable[bytes] = ()) -> int:
        """Apply one /provide batch from publisher: withdraw `withdrawn`, then store `keys`.

        Returns how many of `keys` were recorded (the rest were refused: the store is full or a limit applied).
        """
        check_id(publisher)
        keys, withdrawn = list(keys), list(withdrawn)
        for key in keys + withdrawn:
            check_id(key)
        with self._lock:
            for key in withdrawn:
                self.withdraw(key, publisher)
            return sum(self.store(key, publisher) for key in keys)

    def _within_limits(self, key: bytes, publisher: bytes, now: float) -> bool:
        """True if publisher may add a record for key under the per-publisher limit; makes room under the
        per-key limit by evicting a random non-senior publisher of key.

        Expired records are dropped first where a limit is reached, so they don't count. Caller holds the lock.
        """
        entry = self._publishers.get(publisher)
        if entry is not None and len(entry.keys) >= self.max_per_publisher and publisher != self.own_id:
            self._drop_expired([(owned, publisher) for owned in entry.keys], now)
            if publisher in self._publishers and len(self._publishers[publisher].keys) >= self.max_per_publisher:
                return False
        records = self._records.get(key)
        if records is not None and len(records) >= self.max_per_key:
            self._drop_expired([(key, other) for other in records], now)
            records = self._records.get(key, {})
            if len(records) >= self.max_per_key:
                # Records are in first-stored order: the K/2 oldest (the half /find replies favour) stay
                senior = min(K // 2, self.max_per_key - 1)
                victim = list(records)[self.rng.randrange(senior, len(records))]
                self._remove(key, victim)
                self._dirty = True
        return True

    def _drop_expired(self, pairs: list[tuple[bytes, bytes]], now: float):
        """Remove those of the (key, publisher) records given that have expired by now. Caller holds the lock."""
        for key, publisher in pairs:
            record = self._records.get(key, {}).get(publisher)
            if record is not None and now - record.last_stored >= self.ttl:
                self._remove(key, publisher)
                self._dirty = True

    def _evict_for(self, publisher: bytes) -> bool:
        """Make room for a record of publisher by evicting one of the newest publisher's. False if refused."""
        newest = next(reversed(self._publishers), None)
        if newest is None or newest == publisher or publisher not in self._publishers:
            return False
        # Of the newest publisher's records, drop the one it refreshed least recently
        entry = self._publishers[newest]
        victim = min(entry.keys, key=lambda key: self._records[key][newest].last_stored)
        self._remove(victim, newest)
        return True

    def _remove(self, key: bytes, publisher: bytes) -> bool:
        """Delete one record, and the publisher once it has none left. Caller holds the lock."""
        records = self._records.get(key)
        if records is None or records.pop(publisher, None) is None:
            return False
        if not records:
            del self._records[key]
        self._count -= 1
        entry = self._publishers[publisher]
        entry.keys.discard(key)
        if not entry.keys:
            del self._publishers[publisher]  # spec: first-seen times are dropped once no records are left
        return True

    def _live_record(self, key: bytes, publisher: bytes, now: float) -> _Record | None:
        """The unexpired record of publisher for key, or None. Caller holds the lock."""
        record = self._records.get(key, {}).get(publisher)
        if record is None or now - record.last_stored >= self.ttl:
            return None
        return record

    # Expiry

    def expire(self) -> int:
        """Remove every expired record. Returns how many were removed."""
        with self._lock:
            return self._expire(self.clock())

    def _expire(self, now: float) -> int:
        """Remove records last stored ttl or more before now, clamping times in the future. Caller holds the lock."""
        self._last_expire = now
        self._clamp(now)
        expired = [(key, publisher) for key, records in self._records.items()
                   for publisher, record in records.items() if now - record.last_stored >= self.ttl]
        for key, publisher in expired:
            self._remove(key, publisher)
        self._dirty = self._dirty or bool(expired)
        return len(expired)

    def _clamp(self, now: float):
        """Bring every time after now back to now (the clock was corrected backwards). Caller holds the lock."""
        for records in self._records.values():
            for record in records.values():
                if record.last_stored > now:
                    record.last_stored = now
                    self._dirty = True
                if record.first_stored > now:
                    record.first_stored = now
                    self._dirty = True
        for entry in self._publishers.values():
            if entry.first_seen > now:
                entry.first_seen = now
                self._dirty = True

    # Queries

    def for_reply(self, key: bytes, exclude: Callable[[bytes], bool] = _never) -> list[bytes]:
        """Up to K providers of key for a /find reply, leaving out expired and excluded (banned) ones.

        The K/2 with the oldest records come first, favouring long-standing providers; then providers
        chosen at random from the rest, so early squatters can't hold every slot. exclude is only asked
        about candidates as they are chosen, not about every provider of the key.
        """
        candidates = self._live_unsorted(key)  # first-stored order: records are kept in insertion order
        reply = []
        index = 0
        while len(reply) < K // 2 and index < len(candidates):
            if not exclude(candidates[index]):
                reply.append(candidates[index])
            index += 1
        rest = candidates[index:]
        while len(reply) < K and rest:
            # Pick one at random and remove it by swapping in the last, so each is considered once
            pick = self.rng.randrange(len(rest))
            rest[pick], rest[-1] = rest[-1], rest[pick]
            candidate = rest.pop()
            if not exclude(candidate):
                reply.append(candidate)
        return reply

    def providers(self, key: bytes, exclude: Callable[[bytes], bool] = _never) -> list[bytes]:
        """Every unexpired, non-excluded provider recorded here for key, oldest record first."""
        return self._live(key, exclude)

    def _live_unsorted(self, key: bytes) -> list[bytes]:
        """Unexpired providers of key in the order their records were stored (at most max_per_key)."""
        with self._lock:
            now = self.clock()
            return [publisher for publisher, record in self._records.get(key, {}).items()
                    if now - record.last_stored < self.ttl]

    def _live(self, key: bytes, exclude: Callable[[bytes], bool]) -> list[bytes]:
        """Unexpired, non-excluded providers of key, oldest record first."""
        with self._lock:
            now = self.clock()
            records = [(record.first_stored, publisher) for publisher, record in self._records.get(key, {}).items()
                       if now - record.last_stored < self.ttl]
        records.sort()
        # exclude may consult other locked state (the routing table's bans), so call it outside our lock
        return [publisher for _, publisher in records if not exclude(publisher)]

    def record_count(self) -> int:
        """Number of records held, including expired ones not yet removed by expire()."""
        with self._lock:
            return self._count

    def publisher_count(self) -> int:
        """Number of publishers with records here."""
        with self._lock:
            return len(self._publishers)

    def first_seen(self, publisher: bytes) -> float | None:
        """When publisher was first seen (clock time), or None if it has no records here."""
        with self._lock:
            entry = self._publishers.get(publisher)
            return None if entry is None else entry.first_seen

    # Persistence

    def save(self):
        """Write records and first-seen times to state_path atomically, if anything changed since the last save.

        Call it periodically and at shutdown; storing doesn't save, since a full store is megabytes.
        """
        if self.state_path is None:
            return
        # Saves come from the maintenance thread and from stop(): one at a time, or a snapshot taken before
        # the other's could be written last, with _dirty already cleared
        with self._save_lock:
            with self._lock:
                if not self._dirty:
                    return
                state = {
                    "version": _STATE_VERSION,
                    "publishers": {publisher.hex(): entry.first_seen
                                   for publisher, entry in self._publishers.items()},
                    "records": [[key.hex(), publisher.hex(), record.first_stored, record.last_stored]
                                for key, records in self._records.items() for publisher, record in records.items()],
                }
                self._dirty = False
            directory = os.path.dirname(os.path.abspath(self.state_path))
            os.makedirs(directory, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(dir=directory, prefix=".providers-", suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(state, f, separators=(",", ":"))
                    f.flush()
                    os.fsync(f.fileno())
                fsutil.replace(temp_path, self.state_path)
            except BaseException:
                with self._lock:
                    self._dirty = True  # try again next time
                try:
                    fsutil.remove(temp_path)
                except OSError:
                    pass
                raise

    def _load(self):
        """Load state_path if it exists. A missing or unreadable file starts empty: records get republished."""
        try:
            with open(self.state_path, encoding="utf-8") as f:
                state = json.load(f)
            if state.get("version") != _STATE_VERSION:
                return
            first_seen = {bytes.fromhex(p): float(t) for p, t in state["publishers"].items()}
            records = [(bytes.fromhex(k), bytes.fromhex(p), float(first), float(last))
                       for k, p, first, last in state["records"]]
        except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError):
            return
        now = self.clock()
        # Times in the future (a clock that ran ahead, since corrected) count as now; NaN is dropped
        first_seen = {p: min(t, now) for p, t in first_seen.items() if t == t}
        records = [(k, p, min(first, now), min(last, now)) for k, p, first, last in records
                   if first == first and last == last]
        # Rebuild first-seen order, oldest publisher first
        for publisher, seen in sorted(first_seen.items(), key=lambda item: item[1]):
            if len(publisher) == ID_LENGTH:
                self._publishers[publisher] = _Publisher(seen)
        # Oldest records first, so each key's records are in first-stored order and the limits keep the oldest
        for key, publisher, first, last in sorted(records, key=lambda record: record[2]):
            if (len(key) != ID_LENGTH or publisher not in self._publishers or now - last >= self.ttl
                    or self._count >= self.max_records or publisher in self._records.get(key, {})
                    or (publisher != self.own_id
                        and len(self._publishers[publisher].keys) >= self.max_per_publisher)
                    or len(self._records.get(key, ())) >= self.max_per_key):
                continue
            self._records.setdefault(key, {})[publisher] = _Record(first, last)
            self._publishers[publisher].keys.add(key)
            self._count += 1
        for publisher in [p for p, entry in self._publishers.items() if not entry.keys]:
            del self._publishers[publisher]
