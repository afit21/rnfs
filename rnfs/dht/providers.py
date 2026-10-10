"""Provider records stored for other nodes (specification section 6). No RNS dependency.

A record says "publisher provides key". Records expire PROVIDER_TTL after they were last stored and keep
their first-stored time across republishes. At most max_records are kept: when full, records of the
publisher first seen most recently go first, so a flood of new identities displaces only itself.
"""
import json
import os
import random
import tempfile
import threading
import time
from collections.abc import Callable, Iterable

from rnfs.constants import ID_LENGTH, K, PROVIDER_TTL
from rnfs.dht.routing import check_id

_STATE_VERSION = 1
_EXPIRE_INTERVAL = 60  # when full, scan for expired records at most this often (seconds), not on every store


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
    random half of /find replies.
    """

    def __init__(self, max_records: int, state_path: str | None, ttl: float = PROVIDER_TTL,
                 clock: Callable[[], float] = time.time, rng: random.Random = random):
        """Store of at most max_records records, loaded from state_path if it exists."""
        self.max_records = max_records
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
        if state_path is not None:
            self._load()

    # Storing and withdrawing

    def store(self, key: bytes, publisher: bytes) -> bool:
        """Record that publisher provides key, or refresh its record. False if refused because full.

        A full store evicts a record of the publisher first seen most recently, unless that is the
        newcomer itself (or the newcomer is a new publisher, hence the newest): then it is refused,
        so new identities can neither displace older publishers nor churn their own records.
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

        Returns how many of `keys` were recorded (the rest were refused because the store is full).
        """
        check_id(publisher)
        keys, withdrawn = list(keys), list(withdrawn)
        for key in keys + withdrawn:
            check_id(key)
        with self._lock:
            for key in withdrawn:
                self.withdraw(key, publisher)
            return sum(self.store(key, publisher) for key in keys)

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
        """Remove records last stored ttl or more before now. Caller holds the lock."""
        self._last_expire = now
        expired = [(key, publisher) for key, records in self._records.items()
                   for publisher, record in records.items() if now - record.last_stored >= self.ttl]
        for key, publisher in expired:
            self._remove(key, publisher)
        self._dirty = self._dirty or bool(expired)
        return len(expired)

    # Queries

    def for_reply(self, key: bytes, exclude: Callable[[bytes], bool] = _never) -> list[bytes]:
        """Up to K providers of key for a /find reply, leaving out expired and excluded (banned) ones.

        The K/2 with the oldest records come first, favouring long-standing providers; then providers
        chosen at random from the rest, so early squatters can't hold every slot.
        """
        live = self._live(key, exclude)
        oldest, rest = live[:K // 2], live[K // 2:]
        return oldest + self.rng.sample(rest, min(K - len(oldest), len(rest)))

    def providers(self, key: bytes, exclude: Callable[[bytes], bool] = _never) -> list[bytes]:
        """Every unexpired, non-excluded provider recorded here for key, oldest record first."""
        return self._live(key, exclude)

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
        with self._lock:
            if not self._dirty:
                return
            state = {
                "version": _STATE_VERSION,
                "publishers": {publisher.hex(): entry.first_seen for publisher, entry in self._publishers.items()},
                "records": [[key.hex(), publisher.hex(), record.first_stored, record.last_stored]
                            for key, records in self._records.items() for publisher, record in records.items()],
            }
            self._dirty = False
        directory = os.path.dirname(os.path.abspath(self.state_path))
        os.makedirs(directory, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(dir=directory, prefix=".providers-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(state, f, separators=(",", ":"))
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, self.state_path)
        except BaseException:
            with self._lock:
                self._dirty = True  # try again next time
            try:
                os.unlink(temp_path)
            except OSError:
                pass
            raise

    def _load(self):
        """Load state_path if it exists. A missing or unreadable file starts empty: records get republished."""
        try:
            with open(self.state_path) as f:
                state = json.load(f)
            if state.get("version") != _STATE_VERSION:
                return
            first_seen = {bytes.fromhex(p): float(t) for p, t in state["publishers"].items()}
            records = [(bytes.fromhex(k), bytes.fromhex(p), float(first), float(last))
                       for k, p, first, last in state["records"]]
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            return
        now = self.clock()
        # Rebuild first-seen order, oldest publisher first
        for publisher, seen in sorted(first_seen.items(), key=lambda item: item[1]):
            if len(publisher) == ID_LENGTH:
                self._publishers[publisher] = _Publisher(seen)
        for key, publisher, first, last in records:
            if (len(key) != ID_LENGTH or publisher not in self._publishers or now - last >= self.ttl
                    or self._count >= self.max_records or publisher in self._records.get(key, {})):
                continue
            self._records.setdefault(key, {})[publisher] = _Record(first, last)
            self._publishers[publisher].keys.add(key)
            self._count += 1
        for publisher in [p for p, entry in self._publishers.items() if not entry.keys]:
            del self._publishers[publisher]
