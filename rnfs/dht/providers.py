"""Provider records stored for other nodes (specification section 6). No RNS dependency.

A record says "publisher provides key". Records expire PROVIDER_TTL after they were last stored and keep
their first-stored time across republishes. At most max_records are kept, a tenth of them in a newcomer
pool. When the rest is full, a publisher seen before evicts a record of the publisher first seen most
recently, so a flood of new identities can't displace older publishers; a publisher not seen before (or the
one seen most recently) stores into the pool instead, evicting a random pooled record when it is full, so
a new publisher can always store something. This node's own records are always accepted.

Local limits, not part of the protocol, bound what one identity can take: a publisher holds at most
max_records // PUBLISHER_SHARE records once it has been seen for `seniority`, and a tenth of that before (so
one long-lived identity can't fill the store and lock everyone else out, and a fresh one gets little; this
node's own records are exempt); and a key has at most MAX_PUBLISHERS_PER_KEY publishers (so /find replies
stay cheap however many identities advertise one key). A newcomer to a full key evicts one of its publishers
at random, never one of the K/2 oldest records: refusing newcomers would let early Sybil identities hold the
key, and the random half of /find replies relies on newcomers getting stored. A publisher's first-seen time
is kept for a TTL after its last record goes, so one that was away for a while isn't new again.

Times are wall clock and persisted, so a time in the future (the clock ran ahead, then was corrected) is
clamped to now on load and at each expire(); otherwise such records would outlive their TTL.
"""
import heapq
import itertools
import json
import os
import random
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable

from rnfs import fsutil
from rnfs.constants import ID_LENGTH, K, PROVIDER_TTL
from rnfs.dht.routing import check_id

_STATE_VERSION = 1
_EXPIRE_INTERVAL = 60  # when full, scan for expired records at most this often (seconds), not on every store
PUBLISHER_SHARE = 100  # a publisher may hold at most max_records // PUBLISHER_SHARE records ...
MIN_PER_PUBLISHER = 10  # ... but at least this many, so small stores still work
NEWCOMER_SHARE = 10  # until seen for `seniority`, a publisher may hold a tenth of that (at least MIN_PER_PUBLISHER)
POOL_SHARE = 10  # max_records // POOL_SHARE records are a newcomer pool, with random eviction inside it
MAX_PUBLISHERS_PER_KEY = 1000  # publishers recorded per key; a newcomer evicts a random non-senior one
DEPARTED_SHARE = 10  # first-seen times kept for publishers with no records left: at most max_records // this


def _never(node_id: bytes) -> bool:
    """Default exclude predicate: excludes nothing."""
    return False


class _Record:
    """When a record was first stored and last stored (republished), and whether it is in the newcomer pool."""

    __slots__ = ("first_stored", "last_stored", "pooled")

    def __init__(self, first_stored: float, last_stored: float, pooled: bool = False):
        """Record stored at first_stored, last refreshed at last_stored."""
        self.first_stored = first_stored
        self.last_stored = last_stored
        self.pooled = pooled


class _Publisher:
    """A publisher with records here: when it was first seen, which keys it provides, and how many of its
    records are outside the pool. `order` breaks first-seen ties (the one stored later is newer)."""

    __slots__ = ("first_seen", "order", "keys", "main")

    def __init__(self, first_seen: float, order: int):
        """Publisher first seen at first_seen, with no keys yet."""
        self.first_seen = first_seen
        self.order = order
        self.keys: set[bytes] = set()
        self.main = 0


class ProviderStore:
    """Provider records, publishers' first-seen times, and /find reply selection. Thread-safe.

    `clock` is wall-clock seconds, since times are persisted. `state_path` (None: memory only) is where
    save() writes records and first-seen times as JSON; it is loaded on construction. `rng` picks the
    random half of /find replies, the publisher a full key evicts and the pooled record a newcomer evicts.
    """

    def __init__(self, max_records: int, state_path: str | None, ttl: float = PROVIDER_TTL,
                 clock: Callable[[], float] = time.time, rng: random.Random = random,
                 max_per_publisher: int | None = None, max_per_key: int = MAX_PUBLISHERS_PER_KEY,
                 own_id: bytes | None = None, seniority: float = 0.0):
        """Store of at most max_records records, loaded from state_path if it exists.

        max_per_publisher defaults to max(MIN_PER_PUBLISHER, max_records // PUBLISHER_SHARE); a publisher
        seen for less than `seniority` seconds (the node passes provider_seniority; 0: no ramp-up) may hold
        newcomer_per_publisher, a tenth of it. own_id's records (the node recording itself) aren't limited by
        either, and are never refused for room: they are as many as the node provides.
        """
        self.max_records = max_records
        self.max_per_publisher = (max_per_publisher if max_per_publisher is not None
                                  else max(MIN_PER_PUBLISHER, max_records // PUBLISHER_SHARE))
        self.newcomer_per_publisher = min(self.max_per_publisher,
                                          max(MIN_PER_PUBLISHER, self.max_per_publisher // NEWCOMER_SHARE))
        self.pool_size = max(0, max_records) // POOL_SHARE
        self.main_size = max(0, max_records - self.pool_size)
        self.max_per_key = max_per_key
        self.own_id = own_id
        self.seniority = seniority
        self.state_path = state_path
        self.ttl = ttl
        self.clock = clock
        self.rng = rng
        self._records: dict[bytes, dict[bytes, _Record]] = {}  # key -> publisher -> record
        self._publishers: dict[bytes, _Publisher] = {}
        self._order = itertools.count()  # first-seen tie-breaker
        # Publishers with records outside the pool, newest first: (-first_seen, -order, publisher). Entries go
        # stale as publishers lose their last such record and are dropped when they reach the top
        self._newest: list[tuple[float, int, bytes]] = []
        self._pool: list[tuple[bytes, bytes]] = []  # pooled (key, publisher) records, for a random pick
        self._pool_index: dict[tuple[bytes, bytes], int] = {}  # position in _pool
        # Publishers whose last record went: publisher -> (first seen, when it went), oldest departure first
        self._departed: OrderedDict[bytes, tuple[float, float]] = OrderedDict()
        self._departed_size = max(MIN_PER_PUBLISHER, max(0, max_records) // DEPARTED_SHARE)
        self._count = 0
        self._main_count = 0
        self._dirty = False
        self._last_expire = float("-inf")
        self._lock = threading.RLock()
        self._save_lock = threading.Lock()  # one save at a time, so an older snapshot never lands last
        if state_path is not None:
            self._load()

    # Storing and withdrawing

    def store(self, key: bytes, publisher: bytes) -> bool:
        """Record that publisher provides key, or refresh its record. False if refused.

        Refused if the publisher already holds its allowance (max_per_publisher, or newcomer_per_publisher
        while it has been seen for less than `seniority`; expired records don't count; own_id is exempt). A
        key with max_per_key publishers evicts one at random, never one of the K/2 oldest records (those
        /find replies list first). Then, if the store outside the pool is full, a publisher seen before the
        newest one there evicts a record of that newest publisher; any other goes into the pool, evicting a
        random pooled record if it is full (refused if that record is its own, so it can't churn its own
        records). So new identities can't displace older publishers, and a new publisher always gets in.
        """
        check_id(key)
        check_id(publisher)
        with self._lock:
            now = self.clock()
            record = self._live_record(key, publisher, now)
            if record is not None:
                record.last_stored = now
                if record.pooled and self._main_count < self.main_size:
                    self._unpool(key, publisher, record)  # room outside the pool: it leaves random eviction
                self._dirty = True
                return True
            # An expired record still counted is replaced, starting a new first-stored time
            self._remove(key, publisher)
            if not self._within_limits(key, publisher, now):
                return False

            if self._count >= self.max_records and now - self._last_expire >= _EXPIRE_INTERVAL:
                self._expire(now)
            pooled = self._make_room(publisher, now)
            if pooled is None:
                return False

            entry = self._publishers.get(publisher)
            if entry is None:
                first_seen = self._departed_first_seen(publisher, now)
                self._departed.pop(publisher, None)
                entry = self._publishers[publisher] = _Publisher(now if first_seen is None else first_seen,
                                                                 next(self._order))
            entry.keys.add(key)
            record = self._records.setdefault(key, {})[publisher] = _Record(now, now, pooled)
            self._count += 1
            if pooled:
                self._pool_index[(key, publisher)] = len(self._pool)
                self._pool.append((key, publisher))
            else:
                self._add_main(publisher, entry)
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

    def allowance(self, publisher: bytes) -> int | None:
        """How many records publisher may hold now: None (no limit) for own_id."""
        with self._lock:
            return self._allowance(publisher, self.clock())

    def _allowance(self, publisher: bytes, now: float) -> int | None:
        """allowance() with the lock held."""
        if publisher == self.own_id:
            return None
        entry = self._publishers.get(publisher)
        first_seen = entry.first_seen if entry is not None else self._departed_first_seen(publisher, now)
        if first_seen is not None and now - first_seen >= self.seniority:
            return self.max_per_publisher
        return self.newcomer_per_publisher

    def _within_limits(self, key: bytes, publisher: bytes, now: float) -> bool:
        """True if publisher may add a record for key under the per-publisher limit; makes room under the
        per-key limit by evicting a random non-senior publisher of key.

        Expired records are dropped first where a limit is reached, so they don't count. Caller holds the lock.
        """
        entry = self._publishers.get(publisher)
        allowance = self._allowance(publisher, now)
        if entry is not None and allowance is not None and len(entry.keys) >= allowance:
            self._drop_expired([(owned, publisher) for owned in entry.keys], now)
            if publisher in self._publishers and len(self._publishers[publisher].keys) >= allowance:
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

    def _make_room(self, publisher: bytes, now: float) -> bool | None:
        """Where a new record of publisher goes: False outside the pool, True in it, None if refused. Evicts
        what it must. Caller holds the lock."""
        if self._main_count < self.main_size:
            return False
        own = publisher == self.own_id
        entry = self._publishers.get(publisher)
        if entry is not None:
            rank = self._rank(entry)
        else:
            # Back within the grace: as old as when first seen, and older than anyone first seen at that time
            first_seen = self._departed_first_seen(publisher, now)
            rank = None if first_seen is None else (first_seen, -1)
        newest = self._newest_main(skip={publisher, self.own_id}) if own or rank is not None else None
        if newest is not None and (own or rank < self._rank(newest[1])):
            # Seen before the newest publisher outside the pool: that one loses the record it refreshed least
            # recently (own records always get in this way)
            victim_id, victim = newest
            key = min((k for k in victim.keys if not self._records[k][victim_id].pooled),
                      key=lambda k: self._records[k][victim_id].last_stored)
            self._remove(key, victim_id)
            return False
        if self.pool_size <= 0:
            return None
        if len(self._pool) < self.pool_size:
            return True
        key, victim_id = self._pool[self.rng.randrange(len(self._pool))]
        if victim_id == publisher and not own:
            return None  # a publisher can't churn its own pooled records
        self._remove(key, victim_id)
        return True

    @staticmethod
    def _rank(entry: _Publisher) -> tuple[float, int]:
        """Sort key for first-seen order: lower was seen earlier."""
        return entry.first_seen, entry.order

    def _newest_main(self, skip: set) -> tuple[bytes, _Publisher] | None:
        """The publisher first seen most recently that has a record outside the pool, leaving out skip; None if
        there is none. Caller holds the lock."""
        aside = []
        found = None
        while self._newest:
            neg_seen, neg_order, publisher = self._newest[0]
            entry = self._publishers.get(publisher)
            if entry is None or entry.main <= 0 or (-neg_seen, -neg_order) != self._rank(entry):
                heapq.heappop(self._newest)  # stale: gone, pooled only, or re-added since
                continue
            if publisher in skip:
                aside.append(heapq.heappop(self._newest))
                continue
            found = publisher, entry
            break
        for item in aside:
            heapq.heappush(self._newest, item)
        return found

    def _add_main(self, publisher: bytes, entry: _Publisher):
        """Count a record of publisher outside the pool. Caller holds the lock."""
        self._main_count += 1
        entry.main += 1
        if entry.main == 1:
            heapq.heappush(self._newest, (-entry.first_seen, -entry.order, publisher))
            if len(self._newest) > 2 * len(self._publishers) + 64:
                self._rebuild_newest()

    def _rebuild_newest(self):
        """Recreate the newest-publisher heap without stale entries. Caller holds the lock."""
        self._newest = [(-entry.first_seen, -entry.order, publisher)
                        for publisher, entry in self._publishers.items() if entry.main > 0]
        heapq.heapify(self._newest)

    def _unpool(self, key: bytes, publisher: bytes, record: _Record):
        """Move a pooled record out of the pool. Caller holds the lock."""
        self._drop_from_pool(key, publisher)
        record.pooled = False
        self._add_main(publisher, self._publishers[publisher])

    def _drop_from_pool(self, key: bytes, publisher: bytes):
        """Forget a pooled record's place in the pool, swapping the last one in. Caller holds the lock."""
        index = self._pool_index.pop((key, publisher))
        last = self._pool.pop()
        if index < len(self._pool):
            self._pool[index] = last
            self._pool_index[last] = index

    def _remove(self, key: bytes, publisher: bytes) -> bool:
        """Delete one record, and the publisher once it has none left. Caller holds the lock."""
        records = self._records.get(key)
        if records is None:
            return False
        record = records.pop(publisher, None)
        if record is None:
            return False
        if not records:
            del self._records[key]
        self._count -= 1
        entry = self._publishers[publisher]
        entry.keys.discard(key)
        if record.pooled:
            self._drop_from_pool(key, publisher)
        else:
            self._main_count -= 1
            entry.main -= 1
        if not entry.keys:
            del self._publishers[publisher]
            self._depart(publisher, entry.first_seen, self.clock())
        return True

    def _depart(self, publisher: bytes, first_seen: float, now: float):
        """Keep the first-seen time of a publisher whose last record went, for a TTL, if it had been seen for
        `seniority` (a younger one has no standing to lose, and leaving those out means a flood of fresh
        identities can't push the others out). Caller holds the lock."""
        self._departed.pop(publisher, None)
        if now - first_seen < self.seniority:
            return
        self._departed[publisher] = (first_seen, now)
        while len(self._departed) > self._departed_size:
            self._departed.popitem(last=False)

    def _departed_first_seen(self, publisher: bytes, now: float) -> float | None:
        """First-seen time of a publisher whose last record went less than a TTL ago, else None. Caller holds
        the lock."""
        departed = self._departed.get(publisher)
        if departed is None or now - departed[1] >= self.ttl:
            return None
        return departed[0]

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
        # Departures are in order, oldest first
        while self._departed and now - next(iter(self._departed.values()))[1] >= self.ttl:
            self._departed.popitem(last=False)
            self._dirty = True
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
        moved = False
        for entry in self._publishers.values():
            if entry.first_seen > now:
                entry.first_seen = now
                moved = True
        for publisher, (first_seen, departed) in list(self._departed.items()):
            if first_seen > now or departed > now:
                self._departed[publisher] = (min(first_seen, now), min(departed, now))
                self._dirty = True
        if moved:
            self._rebuild_newest()
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
                    "records": [[key.hex(), publisher.hex(), record.first_stored, record.last_stored,
                                 int(record.pooled)]
                                for key, records in self._records.items() for publisher, record in records.items()],
                    "departed": [[publisher.hex(), first_seen, departed]
                                 for publisher, (first_seen, departed) in self._departed.items()],
                }
                self._dirty = False
            directory = os.path.dirname(os.path.abspath(self.state_path))
            os.makedirs(directory, exist_ok=True)
            try:
                fsutil.write_atomic(self.state_path, json.dumps(state, separators=(",", ":")).encode(), mode=0o600)
            except BaseException:
                with self._lock:
                    self._dirty = True  # try again next time
                raise

    def _load(self):
        """Load state_path if it exists. A missing or unreadable file starts empty: records get republished.

        Files written before the newcomer pool (four fields per record, no departures) load with every record
        outside the pool, as far as it has room.
        """
        try:
            with open(self.state_path, encoding="utf-8") as f:
                state = json.load(f)
            if state.get("version") != _STATE_VERSION:
                return
            first_seen = {bytes.fromhex(p): float(t) for p, t in state["publishers"].items()}
            records = [(bytes.fromhex(r[0]), bytes.fromhex(r[1]), float(r[2]), float(r[3]),
                        len(r) > 4 and bool(r[4])) for r in state["records"]]
            departed = [(bytes.fromhex(p), float(seen), float(left)) for p, seen, left in state.get("departed", [])]
        except (OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError, IndexError):
            return
        now = self.clock()
        # Times in the future (a clock that ran ahead, since corrected) count as now; NaN is dropped
        first_seen = {p: min(t, now) for p, t in first_seen.items() if t == t}
        records = [(k, p, min(first, now), min(last, now), pooled) for k, p, first, last, pooled in records
                   if first == first and last == last]
        # Rebuild first-seen order, oldest publisher first
        for publisher, seen in sorted(first_seen.items(), key=lambda item: item[1]):
            if len(publisher) == ID_LENGTH:
                self._publishers[publisher] = _Publisher(seen, next(self._order))
        # Oldest records first, so each key's records are in first-stored order and the limits keep the oldest
        for key, publisher, first, last, pooled in sorted(records, key=lambda record: record[2]):
            if (len(key) != ID_LENGTH or publisher not in self._publishers or now - last >= self.ttl
                    or publisher in self._records.get(key, {})
                    or (publisher != self.own_id
                        and len(self._publishers[publisher].keys) >= self.max_per_publisher)
                    or len(self._records.get(key, ())) >= self.max_per_key):
                continue
            # Where it was if there is room there, else wherever there is
            main_room = self._main_count < self.main_size
            pool_room = len(self._pool) < self.pool_size
            if not (main_room or pool_room):
                continue
            pooled = pool_room if pooled or not main_room else False
            entry = self._publishers[publisher]
            self._records.setdefault(key, {})[publisher] = _Record(first, last, pooled)
            entry.keys.add(key)
            self._count += 1
            if pooled:
                self._pool_index[(key, publisher)] = len(self._pool)
                self._pool.append((key, publisher))
            else:
                self._main_count += 1
                entry.main += 1
        for publisher in [p for p, entry in self._publishers.items() if not entry.keys]:
            del self._publishers[publisher]
        self._rebuild_newest()
        for publisher, seen, left in sorted(departed, key=lambda item: item[2]):
            if (len(publisher) == ID_LENGTH and seen == seen and left == left and publisher not in self._publishers
                    and now - min(left, now) < self.ttl):
                self._depart(publisher, min(seen, now), min(left, now))
