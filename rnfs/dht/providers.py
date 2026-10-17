"""Provider records stored for other nodes (specification section 6). No network access (RNS is loaded
only through rnfs.log, for logging).

A record says "publisher provides key". Records expire PROVIDER_TTL after they were last stored and keep
their first-stored time across republishes. At most max_records are kept, a tenth of them in a newcomer
pool. When the rest is full, a publisher seen before evicts a record of the publisher first seen most
recently, so a flood of new identities can't displace older publishers; a publisher not seen before (or the
one seen most recently) stores into the pool instead. A full pool evicts a record of the publisher holding the
most pooled records (one of them at random, if several hold as many), never the storer's own, so a new
publisher can always store something and a flood of identities evens out the pool rather than emptying it of
everyone else's records. This node's own records are always accepted and never evicted: they are what the node
provides, and its /find replies list it as a provider from them.

Local limits, not part of the protocol, bound what one identity can take: a publisher holds at most
max_records // PUBLISHER_SHARE records once it has been seen for `seniority`, and a tenth of that before (so
one long-lived identity can't fill the store and lock everyone else out, and a fresh one gets little; this
node's own records are exempt); and a key has at most MAX_PUBLISHERS_PER_KEY publishers besides this node (so
/find replies stay cheap however many identities advertise one key). A newcomer to a full key evicts one of its
publishers at random, never one of the K/2 oldest records nor this node's own: refusing newcomers would let early
Sybil identities hold the key, and the random half of /find replies relies on newcomers getting stored. A
publisher's first-seen time is kept for a TTL after its last record goes, so one that was away for a while isn't
new again; that of a newcomer whose last record was evicted from the pool is kept too, in a smaller memory that
forgets the most recently first seen first, so a flood can't keep a new publisher new forever.

Records, first-seen times and departures are persisted in the state database (rnfs.statedb: the
provider_records, provider_publishers, provider_departed and provider_evicted tables) by save(), which the node
calls periodically and at shutdown, writing only the rows changed since the last save; they are read from it
only at startup. Times are wall clock and persisted, so a time in the future (the clock ran ahead, then was
corrected) is clamped to now on load and at each expire(); otherwise such records would outlive their TTL.
"""
import heapq
import itertools
import json
import math
import os
import random
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable

from rnfs import statedb
from rnfs.constants import K, PROVIDER_TTL
from rnfs.dht.routing import check_id, is_id
from rnfs.log import LOG_ERROR, LOG_WARNING, log, plural

_STATE_VERSION = 1  # of providers.json, which rnfs 0.1.4 and earlier wrote
_EXPIRE_INTERVAL = 60  # when full, scan for expired records at most this often (seconds), not on every store
PUBLISHER_SHARE = 100  # a publisher may hold at most max_records // PUBLISHER_SHARE records ...
MIN_PER_PUBLISHER = 10  # ... but at least this many, so small stores still work
NEWCOMER_SHARE = 10  # until seen for `seniority`, a publisher may hold a tenth of that (at least MIN_PER_PUBLISHER)
POOL_SHARE = 10  # max_records // POOL_SHARE records are a newcomer pool, evicting from its biggest holder
MAX_PUBLISHERS_PER_KEY = 1000  # publishers recorded per key; a newcomer evicts a random non-senior one
DEPARTED_SHARE = 10  # first-seen times kept for publishers with no records left: at most max_records // this
EVICTED_SHARE = 10  # ... and for newcomers evicted from the pool: at most pool_size // this (MIN_PER_PUBLISHER)


def _never(node_id: bytes) -> bool:
    """Default exclude predicate: excludes nothing."""
    return False


def _number(value) -> float | None:
    """A saved time as a float, or None if it isn't a number or is NaN (infinities are kept, as the JSON file's
    float() kept them: the load's clamping and TTL check deal with them)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or math.isnan(value):
        return None
    return float(value)


# Migration of providers.json (rnfs 0.1.4 and earlier)


def _parse_state(state) -> tuple[dict[bytes, float], list, list, list]:
    """(first seen by publisher, records, departures, evictions) of a providers.json value, converted as rnfs
    0.1.4 loaded it: records are (key, publisher, first stored, last stored, pooled), files written before the
    newcomer pool having four fields per record and no departures or evictions. Raises ValueError (a version
    other than _STATE_VERSION), KeyError, TypeError, AttributeError, OverflowError or IndexError if anything in it
    is malformed: those versions then started empty."""
    if state.get("version") != _STATE_VERSION:
        raise ValueError(f"version {state.get('version')!r}, not {_STATE_VERSION}")
    first_seen = {bytes.fromhex(p): float(t) for p, t in state["publishers"].items()}
    records = [(bytes.fromhex(r[0]), bytes.fromhex(r[1]), float(r[2]), float(r[3]), len(r) > 4 and bool(r[4]))
               for r in state["records"]]
    departed = [(bytes.fromhex(p), float(seen), float(left)) for p, seen, left in state.get("departed", [])]
    evicted = [(bytes.fromhex(p), float(seen), float(left)) for p, seen, left in state.get("evicted", [])]
    return first_seen, records, departed, evicted


_MALFORMED = (ValueError, KeyError, TypeError, AttributeError, OverflowError, IndexError)  # _parse_state's errors


def read_records(path: str, shape=None):
    """providers.json's value, for statedb.migrate (shape is unused), or None if there is none to import.

    rnfs 0.1.4 started empty on any problem with it, and the next save overwrote it. Now one that isn't JSON, is
    of another version or holds a malformed record is moved aside as damaged (the node starts without its
    records, which publishers republish), and one that can't be read is logged and left, to import at the next
    start.
    """
    try:
        with open(path, encoding="utf-8") as f:
            state = json.load(f)
    except FileNotFoundError:
        return None
    except ValueError as e:
        statedb.set_aside(path, str(e))
        return None
    except OSError as e:
        if os.path.isdir(path):  # Windows reports opening a directory as PermissionError
            statedb.set_aside(path, "it is a directory")
        else:
            log(f"providers: could not read {path} ({e}); starting without its records, and importing it at the "
                "next start", LOG_ERROR)
        return None
    try:
        _parse_state(state)
    except _MALFORMED as e:
        statedb.set_aside(path, f"malformed: {e!r}")
        return None
    return state


def import_records(value: dict, tx) -> int:
    """Import providers.json (statedb.migrate): its publishers, records, departures and evictions replace the
    four provider tables, converted as rnfs 0.1.4 loaded them. What that load dropped whatever the clock and
    limits were (NaN times, IDs of the wrong length, a record repeated: the first stored is kept) is left out;
    the rest (expiry, future times, limits, records of unknown publishers) is the load's. Returns the number of
    records imported."""
    for table in ("provider_publishers", "provider_records", "provider_departed", "provider_evicted"):
        tx.execute(f"DELETE FROM {table}")
    try:
        first_seen, records, departed, evicted = _parse_state(value)
    except _MALFORMED as e:  # read_records checked it: not expected
        log(f"providers: not importing providers.json ({e!r}); starting without its records", LOG_ERROR)
        return 0
    tx.executemany("INSERT INTO provider_publishers (publisher, first_seen) VALUES (?, ?)",
                   [(p, t) for p, t in first_seen.items() if is_id(p) and t == t])
    kept: dict[tuple[bytes, bytes], tuple] = {}
    for key, publisher, first, last, pooled in sorted(records, key=lambda record: record[2]):
        if is_id(key) and is_id(publisher) and first == first and last == last:
            kept.setdefault((key, publisher), (key, publisher, first, last, int(pooled)))
    tx.executemany("INSERT INTO provider_records (key, publisher, first_stored, last_stored, pooled)"
                   " VALUES (?, ?, ?, ?, ?)", list(kept.values()))
    for table, entries in (("provider_departed", departed), ("provider_evicted", evicted)):
        # The load applied them oldest first, a later one replacing an earlier one of the same publisher
        tx.executemany(f"INSERT OR REPLACE INTO {table} (publisher, first_seen, at) VALUES (?, ?, ?)",
                       [(p, seen, left) for p, seen, left in sorted(entries, key=lambda item: item[2])
                        if is_id(p) and seen == seen and left == left])
    return len(kept)


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
    records are outside the pool. `order` breaks first-seen ties (the one stored later is newer).
    `sweep_after`: none of its records can expire before this time (known from its last sweep for expired
    records; later stores only add later expiries), so a sweep before then would find nothing."""

    __slots__ = ("first_seen", "order", "keys", "main", "sweep_after")

    def __init__(self, first_seen: float, order: int):
        """Publisher first seen at first_seen, with no keys yet."""
        self.first_seen = first_seen
        self.order = order
        self.keys: set[bytes] = set()
        self.main = 0
        self.sweep_after = float("-inf")


class ProviderStore:
    """Provider records, publishers' first-seen times, and /find reply selection. Thread-safe.

    `clock` is wall-clock seconds, since times are persisted. `state_dir` (None: memory only) holds the state
    database save() writes records and first-seen times to; they are loaded on construction. `rng` picks the
    random half of /find replies, the publisher a full key evicts and, among the publishers holding the most
    pooled records, the one a full pool evicts from.
    """

    def __init__(self, max_records: int, state_dir: str | None, ttl: float = PROVIDER_TTL,
                 clock: Callable[[], float] = time.time, rng: random.Random = random,
                 max_per_publisher: int | None = None, max_per_key: int = MAX_PUBLISHERS_PER_KEY,
                 own_id: bytes | None = None, seniority: float = 0.0):
        """Store of at most max_records records, loaded from the state database in state_dir if given.

        max_per_publisher defaults to max(MIN_PER_PUBLISHER, max_records // PUBLISHER_SHARE); a publisher
        seen for less than `seniority` seconds (the node passes provider_seniority; 0: no ramp-up) may hold
        newcomer_per_publisher, a tenth of it. own_id's records (the node recording itself) aren't limited by
        either, nor counted toward max_per_key, and are never refused for room or evicted: they are as many as
        the node provides (beyond max_records, in the rare case that it provides more than the store holds).
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
        self.ttl = ttl
        self.clock = clock
        self.rng = rng
        self._records: dict[bytes, dict[bytes, _Record]] = {}  # key -> publisher -> record
        self._publishers: dict[bytes, _Publisher] = {}
        self._order = itertools.count()  # first-seen tie-breaker
        # Publishers with records outside the pool, newest first: (-first_seen, -order, publisher). Entries go
        # stale as publishers lose their last such record and are dropped when they reach the top
        self._newest: list[tuple[float, int, bytes]] = []
        self._pooled: dict[bytes, dict[bytes, None]] = {}  # publisher -> its pooled keys, oldest pooled first
        self._pool_count = 0
        # Publishers by how many pooled records they hold (count -> list, with each one's place in its list), so
        # a full pool finds one of its biggest holders in O(1)
        self._holders: dict[int, list[bytes]] = {}
        self._holder_index: dict[bytes, int] = {}
        self._most_pooled = 0
        # Publishers whose last record went: publisher -> (first seen, when it went), oldest departure first
        self._departed: OrderedDict[bytes, tuple[float, float]] = OrderedDict()
        self._departed_size = max(MIN_PER_PUBLISHER, max(0, max_records) // DEPARTED_SHARE)
        # Newcomers (not seen for `seniority`) whose last record a full pool evicted: publisher -> (first seen,
        # when evicted), oldest eviction first. When full, the one first seen most recently is forgotten, so a
        # flood of fresh identities can't push out a newcomer seen before it
        self._evicted: dict[bytes, tuple[float, float]] = {}
        self._evicted_size = max(MIN_PER_PUBLISHER, self.pool_size // EVICTED_SHARE)
        self._count = 0
        self._main_count = 0
        # Rows changed since the last save (only tracked when persisted): records by (key, publisher), publishers,
        # and publishers whose departed or evicted entry changed. save() writes each as memory has it then, or
        # deletes it if memory has none
        self._dirty_records: set[tuple[bytes, bytes]] = set()
        self._dirty_publishers: set[bytes] = set()
        self._dirty_gone: set[bytes] = set()
        self._last_expire = float("-inf")
        self._lock = threading.RLock()
        self._save_lock = threading.Lock()  # one save at a time, so an older snapshot never lands last
        self._db = None if state_dir is None else statedb.connect(state_dir)
        if self._db is not None:
            try:
                self._load()
            except BaseException:
                self._db.close()
                raise

    # Storing and withdrawing

    def store(self, key: bytes, publisher: bytes) -> bool:
        """Record that publisher provides key, or refresh its record. False if refused.

        Refused if the publisher already holds its allowance (max_per_publisher, or newcomer_per_publisher
        while it has been seen for less than `seniority`; expired records don't count; own_id is exempt). A
        key with max_per_key publishers evicts one at random, never one of the K/2 oldest records (those
        /find replies list first). Then, if the store outside the pool is full, a publisher seen before the
        newest one there evicts a record of that newest publisher; any other goes into the pool. A full pool
        evicts the record pooled first by one of the publishers holding the most pooled records, and refuses a
        publisher already holding that many (so it can't churn its own records). So new identities can't
        displace older publishers, and a new publisher always gets in. own_id's records are never evicted by
        either limit, nor counted toward max_per_key: Sybil stores to a key could otherwise push the node's own
        record out of its own store.
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
                self._touch(key, publisher)
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
                self._forget_gone(publisher)
                entry = self._publishers[publisher] = _Publisher(now if first_seen is None else first_seen,
                                                                 next(self._order))
                self._touch_publisher(publisher)
            entry.keys.add(key)
            record = self._records.setdefault(key, {})[publisher] = _Record(now, now, pooled)
            self._count += 1
            if pooled:
                self._pool_add(key, publisher)
            else:
                self._add_main(publisher, entry)
            self._touch(key, publisher)
            return True

    def withdraw(self, key: bytes, publisher: bytes) -> bool:
        """Remove publisher's record for key. True if there was one."""
        with self._lock:
            return self._remove(key, publisher)

    def provide(self, publisher: bytes, keys: Iterable[bytes], withdrawn: Iterable[bytes] = ()) -> int:
        """Apply one /provide batch from publisher: withdraw `withdrawn`, then store `keys`.

        Returns how many of `keys` were recorded (the rest were refused: the store is full or a limit applied).
        A publisher at its allowance costs little: its expired records are swept at most once per batch (none
        while none can have expired), and each new key is then refused without scanning its records again.
        """
        check_id(publisher)
        keys, withdrawn = list(keys), list(withdrawn)
        for key in keys + withdrawn:
            check_id(key)
        with self._lock:
            for key in withdrawn:
                self.withdraw(key, publisher)
            return sum(self.store(key, publisher) for key in keys)

    def retain(self, keep: Callable[[bytes], bool]) -> int:
        """Remove every record of publishers for which keep(publisher) is False (own_id's are always kept), as
        private mode does with non-members' records. Returns how many records were removed.

        keep is asked once per publisher, outside the lock (it may consult other locked state, like RNS's).
        """
        with self._lock:
            publishers = [publisher for publisher in self._publishers if publisher != self.own_id]
        dropped = {publisher for publisher in publishers if not keep(publisher)}
        removed = 0
        with self._lock:
            for publisher in dropped:
                entry = self._publishers.get(publisher)
                for key in list(entry.keys) if entry is not None else ():
                    removed += self._remove(key, publisher)
                # Not kept as departed either: its standing here came from records it shouldn't have had
                self._forget_gone(publisher)
        return removed

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
            # At its limit: refused at once unless one of its records may have expired since its last sweep
            # (scanning all of them for every key of a refused batch made each refusal cost thousands of checks)
            if now < entry.sweep_after:
                return False
            self._sweep_publisher(publisher, entry, now)
            if publisher in self._publishers and len(self._publishers[publisher].keys) >= allowance:
                return False
        if publisher == self.own_id:
            return True  # the node's own record doesn't count toward max_per_key, so it evicts no one
        records = self._records.get(key)
        # The node's own record is neither counted nor a candidate: otherwise about a thousand Sybil stores to a
        # key it provides would evict it from its own store, and its own /find replies would leave it out
        if records is not None and len(records) - (self.own_id in records) >= self.max_per_key:
            self._drop_expired([(key, other) for other in records], now)
            others = [other for other in self._records.get(key, {}) if other != self.own_id]
            if len(others) >= self.max_per_key:
                # Records are in first-stored order: the K/2 oldest (the half /find replies favour) stay
                senior = min(K // 2, self.max_per_key - 1)
                self._remove(key, others[self.rng.randrange(senior, len(others))])
        return True

    def _sweep_publisher(self, publisher: bytes, entry: _Publisher, now: float):
        """Remove publisher's expired records, and note when the next of the rest expires. Caller holds the lock."""
        earliest = float("inf")
        for key in list(entry.keys):
            record = self._records[key][publisher]
            if now - record.last_stored >= self.ttl:
                self._remove(key, publisher)
            else:
                earliest = min(earliest, record.last_stored)
        entry.sweep_after = earliest + self.ttl

    def _drop_expired(self, pairs: list[tuple[bytes, bytes]], now: float):
        """Remove those of the (key, publisher) records given that have expired by now. Caller holds the lock."""
        for key, publisher in pairs:
            record = self._records.get(key, {}).get(publisher)
            if record is not None and now - record.last_stored >= self.ttl:
                self._remove(key, publisher)

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
        if self._pool_count < self.pool_size:
            return True
        # Fair shares: a publisher already holding as many pooled records as anyone can't push others out (nor
        # churn its own); any other takes a record from one of those holding the most. The node's own records are
        # never taken, nor counted among the most held (it holds what it provides, not a share)
        most = self._most_pooled_by_others()
        if not own and len(self._pooled.get(publisher, ())) >= most:
            return None
        if most == 0:
            return True  # only own records are pooled: the node's own goes over the pool rather than evict its own
        holders = self._holders[most]
        index = self.rng.randrange(len(holders))
        victim_id = holders[index]
        if victim_id == self.own_id:  # at most one entry, and not the only one at `most`
            victim_id = holders[(index + 1) % len(holders)]
        key = next(iter(self._pooled[victim_id]))  # the one it had pooled first
        self._remove(key, victim_id, evicted=True)
        return True

    def _most_pooled_by_others(self) -> int:
        """The most pooled records any publisher but own_id holds (0: none). Caller holds the lock.

        _most_pooled unless own_id alone holds that many: then the next count held, found by stepping down (only
        then, so O(1) otherwise).
        """
        most = self._most_pooled
        while most and self._holders.get(most, [self.own_id]) == [self.own_id]:
            most -= 1
        return most

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

    def _pool_add(self, key: bytes, publisher: bytes):
        """Count a record of publisher in the pool. Caller holds the lock."""
        keys = self._pooled.setdefault(publisher, {})
        keys[key] = None
        self._pool_count += 1
        self._recount(publisher, len(keys) - 1, len(keys))

    def _drop_from_pool(self, key: bytes, publisher: bytes):
        """Forget a pooled record. Caller holds the lock."""
        keys = self._pooled[publisher]
        del keys[key]
        if not keys:
            del self._pooled[publisher]
        self._pool_count -= 1
        self._recount(publisher, len(keys) + 1, len(keys))

    def _recount(self, publisher: bytes, old: int, new: int):
        """Move publisher from the holders of old pooled records to those of new (0: none). Caller holds the lock."""
        if old:
            holders = self._holders[old]
            index = self._holder_index.pop(publisher)
            last = holders.pop()
            if index < len(holders):
                holders[index] = last
                self._holder_index[last] = index
            if not holders:
                del self._holders[old]
        if new:
            holders = self._holders.setdefault(new, [])
            self._holder_index[publisher] = len(holders)
            holders.append(publisher)
        # Counts move by one, so the most held drops by at most one
        self._most_pooled = max(self._most_pooled, new)
        if self._most_pooled and self._most_pooled not in self._holders:
            self._most_pooled -= 1

    def _remove(self, key: bytes, publisher: bytes, evicted: bool = False) -> bool:
        """Delete one record, and the publisher once it has none left (evicted: a full pool took it). Caller holds
        the lock."""
        records = self._records.get(key)
        if records is None:
            return False
        record = records.pop(publisher, None)
        if record is None:
            return False
        if not records:
            del self._records[key]
        self._count -= 1
        self._touch(key, publisher)
        entry = self._publishers[publisher]
        entry.keys.discard(key)
        if record.pooled:
            self._drop_from_pool(key, publisher)
        else:
            self._main_count -= 1
            entry.main -= 1
        if not entry.keys:
            del self._publishers[publisher]
            self._touch_publisher(publisher)
            self._depart(publisher, entry.first_seen, self.clock(), evicted)
        return True

    def _depart(self, publisher: bytes, first_seen: float, now: float, evicted: bool = False):
        """Keep the first-seen time of a publisher whose last record went, for a TTL, if it had been seen for
        `seniority` (a younger one has no standing to lose, and leaving those out means a flood of fresh
        identities can't push the others out). A younger one whose last record a full pool evicted is kept
        in a memory of its own, so being pushed out doesn't make it new again. Caller holds the lock."""
        self._forget_gone(publisher)
        if now - first_seen < self.seniority:
            if evicted:
                self._remember_evicted(publisher, first_seen, now)
            return
        self._departed[publisher] = (first_seen, now)
        self._touch_gone(publisher)
        while len(self._departed) > self._departed_size:
            self._touch_gone(self._departed.popitem(last=False)[0])

    def _remember_evicted(self, publisher: bytes, first_seen: float, now: float):
        """Keep a pool-evicted newcomer's first-seen time; when the memory is full, the one first seen most
        recently (perhaps this one) is forgotten. Caller holds the lock."""
        self._evicted[publisher] = (first_seen, now)
        self._touch_gone(publisher)
        if len(self._evicted) > self._evicted_size:
            newest = max(self._evicted, key=lambda p: self._evicted[p][0])
            del self._evicted[newest]
            self._touch_gone(newest)

    def _forget_gone(self, publisher: bytes):
        """Drop publisher's departed or evicted entry, if it has one. Caller holds the lock."""
        departed = self._departed.pop(publisher, None)
        evicted = self._evicted.pop(publisher, None)
        if departed is not None or evicted is not None:
            self._touch_gone(publisher)

    def _touch(self, key: bytes, publisher: bytes):
        """Note that publisher's record for key changed (or went), for the next save. Caller holds the lock."""
        if self._db is not None:
            self._dirty_records.add((key, publisher))

    def _touch_publisher(self, publisher: bytes):
        """Note that publisher's first-seen time changed (or went), for the next save. Caller holds the lock."""
        if self._db is not None:
            self._dirty_publishers.add(publisher)

    def _touch_gone(self, publisher: bytes):
        """Note that publisher's departed or evicted entry changed (or went), for the next save. Caller holds the
        lock."""
        if self._db is not None:
            self._dirty_gone.add(publisher)

    def _departed_first_seen(self, publisher: bytes, now: float) -> float | None:
        """First-seen time of a publisher whose last record went less than a TTL ago, else None. Caller holds
        the lock."""
        departed = self._departed.get(publisher) or self._evicted.get(publisher)
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
            self._touch_gone(self._departed.popitem(last=False)[0])
        while self._evicted and now - next(iter(self._evicted.values()))[1] >= self.ttl:
            publisher = next(iter(self._evicted))
            del self._evicted[publisher]
            self._touch_gone(publisher)
        return len(expired)

    def _clamp(self, now: float):
        """Bring every time after now back to now (the clock was corrected backwards). Caller holds the lock."""
        stored_moved = False
        for key, records in self._records.items():
            for publisher, record in records.items():
                if record.last_stored > now or record.first_stored > now:
                    stored_moved = stored_moved or record.last_stored > now
                    record.last_stored = min(record.last_stored, now)
                    record.first_stored = min(record.first_stored, now)
                    self._touch(key, publisher)
        moved = False
        for publisher, entry in self._publishers.items():
            if stored_moved:
                entry.sweep_after = float("-inf")  # expiries moved earlier: its next sweep can't be skipped
            if entry.first_seen > now:
                entry.first_seen = now
                self._touch_publisher(publisher)
                moved = True
        for memory in (self._departed, self._evicted):
            for publisher, (first_seen, departed) in list(memory.items()):
                if first_seen > now or departed > now:
                    memory[publisher] = (min(first_seen, now), min(departed, now))
                    self._touch_gone(publisher)
        if moved:
            self._rebuild_newest()

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
        """Unexpired providers of key in the order their records were stored (at most max_per_key, and own_id)."""
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
        """Write the records, first-seen times and departures changed since the last save, if any.

        Call it periodically and at shutdown; storing doesn't save, since records change with every /provide.
        """
        if self._db is None:
            return
        # Saves come from the maintenance thread and from stop(): one at a time, or a snapshot taken before
        # the other's could be written last, with the changes it holds already cleared
        with self._save_lock:
            with self._lock:
                records, publishers, gone = self._dirty_records, self._dirty_publishers, self._dirty_gone
                if not (records or publishers or gone):
                    return
                self._dirty_records, self._dirty_publishers, self._dirty_gone = set(), set(), set()
                # Only copied under the lock (records change in place); written without it
                stored, removed = [], []
                for key, publisher in records:
                    record = self._records.get(key, {}).get(publisher)
                    if record is None:
                        removed.append((key, publisher))
                    else:
                        stored.append((key, publisher, record.first_stored, record.last_stored, int(record.pooled)))
                seen = [(p, self._publishers[p].first_seen) for p in publishers if p in self._publishers]
                unseen = [(p,) for p in publishers if p not in self._publishers]
                departed = [(p, *self._departed[p]) for p in gone if p in self._departed]
                evicted = [(p, *self._evicted[p]) for p in gone if p in self._evicted]
                no_longer = [(p,) for p in gone]
            stored.sort()  # in key order, as the table is: a full save writes a third faster
            try:
                with self._db.transaction() as tx:
                    tx.executemany("DELETE FROM provider_records WHERE key = ? AND publisher = ?", removed)
                    tx.executemany("INSERT OR REPLACE INTO provider_records (key, publisher, first_stored, last_stored,"
                                   " pooled) VALUES (?, ?, ?, ?, ?)", stored)
                    tx.executemany("DELETE FROM provider_publishers WHERE publisher = ?", unseen)
                    tx.executemany("INSERT OR REPLACE INTO provider_publishers (publisher, first_seen) VALUES (?, ?)",
                                   seen)
                    tx.executemany("DELETE FROM provider_departed WHERE publisher = ?", no_longer)
                    tx.executemany("DELETE FROM provider_evicted WHERE publisher = ?", no_longer)
                    tx.executemany("INSERT OR REPLACE INTO provider_departed (publisher, first_seen, at)"
                                   " VALUES (?, ?, ?)", departed)
                    tx.executemany("INSERT OR REPLACE INTO provider_evicted (publisher, first_seen, at)"
                                   " VALUES (?, ?, ?)", evicted)
            except BaseException:
                with self._lock:  # try again next time
                    self._dirty_records |= records
                    self._dirty_publishers |= publishers
                    self._dirty_gone |= gone
                raise

    def close(self):
        """Release the state database (the node calls this once it has saved everything at shutdown)."""
        if self._db is not None:
            self._db.close()

    def _load(self):
        """Load the saved records, first-seen times and departures, applying the TTL and limits as configured now.

        Rows that don't load (malformed, expired, past a limit, of a publisher left without records) or that load
        changed (times clamped, a record moved in or out of the pool) are marked changed, so the next save deletes
        or rewrites them and the tables hold what memory does. Files rnfs 0.1.3 wrote, before the newcomer pool,
        were imported with every record outside it: those load outside the pool, as far as it has room.
        """
        publisher_rows = self._db.query("SELECT publisher, first_seen FROM provider_publishers")
        record_rows = self._db.query("SELECT key, publisher, first_stored, last_stored, pooled FROM provider_records")
        departed_rows = self._db.query("SELECT publisher, first_seen, at FROM provider_departed")
        evicted_rows = self._db.query("SELECT publisher, first_seen, at FROM provider_evicted")
        now = self.clock()
        malformed = 0
        # Times in the future (a clock that ran ahead, since corrected) count as now; NaN is dropped
        first_seen = {}
        for publisher, seen in publisher_rows:
            seen = _number(seen)
            if seen is None or not is_id(publisher):
                malformed += 1
                continue
            first_seen[publisher] = min(seen, now)
        records = []
        for key, publisher, first, last, pooled in record_rows:
            first, last = _number(first), _number(last)
            if first is None or last is None or not is_id(key) or not isinstance(pooled, int):
                malformed += 1
                continue
            records.append((key, publisher, min(first, now), min(last, now), bool(pooled)))
        # Rebuild first-seen order, oldest publisher first
        for publisher, seen in sorted(first_seen.items(), key=lambda item: item[1]):
            self._publishers[publisher] = _Publisher(seen, next(self._order))
        # Oldest records first, so each key's records are in first-stored order and the limits keep the oldest
        for key, publisher, first, last, pooled in sorted(records, key=lambda record: record[2]):
            own = publisher == self.own_id
            held = self._records.get(key, {})
            # As store() keeps them: the node's own records aren't limited, nor counted toward max_per_key
            if (publisher not in self._publishers or now - last >= self.ttl
                    or (not own and len(self._publishers[publisher].keys) >= self.max_per_publisher)
                    or (not own and len(held) - (self.own_id in held) >= self.max_per_key)):
                continue
            # Where it was if there is room there, else wherever there is (the node's own: where it was, as
            # store() lets them exceed the store rather than evict them)
            main_room = self._main_count < self.main_size
            pool_room = self._pool_count < self.pool_size
            if not (main_room or pool_room or own):
                continue
            if main_room or pool_room:
                pooled = pool_room if pooled or not main_room else False
            entry = self._publishers[publisher]
            self._records.setdefault(key, {})[publisher] = _Record(first, last, pooled)
            entry.keys.add(key)
            self._count += 1
            if pooled:
                self._pool_add(key, publisher)
            else:
                self._main_count += 1
                entry.main += 1
        for publisher in [p for p, entry in self._publishers.items() if not entry.keys]:
            del self._publishers[publisher]
        self._rebuild_newest()
        departed, evicted = [], []
        for rows, entries in ((departed_rows, departed), (evicted_rows, evicted)):
            for publisher, seen, left in rows:
                seen, left = _number(seen), _number(left)
                if seen is None or left is None or not is_id(publisher):
                    malformed += 1
                    continue
                entries.append((publisher, seen, left))
        for publisher, seen, left in sorted(departed, key=lambda item: item[2]):
            if publisher not in self._publishers and now - min(left, now) < self.ttl:
                self._depart(publisher, min(seen, now), min(left, now))
        for publisher, seen, left in sorted(evicted, key=lambda item: item[2]):
            if (publisher not in self._publishers and publisher not in self._departed
                    and now - min(left, now) < self.ttl):
                self._remember_evicted(publisher, min(seen, now), min(left, now))
        if malformed:
            log(f"providers: ignoring {plural(malformed, 'malformed row')} in the state database", LOG_WARNING)
        # The rows memory doesn't hold as they are
        self._dirty_publishers = {publisher for publisher, seen in publisher_rows
                                  if getattr(self._publishers.get(publisher), "first_seen", None) != seen}
        self._dirty_records = {(key, publisher) for key, publisher, first, last, pooled in record_rows
                               if self._saved_differs(key, publisher, first, last, pooled)}
        self._dirty_gone = ({publisher for publisher, seen, left in departed_rows
                             if self._departed.get(publisher) != (seen, left)}
                            | {publisher for publisher, seen, left in evicted_rows
                               if self._evicted.get(publisher) != (seen, left)})

    def _saved_differs(self, key, publisher, first, last, pooled) -> bool:
        """True unless memory holds a record of publisher for key with exactly these saved values (_load)."""
        record = self._records.get(key, {}).get(publisher)
        return record is None or (record.first_stored, record.last_stored, int(record.pooled)) != (first, last, pooled)
