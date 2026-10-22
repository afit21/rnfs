"""Provider records stored for other nodes (specification section 6). No network access (RNS is loaded
only through rnfs.log, for logging).

A record says "publisher provides key". Records expire PROVIDER_TTL after they were last stored and keep their
first-stored time across republishes.

Records live in the state database (provider_records) and are read per key, through its primary key, when a /find
or a store needs them, with a small cache of keys in memory: nothing in memory grows with the number of records or
publishers. Changes are written in batches. store(), withdraw() and expiry change the cache and note the rows they
changed, which save() writes in one transaction (the node calls maintain(), which saves, every 30 s, and save() at
shutdown; a store writes them itself once FLUSH_CHANGES are waiting). A refresh is written only once it moves a
record's last-stored time by more than REFRESH_WRITE, and a refused store writes nothing. Each publisher's standing
is kept the same way: provider_standing (records held, tier, the UTC days it refreshed a record on) and
provider_publishers (when it was first seen).

Writes are budgeted, full or not (_WriteBudget): new records and withdrawals spend a budget of rows per WRITE_WINDOW
(the node's save interval), WRITE_BUDGET, part of it open to every tier and parts kept for Earned and for Known
publishers, unused budget carried over for WRITE_CARRY windows. Past it, others' new records are refused (as by a
full store); withdrawals spend what is left but are never refused, and refreshes cost nothing. Every record inserted
is deleted at most once, so the rows written stay within twice the budget, whatever a publisher sends. A withdrawal
followed by a store of the same key and publisher before the next save is a refresh: the record comes back with its
first-stored time (_withdrawn), and the row the withdrawal spent is given back (REL-13): neither is written.

Room is local policy (spec section 6). Records may take record_budget bytes of the state database, measured as its
pages in use over the records held (so it counts their indexes, B-tree overhead and the database's other state), and
never the free disk the blocks' reserve needs (free_space). While there is room anyone stores freely. Once the store
is full, publishers are in tiers:
- Known: known(publisher) (the node's contacts of kind node, private-mode members, bootstrap nodes);
- Earned: refreshed a record it held on at least EARNED_DAYS distinct UTC days, the day of a first store not counted;
- Newcomer: everyone else;
and a store makes room by evicting a record of a tier's largest holder (the one it refreshed least recently): from
the lowest tier below the storer's that holds more than its floor (FLOORS, shares of the budget); else, if the
storer's own tier holds less than its floor, from a tier above it that holds more than its floor; else from the
storer's own tier, unless the storer holds as many records as that tier's largest holder (refused), or the tier is
Earned (Earned publishers never evict each other: refused). While full, a Newcomer's record last stored
NEWCOMER_TTL_FULL ago counts as expired. A key keeps at most max_per_key publishers, and a new publisher of a full key
evicts a random record there, never one of the K/2 oldest, which /find replies list first. Newcomers keep a share of
the key (KEY_NEWCOMER_SHARE): a Newcomer evicts an Earned record while they hold less, else a Newcomer one; an Earned
or Known storer evicts an Earned record while they hold no more, else a Newcomer one first; only a Known storer evicts
a Known record, and only when no other is left. Otherwise it is refused.

This node's own records (own_id) are always accepted, never evicted, and not counted toward max_per_key: they are
what the node provides, and its /find replies list it as a provider from them. They take room in the budget.

For the node's status (usage()), the store counts in memory the new records it refused and the records it evicted to
make room (from a full budget or a full key), by tier, over the last hour and day (rnfs.dht.recent): a flood shows as
Newcomers refused or evicted. Expiry, early or not, isn't counted.

Times are wall clock and persisted, so a time in the future (the clock ran ahead, then was corrected) is clamped to
now on load and at the first expire() after the clock went backwards; otherwise such records would outlive their TTL.
"""
import heapq
import json
import math
import os
import random
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable

from rnfs import statedb
from rnfs.constants import DAY, GIB, HOUR, ID_LENGTH, K, PROVIDER_TTL
from rnfs.dht.recent import RecentCounts
from rnfs.dht.routing import check_id, is_id
from rnfs.log import LOG_ERROR, LOG_WARNING, log

_STATE_VERSION = 1  # of providers.json, which rnfs 0.1.4 and earlier wrote
NEWCOMER, EARNED, KNOWN, OWN = 0, 1, 2, 3  # tiers, as provider_standing stores them; OWN: this node's records
TIERS = (NEWCOMER, EARNED, KNOWN)  # the tiers a full store makes room between, lowest first
TIER_NAMES = ("newcomer", "earned", "known", "own")
FLOORS = (0.10, 0.60, 0.30)  # by tier: the share of the budget a full store keeps for it
EARNED_DAYS = 3  # distinct UTC days a publisher must refresh a record it holds on to be Earned
MAX_DAYS = 1000  # refresh days counted at most (only whether there are EARNED_DAYS matters)
MAX_STANDING_RECORDS = 2 ** 40  # records a standing may say a publisher holds: far past any store (about 180 TB)
# Standings kept for publishers left without records at most (beyond it the hourly pass forgets those with the fewest
# refresh days, the longest unrefreshed first among equals). Each takes about 85-120 bytes of the state database, so
# 50,000 are about 5 MB, half a percent of the default record budget; uncapped, identities that each refreshed one
# record a day later and then withdrew it kept a row a week each: within the write budget, a couple of million rows,
# taking a quarter of the default budget from records. An honest server meets far fewer publishers leaving in a week
MAX_DEPARTED = 50_000
NEWCOMER_TTL_FULL = 4.5 * DAY  # while full, a Newcomer's record last stored this long ago counts as expired
REFRESH_WRITE = 6 * HOUR  # a refresh moving a record's last-stored time by less is not written (TTL: 7 days)
DEFAULT_RECORD_BUDGET = GIB  # bytes (Config.record_budget's default)
MAX_PUBLISHERS_PER_KEY = 1000  # publishers recorded per key, besides this node
# Newcomers' share of a full key's publishers (as of the Newcomer tier's floor): below it a Newcomer evicts a random
# Earned record, so new providers always get in and the random half of /find replies has newcomers to pick; past
# it Newcomers give way first, so a flood of fresh identities holds at most this share of a key
KEY_NEWCOMER_SHARE = 0.10
# The write budget: new records and withdrawals a WRITE_WINDOW (the node's save interval, RECORDS_INTERVAL) by pool:
# WRITE_BUDGET[NEWCOMER] is open to every tier, the others are kept for Earned and for Known publishers, who spend
# their own first. Unused budget is carried over for up to WRITE_CARRY windows (6 h). So new records are written at
# most 2 + 1 + 1 a second, each deleted again at most once: on a 1M-record database about 12 kB is written per new
# record of one publisher and 36 kB per record of a fresh identity (its standing too), so a Newcomer flood writes
# at most about 150 kB/s, against the 10-20 MB/s one identity's store-and-withdraw loop wrote without it. A 0.1.x
# publisher stores at most about 3.3 new keys a second (a lookup each), so the 43,200 carried over for all covers
# a first round of over 100,000 keys to one server; republishing is refreshes, which cost nothing
WRITE_WINDOW = 30.0
WRITE_BUDGET = (60, 30, 30)
WRITE_CARRY = 720
FLUSH_CHANGES = 20_000  # rows changed and not yet written past which a store writes them itself
MAX_CHANGES = 2 * FLUSH_CHANGES  # past this (writes failing), others' new records are refused: memory stays bounded
CACHE_ROWS = 50_000  # records of the keys used most recently kept in memory, at most ...
CACHE_KEYS = 4096  # ... of at most this many keys
STANDING_CACHE = 4096  # standings kept in memory besides those changed since the last save
SWEEP_BATCH = 1000  # rows an expiry pass reads at a time (the lock is let go between batches)
MEASURE_RECORDS = 10_000  # bytes per record are measured over at least this many records (_measure)
# What one more record adds to the state database, for usage()'s capacity (measured: about 171 bytes, with 100 or
# 1000 keys a publisher). Only reported: whether the store is full goes by the database itself (_capacity)
RECORD_BYTES_ESTIMATE = 180
_EXPIRE_INTERVAL = 60  # when full, a store sweeps for expired records at most this often (seconds)
_HOURLY = HOUR  # how often maintain() forgets old standings and checks the Known tier
_STORED, _REFUSED, _FULL = "stored", "refused", "full"  # what _store did
# A provider_records row whose key isn't an ID (a damaged page, or a hand edit: the store never writes one, and no
# /find can reach it) is left where it is, never read by the passes that list rows rather than read a key's: a key of
# another type met there broke the log line naming it, and the sorted save of the rows changed
_KEY_IS_ID = f"typeof(key) = 'blob' AND length(key) = {ID_LENGTH}"
# Expiry passes (_sweep): rows (last stored, key, publisher) last stored at or before a time, in that order, and the
# row value continuing after the last row read. Records past the TTL; and Newcomers' records, while full
_EXPIRED = (f"SELECT last_stored, key, publisher FROM provider_records WHERE last_stored <= ? AND {_KEY_IS_ID}",
            "(last_stored, key, publisher)")
_NEWCOMERS_EXPIRED = ("SELECT r.last_stored, r.key, r.publisher FROM provider_records r"
                      " JOIN provider_standing s ON s.publisher = r.publisher WHERE r.last_stored <= ? AND s.tier = 0"
                      f" AND typeof(r.key) = 'blob' AND length(r.key) = {ID_LENGTH}",
                      "(r.last_stored, r.key, r.publisher)")


def _sound_standing(table: str = "provider_standing") -> str:
    """The SQL condition that a provider_standing row (of table, its name or alias) is one the store can use: a
    publisher ID, and records, days and last_day integers in range (its tier is read leniently: _tier). Any other
    (damaged, or edited by hand) counts as no standing, as a missing row does: it is left out of the counts, its
    publisher's records are deleted as they are met, and so is the row itself once read (_get_standing). Unchecked,
    a days or records that wasn't an integer stopped the start (TypeError), and records summing past 2^63 too."""
    return (f"typeof({table}.publisher) = 'blob' AND length({table}.publisher) = {ID_LENGTH}"
            f" AND typeof({table}.records) = 'integer' AND {table}.records BETWEEN 0 AND {MAX_STANDING_RECORDS}"
            f" AND typeof({table}.days) = 'integer' AND {table}.days BETWEEN 0 AND {MAX_DAYS}"
            f" AND typeof({table}.last_day) = 'integer'")


def _row_order(row: tuple):
    """The order rows are written in (_write): by their first column, a key or a publisher, as the tables keep them.
    A damaged row being deleted may have one of another type, put after the rest, so no two types are compared."""
    first = row[0]
    return (0, first) if isinstance(first, bytes) else (1, repr(first))


def _shown(key) -> str:
    """A key or publisher read from the state database, for a log line: its hex digits, or its repr if it isn't
    bytes (a damaged row's)."""
    return key.hex() if isinstance(key, bytes) else repr(key)


def _never(node_id: bytes) -> bool:
    """Default exclude and known predicate: excludes nothing, knows no one."""
    return False


def _number(value) -> float | None:
    """A saved time as a float, or None if it isn't a number or is NaN (infinities are kept, as the JSON file's
    float() kept them: the clamping and TTL checks deal with them)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or math.isnan(value):
        return None
    return float(value)


def _day(when: float) -> int:
    """The UTC day (days since the epoch) of a wall-clock time."""
    return math.floor(when / DAY) if math.isfinite(when) else 0


def _by_publisher(changes: dict[bytes, dict[bytes, object]]) -> dict[bytes, set[bytes]]:
    """The keys of each publisher in changes (key -> publisher -> anything)."""
    keys: dict[bytes, set[bytes]] = {}
    for key, changed in changes.items():
        for publisher in changed:
            keys.setdefault(publisher, set()).add(key)
    return keys


def _tier(value) -> int:
    """A tier as provider_standing holds it; a value that isn't one (a damaged row) counts as Newcomer."""
    return value if type(value) is int and value in (NEWCOMER, EARNED, KNOWN, OWN) else NEWCOMER


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
    except RecursionError:  # nested deeper than json can parse (about 10,000 levels): damage too
        statedb.set_aside(path, "nested too deeply to parse")
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
    """Import providers.json (statedb.migrate): its publishers and records replace the provider tables, converted as
    rnfs 0.1.4 loaded them, and each publisher's standing is built from them as the 0.2.0 schema step builds it from
    0.1.x's tables (statedb.PROVIDER_STANDING). What that load dropped whatever the clock and limits were (NaN times,
    IDs of the wrong length, a record repeated: the first stored is kept) is left out; the rest (expiry, future
    times, records of unknown publishers) is the store's. Departures and evictions are not imported: 0.2.0 keeps no
    such memories. Returns the number of records imported."""
    for table in ("provider_publishers", "provider_records", "provider_departed", "provider_evicted",
                  "provider_standing"):
        tx.execute(f"DELETE FROM {table}")
    try:
        first_seen, records, _, _ = _parse_state(value)
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
    tx.execute(statedb.PROVIDER_STANDING)
    tx.execute("UPDATE provider_records SET pooled = 0 WHERE pooled != 0")
    return len(kept)


class _WriteBudget:
    """The rows (new records, withdrawals) the store may still write, by pool (WRITE_BUDGET), refilled as monotonic
    time passes and starting full."""

    __slots__ = ("per_window", "left", "at", "clock")

    def __init__(self, per_window: tuple[int, ...], clock: Callable[[], float]):
        """A budget of per_window rows a WRITE_WINDOW by pool, on clock (monotonic seconds)."""
        self.per_window = per_window
        self.clock = clock
        self.left = [float(WRITE_CARRY * rows) for rows in per_window]
        self.at = clock()

    @staticmethod
    def _pools(tier: int) -> tuple[int, ...]:
        """The pools a publisher of tier spends from, its own first."""
        return (tier, NEWCOMER) if tier in (EARNED, KNOWN) else (NEWCOMER,)

    def _refill(self):
        """Add what the time since the last refill earns, up to WRITE_CARRY windows' worth."""
        now = self.clock()
        if now > self.at:
            windows = (now - self.at) / WRITE_WINDOW
            self.left = [min(WRITE_CARRY * rows, left + windows * rows)
                         for left, rows in zip(self.left, self.per_window, strict=True)]
        self.at = now

    def allows(self, tier: int) -> bool:
        """Whether a publisher of tier may write a row now."""
        self._refill()
        return any(self.left[pool] >= 1 for pool in self._pools(tier))

    def spend(self, tier: int) -> int | None:
        """Spend a row from tier's pools, if one is left: the pool it came from, or None if none is left."""
        self._refill()
        for pool in self._pools(tier):
            if self.left[pool] >= 1:
                self.left[pool] -= 1
                return pool
        return None

    def refund(self, pool: int):
        """Give back a row spent from pool that won't be written after all (a withdrawal undone before the save)."""
        self._refill()
        self.left[pool] = min(WRITE_CARRY * self.per_window[pool], self.left[pool] + 1)


class _Standing:
    """A publisher's standing here: when first seen, how many records it holds, its tier as last worked out, and the
    UTC days on which it refreshed a record it held (days, the last of them last_day)."""

    __slots__ = ("first_seen", "records", "tier", "days", "last_day")

    def __init__(self, first_seen: float, records: int, tier: int, days: int, last_day: int):
        """A standing with these values."""
        self.first_seen = first_seen
        self.records = records
        self.tier = tier
        self.days = days
        self.last_day = last_day

    def row(self) -> tuple:
        """provider_standing's columns but the publisher."""
        return self.records, self.tier, self.days, self.last_day


class ProviderStore:
    """Provider records, publishers' standing, the record budget and its tiers, and /find reply selection.
    Thread-safe: the /find handler reads while stores are made.

    `state_dir` holds the state database (None: a private in-memory one, nothing persisted). `clock` is wall-clock
    seconds, since times are persisted. `rng` picks the random half of /find replies and the publisher a full key
    evicts. `known(publisher)` tells the Known tier; `free_space()` gives the bytes free on the state database's disk
    beyond the blocks' reserve (None: not checked); `record_bytes` fixes the bytes a record takes (None: measured).
    `monotonic` is the clock of the recent refusals and evictions usage() reports, and of the write budget;
    `write_budget` is its rows a WRITE_WINDOW by pool (WRITE_BUDGET; None: no budget).
    """

    def __init__(self, state_dir: str | None, record_budget: int = DEFAULT_RECORD_BUDGET, *,
                 ttl: float = PROVIDER_TTL, clock: Callable[[], float] = time.time, rng: random.Random = random,
                 max_per_key: int = MAX_PUBLISHERS_PER_KEY, own_id: bytes | None = None,
                 known: Callable[[bytes], bool] = _never, free_space: Callable[[], int] | None = None,
                 record_bytes: float | None = None, monotonic: Callable[[], float] = time.monotonic,
                 write_budget: tuple[int, int, int] | None = WRITE_BUDGET):
        """A store of records taking up to record_budget bytes, on the state database in state_dir if given."""
        self.record_budget = record_budget
        self.ttl = ttl
        self.clock = clock
        self.rng = rng
        self.max_per_key = max_per_key
        self.own_id = own_id
        self.known = known
        self.free_space = free_space
        self.fixed_record_bytes = record_bytes
        # Records changed since the last save: key -> publisher -> (first stored, last stored), or None if deleted
        self._dirty: dict[bytes, dict[bytes, tuple[float, float] | None]] = {}
        self._dirty_count = 0
        self._dirty_by_publisher: dict[bytes, set[bytes]] = {}  # the keys of each publisher's changed records
        # Records withdrawn since the last save, (key, publisher) -> (first stored, last stored): one stored again
        # before then is a refresh. Each is a deleted row in _dirty, so they are bounded with it
        self._withdrawn: dict[tuple[bytes, bytes], tuple[float, float]] = {}
        # The write-budget pool each of those withdrawals spent from: given back if it is undone (REL-13)
        self._withdraw_spent: dict[tuple[bytes, bytes], int] = {}
        self._writes = None if write_budget is None else _WriteBudget(write_budget, monotonic)
        # Records of the keys used most recently, as the database has them with _dirty's changes: key -> publisher ->
        # (first stored, last stored), least recently used first, at most CACHE_ROWS rows of CACHE_KEYS keys
        self._cache: OrderedDict[bytes, dict[bytes, tuple[float, float]]] = OrderedDict()
        self._cache_rows = 0
        # Standings by publisher (None: it has none), the same way: changed ones (_dirty_standing, and _dirty_seen
        # for provider_publishers) pinned until the next save, and others, least recently used first
        self._pinned_standings: dict[bytes, _Standing | None] = {}
        self._standings: OrderedDict[bytes, _Standing | None] = OrderedDict()
        self._dirty_standing: set[bytes] = set()
        self._dirty_seen: set[bytes] = set()
        # Changed standings by tier, largest holder first: (-records, publisher), stale entries dropped as they come
        # to the top. With _db_holders (each tier's holders on disk, largest first, read a page at a time and
        # stepped past those changed in memory), a tier's largest holder is found without a scan. Both are reset by
        # a save, which changes what is on disk
        self._heaps: list[list[tuple[int, bytes]]] = [[] for _ in TIERS]
        self._db_holders: list[dict] = [{} for _ in TIERS]
        self._victim_after: dict[bytes, tuple] = {}  # publisher -> its records on disk passed over until a save
        self._tier_records = [0, 0, 0, 0]  # records held, by tier (OWN: this node's)
        # New records refused, and records evicted to make room, by tier (the storer's, the evicted publisher's)
        self._refused = [RecentCounts(monotonic) for _ in TIERS]
        self._evicted = [RecentCounts(monotonic) for _ in TIERS]
        self._publishers = 0  # publishers holding a record
        self._record_bytes = 1.0  # measured (_measure) unless fixed
        self._room: int | None = None  # bytes free beyond the reserve when last measured (None: not checked)
        self._measured = 0  # records held then
        self._db_bytes: int | None = None  # the state database's pages in use then (None: not measured)
        self._retry_at = 0  # after a failed save, the changes waiting before a store tries again (_save_due)
        self._last_sweep = float("-inf")
        self._last_hourly = float("-inf")
        # The latest clock reading any time held here came from: none is later, so expiry needs to clamp times only
        # when the clock reads earlier than this (it was corrected backwards)
        self._latest = float("-inf")
        self._lock = threading.RLock()
        if state_dir is None:
            self._db = statedb.connect_memory(("providers", "providers_on_disk"))
        else:
            self._db = statedb.connect(state_dir)
        try:
            self._load()
        except BaseException:
            self._db.close()
            raise

    # Storing and withdrawing

    def store(self, key: bytes, publisher: bytes) -> bool:
        """Record that publisher provides key, or refresh its record. False if refused: the key is full of
        publishers it may not displace, or the store is full and its tier may not make room (see the module)."""
        check_id(key)
        check_id(publisher)
        with self._lock:
            return self._store(key, publisher, self._now()) == _STORED

    def withdraw(self, key: bytes, publisher: bytes) -> bool:
        """Remove publisher's record for key. True if there was one."""
        check_id(key)
        check_id(publisher)
        with self._lock:
            withdrawn = self._withdraw(key, publisher)
            if self._save_due():
                self._try_save()
            return withdrawn

    def provide(self, publisher: bytes, keys: Iterable[bytes], withdrawn: Iterable[bytes] = ()) -> int:
        """Apply one /provide batch from publisher: withdraw `withdrawn`, then store `keys`.

        Returns how many of `keys` were recorded (the rest were refused). Once the full store refuses one key, the
        batch's other new keys are refused without looking for room again: each costs one read of its key, and
        refreshes of records the publisher holds are still accepted.
        """
        check_id(publisher)
        keys, withdrawn = list(keys), list(withdrawn)
        for key in keys + withdrawn:
            check_id(key)
        with self._lock:
            now = self._now()
            for key in withdrawn:
                self._withdraw(key, publisher)
            stored = 0
            room = True
            for key in keys:
                result = self._store(key, publisher, now, room)
                stored += result == _STORED
                room = room and result != _FULL
            return stored

    def _withdraw(self, key: bytes, publisher: bytes) -> bool:
        """Remove publisher's record for key at its request, kept in _withdrawn until the next save, spending the
        write budget if any is left (a withdrawal is never refused). True if there was one. Lock held."""
        record = self._key_records(key).get(publisher)
        if record is None:
            return False
        tier = self._tier_of(publisher, self._get_standing(publisher))
        self._remove(key, publisher)
        if publisher != self.own_id:
            self._withdrawn[(key, publisher)] = record
            if self._writes is not None:
                pool = self._writes.spend(tier)
                if pool is not None:
                    self._withdraw_spent[(key, publisher)] = pool
        return True

    def _refuse(self, publisher: bytes, result: str) -> str:
        """Count a refused new record of publisher (not this node's) in its tier's refusals; result. Lock held."""
        tier = self._tier_of(publisher, self._get_standing(publisher))
        if tier in TIERS:
            self._refused[tier].add()
        return result

    def retain(self, keep: Callable[[bytes], bool]) -> int:
        """Remove every record of publishers for which keep(publisher) is False (own_id's are always kept), and
        their standing, as private mode does with non-members' records. Returns how many records were removed.

        keep is asked once per publisher, outside the lock (it may consult other locked state, like RNS's).
        """
        removed = 0
        with self._lock:  # those not written yet, then those the database lists, a page at a time
            publishers = {p for p, s in self._pinned_standings.items() if s is not None and s.records > 0}
        after = b""
        while True:
            with self._lock:
                rows = self._db.query("SELECT publisher FROM provider_standing WHERE publisher > ? AND records > 0"
                                      " ORDER BY publisher LIMIT ?", (after, SWEEP_BATCH))
            publishers.update(publisher for (publisher,) in rows if is_id(publisher))
            dropped = [publisher for publisher in sorted(publishers)
                       if publisher != self.own_id and not keep(publisher)]
            with self._lock:
                for publisher in dropped:
                    removed += self._remove_publisher(publisher)
            if len(rows) < SWEEP_BATCH:
                return removed
            after = rows[-1][0]
            publishers = set()

    def _store(self, key: bytes, publisher: bytes, now: float, room: bool = True) -> str:
        """store() with the lock held: _STORED, _REFUSED (the key is full, or too many changes wait to be written),
        or _FULL (the store is full). room False: refuse any new record as _FULL (provide, after a _FULL)."""
        records = self._key_records(key)
        record = records.get(publisher)
        if record is not None and now - record[1] < self.ttl:
            self._refresh(key, publisher, record, now)
            return _STORED
        if not room:
            return self._refuse(publisher, _FULL)
        # Withdrawn since the last save: a refresh of that record rather than a new one
        restored = self._withdrawn.pop((key, publisher), None) if record is None else None
        spent = self._withdraw_spent.pop((key, publisher), None) if restored is not None else None
        if restored is not None and now - restored[1] >= self.ttl:
            restored = None  # expired meanwhile: stored as a new record, its withdrawal written too
        victim = None
        tier = OWN
        if publisher != self.own_id:
            if self._changes() >= MAX_CHANGES and not (self._save_due() and self._try_save()):
                return self._refuse(publisher, _REFUSED)
            standing = self._get_standing(publisher)
            tier = self._tier_of(publisher, standing)
            if restored is None and self._writes is not None and not self._writes.allows(tier):
                return self._refuse(publisher, _FULL)  # the write budget is spent: as when full, for the whole batch
            allowed, victim = self._key_room(records, tier, now)
            if not allowed:
                return self._refuse(publisher, _REFUSED)
            # A record evicted from the key leaves room for this one; else the budget may have to make some
            if victim is None and self._full(now) and not self._reclaim(publisher, tier):
                return self._refuse(publisher, _FULL)
        if record is not None:
            self._remove(key, publisher)  # expired: replaced by a new record, with a new first-stored time
        if victim is not None:
            self._count_eviction(victim)
            self._remove(key, victim)
        if restored is None:
            self._add(key, publisher, now)
            if tier != OWN and self._writes is not None:
                self._writes.spend(tier)
        else:
            first, last = restored
            self._add(key, publisher, now, (first, last if abs(now - last) <= REFRESH_WRITE else now))
            self._refreshed_on(publisher, now)
            if spent is not None and self._writes is not None:
                # The withdrawal won't be written now: its row is given back, so a loop of withdrawals and restores
                # can't spend the budget others' new records need while writing nothing (REL-13)
                self._writes.refund(spent)
        if self._save_due():
            self._try_save()
        return _STORED

    def _refresh(self, key: bytes, publisher: bytes, record: tuple[float, float], now: float):
        """publisher republished its live record for key: its last-stored time is now (written only if that moves
        it by more than REFRESH_WRITE), and the day counts toward Earned if it is a new one. Lock held."""
        first, last = record
        if abs(now - last) > REFRESH_WRITE:
            self._set_record(key, publisher, (min(first, now), now))
        self._refreshed_on(publisher, now)

    def _refreshed_on(self, publisher: bytes, now: float):
        """publisher refreshed a record now: the day counts toward Earned if it is a new one. Lock held."""
        standing = self._get_standing(publisher)
        if standing is None:
            return
        day = _day(now)
        if day > standing.last_day:
            standing.days = min(standing.days + 1, MAX_DAYS)
            standing.last_day = day
            self._standing_changed(publisher, standing)
        self._retier(publisher, standing)  # Earned now, or known since

    def _key_room(self, records: dict, tier: int, now: float) -> tuple[bool, bytes | None]:
        """Whether a new publisher of tier may join a key holding records, and the publisher it evicts if the key is
        full (None if not full; the module's docstring has the rules). Lock held."""
        others = [p for p, (_, last) in records.items() if p != self.own_id and now - last < self.ttl]
        if len(others) < self.max_per_key:
            return True, None
        # The oldest records stay: /find replies list the K/2 oldest first
        others.sort(key=lambda p: (records[p][0], p))
        candidates = others[min(K // 2, self.max_per_key - 1):]
        tiers = self._tiers(candidates)
        by_tier: dict[int, list[bytes]] = {NEWCOMER: [], EARNED: [], KNOWN: []}
        for publisher in candidates:
            by_tier[tiers[publisher]].append(publisher)
        # Newcomers keep a share of the key: below it a Newcomer evicts an Earned record, and Earned or Known storers
        # spare them; past it they give way first. Earned records give way to Earned storers too, so identities that
        # earned their tier can't hold the key against everyone but Known publishers
        share = KEY_NEWCOMER_SHARE * self.max_per_key
        newcomers = len(by_tier[NEWCOMER])
        if tier == NEWCOMER:
            order = (EARNED, NEWCOMER) if newcomers < share else (NEWCOMER,)
        else:
            order = (NEWCOMER, EARNED) if newcomers > share else (EARNED, NEWCOMER)
            if tier == KNOWN:
                order += (KNOWN,)
        for victim_tier in order:
            pool = by_tier[victim_tier]
            if pool:
                return True, pool[self.rng.randrange(len(pool))]
        return False, None

    def _tiers(self, publishers: list[bytes]) -> dict[bytes, int]:
        """The tier of each of publishers (each holds a record here), read together. Lock held."""
        days = {}
        unknown = []
        for publisher in publishers:
            standing = self._pinned_standings.get(publisher, self._standings.get(publisher))
            if standing is not None:
                days[publisher] = standing.days
            else:
                unknown.append(publisher)
        for start in range(0, len(unknown), 500):  # SQLite's variable limit is 999 in older versions
            chunk = unknown[start:start + 500]
            days.update(self._db.query(f"SELECT publisher, days FROM provider_standing WHERE publisher IN "
                                       f"({', '.join('?' * len(chunk))}) AND {_sound_standing()}", chunk))
        return {publisher: KNOWN if self.known(publisher) else EARNED if days.get(publisher, 0) >= EARNED_DAYS
                else NEWCOMER for publisher in publishers}

    def _full(self, now: float) -> bool:
        """Whether the store is full (records reach the budget or the free disk), after expiring what has expired if
        that is due. Lock held."""
        if self._count() < self._capacity():
            return False
        if now - self._last_sweep >= _EXPIRE_INTERVAL:
            self._expire(now)
        return self._count() >= self._capacity()

    def _reclaim(self, publisher: bytes, tier: int) -> bool:
        """Make room in the full store for a new record of publisher, in tier, by the tiers' rules (the module's
        docstring); False if it may not. Lock held."""
        capacity = self._capacity()
        if capacity < 1:
            return False
        floors = [share * capacity for share in FLOORS]
        usage = self._tier_records
        for lower in TIERS[:tier]:  # lowest first
            if usage[lower] > floors[lower] and self._evict_largest(lower, publisher):
                return True
        if usage[tier] < floors[tier]:
            for higher in TIERS[tier + 1:]:
                if usage[higher] > floors[higher] and self._evict_largest(higher, publisher):
                    return True
        if tier == EARNED:
            return False  # Earned publishers never evict each other
        largest = self._largest(tier)
        standing = self._get_standing(publisher)
        held = standing.records if standing is not None else 0
        if largest is None or largest[1] <= held:
            return False  # it holds as many as anyone in its tier (or is the largest): no churning
        return self._evict_from(largest[0])

    def _evict_largest(self, tier: int, skip: bytes) -> bool:
        """Evict a record of tier's largest holder (not skip, the storer); False if the tier has none. Lock held."""
        largest = self._largest(tier, skip)
        return largest is not None and self._evict_from(largest[0])

    def _evict_from(self, publisher: bytes) -> bool:
        """Evict the record publisher refreshed least recently; False if it has none. Lock held."""
        key = self._oldest_key(publisher)
        if key is None or publisher not in self._key_records(key):
            return False
        self._count_eviction(publisher)
        return self._remove(key, publisher)

    def _count_eviction(self, publisher: bytes):
        """Count an evicted record of publisher in its tier's evictions (its tier as last worked out). Lock held."""
        standing = self._get_standing(publisher)
        if standing is not None and standing.tier in TIERS:
            self._evicted[standing.tier].add()

    # Records and standings, read through the caches

    def _key_records(self, key: bytes) -> dict[bytes, tuple[float, float]]:
        """key's records, publisher -> (first stored, last stored), from the cache or the database. Rows that don't
        load (malformed, or of a publisher with no standing) are left out, and deleted by the next save. The dict is
        the cache's: valid until the next call that may read another key. Lock held."""
        records = self._cache.get(key)
        if records is not None:
            self._cache.move_to_end(key)
            return records
        self._trim_cache(1)
        changed = self._dirty.get(key, {})
        records = {}
        bad = []
        for publisher, first, last, has_standing in self._db.query(
                f"SELECT r.publisher, r.first_stored, r.last_stored, s.publisher IS NOT NULL AND {_sound_standing('s')}"
                " FROM provider_records r LEFT JOIN provider_standing s ON s.publisher = r.publisher WHERE r.key = ?",
                (key,)):
            if publisher in changed:
                continue  # as changed below (a bad row read before: deleted by the next save)
            first, last = _number(first), _number(last)
            if first is None or last is None or not is_id(publisher) or not has_standing:
                bad.append((publisher, has_standing))
                continue
            records[publisher] = (first, last)
        records.update((publisher, record) for publisher, record in changed.items() if record is not None)
        self._cache[key] = records
        self._cache_rows += len(records)
        for publisher, has_standing in bad:
            log(f"providers: ignoring a malformed record of {_shown(key)} in the state database", LOG_WARNING)
            self._mark(key, publisher, None)
            if has_standing and is_id(publisher):
                self._uncount(publisher)  # counted in its standing
        return records

    def _get_standing(self, publisher: bytes) -> _Standing | None:
        """publisher's standing, from the caches or the database; None if it has none. Lock held."""
        if publisher in self._pinned_standings:
            return self._pinned_standings[publisher]
        if publisher in self._standings:
            self._standings.move_to_end(publisher)
            return self._standings[publisher]
        while len(self._standings) >= STANDING_CACHE:
            self._standings.popitem(last=False)
        rows = self._db.query(f"SELECT s.records, s.tier, s.days, s.last_day, p.first_seen, {_sound_standing('s')}"
                              " FROM provider_standing s LEFT JOIN provider_publishers p ON p.publisher = s.publisher"
                              " WHERE s.publisher = ?", (publisher,))
        if not rows:
            self._standings[publisher] = None
            return None
        records, tier, days, last_day, first_seen, sound = rows[0]
        if not sound:  # damaged: none (_sound_standing), and deleted with its first-seen time by the next save
            log(f"providers: ignoring the malformed standing of {_shown(publisher)} in the state database", LOG_WARNING)
            self._pin_standing(publisher, None)
            self._dirty_standing.add(publisher)
            self._dirty_seen.add(publisher)
            return None
        seen = _number(first_seen)
        standing = _Standing(self._latest if seen is None else seen, records, _tier(tier), days, last_day)
        self._standings[publisher] = standing
        if seen is None:  # no provider_publishers row: one is written
            self._seen_changed(publisher, standing)
        if standing.tier != tier:  # not a tier (a damaged row), counted as a Newcomer at load: written so
            self._standing_changed(publisher, standing)
        return standing

    def _set_record(self, key: bytes, publisher: bytes, record: tuple[float, float] | None):
        """Set (record None: delete) publisher's record for key, for the next save. Lock held."""
        records = self._key_records(key)
        self._mark(key, publisher, record)
        if record is None:
            if records.pop(publisher, None) is not None:
                self._cache_rows -= 1
        else:
            if publisher not in records:
                self._cache_rows += 1
            records[publisher] = record

    def _mark(self, key: bytes, publisher: bytes, record: tuple[float, float] | None):
        """Note publisher's row for key as it is now (None: deleted), for the next save. Lock held."""
        changed = self._dirty.setdefault(key, {})
        if publisher not in changed:
            self._dirty_count += 1
            self._dirty_by_publisher.setdefault(publisher, set()).add(key)
        changed[publisher] = record

    def _add(self, key: bytes, publisher: bytes, now: float, record: tuple[float, float] | None = None):
        """Add a record of publisher for key, first and last stored now unless record gives them, and count it. Lock
        held."""
        self._set_record(key, publisher, (now, now) if record is None else record)
        standing = self._get_standing(publisher)
        if standing is None:
            # The day of a first store doesn't count toward Earned: only refreshes on later days do
            standing = _Standing(now, 0, NEWCOMER, 0, _day(now))
            self._seen_changed(publisher, standing)
        self._retier(publisher, standing)
        self._count_records(publisher, standing, 1)

    def _remove(self, key: bytes, publisher: bytes) -> bool:
        """Delete publisher's record for key, if any, and count it gone; a publisher left with none keeps its
        standing only if it has refresh days to keep (forgotten a TTL after the last, or sooner past MAX_DEPARTED such
        standings). Lock held."""
        if publisher not in self._key_records(key):
            return False
        self._set_record(key, publisher, None)
        self._uncount(publisher)
        return True

    def _uncount(self, publisher: bytes):
        """One record of publisher went: counted so, and its standing forgotten if it is left with neither records
        nor refresh days. Lock held."""
        standing = self._get_standing(publisher)
        if standing is not None:
            self._count_records(publisher, standing, -1)
            if standing.records <= 0 and standing.days == 0:
                self._forget(publisher)

    def _remove_publisher(self, publisher: bytes) -> int:
        """Delete every record of publisher and its standing (retain). Returns how many records went. Lock held."""
        keys = {key for key in self._dirty_by_publisher.get(publisher, ()) if self._dirty[key][publisher] is not None}
        after = b""
        while True:
            rows = self._db.query("SELECT key FROM provider_records WHERE publisher = ? AND key > ? ORDER BY key"
                                  " LIMIT ?", (publisher, after, SWEEP_BATCH))
            keys.update(key for (key,) in rows)
            if len(rows) < SWEEP_BATCH:
                break
            after = rows[-1][0]
        removed = sum(self._remove(key, publisher) for key in sorted(keys))
        if self._get_standing(publisher) is not None:
            self._forget(publisher)  # its standing came from records it shouldn't have had
        if self._save_due():
            self._try_save()
        return removed

    def _forget(self, publisher: bytes):
        """Drop publisher's standing (it holds no records) and first-seen time. Lock held."""
        standing = self._get_standing(publisher)
        if standing is not None and standing.records > 0:
            self._count_records(publisher, standing, -standing.records)
        self._pin_standing(publisher, None)
        self._dirty_standing.add(publisher)
        self._dirty_seen.add(publisher)

    def _count_records(self, publisher: bytes, standing: _Standing, change: int):
        """publisher, in its standing's tier, holds change more records. Lock held."""
        if standing.records <= 0 < standing.records + change:
            self._publishers += 1
        elif standing.records > 0 >= standing.records + change:
            self._publishers -= 1
        standing.records += change
        self._tier_records[standing.tier] += change
        self._standing_changed(publisher, standing)

    def _tier_of(self, publisher: bytes, standing: _Standing | None) -> int:
        """publisher's tier now. Lock held."""
        if publisher == self.own_id:
            return OWN
        if self.known(publisher):
            return KNOWN
        if standing is not None and standing.days >= EARNED_DAYS:
            return EARNED
        return NEWCOMER

    def _retier(self, publisher: bytes, standing: _Standing):
        """Move publisher's records to its tier now, if that changed (only then is the standing written). Lock held."""
        tier = self._tier_of(publisher, standing)
        if tier != standing.tier:
            self._tier_records[standing.tier] -= standing.records
            self._tier_records[tier] += standing.records
            standing.tier = tier
            self._standing_changed(publisher, standing)

    def _pin_standing(self, publisher: bytes, standing: _Standing | None):
        """Keep publisher's standing in memory until the next save, which writes it. Lock held."""
        self._standings.pop(publisher, None)
        self._pinned_standings[publisher] = standing

    def _seen_changed(self, publisher: bytes, standing: _Standing):
        """Note publisher's first-seen time changed, for the next save. Lock held."""
        self._pin_standing(publisher, standing)
        self._dirty_seen.add(publisher)

    def _standing_changed(self, publisher: bytes, standing: _Standing):
        """Note publisher's standing changed, for the next save and its tier's largest holders. Lock held."""
        self._pin_standing(publisher, standing)
        self._dirty_standing.add(publisher)
        if standing.tier in TIERS and standing.records > 0:
            heap = self._heaps[standing.tier]
            heapq.heappush(heap, (-standing.records, publisher))
            if len(heap) > 4 * len(self._dirty_standing) + 64:  # mostly stale entries: rebuilt
                self._heaps[standing.tier] = self._changed_holders(standing.tier)

    def _changed_holders(self, tier: int) -> list[tuple[int, bytes]]:
        """A heap of the publishers of tier holding records whose standing changed since the last save. Lock held."""
        heap = [(-s.records, p) for p in self._dirty_standing
                if (s := self._pinned_standings[p]) is not None and s.tier == tier and s.records > 0]
        heapq.heapify(heap)
        return heap

    def _trim_cache(self, room: int = 0):
        """Forget the least recently used keys' records (those changed stay in _dirty), leaving room for this many
        more keys under CACHE_KEYS, and CACHE_ROWS rows at most. Lock held."""
        while self._cache and (len(self._cache) + room > CACHE_KEYS or self._cache_rows > CACHE_ROWS):
            self._cache_rows -= len(self._cache.popitem(last=False)[1])

    def _save_due(self) -> bool:
        """Whether so much waits to be written that a store should save now rather than wait for the node's next
        save (after a failed one, only once a tenth as many more are waiting). Lock held."""
        return self._changes() >= max(FLUSH_CHANGES, self._retry_at)

    # A tier's largest holder, and its record refreshed least recently

    def _largest(self, tier: int, skip: bytes | None = None) -> tuple[bytes, int] | None:
        """(publisher, records) of a publisher of tier holding the most records, not skip nor own_id; None if none.
        Lock held."""
        while True:
            changed = self._heap_top(tier, skip)
            on_disk = self._disk_top(tier, skip)
            if changed is None and on_disk is None:
                return None
            publisher = (changed if on_disk is None or (changed is not None and changed[1] >= on_disk[1])
                         else on_disk)[0]
            standing = self._get_standing(publisher)
            if standing is not None and self._tier_of(publisher, standing) == tier:
                return publisher, standing.records
            if standing is None:  # not expected: the cache holds what the database does
                self._pin_standing(publisher, None)
                self._dirty_standing.add(publisher)  # so neither looks at it again until saved
            else:
                # Its tier changed since it was worked out (a contact added or removed): moved, and looked again
                self._retier(publisher, standing)

    def _heap_top(self, tier: int, skip: bytes | None) -> tuple[bytes, int] | None:
        """The largest holder of tier among standings changed since the last save. Lock held."""
        heap = self._heaps[tier]
        aside = None
        found = None
        while heap:
            negative, publisher = heap[0]
            standing = self._pinned_standings.get(publisher)
            if (publisher not in self._dirty_standing or standing is None or standing.tier != tier
                    or standing.records != -negative or standing.records <= 0):
                heapq.heappop(heap)  # stale
                continue
            if publisher in (skip, self.own_id):
                aside = heapq.heappop(heap)
                continue
            found = publisher, standing.records
            break
        if aside is not None:
            heapq.heappush(heap, aside)
        return found

    def _disk_top(self, tier: int, skip: bytes | None) -> tuple[bytes, int] | None:
        """The largest holder of tier as the database has it, leaving out those changed since (their standing in
        memory is current). Pages are read largest first and kept until the next save, which changes them. Lock
        held."""
        state = self._db_holders[tier]
        rows = state.setdefault("rows", [])
        index = state.get("index", 0)
        while True:
            while index < len(rows):
                records, publisher = rows[index]
                if publisher in self._dirty_standing or publisher == self.own_id:
                    index += 1  # changed in memory (until the next save), or this node
                    state["index"] = index
                    continue
                if publisher == skip:
                    index += 1  # passed over this time only
                    continue
                return publisher, records
            if state.get("done"):
                return None
            if rows:
                last = rows[-1]
                more = self._db.query("SELECT records, publisher FROM provider_standing WHERE tier = ? AND records > 0"
                                      f" AND {_sound_standing()} AND (records, publisher) < (?, ?)"
                                      " ORDER BY records DESC, publisher DESC LIMIT 64", (tier, *last))
            else:
                more = self._db.query("SELECT records, publisher FROM provider_standing WHERE tier = ? AND records > 0"
                                      f" AND {_sound_standing()} ORDER BY records DESC, publisher DESC LIMIT 64",
                                      (tier,))
            if len(more) < 64:
                state["done"] = True
            rows.extend(more)

    def _oldest_key(self, publisher: bytes) -> bytes | None:
        """The key of the record publisher refreshed least recently, preferring those written already; None if it
        holds none. Lock held."""
        after = self._victim_after.get(publisher)
        while True:
            if after is None:
                rows = self._db.query("SELECT last_stored, key FROM provider_records WHERE publisher = ?"
                                      f" AND {_KEY_IS_ID} ORDER BY last_stored, key LIMIT 32", (publisher,))
            else:
                rows = self._db.query("SELECT last_stored, key FROM provider_records WHERE publisher = ?"
                                      f" AND {_KEY_IS_ID} AND (last_stored, key) > (?, ?) ORDER BY last_stored, key"
                                      " LIMIT 32", (publisher, *after))
            for last, key in rows:
                after = last, key
                if publisher in self._dirty.get(key, ()):
                    continue  # changed in memory: what the database has of it is stale
                self._victim_after[publisher] = after
                return key
            if len(rows) < 32:
                break
        if after is not None:
            self._victim_after[publisher] = after
        changed = [key for key in self._dirty_by_publisher.get(publisher, ()) if self._dirty[key][publisher] is not None]
        return min(changed, key=lambda key: (self._dirty[key][publisher][1], key), default=None)

    # Room

    def _count(self) -> int:
        """Records held, including this node's and expired ones not yet removed. Lock held."""
        return sum(self._tier_records)

    def _changes(self) -> int:
        """Rows changed and not yet written. Lock held."""
        return self._dirty_count + len(self._dirty_standing) + len(self._dirty_seen)

    def _capacity(self) -> float:
        """How many records the store holds when full: record_budget, or the free disk beyond the reserve when last
        measured, over the bytes a record takes. Lock held."""
        size = self.fixed_record_bytes or self._record_bytes
        capacity = max(0, self.record_budget) / size
        if self._room is not None:
            capacity = min(capacity, self._measured + max(0, self._room) / size)
        return capacity

    def _measure(self):
        """Measure the bytes a record takes (the database's pages in use over the records held) and the free disk.
        Lock held."""
        if self.fixed_record_bytes is None:
            page_size = self._db.query("PRAGMA page_size")[0][0]
            pages = self._db.query("PRAGMA page_count")[0][0] - self._db.query("PRAGMA freelist_count")[0][0]
            self._db_bytes = pages * page_size
            # Over fewer records the database is mostly the rest of rnfs's state, and the ratio says nothing about
            # records; a store that small is far below any budget but a tiny one
            self._record_bytes = max(1.0, pages * page_size / max(MEASURE_RECORDS, self._count()))
        if self.free_space is not None:
            try:
                self._room = self.free_space()
            except OSError as e:
                log(f"providers: could not check the free disk space ({e})", LOG_WARNING)
        self._measured = self._count()

    def is_full(self) -> bool:
        """Whether the store is full: its records reach record_budget or the free disk, so the tiers apply."""
        with self._lock:
            return self._count() >= self._capacity()

    # Expiry

    def expire(self) -> int:
        """Remove every expired record: past the TTL, and while the store is full, Newcomers' records last stored
        NEWCOMER_TTL_FULL ago. Returns how many were removed."""
        with self._lock:
            now = self._now()
        return self._expire(now)

    def _now(self) -> float:
        """The clock's time, for a time about to be held: noted in _latest. Lock held."""
        now = self.clock()
        self._latest = max(self._latest, now)
        return now

    def _expire(self, now: float) -> int:
        """expire(), the lock taken for each batch (or held by the caller). Clamps times in the future first if the
        clock went backwards."""
        with self._lock:
            self._last_sweep = now
            if now < self._latest:
                self._clamp(now)
            removed = self._sweep_changed(now)
        removed += self._sweep(_EXPIRED, now - self.ttl, newcomers_only=False)
        with self._lock:
            full = self._count() >= self._capacity()
        if full:
            removed += self._sweep(_NEWCOMERS_EXPIRED, now - NEWCOMER_TTL_FULL, newcomers_only=True)
        return removed

    def _sweep_changed(self, now: float) -> int:
        """Remove the expired records among those changed since the last save (which the database doesn't have as
        they are). Lock held."""
        full = self._count() >= self._capacity()
        expired = []
        for key, changed in self._dirty.items():
            for publisher, record in changed.items():
                if record is None or publisher == self.own_id:
                    continue
                age = now - record[1]
                if age >= self.ttl or (full and age >= NEWCOMER_TTL_FULL
                                       and self._tier_of(publisher, self._get_standing(publisher)) == NEWCOMER):
                    expired.append((key, publisher))
        return sum(self._remove(key, publisher) for key, publisher in expired)

    def _sweep(self, query: tuple[str, str], cutoff: float, newcomers_only: bool) -> int:
        """Remove the records query lists (_EXPIRED, _NEWCOMERS_EXPIRED) as of cutoff, a batch at a time under the
        lock: those changed since the last save are _sweep_changed's, this node's are never removed, and if
        newcomers_only, only those of publishers still Newcomers. Returns how many were removed."""
        select, row = query
        removed = 0
        after = None
        while True:
            with self._lock:
                if after is None:
                    rows = self._db.query(f"{select} ORDER BY 1, 2, 3 LIMIT ?", (cutoff, SWEEP_BATCH))
                else:
                    rows = self._db.query(f"{select} AND {row} > (?, ?, ?) ORDER BY 1, 2, 3 LIMIT ?",
                                          (cutoff, *after, SWEEP_BATCH))
                for last, key, publisher in rows:
                    after = last, key, publisher
                    if publisher in self._dirty.get(key, ()) or publisher == self.own_id:
                        continue
                    if newcomers_only and self._tier_of(publisher, self._get_standing(publisher)) != NEWCOMER:
                        continue
                    removed += self._remove(key, publisher)
                if self._changes() >= FLUSH_CHANGES:
                    self._try_save()
            if len(rows) < SWEEP_BATCH:
                return removed

    def _clamp(self, now: float):
        """Bring every time after now back to now (the clock was corrected backwards), so none is later than now any
        more. Lock held."""
        late = [(key, publisher) for key, changed in self._dirty.items() for publisher, record in changed.items()
                if record is not None and max(record) > now]
        late += self._db.query("SELECT key, publisher FROM provider_records WHERE (last_stored > ? OR first_stored > ?)"
                               f" AND {_KEY_IS_ID}", (now, now))
        for key, publisher in late:
            record = self._key_records(key).get(publisher)
            if record is not None and max(record) > now:
                self._set_record(key, publisher, (min(record[0], now), min(record[1], now)))
        today = _day(now)
        publishers = {publisher for (publisher,) in self._db.query(
            "SELECT publisher FROM provider_publishers WHERE first_seen > ?", (now,))}
        publishers |= {publisher for publisher, standing in [*self._pinned_standings.items(), *self._standings.items()]
                       if standing is not None and (standing.first_seen > now or standing.last_day > today)}
        for publisher in publishers:
            standing = self._get_standing(publisher)
            if standing is not None and standing.first_seen > now:
                standing.first_seen = now
                self._seen_changed(publisher, standing)
            if standing is not None and standing.last_day > today:
                standing.last_day = today
                self._standing_changed(publisher, standing)
        self._latest = now

    # Periodic work

    def maintain(self):
        """The node's periodic pass (every 30 s): expire, hourly forget standings left without records for a TTL
        and check the Known tier, then save, and hourly forget standings without records past MAX_DEPARTED (saved
        again). Raises OSError (StateDbError) if a save fails; the changes are kept for the next one."""
        with self._lock:
            now = self._now()
        self._expire(now)
        with self._lock:
            hourly = now - self._last_hourly >= _HOURLY
            if hourly:
                self._last_hourly = now
                self._forget_departed(now)
                self._check_known()
        self.save()
        if hourly:
            with self._lock:  # counted on disk, so after the save
                if self._cap_departed():
                    self._save()

    def _forget_departed(self, now: float):
        """Forget the standing of publishers left without records whose last refresh day is a TTL ago. Lock held."""
        cutoff = _day(now - self.ttl)
        stale = {publisher for (publisher,) in self._db.query(
            "SELECT publisher FROM provider_standing WHERE tier IN (0, 1, 2, 3) AND records = 0 AND last_day < ?",
            (cutoff,))}
        stale |= {publisher for publisher, standing in self._pinned_standings.items()
                  if standing is not None and standing.records <= 0 and standing.last_day < cutoff}
        for publisher in stale:
            standing = self._get_standing(publisher)
            if standing is not None and standing.records <= 0 and standing.last_day < cutoff:
                self._forget(publisher)

    def _cap_departed(self) -> int:
        """Forget the standings of publishers without records past MAX_DEPARTED of them (this node's aside), those with
        the fewest refresh days first, the longest unrefreshed first among equals, so Earned publishers that left
        outlast identities with a day or two of refreshes. Counted from the database, so called just after a save.
        Returns how many were forgotten. Lock held."""
        excess = self._db.query("SELECT COUNT(*) FROM provider_standing WHERE tier IN (0, 1, 2) AND records = 0"
                                )[0][0] - MAX_DEPARTED
        if excess <= 0:
            return 0
        forgotten = 0
        for (publisher,) in self._db.query("SELECT publisher FROM provider_standing WHERE tier IN (0, 1, 2) AND"
                                           " records = 0 ORDER BY days, last_day, publisher LIMIT ?", (excess,)):
            standing = self._get_standing(publisher)
            if publisher != self.own_id and standing is not None and standing.records <= 0:
                self._forget(publisher)
                forgotten += 1
        return forgotten

    def _check_known(self):
        """Move publishers written as Known that no longer are (a contact removed) to their tier now. Lock held."""
        for (publisher,) in self._db.query("SELECT publisher FROM provider_standing WHERE tier = ?", (KNOWN,)):
            standing = self._get_standing(publisher)
            if standing is not None:
                self._retier(publisher, standing)

    # Queries

    def for_reply(self, key: bytes, exclude: Callable[[bytes], bool] = _never) -> list[bytes]:
        """Up to K providers of key for a /find reply, leaving out expired and excluded (banned) ones.

        The K/2 with the oldest records come first, favouring long-standing providers; then providers
        chosen at random from the rest, so early squatters can't hold every slot. exclude is only asked
        about candidates as they are chosen, not about every provider of the key.
        """
        candidates = self._live(key)
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
        # exclude may consult other locked state (the routing table's bans), so it is called outside our lock
        return [publisher for publisher in self._live(key) if not exclude(publisher)]

    def _live(self, key: bytes) -> list[bytes]:
        """Unexpired providers of key, oldest record first (at most max_per_key, and own_id)."""
        with self._lock:
            now = self.clock()
            records = [(first, publisher) for publisher, (first, last) in self._key_records(key).items()
                       if now - last < self.ttl]
        records.sort()
        return [publisher for _, publisher in records]

    def record(self, key: bytes, publisher: bytes) -> tuple[float, float] | None:
        """(first stored, last stored) of publisher's record for key, expired or not; None if there is none."""
        with self._lock:
            return self._key_records(key).get(publisher)

    def record_count(self) -> int:
        """Number of records held, including expired ones not yet removed by expire()."""
        with self._lock:
            return self._count()

    def tier_counts(self) -> dict[str, int]:
        """Records held by tier name ("own": this node's), as each publisher's tier was last worked out."""
        with self._lock:
            return dict(zip(TIER_NAMES, self._tier_records, strict=True))

    def usage(self) -> dict:
        """The store against its budget, for the node's status, from counts kept in memory (no read of the records):
        {"bytes": what counts against record_budget (the state database's size in use, records and the rest of rnfs's
        state, as measured at the last save, with the records stored since), "budget": record_budget, "capacity": the
        records it would hold when full (_reported_capacity), "full", "publishers": publishers holding records here
        (this node too, if it does), "own_records": this node's, "tiers": {"known"|"earned"|"newcomer": {"records",
        "share" (of the capacity), "floor" (the share a full store keeps for it), "refused_hour", "refused_day" (new
        records refused), "evicted_hour", "evicted_day" (records evicted to make room)}}}."""
        with self._lock:
            count, capacity = self._count(), self._capacity()
            full = count >= capacity
            used, capacity = self._reported_usage(count, capacity, full)
            records, publishers = list(self._tier_records), self._publishers
        tiers = {}
        for tier in reversed(TIERS):  # highest first, as rnfs status lists them
            refused_hour, refused_day = self._refused[tier].hour_and_day()
            evicted_hour, evicted_day = self._evicted[tier].hour_and_day()
            tiers[TIER_NAMES[tier]] = {"records": records[tier],
                                       "share": round(records[tier] / capacity, 4) if capacity > 0 else 0.0,
                                       "floor": FLOORS[tier], "refused_hour": refused_hour, "refused_day": refused_day,
                                       "evicted_hour": evicted_hour, "evicted_day": evicted_day}
        return {"bytes": round(used), "budget": self.record_budget, "capacity": math.floor(capacity),
                "full": full, "publishers": publishers, "own_records": records[OWN], "tiers": tiers}

    def _reported_usage(self, count: int, capacity: float, full: bool) -> tuple[float, float]:
        """(bytes counting against the budget, records the store would hold when full) for usage(), with count records
        held, _capacity() capacity and whether that makes the store full. Lock held.

        _capacity divides the database by the records (at least MEASURE_RECORDS), so while they are few next to the
        rest of rnfs's state (a node's block sizes alone take tens of megabytes for a million blocks) each looks
        to cost kilobytes and the capacity looks a fraction of what it is. Whether the store is full goes by the
        database reaching the budget either way; for the report, the room left is counted at RECORD_BYTES_ESTIMATE a
        record instead, within the free disk, but never as fewer records than the store takes (capacity): with the
        rest of the state over the budget there is no room left by that count, yet the store, measuring a record over
        at least MEASURE_RECORDS, still takes records until it is full. A fixed record size (record_bytes) is reported
        as it is."""
        if self.fixed_record_bytes is not None or self._db_bytes is None:
            size = self.fixed_record_bytes or self._record_bytes
            return count * size, capacity
        used = self._db_bytes + max(0, count - self._measured) * RECORD_BYTES_ESTIMATE
        if full:
            return used, count
        room = max(0, self.record_budget - used)
        if self._room is not None:
            room = min(room, max(0, self._room - max(0, count - self._measured) * RECORD_BYTES_ESTIMATE))
        return used, max(capacity, count + room / RECORD_BYTES_ESTIMATE)

    def publisher_count(self) -> int:
        """Number of publishers with records here."""
        with self._lock:
            return self._publishers

    def first_seen(self, publisher: bytes) -> float | None:
        """When publisher was first seen (clock time), or None if it has no records here."""
        with self._lock:
            standing = self._get_standing(publisher)
            return standing.first_seen if standing is not None and standing.records > 0 else None

    def standing(self, publisher: bytes) -> dict | None:
        """publisher's standing: {"first_seen", "records", "tier" (TIER_NAMES, as it is now), "days" (distinct UTC
        days it refreshed a record on)}, or None if it has none (kept a TTL past its last record if it has days)."""
        with self._lock:
            standing = self._get_standing(publisher)
            if standing is None:
                return None
            return {"first_seen": standing.first_seen, "records": standing.records,
                    "tier": TIER_NAMES[self._tier_of(publisher, standing)], "days": standing.days}

    # Persistence

    def save(self):
        """Write the rows changed since the last save, in one transaction (none if nothing changed), then measure the
        bytes a record takes and the free disk. Raises OSError (StateDbError) if the write fails; the changes are kept
        for the next save."""
        with self._lock:
            self._save()

    def _save(self):
        """save() with the lock held."""
        if self._changes():
            self._write()
        self._measure()

    def _write(self):
        """Write the rows changed since the last save, in one transaction. Lock held."""
        stored, removed = [], []
        for key, changed in self._dirty.items():
            for publisher, record in changed.items():
                if record is None:
                    removed.append((key, publisher))
                else:
                    stored.append((key, publisher, record[0], record[1], 0))
        standings, gone = [], []
        for publisher in self._dirty_standing:
            standing = self._pinned_standings[publisher]
            if standing is None:
                gone.append((publisher,))
            else:
                standings.append((publisher, *standing.row()))
        seen, unseen = [], []
        for publisher in self._dirty_seen:
            standing = self._pinned_standings[publisher]
            if standing is None:
                unseen.append((publisher,))
            else:
                seen.append((publisher, standing.first_seen))
        for rows in (stored, removed, standings, gone, seen, unseen):
            rows.sort(key=_row_order)  # in key order, as the tables are: fewer pages touched
        with self._db.transaction() as tx:
            tx.executemany("DELETE FROM provider_records WHERE key = ? AND publisher = ?", removed)
            tx.executemany("INSERT OR REPLACE INTO provider_records (key, publisher, first_stored, last_stored, pooled)"
                           " VALUES (?, ?, ?, ?, ?)", stored)
            tx.executemany("DELETE FROM provider_standing WHERE publisher = ?", gone)
            tx.executemany("INSERT OR REPLACE INTO provider_standing (publisher, records, tier, days, last_day)"
                           " VALUES (?, ?, ?, ?, ?)", standings)
            tx.executemany("DELETE FROM provider_publishers WHERE publisher = ?", unseen)
            tx.executemany("INSERT OR REPLACE INTO provider_publishers (publisher, first_seen) VALUES (?, ?)", seen)
        self._dirty.clear()
        self._dirty_count = 0
        self._dirty_by_publisher.clear()
        self._withdrawn.clear()
        self._withdraw_spent.clear()
        self._dirty_standing.clear()
        self._dirty_seen.clear()
        # The changed standings are what the database has now: cached as any other, the most recently used
        self._standings.update(self._pinned_standings)
        self._pinned_standings.clear()
        while len(self._standings) > STANDING_CACHE:
            self._standings.popitem(last=False)
        self._heaps = [[] for _ in TIERS]
        self._db_holders = [{} for _ in TIERS]
        self._victim_after.clear()
        self._retry_at = 0

    def _try_save(self) -> bool:
        """Save now (too many changes are waiting); False, logged, if the write fails. Lock held."""
        try:
            self._save()
        except OSError as e:
            log(f"providers: saving provider records failed ({e}); trying again later", LOG_ERROR)
            self._retry_at = self._changes() + FLUSH_CHANGES // 10
            return False
        return True

    def close(self):
        """Release the state database (the node calls this once it has saved everything at shutdown)."""
        self._db.close()

    def _load(self):
        """Read the counts from the standings, put this node's records in their own tier, clamp times in the future,
        and measure. Nothing is read per record: records are read per key as they are needed."""
        now = self._now()
        for tier, records, holders in self._db.query(
                f"SELECT tier, TOTAL(records), TOTAL(records > 0) FROM provider_standing WHERE {_sound_standing()}"
                " GROUP BY tier"):
            self._tier_records[_tier(tier)] += int(records)
            self._publishers += int(holders)
        # This node's records, and those of an earlier identity (or of 0.1.x's tables) now an ordinary publisher's
        listed = {publisher for (publisher,) in self._db.query(
            "SELECT publisher FROM provider_standing WHERE tier = ?", (OWN,))}
        for publisher in listed | ({self.own_id} if self.own_id is not None else set()):
            standing = self._get_standing(publisher)
            if standing is not None and self._tier_of(publisher, standing) != standing.tier:
                self._retier(publisher, standing)
        latest = self._db.query("SELECT MAX(last_stored) FROM provider_records")[0][0]
        if isinstance(latest, (int, float)) and latest > now:
            self._clamp(now)
        self._measure()

    def _merged(self) -> tuple[dict[tuple[bytes, bytes], tuple[float, float]], dict[bytes, _Standing]]:
        """Every record, (key, publisher) -> (first stored, last stored), and every standing, publisher -> _Standing,
        as the database and the changes not yet written make them (for tests: it reads the whole tables). Lock
        held."""
        records = {(key, publisher): (first, last) for key, publisher, first, last in self._db.query(
            "SELECT key, publisher, first_stored, last_stored FROM provider_records")}
        for key, changed in self._dirty.items():
            for publisher, record in changed.items():
                if record is None:
                    records.pop((key, publisher), None)
                else:
                    records[(key, publisher)] = record
        standings = {publisher: _Standing(seen, count, tier, days, last_day)
                     for publisher, count, tier, days, last_day, seen in self._db.query(
                         "SELECT s.publisher, s.records, s.tier, s.days, s.last_day, p.first_seen"
                         " FROM provider_standing s LEFT JOIN provider_publishers p ON p.publisher = s.publisher")}
        for publisher, standing in self._pinned_standings.items():
            if standing is None:
                standings.pop(publisher, None)
            else:
                standings[publisher] = standing
        return records, standings

    def _check_consistency(self):
        """Recompute the counts and standings from the records, as the database and the changes in memory make them,
        and raise AssertionError where one differs (for tests). A cache or count added to the store belongs here."""
        def check(condition: bool, what: str):
            """Raise AssertionError(what) unless condition holds."""
            if not condition:
                raise AssertionError(f"ProviderStore inconsistent: {what}")

        with self._lock:
            records, standings = self._merged()
            for key, cached in self._cache.items():
                held = {publisher: record for (k, publisher), record in records.items() if k == key}
                check(cached == held, f"key {key.hex()}'s cached records")
            check(self._cache_rows == sum(map(len, self._cache.values())), "_cache_rows")
            check(len(self._cache) <= CACHE_KEYS, "the key cache is over its size")
            check(self._dirty_count == sum(map(len, self._dirty.values())), "_dirty_count")
            check(self._dirty_by_publisher == _by_publisher(self._dirty), "_dirty_by_publisher")
            check(all(self._dirty.get(key, {}).get(publisher, ()) is None for key, publisher in self._withdrawn),
                  "a record in _withdrawn isn't a deleted row in _dirty")
            check(self._dirty_standing | self._dirty_seen == set(self._pinned_standings),
                  "the standings kept until saved aren't those changed")
            for publisher, standing in self._standings.items():
                written = standings.get(publisher)
                check((written and written.row()) == (standing and standing.row()),
                      f"{_shown(publisher)}'s cached standing")  # a damaged row's, read, may be of any type
            check(len(self._standings) <= STANDING_CACHE, "the standing cache is over its size")
            held: dict[bytes, int] = {}
            for _, publisher in records:
                held[publisher] = held.get(publisher, 0) + 1
            check(set(held) <= set(standings), "a publisher with records has no standing")
            by_tier = [0, 0, 0, 0]
            for publisher, standing in standings.items():
                check(standing.records == held.get(publisher, 0),
                      f"{publisher.hex()} holds {held.get(publisher, 0)} records, its standing says {standing.records}")
                check(standing.records > 0 or standing.days > 0, f"{publisher.hex()}'s empty standing was kept")
                check(standing.first_seen is not None, f"{publisher.hex()} has no first-seen time")
                by_tier[standing.tier] += standing.records
            check(by_tier == self._tier_records, f"_tier_records {self._tier_records}, records by tier {by_tier}")
            check(self._publishers == len(held), f"_publishers {self._publishers}, publishers holding {len(held)}")
            if self.own_id is not None and self.own_id in standings:
                check(standings[self.own_id].tier == OWN, "this node's records aren't in their own tier")
            latest = max([time for record in records.values() for time in record], default=float("-inf"))
            check(latest <= self._latest, f"a time held ({latest}) is later than _latest ({self._latest})")
