"""Keeping what this node provides advertised in the DHT (specification section 6).

- **What is provided** (provide_strategy): `pinned` provides every complete pin; `roots` also every complete
  fetched target while it is cached (the node says which cached targets qualify: gateway fetches never do).
  Only these CIDs' DAGs are served to /blocks (section 7). A CID whose DAG holds a block the deny list (the
  storage's) denies is never provided: a change to the list takes effect at the next refresh(). A refresh
  recomputes this only when something it is computed from changed (the node's generation(), the deny list,
  the key sources' keys, the strategy, provide_entries and, on slow nodes, the network size it is judged by), so
  the many refreshes that change nothing cost almost nothing.
- **Entries** (provide_entries): also every CID a directory entry names inside a complete pin of the user's own
  content (storage.is_own_source: the user's, the shared folder's, names' and apps' pins; not those other nodes
  asked for or followed names'), its files and directories at every depth (not chunks, file nodes or index nodes),
  so each can be found by its own CID. Never those of cached downloads. Up to provide_entries_max entry keys, pins
  taken oldest first and each pin's entries all or none; unset, on, but on a slow node only while republishing
  every key is estimated to take at most ENTRY_AIRTIME_ON of the channel (republish_airtime), and otherwise none at
  all. An entry's DAG is inside its pin's, so the served set doesn't change.
- **Publishing.** A CID is due when it has never been published or its republish_interval has passed. The due
  CIDs' K closest nodes are found by sweep providing (rnfs.dht.sweep): one lookup per region of due keys,
  PUBLISH_LOOKUPS at once (one at a time on slow nodes), regions sized by the network size estimate too
  (set_network_size). Each key is stored on its store set: its K closest (and
  each disjoint path's, when the region's lookup ran as disjoint paths), the ring servers of its widened lookup when
  the density check flags it on its region's servers (set_widening; the widened lookup counts as one of the round's
  lookups), and the servers of the parts add_store_part added. Keys are grouped by node, and each node gets its
  keys (and any withdrawals for it) in as few /provide requests as fit, over one identified link. A refused or
  failed store isn't retried until the next republish. The node also records itself locally.
- **Withdrawing.** When a CID stops being provided (unpinned, evicted, gc'd, damaged), its key is withdrawn
  from the nodes it was last published to, at once (paced on slow nodes), and from nodes of earlier rounds (or
  of a round cut short) whose record may not have expired yet: the closest set changes as nodes come and go.

- **State on disk.** What each provided item was last published to, and when, lives in the state database rather
  than in memory (rnfs.statedb: publish_items, a row per provided item with the time it is next due, indexed), and is
  read per round: a round reads the due rows, the most overdue first and at most ROUND_KEYS of them, and a wake asks
  the index for the earliest due time, so neither looks at the whole provided set; what memory holds per provided
  item is the provide set itself. Changes are kept in memory until the next save (the existing durability points: a
  refresh that changed the provide set, every SAVE_EVERY_NODES nodes or SAVE_EVERY_SECONDS of a round, its end, the
  node's stop), which writes them in one transaction (not durably: NORMAL). A round writes each item it published
  once, at its end; while it runs, only the stores servers answered are saved (publish_stores, a small row each), so a
  crash in a round still leaves where it stored, merged into the items' rows at the next start. Pending withdrawals
  stay in memory, saved with the rest (withdrawals). A restart doesn't republish what isn't due. A CID whose lookup
  found no node at all (nothing was sent) is retried after RETRY_INTERVAL rather than a whole republish interval; one
  whose round was stopped before every node was sent its keys stays due. Times are wall clock: when the clock goes
  back (by more than CLOCK_SLACK), times after it are brought back to it.

- **Feature keys** (DhtKey, e.g. kept names) are republished at max(republish interval, KEY_MIN_INTERVAL),
  with a fixed jitter of up to +-KEY_JITTER from the key's first byte, so they don't all come due in one burst
  but a region's keys come due together; always below PROVIDER_TTL / 2.
- **Slow nodes** pace lookups: at most SLOW_ROUND_LOOKUPS region and widened lookups per round and on average one
  per SLOW_LOOKUP_PACE seconds; never-published items go first, then the longest unpublished. A flagged key whose
  widened lookup doesn't fit in the round is left due for a later one. Lookups outside publishing that the node
  paces the same way (a search's fallback over disjoint paths) take their share through pace_lookups. Withdrawals
  are paced too: they ride along to the servers a round stores to, and each server only withdrawals reach costs one
  lookup. Other nodes send every withdrawal at once.
- A key both withdrawn from and provided to a node in one round is only provided (a restart may queue
  withdrawals of keys that are still provided).
- **Partition mode** (set_islands; specification section 6, "LoRa islands"): while the node is cut off from the
  servers beyond its island, a round stores to and withdraws from local servers only (the rest stay queued, and a
  store set's off-island servers count as not stored to), and each server's answer or failure is told to it.
- **The heal republish.** An item whose round reached no off-island server, in partition mode or after trying, is
  marked (persisted with its published row, in published_heal, so a restart during a partition keeps it); heal(),
  when partition mode ends (or at the first off-island contact after a restart), makes every marked item due, so the
  rounds that follow (paced as ever on slow nodes) put its records beyond the island within hours rather than at its
  next republish (3 days on a slow node). A round reaching an off-island server clears the mark.
"""
import hashlib
import math
import struct
import threading
import time
from collections import defaultdict
from collections.abc import Iterable
from typing import Callable

from rnfs import statedb
from rnfs.config import Config
from rnfs.constants import DAY, HOUR, ID_LENGTH, K, PROVIDER_TTL
from rnfs.content import dag, walker
from rnfs.content.cid import Cid
from rnfs.content.storage import Storage
from rnfs.dht.sweep import ID_BITS, Region, Sweep, find_region
from rnfs.errors import InvalidDag, NotFound
from rnfs.log import LOG_ERROR, LOG_NOTICE, LOG_VERBOSE, LOG_WARNING, log, node_name, plural

RETRY_INTERVAL = 30.0  # seconds before retrying a CID whose lookup found no nodes to publish to
MAX_WAIT = 60.0  # the loop wakes at least this often
KEY_PREFIX = "key:"  # how a DhtKey is written as a published item (str())
# A kept name's record changes rarely and keepers answer for years-old ones; its provider record lives
# PROVIDER_TTL (7 days), so republishing every 3 days still survives a missed round
KEY_MIN_INTERVAL = 3 * DAY
KEY_JITTER = 0.1  # +-10% of the interval, fixed per first byte of the key (256 slots)
# While a round sends, where it stored is saved after this many nodes or seconds, whichever comes first, so a
# crash loses at most that much of it (a record no saved state lists can't be withdrawn: it lives PROVIDER_TTL)
SAVE_EVERY_NODES = 10
SAVE_EVERY_SECONDS = 30.0
# Due items a round takes at most, the most overdue first (never published ones by key, so a large first round's
# items come due again in slices of the key space, each region's together, and later rounds need no more lookups
# than one round would): memory for a round stays bounded however many keys are due. A round holds about 2 KB per
# key while it runs (about 100 MB at most)
ROUND_KEYS = 50_000
# The wall clock going back by more than this brings the times after it back to it (a full pass over the items);
# less is left: a time a few seconds ahead only delays a republish by as much
CLOCK_SLACK = 60.0
READ_BATCH = 500  # items read from the state database in one query (well within SQLite's variable limit)
SCAN_BATCH = 10_000  # rows the first refresh reads at a time looking for items no longer provided
# Slow nodes: a closest-nodes lookup is several /find exchanges (alpha 1, a few hops), seconds of airtime at
# LoRa rates. One per 30 s keeps republishing a hundred keys to well under 1% of a day's airtime per hour
SLOW_LOOKUP_PACE = 30.0
SLOW_ROUND_LOOKUPS = 10  # region lookups per round on slow nodes, so /provide batching still groups keys per node
# Other nodes: region lookups a round runs at once. Each keeps alpha (3) /find queries in flight, so 4
# keep about 12, small requests (a link still carries at most MAX_REQUESTS_PER_LINK at once; the rest wait for a
# slot); a round of a few hundred keys, each lookup a few round trips one after another, then takes about a
# quarter of the time, with few threads. The airtime controls (upload_budget, interface rules, quiet hours)
# apply to /blocks only (spec section 3): these send the same /find messages as one at a time would, overlapping
PUBLISH_LOOKUPS = 4
# Other nodes: servers a round sends /provide to at once, each over its own identified link and in its own batches
# (withdrawals first, stopping at the first refusal), so a server sees what it would one at a time. A /provide is
# latency-bound (path, link setup, identify, a packet a batch), not bandwidth-bound: 16 links in flight are a few
# kilobytes, on the order of the /find the lookups already keep in flight (12). The load study puts a fast node's
# round for 1M keys at about 1.1 h with sweep providing and 16 at once (83 h with a lookup per key, one server at a
# time). The airtime controls apply to /blocks only (spec section 3). Slow nodes send one at a time
PROVIDE_PARALLEL = 16

# provide_entries (spec section 6). Each entry key costs about 165 bytes a republish round (16 bytes to each of K
# servers, and request overhead) and multiplies a node's keys 3-5x for folders of a few files, over 100x for large
# ones: past provide_entries_max (default 100,000; a 70k-file folder fits, at ~11.6 MB a day), the entries of the
# newest pins are left out
# Slow nodes provide entries by default only while republishing every key is estimated (republish_airtime) to take
# at most ENTRY_AIRTIME_ON of the channel over a republish interval; once on, they stay on up to ENTRY_AIRTIME_OFF,
# so a network size estimate wavering around the line doesn't withdraw and provide every entry again and again
ENTRY_AIRTIME_ON = 0.02
ENTRY_AIRTIME_OFF = 0.025
# Republish airtime at REFERENCE_BITRATE, from the load and topology studies (0.2.0-progress.md, finding 5): a key's
# K stores perfectly batched take KEY_AIRTIME, and a store set holds STORES_PER_KEY servers (its K closest and about
# 2 senior ones); a region's lookup (2 disjoint paths, its extra /find) takes REGION_AIRTIME, the slow end of 34-44 s;
# each server contacted costs LINK_AIRTIME for its identified link. A round needs a region lookup per
# SERVERS_PER_REGION servers at most
REFERENCE_BITRATE = 1000.0
KEY_AIRTIME = 2.63
STORES_PER_KEY = K + 2
REGION_AIRTIME = 44.0
LINK_AIRTIME = 6.4
SERVERS_PER_REGION = 12.5
# The network size assumed before any region lookup has given an estimate (a lower bound: the servers known). Past
# about 500 servers the estimate no longer changes (every key is a region of its own), so this is the cautious end
UNKNOWN_NETWORK = 1000
REGION_SMOOTHING = 16  # the network size estimate follows region sizes as a running mean over about this many
# Its meta values: "region_bits" (that mean), "entries_on" (the last decision), "interval" (the republish interval the
# items' due times were computed with) and "clock" (the latest wall-clock time the rows may hold)
META_STORE = "publisher"


def _in_order(items: list, work: Callable, should_stop: Callable[[], bool], parallel: int):
    """Yield (item, work(item)) for items in their order, running work for up to parallel items at once on
    rnfs-publish-provide threads (in the caller's, one at a time, for parallel 1). should_stop() is checked before
    each item starts: once it is True no more start, and those running finish and are yielded. When work raises, no
    more start; the exception is raised once the items that ran have been yielded, so what they did is recorded."""
    if parallel <= 1:
        for item in items:
            if should_stop():
                return
            yield item, work(item)
        return
    cond = threading.Condition()
    results: dict[int, tuple[bool, object]] = {}  # index -> (raised, result or exception)
    state = {"next": 0, "ended": False}  # the next index to start; whether no more may start

    def worker():
        """Run work for the next item in turn until none is left or the round ends."""
        while True:
            with cond:
                if state["ended"] or state["next"] == len(items):
                    return
                if should_stop():
                    state["ended"] = True
                    cond.notify_all()
                    return
                index = state["next"]
                state["next"] += 1
            try:
                result = (False, work(items[index]))
            except BaseException as e:  # noqa: BLE001 - raised in the caller's thread once the others are in
                result = (True, e)
            with cond:
                results[index] = result
                if result[0]:
                    state["ended"] = True
                cond.notify_all()

    threads = [threading.Thread(target=worker, name="rnfs-publish-provide", daemon=True)
               for _ in range(min(parallel, len(items)))]
    for thread in threads:
        thread.start()
    failure = None
    try:
        for index in range(len(items)):
            with cond:
                # Wait for this item unless it will never start
                while index not in results and not (state["ended"] and index >= state["next"]):
                    cond.wait()
                if index not in results:
                    break
                raised, result = results.pop(index)
            if raised:
                failure = failure or result
                continue
            yield items[index], result
    finally:
        with cond:
            state["ended"] = True  # the caller may have stopped iterating (an exception of its own)
        for thread in threads:
            thread.join()
    if failure is not None:
        raise failure


class DhtKey:
    """A DHT key a feature provides that isn't a CID's (e.g. a kept name, rnfs.features.keepers).

    Published, republished and withdrawn exactly like a provided CID, batched with them, but never served
    to /blocks. It quacks like a Cid where the publisher needs it: .key, hashing, str() and parse().
    """

    __slots__ = ("key",)

    def __init__(self, key: bytes):
        """The provided key (ID_LENGTH bytes; callers domain-separate it from CID keys)."""
        self.key = bytes(key)

    @classmethod
    def parse(cls, text: str) -> "DhtKey":
        """The key written by str(). Raises ValueError."""
        if not isinstance(text, str) or not text.startswith(KEY_PREFIX):
            raise ValueError(f"not a provided key: {text!r}")
        return cls(bytes.fromhex(text[len(KEY_PREFIX):]))

    def __str__(self) -> str:
        return KEY_PREFIX + self.key.hex()

    def __repr__(self) -> str:
        return f"DhtKey({self.key.hex()})"

    def __eq__(self, other) -> bool:
        return isinstance(other, DhtKey) and other.key == self.key

    def __hash__(self) -> int:
        return hash((DhtKey, self.key))


# A part of each key's store set besides its K closest (add_store_part): (key, its region) -> servers
StorePart = Callable[[bytes, Region], Iterable[bytes]]
# The density check over a key's region (set_widening): (key, its region) -> True if the key needs a widened lookup
Flags = Callable[[bytes, Region], bool]
# The widened lookup of a key flagged: (key, its region) -> the ring servers to store it on too
Widen = Callable[[bytes, Region], list[bytes]]

# What the publisher provides: a CID, or a feature's DhtKey. Both have .key, hashing, str() and parse(), but only a
# Cid has .binary or is served, so code that needs those must check (provided() does)
ProvidedItem = Cid | DhtKey


def key_interval(key: bytes, base: float) -> float:
    """The republish interval of DhtKey key: max(base, KEY_MIN_INTERVAL) with a fixed jitter, below
    PROVIDER_TTL / 2 (base is already below it: Config.validate).

    The jitter comes from the key's first byte, not the whole key: keys of one region share their leading bits, so
    they come due together and one lookup republishes them all (sweep providing), while the 256 slots still keep
    many keys from coming due at once."""
    interval = max(base, KEY_MIN_INTERVAL)
    fraction = int.from_bytes(hashlib.sha256(b"rnfs-republish-jitter" + key[:1]).digest()[:8], "big") / 2**64
    jittered = interval * (1 + KEY_JITTER * (2 * fraction - 1))
    # Jitter upward never reaches PROVIDER_TTL / 2: a record must survive one missed republish
    return min(jittered, max(interval, PROVIDER_TTL / 2 - HOUR))


def republish_airtime(keys: int, servers: int, bitrate: float | None = None) -> float:
    """Seconds of channel airtime a round republishing keys keys to a network of servers servers takes, from a
    node whose slowest interface runs at bitrate bits/s (None: REFERENCE_BITRATE).

    keys × KEY_AIRTIME × STORES_PER_KEY / K for the stores, a REGION_AIRTIME lookup for each region (one per key,
    up to one per SERVERS_PER_REGION servers: sweep providing) and LINK_AIRTIME for each server contacted (at most
    STORES_PER_KEY per key, at most every server), all scaled by REFERENCE_BITRATE / bitrate."""
    if keys <= 0:
        return 0.0
    servers = max(1, servers)
    regions = min(keys, math.ceil(servers / SERVERS_PER_REGION))
    contacted = min(servers, keys * STORES_PER_KEY)
    seconds = keys * KEY_AIRTIME * STORES_PER_KEY / K + regions * REGION_AIRTIME + contacted * LINK_AIRTIME
    return seconds * REFERENCE_BITRATE / (bitrate if bitrate else REFERENCE_BITRATE)


def network_size(region_bits: float | None, known: int) -> int:
    """The network size republish_airtime is given: from region_bits (the running mean of the prefix length regions
    shared with their K-th closest server, so about K × 2^(bits + 0.5) servers), at least known (the servers known);
    UNKNOWN_NETWORK at least while there is no estimate."""
    if region_bits is None:
        return max(known, UNKNOWN_NETWORK)
    return max(known, round(K * 2 ** (region_bits + 0.5)))


def _parse_item(text: str) -> ProvidedItem:
    """A provided item as str() writes it (the published tables' item). Raises ValueError."""
    return DhtKey.parse(text) if isinstance(text, str) and text.startswith(KEY_PREFIX) else Cid.parse(text)


def _item(text) -> ProvidedItem:
    """A saved provided item: _parse_item, but only an ID_LENGTH-byte DhtKey. Raises ValueError."""
    item = _parse_item(text)
    if isinstance(item, DhtKey) and len(item.key) != ID_LENGTH:
        raise ValueError(f"not a provided key: {text!r}")
    return item


def _is_number(value) -> bool:
    """True if value is a finite int or float (not a bool)."""
    return type(value) in (int, float) and math.isfinite(value)


def _time(number) -> float:
    """A saved time; ValueError if it isn't a finite number."""
    if isinstance(number, bool) or not isinstance(number, (int, float)) or not math.isfinite(number):
        raise ValueError(f"not a time: {number!r}")
    return float(number)


def _id(text) -> bytes:
    """A node ID or key as published.json wrote it (hex). Raises ValueError or TypeError."""
    value = bytes.fromhex(text)
    if len(value) != ID_LENGTH:
        raise ValueError(f"not {ID_LENGTH} bytes: {text!r}")
    return value


def _ids(blob) -> list[bytes]:
    """The node IDs a published row's nodes concatenates. Raises ValueError."""
    if not isinstance(blob, bytes) or len(blob) % ID_LENGTH:
        raise ValueError(f"not a list of node IDs: {blob!r}")
    return [blob[i:i + ID_LENGTH] for i in range(0, len(blob), ID_LENGTH)]


def import_published(value: dict, tx) -> int:
    """Import published.json (statedb.migrate): its last rounds, earlier rounds' nodes and pending withdrawals
    replace the publisher's rows (publish_items, through the published and published_earlier tables rnfs 0.1.x kept,
    which statedb.move_published empties again, and withdrawals). Each entry is checked as rnfs 0.1.4 loaded it, one
    of the wrong shape being skipped, and node IDs, keys and DhtKeys must be ID_LENGTH bytes; times are kept as
    written (the load clamps them, and earlier nodes past PROVIDER_TTL are dropped as rows are read). Returns the
    number of rows imported."""
    for table in ("published", "published_earlier", "withdrawals", "publish_items"):
        tx.execute(f"DELETE FROM {table}")
    published: dict[str, tuple[float, bytes]] = {}
    for text, record in value.get("published", {}).items():
        try:
            at, nodes = record
            if not isinstance(nodes, list):
                raise TypeError("nodes must be a list")
            published[str(_item(text))] = (_time(at), b"".join(_id(n) for n in nodes))
        except (ValueError, TypeError, OverflowError):
            continue
    earlier: dict[str, dict[bytes, float]] = {}
    for text, nodes in value.get("earlier", {}).items():
        try:
            if not isinstance(nodes, dict):
                raise TypeError("nodes must be an object")
            earlier[str(_item(text))] = {_id(n): _time(stored) for n, stored in nodes.items()}
        except (ValueError, TypeError, OverflowError):
            continue
    withdrawals: dict[bytes, set[bytes]] = {}
    for node_hex, keys in value.get("withdraw", {}).items():
        try:
            if not isinstance(keys, list):
                raise TypeError("keys must be a list")
            withdrawals[_id(node_hex)] = {_id(k) for k in keys}
        except (ValueError, TypeError):
            continue
    rows = [[(text, at, nodes) for text, (at, nodes) in published.items()],
            [(text, node, at) for text, nodes in earlier.items() for node, at in nodes.items()],
            [(node, key) for node, keys in withdrawals.items() for key in keys]]
    tx.executemany("INSERT INTO published (item, at, nodes) VALUES (?, ?, ?)", rows[0])
    tx.executemany("INSERT INTO published_earlier (item, node, at) VALUES (?, ?, ?)", rows[1])
    tx.executemany("INSERT INTO withdrawals (node, key) VALUES (?, ?)", rows[2])
    statedb.move_published(tx)
    return sum(len(table) for table in rows)


def _item_id(item: ProvidedItem) -> bytes:
    """item as publish_items keys it: a CID's binary form, or KEY_ITEM_PREFIX and a DhtKey's key."""
    return statedb.KEY_ITEM_PREFIX + item.key if isinstance(item, DhtKey) else item.binary


def _from_id(blob) -> ProvidedItem:
    """The item publish_items' item names (_item_id). Raises ValueError."""
    if not isinstance(blob, bytes):
        raise ValueError(f"not an item: {blob!r}")
    if blob[:1] == statedb.KEY_ITEM_PREFIX:
        if len(blob) != 1 + ID_LENGTH:
            raise ValueError(f"not a provided key: {blob!r}")
        return DhtKey(blob[1:])
    return Cid.from_bytes(blob)


def _maybe_item(blob) -> ProvidedItem | None:
    """The item publish_items' item names, or None if it is malformed."""
    try:
        return _from_id(blob)
    except ValueError:
        return None


def _optional_time(value) -> float | None:
    """A saved time that may be NULL; ValueError if it is neither."""
    return None if value is None else _time(value)


class _Item:
    """What the publisher knows of one provided item, a publish_items row: when it was last published (at, None if
    never or that round was cut short) and to which servers (nodes), other servers that may still hold a record of it
    with when it was stored there (earlier), when it is tried again sooner than its republish (retry, or None), and
    whether its last round reached no server beyond the island (heal)."""

    __slots__ = ("at", "nodes", "earlier", "retry", "heal")

    def __init__(self, at: float | None = None, nodes: list[bytes] | None = None,
                 earlier: dict[bytes, float] | None = None, retry: float | None = None, heal: bool = False):
        """An item's state; by default one never published."""
        self.at = at
        self.nodes = nodes if nodes is not None else []
        self.earlier = earlier if earlier is not None else {}
        self.retry = retry
        self.heal = heal

    @classmethod
    def from_row(cls, at, retry, nodes, earlier, heal, now: float) -> "_Item":
        """The state a row's columns hold; times after now (the clock went back) read as now, and earlier servers
        whose record has expired (PROVIDER_TTL) are left out. Raises ValueError (or TypeError) if a column is
        malformed."""
        if not isinstance(earlier, bytes) or len(earlier) % (ID_LENGTH + statedb.ITEM_TIME.size) or heal not in (0, 1):
            raise ValueError("malformed earlier servers or mark")
        at, retry = _optional_time(at), _optional_time(retry)
        servers: dict[bytes, float] = {}
        step = ID_LENGTH + statedb.ITEM_TIME.size
        for i in range(0, len(earlier), step):
            stored = _time(statedb.ITEM_TIME.unpack_from(earlier, i + ID_LENGTH)[0])
            if now - stored < PROVIDER_TTL:
                servers[earlier[i:i + ID_LENGTH]] = min(stored, now)
        return cls(None if at is None else min(at, now), _ids(nodes), servers, retry, bool(heal))

    def row(self, item: ProvidedItem, due: float) -> tuple:
        """The publish_items row of item in this state, due at due."""
        earlier = b"".join(node + statedb.ITEM_TIME.pack(at) for node, at in self.earlier.items())
        return (item.key, _item_id(item), due, self.at, self.retry, b"".join(self.nodes), earlier, int(self.heal))


_ITEM, _NODE, _META = "item", "node", "meta"  # the kinds of key _PublishState writes
# Its markers: what else a save writes
_ADDED, _STORES, _ROUND_END = (("marker", "added"), ("marker", "stores"), ("marker", "round end"))
_MISSING = object()  # Publisher._items has no entry for an item


class _PublishState(statedb.RowStore):
    """The publisher's rows and its meta values, saved together (not durably: NORMAL) in one transaction. A key is
    (_ITEM, item), whose publish_items row is written again as Publisher._items holds it (deleted if None), (_NODE,
    node ID), whose withdrawals rows are, (_META, name), the meta value name (META_STORE), or a marker: _ADDED (the
    items refresh() added get a row if they have none), _STORES (the round's stores so far go to publish_stores) or
    _ROUND_END (publish_stores is emptied: the round's items have their rows).

    Once a save is written, the items it wrote that haven't changed since leave Publisher._items (unless the round in
    progress holds them): the database has them as memory did."""

    def __init__(self, db: statedb.StateDb, publisher: "Publisher"):
        """The saves of publisher's state, whose lock guards it."""
        super().__init__(db, "publish_items", ("key",), durable=False, mode=statedb.SNAPSHOT, lock=publisher._lock)
        self.publisher = publisher

    def is_dirty(self, key) -> bool:
        """True if key changed and isn't written yet (lock held)."""
        return key in self._dirty

    def snapshot(self, keys) -> dict:
        """What a write of keys writes, taken under the lock: the items written ("items"), their rows ("rows") and
        those deleted ("deleted"), the batches of items refresh() added and the provide set then ("added",
        "provided"), the round's stores so far ("stores", how many "stored", their rows "store_rows", and the round
        whose item list they include, if they do: "listed"), whether the round ended ("round_end"), the node IDs and
        their withdrawal rows ("nodes", "withdrawals") and the meta values ("meta")."""
        publisher = self.publisher
        items = [key for kind, key in keys if kind == _ITEM]
        rows, deleted = [], []
        for item in items:
            state = publisher._items.get(item, _MISSING)
            if state is None:
                deleted.append((item.key,))
            elif state is not _MISSING:
                rows.append(state.row(item, publisher._due_of(item, state)))
        added = list(publisher._added) if _ADDED in keys else []
        stores, store_rows, listed = publisher._stores, [], None
        if _STORES in keys and stores:
            if not publisher._round_listed:
                # The round's items, once, so each server's row needs only their places
                listed = publisher._round_number
                store_rows.append((b"", 0.0, b"".join(bytes([len(item_id)]) + item_id
                                                      for item_id in map(_item_id, publisher._round_order))))
            index = publisher._round_index
            store_rows += [(node, at, struct.pack(f">{len(items)}I", *(index[item] for item in items)))
                           for node, at, items in stores]
        nodes = [key for kind, key in keys if kind == _NODE]
        withdrawals = [(node, key) for node in nodes for key in publisher._withdrawals.get(node, ())]
        meta = [(name, publisher._meta_value(name)) for kind, name in keys if kind == _META]
        if rows or store_rows or added:
            publisher._latest = max(publisher._latest, publisher.clock())
            meta.append(("clock", publisher._latest))  # every time written is at most this
        return {"items": items, "rows": rows, "deleted": deleted, "added": added, "provided": publisher._provided,
                "stores": stores, "stored": len(stores) if _STORES in keys else 0, "store_rows": store_rows,
                "listed": listed, "round_end": _ROUND_END in keys, "nodes": nodes,
                "withdrawals": withdrawals, "meta": meta}

    def write_rows(self, tx, snapshot: dict):
        """Write the snapshot inside transaction tx."""
        rows, deleted, added, provided = snapshot["rows"], snapshot["deleted"], snapshot["added"], snapshot["provided"]
        for name, value in snapshot["meta"]:
            if value is None:
                tx.execute("DELETE FROM meta WHERE store = ? AND key = ?", (META_STORE, name))
            else:
                statedb.set_meta(tx, META_STORE, name, value)
        # Each in the table's order: a transaction changing more pages than SQLite's cache holds writes some of them
        # more than once in random order, many times in all (a round's rows: about 20x)
        tx.executemany("DELETE FROM publish_items WHERE key = ?", sorted(deleted))
        tx.executemany("INSERT OR REPLACE INTO publish_items (key, item, due, at, retry, nodes, earlier, heal)"
                       " VALUES (?, ?, ?, ?, ?, ?, ?, ?)", sorted(rows))
        # A new item's row says it was never published; one already there (a restart) is kept. Items no longer
        # provided (removed since) get none
        gone = {key for (key,) in deleted}
        new = sorted((item.key, _item_id(item)) for batch in added for item in batch
                     if item in provided and item.key not in gone)
        tx.executemany("INSERT OR IGNORE INTO publish_items (key, item, due, at, retry, nodes, earlier, heal)"
                       " VALUES (?, ?, 0, NULL, NULL, x'', x'', 0)", new)
        if snapshot["round_end"]:
            tx.execute("DELETE FROM publish_stores")
        tx.executemany("INSERT INTO publish_stores (node, at, items) VALUES (?, ?, ?)", snapshot["store_rows"])
        tx.executemany("DELETE FROM withdrawals WHERE node = ?", [(node,) for node in snapshot["nodes"]])
        tx.executemany("INSERT OR REPLACE INTO withdrawals (node, key) VALUES (?, ?)", snapshot["withdrawals"])

    def written(self, snapshot: dict):
        """The snapshot was committed: forget what it wrote that hasn't changed since."""
        publisher = self.publisher
        stores, stored = snapshot["stores"], snapshot["stored"]
        with self.lock:
            for item in snapshot["items"]:
                if (item not in publisher._round and not self.is_dirty((_ITEM, item))
                        and item in publisher._items):
                    del publisher._items[item]
            done = {id(batch) for batch in snapshot["added"]}
            publisher._added = [batch for batch in publisher._added if id(batch) not in done]
            if stored and stores is publisher._stores:
                del stores[:stored]
            if snapshot["listed"] is not None and snapshot["listed"] == publisher._round_number:
                publisher._round_listed = True


class Publisher:
    """The provide set, the served set, and the publish/withdraw schedule. Thread-safe."""

    def __init__(self, own_id: bytes, config: Config, storage: Storage, providers, client,
                 make_lookup: Callable[[bytes], object], slow_node: bool = False, state_dir: str | None = None,
                 cached_eligible: Callable[[], set[Cid]] = set, clock: Callable[[], float] = time.time,
                 retry_interval: float = RETRY_INTERVAL, generation: Callable[[], object] | None = None,
                 bitrate: float | None = None, known_servers: Callable[[], int] = lambda: 0,
                 lookups_at_once: int | None = None):
        """Publisher for own_id. providers is the ProviderStore (to record ourselves), client the PeerClient.

        make_lookup(key) returns a dht.lookup.Lookup; cached_eligible() returns the cached targets the
        roots strategy may provide. generation(), if given, returns a value that changes whenever storage's pins,
        cached targets or blocks, or cached_eligible(), may have: refresh() skips its recompute while it, the deny
        list and the key sources' keys are as at the last one (without it, every refresh recomputes). bitrate (the
        slowest interface's, bits/s; None if unknown) and known_servers() (the routing table's size, a lower bound
        of the network's) decide whether a slow node provides entries by default (republish_airtime).
        lookups_at_once is how many region lookups a round runs at once on a node that isn't slow (PUBLISH_LOOKUPS if
        None; 1 makes a round's outcome independent of thread timing, for tests that measure placement).
        """
        self.own_id = own_id
        self.config = config
        self.storage = storage
        self.providers = providers
        self.client = client
        self.make_lookup = make_lookup
        self.slow_node = slow_node
        self.cached_eligible = cached_eligible
        self.clock = clock
        self.retry_interval = retry_interval
        self.generation = generation
        self.bitrate = bitrate
        self.known_servers = known_servers
        # A slow node's radio's measured load (rnfs.network.channel.ChannelLoad, set by the node): stretch(wait, slack)
        # is how long a round's lookups pace the next round instead of wait (at most max(wait, slack), slack being how
        # long republishing may slip before a record would go unrepublished past PROVIDER_TTL / 2), and
        # measured_airtime() the share of the channel this node's own transmissions took over the last hour (None:
        # unknown), over ENTRY_AIRTIME_ON of which a slow node's default doesn't turn entries on
        self.stretch: Callable[[float, float], float] = lambda wait, slack: wait
        self.measured_airtime: Callable[[], float | None] = lambda: None
        self.lookups_at_once = PUBLISH_LOOKUPS if lookups_at_once is None else max(1, lookups_at_once)
        self._lock = threading.RLock()
        self._wake = threading.Event()
        self._provided: frozenset[ProvidedItem] = frozenset()
        self._served: frozenset[Cid] = frozenset()
        # What _compute last built, and from what, so a refresh that changed nothing needn't build it again: the
        # served set from each provided CID's dag_blocks (memoised by storage, so the same object while its DAG
        # stays complete), and the DhtKeys from the keys the key sources returned
        self._served_from: tuple[dict[Cid, frozenset[Cid]], frozenset[Cid]] = ({}, frozenset())
        self._keys_from: tuple[set[bytes], frozenset[DhtKey]] = (set(), frozenset())
        # CID -> (deny list's CID set, dag_blocks object, whether one of them is denied), so each DAG is checked
        # against the deny list once per change of either
        self._denied_dags: dict[Cid, tuple[frozenset[bytes], frozenset[Cid], bool]] = {}
        # provide_entries: the entries provided that aren't provided as roots too; each complete pin's entries
        # (_walk_entries, kept while it is a complete pin: a CID's content never changes); and what the last
        # choice of entries was made from and gave: (own_pinned_cids() then, the complete pins, the cap, entries
        # chosen, pins left out at the cap), so a refresh that changed no pin chooses nothing again
        self._entries: frozenset[Cid] = frozenset()
        self._entries_of: dict[Cid, frozenset[Cid]] = {}
        self._entries_from: tuple = (None, frozenset(), None, frozenset(), ())
        self._entries_off: str | None = None  # why entries aren't provided (the setting, or a slow node's airtime)
        self._estimated_off = False  # whether the slow-node estimate is why (logged when that changes)
        self._entries_on = False  # whether the last computing refresh provided entries (persisted, as "entries_on")
        self._region_bits_mean: float | None = None  # the network size estimate (network_size), persisted
        # (generation(), deny list's CID set, keys, strategy, what entries depend on) of the last full refresh;
        # None: the next computes
        self._inputs: tuple | None = None
        # The items' state changed since the last save, and that of the items the round in progress holds (_round),
        # over what the state database has (publish_items): item -> its state, or None if its row is to go
        self._items: dict[ProvidedItem, _Item | None] = {}
        self._round: set[ProvidedItem] = set()  # items of the round in progress, kept in _items until it ends
        # The round's stores not saved yet (publish_stores): (server, when it answered, the items it recorded)
        self._stores: list[tuple[bytes, float, list[ProvidedItem]]] = []
        self._round_stored: set[ProvidedItem] = set()  # the round's items a server recorded
        self._round_order: list[ProvidedItem] = []  # the round's items in order, and each one's place in it
        self._round_index: dict[ProvidedItem, int] = {}
        self._round_listed = False  # whether publish_stores lists the round's items
        self._round_number = 0  # counts rounds, so a save of an earlier round's list isn't taken for this one's
        self._added: list[frozenset[ProvidedItem]] = []  # items refresh() added, to get a row at the next save
        self._synced = False  # whether a refresh has looked for rows of items no longer provided (the first does)
        self._latest = 0.0  # the latest clock time seen (CLOCK_SLACK), and saved ("clock")
        self._due_interval: float | None = None  # the republish interval the rows' due times were computed with
        self._withdrawals: dict[bytes, set[bytes]] = {}  # node -> keys to withdraw from it
        self._next_lookup = 0.0  # slow nodes: clock time before which no lookup round starts
        # ... at most this far from now (a wall clock corrected backwards): a full round's pacing, or the last stretched
        self._pace_cap = SLOW_LOOKUP_PACE * SLOW_ROUND_LOOKUPS
        self._region_bits: int | None = None  # the prefix length of a recent region (sweep providing), or None
        self._store_parts: list[StorePart] = []  # add_store_part
        self._widening: tuple[Flags, Widen] | None = None  # set_widening
        self._sizes: Callable[[bytes], float | None] | None = None  # set_network_size
        self._islands = None  # set_islands: the node's partition mode (rnfs.dht.local.PartitionMode), if any
        # Functions returning the non-CID keys features provide (add_key_source); polled by refresh()
        self._key_sources: list[Callable[[], set[bytes]]] = []
        # Writes what changed since the last save (_touch_item, _touch_node); without a state directory, to a
        # private in-memory database
        self._store: _PublishState | None = None
        if state_dir is None:
            self._db = statedb.connect_memory(("statedb", "publisher", "publisher_heal", "publisher_on_disk"))
            self._load(self._db)
        else:
            self._db = statedb.open_store(state_dir, self._load)

    def add_key_source(self, source: Callable[[], set[bytes]]):
        """Also provide the keys source() returns (DhtKeys), recomputed at each refresh(). Must not block."""
        with self._lock:
            self._key_sources.append(source)
            self._inputs = None  # its keys aren't in the provided set yet

    @property
    def interval(self) -> float:
        """Seconds between republishes: republish_interval, 3 days on slow nodes by default."""
        return self.config.effective_republish_interval(self.slow_node)

    def interval_for(self, item: ProvidedItem) -> float:
        """Seconds between republishes of item: interval for a CID, key_interval for a DhtKey."""
        if isinstance(item, DhtKey):
            return key_interval(item.key, self.interval)
        return self.interval

    # Persistence

    def _load(self, db: statedb.StateDb):
        """Load pending withdrawals and the meta values, and make the items' rows ready to be read per round: the
        stores of a round a crash ended are merged into them, and if the clock has gone back since they were written
        (or the republish interval isn't the one their due times were computed with) they are brought up to date.
        Withdrawal rows of the wrong shape are skipped (logged) and deleted by the next save; malformed item rows are
        skipped (and rewritten) as rounds read them."""
        self._db, self._store = db, _PublishState(db, self)
        now = self.clock()
        malformed = 0
        for node, key in db.query("SELECT node, key FROM withdrawals"):
            if not all(isinstance(value, bytes) and len(value) == ID_LENGTH for value in (node, key)):
                malformed += 1
                self._touch_node(node)
                continue
            self._withdrawals.setdefault(node, set()).add(key)
        bits = db.meta(META_STORE, "region_bits")
        if _is_number(bits) and 0 <= bits <= ID_BITS:
            self._region_bits_mean = float(bits)
        self._entries_on = db.meta(META_STORE, "entries_on") == 1
        if malformed:
            log(f"publisher: ignoring {plural(malformed, 'malformed row')} in the state database", LOG_WARNING)
        malformed += self._merge_stores(db, now)
        interval, latest = db.meta(META_STORE, "interval"), db.meta(META_STORE, "clock")
        if _is_number(interval):
            self._due_interval = float(interval)
        self._latest = float(latest) if _is_number(latest) else math.inf  # unknown: as if the clock had gone back
        with self._lock:
            self._clamp(now)
            self._check_interval()

    def _merge_stores(self, db: statedb.StateDb, now: float) -> int:
        """Merge the stores a round saved as it went (publish_stores, left by a crash in it) into their items' earlier
        servers, and queue withdrawals of those whose item has no row (it stopped being provided meanwhile), in one
        transaction. Returns how many rows were malformed (skipped)."""
        rows = db.query("SELECT node, at, items FROM publish_stores ORDER BY rowid")
        if not rows:
            return 0
        stored: dict[bytes, dict[bytes, float]] = defaultdict(dict)  # item -> server -> when
        malformed = 0
        listed: list[bytes] = []  # the round's items, from its first row
        for node, at, items in rows:
            try:
                if not isinstance(node, bytes) or not isinstance(items, bytes):
                    raise ValueError(f"not a store: {node!r}")
                if node == b"":
                    listed, i = [], 0
                    while i < len(items):
                        listed.append(items[i + 1:i + 1 + items[i]])
                        _from_id(listed[-1])
                        i += 1 + items[i]
                    continue
                if len(node) != ID_LENGTH or len(items) % 4:
                    raise ValueError(f"not a store: {node!r}")
                when = min(_time(at), now)
                ids = [listed[n] for n in struct.unpack(f">{len(items) // 4}I", items)]
            except (ValueError, TypeError, IndexError):
                malformed += 1
                continue
            for item_id in ids:
                stored[item_id][node] = max(when, stored[item_id].get(node, when))
        found = {}
        keys = [_from_id(item_id).key for item_id in stored]
        for start in range(0, len(keys), READ_BATCH):
            batch = keys[start:start + READ_BATCH]
            found.update({row[0]: row for row in db.query(
                "SELECT key, item, due, at, retry, nodes, earlier, heal FROM publish_items WHERE key IN "
                f"({', '.join('?' * len(batch))})", batch)})
        merged = []
        with self._lock:
            for item_id, servers in stored.items():
                item = _from_id(item_id)
                row = found.get(item.key)
                if row is None:
                    for node in servers:
                        self._withdrawals.setdefault(node, set()).add(item.key)
                        self._touch_node(node)
                    continue
                try:
                    state = _Item.from_row(*row[3:], now)
                except (ValueError, TypeError):
                    state = _Item()  # malformed: never published, so republished soon
                    malformed += 1
                for node, at in servers.items():
                    state.earlier[node] = max(at, state.earlier.get(node, at))
                merged.append(state.row(item, row[2]))
        with db.transaction() as tx:
            tx.executemany("INSERT OR REPLACE INTO publish_items (key, item, due, at, retry, nodes, earlier, heal)"
                           " VALUES (?, ?, ?, ?, ?, ?, ?, ?)", sorted(merged))
            tx.execute("DELETE FROM publish_stores")
        log(f"publisher: the last round was cut short; kept where it stored {plural(len(stored), 'key')}", LOG_NOTICE)
        return malformed

    def _check_interval(self):
        """If the republish interval isn't the one the rows' due times were computed with, compute them again. Lock
        held."""
        if self._due_interval != self.interval:
            self._recompute()

    def _clamp(self, now: float):
        """If the wall clock has gone back (by more than CLOCK_SLACK) since a time was saved or seen, bring every
        time after now back to now (retries to now + retry_interval), so republishing and pacing don't stall until
        the clock catches up. Lock held."""
        if now < self._latest - CLOCK_SLACK:
            self._recompute(now)
            self._latest = now
            self._touch_meta("clock")
        self._latest = max(self._latest, now)
        self._next_lookup = min(self._next_lookup, now + self._pace_cap)

    def _recompute(self, clamp_to: float | None = None):
        """Compute the rows' due times again with the republish interval (and first, with clamp_to, bring their times
        after it back to it: retries to clamp_to + retry_interval), in one transaction, and the same in memory: a pass
        over every row, writing only those that change. Lock held.

        A direct write: a save in progress may land after it, but only with rows of items memory still holds (_items),
        which the next save writes again as they are now."""
        interval = self.interval
        due = "CASE WHEN retry IS NOT NULL THEN retry WHEN at IS NULL THEN 0 ELSE at + ? END"
        with self._db.transaction() as tx:
            if clamp_to is not None:
                retry = clamp_to + self.retry_interval
                # Earlier servers' times are brought back as rows are read (_Item.from_row)
                tx.execute("UPDATE publish_items SET at = MIN(at, ?), retry = MIN(retry, ?) WHERE at > ? OR retry > ?",
                           (clamp_to, retry, clamp_to, retry))
            tx.execute(f"UPDATE publish_items SET due = {due} WHERE item >= x'01' AND due != {due}", (interval, interval))
            # A DhtKey's interval is its own (key_interval)
            changed = []
            for item_id, saved, at, retry_at in tx.execute(
                    "SELECT item, due, at, retry FROM publish_items WHERE item < x'01'").fetchall():  # a scan: rare
                try:
                    item = _from_id(item_id)
                    state = _Item(_optional_time(at), retry=_optional_time(retry_at))
                except (ValueError, TypeError):
                    continue  # written again when a round reads it
                if self._due_of(item, state) != saved:
                    changed.append((self._due_of(item, state), item.key))
            tx.executemany("UPDATE publish_items SET due = ? WHERE key = ?", changed)
        for item, state in self._items.items():
            if state is None:
                continue
            if clamp_to is not None:
                state.at = None if state.at is None else min(state.at, clamp_to)
                state.retry = None if state.retry is None else min(state.retry, clamp_to + self.retry_interval)
                state.earlier = {node: min(at, clamp_to) for node, at in state.earlier.items()}
            self._touch_item(item)
        self._due_interval = interval
        self._touch_meta("interval")

    def save(self):
        """Persist the publish state changed since the last save, if any. Called without the lock: it is written
        outside it, one save at a time (saves come from refresh(), the publish loop and stop()). Raises OSError
        (StateDbError) if the database fails; what it held is then written next time."""
        if self._store is not None:
            self._store.save()

    def close(self):
        """Release the state database (the node calls this once it has saved everything at shutdown)."""
        if self._db is not None:
            self._db.close()

    def _touch_item(self, item: ProvidedItem):
        """Note that item's state (in _items) changed, for the next save. Caller holds the lock."""
        if self._store is not None:
            self._store.changed((_ITEM, item))

    def _touch_node(self, node_id: bytes):
        """Note that the withdrawals queued for node_id changed, for the next save. Caller holds the lock."""
        if self._store is not None:
            self._store.changed((_NODE, node_id))

    def _touch_meta(self, name: str):
        """Note that the meta value name changed, for the next save. Caller holds the lock."""
        if self._store is not None:
            self._store.changed((_META, name))

    def _meta_value(self, name: str):
        """The meta value name as saved (META_STORE): "region_bits" (None while there is no estimate), "entries_on"
        (1 or 0), "interval" or "clock". Lock held."""
        if name == "region_bits":
            return self._region_bits_mean
        if name == "interval":
            return self._due_interval
        if name == "clock":
            return self._latest
        return int(self._entries_on)

    # Item states: _items over the state database

    def _due_of(self, item: ProvidedItem, state: _Item) -> float:
        """When item, in state, is next due: its retry if any, else its last publish plus its republish interval, or
        0 if never published."""
        if state.retry is not None:
            return state.retry
        if state.at is None:
            return 0.0
        return state.at + self.interval_for(item)

    def _read(self, items: Iterable[ProvidedItem]) -> dict[ProvidedItem, _Item]:
        """The states the state database holds of items (none for one without a row; a malformed row reads as never
        published, and is written again). Lock held."""
        wanted = {item.key: item for item in items}
        found: dict[ProvidedItem, _Item] = {}
        if not wanted:
            return found
        now = self.clock()
        keys = list(wanted)
        for start in range(0, len(keys), READ_BATCH):
            batch = keys[start:start + READ_BATCH]
            for key, *columns in self._db.query(
                    f"SELECT key, at, retry, nodes, earlier, heal FROM publish_items WHERE key IN "
                    f"({', '.join('?' * len(batch))})", batch):
                item = wanted[key]
                found[item] = self._parse(item, columns, now)
        return found

    def _parse(self, item: ProvidedItem, columns, now: float) -> _Item:
        """item's state from its row's columns (at, retry, nodes, earlier, heal); a malformed row reads as never
        published, kept in _items so the next save writes it again (logged). Lock held."""
        try:
            return _Item.from_row(*columns, now)
        except (ValueError, TypeError):
            log(f"publisher: ignoring a malformed row for {item} in the state database", LOG_WARNING)
            state = self._items[item] = _Item()
            self._touch_item(item)
            return state

    def _peek(self, item: ProvidedItem) -> _Item | None:
        """item's state, from memory or the state database (None if it has none), without keeping it. Lock held."""
        state = self._items.get(item, _MISSING)
        if state is _MISSING:
            return self._read([item]).get(item)
        return state

    def _get(self, item: ProvidedItem) -> _Item:
        """The state of item, a provided item, kept in _items (to change it: then _touch_item, or to hold it for the
        round in progress); one never published if it has none. Lock held."""
        state = self._items.get(item)
        if state is None:
            state = self._read([item]).get(item) if item not in self._items else None
            state = self._items[item] = state if state is not None else _Item()
        return state

    def states(self) -> dict[ProvidedItem, dict]:
        """Every provided item's state, read whole (for tests and diagnostics, not for a running node's use): item ->
        {"at": last published or None, "nodes": servers that recorded it then, "earlier": {server: when stored},
        "retry": when it is tried again sooner or None, "heal": marked for the heal republish, "due": when next due}."""
        with self._lock:
            now = self.clock()
            states: dict[ProvidedItem, _Item] = {}
            for item_id, *columns in self._db.query("SELECT item, at, retry, nodes, earlier, heal FROM publish_items"):
                try:
                    item = _from_id(item_id)
                    states[item] = _Item.from_row(*columns, now)
                except (ValueError, TypeError):
                    continue
            for item, state in self._items.items():
                if state is None:
                    states.pop(item, None)
                else:
                    states[item] = state
            for item in self._provided:
                states.setdefault(item, _Item())  # added, its row not written yet
            return {item: {"at": state.at, "nodes": list(state.nodes), "earlier": dict(state.earlier),
                           "retry": state.retry, "heal": state.heal, "due": self._due_of(item, state)}
                    for item, state in states.items() if item in self._provided}

    # The provide set

    def provided(self) -> frozenset[Cid]:
        """The CIDs this node currently provides, entries included (not the DhtKeys of add_key_source)."""
        with self._lock:
            return frozenset(item for item in self._provided if isinstance(item, Cid))

    def provided_entries(self) -> frozenset[Cid]:
        """The entries this node currently provides (provide_entries) that it doesn't provide as roots too."""
        with self._lock:
            return self._entries

    def entries_status(self) -> dict:
        """For the node's status: {"provided_entries": entry keys provided (not provided as roots too),
        "provide_entries_off": why none are, or None, "provide_entries_capped": pins whose entries were left
        out at provide_entries_max}."""
        with self._lock:
            capped = len(self._entries_from[4]) if self._entries_off is None else 0
            return {"provided_entries": len(self._entries), "provide_entries_off": self._entries_off,
                    "provide_entries_capped": capped}

    def provided_keys(self) -> frozenset[bytes]:
        """The non-CID keys this node currently provides."""
        with self._lock:
            return frozenset(item.key for item in self._provided if isinstance(item, DhtKey))

    def serves(self, cid: Cid) -> bool:
        """True if cid is in the DAG of a provided CID, so /blocks may serve it."""
        return cid in self._served  # a frozenset swapped whole, so reading it needs no lock

    def _keys(self) -> set[bytes]:
        """The keys the key sources return now (a failing source's are left out, logged). Lock held."""
        keys: set[bytes] = set()
        for source in list(self._key_sources):
            try:
                keys |= {bytes(key) for key in source()}  # as DhtKey takes them
            except Exception as e:  # noqa: BLE001 - a broken feature mustn't stop CIDs being provided
                log(f"a provided-key source failed: {e!r}", LOG_ERROR)
        return keys

    def _compute(self, keys: set[bytes], denied: frozenset[bytes]) -> tuple[frozenset[ProvidedItem], frozenset[Cid]]:
        """(provided CIDs, their entries (_choose_entries) and DhtKeys of keys, blocks they reach) from storage, the
        strategy and denied (the deny list's binary CIDs). Lock held.

        The served set and the DhtKeys are those of the last call when what they are built from hasn't changed
        (most refreshes): the same complete CIDs with the same dag_blocks objects, and the same keys. CIDs only
        added since add their blocks to it; only one gone or changed builds it again.
        """
        pinned = candidates = self.storage.pinned_cids()  # the same frozenset until pins change: not copied
        if self.config.provide_strategy == "roots":
            candidates = pinned | (self.storage.cached_targets() & set(self.cached_eligible()))
        dags: dict[Cid, frozenset[Cid]] = {}
        for cid in candidates:
            try:
                dags[cid] = self.storage.dag_blocks(cid)  # memoised; raises unless complete
            except (NotFound, InvalidDag):
                continue
        if denied:
            dags = {cid: blocks for cid, blocks in dags.items() if not self._denied(cid, blocks, denied)}
        self._denied_dags = {cid: checked for cid, checked in self._denied_dags.items() if cid in candidates}
        last_dags, served = self._served_from
        new = [blocks for cid, blocks in dags.items() if last_dags.get(cid) is not blocks]
        if len(dags) - len(new) < len(last_dags):  # one went, or its blocks changed: built again
            blocks_reached: set[Cid] = set()
            for blocks in dags.values():
                blocks_reached |= blocks
            served = frozenset(blocks_reached)
        elif new:  # only some added (a pin or a fetch)
            served = served.union(*new)
        self._served_from = (dags, served)
        if keys != self._keys_from[0]:
            self._keys_from = (keys, frozenset(DhtKey(key) for key in keys))
        roots = frozenset(dags)
        self._entries = self._choose_entries(dags, len(roots) + len(keys))
        # An entry is a block of a complete pin's DAG, so serving needs nothing more: the entry's DAG is inside it.
        # Entries come only from pins provided here, never on their own
        assert self._entries <= served, "an entry was chosen outside the served set"
        return roots | self._keys_from[1] | self._entries, served

    def _choose_entries(self, dags: dict[Cid, frozenset[Cid]], other_keys: int) -> frozenset[Cid]:
        """The entries to provide (provide_entries) that aren't provided as roots: those of the pins of the user's own
        content (own_pinned_cids()) among dags (the CIDs provided, with their blocks), up to provide_entries_max, or
        none (the setting is false, or a slow node's default finds that republishing them with the other_keys other
        keys would take too much of the channel). Notes why none are in _entries_off, logging when the estimate turns
        them off or on. Lock held."""
        setting = self.config.provide_entries
        reason = None
        chosen: frozenset[Cid] = frozenset()
        if setting is False:
            reason = "provide_entries is false"
        else:
            own = self.storage.own_pinned_cids()  # the same frozenset until pins change: not copied
            chosen = self._capped_entries(own, own & dags.keys()) - dags.keys()
            if setting is None and self.slow_node and chosen:
                keys = other_keys + len(chosen)
                servers = self._network_size()
                share = republish_airtime(keys, servers, self.bitrate) / self.interval
                if share > (ENTRY_AIRTIME_OFF if self._entries_on else ENTRY_AIRTIME_ON):
                    reason = (f"slow node: republishing {plural(keys, 'key')} with entries would take about "
                              f"{share:.1%} of the channel (at {self.bitrate or REFERENCE_BITRATE:g} bits/s, about "
                              f"{plural(servers, 'server')}); entries are provided by default only under "
                              f"{ENTRY_AIRTIME_ON:.0%} (provide_entries = true provides them anyway)")
                    chosen = frozenset()
                elif not self._entries_on and self._measured_busy():
                    # The estimate covers republishing alone; the radio measures all this node sends
                    reason = (f"slow node: its radio measured its own transmissions at over {ENTRY_AIRTIME_ON:.0%} of "
                              "the channel in the last hour; entries are turned on by default only under that "
                              "(provide_entries = true provides them anyway)")
                    chosen = frozenset()
        on = bool(chosen)
        if on != self._entries_on:
            self._entries_on = on
            self._touch_meta("entries_on")
        # The estimate's decision is logged when it changes (the setting's is the user's own: not logged)
        estimated_off = setting is None and reason is not None
        if estimated_off != self._estimated_off:
            log(f"provide_entries: {reason}" if estimated_off else "provide_entries: providing the files and folders "
                "inside pins again", LOG_NOTICE)
            self._estimated_off = estimated_off
        self._entries_off = reason
        return chosen

    def _capped_entries(self, pinned: frozenset[Cid], complete: frozenset[Cid]) -> frozenset[Cid]:
        """The entries of the pins in complete (pinned is own_pinned_cids()), up to provide_entries_max: pins oldest
        first (Storage.pin_order), each pin's entries all or none, so adding a pin never takes away an older pin's
        entries and a folder is findable whole or not at all; a pin that doesn't fit is left out (logged) and later
        ones still may. The same as last time while pins, complete and the cap are. Lock held."""
        cap = self.config.provide_entries_max
        last_pinned, last_complete, last_cap, chosen, skipped = self._entries_from
        if last_pinned is pinned and last_complete == complete and last_cap == cap:
            return chosen
        self._entries_of = {cid: entries for cid, entries in self._entries_of.items() if cid in complete}
        found: set[Cid] = set()
        left_out: list[Cid] = []
        walked = True
        for root in self.storage.pin_order(complete):
            entries = self._entries_of.get(root)
            if entries is None:
                entries = self._walk_entries(root)
                if entries is None:  # a block deleted meanwhile: computed again once the generation moves
                    walked = False
                    continue
                self._entries_of[root] = entries
            if len(found) + len(entries) > cap and len(found | entries) > cap:
                left_out.append(root)
                continue
            found |= entries
        chosen = frozenset(found)
        if left_out and tuple(left_out) != skipped:
            log(f"provide_entries: the files and folders inside {plural(len(left_out), 'pin')} (the newest) aren't "
                f"provided: they would take this node past {cap} entry keys (provide_entries_max)", LOG_NOTICE)
        self._entries_from = (pinned if walked else None, complete, cap, chosen, tuple(left_out))
        return chosen

    def _walk_entries(self, root: Cid) -> frozenset[Cid] | None:
        """The CIDs directory entries name anywhere in root's DAG, which is complete: every file and directory below
        it, not chunks, file nodes or index nodes; none for a file. None if a block went missing meanwhile.

        Reads the directory and index nodes only, each directory once however often it appears."""
        if root.is_raw:
            return frozenset()
        get = self.storage.getter()
        top = walker.root_expect(root)
        found: set[Cid] = set()
        try:
            if not isinstance(walker.decode_checked(top, get(top)), (dag.DirectoryNode, dag.IndexNode)):
                return frozenset()
            memo: dict = {}  # directory and index nodes decoded, for walker.directory_entries
            seen = {root}
            stack = [top]
            while stack:
                for entry, child in walker.directory_entries(stack.pop(), get, memo):
                    found.add(entry.cid)
                    if entry.kind == dag.KIND_DIRECTORY and entry.cid not in seen:
                        seen.add(entry.cid)
                        stack.append(child)
        except (NotFound, InvalidDag):
            return None
        return frozenset(found)

    def _network_size(self) -> int:
        """The network size a slow node's entries are judged by (network_size). Lock held."""
        try:
            known = int(self.known_servers())
        except Exception as e:  # noqa: BLE001 - an estimate without the routing table is still an estimate
            log(f"publisher: the routing table's size couldn't be read: {e!r}", LOG_ERROR)
            known = 0
        return network_size(self._region_bits_mean, known)

    def _entries_input(self) -> tuple:
        """What the entries a refresh chooses depend on besides pins (generation()): the settings, and for a slow
        node's default the network size and republish interval its estimate uses and whether the radio measured it
        busy. Lock held."""
        setting, cap = self.config.provide_entries, self.config.provide_entries_max
        if setting is None and self.slow_node:
            return setting, cap, self._network_size(), self.interval, self._measured_busy()
        return setting, cap

    def _measured_busy(self) -> bool:
        """True if the radio measured this node's own airtime over the last hour above ENTRY_AIRTIME_ON."""
        measured = self.measured_airtime()
        return measured is not None and measured > ENTRY_AIRTIME_ON

    def _denied(self, cid: Cid, blocks: frozenset[Cid], denied: frozenset[bytes]) -> bool:
        """True if one of blocks (cid's DAG) is in denied, the deny list's binary CIDs; memoised. Lock held."""
        checked = self._denied_dags.get(cid)
        if checked is None or checked[0] is not denied or checked[1] is not blocks:
            checked = (denied, blocks, any(block.binary in denied for block in blocks))
            self._denied_dags[cid] = checked
        return checked[2]

    def refresh(self):
        """Recompute what is provided (after pins, fetches, evictions); withdraw what no longer is, at once.

        Skipped when generation(), the deny list, the key sources' keys and the strategy are as at the last
        refresh that computed: the same inputs give the same sets, so nothing would be added or removed. The state
        database holds a row for exactly the items provided (once a save has written what a refresh changed): the
        first refresh looks through its rows for items no longer provided (removed while the node was stopped: a
        setting, the deny list), later ones only compare the provide sets. The first refresh after construction
        always computes (as does the first after add_key_source).
        """
        with self._lock:
            keys, denied = self._keys(), self.storage.deny.cids  # the deny set is swapped whole
            # Read before computing: a change made while this computes moves the generation past what is
            # remembered here, so the next refresh computes again (perhaps once more than needed), never misses it
            inputs = (None if self.generation is None
                      else (self.generation(), denied, keys, self.config.provide_strategy, self._entries_input()))
            last = self._inputs
            if (inputs is not None and last is not None and (last[1] is denied or last[1] == denied)
                    and last[0] == inputs[0] and last[2:] == inputs[2:]):
                return
            provided, served = self._compute(keys, denied)
            removed = set(self._provided - provided)
            if not self._synced:
                removed |= self._unprovided_rows(provided)
            added = provided - self._provided
            self._provided, self._served = provided, served
            now = self.clock()
            states = {item: self._items[item] for item in removed if item in self._items}
            states.update(self._read(item for item in removed if item not in self._items))
            for cid in removed:
                self.providers.withdraw(cid.key, self.own_id)
                state = states.get(cid)
                self._items[cid] = None  # its row (if any) goes at the next save
                self._touch_item(cid)
                if state is None:
                    continue
                earlier = {node for node, at in state.earlier.items() if now - at < PROVIDER_TTL}
                for node_id in set(state.nodes) | earlier:
                    self._withdrawals.setdefault(node_id, set()).add(cid.key)
                    self._touch_node(node_id)
            if added:
                self._added.append(added)  # each gets a row at the next save, unless it has one
                self._store.changed(_ADDED)
            # The keys withdrawals are queued for, gathered once: most added items have none to cancel
            pending = {key for keys in self._withdrawals.values() for key in keys} if added else set()
            for cid in added:
                self._provide_again(cid, pending)
            self._synced = True
            self._inputs = inputs  # only once everything above is done: an exception computes again next time
            if removed:
                log(f"no longer providing {plural(len(removed), 'CID')}", LOG_VERBOSE)
        if removed or added:
            self.save()
            self.wake()

    def _unprovided_rows(self, provided: frozenset[ProvidedItem]) -> set[ProvidedItem]:
        """The items with a row in the state database that aren't in provided, read SCAN_BATCH rows at a time; rows
        whose item doesn't parse are deleted. Lock held."""
        found: set[ProvidedItem] = set()
        malformed: list[tuple[bytes]] = []
        after = b""
        while True:
            rows = self._db.query("SELECT key, item FROM publish_items WHERE key > ? ORDER BY key LIMIT ?",
                                  (after, SCAN_BATCH))
            for key, item_id in rows:
                try:
                    item = _from_id(item_id)
                    if item.key != key:
                        raise ValueError("not its key")
                except ValueError:
                    malformed.append((key,))
                    continue
                if item not in provided:
                    found.add(item)
            if len(rows) < SCAN_BATCH:
                break
            after = rows[-1][0]
        if malformed:
            log(f"publisher: ignoring {plural(len(malformed), 'malformed row')} in the state database", LOG_WARNING)
            statedb.delete_rows(self._db, "publish_items", ("key",), malformed)
        return found

    def _provide_again(self, cid: ProvidedItem, pending: set[bytes]) -> bool:
        """cid is provided (again): record ourselves locally and cancel withdrawals of its key still queued;
        True if any were. pending holds every key a withdrawal is queued for (it may hold more: those already
        cancelled), so the queues are searched only for those. Lock held.

        A withdrawal queued when it stopped being provided would otherwise reach a node after a round stored it
        there again, deleting the fresh record (section 6). Those nodes may still hold a record of it, so they
        join its earlier nodes, to be withdrawn from should it stop being provided again.
        """
        key = cid.key
        self.providers.store(key, self.own_id)  # refresh() withdrew it when it stopped being provided
        if key not in pending:
            return False
        now = self.clock()
        cancelled = False
        for node_id in [node_id for node_id, keys in self._withdrawals.items() if key in keys]:
            keys = self._withdrawals[node_id]
            keys.discard(key)
            if not keys:
                del self._withdrawals[node_id]
            earlier = self._get(cid).earlier
            earlier[node_id] = max(now, earlier.get(node_id, now))
            self._touch_node(node_id)
            self._touch_item(cid)
            cancelled = True
        return cancelled

    # Publishing

    def wake(self):
        """Make the publish loop look for due work now."""
        self._wake.set()

    def _is_due(self, cid: ProvidedItem, now: float) -> bool:
        """True if cid should be (re)published now."""
        with self._lock:
            state = self._peek(cid)
            return state is None or self._due_of(cid, state) <= now

    def pace_lookups(self, count: int) -> float:
        """Slow nodes: may count lookups outside publishing (a search's fallback over disjoint paths) start now? 0.0 if
        the round pacing allows it, and the next round then waits for them as for its own lookups (SLOW_LOOKUP_PACE
        each); otherwise the seconds until it would, and nothing is counted."""
        now = self.clock()
        with self._lock:
            self._next_lookup = min(self._next_lookup, now + self._pace_cap)  # as _clamp
            wait = self._next_lookup - now
            if wait > 0:
                return wait
            self._next_lookup = now + SLOW_LOOKUP_PACE * max(0, count)
            return 0.0

    def next_due(self) -> float:
        """Seconds until something is due (0 if now); withdrawals pending count as due (paced on slow nodes). Asks the
        state database's index for the earliest due time, over what memory holds changed."""
        now = self.clock()
        with self._lock:
            self._clamp(now)
            self._check_interval()
            waits = []
            if any(self._withdrawals.values()):
                if not self.slow_node:
                    return 0.0
                waits.append(0.0)
            if self._added:
                waits.append(0.0)  # items added whose rows aren't written yet: perhaps never published
            held = {item.key for item in self._items}
            if self._synced:
                # The earliest row memory doesn't hold changed (rows are of provided items only)
                for key, due in self._db.query("SELECT key, due FROM publish_items ORDER BY due LIMIT ?",
                                               (len(held) + 1,)):
                    if key not in held:
                        waits.append(due - now)
                        break
            for item, state in self._items.items():
                if state is not None and item in self._provided:
                    waits.append(self._due_of(item, state) - now)
            wait = min(waits, default=MAX_WAIT)
            if self.slow_node and waits:
                wait = max(wait, self._next_lookup - now)  # lookups and withdrawals paced
        return max(0.0, wait)

    def _take_due(self, now: float) -> list[ProvidedItem]:
        """The items due now, never published first, then the longest unpublished, at most ROUND_KEYS, their states
        held in _items for the round (_round). Read from the state database through its due index, over what memory
        holds changed. Lock held."""
        found: dict[ProvidedItem, _Item] = {}
        if not self._synced:
            return []  # nothing is provided before the first refresh
        # In the due index's order (no sort): items published in one round share a due time, and the index holds the
        # table's key after it, so they come in key order, and never published ones too
        rows = self._db.query("SELECT item, at, retry, nodes, earlier, heal FROM publish_items WHERE due <= ?"
                              " ORDER BY due LIMIT ?", (now, ROUND_KEYS))
        for item_id, *columns in rows:
            try:
                item = _from_id(item_id)
            except ValueError:
                continue  # deleted by the next start's first refresh
            if item in self._items:
                continue  # memory's state wins
            if item not in self._provided:
                # A row left of an item no longer provided (a save that failed): it goes at the next save
                self._items[item] = None
                self._touch_item(item)
                continue
            found[item] = self._parse(item, columns, now)
        for item, state in self._items.items():
            if state is not None and item in self._provided and self._due_of(item, state) <= now:
                found[item] = state
        due = sorted(found, key=lambda item: (found[item].at if found[item].at is not None else -math.inf,
                                              item.key))[:ROUND_KEYS]
        for item in due:
            self._items.setdefault(item, found[item])
        self._round = set(due)
        self._round_order, self._round_index = due, {item: n for n, item in enumerate(due)}
        return due

    def _slack(self, now: float, skip) -> float:
        """How long republishing may slip before a published item not in skip (this round's) would go unrepublished
        past PROVIDER_TTL / 2: PROVIDER_TTL / 2 - max(its age, its interval) at its smallest (PROVIDER_TTL / 2 with
        none). Lock held.

        The smallest is the oldest publish's or the longest interval's, so it reads the oldest rows (as many as skip
        and memory hold, plus one) and the DhtKeys' (a feature's few keys; CIDs share one interval), never every
        item's: a slow node's own, which provides few keys."""
        held = {item.key for item in self._items}
        candidates: dict[ProvidedItem, float] = {}
        rows = self._db.query("SELECT item, at FROM publish_items WHERE at IS NOT NULL ORDER BY at LIMIT ?",
                              (len(held) + len(skip) + 1,))
        rows += self._db.query("SELECT item, at FROM publish_items WHERE at IS NOT NULL AND item < x'01'")
        for item_id, at in rows:
            item = _maybe_item(item_id)
            if item is not None and item.key not in held and item not in skip and _is_number(at):
                candidates[item] = min(float(at), candidates.get(item, at))
        for item, state in self._items.items():
            if state is not None and state.at is not None and item not in skip and item in self._provided:
                candidates[item] = state.at
        # Is any CID published (and not skipped)? Among one more published CID than skip and memory hold, one is
        cids = any(not isinstance(item, DhtKey) for item in candidates) or any(
            item is not None and item.key not in held and item not in skip for item in (
                _maybe_item(item_id) for (item_id,) in self._db.query(
                "SELECT item FROM publish_items WHERE at IS NOT NULL AND item >= x'01' LIMIT ?",
                (len(held) + len(skip) + 1,))))
        oldest = min(candidates.values(), default=None)
        longest = max([self.interval_for(item) for item in candidates] + ([self.interval] if cids else []),
                      default=None)
        if oldest is None and longest is None:
            return PROVIDER_TTL / 2
        return PROVIDER_TTL / 2 - max(now - oldest if oldest is not None else 0.0, longest or 0.0)

    def _end_round(self, finished: bool):
        """The round ended (finished: it reached its end; else it raised): memory lets go of the states it held that
        haven't changed. A round that raised has the items servers recorded written whole at the next save, so where
        they were stored isn't only in publish_stores. Lock held."""
        if not finished:
            for item in self._round_stored:
                if self._items.get(item) is not None:
                    self._touch_item(item)
        if self._round_stored:
            # Every store is in its item's row now (or a withdrawal): publish_stores is emptied in the same save
            self._stores = []
            self._store.changed(_ROUND_END)
        for item in self._round:
            if not self._store.is_dirty((_ITEM, item)):
                self._items.pop(item, None)
        self._round, self._round_stored, self._round_order, self._round_index = set(), set(), [], {}
        self._round_listed = False
        self._round_number += 1

    def publish_due(self, should_stop: Callable[[], bool] = lambda: False) -> dict:
        """One publishing round: provide every due CID (the ROUND_KEYS most overdue) and send pending withdrawals,
        batched per node.

        Returns {"published": CIDs published, "nodes": nodes contacted, "withdrawn": keys withdrawn, "lookups":
        region lookups run, "finds": extra /find their regions sent, "widened": widened lookups run for keys the
        density check flagged}. One round at a time.
        """
        if self._added:
            self.save()  # the rows of items a refresh added, so they are read with the rest
        now = self.clock()
        with self._lock:
            self._clamp(now)
            self._check_interval()
            paced = self.slow_node and now < self._next_lookup
            due = [] if paced else self._take_due(now)
        finished = False
        try:
            result = self._publish(due, paced, now, should_stop)
            finished = True
            return result
        finally:
            with self._lock:
                self._end_round(finished)

    def _publish(self, due: list[ProvidedItem], paced: bool, now: float, should_stop: Callable[[], bool]) -> dict:
        """publish_due's round over due (its items, in order; none if paced), started at now."""
        islands = self._islands
        partitioned = islands is not None and islands.active  # at the round's start (or its end, below)
        with self._lock:
            limit = None
            if self.slow_node:
                # Servers only withdrawals reach cost a lookup's worth each: while any are queued, up to half the
                # round is kept for them (those also stored to this round take them along for free)
                queued = sum(1 for keys in self._withdrawals.values() if keys)
                limit = SLOW_ROUND_LOOKUPS - min(queued, SLOW_ROUND_LOOKUPS // 2)

        sweep = self._sweep(due, should_stop, limit)
        lookups, finds = len(sweep.regions), sum(region.finds for region in sweep.regions)
        if sweep.regions:
            with self._lock:
                self._learn_network_size(sweep.regions)
        # In due's order, so a round's batches are the same however the lookups were run. A key whose widened lookup
        # the round couldn't afford waits for a later round (it stays due)
        regions = {item: sweep.covered[item.key] for item in due
                   if item.key in sweep.covered and item.key not in sweep.covered[item.key].deferred}
        for cid in regions:
            self.providers.store(cid.key, self.own_id)  # providers also record themselves locally
        # Built without the lock: a part may take other locks (the first-seen log's)
        closest = {cid: self._store_set(cid, region) for cid, region in regions.items()}
        with self._lock:
            # Taken after the lookups, which can be long, so withdrawals queued meanwhile go in this round. In
            # partition mode those for servers off the island wait (they can't be reached)
            withdrawals = {node: set(keys) for node, keys in self._withdrawals.items()
                           if keys and (islands is None or islands.allows(node))}
            for cid in [cid for cid in closest if cid not in self._provided]:
                # Stopped being provided during its lookup: refresh() has withdrawn it, so don't store it
                del closest[cid]
                self.providers.withdraw(cid.key, self.own_id)

            by_node: dict[bytes, list[ProvidedItem]] = {}
            for cid, nodes in closest.items():
                for node_id in nodes:
                    by_node.setdefault(node_id, []).append(cid)
            for node_id, cids in by_node.items():
                # Withdrawing a key from a node while storing it there would delete the fresh record: the store
                # wins, and the withdrawal is dropped (refresh() cancels those of a key provided again, so this
                # is only for state from elsewhere)
                superseded = withdrawals.get(node_id, set()) & {cid.key for cid in cids}
                if superseded:
                    withdrawals[node_id] -= superseded
                    self._withdrawals[node_id] -= superseded
                    if not self._withdrawals[node_id]:
                        del self._withdrawals[node_id]
                    self._touch_node(node_id)
            if self.slow_node:
                # Withdrawals ride along to the servers stored to; each server only they reach takes one of the
                # round's lookups left, those with the most keys queued first, the rest waiting for a later round
                alone = sorted((node_id for node_id, keys in withdrawals.items() if keys and node_id not in by_node),
                               key=lambda node_id: (-len(withdrawals[node_id]), node_id))
                room = 0 if paced else max(0, SLOW_ROUND_LOOKUPS - lookups - sweep.widened)
                for node_id in alone[room:]:
                    del withdrawals[node_id]
                spent = lookups + sweep.widened + min(room, len(alone))
                if spent:
                    # Stretched while the channel is busy (self.stretch), never past PROVIDER_TTL / 2 for a record this
                    # round doesn't republish
                    wait = self.stretch(SLOW_LOOKUP_PACE * spent, self._slack(now, closest))
                    self._pace_cap = max(SLOW_LOOKUP_PACE * SLOW_ROUND_LOOKUPS, wait)
                    self._next_lookup = self.clock() + wait
        accepted: dict[ProvidedItem, list[bytes]] = {cid: [] for cid in closest}
        sent_withdrawals: dict[bytes, set[bytes]] = {}
        unsaved, saved_at = 0, self.clock()

        def send(node_id: bytes) -> tuple | None:
            """Send node_id its keys and the withdrawals still queued for it, over one identified link: (keys,
            withdrawn, keys it may have recorded, when it answered), or None if there was nothing to send."""
            cids = by_node.get(node_id, [])
            with self._lock:
                # Only what is still queued: a key provided again since the snapshot had its withdrawals cancelled
                withdrawn = withdrawals.get(node_id, set()) & self._withdrawals.get(node_id, set())
            if not cids and not withdrawn:
                return None
            if islands is not None and not islands.allows(node_id):
                # Partition mode began during the round: nothing is sent to a server off the island (as if refused,
                # withdrawals kept queued), which would only cost a link timeout
                return cids, set(), set(), self.clock()
            # Not retried until the next republish whatever the outcome, so it counts as sent either way.
            # The node may hold records of the keys it got before any refusal: those are withdrawn later
            recorded = self.client.provide(node_id, [cid.key for cid in cids], sorted(withdrawn))
            if islands is not None and cids:
                # No key recorded: no identified link could be opened (a refusal still names the batch's keys)
                (islands.answered if recorded else islands.failed)(node_id)
            return cids, withdrawn, recorded, self.clock()

        parallel = 1 if self.slow_node else PROVIDE_PARALLEL
        for node_id, outcome in _in_order(sorted(set(by_node) | set(withdrawals)), send, should_stop, parallel):
            if outcome is None:
                continue
            cids, withdrawn, recorded, answered = outcome
            sent_withdrawals[node_id] = withdrawn
            with self._lock:
                kept = []
                for cid in cids:
                    if cid.key not in recorded:
                        continue
                    accepted[cid].append(node_id)
                    # Remembered at once, and saved every few nodes (publish_stores), so a round cut short by a crash
                    # still leaves a record of where it stored, to withdraw from (the round's end writes each item's
                    # row)
                    if cid in self._provided:
                        earlier = self._get(cid).earlier
                        earlier[node_id] = max(answered, earlier.get(node_id, answered))
                        kept.append(cid)
                    else:
                        self._withdrawals.setdefault(node_id, set()).add(cid.key)
                        self._touch_node(node_id)
                if kept:
                    self._stores.append((node_id, answered, kept))
                    self._round_stored.update(kept)
                    self._store.changed(_STORES)
            missed = sum(1 for cid in cids if cid.key not in recorded)
            if missed:
                log(f"{node_name(node_id)} didn't record {plural(missed, 'key')}", LOG_VERBOSE)
            unsaved += 1
            if unsaved >= SAVE_EVERY_NODES or answered - saved_at >= SAVE_EVERY_SECONDS:
                self.save()
                unsaved, saved_at = 0, answered

        unfinished: list[ProvidedItem] = []
        with self._lock:
            for node_id, keys in sent_withdrawals.items():
                remaining = self._withdrawals.get(node_id, set()) - keys
                if remaining:
                    self._withdrawals[node_id] = remaining
                else:
                    self._withdrawals.pop(node_id, None)
                if keys:
                    self._touch_node(node_id)
            for cid, nodes in closest.items():
                if cid not in self._provided:
                    # Stopped being provided while this round ran: withdraw what was just stored
                    for node_id in accepted[cid]:
                        self._withdrawals.setdefault(node_id, set()).add(cid.key)
                        self._touch_node(node_id)
                    continue
                state = self._get(cid)
                self._touch_item(cid)
                if not nodes:
                    state.retry = self.clock() + self.retry_interval
                    continue
                state.retry = None
                if islands is not None:
                    marked = self._mark_heal(state, nodes, accepted[cid], partitioned or islands.active)
                    if marked and partitioned and not islands.active:
                        # Partition mode ended while this round ran, after heal() made the marks due: due again
                        state.retry = now
                # Nodes this round didn't store it on may still hold an earlier record: withdrawn from too
                earlier = {n: t for n, t in state.earlier.items() if now - t < PROVIDER_TTL}
                if state.at is not None:
                    for node_id in state.nodes:
                        earlier[node_id] = max(state.at, earlier.get(node_id, state.at))
                if all(node_id in sent_withdrawals for node_id in nodes):
                    state.at, state.nodes = now, accepted[cid]
                    for node_id in accepted[cid]:
                        earlier.pop(node_id, None)
                else:
                    # Stopped before every node was sent its keys: left due (unpublished), keeping where it went
                    state.at, state.nodes = None, []
                    for node_id in accepted[cid]:
                        earlier[node_id] = max(now, earlier.get(node_id, now))
                    unfinished.append(cid)
                state.earlier = earlier
            self._end_round(True)
        published = [cid for cid in closest if cid not in unfinished]
        if closest or sent_withdrawals:
            self.save()
            stored = sum(len(nodes) for nodes in accepted.values())
            log(f"published {plural(len(published), 'CID')} ({stored} records on "
                f"{plural(len(by_node), 'node')}; {plural(lookups, 'region lookup')}, {finds} extra /find"
                + (f", {plural(sweep.widened, 'widened lookup')}" if sweep.widened else "") + "), "
                f"withdrew {sum(len(k) for k in sent_withdrawals.values())}"
                + (f"; stopped before sending {plural(len(unfinished), 'CID')} to every node" if unfinished else ""),
                LOG_VERBOSE)
        return {"published": published, "nodes": sorted(set(by_node) | set(sent_withdrawals)),
                "withdrawn": sum(len(keys) for keys in sent_withdrawals.values()), "lookups": lookups, "finds": finds,
                "widened": sweep.widened}

    def _mark_heal(self, state: _Item, nodes: list[bytes], accepted: list[bytes], partitioned: bool) -> bool:
        """Mark an item (its state) for the heal republish if its round (store set nodes, accepted those that may
        hold it) reached no off-island server, in partition mode or after trying one; clear the mark if it reached
        one. True if it is marked now. Lock held."""
        islands = self._islands
        if any(not islands.is_local(node_id) for node_id in accepted):
            state.heal = False
            return False
        if partitioned or any(not islands.is_local(node_id) for node_id in nodes):
            state.heal = True
        return state.heal

    def heal(self) -> int:
        """Partition mode ended: every item marked (its last round reached no off-island server) is due now, so the
        rounds that follow (paced on slow nodes as ever) republish it beyond the island. Returns how many."""
        now = self.clock()
        with self._lock:
            marked = self._marked()
            if marked:
                with self._db.transaction() as tx:  # a direct write, as _recompute's
                    tx.execute("UPDATE publish_items SET retry = ?, due = ? WHERE heal != 0", (now, now))
                for item, state in self._items.items():
                    if state is not None and state.heal and item in self._provided:
                        state.retry = now
                    if state is not None:
                        self._touch_item(item)
        if marked:
            log(f"partition mode ended: republishing {plural(marked, 'key')} stored only on the island", LOG_NOTICE)
            self.wake()
        return marked

    def heal_marked(self) -> int:
        """How many provided items are marked for the heal republish."""
        with self._lock:
            return self._marked()

    def _marked(self) -> int:
        """How many provided items are marked for the heal republish: the marked rows (an index of their own), with
        memory's states over theirs. Lock held."""
        if not self._synced:
            return 0  # nothing is provided before the first refresh
        count = self._db.query("SELECT COUNT(*) FROM publish_items WHERE heal != 0")[0][0]
        held = [item.key for item in self._items]
        for start in range(0, len(held), READ_BATCH):
            batch = held[start:start + READ_BATCH]
            count -= self._db.query(f"SELECT COUNT(*) FROM publish_items WHERE heal != 0 AND key IN "
                                    f"({', '.join('?' * len(batch))})", batch)[0][0]
        return count + sum(1 for item, state in self._items.items()
                           if state is not None and state.heal and item in self._provided)

    def _learn_network_size(self, regions: list[Region]):
        """Fold the prefix lengths of regions a round found (those that found a server) into the running mean the
        network size is estimated from (network_size), saved with the publish state. Lock held."""
        for region in regions:
            if not region.closest or region.single:
                continue  # a single region's prefix isn't a region's size (sweep.find_region)
            mean = self._region_bits_mean
            self._region_bits_mean = (float(region.bits) if mean is None
                                      else mean + (region.bits - mean) / REGION_SMOOTHING)
            self._touch_meta("region_bits")

    def add_store_part(self, part: StorePart):
        """Also store each due key on the servers part(key, region) gives (the announce first-seen log's senior
        servers, say), besides its K closest from its region. Called without the lock once the round's lookups are
        done; it must not do network I/O. A key whose region found no server is stored nowhere (retried soon)."""
        with self._lock:
            self._store_parts.append(part)

    def set_islands(self, islands):
        """Publish under islands, the node's partition mode (rnfs.dht.local.PartitionMode): only to the servers its
        allows() takes, telling it each server's answer (answered) or failure (failed) to a /provide."""
        with self._lock:
            self._islands = islands

    def set_widening(self, flags: Flags, widen: Widen):
        """Check each key of a region found for a dense core: flags(key, region) says whether it needs a widened lookup
        (the density check, rnfs.dht.density), which widen(key, region) runs, returning the ring servers to store the
        key on too. Called by the lookup threads once a region is found, without the lock."""
        with self._lock:
            self._widening = (flags, widen)

    def set_network_size(self, estimate: Callable[[bytes], float | None]):
        """Size regions by the network size estimate too: estimate(key) is it (rnfs.dht.density.SizeEstimate, leaving
        out key's own sample), None while there is none (sweep.find_region)."""
        with self._lock:
            self._sizes = estimate

    def _widen(self, sweep: Sweep, region: Region, keys: list[bytes], limit: int | None):
        """Run the widened lookups of region's keys (those its lookup covered) that the density check flags, within
        the round's limit of lookups; those over it are deferred. Called without the lock."""
        widening = self._widening
        if widening is None:
            return
        flags, widen = widening
        for key in keys:
            if not flags(key, region):
                continue
            if not sweep.spend(limit):
                region.deferred.add(key)
                continue
            region.rings[key] = list(widen(key, region))

    def _store_set(self, cid: ProvidedItem, region: Region) -> list[bytes]:
        """The servers cid is stored on this round: its K closest from region, each disjoint path's K closest, the
        ring servers of its widened lookup, then each store part's servers, without duplicates or this node; none if
        the region found no server. Called without the lock."""
        nodes = [n for n in region.closest_to(cid.key) if n != self.own_id]
        if not nodes:
            return []
        chosen = set(nodes)
        for node_id in region.path_closest(cid.key) + region.rings.get(cid.key, []):
            if node_id != self.own_id and node_id not in chosen:
                chosen.add(node_id)
                nodes.append(node_id)
        for part in list(self._store_parts):
            try:
                extra = list(part(cid.key, region))
            except Exception as e:  # noqa: BLE001 - a broken part mustn't stop the K closest being stored to
                log(f"a store-set part failed for {cid}: {e!r}", LOG_ERROR)
                continue
            for node_id in extra:
                if node_id != self.own_id and node_id not in chosen:
                    chosen.add(node_id)
                    nodes.append(node_id)
        islands = self._islands
        if islands is not None and islands.active:
            # Partition mode: the store parts may name off-island servers (senior ones), which can't be reached
            nodes = [node_id for node_id in nodes if islands.allows(node_id)]
        return nodes

    def _sweep(self, due: list[ProvidedItem], should_stop: Callable[[], bool], limit: int | None) -> Sweep:
        """The regions of due's keys whose lookups ran (dht.sweep), taking keys in due's order: at most limit
        lookups, widened ones included (None: no limit). Called without the lock: refresh() may run meanwhile.

        should_stop() is checked before each lookup starts; one running isn't interrupted. Slow nodes (and a
        single item) look up one at a time; other nodes run up to lookups_at_once at once on
        rnfs-publish-lookup threads. A lookup that raises starts no more; the exception is raised once those
        running have finished, ending the round as it would one at a time.
        """
        sweep = Sweep([item.key for item in due], self._region_bits)

        def look_up(key: bytes):
            """Run the lookup for key, cover its region and widen the lookups of the keys the density check flags."""
            sizes = self._sizes
            region = find_region(self.make_lookup(key), key, self.own_id, lambda bits: sweep.members(key, bits),
                                 None if sizes is None else lambda: sizes(key))
            self._widen(sweep, region, sweep.cover(region), limit)

        if self.slow_node or len(due) < 2 or self.lookups_at_once < 2:
            while (limit is None or sweep.lookups < limit) and not should_stop():
                key = sweep.take()
                if key is None:
                    break
                look_up(key)
        else:
            self._sweep_at_once(sweep, look_up, should_stop, self.lookups_at_once)
        if sweep.regions:
            self._region_bits = sweep.estimate
        return sweep

    @staticmethod
    def _sweep_at_once(sweep: Sweep, look_up: Callable[[bytes], None], should_stop: Callable[[], bool], count: int):
        """Run sweep's lookups count at once (_sweep), raising the first exception one raised."""
        ended: list[BaseException | None] = []  # None once stopped, else the exception a lookup raised

        def work():
            """Run lookups in turn until none is left, the round is stopped, or one has raised."""
            while True:
                if ended or should_stop():
                    ended.append(None)
                    sweep.stop()
                    return
                key = sweep.take()  # waits while every key left is claimed by a lookup running
                if key is None:
                    return
                if ended or should_stop():
                    sweep.release(key)
                    continue
                try:
                    look_up(key)
                except BaseException as e:  # noqa: BLE001 - raised by publish_due once the others finish
                    ended.append(e)
                    sweep.release(key)
                    sweep.stop()
                    return

        threads = [threading.Thread(target=work, name="rnfs-publish-lookup", daemon=True)
                   for _ in range(count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        failure = next((e for e in ended if e is not None), None)
        if failure is not None:
            raise failure

    def run(self, stop_event: threading.Event, ready: threading.Event | None = None):
        """The publish loop, for a thread of its own: waits for ready (the join), then publishes what's due."""
        if ready is not None:
            while not ready.wait(1.0):
                if stop_event.is_set():
                    return
        while not stop_event.is_set():
            self._wake.clear()
            try:
                if self.slow_node and self.config.provide_entries is None:
                    # Whether a slow node provides entries follows the network's size, which no pin or fetch
                    # changes: looked at again at each wake (a refresh with nothing changed returns at once)
                    self.refresh()
                if self.next_due() <= 0:
                    self.publish_due(stop_event.is_set)
            except Exception as e:  # noqa: BLE001 - the publisher thread keeps running; logged
                log(f"publishing failed: {e!r}", LOG_ERROR)
            self._wake.wait(min(MAX_WAIT, max(0.5, self.next_due())))
            if stop_event.is_set():
                break
