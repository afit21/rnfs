"""Keeping what this node provides advertised in the DHT (specification section 6).

- **What is provided** (provide_strategy): `pinned` provides every complete pin; `roots` also every complete
  fetched target while it is cached (the node says which cached targets qualify: gateway fetches never do).
  Only these CIDs' DAGs are served to /blocks (section 7). A CID whose DAG holds a block the deny list (the
  storage's) denies is never provided: a change to the list takes effect at the next refresh(). A refresh
  recomputes this only when something it is computed from changed (the node's generation(), the deny list,
  the key sources' keys, the strategy, provide_entries and, on slow nodes, the network size it is judged by), so
  the many refreshes that change nothing cost almost nothing.
- **Entries** (provide_entries): also every CID a directory entry names inside a complete pin (its files and
  directories at every depth, not chunks, file nodes or index nodes), so each can be found by its own CID. Never
  those of cached downloads. Unset, it is on up to MAX_ENTRY_KEYS entry keys, pins taken oldest first and each
  pin's entries all or none; on a slow node only while republishing every key is estimated to take at most
  ENTRY_AIRTIME_ON of the channel (republish_airtime), and otherwise none at all. An entry's DAG is inside its
  pin's, so the served set doesn't change.
- **Publishing.** A CID is due when it has never been published or its republish_interval has passed. The due
  CIDs' K closest nodes are found by sweep providing (rnfs.dht.sweep): one lookup per region of due keys,
  PUBLISH_LOOKUPS at once (one at a time on slow nodes). Each key is stored on its store set: its K closest, and
  the servers of the parts add_store_part added. Keys are grouped by node, and each node gets its keys (and any
  withdrawals for it) in as few /provide requests as fit, over one identified link. A refused or failed store
  isn't retried until the next republish. The node also records itself locally.
- **Withdrawing.** When a CID stops being provided (unpinned, evicted, gc'd, damaged), its key is withdrawn
  from the nodes it was last published to, at once (paced on slow nodes), and from nodes of earlier rounds (or
  of a round cut short) whose record may not have expired yet: the closest set changes as nodes come and go.

Last-published nodes and times, earlier rounds' nodes and pending withdrawals are persisted in the state
database (rnfs.statedb: the published, published_earlier and withdrawals tables), each save writing only the
items and nodes changed since the last, so a restart doesn't republish what isn't due. A CID whose lookup found
no node at all (nothing was sent) is retried after RETRY_INTERVAL rather than a whole republish interval; one
whose round was stopped before every node was sent its keys stays due. Times are wall clock: any in the future
(the clock was corrected) count as now.

- **Feature keys** (DhtKey, e.g. kept names) are republished at max(republish interval, KEY_MIN_INTERVAL),
  with a fixed jitter of up to +-KEY_JITTER from the key's first byte, so they don't all come due in one burst
  but a region's keys come due together; always below PROVIDER_TTL / 2.
- **Slow nodes** pace lookups: at most SLOW_ROUND_LOOKUPS region lookups per round and on average one per
  SLOW_LOOKUP_PACE seconds; never-published items go first, then the longest unpublished. Withdrawals are paced
  too: they ride along to the servers a round stores to, and each server only withdrawals reach costs one lookup.
  Other nodes send every withdrawal at once.
- A key both withdrawn from and provided to a node in one round is only provided (a restart may queue
  withdrawals of keys that are still provided).
"""
import hashlib
import math
import threading
import time
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
# ones: past this many, the entries of the newest pins are left out (a 70k-file folder fits, at ~11.6 MB a day)
MAX_ENTRY_KEYS = 100_000
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
META_STORE = "publisher"  # its meta values: "region_bits" (that mean), "entries_on" (the last decision)


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
    replace the published, published_earlier and withdrawals tables. Each entry is checked as rnfs 0.1.4 loaded
    it, one of the wrong shape being skipped, and node IDs, keys and DhtKeys must be ID_LENGTH bytes; times are
    kept as written (the load clamps them and drops earlier nodes past PROVIDER_TTL). Returns the number of rows
    imported."""
    for table in ("published", "published_earlier", "withdrawals"):
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
    return sum(len(table) for table in rows)


_ITEM, _NODE, _META = "item", "node", "meta"  # the kinds of key _PublishState writes


class _PublishState(statedb.RowStore):
    """The publisher's three tables and its meta values, saved together (not durably: NORMAL) in one transaction. A
    key is (_ITEM, item), whose published row and published_earlier rows are written again, (_NODE, node ID), whose
    withdrawals rows are, or (_META, name), the meta value name (META_STORE)."""

    def __init__(self, db: statedb.StateDb, publisher: "Publisher"):
        """The saves of publisher's state, whose lock guards it."""
        super().__init__(db, "published", ("item",), durable=False, mode=statedb.SNAPSHOT, lock=publisher._lock)
        self.publisher = publisher

    def snapshot(self, keys) -> tuple:
        """The rows of keys (lock held): (item texts, published rows, earlier rows, node IDs, withdrawal rows, meta
        values)."""
        publisher = self.publisher
        items = [key for kind, key in keys if kind == _ITEM]
        nodes = [key for kind, key in keys if kind == _NODE]
        texts = {item: str(item) for item in items}
        published = [(texts[item], publisher._published[item][0], b"".join(publisher._published[item][1]))
                     for item in items if item in publisher._published]
        earlier = [(texts[item], node, at) for item in items for node, at in publisher._earlier.get(item, {}).items()]
        withdrawals = [(node, key) for node in nodes for key in publisher._withdrawals.get(node, ())]
        meta = [(name, publisher._meta_value(name)) for kind, name in keys if kind == _META]
        return list(texts.values()), published, earlier, nodes, withdrawals, meta

    def write_rows(self, tx, snapshot: tuple):
        """Replace the rows of the snapshot's items and nodes, and its meta values, inside transaction tx."""
        texts, published, earlier, nodes, withdrawals, meta = snapshot
        for name, value in meta:
            if value is None:
                tx.execute("DELETE FROM meta WHERE store = ? AND key = ?", (META_STORE, name))
            else:
                statedb.set_meta(tx, META_STORE, name, value)
        tx.executemany("DELETE FROM published WHERE item = ?", [(text,) for text in texts])
        tx.executemany("DELETE FROM published_earlier WHERE item = ?", [(text,) for text in texts])
        tx.executemany("INSERT OR REPLACE INTO published (item, at, nodes) VALUES (?, ?, ?)", published)
        tx.executemany("INSERT OR REPLACE INTO published_earlier (item, node, at) VALUES (?, ?, ?)", earlier)
        tx.executemany("DELETE FROM withdrawals WHERE node = ?", [(node,) for node in nodes])
        tx.executemany("INSERT OR REPLACE INTO withdrawals (node, key) VALUES (?, ?)", withdrawals)


class Publisher:
    """The provide set, the served set, and the publish/withdraw schedule. Thread-safe."""

    def __init__(self, own_id: bytes, config: Config, storage: Storage, providers, client,
                 make_lookup: Callable[[bytes], object], slow_node: bool = False, state_dir: str | None = None,
                 cached_eligible: Callable[[], set[Cid]] = set, clock: Callable[[], float] = time.time,
                 retry_interval: float = RETRY_INTERVAL, generation: Callable[[], object] | None = None,
                 bitrate: float | None = None, known_servers: Callable[[], int] = lambda: 0):
        """Publisher for own_id. providers is the ProviderStore (to record ourselves), client the PeerClient.

        make_lookup(key) returns a dht.lookup.Lookup; cached_eligible() returns the cached targets the
        roots strategy may provide. generation(), if given, returns a value that changes whenever storage's pins,
        cached targets or blocks, or cached_eligible(), may have: refresh() skips its recompute while it, the deny
        list and the key sources' keys are as at the last one (without it, every refresh recomputes). bitrate (the
        slowest interface's, bits/s; None if unknown) and known_servers() (the routing table's size, a lower bound
        of the network's) decide whether a slow node provides entries by default (republish_airtime).
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
        # choice of entries was made from and gave: (pinned_cids() then, the complete pins, entries chosen, pins left
        # out at MAX_ENTRY_KEYS), so a refresh that changed no pin chooses nothing again
        self._entries: frozenset[Cid] = frozenset()
        self._entries_of: dict[Cid, frozenset[Cid]] = {}
        self._entries_from: tuple = (None, frozenset(), frozenset(), ())
        self._entries_off: str | None = None  # why entries aren't provided (the setting, or a slow node's airtime)
        self._estimated_off = False  # whether the slow-node estimate is why (logged when that changes)
        self._entries_on = False  # whether the last computing refresh provided entries (persisted, as "entries_on")
        self._region_bits_mean: float | None = None  # the network size estimate (network_size), persisted
        # (generation(), deny list's CID set, keys, strategy, what entries depend on) of the last full refresh;
        # None: the next computes
        self._inputs: tuple | None = None
        self._published: dict[ProvidedItem, tuple[float, list[bytes]]] = {}  # item -> (when, nodes that recorded it)
        # Other nodes that may still hold our record of an item (earlier rounds' closest nodes, or a round cut
        # short), with when it was stored there; withdrawn from too, and forgotten once PROVIDER_TTL has passed
        self._earlier: dict[ProvidedItem, dict[bytes, float]] = {}
        self._withdrawals: dict[bytes, set[bytes]] = {}  # node -> keys to withdraw from it
        self._retry_at: dict[ProvidedItem, float] = {}  # items whose last lookup found no nodes
        self._next_lookup = 0.0  # slow nodes: clock time before which no lookup round starts
        self._region_bits: int | None = None  # the prefix length of a recent region (sweep providing), or None
        self._store_parts: list[StorePart] = []  # add_store_part
        # Functions returning the non-CID keys features provide (add_key_source); polled by refresh()
        self._key_sources: list[Callable[[], set[bytes]]] = []
        # Writes what changed since the last save (_touch_item, _touch_node), when persisted
        self._store: _PublishState | None = None
        self._db = None if state_dir is None else statedb.open_store(state_dir, self._load)

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
        """Load last-published times and nodes, earlier rounds' nodes and pending withdrawals. Rows of the wrong
        shape are skipped (logged) and deleted; those the load changes (times clamped, earlier nodes past
        PROVIDER_TTL dropped) are marked changed, so the next save writes them as memory has them."""
        self._store = _PublishState(db, self)
        now = self.clock()

        def when(number) -> float:
            """A saved time, clamped to now; ValueError if it isn't a finite number."""
            return min(_time(number), now)

        doomed: set = set()  # item texts whose rows are deleted here: they don't parse, or aren't as str() writes
        malformed = 0

        def item_of(text) -> ProvidedItem | None:
            """The item a row's text names, or None if it doesn't parse (counted and doomed)."""
            nonlocal malformed
            try:
                item = _item(text)
            except (ValueError, TypeError):
                malformed += 1
                doomed.add(text)
                return None
            if str(item) != text:  # deleted, and written again as str() writes it
                doomed.add(text)
                self._touch_item(item)
            return item

        for text, at, nodes in db.query("SELECT item, at, nodes FROM published"):
            item = item_of(text)
            if item is None:
                continue
            try:
                self._published[item] = (when(at), _ids(nodes))
            except (ValueError, TypeError, OverflowError):
                malformed += 1
                self._touch_item(item)
                continue
            if self._published[item][0] != at:
                self._touch_item(item)
        for text, node, stored in db.query("SELECT item, node, at FROM published_earlier"):
            item = item_of(text)
            if item is None:
                continue
            try:
                if not isinstance(node, bytes) or len(node) != ID_LENGTH:
                    raise ValueError(f"not a node ID: {node!r}")
                stored_at = when(stored)
            except (ValueError, TypeError, OverflowError):
                malformed += 1
                self._touch_item(item)
                continue
            if now - stored_at < PROVIDER_TTL:
                self._earlier.setdefault(item, {})[node] = stored_at
            if stored_at != stored or now - stored_at >= PROVIDER_TTL:
                self._touch_item(item)
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
        if doomed:
            with db.transaction() as tx:
                tx.executemany("DELETE FROM published WHERE item = ?", [(text,) for text in doomed])
                tx.executemany("DELETE FROM published_earlier WHERE item = ?", [(text,) for text in doomed])

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
        """Note that item's published or earlier entry changed, for the next save. Caller holds the lock."""
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
        """The meta value name as saved (META_STORE): "region_bits" (None while there is no estimate), or
        "entries_on" (1 or 0). Lock held."""
        if name == "region_bits":
            return self._region_bits_mean
        return int(self._entries_on)

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
        out at MAX_ENTRY_KEYS}."""
        with self._lock:
            capped = len(self._entries_from[3]) if self._entries_off is None else 0
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
        self._entries = self._choose_entries(pinned, dags, len(roots) + len(keys))
        # An entry is a block of a complete pin's DAG, so serving needs nothing more: the entry's DAG is inside it.
        # Entries come only from pins provided here, never on their own
        assert self._entries <= served, "an entry was chosen outside the served set"
        return roots | self._keys_from[1] | self._entries, served

    def _choose_entries(self, pinned: frozenset[Cid], dags: dict[Cid, frozenset[Cid]],
                        other_keys: int) -> frozenset[Cid]:
        """The entries to provide (provide_entries) that aren't provided as roots: those of the pins (pinned, as
        pinned_cids() gave it) among dags (the CIDs provided, with their blocks), up to MAX_ENTRY_KEYS, or none (the
        setting is false, or a slow
        node's default finds that republishing them with the other_keys other keys would take too much of the
        channel). Notes why none are in _entries_off, logging when the estimate turns them off or on. Lock held."""
        setting = self.config.provide_entries
        reason = None
        chosen: frozenset[Cid] = frozenset()
        if setting is False:
            reason = "provide_entries is false"
        else:
            chosen = self._capped_entries(pinned, pinned & dags.keys()) - dags.keys()
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
        """The entries of the pins in complete (pinned is pinned_cids()), up to MAX_ENTRY_KEYS: pins oldest first
        (Storage.pin_order), each pin's entries all or none, so adding a pin never takes away an older pin's
        entries and a folder is findable whole or not at all; a pin that doesn't fit is left out (logged) and later
        ones still may. The same as last time while pins and complete are. Lock held."""
        last_pinned, last_complete, chosen, skipped = self._entries_from
        if last_pinned is pinned and last_complete == complete:
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
            if len(found) + len(entries) > MAX_ENTRY_KEYS and len(found | entries) > MAX_ENTRY_KEYS:
                left_out.append(root)
                continue
            found |= entries
        chosen = frozenset(found)
        if left_out and tuple(left_out) != skipped:
            log(f"provide_entries: the files and folders inside {plural(len(left_out), 'pin')} (the newest) aren't "
                f"provided: they would take this node past {MAX_ENTRY_KEYS} entry keys", LOG_NOTICE)
        self._entries_from = (pinned if walked else None, complete, chosen, tuple(left_out))
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
        """What the entries a refresh chooses depend on besides pins (generation()): the setting, and for a slow
        node's default the network size and republish interval its estimate uses. Lock held."""
        setting = self.config.provide_entries
        if setting is None and self.slow_node:
            return setting, self._network_size(), self.interval
        return (setting,)

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
        refresh that computed: the same inputs give the same sets, so nothing would be added or removed.
        removed also takes in _published, but _published stays within _provided between refreshes: a refresh
        drops from _published what it doesn't provide, only refresh() changes _provided, and a round records
        only items still in _provided. _load fills _published before any refresh, so the first refresh after
        construction always computes (as does the first after add_key_source).
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
            removed = (set(self._provided) | set(self._published)) - provided
            added = provided - self._provided
            self._provided, self._served = provided, served
            for cid in removed:
                self._retry_at.pop(cid, None)
                self.providers.withdraw(cid.key, self.own_id)
                record = self._published.pop(cid, None)
                earlier = self._earlier.pop(cid, {})
                if record is not None or earlier:
                    self._touch_item(cid)
                for node_id in set(record[1] if record else []) | set(earlier):
                    self._withdrawals.setdefault(node_id, set()).add(cid.key)
                    self._touch_node(node_id)
            # The keys withdrawals are queued for, gathered once: most added items have none to cancel
            pending = {key for keys in self._withdrawals.values() for key in keys} if added else set()
            cancelled = [cid for cid in added if self._provide_again(cid, pending)]
            self._inputs = inputs  # only once everything above is done: an exception computes again next time
            if removed:
                log(f"no longer providing {plural(len(removed), 'CID')}", LOG_VERBOSE)
        if removed or cancelled:
            self.save()
        if removed or any(self._is_due(cid, self.clock()) for cid in added):
            self.wake()

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
            earlier = self._earlier.setdefault(cid, {})
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
        """True if cid should be (re)published now (lock held or not; reads are atomic)."""
        retry = self._retry_at.get(cid)
        if retry is not None:
            return now >= retry
        record = self._published.get(cid)
        return record is None or now - record[0] >= self.interval_for(cid)

    def _clamp(self, now: float):
        """Bring times after now back to now (the wall clock was corrected backwards), so republishing and
        pacing don't stall until the clock catches up. Caller holds the lock."""
        for cid, (when, nodes) in list(self._published.items()):
            if when > now:
                self._published[cid] = (now, nodes)
                self._touch_item(cid)
        for cid, retry in list(self._retry_at.items()):
            self._retry_at[cid] = min(retry, now + self.retry_interval)
        self._next_lookup = min(self._next_lookup, now + SLOW_LOOKUP_PACE * SLOW_ROUND_LOOKUPS)

    def next_due(self) -> float:
        """Seconds until something is due (0 if now); withdrawals pending count as due (paced on slow nodes)."""
        now = self.clock()
        with self._lock:
            self._clamp(now)
            waits = []
            if any(self._withdrawals.values()):
                if not self.slow_node:
                    return 0.0
                waits.append(0.0)
            for cid in self._provided:
                retry = self._retry_at.get(cid)
                record = self._published.get(cid)
                if retry is not None:
                    waits.append(retry - now)
                elif record is None:
                    waits.append(0.0)
                else:
                    waits.append(record[0] + self.interval_for(cid) - now)
            wait = min(waits, default=MAX_WAIT)
            if self.slow_node and waits:
                wait = max(wait, self._next_lookup - now)  # lookups and withdrawals paced
        return max(0.0, wait)

    def publish_due(self, should_stop: Callable[[], bool] = lambda: False) -> dict:
        """One publishing round: provide every due CID and send pending withdrawals, batched per node.

        Returns {"published": CIDs published, "nodes": nodes contacted, "withdrawn": keys withdrawn, "lookups":
        region lookups run, "finds": extra /find their regions sent}.
        """
        now = self.clock()
        with self._lock:
            self._clamp(now)
            # Never published first, then the longest unpublished: what pacing defers is the least urgent
            due = sorted((cid for cid in self._provided if self._is_due(cid, now)),
                         key=lambda item: (self._published.get(item, (float("-inf"),))[0], item.key))
            paced = self.slow_node and now < self._next_lookup
            if paced:
                due = []
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
        # In due's order, so a round's batches are the same however the lookups were run
        regions = {item: sweep.covered[item.key] for item in due if item.key in sweep.covered}
        for cid in regions:
            self.providers.store(cid.key, self.own_id)  # providers also record themselves locally
        # Built without the lock: a part may take other locks (the first-seen log's)
        closest = {cid: self._store_set(cid, region) for cid, region in regions.items()}
        with self._lock:
            # Taken after the lookups, which can be long, so withdrawals queued meanwhile go in this round
            withdrawals = {node: set(keys) for node, keys in self._withdrawals.items() if keys}
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
                room = 0 if paced else max(0, SLOW_ROUND_LOOKUPS - lookups)
                for node_id in alone[room:]:
                    del withdrawals[node_id]
                spent = lookups + min(room, len(alone))
                if spent:
                    self._next_lookup = self.clock() + SLOW_LOOKUP_PACE * spent
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
            # Not retried until the next republish whatever the outcome, so it counts as sent either way.
            # The node may hold records of the keys it got before any refusal: those are withdrawn later
            recorded = self.client.provide(node_id, [cid.key for cid in cids], sorted(withdrawn))
            return cids, withdrawn, recorded, self.clock()

        parallel = 1 if self.slow_node else PROVIDE_PARALLEL
        for node_id, outcome in _in_order(sorted(set(by_node) | set(withdrawals)), send, should_stop, parallel):
            if outcome is None:
                continue
            cids, withdrawn, recorded, answered = outcome
            sent_withdrawals[node_id] = withdrawn
            with self._lock:
                for cid in cids:
                    if cid.key not in recorded:
                        continue
                    accepted[cid].append(node_id)
                    # Remembered at once, and saved every few nodes, so a round cut short by a crash still
                    # leaves a record of where it stored, to withdraw from (the round's end settles the rest)
                    if cid in self._provided:
                        earlier = self._earlier.setdefault(cid, {})
                        earlier[node_id] = max(answered, earlier.get(node_id, answered))
                        self._touch_item(cid)
                    else:
                        self._withdrawals.setdefault(node_id, set()).add(cid.key)
                        self._touch_node(node_id)
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
                if not nodes:
                    self._retry_at[cid] = self.clock() + self.retry_interval
                    continue
                self._retry_at.pop(cid, None)
                self._touch_item(cid)
                # Nodes this round didn't store it on may still hold an earlier record: withdrawn from too
                earlier = {n: t for n, t in self._earlier.pop(cid, {}).items() if now - t < PROVIDER_TTL}
                previous = self._published.pop(cid, None)
                for node_id in (previous[1] if previous else []):
                    earlier[node_id] = max(previous[0], earlier.get(node_id, previous[0]))
                if all(node_id in sent_withdrawals for node_id in nodes):
                    self._published[cid] = (now, accepted[cid])
                    for node_id in accepted[cid]:
                        earlier.pop(node_id, None)
                else:
                    # Stopped before every node was sent its keys: left due (unpublished), keeping where it went
                    for node_id in accepted[cid]:
                        earlier[node_id] = max(now, earlier.get(node_id, now))
                    unfinished.append(cid)
                if earlier:
                    self._earlier[cid] = earlier
        published = [cid for cid in closest if cid not in unfinished]
        if closest or sent_withdrawals:
            self.save()
            stored = sum(len(nodes) for nodes in accepted.values())
            log(f"published {plural(len(published), 'CID')} ({stored} records on "
                f"{plural(len(by_node), 'node')}; {plural(lookups, 'region lookup')}, {finds} extra /find), "
                f"withdrew {sum(len(k) for k in sent_withdrawals.values())}"
                + (f"; stopped before sending {plural(len(unfinished), 'CID')} to every node" if unfinished else ""),
                LOG_VERBOSE)
        return {"published": published, "nodes": sorted(set(by_node) | set(sent_withdrawals)),
                "withdrawn": sum(len(keys) for keys in sent_withdrawals.values()), "lookups": lookups, "finds": finds}

    def _learn_network_size(self, regions: list[Region]):
        """Fold the prefix lengths of regions a round found (those that found a server) into the running mean the
        network size is estimated from (network_size), saved with the publish state. Lock held."""
        for region in regions:
            if not region.closest:
                continue
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

    def _store_set(self, cid: ProvidedItem, region: Region) -> list[bytes]:
        """The servers cid is stored on this round: its K closest from region, then each store part's servers, without
        duplicates or this node; none if the region found no server. Called without the lock."""
        nodes = [n for n in region.closest_to(cid.key) if n != self.own_id]
        if not nodes:
            return []
        chosen = set(nodes)
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
        return nodes

    def _sweep(self, due: list[ProvidedItem], should_stop: Callable[[], bool], limit: int | None) -> Sweep:
        """The regions of due's keys whose lookups ran (dht.sweep), taking keys in due's order: at most limit
        lookups (None: no limit). Called without the lock: refresh() may run meanwhile.

        should_stop() is checked before each lookup starts; one running isn't interrupted. Slow nodes (and a
        single item) look up one at a time; other nodes run up to PUBLISH_LOOKUPS at once on
        rnfs-publish-lookup threads. A lookup that raises starts no more; the exception is raised once those
        running have finished, ending the round as it would one at a time.
        """
        sweep = Sweep([item.key for item in due], self._region_bits)

        def look_up(key: bytes):
            """Run the lookup for key and cover its region."""
            region = find_region(self.make_lookup(key), key, self.own_id, lambda bits: sweep.members(key, bits))
            sweep.cover(region)

        if self.slow_node or len(due) < 2 or PUBLISH_LOOKUPS < 2:
            while (limit is None or len(sweep.regions) < limit) and not should_stop():
                key = sweep.take()
                if key is None:
                    break
                look_up(key)
        else:
            self._sweep_at_once(sweep, look_up, should_stop)
        if sweep.regions:
            self._region_bits = sweep.estimate
        return sweep

    @staticmethod
    def _sweep_at_once(sweep: Sweep, look_up: Callable[[bytes], None], should_stop: Callable[[], bool]):
        """Run sweep's lookups PUBLISH_LOOKUPS at once (_sweep), raising the first exception one raised."""
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
                   for _ in range(PUBLISH_LOOKUPS)]
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
