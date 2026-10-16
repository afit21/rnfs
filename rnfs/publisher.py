"""Keeping what this node provides advertised in the DHT (specification section 6).

- **What is provided** (provide_strategy): `pinned` provides every complete pin; `roots` also every complete
  fetched target while it is cached (the node says which cached targets qualify: gateway fetches never do).
  Only these CIDs' DAGs are served to /blocks (section 7).
- **Publishing.** A CID is due when it has never been published or its republish_interval has passed. For
  each due CID the K closest nodes are found; keys are grouped by node, and each node gets its keys (and any
  withdrawals for it) in as few /provide requests as fit, over one identified link. A refused or failed
  store isn't retried until the next republish. The node also records itself locally.
- **Withdrawing.** When a CID stops being provided (unpinned, evicted, gc'd, damaged), its key is withdrawn
  from the nodes it was last published to, at once, and from nodes of earlier rounds (or of a round cut
  short) whose record may not have expired yet: the closest set changes as nodes come and go.

Last-published nodes and times, earlier rounds' nodes and pending withdrawals are persisted in the state
database (rnfs.statedb: the published, published_earlier and withdrawals tables), each save writing only the
items and nodes changed since the last, so a restart doesn't republish what isn't due. A CID whose lookup found
no node at all (nothing was sent) is retried after RETRY_INTERVAL rather than a whole republish interval; one
whose round was stopped before every node was sent its keys stays due. Times are wall clock: any in the future
(the clock was corrected) count as now.

- **Feature keys** (DhtKey, e.g. kept names) are republished at max(republish interval, KEY_MIN_INTERVAL),
  each with its own fixed jitter of up to +-KEY_JITTER, so they don't all come due in one burst; always
  below PROVIDER_TTL / 2.
- **Slow nodes** pace lookups: at most SLOW_ROUND_LOOKUPS per round and on average one per SLOW_LOOKUP_PACE
  seconds; never-published items go first, then the longest unpublished.
- A key both withdrawn from and provided to a node in one round is only provided (a restart may queue
  withdrawals of keys that are still provided).
"""
import hashlib
import math
import threading
import time
from typing import Callable

from rnfs import statedb
from rnfs.config import Config
from rnfs.constants import DAY, HOUR, ID_LENGTH, PROVIDER_TTL
from rnfs.content.cid import Cid
from rnfs.content.storage import Storage
from rnfs.errors import InvalidDag, NotFound
from rnfs.log import LOG_ERROR, LOG_VERBOSE, LOG_WARNING, log, node_name, plural

RETRY_INTERVAL = 30.0  # seconds before retrying a CID whose lookup found no nodes to publish to
MAX_WAIT = 60.0  # the loop wakes at least this often
KEY_PREFIX = "key:"  # how a DhtKey is written as a published item (str())
# A kept name's record changes rarely and keepers answer for years-old ones; its provider record lives
# PROVIDER_TTL (7 days), so republishing every 3 days still survives a missed round
KEY_MIN_INTERVAL = 3 * DAY
KEY_JITTER = 0.1  # +-10% of the interval, fixed per key
# While a round sends, where it stored is saved after this many nodes or seconds, whichever comes first, so a
# crash loses at most that much of it (a record no saved state lists can't be withdrawn: it lives PROVIDER_TTL)
SAVE_EVERY_NODES = 10
SAVE_EVERY_SECONDS = 30.0
# Slow nodes: a closest-nodes lookup is several /find exchanges (alpha 1, a few hops), seconds of airtime at
# LoRa rates. One per 30 s keeps republishing a hundred keys to well under 1% of a day's airtime per hour
SLOW_LOOKUP_PACE = 30.0
SLOW_ROUND_LOOKUPS = 10  # lookups per round on slow nodes, so /provide batching still groups keys per node


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


def key_interval(key: bytes, base: float) -> float:
    """The republish interval of DhtKey key: max(base, KEY_MIN_INTERVAL) with key's fixed jitter, below
    PROVIDER_TTL / 2 (base is already below it: Config.validate)."""
    interval = max(base, KEY_MIN_INTERVAL)
    fraction = int.from_bytes(hashlib.sha256(b"rnfs-republish-jitter" + key).digest()[:8], "big") / 2**64
    jittered = interval * (1 + KEY_JITTER * (2 * fraction - 1))
    # Jitter upward never reaches PROVIDER_TTL / 2: a record must survive one missed republish
    return min(jittered, max(interval, PROVIDER_TTL / 2 - HOUR))


def _parse_item(text: str) -> "Cid | DhtKey":
    """A provided item as str() writes it (the published tables' item). Raises ValueError."""
    return DhtKey.parse(text) if isinstance(text, str) and text.startswith(KEY_PREFIX) else Cid.parse(text)


def _item(text) -> "Cid | DhtKey":
    """A saved provided item: _parse_item, but only an ID_LENGTH-byte DhtKey. Raises ValueError."""
    item = _parse_item(text)
    if isinstance(item, DhtKey) and len(item.key) != ID_LENGTH:
        raise ValueError(f"not a provided key: {text!r}")
    return item


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


class Publisher:
    """The provide set, the served set, and the publish/withdraw schedule. Thread-safe."""

    def __init__(self, own_id: bytes, config: Config, storage: Storage, providers, client,
                 make_lookup: Callable[[bytes], object], slow_node: bool = False, state_dir: str | None = None,
                 cached_eligible: Callable[[], set[Cid]] = set, clock: Callable[[], float] = time.time,
                 retry_interval: float = RETRY_INTERVAL):
        """Publisher for own_id. providers is the ProviderStore (to record ourselves), client the PeerClient.

        make_lookup(key) returns a dht.lookup.Lookup; cached_eligible() returns the cached targets the
        roots strategy may provide.
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
        self._lock = threading.RLock()
        self._save_lock = threading.Lock()  # one save at a time, so an older snapshot never lands last
        self._wake = threading.Event()
        self._provided: frozenset[Cid] = frozenset()
        self._served: frozenset[Cid] = frozenset()
        # What _compute last built, and from what, so a refresh that changed nothing needn't build it again: the
        # served set from each provided CID's dag_blocks (memoised by storage, so the same object while its DAG
        # stays complete), and the DhtKeys from the keys the key sources returned
        self._served_from: tuple[dict[Cid, frozenset[Cid]], frozenset[Cid]] = ({}, frozenset())
        self._keys_from: tuple[set[bytes], frozenset[DhtKey]] = (set(), frozenset())
        self._published: dict[Cid, tuple[float, list[bytes]]] = {}  # CID -> (when, nodes that recorded it)
        # Other nodes that may still hold our record of a CID (earlier rounds' closest nodes, or a round cut
        # short), with when it was stored there; withdrawn from too, and forgotten once PROVIDER_TTL has passed
        self._earlier: dict[Cid, dict[bytes, float]] = {}
        self._withdrawals: dict[bytes, set[bytes]] = {}  # node -> keys to withdraw from it
        self._retry_at: dict[Cid, float] = {}  # CIDs whose last lookup found no nodes
        self._next_lookup = 0.0  # slow nodes: clock time before which no lookup round starts
        # Functions returning the non-CID keys features provide (add_key_source); polled by refresh()
        self._key_sources: list[Callable[[], set[bytes]]] = []
        # Changed since the last save (only tracked when persisted): items whose published or earlier rows, and
        # nodes whose withdrawals, save() writes again
        self._dirty_items: set = set()
        self._dirty_nodes: set = set()
        self._db = None if state_dir is None else statedb.connect(state_dir)
        if self._db is not None:
            try:
                self._load()
            except BaseException:
                self._db.close()
                raise

    def add_key_source(self, source: Callable[[], set[bytes]]):
        """Also provide the keys source() returns (DhtKeys), recomputed at each refresh(). Must not block."""
        with self._lock:
            self._key_sources.append(source)

    @property
    def interval(self) -> float:
        """Seconds between republishes: republish_interval, 3 days on slow nodes by default."""
        return self.config.effective_republish_interval(self.slow_node)

    def interval_for(self, item: "Cid | DhtKey") -> float:
        """Seconds between republishes of item: interval for a CID, key_interval for a DhtKey."""
        if isinstance(item, DhtKey):
            return key_interval(item.key, self.interval)
        return self.interval

    # Persistence

    def _load(self):
        """Load last-published times and nodes, earlier rounds' nodes and pending withdrawals. Rows of the wrong
        shape are skipped (logged) and deleted; those the load changes (times clamped, earlier nodes past
        PROVIDER_TTL dropped) are marked changed, so the next save writes them as memory has them."""
        now = self.clock()

        def when(number) -> float:
            """A saved time, clamped to now; ValueError if it isn't a finite number."""
            return min(_time(number), now)

        doomed: set = set()  # item texts whose rows are deleted here: they don't parse, or aren't as str() writes
        malformed = 0

        def item_of(text) -> "Cid | DhtKey | None":
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

        for text, at, nodes in self._db.query("SELECT item, at, nodes FROM published"):
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
        for text, node, stored in self._db.query("SELECT item, node, at FROM published_earlier"):
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
        for node, key in self._db.query("SELECT node, key FROM withdrawals"):
            if not all(isinstance(value, bytes) and len(value) == ID_LENGTH for value in (node, key)):
                malformed += 1
                self._touch_node(node)
                continue
            self._withdrawals.setdefault(node, set()).add(key)
        if malformed:
            log(f"publisher: ignoring {plural(malformed, 'malformed row')} in the state database", LOG_WARNING)
        if doomed:
            with self._db.transaction() as tx:
                tx.executemany("DELETE FROM published WHERE item = ?", [(text,) for text in doomed])
                tx.executemany("DELETE FROM published_earlier WHERE item = ?", [(text,) for text in doomed])

    def save(self):
        """Persist the publish state changed since the last save, if any. Called without the lock: it is written
        outside it. Raises OSError (StateDbError) if the database fails; what it held is then written next time."""
        if self._db is None:
            return
        # Saves come from refresh(), the publish loop and stop(): one at a time, or a snapshot taken before the
        # other's could be written last
        with self._save_lock:
            with self._lock:
                items, nodes = self._dirty_items, self._dirty_nodes
                if not items and not nodes:
                    return
                self._dirty_items, self._dirty_nodes = set(), set()
                texts = {item: str(item) for item in items}
                published = [(texts[item], self._published[item][0], b"".join(self._published[item][1]))
                             for item in items if item in self._published]
                earlier = [(texts[item], node, at)
                           for item in items for node, at in self._earlier.get(item, {}).items()]
                withdrawals = [(node, key) for node in nodes for key in self._withdrawals.get(node, ())]
            try:
                with self._db.transaction() as tx:
                    tx.executemany("DELETE FROM published WHERE item = ?", [(text,) for text in texts.values()])
                    tx.executemany("DELETE FROM published_earlier WHERE item = ?",
                                   [(text,) for text in texts.values()])
                    tx.executemany("INSERT OR REPLACE INTO published (item, at, nodes) VALUES (?, ?, ?)", published)
                    tx.executemany("INSERT OR REPLACE INTO published_earlier (item, node, at) VALUES (?, ?, ?)",
                                   earlier)
                    tx.executemany("DELETE FROM withdrawals WHERE node = ?", [(node,) for node in nodes])
                    tx.executemany("INSERT OR REPLACE INTO withdrawals (node, key) VALUES (?, ?)", withdrawals)
            except BaseException:
                with self._lock:  # written by the next save
                    self._dirty_items |= items
                    self._dirty_nodes |= nodes
                raise

    def close(self):
        """Release the state database (the node calls this once it has saved everything at shutdown)."""
        if self._db is not None:
            self._db.close()

    def _touch_item(self, item: "Cid | DhtKey"):
        """Note that item's published or earlier entry changed, for the next save. Caller holds the lock."""
        if self._db is not None:
            self._dirty_items.add(item)

    def _touch_node(self, node_id: bytes):
        """Note that the withdrawals queued for node_id changed, for the next save. Caller holds the lock."""
        if self._db is not None:
            self._dirty_nodes.add(node_id)

    # The provide set

    def provided(self) -> frozenset[Cid]:
        """The CIDs this node currently provides (not the DhtKeys of add_key_source)."""
        with self._lock:
            return frozenset(item for item in self._provided if isinstance(item, Cid))

    def provided_keys(self) -> frozenset[bytes]:
        """The non-CID keys this node currently provides."""
        with self._lock:
            return frozenset(item.key for item in self._provided if isinstance(item, DhtKey))

    def serves(self, cid: Cid) -> bool:
        """True if cid is in the DAG of a provided CID, so /blocks may serve it."""
        return cid in self._served  # a frozenset swapped whole, so reading it needs no lock

    def _compute(self) -> tuple[frozenset[Cid], frozenset[Cid]]:
        """(provided CIDs, blocks they reach) from storage and the strategy. Lock held.

        The served set and the DhtKeys are those of the last call when what they are built from hasn't changed
        (most refreshes): the same complete CIDs with the same dag_blocks objects, and the same keys. CIDs only
        added since add their blocks to it; only one gone or changed builds it again.
        """
        candidates = set(self.storage.pins())
        if self.config.provide_strategy == "roots":
            candidates |= self.storage.cached_targets() & set(self.cached_eligible())
        dags: dict[Cid, frozenset[Cid]] = {}
        for cid in candidates:
            try:
                dags[cid] = self.storage.dag_blocks(cid)  # memoised; raises unless complete
            except (NotFound, InvalidDag):
                continue
        keys: set[bytes] = set()
        for source in list(self._key_sources):
            try:
                keys |= {bytes(key) for key in source()}  # as DhtKey takes them
            except Exception as e:  # noqa: BLE001 - a broken feature mustn't stop CIDs being provided
                log(f"a provided-key source failed: {e!r}", LOG_ERROR)
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
        return frozenset(dags) | self._keys_from[1], served

    def refresh(self):
        """Recompute what is provided (after pins, fetches, evictions); withdraw what no longer is, at once."""
        with self._lock:
            provided, served = self._compute()
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
            cancelled = [cid for cid in added if self._provide_again(cid)]
            if removed:
                log(f"no longer providing {plural(len(removed), 'CID')}", LOG_VERBOSE)
        if removed or cancelled:
            self.save()
        if removed or any(self._is_due(cid, self.clock()) for cid in added):
            self.wake()

    def _provide_again(self, cid: "Cid | DhtKey") -> bool:
        """cid is provided (again): record ourselves locally and cancel withdrawals of its key still queued;
        True if any were. Lock held.

        A withdrawal queued when it stopped being provided would otherwise reach a node after a round stored it
        there again, deleting the fresh record (section 6). Those nodes may still hold a record of it, so they
        join its earlier nodes, to be withdrawn from should it stop being provided again.
        """
        key = cid.key
        self.providers.store(key, self.own_id)  # refresh() withdrew it when it stopped being provided
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

    def _is_due(self, cid: Cid, now: float) -> bool:
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
        """Seconds until something is due (0 if now); withdrawals pending count as due."""
        now = self.clock()
        with self._lock:
            self._clamp(now)
            if any(self._withdrawals.values()):
                return 0.0
            waits = []
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
                wait = max(wait, self._next_lookup - now)  # lookups paced
        return max(0.0, wait)

    def publish_due(self, should_stop: Callable[[], bool] = lambda: False) -> dict:
        """One publishing round: provide every due CID and send pending withdrawals, batched per node.

        Returns {"published": CIDs published, "nodes": nodes contacted, "withdrawn": keys withdrawn}.
        """
        now = self.clock()
        with self._lock:
            self._clamp(now)
            # Never published first, then the longest unpublished: what pacing defers is the least urgent
            due = sorted((cid for cid in self._provided if self._is_due(cid, now)),
                         key=lambda item: (self._published.get(item, (float("-inf"),))[0], item.key))
            if self.slow_node:
                due = due[:SLOW_ROUND_LOOKUPS] if now >= self._next_lookup else []

        closest: dict[Cid, list[bytes]] = {}
        for cid in due:
            if should_stop():
                break
            self.providers.store(cid.key, self.own_id)  # providers also record themselves locally
            closest[cid] = [n for n in self.make_lookup(cid.key).find_closest() if n != self.own_id]
        with self._lock:
            if self.slow_node and closest:
                self._next_lookup = self.clock() + SLOW_LOOKUP_PACE * len(closest)
            # Taken after the lookups, which can be long, so withdrawals queued meanwhile go in this round
            withdrawals = {node: set(keys) for node, keys in self._withdrawals.items() if keys}
            for cid in [cid for cid in closest if cid not in self._provided]:
                # Stopped being provided during its lookup: refresh() has withdrawn it, so don't store it
                del closest[cid]
                self.providers.withdraw(cid.key, self.own_id)

            by_node: dict[bytes, list[Cid]] = {}
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
        accepted: dict[Cid, list[bytes]] = {cid: [] for cid in closest}
        sent_withdrawals: dict[bytes, set[bytes]] = {}
        unsaved, saved_at = 0, self.clock()
        for node_id in sorted(set(by_node) | set(withdrawals)):
            if should_stop():
                break
            cids = by_node.get(node_id, [])
            with self._lock:
                # Only what is still queued: a key provided again since the snapshot had its withdrawals cancelled
                withdrawn = withdrawals.get(node_id, set()) & self._withdrawals.get(node_id, set())
            if not cids and not withdrawn:
                continue
            # Not retried until the next republish whatever the outcome, so it counts as sent either way.
            # The node may hold records of the keys it got before any refusal: those are withdrawn later
            recorded = self.client.provide(node_id, [cid.key for cid in cids], sorted(withdrawn))
            sent_withdrawals[node_id] = withdrawn
            answered = self.clock()
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

        unfinished: list[Cid] = []
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
                f"{plural(len(by_node), 'node')}), withdrew {sum(len(k) for k in sent_withdrawals.values())}"
                + (f"; stopped before sending {plural(len(unfinished), 'CID')} to every node" if unfinished else ""),
                LOG_VERBOSE)
        return {"published": published, "nodes": sorted(set(by_node) | set(sent_withdrawals)),
                "withdrawn": sum(len(keys) for keys in sent_withdrawals.values())}

    def run(self, stop_event: threading.Event, ready: threading.Event | None = None):
        """The publish loop, for a thread of its own: waits for ready (the join), then publishes what's due."""
        if ready is not None:
            while not ready.wait(1.0):
                if stop_event.is_set():
                    return
        while not stop_event.is_set():
            self._wake.clear()
            try:
                if self.next_due() <= 0:
                    self.publish_due(stop_event.is_set)
            except Exception as e:
                log(f"publishing failed: {e!r}", LOG_ERROR)
            self._wake.wait(min(MAX_WAIT, max(0.5, self.next_due())))
            if stop_event.is_set():
                break
