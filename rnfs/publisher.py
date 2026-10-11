"""Keeping what this node provides advertised in the DHT (specification section 6).

- **What is provided** (provide_strategy): `pinned` provides every complete pin; `roots` also every complete
  fetched target while it is cached (the node says which cached targets qualify: gateway fetches never do).
  Only these CIDs' DAGs are served to /blocks (section 7).
- **Publishing.** A CID is due when it has never been published or its republish_interval has passed. For
  each due CID the K closest nodes are found; keys are grouped by node, and each node gets its keys (and any
  withdrawals for it) in as few /provide requests as fit, over one identified link. A refused or failed
  store isn't retried until the next republish. The node also records itself locally.
- **Withdrawing.** When a CID stops being provided (unpinned, evicted, gc'd, damaged), its key is withdrawn
  from the nodes it was last published to, at once.

Last-published nodes and times are persisted in <state dir>/published.json, so a restart doesn't
republish what isn't due. A CID whose lookup found no node at all (nothing was sent) is retried after
RETRY_INTERVAL rather than a whole republish interval.

- **Feature keys** (DhtKey, e.g. kept names) are republished at max(republish interval, KEY_MIN_INTERVAL),
  each with its own fixed jitter of up to +-KEY_JITTER, so they don't all come due in one burst; always
  below PROVIDER_TTL / 2.
- **Slow nodes** pace lookups: at most SLOW_ROUND_LOOKUPS per round and on average one per SLOW_LOOKUP_PACE
  seconds; never-published items go first, then the longest unpublished.
- A key both withdrawn from and provided to a node in one round is only provided (a restart may queue
  withdrawals of keys that are still provided).
"""
import hashlib
import os
import threading
import time
from typing import Callable

from rnfs.config import Config
from rnfs.constants import DAY, HOUR, PROVIDER_TTL
from rnfs.content.cid import Cid
from rnfs.content.storage import Storage, read_json, write_json
from rnfs.errors import InvalidDag, NotFound
from rnfs.log import LOG_ERROR, LOG_VERBOSE, log, node_name, plural

STATE_FILE = "published.json"
RETRY_INTERVAL = 30.0  # seconds before retrying a CID whose lookup found no nodes to publish to
MAX_WAIT = 60.0  # the loop wakes at least this often
KEY_PREFIX = "key:"  # how a DhtKey is written in the state file
# A kept name's record changes rarely and keepers answer for years-old ones; its provider record lives
# PROVIDER_TTL (7 days), so republishing every 3 days still survives a missed round
KEY_MIN_INTERVAL = 3 * DAY
KEY_JITTER = 0.1  # +-10% of the interval, fixed per key
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
    """A provided item as the state file writes it. Raises ValueError."""
    return DhtKey.parse(text) if isinstance(text, str) and text.startswith(KEY_PREFIX) else Cid.parse(text)


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
        self._state_path = None if state_dir is None else os.path.join(state_dir, STATE_FILE)
        self._lock = threading.RLock()
        self._wake = threading.Event()
        self._provided: frozenset[Cid] = frozenset()
        self._served: frozenset[Cid] = frozenset()
        self._published: dict[Cid, tuple[float, list[bytes]]] = {}  # CID -> (when, nodes that recorded it)
        self._withdrawals: dict[bytes, set[bytes]] = {}  # node -> keys to withdraw from it
        self._retry_at: dict[Cid, float] = {}  # CIDs whose last lookup found no nodes
        self._next_lookup = 0.0  # slow nodes: clock time before which no lookup round starts
        # Functions returning the non-CID keys features provide (add_key_source); polled by refresh()
        self._key_sources: list[Callable[[], set[bytes]]] = []
        self._load()

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
        """Load last-published times and nodes, and pending withdrawals."""
        if self._state_path is None:
            return
        value = read_json(self._state_path) or {}
        for text, record in (value.get("published") or {}).items():
            try:
                when, nodes = record
                self._published[_parse_item(text)] = (float(when), [bytes.fromhex(n) for n in nodes])
            except (ValueError, TypeError):
                continue
        for node_hex, keys in (value.get("withdraw") or {}).items():
            try:
                self._withdrawals[bytes.fromhex(node_hex)] = {bytes.fromhex(k) for k in keys}
            except (ValueError, TypeError):
                continue

    def save(self):
        """Persist the publish state (atomic)."""
        if self._state_path is None:
            return
        with self._lock:
            value = {"published": {str(cid): [when, [n.hex() for n in nodes]]
                                   for cid, (when, nodes) in self._published.items()},
                     "withdraw": {node.hex(): sorted(k.hex() for k in keys)
                                  for node, keys in self._withdrawals.items() if keys}}
            write_json(self._state_path, value)

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
        """(provided CIDs, blocks they reach) from storage and the strategy."""
        candidates = set(self.storage.pins())
        if self.config.provide_strategy == "roots":
            candidates |= self.storage.cached_targets() & set(self.cached_eligible())
        provided = set()
        served: set[Cid] = set()
        for cid in candidates:
            try:
                blocks = self.storage.dag_blocks(cid)  # memoised; raises unless complete
            except (NotFound, InvalidDag):
                continue
            provided.add(cid)
            served |= blocks
        for source in list(self._key_sources):
            try:
                provided |= {DhtKey(key) for key in source()}
            except Exception as e:  # noqa: BLE001 - a broken feature mustn't stop CIDs being provided
                log(f"a provided-key source failed: {e!r}", LOG_ERROR)
        return frozenset(provided), frozenset(served)

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
                for node_id in (record[1] if record else []):
                    self._withdrawals.setdefault(node_id, set()).add(cid.key)
            if removed:
                log(f"no longer providing {plural(len(removed), 'CID')}", LOG_VERBOSE)
        if removed:
            self.save()
        if removed or any(self._is_due(cid, self.clock()) for cid in added):
            self.wake()

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

    def next_due(self) -> float:
        """Seconds until something is due (0 if now); withdrawals pending count as due."""
        now = self.clock()
        with self._lock:
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
            # Never published first, then the longest unpublished: what pacing defers is the least urgent
            due = sorted((cid for cid in self._provided if self._is_due(cid, now)),
                         key=lambda item: (self._published.get(item, (float("-inf"),))[0], item.key))
            withdrawals = {node: set(keys) for node, keys in self._withdrawals.items() if keys}
            if self.slow_node:
                due = due[:SLOW_ROUND_LOOKUPS] if now >= self._next_lookup else []

        closest: dict[Cid, list[bytes]] = {}
        for cid in due:
            if should_stop():
                break
            self.providers.store(cid.key, self.own_id)  # providers also record themselves locally
            closest[cid] = [n for n in self.make_lookup(cid.key).find_closest() if n != self.own_id]
        if self.slow_node and closest:
            with self._lock:
                self._next_lookup = self.clock() + SLOW_LOOKUP_PACE * len(closest)

        by_node: dict[bytes, list[Cid]] = {}
        for cid, nodes in closest.items():
            for node_id in nodes:
                by_node.setdefault(node_id, []).append(cid)
        superseded: dict[bytes, set[bytes]] = {}
        for node_id, cids in by_node.items():
            # Withdrawing a key from a node while storing it there would delete the fresh record (a restart
            # can queue withdrawals of keys still provided): the store wins, and the withdrawal is dropped
            if node_id in withdrawals:
                superseded[node_id] = withdrawals[node_id] & {cid.key for cid in cids}
                withdrawals[node_id] -= superseded[node_id]
        accepted: dict[Cid, list[bytes]] = {cid: [] for cid in closest}
        sent_withdrawals: dict[bytes, set[bytes]] = {}
        for node_id in sorted(set(by_node) | set(withdrawals)):
            if should_stop():
                break
            cids = by_node.get(node_id, [])
            withdrawn = withdrawals.get(node_id, set())
            # Not retried until the next republish whatever the outcome, so it counts as sent either way
            ok = self.client.provide(node_id, [cid.key for cid in cids], sorted(withdrawn))
            sent_withdrawals[node_id] = withdrawn
            if ok:
                for cid in cids:
                    accepted[cid].append(node_id)
            else:
                log(f"{node_name(node_id)} didn't record {plural(len(cids), 'key')}", LOG_VERBOSE)

        with self._lock:
            for node_id, keys in sent_withdrawals.items():
                remaining = self._withdrawals.get(node_id, set()) - keys - superseded.get(node_id, set())
                if remaining:
                    self._withdrawals[node_id] = remaining
                else:
                    self._withdrawals.pop(node_id, None)
            for cid, nodes in closest.items():
                if cid not in self._provided:
                    # Stopped being provided while this round ran: withdraw what was just stored
                    for node_id in accepted[cid]:
                        self._withdrawals.setdefault(node_id, set()).add(cid.key)
                    continue
                if not nodes:
                    self._retry_at[cid] = self.clock() + self.retry_interval
                    continue
                self._retry_at.pop(cid, None)
                self._published[cid] = (now, accepted[cid])
        if closest or sent_withdrawals:
            self.save()
            stored = sum(len(nodes) for nodes in accepted.values())
            log(f"published {plural(len(closest), 'CID')} ({stored} records on "
                f"{plural(len(by_node), 'node')}), withdrew {sum(len(k) for k in sent_withdrawals.values())}",
                LOG_VERBOSE)
        return {"published": list(closest), "nodes": sorted(set(by_node) | set(sent_withdrawals)),
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
