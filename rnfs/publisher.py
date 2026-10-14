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

Last-published nodes and times are persisted in <state dir>/published.json, so a restart doesn't
republish what isn't due. A CID whose lookup found no node at all (nothing was sent) is retried after
RETRY_INTERVAL rather than a whole republish interval; one whose round was stopped before every node was
sent its keys stays due. Times are wall clock: any in the future (the clock was corrected) count as now.

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
import os
import threading
import time
from typing import Callable

from rnfs.config import Config
from rnfs.constants import DAY, HOUR, PROVIDER_TTL
from rnfs.content.cid import Cid
from rnfs.content.storage import Storage, read_json, shaped, write_json
from rnfs.errors import InvalidDag, NotFound
from rnfs.log import LOG_ERROR, LOG_VERBOSE, log, node_name, plural

STATE_FILE = "published.json"
SECTIONS = ("published", "earlier", "withdraw")  # the state file's objects
RETRY_INTERVAL = 30.0  # seconds before retrying a CID whose lookup found no nodes to publish to
MAX_WAIT = 60.0  # the loop wakes at least this often
KEY_PREFIX = "key:"  # how a DhtKey is written in the state file
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
        # Other nodes that may still hold our record of a CID (earlier rounds' closest nodes, or a round cut
        # short), with when it was stored there; withdrawn from too, and forgotten once PROVIDER_TTL has passed
        self._earlier: dict[Cid, dict[bytes, float]] = {}
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
        """Load last-published times and nodes, and pending withdrawals. A file of the wrong shape is set aside
        as damaged; entries of the wrong shape are skipped."""
        if self._state_path is None:
            return
        value = read_json(self._state_path, shape=lambda v: all(shaped(key, dict)(v) for key in SECTIONS)) or {}
        now = self.clock()

        def entries(key: str) -> list:
            """The items of value[key]."""
            return list(value.get(key, {}).items())

        def when(number) -> float:
            """A persisted time, clamped to now; ValueError if it isn't a finite number."""
            if isinstance(number, bool) or not isinstance(number, (int, float)) or not math.isfinite(number):
                raise ValueError(f"not a time: {number!r}")
            return min(float(number), now)

        for text, record in entries("published"):
            try:
                published, nodes = record
                if not isinstance(nodes, list):
                    raise TypeError("nodes must be a list")
                self._published[_parse_item(text)] = (when(published), [bytes.fromhex(n) for n in nodes])
            except (ValueError, TypeError, OverflowError):
                continue
        for text, nodes in entries("earlier"):
            try:
                if not isinstance(nodes, dict):
                    raise TypeError("nodes must be an object")
                earlier = {bytes.fromhex(n): when(stored) for n, stored in nodes.items()}
                self._earlier[_parse_item(text)] = {n: t for n, t in earlier.items() if now - t < PROVIDER_TTL}
            except (ValueError, TypeError, OverflowError):
                continue
        for node_hex, keys in entries("withdraw"):
            try:
                if not isinstance(keys, list):
                    raise TypeError("keys must be a list")
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
                     "earlier": {str(cid): {n.hex(): when for n, when in nodes.items()}
                                 for cid, nodes in self._earlier.items() if nodes},
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
                for node_id in set(record[1] if record else []) | set(self._earlier.pop(cid, {})):
                    self._withdrawals.setdefault(node_id, set()).add(cid.key)
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
        self.providers.store(cid.key, self.own_id)  # refresh() withdrew it when it stopped being provided
        now = self.clock()
        cancelled = False
        for node_id, keys in list(self._withdrawals.items()):
            if cid.key in keys:
                keys.discard(cid.key)
                if not keys:
                    del self._withdrawals[node_id]
                earlier = self._earlier.setdefault(cid, {})
                earlier[node_id] = max(now, earlier.get(node_id, now))
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
                    else:
                        self._withdrawals.setdefault(node_id, set()).add(cid.key)
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
