"""Keepers (feature "keepers"): names that outlive their owner.

A name resolves through RNS only while its owner is online or a transport node still holds its announce. A
**keeper** stores the latest validly signed announce of a name and serves it, so the name keeps resolving.

- **Signed record.** Exactly what RNS signs in an `rnfs.name` announce: the name key's public key, the name
  hash of "rnfs.name", the random hash (whose last 5 bytes are the signed emission time), the ratchet (empty:
  rnfs names don't use ratchets) and the app_data `[sequence, cid]`, with the signature. Anyone can check it
  without the owner: the signature over `name + public key + name hash + random hash + ratchet + app_data`
  must validate with the public key, and `Destination.hash(public key, "rnfs", "name")` must be the name.
- **`/name`** on rnfs.dht: request `name` (16-byte bin); response `[public_key, name_hash, random_hash,
  ratchet, signature, app_data]` (all bin), or `[]` if this node keeps no record of it (RNS sends nothing
  for a None response), or False if malformed or refused. It fits one packet (at most ~280 bytes).
- **Advertising.** Keepers provide the DHT key `SHA-256(b"rnfs-name" + name)[:16]` through the publisher, like
  a CID (batched /provide, republish, withdrawal); the prefix keeps it apart from CID keys (SHA-256 of a
  36- or 39-byte binary CID). A name is advertised only once a record of it is held.
- **Resolving** (NameManager.resolve): when the path request heard nothing (or always, with keeper_query =
  "always"), find providers of the name's key (past the first alpha, with local records only candidates, until
  two established servers list some), rank them by routing-table standing as it was before the search, then at
  random, and ask them with /name a batch of alpha at a time, each batch drawing first on sources no earlier
  pick came from, asking another batch (up to a bound) while no answer reaches the sequence already known or
  only one keeper (one established keeper, when no sequence is known) answered; only verifying records are
  offered to the usual sequence rules (persisted highest sequence, no rollback).
- **What is kept.** Names kept by the user (`rnfs name keep`), followed names (rnfs.features.follow), and
  this node's own published names, which it signs itself. At most keep_max_names, own names not counted.
- **Refreshing.** On hearing an announce with a higher sequence for a kept name, the signed announce is read
  from RNS's packet cache (Transport.get_cached_packet by the packet hash the announce handler is given:
  an RNS internal, used best effort) and verified; failing that, the keepers are asked in the background,
  which reaches the owner's node too, since it keeps its own names. Kept names still without a record are
  asked again every announce_interval, at most MAX_PENDING at a time, round robin.
- **A floor.** Kept records are offered to NameManager at startup and on every resolve, so a replayed older
  announce is never accepted for a kept name, even after a restart or name table eviction.
- State: <state dir>/keepers.json (atomic; records re-verified at load).

No RNS at module level (the CLI imports this module): it is imported inside functions.
"""
import hashlib
import os
import random
import threading
import time
from dataclasses import dataclass

from rnfs.constants import ID_LENGTH
from rnfs.content.cid import Cid
from rnfs.errors import NotFound
from rnfs.features import Feature, _common
from rnfs.features._common import LOG_ERROR, LOG_NOTICE, LOG_VERBOSE

NAME = "keepers"
NAME_PATH = "/name"
KEY_DOMAIN = b"rnfs-name"
KEEPERS_FILE = "keepers.json"
STATE_VERSION = 1
# Field sizes of an RNS announce (RNS.Identity: KEYSIZE, NAME_HASH_LENGTH, random hash, RATCHETSIZE, SIGLENGTH)
PUBLIC_KEY_LENGTH = 64
NAME_HASH_LENGTH = 10
RANDOM_HASH_LENGTH = 10
RATCHET_LENGTH = 32
SIGNATURE_LENGTH = 64
MAX_APP_DATA = 64  # rnfs.names.MAX_RECORD_SIZE: a record is at most ~52 bytes
# RNS's name hash of the "rnfs.name" aspects: SHA-256 of the expanded name, truncated
NAME_HASH = hashlib.sha256(b"rnfs.name").digest()[:NAME_HASH_LENGTH]
USER, FOLLOW, OWN = "user", "follow", "own"  # why a name is kept
REASONS = (USER, FOLLOW, OWN)
MAX_PENDING = 64  # kept names waiting for a background refresh; more wait for the next round robin retry
RNS_CACHE_TRIES = 4  # a shared instance forwards an announce to its clients before caching it
RNS_CACHE_WAIT = 0.5
LOOP_INTERVAL = 60.0  # the background thread syncs own names and saves at least this often
KEEPER_CANDIDATES = 3  # the search for a name's keepers runs until 3 × alpha are known (from 2+ sources)
KEEPER_ROUNDS = 3  # batches of alpha keepers one query asks at most (2 on slow nodes)
REQUEST_ID_LENGTH = 16
_UNVOUCHED = b""  # ask_keepers: the source of candidates no established node (nor this one) listed


def name_key(name: bytes) -> bytes:
    """The DHT key keepers of name provide: SHA-256(b"rnfs-name" + name), truncated to 128 bits."""
    return hashlib.sha256(KEY_DOMAIN + name).digest()[:ID_LENGTH]


def _log(message: str, level: int = LOG_VERBOSE):
    """Log with this feature's prefix."""
    _common.log(f"keepers: {message}", level)


# Signed records


@dataclass(frozen=True)
class SignedRecord:
    """A verified rnfs.name announce: everything needed to check it again without the owner."""

    name: bytes
    public_key: bytes
    random_hash: bytes
    ratchet: bytes
    signature: bytes
    app_data: bytes
    sequence: int
    cid: Cid

    @property
    def emitted(self) -> int:
        """The emission time the owner signed (seconds since the epoch, the owner's clock)."""
        return int.from_bytes(self.random_hash[5:10], "big")

    def wire(self) -> list[bytes]:
        """The /name response: [public_key, name_hash, random_hash, ratchet, signature, app_data]."""
        return [self.public_key, NAME_HASH, self.random_hash, self.ratchet, self.signature, self.app_data]

    def newer_than(self, other: "SignedRecord | None") -> bool:
        """True if this record should replace other: a higher sequence, or the same record emitted later.

        At an equal sequence with a different CID the record held is kept, as NameManager does.
        """
        if other is None or self.sequence > other.sequence:
            return True
        return self.sequence == other.sequence and self.cid == other.cid and self.emitted > other.emitted


def signed_data(name: bytes, public_key: bytes, random_hash: bytes, ratchet: bytes, app_data: bytes) -> bytes:
    """The bytes RNS signs in an announce (Destination.announce / Identity.validate_announce)."""
    return name + public_key + NAME_HASH + random_hash + ratchet + app_data


def parse_signed(name, fields) -> SignedRecord | None:
    """A SignedRecord for name from [public_key, name_hash, random_hash, ratchet, signature, app_data], or None
    unless every field is exactly well-formed, the record is canonical, the public key hashes to name and
    the signature validates. Everything here may come from a hostile node.
    """
    if type(name) is not bytes or len(name) != ID_LENGTH:
        return None
    if type(fields) is not list or len(fields) != 6 or any(type(field) is not bytes for field in fields):
        return None
    public_key, name_hash, random_hash, ratchet, signature, app_data = fields
    if (len(public_key) != PUBLIC_KEY_LENGTH or name_hash != NAME_HASH or len(random_hash) != RANDOM_HASH_LENGTH
            or len(ratchet) not in (0, RATCHET_LENGTH) or len(signature) != SIGNATURE_LENGTH
            or len(app_data) > MAX_APP_DATA):
        return None
    # The destination check needs only hashes: Destination.hash = SHA-256(name hash + identity hash)[:16],
    # with the identity hash SHA-256(public key)[:16]. Done before the signature, which costs more
    identity_hash = hashlib.sha256(public_key).digest()[:ID_LENGTH]
    if hashlib.sha256(NAME_HASH + identity_hash).digest()[:ID_LENGTH] != name:
        return None
    from rnfs.names import parse_record
    record = parse_record(app_data)
    if record is None:
        return None
    if not _signature_valid(public_key, signature, signed_data(name, public_key, random_hash, ratchet, app_data)):
        return None
    sequence, cid = record
    return SignedRecord(name, public_key, random_hash, ratchet, signature, app_data, sequence, cid)


def _signature_valid(public_key: bytes, signature: bytes, data: bytes) -> bool:
    """True if signature is the Ed25519 signature of data by the RNS identity with public_key."""
    import RNS
    try:
        identity = RNS.Identity(create_keys=False)
        identity.load_public_key(public_key)
        return identity.pub is not None and bool(identity.validate(signature, data))
    except Exception:  # noqa: BLE001 - a malformed key or signature is just invalid
        return False


def from_announce_data(name: bytes, data, has_ratchet: bool) -> SignedRecord | None:
    """A SignedRecord from an RNS announce packet's data (as Identity.validate_announce splits it), or None."""
    if type(data) is not bytes:
        return None
    ratchet_length = RATCHET_LENGTH if has_ratchet else 0
    end = PUBLIC_KEY_LENGTH + NAME_HASH_LENGTH + RANDOM_HASH_LENGTH
    public_key = data[:PUBLIC_KEY_LENGTH]
    name_hash = data[PUBLIC_KEY_LENGTH:PUBLIC_KEY_LENGTH + NAME_HASH_LENGTH]
    random_hash = data[PUBLIC_KEY_LENGTH + NAME_HASH_LENGTH:end]
    ratchet = data[end:end + ratchet_length]
    signature = data[end + ratchet_length:end + ratchet_length + SIGNATURE_LENGTH]
    app_data = data[end + ratchet_length + SIGNATURE_LENGTH:]
    if len(ratchet) != ratchet_length:
        return None
    return parse_signed(name, [public_key, name_hash, random_hash, ratchet, signature, app_data])


def sign_record(identity, name: bytes, app_data: bytes, now: float | None = None) -> SignedRecord:
    """Sign app_data as name's announce would, with the name's RNS identity (no Reticulum needed)."""
    from rnfs.names import parse_record
    parsed = parse_record(app_data)
    if parsed is None:
        raise ValueError("not a name record")
    random_hash = os.urandom(5) + int(time.time() if now is None else now).to_bytes(5, "big")
    public_key = identity.get_public_key()
    signature = identity.sign(signed_data(name, public_key, random_hash, b"", app_data))
    return SignedRecord(name, public_key, random_hash, b"", signature, app_data, parsed[0], parsed[1])


# /name messages


def encode_request(name: bytes) -> bytes:
    """/name request for name."""
    return name


def parse_request(data) -> bytes | None:
    """The name of a /name request, or None if malformed."""
    return data if type(data) is bytes and len(data) == ID_LENGTH else None


def encode_response(record: SignedRecord | None) -> list:
    """/name response: the record's fields, or [] when none is kept."""
    return record.wire() if record is not None else []


def parse_response(name: bytes, data) -> SignedRecord | None:
    """The verified record in a /name response for name; None for [], False and anything malformed."""
    if data == [] or type(data) is not list:
        return None
    return parse_signed(name, data)


def max_response_size() -> int:
    """Largest honest /name response as RNS packs it (for max_response_size: it always fits one packet)."""
    from RNS.vendor import umsgpack
    largest = [bytes(PUBLIC_KEY_LENGTH), NAME_HASH, bytes(RANDOM_HASH_LENGTH), bytes(RATCHET_LENGTH),
               bytes(SIGNATURE_LENGTH), bytes(MAX_APP_DATA)]
    return len(umsgpack.packb([bytes(REQUEST_ID_LENGTH), largest]))


class KeeperClient:
    """Sends /name over the node's shared unidentified link (identified in private mode, by the LinkPool)."""

    def __init__(self, links):
        """Client over a LinkPool."""
        self.links = links

    def name_record(self, node_id: bytes, name: bytes):
        """The raw /name answer of node_id for name, or None if unreachable or unanswered."""
        peer = self.links.get(node_id)
        if peer is None:
            return None
        exchange = self.links.call(peer, NAME_PATH, encode_request(name), max_response_size=max_response_size())
        return exchange.response if exchange.answered else None


# The store


class KeeperStore:
    """Kept names, why each is kept, and the newest signed record of each. Thread-safe."""

    def __init__(self, path: str, max_names: int):
        """Load path; at most max_names names kept for others (own names not counted)."""
        self.path = path
        self.max_names = max_names
        self._lock = threading.Lock()
        self._reasons: dict[bytes, set[str]] = {}
        self._records: dict[bytes, SignedRecord] = {}
        self.dirty = False
        self._save_lock = threading.Lock()  # one save at a time, so an older snapshot never lands last
        self._load()

    def _load(self):
        """Read the state file, re-verifying every record (a damaged entry is dropped, not trusted)."""
        from rnfs.content.storage import read_json
        state = read_json(self.path)
        entries = state.get("names") if isinstance(state, dict) else None
        if not isinstance(entries, dict):
            return
        for name_hex, entry in entries.items():
            try:
                name = bytes.fromhex(name_hex)
                reasons = {reason for reason in entry["reasons"] if reason in REASONS}
                if len(name) != ID_LENGTH or not reasons:
                    continue
                record = None
                if entry.get("record") is not None:
                    record = parse_signed(name, [bytes.fromhex(field) for field in entry["record"]])
            except (AttributeError, KeyError, TypeError, ValueError):
                continue
            self._reasons[name] = reasons
            if record is not None:
                self._records[name] = record

    def save(self):
        """Write the state file atomically if anything changed."""
        from rnfs.content.storage import write_json
        with self._save_lock:
            with self._lock:
                if not self.dirty:
                    return
                state = {"version": STATE_VERSION,
                         "names": {name.hex(): {"reasons": sorted(reasons),
                                                "record": ([field.hex() for field in self._records[name].wire()]
                                                           if name in self._records else None)}
                                   for name, reasons in sorted(self._reasons.items())}}
                self.dirty = False
            try:
                write_json(self.path, state)
            except OSError:
                self.dirty = True  # try again at the next save
                raise

    def _others(self) -> int:
        """Names kept for others (lock held)."""
        return sum(1 for reasons in self._reasons.values() if OWN not in reasons)

    def keep(self, name: bytes, reason: str) -> bool:
        """Keep name for reason; True if it wasn't kept for that reason. ValueError past max_names."""
        with self._lock:
            reasons = self._reasons.get(name)
            if reasons is not None and reason in reasons:
                return False
            if reasons is None and reason != OWN and self._others() >= self.max_names:
                raise ValueError(f"already keeping {self.max_names} names (keep_max_names)")
            self._reasons.setdefault(name, set()).add(reason)
            self.dirty = True
            return True

    def release(self, name: bytes, reason: str) -> bool:
        """Stop keeping name for reason; True if it was kept for it. The record goes with the last reason."""
        with self._lock:
            reasons = self._reasons.get(name)
            if reasons is None or reason not in reasons:
                return False
            reasons.discard(reason)
            if not reasons:
                del self._reasons[name]
                self._records.pop(name, None)
            self.dirty = True
            return True

    def offer(self, record: SignedRecord) -> bool:
        """Store a verified record of a kept name if it is newer than the one held; True if stored."""
        with self._lock:
            if record.name not in self._reasons or not record.newer_than(self._records.get(record.name)):
                return False
            self._records[record.name] = record
            self.dirty = True
            return True

    def get(self, name: bytes) -> SignedRecord | None:
        """The record held for name, if it is kept and one is held."""
        with self._lock:
            return self._records.get(name)

    def reasons(self, name: bytes) -> set[str]:
        """Why name is kept (empty if it isn't)."""
        with self._lock:
            return set(self._reasons.get(name, ()))

    def names(self, reason: str | None = None) -> list[bytes]:
        """Kept names (for reason, if given)."""
        with self._lock:
            return sorted(name for name, reasons in self._reasons.items() if reason is None or reason in reasons)

    def provided_keys(self) -> set[bytes]:
        """The DHT keys to provide: one per kept name whose record is held."""
        with self._lock:
            return {name_key(name) for name in self._records}

    def count(self) -> int:
        """Names kept."""
        with self._lock:
            return len(self._reasons)


# The feature


class KeepersFeature(Feature):
    """Keeps and serves signed name records, and asks keepers when resolving. Thread-safe."""

    name = NAME

    def __init__(self, node):
        """Load kept names, hook into names and the publisher; no network."""
        super().__init__(node)
        self.store = KeeperStore(os.path.join(node.config.state_dir, KEEPERS_FILE), node.config.keep_max_names)
        links = getattr(node, "links", None)
        # Offline test nodes have no LinkPool: their fake client may answer name_record itself
        self.client = KeeperClient(links) if links is not None else node.client
        self._lock = threading.Lock()
        self._pending: dict[bytes, None] = {}  # names to refresh in the background, in order
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._listeners: list = []
        # The last name the periodic retry queued: the next retry starts after it, so past MAX_PENDING
        # names without a record every one is asked in turn
        self._retry_cursor: bytes | None = None
        self._rng = random.Random()  # breaks ties between keepers of equal standing
        node.names.keepers = self
        node.publisher.add_key_source(self.store.provided_keys)
        # Kept records are the floor for resolving: NameManager persists only names resolved by the user,
        # so after a restart it would otherwise accept an older replayed announce of a kept name
        for name in self.store.names():
            kept = self.local_record(name)
            if kept is not None:
                node.names.offer(name, *kept)
        self.sync_own()

    # Lifecycle

    def start(self):
        """Start the background refresher."""
        self._thread = threading.Thread(target=self._loop, name="rnfs-keepers", daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the refresher and save."""
        self._stop.set()
        self._wake.set()
        self.save()

    def save(self):
        """Save kept records if they changed."""
        self.store.save()

    def status(self) -> dict:
        """Names kept."""
        return {"kept_names": self.store.count()}

    def add_listener(self, listener):
        """Call listener(name) whenever a kept name may have a new record (from a thread of RNS or ours)."""
        self._listeners.append(listener)

    def _notify(self, name: bytes):
        """Tell listeners name may have changed."""
        for listener in list(self._listeners):
            try:
                listener(name)
            except Exception as e:  # noqa: BLE001 - one listener mustn't break the others
                _log(f"listener failed: {e!r}", LOG_ERROR)

    # Keeping

    def is_kept(self, name: bytes) -> bool:
        """True if name is kept for any reason."""
        return bool(self.store.reasons(name))

    def keep(self, name: bytes, reason: str = USER) -> dict:
        """Keep name, and fetch its record in the background if none is held. ValueError if full."""
        if self.store.keep(name, reason):
            self.store.save()
            _log(f"keeping name {name.hex()} ({reason})", LOG_NOTICE)
        if self.store.get(name) is None:
            # NameManager may hold its app_data, but only a keeper's answer (or a cached announce) carries
            # the signature others need to check it
            self.refresh_soon(name)
        return self._entry(name)

    def unkeep(self, name: bytes, reason: str = USER):
        """Stop keeping name for reason (withdrawing it once no reason is left). NotFound if not kept for it."""
        if not self.store.release(name, reason):
            raise NotFound(f"name {name.hex()} is not kept" + ("" if reason == USER else f" for {reason}"))
        self.store.save()
        self.node.publisher.refresh()
        _log(f"no longer keeping name {name.hex()} for {reason}", LOG_NOTICE)

    def kept(self) -> list[dict]:
        """Every kept name: reasons, and the record held if any."""
        return [self._entry(name) for name in self.store.names()]

    def local_record(self, name: bytes) -> tuple[int, Cid, float] | None:
        """(sequence, CID, emission time) of the record kept for name, or None. No network."""
        record = self.store.get(name)
        return None if record is None else (record.sequence, record.cid, float(record.emitted))

    def _entry(self, name: bytes) -> dict:
        """A kept name as the API shows it."""
        record = self.store.get(name)
        return {"name": name.hex(), "reasons": sorted(self.store.reasons(name)),
                "sequence": record.sequence if record else None, "cid": str(record.cid) if record else None,
                "emitted": record.emitted if record else None}

    def _store(self, record: SignedRecord) -> bool:
        """Store a verified record; on a change, re-advertise, tell NameManager and listeners.

        A record below the sequence NameManager already holds is refused: a keeper could otherwise hand this
        node an old, validly signed record to serve until a newer one is heard.
        """
        known = self.node.names.record(record.name)
        if known is not None and record.sequence < known.sequence:
            _log(f"refused record {record.sequence} of name {record.name.hex()}: sequence {known.sequence} is known")
            return False
        first = self.store.get(record.name) is None
        if not self.store.offer(record):
            return False
        _log(f"kept record {record.sequence} of name {record.name.hex()}")
        if first:
            self.node.publisher.refresh()  # its key is provided from now on
        self.node.names.offer(record.name, record.sequence, record.cid, float(record.emitted))
        self._notify(record.name)
        return True

    # Own names

    def published(self, name: bytes, identity, app_data: bytes):
        """NameManager published one of this node's names: sign its record and keep it."""
        try:
            record = sign_record(identity, name, app_data)
        except ValueError:
            return
        self.store.keep(name, OWN)
        self._store(record)

    def sync_own(self):
        """Keep every published local name with a record signed here; release names no longer local."""
        names = self.node.names
        own = {}
        for entry in names.list():
            if entry["cid"] is not None:
                own[bytes.fromhex(entry["name"])] = entry
        for name in self.store.names(OWN):
            if name not in own:
                self.store.release(name, OWN)
                self.node.publisher.refresh()
        for name, entry in own.items():
            record = self.store.get(name)
            if record is not None and record.sequence == entry["sequence"] and str(record.cid) == entry["cid"]:
                continue
            import RNS
            from rnfs.names import encode_record
            try:
                identity = RNS.Identity.from_bytes(names.export_key(entry["alias"]))
            except NotFound:
                continue  # removed meanwhile
            self.store.keep(name, OWN)
            self._store(sign_record(identity, name, encode_record(entry["sequence"], Cid.parse(entry["cid"]))))

    # Heard announces

    def heard(self, name: bytes, sequence: int, cid: Cid, packet_hash: bytes | None):
        """NameManager heard a validly signed announce of name (on an RNS thread): refresh a kept name."""
        if not self.is_kept(name):
            return
        current = self.store.get(name)
        if current is not None and current.sequence >= sequence:
            return
        record = self._from_rns_cache(name, packet_hash)
        if record is not None and record.sequence >= sequence:
            self._store(record)
        else:
            self.refresh_soon(name)
        self._notify(name)

    def _from_rns_cache(self, name: bytes, packet_hash: bytes | None) -> SignedRecord | None:
        """The signed announce with packet_hash from RNS's packet cache, verified, or None.

        RNS internal (Transport.get_cached_packet); with a shared instance the cache is rnsd's, written just
        after it forwards the announce to us, so this retries briefly.
        """
        if packet_hash is None or getattr(self.node, "reticulum", None) is None:
            return None
        try:
            import RNS
            for attempt in range(RNS_CACHE_TRIES):
                packet = RNS.Transport.get_cached_packet(packet_hash, packet_type="announce")
                if packet is not None and packet.unpack() is not False and packet.destination_hash == name \
                        and packet.packet_type == RNS.Packet.ANNOUNCE:
                    return from_announce_data(name, bytes(packet.data), packet.context_flag == RNS.Packet.FLAG_SET)
                if self._stop.wait(RNS_CACHE_WAIT):
                    return None
        except Exception as e:  # noqa: BLE001 - an internal that changed: fall back to asking keepers
            _log(f"reading RNS's announce cache failed: {e!r}")
        return None

    def refresh_soon(self, name: bytes):
        """Ask keepers for name's newest record on the background thread."""
        with self._lock:
            if name in self._pending or len(self._pending) >= MAX_PENDING:
                return
            self._pending[name] = None
        self._wake.set()

    def _loop(self):
        """Background work: pending refreshes, own names, periodic saves."""
        last_retry = time.monotonic()
        while not self._stop.is_set():
            self._wake.wait(LOOP_INTERVAL)
            self._wake.clear()
            try:
                if time.monotonic() - last_retry >= self.node.config.announce_interval:
                    # Kept names still without a record: their keepers may have come online since
                    last_retry = time.monotonic()
                    self.retry_missing()
                self.run_pending()
                self.sync_own()
                self.save()
            except Exception as e:  # noqa: BLE001 - keep the thread alive
                _log(f"background work failed: {e!r}", LOG_ERROR)

    def retry_missing(self) -> list[bytes]:
        """Queue kept names still without a record, round robin from the cursor; the names queued."""
        missing = [name for name in self.store.names() if self.store.get(name) is None]
        if not missing:
            return []
        cursor = self._retry_cursor
        start = 0 if cursor is None else next((i for i, name in enumerate(missing) if name > cursor), 0)
        with self._lock:
            room = MAX_PENDING - len(self._pending)
            queued = [name for name in missing[start:] + missing[:start] if name not in self._pending][:room]
            for name in queued:
                self._pending[name] = None
        if queued:
            self._retry_cursor = queued[-1]
            self._wake.set()
        return queued

    def run_pending(self):
        """Refresh every pending name now (tests call this on offline nodes)."""
        while not self._stop.is_set():
            with self._lock:
                if not self._pending:
                    return
                name = next(iter(self._pending))
                del self._pending[name]
            self.query(name)

    # Resolving through keepers

    def always_ask(self) -> bool:
        """True if keeper_query is "always": ask keepers on every network resolve."""
        return self.node.config.keeper_query == "always"

    def query(self, name: bytes) -> list[tuple[int, Cid, float]]:
        """Ask keepers of name for its record: (sequence, CID, emission time) of each verifying answer.

        The newest is also stored if name is kept here. Called by NameManager.resolve, never under its lock.
        """
        records = self.ask_keepers(name)
        best = None
        for record in records:
            if record.newer_than(best):
                best = record
        if best is not None and self.is_kept(name):
            self._store(best)
        now = time.time()
        return [(record.sequence, record.cid, min(float(record.emitted), now)) for record in records]

    def ask_keepers(self, name: bytes) -> list[SignedRecord]:
        """Find keepers of name in the DHT and ask them with /name, a batch of alpha at a time; the verifying
        records.

        Storing a provider record is free, and so is answering /find, so who vouches for a keeper counts as
        well as how many: standing (RoutingTable.standing) is read from the table as it was before the search
        (a node the lookup meets doesn't gain rank from answering it; one that fails meanwhile loses rank), and
        an *established* node is one senior and healthy then. Keepers recorded here are candidates, never a
        reason to skip the search, which runs until KEEPER_CANDIDATES × alpha are known and two established
        sources listed some (this node counts as one if an established publisher stored a record here), or
        the lookup is complete. Candidates are ranked by standing then at random (rank_keepers), and each batch
        takes first, in that order, those listed by a source no earlier pick was listed by (this node, an
        established listing node, or, as one source, nobody established), then the rest in rank order. Unknown keepers (a path request each) get a budget of
        alpha of their own, apart from the lookup's. Another batch is asked, up to keeper_rounds() in all,
        while no answer reaches the highest sequence already known for the name (from an announce, a resolve
        or a kept record), or fewer than two keepers answered (when no sequence is known, two established
        keepers; on a slow node a second batch is asked for this only when nothing is known to compare
        against). Each verifying record is returned; the caller keeps the highest sequence.
        """
        node = self.node
        key = name_key(name)
        alpha = node.config.effective_alpha(node.slow)
        banned = node.routing.is_banned
        # Taken before the lookup, whose answers add nodes to the routing table
        before = node.routing.standings(node.config.provider_seniority)
        local = [p for p in node.records.providers(key, exclude=banned) if p != node.node_id]
        found = dict.fromkeys(local)
        lookup = node._lookup(key)
        wanted = KEEPER_CANDIDATES * alpha

        def established(node_id: bytes) -> bool:
            """Senior and healthy in the table before the search, and not failed or gone silent since."""
            return self._standing(node_id, before) == 0

        # Records stored here count as a source only if an established publisher stored one, so identities
        # nobody knows can't end the search by storing records for the key here first
        local_source = any(established(p) for p in local)

        def add(provider: bytes):
            """Collect a provider the lookup reports."""
            if provider != node.node_id and provider not in found and not banned(provider):
                found[provider] = None

        def enough() -> bool:
            """Enough candidates, listed by more than one established source."""
            if len(found) < wanted:
                return False
            sources = sum(1 for lister in lookup.listing_nodes() if established(lister))
            return sources + (1 if local_source else 0) >= 2

        lookup.find_providers(add, should_stop=enough)
        candidates = self.rank_keepers(list(found), before)
        if not candidates:
            _log(f"no keepers found for name {name.hex()}")
            return []
        # Who vouches for each candidate: established nodes that listed it, and this node if it holds a record of
        # it; candidates nobody established vouches for count as one source between them
        here = set(local)
        vouched = {provider: ({lister for lister in lookup.listers(provider) if established(lister)}
                              | ({node.node_id} if provider in here else set())) or {_UNVOUCHED}
                   for provider in candidates}
        floor = self._known_sequence(name)
        results: list[SignedRecord] = []
        answered: set[bytes] = set()
        lock = threading.Lock()
        unknown: set[bytes] = set()  # unknown keepers given a path request: at most alpha, apart from the lookup's

        def may_ask(provider: bytes) -> bool:
            """True if provider's identity is known, or the unknown-keeper budget has room."""
            if node._is_known(provider) or provider in unknown:
                return True
            if len(unknown) >= alpha:
                return False
            unknown.add(provider)
            return True

        used: set[bytes] = set()  # sources that listed a keeper already picked

        def next_batch() -> list[bytes]:
            """Up to alpha candidates: first those listed by a source not used yet, then in rank order."""
            batch = []
            for fresh in (True, False):
                for provider in list(candidates):
                    if len(batch) >= alpha:
                        break
                    if fresh and not vouched[provider] - used:
                        continue
                    candidates.remove(provider)
                    if may_ask(provider):
                        batch.append(provider)
                        used.update(vouched[provider])
            return batch

        def ask(provider: bytes):
            """Ask one keeper; keep its record if it verifies."""
            try:
                record = parse_response(name, self.client.name_record(provider, name))
            except Exception as e:  # noqa: BLE001 - a failed request is just no answer
                _log(f"/name to {provider.hex()} failed: {e!r}")
                return
            if record is None:
                return
            with lock:
                results.append(record)
                answered.add(provider)

        deadline = time.monotonic() + max(1.0, lookup.remaining())
        asked = 0
        for _ in range(self.keeper_rounds()):
            batch = next_batch()
            if not batch:
                break
            asked += len(batch)
            threads = [threading.Thread(target=ask, args=(provider,), name="rnfs-keeper-query", daemon=True)
                       for provider in batch]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(max(0.0, deadline - time.monotonic()))
            with lock:
                best = max((record.sequence for record in results), default=None)
                responders = list(answered)
            current = best is not None and (floor is None or best >= floor)
            if floor is None:
                # Nothing to check answers against: replayed old records from identities that answered the
                # lookup must not settle it, so only established keepers confirm each other
                confirmed = sum(1 for provider in responders if established(provider)) >= 2
            else:
                # A second keeper's answer is only worth its airtime on a slow node if there's nothing to check
                # the first against
                confirmed = len(responders) >= 2 or node.slow
            if (current and confirmed) or time.monotonic() >= deadline:
                break
        with lock:
            _log(f"asked {asked} of {asked + len(candidates)} keepers of name {name.hex()}: "
                 f"{len(results)} verifying records")
            return list(results)

    def keeper_rounds(self) -> int:
        """How many batches of alpha keepers one query asks at most: KEEPER_ROUNDS, or 2 on a slow node, where
        alpha is 1 and each /name costs a link and two packets of airtime."""
        return 2 if self.node.slow else KEEPER_ROUNDS

    def rank_keepers(self, candidates: list[bytes], before: dict[bytes, int] | None = None) -> list[bytes]:
        """candidates in the order to ask them: by routing-table standing (RoutingTable.standing: servers known
        for provider_seniority and healthy first, then healthy ones, then those that failed a query or went
        silent, such as an owner that has left, then IDs not in the table), then at random, as the fetcher
        ranks providers by seniority first. Recording a provider record is free, so neither its age nor the
        order replies list it in decides. before: RoutingTable.standings() taken before the search, so nodes
        added to the table since don't rank above where they stood then."""
        ranked = [(self._standing(provider, before), self._rng.random(), provider) for provider in candidates]
        ranked.sort()
        return [provider for _, _, provider in ranked]

    def _standing(self, node_id: bytes, before: dict[bytes, int] | None = None) -> int:
        """node_id's routing-table standing now, or where it stood in before if that was worse."""
        now = self.node.routing.standing(node_id, self.node.config.provider_seniority)
        return now if before is None else max(now, before.get(node_id, 3))

    def _known_sequence(self, name: bytes) -> int | None:
        """The highest sequence this node knows for name (record held by NameManager, or kept), or None."""
        known = []
        record = self.node.names.record(name)
        if record is not None:
            known.append(record.sequence)
        kept = self.store.get(name)
        if kept is not None:
            known.append(kept.sequence)
        return max(known, default=None)

    # Serving

    def requests(self) -> dict:
        """/name: the record of a kept name."""
        return {NAME_PATH: self._serve}

    def _serve(self, data, remote_identity):
        """Answer /name: the record's fields, [] if none is kept, False if malformed."""
        name = parse_request(data)
        if name is None:
            return False
        return encode_response(self.store.get(name))

    # API and CLI

    @classmethod
    def api_routes(cls) -> dict:
        """POST /api/name/keep, /api/name/unkeep; GET /api/name/kept."""
        return {("POST", "/api/name/keep"): _api_keep, ("POST", "/api/name/unkeep"): _api_unkeep,
                ("GET", "/api/name/kept"): _api_kept}

    @classmethod
    def cli_commands(cls, commands, command):
        """`rnfs name keep|unkeep|kept`."""
        name = name_subcommands(commands)
        command(name, "keep", run_keep, "keep and serve a name's signed record, so it outlives its owner"
                ).add_argument("name", help="the name (32 hex characters) or a name contact's nick")
        command(name, "unkeep", run_unkeep, "stop keeping a name").add_argument(
            "name", help="the name (32 hex characters) or a name contact's nick")
        command(name, "kept", run_kept, "list kept names: name, sequence, CID, why kept")


FEATURE = KeepersFeature


def name_subcommands(commands):
    """The subparsers of the CLI's `name` command, so features can add `name <verb>` subcommands."""
    import argparse
    for action in commands.choices["name"]._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise RuntimeError("the CLI has no name subcommands")


def parse_name_arg(node, text) -> bytes:
    """A name from 32 hex characters, or a name contact's nick (rnfs.features.contacts). ValueError."""
    from rnfs.names import parse_name
    contacts = node.features.get("contacts") if hasattr(node, "features") else None
    if contacts is not None:
        return contacts.resolve_id(text, "name")
    return parse_name(text)


def _feature(handler) -> KeepersFeature:
    """The node's keepers feature. NotFound if the daemon has none."""
    return _common.feature(handler, NAME, "this daemon doesn't keep names")


def _api_keep(handler):
    """POST /api/name/keep {"name"}: the kept name."""
    feature = _feature(handler)
    handler.send_json(200, feature.keep(parse_name_arg(handler.node, handler.json_body().get("name"))))


def _api_unkeep(handler):
    """POST /api/name/unkeep {"name"}: {}."""
    feature = _feature(handler)
    feature.unkeep(parse_name_arg(handler.node, handler.json_body().get("name")))
    handler.send_json(200, {})


def _api_kept(handler):
    """GET /api/name/kept: {"kept": [{"name", "reasons", "sequence", "cid", "emitted"}]}."""
    handler.send_json(200, {"kept": _feature(handler).kept()})


def run_keep(args):
    """Keep a name."""
    import rnfs.cli
    entry = rnfs.cli.client(args).post("/api/name/keep", name=args.name)
    detail = f" (sequence {entry['sequence']}, {entry['cid']})" if entry["cid"] else \
        ": no record held yet, asking keepers"
    print(rnfs.cli.printable(f"keeping {entry['name']}{detail}"))


def run_unkeep(args):
    """Stop keeping a name."""
    import rnfs.cli
    rnfs.cli.client(args).post("/api/name/unkeep", name=args.name)


def run_kept(args):
    """List kept names."""
    import rnfs.cli
    for entry in rnfs.cli.client(args).get("/api/name/kept")["kept"]:
        record = f"{entry['sequence']} {entry['cid']}" if entry["cid"] else "- -"
        print(rnfs.cli.printable(f"{entry['name']} {record} {','.join(entry['reasons'])}"))
