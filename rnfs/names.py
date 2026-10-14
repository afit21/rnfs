"""Mutable names, the IPNS equivalent (specification section 9).

A name is the hash of an `rnfs.name` destination whose RNS identity is the name's key. Its record,
`[sequence, binary CID]` in msgpack, rides in the destination's announce app_data, which RNS signs. RNS stores
any validly signed announce, including a replayed old one, so the highest sequence seen is persisted for every
name this node publishes or resolves for the user, and lower sequences are never accepted from any source.
Names resolved for the gateway or LXMF auto-fetch keep their records in a bounded table, where announces of
names this node never resolved can't push them out, and their highest sequence in a bounded persisted floor.

Every RNS call goes through `RnsNetwork`, so tests swap in a fake and run without Reticulum.
"""
import json
import os
import re
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass

import RNS
from RNS.vendor import umsgpack

from rnfs import fsutil
from rnfs.config import Config
from rnfs.constants import APP_NAME, ID_LENGTH, NAME_ASPECT
from rnfs.content.cid import Cid
from rnfs.content.storage import read_json
from rnfs.errors import NotFound, RnfsError
from rnfs.log import LOG_ERROR, LOG_NOTICE, LOG_VERBOSE, LOG_WARNING, log

NAME_LENGTH = ID_LENGTH  # a name is a destination hash: 16 bytes
KEY_LENGTH = RNS.Identity.KEYSIZE // 8  # private key bytes: X25519 + Ed25519, 64 bytes
MAX_SEQUENCE = 2**64 - 1  # largest msgpack unsigned integer
# A record is at most ~52 bytes (array header, uint64, bin header, 39-byte CID); anything longer is
# rejected before decoding, which keeps the announce handler cheap under a flood
MAX_RECORD_SIZE = 64
STATE_FILE = "names.json"
STATE_VERSION = 1
FLOOR_FILE = "name-floor.json"
FLOOR_VERSION = 1
FLOOR_FACTOR = 4  # the floor holds up to this many times name_table_size names, so it outlasts table churn
PIN_SOURCE_PREFIX = "name:"
_ALIAS = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_POLL_INTERVAL = 0.25  # how often a refresh checks whether RNS has heard a path response
_MAX_LOOP_WAIT = 60  # the announce loop wakes at least this often, so new schedules are picked up


# Records and names


def encode_record(sequence: int, cid: Cid) -> bytes:
    """A name record as announce app_data: msgpack [sequence, binary CID]."""
    return umsgpack.packb([sequence, cid.binary])


def parse_record(app_data) -> tuple[int, Cid] | None:
    """(sequence, CID) from heard announce app_data, or None unless it is exactly a canonical record."""
    if not isinstance(app_data, (bytes, bytearray)) or len(app_data) > MAX_RECORD_SIZE:
        return None
    app_data = bytes(app_data)
    try:
        value = umsgpack.unpackb(app_data)
    except Exception:
        return None
    if not isinstance(value, list) or len(value) != 2:
        return None
    sequence, binary = value
    # bool is a subclass of int, and msgpack true/false must not pass as a sequence
    if type(sequence) is not int or not 0 <= sequence <= MAX_SEQUENCE:
        return None
    if not isinstance(binary, bytes):
        return None
    try:
        cid = Cid.from_bytes(binary)
    except ValueError:
        return None
    # umsgpack ignores trailing bytes and accepts non-shortest encodings; re-encoding rejects both, so a
    # record has one encoding, as DAG nodes do
    if encode_record(sequence, cid) != app_data:
        return None
    return sequence, cid


def parse_name(text: str) -> bytes:
    """A name from its 32 hex character form, with or without RNS's <angle brackets>. Raises ValueError."""
    if not isinstance(text, str):
        raise ValueError("name must be a string")
    stripped = text.strip().strip("<>")
    if len(stripped) != NAME_LENGTH * 2:
        raise ValueError(f"name must be {NAME_LENGTH * 2} hex characters: {text!r}")
    try:
        return bytes.fromhex(stripped)
    except ValueError:
        raise ValueError(f"name must be {NAME_LENGTH * 2} hex characters: {text!r}") from None


def check_alias(alias) -> str:
    """alias if it is safe as a file name under names_dir; raises ValueError otherwise."""
    if not isinstance(alias, str) or not _ALIAS.fullmatch(alias):
        raise ValueError(f"invalid alias {alias!r}: use 1-64 letters, digits, '.', '_' or '-', "
                         "starting with a letter or digit")
    # On Windows "con" or "nul.txt" would open a device, and "site." would be the file "site"
    reason = fsutil.windows_name_reason(alias) if fsutil.IS_WINDOWS else None
    if reason is not None:
        raise ValueError(f"invalid alias {alias!r}: {reason}")
    return alias


def name_for_identity(identity: RNS.Identity) -> bytes:
    """The name a key holds: the hash of its rnfs.name destination. Needs no running Reticulum."""
    return RNS.Destination.hash(identity, APP_NAME, NAME_ASPECT)


def _check_name(name) -> bytes:
    """name if it is NAME_LENGTH bytes; raises ValueError otherwise."""
    if not isinstance(name, (bytes, bytearray)) or len(name) != NAME_LENGTH:
        raise ValueError(f"a name is {NAME_LENGTH} bytes")
    return bytes(name)


@dataclass(frozen=True)
class Record:
    """A name record held by this node, and when (wall clock) it was last heard."""

    sequence: int
    cid: Cid
    heard: float


@dataclass(frozen=True)
class _Published:
    """What this node announces for one of its own names. cid is None until the first publish."""

    sequence: int
    cid: Cid | None
    # A key imported from elsewhere (or found with no state: names.json was lost) may already have records on
    # the network and at keepers, so its first publish asks both before choosing a sequence
    imported: bool = False


# RNS access


class RnsNetwork:
    """The RNS calls names make. Tests replace it with a fake; this one needs a running Reticulum."""

    def name_destination(self, identity: RNS.Identity) -> RNS.Destination:
        """Register the IN/SINGLE rnfs.name destination of identity (raises KeyError if already registered)."""
        destination = RNS.Destination(identity, RNS.Destination.IN, RNS.Destination.SINGLE, APP_NAME, NAME_ASPECT)
        # Nothing talks to a name destination; it only announces
        destination.accepts_links(False)
        return destination

    def close_destination(self, destination: RNS.Destination):
        """Stop RNS answering for destination (path requests, packets)."""
        RNS.Transport.deregister_destination(destination)

    def register_announce_handler(self, handler):
        """Have RNS call handler.received_announce for matching announces."""
        RNS.Transport.register_announce_handler(handler)

    def deregister_announce_handler(self, handler):
        """Undo register_announce_handler."""
        RNS.Transport.deregister_announce_handler(handler)

    def request_path(self, name: bytes):
        """Ask the network for name's announce; answers arrive as path responses."""
        RNS.Transport.request_path(name)

    def recall_app_data(self, name: bytes) -> bytes | None:
        """The app_data of the last validly signed announce RNS heard for name."""
        return RNS.Identity.recall_app_data(name)

    def heard_time(self, name: bytes) -> float | None:
        """When RNS last stored a validly signed announce for name (wall clock), or None.

        RNS internal (Identity.known_destinations[hash][0], set by Identity.remember for every valid announce
        and path response, even ones that don't change the path table and so never reach announce handlers).
        If it changes in another RNS version this returns None and callers fall back to their own times.
        """
        try:
            entry = RNS.Identity.known_destinations.get(name)
            return float(entry[0]) if entry else None
        except Exception:
            return None


class NameAnnounceHandler:
    """RNS announce handler for every rnfs.name announce, live or in answer to a path request."""

    def __init__(self, on_announce):
        """Handler passing (name, app_data) to on_announce."""
        # RNS only calls us for announces whose destination hash matches this full name
        self.aspect_filter = f"{APP_NAME}.{NAME_ASPECT}"
        # Resolving relies on path responses: without this RNS drops them before handlers
        self.receive_path_responses = True
        self._on_announce = on_announce

    def received_announce(self, destination_hash, announced_identity, app_data, announce_packet_hash=None,
                          is_path_response=False):
        """RNS calls this on its own new thread per announce; the work done is bounded and cheap.

        Five parameters, so RNS also passes the announce's packet hash: keepers use it to find the signed
        announce in RNS's packet cache (rnfs.features.keepers).
        """
        self._on_announce(destination_hash, app_data, announce_packet_hash)


# Names


class NameManager:
    """This node's name keys and records, and the records it has heard. Thread-safe.

    Lifecycle: construct (loads keys and state, no RNS needed), release_gone() once the node and its features are
    set up, start() once Reticulum runs, run(stop_event) on a thread for re-announces, stop() at shutdown.
    """

    def __init__(self, config: Config, pins, network=None, clock=time.time, monotonic=time.monotonic):
        """Names under config.names_dir and config.state_dir; pins has pin(cid, source) and unpin(cid, source)."""
        self.config = config
        self._pins = pins
        self._network = network if network is not None else RnsNetwork()
        self._clock = clock  # wall clock: heard times are persisted across restarts
        self._monotonic = monotonic  # announce schedule
        self._lock = threading.RLock()
        self._state_path = os.path.join(config.state_dir, STATE_FILE)

        self._keys: dict[str, RNS.Identity] = {}
        self._names: dict[str, bytes] = {}  # alias -> name
        self._aliases: dict[bytes, str] = {}  # name -> alias
        self._published: dict[str, _Published] = {}
        # Highest records of names published here or resolved through the CLI or API: persisted, unbounded
        # (they only grow by the user's own actions)
        self._held: dict[bytes, Record] = {}
        # Every other name's record: in memory, least recently used first, at most name_table_size
        self._table: OrderedDict[bytes, Record] = OrderedDict()
        # The table's names that were only heard, never resolved here, least recently used first. An announce of a
        # name this node isn't resolving and doesn't know may only take the place of one of these, so a flood of
        # new names can't push out the names it resolved
        self._heard_only: OrderedDict[bytes, None] = OrderedDict()
        # Highest sequence of each name resolved without persist (gateway, LXMF auto-fetch), least recently resolved
        # first, at most FLOOR_FACTOR × name_table_size: saved, so such a resolve never goes back after table churn
        # or a restart
        self._floor: OrderedDict[bytes, int] = OrderedDict()
        self._floor_path = os.path.join(config.state_dir, FLOOR_FILE)
        self._floor_dirty = False
        self._held_dirty = False  # heard times changed without a save

        self._destinations: dict[str, object] = {}
        self._next_announce: dict[str, float] = {}  # alias -> monotonic time
        self._waiters: dict[bytes, list[threading.Event]] = {}
        self._handler = NameAnnounceHandler(self._heard)
        self._started = False
        # The keepers feature (rnfs.features.keepers), if loaded: told of heard and published records, and asked
        # for signed records when the network has none. Never called with self._lock held
        self.keepers = None
        # Pins of names whose key file was deleted by hand, released by release_gone() once the node is set up
        self._gone: list[tuple[str, Cid]] = []

        self._remove_old_temps()
        self._load_keys()
        self._load_state()
        self._load_floor()

    # Keys

    def create(self, alias: str) -> bytes:
        """Make a new name key under alias and return its name. ValueError if alias is invalid or taken."""
        check_alias(alias)
        with self._lock:
            self._check_free(alias)
            identity = RNS.Identity()
            self._add_key(alias, identity, _Published(0, None))
        log(f"created name {self._names[alias].hex()} as {alias}", LOG_NOTICE)
        return self._names[alias]

    def import_key(self, alias: str, data: bytes) -> bytes:
        """Store a key exported by export_key under alias and return its name. ValueError if invalid or taken."""
        check_alias(alias)
        if not isinstance(data, (bytes, bytearray)) or len(data) != KEY_LENGTH:
            raise ValueError(f"a name key is {KEY_LENGTH} bytes")
        identity = RNS.Identity.from_bytes(bytes(data))
        if identity is None:
            raise ValueError("invalid name key")
        name = name_for_identity(identity)
        with self._lock:
            self._check_free(alias)
            if name in self._aliases:
                raise ValueError(f"this key is already held as {self._aliases[name]}")
            self._add_key(alias, identity, _Published(0, None, imported=True))
        log(f"imported name {name.hex()} as {alias}", LOG_NOTICE)
        return name

    def export_key(self, alias: str) -> bytes:
        """The private key of alias's name, for import_key on another machine. NotFound if no such alias."""
        with self._lock:
            return self._identity(alias).get_private_key()

    def remove(self, alias: str):
        """Delete alias's key, stop announcing it and release its pin. Its highest sequence stays persisted."""
        with self._lock:
            self._identity(alias)
            published = self._published.pop(alias, None)
            if published is not None and published.cid is not None:
                self._pins.unpin(published.cid, PIN_SOURCE_PREFIX + alias)
            self._next_announce.pop(alias, None)
            destination = self._destinations.pop(alias, None)
            if destination is not None:
                self._network.close_destination(destination)
            name = self._names.pop(alias)
            del self._aliases[name]
            del self._keys[alias]
            # State first: a crash before the key file is gone leaves an unpublished name, not a lost pin
            self._save()
            key_path = self._key_path(alias)
            if not fsutil.remove(key_path):
                # Windows, with something holding the key open: the name would come back at the next start
                log(f"couldn't delete {key_path}, which is in use; delete it, or {alias} returns at the next start",
                    LOG_WARNING)
        log(f"removed name {name.hex()} ({alias})", LOG_NOTICE)

    def release_gone(self):
        """Release the pins of names whose key file was deleted by hand (found by the constructor).

        Not done while loading: releasing a pin recomputes what the node provides, which needs the rest of the
        node, and the features' keys (kept names), set up first.
        """
        with self._lock:
            gone, self._gone = self._gone, []
            for alias, cid in gone:
                log(f"name key {alias} is gone; releasing its pin", LOG_WARNING)
                self._pins.unpin(cid, PIN_SOURCE_PREFIX + alias)
            if gone:
                self._save()  # its entry is dropped only once its pin is released

    def name(self, alias: str) -> bytes:
        """alias's name. NotFound if no such alias."""
        with self._lock:
            self._identity(alias)
            return self._names[alias]

    def list(self) -> list[dict]:
        """[{"alias", "name", "cid" or None, "sequence"}] for every local name, by alias."""
        with self._lock:
            result = []
            for alias in sorted(self._keys):
                published = self._published.get(alias, _Published(0, None))
                result.append({"alias": alias, "name": self._names[alias].hex(),
                               "cid": str(published.cid) if published.cid is not None else None,
                               "sequence": published.sequence})
            return result

    # Publishing

    def publish(self, alias: str, cid: Cid) -> dict:
        """Point alias's name at cid, pin it, announce it. Returns {"name", "cid", "sequence"}."""
        if not isinstance(cid, Cid):
            raise ValueError("publish needs a CID")
        with self._lock:
            name = self.name(alias)
            refresh = self._published[alias].imported and self._published[alias].cid is None
        if refresh:
            # A freshly imported key: its previous home may have published sequences we've never heard, which
            # keepers may hold after its announces have expired (section 9: above the highest it can resolve)
            self._offer_kept(name)
            self._refresh(name)
            if self.keepers is not None:
                for sequence, kept_cid, heard in self.keepers.query(name):
                    self.offer(name, sequence, kept_cid, heard)

        with self._lock:
            self._identity(alias)  # removed while refreshing?
            self._absorb_recalled(name)
            previous = self._published[alias]
            known = self._record(name, touch=False)
            sequence = 1 + max(previous.sequence, known.sequence if known is not None else 0)
            if sequence > MAX_SEQUENCE:
                raise RnfsError(f"name {alias} has reached the highest sequence")

            source = PIN_SOURCE_PREFIX + alias
            # Pin the new target before releasing the old one, so blocks they share are never unprotected
            self._pins.pin(cid, source)
            if previous.cid is not None and previous.cid != cid:
                self._pins.unpin(previous.cid, source)

            self._published[alias] = _Published(sequence, cid)
            self._drop_from_table(name)
            self._held[name] = Record(sequence, cid, self._clock())
            self._save()
            self._announce(alias)
            identity = self._keys[alias]
        log(f"published {name.hex()} ({alias}) -> {cid}, sequence {sequence}", LOG_NOTICE)
        if self.keepers is not None:
            # This node keeps its own names, so they resolve through it while the owner's announce is gone
            self.keepers.published(name, identity, encode_record(sequence, cid))
        return {"name": name.hex(), "cid": str(cid), "sequence": sequence}

    def start(self):
        """Register the announce handler, re-create every local name's destination and announce published ones."""
        with self._lock:
            if self._started:
                return
            self._started = True
            self._network.register_announce_handler(self._handler)
            for alias in sorted(self._keys):
                self._open_destination(alias)
            # Announced again after startup_announce_delay, as rnfs.dht is, since interfaces may not have been up
            first = self._monotonic() + self.config.startup_announce_delay
            for alias in sorted(self._destinations):
                if self._announce(alias):
                    self._next_announce[alias] = first

    def stop(self):
        """Deregister from RNS, wake waiting resolves and save state."""
        with self._lock:
            if self._started:
                self._started = False
                self._network.deregister_announce_handler(self._handler)
                for destination in self._destinations.values():
                    self._network.close_destination(destination)
                self._destinations.clear()
                self._next_announce.clear()
            for events in self._waiters.values():
                for event in events:
                    event.set()
            self._save()
            self._save_floor()

    def announce_due(self) -> float:
        """Re-announce every name whose announce_interval has passed; seconds until the next is due."""
        with self._lock:
            now = self._monotonic()
            for alias, due in sorted(self._next_announce.items()):
                if due <= now:
                    self._announce(alias)
            if self._held_dirty:
                self._save()
            if self._floor_dirty:
                self._save_floor()
            if not self._next_announce:
                return self.config.announce_interval
            return max(0.0, min(self._next_announce.values()) - now)

    def run(self, stop_event: threading.Event):
        """The re-announce loop, for a thread of its own; returns once stop_event is set."""
        while not stop_event.is_set():
            try:
                delay = self.announce_due()
            except Exception as e:
                log(f"name re-announce failed: {e}", LOG_ERROR)
                delay = _MAX_LOOP_WAIT
            stop_event.wait(min(delay, _MAX_LOOP_WAIT))

    # Resolving

    def resolve(self, name: bytes, persist: bool, max_age: float = 0.0) -> dict | None:
        """The highest record held for name after refreshing it: {"name", "cid", "sequence", "age"}, or None.

        persist is True for the CLI and API (the highest sequence is then kept forever) and False for the gateway
        and LXMF auto-fetch (the record goes only into the bounded table, and its sequence into the bounded floor).
        The network refresh, which can take path_timeout, is skipped for this node's own names and when the held
        record was heard less than max_age seconds ago.
        """
        name = _check_name(name)
        self._offer_kept(name)
        with self._lock:
            held = self._record(name, touch=False)
            # A heard time in the future (the clock was corrected since) isn't fresh
            fresh = held is not None and max_age > 0 and 0 <= self._clock() - held.heard < max_age
            alias = self._aliases.get(name)
            # This node's own names need no refresh, unless imported and not yet published here
            local = alias is not None and not (self._published[alias].imported and self._published[alias].cid is None)
        if not local and not fresh:
            started = self._clock()
            self._refresh(name)
            if self.keepers is not None:
                with self._lock:
                    self._absorb_recalled(name)
                    record = self._record(name, touch=False)
                # Nothing heard during the refresh: the owner and the paths may be gone, so ask keepers
                if record is None or record.heard < started or self.keepers.always_ask():
                    for sequence, cid, heard in self.keepers.query(name):
                        self.offer(name, sequence, cid, heard)

        # Again: the bounded table may have evicted it meanwhile, letting a replayed announce in
        self._offer_kept(name)
        with self._lock:
            self._absorb_recalled(name)
            record = self._record(name, touch=True)
            if record is None:
                return None
            if persist and name not in self._held:
                self._drop_from_table(name)
                self._held[name] = record
                self._save()
            elif name not in self._held:
                self._resolved(name, record.sequence)
            return {"name": name.hex(), "cid": str(record.cid), "sequence": record.sequence,
                    "age": max(0.0, self._clock() - record.heard)}

    def _offer_kept(self, name: bytes):
        """Offer the signed record keepers hold for name, so it is a floor no replayed announce gets under.

        Keepers persist the newest record of every kept name, but this node only persists names resolved
        through the CLI or API; without this, a restart or table eviction would let an older one back in.
        """
        if self.keepers is None:
            return
        kept = self.keepers.local_record(name)
        if kept is not None:
            self.offer(name, *kept)

    def _refresh(self, name: bytes):
        """Request a path to name and wait, up to path_timeout, for an answer to reach RNS."""
        event = threading.Event()
        with self._lock:
            self._waiters.setdefault(name, []).append(event)
        started = self._clock()
        try:
            self._network.request_path(name)
            # Real time, not the injected clock: this is an actual wait
            deadline = time.monotonic() + self.config.path_timeout
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or event.wait(min(_POLL_INTERVAL, remaining)):
                    break
                # A path response repeating an announce RNS already has doesn't change the path table, so it
                # never reaches the handler; RNS still records when it heard it
                heard = self._network.heard_time(name)
                if heard is not None and heard >= started:
                    break
        except Exception as e:
            log(f"path request for name {name.hex()} failed: {e}", LOG_WARNING)
        finally:
            with self._lock:
                events = self._waiters.get(name, [])
                if event in events:
                    events.remove(event)
                if not events:
                    self._waiters.pop(name, None)

    def _absorb_recalled(self, name: bytes):
        """Offer the record RNS last stored for name (which may be a replay) to the sequence rules."""
        try:
            app_data = self._network.recall_app_data(name)
        except Exception as e:
            log(f"could not recall name {name.hex()}: {e}", LOG_WARNING)
            return
        parsed = parse_record(app_data)
        if parsed is None:
            return
        sequence, cid = parsed
        heard = self._network.heard_time(name)
        if heard is None:
            # Without RNS's time, the record we hold is as old as we last heard it; a new one is assumed just heard
            held = self._record(name, touch=False)
            same = held is not None and held.sequence == sequence and held.cid == cid
            heard = held.heard if same else self._clock()
        self._offer(name, sequence, cid, heard)

    def _heard(self, name: bytes, app_data, packet_hash: bytes | None = None):
        """Announce handler: apply a heard record and wake resolves waiting for it."""
        parsed = parse_record(app_data)
        # A name kept here may take a table place like a resolved one (asked outside our lock, as always)
        kept = parsed is not None and self.keepers is not None and self.keepers.local_record(bytes(name)) is not None
        with self._lock:
            if parsed is not None:
                # Unasked: not being resolved (a path response would have a waiter) nor kept
                unasked = not kept and name not in self._waiters
                self._offer(bytes(name), parsed[0], parsed[1], self._clock(), unasked=unasked)
            else:
                log(f"ignored malformed record for name {RNS.prettyhexrep(name)}", LOG_VERBOSE)
            for event in self._waiters.get(name, ()):
                event.set()
        if parsed is not None and self.keepers is not None:
            self.keepers.heard(bytes(name), parsed[0], parsed[1], packet_hash)

    def offer(self, name: bytes, sequence: int, cid: Cid, heard: float) -> bool:
        """Apply a record whose signature the caller verified (a keeper's answer), under the sequence rules.

        True if it replaced the record held. Wakes resolves waiting for name.
        """
        name = _check_name(name)
        with self._lock:
            replaced = self._offer(name, sequence, cid, min(heard, self._clock()))
            for event in self._waiters.get(name, ()):
                event.set()
        return replaced

    def record(self, name: bytes) -> Record | None:
        """The record held for name, without any network refresh (None if none)."""
        with self._lock:
            return self._record(_check_name(name), touch=False)

    # Sequence rules; callers hold the lock

    def _record(self, name: bytes, touch: bool) -> Record | None:
        """The record held for name, if any; touch marks it recently used in the table."""
        record = self._held.get(name)
        if record is not None:
            return record
        record = self._table.get(name)
        if record is not None and touch:
            self._table.move_to_end(name)
            if name in self._heard_only:
                self._heard_only.move_to_end(name)
        return record

    def _offer(self, name: bytes, sequence: int, cid: Cid, heard: float, unasked: bool = False) -> bool:
        """Apply a validly signed record: True if it replaced the one held. Lower sequences (than the record held,
        or the floor of a name resolved before) are refused, and at an equal sequence the record already held is
        kept. unasked: an announce of a name this node isn't resolving or keeping, which takes a table place only
        if it is free or held by another name that was only heard."""
        held_here = name in self._held
        current = self._record(name, touch=False)
        floor = None if held_here else self._floor.get(name)
        if floor is not None and sequence < floor:
            log(f"refused record {sequence} for name {name.hex()}: already resolved {floor}", LOG_VERBOSE)
            return False
        if current is not None:
            if sequence < current.sequence:
                log(f"refused record {sequence} for name {name.hex()}: already hold {current.sequence}",
                    LOG_VERBOSE)
                return False
            if sequence == current.sequence:
                if cid == current.cid and heard > current.heard:
                    # The same record heard again: it is that much younger
                    self._store(name, Record(sequence, cid, heard), held_here)
                    self._held_dirty |= held_here
                elif not held_here:
                    self._touch(name)
                return False

        if not self._store(name, Record(sequence, cid, heard), held_here, unasked):
            return False
        if held_here:
            self._save()
        return True

    def _store(self, name: bytes, record: Record, held_here: bool, unasked: bool = False) -> bool:
        """Put record in the persisted map or the bounded table, making room in the table. False if an unasked
        record found no room (every place is a name resolved or kept here)."""
        if held_here:
            self._held[name] = record
            return True
        limit = max(0, self.config.name_table_size)
        if name not in self._table:
            # Names only heard make room first; one resolved or kept here only for another such name
            while len(self._table) >= limit and self._heard_only:
                evicted, _ = self._heard_only.popitem(last=False)
                del self._table[evicted]
            if len(self._table) >= limit and unasked:
                return False
            while self._table and len(self._table) >= limit:
                self._table.popitem(last=False)
            if unasked:
                self._heard_only[name] = None
        self._table[name] = record
        self._touch(name)
        floor = self._floor.get(name)
        if floor is not None and record.sequence > floor:
            self._floor[name] = record.sequence
            self._floor_dirty = True
        if len(self._table) > limit:  # name_table_size 0: nothing is kept
            self._drop_from_table(name)
        return True

    def _touch(self, name: bytes):
        """Mark a table entry recently used."""
        self._table.move_to_end(name)
        if name in self._heard_only:
            self._heard_only.move_to_end(name)

    def _drop_from_table(self, name: bytes):
        """Remove name from the table (it is held now)."""
        self._table.pop(name, None)
        self._heard_only.pop(name, None)

    def _resolved(self, name: bytes, sequence: int):
        """A resolve without persist returned name's record: it is no longer only heard, and its sequence is the
        floor for later resolves (least recently resolved forgotten first)."""
        self._heard_only.pop(name, None)
        previous = self._floor.pop(name, None)
        self._floor[name] = sequence if previous is None else max(previous, sequence)
        if self._floor[name] != previous:
            self._floor_dirty = True
        while len(self._floor) > self._floor_size():
            self._floor.popitem(last=False)
            self._floor_dirty = True

    def _floor_size(self) -> int:
        """How many names the floor holds at most."""
        return FLOOR_FACTOR * max(0, self.config.name_table_size)

    # RNS destinations; callers hold the lock

    def _open_destination(self, alias: str):
        """Register alias's destination with its record as default app_data (so path responses carry it)."""
        try:
            destination = self._network.name_destination(self._keys[alias])
        except Exception as e:
            log(f"could not open name {alias}: {e}", LOG_ERROR)
            return
        self._destinations[alias] = destination
        published = self._published.get(alias)
        if published is not None and published.cid is not None:
            destination.set_default_app_data(encode_record(published.sequence, published.cid))

    def _announce(self, alias: str) -> bool:
        """Announce alias's record now and schedule the next one; False if it has no destination or record."""
        destination = self._destinations.get(alias)
        published = self._published.get(alias)
        if destination is None or published is None or published.cid is None:
            self._next_announce.pop(alias, None)
            return False
        self._next_announce[alias] = self._monotonic() + self.config.announce_interval
        try:
            destination.set_default_app_data(encode_record(published.sequence, published.cid))
            destination.announce()
        except Exception as e:
            log(f"could not announce name {alias}: {e}", LOG_ERROR)
            return False
        log(f"announced name {self._names[alias].hex()} ({alias}), sequence {published.sequence}", LOG_VERBOSE)
        return True

    # Key files; callers hold the lock

    def _key_path(self, alias: str) -> str:
        """Where alias's key lives."""
        return os.path.join(self.config.names_dir, alias)

    def _identity(self, alias: str) -> RNS.Identity:
        """alias's key; NotFound if no such alias."""
        check_alias(alias)
        identity = self._keys.get(alias)
        if identity is None:
            raise NotFound(f"no name with alias {alias}")
        return identity

    def _check_free(self, alias: str):
        """ValueError if alias is in use (in memory or on disk)."""
        if alias in self._keys or os.path.exists(self._key_path(alias)):
            raise ValueError(f"alias {alias} already exists")

    def _add_key(self, alias: str, identity: RNS.Identity, published: _Published):
        """Save identity as alias's key, track it, and open its destination if running."""
        os.makedirs(self.config.names_dir, mode=0o700, exist_ok=True)
        # Written atomically and private: RNS's Identity.to_file does neither. The temp name starts with "."
        # (fsutil.PARTIAL_PREFIX) so it can never be an alias
        fsutil.write_atomic(self._key_path(alias), identity.get_private_key(), mode=0o600)
        name = name_for_identity(identity)
        self._keys[alias] = identity
        self._names[alias] = name
        self._aliases[name] = alias
        self._published[alias] = published
        self._save()
        if self._started:
            self._open_destination(alias)

    def _remove_old_temps(self):
        """Delete the temp files an interrupted write of rnfs 0.1.1 or earlier left (names.json.tmp and
        .<alias>.tmp): it reused those names, but writes now take new ones each time."""
        try:
            entries = os.listdir(self.config.names_dir)
        except OSError:
            entries = []
        old = [os.path.join(self.config.names_dir, entry) for entry in entries
               if entry.startswith(".") and entry.endswith(".tmp") and _ALIAS.fullmatch(entry[1:-4])]
        for path in (self._state_path + ".tmp", *old):
            try:
                fsutil.remove(path)
            except OSError:
                pass

    def _load_keys(self):
        """Read every key file in names_dir."""
        try:
            entries = sorted(os.listdir(self.config.names_dir))
        except FileNotFoundError:
            return
        for alias in entries:
            path = self._key_path(alias)
            if not _ALIAS.fullmatch(alias) or not os.path.isfile(path):
                continue
            identity = RNS.Identity.from_file(path)
            if identity is None:
                log(f"could not read name key {path}; skipping it", LOG_ERROR)
                continue
            name = name_for_identity(identity)
            if name in self._aliases:
                log(f"name key {alias} is the same key as {self._aliases[name]}; skipping it", LOG_WARNING)
                continue
            self._keys[alias] = identity
            self._names[alias] = name
            self._aliases[name] = alias

    # State; callers hold the lock

    def _load_state(self):
        """Read persisted records. A corrupt file is an error: starting afresh would allow rollback."""
        try:
            with open(self._state_path, "rb") as f:
                state = json.load(f)
        except FileNotFoundError:
            state = {}
        except (OSError, ValueError) as e:
            raise RnfsError(f"could not read {self._state_path}: {e}") from None
        try:
            for alias, entry in state.get("published", {}).items():
                cid = Cid.parse(entry["cid"]) if entry["cid"] is not None else None
                published = _Published(int(entry["sequence"]), cid, bool(entry.get("imported", False)))
                if alias in self._keys:
                    self._published[alias] = published
                elif cid is not None:
                    # Its key file was deleted by hand: nothing will republish it, so release its pin
                    self._gone.append((alias, cid))
            for name_hex, entry in state.get("held", {}).items():
                self._held[parse_name(name_hex)] = Record(int(entry["sequence"]), Cid.parse(entry["cid"]),
                                                          float(entry["heard"]))
        except (AttributeError, KeyError, TypeError, ValueError) as e:
            raise RnfsError(f"malformed {self._state_path}: {e}") from None
        for alias in self._keys:
            # A key with no state entry (names.json lost, or the key copied in by hand) may have been published
            # elsewhere: treated as imported, so its first publish looks for its records first
            self._published.setdefault(alias, _Published(0, None, imported=True))

    def _load_floor(self):
        """Read the floor of names resolved without persist. A damaged file is moved aside and the node starts
        without it (it only bounds rollback of gateway and auto-fetch resolves); entries that don't parse are
        skipped."""
        state = read_json(self._floor_path, shape=lambda value: isinstance(value, dict)
                          and value.get("version") == FLOOR_VERSION and isinstance(value.get("floor"), list))
        if state is None:
            return
        for entry in state["floor"]:
            try:
                name_hex, sequence = entry
                name = parse_name(name_hex)
            except (TypeError, ValueError):
                continue
            if type(sequence) is int and 0 <= sequence <= MAX_SEQUENCE and name not in self._held:
                self._floor.pop(name, None)
                self._floor[name] = sequence
        while len(self._floor) > self._floor_size():
            self._floor.popitem(last=False)

    def _save_floor(self):
        """Write the floor atomically, least recently resolved first, if it changed. Saved by announce_due (at
        least every minute while running) and stop(), not on every resolve, since it holds thousands of names."""
        if not self._floor_dirty:
            return
        state = {"version": FLOOR_VERSION, "floor": [[name.hex(), sequence] for name, sequence in self._floor.items()]}
        os.makedirs(self.config.state_dir, exist_ok=True)
        try:
            fsutil.write_atomic(self._floor_path, json.dumps(state, separators=(",", ":")).encode())
        except OSError as e:
            log(f"could not save {self._floor_path}: {e}", LOG_WARNING)
            return
        self._floor_dirty = False

    def _save(self):
        """Write published and held records atomically."""
        state = {
            "version": STATE_VERSION,
            "published": {alias: {"sequence": p.sequence, "cid": str(p.cid) if p.cid is not None else None,
                                  "imported": p.imported}
                          for alias, p in sorted(self._published.items())},
            "held": {name.hex(): {"sequence": r.sequence, "cid": str(r.cid), "heard": r.heard}
                     for name, r in sorted(self._held.items())},
        }
        os.makedirs(self.config.state_dir, exist_ok=True)
        fsutil.write_atomic(self._state_path, json.dumps(state, indent=1).encode())
        self._held_dirty = False
