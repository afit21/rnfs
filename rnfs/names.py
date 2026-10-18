"""Mutable names, the IPNS equivalent (specification section 9).

A name is the hash of an `rnfs.name` destination whose RNS identity is the name's key. Its record,
`[sequence, binary CID]` in msgpack, rides in the destination's announce app_data, which RNS signs. RNS stores
any validly signed announce, including a replayed old one, so the highest sequence seen is persisted for every
name this node publishes or resolves for the user, and lower sequences are never accepted from any source. RNS
also remembers a replayed announce as just heard, so an announce shows its record current only up to
ANNOUNCE_VALIDITY announce intervals after it was signed (_announce_heard): past that, resolves ask keepers. One
older than the announce RNS took the path from, or carrying another record, is a suspected replay, as old as it is
(RnsNetwork.recalled_emission), and resolves ask keepers at once.
Names resolved for the gateway or LXMF auto-fetch keep their records in a bounded table, where announces of
names this node never resolved can't push them out, and their highest sequence in a bounded persisted floor, saved
within seconds of a resolve or an announce raising it, where names resolved in the last FLOOR_KEEP_AGE are forgotten
only past a hard bound.

State lives in the state database (rnfs.statedb), read only at startup. The records of names published here and
of names resolved for the user (names_published, names_held) are saved at once and durably, each save writing only
the rows that changed; that save is a publish's commit point. A malformed one stops startup: starting without it
would allow rollback. Floor saves (name_floor) write only the entries changed too, with synchronous=NORMAL, so a
power loss may lose the last of them; a malformed floor entry is dropped. The names.json and name-floor.json of
rnfs 0.1.4 and earlier are imported once (import_names, import_floor).

Watching (rnfs.Node's names.watch, GET /api/name/watch): a watched name's announces count as asked for, so a full
table never refuses them, and record listeners are told, with the lock held, of each newer record accepted for one
(an announce, a path response, a keeper's answer, a publish here); they only queue it.

Every RNS call goes through `RnsNetwork`, so tests swap in a fake and run without Reticulum.
"""
import json
import math
import os
import re
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass

import RNS
from RNS.vendor import umsgpack

from rnfs import fsutil, ids, statedb
from rnfs.config import Config
from rnfs.constants import APP_NAME, ID_LENGTH, MISSED_ANNOUNCES, NAME_ASPECT
from rnfs.content.cid import Cid
from rnfs.errors import NotFound, RnfsError
from rnfs.log import LOG_ERROR, LOG_NOTICE, LOG_VERBOSE, LOG_WARNING, log

NAME_LENGTH = ID_LENGTH  # a name is a destination hash: 16 bytes
KEY_LENGTH = RNS.Identity.KEYSIZE // 8  # private key bytes: X25519 + Ed25519, 64 bytes
MAX_SEQUENCE = 2**64 - 1  # largest msgpack unsigned integer
# A record is at most ~52 bytes (array header, uint64, bin header, 39-byte CID); anything longer is
# rejected before decoding, which keeps the announce handler cheap under a flood
MAX_RECORD_SIZE = 64
STATE_FILE = "names.json"  # the published and held records of rnfs 0.1.4 and earlier (import_names)
FLOOR_FILE = "name-floor.json"  # the floor of rnfs 0.1.4 and earlier (import_floor)
FLOOR_VERSION = 1
FLOOR_FACTOR = 4  # the floor holds up to this many times name_table_size names, so it outlasts table churn ...
FLOOR_KEEP_AGE = 7 * 24 * 3600.0  # ... except that names resolved this recently (seconds) are kept past that ...
FLOOR_HARD_FACTOR = 2  # ... up to this many times as many in all (the file stays a few MB at the defaults)
FLOOR_SAVE_INTERVAL = 5.0  # seconds: a resolve raising the floor saves it at once, or this long after the last save
FLOOR_TOUCH_SAVE = 3600.0  # a resolve that only renews a floor entry's time marks it for the next save after this
PIN_SOURCE_PREFIX = "name:"
# Private mode restricts rnfs's links, not RNS announces: a name's record rides in its announce, which RNS
# propagates to every node that hears it (specification sections 3 and 10)
PRIVATE_NAME_WARNING = ("private mode doesn't hide names: this name's announce, with its sequence and target CID, "
                        "is visible outside the private network, though its content is served to members only")
_ALIAS = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_PARTIAL = re.compile(re.escape(fsutil.PARTIAL_PREFIX) + r"[0-9a-f]{16}")  # a key write's temp file (write_atomic)
_DECIMAL = re.compile(r"-?[0-9]+")  # a sequence column (decimal text: sequences don't fit SQLite's integers)
_MAX_USED = 2**62  # floor rows' used values stay below this, so the ones after them still fit SQLite's integers
_POLL_INTERVAL = 0.25  # how often a refresh checks whether RNS has heard a path response
# An announce shows its record current for this many announce_intervals after it was emitted, however recently
# it was heard: RNS remembers a replayed announce as just heard (_announce_heard)
ANNOUNCE_VALIDITY = MISSED_ANNOUNCES
_PATH_RANDOM_BLOBS = 4  # RNS's IDX_PT_RANDBLOBS: where a path table entry holds its announces' random blobs
_PATH_PACKET = 6  # RNS's IDX_PT_PACKET: where a path table entry holds the packet hash of the path's announce
_MAX_LOOP_WAIT = 60  # the announce loop wakes at least this often, so new schedules are picked up
REFRESHED_SIZE = 4096  # how many names' last network refresh a resolve remembers (E1), least recent forgotten first


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
    except Exception:  # noqa: BLE001 - umsgpack raises several types for hostile bytes
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
    return ids.parse_hex16(text, "name", brackets=True)


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
    # A key imported from elsewhere (or found with no saved record: the state was lost) may already have records
    # on the network and at keepers, so its first publish asks both before choosing a sequence
    imported: bool = False

    @property
    def unsettled(self) -> bool:
        """Imported and not yet published here: its records elsewhere are unknown."""
        return self.imported and self.cid is None


# State


def _sequence(text) -> int:
    """A sequence from its decimal text column. Raises ValueError (TypeError if it isn't text)."""
    if not isinstance(text, str):
        raise TypeError(f"sequence {text!r} isn't text")
    if not _DECIMAL.fullmatch(text):
        raise ValueError(f"sequence {text!r} isn't decimal")
    return int(text)


def _published_row(alias, sequence, cid, imported) -> _Published:
    """A names_published row as what it records. Raises ValueError or TypeError if it is malformed."""
    if not isinstance(alias, str) or not _ALIAS.fullmatch(alias):
        raise ValueError(f"invalid alias {alias!r}")
    if type(imported) is not int or imported not in (0, 1):
        raise ValueError(f"invalid imported flag {imported!r} of {alias}")
    return _Published(_sequence(sequence), None if cid is None else Cid.from_bytes(cid), bool(imported))


def _held_row(name, sequence, cid, heard) -> tuple[bytes, Record]:
    """A names_held row as (name, record). Raises ValueError or TypeError if it is malformed."""
    name = _check_name(name)
    if type(heard) not in (int, float):
        raise TypeError(f"heard time {heard!r} of {name.hex()} isn't a number")
    return name, Record(_sequence(sequence), Cid.from_bytes(cid), float(heard))


def read_names(path: str, shape=None):
    """names.json's value, for statedb.migrate (shape is unused), or None if there is none. Raises RnfsError if it
    can't be read or isn't JSON, leaving it in place: starting without it would allow rollback."""
    try:
        with open(path, "rb") as f:
            value = json.load(f)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as e:
        raise RnfsError(f"could not read {path}: {e}") from None
    if not isinstance(value, dict):  # null too, which rnfs 0.1.4 refused as malformed
        raise RnfsError(f"malformed {path}: not an object")
    return value


def import_names(value, tx) -> int:
    """Import names.json (statedb.migrate): its published and held records replace the names_published and
    names_held rows, every one checked as rnfs 0.1.4 loaded it, whether or not its key file is there. A malformed
    record raises RnfsError, which rolls the import back and stops startup, as it did then; an alias no key file
    can have is skipped with a warning. Returns the number of records imported."""
    published, held = [], {}
    try:
        for alias, entry in value.get("published", {}).items():
            cid = Cid.parse(entry["cid"]) if entry["cid"] is not None else None
            row = (alias, str(int(entry["sequence"])), None if cid is None else cid.binary,
                   int(bool(entry.get("imported", False))))
            if not _ALIAS.fullmatch(alias):
                # Never a key's (_load_keys reads only files so named); reconcile_pins releases any pin it has
                log(f"skipping the record of {alias!r} in {STATE_FILE}: not a valid alias", LOG_WARNING)
                continue
            published.append(row)
        for name_hex, entry in value.get("held", {}).items():
            heard = float(entry["heard"])
            held[parse_name(name_hex)] = (str(int(entry["sequence"])), Cid.parse(entry["cid"]).binary,
                                          0.0 if math.isnan(heard) else heard)  # SQLite has no NaN: heard long ago
    except (AttributeError, KeyError, OverflowError, TypeError, ValueError) as e:
        raise RnfsError(f"malformed {STATE_FILE}: {e}") from None
    tx.execute("DELETE FROM names_published")
    tx.execute("DELETE FROM names_held")
    tx.executemany("INSERT INTO names_published (alias, sequence, cid, imported) VALUES (?, ?, ?, ?)", published)
    tx.executemany("INSERT INTO names_held (name, sequence, cid, heard) VALUES (?, ?, ?, ?)",
                   [(name, *row) for name, row in held.items()])
    return len(published) + len(held)


def _floor_shape(value) -> bool:
    """True if value is a name-floor.json of FLOOR_VERSION: {"version", "floor": [entry, ...]}."""
    return isinstance(value, dict) and value.get("version") == FLOOR_VERSION and isinstance(value.get("floor"), list)


def read_floor(path: str, shape=None):
    """name-floor.json's value, for statedb.migrate (shape is unused), or None if there is none. A damaged file is
    moved aside and gives None: the node starts without it, since it only bounds rollback of gateway and
    auto-fetch resolves. Raises OSError if it can't be read."""
    return statedb.read_json(path, _floor_shape)


def _resolve_time(rest: list) -> float:
    """The resolve time a name-floor.json entry ends with: 0.0 (long ago) unless it is one finite number, as
    entries written before resolve times were kept lack it."""
    if len(rest) != 1 or type(rest[0]) not in (int, float):
        return 0.0
    try:
        at = float(rest[0])
    except OverflowError:  # an integer past float's range
        return 0.0
    return at if math.isfinite(at) else 0.0


def import_floor(value: dict, tx) -> int:
    """Import name-floor.json (statedb.migrate): its [name, sequence, resolved] entries, least recently resolved
    first, replace the name_floor rows, each checked as rnfs 0.1.4 loaded it: one that doesn't parse is skipped
    (and counted in a warning), one without a valid resolve time counts as resolved long ago, and a name listed
    twice keeps the higher sequence, at its later place. Held names, future times and the floor's bound are left
    to the load (_NameFloor.load). Returns the number of entries imported."""
    floor: OrderedDict[bytes, tuple[int, float]] = OrderedDict()
    skipped = 0
    for entry in value["floor"]:
        try:
            name_hex, sequence, *rest = entry
            name = parse_name(name_hex)
        except (TypeError, ValueError):
            skipped += 1
            continue
        if type(sequence) is not int or not 0 <= sequence <= MAX_SEQUENCE:
            skipped += 1
            continue
        earlier = floor.pop(name, None)
        floor[name] = (max(sequence, earlier[0] if earlier else 0), _resolve_time(rest))
    if skipped:
        log(f"skipped {skipped} malformed entr{'y' if skipped == 1 else 'ies'} in {FLOOR_FILE}", LOG_WARNING)
    tx.execute("DELETE FROM name_floor")
    tx.executemany("INSERT INTO name_floor (name, sequence, resolved, used) VALUES (?, ?, ?, ?)",
                   [(name, str(sequence), at, used) for used, (name, (sequence, at)) in enumerate(floor.items())])
    return len(floor)


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

    def emitted_time(self, name: bytes) -> float | None:
        """When the newest announce RNS took a path to name from was emitted (its owner's clock, signed), or None.

        RNS internal (Transport.path_table[hash][IDX_PT_RANDBLOBS], the random blobs of the announces it took the
        path from, whose bytes 5-10 are each one's emission time: Transport.timebase_from_random_blobs). RNS adds an
        announce's blob only if it was emitted later than the path's newest, so repeating an old announce, which
        RNS still remembers as just heard (heard_time), leaves this as it was. If it changes in another RNS version
        this returns None and callers go by the heard time alone.
        """
        try:
            entry = RNS.Transport.path_table.get(name)
            if not entry:
                return None
            emitted = RNS.Transport.timebase_from_random_blobs(entry[_PATH_RANDOM_BLOBS])
            return float(emitted) if type(emitted) is int and emitted > 0 else None
        except Exception:  # noqa: BLE001 - RNS internals: unknown if they fail
            return None

    def recalled_emission(self, name: bytes, app_data: bytes) -> tuple[float | None, bool]:
        """(emitted, takes_path) for the announce RNS last stored for name, whose app_data (recall_app_data) is
        app_data: when that announce was emitted (its owner's clock, signed), or None if RNS no longer has it; and
        False if RNS shows it isn't the record of the announce RNS took name's path from (a replay, usually of an
        older announce, which RNS stores all the same).

        RNS internals (Identity.known_destinations[hash][1], the packet hash Identity.remember stores;
        Transport.path_table[hash][IDX_PT_PACKET], the packet hash of the path's announce; Transport's announce
        cache, written only for an announce that takes the path, read with Transport.get_cached_packet). The path's
        announce is read first: if it carries app_data, so does the path (an announce heard again, or repeated in
        a path response, which RNS hashes with its context, gets another packet hash and is never cached). Else
        the stored one is read for its own emission time. Without a path, or with neither announce cached (a
        shared instance's client whose cache isn't rnsd's), nothing shows a replay: (None, True), or the path's
        emission time if the stored announce is the path's. If an internal changes in another RNS version this
        returns (None, True) and callers go by emitted_time alone.
        """
        try:
            known = RNS.Identity.known_destinations.get(name)
            path = RNS.Transport.path_table.get(name)
            # known[3] is another record if RNS stored a newer announce since app_data was read: judge that one later
            if not known or not path or known[3] != app_data:
                return None, True
            stored, path_packet = known[1], path[_PATH_PACKET]
            in_path = self._cached_announce(name, path_packet)
            if in_path is not None and in_path[1] == app_data:
                return in_path[0], True
            own = in_path if stored == path_packet else self._cached_announce(name, stored)
            emitted = own[0] if own is not None and own[1] == app_data else None
            if in_path is not None:
                return emitted, False  # the path's announce carries another record
            if stored == path_packet:
                return self.emitted_time(name), True
            return emitted, True
        except Exception:  # noqa: BLE001 - RNS internals: unknown if they fail
            return None, True

    @staticmethod
    def _cached_announce(name: bytes, packet_hash) -> tuple[float, bytes] | None:
        """(emission time, app_data) of name's announce with packet_hash in RNS's announce cache, or None if it
        isn't there (or isn't an announce of name). Not verified again: RNS caches only announces it validated."""
        packet = RNS.Transport.get_cached_packet(packet_hash, packet_type="announce")
        if packet is None or packet.unpack() is False or packet.packet_type != RNS.Packet.ANNOUNCE \
                or packet.destination_hash != name:
            return None
        data = bytes(packet.data)
        blob = RNS.Identity.KEYSIZE // 8 + RNS.Identity.NAME_HASH_LENGTH // 8  # the random blob: emission time
        ratchet = RNS.Identity.RATCHETSIZE // 8 if packet.context_flag == RNS.Packet.FLAG_SET else 0
        app_data = data[blob + 10 + ratchet + RNS.Identity.SIGLENGTH // 8:]
        return float(RNS.Transport.timebase_from_random_blob(data[blob:blob + 10])), app_data

    def heard_time(self, name: bytes) -> float | None:
        """When RNS last stored a validly signed announce for name (wall clock), or None.

        RNS internal (Identity.known_destinations[hash][0], set by Identity.remember for every valid announce
        and path response, even ones that don't change the path table and so never reach announce handlers).
        If it changes in another RNS version this returns None and callers fall back to their own times.
        """
        try:
            entry = RNS.Identity.known_destinations.get(name)
            return float(entry[0]) if entry else None
        except Exception:  # noqa: BLE001 - RNS internals: unknown if they fail
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


# The floor


class _NameFloor(Mapping):
    """The highest sequence of each name resolved without persist (gateway, LXMF auto-fetch), and when it was last
    resolved (wall clock), least recently resolved first, persisted in the name_floor table. One value per name,
    (sequence, resolved, used), used being its row's place in the table; read as a mapping of name -> (sequence,
    resolved).

    At most FLOOR_FACTOR × name_table_size names (size()), more (up to FLOOR_HARD_FACTOR times that) while those
    resolved within FLOOR_KEEP_AGE need the room (trim). Saved, so such a resolve never goes back after table churn
    or a restart: at once, or FLOOR_SAVE_INTERVAL after the last save (a timer), writing only the rows changed.

    Changed only under the manager's lock, which every caller but save() holds; save() snapshots the rows under it
    and writes without it.
    """

    def __init__(self, db, lock, clock, monotonic, size):
        """An empty floor over the state database db (load() reads it); lock is the manager's, clock and monotonic
        its clocks, and size() how many names the floor holds before trimming."""
        self._db = db
        self._lock = lock
        self._clock = clock  # resolve times: persisted, so wall clock
        self._monotonic = monotonic  # save pacing
        self._size = size
        self._entries: OrderedDict[bytes, tuple[int, float, int]] = OrderedDict()
        self._next_used = 0  # above every used value given out
        self._changed: set[bytes] = set()  # names whose row the next save writes (or deletes)
        self._dirty = False  # a save is due
        self._saved_at = float("-inf")  # monotonic time of the last save
        self._timer: threading.Timer | None = None  # a save waiting for FLOOR_SAVE_INTERVAL to pass
        self._closed = False  # close() has run: no timer is started
        # Saves write without the lock, one at a time, so an older snapshot never lands last. Taken before the
        # manager's lock, never while holding it
        self._save_lock = threading.Lock()

    # Reading: name -> (sequence, resolved)

    def __getitem__(self, name: bytes) -> tuple[int, float]:
        """name's (sequence, resolve time); KeyError if it has no entry."""
        sequence, resolved, _ = self._entries[name]
        return sequence, resolved

    def __iter__(self):
        """The names, least recently resolved first."""
        return iter(self._entries)

    def __len__(self) -> int:
        """How many names the floor holds."""
        return len(self._entries)

    def __contains__(self, name) -> bool:
        """True if name has an entry."""
        return name in self._entries

    # Changes; callers hold the manager's lock

    def raise_to(self, name: bytes, sequence: int):
        """A higher record was accepted for name: raise its entry (if it has one) to sequence, keeping its place and
        resolve time, and save it within FLOOR_SAVE_INTERVAL (by the timer, since callers hold the lock, some on
        RNS's thread)."""
        entry = self._entries.get(name)
        if entry is not None and sequence > entry[0]:
            self._entries[name] = (sequence, entry[1], entry[2])
            self._changed.add(name)
            self._dirty = True
            self.save_soon(inline=False)

    def drop(self, name: bytes):
        """Forget name (held for the user now: its held record is its floor); the next save deletes its row."""
        if self._entries.pop(name, None) is not None:
            self._changed.add(name)
            self._dirty = True

    def resolved(self, name: bytes, sequence: int) -> bool:
        """A resolve without persist returned name's record at sequence: it becomes the most recently resolved, and
        its sequence the floor for later resolves (trim says which are forgotten). A floor raised (or a new one) is
        saved at once, or FLOOR_SAVE_INTERVAL after the last save, so a crash soon after loses little. True if the
        caller is to save it now (save()), once it has released the lock."""
        now = self._clock()
        previous = self._entries.pop(name, None)
        # Last, with a new used value: the most recently resolved
        self._entries[name] = (sequence if previous is None else max(previous[0], sequence), now, self._next_used)
        self._next_used += 1
        self._changed.add(name)  # written by the next save, even if this change doesn't call for one
        trimmed = self.trim()
        if previous is None or self._entries[name][0] != previous[0] or trimmed:
            self._dirty = True
            return self.save_soon()
        if not 0 <= now - previous[1] < FLOOR_TOUCH_SAVE:
            self._dirty = True  # only its time renewed: saved with the next change or by announce_due
        return False

    def trim(self) -> bool:
        """Forget the least recently resolved names over size(), but not those resolved within FLOOR_KEEP_AGE unless
        the floor holds FLOOR_HARD_FACTOR times that. True if any went."""
        size = self._size()
        now = self._clock()
        trimmed = False
        while len(self._entries) > size:
            _, at, _ = next(iter(self._entries.values()))
            if len(self._entries) <= FLOOR_HARD_FACTOR * size and now - at < FLOOR_KEEP_AGE:
                break
            name, _ = self._entries.popitem(last=False)
            self._changed.add(name)
            trimmed = True
        return trimmed

    def save_soon(self, inline: bool = True) -> bool:
        """True if the floor is to be saved now (the last save was FLOOR_SAVE_INTERVAL ago), by the caller once it
        has released the lock; else it is saved once it is (a timer). Without inline, a save due now is the
        timer's too, started at once."""
        wait = self._saved_at + FLOOR_SAVE_INTERVAL - self._monotonic()
        if wait <= 0 and inline:
            return True
        if self._timer is None and not self._closed:
            timer = threading.Timer(max(0.0, wait), self._save_later)
            timer.daemon = True
            self._timer = timer
            timer.start()
        return False

    def cancel_timer(self):
        """Cancel a save waiting for its time (stop() saves at once instead)."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def close(self):
        """Start no timer from now on, and cancel a waiting one."""
        self._closed = True
        self.cancel_timer()

    # Loading and saving

    def load(self, held):
        """Read the name_floor rows, least recently resolved first. A row that doesn't parse is logged and deleted
        (the floor only bounds rollback of gateway and auto-fetch resolves), as are those of names in held (resolved
        for the user since: their held record is their floor) and those trim drops. A resolve time in the future
        (the clock was corrected since) counts as now, and is written so by the next save that has a change to
        write."""
        now = self._clock()
        dropped = []
        for name, sequence, resolved, used in self._db.query(
                "SELECT name, sequence, resolved, used FROM name_floor ORDER BY used"):
            try:
                _check_name(name)
                sequence = _sequence(sequence)
                if not 0 <= sequence <= MAX_SEQUENCE:
                    raise ValueError(f"sequence {sequence} out of range")
                if type(resolved) not in (int, float) or not math.isfinite(resolved):
                    raise ValueError(f"invalid resolve time {resolved!r}")
                if type(used) is not int or not 0 <= used < _MAX_USED:
                    raise ValueError(f"invalid used value {used!r}")
            except (TypeError, ValueError) as e:
                log(f"dropping an invalid name floor entry {name!r}: {e}", LOG_WARNING)
                dropped.append(name)
                continue
            if name in held:
                dropped.append(name)
                continue
            self._entries[name] = (sequence, min(float(resolved), now), used)
            self._next_used = max(self._next_used, used + 1)
            if resolved > now:
                self._changed.add(name)
        self.trim()
        trimmed = [name for name in self._changed if name not in self._entries]
        self._changed.difference_update(trimmed)
        if dropped or trimmed:
            with self._db.transaction() as tx:
                tx.executemany("DELETE FROM name_floor WHERE name = ?", [(name,) for name in dropped + trimmed])

    def _snapshot(self) -> tuple[set[bytes], list[tuple], list[tuple]]:
        """(the names changed, the rows to write for them, the rows to delete), taken as saved. The lock held."""
        changed, self._changed = self._changed, set()
        rows = [(name, str(self._entries[name][0]), self._entries[name][1], self._entries[name][2])
                for name in changed if name in self._entries]
        gone = [(name,) for name in changed if name not in self._entries]
        self._dirty = False
        return changed, rows, gone

    def save(self):
        """Write the entries changed (or forgotten) since the last save, if a save is due, with synchronous=NORMAL.
        Saved by a resolve that raised it (at most every FLOOR_SAVE_INTERVAL, since it holds thousands of names),
        the timer, announce_due (at least every minute while running) and stop().

        Called without the lock: only the rows are made under it, so resolves and announces needn't wait for the
        write. The save counts from when they were made; a failed one is retried by the next."""
        with self._save_lock:
            with self._lock:
                if not self._dirty:
                    return
                changed, rows, gone = self._snapshot()
                saved_at, self._saved_at = self._saved_at, self._monotonic()
            saved = False
            try:
                try:
                    with self._db.transaction() as tx:
                        tx.executemany("DELETE FROM name_floor WHERE name = ?", gone)
                        tx.executemany("INSERT OR REPLACE INTO name_floor (name, sequence, resolved, used) "
                                       "VALUES (?, ?, ?, ?)", rows)
                    saved = True
                except OSError as e:
                    log(f"could not save the name floor: {e}", LOG_WARNING)
            finally:
                if not saved:
                    with self._lock:
                        self._changed |= changed
                        self._dirty = True
                        self._saved_at = saved_at

    def _save_later(self):
        """The timer's save."""
        with self._lock:
            self._timer = None
        self.save()


# Names


def _wait_event(event: threading.Event, timeout: float) -> bool:
    """Wait up to timeout seconds for event to be set: True if it was."""
    return event.wait(timeout)


class NameManager:
    """This node's name keys and records, and the records it has heard. Thread-safe.

    Lifecycle: construct (loads keys and state, no RNS needed), reconcile_pins() once the node and its features
    are set up, start() once Reticulum runs, run(stop_event) on a thread for re-announces, stop() and close() at
    shutdown.
    """

    def __init__(self, config: Config, pins, network=None, clock=time.time, monotonic=time.monotonic,
                 wait=_wait_event):
        """Names under config.names_dir and config.state_dir; pins has pin(cid, source) and unpin(cid, source).
        wait(event, timeout) is how a refresh waits for an answer (_wait_event; tests pass one that doesn't
        block)."""
        self.config = config
        self._pins = pins
        self._network = network if network is not None else RnsNetwork()
        self._clock = clock  # wall clock: heard times are persisted across restarts
        self._monotonic = monotonic  # announce schedule
        self._wait = wait
        self._lock = threading.RLock()

        self._keys: dict[str, RNS.Identity] = {}
        self._names: dict[str, bytes] = {}  # alias -> name
        self._aliases: dict[bytes, str] = {}  # name -> alias
        self._published: dict[str, _Published] = {}
        # Highest records of names published here or resolved through the CLI or API: persisted, unbounded
        # (they only grow by the user's own actions)
        self._held: dict[bytes, Record] = {}
        # What the names_published rows (but those of _unreadable aliases) and the names_held rows record, so a
        # save writes only the rows that changed
        self._saved_published: dict[str, _Published] = {}
        self._saved_held: dict[bytes, Record] = {}
        # Every other name's record: in memory, least recently used first, at most name_table_size
        self._table: OrderedDict[bytes, Record] = OrderedDict()
        # The table's names that were only heard, never resolved here, least recently used first. An announce of a
        # name this node isn't resolving and doesn't know may only take the place of one of these, so a flood of
        # new names can't push out the names it resolved
        self._heard_only: OrderedDict[bytes, None] = OrderedDict()
        self._held_dirty = False  # held records changed without a save (heard again, or a save that failed)

        self._destinations: dict[str, object] = {}
        self._next_announce: dict[str, float] = {}  # alias -> monotonic time
        self._waiters: dict[bytes, list[threading.Event]] = {}
        # A resolve's network refresh of each name (E1): when it last finished (monotonic) and whether a record was
        # held after it, least recent first, at most REFRESHED_SIZE; and the refreshes running, which other
        # resolves of the name wait for rather than refresh again
        self._refreshed: OrderedDict[bytes, tuple[float, bool]] = OrderedDict()
        self._refreshing: dict[bytes, threading.Event] = {}
        self._handler = NameAnnounceHandler(self._heard)
        self._started = False
        # The keepers feature (rnfs.features.keepers), if loaded: told of heard and published records, and asked
        # for signed records when the network has none. Never called with self._lock held
        self.keepers = None
        # Names watched (rnfs.Node's names.watch, GET /api/name/watch), and how many watches each: their announces
        # count as asked, so a full table never refuses them. Record listeners (add_record_listener) are told of
        # each newer record accepted for a watched name; swapped whole, so they are read without the lock
        self._watched: dict[bytes, int] = {}
        self._record_listeners: tuple = ()
        # Called as held_listener(name, record), with the lock held, for each newer record accepted for a name held
        # (persisted: this node's own, kept, followed, resolved) or watched: the node's "name" events. Must only queue
        self.held_listener = None
        # Pins of names whose key file was deleted by hand, released by release_gone() once the node is set up
        self._gone: list[tuple[str, Cid]] = []
        # Aliases whose key file is there but couldn't be read (permissions, damage): not names this node can
        # use, but not gone either, so their pins stay and saves leave their names_published row (if any) as it
        # is, for when the file can be read again
        self._unreadable: set[str] = set()
        # Aliases with a saved record: for these the recorded target (or none) is the only CID their pin source
        # may hold (reconcile_pins). A key without one (the state was lost) may have pins nothing records
        self._recorded: set[str] = set()
        # alias -> CIDs its pin source holds that its record doesn't (left by a crash, or an unpin that failed):
        # released by its next publish or removal
        self._stale: dict[str, set[Cid]] = {}

        self._remove_old_temps()
        self._db = statedb.connect(config.state_dir)
        # The highest sequence of each name resolved without persist (gateway, LXMF auto-fetch), bounded and saved
        self._floor = _NameFloor(self._db, self._lock, clock, monotonic, self._floor_size)
        try:
            self._load_keys()
            self._load_state()
            self._floor.load(self._held)
        except BaseException:
            self._db.close()
            raise

    # Keys

    def create(self, alias: str) -> bytes:
        """Make a new name key under alias and return its name. ValueError if alias is invalid or taken."""
        check_alias(alias)
        with self._lock:
            self._check_free(alias)
            identity = RNS.Identity()
            self._add_key(alias, identity, _Published(0, None))
            name = self._names[alias]  # under the lock: a remove may follow at once
        log(f"created name {name.hex()} as {alias}", LOG_NOTICE)
        return name

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
            held = self._stale.pop(alias, set())
            if published is not None and published.cid is not None:
                held.add(published.cid)
            self._release(alias, held)
            self._stale.pop(alias, None)  # one that failed is released at the next start (reconcile_pins)
            self._next_announce.pop(alias, None)
            destination = self._destinations.pop(alias, None)
            if destination is not None:
                self._network.close_destination(destination)
            name = self._names.pop(alias)
            del self._aliases[name]
            del self._keys[alias]
            self._recorded.discard(alias)
            # State first: a crash before the key file is gone leaves an unpublished name, not a lost pin
            self._save()
            key_path = self._key_path(alias)
            if not fsutil.remove(key_path):
                # Windows, with something holding the key open: the name would come back at the next start
                log(f"couldn't delete {key_path}, which is in use; delete it, or {alias} returns at the next start",
                    LOG_WARNING)
        log(f"removed name {name.hex()} ({alias})", LOG_NOTICE)

    def release_gone(self) -> set[tuple[str, Cid]]:
        """Release the pins of names whose key file was deleted by hand (found by the constructor); returns the
        (alias, CID) pins released.

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
            return set(gone)

    def reconcile_pins(self, pins: dict[Cid, set[str]]):
        """Make the name pins (pins: every pin and its sources, as storage.pins() gives them) match the saved
        records, once the node is set up: release_gone(), then pin each recorded target that lacks its name's pin
        and release name pins nothing records (a crash inside publish() can leave one, and earlier rnfs versions
        could leave one that `pin rm` and `name rm` couldn't remove).

        A key with no saved record (the state was lost) keeps its pins until its next publish or removal, which
        release them; a pin of an alias with no key file at all is released, but one whose key file is there and
        can't be read is kept.
        """
        released = self.release_gone()
        with self._lock:
            held: dict[str, set[Cid]] = {}
            for cid, sources in pins.items():
                for source in sources:
                    if not source.startswith(PIN_SOURCE_PREFIX):
                        continue
                    alias = source[len(PIN_SOURCE_PREFIX):]
                    if (alias, cid) not in released:  # pins taken before release_gone() let go of these
                        held.setdefault(alias, set()).add(cid)
            for alias in sorted(set(held) | set(self._keys)):
                cids = held.get(alias, set())
                if alias in self._unreadable:
                    log(f"keeping the pins of name {alias}, whose key file can't be read", LOG_WARNING)
                    continue
                if alias not in self._keys:
                    log(f"releasing the pins of name {alias}, which has no key", LOG_WARNING)
                    self._release(alias, cids)
                    self._stale.pop(alias, None)  # retried at the next start
                    continue
                recorded = self._published[alias].cid
                if alias not in self._recorded:
                    if cids:
                        log(f"name {alias} has no saved record; its pins are released at its next publish",
                            LOG_WARNING)
                        self._stale.setdefault(alias, set()).update(cids)
                    continue
                if recorded is not None and recorded not in cids:
                    log(f"pinning {recorded} again for name {alias}, as its saved record says", LOG_WARNING)
                    try:
                        self._pins.pin(recorded, PIN_SOURCE_PREFIX + alias)
                    except OSError as e:  # e.g. a read-only state directory: start all the same
                        log(f"could not pin {recorded} for name {alias}: {e}", LOG_ERROR)
                extra = cids - {recorded}
                if extra:
                    log(f"releasing {len(extra)} pin(s) of name {alias} that its saved record doesn't hold",
                        LOG_WARNING)
                    self._release(alias, extra)

    def _release(self, alias: str, cids):
        """Release alias's pin source from cids; one that fails (a disk error saving pins) is logged and kept in
        _stale, for the next publish or removal. Lock held."""
        for cid in cids:
            try:
                self._pins.unpin(cid, PIN_SOURCE_PREFIX + alias)
            except OSError as e:
                log(f"could not release {cid} from name {alias}: {e}", LOG_ERROR)
                self._stale.setdefault(alias, set()).add(cid)

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
        """Point alias's name at cid, pin it, announce it. Returns {"name", "cid", "sequence"}, and in private mode
        a "warning" (PRIVATE_NAME_WARNING): the announce reaches outsiders all the same."""
        if not isinstance(cid, Cid):
            raise ValueError("publish needs a CID")
        with self._lock:
            name = self.name(alias)
            refresh = self._published[alias].unsettled
        if refresh:
            # A freshly imported key: its previous home may have published sequences we've never heard, which
            # keepers may hold after its announces have expired (section 9: above the highest it can resolve)
            self._offer_kept(name)
            self._refresh_name(name, ask="always")

        with self._lock:
            if self.name(alias) != name:  # NotFound if removed while refreshing
                raise NotFound(f"name {alias} was replaced while publishing")
            self._absorb_recalled(name)
            previous = self._published[alias]
            known = self._record(name, touch=False)
            sequence = 1 + max(previous.sequence, known.sequence if known is not None else 0)
            if sequence > MAX_SEQUENCE:
                raise RnfsError(f"name {alias} has reached the highest sequence")

            source = PIN_SOURCE_PREFIX + alias
            # Crash-consistent: pin the new target, then save the records (the commit point), then release the old
            # target. A crash leaves at most an extra pin of this source, which reconcile_pins() releases at the
            # next start, and the recorded target is pinned at every step (blocks they share stay protected)
            self._pins.pin(cid, source)
            held_before = self._held.get(name)
            self._published[alias] = _Published(sequence, cid)
            self._held[name] = Record(sequence, cid, self._clock())
            try:
                self._save()
            except Exception:
                # Not published: back to what the database still says (ENOSPC, EIO)
                self._published[alias] = previous
                if held_before is None:
                    self._held.pop(name, None)
                else:
                    self._held[name] = held_before
                if cid != previous.cid and cid not in self._stale.get(alias, set()):
                    self._release(alias, {cid})
                raise
            self._hold(name)
            self._release(alias, ({previous.cid} | self._stale.pop(alias, set())) - {cid, None})
            self._announce(alias)
            identity = self._keys[alias]
            self._accepted(name, self._held[name])
        log(f"published {name.hex()} ({alias}) -> {cid}, sequence {sequence}", LOG_NOTICE)
        if self.keepers is not None:
            # This node keeps its own names, so they resolve through it while the owner's announce is gone
            self.keepers.published(name, identity, encode_record(sequence, cid))
        result = {"name": name.hex(), "cid": str(cid), "sequence": sequence}
        if self.config.private:
            result["warning"] = PRIVATE_NAME_WARNING
        return result

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
            self._floor.cancel_timer()
            self._save()
        self._floor.save()

    def close(self):
        """Release the state database, once stop() has saved everything (idempotent)."""
        with self._lock:
            self._floor.close()
        self._db.close()

    def announce_due(self) -> float:
        """Re-announce every name whose announce_interval has passed; seconds until the next is due."""
        with self._lock:
            now = self._monotonic()
            for alias, due in sorted(self._next_announce.items()):
                if due <= now:
                    self._announce(alias)
            if self._held_dirty:
                self._save()
            if not self._next_announce:
                wait = self.config.announce_interval
            else:
                wait = max(0.0, min(self._next_announce.values()) - now)
        self._floor.save()
        return wait

    def run(self, stop_event: threading.Event):
        """The re-announce loop, for a thread of its own; returns once stop_event is set."""
        while not stop_event.is_set():
            try:
                delay = self.announce_due()
            except Exception as e:  # noqa: BLE001 - the announce thread keeps running; logged
                log(f"name re-announce failed: {e}", LOG_ERROR)
                delay = _MAX_LOOP_WAIT
            stop_event.wait(min(delay, _MAX_LOOP_WAIT))

    # Watching

    def watch(self, name: bytes):
        """Watch name: its announces count as asked, and record listeners hear of its newer records."""
        name = _check_name(name)
        with self._lock:
            self._watched[name] = self._watched.get(name, 0) + 1
            self._heard_only.pop(name, None)  # asked now: no longer the first a full table gives up

    def unwatch(self, name: bytes):
        """Undo one watch(name)."""
        with self._lock:
            count = self._watched.get(name, 0) - 1
            if count > 0:
                self._watched[name] = count
            else:
                self._watched.pop(name, None)

    def add_record_listener(self, listener):
        """Call listener(name, record) for each newer record accepted for a watched name (an announce, a path
        response, a keeper's answer, or a publish here). It is called with the names lock held, on whichever
        thread accepted the record (RNS's included), so it must only hand the record on, never block or call
        back into rnfs."""
        with self._lock:
            self._record_listeners = (*self._record_listeners, listener)

    def remove_record_listener(self, listener):
        """Stop calling listener."""
        with self._lock:
            # ==, not is: a bound method is a new object at each access, equal to the one added
            self._record_listeners = tuple(item for item in self._record_listeners if item != listener)

    def _accepted(self, name: bytes, record: Record):
        """A newer record was accepted for name: tell the record listeners if it is watched, and held_listener if it
        is held or watched. Lock held."""
        if self.held_listener is not None and (name in self._held or name in self._watched):
            try:
                self.held_listener(name, record)
            except Exception as e:  # noqa: BLE001 - as below
                log(f"record listener failed: {e!r}", LOG_ERROR)
        if name not in self._watched:
            return
        for listener in self._record_listeners:
            try:
                listener(name, record)
            except Exception as e:  # noqa: BLE001 - one listener mustn't break the others, or the announce
                log(f"record listener failed: {e!r}", LOG_ERROR)

    # Resolving

    def resolve(self, name: bytes, persist: bool, max_age: float = 0.0) -> dict | None:
        """The highest record held for name after refreshing it: {"name", "cid", "sequence", "age"}, or None.

        persist is True for the CLI and API (the highest sequence is then kept forever) and False for the gateway
        and LXMF auto-fetch (the record goes only into the bounded table, and its sequence into the bounded floor).
        The network refresh, which can take path_timeout and then a keeper query, is skipped for this node's own
        names, when the held record was heard less than max_age seconds ago, and when a refresh of the name ended
        less than max_age ago (_shared_refresh). A resolve finding a refresh of the name running waits for it.
        """
        name = _check_name(name)
        self._offer_kept(name)
        with self._lock:
            held = self._record(name, touch=False)
            # A heard time in the future (the clock was corrected since) isn't fresh
            fresh = held is not None and max_age > 0 and 0 <= self._clock() - held.heard < max_age
            alias = self._aliases.get(name)
            # This node's own names need no refresh, unless imported and not yet published here
            local = alias is not None and not self._published[alias].unsettled
        if not local and not fresh:
            self._shared_refresh(name, max_age)

        # Again: the bounded table may have evicted it meanwhile, letting a replayed announce in
        self._offer_kept(name)
        save_floor = False
        with self._lock:
            self._absorb_recalled(name)
            record = self._record(name, touch=True)
            if record is None:
                return None
            if persist and name not in self._held:
                self._hold(name)
                self._held[name] = record
                self._save()
            elif name not in self._held:
                save_floor = self._resolved(name, record.sequence)
            result = {"name": name.hex(), "cid": str(record.cid), "sequence": record.sequence,
                      "age": max(0.0, self._clock() - record.heard)}
        if save_floor:
            self._floor.save()
        return result

    def _shared_refresh(self, name: bytes, max_age: float):
        """A resolve's network refresh of name (_refresh_name, asking keepers if nothing was heard), shared (E1).

        Skipped if one ended less than max_age ago (never for max_age 0, the CLI's), unless it left a record that
        has gone since (the bounded table evicted it): with the owner offline, a keeper's record shows its old
        emission time, so going by heard times alone every gateway request would refresh again. Nothing found
        counts too, a short negative cache. A refresh leaving a suspected replay (DHT-2) isn't remembered.
        Resolves of a name being refreshed wait for that refresh, whatever their max_age, then each takes the
        record held. Called without the lock."""
        with self._lock:
            if max_age > 0 and self._refreshed_within(name, max_age):
                return
            running = self._refreshing.get(name)
            if running is None:
                running = self._refreshing[name] = threading.Event()
                leader = True
            else:
                leader = False
        if not leader:
            running.wait()
            return
        suspect = True  # a refresh that raised isn't remembered
        try:
            suspect = self._refresh_name(name, ask="if_unheard")
        finally:
            with self._lock:
                del self._refreshing[name]
                if not suspect:
                    self._refreshed.pop(name, None)
                    self._refreshed[name] = (self._monotonic(), self._record(name, touch=False) is not None)
                    while len(self._refreshed) > REFRESHED_SIZE:
                        self._refreshed.popitem(last=False)
            running.set()

    def _refreshed_within(self, name: bytes, max_age: float) -> bool:
        """True if a refresh of name ended less than max_age ago, and the record it left (if any) is still held.
        Lock held."""
        refreshed = self._refreshed.get(name)
        if refreshed is None:
            return False
        at, found = refreshed
        return 0 <= self._monotonic() - at < max_age and (not found or self._record(name, touch=False) is not None)

    def _refresh_name(self, name: bytes, *, ask: str) -> bool:
        """Ask the network for name's newest record (_refresh), then keepers, offering what they answer. ask is
        "always" (the first publish of an imported key: its previous home's sequences may be held by keepers alone)
        or "if_unheard" (a resolve: only if nothing was heard during the refresh, the record RNS stored is a
        suspected replay, or keeper_query is "always"). True if the record held after it is a suspected replay
        (DHT-2; asked only with "if_unheard"). Called without the lock."""
        if ask not in ("always", "if_unheard"):
            raise ValueError(f"ask is 'always' or 'if_unheard', not {ask!r}")
        started = self._clock()
        self._refresh(name)
        suspect = None
        if ask == "if_unheard":
            with self._lock:
                suspect = self._absorb_recalled(name)
                record = self._record(name, touch=False)
            # Heard during the refresh, and no replay suspected: the owner (or a path to it) answered. If not, the
            # owner and the paths may be gone, or a neighbour repeats an old announce
            heard = suspect is None and record is not None and record.heard >= started
            if self.keepers is None or (heard and not self.keepers.always_ask()):
                return self._held_is(name, suspect)
        if self.keepers is not None:
            for sequence, cid, kept_heard in self.keepers.query(name):
                self.offer(name, sequence, cid, kept_heard)
        return self._held_is(name, suspect)

    def _held_is(self, name: bytes, record: tuple[int, Cid] | None) -> bool:
        """True if the record held for name is record, a (sequence, CID)."""
        if record is None:
            return False
        with self._lock:
            held = self._record(name, touch=False)
            return held is not None and (held.sequence, held.cid) == record

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
            # Counted in the steps waited rather than read from a clock, so a test's wait runs it in virtual time
            remaining = self.config.path_timeout
            while remaining > 0:
                step = min(_POLL_INTERVAL, remaining)
                if self._wait(event, step):
                    break
                remaining -= step
                # A path response repeating an announce RNS already has doesn't change the path table, so it
                # never reaches the handler; RNS still records when it heard it
                heard = self._network.heard_time(name)
                if heard is not None and heard >= started:
                    break
        except Exception as e:  # noqa: BLE001 - the resolve goes on with what is held; logged
            log(f"path request for name {name.hex()} failed: {e}", LOG_WARNING)
        finally:
            with self._lock:
                events = self._waiters.get(name, [])
                if event in events:
                    events.remove(event)
                if not events:
                    self._waiters.pop(name, None)

    def _absorb_recalled(self, name: bytes) -> tuple[int, Cid] | None:
        """Offer the record RNS last stored for name (which may be a replay) to the sequence rules. Its (sequence,
        CID) if it is a suspected replay (_announce_heard), which a resolve takes as a reason to ask keepers."""
        try:
            app_data = self._network.recall_app_data(name)
        except Exception as e:  # noqa: BLE001 - the resolve goes on with what is held; logged
            log(f"could not recall name {name.hex()}: {e}", LOG_WARNING)
            return None
        parsed = parse_record(app_data)
        if parsed is None:
            return None
        sequence, cid = parsed
        heard = self._network.heard_time(name)
        if heard is None:
            # Without RNS's time, the record we hold is as old as we last heard it; a new one is assumed just heard
            held = self._record(name, touch=False)
            same = held is not None and held.sequence == sequence and held.cid == cid
            heard = held.heard if same else self._clock()
        heard, suspect = self._announce_heard(name, bytes(app_data), heard)
        self._offer(name, sequence, cid, heard)
        return parsed if suspect else None

    def _announce_heard(self, name: bytes, app_data: bytes, heard: float) -> tuple[float, bool]:
        """(heard time, suspected replay) of name's record from the announce RNS last stored, carrying app_data and
        heard at heard.

        Anyone may repeat an old announce, and RNS stores it as just heard, so by its time alone a record its
        owner has since replaced would look current, and resolves would never ask keepers for the newer one:
        - The record is heard no later than ANNOUNCE_VALIDITY announce_intervals after RNS's newest announce of
          name was emitted (the path's: emitted_time), since the owner announces every announce_interval.
        - An announce emitted before the path's newest, or carrying another record than the path's announce
          (RnsNetwork.recalled_emission), is a suspected replay (DHT-2): it is heard no later than its own
          emission time, or the path's if its own is unknown. Without a path nothing is compared.
        """
        path_emitted = self._network.emitted_time(name)
        if path_emitted is None:
            return heard, False
        emitted, takes_path = self._network.recalled_emission(name, app_data)
        suspect = not takes_path or (emitted is not None and emitted < path_emitted)
        if suspect:
            heard = min(heard, emitted if emitted is not None else path_emitted)
        return min(heard, path_emitted + ANNOUNCE_VALIDITY * self.config.announce_interval), suspect

    def _heard(self, name: bytes, app_data, packet_hash: bytes | None = None):
        """Announce handler: apply a heard record and wake resolves waiting for it."""
        name = bytes(name)
        parsed = parse_record(app_data)
        # A name kept here may take a table place like a resolved one (asked outside our lock, as always)
        kept = parsed is not None and self.keepers is not None and self.keepers.local_record(name) is not None
        # Read before taking the lock: it may read RNS's announce cache on disk
        heard = self._announce_heard(name, bytes(app_data), self._clock())[0] if parsed is not None else None
        with self._lock:
            if parsed is not None:
                # Unasked: not being resolved (a path response would have a waiter), kept nor watched
                unasked = not kept and name not in self._waiters and name not in self._watched
                self._offer(name, parsed[0], parsed[1], heard, unasked=unasked)
            else:
                log(f"ignored malformed record for name {RNS.prettyhexrep(name)}", LOG_VERBOSE)
            for event in self._waiters.get(name, ()):
                event.set()
        if parsed is not None and self.keepers is not None:
            self.keepers.heard(name, parsed[0], parsed[1], packet_hash)

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
            self._touch(name)
        return record

    def _offer(self, name: bytes, sequence: int, cid: Cid, heard: float, unasked: bool = False) -> bool:
        """Apply a validly signed record: True if it replaced the one held. Lower sequences (than the record held,
        or the floor of a name resolved before) are refused, and at an equal sequence the record already held is
        kept. unasked: an announce of a name this node isn't resolving or keeping, which takes a table place only
        if it is free or held by another name that was only heard. A held record whose save fails is logged and
        saved by the next save."""
        held_here = name in self._held
        current = self._record(name, touch=False)
        floor = None if held_here or name not in self._floor else self._floor[name][0]
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

        record = Record(sequence, cid, heard)
        if not self._store(name, record, held_here, unasked):
            return False
        if held_here:
            try:
                self._save()
            except OSError as e:
                # Held all the same, and saved by the next save (announce_due's within a minute): raising here
                # would skip the waking, keepers and listeners, and on RNS's thread reach no one
                log(f"could not save the record of name {name.hex()}: {e}", LOG_WARNING)
                self._held_dirty = True
        self._accepted(name, record)
        return True

    def _store(self, name: bytes, record: Record, held_here: bool, unasked: bool = False) -> bool:
        """Put record in the persisted map or the bounded table, making room in the table, and raise the name's
        floor to it (saved by the timer, since callers hold the lock, some on RNS's thread). False if an unasked
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
        self._floor.raise_to(name, record.sequence)
        if len(self._table) > limit:  # name_table_size 0: nothing is kept
            self._drop_from_table(name)
        return True

    def _touch(self, name: bytes):
        """Mark a table entry recently used."""
        self._table.move_to_end(name)
        if name in self._heard_only:
            self._heard_only.move_to_end(name)

    def _drop_from_table(self, name: bytes):
        """Remove name from the table."""
        self._table.pop(name, None)
        self._heard_only.pop(name, None)

    def _hold(self, name: bytes):
        """name's record is held now: out of the table, and out of the floor (its held record is its floor), whose
        row the next floor save deletes."""
        self._drop_from_table(name)
        self._floor.drop(name)

    def _resolved(self, name: bytes, sequence: int) -> bool:
        """A resolve without persist returned name's record: it is no longer only heard, and its sequence is the
        floor for later resolves (_NameFloor.resolved). True if the caller is to save the floor now, once it has
        released the lock."""
        self._heard_only.pop(name, None)
        return self._floor.resolved(name, sequence)

    def _floor_size(self) -> int:
        """How many names the floor holds, past which only names resolved within FLOOR_KEEP_AGE are kept."""
        return FLOOR_FACTOR * max(0, self.config.name_table_size)

    # RNS destinations; callers hold the lock

    def _open_destination(self, alias: str):
        """Register alias's destination with its record as default app_data (so path responses carry it)."""
        try:
            destination = self._network.name_destination(self._keys[alias])
        except Exception as e:  # noqa: BLE001 - one name's failure doesn't stop the others; logged
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
        except Exception as e:  # noqa: BLE001 - one name's failure doesn't stop the others; logged
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
        """ValueError if alias is in use (in memory or on disk, a dangling symlink included)."""
        if alias in self._keys or os.path.lexists(self._key_path(alias)):
            raise ValueError(f"alias {alias} already exists")

    def _add_key(self, alias: str, identity: RNS.Identity, published: _Published):
        """Save identity as alias's key, track it, and open its destination if running."""
        os.makedirs(self.config.names_dir, mode=0o700, exist_ok=True)
        # Written atomically and private: RNS's Identity.to_file does neither. The temp name starts with "."
        # (fsutil.PARTIAL_PREFIX) so it can never be an alias
        fsutil.write_atomic(self._key_path(alias), identity.get_private_key(), mode=0o600, private=True)
        name = name_for_identity(identity)
        self._keys[alias] = identity
        self._names[alias] = name
        self._aliases[name] = alias
        self._published[alias] = published
        self._save()
        self._recorded.add(alias)
        if self._started:
            self._open_destination(alias)

    def _remove_old_temps(self):
        """Delete the temp files an interrupted key write left: fsutil.write_atomic's (.partial-<16 hex>), which
        hold a private key, and those of rnfs 0.1.1 or earlier (names.json.tmp and .<alias>.tmp), which reused
        those names. Only this node writes here (it holds the rnfs dir's lock), and not yet."""
        try:
            entries = os.listdir(self.config.names_dir)
        except OSError:
            entries = []
        old = [os.path.join(self.config.names_dir, entry) for entry in entries
               if (entry.startswith(".") and entry.endswith(".tmp") and _ALIAS.fullmatch(entry[1:-4]))
               or _PARTIAL.fullmatch(entry)]
        for path in (os.path.join(self.config.state_dir, STATE_FILE + ".tmp"), *old):
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
            if not _ALIAS.fullmatch(alias) or not (os.path.isfile(path) or os.path.islink(path)):
                continue  # not a key file (a directory, a FIFO, ...)
            identity = RNS.Identity.from_file(path) if os.path.isfile(path) else None  # None: a dangling symlink
            if identity is None:
                log(f"could not read name key {path}; skipping it, and keeping its pins and records", LOG_ERROR)
                self._unreadable.add(alias)
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
        """Read the published and held records. A malformed row is an error, as a malformed names.json was:
        starting without it would allow rollback."""
        try:
            for row in self._db.query("SELECT alias, sequence, cid, imported FROM names_published ORDER BY alias"):
                alias, published = row[0], _published_row(*row)
                if alias in self._unreadable:
                    continue  # left as it is by every save, for when the key file can be read again
                self._saved_published[alias] = published
                if alias in self._keys:
                    self._published[alias] = published
                    self._recorded.add(alias)
                elif published.cid is not None:
                    # Its key file was deleted by hand: nothing will republish it, so release its pin
                    self._gone.append((alias, published.cid))
            for row in self._db.query("SELECT name, sequence, cid, heard FROM names_held ORDER BY name"):
                name, record = _held_row(*row)
                self._held[name] = record
        except (TypeError, ValueError) as e:
            raise RnfsError(f"malformed name record in {self._db.path} ({e}); rnfs won't start on it, since starting "
                            "without it would let a name go back to an older record") from None
        self._saved_held = dict(self._held)
        for alias in self._keys:
            # A key with no saved record (the state was lost, or the key copied in by hand) may have been
            # published elsewhere: treated as imported, so its first publish looks for its records first
            self._published.setdefault(alias, _Published(0, None, imported=True))

    def _save(self):
        """Write the published and held records changed since the last save, durably (on disk when it returns:
        this is a publish's commit point); the rows of _unreadable aliases are left as they are. Raises OSError if
        the database fails, having written nothing."""
        published = [(alias, p) for alias, p in self._published.items() if self._saved_published.get(alias) != p]
        unpublished = [alias for alias in self._saved_published if alias not in self._published]
        held = [(name, r) for name, r in self._held.items() if self._saved_held.get(name) != r]
        unheld = [name for name in self._saved_held if name not in self._held]
        if published or unpublished or held or unheld:
            with self._db.transaction(durable=True) as tx:
                tx.executemany("DELETE FROM names_published WHERE alias = ?", [(alias,) for alias in unpublished])
                tx.executemany("INSERT OR REPLACE INTO names_published (alias, sequence, cid, imported) "
                               "VALUES (?, ?, ?, ?)",
                               [(alias, str(p.sequence), None if p.cid is None else p.cid.binary, int(p.imported))
                                for alias, p in published])
                tx.executemany("DELETE FROM names_held WHERE name = ?", [(name,) for name in unheld])
                tx.executemany("INSERT OR REPLACE INTO names_held (name, sequence, cid, heard) VALUES (?, ?, ?, ?)",
                               [(name, str(r.sequence), r.cid.binary, r.heard) for name, r in held])
            for alias in unpublished:
                del self._saved_published[alias]
            self._saved_published.update(published)
            for name in unheld:
                del self._saved_held[name]
            self._saved_held.update(held)
        self._held_dirty = False
