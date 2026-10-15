"""Being considerate on shared radio meshes (the mesh feature; scratchpad spec text in docs/mesh.md).

- Provider choice: within each seniority group, providers fewer hops away are tried first, then those whose
  next hop goes out on a faster interface (provider_rank). RNS is reached only through RnsPaths, which the
  node hands to the fetcher as path_info, so the fetcher stays testable without RNS.
- Airtime controls for /blocks (AirtimePolicy): a node-wide upload budget (a token bucket), interface rules
  and quiet hours. /find, /provide and /pin are tiny and never refused by them.
- Traffic: bounded, thread-safe counters of bytes and requests per peer and per interface, for `rnfs peers`,
  `rnfs stats` and `rnfs status`.

Pure policy with injected clocks; nothing here imports RNS (or rnfs.log, which does) at module level, since
the CLI imports the peers feature, which uses this module.
"""
import math
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Callable

from rnfs.constants import HOUR, RESPONSE_OVERHEAD

MAX_PEERS = 1000  # peers tracked by Traffic; the least recently active is forgotten beyond this
MAX_INTERFACES = 64  # interfaces tracked by Traffic, likewise
STATS_CACHE = 30.0  # seconds a shared instance's interface stats are reused for bitrate lookups
PATHFINDER_M = 128  # RNS.Transport.PATHFINDER_M: what hops_to returns for an unknown path

# Why a /blocks request was refused by an airtime control (Traffic.refused keys)
REFUSED_BUDGET = "budget"
REFUSED_INTERFACE = "interface"
REFUSED_QUIET = "quiet_hours"
REFUSALS = (REFUSED_BUDGET, REFUSED_INTERFACE, REFUSED_QUIET)

# Outcomes of a request we sent (Traffic.request)
ANSWERED = "answered"
REFUSED = "refused"  # answered False (busy, or refused under the node's limits)
FAILED = "failed"  # no link, no answer, stalled or malformed


# Provider choice

@dataclass(frozen=True)
class PathInfo:
    """What RNS knows about the path to a node; None where it doesn't know."""

    hops: int | None = None
    interface: str | None = None  # the next-hop interface's name
    bitrate: float | None = None  # the next-hop interface's bitrate in bits/s


NO_PATH = PathInfo()


def no_path_info(node_id: bytes) -> PathInfo:
    """The path_info of a node without Reticulum (offline test nodes): nothing is known."""
    return NO_PATH


def provider_rank(senior: bool, path: PathInfo, order: int) -> tuple:
    """Sort key for providers: senior first; then fewer hops, then a faster next-hop interface (unknown
    last for both); then the order they were found in."""
    hops = path.hops if path.hops is not None else math.inf
    bitrate = -path.bitrate if path.bitrate else math.inf
    return (0 if senior else 1, hops, bitrate, order)


class RnsPaths:
    """path_info backed by RNS.Transport (or, attached to a shared instance, by its RPC). Thread-safe.

    In a shared-instance client, next_hop_interface is the local interface to the shared instance, so the
    interface name comes from the instance (Reticulum.get_next_hop_if_name) and its bitrate from the
    instance's interface stats, cached for STATS_CACHE.
    """

    def __init__(self, reticulum=None, clock: Callable[[], float] = time.monotonic):
        """Paths through reticulum (default: the process's RNS.Reticulum instance)."""
        self.reticulum = reticulum
        self.clock = clock
        self._stats: dict[str, tuple[str, float | None]] = {}  # full name -> (short name, bitrate)
        self._stats_at = -math.inf
        self._lock = threading.Lock()

    def info(self, node_id: bytes) -> PathInfo:
        """Hops and next-hop interface of the path to node_id; NO_PATH if RNS has none or fails."""
        import RNS
        try:
            if not RNS.Transport.has_path(node_id):
                return NO_PATH
            hops = RNS.Transport.hops_to(node_id)
            hops = hops if isinstance(hops, int) and 0 <= hops < PATHFINDER_M else None
            reticulum = self.reticulum or RNS.Reticulum.get_instance()
            if reticulum is not None and getattr(reticulum, "is_connected_to_shared_instance", False):
                name, bitrate = self._shared_interface(reticulum, reticulum.get_next_hop_if_name(node_id))
            else:
                interface = RNS.Transport.next_hop_interface(node_id)
                name, bitrate = interface_label(interface), _bitrate(getattr(interface, "bitrate", None))
            return PathInfo(hops, name, bitrate)
        except Exception:
            return NO_PATH

    def _shared_interface(self, reticulum, full_name) -> tuple[str | None, float | None]:
        """(short name, bitrate) of the shared instance's interface called full_name (str(interface))."""
        if not isinstance(full_name, str) or not full_name or full_name == "None":
            return None, None
        with self._lock:
            if self.clock() - self._stats_at > STATS_CACHE:
                self._stats = {}
                for entry in (reticulum.get_interface_stats() or {}).get("interfaces", []) or []:
                    if isinstance(entry, dict) and isinstance(entry.get("name"), str):
                        self._stats[entry["name"]] = (str(entry.get("short_name") or entry["name"]),
                                                      _bitrate(entry.get("bitrate")))
                self._stats_at = self.clock()
            return self._stats.get(full_name, (full_name, None))


def _bitrate(value) -> float | None:
    """A usable bitrate, or None."""
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
        return float(value)
    return None


# Interfaces

def interface_names(interface) -> list[str]:
    """Names an interface rule may match: its configured name and str(), and its parent's (the server
    interface a TCP or I2P client connection was spawned by). Empty for an unknown interface."""
    names: list[str] = []
    for candidate in (interface, getattr(interface, "parent_interface", None)):
        if candidate is None:
            continue
        for name in (getattr(candidate, "name", None), str(candidate)):
            if isinstance(name, str) and name and name not in names:
                names.append(name)
    return names


def interface_label(interface) -> str | None:
    """The name traffic is counted under: the parent's for a spawned interface, so client connections to
    one TCP server interface count together."""
    if interface is None:
        return None
    named = getattr(interface, "parent_interface", None) or interface
    name = getattr(named, "name", None)
    return name if isinstance(name, str) and name else str(named)


def matches(names: list[str], patterns: list[str]) -> bool:
    """True if any name equals or starts with any pattern, ignoring case."""
    folded = [name.casefold() for name in names]
    return any(name.startswith(pattern.casefold()) for pattern in patterns if pattern for name in folded)


def interface_allowed(names: list[str], serve: list[str], no_serve: list[str]) -> bool:
    """Whether /blocks may be served on an interface with these names: not on no_serve, and on serve if it
    is set. An unknown interface (no names) is served only while serve is empty."""
    if matches(names, no_serve):
        return False
    return not serve or matches(names, serve)


# Quiet hours

def parse_quiet_hours(text: str) -> list[tuple[int, int]]:
    """Ranges of minutes since midnight from "HH:MM-HH:MM[,HH:MM-HH:MM...]"; [] for "". Raises ValueError."""
    if not isinstance(text, str):
        raise ValueError("quiet_hours must be a string like 22:00-06:00")
    if not text.strip():
        return []
    ranges = []
    for part in text.split(","):
        part = part.strip()
        try:
            start, end = (_minute(value) for value in part.split("-"))
        except ValueError:
            raise ValueError(f"quiet_hours: {part!r} is not HH:MM-HH:MM") from None
        if start == end:
            raise ValueError(f"quiet_hours: {part!r} is empty")
        ranges.append((start, end))
    return ranges


def _minute(text: str) -> int:
    """Minutes since midnight of "HH:MM". Raises ValueError."""
    hours, minutes = text.strip().split(":")
    if not (hours.isdigit() and minutes.isdigit() and len(minutes) == 2 and 1 <= len(hours) <= 2):
        raise ValueError(text)
    hour, minute = int(hours), int(minutes)
    if hour > 23 or minute > 59:
        raise ValueError(text)
    return hour * 60 + minute


def in_quiet_hours(ranges: list[tuple[int, int]], minute: int) -> bool:
    """True if minute (since midnight) is in any range [start, end), which may wrap past midnight."""
    for start, end in ranges:
        if start < end and start <= minute < end:
            return True
        if start > end and (minute >= start or minute < end):
            return True
    return False


def local_minute() -> int:
    """Minutes since local midnight now."""
    now = time.localtime()
    return now.tm_hour * 60 + now.tm_min


# Upload budget

class TokenBucket:
    """capacity bytes per period, refilled continuously. Thread-safe.

    A response is allowed if the bucket holds at least its size, or is full: so one response larger than
    the whole budget is still sent once the bucket has refilled (leaving it in debt), rather than never.
    """

    def __init__(self, capacity: float, period: float = HOUR, clock: Callable[[], float] = time.monotonic):
        """A full bucket."""
        self.capacity = float(capacity)
        self.rate = self.capacity / period
        self.clock = clock
        self.tokens = self.capacity
        self.updated = clock()
        self.lock = threading.Lock()

    def _refill(self):
        """Add what has accrued since the last update. Caller holds the lock."""
        now = self.clock()
        self.tokens = min(self.capacity, self.tokens + max(0.0, now - self.updated) * self.rate)
        self.updated = now

    def available(self) -> float:
        """Bytes in the bucket now (negative while in debt)."""
        with self.lock:
            self._refill()
            return self.tokens

    def take(self, nbytes: int) -> bool:
        """Spend nbytes if allowed (see the class). False, spending nothing, if not."""
        with self.lock:
            self._refill()
            if self.tokens >= nbytes or self.tokens >= self.capacity:
                self.tokens -= nbytes
                return True
            return False

    def give_back(self, nbytes: int):
        """Return bytes spent on a response that wasn't sent after all."""
        with self.lock:
            self._refill()
            self.tokens = min(self.capacity, self.tokens + nbytes)

    def reserve(self, nbytes: int, reservation: int, committed: int = 0) -> bool:
        """Spend reservation of a response of nbytes if the response is allowed (see the class), counting
        committed bytes (owed by responses in flight but not yet spent) as gone. False, spending nothing, if not."""
        with self.lock:
            self._refill()
            if self.tokens - committed >= nbytes or (self.tokens >= self.capacity and committed <= 0):
                self.tokens -= reservation
                return True
            return False

    def charge(self, nbytes: int):
        """Spend nbytes that have been sent, into debt if need be."""
        with self.lock:
            self._refill()
            self.tokens -= nbytes


class AirtimePolicy:
    """The airtime controls on /blocks: upload_budget, serve_interfaces / no_serve_interfaces, quiet_hours.

    clock is monotonic seconds (the bucket); minute_of_day gives local minutes since midnight (quiet hours).
    """

    def __init__(self, config, clock: Callable[[], float] = time.monotonic,
                 minute_of_day: Callable[[], int] = local_minute):
        """Controls from config's mesh settings (validated by Config.validate)."""
        self.serve = list(config.serve_interfaces)
        self.no_serve = list(config.no_serve_interfaces)
        self.quiet = parse_quiet_hours(config.quiet_hours)
        self.minute_of_day = minute_of_day
        self.bucket = TokenBucket(config.upload_budget, HOUR, clock) if config.upload_budget else None

    def refusal(self, interface_names_: list[str]) -> str | None:
        """Why a /blocks request arriving on an interface with these names is refused now, or None."""
        if (self.serve or self.no_serve) and not interface_allowed(interface_names_, self.serve, self.no_serve):
            return REFUSED_INTERFACE
        if self.quiet and in_quiet_hours(self.quiet, self.minute_of_day()):
            return REFUSED_QUIET
        # Nothing at all left: refuse before building a response (spend() decides once its size is known)
        if self.bucket is not None and self.bucket.available() <= 0:
            return REFUSED_BUDGET
        return None

    def limit(self, limit: int, committed: int = 0) -> int:
        """A response's block budget (after its first block) cut to what the upload budget holds less what
        responses in flight are still committed to send, unless the bucket is full and nothing is committed
        (when any one response may go)."""
        if self.bucket is None:
            return limit
        available = self.bucket.available()
        if available >= self.bucket.capacity and committed <= 0:
            return limit
        return max(0, min(limit, int(available - committed) - RESPONSE_OVERHEAD))

    def spend(self, nbytes: int) -> bool:
        """Charge a response of nbytes to the upload budget. False if it must be refused."""
        return self.bucket is None or self.bucket.take(nbytes)

    def reserve(self, nbytes: int, reservation: int, committed: int = 0) -> bool:
        """Admit a response of nbytes, charging reservation of it now (the rest is charged as it is sent).
        False if it must be refused; committed is what responses in flight are still committed to send."""
        return self.bucket is None or self.bucket.reserve(nbytes, reservation, committed)

    def charge(self, nbytes: int):
        """Charge nbytes of an admitted response as they are sent."""
        if self.bucket is not None and nbytes > 0:
            self.bucket.charge(nbytes)

    def refund(self, nbytes: int):
        """A charged response wasn't sent."""
        if self.bucket is not None and nbytes:
            self.bucket.give_back(nbytes)

    def remaining(self) -> int | None:
        """Bytes left in the upload budget, or None without one."""
        return None if self.bucket is None else int(self.bucket.available())


def validate(config):
    """Raise ValueError for invalid mesh settings (called by Config.validate)."""
    budget = config.upload_budget
    if budget is not None and (not isinstance(budget, int) or isinstance(budget, bool) or budget <= 0):
        raise ValueError("upload_budget must be a positive number of bytes per hour, or unset")
    for name in ("serve_interfaces", "no_serve_interfaces"):
        value = getattr(config, name)
        if not isinstance(value, list) or not all(isinstance(item, str) and item.strip() for item in value):
            raise ValueError(f"{name} must be a list of interface names or name prefixes")
    parse_quiet_hours(config.quiet_hours)


def shared_instance_warning(config, reticulum) -> str | None:
    """A warning if interface rules are set while attached to a shared instance, where every link arrives
    on the local shared-instance interface, so the rules can't tell real interfaces apart."""
    if (config.serve_interfaces or config.no_serve_interfaces) and \
            getattr(reticulum, "is_connected_to_shared_instance", False):
        return ("serve_interfaces / no_serve_interfaces see only the shared instance's local interface "
                "while attached to a shared instance; set them up in rnsd (or run rnfs as the instance)")
    return None


# Traffic counters

class _Peer:
    """Counters for one node."""

    __slots__ = ("sent", "received", "requests", "answered", "refused", "failed", "bad", "mode", "last_heard",
                 "last_active")

    def __init__(self):
        """Nothing counted yet."""
        self.sent = self.received = 0
        self.requests = self.answered = self.refused = self.failed = self.bad = 0
        self.mode: str | None = None  # "server" or "client", from its announces
        self.last_heard: float | None = None  # wall time of its last announce
        self.last_active: float | None = None  # wall time of the last traffic with it

    def as_dict(self) -> dict:
        """JSON-ready counters."""
        return {name: getattr(self, name) for name in self.__slots__}


class Traffic:
    """Bytes and requests per peer, per interface and in total since start. Thread-safe, bounded.

    Peers and interfaces are kept in LRU order and capped (MAX_PEERS, MAX_INTERFACES), so a flood of
    identities or spawned interfaces costs a fixed amount of memory. clock is wall time (for display).
    """

    def __init__(self, max_peers: int = MAX_PEERS, max_interfaces: int = MAX_INTERFACES,
                 clock: Callable[[], float] = time.time):
        """Empty counters."""
        self.max_peers = max_peers
        self.max_interfaces = max_interfaces
        self.clock = clock
        self.started = clock()
        self._peers: OrderedDict[bytes, _Peer] = OrderedDict()
        self._interfaces: OrderedDict[str, dict[str, int]] = OrderedDict()
        self._totals = {"sent": 0, "received": 0, "responses": 0, "requests": 0, "answered": 0, "refused": 0,
                        "failed": 0}
        self._refusals = dict.fromkeys(REFUSALS, 0)
        self._lock = threading.Lock()

    def _peer(self, node_id: bytes) -> _Peer:
        """node_id's counters, made most recent (created, evicting the oldest, if new). Caller holds the lock."""
        peer = self._peers.get(node_id)
        if peer is None:
            peer = self._peers[node_id] = _Peer()
            while len(self._peers) > self.max_peers:
                self._peers.popitem(last=False)
        else:
            self._peers.move_to_end(node_id)
        return peer

    def _interface(self, name: str | None, field: str, nbytes: int):
        """Count nbytes against an interface. Caller holds the lock."""
        if name is None:
            return
        counters = self._interfaces.get(name)
        if counters is None:
            counters = self._interfaces[name] = {"sent": 0, "received": 0}
            while len(self._interfaces) > self.max_interfaces:
                self._interfaces.popitem(last=False)
        else:
            self._interfaces.move_to_end(name)
        counters[field] += nbytes

    def sent(self, node_id: bytes | None, interface: str | None, nbytes: int):
        """A /blocks response of nbytes was handed to RNS (node_id None: an unidentified requester)."""
        with self._lock:
            self._totals["sent"] += nbytes
            self._totals["responses"] += 1
            self._interface(interface, "sent", nbytes)
            if node_id is not None:
                peer = self._peer(node_id)
                peer.sent += nbytes
                peer.last_active = self.clock()

    def request(self, node_id: bytes, outcome: str, received: int = 0, interface: str | None = None,
                bad: int = 0):
        """A request we sent to node_id ended with outcome (ANSWERED, REFUSED, FAILED), bringing received
        verified block bytes and bad blocks not matching their CID."""
        with self._lock:
            self._totals["requests"] += 1
            self._totals[outcome] += 1
            self._totals["received"] += received
            self._interface(interface, "received", received)
            peer = self._peer(node_id)
            peer.requests += 1
            setattr(peer, outcome, getattr(peer, outcome) + 1)
            peer.received += received
            peer.bad += bad
            peer.last_active = self.clock()

    def heard(self, node_id: bytes, mode: str):
        """node_id announced itself in DHT mode ("server" or "client")."""
        with self._lock:
            peer = self._peer(node_id)
            peer.mode = mode
            peer.last_heard = self.clock()

    def refused(self, reason: str):
        """A /blocks request was refused by an airtime control (one of REFUSALS)."""
        with self._lock:
            self._refusals[reason] = self._refusals.get(reason, 0) + 1

    def peers(self) -> dict[bytes, dict]:
        """Every tracked peer's counters, least recently active first."""
        with self._lock:
            return {node_id: peer.as_dict() for node_id, peer in self._peers.items()}

    def peer(self, node_id: bytes) -> dict | None:
        """node_id's counters, or None (without making it more recent)."""
        with self._lock:
            peer = self._peers.get(node_id)
            return None if peer is None else peer.as_dict()

    def snapshot(self) -> dict:
        """Totals, refusals and per-interface totals since start."""
        with self._lock:
            return {"since": self.started, "totals": dict(self._totals), "refused": dict(self._refusals),
                    "interfaces": {name: dict(counters) for name, counters in self._interfaces.items()}}
