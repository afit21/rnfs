"""Being considerate on shared radio meshes (the mesh feature: specification.md section 3, upload budget,
interface rules and quiet hours; design.md, "Mesh").

- Provider choice: within each seniority group, providers with a better local score (its band, rnfs.dht.scores)
  are tried first, then those fewer hops away, then those whose next hop goes out on a faster interface
  (provider_rank). RNS is reached only through RnsPaths, which the
  node hands to the fetcher as path_info, so the fetcher stays testable without RNS.
- Airtime controls for /blocks (AirtimePolicy): a node-wide upload budget (a token bucket), interface rules,
  quiet hours and, on a slow node, its radio's measured airtime (rnfs.network.channel). /find, /provide and /pin are tiny and never refused by them.
- Traffic: bounded, thread-safe counters of bytes and requests per peer and per interface, for `rnfs peers`,
  `rnfs stats` and `rnfs status`, with each peer's last link round-trip time and rate estimate and the last sign it
  was up, from which online() tells whether it is online.
- Slow paths (SlowPaths, specification section 6): a node that isn't slow asks servers behind a slow path last in its
  lookups, so LoRa islands carry little of the internet's lookup traffic. A path of more than SLOW_PATH_HOPS hops is
  slow, and so is a server whose last link from this node, within SLOW_LINK_MEMORY, had a round trip above
  SLOW_PATH_RTT and a rate estimate below slow_bitrate (RTT alone would catch I2P, which costs no radio airtime).
- Local servers (is_local_path, specification section 6, "LoRa islands"): a server is local when the next-hop interface
  of the path to it is named in local_interfaces, or else (none named) when it is within local_hops over an interface
  below slow_bitrate. RnsPaths.scan reads every path within a hop count at once (through a shared instance, one path
  table RPC), for the node's local-server set.

Pure policy with injected clocks; nothing here imports RNS (or rnfs.log, which does) at module level, since
the CLI imports the peers feature, which uses this module.
"""
import math
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Callable

from rnfs.constants import HOUR, RESPONSE_OVERHEAD

MAX_PEERS = 1000  # peers tracked by Traffic; the least recently active is forgotten beyond this
MAX_INTERFACES = 64  # interfaces tracked by Traffic, likewise
STATS_CACHE = 30.0  # seconds a shared instance's interface stats are reused for bitrate lookups
# Seconds a shared instance's answer for a node's next-hop interface is reused, and how many nodes' answers are
# kept: each answer is an RPC to the instance, about 41 ms over TCP (macOS, Windows), asked per open link and
# per provider at the start of every fetch, and per peer by `rnfs peers`
PATH_CACHE = 15.0
MAX_CACHED_PATHS = 1024
PATHFINDER_M = 128  # RNS.Transport.PATHFINDER_M: what hops_to returns for an unknown path
# Where RNS.Transport.path_table entries keep the hop count and the interface the path came on (RNS's IDX_PT_HOPS,
# IDX_PT_RVCD_IF), for RnsPaths.scan
PATH_TABLE_HOPS = 2
PATH_TABLE_INTERFACE = 5
# Seconds a peer stays online after the last sign it was up (online()). Three link_idle_timeouts (300 s by
# default): a peer just talked to doesn't turn offline the moment its idle link closes, nor between a fetch's
# requests or a republish's. Far below announce_interval (6 h by default), so one announce doesn't keep a node
# shown online for hours after it went away; a node that only announces is shown online for this long after each
ONLINE_WINDOW = 900.0
# Seconds an announce's emission time (its sender's clock) may be off from ours and still count as now
# (Traffic.announced): nodes on a mesh without NTP drift by minutes
EMISSION_SKEW = 300.0
# Slow paths (SlowPaths). Backbone paths take at most 3 hops in the topology study (a node, its hub, the other node's
# hub, the other node); a server behind a gateway and LoRa hops takes more. Deeper backbones need the link signal
SLOW_PATH_HOPS = 3  # a path of more hops is slow
SLOW_PATH_RTT = 1.0  # seconds: a link with a longer round trip and a rate estimate below slow_bitrate is slow
SLOW_LINK_MEMORY = 24 * HOUR  # a link established longer ago says nothing about the path now
# Local servers each key is also stored on at most (local_replicas): the topology study's 5 cost +25% of an island
# publisher's airtime, 3 (the default) +12%
MAX_LOCAL_REPLICAS = 5

# Why a /blocks request was refused by an airtime control (Traffic.refused keys)
REFUSED_BUDGET = "budget"
REFUSED_INTERFACE = "interface"
REFUSED_QUIET = "quiet_hours"
REFUSED_AIRTIME = "airtime"  # a slow node's radio measured its own airtime over budget (rnfs.network.channel)
REFUSALS = (REFUSED_BUDGET, REFUSED_INTERFACE, REFUSED_QUIET, REFUSED_AIRTIME)

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
    # Every name of the next-hop interface an interface rule may match (local_interfaces); not compared, as a node's
    # path is the same path whichever names were read for it
    names: tuple[str, ...] = field(default=(), compare=False)


NO_PATH = PathInfo()


def no_path_info(node_id: bytes) -> PathInfo:
    """The path_info of a node without Reticulum (offline test nodes): nothing is known."""
    return NO_PATH


def provider_rank(senior: bool, path: PathInfo, order: int, score_band: int = 0) -> tuple:
    """Sort key for providers: senior first (seniority stays the first key: a score can be earned quickly by
    serving popular blocks, an age can't); then a higher local score band; then fewer hops, then a faster
    next-hop interface (unknown last for both); then the order they were found in."""
    hops = path.hops if path.hops is not None else math.inf
    bitrate = -path.bitrate if path.bitrate else math.inf
    return (0 if senior else 1, -score_band, hops, bitrate, order)


class RnsPaths:
    """path_info backed by RNS.Transport (or, attached to a shared instance, by its RPC). Thread-safe.

    In a shared-instance client, next_hop_interface is the local interface to the shared instance, so the
    interface name comes from the instance (Reticulum.get_next_hop_if_name, cached per node for PATH_CACHE) and
    its bitrate from the instance's interface stats, cached for STATS_CACHE. Whether there is a path, and its
    hops, are always read fresh (they are local); only the interface may be up to PATH_CACHE old.
    """

    def __init__(self, reticulum=None, clock: Callable[[], float] = time.monotonic):
        """Paths through reticulum (default: the process's RNS.Reticulum instance)."""
        self.reticulum = reticulum
        self.clock = clock
        self._stats: dict[str, tuple[str, float | None]] = {}  # full name -> (short name, bitrate)
        self._stats_at = -math.inf
        self._lock = threading.Lock()
        # node ID -> (when asked, the instance's next-hop interface name), oldest first: an entry is moved to
        # the end when stored, so expired entries and those evicted beyond MAX_CACHED_PATHS are at the front
        self._names: OrderedDict[bytes, tuple[float, str | None]] = OrderedDict()
        self._names_lock = threading.Lock()
        from rnfs.log import ThrottledLog  # here, not at module level: it loads RNS
        # RNS failing to answer is a bug or an RNS change, asked about per provider and per peer: a line a minute
        self._failure_log = ThrottledLog()

    def hops(self, node_id: bytes) -> int | None:
        """Hops of the path to node_id, or None if RNS has none (or fails). Local, even attached to a shared instance:
        no RPC, so lookups may ask it for every node they learn of."""
        import RNS
        try:
            return _hops(RNS.Transport, node_id) if RNS.Transport.has_path(node_id) else None
        except Exception as e:  # noqa: BLE001 - RNS internals: no path known if they fail; logged
            self._failure_log(f"couldn't read the path to {RNS.prettyhexrep(node_id)} from RNS: {e!r}", RNS.LOG_WARNING)
            return None

    def info(self, node_id: bytes) -> PathInfo:
        """Hops and next-hop interface of the path to node_id; NO_PATH if RNS has none or fails."""
        import RNS
        try:
            if not RNS.Transport.has_path(node_id):
                return NO_PATH
            hops = _hops(RNS.Transport, node_id)
            reticulum = self._shared_instance()
            if reticulum is not None:
                full_name = self._next_hop_name(reticulum, node_id)
                name, bitrate = self._shared_interface(reticulum, full_name)
                names = tuple(dict.fromkeys(n for n in (name, full_name) if isinstance(n, str) and n))
            else:
                interface = RNS.Transport.next_hop_interface(node_id)
                name, bitrate = interface_label(interface), _bitrate(getattr(interface, "bitrate", None))
                names = tuple(interface_names(interface))
            return PathInfo(hops, name, bitrate, names)
        except Exception as e:  # noqa: BLE001 - RNS internals: no path known if they fail; logged
            # Inside the except, so the debug log has the traceback
            self._failure_log(f"couldn't read the path to {RNS.prettyhexrep(node_id)} from RNS: {e!r}", RNS.LOG_WARNING)
            return NO_PATH

    def infos(self, node_ids: list[bytes]) -> dict[bytes, PathInfo]:
        """info() of each of node_ids. Attached to a shared instance, the next-hop interfaces not cached are
        read from one path table RPC (Reticulum.get_path_table) instead of one RPC per node."""
        import RNS
        try:
            reticulum = self._shared_instance()
            if reticulum is not None:
                now = self.clock()
                with self._names_lock:
                    missing = {node_id for node_id in node_ids if not self._cached(node_id, now)}
                missing = {node_id for node_id in missing if RNS.Transport.has_path(node_id)}
                # One RPC either way for a single node: its own answer is the smaller one
                if len(missing) > 1:
                    table = reticulum.get_path_table() or []
                    now = self.clock()
                    for entry in table:
                        node_id = entry.get("hash") if isinstance(entry, dict) else None
                        if node_id in missing:
                            # The "interface" here is str() of the same path table entry's interface that
                            # get_next_hop_if_name answers with; a node not in the table is asked on its own
                            self._remember(node_id, entry.get("interface"), now)
        except Exception:  # noqa: BLE001 - RNS internals: info() asks node by node if the table fails
            pass
        return {node_id: self.info(node_id) for node_id in node_ids}

    def scan(self, max_hops: int | None = None) -> dict[bytes, PathInfo]:
        """The PathInfo of every destination RNS has a path to of at most max_hops hops (None: any), from one read of
        the path table: attached to a shared instance, the instance's (one get_path_table RPC, its interfaces named
        and rated from its interface stats), whose paths a client's own table only learns as it uses them. Raises
        what RNS or the RPC raises (the caller then takes nothing to be local)."""
        import RNS
        reticulum = self._shared_instance()
        found: dict[bytes, PathInfo] = {}
        if reticulum is not None:
            for entry in reticulum.get_path_table(max_hops=max_hops) or []:
                if not isinstance(entry, dict) or not isinstance(entry.get("hash"), bytes):
                    continue
                hops = entry.get("hops")
                if not isinstance(hops, int) or isinstance(hops, bool) or not 0 <= hops < PATHFINDER_M:
                    continue
                full_name = entry.get("interface")
                name, bitrate = self._shared_interface(reticulum, full_name)
                names = tuple(dict.fromkeys(n for n in (name, full_name) if isinstance(n, str) and n))
                found[bytes(entry["hash"])] = PathInfo(hops, name, bitrate, names)
            return found
        for node_id, entry in list(RNS.Transport.path_table.items()):
            hops, interface = entry[PATH_TABLE_HOPS], entry[PATH_TABLE_INTERFACE]
            if not isinstance(hops, int) or not 0 <= hops < PATHFINDER_M or (max_hops is not None and hops > max_hops):
                continue
            found[bytes(node_id)] = PathInfo(hops, interface_label(interface),
                                             _bitrate(getattr(interface, "bitrate", None)),
                                             tuple(interface_names(interface)))
        return found

    def _shared_instance(self):
        """The Reticulum instance if it is attached to a shared instance (RPC answers paths), else None."""
        import RNS
        reticulum = self.reticulum or RNS.Reticulum.get_instance()
        if reticulum is not None and getattr(reticulum, "is_connected_to_shared_instance", False):
            return reticulum
        return None

    def _next_hop_name(self, reticulum, node_id: bytes):
        """The shared instance's name for node_id's next-hop interface, asked at most once per PATH_CACHE."""
        with self._names_lock:
            cached = self._cached(node_id, self.clock())
        if cached is not None:
            return cached[1]
        # Asked outside the lock: an RPC can take 41 ms, and other nodes' cached answers needn't wait for it
        name = reticulum.get_next_hop_if_name(node_id)
        self._remember(node_id, name, self.clock())
        return name

    def _cached(self, node_id: bytes, now: float) -> tuple[float, str | None] | None:
        """node_id's cached (when, name) if younger than PATH_CACHE, else None. Caller holds _names_lock."""
        cached = self._names.get(node_id)
        return cached if cached is not None and now - cached[0] <= PATH_CACHE else None

    def _remember(self, node_id: bytes, name, now: float):
        """Cache the instance's next-hop interface name for node_id, dropping expired and excess entries."""
        with self._names_lock:
            self._names[node_id] = (now, name)
            self._names.move_to_end(node_id)
            while self._names:
                oldest = next(iter(self._names.values()))
                if len(self._names) <= MAX_CACHED_PATHS and now - oldest[0] <= PATH_CACHE:
                    break
                self._names.popitem(last=False)

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


def _hops(transport, node_id: bytes) -> int | None:
    """transport.hops_to(node_id) (RNS.Transport's) if it is a hop count, else None (PATHFINDER_M: no path)."""
    hops = transport.hops_to(node_id)
    return hops if isinstance(hops, int) and 0 <= hops < PATHFINDER_M else None


class SlowPaths:
    """Whether a server is behind a slow path (specification section 6): its path has more than SLOW_PATH_HOPS hops
    (hops(node_id): RnsPaths.hops, None without a path), or this node's last link to it, established within
    SLOW_LINK_MEMORY, had a round trip above SLOW_PATH_RTT and a rate estimate below slow_bitrate (traffic's,
    Traffic.slow_link). Neither known: not slow. Called with a node ID; lookups call it once per node they learn of.

    Only an RNS transport node on the path can raise its hop count (announce signatures don't cover hops) or delay a
    link to inflate its round trip, and such a node could already drop the traffic; a lookup only asks such servers
    later, so a hub that makes the servers behind it look slow can't hide them."""

    def __init__(self, traffic: "Traffic | None", slow_bitrate: float,
                 hops: Callable[[bytes], int | None] = lambda node_id: None):
        """Slow paths judged from hops (the node sets RnsPaths.hops once Reticulum runs) and traffic's links."""
        self.traffic = traffic
        self.slow_bitrate = slow_bitrate
        self.hops = hops

    def __call__(self, node_id: bytes) -> bool:
        """True if node_id is behind a slow path."""
        hops = self.hops(node_id)
        if hops is not None and hops > SLOW_PATH_HOPS:
            return True
        return self.traffic is not None and self.traffic.slow_link(node_id, self.slow_bitrate)


def is_local_path(path: PathInfo, interfaces: list[str], max_hops: int, slow_bitrate: float) -> bool:
    """Whether a server whose path is path is local (specification section 6, "LoRa islands"): its next-hop interface
    is named in interfaces (names or prefixes, matched as serve_interfaces is), or, if none are named, it is within
    max_hops hops and that interface runs below slow_bitrate. Judged from the path to the server, never from the
    interface a request of its arrived on. No path, or an unknown interface or bitrate: not local."""
    if path.hops is None:
        return False
    if interfaces:
        names = list(path.names) or ([path.interface] if path.interface else [])
        return matches(names, interfaces)
    return path.hops <= max_hops and path.bitrate is not None and path.bitrate < slow_bitrate


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
    if not (hours.isascii() and minutes.isascii() and hours.isdigit() and minutes.isdigit() and len(minutes) == 2
            and 1 <= len(hours) <= 2):  # isdigit alone takes other scripts' digits
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
    """The airtime controls on /blocks: upload_budget, serve_interfaces / no_serve_interfaces, quiet_hours, and on a
    slow node its radio's measured airtime (over_airtime: rnfs.network.channel.ChannelLoad.over_budget, set by the node).

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
        self.over_airtime: Callable[[], bool] | None = None  # True while the radio measures this node over budget

    def refusal(self, interface_names_: list[str]) -> str | None:
        """Why a /blocks request arriving on an interface with these names is refused now, or None."""
        if (self.serve or self.no_serve) and not interface_allowed(interface_names_, self.serve, self.no_serve):
            return REFUSED_INTERFACE
        if self.quiet and in_quiet_hours(self.quiet, self.minute_of_day()):
            return REFUSED_QUIET
        if self.over_airtime is not None and self.over_airtime():
            return REFUSED_AIRTIME
        # Nothing at all left: refuse before building a response (RequestServer._admit, through reserve(),
        # decides once its size is known)
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
    # At most 64 bits, as rnfs.toml holds: a larger one passed, and the token bucket's float of it raised (REL-17)
    if budget is not None and (not isinstance(budget, int) or isinstance(budget, bool) or not 0 < budget < 2 ** 63):
        raise ValueError("upload_budget must be a positive number of bytes per hour, or unset")
    for name in ("serve_interfaces", "no_serve_interfaces", "local_interfaces"):
        value = getattr(config, name)
        if not isinstance(value, list) or not all(isinstance(item, str) and item.strip() for item in value):
            raise ValueError(f"{name} must be a list of interface names or name prefixes")
    parse_quiet_hours(config.quiet_hours)
    if type(config.local_hops) is not int or config.local_hops < 1:
        raise ValueError("local_hops must be a whole number of at least 1")
    if type(config.local_replicas) is not int or not 0 <= config.local_replicas <= MAX_LOCAL_REPLICAS:
        raise ValueError(f"local_replicas must be a whole number, 0-{MAX_LOCAL_REPLICAS}")


def shared_instance_warning(config, reticulum) -> str | None:
    """A warning if interface rules are set while attached to a shared instance, where every link arrives
    on the local shared-instance interface, so the rules can't tell real interfaces apart."""
    if (config.serve_interfaces or config.no_serve_interfaces) and \
            getattr(reticulum, "is_connected_to_shared_instance", False):
        return ("serve_interfaces / no_serve_interfaces see only the shared instance's local interface "
                "while attached to a shared instance; set them up in rnsd (or run rnfs as the instance)")
    return None


# Traffic counters

def online(link: bool, last_seen: float | None, now: float) -> bool:
    """Whether a peer counts as online (`rnfs peers`, "peer" events): an rnfs link to it is open, or the last sign
    it was up (Traffic: last_seen, monotonic like now) is at most ONLINE_WINDOW old; a negative age (a clock that
    went back) counts as offline. A guess from what the node already knows, without network I/O: a node up but
    silent shows offline (ping tells for sure), and one that went away shows online until the window passes or
    its link closes."""
    return link or (last_seen is not None and 0 <= now - last_seen <= ONLINE_WINDOW)


class _Peer:
    """Counters for one node. Times shown to users are wall time; those decisions are made from (last_seen,
    first_counted) are Traffic.monotonic's, so a step of the wall clock can't turn peers online, offline or new."""

    __slots__ = ("sent", "received", "requests", "answered", "refused", "failed", "bad", "mode", "last_heard",
                 "last_active", "first_heard", "first_counted", "last_seen", "last_emitted", "rtt", "rtt_at",
                 "link_at", "link_rate")

    def __init__(self, now: float, monotonic: float):
        """Nothing counted yet; first counted at now (wall time), monotonic (Traffic.monotonic)."""
        self.sent = self.received = 0
        self.requests = self.answered = self.refused = self.failed = self.bad = 0
        self.mode: str | None = None  # "server" or "client", from its announces
        self.last_heard: float | None = None  # wall time of its last announce (or path response)
        self.last_active: float | None = None  # wall time of the last traffic with it
        self.first_heard = now  # wall time it was first counted (this run, or since it was last forgotten)
        self.first_counted = monotonic  # the same, monotonic: what "new" peer events are decided from
        # Monotonic time of the last sign it was up: an announce emitted recently (Traffic.announced), an answer to a
        # request (a refusal too), a request of its own served, or a link established
        self.last_seen: float | None = None
        self.last_emitted: float | None = None  # emission time (its clock) of the newest announce counted
        self.rtt: float | None = None  # seconds: the round trip RNS measured on the last link established to it
        self.rtt_at: float | None = None  # wall time it was measured
        self.link_at: float | None = None  # monotonic time that link was established (slow_link)
        self.link_rate: float | None = None  # bits/s: that link's rate estimate (section 2), None until measured

    def as_dict(self) -> dict:
        """JSON-ready counters."""
        return {name: getattr(self, name) for name in self.__slots__}


class Traffic:
    """Bytes and requests per peer, per interface and in total since start. Thread-safe, bounded.

    Peers and interfaces are kept in LRU order and capped (MAX_PEERS, MAX_INTERFACES), so a flood of
    identities or spawned interfaces costs a fixed amount of memory. clock is wall time (for display, and for
    announces' emission times); monotonic is what online() and "new" peers are decided from.
    """

    def __init__(self, max_peers: int = MAX_PEERS, max_interfaces: int = MAX_INTERFACES,
                 clock: Callable[[], float] = time.time, monotonic: Callable[[], float] = time.monotonic):
        """Empty counters."""
        self.max_peers = max_peers
        self.max_interfaces = max_interfaces
        self.clock = clock
        self.monotonic = monotonic
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
            peer = self._peers[node_id] = _Peer(self.clock(), self.monotonic())
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
                peer.last_seen = self.monotonic()  # it just asked

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
            if outcome != FAILED:  # it answered, if only to refuse
                peer.last_seen = self.monotonic()

    def heard(self, node_id: bytes, mode: str):
        """node_id announced itself in DHT mode ("server" or "client"), or a path response for it arrived."""
        with self._lock:
            peer = self._peer(node_id)
            peer.mode = mode
            peer.last_heard = self.clock()

    def announced(self, node_id: bytes, emitted: float | None) -> bool:
        """An announce of node_id's own arrived, emitted at emitted (Unix time on its clock, signed; None if
        unknown). True if it counts as a sign node_id was up: emitted later than the newest counted before (a replay
        doesn't count twice) and at most ONLINE_WINDOW ago, allowing EMISSION_SKEW of clock difference. An announce
        delivered late (held in an announce queue, or a path answer a shared instance passes on as an announce) says
        nothing of now. It is seen when it was emitted (less the skew, never later than now), not when it arrived."""
        if not isinstance(emitted, (int, float)) or isinstance(emitted, bool) or not math.isfinite(emitted):
            return False
        with self._lock:
            peer = self._peer(node_id)
            if peer.last_emitted is not None and emitted <= peer.last_emitted:
                return False
            peer.last_emitted = float(emitted)
            age = max(0.0, self.clock() - emitted - EMISSION_SKEW)
            if age > ONLINE_WINDOW:
                return False
            seen = self.monotonic() - age
            if peer.last_seen is None or seen > peer.last_seen:
                peer.last_seen = seen
            return True

    def measured(self, node_id: bytes, rtt: float):
        """A link to node_id was established, RNS measuring a round trip of rtt seconds. It has no rate estimate
        yet."""
        with self._lock:
            peer = self._peer(node_id)
            peer.rtt = float(rtt)
            peer.rtt_at = self.clock()
            peer.last_seen = peer.link_at = self.monotonic()
            peer.link_rate = None

    def rated(self, node_id: bytes, rate: float):
        """The link to node_id got a rate estimate of rate bits/s (section 2): the last link's, for slow_link."""
        with self._lock:
            self._peer(node_id).link_rate = float(rate)

    def slow_link(self, node_id: bytes, slow_bitrate: float) -> bool:
        """True if the last link to node_id, established within SLOW_LINK_MEMORY, had a round trip above
        SLOW_PATH_RTT and a rate estimate below slow_bitrate (SlowPaths). Doesn't make the peer more recent."""
        with self._lock:
            peer = self._peers.get(node_id)
            if peer is None or peer.rtt is None or peer.link_rate is None or peer.link_at is None:
                return False
            if not 0 <= self.monotonic() - peer.link_at <= SLOW_LINK_MEMORY:
                return False
            return peer.rtt > SLOW_PATH_RTT and peer.link_rate < slow_bitrate

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
