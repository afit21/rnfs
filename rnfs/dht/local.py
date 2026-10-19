"""Local servers (specification section 6, "LoRa islands"): the DHT servers on this node's own island, for two-level
records and the local-first search. No RNS dependency: the node judges each server from the path to it
(mesh.is_local_path) and tells LocalServers what it found.

- **Which servers.** A server is local when the next-hop interface of the path to it is named in `local_interfaces`,
  or, with none named, when it is within `local_hops` over an interface below `slow_bitrate`. Judged from the path,
  never from the interface a request arrived on (through a shared instance that is always the local one). The node
  judges a server when its server-mode announce (or a path response) arrives, and judges every server it knows of
  from one read of the path table at start and every SCAN_INTERVAL (replace): a client of a shared instance learns
  paths in its own table only as it uses them. A node with neither local_interfaces nor a slow interface has none.
- **Two-level records.** Each key a node provides is also stored on its `local_replicas` closest local servers (a store
  part of the publisher), so it can be found on the island while the island is cut off from the rest.
- **Local-first search.** A search asks the closest local server first (LOCAL_FIRST; LOCAL_FIRST_RADIUS of them when
  local is a hop radius, which misses some of the island), and its answer seeds the rest of the lookup
  (dht.lookup's `first`). In the topology study this found island content 100% of the time during a partition (55-72%
  before) and cut island-to-island lookup airtime by 42%.

- **Partition mode** (PartitionMode). The servers that aren't local are *off-island*. A node enters partition mode
  after FAILURES_TO_ENTER (3) failures in a row reaching off-island servers (a /find or /provide unanswered, no answer
  from any off-island server between), or after off-island silence (no fresh off-island announce nor answer) longer
  than SILENCE_FACTOR (4) times the usual gap between the off-island announces it heard (their mean over the last
  GAP_SAMPLES, once it has MIN_GAPS; at least MIN_SILENCE) if an off-island probe then fails: with servers announcing
  every 6 h, an island hearing 20 of them hears one every ~18 minutes, so silence alone would flap on a small network.
  In it, lookups and publishes use local servers only (`allows`), so a lookup doesn't spend minutes on timeouts. Any
  off-island answer or fresh off-island announce ends it, and a probe (one /find to an off-island server, the failed
  ones and the recently heard in turn) every PROBE_INTERVAL (slow nodes: SLOW_PROBE_INTERVAL, doubling to
  SLOW_PROBE_MAX, as each probe on radio costs a link's airtime) keeps it from sticking. Leaving it starts the heal
  republish (the node's on_change).

At most MAX_LOCAL_SERVERS are kept, and the judgements of at most MAX_JUDGED servers (the least recently judged
forgotten first): an island has tens of servers, but local_interfaces may name a busy hub.
"""
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Callable, Mapping

from rnfs.dht.routing import distance

LOCAL_FIRST = 1  # local servers a search asks first when local_interfaces names the island's interfaces
LOCAL_FIRST_RADIUS = 3  # ... when local is a hop radius (the study: 97-98% found locally against 80% with one)
MAX_LOCAL_SERVERS = 1024  # local servers kept; the least recently judged local forgotten first
MAX_JUDGED = 4096  # servers whose judgement is kept (local or not); the least recently judged forgotten first
SCAN_INTERVAL = 600.0  # seconds between reads of the path table for the local servers
# Partition mode
FAILURES_TO_ENTER = 3  # failures in a row reaching off-island servers that enter it (the study's PH)
SILENCE_FACTOR = 4  # off-island silence counts once over this many times the usual gap between off-island announces
MIN_SILENCE = 600.0  # seconds: and never under this
GAP_SAMPLES = 32  # the usual gap is the mean of this many recent gaps between off-island announces
MIN_GAPS = 8  # silence doesn't count before this many gaps were measured
PROBE_INTERVAL = 300.0  # seconds between off-island probes in partition mode
SLOW_PROBE_INTERVAL = 900.0  # on slow nodes, the first; doubling after each failed probe up to SLOW_PROBE_MAX
SLOW_PROBE_MAX = 7200.0
RECENT_OFF_ISLAND = 16  # off-island servers remembered as probe targets (heard or answered; and those that failed)


def _never(node_id: bytes) -> bool:
    """Default exclude: excludes nothing."""
    return False


class LocalServers:
    """The local servers and the last judgement of each server judged. Thread-safe."""

    def __init__(self, max_servers: int = MAX_LOCAL_SERVERS, max_judged: int = MAX_JUDGED):
        """No server judged yet: nothing is local."""
        self.max_servers = max_servers
        self.max_judged = max_judged
        self._lock = threading.Lock()
        self._local: OrderedDict[bytes, None] = OrderedDict()  # least recently judged first
        self._judged: OrderedDict[bytes, bool] = OrderedDict()  # node ID -> local; least recently judged first

    def note(self, node_id: bytes, local: bool):
        """node_id, a server, was judged (local or not) from the path to it now."""
        with self._lock:
            self._note(node_id, local)

    def _note(self, node_id: bytes, local: bool):
        """note(), lock held."""
        self._judged[node_id] = local
        self._judged.move_to_end(node_id)
        while len(self._judged) > self.max_judged:
            self._judged.popitem(last=False)
        if local:
            self._local[node_id] = None
            self._local.move_to_end(node_id)
            while len(self._local) > self.max_servers:
                self._local.popitem(last=False)
        else:
            self._local.pop(node_id, None)

    def replace(self, judged: Mapping[bytes, bool]):
        """A scan judged every server it knows of with a path (judged: node ID -> local): the local servers are now
        those it found local; a server it didn't see (no path any more, or beyond the hops scanned) isn't local."""
        with self._lock:
            self._local.clear()
            for node_id, local in judged.items():
                self._note(node_id, local)
            for node_id in [n for n, local in self._judged.items() if local and n not in judged]:
                self._judged[node_id] = False

    def forget(self, node_id: bytes):
        """node_id is no server any more (it announced client mode)."""
        with self._lock:
            self._local.pop(node_id, None)
            self._judged.pop(node_id, None)

    def clear(self):
        """Nothing is local (the path table couldn't be read)."""
        with self._lock:
            self._local.clear()
            self._judged.clear()

    def is_local(self, node_id: bytes) -> bool:
        """True if node_id is a local server."""
        return node_id in self._local  # a single dict lookup: atomic, no lock needed

    def judged(self, node_id: bytes) -> bool | None:
        """node_id's last judgement (True: local), or None if it hasn't been judged (not known as a server)."""
        return self._judged.get(node_id)

    def closest(self, key: bytes, count: int, exclude: Callable[[bytes], bool] = _never) -> list[bytes]:
        """The count local servers closest to key, closest first, leaving out those exclude rejects."""
        if count <= 0:
            return []
        with self._lock:
            nodes = list(self._local)
        return sorted((n for n in nodes if not exclude(n)), key=lambda n: distance(n, key))[:count]

    def nodes(self) -> list[bytes]:
        """Every local server."""
        with self._lock:
            return list(self._local)

    def __len__(self) -> int:
        """How many local servers there are."""
        return len(self._local)


def first_count(named: bool) -> int:
    """How many local servers a search asks first: LOCAL_FIRST when local_interfaces names the island (named), else
    LOCAL_FIRST_RADIUS."""
    return LOCAL_FIRST if named else LOCAL_FIRST_RADIUS


def judge_all(paths: Mapping[bytes, object], is_server: Callable[[bytes], bool],
              is_local: Callable[[object], bool]) -> dict[bytes, bool]:
    """The judgement of every server among paths (node ID -> its mesh.PathInfo, from a scan): is_server(id) says
    which destinations are DHT servers this node knows of, is_local(path) judges a path."""
    return {node_id: bool(is_local(path)) for node_id, path in paths.items() if is_server(node_id)}


def _remember(nodes: OrderedDict, node_id: bytes):
    """Put node_id last in nodes (most recent), keeping at most RECENT_OFF_ISLAND."""
    nodes[node_id] = None
    nodes.move_to_end(node_id)
    while len(nodes) > RECENT_OFF_ISLAND:
        nodes.popitem(last=False)


class PartitionMode:
    """Whether this node is cut off from the servers beyond its island (see the module docstring). Thread-safe.

    is_local(id) says which servers are local; the rest are off-island. The node reports off-island answers
    (answered), failures (failed) and fresh announces (announced), asks probe_due() and probe_target() and reports the
    probe's outcome (probed); on_change(active) is called, without the lock, on entering and leaving. clock is
    monotonic, wall_clock gives since's wall time."""

    def __init__(self, is_local: Callable[[bytes], bool], slow: bool = False,
                 on_change: Callable[[bool], object] = lambda active: None,
                 clock: Callable[[], float] = time.monotonic, wall_clock: Callable[[], float] = time.time):
        """Not in partition mode, nothing heard yet."""
        self.is_local = is_local
        self.slow = slow
        self.on_change = on_change
        self.clock = clock
        self.wall_clock = wall_clock
        self._lock = threading.Lock()
        self.active = False
        self.since: float | None = None  # wall time it was entered, while active
        self.entered = 0  # times entered since start
        self._failures = 0  # off-island failures in a row
        self._last_contact = clock()  # an off-island answer or fresh announce (or the start)
        self._last_announce: float | None = None  # the last fresh off-island announce
        self._clean = False  # no partition mode since _last_announce: the gap to the next one is a usual gap
        self._gaps: deque[float] = deque(maxlen=GAP_SAMPLES)
        self._recent: OrderedDict[bytes, None] = OrderedDict()  # off-island servers heard or answered, oldest first
        self._unreached: OrderedDict[bytes, None] = OrderedDict()  # off-island servers that failed, oldest first
        self._probe_interval = SLOW_PROBE_INTERVAL if slow else PROBE_INTERVAL
        self._next_probe: float | None = None  # in partition mode: when the next probe is due
        self._turn = 0  # probe targets taken in turn

    def allows(self, node_id: bytes) -> bool:
        """Whether a lookup or a publish may use node_id now: any server, or in partition mode local ones only."""
        return not self.active or self.is_local(node_id)

    def answered(self, node_id: bytes):
        """node_id answered a /find or /provide: an off-island answer ends partition mode."""
        if self.is_local(node_id):
            return
        with self._lock:
            self._failures = 0
            self._last_contact = self.clock()
            self._unreached.pop(node_id, None)
            _remember(self._recent, node_id)
            left = self._leave()
        if left:
            self.on_change(False)

    def failed(self, node_id: bytes):
        """A /find or /provide to node_id failed: the FAILURES_TO_ENTER-th off-island failure in a row enters partition
        mode."""
        if self.is_local(node_id):
            return
        with self._lock:
            self._failures += 1
            _remember(self._unreached, node_id)
            entered = self._failures >= FAILURES_TO_ENTER and self._enter()
        if entered:
            self.on_change(True)

    def announced(self, node_id: bytes):
        """A fresh announce of node_id, an off-island server, arrived (emitted within minutes: not a path answer from a
        cache): a sample of the usual gap, and the end of partition mode."""
        with self._lock:
            now = self.clock()
            if self._last_announce is not None and self._clean:
                self._gaps.append(now - self._last_announce)
            self._last_announce = now
            self._last_contact = now
            self._failures = 0
            self._unreached.pop(node_id, None)
            _remember(self._recent, node_id)
            left = self._leave()
            self._clean = True
        if left:
            self.on_change(False)

    def silence_limit(self) -> float | None:
        """Seconds of off-island silence that count (a probe then), or None while too few gaps were measured."""
        with self._lock:
            return self._silence_limit()

    def _silence_limit(self) -> float | None:
        """silence_limit(), lock held."""
        if len(self._gaps) < MIN_GAPS:
            return None
        return max(MIN_SILENCE, SILENCE_FACTOR * sum(self._gaps) / len(self._gaps))

    def probe_due(self) -> bool:
        """Whether an off-island probe is due: in partition mode every probe interval; otherwise once off-island
        silence passes silence_limit()."""
        with self._lock:
            now = self.clock()
            if self.active:
                return self._next_probe is not None and now >= self._next_probe
            limit = self._silence_limit()
            return limit is not None and now - self._last_contact > limit

    def probe_target(self) -> bytes | None:
        """The off-island server to probe: in turn, those that failed and those recently heard or answered (most recent
        first); None if none is known."""
        with self._lock:
            targets = list(reversed(self._unreached)) + [n for n in reversed(self._recent) if n not in self._unreached]
            targets = [n for n in targets if not self.is_local(n)]
            if not targets:
                return None
            self._turn += 1
            return targets[(self._turn - 1) % len(targets)]

    def probed(self, node_id: bytes, answered: bool):
        """The probe of node_id was answered or not. Unanswered after silence, partition mode is entered; in it, the
        next probe is scheduled (on slow nodes later each time)."""
        if answered:
            self.answered(node_id)
            return
        if self.is_local(node_id):
            return
        with self._lock:
            self._failures += 1
            _remember(self._unreached, node_id)
            if self.active:
                if self.slow:
                    self._probe_interval = min(SLOW_PROBE_MAX, 2 * self._probe_interval)
                self._next_probe = self.clock() + self._probe_interval
                return
            entered = self._enter()
        if entered:
            self.on_change(True)

    def no_target(self):
        """A probe was due but no off-island server is known to probe: the node knows only its island, so the silence
        counts from now."""
        with self._lock:
            self._last_contact = self.clock()
            if self.active:
                self._next_probe = self.clock() + self._probe_interval

    def state(self) -> dict:
        """{"partition": whether in partition mode, "partition_since": the wall time it was entered, or None}."""
        with self._lock:
            return {"partition": self.active, "partition_since": self.since}

    def _enter(self) -> bool:
        """Enter partition mode if not in it; True if entered. Lock held."""
        if self.active:
            return False
        self.active = True
        self.since = self.wall_clock()
        self.entered += 1
        self._clean = False
        self._probe_interval = SLOW_PROBE_INTERVAL if self.slow else PROBE_INTERVAL
        self._next_probe = self.clock() + self._probe_interval
        return True

    def _leave(self) -> bool:
        """Leave partition mode if in it; True if left. Lock held."""
        if not self.active:
            return False
        self.active = False
        self.since = None
        self._failures = 0
        self._next_probe = None
        return True

