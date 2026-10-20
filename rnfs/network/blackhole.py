"""Honouring RNS's identity blackhole (specification section 10, "Blackholed identities").

RNS 1.5.6 keeps a list of blackholed identities (Transport.blackholed_identities: identity hash -> {"source",
"until", "reason"}), filled by the instance's operator (`rnpath -B`) and by the blackhole lists it subscribes to
(blackhole_sources), and Reticulum.get_blackholed_identities() returns it, through a shared instance by RPC. RNS itself
drops their announces, removes the paths to their destinations, and tears down an incoming link that identifies as
one. rnfs also, while an identity is listed:

- treats its node ID (the rnfs.dht destination of that identity) as banned: the routing table drops and ignores it,
  lookups leave it out, its provider records aren't handed out or used, and fetches don't ask it
  (rnfs.dht.routing.RoutingTable.blackholed);
- answers False to every request on a link identified as it (rnfs.network.server.RequestServer.blackholed): one that
  identified before the identity was listed, which RNS lets be.

The list is read every REFRESH by the node's maintenance thread, never on the path of a request or an announce, which
check the last list read. An entry whose "until" has passed counts as lifted, as RNS lifts it within a minute.
"""
import math
import threading
import time
from typing import Callable

from rnfs.constants import APP_NAME, DHT_ASPECT, ID_LENGTH
from rnfs.log import LOG_NOTICE, LOG_WARNING, ThrottledLog, log, plural

REFRESH = 300.0  # seconds between reads of the list ("cached for minutes")


def _rns_instance():
    """The running RNS.Reticulum instance, or None."""
    import RNS
    return RNS.Reticulum.get_instance()


def _node_id(identity_hash: bytes) -> bytes:
    """The node ID of the identity with this hash: Destination.hash(identity hash, "rnfs", "dht")."""
    import RNS
    return RNS.Destination.hash(identity_hash, APP_NAME, DHT_ASPECT)


def listed(blackholed, now: float) -> set[bytes]:
    """The identity hashes in what Reticulum.get_blackholed_identities() returned ({identity hash: {"until": Unix time
    or None, ...}}) whose entry hasn't expired at now (Unix time). Malformed entries are left out."""
    if not isinstance(blackholed, dict):
        return set()
    found = set()
    for identity_hash, entry in blackholed.items():
        if not isinstance(identity_hash, bytes) or len(identity_hash) != ID_LENGTH:
            continue
        until = entry.get("until") if isinstance(entry, dict) else None
        if isinstance(until, (int, float)) and not isinstance(until, bool) and math.isfinite(until) and until <= now:
            continue
        found.add(bytes(identity_hash))
    return found


class Blackhole:
    """The identities RNS blackholes and their node IDs, as last read. Thread-safe; the checks never block.

    reticulum() gives the running Reticulum instance (None: nothing is listed); clock is monotonic time and wall_clock
    Unix time (an entry's until)."""

    def __init__(self, reticulum: Callable[[], object] = _rns_instance, clock: Callable[[], float] = time.monotonic,
                 wall_clock: Callable[[], float] = time.time, refresh: float = REFRESH):
        """Nothing listed until the first read."""
        self._reticulum = reticulum
        self.clock = clock
        self.wall_clock = wall_clock
        self.interval = refresh
        self._identities: frozenset[bytes] = frozenset()
        self._nodes: frozenset[bytes] = frozenset()
        self._node_ids: dict[bytes, bytes] = {}  # identity hash -> node ID, for those listed (hashing each once)
        self._read_at = -math.inf
        self._lock = threading.Lock()
        self._failure_log = ThrottledLog()
        self.reads = 0  # reads of the list so far (tests count them)

    def node_listed(self, node_id: bytes) -> bool:
        """True if node_id is the node ID of an identity on the list as last read."""
        return node_id in self._nodes

    def identity_listed(self, identity_hash: bytes) -> bool:
        """True if the identity with this hash is on the list as last read."""
        return identity_hash in self._identities

    def refresh_if_due(self) -> set[bytes]:
        """refresh() if REFRESH has passed since the last read; else nothing (an empty set)."""
        if 0 <= self.clock() - self._read_at < self.interval:
            return set()
        return self.refresh()

    def refresh(self) -> set[bytes]:
        """Read the list now: the node IDs newly listed since the last read (the node drops them from its routing
        table). A list that can't be read keeps the last one."""
        self._read_at = self.clock()
        try:
            reticulum = self._reticulum()
            identities = set() if reticulum is None else listed(reticulum.get_blackholed_identities(),
                                                                self.wall_clock())
        except Exception as e:  # noqa: BLE001 - RNS internals or the instance's RPC: the last list holds; logged
            self._failure_log(f"couldn't read RNS's blackholed identities: {e!r}", LOG_WARNING)
            return set()
        with self._lock:
            node_ids = {identity: self._node_ids.get(identity) or _node_id(identity) for identity in identities}
            nodes = frozenset(node_ids.values())
            added, lifted = nodes - self._nodes, self._nodes - nodes
            self._node_ids, self._identities, self._nodes = node_ids, frozenset(identities), nodes
            self.reads += 1
        if added or lifted:
            log(f"RNS blackholes {plural(len(identities), 'identity')}: their nodes are treated as banned"
                + (f" ({len(added)} newly listed" + (f", {len(lifted)} lifted)" if lifted else ")")
                   if added else f" ({len(lifted)} lifted)"), LOG_NOTICE)
        return set(added)
