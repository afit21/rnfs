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

At most MAX_LOCAL_SERVERS are kept, and the judgements of at most MAX_JUDGED servers (the least recently judged
forgotten first): an island has tens of servers, but local_interfaces may name a busy hub.
"""
import threading
from collections import OrderedDict
from collections.abc import Callable, Mapping

from rnfs.dht.routing import distance

LOCAL_FIRST = 1  # local servers a search asks first when local_interfaces names the island's interfaces
LOCAL_FIRST_RADIUS = 3  # ... when local is a hop radius (the study: 97-98% found locally against 80% with one)
MAX_LOCAL_SERVERS = 1024  # local servers kept; the least recently judged local forgotten first
MAX_JUDGED = 4096  # servers whose judgement is kept (local or not); the least recently judged forgotten first
SCAN_INTERVAL = 600.0  # seconds between reads of the path table for the local servers


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

