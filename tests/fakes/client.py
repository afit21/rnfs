"""FakeClient, the PeerClient stand-in the fetcher, publisher, mesh and names tests use. Its methods keep
PeerClient's names and parameters (tests/test_fake_contract.py), so production code calls them as it calls the
real client, without looking up with getattr what a fake might lack."""
import threading

from rnfs.content.cid import Cid
from rnfs.network.client import OK, BlocksResult

BATCH = 4  # CIDs the fake client sends per /blocks request, like a packet limit; the rest come back unsent


class FakeClient:
    """Stands in for PeerClient.

    /blocks: each node holds a dict of blocks, or behaves as a test function says. /provide: each request is
    recorded; nodes in refuse record nothing, a node in limit records only its first keys.
    """

    def __init__(self):
        """No nodes, no open links, no requests yet."""
        # /blocks and links
        self.holdings: dict[bytes, dict[Cid, bytes]] = {}
        self.behaviour: dict[bytes, object] = {}  # node -> f(cids) -> BlocksResult, or None for the default
        self.links: list[bytes] = []
        self.rate: float | None = None
        self.mdu: int | None = None  # every link's MDU; None, as for a node with no open link: not known
        self.requests: list[tuple[bytes, list[Cid]]] = []
        self.closed: list[bytes] = []
        self.lock = threading.Lock()
        # /provide
        self.calls: list[tuple[bytes, list[bytes], list[bytes]]] = []
        self.refuse: set[bytes] = set()
        self.limit: dict[bytes, int] = {}

    def open_links(self) -> list[bytes]:
        """Nodes with an open link."""
        return list(self.links)

    def rate_estimate(self, node_id: bytes):
        """The same estimate for every link."""
        return self.rate

    def link_mdu(self, node_id: bytes):
        """The same MDU for every link (None: not known)."""
        return self.mdu

    def close(self, node_id: bytes):
        """Close the link to node_id (noted)."""
        with self.lock:
            self.closed.append(node_id)

    def blocks(self, node_id: bytes, cids: list[Cid], on_progress=None) -> BlocksResult:
        """Serve what node_id holds, BATCH CIDs per request."""
        with self.lock:
            self.requests.append((node_id, list(cids)))
        self.on_progress = on_progress
        behaviour = self.behaviour.get(node_id)
        if behaviour is not None:
            result = behaviour(cids)
            if result is not None:
                return result
        held = self.holdings.get(node_id, {})
        result = BlocksResult(OK)
        for cid in cids[:BATCH]:
            if cid in held:
                result.blocks[cid] = held[cid]
            else:
                result.missing.append(cid)
        result.unsent = list(cids[BATCH:])
        return result

    def provide(self, node_id, keys, withdrawn):
        """Record one batched /provide; the keys the node may have recorded, as PeerClient.provide returns."""
        self.calls.append((node_id, list(keys), list(withdrawn)))
        if node_id in self.refuse:
            return set()
        return set(list(keys)[:self.limit.get(node_id, len(keys))])

    def asked(self, node_id: bytes) -> list[Cid]:
        """Every CID sent to node_id in /blocks requests."""
        return [cid for node, cids in self.requests if node == node_id for cid in cids[:BATCH]]
