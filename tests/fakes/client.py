"""FakeClient, the PeerClient stand-in the fetcher, publisher, mesh and names tests use. Its methods keep
PeerClient's names and parameters (tests/test_fake_contract.py), so production code calls them as it calls the
real client, without looking up with getattr what a fake might lack."""
import bz2
import threading

from rnfs.content.cid import Cid
from rnfs.network.client import FAILED, LOST, OK, BlocksResult, RangeResult

BATCH = 4  # CIDs the fake client sends per /blocks request, like a packet limit; the rest come back unsent


class FakeClient:
    """Stands in for PeerClient.

    /blocks and /range: each node holds a dict of blocks, or behaves as a test function says. /provide: each request
    is recorded; nodes in refuse record nothing, a node in limit records only its first keys.
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
        self.held: list[tuple[bytes, float]] = []  # hold_link calls: (node, seconds)
        self.kept_open: list[bytes] = []  # close_unless_used calls that left the link open
        self.users: dict[bytes, dict[int, int]] = {}  # node -> threads with a /blocks call to it in progress
        # /range (rnfs 0.2.0): every range asked (node, CID, offset, max_bytes, compress), the nodes that leave it
        # unanswered (before 0.2.0), and per node f(cid, offset, max_bytes, compress) -> RangeResult, or None for the
        # default
        self.ranges: list[tuple[bytes, Cid, int, int, bool]] = []
        self.no_range: set[bytes] = set()
        self.range_behaviour: dict[bytes, object] = {}
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

    def hold_link(self, node_id: bytes, seconds: float) -> bool:
        """Count the link to node_id as in use for seconds (noted)."""
        with self.lock:
            self.held.append((node_id, seconds))
        return True

    def close(self, node_id: bytes):
        """Close the link to node_id (noted)."""
        with self.lock:
            self.closed.append(node_id)

    def close_unless_used(self, node_id: bytes, own: set[int]) -> bool:
        """close(node_id) unless a thread not in own has a /blocks call to node_id in progress (noted in
        kept_open)."""
        with self.lock:
            others = any(ident not in own for ident in self.users.get(node_id, {}))
            if others:
                self.kept_open.append(node_id)
        if others:
            return False
        self.close(node_id)
        return True

    def blocks(self, node_id: bytes, cids: list[Cid], on_progress=None) -> BlocksResult:
        """Serve what node_id holds, BATCH CIDs per request (the calling thread counted among node_id's users)."""
        ident = threading.get_ident()
        with self.lock:
            users = self.users.setdefault(node_id, {})
            users[ident] = users.get(ident, 0) + 1
        try:
            return self._blocks(node_id, cids, on_progress)
        finally:
            with self.lock:
                users[ident] -= 1
                if not users[ident]:
                    del users[ident]

    def _blocks(self, node_id: bytes, cids: list[Cid], on_progress) -> BlocksResult:
        """blocks(), served."""
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

    def block_range(self, node_id: bytes, cid: Cid, held: bytes = b"", max_bytes: int = 0, compress: bool = False,
                    block_size: int | None = None, on_progress=None) -> RangeResult:
        """Serve the next range of cid's block from what node_id holds, as a 0.2.0 node answers /range; recorded in
        ranges, and the first range of a block also in requests (as a request for [cid]: asked() counts each block
        asked for once, however many ranges it takes). A node in no_range leaves it unanswered, as one before 0.2.0
        does; a node's /blocks behaviour, if set, decides first (anything but OK with the block is its outcome)."""
        ident = threading.get_ident()
        with self.lock:
            if not held:
                self.requests.append((node_id, [cid]))
            self.ranges.append((node_id, cid, len(held), max_bytes, compress))
            users = self.users.setdefault(node_id, {})
            users[ident] = users.get(ident, 0) + 1
        try:
            self.on_progress = on_progress
            if node_id in self.no_range:
                return RangeResult(LOST, silent=True, reason="no answer")
            behaviour = self.range_behaviour.get(node_id)
            if behaviour is not None:
                result = behaviour(cid, len(held), max_bytes, compress)
                if result is not None:
                    return result
            block = self.holdings.get(node_id, {}).get(cid)
            behaviour = self.behaviour.get(node_id)
            if behaviour is not None:
                outcome = behaviour([cid])
                if outcome is not None:
                    if outcome.status != OK or cid not in outcome.blocks:
                        return RangeResult(outcome.status, missing=cid in outcome.missing, reason=outcome.reason)
                    block = outcome.blocks[cid]
            if block is None:
                return RangeResult(OK, missing=True)
            offset = len(held)
            if (block_size is not None and block_size != len(block)) or offset > len(block):
                return RangeResult(FAILED, reason="malformed response")
            data = block[offset:offset + max_bytes]
            wire = len(data)
            compressed = False
            if compress and data and len(bz2.compress(data)) < len(data):
                wire, compressed = len(bz2.compress(data)), True
            result = RangeResult(OK, size=len(block), data=data, wire=wire + 32, compressed=compressed, elapsed=0.0)
            if offset + len(data) == len(block):
                whole = held + data
                if cid.verify(whole):
                    result.block = whole
                else:
                    result.bad = True
            return result
        finally:
            with self.lock:
                users[ident] -= 1
                if not users[ident]:
                    del users[ident]

    def provide(self, node_id, keys, withdrawn):
        """Record one batched /provide; the keys the node may have recorded, as PeerClient.provide returns."""
        self.calls.append((node_id, list(keys), list(withdrawn)))
        if node_id in self.refuse:
            return set()
        return set(list(keys)[:self.limit.get(node_id, len(keys))])

    def asked(self, node_id: bytes) -> list[Cid]:
        """Every CID sent to node_id in /blocks requests."""
        return [cid for node, cids in self.requests if node == node_id for cid in cids[:BATCH]]
