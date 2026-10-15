"""Shared helpers for the content-group security tests (not a test module): a DAG that declares 1 byte of content
but holds megabytes of file nodes, and two offline nodes where B fetches from A over a NetClient that counts what
B downloads."""
import os
import tempfile
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import MAX_BLOCK_SIZE
from rnfs.content import dag
from rnfs.content.cid import DAG, EMPTY_FILE, RAW, Cid
from rnfs.content.storage import USER
from rnfs.node import RnfsNode
from tests.test_names import FakeNetwork
from tests.test_node import A_ID, B_ID, NetClient

# Distinct file nodes made only of empty chunks below the bomb's root, and the chunks in the first of them
BOMB_NODES = 100
BOMB_CHILDREN = 500  # about 20 KB of node per file node: 2 MB of them in all
# What fetching a root that declares N content bytes may download beyond N: two full DAG nodes. An honest
# 1-byte file is a single 1-byte block, so this leaves room for any per-node overhead a bound could allow
SLACK = 2 * MAX_BLOCK_SIZE


class Bomb:
    """A file root whose content is b"x" (1 byte) but whose DAG holds BOMB_NODES distinct file nodes of empty
    chunks: zero-size children, which nothing on the read side rejects (audit L3)."""

    def __init__(self, nodes: int = BOMB_NODES, children: int = BOMB_CHILDREN):
        """Build the blocks into self.blocks; self.root is the root's CID."""
        self.blocks: dict[Cid, bytes] = {EMPTY_FILE: b""}
        # Each node has a different number of empty chunks, so each is a distinct block that must be fetched
        empties = [self.node(dag.FileNode([dag.Child(EMPTY_FILE, 0)] * (children + i))) for i in range(nodes)]
        self.content = b"x"
        x = Cid.for_block(RAW, self.content)
        self.blocks[x] = self.content
        self.root = self.node(dag.FileNode([dag.Child(cid, 0) for cid in empties] + [dag.Child(x, 1)]))
        self.size = len(self.content)  # what the root declares: the sum of its children's sizes

    def node(self, node) -> Cid:
        """Encode node and keep its block."""
        block = dag.encode(node)
        cid = Cid.for_block(DAG, block)
        self.blocks[cid] = block
        return cid

    def total_bytes(self) -> int:
        """Bytes of every distinct block of the DAG."""
        return sum(len(block) for block in self.blocks.values())


class CountingNetClient(NetClient):
    """A NetClient that adds up the blocks every /blocks request returns: what the fetching node downloaded."""

    def __init__(self):
        """Nothing downloaded yet."""
        super().__init__()
        self.downloaded_blocks = 0
        self.downloaded_bytes = 0

    def blocks(self, node_id, cids, on_progress=None):
        """NetClient.blocks, counted."""
        result = super().blocks(node_id, cids, on_progress)
        self.downloaded_blocks += len(result.blocks)
        self.downloaded_bytes += sum(len(block) for block in result.blocks.values())
        return result

    def reset(self):
        """Start counting again."""
        self.downloaded_blocks = self.downloaded_bytes = 0


class TwoNodeTest(unittest.TestCase):
    """Two offline nodes, as tests/test_node.py's NodeTest: A serves what it provides and B fetches it. B uses the
    default config limits (no max_download_size, max_output_entries 100,000) unless b_settings says otherwise."""

    b_settings: dict = {}

    def setUp(self):
        """Fresh nodes in a temporary directory, with RNS's log silenced."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = CountingNetClient()
        self.net.sender = B_ID
        self.a = self.make_node("a", A_ID)
        self.b = self.make_node("b", B_ID, **self.b_settings)
        self.b.routing.heard(A_ID)

    def make_node(self, name: str, node_id: bytes, **settings) -> RnfsNode:
        """An offline node with its own directory (settings as NodeTest.make_node)."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, name), slow="no", lookup_timeout=5, path_timeout=0.5,
                        **settings)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def serve(self, blocks: dict[Cid, bytes], root: Cid):
        """Store blocks on A, pin root there and advertise A as its provider, so B can fetch it."""
        with self.a.storage.protect() as session:
            for cid, block in blocks.items():
                session.put(cid, block)
        self.a.storage.pin(root, USER)
        self.a.publisher.refresh()
        self.a.records.store(root.key, A_ID)
        self.assertIsNotNone(self.a.backend.served_block(root), "A should serve the root")

    def serve_bomb(self) -> Bomb:
        """A Bomb that A serves."""
        bomb = Bomb()
        self.serve(bomb.blocks, bomb.root)
        return bomb
