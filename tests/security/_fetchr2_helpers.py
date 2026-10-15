"""Shared helpers for the fetch-path bound tests (N1-R1, N1-R2, N1-R3; not a test module): hand-made DAGs whose
nodes are padded with empty children or chained, and the bound a byte-range read may use, computed here from the
protocol constants rather than taken from rnfs.fetcher, so the tests also check its formula."""
from rnfs.constants import MAX_BLOCK_SIZE, MAX_CHILDREN, MAX_DEPTH
from rnfs.content import dag
from rnfs.content.cid import DAG, EMPTY_FILE, RAW, Cid
from rnfs.fetcher import DAG_GRANULE, dag_bound

PAD = 3000  # empty children per padded node: about 118 KiB of node, under MAX_BLOCK_SIZE
# The largest file node the tree rule writes: MAX_CHILDREN children of the longest CID and largest size
FULL_FILE_NODE = len(dag.encode(dag.FileNode([dag.Child(Cid.for_block(DAG, b"%d" % i), dag.MAX_SIZE)
                                              for i in range(MAX_CHILDREN)])))


def file_levels(size: int) -> int:
    """File node levels the tree rule gives a file of size bytes cut into chunks of at least DAG_GRANULE (its last
    may be shorter): the smallest height whose MAX_CHILDREN-wide levels hold size // DAG_GRANULE + 1 chunks."""
    chunks = size // DAG_GRANULE + 1
    levels = 1
    while MAX_CHILDREN ** levels < chunks and levels < MAX_DEPTH:
        levels += 1
    return levels


def range_allowance(size: int, length: int) -> int:
    """The most bytes of file nodes reading length bytes of a file declaring size may meet: what the range itself
    can hold (dag_bound), plus two full file nodes (the range's two ends) on each level the file can have."""
    return dag_bound(length) + 2 * file_levels(size) * FULL_FILE_NODE


class Blocks:
    """Blocks for A to serve, built by hand."""

    def __init__(self):
        """Just the empty chunk, which padded nodes list."""
        self.blocks: dict[Cid, bytes] = {EMPTY_FILE: b""}

    def raw(self, data: bytes) -> Cid:
        """A raw chunk."""
        cid = Cid.for_block(RAW, data)
        self.blocks[cid] = data
        return cid

    def node(self, node) -> Cid:
        """A DAG node."""
        block = dag.encode(node)
        assert len(block) <= MAX_BLOCK_SIZE, len(block)
        cid = Cid.for_block(DAG, block)
        self.blocks[cid] = block
        return cid

    def chain(self, leaf: Cid, size: int, levels: int, pad: int = PAD) -> Cid:
        """levels file nodes above leaf (size bytes), each with that one child and pad empty children after it.
        Every level's node differs (it lists a different child), so each is a distinct block to fetch."""
        cid = leaf
        for _ in range(levels):
            cid = self.node(dag.FileNode([dag.Child(cid, size)] + [dag.Child(EMPTY_FILE, 0)] * pad))
        return cid

    def index_chain(self, name: str, levels: int) -> Cid:
        """A directory holding one empty file called name, below levels index nodes of one child each."""
        cid = self.node(dag.DirectoryNode([dag.Entry(name, EMPTY_FILE, 0, dag.KIND_FILE)]))
        for _ in range(levels):
            cid = self.node(dag.IndexNode([dag.IndexChild(name, cid, 0)]))
        return cid
