"""DAG node encoding, strict decoding and name rules (specification section 5). No RNS dependency
beyond its bundled msgpack.

| Node            | Encoding                                  | Children                               |
| File            | [1, [[cid, size], ...]]                   | raw chunks or file nodes, in order     |
| Directory       | [2, [[name, cid, size, kind], ...]]       | entries sorted by name, names unique   |
| Directory index | [3, [[first_name, cid, size], ...]]       | directory or index nodes, in name order|

Writers produce exactly what umsgpack.packb produces: str as str, bytes as bin, shortest headers.
Readers accept nothing else: the decoder below understands only the few msgpack forms a node can
contain, and a node must re-encode to identical bytes, which rules out non-shortest headers.

Decoding is a pure function of the block's bytes and nodes are immutable, so decode() remembers the nodes of
the blocks it decoded last (MEMO_BLOCKS of them, MEMO_BYTES of blocks): a read walks the same file and
directory nodes many times (a cat of 64 KiB from a large file decoded its root node about twenty times).
"""
import struct
import threading
from collections import OrderedDict
from dataclasses import dataclass

from RNS.vendor import umsgpack

from rnfs.constants import MAX_BLOCK_SIZE
from rnfs.content.cid import Cid
from rnfs.errors import InvalidDag

FILE, DIRECTORY, INDEX = 1, 2, 3
KIND_FILE, KIND_EXECUTABLE, KIND_DIRECTORY = 0, 1, 2
KINDS = (KIND_FILE, KIND_EXECUTABLE, KIND_DIRECTORY)
KIND_NAMES = {KIND_FILE: "file", KIND_EXECUTABLE: "executable", KIND_DIRECTORY: "directory"}  # as `ls` shows them

MAX_SIZE = 2**64 - 1  # a size above this makes a DAG invalid
MAX_NAME_BYTES = 255
# decode()'s memo of nodes by block bytes, least recently used forgotten first: a node's Python objects take
# several times its block's bytes, so this holds well under 20 MB
MEMO_BLOCKS = 256
MEMO_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class Child:
    """A file node's child: a raw chunk or a file node, and the content bytes below it."""

    cid: Cid
    size: int


@dataclass(frozen=True)
class Entry:
    """A directory entry: name, the entry's root CID, content bytes below it, and kind (0, 1 or 2)."""

    name: str
    cid: Cid
    size: int
    kind: int


@dataclass(frozen=True)
class IndexChild:
    """A directory index's child: a directory or index node, and the first name below it."""

    first_name: str
    cid: Cid
    size: int


@dataclass(frozen=True)
class FileNode:
    """Type 1: a file's content is its children's content, concatenated in order."""

    children: tuple[Child, ...]

    def __post_init__(self):
        """Store children as a tuple, so nodes are hashable whatever sequence they were built from."""
        object.__setattr__(self, "children", tuple(self.children))

    @property
    def size(self) -> int:
        """Content bytes below this node: the sum of its children's sizes."""
        return sum(child.size for child in self.children)

    @property
    def child_cids(self) -> tuple[Cid, ...]:
        """The CIDs this node lists, in order."""
        return tuple(child.cid for child in self.children)

    @property
    def names(self) -> tuple[str, ...]:
        """The names this node lists: none, for a file node."""
        return ()


@dataclass(frozen=True)
class DirectoryNode:
    """Type 2: a directory, or one group of a large directory's entries."""

    entries: tuple[Entry, ...]

    def __post_init__(self):
        """Store entries as a tuple."""
        object.__setattr__(self, "entries", tuple(self.entries))

    @property
    def size(self) -> int:
        """Content bytes below this node: the sum of its entries' sizes."""
        return sum(entry.size for entry in self.entries)

    @property
    def child_cids(self) -> tuple[Cid, ...]:
        """The CIDs this node lists (its entries'), in order."""
        return tuple(entry.cid for entry in self.entries)

    @property
    def names(self) -> tuple[str, ...]:
        """Its entries' names, in order."""
        return tuple(entry.name for entry in self.entries)


@dataclass(frozen=True)
class IndexNode:
    """Type 3: a level above a large directory's groups of entries."""

    children: tuple[IndexChild, ...]

    def __post_init__(self):
        """Store children as a tuple."""
        object.__setattr__(self, "children", tuple(self.children))

    @property
    def size(self) -> int:
        """Content bytes below this node: the sum of its children's sizes."""
        return sum(child.size for child in self.children)

    @property
    def child_cids(self) -> tuple[Cid, ...]:
        """The CIDs this node lists, in order."""
        return tuple(child.cid for child in self.children)

    @property
    def names(self) -> tuple[str, ...]:
        """Its children's first names, in order."""
        return tuple(child.first_name for child in self.children)


Node = FileNode | DirectoryNode | IndexNode


def check_name(name) -> None:
    """Raise ValueError unless name is a valid entry name: UTF-8, 1-255 bytes, no / or NUL, not . or .."""
    if not isinstance(name, str):
        raise ValueError("name must be a string")
    try:
        encoded = name.encode("utf-8")
    except UnicodeEncodeError:
        # Lone surrogates, e.g. from a filename that wasn't valid UTF-8 (os.fsdecode's surrogateescape)
        raise ValueError(f"name {name!r} is not valid UTF-8") from None
    if not 1 <= len(encoded) <= MAX_NAME_BYTES:
        raise ValueError(f"name {name!r} must be 1-{MAX_NAME_BYTES} bytes of UTF-8")
    if "/" in name or "\0" in name:
        raise ValueError(f"name {name!r} contains / or NUL")
    if name in (".", ".."):
        raise ValueError(f"name {name!r} is not allowed")


def is_valid_name(name) -> bool:
    """True if check_name accepts name."""
    try:
        check_name(name)
        return True
    except ValueError:
        return False


def encode(node: Node) -> bytes:
    """The block for node, exactly as umsgpack.packb writes it."""
    if isinstance(node, FileNode):
        return umsgpack.packb([FILE, [[c.cid.binary, c.size] for c in node.children]])
    if isinstance(node, DirectoryNode):
        return umsgpack.packb([DIRECTORY, [[e.name, e.cid.binary, e.size, e.kind] for e in node.entries]])
    if isinstance(node, IndexNode):
        return umsgpack.packb([INDEX, [[c.first_name, c.cid.binary, c.size] for c in node.children]])
    raise TypeError(f"not a DAG node: {node!r}")


# The big-endian lengths and integers after a msgpack type byte, by type byte (struct formats compiled once)
_ARRAY_LENGTHS = {0xDC: struct.Struct(">H"), 0xDD: struct.Struct(">I")}
_UINTS = {0xCC: struct.Struct(">B"), 0xCD: struct.Struct(">H"), 0xCE: struct.Struct(">I"), 0xCF: struct.Struct(">Q")}
_BIN_LENGTHS = {0xC4: struct.Struct(">B"), 0xC5: struct.Struct(">H"), 0xC6: struct.Struct(">I")}
_STR_LENGTHS = {0xD9: struct.Struct(">B"), 0xDA: struct.Struct(">H"), 0xDB: struct.Struct(">I")}


class _Reader:
    """Reads the handful of msgpack forms a DAG node may contain, rejecting every other form."""

    def __init__(self, data: bytes):
        """Read from the start of data."""
        self.data = data
        self.pos = 0

    def _take(self, count: int) -> bytes:
        """The next count bytes. Raises InvalidDag if the block ends first."""
        end = self.pos + count
        if end > len(self.data):
            raise InvalidDag("DAG node is truncated")
        chunk = self.data[self.pos:end]
        self.pos = end
        return chunk

    def _byte(self) -> int:
        """The next byte."""
        pos = self.pos
        if pos >= len(self.data):
            raise InvalidDag("DAG node is truncated")
        self.pos = pos + 1
        return self.data[pos]

    def _length(self, size_format: struct.Struct) -> int:
        """A big-endian length or integer of the given compiled struct format."""
        end = self.pos + size_format.size
        if end > len(self.data):
            raise InvalidDag("DAG node is truncated")
        value = size_format.unpack_from(self.data, self.pos)[0]
        self.pos = end
        return value

    def array(self) -> int:
        """An array header; returns its length."""
        tag = self._byte()
        if 0x90 <= tag <= 0x9F:
            return tag & 0x0F
        size_format = _ARRAY_LENGTHS.get(tag)
        if size_format is not None:
            return self._length(size_format)
        raise InvalidDag(f"expected a msgpack array, found type byte 0x{tag:02x}")

    def uint(self) -> int:
        """A non-negative integer. Booleans, negative integers and floats are rejected."""
        tag = self._byte()
        if tag <= 0x7F:
            return tag
        size_format = _UINTS.get(tag)
        if size_format is not None:
            return self._length(size_format)
        raise InvalidDag(f"expected a non-negative msgpack integer, found type byte 0x{tag:02x}")

    def bin(self) -> bytes:
        """A bin."""
        tag = self._byte()
        size_format = _BIN_LENGTHS.get(tag)
        if size_format is None:
            raise InvalidDag(f"expected msgpack bin, found type byte 0x{tag:02x}")
        return bytes(self._take(self._length(size_format)))

    def text(self) -> str:
        """A str holding valid UTF-8."""
        tag = self._byte()
        if 0xA0 <= tag <= 0xBF:
            length = tag & 0x1F
        elif tag in _STR_LENGTHS:
            length = self._length(_STR_LENGTHS[tag])
        else:
            raise InvalidDag(f"expected msgpack str, found type byte 0x{tag:02x}")
        try:
            return bytes(self._take(length)).decode("utf-8")
        except UnicodeDecodeError:
            raise InvalidDag("name is not valid UTF-8") from None

    def cid(self) -> Cid:
        """A binary CID as bin."""
        try:
            return Cid.from_bytes(self.bin())
        except ValueError as e:
            raise InvalidDag(f"invalid child CID: {e}") from None

    def fields(self, count: int):
        """An array header that must have exactly count elements."""
        length = self.array()
        if length != count:
            raise InvalidDag(f"expected an array of {count} fields, found {length}")


class _Memo:
    """decode()'s memo: block bytes -> node, for blocks that decoded (an invalid one is checked every time)."""

    def __init__(self):
        """Empty."""
        self._lock = threading.Lock()
        self._nodes: OrderedDict[bytes, Node] = OrderedDict()  # least recently used first
        self._bytes = 0

    def get(self, block: bytes) -> "Node | None":
        """The node block decoded to, if remembered."""
        with self._lock:
            node = self._nodes.get(block)
            if node is not None:
                self._nodes.move_to_end(block)
            return node

    def put(self, block: bytes, node: Node):
        """Remember block's node, forgetting the least recently used past MEMO_BLOCKS or MEMO_BYTES."""
        with self._lock:
            if block not in self._nodes:
                self._bytes += len(block)
            self._nodes[block] = node
            self._nodes.move_to_end(block)
            while len(self._nodes) > MEMO_BLOCKS or self._bytes > MEMO_BYTES:
                forgotten, _ = self._nodes.popitem(last=False)
                self._bytes -= len(forgotten)


_memo = _Memo()


def decode(block: bytes) -> Node:
    """The DAG node encoded by block, checked strictly. Raises InvalidDag for anything not exactly valid.

    Checks the encoding rules (types, shortest headers, no extra fields or trailing bytes), the name
    rules, sorted unique names, strictly increasing first_names, kinds and size overflow. Checks that
    need a node's context (child types, sizes against children, index bounds, depth) are walker.py's.
    The node of a block decoded recently is remembered: the same bytes always give the same node.
    """
    if not isinstance(block, (bytes, bytearray, memoryview)):
        raise TypeError("block must be bytes")
    block = bytes(block)
    node = _memo.get(block)
    if node is None:
        node = _decode(block)
        _memo.put(block, node)
    return node


def _decode(block: bytes) -> Node:
    """decode(), without the memo."""
    if len(block) > MAX_BLOCK_SIZE:
        raise InvalidDag(f"block of {len(block)} bytes exceeds MAX_BLOCK_SIZE")
    reader = _Reader(block)
    reader.fields(2)
    node_type = reader.uint()
    count = reader.array()
    # Every child takes at least a few bytes, so a huge count in a short block fails fast as truncated
    if node_type == FILE:
        children = []
        for _ in range(count):
            reader.fields(2)
            children.append(Child(reader.cid(), reader.uint()))
        node = FileNode(children)
    elif node_type == DIRECTORY:
        entries = []
        for _ in range(count):
            reader.fields(4)
            name = reader.text()
            entries.append(Entry(name, reader.cid(), reader.uint(), reader.uint()))
        node = DirectoryNode(entries)
    elif node_type == INDEX:
        index_children = []
        for _ in range(count):
            reader.fields(3)
            first_name = reader.text()
            index_children.append(IndexChild(first_name, reader.cid(), reader.uint()))
        node = IndexNode(index_children)
    else:
        raise InvalidDag(f"unknown DAG node type {node_type}")
    if reader.pos != len(block):
        raise InvalidDag("trailing bytes after DAG node")

    _check_node(node)
    # Rules out non-shortest headers (and anything else the reader let through that a writer wouldn't write)
    if encode(node) != block:
        raise InvalidDag("DAG node does not re-encode to identical bytes")
    return node


def _check_node(node: Node):
    """The checks a decoded node must pass on its own. Raises InvalidDag."""
    if isinstance(node, DirectoryNode):
        for entry in node.entries:
            if entry.kind not in KINDS:
                raise InvalidDag(f"entry {entry.name!r} has unknown kind {entry.kind}")
    names = node.names
    for name in names:
        try:
            check_name(name)
        except ValueError as e:
            raise InvalidDag(str(e)) from None
    # Python orders str by code point, which for valid UTF-8 is the same as bytewise UTF-8 order
    for previous, name in zip(names, names[1:]):
        if not previous < name:
            raise InvalidDag(f"names not sorted and unique: {previous!r} then {name!r}")
    if node.size > MAX_SIZE:
        raise InvalidDag("sizes add up to more than 2^64 - 1")


def node_type_name(node: Node) -> str:
    """The walker's name for node's type: "file", "directory" or "index"."""
    if isinstance(node, FileNode):
        return "file"
    if isinstance(node, DirectoryNode):
        return "directory"
    return "index"
