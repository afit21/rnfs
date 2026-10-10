"""Building trees of blocks from files, streams and directories (specification section 5).
No RNS dependency.

Writers MUST follow the tree rule so the same content always gets the same CID:
1. start with the list of children: a file's CHUNK_SIZE chunks (the last may be shorter, never empty),
   or a directory's entries sorted by name;
2. while the list has more than MAX_CHILDREN children, split it into consecutive groups of MAX_CHILDREN
   (the last may be smaller), make one node per group (files: type 1; directories: type 2 for groups of
   entries, type 3 above that) and continue with the list of those nodes;
3. the final list becomes the root node.
A file with one chunk is just that raw block, an empty file is a zero-byte raw block, and an empty
directory is [2, []].

Builders hand every block to a `put(cid, block)` callback, so they don't depend on Storage; a no-op
callback (NO_PUT) gives only-hash mode. Files are streamed, one chunk in memory at a time.
"""
import io
import os
import stat
from dataclasses import dataclass
from typing import BinaryIO, Callable, Iterable

from rnfs import constants
from rnfs.content import dag
from rnfs.content.cid import DAG, EMPTY_FILE, RAW, Cid
from rnfs.content.dag import Child, DirectoryNode, Entry, FileNode, IndexChild, IndexNode
from rnfs.errors import TooLarge

Put = Callable[[Cid, bytes], None]


def NO_PUT(cid: Cid, block: bytes) -> None:
    """A put callback that stores nothing: only-hash mode."""


@dataclass(frozen=True)
class Built:
    """The root of a tree just built."""

    cid: Cid
    size: int  # content bytes below the root
    kind: int  # the kind a directory entry for it would have: dag.KIND_FILE, KIND_EXECUTABLE or KIND_DIRECTORY
    height: int  # DAG nodes on the longest path from the root down, all types counted; 0 for a raw block


class _Levels:
    """The tree rule applied incrementally, so a file's chunk list never has to be held in full.

    Level 0 holds the original children. Once a level has more than MAX_CHILDREN items, the whole list
    is certain to be grouped, and its first MAX_CHILDREN items are exactly its next group.
    """

    def __init__(self, make_group: Callable[[int, list], tuple], max_children: int):
        """make_group(level, items) builds the node for a group of (item, height) pairs at level."""
        self.make_group = make_group
        self.max_children = max_children
        self.levels: list[list[tuple]] = [[]]

    def add(self, item, height: int):
        """Append an original child (level 0)."""
        self._push(0, (item, height))

    def _push(self, level: int, pair: tuple):
        """Append pair to level, grouping its first MAX_CHILDREN items once it has more than that."""
        items = self.levels[level]
        items.append(pair)
        if len(items) > self.max_children:
            group = items[:self.max_children]
            del items[:self.max_children]
            if level + 1 == len(self.levels):
                self.levels.append([])
            self._push(level + 1, self.make_group(level, group))

    def finish(self) -> tuple[int, list[tuple]]:
        """The final list and its level: group what's left at each level that has already been grouped."""
        level = 0
        while level + 1 < len(self.levels):
            remainder = self.levels[level]
            if remainder:
                self.levels[level] = []
                self._push(level + 1, self.make_group(level, remainder))
            level += 1
        return level, self.levels[level]


def _items(pairs: list[tuple]) -> list:
    """The items of (item, height) pairs."""
    return [item for item, _ in pairs]


def _height(pairs: list[tuple]) -> int:
    """Height of a node whose children are these (item, height) pairs."""
    return 1 + max((height for _, height in pairs), default=0)


def _check_depth(height: int):
    """Raise TooLarge if a tree this high breaks MAX_DEPTH, which `add` must refuse."""
    if height > constants.MAX_DEPTH:
        raise TooLarge(f"tree is {height} DAG nodes deep, more than MAX_DEPTH ({constants.MAX_DEPTH})")


def _put_node(node: dag.Node, put: Put) -> Cid:
    """Encode node, hand it to put and return its CID."""
    block = dag.encode(node)
    if len(block) > constants.MAX_BLOCK_SIZE:
        # Can't happen with the protocol constants (256 maximal entries are ~80 KiB), but never write one
        raise TooLarge("DAG node exceeds MAX_BLOCK_SIZE")
    cid = Cid.for_block(DAG, block)
    put(cid, block)
    return cid


def _read_full(stream: BinaryIO, want: int) -> bytes:
    """Up to want bytes from stream, fewer only at end of stream (pipes and sockets return short reads)."""
    parts = []
    got = 0
    while got < want:
        data = stream.read(want - got)
        if not data:
            break
        parts.append(data)
        got += len(data)
    return b"".join(parts)


def build_file(stream: BinaryIO, put: Put, size: int | None = None, kind: int = dag.KIND_FILE) -> Built:
    """Build a file from everything read from stream, or exactly size bytes of it (ValueError if it ends early)."""
    chunk_size = constants.CHUNK_SIZE

    def make_group(level: int, pairs: list[tuple]) -> tuple:
        """A type 1 node over a group of chunks or file nodes."""
        node = FileNode(_items(pairs))
        return Child(_put_node(node, put), node.size), _height(pairs)

    levels = _Levels(make_group, constants.MAX_CHILDREN)
    remaining = size
    while remaining is None or remaining > 0:
        want = chunk_size if remaining is None else min(chunk_size, remaining)
        chunk = _read_full(stream, want)
        if remaining is not None:
            if len(chunk) < want:
                raise ValueError(f"stream ended {remaining - len(chunk)} bytes early")
            remaining -= len(chunk)
        if not chunk:
            break
        cid = Cid.for_block(RAW, chunk)
        put(cid, chunk)
        levels.add(Child(cid, len(chunk)), 0)
        if len(chunk) < want:
            break  # end of stream

    level, pairs = levels.finish()
    if level == 0 and not pairs:
        put(EMPTY_FILE, b"")
        return Built(EMPTY_FILE, 0, kind, 0)
    if level == 0 and len(pairs) == 1:
        chunk_child = pairs[0][0]
        return Built(chunk_child.cid, chunk_child.size, kind, 0)
    child, height = make_group(level, pairs)
    _check_depth(height)
    return Built(child.cid, child.size, kind, height)


def build_bytes(data: bytes, put: Put, kind: int = dag.KIND_FILE) -> Built:
    """Build a file from bytes in memory."""
    return build_file(io.BytesIO(data), put, len(data), kind)


def build_directory(entries: Iterable[Entry], put: Put, child_height: int = 0) -> Built:
    """Build a directory from its entries, in any order.

    child_height is the greatest height among the entries' trees, used for the MAX_DEPTH check; pass 0
    if unknown and check the finished tree's depth separately. Raises ValueError for an invalid or
    duplicate name, TooLarge if the tree is deeper than MAX_DEPTH.
    """
    entries = list(entries)
    for entry in entries:
        dag.check_name(entry.name)
        if entry.kind not in dag.KINDS:
            raise ValueError(f"entry {entry.name!r} has invalid kind {entry.kind}")
    # str order is code point order, which is bytewise UTF-8 order
    entries.sort(key=lambda entry: entry.name)
    for previous, entry in zip(entries, entries[1:]):
        if previous.name == entry.name:
            raise ValueError(f"duplicate name {entry.name!r}")

    def make_group(level: int, pairs: list[tuple]) -> tuple:
        """A type 2 node over a group of entries, or a type 3 node over a group of nodes above that."""
        items = _items(pairs)
        if level == 0:
            node = DirectoryNode(items)
            first_name = items[0].name
        else:
            node = IndexNode(items)
            first_name = items[0].first_name
        return IndexChild(first_name, _put_node(node, put), node.size), _height(pairs)

    levels = _Levels(make_group, constants.MAX_CHILDREN)
    for entry in entries:
        levels.add(entry, child_height)
    level, pairs = levels.finish()
    if level == 0:
        node = DirectoryNode(_items(pairs))
        height = 1 + child_height
        cid = _put_node(node, put)
        size = node.size
    else:
        child, height = make_group(level, pairs)
        cid, size = child.cid, child.size
    if size > dag.MAX_SIZE:
        raise TooLarge("directory content exceeds 2^64 - 1 bytes")
    _check_depth(height)
    return Built(cid, size, dag.KIND_DIRECTORY, height)


def wrap(name: str, built: Built, put: Put) -> Built:
    """Build a directory holding built as its one entry, called name. Raises ValueError for an invalid name."""
    return build_directory([Entry(name, built.cid, built.size, built.kind)], put, built.height)


def display_path(path: str) -> str:
    """path for messages, with bytes that weren't valid UTF-8 shown as escapes."""
    return path.encode("utf-8", "backslashreplace").decode("utf-8")


def _is_executable(mode: int) -> bool:
    """True if a file mode has the owner's execute bit, as git records it."""
    return bool(mode & stat.S_IXUSR)


def build_path(path: str, put: Put, warnings: list[str]) -> Built:
    """Build the file or directory at path, appending to warnings what was skipped.

    The path itself is followed if it is a symlink (the user named it); inside a directory, hidden files
    are included and symlinks, devices, sockets, FIFOs and invalid names are skipped with a warning.
    Raises TooLarge for a tree deeper than MAX_DEPTH, ValueError if path is neither file nor directory.
    """
    st = os.stat(path)
    if stat.S_ISDIR(st.st_mode):
        return _build_tree(path, put, warnings, 1)
    if stat.S_ISREG(st.st_mode):
        kind = dag.KIND_EXECUTABLE if _is_executable(st.st_mode) else dag.KIND_FILE
        with open(path, "rb") as f:
            return build_file(f, put, kind=kind)
    raise ValueError(f"{display_path(path)} is not a regular file or directory")


def _build_tree(path: str, put: Put, warnings: list[str], depth: int) -> Built:
    """Build the directory at path, which sits depth directories from the root (1 for the root)."""
    # Every directory level adds at least one DAG node, so this bounds the recursion before it gets deep
    if depth > constants.MAX_DEPTH:
        raise TooLarge(f"{display_path(path)} is nested deeper than MAX_DEPTH ({constants.MAX_DEPTH})")
    entries = []
    child_height = 0
    with os.scandir(path) as listing:
        children = sorted(listing, key=lambda entry: entry.name)
    for item in children:
        shown = display_path(item.path)
        if not dag.is_valid_name(item.name):
            warnings.append(f"skipped {shown}: name is not valid UTF-8 or breaks the name rules")
            continue
        if item.is_symlink():
            warnings.append(f"skipped symlink {shown}")
            continue
        if item.is_dir(follow_symlinks=False):
            child = _build_tree(item.path, put, warnings, depth + 1)
        elif item.is_file(follow_symlinks=False):
            mode = item.stat(follow_symlinks=False).st_mode
            kind = dag.KIND_EXECUTABLE if _is_executable(mode) else dag.KIND_FILE
            # O_NOFOLLOW: it was a regular file a moment ago; don't follow it if swapped for a symlink since
            fd = os.open(item.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            with os.fdopen(fd, "rb") as f:
                child = build_file(f, put, kind=kind)
        else:
            warnings.append(f"skipped {shown}: not a regular file or directory")
            continue
        entries.append(Entry(item.name, child.cid, child.size, child.kind))
        child_height = max(child_height, child.height)
    return build_directory(entries, put, child_height)


def add_path(path: str, put: Put, wrap_it: bool = False) -> tuple[Built, list[str]]:
    """Build the file or directory at path, optionally wrapped in a directory under its own name.

    Returns the root and the warnings for what was skipped.
    """
    warnings: list[str] = []
    built = build_path(path, put, warnings)
    if wrap_it:
        name = os.path.basename(os.path.normpath(os.path.abspath(path)))
        built = wrap(name, built, put)
    return built, warnings


def add_stream(stream: BinaryIO, put: Put, size: int | None = None, name: str | None = None,
               executable: bool = False) -> Built:
    """Build a file from a stream, wrapped as the entry name in a directory if name is given."""
    kind = dag.KIND_EXECUTABLE if executable else dag.KIND_FILE
    if name is not None:
        dag.check_name(name)  # before reading a possibly large stream
    built = build_file(stream, put, size, kind)
    if name is not None:
        built = wrap(name, built, put)
    return built
