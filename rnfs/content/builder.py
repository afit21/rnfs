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
import errno
import io
import os
import stat
from dataclasses import dataclass
from typing import BinaryIO, Callable, Iterable

from rnfs import constants, fsutil
from rnfs.content import chunker, dag
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

    def make_group(level: int, pairs: list[tuple]) -> tuple:
        """A type 1 node over a group of chunks or file nodes."""
        node = FileNode(_items(pairs))
        return Child(_put_node(node, put), node.size), _height(pairs)

    levels = _Levels(make_group, constants.MAX_CHILDREN)
    # Fixed CHUNK_SIZE pieces unless `add --chunker fastcdc` selected content-defined chunks (chunker.using)
    for chunk in chunker.chunks(stream, size):
        cid = Cid.for_block(RAW, chunk)
        put(cid, chunk)
        levels.add(Child(cid, len(chunk)), 0)

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


def is_executable(mode: int) -> bool:
    """True if a file mode has the owner's execute bit, as git records it; always False on Windows.

    Windows has no execute bit, but Python's stat reports one for .exe, .bat, .cmd and .com files, so taking
    it would give the same tree a different CID there than elsewhere.
    """
    return not fsutil.IS_WINDOWS and bool(mode & stat.S_IXUSR)


# Opening paths. A directory being added may change underneath the walk (the shared folder is writable by
# whatever puts files there), so every entry is opened relative to its parent's descriptor without following
# a symlink or blocking on a FIFO, and the opened object must be the regular file or directory that was listed.
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_NONBLOCK = getattr(os, "O_NONBLOCK", 0)  # opening a FIFO for reading would otherwise wait for a writer
_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_REPARSE_JUNCTION = getattr(stat, "IO_REPARSE_TAG_MOUNT_POINT", 0xA0000003)
# Platforms without these (not Linux or macOS) walk directories by path, checking each entry before using it
FD_WALK = (os.open in os.supports_dir_fd and os.scandir in os.supports_fd
           and hasattr(os, "O_NOFOLLOW") and hasattr(os, "O_DIRECTORY"))
# What open(O_NOFOLLOW | O_NONBLOCK [| O_DIRECTORY]) reports for a symlink, socket or non-directory
_SWAPPED_ERRNOS = {errno.ELOOP, errno.EMLINK, errno.ENOTDIR, errno.ENXIO}

Handle = int | str  # an open file's descriptor; an open directory's descriptor, or its path without FD_WALK


class Changed(OSError):
    """An entry changed type or identity between being listed and being opened."""


def _check_opened(path: str, expected: os.stat_result, handle: Handle, follow: bool = False):
    """Raise Changed unless handle is the regular file or directory that was listed as expected.

    follow says whether a path handle (a directory without FD_WALK) was named through a symlink to it.
    """
    st = os.fstat(handle) if isinstance(handle, int) else os.stat(handle, follow_symlinks=follow)
    same_type = stat.S_IFMT(st.st_mode) == stat.S_IFMT(expected.st_mode)
    if not (same_type and (stat.S_ISREG(st.st_mode) or stat.S_ISDIR(st.st_mode))
            and (st.st_dev, st.st_ino) == (expected.st_dev, expected.st_ino)):
        raise Changed(errno.ESTALE, "changed while being read", display_path(path))


def _open(target: str, dir_fd: int | None, path: str, expected: os.stat_result, follow: bool) -> Handle:
    """Open target (a name in dir_fd, or a path) as the regular file or directory expected, never blocking."""
    is_dir = stat.S_ISDIR(expected.st_mode)
    if is_dir and not FD_WALK:
        handle: Handle = path  # can't open directories here; checked by path instead
    else:
        flags = os.O_RDONLY | fsutil.O_BINARY | _O_NONBLOCK | _O_CLOEXEC | (_O_DIRECTORY if is_dir else 0)
        if not follow:
            flags |= _O_NOFOLLOW
        try:
            handle = os.open(target, flags, dir_fd=dir_fd) if dir_fd is not None else os.open(target, flags)
        except OSError as e:
            if e.errno in _SWAPPED_ERRNOS:
                raise Changed(e.errno, "changed while being read", display_path(path)) from e
            raise
    try:
        _check_opened(path, expected, handle, follow)
    except BaseException:
        close(handle)
        raise
    return handle


def open_path(path: str, follow_symlinks: bool = True) -> tuple[Handle, os.stat_result]:
    """Open the regular file or directory at path without blocking, with its stat.

    With follow_symlinks, a symlink at path is followed (the user named it). Raises ValueError for anything
    else, such as a FIFO or device (which is never opened), Changed if it is swapped while being opened.
    """
    st = os.stat(path, follow_symlinks=follow_symlinks)
    if not (stat.S_ISREG(st.st_mode) or stat.S_ISDIR(st.st_mode)):
        raise ValueError(f"{display_path(path)} is not a regular file or directory")
    return _open(path, None, path, st, follow_symlinks), st


def open_child(parent: Handle, name: str, path: str, expected: os.stat_result) -> Handle:
    """Open entry name of the open directory parent (at path), which was listed as expected, not following it.

    Raises Changed if it is no longer that regular file or directory.
    """
    if isinstance(parent, int):
        return _open(name, parent, path, expected, False)
    return _open(path, None, path, expected, False)


def listed_stat(item: os.DirEntry, path: str) -> os.stat_result:
    """The stat of a listed entry (at path), not following a symlink.

    On Windows a DirEntry's stat has st_ino and st_dev zero, but opening checks them, so there it comes from
    the path instead (one more system call per entry).
    """
    st = item.stat(follow_symlinks=False)
    if st.st_ino == 0 and st.st_dev == 0:
        st = os.stat(path, follow_symlinks=False)
    return st


def is_link(st: os.stat_result) -> bool:
    """True for a symlink, or a Windows directory junction (which stat reports as a directory, yet it links)."""
    return stat.S_ISLNK(st.st_mode) or getattr(st, "st_reparse_tag", 0) == _REPARSE_JUNCTION


def close(handle: Handle):
    """Close a handle from open_path or open_child."""
    if isinstance(handle, int):
        os.close(handle)


def build_path(path: str, put: Put, warnings: list[str]) -> Built:
    """Build the file or directory at path, appending to warnings what was skipped.

    The path itself is followed if it is a symlink (the user named it); inside a directory, hidden files
    are included and symlinks, devices, sockets, FIFOs and invalid names are skipped with a warning.
    Raises TooLarge for a tree deeper than MAX_DEPTH, ValueError if path is neither file nor directory.
    """
    handle, st = open_path(path)
    try:
        return build_open(handle, st, path, put, warnings)
    finally:
        close(handle)


def build_open(handle: Handle, st: os.stat_result, path: str, put: Put, warnings: list[str],
               depth: int = 1) -> Built:
    """Build the regular file or directory open as handle (from open_path or open_child), whose stat is st.

    path is only for messages (and for directories without FD_WALK); depth as for _build_tree.
    """
    if stat.S_ISDIR(st.st_mode):
        return _build_tree(handle, path, put, warnings, depth)
    kind = dag.KIND_EXECUTABLE if is_executable(st.st_mode) else dag.KIND_FILE
    # Reading a regular file never blocks, so O_NONBLOCK can stay; the caller keeps the descriptor
    with os.fdopen(handle, "rb", closefd=False) as f:
        return build_file(f, put, kind=kind)


def _build_tree(directory: Handle, path: str, put: Put, warnings: list[str], depth: int) -> Built:
    """Build the open directory at path, which sits depth directories from the root (1 for the root)."""
    # Every directory level adds at least one DAG node, so this bounds the recursion before it gets deep
    if depth > constants.MAX_DEPTH:
        raise TooLarge(f"{display_path(path)} is nested deeper than MAX_DEPTH ({constants.MAX_DEPTH})")
    entries = []
    child_height = 0
    with os.scandir(directory) as listing:
        children = sorted(listing, key=lambda entry: entry.name)
    for item in children:
        child_path = os.path.join(path, item.name)
        shown = display_path(child_path)
        if not dag.is_valid_name(item.name):
            warnings.append(f"skipped {shown}: name is not valid UTF-8 or breaks the name rules")
            continue
        st = listed_stat(item, child_path)
        if is_link(st):
            warnings.append(f"skipped symlink {shown}")
            continue
        if not (stat.S_ISDIR(st.st_mode) or stat.S_ISREG(st.st_mode)):
            warnings.append(f"skipped {shown}: not a regular file or directory")
            continue
        try:
            handle = open_child(directory, item.name, child_path, st)
        except Changed:
            warnings.append(f"skipped {shown}: it changed while being added")
            continue
        try:
            child = build_open(handle, st, child_path, put, warnings, depth + 1)
        finally:
            close(handle)
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
