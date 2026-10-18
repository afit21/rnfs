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
callback (NO_PUT) gives only-hash mode. Files are streamed, one chunk in memory at a time. A build's
BuildOptions are passed explicitly: the chunker, which decides files' CIDs, and check, which is given each
file's whole-content SHA-256 (the deny list's file hashes) and may raise to stop the build before the file's
root node is stored. Explicit, because a ContextVar isn't copied to a worker thread: a build moved onto one would
silently fall back to the fixed chunker and skip the check (maintainability audit CT2). Without options, a build
takes them from chunker.using() and checking(), the wrappers that set them for the current context.
"""
import errno
import hashlib
import io
import os
import stat
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import BinaryIO, Callable, Iterable, Iterator, Protocol

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
    # A file's content SHA-256, when the build computed it (its BuildOptions had a check); None otherwise
    digest: bytes | None = field(default=None, compare=False)


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


@dataclass(frozen=True)
class BuildOptions:
    """What a build needs beyond its input, passed down explicitly so it holds on any thread.

    chunker: the name of the chunker cutting files (chunker.FIXED, the default, or FASTCDC); it decides their CIDs.
    check: called with each file's content SHA-256 once it is read, before the file's root node is stored; it may
    raise to stop the build (the deny list's file hashes). None checks nothing, and hashes nothing for it.
    progress: called with the length of each read from a file of the tree being built (build_open: a path's files,
    not a stream's), and with 0 for each directory entry met, so a walk over empty files reports too; it may raise
    to stop the build (an add job's cancel). None reports nothing.
    Raises ValueError for an unknown chunker.
    """

    chunker: str = chunker.FIXED
    check: Callable[[bytes], None] | None = None
    progress: Callable[[int], None] | None = None

    def __post_init__(self):
        """Check the chunker's name (None is the default)."""
        object.__setattr__(self, "chunker", chunker.check(self.chunker or chunker.FIXED))


# What build_file passes each file's content SHA-256 to in this thread/context (checking), if anything
_check: ContextVar[Callable[[bytes], None] | None] = ContextVar("rnfs_file_check", default=None)


@contextmanager
def checking(check: Callable[[bytes], None] | None) -> Iterator[None]:
    """Within the block, a build given no BuildOptions calls check(digest) with each file's content SHA-256 once it
    is read, before the file's root node is stored; check may raise to stop the build. None checks nothing.

    A wrapper kept for callers that don't pass BuildOptions yet: it holds only in this thread/context."""
    token = _check.set(check)
    try:
        yield
    finally:
        _check.reset(token)


def current_options() -> BuildOptions:
    """The BuildOptions that chunker.using() and checking() set for this thread/context (the defaults if neither)."""
    return BuildOptions(chunker.active(), _check.get())


def _resolve(options: BuildOptions | None) -> BuildOptions:
    """options, or those of this context (current_options) if None."""
    return options if options is not None else current_options()


def _chunks(stream: BinaryIO, size: int | None, name: str) -> Iterator[bytes]:
    """A file's chunks by the chunker called name (as chunker.chunks gives them for the active one)."""
    if name == chunker.FASTCDC:
        return chunker.fastcdc_stream(stream, size)
    return chunker.fixed_chunks(stream, size)


def build_file(stream: BinaryIO, put: Put, size: int | None = None, kind: int = dag.KIND_FILE,
               options: BuildOptions | None = None) -> Built:
    """Build a file from everything read from stream, or exactly size bytes of it (ValueError if it ends early).

    options: the chunker and check (by default those of this context, current_options)."""

    def make_group(level: int, pairs: list[tuple]) -> tuple:
        """A type 1 node over a group of chunks or file nodes."""
        node = FileNode(_items(pairs))
        return Child(_put_node(node, put), node.size), _height(pairs)

    levels = _Levels(make_group, constants.MAX_CHILDREN)
    options = _resolve(options)
    check = options.check
    digest = hashlib.sha256() if check is not None else None
    # Fixed CHUNK_SIZE pieces unless `add --chunker fastcdc` selected content-defined chunks
    for chunk in _chunks(stream, size, options.chunker):
        cid = Cid.for_block(RAW, chunk)
        if digest is not None:
            digest.update(chunk)
        put(cid, chunk)
        levels.add(Child(cid, len(chunk)), 0)
    content_hash = None
    if check is not None:
        content_hash = digest.digest()
        check(content_hash)

    level, pairs = levels.finish()
    if level == 0 and not pairs:
        put(EMPTY_FILE, b"")
        return Built(EMPTY_FILE, 0, kind, 0, content_hash)
    if level == 0 and len(pairs) == 1:
        chunk_child = pairs[0][0]
        return Built(chunk_child.cid, chunk_child.size, kind, 0, content_hash)
    child, height = make_group(level, pairs)
    _check_depth(height)
    return Built(child.cid, child.size, kind, height, content_hash)


def build_bytes(data: bytes, put: Put, kind: int = dag.KIND_FILE, options: BuildOptions | None = None) -> Built:
    """Build a file from bytes in memory (options as for build_file)."""
    return build_file(io.BytesIO(data), put, len(data), kind, options)


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
# Where directories can't be opened (FD_WALK is False, as on Windows) they are walked by path, and a directory
# checked once could be swapped for a link or junction afterwards: so each one is checked again, by stat and by
# real path, whenever it is listed, and each file opened below it must really be below the walk's root (its
# final path, from the open file) before a byte is read (audit L8).
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_NONBLOCK = getattr(os, "O_NONBLOCK", 0)  # opening a FIFO for reading would otherwise wait for a writer
_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
# Platforms without these (not Linux or macOS) walk directories by path, checking each entry before using it
FD_WALK = (os.open in os.supports_dir_fd and os.scandir in os.supports_fd
           and hasattr(os, "O_NOFOLLOW") and hasattr(os, "O_DIRECTORY"))
# What open(O_NOFOLLOW | O_NONBLOCK [| O_DIRECTORY]) reports for a symlink, socket or non-directory
_SWAPPED_ERRNOS = {errno.ELOOP, errno.EMLINK, errno.ENOTDIR, errno.ENXIO}

Handle = int | str  # an open file's descriptor; an open directory's descriptor, or a _DirPath without FD_WALK


class Changed(OSError):
    """An entry changed type or identity between being listed and being opened."""


class _DirPath(str):
    """A directory "opened" without FD_WALK: its path, with what is needed to check it again before each use.

    expected is the stat it was listed (or first opened) with, follow whether its path may end in a link (only
    the root the user named), and root the real path of the walk's root, which nothing below may leave.
    """
    expected: os.stat_result
    follow: bool
    root: str


def _dir_path(path: str, expected: os.stat_result, follow: bool, root: str | None) -> _DirPath:
    """A _DirPath for path; root None makes it the root of a walk, whose real path must still be it."""
    handle = _DirPath(path)
    handle.expected = expected
    handle.follow = follow
    if root is None:
        root = os.path.realpath(path)
        st = os.stat(root)
        if (st.st_dev, st.st_ino) != (expected.st_dev, expected.st_ino):
            raise Changed(errno.ESTALE, "changed while being read", display_path(path))
    handle.root = root
    return handle


def _check_dir_path(handle: _DirPath):
    """Raise Changed unless the path of handle still leads, without a link or junction on the way below its
    root, to the directory it was opened as."""
    changed = Changed(errno.ESTALE, "changed while being read", display_path(handle))
    st = os.stat(handle, follow_symlinks=handle.follow)
    if (is_link(st) or not stat.S_ISDIR(st.st_mode)
            or (st.st_dev, st.st_ino) != (handle.expected.st_dev, handle.expected.st_ino)):
        raise changed
    # A parent directory swapped for a link would still pass the stat above, if it leads to the same directory
    # by another way; the real path shows where it is
    real = os.path.realpath(handle)
    if not fsutil.is_below(real, handle.root):
        raise changed
    st = os.stat(real)
    if (st.st_dev, st.st_ino) != (handle.expected.st_dev, handle.expected.st_ino):
        raise changed


def _check_below(fd: int, root: str, path: str):
    """Raise Changed unless the file open as fd (opened by path, below a _DirPath) is really below root.

    Its final path comes from the open file where the system can say (fsutil.final_path): a directory on the
    way, swapped for a link or junction after its check, can't hide where the open led. Elsewhere the path is
    resolved again, and must lead to this very file below root.
    """
    changed = Changed(errno.ESTALE, "changed while being read", display_path(path))
    final = fsutil.final_path(fd)
    if final is None:
        final = os.path.realpath(path)
        st, opened = os.stat(final), os.fstat(fd)
        if (st.st_dev, st.st_ino) != (opened.st_dev, opened.st_ino):
            raise changed
    if not fsutil.is_below(final, root):
        raise changed


def list_directory(handle: Handle) -> list[os.DirEntry]:
    """The entries of the open directory handle, sorted by name.

    A _DirPath is checked before and after it is listed, so a listing of another directory swapped in
    meanwhile is never used. Raises Changed then, OSError if it can't be listed.
    """
    if isinstance(handle, _DirPath):
        _check_dir_path(handle)
    with os.scandir(handle) as listing:
        entries = sorted(listing, key=lambda entry: entry.name)
    if isinstance(handle, _DirPath):
        _check_dir_path(handle)
    return entries


def _check_opened(path: str, expected: os.stat_result, handle: Handle, follow: bool = False):
    """Raise Changed unless handle is the regular file or directory that was listed as expected.

    follow says whether a path handle (a directory without FD_WALK) was named through a symlink to it.
    """
    st = os.fstat(handle) if isinstance(handle, int) else os.stat(handle, follow_symlinks=follow)
    same_type = stat.S_IFMT(st.st_mode) == stat.S_IFMT(expected.st_mode)
    if not (same_type and (stat.S_ISREG(st.st_mode) or stat.S_ISDIR(st.st_mode))
            and (st.st_dev, st.st_ino) == (expected.st_dev, expected.st_ino)):
        raise Changed(errno.ESTALE, "changed while being read", display_path(path))


def _open(target: str, dir_fd: int | None, path: str, expected: os.stat_result, follow: bool,
          root: str | None = None) -> Handle:
    """Open target (a name in dir_fd, or a path) as the regular file or directory expected, never blocking.

    root is the real path of the walk's root when target is a path below a _DirPath, else None.
    """
    is_dir = stat.S_ISDIR(expected.st_mode)
    if is_dir and not FD_WALK:
        handle: Handle = _dir_path(path, expected, follow, root)  # can't open directories here: checked by path
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
        if isinstance(handle, _DirPath):
            _check_dir_path(handle)
        elif root is not None:
            _check_below(handle, root, path)
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
    root = getattr(parent, "root", None) or os.path.realpath(parent)
    return _open(path, None, path, expected, False, root)


def listed_stat(item: os.DirEntry, path: str) -> os.stat_result:
    """The stat of a listed entry (at path), not following a symlink.

    On Windows a DirEntry's stat has st_ino and st_dev zero, but opening checks them, so there it comes from
    the path instead (one more system call per entry).
    """
    st = item.stat(follow_symlinks=False)
    if st.st_ino == 0 and st.st_dev == 0:
        st = os.stat(path, follow_symlinks=False)
    return st


is_link = fsutil.is_link  # a symlink, junction or other link-like Windows reparse point


def close(handle: Handle):
    """Close a handle from open_path or open_child."""
    if isinstance(handle, int):
        os.close(handle)


def build_path(path: str, put: Put, warnings: list[str], options: BuildOptions | None = None) -> Built:
    """Build the file or directory at path, appending to warnings what was skipped (options as for build_file).

    The path itself is followed if it is a symlink (the user named it); inside a directory, hidden files
    are included and symlinks, devices, sockets, FIFOs and invalid names are skipped with a warning.
    Raises TooLarge for a tree deeper than MAX_DEPTH, ValueError if path is neither file nor directory.
    """
    handle, st = open_path(path)
    try:
        return build_open(handle, st, path, put, warnings, options=options)
    finally:
        close(handle)


FileState = tuple[int, int, int, int, bool]


def file_state(st: os.stat_result) -> FileState:
    """What a FileCache knows a regular file by: (device, inode, size, mtime in ns, execute bit). The trust is the
    shared folder's signature's: a file rewritten with the same size and modification time looks unchanged."""
    return st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, is_executable(st.st_mode)


class FileCache(Protocol):
    """What build_open asks about each regular file before reading it, and tells after reading one (the shared
    folder's per-file cache, audit D1). st is the open file's stat, taken before it is read."""

    def lookup(self, st: os.stat_result, kind: int) -> Built | None:
        """The Built of the file whose stat is st, from an earlier build, if it may stand for the file now: its
        blocks all stored, and protected for this build. None to read the file."""

    def remember(self, st: os.stat_result, built: Built):
        """The file whose stat was st, unchanged while it was read, was built as built."""


def hard_links(handle: Handle) -> int:
    """How many hard links the open regular file handle has beyond one (0 for a directory)."""
    if not isinstance(handle, int):
        return 0
    st = os.fstat(handle)
    return st.st_nlink - 1 if stat.S_ISREG(st.st_mode) and st.st_nlink > 1 else 0


def build_open(handle: Handle, st: os.stat_result, path: str, put: Put, warnings: list[str],
               depth: int = 1, skip_hard_links: bool = False, options: BuildOptions | None = None,
               cache: FileCache | None = None) -> Built:
    """Build the regular file or directory open as handle (from open_path or open_child), whose stat is st.

    path is only for messages (and for directories without FD_WALK); depth as for _build_tree. With
    skip_hard_links, files below a directory that have other hard links are skipped with a warning (the shared
    folder: someone who may write to it could link a file of yours there that you never put in it). options as
    for build_file, taken from this context once, here, if not given: the whole tree is built with them. cache,
    if given, may stand in for reading each regular file (_build_cached).
    """
    options = _resolve(options)
    if stat.S_ISDIR(st.st_mode):
        return _build_tree(handle, path, put, warnings, depth, skip_hard_links, options, cache)
    kind = dag.KIND_EXECUTABLE if is_executable(st.st_mode) else dag.KIND_FILE
    if cache is not None:
        return _build_cached(handle, put, kind, options, cache)
    # Reading a regular file never blocks, so O_NONBLOCK can stay; the caller keeps the descriptor
    with os.fdopen(handle, "rb", closefd=False) as f:
        return build_file(counted(f, options.progress), put, kind=kind, options=options)


class _Counted:
    """A binary reader passing the length of each read on to progress (BuildOptions.progress)."""

    def __init__(self, stream: BinaryIO, progress: Callable[[int], None]):
        """Read from stream, reporting to progress."""
        self.stream = stream
        self.progress = progress

    def read(self, size: int = -1) -> bytes:
        """Up to size bytes of the stream, reported once read (progress may raise to stop the build)."""
        data = self.stream.read(size)
        self.progress(len(data))
        return data


def counted(stream: BinaryIO, progress: Callable[[int], None] | None) -> BinaryIO:
    """stream, reporting the length of each read to progress if it is given (BuildOptions.progress)."""
    return stream if progress is None else _Counted(stream, progress)


def _build_cached(handle: Handle, put: Put, kind: int, options: BuildOptions, cache: FileCache) -> Built:
    """build_open of the regular file open as handle, through cache: a file it knows isn't read again.

    Its remembered content hash still goes to options.check, so a hash denied since is refused as if the file
    were read; a file remembered without one (built when there was no check) is read again when there is a check.
    """
    before = os.fstat(handle)
    known = cache.lookup(before, kind)
    if known is not None and (options.check is None or known.digest is not None):
        if options.check is not None:
            options.check(known.digest)
        return known
    with os.fdopen(handle, "rb", closefd=False) as f:
        built = build_file(f, put, kind=kind, options=options)
    # Changed while being read, what was read may be neither version: not remembered, so it is read again next time
    if built.size == before.st_size and file_state(os.fstat(handle)) == file_state(before):
        cache.remember(before, built)
    return built


def _build_tree(directory: Handle, path: str, put: Put, warnings: list[str], depth: int,
                skip_hard_links: bool, options: BuildOptions, cache: FileCache | None = None) -> Built:
    """Build the open directory at path, which sits depth directories from the root (1 for the root)."""
    # Every directory level adds at least one DAG node, so this bounds the recursion before it gets deep
    if depth > constants.MAX_DEPTH:
        raise TooLarge(f"{display_path(path)} is nested deeper than MAX_DEPTH ({constants.MAX_DEPTH})")
    entries = []
    child_height = 0
    children = list_directory(directory)
    for item in children:
        if options.progress is not None:
            options.progress(0)  # a cancel check even where no file is read (a tree of empty directories)
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
            if skip_hard_links and (links := hard_links(handle)):
                warnings.append(f"skipped {shown}: {hard_link_reason(links)}")
                continue
            child = build_open(handle, st, child_path, put, warnings, depth + 1, skip_hard_links, options, cache)
        except Changed:
            # A directory walked by path, swapped when it was listed (see list_directory)
            warnings.append(f"skipped {shown}: it changed while being added")
            continue
        finally:
            close(handle)
        entries.append(Entry(item.name, child.cid, child.size, child.kind))
        child_height = max(child_height, child.height)
    return build_directory(entries, put, child_height)


def tree_size(path: str, check: Callable[[], None] | None = None) -> int:
    """The bytes add_path would read at path: a regular file's size (path followed if a symlink, as the add follows
    it), or the sizes of the regular files below a directory that _build_tree reads, which skips symlinks, other
    types and invalid names. An add job's total_bytes, measured before it reads anything: files changing meanwhile
    make it an estimate. Unreadable directories count nothing (the add reports them); check(), if given, is called
    for each entry and may raise (a job's cancel)."""
    st = os.stat(path)
    if stat.S_ISREG(st.st_mode):
        return st.st_size
    if not stat.S_ISDIR(st.st_mode):
        return 0
    total = 0
    stack = [(path, 1)]
    while stack:
        directory, depth = stack.pop()
        if depth > constants.MAX_DEPTH:
            continue  # the add refuses it (TooLarge) when it gets there
        try:
            listing = os.scandir(directory)
        except OSError:
            continue
        with listing:
            for item in listing:
                if check is not None:
                    check()
                if not dag.is_valid_name(item.name):
                    continue
                try:
                    st = item.stat(follow_symlinks=False)
                except OSError:
                    continue
                if is_link(st):
                    continue
                if stat.S_ISREG(st.st_mode):
                    total += st.st_size
                elif stat.S_ISDIR(st.st_mode):
                    stack.append((item.path, depth + 1))
    return total


def hard_link_reason(links: int) -> str:
    """Why a file with links other hard links is skipped (shared folder)."""
    return (f"it has {links} other hard link{'s' if links > 1 else ''}, so it is also a file elsewhere that may "
            f"not be meant for sharing")


def add_path(path: str, put: Put, wrap_it: bool = False,
             options: BuildOptions | None = None) -> tuple[Built, list[str]]:
    """Build the file or directory at path, optionally wrapped in a directory under its own name (options as for
    build_file).

    Returns the root and the warnings for what was skipped.
    """
    warnings: list[str] = []
    built = build_path(path, put, warnings, options)
    if wrap_it:
        name = os.path.basename(os.path.normpath(os.path.abspath(path)))
        built = wrap(name, built, put)
    return built, warnings


def add_stream(stream: BinaryIO, put: Put, size: int | None = None, name: str | None = None,
               executable: bool = False, options: BuildOptions | None = None) -> Built:
    """Build a file from a stream, wrapped as the entry name in a directory if name is given (options as for
    build_file)."""
    kind = dag.KIND_EXECUTABLE if executable else dag.KIND_FILE
    if name is not None:
        dag.check_name(name)  # before reading a possibly large stream
    built = build_file(stream, put, size, kind, options)
    if name is not None:
        built = wrap(name, built, put)
    return built
