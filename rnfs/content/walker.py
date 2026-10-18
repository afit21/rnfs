"""Walking DAGs: reader checks, path lookup, byte ranges, listing and writing out (sections 5 and 7).
No RNS dependency.

Reader checks are driven by an expectation (Expect) of what a block must be, derived from the node that
listed it: allowed types, size, depth, and for children of an index the first name and an upper bound on
the names below. expand() checks a verified block against its expectation and returns its children's.
The fetcher drives expand/step/range_children itself; the local operations at the end of this module
take a blocking `get(expect) -> bytes` that raises NotFound when a block is absent.

Repeated CIDs: every occurrence is checked against its own expectation, but identical expectations
(the same CID listed the same way at the same depth) are checked once, which is what keeps DAGs that
repeat subtrees cheap to walk.
"""
import bisect
import errno
import os
import stat
import threading
from collections import OrderedDict
from dataclasses import dataclass, replace
from typing import BinaryIO, Callable, Iterator

from rnfs import constants, fsutil
from rnfs.content import dag
from rnfs.content.cid import Cid
from rnfs.content.dag import DirectoryNode, Entry, FileNode, IndexNode
from rnfs.errors import InvalidDag, NotFound, TooLarge
from rnfs.log import LOG_WARNING, log

# Errors creating an entry that mean this filesystem can't take its name: the entry is skipped and reported
_NAME_ERRORS = frozenset({errno.EINVAL, errno.ENAMETOOLONG, errno.EILSEQ})

RAW_TYPE, FILE_TYPE, DIRECTORY_TYPE, INDEX_TYPE = "raw", "file", "directory", "index"
ANY_TYPES = frozenset({RAW_TYPE, FILE_TYPE, DIRECTORY_TYPE, INDEX_TYPE})
FILE_TYPES = frozenset({RAW_TYPE, FILE_TYPE})  # an entry of kind 0 or 1, or a file node's child
DIRECTORY_TYPES = frozenset({DIRECTORY_TYPE, INDEX_TYPE})  # an entry of kind 2, or an index's child
# height()'s memo of checked subtrees' heights, kept across calls (least recently used forgotten first): every
# entry of a directory of tens of thousands, in about 11 MB when full
HEIGHT_MEMO = 32768


@dataclass(frozen=True)
class Expect:
    """What a block must be, from the node that listed it (or nothing, for a root).

    types: allowed types, a subset of ANY_TYPES. size: content bytes below it, or None if unknown.
    depth: DAG nodes from the root down to this block, counting itself (the root is 1).
    first_name: for a child of an index, the name the first name below it must equal.
    below: for a child of an index, an exclusive upper bound for every name below it.
    kind: for a directory entry, its kind (dag.KIND_*), so an executable target can be recognised.
    """

    cid: Cid
    size: int | None
    types: frozenset[str]
    depth: int
    first_name: str | None = None
    below: str | None = None
    kind: int | None = None


Get = Callable[[Expect], bytes]


def root_expect(cid: Cid) -> Expect:
    """The expectation for a root: any type, any size, depth 1."""
    return Expect(cid, None, ANY_TYPES, 1)


def check_cid(expect: Expect):
    """Raise InvalidDag if expect's CID can't be of an allowed type, so a block needn't be fetched to know."""
    if expect.cid.is_raw:
        if RAW_TYPE not in expect.types:
            raise InvalidDag(f"{expect.cid} is a raw block where a {_types(expect)} node is required")
    elif not expect.types - {RAW_TYPE}:
        raise InvalidDag(f"{expect.cid} is a DAG node where a raw block is required")


def check_raw(expect: Expect, length: int):
    """Check a raw block of length bytes against expect. Raises InvalidDag."""
    check_cid(expect)
    if length > constants.MAX_BLOCK_SIZE:
        raise InvalidDag(f"{expect.cid} is larger than MAX_BLOCK_SIZE")
    if expect.size is not None and length != expect.size:
        raise InvalidDag(f"{expect.cid} has {length} bytes, but its parent says {expect.size}")


def _types(expect: Expect) -> str:
    """Allowed types for messages."""
    return " or ".join(sorted(expect.types))


def check_node(expect: Expect, node: dag.Node):
    """Check a decoded DAG node against expect: type, depth, size and name bounds. Raises InvalidDag."""
    check_cid(expect)
    type_name = dag.node_type_name(node)
    if type_name not in expect.types:
        raise InvalidDag(f"{expect.cid} is a {type_name} node where a {_types(expect)} is required")
    if expect.depth > constants.MAX_DEPTH:
        raise InvalidDag(f"{expect.cid} is deeper than MAX_DEPTH ({constants.MAX_DEPTH})")
    if expect.size is not None and node.size != expect.size:
        raise InvalidDag(f"{expect.cid} holds {node.size} bytes, but its parent says {expect.size}")
    names = node.names
    if expect.first_name is not None:
        if not names or names[0] != expect.first_name:
            raise InvalidDag(f"{expect.cid}: first name below an index child isn't its first_name")
    if expect.below is not None and names and not names[-1] < expect.below:
        raise InvalidDag(f"{expect.cid}: name {names[-1]!r} isn't below the next index child's first_name")


def children(expect: Expect, node: dag.Node) -> list[Expect]:
    """The expectations for a checked node's children, in order."""
    depth = expect.depth + 1
    if isinstance(node, FileNode):
        return [Expect(child.cid, child.size, FILE_TYPES, depth) for child in node.children]
    if isinstance(node, DirectoryNode):
        return [Expect(entry.cid, entry.size, DIRECTORY_TYPES if entry.kind == dag.KIND_DIRECTORY else FILE_TYPES,
                       depth, kind=entry.kind) for entry in node.entries]
    result = []
    for i, child in enumerate(node.children):
        below = node.children[i + 1].first_name if i + 1 < len(node.children) else expect.below
        result.append(Expect(child.cid, child.size, DIRECTORY_TYPES, depth, child.first_name, below))
    return result


def decode_checked(expect: Expect, block: bytes) -> dag.Node | None:
    """Check a verified block against expect; the decoded node, or None for a raw block. Raises InvalidDag."""
    if expect.cid.is_raw:
        check_raw(expect, len(block))
        return None
    check_cid(expect)
    node = dag.decode(block)
    check_node(expect, node)
    return node


def expand(expect: Expect, block: bytes) -> list[Expect]:
    """Check a verified block against its expectation and return its children's. Raises InvalidDag."""
    node = decode_checked(expect, block)
    return [] if node is None else children(expect, node)


def is_index_step(expect: Expect) -> bool:
    """True if expect came from stepping into an index, so the same name must be stepped again."""
    return expect.first_name is not None


def index_child(node: IndexNode, name: str) -> int | None:
    """The position of the child of an index node that would hold name, if any is: the one with the greatest
    first_name <= name (first names are strictly increasing in a decoded node); None if name sorts before them
    all. The one rule for stepping through an index, here and in Storage."""
    position = bisect.bisect_right(node.names, name) - 1
    return None if position < 0 else position


def step(expect: Expect, block: bytes, name: str) -> Expect | None:
    """One path step: the entry called name in a directory node, or for an index the child that would hold it.

    Returns None if there is no such name, or if the block is a file. After an index step (is_index_step),
    step again with the same name until a directory entry is reached.
    """
    node = decode_checked(expect, block)
    if not isinstance(node, (DirectoryNode, IndexNode)):
        return None
    child_expects = children(expect, node)
    if isinstance(node, DirectoryNode):
        for entry, child in zip(node.entries, child_expects):
            if entry.name == name:
                return child
        return None
    position = index_child(node, name)
    return None if position is None else child_expects[position]


def range_children(expect: Expect, block: bytes, offset: int, length: int) -> list[tuple[Expect, int, int]]:
    """Children of a file node overlapping [offset, offset + length), each with the range relative to it."""
    node = decode_checked(expect, block)
    if not isinstance(node, FileNode):
        return []
    result = []
    start = 0
    end = offset + length
    for child in children(expect, node):
        if start >= end:
            break
        child_end = start + child.size
        low, high = max(offset, start), min(end, child_end)
        # Empty children hold none of the range: skipping them keeps a range walk proportional to its bytes
        if high > low:
            result.append((child, low - start, high - low))
        start = child_end
    return result


class SmallLeaves:
    """Counts the small leaves a read meets, so a DAG of tiny chunks can't make one read endless.

    Honest files have CHUNK_SIZE chunks, so a read meets at most two shorter pieces (where its range starts
    and ends), plus one per file written out. Pieces shorter than CHUNK_SIZE / 16 count against
    max_output_entries; larger ones cost little per byte and are free.
    """

    def __init__(self, max_entries: int):
        """Allow max_entries small leaves, plus the two at a range's ends."""
        self.left = max_entries + 2

    def meet(self, length: int):
        """Count a leaf piece of length bytes. Raises TooLarge once too many were small."""
        if length < constants.CHUNK_SIZE // 16:
            self.left -= 1
            if self.left < 0:
                raise TooLarge("file has more small chunks than max_output_entries allows")


def parse_target(text: str) -> tuple[Cid, list[str]]:
    """The root CID and path names of a target "<cid>[/name...]" (one trailing slash allowed). ValueError if
    malformed."""
    if not isinstance(text, str) or not text:
        raise ValueError("empty target")
    parts = text.split("/")
    if len(parts) > 1 and parts[-1] == "":
        parts.pop()  # "<cid>/dir/" names the same directory as "<cid>/dir"
    cid = Cid.parse(parts[0])
    names = parts[1:]
    for name in names:
        dag.check_name(name)
    return cid, names


def kind_of(expect: Expect, block: bytes) -> str:
    """FILE_TYPE ("file") or DIRECTORY_TYPE ("directory"): what the checked block is the root of."""
    node = decode_checked(expect, block)
    return DIRECTORY_TYPE if isinstance(node, (DirectoryNode, IndexNode)) else FILE_TYPE


def size_of(expect: Expect, block: bytes) -> int:
    """Content bytes below a checked block."""
    node = decode_checked(expect, block)
    return len(block) if node is None else node.size


# Local operations, over a blocking get(expect) -> bytes that raises NotFound when a block is absent


def step_name(expect: Expect, name: str, get: Get) -> Expect | None:
    """The entry called name in the directory expect names, stepping through its index nodes; None if there is
    no such name (or expect is a file)."""
    while True:
        child = step(expect, get(expect), name)
        if child is None or not is_index_step(child):
            return child
        expect = child


def resolve(root: Cid, names: list[str], get: Get) -> Expect:
    """The expectation for the target <root>/<names...>. Raises NotFound if a name doesn't exist."""
    expect = root_expect(root)
    for i, name in enumerate(names):
        expect = step_name(expect, name, get)
        if expect is None:
            raise NotFound(f"{root}/{'/'.join(names[:i + 1])} does not exist")
    return expect


def iter_dag(expect: Expect, get: Get) -> Iterator[tuple[Expect, bytes]]:
    """Every block of a DAG as (expect, block), depth-first, each CID once; every occurrence is checked."""
    return iter_dags([expect], get)


def iter_dags(expects: list[Expect], get: Get) -> Iterator[tuple[Expect, bytes]]:
    """Every block of the DAGs below expects as (expect, block), depth-first from each in turn, each CID once
    over them all; every occurrence is checked.

    Kept for the whole walk, so a large export holds little per block: each raw block's length (all its
    check needs) and the DAG node expectations already checked. A node met again under another expectation
    is read again rather than having every node's bytes kept.
    """
    raw_lengths: dict[bytes, int] = {}  # binary CID -> length, for every raw block yielded
    node_expects: set[Expect] = set()  # DAG node expectations checked
    nodes: set[Cid] = set()  # DAG nodes yielded
    for root in expects:
        stack = [root]
        while stack:
            current = stack.pop()
            cid = current.cid
            if cid.is_raw:
                length = raw_lengths.get(cid.binary)
                if length is not None:
                    check_raw(current, length)
                    continue
            elif current in node_expects:
                continue
            check_cid(current)  # before reading a block that can't be right
            block = get(current)
            child_expects = expand(current, block)
            if cid.is_raw:
                raw_lengths[cid.binary] = len(block)
                yield current, block
                continue
            node_expects.add(current)
            if cid not in nodes:
                nodes.add(cid)
                yield current, block
            stack.extend(reversed(child_expects))


def read_range(expect: Expect, get: Get, offset: int = 0, length: int | None = None,
               small: SmallLeaves | None = None) -> Iterator[bytes]:
    """The bytes of a file in [offset, offset + length) (to the end if length is None), in order.

    Raises ValueError if the target is a directory, TooLarge if small (if given) runs out.
    """
    if offset < 0 or (length is not None and length < 0):
        raise ValueError("offset and length must be non-negative")
    block = get(expect)
    node = decode_checked(expect, block)
    if node is None:
        end = len(block) if length is None else min(len(block), offset + length)
        if offset < end:
            if small is not None:
                small.meet(end - offset)
            yield block[offset:end]
        return
    if not isinstance(node, FileNode):
        raise ValueError(f"{expect.cid} is a directory")
    if length is None:
        length = max(0, node.size - offset)
    # Depth-first over the chunks covering the range; DAG node blocks are read as they're reached
    stack: list[tuple[Expect, int, int, bytes | None]] = [(expect, offset, length, block)]
    while stack:
        current, child_offset, child_length, current_block = stack.pop()
        if current_block is None:
            check_cid(current)
            current_block = get(current)
        if current.cid.is_raw:
            check_raw(current, len(current_block))
            if small is not None:
                small.meet(child_length)
            yield current_block[child_offset:child_offset + child_length]
            continue
        parts = range_children(current, current_block, child_offset, child_length)
        stack.extend((child, o, n, None) for child, o, n in reversed(parts))


def leaf_bytes(expect: Expect, get: Get, stop_at: int | None = None) -> int:
    """Bytes of the distinct raw chunks of the file expect names: each distinct raw CID's size once, as the file
    node listing it first gives it. What hashing the file costs beyond this is repetition (the deny list's bound).

    Only file nodes are read, each distinct one once; raw blocks never are (but a one-block file, whose length
    this is), so get must serve a DAG whose chunks are all stored and checked (fetch_dag's, a complete root's) for
    the count to be what they hold. Stops once
    stop_at bytes are counted, returning at least stop_at. Raises ValueError for a directory, InvalidDag for a
    node that breaks a reader check or a chunk said to be larger than MAX_BLOCK_SIZE.
    """
    if expect.cid.is_raw:
        return size_of(expect, get(expect))
    counted: set[Cid] = set()  # raw chunks counted
    nodes: set[Cid] = {expect.cid}  # file nodes met
    total = 0
    stack = [expect]
    while stack:
        current = stack.pop()
        check_cid(current)
        node = decode_checked(current, get(current))
        if not isinstance(node, FileNode):
            raise ValueError(f"{expect.cid} is a directory")
        for child in children(current, node):
            if child.cid.is_raw:
                if child.cid not in counted:
                    # Its block isn't read, so its parent's claim is what counts: the cap keeps a claim from
                    # counting more than any chunk can hold (reading it would find the DAG invalid anyway)
                    check_raw(child, child.size)
                    counted.add(child.cid)
                    total += child.size
                    if stop_at is not None and total >= stop_at:
                        return total
            elif child.cid not in nodes:
                nodes.add(child.cid)
                stack.append(child)
    return total


def _directory_entries(expect: Expect, get: Get, memo: dict | None = None) -> Iterator[tuple[Entry, Expect]]:
    """Every entry of a directory (through its index nodes) in name order, with the entry's expectation.

    Raises ValueError if the target is a file.
    """
    stack = [expect]
    first = True
    while stack:
        current = stack.pop()
        node = memo.get(current) if memo is not None else None
        if node is None:
            check_cid(current)
            node = decode_checked(current, get(current))
            if memo is not None and node is not None:
                memo[current] = node
        if first and not isinstance(node, (DirectoryNode, IndexNode)):
            raise ValueError(f"{expect.cid} is not a directory")
        first = False
        if isinstance(node, DirectoryNode):
            yield from zip(node.entries, children(current, node))
        else:
            stack.extend(reversed(children(current, node)))


def directory_entries(expect: Expect, get: Get, memo: dict | None = None) -> Iterator[tuple[Entry, Expect]]:
    """Every entry of a directory in name order with the entry's expectation (memo: directory and index nodes
    already decoded, kept for repeated subtrees). Raises ValueError for a file. Unbounded: the caller counts."""
    return _directory_entries(expect, get, memo)


def list_directory(expect: Expect, get: Get, max_entries: int | None = None) -> Iterator[Entry]:
    """The entries of a directory in name order. Raises ValueError for a file, TooLarge past max_entries."""
    for count, (entry, _) in enumerate(_directory_entries(expect, get), 1):
        if max_entries is not None and count > max_entries:
            raise TooLarge(f"directory has more than max_output_entries ({max_entries}) entries")
        yield entry


def count_entries(expect: Expect, get: Get, limit: int) -> int:
    """Directory entries in a DAG, counting every occurrence. Raises TooLarge once over limit.

    Reads only directory and index nodes; a subtree repeated under the same expectation is counted from
    a memo rather than walked again, so repeated empty subtrees cost nothing but are still counted.
    """
    memo: dict[Expect, int] = {}
    total = _count(expect, get, memo, limit)
    if total > limit:
        raise TooLarge(f"more than max_output_entries ({limit}) entries")
    return total


def _count(expect: Expect, get: Get, memo: dict[Expect, int], limit: int) -> int:
    """Entries below expect (recursion depth is bounded by MAX_DEPTH, checked by decode_checked)."""
    if expect in memo:
        return memo[expect]
    total = 0
    if not expect.cid.is_raw and expect.types & DIRECTORY_TYPES:
        check_cid(expect)
        node = decode_checked(expect, get(expect))
        if isinstance(node, (DirectoryNode, IndexNode)):
            for child in children(expect, node):
                if isinstance(node, DirectoryNode):
                    total += 1
                total += _count(child, get, memo, limit)
                if total > limit:
                    raise TooLarge(f"more than max_output_entries ({limit}) entries")
    memo[expect] = total
    return total


class _Heights:
    """height()'s memo across calls: the height of a subtree that passed every check, by the expectation it was
    checked under (depth set to 1), so `dir set` and `dir rm` don't walk a whole tree again for each edit.

    Exact forever: a CID's content never changes, and the checks a subtree passes and its height depend only on
    its blocks and that expectation. Least recently used forgotten first, past HEIGHT_MEMO entries.
    """

    def __init__(self):
        """Empty."""
        self._lock = threading.Lock()
        self._heights: OrderedDict[Expect, int] = OrderedDict()

    def get(self, expect: Expect) -> int | None:
        """The height remembered for expect, if any."""
        with self._lock:
            known = self._heights.get(expect)
            if known is not None:
                self._heights.move_to_end(expect)
            return known

    def update(self, heights: dict[Expect, int]):
        """Remember heights, forgetting the least recently used past HEIGHT_MEMO."""
        with self._lock:
            for expect, known in heights.items():
                self._heights[expect] = known
                self._heights.move_to_end(expect)
            while len(self._heights) > HEIGHT_MEMO:
                self._heights.popitem(last=False)


_heights = _Heights()


def height(expect: Expect, get: Get) -> int:
    """DAG nodes on the longest path from expect down (0 for a raw block), checking every DAG node met.

    Depth isn't checked against MAX_DEPTH, since measuring it is the point; the walk stops once the
    height is known to exceed MAX_DEPTH, returning MAX_DEPTH + 1. A subtree measured (and checked) before under
    the same expectation isn't walked again (_Heights), so a tree edited in one place costs only the new nodes.
    """
    limit = constants.MAX_DEPTH
    memo: dict[Expect, int] = {}
    truncated = False  # a node was reached more than limit nodes down: memoised heights may be short, but it's too deep

    def visit(current: Expect, level: int) -> int:
        """Height below current, found level nodes down; memoised, and recursion bounded by limit."""
        if current.cid.is_raw:
            check_cid(current)
            return 0
        if level > limit:
            nonlocal truncated
            truncated = True
            return 1
        current = replace(current, depth=1)  # every node is checked as if at the top, so depth can't trip
        if current not in memo:
            known = _heights.get(current)
            if known is None:
                child_expects = expand(current, get(current))
                known = 1 + max((visit(child, level + 1) for child in child_expects), default=0)
            memo[current] = known
        return memo[current]

    result = visit(expect, 1)
    if truncated:
        return limit + 1  # nothing kept: heights found below a cut-off walk may be short
    _heights.update(memo)  # every one exact, the walk having reached the bottom of each
    # A remembered subtree can make the tree too deep without the walk going below limit
    return min(result, limit + 1)


# Writing out. A directory output must stay where it was made while it is written: anyone who could rename inside
# it could swap one of its directories for a link or junction between the directory's mkdir and its children's
# writes, and have the DAG's files (execute bit included) created where they chose (audit CONT-2). So where the
# platform allows (FD_WRITE: Linux, macOS) everything is created relative to its parent's descriptor, below a root
# that is private while written; on Windows the root gets a private DACL and every file's final path is checked.
# Either way, the caller checks the root is still the one made (check_unchanged) before renaming it.

_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
# Platforms without these write directory outputs by path, as Windows does
FD_WRITE = (os.open in os.supports_dir_fd and os.mkdir in os.supports_dir_fd and os.listdir in os.supports_fd
            and hasattr(os, "O_NOFOLLOW") and hasattr(os, "O_DIRECTORY") and hasattr(os, "fchmod"))
# What opening a directory with O_NOFOLLOW | O_DIRECTORY reports for a symlink (ELOOP; EMLINK on FreeBSD), a
# non-directory or a missing one
_SWAPPED_ERRNOS = frozenset({errno.ELOOP, errno.EMLINK, errno.ENOTDIR, errno.ENOENT})
CHANGED = "output was changed while being written"  # the ValueError's message when one was replaced

Identity = tuple[int, int]  # a file's (st_dev, st_ino)


def unsafe_name_reason(name: str) -> str | None:
    """Why the local filesystem can't take name as a plain file name (it would be read as a path), or None."""
    return fsutil.unsafe_name_reason(name)  # in fsutil so the CLI can check names without loading RNS


def iter_files(expect: Expect, get: Get) -> Iterator[Expect]:
    """The expectation of every file in a DAG (expect itself if it is a file), each distinct one once."""
    block = get(expect)
    if not isinstance(decode_checked(expect, block), (DirectoryNode, IndexNode)):
        yield expect
        return
    seen: set[Expect] = set()
    memo: dict[Expect, dag.Node] = {}
    stack = [expect]
    while stack:
        directory = stack.pop()
        for entry, child in _directory_entries(directory, get, memo):
            if child in seen:
                continue
            seen.add(child)
            if entry.kind == dag.KIND_DIRECTORY:
                stack.append(child)
            else:
                yield child


def write_out(expect: Expect, get: Get, dest: str, max_entries: int,
              check: Callable[[Expect], None] | None = None, created: list[Identity] | None = None) -> list[str]:
    """Write the file or directory expect names to dest, which must not exist yet. Returns warnings.

    check(file expect), if given, runs before each file is created and may raise to stop the write-out (the
    deny list's file hashes); what was written stays for the caller to remove. created, if given, gets a
    directory output's identity (st_dev, st_ino) once dest is made, for check_unchanged before dest is renamed.

    Entries whose names the local filesystem would read as a path, or can't represent (it refuses to create
    them), are skipped and reported, as are names that collide on this filesystem (e.g. case-insensitive ones).
    Kind 1 entries get the execute bit, except on Windows, which has none. Raises TooLarge after max_entries
    entries, counting every occurrence, or after max_entries small chunks (SmallLeaves); ValueError (CHANGED)
    if a directory of the output was replaced while it was written (_FdOutput, _PathOutput).
    """
    if os.path.lexists(dest):
        raise ValueError(f"{dest} already exists")
    warnings: list[str] = []
    memo: dict[Expect, dag.Node] = {}  # directory and index nodes, so repeated subtrees aren't reread
    count = 0
    small = SmallLeaves(max_entries)  # shared by every file written
    block = get(expect)
    node = decode_checked(expect, block)
    if not isinstance(node, (DirectoryNode, IndexNode)):
        # Straight into the directory the user chose: there is nothing below it to swap
        if check is not None:
            check(expect)
        with open(dest, "xb") as out:
            _write_file(expect, get, out, small, block)
        return warnings

    memo[expect] = node
    output = _FdOutput(dest) if FD_WRITE and not fsutil.IS_WINDOWS else _PathOutput(dest)
    try:
        if created is not None:
            created.append(output.identity)
        # (directory, its location in the output, path shown in warnings): names and identities rather than open
        # descriptors, so however many directories a DAG has, only a few descriptors are open at once
        stack: list[tuple[Expect, object, str]] = [(expect, output.root, "")]
        while stack:
            directory, location, shown = stack.pop()
            subdirectories = []
            parent = output.enter(location)
            try:
                for entry, child in _directory_entries(directory, get, memo):
                    count += 1
                    if count > max_entries:
                        raise TooLarge(f"output has more than max_output_entries ({max_entries}) entries")
                    entry_shown = f"{shown}/{entry.name}" if shown else entry.name
                    reason = unsafe_name_reason(entry.name)
                    if reason is not None:
                        warnings.append(f"skipped {entry_shown!r}: {reason}")
                        continue
                    if check is not None and entry.kind != dag.KIND_DIRECTORY:
                        check(child)
                    try:
                        if entry.kind == dag.KIND_DIRECTORY:
                            below = output.make_directory(parent, location, entry.name)
                        else:
                            out = output.create_file(parent, entry.name)
                    except FileExistsError:
                        # Everything here was created by this call, so this is another name the filesystem folded
                        warnings.append(f"skipped {entry_shown!r}: it collides with another name on this filesystem")
                        continue
                    except OSError as e:
                        if e.errno not in _NAME_ERRORS:
                            raise
                        # Allowed on most filesystems, but not this one (e.g. ':' or '?' on FAT, long names on
                        # eCryptfs)
                        warnings.append(f"skipped {entry_shown!r}: this filesystem can't represent it ({e.strerror})")
                        continue
                    if entry.kind == dag.KIND_DIRECTORY:
                        subdirectories.append((child, below, entry_shown))
                    else:
                        with out:
                            _write_file(child, get, out, small)
            finally:
                output.leave(parent)
            stack.extend(reversed(subdirectories))
        output.finish()
    finally:
        output.close()
    for warning in warnings:
        log(f"get: {warning}", LOG_WARNING)
    return warnings


def check_unchanged(path: str, identity: Identity):
    """Raise ValueError (CHANGED) unless path is still the directory write_out made there with identity: not
    moved away and replaced by a link or another directory (by anyone who can write beside it)."""
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        raise ValueError(f"{CHANGED}: {path} is gone") from None
    if fsutil.is_link(st) or (st.st_dev, st.st_ino) != identity:
        raise ValueError(f"{CHANGED}: {path} was replaced")


class _FdOutput:
    """A directory output written through descriptors (FD_WRITE): its root is held open, and private to this
    user (0700) while it is written.

    A location is the (name, identity) of each directory from the root down to it, the root's being (). Each
    directory is created with mkdir(dir_fd=) and reopened without following links to record its identity; to
    write its entries it is reopened from the root, one component at a time, each checked against its recorded
    identity, so a directory swapped for a link or for another directory is never written into.
    """

    def __init__(self, dest: str):
        """Create dest and open it, then make it private and check that nobody got in first.

        The mode is set through the descriptor, not by the umask (which is process-wide), and keeps setgid, so
        what is created below it still gets a shared directory's group. Raises ValueError (CHANGED) if dest was
        replaced, isn't this user's or already holds an entry, OSError if it can't be made.
        """
        os.mkdir(dest)
        self.fd = _open_directory(dest)
        try:
            st = os.fstat(self.fd)
            if st.st_uid != os.geteuid():
                raise ValueError(f"{CHANGED}: {dest} belongs to someone else")
            self.mode = stat.S_IMODE(st.st_mode)  # what mkdir gave it, given back by finish()
            os.fchmod(self.fd, 0o700 | (self.mode & stat.S_ISGID))
            # Until the chmod, anyone the parent's mode lets in could create an entry in it; from now on nobody
            # else can, so an empty root stays this write-out's alone
            planted = os.listdir(self.fd)
            if planted:
                raise ValueError(f"{CHANGED}: {dest} held {fsutil.printable(planted[0])!r} before anything was "
                                 f"written")
        except BaseException:
            os.close(self.fd)
            raise
        self.identity: Identity = (st.st_dev, st.st_ino)
        self.root: tuple = ()

    def enter(self, location: tuple) -> int:
        """A descriptor for the directory at location, reopened from the root; give it back with leave(). Raises
        ValueError (CHANGED) if a directory on the way isn't the one made there."""
        fd = self.fd
        try:
            for name, identity in location:
                below = _open_directory(name, fd)
                if fd != self.fd:
                    os.close(fd)
                fd = below
                st = os.fstat(fd)
                if (st.st_dev, st.st_ino) != identity:
                    raise ValueError(CHANGED)
        except BaseException:
            if fd != self.fd:
                os.close(fd)
            raise
        return fd

    def leave(self, fd: int):
        """Close a descriptor from enter() (the root's stays open)."""
        if fd != self.fd:
            os.close(fd)

    def make_directory(self, parent: int, location: tuple, name: str) -> tuple:
        """Create the directory name in parent (which is at location); its own location. Raises OSError
        (FileExistsError if name is taken), ValueError (CHANGED) if it was replaced at once."""
        os.mkdir(name, dir_fd=parent)
        fd = _open_directory(name, parent)
        try:
            st = os.fstat(fd)
        finally:
            os.close(fd)
        return (*location, (name, (st.st_dev, st.st_ino)))

    def create_file(self, parent: int, name: str) -> BinaryIO:
        """Create the file name in parent, open for writing: never through a link, nor over anything there.
        Raises OSError (FileExistsError if name is taken)."""
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | fsutil.O_NOFOLLOW | _O_CLOEXEC
        return os.fdopen(os.open(name, flags, 0o666, dir_fd=parent), "wb")

    def finish(self):
        """The write-out is complete: give the root back the mode mkdir gave it."""
        os.fchmod(self.fd, self.mode)

    def close(self):
        """Close the root's descriptor."""
        os.close(self.fd)


class _PathOutput:
    """A directory output written by path: on Windows, which can't open a directory as a descriptor, and where
    the platform lacks FD_WRITE. A location is a directory's path.

    On Windows the root is created with a protected DACL for this user and SYSTEM only (winacl.make_private_dir),
    which everything below it inherits, so nobody else can rename inside it. In case a directory was swapped for
    a junction all the same, each file's final path, from the open file, must be inside the root before a byte
    is written. Such a file was then created (empty) where the junction led; it isn't removed, since removing it
    by path could be redirected too.
    """

    def __init__(self, dest: str):
        """Create dest (private, on Windows). Raises ValueError (CHANGED) if it was replaced at once or already
        holds an entry, OSError if it can't be made."""
        if fsutil.IS_WINDOWS:
            from rnfs import winacl
            if not winacl.make_private_dir(dest):
                raise FileExistsError(errno.EEXIST, "it already exists", dest)
        else:
            os.mkdir(dest)
        st = os.lstat(dest)
        if fsutil.is_link(st) or not stat.S_ISDIR(st.st_mode):
            raise ValueError(f"{CHANGED}: {dest} was replaced")
        if os.listdir(dest):
            raise ValueError(f"{CHANGED}: {dest} held an entry before anything was written")
        self.identity: Identity = (st.st_dev, st.st_ino)
        self.real_root = os.path.realpath(dest)
        self.root = dest

    def enter(self, location: str) -> str:
        """The directory's path: nothing to open."""
        return location

    def leave(self, path: str):
        """Nothing to close."""

    def make_directory(self, parent: str, location: str, name: str) -> str:
        """Create the directory name in parent; its path. Raises OSError (FileExistsError if name is taken)."""
        path = os.path.join(parent, name)
        os.mkdir(path)
        return path

    def create_file(self, parent: str, name: str) -> BinaryIO:
        """Create the file name in parent, open for writing, and check it is inside the root. Raises OSError
        (FileExistsError if name is taken), ValueError (CHANGED) if it isn't inside the root."""
        out = open(os.path.join(parent, name), "xb")
        try:
            final = fsutil.final_path(out.fileno())
            if final is not None and not fsutil.is_below(final, self.real_root):
                raise ValueError(f"{CHANGED}: a directory in it was replaced by a link")
        except BaseException:
            out.close()
            raise
        return out

    def finish(self):
        """Nothing to give back: on Windows, the caller makes the output inherit its folder's access list."""

    def close(self):
        """Nothing to close."""


def _open_directory(name: str, dir_fd: int | None = None) -> int:
    """Open the directory name (in dir_fd) without following a link. Raises ValueError (CHANGED) if it isn't one
    (a link, a file, or gone), OSError for any other failure."""
    try:
        return os.open(name, os.O_RDONLY | _O_DIRECTORY | fsutil.O_NOFOLLOW | _O_CLOEXEC, dir_fd=dir_fd)
    except OSError as e:
        if e.errno in _SWAPPED_ERRNOS:
            raise ValueError(CHANGED) from None
        raise


def _write_file(expect: Expect, get: Get, out, small: SmallLeaves, block: bytes | None = None):
    """Write the file expect names to out, a file just created, with the execute bit for kind 1."""
    if block is not None and expect.cid.is_raw:
        check_raw(expect, len(block))
        small.meet(len(block))
        out.write(block)
    else:
        for data in read_range(expect, get, small=small):
            out.write(data)
    # Windows has no execute bit (os.chmod there only sets or clears read-only), so kind 1 writes a plain file
    if expect.kind == dag.KIND_EXECUTABLE and not fsutil.IS_WINDOWS:
        # Through the descriptor, so a path swapped for a symlink meanwhile can't redirect the chmod
        mode = os.fstat(out.fileno()).st_mode
        # Execute wherever read is allowed, as the umask already decided read permissions
        os.fchmod(out.fileno(), stat.S_IMODE(mode) | ((mode & 0o444) >> 2))
