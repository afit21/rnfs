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
import os
import stat
from dataclasses import dataclass, replace
from typing import Callable, Iterator

from rnfs import constants
from rnfs.content import dag
from rnfs.content.cid import Cid
from rnfs.content.dag import DirectoryNode, Entry, FileNode, IndexNode
from rnfs.errors import InvalidDag, NotFound, TooLarge
from rnfs.log import LOG_WARNING, log

RAW_TYPE, FILE_TYPE, DIRECTORY_TYPE, INDEX_TYPE = "raw", "file", "directory", "index"
ANY_TYPES = frozenset({RAW_TYPE, FILE_TYPE, DIRECTORY_TYPE, INDEX_TYPE})
FILE_TYPES = frozenset({RAW_TYPE, FILE_TYPE})  # an entry of kind 0 or 1, or a file node's child
DIRECTORY_TYPES = frozenset({DIRECTORY_TYPE, INDEX_TYPE})  # an entry of kind 2, or an index's child


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
    if isinstance(node, DirectoryNode):
        names = [entry.name for entry in node.entries]
    elif isinstance(node, IndexNode):
        names = [child.first_name for child in node.children]
    else:
        names = None
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
    # The name is in the child with the greatest first_name <= name, if anywhere
    found = None
    for index_child, child in zip(node.children, child_expects):
        if index_child.first_name <= name:
            found = child
        else:
            break
    return found


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
        if child_end > offset:
            low, high = max(offset, start), min(end, child_end)
            result.append((child, low - start, high - low))
        start = child_end
    return result


def parse_target(text: str) -> tuple[Cid, list[str]]:
    """The root CID and path names of a target "<cid>[/name...]" (one trailing slash allowed). ValueError if malformed."""
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
    """"file" or "directory": what the checked block is the root of."""
    node = decode_checked(expect, block)
    return "directory" if isinstance(node, (DirectoryNode, IndexNode)) else "file"


def size_of(expect: Expect, block: bytes) -> int:
    """Content bytes below a checked block."""
    node = decode_checked(expect, block)
    return len(block) if node is None else node.size


# Local operations, over a blocking get(expect) -> bytes that raises NotFound when a block is absent


def resolve(root: Cid, names: list[str], get: Get) -> Expect:
    """The expectation for the target <root>/<names...>. Raises NotFound if a name doesn't exist."""
    expect = root_expect(root)
    for i, name in enumerate(names):
        while True:
            child = step(expect, get(expect), name)
            if child is None:
                raise NotFound(f"{root}/{'/'.join(names[:i + 1])} does not exist")
            expect = child
            if not is_index_step(expect):
                break
    return expect


def iter_dag(expect: Expect, get: Get) -> Iterator[tuple[Expect, bytes]]:
    """Every block of a DAG as (expect, block), depth-first, each CID once; every occurrence is checked."""
    seen_expects: set[Expect] = set()
    seen_cids: set[Cid] = set()
    nodes: dict[Cid, bytes] = {}  # DAG node blocks already read, to check later occurrences without rereading
    raw_lengths: dict[Cid, int] = {}  # raw blocks are only checked by length; don't keep their bytes
    stack = [expect]
    while stack:
        current = stack.pop()
        if current in seen_expects:
            continue
        seen_expects.add(current)
        cid = current.cid
        if cid in raw_lengths:
            check_raw(current, raw_lengths[cid])
            continue
        if cid in nodes:
            stack.extend(reversed(expand(current, nodes[cid])))
            continue
        check_cid(current)  # before reading a block that can't be right
        block = get(current)
        child_expects = expand(current, block)
        if cid.is_raw:
            raw_lengths[cid] = len(block)
        else:
            nodes[cid] = block
        if cid not in seen_cids:
            seen_cids.add(cid)
            yield current, block
        stack.extend(reversed(child_expects))


def read_range(expect: Expect, get: Get, offset: int = 0, length: int | None = None) -> Iterator[bytes]:
    """The bytes of a file in [offset, offset + length) (to the end if length is None), in order.

    Raises ValueError if the target is a directory.
    """
    if offset < 0 or (length is not None and length < 0):
        raise ValueError("offset and length must be non-negative")
    block = get(expect)
    node = decode_checked(expect, block)
    if node is None:
        end = len(block) if length is None else min(len(block), offset + length)
        if offset < end:
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
            yield current_block[child_offset:child_offset + child_length]
            continue
        parts = range_children(current, current_block, child_offset, child_length)
        stack.extend((child, o, n, None) for child, o, n in reversed(parts))


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


def height(expect: Expect, get: Get, limit: int | None = None) -> int:
    """DAG nodes on the longest path from expect down (0 for a raw block), checking every DAG node met.

    Depth isn't checked against MAX_DEPTH, since measuring it is the point; the walk stops once the
    height is known to exceed limit (default MAX_DEPTH), returning limit + 1.
    """
    limit = constants.MAX_DEPTH if limit is None else limit
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
            child_expects = expand(current, get(current))
            memo[current] = 1 + max((visit(child, level + 1) for child in child_expects), default=0)
        return memo[current]

    result = visit(expect, 1)
    return limit + 1 if truncated else result


# Writing out


_WINDOWS_RESERVED = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)), *(f"LPT{i}" for i in range(1, 10))}


def unsafe_name_reason(name: str) -> str | None:
    """Why the local filesystem can't take name as a plain file name (it would be read as a path), or None."""
    if name in (".", "..") or "/" in name or "\0" in name:
        return "it would be interpreted as a path"
    if os.sep in name or (os.altsep and os.altsep in name):
        return "it contains a path separator"
    if os.name == "nt":
        if ":" in name:
            return "it contains ':' (a drive or stream on Windows)"
        if any(c in '<>"|?*' or ord(c) < 32 for c in name) or name[-1] in ". ":
            return "Windows can't represent it"
        if name.split(".")[0].upper() in _WINDOWS_RESERVED:
            return "it is a reserved device name on Windows"
    try:
        os.fsencode(name)
    except UnicodeEncodeError:
        return "the filesystem encoding can't represent it"
    return None


def write_out(expect: Expect, get: Get, dest: str, max_entries: int) -> list[str]:
    """Write the file or directory expect names to dest, which must not exist yet. Returns warnings.

    Entries whose names the local filesystem would read as a path, or can't represent, are skipped and
    reported, as are names that collide on this filesystem (e.g. case-insensitive ones). Kind 1 entries
    get the execute bit. Raises TooLarge after max_entries entries, counting every occurrence.
    """
    if os.path.lexists(dest):
        raise ValueError(f"{dest} already exists")
    warnings: list[str] = []
    memo: dict[Expect, dag.Node] = {}  # directory and index nodes, so repeated subtrees aren't reread
    count = 0
    block = get(expect)
    node = decode_checked(expect, block)
    if not isinstance(node, (DirectoryNode, IndexNode)):
        _write_file(expect, get, dest, block)
        return warnings

    memo[expect] = node
    os.mkdir(dest)
    stack: list[tuple[Expect, str, str]] = [(expect, dest, "")]  # (directory, where, path shown in warnings)
    while stack:
        directory, where, shown = stack.pop()
        subdirectories = []
        for entry, child in _directory_entries(directory, get, memo):
            count += 1
            if count > max_entries:
                raise TooLarge(f"output has more than max_output_entries ({max_entries}) entries")
            entry_shown = f"{shown}/{entry.name}" if shown else entry.name
            reason = unsafe_name_reason(entry.name)
            if reason is not None:
                warnings.append(f"skipped {entry_shown!r}: {reason}")
                continue
            path = os.path.join(where, entry.name)
            if os.path.lexists(path):
                # Everything here was created by this call, so this is another name the filesystem folded
                warnings.append(f"skipped {entry_shown!r}: it collides with another name on this filesystem")
                continue
            if entry.kind == dag.KIND_DIRECTORY:
                os.mkdir(path)
                subdirectories.append((child, path, entry_shown))
            else:
                _write_file(child, get, path)
        stack.extend(reversed(subdirectories))
    for warning in warnings:
        log(f"get: {warning}", LOG_WARNING)
    return warnings


def _write_file(expect: Expect, get: Get, path: str, block: bytes | None = None):
    """Write the file expect names to path (exclusively created), with the execute bit for kind 1."""
    with open(path, "xb") as out:
        if block is not None and expect.cid.is_raw:
            check_raw(expect, len(block))
            out.write(block)
        else:
            for data in read_range(expect, get):
                out.write(data)
    if expect.kind == dag.KIND_EXECUTABLE:
        mode = os.stat(path).st_mode
        # Execute wherever read is allowed, as the umask already decided read permissions
        os.chmod(path, stat.S_IMODE(mode) | ((mode & 0o444) >> 2))
