"""dir set / dir rm: new directories with an entry added, replaced or removed (specification section 11).
No RNS dependency.

DAGs are immutable, so an edit at <root>/<path> rebuilds the directory there and every directory above it
along the path, with the builder, so the result has the same CID as adding the edited tree from scratch.
The caller makes sure the directories involved (and, per the spec, the complete directory) are stored.
"""
from rnfs import constants
from rnfs.content import builder, dag, walker
from rnfs.content.builder import Put
from rnfs.content.cid import Cid
from rnfs.content.dag import Entry
from rnfs.errors import NotFound, TooLarge


def _directory_chain(root: Cid, names: list[str], get: walker.Get) -> list[walker.Expect]:
    """Expectations for the root and each directory along names, in order (len(names) + 1 of them)."""
    chain = [walker.root_expect(root)]
    for i, name in enumerate(names):
        expect = walker.step_name(chain[-1], name, get)
        if expect is None:
            raise NotFound(f"{root}/{'/'.join(names[:i + 1])} does not exist")
        if expect.kind != dag.KIND_DIRECTORY:
            raise ValueError(f"{root}/{'/'.join(names[:i + 1])} is not a directory")
        chain.append(expect)
    return chain


def _rebuild(chain: list[walker.Expect], names: list[str], entries: list[Entry], get: walker.Get,
             put: Put) -> Cid:
    """Build the directory at the end of chain from entries, then each directory above it along names."""
    built = builder.build_directory(entries, put)
    for parent, name in zip(reversed(chain[:-1]), reversed(names)):
        siblings = [entry for entry in walker.list_directory(parent, get) if entry.name != name]
        siblings.append(Entry(name, built.cid, built.size, dag.KIND_DIRECTORY))
        built = builder.build_directory(siblings, put)
    # The builder can't see the depth of subtrees it didn't build, so check the finished tree: walker.height
    # remembers the subtrees it measured before, so once a tree has been measured an edit of it reads only the
    # directories it rebuilt (rather than every node: building a directory one entry at a time was O(n²))
    if walker.height(walker.root_expect(built.cid), get) > constants.MAX_DEPTH:
        raise TooLarge(f"the edited tree would be deeper than MAX_DEPTH ({constants.MAX_DEPTH})")
    return built.cid


def dir_set(root: Cid, names: list[str], name: str, child: Cid, executable: bool, get: walker.Get,
            put: Put) -> Cid:
    """The new root after setting entry name in the directory <root>/<names...> to child.

    The child's kind comes from its root block (stored, via get): a directory, or a file (kind 1 if
    executable). Raises ValueError for an invalid name, a non-directory target or an executable directory,
    NotFound for a missing path, InvalidDag for an invalid child root.
    """
    dag.check_name(name)
    child_expect = walker.root_expect(child)
    child_block = get(child_expect)
    if walker.kind_of(child_expect, child_block) == walker.DIRECTORY_TYPE:
        if executable:
            raise ValueError("a directory can't be executable")
        kind = dag.KIND_DIRECTORY
    else:
        kind = dag.KIND_EXECUTABLE if executable else dag.KIND_FILE
    entry = Entry(name, child, walker.size_of(child_expect, child_block), kind)
    chain = _directory_chain(root, names, get)
    entries = [e for e in walker.list_directory(chain[-1], get) if e.name != name]
    entries.append(entry)
    return _rebuild(chain, names, entries, get, put)


NEEDS_ENTRY = "dir rm needs <cid>/<path>: the entry to remove"


def check_entry_target(target) -> None:
    """Raise ValueError unless target names an entry inside a directory, `<cid>/<path>` (dir rm's target: a root
    can't be removed). The node checks this before fetching anything; the path itself is checked as it is
    parsed."""
    if not isinstance(target, str) or "/" not in target.rstrip("/"):
        raise ValueError(NEEDS_ENTRY)


def dir_rm(root: Cid, names: list[str], get: walker.Get, put: Put) -> Cid:
    """The new root after removing the entry <root>/<names...>. Raises NotFound if it doesn't exist."""
    if not names:
        raise ValueError(NEEDS_ENTRY)
    chain = _directory_chain(root, names[:-1], get)
    entries = list(walker.list_directory(chain[-1], get))
    remaining = [e for e in entries if e.name != names[-1]]
    if len(remaining) == len(entries):
        raise NotFound(f"{root}/{'/'.join(names)} does not exist")
    return _rebuild(chain, names[:-1], remaining, get, put)
