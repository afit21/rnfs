"""Directories as tar streams, for a client whose files the daemon can't open (a sandboxed app, such as a Flatpak's):
POST /api/add/stream?format=tar builds a directory from an uploaded tar (build_tar), GET /api/get/stream?format=tar
sends a target back as one (plan, length, chunks). No RNS beyond rnfs.log's, through walker.

Reading treats the stream as untrusted input, read once, in order, never held whole (tarfile's stream mode):
- Regular files and directories become the tree, under the name rules of `add` (builder._build_tree): a member
  whose name is absolute, has a `..` component, or has a part that isn't a valid entry name (not UTF-8, too
  long) is skipped with a warning, as are symlinks, hard links, devices and FIFOs. Directories a path implies
  are made without a member of their own; a later file replaces an earlier one of the same path (tar's own
  rule, which `tar -r` relies on), and a member whose path a file or directory already holds as the other type
  is skipped with a warning. A file is executable if its mode has the owner's execute bit, on every platform: a
  tar's mode is its sender's, not a guess of Windows'.
- Bounds: at most max_entries files and directories (TooLarge), nesting at most MAX_DEPTH (TooLarge, as `add`),
  and warnings kept up to a limit and counted beyond it (Warnings), each naming at most MAX_SHOWN characters of
  the member's name, so the memory a stream can take is bounded by those and not by its length. tarfile reads a
  pax or GNU long-name header whole, keeps global pax headers' keys for the rest of the stream (copying and
  applying them for every member), and reads a sparse file's map as it goes, so a header over MAX_EXTENSION
  bytes, more than MAX_EXTENSIONS of them in a row, global headers of more than MAX_GLOBAL_BYTES in all, and
  sparse files are refused (_header_class).
- A damaged upload is never taken for a shorter one: a header that isn't valid anywhere in the stream is an error
  (not tarfile's quiet end of the archive), and so is anything but zeros after the tar's end (a zeroed header
  block in the middle would otherwise end it there).
- Plain tar only (no compression): the stream comes from a program on this machine, where compressing gains
  nothing.

Writing (for a target from anyone's DAG): PAX format, written by hand so the exact length is known before the first
byte (an exact Content-Length, so a client can tell a download that was cut short). Entry names are DAG names,
which hold no `/` or NUL and are never `.` or `..`, so a member's path is relative and stays inside the directory
it is extracted to; a name holding a character some extractors take for a separator (UNSAFE_CHARACTERS: a
backslash, and the fullwidth reverse solidus and solidus that "best fit" code pages map to them) is skipped, with everything below it, and
counted in the skipped Warnings. Every member is planned (paths kept as parent links, joined only when written) and
the small chunks its files would meet counted before the length is given, so writing can't fail on a limit after
a response has started. Files are their stored bytes: a sealed file stays sealed, as in a directory's `get` (GET
/api/cat decrypts one). Directories are mode 0755, files 0644 (0755 if executable), every member dated mtime (the
request's time), owned by uid and gid 0 with no names.
"""
import tarfile
from typing import BinaryIO, Callable, Iterator, NamedTuple

from rnfs import constants, log
from rnfs.content import builder, dag, walker
from rnfs.content.builder import BuildOptions, Built, Put
from rnfs.content.dag import Entry
from rnfs.errors import InvalidDag, TooLarge

BLOCK = tarfile.BLOCKSIZE  # 512: headers, and file data padded to a multiple of it
RECORD = tarfile.RECORDSIZE  # 10240: a tar ends with two zero blocks, padded to a multiple of it, as tarfile pads
# Bytes of one pax or GNU long-name header read at most: a valid path is at most MAX_DEPTH names of 255 bytes
MAX_EXTENSION = 64 * 1024
MAX_EXTENSIONS = 8  # extension headers in a row (each read whole, tarfile following them recursively)
# Bytes of global pax headers in a stream, all told: tarfile applies their keys to every later member (git archive
# writes one of 52 bytes)
MAX_GLOBAL_BYTES = 4096
MAX_SHOWN = 300  # characters of a member's name a warning shows (one may be 64 KiB, escaped six times longer)
# Characters some extractors read as a path separator or a drive (Windows'), never written in a tar's member names
# Not ":": ordinary POSIX names hold it (12:30.log), and to an extractor it is no separator, only a character a
# Windows file name can't have (which that extractor refuses or replaces); a name starting as a drive does is
# refused apart (safe_name)
UNSAFE_CHARACTERS = frozenset("\\\uff3c\uff0f")
COPY_SIZE = 64 * 1024  # what is read at a time of what follows a tar's end
_EXTENSION_TYPES = frozenset({tarfile.XHDTYPE, tarfile.XGLTYPE, tarfile.SOLARIS_XHDTYPE, tarfile.GNUTYPE_LONGNAME,
                              tarfile.GNUTYPE_LONGLINK})
# tarfile's readers of sparse maps, which the header class refuses
SPARSE_READERS = ("_proc_sparse", "_proc_gnusparse_00", "_proc_gnusparse_01", "_proc_gnusparse_10")
# The TarInfo internals the header class overrides (_proc_member: where every header, however parsed, is
# processed), the same in Python 3.11 to 3.14: the tests check each exists, so a Python that renames one is noticed
# rather than read unbounded
TARFILE_INTERNALS = ("_proc_member", *SPARSE_READERS)


class Warnings:
    """What a build skipped: the first limit warnings kept, the rest counted (a tar of a million symlinks mustn't
    fill memory). Called with each warning."""

    def __init__(self, limit: int = log.MAX_WARNINGS):
        """No warnings yet; keep at most limit."""
        self.limit = limit
        self.kept: list[str] = []
        self.total = 0

    def __call__(self, text: str):
        """Count a warning, keeping it while fewer than limit are kept."""
        self.note(lambda: text)

    def note(self, make: Callable[[], str]):
        """Count a warning, making its text (make()) only if it is kept: past the limit it costs nothing."""
        self.total += 1
        if len(self.kept) < self.limit:
            self.kept.append(make())

    def result(self) -> dict:
        """{"warnings": the kept ones} and, past the limit, a last one counting the rest and "warnings_total", as
        log.cap_warnings gives them."""
        if self.total <= len(self.kept):
            return {"warnings": list(self.kept)}
        rest = self.total - len(self.kept)
        return {"warnings": [*self.kept, f"... and {log.plural(rest, 'more warning')}"], "warnings_total": self.total}


def _refused(reason: str) -> tarfile.SubsequentHeaderError:
    """The error a header check raises: one TarFile.next() turns into ReadError, wherever in the stream it is."""
    return tarfile.SubsequentHeaderError(reason)


def _header_class() -> type:
    """A TarInfo subclass of its own for one stream, holding that stream's header counts, refusing what tarfile would
    read without bound (the module docstring) and every malformed header."""

    class Header(tarfile.TarInfo):
        """A header of the stream being read."""

        extensions = 0  # extension headers since the last member's
        global_bytes = 0  # bytes of global pax headers so far

        @classmethod
        def fromtarfile(cls, tarfile_):
            """The next member; a malformed header anywhere is an error (tarfile ends the archive quietly at one
            past the first member, which would take a damaged upload for a shorter one)."""
            try:
                return super().fromtarfile(tarfile_)
            except (tarfile.InvalidHeaderError, tarfile.TruncatedHeaderError) as e:
                raise _refused(f"malformed tar header ({e})") from None

        def _proc_member(self, tarfile_):
            """Check a header just parsed against the limits, before tarfile reads what it announces (tarfile calls
            this for every header block, extension headers' too, however it parsed it)."""
            cls = type(self)
            if self.type == tarfile.GNUTYPE_SPARSE:
                raise _refused("sparse files aren't supported")
            if self.type in _EXTENSION_TYPES:
                cls.extensions += 1
                if self.size > MAX_EXTENSION:
                    raise _refused(f"a tar extended header of {self.size} bytes (at most {MAX_EXTENSION})")
                if cls.extensions > MAX_EXTENSIONS:
                    raise _refused(f"more than {MAX_EXTENSIONS} tar extended headers in a row")
                if self.type == tarfile.XGLTYPE:
                    cls.global_bytes += self.size
                    if cls.global_bytes > MAX_GLOBAL_BYTES:
                        raise _refused(f"global pax headers of more than {MAX_GLOBAL_BYTES} bytes")
            else:
                cls.extensions = 0
            return super()._proc_member(tarfile_)

    def sparse(self, *args):
        """Refuse a sparse file: its map would be read without bound."""
        raise _refused("sparse files aren't supported")

    for name in SPARSE_READERS:
        setattr(Header, name, sparse)
    return Header


def _shown(text: str) -> str:
    """text for a warning or log line, cut to MAX_SHOWN characters (a member's name may be 64 KiB)."""
    return text if len(text) <= MAX_SHOWN else text[:MAX_SHOWN] + "..."


def _parts(name: str) -> list[str] | str:
    """A member name's path parts ("." and empty ones dropped: `./a//b` is a/b), or why it is skipped."""
    if name.startswith("/"):
        return "an absolute path"
    parts = [part for part in name.split("/") if part not in ("", ".")]
    if ".." in parts:
        return "a path with a .. component"
    if not all(dag.is_valid_name(part) for part in parts):
        return "name is not valid UTF-8 or breaks the name rules"
    return parts


class _Tree:
    """The directories a tar builds, as nested dicts of name to a dict (a directory) or a Built (a file)."""

    def __init__(self, max_entries: int):
        """An empty root; at most max_entries files and directories below it."""
        self.root: dict = {}
        self.entries = 0
        self.max_entries = max_entries

    def _count(self):
        """One more entry. Raises TooLarge past max_entries."""
        self.entries += 1
        if self.entries > self.max_entries:
            raise TooLarge(f"the tar has more than max_output_entries ({self.max_entries}) files and directories")

    def directory(self, parts: list[str]) -> dict | None:
        """The directory at parts, made with those above it as needed; None if a file holds one of the names."""
        current = self.root
        for part in parts:
            child = current.get(part)
            if child is None:
                self._count()
                child = current[part] = {}
            elif not isinstance(child, dict):
                return None
            current = child
        return current

    def file_parent(self, parts: list[str]) -> dict | str:
        """The directory a file at parts goes in (made with those above it as needed, and the file counted if
        new: a file already there is replaced), or why it can't go there."""
        parent = self.directory(parts[:-1])
        if parent is None:
            return "a file holds the name of one of its directories"
        existing = parent.get(parts[-1])
        if isinstance(existing, dict):
            return "a directory of that name came first"
        if existing is None:
            self._count()
        return parent

    def build(self, put: Put) -> Built:
        """Build every directory, deepest first (recursion bounded by MAX_DEPTH, checked as members came)."""

        def build(directory: dict) -> Built:
            """One directory, after those below it."""
            entries, child_height = [], 0
            for name, child in directory.items():
                built = build(child) if isinstance(child, dict) else child
                entries.append(Entry(name, built.cid, built.size, built.kind))
                child_height = max(child_height, built.height)
            return builder.build_directory(entries, put, child_height)

        return build(self.root)


def build_tar(stream: BinaryIO, put: Put, warn: Callable[[str], None], max_entries: int,
              options: BuildOptions | None = None, wrap: str | None = None) -> Built:
    """Build the directory a tar stream holds (the module docstring's rules), wrapped as the entry wrap if given;
    warn(text) is told what was skipped. options as for builder.build_file (the chunker, the deny list's check).

    Reads stream to its end, which after the tar's own may hold only zeros (its padding). Raises ValueError for a
    stream that isn't a valid plain tar, ends early or has data after its end, TooLarge past max_entries or
    MAX_DEPTH, and what options.check raises.
    """
    if wrap is not None:
        dag.check_name(wrap)  # before reading a possibly large stream
    tree = _Tree(max_entries)
    try:
        archive = tarfile.open(fileobj=stream, mode="r|", tarinfo=_header_class(), encoding="utf-8",
                               errors="surrogateescape")
        with archive:
            while (member := archive.next()) is not None:
                # Stream mode keeps every member read, for a later getmembers(); nothing here needs them
                archive.members.clear()
                _add_member(archive, member, tree, put, warn, options)
            # tarfile stops at the first zero block, which damage in the middle of a tar can make: through the
            # stream it read ahead in, the rest must be the end blocks and padding
            while data := archive.fileobj.read(COPY_SIZE):
                if data.count(0) != len(data):
                    raise ValueError("not a valid tar stream: data after its end (a damaged header?)")
    except tarfile.TarError as e:
        raise ValueError(f"not a valid tar stream: {e}") from None
    built = tree.build(put)
    return builder.wrap(wrap, built, put) if wrap is not None else built


def _add_member(archive: tarfile.TarFile, member: tarfile.TarInfo, tree: _Tree, put: Put,
                warn: Callable[[str], None], options: BuildOptions | None):
    """Put one member into tree (or warn why it is skipped); a file's data is built as it is read."""
    shown = _shown(builder.display_path(member.name))
    parts = _parts(member.name)
    if isinstance(parts, str):
        warn(f"skipped {shown}: {parts}")
        return
    if not parts:  # "." or "./": the root itself
        if not member.isdir():
            warn(f"skipped {shown}: not a valid entry name")
        return
    if len(parts) > constants.MAX_DEPTH:
        raise TooLarge(f"{shown} is nested deeper than MAX_DEPTH ({constants.MAX_DEPTH})")
    if member.issym():
        warn(f"skipped symlink {shown}")
    elif member.islnk():
        warn(f"skipped hard link {shown}")
    elif member.isdir():
        if tree.directory(parts) is None:
            warn(f"skipped {shown}: a file holds its name or the name of one of its directories")
    elif not member.isreg():
        warn(f"skipped {shown}: not a regular file or directory")
    else:
        parent = tree.file_parent(parts)  # checked before its data is read: a skipped file's is passed over
        if isinstance(parent, str):
            warn(f"skipped {shown}: {parent}")
            return
        kind = dag.KIND_EXECUTABLE if member.mode & 0o100 else dag.KIND_FILE
        parent[parts[-1]] = builder.build_file(archive.extractfile(member), put, member.size, kind, options)


# Writing


class Member(NamedTuple):
    """One member of a tar being written: the directory member it is in (None at the top), its name, kind
    (dag.KIND_*), size (0 for a directory) and expectation. Its path is joined only when its header is written, so a
    plan of many deep members holds each name once (a DAG's own, shared by its repeated subtrees)."""

    parent: "Member | None"
    name: str
    kind: int
    size: int
    expect: walker.Expect

    @property
    def path(self) -> str:
        """The member's path in the tar (its parents' names, then its own, joined by /)."""
        names = []
        member: Member | None = self
        while member is not None:
            names.append(member.name)
            member = member.parent
        return "/".join(reversed(names))


def safe_name(name: str) -> bool:
    """True if name is safe as a part of a member's path: none of UNSAFE_CHARACTERS, and not starting as a Windows
    drive does (`C:`, which as a member's first part would make a drive-relative path there; the rest is the DAG's
    name rules)."""
    drive = len(name) >= 2 and name[1] == ":" and name[0].isascii() and name[0].isalpha()
    return not drive and UNSAFE_CHARACTERS.isdisjoint(name)


_SMALL = constants.CHUNK_SIZE // 16  # a piece shorter than this counts against max_entries (walker.SmallLeaves)


def _small_chunks(expect: walker.Expect, size: int, get: walker.Get, memo: dict, limit: int) -> int:
    """How many small pieces reading the whole file expect names (of size bytes) meets, as walker.SmallLeaves counts
    them, every occurrence counted; file nodes are read (each distinct one once: memo), chunks never. Stops counting
    past limit."""
    if expect.cid.is_raw:
        return 1 if 0 < size < _SMALL else 0
    if expect in memo:
        return memo[expect]
    walker.check_cid(expect)
    node = walker.decode_checked(expect, get(expect))
    total = 0
    for child in walker.children(expect, node):
        total += _small_chunks(child, child.size, get, memo, limit)
        if total > limit:
            break
    memo[expect] = total
    return total


def plan(expect: walker.Expect, get: walker.Get, name: str, max_entries: int, skipped: Warnings) -> list[Member]:
    """The members of the tar of the file or directory expect names (its blocks all stored): a file is one member
    called name; a directory's entries are at the top (its contents, as `tar -C dir .` makes them), each directory
    before what it holds. Names unsafe in a tar are skipped with what is below them, counted in skipped (their
    paths kept up to its limit). Raises TooLarge past max_entries entries, counting every occurrence, or past
    max_entries small chunks in all (walker.SmallLeaves, which chunks() applies): before anything is sent."""
    small_left = max_entries + 2  # as SmallLeaves allows
    memo_small: dict = {}

    def file_member(parent: Member | None, member_name: str, kind: int, child: walker.Expect) -> Member:
        """A file's member, its small chunks counted."""
        nonlocal small_left
        size = walker.size_of(child, get(child))
        small_left -= _small_chunks(child, size, get, memo_small, small_left)
        if small_left < 0:
            raise TooLarge("a file has more small chunks than max_output_entries allows")
        return Member(parent, member_name, kind, size, child)

    block = get(expect)
    if walker.kind_of(expect, block) == walker.FILE_TYPE:
        kind = dag.KIND_EXECUTABLE if expect.kind == dag.KIND_EXECUTABLE else dag.KIND_FILE
        return [file_member(None, name, kind, expect)]
    members: list[Member] = []
    memo: dict = {}
    stack: list[tuple[walker.Expect, Member | None]] = [(expect, None)]
    while stack:
        directory, parent = stack.pop()
        below = []
        for entry, child in walker.directory_entries(directory, get, memo):
            if not safe_name(entry.name):
                skipped.note(lambda entry=entry, parent=parent: _shown(Member(parent, entry.name, 0, 0, child).path))
                continue
            if len(members) >= max_entries:
                raise TooLarge(f"more than max_output_entries ({max_entries}) entries")
            if entry.kind == dag.KIND_DIRECTORY:
                member = Member(parent, entry.name, entry.kind, 0, child)
                below.append((child, member))
            else:
                member = file_member(parent, entry.name, entry.kind, child)
            members.append(member)
        stack.extend(reversed(below))
    return members


def _header(member: Member, mtime: int) -> bytes:
    """A member's header blocks (a pax header first where its path or size needs one)."""
    info = tarfile.TarInfo(member.path)
    info.mtime = mtime
    if member.kind == dag.KIND_DIRECTORY:
        info.type = tarfile.DIRTYPE
        info.mode = 0o755
    else:
        info.size = member.size
        info.mode = 0o755 if member.kind == dag.KIND_EXECUTABLE else 0o644
    return info.tobuf(tarfile.PAX_FORMAT, "utf-8", "strict")


def _padding(size: int, unit: int) -> int:
    """The zero bytes after size bytes up to a multiple of unit."""
    return -size % unit


def length(members: list[Member], mtime: int) -> int:
    """The exact bytes chunks() writes for members."""
    total = sum(len(_header(member, mtime)) + member.size + _padding(member.size, BLOCK) for member in members)
    total += 2 * BLOCK
    return total + _padding(total, RECORD)


def chunks(members: list[Member], get: walker.Get, mtime: int, max_entries: int) -> Iterator[bytes]:
    """The tar of members, as length() counts it. Raises InvalidDag if a file's bytes aren't its size (its
    blocks were checked, so only a bug could), TooLarge past max_entries small chunks (walker.SmallLeaves, as a
    write-out; plan has counted them, so only a bug could either)."""
    small = walker.SmallLeaves(max_entries)  # shared by every file, as write_out's
    written = 0
    for member in members:
        header = _header(member, mtime)
        yield header
        written += len(header)
        if member.kind == dag.KIND_DIRECTORY:
            continue
        sent = 0
        for data in walker.read_range(member.expect, get, small=small):
            sent += len(data)
            yield data
        if sent != member.size:
            raise InvalidDag(f"{member.expect.cid} held {sent} bytes, not its size {member.size}")
        written += sent
        if _padding(sent, BLOCK):
            yield bytes(_padding(sent, BLOCK))
            written += _padding(sent, BLOCK)
    end = 2 * BLOCK + _padding(written + 2 * BLOCK, RECORD)
    yield bytes(end)
