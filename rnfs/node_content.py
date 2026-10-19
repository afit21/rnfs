"""The node's side of the "content" features (rnfs/node.py calls these; logic kept out of the shared file):

- `add --no-pin`: what was added is kept as a cached fetch instead of a "user" pin (keep_added).
- Sealed files (rnfs/content/seal.py): resolving recipients to public keys, sealing a file being added,
  and opening a sealed target with the node's identity for get, cat and info. Sealed folders: a folder being
  added is sealed as one tar named `<folder>.tar`, marked as a sealed folder (add_sealed_path), and get unpacks a
  sealed `.tar` only with that mark (is_sealed_folder, get_sealed, unpack_sealed).
- Gateway formats: a CID's own block (?format=raw) and a target's archive streamed with its exact length
  (?format=archive, and GET /api/export/stream); a target as a tar streamed the same way (GET /api/get/stream).

Functions take the RnfsNode first. RNS is imported inside functions: the daemon has loaded it already.
"""
import itertools
import os
import stat
import time
from collections.abc import Callable, Iterator
from typing import BinaryIO

from rnfs.content import archive, builder, dag, seal, walker
from rnfs.content.cid import Cid
from rnfs.content.storage import USER
from rnfs.errors import Denied, InvalidDag, NotFound


def keep_added(node, cid: Cid, pin: bool):
    """Keep what was just added (inside the add's protection session): a "user" pin, or for add --no-pin a
    cached fetch of it, evicted LRU like any other and, under the roots strategy, provided while cached."""
    if pin:
        node.storage.pin(cid, USER)
        return
    dropped = node.storage.record_fetch(cid, [], cid)
    node._note_fetched(cid)
    if dropped:
        node._prune_fetched()


def contact_hash(node, text: str) -> bytes | None:
    """The destination hash a node or LXMF contact's nick stands for (rnfs/features/contacts.py), or None.

    Name contacts don't count: a name's key belongs to the name, not to a person who could read the file.
    """
    feature = getattr(node, "features", {}).get("contacts")
    lookup = getattr(feature, "lookup", None)
    if lookup is None:
        return None
    return lookup(text, "node") or lookup(text, "lxmf")


def seal_recipients(node, to: list[str] | None, to_self: bool, only_hash: bool) -> list[bytes]:
    """The public keys an add seals for (none: don't seal). Raises ValueError for unknown recipients."""
    if not to and not to_self:
        return []
    if only_hash:
        raise ValueError("--only-hash can't be used with --to: sealing is randomised, so the CID differs each time")
    identity = getattr(node, "identity", None)
    own = identity.get_public_key() if identity is not None else None
    if to_self and own is None:
        raise ValueError("this node has no identity to seal for")

    def recall(hash_: bytes) -> bytes | None:
        """The public key RNS knows for a destination or identity hash (this node's own included)."""
        import RNS

        if own is not None and hash_ in (node.node_id, seal.identity_hash(own)):
            return own
        for kwargs in ({}, {"from_identity_hash": True}):
            try:
                found = RNS.Identity.recall(hash_, **kwargs)
            except AttributeError:  # no Reticulum instance (offline test nodes): look without marking use
                found = RNS.Identity.recall(hash_, _no_use=True, **kwargs)
            if found is not None:
                return found.get_public_key()
        return None

    keys = [seal.parse_recipient(text, recall, lambda nick: contact_hash(node, nick)) for text in (to or [])]
    return keys + ([own] if to_self else [])


def sealing_stream(stream: BinaryIO, size: int | None, recipients: list[bytes],
                   seal_name: str | None) -> tuple[BinaryIO, int | None]:
    """(stream, size) to build from: the sealed form of stream when there are recipients (its size unknown)."""
    if not recipients:
        return stream, size
    return seal.SealingReader(stream, recipients, seal_name, size), None


TAR_SUFFIX = ".tar"  # a sealed folder's sealed name is the folder's name and this


def folder_sealed_name(name: str, seal_name: bool) -> str:
    """The sealed name of a folder called name: name + TAR_SUFFIX. Raises ValueError if the folder can't be sealed
    under it, rather than sealing it unnamed as a file with an invalid name is (the name is what makes a recipient's
    get unpack it): a name that isn't a valid entry name, one over 251 bytes (the sealed name would break the name
    rules), and seal_name False (--no-sealed-name)."""
    if not seal_name:
        raise ValueError(f"--no-sealed-name can't be used with a folder: its sealed name (the folder's name and "
                         f"{TAR_SUFFIX}) is what tells a recipient's get to unpack it; rename the folder to change it")
    if not dag.is_valid_name(name):
        raise ValueError(f"the folder's name {builder.display_path(name)!r} is not valid UTF-8 or breaks the name "
                         f"rules, so it can't be sealed under it")
    sealed = name + TAR_SUFFIX
    if not dag.is_valid_name(sealed):
        raise ValueError(f"the folder's name is {len(name.encode('utf-8'))} bytes: a sealed folder's name may have "
                         f"at most {dag.MAX_NAME_BYTES - len(TAR_SUFFIX)} (with {TAR_SUFFIX}, {dag.MAX_NAME_BYTES}); "
                         f"rename it")
    return sealed


def add_sealed_path(path: str, put: builder.Put, wrap: bool, recipients: list[bytes], seal_name: bool,
                    options: builder.BuildOptions | None = None,
                    max_entries: int = 100_000) -> tuple[builder.Built, list[str]]:
    """Seal the regular file or folder at path and build it (wrapped under its visible name if wrap), with options
    (the chunker, the deny check of the sealed bytes, builder.build_file; and progress, given the plaintext read).
    Returns it and the warnings for what was skipped.

    A folder is packed as one tar (tarstream.folder_chunks: add's rules, everything under the folder's name, at
    most max_entries files and directories) and sealed as `<folder>.tar` (folder_sealed_name), which is also its
    visible name with wrap."""
    from rnfs.content import tarstream

    handle, st = builder.open_path(path)
    try:
        name = os.path.basename(os.path.normpath(os.path.abspath(path)))
        progress = options.progress if options is not None else None
        warnings: list[str] = []
        if stat.S_ISDIR(st.st_mode):
            sealed_name = folder_sealed_name(name, seal_name)
            tar = tarstream.IteratorReader(tarstream.folder_chunks(handle, path, name, max_entries, warnings.append,
                                                                   progress))
            try:
                built = builder.add_stream(seal.SealingReader(tar, recipients, sealed_name), put, None,
                                           sealed_name if wrap else None, options=options)
            finally:
                tar.close()
            return built, warnings
        sealed_name = name if seal_name and dag.is_valid_name(name) else None
        with os.fdopen(handle, "rb", closefd=False) as f:
            plain = builder.counted(f, progress)
            built = builder.add_stream(seal.SealingReader(plain, recipients, sealed_name), put, None,
                                       name if wrap else None, options=options)
        return built, warnings
    finally:
        builder.close(handle)


def open_sealed(node, fetch, expect: walker.Expect, unlock: bool = True) -> seal.SealedFile | None:
    """The sealed file at a resolved target, unlocked with the node's identity; None if it isn't one.

    Raises NotRecipient (unlocking) if the node's identity can't open it, SealError if it is damaged.
    """
    block = fetch.get(expect)
    if walker.kind_of(expect, block) != walker.FILE_TYPE:
        return None
    stored_size = walker.size_of(expect, block)
    sealed = seal.open_sealed(lambda offset, length: fetch.read_range(expect, offset, length), stored_size)
    if sealed is not None and getattr(fetch, "observer", None) is not None:
        # read_range doesn't report a total (it serves many small ranges), so a job decrypting this file
        # would show "of ?": its stored size is what the chunks met add up to
        fetch.observer.total(stored_size)
    if sealed is not None and unlock:
        unlock_sealed(node, sealed)
    return sealed


def unlock_sealed(node, sealed: seal.SealedFile):
    """Unlock sealed with the node's identity. Raises NotRecipient if the node has none or isn't a recipient,
    SealError if the content key sealed for it doesn't decrypt."""
    identity = getattr(node, "identity", None)
    if identity is None:
        raise seal.NotRecipient("this node has no identity to open sealed files with (use --raw)")
    sealed.unlock(identity)


def is_recipient(node, sealed: seal.SealedFile) -> bool:
    """True if the node's identity is one of sealed's recipients."""
    identity = getattr(node, "identity", None)
    return identity is not None and bytes(identity.hash) in sealed.recipients


def sealed_info(node, info: dict, sealed: seal.SealedFile | None) -> dict:
    """A file's info, plus when it is sealed (sealed, as open_sealed opened it without unlocking) "sealed",
    "recipient" and "stored_size", and when this node is a recipient its plaintext "size", sealed "name" and
    "sealed_folder" (is_sealed_folder: get would unpack it) (sealed is then unlocked)."""
    if sealed is None:
        return info
    recipient = is_recipient(node, sealed)
    info = {**info, "sealed": True, "recipient": recipient, "stored_size": info["size"]}
    if recipient:
        unlock_sealed(node, sealed)
        info.update(size=sealed.size, name=sealed_name(sealed), sealed_folder=is_sealed_folder(sealed))
    return info


def sealed_name(sealed: seal.SealedFile) -> str | None:
    """A sealed file's name if it is fit to be a default output name; None for a hidden or unprintable one.

    The sealer chose it, and `rnfs get` without -o writes there: a dotfile such as .bash_login written into
    the current directory unnoticed could change how other programs behave.
    """
    from rnfs import fsutil

    return fsutil.safe_default_name(sealed.name, None)


def write_sealed(sealed: seal.SealedFile, output: str, blocks_dir: str | None = None, to_fetch: int = 0) -> dict:
    """Decrypt a sealed file to output (created exclusively), removing it again if anything fails.

    Refused (TooLarge) unless it leaves the disk reserve free, counting the to_fetch bytes still to be stored
    in blocks_dir if that is on the same filesystem (rnfs.node._check_free_space)."""
    from rnfs.node import _check_free_space, _remove_created  # rnfs.node imports this module

    _check_free_space(output, sealed.size, blocks_dir, to_fetch)
    name = sealed_name(sealed)  # before creating output, so nothing raising here leaves a file behind
    try:
        out = open(output, "xb")
    except FileExistsError:
        raise ValueError(f"{output} already exists") from None
    created = os.fstat(out.fileno())
    try:
        with out:
            for data in sealed.read():
                out.write(data)
    except BaseException:
        _remove_created(output, created)
        raise
    return {"sealed": True, "name": name}


class NotATar(Exception):
    """A sealed file named as a tar holds something else (unpack_sealed): it is written as a file instead."""


def is_tar_name(name: str | None) -> bool:
    """True if a sealed name says the file is a tar to unpack: it ends in TAR_SUFFIX, after at least one character."""
    return isinstance(name, str) and len(name) > len(TAR_SUFFIX) and name.endswith(TAR_SUFFIX)


def is_sealed_folder(sealed: seal.SealedFile) -> bool:
    """True if an unlocked sealed file is a sealed folder, as add --to of a folder makes one: its sealed name ends in
    .tar and its plaintext starts with the sealed-folder marker (tarstream.is_marked; decrypting its first segment
    only). A tar sealed as a file (by 0.1.x, or anyone) has no marker: it stays a file."""
    from rnfs.content import tarstream

    if not is_tar_name(sealed.name):
        return False
    return tarstream.is_marked(b"".join(sealed.read(0, tarstream.MARKER_PEEK)))


def get_sealed(node, sealed: seal.SealedFile, output: str, to_fetch: int = 0,
               created: list[walker.Identity] | None = None, extract: bool = True) -> dict:
    """Write a sealed file the node opened (unlocked) to output, which must not exist: unpacked (unpack_sealed) if
    extract and it is a sealed folder (is_sealed_folder) whose plaintext is a valid tar; otherwise decrypted as the
    file (write_sealed), as rnfs 0.1.6 writes any sealed file, with a warning only for a marked tar that isn't valid.
    get's {"sealed", "name", "warnings"} (+ "warnings_total"; "extracted" True when unpacked). to_fetch and
    created as for unpack_sealed."""
    warnings = []
    if extract and is_sealed_folder(sealed):
        try:
            return unpack_sealed(node, sealed, output, to_fetch, created)
        except NotATar as e:
            warnings.append(f"not unpacked, written as the file: its content isn't a valid tar ({e})")
    return {**write_sealed(sealed, output, node.config.blocks_dir, to_fetch), "warnings": warnings}


def unpack_sealed(node, sealed: seal.SealedFile, output: str, to_fetch: int = 0,
                  created: list[walker.Identity] | None = None) -> dict:
    """Unpack a sealed tar to the directory output (which must not exist), reusing both existing rule sets as they
    are: its plaintext is built into the block store by tarstream.build_tar (POST /api/add/stream?format=tar's
    reader: regular files and directories only, no absolute or .. paths, links and devices skipped with warnings,
    max_output_entries and MAX_DEPTH, the deny list's file hashes), then written out by walker.write_out (get's
    rules: unsafe names skipped, the same limits); created gets the output directory's identity, as write_out
    gives it. Everything happens inside a protection session, so no collection takes the blocks meanwhile, and they
    are never pinned, recorded as a fetch or provided: once it ends (unpacked or not) they are collected.

    The output is the tar's one top directory (`<folder>/`, as add packs a folder), or, for a tar holding anything
    else, the whole tar as a directory; "name" is that directory's name (the top one's, or the sealed name without
    .tar), only if safe_default_name accepts it. Raises NotATar (having written nothing) if the plaintext isn't a
    valid tar, TooLarge past a limit or if the disk can't hold both the plaintext's blocks and the files written
    (they may be on one filesystem: counted twice then, with the to_fetch bytes still to fetch), and what the
    decryption raises (SealError, NotRecipient)."""
    from rnfs import fsutil
    from rnfs.content import tarstream
    from rnfs.errors import BlockMismatch, TooLarge
    from rnfs.log import LOG_ERROR, log
    from rnfs.node import _check_free_space  # rnfs.node imports this module

    plain = sealed.size
    blocks_dir = node.config.blocks_dir
    # The blocks and the files: the second is counted on the output's filesystem only if the blocks are there too
    _check_free_space(output, plain, blocks_dir, to_fetch + plain)
    node.storage.check_disk(plain + to_fetch)
    max_entries = node.config.max_output_entries
    warnings = tarstream.Warnings()
    tar = tarstream.IteratorReader(sealed.read())
    try:
        with node.storage.protect() as session:
            try:
                with session.writer() as put:
                    built = tarstream.build_tar(tar, put, warnings, max_entries,
                                                builder.BuildOptions(check=node._hash_check()))
            except (seal.SealError, BlockMismatch):
                raise  # ValueErrors, but a damaged sealed file or a bug, not a plaintext that isn't a tar
            except TooLarge as e:
                raise TooLarge(f"can't unpack it: {e} (get --no-extract writes the tar itself)") from None
            except ValueError as e:
                raise NotATar(str(e)) from None
            get = node.storage.getter()
            root = walker.root_expect(built.cid)
            first = list(itertools.islice(walker.directory_entries(root, get), 2))
            if len(first) == 1 and first[0][0].kind == dag.KIND_DIRECTORY:
                entry, target = first[0]
                name = entry.name
            else:
                target, name = root, sealed.name[:-len(TAR_SUFFIX)]
            try:
                written = walker.write_out(target, get, output, max_entries, created=created)
            except TooLarge as e:
                raise TooLarge(f"can't unpack it: {e} (get --no-extract writes the tar itself)") from None
    finally:
        tar.close()
        # Nothing reaches the plaintext's blocks now (unless the same blocks are kept for another reason)
        try:
            node.storage.collect()
        except Exception as e:  # noqa: BLE001 - never hide the get's own outcome; the next collection retries
            log(f"get: collecting an unpacked tar's blocks failed: {e!r}", LOG_ERROR)
    for warning in written:
        warnings(warning)
    return {"sealed": True, "name": fsutil.safe_default_name(name, None), "extracted": True, **warnings.result()}


def stored_head(node, target: str, length: int, deny: bool = True) -> bytes | None:
    """A file's first length bytes from stored blocks only (the gateway's sealed check and sniffing), or
    None if one they need isn't stored (or the target is a directory or invalid). Never fetches.

    With deny, the deny list applies as to a read (a HEAD of a denied file answers 451, like GET): Denied for a
    denied CID met, and while there are file hashes the file is checked first (node._check_file, bounded), which
    takes it wholly stored (None otherwise: it can't be checked without fetching). `pin ls` passes deny=False.
    """
    try:
        root, names = walker.parse_target(target)
        get = node.storage.getter()
        if deny:
            get = denying(node, get)
        expect = walker.resolve(root, names, get)
        if deny and node.deny.hashes and walker.kind_of(expect, get(expect)) == walker.FILE_TYPE:
            if not expect.cid.is_raw and node.deny.known_hash(expect.cid) is None \
                    and not node.storage.is_complete(expect.cid):
                # leaf_bytes counts the chunks its file nodes list, so they must all be there to be counted
                return None
            node._check_file(expect, get)
        return b"".join(walker.read_range(expect, get, 0, length))
    except (ValueError, NotFound, InvalidDag):
        return None


def denying(node, get: walker.Get) -> walker.Get:
    """get, raising Denied for a CID on the node's deny list instead of reading it (as a fetch does)."""

    def checked(expect: walker.Expect) -> bytes:
        """The block for expect, unless its CID is denied."""
        if node.deny.denies(expect.cid):
            raise Denied(f"{expect.cid} is on this node's deny list")
        return get(expect)

    return checked


def block(node, cid: str, for_gateway: bool = False) -> bytes:
    """A CID's own block, fetched if needed and re-verified. A raw block is a whole file, so the deny list's
    file hashes apply to it (its digest is its content's SHA-256)."""
    root = Cid.parse(cid)
    with node._fetching(root, [], None, for_gateway) as fetch:
        expect = fetch.resolve()
        if expect.cid.is_raw:
            node._check_file(expect, fetch.get)  # never reads: a raw CID carries the hash
        data = node.storage.get(expect.cid, verify=True)
    if data is None:
        raise NotFound(f"block {cid} was damaged locally and has been deleted; try again")
    return data


def export_stream(node, target: str, start: Callable[[int], BinaryIO | None], for_gateway: bool = False) -> dict:
    """Fetch the target's DAG, then write its archive to start(exact archive length): the stream to write to,
    or None to write nothing. {"roots", "blocks", "length"}; limits and checks as export_chunks."""
    chunks = export_chunks(node, target, for_gateway)
    try:
        info = next(chunks)
        out = start(info["length"])
        if out is not None:
            for data in chunks:
                out.write(data)
        return info
    finally:
        chunks.close()  # ends the fetch, as leaving its block would


def _fetched_dag(node, fetch, expect: walker.Expect):
    """Fetch the whole DAG of a resolved target to send it (an archive or a tar): limits as for export
    (max_download_size, max_output_entries counting every occurrence, the DAG byte bound of fetch_dag), and every
    file checked against the deny list's file hashes before anything is sent, as the gateway checks what it
    sends."""
    fetch.fetch_dag(expect, node.config.max_output_entries)
    walker.count_entries(expect, fetch.get, node.config.max_output_entries)
    if node.deny.hashes:
        for file in walker.iter_files(expect, fetch.get):
            node._check_file(file, fetch.get)


def export_chunks(node, target: str, for_gateway: bool = False) -> Iterator:
    """The target's archive as a generator: first {"roots", "blocks", "length"} once its DAG is fetched and
    checked (_fetched_dag), then its bytes, byte-identical to `rnfs export` (?format=archive, GET
    /api/export/stream). The fetch stays open until the generator ends or is closed. A leading contact nick is
    expanded unless for_gateway."""
    root, names = node._parse_target(target, for_gateway)
    with node._fetching(root, names, None, for_gateway) as fetch:
        expect = fetch.resolve()
        _fetched_dag(node, fetch, expect)
        blocks = node.storage.dag_blocks(expect.cid)
        length = archive.archive_size([expect.cid], ((cid, node.storage.block_size(cid) or 0) for cid in blocks))
        yield {"roots": [str(expect.cid)], "blocks": len(blocks), "length": length}
        yield from archive.archive_chunks([expect], fetch.get)


SKIPS_LOGGED = 10  # entries a tar of a target skips that are logged by name (the rest only counted)


def tar_chunks(node, target: str) -> Iterator:
    """The target as a tar (rnfs/content/tarstream.py), as a generator: first {"cid", "length", "skipped"} once its
    DAG is fetched and checked (_fetched_dag), then the tar's bytes (GET /api/get/stream?format=tar). A file is
    one member named by the target's last path name (or its CID for a bare CID, or one unsafe in a tar); entries
    whose names are unsafe in a tar are skipped (counted in "skipped", the first SKIPS_LOGGED logged). The fetch
    stays open until the generator ends or is closed."""
    from rnfs.content import tarstream
    from rnfs.log import LOG_WARNING, log

    root, names = node._parse_target(target)
    max_entries = node.config.max_output_entries
    with node._fetching(root, names) as fetch:
        expect = fetch.resolve()
        _fetched_dag(node, fetch, expect)
        name = names[-1] if names and tarstream.safe_name(names[-1]) else str(expect.cid)
        skipped = tarstream.Warnings(SKIPS_LOGGED)
        members = tarstream.plan(expect, fetch.get, name, max_entries, skipped)
        for path in skipped.kept:
            log(f"tar of {target}: skipped {path!r}: a character in its name could be read as a path", LOG_WARNING)
        if skipped.total > len(skipped.kept):
            log(f"tar of {target}: skipped {skipped.total - len(skipped.kept)} more such entries", LOG_WARNING)
        mtime = int(time.time())
        yield {"cid": str(expect.cid), "length": tarstream.length(members, mtime), "skipped": skipped.total}
        yield from tarstream.chunks(members, fetch.get, mtime, max_entries)


# API parameters


def add_options(value: Callable[[str], object], stream: bool = False) -> dict:
    """The add options this feature adds (pin, to, to_self, seal_name, chunker), from a JSON body's or a query
    string's value(name) (None when absent); only those set, so the facade's defaults apply otherwise.

    seal_name means two things: for POST /api/add (JSON) whether to seal the file's name, which must be a JSON
    boolean (ValueError otherwise); for POST /api/add/stream (stream, a query string) the name to seal inside.
    """
    from rnfs.api import parse_bool

    options: dict = {}
    if value("pin") is not None:
        options["pin"] = parse_bool(value("pin"))
    to = value("to")
    if to:
        if isinstance(to, str):
            to = [part for part in to.split(",") if part.strip()]  # query strings: comma-separated
        if not isinstance(to, list) or not all(isinstance(item, str) for item in to):
            raise ValueError("to must be a list of recipients")
        options["to"] = to
    if value("to_self") is not None and parse_bool(value("to_self")):
        options["to_self"] = True
    seal_name = value("seal_name")
    if seal_name is not None:
        # isinstance, not ==: True == 1, and a string like "false" would count as true
        if not stream and not isinstance(seal_name, bool):
            raise ValueError("seal_name must be true or false")
        options["seal_name"] = seal_name
    if value("chunker"):
        from rnfs.content import chunker
        options["chunker"] = chunker.check(str(value("chunker")))
    return options
