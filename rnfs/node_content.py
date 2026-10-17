"""The node's side of the "content" features (rnfs/node.py calls these; logic kept out of the shared file):

- `add --no-pin`: what was added is kept as a cached fetch instead of a "user" pin (keep_added).
- Sealed files (rnfs/content/seal.py): resolving recipients to public keys, sealing a file being added,
  and opening a sealed target with the node's identity for get, cat and info.
- Gateway formats: a CID's own block (?format=raw) and a target's archive streamed with its exact length
  (?format=archive).

Functions take the RnfsNode first. RNS is imported inside functions: the daemon has loaded it already.
"""
import os
import stat
from collections.abc import Callable
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


def add_sealed_path(path: str, put: builder.Put, wrap: bool, recipients: list[bytes], seal_name: bool,
                    options: builder.BuildOptions | None = None) -> builder.Built:
    """Seal the regular file at path and build it (wrapped under its visible name if wrap), with options (the
    chunker, and the deny check of the sealed bytes; builder.build_file)."""
    handle, st = builder.open_path(path)
    try:
        if not stat.S_ISREG(st.st_mode):
            raise ValueError("--to seals single files: put a directory in an archive (e.g. tar) and add that")
        name = os.path.basename(os.path.normpath(os.path.abspath(path)))
        sealed_name = name if seal_name and dag.is_valid_name(name) else None
        with os.fdopen(handle, "rb", closefd=False) as f:
            return builder.add_stream(seal.SealingReader(f, recipients, sealed_name), put, None,
                                      name if wrap else None, options=options)
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
        identity = getattr(node, "identity", None)
        if identity is None:
            raise seal.NotRecipient("this node has no identity to open sealed files with (use --raw)")
        sealed.unlock(identity)
    return sealed


def sealed_info(node, fetch, expect: walker.Expect, info: dict) -> dict:
    """info, plus for a sealed file "sealed", "recipient" and "stored_size", and when this node is a
    recipient its plaintext "size" and sealed "name"."""
    if info.get("type") != "file":
        return info
    sealed = open_sealed(node, fetch, expect, unlock=False)
    if sealed is None:
        return info
    identity = getattr(node, "identity", None)
    recipient = identity is not None and bytes(identity.hash) in sealed.recipients
    info = {**info, "sealed": True, "recipient": recipient, "stored_size": info["size"]}
    if recipient:
        sealed.unlock(identity)
        info.update(size=sealed.size, name=sealed_name(sealed))
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
    or None to write nothing. Limits as for export: max_download_size, max_output_entries, and the DAG
    byte bound of fetch_dag. Every file is checked against the deny list's file hashes before anything is sent,
    as the gateway checks what it sends."""
    root, names = walker.parse_target(target)
    with node._fetching(root, names, None, for_gateway) as fetch:
        expect = fetch.resolve()
        fetch.fetch_dag(expect, node.config.max_output_entries)
        walker.count_entries(expect, fetch.get, node.config.max_output_entries)
        if node.deny.hashes:
            for file in walker.iter_files(expect, fetch.get):
                node._check_file(file, fetch.get)
        blocks = node.storage.dag_blocks(expect.cid)
        length = archive.archive_size([expect.cid], ((cid, node.storage.block_size(cid) or 0) for cid in blocks))
        out = start(length)
        if out is None:
            return {"roots": [str(expect.cid)], "blocks": len(blocks), "length": length}
        stats = archive.export_archive([expect], fetch.get, out)
    return {"roots": [str(cid) for cid in stats["roots"]], "blocks": stats["blocks"], "length": length}


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
