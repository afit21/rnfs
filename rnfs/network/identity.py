"""Node identities: the RNS key pair a node keeps across restarts, and the node IDs derived from it."""
import os
import stat

import RNS

from rnfs import fsutil, winacl
from rnfs.constants import APP_NAME, DHT_ASPECT, ID_LENGTH
from rnfs.errors import RnfsError
from rnfs.log import LOG_WARNING, log

IDENTITY_MODE = 0o600  # the file holds the private key: readable by its owner only
MAX_IDENTITY_FILE = 4096  # RNS's key file is 64 bytes; a larger file isn't one


class IdentityFileError(RnfsError, RuntimeError):
    """The identity file can't be used: a link, not a regular file, someone else's, or not made private.
    Stops the node with this message (it is an RnfsError) rather than a traceback."""


def load_or_create_identity(path: str) -> RNS.Identity:
    """The identity saved at path, or a new one saved there. Raises RuntimeError if it can't be read or written
    (IdentityFileError if the file there can't be trusted)."""
    try:
        return _load_identity(path)
    except FileNotFoundError:
        pass
    os.makedirs(os.path.dirname(path), exist_ok=True)
    identity = RNS.Identity()
    try:
        if save_identity(identity, path, replace=False):
            return identity
    except OSError as e:
        raise RuntimeError(f"Could not save identity to {path}: {e}") from e
    # Another process starting on the same directory saved its identity first: use that one
    try:
        return _load_identity(path)
    except FileNotFoundError:
        raise RuntimeError(f"Could not load identity from {path}: it disappeared") from None


def save_identity(identity: RNS.Identity, path: str, replace: bool = True) -> bool:
    """Write identity's private key to path in RNS's raw format, owner-only and atomically. Raises OSError.

    With replace False, an existing file at path is kept (False is returned), even one another process creates
    meanwhile. Not Identity.to_file, which creates the file with the process umask (often group- or
    world-readable) and could leave a truncated key behind if interrupted.
    """
    # The file is created with O_CREAT | O_EXCL and mode 0600 (IDENTITY_MODE), which a umask can only narrow,
    # so no chmod is needed (and os.fchmod doesn't exist on Windows before Python 3.13). On Windows, where the
    # mode sets no access list, it gets a protected one for this user and SYSTEM only (private). Without
    # replace, rename_new keeps an existing file atomically where the platform allows (hard link,
    # RENAME_NOREPLACE)
    try:
        fsutil.write_atomic(path, identity.get_private_key(), mode=IDENTITY_MODE, exclusive=not replace,
                            private=True)
    except FileExistsError:
        if replace:
            raise
        return False
    return True


def _load_identity(path: str) -> RNS.Identity:
    """The identity in the existing file at path, read through the very descriptor whose type, owner and
    permissions were checked. Raises FileNotFoundError if nothing is at path, IdentityFileError if what is
    there can't be trusted, RuntimeError if it holds no key.

    The file must be a regular file (a FIFO would block the start, a device be read without end), not a
    symlink or junction (someone who can write to the directory could point it at a key of theirs), owned by
    this user, and private to them: an existing file others may read (e.g. an older rnfs's) is tightened
    through the descriptor (with a warning), and if that fails rnfs stops, since the key may be read by others.
    On Windows, which has no Unix permission bits (st_mode always shows group and others allowed), its owner
    and access list are checked and tightened instead (winacl).
    """
    try:
        listed = os.lstat(path)
    except FileNotFoundError:
        raise
    except OSError as e:
        raise IdentityFileError(f"can't use the identity file {path}: {e}") from None
    if fsutil.is_link(listed):
        raise IdentityFileError(f"the identity file {path} is a symlink or junction; rnfs only uses a regular "
                                f"file there. Put the key file itself there (it decides the node ID)")
    try:
        fd = os.open(path, os.O_RDONLY | fsutil.O_NOFOLLOW | fsutil.O_BINARY | getattr(os, "O_NONBLOCK", 0)
                     | getattr(os, "O_CLOEXEC", 0))
    except FileNotFoundError:
        raise
    except OSError as e:  # ELOOP: swapped for a symlink since the lstat
        raise IdentityFileError(f"can't use the identity file {path}: {e}") from None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise IdentityFileError(f"the identity file {path} is not a regular file; rnfs only uses a regular "
                                    f"file there")
        if (info.st_dev, info.st_ino) != (listed.st_dev, listed.st_ino):
            raise IdentityFileError(f"the identity file {path} changed while being opened")
        _check_private(path, fd, info)
        data = b""
        while len(data) <= MAX_IDENTITY_FILE:
            chunk = os.read(fd, MAX_IDENTITY_FILE + 1 - len(data))
            if not chunk:
                break
            data += chunk
    except OSError as e:
        raise IdentityFileError(f"can't use the identity file {path}: {e}") from None
    finally:
        os.close(fd)
    identity = RNS.Identity.from_bytes(data) if len(data) <= MAX_IDENTITY_FILE else None
    if identity is None:
        # Don't overwrite: a new identity would mean a new node ID
        raise RuntimeError(f"Could not load identity from {path}")
    return identity


def _check_private(path: str, fd: int, info: os.stat_result):
    """Check that the identity file open as fd (whose fstat is info) is the user's own and private to them,
    tightening its permissions if others may access it. Raises IdentityFileError, OSError."""
    if fsutil.IS_WINDOWS:
        try:
            reason = winacl.tighten(path, fd=fd)
        except OSError as e:
            raise IdentityFileError(f"the identity file {path} can't be made private to you: {e}") from None
        if reason is not None:
            log(f"identity file {path} was accessible to other users ({reason}); "
                f"restricted it to you and SYSTEM", LOG_WARNING)
        return
    if info.st_uid != os.geteuid():
        raise IdentityFileError(f"the identity file {path} belongs to another user (uid {info.st_uid}); "
                                f"rnfs only uses a key file of your own")
    mode = stat.S_IMODE(info.st_mode)
    if mode & 0o077:
        try:
            os.fchmod(fd, IDENTITY_MODE)
        except OSError as e:
            raise IdentityFileError(f"the identity file {path} is accessible to other users (mode {mode:o}) "
                                    f"and can't be restricted: {e}") from None
        log(f"identity file {path} was accessible to other users (mode {mode:o}); "
            f"restricted it to {IDENTITY_MODE:o}", LOG_WARNING)


def node_id_for_identity(identity: RNS.Identity) -> bytes:
    """The node ID of whoever holds identity: Destination.hash(identity, "rnfs", "dht")."""
    return RNS.Destination.hash(identity, APP_NAME, DHT_ASPECT)


def parse_node_id(hex_id: str) -> bytes:
    """Node ID from its hex form, with or without RNS's <angle brackets>. Raises ValueError if malformed."""
    node_id = bytes.fromhex(hex_id.strip().strip("<>"))
    if len(node_id) != ID_LENGTH:
        raise ValueError(f"node ID must be {ID_LENGTH * 2} hex characters: {hex_id}")
    return node_id


def parse_identity_hash(hex_hash: str) -> bytes:
    """RNS identity hash from its 32-hex-character form (members, remote_pin_identities). Raises ValueError."""
    identity_hash = bytes.fromhex(hex_hash.strip().strip("<>"))
    if len(identity_hash) != RNS.Identity.TRUNCATED_HASHLENGTH // 8:
        raise ValueError(f"identity hash must be {RNS.Identity.TRUNCATED_HASHLENGTH // 4} hex characters: {hex_hash}")
    return identity_hash
