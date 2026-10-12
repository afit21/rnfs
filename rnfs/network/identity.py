"""Node identities: the RNS key pair a node keeps across restarts, and the node IDs derived from it."""
import os
import stat

import RNS

from rnfs import fsutil
from rnfs.constants import APP_NAME, DHT_ASPECT, ID_LENGTH
from rnfs.log import LOG_WARNING, log

IDENTITY_MODE = 0o600  # the file holds the private key: readable by its owner only


def load_or_create_identity(path: str) -> RNS.Identity:
    """The identity saved at path, or a new one saved there. Raises RuntimeError if it can't be read or written."""
    if not os.path.isfile(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        identity = RNS.Identity()
        try:
            if save_identity(identity, path, replace=False):
                return identity
        except OSError as e:
            raise RuntimeError(f"Could not save identity to {path}: {e}") from e
        # Another process starting on the same directory saved its identity first: use that one
    _restrict_permissions(path)
    identity = RNS.Identity.from_file(path)
    if identity is None:
        # Don't overwrite: a new identity would mean a new node ID
        raise RuntimeError(f"Could not load identity from {path}")
    return identity


def save_identity(identity: RNS.Identity, path: str, replace: bool = True) -> bool:
    """Write identity's private key to path in RNS's raw format, owner-only and atomically. Raises OSError.

    With replace False, an existing file at path is kept (False is returned), even one another process creates
    meanwhile. Not Identity.to_file, which creates the file with the process umask (often group- or
    world-readable) and could leave a truncated key behind if interrupted.
    """
    # The file is created with O_CREAT | O_EXCL and mode 0600 (IDENTITY_MODE), which a umask can only narrow,
    # so no chmod is needed (and os.fchmod doesn't exist on Windows before Python 3.13). On Windows the file
    # inherits the rnfs dir's ACL instead: under the user's profile, only them, admins and SYSTEM. Without
    # replace, rename_new keeps an existing file atomically where the platform allows (hard link,
    # RENAME_NOREPLACE)
    try:
        fsutil.write_atomic(path, identity.get_private_key(), mode=IDENTITY_MODE, exclusive=not replace)
    except FileExistsError:
        if replace:
            raise
        return False
    return True


def _restrict_permissions(path: str):
    """Make an existing identity file owner-only if group or others could access it (e.g. an older rnfs's)."""
    if fsutil.IS_WINDOWS:
        return  # no Unix permission bits: st_mode always shows group and others allowed, so this would always fire
    try:
        mode = stat.S_IMODE(os.stat(path).st_mode)
        if mode & 0o077:
            os.chmod(path, IDENTITY_MODE)
            log(f"identity file {path} was accessible to other users (mode {mode:o}); "
                f"restricted it to {IDENTITY_MODE:o}", LOG_WARNING)
    except OSError as e:
        log(f"could not restrict the permissions of identity file {path}: {e}", LOG_WARNING)


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
