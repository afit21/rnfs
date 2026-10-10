"""Node identities: the RNS key pair a node keeps across restarts, and the node IDs derived from it."""
import os

import RNS

from rnfs.constants import APP_NAME, DHT_ASPECT, ID_LENGTH


def load_or_create_identity(path: str) -> RNS.Identity:
    """The identity saved at path, or a new one saved there. Raises RuntimeError if it can't be read or written."""
    if os.path.isfile(path):
        identity = RNS.Identity.from_file(path)
        if identity is None:
            # Don't overwrite: a new identity would mean a new node ID
            raise RuntimeError(f"Could not load identity from {path}")
        return identity

    os.makedirs(os.path.dirname(path), exist_ok=True)
    identity = RNS.Identity()
    if not identity.to_file(path):
        raise RuntimeError(f"Could not save identity to {path}")
    return identity


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
