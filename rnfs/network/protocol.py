"""rnfs.dht wire format (specification section 3). Messages are RNS link requests, encoded with msgpack by RNS.

| Path      | Request                           | Response                       |
| /find     | key                               | [[provider IDs], [node IDs]]   |
| /provide  | [[key, ...], [withdrawn key, ...]]| True if recorded               |
| /blocks   | [max_bytes, [cid, ...]]           | [block or None, ...]           |
| /pin      | [cid, pin]                        | True if accepted               |

Keys and node IDs are 16-byte bin, CIDs binary CIDs as bin, blocks bin, max_bytes a non-negative integer
(never a boolean), pin a boolean. Any other shape, or a refusal, is answered False.

Everything parsed here comes from other nodes, so parsers are strict and return None for anything malformed.
Every request must fit in one packet: at most min(link MDU, MAX_REQUEST_SIZE) bytes including RNS's request
framing, which is computed exactly here the way Link.request packs it.
"""
import time

import RNS
from RNS.vendor import umsgpack

from rnfs.constants import (BLOCKS_PATH, FIND_PATH, ID_LENGTH, K, KIB, MAX_BLOCK_SIZE, MAX_REQUEST_SIZE,
                            MAX_RESPONSE_SIZE, PIN_PATH, PROVIDE_PATH)
from rnfs.content.cid import Cid

__all__ = ["FIND_PATH", "PROVIDE_PATH", "BLOCKS_PATH", "PIN_PATH"]

DEFAULT_MDU = RNS.Link.MDU  # packet MDU of a link at the default 500-byte MTU (431 bytes)


# Request sizes and batching

def request_size(path: str, data) -> int:
    """Bytes of the packed request Link.request would send for data, including RNS's framing."""
    # Link.request packs [time.time(), truncated path hash, data]; umsgpack always packs a float as a
    # 9-byte double, so the timestamp's value doesn't change the size
    path_hash = RNS.Identity.truncated_hash(path.encode("utf-8"))
    return len(umsgpack.packb([time.time(), path_hash, data]))


def request_limit(mdu: int | None) -> int:
    """Largest packed request allowed on a link with this packet MDU (None: the default MTU's)."""
    return min(mdu if mdu else DEFAULT_MDU, MAX_REQUEST_SIZE)


def fits(path: str, data, mdu: int | None) -> bool:
    """True if a request with this data fits in one packet on a link with this MDU."""
    return request_size(path, data) <= request_limit(mdu)


# Field checks

def is_id(value) -> bool:
    """True if value is a well-formed node ID or DHT key: exactly ID_LENGTH bytes."""
    return type(value) is bytes and len(value) == ID_LENGTH


def is_id_list(value, max_length: int | None = None) -> bool:
    """True if value is a list of well-formed IDs, at most max_length long if given."""
    if type(value) is not list or (max_length is not None and len(value) > max_length):
        return False
    return all(is_id(item) for item in value)


def parse_cid(value) -> Cid | None:
    """The CID whose binary form is value, or None if it isn't a valid binary CID."""
    if type(value) is not bytes:
        return None
    try:
        return Cid.from_bytes(value)
    except ValueError:
        return None


def is_count(value) -> bool:
    """True if value is a non-negative integer and not a boolean (msgpack keeps them apart)."""
    return type(value) is int and value >= 0


# /find

def encode_find(key: bytes) -> bytes:
    """/find request for key."""
    return key


def parse_find_request(data) -> bytes | None:
    """The key from a /find request, or None if malformed."""
    return data if is_id(data) else None


def encode_find_response(providers: list[bytes], nodes: list[bytes]) -> list:
    """/find response, each list truncated to K."""
    return [list(providers[:K]), list(nodes[:K])]


def parse_find_response(data) -> tuple[list[bytes], list[bytes]] | None:
    """(providers, nodes) from a /find response, or None if malformed (including False, a refusal)."""
    if type(data) is not list or len(data) != 2:
        return None
    providers, nodes = data
    if not is_id_list(providers, K) or not is_id_list(nodes, K):
        return None
    return providers, nodes


# /provide

def encode_provide(keys: list[bytes], withdrawn: list[bytes]) -> list:
    """/provide request storing keys and withdrawing withdrawn."""
    return [list(keys), list(withdrawn)]


def parse_provide_request(data) -> tuple[list[bytes], list[bytes]] | None:
    """(keys, withdrawn keys) from a /provide request, or None if malformed."""
    if type(data) is not list or len(data) != 2:
        return None
    keys, withdrawn = data
    if not is_id_list(keys) or not is_id_list(withdrawn):
        return None
    return keys, withdrawn


def provide_batches(keys: list[bytes], withdrawn: list[bytes], mdu: int | None) -> list[tuple[list, list]]:
    """Split keys and withdrawn keys into as few /provide requests as fit one packet each, in order."""
    batches = []
    current_keys: list[bytes] = []
    current_withdrawn: list[bytes] = []
    items = [(key, False) for key in keys] + [(key, True) for key in withdrawn]
    for key, is_withdrawal in items:
        trial_keys = current_keys + ([] if is_withdrawal else [key])
        trial_withdrawn = current_withdrawn + ([key] if is_withdrawal else [])
        if fits(PROVIDE_PATH, encode_provide(trial_keys, trial_withdrawn), mdu):
            current_keys, current_withdrawn = trial_keys, trial_withdrawn
            continue
        # A single 16-byte key always fits, so the current batch isn't empty here
        batches.append((current_keys, current_withdrawn))
        current_keys = [] if is_withdrawal else [key]
        current_withdrawn = [key] if is_withdrawal else []
    if current_keys or current_withdrawn:
        batches.append((current_keys, current_withdrawn))
    return batches


# /blocks

def encode_blocks(max_bytes: int, cids: list[Cid]) -> list:
    """/blocks request for cids, asking for at most max_bytes of blocks after the first."""
    return [int(max_bytes), [cid.binary for cid in cids]]


def parse_blocks_request(data) -> tuple[int, list[Cid]] | None:
    """(max_bytes, CIDs) from a /blocks request, or None if malformed."""
    if type(data) is not list or len(data) != 2:
        return None
    max_bytes, binaries = data
    if not is_count(max_bytes) or type(binaries) is not list:
        return None
    cids = [parse_cid(binary) for binary in binaries]
    if any(cid is None for cid in cids):
        return None
    return max_bytes, cids


def blocks_batch_size(max_bytes: int, cids: list[Cid], mdu: int | None) -> int:
    """How many of cids, from the start, fit in one /blocks request with this max_bytes."""
    count = 0
    # CIDs are 36 or 39 bytes, so a packet holds at most a few dozen; a linear search is cheap
    while count < len(cids) and fits(BLOCKS_PATH, encode_blocks(max_bytes, cids[:count + 1]), mdu):
        count += 1
    return count


def max_response_size(max_bytes: int) -> int:
    """The max_response_size a requester passes to RNS for a /blocks request (section 7)."""
    return min(max(max_bytes, MAX_BLOCK_SIZE) + KIB, MAX_RESPONSE_SIZE)


def parse_blocks_response(data, requested: int) -> list[bytes | None] | None:
    """Entries of a /blocks response to a request for `requested` CIDs, or None if malformed.

    Each entry is a block (bytes, at most MAX_BLOCK_SIZE) or None, and there are no more entries than CIDs
    requested. Blocks still have to be verified against their CIDs.
    """
    if type(data) is not list or len(data) > requested:
        return None
    for entry in data:
        if entry is None:
            continue
        if type(entry) is not bytes or len(entry) > MAX_BLOCK_SIZE:
            return None
    return data


def encode_blocks_response(entries: list[bytes | None]) -> list:
    """/blocks response: one entry per CID until the budget was reached."""
    return list(entries)


# /pin

def encode_pin(cid: Cid, pin: bool) -> list:
    """/pin request asking the node to pin (True) or release (False) cid."""
    return [cid.binary, bool(pin)]


def parse_pin_request(data) -> tuple[Cid, bool] | None:
    """(CID, pin) from a /pin request, or None if malformed."""
    if type(data) is not list or len(data) != 2:
        return None
    cid = parse_cid(data[0])
    if cid is None or type(data[1]) is not bool:
        return None
    return cid, data[1]


# Responses that are just True or False

def is_accepted(data) -> bool:
    """True only if a /provide or /pin response says True."""
    return data is True


def packed_response_size(request_id: bytes, response) -> int:
    """Bytes RNS packs for a response, which decides whether it goes as a packet or a Resource."""
    return len(umsgpack.packb([request_id, response]))
