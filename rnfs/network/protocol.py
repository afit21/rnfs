"""rnfs.dht wire format (specification section 3). Messages are RNS link requests, encoded with msgpack by RNS.

| Path      | Request                           | Response                       |
| /find     | key                               | [[provider IDs], [node IDs]]   |
| /provide  | [[key, ...], [withdrawn key, ...]]| True if recorded               |
| /blocks   | [max_bytes, [cid, ...]]           | [block or None, ...]           |
| /pin      | [cid, pin]                        | True if accepted               |
| /name     | name                              | the record's fields, or []     |

Keys, node IDs and names are 16-byte bin, CIDs binary CIDs as bin, blocks bin, max_bytes a non-negative integer
(never a boolean), pin a boolean. Any other shape, or a refusal, is answered False.

/name is the keepers feature's path (rnfs.features.keepers encodes and parses it). Its response is a kept
record's [public_key, name_hash, random_hash, ratchet, signature, app_data], every field bin, or [] if the node
keeps none: the shape of a /blocks answer, which is what lets response_refusal accept it.

Everything parsed here comes from other nodes, so parsers are strict and return None for anything malformed.
Every request must fit in one packet: at most min(link MDU, MAX_REQUEST_SIZE) bytes including RNS's request
framing, which is computed exactly here the way Link.request packs it.
"""
import time

import RNS
from RNS.vendor import umsgpack

from rnfs.constants import (BLOCKS_PATH, FIND_PATH, ID_LENGTH, K, KIB, MAX_BLOCK_SIZE, MAX_PROVIDE_KEYS,
                            MAX_REQUEST_SIZE, MAX_RESPONSE_SIZE, PIN_PATH, PROVIDE_PATH)
from rnfs.content.cid import RAW, Cid

__all__ = ["FIND_PATH", "PROVIDE_PATH", "BLOCKS_PATH", "PIN_PATH"]

DEFAULT_MDU = RNS.Link.MDU  # packet MDU of a link at the default 500-byte MTU (431 bytes)
# RNS's request timeout without RTT (11.25 s): what it adds to rtt × traffic_timeout_factor (Link.request with
# timeout=None). Upload pacing holds a /blocks response back at most half of it, 5.625 s (section 3)
REQUEST_TIMEOUT_WITHOUT_RTT = RNS.Resource.RESPONSE_MAX_GRACE_TIME * 1.125


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
    """(keys, withdrawn keys) from a /provide request, or None if malformed (over MAX_PROVIDE_KEYS in all)."""
    if type(data) is not list or len(data) != 2:
        return None
    keys, withdrawn = data
    if type(keys) is not list or type(withdrawn) is not list or len(keys) + len(withdrawn) > MAX_PROVIDE_KEYS:
        return None
    if not is_id_list(keys) or not is_id_list(withdrawn):
        return None
    return keys, withdrawn


def provide_batches(keys: list[bytes], withdrawn: list[bytes], mdu: int | None) -> list[tuple[list, list]]:
    """Split keys and withdrawn keys into as few /provide requests as fit one packet each, in order.

    Withdrawals go first: a batch withdraws before it stores, so a later batch's withdrawal would delete a
    record an earlier one stored. A key in both lists is only stored.
    """
    batches = []
    current_keys: list[bytes] = []
    current_withdrawn: list[bytes] = []
    stored = set(keys)
    items = [(key, True) for key in withdrawn if key not in stored] + [(key, False) for key in keys]
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
    """(max_bytes, CIDs) from a /blocks request, or None if malformed.

    A request that repeats a CID is malformed: honest requesters ask for each CID once (the client drops
    duplicates), so a duplicate is only ever an attempt to make the node read and serve the same block several
    times in one response (specification section 7).
    """
    if type(data) is not list or len(data) != 2:
        return None
    max_bytes, binaries = data
    if not is_count(max_bytes) or type(binaries) is not list:
        return None
    cids = [parse_cid(binary) for binary in binaries]
    if any(cid is None for cid in cids) or len(set(cids)) != len(cids):
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
    requested. A response cut short by the budget holds at least one block, since the first served block is
    always included (section 7): so an empty answer to a non-empty request, or a short one of only None, is
    malformed (otherwise a responder could answer every request with nothing and be asked again forever).
    Blocks still have to be verified against their CIDs.
    """
    if type(data) is not list or len(data) > requested:
        return None
    served = False
    for entry in data:
        if entry is None:
            continue
        if type(entry) is not bytes or len(entry) > MAX_BLOCK_SIZE:
            return None
        served = True
    if len(data) < requested and not served:
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


# Largest honest responses, as RNS packs them, for the max_response_size requesters pass. RNS checks it
# against a response Resource's advertised size before accepting it, and these all fit one packet, so a node
# trying to send a /find, /provide or /pin answer as a slow Resource is refused at the advertisement.
REQUEST_ID_LENGTH = RNS.Identity.TRUNCATED_HASHLENGTH // 8
MAX_FIND_RESPONSE_SIZE = packed_response_size(bytes(REQUEST_ID_LENGTH),
                                              encode_find_response([bytes(ID_LENGTH)] * K, [bytes(ID_LENGTH)] * K))
MAX_ACCEPT_RESPONSE_SIZE = packed_response_size(bytes(REQUEST_ID_LENGTH), True)  # /provide, /pin: True or False


def _most_cids_per_request() -> int:
    """The most CIDs one /blocks request of at most MAX_REQUEST_SIZE can carry: all of the shortest kind (raw)."""
    shortest = Cid.for_block(RAW, b"")
    count = 1
    while fits(BLOCKS_PATH, encode_blocks(0, [shortest] * (count + 1)), MAX_REQUEST_SIZE):
        count += 1
    return count


# So also the most entries a /blocks response can have (26)
MAX_BLOCKS_CIDS = _most_cids_per_request()


# Packed responses, checked before RNS decodes them (section 3)
#
# RNS decodes a response with umsgpack before handing it over, and a payload of one-byte empty arrays or maps
# becomes some 70 times its size in Python objects. response_refusal reads only msgpack headers: it builds no
# objects, skips each bin by its length and reads at most a few dozen headers, so it costs the same for a
# response of any size. It accepts every response that one of rnfs.dht's parsers could accept.

_NIL, _FALSE, _TRUE = 0xC0, 0xC2, 0xC3
_BIN_WIDTHS = {0xC4: 1, 0xC5: 2, 0xC6: 4}  # bin8, bin16, bin32: bytes of length after the code
_ARRAY_WIDTHS = {0xDC: 2, 0xDD: 4}  # array16, array32: bytes of count after the code (fixarray: 0x90-0x9f)


def _array_at(packed: bytes, position: int) -> tuple[int, int] | None:
    """(element count, position after the header) of an array header at position, or None if there's none.

    An array claiming more elements than there are bytes left is refused: every element takes at least one.
    """
    if position >= len(packed):
        return None
    code = packed[position]
    if 0x90 <= code <= 0x9F:
        count, position = code & 0x0F, position + 1
    elif code in _ARRAY_WIDTHS:
        start = position + 1 + _ARRAY_WIDTHS[code]
        if start > len(packed):
            return None
        count, position = int.from_bytes(packed[position + 1:start], "big"), start
    else:
        return None
    return (count, position) if count <= len(packed) - position else None


def _bin_at(packed: bytes, position: int) -> tuple[int, int] | None:
    """(length, position after its data) of a bin at position, or None if there's none or it overruns."""
    if position >= len(packed) or packed[position] not in _BIN_WIDTHS:
        return None
    start = position + 1 + _BIN_WIDTHS[packed[position]]
    if start > len(packed):
        return None
    length = int.from_bytes(packed[position + 1:start], "big")
    return (length, start + length) if start + length <= len(packed) else None


def _find_answer_end(packed: bytes, position: int) -> int | None:
    """Position after a /find answer ([[ID, …], [ID, …]], each list at most K long) at position, or None."""
    outer = _array_at(packed, position)
    if outer is None or outer[0] != 2:
        return None
    position = outer[1]
    for _ in range(2):
        ids = _array_at(packed, position)
        if ids is None or ids[0] > K:
            return None
        position = ids[1]
        for _ in range(ids[0]):
            item = _bin_at(packed, position)
            if item is None or item[0] != ID_LENGTH:
                return None
            position = item[1]
    return position


def _entries_end(packed: bytes, position: int) -> tuple[int | None, str | None]:
    """(position after, None) for a /blocks answer at position: a list of at most MAX_BLOCKS_CIDS entries, each
    a bin of at most MAX_BLOCK_SIZE bytes or nil; else (None, why not)."""
    entries = _array_at(packed, position)
    if entries is None:
        return None, "not a list"
    count, position = entries
    if count > MAX_BLOCKS_CIDS:
        return None, f"{count} entries, more than a request can ask for"
    for _ in range(count):
        if position < len(packed) and packed[position] == _NIL:
            position += 1
            continue
        block = _bin_at(packed, position)
        if block is None:
            return None, "an entry that is neither a block nor None"
        if block[0] > MAX_BLOCK_SIZE:
            return None, f"a {block[0]}-byte entry, over MAX_BLOCK_SIZE"
        position = block[1]
    return position, None


def response_refusal(packed: bytes) -> str | None:
    """Why packed (RNS's response framing, [request_id, response], as received) can't hold any rnfs.dht
    response, judged from msgpack headers alone without decoding it; None if it may.

    The response may be any that body_refusal accepts. Nothing may follow it.
    """
    outer = _array_at(packed, 0)
    request_id = _bin_at(packed, outer[1]) if outer is not None and outer[0] == 2 else None
    if request_id is None or request_id[0] != REQUEST_ID_LENGTH or request_id[1] >= len(packed):
        return "not a response"
    return body_refusal(packed, request_id[1])


def body_refusal(packed: bytes, position: int = 0) -> str | None:
    """Why the packed response at position (to the end of packed) can't be any rnfs.dht response, from msgpack
    headers alone; None if it may.

    The response may be True, False or None (a /provide or /pin answer, or a refusal), a /find answer, or a
    /blocks answer (also the shape of a /name one). Nothing may follow it. Requesters check every response
    with this (through response_refusal), so a node checks its features' responses with it before sending.
    """
    if position >= len(packed):
        return "no response"
    if packed[position] in (_NIL, _FALSE, _TRUE):
        end = position + 1
    else:
        end = _find_answer_end(packed, position)
        if end is None:
            end, reason = _entries_end(packed, position)
            if end is None:
                return reason
    if end != len(packed):
        return f"{len(packed) - end} bytes after the response"
    return None


def packed_request_id(packed: bytes) -> bytes | None:
    """The request ID at the head of packed response framing, read without decoding the rest; None if absent."""
    outer = _array_at(packed, 0)
    request_id = _bin_at(packed, outer[1]) if outer is not None and outer[0] == 2 else None
    if request_id is None or request_id[0] != REQUEST_ID_LENGTH:
        return None
    return bytes(packed[request_id[1] - REQUEST_ID_LENGTH:request_id[1]])
