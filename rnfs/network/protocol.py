"""rnfs.dht wire format (specification section 3). Messages are RNS link requests, encoded with msgpack by RNS.

| Path      | Request                           | Response                       |
| /find     | key                               | [[provider IDs], [node IDs]]   |
| /provide  | [[key, ...], [withdrawn key, ...]]| True if recorded               |
| /blocks   | [max_bytes, [cid, ...]]           | [block or None, ...]           |
| /pin      | [cid, pin]                        | True if accepted               |
| /name     | name                              | the record's fields, or []     |
| /range    | [cid, offset, max_bytes, flags]   | [size, encoding, bytes], or [] |

Keys, node IDs and names are 16-byte bin, CIDs binary CIDs as bin, blocks bin, max_bytes a non-negative integer
(never a boolean), pin a boolean. Any other shape, or a refusal, is answered False.

/range (rnfs 0.2.0, optional) is a byte range of one block, its bytes bz2-compressed if flags asks (RANGE_BZ2) and
that makes them smaller; [] if the block isn't served (RNS sends nothing for a handler's None). Nodes before 0.2.0
have no handler for it, so RNS leaves it unanswered there: requesters learn which nodes answer it (fetcher, section
7). A requester decompresses an answer itself, to at most the range asked for plus one byte (decompress_range).

/name is the keepers feature's path (rnfs.features.keepers encodes and parses it). Its response is a kept
record's [public_key, name_hash, random_hash, ratchet, signature, app_data], every field bin, or [] if the node
keeps none: the shape of a /blocks answer, which is what lets response_refusal accept it.

Everything parsed here comes from other nodes, so parsers are strict and return None for anything malformed.
Every request must fit in one packet: at most min(link MDU, MAX_REQUEST_SIZE) bytes including RNS's request
framing, which is computed exactly here the way Link.request packs it.
"""
import bz2
import time

import RNS
from RNS.vendor import umsgpack

from rnfs.constants import (BLOCKS_PATH, FIND_PATH, ID_LENGTH, K, KIB, MAX_BLOCK_SIZE, MAX_PROVIDE_KEYS,
                            MAX_REQUEST_SIZE, MAX_RESPONSE_SIZE, PIN_PATH, PROVIDE_PATH, RANGE_BZ2, RANGE_PATH)
from rnfs.content.cid import RAW, Cid

__all__ = ["FIND_PATH", "PROVIDE_PATH", "BLOCKS_PATH", "PIN_PATH", "RANGE_PATH"]

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


# /range (rnfs 0.2.0, section 7)

RANGE_RAW = 0  # a /range answer's encoding: the bytes as they are (RANGE_BZ2: one bz2 stream of them)
BZ2_LEVEL = 9  # what RNS compresses Resources with (bz2.compress's default)


def encode_range(cid: Cid, offset: int, max_bytes: int, flags: int = 0) -> list:
    """/range request for at most max_bytes bytes of cid's block from offset, compressed if flags asks (RANGE_BZ2)."""
    return [cid.binary, int(offset), int(max_bytes), int(flags)]


def parse_range_request(data) -> tuple[Cid, int, int, int] | None:
    """(CID, offset, max_bytes, flags) from a /range request, or None if malformed: offset at most MAX_BLOCK_SIZE,
    max_bytes 1 to MAX_BLOCK_SIZE, flags any non-negative integer (its reserved bits are ignored, not refused, so a
    later flag can be added without older responders refusing it)."""
    if type(data) is not list or len(data) != 4:
        return None
    binary, offset, max_bytes, flags = data
    cid = parse_cid(binary)
    if cid is None or not is_count(offset) or not is_count(max_bytes) or not is_count(flags):
        return None
    if offset > MAX_BLOCK_SIZE or not 1 <= max_bytes <= MAX_BLOCK_SIZE:
        return None
    return cid, offset, max_bytes, flags


def range_length(size: int, offset: int, max_bytes: int) -> int:
    """How many bytes a /range answer carries: max_bytes, or what is left of a block of size bytes after offset."""
    return max(0, min(max_bytes, size - offset))


def encode_range_response(size: int, encoding: int, data: bytes) -> list:
    """/range answer: the block's size, the encoding of data (RANGE_RAW or RANGE_BZ2) and data."""
    return [int(size), int(encoding), bytes(data)]


def range_max_response_size(max_bytes: int) -> int:
    """The max_response_size a requester passes to RNS for a /range request (section 7): the range, its framing."""
    return min(max_bytes + KIB, MAX_RESPONSE_SIZE)


def compress_range(data: bytes) -> bytes:
    """data as one bz2 stream, as a responder sends a range asked for compressed."""
    return bz2.compress(data, BZ2_LEVEL)


def decompress_range(stream: bytes, limit: int) -> bytes | None:
    """What the bz2 stream holds, if it is one complete stream of at most limit bytes with nothing after it; else
    None (also for a corrupt one).

    The decompressor is asked for at most limit + 1 bytes, so a stream expanding to far more (a decompression bomb:
    64 MiB of zeros is about 50 bytes of bz2) costs that much output and is refused there, at its first call.
    """
    decompressor = bz2.BZ2Decompressor()
    try:
        data = decompressor.decompress(stream, max_length=limit + 1)
    except (OSError, ValueError, EOFError):
        return None
    if len(data) > limit or not decompressor.eof or decompressor.unused_data:
        return None
    return data


def is_range_unserved(data) -> bool:
    """True if a /range answer says the node doesn't hold or serve the block: an empty list."""
    return type(data) is list and not data


def parse_range_response(data, offset: int, max_bytes: int, compressed: bool,
                         size: int | None = None) -> tuple[int, bytes] | None:
    """(the block's size, the range's bytes) from a /range answer to a request for max_bytes from offset, or None if
    malformed (including False, a refusal, and [], unserved: is_range_unserved tells those apart first).

    The size must be at most MAX_BLOCK_SIZE and, if given (what the parent declares, or earlier ranges said), equal to
    it; the encoding RANGE_RAW, or RANGE_BZ2 if compressed (the request asked for it); and the bytes, decompressed
    to at most their expected length (decompress_range), exactly range_length(size, offset, max_bytes) of them.
    """
    if type(data) is not list or len(data) != 3:
        return None
    total, encoding, payload = data
    if not is_count(total) or not is_count(encoding) or type(payload) is not bytes:
        return None
    if total > MAX_BLOCK_SIZE or (size is not None and total != size) or offset > total:
        return None
    expected = range_length(total, offset, max_bytes)
    if encoding == RANGE_RAW:
        raw = payload
    elif encoding == RANGE_BZ2 and compressed:
        raw = decompress_range(payload, expected)
    else:
        return None
    if raw is None or len(raw) != expected:
        return None
    return total, raw


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


_UINT_WIDTHS = {0xCC: 1, 0xCD: 2, 0xCE: 4, 0xCF: 8}  # uint8 to uint64: bytes after the code (fixint: 0x00-0x7f)


def _uint_end(packed: bytes, position: int) -> int | None:
    """Position after a non-negative integer at position, or None if there's none."""
    if position >= len(packed):
        return None
    code = packed[position]
    if code <= 0x7F:
        return position + 1
    width = _UINT_WIDTHS.get(code)
    if width is None or position + 1 + width > len(packed):
        return None
    return position + 1 + width


def _range_answer_end(packed: bytes, position: int) -> int | None:
    """Position after a /range answer ([size, encoding, data]: two non-negative integers and a bin of at most
    MAX_BLOCK_SIZE) at position, or None."""
    outer = _array_at(packed, position)
    if outer is None or outer[0] != 3:
        return None
    position = outer[1]
    for _ in range(2):
        position = _uint_end(packed, position)
        if position is None:
            return None
    data = _bin_at(packed, position)
    if data is None or data[0] > MAX_BLOCK_SIZE:
        return None
    return data[1]


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

    The response may be True, False or None (a /provide or /pin answer, or a refusal), a /find answer, a /blocks
    answer (also the shape of a /name one, and of an empty /range answer), or a /range answer. Nothing may follow it.
    Requesters check every response with this (through response_refusal), so a node checks its features' responses
    with it before sending.
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
                end = _range_answer_end(packed, position)
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


# NomadNet answers (rnfs/network/nomadnet.py), checked the same way on the links rnfs opens to NomadNet nodes
#
# A page is answered as [request_id, page], the page one flat byte string (NomadNet sends bytes); nothing, or nil or
# False, when there is none. A file comes as a Resource with metadata, which RNS hands over undecoded (an open
# file) after decoding the metadata, which is checked on its own first (metadata_refusal).

_STR_WIDTHS = {0xD9: 1, 0xDA: 2, 0xDB: 4}  # str8, str16, str32: bytes of length after the code (fixstr: 0xa0-0xbf)
_INT_WIDTHS = {0xCC: 1, 0xCD: 2, 0xCE: 4, 0xCF: 8, 0xD0: 1, 0xD1: 2, 0xD2: 4, 0xD3: 8}  # uint8-64, int8-64
PAGE_HEAD_SIZE = 32  # bytes of a page answer page_response_refusal needs: its framing and the page's header
MAX_METADATA_SIZE = 4 * KIB  # a NomadNet file's metadata, {"name": <file name>}, is a few hundred bytes at most
MAX_METADATA_ENTRIES = 16


def _string_at(packed: bytes, position: int, length: int | None = None) -> tuple[int, int] | None:
    """(length, position after its data) of a bin or str at position, or None if there's none or it would end
    past length (the whole answer's size, of which packed may hold only the start; default len(packed))."""
    length = len(packed) if length is None else length
    if position >= len(packed):
        return None
    code = packed[position]
    if 0xA0 <= code <= 0xBF:
        size, start = code & 0x1F, position + 1
    else:
        width = _BIN_WIDTHS.get(code) or _STR_WIDTHS.get(code)
        start = position + 1 + (width or 0)
        if width is None or start > len(packed):
            return None
        size = int.from_bytes(packed[position + 1:start], "big")
    return (size, start + size) if start + size <= length else None


def page_response_refusal(head: bytes, length: int) -> str | None:
    """Why a NomadNet answer of length bytes, whose first bytes are head (at least PAGE_HEAD_SIZE of them, or all),
    can't be a page answer, judged from msgpack headers alone; None if it may be.

    It must be [request_id, page]: a 16-byte bin request ID, then the page as one bin or str, or nil or False
    (none), with nothing after it. So RNS decodes it into one byte string at most, never a structure.
    """
    outer = _array_at(head, 0)
    request_id = _bin_at(head, outer[1]) if outer is not None and outer[0] == 2 else None
    if request_id is None or request_id[0] != REQUEST_ID_LENGTH or request_id[1] >= length:
        return "not a response"
    position = request_id[1]
    if position < len(head) and head[position] in (_NIL, _FALSE):
        end = position + 1
    else:
        page = _string_at(head, position, length)
        if page is None:
            return "not a page: a NomadNet page is one byte string"
        end = page[1]
    if end != length:
        return f"{length - end} bytes after the page"
    return None


def metadata_refusal(packed: bytes) -> str | None:
    """Why packed (a Resource's metadata as RNS stores it, before decoding it) isn't what a NomadNet file carries,
    from msgpack headers alone; None if it may be: a map of at most MAX_METADATA_ENTRIES entries whose keys are
    bin or str and whose values are bin, str, nil, booleans or integers, at most MAX_METADATA_SIZE bytes in all.
    Nothing nested, so RNS's decoding of it can't recurse or build more than a few objects."""
    if len(packed) > MAX_METADATA_SIZE:
        return f"{len(packed)} bytes of metadata, over {MAX_METADATA_SIZE}"
    if not packed:
        return "empty metadata"
    if 0x80 <= packed[0] <= 0x8F:
        count, position = packed[0] & 0x0F, 1
    elif packed[0] == 0xDE and len(packed) >= 3:
        count, position = int.from_bytes(packed[1:3], "big"), 3
    else:
        return "metadata that isn't a map"
    if count > MAX_METADATA_ENTRIES:
        return f"metadata of {count} entries, over {MAX_METADATA_ENTRIES}"
    for index in range(2 * count):
        position = _scalar_end(packed, position, key=index % 2 == 0)
        if position is None:
            return "metadata holding something other than names, numbers and flags"
    if position != len(packed):
        return f"{len(packed) - position} bytes after the metadata"
    return None


def _scalar_end(packed: bytes, position: int, key: bool) -> int | None:
    """Position after the bin or str (and unless key: nil, boolean or integer) at position, or None."""
    string = _string_at(packed, position)
    if string is not None:
        return string[1]
    if key or position >= len(packed):
        return None
    code = packed[position]
    if code in (_NIL, _FALSE, _TRUE) or code <= 0x7F or code >= 0xE0:  # nil, booleans, positive and negative fixints
        return position + 1
    width = _INT_WIDTHS.get(code)
    if width is None or position + 1 + width > len(packed):
        return None
    return position + 1 + width
