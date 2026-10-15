"""LXMF integration: rnfs links sent and received as LXMF messages (Sideband, NomadNet, MeshChat, lxmd).

The node gets an LXMF delivery identity of its own (<rnfs dir>/storage/lxmf_identity, separate from the
rnfs.dht identity, so the two can't be linked by key), announced with a display name at startup, after
startup_announce_delay and every announce_interval. LXMF keeps its router state in <rnfs dir>/lxmf/.

- Links: `rnfs:<cid>[/path][?from=<node ID>]` and `rnfs:name/<name>[/path][?from=<node ID>]`, path names
  percent-encoded as UTF-8 (parse_uri / format_uri). Messages carry them in their text, for people and
  clients that know nothing of rnfs, and in LXMF's custom fields (FIELD_CUSTOM_TYPE "rnfs",
  FIELD_CUSTOM_DATA a list of link maps) for rnfs-aware clients (encode_field / decode_field).
- Sending (`rnfs send`): direct delivery over a link by default; propagated through a propagation node with
  --propagate, or automatically when direct delivery fails and lxmf_propagation_node is set. A propagation
  node only announcing itself is used only when the user asks (--nearest): it could be anyone's. The outbox
  records each message's state.
- Receiving: messages with rnfs links go to a bounded inbox (state/lxmf_inbox.json). Links from senders in
  lxmf_auto_fetch_from are fetched and pinned with source `lxmf:<sender>`, within lxmf_auto_fetch_quota per
  sender; removing the message from the inbox releases those pins, and a message holding pins is never
  dropped to make room for others. lxmf_accept_from restricts senders.
- Messages waiting at lxmf_propagation_node are downloaded every lxmf_sync_interval.

Everything received is untrusted: fields are parsed strictly and capped, text is capped and escaped when
printed. A message is only accepted once LXMF has validated its signature against the sender's identity; one
from a sender whose identity isn't known yet is held while a path request fetches it.

LXMF is optional: without the package (or with lxmf_enabled = false) the feature stays off and says so once.
Nothing here imports RNS or LXMF at module level (CLI commands mustn't load them); LxmfNetwork, made in
start(), holds every RNS and LXMF object, so the rest works on offline test nodes with a stand-in network.
"""
import json
import math
import os
import re
import signal
import sys
import threading
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from urllib.parse import quote, unquote_to_bytes

from rnfs import features, fsutil
from rnfs.features import _common
from rnfs.features._common import LOG_ERROR, LOG_INFO, LOG_NOTICE, LOG_VERBOSE, LOG_WARNING
from rnfs.constants import ID_LENGTH, MAX_DEPTH
from rnfs.content.cid import Cid

NAME = "lxmf"

# LXMF's fields for embedding other protocols (LXMF/LXMF.py): a type identifier and its payload. LXMF reserves
# the unallocated fields below 0x80 for its own future use and calls those above 0xFF unstable
FIELD_CUSTOM_TYPE = 0xFB
FIELD_CUSTOM_DATA = 0xFC
CUSTOM_TYPE = "rnfs"  # FIELD_CUSTOM_TYPE value; an incompatible change of FIELD_CUSTOM_DATA uses a new one

URI_SCHEME = "rnfs:"
NAME_SEGMENT = "name"
MAX_URI_LENGTH = 4096  # characters: 64 names of up to 255 bytes, percent-encoded, don't fit in a message anyway
MAX_PATH_NAMES = MAX_DEPTH  # a path can't be deeper than the DAG it walks
MAX_LINKS = 8  # links read from one message (field or text)
MAX_LINK_KEYS = 16  # keys in one link map, known or not
MAX_TEXT_CHARS = 4000  # message text kept (and accepted for sending)
MAX_TITLE_CHARS = 256
MAX_DISPLAY_NAME_CHARS = 64
MAX_SCAN_CHARS = 16 * 1024  # message text searched for links
INBOX_LIMIT = 500  # messages kept; the oldest holding no auto-fetched pins is dropped
INBOX_PER_SENDER = 50  # messages kept from one sender not in lxmf_auto_fetch_from (senders' identities are free)
# Senders the user named (lxmf_accept_from, lxmf_auto_fetch_from, LXMF contacts) are trusted: their records are
# dropped only for each other's. Others' new records come from a node-wide token bucket, since identities are free
UNTRUSTED_INTERVAL = 60.0  # seconds per token
UNTRUSTED_BURST = 32
MAX_ENTRY_BYTES = 16 * 1024  # an inbox record as JSON: links beyond it are dropped, then text is cut
MAX_INBOX_BYTES = 4 * 1024 * 1024  # every inbox record as JSON (held in memory, rewritten on change)
OUTBOX_LIMIT = 200
AUTO_QUEUE_LIMIT = 64  # auto-fetches waiting (taken round-robin by sender); more are skipped
DELIVERY_LIMIT_KB = 256  # largest LXMF message accepted (LXMF's delivery_limit, in kB); also caps PN downloads
HOLD_LIMIT = 32  # messages from senders not known yet, held while their identity is requested; more are dropped
HOLD_PER_SENDER = 8  # of them claiming one sender (the claim is unauthenticated until the identity is known)
HOLD_TIME = 120.0  # seconds a held message waits for its sender's identity
HOLD_PATH_WAIT = 30.0  # seconds after its path request (or its arrival, if later) a held message is dropped unanswered
# Path requests for held messages' senders: their source is unauthenticated until the identity is known, so
# anyone can claim any. One per sender per HOLD_TIME, and node-wide a token bucket (each one floods all
# interfaces)
PATH_REQUEST_INTERVAL = 10.0  # seconds per token
PATH_REQUEST_BURST = 3
SYNC_MAX_MESSAGES = 64  # messages asked of the propagation node per download (the rest wait for the next)
# Largest Resource accepted on the propagation node link: LXMF asks for at most DELIVERY_LIMIT_KB per
# download, so a node sending more (or a multi-segment Resource) isn't playing by the rules
MAX_PN_RESOURCE = 4 * DELIVERY_LIMIT_KB * 1000
DEFAULT_DISPLAY_NAME = "rnfs node"  # nothing in it links the LXMF address to the node ID
PROPAGATION_NODES_LIMIT = 64  # propagation nodes remembered from announces
PROPAGATION_NODE_AGE = 7 * 24 * 3600.0  # forget a propagation node not heard this long
IDENTITY_WAIT = 15.0  # seconds `send` waits for an unknown recipient's identity (capped by path_timeout)
TICK = 1.0  # seconds between polls of outbound message states
SYNC_TIMEOUT = 900.0  # a propagation node download still unfinished after this is abandoned

_HEX32 = re.compile(r"[0-9a-fA-F]{32}")
_SEGMENT = re.compile(r"(?:[A-Za-z0-9\-._~!$&'()*+,;=:@]|%[0-9A-Fa-f]{2})+")
_QUERY_VALUE = re.compile(r"(?:[A-Za-z0-9\-._~!$'()*+,;=:@/]|%[0-9A-Fa-f]{2})*")  # "&" separates parameters
# An rnfs: link in free text: not glued to a preceding word, ending at whitespace or a character URIs never hold
_TEXT_LINK = re.compile(r"(?<![A-Za-z0-9+.\-])[Rr][Nn][Ff][Ss]:[^\s<>\"'`{}|\\^\[\]]+")
_TRAILING = ".,;:!?)]}'\""

STATE_NAMES = {0x00: "generating", 0x01: "outbound", 0x02: "sending", 0x04: "sent", 0x08: "delivered",
               0xFD: "rejected", 0xFE: "cancelled", 0xFF: "failed"}
FINAL_STATES = {"delivered", "rejected", "cancelled", "failed"}  # and "sent" for a propagated message
METHOD_NAMES = {0x01: "opportunistic", 0x02: "direct", 0x03: "propagated", 0x05: "paper"}


def _log(message: str, level: int = LOG_INFO):
    """Log with this feature's prefix."""
    _common.log(f"lxmf: {message}", level)


_NO_PN_HELP = ("no LXMF propagation node is set: set lxmf_propagation_node, or use --nearest for the nearest "
               "one heard")


def _check_name(name: str):
    """dag.check_name, imported here because rnfs.content.dag loads RNS's msgpack."""
    from rnfs.content import dag
    dag.check_name(name)


def parse_address(text) -> bytes:
    """An LXMF address (destination hash) from 32 hex characters, with or without <angle brackets>."""
    if not isinstance(text, str) or not _HEX32.fullmatch(text.strip().strip("<>")):
        raise ValueError(f"invalid LXMF address {text!r}: must be 32 hex characters")
    return bytes.fromhex(text.strip().strip("<>"))


def validate_settings(config):
    """Raise ValueError for invalid lxmf_* settings (called by Config.validate)."""
    for name in ("lxmf_accept_from", "lxmf_auto_fetch_from"):
        values = getattr(config, name)
        if not isinstance(values, list):
            raise ValueError(f"{name} must be a list of LXMF addresses")
        for value in values:
            parse_address(value)
    if config.lxmf_propagation_node is not None:
        parse_address(config.lxmf_propagation_node)
    if config.lxmf_enabled is not None and type(config.lxmf_enabled) is not bool:  # 0 == False, yet not False
        raise ValueError("lxmf_enabled must be true or false")
    if type(config.lxmf_send_from_hint) is not bool:
        raise ValueError("lxmf_send_from_hint must be true or false")
    name = config.lxmf_display_name
    if name is not None and (not isinstance(name, str) or not 1 <= len(name) <= MAX_DISPLAY_NAME_CHARS
                             or not name.isprintable()):
        raise ValueError(f"lxmf_display_name must be 1-{MAX_DISPLAY_NAME_CHARS} printable characters")
    quota = config.lxmf_auto_fetch_quota
    if isinstance(quota, bool) or not isinstance(quota, int) or quota < 0:
        raise ValueError("lxmf_auto_fetch_quota must be a non-negative number of bytes")
    interval = config.lxmf_sync_interval
    if isinstance(interval, bool) or not isinstance(interval, (int, float)) or not 0 <= interval < math.inf:
        raise ValueError("lxmf_sync_interval must be a non-negative number of seconds")


def lxmf_identity_path(config) -> str:
    """Where the node's LXMF identity is kept: <rnfs dir>/storage/lxmf_identity."""
    return os.path.join(os.path.dirname(config.identity_path), "lxmf_identity")


def human_size(num_bytes: int) -> str:
    """A size for people, e.g. "12.3 MB" (rnfs.log.size needs RNS, which client commands don't load)."""
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1000 or unit == "TB":
            return f"{int(value)} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1000
    return f"{num_bytes} B"


# Links


@dataclass(frozen=True)
class Link:
    """One rnfs link: a CID or a name, a path below it, and optionally its size and a node that has it."""

    cid: Cid | None = None  # with a name: the CID it pointed at when sent
    name: bytes | None = None  # 16-byte rnfs name
    path: tuple[str, ...] = ()
    size: int | None = None  # content bytes, as the sender knew them (untrusted; fetches verify sizes)
    from_node: bytes | None = None  # the sender's rnfs node ID: a provider hint, like `get --from`

    def target(self) -> str:
        """`<cid>[/path]` for the node's facade; ValueError for a name link (resolve it first)."""
        if self.cid is None:
            raise ValueError("a name link has no CID until its name is resolved")
        return "/".join([str(self.cid), *self.path])

    def uri(self) -> str:
        """The canonical rnfs: URI."""
        return format_uri(self)

    def to_json(self) -> dict:
        """JSON-ready form (inbox, outbox and API)."""
        return {"uri": format_uri(self), "cid": str(self.cid) if self.cid is not None else None,
                "name": self.name.hex() if self.name is not None else None, "path": list(self.path),
                "size": self.size, "from": self.from_node.hex() if self.from_node is not None else None}

    @classmethod
    def from_json(cls, value) -> "Link":
        """A link from to_json's form; ValueError if it is malformed (e.g. a damaged state file)."""
        if not isinstance(value, dict):
            raise ValueError("a link must be an object")
        link = parse_uri(value.get("uri"))
        cid = Cid.parse(value["cid"]) if value.get("cid") is not None else link.cid
        size = value.get("size")
        if size is not None and (isinstance(size, bool) or not isinstance(size, int) or not 0 <= size < 2 ** 64):
            raise ValueError("invalid size")
        return Link(cid, link.name, link.path, size, link.from_node)


def format_uri(link: Link) -> str:
    """`rnfs:<cid>[/path]` or `rnfs:name/<name>[/path]`, then `?from=<node ID>` if known.

    Path names are UTF-8 with every byte other than ASCII letters, digits and "-._~" percent-encoded
    (upper-case hex), so each link has one written form.
    """
    if link.name is not None:
        head = f"{NAME_SEGMENT}/{link.name.hex()}"
    elif link.cid is not None:
        head = str(link.cid)
    else:
        raise ValueError("a link needs a CID or a name")
    path = "".join("/" + quote(name.encode("utf-8"), safe="") for name in link.path)
    query = f"?from={link.from_node.hex()}" if link.from_node is not None else ""
    return URI_SCHEME + head + path + query


def parse_uri(text) -> Link:
    """The link an rnfs: URI names. Raises ValueError for anything malformed.

    The scheme and hex digits may be in either case; one trailing slash is allowed; path names must decode
    to valid entry names (section 5); query parameters other than `from` are ignored, a repeated `from` is
    malformed. Fragments, spaces, non-ASCII characters and empty path names are malformed.
    """
    if not isinstance(text, str) or not text or len(text) > MAX_URI_LENGTH:
        raise ValueError("an rnfs: URI is required (at most 4096 characters)")
    if text[:len(URI_SCHEME)].lower() != URI_SCHEME:
        raise ValueError(f"not an rnfs: URI: {text!r}")
    rest = text[len(URI_SCHEME):]
    if not rest.isascii() or not rest.isprintable() or " " in rest or "#" in rest:
        raise ValueError("an rnfs: URI may only hold printable ASCII, without spaces or a #fragment")
    path_part, has_query, query = rest.partition("?")
    segments = path_part.split("/")
    if len(segments) > 1 and segments[-1] == "":
        segments.pop()  # "<cid>/dir/" names the same directory as "<cid>/dir"
    cid = name = None
    if segments[0] == NAME_SEGMENT:
        if len(segments) < 2 or not _HEX32.fullmatch(segments[1]):
            raise ValueError("rnfs:name/ must be followed by a name: 32 hex characters")
        name = bytes.fromhex(segments[1])
        segments = segments[2:]
    else:
        cid = Cid.parse(segments[0])
        segments = segments[1:]
    if len(segments) > MAX_PATH_NAMES:
        raise ValueError(f"a path has at most {MAX_PATH_NAMES} names")
    names = []
    for segment in segments:
        if not _SEGMENT.fullmatch(segment):
            raise ValueError(f"malformed path name {segment!r} (empty, or a character that must be %-encoded)")
        try:
            decoded = unquote_to_bytes(segment).decode("utf-8")
        except UnicodeDecodeError:
            raise ValueError(f"path name {segment!r} is not UTF-8") from None
        _check_name(decoded)
        names.append(decoded)
    from_node = None
    if has_query:
        if not query:
            raise ValueError("empty query after ?")
        for pair in query.split("&"):
            key, has_value, value = pair.partition("=")
            if not key or not has_value or not _SEGMENT.fullmatch(key) or not _QUERY_VALUE.fullmatch(value):
                raise ValueError(f"malformed query parameter {pair!r}")
            if key == "from":
                if from_node is not None:
                    raise ValueError("from is given twice")
                if not _HEX32.fullmatch(value):
                    raise ValueError("from must be a node ID: 32 hex characters")
                from_node = bytes.fromhex(value)
    return Link(cid, name, tuple(names), None, from_node)


def parse_target(text) -> Link:
    """What `send` accepts: an rnfs: URI, `name/<name>[/path]` or `<cid>[/path]`. Raises ValueError."""
    if not isinstance(text, str) or not text:
        raise ValueError("a target is required: <cid>[/path], name/<name>[/path] or an rnfs: URI")
    if text[:len(URI_SCHEME)].lower() == URI_SCHEME:
        return parse_uri(text)
    if "?" in text or "#" in text or "%" in text:
        raise ValueError("use an rnfs: URI for a target with ?, # or %")
    return parse_uri(URI_SCHEME + "/".join(quote(part.encode("utf-8"), safe="") for part in text.split("/")))


def find_links(text) -> list[Link]:
    """The distinct rnfs: links in a message's text, in order (at most MAX_LINKS); malformed ones are skipped.

    A link filling a line of its own (as message_text writes them) is read whole; elsewhere trailing
    punctuation (".,;:!?)]}'\\"") is taken as the sentence's, not the link's.
    """
    if not isinstance(text, str):
        return []
    text = text[:MAX_SCAN_CHARS]
    found: list[Link] = []
    for match in _TEXT_LINK.finditer(text):
        line_start = text.rfind("\n", 0, match.start()) + 1
        line_end = text.find("\n", match.end())
        candidates = [match.group(0).rstrip(_TRAILING)]
        if not text[line_start:match.start()].strip():
            candidates.insert(0, text[match.start():len(text) if line_end < 0 else line_end].strip())
        link = None
        for candidate in candidates:
            try:
                link = parse_uri(candidate)
                break
            except ValueError:
                continue
        if link is None:
            continue
        if link not in found:
            found.append(link)
            if len(found) == MAX_LINKS:
                break
    return found


def encode_field(links: list[Link]) -> dict:
    """The LXMF fields carrying links: {FIELD_CUSTOM_TYPE: "rnfs", FIELD_CUSTOM_DATA: [link map, ...]}.

    A link map has "cid" (binary CID) and/or "name" (16 bytes), "path" (list of names) if not empty, and
    "size" (integer) and "from" (16-byte node ID) when known.
    """
    if not 1 <= len(links) <= MAX_LINKS:
        raise ValueError(f"a message carries 1-{MAX_LINKS} links")
    data = []
    for link in links:
        entry: dict = {}
        if link.cid is not None:
            entry["cid"] = link.cid.binary
        if link.name is not None:
            entry["name"] = link.name
        if not entry:
            raise ValueError("a link needs a CID or a name")
        if link.path:
            entry["path"] = list(link.path)
        if link.size is not None:
            entry["size"] = link.size
        if link.from_node is not None:
            entry["from"] = link.from_node
        data.append(entry)
    return {FIELD_CUSTOM_TYPE: CUSTOM_TYPE, FIELD_CUSTOM_DATA: data}


def _id_field(value, what: str) -> bytes:
    """A 16-byte binary field of a link map."""
    if type(value) is not bytes or len(value) != ID_LENGTH:
        raise ValueError(f"{what} must be {ID_LENGTH} bytes")
    return value


def decode_field(fields) -> list[Link] | None:
    """The links in a received message's fields; None if it has no rnfs field. ValueError if it is malformed.

    Strict: exact types (bin, str, non-negative integers that aren't booleans), sizes below 2^64, valid CIDs
    and names, at most MAX_LINKS links of at most MAX_LINK_KEYS keys. Unknown keys are ignored, so later
    versions can add some.
    """
    if not isinstance(fields, dict) or fields.get(FIELD_CUSTOM_TYPE) != CUSTOM_TYPE:
        return None
    data = fields.get(FIELD_CUSTOM_DATA)
    if type(data) is not list or not 1 <= len(data) <= MAX_LINKS:
        raise ValueError(f"the rnfs field must be a list of 1-{MAX_LINKS} links")
    links = []
    for entry in data:
        if type(entry) is not dict or len(entry) > MAX_LINK_KEYS or not all(type(k) is str for k in entry):
            raise ValueError("a link must be a map with string keys")
        cid = name = size = from_node = None
        if "cid" in entry:
            if type(entry["cid"]) is not bytes:
                raise ValueError("cid must be binary")
            cid = Cid.from_bytes(entry["cid"])
        if "name" in entry:
            name = _id_field(entry["name"], "name")
        if cid is None and name is None:
            raise ValueError("a link needs a cid or a name")
        path = entry.get("path", [])
        if type(path) is not list or len(path) > MAX_PATH_NAMES or not all(type(n) is str for n in path):
            raise ValueError(f"path must be a list of at most {MAX_PATH_NAMES} names")
        for item in path:
            _check_name(item)
        if "size" in entry:
            size = entry["size"]
            if type(size) is not int or not 0 <= size < 2 ** 64:
                raise ValueError("size must be a non-negative integer below 2^64")
        if "from" in entry:
            from_node = _id_field(entry["from"], "from")
        link = Link(cid, name, tuple(path), size, from_node)
        if len(format_uri(link)) > MAX_URI_LENGTH:  # parse_uri, and so the inbox, refuses it anyway
            raise ValueError(f"a link is longer than {MAX_URI_LENGTH} characters as a URI")
        links.append(link)
    return links


def message_text(text: str | None, links: list[Link]) -> str:
    """A message's content: the user's text, then each link's URI on a line of its own."""
    lines = [link.uri() for link in links]
    return (text.strip() + "\n\n" if text and text.strip() else "") + "\n".join(lines)


# Inbox and outbox


def _json_bytes(value) -> int:
    """Bytes value takes as UTF-8 JSON, as MessageLog writes it."""
    return len(json.dumps(value, ensure_ascii=False).encode("utf-8", "backslashreplace"))


def _write_json(path: str, value):
    """Write value as UTF-8 JSON (non-ASCII as is, not as 6-byte escapes) to path atomically and durably: a
    power loss leaves the old or the new file whole (an emptied inbox would leave lxmf: pins nothing releases)."""
    from rnfs.content.storage import write_json
    write_json(path, value, ensure_ascii=False)


class MessageLog:
    """A bounded list of message records with stable ids, saved atomically as UTF-8 JSON. Thread-safe.

    At most limit records and max_bytes of JSON are kept. To make room, the oldest record protect() is false
    for is dropped; a protected one only when every other is protected too. add() may also name records that
    are dropped only for each other's (reserved). valid() checks records loaded from the file (malformed ones
    are dropped; clean says whether the file was there and every record in it was read).
    """

    def __init__(self, path: str, limit: int, max_bytes: int | None = None, protect=None, valid=None):
        """Load path if it exists (malformed records are dropped)."""
        self.path = path
        self.limit = limit
        self.max_bytes = max_bytes
        self.protect = protect or (lambda entry: False)
        self._lock = threading.Lock()
        self._save_lock = threading.Lock()  # one save at a time, so an older snapshot never lands last
        self._dirty = False
        self._next_id = 1
        self._entries: list[dict] = []
        self._sizes: dict[int, int] = {}  # id -> bytes as JSON
        from rnfs.content.storage import read_json
        stored = read_json(path)  # a damaged file is moved aside and gives None, as a missing one does
        self.clean = False  # the file was there and every record in it was read
        if isinstance(stored, dict):
            entries = stored.get("messages")
            bad, seen = 0 if isinstance(entries, list) else 1, set()
            for entry in entries if isinstance(entries, list) else []:
                if isinstance(entry, dict) and type(entry.get("id")) is int and entry["id"] > 0 \
                        and entry["id"] not in seen and (valid is None or valid(entry)):
                    self._entries.append(entry)
                    seen.add(entry["id"])
                else:
                    bad += 1
            if bad:
                _log(f"dropped {bad} malformed record(s) from {path}", LOG_WARNING)
            self.clean = not bad
            self._entries = self._entries[-limit:]
            self._sizes = {e["id"]: _json_bytes(e) for e in self._entries}
            while self.max_bytes is not None and len(self._entries) > 1 and sum(self._sizes.values()) > max_bytes:
                self._sizes.pop(self._entries.pop(0)["id"], None)
            next_id = stored.get("next_id")
            self._next_id = max([next_id if type(next_id) is int else 1] + [e["id"] + 1 for e in self._entries])

    def add(self, entry: dict, per_from: int | None = None, reserved=None) -> tuple[int | None, list[dict]]:
        """Append entry with a new id; returns the id (None if entry itself was dropped: every other record is
        protected and it isn't) and the entries dropped to stay within the bounds. per_from: also keep at most
        that many unprotected records with entry's "from". reserved(record): records it is true for are dropped
        to make room only for an entry it is true for too, and only once no other unprotected record is left
        (trusted senders' records, so new identities can't push them out)."""
        reserved = reserved or (lambda record: False)
        with self._lock:
            entry["id"] = self._next_id
            self._next_id += 1
            self._entries.append(entry)
            self._sizes[entry["id"]] = _json_bytes(entry)
            dropped: list[dict] = []
            if per_from is not None:
                same = [e for e in self._entries if e.get("from") == entry.get("from") and not self.protect(e)]
                dropped += same[:max(0, len(same) - per_from)]
            entry_reserved = reserved(entry)
            while self._over(dropped):
                gone = {id(e) for e in dropped}
                others = [e for e in self._entries if id(e) not in gone and e is not entry and not self.protect(e)]
                victim = next((e for e in others if not reserved(e)), None)
                if victim is None and entry_reserved and others:
                    victim = others[0]
                if victim is None:
                    # Every other record holds pins (or is reserved, and entry isn't): refuse entry rather than
                    # drop them, unless it holds pins too (then the oldest goes; only a trusted sender gets here)
                    victim = entry if not self.protect(entry) or len(self._entries) - len(dropped) == 1 else \
                        next(e for e in self._entries if id(e) not in gone and e is not entry)
                dropped.append(victim)
                if victim is entry:
                    break
            gone = {id(e) for e in dropped}
            self._entries = [e for e in self._entries if id(e) not in gone]
            for e in dropped:
                self._sizes.pop(e["id"], None)
            self._dirty = True
            return (None if id(entry) in gone else entry["id"]), [e for e in dropped if e is not entry]

    def _over(self, dropped: list[dict]) -> bool:
        """Whether the records left after dropping these exceed the limit or max_bytes (lock held)."""
        if len(self._entries) - len(dropped) > self.limit:
            return True
        if self.max_bytes is None:
            return False
        return sum(self._sizes.values()) - sum(self._sizes.get(e["id"], 0) for e in dropped) > self.max_bytes

    def get(self, entry_id: int) -> dict | None:
        """A copy of the entry with that id, or None."""
        with self._lock:
            for entry in self._entries:
                if entry["id"] == entry_id:
                    return dict(entry)
        return None

    def find(self, key: str, value) -> dict | None:
        """A copy of the newest entry whose key equals value, or None."""
        with self._lock:
            for entry in reversed(self._entries):
                if entry.get(key) == value:
                    return dict(entry)
        return None

    def count(self, key: str, value) -> int:
        """Number of entries whose key equals value."""
        with self._lock:
            return sum(1 for entry in self._entries if entry.get(key) == value)

    def update(self, entry_id: int, **values) -> bool:
        """Set fields of an entry; False if it no longer exists."""
        with self._lock:
            for entry in self._entries:
                if entry["id"] == entry_id:
                    entry.update(values)
                    self._sizes[entry_id] = _json_bytes(entry)
                    self._dirty = True
                    return True
        return False

    def remove(self, entry_id: int | None) -> list[dict]:
        """Remove the entry with that id (every entry for None); returns what was removed."""
        with self._lock:
            removed = [e for e in self._entries if entry_id is None or e["id"] == entry_id]
            if removed:
                gone = {id(e) for e in removed}
                self._entries = [e for e in self._entries if id(e) not in gone]
                for e in removed:
                    self._sizes.pop(e["id"], None)
                self._dirty = True
            return removed

    def entries(self) -> list[dict]:
        """Copies of every entry, oldest first."""
        with self._lock:
            return [dict(entry) for entry in self._entries]

    def __len__(self) -> int:
        """Number of entries."""
        with self._lock:
            return len(self._entries)

    def save(self):
        """Write the log if it changed (atomic; raises OSError)."""
        with self._save_lock:
            with self._lock:
                if not self._dirty:
                    return
                snapshot = {"next_id": self._next_id, "messages": [dict(e) for e in self._entries]}
                self._dirty = False
            try:
                _write_json(self.path, snapshot)
            except OSError:
                with self._lock:
                    self._dirty = True
                raise


def _str_or_none(value) -> bool:
    """Whether value is a string or None."""
    return value is None or isinstance(value, str)


def _valid_links(value) -> bool:
    """Whether value is a list of link records (dicts; Link.from_json checks each when it is used)."""
    return isinstance(value, list) and all(isinstance(link, dict) for link in value)


def _valid_inbox_entry(entry: dict) -> bool:
    """Whether a loaded inbox record has the shape received() gives it."""
    auto = entry.get("auto")
    return (isinstance(entry.get("hash"), str) and isinstance(entry.get("from"), str)
            and _HEX32.fullmatch(entry["from"]) is not None and _valid_links(entry.get("links"))
            and (auto is None or (isinstance(auto, list) and len(auto) == len(entry["links"])
                                  and all(isinstance(a, dict) and _str_or_none(a.get("state")) for a in auto)))
            and all(_str_or_none(entry.get(key)) for key in ("display_name", "title", "text", "method")))


def _valid_outbox_entry(entry: dict) -> bool:
    """Whether a loaded outbox record has the shape send() gives it."""
    return (all(isinstance(entry.get(key), str) for key in ("hash", "to", "state", "method"))
            and _valid_links(entry.get("links")) and all(_str_or_none(entry.get(key)) for key in ("text", "error")))


def _holds_pins(entry: dict) -> bool:
    """Whether an inbox record has auto-fetched pins, or is about to (queued): a flood mustn't release them."""
    return any(isinstance(a, dict) and a.get("state") in ("queued", "pinned") for a in entry.get("auto") or [])


@dataclass
class Incoming:
    """A received LXMF message whose signature LXMF has validated, as plain data."""

    source: bytes  # sender's LXMF address
    hash: bytes  # message hash
    timestamp: object  # sender's clock (untrusted)
    title: bytes
    content: bytes
    fields: object
    method: str  # "direct", "opportunistic", "propagated" or "unknown"
    display_name: str | None = None


def _text(data, limit: int) -> str:
    """Untrusted bytes as text: UTF-8 with replacement characters, NULs removed, at most limit characters."""
    if not isinstance(data, (bytes, bytearray)):
        return ""
    return bytes(data[:limit * 4]).decode("utf-8", "replace").replace("\x00", "")[:limit]


def _timestamp(value) -> float | None:
    """A sender's timestamp if it is a plausible number of seconds since the epoch."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value < 2 ** 40:
        return None
    return float(value)


def _fit_entry(entry: dict):
    """Bound an inbox record to MAX_ENTRY_BYTES as JSON: drop links from the last (keeping one), then cut text.

    Room is left for the auto-fetch results it may get."""
    reserve = 128 * len(entry["links"])  # {"state": "pinned", "cid": ...} per link
    while len(entry["links"]) > 1 and _json_bytes(entry) + reserve > MAX_ENTRY_BYTES:
        entry["links"].pop()
        reserve -= 128
    excess = _json_bytes(entry) + reserve - MAX_ENTRY_BYTES
    if excess > 0:
        # JSON escapes can take up to 6 bytes per character: cut generously, then re-check
        while excess > 0 and entry["text"]:
            entry["text"] = entry["text"][:max(0, len(entry["text"]) - max(excess // 6, 1) - 1)]
            excess = _json_bytes(entry) + reserve - MAX_ENTRY_BYTES
        if excess > 0:
            entry["title"] = ""


class _RoundRobin:
    """Items waiting in a queue per key (an auto-fetch sender), taken one key at a time in turn, so one key's
    backlog doesn't hold up another's. Not thread-safe: the caller locks."""

    def __init__(self):
        """Nothing waiting."""
        self._queues: dict = {}  # key -> deque of its items; dict order is the turn order
        self._count = 0

    def __len__(self) -> int:
        """Items waiting."""
        return self._count

    def __iter__(self):
        """The items, in the order popleft() takes them."""
        queues = [list(queue) for queue in self._queues.values()]
        for turn in range(max(map(len, queues), default=0)):
            yield from (queue[turn] for queue in queues if turn < len(queue))

    def count(self, key) -> int:
        """Items waiting for key."""
        return len(self._queues.get(key, ()))

    def append(self, key, item):
        """Add item at the end of key's queue (a new key takes its turn after the others)."""
        self._queues.setdefault(key, deque()).append(item)
        self._count += 1

    def popleft(self):
        """The next item: the oldest of the key whose turn it is, which then goes last. IndexError if empty."""
        if not self._queues:
            raise IndexError("nothing waiting")
        key = next(iter(self._queues))
        queue = self._queues.pop(key)
        item = queue.popleft()
        self._count -= 1
        if queue:
            self._queues[key] = queue
        return item

    def pop_busiest(self, than: int):
        """Remove and return the newest item of the key with the most waiting, if it has over than + 1 (so taking
        one from it for a key with than waiting leaves things fairer); else None."""
        key = max(self._queues, key=lambda k: len(self._queues[k]), default=None)
        if key is None or len(self._queues[key]) <= than + 1:
            return None
        self._count -= 1
        return self._queues[key].pop()

    def clear(self):
        """Forget everything waiting."""
        self._queues.clear()
        self._count = 0


# The feature


class LxmfFeature(features.Feature):
    """Sends and receives rnfs links over LXMF; see the module docstring."""

    name = NAME

    def __init__(self, node):
        """Load the inbox and outbox; LXMF itself is started in start()."""
        super().__init__(node)
        config = node.config
        self.config = config
        self.network = None  # LxmfNetwork once started (or a stand-in in tests)
        self.off_reason = "LXMF has not been started"
        self.accept_from = {parse_address(a).hex() for a in config.lxmf_accept_from}
        self.auto_fetch_from = {parse_address(a).hex() for a in config.lxmf_auto_fetch_from}
        self.inbox = MessageLog(os.path.join(config.state_dir, "lxmf_inbox.json"), INBOX_LIMIT, MAX_INBOX_BYTES,
                                protect=_holds_pins, valid=_valid_inbox_entry)
        self.outbox = MessageLog(os.path.join(config.state_dir, "lxmf_outbox.json"), OUTBOX_LIMIT,
                                 valid=_valid_outbox_entry)
        for entry in self.outbox.entries():
            if entry.get("state") not in FINAL_STATES and not self._propagated_sent(entry):
                # LXMF doesn't persist its outbound queue
                self.outbox.update(entry["id"], state="failed", error="the daemon stopped before it was delivered")
        self._lock = threading.RLock()  # inbox removals and auto-fetch pins, so a pin is never left behind
        self._release_unrecorded()
        self._resend: dict[str, tuple] = {}  # direct message hash -> (address, content, fields, nearest)
        self._told_no_pn = False  # logged once how to set a propagation node
        self._auto_queue = _RoundRobin()  # (entry id, link index) by sender
        self.clock = time.monotonic  # for the rate limit on senders not trusted (tests set their own)
        self._untrusted_tokens = float(UNTRUSTED_BURST)
        self._untrusted_at: float | None = None
        self._untrusted_dry = False  # logged (notice) that the bucket ran dry; verbose until a token is taken
        self._auto_wanted = threading.Condition()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._threads: list[threading.Thread] = []

    def _release_unrecorded(self):
        """Release lxmf: pins no inbox message records as pinned. Pins are saved at once and the inbox every
        few seconds, so a crash can leave a pin whose message is gone or still queued; nothing else would release
        it (`pin rm` releases only the user's pins). A queued link is fetched and pinned again at start().

        Only if the inbox file was read whole: when it is missing, damaged (moved aside) or had a malformed record,
        such a pin may be one a lost message held. Then each sender's unrecorded pins are kept and recorded in a
        new inbox message, so they are kept at later starts too and removing that message releases them."""
        from rnfs.content.storage import LXMF_PREFIX
        recorded = {(entry.get("from"), result.get("cid")) for entry in self.inbox.entries()
                    for result in entry.get("auto") or [] if isinstance(result, dict) and result.get("state") == "pinned"}
        unrecorded: dict[str, list[Cid]] = {}
        for cid, sources in self.node.storage.pins().items():
            for source in sources:
                if source.startswith(LXMF_PREFIX) and (source[len(LXMF_PREFIX):], str(cid)) not in recorded:
                    unrecorded.setdefault(source[len(LXMF_PREFIX):], []).append(cid)
        if not self.inbox.clean:
            for sender, cids in sorted(unrecorded.items()):
                self._record_kept(sender, sorted(cids, key=str))
            return
        for sender, cids in unrecorded.items():
            for cid in cids:
                _log(f"releasing {cid}, pinned for a message from {sender} that the inbox no longer records",
                     LOG_WARNING)
                try:
                    self.node.storage.unpin(cid, LXMF_PREFIX + sender)
                except OSError as e:  # e.g. a read-only state directory: tried again at the next start
                    _log(f"could not release {cid}: {e}", LOG_ERROR)

    def _record_kept(self, sender: str, cids: list[Cid]):
        """Keep sender's lxmf: pins of cids, which an inbox that didn't load whole doesn't record: add an inbox
        message from sender recording them as pinned, and save the inbox."""
        listed = ", ".join(str(cid) for cid in cids)
        if _HEX32.fullmatch(sender) is None:  # not a source this feature makes: kept, never recorded
            _log(f"keeping {listed}, pinned for {sender!r}, which isn't an LXMF address", LOG_WARNING)
            return
        _log(f"the inbox file is missing, damaged or incomplete: keeping {listed}, pinned for a message from "
             f"{sender} it no longer has, in a new inbox message (remove it to release them)", LOG_WARNING)
        links = [Link(cid) for cid in cids]
        entry = {"hash": "", "from": sender, "display_name": None, "sent": None, "received": time.time(),
                 "title": "", "method": "unknown", "links": [link.to_json() for link in links],
                 "text": "Restored by rnfs at start: the inbox file was missing or damaged, and these links were "
                         "pinned for a message from this sender that it no longer has. Remove this message to "
                         "release them.",
                 "auto": [{"state": "pinned", "cid": str(cid)} for cid in cids]}
        with self._lock:
            _, dropped = self.inbox.add(entry)
            self._release(dropped)
        try:
            self.inbox.save()
        except OSError as e:  # the pins are kept; recorded again at the next start
            _log(f"saving the inbox failed: {e}", LOG_ERROR)

    @staticmethod
    def _propagated_sent(entry: dict) -> bool:
        """A propagated message is done once the propagation node has it."""
        return entry.get("method") == "propagated" and entry.get("state") == "sent"

    # Lifecycle

    def start(self):
        """Start LXMF (if enabled and installed), the announce/sync loop and the auto-fetch worker."""
        if self.config.lxmf_enabled is False:
            self.off_reason = "LXMF is off (lxmf_enabled = false)"
            _log("off: lxmf_enabled is false", LOG_VERBOSE)
            return
        try:
            network = LxmfNetwork(self, self.config, self.node.node_id)
        except ImportError as e:
            self.off_reason = "LXMF is off: the LXMF package is not installed (pip install lxmf)"
            _log(f"the LXMF package can't be imported ({e}), so LXMF messaging is off. Install it with "
                 f"`pip install lxmf`", LOG_WARNING if self.config.lxmf_enabled else LOG_NOTICE)
            return
        except Exception as e:  # noqa: BLE001 - LXMF failing must not stop the node
            self.off_reason = f"LXMF failed to start: {e}"
            _log(f"failed to start: {e!r}", LOG_ERROR)
            return
        self.attach(network)
        self._requeue_auto_fetches()
        for name, target in (("rnfs-lxmf", self._run), ("rnfs-lxmf-fetch", self._run_auto_fetch)):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)
        _log(f"LXMF address {network.address.hex()} ({network.display_name})", LOG_NOTICE)

    def attach(self, network):
        """Use network (LxmfNetwork, or a stand-in in tests) for sending and receiving."""
        self.network = network

    def stop(self):
        """Stop the loops, LXMF's router, and save."""
        self._stop.set()
        self._wake.set()
        with self._auto_wanted:
            self._auto_wanted.notify_all()
        for thread in self._threads:
            thread.join(2.0)
        if self.network is not None:
            try:
                self.network.stop()
            except Exception as e:  # noqa: BLE001
                _log(f"stopping LXMF failed: {e!r}", LOG_ERROR)
        self.save()

    def save(self):
        """Save the inbox and outbox if they changed."""
        self.inbox.save()
        self.outbox.save()

    def status(self) -> dict:
        """The LXMF address (None when off) and the number of inbox messages."""
        return {"lxmf_address": self.network.address.hex() if self.network is not None else None,
                "lxmf_inbox": len(self.inbox)}

    def _require_network(self):
        """The network, or ValueError explaining why LXMF is off."""
        if self.network is None:
            raise ValueError(self.off_reason)
        return self.network

    def _run(self):
        """Announce (at start, after startup_announce_delay, then every announce_interval), download from the
        propagation node every lxmf_sync_interval, and poll outbound messages."""
        network = self.network
        now = time.monotonic()
        announce_at = [now, now + self.config.startup_announce_delay]
        sync_at = now + max(self.config.startup_announce_delay, 10.0) if self.config.lxmf_sync_interval else None
        while not self._stop.is_set():
            now = time.monotonic()
            try:
                if announce_at and now >= announce_at[0]:
                    announce_at.pop(0)
                    if not announce_at:
                        announce_at.append(now + self.config.announce_interval)
                    network.announce()
                if sync_at is not None and now >= sync_at:
                    sync_at = now + self.config.lxmf_sync_interval
                    if network.propagation_node() is None:
                        self._no_propagation_node("messages waiting at propagation nodes aren't downloaded")
                    else:
                        network.sync()
                for message_hash, state, progress in network.poll():
                    self.outbound_changed(message_hash, state, progress)
            except Exception as e:  # noqa: BLE001 - keep the loop alive
                _log(f"loop failed: {e!r}", LOG_ERROR)
            self._wake.wait(TICK)
            self._wake.clear()

    def _no_propagation_node(self, consequence: str):
        """Log, once, that no propagation node is set and how to set one."""
        if self._told_no_pn:
            return
        self._told_no_pn = True
        nearest = self.network.nearest_propagation_node() if self.network is not None else None
        heard = f" The nearest heard announcing itself is {nearest.hex()}." if nearest is not None else ""
        _log(f"no propagation node is set (lxmf_propagation_node), so {consequence} and failed direct messages "
             f"aren't sent through one.{heard} To use one, set lxmf_propagation_node = \"<32 hex digits>\" to a "
             f"node you trust to hold your messages, or use `rnfs lxmf sync --nearest` / "
             f"`rnfs send --propagate --nearest` to use the nearest one heard, once", LOG_NOTICE)

    # Sending

    def address(self) -> dict:
        """This node's LXMF address, display name, configured propagation node and the nearest one heard."""
        network = self._require_network()
        node = network.propagation_node()
        nearest = network.nearest_propagation_node()
        return {"address": network.address.hex(), "display_name": network.display_name,
                "propagation_node": node.hex() if node is not None else None,
                "nearest_propagation_node": nearest.hex() if nearest is not None else None}

    def _recipient(self, to: str) -> bytes:
        """An LXMF address, or the LXMF address of a contact nick."""
        if not isinstance(to, str) or not to.strip():
            raise ValueError("a recipient is required: an LXMF address or a contact")
        if _HEX32.fullmatch(to.strip().strip("<>")):
            return parse_address(to)
        # A contact nick, through rnfs/features/contacts.py when that feature is loaded
        contacts = getattr(self.node, "features", {}).get("contacts")
        if contacts is not None and hasattr(contacts, "lookup"):
            found = contacts.lookup(to, "lxmf")
            if isinstance(found, (bytes, bytearray)) and len(found) == ID_LENGTH:
                return bytes(found)
            if contacts.lookup(to) is not None:
                raise ValueError(f"contact {to} is not an LXMF address")
            raise ValueError(f"no LXMF contact {to!r}, and it isn't an LXMF address (32 hex characters)")
        raise ValueError(f"{to!r} is not an LXMF address (32 hex characters), and contacts aren't available")

    def _contact_target(self, target):
        """target with a leading name contact's nick replaced by name/<name>, so the link follows the name."""
        contacts = getattr(self.node, "features", {}).get("contacts")
        if not isinstance(target, str) or contacts is None or not hasattr(contacts, "lookup"):
            return target
        first, slash, rest = target.partition("/")
        if first == NAME_SEGMENT or target[:len(URI_SCHEME)].lower() == URI_SCHEME:
            return target
        name = contacts.lookup(first, "name")
        return f"{NAME_SEGMENT}/{name.hex()}{slash}{rest}" if isinstance(name, bytes) else target

    def _describe(self, link: Link) -> Link:
        """link with this node as its provider hint, and the CID and size this node knows for it, if any."""
        cid, size = link.cid, None
        if link.name is not None and cid is None:
            for entry in self.node.name_list():
                if entry.get("name") == link.name.hex() and entry.get("cid"):
                    cid = Cid.parse(entry["cid"])
        if cid is not None:
            target = "/".join([str(cid), *link.path])
            try:
                if self.node.is_local(target):  # then info() needs no network
                    size = self.node.info(target)["size"]
            except Exception:  # noqa: BLE001 - the size is only a hint
                size = None
        # The provider hint links this LXMF address to the node ID (lxmf_send_from_hint = false omits it)
        return Link(cid, link.name, link.path, size, self.node.node_id if self.config.lxmf_send_from_hint else None)

    def send(self, to: str, target: str, text: str | None = None, propagate: bool = False,
             nearest: bool = False) -> dict:
        """Send an rnfs link (and text) to an LXMF address or contact; returns {"id", "hash", "to", "method"}.

        Direct delivery unless propagate; a direct message that fails is sent again through
        lxmf_propagation_node, if set. nearest: without lxmf_propagation_node, use the nearest propagation node
        heard for this message. Returns once the message is queued: the outbox shows how it went.
        """
        network = self._require_network()
        address = self._recipient(to)
        if text is not None and (not isinstance(text, str) or len(text) > MAX_TEXT_CHARS):
            raise ValueError(f"the message text must be at most {MAX_TEXT_CHARS} characters")
        link = self._describe(parse_target(self._contact_target(target)))
        content = message_text(text, [link])
        fields = encode_field([link])
        if propagate and network.propagation_node(nearest) is None:
            raise ValueError(_NO_PN_HELP if not nearest else "no LXMF propagation node is set or heard")
        timeout = min(IDENTITY_WAIT, self.config.path_timeout)
        if not network.wait_identity(address, timeout):
            from rnfs.errors import NotFound
            raise NotFound(f"LXMF address {address.hex()} is unknown here: no announce from it has been heard. "
                           f"A path request was sent; try again shortly")
        message_hash = network.send(address, content, fields, propagate, nearest)
        method = "propagated" if propagate else "direct"
        entry = {"hash": message_hash.hex(), "to": address.hex(), "time": time.time(), "links": [link.to_json()],
                 "text": text or "", "method": method, "state": "outbound", "progress": 0.0}
        entry_id, _ = self.outbox.add(entry)
        if not propagate:
            with self._lock:
                self._resend[message_hash.hex()] = (address, content, fields, nearest)
        _log(f"queued {method} message {message_hash.hex()} to {address.hex()} with {link.uri()}", LOG_INFO)
        return {"id": entry_id, "hash": message_hash.hex(), "to": address.hex(), "method": method}

    def outbound_changed(self, message_hash: bytes, state: str, progress: float | None):
        """Record a sent message's new state; a failed direct message falls back to propagation (also once its
        outbox record was dropped to make room)."""
        entry = self.outbox.find("hash", message_hash.hex())
        values = {"state": state}
        if isinstance(progress, (int, float)) and not isinstance(progress, bool):
            values["progress"] = round(float(progress), 3)
        with self._lock:
            resend = self._resend.pop(message_hash.hex(), None) if state in FINAL_STATES else None
        if state == "failed" and resend is not None and (entry is None or entry.get("method") == "direct"):
            network = self.network
            nearest = resend[3]
            if network is not None and network.propagation_node(nearest) is None:
                self._no_propagation_node("messages waiting at propagation nodes aren't downloaded")
            elif network is not None:
                try:
                    new_hash = network.send(resend[0], resend[1], resend[2], True, nearest)
                    if entry is not None:
                        self.outbox.update(entry["id"], hash=new_hash.hex(), method="propagated",
                                           state="outbound", progress=0.0, direct_hash=message_hash.hex(),
                                           fallback=True)
                    _log(f"direct delivery of {message_hash.hex()} failed; sending it through the propagation "
                         f"node as {new_hash.hex()}", LOG_NOTICE)
                    return
                except Exception as e:  # noqa: BLE001
                    values["error"] = f"propagation fallback failed: {fsutil.printable(e)}"
        if entry is not None:
            self.outbox.update(entry["id"], **values)

    # Receiving

    def received(self, message: Incoming) -> int | None:
        """Handle a validated incoming message: store it if it has rnfs links and its sender is accepted,
        then queue auto-fetches. Returns its inbox id, or None if it was ignored."""
        sender = message.source.hex()
        if self.network is not None and message.source == self.network.address:
            return None
        if not self.accepts(message.source):
            _log(f"ignored a message from {sender}: not in lxmf_accept_from", LOG_VERBOSE)
            return None
        message_hash = message.hash.hex()
        if self.inbox.find("hash", message_hash) is not None:
            return None
        text = _text(message.content, MAX_TEXT_CHARS)
        try:
            links = decode_field(message.fields)
        except ValueError as e:
            _log(f"ignored the malformed rnfs field of message {message_hash} from {sender}: {fsutil.printable(e)}",
                 LOG_VERBOSE)
            links = None
        if links is None:
            # Links past the text kept count too: up to MAX_SCAN_CHARS are searched
            links = find_links(_text(message.content, MAX_SCAN_CHARS))
        if not links:
            _log(f"message {message_hash} from {sender} has no rnfs links; not kept", LOG_VERBOSE)
            return None
        display_name = message.display_name if isinstance(message.display_name, str) else None
        entry = {"hash": message_hash, "from": sender,
                 "display_name": display_name[:MAX_DISPLAY_NAME_CHARS] if display_name else None,
                 "sent": _timestamp(message.timestamp), "received": time.time(),
                 "title": _text(message.title, MAX_TITLE_CHARS), "text": text,
                 "method": message.method if isinstance(message.method, str) else "unknown",
                 "links": [link.to_json() for link in links], "auto": None}
        _fit_entry(entry)
        auto = sender in self.auto_fetch_from
        if auto:
            entry["auto"] = [{"state": "queued"} for _ in entry["links"]]
        if not self.trusted(message.source) and not self._untrusted_token():
            _log(f"message {message_hash} from {sender} not kept: too many new messages from senders not in "
                 f"contacts, lxmf_accept_from or lxmf_auto_fetch_from",
                 LOG_VERBOSE if self._untrusted_dry else LOG_NOTICE)
            self._untrusted_dry = True
            return None
        with self._lock:
            # A sender not trusted with auto-fetch gets INBOX_PER_SENDER records, so one can't push out others',
            # and records of trusted senders are dropped only for each other's: identities are free
            entry_id, dropped = self.inbox.add(entry, per_from=None if auto else INBOX_PER_SENDER,
                                               reserved=lambda record: self._trusted_hex(record.get("from")))
            self._release(dropped)
        if entry_id is None:
            _log(f"message {message_hash} from {sender} not kept: the inbox is full of messages from trusted "
                 f"senders or holding auto-fetched pins (rnfs inbox rm)", LOG_NOTICE)
            return None
        _log(f"message {message_hash} from {sender} with {len(entry['links'])} rnfs link(s), inbox #{entry_id}",
             LOG_NOTICE)
        if auto:
            for index in range(len(entry["links"])):
                self._queue_auto_fetch(entry_id, index, sender)
        return entry_id

    def accepts(self, source: bytes) -> bool:
        """Whether messages from LXMF address source are kept: lxmf_accept_from (empty: everyone), and
        lxmf_auto_fetch_from."""
        sender = source.hex()
        return not self.accept_from or sender in self.accept_from or sender in self.auto_fetch_from

    def trusted(self, source: bytes) -> bool:
        """Whether the user named LXMF address source: in lxmf_accept_from or lxmf_auto_fetch_from, or an LXMF
        contact. Their inbox records are dropped only for each other's, their new records aren't rate limited,
        and their messages held for an identity take places held for others'."""
        return self._trusted_hex(source.hex())

    def _trusted_hex(self, sender) -> bool:
        """trusted() for an address as 32 hex characters (an inbox record's "from")."""
        if sender in self.accept_from or sender in self.auto_fetch_from:
            return True
        contacts = getattr(self.node, "features", {}).get("contacts")
        if contacts is None or not hasattr(contacts, "has") or not isinstance(sender, str):
            return False
        try:
            return bool(contacts.has(bytes.fromhex(sender), "lxmf"))
        except ValueError:
            return False

    def _untrusted_token(self) -> bool:
        """Take a token for a new record from a sender not trusted (UNTRUSTED_BURST, then one per
        UNTRUSTED_INTERVAL, node-wide); False if there is none."""
        now = self.clock()
        with self._lock:
            if self._untrusted_at is not None:
                self._untrusted_tokens = min(float(UNTRUSTED_BURST), self._untrusted_tokens
                                             + max(0.0, now - self._untrusted_at) / UNTRUSTED_INTERVAL)
            self._untrusted_at = now
            if self._untrusted_tokens < 1:
                return False
            self._untrusted_tokens -= 1
            self._untrusted_dry = False
            return True

    def inbox_list(self) -> list[dict]:
        """Inbox messages, oldest first."""
        return self.inbox.entries()

    def outbox_list(self) -> list[dict]:
        """Sent messages, oldest first."""
        return self.outbox.entries()

    def inbox_rm(self, entry_id: int | None) -> int:
        """Remove one inbox message (every one for None), releasing the pins it auto-fetched. Returns how many."""
        with self._lock:
            removed = self.inbox.remove(entry_id)
            self._release(removed)
        if entry_id is not None and not removed:
            from rnfs.errors import NotFound
            raise NotFound(f"no inbox message #{entry_id}")
        return len(removed)

    def _release(self, removed: list[dict]):
        """Release the lxmf:<sender> pins of removed entries that no remaining entry of that sender holds."""
        from rnfs.content.storage import LXMF_PREFIX
        if not removed:
            return
        kept = {(e.get("from"), a.get("cid")) for e in self.inbox.entries()
                for a in (e.get("auto") or []) if isinstance(a, dict) and a.get("state") == "pinned"}
        released = False
        for entry in removed:
            for result in entry.get("auto") or []:
                if not isinstance(result, dict) or result.get("state") != "pinned":
                    continue
                if (entry.get("from"), result.get("cid")) in kept:
                    continue
                try:
                    released |= self.node.storage.unpin(Cid.parse(result["cid"]), LXMF_PREFIX + entry["from"])
                except (ValueError, KeyError, TypeError):
                    continue
        if released:
            self.node.publisher.refresh()

    def _resolve(self, link: Link, persist: bool = True) -> tuple[Cid, list[str]]:
        """The root CID and path a link names, resolving a name (falling back to the CID it was sent with).

        persist False (auto-fetch): the name's record goes only into the bounded table, so names arriving
        unasked don't grow names.json; its sequence still goes into the bounded, saved floor, so a later
        auto-fetch of the name never resolves to an older record."""
        if link.name is None:
            return link.cid, list(link.path)
        from rnfs.errors import NotFound
        try:
            if persist:
                return Cid.parse(self.node.name_resolve(link.name.hex())["cid"]), list(link.path)
            record = self.node.names.resolve(link.name, persist=False)
            if record is None:
                raise NotFound(f"name {link.name.hex()} could not be resolved")
            return Cid.parse(record["cid"]), list(link.path)
        except NotFound:
            if link.cid is None:
                raise
            _log(f"name {link.name.hex()} could not be resolved; using the CID it was sent with", LOG_NOTICE)
            return link.cid, list(link.path)

    def _hints(self, link: Link) -> list[bytes | None]:
        """Where to fetch from: the link's provider hint first (unless it is this node), then the DHT."""
        if link.from_node is not None and link.from_node != self.node.node_id:
            return [link.from_node, None]
        return [None]

    def inbox_get(self, entry_id: int, index: int | None = None, output: str | None = None,
                  directory: str | None = None) -> list[dict]:
        """Fetch the links of inbox message entry_id (only link index, 1-based, if given) and write each out:
        to output, or into directory under its last path name (the root CID if that name is hidden,
        non-printable or not a plain file name here: the sender chose it). Tries the sender's node first, then
        the DHT. Once one link has started, a later one's failure is a result with "error", not raised."""
        from rnfs.errors import NotFound
        entry = self.inbox.get(entry_id)
        if entry is None:
            raise NotFound(f"no inbox message #{entry_id}")
        links = [Link.from_json(value) for value in entry.get("links") or []]
        if index is not None:
            if not 1 <= index <= len(links):
                raise ValueError(f"message #{entry_id} has {len(links)} link(s)")
            links = [links[index - 1]]
        if output is not None and len(links) != 1:
            raise ValueError("-o needs a single link: choose one with --link")
        if output is None and directory is None:
            raise ValueError("an output path or directory is required")
        # Every output is checked before anything starts, so a clash can't stop the fetches halfway
        planned = []
        for number, link in enumerate(links, 1):
            root, names = self._resolve(link)
            target = "/".join([str(root), *names])
            if output is not None:
                path = output
            else:
                from rnfs import fsutil
                path = os.path.join(directory, fsutil.safe_default_name(names[-1] if names else None, str(root)))
            if os.path.lexists(path):
                raise ValueError(f"{fsutil.printable(path)} already exists; remove it or choose another with -o")
            same = next((i for i, (_, other, _) in enumerate(planned) if other == path), None)
            if same is not None:
                raise ValueError(f"links {same + 1} and {number} would both be written to {fsutil.printable(path)}: "
                                 f"get them one at a time with --link and -o")
            planned.append((link, path, target))
        results = []
        for link, path, target in planned:
            try:
                results.append(self._get_link(link, path, target))
            except Exception as e:  # noqa: BLE001 - raised as is when nothing has started
                if not results:
                    raise
                # Something has started already: report it, and this failure with it
                results.append({"target": target, "output": path, "error": str(e) or type(e).__name__})
        return results

    def _get_link(self, link: Link, path: str, target: str) -> dict:
        """Fetch one link of `inbox get` to path: as a job if the jobs feature is loaded, else here and now."""
        from rnfs.errors import InvalidDag, RnfsError, StorageFull, TooLarge
        job = self._start_job(target, path, link)
        if job is not None:
            return {"target": target, "output": path, "job": job}
        hints = self._hints(link)
        for attempt, from_node in enumerate(hints):
            try:
                result = self.node.get(target, path, from_node=from_node)
                break
            except (TooLarge, InvalidDag, StorageFull):
                raise
            except RnfsError as e:
                if attempt == len(hints) - 1 or os.path.lexists(path):
                    raise
                _log(f"{from_node.hex()} could not supply {fsutil.printable(target)} ({fsutil.printable(e)}); trying "
                     f"the DHT", LOG_NOTICE)
        return {"target": target, "output": path, "cid": result["cid"], "warnings": result.get("warnings", [])}

    def _start_job(self, target: str, output: str, link: Link):
        """Start the fetch as a background job when rnfs/features/jobs.py is loaded; the job's dict, or None
        to fetch here and now.

        Like a fetch here: the link's provider hint first, then (fallback_dht) ordinary DHT discovery.
        """
        jobs = getattr(self.node, "features", {}).get("jobs")
        if jobs is None or not hasattr(jobs, "submit"):
            return None
        from_node = self._hints(link)[0]
        if from_node is None:
            return jobs.submit("get", target, output)
        return jobs.submit("get", target, output, from_node=from_node.hex(), fallback_dht=True)

    # Auto-fetch

    def _queue_auto_fetch(self, entry_id: int, index: int, sender: str):
        """Queue one link of an inbox message from sender for auto-fetch. When the queue is full, the newest link
        of the sender with the most waiting is skipped instead, if that sender has two more than this one; else
        this link is."""
        skipped = {"state": "skipped", "error": "too many auto-fetches waiting"}
        with self._auto_wanted:
            if len(self._auto_queue) >= AUTO_QUEUE_LIMIT:
                displaced = self._auto_queue.pop_busiest(self._auto_queue.count(sender))
                if displaced is None:
                    self._set_auto(entry_id, index, skipped)
                    return
                self._set_auto(*displaced, skipped)
            self._auto_queue.append(sender, (entry_id, index))
            self._auto_wanted.notify()

    def _requeue_auto_fetches(self):
        """Queue again, oldest first, the auto-fetches still "queued" when the daemon stopped (also one that
        was running then): the queue itself isn't saved, and a queued record is never dropped for others."""
        for entry in self.inbox.entries():
            for index, result in enumerate(entry.get("auto") or []):
                if isinstance(result, dict) and result.get("state") == "queued":
                    self._queue_auto_fetch(entry["id"], index, entry["from"])

    def _run_auto_fetch(self):
        """The auto-fetch worker: one fetch at a time, so quotas are checked against settled usage, taking senders
        in turn."""
        while not self._stop.is_set():
            with self._auto_wanted:
                while not self._auto_queue and not self._stop.is_set():
                    self._auto_wanted.wait(5.0)
                if self._stop.is_set():
                    return
                item = self._auto_queue.popleft()
            self.auto_fetch(*item)

    def process_auto_fetches(self):
        """Run every queued auto-fetch now (tests; the worker thread does this in a running node)."""
        while True:
            with self._auto_wanted:
                if not self._auto_queue:
                    return
                item = self._auto_queue.popleft()
            self.auto_fetch(*item)

    def _set_auto(self, entry_id: int, index: int, result: dict):
        """Record an auto-fetch result in its inbox entry."""
        entry = self.inbox.get(entry_id)
        if entry is None:
            return
        auto = list(entry.get("auto") or [])
        if 0 <= index < len(auto):
            auto[index] = result
            self.inbox.update(entry_id, auto=auto)

    def auto_fetch(self, entry_id: int, index: int):
        """Fetch one link of an inbox message and pin it with source lxmf:<sender>, within the sender's quota.

        The fetch stops (within fetcher.POLL) when the node stops, leaving the link queued for the next start,
        or when the message is removed."""
        from rnfs import fetcher
        from rnfs.content.storage import LXMF_PREFIX
        from rnfs.errors import InvalidDag, RnfsError, StorageFull, TooLarge
        entry = self.inbox.get(entry_id)
        if entry is None:
            return
        observer = fetcher.Observer()
        observer.cancelled = lambda: self._stop.is_set() or self.inbox.get(entry_id) is None
        try:
            link = Link.from_json(entry["links"][index])
            source = LXMF_PREFIX + entry["from"]
            root, names = self._resolve(link, persist=False)
            hints = self._hints(link)
            for attempt, from_node in enumerate(hints):
                try:
                    with fetcher.observing(observer):
                        cid = self._fetch_and_pin(entry_id, index, root, names, from_node, source)
                    break
                except (TooLarge, InvalidDag, StorageFull, fetcher.Cancelled):
                    raise
                except RnfsError as e:
                    if attempt == len(hints) - 1:
                        raise
                    _log(f"{from_node.hex()} could not supply {root} ({fsutil.printable(e)}); trying the DHT",
                         LOG_VERBOSE)
            if cid is not None:
                _log(f"auto-fetched and pinned {cid} from inbox #{entry_id}", LOG_NOTICE)
            return  # pinned and recorded by _fetch_and_pin, or the message was removed meanwhile
        except fetcher.Cancelled:
            _log(f"auto-fetch of inbox #{entry_id} link {index + 1} stopped", LOG_VERBOSE)
            return  # still "queued" if the node is stopping: fetched at the next start
        except TooLarge as e:
            result = {"state": "refused", "error": str(e)}
        except (RnfsError, ValueError, KeyError, IndexError, TypeError) as e:
            result = {"state": "failed", "error": str(e) or type(e).__name__}
        except Exception as e:  # noqa: BLE001
            _log(f"auto-fetch of inbox #{entry_id} failed: {e!r}", LOG_ERROR)
            result = {"state": "failed", "error": repr(e)}
        _log(f"auto-fetch of inbox #{entry_id} link {index + 1}: {result['state']}: "
             f"{fsutil.printable(result['error'])}", LOG_NOTICE)
        self._set_auto(entry_id, index, result)

    def _pin_recorded(self, entry_id: int, index: int, cid: Cid, source: str) -> bool:
        """Pin cid with source and record it in the inbox entry, in one locked step so a removal can't come
        between them and leave a pin no entry releases; False (nothing pinned) if the entry is gone."""
        with self._lock:
            if self.inbox.get(entry_id) is None:
                return False
            self.node.storage.pin(cid, source)
            self._set_auto(entry_id, index, {"state": "pinned", "cid": str(cid)})
            return True

    def _fetch_and_pin(self, entry_id: int, index: int, root: Cid, names: list[str], from_node: bytes | None,
                       source: str) -> Cid | None:
        """Fetch <root>/<names> completely and pin the target with source, unless it would exceed the
        sender's quota (TooLarge). Returns the CID pinned, or None if the message went away meanwhile."""
        from rnfs.errors import TooLarge
        quota = self.config.lxmf_auto_fetch_quota
        storage = self.node.storage
        with self.node.fetcher.open(root, names, from_node, pinned=True) as fetch:
            expect = fetch.resolve()
            if source in storage.pins().get(expect.cid, set()):
                return expect.cid if self._pin_recorded(entry_id, index, expect.cid, source) else None
            content = fetch.size_of(expect)
            used = storage.source_usage(source)
            if used + content > quota:
                raise TooLarge(f"{content} bytes would exceed the sender's quota "
                               f"({used} of {quota} bytes used, lxmf_auto_fetch_quota)")
            # A DAG of empty files has size 0 yet can hold many directory nodes: count its blocks too
            fetch.max_dag_bytes = quota - used
            fetch.fetch_dag(expect)
            if not self._pin_recorded(entry_id, index, expect.cid, source):
                return None
        self.node.publisher.refresh()
        return expect.cid

    # API

    @classmethod
    def api_routes(cls) -> dict:
        """The /api/lxmf/ endpoints."""
        return {("GET", "/api/lxmf/address"): _api_address, ("POST", "/api/lxmf/send"): _api_send,
                ("GET", "/api/lxmf/inbox"): _api_inbox, ("POST", "/api/lxmf/inbox/get"): _api_inbox_get,
                ("POST", "/api/lxmf/inbox/rm"): _api_inbox_rm, ("GET", "/api/lxmf/outbox"): _api_outbox,
                ("POST", "/api/lxmf/sync"): _api_sync}

    @classmethod
    def cli_commands(cls, commands, command):
        """send, inbox [get|rm], outbox, lxmf address|sync."""
        send = command(commands, "send", _run_send, "send an rnfs link to an LXMF address or contact")
        send.add_argument("to", metavar="ADDRESS|CONTACT", help="LXMF address (32 hex characters) or contact")
        send.add_argument("target", metavar="CID[/PATH]", help="<cid>[/path], name/<name>[/path] or an rnfs: URI")
        send.add_argument("-m", "--message", help="text to send with the link")
        send.add_argument("--propagate", action="store_true",
                          help="deliver through the LXMF propagation node (store and forward)")
        send.add_argument("--nearest", action="store_true",
                          help="without lxmf_propagation_node set, use the nearest propagation node heard (for "
                               "--propagate, or if direct delivery fails)")

        inbox = command(commands, "inbox", _run_inbox, "list received LXMF messages with rnfs links")
        inbox_commands = inbox.add_subparsers(dest="inbox_command", metavar="command")
        get = command(inbox_commands, "get", _run_inbox_get, "fetch the content an inbox message links to")
        get.add_argument("n", type=int, metavar="N", help="message number, from `rnfs inbox`")
        get.add_argument("--link", type=int, metavar="K", help="only the K-th link of the message")
        get.add_argument("-o", "--output", help="where to write (one link only; default: its name, here)")
        rm = command(inbox_commands, "rm", _run_inbox_rm,
                     "remove inbox messages, releasing what they auto-fetched")
        rm.add_argument("n", type=int, nargs="?", metavar="N", help="message number")
        rm.add_argument("--all", action="store_true", help="remove every message")

        command(commands, "outbox", _run_outbox, "list sent LXMF messages and their delivery state")

        lxmf = commands.add_parser("lxmf", help="this node's LXMF identity").add_subparsers(
            dest="lxmf_command", required=True, metavar="command")
        command(lxmf, "address", _run_address, "print this node's LXMF address, to share")
        sync = command(lxmf, "sync", _run_sync, "download waiting messages from the propagation node now")
        sync.add_argument("--nearest", action="store_true",
                          help="without lxmf_propagation_node set, use the nearest propagation node heard")


FEATURE = LxmfFeature


# API handlers


def _feature(handler) -> LxmfFeature:
    """The lxmf feature of the handler's node. NotFound if the daemon has none."""
    return _common.feature(handler, NAME)


def _api_address(handler):
    """GET /api/lxmf/address: {"address", "display_name", "propagation_node", "nearest_propagation_node"}."""
    handler.send_json(200, _feature(handler).address())


def _api_send(handler):
    """POST /api/lxmf/send {to, target, message?, propagate?, nearest?}: {"id", "hash", "to", "method"}."""
    from rnfs.api import parse_bool
    body = handler.json_body()
    handler.send_json(200, _feature(handler).send(body.get("to"), body.get("target"), body.get("message"),
                                                  parse_bool(body.get("propagate", False)),
                                                  parse_bool(body.get("nearest", False))))


def _api_inbox(handler):
    """GET /api/lxmf/inbox: {"messages": [...]}."""
    handler.send_json(200, {"messages": _feature(handler).inbox_list()})


def _int(value, name: str, required: bool = True) -> int | None:
    """A positive integer from a JSON body."""
    if value is None and not required:
        return None
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _api_inbox_get(handler):
    """POST /api/lxmf/inbox/get {id, link?, output? | directory?}: {"results": [...]}."""
    from rnfs.api import parse_path
    body = handler.json_body()
    output = parse_path(body["output"]) if body.get("output") is not None else None
    directory = parse_path(body["directory"]) if body.get("directory") is not None else None
    results = _feature(handler).inbox_get(_int(body.get("id"), "id"), _int(body.get("link"), "link", False),
                                          output, directory)
    handler.send_json(200, {"results": results})


def _api_inbox_rm(handler):
    """POST /api/lxmf/inbox/rm {id} or {all: true}: {"removed": n}."""
    from rnfs.api import parse_bool
    body = handler.json_body()
    if parse_bool(body.get("all", False)):
        removed = _feature(handler).inbox_rm(None)
    else:
        removed = _feature(handler).inbox_rm(_int(body.get("id"), "id"))
    handler.send_json(200, {"removed": removed})


def _api_outbox(handler):
    """GET /api/lxmf/outbox: {"messages": [...]}."""
    handler.send_json(200, {"messages": _feature(handler).outbox_list()})


def _api_sync(handler):
    """POST /api/lxmf/sync {nearest?}: {"propagation_node", "started"}; nearest: without lxmf_propagation_node
    set, the nearest propagation node heard."""
    from rnfs.api import parse_bool
    body = handler.json_body()
    nearest = parse_bool(body.get("nearest", False))
    network = _feature(handler)._require_network()
    node = network.propagation_node(nearest)
    handler.send_json(200, {"propagation_node": node.hex() if node is not None else None,
                            "started": bool(node is not None and network.sync(nearest))})


# CLI commands


def _when(timestamp) -> str:
    """Local time of a timestamp, or "?"."""
    if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)) or not 0 < timestamp < 2 ** 40:
        return "?"
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(timestamp))


def _print_links(entry: dict, printable):
    """Print a message's links, one per line, with size and auto-fetch state."""
    auto = entry.get("auto") if isinstance(entry.get("auto"), list) else []
    for i, link in enumerate(entry.get("links") or []):
        if not isinstance(link, dict):
            continue
        notes = []
        if isinstance(link.get("size"), int):
            notes.append(human_size(link["size"]))
        if i < len(auto) and isinstance(auto[i], dict):
            state = auto[i].get("state")
            notes.append(f"auto-fetch {state}" + (f": {auto[i]['error']}" if auto[i].get("error") else ""))
        print(f"    [{i + 1}] {printable(link.get('uri'))}" + (f"  ({printable(', '.join(notes))})" if notes else ""))


def _print_text(text, printable):
    """Print message text indented, each line escaped."""
    for line in str(text or "").splitlines()[:50]:
        print(f"    | {printable(line)}")


def _run_send(args):
    """Send an rnfs link; print the LXMF message hash."""
    from rnfs import cli
    result = cli.client(args).post("/api/lxmf/send", to=args.to, target=args.target, message=args.message,
                                   propagate=args.propagate, nearest=args.nearest)
    print(cli.printable(result["hash"]))


def _run_inbox(args):
    """List inbox messages: number, time, sender, links and text."""
    from rnfs import cli
    messages = cli.client(args).get("/api/lxmf/inbox")["messages"]
    if not messages:
        print("no messages")
    for entry in messages:
        sender = entry.get("from") or "?"
        name = entry.get("display_name")
        who = f"{cli.printable(name)} <{cli.printable(sender)}>" if name else f"<{cli.printable(sender)}>"
        print(f"#{cli.printable(entry.get('id'))}  {_when(entry.get('received'))}  {who}  "
              f"{cli.printable(entry.get('method') or '')}")
        _print_links(entry, cli.printable)
        _print_text(_strip_links(entry), cli.printable)


def _strip_links(entry: dict) -> str:
    """A message's text without the lines that are just its links (they are listed already)."""
    uris = {link.get("uri") for link in entry.get("links") or [] if isinstance(link, dict)}
    lines = [line for line in str(entry.get("text") or "").splitlines() if line.strip() not in uris]
    return "\n".join(lines).strip()


def _run_inbox_get(args):
    """Fetch what an inbox message links to; print where each was written."""
    from rnfs import cli
    output = cli.absolute(args.output) if args.output else None
    if output is not None and os.path.lexists(output):
        cli.fail(f"{output} already exists; remove it or choose another with -o")
    result = cli.client(args).post("/api/lxmf/inbox/get", id=args.n, link=args.link, output=output,
                                   directory=os.getcwd())
    failed = False
    for item in result["results"]:
        cli.print_warnings(item)
        job = item.get("job")
        if item.get("error") is not None:
            failed = True
            print(f"rnfs: {cli.printable(item['output'])}: {cli.printable(item['error'])}", file=sys.stderr)
        elif isinstance(job, dict):
            print(f"job {cli.printable(job.get('id'))}: {cli.printable(item['output'])} (rnfs jobs shows its progress)")
        else:
            print(cli.printable(item["output"]))
    if failed:
        sys.exit(1)


def _run_inbox_rm(args):
    """Remove one or every inbox message."""
    from rnfs import cli
    if args.all == (args.n is not None):
        cli.fail("give a message number or --all")
    result = cli.client(args).post("/api/lxmf/inbox/rm", **({"all": True} if args.all else {"id": args.n}))
    print(f"removed {cli.printable(result['removed'])} message(s)")


def _run_outbox(args):
    """List sent messages: number, time, recipient, method, state and hash."""
    from rnfs import cli
    messages = cli.client(args).get("/api/lxmf/outbox")["messages"]
    if not messages:
        print("no messages")
    for entry in messages:
        state = entry.get("state")
        if state == "sent" and entry.get("method") == "propagated":
            state = "sent to the propagation node"
        elif state not in FINAL_STATES and isinstance(entry.get("progress"), (int, float)):
            state = f"{state} {round(entry['progress'] * 100)}%"
        print(f"#{cli.printable(entry.get('id'))}  {_when(entry.get('time'))}  to <{cli.printable(entry.get('to'))}>  "
              f"{cli.printable(entry.get('method'))}  {cli.printable(state)}  {cli.printable(entry.get('hash'))}")
        if entry.get("error"):
            print(f"    error: {cli.printable(entry['error'])}")
        _print_links(entry, cli.printable)
        _print_text(entry.get("text"), cli.printable)


def _run_address(args):
    """Print this node's LXMF address, then a line to share it with."""
    from rnfs import cli
    result = cli.client(args).get("/api/lxmf/address")
    print(cli.printable(result["address"]))
    print(f"Send me rnfs links over LXMF (Sideband, NomadNet, MeshChat): {cli.printable(result['address'])} "
          f"\"{cli.printable(result['display_name'])}\"")


def _run_sync(args):
    """Ask the daemon to download waiting messages from the propagation node."""
    from rnfs import cli
    result = cli.client(args).post("/api/lxmf/sync", nearest=args.nearest)
    if result["propagation_node"] is None:
        cli.fail("no LXMF propagation node is set or heard" if args.nearest else _NO_PN_HELP)
    print(f"{'downloading' if result['started'] else 'already downloading'} from "
          f"{cli.printable(result['propagation_node'])}")


# LXMF and RNS


@contextmanager
def _keeping_signal_handlers():
    """Stop LXMRouter installing its SIGINT/SIGTERM handlers, which exit at once: the daemon shuts down itself.

    signal.signal is replaced for the moment LXMRouter is made, which also lets it be made off the main
    thread (where signal.signal would raise).
    """
    original = signal.signal
    signal.signal = lambda signum, handler: signal.getsignal(signum)
    try:
        yield
    finally:
        signal.signal = original


class _PropagationAnnounces:
    """RNS announce handler remembering LXMF propagation nodes, for choosing one when none is configured."""

    def __init__(self, network: "LxmfNetwork"):
        """Handler for network."""
        self.aspect_filter = "lxmf.propagation"
        self.receive_path_responses = True
        self.network = network

    def received_announce(self, destination_hash, announced_identity, app_data):
        """Record (or forget) a propagation node from its announce."""
        try:
            self.network.heard_propagation_node(bytes(destination_hash), app_data)
        except Exception:  # noqa: BLE001 - untrusted announce data
            pass


class LxmfNetwork:
    """Every RNS and LXMF object of the feature: the router, the delivery destination, announces, sending,
    receiving and propagation node downloads."""

    def __init__(self, feature: LxmfFeature, config, node_id: bytes):
        """Start LXMF's router with the node's LXMF identity. Raises ImportError if LXMF isn't installed."""
        import LXMF
        import RNS
        from RNS.vendor import umsgpack

        from rnfs.network import guards
        from rnfs.network.identity import load_or_create_identity
        self.RNS, self.LXMF, self.msgpack = RNS, LXMF, umsgpack
        self.feature = feature
        self.config = config
        self._lock = threading.Lock()
        self._outbound: dict[bytes, list] = {}  # hash -> [LXMessage, last reported state, progress]
        self._held: list = []  # (deadline, packed bytes, method) awaiting identities, at most HOLD_LIMIT
        self._path_requested: dict[bytes, float] = {}  # held message's sender -> when its path was requested
        self._path_tokens = float(PATH_REQUEST_BURST)
        self._path_tokens_at = time.monotonic()
        self._propagation_nodes: dict[bytes, float] = {}  # heard propagation node -> when
        self._configured_pn = parse_address(config.lxmf_propagation_node) if config.lxmf_propagation_node else None
        self._current_pn: bytes | None = None
        self._sync_started = float("-inf")
        self.display_name = config.lxmf_display_name or DEFAULT_DISPLAY_NAME
        os.makedirs(os.path.join(config.root, "lxmf"), mode=0o700, exist_ok=True)  # it holds ratchet keys
        self.identity = load_or_create_identity(lxmf_identity_path(config))
        with _keeping_signal_handlers():
            # storagepath + "/lxmf" is where LXMF keeps its state: <rnfs dir>/lxmf/
            self.router = LXMF.LXMRouter(identity=self.identity, storagepath=config.root,
                                         delivery_limit=DELIVERY_LIMIT_KB, autopeer=False)
        # Announce without compression support: rnfs refuses compressed Resources (section 3), so senders
        # that honour the announce send uncompressed; older ones are bounded below
        self.router.get_announce_app_data = self._announce_app_data
        original_began = self.router.delivery_resource_transfer_began

        def began(resource):
            """Bound decompression of an incoming message to the delivery limit (rnfs sets RNS's to 0)."""
            resource.max_decompressed_size = DELIVERY_LIMIT_KB * 1000
            original_began(resource)

        self.router.delivery_resource_transfer_began = began
        # LXMF calls this again itself once the propagation node's link is up (or its path known): bound the
        # link's Resources before any request on it can be answered
        original_request = self.router.request_messages_from_propagation_node

        def request_messages(identity, max_messages=SYNC_MAX_MESSAGES):
            """LXMRouter.request_messages_from_propagation_node with the propagation node link bounded."""
            self._bound_propagation_downloads()
            original_request(identity, max_messages)
            self._bound_propagation_downloads()

        self.router.request_messages_from_propagation_node = request_messages
        self.destination = self.router.register_delivery_identity(self.identity, display_name=self.display_name)
        # The delivery destination takes link requests too: bound its half-open links as rnfs.dht's (section 3)
        guards.limit_half_open(self.destination, config.max_incoming_links)
        self.address = bytes(self.destination.hash)
        self.router.register_delivery_callback(self._delivered)
        self._announce_handler = None
        if self._configured_pn is not None:
            self._use_propagation_node(self._configured_pn)
        else:
            self._announce_handler = _PropagationAnnounces(self)
            RNS.Transport.register_announce_handler(self._announce_handler)

    def _announce_app_data(self, destination_hash):
        """LXMF's announce data, [display name, stamp cost, supported functions], with no functions listed."""
        destination = self.router.delivery_destinations.get(destination_hash)
        if destination is None:
            return None
        name = destination.display_name.encode("utf-8") if destination.display_name else None
        cost = destination.stamp_cost if isinstance(destination.stamp_cost, int) and 0 < destination.stamp_cost < 255 \
            else None
        return self.msgpack.packb([name, cost, []])

    def announce(self):
        """Announce the delivery destination (with the display name)."""
        self.router.announce(self.destination.hash)
        _log(f"announced {self.address.hex()}", LOG_VERBOSE)

    def stop(self):
        """Persist LXMF's state and stop listening for propagation node announces."""
        if self._announce_handler is not None:
            try:
                self.RNS.Transport.deregister_announce_handler(self._announce_handler)
            except Exception:  # noqa: BLE001
                pass
        self.router.exit_handler()

    # Propagation nodes

    def heard_propagation_node(self, destination_hash: bytes, app_data):
        """Remember a propagation node that announced itself enabled; forget one that announced it isn't."""
        if not self.LXMF.pn_announce_data_is_valid(app_data):
            return
        enabled = self.msgpack.unpackb(app_data)[2] is True
        with self._lock:
            if not enabled:
                self._propagation_nodes.pop(destination_hash, None)
                return
            self._propagation_nodes[destination_hash] = time.time()
            if len(self._propagation_nodes) > PROPAGATION_NODES_LIMIT:
                oldest = min(self._propagation_nodes, key=self._propagation_nodes.get)
                del self._propagation_nodes[oldest]

    def nearest_propagation_node(self) -> bytes | None:
        """The nearest propagation node heard announcing itself within PROPAGATION_NODE_AGE (not selected)."""
        horizon = time.time() - PROPAGATION_NODE_AGE
        with self._lock:
            candidates = [h for h, heard in self._propagation_nodes.items() if heard >= horizon]
            heard = dict(self._propagation_nodes)
        if not candidates:
            return None
        return min(candidates, key=lambda h: (self.RNS.Transport.hops_to(h), -heard[h]))

    def propagation_node(self, nearest: bool = False) -> bytes | None:
        """The configured propagation node; else, if nearest, the nearest one heard (made LXMF's), else None.

        A node heard announcing itself is anyone's, so it is used only when the user asks: it would get the
        messages sent through it, and could send any amount back on every download.
        """
        if self._configured_pn is not None:
            return self._configured_pn
        best = self.nearest_propagation_node() if nearest else None
        if best is not None:
            self._use_propagation_node(best)
        return best

    def _use_propagation_node(self, destination_hash: bytes):
        """Make destination_hash LXMF's outbound propagation node."""
        if destination_hash != self._current_pn:
            self.router.set_outbound_propagation_node(destination_hash)
            self._current_pn = destination_hash
            _log(f"using propagation node {destination_hash.hex()}", LOG_INFO)

    def sync(self, nearest: bool = False) -> bool:
        """Start downloading waiting messages from the propagation node (propagation_node(nearest)); False if
        there is none or a download runs."""
        node = self.propagation_node(nearest)
        if node is None:
            return False
        router = self.router
        busy = router.PR_PATH_REQUESTED <= router.propagation_transfer_state < router.PR_COMPLETE
        if busy and time.monotonic() - self._sync_started < SYNC_TIMEOUT:
            return False
        if busy:
            router.cancel_propagation_node_requests()  # stuck (e.g. a link that never came up): start over
        router.acknowledge_sync_completion(reset_state=True)
        self._sync_started = time.monotonic()
        router.request_messages_from_propagation_node(self.identity, SYNC_MAX_MESSAGES)
        return True

    # Sending

    def wait_identity(self, address: bytes, timeout: float) -> bool:
        """True once RNS knows address's identity, requesting a path if it doesn't, waiting up to timeout."""
        RNS = self.RNS
        if RNS.Identity.recall(address) is not None:
            return True
        RNS.Transport.request_path(address)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            time.sleep(0.25)
            if RNS.Identity.recall(address) is not None:
                return True
        return False

    def send(self, address: bytes, content: str, fields: dict, propagated: bool, nearest: bool = False) -> bytes:
        """Queue an LXMF message (propagated: through propagation_node(nearest)); returns its hash."""
        RNS, LXMF = self.RNS, self.LXMF
        identity = RNS.Identity.recall(address)
        if identity is None:
            from rnfs.errors import NotFound
            raise NotFound(f"LXMF address {address.hex()} is unknown here")
        if propagated:
            node = self.propagation_node(nearest)
            if node is None:
                raise ValueError(_NO_PN_HELP)
        destination = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE, "lxmf", "delivery")
        method = LXMF.LXMessage.PROPAGATED if propagated else LXMF.LXMessage.DIRECT
        message = LXMF.LXMessage(destination, self.destination, content, title="", fields=fields,
                                 desired_method=method)
        self.router.handle_outbound(message)
        with self._lock:
            self._outbound[bytes(message.hash)] = [message, None, None]
        return bytes(message.hash)

    def poll(self) -> list[tuple[bytes, str, float | None]]:
        """Changes in outbound messages' states since the last poll, and held messages retried."""
        changes = []
        with self._lock:
            for message_hash, record in list(self._outbound.items()):
                message = record[0]
                state = STATE_NAMES.get(message.state, "outbound")
                progress = round(message.progress or 0.0, 2)
                if (state, progress) != (record[1], record[2]):
                    record[1], record[2] = state, progress
                    changes.append((message_hash, state, progress))
                propagated = message.method == self.LXMF.LXMessage.PROPAGATED
                if state in FINAL_STATES or (propagated and state == "sent"):
                    del self._outbound[message_hash]
        self._retry_held()
        self._bound_propagation_downloads()
        return changes

    def _bound_propagation_downloads(self):
        """Bound the propagation node link's Resources (LXMF passes no max_response_size): see _pn_resource."""
        link = self.router.outbound_propagation_link
        if link is not None and not getattr(link, "rnfs_bounded", False):
            link.set_resource_started_callback(self._pn_resource)
            link.rnfs_bounded = True

    def _pn_resource(self, resource):
        """A Resource starting on the propagation node link: refuse (tearing the link down) one larger than
        MAX_PN_RESOURCE or of several segments, and bound its decompression (rnfs sets RNS's bound to 0)."""
        resource.max_decompressed_size = MAX_PN_RESOURCE
        sizes = (resource.get_data_size(), resource.get_transfer_size())
        if resource.get_segments() != 1 or any(not isinstance(n, int) or n > MAX_PN_RESOURCE for n in sizes):
            _log(f"the propagation node sent a {sizes[0]}-byte response in {resource.get_segments()} segment(s), "
                 f"over the {MAX_PN_RESOURCE}-byte bound; download stopped", LOG_WARNING)
            resource.cancel()
            resource.link.teardown()

    # Receiving

    def _incoming(self, message) -> Incoming:
        """An Incoming from a validated LXMessage."""
        display_name = None
        try:
            display_name = self.LXMF.display_name_from_app_data(self.RNS.Identity.recall_app_data(message.source_hash))
        except Exception:  # noqa: BLE001 - untrusted announce data
            pass
        return Incoming(bytes(message.source_hash), bytes(message.hash), message.timestamp, message.title,
                        message.content, message.fields, METHOD_NAMES.get(message.method, "unknown"),
                        display_name if isinstance(display_name, str) else None)

    def _delivered(self, message):
        """LXMF's delivery callback (on its threads): pass validated messages on, hold those from unknown senders."""
        try:
            if message.signature_validated:
                self.feature.received(self._incoming(message))
            elif message.unverified_reason == self.LXMF.LXMessage.SOURCE_UNKNOWN and message.packed:
                # Unsigned as far as we can tell: anyone can send these claiming any source, so the queue
                # keeps what it holds (the newest is dropped) unless a trusted sender is claimed, a claimed
                # sender gets only HOLD_PER_SENDER places, and path requests are rate limited
                source, packed = bytes(message.source_hash), bytes(message.packed)
                if not self.feature.accepts(source):
                    # received() would ignore it: holding it would only let forged sources fill the queue
                    _log(f"dropped a message from unknown sender {source.hex()}: not in lxmf_accept_from",
                         LOG_VERBOSE)
                    return
                with self._lock:
                    queued = self._hold(source, packed, message.method)
                if not queued:
                    _log(f"dropped a message from unknown sender {source.hex()}: already held, "
                         f"{HOLD_PER_SENDER} held from it, or {HOLD_LIMIT} held", LOG_VERBOSE)
                    return
                self._request_path(source)
                _log(f"holding a message from {source.hex()} until its identity is known", LOG_VERBOSE)
            else:
                _log(f"dropped a message with an invalid signature from {bytes(message.source_hash).hex()}",
                     LOG_NOTICE)
        except Exception as e:  # noqa: BLE001
            _log(f"handling a received message failed: {e!r}", LOG_ERROR)

    def _hold(self, source: bytes, packed: bytes, method) -> bool:
        """Hold a message claiming source until its identity is known (lock held); False if it is dropped: the
        same message is held, HOLD_PER_SENDER claiming source are, or HOLD_LIMIT are. A message claiming a
        trusted sender (feature.trusted) then takes the place of the newest held for the untrusted sender
        holding the most."""
        if any(p == packed for _, p, _ in self._held):
            return False
        claims: dict[bytes, int] = {}
        for _, p, _ in self._held:
            claims[p[ID_LENGTH:2 * ID_LENGTH]] = claims.get(p[ID_LENGTH:2 * ID_LENGTH], 0) + 1
        if claims.get(source, 0) >= HOLD_PER_SENDER:
            return False
        if len(self._held) >= HOLD_LIMIT:
            if not self.feature.trusted(source):
                return False
            others = [item for item in reversed(self._held)
                      if not self.feature.trusted(item[1][ID_LENGTH:2 * ID_LENGTH])]
            if not others:
                return False
            victim = max(others, key=lambda item: claims[item[1][ID_LENGTH:2 * ID_LENGTH]])  # newest of most
            self._held = [item for item in self._held if item is not victim]
            _log(f"dropped a held message from unknown sender {victim[1][ID_LENGTH:2 * ID_LENGTH].hex()} for one "
                 f"from trusted sender {source.hex()}", LOG_VERBOSE)
        self._held.append((time.monotonic() + HOLD_TIME, packed, method))
        return True

    def _request_path(self, destination: bytes) -> bool:
        """Request a path to a held message's sender, at most once per HOLD_TIME per sender and within the
        node-wide token bucket; False if that didn't allow it now (_retry_held tries again)."""
        now = time.monotonic()
        with self._lock:
            for known, at in list(self._path_requested.items()):
                if now - at >= HOLD_TIME:
                    del self._path_requested[known]
            if destination in self._path_requested:
                return False
            self._path_tokens = min(float(PATH_REQUEST_BURST),
                                    self._path_tokens + (now - self._path_tokens_at) / PATH_REQUEST_INTERVAL)
            self._path_tokens_at = now
            if self._path_tokens < 1:
                return False
            self._path_tokens -= 1
            self._path_requested[destination] = now
        self.RNS.Transport.request_path(destination)
        return True

    def _retry_held(self):
        """Validate held messages whose sender's identity is now known; drop those that waited too long: their
        path request went unanswered for HOLD_PATH_WAIT (counted from their arrival if that is later), or
        HOLD_TIME passed. Trusted senders' messages go first, so they get the path-request tokens first.

        Each stays in the queue until it is done with, so messages arriving meanwhile see it (as a duplicate,
        and against HOLD_LIMIT)."""
        with self._lock:
            held = list(self._held)
        now = time.monotonic()
        held.sort(key=lambda item: not self.feature.trusted(item[1][ID_LENGTH:2 * ID_LENGTH]))
        for item in held:
            deadline, packed, method = item
            source = packed[ID_LENGTH:2 * ID_LENGTH]
            known = self.RNS.Identity.recall(source) is not None
            if not known and now < deadline:
                with self._lock:
                    asked = self._path_requested.get(source)
                if asked is None or now < max(asked, deadline - HOLD_TIME) + HOLD_PATH_WAIT:
                    self._request_path(source)  # if the rate limit held it back before
                    continue
            try:
                if not known:
                    why = "its path request got no answer" if now < deadline else "its identity never became known"
                    _log(f"dropped a message from {source.hex()}: {why}", LOG_NOTICE)
                    continue
                message = self.LXMF.LXMessage.unpack_from_bytes(packed)
                message.method = method
                if message.signature_validated:
                    self.feature.received(self._incoming(message))
                else:
                    _log(f"dropped a message with an invalid signature from {source.hex()}", LOG_NOTICE)
            except Exception as e:  # noqa: BLE001
                _log(f"a held message could not be read: {e!r}", LOG_VERBOSE)
            finally:
                with self._lock:
                    self._held = [other for other in self._held if other is not item]
