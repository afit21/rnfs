"""rnfs links (specification section 13): `rnfs:` URIs, and the LXMF link-map field that carries them in messages.

- `rnfs:<cid>[/path][?from=<node ID>]` and `rnfs:name/<name>[/path][?from=<node ID>]`, path names percent-encoded
  as UTF-8 (parse_uri / format_uri). rnfs.Node's and rnfs.Client's targets may be one (interface.Rnfs._target).
- The link-map field: a message carries its links in LXMF's custom fields (FIELD_CUSTOM_TYPE "rnfs",
  FIELD_CUSTOM_DATA a list of link maps) for clients that know rnfs (encode_field / decode_field), as the LXMF
  feature (rnfs/features/lxmf.py) sends them. tests/test_wire.py checks its bytes against the vectors of earlier
  releases.

Everything decoded is untrusted: parsed strictly and capped. Standard library only (no RNS: the CLI and
rnfs.Client import this).
"""
import re
from dataclasses import dataclass
from urllib.parse import quote, unquote_to_bytes

from rnfs import ids
from rnfs.constants import ID_LENGTH, MAX_DEPTH
from rnfs.content.cid import Cid

# LXMF's fields for embedding other protocols (LXMF/LXMF.py): a type identifier and its payload. LXMF reserves
# the unallocated fields below 0x80 for its own future use and calls those above 0xFF unstable
FIELD_CUSTOM_TYPE = 0xFB
FIELD_CUSTOM_DATA = 0xFC
CUSTOM_TYPE = "rnfs"  # FIELD_CUSTOM_TYPE value; an incompatible change of FIELD_CUSTOM_DATA uses a new one

URI_SCHEME = "rnfs:"
NAME_SEGMENT = "name"
MAX_URI_LENGTH = 4096  # characters: 64 names of up to 255 bytes, percent-encoded, don't fit in a message anyway
MAX_PATH_NAMES = MAX_DEPTH  # a path can't be deeper than the DAG it walks
MAX_LINKS = 8  # links in one message's field
MAX_LINK_KEYS = 16  # keys in one link map, known or not

_SEGMENT = re.compile(r"(?:[A-Za-z0-9\-._~!$&'()*+,;=:@]|%[0-9A-Fa-f]{2})+")
_QUERY_VALUE = re.compile(r"(?:[A-Za-z0-9\-._~!$'()*+,;=:@/]|%[0-9A-Fa-f]{2})*")  # "&" separates parameters


def _check_name(name: str):
    """dag.check_name, imported here because rnfs.content.dag loads RNS's msgpack."""
    from rnfs.content import dag
    dag.check_name(name)


@dataclass(frozen=True)
class Link:
    """One rnfs link: a CID or a name, a path below it, and optionally its size and a node that has it."""

    cid: Cid | None = None  # with a name: the CID it pointed at when sent
    name: bytes | None = None  # 16-byte rnfs name
    path: tuple[str, ...] = ()
    size: int | None = None  # content bytes, as the sender knew them (untrusted; fetches verify sizes)
    from_node: bytes | None = None  # the sender's rnfs node ID: a provider hint, like `get --from`

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
        if len(segments) < 2 or not ids.HEX16.fullmatch(segments[1]):
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
                if not ids.HEX16.fullmatch(value):
                    raise ValueError("from must be a node ID: 32 hex characters")
                from_node = bytes.fromhex(value)
    return Link(cid, name, tuple(names), None, from_node)


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
        if len(format_uri(link)) > MAX_URI_LENGTH:  # parse_uri refuses it anyway
            raise ValueError(f"a link is longer than {MAX_URI_LENGTH} characters as a URI")
        links.append(link)
    return links
