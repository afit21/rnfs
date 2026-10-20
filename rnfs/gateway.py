"""HTTP gateway of a running node (specification section 11), bound to 127.0.0.1, separate from the API.

GET/HEAD /rnfs/<cid>[/path]   content by CID: immutable, ETag "<cid>" of the target
GET/HEAD /name/<name>[/path]  content a name points at, resolved with for_gateway=True

Subdomain hosts (sites), an origin per root so a page can read its own DAG with fetch():
GET/HEAD http://<k + base36 CID>.rnfs.localhost:<port>/[path]  as /rnfs/<cid>/[path]
GET/HEAD http://<name>.name.localhost:<port>/[path]            as /name/<name>/[path]

Behaviour:
- Path segments are percent-decoded one by one; a decoded segment can't contain "/" or NUL, so every
  segment is exactly one name.
- Files: Content-Type from ?filename=, else the path's last name, else sniffed from the first bytes (on site
  hosts, never from ?filename=, and a file inside the site's DAG never sniffed as a page: see Sites). The
  user's own HEAD, or range not starting at byte 0, uses the first bytes only if they are stored (else the
  name, or application/octet-stream), so it never fetches what it doesn't send; the deny list applies to
  them as to any read (451). A web page's request (see
  limited() below) always reads them, fetching them if need be, so what a page can observe of the type
  (e.g. through nosniff or opaque-response blocking) never depends on what is stored.
  Single byte ranges (206, 416, Accept-Ranges; If-Range honoured); several ranges get the whole file.
- Directories: without a trailing slash, 301 to the slash form; with one, index.html if present, else
  an HTML-escaped listing with percent-encoded links.
- /rnfs/ responses carry ETag "<cid>" and "Cache-Control: public, max-age=31536000, immutable";
  If-None-Match answers 304. /name/ responses are mutable and carry neither.
- Every response, errors and redirects included, carries X-Content-Type-Options: nosniff and the spec's
  sandboxing Content-Security-Policy. With gateway_scripts off, allow-scripts and 'unsafe-eval' are
  dropped and script-src 'none' added, so no script runs even if a browser ignores the sandbox.
- No response carries Access-Control-* headers. Path gateway pages run in a sandboxed, opaque origin, so they
  can load files of their DAG as subresources (img, script, link…) but can't read gateway responses with fetch()
  or XMLHttpRequest, and neither can any other web page. Sending Access-Control-Allow-Origin for
  `Origin: null` would let every site's sandboxed iframe or data: page read the gateway.
- Sites: parse_host matches the Host header exactly (lowercased, as browsers send it), apart from the API's own
  check (api.host_allowed, unchanged); the base36 CID becomes a Cid at once, so ETags, listings and logs show
  base32. A site's page keeps its origin (CSP_SITE_SCRIPTS: sandbox with allow-same-origin, frame-ancestors
  'self', plus Origin-Agent-Cluster), so its fetch() of its own files is same-origin and needs no CORS header;
  every other origin is still refused by the browser for want of one. Its path is a DAG path and nothing else
  (no /rnfs/ or /name/ prefix, no ?format=), every segment a valid name, and its redirects are rebuilt from
  the decoded names. Requests for a service worker script (Service-Worker header) get 403: a worker on a
  name's site could go on serving old content after the name moves. A site's page runs script in the site's own
  origin, so a file put into its DAG mustn't become one unless the DAG names it so (REV6): ?filename= only names a
  download (Content-Disposition: attachment, the type still from the path's own name), and a file reached by a path
  inside the site's DAG is sniffed only to the passive types of SITE_SNIFFED_TYPES (never HTML, SVG, XML or PDF;
  anything else is application/octet-stream). A site whose root is itself a file is sniffed as on the path gateway:
  that file is its origin's only content. Everything else is as on the path gateway.
- A GatewayError (the gateway's own refusals) or NotFound says what went wrong. Every other error (a 400,
  413, 422, 503 or 507 from the node, 451 for content the deny list blocks, or a 500) gets only a fixed
  message for its status: a node error's text
  can name local paths, disk space or limits. The detail goes to the log (500: as an error; others verbose).
- Requests that aren't the user's own navigation (Sec-Fetch-Site present and not "none") may hold at most
  gateway_fetches network fetches or name resolutions at a time; over that, 503 with Retry-After.
  Content already stored (node.is_local) is served without limit. A request with no Sec-Fetch-Site is
  treated as the user's own only if its User-Agent doesn't start with "Mozilla/": browsers with Fetch
  Metadata send the header to 127.0.0.1/localhost (potentially trustworthy origins), so its absence means
  a local tool such as curl, run by the user, or a browser without Fetch Metadata (Safari < 16.4, Firefox
  < 90), whose every request is then treated as a web page's (no ?format=, held to gateway_fetches).
- Everything is requested with for_gateway=True: fetched content is cache, never provided, and names go
  only into the capped table. Name resolutions are reused for NAME_CACHE_TIME, so a page's many
  subresources don't each start a resolution against gateway_fetches.
- Connections are limited as for the API (rnfs/api.py): the request must arrive within request_timeout, at
  most max_connections are served at once (more get a 503 carrying the headers above), and a response write
  may block response_timeout, so clients that stop reading can't hold every slot for ever.
- With the debug log (rnfs/debuglog.py), each request is numbered and logged there with the headers that decide
  how it is served (DEBUG_HEADERS), then its status, bytes written and time; what the gateway decided on the way
  (name resolutions, stored or fetched, index or listing, type and range) is logged at LOG_DEBUG.
"""
import codecs
import html
import itertools
import mimetypes
import re
import threading
import time
from collections import OrderedDict
from urllib.parse import parse_qs, quote, unquote, urlsplit

from rnfs.api import LocalServer, TimedHandler, error_status, host_allowed, loggable, parse_hex_id, raw_response
from rnfs import debuglog, ids
from rnfs import gateway_formats  # --- content ---
from rnfs.content.cid import Cid, from_base36
from rnfs.content.seal import is_sealed  # --- content ---
from rnfs.errors import NotFound
from rnfs.log import LOG_DEBUG, LOG_ERROR, LOG_VERBOSE, log

CSP_SCRIPTS = ("sandbox allow-scripts allow-forms allow-popups; "
               "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob:; form-action 'self'")
CSP_NO_SCRIPTS = ("sandbox allow-forms allow-popups; "
                  "default-src 'self' 'unsafe-inline' data: blob:; script-src 'none'; form-action 'self'")
# On subdomain hosts (sites): the page keeps its own origin (allow-same-origin), so it can read its own DAG with
# fetch(), and only pages of its own origin may frame it
CSP_SITE_SCRIPTS = ("sandbox allow-same-origin allow-scripts allow-forms allow-popups; "
                    "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob:; form-action 'self'; "
                    "frame-ancestors 'self'")
CSP_SITE_NO_SCRIPTS = ("sandbox allow-same-origin allow-forms allow-popups; "
                       "default-src 'self' 'unsafe-inline' data: blob:; script-src 'none'; form-action 'self'; "
                       "frame-ancestors 'self'")
SITE_DOMAINS = {"rnfs": "rnfs.localhost", "name": "name.localhost"}  # site kind -> the domain its hosts are under
HOST_REFUSED = ("Host header must be 127.0.0.1:<port> or localhost:<port>, or <k + base36 CID>.rnfs.localhost:<port> "
                "or <name>.name.localhost:<port>")
IMMUTABLE = "public, max-age=31536000, immutable"
RETRY_AFTER = "10"  # seconds a limited request should wait before retrying
SNIFF_SIZE = 512  # bytes read to guess a type, as in the WHATWG MIME sniffing standard
NAME_CACHE_TIME = 60.0
NAME_CACHE_SIZE = 1000
INDEX = "index.html"
# What an error from the node says on the gateway, by status: never its own text (see the module docstring). Every
# status of rnfs.errors.STATUSES has one, and 500; a NotFound's own text is shown, this only if it has none
ERROR_MESSAGES = {400: "invalid request", 404: "not found", 413: "too large for this node's download limits",
                  451: "unavailable: this node's deny list blocks this content",
                  422: "invalid content: it breaks the rnfs format's rules", 503: "the node is busy; try again shortly",
                  507: "not enough free disk space on this node", 500: "internal error; see the rnfs daemon's log"}
RANGE_NUMBER = re.compile(r"[0-9]{1,18}")  # ASCII digits only (str.isdigit() takes "¹²³"), and below 2**63
# Request headers the debug log shows: those that decide how a request is answered
DEBUG_HEADERS = ("Host", "Range", "If-Range", "If-None-Match", "Sec-Fetch-Site", "Sec-Fetch-Mode", "Sec-Fetch-Dest",
                 "User-Agent")
_request_numbers = itertools.count(1)  # the debug log's numbers for gateway requests

# Types some platforms' mimetypes tables lack or get wrong (on Windows, mimetypes also reads the registry,
# which installed software edits), so the commonest web types are fixed here too
EXTRA_TYPES = {".html": "text/html", ".htm": "text/html", ".css": "text/css", ".txt": "text/plain",
               ".js": "text/javascript", ".mjs": "text/javascript", ".wasm": "application/wasm",
               ".webp": "image/webp", ".avif": "image/avif", ".svg": "image/svg+xml", ".json": "application/json",
               ".md": "text/markdown", ".webm": "video/webm", ".opus": "audio/ogg", ".woff2": "font/woff2"}

# What sniffing may give a file inside a site's DAG (REV6), whose pages keep their origin: only types a browser never
# renders as a document running script there, so a file put into a site's DAG under a name that gives no type can't
# run script in the site's origin (a site whose root is a file is sniffed freely: it is its origin's only content). Anything else sniff() returns (HTML, SVG, PDF...) is served as SITE_UNSNIFFED
SITE_SNIFFED_TYPES = frozenset({
    "image/png", "image/jpeg", "image/gif", "image/webp", "audio/ogg", "audio/mpeg", "audio/flac", "audio/wav",
    "video/webm", "video/mp4", "font/woff2", "application/wasm", "text/plain; charset=utf-8"})
SITE_UNSNIFFED = "application/octet-stream"

# Tags that make a document HTML when it starts with them (WHATWG MIME sniffing, "identifying HTML")
HTML_TAGS = (b"<!doctype html", b"<html", b"<head", b"<script", b"<iframe", b"<h1", b"<div", b"<font", b"<table",
             b"<a", b"<style", b"<title", b"<b", b"<body", b"<br", b"<p", b"<!--")


def type_from_name(name: str | None) -> str | None:
    """The Content-Type a file name suggests, or None."""
    if not name:
        return None
    dot = name.rfind(".")
    if dot > 0 and name[dot:].lower() in EXTRA_TYPES:
        return EXTRA_TYPES[name[dot:].lower()]
    return mimetypes.guess_type(name, strict=False)[0]


def sniff(head: bytes) -> str:
    """Guess a Content-Type from a file's first bytes: a few binary signatures, HTML, SVG, UTF-8 text."""
    signatures = [(b"\x89PNG\r\n\x1a\n", "image/png"), (b"\xff\xd8\xff", "image/jpeg"), (b"GIF87a", "image/gif"),
                  (b"GIF89a", "image/gif"), (b"%PDF-", "application/pdf"), (b"OggS\x00", "audio/ogg"),
                  (b"\x1a\x45\xdf\xa3", "video/webm"), (b"ID3", "audio/mpeg"), (b"fLaC", "audio/flac"),
                  (b"wOF2", "font/woff2"), (b"\x00asm", "application/wasm")]
    for signature, content_type in signatures:
        if head.startswith(signature):
            return content_type
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "image/webp"
    if head[:4] == b"RIFF" and head[8:12] == b"WAVE":
        return "audio/wav"
    if head[4:8] == b"ftyp":
        return "video/mp4"

    text = head[3:] if head.startswith(b"\xef\xbb\xbf") else head
    lowered = text.lstrip(b" \t\n\r\x0c").lower()
    for tag in HTML_TAGS:
        # The tag must end there: "<b>" or "<b " is HTML, "<base64...>" isn't
        if lowered.startswith(tag) and lowered[len(tag):len(tag) + 1] in (b" ", b">", b"\t", b"\n", b"\r", b"\x0c"):
            return "text/html"
    if (lowered.startswith(b"<svg") or lowered.startswith(b"<?xml")) and b"<svg" in lowered:
        return "image/svg+xml"
    if is_utf8_text(text):
        return "text/plain; charset=utf-8"
    return "application/octet-stream"


def is_utf8_text(head: bytes) -> bool:
    """True if head is UTF-8 without binary control characters (a sequence cut off at the end is allowed)."""
    try:
        # final=False accepts a character cut off at the end of the sniffed prefix, and nothing else
        text = codecs.getincrementaldecoder("utf-8")().decode(head, final=False)
    except UnicodeDecodeError:
        return False
    return not any(ord(c) < 32 and c not in "\t\n\r\x0c\x1b" for c in text)


def parse_range(header: str | None, size: int) -> tuple[int, int] | None | str:
    """A single `bytes=` range as (start, end inclusive); None to serve everything; "unsatisfiable" for 416.

    Malformed headers and multiple ranges are ignored (the whole file is sent), as RFC 9110 allows.
    """
    if not header or not header.startswith("bytes="):
        return None
    spec = header[len("bytes="):].strip()
    if "," in spec or "-" not in spec:
        return None
    first, last = (part.strip() for part in spec.split("-", 1))
    if not (RANGE_NUMBER.fullmatch(first) or first == "") or not (RANGE_NUMBER.fullmatch(last) or last == "") \
            or first == last == "":
        return None
    if first == "":  # suffix: the last N bytes
        suffix = int(last)
        if suffix == 0 or size == 0:
            return "unsatisfiable"
        return max(size - suffix, 0), size - 1
    start = int(first)
    if last and int(last) < start:
        return None  # invalid, so ignored
    if start >= size:
        return "unsatisfiable"
    return start, min(int(last), size - 1) if last else size - 1


def etag_matches(header: str | None, etag: str) -> bool:
    """True if an If-None-Match header matches etag (weak comparison, or "*")."""
    if not header:
        return False
    for tag in header.split(","):
        tag = tag.strip()
        if tag == "*" or tag.removeprefix("W/") == etag:
            return True
    return False


def printable_size(size: int) -> str:
    """A size for listings: bytes, KiB, MiB or GiB."""
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024 or unit == "GiB":
            return f"{size} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024


def listing_html(title: str, entries: list[dict], has_parent: bool) -> bytes:
    """An HTML directory listing. Every name is HTML-escaped and every link percent-encoded."""
    rows = []
    if has_parent:
        rows.append('<tr><td><a href="../">../</a></td><td></td><td></td><td></td></tr>')
    for entry in entries:
        is_dir = entry["kind"] == "directory"
        # quote(safe="") also encodes ":" so a name like "javascript:x" can't become a URL scheme
        href = "./" + quote(entry["name"], safe="") + ("/" if is_dir else "")
        label = html.escape(entry["name"]) + ("/" if is_dir else "")
        rows.append(f'<tr><td><a href="{html.escape(href)}">{label}</a></td><td>{html.escape(entry["kind"])}</td>'
                    f'<td>{html.escape(printable_size(entry["size"]))}</td>'
                    f'<td><code>{html.escape(entry["cid"])}</code></td></tr>')
    page = (f'<!DOCTYPE html>\n<html><head><meta charset="utf-8"><title>{html.escape(title)}</title>\n'
            '<style>body{font-family:sans-serif;margin:1em}td{padding:0 1em 0 0}code{font-size:.85em}</style>\n'
            f'</head><body><h1>{html.escape(title)}</h1>\n<table>\n' + "\n".join(rows) + '\n</table></body></html>\n')
    return page.encode("utf-8")


class GatewayError(Exception):
    """A request the gateway answers with an error: status, message and extra headers."""

    def __init__(self, status: int, message: str, headers: dict | None = None):
        """An error answered with status, message and headers (none if None)."""
        super().__init__(message)
        self.status = status
        self.headers = headers or {}


def parse_host(host: str | None, port: int) -> tuple[str, Cid | bytes] | None:
    """What a request's Host header names on the gateway listening on port.

    None: the path gateway, 127.0.0.1:<port> or localhost:<port> exactly as the API allows (api.host_allowed).
    ("rnfs", Cid): <k + base36 CID>.rnfs.localhost:<port>. ("name", 16 bytes): <32 hex>.name.localhost:<port>.
    Hosts are compared lowercased, as browsers send them; nothing else matches: no other label, no IP address,
    no trailing dot, no missing or other port. Raises GatewayError 403 for any other host (DNS rebinding) and 400
    for a site host whose CID or name is invalid. The base36 CID is turned into a Cid here, so it is written in
    base32 from then on (specification section 4).
    """
    if host_allowed(host, port):
        return None
    if host is None or not host.isascii():  # so lower() maps nothing outside ASCII onto it
        raise GatewayError(403, HOST_REFUSED)
    label, _, domain = host.lower().partition(".")
    kind = next((kind for kind, name in SITE_DOMAINS.items() if domain == f"{name}:{port}"), None)
    if kind is None:
        raise GatewayError(403, HOST_REFUSED)
    if kind == "rnfs":
        try:
            return kind, from_base36(label)
        except ValueError as e:
            raise GatewayError(400, str(e)) from None
    if not ids.HEX16_LOWER.fullmatch(label):
        raise GatewayError(400, f"invalid name {label!r} in host: must be 32 hex characters")
    return kind, bytes.fromhex(label)


class FetchLimiter:
    """At most `limit` concurrent network fetches or name resolutions for requests that aren't navigation."""

    def __init__(self, limit: int):
        """Allow limit fetches at once."""
        self.limit = limit
        self.active = 0
        self.lock = threading.Lock()

    def try_acquire(self) -> bool:
        """Take a slot if one is free."""
        with self.lock:
            if self.active >= self.limit:
                return False
            self.active += 1
            return True

    def release(self):
        """Give a slot back."""
        with self.lock:
            self.active -= 1


class NameCache:
    """Recent name resolutions (name → CID), reused for NAME_CACHE_TIME. LRU, at most NAME_CACHE_SIZE."""

    def __init__(self, clock=time.monotonic):
        """An empty cache, aged by clock."""
        self.entries: OrderedDict[str, tuple[float, str]] = OrderedDict()
        self.lock = threading.Lock()
        self.clock = clock

    def get(self, name: str) -> str | None:
        """The CID name resolved to recently, or None."""
        with self.lock:
            entry = self.entries.get(name)
            if entry is None or self.clock() - entry[0] > NAME_CACHE_TIME:
                self.entries.pop(name, None)
                return None
            self.entries.move_to_end(name)
            return entry[1]

    def put(self, name: str, cid: str):
        """Remember a resolution."""
        with self.lock:
            self.entries[name] = (self.clock(), cid)
            self.entries.move_to_end(name)
            while len(self.entries) > NAME_CACHE_SIZE:
                self.entries.popitem(last=False)


class _CountingWriter:
    """A response stream counting the bytes written to it, for the debug log; otherwise the stream itself."""

    def __init__(self, stream):
        """Count what is written to stream."""
        self._stream = stream
        self.count = 0

    def write(self, data) -> int:
        """Write data and count it."""
        written = self._stream.write(data)
        self.count += len(data)
        return written

    def __getattr__(self, name):
        """Everything else from the stream itself."""
        return getattr(self._stream, name)


class GatewayServer(LocalServer):
    """The gateway for node (an RnfsNode, or anything with its facade methods), using config's gateway settings."""

    max_connections = 64  # a page's subresources come in parallel; browsers open about 6 per host anyway
    # A paused video stops reading, so this is generous; it only stops a client holding a slot for ever
    response_timeout = 300.0

    def __init__(self, node, config, port: int):
        """Bind to 127.0.0.1:port; requests are served once start() is called."""
        super().__init__(GatewayHandler, port, node=node, config=config, limiter=FetchLimiter(config.gateway_fetches),
                         names=NameCache())


class GatewayHandler(TimedHandler):
    """Handles one gateway request; node, config, limiter and names are set on the subclass GatewayServer creates."""

    node = None
    config = None
    limiter: FetchLimiter = None
    names: NameCache = None
    server_version = "rnfs"
    sys_version = ""
    status_sent: int | None = None  # the response's status, once sent (for the debug log)
    site: tuple[str, Cid | bytes] | None = None  # the subdomain host's site (parse_host), or None

    def log_message(self, format, *args):
        """Log nothing: RNS does the node's logging, and per-request lines would drown it out."""

    @classmethod
    def busy_response(cls) -> bytes:
        """503 for a connection over max_connections, with the headers every gateway response carries."""
        return raw_response("503 Service Unavailable", {
            "Content-Type": "text/plain; charset=utf-8", "Retry-After": RETRY_AFTER,
            "X-Content-Type-Options": "nosniff",
            "Content-Security-Policy": CSP_SCRIPTS if cls.config.gateway_scripts else CSP_NO_SCRIPTS,
        }, b"too many gateway connections; try again shortly\n")

    # Responses

    def send_response(self, code: int, message: str | None = None):
        """Send the status line, noting the status."""
        self.status_sent = code
        super().send_response(code, message)

    def end_headers(self):
        """Add the headers every response carries, then end the headers."""
        self.send_header("X-Content-Type-Options", "nosniff")
        if self.site is None:
            self.send_header("Content-Security-Policy", CSP_SCRIPTS if self.config.gateway_scripts else CSP_NO_SCRIPTS)
        else:
            csp = CSP_SITE_SCRIPTS if self.config.gateway_scripts else CSP_SITE_NO_SCRIPTS
            self.send_header("Content-Security-Policy", csp)
            # Origin-keyed: document.domain can't be relaxed to rnfs.localhost, which every site's host shares
            self.send_header("Origin-Agent-Cluster", "?1")
        # Deliberately no Access-Control-* headers, for any Origin or Referer (module docstring)
        self.headers_sent = True
        super().end_headers()

    def send_text(self, status: int, message: str, headers: dict | None = None):
        """Send a plain-text response."""
        data = (message + "\n").encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def send_body(self, status: int, content_type: str, data: bytes, headers: dict):
        """Send a complete in-memory body."""
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        for name, value in headers.items():
            self.send_header(name, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    # Dispatch

    def do_GET(self):
        """Serve content."""
        self.dispatch()

    def do_HEAD(self):
        """Serve headers only."""
        self.dispatch()

    def do_POST(self):
        """Refuse: the gateway is read-only."""
        self.dispatch()

    def do_PUT(self):
        """Refuse: the gateway is read-only."""
        self.dispatch()

    def do_DELETE(self):
        """Refuse: the gateway is read-only."""
        self.dispatch()

    def do_OPTIONS(self):
        """Refuse: no CORS preflight is ever approved."""
        self.dispatch()

    def dispatch(self):
        """Serve the request (handle_request); with the debug log, logged there with its outcome."""
        debug = debuglog.current()
        if debug is None:
            return self.handle_request()
        number = next(_request_numbers)
        started = time.monotonic()
        self.wfile = _CountingWriter(self.wfile)
        shown = ", ".join(f"{name}: {self.host_for_log() if name == 'Host' else self.headers[name]}"
                          for name in DEBUG_HEADERS if self.headers.get(name) is not None)
        with debug.tag(f"gateway #{number}"):
            debug.note(f"gateway {self.command} {self.path}" + (f" ({shown})" if shown else ""))
            try:
                self.handle_request()
            finally:
                debug.note(f"gateway answered {self.status_sent or 'nothing'}: {self.wfile.count} bytes written "
                           f"(headers included) in {(time.monotonic() - started) * 1000:.0f} ms"
                           + (", holding a gateway_fetches slot" if self.slot else ""))

    def handle_request(self):
        """Check the request, serve it, and turn every failure into a response carrying the common headers."""
        self.close_connection = True
        self.headers_sent = False
        self.slot = False
        self.site = None
        try:
            self.site = parse_host(self.headers.get("Host"), self.server.server_address[1])
            if self.command not in ("GET", "HEAD"):
                raise GatewayError(405, "the gateway only answers GET and HEAD", {"Allow": "GET, HEAD"})
            self.request_read()  # GET and HEAD have no body
            self.serve()
        except GatewayError as e:
            log(f"gateway refused {self.path_for_log()} with {e.status}: {e}", LOG_DEBUG)
            self.respond_error(e.status, str(e), e.headers)
        except NotFound as e:
            log(f"gateway: {self.path_for_log()} not found: {e}", LOG_DEBUG)
            self.respond_error(404, str(e) or ERROR_MESSAGES[404], {})
        except Exception as e:  # noqa: BLE001 - every error becomes a response, never a dropped connection
            status = error_status(e)
            # str(e) may name local paths (a StorageFull's or an OSError's) or the node's limits: logged only
            log(f"gateway {self.command} {self.path_for_log()} failed: {e!r}",
                LOG_ERROR if status == 500 else LOG_VERBOSE)
            self.respond_error(status, ERROR_MESSAGES.get(status, ERROR_MESSAGES[500]),
                               {"Retry-After": RETRY_AFTER} if status == 503 else {})
        finally:
            if self.slot:
                self.limiter.release()

    def path_for_log(self) -> str:
        """The request's path for the log, after its site on a subdomain host (the CID written in base32)."""
        if self.site is None:
            return loggable(self.path)
        kind, value = self.site
        return f"{kind} site {value if kind == 'rnfs' else value.hex()} {loggable(self.path)}"

    def host_for_log(self) -> str:
        """The Host header for the debug log: a subdomain host as its site, so a CID shows in base32."""
        host = self.headers.get("Host")
        try:
            site = parse_host(host, self.server.server_address[1])
        except GatewayError:
            return host
        if site is None:
            return host
        kind, value = site
        return f"{value if kind == 'rnfs' else value.hex()}.{SITE_DOMAINS[kind]} (subdomain)"

    def respond_error(self, status: int, message: str, headers: dict):
        """Send an error, unless a response is already under way (then the connection is just closed)."""
        if self.headers_sent:
            return
        try:
            self.send_text(status, message, headers)
        except OSError:
            pass  # the client has gone away

    # Limits

    def limited(self) -> bool:
        """True if this request isn't the user's own navigation, so its network use is limited."""
        site = self.headers.get("Sec-Fetch-Site")
        if site is None:
            # Browsers without Fetch Metadata (Safari < 16.4, Firefox < 90) send no Sec-Fetch-Site for a web page's
            # requests either, and every browser's User-Agent starts with "Mozilla/": only a tool's is the user's own
            return (self.headers.get("User-Agent") or "").strip().lower().startswith("mozilla/")
        return site.strip().lower() != "none"

    def need_network(self):
        """Take a gateway_fetches slot for a limited request about to use the network (once per request)."""
        if self.slot or not self.limited():
            return
        if not self.limiter.try_acquire():
            raise GatewayError(503, "too many gateway fetches in progress; try again shortly",
                               {"Retry-After": RETRY_AFTER})
        self.slot = True

    # Serving

    def serve(self):
        """Route /rnfs/ and /name/ requests, and every request on a subdomain host."""
        url = urlsplit(self.path)
        self.query = parse_qs(url.query, keep_blank_values=True)
        if self.site is not None:
            return self.serve_site(url)
        segments = url.path.split("/")  # "", "rnfs", <cid>, names…; a trailing "" means a trailing slash
        if len(segments) < 3 or segments[0] != "" or segments[1] not in ("rnfs", "name") or not segments[2]:
            raise GatewayError(404, "gateway paths are /rnfs/<cid>[/path] and /name/<name>[/path]")
        slash = len(segments) > 3 and segments[-1] == ""
        names = [self.decode_segment(segment) for segment in (segments[3:-1] if slash else segments[3:])]
        if self.param("format") is not None:  # --- trustless ?format=raw|archive: the user's own GET only ---
            return gateway_formats.serve_format(self, segments, names)

        if segments[1] == "rnfs":
            try:
                root = str(Cid.parse(segments[2]))
            except ValueError as e:
                raise GatewayError(400, str(e)) from None
        else:
            root = self.resolve_name(segments[2])
        # url.path starts with /rnfs/ or /name/, so this can't become a protocol-relative open redirect
        location = url.path + "/" + (f"?{url.query}" if url.query else "")
        self.serve_target(root, names, slash, segments[1] == "rnfs", f"/{segments[1]}/{segments[2]}", location)

    def serve_site(self, url):
        """Serve a request on a subdomain host: its path is a path in the site's root, nothing else."""
        if self.headers.get("Service-Worker") is not None:
            # A worker on a name's site could go on serving old content after the name moves
            raise GatewayError(403, "service workers aren't allowed on the gateway")
        if not self.path.startswith("/"):  # origin-form only: an absolute-form target could name another host
            raise GatewayError(400, "the path must start with /")
        segments = url.path[1:].split("/")  # names…; a trailing "" means a trailing slash ("/" is the root's)
        slash = segments[-1] == ""
        # Every segment is a valid name: an empty one ("//") is refused, so no redirect is protocol-relative
        names = [self.decode_segment(segment) for segment in (segments[:-1] if slash else segments)]
        if self.param("format") is not None:
            raise GatewayError(400, "?format= is only answered on the path gateway: /rnfs/<cid>?format=raw|archive")
        kind, value = self.site
        root = str(value) if kind == "rnfs" else self.resolve_name(value.hex())
        # Built from the decoded names, each percent-encoded whole, so the redirect stays a path on this site
        location = "/" + "".join(quote(name, safe="") + "/" for name in names) + (f"?{url.query}" if url.query else "")
        self.serve_target(root, names, slash, kind == "rnfs", f"/{kind}/{root if kind == 'rnfs' else value.hex()}",
                          location)

    def serve_target(self, root: str, names: list[str], slash: bool, immutable: bool, title: str, location: str):
        """Serve root (a written CID) / names: slash if the URL ended with one, immutable for content named by
        CID, title the listing's title for root, location the slash form a directory without one redirects to."""
        # Answer a revalidation of a bare CID before touching the node at all
        if immutable and not names and etag_matches(self.headers.get("If-None-Match"), f'"{root}"'):
            return self.send_not_modified(root)
        target = "/".join([root, *names])
        if self.limited() and not self.node.is_local(target):
            self.need_network()
        if debuglog.current() is not None:
            log(f"gateway: {target} is " + ("stored here" if self.node.is_local(target) else "not all stored here; "
                                            "fetching what it needs") + (", for a web page" if self.limited() else ""),
                LOG_DEBUG)
        info = self.node.info(target, for_gateway=True)
        etag = f'"{info["cid"]}"'
        if immutable and etag_matches(self.headers.get("If-None-Match"), etag):
            return self.send_not_modified(info["cid"])
        cache = {"ETag": etag, "Cache-Control": IMMUTABLE} if immutable else {}

        if info["type"] == "directory":
            if not slash:
                return self.redirect_to_slash(location)
            return self.serve_directory(target, "/".join([title, *names]) + "/", bool(names), cache)
        if self.site is None:
            filename = self.param("filename") or (names[-1] if names else None)
        else:
            filename = names[-1] if names else None  # ?filename= only names the download on a site (REV6)
        # A site's root that is a file is its origin's only content, so sniffing it as a page gains nobody anything;
        # a file reached by a path inside a site's DAG may be someone else's, put there (REV6)
        self.serve_file(target, info["size"], filename, cache, passive_only=self.site is not None and bool(names))

    def decode_segment(self, segment: str) -> str:
        """One percent-decoded path segment, which must be a single valid name."""
        try:
            name = unquote(segment, errors="strict")
        except UnicodeDecodeError:
            raise GatewayError(400, "path segments must be UTF-8") from None
        if not name or "/" in name or "\x00" in name or name in (".", ".."):
            raise GatewayError(400, f"invalid name in path: {segment!r}")
        return name

    def param(self, name: str) -> str | None:
        """A query-string parameter, or None."""
        values = self.query.get(name)
        return values[-1] if values else None

    def resolve_name(self, name: str) -> str:
        """The CID a name points at, from the short-lived cache or a resolution counted against the limit."""
        try:
            name = parse_hex_id(name, "name").hex()
        except ValueError as e:
            raise GatewayError(400, str(e)) from None
        cid = self.names.get(name)
        if cid is None:
            self.need_network()
            cid = self.node.name_resolve(name, for_gateway=True)["cid"]
            self.names.put(name, cid)
            log(f"gateway resolved name {name} to {cid}", LOG_DEBUG)
        else:
            log(f"gateway: name {name} is {cid} (resolved within {NAME_CACHE_TIME:g} s)", LOG_DEBUG)
        return cid

    def send_not_modified(self, cid: str):
        """304 for a cached immutable response."""
        log(f"gateway: {cid} not modified", LOG_DEBUG)
        self.send_response(304)
        self.send_header("ETag", f'"{cid}"')
        self.send_header("Cache-Control", IMMUTABLE)
        self.end_headers()

    def redirect_to_slash(self, location: str):
        """301 from a directory path without a trailing slash to location, its slash form."""
        self.send_text(301, f"see {location}", {"Location": location})

    def serve_directory(self, target: str, title: str, has_parent: bool, cache: dict):
        """Serve a directory as its index.html, or as a listing titled title."""
        entries = list(self.node.ls(target, for_gateway=True))
        for entry in entries:
            if entry["name"] == INDEX and entry["kind"] != "directory":
                index_target = f"{target}/{INDEX}"
                if self.limited() and not self.node.is_local(index_target):
                    self.need_network()  # the directory may be stored without its index.html
                log(f"gateway: serving {index_target}", LOG_DEBUG)
                return self.serve_file(index_target, entry["size"], INDEX, cache, passive_only=self.site is not None)
        log(f"gateway: listing {target} ({len(entries)} entries)", LOG_DEBUG)
        self.send_body(200, "text/html; charset=utf-8", listing_html(title, entries, has_parent), cache)

    def serve_file(self, target: str, size: int, filename: str | None, cache: dict, passive_only: bool = False):
        """Serve a file, or the requested byte range of it; passive_only: sniff only to SITE_SNIFFED_TYPES."""
        byte_range = parse_range(self.headers.get("Range"), size)
        if_range = self.headers.get("If-Range")
        if if_range is not None and if_range.strip() != cache.get("ETag"):
            byte_range = None  # the client's partial copy may be of something else: send it all
        if byte_range == "unsatisfiable":
            raise GatewayError(416, "range not satisfiable", {"Content-Range": f"bytes */{size}", "Accept-Ranges": "bytes"})
        # A web page's HEAD or later range reads the first bytes too: its type mustn't depend on what is stored
        content_type = self.file_type(target, size, filename, self.limited() or (
            self.command != "HEAD" and (byte_range is None or byte_range[0] == 0)), passive_only)
        headers = {"Accept-Ranges": "bytes", **cache}
        if self.param("filename"):
            # On a site, a link naming a file x.html mustn't make it render in the site's origin (REV6)
            disposition = "inline" if self.site is None else "attachment"
            headers["Content-Disposition"] = f"{disposition}; filename*=UTF-8''{quote(self.param('filename'), safe='')}"

        if byte_range is None:
            status, start, length = 200, 0, size
        else:
            status, start, length = 206, byte_range[0], byte_range[1] - byte_range[0] + 1
            headers["Content-Range"] = f"bytes {byte_range[0]}-{byte_range[1]}/{size}"
        log(f"gateway: {target} as {content_type}, {length} bytes from byte {start} of {size}", LOG_DEBUG)

        if self.command == "HEAD":
            return self.send_stream(status, content_type, length, headers, iter(()))
        chunks = iter(self.node.read(target, offset=start, length=length, for_gateway=True))
        self.send_stream(status, content_type, length, headers, chunks)

    def file_type(self, target: str, size: int, filename: str | None, read_start: bool,
                  passive_only: bool = False) -> str:
        """A file's Content-Type: octet-stream for a sealed file (never decrypted here), else by name or sniffed
        (with passive_only, a file inside a site's DAG, only to SITE_SNIFFED_TYPES).

        With read_start (a response that sends the first bytes anyway, or any web page's request) they are
        read, fetched if need be. Otherwise (the user's own HEAD or range starting later) they are used only if
        already stored, so such a request fetches nothing it doesn't send.
        """
        length = min(size, SNIFF_SIZE)
        if read_start:
            head = b"".join(self.node.read(target, offset=0, length=length, for_gateway=True))
        else:
            head = self.node.stored_head(target, length)
            if head is None:
                return type_from_name(filename) or "application/octet-stream"
        if is_sealed(head):
            return "application/octet-stream"
        by_name = type_from_name(filename)
        if by_name:
            return by_name
        sniffed = sniff(head)
        if passive_only and sniffed not in SITE_SNIFFED_TYPES:
            return SITE_UNSNIFFED  # a file its DAG doesn't name as a page never becomes one in a site's origin
        return sniffed

    def send_stream(self, status: int, content_type: str, length: int, headers: dict, chunks):
        """Send length bytes from chunks. The first chunk is read before the status, so early errors show."""
        try:
            first = next(chunks, b"")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(length))
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            if self.command == "HEAD":
                return
            self.wfile.write(first)
            for chunk in chunks:
                self.wfile.write(chunk)
        except Exception as e:
            if not self.headers_sent:
                raise
            # Too late for an error status; the short body (Content-Length) tells the client
            if not isinstance(e, OSError):
                log(f"gateway {self.path_for_log()} failed mid-stream: {e!r}", LOG_ERROR)
        finally:
            close = getattr(chunks, "close", None)
            if close:
                close()  # stop a generator's fetch when the client has gone away
