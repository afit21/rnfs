"""HTTP gateway of a running node (specification section 11), bound to 127.0.0.1, separate from the API.

GET/HEAD /rnfs/<cid>[/path]   content by CID: immutable, ETag "<cid>" of the target
GET/HEAD /name/<name>[/path]  content a name points at, resolved with for_gateway=True

Behaviour:
- Path segments are percent-decoded one by one; a decoded segment can't contain "/" or NUL, so every
  segment is exactly one name.
- Files: Content-Type from ?filename=, else the path's last name, else sniffed from the first bytes. A
  HEAD, or a range not starting at byte 0, uses the first bytes only if they are stored (else the name, or
  application/octet-stream), so it never fetches what it doesn't send.
  Single byte ranges (206, 416, Accept-Ranges; If-Range honoured); several ranges get the whole file.
- Directories: without a trailing slash, 301 to the slash form; with one, index.html if present, else
  an HTML-escaped listing with percent-encoded links.
- /rnfs/ responses carry ETag "<cid>" and "Cache-Control: public, max-age=31536000, immutable";
  If-None-Match answers 304. /name/ responses are mutable and carry neither.
- Every response, errors and redirects included, carries X-Content-Type-Options: nosniff and the spec's
  sandboxing Content-Security-Policy. With gateway_scripts off, allow-scripts and 'unsafe-eval' are
  dropped and script-src 'none' added, so no script runs even if a browser ignores the sandbox.
- Access-Control-Allow-Origin: * on GET (and HEAD, which mirrors GET) only when the Referer's origin is
  this gateway (http://127.0.0.1:<port> or http://localhost:<port>), so sandboxed pages, whose origin is
  opaque, can fetch() their own DAG. Never in private mode. Vary: Referer keeps browser caches honest.
- Errors other than 500 say what went wrong; a 500 says only that, and its detail (which may name local
  paths) goes to the log, since a gateway page may be allowed to read the response (the CORS rule above).
- Requests that aren't the user's own navigation (Sec-Fetch-Site present and not "none") may hold at most
  gateway_fetches network fetches or name resolutions at a time; over that, 503 with Retry-After.
  Content already stored (node.is_local) is served without limit. A request with no Sec-Fetch-Site is
  treated as the user's own: every browser sends the header to 127.0.0.1/localhost (potentially
  trustworthy origins), so its absence means a local tool such as curl, run by the user.
- Everything is requested with for_gateway=True: fetched content is cache, never provided, and names go
  only into the capped table. Name resolutions are reused for NAME_CACHE_TIME, so a page's many
  subresources don't each start a resolution against gateway_fetches.
- Connections are limited as for the API (rnfs/api.py): the request must arrive within request_timeout, at
  most max_connections are served at once (more get a 503 carrying the headers above), and a response write
  may block response_timeout, so clients that stop reading can't hold every slot for ever.
"""
import codecs
import html
import mimetypes
import re
import threading
import time
from collections import OrderedDict
from urllib.parse import parse_qs, quote, unquote, urlsplit

from rnfs.api import LocalServer, TimedHandler, error_status, host_allowed, loggable, parse_hex_id, raw_response
from rnfs import gateway_formats  # --- content ---
from rnfs.content.cid import Cid
from rnfs.content.seal import is_sealed  # --- content ---
from rnfs.log import LOG_ERROR, log

CSP_SCRIPTS = ("sandbox allow-scripts allow-forms allow-popups; "
               "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob:; form-action 'self'")
CSP_NO_SCRIPTS = ("sandbox allow-forms allow-popups; "
                  "default-src 'self' 'unsafe-inline' data: blob:; script-src 'none'; form-action 'self'")
IMMUTABLE = "public, max-age=31536000, immutable"
RETRY_AFTER = "10"  # seconds a limited request should wait before retrying
SNIFF_SIZE = 512  # bytes read to guess a type, as in the WHATWG MIME sniffing standard
NAME_CACHE_TIME = 60.0
NAME_CACHE_SIZE = 1000
INDEX = "index.html"
RANGE_NUMBER = re.compile(r"[0-9]{1,18}")  # ASCII digits only (str.isdigit() takes "¹²³"), and below 2**63

# Types some platforms' mimetypes tables lack or get wrong (on Windows, mimetypes also reads the registry,
# which installed software edits), so the commonest web types are fixed here too
EXTRA_TYPES = {".html": "text/html", ".htm": "text/html", ".css": "text/css", ".txt": "text/plain",
               ".js": "text/javascript", ".mjs": "text/javascript", ".wasm": "application/wasm",
               ".webp": "image/webp", ".avif": "image/avif", ".svg": "image/svg+xml", ".json": "application/json",
               ".md": "text/markdown", ".webm": "video/webm", ".opus": "audio/ogg", ".woff2": "font/woff2"}

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
    return str(size)


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
        super().__init__(message)
        self.status = status
        self.headers = headers or {}


class FetchLimiter:
    """At most `limit` concurrent network fetches or name resolutions for requests that aren't navigation."""

    def __init__(self, limit: int):
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

    def end_headers(self):
        """Add the headers every response carries, then end the headers."""
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", CSP_SCRIPTS if self.config.gateway_scripts else CSP_NO_SCRIPTS)
        if self.command in ("GET", "HEAD"):
            self.send_header("Vary", "Referer")
            if self.cors_allowed():
                self.send_header("Access-Control-Allow-Origin", "*")
        self.headers_sent = True
        super().end_headers()

    def cors_allowed(self) -> bool:
        """True if the Referer is a page on this gateway and the node isn't in private mode."""
        if self.config.private:
            return False
        # Errors raised while parsing the request (e.g. 431) are sent before self.headers exists
        headers = getattr(self, "headers", None)
        referer = headers.get("Referer") if headers is not None else None
        if not referer:
            return False
        url = urlsplit(referer)
        port = self.server.server_address[1]
        return url.scheme == "http" and url.netloc.lower() in (f"127.0.0.1:{port}", f"localhost:{port}")

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
        """Check the request, serve it, and turn every failure into a response carrying the common headers."""
        self.close_connection = True
        self.headers_sent = False
        self.slot = False
        try:
            if not host_allowed(self.headers.get("Host"), self.server.server_address[1]):
                raise GatewayError(403, "Host header must be 127.0.0.1:<port> or localhost:<port>")
            if self.command not in ("GET", "HEAD"):
                raise GatewayError(405, "the gateway only answers GET and HEAD", {"Allow": "GET, HEAD"})
            self.request_read()  # GET and HEAD have no body
            self.serve()
        except GatewayError as e:
            self.respond_error(e.status, str(e), e.headers)
        except Exception as e:  # noqa: BLE001 - every error becomes a response, never a dropped connection
            status = error_status(e)
            if status == 500:
                log(f"gateway {self.command} {loggable(self.path)} failed: {e!r}", LOG_ERROR)
                message = "internal error; see the rnfs daemon's log"  # str(e) may name local paths
            else:
                message = str(e) or type(e).__name__
            self.respond_error(status, message, {"Retry-After": RETRY_AFTER} if status == 503 else {})
        finally:
            if self.slot:
                self.limiter.release()

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
        return site is not None and site.strip().lower() != "none"

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
        """Route /rnfs/ and /name/ requests."""
        url = urlsplit(self.path)
        self.query = parse_qs(url.query, keep_blank_values=True)
        segments = url.path.split("/")  # "", "rnfs", <cid>, names…; a trailing "" means a trailing slash
        if len(segments) < 3 or segments[0] != "" or segments[1] not in ("rnfs", "name") or not segments[2]:
            raise GatewayError(404, "gateway paths are /rnfs/<cid>[/path] and /name/<name>[/path]")
        slash = len(segments) > 3 and segments[-1] == ""
        names = [self.decode_segment(segment) for segment in (segments[3:-1] if slash else segments[3:])]
        if self.param("format") is not None:  # --- content: trustless ?format=raw|archive ---
            return gateway_formats.serve_format(self, segments, names)

        if segments[1] == "rnfs":
            try:
                root = str(Cid.parse(segments[2]))
            except ValueError as e:
                raise GatewayError(400, str(e)) from None
            immutable = True
            # Answer a revalidation of a bare CID before touching the node at all
            if not names and etag_matches(self.headers.get("If-None-Match"), f'"{root}"'):
                return self.send_not_modified(root)
        else:
            root = self.resolve_name(segments[2])
            immutable = False

        target = "/".join([root, *names])
        if self.limited() and not self.node.is_local(target):
            self.need_network()
        info = self.node.info(target, for_gateway=True)
        etag = f'"{info["cid"]}"'
        if immutable and etag_matches(self.headers.get("If-None-Match"), etag):
            return self.send_not_modified(info["cid"])
        cache = {"ETag": etag, "Cache-Control": IMMUTABLE} if immutable else {}

        if info["type"] == "directory":
            if not slash:
                return self.redirect_to_slash(url)
            return self.serve_directory(target, segments, names, cache)
        filename = self.param("filename") or (names[-1] if names else None)
        self.serve_file(target, info["size"], filename, cache)

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
        return cid

    def send_not_modified(self, cid: str):
        """304 for a cached immutable response."""
        self.send_response(304)
        self.send_header("ETag", f'"{cid}"')
        self.send_header("Cache-Control", IMMUTABLE)
        self.end_headers()

    def redirect_to_slash(self, url):
        """301 from a directory path without a trailing slash to the slash form, keeping the query."""
        # url.path starts with /rnfs/ or /name/, so this can't become a protocol-relative open redirect
        location = url.path + "/" + (f"?{url.query}" if url.query else "")
        self.send_text(301, f"see {location}", {"Location": location})

    def serve_directory(self, target: str, segments: list[str], names: list[str], cache: dict):
        """Serve a directory as its index.html, or as a listing."""
        entries = list(self.node.ls(target, for_gateway=True))
        for entry in entries:
            if entry["name"] == INDEX and entry["kind"] != "directory":
                index_target = f"{target}/{INDEX}"
                if self.limited() and not self.node.is_local(index_target):
                    self.need_network()  # the directory may be stored without its index.html
                return self.serve_file(index_target, entry["size"], INDEX, cache)
        title = "/" + "/".join([segments[1], segments[2], *names]) + "/"
        self.send_body(200, "text/html; charset=utf-8", listing_html(title, entries, bool(names)), cache)

    def serve_file(self, target: str, size: int, filename: str | None, cache: dict):
        """Serve a file, or the requested byte range of it."""
        byte_range = parse_range(self.headers.get("Range"), size)
        if_range = self.headers.get("If-Range")
        if if_range is not None and if_range.strip() != cache.get("ETag"):
            byte_range = None  # the client's partial copy may be of something else: send it all
        if byte_range == "unsatisfiable":
            raise GatewayError(416, "range not satisfiable", {"Content-Range": f"bytes */{size}", "Accept-Ranges": "bytes"})
        content_type = self.file_type(target, size, filename, self.command != "HEAD" and
                                      (byte_range is None or byte_range[0] == 0))
        headers = {"Accept-Ranges": "bytes", **cache}
        if self.param("filename"):
            headers["Content-Disposition"] = f"inline; filename*=UTF-8''{quote(self.param('filename'), safe='')}"

        if byte_range is None:
            status, start, length = 200, 0, size
        else:
            status, start, length = 206, byte_range[0], byte_range[1] - byte_range[0] + 1
            headers["Content-Range"] = f"bytes {byte_range[0]}-{byte_range[1]}/{size}"

        if self.command == "HEAD":
            return self.send_stream(status, content_type, length, headers, iter(()))
        chunks = iter(self.node.read(target, offset=start, length=length, for_gateway=True))
        self.send_stream(status, content_type, length, headers, chunks)

    def file_type(self, target: str, size: int, filename: str | None, sends_start: bool) -> str:
        """A file's Content-Type: octet-stream for a sealed file (never decrypted here), else by name or sniffed.

        The first bytes are fetched only when the response sends them anyway; for a HEAD or a range starting
        later they are used only if already stored, so such a request fetches nothing it doesn't send.
        """
        length = min(size, SNIFF_SIZE)
        if sends_start:
            head = b"".join(self.node.read(target, offset=0, length=length, for_gateway=True))
        else:
            stored_head = getattr(self.node, "stored_head", None)  # facades without it: only complete targets
            if stored_head is not None:
                head = stored_head(target, length)
            elif self.node.is_local(target):
                head = b"".join(self.node.read(target, offset=0, length=length, for_gateway=True))
            else:
                head = None
            if head is None:
                return type_from_name(filename) or "application/octet-stream"
        return "application/octet-stream" if is_sealed(head) else type_from_name(filename) or sniff(head)

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
                log(f"gateway {loggable(self.path)} failed mid-stream: {e!r}", LOG_ERROR)
        finally:
            close = getattr(chunks, "close", None)
            if close:
                close()  # stop a generator's fetch when the client has gone away
