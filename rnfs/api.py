"""Local JSON API of a running node (specification section 11), bound to 127.0.0.1 only.

Every command of the CLI is one request. Targets are `<cid>[/name…]` strings; node IDs and names are
32 hex characters; paths are absolute paths on the daemon's machine. Successful responses are JSON
(except cat and name export, which send bytes); errors are `{"error": message}`.

| Method | Path | Parameters | Response / facade call |
|---|---|---|---|
| GET  | /api/status | | `status()` |
| GET  | /api/info | `target` | `info(target)` |
| GET  | /api/cat | `target`, `offset`, `length` | the bytes of a file (`read`), Content-Length set |
| GET  | /api/ls | `target` | `{"entries": ls(target)}` |
| GET  | /api/stat | `target` | `stat(target)` |
| GET  | /api/pin/ls | `cid` (optional) | `{"pins": pin_ls(cid)}` |
| GET  | /api/providers | `cid` | `{"providers": [node ID]}` |
| GET  | /api/ping | `node` | `{"rtt": seconds or null}` |
| GET  | /api/name/list | | `{"names": name_list()}` |
| GET  | /api/name/resolve | `name` | `name_resolve(name)` |
| POST | /api/add | JSON `path`, `wrap`, `only_hash` | `add_path(...)` |
| POST | /api/add/stream | query `name`, `only_hash`; body is the data | `add_stream(...)` |
| POST | /api/get | JSON `target`, `output`, `from` | `get(...)` |
| POST | /api/dir/set | JSON `target`, `name`, `cid`, `executable` | `dir_set(...)` |
| POST | /api/dir/rm | JSON `target` | `dir_rm(target)` |
| POST | /api/pin/add | JSON `target` | `pin_add(target)` |
| POST | /api/pin/rm | JSON `cid` | `{}` after `pin_rm(cid)` |
| POST | /api/pin/remote | JSON `node`, `cid`, `pin` | `{"accepted": bool}` |
| POST | /api/gc, /api/verify | | `gc()`, `verify()` |
| POST | /api/name/create | JSON `alias` | `name_create(alias)` |
| POST | /api/name/rm | JSON `alias` | `{}` after `name_rm(alias)` |
| POST | /api/name/export | JSON `alias` | the key bytes |
| POST | /api/name/import | query `alias`; body is the key bytes | `name_import(alias, key)` |
| POST | /api/name/publish | JSON `alias`, `target` | `name_publish(alias, target)` |
| POST | /api/export | JSON `target`, `output` | `export(target, output)` |
| POST | /api/import | JSON `path` | `import_archive(path)` |

GET parameters are in the query string. Request bodies may use Content-Length or chunked encoding.

Security:
- The Host header must be `127.0.0.1:<port>` or `localhost:<port>`, which blocks DNS rebinding.
- Every request except `GET /api/status` must carry `X-Rnfs-Api: 1`. The spec requires it for requests
  that change state; here even reads need it, since they can fetch from the network, fill the cache,
  persist name sequences or export a key. A custom header forces a CORS preflight, which fails because
  the API never sends CORS headers, so web pages can't make the API do anything. Status stays open for
  health checks.
- Errors map to statuses as design.md says: ValueError 400, NotFound 404, TooLarge 413, InvalidDag 422,
  Busy 503, StorageFull 507, other errors 500.
"""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from rnfs.content.cid import Cid
from rnfs.errors import Busy, InvalidDag, NotFound, StorageFull, TooLarge
from rnfs.log import LOG_ERROR, log

API_HEADER = "X-Rnfs-Api"
MAX_JSON_BODY = 1024 * 1024  # JSON parameters are tiny; this only stops a runaway body filling memory
MAX_KEY_BODY = 64 * 1024  # an RNS identity file is 64 bytes

# Exception class → HTTP status, checked in order (design.md); anything else is 500
ERROR_STATUSES = [(ValueError, 400), (NotFound, 404), (TooLarge, 413), (InvalidDag, 422), (Busy, 503),
                  (StorageFull, 507)]


def error_status(error: Exception) -> int:
    """The HTTP status for an exception raised by the node."""
    for error_class, status in ERROR_STATUSES:
        if isinstance(error, error_class):
            return status
    return 500


def host_allowed(host: str | None, port: int) -> bool:
    """True if a Host header names this server as 127.0.0.1:<port> or localhost:<port> (DNS rebinding)."""
    if host is None:
        return False
    return host.lower() in (f"127.0.0.1:{port}", f"localhost:{port}")


def parse_target(text) -> str:
    """text if it looks like `<cid>[/name…]`; the node checks the path. Raises ValueError."""
    if not isinstance(text, str) or not text:
        raise ValueError("a target <cid>[/path] is required")
    Cid.parse(text.split("/", 1)[0])
    return text


def parse_cid(text) -> str:
    """text if it is a valid written CID, else ValueError."""
    if not isinstance(text, str):
        raise ValueError("a CID is required")
    Cid.parse(text)
    return text


def parse_hex_id(text, what: str = "node ID") -> bytes:
    """A 16-byte node ID or name written as 32 hex characters. Raises ValueError."""
    try:
        value = bytes.fromhex(text) if isinstance(text, str) else b""
    except ValueError:
        value = b""
    if len(value) != 16:
        raise ValueError(f"invalid {what} {text!r}: must be 32 hex characters")
    return value


def parse_path(text) -> str:
    """text if it is an absolute local path, else ValueError (the CLI resolves relative paths)."""
    if not isinstance(text, str) or not os.path.isabs(text):
        raise ValueError(f"path must be absolute: {text!r}")
    return text


def parse_alias(text) -> str:
    """text if it is a usable alias; the node applies its own rules on top."""
    if not isinstance(text, str) or not text:
        raise ValueError("an alias is required")
    return text


def parse_int(text, name: str, default: int | None) -> int | None:
    """A non-negative integer parameter, or default when absent. Raises ValueError."""
    if text is None or text == "":
        return default
    try:
        value = int(text)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be an integer") from None
    if value < 0:
        raise ValueError(f"{name} must not be negative")
    return value


def parse_bool(value) -> bool:
    """A flag from JSON (true/false) or a query string ("1", "true", "yes")."""
    if isinstance(value, str):
        return value.lower() in ("1", "true", "yes")
    return bool(value)


class BodyReader:
    """File-like reader for a request body sent with Content-Length or chunked encoding.

    Raises ValueError if the body ends early, so a cut-off upload is never stored as if complete.
    """

    def __init__(self, rfile, length: int | None, chunked: bool):
        """Read from rfile: length bytes, or chunks until the last one when chunked."""
        self.rfile = rfile
        self.remaining = length  # bytes left: of the body, or of the current chunk when chunked
        self.chunked = chunked
        self.done = (length == 0) and not chunked

    def _next_chunk(self):
        """Read the next chunk header; marks the body done at the zero-length chunk."""
        line = self.rfile.readline(1024)
        if not line.endswith(b"\n"):
            raise ValueError("request body ended early")
        try:
            size = int(line.split(b";", 1)[0].strip(), 16)
        except ValueError:
            raise ValueError("invalid chunked request body") from None
        if size < 0:
            raise ValueError("invalid chunked request body")
        if size == 0:
            while self.rfile.readline(1024).strip():  # trailers, up to the blank line
                pass
            self.done = True
        self.remaining = size

    def read(self, size: int = -1) -> bytes:
        """Up to size bytes (all that remain if size < 0); b"" at the end of the body."""
        parts = []
        while not self.done and (size < 0 or size > 0):
            if self.chunked and self.remaining == 0:
                self._next_chunk()
                continue
            want = self.remaining if size < 0 else min(size, self.remaining)
            data = self.rfile.read(want)
            if not data:
                raise ValueError("request body ended early")
            parts.append(data)
            self.remaining -= len(data)
            if size >= 0:
                size -= len(data)
            if self.remaining == 0:
                if self.chunked:
                    if self.rfile.read(2) != b"\r\n":
                        raise ValueError("invalid chunked request body")
                else:
                    self.done = True
        return b"".join(parts)

    def readinto(self, buffer) -> int:
        """Fill buffer from the body; returns the number of bytes read."""
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)

    def readable(self) -> bool:
        return True


class LocalServer:
    """A ThreadingHTTPServer on 127.0.0.1 run on a daemon thread. Port 0 picks a free port."""

    poll_interval = 0.5  # how often serve_forever checks for stop(); tests lower it to stop quickly

    def __init__(self, handler_class, port: int, **attributes):
        """Bind to 127.0.0.1:port with handler_class; attributes are set on a subclass of it."""
        handler = type(handler_class.__name__, (handler_class,), attributes)
        self.server = ThreadingHTTPServer(("127.0.0.1", port), handler)
        self.server.daemon_threads = True  # long fetches mustn't keep the process alive on shutdown
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(self.poll_interval), daemon=True)

    @property
    def port(self) -> int:
        """The port actually bound, useful when 0 was requested."""
        return self.server.server_address[1]

    def start(self):
        """Start serving requests in the background."""
        self.thread.start()

    def stop(self):
        """Stop serving and close the listening socket. Requests still running finish on their threads."""
        if self.thread.is_alive():
            self.server.shutdown()
        self.server.server_close()


class ApiServer(LocalServer):
    """The local API for node (an RnfsNode, or anything with its facade methods)."""

    def __init__(self, node, port: int):
        """Bind to 127.0.0.1:port; requests are served once start() is called."""
        super().__init__(ApiHandler, port, node=node)


class ApiRequestError(Exception):
    """A request the API refuses before reaching the node: status and message."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


class ApiHandler(BaseHTTPRequestHandler):
    """Handles one API request using node, which is set on the subclass ApiServer creates."""

    node = None
    server_version = "rnfs"
    sys_version = ""

    # (method, path) → handler method name. GET routes never change pins, names or content
    ROUTES = {
        ("GET", "/api/status"): "api_status",
        ("GET", "/api/info"): "api_info",
        ("GET", "/api/cat"): "api_cat",
        ("GET", "/api/ls"): "api_ls",
        ("GET", "/api/stat"): "api_stat",
        ("GET", "/api/pin/ls"): "api_pin_ls",
        ("GET", "/api/providers"): "api_providers",
        ("GET", "/api/ping"): "api_ping",
        ("GET", "/api/name/list"): "api_name_list",
        ("GET", "/api/name/resolve"): "api_name_resolve",
        ("POST", "/api/add"): "api_add",
        ("POST", "/api/add/stream"): "api_add_stream",
        ("POST", "/api/get"): "api_get",
        ("POST", "/api/dir/set"): "api_dir_set",
        ("POST", "/api/dir/rm"): "api_dir_rm",
        ("POST", "/api/pin/add"): "api_pin_add",
        ("POST", "/api/pin/rm"): "api_pin_rm",
        ("POST", "/api/pin/remote"): "api_pin_remote",
        ("POST", "/api/gc"): "api_gc",
        ("POST", "/api/verify"): "api_verify",
        ("POST", "/api/name/create"): "api_name_create",
        ("POST", "/api/name/rm"): "api_name_rm",
        ("POST", "/api/name/export"): "api_name_export",
        ("POST", "/api/name/import"): "api_name_import",
        ("POST", "/api/name/publish"): "api_name_publish",
        ("POST", "/api/export"): "api_export",
        ("POST", "/api/import"): "api_import",
    }
    OPEN_ROUTES = {("GET", "/api/status")}  # allowed without X-Rnfs-Api

    def log_message(self, format, *args):
        """Log nothing: RNS does the node's logging, and per-request lines would drown it out."""

    # Responses

    def send_common_headers(self):
        """Headers on every API response. Deliberately no Access-Control-* headers (section 11)."""
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")

    def send_json(self, status: int, body):
        """Send body as a JSON response with the given status."""
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_common_headers()
        self.end_headers()
        self.wfile.write(data)

    def send_error_json(self, status: int, message: str, headers: dict | None = None):
        """Send {"error": message} with the given status."""
        data = json.dumps({"error": message}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_common_headers()
        self.end_headers()
        self.wfile.write(data)

    def send_bytes(self, data: bytes):
        """Send data as an application/octet-stream response."""
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.send_common_headers()
        self.end_headers()
        self.wfile.write(data)

    # Dispatch

    def do_GET(self):
        """Handle a GET request."""
        self.dispatch()

    def do_POST(self):
        """Handle a POST request."""
        self.dispatch()

    def do_PUT(self):
        """Refuse PUT (405 for known paths)."""
        self.dispatch()

    def do_DELETE(self):
        """Refuse DELETE (405 for known paths)."""
        self.dispatch()

    def do_HEAD(self):
        """Refuse HEAD: API answers are generated, so there's nothing cheap to describe."""
        self.dispatch()

    def dispatch(self):
        """Check Host and X-Rnfs-Api, route the request, and map exceptions to error responses."""
        self.close_connection = True  # one request per connection keeps body handling simple
        url = urlsplit(self.path)
        self.query = parse_qs(url.query, keep_blank_values=True)
        self.headers_sent = False
        try:
            if not host_allowed(self.headers.get("Host"), self.server.server_address[1]):
                raise ApiRequestError(403, "Host header must be 127.0.0.1:<port> or localhost:<port>")
            route = (self.command, url.path)
            name = self.ROUTES.get(route)
            if name is None:
                allowed = sorted(method for method, path in self.ROUTES if path == url.path)
                if allowed:
                    return self.send_error_json(405, f"use {' or '.join(allowed)}", {"Allow": ", ".join(allowed)})
                raise ApiRequestError(404, f"no such API endpoint: {url.path}")
            if route not in self.OPEN_ROUTES and self.headers.get(API_HEADER) != "1":
                raise ApiRequestError(403, f"API requests must carry the header {API_HEADER}: 1")
            getattr(self, name)()
        except ApiRequestError as e:
            self.respond_error(e.status, str(e))
        except Exception as e:  # noqa: BLE001 - every error becomes a response, never a dropped connection
            status = error_status(e)
            if status == 500:
                log(f"API {self.command} {url.path} failed: {e!r}", LOG_ERROR)
            message = str(e) or type(e).__name__
            self.respond_error(status, message, {"Retry-After": "10"} if status == 503 else None)

    def respond_error(self, status: int, message: str, headers: dict | None = None):
        """Send an error response, unless a response is already under way (then just drop the connection)."""
        if self.headers_sent:
            return
        try:
            self.send_error_json(status, message, headers)
        except OSError:
            pass  # the client has gone away

    def end_headers(self):
        """End the headers, remembering that a response has started."""
        self.headers_sent = True
        super().end_headers()

    # Request parameters

    def param(self, name: str, required: bool = True) -> str | None:
        """A query-string parameter (the last if repeated), or None. Raises ValueError if required and missing."""
        values = self.query.get(name)
        if not values:
            if required:
                raise ValueError(f"missing parameter {name!r}")
            return None
        return values[-1]

    def body_reader(self) -> BodyReader:
        """A reader for the request body (Content-Length or chunked). Raises ApiRequestError 411 for neither."""
        if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
            return BodyReader(self.rfile, 0, chunked=True)
        length = self.headers.get("Content-Length")
        if length is None:
            raise ApiRequestError(411, "Content-Length or chunked encoding required")
        try:
            size = int(length)
        except ValueError:
            raise ApiRequestError(411, "invalid Content-Length") from None
        if size < 0:
            raise ApiRequestError(400, "invalid Content-Length")
        return BodyReader(self.rfile, size, chunked=False)

    def read_body(self, limit: int) -> bytes:
        """The whole request body, at most limit bytes (413 beyond that). An absent body is empty."""
        if "Content-Length" not in self.headers and "Transfer-Encoding" not in self.headers:
            return b""
        data = self.body_reader().read(limit + 1)
        if len(data) > limit:
            raise ApiRequestError(413, "request body too large")
        return data

    def json_body(self) -> dict:
        """The request body as a JSON object ({} when empty). Raises ValueError if it isn't one."""
        data = self.read_body(MAX_JSON_BODY)
        if not data:
            return {}
        try:
            body = json.loads(data)
        except ValueError:
            raise ValueError("request body must be JSON") from None
        if not isinstance(body, dict):
            raise ValueError("request body must be a JSON object")
        return body

    # GET endpoints

    def api_status(self):
        """GET /api/status: the node's summary."""
        self.send_json(200, self.node.status())

    def api_info(self):
        """GET /api/info?target=: type, size and CID of a target."""
        self.send_json(200, self.node.info(parse_target(self.param("target"))))

    def api_cat(self):
        """GET /api/cat?target=&offset=&length=: stream a file's bytes, fetched as needed."""
        target = parse_target(self.param("target"))
        offset = parse_int(self.param("offset", False), "offset", 0)
        length = parse_int(self.param("length", False), "length", None)
        info = self.node.info(target)
        if info["type"] != "file":
            raise ValueError(f"{target} is a directory")
        # Content-Length is known up front, so a client sees a fetch failing mid-stream as a short body
        available = max(info["size"] - offset, 0)
        length = available if length is None else min(length, available)
        chunks = iter(self.node.read(target, offset=offset, length=length))
        try:
            # The first chunk is fetched before the status line, so early errors still get an error status
            first = next(chunks, b"")
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(length))
            self.send_common_headers()
            self.end_headers()
            self.wfile.write(first)
            for chunk in chunks:
                self.wfile.write(chunk)
        except Exception as e:
            if self.headers_sent:
                # Too late for an error status; Content-Length tells the client the body was cut short
                if not isinstance(e, OSError):
                    log(f"API cat {target} failed mid-stream: {e!r}", LOG_ERROR)
                return
            raise
        finally:
            close = getattr(chunks, "close", None)
            if close:
                close()  # stop a generator's fetch if the client went away

    def api_ls(self):
        """GET /api/ls?target=: directory entries."""
        self.send_json(200, {"entries": list(self.node.ls(parse_target(self.param("target"))))})

    def api_stat(self):
        """GET /api/stat?target=: size, block count and local holdings."""
        self.send_json(200, self.node.stat(parse_target(self.param("target"))))

    def api_pin_ls(self):
        """GET /api/pin/ls[?cid=]: pins and their sources."""
        cid = self.param("cid", False)
        self.send_json(200, {"pins": self.node.pin_ls(parse_cid(cid) if cid else None)})

    def api_providers(self):
        """GET /api/providers?cid=: node IDs providing a CID."""
        providers = self.node.providers(parse_cid(self.param("cid")))
        self.send_json(200, {"providers": [provider.hex() for provider in providers]})

    def api_ping(self):
        """GET /api/ping?node=: round-trip time to a node, or null."""
        self.send_json(200, {"rtt": self.node.ping(parse_hex_id(self.param("node")))})

    def api_name_list(self):
        """GET /api/name/list: this node's names."""
        self.send_json(200, {"names": self.node.name_list()})

    def api_name_resolve(self):
        """GET /api/name/resolve?name=: the record a name points at (persisting its sequence)."""
        name = self.param("name")
        parse_hex_id(name, "name")
        self.send_json(200, self.node.name_resolve(name))

    # POST endpoints

    def api_add(self):
        """POST /api/add {"path", "wrap", "only_hash"}: import a file or directory on this machine."""
        body = self.json_body()
        self.send_json(200, self.node.add_path(parse_path(body.get("path")), wrap=parse_bool(body.get("wrap")),
                                               only_hash=parse_bool(body.get("only_hash"))))

    def api_add_stream(self):
        """POST /api/add/stream?name=&only_hash=: import the request body, wrapped as name if given."""
        reader = self.body_reader()
        size = None if reader.chunked else reader.remaining
        name = self.param("name", False) or None
        only_hash = parse_bool(self.param("only_hash", False) or "")
        self.send_json(200, self.node.add_stream(reader, size=size, name=name, only_hash=only_hash))

    def api_get(self):
        """POST /api/get {"target", "output", "from"}: fetch and write out to a path on this machine."""
        body = self.json_body()
        from_node = parse_hex_id(body["from"]) if body.get("from") else None
        self.send_json(200, self.node.get(parse_target(body.get("target")), parse_path(body.get("output")),
                                          from_node=from_node))

    def api_dir_set(self):
        """POST /api/dir/set {"target", "name", "cid", "executable"}: a new directory with an entry set."""
        body = self.json_body()
        name = body.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError("an entry name is required")
        self.send_json(200, self.node.dir_set(parse_target(body.get("target")), name, parse_cid(body.get("cid")),
                                              executable=parse_bool(body.get("executable"))))

    def api_dir_rm(self):
        """POST /api/dir/rm {"target"}: a new directory without the target's entry."""
        target = parse_target(self.json_body().get("target"))
        if "/" not in target.rstrip("/"):
            raise ValueError("dir rm needs <cid>/<path>: the entry to remove")
        self.send_json(200, self.node.dir_rm(target))

    def api_pin_add(self):
        """POST /api/pin/add {"target"}: fetch and pin with source user; answers once complete."""
        self.send_json(200, self.node.pin_add(parse_target(self.json_body().get("target"))))

    def api_pin_rm(self):
        """POST /api/pin/rm {"cid"}: remove the user source."""
        self.node.pin_rm(parse_cid(self.json_body().get("cid")))
        self.send_json(200, {})

    def api_pin_remote(self):
        """POST /api/pin/remote {"node", "cid", "pin"}: ask a node to pin (or release) a CID."""
        body = self.json_body()
        if not isinstance(body.get("pin"), bool):
            raise ValueError("pin must be true or false")
        accepted = self.node.pin_remote(parse_hex_id(body.get("node")), parse_cid(body.get("cid")), body["pin"])
        self.send_json(200, {"accepted": bool(accepted)})

    def api_gc(self):
        """POST /api/gc: evict every cached fetch."""
        self.json_body()
        self.send_json(200, self.node.gc())

    def api_verify(self):
        """POST /api/verify: re-check every stored block."""
        self.json_body()
        self.send_json(200, self.node.verify())

    def api_name_create(self):
        """POST /api/name/create {"alias"}: a new name key."""
        self.send_json(200, self.node.name_create(parse_alias(self.json_body().get("alias"))))

    def api_name_rm(self):
        """POST /api/name/rm {"alias"}: delete a name key and release its pin."""
        self.node.name_rm(parse_alias(self.json_body().get("alias")))
        self.send_json(200, {})

    def api_name_export(self):
        """POST /api/name/export {"alias"}: the name's key bytes. POST, so it can never be a cached GET."""
        self.send_bytes(bytes(self.node.name_export(parse_alias(self.json_body().get("alias")))))

    def api_name_import(self):
        """POST /api/name/import?alias=: store the key in the body under alias."""
        alias = parse_alias(self.param("alias"))
        key = self.read_body(MAX_KEY_BODY)
        if not key:
            raise ValueError("the request body must be the key")
        self.send_json(200, self.node.name_import(alias, key))

    def api_name_publish(self):
        """POST /api/name/publish {"alias", "target"}: point a name at a target."""
        body = self.json_body()
        self.send_json(200, self.node.name_publish(parse_alias(body.get("alias")), parse_target(body.get("target"))))

    def api_export(self):
        """POST /api/export {"target", "output"}: write an archive to a path on this machine."""
        body = self.json_body()
        self.send_json(200, self.node.export(parse_target(body.get("target")), parse_path(body.get("output"))))

    def api_import(self):
        """POST /api/import {"path"}: import an archive from a path on this machine."""
        self.send_json(200, self.node.import_archive(parse_path(self.json_body().get("path"))))
