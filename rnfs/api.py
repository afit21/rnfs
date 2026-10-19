"""Local JSON API of a running node (specification section 11), bound to 127.0.0.1 only.

Every command of the CLI is one request. Targets are `<cid>[/name…]` strings; node IDs and names are
32 hex characters; paths are absolute paths on the daemon's machine. Successful responses are JSON
(except cat and name export, which send bytes); errors are `{"error": message, "code": code}` (below).

| Method | Path | Parameters | Response / facade call |
|---|---|---|---|
| GET  | /api/status | | `status()`; without the token only `{"version"}` |
| GET  | /api/info | `target`, `from` | `info(target)` (fetched from node `from` if given) |
| GET  | /api/cat | `target`, `offset`, `length`, `raw`, `block`, `from` | the bytes of a file (`read`; sealed files decrypted unless `raw`; fetched from node `from` if given), Content-Length set; `block=1`: the CID's own block |
| GET  | /api/ls | `target` | `{"entries": ls(target)}` |
| GET  | /api/stat | `target` | `stat(target)` |
| GET  | /api/pin/ls | `cid` (optional) | `{"pins": pin_ls(cid)}`: `cid`, `sources`, `complete`, `type`, `sealed`, `name`, and since 0.2.0 `label`, `origin`, `size`, `entries`, `added` |
| GET  | /api/providers | `cid` | `{"providers": [node ID]}` |
| GET  | /api/ping | `node` | `{"rtt": seconds or null}` |
| GET  | /api/name/list | | `{"names": name_list()}` |
| GET  | /api/name/resolve | `name` | `name_resolve(name)` |
| GET  | /api/name/watch | `names` (comma-separated), `after` (a sequence or `-` per name), `wait` (≤ 30 s) | `{"records": [{"name", "cid", "sequence", "age"}]}`: the records held whose sequence is above `after`, as soon as there is one or after `wait` (below) |
| POST | /api/add | JSON `path`, `wrap`, `only_hash`, `pin`, `to` (list), `to_self`, `seal_name` (bool), `chunker`; or `nomadnet` (a NomadNet link) instead of `path`, with `name` and `identify` too | `add_path(...)`; with `nomadnet`, `add_nomadnet(...)`: its result has the snapshot's `name` and the `link` too |
| POST | /api/add/stream | query `name`, `only_hash`, `pin`, `to` (comma-separated), `to_self`, `seal_name` (the name), `chunker`, `format`; body is the data | `add_stream(...)`; `format=tar`: the body is a tar stream, `add_tar(stream, name, ...)` builds the directory it holds (no `to`, `to_self` or `seal_name`) |
| GET  | /api/get/stream | `target`, `format` (`tar`) | the target as a tar stream (`get_tar`), fetched first, Content-Length set |
| GET  | /api/export/stream | `target` | the target's archive (`export_chunks`), byte-identical to `export`, Content-Length set |
| POST | /api/import/stream | body is an archive | `import_stream(body)`, as `/api/import` answers |
| POST | /api/get | JSON `target`, `output`, `from`, `raw`, `no_extract` | `get(...)`; a sealed folder is unpacked unless `no_extract` |
| POST | /api/dir/set | JSON `target`, `name`, `cid`, `executable` | `dir_set(...)` |
| POST | /api/dir/rm | JSON `target` | `dir_rm(target)` |
| POST | /api/pin/add | JSON `target` | `pin_add(target)` |
| POST | /api/pin/rm | JSON `cid` | `{}` after `pin_rm(cid)` |
| POST | /api/pin/remote | JSON `node`, `cid`, `pin` | `{"accepted": bool}` |
| GET  | /api/pin/remote | `node`, `cid` (both optional), `check` | `{"pins": pin_remote_ls(...)}`: the remote pins this node asked for, most recently asked first (`node`, `cid`, `pin`, `accepted`, `asked`, `state`, `done`, `checked`, `seen`); `check=1` checks them first (provider lookups, paced) |
| GET  | /api/pin/remote/served | | `pin_remote_served()`: `{"pins": [{"identity", "cid", "state", "reason", "at", "detail"}], "usage": [{"identity", "used", "quota", "pinned", "waiting", "allowed"}], "quota"}` |
| POST | /api/pin/label | JSON `cid`, `label` (null or "" clears it) | `pin_label(cid, label)`: the pin's `pin_ls` entry |
| GET  | /api/cache/ls | | `{"cache": cache_ls()}`: the cached fetches, most recently used first |
| POST | /api/cache/rm | JSON `cid` | `cache_rm(cid)`: `{"removed_records", "removed_blocks", "freed_bytes"}` |
| POST | /api/gc, /api/verify | | `gc()`, `verify()` |
| POST | /api/name/create | JSON `alias` | `name_create(alias)` |
| POST | /api/name/rm | JSON `alias` | `{}` after `name_rm(alias)` |
| POST | /api/name/export | JSON `alias` | the key bytes |
| POST | /api/name/import | query `alias`; body is the key bytes | `name_import(alias, key)` |
| POST | /api/name/publish | JSON `alias`, `target` | `name_publish(alias, target)` |
| POST | /api/export | JSON `target`, `output` | `export(target, output)` |
| POST | /api/import | JSON `path` | `import_archive(path)` |
| GET  | /api/capabilities | | `{"version", "capabilities": [...], "routes": [[method, path]...], "features": [...], "event_kinds": [...], "error_codes": [...], "deprecated": [feature]}` (below) |
| GET  | /api/events | `after` (a sequence; absent: none, only `next`), `kinds` (comma-separated), `epoch`, `wait` (≤ 30 s) | `{"events": [event], "next", "lost", "epoch"}` (rnfs.events): the events after `after`, as soon as there is one or after `wait` |
| POST | /api/shutdown | | `{"stopping": true}`, then the daemon stops as on SIGTERM (`unsupported` 404 where nothing can stop it) |
| GET  | /api/apps | | `{"apps": app_ls()}`: `[{"name", "created"}]` (the CLI's token only, below) |
| POST | /api/apps/add | JSON `name` | `app_add(name)`: `{"name", "created", "token"}`, the only time the token is given |
| POST | /api/apps/rm | JSON `name` | `app_rm(name)`: `{"released"}`, the app's token revoked and its pins released |

The features (rnfs.features, each's `api_routes()`) add these; `name` is a name or a name contact's nick:

| Method | Path | Parameters | Response |
|---|---|---|---|
| GET  | /api/jobs | | `{"jobs": [job]}`, oldest first |
| POST | /api/jobs | JSON `kind` (get, pin, export), `target`, `output`, `from`, `sealed_name`, `fallback_dht`, `no_extract` | the job queued |
| GET  | /api/jobs/get | `id`, `wait` (≤ 30 s) | the job, once it ends or after `wait` |
| POST | /api/jobs/cancel | JSON `id` | the job |
| POST | /api/jobs/clear | JSON `id` (optional) | `{"removed": count}` of the finished jobs forgotten |
| GET  | /api/peers | | `{"peers": [...]}` |
| GET  | /api/stats | | traffic totals, refusals and per-interface bytes |
| POST | /api/name/keep | JSON `name` | the kept name |
| POST | /api/name/unkeep | JSON `name` | `{}` |
| GET  | /api/name/kept | | `{"kept": [{"name", "reasons", "sequence", "cid", "emitted"}]}` |
| POST | /api/name/follow | JSON `name` | the followed name |
| POST | /api/name/unfollow | JSON `name` | `{}` |
| GET  | /api/name/following | | `{"following": [{"name", "pinned", "latest", "sequence", "state"}]}` |
| GET  | /api/contacts | | `{"contacts": [{"nick", "kind", "id"}]}` |
| GET  | /api/contacts/get | `nick` | the contact |
| POST | /api/contacts/add | JSON `nick`, `id`, `kind` (optional) | the contact |
| POST | /api/contacts/rm | JSON `nick` | `{}` |
| GET  | /api/lxmf/address | | `{"address", "display_name", "propagation_node", "nearest_propagation_node"}` |
| POST | /api/lxmf/send | JSON `to`, `target`, `message`, `propagate`, `nearest` | `{"id", "hash", "to", "method"}` |
| GET  | /api/lxmf/inbox | | `{"messages": [...]}` |
| POST | /api/lxmf/inbox/get | JSON `id`, `link`, `output` or `directory`, `use_sent_cid` | `{"results": [...]}` |
| POST | /api/lxmf/inbox/rm | JSON `id`, or `all` | `{"removed": count}` |
| GET  | /api/lxmf/outbox | | `{"messages": [...]}` |
| POST | /api/lxmf/sync | JSON `nearest` | `{"propagation_node", "started"}` |
| GET  | /api/deny | | `{"entries": [local entry], "lists": [{"source", "cid", "entries", "state"}]}` |
| POST | /api/deny/add | JSON `entries` | `{"added": [entries new to the local list]}` |
| POST | /api/deny/rm | JSON `entries` | `{"removed": [...]}` (404 if one isn't on the local list) |
| GET  | /api/config | | `{"path", "settings": [schema entry + "value", "file", "overridden", "pending"], "file_error", "warnings"}` (rnfs.features.settings) |
| POST | /api/config/set | JSON `settings` ({name: value, or null to remove it from rnfs.toml}) | `{"path", "applied", "restart_required", "overridden", "warnings"}`; nothing written unless the whole result is valid (400) |

Both tables are checked against ApiHandler.ROUTES and the features' routes (tests/test_api.py).

Errors are `{"error": message, "code": code}`: code is machine-readable (rnfs.errors.CODES, design.md "Error
codes"), so a client tells "no such endpoint" (`no_such_endpoint`, an older daemon) from "no such content"
(`not_found`), both 404, without reading the message. `GET /api/capabilities` lists what this daemon offers
(CAPABILITIES, every route and feature), so a client checks once instead of trying each endpoint.

Deprecated features (rnfs.features, Feature.deprecated; in 0.2.0 lxmf, which moves to an app of its own): their
routes work as before, but every response of theirs, errors included, carries `Deprecation: @<Unix time>` (RFC
9745, DEPRECATION_HEADER), and `GET /api/capabilities` lists them under `deprecated`. Bodies are unchanged.

Events (rnfs.events, `GET /api/events`): a long poll like the name watch below, at most MAX_EVENT_WAITERS waiting
at once (503 beyond); `after` absent answers at once with the `next` to start from.

GET parameters are in the query string. Request bodies may use Content-Length or chunked encoding.

Watching names (rnfs.Client's names.watch): `GET /api/name/watch` is a long poll, level-triggered so a client loses
nothing between calls. Each call marks its names (at most MAX_WATCH_NAMES) watched until WATCH_GRACE seconds after
it ends (NameManager.watch: their announces count as asked), waits on NameManager's record listener, and answers
from the records held; it sends nothing to the network. At most MAX_WATCH_WAITERS calls wait at once (503 beyond;
a `wait=0` call never waits, so it is always answered), so watchers can't take the API's connections, and at most
MAX_WATCHED names stay watched.

Security:
- The Host header must be `127.0.0.1:<port>` or `localhost:<port>`, which blocks DNS rebinding.
- Every request except `GET /api/status` must carry `X-Rnfs-Api: <token>`, the random token the daemon
  writes to `<rnfs dir>/api_token` (mode 0600) at each start. Every local process, of every user, can
  connect to 127.0.0.1, and the API reads and writes files as the daemon's user, so only those who can
  read the rnfs directory may use it. A custom header also forces a CORS preflight, which fails because
  the API never sends CORS headers, and a web page can't know the token anyway. Status stays open for
  health checks and the CLI's proof check below, but without the token it answers only `{"version": ...}`:
  the node and identity hashes, LXMF address and traffic counters need the token like everything else.
- A request carrying `X-Rnfs-Challenge: <32 hex digits>` gets `X-Rnfs-Proof: <rnfs.apiclient.api_proof>` in its
  response, which only a server knowing the token can make. Before it first sends the token, a client
  (rnfs.apiclient, which the CLI uses) makes whatever listens on the port prove this (with a status request),
  so another local user listening there while the daemon is down learns neither the token nor a name key being
  imported.
- App tokens (rnfs.apps; 0.2.0): a request naming an app in `X-Rnfs-App: <name>` passes with that app's token in
  `X-Rnfs-Api` instead, and only with it (an app's token without its name, or the API token with one, doesn't
  pass); its proof is keyed by the app's verifier (apiclient.app_proof_key), and none is sent for an app there
  isn't. ApiHandler.caller() says who called: CLI ("cli") or "app:<name>", which dispatch keeps in principal.
  An app's token does everything the API token does but manage apps: the /api/apps routes (CLI_ROUTES) answer it
  403 `forbidden`. The routes that pin or release for the caller (add, add/stream, dir/set, dir/rm, pin/add,
  pin/rm, import, import/stream, and the pin, add and import jobs) use source "user" for the CLI and
  "app:<name>" for an app, "app:<name>:<key>" with `X-Rnfs-Pin-Key: <key>` (pin_source; with the CLI's token a
  pin key is refused, 400): an app's pin rm releases only its own.
- Errors map to statuses as design.md says (rnfs.errors.STATUSES): ValueError 400, NotFound 404, TooLarge 413,
  Denied 451 (the deny list), InvalidDag 422, Busy 503, StorageFull 507, other errors 500. A 500 is logged as an
  error, and a BlockMismatch (a 400, but a bug: the node handed the store a block that doesn't match) as a
  warning, with its traceback in the debug log.

Connections (both this API and the gateway, through LocalServer):
- The request line, headers and any JSON body must arrive within request_timeout of connecting, else the
  connection is dropped (408 if the headers were complete); a streamed upload may pause up to body_timeout
  between reads. Node work has no time limit: socket timeouts only apply while the socket is used, and
  writing a response may block for response_timeout (None: forever, for `cat | less`).
- At most max_connections are served at once; more get an immediate 503 and are closed, so held-open
  connections can't exhaust threads or file descriptors.
"""
import hmac
import io
import json
import os
import re
import secrets
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from rnfs import __version__, events, fsutil, ids, nomadnet
from rnfs import node_content  # --- content: add options (pin, to, chunker) ---
from rnfs.apiclient import API_HEADER, APP_HEADER, CHALLENGE_HEADER, PIN_KEY_HEADER, PROOF_HEADER, api_proof
from rnfs.content.cid import Cid
from rnfs.content.storage import APP_PREFIX, app_source, check_app_name
from rnfs.errors import CODES, STATUSES, BlockMismatch, Busy, Unsupported, error_code
from rnfs.log import LOG_ERROR, LOG_NOTICE, LOG_WARNING, log

MAX_JSON_BODY = 1024 * 1024  # JSON parameters are tiny; this only stops a runaway body filling memory
MAX_KEY_BODY = 64 * 1024  # an RNS identity file is 64 bytes
CHALLENGE = re.compile(r"[0-9a-f]{32}")  # what the CLI sends in X-Rnfs-Challenge
CLI = "cli"  # ApiHandler.caller() for the CLI's token (api_token); an app's is "app:<name>"
CHUNK_SIZE_LINE = re.compile(rb"[0-9A-Fa-f]{1,16}")  # a chunk size: hex digits only (no 0x, +, _ or spaces)
MAX_TRAILER_LINES = 32
MAX_WATCH_NAMES = 64  # names one GET /api/name/watch may list
MAX_WATCH_WAITERS = 4  # GET /api/name/watch calls waiting at once
MAX_WATCH_WAIT = 30  # seconds one may wait, as GET /api/jobs/get's wait=
WATCH_GRACE = 60.0  # seconds a name stays watched after the last call naming it ends
MAX_WATCHED = 1024  # names watched for API clients at most; the soonest to end go first
MAX_EVENT_WAITERS = 8  # GET /api/events calls waiting at once
DEPRECATION_HEADER = "Deprecation"  # RFC 9745, on the responses of a deprecated feature's routes
# What this daemon offers beyond the routes themselves (GET /api/capabilities), each a promise kept once released
CAPABILITIES = ("error_codes", "capabilities", "events", "shutdown", "daemon_json", "pin_details", "pin_label",
                "cache", "peer_details", "config", "jobs_add", "jobs_import", "add_tar", "get_tar", "export_stream",
                "import_stream", "sealed_folders", "apps", "remote_pin_status", "add_nomadnet")

# Exception class → HTTP status, checked in order (design.md); anything else is 500. The one table, rnfs.errors'
ERROR_STATUSES = STATUSES


_feature_routes: dict | None = None
_deprecated_routes: dict | None = None


def feature_routes() -> dict:
    """The features' API routes (rnfs.features), loaded once."""
    global _feature_routes
    if _feature_routes is None:
        from rnfs import features
        _feature_routes = features.api_routes()
    return _feature_routes


def deprecated_routes() -> dict:
    """The deprecated features' API routes, route -> when deprecated (a Unix time), loaded once."""
    global _deprecated_routes
    if _deprecated_routes is None:
        from rnfs import features
        _deprecated_routes = features.deprecated_routes()
    return _deprecated_routes


def capabilities(node, without=()) -> dict:
    """GET /api/capabilities' answer for node: {"version", "capabilities" (CAPABILITIES but those in without),
    "routes" (every core and feature route, by path), "features" (those node loaded), "event_kinds",
    "error_codes", "deprecated" (the features that still work but a later release removes)}. rnfs.Node's
    capabilities() too, without what an embedded node can't do."""
    from rnfs import features
    loaded = getattr(node, "features", None) or {}
    routes = sorted({*ApiHandler.ROUTES, *feature_routes()}, key=lambda route: (route[1], route[0]))
    return {"version": __version__, "capabilities": sorted(set(CAPABILITIES) - set(without)),
            "routes": [list(route) for route in routes], "features": sorted(loaded),
            "event_kinds": list(events.EVENT_KINDS), "error_codes": list(CODES),
            "deprecated": features.deprecated_features()}


def error_status(error: Exception) -> int:
    """The HTTP status for an exception raised by the node."""
    for error_class, status in ERROR_STATUSES:
        if isinstance(error, error_class):
            return status
    return 500


# text with non-printable characters escaped; rnfs.log.log escapes every message too, so this is kept for
# callers and tests that escape a value on its own
loggable = fsutil.printable


def raw_response(status: str, headers: dict, body: bytes) -> bytes:
    """A complete HTTP/1.1 response that closes the connection, for answering without a handler."""
    lines = [f"HTTP/1.1 {status}", f"Content-Length: {len(body)}", "Connection: close",
             *(f"{name}: {value}" for name, value in headers.items())]
    return ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1") + body


def host_allowed(host: str | None, port: int) -> bool:
    """True if a Host header names this server as 127.0.0.1:<port> or localhost:<port> (DNS rebinding)."""
    if host is None:
        return False
    return host.lower() in (f"127.0.0.1:{port}", f"localhost:{port}")


def parse_target(text) -> str:
    """text if it looks like `<cid>[/name…]`; the node checks the path. Raises ValueError."""
    if not isinstance(text, str) or not text:
        raise ValueError("a target <cid>[/path] is required")
    from rnfs.features.contacts import is_nick  # a name contact's nick, which the node resolves to its CID
    if not is_nick(text.split("/", 1)[0]):
        Cid.parse(text.split("/", 1)[0])
    return text


def parse_cid(text) -> str:
    """text if it is a valid written CID, else ValueError."""
    if not isinstance(text, str):
        raise ValueError("a CID is required")
    Cid.parse(text)
    return text


def parse_hex_id(text, what: str = "node ID") -> bytes:
    """A 16-byte node ID or name written as 32 hex characters (spaces around allowed, no <angle brackets>).
    Raises ValueError."""
    return ids.parse_hex16(text, what)


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


def parse_nomadnet(text) -> str:
    """A NomadNet link (<32 hex>:/page/<path> or :/file/<path>, rnfs.nomadnet.parse) as written, or ValueError."""
    return str(nomadnet.parse(text))


def parse_entry_name(value) -> str | None:
    """An entry name (rnfs.nomadnet.is_valid_name), or None if absent or empty; ValueError otherwise."""
    if value is None or value == "":
        return None
    if not nomadnet.is_valid_name(value):
        raise ValueError(f"invalid entry name {value!r}")
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
        digits = line.split(b";", 1)[0].strip(b" \t\r\n")
        if not CHUNK_SIZE_LINE.fullmatch(digits):
            raise ValueError("invalid chunked request body")
        size = int(digits, 16)
        if size == 0:
            for _ in range(MAX_TRAILER_LINES + 1):  # trailers, up to the blank line
                if not self.rfile.readline(1024).strip():
                    break
            else:
                raise ValueError("too many trailer lines in a chunked request body")
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
        """True: a body is read."""
        return True


class RequestTimeout(TimeoutError):
    """The client took too long sending its request (a TimeoutError, so http.server drops the connection)."""


class TimedReader(io.RawIOBase):
    """A connection's raw input that applies the handler's current read time limit before every recv."""

    def __init__(self, sock: socket.socket, handler: "TimedHandler"):
        """Read from sock, with time limits from handler.read_timeout()."""
        self.sock = sock
        self.handler = handler

    def readable(self) -> bool:
        """True: the connection is read."""
        return True

    def readinto(self, buffer) -> int:
        """Receive into buffer, raising RequestTimeout if the client is too slow."""
        self.sock.settimeout(self.handler.read_timeout())
        try:
            return self.sock.recv_into(buffer)
        except TimeoutError:
            raise RequestTimeout("timed out reading the request") from None


class TimedHandler(BaseHTTPRequestHandler):
    """A request handler whose reads are time-limited: the whole request by a deadline, then per phase.

    LocalServer sets the three limits from its own attributes. Subclasses call reading_body() before
    streaming an upload and request_read() once the request is read, before node work and the response.
    """

    request_timeout: float = 15.0
    body_timeout: float = 60.0
    response_timeout: float | None = None
    # Not http.server's HTTP/0.9: responses to it have no status line or headers (no CSP, no nosniff), and that
    # includes the error sent for a malformed request line
    default_request_version = "HTTP/1.0"

    def parse_request(self) -> bool:
        """Parse the request line and headers, refusing HTTP/0.9 (a request line without a version, or with
        HTTP/0.9), so every response here is HTTP/1.x with its headers. False if refused or malformed."""
        words = str(self.raw_requestline, "iso-8859-1").split()
        if len(words) == 2 or (len(words) == 3 and words[2] == "HTTP/0.9"):
            self.command = None
            self.requestline = str(self.raw_requestline, "iso-8859-1").rstrip("\r\n")
            self.request_version = self.default_request_version
            self.close_connection = True
            self.send_error(400, "HTTP/0.9 is not supported")
            return False
        return super().parse_request()

    def setup(self):
        """Set up the connection, replacing rfile with one that enforces the time limits."""
        super().setup()
        self.deadline = time.monotonic() + self.request_timeout  # for the request line, headers and small bodies
        self.phase = "request"
        # Closed at once: the socket only really closes once every file made from it is closed
        self.rfile.close()
        self.rfile = io.BufferedReader(TimedReader(self.connection, self), io.DEFAULT_BUFFER_SIZE)

    def read_timeout(self) -> float | None:
        """The time limit for the next recv in the current phase. Raises RequestTimeout past the deadline."""
        if self.phase == "body":
            return self.body_timeout
        if self.phase == "done":
            return self.response_timeout
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise RequestTimeout("timed out reading the request")
        return remaining

    def reading_body(self):
        """Allow body_timeout between reads from now on: an upload may be large, so no overall deadline."""
        self.phase = "body"

    def request_read(self):
        """The request is fully read: from now on only writing the response is time-limited."""
        self.phase = "done"
        self.connection.settimeout(self.response_timeout)

    @classmethod
    def busy_response(cls) -> bytes:
        """The 503 sent to a connection refused because the server is full."""
        raise NotImplementedError


class BoundedHTTPServer(ThreadingHTTPServer):
    """A ThreadingHTTPServer that serves at most max_connections at once and refuses the rest with a 503."""

    daemon_threads = True  # long fetches mustn't keep the process alive on shutdown
    LINGER_SECONDS = 1.0  # longest a closing connection is drained (see shutdown_request)
    LINGER_BYTES = 256 * 1024  # most a closing connection drains before closing anyway
    MAX_CLOSERS = 16  # refused connections closed gently at once

    def __init__(self, address, handler_class, max_connections: int):
        """Bind to address; handler_class.busy_response() is sent to connections over max_connections."""
        self.slots = threading.BoundedSemaphore(max_connections)
        self.closers = threading.BoundedSemaphore(self.MAX_CLOSERS)
        super().__init__(address, handler_class)

    def server_bind(self):
        """Bind the listening socket, exclusively on Windows."""
        if fsutil.IS_WINDOWS:
            # There SO_REUSEADDR (HTTPServer's default) lets another socket bind a port already listening, so a
            # second daemon, or another local program listening in on API tokens, could share it
            self.allow_reuse_address = False
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()

    def process_request(self, request, client_address):
        """Serve the connection on a new thread if a slot is free, else refuse it at once."""
        if not self.slots.acquire(blocking=False):
            return self.refuse(request)
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.slots.release()  # the thread never started
            raise

    def process_request_thread(self, request, client_address):
        """Serve the connection, free its slot, then close it (gently, if a closer slot is free)."""
        try:
            self.finish_request(request, client_address)
        except Exception:  # noqa: BLE001 - as socketserver does: report, then close the connection
            self.handle_error(request, client_address)
        finally:
            # Freed before the lingering close, so a client slow to close doesn't hold a connection slot
            self.slots.release()
            if self.closers.acquire(blocking=False):
                try:
                    self.shutdown_request(request)
                finally:
                    self.closers.release()
            else:
                self.close_request(request)

    def refuse(self, request):
        """Send the 503 and close, without blocking: this runs on the thread that accepts connections.

        The lingering close (see shutdown_request) runs on a short-lived thread, at most MAX_CLOSERS at once;
        beyond that the connection is closed at once, and the client may see a reset instead of the 503.
        """
        try:
            request.setblocking(False)
            request.sendall(self.RequestHandlerClass.busy_response())
        except OSError:
            pass  # the client has gone, or isn't reading; it is closed either way
        if self.closers.acquire(blocking=False):
            def close():
                """Close request gently, then free the closer slot."""
                try:
                    self.shutdown_request(request)
                finally:
                    self.closers.release()
            threading.Thread(target=close, name="rnfs-http-close", daemon=True).start()
        else:
            self.close_request(request)

    def shutdown_request(self, request):
        """Close a connection without losing the response: stop sending, drain what the client still sends,
        then close.

        Closing a socket with unread input makes the kernel send a reset, and on Windows a reset makes the
        client discard a response it hasn't read yet (e.g. a 403 sent before reading the request body).
        Draining is bounded by LINGER_BYTES and LINGER_SECONDS.
        """
        try:
            request.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        try:
            request.setblocking(True)
            deadline = time.monotonic() + self.LINGER_SECONDS
            drained = 0
            while drained < self.LINGER_BYTES:
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                request.settimeout(left)
                data = request.recv(65536)
                if not data:
                    break  # the client closed its side: nothing unread is left
                drained += len(data)
        except OSError:
            pass
        self.close_request(request)


class LocalServer:
    """A BoundedHTTPServer on 127.0.0.1 run on a daemon thread. Port 0 picks a free port.

    The connection limits are class attributes (see the module docstring), read when the server is made.
    """

    poll_interval = 0.5  # how often serve_forever checks for stop(); tests lower it to stop quickly
    request_timeout = 15.0  # seconds for the request line, headers and a JSON body to arrive
    body_timeout = 60.0  # seconds a streamed upload may pause between reads
    response_timeout: float | None = None  # seconds a response write may block; None: forever
    max_connections = 32

    def __init__(self, handler_class, port: int, **attributes):
        """Bind to 127.0.0.1:port with handler_class; attributes are set on a subclass of it."""
        attributes.update(request_timeout=self.request_timeout, body_timeout=self.body_timeout,
                          response_timeout=self.response_timeout)
        handler = type(handler_class.__name__, (handler_class,), attributes)
        self.server = BoundedHTTPServer(("127.0.0.1", port), handler, self.max_connections)
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


class NameWatches:
    """GET /api/name/watch's state, one per ApiServer: the names watched for API clients, each until WATCH_GRACE
    after the last call naming it ends, and the calls waiting. Each name in until holds exactly one
    NameManager.watch, so its lock is held while watching and unwatching there (taken before the names lock,
    never under it: the record listener, called with the names lock held, only sets the waiting calls' events)."""

    def __init__(self, node, clock=time.monotonic):
        """Watches on node's names (read only once a call comes)."""
        self.node = node
        self.clock = clock
        self.lock = threading.Lock()
        self.until: dict[bytes, float] = {}  # name -> when its watch ends (each one NameManager.watch)
        self.waiters: tuple = ()  # (names, event) of each call waiting; swapped whole, read without the lock
        self.listening = False

    def wait(self, names: list[bytes], after: list[int | None], wait: float) -> list[dict]:
        """The records held for names above the matching after (None: any), once there is one or after wait
        seconds. Raises Busy if wait > 0 with MAX_WATCH_WAITERS calls waiting already (a wait=0 call never
        waits, so it holds no connection and is always answered)."""
        manager = self.node.names
        event = threading.Event()
        waiter = (frozenset(names), event)
        with self.lock:
            if wait > 0:
                if len(self.waiters) >= MAX_WATCH_WAITERS:
                    raise Busy(f"{MAX_WATCH_WAITERS} name watches are waiting already")
                if not self.listening:
                    manager.add_record_listener(self._heard)
                    self.listening = True
                self.waiters = (*self.waiters, waiter)
            self._extend(names, self.clock() + wait + WATCH_GRACE)
        try:
            deadline = self.clock() + wait
            while True:
                found = self._records(names, after)
                remaining = deadline - self.clock()
                if found or remaining <= 0:
                    return found
                event.wait(remaining)
                event.clear()
        finally:
            with self.lock:
                self.waiters = tuple(item for item in self.waiters if item is not waiter)
                self._extend(names, self.clock() + WATCH_GRACE)

    def _extend(self, names: list[bytes], until: float):
        """Keep names (distinct) watched until at least until, unwatching those whose watch has ended, and the
        soonest to end past MAX_WATCHED, in NameManager too. Lock held."""
        manager = self.node.names
        now = self.clock()
        for name in [name for name, end in self.until.items() if end <= now and name not in names]:
            del self.until[name]
            manager.unwatch(name)
        for name in names:
            if name not in self.until:
                manager.watch(name)
            self.until[name] = max(self.until.get(name, until), until)
        while len(self.until) > MAX_WATCHED:
            name = min((item for item in self.until if item not in names), key=self.until.get)
            del self.until[name]
            manager.unwatch(name)

    def _records(self, names: list[bytes], after: list[int | None]) -> list[dict]:
        """The records held for names above the matching after."""
        found = []
        for name, sequence in zip(names, after):
            record = self.node.names.record(name)
            if record is not None and (sequence is None or record.sequence > sequence):
                found.append({"name": name.hex(), "cid": str(record.cid), "sequence": record.sequence,
                              "age": max(0.0, time.time() - record.heard)})
        return found

    def _heard(self, name: bytes, record):
        """NameManager's record listener (names lock held): wake the calls waiting for name."""
        for names, event in self.waiters:
            if name in names:
                event.set()


class ApiServer(LocalServer):
    """The local API for node (an RnfsNode, or anything with its facade methods).

    Every request but status must carry token (self.token); the daemon writes it to <rnfs dir>/api_token.
    """

    max_connections = 32  # the CLI makes one connection per command; this is plenty for scripts too

    def __init__(self, node, port: int, token: str | None = None, shutdown=None):
        """Bind to 127.0.0.1:port, requiring token (a new random one if None); serves once start() is called.
        shutdown, if given, is called (with no arguments) once POST /api/shutdown has been answered: the daemon
        passes what its signal handlers do. Without it that route answers 404 unsupported."""
        self.token = token if token is not None else secrets.token_urlsafe(32)
        if not self.token:
            raise ValueError("the API token must not be empty")
        super().__init__(ApiHandler, port, node=node, token=self.token, watches=NameWatches(node),
                         event_waiters=threading.BoundedSemaphore(MAX_EVENT_WAITERS),
                         shutdown_callback=None if shutdown is None else staticmethod(shutdown))


class ApiRequestError(Exception):
    """A request the API refuses before reaching the node: status, message and code (rnfs.errors.CODES)."""

    def __init__(self, status: int, message: str, code: str):
        """A refusal with status, message and code."""
        super().__init__(message)
        self.status = status
        self.code = code


class ApiHandler(TimedHandler):
    """Handles one API request using node, token and watches, which are set on the subclass ApiServer creates."""

    node = None
    token: str | None = None
    watches: NameWatches | None = None
    event_waiters: threading.BoundedSemaphore | None = None
    shutdown_callback = None  # what POST /api/shutdown calls once answered (ApiServer's shutdown)
    server_version = "rnfs"
    sys_version = ""

    # (method, path) → handler method name, as the module docstring's table lists them (test_api checks both).
    # GET routes never change pins, names or content
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
        ("GET", "/api/name/watch"): "api_name_watch",
        ("POST", "/api/add"): "api_add",
        ("POST", "/api/add/stream"): "api_add_stream",
        ("GET", "/api/get/stream"): "api_get_stream",
        ("GET", "/api/export/stream"): "api_export_stream",
        ("POST", "/api/import/stream"): "api_import_stream",
        ("POST", "/api/get"): "api_get",
        ("POST", "/api/dir/set"): "api_dir_set",
        ("POST", "/api/dir/rm"): "api_dir_rm",
        ("POST", "/api/pin/add"): "api_pin_add",
        ("POST", "/api/pin/rm"): "api_pin_rm",
        ("POST", "/api/pin/remote"): "api_pin_remote",
        ("GET", "/api/pin/remote"): "api_pin_remote_ls",
        ("GET", "/api/pin/remote/served"): "api_pin_remote_served",
        ("POST", "/api/gc"): "api_gc",
        ("POST", "/api/verify"): "api_verify",
        ("POST", "/api/name/create"): "api_name_create",
        ("POST", "/api/name/rm"): "api_name_rm",
        ("POST", "/api/name/export"): "api_name_export",
        ("POST", "/api/name/import"): "api_name_import",
        ("POST", "/api/name/publish"): "api_name_publish",
        ("POST", "/api/export"): "api_export",
        ("POST", "/api/import"): "api_import",
        ("POST", "/api/pin/label"): "api_pin_label",
        ("GET", "/api/cache/ls"): "api_cache_ls",
        ("POST", "/api/cache/rm"): "api_cache_rm",
        ("GET", "/api/capabilities"): "api_capabilities",
        ("GET", "/api/events"): "api_events",
        ("POST", "/api/shutdown"): "api_shutdown",
        ("GET", "/api/apps"): "api_apps",
        ("POST", "/api/apps/add"): "api_app_add",
        ("POST", "/api/apps/rm"): "api_app_rm",
    }
    OPEN_ROUTES = {("GET", "/api/status")}  # allowed without the token
    # Only the CLI's token (api_token) may use these: an app can't issue itself or another app a token, nor revoke one
    CLI_ROUTES = {("GET", "/api/apps"), ("POST", "/api/apps/add"), ("POST", "/api/apps/rm")}
    PROOF_ROUTE = ("GET", "/api/status")
    proved = False  # this connection had its proof request kept open: it is closed after the next request
    principal: str | None = None  # caller() of the request being answered, set by dispatch
    keep_open = False  # the request being answered is a proof request whose connection stays open
    deprecated_since: int | None = None  # set by dispatch for a deprecated feature's route: its Deprecation date

    def log_message(self, format, *args):
        """Log nothing: RNS does the node's logging, and per-request lines would drown it out."""

    @classmethod
    def busy_response(cls) -> bytes:
        """503 for a connection over max_connections."""
        body = json.dumps({"error": "too many API connections; try again shortly",
                           "code": "too_many_connections"}).encode()
        return raw_response("503 Service Unavailable", {"Content-Type": "application/json", "Retry-After": "10",
                                                        "Cache-Control": "no-store",
                                                        "X-Content-Type-Options": "nosniff"}, body)

    def caller(self) -> str | None:
        """Who sent the request: CLI ("cli") for this server's token in X-Rnfs-Api; "app:<name>" (the app's pin
        source) for app name's token with X-Rnfs-App naming it; None for neither. Tokens are compared in constant
        time. Without X-Rnfs-App only the API token passes, so a request is never read as an app's by mistake."""
        value = self.headers.get(API_HEADER)
        if value is None:
            return None
        app = self.headers.get(APP_HEADER)
        if app is None:
            # Bytes, since compare_digest refuses non-ASCII str and a header may hold any Latin-1 text
            return CLI if hmac.compare_digest(value.strip().encode("utf-8", "replace"), self.token.encode()) else None
        apps, name = getattr(self.node, "apps", None), app.strip()
        if apps is None or not apps.verify(name, value.strip()):
            return None
        return APP_PREFIX + name

    def token_valid(self) -> bool:
        """True if the request carries a token that passes (the API token, or an app's with X-Rnfs-App)."""
        return self.caller() is not None

    def proof_key(self) -> str | None:
        """What keys this response's proof: the API token, or (X-Rnfs-App) that app's verifier (rnfs.apps), None
        for an app there isn't (no proof is sent then)."""
        app = self.headers.get(APP_HEADER)
        if app is None:
            return self.token
        apps = getattr(self.node, "apps", None)
        return apps.proof_key(app.strip()) if apps is not None else None

    def pin_source(self) -> dict:
        """The source the routes that pin or release for the caller use, as the facade's keyword argument: {} for
        the CLI (the facade's "user"), {"source": "app:<name>"} for an app, or "app:<name>:<key>" with
        X-Rnfs-Pin-Key. ValueError for a pin key with the CLI's token, or one that isn't valid."""
        key = self.headers.get(PIN_KEY_HEADER)
        key = None if key is None else key.strip()
        if self.principal is None or not self.principal.startswith(APP_PREFIX):
            if key is not None:
                raise ValueError(f"{PIN_KEY_HEADER} is for apps: the CLI's pins are always user's")
            return {}
        return {"source": app_source(self.principal[len(APP_PREFIX):], key)}

    # Responses

    def send_common_headers(self):
        """The proof, on every response of the API's own (end_headers adds the headers every response carries)."""
        challenge = (self.headers.get(CHALLENGE_HEADER) or "").strip()
        key = self.proof_key() if CHALLENGE.fullmatch(challenge) else None
        if key is not None:
            self.send_header(PROOF_HEADER, api_proof(key, self.server.server_address[1], challenge))
        if self.deprecated_since is not None:
            # RFC 9745: the route still works, but is deprecated since that date; errors carry it too, so a
            # client sees it whatever the route answers
            self.send_header(DEPRECATION_HEADER, f"@{self.deprecated_since}")
        if self.keep_open:
            self.send_header("Connection", "keep-alive")

    def send_json(self, status: int, body):
        """Send body as a JSON response with the given status."""
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_common_headers()
        self.end_headers()
        self.wfile.write(data)

    def send_error_json(self, status: int, message: str, code: str, headers: dict | None = None):
        """Send {"error": message, "code": code} with the given status."""
        data = json.dumps({"error": message, "code": code}).encode()
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
        """Check Host and the token, route the request, and map exceptions to error responses."""
        # One request per connection keeps body handling simple, but for a proof request: its client then sends
        # its request on that connection, which can only reach this server (apiclient.ApiConnection._prove)
        self.close_connection = True
        self.keep_open = False
        self.deprecated_since = None
        url = urlsplit(self.path)
        self.query = parse_qs(url.query, keep_blank_values=True)
        self.headers_sent = False
        try:
            if not host_allowed(self.headers.get("Host"), self.server.server_address[1]):
                raise ApiRequestError(403, "Host header must be 127.0.0.1:<port> or localhost:<port>", "bad_host")
            route = (self.command, url.path)
            name = self.ROUTES.get(route)
            feature_route = None if name is not None else feature_routes().get(route)
            if name is None and feature_route is None:
                allowed = sorted(method for method, path in (*self.ROUTES, *feature_routes()) if path == url.path)
                if allowed:
                    return self.send_error_json(405, f"use {' or '.join(allowed)}", "method_not_allowed",
                                                {"Allow": ", ".join(allowed)})
                raise ApiRequestError(404, f"no such API endpoint: {url.path}", "no_such_endpoint")
            if feature_route is not None:
                self.deprecated_since = deprecated_routes().get(route)
            self.principal = self.caller()
            if route not in self.OPEN_ROUTES and self.principal is None:
                if self.headers.get(APP_HEADER) is not None:
                    raise ApiRequestError(403, f"an app's requests must carry the header {API_HEADER}: <its token>, "
                                               f"as rnfs app add gave it, for the app {APP_HEADER} names",
                                          "unauthorized")
                raise ApiRequestError(403, f"API requests must carry the header {API_HEADER}: <token>, with the "
                                           f"token the daemon wrote to <rnfs dir>/api_token", "unauthorized")
            if route in self.CLI_ROUTES and self.principal != CLI:
                raise ApiRequestError(403, "only the CLI's token (<rnfs dir>/api_token) manages apps; an app's token "
                                           "can't", "forbidden")
            if self.command != "POST":
                self.request_read()  # no body; POST endpoints call it once they have read theirs
            self.keep_open = (route == self.PROOF_ROUTE and not self.proved
                              and CHALLENGE.fullmatch((self.headers.get(CHALLENGE_HEADER) or "").strip()) is not None
                              and "keep-alive" in (self.headers.get("Connection") or "").lower()
                              and not self.headers.get("Content-Length") and not self.headers.get("Transfer-Encoding"))
            if feature_route is not None:
                feature_route(self)
            else:
                getattr(self, name)()
            if self.keep_open:
                # The next request on this connection gets the time limits of a new one, and is its last
                self.proved = True
                self.close_connection = False
                self.deadline = time.monotonic() + self.request_timeout
                self.phase = "request"
        except ApiRequestError as e:
            self.respond_error(e.status, str(e), e.code)
        except RequestTimeout as e:
            self.respond_error(408, str(e), "timeout")
        except Exception as e:  # noqa: BLE001 - every error becomes a response, never a dropped connection
            status = error_status(e)
            if status == 500:
                log(f"API {self.command} {loggable(url.path)} failed: {e!r}", LOG_ERROR)
            elif isinstance(e, BlockMismatch):
                # Answered 400 as a ValueError, as before, but the node's own fault: logged, with its traceback
                log(f"API {self.command} {loggable(url.path)} failed: {e!r}", LOG_WARNING)
            message = str(e) or type(e).__name__
            self.respond_error(status, message, error_code(e, status),
                               {"Retry-After": "10"} if status == 503 else None)

    def respond_error(self, status: int, message: str, code: str, headers: dict | None = None):
        """Send an error response, unless a response is already under way (then just drop the connection)."""
        if self.headers_sent:
            return
        try:
            self.send_error_json(status, message, code, headers)
        except OSError:
            pass  # the client has gone away

    def end_headers(self):
        """Add the headers every response carries, http.server's own error pages too (400, 414, 431, 501...),
        then end the headers, remembering that a response has started. Deliberately no Access-Control-* headers
        (section 11)."""
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
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
            raise ApiRequestError(411, "Content-Length or chunked encoding required", "length_required")
        try:
            size = int(length)
        except ValueError:
            raise ApiRequestError(411, "invalid Content-Length", "length_required") from None
        if size < 0:
            raise ApiRequestError(400, "invalid Content-Length", "bad_request")
        return BodyReader(self.rfile, size, chunked=False)

    def stream_reader(self) -> BodyReader:
        """A reader for an upload of any size: reads may pause body_timeout, with no overall deadline."""
        reader = self.body_reader()
        self.reading_body()
        return reader

    def read_body(self, limit: int) -> bytes:
        """The whole request body, at most limit bytes (413 beyond that). An absent body is empty.

        The body must arrive within the request's deadline: these bodies are small.
        """
        if "Content-Length" not in self.headers and "Transfer-Encoding" not in self.headers:
            self.request_read()
            return b""
        data = self.body_reader().read(limit + 1)
        if len(data) > limit:
            raise ApiRequestError(413, "request body too large", "body_too_large")
        self.request_read()
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
        """GET /api/status: the node's summary with the token; without it, only the version (a health check)."""
        if self.principal is None:
            self.send_json(200, {"version": __version__})
            return
        self.send_json(200, self.node.status())

    def api_info(self):
        """GET /api/info?target=&from=: type, size and CID of a target (fetched from that node, if given)."""
        from_node = parse_hex_id(self.param("from", False)) if self.param("from", False) else None
        self.send_json(200, self.node.info(parse_target(self.param("target")), from_node=from_node))

    def api_cat(self):
        """GET /api/cat?target=&offset=&length=&raw=&block=&from=: stream a file's bytes, fetched as needed (block=1:
        the CID's own block)."""
        target = parse_target(self.param("target"))
        offset = parse_int(self.param("offset", False), "offset", 0)
        length = parse_int(self.param("length", False), "length", None)
        if parse_bool(self.param("block", False) or ""):
            return self.send_bytes(self.node.block(parse_cid(target)))  # cat --block: the CID's own block
        options = {"raw": True} if parse_bool(self.param("raw", False) or "") else {}  # sealed files: the ciphertext
        from_text = self.param("from", False)
        if from_text:
            options["from_node"] = parse_hex_id(from_text)
        # One fetch resolves the target, gives its info, then reads (only once the first chunk is asked for)
        info, chunks = self.node.read_file(target, offset=offset, length=length, **options)
        try:
            if info["type"] != "file":
                raise ValueError(f"{target} is a directory")
            # Content-Length is known up front, so a client sees a fetch failing mid-stream as a short body
            available = max(info["size"] - offset, 0)
            length = available if length is None else min(length, available)
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
                    log(f"API cat {loggable(target)} failed mid-stream: {e!r}", LOG_ERROR)
                return
            raise
        finally:
            close = getattr(chunks, "close", None)
            if close:
                close()  # stop a generator's fetch if the client went away

    def api_get_stream(self):
        """GET /api/get/stream?target=&format=tar: the target as a tar (RnfsNode.get_tar), its whole DAG fetched
        first so the response has an exact Content-Length."""
        target = parse_target(self.param("target"))
        if self.param("format") != "tar":
            raise ValueError("format must be tar")
        self.send_chunks("get/stream", target, "application/x-tar", lambda: self.node.get_tar(target))

    def api_export_stream(self):
        """GET /api/export/stream?target=: the target's archive (RnfsNode.export_chunks), as `export` writes it."""
        target = parse_target(self.param("target"))
        self.send_chunks("export/stream", target, "application/vnd.rnfs.archive",
                         lambda: self.node.export_chunks(target))

    def send_chunks(self, what: str, target: str, content_type: str, open_chunks):
        """Answer with what open_chunks() gives, (info with "length", chunks), sent with that exact Content-Length.

        open_chunks fetches the whole DAG first, while a client that disconnects cancels it (as the gateway's
        ?format=archive); an error before the first byte gets its status, one after it cuts the body short, which
        the Content-Length tells the client."""
        from rnfs.errors import Cancelled
        from rnfs.fetcher import observing
        from rnfs.gateway_formats import ClientWatch
        try:
            with observing(ClientWatch(self.connection)):
                info, chunks = open_chunks()
        except Cancelled:
            self.close_connection = True  # nobody to answer
            return
        try:
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(info["length"]))
            self.send_common_headers()
            self.end_headers()
            for chunk in chunks:
                self.wfile.write(chunk)
        except Exception as e:
            if not self.headers_sent:
                raise
            if not isinstance(e, OSError):  # an OSError is the client gone
                log(f"API {what} {loggable(target)} failed mid-stream: {e!r}", LOG_ERROR)
            self.close_connection = True
        finally:
            chunks.close()  # ends the fetch

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
        """GET /api/name/resolve?name=: the record a name (or a name contact's nick) points at, persisted."""
        from rnfs.features.keepers import parse_name_arg  # names as keep and follow take them
        self.send_json(200, self.node.name_resolve(parse_name_arg(self.node, self.param("name")).hex()))

    def api_name_watch(self):
        """GET /api/name/watch?names=&after=&wait=: the records of watched names newer than after (NameWatches)."""
        texts = (self.param("names") or "").split(",")
        if not 1 <= len(texts) <= MAX_WATCH_NAMES:
            raise ValueError(f"names lists 1 to {MAX_WATCH_NAMES} names, comma-separated")
        names = [parse_hex_id(text, "name") for text in texts]
        after_text = self.param("after", False)
        after = [None if text == "-" else parse_int(text, "after", None)
                 for text in (after_text.split(",") if after_text else ["-"] * len(names))]
        if len(after) != len(names):
            raise ValueError("after has one sequence (or -) per name")
        lowest: dict[bytes, int | None] = {}  # a name listed twice: above the lower of its sequences
        for name, sequence in zip(names, after):
            if name in lowest and (lowest[name] is None or sequence is None):
                lowest[name] = None
            else:
                lowest[name] = sequence if name not in lowest else min(lowest[name], sequence)
        wait = min(parse_int(self.param("wait", False), "wait", 0), MAX_WATCH_WAIT)
        self.send_json(200, {"records": self.watches.wait(list(lowest), list(lowest.values()), wait)})

    # POST endpoints

    def api_add(self):
        """POST /api/add: import a file or directory on this machine, or (nomadnet) a snapshot of what a NomadNet node
        serves (parameters: the module docstring's table)."""
        source = self.pin_source()
        body = self.json_body()
        if body.get("nomadnet") is not None:
            if body.get("path") is not None:
                raise ValueError("give either path or nomadnet, not both")
            self.send_json(200, self.node.add_nomadnet(parse_nomadnet(body.get("nomadnet")),
                                                       wrap=parse_bool(body.get("wrap")),
                                                       only_hash=parse_bool(body.get("only_hash")),
                                                       name=parse_entry_name(body.get("name")),
                                                       identify=parse_bool(body.get("identify")),
                                                       **node_content.add_options(body.get), **source))
            return
        self.send_json(200, self.node.add_path(parse_path(body.get("path")), wrap=parse_bool(body.get("wrap")),
                                               only_hash=parse_bool(body.get("only_hash")),
                                               **node_content.add_options(body.get), **source))  # pin, to, chunker…

    def api_add_stream(self):
        """POST /api/add/stream: import the request body, wrapped as name if given (parameters: the module
        docstring's table)."""
        source = self.pin_source()  # refused before the body is read
        reader = self.stream_reader()
        size = None if reader.chunked else reader.remaining
        name = self.param("name", False) or None
        only_hash = parse_bool(self.param("only_hash", False) or "")
        options = node_content.add_options(lambda key: self.param(key, False), stream=True)  # pin, to, seal_name…
        options.update(source)
        stream_format = self.param("format", False)
        if stream_format is None:
            self.send_json(200, self.node.add_stream(reader, size=size, name=name, only_hash=only_hash, **options))
            return
        if stream_format != "tar":
            raise ValueError("format must be tar")
        if {"to", "to_self", "seal_name"} & options.keys():
            raise ValueError("a tar stream can't be sealed: seal a folder by its path (POST /api/add with to)")
        # add_tar reads the body to its end, checking that only zeros follow the tar's (its padding)
        self.send_json(200, self.node.add_tar(reader, name=name, only_hash=only_hash, **options))

    def api_get(self):
        """POST /api/get: fetch and write out to a path on this machine (parameters: the module docstring's table)."""
        body = self.json_body()
        from_node = parse_hex_id(body["from"]) if body.get("from") else None
        options = {"raw": True} if parse_bool(body.get("raw")) else {}  # sealed files: the ciphertext
        if parse_bool(body.get("no_extract")):
            options["no_extract"] = True  # a sealed folder written as its tar
        self.send_json(200, self.node.get(parse_target(body.get("target")), parse_path(body.get("output")),
                                          from_node=from_node, **options))

    def api_dir_set(self):
        """POST /api/dir/set {"target", "name", "cid", "executable"}: a new directory with an entry set, pinned
        with the caller's source."""
        source = self.pin_source()
        body = self.json_body()
        name = body.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError("an entry name is required")
        self.send_json(200, self.node.dir_set(parse_target(body.get("target")), name, parse_cid(body.get("cid")),
                                              executable=parse_bool(body.get("executable")), **source))

    def api_dir_rm(self):
        """POST /api/dir/rm {"target"}: a new directory without the target's entry (the node checks it names one),
        pinned with the caller's source."""
        source = self.pin_source()
        self.send_json(200, self.node.dir_rm(parse_target(self.json_body().get("target")), **source))

    def api_pin_add(self):
        """POST /api/pin/add {"target"}: fetch and pin with the caller's source (user, or an app's); answers once
        complete."""
        source = self.pin_source()
        self.send_json(200, self.node.pin_add(parse_target(self.json_body().get("target")), **source))

    def api_pin_rm(self):
        """POST /api/pin/rm {"cid"}: remove the caller's source (user, or an app's own)."""
        source = self.pin_source()
        self.node.pin_rm(parse_cid(self.json_body().get("cid")), **source)
        self.send_json(200, {})

    def api_pin_label(self):
        """POST /api/pin/label {"cid", "label"}: label a pin (null or "" clears it); its pin ls entry."""
        body = self.json_body()
        self.send_json(200, self.node.pin_label(parse_cid(body.get("cid")), body.get("label")))

    def api_cache_ls(self):
        """GET /api/cache/ls: the cached fetches (downloads not pinned), most recently used first."""
        self.send_json(200, {"cache": self.node.cache_ls()})

    def api_cache_rm(self):
        """POST /api/cache/rm {"cid"}: evict the cached fetches leading to cid now."""
        self.send_json(200, self.node.cache_rm(parse_cid(self.json_body().get("cid"))))

    def api_pin_remote(self):
        """POST /api/pin/remote {"node", "cid", "pin"}: ask a node to pin (or release) a CID."""
        body = self.json_body()
        if not isinstance(body.get("pin"), bool):
            raise ValueError("pin must be true or false")
        accepted = self.node.pin_remote(parse_hex_id(body.get("node")), parse_cid(body.get("cid")), body["pin"])
        self.send_json(200, {"accepted": bool(accepted)})

    def api_pin_remote_ls(self):
        """GET /api/pin/remote[?node=&cid=&check=1]: the remote pins this node asked for, and whether each is done;
        check=1 checks them first (lookups, which change no pin)."""
        node, cid = self.param("node", False), self.param("cid", False)
        pins = self.node.pin_remote_ls(parse_hex_id(node) if node else None, parse_cid(cid) if cid else None,
                                       check=parse_bool(self.param("check", False) or ""))
        self.send_json(200, {"pins": pins})

    def api_pin_remote_served(self):
        """GET /api/pin/remote/served: the remote pins other identities asked of this node, and their quota use."""
        self.send_json(200, self.node.pin_remote_served())

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
        """POST /api/import {"path"}: import an archive from a path on this machine, its roots pinned with the
        caller's source."""
        source = self.pin_source()
        self.send_json(200, self.node.import_archive(parse_path(self.json_body().get("path")), **source))

    def api_import_stream(self):
        """POST /api/import/stream: import the archive the request body holds, read to its end, its roots pinned
        with the caller's source."""
        source = self.pin_source()  # refused before the body is read
        self.send_json(200, self.node.import_stream(self.stream_reader(), **source))

    # Capabilities, events, shutdown

    def api_capabilities(self):
        """GET /api/capabilities: what this daemon offers (the module docstring)."""
        self.send_json(200, capabilities(self.node))

    def api_events(self):
        """GET /api/events?after=&kinds=&epoch=&wait=: the node's events after a sequence (rnfs.events)."""
        event_log = getattr(self.node, "events", None)
        if event_log is None:
            raise Unsupported("this node keeps no event log")
        after = parse_int(self.param("after", False), "after", None)
        kinds_text = self.param("kinds", False)
        kinds = [kind for kind in kinds_text.split(",") if kind] if kinds_text else None
        if kinds_text and not kinds:
            raise ValueError("kinds lists no event kind")
        for kind in kinds or ():
            if kind not in events.EVENT_KINDS:
                raise ValueError(f"unknown event kind {kind!r}; known: {', '.join(events.EVENT_KINDS)}")
        epoch = self.param("epoch", False) or None
        wait = min(parse_int(self.param("wait", False), "wait", 0), MAX_WATCH_WAIT) if after is not None else 0
        if wait > 0:
            if not self.event_waiters.acquire(blocking=False):
                raise Busy(f"{MAX_EVENT_WAITERS} event polls are waiting already")
            try:
                result = event_log.since(after, kinds, wait, epoch)
            finally:
                self.event_waiters.release()
        else:
            result = event_log.since(after, kinds, 0, epoch)
        self.send_json(200, result)

    # Apps (rnfs app; the CLI's token only: CLI_ROUTES)

    def api_apps(self):
        """GET /api/apps: every app, by name, without its token."""
        self.send_json(200, {"apps": self.node.app_ls()})

    def api_app_add(self):
        """POST /api/apps/add {"name"}: a new app and its token, given only this once."""
        self.send_json(200, self.node.app_add(check_app_name(self.json_body().get("name"))))

    def api_app_rm(self):
        """POST /api/apps/rm {"name"}: revoke an app's token and release its pins; {"released"}."""
        self.send_json(200, self.node.app_rm(check_app_name(self.json_body().get("name"))))

    def api_shutdown(self):
        """POST /api/shutdown: answer, then stop the daemon as SIGTERM does (ApiServer's shutdown)."""
        self.json_body()
        if self.shutdown_callback is None:
            raise Unsupported("this daemon can't be stopped through its API")
        log("stopping: asked through the API", LOG_NOTICE)
        self.send_json(200, {"stopping": True})
        self.wfile.flush()
        self.shutdown_callback()
