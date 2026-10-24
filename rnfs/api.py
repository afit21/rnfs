"""Local JSON API of a running node (specification section 11), bound to the loopback only (127.0.0.1 and ::1).

Every command of the CLI is one request. Targets are `<cid>[/name…]` strings; node IDs and names are
32 hex characters; paths are absolute paths on the daemon's machine. Successful responses are JSON
(except cat, name export, get/stream and export/stream, which send bytes); errors are `{"error": message, "code":
code}` (below).

| Method | Path | Parameters | Response / facade call |
|---|---|---|---|
| GET  | /api/status | | `status()`; without the token only `{"version"}` |
| GET  | /api/info | `target`, `from` | `info(target)` (fetched from node `from` if given) |
| GET  | /api/cat | `target`, `offset`, `length`, `raw`, `block`, `from` | the bytes of a file (`read`; sealed files decrypted unless `raw`; fetched from node `from` if given), Content-Length set; `block=1`: the CID's own block |
| GET  | /api/ls | `target` | `{"entries": ls(target)}` |
| GET  | /api/stat | `target` | `stat(target)` |
| GET  | /api/pin/ls | `cid` (optional) | `{"pins": pin_ls(cid)}`: `cid`, `sources`, `complete`, `type`, `sealed`, `name`, and since 0.2.0 `label`, `origin`, `size`, `entries`, `added` |
| GET  | /api/providers | `cid`, `stream` | `{"providers": [node ID]}`; `stream=1` (0.2.1): JSON lines, `{"provider"}` as each is found, then that answer with `"done": true` (below) |
| GET  | /api/ping | `node`, `stream` | `ping_info(node)`: `{"rtt": seconds or null, "reason", "hops", "interface", "path_since"}` (all but `rtt` since 0.2.1); `stream=1`: JSON lines, `{"stage"}` as each step begins (path, identity, link), then that answer with `"done": true` |
| GET  | /api/routing | | `routing_info()` (0.2.1): `{"k", "buckets": [{"index", "nodes": [{"node_id", "first_seen", "last_heard", "failures", "stale"}]}], "nodes", "dropped", "unreliable": [{"node_id", "failures", "retry_at"}], "unreliable_total", "bans": [{"node_id", "until", "reason"}], "bans_total", "blackholed"}` (RoutingTable.snapshot) |
| GET  | /api/log | `after` (a sequence), `limit` (default 200, ≤ 1000), `level` (a name or 0-8: that or more severe) | `{"lines": [{"seq", "time", "level", "level_name", "source", "text"}], "next", "dropped"}` (0.2.1): the daemon's recent log lines, rnfs's and Reticulum's, as its console shows them (rnfs.recentlog: the last 2000, in memory), the last `limit` after `after` |
| GET  | /api/name/list | | `{"names": name_list()}` |
| GET  | /api/name/resolve | `name` | `name_resolve(name)` |
| GET  | /api/name/watch | `names` (comma-separated), `after` (a sequence or `-` per name), `wait` (≤ 30 s) | `{"records": [{"name", "cid", "sequence", "age"}]}`: the records held whose sequence is above `after`, as soon as there is one or after `wait` (below) |
| GET  | /api/name/syncs | | `{"syncs": name_syncs()}`: `[{"alias", "path", "state", "reason", "root", "published", "published_at", "next_publish"}]` (rnfs.name_sync), by alias |
| POST | /api/add | JSON `path`, `wrap`, `only_hash`, `pin`, `to` (list), `to_self`, `seal_name` (bool), `chunker`; or `nomadnet` (a NomadNet link) instead of `path`, with `name` and `identify` too | `add_path(...)`; with `nomadnet`, `add_nomadnet(...)`: its result has the snapshot's `name` and the `link` too |
| POST | /api/add/stream | query `name`, `only_hash`, `pin`, `to` (comma-separated), `to_self`, `seal_name` (the name), `chunker`, `format`, `origin`; body is the data | `add_stream(...)`; `format=tar`: the body is a tar stream, `add_tar(stream, name, ...)` builds the directory it holds, or with `to`/`to_self` (0.2.1) seals it as a folder named `name` (required; no `seal_name`); `origin` (0.2.1): what `pin ls` says it was added from (default `name`, else `(stdin)` or `(tar upload)`) |
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
| GET  | /api/pin/remote/served | | `pin_remote_served()`: `{"pins": [{"identity", "cid", "state", "reason", "at", "detail", "retry_at"}], "usage": [{"identity", "used", "quota", "pinned", "waiting", "allowed"}], "quota"}` |
| POST | /api/pin/label | JSON `cid`, `label` (null or "" clears it) | `pin_label(cid, label)`: the pin's `pin_ls` entry |
| GET  | /api/cache/ls | | `{"cache": cache_ls()}`: the cached fetches, most recently used first |
| POST | /api/cache/rm | JSON `cid` | `cache_rm(cid)`: `{"removed_records", "removed_blocks", "freed_bytes", "unreferenced_blocks", "unreferenced_bytes"}` |
| POST | /api/gc, /api/verify | gc: JSON `dry_run` (0.2.1) | `gc()`, `verify()`; `dry_run`: `gc(dry_run=True)`, nothing changed: what it would free (`removed_blocks`, `freed_bytes`, `allocated_bytes`, `held_back`, `cached_fetches`) and the stored bytes split by what keeps them (`pinned_bytes`, `cached_bytes`, `protected_bytes`, `unreferenced_bytes`; `records_bytes`), with `"dry_run": true` |
| POST | /api/name/create | JSON `alias` | `name_create(alias)` |
| POST | /api/name/rm | JSON `alias` | `{}` after `name_rm(alias)` |
| POST | /api/name/export | JSON `alias` | the key bytes |
| POST | /api/name/import | query `alias`; body is the key bytes | `name_import(alias, key)` |
| POST | /api/name/publish | JSON `alias`, `target` | `name_publish(alias, target)` |
| POST | /api/name/sync | JSON `alias`, `path` (a folder) | `name_sync(alias, path)`: the sync's entry (as `/api/name/syncs` lists it), `warning` in private mode |
| POST | /api/name/unsync | JSON `alias` | `{}` after `name_unsync(alias)` (404 if the name isn't synced) |
| POST | /api/export | JSON `target`, `output` | `export(target, output)` |
| POST | /api/import | JSON `path` | `import_archive(path)` |
| GET  | /api/capabilities | | `{"version", "capabilities": [...], "routes": [[method, path]...], "features": [...], "event_kinds": [...], "error_codes": [...]}` (below) |
| GET  | /api/events | `after` (a sequence; absent: none, only `next`), `kinds` (comma-separated), `epoch`, `wait` (≤ 30 s) | `{"events": [event], "next", "lost", "epoch"}` (rnfs.events): the events after `after`, as soon as there is one or after `wait` |
| POST | /api/shutdown | | `{"stopping": true}`, then the daemon stops as on SIGTERM (`unsupported` 404 where nothing can stop it) |
| POST | /api/restart | | `{"restarting": true}`, then the daemon stops as on SIGTERM and starts again with the same command line (0.2.1; `unsupported` 404 where it can't: daemon.json's `can_restart`, rnfs.launch) |
| GET  | /api/apps | | `{"apps": app_ls()}`: `[{"name", "created"}]` (the CLI's token only, below) |
| POST | /api/apps/add | JSON `name` | `app_add(name)`: `{"name", "created", "token"}`, the only time the token is given |
| POST | /api/apps/rm | JSON `name` | `app_rm(name)`: `{"released"}`, the app's token revoked and its pins released |
| POST | /api/apps/request | header `X-Rnfs-Pairing: <name>`, JSON `description`; no token | `app_request(name, description)` (0.2.1): `{"id", "secret", "code", "expires"}`, the app's request for a token, for the user to approve (below) |
| GET  | /api/apps/request | `id`, header `X-Rnfs-Pairing-Secret: <secret>`; no token | `{"name", "state": "pending" \\| "approved" \\| "denied", "token" (approved: given once), "expires"}` (404 once collected, expired, or for a wrong secret) |
| GET  | /api/apps/requests | | `{"requests": [{"id", "name", "description", "code", "requested", "expires"}]}`, the apps waiting for an answer (the CLI's token only) |
| POST | /api/apps/approve, /api/apps/deny | JSON `id`; approve: `code` (optional, the one the app shows) | approve: `{"name", "created"}`, the app added, its token kept for its requester, the others asking under its name denied; deny: `{"name"}` (the CLI's token only) |

The features (rnfs.features, each's `api_routes()`) add these; `name` is a name or a name contact's nick:

| Method | Path | Parameters | Response |
|---|---|---|---|
| GET  | /api/jobs | | `{"jobs": [job]}`, oldest first |
| POST | /api/jobs | JSON `kind` (get, pin, export: `target`, `output`, `retry` (default false: true waits for content not found yet), and for get `from`, `sealed_name`, `fallback_dht`, `no_extract`; add: `path` or `nomadnet` and `/api/add`'s options; import: `path`) | the job queued |
| GET  | /api/jobs/get | `id`, `wait` (≤ 30 s) | the job, once it ends or after `wait` |
| POST | /api/jobs/cancel | JSON `id` | the job |
| POST | /api/jobs/clear | JSON `id` (optional) | `{"removed": count}` of the finished jobs forgotten |
| GET  | /api/peers | | `{"peers": [...]}` |
| GET  | /api/stats | | traffic totals, refusals and per-interface bytes; from 0.2.1 `reticulum_interfaces`, Reticulum's interfaces read-only (mesh.interface_states) |
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
| GET  | /api/deny | | `{"entries": [local entry], "lists": [{"source", "cid", "entries", "state"}]}` |
| POST | /api/deny/add | JSON `entries` | `{"added": [entries new to the local list]}` |
| POST | /api/deny/rm | JSON `entries` | `{"removed": [...]}` (404 if one isn't on the local list) |
| GET  | /api/config | | `{"path", "settings": [schema entry + "value", "file", "overridden", "pending"], "file_error", "warnings"}` (rnfs.features.settings) |
| POST | /api/config/set | JSON `settings` ({name: value, or null to remove it from rnfs.toml}), `dry_run` (0.2.1) | `{"path", "applied", "restart_required", "overridden", "warnings"}`; nothing written unless the whole result is valid (400); `dry_run`: only checked, nothing written or applied, the answer what it would be with `"dry_run": true` |

Both tables are checked against ApiHandler.ROUTES and the features' routes (tests/test_api.py). docs/openapi.json
describes every route in full (parameters, bodies, answers, errors, headers), checked by tests/test_openapi.py.

Errors are `{"error": message, "code": code}`: code is machine-readable (rnfs.errors.CODES, design.md "Error
codes"), so a client tells "no such endpoint" (`no_such_endpoint`, an older daemon) from "no such content"
(`not_found`), both 404, without reading the message. `GET /api/capabilities` lists what this daemon offers
(CAPABILITIES, every route and feature), so a client checks once instead of trying each endpoint.

Events (rnfs.events, `GET /api/events`): a long poll like the name watch below, at most MAX_EVENT_WAITERS waiting
at once (503 beyond); `after` absent answers at once with the `next` to start from.

Streamed answers (`stream=1`, send_lines; 0.2.1): `application/x-ndjson`, one JSON object per line as the work goes,
the last with `"done": true` (the plain answer's fields, or `{"error", "code", "status"}` for an error after the first
line; one before it is answered as without `stream=`), then the connection closes. A client closing it ends the work
early where it can (a provider lookup). An older daemon ignores `stream=` and answers in plain JSON.

GET parameters are in the query string. Request bodies may use Content-Length or chunked encoding.

Watching names (rnfs.Client's names.watch): `GET /api/name/watch` is a long poll, level-triggered so a client loses
nothing between calls. Each call marks its names (at most MAX_WATCH_NAMES) watched until WATCH_GRACE seconds after
it ends (NameManager.watch: their announces count as asked), waits on NameManager's record listener, and answers
from the records held; it sends nothing to the network. At most MAX_WATCH_WAITERS calls wait at once (503 beyond;
a `wait=0` call never waits, so it is always answered), so watchers can't take the API's connections, and at most
MAX_WATCHED names stay watched.

Security:
- The API listens on 127.0.0.1:<port> and [::1]:<port> (LocalServer; ::1 only where the host has IPv6), so no other
  local user can take `localhost` on ::1, which browsers and curl try first, while the daemon runs (AUD-H2).
  Clients dial 127.0.0.1 itself (rnfs.apiclient does).
- The Host header must be `127.0.0.1:<port>` or `localhost:<port>`, which blocks DNS rebinding; `[::1]:<port>` is
  refused like any other host.
- Every request except `GET /api/status` and pairing's two routes (`POST` and `GET /api/apps/request`, below) must
  carry `X-Rnfs-Api: <token>`, the random token the daemon writes to `<rnfs dir>/api_token` (mode 0600) at each
  start. Every local process, of every user, can
  connect to 127.0.0.1, and the API reads and writes files as the daemon's user, so only those who can
  read the rnfs directory may use it. A custom header also forces a CORS preflight, which fails because
  the API never sends CORS headers, and a web page can't know the token anyway. Status stays open for
  health checks and the CLI's proof check below, but without the token it answers only `{"version": ...}`:
  the node and identity hashes and traffic counters need the token like everything else.
- A request carrying `X-Rnfs-Challenge: <32 hex digits>` gets `X-Rnfs-Proof: <rnfs.apiclient.api_proof>` in its
  response, which only a server knowing the token can make. Before it first sends the token, a client
  (rnfs.apiclient, which the CLI uses) makes whatever listens on the port prove this (with a status request),
  so another local user listening there while the daemon is down learns neither the token nor a name key being
  imported.
- App tokens (rnfs.apps; 0.2.0): a request naming an app in `X-Rnfs-App: <name>` passes with that app's token in
  `X-Rnfs-Api` instead, and only with it (an app's token without its name, or the API token with one, doesn't pass);
  its proof is keyed by the app's verifier (apiclient.app_proof_key), and for an app there isn't by an HMAC of a
  per-start secret and the name (AppRegistry.proof_key), which no client can check: every response, the 403s included,
  looks the same whether or not the app exists, so the open proof doesn't list the installed apps (REV9).
  ApiHandler.caller() says who called: CLI ("cli") or "app:<name>", which dispatch keeps in principal. An app's token
  does everything the API token does but use the /api/apps routes (CLI_ROUTES), which answer it 403 `forbidden`. That
  doesn't keep an app from managing apps in effect: it reads and writes files as the daemon's user (add, get, export),
  which reaches api_token (specification section 11). The routes that pin or release for the caller (add, add/stream,
  dir/set, dir/rm, pin/add, pin/rm, import, import/stream, and the pin, add and import jobs) use source "user" for the
  CLI and "app:<name>" for an app, "app:<name>:<key>" with `X-Rnfs-Pin-Key: <key>` (pin_source; with the CLI's token a
  pin key is refused, 400): an app's pin rm releases only its own.
- Pairing (0.2.1; rnfs.apps.PairingRequests): an app without a token asks for one (POST /api/apps/request, naming
  itself in X-Rnfs-Pairing, a custom header, so no web page can ask: its request would need a CORS preflight, which
  fails) and gets a request id, a secret and a 6-digit code it shows its user. The user sees the request, with the
  same code, where the CLI's token is (rnfs app requests; an app such as RNFS Desktop, told by an "apps" event) and
  approves or denies it: the code tells their app's request from one another process made under its name. Any
  process may ask under any name, as often as it likes (AUD-M5): a name can have several requests waiting, each with
  its own code, and approve takes the code the app shows (`code`, which the CLI asks the user for) and refuses
  another (400), so asking first under an app's name gets a squatter nothing. Approving adds the app and denies the
  others asking under its name; a name an app already has is refused only then (400, a name clash), never when
  asked, so the open route doesn't say which apps exist (AUD-L1). The app collects its token with the secret (GET
  /api/apps/request), once, and the approver never sees it. Requests live in memory, at most 32 waiting (a new one
  beyond takes the oldest's place), for 10 minutes (an approved token for 5 more). A process
  squatting the API port while the daemon is down can answer a pairing app as any listener can: an app that can
  read the rnfs dir should prefer rnfs app add, and every app should show the code for the user to compare.
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
import errno
import hmac
import io
import json
import os
import queue
import re
import secrets
import socket
import socketserver
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from rnfs import __version__, events, fsutil, ids, nomadnet_links
from rnfs import node_content  # --- content: add options (pin, to, chunker) ---
from rnfs.apiclient import (API_HEADER, APP_HEADER, CHALLENGE_HEADER, PAIRING_HEADER, PAIRING_SECRET_HEADER,
                            PIN_KEY_HEADER, PROOF_HEADER, api_proof)
from rnfs.content.cid import Cid
from rnfs.content.storage import APP_PREFIX, USER, app_source, check_app_name
from rnfs.errors import CODES, STATUSES, BlockMismatch, Busy, Unsupported, error_code
from rnfs.log import LOG_ERROR, LOG_NOTICE, LOG_WARNING, log

MAX_JSON_BODY = 1024 * 1024  # JSON parameters are tiny; this only stops a runaway body filling memory
MAX_KEY_BODY = 64 * 1024  # an RNS identity file is 64 bytes
CHALLENGE = re.compile(r"[0-9a-f]{32}")  # what the CLI sends in X-Rnfs-Challenge
CLI = "cli"  # ApiHandler.caller() for the CLI's token (api_token); an app's is "app:<name>"
CHUNK_SIZE_LINE = re.compile(rb"[0-9A-Fa-f]{1,16}")  # a chunk size: hex digits only (no 0x, +, _ or spaces)
DECIMAL = re.compile(r"[0-9]{1,18}")  # an integer parameter or Content-Length: ASCII digits, below 2**63
MAX_TRAILER_LINES = 32
MAX_WATCH_NAMES = 64  # names one GET /api/name/watch may list
MAX_WATCH_WAITERS = 4  # GET /api/name/watch calls waiting at once
MAX_WATCH_WAIT = 30  # seconds one may wait, as GET /api/jobs/get's wait=
WATCH_GRACE = 60.0  # seconds a name stays watched after the last call naming it ends
MAX_WATCHED = 1024  # names watched for API clients at most; the soonest to end go first
MAX_EVENT_WAITERS = 8  # GET /api/events calls waiting at once
LINE_POLL = 0.5  # seconds between checks that the client of a streamed answer (send_lines) is still there
# What binding ::1 fails with where the host has no IPv6 (none in the kernel, or a loopback without ::1): LocalServer
# then listens on 127.0.0.1 alone. On Windows Python gives socket errors their WSA numbers, under these names
NO_IPV6 = frozenset(getattr(errno, name) for name in ("EADDRNOTAVAIL", "EAFNOSUPPORT", "EPROTONOSUPPORT",
                                                      "WSAEADDRNOTAVAIL", "WSAEAFNOSUPPORT", "WSAEPROTONOSUPPORT")
                    if hasattr(errno, name))
# A port another socket holds (Windows answers WSAEACCES where that socket binds it exclusively)
PORT_TAKEN = frozenset(getattr(errno, name) for name in ("EADDRINUSE", "WSAEADDRINUSE", "WSAEACCES")
                       if hasattr(errno, name))
BIND_ATTEMPTS = 16  # ports LocalServer tries for port 0, while ::1 has the one 127.0.0.1 gave
DEFAULT_LOG_LINES = 200  # lines GET /api/log answers without limit=
# What this daemon offers beyond the routes themselves (GET /api/capabilities), each a promise kept once released
CAPABILITIES = ("error_codes", "capabilities", "events", "shutdown", "daemon_json", "pin_details", "pin_label",
                "cache", "peer_details", "config", "jobs_add", "jobs_import", "add_tar", "get_tar", "export_stream",
                "import_stream", "sealed_folders", "apps", "remote_pin_status", "add_nomadnet", "name_sync",
                # 0.2.1: POST /api/gc's dry_run (an older daemon ignores it and collects: check this first), a stream
                # add's origin, a sealed tar upload, ping's reason and path, stream=1 on ping and providers
                "gc_dry_run", "config_dry_run", "add_origin", "sealed_tar", "ping_details", "streamed_answers",
                "restart")

# Exception class → HTTP status, checked in order (design.md); anything else is 500. The one table, rnfs.errors'
ERROR_STATUSES = STATUSES


_feature_routes: dict | None = None


def feature_routes() -> dict:
    """The features' API routes (rnfs.features), loaded once."""
    global _feature_routes
    if _feature_routes is None:
        from rnfs import features
        _feature_routes = features.api_routes()
    return _feature_routes


def capabilities(node, without=()) -> dict:
    """GET /api/capabilities' answer for node: {"version", "capabilities" (CAPABILITIES but those in without),
    "routes" (every core and feature route, by path), "features" (those node loaded), "event_kinds",
    "error_codes"}. rnfs.Node's capabilities() too, without what an embedded node can't do."""
    loaded = getattr(node, "features", None) or {}
    routes = sorted({*ApiHandler.ROUTES, *feature_routes()}, key=lambda route: (route[1], route[0]))
    return {"version": __version__, "capabilities": sorted(set(CAPABILITIES) - set(without)),
            "routes": [list(route) for route in routes], "features": sorted(loaded),
            "event_kinds": list(events.EVENT_KINDS), "error_codes": list(CODES)}


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
    # ASCII digits only, at most 18 (below 2**63): int() also takes "1_0", " 7 ", "+7" and other scripts' digits
    if not isinstance(text, str) or not DECIMAL.fullmatch(text):
        if isinstance(text, str) and text.startswith("-") and DECIMAL.fullmatch(text[1:]):
            raise ValueError(f"{name} must not be negative")
        raise ValueError(f"{name} must be an integer")
    return int(text)


def parse_nomadnet(text) -> str:
    """A NomadNet link (<32 hex>:/page/<path> or :/file/<path>, rnfs.nomadnet_links.parse) as written, or ValueError."""
    return str(nomadnet_links.parse(text))


def parse_entry_name(value) -> str | None:
    """An entry name (rnfs.nomadnet_links.is_valid_name), or None if absent or empty; ValueError otherwise."""
    if value is None or value == "":
        return None
    if not nomadnet_links.is_valid_name(value):
        raise ValueError(f"invalid entry name {value!r}")
    return value


class _Counted:
    """An upload's body as its job counts it (JobsFeature.uploading): each read told to job.read, which raises
    Cancelled once the job is cancelled."""

    def __init__(self, reader, job):
        """Read through reader for job."""
        self.reader, self.job = reader, job

    def read(self, size: int = -1) -> bytes:
        """reader.read, counted."""
        self.job.check_cancelled()
        data = self.reader.read(size)
        self.job.read(len(data))
        return data

    def readinto(self, buffer) -> int:
        """reader.readinto, counted."""
        self.job.check_cancelled()
        count = self.reader.readinto(buffer)
        self.job.read(count or 0)
        return count

    def readable(self) -> bool:
        """True."""
        return True

    def __getattr__(self, name):
        """Everything else is the reader's (its chunked, remaining)."""
        return getattr(self.reader, name)


def _counted(reader, job):
    """reader counted for job, or reader itself without one (no jobs feature)."""
    return reader if job is None else _Counted(reader, job)


def parse_origin(value) -> str | None:
    """An add's origin (what pin ls says it was added from: a URL, the app's own path for it): None if absent or
    blank, else the text without surrounding whitespace. ValueError past pin_names.MAX_NAME_LENGTH characters."""
    from rnfs.pin_names import MAX_NAME_LENGTH
    if value is None or not value.strip():
        return None
    if len(value.strip()) > MAX_NAME_LENGTH:
        raise ValueError(f"origin may have at most {MAX_NAME_LENGTH} characters")
    return value.strip()


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
        # Any line ending HTTP/0.9: http.server takes the version from the last word before it counts them, so a
        # malformed "GET / x HTTP/0.9" was answered without a status line or headers (REL-17)
        if len(words) == 2 or (len(words) >= 3 and words[-1] == "HTTP/0.9"):
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

    def version_string(self) -> str:
        """The Server header: server_version (BaseHTTPRequestHandler appends a space and sys_version, which the
        handlers leave empty, so it read "rnfs ")."""
        return f"{self.server_version} {self.sys_version}".strip()

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

    def __init__(self, address, handler_class, max_connections: int, sibling: "BoundedHTTPServer | None" = None):
        """Bind to address (an IPv6 one if its host has a ":"); handler_class.busy_response() is sent to connections
        over max_connections. With a sibling (the same port on the other loopback, LocalServer), its connection and
        closer slots are shared, so both together serve at most max_connections."""
        if sibling is None:
            self.slots = threading.BoundedSemaphore(max_connections)
            self.closers = threading.BoundedSemaphore(self.MAX_CLOSERS)
        else:
            self.slots, self.closers = sibling.slots, sibling.closers
        if ":" in address[0]:
            self.address_family = socket.AF_INET6
        super().__init__(address, handler_class)

    def server_bind(self):
        """Bind the listening socket, exclusively on Windows; an IPv6 one to IPv6 only. The server's name is its
        address as it is: HTTPServer's own server_bind looks the address up (socket.getfqdn), which for ::1 can
        wait on the system's resolver (mDNS on macOS) before the daemon answers anything, and nothing reads it."""
        if fsutil.IS_WINDOWS:
            # There SO_REUSEADDR (HTTPServer's default) lets another socket bind a port already listening, so a
            # second daemon, or another local program listening in on API tokens, could share it
            self.allow_reuse_address = False
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        if self.address_family == socket.AF_INET6:
            # ::1 and nothing else: 127.0.0.1 is the sibling's
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]

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
        except ConnectionError:
            pass  # the client reset or closed the connection: nothing to answer, and no fault of the server's
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
    """BoundedHTTPServers on 127.0.0.1 and [::1], on the same port, each run on a daemon thread. Port 0 picks a port
    free on both.

    Browsers and curl try a *.localhost name (the gateway's sites) or localhost on ::1 first, so another local user
    listening on [::1]:<port> while the daemon runs would get them (AUD-H2): the daemon holds both, with the same
    handler and connection slots. A port taken on ::1 stops it as one taken on 127.0.0.1 does; only a host without
    IPv6 (NO_IPV6) runs on 127.0.0.1 alone, where nobody can listen on ::1 either. self.server is 127.0.0.1's.

    The connection limits are class attributes (see the module docstring), read when the server is made.
    """

    poll_interval = 0.5  # how often serve_forever checks for stop(); tests lower it to stop quickly
    request_timeout = 15.0  # seconds for the request line, headers and a JSON body to arrive
    body_timeout = 60.0  # seconds a streamed upload may pause between reads
    response_timeout: float | None = None  # seconds a response write may block; None: forever
    max_connections = 32

    def __init__(self, handler_class, port: int, **attributes):
        """Bind to 127.0.0.1:port and [::1]:port with handler_class; attributes are set on a subclass of it.
        Raises OSError if either is taken."""
        attributes.update(request_timeout=self.request_timeout, body_timeout=self.body_timeout,
                          response_timeout=self.response_timeout)
        handler = type(handler_class.__name__, (handler_class,), attributes)
        self.servers = self._bind(handler, port)
        self.server = self.servers[0]
        self.threads = [threading.Thread(target=server.serve_forever, args=(self.poll_interval,), daemon=True)
                        for server in self.servers]

    def _bind(self, handler, port: int) -> list[BoundedHTTPServer]:
        """The servers on 127.0.0.1:port and [::1]:port (127.0.0.1's alone on a host without IPv6). For port 0,
        another port is tried while ::1 has the one 127.0.0.1 got. Raises OSError."""
        for _ in range(BIND_ATTEMPTS if port == 0 else 1):
            ipv4 = BoundedHTTPServer(("127.0.0.1", port), handler, self.max_connections)
            bound = ipv4.server_address[1]
            try:
                return [ipv4, BoundedHTTPServer(("::1", bound), handler, self.max_connections, sibling=ipv4)]
            except OSError as e:
                if e.errno in NO_IPV6 or not socket.has_ipv6:
                    log(f"no IPv6 here for [::1]:{bound} ({e}): listening on 127.0.0.1:{bound} only", LOG_NOTICE)
                    return [ipv4]
                ipv4.server_close()
                if port == 0 and e.errno in PORT_TAKEN:
                    continue
                raise OSError(e.errno, f"[::1]:{bound}: {e.strerror or e}") from e
        raise OSError(errno.EADDRINUSE, f"no port free on both 127.0.0.1 and ::1 in {BIND_ATTEMPTS} tries")

    @property
    def port(self) -> int:
        """The port actually bound, useful when 0 was requested."""
        return self.server.server_address[1]

    def start(self):
        """Start serving requests in the background."""
        for thread in self.threads:
            thread.start()

    def stop(self):
        """Stop serving and close the listening sockets. Requests still running finish on their threads."""
        for server, thread in zip(self.servers, self.threads):
            if thread.is_alive():
                server.shutdown()
            server.server_close()


class NameWatches:
    """GET /api/name/watch's state, one per ApiServer: the names watched for API clients, each until WATCH_GRACE
    after the last call naming it ends, and the calls waiting. Each name in until holds exactly one
    NameManager.watch, so its lock is held while watching and unwatching there (taken before the names lock,
    never under it: the record listener, called with the names lock held, only sets the waiting calls' events).
    A watch ends on time without a later call: one daemon timer (rnfs-api-name-watch) is armed for the soonest end,
    and re-armed when it fires while names are still watched; close() cancels it and ends every watch."""

    def __init__(self, node, clock=time.monotonic):
        """Watches on node's names (read only once a call comes)."""
        self.node = node
        self.clock = clock
        self.lock = threading.Lock()
        self.until: dict[bytes, float] = {}  # name -> when its watch ends (each one NameManager.watch)
        self.waiters: tuple = ()  # (names, event) of each call waiting; swapped whole, read without the lock
        self.listening = False
        self.timer: threading.Timer | None = None  # ends the watches due at timer_due (_arm)
        self.timer_due = 0.0
        self.closed = False

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
                self._arm()

    def _arm(self):
        """Have the timer fire when the soonest watch ends (none while nothing is watched). Lock held."""
        if self.closed or not self.until:
            return
        due = min(self.until.values())
        if self.timer is not None:
            if self.timer_due <= due:
                return  # it fires first, and re-arms for what is left
            self.timer.cancel()
        # At least a moment: a watch found not quite ended when the timer fires doesn't make it spin
        self.timer = threading.Timer(max(due - self.clock(), 0.01), self._expire)
        self.timer.name = "rnfs-api-name-watch"
        self.timer.daemon = True
        self.timer_due = due
        self.timer.start()

    def _expire(self):
        """The timer: end the watches whose grace has passed, then arm it for the next."""
        with self.lock:
            if self.timer is not None and self.timer is not threading.current_thread():
                return  # replaced by a timer due sooner; that one arms what follows
            self.timer = None
            if self.closed:
                return
            self._extend([], self.clock())
            self._arm()

    def close(self):
        """Cancel the timer and end every watch (ApiServer.stop): nothing is watched for a server that stopped."""
        with self.lock:
            self.closed = True
            if self.timer is not None:
                self.timer.cancel()
                self.timer = None
            for name in self.until:
                self.node.names.unwatch(name)
            self.until.clear()

    def _extend(self, names: list[bytes], until: float):
        """Keep names (distinct) watched until at least until, unwatching those whose watch has ended, and the
        soonest to end past MAX_WATCHED, in NameManager too; nothing once closed (a call still answered then
        watches nothing, which no timer would end). Lock held."""
        if self.closed:
            return
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

    Every request but status and pairing's (ApiHandler.OPEN_ROUTES) must carry token (self.token); the daemon writes it
    to <rnfs dir>/api_token.
    """

    max_connections = 32  # the CLI makes one connection per command; this is plenty for scripts too

    def __init__(self, node, port: int, token: str | None = None, shutdown=None, restart=None):
        """Bind to 127.0.0.1:port and [::1]:port (LocalServer), requiring token (a new random one if None); serves
        once start() is called.
        shutdown, if given, is called (with no arguments) once POST /api/shutdown has been answered: the daemon
        passes what its signal handlers do. Without it that route answers 404 unsupported. restart (0.2.1) is
        POST /api/restart's, likewise: the daemon passes it where it can restart itself (rnfs.launch)."""
        self.token = token if token is not None else secrets.token_urlsafe(32)
        if not self.token:
            raise ValueError("the API token must not be empty")
        self.watches = NameWatches(node)
        super().__init__(ApiHandler, port, node=node, token=self.token, watches=self.watches,
                         event_waiters=threading.BoundedSemaphore(MAX_EVENT_WAITERS),
                         shutdown_callback=None if shutdown is None else staticmethod(shutdown),
                         restart_callback=None if restart is None else staticmethod(restart))

    def stop(self):
        """Stop serving, then end the name watches held for API clients (and their timer)."""
        super().stop()
        self.watches.close()


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
    restart_callback = None  # what POST /api/restart calls once answered (ApiServer's restart)
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
        ("GET", "/api/routing"): "api_routing",
        ("GET", "/api/log"): "api_log",
        ("GET", "/api/name/list"): "api_name_list",
        ("GET", "/api/name/resolve"): "api_name_resolve",
        ("GET", "/api/name/watch"): "api_name_watch",
        ("GET", "/api/name/syncs"): "api_name_syncs",
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
        ("POST", "/api/name/sync"): "api_name_sync",
        ("POST", "/api/name/unsync"): "api_name_unsync",
        ("POST", "/api/export"): "api_export",
        ("POST", "/api/import"): "api_import",
        ("POST", "/api/pin/label"): "api_pin_label",
        ("GET", "/api/cache/ls"): "api_cache_ls",
        ("POST", "/api/cache/rm"): "api_cache_rm",
        ("GET", "/api/capabilities"): "api_capabilities",
        ("GET", "/api/events"): "api_events",
        ("POST", "/api/shutdown"): "api_shutdown",
        ("POST", "/api/restart"): "api_restart",
        ("GET", "/api/apps"): "api_apps",
        ("POST", "/api/apps/add"): "api_app_add",
        ("POST", "/api/apps/rm"): "api_app_rm",
        ("POST", "/api/apps/request"): "api_app_request",
        ("GET", "/api/apps/request"): "api_app_request_state",
        ("GET", "/api/apps/requests"): "api_app_requests",
        ("POST", "/api/apps/approve"): "api_app_approve",
        ("POST", "/api/apps/deny"): "api_app_deny",
    }
    # Allowed without the token: status, and an app asking for one and collecting it (pairing, 0.2.1)
    OPEN_ROUTES = {("GET", "/api/status"), ("POST", "/api/apps/request"), ("GET", "/api/apps/request")}
    # Only the CLI's token (api_token) may use these: an app can't issue itself or another app a token, nor revoke one
    CLI_ROUTES = {("GET", "/api/apps"), ("POST", "/api/apps/add"), ("POST", "/api/apps/rm"),
                  ("GET", "/api/apps/requests"), ("POST", "/api/apps/approve"), ("POST", "/api/apps/deny")}
    PROOF_ROUTE = ("GET", "/api/status")
    proved = False  # this connection had its proof request kept open: it is closed after the next request
    principal: str | None = None  # caller() of the request being answered, set by dispatch
    keep_open = False  # the request being answered is a proof request whose connection stays open

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
        """What keys this response's proof: the API token, or (X-Rnfs-App) what AppRegistry.proof_key gives the name,
        an existing app's or not; None on a node without apps (a stand-in, which has none to hide)."""
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
        if self.command != "HEAD":
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
        if self.command != "HEAD":  # an answer to HEAD has the headers only (the 405 every HEAD gets)
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
        length = length.strip(" \t")
        if not DECIMAL.fullmatch(length):
            # Negative, or 2**63 and more (which overflowed the body reader's reads): 400, as a negative one was; not
            # ASCII digits at all (int() also takes "1_0" and other scripts' digits): 411, as before
            numeric = length.lstrip("-").isascii() and length.lstrip("-").isdigit()
            raise (ApiRequestError(400, "invalid Content-Length", "bad_request") if numeric
                   else ApiRequestError(411, "invalid Content-Length", "length_required"))
        size = int(length)
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
        except (ValueError, RecursionError):  # nested deeper than the parser goes: not a request rnfs takes (REL-17)
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
        """GET /api/providers?cid=&stream=: node IDs providing a CID; stream=1: each as found (send_lines)."""
        cid = parse_cid(self.param("cid"))
        if not parse_bool(self.param("stream", False) or ""):
            self.send_json(200, {"providers": [provider.hex() for provider in self.node.providers(cid)]})
            return

        def run(emit, stopped):
            """The lookup, each provider emitted as found, stopped once the client goes."""
            found = self.node.providers(cid, on_provider=lambda node_id: emit({"provider": node_id.hex()}),
                                        should_stop=stopped)
            return {"providers": [provider.hex() for provider in found]}

        self.send_lines("providers", run)

    def api_ping(self):
        """GET /api/ping?node=&stream=: round-trip time to a node, or null and why, with its path; stream=1: each step
        of opening the link as it begins (send_lines)."""
        node_id = parse_hex_id(self.param("node"))
        if not parse_bool(self.param("stream", False) or ""):
            self.send_json(200, self.node.ping_info(node_id))
            return
        self.send_lines("ping", lambda emit, stopped: self.node.ping_info(
            node_id, on_stage=lambda stage: emit({"stage": stage})))

    def api_routing(self):
        """GET /api/routing (0.2.1): the routing table's buckets, unreliable nodes and bans."""
        self.send_json(200, self.node.routing_info())

    def api_log(self):
        """GET /api/log?after=&limit=&level= (0.2.1): the daemon's recent log lines (rnfs.recentlog)."""
        from rnfs import recentlog
        from rnfs.debuglog import LEVEL_NAMES, LEVEL_NUMBERS
        after = parse_int(self.param("after", False), "after", None)
        limit = parse_int(self.param("limit", False), "limit", DEFAULT_LOG_LINES)
        if limit > recentlog.MAX_LIMIT:
            raise ValueError(f"limit may be at most {recentlog.MAX_LIMIT}")
        level_text = self.param("level", False)
        level = None
        if level_text:
            named = {name.lower(): number for name, number in LEVEL_NUMBERS.items()}
            level = named.get(level_text.lower())
            if level is None:
                level = parse_int(level_text, "level", None)
                if level not in LEVEL_NAMES:
                    raise ValueError(f"level is one of {', '.join(LEVEL_NAMES[n].lower() for n in sorted(LEVEL_NAMES))}"
                                     f" or 0 to {max(LEVEL_NAMES)}")
        self.send_json(200, recentlog.LINES.since(after, limit, level))

    def send_lines(self, what: str, run):
        """Answer with JSON lines (application/x-ndjson) as run(emit, stopped) emits them, from a thread of its own
        ("rnfs-api-<what>"), then a last line: what run returns, with "done": true. stopped() is True once the client
        has gone (which ends the answer; run then ends on its own, bounded as the request without stream= is). An
        error before the first line gets its status as any request's; after it, a last line {"error", "code",
        "status", "done": true}. No Content-Length: the connection closes after the last line."""
        from rnfs.gateway_formats import client_gone
        lines: queue.Queue = queue.Queue()
        gone = threading.Event()

        def work():
            """run, its lines and its end queued for this thread."""
            try:
                lines.put(("done", run(lambda item: lines.put(("line", item)), gone.is_set)))
            except Exception as e:  # noqa: BLE001 - handed to the request's thread, which answers it
                lines.put(("error", e))

        threading.Thread(target=work, name=f"rnfs-api-{what}", daemon=True).start()
        self.close_connection = True
        started = False
        try:
            while True:
                try:
                    kind, value = lines.get(timeout=LINE_POLL)
                except queue.Empty:
                    if client_gone(self.connection):
                        return
                    continue
                if kind == "error":
                    if not started:
                        raise value  # answered with its status, as any request's error
                    status = error_status(value)
                    log(f"API {what} failed mid-stream: {value!r}", LOG_ERROR if status >= 500 else LOG_NOTICE)
                    value = {"error": str(value) or type(value).__name__, "code": error_code(value, status),
                             "status": status}
                if not started:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/x-ndjson")
                    self.send_common_headers()
                    self.end_headers()
                    started = True
                last = kind != "line"
                try:
                    self.wfile.write(json.dumps({**value, "done": True} if last else value).encode() + b"\n")
                    self.wfile.flush()
                except OSError:  # the client went away
                    return
                if last:
                    return
        finally:
            gone.set()

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
        origin = parse_origin(self.param("origin", False))
        if origin is not None:
            options["origin"] = origin
        stream_format = self.param("format", False)
        if stream_format not in (None, "tar"):
            raise ValueError("format must be tar")
        if stream_format == "tar" and "seal_name" in options:  # a sealed folder's sealed name is <name>.tar
            raise ValueError("seal_name isn't taken with format=tar: a sealed folder is sealed as <name>.tar")
        with self.upload_job("add", origin or name, size, source) as job:
            reader = _counted(reader, job)
            if stream_format is None:
                result = self.node.add_stream(reader, size=size, name=name, only_hash=only_hash, **options)
            else:
                # add_tar reads the body to its end, checking that only zeros follow the tar's (its padding)
                result = self.node.add_tar(reader, name=name, only_hash=only_hash, **options)
            if job is not None:
                job.result = result
        self.send_json(200, result)

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
        """POST /api/gc {"dry_run"}: evict every cached fetch; dry_run (0.2.1): only say what that would free."""
        if parse_bool(self.json_body().get("dry_run")):
            self.send_json(200, self.node.gc(dry_run=True))
            return
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

    def api_name_sync(self):
        """POST /api/name/sync {"alias", "path"}: keep a name pointing at a folder's current content. Any caller may
        start or stop a name's sync, as any may publish it; the name's pin is the name's own (name:<alias>), whoever
        asked."""
        body = self.json_body()
        self.send_json(200, self.node.name_sync(parse_alias(body.get("alias")), parse_path(body.get("path"))))

    def api_name_unsync(self):
        """POST /api/name/unsync {"alias"}: stop syncing a name; it keeps pointing where it last did."""
        self.node.name_unsync(parse_alias(self.json_body().get("alias")))
        self.send_json(200, {})

    def api_name_syncs(self):
        """GET /api/name/syncs: the names synced to folders."""
        self.send_json(200, {"syncs": self.node.name_syncs()})

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
        reader = self.stream_reader()
        with self.upload_job("import", None, None if reader.chunked else reader.remaining, source) as job:
            result = self.node.import_stream(_counted(reader, job), **source)
            if job is not None:
                job.result = result
        self.send_json(200, result)

    @contextmanager
    def upload_job(self, kind: str, name: str | None, size: int | None, source: dict):
        """The job an upload runs as (JobsFeature.uploading; 0.2.1), or None without the jobs feature. One the user
        cancelled is answered 409 cancelled."""
        jobs = (getattr(self.node, "features", None) or {}).get("jobs")
        if jobs is None:
            yield None
            return
        from rnfs.errors import Cancelled
        try:
            with jobs.uploading(kind, name, size, source.get("source", USER)) as job:
                yield job
        except Cancelled as e:
            if job.cancel_requested:
                raise ApiRequestError(409, f"the upload was cancelled (job {job.id})", "cancelled") from None
            raise ApiRequestError(503, str(e) or "the daemon is stopping", "busy") from None

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

    # Pairing (0.2.1): an app asks for a token, the user approves it (rnfs.apps.PairingRequests)

    def api_app_request(self):
        """POST /api/apps/request {"description"} with X-Rnfs-Pairing: <name>, no token: {"id", "secret", "code",
        "expires"}. The custom header keeps web pages from asking (their request would need a CORS preflight)."""
        name = (self.headers.get(PAIRING_HEADER) or "").strip()
        if not name:
            raise ValueError(f"name the app in the header {PAIRING_HEADER}")
        self.send_json(200, self.node.app_request(check_app_name(name), self.json_body().get("description")))

    def api_app_request_state(self):
        """GET /api/apps/request?id= with X-Rnfs-Pairing-Secret: <its secret>, no token: {"name", "state", "token"
        (once approved, given once), "expires"}."""
        secret = (self.headers.get(PAIRING_SECRET_HEADER) or "").strip()
        if not secret:
            raise ValueError(f"give the request's secret in the header {PAIRING_SECRET_HEADER}")
        self.send_json(200, self.node.app_request_state(self.param("id"), secret))

    def api_app_requests(self):
        """GET /api/apps/requests: the apps waiting for an answer (the CLI's token only)."""
        self.send_json(200, {"requests": self.node.app_requests()})

    def api_app_approve(self):
        """POST /api/apps/approve {"id", "code"}: add the app, its token kept for it alone, if code (optional) is the
        one it shows (the CLI's token only)."""
        body = self.json_body()
        self.send_json(200, self.node.app_approve(body.get("id"), body.get("code")))

    def api_app_deny(self):
        """POST /api/apps/deny {"id"}: refuse the app's request (the CLI's token only)."""
        self.send_json(200, self.node.app_deny(self.json_body().get("id")))

    def api_shutdown(self):
        """POST /api/shutdown: answer, then stop the daemon as SIGTERM does (ApiServer's shutdown)."""
        self.json_body()
        if self.shutdown_callback is None:
            raise Unsupported("this daemon can't be stopped through its API")
        log("stopping: asked through the API", LOG_NOTICE)
        self.send_json(200, {"stopping": True})
        self.wfile.flush()
        self.shutdown_callback()

    def api_restart(self):
        """POST /api/restart (0.2.1): answer, then stop the daemon as SIGTERM does and start it again with the same
        command line (rnfs.launch), so settings that need a restart apply; unsupported where it can't."""
        self.json_body()
        if self.restart_callback is None:
            raise Unsupported("this daemon can't restart itself (daemon.json's can_restart): stop it and start it "
                              "again")
        log("restarting: asked through the API", LOG_NOTICE)
        self.send_json(200, {"restarting": True})
        self.wfile.flush()
        self.restart_callback()
