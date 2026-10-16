"""A client of a running daemon's local API (specification section 11; rnfs/api.py), for companion apps and the CLI.

Standard library only, and it never imports RNS, so an app using it starts quickly:

    from rnfs.apiclient import Client, NotFound

    api = Client()  # the daemon using ~/.rnfs/, on the api_port in its rnfs.toml
    print(api.status()["node_id"])
    cid = api.add_path("/home/me/photos")["cid"]
    with api.cat(f"{cid}/cat.jpg") as stream, open("cat.jpg", "wb") as out:
        for chunk in stream:
            out.write(chunk)
    try:
        print(api.name_resolve("ab" * 16)["cid"])
    except NotFound:
        print("not resolved")

- The daemon writes a new random token to `<rnfs dir>/api_token` (mode 0600, or a private access list on
  Windows) at each start. The client reads it before its first request that needs it and sends it as
  `X-Rnfs-Api` with every request but the open status (version()).
- Before sending the token, the client makes whatever listens on the API port prove it knows it (api_proof,
  verify_daemon), since while the daemon is down any local user could listen there. Nothing else is sent to a
  listener that fails: neither the token nor a request's body.
- That proof request is also where the daemon's version is read (Client.daemon_version), and check_daemon is the
  one place to refuse a daemon by it; every version is accepted for now.
- Every failure raises an ApiError subclass whose message says what went wrong, as the CLI prints it: no daemon
  (DaemonNotRunning), the connection failing (ConnectionFailed), the token (TokenError), the proof or the token
  refused (AuthFailed), the settings (SettingsError), and the API's own error statuses (STATUS_ERRORS: 400
  BadRequest, 404 NotFound, 413 TooLarge, 422 InvalidDag, 451 Denied, 503 Busy, 507 StorageFull, other 5xx
  ServerError).
- Each request opens its own connection, with no timeout unless one is given (fetches can take hours), so a
  Client may be used from several threads. Paths are on the daemon's machine, which is this one: they are made
  absolute here.
"""
import hashlib
import hmac
import http.client
import json
import os
import secrets
import stat
import threading
from urllib.parse import urlencode

from rnfs import fsutil, winacl
from rnfs.config import DEFAULT_API_PORT, Config

DEFAULT_DIR = Config.rnfs_dir
TOKEN_FILE = "api_token"  # in the rnfs directory; written by the daemon, read by every client
API_HEADER = "X-Rnfs-Api"
# Before sending the token, a client makes whatever listens on the API port prove it knows it (api_proof)
CHALLENGE_HEADER = "X-Rnfs-Challenge"
PROOF_HEADER = "X-Rnfs-Proof"
COPY_SIZE = 64 * 1024  # what a Stream reads at a time when iterated


class ApiError(Exception):
    """An API call failed; str() says why. status is the HTTP status, or None if the daemon never answered."""

    def __init__(self, message: str, status: int | None = None):
        """An error with message, from a response with status (None: no response)."""
        super().__init__(message)
        self.status = status


class DaemonNotRunning(ApiError):
    """Nothing listens on the API port."""


class ConnectionFailed(ApiError):
    """The daemon dropped the connection, couldn't be reached, cut a response short or sent an invalid one."""


class TokenError(ApiError):
    """The API token file is missing, unreadable, empty, or readable by other users (so not to be trusted)."""


class AuthFailed(ApiError):
    """What listens on the API port couldn't prove it knows the token (nothing was sent to it), or the daemon
    refused the request (403)."""


class SettingsError(ApiError):
    """The rnfs dir's rnfs.toml couldn't be read for the API port."""


class BadRequest(ApiError):
    """400: the request's parameters were refused (the node raised ValueError)."""


class NotFound(ApiError):
    """404: no such content, name, pin, job or endpoint."""


class TooLarge(ApiError):
    """413: over a size limit (max_download_size, a quota, the request body)."""


class InvalidDag(ApiError):
    """422: the content fetched isn't a valid DAG."""


class Denied(ApiError):
    """451: the content is on the node's deny list."""


class Busy(ApiError):
    """503: the daemon is too busy (or full of connections); try again later."""


class StorageFull(ApiError):
    """507: not enough free disk space."""


class ServerError(ApiError):
    """500 or another 5xx: the daemon failed (see its log)."""


# The API's error statuses (design.md, "Node facade"); other 4xx raise ApiError, other 5xx ServerError
STATUS_ERRORS = {400: BadRequest, 403: AuthFailed, 404: NotFound, 413: TooLarge, 422: InvalidDag, 451: Denied,
                 503: Busy, 507: StorageFull}


def error_for(status: int, message: str) -> ApiError:
    """The ApiError subclass for an error response's status, with its message."""
    error_class = STATUS_ERRORS.get(status) or (ServerError if status >= 500 else ApiError)
    return error_class(message, status)


def token_path(rnfs_dir: str = DEFAULT_DIR) -> str:
    """Where the daemon using rnfs_dir keeps its API token."""
    return os.path.join(os.path.expanduser(rnfs_dir), TOKEN_FILE)


def api_port(rnfs_dir: str = DEFAULT_DIR) -> int:
    """The port the daemon using rnfs_dir listens on: api_port in its rnfs.toml, else DEFAULT_API_PORT. Raises
    SettingsError if rnfs.toml can't be read."""
    try:
        port = Config.load(rnfs_dir).api_port
    except (ValueError, TypeError, OSError) as e:  # TypeError: a badly typed setting validate() missed
        raise SettingsError(f"can't read the settings in {rnfs_dir}: {e}") from None
    return port if port is not None else DEFAULT_API_PORT


def api_proof(token: str, port: int, challenge: str) -> str:
    """The daemon's answer to a challenge: HMAC-SHA256, keyed by the API token, of the challenge and the port
    it listens on (so a program on another port can't pass on the daemon's answer)."""
    return hmac.new(token.encode(), f"rnfs api proof {port} {challenge}".encode(), hashlib.sha256).hexdigest()


def read_token(path: str) -> str:
    """The daemon's API token from path. Raises TokenError if it is missing, unreadable or empty, or if other
    users can read it (so it can't be trusted)."""
    try:
        fd = os.open(path, os.O_RDONLY | fsutil.O_NOFOLLOW | fsutil.O_BINARY)
    except FileNotFoundError:
        raise TokenError(f"no API token at {path}: is the daemon running, with this --dir?") from None
    except PermissionError:
        raise TokenError(f"can't read the API token at {path}: permission denied. Is the daemon running as another "
                         f"user?") from None
    except OSError as e:
        raise TokenError(f"can't read the API token at {path}: {e}") from None
    with os.fdopen(fd, "rb") as f:
        mode = stat.S_IMODE(os.fstat(f.fileno()).st_mode)
        # Windows has no Unix permission bits to check, but an access list (of the very file opened)
        if not fsutil.IS_WINDOWS and mode & 0o077:
            raise TokenError(f"{path} is accessible to other users (mode {mode:04o}), so its token can't be trusted; "
                             f"restart the daemon to make a new one")
        try:
            reason = winacl.check_private(path, f.fileno())
        except OSError as e:
            raise TokenError(f"can't check who may read the API token at {path}: {e}") from None
        if reason is not None:
            raise TokenError(f"{path} is accessible to other users ({reason}), so its token can't be trusted; "
                             f"restart the daemon to make a new one")
        token = f.read(1024).decode("ascii", "replace").strip()
    if not token:
        raise TokenError(f"the API token file {path} is empty; restart the daemon")
    return token


def read_all(response: http.client.HTTPResponse) -> bytes:
    """A response's whole body. Raises ConnectionFailed if it is shorter than its Content-Length (the daemon hit
    an error)."""
    try:
        return response.read()
    except http.client.IncompleteRead:
        raise ConnectionFailed("the response was cut short; see the daemon's log") from None


class Stream:
    """A response body read as it arrives (cat, name export): read(size), iteration in COPY_SIZE chunks, close(),
    and a context manager. The last read raises ConnectionFailed if the body ended before its Content-Length."""

    def __init__(self, response: http.client.HTTPResponse):
        """The body of response, a successful one."""
        self.response = response
        header = response.getheader("Content-Length")
        try:
            self.length: int | None = int(header) if header is not None else None  # None: sent until closed
        except ValueError:
            self.length = None
        self.received = 0

    def read(self, size: int = -1) -> bytes:
        """Up to size bytes (all that is left if size is negative); b"" at the end."""
        if size is None or size < 0:
            return b"".join(iter(lambda: self.read(COPY_SIZE), b""))
        try:
            chunk = self.response.read(size)
        except http.client.IncompleteRead:
            chunk = b""
        self.received += len(chunk)
        if not chunk and size and self.length is not None and self.received != self.length:
            raise ConnectionFailed(f"the transfer stopped after {self.received} of {self.length} bytes; "
                                   f"see the daemon's log")
        return chunk

    def __iter__(self):
        """The body in chunks of at most COPY_SIZE bytes."""
        return iter(lambda: self.read(COPY_SIZE), b"")

    def close(self):
        """Close the connection (idempotent)."""
        self.response.close()

    def __enter__(self) -> "Stream":
        """The stream itself."""
        return self

    def __exit__(self, *exc):
        """Close the stream."""
        self.close()


def _flag(value: bool) -> int | None:
    """A query-string flag: 1 if set, else left out."""
    return 1 if value else None


class Client:
    """The local API of the daemon using rnfs_dir (default ~/.rnfs/), on 127.0.0.1:port (default: the api_port in
    its rnfs.toml). token_file defaults to <rnfs_dir>/api_token and is read on the first request needing it.

    request() and call() are the whole protocol; get() and post() call any endpoint (features' too); the other
    methods are the main operations and the deny list, returning what the API answers (design.md, "Node
    facade"). Every failure raises an ApiError subclass.
    """

    def __init__(self, rnfs_dir: str | None = None, port: int | None = None, token_file: str | None = None,
                 timeout: float | None = None):
        """A client; nothing is sent until the first request. Raises SettingsError if port isn't given and
        rnfs.toml can't be read."""
        rnfs_dir = DEFAULT_DIR if rnfs_dir is None else rnfs_dir
        self.port = api_port(rnfs_dir) if port is None else port
        self.token_file = token_path(rnfs_dir) if token_file is None else token_file
        self.timeout = timeout
        self.token: str | None = None  # once the daemon has proved it knows it
        self.daemon_version: str | None = None  # what the daemon said in the proof request
        self._token_lock = threading.Lock()

    # The protocol

    def request(self, method: str, path: str, params: dict | None = None, json_body: dict | None = None,
                body=None, length: int | None = None, extra_headers: dict | None = None,
                authenticated: bool = True) -> http.client.HTTPResponse:
        """Send a request and return the successful response, whose body the caller reads (read_all, Stream).

        params go in the query string (None values left out); json_body is sent as JSON; body may be bytes or a
        binary file, sent chunked without length. Unless authenticated is False (version(), and the proof
        request itself), the token is sent, once the daemon has proved it knows it.
        """
        url = path + ("?" + urlencode({k: v for k, v in params.items() if v is not None}) if params else "")
        # Host is set explicitly: the API only accepts 127.0.0.1:<port>, even for port 80
        headers = {"Host": f"127.0.0.1:{self.port}", **(extra_headers or {})}
        if authenticated:
            headers[API_HEADER] = self._proven_token()
        if json_body is not None:
            body = json.dumps(json_body).encode()
            headers["Content-Type"] = "application/json"
        elif body is not None:
            headers["Content-Type"] = "application/octet-stream"
            if length is not None:
                headers["Content-Length"] = str(length)
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=self.timeout)
        try:
            connection.request(method, url, body=body, headers=headers)
            response = connection.getresponse()
        except ConnectionRefusedError:
            raise DaemonNotRunning(f"no daemon is running on API port {self.port}. Start one with: rnfs daemon") \
                from None
        except (http.client.HTTPException, ConnectionError) as e:
            raise ConnectionFailed(f"the daemon dropped the connection ({e!r}); see its log") from None
        except OSError as e:
            raise ConnectionFailed(f"can't reach the daemon on API port {self.port}: {e}") from None
        if response.status >= 400:
            data = read_all(response)
            try:
                message = json.loads(data)["error"]
            except (ValueError, KeyError, TypeError):
                message = f"HTTP {response.status} {response.reason}"
            raise error_for(response.status, str(message))
        return response

    def _proven_token(self) -> str:
        """The token, read and proved (verify_daemon) on first use."""
        with self._token_lock:
            if self.token is None:
                token = read_token(self.token_file)
                self.verify_daemon(token)
                self.token = token
            return self.token

    def verify_daemon(self, token: str):
        """Raise AuthFailed unless whatever listens on the API port proves it knows token, before the token (or
        a name key) is sent to it: while the daemon is down, any local user could be listening on its port.
        Then check_daemon() with its version."""
        challenge = secrets.token_hex(16)
        response = self.request("GET", "/api/status", extra_headers={CHALLENGE_HEADER: challenge},
                                authenticated=False)
        data = read_all(response)
        proof = (response.getheader(PROOF_HEADER) or "").strip().encode("ascii", "replace")
        if not hmac.compare_digest(proof, api_proof(token, self.port, challenge).encode()):
            raise AuthFailed(f"what listens on API port {self.port} couldn't prove it knows the API token in "
                             f"{self.token_file}, so nothing was sent to it: it may be another program, or a "
                             f"daemon using another --dir (or one started before an upgrade: restart it)")
        try:
            version = json.loads(data).get("version")
        except (ValueError, AttributeError):
            version = None
        self.daemon_version = version if isinstance(version, str) else None
        self.check_daemon(self.daemon_version)

    def check_daemon(self, version: str | None):
        """The one place to refuse a daemon by its version (None if it didn't say), before the token is sent;
        raise an ApiError to refuse. Every version is accepted for now."""

    def call(self, method: str, path: str, params: dict | None = None, json_body: dict | None = None,
             body=None, length: int | None = None, authenticated: bool = True):
        """Send a request and return its decoded JSON response."""
        response = self.request(method, path, params, json_body, body, length, authenticated=authenticated)
        try:
            return json.loads(read_all(response))
        except ValueError:
            raise ConnectionFailed("the daemon sent an invalid response") from None

    def get(self, path: str, /, **params):
        """GET path with query parameters; JSON response."""
        return self.call("GET", path, params=params)

    def post(self, path: str, /, **fields):
        """POST path with a JSON body; JSON response."""
        return self.call("POST", path, json_body=fields)

    def _post(self, path: str, /, **fields):
        """POST path with the fields that aren't None."""
        return self.post(path, **{name: value for name, value in fields.items() if value is not None})

    # Status

    def version(self) -> str:
        """The daemon's version, from the status anyone may ask for (no token needed or sent)."""
        return self.call("GET", "/api/status", authenticated=False)["version"]

    def status(self) -> dict:
        """The node's status: node ID, DHT mode, counts, storage and every feature's fields."""
        return self.get("/api/status")

    # Content

    def add_path(self, path: str, wrap: bool = False, only_hash: bool = False, pin: bool = True,
                 to: list[str] | None = None, to_self: bool = False, seal_name: bool = True,
                 chunker: str | None = None) -> dict:
        """Add a file or directory on this machine: {"cid", "size", "warnings"} (+ "sealed"). pin=False keeps it
        as evictable cache; to/to_self seal it for those recipients; chunker is "fixed" or "fastcdc"."""
        return self._post("/api/add", path=os.path.abspath(os.path.expanduser(path)), wrap=wrap or None,
                          only_hash=only_hash or None, pin=None if pin else False, to=to or None,
                          to_self=to_self or None, seal_name=None if seal_name else False, chunker=chunker)

    def add_stream(self, data, name: str | None = None, length: int | None = None, only_hash: bool = False,
                   pin: bool = True, to: list[str] | None = None, to_self: bool = False,
                   seal_name: str | None = None, chunker: str | None = None) -> dict:
        """Add bytes, or a binary file read to its end (sent chunked unless length is given): as add_path. name
        wraps it in a directory as that entry; when sealing, seal_name is the name sealed inside."""
        if isinstance(data, (bytes, bytearray, memoryview)) and length is None:
            length = len(data)
        params = {"name": name, "only_hash": _flag(only_hash), "pin": None if pin else 0,
                  "to": ",".join(to) if to else None, "to_self": _flag(to_self), "seal_name": seal_name,
                  "chunker": chunker}
        return self.call("POST", "/api/add/stream", params=params, body=data, length=length)

    def fetch(self, target: str, output: str, from_node: str | None = None, raw: bool = False) -> dict:
        """Fetch a file or directory and have the daemon write it to output (a path on this machine, which must
        not exist), in one request: {"cid", "warnings"} (+ "sealed", "name"). For progress, use submit_job."""
        return self._post("/api/get", target=target, output=os.path.abspath(os.path.expanduser(output)),
                          raw=raw or None, **{"from": from_node})

    def cat(self, target: str, offset: int | None = None, length: int | None = None, raw: bool = False,
            block: bool = False) -> Stream:
        """A file's bytes (or a range of them) as they arrive. raw: a sealed file as stored; block: the CID's own
        block, verified."""
        return Stream(self.request("GET", "/api/cat", params={"target": target, "offset": offset, "length": length,
                                                             "raw": _flag(raw), "block": _flag(block)}))

    def info(self, target: str, from_node: str | None = None) -> dict:
        """{"cid", "type", "size", "executable"} (+ sealed-file fields) of a target."""
        return self.get("/api/info", target=target, **{"from": from_node})

    def ls(self, target: str) -> list[dict]:
        """A directory's entries: [{"name", "kind", "size", "cid"}]."""
        return self.get("/api/ls", target=target)["entries"]

    def stat(self, target: str) -> dict:
        """{"cid", "size", "blocks", "local_blocks", "local_bytes"} of a target."""
        return self.get("/api/stat", target=target)

    # Pins

    def pin_add(self, target: str) -> dict:
        """Fetch and pin a target, in one request: {"cid"} once complete. For progress, use submit_job."""
        return self.post("/api/pin/add", target=target)

    def pin_rm(self, cid: str):
        """Remove the user pin of cid (NotFound if it has none)."""
        self.post("/api/pin/rm", cid=cid)

    def pin_ls(self, cid: str | None = None) -> list[dict]:
        """Every pin (or cid's): [{"cid", "sources", "complete", "type", "sealed", "name"}]."""
        return self.get("/api/pin/ls", cid=cid)["pins"]

    # Names

    def name_list(self) -> list[dict]:
        """This node's names: [{"alias", "name", "cid", "sequence"}]."""
        return self.get("/api/name/list")["names"]

    def name_create(self, alias: str) -> dict:
        """Create a name key: {"alias", "name"}."""
        return self.post("/api/name/create", alias=alias)

    def name_publish(self, alias: str, target: str) -> dict:
        """Point a name of this node at a target: {"name", "cid", "sequence"}."""
        return self.post("/api/name/publish", alias=alias, target=target)

    def name_resolve(self, name: str) -> dict:
        """{"name", "cid", "sequence", "age"} of a name (32 hex characters, or a name contact's nick); NotFound if
        it doesn't resolve."""
        return self.get("/api/name/resolve", name=name)

    # The deny list

    def deny(self) -> dict:
        """{"entries": [the local list], "lists": [{"source", "cid", "entries", "state"}]}."""
        return self.get("/api/deny")

    def deny_add(self, entries: list[str]) -> list[str]:
        """Add CIDs or file hashes (sha256:<hex>) to the local deny list; those new."""
        return self.post("/api/deny/add", entries=list(entries))["added"]

    def deny_rm(self, entries: list[str]) -> list[str]:
        """Remove entries from the local deny list (NotFound, removing none, if one isn't on it)."""
        return self.post("/api/deny/rm", entries=list(entries))["removed"]

    # Jobs (background transfers, with progress)

    def submit_job(self, kind: str, target: str, output: str | None = None, from_node: str | None = None,
                   **options) -> dict:
        """Start a job: kind "get" (output needed), "pin" or "export" (output needed). Its record, with its
        "id". options: get's sealed_name and fallback_dht."""
        output = os.path.abspath(os.path.expanduser(output)) if output is not None else None
        return self._post("/api/jobs", kind=kind, target=target, output=output, **{"from": from_node}, **options)

    def job(self, job_id: str, wait: int = 0) -> dict:
        """A job's record; waits up to wait seconds (the API caps it) for it to change state or progress."""
        return self.get("/api/jobs/get", id=job_id, wait=wait or None)

    def jobs(self) -> list[dict]:
        """Every job's record, oldest first."""
        return self.get("/api/jobs")["jobs"]

    def cancel_job(self, job_id: str) -> dict:
        """Cancel a job: its record."""
        return self.post("/api/jobs/cancel", id=job_id)

    def clear_jobs(self, job_id: str | None = None) -> int:
        """Forget finished jobs (or that one); how many."""
        return self._post("/api/jobs/clear", id=job_id)["removed"]
