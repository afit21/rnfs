"""The protocol layer of a running daemon's local API (specification section 11; rnfs/api.py): what rnfs.Client
(rnfs/remote.py) and the CLI are built on. Programs use rnfs.Client, whose objects and errors are rnfs.Node's
too; ApiConnection's dict-returning methods are its backend, and the CLI's ApiClient subclasses it.

Standard library only, and it never imports RNS, so a program using it starts quickly.

- The daemon writes a new random token to `<rnfs dir>/api_token` (mode 0600, or a private access list on
  Windows) at each start. The client reads it before its first request that needs it, and again
  when a proof with it fails, and sends it as `X-Rnfs-Api` with every request but the open status (version()).
- An app (rnfs app add; 0.2.0) uses its own token instead, given (token=) or in a file it names (token_file=,
  checked as api_token is), and names itself (app=) in `X-Rnfs-App` on every request: the proof is then keyed
  by its token's verifier (app_proof_key), which is what the daemon keeps of it. An app's token is not scoped:
  it does all the CLI's does but manage apps. Its pins get the source app:<app>, or app:<app>:<key> for the
  requests made inside pin_key(key) (PIN_KEY_HEADER).
- Before sending the token, the client makes whatever listens on the API port prove it knows it (api_proof,
  verify_daemon), since while the daemon is down any local user could listen there. Nothing else is sent to a
  listener that fails: neither the token nor a request's body. Nor is anything it answers believed: whatever its
  status and text, the client raises AuthFailed (Busy for a 503, as a full daemon answers) in its own words.
  Connecting and proving take at most PROOF_TIMEOUT seconds, reading at most PROOF_READ_LIMIT bytes. The proof
  is made on the connection that then carries the request (the daemon keeps it open for that one request), for
  every request, so a client used for a long time sends nothing to a program listening on the port after the
  daemon stopped. A daemon that closes that connection gets nothing more (ConnectionFailed).
- The port: a running daemon (0.2.0 on) writes `<rnfs dir>/daemon.json` (read_daemon_info: its PID, ports,
  version, start time and Reticulum config), which is read first, so a client finds the daemon whatever its
  rnfs.toml holds (an older client can't read a newer one's settings); without it (or with a stale one), the
  api_port in rnfs.toml. Nothing is trusted from it but the port, and the proof below still comes first.
- That proof request is also where the daemon's version is read (Client.daemon_version), and check_daemon is the
  one place to refuse a daemon by it: one before 0.1.5 (OLDEST_DAEMON), which closes the connection it proved
  itself on so the request could only go on a new one, is refused before the token is sent (until 0.2.0 the
  client did send it on a new connection).
- Every failure raises an ApiError subclass whose message says what went wrong, as the CLI prints it: no daemon
  (DaemonNotRunning; DaemonStarting when nothing answers but a process holds the rnfs dir's lock, as a daemon
  does from before it opens its block store until it has stopped), the connection failing (ConnectionFailed), the token (TokenError), the proof or the token
  refused (AuthFailed), the settings (SettingsError), and the API's own error statuses (STATUS_ERRORS, from
  rnfs.errors.STATUSES: 400 BadRequest, 404 NotFound, 413 TooLarge, 422 InvalidDag, 451 Denied, 503 Busy, 507
  StorageFull, other 5xx ServerError). ApiError is an rnfs.errors.RnfsError, and each status's error also a
  subclass of the node's own (NotFound of rnfs.errors.NotFound, BadRequest of ValueError, ...), so one except
  clause catches what rnfs.Node and rnfs.Client raise alike. Every ApiError has the daemon's machine-readable
  code (rnfs.errors.CODES; None from a daemon before 0.2.0): a 404 for an endpoint the daemon doesn't have is
  NoSuchEndpoint (also told from an older daemon's message), one for something it can't do Unsupported, both
  NotFound subclasses.
- Each request opens its own connection (its proof, then the request), so an ApiConnection may be used from
  several threads. A request has no timeout unless one is given (fetches can take hours): the connection's
  (timeout=), replaced for every request one thread makes inside using_timeout(seconds), or for one call by
  request/call/get/post's own timeout= (None: none, even if the connection has one). Connecting and proving take
  at most the smaller of it and PROOF_TIMEOUT. A request outlasting it raises ConnectionFailed. Paths are on the
  daemon's machine, which is this one: they are made absolute here.
"""
import contextlib
import hashlib
import hmac
import http.client
import json
import math
import os
import re
import secrets
import stat
import threading
import time
import warnings
from urllib.parse import urlencode

from rnfs import errors, fsutil, winacl
from rnfs.config import DEFAULT_API_PORT, LOCK_FILE, Config

DEFAULT_DIR = Config.rnfs_dir
TOKEN_FILE = "api_token"  # in the rnfs directory; written by the daemon, read by every client
DAEMON_FILE = "daemon.json"  # in the rnfs directory: the running daemon's PID, ports, version (read_daemon_info)
MAX_DAEMON_FILE = 64 * 1024  # bytes of daemon.json read at most (the daemon's is a few hundred)
API_HEADER = "X-Rnfs-Api"
# An app's requests (rnfs app add; rnfs 0.2.0) name it, and carry its token in API_HEADER: the daemon then checks
# that token, keys the proof with it (app_proof_key) and gives the app's pins its source (rnfs.apps)
APP_HEADER = "X-Rnfs-App"
# An app's request may name a group of its pins: they are then pinned and released as app:<app>:<key>
PIN_KEY_HEADER = "X-Rnfs-Pin-Key"
# Before sending the token, a client makes whatever listens on the API port prove it knows it (api_proof)
CHALLENGE_HEADER = "X-Rnfs-Challenge"
PROOF_HEADER = "X-Rnfs-Proof"
PROOF_TIMEOUT = 15  # seconds to connect and prove (at most the client's timeout): a listener mustn't hang a client
PROOF_READ_LIMIT = 64 * 1024  # the most read of a proof's response (the daemon's is a few dozen bytes)
COPY_SIZE = 64 * 1024  # what a Stream reads at a time when iterated
# The oldest daemon a client talks to: one before 0.1.5 closes the connection it proved itself on, so the request
# could only go on a new connection, which another listener could take (clients before 0.2.0 sent it there)
OLDEST_DAEMON = (0, 1, 5)


class ApiError(errors.RnfsError):
    """An API call failed; str() says why. status is the HTTP status, or None if the daemon never answered; code
    the daemon's machine-readable code (rnfs.errors.CODES), or None (no response, or a daemon before 0.2.0)."""

    def __init__(self, message: str, status: int | None = None, code: str | None = None):
        """An error with message, from a response with status (None: no response) and code."""
        super().__init__(message)
        self.status = status
        self.code = code


class DaemonNotRunning(ApiError):
    """Nothing listens on the API port."""


class DaemonStarting(DaemonNotRunning):
    """Nothing listens on the API port, but a process holds the rnfs dir's lock (dir_holder), its process ID pid: a
    daemon still starting (it locks the dir before opening the block store, which takes a while when large, and
    listens only after), or stopping (it keeps the lock until it has saved its state), or an app's own rnfs.Node,
    which serves no API. A DaemonNotRunning, so code catching that is unchanged."""

    def __init__(self, message: str, pid: int):
        """The error, naming the process pid."""
        super().__init__(message)
        self.pid = pid


class ConnectionFailed(ApiError):
    """The daemon dropped the connection, couldn't be reached, cut a response short or sent an invalid one."""


class TokenError(ApiError):
    """The API token file is missing, unreadable, empty, or readable by other users (so not to be trusted)."""


class AuthFailed(ApiError):
    """What listens on the API port couldn't prove it knows the token (nothing was sent to it), or the daemon
    refused the request (403)."""


class SettingsError(ApiError):
    """The rnfs dir's rnfs.toml couldn't be read for the API port."""


class BadRequest(ApiError, ValueError):
    """400: the request's parameters were refused (the node raised ValueError)."""


class NotFound(ApiError, errors.NotFound):
    """404: no such content, name, pin, job or endpoint."""


class NoSuchEndpoint(NotFound):
    """404 no_such_endpoint: the daemon has no such endpoint (it predates it), not that some content is missing."""


class Unsupported(NotFound, errors.Unsupported):
    """404 unsupported: the daemon can't do this at all (a daemon that can't be stopped through its API, say)."""


class TooLarge(ApiError, errors.TooLarge):
    """413: over a size limit (max_download_size, a quota, the request body)."""


class InvalidDag(ApiError, errors.InvalidDag):
    """422: the content fetched isn't a valid DAG."""


class Denied(ApiError, errors.Denied):
    """451: the content is on the node's deny list."""


class Busy(ApiError, errors.Busy):
    """503: the daemon is too busy (or full of connections); try again later."""


class StorageFull(ApiError, errors.StorageFull):
    """507: not enough free disk space."""


class ServerError(ApiError):
    """500 or another 5xx: the daemon failed (see its log)."""


# The client's error for each of the node's (errors.STATUSES), a subclass of both ApiError and the node's class
_STATUS_CLASSES = (BadRequest, NotFound, TooLarge, InvalidDag, Denied, Busy, StorageFull)


def _status_errors() -> dict[int, type[ApiError]]:
    """Each API error status's ApiError subclass: errors.STATUSES' statuses, each to the client class of its node
    error, and 403 (the token refused) to AuthFailed. A status added there without a class here fails here, at
    import."""
    table: dict[int, type[ApiError]] = {403: AuthFailed}
    for error_class, status in errors.STATUSES:
        table[status] = next(client for client in _STATUS_CLASSES if issubclass(client, error_class))
    return table


# The API's error statuses (design.md, "Node facade"); other 4xx raise ApiError, other 5xx ServerError
STATUS_ERRORS = _status_errors()


# A daemon before 0.2.0 sends no code; its 404 for an unknown endpoint says this
_NO_ENDPOINT_MESSAGE = "no such API endpoint"


def error_for(status: int, message: str, code: str | None = None) -> ApiError:
    """The ApiError subclass for an error response's status (and code), with its message."""
    if status == 404 and (code == "no_such_endpoint" or (code is None and message.startswith(_NO_ENDPOINT_MESSAGE))):
        return NoSuchEndpoint(message, status, code or "no_such_endpoint")
    if status == 404 and code == "unsupported":
        return Unsupported(message, status, code)
    error_class = STATUS_ERRORS.get(status) or (ServerError if status >= 500 else ApiError)
    return error_class(message, status, code)


def version_numbers(version) -> tuple[int, ...] | None:
    """The numbers a version string starts with ("0.1.4.post1": (0, 1, 4)); None if it isn't one that does."""
    match = re.match(r"\d+(?:\.\d+)*", version) if isinstance(version, str) else None
    return tuple(int(number) for number in match.group().split(".")) if match else None


def token_path(rnfs_dir: str = DEFAULT_DIR) -> str:
    """Where the daemon using rnfs_dir keeps its API token."""
    return os.path.join(os.path.expanduser(rnfs_dir), TOKEN_FILE)


def load_settings(rnfs_dir: str = DEFAULT_DIR) -> Config:
    """The settings of the daemon using rnfs_dir (Config.load), with what was changed to trust them in its
    warnings, for the caller to show. Raises SettingsError if rnfs.toml can't be read or trusted."""
    try:
        return Config.load(rnfs_dir)
    except (ValueError, TypeError, OSError) as e:  # TypeError: a badly typed setting validate() missed
        raise SettingsError(f"can't read the settings in {rnfs_dir}: {e}") from None


def daemon_info_path(rnfs_dir: str = DEFAULT_DIR) -> str:
    """Where the daemon using rnfs_dir describes itself (DAEMON_FILE)."""
    return os.path.join(os.path.expanduser(rnfs_dir), DAEMON_FILE)


def dir_holder(rnfs_dir: str = DEFAULT_DIR) -> int | None:
    """The process ID of the daemon or rnfs.Node holding rnfs_dir's lock (<rnfs dir>/daemon.lock), or None if none
    does or it can't be told. The process the lock file names is checked first, and the lock tested only while it
    runs: testing it could make a daemon starting at that very instant fail (fsutil.lock_held). This process is
    never named (its own Node holds the lock, but this isn't a daemon for its Client to wait for)."""
    path = os.path.join(os.path.expanduser(rnfs_dir), LOCK_FILE)
    pid = fsutil.lock_holder(path)
    if pid is None or pid <= 0 or pid == os.getpid() or not _process_alive(pid):
        return None
    try:
        return pid if fsutil.lock_held(path) else None
    except OSError:
        return None


def _process_alive(pid: int) -> bool:
    """False only if no process pid runs; True when it does, or when that can't be told."""
    if fsutil.IS_WINDOWS:
        return _windows_process_alive(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:  # PermissionError: it runs, as another user
        return True
    return True


def _windows_process_alive(pid: int) -> bool:
    """Windows: False only if OpenProcess says no process pid exists, or it has exited (GetExitCodeProcess);
    True otherwise, also when that can't be told (another user's process can't be opened: it runs)."""
    try:
        import ctypes
        from ctypes import wintypes
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel32.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        handle = kernel32.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return ctypes.get_last_error() != 87  # ERROR_INVALID_PARAMETER: no such process
        try:
            code = wintypes.DWORD()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return True
            return code.value == 259  # STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)
    except (OSError, AttributeError, ValueError):
        return True


def read_daemon_info(rnfs_dir: str = DEFAULT_DIR) -> dict | None:
    """What the daemon using rnfs_dir wrote about itself at its start (DAEMON_FILE): {"pid", "api_port",
    "gateway_port", "version", "started", "rnfs_dir", "reticulum_config", "shared_instance", "node_id"}, or None
    if there is none, it can't be trusted (a symlink, not a regular file, writable by other users), it doesn't
    parse, or its process no longer runs (a daemon that crashed). Never raises for a bad file: a client then uses
    rnfs.toml, as before daemons wrote one."""
    path = daemon_info_path(rnfs_dir)
    try:
        fd = os.open(path, os.O_RDONLY | fsutil.O_NOFOLLOW | fsutil.O_BINARY | getattr(os, "O_NONBLOCK", 0))
    except OSError:
        return None
    try:
        with os.fdopen(fd, "rb") as f:
            info = os.fstat(f.fileno())
            if not stat.S_ISREG(info.st_mode):
                return None
            if not fsutil.IS_WINDOWS and (info.st_mode & 0o022 or info.st_uid != os.getuid()):
                return None
            raw = f.read(MAX_DAEMON_FILE + 1)
        if len(raw) > MAX_DAEMON_FILE:
            return None
        data = json.loads(raw)
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    pid, port = data.get("pid"), data.get("api_port")
    if type(pid) is not int or type(port) is not int or not 1 <= port <= 65535 or not _process_alive(pid):
        return None
    return data


def api_port(rnfs_dir: str = DEFAULT_DIR) -> int:
    """The port the daemon using rnfs_dir listens on: the one in its daemon.json (read_daemon_info), else api_port
    in its rnfs.toml, else DEFAULT_API_PORT. A settings file made private on the way (others could access it) is
    reported as a RuntimeWarning. Raises SettingsError if there is no usable daemon.json and rnfs.toml can't be
    read."""
    running = read_daemon_info(rnfs_dir)
    if running is not None:
        return running["api_port"]
    config = load_settings(rnfs_dir)
    for message in config.warnings:
        warnings.warn(message, RuntimeWarning, stacklevel=2)
    return config.api_port if config.api_port is not None else DEFAULT_API_PORT


def api_proof(token: str, port: int, challenge: str) -> str:
    """The daemon's answer to a challenge: HMAC-SHA256, keyed by the API token, of the challenge and the port
    it listens on (so a program on another port can't pass on the daemon's answer)."""
    return hmac.new(token.encode(), f"rnfs api proof {port} {challenge}".encode(), hashlib.sha256).hexdigest()


def app_verifier(token: str) -> bytes:
    """What the daemon keeps of an app's token (rnfs.apps) instead of the token: SHA-256 of "rnfs app token " and
    the token. It checks a token presented, and (in hex, app_proof_key) keys the proof the daemon gives the app."""
    return hashlib.sha256(b"rnfs app token " + token.encode("utf-8", "replace")).digest()


def app_proof_key(token: str) -> str:
    """The key of the proof an app checks (api_proof's token, in place of the API token): its token's verifier in
    hex, which the daemon has without keeping the token."""
    return app_verifier(token).hex()


def read_token(path: str, app: str | None = None) -> str:
    """The daemon's API token from path, or (app) that app's token. Raises TokenError if it is missing, unreadable
    or empty, or if other users can read it (so it can't be trusted)."""
    try:
        fd = os.open(path, os.O_RDONLY | fsutil.O_NOFOLLOW | fsutil.O_BINARY)
    except FileNotFoundError:
        if app is not None:
            raise TokenError(f"no token for app {app} at {path} (rnfs app add {app} -o {path} writes one)") from None
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
    except TimeoutError:
        raise ConnectionFailed("the daemon didn't finish its answer within the timeout") from None
    except OSError as e:  # a reset
        raise ConnectionFailed(f"the response failed ({e!r}); see the daemon's log") from None


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
        except TimeoutError:
            raise ConnectionFailed(f"the daemon stopped sending within the timeout, after {self.received} bytes") \
                from None
        except OSError as e:  # a reset
            raise ConnectionFailed(f"the transfer failed after {self.received} bytes ({e!r}); see the daemon's "
                                   f"log") from None
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


class _Default:
    """The type of DEFAULT."""

    def __repr__(self) -> str:
        """Its name, for signatures in help()."""
        return "DEFAULT"


# A request's timeout when none is given for it: this thread's (using_timeout), else the connection's. Not None,
# which is a timeout of its own: none at all
DEFAULT = _Default()


def check_timeout(timeout) -> float | None:
    """timeout if it is None or a positive number of seconds; ValueError otherwise (0 would make the socket
    non-blocking, and a bool is a mistake)."""
    if timeout is None:
        return None
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout < math.inf:
        raise ValueError(f"a timeout is None or a positive number of seconds, not {timeout!r}")
    return timeout


class ApiConnection:
    """The local API of the daemon using rnfs_dir (default ~/.rnfs/), on 127.0.0.1:port (default: the api_port in
    its rnfs.toml). token_file defaults to <rnfs_dir>/api_token and is read on the first request needing it. As an
    app (app=, its name: rnfs app add), token (the app's token) or token_file (a file holding it) is needed.

    request() and call() are the whole protocol; get() and post() call any endpoint (features' too); the other
    methods are every core route (api.ApiHandler.ROUTES), jobs, the deny list and the features' routes rnfs.Client
    uses (peers, contacts, keep, follow, LXMF), returning what the API answers (design.md, "Node facade"). Every
    failure raises an ApiError subclass. timeout bounds each request (None: no bound; see the module docstring for
    using_timeout and a call's own).
    """

    def __init__(self, rnfs_dir: str | None = None, port: int | None = None, token_file: str | None = None,
                 timeout: float | None = None, *, app: str | None = None, token: str | None = None):
        """A client; nothing is sent until the first request. Raises SettingsError if port isn't given and
        rnfs.toml can't be read, ValueError for an app without token or token_file, or a token that is empty or
        given with a token_file."""
        if token is not None and (not isinstance(token, str) or not token.strip() or token_file is not None):
            raise ValueError("token is a non-empty string, given instead of token_file")
        if app is not None and token is None and token_file is None:
            raise ValueError(f"app {app} needs its token: token= or token_file= (rnfs app add {app} gives it)")
        if app is not None and (not isinstance(app, str) or not app or not app.isprintable()):
            raise ValueError(f"invalid app name {app!r}")
        rnfs_dir = DEFAULT_DIR if rnfs_dir is None else rnfs_dir
        self.rnfs_dir = rnfs_dir
        self.port = api_port(rnfs_dir) if port is None else port
        self.app = app  # sent as X-Rnfs-App with every request: the daemon checks the token as this app's
        self._given_token = token.strip() if token is not None else None  # never read again from a file
        self.token_file = token_path(rnfs_dir) if token_file is None and token is None else token_file
        self.timeout = timeout  # each request's, unless the thread or the call gives another
        self._local = threading.local()  # .timeout: this thread's, inside using_timeout(); .pin_key, pin_key()
        self.token: str | None = None  # the token read last, once a daemon has proved it knows it
        self.daemon_version: str | None = None  # what the daemon said in the last proof request

    # Timeouts

    def effective_timeout(self, timeout=DEFAULT) -> float | None:
        """The timeout a request made now by this thread gets: timeout unless DEFAULT, else the one this thread's
        using_timeout() set, else the connection's. ValueError for a timeout given that isn't one."""
        if timeout is not DEFAULT:
            return check_timeout(timeout)
        timeout = getattr(self._local, "timeout", DEFAULT)
        return self.timeout if timeout is DEFAULT else timeout

    @contextlib.contextmanager
    def using_timeout(self, timeout: float | None):
        """A context manager: requests this thread makes inside it get timeout (None: none) instead of the
        connection's, unless a call gives its own. Thread-local, so other threads (and their requests) keep
        theirs; nests, restoring the outer one on exit. ValueError unless None or a positive number."""
        check_timeout(timeout)
        previous = getattr(self._local, "timeout", DEFAULT)
        self._local.timeout = timeout
        try:
            yield self
        finally:
            self._local.timeout = previous

    @contextlib.contextmanager
    def pin_key(self, key: str | None):
        """A context manager: requests this thread makes inside it carry key as X-Rnfs-Pin-Key, so the pins this
        app makes and releases in them (add, pin, dir set/rm, import, and those jobs) are app:<app>:<key>'s, a group
        of its own (None: app:<app>'s, as outside). Thread-local and nesting, as using_timeout. ValueError unless
        this connection is an app's, or for a key that isn't a non-empty string (the daemon checks its characters:
        BadRequest)."""
        if self.app is None:
            raise ValueError("a pin key is for apps (app=): the CLI's pins are always user's")
        if key is not None and (not isinstance(key, str) or not key):
            raise ValueError(f"a pin key is a non-empty string, not {key!r}")
        previous = getattr(self._local, "pin_key", None)
        self._local.pin_key = key
        try:
            yield self
        finally:
            self._local.pin_key = previous

    def _identity_headers(self) -> dict:
        """The headers naming who calls: X-Rnfs-App for an app, with its pin key inside pin_key()."""
        if self.app is None:
            return {}
        key = getattr(self._local, "pin_key", None)
        return {APP_HEADER: self.app, **({PIN_KEY_HEADER: key} if key is not None else {})}

    # The protocol

    def request(self, method: str, path: str, params: dict | None = None, json_body: dict | None = None,
                body=None, length: int | None = None, extra_headers: dict | None = None,
                authenticated: bool = True, timeout=DEFAULT) -> http.client.HTTPResponse:
        """Send a request and return the successful response, whose body the caller reads (read_all, Stream).

        params go in the query string (None values left out); json_body is sent as JSON; body may be bytes or a
        binary file, sent chunked without length. Unless authenticated is False (version(), and the proof
        request itself), the token is sent, on a connection whose other end has just proved it knows it.
        timeout bounds this request (None: no bound; DEFAULT: effective_timeout()), connecting and proving within
        at most PROOF_TIMEOUT of it; it stays the response's, for reading its body.
        """
        timeout = self.effective_timeout(timeout)
        url = path + ("?" + urlencode({k: v for k, v in params.items() if v is not None}) if params else "")
        # Host is set explicitly: the API only accepts 127.0.0.1:<port>, even for port 80
        headers = {"Host": f"127.0.0.1:{self.port}", **(self._identity_headers() if authenticated else {}),
                   **(extra_headers or {})}
        if json_body is not None:
            body = json.dumps(json_body).encode()
            headers["Content-Type"] = "application/json"
        elif body is not None:
            headers["Content-Type"] = "application/octet-stream"
            if length is not None:
                headers["Content-Length"] = str(length)
        if authenticated:
            connection, headers[API_HEADER] = self._prove(self._connect(self._proof_timeout(timeout)), timeout)
            # Proved: the request itself may take as long as its timeout allows
            connection.timeout = timeout
            if connection.sock is not None:
                connection.sock.settimeout(timeout)
        else:
            connection = self._connect(timeout)
        response = self._exchange(connection, method, url, body, headers)
        try:
            return self._raise_for(response)
        except ApiError:
            connection.close()
            raise

    def _connect(self, timeout: float | None) -> http.client.HTTPConnection:
        """A new connection to the API port (opened by its first request), with timeout (None: none)."""
        return http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)

    @staticmethod
    def _proof_timeout(timeout: float | None) -> float:
        """The timeout for connecting and proving before a request with timeout: PROOF_TIMEOUT, or timeout if
        shorter (a listener mustn't hold a request longer than the caller allows it in all)."""
        return PROOF_TIMEOUT if timeout is None else min(timeout, PROOF_TIMEOUT)

    def _exchange(self, connection: http.client.HTTPConnection, method: str, url: str, body,
                  headers: dict) -> http.client.HTTPResponse:
        """Send one request on connection and return its response, whatever its status; a connection that fails
        raises DaemonNotRunning (DaemonStarting while a process holds the rnfs dir) or ConnectionFailed (also when
        the answer doesn't come within its timeout)."""
        try:
            connection.request(method, url, body=body, headers=headers)
            return connection.getresponse()
        except (http.client.HTTPException, OSError) as e:
            connection.close()
            if isinstance(e, ConnectionRefusedError):
                holder = dir_holder(self.rnfs_dir)
                if holder is not None:
                    raise DaemonStarting(f"the daemon (process {holder}) is starting: it holds {self.rnfs_dir} but "
                                         f"doesn't answer on API port {self.port} yet; try again in a moment. If "
                                         f"this lasts, process {holder} may be stopping, or be an app's own rnfs "
                                         f"node, which has no API", holder) from None
                raise DaemonNotRunning(f"no daemon is running on API port {self.port}. Start one with: rnfs daemon") \
                    from None
            if isinstance(e, TimeoutError) and connection.timeout is not None:  # else the system's (ETIMEDOUT)
                raise ConnectionFailed(f"the daemon on API port {self.port} didn't answer within its timeout "
                                       f"({connection.timeout:g} seconds)") from None
            if isinstance(e, (http.client.HTTPException, ConnectionError)):
                raise ConnectionFailed(f"the daemon dropped the connection ({e!r}); see its log") from None
            raise ConnectionFailed(f"can't reach the daemon on API port {self.port}: {e}") from None

    def _raise_for(self, response: http.client.HTTPResponse, data: bytes | None = None) -> http.client.HTTPResponse:
        """response if it succeeded; else raise the ApiError for its status (error_for, the one mapping), with the
        message of its body (data, if read already). Only for a response of the daemon, or of the open status."""
        if response.status < 400:
            return response
        if data is None:
            data = read_all(response)
        try:
            body = json.loads(data)
            message = body["error"]
        except (ValueError, KeyError, TypeError):
            body, message = None, f"HTTP {response.status} {response.reason}"
        code = body.get("code") if isinstance(body, dict) else None
        raise error_for(response.status, str(message), code if isinstance(code, str) else None)

    def _prove(self, connection: http.client.HTTPConnection,
               timeout: float | None) -> tuple[http.client.HTTPConnection, str]:
        """Make what listens on the API port prove, on connection, that it knows the token, and return the
        connection to send a request with timeout on, and the token. The daemon keeps a connection it proved itself
        on open for one more request, so the token and the request's body can only reach it: a proof made once,
        on another connection, would hold no longer than the daemon, and any local user can listen on its port
        once it stops. The token read is kept, and read again if the proof fails (a daemon restarted since
        wrote a new one). A daemon that closes the connection anyway gets nothing more: ConnectionFailed (one
        before 0.1.5, which always does, was refused by check_daemon already). A token given (token=) is never
        read again."""
        token = self._given_token or self.token or read_token(self.token_file, self.app)
        try:
            self.verify_daemon(token, connection)
        except AuthFailed:
            if self._given_token is not None:
                raise
            fresh = read_token(self.token_file, self.app)
            if fresh == token:
                raise
            connection.close()
            connection = self._connect(self._proof_timeout(timeout))
            self.verify_daemon(fresh, connection)
            token = fresh
        self.token = token
        if connection.sock is None:
            # A new connection could reach another listener (the daemon may stop in between): nothing more is sent
            raise ConnectionFailed(f"the daemon on API port {self.port} closed the connection it proved itself on, "
                                   f"so the request wasn't sent; see its log")
        connection.auto_open = 0  # never reconnect: a new connection could reach another listener
        return connection, token

    def verify_daemon(self, token: str, connection: http.client.HTTPConnection | None = None) -> str | None:
        """Raise AuthFailed unless whatever listens on the API port (on connection, else a new one) proves it
        knows token, before the token (or a name key) is sent to it. Then check_daemon() with its version, which
        is returned (None if it didn't say) and kept in daemon_version.

        The proof is checked before anything else of the answer is believed: a listener that can't prove itself
        chooses neither the error (a 503 is Busy, as a daemon full of connections answers before reading the
        request; every other status AuthFailed) nor its text. A proved daemon's error status raises its ApiError."""
        challenge = secrets.token_hex(16)
        headers = {"Host": f"127.0.0.1:{self.port}", CHALLENGE_HEADER: challenge, "Connection": "keep-alive"}
        if self.app is not None:
            headers[APP_HEADER] = self.app  # the proof is then keyed by the app's token (its verifier)
        connection = connection or self._connect(self._proof_timeout(self.effective_timeout()))
        response = self._exchange(connection, "GET", "/api/status", None, headers)
        proof = (response.getheader(PROOF_HEADER) or "").strip().encode("ascii", "replace")
        key = app_proof_key(token) if self.app is not None else token
        if not hmac.compare_digest(proof, api_proof(key, self.port, challenge).encode()):
            response.close()
            connection.close()
            if response.status == 503:
                raise Busy(f"the daemon on API port {self.port} is too busy (or has too many connections); try "
                           f"again shortly", 503)
            if self.app is not None:
                where = f"in {self.token_file}" if self._given_token is None else "given"
                raise AuthFailed(f"what listens on API port {self.port} couldn't prove it knows app {self.app}'s "
                                 f"token ({where}), so nothing was sent to it: it may be another program, a daemon "
                                 f"using another --dir or one before rnfs 0.2.0, or there is no app {self.app} "
                                 f"there (rnfs app ls), or the token isn't its")
            raise AuthFailed(f"what listens on API port {self.port} couldn't prove it knows the API token in "
                             f"{self.token_file}, so nothing was sent to it: it may be another program, or a "
                             f"daemon using another --dir (or one started before an upgrade: restart it)")
        try:
            data = response.read(PROOF_READ_LIMIT)
            # Anything past PROOF_READ_LIMIT: no daemon's status, and the connection is unusable for the request
            complete = response.isclosed() or not response.read(1)
        except (http.client.HTTPException, OSError):  # IncompleteRead, a timeout, a reset
            complete = False
        if not complete:
            response.close()
            connection.close()
            raise ConnectionFailed("the daemon sent an invalid or incomplete response; see its log")
        try:
            self._raise_for(response, data)
        except ApiError:
            connection.close()
            raise
        try:
            version = json.loads(data).get("version")
        except (ValueError, AttributeError):
            version = None
        version = version if isinstance(version, str) else None
        self.daemon_version = version
        try:
            self.check_daemon(version)
        except ApiError:
            connection.close()
            raise
        return version

    def check_daemon(self, version: str | None):
        """The one place to refuse a daemon by its version (None if it didn't say), before the token is sent;
        raise an ApiError to refuse. A daemon proved older than OLDEST_DAEMON is refused (one that says no version,
        or one that doesn't parse, is a newer one's business: a daemon that closes the connection is refused after
        anyway)."""
        numbers = version_numbers(version)
        if numbers is not None and numbers < OLDEST_DAEMON:
            raise ApiError(f"the daemon on API port {self.port} is rnfs {version}, too old for this client (it needs "
                           f"{'.'.join(map(str, OLDEST_DAEMON))} or later); upgrade it, or use rnfs {version}'s own "
                           f"command line")

    def call(self, method: str, path: str, params: dict | None = None, json_body: dict | None = None,
             body=None, length: int | None = None, authenticated: bool = True, timeout=DEFAULT):
        """Send a request and return its decoded JSON response (timeout as request's)."""
        response = self.request(method, path, params, json_body, body, length, authenticated=authenticated,
                                timeout=timeout)
        try:
            return json.loads(read_all(response))
        except ValueError:
            raise ConnectionFailed("the daemon sent an invalid response") from None

    def get(self, path: str, /, *, timeout=DEFAULT, **params):
        """GET path with query parameters; JSON response (timeout as request's: no endpoint takes a parameter
        called timeout)."""
        return self.call("GET", path, params=params, timeout=timeout)

    def post(self, path: str, /, *, timeout=DEFAULT, **fields):
        """POST path with a JSON body; JSON response (timeout as request's: no endpoint takes a field called
        timeout)."""
        return self.call("POST", path, json_body=fields, timeout=timeout)

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

    def capabilities(self) -> dict:
        """What the daemon offers: {"version", "capabilities", "routes", "features", "event_kinds", "error_codes",
        "deprecated"};
        from a daemon before 0.2.0, which has no such endpoint, {} (so every capability is absent)."""
        try:
            return self.get("/api/capabilities")
        except NoSuchEndpoint:
            return {}

    def events(self, after: int | None = None, kinds: list[str] | None = None, wait: int = 0,
               epoch: str | None = None) -> dict:
        """The daemon's events after sequence after (of kinds, if given), as soon as there is one or after wait
        seconds (the API caps it): {"events", "next", "lost", "epoch"}. after None answers at once with the
        "next" to start from; pass the epoch given, so a restarted daemon answers "lost". NoSuchEndpoint from a
        daemon before 0.2.0."""
        return self.get("/api/events", after=after, kinds=",".join(kinds) if kinds else None, wait=wait or None,
                        epoch=epoch)

    def shutdown(self, wait: float | None = None) -> bool:
        """Ask the daemon to stop (as SIGTERM does). With wait, poll up to that many seconds (at least once) until
        it has stopped: its API no longer answers and, when its daemon.json in this client's rnfs dir names this
        port, its process has ended (it saves its state and releases the rnfs dir's lock after its API stops, so
        a daemon started next can take it); True once it has, False if not within wait. Without wait, True once it
        agreed. Unsupported from a daemon that can't be stopped this way, NoSuchEndpoint from one before 0.2.0."""
        running = self.daemon_info()
        pid = running["pid"] if running is not None and running["api_port"] == self.port else None
        self.post("/api/shutdown")
        if wait is None:
            return True
        deadline = time.monotonic() + wait
        while True:
            remaining = deadline - time.monotonic()
            if self._stopped(pid, max(0.1, remaining)):
                return True
            if remaining <= 0:
                return False
            time.sleep(min(0.2, remaining))

    def _stopped(self, pid: int | None, timeout: float) -> bool:
        """True if the daemon's API no longer answers (within timeout: whatever holds the port then mustn't hold
        the caller) and its process pid, if known, has ended."""
        try:
            self.call("GET", "/api/status", authenticated=False, timeout=timeout)
            return False
        except DaemonNotRunning:
            pass
        except ConnectionFailed:
            pass  # dropped, or a listener that doesn't answer: not the daemon's API either way
        except ApiError:
            return False  # something answered
        return pid is None or not _process_alive(pid)

    def daemon_info(self) -> dict | None:
        """What the daemon wrote about itself (read_daemon_info) in this client's rnfs dir, or None."""
        return read_daemon_info(self.rnfs_dir)

    # Apps (the CLI's token only: an app's is refused, AuthFailed)

    def apps(self) -> list[dict]:
        """Every app the daemon issued a token to: [{"name", "created"}], by name."""
        return self.get("/api/apps")["apps"]

    def app_add(self, name: str) -> dict:
        """Issue a token to a new app called name: {"name", "created", "token"}, the only time the token is given.
        BadRequest for an invalid name, one taken, or lxmf while the daemon's own LXMF is on."""
        return self.post("/api/apps/add", name=name)

    def app_rm(self, name: str) -> dict:
        """Revoke app name's token and release every pin it made: {"released"}. NotFound for an app there isn't."""
        return self.post("/api/apps/rm", name=name)

    # Content

    def add_path(self, path: str, wrap: bool = False, only_hash: bool = False, pin: bool = True,
                 to: list[str] | None = None, to_self: bool = False, seal_name: bool = True,
                 chunker: str | None = None) -> dict:
        """Add a file or directory on this machine: {"cid", "size", "warnings"} (+ "sealed"). pin=False keeps it
        as evictable cache; to/to_self seal it for those recipients; chunker is "fixed" or "fastcdc"."""
        return self._post("/api/add", path=os.path.abspath(os.path.expanduser(path)), wrap=wrap or None,
                          only_hash=only_hash or None, pin=None if pin else False, to=to or None,
                          to_self=to_self or None, seal_name=None if seal_name else False, chunker=chunker)

    def add_nomadnet(self, link: str, wrap: bool = False, only_hash: bool = False, pin: bool = True,
                     to: list[str] | None = None, to_self: bool = False, seal_name: bool = True,
                     chunker: str | None = None, name: str | None = None, identify: bool = False) -> dict:
        """Add a snapshot of what a NomadNet node serves at link (<destination>:/page/<path> or :/file/<path>), as
        add_path adds a file: {"cid", "size", "warnings", "name", "link"} (+ "sealed"). name wraps it in a directory
        as that entry (with wrap, under the node's name for it); identify identifies with the daemon's NomadNet
        identity. A daemon without the "add_nomadnet" capability (before 0.2.0) answers BadRequest (no path)."""
        return self._post("/api/add", nomadnet=link, wrap=wrap or None, only_hash=only_hash or None,
                          pin=None if pin else False, to=to or None, to_self=to_self or None,
                          seal_name=None if seal_name else False, chunker=chunker, name=name,
                          identify=identify or None)

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

    def add_tar(self, data, name: str | None = None, length: int | None = None, only_hash: bool = False,
                pin: bool = True, chunker: str | None = None) -> dict:
        """Add the directory a tar holds (bytes, or a binary file read to its end, sent chunked unless length is
        given): regular files and directories, the rest skipped with a warning; name wraps it in a directory as that
        entry. {"cid", "size", "warnings"} (+ "warnings_total"). For a program whose files the daemon can't open."""
        if isinstance(data, (bytes, bytearray, memoryview)) and length is None:
            length = len(data)
        params = {"format": "tar", "name": name, "only_hash": _flag(only_hash), "pin": None if pin else 0,
                  "chunker": chunker}
        return self.call("POST", "/api/add/stream", params=params, body=data, length=length)

    def get_tar(self, target: str) -> Stream:
        """A target (file or directory) as a tar, read as it arrives: the daemon fetches all of it first and sends its
        exact length, so a transfer cut short raises ConnectionFailed. Files are as stored (a sealed file sealed)."""
        return Stream(self.request("GET", "/api/get/stream", params={"target": target, "format": "tar"}))

    def export_stream(self, target: str) -> Stream:
        """A target's archive, as export writes it, read as it arrives (fetched first, with its exact length)."""
        return Stream(self.request("GET", "/api/export/stream", params={"target": target}))

    def import_stream(self, data, length: int | None = None) -> dict:
        """Import an archive sent as bytes or a binary file read to its end (chunked unless length is given): as
        import_archive, {"roots", "incomplete", ...}."""
        if isinstance(data, (bytes, bytearray, memoryview)) and length is None:
            length = len(data)
        return self.call("POST", "/api/import/stream", body=data, length=length)

    def fetch(self, target: str, output: str, from_node: str | None = None, raw: bool = False,
              no_extract: bool = False) -> dict:
        """Fetch a file or directory and have the daemon write it to output (a path on this machine, which must
        not exist), in one request: {"cid", "warnings"} (+ "sealed", "name", "extracted"). A sealed folder is
        unpacked unless no_extract (sent only when set: a daemon older than 0.2.0 ignores it, and never unpacks).
        For progress, use submit_job."""
        return self._post("/api/get", target=target, output=os.path.abspath(os.path.expanduser(output)),
                          raw=raw or None, no_extract=no_extract or None, **{"from": from_node})

    def cat(self, target: str, offset: int | None = None, length: int | None = None, raw: bool = False,
            block: bool = False, from_node: str | None = None) -> Stream:
        """A file's bytes (or a range of them) as they arrive. raw: a sealed file as stored; block: the CID's own
        block, verified; from_node a node to fetch from (a daemon that predates it ignores it)."""
        return Stream(self.request("GET", "/api/cat", params={"target": target, "offset": offset, "length": length,
                                                             "raw": _flag(raw), "block": _flag(block),
                                                             "from": from_node}))

    def info(self, target: str, from_node: str | None = None) -> dict:
        """{"cid", "type", "size", "executable"} (+ sealed-file fields) of a target."""
        return self.get("/api/info", target=target, **{"from": from_node})

    def ls(self, target: str) -> list[dict]:
        """A directory's entries: [{"name", "kind", "size", "cid"}]."""
        return self.get("/api/ls", target=target)["entries"]

    def stat(self, target: str) -> dict:
        """{"cid", "size", "blocks", "local_blocks", "local_bytes"} of a target."""
        return self.get("/api/stat", target=target)

    def dir_set(self, target: str, name: str, cid: str, executable: bool = False) -> dict:
        """A new directory: the directory target with entry name set to cid (a file, executable if asked, or a
        directory), pinned user: {"cid"}."""
        return self.post("/api/dir/set", target=target, name=name, cid=cid, executable=executable)

    def dir_rm(self, target: str) -> dict:
        """A new directory without the entry target (<cid>/<path>) names, pinned user: {"cid"}."""
        return self.post("/api/dir/rm", target=target)

    def export(self, target: str, output: str) -> dict:
        """Write a target's DAG as an archive to output (a path on this machine, which must not exist):
        {"roots", "blocks", ...}."""
        return self.post("/api/export", target=target, output=os.path.abspath(os.path.expanduser(output)))

    def import_archive(self, path: str) -> dict:
        """Import an archive on this machine: {"roots", "incomplete", ...}."""
        return self.post("/api/import", path=os.path.abspath(os.path.expanduser(path)))

    def gc(self) -> dict:
        """Remove blocks no pin needs: {"removed_blocks", "freed_bytes"}."""
        return self.post("/api/gc")

    def verify(self) -> dict:
        """Check every stored block against its CID: {"checked", "bad", ...}."""
        return self.post("/api/verify")

    # Pins

    def pin_add(self, target: str) -> dict:
        """Fetch and pin a target, in one request: {"cid"} once complete. For progress, use submit_job."""
        return self.post("/api/pin/add", target=target)

    def pin_rm(self, cid: str):
        """Remove the user pin of cid, or an app's own (app:<app>, or its pin_key's): NotFound if it has none."""
        self.post("/api/pin/rm", cid=cid)

    def pin_ls(self, cid: str | None = None) -> list[dict]:
        """Every pin (or cid's): [{"cid", "sources", "complete", "type", "sealed", "name", and from a daemon since
        0.2.0 "label", "origin", "size", "entries", "added"}]."""
        return self.get("/api/pin/ls", cid=cid)["pins"]

    def pin_label(self, cid: str, label: str | None) -> dict:
        """Label a pin (None or "" clears its label): its pin_ls entry. NotFound if cid isn't pinned."""
        return self.post("/api/pin/label", cid=cid, label=label)

    # The cache

    def cache_ls(self) -> list[dict]:
        """The cached fetches (downloads not pinned), most recently used first: [{"cid", "root", "path", "pinned",
        "complete", "type", "size", "provided"}]."""
        return self.get("/api/cache/ls")["cache"]

    def cache_rm(self, cid: str) -> dict:
        """Evict the cached fetches leading to cid now: {"removed_records", "removed_blocks", "freed_bytes"}.
        NotFound if none does."""
        return self.post("/api/cache/rm", cid=cid)

    def pin_remote(self, node: str, cid: str, pin: bool = True) -> bool:
        """Ask node (hex) to pin cid for this node, or (pin=False) to release that pin; True if it accepted."""
        return self.post("/api/pin/remote", node=node, cid=cid, pin=pin)["accepted"]

    def pin_remote_ls(self, node: str | None = None, cid: str | None = None, check: bool = False) -> list[dict]:
        """The remote pins this node asked for (node's, cid's, or every one), most recently asked first: [{"node",
        "cid", "pin", "accepted", "asked", "state", "done", "checked", "seen"}]; check: check them first (provider
        lookups, paced). NoSuchEndpoint from a daemon before 0.2.0."""
        try:
            return self.get("/api/pin/remote", node=node, cid=cid, check=_flag(check))["pins"]
        except ApiError as e:
            if e.status == 405:  # a daemon before 0.2.0 has only POST /api/pin/remote
                raise NoSuchEndpoint("this daemon doesn't list remote pins (rnfs 0.2.0 does)", 404,
                                     "no_such_endpoint") from None
            raise

    def pin_remote_served(self) -> dict:
        """The remote pins other identities asked of this node: {"pins": [{"identity", "cid", "state", "reason",
        "at", "detail"}], "usage": [{"identity", "used", "quota", "pinned", "waiting", "allowed"}], "quota"}."""
        return self.get("/api/pin/remote/served")

    # The network

    def providers(self, cid: str) -> list[str]:
        """The nodes (hex) providing cid: the records held, then what a provider lookup finds."""
        return self.get("/api/providers", cid=cid)["providers"]

    def ping(self, node: str) -> float | None:
        """The round-trip time in seconds of a new link to node (hex), or None if it is unreachable."""
        return self.get("/api/ping", node=node)["rtt"]

    # Names

    def name_list(self) -> list[dict]:
        """This node's names: [{"alias", "name", "cid", "sequence"}]."""
        return self.get("/api/name/list")["names"]

    def name_create(self, alias: str) -> dict:
        """Create a name key: {"alias", "name"}."""
        return self.post("/api/name/create", alias=alias)

    def name_import(self, alias: str, key: bytes) -> dict:
        """Import a name key exported elsewhere (its bytes) as alias: {"alias", "name"}."""
        body = bytes(key)
        return self.call("POST", "/api/name/import", params={"alias": alias}, body=body, length=len(body))

    def name_export(self, alias: str) -> bytes:
        """A name's key, to import elsewhere: whoever has it can publish the name."""
        return read_all(self.request("POST", "/api/name/export", json_body={"alias": alias}))

    def name_rm(self, alias: str):
        """Delete a name's key (NotFound if there is no such alias)."""
        self.post("/api/name/rm", alias=alias)

    def name_publish(self, alias: str, target: str) -> dict:
        """Point a name of this node at a target: {"name", "cid", "sequence"}, and "warning" in private mode (the
        name's announce is visible outside the private network)."""
        return self.post("/api/name/publish", alias=alias, target=target)

    def name_sync(self, alias: str, path: str) -> dict:
        """Keep a name of this node pointing at a folder's current content (path: absolute, on the daemon's
        machine): {"alias", "path", "state", "reason", "root", "published", "published_at", "next_publish"}, and
        "warning" in private mode. NotFound for no such name or folder."""
        return self.post("/api/name/sync", alias=alias, path=path)

    def name_unsync(self, alias: str):
        """Stop syncing a name to its folder (NotFound if it isn't synced); it keeps pointing where it last did."""
        self.post("/api/name/unsync", alias=alias)

    def name_syncs(self) -> list[dict]:
        """Every name synced to a folder, as name_sync answers, by alias."""
        return self.get("/api/name/syncs")["syncs"]

    def name_resolve(self, name: str) -> dict:
        """{"name", "cid", "sequence", "age"} of a name (32 hex characters, or a name contact's nick); NotFound if
        it doesn't resolve."""
        return self.get("/api/name/resolve", name=name)

    def name_watch(self, names: list[str], after: list[int | None], wait: int = 0) -> list[dict]:
        """The records held for names (hex) whose sequence is above the matching after (None: any), once there is
        one or after wait seconds (the API caps it): [{"name", "cid", "sequence", "age"}]. NotFound from a daemon
        that can't watch names."""
        return self.get("/api/name/watch", names=",".join(names),
                        after=",".join("-" if sequence is None else str(sequence) for sequence in after),
                        wait=wait or None)["records"]

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

    # Settings (rnfs.toml; the schema alone is rnfs.config.settings_schema(), without a daemon)

    def config(self) -> dict:
        """The daemon's settings: {"path" (its rnfs.toml), "settings": [each setting's schema entry with "value"
        (running), "file" (rnfs.toml's, None if it doesn't set it), "overridden" (by the daemon's command line,
        which wins), "pending" (a restart would change it)], "file_error" (why a restart would refuse rnfs.toml,
        or None), "warnings"}. NoSuchEndpoint from a daemon before 0.2.0."""
        return self.get("/api/config")

    def config_set(self, settings: dict) -> dict:
        """Set settings ({name: value, or None to remove it from rnfs.toml}) in the daemon's rnfs.toml, applying at
        once those that can be: {"path", "applied", "restart_required", "overridden", "warnings"}. BadRequest,
        writing nothing, unless the whole result is valid."""
        return self.post("/api/config/set", settings=dict(settings))

    # Jobs (background transfers, with progress)

    def submit_job(self, kind: str, target: str | None = None, output: str | None = None,
                   from_node: str | None = None, path: str | None = None, options: dict | None = None,
                   **flags) -> dict:
        """Start a job: kind "get" (output needed), "pin" or "export" (output needed) of a target, or "add" or
        "import" of a path on this machine. Its record, with its "id". flags: get's sealed_name, fallback_dht and
        no_extract; options: an add's, as add_path takes them in the API's form (wrap, only_hash, pin, to, to_self,
        seal_name, chunker; only those set)."""
        output = os.path.abspath(os.path.expanduser(output)) if output is not None else None
        path = os.path.abspath(os.path.expanduser(path)) if path is not None else None
        return self._post("/api/jobs", kind=kind, target=target, output=output, path=path, **{"from": from_node},
                          **(options or {}), **flags)

    def job(self, job_id: str, wait: int = 0) -> dict:
        """A job's record; waits up to wait seconds (the API caps it) for it to finish."""
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

    # The features' routes rnfs.Client uses (rnfs/features/: NotFound from a daemon without the feature). A name
    # is 32 hex characters or a name contact's nick; an LXMF recipient an address (hex) or an LXMF contact's nick

    def peers(self) -> list[dict]:
        """Every node the daemon knows, nearest first: [{"node_id", "mode", "in_table", "seniority", "senior",
        "hops", "interface", "bitrate", "link", "rate", "last_heard", "last_active", "banned", "sent",
        "received", "requests", "answered", "refused", "failed", "bad", ...}] (peers feature)."""
        return self.get("/api/peers")["peers"]

    def stats(self) -> dict:
        """Traffic since the daemon started: {"since", "totals", "refused", "interfaces", "upload_budget",
        "upload_budget_left"} (peers feature)."""
        return self.get("/api/stats")

    def contacts(self) -> list[dict]:
        """Every contact, by nick: [{"nick", "kind", "id"}]."""
        return self.get("/api/contacts")["contacts"]

    def contact_get(self, nick: str) -> dict:
        """One contact: {"nick", "kind", "id"}; NotFound if there is none."""
        return self.get("/api/contacts/get", nick=nick)

    def contact_add(self, nick: str, ident: str, kind: str | None = None) -> dict:
        """Add or replace a contact for ident (32 hex characters): kind "name", "node" or "lxmf", or None to have
        the daemon tell (ValueError if it can't). The contact."""
        return self._post("/api/contacts/add", nick=nick, id=ident, kind=kind)

    def contact_rm(self, nick: str):
        """Delete a contact (NotFound if there is none)."""
        self.post("/api/contacts/rm", nick=nick)

    def name_keep(self, name: str) -> dict:
        """Keep a name's signed record: {"name", "reasons", "sequence", "cid", "emitted"} (keepers feature)."""
        return self.post("/api/name/keep", name=name)

    def name_unkeep(self, name: str):
        """Stop keeping a name (NotFound if it isn't kept by the user)."""
        self.post("/api/name/unkeep", name=name)

    def name_kept(self) -> list[dict]:
        """Every kept name: [{"name", "reasons", "sequence", "cid", "emitted"}]."""
        return self.get("/api/name/kept")["kept"]

    def name_follow(self, name: str) -> dict:
        """Follow a name, pinning its latest target: {"name", "pinned", "latest", "sequence", "state"} (follow
        feature)."""
        return self.post("/api/name/follow", name=name)

    def name_unfollow(self, name: str):
        """Stop following a name, releasing its pin (NotFound if it isn't followed)."""
        self.post("/api/name/unfollow", name=name)

    def name_following(self) -> list[dict]:
        """Every followed name: [{"name", "pinned", "latest", "sequence", "state"}]."""
        return self.get("/api/name/following")["following"]

    def lxmf_address(self) -> dict:
        """{"address", "display_name", "propagation_node", "nearest_propagation_node"}; BadRequest if LXMF is off."""
        return self.get("/api/lxmf/address")

    def lxmf_send(self, to: str, target: str, message: str | None = None, propagate: bool = False,
                  nearest: bool = False) -> dict:
        """Send a link to target (and message) over LXMF, queued: {"id", "hash", "to", "method"}."""
        return self._post("/api/lxmf/send", to=to, target=target, message=message, propagate=propagate or None,
                          nearest=nearest or None)

    def lxmf_inbox(self) -> list[dict]:
        """The inbox, oldest first: [{"id", "hash", "from", "display_name", "sent", "received", "title", "text",
        "method", "links", "auto", "trusted", "contact"}]."""
        return self.get("/api/lxmf/inbox")["messages"]

    def lxmf_inbox_get(self, entry_id: int, link: int | None = None, output: str | None = None,
                       directory: str | None = None, use_sent_cid: bool = False) -> list[dict]:
        """Fetch inbox message entry_id's links (or link, 1-based) to output or into directory (paths on this
        machine): [{"target", "output", "job" | "cid", "warnings"?, "error"?}]."""
        output = os.path.abspath(os.path.expanduser(output)) if output is not None else None
        directory = os.path.abspath(os.path.expanduser(directory)) if directory is not None else None
        return self._post("/api/lxmf/inbox/get", id=entry_id, link=link, output=output, directory=directory,
                          use_sent_cid=use_sent_cid or None)["results"]

    def lxmf_inbox_rm(self, entry_id: int | None = None, every: bool = False) -> int:
        """Remove inbox message entry_id, or every one (every); how many (NotFound for an id there isn't)."""
        return self.post("/api/lxmf/inbox/rm", **({"all": True} if every else {"id": entry_id}))["removed"]

    def lxmf_outbox(self) -> list[dict]:
        """The messages sent, oldest first: [{"id", "hash", "to", "time", "links", "text", "method", "state",
        "progress", ...}]."""
        return self.get("/api/lxmf/outbox")["messages"]

    def lxmf_sync(self, nearest: bool = False) -> dict:
        """Start downloading waiting messages from the propagation node: {"propagation_node", "started"}."""
        return self._post("/api/lxmf/sync", nearest=nearest or None)
