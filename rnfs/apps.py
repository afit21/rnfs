"""App tokens (specification section 11): the labelled tokens `rnfs app add <name>` issues to applications, so the
API can tell which app called and give the pins it makes the source app:<name> (storage.app_source). No RNS
dependency.

- An app is a name (storage.APP_NAME: lowercase alias characters, never ":") with a token, kept in the state
  database's apps table, so it outlives restarts (unlike api_token, made anew at each start). The token is
  random (256 bits) and handed out once, when the app is added; the table keeps only its verifier
  (apiclient.app_verifier: SHA-256 of a fixed label and the token), from which the token can't be found. The API
  checks a token presented as X-Rnfs-Api against it (verify, in constant time), and keys the proof it gives the
  app (the challenge, X-Rnfs-Proof) with it (proof_key): so the token itself never has to be stored. Whoever can
  read rnfs.db could answer an app's proof in the daemon's place while it is down, and so be sent that app's
  token; they can read the rnfs directory, api_token included, already.
- App names stay private (0.2.0 review, REV9): the proof needs no token, so proof_key keys a name there isn't
  with HMAC-SHA256 of a random secret made at each start and the name, a key no client has: the API's answer
  (status, headers, body, a proof of the same shape, the same each time within a start) is then the same whether
  or not the app exists, and both are made with the same work, so neither the answer nor its time says which
  names are installed.
- App tokens are not scoped: an app can do anything the CLI can (export name keys, change settings, stop the
  daemon), but manage apps through the /api/apps routes, which only the CLI's token may (rnfs.api). That is no
  real limit: whatever reads and writes files as the daemon's user can read api_token (specification section 11).
- remove forgets an app at once, so its token stops passing, then calls the release it is given (the node
  releases the app's pins), then deletes its row; if either fails the app is put back, to be removed again.
  Rows are written durably, one per change. A row that doesn't parse is logged and deleted at load.
"""
import hashlib
import hmac
import math
import secrets
import threading
import time
from typing import Callable

from rnfs import statedb
from rnfs.apiclient import app_verifier
from rnfs.content.storage import check_app_name
from rnfs.errors import NotFound
from rnfs.log import LOG_NOTICE, LOG_WARNING, log

TOKEN_BYTES = 32  # an app token's randomness
VERIFIER_LENGTH = 32  # bytes of apiclient.app_verifier's digest
SECRET_BYTES = 32  # the per-start secret keying the proof of an app there isn't (proof_key)


class AppRegistry:
    """The apps, by name: their token verifiers and when they were added. Thread-safe."""

    def __init__(self, state_dir: str):
        """Load the apps saved in state_dir; a row that doesn't parse is logged and deleted."""
        self._lock = threading.Lock()
        self._apps: dict[str, tuple[bytes, float]] = {}  # name -> (verifier, created)
        # Keys the proof for names there aren't (proof_key): new at each start, never stored or sent
        self._secret = secrets.token_bytes(SECRET_BYTES)
        self._db = statedb.open_store(state_dir, self._load)

    def _load(self, db: statedb.StateDb):
        """Read the apps table."""
        self._store = statedb.RowStore(db, "apps", ("name",), durable=True, lock=self._lock, row=self._row)
        bad = []
        for name, verifier, created in db.query("SELECT name, verifier, created FROM apps"):
            try:
                check_app_name(name)
                if not isinstance(verifier, bytes) or len(verifier) != VERIFIER_LENGTH:
                    raise ValueError("not a verifier")
                if isinstance(created, bool) or not isinstance(created, (int, float)) or not math.isfinite(created):
                    raise ValueError("not a time")
            except ValueError:
                log(f"apps: ignoring invalid record {name!r}", LOG_WARNING)
                bad.append((name,))
                continue
            self._apps[name] = (verifier, float(created))
        statedb.delete_rows(db, "apps", ("name",), bad)

    def _row(self, name: str) -> tuple | None:
        """name's apps row, or None once it is gone (lock held)."""
        app = self._apps.get(name)
        return None if app is None else (name, app[0], app[1])

    def add(self, name: str) -> dict:
        """Add app name with a new token: {"name", "created", "token"}, the only time the token is given. Raises
        ValueError for an invalid name or one taken, OSError (StateDbError) if it can't be saved (then it isn't
        added)."""
        check_app_name(name)
        token = secrets.token_urlsafe(TOKEN_BYTES)
        created = time.time()
        with self._lock:
            if name in self._apps:
                raise ValueError(f"there is an app called {name} already; remove it first (rnfs app rm {name})")
            self._apps[name] = (app_verifier(token), created)
            try:
                self._store.changed(name)
            except BaseException:
                # Not added: the key stays to write, as a delete now, with the next change
                del self._apps[name]
                raise
        log(f"apps: added {name}", LOG_NOTICE)
        return {"name": name, "created": created, "token": token}

    def list(self) -> list[dict]:
        """Every app, by name: [{"name", "created"}]."""
        with self._lock:
            return [{"name": name, "created": created} for name, (_, created) in sorted(self._apps.items())]

    def __contains__(self, name) -> bool:
        """Whether there is an app called name."""
        with self._lock:
            return name in self._apps

    def verify(self, name: str, token: str) -> bool:
        """Whether token is app name's (compared in constant time); False for an app there isn't."""
        with self._lock:
            app = self._apps.get(name)
        expected = app[0] if app is not None else bytes(VERIFIER_LENGTH)
        # Compared even for an app there isn't, so the time taken doesn't say which names exist
        return secrets.compare_digest(app_verifier(token), expected) and app is not None

    def proof_key(self, name: str) -> str:
        """What keys the proof the API gives app name (apiclient.api_proof's token): its verifier in hex, which the
        app computes from its token; for an app there isn't, HMAC-SHA256 of this start's secret and the name in hex,
        which no client can compute, so the answer doesn't say whether the app exists."""
        # Made for every name, and the same steps after, so the time taken doesn't say which names exist either
        stand_in = hmac.new(self._secret, name.encode("utf-8", "surrogatepass"), hashlib.sha256).digest()
        with self._lock:
            app = self._apps.get(name)
        return (stand_in if app is None else app[0]).hex()

    def remove(self, name: str, release: Callable[[], object] = lambda: None):
        """Remove app name: its token stops passing at once, then release() runs (the node releases its pins),
        then its row is deleted; what release returns is returned. Raises NotFound for an app there isn't; if
        release or the delete fails, the app is put back (its row was never deleted, or is written again) and the
        error raised, so it can be removed again."""
        with self._lock:
            app = self._apps.pop(name, None)
            if app is None:
                raise NotFound(f"no app called {name}")
        try:
            result = release()
            with self._lock:
                self._store.changed(name)
        except BaseException:
            # Its row is still there (a failed delete stays to write, as the row again now, with the next change)
            with self._lock:
                self._apps[name] = app
            raise
        log(f"apps: removed {name}", LOG_NOTICE)
        return result

    def save(self):
        """Write a change whose write failed, if any (each is written as it is made)."""
        self._store.save()

    def close(self):
        """Release the state database (the node calls this once it has saved everything at shutdown)."""
        self._db.close()


# Pairing (0.2.1): an app asks for a token and the user approves it, instead of copying one by hand
MAX_PENDING = 32  # pairing requests waiting at once; a new one beyond takes the place of the oldest (AUD-M5)
REQUEST_LIFE = 600.0  # seconds a request waits for the user's answer
PICKUP_LIFE = 300.0  # seconds an approved request's token waits for its app to collect it
MAX_DESCRIPTION = 200  # characters of what an app says of itself
PENDING, APPROVED, DENIED, EXPIRED = "pending", "approved", "denied", "expired"


class _Request:
    """One pairing request: the app's name and description, the secret only its requester holds, the code both
    sides show, and once answered its state and (approved) token."""

    __slots__ = ("id", "name", "description", "secret", "code", "requested", "state", "token", "until")

    def __init__(self, name: str, description: str | None, now: float):
        self.id = secrets.token_hex(8)
        self.name, self.description = name, description
        self.secret = secrets.token_urlsafe(TOKEN_BYTES)
        self.code = f"{secrets.randbelow(1_000_000):06d}"
        self.requested, self.until = now, now + REQUEST_LIFE
        self.state: str = PENDING
        self.token: str | None = None


class PairingRequests:
    """Apps asking for a token (POST /api/apps/request), for the user to approve or deny through the CLI's token
    (rnfs app requests, approve, deny), in memory only: a restart forgets them. Thread-safe.

    - request(name, description) holds a request for REQUEST_LIFE and gives its requester the request's id, a
      secret and a code. The code is shown both by the app and where the user approves (rnfs app requests, an app
      such as RNFS Desktop), so the user can tell the app's request from one another process made under its name.
    - approve(id, code) adds the app (AppRegistry.add) and keeps its token for the requester alone, for PICKUP_LIFE:
      the approver never sees it; the other requests waiting under its name are denied. code, which the CLI asks
      the user for, must be the one the request's app shows. state(id, secret) gives the requester the request's
      state and, once approved, the token, a single time (the request is then forgotten). A wrong secret is a
      request there isn't (NotFound).
    - Any local process can ask, under any name (0.2.1 audit, AUD-M5), so a name may have several requests waiting,
      each with its own code: asking first under an app's name doesn't keep the app from asking. At most
      MAX_PENDING wait at once, and a new one beyond takes the place of the oldest, so filling them once doesn't
      shut pairing out: that takes a flood faster than the user answers, which the user sees. A name that isn't
      valid is refused; one an app already has only when the user approves it, so the open route doesn't say which
      apps exist (AUD-L1, as REV9 for the proof). Requests from web pages can't be made: the route needs a custom
      header (rnfs.api).
    """

    def __init__(self, registry: AppRegistry, clock: Callable[[], float] = time.time):
        """Requests for apps of registry."""
        self.registry = registry
        self.clock = clock
        self._lock = threading.Lock()
        self._requests: dict[str, _Request] = {}

    def _expire(self, now: float):
        """Forget requests whose time is up (lock held)."""
        for request_id in [rid for rid, request in self._requests.items() if request.until <= now]:
            del self._requests[request_id]

    def request(self, name: str, description: str | None = None) -> dict:
        """Ask for a token for app name: {"id", "secret", "code", "expires"}. Raises ValueError (an invalid name or
        description). Never looks at the apps there are, so every valid name is answered alike (AUD-L1)."""
        check_app_name(name)
        if description is not None:
            if not isinstance(description, str) or len(description) > MAX_DESCRIPTION or not description.isprintable():
                raise ValueError(f"description is printable text of at most {MAX_DESCRIPTION} characters")
            description = description.strip() or None
        with self._lock:
            now = self.clock()
            self._expire(now)
            # In the order they came (a dict keeps it): the oldest make way
            waiting = [request for request in self._requests.values() if request.state == PENDING]
            dropped = waiting[:max(0, len(waiting) - MAX_PENDING + 1)]
            for old in dropped:
                del self._requests[old.id]
            request = _Request(name, description, now)
            self._requests[request.id] = request
        if dropped:
            log(f"apps: {MAX_PENDING} requests for a token were waiting; forgot the oldest, from "
                f"{', '.join(old.name for old in dropped)}", LOG_WARNING)
        log(f"apps: {name} asks for a token (code {request.code}): rnfs app approve {request.id}", LOG_NOTICE)
        return {"id": request.id, "secret": request.secret, "code": request.code, "expires": request.until}

    def pending(self) -> list[dict]:
        """The requests waiting for an answer, oldest first: [{"id", "name", "description", "code", "requested",
        "expires"}]."""
        with self._lock:
            self._expire(self.clock())
            waiting = [request for request in self._requests.values() if request.state == PENDING]
        return [{"id": request.id, "name": request.name, "description": request.description, "code": request.code,
                 "requested": request.requested, "expires": request.until}
                for request in sorted(waiting, key=lambda r: r.requested)]

    def _waiting(self, request_id: str) -> _Request:
        """The request waiting for an answer with this id (lock held). Raises NotFound."""
        self._expire(self.clock())
        request = self._requests.get(request_id) if isinstance(request_id, str) else None
        if request is None or request.state != PENDING:
            raise NotFound(f"no app is waiting for an answer as request {request_id}")
        return request

    def approve(self, request_id: str, code: str | None = None) -> dict:
        """Add the app request_id asks for, keeping its token for the requester, and deny the other requests waiting
        under its name: {"name", "created"}. code, if given, must be the request's: the one its app shows. Raises
        NotFound (no such request waiting), ValueError (another code; an app of that name already: a name clash,
        the request maybe another program's under that app's name). A request refused so stays waiting."""
        if code is not None and not isinstance(code, str):
            raise ValueError("code must be the 6 digits the app shows")
        with self._lock:
            request = self._waiting(request_id)
            if code is not None and not hmac.compare_digest(code.encode("utf-8", "replace"), request.code.encode()):
                raise ValueError(f"that isn't the code of request {request_id} (app {request.name}): approve the "
                                 f"request showing the code the app shows")
            if request.name in self.registry:
                raise ValueError(f"there is an app called {request.name} already: a name clash, and this request may "
                                 f"be another program's using its name. Deny it; to replace that app, remove it "
                                 f"first (rnfs app rm {request.name})")
            added = self.registry.add(request.name)
            request.state, request.token = APPROVED, added["token"]
            request.until = self.clock() + PICKUP_LIFE
            # Only one of them gets the name: the others asking under it are told no
            for other in self._requests.values():
                if other.name == request.name and other.state == PENDING:
                    other.state, other.until = DENIED, request.until
        return {"name": added["name"], "created": added["created"]}

    def deny(self, request_id: str) -> dict:
        """Refuse request_id: its requester learns so. {"name"}. Raises NotFound."""
        with self._lock:
            request = self._waiting(request_id)
            request.state, request.until = DENIED, self.clock() + PICKUP_LIFE
        log(f"apps: refused {request.name}'s request for a token", LOG_NOTICE)
        return {"name": request.name}

    def state(self, request_id: str, secret: str) -> dict:
        """What became of the request, for its requester: {"name", "state" (pending, approved, denied), "token"
        (approved: given this once, the request then forgotten), "expires"}. Raises NotFound for a request there
        isn't, one expired, or a wrong secret (the same answer, compared in constant time)."""
        with self._lock:
            self._expire(self.clock())
            request = self._requests.get(request_id) if isinstance(request_id, str) else None
            given = (secret or "").encode("utf-8", "replace")
            expected = request.secret.encode() if request is not None else b"\0" * len(given)
            if request is None or not hmac.compare_digest(given, expected):
                raise NotFound("no such pairing request (it expired, was collected, or the secret is wrong)")
            answer = {"name": request.name, "state": request.state, "expires": request.until}
            if request.state == APPROVED:
                answer["token"] = request.token
            if request.state != PENDING:
                del self._requests[request_id]  # answered: told once
        return answer
