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
  daemon), but manage apps, which only the CLI's token may (rnfs.api).
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
