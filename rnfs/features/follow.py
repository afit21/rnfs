"""Follow (feature "follow"): keep the latest target of a name pinned, like a subscription.

- `rnfs name follow <name>` keeps the name (a keeper, rnfs.features.keepers) and pins whatever it points at
  with source `follow:<name>`. When the name moves, the new target is fetched in the background and
  pinned; the old target's follow pin is released only once the new one is complete, so there is always a
  complete copy. `rnfs name unfollow <name>` releases the pin and stops keeping the name for following.
- Checks: when an announce or keeper answer brings a kept name a newer record (no network: the record held
  is used), and every announce_interval. That periodic check uses the record held unless nothing was heard
  for 2 x announce_interval (the owner announces every announce_interval); only then does it resolve
  through the network (path request, then keepers). A failed check is retried after RETRY_INTERVAL,
  doubling with each further failure up to announce_interval; a failed fetch is retried with the record
  held, without a path request.
- One background thread does every check and fetch, one at a time, so following many names never runs more
  than one transfer. Fetches obey the usual limits (max_download_size, disk reserve); a followed target is
  a pin, so it is provided.
- Followed names count against keep_max_names. State: <state dir>/follow.json (atomic).

No RNS at module level (the CLI imports this module).
"""
import os
import threading
import time

from rnfs.content.cid import Cid
from rnfs.errors import NotFound, RnfsError
from rnfs.features import Feature, _common
from rnfs.features._common import LOG_ERROR, LOG_NOTICE, LOG_VERBOSE, LOG_WARNING
from rnfs.features.keepers import FOLLOW, name_subcommands, parse_name_arg
from rnfs.fsutil import printable

NAME = "follow"
FOLLOW_FILE = "follow.json"
STATE_VERSION = 1
SOURCE_PREFIX = "follow:"
RETRY_INTERVAL = 300.0  # seconds before a failed check or fetch is first tried again; doubles per failure
FAR_FUTURE = float("inf")
STALE_ANNOUNCES = 2  # a record not heard for this many announce_intervals is refreshed from the network


def retry_delay(failures: int, announce_interval: float) -> float:
    """Seconds before the next check after failures failed checks in a row: RETRY_INTERVAL doubling, at most
    announce_interval (and never less than RETRY_INTERVAL, even with a shorter announce_interval)."""
    return max(RETRY_INTERVAL, min(RETRY_INTERVAL * 2 ** min(failures - 1, 32), announce_interval))


def follow_source(name: bytes) -> str:
    """The pin source for a followed name."""
    return SOURCE_PREFIX + name.hex()


def _log(message: str, level: int = LOG_VERBOSE):
    """Log with this feature's prefix."""
    _common.log(f"follow: {message}", level)


class FollowFeature(Feature):
    """Followed names and their pins. Thread-safe."""

    name = NAME

    def __init__(self, node):
        """Load followed names; no network."""
        super().__init__(node)
        self._path = os.path.join(node.config.state_dir, FOLLOW_FILE)
        self._lock = threading.Lock()
        self._pinned: dict[bytes, Cid | None] = {}  # followed name -> target pinned for it (complete)
        self._state: dict[bytes, str] = {}  # name -> what the last check did, for `following`
        self._due: dict[bytes, tuple[float, bool]] = {}  # name -> (monotonic time, use the network)
        self._failures: dict[bytes, int] = {}  # name -> checks failed in a row (for the retry backoff)
        self._resolved: set[bytes] = set()  # names whose last check resolved them (a failed fetch, then)
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._keepers = node.feature("keepers")
        self._keepers.add_listener(self._changed)
        self._load()

    # State

    def _load(self):
        """Read follow.json, skipping malformed entries."""
        from rnfs.content.storage import read_json
        state = read_json(self._path)
        entries = state.get("names") if isinstance(state, dict) else None
        if not isinstance(entries, dict):
            return
        for name_hex, entry in entries.items():
            try:
                name = bytes.fromhex(name_hex)
                pinned = Cid.parse(entry["pinned"]) if entry.get("pinned") is not None else None
            except (AttributeError, KeyError, TypeError, ValueError):
                continue
            if len(name) == 16:
                self._pinned[name] = pinned

    def _save(self):
        """Write follow.json atomically (lock held)."""
        from rnfs.content.storage import write_json
        write_json(self._path, {"version": STATE_VERSION,
                                "names": {name.hex(): {"pinned": str(cid) if cid is not None else None}
                                          for name, cid in sorted(self._pinned.items())}})

    # Lifecycle

    def start(self):
        """Check every followed name once interfaces had time to come up, then keep checking."""
        first = time.monotonic() + self.node.config.startup_announce_delay
        with self._lock:
            for name in self._pinned:
                self._due[name] = (first, True)
        self._thread = threading.Thread(target=self._loop, name="rnfs-follow", daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the background thread (a fetch in progress stops at its next network wait)."""
        self._stop.set()
        self._wake.set()

    def status(self) -> dict:
        """Names followed."""
        with self._lock:
            return {"followed_names": len(self._pinned)}

    # Following

    def follow(self, name: bytes) -> dict:
        """Follow name: keep it and pin its latest target in the background. ValueError if keepers are full."""
        self._keepers.keep(name, FOLLOW)
        with self._lock:
            if name not in self._pinned:
                self._pinned[name] = None
                self._save()
                _log(f"following name {name.hex()}", LOG_NOTICE)
            self._failures.pop(name, None)  # asked again by the user: start the backoff afresh
            self._due[name] = (time.monotonic(), True)
        self._wake.set()
        return self._entry(name)

    def unfollow(self, name: bytes):
        """Stop following name, releasing its pin. NotFound if it isn't followed."""
        with self._lock:
            if name not in self._pinned:
                raise NotFound(f"name {name.hex()} is not followed")
            pinned = self._pinned.pop(name)
            self._due.pop(name, None)
            self._state.pop(name, None)
            self._failures.pop(name, None)
            self._resolved.discard(name)
            self._save()
        if pinned is not None:
            self.node.storage.unpin(pinned, follow_source(name))
        try:
            self._keepers.unkeep(name, FOLLOW)
        except NotFound:
            self.node.publisher.refresh()
        _log(f"no longer following name {name.hex()}", LOG_NOTICE)

    def following(self) -> list[dict]:
        """Every followed name: the target pinned for it and what the last check did."""
        with self._lock:
            names = sorted(self._pinned)
        return [self._entry(name) for name in names]

    def _entry(self, name: bytes) -> dict:
        """A followed name as the API shows it."""
        record = self.node.names.record(name)
        with self._lock:
            pinned = self._pinned.get(name)
            state = self._state.get(name, "waiting")
        return {"name": name.hex(), "pinned": str(pinned) if pinned is not None else None,
                "latest": str(record.cid) if record is not None else None,
                "sequence": record.sequence if record is not None else None, "state": state}

    def _changed(self, name: bytes):
        """Keepers listener: a kept name may have a new record; check it soon without the network."""
        with self._lock:
            if name not in self._pinned:
                return
            when, network = self._due.get(name, (FAR_FUTURE, False))
            self._due[name] = (time.monotonic(), network)
        self._wake.set()

    # Checking and fetching

    def check(self, name: bytes, network: bool = True) -> bool:
        """Bring name's follow pin up to date with its latest target; True if it is (or nothing to do).

        network=True resolves through the network (a flooded path request, maybe a DHT lookup) only if the
        record held wasn't heard within STALE_ANNOUNCES x announce_interval; network=False uses the record held
        whatever its age (the network only when none is held).
        """
        with self._lock:
            if name not in self._pinned:
                return True
            pinned = self._pinned[name]
            self._resolved.discard(name)
        try:
            max_age = STALE_ANNOUNCES * self.node.config.announce_interval if network else FAR_FUTURE
            result = self.node.names.resolve(name, persist=True, max_age=max_age)
        except Exception as e:  # noqa: BLE001 - resolving is retried later
            self._set_state(name, f"resolve failed: {e}")
            return False
        if result is None:
            self._set_state(name, "unresolved")
            return False
        with self._lock:
            self._resolved.add(name)
        target = Cid.parse(result["cid"])
        if target == pinned:
            self._set_state(name, "current")
            return True
        self._set_state(name, f"fetching {target}")
        source = follow_source(name)
        try:
            with self.node.fetcher.open(target, pinned=True) as fetch:
                fetch.fetch_dag(fetch.resolve())
                with self._lock:
                    followed = name in self._pinned
                    if followed:
                        # Pinned inside the fetch's protection, so the new target is never unprotected
                        self.node.storage.pin(target, source)
                        previous = self._pinned[name]
                        self._pinned[name] = target
                        try:
                            self._save()
                        except OSError as e:  # the pins are right; follow.json catches up with the next save
                            _log(f"saving {FOLLOW_FILE} failed: {e}", LOG_ERROR)
        except RnfsError as e:
            self._set_state(name, f"fetch of {target} failed: {e}")
            _log(f"fetching {target} for name {name.hex()} failed: {e}", LOG_WARNING)
            return False
        if not followed:
            return True  # unfollowed while fetching: nothing was pinned
        # The old target is released only now that the new one is complete
        if previous is not None and previous != target:
            self.node.storage.unpin(previous, source)
        self.node.publisher.refresh()
        self._set_state(name, "current")
        _log(f"name {name.hex()} now pinned at {target}", LOG_NOTICE)
        return True

    def _set_state(self, name: bytes, state: str):
        """Record what the last check of name did."""
        with self._lock:
            if name in self._pinned:
                self._state[name] = state

    def run_due(self, now: float | None = None) -> float:
        """Check every name that is due; seconds until the next is due."""
        interval = self.node.config.announce_interval
        while not self._stop.is_set():
            now = time.monotonic() if now is None else now
            with self._lock:
                due = [(name, network) for name, (when, network) in self._due.items() if when <= now]
                if not due:
                    upcoming = min((when for when, _ in self._due.values()), default=now + interval)
                    return max(0.0, upcoming - now)
                name, network = min(due, key=lambda item: self._due[item[0]][0])
                self._due[name] = (FAR_FUTURE, False)
            try:
                ok = self.check(name, network)
            except Exception as e:  # noqa: BLE001 - e.g. disk full saving: a failed check, tried again later
                _log(f"checking name {name.hex()} failed: {e!r}", LOG_ERROR)
                self._set_state(name, f"check failed: {e}")
                ok = False
            with self._lock:
                if name in self._pinned and self._due.get(name, (FAR_FUTURE,))[0] == FAR_FUTURE:
                    if ok:
                        self._failures.pop(name, None)
                        self._due[name] = (time.monotonic() + interval, True)
                    else:
                        # Backing off: an owner gone for good mustn't cost a flooded path request every few
                        # minutes forever. A failed fetch already has its record: retry it without a path request
                        failures = self._failures.get(name, 0) + 1
                        self._failures[name] = failures
                        self._due[name] = (time.monotonic() + retry_delay(failures, interval),
                                           name not in self._resolved)
            now = None
        return 0.0

    def _loop(self):
        """The background thread: checks and fetches, one at a time."""
        while not self._stop.is_set():
            try:
                delay = self.run_due()
            except Exception as e:  # noqa: BLE001 - keep following
                _log(f"checking followed names failed: {e!r}", LOG_ERROR)
                delay = RETRY_INTERVAL
            self._wake.wait(min(delay, 60.0))
            self._wake.clear()

    # API and CLI

    @classmethod
    def api_routes(cls) -> dict:
        """POST /api/name/follow, /api/name/unfollow; GET /api/name/following."""
        return {("POST", "/api/name/follow"): _api_follow, ("POST", "/api/name/unfollow"): _api_unfollow,
                ("GET", "/api/name/following"): _api_following}

    @classmethod
    def cli_commands(cls, commands, command):
        """`rnfs name follow|unfollow|following`."""
        name = name_subcommands(commands)
        command(name, "follow", run_follow, "keep a name's latest target pinned (and keep the name)"
                ).add_argument("name", help="the name (32 hex characters) or a name contact's nick")
        command(name, "unfollow", run_unfollow, "stop following a name, releasing its pin").add_argument(
            "name", help="the name (32 hex characters) or a name contact's nick")
        command(name, "following", run_following, "list followed names: name, pinned CID, latest CID, state")


FEATURE = FollowFeature


def _feature(handler) -> FollowFeature:
    """The node's follow feature. NotFound if the daemon has none."""
    return _common.feature(handler, NAME, "this daemon can't follow names")


def _api_follow(handler):
    """POST /api/name/follow {"name"}: the followed name."""
    feature = _feature(handler)
    handler.send_json(200, feature.follow(parse_name_arg(handler.node, handler.json_body().get("name"))))


def _api_unfollow(handler):
    """POST /api/name/unfollow {"name"}: {}."""
    feature = _feature(handler)
    feature.unfollow(parse_name_arg(handler.node, handler.json_body().get("name")))
    handler.send_json(200, {})


def _api_following(handler):
    """GET /api/name/following: {"following": [{"name", "pinned", "latest", "sequence", "state"}]}."""
    handler.send_json(200, {"following": _feature(handler).following()})


def run_follow(args):
    """Follow a name."""
    import rnfs.cli
    entry = rnfs.cli.client(args).post("/api/name/follow", name=args.name)
    print(f"following {printable(entry['name'])}; its target is fetched and pinned in the background")


def run_unfollow(args):
    """Stop following a name."""
    import rnfs.cli
    rnfs.cli.client(args).post("/api/name/unfollow", name=args.name)


def run_following(args):
    """List followed names."""
    import rnfs.cli
    for entry in rnfs.cli.client(args).get("/api/name/following")["following"]:
        print(f"{printable(entry['name'])} {printable(entry['pinned'] or '-')} {printable(entry['latest'] or '-')} "
              f"{printable(entry['state'])}")
