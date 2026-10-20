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
  than one transfer. Fetches obey the usual limits (max_download_size, the DAG byte bound of the new
  target's declared size, disk reserve) and follow_quota: a new target whose size, or the bytes of its DAG's
  blocks, would exceed it is refused (TooLarge, shown as the check's state; the old target stays pinned), and 0
  fetches nothing. A pinned fetch never stops at max_storage, so the quota is what keeps a name's owner from
  filling the disk; while a target replaces another both are held, so up to twice the quota per name. A target
  pinned before the quota was lowered stays. A followed target is a pin, so it is provided.
- Followed names count against keep_max_names. State: the follow table of the state database (rnfs.statedb),
  each change committed durably as it is made (the rows of the names it changes), and a meta value saying it
  was saved whole ("clean"), which the reconcile at start needs.

No RNS at module level (the CLI imports this module).
"""
import threading
import time

from rnfs.constants import ID_LENGTH, MISSED_ANNOUNCES
from rnfs.content.cid import Cid
from rnfs.errors import NotFound, RnfsError, TooLarge
from rnfs.features import Feature, _common
from rnfs.features._common import LOG_ERROR, LOG_NOTICE, LOG_VERBOSE, LOG_WARNING, RETRY_INTERVAL, retry_delay
from rnfs.features.keepers import FOLLOW, name_subcommands, parse_name_arg
from rnfs.fsutil import printable

NAME = "follow"
SOURCE_PREFIX = "follow:"
FAR_FUTURE = float("inf")


def follow_source(name: bytes) -> str:
    """The pin source for a followed name."""
    return SOURCE_PREFIX + name.hex()


def import_follow(value, tx) -> int:
    """Import follow.json (statedb.migrate): its names replace the follow table, each entry checked as rnfs 0.1.4
    loaded it (a malformed one is skipped). The state is clean only if every entry was read, as the reconcile at
    start needs (FollowFeature._reconcile). Returns the number of names imported."""
    from rnfs import statedb
    tx.execute("DELETE FROM follow")
    entries = value.get("names") if isinstance(value, dict) else None
    clean, names = isinstance(entries, dict), set()
    for name_hex, entry in entries.items() if isinstance(entries, dict) else ():
        try:
            name = bytes.fromhex(name_hex)
            pinned = Cid.parse(entry["pinned"]) if entry.get("pinned") is not None else None
            if len(name) != ID_LENGTH:
                raise ValueError("not a name")
            # A name repeated in another case is the same name: the last entry wins, as it did in memory
            tx.execute("INSERT OR REPLACE INTO follow (name, pinned) VALUES (?, ?)",
                       (name, pinned.binary if pinned is not None else None))
        except (AttributeError, KeyError, TypeError, ValueError):
            clean = False
            continue
        names.add(name)
    statedb.set_meta(tx, NAME, "clean", int(clean))
    return len(names)


def _log(message: str, level: int = LOG_VERBOSE):
    """Log with this feature's prefix."""
    _common.log(f"follow: {message}", level)


class FollowFeature(Feature):
    """Followed names and their pins. Thread-safe."""

    name = NAME
    requires = ("keepers",)  # a followed name is kept
    optional = ("contacts",)  # a name contact's nick names a name (parse_name_arg)

    def __init__(self, node):
        """Load followed names; no network."""
        from rnfs import statedb
        super().__init__(node)
        self._db = statedb.connect(node.config.state_dir)
        self._lock = threading.Lock()
        self._pinned: dict[bytes, Cid | None] = {}  # followed name -> target pinned for it (complete)
        self._unsaved: set[bytes] = set()  # names a failed save didn't write: the next save writes them too
        self._state: dict[bytes, str] = {}  # name -> what the last check did, for `following`
        self._due: dict[bytes, tuple[float, bool]] = {}  # name -> (monotonic time, use the network)
        self._failures: dict[bytes, int] = {}  # name -> checks failed in a row (for the retry backoff)
        self._resolved: set[bytes] = set()  # names whose last check resolved them (a failed fetch, then)
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._keepers = self.dependency("keepers")
        self._keepers.add_listener(self._changed)
        try:
            self._reconcile(self._load())
        except BaseException:
            self._db.close()
            raise

    # State

    def _load(self) -> bool:
        """Read the followed names, skipping malformed rows (deleted); True if the state was saved whole (its
        clean meta value: never saved, or follow.json missing, damaged or with a malformed entry when it was
        imported, isn't) and every row was read."""
        from rnfs import statedb
        clean = self._db.meta(NAME, "clean") == 1
        bad = []
        for name, pinned in self._db.query("SELECT name, pinned FROM follow"):
            try:
                if type(name) is not bytes or len(name) != ID_LENGTH:
                    raise ValueError("not a name")
                self._pinned[name] = Cid.from_bytes(pinned) if pinned is not None else None
            except (TypeError, ValueError):
                bad.append(name)
        if bad:
            clean = False
            with self._db.transaction(durable=True) as tx:  # and not clean, until the next save
                tx.executemany("DELETE FROM follow WHERE name = ?", [(name,) for name in bad])
                statedb.set_meta(tx, NAME, "clean", 0)
        return clean

    def _reconcile(self, clean: bool):
        """Make the follow pins match the saved names. If they were read whole (clean), release follow pins of
        names they don't list: a crash inside unfollow, after the name was deleted, leaves one that nothing else
        would release (`pin rm` releases only the user's pins). Otherwise (never saved, follow.json missing or
        damaged when imported, or a malformed entry) such a pin is the record of a name still followed: follow it
        again, so it is kept, checked and can be unfollowed."""
        held: dict[bytes, set[Cid]] = {}
        for cid, sources in self.node.storage.pins().items():
            for source in sources:
                if not source.startswith(SOURCE_PREFIX):
                    continue
                try:
                    name = bytes.fromhex(source[len(SOURCE_PREFIX):])
                except ValueError:
                    continue
                if len(name) == ID_LENGTH and name not in self._pinned:
                    held.setdefault(name, set()).add(cid)
        if not held:
            return
        if not clean:
            for name, cids in sorted(held.items()):
                # One target is its pin; several (a crash during a check) are sorted out by its next check
                self._pinned[name] = next(iter(cids)) if len(cids) == 1 else None
                _log(f"the followed names are missing, damaged or incomplete: following name {name.hex()} again, "
                     f"as its pin of {', '.join(sorted(str(cid) for cid in cids))} shows", LOG_WARNING)
            try:
                self._save(*held)
            except OSError as e:  # the pins are kept; the names are followed again at the next start
                _log(f"saving the followed names failed: {e}", LOG_ERROR)
            return
        for name, cids in held.items():
            source = follow_source(name)
            for cid in cids:
                _log(f"releasing {cid}, pinned for name {name.hex()}, which is no longer followed", LOG_WARNING)
                try:
                    self.node.storage.unpin(cid, source)
                except OSError as e:  # e.g. a read-only state directory: tried again at the next start
                    _log(f"could not release {cid}: {e}", LOG_ERROR)

    def _save(self, *names: bytes):
        """Persist the entries of names, and of any a failed save left unsaved, durably, and mark the state clean
        (lock held). Raises OSError; the names are then written by the next save."""
        from rnfs import statedb
        names = self._unsaved.union(names)
        try:
            with self._db.transaction(durable=True) as tx:
                tx.executemany("DELETE FROM follow WHERE name = ?",
                               [(name,) for name in names if name not in self._pinned])
                tx.executemany("INSERT OR REPLACE INTO follow (name, pinned) VALUES (?, ?)",
                               [(name, self._pinned[name].binary if self._pinned[name] is not None else None)
                                for name in sorted(names) if name in self._pinned])
                statedb.set_meta(tx, NAME, "clean", 1)
        except BaseException:
            self._unsaved = names
            raise
        self._unsaved = set()

    # Lifecycle

    def start(self):
        """Check every followed name once interfaces had time to come up, then keep checking."""
        first = time.monotonic() + self.node.config.startup_announce_delay
        with self._lock:
            for name in self._pinned:
                self._due[name] = (first, True)
        self._spawn(self._loop, "rnfs-follow")

    def stop(self, deadline: float | None = None):
        """Stop the background thread and wait for it until deadline (a fetch in progress stops at its next network
        wait), so the node doesn't close the state database under it."""
        self._stop.set()
        self._wake.set()
        self._join(deadline)

    def close(self):
        """Release the state database."""
        self._db.close()

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
                self._save(name)
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
            self._save(name)
        # Every target its source holds: a crash during an earlier check may have left more than the one recorded
        source = follow_source(name)
        for cid in self.node.storage.pinned_by(source) | ({pinned} if pinned is not None else set()):
            self.node.storage.unpin(cid, source)
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
            _, network = self._due.get(name, (FAR_FUTURE, False))
            self._due[name] = (time.monotonic(), network)
        self._wake.set()

    # Checking and fetching

    def check(self, name: bytes, network: bool = True) -> bool:
        """Bring name's follow pin up to date with its latest target; True if it is (or nothing to do).

        network=True resolves through the network (a flooded path request, maybe a DHT lookup) only if the
        record held wasn't heard within MISSED_ANNOUNCES x announce_interval; network=False uses the record held
        whatever its age (the network only when none is held).
        """
        with self._lock:
            if name not in self._pinned:
                return True
            pinned = self._pinned[name]
            self._resolved.discard(name)
        try:
            max_age = MISSED_ANNOUNCES * self.node.config.announce_interval if network else FAR_FUTURE
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
        quota = self.node.config.follow_quota
        try:
            if quota == 0:
                raise TooLarge("follow fetches are off (follow_quota is 0)")
            with self.node.pinned_fetch(target) as fetch:
                # A pinned fetch never stops at max_storage, so without this the name's owner could fill the disk
                expect = fetch.resolve()
                content = fetch.size_of(expect)
                if content > quota:
                    raise TooLarge(f"{content} bytes would exceed follow_quota ({quota})")
                # A DAG of empty files has size 0 yet can hold gigabytes of directory nodes: count its blocks too
                # (fetch_dag also bounds them by what the size can hold, whichever is lower)
                fetch.max_dag_bytes = quota
                try:
                    fetch.fetch_dag(expect)
                except TooLarge as e:
                    if fetch.dag_bytes <= quota:
                        raise  # the bound of its size, not the quota
                    raise TooLarge(f"{e}, more than follow_quota") from e
                with self._lock:
                    followed = name in self._pinned
                    if followed:
                        # Pinned inside the fetch's protection, so the new target is never unprotected
                        self.node.storage.pin(target, source)
                        previous = self._pinned[name]
                        self._pinned[name] = target
                        try:
                            self._save(name)
                        except OSError as e:  # the pins are right; the next save writes the name too
                            _log(f"saving the followed names failed: {e}", LOG_ERROR)
        except RnfsError as e:
            self._set_state(name, f"fetch of {target} failed: {e}")
            _log(f"fetching {target} for name {name.hex()} failed: {e}", LOG_WARNING)
            return False
        if not followed:
            return True  # unfollowed while fetching: nothing was pinned
        # The old target is released only now that the new one is complete, with any other the source holds (a
        # crash between pinning and saving the name leaves the target pinned but not recorded)
        for cid in self.node.storage.pinned_by(source) | ({previous} if previous is not None else set()):
            if cid != target:
                self.node.storage.unpin(cid, source)
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
