"""Deny lists (feature "deny"): the deny_lists setting's sources, and the local list's CLI and API (section 8).

The deny list itself (what is checked, and where) is core: rnfs/denylist.py. This feature keeps it up to date:
- Each deny_lists entry is a source: `<cid>[/path]`, a fixed list, or `name/<name>[/path]`, a list that follows
  the name. The source's target must be a file of at most denylist.MAX_LIST_BYTES; it is fetched pinned, read,
  checked (denylist.parse_list) and only then accepted: its CID is recorded (the deny_lists table) and its
  blocks pinned with source "deny" while it is in use, then the version it replaces is released. A list that
  can't be fetched, read or parsed is logged, and the source keeps the version it had.
- A name is resolved as `name resolve` does (persisted, so its announces keep the record held up to date),
  checked every CHECK_INTERVAL from the record held, and through the network only if nothing was heard for
  MISSED_ANNOUNCES x announce_interval. A failed check is retried after RETRY_INTERVAL, doubling up to
  announce_interval; a CID source once accepted is never checked again.
- At start, sources no longer in deny_lists are forgotten and every "deny" pin no source uses is released.
- One background thread does every check and fetch, one at a time.
- `rnfs deny add|rm|ls` and /api/deny edit and show the local list (denylist.DenyList) and the sources' state.

No RNS at module level (the CLI imports this module, and Config.validate calls validate_settings).
"""
import threading
import time

from rnfs.constants import ID_LENGTH, MISSED_ANNOUNCES
from rnfs.content.cid import Cid
from rnfs.errors import RnfsError
from rnfs.features import Feature, _common
from rnfs.features._common import LOG_ERROR, LOG_NOTICE, LOG_VERBOSE, LOG_WARNING, RETRY_INTERVAL, retry_delay
from rnfs.fsutil import printable

NAME = "deny"
NAME_PREFIX = "name/"
CHECK_INTERVAL = 600.0  # seconds between checks of a name source's record held (no network)
FAR_FUTURE = float("inf")


def parse_source(text) -> tuple[Cid | None, bytes | None, list[str]]:
    """(root CID or None, name or None, path names) of a deny_lists entry: `<cid>[/path]` or
    `name/<name>[/path]`. Raises ValueError."""
    if not isinstance(text, str) or not text.strip():
        raise ValueError("deny_lists entries must be <cid>[/path] or name/<name>[/path]")
    text = text.strip()
    named = text.startswith(NAME_PREFIX)
    first, *names = text[len(NAME_PREFIX):].split("/") if named else text.split("/")
    if any(not name for name in names):
        raise ValueError(f"invalid deny_lists entry {text!r}: empty name in its path")
    if not named:
        try:
            return Cid.parse(first), None, names
        except ValueError:
            raise ValueError(f"invalid deny_lists entry {text!r}: not <cid>[/path] or name/<name>[/path]") from None
    try:
        name = bytes.fromhex(first)
    except ValueError:
        name = b""
    if len(name) != ID_LENGTH:
        raise ValueError(f"invalid deny_lists entry {text!r}: a name is {2 * ID_LENGTH} hex characters")
    return None, name, names


def validate_settings(config):
    """Raise ValueError unless deny_lists is a list of sources (Config.validate)."""
    if not isinstance(config.deny_lists, list):
        raise ValueError("deny_lists must be a list of <cid>[/path] or name/<name>[/path]")
    for text in config.deny_lists:
        parse_source(text)


def _log(message: str, level: int = LOG_VERBOSE):
    """Log with this feature's prefix."""
    _common.log(f"deny list: {message}", level)


class DenyFeature(Feature):
    """The deny_lists sources: fetched, checked and followed on one thread. Thread-safe."""

    name = NAME

    def __init__(self, node):
        """Read the sources, forget those no longer configured and release their pins; no network."""
        super().__init__(node)
        self.deny = node.deny
        self._sources = list(dict.fromkeys(text.strip() for text in node.config.deny_lists))
        self._lock = threading.Lock()
        self._state: dict[str, str] = {}  # source -> what its last check did
        self._due: dict[str, float] = {}  # source -> monotonic time of its next check
        self._failures: dict[str, int] = {}  # source -> checks failed in a row
        self._wake = threading.Event()
        self._stop = threading.Event()
        for source in self.deny.accepted():
            if source not in self._sources:
                self.deny.forget(source)
                _log(f"stopped using {source}: it is no longer in deny_lists", LOG_NOTICE)
        self._release_unused()
        for source in self._sources:
            self._state[source] = "in use" if self.deny.in_use(source) else "not fetched yet"

    def _release_unused(self):
        """Release every "deny" pin that no source's accepted version is."""
        from rnfs.content.storage import DENY
        storage = self.node.storage
        used = set(self.deny.accepted().values())
        released = [cid for cid in storage.pinned_by(DENY) if cid not in used and storage.unpin(cid, DENY)]
        if released:
            self.node.publisher.refresh()

    # Lifecycle

    def start(self):
        """Check every source once interfaces had time to come up, then keep checking."""
        first = time.monotonic() + self.node.config.startup_announce_delay
        with self._lock:
            for source in self._sources:
                self._due[source] = first
        self._spawn(self._loop, "rnfs-deny")

    def stop(self, deadline: float | None = None):
        """Stop the background thread and wait for it until deadline (a fetch in progress stops at its next network
        wait), so the node doesn't close the state database under it."""
        self._stop.set()
        self._wake.set()
        self._join(deadline)

    def status(self) -> dict:
        """Entries denied (the local list's and every source's) and sources configured."""
        return {"deny_entries": len(self.deny.cids) + len(self.deny.hashes), "deny_lists": len(self._sources)}

    def lists(self) -> list[dict]:
        """Every source: {"source", "cid" (the version in use, or None), "entries", "state"}."""
        accepted = self.deny.accepted()
        with self._lock:
            return [{"source": source, "cid": str(accepted[source]) if source in accepted else None,
                     "entries": self.deny.entry_count(source), "state": self._state.get(source, "")}
                    for source in self._sources]

    # Checking and fetching

    def check(self, source: str) -> bool:
        """Bring source's list up to date: True if it is current (or a CID source already accepted)."""
        from rnfs import denylist
        from rnfs.content import walker
        from rnfs.content.storage import DENY
        root, name, names = parse_source(source)
        previous = self.deny.accepted().get(source)
        if name is not None:
            max_age = MISSED_ANNOUNCES * self.node.config.announce_interval
            try:
                result = self.node.names.resolve(name, persist=True, max_age=max_age)
            except Exception as e:  # noqa: BLE001 - resolving is retried later
                self._set_state(source, f"resolve failed: {e}")
                return False
            if result is None:
                self._set_state(source, "unresolved")
                return False
            root = Cid.parse(result["cid"])
        elif previous is not None and self.deny.in_use(source):
            self._set_state(source, "in use")
            return True
        try:
            with self.node.pinned_fetch(root, names, explicit=False) as fetch:
                expect = fetch.resolve()
                if previous == expect.cid and self.deny.in_use(source):
                    self._set_state(source, "in use")
                    return True
                if walker.kind_of(expect, fetch.get(expect)) != walker.FILE_TYPE:
                    raise ValueError("it is a directory, not a file")
                if fetch.size_of(expect) > denylist.MAX_LIST_BYTES:
                    raise ValueError(f"it is over {denylist.MAX_LIST_BYTES // 1024} KiB")
                self._set_state(source, f"fetching {expect.cid}")
                fetch.fetch_dag(expect)
                entries = denylist.parse_list(b"".join(walker.read_range(expect, fetch.get)))
                # Pinned inside the fetch's protection, so the new version is never unprotected
                self.node.storage.pin(expect.cid, DENY)
                self.deny.accept(source, expect.cid, entries)
        except (RnfsError, ValueError, OSError) as e:
            kept = f"; still using {previous}" if previous is not None and self.deny.in_use(source) else ""
            self._set_state(source, f"rejected: {e}{kept}")
            _log(f"couldn't use {source}: {e}{kept}", LOG_WARNING)
            return False
        if previous is not None and previous != expect.cid:
            self._release_unused()
        self._set_state(source, "in use")
        _log(f"using {source} at {expect.cid}: {len(entries[0]) + len(entries[1])} entries", LOG_NOTICE)
        return True

    def _set_state(self, source: str, state: str):
        """Record what the last check of source did."""
        with self._lock:
            self._state[source] = state

    def run_due(self, now: float | None = None) -> float:
        """Check every source that is due; seconds until the next is due."""
        interval = self.node.config.announce_interval
        while not self._stop.is_set():
            now = time.monotonic() if now is None else now
            with self._lock:
                due = [source for source, when in self._due.items() if when <= now]
                if not due:
                    upcoming = min(self._due.values(), default=now + interval)
                    return max(0.0, upcoming - now)
                source = min(due, key=self._due.__getitem__)
                self._due[source] = FAR_FUTURE
            try:
                ok = self.check(source)
            except Exception as e:  # noqa: BLE001 - e.g. disk full saving: a failed check, tried again later
                _log(f"checking {source} failed: {e!r}", LOG_ERROR)
                self._set_state(source, f"check failed: {e}")
                ok = False
            with self._lock:
                if ok:
                    self._failures.pop(source, None)
                    # A CID source, once in use, never changes
                    self._due[source] = time.monotonic() + CHECK_INTERVAL if source.startswith(NAME_PREFIX) \
                        else FAR_FUTURE
                else:
                    failures = self._failures.get(source, 0) + 1
                    self._failures[source] = failures
                    self._due[source] = time.monotonic() + retry_delay(failures, interval)
            now = None
        return 0.0

    def _loop(self):
        """The background thread: checks and fetches, one at a time, once the node has joined."""
        # In steps, so a stop during the join (minutes on a radio) ends this at once rather than at the node's stop
        # deadline, which would leave the other features' threads and the node's no time
        while not self.node.joined.wait(0.5):
            if self._stop.is_set():
                return
        while not self._stop.is_set():
            try:
                delay = self.run_due()
            except Exception as e:  # noqa: BLE001 - keep checking
                _log(f"checking deny lists failed: {e!r}", LOG_ERROR)
                delay = RETRY_INTERVAL
            self._wake.wait(min(delay, 60.0))
            self._wake.clear()

    # API and CLI

    @classmethod
    def api_routes(cls) -> dict:
        """GET /api/deny; POST /api/deny/add, /api/deny/rm."""
        return {("GET", "/api/deny"): _api_list, ("POST", "/api/deny/add"): _api_add,
                ("POST", "/api/deny/rm"): _api_rm}

    @classmethod
    def cli_commands(cls, commands, command):
        """`rnfs deny add|rm|ls`."""
        deny = commands.add_parser("deny", help="the deny list: content this node refuses").add_subparsers(
            dest="deny_command", required=True, metavar="command")
        command(deny, "add", run_add, "deny CIDs or file hashes (sha256:<hex>): never fetched, stored, served or "
                "provided").add_argument("entries", nargs="+", metavar="ENTRY")
        command(deny, "rm", run_rm, "remove entries from the local deny list").add_argument(
            "entries", nargs="+", metavar="ENTRY")
        command(deny, "ls", run_ls, "list the local deny list, then each deny list source and its state")


FEATURE = DenyFeature


def _entries(handler) -> list:
    """The request's "entries": a list of strings (ValueError otherwise)."""
    entries = handler.json_body().get("entries")
    if not isinstance(entries, list) or not entries or not all(isinstance(entry, str) for entry in entries):
        raise ValueError("entries must be a non-empty list of CIDs or sha256:<hex> file hashes")
    return entries


def _feature(handler) -> DenyFeature:
    """The node's deny feature. NotFound if the daemon has none."""
    return _common.feature(handler, NAME, "this daemon has no deny list")


def _api_list(handler):
    """GET /api/deny: {"entries": [local entry], "lists": [{"source", "cid", "entries", "state"}]}."""
    feature = _feature(handler)
    handler.send_json(200, {"entries": feature.deny.entries(), "lists": feature.lists()})


def _api_add(handler):
    """POST /api/deny/add {"entries": [...]}: {"added": [entries new to the local list]}."""
    feature = _feature(handler)
    handler.send_json(200, {"added": feature.deny.add(_entries(handler))})


def _api_rm(handler):
    """POST /api/deny/rm {"entries": [...]}: {"removed": [...]}; 404 if one isn't on the local list."""
    feature = _feature(handler)
    handler.send_json(200, {"removed": feature.deny.remove(_entries(handler))})


def run_add(args):
    """Add entries to the local deny list."""
    import rnfs.cli
    added = rnfs.cli.client(args).post("/api/deny/add", entries=args.entries)["added"]
    print(f"added {len(added)} entr{'y' if len(added) == 1 else 'ies'} to the deny list")


def run_rm(args):
    """Remove entries from the local deny list."""
    import rnfs.cli
    removed = rnfs.cli.client(args).post("/api/deny/rm", entries=args.entries)["removed"]
    print(f"removed {len(removed)} entr{'y' if len(removed) == 1 else 'ies'} from the deny list")


def run_ls(args):
    """Print the local entries, one per line, then each source: source, CID in use, entries, state."""
    import rnfs.cli
    result = rnfs.cli.client(args).get("/api/deny")
    for entry in result["entries"]:
        print(printable(entry))
    for entry in result["lists"]:
        print(f"list {printable(entry['source'])} {printable(entry['cid'] or '-')} "
              f"{printable(entry['entries'])} entries: {printable(entry['state'])}")
