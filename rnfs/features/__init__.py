"""Optional features built on top of the node: one module per feature, each defining a Feature subclass.

A feature owns its state, background work, API routes and CLI commands, so adding one doesn't mean editing
the node, the API and the CLI in many places. The node creates every feature listed in FEATURE_MODULES at
startup (`node.feature(name)` returns it), starts them after its own services and stops them before its own.

Rules for feature modules:
- Don't import RNS (or anything that does) at module level: the CLI imports every feature module to build
  its parser, and client commands must not pay for loading RNS. Import it inside functions.
- The constructor must be cheap and must not need Reticulum, so tests can build an offline node.
- API handlers take the ApiHandler and use its helpers (param, json_body, send_json); they reach the
  feature through `_common.feature(handler, NAME)`. Every feature route requires the API token. Routes and
  request paths are checked when the node loads the features (api_routes, request_handlers): one that is
  malformed, or that rnfs or another feature defines too, stops the node with a FeatureError.
- CLI commands are added with the `command(subparsers, name, run, help)` helper the CLI passes in, and run
  functions talk to the daemon with `rnfs.cli.client(args)` (import rnfs.cli inside the function).
- Background threads are started with `self._spawn(...)`; `stop(deadline)` asks them to end, then waits for them
  with `self._join(deadline)`, so the node's stop budget bounds every feature's.
- A feature that uses another declares it: in `requires` if it can't work without it (that feature must come
  before it in FEATURE_MODULES, or the node refuses to load), in `optional` if it uses it when loaded. It
  reaches it with `self.dependency(name)`, which refuses a name it didn't declare. What the contacts feature
  offers the others is the ContactsApi Protocol (tests/test_contacts.py checks the feature against it).
"""
import importlib
import inspect
import re
import threading
import time
from collections.abc import Callable, Iterable
from typing import Literal, Protocol, overload, runtime_checkable

from rnfs.constants import BLOCKS_PATH, FIND_PATH, PIN_PATH, PROVIDE_PATH

# Module paths, in start order. A feature may use the features before it
FEATURE_MODULES: list[str] = ["rnfs.features.peers", "rnfs.features.jobs", "rnfs.features.keepers",
                               "rnfs.features.follow", "rnfs.features.contacts",
                               "rnfs.features.deny", "rnfs.features.settings"]

ApiRoute = Callable[[object], None]  # handler(api_handler)
API_METHODS = ("GET", "POST")  # what a feature's API route may answer
API_PATH = re.compile(r"/api/[a-z0-9][a-z0-9_/-]*")  # a feature API route's path
REQUEST_PATH = re.compile(r"/[a-z0-9][a-z0-9_/-]*")  # a feature request path (rnfs.dht)
CORE_REQUEST_PATHS = (FIND_PATH, PROVIDE_PATH, BLOCKS_PATH, PIN_PATH)  # served by rnfs itself (network/server.py)

STOP_WAIT = 5.0  # seconds stop() waits for a feature's threads when given no deadline (the node's own budget)


class FeatureError(RuntimeError):
    """A feature that can't be loaded as declared (raised while the node loads its features, before any runs), or
    one reaching a feature it didn't declare."""


@runtime_checkable
class ContactsApi(Protocol):
    """What other features may call on the contacts feature (rnfs.features.contacts.ContactsFeature, which
    tests/test_contacts.py checks against this): a renamed method fails that test instead of silently turning
    off what used it."""

    def lookup(self, nick: str, kind: str | None = None) -> bytes | None:
        """The hash of contact nick (of that kind, if given), or None."""

    def has(self, ident: bytes, kind: str) -> bool:
        """Whether some contact of that kind has hash ident."""

    def nick_of(self, ident: bytes, kind: str) -> str | None:
        """The nick of the contact of that kind with hash ident, or None."""

    def resolve_id(self, text: str, kind: str) -> bytes:
        """32 hex characters as bytes, or the hash of a contact of kind with that nick. ValueError otherwise."""

    def expand_target(self, target):
        """target with a leading name contact's nick replaced by the CID its name points at now."""


@overload
def find(node, name: Literal["contacts"]) -> ContactsApi | None: ...
@overload
def find(node, name: str) -> "Feature | None": ...


def find(node, name):
    """node's feature called name, or None if it isn't loaded (or node is a stand-in without features)."""
    return (getattr(node, "features", None) or {}).get(name)


class Feature:
    """Base class: override what the feature needs."""

    name: str = ""  # unique, used for node.feature(name) and in status
    requires: tuple[str, ...] = ()  # features this one can't work without, each before it in FEATURE_MODULES
    optional: tuple[str, ...] = ()  # features this one uses when they are loaded
    # Why this feature is deprecated (it still works, but a later release removes it), or None. Its API routes'
    # responses then carry a Deprecation header (RFC 9745) dated deprecated_since (a Unix time, in whole seconds),
    # and GET /api/capabilities lists it under "deprecated"
    deprecated: str | None = None
    deprecated_since: int = 0

    def __init__(self, node):
        """Set up state from node.config without touching the network."""
        self.node = node
        self._spawned: list[threading.Thread] = []  # started by _spawn and not yet seen ended; _join waits for them
        self._spawned_lock = threading.Lock()

    @overload
    def dependency(self, name: Literal["contacts"]) -> ContactsApi | None: ...
    @overload
    def dependency(self, name: str) -> "Feature | None": ...

    def dependency(self, name):
        """The node's feature called name, which this one declares in requires or optional; None if it isn't
        loaded. FeatureError for a name not declared, so a use the load-time check doesn't know of can't slip in."""
        if name not in self.requires and name not in self.optional:
            raise _refused(self.name, f"it uses feature {name} without declaring it")
        return find(self.node, name)

    def start(self):
        """Start background work, once the node's own services run. Not called on offline test nodes."""

    def stop(self, deadline: float | None = None):
        """Stop background work and save state. Called before the node stops its own services.

        deadline is when to stop waiting for the feature's threads, a time.monotonic() value (the node passes
        its stop budget's); None waits STOP_WAIT from now. An override asks its threads to end, then calls
        self._join(deadline); the base only waits for the threads _spawn started.
        """
        self._join(deadline)

    def _spawn(self, target: Callable[..., object], name: str, *args) -> threading.Thread:
        """Start a daemon thread named name running target(*args), which _join (and so stop) waits for. Raises
        RuntimeError if it can't be started."""
        thread = threading.Thread(target=target, args=args, name=name, daemon=True)
        thread.start()
        with self._spawned_lock:
            # Pruned here, so a feature starting threads now and then doesn't keep every ended one
            self._spawned = [t for t in self._spawned if t.is_alive()]
            self._spawned.append(thread)
        return thread

    def _join(self, deadline: float | None = None, threads: list[threading.Thread] | None = None) -> bool:
        """Wait until threads (by default those _spawn started) end or deadline (a time.monotonic() value; None:
        STOP_WAIT from now) passes; True if they all ended. Ask them to end first."""
        if deadline is None:
            deadline = time.monotonic() + STOP_WAIT
        if threads is None:
            with self._spawned_lock:
                threads = list(self._spawned)
        for thread in threads:
            thread.join(max(0.0, deadline - time.monotonic()))
        return not any(thread.is_alive() for thread in threads)

    def save(self):
        """Save state that is written in batches (called periodically and at stop)."""

    def close(self):
        """Release the state database, once the node has saved everything at shutdown (after stop and save)."""

    def status(self) -> dict:
        """Fields added to `rnfs status`."""
        return {}

    def app_removed(self, name: str):
        """App name was removed (rnfs app rm): its token no longer passes, and its pins are released once this
        returns. Drop what the feature holds or runs for it."""

    def requests(self) -> dict[str, Callable]:
        """{path: handler(data, remote_identity) -> response} for new rnfs.dht request paths.

        The request server registers them at startup and applies private mode and the per-link request
        limit; a handler must strictly parse data (untrusted), never return None (RNS would send nothing),
        and return False for anything malformed or refused. An exception answers False.

        The response must have a shape every requester's `protocol.response_refusal` accepts (nodes since 0.1.4
        check it before decoding): True, False or None, a /find answer (two lists of at most K IDs), or a list of
        at most MAX_BLOCKS_CIDS entries, each bytes of at most MAX_BLOCK_SIZE or None (a /blocks answer). Any
        other shape (a dict, a string, a longer list) is refused as malformed, and widening that check wouldn't
        reach the nodes already running. The request server checks each response the same way
        (`protocol.body_refusal`) and answers False instead of one requesters would refuse, logging an error.

        Called once, when the node loads its features: a path that isn't "/" and a name (REQUEST_PATH), one rnfs
        serves itself, or one another feature adds too stops the node (FeatureError, request_handlers).
        """
        return {}

    @classmethod
    def api_routes(cls) -> dict[tuple[str, str], ApiRoute]:
        """{(method, path): handler} for this feature's API endpoints: GET or POST of a path under /api/
        (API_PATH). A route the core API or another feature defines too stops the node from loading (FeatureError,
        api_routes)."""
        return {}

    @classmethod
    def cli_commands(cls, commands, command):
        """Add this feature's subcommands: command(commands, name, run, help) returns an argparse parser."""


def stop_feature(feature: Feature, deadline: float):
    """feature.stop(deadline=deadline), or feature.stop() for a feature whose stop takes no deadline (written for
    rnfs before 0.1.6: it waits as it always did)."""
    try:
        inspect.signature(feature.stop).bind(deadline=deadline)
    except (TypeError, ValueError):  # no deadline parameter; ValueError: no signature to read
        feature.stop()
        return
    feature.stop(deadline=deadline)


def feature_classes() -> list[type[Feature]]:
    """The Feature subclass of every module in FEATURE_MODULES, in order."""
    return [importlib.import_module(path).FEATURE for path in FEATURE_MODULES]


def checked_classes() -> list[type[Feature]]:
    """feature_classes(), for the node to load in order, once checked: FeatureError if two share a name or one
    has none, if one requires a feature that isn't loaded before it, or for an API route api_routes() refuses."""
    classes = feature_classes()
    loaded: list[str] = []
    for cls in classes:
        if not isinstance(cls.name, str) or not cls.name:
            raise _refused(f"{cls.__module__}.{cls.__qualname__}", "it has no name")
        if cls.name in loaded:
            raise _refused(cls.name, "another feature has that name")
        for kind in ("requires", "optional"):
            names = getattr(cls, kind)
            if not isinstance(names, tuple) or not all(isinstance(name, str) for name in names):
                raise _refused(cls.name, f"{kind} must be a tuple of feature names")
        for name in cls.requires:
            if name not in loaded:
                raise _refused(cls.name, f"it requires feature {name}, which isn't loaded before it")
        if cls.deprecated is not None and not (isinstance(cls.deprecated, str) and cls.deprecated
                                               and type(cls.deprecated_since) is int and cls.deprecated_since >= 0):
            raise _refused(cls.name, "deprecated must be a reason, and deprecated_since a Unix time in seconds")
        loaded.append(cls.name)
    _api_routes(classes)
    return classes


def deprecated_features() -> list[str]:
    """The names of the deprecated features (Feature.deprecated), sorted."""
    return sorted(cls.name for cls in feature_classes() if cls.deprecated is not None)


def deprecated_routes() -> dict[tuple[str, str], int]:
    """The API routes of the deprecated features (Feature.deprecated): route -> when deprecated (deprecated_since)."""
    return {route: cls.deprecated_since for cls in feature_classes() if cls.deprecated is not None
            for route in cls.api_routes()}


def api_routes() -> dict[tuple[str, str], ApiRoute]:
    """Every feature's API routes, checked: FeatureError for a route that isn't GET or POST of a path under /api/
    with a handler, or that the core API (rnfs.api.ApiHandler.ROUTES) or another feature defines too."""
    return _api_routes(feature_classes())


def _api_routes(classes: list[type[Feature]]) -> dict[tuple[str, str], ApiRoute]:
    """api_routes() of classes."""
    from rnfs.api import ApiHandler  # here: only what serves or loads features needs the API's table
    owners = dict.fromkeys(ApiHandler.ROUTES, "rnfs")  # route -> what defines it, for the error
    routes: dict[tuple[str, str], ApiRoute] = {}
    for cls in classes:
        for route, handler in cls.api_routes().items():
            if not (isinstance(route, tuple) and len(route) == 2 and route[0] in API_METHODS
                    and isinstance(route[1], str) and API_PATH.fullmatch(route[1]) and callable(handler)):
                raise _refused(cls.name, f"API route {route!r} must be (GET or POST, a path under /api/) with a "
                                         "handler")
            _claim(owners, route, cls.name, f"API route {route[0]} {route[1]}")
            routes[route] = handler
    return routes


def request_handlers(loaded: Iterable[Feature]) -> dict[str, Callable]:
    """Every loaded feature's request paths (Feature.requests), checked: FeatureError for a path that isn't "/"
    and a name with a handler, or that rnfs serves itself (CORE_REQUEST_PATHS) or another feature adds too."""
    owners = dict.fromkeys(CORE_REQUEST_PATHS, "rnfs")
    handlers: dict[str, Callable] = {}
    for feature in loaded:
        for path, handler in feature.requests().items():
            if not (isinstance(path, str) and REQUEST_PATH.fullmatch(path) and callable(handler)):
                raise _refused(feature.name, f"request path {path!r} must be \"/\" and a name, with a handler")
            _claim(owners, path, feature.name, f"request path {path}")
            handlers[path] = handler
    return handlers


def _claim(owners: dict, key, name: str, what: str):
    """Record that feature name defines key (what: how to say it); FeatureError if rnfs or a feature already does."""
    if key in owners:
        other = owners[key]
        raise _refused(name, f"{what} is already defined by {'rnfs' if other == 'rnfs' else 'feature ' + other}")
    owners[key] = name


def _refused(name: str, problem: str) -> FeatureError:
    """The one error for a feature that can't be loaded as it is: "feature <name>: <problem>"."""
    return FeatureError(f"feature {name}: {problem}")
