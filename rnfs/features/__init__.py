"""Optional features built on top of the node: one module per feature, each defining a Feature subclass.

A feature owns its state, background work, API routes and CLI commands, so adding one doesn't mean editing
the node, the API and the CLI in many places. The node creates every feature listed in FEATURE_MODULES at
startup (`node.feature(name)` returns it), starts them after its own services and stops them before its own.

Rules for feature modules:
- Don't import RNS (or anything that does) at module level: the CLI imports every feature module to build
  its parser, and client commands must not pay for loading RNS. Import it inside functions.
- The constructor must be cheap and must not need Reticulum, so tests can build an offline node.
- API handlers take the ApiHandler and use its helpers (param, json_body, send_json); they reach the
  feature through `_common.feature(handler, NAME)`. Every feature route requires the API token.
- CLI commands are added with the `command(subparsers, name, run, help)` helper the CLI passes in, and run
  functions talk to the daemon with `rnfs.cli.client(args)` (import rnfs.cli inside the function).
- Background threads are started with `self._spawn(...)`; `stop(deadline)` asks them to end, then waits for them
  with `self._join(deadline)`, so the node's stop budget bounds every feature's.
"""
import importlib
import inspect
import threading
import time
from collections.abc import Callable

# Module paths, in start order. A feature may use the features before it
FEATURE_MODULES: list[str] = ["rnfs.features.peers", "rnfs.features.jobs", "rnfs.features.keepers",
                               "rnfs.features.follow", "rnfs.features.contacts", "rnfs.features.lxmf",
                               "rnfs.features.deny"]

ApiRoute = Callable[[object], None]  # handler(api_handler)

STOP_WAIT = 5.0  # seconds stop() waits for a feature's threads when given no deadline (the node's own budget)


class Feature:
    """Base class: override what the feature needs."""

    name: str = ""  # unique, used for node.feature(name) and in status

    def __init__(self, node):
        """Set up state from node.config without touching the network."""
        self.node = node
        self._spawned: list[threading.Thread] = []  # started by _spawn and not yet seen ended; _join waits for them
        self._spawned_lock = threading.Lock()

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
        """
        return {}

    @classmethod
    def api_routes(cls) -> dict[tuple[str, str], ApiRoute]:
        """{(method, path): handler} for this feature's API endpoints (paths under /api/)."""
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


def api_routes() -> dict[tuple[str, str], ApiRoute]:
    """Every feature's API routes."""
    routes: dict[tuple[str, str], ApiRoute] = {}
    for cls in feature_classes():
        for route, handler in cls.api_routes().items():
            if route in routes:
                raise RuntimeError(f"API route {route} is defined by two features")
            routes[route] = handler
    return routes
