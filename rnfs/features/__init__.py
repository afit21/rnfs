"""Optional features built on top of the node: one module per feature, each defining a Feature subclass.

A feature owns its state, background work, API routes and CLI commands, so adding one doesn't mean editing
the node, the API and the CLI in many places. The node creates every feature listed in FEATURE_MODULES at
startup (`node.feature(name)` returns it), starts them after its own services and stops them before its own.

Rules for feature modules:
- Don't import RNS (or anything that does) at module level: the CLI imports every feature module to build
  its parser, and client commands must not pay for loading RNS. Import it inside functions.
- The constructor must be cheap and must not need Reticulum, so tests can build an offline node.
- API handlers take the ApiHandler and use its helpers (param, json_body, send_json); they reach the
  feature through `handler.node.feature(NAME)`. Every feature route requires the API token.
- CLI commands are added with the `command(subparsers, name, run, help)` helper the CLI passes in, and run
  functions talk to the daemon with `rnfs.cli.client(args)` (import rnfs.cli inside the function).
"""
import importlib
from collections.abc import Callable

# Module paths, in start order. A feature may use the features before it
FEATURE_MODULES: list[str] = ["rnfs.features.peers", "rnfs.features.jobs", "rnfs.features.keepers",
                               "rnfs.features.follow", "rnfs.features.contacts", "rnfs.features.lxmf"]

ApiRoute = Callable[[object], None]  # handler(api_handler)


class Feature:
    """Base class: override what the feature needs."""

    name: str = ""  # unique, used for node.feature(name) and in status

    def __init__(self, node):
        """Set up state from node.config without touching the network."""
        self.node = node

    def start(self):
        """Start background work, once the node's own services run. Not called on offline test nodes."""

    def stop(self):
        """Stop background work and save state. Called before the node stops its own services."""

    def save(self):
        """Save state that is written in batches (called periodically and at stop)."""

    def status(self) -> dict:
        """Fields added to `rnfs status`."""
        return {}

    def requests(self) -> dict[str, Callable]:
        """{path: handler(data, remote_identity) -> response} for new rnfs.dht request paths.

        The request server registers them at startup and applies private mode and the per-link request
        limit; a handler must strictly parse data (untrusted), never return None (RNS would send nothing),
        and return False for anything malformed or refused. An exception answers False.
        """
        return {}

    @classmethod
    def api_routes(cls) -> dict[tuple[str, str], ApiRoute]:
        """{(method, path): handler} for this feature's API endpoints (paths under /api/)."""
        return {}

    @classmethod
    def cli_commands(cls, commands, command):
        """Add this feature's subcommands: command(commands, name, run, help) returns an argparse parser."""


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
