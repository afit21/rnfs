"""rnfs: content-addressed file sharing over Reticulum (RNS).

For programs (design.md, "Public API"). PROVISIONAL until rnfs 0.2 (API_STATUS): names, signatures and objects
may still change. Two classes with the same methods, objects and exceptions, so a program switches by changing
the line that constructs one:
- rnfs.Node(rnfs_dir, reticulum=..., settings=...) runs a node inside the program, beside its own Reticulum;
- rnfs.Client(rnfs_dir=None) uses the user's running daemon through its local API (it never loads RNS).
Every public name is loaded when first used, so `import rnfs` loads nothing more, and RNS only with rnfs.Node.

content/     CIDs, DAGs, storage, pins, archives, shared folder (no RNS)
dht/         Kademlia routing table, provider records, iterative lookups (no RNS)
network/     everything that talks to RNS: identity, links, request client and server, announces
names.py     mutable names
fetcher.py   fetching targets from providers
publisher.py provide strategy, publishing and withdrawal in the DHT
node.py      RnfsNode, which ties the above together into a running node and offers the facade
api.py       local HTTP API; gateway.py the HTTP gateway
debuglog.py  the debug log file (debug_log)
cli.py       command line: run a daemon, or talk to one through its API
interface.py, objects.py, embedded.py, remote.py: the public API (rnfs.Node, rnfs.Client)
"""

__version__ = "0.1.5"
API_STATUS = "provisional"  # the public API (rnfs.Node, rnfs.Client) may change until rnfs 0.2

# Public name -> the module defining it, imported on first use (PEP 562)
_EXPORTS = {
    "Node": "rnfs.embedded",
    "Client": "rnfs.remote",
    **{name: "rnfs.objects" for name in ("Cid", "Added", "Written", "Entry", "Info", "Stat", "Pin", "Record",
                                         "Status", "Collected", "Verified", "Exported", "Imported", "DenyState")},
    **{name: "rnfs.interface" for name in ("Rnfs", "Name", "Names", "Job", "Jobs", "Deny", "Watch", "File")},
    **{name: "rnfs.errors" for name in ("RnfsError", "NotFound", "TooLarge", "InvalidDag", "StorageFull", "Busy",
                                        "Denied", "Cancelled", "Closed", "BlockMismatch", "SealError",
                                        "NotRecipient")},
    **{name: "rnfs.apiclient" for name in ("ApiError", "DaemonNotRunning", "ConnectionFailed", "TokenError",
                                           "AuthFailed", "SettingsError", "BadRequest", "ServerError")},
}
__all__ = ["API_STATUS", "__version__", *_EXPORTS]


def __getattr__(name: str):
    """A public name, its module imported now."""
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module 'rnfs' has no attribute {name!r}")
    import importlib
    value = getattr(importlib.import_module(module), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """The module's names, the public ones included."""
    return sorted({*globals(), *_EXPORTS})
