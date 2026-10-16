"""rnfs.Node: an rnfs node inside the program's own process, as RNS runs inside it (design.md, "Public API";
provisional until 0.2). Importing this module loads RNS; `import rnfs` loads it only when rnfs.Node is used.

It is an RnfsNode (rnfs/node.py) started embedded, in its own rnfs dir, behind rnfs.interface's methods:
- Reticulum is the program's running instance (or one the node starts, from the reticulum_dir setting, if none
  runs); the node never stops it.
- The guards and the compression rule cover only its rnfs.dht links (network/guards.py, DHT); the program's
  own links behave as with RNS alone.
- Where neither rnfs.toml nor settings say otherwise, it runs as a DHT client with LXMF off (EMBEDDED_DEFAULTS).
  It serves no API or gateway, writes no debug log and installs no signal handlers of its own (a Reticulum the
  node starts installs RNS's, which needs the main thread: start Reticulum first to run a Node off it).
- If starting fails, nothing of it is left in the process (RnfsNode releases its rnfs.dht destination), so the
  program may try again.
- It locks the rnfs dir as the daemon does (node.lock_rnfs_dir), so a daemon or another node can't share it,
  and only one Node runs in a process at a time.

Name records are heard through NameManager's record listener, which only queues them for the event thread; jobs
are the jobs feature's, polled for progress.
"""
import io
import os
import threading
import time
from dataclasses import fields

import RNS

from rnfs import fetcher, fsutil, interface
from rnfs.config import Config
from rnfs.errors import RnfsError
from rnfs.log import LOG_ERROR, log
from rnfs.node import RnfsNode, lock_rnfs_dir
from rnfs.objects import Record

# An embedded node's settings where neither rnfs.toml nor the program's settings give them: an app's node is
# often short-lived, and short-lived nodes should be DHT clients (section 6); the program has its own LXMF
EMBEDDED_DEFAULTS = {"dht_mode": "client", "lxmf_enabled": False}

_process_lock = threading.Lock()
_running: "Node | None" = None  # the Node running in this process: one at a time, as RNS.Reticulum is one


def _bytes(node_id: str | None) -> bytes | None:
    """A hex node ID as bytes (None stays None)."""
    return bytes.fromhex(node_id) if node_id is not None else None


class Node(interface.Rnfs):
    """An rnfs node running inside this program, in rnfs_dir (its identity, blocks and state; created private).

    reticulum is this process's running RNS.Reticulum; None uses the running instance, or starts one from the
    reticulum_dir setting if none runs. settings are Config fields over the rnfs dir's rnfs.toml (api_port,
    gateway_port and debug_log, the daemon's, are unused). Starts at once: state loaded, threads running, the
    DHT joined in the background (wait_joined()). Raises RnfsError (the dir is in use, or not private; a Node
    already runs here), ValueError (settings), OSError.
    """

    job_poll = fetcher.POLL  # a fetch's progress changes no faster

    def __init__(self, rnfs_dir, *, reticulum: "RNS.Reticulum | None" = None, settings: dict | None = None):
        """Start the node (see the class)."""
        global _running
        if reticulum is not None and reticulum is not RNS.Reticulum.get_instance():
            raise ValueError("reticulum must be this process's running RNS.Reticulum (RNS.Reticulum.get_instance())")
        settings = dict(settings or {})
        unknown = settings.keys() - ({f.name for f in fields(Config)} - {"rnfs_dir"})
        if unknown:
            raise ValueError(f"unknown settings: {', '.join(sorted(unknown))}")
        config = Config.load(os.fspath(rnfs_dir), defaults=EMBEDDED_DEFAULTS, **settings)
        with _process_lock:
            if _running is not None:
                raise RnfsError("an rnfs.Node is already running in this process; close it first")
            _running = self
        try:
            self._lock_fd = lock_rnfs_dir(config)
            try:
                node = RnfsNode(config, reticulum=reticulum, embedded=True)
            except BaseException:
                fsutil.unlock(self._lock_fd)
                raise
        except BaseException:
            with _process_lock:
                _running = None
            raise
        self._node = node
        super().__init__(_Backend(node))
        node.names.add_record_listener(self._heard)

    @property
    def internal(self) -> RnfsNode:
        """The RnfsNode underneath. Unsupported: it changes with any release."""
        return self._node

    def _close(self):
        """Stop the node (saving its state), release the rnfs dir; Reticulum keeps running."""
        global _running
        node = self._node
        node.names.remove_record_listener(self._heard)
        try:
            node.stop()
        finally:
            fsutil.unlock(self._lock_fd)
            with _process_lock:
                if _running is self:
                    _running = None

    def _report(self, message: str):
        """Log through rnfs's log."""
        log(message, LOG_ERROR)

    # Name watches

    def _heard(self, name: bytes, record):
        """NameManager's record listener (names lock held, any thread): queue the record for the watches."""
        self._record_heard(name, Record(name, record.cid, record.sequence, max(0.0, time.time() - record.heard)))

    def _watch_started(self, name: bytes):
        """Have NameManager treat name as watched."""
        self._node.names.watch(name)

    def _unwatch_name(self, name: bytes):
        """Undo _watch_started."""
        self._node.names.unwatch(name)

    def _baseline(self, name: bytes) -> int | None:
        """The sequence of name's record held now."""
        record = self._node.names.record(name)
        return record.sequence if record is not None else None


class _Backend:
    """rnfs.interface's backend for an RnfsNode: its facade, taking and returning the API's forms."""

    def __init__(self, node: RnfsNode):
        """The backend of node."""
        self.node = node

    def status(self) -> dict:
        """The node's status."""
        return self.node.status()

    def node_id(self) -> str:
        """The node ID in hex."""
        return self.node.node_id.hex()

    def wait_joined(self, timeout: float | None) -> bool:
        """Wait for the first join to finish."""
        return self.node.joined.wait(timeout)

    def add_path(self, path: str, **options) -> dict:
        """RnfsNode.add_path."""
        return self.node.add_path(path, **options)

    def add_stream(self, data, length: int | None, name: str | None, **options) -> dict:
        """RnfsNode.add_stream of bytes or a binary file."""
        stream = io.BytesIO(data) if isinstance(data, bytes) else data
        return self.node.add_stream(stream, size=length, name=name, **options)

    def get(self, target: str, output: str, from_node: str | None, raw: bool) -> dict:
        """RnfsNode.get."""
        return self.node.get(target, output, from_node=_bytes(from_node), raw=raw)

    def read(self, target: str, offset: int, length: int | None, raw: bool, from_node: str | None):
        """RnfsNode.read: a generator, fetching as it is read."""
        return self.node.read(target, offset=offset, length=length, raw=raw, from_node=_bytes(from_node))

    def block(self, cid: str) -> bytes:
        """RnfsNode.block."""
        return self.node.block(cid)

    def info(self, target: str, from_node: str | None, raw: bool) -> dict:
        """RnfsNode.info."""
        return self.node.info(target, from_node=_bytes(from_node), raw=raw)

    def ls(self, target: str) -> list[dict]:
        """RnfsNode.ls."""
        return self.node.ls(target)

    def stat(self, target: str) -> dict:
        """RnfsNode.stat."""
        return self.node.stat(target)

    def dir_set(self, target: str, name: str, child: str, executable: bool) -> dict:
        """RnfsNode.dir_set."""
        return self.node.dir_set(target, name, child, executable=executable)

    def dir_rm(self, target: str) -> dict:
        """RnfsNode.dir_rm, which needs a path to remove."""
        if "/" not in target.rstrip("/"):
            raise ValueError("dir_rm needs <cid>/<path>: the entry to remove")
        return self.node.dir_rm(target)

    def pin_add(self, target: str) -> dict:
        """RnfsNode.pin_add."""
        return self.node.pin_add(target)

    def pin_rm(self, cid: str):
        """RnfsNode.pin_rm."""
        self.node.pin_rm(cid)

    def pin_ls(self, cid: str | None) -> list[dict]:
        """RnfsNode.pin_ls."""
        return self.node.pin_ls(cid)

    def pin_remote(self, node_id: str, cid: str, pin: bool) -> bool:
        """RnfsNode.pin_remote."""
        return self.node.pin_remote(_bytes(node_id), cid, pin)

    def providers(self, cid: str) -> list[str]:
        """RnfsNode.providers, in hex."""
        return [node_id.hex() for node_id in self.node.providers(cid)]

    def ping(self, node_id: str) -> float | None:
        """RnfsNode.ping."""
        return self.node.ping(_bytes(node_id))

    def export(self, target: str, output: str) -> dict:
        """RnfsNode.export."""
        return self.node.export(target, output)

    def import_archive(self, path: str) -> dict:
        """RnfsNode.import_archive."""
        return self.node.import_archive(path)

    def gc(self) -> dict:
        """RnfsNode.gc."""
        return self.node.gc()

    def verify(self) -> dict:
        """RnfsNode.verify."""
        return self.node.verify()

    def name_create(self, alias: str) -> dict:
        """RnfsNode.name_create."""
        return self.node.name_create(alias)

    def name_import(self, alias: str, key: bytes) -> dict:
        """RnfsNode.name_import (key any bytes-like object, as a Client sends it)."""
        return self.node.name_import(alias, bytes(key))

    def name_rm(self, alias: str):
        """RnfsNode.name_rm."""
        self.node.name_rm(alias)

    def name_export(self, alias: str) -> bytes:
        """RnfsNode.name_export."""
        return bytes(self.node.name_export(alias))

    def name_list(self) -> list[dict]:
        """RnfsNode.name_list."""
        return self.node.name_list()

    def name_publish(self, alias: str, target: str) -> dict:
        """RnfsNode.name_publish."""
        return self.node.name_publish(alias, target)

    def name_resolve(self, name: str) -> dict:
        """RnfsNode.name_resolve of a name or a name contact's nick, as GET /api/name/resolve takes them."""
        from rnfs.features.keepers import parse_name_arg
        return self.node.name_resolve(parse_name_arg(self.node, name).hex())

    def submit_job(self, kind: str, target: str, output: str | None, from_node: str | None, **flags) -> dict:
        """JobsFeature.submit."""
        return self.node.feature("jobs").submit(kind, target, output, from_node, **flags)

    def job(self, job_id: str, wait: float) -> dict:
        """JobsFeature.get, waiting up to wait seconds for the job to finish."""
        from rnfs.features.jobs import check_job_id
        return self.node.feature("jobs").get(check_job_id(job_id), wait)

    def jobs(self) -> list[dict]:
        """JobsFeature.list."""
        return self.node.feature("jobs").list()

    def cancel_job(self, job_id: str) -> dict:
        """JobsFeature.cancel."""
        from rnfs.features.jobs import check_job_id
        return self.node.feature("jobs").cancel(check_job_id(job_id))

    def clear_jobs(self, job_id: str | None) -> int:
        """JobsFeature.clear."""
        from rnfs.features.jobs import check_job_id
        return self.node.feature("jobs").clear(check_job_id(job_id) if job_id is not None else None)

    def deny(self) -> dict:
        """The local deny list and its sources, as GET /api/deny answers."""
        return {"entries": self.node.deny.entries(), "lists": self.node.feature("deny").lists()}

    def deny_add(self, entries: list[str]) -> list[str]:
        """DenyList.add."""
        return self.node.deny.add(entries)

    def deny_rm(self, entries: list[str]) -> list[str]:
        """DenyList.remove."""
        return self.node.deny.remove(entries)
