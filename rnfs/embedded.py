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
  and only one Node runs in a process at a time. As in the daemon, the rnfs dir is made private before its
  rnfs.toml is read, and what was changed to trust that file is logged as a warning.

Name records are heard through NameManager's record listener, which only queues them for the event thread; jobs
are the jobs feature's, polled for progress.
"""
import contextlib
import io
import json
import os
import threading
import time
from dataclasses import fields

import RNS

from rnfs import fetcher, fsutil, interface
from rnfs.apiclient import check_timeout
from rnfs.config import Config
from rnfs.errors import RnfsError, Unsupported
from rnfs.log import LOG_ERROR, LOG_WARNING, log
from rnfs.node import RnfsNode, lock_rnfs_dir, make_private_root
from rnfs.objects import Record

# An embedded node's settings where neither rnfs.toml nor the program's settings give them: an app's node is
# often short-lived, and short-lived nodes should be DHT clients (section 6); the program has its own LXMF
EMBEDDED_DEFAULTS = {"dht_mode": "client", "lxmf_enabled": False}
# The capabilities (api.CAPABILITIES) a Node lacks: it serves no API to be stopped through, and writes no daemon.json
NODE_LACKS = ("shutdown", "daemon_json")

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
        rnfs_dir = os.fspath(rnfs_dir)
        # As the daemon starts: the rnfs dir made private, then its settings read, then its subdirectories made
        # private and the dir locked (lock_rnfs_dir)
        make_private_root(os.path.expanduser(rnfs_dir))
        config = Config.load(rnfs_dir, defaults=EMBEDDED_DEFAULTS, **settings)
        for warning in config.warnings:
            log(warning, LOG_WARNING)
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
        node.events.remove_listener(self._published)  # if events.watch added it
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

    # Event watches

    def _published(self, event: dict):
        """The event log's listener (any thread, perhaps under the names or storage lock): queue the event for the
        event watches. A Node hears every event of its own run, so it never reports any lost."""
        self._events_heard(self._node.events.epoch, [event])

    def _event_baseline(self) -> tuple[int, str]:
        """Listen to the node's event log (from the first watch on), then its last sequence and epoch: an event
        published in between reaches the watch, which skips it as at or before its baseline."""
        event_log = self._node.events
        with self._watch_lock:
            if not getattr(self, "_listening", False):  # (getattr: tests build a Node without __init__)
                event_log.add_listener(self._published)
                self._listening = True
        return event_log.last, event_log.epoch


class _Backend:
    """rnfs.interface's backend for an RnfsNode: its facade, taking and returning the API's forms.

    A feature's methods are called as its API route calls them, so a Node gives a Client's results and errors: the
    feature looked up through the route's own lookup (each feature module's _feature, or _common.feature), which
    takes anything with the handler's node attribute, as this has, and raises the route's NotFound when the
    feature isn't loaded; then the route's checks of its arguments, in its order."""

    def __init__(self, node: RnfsNode):
        """The backend of node."""
        self.node = node

    def timeout(self, seconds: float | None):
        """Nothing to bound (no request is made): a context manager doing nothing, once seconds is checked as a
        Client checks it."""
        check_timeout(seconds)
        return contextlib.nullcontext()

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

    def get(self, target: str, output: str, from_node: str | None, raw: bool, no_extract: bool) -> dict:
        """RnfsNode.get."""
        return self.node.get(target, output, from_node=_bytes(from_node), raw=raw, no_extract=no_extract)

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
        """RnfsNode.dir_rm, which needs a path to remove (and checks it has one)."""
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

    def submit_job(self, kind: str, target: str | None, output: str | None, from_node: str | None, **flags) -> dict:
        """JobsFeature.submit (flags: get's sealed_name, fallback_dht and no_extract, or an add's or import's path and
        options)."""
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

    # Features (see the class)

    def peers(self) -> list[dict]:
        """PeersFeature.peers, as GET /api/peers."""
        from rnfs.features import _common, peers
        return _common.feature(self, peers.NAME).peers()

    def stats(self) -> dict:
        """PeersFeature.stats, as GET /api/stats."""
        from rnfs.features import _common, peers
        return _common.feature(self, peers.NAME).stats()

    def contacts(self) -> list[dict]:
        """ContactsFeature.list, as GET /api/contacts."""
        from rnfs.features import contacts
        return contacts._feature(self).list()

    def contact_get(self, nick: str) -> dict:
        """ContactsFeature.get, as GET /api/contacts/get."""
        from rnfs.features import contacts
        return contacts._feature(self).get(nick)

    def contact_add(self, nick: str, ident: str, kind: str | None) -> dict:
        """ContactsFeature.add, as POST /api/contacts/add (kind checked after the feature is found)."""
        from rnfs.features import contacts
        return contacts._feature(self).add(nick, ident, None if kind is None else contacts.check_kind(kind))

    def contact_rm(self, nick: str):
        """ContactsFeature.remove, as POST /api/contacts/rm."""
        from rnfs.features import contacts
        contacts._feature(self).remove(nick)

    def name_keep(self, name: str) -> dict:
        """KeepersFeature.keep of a name or a name contact's nick, as POST /api/name/keep."""
        from rnfs.features import keepers
        feature = keepers._feature(self)
        return feature.keep(keepers.parse_name_arg(self.node, name))

    def name_unkeep(self, name: str):
        """KeepersFeature.unkeep, as POST /api/name/unkeep."""
        from rnfs.features import keepers
        feature = keepers._feature(self)
        feature.unkeep(keepers.parse_name_arg(self.node, name))

    def name_kept(self) -> list[dict]:
        """KeepersFeature.kept, as GET /api/name/kept."""
        from rnfs.features import keepers
        return keepers._feature(self).kept()

    def name_follow(self, name: str) -> dict:
        """FollowFeature.follow of a name or a name contact's nick, as POST /api/name/follow."""
        from rnfs.features import follow, keepers
        feature = follow._feature(self)
        return feature.follow(keepers.parse_name_arg(self.node, name))

    def name_unfollow(self, name: str):
        """FollowFeature.unfollow, as POST /api/name/unfollow."""
        from rnfs.features import follow, keepers
        feature = follow._feature(self)
        feature.unfollow(keepers.parse_name_arg(self.node, name))

    def name_following(self) -> list[dict]:
        """FollowFeature.following, as GET /api/name/following."""
        from rnfs.features import follow
        return follow._feature(self).following()

    def lxmf_address(self) -> dict:
        """LxmfFeature.address, as GET /api/lxmf/address."""
        from rnfs.features import lxmf
        return lxmf._feature(self).address()

    def lxmf_send(self, to: str, target: str, message: str | None, propagate: bool, nearest: bool) -> dict:
        """LxmfFeature.send, as POST /api/lxmf/send."""
        from rnfs.features import lxmf
        return lxmf._feature(self).send(to, target, message, propagate, nearest)

    def lxmf_inbox(self) -> list[dict]:
        """LxmfFeature.inbox_list, as GET /api/lxmf/inbox."""
        from rnfs.features import lxmf
        return lxmf._feature(self).inbox_list()

    def lxmf_inbox_get(self, entry_id, link, output: str | None, directory: str | None,
                       use_sent_cid: bool) -> list[dict]:
        """LxmfFeature.inbox_get, as POST /api/lxmf/inbox/get: id and link checked as it checks them."""
        from rnfs.features import lxmf
        return lxmf._feature(self).inbox_get(lxmf._int(entry_id, "id"), lxmf._int(link, "link", False), output,
                                             directory, use_sent_cid)

    def lxmf_inbox_rm(self, entry_id, every: bool) -> int:
        """LxmfFeature.inbox_rm of entry_id (checked as POST /api/lxmf/inbox/rm checks it), or every message."""
        from rnfs.features import lxmf
        if every:
            return lxmf._feature(self).inbox_rm(None)
        return lxmf._feature(self).inbox_rm(lxmf._int(entry_id, "id"))

    def lxmf_outbox(self) -> list[dict]:
        """LxmfFeature.outbox_list, as GET /api/lxmf/outbox."""
        from rnfs.features import lxmf
        return lxmf._feature(self).outbox_list()

    def lxmf_sync(self, nearest: bool) -> dict:
        """LxmfFeature.sync, as POST /api/lxmf/sync."""
        from rnfs.features import lxmf
        return lxmf._feature(self).sync(nearest)

    # Streams (rnfs 0.2.0)

    def add_tar(self, data, name: str | None, pin: bool, only_hash: bool, chunker: str | None) -> dict:
        """RnfsNode.add_tar of bytes or a binary file."""
        stream = io.BytesIO(data) if isinstance(data, bytes) else data
        return self.node.add_tar(stream, name=name, only_hash=only_hash, pin=pin, chunker=chunker)

    def get_tar(self, target: str):
        """RnfsNode.get_tar's chunks: a generator, fetching (and raising) once iterated, as a Client's."""
        def chunks():
            """The tar's bytes, the fetch ended when done or closed."""
            _, inner = self.node.get_tar(target)
            try:
                yield from inner
            finally:
                inner.close()
        return chunks()

    def export_stream(self, target: str):
        """RnfsNode.export_chunks' chunks: a generator, fetching (and raising) once iterated, as a Client's."""
        def chunks():
            """The archive's bytes, the fetch ended when done or closed."""
            _, inner = self.node.export_chunks(target)
            try:
                yield from inner
            finally:
                inner.close()
        return chunks()

    def import_stream(self, data) -> dict:
        """RnfsNode.import_stream of bytes or a binary file."""
        return self.node.import_stream(io.BytesIO(data) if isinstance(data, bytes) else data)

    # Pins, the cache, the node (rnfs 0.2.0)

    def pin_label(self, cid: str, label: str | None) -> dict:
        """RnfsNode.pin_label."""
        return self.node.pin_label(cid, label)

    def cache_ls(self) -> list[dict]:
        """RnfsNode.cache_ls."""
        return self.node.cache_ls()

    def cache_rm(self, cid: str) -> dict:
        """RnfsNode.cache_rm."""
        return self.node.cache_rm(cid)

    def capabilities(self) -> dict:
        """GET /api/capabilities' answer for this node (api.capabilities), but what an embedded node can't do: be
        stopped through an API, or describe itself in daemon.json."""
        from rnfs import api
        return api.capabilities(self.node, without=NODE_LACKS)

    def shutdown(self, wait: float | None) -> bool:
        """Refused: a Node is the program's; close() stops it."""
        raise Unsupported("an rnfs.Node runs inside this program: close() it to stop it")

    def daemon_info(self) -> dict | None:
        """None: a Node is no daemon, and its rnfs dir has no daemon.json."""
        return None

    def config(self) -> dict:
        """SettingsFeature.describe, as GET /api/config answers it (through JSON, so the values have the forms a
        Client's have)."""
        from rnfs.features import _common, settings
        return json.loads(json.dumps(_common.feature(self, settings.NAME).describe()))

    def config_set(self, changes: dict) -> dict:
        """SettingsFeature.set of changes as POST /api/config/set's JSON body carries them (tuples as lists: the
        feature checks rnfs.toml reads back as exactly what it was given)."""
        from rnfs.features import _common, settings
        feature = _common.feature(self, settings.NAME)
        return feature.set(json.loads(json.dumps(changes)))

    def events_since(self, after: int | None, kinds: list[str] | None, wait: int, epoch: str | None) -> dict:
        """EventLog.since, as GET /api/events calls it: wait capped as it caps it, and none without after."""
        from rnfs import api
        event_log = getattr(self.node, "events", None)
        if event_log is None:
            raise Unsupported("this node keeps no event log")
        return event_log.since(after, kinds, min(wait, api.MAX_WATCH_WAIT) if after is not None else 0, epoch)
