"""RnfsNode: one running rnfs node, wiring Reticulum to the DHT, storage, fetching and publishing, and the
facade that the API, gateway and CLI call (design.md, "Node facade").

Startup: Reticulum from <rnfs dir> (attaching to a shared instance if its config shares one), the node's
identity and its rnfs.dht destination, then the routing table, provider records, storage, shared folder,
fetcher, publisher and names; the request server answers through NodeBackend. Background threads: the
announcer, the join (bootstrap lookup for our own ID), the publish loop, the name re-announce loop and
maintenance (shared-folder scans, provider record expiry and saves, max_storage enforcement).

Threading: RNS calls the announce and request handlers on its own threads; facade calls come from API and
gateway threads. Every component locks internally; the node itself only guards the small sets it owns.
"""
import os
import secrets
import shutil
import stat
import threading
import time
from contextlib import contextmanager
from typing import BinaryIO, Iterator

import RNS

from rnfs import __version__, features, fsutil
from rnfs.config import Config
from rnfs.constants import APP_NAME, DHT_ASPECT, MODE_SERVER
from rnfs.content import archive, builder, dag, edit, walker
from rnfs.content.cid import Cid
from rnfs import node_content  # --- content: add --no-pin/--to/--chunker, sealed files, gateway formats ---
from rnfs.content import chunker as content_chunker
from rnfs.content.shared_folder import SharedFolder
from rnfs.content.storage import FOLDER, USER, Storage, read_json, remote_source, write_json
from rnfs.dht.lookup import Lookup
from rnfs.dht.providers import ProviderStore
from rnfs.dht.routing import RoutingTable
from rnfs.errors import InvalidDag, NotFound, RnfsError, TooLarge
from rnfs.fetcher import Fetch, Fetcher, describe
from rnfs.mesh import RnsPaths
from rnfs.log import LOG_ERROR, LOG_NOTICE, LOG_VERBOSE, LOG_WARNING, cap_warnings, log, node_name, plural, size
from rnfs.names import NameManager, parse_name
from rnfs.pin_names import PinNames
from rnfs.network import guards, rates
from rnfs.network.announces import DhtAnnounceHandler, DhtAnnouncer, parse_mode
from rnfs.network.client import PeerClient
from rnfs.network.identity import load_or_create_identity, parse_node_id
from rnfs.network.links import LinkPool
from rnfs.network.server import RequestServer
from rnfs.publisher import Publisher

PROVIDERS_FILE = "providers.json"
FETCHED_FILE = "fetched.json"  # cached targets fetched for the user (not the gateway): the roots strategy's
SAVE_INTERVAL = 300.0  # seconds between saves of provider records
STATE_SAVE_INTERVAL = 30.0  # seconds between saves of cache records and invalid roots, when they changed
ARCHIVE_BLOCK_OVERHEAD = 64  # bytes an archive spends on each block beyond the block (msgpack framing, CID)
EVICT_INTERVAL = 60.0  # seconds between max_storage checks when nothing else triggers one
GATEWAY_NAME_AGE = 60.0  # a gateway resolution reuses a record heard this recently
STOP_TIMEOUT = 5.0
KIND_NAMES = {dag.KIND_FILE: "file", dag.KIND_EXECUTABLE: "executable", dag.KIND_DIRECTORY: "directory"}


def _make_private_dirs(config: Config):
    """Create the rnfs dir and the node's subdirectories readable by the owner only (0700).

    They hold the node's identity, name keys and the local API token, so other local users must not read
    them; a root left group- or world-accessible by earlier versions is tightened.
    """
    root = config.root
    os.makedirs(root, mode=0o700, exist_ok=True)
    # Windows has no Unix permission bits (st_mode always shows others allowed), so only the ACL that the
    # rnfs dir inherits from the user's profile protects it there
    mode = stat.S_IMODE(os.stat(root).st_mode)
    if mode & 0o077 and not fsutil.IS_WINDOWS:
        try:
            os.chmod(root, 0o700)
            log(f"made {root} private to its owner (it had mode {mode:o})", LOG_NOTICE)
        except OSError as e:
            log(f"couldn't make {root} private to its owner (mode {mode:o}): {e}", LOG_WARNING)
    for directory in (config.state_dir, config.blocks_dir, os.path.dirname(config.identity_path), config.names_dir):
        os.makedirs(directory, mode=0o700, exist_ok=True)


def _check_free_space(path: str, needed: int):
    """Raise TooLarge if needed bytes won't fit in the free space of the filesystem path is to be written on."""
    directory = os.path.dirname(os.path.abspath(path))
    try:
        free = shutil.disk_usage(directory).free
    except OSError:
        return  # a missing or unusable directory is reported by the write itself
    if needed > free:
        raise TooLarge(f"{size(needed)} won't fit in the {size(free)} free in {directory}")


PARTIAL_SUFFIX = ".partial"  # get writes out under .rnfs-get-<random>.partial, then renames it


def _remove_partial(path: str):
    """Delete a partial get output (file or directory tree) if there is one; a symlink is removed, not followed."""
    try:
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        elif os.path.lexists(path):
            fsutil.remove(path)
    except OSError as e:
        log(f"removing {path} failed: {e!r}", LOG_WARNING)


def _remove_created(path: str, created: os.stat_result):
    """Remove path if it is still the file this call created (same device and inode), and nothing else."""
    try:
        current = os.lstat(path)
        if (current.st_dev, current.st_ino) == (created.st_dev, created.st_ino):
            fsutil.remove(path)
    except OSError:
        pass


def _rns_is_known(node_id: bytes) -> bool:
    """True if RNS knows node_id's identity, so contacting it needs no path request for the key."""
    return RNS.Identity.recall(node_id) is not None


def _warn_unused_reticulum_config(config: Config):
    """Warn about a Reticulum config left in the rnfs dir by earlier versions, which used it as RNS's configdir."""
    leftover = os.path.join(config.root, "config")
    in_use = os.path.realpath(RNS.Reticulum.configdir) == os.path.realpath(config.root)
    if os.path.isfile(leftover) and not in_use:
        log(f"ignoring the Reticulum config in {leftover}: Reticulum now uses {RNS.Reticulum.configdir}. "
            f"Set reticulum_dir (or --rnsconfig) to {config.root} to keep using it, or delete it", LOG_WARNING)


class _NamePins:
    """The pins NameManager uses: storage pins that also update what the node provides."""

    def __init__(self, node: "RnfsNode"):
        """Pins for node's NameManager."""
        self.node = node

    def pin(self, cid: Cid, source: str):
        """Pin cid with a name's source."""
        self.node.storage.pin(cid, source)
        self.node.publisher.refresh()

    def unpin(self, cid: Cid, source: str) -> bool:
        """Release a name's source from cid; withdraws it at once if no longer provided."""
        released = self.node.storage.unpin(cid, source)
        self.node.publisher.refresh()
        return released


class NodeBackend:
    """What RequestServer asks of the node (network/server.py's Backend). Called on RNS threads."""

    def __init__(self, node: "RnfsNode"):
        """Backend for node."""
        self.node = node

    def is_dht_server(self) -> bool:
        """True in server mode."""
        return self.node.config.dht_mode == "server"

    def find(self, key: bytes) -> tuple[list[bytes], list[bytes]]:
        """Providers of key (oldest half plus a random half, banned left out) and the closest servers."""
        routing = self.node.routing
        providers = self.node.records.for_reply(key, exclude=routing.is_banned)
        nodes = [n for n in routing.closest(key, for_reply=True) if n != self.node.node_id]
        return providers, nodes

    def provide(self, publisher_id: bytes, keys: list[bytes], withdrawn: list[bytes]) -> bool:
        """Record publisher_id for keys; True only if every key was recorded. Banned publishers are ignored."""
        if not self.is_dht_server() or self.node.routing.is_banned(publisher_id):
            return False
        return self.node.records.provide(publisher_id, keys, withdrawn) == len(keys)

    def served_block(self, cid: Cid) -> bytes | None:
        """cid's block if it's in the DAG of a provided CID, re-checked so a damaged block isn't sent."""
        if not self.node.publisher.serves(cid):
            return None
        block = self.node.storage.get(cid, verify=True)
        if block is None:
            # It was damaged and has been deleted: what reached it is no longer complete
            self.node.refresh_soon()
        return block

    def remote_pin(self, identity_hash: bytes, cid: Cid, pin: bool) -> bool:
        """Accept a /pin from an allowed identity (the server checked remote_pin_identities)."""
        return self.node.remote_pin(identity_hash, cid, pin)

    def is_member(self, identity_hash: bytes) -> bool:
        """True if the identity is on the members list (private mode)."""
        return identity_hash.hex() in self.node.members

    def feature_requests(self) -> dict:
        """Request paths added by features (Feature.requests): {path: handler(data, remote_identity)}."""
        return {path: handler for feature in self.node.features.values()
                for path, handler in feature.requests().items()}


class RnfsNode:
    """A running node. Only one per process: RNS.Reticulum is a singleton, and rnfs changes a process-wide
    RNS setting (section 3)."""

    def __init__(self, config: Config):
        """Start Reticulum and every service of the node, then announce, join and publish in the background."""
        config.validate()
        _make_private_dirs(config)  # before Reticulum and the identity, which write under it
        # Refuse compressed Resources and stop compressing ours (section 3); the server sets it too
        RNS.Resource.AUTO_COMPRESS_MAX_SIZE = 0
        guards.install()  # reject oversized Resource advertisements before RNS allocates for them
        # By default this is RNS's own config directory, the one rnsd uses: attaching to a shared instance needs
        # its RPC key, which RNS derives from the transport identity stored in that directory (section 3)
        self.reticulum = RNS.Reticulum(configdir=config.reticulum_root, verbosity=config.verbosity or None)
        log(f"Reticulum config in {RNS.Reticulum.configdir}"
            + (", attached to a shared instance" if self.reticulum.is_connected_to_shared_instance else ""))
        _warn_unused_reticulum_config(config)
        self.identity = load_or_create_identity(config.identity_path)
        self.destination = RNS.Destination(self.identity, RNS.Destination.IN, RNS.Destination.SINGLE,
                                           APP_NAME, DHT_ASPECT)
        slow = rates.is_slow_node(config, self.reticulum)
        self.links = LinkPool(self.identity, config)
        self._setup(config, bytes(self.destination.hash), PeerClient(self.links, config), slow,
                    names_network=None, is_known=_rns_is_known)
        # mesh: hops and next-hop interfaces rank providers; one set of traffic counters for client and server
        self.paths = RnsPaths(self.reticulum)
        self.fetcher.path_info = self.paths.info

        self.server = RequestServer(self.destination, config, self.backend, traffic=self.client.traffic)
        self.announcer = DhtAnnouncer(self.destination, config)
        self.announce_handler = DhtAnnounceHandler(self._heard_server, self._heard_client, self.routing.is_banned,
                                                   self.node_id)
        self.announce_handler.register()
        self.names.start()
        log(f"node {node_name(self.node_id)} running as a DHT {config.dht_mode}"
            + (" (slow node)" if slow else "") + (" in private mode" if config.private else ""), LOG_NOTICE)
        log(f"storing blocks in {config.blocks_dir}, sharing everything in {config.files_dir}")
        self.announcer.start()
        self._start_threads()

    @classmethod
    def offline(cls, config: Config, node_id: bytes, client, names_network, slow: bool = False) -> "RnfsNode":
        """A node without Reticulum, for tests: client and names_network stand in for the network.

        No background thread runs; tests call publisher.publish_due(), maintain() etc. themselves.
        """
        node = cls.__new__(cls)
        node.reticulum = node.links = node.server = node.announcer = node.announce_handler = None
        node._setup(config, node_id, client, slow, names_network=names_network, is_known=lambda node_id: True)
        node.joined.set()
        return node

    def _setup(self, config: Config, node_id: bytes, client, slow: bool, names_network, is_known):
        """Everything that doesn't need Reticulum itself."""
        self.config = config
        self.node_id = node_id
        self.client = client
        self.slow = slow
        self._is_known = is_known
        self.members = {h.strip().strip("<>").lower() for h in config.members}
        _make_private_dirs(config)

        self.routing = RoutingTable(node_id, config, admit=self._is_member_node if config.private else None)
        self.records = ProviderStore(config.max_records, os.path.join(config.state_dir, PROVIDERS_FILE))
        self.storage = Storage(config.blocks_dir, config.state_dir, config.max_storage)
        self.shared_folder = SharedFolder(config.files_dir, self.storage, config.state_dir)
        self.pin_names = PinNames(config.state_dir)  # where pins came from, for pin ls (local only)
        self.fetcher = Fetcher(self.storage, client, config, node_id, self.routing, self._lookup, slow,
                               config.state_dir, after_store=self._enforce_storage, make_room=self._make_room)
        self.publisher = Publisher(node_id, config, self.storage, self.records, client, self._lookup, slow,
                                   config.state_dir, cached_eligible=self._eligible_targets)
        self.names = NameManager(config, _NamePins(self), network=names_network)
        self.backend = NodeBackend(self)

        self._lock = threading.Lock()
        self._fetched_path = os.path.join(config.state_dir, FETCHED_FILE)
        self._fetched: set[Cid] = set()
        for text in read_json(self._fetched_path) or []:
            try:
                self._fetched.add(Cid.parse(text))
            except (ValueError, TypeError):
                continue
        self._remote_reserved: dict[str, dict[Cid, int]] = {}  # remote source -> CIDs being fetched -> size
        self._remote_released: set[tuple[str, Cid]] = set()  # remote pins released while still being fetched
        self._refresh_wanted = threading.Event()
        self.stop_event = threading.Event()
        self.joined = threading.Event()
        self.threads: list[threading.Thread] = []
        self._stopped = False
        self.features: dict[str, features.Feature] = {}
        for cls in features.feature_classes():
            self.features[cls.name] = cls(self)
        # What is provided, before /blocks can be asked; after the features, whose keys (kept names) would
        # otherwise all be withdrawn at every start
        self.publisher.refresh()

    def feature(self, name: str) -> "features.Feature":
        """The feature called name (rnfs.features). Raises KeyError if it isn't loaded."""
        return self.features[name]

    def _start_threads(self):
        """Start the background loops."""
        for name, target in (("rnfs-join", self._join), ("rnfs-publisher", self._run_publisher),
                             ("rnfs-names", lambda: self.names.run(self.stop_event)),
                             ("rnfs-maintenance", self._maintenance_loop)):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self.threads.append(thread)
        for feature in self.features.values():
            try:
                feature.start()
            except Exception as e:  # noqa: BLE001 - one broken feature mustn't stop the node
                log(f"feature {feature.name} failed to start: {e!r}", LOG_ERROR)

    def stop(self, timeout: float = STOP_TIMEOUT):
        """Stop background work, save state and close links. Work still waiting on the network is abandoned."""
        if self._stopped:
            return
        self._stopped = True
        log("stopping", LOG_NOTICE)
        for feature in reversed(list(self.features.values())):
            try:
                feature.stop()
            except Exception as e:  # noqa: BLE001 - keep stopping the rest
                log(f"feature {feature.name} failed to stop: {e!r}", LOG_ERROR)
        self.stop_event.set()
        self.publisher.wake()
        if self.announcer is not None:
            self.announcer.stop()
        if self.announce_handler is not None:
            self.announce_handler.deregister()
        if self.server is not None:
            self.server.stop()
        self.names.stop()
        deadline = time.monotonic() + timeout
        for thread in self.threads:
            thread.join(max(0.0, deadline - time.monotonic()))
        self.records.save()
        self.publisher.save()
        self._save_state()
        if self.links is not None:
            self.links.close_all()

    def _save_state(self):
        """Save cache records and invalid roots if they changed (each change isn't written at once)."""
        for save in (self.storage.save, self.fetcher.save, *(f.save for f in self.features.values())):
            try:
                save()
            except OSError as e:
                log(f"saving state failed: {e!r}", LOG_ERROR)

    # DHT wiring

    def _lookup(self, key: bytes) -> Lookup:
        """A lookup for key starting from the routing table, counting answers and failures in it."""
        return Lookup(key, self.routing.closest(key), self.client.find, self.node_id,
                      alpha=self.config.effective_alpha(self.slow), beta=self.config.beta,
                      timeout=self.config.lookup_timeout, is_known=self._is_known,
                      on_answer=self._answered, on_failure=self.routing.failed, exclude=self.routing.is_banned)

    def _answered(self, node_id: bytes):
        """A node answered /find, proving it is a reachable server."""
        if self.routing.heard(node_id):
            self.publisher.wake()  # a CID that found no nodes may now be publishable

    def _heard_server(self, node_id: bytes):
        """A server-mode announce (or path response) from node_id."""
        if self.routing.heard(node_id):
            log(f"heard DHT server {node_name(node_id)} ({self.routing.node_count()} known)", LOG_VERBOSE)
            self.publisher.wake()

    def _heard_client(self, node_id: bytes):
        """node_id announces client mode: it doesn't belong in the routing table."""
        self.routing.remove(node_id)

    def _is_member_node(self, node_id: bytes) -> bool:
        """Private mode's routing table admission: node_id's identity is a member."""
        identity = RNS.Identity.recall(node_id)
        return identity is not None and identity.hash.hex() in self.members

    def _seed_routing(self):
        """Add servers whose announces RNS heard (and cached) within 2 × announce_interval, before a restart.

        RNS internal (Identity.known_destinations: hash -> [time heard, packet hash, public key, app_data, ...]);
        skipped if it isn't there. Each is checked to be an rnfs.dht destination of the announced key.
        """
        try:
            entries = list(RNS.Identity.known_destinations.items())
        except Exception:
            return
        horizon = time.time() - 2 * self.config.announce_interval
        added = 0
        for destination_hash, entry in entries:
            try:
                if entry[0] < horizon or parse_mode(entry[3]) != MODE_SERVER:
                    continue
                identity = RNS.Identity(create_keys=False)
                identity.load_public_key(entry[2])
                if RNS.Destination.hash(identity, APP_NAME, DHT_ASPECT) == destination_hash:
                    added += self.routing.heard(bytes(destination_hash))
            except Exception:
                continue
        if added:
            log(f"{plural(added, 'DHT server')} known from announces heard before", LOG_VERBOSE)

    def _join(self):
        """Query the bootstrap nodes for our own ID and continue that lookup, filling the routing table."""
        try:
            if self.reticulum is not None:
                self._seed_routing()
            bootstrap = []
            for text in self.config.bootstrap_nodes:
                try:
                    bootstrap.append(parse_node_id(text))
                except ValueError as e:
                    log(f"ignoring bootstrap node: {e}", LOG_WARNING)
            if bootstrap or self.routing.node_count():
                lookup = Lookup(self.node_id, bootstrap + self.routing.closest(self.node_id), self.client.find,
                                self.node_id, alpha=self.config.effective_alpha(self.slow), beta=self.config.beta,
                                timeout=self.config.lookup_timeout, is_known=self._is_known,
                                on_answer=self._answered, on_failure=self.routing.failed,
                                exclude=self.routing.is_banned)
                lookup.find_closest()
                log(f"joined the DHT: {plural(self.routing.node_count(), 'server')} known", LOG_NOTICE)
        except Exception as e:
            log(f"joining the DHT failed: {e!r}", LOG_ERROR)
        finally:
            self.joined.set()

    def _run_publisher(self):
        """The publish loop, started once the join has filled the routing table."""
        self.publisher.run(self.stop_event, self.joined)

    # Maintenance

    def refresh_soon(self):
        """Recompute what is provided on the maintenance thread (for callers on RNS threads)."""
        self._refresh_wanted.set()

    def _enforce_storage(self):
        """Evict cached fetches over max_storage, withdrawing targets no longer complete."""
        if self.storage.used_bytes() <= self.config.max_storage:
            return
        self._make_room(0)

    def _make_room(self, nbytes: int):
        """Evict cached fetches so nbytes more fit in max_storage, withdrawing targets no longer complete."""
        if self.storage.evict(nbytes):
            self.publisher.refresh()
        self._prune_fetched()

    def maintain(self, now: float, last: dict) -> None:
        """One maintenance pass: shared folder, provider records, storage. last holds when each ran."""
        if now - last.get("scan", float("-inf")) >= self.config.scan_interval:
            last["scan"] = now
            try:
                self.shared_folder.scan()
                self.publisher.refresh()
            except Exception as e:
                log(f"shared folder scan failed: {e!r}", LOG_ERROR)
        if now - last.get("save", now) >= SAVE_INTERVAL:
            last["save"] = now
            self.records.expire()
            self.records.save()
        last.setdefault("save", now)
        if now - last.get("state", now) >= STATE_SAVE_INTERVAL:
            last["state"] = now
            self._save_state()
        last.setdefault("state", now)
        if now - last.get("evict", float("-inf")) >= EVICT_INTERVAL:
            last["evict"] = now
            self._enforce_storage()
        if self._refresh_wanted.is_set():
            self._refresh_wanted.clear()
            self.publisher.refresh()

    def _maintenance_loop(self):
        """Run maintain() about once a second until stopped."""
        last: dict = {}
        while not self.stop_event.is_set():
            try:
                self.maintain(time.monotonic(), last)
            except Exception as e:
                log(f"maintenance failed: {e!r}", LOG_ERROR)
            self.stop_event.wait(1.0)

    # Cached fetches the roots strategy may provide

    def _eligible_targets(self) -> set[Cid]:
        """Cached targets fetched for the user, which the roots strategy provides once complete."""
        with self._lock:
            return set(self._fetched)

    def _prune_fetched(self):
        """Forget fetched targets that are no longer cached."""
        cached = self.storage.cached_targets()
        with self._lock:
            if self._fetched <= cached:
                return
            self._fetched &= cached
            write_json(self._fetched_path, sorted(str(cid) for cid in self._fetched))

    def _note_fetched(self, target: Cid):
        """Remember a cached target fetched for the user."""
        with self._lock:
            if target in self._fetched:
                return
            self._fetched.add(target)
            write_json(self._fetched_path, sorted(str(cid) for cid in self._fetched))

    # Fetching

    @contextmanager
    def _fetching(self, root: Cid, names: list[str], from_node: bytes | None = None,
                  for_gateway: bool = False) -> Iterator[Fetch]:
        """A fetch whose blocks are recorded as a cached fetch of <root>/<names> when it ends.

        The record is made inside the protection session, so nothing fetched is ever unprotected; a failed
        fetch is recorded too, so what it got is kept and a retry resumes from there. Gateway fetches are
        cache that is never provided.

        Storage may go over max_storage while it runs (its blocks are protected); once its session has ended,
        eviction brings storage back to max_storage, which may evict what it fetched if pins alone fill it.
        """
        try:
            with self.fetcher.open(root, names, from_node) as fetch:
                try:
                    yield fetch
                finally:
                    self._record_fetch(fetch, for_gateway)
        finally:
            try:
                self._enforce_storage()
            except Exception as e:  # never hide the fetch's own outcome
                log(f"enforcing max_storage after a fetch failed: {e!r}", LOG_ERROR)

    def _record_fetch(self, fetch: Fetch, for_gateway: bool):
        """Record a cached fetch (or mark an existing one used), then update what is provided."""
        target = fetch.target.cid if fetch.target is not None else fetch.root
        names = fetch.names if fetch.target is not None else []
        if fetch.downloaded_blocks == 0:
            self.storage.touch(fetch.root, names)
            return
        log(describe(fetch), LOG_VERBOSE)
        dropped = self.storage.record_fetch(fetch.root, names, target)
        if not for_gateway:
            self._note_fetched(target)
        if dropped:
            self._prune_fetched()  # records past MAX_CACHE_RECORDS: no longer cached, so no longer provided
        if dropped or (self.config.provide_strategy == "roots" and not for_gateway):
            self.publisher.refresh()

    # Facade: content

    def add_path(self, path: str, wrap: bool = False, only_hash: bool = False, pin: bool = True,
                 to: list[str] | None = None, to_self: bool = False, seal_name: bool = True,
                 chunker: str | None = None) -> dict:
        """Import a file or directory on this machine, pinned "user" (cache if not pin) unless only_hash.

        With to/to_self the file is sealed for those recipients first (seal.py; directories are refused),
        its name sealed inside unless not seal_name; chunker is "fixed" (default) or "fastcdc".
        """
        if not os.path.lexists(path):
            raise NotFound(f"no such file or directory: {path}")
        recipients = node_content.seal_recipients(self, to, to_self, only_hash)
        with content_chunker.using(chunker):
            if only_hash:
                built, warnings = builder.add_path(path, builder.NO_PUT, wrap)
            else:
                with self.storage.protect() as session:
                    if recipients:
                        built = node_content.add_sealed_path(path, session.put, wrap, recipients, seal_name)
                        warnings = []
                    else:
                        built, warnings = builder.add_path(path, session.put, wrap)
                    node_content.keep_added(self, built.cid, pin)
                if pin:
                    self.pin_names.record(built.cid, os.path.abspath(path))
                self._added(built)
        return {"cid": str(built.cid), "size": built.size, "warnings": warnings, **({"sealed": True} if recipients else {})}

    def add_stream(self, stream: BinaryIO, size: int | None = None, name: str | None = None,
                   only_hash: bool = False, pin: bool = True, to: list[str] | None = None, to_self: bool = False,
                   seal_name: str | None = None, chunker: str | None = None) -> dict:
        """Import a stream (wrapped as the entry name if given), pinned "user" (cache if not pin) unless
        only_hash; sealed for to/to_self (with seal_name inside) if given; chunker as for add_path."""
        recipients = node_content.seal_recipients(self, to, to_self, only_hash)
        stream, size = node_content.sealing_stream(stream, size, recipients, seal_name)
        with content_chunker.using(chunker):
            if only_hash:
                built = builder.add_stream(stream, builder.NO_PUT, size, name)
            else:
                with self.storage.protect() as session:
                    built = builder.add_stream(stream, session.put, size, name)
                    node_content.keep_added(self, built.cid, pin)
                if pin:
                    self.pin_names.record(built.cid, name or seal_name or "(stdin)")
                self._added(built)
        return {"cid": str(built.cid), "size": built.size, "warnings": [], **({"sealed": True} if recipients else {})}

    def block(self, cid: str, for_gateway: bool = False) -> bytes:
        """A CID's own block, fetched if needed and re-verified (the gateway's ?format=raw)."""
        return node_content.block(self, cid, for_gateway)

    def export_stream(self, target: str, start, for_gateway: bool = False) -> dict:
        """Fetch the target's DAG, then write its archive to start(exact length), the stream to write to (None
        writes nothing: HEAD). The gateway's ?format=archive (rnfs/node_content.py)."""
        return node_content.export_stream(self, target, start, for_gateway)

    def _added(self, built: builder.Built):
        """After adding: log, provide it, and enforce max_storage."""
        log(f"added {built.cid} ({size(built.size)})", LOG_NOTICE)
        self.publisher.refresh()
        self._enforce_storage()

    def _parse_target(self, target: str, for_gateway: bool = False) -> tuple[Cid, list[str]]:
        """walker.parse_target, after resolving a leading contact nick to its name's CID (`alice/docs`).

        Never for the gateway: web pages must not reach the address book.
        """
        contacts = self.features.get("contacts")
        if contacts is not None and not for_gateway:
            target = contacts.expand_target(target)
        return walker.parse_target(target)

    def get(self, target: str, output: str, from_node: bytes | None = None, raw: bool = False) -> dict:
        """Fetch a file or directory and write it to output, which must not exist. A sealed file this node
        can open is decrypted, unless raw."""
        root, names = self._parse_target(target)
        if os.path.lexists(output):
            raise ValueError(f"{output} already exists")
        # Written under a temporary name beside output and renamed once complete, so a failed or interrupted
        # write-out never leaves a partial output under the name the user asked for
        partial = os.path.join(os.path.dirname(os.path.abspath(output)),
                               f".rnfs-get-{secrets.token_hex(8)}{PARTIAL_SUFFIX}")
        try:
            with self._fetching(root, names, from_node) as fetch:
                expect = fetch.resolve()
                # Its verified size is what writing it out takes, however few blocks it is made of
                _check_free_space(output, fetch.size_of(expect))
                sealed = None if raw else node_content.open_sealed(self, fetch, expect)
                if sealed is not None:  # decrypted as it is fetched (rnfs/content/seal.py)
                    result = {"cid": str(expect.cid), "warnings": [], **node_content.write_sealed(sealed, partial)}
                else:
                    fetch.fetch_dag(expect, self.config.max_output_entries)
                    warnings = walker.write_out(expect, fetch.get, partial, self.config.max_output_entries)
                    # Every skipped entry is a warning: keep a response (and a job's saved result) small
                    result = {"cid": str(expect.cid), **cap_warnings(warnings)}
            fsutil.rename_new(partial, output)
            return result
        finally:
            _remove_partial(partial)

    def info(self, target: str, for_gateway: bool = False, from_node: bytes | None = None, raw: bool = False) -> dict:
        """The target's own CID, type, size and executable bit (fetching the root, path and target blocks).

        Unless for_gateway or raw, a sealed file's info has its plaintext size (node_content.sealed_info).
        """
        root, names = self._parse_target(target, for_gateway)
        with self._fetching(root, names, from_node, for_gateway) as fetch:
            expect = fetch.resolve()
            block = fetch.get(expect)
            info = {"cid": str(expect.cid), "type": walker.kind_of(expect, block),
                    "size": walker.size_of(expect, block), "executable": expect.kind == dag.KIND_EXECUTABLE}
            return info if for_gateway or raw else node_content.sealed_info(self, fetch, expect, info)

    def read(self, target: str, offset: int = 0, length: int | None = None, for_gateway: bool = False,
             from_node: bytes | None = None, raw: bool = False) -> Iterator[bytes]:
        """A lazy iterator of a file's bytes in [offset, offset + length); only the covering chunks are fetched.

        Unless for_gateway (which never decrypts) or raw, a sealed file is decrypted (its plaintext range).
        """
        root, names = self._parse_target(target, for_gateway)
        if offset < 0 or (length is not None and length < 0):
            raise ValueError("offset and length must be non-negative")

        def generate() -> Iterator[bytes]:
            """Fetch and yield, window by window; closing it ends the fetch."""
            with self._fetching(root, names, from_node, for_gateway) as fetch:
                expect = fetch.resolve()
                sealed = None if raw or for_gateway else node_content.open_sealed(self, fetch, expect)
                if sealed is not None:
                    yield from sealed.read(offset, length)
                    return
                yield from fetch.read_range(expect, offset, length)

        return generate()

    def ls(self, target: str, for_gateway: bool = False) -> list[dict]:
        """A directory's entries in name order (fetching its directory and index nodes only)."""
        root, names = self._parse_target(target, for_gateway)
        with self._fetching(root, names, None, for_gateway) as fetch:
            expect = fetch.resolve()
            if walker.kind_of(expect, fetch.get(expect)) != "directory":
                raise ValueError(f"{target} is not a directory")
            fetch.fetch_listing(expect)
            return [{"name": entry.name, "kind": KIND_NAMES[entry.kind], "size": entry.size, "cid": str(entry.cid)}
                    for entry in walker.list_directory(expect, fetch.get, self.config.max_output_entries)]

    def stat(self, target: str) -> dict:
        """Size, block count and local holdings of the target's DAG (fetching only the path and target block)."""
        root, names = self._parse_target(target)
        with self._fetching(root, names) as fetch:
            expect = fetch.resolve()
        return {"cid": str(expect.cid), **self.storage.stat(expect.cid)}

    def is_local(self, target: str) -> bool:
        """True if everything the target needs is stored, so serving it needs no network. Never fetches."""
        try:
            root, names = walker.parse_target(target)
            expect = walker.resolve(root, names, self.storage.getter())
            return self.storage.is_complete(expect.cid)
        except (ValueError, NotFound, InvalidDag):
            return False

    def stored_head(self, target: str, length: int) -> bytes | None:
        """A file's first length bytes if every block they need is stored, else None. Never fetches."""
        return node_content.stored_head(self, target, length)

    def dir_set(self, target: str, name: str, child_cid: str, executable: bool = False) -> dict:
        """A new root with entry name set to child_cid in the directory target; pinned "user".

        The complete directory and the child are fetched first, so the new root is complete and provided.
        """
        root, names = self._parse_target(target)
        child = Cid.parse(child_cid)
        with self.fetcher.open(root, pinned=True) as fetch, self.fetcher.open(child, pinned=True) as child_fetch:
            fetch.fetch_dag(fetch.resolve())
            child_fetch.fetch_dag(child_fetch.resolve())
            new_root = edit.dir_set(root, names, name, child, executable, fetch.get, fetch.session.put)
            self.storage.pin(new_root, USER)
        self.pin_names.record_if_unknown(new_root, f"dir set {target} {name}")
        self.publisher.refresh()
        return {"cid": str(new_root)}

    def dir_rm(self, target: str) -> dict:
        """A new root without the entry target names; pinned "user" (the complete directory is fetched first)."""
        root, names = self._parse_target(target)
        with self.fetcher.open(root, pinned=True) as fetch:
            fetch.fetch_dag(fetch.resolve())
            new_root = edit.dir_rm(root, names, fetch.get, fetch.session.put)
            self.storage.pin(new_root, USER)
        self.pin_names.record_if_unknown(new_root, f"dir rm {target}")
        self.publisher.refresh()
        return {"cid": str(new_root)}

    # Facade: pins

    def pin_add(self, target: str) -> dict:
        """Fetch the target completely and pin it with source "user"."""
        root, names = self._parse_target(target)
        with self.fetcher.open(root, names, pinned=True) as fetch:
            expect = fetch.resolve()
            fetch.fetch_dag(expect)
            self.storage.pin(expect.cid, USER)
        if target != str(expect.cid):  # a path or a contact nick says more than the CID does
            self.pin_names.record_if_unknown(expect.cid, target)
        log(f"pinned {expect.cid}", LOG_NOTICE)
        self.publisher.refresh()
        return {"cid": str(expect.cid)}

    def pin_rm(self, cid: str):
        """Remove the "user" source of a pin; withdraws the CID at once if no longer provided."""
        parsed = Cid.parse(cid)
        if not self.storage.unpin(parsed, USER):
            raise NotFound(f"{cid} is not pinned by the user")
        self.publisher.refresh()

    def pin_ls(self, cid: str | None = None) -> list[dict]:
        """Pins with their sources, whether they're complete, their type and a local name when one is known.

        The name is where the pin was added from on this machine (pin_names), the shared-folder entry it
        came from, or for a stored directory its single entry's name (add --wrap) or its entry count.
        """
        pins = self.storage.pins()
        self.pin_names.prune(set(pins))
        if cid is not None:
            wanted = Cid.parse(cid)
            pins = {wanted: pins[wanted]} if wanted in pins else {}
        folder_names = self.shared_folder.names()
        result = []
        for c, sources in sorted(pins.items(), key=lambda item: str(item[0])):
            kind, inside = self._pin_shape(c)
            name = self.pin_names.get(c) or (folder_names.get(c) if FOLDER in sources else None) or inside
            result.append({"cid": str(c), "sources": sorted(sources), "complete": self.storage.is_complete(c),
                           "type": kind, "name": name})
        return result

    def _pin_shape(self, cid: Cid) -> tuple[str | None, str | None]:
        """(type, name from inside) of a pin whose root block is stored: "file" or "directory", and for a
        directory its one entry's name or "(directory: N entries)". (None, None) if the root isn't stored."""
        block = self.storage.get(cid)
        if block is None:
            return None, None
        if cid.is_raw:
            return "file", None
        try:
            node = dag.decode(block)
        except InvalidDag:
            return None, None
        if isinstance(node, dag.FileNode):
            return "file", None
        if isinstance(node, dag.DirectoryNode):
            if len(node.entries) == 1:
                return "directory", node.entries[0].name
            return "directory", f"(directory: {len(node.entries)} entries)"
        return "directory", "(directory)"

    def pin_remote(self, node_id: bytes, cid: str, pin: bool) -> bool:
        """Ask node_id to pin (or release our pin of) cid over an identified link; True if accepted."""
        return self.client.pin(node_id, Cid.parse(cid), pin)

    def remote_pin(self, identity_hash: bytes, cid: Cid, pin: bool) -> bool:
        """A /pin from an allowed identity: release at once, or fetch then pin with source remote:<identity>."""
        source = remote_source(identity_hash)
        if not pin:
            with self._lock:
                if cid in self._remote_reserved.get(source, {}):
                    # Still being fetched: it mustn't be pinned when the fetch ends
                    self._remote_released.add((source, cid))
                    log(f"released remote pin of {cid} for {source} while fetching it", LOG_NOTICE)
            if self.storage.unpin(cid, source):
                log(f"released remote pin of {cid} for {source}", LOG_NOTICE)
                self.publisher.refresh()
            return True
        with self._lock:
            reserved = self._remote_reserved.setdefault(source, {})
            if cid in reserved:
                self._remote_released.discard((source, cid))  # pinned again before the fetch ended
                return True
            if source in self.storage.pins().get(cid, set()):
                return True
            reserved[cid] = 0
        threading.Thread(target=self._remote_pin_fetch, args=(cid, source), name="rnfs-remote-pin",
                         daemon=True).start()
        return True

    def _remote_pin_fetch(self, cid: Cid, source: str):
        """Fetch cid for a remote pin, dropping it as soon as its verified size, or the bytes of its DAG's
        blocks met so far, would exceed the quota."""
        quota = self.config.remote_pin_quota
        try:
            with self.fetcher.open(cid, pinned=True) as fetch:
                expect = fetch.resolve()
                content = fetch.size_of(expect)
                with self._lock:
                    others = sum(n for c, n in self._remote_reserved.get(source, {}).items() if c != cid)
                    used = self.storage.source_usage(source) + others
                    if used + content > quota:
                        log(f"dropping remote pin of {cid} for {source}: {size(content)} would exceed its "
                            f"quota ({size(used)} of {size(quota)} used)", LOG_WARNING)
                        return
                    self._remote_reserved[source][cid] = content
                # A DAG of empty files has size 0 yet can hold gigabytes of directory nodes: count its blocks
                fetch.max_dag_bytes = quota - used
                try:
                    fetch.fetch_dag(expect)
                except TooLarge:
                    log(f"dropping remote pin of {cid} for {source}: its blocks would exceed its quota "
                        f"({size(used)} of {size(quota)} used)", LOG_WARNING)
                    return
                with self._lock:
                    # Checked and pinned under the lock, so a release arriving now is either seen or undoes it
                    if (source, cid) in self._remote_released:
                        log(f"not pinning {cid} for {source}: released while it was fetched", LOG_NOTICE)
                        return
                    self.storage.pin(cid, source)
            log(f"pinned {cid} for {source}", LOG_NOTICE)
            self.publisher.refresh()
        except RnfsError as e:
            log(f"remote pin of {cid} for {source} failed: {e}", LOG_WARNING)
        except Exception as e:
            log(f"remote pin of {cid} for {source} failed: {e!r}", LOG_ERROR)
        finally:
            with self._lock:
                self._remote_reserved.get(source, {}).pop(cid, None)
                self._remote_released.discard((source, cid))

    def gc(self) -> dict:
        """Evict every cached fetch and delete unreachable blocks; withdraw what is no longer complete."""
        stats = self.storage.gc()
        self.publisher.refresh()
        self._prune_fetched()
        self._save_state()
        log(f"gc removed {plural(stats['removed_blocks'], 'block')} ({size(stats['freed_bytes'])})", LOG_NOTICE)
        return {"removed_blocks": stats["removed_blocks"], "freed_bytes": stats["freed_bytes"]}

    def verify(self) -> dict:
        """Re-check every stored block against its CID, deleting bad ones."""
        stats = self.storage.verify()
        if stats["bad"]:
            self.publisher.refresh()
        return {"checked": stats["checked"], "bad": [str(cid) for cid in stats["bad"]]}

    # Facade: network

    def providers(self, cid: str) -> list[bytes]:
        """Nodes providing cid: records held here, then everything a complete provider lookup finds."""
        parsed = Cid.parse(cid)
        found: list[bytes] = list(self.records.providers(parsed.key, exclude=self.routing.is_banned))

        def add(node_id: bytes):
            """Collect a provider the lookup reports."""
            if node_id not in found:
                found.append(node_id)

        self._lookup(parsed.key).find_providers(add)
        if parsed in self.publisher.provided() and self.node_id not in found:
            found.append(self.node_id)
        return found

    def ping(self, node_id: bytes) -> float | None:
        """Round-trip time of a new link to node_id, or None if unreachable."""
        return self.client.ping(node_id)

    # Facade: names

    def name_create(self, alias: str) -> dict:
        """Create a name key."""
        return {"alias": alias, "name": self.names.create(alias).hex()}

    def name_import(self, alias: str, key_bytes: bytes) -> dict:
        """Import a name key exported elsewhere."""
        return {"alias": alias, "name": self.names.import_key(alias, key_bytes).hex()}

    def name_rm(self, alias: str):
        """Delete a name key, releasing its pin."""
        self.names.remove(alias)

    def name_export(self, alias: str) -> bytes:
        """A name's private key."""
        return self.names.export_key(alias)

    def name_list(self) -> list[dict]:
        """This node's names."""
        return self.names.list()

    def name_publish(self, alias: str, target: str) -> dict:
        """Point a name at a target, resolving any path locally (section 9)."""
        root, names = self._parse_target(target)
        cid = walker.resolve(root, names, self.storage.getter()).cid if names else root
        return self.names.publish(alias, cid)

    def name_resolve(self, name: str, for_gateway: bool = False) -> dict:
        """The record a name points at; NotFound if it can't be resolved."""
        name_bytes = parse_name(name)
        result = self.names.resolve(name_bytes, persist=not for_gateway,
                                    max_age=GATEWAY_NAME_AGE if for_gateway else 0.0)
        if result is None:
            raise NotFound(f"name {name_bytes.hex()} could not be resolved")
        return result

    # Facade: archives and status

    def export(self, target: str, output: str) -> dict:
        """Fetch the target if needed and write an archive of it to output (which must not exist)."""
        root, names = self._parse_target(target)
        if os.path.lexists(output):
            raise ValueError(f"{output} already exists")
        with self._fetching(root, names) as fetch:
            expect = fetch.resolve()
            fetch.fetch_dag(expect, self.config.max_output_entries)
            blocks = self.storage.dag_blocks(expect.cid)
            _check_free_space(output, sum(self.storage.block_size(cid) or 0 for cid in blocks)
                              + ARCHIVE_BLOCK_OVERHEAD * len(blocks))
            try:
                out = open(output, "xb")
            except FileExistsError:
                raise ValueError(f"{output} already exists") from None  # created during the fetch: not ours
            created = os.fstat(out.fileno())
            try:
                with out:
                    stats = archive.export_archive([expect], fetch.get, out, self.config.max_output_entries)
            except BaseException:
                _remove_created(output, created)
                raise
        return {"roots": [str(cid) for cid in stats["roots"]], "blocks": stats["blocks"]}

    def import_archive(self, path: str) -> dict:
        """Import an archive, pinning its complete roots with source "user"."""
        try:
            stream = open(path, "rb")
        except FileNotFoundError:
            raise NotFound(f"no such file: {path}") from None
        with stream:
            stats = archive.import_archive(stream, self.storage)
        for root in stats.get("complete", []):
            self.pin_names.record_if_unknown(root, f"archive {os.path.abspath(path)}")
        self.publisher.refresh()
        self._enforce_storage()
        return {"roots": [str(cid) for cid in stats["roots"]], "incomplete": [str(cid) for cid in stats["incomplete"]],
                "blocks": stats["blocks"]}

    def status(self) -> dict:
        """A summary of the node."""
        return {"version": __version__, "node_id": self.node_id.hex(),
                # The RNS identity hash, which other nodes' members and remote_pin_identities list (None offline)
                "identity_hash": identity.hash.hex() if (identity := getattr(self, "identity", None)) else None,
                "dht_mode": self.config.dht_mode, "slow": self.slow,
                "private": self.config.private, "known_nodes": self.routing.node_count(),
                "records": self.records.record_count(), "pins": len(self.storage.pins()),
                "provided": len(self.publisher.provided()), "blocks": self.storage.block_count(),
                "used_bytes": self.storage.used_bytes(), "max_storage": self.config.max_storage,
                "names": len(self.names.list()),
                **{key: value for feature in self.features.values() for key, value in feature.status().items()}}

