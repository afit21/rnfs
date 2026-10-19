"""RnfsNode: one running rnfs node, wiring Reticulum to the DHT, storage, fetching and publishing, and the
facade that the API, gateway and CLI call (design.md, "Node facade").

Startup: Reticulum from <rnfs dir> (attaching to a shared instance if its config shares one), or for a node
embedded in another program (rnfs.Node) the program's running instance, then the node's identity and its
rnfs.dht destination, then the state database (rnfs.statedb, which imports the JSON state files of earlier
versions first), the routing table and local scores, provider records, storage, the deny list (its accepted lists
read back from storage), shared folder, fetcher, publisher, names and the features (rnfs.features); the request
server answers through NodeBackend. Background threads: the announcer, the join (saved servers and bootstrap nodes,
then a lookup for our own ID, repeated while too few servers are known), the publish loop, the name re-announce
loop, the features' own, a worker per source with remote pins waiting, and maintenance (shared-folder scans,
provider record expiry and saves, max_storage enforcement, periodic state saves, publisher refreshes asked for,
deny-list changes retried, the remote pins waiting saved). Stopping ends the features, then the node's services and
threads (waiting for them, the features' too, at most STOP_TIMEOUT in all), saves every store and closes the state,
each step guarded: one that fails skips none of the others.

Threading: RNS calls the announce and request handlers on its own threads; facade calls come from API and
gateway threads. Every component locks internally; the node itself only guards the small sets it owns.

The daemon owns its process, so its Resource guards cover every link and RNS's compression is off process-wide
(network/guards.py, PROCESS); an embedded node scopes both to its rnfs.dht links (DHT) and leaves the host
program's Reticulum running when it stops. Neither ever stops Reticulum: the daemon calls RNS.exit() itself.
"""
import errno
import hashlib
import os
import random
import secrets
import shutil
import stat
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
from functools import partial
from typing import BinaryIO, Callable, Iterator, Literal, NamedTuple, Protocol, TypeVar, overload

import RNS

from rnfs import __version__, denylist, events, features, fsutil, statedb, winacl
from rnfs.config import LOCK_FILE, Config
from rnfs.constants import (APP_NAME, DEFAULT_BOOTSTRAP_NODES, DHT_ASPECT, K, MISSED_ANNOUNCES, MODE_SERVER,
                            PARTIAL_SUFFIX)
from rnfs.content import archive, builder, dag, edit, seal, walker
from rnfs.content.cid import Cid
from rnfs import node_content  # --- content: add --no-pin/--to/--chunker, sealed files, gateway formats ---
from rnfs.content.shared_folder import SharedFolder
from rnfs.content.storage import FOLDER, REMOTE_PREFIX, USER, Storage, disk_reserve, remote_source
from rnfs.dht.lookup import Lookup
from rnfs.dht.providers import ProviderStore
from rnfs.dht.routing import RoutingTable
from rnfs.dht.routing_state import RoutingState
from rnfs.dht.scores import Scores
from rnfs.errors import Cancelled, Denied, InvalidDag, NotFound, RnfsError, TooLarge
from rnfs.fetcher import Fetch, Fetcher, Observer, describe, observing
from rnfs.mesh import RnsPaths
from rnfs.log import LOG_DEBUG, LOG_ERROR, LOG_NOTICE, LOG_VERBOSE, LOG_WARNING, cap_warnings, log, node_name, plural, size
from rnfs.names import NameManager, RnsNetwork, parse_name
from rnfs.pin_names import PinNames
from rnfs.network import guards, rates
from rnfs.network.announces import DhtAnnounceHandler, DhtAnnouncer, parse_mode
from rnfs.network.client import PeerClient
from rnfs.network.identity import load_or_create_identity, parse_node_id
from rnfs.network.links import LinkPool
from rnfs.network.server import RequestServer
from rnfs.publisher import Publisher

SAVE_INTERVAL = 300.0  # seconds between saves of provider records
STATE_SAVE_INTERVAL = 30.0  # seconds between saves of cache records, fetched targets and invalid roots, when changed
ARCHIVE_BLOCK_OVERHEAD = 64  # bytes an archive spends on each block beyond the block (msgpack framing, CID)
EVICT_INTERVAL = 60.0  # seconds between max_storage checks when nothing else triggers one
GATEWAY_NAME_AGE = 60.0  # a gateway resolution reuses a record heard this recently
STOP_TIMEOUT = 5.0  # seconds stop() waits in all for the threads it ends (the features' and the node's)
REMOTE_PIN_QUEUE = 64  # remote pins one source may have waiting or being fetched; more are refused
BOOTSTRAP_ANSWERS = 2  # a join stops contacting saved servers and bootstrap nodes once this many have answered
SAVED_JOIN_TRIES = 6  # servers of the saved routing table a join asks at most, before bootstrap nodes
MIN_SERVERS = 3  # while fewer live servers are known, the join runs again
REJOIN_DELAY = 300.0  # seconds before joining again; doubles after each join, up to announce_interval
DENY_RETRY = 60.0  # seconds before maintenance applies a deny-list change again after applying it failed
HASH_NOTICES = 1024  # files whose denied-hash NOTICE was logged, kept so it isn't repeated (then forgotten)
# Resolved targets found not to be sealed files, so reading one again needn't read its first bytes to tell (a CID's
# content never changes); least recently used forgotten first, about 2 MB when full
NOT_SEALED_MEMO = 4096
# Complete pins whose pin_ls type, name from inside and sealed status are remembered (about 3 MB when full); pins
# past it are read each time
PIN_MEMO = 16384
_UNOPENED = object()  # _read's opened when nothing has been opened yet


def import_fetched(value: list, tx) -> int:
    """Import fetched.json (statedb.migrate): its targets replace the fetched table; one that doesn't parse is
    skipped, as rnfs 0.1.4 did. Returns the number imported."""
    tx.execute("DELETE FROM fetched")
    cids = set()
    for text in value:
        try:
            cids.add(Cid.parse(text))
        except (ValueError, TypeError):
            continue
    tx.executemany("INSERT INTO fetched (cid) VALUES (?)", [(cid.binary,) for cid in cids])
    return len(cids)


def import_remote_pins(value: list, tx) -> int:
    """Import remote_pins.json (statedb.migrate): its [source, cid] pairs, in order, replace the remote_pins
    table; malformed ones are counted in a warning and skipped, as rnfs 0.1.4 did. Which sources are still
    allowed, duplicates and REMOTE_PIN_QUEUE are left to the load (_load_remote_pins). Returns the number
    imported."""
    tx.execute("DELETE FROM remote_pins")
    count = malformed = 0
    for entry in value:
        try:
            source, text = entry
            if not isinstance(source, str) or not isinstance(text, str):
                raise TypeError("not a pair of strings")  # a list or dict source can't even be looked up
            tx.execute("INSERT INTO remote_pins (position, source, cid) VALUES (?, ?, ?)",
                       (count, source, Cid.parse(text).binary))
        except (ValueError, TypeError):
            malformed += 1
            continue
        count += 1
    if malformed:
        log(f"skipped {malformed} malformed entr{'y' if malformed == 1 else 'ies'} in remote_pins.json", LOG_WARNING)
    return count


# The node's subdirectories of the rnfs dir (Config.state_dir, blocks_dir, identity_path's, names_dir; LXMF's
# ratchets), made private at each start. Not files/, the shared folder, whose contents the user chooses
PRIVATE_SUBDIRS = ("state", "blocks", "storage", "names", "lxmf")
_DIRECTORY_FLAGS = (os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | fsutil.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NONBLOCK", 0))


def make_private_root(root: str):
    """Create the rnfs dir root readable by the owner only (0700; on Windows, a protected access list for the
    user and SYSTEM from the moment it is created, see winacl), or check and tighten an existing one.

    The first step of a start, before the settings in it are read (then _make_private_dirs, then the lock). It
    holds the node's identity, name keys, settings and the local API token, so other local users must not read
    or change it; a root left group- or world-accessible by earlier versions is tightened. Raises RnfsError if it
    belongs to another user or can't be made private: its owner could swap the identity or plant a config there.
    On Windows also if it is a junction or symlink, or new but not empty.
    """
    if fsutil.IS_WINDOWS:
        # No Unix permission bits there (st_mode always shows others allowed): an explicit access list
        winacl.secure_rnfs_dir(root)
        return
    os.makedirs(root, mode=0o700, exist_ok=True)
    info = os.stat(root)
    if info.st_uid != os.geteuid():
        raise RnfsError(f"{root} belongs to another user (uid {info.st_uid}); the rnfs dir must be your own")
    mode = stat.S_IMODE(info.st_mode)
    if mode & 0o077:
        try:
            os.chmod(root, 0o700)
        except OSError as e:
            raise RnfsError(f"couldn't make {root} private to its owner (mode {mode:o}): {e}") from None
        log(f"made {root} private to its owner (it had mode {mode:o})", LOG_NOTICE)


def _make_private_dirs(root: str):
    """Make the rnfs dir root private (make_private_root), then each of its PRIVATE_SUBDIRS: created 0700 if
    missing (on Windows with the private access list), else checked and tightened (audit API-1).

    Tightening the root later doesn't take back a subdirectory from a user who could once write in a loose rnfs
    dir: they may own one they created, or hold a descriptor on it, and plant blocks (trusted by their length)
    or name keys. So on POSIX each is opened without following a link and checked through that descriptor: a
    symlink, anything but a directory or one of another user's stops the start; a loose one is made 0700 with a
    notice, and if others could write to it, entries in it owned by another user are reported (a warning: they
    may be planted). Raises RnfsError for any of those, or if the root can't be made private.
    """
    if fsutil.IS_WINDOWS:
        winacl.secure_rnfs_dir(root, tuple(os.path.join(root, name) for name in PRIVATE_SUBDIRS))
        return
    make_private_root(root)
    for name in PRIVATE_SUBDIRS:
        _make_private_subdir(os.path.join(root, name))


def _make_private_subdir(path: str):
    """_make_private_dirs on POSIX for one subdirectory at path. Raises RnfsError."""
    try:
        fd = _open_subdir(path)
    except OSError as e:
        # O_NOFOLLOW's answer for a symlink is ELOOP (EMLINK on FreeBSD), or with O_DIRECTORY on Linux ENOTDIR
        if e.errno not in (errno.ELOOP, errno.EMLINK, errno.ENOTDIR):
            raise RnfsError(f"can't use {path}: {e}") from None
        try:
            link = fsutil.is_link(os.lstat(path))
        except OSError:
            link = False
        what = "a symlink" if link else "not a directory"
        raise RnfsError(f"{path} is {what}; rnfs uses only a real directory of your own there. Move it away (rnfs "
                        f"creates a new one)") from None
    try:
        info = os.fstat(fd)
        mode = stat.S_IMODE(info.st_mode)
        fsutil.check_owned_fd(path, fd, info, "the rnfs dir's subdirectory")
    except OSError as e:
        raise RnfsError(f"{e}. rnfs uses only directories of your own there: move it away, or as root give it to "
                        f"yourself (chown -R {os.geteuid()} {path})") from None
    finally:
        os.close(fd)
    if mode & 0o077:
        log(f"made {path} private to its owner (it had mode {mode:o})", LOG_NOTICE)
        if mode & 0o022:
            _warn_foreign_entries(path)


def _open_subdir(path: str) -> int:
    """A descriptor of the directory at path, not following a link (created 0700 if missing). Raises OSError:
    ELOOP, EMLINK or ENOTDIR for a symlink, ENOTDIR for anything else that isn't a directory."""
    try:
        fd = os.open(path, _DIRECTORY_FLAGS)
    except FileNotFoundError:
        try:
            os.mkdir(path, 0o700)
        except FileExistsError:
            pass  # made meanwhile, by another start: checked like any other
        fd = os.open(path, _DIRECTORY_FLAGS)
    if not stat.S_ISDIR(os.fstat(fd).st_mode):  # where O_DIRECTORY is missing
        os.close(fd)
        raise NotADirectoryError(errno.ENOTDIR, "not a directory", path)
    return fd


def _warn_foreign_entries(directory: str):
    """Warn about entries below directory, which others could write to until now, that another user owns: they
    may have been planted (in blocks/, a raw block is trusted by its length). One lstat pass, links not followed;
    nothing is removed."""
    uid = os.geteuid()
    count, first = 0, None
    for parent, directories, files in os.walk(directory):
        for name in directories + files:
            entry = os.path.join(parent, name)
            try:
                if os.lstat(entry).st_uid == uid:
                    continue
            except OSError:
                continue  # gone meanwhile
            count += 1
            first = first or entry
    if count:
        log(f"{directory} was writable by other users, and {count} entr{'y' if count == 1 else 'ies'} in it "
            f"belong to another user (e.g. {first}): they may have been put there to mislead rnfs. Check them, or "
            f"delete them (rnfs fetches blocks again)", LOG_WARNING)


class DirInUse(RnfsError):
    """Another daemon or embedded node holds the rnfs dir's lock; holder is its process ID, if known."""

    def __init__(self, message: str, holder: int | None):
        """The error, naming holder."""
        super().__init__(message)
        self.holder = holder


def lock_rnfs_dir(config: Config) -> int:
    """Make the rnfs dir and its subdirectories private (as the node would) and lock <rnfs dir>/daemon.lock
    without waiting, before anything else uses the dir (the last steps of a start: make_private_root, then the
    settings, come first): two nodes on one dir would load and save the same state and announce the same
    node ID. The open descriptor, whose lock lasts until it is closed or the process ends. Raises DirInUse if
    another process, or another node in this one, holds it; RnfsError or OSError if the dir can't be made
    private or the file locked. The daemon and rnfs.Node both lock through here."""
    _make_private_dirs(config.root)
    path = os.path.join(config.root, LOCK_FILE)
    try:
        return fsutil.lock_exclusive(path)
    except BlockingIOError:
        holder = fsutil.lock_holder(path)
        raise DirInUse(f"another rnfs daemon or node{f' (process {holder})' if holder else ''} is using "
                       f"{config.root}; stop it first, or use another rnfs dir", holder) from None


def _check_free_space(path: str, needed: int, blocks_dir: str | None = None, to_fetch: int = 0):
    """Raise TooLarge unless writing needed bytes to path leaves the disk reserve free on its filesystem.

    If blocks_dir (where a fetch stores its blocks) is on the same filesystem, the to_fetch bytes still to be
    fetched there before the write count too: the fetch keeps the reserve, and the write-out must as well.
    Raises OSError if the free space can't be read (a missing directory, say): unchecked, the write could fill
    the disk.
    """
    directory = os.path.dirname(os.path.abspath(path))
    try:
        usage = shutil.disk_usage(directory)
        if blocks_dir is not None and to_fetch > 0 and os.stat(directory).st_dev == os.stat(blocks_dir).st_dev:
            needed += to_fetch
    except OSError as e:
        # The same kind of error (FileNotFoundError, PermissionError...), saying what couldn't be done
        message = f"can't check the free space for {directory}: {e.strerror or e}"
        raise (OSError(e.errno, message) if e.errno is not None else OSError(message)) from None
    reserve = disk_reserve(usage.total)
    if needed > usage.free - reserve:
        raise TooLarge(f"{size(needed)} won't fit in the {size(usage.free)} free in {directory} "
                       f"while leaving {size(reserve)} free")


def _remove_partial(path: str, identity: walker.Identity | None = None):
    """Delete a partial get output (file or directory tree) if there is one; a symlink is removed, not followed.

    With identity (a directory output's, from write_out), only if path is still that directory: whatever someone
    put in its place isn't this get's to delete."""
    if identity is not None:
        try:
            walker.check_unchanged(path, identity)
        except (ValueError, OSError):
            return
    try:
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        elif os.path.lexists(path):
            fsutil.remove(path)
    except OSError as e:
        log(f"removing {path} failed: {e!r}", LOG_WARNING)


def _inherit_from_parent(partial: str, output: str):
    """On Windows, give a complete directory output the access its folder passes on, where it was private to the
    user while written (walker._PathOutput). If that fails it stays private, which is safe, so a warning is
    logged rather than the get failed."""
    try:
        winacl.inherit_from_parent(partial)
    except OSError as e:
        log(f"get: {output} stays private to you, as its folder's access list couldn't be applied to it: {e}",
            LOG_WARNING)


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
    # A check, not a contact: don't mark the destination used (an RPC each on a shared instance)
    return RNS.Identity.recall(node_id, _no_use=True) is not None


def _warn_unused_reticulum_config(config: Config):
    """Warn about a Reticulum config left in the rnfs dir by earlier versions, which used it as RNS's configdir."""
    leftover = os.path.join(config.root, "config")
    in_use = os.path.realpath(RNS.Reticulum.configdir) == os.path.realpath(config.root)
    if os.path.isfile(leftover) and not in_use:
        log(f"ignoring the Reticulum config in {leftover}: Reticulum now uses {RNS.Reticulum.configdir}. "
            f"Set reticulum_dir (or --rnsconfig) to {config.root} to keep using it, or delete it", LOG_WARNING)


class Store(Protocol):
    """A part of the node that keeps state in the state database; the node lists every one (RnfsNode._add_store).

    save() writes what changed since the last save, if anything; a store written through has nothing to save. It
    raises OSError (StateDbError) if the database fails, keeping the change for the next save. close() releases
    the store's reference to the state database, once everything is saved; the last reference closes it.
    """

    def save(self) -> None:
        """Write the changes not yet saved, if any."""

    def close(self) -> None:
        """Release the state database."""


_StoreT = TypeVar("_StoreT", bound=Store)


class _StoreParts:
    """A Store made of two functions: for a store that has no save (written through) or no close of its own, or for
    the node's own tables."""

    def __init__(self, save: Callable[[], None] = lambda: None, close: Callable[[], None] = lambda: None):
        """A store whose save and close call save and close."""
        self.save = save
        self.close = close


class _PinDetails(NamedTuple):
    """What pin_ls reads from a pin's stored blocks (RnfsNode._pin_details): None for what they can't tell."""

    kind: str | None  # "file" or "directory"
    inside: str | None  # a directory's one entry's name, or "(directory: N entries)"
    sealed: bool | None  # a sealed file
    entries: int | None  # a directory's entry count
    size: int | None  # content bytes, as the root block states them


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
        """Providers of key (oldest half plus a random half, banned left out, and in private mode non-members)
        and the closest servers."""
        routing = self.node.routing
        providers = self.node.records.for_reply(key, exclude=self.node.excluded_provider)
        nodes = [n for n in routing.closest(key, for_reply=True) if n != self.node.node_id]
        return providers, nodes

    def provide(self, publisher_id: bytes, keys: list[bytes], withdrawn: list[bytes]) -> bool:
        """Record publisher_id for keys; True only if every key was recorded. Banned publishers are ignored."""
        if not self.is_dht_server() or self.node.routing.is_banned(publisher_id):
            return False
        if self.node.config.private:
            # The request server let it through as a member (private mode checks every handler's requester)
            self.node._member_publishers.add(publisher_id)
        return self.node.records.provide(publisher_id, keys, withdrawn) == len(keys)

    def served_block(self, cid: Cid) -> bytes | None:
        """cid's block if it's in the DAG of a provided CID, re-checked so a damaged block isn't sent. None for a
        denied one, as for any block not held: the answer's format is unchanged."""
        if self.node.deny.denies(cid) or not self.node.publisher.serves(cid):
            return None
        block = self.node.storage.get(cid, verify=True)
        if block is None:
            # It was damaged and has been deleted: what reached it is no longer complete
            self.node.refresh_soon()
        return block

    def served_block_size(self, cid: Cid) -> int | None:
        """The stored length of the block served_block(cid) would return, or None if it isn't served."""
        if self.node.deny.denies(cid) or not self.node.publisher.serves(cid):
            return None
        return self.node.storage.block_size(cid)

    def remote_pin(self, identity_hash: bytes, cid: Cid, pin: bool) -> bool:
        """Accept a /pin from an allowed identity (the server checked remote_pin_identities)."""
        return self.node.remote_pin(identity_hash, cid, pin)

    def is_member(self, identity_hash: bytes) -> bool:
        """True if the identity is on the members list (private mode)."""
        return identity_hash.hex() in self.node.members

    def feature_requests(self) -> dict:
        """Request paths added by features (Feature.requests): {path: handler(data, remote_identity)}, as checked
        when the node loaded them."""
        return dict(self.node._feature_requests)


class RnfsNode:
    """A running node. Only one per process: RNS.Reticulum is a singleton, and the guards are installed once
    (network/guards.py)."""

    def __init__(self, config: Config, reticulum: "RNS.Reticulum | None" = None, embedded: bool = False):
        """Start every service of the node, then announce, join and publish in the background.

        The daemon starts Reticulum here. An embedded node (rnfs.Node, embedded) uses reticulum, the host
        program's running instance, or starts one if there is none; its guards cover only rnfs.dht links.
        """
        config.validate()
        _make_private_dirs(config.root)  # before Reticulum and the identity, which write under it
        self.embedded = embedded
        # Reject oversized, compressed and request Resource advertisements before RNS allocates for them; in the
        # daemon on every link, with RNS's compression off process-wide (section 3)
        guards.install(guards.DHT if embedded else guards.PROCESS)
        if reticulum is None and embedded:
            reticulum = RNS.Reticulum.get_instance()
        # By default this is RNS's own config directory, the one rnsd uses: attaching to a shared instance needs
        # its RPC key, which RNS derives from the transport identity stored in that directory (section 3)
        self.reticulum = reticulum if reticulum is not None else \
            RNS.Reticulum(configdir=config.reticulum_root, verbosity=config.verbosity or None)
        log(f"Reticulum config in {RNS.Reticulum.configdir}"
            + (", attached to a shared instance" if self.reticulum.is_connected_to_shared_instance else ""))
        _warn_unused_reticulum_config(config)
        self.identity = load_or_create_identity(config.identity_path)
        self.destination = RNS.Destination(self.identity, RNS.Destination.IN, RNS.Destination.SINGLE,
                                           APP_NAME, DHT_ASPECT)
        slow = rates.is_slow_node(config, self.reticulum)
        self.links = LinkPool(self.identity, config)
        self.server = self.announcer = self.announce_handler = None  # made below; stop() skips those not made
        try:
            self._setup_or_close(config, bytes(self.destination.hash), PeerClient(self.links, config), slow,
                                 names_network=None, is_known=_rns_is_known)
        except BaseException:
            if embedded:
                self._release_destination()  # the host's Reticulum runs on: a later node registers it again
            raise
        try:
            self._start_network(config, slow)
        except BaseException:
            if embedded:
                self.stop()  # nothing of this node is left in the host's process (the daemon exits instead)
            raise

    def _start_network(self, config: Config, slow: bool):
        """The services that need Reticulum, once the node is set up: paths, the request server, announces, names
        and the background threads."""
        # mesh: hops and next-hop interfaces rank providers; one set of traffic counters for client and server
        self.paths = RnsPaths(self.reticulum)
        self.fetcher.path_info = self.paths.info
        self.fetcher.path_infos = self.paths.infos

        self.server = RequestServer(self.destination, config, self.backend, traffic=self.client.traffic)
        self.announcer = DhtAnnouncer(self.destination, config)
        self.announce_handler = DhtAnnounceHandler(self._heard_server, self._heard_client, self.routing.is_banned,
                                                   self.node_id, on_announced=self._announced)
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
        node.embedded = False
        node._setup_or_close(config, node_id, client, slow, names_network=names_network,
                             is_known=lambda node_id: True)
        node.joined.set()
        return node

    def _setup_or_close(self, *args, **kwargs):
        """_setup, releasing the state database references taken so far if it raises (a store refusing to
        start), so a caller in the same process can try again or remove the directory."""
        try:
            self._setup(*args, **kwargs)
        except BaseException:
            self._close_state()
            raise

    def _setup(self, config: Config, node_id: bytes, client, slow: bool, names_network, is_known):
        """Everything that doesn't need Reticulum itself."""
        self.config = config
        self.node_id = node_id
        self.client = client
        # What changed, for local clients (GET /api/events, rnfs.events); first, so every part may publish
        self.events = events.EventLog()
        self.slow = slow
        self._is_known = is_known
        self.members = config.member_set
        # Private mode: node IDs that sent /provide as a member this run (their identity was checked then)
        self._member_publishers: set[bytes] = set()
        self._bootstrap_nodes: list[bytes] = []  # bootstrap_nodes that parse, in the order given
        for text in config.bootstrap_nodes:
            try:
                self._bootstrap_nodes.append(parse_node_id(text))
            except ValueError as e:
                log(f"ignoring bootstrap node: {e}", LOG_WARNING)
        _make_private_dirs(config.root)
        # Every store, in the order made, and those _save_state saves (the others save on a schedule of their own).
        # A store is added as soon as it is made, so a _setup that fails releases exactly the ones it made. The
        # features are stores too, listed by self.features (_every_store)
        self._stores: list[Store] = []
        self._batched: list[Store] = []
        # First, so the JSON state files of earlier versions are imported before any store loads its state; closed
        # last
        self._db = statedb.connect(config.state_dir)
        self._add_store(_StoreParts(close=self._db.close), batched=False)

        self.routing = RoutingTable(node_id, config, admit=self._is_member_node if config.private else None)
        # Loaded from the state database, so a restart rejoins from the servers it knew; saved with the batched stores
        self._add_store(RoutingState(self.routing, config.state_dir))
        # Local scores (section 6): bootstrap nodes seeded; saved with the batched stores
        self.scores = self._add_store(Scores(config.state_dir, seeded=self._bootstrap_set()))
        # Saved with their expiry, every SAVE_INTERVAL (maintain): they change with every /provide
        self.records = self._add_store(ProviderStore(config.max_records, config.state_dir, own_id=node_id,
                                                     seniority=config.provider_seniority), batched=False)
        if config.private:
            # Records stored while the node was public, or from a member removed since, are never handed out
            removed = self.records.retain(self._is_member_publisher)
            if removed:
                log(f"private mode: dropped {plural(removed, 'provider record')} of non-members", LOG_NOTICE)
        self.storage = self._add_store(Storage(config.blocks_dir, config.state_dir, config.max_storage))
        self.storage.pin_listener = self._pin_event
        self.storage.cache_listener = self._cache_event
        # Before anything is fetched or provided: the fetcher and the publisher check it through storage.deny
        self.deny = self._add_store(denylist.DenyList(config.state_dir))
        self.storage.deny = self.deny
        self.deny.reload(self._read_deny_list)
        self._hash_noticed: set[Cid] = set()  # files _check_file refused by hash and logged the NOTICE for
        # _open_sealed's memo of targets that aren't sealed files (NOT_SEALED_MEMO), and the deny list's CIDs it
        # was filled under: reading the first bytes also checks the deny list, so a change to it empties the memo
        self._not_sealed: OrderedDict[walker.Expect, None] = OrderedDict()
        self._not_sealed_deny: frozenset[bytes] | None = None
        self._not_sealed_lock = threading.Lock()
        # pin_ls's memo: pinned CID -> its _PinDetails, for complete pins only (_pin_details)
        self._pin_memo: dict[Cid, _PinDetails] = {}
        self._pin_memo_lock = threading.Lock()
        # The shared folder is written through: nothing to save. Its per-file cache (what the files inside its entries
        # were built as) is saved with the batched stores; the shared folder's close releases it too
        self.shared_folder = SharedFolder(config.files_dir, self.storage, config.state_dir, rnfs_dir=config.root)
        self._add_store(_StoreParts(close=self.shared_folder.close), batched=False)
        self._add_store(self.shared_folder.files)
        # Where pins came from, for pin ls (local only). Written through, not durably: a power loss can lose a
        # name, never the pin it names, which is committed durably first; its save writes a name whose write failed
        self.pin_names = self._add_store(PinNames(config.state_dir), batched=False)
        self.pin_names.prune_labels(self.storage.pinned_cids())
        self.fetcher = self._add_store(Fetcher(self.storage, client, config, node_id, self.routing, self._lookup,
                                               slow, config.state_dir, after_store=self._enforce_storage,
                                               make_room=self._make_room, scores=self.scores))
        # Saved by its own refreshes and publish loop
        self.publisher = self._add_store(Publisher(node_id, config, self.storage, self.records, client, self._lookup,
                                                   slow, config.state_dir, cached_eligible=self._eligible_targets,
                                                   generation=self._provide_generation), batched=False)
        # Saves its state itself, and at stop (names.stop)
        self.names = NameManager(config, _NamePins(self), network=names_network)
        self.names.held_listener = self._name_event
        self._add_store(_StoreParts(close=self.names.close), batched=False)
        self.backend = NodeBackend(self)

        self._lock = threading.Lock()
        self._fetched: set[Cid] = set()
        self._fetched_changes = 0  # incremented whenever _fetched changes (_provide_generation)
        self._fetched_saved: set = set()  # what the fetched table holds
        for (binary,) in self._db.query("SELECT cid FROM fetched"):
            self._fetched_saved.add(binary)
            try:
                self._fetched.add(Cid.from_bytes(binary))
            except ValueError:
                continue
        # Changed since last saved (by _save_state: maintenance, gc and stop); a row that didn't parse is deleted
        self._fetched_dirty = len(self._fetched) != len(self._fetched_saved)
        self._fetched_save_lock = threading.Lock()  # one save at a time, so an older snapshot never lands last
        self._add_store(_StoreParts(save=self._save_fetched))  # closed with self._db
        # Remote source -> CIDs waiting or being fetched for it, in the order accepted (fetched one at a time)
        self._remote_reserved: dict[str, dict[Cid, None]] = {}
        self._remote_released: set[tuple[str, Cid]] = set()  # remote pins released while still being fetched
        # Remote source -> the thread fetching its pins (one at most); stop waits for them
        self._remote_workers: dict[str, threading.Thread] = {}
        self._remote_pins_dirty = False  # changed since last saved (by maintenance, and at stop)
        self._remote_pins_save_lock = threading.Lock()  # one save at a time, so an older snapshot never lands last
        # Saved about every second (maintain); closed with self._db
        self._add_store(_StoreParts(save=self._save_remote_pins), batched=False)
        self._refresh_wanted = threading.Event()
        # Taken first (before the publisher's and storage's locks): one deny-list change applied at a time
        self._deny_lock = threading.Lock()
        self._deny_applied: frozenset[bytes] | None = None  # the denied CIDs _apply_deny last applied
        self._deny_retry_at = 0.0  # monotonic time before which maintenance doesn't retry a failed _apply_deny
        self.stop_event = threading.Event()
        self.joined = threading.Event()
        self.threads: list[threading.Thread] = []
        self._stopped = False
        self.features: dict[str, features.Feature] = {}
        # In order, each made before the next, which may use it (requires); a feature that can't be loaded as it
        # declares stops the node here
        for cls in features.checked_classes():
            self.features[cls.name] = cls(self)
        # Their request paths, checked now: one rnfs serves itself, or two features adding the same, stops the node
        self._feature_requests = features.request_handlers(self.features.values())
        # Releasing a deleted name key's pin recomputes what is provided, so it waits for the features too; and
        # name pins are made to match the names state, which a crash inside a publish can leave them disagreeing with
        self.names.reconcile_pins(self.storage.pins())
        # What is provided, before /blocks can be asked; after the features, whose keys (kept names) would
        # otherwise all be withdrawn at every start
        self.publisher.refresh()
        # Changes apply at once; what the list denied at start, maintenance applies first
        self.deny.add_listener(self._apply_deny)

    def _add_store(self, store: _StoreT, batched: bool = True) -> _StoreT:
        """Add store to the node's stores, which are saved at stop and closed by _close_state; returns it.

        batched: _save_state saves it (every STATE_SAVE_INTERVAL, and at gc); False for a store saved on a
        schedule of its own, or written through.
        """
        self._stores.append(store)
        if batched:
            self._batched.append(store)
        return store

    def _every_store(self, batched: bool = False) -> list[Store]:
        """The node's stores (only the batched ones if batched), in the order made, then the features (each saved
        with the batched stores). After a _setup that failed, only those it made."""
        stores = getattr(self, "_batched" if batched else "_stores", [])
        return [*stores, *getattr(self, "features", {}).values()]

    @overload
    def feature(self, name: Literal["contacts"]) -> "features.ContactsApi": ...
    @overload
    def feature(self, name: str) -> "features.Feature": ...

    def feature(self, name):
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
        self._resume_remote_pins()
        self._start_features()

    def _start_features(self):
        """Start every feature, in order. One whose start() raises is logged and the rest still start, where one
        whose constructor raises stopped the node from being made (_setup)."""
        for feature in self.features.values():
            try:
                feature.start()
            except Exception as e:  # noqa: BLE001 - one broken feature mustn't stop the node
                log(f"feature {feature.name} failed to start: {e!r}", LOG_ERROR)

    def stop(self, timeout: float = STOP_TIMEOUT):
        """Stop background work, save state and close links. Work still waiting on the network is abandoned.

        The features' threads and the node's are waited for at most timeout seconds in all. Each step is guarded:
        one that raises is logged and the rest still run, so the state is saved and closed, and an embedded
        node's destination released, whatever failed before.
        """
        if self._stopped:
            return
        self._stopped = True
        log("stopping", LOG_NOTICE)
        deadline = time.monotonic() + timeout
        steps: list[tuple[str, Callable[[], object]]] = [
            (f"stopping feature {feature.name}", partial(features.stop_feature, feature, deadline))
            for feature in reversed(list(self.features.values()))]
        steps += [("signalling the threads to stop", self.stop_event.set),
                  ("waking the publisher", self.publisher.wake)]
        if self.announcer is not None:
            steps.append(("stopping the announcer", self.announcer.stop))
        if self.announce_handler is not None:
            steps.append(("deregistering the announce handler", self.announce_handler.deregister))
        if self.server is not None:
            steps.append(("stopping the request server", self.server.stop))
        steps.append(("deregistering names, saving state", self.names.stop))  # names.stop saves the names state
        if self.links is not None:
            # Before the wait: a thread still connecting to a node that has gone (the publisher, say) is freed
            # now, and ends while the state database is open, rather than once it is closed
            steps.append(("closing links", self.links.close_all))
        steps.append(("waiting for the background threads", partial(self._join_threads, deadline)))
        steps += [("saving state", store.save) for store in self._every_store()]
        steps.append(("closing state", self._close_state))
        if self.links is not None:
            steps.append(("closing links opened since", self.links.close_all))
        if self.embedded:
            steps.append(("releasing the rnfs.dht destination", self._release_destination))
        for label, step in steps:
            try:
                step()
            except Exception as e:  # noqa: BLE001 - the later steps run: the state is closed, the destination released
                log(f"{label} failed: {e!r}", LOG_ERROR)

    def _join_threads(self, deadline: float):
        """Wait until deadline (a time.monotonic() value) for the background loops and the remote-pin workers to
        end, once stop_event is set."""
        with self._lock:
            workers = list(self._remote_workers.values())
        for thread in (*self.threads, *workers):
            thread.join(max(0.0, deadline - time.monotonic()))

    def _release_destination(self):
        """An embedded node's last step: RNS stops answering for its rnfs.dht destination and the links to it
        are closed, while the host program's Reticulum keeps running, so a node started later in the process
        can register it again. (The daemon's process ends with RNS.exit() instead.)"""
        try:
            RNS.Transport.deregister_destination(self.destination)
            for link in list(getattr(self.destination, "links", None) or []):
                link.teardown()
        except Exception as e:  # noqa: BLE001 - stopping goes on
            log(f"releasing the rnfs.dht destination failed: {e!r}", LOG_ERROR)

    def _close_state(self):
        """Release every store's reference to the state database, once everything is saved, in the reverse of the
        order made (the node's own reference last, which closes it). After a _setup that failed, only the stores
        it made."""
        for store in reversed(self._every_store()):
            try:
                store.close()
            except Exception as e:  # noqa: BLE001 - release the rest
                log(f"closing state failed: {e!r}", LOG_ERROR)

    def _save_state(self):
        """Save what changed in the batched stores (cache records, fetched targets, invalid roots, remembered file
        hashes and the features' state: each change isn't written at once). A failing save is logged, and the rest
        are still saved."""
        for store in self._every_store(batched=True):
            try:
                store.save()
            except OSError as e:
                log(f"saving state failed: {e!r}", LOG_ERROR)

    # DHT wiring

    def _lookup(self, key: bytes) -> Lookup:
        """A lookup for key starting from the routing table, counting answers and failures in it.

        It starts from the closest nodes that haven't failed since last heard or gone silent (those a /find
        reply would give), with others only to make up K, so departed nodes don't take its first queries;
        unreliable ones (queries failing however often they announce) only if still short of K.
        """
        fresh = self.routing.closest(key, 2 * K, for_reply=True)
        nearest = fresh + [n for n in self.routing.closest(key, 2 * K) if n not in fresh]
        reliable = [n for n in nearest if not self.routing.unreliable(n)]
        start = (reliable + [n for n in nearest if n not in reliable])[:K]
        return self._new_lookup(key, start, self.routing.is_banned)

    def _new_lookup(self, key: bytes, start: list[bytes], exclude: Callable[[bytes], bool]) -> Lookup:
        """A Lookup for key from start, wired to the routing table (answers, failures, unreliable nodes and
        retries) and the client, leaving out the nodes exclude rejects: _lookup's and the join's."""
        return Lookup(key, start, self.client.find, self.node_id,
                      alpha=self.config.effective_alpha(self.slow), beta=self.config.beta,
                      timeout=self.config.lookup_timeout, is_known=self._is_known,
                      on_answer=self._answered, on_failure=self._query_failed, exclude=exclude,
                      report=lambda text: log(text, LOG_DEBUG), unreliable=self.routing.unreliable,
                      may_retry=self.routing.may_retry, on_useful=self.scores.useful_answer)

    def _answered(self, node_id: bytes):
        """A node answered /find, proving it is a reachable server."""
        if self.routing.answered(node_id):
            self.publisher.wake()  # a CID that found no nodes may now be publishable

    def _query_failed(self, node_id: bytes):
        """A /find to node_id went unanswered: counted in the routing table, and lowering its score."""
        self.routing.failed(node_id)
        self.scores.failed(node_id)

    def _pin_event(self, change: str, cids: list[Cid], source: str):
        """Storage's pin_listener (storage lock held): pins of source added or removed, as a "pin" event; a CID
        no source pins any more loses its label (pin_names' lock is taken under storage's, never the other way)."""
        pin_names = getattr(self, "pin_names", None)  # made after storage; prune_labels covers the gap
        if change == "removed" and pin_names is not None:
            for cid in cids:
                if not self.storage.is_pinned(cid):
                    pin_names.forget_label(cid)
        self.events.publish("pin", change=change, source=source, **events.cid_fields(cids))

    def _cache_event(self, change: str, cids: list[Cid]):
        """Storage's cache_listener (storage lock held): cached fetches added or removed, as a "cache" event."""
        self.events.publish("cache", change=change, **events.cid_fields(cids))

    def _name_event(self, name: bytes, record):
        """NameManager's held_listener (names lock held): a newer record of a name this node holds (its own, kept,
        followed or resolved) or watches, as a "name" event."""
        self.events.publish("name", name=name.hex(), cid=str(record.cid), sequence=record.sequence)

    def _heard_server(self, node_id: bytes):
        """A server-mode announce (or path response) from node_id."""
        self._count_heard(node_id, "server")
        if self.routing.heard(node_id):
            log(f"heard DHT server {node_name(node_id)} ({self.routing.node_count()} known)", LOG_VERBOSE)
            self.publisher.wake()

    def _heard_client(self, node_id: bytes):
        """node_id announces client mode: it doesn't belong in the routing table."""
        self._count_heard(node_id, "client")
        self.routing.remove(node_id)

    def _count_heard(self, node_id: bytes, mode: str):
        """Count node_id's announce in the traffic counters, which `rnfs peers` reads its DHT mode and last-heard
        time from (client-mode nodes never enter the routing table). Done here rather than by a second rnfs.dht
        announce handler in the peers feature: RNS starts a thread per handler for every announce."""
        traffic = getattr(self.client, "traffic", None)
        if traffic is not None:
            traffic.heard(node_id, mode)

    def _announced(self, node_id: bytes):
        """node_id's own announce (not one RNS marks as a path response) arrived: Traffic.announced counts it as a
        sign it is up, which `rnfs peers` reads its online state from (mesh.online), if it was emitted recently.
        Attached to a shared instance, path answers come as announces too, and an announce queue can hold one for
        hours, so only its emission time tells.

        That is the newest emission in node_id's path (RnsNetwork.emitted_time: RNS internal, None if it can't be
        read, and then the announce doesn't count), which is this announce's: RNS calls announce handlers only for
        an announce that takes the path, which needs one emitted later than the path's newest (or an expired path:
        then the newest may be an earlier announce's, which counts only if no later one was). An in-memory read: no
        disk, unlike the
        announce cache, which a shared instance's client doesn't write and whose packet hash a path answer passed on
        as an announce doesn't match."""
        traffic = getattr(self.client, "traffic", None)
        if traffic is not None:
            traffic.announced(node_id, RnsNetwork().emitted_time(node_id))

    def _is_member_node(self, node_id: bytes) -> bool:
        """Private mode's routing table admission: node_id's identity is a member."""
        # Runs on every admission: a check, not a contact, so it doesn't mark the destination used
        identity = RNS.Identity.recall(node_id, _no_use=True)
        return identity is not None and identity.hash.hex() in self.members

    def _is_member_publisher(self, node_id: bytes) -> bool:
        """Private mode: True if node_id's provider records may be kept and handed out (this node, a node that
        provided as a member this run, or one whose identity RNS knows to be a member)."""
        return node_id == self.node_id or node_id in self._member_publishers or self._is_member_node(node_id)

    def excluded_provider(self, node_id: bytes) -> bool:
        """True if a provider must be left out of /find replies and providers(): banned, or in private mode
        not a member."""
        if self.routing.is_banned(node_id):
            return True
        return self.config.private and not self._is_member_publisher(node_id)

    def _seed_routing(self):
        """Add servers whose announces RNS heard (and cached) within MISSED_ANNOUNCES (2) × announce_interval
        (the routing table's silence limit), before a restart.

        RNS internal (Identity.known_destinations: hash -> [time heard, packet hash, public key, app_data, ...]);
        skipped if it isn't there. Each is checked to be an rnfs.dht destination of the announced key.
        """
        try:
            entries = list(RNS.Identity.known_destinations.items())
        except Exception:  # noqa: BLE001 - RNS internals: no seeding if they fail
            return
        horizon = time.time() - MISSED_ANNOUNCES * self.config.announce_interval
        added = 0
        for destination_hash, entry in entries:
            try:
                if entry[0] < horizon or parse_mode(entry[3]) != MODE_SERVER:
                    continue
                identity = RNS.Identity(create_keys=False)
                identity.load_public_key(entry[2])
                if RNS.Destination.hash(identity, APP_NAME, DHT_ASPECT) == destination_hash:
                    added += self.routing.heard(bytes(destination_hash))
            except Exception:  # noqa: BLE001 - one bad entry is skipped
                continue
        if added:
            log(f"{plural(added, 'DHT server')} known from announces heard before", LOG_VERBOSE)

    def _join(self):
        """Join at startup, then again whenever fewer than MIN_SERVERS live servers are known (section 6).

        A join that leaves too few servers is retried after REJOIN_DELAY, doubling up to announce_interval:
        on radio, every path request for a bootstrap node is flooded.
        """
        try:
            if self.reticulum is not None:
                self._seed_routing()
            self.join()
        except Exception as e:  # noqa: BLE001 - joining is retried; logged
            log(f"joining the DHT failed: {e!r}", LOG_ERROR)
        finally:
            self.joined.set()
        delay = REJOIN_DELAY
        while not self.stop_event.wait(delay):
            if self.routing.live_count() >= MIN_SERVERS:
                delay = REJOIN_DELAY
                continue
            try:
                self.join()
            except Exception as e:  # noqa: BLE001 - joining is retried; logged
                log(f"joining the DHT failed: {e!r}", LOG_ERROR)
            delay = min(2 * delay, self.config.announce_interval)

    def join(self):
        """Contact saved servers, then bootstrap nodes, then look up our own ID from what they and the routing
        table know."""
        contacted, start = self._contact_bootstrap()
        start = start + self.routing.closest(self.node_id)
        if not start:
            log("no DHT servers known yet: waiting for announces", LOG_VERBOSE)
            return
        # The nodes contacted already answered (or failed) a /find for this key: not asked again
        lookup = self._new_lookup(self.node_id, start,
                                  lambda node_id: node_id in contacted or self.routing.is_banned(node_id))
        lookup.find_closest()
        log(f"joined the DHT: {plural(self.routing.node_count(), 'server')} known", LOG_NOTICE)

    def bootstrap_ids(self) -> list[bytes]:
        """Bootstrap nodes in the order a join contacts them: bootstrap_nodes as given, then (unless
        default_bootstrap is off or in private mode) DEFAULT_BOOTSTRAP_NODES in a fresh random order."""
        defaults = self._default_bootstrap()
        random.shuffle(defaults)
        ids = list(dict.fromkeys(self._bootstrap_nodes + defaults))
        return [node_id for node_id in ids if node_id != self.node_id and not self.routing.is_banned(node_id)]

    def _default_bootstrap(self) -> list[bytes]:
        """DEFAULT_BOOTSTRAP_NODES, in their order, unless default_bootstrap is off or in private mode."""
        if not self.config.default_bootstrap or self.config.private:
            return []
        return [bytes.fromhex(text) for text in DEFAULT_BOOTSTRAP_NODES]

    def _bootstrap_set(self) -> set[bytes]:
        """Every bootstrap node (the user's and the defaults in use) but this node: those seeded a score."""
        return set(self._bootstrap_nodes + self._default_bootstrap()) - {self.node_id}

    def join_candidates(self) -> list[bytes]:
        """The nodes a join asks for our own ID, in order (section 6, "Joining"): up to SAVED_JOIN_TRIES servers of
        the routing table, which is saved across restarts, that haven't failed a query since last heard, best
        scored first (local scores, by band) and longest known breaking ties; then the bootstrap nodes
        (bootstrap_ids) not among them. A node that has joined once is its own bootstrap list, so a restart doesn't
        flood path requests for bootstrap nodes on radio."""
        bands = self.scores.bands()
        saved = sorted(self.routing.join_order(), key=lambda node_id: -bands.get(node_id, 0))[:SAVED_JOIN_TRIES]
        return saved + [node_id for node_id in self.bootstrap_ids() if node_id not in saved]

    def _contact_bootstrap(self) -> tuple[set[bytes], list[bytes]]:
        """Ask the join's candidates (saved servers, then bootstrap nodes) for our own ID, one at a time, until
        BOOTSTRAP_ANSWERS have answered.

        Each costs a path request if its identity isn't known, outside any lookup's budget for unknown IDs, so an
        unreachable one doesn't stop the rest being tried. Returns the nodes contacted and the servers their
        answers listed.
        """
        contacted: set[bytes] = set()
        found: list[bytes] = []
        answered = 0
        for node_id in self.join_candidates():
            if answered >= BOOTSTRAP_ANSWERS or self.stop_event.is_set():
                break
            contacted.add(node_id)
            try:
                reply = self.client.find(node_id, self.node_id)
            except Exception:  # noqa: BLE001 - an unanswering bootstrap node is skipped
                reply = None
            if reply is None:
                log(f"joining: {node_name(node_id)} didn't answer", LOG_VERBOSE)
                self._query_failed(node_id)
                continue
            answered += 1
            self._answered(node_id)
            found.extend(reply[1])
        return contacted, found

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
        before = self.storage.cache_generation()
        if self.storage.evict(nbytes):
            self.publisher.refresh()
        if self.storage.cache_generation() != before:  # else no cached target went, and there's nothing to prune
            self._prune_fetched()

    def maintain(self, now: float, last: dict) -> None:
        """One maintenance pass: shared folder, provider records, storage. last holds when each ran."""
        if now - last.get("scan", float("-inf")) >= self.config.scan_interval:
            last["scan"] = now
            try:
                self.shared_folder.scan(builder.BuildOptions(check=self._hash_check()))
                self.publisher.refresh()
            except Exception as e:  # noqa: BLE001 - the next scan tries again; logged
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
        if self.deny.cids is not self._deny_applied and time.monotonic() >= self._deny_retry_at:
            self._apply_deny()
        self._save_remote_pins()

    # Deny list (rnfs/denylist.py; its sources: rnfs/features/deny.py)

    def _read_deny_list(self, cid: Cid) -> bytes | None:
        """A deny list's bytes from storage (None if it isn't wholly stored). Raises ValueError for one over
        denylist.MAX_LIST_BYTES, or that is a directory."""
        if not self.storage.is_complete(cid):
            return None
        expect = walker.root_expect(cid)
        get = self.storage.getter()
        if walker.size_of(expect, get(expect)) > denylist.MAX_LIST_BYTES:
            raise ValueError(f"it is over {denylist.MAX_LIST_BYTES // 1024} KiB")
        return b"".join(walker.read_range(expect, get))

    def _hash_check(self):
        """The check of an add's or the shared folder's builder.BuildOptions: raises Denied for a file whose content
        hash is denied; None while the deny list has no file hashes (so nothing is hashed)."""
        if not self.deny.hashes:
            return None

        def check(digest: bytes):
            """Raise Denied if a file with this content hash is denied."""
            if self.deny.denies_hash(digest):
                raise Denied(f"a file with SHA-256 {digest.hex()} is on this node's deny list")
        return check

    def _check_file(self, expect: walker.Expect, get: walker.Get):
        """Raise Denied if file expect's content hashes to a deny-list entry, before any of it is written out or
        sent. get serves its whole DAG, stored and checked (fetch_dag's, or a complete root's); its bytes are read
        only if the hash isn't known yet (a one-block file's CID is it; others are remembered by the deny list).
        A denied hash is logged once per file (a NOTICE suggesting `rnfs deny add <cid>`, which also stops its
        blocks being served).

        A file declaring more than DENY_HASH_RATIO × the bytes of its distinct chunks + DENY_HASH_SLACK is refused
        unread: hashing reads every occurrence of repeated content, so six small blocks can declare 2^56 bytes.
        """
        if not self.deny.hashes:
            return
        digest = expect.cid.digest if expect.cid.is_raw else self.deny.known_hash(expect.cid)
        if digest is None:
            declared = walker.size_of(expect, get(expect))
            if declared > denylist.DENY_HASH_SLACK:
                # The distinct chunk bytes it needs, at least: the walk stops once they are met
                needed = -(-(declared - denylist.DENY_HASH_SLACK) // denylist.DENY_HASH_RATIO)
                if walker.leaf_bytes(expect, get, needed) < needed:
                    raise Denied(f"{expect.cid} can't be checked against this node's deny-list file hashes: it "
                                 f"repeats its content more than {denylist.DENY_HASH_RATIO}×")
            hasher = hashlib.sha256()
            for data in walker.read_range(expect, get):
                hasher.update(data)
            digest = hasher.digest()
            self.deny.remember_hash(expect.cid, digest)
        if self.deny.denies_hash(digest):
            # A hash entry stops the file being assembled, not its blocks being served one by one (spec section 8)
            if expect.cid not in self._hash_noticed:
                if len(self._hash_noticed) >= HASH_NOTICES:
                    self._hash_noticed.clear()
                self._hash_noticed.add(expect.cid)
                log(f"{expect.cid} is a file whose SHA-256 is denied: it isn't read or sent whole, but its blocks can "
                    f"still be fetched, stored and served one by one. To stop that too, deny its CID: "
                    f"rnfs deny add {expect.cid}", LOG_NOTICE)
            raise Denied(f"{expect.cid} is a file whose SHA-256 ({digest.hex()}) is on this node's deny list")

    def _check_fetched(self, fetch: Fetch, expect: walker.Expect):
        """_check_file for a file a fetch reads (cat, the gateway, a sealed get): unless its hash is known, its
        whole DAG is fetched first (fetch_dag: max_download_size, the disk reserve, the DAG byte bound)."""
        if not self.deny.hashes:
            return
        if not expect.cid.is_raw and self.deny.known_hash(expect.cid) is None:
            fetch.fetch_dag(expect, self.config.max_output_entries)
        self._check_file(expect, fetch.get)

    def _apply_deny(self):
        """Bring what is provided, cached and pinned in line with the deny list's CIDs, after a change and at
        start: stop providing (and withdraw) what reaches a denied block; drop cached fetches that do, deleting
        what nothing else reaches; release remote pins that do, which are held for others. Other pins are the
        user's: kept, but never served, provided or read while denied, with a warning naming each."""
        with self._deny_lock:
            denied = self.deny.cids
            if denied is self._deny_applied:
                return
            try:
                self._apply_denied(denied)
            except BaseException:
                self._deny_retry_at = time.monotonic() + DENY_RETRY  # maintenance tries again (logged by the caller)
                raise
            self._deny_applied = denied

    def _apply_denied(self, denied: frozenset[bytes]):
        """_apply_deny's work, for the denied binary CIDs. Lock held."""
        self.publisher.refresh()
        if denied:
            dropped = self.storage.drop_denied(denied)
            if dropped["uncached"]:
                log(f"deny list: dropped {plural(len(dropped['uncached']), 'cached fetch')} holding denied content, "
                    f"deleting {plural(dropped['removed_blocks'], 'block')} ({size(dropped['freed_bytes'])})",
                    LOG_WARNING)
                self._prune_fetched()
            pins = self.storage.pins()
            released = False
            for cid in sorted(self.storage.denied_roots(pins, denied)):
                for source in sorted(source for source in pins[cid] if source.startswith(REMOTE_PREFIX)):
                    if self.storage.unpin(cid, source):
                        released = True
                        log(f"deny list: released the remote pin of {cid} for {source}: it holds denied content",
                            LOG_WARNING)
                kept = sorted(source for source in pins[cid] if not source.startswith(REMOTE_PREFIX))
                if kept:
                    log(f"deny list: pin {cid} ({', '.join(kept)}) holds denied content: it stays stored, but isn't "
                        f"served, provided or read. Remove the pin (rnfs pin rm, or what made it) to delete it",
                        LOG_WARNING)
            if released:
                removed = self.storage.collect()
                if removed:
                    log(f"deny list: deleted {plural(removed, 'block')} only released remote pins held", LOG_WARNING)
            self.publisher.refresh()

    def _maintenance_loop(self):
        """Run maintain() about once a second until stopped."""
        last: dict = {}
        while not self.stop_event.is_set():
            try:
                self.maintain(time.monotonic(), last)
            except Exception as e:  # noqa: BLE001 - the maintenance thread keeps running; logged
                log(f"maintenance failed: {e!r}", LOG_ERROR)
            self.stop_event.wait(1.0)

    # Cached fetches the roots strategy may provide

    def _eligible_targets(self) -> set[Cid]:
        """Cached targets fetched for the user, which the roots strategy provides once complete."""
        with self._lock:
            return set(self._fetched)

    def _provide_generation(self) -> tuple[int, int]:
        """What the publisher's provide set is computed from, as counts that grow with every change: storage's
        pins, cached targets and blocks (Storage.changes), and the targets fetched for the user."""
        with self._lock:
            return self.storage.changes(), self._fetched_changes

    def _prune_fetched(self):
        """Forget fetched targets that are no longer cached."""
        with self._lock:
            # Read under the lock: a fetch recorded after an earlier snapshot would be forgotten (_note_fetched)
            cached = self.storage.cached_targets()
            if self._fetched <= cached:
                return
            self._fetched &= cached
            self._fetched_changes += 1
            self._fetched_dirty = True

    def _note_fetched(self, target: Cid):
        """Remember a cached target fetched for the user."""
        with self._lock:
            if target in self._fetched:
                return
            self._fetched.add(target)
            self._fetched_changes += 1
            self._fetched_dirty = True

    def _save_fetched(self):
        """Save the fetched targets added or forgotten since the last save, if any. With the cache records they're
        intersected with, which are saved in the same pass: a crash loses at most what changed since the last
        pass, as for those."""
        with self._fetched_save_lock:
            with self._lock:
                if not self._fetched_dirty:
                    return
                current = {cid.binary for cid in self._fetched}
                self._fetched_dirty = False
            added, gone = current - self._fetched_saved, self._fetched_saved - current
            try:
                with self._db.transaction() as tx:
                    tx.executemany("DELETE FROM fetched WHERE cid = ?", [(binary,) for binary in gone])
                    tx.executemany("INSERT OR IGNORE INTO fetched (cid) VALUES (?)", [(binary,) for binary in added])
            except BaseException:
                with self._lock:
                    self._fetched_dirty = True
                raise
            self._fetched_saved = current

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
            except Exception as e:  # noqa: BLE001 - never hide the fetch's own outcome
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
                 chunker: str | None = None, progress: Callable[[int], None] | None = None) -> dict:
        """Import a file or directory on this machine, pinned "user" (cache if not pin) unless only_hash.

        With to/to_self the file is sealed for those recipients first (seal.py; directories are refused),
        its name sealed inside unless not seal_name; chunker is "fixed" (default) or "fastcdc". progress, if
        given, is told the bytes read as they are (builder.BuildOptions.progress: an add job's), and may raise to
        stop the add; blocks stored by then are unreferenced, for the next collection.
        """
        if not os.path.lexists(path):
            raise NotFound(f"no such file or directory: {path}")
        recipients = node_content.seal_recipients(self, to, to_self, only_hash)
        options = builder.BuildOptions(chunker, progress=progress)  # ValueError for an unknown chunker, at once
        if only_hash:
            built, warnings = builder.add_path(path, builder.NO_PUT, wrap, options)
        else:
            options = builder.BuildOptions(options.chunker, self._hash_check(), progress)
            with self._refusing(), self.storage.protect() as session:
                with session.writer() as put:
                    if recipients:
                        built = node_content.add_sealed_path(path, put, wrap, recipients, seal_name, options)
                        warnings = []
                    else:
                        built, warnings = builder.add_path(path, put, wrap, options)
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
        options = builder.BuildOptions(chunker)  # ValueError for an unknown chunker, before anything is read
        if only_hash:
            built = builder.add_stream(stream, builder.NO_PUT, size, name, options=options)
        else:
            options = builder.BuildOptions(options.chunker, self._hash_check())
            with self._refusing(), self.storage.protect() as session:
                with session.writer() as put:
                    built = builder.add_stream(stream, put, size, name, options=options)
                node_content.keep_added(self, built.cid, pin)
            if pin:
                self.pin_names.record(built.cid, name or seal_name or "(stdin)")
            self._added(built)
        return {"cid": str(built.cid), "size": built.size, "warnings": [], **({"sealed": True} if recipients else {})}

    def add_tar(self, stream: BinaryIO, name: str | None = None, only_hash: bool = False, pin: bool = True,
                chunker: str | None = None) -> dict:
        """Import the directory a tar stream holds (POST /api/add/stream?format=tar; rnfs/content/tarstream.py's
        rules and limits, max_output_entries files and directories at most), wrapped as the entry name if given,
        pinned "user" (cache if not pin) unless only_hash; chunker as for add_path. {"cid", "size", "warnings"}
        (+ "warnings_total" past MAX_WARNINGS, as get caps them). Reads the stream to its end, where only zeros
        may follow the tar's own."""
        from rnfs.content import tarstream

        options = builder.BuildOptions(chunker)  # ValueError for an unknown chunker, before anything is read
        warnings = tarstream.Warnings()
        max_entries = self.config.max_output_entries
        if only_hash:
            built = tarstream.build_tar(stream, builder.NO_PUT, warnings, max_entries, options, name)
        else:
            options = builder.BuildOptions(options.chunker, self._hash_check())
            with self._refusing(), self.storage.protect() as session:
                with session.writer() as put:
                    built = tarstream.build_tar(stream, put, warnings, max_entries, options, name)
                node_content.keep_added(self, built.cid, pin)
            if pin:
                self.pin_names.record(built.cid, name or "(tar upload)")
            self._added(built)
        return {"cid": str(built.cid), "size": built.size, **warnings.result()}

    def block(self, cid: str, for_gateway: bool = False) -> bytes:
        """A CID's own block, fetched if needed and re-verified (the gateway's ?format=raw)."""
        return node_content.block(self, cid, for_gateway)

    def export_stream(self, target: str, start, for_gateway: bool = False) -> dict:
        """Fetch the target's DAG, then write its archive to start(exact length), the stream to write to (None
        writes nothing: HEAD). The gateway's ?format=archive (rnfs/node_content.py)."""
        return node_content.export_stream(self, target, start, for_gateway)

    def export_chunks(self, target: str, for_gateway: bool = False) -> tuple[dict, Iterator[bytes]]:
        """(info, chunks): the target's DAG fetched and checked, its archive's {"roots", "blocks", "length"}, and an
        iterator of the archive's bytes, byte-identical to `rnfs export` (GET /api/export/stream). Close chunks when
        done, read or not: that ends the fetch."""
        chunks = node_content.export_chunks(self, target, for_gateway)
        return next(chunks), chunks

    def get_tar(self, target: str) -> tuple[dict, Iterator[bytes]]:
        """(info, chunks): the target's DAG fetched and checked, its tar's {"cid", "length", "skipped"}, and an
        iterator of the tar's bytes (GET /api/get/stream?format=tar; node_content.tar_chunks). Close chunks when
        done, read or not: that ends the fetch."""
        chunks = node_content.tar_chunks(self, target)
        return next(chunks), chunks

    @contextmanager
    def _refusing(self) -> Iterator[None]:
        """Around an add or import, which the deny list may stop (a denied block, or a file whose content hash is
        denied: an add's BuildOptions check, _hash_check): when it does, the blocks it stored are deleted once
        nothing reaches them."""
        try:
            yield
        except Denied:
            self.storage.collect()
            raise

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
        created: list[walker.Identity] = []  # a directory output's identity, once write_out has made it
        try:
            with self._fetching(root, names, from_node) as fetch:
                expect = fetch.resolve()
                # Its verified size is what writing it out takes, however few blocks it is made of; what is
                # still to be fetched is stored first, on the same disk if the blocks are there
                content = fetch.size_of(expect)
                to_fetch = 0 if self.storage.is_complete(expect.cid) else \
                    max(0, content - self.storage.dag_bytes(expect.cid))
                _check_free_space(output, content, self.config.blocks_dir, to_fetch)
                sealed = None if raw else node_content.open_sealed(self, fetch, expect)
                if sealed is not None:  # decrypted as it is fetched (rnfs/content/seal.py)
                    self._check_fetched(fetch, expect)  # its stored (sealed) bytes, as others hold them
                    written = node_content.write_sealed(sealed, partial, self.config.blocks_dir, to_fetch)
                    result = {"cid": str(expect.cid), "warnings": [], **written}
                else:
                    fetch.fetch_dag(expect, self.config.max_output_entries)
                    # Each file is checked before it is written while the deny list has file hashes
                    check = {"check": lambda file: self._check_file(file, fetch.get)} if self.deny.hashes else {}
                    warnings = walker.write_out(expect, fetch.get, partial, self.config.max_output_entries,
                                                created=created, **check)
                    # Every skipped entry is a warning: keep a response (and a job's saved result) small
                    result = {"cid": str(expect.cid), **cap_warnings(warnings)}
            if created:
                # Still the directory written, not one put in its place by someone who can write beside it
                walker.check_unchanged(partial, created[0])
                _inherit_from_parent(partial, output)
            fsutil.rename_new(partial, output)
            return result
        finally:
            _remove_partial(partial, created[0] if created else None)

    def info(self, target: str, for_gateway: bool = False, from_node: bytes | None = None, raw: bool = False) -> dict:
        """The target's own CID, type, size and executable bit (fetching the root, path and target blocks).

        Unless for_gateway or raw, a sealed file's info has its plaintext size (node_content.sealed_info).
        """
        root, names = self._parse_target(target, for_gateway)
        with self._fetching(root, names, from_node, for_gateway) as fetch:
            return self._info(fetch, fetch.resolve(), for_gateway or raw)[0]

    def _info(self, fetch: Fetch, expect: walker.Expect, stored: bool) -> tuple[dict, seal.SealedFile | None]:
        """(info() of a resolved target, the sealed file it opened); with stored, a sealed file's info as stored
        (its ciphertext size) and None. The sealed file is unlocked when this node is a recipient."""
        block = fetch.get(expect)
        info = {"cid": str(expect.cid), "type": walker.kind_of(expect, block),
                "size": walker.size_of(expect, block), "executable": expect.kind == dag.KIND_EXECUTABLE}
        if stored or info["type"] != walker.FILE_TYPE:
            return info, None
        sealed = self._open_sealed(fetch, expect, unlock=False)
        return node_content.sealed_info(self, info, sealed), sealed

    def _open_sealed(self, fetch: Fetch, expect: walker.Expect, unlock: bool = True) -> seal.SealedFile | None:
        """node_content.open_sealed, without reading the first bytes again of a target already found not to be
        sealed under the same deny list (_not_sealed)."""
        denied = self.deny.cids  # swapped whole when the list changes
        with self._not_sealed_lock:
            if self._not_sealed_deny is not denied:
                # Reading the first bytes meets the deny list (Denied for a denied first chunk), so what was
                # found under another list is found again
                self._not_sealed.clear()
                self._not_sealed_deny = denied
            elif expect in self._not_sealed:
                self._not_sealed.move_to_end(expect)
                return None
        sealed = node_content.open_sealed(self, fetch, expect, unlock)
        if sealed is None:
            with self._not_sealed_lock:
                if self._not_sealed_deny is denied:
                    self._not_sealed[expect] = None
                    if len(self._not_sealed) > NOT_SEALED_MEMO:
                        self._not_sealed.popitem(last=False)
        return sealed

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
                yield from self._read(fetch, fetch.resolve(), offset, length, for_gateway, raw)

        return generate()

    def read_file(self, target: str, offset: int = 0, length: int | None = None, from_node: bytes | None = None,
                  raw: bool = False) -> tuple[dict, Iterator[bytes]]:
        """info() and read() in one fetch, which resolves the target once: (its info, a lazy iterator of its bytes
        in [offset, offset + length) cut to the size info gives). The iterator reads only once next() is called
        on it, after the caller has checked the info (a directory's raises ValueError); close it when done, read
        or not: that ends the fetch (GET /api/cat)."""
        root, names = self._parse_target(target)
        if offset < 0 or (length is not None and length < 0):
            raise ValueError("offset and length must be non-negative")

        def generate() -> Iterator:
            """The info, then the bytes; closed before reading them, the fetch ends as info()'s does."""
            with self._fetching(root, names, from_node) as fetch:
                expect = fetch.resolve()
                info, sealed = self._info(fetch, expect, raw)
                try:
                    yield info
                except GeneratorExit:
                    return
                if info["type"] != "file":
                    raise ValueError(f"{target} is a directory")
                available = max(info["size"] - offset, 0)
                yield from self._read(fetch, expect, offset, available if length is None else min(length, available),
                                      False, raw, sealed)

        chunks = generate()
        return next(chunks), chunks

    def _read(self, fetch: Fetch, expect: walker.Expect, offset: int, length: int | None, for_gateway: bool,
              raw: bool, opened: "seal.SealedFile | None | object" = _UNOPENED) -> Iterator[bytes]:
        """read() of a resolved target, in fetch. opened: what _info opened in the same fetch (read_file), so the
        file's first bytes aren't read again to tell whether it is sealed."""
        if self.deny.hashes and walker.kind_of(expect, fetch.get(expect)) == walker.FILE_TYPE:
            # The whole file is fetched and hashed (once: then remembered) before any of it is sent
            self._check_fetched(fetch, expect)
        if raw or for_gateway:
            sealed = None
        elif opened is _UNOPENED:
            sealed = self._open_sealed(fetch, expect)
        else:
            sealed = opened
            if sealed is not None and not node_content.is_recipient(self, sealed):
                node_content.unlock_sealed(self, sealed)  # raises NotRecipient after the deny check, as opening did
        if sealed is not None:
            yield from sealed.read(offset, length)
            return
        yield from fetch.read_range(expect, offset, length)

    def ls(self, target: str, for_gateway: bool = False) -> list[dict]:
        """A directory's entries in name order (fetching its directory and index nodes only)."""
        root, names = self._parse_target(target, for_gateway)
        with self._fetching(root, names, None, for_gateway) as fetch:
            expect = fetch.resolve()
            if walker.kind_of(expect, fetch.get(expect)) != walker.DIRECTORY_TYPE:
                raise ValueError(f"{target} is not a directory")
            fetch.fetch_listing(expect)
            return [{"name": entry.name, "kind": dag.KIND_NAMES[entry.kind], "size": entry.size, "cid": str(entry.cid)}
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
        """A file's first length bytes if every block they need is stored, else None. Never fetches. Raises
        Denied as a read does (node_content.stored_head)."""
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
        """A new root without the entry target names; pinned "user" (the complete directory is fetched first).
        Raises ValueError, before fetching anything, unless target is <cid>/<path>."""
        edit.check_entry_target(target)
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
        """Pins with their sources, whether they're complete, their type, whether they're sealed files, a local
        name when one is known, and (rnfs 0.2.0) their label, origin, size, entry count and when added.

        origin is where the pin was added from on this machine (pin_names) or the shared-folder entry it came
        from; label the name its user gave it (pin_label), kept apart from origin. name, as before 0.2.0, is the
        label, else the origin, else for a stored directory its single entry's name (add --wrap) or its entry
        count as text. For a sealed file origin is still the local path or the name given to add (this node's own
        record, for its user): `sealed` tells callers that show pins to others not to show the name or the content.
        `sealed` is None when it can't be told without fetching (the root or the file's first chunk isn't stored).
        size is the content size the root block states (None until it is stored), entries a directory's entry
        count (None for a file, or while a large directory's index isn't all stored), added when it was first
        pinned (None for a pin made before rnfs 0.2.0).
        """
        pins = self.storage.pins()
        self.pin_names.prune(set(pins))
        with self._pin_memo_lock:
            for gone in [c for c in self._pin_memo if c not in pins]:
                del self._pin_memo[gone]
        if cid is not None:
            wanted = Cid.parse(cid)
            pins = {wanted: pins[wanted]} if wanted in pins else {}
        folder_names = self.shared_folder.names()
        result = []
        for c, sources in sorted(pins.items(), key=lambda item: str(item[0])):
            complete = self.storage.is_complete(c)
            details = self._pin_details(c, complete)
            origin = self.pin_names.get(c) or (folder_names.get(c) if FOLDER in sources else None)
            label = self.pin_names.label(c)
            result.append({"cid": str(c), "sources": sorted(sources), "complete": complete,
                           "type": details.kind, "sealed": details.sealed,
                           "name": label or origin or details.inside, "label": label, "origin": origin,
                           "size": details.size, "entries": details.entries, "added": self.storage.pin_added(c)})
        return result

    def pin_label(self, cid: str, label: str | None) -> dict:
        """Give a pin a label (None or "" clears it), kept apart from where it was added from: its pin_ls entry.
        NotFound if cid isn't pinned; ValueError for a label pin_names.check_label refuses. A label lasts while
        the CID stays pinned (by any source)."""
        parsed = Cid.parse(cid)
        if not self.storage.is_pinned(parsed):
            raise NotFound(f"{cid} is not pinned")
        if self.pin_names.set_label(parsed, label):
            events.publish(self, "pin", change="label", source=None, label=self.pin_names.label(parsed),
                           **events.cid_fields([parsed]))
        listed = self.pin_ls(str(parsed))
        if not listed:  # released meanwhile
            raise NotFound(f"{cid} is not pinned")
        return listed[0]

    def _pin_details(self, cid: Cid, complete: bool) -> "_PinDetails":
        """A pin's _PinDetails (_pin_shape, _pin_sealed), remembered while it is complete.

        They are read from stored blocks, which a complete pin has all of and whose content never changes, so
        they hold for as long as it stays complete; an incomplete pin's are read each time (a block may arrive,
        or have gone). pin_ls forgets the pins released; at most PIN_MEMO are remembered, the first met kept:
        pin_ls goes through them in the same order each time, so replacing older ones would remember none.
        """
        if complete:
            with self._pin_memo_lock:
                known = self._pin_memo.get(cid)
            if known is not None:
                return known
        shape = self._pin_shape(cid)
        details = shape._replace(sealed=self._pin_sealed(cid, shape.kind))
        # A complete DAG's blocks always tell them all; None means a block went while they were read
        if complete and details.kind is not None and details.sealed is not None \
                and (details.kind == "file" or details.entries is not None):
            with self._pin_memo_lock:
                if len(self._pin_memo) < PIN_MEMO:
                    self._pin_memo[cid] = details
        return details

    def _pin_sealed(self, cid: Cid, kind: str | None) -> bool | None:
        """True if a pinned file is sealed (its first bytes carry the sealed-file magic), False if it isn't or
        is a directory, None if that isn't known from stored blocks. Never fetches."""
        if kind == "directory":
            return False
        if kind is None:
            return None
        # The user's own list of what is stored: whether a denied pin is sealed is no secret, and isn't worth a hash
        head = node_content.stored_head(self, str(cid), len(seal.MAGIC), deny=False)
        return None if head is None else seal.is_sealed(head)

    def _pin_shape(self, cid: Cid, count: bool = True) -> "_PinDetails":
        """The _PinDetails of a pin whose root block is stored, but sealed (None): its type ("file" or
        "directory"), for a directory its one entry's name or "(directory: N entries)" (inside; a large
        directory's index root, "(directory)" as before 0.2.0) and, if count, its entry count, and its content
        size. All None if the root isn't stored (or isn't a valid DAG node)."""
        unknown = _PinDetails(None, None, None, None, None)
        if cid.is_raw:  # a raw block is its own content: its size is known without reading it
            size = self.storage.block_size(cid)
            return unknown if size is None else _PinDetails("file", None, None, None, size)
        block = self.storage.get(cid)
        if block is None:
            return unknown
        try:
            node = dag.decode(block)
        except InvalidDag:
            return unknown
        if isinstance(node, dag.FileNode):
            return _PinDetails("file", None, None, None, node.size)
        if isinstance(node, dag.DirectoryNode):
            inside = node.entries[0].name if len(node.entries) == 1 else f"(directory: {len(node.entries)} entries)"
            return _PinDetails("directory", inside, None, len(node.entries), node.size)
        return _PinDetails("directory", "(directory)", None, self._index_entries(node) if count else None, node.size)

    def _index_entries(self, index: dag.IndexNode) -> int | None:
        """The entries below a large directory's index node, counted from its stored groups; None if one of its
        index or group nodes isn't stored (or isn't valid), or if the count would pass max_output_entries. Never
        fetches. A node the index lists twice is invalid (a valid index never repeats a child), which also stops
        a few crafted blocks from making the walk grow exponentially; at most max_output_entries nodes are read."""
        count = 0
        seen: set[Cid] = set()
        stack = list(index.child_cids)
        limit = self.config.max_output_entries
        while stack:
            cid = stack.pop()
            if cid in seen or len(seen) >= limit:
                return None
            seen.add(cid)
            block = self.storage.get(cid)
            if block is None:
                return None
            try:
                node = dag.decode(block)
            except InvalidDag:
                return None
            if isinstance(node, dag.DirectoryNode):
                count += len(node.entries)
                if count > limit:
                    return None
            elif isinstance(node, dag.IndexNode):
                stack.extend(node.child_cids)
            else:
                return None
        return count

    def pin_remote(self, node_id: bytes, cid: str, pin: bool) -> bool:
        """Ask node_id to pin (or release our pin of) cid over an identified link; True if accepted."""
        return self.client.pin(node_id, Cid.parse(cid), pin)

    def remote_pin(self, identity_hash: bytes, cid: Cid, pin: bool) -> bool:
        """A /pin from an allowed identity: release at once, or fetch then pin with source remote:<identity>.

        A source's pins are fetched one at a time, so each is checked against its quota with the earlier ones
        counted; one with REMOTE_PIN_QUEUE pins waiting is refused (False), and so is one whose fetch thread
        can't be started. Accepted pins are saved (the remote_pins table) until fetched, and resumed at the next
        start if the node stops first.
        """
        source = remote_source(identity_hash)
        if not pin:
            with self._lock:
                if cid in self._remote_reserved.get(source, {}):
                    # Still being fetched: it mustn't be pinned when the fetch ends
                    self._remote_released.add((source, cid))
                    self._remote_pins_dirty = True
                    log(f"released remote pin of {cid} for {source} while fetching it", LOG_NOTICE)
            if self.storage.unpin(cid, source):
                log(f"released remote pin of {cid} for {source}", LOG_NOTICE)
                self.publisher.refresh()
            return True
        if self.deny.denies(cid):
            log(f"refused remote pin of {cid} for {source}: it is on this node's deny list", LOG_WARNING)
            return False
        with self._lock:
            reserved = self._remote_reserved.setdefault(source, {})
            if cid in reserved:
                if (source, cid) in self._remote_released:
                    self._remote_released.discard((source, cid))  # pinned again before the fetch ended
                    self._remote_pins_dirty = True
                return True
            if source in self.storage.pins().get(cid, set()):
                return True
            if len(reserved) >= REMOTE_PIN_QUEUE:
                log(f"refused remote pin of {cid} for {source}: {len(reserved)} of its pins are waiting", LOG_WARNING)
                return False
            reserved[cid] = None
            self._remote_pins_dirty = True
            return self._ensure_remote_worker(source, cid)

    def _ensure_remote_worker(self, source: str, cid: Cid) -> bool:
        """Start source's fetch thread unless it has one; False if it can't be started, with cid (the pin just
        reserved) given back. Caller holds the lock.

        A worker leaves _remote_workers in the same critical section in which it finds nothing waiting, so a
        pin reserved after that starts a new one, and one reserved before it is fetched by the old one.
        """
        if source in self._remote_workers:
            return True
        thread = threading.Thread(target=self._remote_pin_worker, args=(source,), name="rnfs-remote-pin",
                                  daemon=True)
        try:
            thread.start()
        except RuntimeError as e:  # can't start new thread: out of threads or memory
            reserved = self._remote_reserved.get(source, {})
            reserved.pop(cid, None)
            if not reserved:
                self._remote_reserved.pop(source, None)
            self._remote_pins_dirty = True
            log(f"refused remote pin of {cid} for {source}: couldn't start its fetch: {e}", LOG_ERROR)
            return False
        # Registered after start() succeeds, still under the lock, so the worker can't look before it is
        self._remote_workers[source] = thread
        return True

    def _remote_pin_worker(self, source: str):
        """Fetch source's remote pins one at a time, oldest first, until none is waiting or the node stops.

        Concurrent fetches would each be checked against the quota without the others' blocks, so together
        they could store several times it. Pins waiting when the node stops stay saved for the next start.
        """
        # Resumed and early pins wait for the join: a lookup before it would find no provider and drop them
        while not self.joined.is_set():
            if self.stop_event.wait(0.5):
                break
        while True:
            with self._lock:
                cid = None if self.stop_event.is_set() else next(iter(self._remote_reserved.get(source, {})), None)
                if cid is None:
                    # Deregistered in the same critical section that saw nothing waiting: a /pin arriving after
                    # it starts a new worker instead of relying on this one, which is about to return
                    self._remote_workers.pop(source, None)
                    return
            try:
                self._remote_pin_fetch(cid, source)  # removes cid from _remote_reserved when done
            except BaseException:
                with self._lock:
                    self._remote_workers.pop(source, None)
                raise

    def _save_remote_pins(self):
        """Save the remote pins waiting or being fetched, in the order accepted (released ones left out), if
        they changed, so a restart resumes them. Run by maintenance (about every second) and at stop, not on
        every /pin: a crash loses at most the last second's (/pin is best effort, section 8)."""
        with self._remote_pins_save_lock:
            with self._lock:
                if not self._remote_pins_dirty:
                    return
                waiting = [(source, cid) for source, cids in self._remote_reserved.items() for cid in cids
                           if (source, cid) not in self._remote_released]
                self._remote_pins_dirty = False
            try:
                with self._db.transaction() as tx:  # a few rows (at most REMOTE_PIN_QUEUE per source): rewritten
                    tx.execute("DELETE FROM remote_pins")
                    tx.executemany("INSERT INTO remote_pins (position, source, cid) VALUES (?, ?, ?)",
                                   [(position, source, cid.binary) for position, (source, cid) in enumerate(waiting)])
            except OSError as e:
                with self._lock:
                    self._remote_pins_dirty = True  # try again next time
                log(f"saving the remote pins waiting failed: {e!r}", LOG_ERROR)

    def _load_remote_pins(self) -> list[tuple[str, Cid]]:
        """Remote pins saved as waiting, oldest first, from identities still on remote_pin_identities (at most
        REMOTE_PIN_QUEUE each); others are dropped. Caller holds no lock (it reads the database)."""
        allowed = {remote_source(h) for h in self.config.remote_pin_set}
        try:
            saved = self._db.query("SELECT source, cid FROM remote_pins ORDER BY position")
        except OSError as e:
            log(f"reading the remote pins waiting failed: {e!r}", LOG_ERROR)
            return []
        pins: dict[tuple[str, Cid], None] = {}
        counts: dict[str, int] = {}
        malformed = 0
        for source, binary in saved:
            try:
                if not isinstance(source, str):
                    raise TypeError("not a string")
                cid = Cid.from_bytes(binary)
            except (ValueError, TypeError):
                malformed += 1
                continue
            if source not in allowed or (source, cid) in pins or counts.get(source, 0) >= REMOTE_PIN_QUEUE:
                continue
            counts[source] = counts.get(source, 0) + 1
            pins[(source, cid)] = None
        if malformed:
            log(f"skipped {malformed} malformed remote pin{'' if malformed == 1 else 's'} saved as waiting",
                LOG_WARNING)
        return list(pins)

    def _resume_remote_pins(self):
        """Queue the remote pins saved as waiting when the node last stopped (called once at start)."""
        pins = self._load_remote_pins()
        resumed = 0
        with self._lock:
            pinned = self.storage.pins()
            for source, cid in pins:
                if source not in pinned.get(cid, set()):
                    self._remote_reserved.setdefault(source, {})[cid] = None
                    resumed += 1
            self._remote_pins_dirty = True
            for source, cids in list(self._remote_reserved.items()):
                # A thread that can't be started gives its pin back, so this ends
                while cids and not self._ensure_remote_worker(source, next(iter(cids))):
                    pass
        if resumed:
            log(f"resuming {plural(resumed, 'remote pin')} accepted before the last stop", LOG_NOTICE)

    def _remote_pin_fetch(self, cid: Cid, source: str):
        """Fetch cid for a remote pin, dropping it as soon as its verified size, or the bytes of its DAG's
        blocks met so far, would exceed the quota. The fetch ends at its next step once the node stops."""
        quota = self.config.remote_pin_quota
        observer = Observer()
        observer.cancelled = self.stop_event.is_set
        try:
            with self._lock:
                if (source, cid) in self._remote_released:
                    return  # released while it waited
            with observing(observer), self.fetcher.open(cid, pinned=True) as fetch:
                expect = fetch.resolve()
                content = fetch.size_of(expect)
                # Nothing else is being fetched for source (_remote_pin_worker), so its pins are all it uses
                used = self.storage.source_usage(source)
                if used + content > quota:
                    log(f"dropping remote pin of {cid} for {source}: {size(content)} would exceed its "
                        f"quota ({size(used)} of {size(quota)} used)", LOG_WARNING)
                    return
                # A DAG of empty files has size 0 yet can hold gigabytes of directory nodes: count its blocks
                # (fetch_dag also bounds them by what the size can hold, whichever is lower)
                fetch.max_dag_bytes = quota - used
                try:
                    fetch.fetch_dag(expect)
                except TooLarge as e:
                    log(f"dropping remote pin of {cid} for {source}: its blocks would exceed its quota "
                        f"({size(used)} of {size(quota)} used) or what its size can hold: {e}", LOG_WARNING)
                    return
                with self._lock:
                    # Checked and pinned under the lock, so a release arriving now is either seen or undoes it
                    if (source, cid) in self._remote_released:
                        log(f"not pinning {cid} for {source}: released while it was fetched", LOG_NOTICE)
                        return
                    self.storage.pin(cid, source)
            log(f"pinned {cid} for {source}", LOG_NOTICE)
            self.publisher.refresh()
        except Cancelled:
            log(f"remote pin of {cid} for {source} stopped with the node: it is fetched after the next start",
                LOG_VERBOSE)
        except RnfsError as e:
            log(f"remote pin of {cid} for {source} failed: {e}", LOG_WARNING)
        except Exception as e:  # noqa: BLE001 - the pin waits for the next try; logged
            log(f"remote pin of {cid} for {source} failed: {e!r}", LOG_ERROR)
        finally:
            with self._lock:
                # Once the node is stopping, a fetch cut short stays waiting, so it is saved for the next start
                # (a pin that completed meanwhile is skipped then)
                if not self.stop_event.is_set():
                    self._remote_reserved.get(source, {}).pop(cid, None)
                    self._remote_released.discard((source, cid))
                    self._remote_pins_dirty = True

    def gc(self) -> dict:
        """Evict every cached fetch and delete unreachable blocks; withdraw what is no longer complete."""
        stats = self.storage.gc()
        self.publisher.refresh()
        self._prune_fetched()
        self._save_state()
        log(f"gc removed {plural(stats['removed_blocks'], 'block')} ({size(stats['freed_bytes'])})", LOG_NOTICE)
        return {"removed_blocks": stats["removed_blocks"], "freed_bytes": stats["freed_bytes"]}

    def cache_ls(self) -> list[dict]:
        """The cached fetches (downloads not pinned, kept until evicted; section 8), most recently used first:
        {"cid" (the target), "root", "path" (the names from root to it, [] for a bare CID), "pinned" (the same
        content is also pinned), "complete", "size" (as its root block states it, None until stored), "type",
        "provided" (offered to the network: the roots strategy, a complete download for the user)}. One target
        reached through several paths is listed once per path. Never fetches."""
        provided = self.publisher.provided()
        pinned = self.storage.pinned_cids()
        result = []
        for root, names, target in reversed(self.storage.cached_fetches()):
            shape = self._pin_shape(target, count=False)  # the type and size only: no walk of a large directory
            result.append({"cid": str(target), "root": str(root), "path": list(names), "pinned": target in pinned,
                           "complete": self.storage.is_complete(target), "type": shape.kind, "size": shape.size,
                           "provided": target in provided})
        return result

    def cache_rm(self, cid: str) -> dict:
        """Evict the cached fetches leading to cid now (as gc does for all of them), deleting the blocks nothing
        else needs: {"removed_records", "removed_blocks", "freed_bytes"}. A pin of the same content keeps its
        blocks. NotFound if no cached fetch leads to cid."""
        parsed = Cid.parse(cid)
        stats = self.storage.uncache({parsed})
        if not stats["removed_records"]:
            raise NotFound(f"{cid} is not in the cache")
        self.publisher.refresh()
        self._prune_fetched()
        self._save_state()
        log(f"cache rm {parsed} removed {plural(stats['removed_blocks'], 'block')} ({size(stats['freed_bytes'])})",
            LOG_NOTICE)
        return {"removed_records": stats["removed_records"], "removed_blocks": stats["removed_blocks"],
                "freed_bytes": stats["freed_bytes"]}

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
        found: list[bytes] = list(self.records.providers(parsed.key, exclude=self.excluded_provider))

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

    def import_archive(self, path: str, progress: Callable[[int], None] | None = None) -> dict:
        """Import an archive on this machine, pinning its complete roots with source "user" (import_stream; progress
        as there: an import job's). Raises ValueError for anything but a regular file: opened without blocking, so a
        FIFO can't hold the call (or an import job's slot) waiting for a writer."""
        try:
            fd = os.open(path, os.O_RDONLY | fsutil.O_BINARY | getattr(os, "O_NONBLOCK", 0))
        except FileNotFoundError:
            raise NotFound(f"no such file: {path}") from None
        except IsADirectoryError:
            raise ValueError(f"{path} is not a regular file") from None
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ValueError(f"{path} is not a regular file")
            stream = os.fdopen(fd, "rb")
        except BaseException:
            os.close(fd)
            raise
        with stream:
            return self.import_stream(stream, f"archive {os.path.abspath(path)}", progress)

    def import_stream(self, stream: BinaryIO, label: str = "(archive upload)",
                      progress: Callable[[int], None] | None = None) -> dict:
        """Import an archive read from stream to its end (POST /api/import/stream), pinning its complete roots with
        source "user", each recorded in pin_names as label unless it has a name there. progress, if given, is told
        the bytes read as they are (an import job's) and may raise to stop the import: blocks stored by then stay as
        unpinned blocks, for the next collection. {"roots", "incomplete", "blocks"}."""
        stream = builder.counted(stream, progress)

        def check(roots: list[Cid]):
            """Refuse the import if a file the roots hold has a denied content hash."""
            if self.deny.hashes:
                get = self.storage.getter()
                for root in roots:
                    for file in walker.iter_files(walker.root_expect(root), get):
                        self._check_file(file, get)  # a complete root: every block is stored and checked

        with self._refusing():
            stats = archive.import_archive(stream, self.storage, check=check)
        for root in stats.get("complete", []):
            self.pin_names.record_if_unknown(root, label)
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
                "records": self.records.record_count(), "pins": self.storage.pin_count(),
                "provided": len(self.publisher.provided()), "blocks": self.storage.block_count(),
                "used_bytes": self.storage.used_bytes(), "max_storage": self.config.max_storage,
                "names": len(self.names.list()),
                **{key: value for feature in self.features.values() for key, value in feature.status().items()}}

