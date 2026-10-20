"""The interface rnfs.Node and rnfs.Client share (design.md, "Public API"; provisional until 1.0.0). No RNS.

Rnfs implements every method once, over a backend offering the node facade's operations with the local API's
argument and result forms (JSON-ready dicts; CIDs written out, node IDs and names as hex): rnfs.embedded's over
an RnfsNode, rnfs.remote's over the daemon's API. Both results become the same objects (rnfs.objects), so the
two classes can't drift apart; only lifecycle, how name records are heard, and the escape hatches differ.

A target is a Cid, `<cid>[/name…]` (whose first part may be a name contact's nick), or an rnfs: URI (section 13):
a name URI's name is resolved first (persisted, as names.resolve does), and its from= is the from_node of the
methods taking one. Node IDs are 16 bytes or 32 hex characters; names 16 bytes, 32 hex characters or a Name, and a
name contact's nick where the API takes one (resolve, keep, follow).

The features' methods (peers, stats, contacts, names.keep and follow) call the feature as its API route does,
so a Node raises what a Client gets back: NotFound when the feature isn't loaded, ValueError for what it refuses.
timeout() bounds a Client's requests per thread; a Node, which makes none, only checks its argument.

Callbacks (names.watch, a job's on_progress or watch) run on the instance's event thread, one at a time, in the
order their events arrived, with no rnfs lock held: whatever hears an event only queues it, coalesced per watch
(a slow callback is given the latest state, not a backlog). A callback may call any method, but blocking in it
delays every other callback of the instance; close() from one doesn't wait for the thread. An exception in a
callback is logged and dropped. Watched jobs are polled by a thread of their own, which queues a call when a
job changes (at most every job_poll seconds) and once more when it finishes. Both threads start on first use.
events.watch's callbacks are the exception to coalescing: each event is one call, in order (an event says what
changed, not a state), so a callback slower than the events lets them queue up.
"""
import io
import itertools
import os
import threading
import time
import warnings
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass, field

from rnfs import ids
from rnfs.content.cid import Cid
from rnfs.errors import Closed, NotFound, RnfsError
from rnfs.events import EVENT_KINDS
# A job's final states, and the longest wait one request for a job asks for (the API's cap)
from rnfs.features.jobs import FINISHED, MAX_WAIT as MAX_JOB_WAIT
from rnfs.objects import (Added, Cached, Capabilities, Collected, Contact, DenyState, Entry, Event, EventBatch,
                          Exported, Followed, FolderSync, Imported, Info, Kept, Peer, Pin, Record, RemotePin,
                          ServedPins, Settings, SettingsChange, Stat, Stats, Status, Uncached, Verified, Written)

FILE_BUFFER = 64 * 1024  # what a File reads at least per refill: one read() of the node, one GET /api/cat
# The most one refill of a File read sequentially fetches: each sequential refill doubles from FILE_BUFFER up
# to it, so the per-request cost is spread over more bytes; a seek starts again at FILE_BUFFER
READ_AHEAD = 4 * 1024 * 1024
STOP_WAIT = 5.0  # seconds close() waits for the event and job threads to end


def node_hex(node_id) -> str | None:
    """A node ID or name as 32 hex characters (None stays None). ValueError unless 16 bytes or 32 hex digits."""
    if node_id is None:
        return None
    if isinstance(node_id, (bytes, bytearray)) and len(node_id) == 16:
        return bytes(node_id).hex()
    if isinstance(node_id, str) and ids.HEX16.fullmatch(node_id):
        return node_id.lower()
    raise ValueError(f"invalid node ID or name {node_id!r}: 16 bytes or 32 hex characters")


def name_bytes(name) -> bytes:
    """A name (16 bytes, 32 hex characters or a Name) as bytes. ValueError for anything else."""
    if isinstance(name, Name):
        return name.name
    return bytes.fromhex(node_hex(name))


def name_arg(name) -> str:
    """A name as the API takes it: a string that isn't 32 hex characters as it is (a name contact's nick, which
    the node checks), anything else (16 bytes, hex, a Name) as 32 hex characters. ValueError for anything else."""
    if isinstance(name, str) and not ids.HEX16.fullmatch(name):
        return name
    return node_hex(name_bytes(name))


def hash_arg(value) -> str:
    """A hash (a node ID, name or LXMF address) as the API takes it: 16 bytes or a Name as hex, a string as it is
    (32 hex characters, or a contact's nick where one is taken), for the node to check. ValueError otherwise."""
    if isinstance(value, Name):
        return value.hex
    if isinstance(value, (bytes, bytearray)):
        return node_hex(value)
    if isinstance(value, str):
        return value
    raise ValueError(f"a hash is 16 bytes or 32 hex characters, not {type(value).__name__}")


def recipients(to) -> list[str] | None:
    """Sealing recipients as a list of strings (node IDs, identity hashes or contact nicks, for the node to
    check), None for none. ValueError for anything but a list or tuple of strings, as the API refuses it."""
    if not to:
        return None
    if isinstance(to, str) or not isinstance(to, (list, tuple)) or not all(isinstance(item, str) for item in to):
        raise ValueError("to must be a list of recipients")
    return list(to)


def _body(source, method: str):
    """What a stream method sends: bytes-like data as bytes (a memoryview's len() counts its items, not its
    bytes), a binary file as it is. ValueError for anything else."""
    if isinstance(source, (bytes, bytearray, memoryview)):
        return bytes(source)
    if hasattr(source, "read"):
        return source
    raise ValueError(f"{method}() takes bytes or a binary file, not {type(source).__name__}")


def _abspath(path) -> str:
    """path made absolute, as the daemon and the facade take paths."""
    return os.path.abspath(os.path.expanduser(os.fspath(path)))


class Watch:
    """A watch on a name or a job: cancel() (or leaving it as a context manager) stops further callbacks; one
    already running finishes."""

    def __init__(self, cancel):
        """A watch calling cancel() once when cancelled."""
        self._cancel = cancel
        self._lock = threading.Lock()
        self.active = True

    def cancel(self):
        """Stop the callbacks (idempotent)."""
        with self._lock:
            if not self.active:
                return
            self.active = False
        self._cancel()

    def __enter__(self) -> "Watch":
        """The watch itself."""
        return self

    def __exit__(self, *exc):
        """Cancel the watch."""
        self.cancel()


class _Events:
    """The event thread: calls queued callbacks one at a time, in the order queued, the latest per key."""

    def __init__(self, thread_name: str, report):
        """Calls from a thread called thread_name, reporting a callback's exception with report(message)."""
        self._thread_name = thread_name
        self._report = report
        self._cond = threading.Condition()  # innermost: never held while calling out
        self._pending: OrderedDict = OrderedDict()
        self._thread: threading.Thread | None = None
        self._stopped = False

    def start(self):
        """Start the thread unless it runs (from an app's thread, never an RNS one)."""
        with self._cond:
            if self._thread is not None or self._stopped:
                return
            self._thread = threading.Thread(target=self._run, name=self._thread_name, daemon=True)
            self._thread.start()

    def post(self, key, call):
        """Queue call() under key, replacing one queued under key that hasn't run yet. Never blocks: safe to call
        holding any lock."""
        with self._cond:
            if self._stopped:
                return
            self._pending[key] = call
            self._cond.notify()

    def _run(self):
        """Call what is queued until stopped."""
        while True:
            with self._cond:
                while not self._pending and not self._stopped:
                    self._cond.wait()
                if self._stopped:
                    return
                _, call = self._pending.popitem(last=False)
            try:
                call()
            except Exception as e:  # noqa: BLE001 - an app's callback mustn't end the thread
                self._report(f"a callback failed: {e!r}")

    def stop(self):
        """Drop what is queued and end the thread, waiting for it unless called from it."""
        with self._cond:
            self._stopped = True
            self._pending.clear()
            self._cond.notify()
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(STOP_WAIT)


class _NameWatch:
    """One names.watch: its callback and the sequence last delivered (or held when it began). Records heard
    before that baseline is known wait in pending, and only the newest of them is compared with it."""

    def __init__(self, name: bytes, callback):
        """A watch of name calling callback, not ready until begin()."""
        self.name = name
        self.callback = callback
        self.last: int | None = None
        self.ready = False
        self.pending: Record | None = None
        self.lock = threading.Lock()
        self.handle: Watch | None = None

    def begin(self, baseline: int | None) -> Record | None:
        """Set the sequence held when the watch began; the record heard meanwhile to deliver, if newer."""
        with self.lock:
            self.last, self.ready = baseline, True
            pending, self.pending = self.pending, None
        return pending if pending is not None and (baseline is None or pending.sequence > baseline) else None

    def deliver(self, record: Record):
        """Call the callback if record is newer than the last delivered (on the event thread)."""
        with self.lock:
            if not self.ready:
                if self.pending is None or record.sequence > self.pending.sequence:
                    self.pending = record
                return
            if not self.handle.active or (self.last is not None and record.sequence <= self.last):
                return
            self.last = record.sequence
        self.callback(record)


class _EventWatch:
    """One events.watch: its kinds (None: all), its callback, and its position: the node's run (epoch) and the
    last sequence delivered or skipped. Events heard before the baseline is known wait in pending, then only those
    after it are delivered; a "lost" marker (kind "lost", seq the sequence the source resumes after) moves the
    position there and is delivered unless the watch began after it."""

    def __init__(self, kinds: frozenset | None, callback):
        """A watch calling callback, not ready until begin()."""
        self.kinds = kinds
        self.callback = callback
        # Held while a delivery is queued (deliver: _Events.post, whose condition is innermost), so deliveries are
        # queued in the order the position passes them, whichever thread hears them; never held calling out else
        self.lock = threading.Lock()
        self.ready = False
        self.epoch: str | None = None
        self.after = 0
        self.pending: list[tuple[str, dict]] = []
        self.handle: Watch | None = None

    def begin(self, after: int, epoch: str, deliver) -> list[dict]:
        """Set the position the watch begins at and deliver(event) the events heard meanwhile that are after it,
        in order, before any heard later; those delivered."""
        with self.lock:
            self.ready, self.after, self.epoch = True, after, epoch
            pending, self.pending = self.pending, []
            delivered = [event for run, event in pending if self._take(run, event)]
            for event in delivered:
                deliver(event)
            return delivered

    def offer(self, epoch: str, event: dict, deliver) -> bool:
        """deliver(event) of run epoch if the watch wants it (kept for begin() while the baseline isn't known);
        whether it did."""
        with self.lock:
            if not self.ready:
                self.pending.append((epoch, event))
                return False
            if not self._take(epoch, event):
                return False
            deliver(event)
            return True

    def _take(self, epoch: str, event: dict) -> bool:
        """Whether to deliver event, moving the position past it (lock held, ready)."""
        if event["kind"] == "lost":
            if epoch == self.epoch and event["seq"] <= self.after:
                return False  # the watch began after what was missed
            self.epoch, self.after = epoch, event["seq"]
            return True
        if epoch != self.epoch or event["seq"] <= self.after:
            return False
        self.after = event["seq"]
        return self.kinds is None or event["kind"] in self.kinds


def event_kinds(kinds) -> list[str] | None:
    """kinds (a kind, an iterable of them, or None) as a list, None for every kind (also for none given).
    ValueError for a kind the node doesn't publish (rnfs.events.EVENT_KINDS), as GET /api/events words it."""
    if kinds is None:
        return None
    kinds = [kinds] if isinstance(kinds, str) else list(kinds)
    for kind in kinds:
        if kind not in EVENT_KINDS:
            raise ValueError(f"unknown event kind {kind!r}; known: {', '.join(EVENT_KINDS)}")
    return kinds or None


class _JobPoller:
    """The thread polling watched jobs, queueing each watch's callback when its job changes and when it ends."""

    def __init__(self, owner: "Rnfs", thread_name: str, interval: float):
        """Polls through owner every interval seconds while anything is watched."""
        self._owner = owner
        self._thread_name = thread_name
        self._interval = interval
        self._cond = threading.Condition()
        self._watches: dict[int, tuple[str, object, Watch]] = {}  # id -> (job ID, callback, handle)
        self._last: dict[int, dict] = {}  # id -> the job's dict last queued for it
        self._thread: threading.Thread | None = None
        self._stopped = False
        self._next = 0

    def add(self, job_id: str, callback) -> Watch:
        """Watch job_id: callback(job) on each change, and once when it finishes."""
        with self._cond:
            if self._stopped:
                raise Closed("closed")
            key = self._next
            self._next += 1
            handle = Watch(lambda: self._remove(key))
            self._watches[key] = (job_id, callback, handle)
            if self._thread is None:
                self._thread = threading.Thread(target=self._run, name=self._thread_name, daemon=True)
                self._thread.start()
            self._cond.notify()
        return handle

    def _remove(self, key: int):
        """Forget a watch."""
        with self._cond:
            self._watches.pop(key, None)
            self._last.pop(key, None)

    def _run(self):
        """Poll until stopped."""
        while True:
            with self._cond:
                while not self._watches and not self._stopped:
                    self._cond.wait()
                if self._stopped:
                    return
                watches = dict(self._watches)
            jobs: dict[str, dict | None] = {}
            for key, (job_id, callback, handle) in watches.items():
                if job_id not in jobs:
                    jobs[job_id] = self._poll(job_id)
                self._check(key, jobs[job_id], callback, handle)
            with self._cond:
                if not self._stopped:
                    self._cond.wait(self._interval)

    def _poll(self, job_id: str) -> dict | None:
        """The job's dict now; None if it can't be read (gone: NotFound, cleared; or an error, reported)."""
        try:
            return self._owner._backend.job(job_id, 0)
        except NotFound:
            return None
        except Exception as e:  # noqa: BLE001 - keep watching; a daemon restarting may answer again
            self._owner._report(f"watching job {job_id} failed: {e!r}")
            return {}

    def _check(self, key: int, data: dict | None, callback, handle: Watch):
        """Queue callback for watch key if its job changed; forget the watch once the job is finished or gone."""
        if data == {}:
            return  # unreadable this time
        if data is None:
            handle.cancel()
            return
        with self._cond:
            if self._last.get(key) == data or key not in self._watches:
                return
            self._last[key] = data
        job = Job.from_dict(data, self._owner)

        def call():
            """The app's callback, unless cancelled since."""
            if handle.active:
                callback(job)
        self._owner._events.post(("job", key), call)
        if job.done:
            self._remove(key)

    def stop(self):
        """End the thread, waiting for it unless called from it."""
        with self._cond:
            self._stopped = True
            self._watches.clear()
            self._cond.notify_all()
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(STOP_WAIT)


class Rnfs:
    """What rnfs.Node and rnfs.Client both offer (design.md, "Public API"). Subclasses supply the backend and how
    name records are heard; see the module docstring for targets and callbacks. Thread-safe.

    Pins made and released here are "user"'s, but those of an app's Client (rnfs.Client(app=...)), which are the
    app's own: app:<app>, or app:<app>:<key> inside its pin_key(key)."""

    events_thread = "rnfs-events"
    jobs_thread = "rnfs-job-watch"
    job_poll = 0.5  # seconds between polls of watched jobs

    def __init__(self, backend):
        """The interface over backend (see the module docstring)."""
        self._backend = backend
        self._closed = False
        self._close_lock = threading.Lock()
        self._events = _Events(self.events_thread, self._report)
        self._jobs = _JobPoller(self, self.jobs_thread, self.job_poll)
        self._watch_lock = threading.Lock()  # taken after the names lock (records are heard under it), never before
        self._name_watches: dict[bytes, list[_NameWatch]] = {}
        self._event_watches: dict[int, _EventWatch] = {}  # under _watch_lock
        self._event_keys = itertools.count()  # each event watch's key, and each delivery's (never coalesced)
        self._node_id: bytes | None = None
        self.names = Names(self)
        self.jobs = Jobs(self)
        self.deny = Deny(self)
        self.contacts = Contacts(self)
        self.events = Events(self)

    # Lifecycle

    @property
    def closed(self) -> bool:
        """True once close() has been called."""
        return self._closed

    def close(self):
        """Stop the callbacks and release what the instance holds (idempotent; subclasses add their own)."""
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
        self._jobs.stop()
        self._events.stop()
        with self._watch_lock:
            watches = [watch for each in self._name_watches.values() for watch in each]
            self._name_watches.clear()
            event_watches = list(self._event_watches.values())
            self._event_watches.clear()
        for watch in watches:
            watch.handle.active = False
            self._unwatch_name(watch.name)
        for watch in event_watches:
            watch.handle.active = False
        self._close()

    def _close(self):
        """What a subclass releases at close()."""

    def __enter__(self):
        """The instance itself."""
        return self

    def __exit__(self, *exc):
        """close()."""
        self.close()

    def _check(self):
        """Raise Closed after close()."""
        if self._closed:
            raise Closed(f"this {type(self).__name__} is closed")

    def _report(self, message: str):
        """Log a callback's or a watch's failure (subclasses log where they log)."""

    def wait_joined(self, timeout: float | None = None) -> bool:
        """Wait until the node's first join of the DHT has finished; True if it has (a daemon's has)."""
        self._check()
        return self._backend.wait_joined(timeout)

    def timeout(self, seconds: float | None):
        """A context manager bounding each request a Client makes for this thread inside it to seconds (None: no
        bound), instead of the timeout the Client was made with: `with client.timeout(5): client.peers()`.

        Thread-local: other threads, and the event, job-watch and name-watch threads, keep the Client's. A long
        poll in it (Job.wait) waits within it, as within the Client's; a request outlasting it raises
        ConnectionFailed. Nests, the outer one back on exit. A Node makes no requests, so it only checks seconds:
        code using it runs unchanged on either class. ValueError unless None or a positive number."""
        return self._backend.timeout(seconds)

    # The node: what it offers, the daemon, settings

    def capabilities(self) -> Capabilities:
        """What the node offers: its version, capabilities (has("events"), ...), API routes, features loaded,
        event kinds and error codes. A daemon before 0.2.0 can't say: an empty Capabilities, every has() False. A
        Node's are a daemon's but "shutdown" and "daemon_json", which it can't do; its routes are the API's, whose
        operations it offers through its methods."""
        self._check()
        return Capabilities.from_dict(self._backend.capabilities())

    def shutdown(self, wait: float | None = None) -> bool:
        """Stop the daemon (a Client), as SIGTERM does; this Client stays usable once a daemon runs again. With
        wait, True once its API stops answering within wait seconds (False: still answering); without, True once
        it agreed. Raises Unsupported: from a Node (close() stops it), or a daemon that can't be stopped this way;
        NoSuchEndpoint (a NotFound) from a daemon before 0.2.0."""
        self._check()
        return self._backend.shutdown(wait)

    def daemon_info(self) -> dict | None:
        """What the running daemon wrote about itself in its rnfs dir (daemon.json: "pid", "api_port",
        "gateway_port", "version", "started", "rnfs_dir", "reticulum_config", "shared_instance", "node_id"), or None:
        none there (a daemon before 0.2.0), it can't be trusted or its process has ended; always None for a Node,
        which is no daemon. Read from the file, not asked of the daemon."""
        self._check()
        return self._backend.daemon_info()

    def config(self) -> Settings:
        """The node's settings: each with its schema, running value, rnfs.toml's, and whether the command line (a
        Node's settings) overrides it or a restart would change it; rnfs.toml's path and why a restart would refuse
        it, if it would. Raises NotFound without the settings feature (a daemon before 0.2.0: NoSuchEndpoint)."""
        self._check()
        return Settings.from_dict(self._backend.config())

    def config_set(self, settings: Mapping) -> SettingsChange:
        """Set settings ({name: value, None: removed from rnfs.toml, so back to the default}) in the node's
        rnfs.toml, applying at once those that apply live and that the command line doesn't override. Nothing is
        written unless the whole result is valid. Raises ValueError (unknown, refused or malformed settings), Busy
        (rnfs.toml changed meanwhile: try again), NotFound without the settings feature; a Node OSError, a Client
        ServerError, if rnfs.toml can't be written."""
        self._check()
        if not isinstance(settings, Mapping):
            raise ValueError("settings must be a dict of setting names and values (None: back to the default)")
        return SettingsChange.from_dict(self._backend.config_set(dict(settings)))

    # Targets

    def _target(self, target) -> tuple[str, str | None]:
        """(target as `<cid>[/name…]` or a nick path, the from= node ID in hex of an rnfs: URI, else None)."""
        if isinstance(target, Cid):
            return str(target), None
        if not isinstance(target, str):
            raise ValueError(f"a target is a Cid, <cid>[/path] or an rnfs: URI, not {type(target).__name__}")
        if target[:5].lower() != "rnfs:":
            return target, None
        from rnfs.uris import parse_uri  # no RNS
        link = parse_uri(target)
        cid = link.cid
        if link.name is not None:
            cid = self.names.resolve(link.name).cid
        from_node = link.from_node.hex() if link.from_node is not None else None
        return "/".join([str(cid), *link.path]), from_node

    def _from(self, from_node, hint: str | None) -> str | None:
        """The from_node given (as hex), else an rnfs: URI's."""
        return node_hex(from_node) if from_node is not None else hint

    # Status

    def status(self) -> Status:
        """The node's status."""
        self._check()
        return Status.from_dict(self._backend.status())

    @property
    def node_id(self) -> bytes:
        """The node's ID (its rnfs.dht destination hash)."""
        if self._node_id is None:
            self._check()
            self._node_id = bytes.fromhex(self._backend.node_id())
        return self._node_id

    # Content

    def add(self, source, *, name: str | None = None, wrap: bool = False, pin: bool = True, only_hash: bool = False,
            to: list[str] | None = None, to_self: bool = False, seal_name: bool = True,
            chunker: str | None = None) -> Added:
        """Add a file or directory (a path), or data (bytes, or a binary file read to its end), pinned "user"
        unless pin is False (then evictable cache); only_hash stores nothing.

        Data is wrapped in a directory as the entry name, if given. to/to_self seal a file for those recipients
        (section 5, Sealed files): a path's name is sealed inside unless seal_name is False, and data's name is
        sealed inside instead of wrapping it, unless wrap. chunker is "fixed" (default) or "fastcdc". Raises
        ValueError, NotFound (no such path), Denied (a file's hash is denied), StorageFull, OSError.
        """
        self._check()
        to, wrap, pin, only_hash, to_self, seal_name = (recipients(to), bool(wrap), bool(pin), bool(only_hash),
                                                       bool(to_self), bool(seal_name))
        sealing = bool(to or to_self)
        options = {"only_hash": only_hash, "pin": pin, "to": to, "to_self": to_self, "chunker": chunker}
        if isinstance(source, (str, os.PathLike)):
            if name is not None:
                raise ValueError("name is for adding data; a path is added under its own name")
            result = self._backend.add_path(_abspath(source), wrap=wrap, seal_name=seal_name, **options)
        else:
            if wrap and not name:
                raise ValueError("wrap needs name, the entry's name")
            if isinstance(source, (bytes, bytearray, memoryview)):
                data = bytes(source)
                length = len(data)  # a memoryview's len() counts its items, not its bytes
            elif hasattr(source, "read"):
                data, length = source, None
            else:
                raise ValueError(f"add() takes a path, bytes or a binary file, not {type(source).__name__}")
            entry = name if wrap or not sealing else None
            result = self._backend.add_stream(data, length, entry, seal_name=name if sealing and seal_name else None,
                                              **options)
        return Added.from_dict(result)

    def add_nomadnet(self, link: str, *, name: str | None = None, wrap: bool = False, pin: bool = True,
                     only_hash: bool = False, to: list[str] | None = None, to_self: bool = False,
                     seal_name: bool = True, chunker: str | None = None, identify: bool = False) -> Added:
        """Add a snapshot of what a NomadNet node serves at link (<32 hex destination>:/page/<path> or :/file/<path>)
        as add() adds data: a page as the node renders it for this request, a file as it sends it, fetched over a
        link of its own and bounded as specification section 11 says. It is named by the node's name for a file
        (the path's last component for a page, or one not fit to be a name), wrapped in a directory under that name
        with wrap, or under name if given; to/to_self seal it (that name inside unless seal_name is False, as add()
        seals data's). The link doesn't identify unless identify, then with the node's NomadNet identity (never its
        own). Each call takes a new snapshot. Raises ValueError (a malformed link or option), NotFound (unreachable,
        no answer, or the node's "Request Not Allowed" page), TooLarge, Denied, StorageFull."""
        self._check()
        if not isinstance(link, str):
            raise ValueError(f"a NomadNet link is a string, not {type(link).__name__}")
        options = {"wrap": bool(wrap), "pin": bool(pin), "only_hash": bool(only_hash), "to": recipients(to),
                   "to_self": bool(to_self), "seal_name": bool(seal_name), "chunker": chunker, "name": name or None,
                   "identify": bool(identify)}
        return Added.from_dict(self._backend.add_nomadnet(link, **options))

    def get(self, target, output, *, from_node=None, raw: bool = False, no_extract: bool = False) -> Written:
        """Fetch a file or directory and write it to output (a path that must not exist), all in this call. A
        sealed file this node can open is decrypted unless raw, and a sealed folder (as add seals a folder; a tar
        sealed as a file stays one) unpacked to a directory unless no_extract (Written.extracted; a daemon older than 0.2.0 never unpacks).
        Raises the fetch errors (NotFound, TooLarge, InvalidDag, Denied, StorageFull, Busy), ValueError (output
        exists), OSError."""
        self._check()
        text, hint = self._target(target)
        output = _abspath(output)
        result = self._backend.get(text, output, self._from(from_node, hint), raw, no_extract)
        return Written.from_dict({**result, "output": output})

    def open(self, target, *, from_node=None, raw: bool = False) -> "File":
        """A file to read and seek in, each read after a seek fetching only the chunks it covers, sequential reads
        reading ahead (sealed files read as plaintext unless raw). Raises the fetch errors, ValueError (a
        directory); reads raise the fetch errors."""
        self._check()
        text, hint = self._target(target)
        from_node = self._from(from_node, hint)
        info = Info.from_dict(self._backend.info(text, from_node, raw))
        if info.type != "file":
            raise ValueError(f"{text} is a directory")
        return File(_FileReader(self, text, info, from_node, raw))

    def cat(self, target, *, offset: int = 0, length: int | None = None, from_node=None, raw: bool = False):
        """An iterator of a file's bytes in [offset, offset + length), fetched as it is read (close() it to stop
        early). Raises the fetch errors and ValueError, possibly only once iterated."""
        self._check()
        text, hint = self._target(target)
        return self._backend.read(text, offset, length, raw, self._from(from_node, hint))

    def block(self, cid) -> bytes:
        """A CID's own block, fetched if needed and verified. Raises the fetch errors."""
        self._check()
        return self._backend.block(str(cid))

    def ls(self, target) -> list[Entry]:
        """A directory's entries in name order. Raises the fetch errors, ValueError (not a directory)."""
        self._check()
        return [Entry.from_dict(entry) for entry in self._backend.ls(self._target(target)[0])]

    def info(self, target, *, from_node=None, raw: bool = False) -> Info:
        """A target's own CID, type, size and executable bit (a sealed file's plaintext size if this node can
        open it, unless raw). Raises the fetch errors."""
        self._check()
        text, hint = self._target(target)
        return Info.from_dict(self._backend.info(text, self._from(from_node, hint), raw))

    def stat(self, target) -> Stat:
        """A target's size, block count and local holdings. Raises the fetch errors."""
        self._check()
        return Stat.from_dict(self._backend.stat(self._target(target)[0]))

    def dir_set(self, target, name: str, child, *, executable: bool = False) -> Cid:
        """A new directory, pinned "user": target's with entry name set to child (a Cid). Raises the fetch
        errors, ValueError."""
        self._check()
        return Cid.parse(self._backend.dir_set(self._target(target)[0], name, str(child), executable)["cid"])

    def dir_rm(self, target) -> Cid:
        """A new directory, pinned "user", without the entry target (`<cid>/<path>`) names. Raises the fetch
        errors, ValueError."""
        self._check()
        return Cid.parse(self._backend.dir_rm(self._target(target)[0])["cid"])

    # Pins

    def pin(self, target) -> Cid:
        """Fetch the target completely and pin it "user"; its CID once complete. Raises the fetch errors. For
        progress, jobs.pin()."""
        self._check()
        return Cid.parse(self._backend.pin_add(self._target(target)[0])["cid"])

    def unpin(self, cid):
        """Remove cid's "user" pin; withdrawn at once if no longer provided. Raises NotFound if it has none."""
        self._check()
        self._backend.pin_rm(str(cid))

    def pins(self, cid=None) -> list[Pin]:
        """Every pin, or cid's."""
        self._check()
        return [Pin.from_dict(pin) for pin in self._backend.pin_ls(str(cid) if cid is not None else None)]

    def label_pin(self, cid, label: str | None) -> Pin:
        """Give cid's pin a label (None or "" clears it), kept apart from where it was added from and lasting while
        it stays pinned; the pin. Raises NotFound (not pinned), ValueError (a label refused)."""
        self._check()
        return Pin.from_dict(self._backend.pin_label(str(cid), label))

    def pin_remote(self, node_id, cid, pin: bool = True) -> bool:
        """Ask node_id to pin (or release this node's pin of) cid; True if it accepted. Recorded for remote_pins()."""
        self._check()
        return bool(self._backend.pin_remote(node_hex(node_id), str(cid), pin))

    def remote_pins(self, node_id=None, cid=None, *, check: bool = False) -> list[RemotePin]:
        """The remote pins this node asked for (node_id's, cid's, or every one), most recently asked first, and
        whether each is done: inferred from provider records, so it lags the node's publishing. check: check them
        first, a provider lookup each (paced: at most 32 a call, 4 on a slow node, none checked in the last minute).
        Pins asked before rnfs 0.2.0 aren't listed. Raises NoSuchEndpoint from an older daemon."""
        self._check()
        entries = self._backend.pin_remote_ls(node_hex(node_id), str(cid) if cid is not None else None, check)
        return [RemotePin.from_dict(entry) for entry in entries]

    def served_pins(self) -> ServedPins:
        """The remote pins other identities asked of this node (pinned, waiting, fetching, and the latest outcomes
        of the others: dropped and why, refused, released) and each identity's use of remote_pin_quota. Raises
        NoSuchEndpoint from an older daemon."""
        self._check()
        return ServedPins.from_dict(self._backend.pin_remote_served())

    def gc(self) -> Collected:
        """Evict every cached fetch and delete unreachable blocks."""
        self._check()
        return Collected.from_dict(self._backend.gc())

    def verify(self) -> Verified:
        """Re-check every stored block against its CID, deleting bad ones."""
        self._check()
        return Verified.from_dict(self._backend.verify())

    def cache(self) -> list[Cached]:
        """The cached fetches (downloads not pinned, kept until evicted), most recently used first. Never
        fetches."""
        self._check()
        return [Cached.from_dict(entry) for entry in self._backend.cache_ls()]

    def uncache(self, cid) -> Uncached:
        """Evict the cached fetches leading to cid now, deleting the blocks nothing else needs (a pin of the same
        content keeps its own). Raises NotFound (no cached fetch leads to cid), ValueError."""
        self._check()
        return Uncached.from_dict(self._backend.cache_rm(str(cid)))

    # Network

    def providers(self, cid) -> list[bytes]:
        """Node IDs providing cid: records held here, then what a provider lookup finds."""
        self._check()
        return [bytes.fromhex(node_id) for node_id in self._backend.providers(str(cid))]

    def ping(self, node_id) -> float | None:
        """Round-trip seconds of a new link to node_id, or None if it can't be reached."""
        self._check()
        return self._backend.ping(node_hex(node_id))

    def peers(self) -> list[Peer]:
        """Every node this one knows (in its routing table, with a link open, or counted in its traffic), nearest
        first, with its path, link and traffic since the node started. Raises NotFound without the peers
        feature."""
        self._check()
        return [Peer.from_dict(peer) for peer in self._backend.peers()]

    def stats(self) -> Stats:
        """The node's traffic since it started: totals, refusals, per interface, the upload budget left. Raises
        NotFound without the peers feature."""
        self._check()
        return Stats.from_dict(self._backend.stats())

    # Archives

    def export(self, target, output) -> Exported:
        """Fetch the target if needed and write an archive of it to output (which must not exist). Raises the
        fetch errors, ValueError, OSError."""
        self._check()
        return Exported.from_dict(self._backend.export(self._target(target)[0], _abspath(output)))

    def import_archive(self, path) -> Imported:
        """Import an archive, pinning its complete roots "user". Raises NotFound, InvalidDag, Denied,
        StorageFull, ValueError, OSError."""
        self._check()
        return Imported.from_dict(self._backend.import_archive(_abspath(path)))

    # Streams: tars and archives through the program, for files the daemon can't open (or a Node's app has
    # in memory). A stream out is sent once iterated, as cat's, its target's whole DAG fetched first

    def add_tar(self, source, *, name: str | None = None, pin: bool = True, only_hash: bool = False,
                chunker: str | None = None) -> Added:
        """Add the directory a tar holds (bytes, or a binary file read up to the tar's end): its regular files
        and directories, the rest skipped with a warning; wrapped as the entry name if given, pinned "user" unless
        pin is False (evictable cache) or only_hash (nothing stored); chunker as add's. Raises ValueError (a
        malformed tar, an unknown chunker), Denied (a file's hash), TooLarge (max_output_entries), StorageFull."""
        self._check()
        return Added.from_dict(self._backend.add_tar(_body(source, "add_tar"), name or None, pin, only_hash, chunker))

    def get_tar(self, target):
        """An iterator of target (a file or directory) as a tar, files as stored (a sealed file sealed); entries
        whose names are unsafe in a tar are left out. The whole DAG is fetched first (max_download_size,
        max_output_entries), once iterated; close() it to stop early. Raises the fetch errors, ValueError,
        possibly only once iterated."""
        self._check()
        return self._backend.get_tar(self._target(target)[0])

    def export_stream(self, target):
        """An iterator of target's archive, byte for byte what export() writes, its whole DAG fetched first once
        iterated; close() it to stop early. Raises the fetch errors, ValueError, possibly only once iterated."""
        self._check()
        return self._backend.export_stream(self._target(target)[0])

    def import_stream(self, source) -> Imported:
        """Import an archive (bytes, or a binary file read to its end), as import_archive: its complete roots
        pinned "user". Raises InvalidDag, Denied, StorageFull, ValueError."""
        self._check()
        return Imported.from_dict(self._backend.import_stream(_body(source, "import_stream")))

    # Name watches (Names.watch)

    def _watch_name(self, name: bytes, callback) -> Watch:
        """Call callback(record) for each record of name newer than the one held now."""
        self._check()
        watch = _NameWatch(name, callback)
        watch.handle = Watch(lambda: self._end_name_watch(watch))
        self._events.start()
        with self._watch_lock:
            self._name_watches.setdefault(name, []).append(watch)
        try:
            self._watch_started(name)
            baseline = self._baseline(name)
        except BaseException:
            self._end_name_watch(watch)
            raise
        pending = watch.begin(baseline)
        if pending is not None:
            # A key of its own: a newer record's delivery already queued must not be replaced by it
            self._events.post(("name-pending", id(watch)), lambda: watch.deliver(pending))
        return watch.handle

    def _end_name_watch(self, watch: _NameWatch):
        """Forget a cancelled watch."""
        watch.handle.active = False
        with self._watch_lock:
            watches = self._name_watches.get(watch.name, [])
            if watch not in watches:
                return
            watches.remove(watch)
            if not watches:
                del self._name_watches[watch.name]
        self._unwatch_name(watch.name)

    def _record_heard(self, name: bytes, record: Record):
        """A newer record of name was heard: queue its delivery to each watch (any thread; never blocks)."""
        with self._watch_lock:
            watches = list(self._name_watches.get(name, ()))
        for watch in watches:
            self._events.post(("name", id(watch)), lambda watch=watch: watch.deliver(record))

    def _watched_names(self) -> list[bytes]:
        """The names watched now."""
        with self._watch_lock:
            return list(self._name_watches)

    def _last_sequences(self, names: list[bytes]) -> list[int | None]:
        """For each name, the lowest sequence any of its watches last delivered (None: one has none)."""
        with self._watch_lock:
            watches = {name: list(self._name_watches.get(name, ())) for name in names}
        result = []
        for name in names:
            lasts = [watch.last for watch in watches[name]]
            result.append(None if not lasts or None in lasts else min(lasts))
        return result

    def _watch_started(self, name: bytes):
        """A watch of name began: have the node treat it as watched (subclasses)."""

    def _unwatch_name(self, name: bytes):
        """A watch of name ended (subclasses)."""

    def _baseline(self, name: bytes) -> int | None:
        """The sequence of name's record held now, or None (subclasses)."""
        return None

    # Event watches (Events.watch)

    def _watch_events(self, kinds: frozenset | None, callback) -> Watch:
        """Call callback(event) for each event of kinds (None: every kind) published from now on. The watch is
        registered before its baseline is asked for, so whatever is heard meanwhile waits in it, not lost."""
        self._check()
        key = next(self._event_keys)
        watch = _EventWatch(kinds, callback)
        watch.handle = Watch(lambda: self._end_event_watch(key))
        self._events.start()
        with self._watch_lock:
            self._event_watches[key] = watch
        try:
            after, epoch = self._event_baseline()
        except BaseException:
            watch.handle.cancel()
            raise
        watch.begin(after, epoch, lambda event: self._deliver_event(watch, event))
        return watch.handle

    def _end_event_watch(self, key: int):
        """Forget a cancelled event watch."""
        with self._watch_lock:
            self._event_watches.pop(key, None)

    def _event_watch_count(self) -> int:
        """How many event watches there are (begun or not)."""
        with self._watch_lock:
            return len(self._event_watches)

    def _events_heard(self, epoch: str, events: list[dict]):
        """Events of the node's run epoch were heard, in order: queue each for every watch wanting it (any
        thread; never blocks)."""
        with self._watch_lock:
            watches = list(self._event_watches.values())
        for watch in watches:
            for event in events:
                watch.offer(epoch, event, lambda event, watch=watch: self._deliver_event(watch, event))

    def _events_lost(self, next_seq: int, epoch: str):
        """Events up to next_seq were missed (the watcher fell behind, or the node's run is now epoch): queue the
        "lost" marker for every watch whose position is before it."""
        self._events_heard(epoch, [{"seq": next_seq, "kind": "lost", "time": None, "epoch": epoch}])

    def _deliver_event(self, watch: "_EventWatch", event: dict):
        """Queue watch's callback with event, under a key of its own: events are never coalesced."""
        value = Event.from_dict(event)

        def call():
            """The app's callback, unless cancelled since."""
            if watch.handle.active:
                watch.callback(value)
        self._events.post(("event", next(self._event_keys)), call)

    def _event_baseline(self) -> tuple[int, str]:
        """(the sequence of the last event published, the run's epoch) now, the event source started if needed
        (subclasses)."""
        raise NotImplementedError


class _FileReader(io.RawIOBase):
    """File's raw stream: each refill is one read of the target opened from the position, of what readinto asks
    for, or (read sequentially) of up to READ_AHEAD bytes ahead, kept for the next readintos.

    By the target (root and path), not the file's own CID: providers announce roots (provide_strategy), so the
    file's CID may have none."""

    def __init__(self, owner: Rnfs, target: str, info: Info, from_node: str | None, raw: bool):
        """A reader of the file target names, whose info open() read."""
        super().__init__()
        self.owner = owner
        self.name = target
        self.info = info
        self.from_node = from_node
        self.raw = raw
        self.position = 0
        self._ahead = memoryview(b"")  # bytes read from the position on but not handed out yet
        self._fetched_to: int | None = None  # where the last read ended, while no seek has moved the position
        self._window = FILE_BUFFER  # the last refill's read-ahead size, doubled while reads are sequential

    def readable(self) -> bool:
        """True."""
        return True

    def seekable(self) -> bool:
        """True."""
        return True

    def readinto(self, buffer) -> int:
        """Fill buffer from the position, from what was read ahead or else one read; 0 at the end."""
        wanted = min(len(buffer), self.info.size - self.position)
        if wanted <= 0:
            return 0
        if not self._ahead:
            self._ahead = memoryview(self._read(self._refill_size(wanted)))
        count = min(wanted, len(self._ahead))
        buffer[:count] = self._ahead[:count]
        self._ahead = self._ahead[count:]
        self.position += count
        return count

    def readall(self) -> bytes:
        """The rest of the file in one read (RawIOBase's would make one per 8 KiB), after what was read ahead."""
        chunks = [bytes(self._ahead)]
        self.position += len(self._ahead)
        self._ahead = memoryview(b"")
        while self.position < self.info.size:
            data = self._read(self.info.size - self.position)
            chunks.append(data)
            self.position += len(data)
        return b"".join(chunks)

    def _refill_size(self, wanted: int) -> int:
        """How much a refill of wanted bytes reads: just those, unless it continues where the last read ended.
        Then the read-ahead doubles (up to READ_AHEAD), so a sequential reader makes few requests, and no more
        than about what it has already read is fetched beyond what it reads."""
        if self._fetched_to != self.position:
            return wanted
        self._window = min(2 * self._window, READ_AHEAD)
        return min(max(wanted, self._window), self.info.size - self.position)

    def _read(self, length: int) -> bytes:
        """One read of [position, position + length), fetching only what it covers. Raises RnfsError if the
        file ends before it."""
        self.owner._check()
        chunks = self.owner._backend.read(self.name, self.position, length, self.raw, self.from_node)
        try:
            data = b"".join(chunks)
        finally:
            close = getattr(chunks, "close", None)
            if close is not None:
                close()
        if not data:
            raise RnfsError(f"{self.name} ended at {self.position} of its {self.info.size} bytes")
        self._fetched_to = self.position + len(data)
        return data

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """Move the position (past the end is allowed: reads there return nothing). A move drops what was read
        ahead, and the next refill reads only what it covers."""
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self.position, io.SEEK_END: self.info.size}.get(whence)
        if base is None:
            raise ValueError(f"invalid whence {whence!r}")
        if base + offset < 0:
            raise ValueError("negative seek position")
        if base + offset != self.position:
            self._ahead = memoryview(b"")
            self._fetched_to = None
            self._window = FILE_BUFFER
        self.position = base + offset
        return self.position

    def tell(self) -> int:
        """The position."""
        return self.position

    def close(self):
        """Close the stream, dropping what was read ahead."""
        self._ahead = memoryview(b"")
        super().close()


class File(io.BufferedReader):
    """A file in rnfs, from open(): read, readinto, readline, iteration, seek and tell, buffered by FILE_BUFFER
    bytes. A refill after open or a seek fetches only the chunks it covers; each refill continuing the last reads
    ahead twice as far, up to READ_AHEAD. Reads go by the target opened (an rnfs: name URI
    resolved once, at open; a contact nick is expanded by the node, which reuses its CID for 60 s). cid is the
    file's own CID, size its size, info its Info, as open() found them. Not thread-safe, like any file object."""

    def __init__(self, reader: _FileReader):
        """The buffered file over reader."""
        super().__init__(reader, FILE_BUFFER)

    @property
    def cid(self) -> Cid:
        """The file's own CID."""
        return self.raw.info.cid

    @property
    def size(self) -> int:
        """The file's size (a sealed file's plaintext size, unless opened raw)."""
        return self.raw.info.size

    @property
    def info(self) -> Info:
        """What open() learnt of the file."""
        return self.raw.info


@dataclass(frozen=True)
class Name:
    """A name of this node: its alias, the name (16 bytes; str() is its hex), the CID and sequence it last
    published (None and 0 before a publish), and whether its key was ever exported or imported (False: losing
    this node's copy would lose the name for good). A snapshot: refresh() reads it again."""

    alias: str
    name: bytes
    cid: Cid | None = None
    sequence: int = 0
    _owner: Rnfs | None = field(default=None, repr=False, compare=False)
    exported: bool = False

    def __str__(self) -> str:
        """The name in hex."""
        return self.name.hex()

    @property
    def hex(self) -> str:
        """The name as 32 hex characters."""
        return self.name.hex()

    @classmethod
    def from_dict(cls, data: dict, owner: Rnfs | None = None) -> "Name":
        """From the API's form: {"alias", "name", "cid", "sequence", "exported"} ("exported" is missing from an
        older daemon's: False)."""
        cid = data.get("cid")
        return cls(data["alias"], bytes.fromhex(data["name"]), Cid.parse(cid) if cid else None,
                   data.get("sequence", 0), owner, bool(data.get("exported", False)))

    def to_dict(self) -> dict:
        """The API's form."""
        return {"alias": self.alias, "name": self.name.hex(), "cid": str(self.cid) if self.cid else None,
                "sequence": self.sequence, "exported": self.exported}

    def publish(self, target) -> Record:
        """Point the name at target (a path in it resolved locally) and announce it; the new record. Raises
        NotFound (a path not stored here), ValueError, RnfsError (the highest sequence reached). In private mode
        the node's warning that the announce is visible outside the private network is raised as a
        RuntimeWarning (warnings.warn), as the CLI prints it; so is the reason the announce is held, if the
        node's clock is behind the name's last announce (the record is published, and announced once it passes)."""
        self._owner._check()
        result = self._owner._backend.name_publish(self.alias, self._owner._target(target)[0])
        for key in ("warning", "announce_held"):
            if result.get(key):
                warnings.warn(result[key], RuntimeWarning, stacklevel=2)
        return Record.from_dict({**result, "age": 0.0})

    def export_key(self) -> bytes:
        """The name's private key: whoever holds it can publish the name."""
        self._owner._check()
        return self._owner._backend.name_export(self.alias)

    def remove(self):
        """Delete the name's key, releasing its pin. Raises NotFound."""
        self._owner._check()
        self._owner._backend.name_rm(self.alias)

    def refresh(self) -> "Name":
        """The name as it is now. Raises NotFound if it was removed."""
        return self._owner.names.get(self.alias)


class Names:
    """A node's names (Rnfs.names): its own, and resolving and watching anyone's."""

    def __init__(self, owner: Rnfs):
        """The names of owner."""
        self._owner = owner

    def create(self, alias: str) -> Name:
        """A new name key under alias. Raises ValueError (alias invalid or taken)."""
        self._owner._check()
        return Name.from_dict(self._owner._backend.name_create(alias), self._owner)

    def import_key(self, alias: str, key: bytes) -> Name:
        """A name key exported elsewhere, stored under alias. Raises ValueError (alias, or a malformed key)."""
        self._owner._check()
        return self.get(Name.from_dict(self._owner._backend.name_import(alias, key)).alias)

    def get(self, alias: str) -> Name:
        """This node's name called alias. Raises NotFound."""
        for name in self.list():
            if name.alias == alias:
                return name
        raise NotFound(f"no name called {alias}")

    def list(self) -> list[Name]:
        """This node's names, by alias."""
        self._owner._check()
        return [Name.from_dict(name, self._owner) for name in self._owner._backend.name_list()]

    def remove(self, alias: str):
        """Delete the name key alias, releasing its pin. Raises NotFound."""
        self._owner._check()
        self._owner._backend.name_rm(alias)

    def resolve(self, name) -> Record:
        """The newest record of name (16 bytes, 32 hex characters, a Name or a name contact's nick), after
        asking the network; persisted, so it never goes back. Raises NotFound (unresolved), ValueError."""
        self._owner._check()
        return Record.from_dict(self._owner._backend.name_resolve(name_arg(name)))

    def watch(self, name, callback) -> Watch:
        """Call callback(record) on the event thread for each record of name (16 bytes, 32 hex characters or a
        Name) newer than the one held now, as announces, path responses and keepers' answers are heard (and
        this node's own publishes). Only listens: resolve() asks. Raises ValueError; a Client NotFound from a
        daemon that can't watch names."""
        return self._owner._watch_name(name_bytes(name), callback)

    def sync(self, alias: str, path) -> FolderSync:
        """Keep the name alias pointing at the folder at path (on the node's machine; a relative path is made
        absolute here): every scan_interval the folder is imported (FastCDC), and when its content changed the name
        is published to it, once it has been unchanged a minute and at most once an hour (rnfs.name_sync). Syncing
        the name again to another folder replaces its folder. Raises NotFound (no such name or folder), ValueError
        (not a folder, or one inside the rnfs directory). In private mode the node's warning that each publish's
        announce is visible outside the private network is raised as a RuntimeWarning, as publish does."""
        self._owner._check()
        result = self._owner._backend.name_sync(alias, _abspath(path))
        if result.get("warning"):
            warnings.warn(result["warning"], RuntimeWarning, stacklevel=2)
        return FolderSync.from_dict(result)

    def unsync(self, alias: str):
        """Stop syncing the name alias to its folder; it keeps pointing where it last did. Raises NotFound (not
        synced)."""
        self._owner._check()
        self._owner._backend.name_unsync(alias)

    def syncs(self) -> "list[FolderSync]":
        """Every name synced to a folder, with how its sync stands, by alias."""
        self._owner._check()
        return [FolderSync.from_dict(entry) for entry in self._owner._backend.name_syncs()]

    # Keepers and follow (rnfs/features/keepers.py, follow.py). After list(), so their annotations name the
    # builtin through strings

    def keep(self, name) -> Kept:
        """Keep name (16 bytes, 32 hex characters, a Name or a name contact's nick): hold and serve its signed
        record so it stays resolvable while its owner is offline (section 9), asking keepers for it in the
        background if none is held. Raises ValueError (a malformed name or unknown nick, keep_max_names reached),
        NotFound without the keepers feature."""
        self._owner._check()
        return Kept.from_dict(self._owner._backend.name_keep(name_arg(name)))

    def unkeep(self, name):
        """Stop keeping name for the user (a name followed stays kept for that). Raises NotFound (not kept by the
        user, or no keepers feature), ValueError."""
        self._owner._check()
        self._owner._backend.name_unkeep(name_arg(name))

    def kept(self) -> "list[Kept]":
        """Every name kept, whatever for, with the record held."""
        self._owner._check()
        return [Kept.from_dict(entry) for entry in self._owner._backend.name_kept()]

    def follow(self, name) -> Followed:
        """Follow name (as keep takes it): keep it and keep its latest target pinned, fetched in the background as
        the name moves (within follow_quota). Raises ValueError (as keep), NotFound without the follow feature."""
        self._owner._check()
        return Followed.from_dict(self._owner._backend.name_follow(name_arg(name)))

    def unfollow(self, name):
        """Stop following name, releasing its pin. Raises NotFound (not followed, or no follow feature),
        ValueError."""
        self._owner._check()
        self._owner._backend.name_unfollow(name_arg(name))

    def following(self) -> "list[Followed]":
        """Every name followed, with the target pinned for it and what its last check did."""
        self._owner._check()
        return [Followed.from_dict(entry) for entry in self._owner._backend.name_following()]


@dataclass(frozen=True)
class Job:
    """A background transfer (get, pin, export, add or import) the node runs, persists and resumes after a restart:
    its record as of the last refresh(), wait() or callback. done is True once it has finished (state "done",
    "failed" or "cancelled"); result is what the matching call returns, error why it failed. An add or import job
    has a path (and an add its options) instead of a target; an add of a NomadNet link has that link (nomadnet)
    instead of a path. source is what a pin, add or import job pins with ("user", or the app's that queued it)."""

    id: str
    kind: str
    target: str | None
    output: str | None = None
    from_node: bytes | None = None
    sealed_name: bool = False
    fallback_dht: bool = False
    path: str | None = None
    options: dict | None = None
    no_extract: bool = False
    source: str = "user"
    nomadnet: str | None = None
    state: str = "queued"
    created: float | None = None
    started: float | None = None
    finished: float | None = None
    error: str | None = None
    result: dict | None = None
    runs: int = 0
    total_bytes: int | None = None
    done_bytes: int = 0
    blocks: int = 0
    blocks_total: int | None = None
    downloaded_bytes: int = 0
    rate: float | None = None
    eta: float | None = None
    _owner: Rnfs | None = field(default=None, repr=False, compare=False)

    FIELDS = ("id", "kind", "target", "output", "sealed_name", "fallback_dht", "path", "options", "no_extract",
              "source", "nomadnet", "state", "created", "started",
              "finished", "error", "result", "runs", "total_bytes", "done_bytes", "blocks", "blocks_total",
              "downloaded_bytes", "rate", "eta")

    @classmethod
    def from_dict(cls, data: dict, owner: Rnfs | None = None) -> "Job":
        """From the API's form (its "from" is from_node)."""
        values = {name: data[name] for name in cls.FIELDS if name in data}
        return cls(**values, from_node=bytes.fromhex(data["from"]) if data.get("from") else None, _owner=owner)

    def to_dict(self) -> dict:
        """The API's form."""
        return {**{name: getattr(self, name) for name in self.FIELDS},
                "from": self.from_node.hex() if self.from_node is not None else None}

    @property
    def done(self) -> bool:
        """True once the job has finished."""
        return self.state in FINISHED

    def refresh(self) -> "Job":
        """The job as it is now. Raises NotFound once cleared."""
        return self._owner.jobs.job(self.id)

    def wait(self, timeout: float | None = None) -> "Job":
        """The job once it has finished (check state and error). Raises TimeoutError if it hasn't within timeout
        seconds, NotFound once cleared."""
        deadline = None if timeout is None else time.monotonic() + timeout
        job = self
        while not job.done:
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                raise TimeoutError(f"job {self.id} hasn't finished")
            job = self._owner.jobs._job(self.id, MAX_JOB_WAIT if remaining is None else min(MAX_JOB_WAIT, remaining))
        return job

    def cancel(self) -> "Job":
        """Cancel the job: a queued one at once, a running one at its fetch's next step."""
        self._owner._check()
        return Job.from_dict(self._owner._backend.cancel_job(self.id), self._owner)

    def watch(self, callback) -> Watch:
        """Call callback(job) on the event thread whenever the job changes (at most every job_poll seconds) and
        once when it finishes."""
        self._owner._check()
        self._owner._events.start()
        return self._owner._jobs.add(self.id, callback)


class Jobs:
    """A node's background jobs (Rnfs.jobs): transfers with progress, kept across restarts."""

    def __init__(self, owner: Rnfs):
        """The jobs of owner."""
        self._owner = owner

    def get(self, target, output, *, from_node=None, sealed_name: bool = False, fallback_dht: bool = False,
            on_progress=None, no_extract: bool = False) -> Job:
        """Fetch target and write it to output (which must not exist) as a job. sealed_name writes a sealed
        file this node opens under its sealed name in output's directory (a sealed folder unpacked: under the
        name of the directory written); fallback_dht retries by DHT discovery if from_node fails; no_extract
        writes a sealed folder as its tar instead of unpacking it. on_progress(job) as Job.watch. Raises
        ValueError (output exists or in use), Busy."""
        return self._submit("get", target, _abspath(output), from_node, on_progress, sealed_name=sealed_name,
                            fallback_dht=fallback_dht, no_extract=no_extract)

    def pin(self, target, *, on_progress=None) -> Job:
        """Fetch target completely and pin it "user", as a job."""
        return self._submit("pin", target, None, None, on_progress)

    def export(self, target, output, *, on_progress=None) -> Job:
        """Write an archive of target to output (which must not exist), as a job."""
        return self._submit("export", target, _abspath(output), None, on_progress)

    def add(self, path, *, wrap: bool = False, pin: bool = True, only_hash: bool = False,
            to: list[str] | None = None, to_self: bool = False, seal_name: bool = True, chunker: str | None = None,
            on_progress=None) -> Job:
        """Add a file or directory on the node's machine as a job: add()'s options for a path, its result add()'s
        Added as a dict; progress counts the bytes read of the size measured first. An interrupted add runs again
        from the start (adding is idempotent). Raises NotFound (no such path), ValueError (an option), Busy."""
        # Flags as bools and recipients checked here, so both classes refuse alike (a Client's JSON would carry
        # wrap=1 as 1, which the daemon takes, while a Node's job refuses it)
        options = {"wrap": True if wrap else None, "pin": None if pin else False,
                   "only_hash": True if only_hash else None, "to": recipients(to),
                   "to_self": True if to_self else None, "seal_name": None if seal_name else False,
                   "chunker": chunker}
        return self._submit("add", None, None, None, on_progress, path=_abspath(path),
                            options={name: value for name, value in options.items() if value is not None} or None)

    def add_nomadnet(self, link: str, *, name: str | None = None, wrap: bool = False, pin: bool = True,
                     only_hash: bool = False, to: list[str] | None = None, to_self: bool = False,
                     seal_name: bool = True, chunker: str | None = None, identify: bool = False,
                     on_progress=None) -> Job:
        """Add a snapshot of a NomadNet link as a job: add_nomadnet()'s options, its result add_nomadnet()'s Added as
        a dict; progress counts the bytes of the node's answer received. An interrupted one fetches a new snapshot
        when it runs again. Raises ValueError (a malformed link or option), Busy."""
        if not isinstance(link, str):
            raise ValueError(f"a NomadNet link is a string, not {type(link).__name__}")
        options = {"wrap": True if wrap else None, "pin": None if pin else False,
                   "only_hash": True if only_hash else None, "to": recipients(to),
                   "to_self": True if to_self else None, "seal_name": None if seal_name else False,
                   "chunker": chunker, "name": name or None, "identify": True if identify else None}
        return self._submit("add", None, None, None, on_progress, nomadnet=link,
                            options={key: value for key, value in options.items() if value is not None} or None)

    def import_archive(self, path, *, on_progress=None) -> Job:
        """Import an archive on the node's machine as a job (import_archive()'s result as a dict); progress counts
        the archive's bytes read. Raises NotFound (no such file), Busy."""
        return self._submit("import", None, None, None, on_progress, path=_abspath(path))

    def _submit(self, kind: str, target, output: str | None, from_node, on_progress, **flags) -> Job:
        """Queue a job (target None for add and import, which take path= instead); with on_progress, watched from
        the start."""
        self._owner._check()
        text, hint = self._owner._target(target) if target is not None else (None, None)
        from_node = self._owner._from(from_node, hint) if kind == "get" else None
        job = Job.from_dict(self._owner._backend.submit_job(kind, text, output, from_node, **flags), self._owner)
        if on_progress is not None:
            job.watch(on_progress)
        return job

    def job(self, job_id: str) -> Job:
        """The job with this ID. Raises NotFound."""
        return self._job(job_id, 0)

    def _job(self, job_id: str, wait: float) -> Job:
        """The job, after waiting up to wait seconds for it to finish."""
        self._owner._check()
        return Job.from_dict(self._owner._backend.job(job_id, wait), self._owner)

    def list(self) -> list[Job]:
        """Every job, oldest first."""
        self._owner._check()
        return [Job.from_dict(job, self._owner) for job in self._owner._backend.jobs()]

    def clear(self, job_id: str | None = None) -> int:
        """Forget finished jobs (or just job_id, which must have finished); how many. Raises NotFound,
        ValueError (not finished)."""
        self._owner._check()
        return self._owner._backend.clear_jobs(job_id)


class Deny:
    """A node's deny list (Rnfs.deny, section 8): content it never fetches, stores, serves or provides."""

    def __init__(self, owner: Rnfs):
        """The deny list of owner."""
        self._owner = owner

    def add(self, entries: list[str]) -> list[str]:
        """Add CIDs or file hashes (sha256:<hex>) to the local list; those that were new. Raises ValueError."""
        self._owner._check()
        return self._owner._backend.deny_add([str(entry) for entry in entries])

    def remove(self, entries: list[str]) -> list[str]:
        """Remove entries from the local list; those removed. Raises NotFound (removing none) if one isn't on it,
        ValueError."""
        self._owner._check()
        return self._owner._backend.deny_rm([str(entry) for entry in entries])

    # After the methods above, whose annotations name the builtin list
    def list(self) -> DenyState:
        """The local entries, and each deny_lists source with its state."""
        self._owner._check()
        return DenyState.from_dict(self._owner._backend.deny())


class Contacts:
    """A node's address book (Rnfs.contacts; rnfs/features/contacts.py): nicks for names, nodes and LXMF
    addresses, taken wherever the node takes one of those (a name contact's nick also starts a target). A nick is
    1-32 lowercase letters, digits, '-', '_' or '.', starting with a letter or digit. Every method raises NotFound
    without the contacts feature."""

    def __init__(self, owner: Rnfs):
        """The address book of owner."""
        self._owner = owner

    def get(self, nick: str) -> Contact:
        """The contact called nick. Raises NotFound (none), ValueError (not a nick)."""
        self._owner._check()
        return Contact.from_dict(self._owner._backend.contact_get(nick))

    def add(self, nick: str, id, kind: str | None = None) -> Contact:
        """Add or replace the contact nick for id (16 bytes, 32 hex characters or a Name) of kind "name", "node"
        or "lxmf"; None has the node tell what id is (one of its names or a name it knows, a node it knows, an
        RNS destination it has heard). Raises ValueError (nick, id or kind invalid, a kind it can't tell, the
        address book full)."""
        self._owner._check()
        return Contact.from_dict(self._owner._backend.contact_add(nick, hash_arg(id), kind))

    def remove(self, nick: str):
        """Delete the contact nick. Raises NotFound (none), ValueError (not a nick)."""
        self._owner._check()
        self._owner._backend.contact_rm(nick)

    # After the methods above, whose annotations name the builtin list
    def list(self) -> list[Contact]:
        """Every contact, by nick."""
        self._owner._check()
        return [Contact.from_dict(contact) for contact in self._owner._backend.contacts()]


def _whole(value, name: str, none: bool = False) -> int | None:
    """value if it is a whole number of at least 0 (or None, if none); ValueError otherwise, as the API's
    parse_int refuses it."""
    if value is None and none:
        return None
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


class Events:
    """What changes on a node (Rnfs.events; rnfs/events.py; design.md, "Events"): jobs, pins, the cache, names,
    peers and remote pins, each an Event, so an app follows them instead of polling every list. A Node hears its
    own as they are published; a Client long-polls GET /api/events. A daemon before 0.2.0 has no events:
    NoSuchEndpoint (a NotFound)."""

    def __init__(self, owner: Rnfs):
        """The events of owner."""
        self._owner = owner

    def watch(self, callback, kinds=None) -> Watch:
        """Call callback(event) on the event thread for each event of kinds (a kind or several; None: all)
        published from now on, in order, one call each. If events were missed (a Client fell more than the node's
        1024 kept events behind, or the daemon restarted), callback gets an Event of kind "lost" (event.lost) in
        their place: read again what the app shows. A Client polls on a thread of its own
        (rnfs-client-event-watch), within its timeout as the name poller does. Raises ValueError (an unknown
        kind); a Client NoSuchEndpoint from a daemon without events."""
        return self._owner._watch_events(frozenset(kinds) if (kinds := event_kinds(kinds)) else None, callback)

    def since(self, after: int | None = None, kinds=None, wait: int = 0, epoch: str | None = None) -> EventBatch:
        """The events after sequence after (of kinds, if given) as soon as there is one, or none after wait whole
        seconds (at most 30; a Client's within its timeout, as Job.wait's). after None answers at once with only
        the next to ask after. Pass back the batch's next and epoch: lost then says events were missed (the
        node's run changed, or they are no longer held), and events holds every one held. Raises ValueError; a
        Client NoSuchEndpoint from a daemon without events, Busy while 8 other polls wait."""
        self._owner._check()
        kinds = event_kinds(kinds)
        # epoch "" is no epoch, as the API reads an empty parameter (a Node's log would call it another run)
        data = self._owner._backend.events_since(_whole(after, "after", none=True), kinds, _whole(wait, "wait"),
                                                 epoch or None)
        return EventBatch.from_dict(data)
