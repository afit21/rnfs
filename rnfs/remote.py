"""rnfs.Client: the user's running rnfs daemon, driven through its local API (design.md, "Public API";
provisional until 0.2). Standard library only, and it never imports RNS.

The same methods, objects and exceptions as rnfs.Node (rnfs.interface), over rnfs.apiclient's ApiConnection:
the token is read from the rnfs dir and sent only once whatever listens on the API port has proved it knows it.
Besides the node's errors (each raised as its ApiError subclass), calls may raise DaemonNotRunning,
ConnectionFailed, TokenError, AuthFailed and ServerError. Paths are opened by the daemon, on this machine, as
the user running it.

Callbacks run on the client's event thread, as rnfs.Node's do. Watched jobs are polled once a second (GET
/api/jobs/get), and watched names through GET /api/name/watch: a long poll on a thread of its own
(rnfs-client-name-watch) naming every name watched, so a name watched while one is waiting joins the next poll,
within MAX_WATCH_WAIT seconds, and nothing heard meanwhile is lost (the daemon answers from the records it holds).
With a timeout, a long poll (names, Job.wait) waits at most LONG_POLL_MARGIN seconds less than it, so the request
never outlasts its own socket; one that leaves no wait at all polls every second instead.

Watched events come from GET /api/events, long-polled the same way on a thread of its own (rnfs-client-event-watch)
for every kind, each watch keeping those it wants: it starts where the first watch's baseline (a call without
after, made inside watch()) is, passes back the "next" and "epoch" each answer gives, and on "lost" (it fell more
than the daemon's kept events behind, or the daemon restarted) tells each watch so and goes on from the answer's
"next". It stops polling once no watch is left. A failed poll is reported and retried after RETRY_DELAY.

Timeouts: the client's (timeout=) bounds every request; timeout(seconds) replaces it for the requests one thread
makes inside it (ApiConnection.using_timeout, thread-local), Job.wait's and events.since's long polls included,
each fitted to the timeout in force when it is sent (long_poll_wait). The name and event pollers, on their own
threads, keep the client's.
"""
import logging
import math
import threading
import time

from rnfs import interface
from rnfs.apiclient import ApiConnection
from rnfs.fsutil import printable
from rnfs.objects import Record

MAX_WATCH_WAIT = 30  # seconds one GET /api/name/watch or /api/jobs/get waits (the API's cap)
LONG_POLL_MARGIN = 2  # seconds a long poll leaves of the request's timeout
RETRY_DELAY = 5.0  # seconds before polling names again after a failure
SHORT_POLL = 1.0  # seconds between polls when the timeout leaves no wait

_logger = logging.getLogger("rnfs")


def long_poll_wait(timeout: float | None) -> int:
    """The longest (whole) seconds a long poll may ask the daemon to wait within a request timeout: MAX_WATCH_WAIT
    without one, else LONG_POLL_MARGIN less than it, rounded down (0: it leaves none)."""
    return MAX_WATCH_WAIT if timeout is None else max(0, min(MAX_WATCH_WAIT, math.floor(timeout) - LONG_POLL_MARGIN))


class Client(interface.Rnfs):
    """The daemon using rnfs_dir (default ~/.rnfs/), on 127.0.0.1:port (default: the api_port in its rnfs.toml),
    with the token in token_file (default <rnfs_dir>/api_token). timeout bounds each request (default: none, as
    fetches can take hours; timeout() sets another for one thread's calls). Nothing is sent until the first call.
    Raises SettingsError if rnfs.toml can't be read for the port."""

    events_thread = "rnfs-client-events"
    jobs_thread = "rnfs-client-job-watch"
    job_poll = 1.0  # what the CLI's progress line polls at

    def __init__(self, rnfs_dir: str | None = None, *, port: int | None = None, token_file: str | None = None,
                 timeout: float | None = None):
        """A client of the daemon (see the class)."""
        self._api = ApiConnection(rnfs_dir, port=port, token_file=token_file, timeout=timeout)
        # The longest the name poller's long poll may wait within the client's timeout (its thread has no other)
        self._long_wait = long_poll_wait(timeout)
        super().__init__(_Backend(self._api))
        self._poll_cond = threading.Condition()  # taken before _watch_lock, never under it
        self._poller: threading.Thread | None = None
        self._names_changed = False
        self._event_poller: threading.Thread | None = None
        self._baseline_lock = threading.Lock()  # one event baseline at a time (_event_baseline); taken first
        # (the sequence the event poller asks after, the daemon run's epoch); None while no event watch needs it
        self._event_position: tuple[int, str] | None = None
        # The highest sequence each name's poll has answered: the next asks above it, so a delivery still waiting
        # on the event thread isn't fetched again and again (only the poller uses it)
        self._polled: dict[bytes, int] = {}

    @property
    def api(self) -> ApiConnection:
        """The API connection underneath, whose get(path, **params) and post(path, **fields) reach every endpoint.
        Unsupported: it changes with any release."""
        return self._api

    def _report(self, message: str):
        """Log through Python's logging (logger "rnfs"), escaped: the daemon's message can quote names from
        other people's DAGs."""
        _logger.error("rnfs: %s", printable(message))

    def _close(self):
        """Wake the name and event pollers so they end."""
        with self._poll_cond:
            self._poll_cond.notify_all()

    # Name watches

    def _baseline(self, name: bytes) -> int | None:
        """The sequence of the record the daemon holds for name now (this call also watches it there)."""
        records = self._api.name_watch([name.hex()], [None], 0)
        return records[0]["sequence"] if records else None

    def _watch_started(self, name: bytes):
        """Start the name poller (if needed), which names every watched name in its next call."""
        with self._poll_cond:
            self._names_changed = True
            if self._poller is None:
                self._poller = threading.Thread(target=self._poll_names, name="rnfs-client-name-watch", daemon=True)
                self._poller.start()
            self._poll_cond.notify_all()

    def _poll_names(self):
        """Long-poll the daemon for every watched name's newer records until closed."""
        while not self.closed:
            names = self._watched_names()
            if not names:
                with self._poll_cond:
                    while not self._names_changed and not self.closed:
                        self._poll_cond.wait()
                    self._names_changed = False
                continue
            with self._poll_cond:
                self._names_changed = False
            after = [last if name not in self._polled else max(self._polled[name], -1 if last is None else last)
                     for name, last in zip(names, self._last_sequences(names))]
            started = time.monotonic()
            try:
                records = self._api.name_watch([name.hex() for name in names], after, self._long_wait)
            except Exception as e:  # noqa: BLE001 - a daemon restarting answers again later
                if self.closed:
                    return
                self._report(f"watching names failed: {e}")
                with self._poll_cond:
                    self._poll_cond.wait(RETRY_DELAY)
                continue
            for data in records:
                record = Record.from_dict(data)
                self._polled[record.name] = max(self._polled.get(record.name, -1), record.sequence)
                self._record_heard(record.name, record)
            if not records and self._long_wait == 0 and time.monotonic() - started < SHORT_POLL:
                with self._poll_cond:
                    self._poll_cond.wait(SHORT_POLL)

    # Event watches

    def _event_baseline(self) -> tuple[int, str]:
        """GET /api/events without after: the daemon's last sequence and run now (NoSuchEndpoint from a daemon
        without events). The event poller starts there unless it has a position already, which is at or before
        it: the watch skips what it began after. Even one of another run (the daemon restarted) is kept: the next
        poll then answers "lost", which moves every older watch on to the new run (the new one began in it, and
        skips the marker unless it missed events too).

        One baseline at a time, the request and the position set together (_baseline_lock, not _poll_cond, so
        close() never waits for a request): two first watches can't leave the position past events the one with
        the older baseline hasn't had."""
        with self._baseline_lock:
            batch = self._api.events()
            with self._poll_cond:
                if self._event_position is None:
                    self._event_position = (batch["next"], batch["epoch"])
                if self._event_poller is None:
                    self._event_poller = threading.Thread(target=self._poll_events, name="rnfs-client-event-watch",
                                                          daemon=True)
                    self._event_poller.start()
                self._poll_cond.notify_all()
        return batch["next"], batch["epoch"]

    def _poll_events(self):
        """Long-poll the daemon's events (every kind) for the event watches until closed; idle, and forgetting
        where it was, while there are none."""
        while True:
            with self._poll_cond:
                while not self.closed and (self._event_position is None or not self._event_watch_count()):
                    if not self._event_watch_count():
                        self._event_position = None  # the next watch's baseline sets it
                    self._poll_cond.wait()
                if self.closed:
                    return
                after, epoch = self._event_position
            started = time.monotonic()
            try:
                batch = self._api.events(after, None, self._long_wait, epoch)
            except Exception as e:  # noqa: BLE001 - a daemon restarting answers again later (then "lost")
                if self.closed:
                    return
                self._report(f"watching events failed: {e}")
                with self._poll_cond:
                    self._poll_cond.wait(RETRY_DELAY)
                continue
            with self._poll_cond:
                self._event_position = (batch["next"], batch["epoch"])
            if batch["lost"]:
                # The events held are older than what was missed, or of another run: the watches read again
                self._events_lost(batch["next"], batch["epoch"])
            else:
                self._events_heard(batch["epoch"], batch["events"])
            if not batch["events"] and not batch["lost"] and self._long_wait == 0 \
                    and time.monotonic() - started < SHORT_POLL:
                with self._poll_cond:
                    self._poll_cond.wait(SHORT_POLL)


class _Backend:
    """rnfs.interface's backend for the daemon's API (ApiConnection's methods, in the forms the API takes)."""

    def __init__(self, api: ApiConnection):
        """The backend of api."""
        self.api = api

    def timeout(self, seconds: float | None):
        """ApiConnection.using_timeout: this thread's requests bounded by seconds inside it."""
        return self.api.using_timeout(seconds)

    def status(self) -> dict:
        """GET /api/status."""
        return self.api.status()

    def node_id(self) -> str:
        """The node ID in hex, from the status."""
        return self.api.status()["node_id"]

    def wait_joined(self, timeout: float | None) -> bool:
        """A running daemon has joined (or keeps joining) on its own: True once it answers."""
        self.api.status()
        return True

    def add_path(self, path: str, **options) -> dict:
        """POST /api/add."""
        return self.api.add_path(path, **options)

    def add_stream(self, data, length: int | None, name: str | None, **options) -> dict:
        """POST /api/add/stream."""
        return self.api.add_stream(data, name=name, length=length, **options)

    def get(self, target: str, output: str, from_node: str | None, raw: bool) -> dict:
        """POST /api/get."""
        return self.api.fetch(target, output, from_node=from_node, raw=raw)

    def read(self, target: str, offset: int, length: int | None, raw: bool, from_node: str | None):
        """GET /api/cat: the body, read as it arrives, closing the connection when done or closed early. Sent once
        iterated (its errors raised then, as a Node's are), so an iterator closed before that sends nothing."""
        def chunks():
            """The body's chunks."""
            with self.api.cat(target, offset=offset or None, length=length, raw=raw, from_node=from_node) as stream:
                yield from stream
        return chunks()

    def block(self, cid: str) -> bytes:
        """GET /api/cat?block=1."""
        with self.api.cat(cid, block=True) as stream:
            return stream.read()

    def info(self, target: str, from_node: str | None, raw: bool) -> dict:
        """GET /api/info; raw: a sealed file as stored (its stored size, without the sealed fields), as the
        facade's info(raw=True) answers."""
        info = self.api.info(target, from_node=from_node)
        if raw and info.get("sealed"):
            return {"cid": info["cid"], "type": info["type"], "size": info["stored_size"],
                    "executable": info["executable"]}
        return info

    def ls(self, target: str) -> list[dict]:
        """GET /api/ls."""
        return self.api.ls(target)

    def stat(self, target: str) -> dict:
        """GET /api/stat."""
        return self.api.stat(target)

    def dir_set(self, target: str, name: str, child: str, executable: bool) -> dict:
        """POST /api/dir/set."""
        return self.api.dir_set(target, name, child, executable)

    def dir_rm(self, target: str) -> dict:
        """POST /api/dir/rm."""
        return self.api.dir_rm(target)

    def pin_add(self, target: str) -> dict:
        """POST /api/pin/add."""
        return self.api.pin_add(target)

    def pin_rm(self, cid: str):
        """POST /api/pin/rm."""
        self.api.pin_rm(cid)

    def pin_ls(self, cid: str | None) -> list[dict]:
        """GET /api/pin/ls."""
        return self.api.pin_ls(cid)

    def pin_remote(self, node_id: str, cid: str, pin: bool) -> bool:
        """POST /api/pin/remote."""
        return self.api.pin_remote(node_id, cid, pin)

    def providers(self, cid: str) -> list[str]:
        """GET /api/providers."""
        return self.api.providers(cid)

    def ping(self, node_id: str) -> float | None:
        """GET /api/ping."""
        return self.api.ping(node_id)

    def export(self, target: str, output: str) -> dict:
        """POST /api/export."""
        return self.api.export(target, output)

    def import_archive(self, path: str) -> dict:
        """POST /api/import."""
        return self.api.import_archive(path)

    def gc(self) -> dict:
        """POST /api/gc."""
        return self.api.gc()

    def verify(self) -> dict:
        """POST /api/verify."""
        return self.api.verify()

    def name_create(self, alias: str) -> dict:
        """POST /api/name/create."""
        return self.api.name_create(alias)

    def name_import(self, alias: str, key: bytes) -> dict:
        """POST /api/name/import (the key as the body)."""
        return self.api.name_import(alias, key)

    def name_rm(self, alias: str):
        """POST /api/name/rm."""
        self.api.name_rm(alias)

    def name_export(self, alias: str) -> bytes:
        """POST /api/name/export: the key bytes."""
        return self.api.name_export(alias)

    def name_list(self) -> list[dict]:
        """GET /api/name/list."""
        return self.api.name_list()

    def name_publish(self, alias: str, target: str) -> dict:
        """POST /api/name/publish."""
        return self.api.name_publish(alias, target)

    def name_resolve(self, name: str) -> dict:
        """GET /api/name/resolve."""
        return self.api.name_resolve(name)

    def submit_job(self, kind: str, target: str | None, output: str | None, from_node: str | None, **flags) -> dict:
        """POST /api/jobs (only the flags set, as the CLI sends them; an add's or import's path and options too)."""
        return self.api.submit_job(kind, target, output, from_node, **{k: v for k, v in flags.items() if v})

    def job(self, job_id: str, wait: float) -> dict:
        """GET /api/jobs/get, waiting up to wait seconds (whole ones, within the timeout this thread's request has)
        for the job to finish; when that timeout leaves no wait, a second's pause after an unfinished answer
        instead."""
        if wait <= 0:
            return self.api.job(job_id, 0)
        long_wait = long_poll_wait(self.api.effective_timeout())
        if long_wait == 0:
            job = self.api.job(job_id, 0)
            if job.get("state") not in interface.FINISHED:
                time.sleep(min(wait, SHORT_POLL))
            return job
        return self.api.job(job_id, min(long_wait, math.ceil(wait)))

    def jobs(self) -> list[dict]:
        """GET /api/jobs."""
        return self.api.jobs()

    def cancel_job(self, job_id: str) -> dict:
        """POST /api/jobs/cancel."""
        return self.api.cancel_job(job_id)

    def clear_jobs(self, job_id: str | None) -> int:
        """POST /api/jobs/clear."""
        return self.api.clear_jobs(job_id)

    def deny(self) -> dict:
        """GET /api/deny."""
        return self.api.deny()

    def deny_add(self, entries: list[str]) -> list[str]:
        """POST /api/deny/add."""
        return self.api.deny_add(entries)

    def deny_rm(self, entries: list[str]) -> list[str]:
        """POST /api/deny/rm."""
        return self.api.deny_rm(entries)

    def peers(self) -> list[dict]:
        """GET /api/peers."""
        return self.api.peers()

    def stats(self) -> dict:
        """GET /api/stats."""
        return self.api.stats()

    def contacts(self) -> list[dict]:
        """GET /api/contacts."""
        return self.api.contacts()

    def contact_get(self, nick: str) -> dict:
        """GET /api/contacts/get."""
        return self.api.contact_get(nick)

    def contact_add(self, nick: str, ident: str, kind: str | None) -> dict:
        """POST /api/contacts/add."""
        return self.api.contact_add(nick, ident, kind)

    def contact_rm(self, nick: str):
        """POST /api/contacts/rm."""
        self.api.contact_rm(nick)

    def name_keep(self, name: str) -> dict:
        """POST /api/name/keep."""
        return self.api.name_keep(name)

    def name_unkeep(self, name: str):
        """POST /api/name/unkeep."""
        self.api.name_unkeep(name)

    def name_kept(self) -> list[dict]:
        """GET /api/name/kept."""
        return self.api.name_kept()

    def name_follow(self, name: str) -> dict:
        """POST /api/name/follow."""
        return self.api.name_follow(name)

    def name_unfollow(self, name: str):
        """POST /api/name/unfollow."""
        self.api.name_unfollow(name)

    def name_following(self) -> list[dict]:
        """GET /api/name/following."""
        return self.api.name_following()

    def lxmf_address(self) -> dict:
        """GET /api/lxmf/address."""
        return self.api.lxmf_address()

    def lxmf_send(self, to: str, target: str, message: str | None, propagate: bool, nearest: bool) -> dict:
        """POST /api/lxmf/send."""
        return self.api.lxmf_send(to, target, message, propagate, nearest)

    def lxmf_inbox(self) -> list[dict]:
        """GET /api/lxmf/inbox."""
        return self.api.lxmf_inbox()

    def lxmf_inbox_get(self, entry_id, link, output: str | None, directory: str | None,
                       use_sent_cid: bool) -> list[dict]:
        """POST /api/lxmf/inbox/get."""
        return self.api.lxmf_inbox_get(entry_id, link, output, directory, use_sent_cid)

    def lxmf_inbox_rm(self, entry_id, every: bool) -> int:
        """POST /api/lxmf/inbox/rm."""
        return self.api.lxmf_inbox_rm(entry_id, every)

    def lxmf_outbox(self) -> list[dict]:
        """GET /api/lxmf/outbox."""
        return self.api.lxmf_outbox()

    def lxmf_sync(self, nearest: bool) -> dict:
        """POST /api/lxmf/sync."""
        return self.api.lxmf_sync(nearest)

    def pin_label(self, cid: str, label: str | None) -> dict:
        """POST /api/pin/label."""
        return self.api.pin_label(cid, label)

    def cache_ls(self) -> list[dict]:
        """GET /api/cache/ls."""
        return self.api.cache_ls()

    def cache_rm(self, cid: str) -> dict:
        """POST /api/cache/rm."""
        return self.api.cache_rm(cid)

    def capabilities(self) -> dict:
        """GET /api/capabilities ({} from a daemon before 0.2.0)."""
        return self.api.capabilities()

    def shutdown(self, wait: float | None) -> bool:
        """POST /api/shutdown, then (wait) polling the open status until it stops answering."""
        return self.api.shutdown(wait)

    def daemon_info(self) -> dict | None:
        """The daemon's daemon.json (read_daemon_info)."""
        return self.api.daemon_info()

    def config(self) -> dict:
        """GET /api/config."""
        return self.api.config()

    def config_set(self, settings: dict) -> dict:
        """POST /api/config/set."""
        return self.api.config_set(settings)

    def add_tar(self, data, name: str | None, pin: bool, only_hash: bool, chunker: str | None) -> dict:
        """POST /api/add/stream?format=tar."""
        return self.api.add_tar(data, name=name, only_hash=only_hash, pin=pin, chunker=chunker)

    def get_tar(self, target: str):
        """GET /api/get/stream?format=tar: the body, read as it arrives; sent once iterated, as read()'s."""
        def chunks():
            """The body's chunks."""
            with self.api.get_tar(target) as stream:
                yield from stream
        return chunks()

    def export_stream(self, target: str):
        """GET /api/export/stream: the body, read as it arrives; sent once iterated, as read()'s."""
        def chunks():
            """The body's chunks."""
            with self.api.export_stream(target) as stream:
                yield from stream
        return chunks()

    def import_stream(self, data) -> dict:
        """POST /api/import/stream."""
        return self.api.import_stream(data)

    def events_since(self, after: int | None, kinds: list[str] | None, wait: int, epoch: str | None) -> dict:
        """GET /api/events, waiting at most what this thread's timeout leaves (long_poll_wait); when it leaves no
        wait, a pause of up to a second after an empty answer instead, as job()'s, so a caller looping on it
        doesn't spin."""
        long_wait = long_poll_wait(self.api.effective_timeout())
        batch = self.api.events(after, kinds, min(wait, long_wait), epoch)
        if after is not None and wait > 0 and long_wait == 0 and not batch["events"] and not batch["lost"]:
            time.sleep(min(wait, SHORT_POLL))
        return batch
