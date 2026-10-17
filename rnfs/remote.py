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
"""
import logging
import math
import threading
import time

from rnfs import interface
from rnfs.apiclient import ApiConnection, read_all
from rnfs.fsutil import printable
from rnfs.objects import Record

MAX_WATCH_WAIT = 30  # seconds one GET /api/name/watch or /api/jobs/get waits (the API's cap)
LONG_POLL_MARGIN = 2  # seconds a long poll leaves of the client's timeout
RETRY_DELAY = 5.0  # seconds before polling names again after a failure
SHORT_POLL = 1.0  # seconds between polls when the timeout leaves no wait

_logger = logging.getLogger("rnfs")


class Client(interface.Rnfs):
    """The daemon using rnfs_dir (default ~/.rnfs/), on 127.0.0.1:port (default: the api_port in its rnfs.toml),
    with the token in token_file (default <rnfs_dir>/api_token). timeout bounds each request (default: none, as
    fetches can take hours). Nothing is sent until the first call. Raises SettingsError if rnfs.toml can't be read
    for the port."""

    events_thread = "rnfs-client-events"
    jobs_thread = "rnfs-client-job-watch"
    job_poll = 1.0  # what the CLI's progress line polls at

    def __init__(self, rnfs_dir: str | None = None, *, port: int | None = None, token_file: str | None = None,
                 timeout: float | None = None):
        """A client of the daemon (see the class)."""
        self._api = ApiConnection(rnfs_dir, port=port, token_file=token_file, timeout=timeout)
        # The longest a long poll may wait within the timeout
        self._long_wait = MAX_WATCH_WAIT if timeout is None else \
            max(0, min(MAX_WATCH_WAIT, math.floor(timeout) - LONG_POLL_MARGIN))
        super().__init__(_Backend(self._api, self._long_wait))
        self._poll_cond = threading.Condition()
        self._poller: threading.Thread | None = None
        self._names_changed = False
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
        """Wake the name poller so it ends."""
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


class _Backend:
    """rnfs.interface's backend for the daemon's API (ApiConnection's methods, in the forms the API takes)."""

    def __init__(self, api: ApiConnection, long_wait: int):
        """The backend of api, whose long polls wait at most long_wait seconds."""
        self.api = api
        self.long_wait = long_wait

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
        return self.api.post("/api/dir/set", target=target, name=name, cid=child, executable=executable)

    def dir_rm(self, target: str) -> dict:
        """POST /api/dir/rm."""
        return self.api.post("/api/dir/rm", target=target)

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
        return self.api.post("/api/pin/remote", node=node_id, cid=cid, pin=pin)["accepted"]

    def providers(self, cid: str) -> list[str]:
        """GET /api/providers."""
        return self.api.get("/api/providers", cid=cid)["providers"]

    def ping(self, node_id: str) -> float | None:
        """GET /api/ping."""
        return self.api.get("/api/ping", node=node_id)["rtt"]

    def export(self, target: str, output: str) -> dict:
        """POST /api/export."""
        return self.api.post("/api/export", target=target, output=output)

    def import_archive(self, path: str) -> dict:
        """POST /api/import."""
        return self.api.post("/api/import", path=path)

    def gc(self) -> dict:
        """POST /api/gc."""
        return self.api.post("/api/gc")

    def verify(self) -> dict:
        """POST /api/verify."""
        return self.api.post("/api/verify")

    def name_create(self, alias: str) -> dict:
        """POST /api/name/create."""
        return self.api.name_create(alias)

    def name_import(self, alias: str, key: bytes) -> dict:
        """POST /api/name/import (the key as the body)."""
        body = bytes(key)
        return self.api.call("POST", "/api/name/import", params={"alias": alias}, body=body, length=len(body))

    def name_rm(self, alias: str):
        """POST /api/name/rm."""
        self.api.post("/api/name/rm", alias=alias)

    def name_export(self, alias: str) -> bytes:
        """POST /api/name/export: the key bytes."""
        return read_all(self.api.request("POST", "/api/name/export", json_body={"alias": alias}))

    def name_list(self) -> list[dict]:
        """GET /api/name/list."""
        return self.api.name_list()

    def name_publish(self, alias: str, target: str) -> dict:
        """POST /api/name/publish."""
        return self.api.name_publish(alias, target)

    def name_resolve(self, name: str) -> dict:
        """GET /api/name/resolve."""
        return self.api.name_resolve(name)

    def submit_job(self, kind: str, target: str, output: str | None, from_node: str | None, **flags) -> dict:
        """POST /api/jobs (only the flags set, as the CLI sends them)."""
        return self.api.submit_job(kind, target, output, from_node, **{k: v for k, v in flags.items() if v})

    def job(self, job_id: str, wait: float) -> dict:
        """GET /api/jobs/get, waiting up to wait seconds (whole ones, within the timeout) for the job to finish;
        when the timeout leaves no wait, a second's pause after an unfinished answer instead."""
        if wait <= 0:
            return self.api.job(job_id, 0)
        if self.long_wait == 0:
            job = self.api.job(job_id, 0)
            if job.get("state") not in interface.FINISHED:
                time.sleep(min(wait, SHORT_POLL))
            return job
        return self.api.job(job_id, min(self.long_wait, math.ceil(wait)))

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
