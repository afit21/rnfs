"""Background jobs for long transfers (feature "jobs"): get, pin, export, add and import run by the daemon, with
progress.

On LoRa a 1 MB file can take an hour, so a transfer runs as a job the daemon owns instead of inside one API
request. `rnfs get`, `pin add` and `export` start a job and follow it with a progress line on stderr (or
return at once with --background); `rnfs add <path> --background` and `rnfs import --background` start an add
or import job; `rnfs jobs` lists, shows, waits for, cancels and clears jobs.

- A job runs one node facade call (get, pin_add, export, add_path, import_archive) on a thread of its own, inside
  fetcher.observing(): the fetch reports the verified size of what it fetches and each block it meets, and
  raises Cancelled at its next step (within fetcher.POLL) once the job is cancelled or the node stops.
  At most max_jobs run at once (2; 1 on slow nodes); queued jobs start oldest first.
- Add and import jobs read a file, directory or archive on this machine (path) instead of fetching a target:
  their total_bytes is measured before reading (builder.tree_size, or the archive's size), each read is counted
  as done (Job.read, the facade's progress callback), which raises Cancelled at once when the job is cancelled
  or the node stops, and their rate is of bytes read. They hold no protection session: an interrupted one runs
  again from the start, which is safe because adding is idempotent (the same content gives the same CIDs, and
  blocks stored already are kept), and what a cancelled one stored is unreferenced until the next collection.
  An add job's options are POST /api/add's (ADD_OPTIONS); a sealed add run again seals afresh, so only the run
  that finishes gives the job its CID.
- An add job may add a NomadNet link (nomadnet, `rnfs add <destination>:/page/<path>`) instead of a path: the node's
  add_nomadnet, with name and identify among its options too (NOMADNET_OPTIONS). Its total_bytes is the answer's
  advertised size once a Resource brings it, done_bytes what has arrived, its rate bytes received; cancelling it, or
  the node stopping, closes its link at the fetch's next poll. A resumed one fetches a new snapshot.
- Every change of a job's state (queued, running, done, failed, cancelled, and forgotten: "removed") and, at
  most once a second (EVENT_INTERVAL), a running job's progress is published as a "job" event (rnfs.events) with
  the job's state and progress fields (Job.event; the job's kind as "job_kind", an event's "kind" being "job"),
  not its result: a client fetches the job for that.
- Jobs are saved in the state database's jobs table (rnfs.statedb), durably, at every state change and with
  the node's periodic saves (keeping a running job's progress recent); each save writes only the jobs changed
  since the last. At startup, jobs that were queued or running are queued again and resume where they
  stopped, since blocks already stored are never fetched again.
- Resuming needs those blocks to stay stored: a fetch's own protection session ends with the fetch, after
  which a get's or export's blocks are evictable cache and a pin's are unreferenced. So while a job is queued
  or running, this feature holds a protection session on its root, which keeps everything stored below it
  through eviction and gc, also across a restart (the sessions are opened when the jobs are loaded, before
  any eviction runs). Failed and cancelled jobs release theirs; starting the transfer again refetches
  whatever was evicted meanwhile.
- A get job writes to <output's directory>/.rnfs-job-<id>.partial and renames that to its output once
  complete, so an interrupted write-out never leaves a partial output behind, and a resumed run removes what
  an earlier run left there. With sealed_name, a sealed file this node opens is renamed to its sealed name
  (if fit, fsutil.safe_default_name) in the output's directory instead: for a sealed folder unpacked, the
  name of the directory written. With no_extract, a sealed folder is written as its tar rather than unpacked
  (node.get's no_extract). With fallback_dht, a get whose from node fails is retried with ordinary DHT
  discovery.
- A get, pin or export job submitted with retry true (kept in its record) whose content no provider could supply
  (NotFound, but not a path its stored directories show doesn't exist) is queued again rather than failed, keeping
  its protection, to start no sooner than its retry_at (rnfs.retry: 10 minutes, doubling up to 6 hours, for up to 3
  days after it first wasn't found; then it fails). Its error says why it waits; retry_since is when it was first
  not found. The rnfs-job-retry thread starts the jobs due (RETRY_POLL). Retrying is opt-in: without retry a job
  fails at once, as in 0.1.6, so an app written for 0.1.6 polling its job sees it fail rather than stay queued for
  days. The CLI's --background transfers (run_transfer) opt in, as they run unattended; its foreground ones don't,
  so a command or script never waits hours.
- Finished jobs (done, failed, cancelled) are kept for `rnfs jobs`, at most max_finished_jobs, but each
  for at least FINISHED_GRACE: a CLI following a fast job must still find it once it ends.
- Job threads are daemon threads, so they never hold up the process; stop() asks running fetches to stop
  and waits for them until the deadline the node passes (Feature.stop: its stop budget). A job interrupted by
  the shutdown is saved as queued.

No RNS at module level (the CLI imports this module): node-side modules are imported inside functions.
"""
import json
import os
import re
import secrets
import shutil
import sys
import threading
import time
from collections import deque
from contextlib import ExitStack
from typing import Callable

from rnfs import ids
from rnfs.constants import PARTIAL_SUFFIX
from rnfs.retry import next_retry
from rnfs.features import Feature, _common
from rnfs.features._common import LOG_ERROR, LOG_INFO, LOG_NOTICE, LOG_WARNING
from rnfs.fsutil import printable

NAME = "jobs"
KINDS = ("get", "pin", "export", "add", "import")
LOCAL_KINDS = frozenset({"add", "import"})  # read a path on this machine rather than fetch a target
PINNING_KINDS = frozenset({"pin", "add", "import"})  # pin with their source (user, or the app's that queued them)
USER = "user"  # storage.USER, the source of the CLI's (and an embedded node's) pins: not imported, no RNS here
# An add job's options: POST /api/add's, each with its JSON type (absent: add_path's default)
ADD_OPTIONS = {"wrap": bool, "only_hash": bool, "pin": bool, "to": list, "to_self": bool, "seal_name": bool,
               "chunker": str}
NOMADNET_OPTIONS = {"name": str, "identify": bool}  # what an add job of a NomadNet link takes besides ADD_OPTIONS
MAX_RECIPIENT_TEXT = 256  # characters of one of an add job's to= recipients (a key is 128 hex digits)
QUEUED, RUNNING, DONE, FAILED, CANCELLED = "queued", "running", "done", "failed", "cancelled"
STATES = (QUEUED, RUNNING, DONE, FAILED, CANCELLED)
ACTIVE = frozenset({QUEUED, RUNNING})
FINISHED = frozenset({DONE, FAILED, CANCELLED})
MAX_ACTIVE = 1000  # queued and running jobs at most, so a script can't queue without bound
RATE_WINDOW = 300.0  # seconds the rate is averaged over: LoRa responses can arrive minutes apart
SAMPLE_INTERVAL = 1.0  # seconds between rate samples
MAX_WAIT = 30  # longest wait= a GET /api/jobs/get may ask for, in seconds
POLL_TTY = 1  # seconds between progress updates the CLI shows
POLL_QUIET = 10  # seconds a CLI without a progress line waits per request (it returns early when the job ends)
FINISHED_GRACE = 60.0  # seconds a finished job is kept whatever max_finished_jobs says
EVENT_INTERVAL = 1.0  # seconds between a running job's progress events, at least
RETRY_POLL = 30.0  # seconds between the checks for queued jobs whose retry_at has come
RETRYING_KINDS = frozenset({"get", "pin", "export"})  # queued again when their content isn't found (rnfs.retry)
# A "job" event's fields (rnfs.events): what changes, not the result (clients fetch the job for that); "kind" is
# sent as "job_kind", since an event's own "kind" is "job"
EVENT_FIELDS = ("id", "kind", "state", "error", "total_bytes", "done_bytes", "blocks", "blocks_total",
                "downloaded_bytes", "rate", "eta", "retry_at")
_JOB_ID = re.compile(r"[0-9a-f]{8}")
_NODE_ID = ids.HEX16_LOWER  # a saved job's from_node, as bytes.hex() writes it (a row that fails is deleted)


def _log(message: str, level: int = LOG_INFO):
    """Log with this feature's prefix."""
    _common.log(f"jobs: {message}", level)


class Job:
    """One transfer: what it is, its state and its progress. Also the fetch Observer of its runs."""

    def __init__(self, job_id: str, kind: str, target: str | None, output: str | None = None,
                 from_node: str | None = None, created: float | None = None, sealed_name: bool = False,
                 fallback_dht: bool = False, path: str | None = None, options: dict | None = None,
                 no_extract: bool = False, source: str = USER, nomadnet: str | None = None, retry: bool = False):
        """A queued job; from_node is a node ID in hex. retry: a get, pin or export whose content isn't found is
        queued again to be tried later (False: it fails at once, as in 0.1.6). sealed_name, fallback_dht and
        no_extract are for get jobs; path (and for add, options: ADD_OPTIONS) for add and import jobs, which have no
        target; source the pin source of a pin, add or import job (user, or the app's whose token queued it); an add
        job may have a NomadNet link (nomadnet) instead of a path."""
        self.id = job_id
        self.kind = kind
        self.target = target
        self.output = output
        self.from_node = from_node
        self.sealed_name = sealed_name  # write a sealed file under its sealed name, beside output
        self.fallback_dht = fallback_dht  # if from_node fails, try again with ordinary DHT discovery
        self.no_extract = no_extract  # write a sealed folder as its tar rather than unpacking it
        self.path = path  # what an add or import job reads, on this machine
        self.options = options  # an add job's add_path options (only those set)
        self.source = source  # what a pin, add or import job pins with: kept so a resumed job pins the same
        self.nomadnet = nomadnet  # an add job's NomadNet link, which it adds instead of a path
        self.retry = retry  # queued again when its content isn't found (rnfs.retry)
        self.state = QUEUED
        self.created = time.time() if created is None else created
        self.started: float | None = None  # when the latest run started
        self.finished: float | None = None
        self.error: str | None = None
        self.result: dict | None = None
        # Queued again because its content wasn't found (rnfs.retry): when it may start, and when it first wasn't
        self.retry_at: float | None = None
        self.retry_since: float | None = None
        self.runs = 0  # runs started, counting resumes after a restart
        self.total_bytes: int | None = None  # verified content size, once the target's block is checked
        self.done_bytes = 0  # content bytes stored of it, whether downloaded or already stored
        self.blocks = 0  # distinct blocks met this run
        self.blocks_total: int | None = None  # blocks of the target's DAG, known once it is complete
        self.downloaded_bytes = 0  # bytes downloaded this run (DAG nodes too), for the rate
        self.cancel_requested = False
        self.ended = threading.Event()  # set once finished, for waiters
        self.stop_event: threading.Event | None = None  # the feature's, set when the node stops
        self.protection: ExitStack | None = None  # the session keeping its blocks while queued or running
        self.position: int | None = None  # its row's place in the jobs table (creation order), once queued or loaded
        # Called with the job at most every EVENT_INTERVAL while it progresses (the feature's progress event)
        self.on_progress: Callable[["Job"], None] | None = None
        self._lock = threading.Lock()  # guards _samples, _moved, _seen and _partial
        self._moved = 0  # what the rate counts: bytes downloaded (transfers) or read (add, import) this run
        self._samples: deque[tuple[float, int]] = deque()  # (monotonic time, _moved)
        self._seen: set = set()  # CIDs met this run, so each counts once
        self._partial: dict = {}  # bytes counted of each block not yet met, from the ranges of it (/range)
        self._last_event = float("-inf")  # monotonic time of the last progress event

    @property
    def subject(self) -> str:
        """What the job works on, for messages: its target, or an add's or import's path or NomadNet link."""
        if self.target is not None:
            return self.target
        return self.nomadnet if self.nomadnet is not None else str(self.path)

    # Observer (rnfs.fetcher.Observer), called on the job's thread

    def total(self, size: int):
        """The fetch knows the verified size of what it fetches (or an add or import measured what it reads)."""
        self.total_bytes = size

    def block(self, cid, length: int, downloaded: bool):
        """A block met: count it once, its bytes as content if it is a chunk."""
        now = time.monotonic()
        with self._lock:
            counted = self._partial.pop(cid, 0)  # its ranges' bytes, counted already
            if downloaded:
                self.downloaded_bytes += max(0, length - counted)
                self._sample(now, max(0, length - counted))
            if cid not in self._seen:
                self._seen.add(cid)
                self.blocks += 1
                if cid.is_raw:
                    self.done_bytes += max(0, length - counted)
        self._progressed(now)

    def partial(self, cid, held: int):
        """Part of a block not yet met arrived with /range, held bytes of it from one provider: what goes beyond the
        bytes counted of it is counted as downloaded (and done, for a chunk), so a fetch over a slow link, where one
        block takes minutes, shows its progress and rate before the block is whole."""
        now = time.monotonic()
        with self._lock:
            gained = held - self._partial.get(cid, 0)
            if cid in self._seen or gained <= 0:
                return
            self._partial[cid] = held
            self.downloaded_bytes += gained
            self._sample(now, gained)
            if cid.is_raw:
                self.done_bytes += gained
        self._progressed(now)

    def read(self, length: int):
        """An add or import read length bytes of its path (the facade's progress callback, on the job's thread):
        counted as done. Raises Cancelled once the job is cancelled or the node stops, which stops the read at
        once."""
        from rnfs.errors import Cancelled
        if self.cancelled():
            raise Cancelled(f"job {self.id} was cancelled")
        now = time.monotonic()
        with self._lock:
            self.done_bytes += length
            self._sample(now, length)
        self._progressed(now)

    def check_cancelled(self):
        """Raise Cancelled once the job is cancelled or the node stops (an add's measuring, between entries)."""
        self.read(0)

    def _sample(self, now: float, length: int):
        """Count length bytes moved for the rate, sampling at most every SAMPLE_INTERVAL. Called with _lock held."""
        self._moved += length
        if not self._samples or now - self._samples[-1][0] >= SAMPLE_INTERVAL:
            self._samples.append((now, self._moved))
            # Prune here too, not only in rate(): a job nobody polls would otherwise keep a sample a second for
            # as long as it runs (about 10 MB a day)
            self._prune(now)

    def _progressed(self, now: float):
        """Tell on_progress, if EVENT_INTERVAL has passed since it was last told (on the job's thread, no lock)."""
        if self.on_progress is None or now - self._last_event < EVENT_INTERVAL:
            return
        self._last_event = now
        self.on_progress(self)

    def cancelled(self) -> bool:
        """True once the job is cancelled or the node stops."""
        return self.cancel_requested or (self.stop_event is not None and self.stop_event.is_set())

    # Runs

    def begin_run(self):
        """Reset this run's progress (stored blocks are met again quickly, so a resume catches up at once)."""
        with self._lock:
            self.state = RUNNING
            self.started = time.time()
            self.retry_at = None
            self.runs += 1
            self.done_bytes = self.blocks = self.downloaded_bytes = self._moved = 0
            self._seen = set()
            self._partial = {}
            self._samples = deque([(time.monotonic(), 0)])
            self._last_event = time.monotonic()  # the "running" event has just been published

    def _prune(self, now: float):
        """Drop samples older than RATE_WINDOW, keeping one at or before the window's start as the base to
        measure from. Called with _lock held."""
        while len(self._samples) > 1 and self._samples[1][0] <= now - RATE_WINDOW:
            self._samples.popleft()

    def rate(self, now: float | None = None) -> float | None:
        """Bytes/s downloaded (read, for add and import) over the last RATE_WINDOW of a running job (it decays
        while nothing arrives)."""
        now = time.monotonic() if now is None else now
        with self._lock:
            if self.state != RUNNING or not self._samples:
                return None
            self._prune(now)
            since, base = self._samples[0]
            moved = self._moved
        elapsed = now - since
        if elapsed < SAMPLE_INTERVAL:
            return None
        return max(0.0, (moved - base) / elapsed)

    def to_dict(self) -> dict:
        """The job as the API shows it, with its current rate and ETA."""
        rate = self.rate()
        eta = None
        if rate and self.total_bytes is not None:
            eta = max(0.0, (self.total_bytes - self.done_bytes) / rate)
        return {**self.to_record(), "state": self.state, "rate": rate, "eta": eta, "retry_at": self.retry_at}

    def event(self) -> dict:
        """The job's "job" event fields (EVENT_FIELDS): its state and progress, not its result. Its kind is
        "job_kind": an event's "kind" is the event's own ("job")."""
        job = self.to_dict()
        return {("job_kind" if name == "kind" else name): job[name] for name in EVENT_FIELDS}

    def to_record(self) -> dict:
        """The job as saved (a running job is saved as such, and queued again when loaded). path and options
        only for the kinds that have them (add, import), no_extract only when set, source only when it isn't "user"
        (an app's), nomadnet only for an add of a NomadNet link, retry only when true, and retry_at and
        retry_since only while it waits to be tried again, so the record of every other job is as 0.1.6 saved
        it."""
        record = {"id": self.id, "kind": self.kind, "target": self.target, "output": self.output,
                  "from": self.from_node, "sealed_name": self.sealed_name, "fallback_dht": self.fallback_dht}
        if self.no_extract:
            record["no_extract"] = True
        if self.path is not None or self.options is not None or self.nomadnet is not None:
            record.update(path=self.path, options=self.options)
        if self.source != USER:
            record["source"] = self.source
        if self.nomadnet is not None:
            record["nomadnet"] = self.nomadnet
        if self.retry:
            record["retry"] = True
        if self.retry_since is not None:
            record.update(retry_at=self.retry_at, retry_since=self.retry_since)
        record.update({"state": self.state, "created": self.created, "started": self.started,
                       "finished": self.finished, "error": self.error, "result": self.result, "runs": self.runs,
                       "total_bytes": self.total_bytes, "done_bytes": self.done_bytes, "blocks": self.blocks,
                       "blocks_total": self.blocks_total, "downloaded_bytes": self.downloaded_bytes})
        return record

    @classmethod
    def from_record(cls, record) -> "Job":
        """A job from a saved record. Raises ValueError if anything in it is malformed."""
        if not isinstance(record, dict):
            raise ValueError("not an object")
        job_id, kind, target = record.get("id"), record.get("kind"), record.get("target")
        output, from_node, state = record.get("output"), record.get("from"), record.get("state")
        if not isinstance(job_id, str) or not _JOB_ID.fullmatch(job_id):
            raise ValueError("bad id")
        if kind not in KINDS or state not in STATES:
            raise ValueError("bad kind or state")
        sealed_name, fallback_dht = record.get("sealed_name", False), record.get("fallback_dht", False)
        path, options, no_extract = record.get("path"), record.get("options"), record.get("no_extract", False)
        source, nomadnet, retry = record.get("source", USER), record.get("nomadnet"), record.get("retry", False)
        check_job_params(kind, target, output, from_node, sealed_name, fallback_dht, path, options, no_extract,
                         source, nomadnet=nomadnet, retry=retry)
        job = cls(job_id, kind, target, output, from_node, _number(record.get("created"), time.time()),
                  sealed_name, fallback_dht, path, options, no_extract, source, nomadnet=nomadnet, retry=retry)
        job.state = state
        job.started = _number(record.get("started"), None)
        job.finished = _number(record.get("finished"), None)
        error, result = record.get("error"), record.get("result")
        job.error = error if isinstance(error, str) else None
        job.result = _capped(result) if isinstance(result, dict) else None
        for name in ("runs", "done_bytes", "blocks", "downloaded_bytes"):
            setattr(job, name, _count(record.get(name), 0))
        job.total_bytes = _count(record.get("total_bytes"), None)
        job.blocks_total = _count(record.get("blocks_total"), None)
        job.retry_since = _number(record.get("retry_since"), None)
        if job.retry_since is not None:
            job.retry_at = _number(record.get("retry_at"), None)
        return job


def _number(value, default):
    """value if it is a finite non-negative number, else default."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            number = float(value)
        except OverflowError:  # an int too large for a float
            return default
        if 0 <= number < float("inf"):
            return number
    return default


def _count(value, default):
    """value if it is a non-negative int, else default."""
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else default


def _capped(result: dict) -> dict:
    """result with at most log.MAX_WARNINGS warnings (and a count of the rest), so saved jobs stay small."""
    from rnfs import log
    warnings = result.get("warnings")
    # node.get caps its own (adding warnings_total); this covers results saved before it did
    if isinstance(warnings, list) and len(warnings) > log.MAX_WARNINGS and "warnings_total" not in result:
        return {**result, **log.cap_warnings(warnings)}
    return result


def check_job_params(kind: str, target, output, from_node, sealed_name=False, fallback_dht=False, path=None,
                     options=None, no_extract=False, source=USER, nomadnet=None, retry=False):
    """Raise ValueError unless these fit a job of kind: for get, pin and export a well-formed target, an absolute
    output path for get and export (none for pin), and from (a node ID in hex), sealed_name, fallback_dht and
    no_extract (booleans) only for get; for add and import an absolute path and neither target nor output, and options
    (check_add_options) only for add; source "user", or for pin, add and import an app's (app:<name>[:<key>]); an add
    may have a NomadNet link (nomadnet, as written) instead of a path; retry is a boolean (any kind: it only
    changes a get's, pin's or export's)."""
    from rnfs import nomadnet as nomadnet_links
    from rnfs.content import walker  # validates the root CID and every path name
    from rnfs.content.storage import app_of
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {', '.join(KINDS)}")
    if type(retry) is not bool:
        raise ValueError("retry must be true or false")
    if source != USER and (kind not in PINNING_KINDS or app_of(source) is None):
        raise ValueError(f"a {kind} job can't pin with source {source!r}")
    if nomadnet is not None:
        if kind != "add" or path is not None:
            raise ValueError("only an add job takes a NomadNet link, instead of a path")
        if not isinstance(nomadnet, str) or str(nomadnet_links.parse(nomadnet)) != nomadnet:
            raise ValueError(f"malformed NomadNet link {nomadnet!r}")
    if kind in LOCAL_KINDS:
        if target is not None or output is not None:
            raise ValueError(f"an {kind} job reads a path: it takes no target or output")
        if nomadnet is None and (not isinstance(path, str) or not os.path.isabs(path) or "\x00" in path):
            raise ValueError(f"path must be an absolute path: {path!r}")
        if options is not None:
            if kind != "add":
                raise ValueError("only add jobs take options")
            check_add_options(options, nomadnet is not None)
    elif path is not None or options is not None:
        raise ValueError(f"a {kind} job takes a target, not a path")
    else:
        walker.parse_target(target)
    if kind == "pin" or kind in LOCAL_KINDS:
        if output is not None:
            raise ValueError(f"a {kind} job has no output")
    elif not isinstance(output, str) or not os.path.isabs(output) or "\x00" in output:
        raise ValueError(f"output must be an absolute path: {output!r}")
    if from_node is not None:
        if kind != "get":
            raise ValueError("only get jobs take from")
        if not isinstance(from_node, str) or not _NODE_ID.fullmatch(from_node):
            raise ValueError(f"invalid node ID {from_node!r}: must be 32 hex characters")
    for name, value in (("sealed_name", sealed_name), ("fallback_dht", fallback_dht), ("no_extract", no_extract)):
        if type(value) is not bool:
            raise ValueError(f"{name} must be true or false")
        if value and kind != "get":
            raise ValueError(f"only get jobs take {name}")


def check_add_options(options, nomadnet: bool = False):
    """Raise ValueError unless options are an add job's: a JSON object of ADD_OPTIONS (and NOMADNET_OPTIONS for an add
    of a NomadNet link), each of its type, to a list of at most MAX_RECIPIENTS recipient strings, chunker a known
    chunker's name, name a valid entry name. What the node checks only when the add runs (recipients it knows,
    only_hash with to) fails the job then, with the add's own message."""
    from rnfs import nomadnet as nomadnet_links
    from rnfs.content import chunker, seal
    if not isinstance(options, dict):
        raise ValueError("an add job's options must be an object")
    for name, value in options.items():
        expected = ADD_OPTIONS.get(name) or (NOMADNET_OPTIONS.get(name) if nomadnet else None)
        if expected is None:
            raise ValueError(f"unknown add option {name!r}")
        if type(value) is not expected:
            raise ValueError(f"add option {name} must be a {expected.__name__}")
    recipients = options.get("to", [])
    if len(recipients) > seal.MAX_RECIPIENTS or not all(
            isinstance(text, str) and 0 < len(text) <= MAX_RECIPIENT_TEXT for text in recipients):
        raise ValueError(f"to must list at most {seal.MAX_RECIPIENTS} recipients")
    if "chunker" in options:
        chunker.check(options["chunker"])
    if "name" in options and not nomadnet_links.is_valid_name(options["name"]):
        raise ValueError(f"invalid entry name {options['name']!r}")


def add_job_options(value, nomadnet: bool = False) -> dict | None:
    """An add job's options from a JSON body's value(name) (None when absent), as POST /api/add takes them: wrap
    and only_hash (flags), and node_content.add_options' (pin, to, to_self, seal_name, chunker), and for an add of a
    NomadNet link name and identify; only those set, or None for none. Raises ValueError as POST /api/add does."""
    from rnfs import api, node_content
    flags = ("wrap", "only_hash", "identify") if nomadnet else ("wrap", "only_hash")
    options = {name: True for name in flags if api.parse_bool(value(name))}
    options.update(node_content.add_options(value))
    if nomadnet and api.parse_entry_name(value("name")) is not None:
        options["name"] = value("name")
    return options or None


def import_jobs(value: dict, tx) -> int:
    """Import jobs.json (statedb.migrate): its jobs, in order, replace the jobs table, each checked as rnfs 0.1.4
    loaded it (a malformed one is logged and skipped; of a repeated id, the first is kept) and saved as
    Job.to_record() gives it. Returns the number imported."""
    from rnfs import statedb
    tx.execute("DELETE FROM jobs")
    ids: set[str] = set()
    for record in value.get("jobs", []):
        try:
            job = Job.from_record(record)
            if job.id in ids:
                continue
            tx.execute("INSERT INTO jobs (id, position, record) VALUES (?, ?, ?)",
                       (job.id, len(ids), statedb.dump(job.to_record())))
        except (ValueError, TypeError, OverflowError) as e:
            _log(f"ignoring a saved job ({e})", LOG_WARNING)
            continue
        ids.add(job.id)
    return len(ids)


def _source(job: Job) -> dict:
    """The facade's source argument for a job's pins: none for "user" (the facade's default), else the app's."""
    return {} if job.source == USER else {"source": job.source}


def check_job_id(text) -> str:
    """text if it is a job ID (8 hex characters), else ValueError."""
    if not isinstance(text, str) or not _JOB_ID.fullmatch(text):
        raise ValueError(f"invalid job ID {text!r}: must be 8 hex characters")
    return text


class JobsFeature(Feature):
    """The job manager: queue, threads, persistence, protection of resumable blocks. Thread-safe."""

    name = NAME
    optional = ("contacts",)  # a target may start with a name contact's nick

    def __init__(self, node):
        """Load saved jobs, queueing again those that hadn't finished and protecting their blocks."""
        from rnfs import statedb
        super().__init__(node)
        self._lock = threading.Lock()
        self._jobs: dict[str, Job] = {}  # in creation order
        self._threads: dict[str, threading.Thread] = {}
        self._started = False
        self._stop_event = threading.Event()
        self._position = 0  # the next new job's position (after every one saved)
        self._db = statedb.open_store(node.config.state_dir, self._load)

    @property
    def max_jobs(self) -> int:
        """Jobs running at once: max_jobs, by default 2, or 1 on slow nodes."""
        configured = self.node.config.max_jobs
        return configured if configured is not None else (1 if self.node.slow else 2)

    # Persistence

    def _load(self, db):
        """Read the jobs table, oldest first; a malformed row is logged and deleted."""
        from rnfs import statedb
        # Saved durably at each change and periodically (a running job's progress), outside the lock; only the
        # jobs whose record changed are written
        self._store = statedb.RowStore(db, "jobs", ("id",), durable=True, mode=statedb.SNAPSHOT, lock=self._lock,
                                       row=self._row, skip_unchanged=True)
        bad = []
        for job_id, position, text in db.query("SELECT id, position, record FROM jobs ORDER BY position"):
            try:
                if not isinstance(position, int):
                    raise TypeError("bad position")
                self._position = max(self._position, position + 1)
                job = Job.from_record(statedb.load(text))
                if job.id != job_id:
                    raise ValueError("its id isn't its row's")
            except (ValueError, TypeError, OverflowError) as e:
                _log(f"ignoring a saved job ({e})", LOG_WARNING)
                bad.append((job_id,))
                continue
            job.position = position
            job.on_progress = self._publish
            if job.state in ACTIVE:
                if job.state == RUNNING:
                    self._store.changed(job.id)  # saved as queued by the next save
                job.state = QUEUED  # interrupted: it resumes once the node runs
                job.stop_event = self._stop_event
                self._protect(job)
            else:
                job.ended.set()
            self._jobs[job.id] = job
        self._cap_finished()
        resumed = sum(1 for job in self._jobs.values() if job.state == QUEUED)
        if resumed:
            _log(f"{resumed} unfinished job(s) will resume")
        statedb.delete_rows(db, "jobs", ("id",), bad)

    def _row(self, job_id: str) -> tuple | None:
        """job_id's jobs row, or None once it is forgotten (lock held)."""
        from rnfs import statedb
        job = self._jobs.get(job_id)
        return None if job is None else (job_id, job.position, statedb.dump(job.to_record()))

    def save(self):
        """Save the jobs changed since the last save, and forget those gone, durably (a queued job must survive a
        crash); running jobs whose progress changed too (the node calls this periodically, keeping saved progress
        recent). Each change is noted with _store.changed(job ID), under the lock."""
        with self._lock:
            self._store.changed(*(job_id for job_id, job in self._jobs.items() if job.state == RUNNING))
        self._store.save()

    def _save_quietly(self):
        """save() after a change, logging rather than raising: a job's outcome mustn't be lost to a full disk (it
        is saved again by the next save)."""
        try:
            self.save()
        except OSError as e:
            _log(f"saving the jobs failed: {e!r}", LOG_ERROR)

    def close(self):
        """Release the state database (the node calls this once it has saved everything at shutdown)."""
        self._db.close()

    def _cap_finished(self):
        """Forget the oldest finished jobs beyond max_finished_jobs (lock held, or before any thread runs).

        Jobs finished less than FINISHED_GRACE ago stay regardless: the CLI following a job (or a script
        running `rnfs jobs wait`) asks for it after it ends, and a small cap mustn't make it vanish first.
        """
        limit = self.node.config.max_finished_jobs
        finished = [job for job in self._jobs.values() if job.state in FINISHED]
        finished.sort(key=lambda job: job.finished or job.created)
        recent = time.time() - FINISHED_GRACE
        for job in finished[:max(0, len(finished) - limit)]:
            if (job.finished or job.created) < recent:
                del self._jobs[job.id]
                self._store.changed(job.id)
                self._removed(job)

    # Events

    def _publish(self, job: Job):
        """Publish a "job" event of job's state and progress (Job.event; rnfs.events never raises). Also each job's
        on_progress, called on its thread with no lock held."""
        from rnfs import events
        events.publish(self.node, "job", **job.event())

    def _removed(self, job: Job):
        """Publish that job was forgotten (cleared, or past max_finished_jobs): its last state, and "removed"."""
        from rnfs import events
        events.publish(self.node, "job", id=job.id, job_kind=job.kind, state=job.state, removed=True)

    # Protection

    def _protect(self, job: Job):
        """Keep everything stored below the job's root while it is queued or running; an add or import job has none
        (it runs again from the start: the module docstring)."""
        from rnfs.content import walker
        if job.target is None:
            return
        root, _ = walker.parse_target(job.target)
        stack = ExitStack()
        session = stack.enter_context(self.node.storage.protect())
        session.add(root)
        job.protection = stack

    @staticmethod
    def _release(job: Job):
        """End the job's protection session: its blocks are ordinary cache (or garbage) again."""
        if job.protection is not None:
            job.protection.close()
            job.protection = None

    # Lifecycle

    def start(self):
        """Start running queued jobs."""
        with self._lock:
            self._started = True
        self._schedule()
        self._spawn(self._retry_loop, "rnfs-job-retry")

    def _retry_loop(self):
        """Start the queued jobs whose retry_at has come, every RETRY_POLL, until the node stops."""
        while not self._stop_event.wait(RETRY_POLL):
            now = time.time()
            with self._lock:
                due = any(job.state == QUEUED and job.retry_at is not None and job.retry_at <= now
                          for job in self._jobs.values())
            if due:
                self._schedule()

    def stop(self, deadline: float | None = None):
        """Ask running jobs to stop (they are saved as queued, to resume), wait for them until deadline (as
        Feature.stop), save."""
        with self._lock:
            self._started = False
            threads = list(self._threads.values())
        self._stop_event.set()
        self._join(deadline, threads)
        try:
            self.save()  # only if something changed: offline test nodes with no jobs write nothing
        except OSError as e:
            _log(f"saving the jobs failed: {e!r}", LOG_ERROR)

    def status(self) -> dict:
        """Running and queued job counts for `rnfs status`."""
        with self._lock:
            states = [job.state for job in self._jobs.values()]
        return {"jobs_running": states.count(RUNNING), "jobs_queued": states.count(QUEUED)}

    def _schedule(self) -> bool:
        """Start queued jobs, oldest first, while fewer than max_jobs run; True if any started (and were saved)."""
        starting = []
        with self._lock:
            if not self._started or self._stop_event.is_set():
                return False
            running = sum(1 for job in self._jobs.values() if job.state == RUNNING)
            now = time.time()
            for job in self._jobs.values():
                if running >= self.max_jobs:
                    break
                if job.state != QUEUED or (job.retry_at is not None and job.retry_at > now):
                    continue
                job.begin_run()
                self._store.changed(job.id)
                thread = threading.Thread(target=self._run, args=(job,), name=f"rnfs-job-{job.id}", daemon=True)
                self._threads[job.id] = thread
                starting.append((job, thread))
                running += 1
        for job, _ in starting:
            self._publish(job)  # before its thread runs, so its first progress event comes after
        for _, thread in starting:
            thread.start()
        if starting:
            self._save_quietly()
        return bool(starting)

    def _run(self, job: Job):
        """Run a job to its end on this thread, then start the next."""
        from rnfs import fetcher
        from rnfs.errors import RnfsError
        state, error, result = FAILED, None, None
        try:
            with fetcher.observing(job):
                result = self._execute(job)
            state = DONE
        except fetcher.Cancelled:
            # Stopped by the shutdown rather than the user: queued again, to resume at the next start
            state = CANCELLED if job.cancel_requested or not self._stop_event.is_set() else QUEUED
        except (RnfsError, ValueError, OSError) as e:
            error = str(e) or type(e).__name__
            if self._retries(job, e):
                state = QUEUED
        except Exception as e:  # noqa: BLE001 - a job's failure mustn't take its thread's bookkeeping with it
            error = str(e) or type(e).__name__
            _log(f"job {job.id} ({job.kind} {job.subject}) failed: {e!r}", LOG_ERROR)
        self._finish(job, state, error, result)

    def _retries(self, job: Job, error: Exception) -> bool:
        """Whether a run that failed with error is tried again later: a get, pin or export whose content no provider
        could supply (NotFound), if it was submitted with retry true, unless the job was cancelled, the node is
        stopping, its path is known not to exist (_absent_path) or rnfs.retry.RETRY_FOR has passed since it first
        wasn't found. Sets its retry_at (and retry_since, the first time)."""
        from rnfs.errors import NotFound, Unsupported
        if (not job.retry or job.kind not in RETRYING_KINDS or not isinstance(error, NotFound)
                or isinstance(error, Unsupported)
                or job.cancel_requested or self._stop_event.is_set() or self._absent_path(job.target)):
            return False
        now = time.time()
        since = job.retry_since if job.retry_since is not None else now
        retry_at = next_retry(since, now)
        if retry_at is None:
            return False
        with self._lock:
            job.retry_at, job.retry_since = retry_at, since
        _log(f"job {job.id} ({job.kind} {job.subject}): {error}; trying again in {(retry_at - now) / 60:.0f} minutes",
             LOG_NOTICE)
        return True

    def _absent_path(self, target: str) -> bool:
        """True if target's path is known not to exist: every block along it that is needed to tell is stored, and a
        name isn't there (trying again can't change that: a CID's content never does)."""
        from rnfs.content import walker
        from rnfs.errors import NotFound
        root, names = walker.parse_target(target)
        expect = walker.root_expect(root)
        get = self.node.storage.getter()
        try:
            for name in names:
                expect = walker.step_name(expect, name, get)
                if expect is None:
                    return True
        except (NotFound, ValueError):  # a block along it isn't stored (or isn't a directory's): not known
            return False
        return False

    def _execute(self, job: Job) -> dict:
        """The facade call the job stands for; its result."""
        node = self.node
        if job.kind == "get":
            result = self._get(job)
        elif job.kind == "pin":
            result = node.pin_add(job.target, **_source(job))
        elif job.kind == "export":
            result = node.export(job.target, job.output)
        elif job.kind == "add":
            result = self._add(job)
        else:
            result = self._import(job)
        job.blocks_total = self._dag_block_count(result)
        return result

    def _add(self, job: Job) -> dict:
        """An add job: measure what it will read, then node.add_path with its options, as POST /api/add calls it; or
        node.add_nomadnet of its NomadNet link, its progress what has arrived of the answer."""
        if job.nomadnet is not None:
            return self.node.add_nomadnet(job.nomadnet, progress=_answer_progress(job), **(job.options or {}),
                                          **_source(job))
        from rnfs.content import builder, shared_folder
        try:
            skip_dirs = shared_folder.private_dirs(self.node.config.root)  # as add_path skips them
            job.total(builder.tree_size(job.path, job.check_cancelled, skip_dirs))
        except OSError:
            pass  # add_path reports it, in its own words
        return self.node.add_path(job.path, progress=job.read, **(job.options or {}), **_source(job))

    def _import(self, job: Job) -> dict:
        """An import job: node.import_archive, as POST /api/import calls it; its size is what it will read."""
        try:
            job.total(os.path.getsize(job.path))
        except OSError:
            pass  # import_archive reports it, in its own words
        return self.node.import_archive(job.path, progress=job.read, **_source(job))

    def _get(self, job: Job) -> dict:
        """A get job: write to its partial path, then rename that to the output (or the sealed name)."""
        from rnfs import fsutil
        partial = partial_path(job)
        remove_partial(partial)  # what an interrupted earlier run (or a crash) left
        try:
            result = self._fetch_to(job, partial)
            output = job.output
            if job.sealed_name and result.get("sealed"):
                # node_content only names a sealed file fit to be an output name; checked again here since
                # the name is the sender's choice and becomes a file in the user's directory
                name = fsutil.safe_default_name(result.get("name"), None)
                if name is not None:
                    output = os.path.join(os.path.dirname(job.output), name)
            fsutil.rename_new(partial, output)
        except BaseException:
            try:
                remove_partial(partial)  # stored blocks stay, so a retry or resume writes it out again quickly
            except OSError as e:  # the job's own error matters more; the next run tries again
                _log(f"job {job.id}: removing {partial} failed: {e!r}", LOG_WARNING)
            raise
        with self._lock:
            job.output = output
        return {**result, "output": output}

    def _fetch_to(self, job: Job, path: str) -> dict:
        """node.get of the job's target to path: from its from node, then (fallback_dht) by DHT discovery."""
        from rnfs import fetcher
        from rnfs.errors import Denied, RnfsError, StorageFull, TooLarge
        from_node = bytes.fromhex(job.from_node) if job.from_node else None
        options = {"no_extract": True} if job.no_extract else {}
        try:
            return self.node.get(job.target, path, from_node=from_node, **options)
        except RnfsError as e:
            # Cancelled must stop the job; the content's size, the node's storage and its deny list don't depend
            # on the source
            if from_node is None or not job.fallback_dht or isinstance(e, (fetcher.Cancelled, TooLarge, StorageFull,
                                                                           Denied)):
                raise
            _log(f"job {job.id}: getting {job.target} from {job.from_node} failed ({e}); trying the DHT",
                 LOG_NOTICE)
        remove_partial(path)
        return self.node.get(job.target, path, **options)

    def _dag_block_count(self, result: dict) -> int | None:
        """How many blocks the finished job's DAG has, now that it is complete."""
        from rnfs.content.cid import Cid
        if "blocks" in result:
            return result["blocks"]
        try:
            return len(self.node.storage.dag_blocks(Cid.parse(result["cid"])))
        except Exception:  # noqa: BLE001 - only a progress figure
            return None

    def _finish(self, job: Job, state: str, error: str | None, result: dict | None):
        """Record a run's outcome, release a finished job's blocks, save and start the next job."""
        if state in FINISHED:
            self._release(job)  # first, so whoever sees it finished (e.g. pin rm then gc) finds it released
        with self._lock:
            job.state = state
            job.error = error
            job.result = _capped(result) if result is not None else None
            if state == DONE and job.total_bytes is not None:
                job.done_bytes = job.total_bytes
            if state in FINISHED:
                job.finished = time.time()
                job.retry_at = job.retry_since = None
            self._threads.pop(job.id, None)
            self._store.changed(job.id)
            self._cap_finished()
        self._publish(job)
        if state in FINISHED:
            job.ended.set()
            _log(f"job {job.id} ({job.kind} {job.subject}) {state}" + (f": {error}" if error else ""))
        if not self._schedule():  # starting the next saves this outcome with it
            self._save_quietly()

    # Operations (API)

    def submit(self, kind: str, target: str | None = None, output: str | None = None, from_node: str | None = None,
               sealed_name: bool = False, fallback_dht: bool = False, path: str | None = None,
               options: dict | None = None, no_extract: bool = False, source: str = USER,
               nomadnet: str | None = None, retry: bool = False) -> dict:
        """Queue a job; its dict. Raises ValueError for bad parameters or an output in use, NotFound for an add's
        or import's path that doesn't exist, Busy when full.

        For get: sealed_name writes a sealed file under its sealed name beside output; fallback_dht retries
        by DHT discovery if from_node fails; no_extract writes a sealed folder as its tar. For add and import:
        path, and for add options (ADD_OPTIONS); or for add a NomadNet link (nomadnet) instead of a path, with
        NOMADNET_OPTIONS among its options too. For pin, add and import: source, what they pin with ("user", or the
        app's that queued it, which removing the app cancels: app_removed). retry True: a get, pin or export whose
        content isn't found is queued again to be tried later (rnfs.retry) rather than failing at once, as in 0.1.6.
        """
        from rnfs.errors import Busy, NotFound
        contacts = self.dependency("contacts")
        if contacts is not None and target is not None:
            # `alice/docs`: a name contact's nick becomes the CID its name points at now (rnfs.features.contacts)
            target = contacts.expand_target(target)
        check_job_params(kind, target, output, from_node, sealed_name, fallback_dht, path, options, no_extract,
                         source, nomadnet=nomadnet, retry=retry)
        if output is not None and os.path.lexists(output):
            raise ValueError(f"{output} already exists")
        if path is not None and not os.path.lexists(path):
            # As add_path and import_archive say it, but before queueing: the CLI and apps learn it at once
            raise NotFound(f"no such file or directory: {path}" if kind == "add" else f"no such file: {path}")
        if kind == "add" and path is not None:
            from rnfs.content import shared_folder
            # As add_path refuses it (audit REV1), before the size of all of ~ is measured for nothing
            refused = shared_folder.add_refused_reason(path, self.node.config.root)
            if refused is not None:
                raise ValueError(f"refusing to add {path}: {refused}")
        if kind == "import" and not os.path.isfile(path):
            # A FIFO or device would hold a job slot until something writes to it (import_archive refuses it too)
            raise ValueError(f"{path} is not a regular file")
        with self._lock:
            active = [job for job in self._jobs.values() if job.state in ACTIVE]
            if len(active) >= MAX_ACTIVE:
                raise Busy(f"{MAX_ACTIVE} jobs are already queued or running")
            for other in active:
                if output is not None and other.output == output:
                    raise ValueError(f"job {other.id} is already writing {output}")
            job_id = secrets.token_hex(4)
            while job_id in self._jobs:
                job_id = secrets.token_hex(4)
            job = Job(job_id, kind, target, output, from_node, sealed_name=sealed_name, fallback_dht=fallback_dht,
                      path=path, options=options, no_extract=no_extract, source=source, nomadnet=nomadnet,
                      retry=retry)
            job.stop_event = self._stop_event
            job.on_progress = self._publish
            job.position = self._position
            self._position += 1
            self._protect(job)
            self._jobs[job_id] = job
            self._store.changed(job_id)
        _log(f"job {job_id} queued: {kind} {job.subject}" + (f" to {output}" if output else ""))
        self._publish(job)  # queued, before _schedule may start it
        if not self._schedule():  # else saved as it started
            self._save_quietly()
        return job.to_dict()

    def _job(self, job_id: str) -> Job:
        """The job with this ID. Raises NotFound."""
        from rnfs.errors import NotFound
        with self._lock:
            job = self._jobs.get(job_id)
        if job is None:
            raise NotFound(f"no job {job_id}")
        return job

    def get(self, job_id: str, wait: float = 0) -> dict:
        """A job's dict, after waiting up to wait seconds for it to finish."""
        job = self._job(job_id)
        if wait > 0:
            job.ended.wait(wait)
        return job.to_dict()

    def list(self) -> list[dict]:
        """Every job, oldest first."""
        with self._lock:
            jobs = list(self._jobs.values())
        return [job.to_dict() for job in jobs]

    def cancel(self, job_id: str) -> dict:
        """Cancel a job: a queued one at once, a running one at its fetch's next step (an add's or import's next
        read). Finished ones stay."""
        job = self._job(job_id)
        ended = False
        with self._lock:
            if job.state == QUEUED:
                job.state = CANCELLED
                job.finished = time.time()
                job.retry_at = job.retry_since = None
                self._store.changed(job.id)
                ended = True
            elif job.state == RUNNING:
                job.cancel_requested = True
        if ended:
            self._release(job)
            job.ended.set()
            self._publish(job)
            with self._lock:
                self._cap_finished()
            self._save_quietly()
        return job.to_dict()

    def app_removed(self, name: str):
        """App name was removed: cancel its queued and running jobs (a pin a running one still makes is released
        by the node, RnfsNode._app_still_there)."""
        from rnfs.content.storage import app_of
        with self._lock:
            mine = [job.id for job in self._jobs.values() if job.state in ACTIVE and app_of(job.source) == name]
        for job_id in mine:
            self.cancel(job_id)
        if mine:
            _log(f"cancelled {len(mine)} job{'' if len(mine) == 1 else 's'} of app {name}, which was removed")

    def clear(self, job_id: str | None = None) -> int:
        """Forget finished jobs (or just job_id, which must be finished); how many were removed."""
        if job_id is not None and self._job(job_id).state not in FINISHED:
            raise ValueError(f"job {job_id} hasn't finished; cancel it first")
        with self._lock:
            removed = [jid for jid, job in self._jobs.items()
                       if job.state in FINISHED and (job_id is None or jid == job_id)]
            for jid in removed:
                self._removed(self._jobs.pop(jid))
            self._store.changed(*removed)
        if removed:
            self._save_quietly()
        return len(removed)

    # API

    @classmethod
    def api_routes(cls):
        """The /api/jobs endpoints."""
        return {("POST", "/api/jobs"): api_start, ("GET", "/api/jobs"): api_list,
                ("GET", "/api/jobs/get"): api_get, ("POST", "/api/jobs/cancel"): api_cancel,
                ("POST", "/api/jobs/clear"): api_clear}

    # CLI

    @classmethod
    def cli_commands(cls, commands, command):
        """`rnfs jobs [list|show|wait|cancel|clear]`."""
        jobs = command(commands, "jobs", run_list, "list background transfers with their progress")
        sub = jobs.add_subparsers(dest="jobs_command", metavar="command")
        command(sub, "list", run_list, "list jobs: ID, state, kind, progress, target")
        for name, run, help in (("show", run_show, "show everything about a job"),
                                ("wait", run_wait, "wait for a job to end, showing its progress; fails if it does"),
                                ("cancel", run_cancel, "cancel a queued or running job")):
            command(sub, name, run, help).add_argument("id", metavar="ID")
        clear = command(sub, "clear", run_clear, "forget finished jobs")
        clear.add_argument("id", metavar="ID", nargs="?", help="just this finished job")


FEATURE = JobsFeature


def _answer_progress(job: Job):
    """The progress callback (received, total) an add job of a NomadNet link gives the fetch: the answer's size as
    the job's total, what has arrived as read (Job.read, which raises Cancelled once the job is cancelled)."""
    counted = 0

    def progress(received: int, total: int | None):
        """Count what arrived since the last call; checks for cancellation even when nothing did."""
        nonlocal counted
        if total is not None and job.total_bytes != total:
            job.total(total)
        job.read(max(0, received - counted))
        counted = max(counted, received)

    return progress


def partial_path(job: Job) -> str:
    """Where a get job writes before renaming to its output: hidden, beside it, named by the job's ID."""
    return os.path.join(os.path.dirname(job.output), f".rnfs-job-{job.id}{PARTIAL_SUFFIX}")


def remove_partial(path: str):
    """Delete a get job's partial output, file or directory tree; a symlink there is removed, not followed."""
    from rnfs import fsutil
    if os.path.isdir(path) and not os.path.islink(path):
        shutil.rmtree(path)
    else:
        fsutil.remove(path)


# API handlers: handler is the ApiHandler; every route requires the API token


def _feature(handler) -> JobsFeature:
    """The node's job manager. Raises NotFound if the node has none (clients then fall back)."""
    return _common.feature(handler, NAME, "this daemon has no job manager")


def api_start(handler):
    """POST /api/jobs {"kind", "target", "output", "from", "sealed_name", "fallback_dht", "no_extract", "retry"} for
    get, pin and export; {"kind": "add", "path" or "nomadnet", and POST /api/add's options} or {"kind": "import",
    "path"}: queue a job; the job. A pin, add or import job pins with the caller's source (handler.pin_source:
    user, or an app's)."""
    from rnfs import api
    source = handler.pin_source()
    body = handler.json_body()
    kind = body.get("kind")
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {', '.join(KINDS)}")
    if kind == "add" and body.get("nomadnet") is not None:
        if body.get("path") is not None:
            raise ValueError("give either path or nomadnet, not both")
        handler.send_json(200, _feature(handler).submit(kind, body.get("target"), body.get("output"),
                                                        nomadnet=api.parse_nomadnet(body.get("nomadnet")),
                                                        options=add_job_options(body.get, nomadnet=True), **source))
        return
    if kind in LOCAL_KINDS:
        path = api.parse_path(body.get("path"))
        options = add_job_options(body.get) if kind == "add" else None
        # A target or output given is refused by submit's check, as for the other kinds
        handler.send_json(200, _feature(handler).submit(kind, body.get("target"), body.get("output"), path=path,
                                                        options=options, **source))
        return
    target = api.parse_target(body.get("target"))
    output = api.parse_path(body.get("output")) if kind != "pin" else body.get("output")
    from_node = api.parse_hex_id(body["from"]).hex() if body.get("from") else None
    flags = {name: body.get(name, False) for name in ("sealed_name", "fallback_dht")}  # checked by submit
    if "no_extract" in body:
        flags["no_extract"] = body["no_extract"]  # checked by submit too
    if "retry" in body:
        flags["retry"] = body["retry"]  # checked by submit too
    handler.send_json(200, _feature(handler).submit(kind, target, output, from_node, **flags,
                                                    **(source if kind == "pin" else {})))


def api_list(handler):
    """GET /api/jobs: {"jobs": [job]}, oldest first."""
    handler.send_json(200, {"jobs": _feature(handler).list()})


def api_get(handler):
    """GET /api/jobs/get?id=&wait=: one job, after waiting up to wait seconds (at most MAX_WAIT) for its end."""
    from rnfs import api
    job_id = check_job_id(handler.param("id"))
    wait = min(api.parse_int(handler.param("wait", False), "wait", 0), MAX_WAIT)
    handler.send_json(200, _feature(handler).get(job_id, wait))


def api_cancel(handler):
    """POST /api/jobs/cancel {"id"}: cancel a job; the job."""
    job_id = check_job_id(handler.json_body().get("id"))
    handler.send_json(200, _feature(handler).cancel(job_id))


def api_clear(handler):
    """POST /api/jobs/clear {"id" (optional)}: forget finished jobs; {"removed": count}."""
    job_id = handler.json_body().get("id")
    if job_id is not None:
        check_job_id(job_id)
    handler.send_json(200, {"removed": _feature(handler).clear(job_id)})


# Formatting (pure, for the CLI and its tests)


def human_size(num_bytes: float | None) -> str:
    """A size in decimal units: "512 B", "1.5 KB", "3.20 MB"."""
    if num_bytes is None:
        return "?"
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1000 or unit == "TB":
            if unit == "B":
                return f"{int(value)} B"
            return f"{value:.2f} {unit}" if value < 10 else f"{value:.1f} {unit}" if value < 100 else f"{value:.0f} {unit}"
        value /= 1000
    return f"{value:.0f} TB"  # not reached


def human_duration(seconds: float | None) -> str:
    """A duration: "42s", "5m03s", "2h07m", "3d04h"; "?" if unknown."""
    if seconds is None:
        return "?"
    seconds = int(round(seconds))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    if seconds < 86400:
        return f"{seconds // 3600}h{seconds % 3600 // 60:02d}m"
    return f"{seconds // 86400}d{seconds % 86400 // 3600:02d}h"


def progress_bar(fraction: float, width: int = 20) -> str:
    """[#####-----] for a fraction between 0 and 1."""
    filled = int(max(0.0, min(1.0, fraction)) * width)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def format_progress(job: dict, width: int = 20) -> str:
    """A job's progress on one line: bar, percent, bytes, rate and ETA while running; its outcome after."""
    state = job.get("state")
    total, done = job.get("total_bytes"), job.get("done_bytes") or 0
    if state == QUEUED and job.get("retry_at") is not None:
        when = time.strftime("%H:%M", time.localtime(job["retry_at"]))
        return f"waiting, tried again at {when}: " + (job.get("error") or "not found")
    if state == QUEUED:
        return "queued"
    if state == RUNNING:
        rate = job.get("rate")
        speed = f"{human_size(rate)}/s" if rate is not None else "-/s"
        if total is None and job.get("nomadnet"):
            return f"waiting for the NomadNet node's answer: {human_size(done)}, {speed}"
        if total is None and job.get("kind") in LOCAL_KINDS:
            return f"reading: {human_size(done)}, {speed}"  # its size is being measured
        if total is None:
            blocks = job.get("blocks") or 0
            return f"finding the target: {blocks} block{'' if blocks == 1 else 's'}, {speed}"
        fraction = done / total if total else 1.0
        return (f"{progress_bar(fraction, width)} {int(min(fraction, 1.0) * 100):3d}%  {human_size(done)} of "
                f"{human_size(total)}  {speed}  ETA {human_duration(job.get('eta'))}")
    if state == DONE:
        return f"done, {human_size(total)}" if total is not None else "done"
    if state == FAILED:
        return "failed: " + (job.get("error") or "unknown error")
    return state or "?"


def format_job(job: dict) -> str:
    """One `rnfs jobs` line: ID, state, kind, progress, target (and output), or an add's or import's path."""
    where = f" -> {job['output']}" if job.get("output") else ""
    subject = job.get("target") or job.get("path")
    return printable(f"{job['id']}  {job['state']:<9} {job['kind']:<6} {format_progress(job)}  {subject}{where}")


def job_fields(job: dict) -> dict:
    """A job's fields for `rnfs jobs show`, readable."""
    fields = {"id": job["id"], "kind": job["kind"], "state": job["state"]}
    for name in ("target", "path", "output", "from", "sealed_name", "fallback_dht", "no_extract", "source"):
        if job.get(name):
            fields[name] = job[name]
    if job.get("options"):
        fields["options"] = json.dumps(job["options"])
    fields["progress"] = format_progress(job)
    total = job.get("total_bytes")
    fields["bytes"] = f"{job.get('done_bytes') or 0} of {total if total is not None else '?'}"
    fields["blocks"] = job.get("blocks") if job.get("blocks_total") is None else f"{job['blocks']} of {job['blocks_total']}"
    fields["downloaded"] = human_size(job.get("downloaded_bytes") or 0)
    if job.get("rate") is not None:
        fields["rate"] = f"{human_size(job['rate'])}/s"
        fields["eta"] = human_duration(job.get("eta"))
    for name in ("created", "started", "finished", "retry_at", "retry_since"):
        if job.get(name):
            fields[name] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(job[name]))
    fields["runs"] = job.get("runs", 0)
    if job.get("error"):
        fields["error"] = job["error"]
    if job.get("result"):
        fields["result"] = json.dumps(job["result"])
    return fields


class ProgressLine:
    """One line on a terminal (stderr), rewritten in place; cleared when done. Only for TTYs."""

    def __init__(self, stream=None):
        """Write to stream (default stderr)."""
        self.stream = stream or sys.stderr
        self.shown = 0  # length of what is on the line now

    def show(self, text: str):
        """Replace the line with text, cut to the terminal's width."""
        width = max(10, shutil.get_terminal_size((80, 20)).columns - 1)
        text = text[:width]
        # Spaces rather than an ANSI erase, which old Windows consoles don't understand
        self.stream.write("\r" + text + " " * max(0, self.shown - len(text)))
        self.stream.flush()
        self.shown = len(text)

    def clear(self):
        """Erase the line."""
        if self.shown:
            self.stream.write("\r" + " " * self.shown + "\r")
            self.stream.flush()
            self.shown = 0


class StreamProgress:
    """Progress of a streamed download (`rnfs cat` into a pipe or file): called with (bytes so far, expected)."""

    def __init__(self, stream=None, clock=time.monotonic):
        """Show on stream (default stderr), at most a few times a second."""
        self.line = ProgressLine(stream)
        self.clock = clock
        self.start = clock()
        self.last = float("-inf")

    def __call__(self, copied: int, expected):
        """Update the line if it is time to."""
        now = self.clock()
        if now - self.last < 0.25:
            return
        self.last = now
        self.line.show(format_stream_progress(copied, int(expected) if expected is not None else None,
                                              now - self.start))

    def close(self):
        """Erase the line."""
        self.line.clear()


def format_stream_progress(copied: int, expected: int | None, elapsed: float) -> str:
    """bar, percent, bytes and average rate of a stream copied for elapsed seconds."""
    rate = copied / elapsed if elapsed > 0 else None
    speed = f"{human_size(rate)}/s" if rate is not None else "-/s"
    if not expected:
        return f"{human_size(copied)}  {speed}"
    fraction = copied / expected
    eta = (expected - copied) / rate if rate else None
    return (f"{progress_bar(fraction)} {int(min(fraction, 1.0) * 100):3d}%  {human_size(copied)} of "
            f"{human_size(expected)}  {speed}  ETA {human_duration(eta)}")


# CLI


def follow(api, job_id: str, cancel_on_interrupt: bool) -> dict:
    """Poll a job until it ends, with a progress line if stderr is a terminal; the finished job.

    Ctrl+C cancels the job if cancel_on_interrupt (a foreground transfer), else just stops waiting; both
    exit with status 130.
    """
    line = ProgressLine() if sys.stderr.isatty() else None
    try:
        while True:
            job = api.get("/api/jobs/get", id=job_id, wait=POLL_TTY if line else POLL_QUIET)
            if job["state"] in FINISHED:
                return job
            if line:
                line.show(printable(format_progress(job)))
    except KeyboardInterrupt:
        if line:
            line.clear()
        if cancel_on_interrupt:
            api.post("/api/jobs/cancel", id=job_id)
            print(f"rnfs: cancelled job {printable(job_id)} (with --background, a transfer keeps running after "
                  "the command ends)", file=sys.stderr)
        sys.exit(130)
    finally:
        if line:
            line.clear()


def has_jobs(api) -> bool:
    """True if the daemon runs the job manager (its status counts jobs); older daemons don't."""
    return "jobs_queued" in api.get("/api/status")


def run_transfer(args, fields: dict, fallback) -> dict | None:
    """Run a transfer as a job and follow it; its result, or None with --background (its ID is printed).

    fallback(api) does the transfer in one request instead, for a daemon without the job manager (None: only with
    --background, which fails then). A daemon whose job manager predates the kind refuses it (400, kind must be one
    of ...).
    """
    from rnfs import cli
    api = cli.client(args)
    if not has_jobs(api):
        if getattr(args, "background", False):
            cli.fail("this daemon can't run background jobs")
        return fallback(api)
    if getattr(args, "background", False) and fields.get("kind") in RETRYING_KINDS:
        # A background job runs unattended, so it waits for content not found yet (over LoRa, often not published
        # yet); in the foreground it fails the command at once rather than waiting hours. A daemon before 0.2.0
        # ignores the field: it never waits.
        fields = {**fields, "retry": True}
    job = api.post("/api/jobs", **fields)
    if getattr(args, "background", False):
        # Everything the daemon sends is escaped before it reaches the terminal, its job IDs too
        job_id = printable(job["id"])
        print(job_id)
        if sys.stderr.isatty():
            print(f"rnfs: started job {job_id}; follow it with: rnfs jobs wait {job_id}", file=sys.stderr)
        return None
    job = follow(api, job["id"], cancel_on_interrupt=True)
    if job["state"] != DONE:
        cli.fail(job.get("error") or f"job {job['id']} was {job['state']}")  # fail() escapes
    return job["result"]


def print_result(job: dict):
    """Print a finished job's result as the command that started it would have."""
    from rnfs import cli
    result = job.get("result") or {}
    # Outputs, targets and warnings can carry names other people chose: escaped, never raw to the terminal
    if job["kind"] == "get":
        cli.print_warnings(result)
        print(cli.printable(result.get("output") or job["output"]))
    elif job["kind"] == "pin":
        print(cli.printable(result.get("cid")))
    elif job["kind"] == "add":
        cli.print_added(result)
    elif job["kind"] == "import":
        cli.print_imported(result)
    else:
        print(cli.printable(f"exported {result.get('blocks')} blocks of {', '.join(result.get('roots') or [])} "
                            f"to {job['output']}"))


def run_list(args):
    """List jobs, oldest first."""
    from rnfs import cli
    jobs = cli.client(args).get("/api/jobs")["jobs"]
    if not jobs:
        print("no jobs")
    for job in jobs:
        print(format_job(job))


def run_show(args):
    """Print one job's fields."""
    from rnfs import cli
    cli.print_fields(job_fields(cli.client(args).get("/api/jobs/get", id=_cli_id(args.id))))


def run_wait(args):
    """Wait for a job to end; print its result, or fail with its error."""
    from rnfs import cli
    job = follow(cli.client(args), _cli_id(args.id), cancel_on_interrupt=False)
    if job["state"] != DONE:
        cli.fail(f"job {job['id']} {job['state']}" + (f": {job['error']}" if job.get("error") else ""))
    print_result(job)


def run_cancel(args):
    """Cancel a job."""
    from rnfs import cli
    job = cli.client(args).post("/api/jobs/cancel", id=_cli_id(args.id))
    if job["state"] == RUNNING:
        print(f"cancelling {printable(job['id'])}")
    elif job["state"] == CANCELLED:
        print(f"cancelled {printable(job['id'])}")
    else:
        print(printable(f"{job['id']} already {job['state']}"))


def run_clear(args):
    """Forget finished jobs."""
    from rnfs import cli
    removed = cli.client(args).post("/api/jobs/clear", id=_cli_id(args.id) if args.id else None)["removed"]
    print(printable(f"removed {removed} finished job{'' if removed == 1 else 's'}"))


def _cli_id(text: str) -> str:
    """A job ID from the command line, or exit with an error."""
    from rnfs import cli
    try:
        return check_job_id(text.strip().lower())
    except ValueError as e:
        cli.fail(str(e))
