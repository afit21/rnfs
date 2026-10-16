"""Background jobs for long transfers (feature "jobs"): get, pin and export run by the daemon, with progress.

On LoRa a 1 MB file can take an hour, so a transfer runs as a job the daemon owns instead of inside one API
request. `rnfs get`, `pin add` and `export` start a job and follow it with a progress line on stderr (or
return at once with --background); `rnfs jobs` lists, shows, waits for, cancels and clears jobs.

- A job runs one node facade call (get, pin_add, export) on a thread of its own, inside
  fetcher.observing(): the fetch reports the verified size of what it fetches and each block it meets, and
  raises Cancelled at its next step (within fetcher.POLL) once the job is cancelled or the node stops.
  At most max_jobs run at once (2; 1 on slow nodes); queued jobs start oldest first.
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
  (if fit, fsutil.safe_default_name) in the output's directory instead. With fallback_dht, a get whose
  from node fails is retried with ordinary DHT discovery.
- Finished jobs (done, failed, cancelled) are kept for `rnfs jobs`, at most max_finished_jobs, but each
  for at least FINISHED_GRACE: a CLI following a fast job must still find it once it ends.
- Job threads are daemon threads, so they never hold up the process; stop() asks running fetches to stop
  and waits at most STOP_WAIT for them. A job interrupted by the shutdown is saved as queued.

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

from rnfs.constants import PARTIAL_SUFFIX
from rnfs.features import Feature, _common
from rnfs.features._common import LOG_ERROR, LOG_INFO, LOG_NOTICE, LOG_WARNING

NAME = "jobs"
KINDS = ("get", "pin", "export")
QUEUED, RUNNING, DONE, FAILED, CANCELLED = "queued", "running", "done", "failed", "cancelled"
STATES = (QUEUED, RUNNING, DONE, FAILED, CANCELLED)
ACTIVE = frozenset({QUEUED, RUNNING})
FINISHED = frozenset({DONE, FAILED, CANCELLED})
MAX_ACTIVE = 1000  # queued and running jobs at most, so a script can't queue without bound
RATE_WINDOW = 300.0  # seconds the rate is averaged over: LoRa responses can arrive minutes apart
SAMPLE_INTERVAL = 1.0  # seconds between rate samples
STOP_WAIT = 2.0  # seconds stop() waits for running jobs to notice they must stop
MAX_WAIT = 30  # longest wait= a GET /api/jobs/get may ask for, in seconds
POLL_TTY = 1  # seconds between progress updates the CLI shows
POLL_QUIET = 10  # seconds a CLI without a progress line waits per request (it returns early when the job ends)
FINISHED_GRACE = 60.0  # seconds a finished job is kept whatever max_finished_jobs says
_JOB_ID = re.compile(r"[0-9a-f]{8}")
_NODE_ID = re.compile(r"[0-9a-f]{32}")


def _log(message: str, level: int = LOG_INFO):
    """Log with this feature's prefix."""
    _common.log(f"jobs: {message}", level)


class Job:
    """One transfer: what it is, its state and its progress. Also the fetch Observer of its runs."""

    def __init__(self, job_id: str, kind: str, target: str, output: str | None = None,
                 from_node: str | None = None, created: float | None = None, sealed_name: bool = False,
                 fallback_dht: bool = False):
        """A queued job; from_node is a node ID in hex. sealed_name and fallback_dht are for get jobs."""
        self.id = job_id
        self.kind = kind
        self.target = target
        self.output = output
        self.from_node = from_node
        self.sealed_name = sealed_name  # write a sealed file under its sealed name, beside output
        self.fallback_dht = fallback_dht  # if from_node fails, try again with ordinary DHT discovery
        self.state = QUEUED
        self.created = time.time() if created is None else created
        self.started: float | None = None  # when the latest run started
        self.finished: float | None = None
        self.error: str | None = None
        self.result: dict | None = None
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
        self._lock = threading.Lock()  # guards _samples and _seen
        self._samples: deque[tuple[float, int]] = deque()  # (monotonic time, downloaded_bytes)
        self._seen: set = set()  # CIDs met this run, so each counts once

    # Observer (rnfs.fetcher.Observer), called on the job's thread

    def total(self, size: int):
        """The fetch knows the verified size of what it fetches."""
        self.total_bytes = size

    def block(self, cid, length: int, downloaded: bool):
        """A block met: count it once, its bytes as content if it is a chunk."""
        now = time.monotonic()
        with self._lock:
            if downloaded:
                self.downloaded_bytes += length
                if not self._samples or now - self._samples[-1][0] >= SAMPLE_INTERVAL:
                    self._samples.append((now, self.downloaded_bytes))
            if cid in self._seen:
                return
            self._seen.add(cid)
            self.blocks += 1
            if cid.is_raw:
                self.done_bytes += length

    def cancelled(self) -> bool:
        """True once the job is cancelled or the node stops."""
        return self.cancel_requested or (self.stop_event is not None and self.stop_event.is_set())

    # Runs

    def begin_run(self):
        """Reset this run's progress (stored blocks are met again quickly, so a resume catches up at once)."""
        with self._lock:
            self.state = RUNNING
            self.started = time.time()
            self.runs += 1
            self.done_bytes = self.blocks = self.downloaded_bytes = 0
            self._seen = set()
            self._samples = deque([(time.monotonic(), 0)])

    def rate(self, now: float | None = None) -> float | None:
        """Bytes/s downloaded over the last RATE_WINDOW of a running job (it decays while nothing arrives)."""
        now = time.monotonic() if now is None else now
        with self._lock:
            if self.state != RUNNING or not self._samples:
                return None
            # Keep one sample at or before the window's start, as the base to measure from
            while len(self._samples) > 1 and self._samples[1][0] <= now - RATE_WINDOW:
                self._samples.popleft()
            since, base = self._samples[0]
            downloaded = self.downloaded_bytes
        elapsed = now - since
        if elapsed < SAMPLE_INTERVAL:
            return None
        return max(0.0, (downloaded - base) / elapsed)

    def to_dict(self) -> dict:
        """The job as the API shows it, with its current rate and ETA."""
        rate = self.rate()
        eta = None
        if rate and self.total_bytes is not None:
            eta = max(0.0, (self.total_bytes - self.done_bytes) / rate)
        return {**self.to_record(), "state": self.state, "rate": rate, "eta": eta}

    def to_record(self) -> dict:
        """The job as saved (a running job is saved as such, and queued again when loaded)."""
        return {"id": self.id, "kind": self.kind, "target": self.target, "output": self.output,
                "from": self.from_node, "sealed_name": self.sealed_name, "fallback_dht": self.fallback_dht,
                "state": self.state, "created": self.created, "started": self.started,
                "finished": self.finished, "error": self.error, "result": self.result, "runs": self.runs,
                "total_bytes": self.total_bytes, "done_bytes": self.done_bytes, "blocks": self.blocks,
                "blocks_total": self.blocks_total, "downloaded_bytes": self.downloaded_bytes}

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
        check_job_params(kind, target, output, from_node, sealed_name, fallback_dht)
        job = cls(job_id, kind, target, output, from_node, _number(record.get("created"), time.time()),
                  sealed_name, fallback_dht)
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


def check_job_params(kind: str, target, output, from_node, sealed_name=False, fallback_dht=False):
    """Raise ValueError unless these fit a job of kind: a well-formed target, an absolute output path for
    get and export (none for pin), and from (a node ID in hex), sealed_name and fallback_dht (booleans)
    only for get."""
    from rnfs.content import walker  # validates the root CID and every path name
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {', '.join(KINDS)}")
    walker.parse_target(target)
    if kind == "pin":
        if output is not None:
            raise ValueError("a pin job has no output")
    elif not isinstance(output, str) or not os.path.isabs(output) or "\x00" in output:
        raise ValueError(f"output must be an absolute path: {output!r}")
    if from_node is not None:
        if kind != "get":
            raise ValueError("only get jobs take from")
        if not isinstance(from_node, str) or not _NODE_ID.fullmatch(from_node):
            raise ValueError(f"invalid node ID {from_node!r}: must be 32 hex characters")
    for name, value in (("sealed_name", sealed_name), ("fallback_dht", fallback_dht)):
        if type(value) is not bool:
            raise ValueError(f"{name} must be true or false")
        if value and kind != "get":
            raise ValueError(f"only get jobs take {name}")


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


def check_job_id(text) -> str:
    """text if it is a job ID (8 hex characters), else ValueError."""
    if not isinstance(text, str) or not _JOB_ID.fullmatch(text):
        raise ValueError(f"invalid job ID {text!r}: must be 8 hex characters")
    return text


class JobsFeature(Feature):
    """The job manager: queue, threads, persistence, protection of resumable blocks. Thread-safe."""

    name = NAME

    def __init__(self, node):
        """Load saved jobs, queueing again those that hadn't finished and protecting their blocks."""
        from rnfs import statedb
        super().__init__(node)
        self._lock = threading.Lock()
        self._save_lock = threading.Lock()  # keeps saves in order without holding _lock while writing
        self._jobs: dict[str, Job] = {}  # in creation order
        self._threads: dict[str, threading.Thread] = {}
        self._started = False
        self._dirty = False  # jobs changed since the last save
        self._stop_event = threading.Event()
        # What the jobs table holds: id -> (position, dump()ed record); and the next new job's position (after
        # every one saved). Both used under _save_lock only
        self._saved: dict[str, tuple[int, str]] = {}
        self._position = 0
        self._db = statedb.connect(node.config.state_dir)
        try:
            self._load()
        except BaseException:
            self.close()
            raise

    @property
    def max_jobs(self) -> int:
        """Jobs running at once: max_jobs, by default 2, or 1 on slow nodes."""
        configured = getattr(self.node.config, "max_jobs", None)
        return configured if configured is not None else (1 if self.node.slow else 2)

    # Persistence

    def _load(self):
        """Read the jobs table, oldest first; a malformed row is logged and deleted."""
        from rnfs import statedb
        bad = []
        for job_id, position, text in self._db.query("SELECT id, position, record FROM jobs ORDER BY position"):
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
            self._saved[job.id] = (position, text)
            if job.state in ACTIVE:
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
        if bad:
            with self._db.transaction() as tx:
                tx.executemany("DELETE FROM jobs WHERE id = ?", bad)

    def save(self):
        """Save the jobs changed since the last save, and forget those gone, durably, if jobs changed or one runs
        (the node calls this periodically, keeping saved progress recent)."""
        from rnfs import statedb
        with self._save_lock:
            with self._lock:
                if not self._dirty and not any(job.state == RUNNING for job in self._jobs.values()):
                    return
                records = [(job_id, statedb.dump(job.to_record())) for job_id, job in self._jobs.items()]
                self._dirty = False
            saved, position, rows = {}, self._position, []
            for job_id, text in records:
                old = self._saved.get(job_id)
                if old is None:
                    old = (position, None)
                    position += 1
                if old[1] != text:
                    rows.append((job_id, old[0], text))
                saved[job_id] = (old[0], text)
            gone = [(job_id,) for job_id in self._saved if job_id not in saved]
            try:
                if rows or gone:
                    with self._db.transaction(durable=True) as tx:  # a queued job must survive a crash
                        tx.executemany("DELETE FROM jobs WHERE id = ?", gone)
                        tx.executemany("INSERT OR REPLACE INTO jobs (id, position, record) VALUES (?, ?, ?)", rows)
            except BaseException:
                with self._lock:
                    self._dirty = True
                raise
            self._saved, self._position = saved, position

    def _save_quietly(self):
        """save() after a change, logging rather than raising: a job's outcome mustn't be lost to a full disk."""
        with self._lock:
            self._dirty = True
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

    # Protection

    def _protect(self, job: Job):
        """Keep everything stored below the job's root while it is queued or running."""
        from rnfs.content import walker
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

    def stop(self):
        """Ask running jobs to stop (they are saved as queued, to resume), wait briefly, save."""
        with self._lock:
            self._started = False
            threads = list(self._threads.values())
        self._stop_event.set()
        deadline = time.monotonic() + STOP_WAIT
        for thread in threads:
            thread.join(max(0.0, deadline - time.monotonic()))
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
            for job in self._jobs.values():
                if running >= self.max_jobs:
                    break
                if job.state != QUEUED:
                    continue
                job.begin_run()
                thread = threading.Thread(target=self._run, args=(job,), name=f"rnfs-job-{job.id}", daemon=True)
                self._threads[job.id] = thread
                starting.append(thread)
                running += 1
        for thread in starting:
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
        except Exception as e:  # noqa: BLE001 - a job's failure mustn't take its thread's bookkeeping with it
            error = str(e) or type(e).__name__
            _log(f"job {job.id} ({job.kind} {job.target}) failed: {e!r}", LOG_ERROR)
        self._finish(job, state, error, result)

    def _execute(self, job: Job) -> dict:
        """The facade call the job stands for; its result."""
        node = self.node
        if job.kind == "get":
            result = self._get(job)
        elif job.kind == "pin":
            result = node.pin_add(job.target)
        else:
            result = node.export(job.target, job.output)
        job.blocks_total = self._dag_block_count(result)
        return result

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
        try:
            return self.node.get(job.target, path, from_node=from_node)
        except RnfsError as e:
            # Cancelled must stop the job; the content's size, the node's storage and its deny list don't depend
            # on the source
            if from_node is None or not job.fallback_dht or isinstance(e, (fetcher.Cancelled, TooLarge, StorageFull,
                                                                           Denied)):
                raise
            _log(f"job {job.id}: getting {job.target} from {job.from_node} failed ({e}); trying the DHT",
                 LOG_NOTICE)
        remove_partial(path)
        return self.node.get(job.target, path)

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
            self._threads.pop(job.id, None)
            self._cap_finished()
        if state in FINISHED:
            job.ended.set()
            _log(f"job {job.id} ({job.kind} {job.target}) {state}" + (f": {error}" if error else ""))
        if not self._schedule():  # starting the next saves this outcome with it
            self._save_quietly()

    # Operations (API)

    def submit(self, kind: str, target: str, output: str | None = None, from_node: str | None = None,
               sealed_name: bool = False, fallback_dht: bool = False) -> dict:
        """Queue a job; its dict. Raises ValueError for bad parameters or an output in use, Busy when full.

        For get: sealed_name writes a sealed file under its sealed name beside output; fallback_dht retries
        by DHT discovery if from_node fails.
        """
        from rnfs.errors import Busy
        contacts = getattr(self.node, "features", {}).get("contacts")
        if contacts is not None:
            # `alice/docs`: a name contact's nick becomes the CID its name points at now (rnfs.features.contacts)
            target = contacts.expand_target(target)
        check_job_params(kind, target, output, from_node, sealed_name, fallback_dht)
        if output is not None and os.path.lexists(output):
            raise ValueError(f"{output} already exists")
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
            job = Job(job_id, kind, target, output, from_node, sealed_name=sealed_name, fallback_dht=fallback_dht)
            job.stop_event = self._stop_event
            self._protect(job)
            self._jobs[job_id] = job
        _log(f"job {job_id} queued: {kind} {target}" + (f" to {output}" if output else ""))
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
        """Cancel a job: a queued one at once, a running one at its fetch's next step. Finished ones stay."""
        job = self._job(job_id)
        ended = False
        with self._lock:
            if job.state == QUEUED:
                job.state = CANCELLED
                job.finished = time.time()
                ended = True
            elif job.state == RUNNING:
                job.cancel_requested = True
        if ended:
            self._release(job)
            job.ended.set()
            with self._lock:
                self._cap_finished()
            self._save_quietly()
        return job.to_dict()

    def clear(self, job_id: str | None = None) -> int:
        """Forget finished jobs (or just job_id, which must be finished); how many were removed."""
        if job_id is not None and self._job(job_id).state not in FINISHED:
            raise ValueError(f"job {job_id} hasn't finished; cancel it first")
        with self._lock:
            removed = [jid for jid, job in self._jobs.items()
                       if job.state in FINISHED and (job_id is None or jid == job_id)]
            for jid in removed:
                del self._jobs[jid]
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
    """POST /api/jobs {"kind", "target", "output", "from", "sealed_name", "fallback_dht"}: queue a job; the job."""
    from rnfs import api
    body = handler.json_body()
    kind = body.get("kind")
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {', '.join(KINDS)}")
    target = api.parse_target(body.get("target"))
    output = api.parse_path(body.get("output")) if kind != "pin" else body.get("output")
    from_node = api.parse_hex_id(body["from"]).hex() if body.get("from") else None
    flags = {name: body.get(name, False) for name in ("sealed_name", "fallback_dht")}  # checked by submit
    handler.send_json(200, _feature(handler).submit(kind, target, output, from_node, **flags))


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
    if state == QUEUED:
        return "queued"
    if state == RUNNING:
        rate = job.get("rate")
        speed = f"{human_size(rate)}/s" if rate is not None else "-/s"
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
    """One `rnfs jobs` line: ID, state, kind, progress, target (and output)."""
    from rnfs.cli import printable
    where = f" -> {job['output']}" if job.get("output") else ""
    return printable(f"{job['id']}  {job['state']:<9} {job['kind']:<6} {format_progress(job)}  {job['target']}{where}")


def job_fields(job: dict) -> dict:
    """A job's fields for `rnfs jobs show`, readable."""
    fields = {"id": job["id"], "kind": job["kind"], "state": job["state"], "target": job["target"]}
    for name in ("output", "from", "sealed_name", "fallback_dht"):
        if job.get(name):
            fields[name] = job[name]
    fields["progress"] = format_progress(job)
    total = job.get("total_bytes")
    fields["bytes"] = f"{job.get('done_bytes') or 0} of {total if total is not None else '?'}"
    fields["blocks"] = job.get("blocks") if job.get("blocks_total") is None else f"{job['blocks']} of {job['blocks_total']}"
    fields["downloaded"] = human_size(job.get("downloaded_bytes") or 0)
    if job.get("rate") is not None:
        fields["rate"] = f"{human_size(job['rate'])}/s"
        fields["eta"] = human_duration(job.get("eta"))
    for name in ("created", "started", "finished"):
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
                line.show(format_progress(job))
    except KeyboardInterrupt:
        if line:
            line.clear()
        if cancel_on_interrupt:
            api.post("/api/jobs/cancel", id=job_id)
            print(f"rnfs: cancelled job {job_id} (with --background, a transfer keeps running after the command "
                  "ends)", file=sys.stderr)
        sys.exit(130)
    finally:
        if line:
            line.clear()


def has_jobs(api) -> bool:
    """True if the daemon runs the job manager (its status counts jobs); older daemons don't."""
    return "jobs_queued" in api.get("/api/status")


def run_transfer(args, fields: dict, fallback) -> dict | None:
    """Run a transfer as a job and follow it; its result, or None with --background (its ID is printed).

    fallback(api) does the transfer in one request instead, for a daemon without the job manager.
    """
    from rnfs import cli
    api = cli.client(args)
    if not has_jobs(api):
        if getattr(args, "background", False):
            cli.fail("this daemon can't run background jobs")
        return fallback(api)
    job = api.post("/api/jobs", **fields)
    if getattr(args, "background", False):
        print(job["id"])
        if sys.stderr.isatty():
            print(f"rnfs: started job {job['id']}; follow it with: rnfs jobs wait {job['id']}", file=sys.stderr)
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
        print(f"cancelling {job['id']}")
    elif job["state"] == CANCELLED:
        print(f"cancelled {job['id']}")
    else:
        print(f"{job['id']} already {job['state']}")


def run_clear(args):
    """Forget finished jobs."""
    from rnfs import cli
    removed = cli.client(args).post("/api/jobs/clear", id=_cli_id(args.id) if args.id else None)["removed"]
    print(f"removed {removed} finished job{'' if removed == 1 else 's'}")


def _cli_id(text: str) -> str:
    """A job ID from the command line, or exit with an error."""
    from rnfs import cli
    try:
        return check_job_id(text.strip().lower())
    except ValueError as e:
        cli.fail(str(e))
