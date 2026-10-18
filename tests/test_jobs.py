"""Tests for background transfer jobs (rnfs/features/jobs.py) on offline nodes, their API routes and CLI."""
import contextlib
import http.client
import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

import RNS

from rnfs import cli, statedb
from rnfs.api import ApiServer
from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.content.cid import RAW, Cid
from rnfs.content.storage import USER
from rnfs.features import jobs
from rnfs.node import STOP_TIMEOUT, RnfsNode
from tests.support import failing_commits, real, state_execute, state_rows, wait_for, write_legacy
from tests.test_api import FakeNode
from tests.test_names import FakeNetwork
from tests.test_node import A_ID, B_ID, NetClient

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the nodes' log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class GatedClient(NetClient):
    """A NetClient whose /blocks requests can be held at a gate, serving at most per_request blocks each."""

    def __init__(self):
        """Gate open, no limits."""
        super().__init__()
        self.gate = threading.Event()
        self.gate.set()
        self.per_request: int | None = None
        self.open_requests: int | None = None  # requests let through before the gate closes by itself
        self.requested: list[Cid] = []
        self.waiting = 0  # requests held at the gate now
        self._lock = threading.Lock()

    def blocks(self, node_id, cids, on_progress=None):
        """Record, maybe hold at the gate, then serve (cut to per_request)."""
        cids = list(cids)
        with self._lock:
            self.requested.extend(cids[:self.per_request] if self.per_request else cids)
            if self.open_requests is not None:
                if self.open_requests <= 0:
                    self.gate.clear()
                self.open_requests -= 1
            self.waiting += 1
        try:
            self.gate.wait(20)
        finally:
            with self._lock:
                self.waiting -= 1
        if self.per_request is None:
            return super().blocks(node_id, cids, on_progress)
        result = super().blocks(node_id, cids[:self.per_request], on_progress)
        result.unsent = list(cids[self.per_request:]) + list(result.unsent)
        return result


class JobsBase(unittest.TestCase):
    """Base: nodes A and B joined by a GatedClient, B's job manager started."""

    def setUp(self):
        """Nodes A and B joined by a GatedClient; B's job manager started."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = GatedClient()
        self.net.sender = B_ID
        self.addCleanup(self.net.gate.set)  # never leave request threads waiting
        self.a = self.make_node("a", A_ID)
        self.b = self.make_node("b", B_ID)
        self.b.routing.heard(A_ID)
        self.jobs = self.b.feature("jobs")
        self.jobs.start()

    def make_node(self, name: str, node_id: bytes, **settings) -> RnfsNode:
        """An offline node with its own directory."""
        settings.setdefault("max_jobs", 2)
        config = Config(rnfs_dir=os.path.join(self.tmp.name, name), slow="no", lookup_timeout=5, path_timeout=0.5,
                        **settings)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def share(self, data: bytes) -> str:
        """Add data on A and make A its provider."""
        cid = self.a.add_stream(io.BytesIO(data))["cid"]
        self.a.records.store(Cid.parse(cid).key, A_ID)
        return cid

    def out(self, name: str) -> str:
        """An output path in the temporary directory."""
        return os.path.join(self.tmp.name, name)

    def finished(self, job_id: str, timeout: float = 10.0) -> dict:
        """Wait for a job to finish; its dict."""
        self.assertTrue(wait_for(lambda: self.jobs.get(job_id)["state"] in jobs.FINISHED, timeout), self.jobs.get(job_id))
        return self.jobs.get(job_id)

    def saved(self, name: str = "b") -> list[dict]:
        """The job records node name's jobs table holds, oldest first."""
        rows = state_rows(os.path.join(self.tmp.name, name, "state"), "SELECT record FROM jobs ORDER BY position")
        return [statedb.load(record) for (record,) in rows]


class JobsTest(JobsBase):
    """B fetching from A through jobs."""

    # Lifecycle

    def test_get_job(self):
        data = os.urandom(3 * CHUNK_SIZE + 5)
        cid = self.share(data)
        job = self.jobs.submit("get", cid, self.out("file"))
        self.assertEqual(len(job["id"]), 8)
        self.assertIn(job["state"], ("queued", "running"))
        job = self.finished(job["id"])
        self.assertEqual(job["state"], "done", job)
        self.assertEqual(job["result"]["cid"], cid)
        self.assertEqual((job["done_bytes"], job["total_bytes"]), (len(data), len(data)))
        self.assertEqual(job["blocks_total"], 5)  # a file node and four chunks
        self.assertGreater(job["downloaded_bytes"], len(data))
        self.assertIsNotNone(job["finished"])
        with open(self.out("file"), "rb") as f:
            self.assertEqual(f.read(), data)
        self.assertEqual(self.jobs.status(), {"jobs_running": 0, "jobs_queued": 0})

    def test_pin_and_export_jobs(self):
        cid = self.share(b"pin me")
        job = self.finished(self.jobs.submit("pin", cid)["id"])
        self.assertEqual(job["state"], "done", job)
        self.assertIn(USER, self.b.storage.pins()[Cid.parse(cid)])
        job = self.finished(self.jobs.submit("export", cid, self.out("archive"))["id"])
        self.assertEqual(job["state"], "done", job)
        self.assertEqual(job["result"]["roots"], [cid])
        self.assertTrue(os.path.isfile(self.out("archive")))

    def test_failed_job(self):
        missing = str(Cid.for_block(RAW, b"nobody has this"))
        job = self.finished(self.jobs.submit("get", missing, self.out("missing"))["id"])
        self.assertEqual(job["state"], "failed")
        self.assertIn("no provider", job["error"])
        self.assertFalse(os.path.exists(self.out("missing")))

    def test_bad_submissions(self):
        cid = self.share(b"x")
        for args in (("copy", cid, self.out("x")), ("get", "not-a-cid", self.out("x")), ("get", cid, "relative"),
                     ("get", cid, None), ("pin", cid, self.out("x")), ("pin", cid, None, "aa" * 16),
                     ("get", cid, self.out("x"), "zz")):
            with self.assertRaises(ValueError, msg=args):
                self.jobs.submit(*args)
        with open(self.out("exists"), "wb"):
            pass
        with self.assertRaises(ValueError):
            self.jobs.submit("get", cid, self.out("exists"))

    def test_same_output_refused_while_active(self):
        cid = self.share(b"y")
        self.net.gate.clear()
        self.jobs.submit("get", cid, self.out("same"))
        with self.assertRaises(ValueError):
            self.jobs.submit("get", cid, self.out("same"))

    # Cancel and concurrency

    def test_cancel_running_job_stops_its_fetch(self):
        cid = self.share(os.urandom(2 * CHUNK_SIZE))
        self.net.gate.clear()
        job = self.jobs.submit("get", cid, self.out("cancelled"))
        self.assertTrue(wait_for(lambda: self.net.waiting > 0))
        self.assertEqual(self.jobs.get(job["id"])["state"], "running")
        started = time.monotonic()
        self.assertEqual(self.jobs.cancel(job["id"])["state"], "running")  # it stops at the fetch's next step
        job = self.finished(job["id"], timeout=5)
        self.assertEqual(job["state"], "cancelled")
        self.assertLess(time.monotonic() - started, 3)  # within a poll, though the request still hangs
        self.assertFalse(os.path.exists(self.out("cancelled")))
        self.assertEqual(self.b.storage._sessions, [])  # its protection is released

    def test_submit_saves_once_whether_the_job_starts_or_waits(self):
        """A submitted job is saved once: as it starts, or as queued when it can't start yet."""
        self.b.config.max_jobs = 1
        first, second = self.share(b"first"), self.share(b"second")
        self.net.gate.clear()
        with mock.patch.object(self.jobs._db, "transaction", wraps=self.jobs._db.transaction) as write:
            started = self.jobs.submit("get", first, self.out("first"))["id"]
            self.assertEqual(write.call_count, 1)  # saved as it started, not also before
            queued = self.jobs.submit("get", second, self.out("second"))["id"]
            self.assertEqual(write.call_count, 2)  # nothing started, so saved as queued
        saved = {job["id"]: job["state"] for job in self.saved()}
        self.assertEqual(saved, {started: "running", queued: "queued"})
        self.net.gate.set()
        self.assertEqual(self.finished(started)["state"], "done")
        self.assertEqual(self.finished(queued)["state"], "done")

    def test_a_stalled_running_job_isnt_saved_again(self):
        """The periodic save writes a running job only when its record changed: a stalled one costs no commit."""
        cid = self.share(b"stalled")
        self.net.gate.clear()
        job_id = self.jobs.submit("get", cid, self.out("stalled"))["id"]
        self.assertTrue(wait_for(lambda: self.jobs.get(job_id)["state"] == "running"))
        self.jobs.save()
        with mock.patch.object(self.jobs._db, "transaction", wraps=self.jobs._db.transaction) as write:
            self.jobs.save()
            self.jobs.save()
        self.assertEqual(write.call_count, 0)
        self.net.gate.set()
        self.assertEqual(self.finished(job_id)["state"], "done")
        self.assertTrue(wait_for(lambda: [r["state"] for r in self.saved()] == ["done"]))  # its outcome saved

    def test_cancel_queued_job_and_concurrency_limit(self):
        self.b.config.max_jobs = 1
        cids = [self.share(f"file {i}".encode()) for i in range(3)]
        self.net.gate.clear()
        submitted = [self.jobs.submit("get", cid, self.out(f"q{i}"))["id"] for i, cid in enumerate(cids)]
        self.assertTrue(wait_for(lambda: self.net.waiting == 1))
        time.sleep(0.2)  # time for a second job to start, were the limit not kept
        self.assertEqual(self.jobs.status(), {"jobs_running": 1, "jobs_queued": 2})
        self.assertEqual(self.net.waiting, 1)  # only one job fetches
        self.assertEqual(self.jobs.cancel(submitted[2])["state"], "cancelled")
        self.net.gate.set()
        self.assertEqual(self.finished(submitted[0])["state"], "done")
        self.assertEqual(self.finished(submitted[1])["state"], "done")
        self.assertFalse(os.path.exists(self.out("q2")))
        self.assertEqual(self.jobs.cancel(submitted[0])["state"], "done")  # finished jobs stay as they are

    def test_finished_jobs_are_capped_and_cleared(self):
        self.b.config.max_finished_jobs = 3
        ids = []
        with mock.patch.object(jobs, "FINISHED_GRACE", 0):
            for i in range(5):
                cid = self.b.add_stream(io.BytesIO(f"local {i}".encode()))["cid"]
                ids.append(self.finished(self.jobs.submit("pin", cid)["id"])["id"])
        self.assertEqual([job["id"] for job in self.jobs.list()], ids[2:])
        with self.assertRaises(ValueError):
            self.jobs.submit("pin", "x")
        self.assertEqual(self.jobs.clear(ids[4]), 1)
        self.assertEqual(self.jobs.clear(), 2)
        self.assertEqual(self.jobs.list(), [])
        self.assertEqual(self.saved(), [])  # forgotten jobs are deleted

    def test_recently_finished_jobs_outlive_the_cap(self):
        # A CLI following a fast job asks for it once it ends: a small cap mustn't have forgotten it already
        self.b.config.max_finished_jobs = 1
        ids = []
        for i in range(3):
            cid = self.b.add_stream(io.BytesIO(f"fast {i}".encode()))["cid"]
            ids.append(self.jobs.submit("pin", cid)["id"])
        for job_id in ids:
            self.assertEqual(self.finished(job_id)["state"], "done")
        with mock.patch.object(time, "time", return_value=time.time() + jobs.FINISHED_GRACE + 1):
            with self.jobs._lock:
                self.jobs._cap_finished()
        self.assertEqual([job["id"] for job in self.jobs.list()], ids[2:])

    # Get jobs: partial outputs, sealed names, fallback to the DHT, warnings

    def test_partial_output_never_left_behind(self):
        cid = self.share(b"partial")

        def failing_get(target, output, **kwargs):
            """Write half a directory, then fail as a stop or a full disk would."""
            os.makedirs(os.path.join(output, "half"))
            raise OSError("disk full")

        with mock.patch.object(self.b, "get", failing_get):
            job = self.finished(self.jobs.submit("get", cid, self.out("p"))["id"])
        self.assertEqual((job["state"], job["error"]), ("failed", "disk full"))
        self.assertEqual(sorted(os.listdir(self.tmp.name)), ["a", "b"])  # neither the output nor its partial

    def test_resume_removes_what_an_interrupted_run_left(self):
        # A directory write-out cut off by a crash leaves its partial path; the resumed run starts it afresh
        a_dir = os.path.join(self.tmp.name, "src")
        os.makedirs(os.path.join(a_dir, "sub"))
        with open(os.path.join(a_dir, "sub", "f.txt"), "wb") as f:
            f.write(b"whole")
        cid = self.a.add_path(a_dir)["cid"]
        self.a.records.store(Cid.parse(cid).key, A_ID)
        self.b.stop()
        output = self.out("dir-out")
        # As rnfs 0.1.4 left it (imported at the restart), a record without the fields it can do without
        write_legacy(os.path.join(self.tmp.name, "b", "state"), "jobs.json",
                     {"jobs": [{"id": "0123abcd", "kind": "get", "target": cid, "output": output,
                                "state": "running", "created": 1.0}]})
        partial = os.path.join(self.tmp.name, ".rnfs-job-0123abcd.partial")
        os.makedirs(os.path.join(partial, "sub"))
        with open(os.path.join(partial, "sub", "f.txt"), "wb") as f:
            f.write(b"wh")
        b2 = self.make_node("b", B_ID)
        b2.routing.heard(A_ID)
        manager = b2.feature("jobs")
        manager.start()
        self.assertTrue(wait_for(lambda: manager.get("0123abcd")["state"] in jobs.FINISHED))
        self.assertEqual(manager.get("0123abcd")["state"], "done", manager.get("0123abcd"))
        with open(os.path.join(output, "sub", "f.txt"), "rb") as f:
            self.assertEqual(f.read(), b"whole")
        self.assertFalse(os.path.lexists(partial))

    def sealed_get(self, name):
        """A stand-in for node.get writing a sealed file this node opened, whose sealed name is name."""
        def get(target, output, **kwargs):
            """Write the plaintext to output, as node_content.write_sealed does."""
            with open(output, "xb") as f:
                f.write(b"plaintext")
            return {"cid": target, "warnings": [], "sealed": True, "name": name}
        return get

    def test_sealed_name_chosen_by_the_job(self):
        cid = self.share(b"sealed stand-in")
        for name, expected in (("report.pdf", "report.pdf"), (".bash_login", cid), ("\x1b]0;x\x07", cid),
                               (None, cid)):
            with self.subTest(name=name), mock.patch.object(self.b, "get", self.sealed_get(name)):
                job = self.finished(self.jobs.submit("get", cid, self.out(cid), sealed_name=True)["id"])
                self.assertEqual(job["state"], "done", job)
                self.assertEqual((job["output"], job["result"]["output"]), (self.out(expected),) * 2)
                with open(self.out(expected), "rb") as f:
                    self.assertEqual(f.read(), b"plaintext")
                os.remove(self.out(expected))
        # Without sealed_name the output is what was asked for; an existing sealed name isn't overwritten
        with mock.patch.object(self.b, "get", self.sealed_get("report.pdf")):
            job = self.finished(self.jobs.submit("get", cid, self.out("asked"))["id"])
            self.assertEqual(job["output"], self.out("asked"))
            with open(self.out("report.pdf"), "wb"):
                pass
            job = self.finished(self.jobs.submit("get", cid, self.out(cid), sealed_name=True)["id"])
        self.assertEqual(job["state"], "failed")
        self.assertIn("already exists", job["error"])
        self.assertEqual(os.path.getsize(self.out("report.pdf")), 0)
        self.assertFalse(any(n.startswith(".rnfs-job-") for n in os.listdir(self.tmp.name)))

    def test_fallback_dht(self):
        cid = self.share(b"only A has it")
        absent = "cc" * 16  # a from node that answers nothing
        job = self.finished(self.jobs.submit("get", cid, self.out("strict"), absent)["id"])
        self.assertEqual(job["state"], "failed", job)
        job = self.finished(self.jobs.submit("get", cid, self.out("fallback"), absent, fallback_dht=True)["id"])
        self.assertEqual(job["state"], "done", job)
        self.assertTrue(job["fallback_dht"])
        with open(self.out("fallback"), "rb") as f:
            self.assertEqual(f.read(), b"only A has it")
        for bad in ({"fallback_dht": 1}, {"sealed_name": "yes"}):
            with self.assertRaises(ValueError):
                self.jobs.submit("get", cid, self.out("bad"), **bad)
        with self.assertRaises(ValueError):
            self.jobs.submit("pin", cid, fallback_dht=True)

    def test_write_out_warnings_are_capped(self):
        from rnfs import log
        from rnfs.content import walker
        a_dir = os.path.join(self.tmp.name, "many")
        os.makedirs(a_dir)
        with open(os.path.join(a_dir, "f"), "wb") as f:
            f.write(b"x")
        cid = self.a.add_path(a_dir)["cid"]
        self.a.records.store(Cid.parse(cid).key, A_ID)
        real = walker.write_out

        def noisy(*args, **kwargs):
            """Write out, reporting 5000 skipped entries."""
            return real(*args, **kwargs) + [f"skipped entry {i}" for i in range(5000)]

        with mock.patch.object(walker, "write_out", noisy):
            job = self.finished(self.jobs.submit("get", cid, self.out("noisy"))["id"])
        self.assertEqual(job["state"], "done", job)
        warnings = job["result"]["warnings"]
        self.assertEqual(len(warnings), log.MAX_WARNINGS + 1)
        self.assertEqual(warnings[-1], f"... and {5000 - log.MAX_WARNINGS} more warnings")
        self.assertEqual(job["result"]["warnings_total"], 5000)
        self.jobs.save()
        (record,) = state_rows(self.b.config.state_dir, "SELECT record FROM jobs WHERE id = ?", (job["id"],))[0]
        self.assertLess(len(record), 20_000)

    # Persistence and resume

    def test_resume_after_restart_keeps_fetched_blocks(self):
        data = os.urandom(6 * CHUNK_SIZE)
        cid = self.share(data)
        self.net.per_request = 1
        self.net.open_requests = 3  # the file node and two chunks arrive, then the gate closes
        job = self.jobs.submit("get", cid, self.out("resumed"))
        self.assertTrue(wait_for(lambda: self.net.waiting > 0 and not self.net.gate.is_set()))
        self.assertTrue(wait_for(lambda: self.jobs.get(job["id"])["done_bytes"] == 2 * CHUNK_SIZE))
        self.b.stop()  # the running job is interrupted and saved as queued
        self.assertEqual([(r["id"], r["state"]) for r in self.saved()], [(job["id"], "queued")])
        fetched = {c for c in self.net.requested[:3]}
        self.net.gate.set()

        b2 = self.make_node("b", B_ID)
        b2.routing.heard(A_ID)  # the routing table isn't saved
        manager = b2.feature("jobs")
        self.assertEqual(manager.get(job["id"])["state"], "queued")
        b2.gc()  # its blocks are protected while it waits, though nothing pins them
        self.assertTrue(all(b2.storage.has(c) for c in fetched))
        self.net.requested.clear()
        self.net.per_request = self.net.open_requests = None
        manager.start()
        self.assertTrue(wait_for(lambda: manager.get(job["id"])["state"] in jobs.FINISHED))
        resumed = manager.get(job["id"])
        self.assertEqual(resumed["state"], "done", resumed)
        self.assertEqual(resumed["runs"], 2)
        self.assertFalse(fetched & set(self.net.requested))  # nothing fetched before is fetched again
        self.assertEqual(len(self.net.requested), 4)
        with open(self.out("resumed"), "rb") as f:
            self.assertEqual(f.read(), data)

    def test_finished_jobs_survive_restart(self):
        """Finished jobs are loaded again after a restart, oldest first."""
        cid = self.b.add_stream(io.BytesIO(b"kept"))["cid"]
        job_id = self.finished(self.jobs.submit("pin", cid)["id"])["id"]
        other = self.finished(self.jobs.submit("pin", cid)["id"])["id"]
        self.b.stop()
        b2 = self.make_node("b", B_ID)
        job = b2.feature("jobs").get(job_id)
        self.assertEqual((job["state"], job["result"]), ("done", {"cid": cid}))
        self.assertEqual([j["id"] for j in b2.feature("jobs").list()], [job_id, other])

    def test_damaged_jobs_file_records_skipped(self):
        """jobs.json's malformed records are skipped when it is imported, and a repeated id keeps the first."""
        cid = str(Cid.for_block(RAW, b"c"))
        good = {"id": "0123abcd", "kind": "pin", "target": cid, "state": "running", "created": 1.0}
        write_legacy(os.path.join(self.tmp.name, "c", "state"), "jobs.json",
                     {"jobs": [good, {"id": "nothex!!", "kind": "pin", "target": cid, "state": "queued"},
                               {**good, "id": "0123abce", "kind": "get", "output": "relative"}, "junk",
                               {**good, "state": "done"}]})
        c = self.make_node("c", bytes([0xC] * 16))
        self.assertEqual([(j["id"], j["state"]) for j in c.feature("jobs").list()], [("0123abcd", "queued")])

    def test_jobs_file_of_the_wrong_shape_is_moved_aside(self):
        """A jobs.json of the wrong shape is moved aside when imported, and the node starts without it."""
        path = write_legacy(os.path.join(self.tmp.name, "c", "state"), "jobs.json", {"jobs": {"not": "a list"}})
        c = self.make_node("c", bytes([0xC] * 16))
        self.assertEqual(c.feature("jobs").list(), [])
        self.assertTrue(os.path.exists(path + ".damaged"))

    def test_damaged_rows_are_deleted(self):
        """Rows whose record isn't a valid job (or another job's, or with no position) are skipped at load and
        deleted; the others load in order."""
        cid = str(Cid.for_block(RAW, b"c"))
        c = self.make_node("c", bytes([0xC] * 16))
        state = c.config.state_dir
        c.stop()
        good = {"id": "0123abcd", "kind": "pin", "target": cid, "state": "done", "created": 1.0}
        for job_id, position, record in (("0123abce", 1, "not json"), ("0123abcf", 2, statedb.dump(good)),
                                         ("0123abd0", 3, statedb.dump({**good, "id": "0123abd0", "kind": "x"})),
                                         ("0123abd1", "x", statedb.dump({**good, "id": "0123abd1"})),
                                         ("0123abcd", 9, statedb.dump(good)),
                                         ("0123abd2", 5, statedb.dump({**good, "id": "0123abd2"}))):
            state_execute(state, "INSERT INTO jobs (id, position, record) VALUES (?, ?, ?)", (job_id, position, record))
        c = self.make_node("c", bytes([0xC] * 16))
        self.assertEqual([j["id"] for j in c.feature("jobs").list()], ["0123abd2", "0123abcd"])
        self.assertEqual([r["id"] for r in self.saved("c")], ["0123abd2", "0123abcd"])

    def test_a_save_writes_only_the_jobs_changed(self):
        """A save rewrites only the jobs whose record changed since the last; new ones go after the others."""
        first = self.finished(self.jobs.submit("pin", self.b.add_stream(io.BytesIO(b"1"))["cid"])["id"])["id"]
        self.assertTrue(wait_for(lambda: [r["state"] for r in self.saved()] == ["done"]))  # its outcome saved
        state_execute(self.b.config.state_dir, "UPDATE jobs SET record = ? WHERE id = ?",
                      (statedb.dump({**self.saved()[0], "error": "planted"}), first))
        second = self.finished(self.jobs.submit("pin", self.b.add_stream(io.BytesIO(b"2"))["cid"])["id"])["id"]
        self.assertTrue(wait_for(lambda: [r["state"] for r in self.saved()] == ["done", "done"]))
        self.assertEqual([(r["id"], r["error"]) for r in self.saved()], [(first, "planted"), (second, None)])

    def test_failed_save_leaves_the_job_to_the_next(self):
        """A job whose save failed (logged, not raised) is still saved by the next save."""
        self.b.config.max_jobs = 0  # stays queued
        cid = self.b.add_stream(io.BytesIO(b"later"))["cid"]
        with failing_commits():
            job_id = self.jobs.submit("pin", cid)["id"]
        self.assertEqual(self.saved(), [])
        self.assertEqual(self.jobs._store.pending, {job_id})  # left for the next save
        self.jobs.save()
        self.assertEqual([(r["id"], r["state"]) for r in self.saved()], [(job_id, "queued")])

    def test_huge_numbers_in_a_record_are_ignored(self):
        cid = str(Cid.for_block(RAW, b"c"))
        record = {"id": "0123abcd", "kind": "pin", "target": cid, "state": "done", "created": 10**400,
                  "started": 10**400, "finished": float("nan")}
        job = jobs.Job.from_record(record)  # 10**400 is valid JSON, but too large for a float
        self.assertEqual((job.started, job.finished), (None, None))
        self.assertLess(job.created, 10**12)

    def test_stop_never_hangs(self):
        cid = self.share(os.urandom(CHUNK_SIZE))
        self.net.gate.clear()
        self.jobs.submit("get", cid, self.out("hang"))
        self.assertTrue(wait_for(lambda: self.net.waiting > 0))
        started = time.monotonic()
        self.b.stop()
        # Its fetch notices the stop at its next step, well within the node's stop budget
        self.assertLess(time.monotonic() - started, STOP_TIMEOUT - 1)


class ApiBase(JobsBase):
    """Base: B behind an ApiServer with the token "secret"."""

    def setUp(self):
        """B's API."""
        super().setUp()
        self.server = ApiServer(self.b, 0, token="secret")
        self.server.start()
        self.addCleanup(self.server.stop)

    def call(self, method: str, path: str, body=None, token: str | None = "secret", server=None):
        """(status, decoded JSON) of one request."""
        server = server or self.server
        headers = {"Host": f"127.0.0.1:{server.port}"}
        if token is not None:
            headers["X-Rnfs-Api"] = token
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        conn = http.client.HTTPConnection("127.0.0.1", server.port, timeout=20)
        conn.request(method, path, body=data, headers=headers)
        response = conn.getresponse()
        result = json.loads(response.read())
        conn.close()
        return response.status, result


class ApiTest(ApiBase):
    """The /api/jobs routes, with the token, on an offline node."""

    def test_routes(self):
        cid = self.share(b"through the API")
        self.assertEqual(self.call("GET", "/api/jobs", token=None)[0], 403)
        self.assertEqual(self.call("POST", "/api/jobs", {"kind": "get", "target": cid}, token="wrong")[0], 403)
        for body in ({"kind": "nope", "target": cid}, {"kind": "get", "target": cid, "output": "rel"},
                     {"kind": "get", "target": "bad", "output": self.out("x")},
                     {"kind": "get", "target": cid, "output": self.out("x"), "from": "12"},
                     {"kind": "pin", "target": cid, "output": self.out("x")},
                     {"kind": "export", "target": cid, "output": self.out("x"), "from": "aa" * 16}):
            self.assertEqual(self.call("POST", "/api/jobs", body)[0], 400, body)
        status, job = self.call("POST", "/api/jobs", {"kind": "get", "target": cid, "output": self.out("api"),
                                                      "from": A_ID.hex()})
        self.assertEqual(status, 200, job)
        self.assertEqual(job["from"], A_ID.hex())
        status, job = self.call("GET", f"/api/jobs/get?id={job['id']}&wait=10")
        self.assertEqual((status, job["state"]), (200, "done"), job)
        self.assertEqual(self.call("GET", "/api/jobs")[1]["jobs"][0]["id"], job["id"])
        self.assertEqual(self.call("GET", "/api/jobs/get?id=xyz")[0], 400)
        self.assertEqual(self.call("GET", "/api/jobs/get?id=00000000")[0], 404)
        self.assertEqual(self.call("GET", f"/api/jobs/get?id={job['id']}&wait=-1")[0], 400)
        self.assertEqual(self.call("POST", "/api/jobs/cancel", {"id": job["id"]})[1]["state"], "done")
        self.assertEqual(self.call("POST", "/api/jobs/cancel", {})[0], 400)
        self.assertEqual(self.call("POST", "/api/jobs/clear", {"id": "bad"})[0], 400)
        self.assertEqual(self.call("POST", "/api/jobs/clear", {})[1], {"removed": 1})
        self.assertEqual(self.call("GET", "/api/status")[1]["jobs_queued"], 0)

    def test_daemon_without_job_manager(self):
        server = ApiServer(FakeNode(), 0, token="secret")
        server.start()
        self.addCleanup(server.stop)
        status, body = self.call("GET", "/api/jobs", server=server)
        self.assertEqual(status, 404)
        self.assertIn("job manager", body["error"])


class CliTest(ApiBase):
    """The CLI against B's API: get --background, jobs list/show/wait/cancel/clear, foreground get."""

    def setUp(self):
        """B's API, with its token where the CLI looks for it."""
        super().setUp()
        self.cli_dir = os.path.join(self.tmp.name, "cli")
        os.makedirs(self.cli_dir)
        cli.write_token(cli.token_path(self.cli_dir), self.server.token)

    def run_cli(self, *argv) -> tuple[int, str, str]:
        """(exit code, stdout, stderr) of cli.main(argv)."""
        stdout, stderr = io.StringIO(), io.StringIO()
        code = 0
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            try:
                cli.main([*argv, "--api-port", str(self.server.port), "--dir", self.cli_dir])
            except SystemExit as e:
                if isinstance(e.code, str):
                    print(e.code, file=sys.stderr)
                    code = 1
                else:
                    code = e.code or 0
        return code, stdout.getvalue(), stderr.getvalue()

    def test_background_get_and_jobs_commands(self):
        cid = self.share(os.urandom(CHUNK_SIZE + 1))
        self.net.gate.clear()
        code, out, err = self.run_cli("get", cid, "-o", self.out("bg"), "--background")
        self.assertEqual(code, 0, err)
        job_id = out.strip()
        self.assertTrue(jobs._JOB_ID.fullmatch(job_id), out)
        code, out, _ = self.run_cli("jobs")
        self.assertIn(job_id, out)
        self.assertIn("get", out)
        code, out, _ = self.run_cli("jobs", "show", job_id)
        self.assertIn(f"id: {job_id}", out)
        self.assertIn(f"output: {self.out('bg')}", out)
        self.net.gate.set()
        code, out, err = self.run_cli("jobs", "wait", job_id)
        self.assertEqual((code, out.strip()), (0, self.out("bg")), err)
        self.assertEqual(self.run_cli("jobs", "clear")[1].strip(), "removed 1 finished job")
        self.assertEqual(self.run_cli("jobs")[1].strip(), "no jobs")
        self.assertEqual(self.run_cli("jobs", "show", "nothex")[0], 1)
        self.assertEqual(self.run_cli("jobs", "wait", "00000000")[0], 1)

    def test_foreground_commands_run_as_jobs(self):
        cid = self.share(b"foreground")
        code, out, err = self.run_cli("get", cid, "-o", self.out("fg"))
        self.assertEqual((code, out.strip()), (0, self.out("fg")), err)
        self.assertEqual(self.jobs.list()[0]["kind"], "get")
        self.assertEqual(self.run_cli("pin", "add", cid)[1].strip(), cid)
        code, out, err = self.run_cli("export", cid, "-o", self.out("fg.rnfs"))
        self.assertIn("exported", out)
        self.assertEqual([job["kind"] for job in self.jobs.list()], ["get", "pin", "export"])
        missing = str(Cid.for_block(RAW, b"missing"))
        code, _, err = self.run_cli("get", missing, "-o", self.out("nothing"))
        self.assertEqual(code, 1)
        self.assertIn("no provider", err)

    def test_get_without_output_takes_a_safe_sealed_name(self):
        cid = self.share(b"sealed stand-in")
        with contextlib.chdir(self.tmp.name):
            with mock.patch.object(self.b, "get", JobsTest.sealed_get(self, "\x1b[31mevil")):
                code, out, err = self.run_cli("get", cid)
            self.assertEqual((code, real(out.strip())), (0, real(os.path.join(self.tmp.name, cid))), err)
            os.remove(cid)
            with mock.patch.object(self.b, "get", JobsTest.sealed_get(self, "report.pdf")):
                code, out, err = self.run_cli("get", cid)
        self.assertEqual((code, real(out.strip())), (0, real(os.path.join(self.tmp.name, "report.pdf"))), err)
        self.assertTrue(self.jobs.list()[-1]["sealed_name"])

    def test_background_get_from_asks_nothing_first(self):
        # The job picks the sealed name: no /api/info (a DHT lookup ignoring --from, blocking --background)
        cid = self.share(b"from A")
        self.net.gate.clear()
        with contextlib.chdir(self.tmp.name), mock.patch.object(self.b, "info", side_effect=AssertionError("info")):
            code, out, err = self.run_cli("get", cid, "--from", A_ID.hex(), "--background")
        self.assertEqual(code, 0, err)
        self.net.gate.set()
        self.assertEqual(self.finished(out.strip())["state"], "done")

    def test_wait_fails_for_cancelled_job(self):
        cid = self.share(b"to cancel")
        self.net.gate.clear()
        job_id = self.run_cli("pin", "add", cid, "--background")[1].strip()
        self.assertTrue(wait_for(lambda: self.net.waiting > 0))
        self.assertIn("cancelling", self.run_cli("jobs", "cancel", job_id)[1])
        code, _, err = self.run_cli("jobs", "wait", job_id)
        self.assertEqual(code, 1)
        self.assertIn("cancelled", err)

    def test_ctrl_c_in_foreground_cancels(self):
        cid = self.share(b"interrupted")
        self.net.gate.clear()
        real_follow = jobs.follow
        calls = []

        def interrupted_get(api, job_id, cancel_on_interrupt):
            """Interrupt the first poll, as Ctrl+C would."""
            calls.append(job_id)
            with mock.patch.object(api, "get", side_effect=KeyboardInterrupt):
                return real_follow(api, job_id, cancel_on_interrupt)

        with mock.patch.object(jobs, "follow", interrupted_get):
            code, _, err = self.run_cli("get", cid, "-o", self.out("int"))
        self.assertEqual(code, 130)
        self.assertIn("cancelled job", err)
        self.assertIn("--background", err)
        self.assertTrue(wait_for(lambda: self.jobs.get(calls[0])["state"] == "cancelled"))


class ParsingAndFormattingTest(unittest.TestCase):
    """CLI parsing and the pure formatting functions."""

    def test_parsing(self):
        parser = cli.build_parser()
        self.assertIs(parser.parse_args(["jobs"]).run, jobs.run_list)
        self.assertIs(parser.parse_args(["jobs", "list"]).run, jobs.run_list)
        args = parser.parse_args(["jobs", "show", "0123abcd", "--dir", "/x"])
        self.assertEqual((args.run, args.id, args.dir), (jobs.run_show, "0123abcd", "/x"))
        for name, run in (("wait", jobs.run_wait), ("cancel", jobs.run_cancel)):
            self.assertIs(parser.parse_args(["jobs", name, "0123abcd"]).run, run)
        self.assertIsNone(parser.parse_args(["jobs", "clear"]).id)
        cid = str(Cid.for_block(RAW, b"p"))
        self.assertTrue(parser.parse_args(["get", cid, "--background"]).background)
        self.assertFalse(parser.parse_args(["get", cid]).background)
        self.assertTrue(parser.parse_args(["pin", "add", cid, "--background"]).background)
        self.assertTrue(parser.parse_args(["export", cid, "-o", "a", "--background"]).background)

    def test_sizes_and_durations(self):
        self.assertEqual([jobs.human_size(n) for n in (0, 999, 1000, 1500, 12_345, 123_456, 3_200_000, None)],
                         ["0 B", "999 B", "1.00 KB", "1.50 KB", "12.3 KB", "123 KB", "3.20 MB", "?"])
        self.assertEqual([jobs.human_duration(s) for s in (0, 42.4, 303, 7620, 3 * 86400 + 4 * 3600, None)],
                         ["0s", "42s", "5m03s", "2h07m", "3d04h", "?"])
        self.assertEqual(jobs.progress_bar(0.5, 10), "[#####-----]")
        self.assertEqual(jobs.progress_bar(2, 4), "[####]")

    def test_format_progress(self):
        self.assertEqual(jobs.format_progress({"state": "queued"}), "queued")
        self.assertEqual(jobs.format_progress({"state": "running", "total_bytes": None, "blocks": 1, "rate": None}),
                         "finding the target: 1 block, -/s")
        line = jobs.format_progress({"state": "running", "total_bytes": 1_000_000, "done_bytes": 450_000,
                                     "rate": 1200.0, "eta": 458.0}, width=10)
        self.assertEqual(line, "[####------]  45%  450 KB of 1.00 MB  1.20 KB/s  ETA 7m38s")
        self.assertEqual(jobs.format_progress({"state": "done", "total_bytes": 2000}), "done, 2.00 KB")
        self.assertEqual(jobs.format_progress({"state": "failed", "error": "no provider"}), "failed: no provider")
        self.assertEqual(jobs.format_progress({"state": "cancelled"}), "cancelled")
        self.assertEqual(jobs.format_stream_progress(500, 1000, 2.0),
                         "[##########----------]  50%  500 B of 1.00 KB  250 B/s  ETA 2s")
        self.assertEqual(jobs.format_stream_progress(500, None, 0), "500 B  -/s")
        job = {"id": "0123abcd", "state": "running", "kind": "get", "target": "cid/evil\x1b[2J", "output": "/o",
               "total_bytes": None, "blocks": 0, "rate": None}
        self.assertNotIn("\x1b", jobs.format_job(job))
        self.assertIn("-> /o", jobs.format_job(job))

    def test_progress_line(self):
        stream = io.StringIO()
        line = jobs.ProgressLine(stream)
        line.show("long text here")
        line.show("short")
        line.clear()
        self.assertEqual(stream.getvalue(), "\rlong text here" + "\rshort" + " " * 9 + "\r" + " " * 5 + "\r")

    def test_rate_is_smoothed_and_decays(self):
        job = jobs.Job("0123abcd", "get", "x", "/o")
        self.assertIsNone(job.rate())
        job.begin_run()
        start = job._samples[0][0]
        job.downloaded_bytes = 10_000
        job._samples.append((start + 10, 10_000))
        self.assertAlmostEqual(job.rate(start + 10), 1000.0)
        self.assertAlmostEqual(job.rate(start + 20), 500.0)  # nothing arrived since: the rate falls
        # Samples older than the window are dropped, keeping one at its start as the base
        job._samples.append((start + jobs.RATE_WINDOW + 50, 10_000))
        self.assertAlmostEqual(job.rate(start + jobs.RATE_WINDOW + 60), 0.0)

    def test_rate_samples_pruned_without_polling(self):
        """S1: a running job nobody polls keeps only the window's samples (plus the base), and its rate is
        the one an unpruned job would give."""
        clock = [1000.0]
        with mock.patch.object(jobs.time, "monotonic", lambda: clock[0]):
            job = jobs.Job("0123abcd", "get", "x", "/o")
            job.begin_run()
            seconds = 3 * int(jobs.RATE_WINDOW)
            for n in range(seconds):
                clock[0] += 1.0
                job.block(Cid.for_block(RAW, n.to_bytes(4, "big")), 100 if n < seconds - 60 else 400, True)
            # One a second over the window, plus the base sample at its start
            self.assertEqual(len(job._samples), int(jobs.RATE_WINDOW / jobs.SAMPLE_INTERVAL) + 1)
            self.assertLessEqual(job._samples[0][0], clock[0] - jobs.RATE_WINDOW)
            self.assertGreater(job._samples[1][0], clock[0] - jobs.RATE_WINDOW)
            # The last RATE_WINDOW seconds: 240 at 100 B/s, 60 at 400 B/s
            self.assertAlmostEqual(job.rate(), (240 * 100 + 60 * 400) / jobs.RATE_WINDOW)
            self.assertEqual(job.downloaded_bytes, (seconds - 60) * 100 + 60 * 400)
            self.assertEqual(job.blocks, seconds)

    def test_settings_validated(self):
        for settings in ({"max_jobs": 0}, {"max_jobs": True}, {"max_jobs": 1.5}, {"max_finished_jobs": -1},
                         {"max_finished_jobs": 0}, {"max_finished_jobs": True}):
            with self.assertRaises(ValueError, msg=settings):
                Config(**settings).validate()
        Config(max_jobs=1, max_finished_jobs=1).validate()

    def test_print_result_escapes_remote_strings(self):
        stdout = io.StringIO()
        job = {"kind": "get", "output": "/x/\x1b]0;t\x07", "result": {"warnings": ["bad \x1b[2J"]}}
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            jobs.print_result(job)
            jobs.print_result({"kind": "export", "output": "/o\x07", "result": {"blocks": 1, "roots": ["r"]}})
        self.assertNotIn("\x1b", stdout.getvalue() + stderr.getvalue())
        self.assertNotIn("\x07", stdout.getvalue())
        self.assertIn("\\x1b]0;t\\x07", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
