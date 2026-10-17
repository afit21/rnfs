"""Tests for the public API (rnfs.Node and rnfs.Client: rnfs/interface.py, objects.py, embedded.py, remote.py).

The interface tests run twice: on an embedded node (an offline RnfsNode behind rnfs.Node's backend) and on a Client
of an ApiServer serving one, so both classes are shown to behave alike. Node A, offline too, provides content.
"""
import array
import contextlib
import importlib.util
import io
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import warnings
from unittest import mock

import RNS

import rnfs
from rnfs import api, cli, embedded, fsutil, interface, objects, remote
from rnfs.api import ApiHandler, ApiServer, NameWatches
from rnfs.config import Config
from rnfs.content.cid import RAW, Cid
from rnfs.names import encode_record
from rnfs.node import DirInUse, RnfsNode, lock_rnfs_dir
from tests.support import wait_for
from tests.test_names import FakeNetwork
from tests.test_node import NetClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
A_ID, B_ID = bytes([0xA] * 16), bytes([0xB] * 16)
OTHER = bytes([0x5E] * 16)  # a name of someone else's

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the nodes' log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class OfflineNode(embedded.Node):
    """rnfs.Node over an offline RnfsNode (no Reticulum, no lock): the embedded class's code, but its start."""

    def __init__(self, node: RnfsNode):
        """The public interface of node."""
        self._node = node
        interface.Rnfs.__init__(self, embedded._Backend(node))
        node.names.add_record_listener(self._heard)

    def _close(self):
        """Stop listening (the test stops the node)."""
        self._node.names.remove_record_listener(self._heard)


class PublicTests:
    """The interface's behaviour, whichever class provides it: subclasses make self.rnfs over node B."""

    def setUp(self):
        """Nodes A and B (B's jobs and names started), and the interface over B."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.net = NetClient()
        self.names_net = FakeNetwork()
        self.a = self.make_node("a", A_ID, FakeNetwork())
        self.b = self.make_node("b", B_ID, self.names_net)
        self.b.routing.heard(A_ID)
        self.b.names.start()
        self.b.feature("jobs").start()
        self.rnfs = self.make(self.b)
        self.addCleanup(self.rnfs.close)

    def make_node(self, name: str, node_id: bytes, names_network) -> RnfsNode:
        """An offline node in its own directory."""
        config = Config(rnfs_dir=os.path.join(self.tmp, name), slow="no", lookup_timeout=2, path_timeout=0.2,
                        max_jobs=2)
        node = RnfsNode.offline(config, node_id, self.net, names_network)
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def share(self, data: bytes, name: str | None = None) -> Cid:
        """Add data on A (wrapped as name, if given) and make A its provider."""
        cid = Cid.parse(self.a.add_stream(io.BytesIO(data), name=name)["cid"])
        self.a.records.store(cid.key, A_ID)
        return cid

    def out(self, name: str) -> str:
        """A path in the temporary directory."""
        return os.path.join(self.tmp, name)

    # Content

    def test_add_ls_info_stat(self):
        """add() of data returns Added; ls, info and stat describe what it stored."""
        data = os.urandom(150_000)
        added = self.rnfs.add(data, name="f.bin")
        self.assertIsInstance(added, rnfs.Added)
        self.assertEqual((added.size, added.warnings, added.sealed, str(added)), (len(data), (), False,
                                                                                  str(added.cid)))
        entries = self.rnfs.ls(added.cid)
        self.assertEqual([(e.name, e.kind, e.size, e.is_dir) for e in entries], [("f.bin", "file", len(data), False)])
        info = self.rnfs.info(f"{added.cid}/f.bin")
        self.assertEqual((info.cid, info.type, info.size, info.executable), (entries[0].cid, "file", len(data), False))
        stat = self.rnfs.stat(added.cid)
        self.assertEqual((stat.cid, stat.size, stat.blocks), (added.cid, len(data), stat.local_blocks))
        self.assertEqual(self.rnfs.add(data, only_hash=True, name="f.bin").cid, added.cid)

    def test_add_path_and_stream(self):
        """A directory, a binary file, and add()'s refusals."""
        path = self.out("dir")
        os.makedirs(os.path.join(path, "sub"))
        with open(os.path.join(path, "sub", "x.txt"), "wb") as f:
            f.write(b"hello")
        added = self.rnfs.add(path)
        self.assertEqual([entry.name for entry in self.rnfs.ls(added.cid)], ["sub"])
        self.assertEqual(self.rnfs.add(io.BytesIO(b"hello")).cid, Cid.for_block(RAW, b"hello"))
        with self.assertRaises(ValueError):
            self.rnfs.add(path, name="x")
        with self.assertRaises(ValueError):
            self.rnfs.add(b"data", wrap=True)
        with self.assertRaises(rnfs.NotFound):
            self.rnfs.add(self.out("missing"))

    def test_import_key_counts_a_memoryviews_bytes(self):
        """import_key of a memoryview whose items are wider than a byte imports all its bytes."""
        name = self.rnfs.names.create("site")
        key = name.export_key()
        name.remove()
        self.assertEqual(self.rnfs.names.import_key("again", memoryview(key).cast("H")).name, name.name)

    def test_add_counts_a_memoryviews_bytes(self):
        """add() of a memoryview whose items are wider than a byte stores all its bytes."""
        data = array.array("i", range(1000))
        added = self.rnfs.add(memoryview(data))
        self.assertEqual((added.cid, added.size), (Cid.for_block(RAW, data.tobytes()), len(data.tobytes())))

    def test_open_reads_only_what_it_covers(self):
        """A File seeks and reads, each refill fetching only the range it covers."""
        data = os.urandom(300_000)
        cid = self.share(data, "f.bin")
        reads = []
        original = self.rnfs._backend.read

        def spy(target, offset, length, raw, from_node):
            """Record each read the file makes."""
            reads.append((offset, length))
            return original(target, offset, length, raw, from_node)
        with mock.patch.object(self.rnfs._backend, "read", spy):
            with self.rnfs.open(f"{cid}/f.bin") as f:
                self.assertIsInstance(f, rnfs.File)
                self.assertEqual((f.size, f.seekable(), f.readable()), (len(data), True, True))
                f.seek(200_000)
                self.assertEqual(f.read(10), data[200_000:200_010])
                self.assertEqual(reads, [(200_000, interface.FILE_BUFFER)])  # one refill: the chunks it covers
                self.assertEqual(f.read(10), data[200_010:200_020])  # from the buffer
                self.assertEqual(len(reads), 1)
                f.seek(-5, io.SEEK_END)
                self.assertEqual((f.read(), f.read()), (data[-5:], b""))
                f.seek(0)
                self.assertEqual(f.read(), data)
                self.assertEqual(f.tell(), len(data))
            self.assertTrue(f.closed)
        with self.assertRaises(ValueError):
            self.rnfs.open(cid)  # a directory
        reads.clear()
        with mock.patch.object(self.rnfs._backend, "read", spy), self.rnfs.open(f"{cid}/f.bin") as f:
            self.assertEqual(f.read(), data)
        self.assertEqual(reads, [(0, len(data))])  # the rest in one read, not one per 8 KiB

    def test_open_reads_ahead_while_sequential(self):
        """Sequential refills double up to READ_AHEAD; a seek starts again with a refill of only what it covers."""
        data = os.urandom(1_000_000)
        cid = self.share(data, "f.bin")
        reads = []
        original = self.rnfs._backend.read

        def spy(target, offset, length, raw, from_node):
            """Record each read the file makes."""
            reads.append((offset, length))
            return original(target, offset, length, raw, from_node)
        buffer = interface.FILE_BUFFER
        with mock.patch.object(self.rnfs._backend, "read", spy), \
                mock.patch.object(interface, "READ_AHEAD", 4 * buffer), self.rnfs.open(f"{cid}/f.bin") as f:
            self.assertEqual(b"".join(iter(lambda: f.read(1000), b"")), data)
            self.assertEqual(f.tell(), len(data))
            self.assertEqual(reads, [(0, buffer), (buffer, 2 * buffer), (3 * buffer, 4 * buffer),
                                     (7 * buffer, 4 * buffer), (11 * buffer, 4 * buffer),
                                     (15 * buffer, len(data) - 15 * buffer)])
            # Random reads: each refill reads only the chunks it covers, however far the read-ahead had grown
            reads.clear()
            for offset in (500_000, 10, 999_990, 300_000):
                f.seek(offset)
                self.assertEqual(f.read(10), data[offset:offset + 10])
            self.assertEqual(reads, [(500_000, buffer), (10, buffer), (999_990, 10), (300_000, buffer)])
            # Reading on from a seek grows again from FILE_BUFFER, and tell() counts what was read ahead as unread
            reads.clear()
            self.assertEqual(f.read(3 * buffer), data[300_010:300_010 + 3 * buffer])
            self.assertEqual(f.tell(), 300_010 + 3 * buffer)
            self.assertEqual(reads, [(300_000 + buffer, 2 * buffer), (300_000 + 3 * buffer, 4 * buffer)])
            # A seek into what was read ahead (past the File's own buffer) reads it again, from there only
            reads.clear()
            f.seek(300_000 + 5 * buffer)
            self.assertEqual(f.read(10), data[300_000 + 5 * buffer:300_010 + 5 * buffer])
            self.assertEqual(reads, [(300_000 + 5 * buffer, buffer)])
            # The rest: what was read ahead, then one read
            reads.clear()
            self.assertEqual(f.read(buffer), data[300_010 + 5 * buffer:300_010 + 6 * buffer])
            self.assertEqual(f.read(), data[300_010 + 6 * buffer:])
            self.assertEqual(reads, [(300_000 + 6 * buffer, 2 * buffer), (300_000 + 8 * buffer,
                                                                          len(data) - 300_000 - 8 * buffer)])

    def test_cat_get_and_uri_targets(self):
        """cat and get, rnfs: URI targets (from= and a name), and block()."""
        data = os.urandom(100_000)
        cid = self.share(data, "f.bin")
        self.assertEqual(b"".join(self.rnfs.cat(f"{cid}/f.bin", offset=10, length=20)), data[10:30])
        written = self.rnfs.get(f"rnfs:{cid}/f.bin?from={A_ID.hex()}", self.out("got"))
        self.assertEqual((written.output, str(written)), (self.out("got"), self.out("got")))
        with open(self.out("got"), "rb") as f:
            self.assertEqual(f.read(), data)
        with self.assertRaises(ValueError):
            self.rnfs.get(cid, self.out("got"))  # exists
        name = self.rnfs.names.create("site")
        name.publish(cid)
        self.assertEqual(self.rnfs.info(f"rnfs:name/{name.hex}/f.bin").size, len(data))
        self.assertEqual(self.rnfs.block(self.rnfs.add(b"hello").cid), b"hello")

    def test_cat_raises_once_iterated(self):
        """cat() of a directory raises ValueError on the first next(), not at the call, whichever class."""
        chunks = self.rnfs.cat(self.rnfs.add(b"x", name="x.txt").cid)
        with self.assertRaises(ValueError):
            next(chunks)

    # Pins and archives

    def test_pins(self):
        """pin, pins, unpin (NotFound the second time), export, import, gc and verify."""
        cid = self.share(os.urandom(70_000))
        self.assertEqual(self.rnfs.pin(cid), cid)
        pins = self.rnfs.pins(cid)
        self.assertEqual([(p.cid, p.sources, p.complete, p.type) for p in pins], [(cid, ("user",), True, "file")])
        self.rnfs.unpin(cid)
        self.assertEqual(self.rnfs.pins(cid), [])
        with self.assertRaises(rnfs.NotFound):
            self.rnfs.unpin(cid)
        exported = self.rnfs.export(cid, self.out("a.rnfs"))
        self.assertEqual(exported.roots, (cid,))
        self.assertEqual(self.rnfs.import_archive(self.out("a.rnfs")).roots, (cid,))
        self.assertIsInstance(self.rnfs.gc(), rnfs.Collected)
        self.assertEqual(self.rnfs.verify().bad, ())

    def test_dir_set_and_rm(self):
        """dir_set adds an entry, dir_rm removes it again; dir_rm of a root is ValueError (a 400 for a Client),
        refused by the node itself before it fetches anything (ND3)."""
        directory = self.share(b"inside", name="a.txt")
        child = self.rnfs.add(b"child").cid
        edited = self.rnfs.dir_set(directory, "b.txt", child, executable=True)
        self.assertEqual([(e.name, e.kind) for e in self.rnfs.ls(edited)], [("a.txt", "file"), ("b.txt", "executable")])
        self.assertEqual(self.rnfs.dir_rm(f"{edited}/b.txt"), directory)
        remote = self.share(b"not here", name="c.txt")
        for target in (remote, f"{remote}/"):
            with self.subTest(target), self.assertRaisesRegex(ValueError, "the entry to remove"):
                self.rnfs.dir_rm(target)
        self.assertFalse(self.b.storage.has(remote))  # not fetched

    def test_remote_pin_providers_and_ping(self):
        """pin_remote asks the node (as /pin), providers finds the provider, ping times a node or answers None."""
        cid = self.share(b"provided")
        self.assertIs(self.rnfs.pin_remote(A_ID, cid), True)
        self.assertIs(self.rnfs.pin_remote(A_ID, cid, pin=False), True)
        self.assertEqual(self.net.pins, [(A_ID, cid, True), (A_ID, cid, False)])
        self.assertIn(A_ID, self.rnfs.providers(cid))
        self.assertEqual(self.rnfs.ping(A_ID), 0.1)
        self.assertIsNone(self.rnfs.ping(OTHER))

    def test_errors_are_one_hierarchy(self):
        """The same exception classes whichever class raises them."""
        data = os.urandom(1000)
        cid = self.share(data)
        with self.assertRaises(ValueError):
            self.rnfs.ls(cid)  # a file
        with self.assertRaises(rnfs.NotFound):
            self.rnfs.names.get("nothing")
        with self.assertRaises(rnfs.RnfsError):
            self.rnfs.names.resolve(OTHER)  # nobody announces it
        with self.assertRaises(ValueError):
            self.rnfs.ping(b"short")

    # Names

    def test_names(self):
        """create, publish, refresh, list, resolve, export, remove and import a name."""
        cid = self.rnfs.add(b"site").cid
        name = self.rnfs.names.create("site")
        self.assertEqual((name.alias, name.cid, name.sequence, str(name)), ("site", None, 0, name.hex))
        record = name.publish(cid)
        self.assertEqual((record.name, record.cid, record.sequence), (name.name, cid, 1))
        name = name.refresh()
        self.assertEqual((name.cid, name.sequence), (cid, 1))
        self.assertEqual(self.rnfs.names.list(), [name])
        self.assertEqual(self.rnfs.names.resolve(name).cid, cid)
        key = name.export_key()
        name.remove()
        with self.assertRaises(rnfs.NotFound):
            self.rnfs.names.get("site")
        self.assertEqual(self.rnfs.names.import_key("again", key).name, name.name)

    def test_publish_warning_is_a_runtime_warning(self):
        """The node's private-mode warning on publish (DHT-4) reaches the program as a RuntimeWarning, and the
        record is returned as usual; without one, nothing is warned."""
        cid = self.rnfs.add(b"site").cid
        name = self.rnfs.names.create("site")
        original = self.rnfs._backend.name_publish

        def warning_publish(alias, target):
            return {**original(alias, target), "warning": "visible outside the private network"}

        with mock.patch.object(self.rnfs._backend, "name_publish", warning_publish), \
                self.assertWarnsRegex(RuntimeWarning, "visible outside the private network"):
            record = name.publish(cid)
        self.assertEqual((record.cid, record.sequence), (cid, 1))
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            self.assertEqual(name.publish(cid).sequence, 2)
        self.assertEqual(rnfs.Name.from_dict(name.to_dict()), name)

    def test_watch_hears_newer_records_on_the_event_thread(self):
        """A watch hears newer records only, on the event thread, with the names lock free, until cancelled."""
        first, second = self.rnfs.add(b"one").cid, self.rnfs.add(b"two").cid
        self.names_net.announce(OTHER, encode_record(3, first), time.time())  # held before the watch
        heard = []
        lock_free = []

        def callback(record):
            """Record the call, and whether the names lock is free here."""
            free = self.b.names._lock.acquire(timeout=2)
            if free:
                self.b.names._lock.release()
            lock_free.append(free)
            heard.append((threading.current_thread().name, record))
        watch = self.rnfs.names.watch(OTHER, callback)
        self.names_net.announce(OTHER, encode_record(2, second), time.time())  # older: refused, not delivered
        self.names_net.announce(OTHER, encode_record(4, second), time.time())
        wait_for(lambda: heard, what="the newer record")
        time.sleep(0.2)
        self.assertEqual([(record.name, record.cid, record.sequence) for _, record in heard], [(OTHER, second, 4)])
        self.assertEqual(heard[0][0], self.rnfs.events_thread)
        self.assertEqual(lock_free, [True])
        watch.cancel()
        self.names_net.announce(OTHER, encode_record(5, first), time.time())
        time.sleep(0.3)
        self.assertEqual(len(heard), 1)

    def test_watch_hears_own_publishes(self):
        """A watch of this node's own name hears its publish."""
        cid = self.rnfs.add(b"site").cid
        name = self.rnfs.names.create("site")
        heard = []
        with self.rnfs.names.watch(name, heard.append):
            name.publish(cid)
            wait_for(lambda: heard, what="the publish")
        self.assertEqual((heard[0].cid, heard[0].sequence), (cid, 1))

    def test_close_from_a_callback(self):
        """close() from a callback returns (it doesn't wait for the event thread it runs on) and closes."""
        cid = self.rnfs.add(b"one").cid
        returned = threading.Event()

        def callback(record):
            """Close the instance from its own event thread."""
            self.rnfs.close()
            returned.set()
        self.rnfs.names.watch(OTHER, callback)
        self.names_net.announce(OTHER, encode_record(1, cid), time.time())
        self.assertTrue(returned.wait(10), "close() from the callback didn't return")
        self.assertTrue(self.rnfs.closed)
        with self.assertRaises(rnfs.Closed):
            self.rnfs.status()

    def test_a_slow_callback_gets_the_latest_record(self):
        """Records heard while a callback runs are coalesced: the next call gets the newest, not each one."""
        cid = self.rnfs.add(b"one").cid
        gate, heard = threading.Event(), []

        def callback(record):
            """Hold the event thread at the first record."""
            heard.append(record.sequence)
            gate.wait(10)
        self.rnfs.names.watch(OTHER, callback)
        self.names_net.announce(OTHER, encode_record(2, cid), time.time())
        wait_for(lambda: heard, what="the first record")
        for sequence in (3, 4):
            self.names_net.announce(OTHER, encode_record(sequence, cid), time.time())
        wait_for(lambda: self.heard_up_to(OTHER, 4), what="record 4 reaching the instance")
        gate.set()
        wait_for(lambda: heard[-1] == 4, what="the newest record")
        time.sleep(0.3)
        self.assertEqual(heard, [2, 4])

    # Jobs and the deny list

    def test_job_with_progress(self):
        """A get job's progress callbacks, on the event thread, up to its end."""
        data = os.urandom(200_000)
        cid = self.share(data)
        progress = []
        job = self.rnfs.jobs.get(cid, self.out("job"), on_progress=lambda job: progress.append(
            (threading.current_thread().name, job)))
        self.assertIsInstance(job, rnfs.Job)
        finished = job.wait(10)
        self.assertEqual((finished.state, finished.done, finished.error), ("done", True, None))
        wait_for(lambda: progress and progress[-1][1].done, what="the final progress call")
        self.assertEqual({thread for thread, _ in progress}, {self.rnfs.events_thread})
        self.assertEqual(progress[-1][1].done_bytes, len(data))
        with open(self.out("job"), "rb") as f:
            self.assertEqual(f.read(), data)
        self.assertEqual([j.id for j in self.rnfs.jobs.list()], [job.id])
        self.assertEqual(self.rnfs.jobs.job(job.id).state, "done")
        self.assertEqual(self.rnfs.jobs.clear(), 1)
        with self.assertRaises(rnfs.NotFound):
            job.refresh()

    def test_job_wait_times_out_and_cancels(self):
        """Job.wait times out, cancel ends a queued job, and a failed job says why."""
        jobs = self.b.feature("jobs")
        with mock.patch.object(jobs, "_schedule", return_value=False):  # it stays queued
            job = self.rnfs.jobs.pin(self.share(b"pinned"))
            started = time.monotonic()
            with self.assertRaises(TimeoutError):
                job.wait(0.5)
            self.assertLess(time.monotonic() - started, 5)
            self.assertEqual(job.cancel().state, "cancelled")
        self.assertEqual(job.wait(10).state, "cancelled")
        failed = self.rnfs.jobs.pin(Cid.for_block(RAW, b"nobody has this")).wait(10)
        self.assertEqual(failed.state, "failed")
        self.assertIn("no provider", failed.error)

    def test_deny(self):
        """The deny list: add, list, remove, and malformed entries."""
        cid = str(Cid.for_block(RAW, b"denied"))
        self.assertEqual(self.rnfs.deny.add([cid]), [cid])
        self.assertEqual(self.rnfs.deny.list().entries, (cid,))
        self.assertEqual(self.rnfs.deny.remove([cid]), [cid])
        with self.assertRaises(rnfs.NotFound):
            self.rnfs.deny.remove([cid])
        with self.assertRaises(ValueError):
            self.rnfs.deny.add(["not an entry"])

    # The example app (examples/rnfs_share.py), with whichever class

    def test_example_app(self):
        """examples/rnfs_share.py's share() works through this class."""
        spec = importlib.util.spec_from_file_location("rnfs_share", os.path.join(ROOT, "examples", "rnfs_share.py"))
        example = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(example)
        folder = self.out("photos")
        os.makedirs(folder)
        data = os.urandom(20_000)
        with open(os.path.join(folder, "cat.jpg"), "wb") as f:
            f.write(data)
        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            output = example.share(self.rnfs, folder)
        self.addCleanup(shutil.rmtree, os.path.dirname(output))
        with open(os.path.join(output, "cat.jpg"), "rb") as f:
            self.assertEqual(f.read(), data)
        self.assertIn("read 4096 bytes from the middle", printed.getvalue())
        self.assertIn("heard rnfs:name/", printed.getvalue())

    # Lifecycle

    def test_status_and_close(self):
        """status(), node_id, wait_joined, and Closed after close()."""
        self.assertEqual(self.rnfs.node_id, B_ID)
        status = self.rnfs.status()
        self.assertEqual((status.node_id, status.version), (B_ID, rnfs.__version__))
        self.assertIn("jobs_running", status.features)
        self.assertEqual(status.to_dict()["jobs_running"], status.features["jobs_running"])
        self.assertTrue(self.rnfs.wait_joined(1))
        with self.rnfs:
            pass
        self.assertTrue(self.rnfs.closed)
        with self.assertRaises(rnfs.Closed):
            self.rnfs.status()
        self.rnfs.close()  # idempotent


class EmbeddedNodeTest(PublicTests, unittest.TestCase):
    """The interface on rnfs.Node."""

    def make(self, node: RnfsNode):
        """rnfs.Node over node."""
        return OfflineNode(node)

    def heard_up_to(self, name: bytes, sequence: int) -> bool:
        """True once the node has handed name's record sequence on to the watches (at once, in a Node)."""
        return True

    def test_internal_is_the_node(self):
        """Node.internal is the RnfsNode."""
        self.assertIs(self.rnfs.internal, self.b)


class ClientTest(PublicTests, unittest.TestCase):
    """The interface on rnfs.Client, through a live ApiServer serving node B."""

    def make(self, node: RnfsNode):
        """A Client of an ApiServer serving node."""
        self.server = ApiServer(node, 0)
        self.server.start()
        self.addCleanup(self.server.stop)
        cli.write_token(os.path.join(node.config.root, "api_token"), self.server.token)
        return rnfs.Client(node.config.rnfs_dir, port=self.server.port)

    def heard_up_to(self, name: bytes, sequence: int) -> bool:
        """True once the client's poller has fetched name's record sequence."""
        return self.rnfs._polled.get(name, -1) >= sequence

    def test_errors_are_the_api_clients(self):
        """A Client's errors are ApiErrors and the node's classes at once."""
        with self.assertRaises(rnfs.NotFound) as caught:
            self.rnfs.names.get("nothing")  # raised by the interface itself
        with self.assertRaises(rnfs.ApiError) as caught:
            self.rnfs.unpin(Cid.for_block(RAW, b"x"))
        self.assertIsInstance(caught.exception, rnfs.NotFound)
        self.assertEqual(caught.exception.status, 404)
        with self.assertRaises(rnfs.BadRequest):
            self.rnfs.ls(self.share(b"file"))

    def test_cat_from_a_node(self):
        """cat's from_node reaches the daemon (GET /api/cat's from)."""
        data = os.urandom(5000)
        cid = self.share(data)
        self.assertEqual(b"".join(self.rnfs.cat(cid, from_node=A_ID)), data)

    def test_cat_closed_before_reading_sends_nothing(self):
        """A cat() iterator closed before its first next() never sends GET /api/cat, so no response is left open;
        one closed after it closes the response."""
        cid = self.share(os.urandom(300_000))
        streams = []
        cat = self.rnfs.api.cat

        def spy(*args, **kwargs):
            """ApiConnection.cat, keeping each stream."""
            streams.append(cat(*args, **kwargs))
            return streams[-1]
        with mock.patch.object(self.rnfs.api, "cat", spy):
            self.rnfs.cat(cid).close()
            self.assertEqual(streams, [])
            chunks = self.rnfs.cat(cid)
            next(chunks)
            self.assertFalse(streams[0].response.isclosed())
            chunks.close()
        self.assertTrue(streams[0].response.isclosed())

    def test_watch_needs_a_daemon_that_watches(self):
        """A daemon without GET /api/name/watch: watch() raises NotFound."""
        without = {route: name for route, name in ApiHandler.ROUTES.items() if route[1] != "/api/name/watch"}
        with mock.patch.object(ApiHandler, "ROUTES", without):
            with self.assertRaises(rnfs.NotFound):
                self.rnfs.names.watch(OTHER, lambda record: None)

    def test_long_polls_fit_the_timeout(self):
        """With a timeout, a long poll waits less than it, so Job.wait times out as asked, not the socket."""
        client = rnfs.Client(self.b.config.rnfs_dir, port=self.server.port, timeout=3)
        self.addCleanup(client.close)
        self.assertEqual((client._long_wait, rnfs.Client(port=1, timeout=1)._long_wait), (1, 0))
        with mock.patch.object(self.b.feature("jobs"), "_schedule", return_value=False):  # it stays queued
            job = client.jobs.pin(self.share(b"pinned"))
            with self.assertRaises(TimeoutError):
                job.wait(4)
            job.cancel()

    def test_api_is_the_connection(self):
        """Client.api reaches any endpoint."""
        self.assertEqual(self.rnfs.api.get("/api/name/list"), {"names": []})


class NodeConstructionTest(unittest.TestCase):
    """rnfs.Node's checks before it starts anything, and its settings."""

    def setUp(self):
        """A temporary rnfs dir."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = os.path.join(tmp.name, "rnfs")

    def test_unknown_settings_are_refused(self):
        """Unknown settings, and rnfs_dir among them, are ValueError."""
        for settings in ({"no_such_setting": 1}, {"rnfs_dir": "/elsewhere"}):
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                rnfs.Node(self.dir, settings=settings)

    def test_another_reticulum_is_refused(self):
        """A reticulum that isn't the running instance is ValueError."""
        with self.assertRaises(ValueError):
            rnfs.Node(self.dir, reticulum=object())

    def test_dir_in_use_is_refused(self):
        """A Node on a dir another holds is refused before it starts anything."""
        fd = lock_rnfs_dir(Config(rnfs_dir=self.dir))
        try:
            with self.assertRaises(rnfs.RnfsError) as caught:
                rnfs.Node(self.dir)
        finally:
            fsutil.unlock(fd)
        self.assertIn("another rnfs daemon or node", str(caught.exception))
        self.assertIsNone(embedded._running)

    def test_one_node_per_process(self):
        """A second Node in the process is refused."""
        with mock.patch.object(embedded, "_running", object()):
            with self.assertRaises(rnfs.RnfsError):
                rnfs.Node(self.dir)

    def test_embedded_defaults_sit_under_the_settings_file(self):
        """EMBEDDED_DEFAULTS apply only where rnfs.toml and settings say nothing."""
        defaults = embedded.EMBEDDED_DEFAULTS
        os.makedirs(self.dir)
        self.assertEqual(Config.load(self.dir, defaults=defaults).dht_mode, "client")
        self.assertIs(Config.load(self.dir, defaults=defaults).lxmf_enabled, False)
        with open(os.path.join(self.dir, "rnfs.toml"), "w", encoding="utf-8") as f:
            f.write('dht_mode = "server"\nlxmf_enabled = true\n')
        config = Config.load(self.dir, defaults=defaults)
        self.assertEqual((config.dht_mode, config.lxmf_enabled), ("server", True))
        self.assertEqual(Config.load(self.dir, defaults=defaults, dht_mode="client").dht_mode, "client")
        self.assertEqual(Config.load(self.dir).dht_mode, "server")


class NodeLifecycleTest(unittest.TestCase):
    """rnfs.Node's own start and close, over a stand-in RnfsNode (the real one needs Reticulum: RNFS_E2E)."""

    def setUp(self):
        """A temporary rnfs dir, RnfsNode replaced by start_node, fsutil.unlock spied on, and no Node left
        running in the process whatever the test does."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.config = Config(rnfs_dir=os.path.join(tmp.name, "rnfs"))
        self.fail_start: BaseException | None = None
        self.started = []
        for patcher in (mock.patch.object(embedded, "RnfsNode", side_effect=self.start_node),
                        mock.patch.object(fsutil, "unlock", wraps=fsutil.unlock),
                        mock.patch.object(embedded, "_running", None)):
            self.addCleanup(patcher.stop)
            patcher.start()

    def start_node(self, config: Config, **options) -> mock.Mock:
        """A stand-in RnfsNode on config (raising self.fail_start instead, if set)."""
        if self.fail_start is not None:
            raise self.fail_start
        self.started.append(mock.Mock(config=config, options=options))
        return self.started[-1]

    def assert_released(self):
        """No Node runs in the process, and the rnfs dir's lock is free."""
        self.assertIsNone(embedded._running)
        fsutil.unlock(lock_rnfs_dir(self.config))

    def test_start_and_close(self):
        """A Node locks its dir, starts the node embedded with its defaults and listens to its records; close()
        stops the node, unlocks the dir and lets another Node run. A second close() does nothing."""
        node = rnfs.Node(self.config.rnfs_dir)
        self.addCleanup(node.close)
        inner, = self.started
        self.assertEqual((inner.options, inner.config.dht_mode, inner.config.lxmf_enabled),
                         ({"reticulum": None, "embedded": True}, "client", False))
        self.assertIs(embedded._running, node)
        inner.names.add_record_listener.assert_called_once_with(node._heard)
        with self.assertRaises(DirInUse):
            lock_rnfs_dir(self.config)
        with self.assertRaises(rnfs.RnfsError) as caught:
            rnfs.Node(self.config.rnfs_dir)
        self.assertIn("already running", str(caught.exception))
        node.close()
        inner.names.remove_record_listener.assert_called_once_with(node._heard)
        inner.stop.assert_called_once_with()
        fsutil.unlock.assert_called_once_with(node._lock_fd)
        self.assert_released()
        node.close()
        inner.stop.assert_called_once_with()
        rnfs.Node(self.config.rnfs_dir).close()

    def test_close_releases_when_stop_fails(self):
        """A node whose stop() raises still has its dir unlocked and its place in the process freed."""
        node = rnfs.Node(self.config.rnfs_dir)
        self.started[0].stop.side_effect = RuntimeError("failing on purpose")
        with self.assertRaises(RuntimeError):
            node.close()
        fsutil.unlock.assert_called_once_with(node._lock_fd)
        self.assert_released()

    def test_a_failed_start_leaves_nothing(self):
        """A node failing to start leaves the dir unlocked and no Node running, so the next start works."""
        self.fail_start = RuntimeError("failing on purpose")
        with self.assertRaises(RuntimeError):
            rnfs.Node(self.config.rnfs_dir)
        fsutil.unlock.assert_called_once()
        self.assert_released()
        self.fail_start = None
        rnfs.Node(self.config.rnfs_dir).close()
        self.assertEqual(len(self.started), 1)


class WatchedNamesTest(unittest.TestCase):
    """NameManager's watches: a watched name's announce takes a table place an unasked one wouldn't."""

    def test_watched_names_count_as_asked(self):
        """A watched name's announce gets a table place; listeners hear only watched names."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        node = RnfsNode.offline(Config(rnfs_dir=tmp.name, name_table_size=1), B_ID, NetClient(), FakeNetwork())
        self.addCleanup(node.stop)
        names, cid = node.names, Cid.for_block(RAW, b"x")
        names.offer(bytes([1] * 16), 1, cid, time.time())  # the table's one place: a name asked for
        names._heard(OTHER, encode_record(1, cid))
        self.assertIsNone(names.record(OTHER))  # unasked: refused
        heard = []
        names.add_record_listener(lambda name, record: heard.append((name, record.sequence)))
        names.watch(OTHER)
        names._heard(OTHER, encode_record(1, cid))
        self.assertEqual(names.record(OTHER).sequence, 1)
        self.assertEqual(heard, [(OTHER, 1)])
        names.unwatch(OTHER)
        names._heard(OTHER, encode_record(2, cid))
        self.assertEqual(heard, [(OTHER, 1)])  # no longer watched: not told

    def test_bound_method_listener_is_removed(self):
        """remove_record_listener removes a bound method, a new object at each access."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        node = RnfsNode.offline(Config(rnfs_dir=tmp.name), B_ID, NetClient(), FakeNetwork())
        self.addCleanup(node.stop)
        node.names.add_record_listener(self.test_bound_method_listener_is_removed)
        node.names.remove_record_listener(self.test_bound_method_listener_is_removed)
        self.assertEqual(node.names._record_listeners, ())


class NameWatchEndpointTest(unittest.TestCase):
    """GET /api/name/watch's long poll (NameWatches) and its parameters."""

    def setUp(self):
        """An offline node behind an ApiServer, and a connection to it."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.node = RnfsNode.offline(Config(rnfs_dir=tmp.name), B_ID, NetClient(), FakeNetwork())
        self.addCleanup(self.node.stop)
        self.server = ApiServer(self.node, 0)
        self.server.start()
        self.addCleanup(self.server.stop)
        cli.write_token(os.path.join(tmp.name, "api_token"), self.server.token)
        self.api = rnfs.Client(tmp.name, port=self.server.port).api
        self.cid = Cid.for_block(RAW, b"x")

    def test_answers_what_is_held_above_after(self):
        """The records held above after, at once."""
        self.assertEqual(self.api.name_watch([OTHER.hex()], [None], 0), [])
        self.node.names.watch(OTHER)  # as a call would; then an announce is kept
        self.node.names._heard(OTHER, encode_record(3, self.cid))
        records = self.api.name_watch([OTHER.hex()], [None], 0)
        self.assertEqual([(r["name"], r["cid"], r["sequence"]) for r in records], [(OTHER.hex(), str(self.cid), 3)])
        self.assertEqual(self.api.name_watch([OTHER.hex()], [3], 0), [])

    def test_waits_for_a_newer_record(self):
        """A call waits until a newer record is heard."""
        self.api.name_watch([OTHER.hex()], [None], 0)  # watched from here on
        threading.Timer(0.3, lambda: self.node.names._heard(OTHER, encode_record(1, self.cid))).start()
        started = time.monotonic()
        records = self.api.name_watch([OTHER.hex()], [None], 5)
        self.assertLess(time.monotonic() - started, 4)
        self.assertEqual([r["sequence"] for r in records], [1])

    def test_parameters_are_checked(self):
        """Malformed names, too many names and after mismatches are 400."""
        for params in ({"names": ""}, {"names": "xyz"}, {"names": ",".join([OTHER.hex()] * 65)},
                       {"names": OTHER.hex(), "after": "1,2"}, {"names": OTHER.hex(), "after": "-1"}):
            with self.subTest(params=params), self.assertRaises(rnfs.BadRequest):
                self.api.get("/api/name/watch", **params)

    def test_waiters_are_capped(self):
        """Past MAX_WATCH_WAITERS calls waiting, a call that would wait gets Busy."""
        watches = NameWatches(self.node)
        watches.waiters = tuple((frozenset(), threading.Event()) for _ in range(api.MAX_WATCH_WAITERS))
        with self.assertRaises(rnfs.Busy):
            watches.wait([OTHER], [None], 1)

    def test_calls_that_dont_wait_are_not_capped(self):
        """With MAX_WATCH_WAITERS calls waiting, a wait=0 call (a Client's watch() baseline) still answers and
        marks its name watched: it holds no connection."""
        watches = NameWatches(self.node)
        waiters = tuple((frozenset(), threading.Event()) for _ in range(api.MAX_WATCH_WAITERS))
        watches.waiters = waiters
        self.assertEqual(watches.wait([OTHER], [None], 0), [])
        self.assertIn(OTHER, watches.until)
        self.assertEqual(self.node.names._watched[OTHER], 1)
        self.assertEqual(watches.waiters, waiters)

    def test_a_watch_ending_during_another_call_is_unwatched(self):
        """A name whose watch ends while another call waits is unwatched in NameManager when that call ends."""
        now = [0.0]
        watches = NameWatches(self.node, clock=lambda: now[0])
        watches.wait([OTHER], [None], 0)  # watched until WATCH_GRACE
        now[0] = api.WATCH_GRACE - 10
        records = watches._records

        def later(names, after):
            """The call's wait, which outlasts OTHER's watch."""
            now[0] = api.WATCH_GRACE + 30
            return records(names, after)
        with mock.patch.object(watches, "_records", later):
            watches.wait([bytes([1] * 16)], [None], 0)
        self.assertEqual(set(watches.until), {bytes([1] * 16)})
        self.assertNotIn(OTHER, self.node.names._watched)

    def test_a_name_listed_twice_is_watched_once(self):
        """names=X,X holds one watch of X, released once."""
        self.api.name_watch([OTHER.hex(), OTHER.hex()], [None, 2], 0)
        self.assertEqual(self.node.names._watched[OTHER], 1)

    def test_watches_end_after_their_grace(self):
        """A name stays watched until WATCH_GRACE after its last call."""
        now = [0.0]
        watches = NameWatches(self.node, clock=lambda: now[0])
        watches.wait([OTHER], [None], 0)
        self.assertIn(OTHER, self.node.names._watched)
        now[0] = api.WATCH_GRACE + 1
        watches.wait([bytes([1] * 16)], [None], 0)
        self.assertNotIn(OTHER, self.node.names._watched)


class ObjectsTest(unittest.TestCase):
    """The typed objects: from_dict(to_dict()) gives the object back, and to_dict is the API's form."""

    def test_round_trips(self):
        """from_dict(to_dict()) gives each object back; to_dict is the API's form."""
        cid = Cid.for_block(RAW, b"x")
        for value in (objects.Added(cid, 1, ("w",), True), objects.Written(cid, "/o", (), None, True, "n"),
                      objects.Entry("n", "file", 1, cid), objects.Info(cid, "file", 1, False, True, True, 2, "n"),
                      objects.Stat(cid, 1, 2, 3, 4), objects.Pin(cid, ("user",), True, "file", False, None),
                      objects.Record(OTHER, cid, 2, 1.5), objects.Collected(1, 2), objects.Verified(3, (cid,)),
                      objects.Exported((cid,), 1), objects.Imported((cid,), (), 1), objects.DenyState(("a",), ()),
                      objects.Status("0.1.4", B_ID, None, "client", features={"jobs_running": 0})):
            with self.subTest(value=type(value).__name__):
                self.assertEqual(type(value).from_dict(value.to_dict()), value)
        self.assertEqual(objects.Entry("n", "file", 1, cid).to_dict(), {"name": "n", "kind": "file", "size": 1,
                                                                        "cid": str(cid)})
        self.assertEqual(objects.Record(OTHER, cid, 2, None).to_dict()["name"], OTHER.hex())


class ImportTest(unittest.TestCase):
    """`import rnfs` stays light: rnfs.Client and the objects load no RNS; rnfs.Node does."""

    def run_python(self, code: str) -> str:
        """code's output in a fresh interpreter."""
        return subprocess.run([sys.executable, "-c", code], capture_output=True, encoding="utf-8", cwd=ROOT,
                              env={**os.environ, "PYTHONPATH": ROOT}, timeout=60, check=True).stdout.strip()

    def test_client_loads_no_rns(self):
        """rnfs.Client, the objects and errors load no RNS."""
        code = ("import sys, rnfs; rnfs.Client(port=1); rnfs.Cid, rnfs.Record, rnfs.NotFound, rnfs.ApiError, "
                "rnfs.Job, rnfs.File, rnfs.SealError, rnfs.NotRecipient, rnfs.BlockMismatch; "
                "print(any(m == 'RNS' or m.startswith('RNS.') for m in sys.modules))")
        self.assertEqual(self.run_python(code), "False")

    def test_node_loads_rns_when_used(self):
        """RNS is loaded only once rnfs.Node is used."""
        code = "import sys, rnfs; print('RNS' in sys.modules); rnfs.Node; print('RNS' in sys.modules)"
        self.assertEqual(self.run_python(code).split(), ["False", "True"])

    def test_names_and_status(self):
        """API_STATUS, the exported names, and an unknown one."""
        self.assertEqual(rnfs.API_STATUS, "provisional")
        self.assertIs(rnfs.NotFound, __import__("rnfs.errors").errors.NotFound)
        # The errors programs could only catch as ValueError before (XC4): the classes rnfs raises
        from rnfs.content import blockstore, seal
        self.assertIs(rnfs.SealError, seal.SealError)
        self.assertIs(rnfs.NotRecipient, seal.NotRecipient)
        self.assertIs(rnfs.BlockMismatch, blockstore.BlockMismatch)
        self.assertIs(rnfs.Client, remote.Client)
        self.assertIn("Node", dir(rnfs))
        with self.assertRaises(AttributeError):
            rnfs.NoSuchThing


if __name__ == "__main__":
    unittest.main()
