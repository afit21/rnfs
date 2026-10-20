"""Tests for the public API (rnfs.Node and rnfs.Client: rnfs/interface.py, objects.py, embedded.py, remote.py).

The interface tests run twice: on an embedded node (an offline RnfsNode behind rnfs.Node's backend) and on a Client
of an ApiServer serving one, so both classes are shown to behave alike. Node A, offline too, provides content.
"""
import array
import contextlib
import dataclasses
import importlib.util
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import unittest
import warnings
from unittest import mock

import RNS

import rnfs
from rnfs import api, cli, embedded, fsutil, interface, name_sync, objects, remote
from rnfs import events as events_module
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

    def test_add_nomadnet(self):
        """add_nomadnet and jobs.add_nomadnet: a snapshot of a NomadNet link added as add() adds data."""
        from rnfs.network import nomadnet as fetching
        from tests.test_nomadnet import FILE, FILE_BYTES, PAGE, FakeNomadnet
        fake = FakeNomadnet()
        with mock.patch.object(fetching, "fetch", fake):
            added = self.rnfs.add_nomadnet(FILE, wrap=True, pin=False)
            self.assertIsInstance(added, objects.Added)
            self.assertEqual([entry.name for entry in self.rnfs.ls(added.cid)], ["song.mp3"])
            self.assertNotIn(str(added.cid), {pin["cid"] for pin in self.b.pin_ls()})
            plain = self.rnfs.add_nomadnet(FILE)
            self.assertEqual(plain.size, len(FILE_BYTES))
            self.assertEqual(self.b.pin_ls(str(plain.cid))[0]["origin"], FILE)
            self.rnfs.add_nomadnet(PAGE, identify=True)
            self.assertIsNotNone(fake.calls[-1]["identity"])
            job = self.rnfs.jobs.add_nomadnet(FILE, name="tune.mp3").wait(10)
            self.assertEqual((job.state, job.kind, job.nomadnet, job.path), ("done", "add", FILE, None))
            self.assertEqual(job.options, {"name": "tune.mp3"})
            for bad in ({"link": FILE, "name": "a/b"}, {"link": 5}, {"link": FILE + "/.."}):
                with self.subTest(bad), self.assertRaises(ValueError):
                    self.rnfs.add_nomadnet(**bad)

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

    def test_remote_pin_status(self):
        """remote_pins() lists what was asked, a check finding the node among the providers (done); served_pins()
        what other identities asked of this node, and their usage."""
        cid = self.share(b"pinned for us")  # A provides it
        other = Cid.for_block(RAW, b"nobody has it")
        self.rnfs.pin_remote(A_ID, cid)
        self.rnfs.pin_remote(A_ID.hex(), other, pin=False)
        released, asked = self.rnfs.remote_pins()
        self.assertIsInstance(asked, rnfs.RemotePin)
        self.assertEqual((asked.node, asked.cid, asked.state, asked.done), (A_ID, cid, "waiting", False))
        self.assertEqual((released.cid, released.pin, released.state), (other, False, "released"))
        checked, = self.rnfs.remote_pins(A_ID, cid, check=True)
        self.assertEqual((checked.state, checked.done, str(checked)), ("done", True, str(cid)))
        self.assertIsInstance(checked.checked, float)
        self.assertEqual(self.rnfs.remote_pins(cid=other)[0].state, "released")
        identity = bytes(range(16))
        self.b.config.remote_pin_identities = [identity.hex()]
        self.assertTrue(self.b.backend.remote_pin(identity, cid, True))
        wait_for(lambda: self.b.storage.has_pin(cid, f"remote:{identity.hex()}"), what="the remote pin")
        served = self.rnfs.served_pins()
        self.assertIsInstance(served, rnfs.ServedPins)
        pin, = served.pins
        self.assertEqual((pin.identity, pin.cid, pin.state, pin.reason), (identity, cid, "pinned", None))
        usage, = served.usage
        self.assertEqual((usage.identity, usage.pinned, usage.waiting, usage.allowed, usage.quota),
                         (identity, 1, 0, True, served.quota))
        self.assertGreater(usage.used, 0)

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

    def test_name_sync(self):
        """names.sync, syncs and unsync: a FolderSync for the folder, listed, published by the node's sync, stopped
        with the name left where it pointed; refusals as NotFound."""
        folder = self.out("site")
        os.makedirs(folder)
        with open(os.path.join(folder, "index.html"), "wb") as f:
            f.write(b"<h1>synced</h1>")
        self.rnfs.names.create("site")
        synced = self.rnfs.names.sync("site", folder)
        self.assertIsInstance(synced, rnfs.FolderSync)
        self.assertEqual((synced.alias, synced.path, synced.state, synced.published, str(synced)),
                         ("site", folder, "new", None, "site"))
        self.assertEqual(self.rnfs.names.syncs(), [synced])
        with mock.patch.object(name_sync, "SETTLE_TIME", 0.0):  # B's sync thread doesn't run: scanned here
            self.b.folder_syncs.tick()
            self.b.folder_syncs.tick()
        (entry,) = self.rnfs.names.syncs()
        self.assertEqual((entry.state, entry.published), ("synced", entry.root))
        self.assertEqual(self.rnfs.names.get("site").cid, entry.published)
        self.rnfs.names.unsync("site")
        self.assertEqual(self.rnfs.names.syncs(), [])
        self.assertEqual(self.rnfs.names.get("site").cid, entry.published)
        with self.assertRaises(rnfs.NotFound):
            self.rnfs.names.unsync("site")
        with self.assertRaises(rnfs.NotFound):
            self.rnfs.names.sync("nope", folder)
        with self.assertRaises(rnfs.NotFound):
            self.rnfs.names.sync("site", self.out("missing"))
        self.assertTrue(self.rnfs.capabilities().has("name_sync"))

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

        # An announce held for the clock is warned of the same way
        def held_publish(alias, target):
            return {**original(alias, target), "announce_held": "announce held: this node's clock is behind"}

        with mock.patch.object(self.rnfs._backend, "name_publish", held_publish), \
                self.assertWarnsRegex(RuntimeWarning, "clock is behind"):
            self.assertEqual(name.publish(cid).sequence, 3)

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

    # The features: peers, contacts, keep and follow

    def test_peers_and_stats(self):
        """peers() lists the nodes the feature does, as Peers; stats() its traffic report, as Stats."""
        peers = self.rnfs.peers()
        listed = [rnfs.Peer.from_dict(peer) for peer in self.b.feature("peers").peers()]
        timeless = {"seniority": None, "first_heard": None}  # times measured at each call
        self.assertEqual([dataclasses.replace(peer, **timeless) for peer in peers],
                         [dataclasses.replace(peer, **timeless) for peer in listed])
        self.assertEqual([(str(peer), peer.node_id, peer.in_table) for peer in peers], [(A_ID.hex(), A_ID, True)])
        stats = self.rnfs.stats()
        self.assertIsInstance(stats, rnfs.Stats)
        self.assertEqual(stats.totals, self.b.feature("peers").stats()["totals"])
        self.assertEqual(stats.upload_budget, self.b.config.upload_budget)

    def test_contacts(self):
        """add (the kind told by the node, or given), get, list and remove; and what they refuse."""
        alice = self.rnfs.contacts.add("alice", A_ID)
        self.assertEqual((alice, str(alice)), (rnfs.Contact("alice", "node", A_ID), "alice"))
        self.assertEqual(self.rnfs.contacts.add("other", OTHER.hex(), kind="name").kind, "name")
        self.assertEqual(self.rnfs.contacts.get("other"), rnfs.Contact("other", "name", OTHER))
        self.assertEqual([contact.nick for contact in self.rnfs.contacts.list()], ["alice", "other"])
        self.rnfs.contacts.remove("alice")
        with self.assertRaises(rnfs.NotFound):
            self.rnfs.contacts.remove("alice")
        with self.assertRaises(rnfs.NotFound):
            self.rnfs.contacts.get("alice")
        for nick, ident, kind in (("Not A Nick", A_ID, None), ("bob", A_ID, "friend"), ("bob", b"short", None),
                                  ("bob", bytes([7] * 16), None)):  # the last: a kind the node can't tell
            with self.subTest(nick=nick, kind=kind), self.assertRaises(ValueError):
                self.rnfs.contacts.add(nick, ident, kind)
        with self.assertRaises(ValueError):
            self.rnfs.contacts.get("Not A Nick")

    def test_keep_and_follow(self):
        """names.keep/unkeep/kept and follow/unfollow/following, a name given as bytes, hex, a Name or a nick."""
        name = self.rnfs.names.create("site")
        name.publish(self.rnfs.add(b"site").cid)
        kept = self.rnfs.names.keep(OTHER)
        self.assertEqual((kept, str(kept)), (rnfs.Kept(OTHER, ("user",)), OTHER.hex()))
        self.assertEqual(self.rnfs.names.keep(name).name, name.name)
        self.assertEqual({entry.name for entry in self.rnfs.names.kept()}, {OTHER, name.name})
        self.rnfs.names.unkeep(name.hex)
        with self.assertRaises(rnfs.NotFound):
            self.rnfs.names.unkeep(name)
        self.rnfs.contacts.add("other", OTHER, kind="name")
        followed = self.rnfs.names.follow("other")
        self.assertEqual((followed.name, followed.pinned, followed.state), (OTHER, None, "waiting"))
        self.assertEqual(self.rnfs.names.following(), [followed])
        self.assertIn(rnfs.Kept(OTHER, ("follow", "user")), self.rnfs.names.kept())
        self.rnfs.names.unfollow(OTHER.hex())
        self.assertEqual(self.rnfs.names.following(), [])
        with self.assertRaises(rnfs.NotFound):
            self.rnfs.names.unfollow(OTHER)
        for bad in ("nobody", b"short", 5):  # an unknown nick, not a name, not a name at all
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.rnfs.names.keep(bad)

    def test_a_missing_feature_is_not_found(self):
        """Each feature's methods raise NotFound, with its API route's message, when the node hasn't loaded it."""
        calls = {"peers": (self.rnfs.peers, "no peers feature"),
                 "contacts": (self.rnfs.contacts.list, "no address book"),
                 "keepers": (self.rnfs.names.kept, "doesn't keep names"),
                 "follow": (lambda: self.rnfs.names.follow(OTHER), "can't follow names")}
        for feature, (call, words) in calls.items():
            with self.subTest(feature=feature), mock.patch.dict(self.b.features):
                del self.b.features[feature]
                with self.assertRaisesRegex(rnfs.NotFound, words):
                    call()

    def test_no_lxmf(self):
        """rnfs has no LXMF since 0.2.0: no lxmf attribute, feature, route or event kind; an LXMF contact stays a
        contact, which an LXMF app reads through the API."""
        self.assertFalse(hasattr(self.rnfs, "lxmf"))
        self.assertNotIn("lxmf", self.b.features)
        capabilities = self.rnfs.capabilities()
        self.assertNotIn("lxmf", capabilities.features)
        self.assertFalse([route for route in capabilities.routes if route[1].startswith("/api/lxmf")])
        self.assertNotIn("inbox", capabilities.event_kinds)
        sender = bytes([0x5D] * 16)
        self.assertEqual(self.rnfs.contacts.add("sender", sender, kind="lxmf"), rnfs.Contact("sender", "lxmf", sender))
        self.assertEqual(self.rnfs.contacts.list(), [rnfs.Contact("sender", "lxmf", sender)])

    def test_timeout_is_checked(self):
        """timeout() takes None or a positive number, on either class, and nests."""
        with self.rnfs.timeout(30), self.rnfs.timeout(None):
            self.assertEqual(self.rnfs.status().node_id, B_ID)
        for bad in (0, -1, True, "5", float("nan")):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                with self.rnfs.timeout(bad):
                    pass

    # 0.2.0: pin details and labels, the cache, capabilities, events, the daemon, settings, streams

    def test_pin_details_and_label(self):
        """pins() gives 0.2.0's details; label_pin labels a pin, None clears it, and an unpinned CID is NotFound."""
        data = os.urandom(5000)
        cid = self.rnfs.add(data).cid
        pin, = self.rnfs.pins(cid)
        self.assertEqual((pin.size, pin.label, pin.entries), (len(data), None, None))
        self.assertIsInstance(pin.added, float)
        labelled = self.rnfs.label_pin(cid, "holiday")
        self.assertEqual((labelled.cid, labelled.label), (cid, "holiday"))
        self.assertEqual(self.rnfs.pins(cid)[0].label, "holiday")
        self.assertIsNone(self.rnfs.label_pin(cid, None).label)
        with self.assertRaises(rnfs.NotFound):
            self.rnfs.label_pin(Cid.for_block(RAW, b"not pinned"), "x")

    def test_cache(self):
        """A fetch not pinned is listed by cache(); uncache() evicts it, then is NotFound."""
        data = os.urandom(5000)
        cid = self.share(data, "f.bin")
        self.rnfs.get(f"{cid}/f.bin", self.out("got"))
        cached = self.rnfs.cache()
        self.assertEqual([(entry.root, entry.path, entry.size, entry.pinned) for entry in cached],
                         [(cid, ("f.bin",), len(data), False)])
        self.assertIsInstance(cached[0], rnfs.Cached)
        removed = self.rnfs.uncache(cached[0].cid)
        self.assertEqual((removed.removed_records, removed.freed_bytes > 0), (1, True))
        self.assertEqual(self.rnfs.cache(), [])
        with self.assertRaises(rnfs.NotFound):
            self.rnfs.uncache(cached[0].cid)

    def test_capabilities(self):
        """capabilities(): the version, what the node offers, its routes, features and event kinds."""
        capabilities = self.rnfs.capabilities()
        self.assertEqual(capabilities.version, rnfs.__version__)
        self.assertTrue(capabilities.has("events") and capabilities.has("config"))
        self.assertIn(("GET", "/api/events"), capabilities.routes)
        self.assertIn("contacts", capabilities.features)
        self.assertIn("pin", capabilities.event_kinds)
        self.assertIn("not_found", capabilities.error_codes)
        self.assertEqual(capabilities.has("shutdown"), self.daemon)

    def test_shutdown_and_daemon_info(self):
        """shutdown() is Unsupported (a Node; a daemon started without a way to stop it); daemon_info() None
        where there is no daemon.json."""
        with self.assertRaises(rnfs.Unsupported):
            self.rnfs.shutdown()
        self.assertIsNone(self.rnfs.daemon_info())

    def test_events_since(self):
        """since(): the baseline, then the events after it (of kinds), lost for another run, and its checks."""
        baseline = self.rnfs.events.since()
        self.assertEqual((baseline.events, baseline.lost), ((), False))
        cid = self.rnfs.add(b"an event").cid
        batch = self.rnfs.events.since(baseline.next, kinds="pin", epoch=baseline.epoch)
        self.assertEqual([(event.kind, event.fields["change"]) for event in batch.events], [("pin", "added")])
        self.assertIn(str(cid), batch.events[0].fields["cids"])
        self.assertGreater(batch.events[0].seq, baseline.next)
        self.assertEqual(self.rnfs.events.since(batch.next, epoch=batch.epoch, wait=1).events, ())
        self.assertTrue(self.rnfs.events.since(baseline.next, epoch="another run").lost)
        self.assertFalse(self.rnfs.events.since(baseline.next, epoch="").lost)  # "" is no epoch, on both
        for bad in ({"kinds": ["nope"]}, {"after": -1}, {"after": 0, "wait": 1.5}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.rnfs.events.since(**bad)

    def test_events_watch(self):
        """watch() calls back on the event thread for each event of its kinds published after it, in order,
        until cancelled."""
        heard = []
        watch = self.rnfs.events.watch(lambda event: heard.append((threading.current_thread().name, event)),
                                       kinds=["pin"])
        first, second = self.rnfs.add(b"one").cid, self.rnfs.add(b"two").cid
        wait_for(lambda: len(heard) >= 2, what="two pin events")
        self.assertEqual({thread for thread, _ in heard}, {self.rnfs.events_thread})
        self.assertEqual([event.fields["cids"] for _, event in heard], [[str(first)], [str(second)]])
        watch.cancel()
        self.rnfs.add(b"three")
        time.sleep(0.3)
        self.assertEqual(len(heard), 2)
        with self.assertRaises(ValueError):
            self.rnfs.events.watch(print, kinds="nope")

    def test_config(self):
        """config() lists the settings; config_set() changes rnfs.toml, applying a live one (a list too), and
        refuses what it can't set."""
        settings = self.rnfs.config()
        self.assertEqual(settings.path, os.path.join(self.b.config.rnfs_dir, "rnfs.toml"))
        self.assertEqual((settings["max_jobs"].value, settings["max_jobs"].live), (2, True))
        change = self.rnfs.config_set({"max_jobs": 3, "serve_interfaces": ("TCP",)})
        self.assertEqual((change.applied, change.restart_required), (("max_jobs", "serve_interfaces"), ()))
        self.assertEqual(change.warnings, ())
        self.assertEqual(self.b.config.max_jobs, 3)
        settings = self.rnfs.config()
        self.assertEqual((settings["max_jobs"].file, settings["serve_interfaces"].file), (3, ("TCP",)))
        self.assertEqual(rnfs.Settings.from_dict(settings.to_dict()), settings)
        # The lxmf_* settings left rnfs with LXMF in 0.2.0
        for bad in ({"no_such_setting": 1}, {}, {"max_jobs": "many"}, "max_jobs", {"lxmf_accept_from": ()}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.rnfs.config_set(bad)

    def test_add_flags_and_recipients(self):
        """jobs.add and add take truthy flags as booleans and refuse recipients that aren't strings with
        ValueError, alike on both classes (the audit's parity: wrap=1, to=[b"x"])."""
        path = self.out("flags")
        os.makedirs(path)
        with open(os.path.join(path, "x.txt"), "wb") as f:
            f.write(b"x")
        job = self.rnfs.jobs.add(path, wrap=1, only_hash=1)
        self.assertEqual(job.wait(10).state, "done")
        self.assertEqual(self.rnfs.add(path, wrap=1, only_hash=1).cid, rnfs.Cid.parse(job.wait(10).result["cid"]))
        for to in ([b"x"], "alice", [5]):
            with self.subTest(to=to), self.assertRaisesRegex(ValueError, "to must be a list of recipients"):
                self.rnfs.jobs.add(path, to=to)
            with self.subTest(to=to), self.assertRaisesRegex(ValueError, "to must be a list of recipients"):
                self.rnfs.add(b"data", to=to)

    def test_sealed_folder(self):
        """add() of a folder with to_self seals it as one tar; get() unpacks it unless no_extract (Written.extracted),
        and jobs.get takes no_extract too, alike on both classes."""
        self.b.identity = RNS.Identity()  # B opens what it seals for itself
        folder = self.out("photos")
        os.makedirs(os.path.join(folder, "sub"))
        with open(os.path.join(folder, "sub", "a.txt"), "wb") as f:
            f.write(b"a")
        added = self.rnfs.add(folder, to_self=True)
        self.assertTrue(added.sealed)
        written = self.rnfs.get(added.cid, self.out("unpacked"))
        self.assertEqual((written.sealed, written.extracted, written.name), (True, True, "photos"))
        with open(os.path.join(self.out("unpacked"), "sub", "a.txt"), "rb") as f:
            self.assertEqual(f.read(), b"a")
        written = self.rnfs.get(added.cid, self.out("tar"), no_extract=True)
        self.assertEqual((written.sealed, written.extracted, written.name), (True, False, "photos.tar"))
        self.assertTrue(tarfile.is_tarfile(self.out("tar")))
        job = self.rnfs.jobs.get(added.cid, self.out("job.tar"), no_extract=True).wait(10)
        self.assertEqual((job.state, job.no_extract), ("done", True))
        self.assertTrue(tarfile.is_tarfile(self.out("job.tar")))
        self.assertTrue(self.rnfs.capabilities().has("sealed_folders"))

    def test_streams(self):
        """add_tar, get_tar, export_stream and import_stream: a tar and an archive through the program."""
        source = self.out("tree")
        os.makedirs(os.path.join(source, "sub"))
        with open(os.path.join(source, "sub", "x.txt"), "wb") as f:
            f.write(b"hello")
        tar = io.BytesIO()
        with tarfile.open(fileobj=tar, mode="w") as archive:
            archive.add(os.path.join(source, "sub"), arcname="sub")
        added = self.rnfs.add_tar(tar.getvalue(), name="tree")
        self.assertEqual([entry.name for entry in self.rnfs.ls(added.cid)], ["tree"])
        self.assertEqual(b"".join(self.rnfs.cat(f"{added.cid}/tree/sub/x.txt")), b"hello")
        self.assertEqual(self.rnfs.add_tar(io.BytesIO(tar.getvalue()), name="tree", only_hash=True).cid, added.cid)
        with tarfile.open(fileobj=io.BytesIO(b"".join(self.rnfs.get_tar(f"{added.cid}/tree")))) as back:
            member, = [name for name in back.getnames() if name.endswith("sub/x.txt")]
            self.assertEqual(back.extractfile(member).read(), b"hello")
        exported = self.rnfs.export(added.cid, self.out("a.rnfs"))
        streamed = b"".join(self.rnfs.export_stream(added.cid))
        with open(self.out("a.rnfs"), "rb") as f:
            self.assertEqual(streamed, f.read())
        self.rnfs.unpin(added.cid)
        self.assertEqual(self.rnfs.import_stream(memoryview(streamed)).roots, exported.roots)
        self.assertEqual(self.rnfs.pins(added.cid)[0].sources, ("user",))
        with self.assertRaises(rnfs.NotFound):
            next(self.rnfs.get_tar(f"{added.cid}/no such entry"))
        with self.assertRaises(ValueError):
            self.rnfs.add_tar(b"not a tar" * 100)
        self.rnfs.get_tar(added.cid).close()  # never iterated: nothing sent, nothing fetched

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
        # What the node sees of the network are fields of their own, not features
        self.assertEqual(status.network["size_samples"], 0)
        self.assertIsNone(status.network["size_estimate"])
        self.assertEqual(set(status.record_store["tiers"]), {"known", "earned", "newcomer"})
        self.assertEqual(status.record_store["budget"], status.to_dict()["record_store"]["budget"])
        self.assertNotIn("network", status.features)
        self.assertTrue(self.rnfs.wait_joined(1))
        with self.rnfs:
            pass
        self.assertTrue(self.rnfs.closed)
        with self.assertRaises(rnfs.Closed):
            self.rnfs.status()
        self.rnfs.close()  # idempotent


class EmbeddedNodeTest(PublicTests, unittest.TestCase):
    """The interface on rnfs.Node."""

    daemon = False  # it can't be stopped through an API

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

    daemon = True

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

    def test_timeout_bounds_this_threads_requests(self):
        """timeout() bounds the requests this thread makes inside it: a slow one raises ConnectionFailed, while
        another thread's, and this thread's after the block, wait for the answer."""
        peers = self.b.feature("peers")
        original = peers.peers

        def slow():
            """The listing, a second late."""
            time.sleep(1)
            return original()
        with mock.patch.object(peers, "peers", slow), mock.patch("rnfs.api.log"):  # answering too late fails
            other = []
            thread = threading.Thread(target=lambda: other.append(self.rnfs.peers()))
            with self.rnfs.timeout(0.3):
                thread.start()
                with self.assertRaisesRegex(rnfs.ConnectionFailed, "didn't answer within its timeout"):
                    self.rnfs.peers()
            self.assertEqual(self.rnfs.peers()[0].node_id, A_ID)
            thread.join(10)
            self.assertEqual(other[0][0].node_id, A_ID)

    def test_timeout_fits_long_polls(self):
        """Inside timeout(), Job.wait's long polls ask to wait within it, so it times out as asked, not the socket;
        outside, they wait the API's cap."""
        waits = []
        job_call = self.rnfs.api.job

        def spy(job_id, wait=0):
            """ApiConnection.job, recording each wait asked."""
            waits.append(wait)
            return job_call(job_id, wait)
        with mock.patch.object(self.b.feature("jobs"), "_schedule", return_value=False), \
                mock.patch.object(self.rnfs.api, "job", spy):  # the job stays queued
            job = self.rnfs.jobs.pin(self.share(b"pinned"))
            with self.rnfs.timeout(3), self.assertRaises(TimeoutError):
                job.wait(4)
            self.assertTrue(waits and max(waits) == 1, waits)
            self.assertEqual(remote.long_poll_wait(None), remote.MAX_WATCH_WAIT)
            job.cancel()

    def test_event_watch_hears_a_restart_as_lost(self):
        """A daemon whose run changed (a new event log, as at a restart): the watch gets a "lost" Event, then the
        new run's events."""
        heard = []
        self.rnfs.events.watch(heard.append)
        old = self.b.events
        self.b.events = events_module.EventLog()  # the next poll asks it with the old run's epoch
        old.publish("peer", node_id=A_ID.hex(), change="new", online=True)  # ends the poll waiting on the old log
        wait_for(lambda: any(event.lost for event in heard), what="the lost marker")
        lost = next(event for event in heard if event.lost)
        self.assertEqual(lost.fields["epoch"], self.b.events.epoch)
        cid = self.rnfs.add(b"after the restart").cid
        wait_for(lambda: heard[-1].kind == "pin" and heard[-1].fields["cids"] == [str(cid)], what="the new event")

    def test_a_watch_begun_in_a_new_run_leaves_older_watches_hearing(self):
        """A daemon restarting while the poller waits to retry, and a new watch begun in the new run: the older
        watch still gets "lost", then the new run's events, as the new watch does (the audit's ev_restart)."""
        older, newer = [], []
        self.rnfs.events.watch(older.append)
        real, failed = self.rnfs.api.events, threading.Event()

        def events(after=None, kinds=None, wait=0, epoch=None):
            """The daemon dropping the poll in flight once, as a restart does."""
            if after is not None and not failed.is_set():
                failed.set()
                raise rnfs.ConnectionFailed("the daemon dropped the connection")
            return real(after, kinds, wait, epoch)
        with mock.patch.object(self.rnfs.api, "events", events), mock.patch.object(remote, "RETRY_DELAY", 1.0), \
                mock.patch.object(self.rnfs, "_report"):
            old, self.b.events = self.b.events, events_module.EventLog()
            old.publish("peer", node_id=A_ID.hex(), change="new", online=True)  # ends the poll on the old run
            wait_for(failed.is_set, what="the failed poll")
            self.rnfs.events.watch(newer.append)  # while the poller waits to retry
            wait_for(lambda: any(event.lost for event in older), what="the older watch's lost marker")
            self.b.events.publish("peer", node_id=A_ID.hex(), change="online", online=True)
            for heard in (older, newer):
                wait_for(lambda heard=heard: heard and heard[-1].fields.get("change") == "online",
                         what="the new run's event")
        self.assertFalse(any(event.lost for event in newer))

    def test_two_first_watches_miss_nothing(self):
        """Two first watches at once: the one with the older baseline still gets the events between the two (the
        poller starts at the first baseline taken; the audit's ev_two_watch)."""
        real, gate = self.rnfs.api.events, threading.Event()

        def events(after=None, kinds=None, wait=0, epoch=None):
            """The first watch's baseline, held up after its answer."""
            result = real(after, kinds, wait, epoch)
            if after is None and threading.current_thread().name == "first":
                gate.wait(5)
            return result
        first, second = [], []
        with mock.patch.object(self.rnfs.api, "events", events):
            one = threading.Thread(name="first", target=lambda: self.rnfs.events.watch(first.append))
            one.start()
            time.sleep(0.3)
            log = self.b.events
            log.publish("peer", node_id=A_ID.hex(), change="new", online=True)
            two = threading.Thread(target=lambda: self.rnfs.events.watch(second.append))
            two.start()
            time.sleep(0.3)
            gate.set()
            one.join(5)
            two.join(5)
            log.publish("peer", node_id=A_ID.hex(), change="online", online=True)
            wait_for(lambda: len(first) == 2 and second, what="the events")
        self.assertEqual([event.fields["change"] for event in first], ["new", "online"])
        self.assertEqual([event.fields["change"] for event in second], ["online"])

    def test_since_pauses_when_the_timeout_leaves_no_wait(self):
        """events.since(wait=) inside a timeout too short for a long poll pauses up to a second on an empty answer,
        as Job.wait does, rather than returning at once for the caller to spin."""
        batch = self.rnfs.events.since()
        with self.rnfs.timeout(2):
            started = time.monotonic()
            self.assertEqual(self.rnfs.events.since(batch.next, wait=5, epoch=batch.epoch).events, ())
            self.assertGreaterEqual(time.monotonic() - started, remote.SHORT_POLL * 0.9)

    def test_old_daemon_has_no_events_or_capabilities(self):
        """A daemon without the 0.2.0 endpoints: empty capabilities, and NoSuchEndpoint (a NotFound) from
        events.since and events.watch."""
        new = {"/api/events", "/api/capabilities"}
        without = {route: name for route, name in ApiHandler.ROUTES.items() if route[1] not in new}
        with mock.patch.object(ApiHandler, "ROUTES", without):
            capabilities = self.rnfs.capabilities()
            self.assertEqual((capabilities, capabilities.has("events")), (rnfs.Capabilities(), False))
            for call in (self.rnfs.events.since, lambda: self.rnfs.events.watch(print)):
                with self.assertRaises(rnfs.NoSuchEndpoint):
                    call()
        self.assertEqual(self.rnfs._event_watch_count(), 0)  # the failed watch was forgotten

    def test_shutdown_and_daemon_json(self):
        """shutdown() stops a daemon that can be stopped through its API; daemon_info() reads its daemon.json."""
        stopped = threading.Event()
        server = ApiServer(self.b, 0, shutdown=stopped.set)
        server.start()
        self.addCleanup(server.stop)
        client = rnfs.Client(self.b.config.rnfs_dir, port=server.port)
        self.addCleanup(client.close)
        cli.write_token(os.path.join(self.b.config.root, "api_token"), server.token)
        self.assertTrue(client.shutdown())
        self.assertTrue(stopped.wait(5))
        info = {"pid": os.getpid(), "api_port": server.port, "version": rnfs.__version__}
        fsutil.write_atomic(os.path.join(self.b.config.rnfs_dir, "daemon.json"), json.dumps(info).encode(),
                            mode=0o600, private=True)
        self.assertEqual(client.daemon_info(), info)

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
        with open(os.path.join(self.dir, "rnfs.toml"), "w", encoding="utf-8") as f:
            f.write('dht_mode = "server"\n')
        config = Config.load(self.dir, defaults=defaults)
        self.assertEqual(config.dht_mode, "server")
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
        self.assertEqual((inner.options, inner.config.dht_mode), ({"reticulum": None, "embedded": True}, "client"))
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


class EventWatchTest(unittest.TestCase):
    """_EventWatch's position: what it delivers before and after its baseline, across runs and losses."""

    @staticmethod
    def event(seq: int, kind: str = "pin") -> dict:
        """An event of kind numbered seq."""
        return {"seq": seq, "kind": kind, "time": 1.0}

    def test_events_heard_before_the_baseline_wait_for_it(self):
        """Events offered before begin() wait; begin delivers only those after the baseline, of its run."""
        delivered = []
        watch = interface._EventWatch(frozenset({"pin"}), print)
        for epoch, event in (("run", self.event(4)), ("run", self.event(6)), ("run", self.event(7, "job")),
                             ("other", self.event(8))):
            self.assertFalse(watch.offer(epoch, event, delivered.append))
        self.assertEqual(delivered, [])
        self.assertEqual(watch.begin(5, "run", delivered.append), [self.event(6)])
        self.assertTrue(watch.offer("run", self.event(9), delivered.append))
        self.assertFalse(watch.offer("run", self.event(9), delivered.append))  # never twice
        self.assertFalse(watch.offer("run", self.event(10, "job"), delivered.append))  # not its kind
        self.assertEqual(delivered, [self.event(6), self.event(9)])

    def test_lost(self):
        """A lost marker after the watch's position is delivered and moves it to the new run; one the watch began
        after is not."""
        delivered = []
        watch = interface._EventWatch(None, print)
        watch.begin(5, "run", delivered.append)
        lost = {"seq": 3, "kind": "lost", "time": None, "epoch": "run"}
        self.assertFalse(watch.offer("run", lost, delivered.append))  # began after what was missed
        self.assertTrue(watch.offer("new", {**lost, "seq": 2, "epoch": "new"}, delivered.append))
        self.assertTrue(watch.offer("new", self.event(3), delivered.append))
        self.assertFalse(watch.offer("run", self.event(20), delivered.append))  # the old run's
        self.assertEqual([event["kind"] for event in delivered], ["lost", "pin"])

    def test_events_heard_while_beginning_come_after_the_pending(self):
        """An event another thread hears while begin() delivers the pending ones is delivered after them: deliveries
        are queued under the watch's lock (the audit's ev_begin_order)."""
        delivered = []
        watch = interface._EventWatch(None, print)
        watch.offer("run", self.event(6), delivered.append)
        other = threading.Thread(target=watch.offer, args=("run", self.event(7), delivered.append))

        def deliver(event):
            """Deliver the pending event, once the other thread is trying to deliver a later one."""
            other.start()
            time.sleep(0.2)
            delivered.append(event)
        watch.begin(5, "run", deliver)
        other.join(5)
        self.assertEqual([event["seq"] for event in delivered], [6, 7])


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
                      objects.Status("0.1.4", B_ID, None, "client", features={"jobs_running": 0}),
                      objects.Status("0.2.0", B_ID, network={"size_estimate": 1240, "size_samples": 16},
                                     record_store={"full": True, "tiers": {"newcomer": {"records": 3}}}),
                      objects.Peer(A_ID, "server", True, 12.5, False, 2, "TCP", 1e6, True, 800.0, 1.7e9, None,
                                   False, 1, 2, 3, 4, 5, 6, 7, 1.6e9, 0.25, 1.7e9, True),
                      objects.Stats(1.7e9, {"sent": 1}, {"budget": 2}, {"TCP": {"sent": 1, "received": 0}}, 10, 5),
                      objects.Contact("alice", "node", A_ID), objects.Kept(OTHER, ("user",), 3, cid, 1.7e9),
                      objects.Followed(OTHER, cid, cid, 3, "current"),
                      objects.FolderSync("site", "/f", "synced", None, cid, cid, 1.7e9, 1.7e9 + 3600),
                      objects.Contact("bob", "lxmf", B_ID),
                      objects.Pin(cid, ("user",), True, "file", False, "n", "label", "/o", 3, None, 1.7e9),
                      objects.Cached(cid, cid, ("a",), False, True, "file", 3, True), objects.Uncached(1, 2, 3),
                      objects.Event(3, "pin", 1.7e9, {"change": "added", "cids": [str(cid)]}),
                      objects.EventBatch((objects.Event(1, "job", 1.0, {"id": "x"}),), 1, False, "e"),
                      objects.Capabilities("0.2.0", frozenset({"events"}), (("GET", "/api/events"),), ("jobs",),
                                           ("pin",), ("not_found",)),
                      objects.Settings("/r/rnfs.toml", (objects.Setting("serve_interfaces", "mesh", "list[string]",
                                                                        None, False, (), None, value=("a",)),),
                                       None, ("w",)),
                      objects.SettingsChange("/r/rnfs.toml", ("max_jobs",), (), (), ())):
            with self.subTest(value=type(value).__name__):
                self.assertEqual(type(value).from_dict(value.to_dict()), value)
        self.assertEqual(objects.Contact.from_dict({"nick": "a", "kind": "node", "id": A_ID.hex(),
                                                    "new_field": 1}).id, A_ID)  # a newer daemon's keys ignored
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
                "rnfs.Job, rnfs.File, rnfs.SealError, rnfs.NotRecipient, rnfs.BlockMismatch, rnfs.Peer, "
                "rnfs.Contact, rnfs.Contacts; "
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
