"""Tests for RnfsNode's facade and backend (rnfs/node.py), on offline nodes with a fake network client."""
import hashlib
import io
import os
import random
import shutil
import stat
import tempfile
import threading
import time
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE, DEFAULT_BOOTSTRAP_NODES, K
from rnfs.content import dag, seal
from rnfs.content.cid import RAW, Cid
from rnfs.content.seal import SEGMENT_SIZE
from rnfs.content.storage import remote_source
from rnfs import fetcher
from rnfs.errors import Denied, InvalidDag, NotFound, RnfsError, StorageFull, TooLarge
from rnfs.features.keepers import name_key
from rnfs.network.client import OK, BlocksResult
from rnfs.network.identity import parse_node_id
from rnfs import fsutil, node as node_module, statedb
from rnfs.node import RnfsNode
from tests.support import POSIX_MODES, failing_commits, state_rows, wait_for
from tests.test_names import FakeNetwork

A_ID, B_ID, N1 = bytes([0xA] * 16), bytes([0xB] * 16), bytes([1] * 16)
PROVIDE_BATCH = 22  # keys per /provide at the default MDU (section 3)
IDENTITY = bytes(range(16))

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node's log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class NetClient:
    """A PeerClient stand-in connecting offline nodes: /blocks asks the other node's backend directly."""

    def __init__(self):
        """No nodes yet."""
        self.nodes: dict[bytes, RnfsNode] = {}
        self.provide_calls: list[tuple[bytes, list[bytes], list[bytes]]] = []
        self.pins: list[tuple[bytes, Cid, bool]] = []

    def open_links(self):
        """No open links: providers come from lookups."""
        return []

    def rate_estimate(self, node_id):
        """No estimates."""
        return None

    def link_mdu(self, node_id):
        """No open links, so no MDU known."""
        return None

    def find(self, node_id, key):
        """Ask a node's backend as /find would."""
        node = self.nodes.get(node_id)
        if node is None or not node.backend.is_dht_server():
            return None
        return node.backend.find(key)

    def provide(self, node_id, keys, withdrawn):
        """Record, and pass to the node's backend as /provide would (as the sender is unknown here, B).

        Like PeerClient.provide, returns the keys sent up to and including the first batch refused."""
        self.provide_calls.append((node_id, list(keys), list(withdrawn)))
        node = self.nodes.get(node_id)
        if node is None:
            return set()
        sent = set()
        for start in range(0, max(len(keys), 1), PROVIDE_BATCH):
            batch = list(keys)[start:start + PROVIDE_BATCH]
            sent.update(batch)
            if not node.backend.provide(self.sender, batch, list(withdrawn) if start == 0 else []):
                break
        return sent

    def blocks(self, node_id, cids, on_progress=None):
        """Serve from a node's backend: blocks it serves, None for the rest."""
        node = self.nodes.get(node_id)
        if node is None:
            return BlocksResult("failed", unsent=list(cids))
        result = BlocksResult(OK)
        for cid in cids[:8]:
            block = node.backend.served_block(cid)
            if block is None:
                result.missing.append(cid)
            else:
                result.blocks[cid] = block
        result.unsent = list(cids[8:])
        return result

    def pin(self, node_id, cid, pin):
        """Record a /pin."""
        self.pins.append((node_id, cid, pin))
        return True

    def ping(self, node_id):
        """Every known node answers."""
        return 0.1 if node_id in self.nodes else None


class NodeTest(unittest.TestCase):
    """Two offline nodes, A and B, joined by a NetClient: A answers B's queries and serves its blocks."""

    def setUp(self):
        """Fresh nodes in a temporary directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = NetClient()
        self.net.sender = B_ID
        self.a = self.make_node("a", A_ID)
        self.b = self.make_node("b", B_ID)
        self.b.routing.heard(A_ID)

    def make_node(self, name: str, node_id: bytes, **settings) -> RnfsNode:
        """An offline node with its own directory."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, name), slow="no", lookup_timeout=5, path_timeout=0.5, **settings)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def tree(self) -> str:
        """A directory on disk: a multi-chunk file, an executable, a nested directory."""
        root = os.path.join(self.tmp.name, "tree")
        os.makedirs(os.path.join(root, "sub", "deeper"))
        self.big = os.urandom(5 * CHUNK_SIZE + 123)
        with open(os.path.join(root, "big.bin"), "wb") as f:
            f.write(self.big)
        script = os.path.join(root, "sub", "run.sh")
        with open(script, "wb") as f:
            f.write(b"#!/bin/sh\necho hi\n")
        os.chmod(script, 0o755)
        with open(os.path.join(root, "sub", "deeper", "note.txt"), "wb") as f:
            f.write(b"deep")
        return root

    def advertise(self, cid: str):
        """Make A a provider of cid in its own records, as its publisher would."""
        self.a.records.store(Cid.parse(cid).key, A_ID)


class LocalFacadeTest(NodeTest):
    """Facade calls on content stored locally."""

    def test_add_and_read_back(self):
        added = self.a.add_path(self.tree())
        cid = added["cid"]
        self.assertEqual(added["warnings"], [])
        self.assertEqual(self.a.info(cid)["type"], "directory")
        info = self.a.info(f"{cid}/sub/run.sh")
        self.assertEqual((info["type"], info["executable"]), ("file", POSIX_MODES))  # Windows: always kind 0
        names = [(e["name"], e["kind"]) for e in self.a.ls(cid)]
        self.assertEqual(names, [("big.bin", "file"), ("sub", "directory")])
        self.assertEqual(b"".join(self.a.read(f"{cid}/big.bin", offset=70000, length=1000)),
                         self.big[70000:71000])
        self.assertTrue(self.a.is_local(cid))
        self.assertFalse(self.a.is_local(str(Cid.for_block(RAW, b"elsewhere"))))
        # The fields of 0.1.7 too: no label yet, the origin as the name, the size and entries from the root block
        listed = self.a.pin_ls()
        self.assertLessEqual(abs(listed[0].pop("added") - time.time()), 60)
        origin = os.path.abspath(os.path.join(self.tmp.name, "tree"))
        self.assertEqual(listed, [{"cid": cid, "sources": ["user"], "complete": True, "type": "directory",
                                   "sealed": False, "name": origin, "label": None, "origin": origin,
                                   "size": added["size"], "entries": 2}])
        self.assertEqual(self.a.stat(cid)["blocks"], self.a.stat(cid)["local_blocks"])
        self.assertIn(Cid.parse(cid), self.a.publisher.provided())
        self.assertEqual(self.a.status()["pins"], 1)
        # members and remote_pin_identities take the identity hash, so status shows it
        self.assertIsNone(self.a.status()["identity_hash"])  # offline test nodes have no identity
        self.a.identity = mock.Mock(hash=bytes(range(16)))
        self.assertEqual(self.a.status()["identity_hash"], bytes(range(16)).hex())

    def test_add_stream_and_only_hash(self):
        result = self.a.add_stream(io.BytesIO(b"hello"), size=5, name="hello.txt")
        self.assertEqual(b"".join(self.a.read(result["cid"] + "/hello.txt")), b"hello")
        hashed = self.a.add_stream(io.BytesIO(b"not stored"), only_hash=True)
        self.assertFalse(self.a.storage.has(Cid.parse(hashed["cid"])))
        with self.assertRaises(NotFound):
            self.a.add_path(os.path.join(self.tmp.name, "missing"))

    def test_read_is_lazy(self):
        missing = str(Cid.for_block(RAW, b"nowhere"))
        chunks = self.a.read(missing)  # nothing happens until the first chunk is asked for
        with self.assertRaises(NotFound):
            next(chunks)

    def test_dir_set_and_rm(self):
        cid = self.a.add_path(self.tree())["cid"]
        extra = self.a.add_stream(io.BytesIO(b"extra"))["cid"]
        edited = self.a.dir_set(f"{cid}/sub", "extra", extra, executable=True)["cid"]
        info = self.a.info(f"{edited}/sub/extra")
        self.assertEqual((info["cid"], info["executable"]), (extra, True))
        removed = self.a.dir_rm(f"{edited}/sub/extra")["cid"]
        self.assertEqual(removed, cid)
        self.assertIn("user", self.a.storage.pins()[Cid.parse(edited)])

    def test_export_import(self):
        cid = self.a.add_path(self.tree())["cid"]
        archive_path = os.path.join(self.tmp.name, "out.rnfs")
        exported = self.a.export(cid, archive_path)
        self.assertEqual(exported["roots"], [cid])
        imported = self.b.import_archive(archive_path)
        self.assertEqual((imported["roots"], imported["incomplete"]), ([cid], []))
        self.assertTrue(self.b.is_local(cid))
        with self.assertRaises(ValueError):
            self.a.export(cid, archive_path)  # refuses to overwrite

    def test_pin_rm_and_gc(self):
        cid = self.a.add_path(self.tree())["cid"]
        self.a.pin_rm(cid)
        with self.assertRaises(NotFound):
            self.a.pin_rm(cid)
        self.assertEqual(self.a.publisher.provided(), set())
        self.assertGreater(self.a.gc()["removed_blocks"], 0)
        self.assertEqual(self.a.storage.block_count(), 0)

    def test_names(self):
        cid = self.a.add_path(self.tree())["cid"]
        name = self.a.name_create("site")["name"]
        published = self.a.name_publish("site", f"{cid}/sub")
        self.assertEqual(published["cid"], self.a.info(f"{cid}/sub")["cid"])
        self.assertEqual(self.a.name_resolve(name)["cid"], published["cid"])
        self.assertIn(Cid.parse(published["cid"]), self.a.publisher.provided())
        with self.assertRaises(NotFound):
            self.b.name_resolve(name)
        self.a.name_rm("site")
        self.assertNotIn(Cid.parse(published["cid"]), self.a.publisher.provided())


class NetworkFacadeTest(NodeTest):
    """B fetching from A through the backend."""

    def test_get_by_cid_and_path(self):
        cid = self.a.add_path(self.tree())["cid"]
        self.advertise(cid)
        out = os.path.join(self.tmp.name, "out")
        result = self.b.get(cid, out)
        self.assertEqual(result["cid"], cid)
        with open(os.path.join(out, "big.bin"), "rb") as f:
            self.assertEqual(f.read(), self.big)
        if POSIX_MODES:  # Windows has no execute bit
            self.assertTrue(os.stat(os.path.join(out, "sub", "run.sh")).st_mode & stat.S_IXUSR)
        single = os.path.join(self.tmp.name, "note.txt")
        self.b.get(f"{cid}/sub/deeper/note.txt", single)
        with open(single, "rb") as f:
            self.assertEqual(f.read(), b"deep")
        self.assertEqual(self.b.storage.cached_targets() >= {Cid.parse(cid)}, True)
        # With the default roots strategy, B re-shares what it downloaded completely, while it is cached
        self.assertIn(Cid.parse(cid), self.b.publisher.provided())
        self.assertIsNotNone(self.b.backend.served_block(Cid.parse(cid)))
        # With pinned, B doesn't provide or serve what it downloaded
        self.b.config.provide_strategy = "pinned"
        self.b.publisher.refresh()
        self.assertEqual(self.b.publisher.provided(), set())
        self.assertIsNone(self.b.backend.served_block(Cid.parse(cid)))

    def test_range_read_fetches_only_part(self):
        cid = self.a.add_path(self.tree())["cid"]
        self.advertise(cid)
        data = b"".join(self.b.read(f"{cid}/big.bin", offset=CHUNK_SIZE + 1, length=10))
        self.assertEqual(data, self.big[CHUNK_SIZE + 1:CHUNK_SIZE + 11])
        stats = self.b.stat(f"{cid}/big.bin")
        self.assertLess(stats["local_blocks"], stats["blocks"])

    def test_roots_strategy_provides_fetches_but_not_gateway_ones(self):
        self.b.config.provide_strategy = "roots"
        first = self.a.add_stream(io.BytesIO(b"for the user"))["cid"]
        second = self.a.add_stream(io.BytesIO(b"for the gateway"))["cid"]
        self.advertise(first)
        self.advertise(second)
        self.b.get(first, os.path.join(self.tmp.name, "first"))
        b"".join(self.b.read(second, for_gateway=True))
        self.assertEqual(self.b.publisher.provided(), {Cid.parse(first)})

    def test_from_node(self):
        cid = self.a.add_stream(io.BytesIO(b"direct"))["cid"]
        self.b.routing.remove(A_ID)  # B knows no DHT node
        self.assertEqual(b"".join(self.b.read(cid, from_node=A_ID)), b"direct")

    def test_providers_and_pin_add(self):
        cid = self.a.add_path(self.tree())["cid"]
        self.advertise(cid)
        self.assertEqual(self.b.providers(cid), [A_ID])
        self.assertEqual(self.b.pin_add(f"{cid}/sub")["cid"], self.a.info(f"{cid}/sub")["cid"])
        self.assertEqual(len(self.b.publisher.provided()), 1)

    def test_read_file_is_info_and_read_in_one_fetch(self):
        """read_file gives info()'s answer and read()'s bytes, cut to the file's size, from one fetch session."""
        cid = self.a.add_path(self.tree())["cid"]
        self.advertise(cid)
        target = f"{cid}/big.bin"
        sessions = []
        fetching = self.b._fetching
        with mock.patch.object(self.b, "_fetching", lambda *args, **kwargs: sessions.append(1) or fetching(*args,
                                                                                                         **kwargs)):
            info, chunks = self.b.read_file(target, offset=CHUNK_SIZE + 1, length=10)
            self.assertEqual(b"".join(chunks), self.big[CHUNK_SIZE + 1:CHUNK_SIZE + 11])
            self.assertEqual(len(sessions), 1)
            self.assertEqual(info, self.b.info(target))
            for offset, length in ((len(self.big) - 5, 100), (len(self.big) + 7, None), (0, 0)):
                info, chunks = self.b.read_file(target, offset=offset, length=length)
                self.assertEqual(b"".join(chunks), self.big[offset:] if length is None else
                                 self.big[offset:offset + length])
        with self.assertRaises(ValueError):
            self.b.read_file(target, offset=-1)

    def test_read_file_of_a_directory_reads_nothing(self):
        """A directory's info comes back; reading it raises ValueError, and closing it unread ends the fetch."""
        cid = self.a.add_path(self.tree())["cid"]
        self.advertise(cid)
        info, chunks = self.b.read_file(f"{cid}/sub")
        self.assertEqual(info["type"], "directory")
        with self.assertRaisesRegex(ValueError, "is a directory"):
            next(chunks)
        info, chunks = self.b.read_file(cid)
        chunks.close()
        self.assertEqual(self.b.storage._sessions, [])  # its protection session ended
        with self.assertRaises(NotFound):
            self.b.read_file(f"{cid}/missing")

    def test_read_file_checks_the_deny_list_before_the_first_byte(self):
        """A file whose hash is denied raises Denied at the first chunk, after its info."""
        cid = self.a.add_stream(io.BytesIO(b"unwanted" * 100))["cid"]
        self.advertise(cid)
        self.b.deny.add(["sha256:" + hashlib.sha256(b"unwanted" * 100).hexdigest()])
        info, chunks = self.b.read_file(cid)
        self.assertEqual(info["size"], 800)
        with self.assertRaises(Denied):
            next(chunks)

    def test_invalid_root_reported(self):
        self.b.fetcher.mark_invalid(Cid.for_block(RAW, b"bad"))
        with self.assertRaises(InvalidDag):
            self.b.info(str(Cid.for_block(RAW, b"bad")))


class SealedCheckTest(NodeTest):
    """Whether a file is sealed is read from its first bytes once: plain files are remembered (_not_sealed), and
    read_file passes what its info opened on to its read."""

    def setUp(self):
        """Node A, with an identity to seal for itself, counting seal.open_sealed's calls (each reads a file's
        first bytes)."""
        super().setUp()
        self.a.identity = RNS.Identity()
        opened = mock.patch.object(seal, "open_sealed", wraps=seal.open_sealed)
        self.opened = opened.start()
        self.addCleanup(opened.stop)

    def test_plain_files_are_checked_once(self):
        """A plain file's first bytes are read once, whatever reads it afterwards; each target separately."""
        data = os.urandom(3 * CHUNK_SIZE)
        cid = self.a.add_stream(io.BytesIO(data))["cid"]
        self.assertEqual(b"".join(self.a.read(cid, CHUNK_SIZE, 10)), data[CHUNK_SIZE:CHUNK_SIZE + 10])
        self.assertEqual(self.opened.call_count, 1)
        info, chunks = self.a.read_file(cid, 5, 10)
        self.assertEqual((info["size"], b"".join(chunks)), (len(data), data[5:15]))
        self.assertNotIn("sealed", self.a.info(cid))
        self.assertEqual(b"".join(self.a.read(cid)), data)
        self.assertEqual(self.opened.call_count, 1)
        # The same file under a directory is another target (its expectation differs): checked once too
        root = self.a.add_stream(io.BytesIO(data), name="f.bin")["cid"]
        for _ in range(2):
            self.assertEqual(b"".join(self.a.read(f"{root}/f.bin", 0, 4)), data[:4])
        self.assertEqual(self.opened.call_count, 2)

    def test_sealed_files_are_opened_each_time_once_per_read_file(self):
        """A sealed file isn't remembered; read_file opens it once (for its info), not again for its bytes."""
        data = os.urandom(SEGMENT_SIZE + 5)
        cid = self.a.add_stream(io.BytesIO(data), to_self=True)["cid"]
        for _ in range(2):
            self.assertEqual(b"".join(self.a.read(cid, 3, 4)), data[3:7])
        self.assertEqual(self.opened.call_count, 2)
        self.assertEqual(len(self.a._not_sealed), 0)
        for count in (3, 4):
            info, chunks = self.a.read_file(cid, SEGMENT_SIZE - 1, 3)
            self.assertEqual((info["sealed"], info["recipient"], info["size"]), (True, True, len(data)))
            self.assertEqual(b"".join(chunks), data[SEGMENT_SIZE - 1:SEGMENT_SIZE + 2])
            self.assertEqual(self.opened.call_count, count)

    def test_read_file_of_a_file_sealed_for_others(self):
        """What info opened isn't unlocked for a non-recipient: the read raises NotRecipient, and for a denied
        file Denied first, as when the read opened it itself."""
        other = RNS.Identity()
        data = b"for someone else" * 10
        cid = self.a.add_stream(io.BytesIO(data), to=[other.get_public_key().hex()])["cid"]
        info, chunks = self.a.read_file(cid)
        self.assertEqual((info["sealed"], info["recipient"]), (True, False))
        with self.assertRaises(seal.NotRecipient):
            next(chunks)
        self.a.identity = None
        info, chunks = self.a.read_file(cid)
        with self.assertRaisesRegex(seal.NotRecipient, "no identity"):
            next(chunks)
        stored = b"".join(self.a.read(cid, raw=True))
        self.a.deny.add(["sha256:" + hashlib.sha256(stored).hexdigest()])
        info, chunks = self.a.read_file(cid)
        with self.assertRaises(Denied):
            next(chunks)

    def test_the_memo_is_bounded(self):
        """At most NOT_SEALED_MEMO targets are remembered, the least recently used forgotten first."""
        cids = [self.a.add_stream(io.BytesIO(b"file %d" % i))["cid"] for i in range(3)]
        with mock.patch.object(node_module, "NOT_SEALED_MEMO", 2):
            for cid in cids[:2]:
                b"".join(self.a.read(cid))
            b"".join(self.a.read(cids[0]))  # used again: now the most recent
            b"".join(self.a.read(cids[2]))
            self.assertEqual([str(expect.cid) for expect in self.a._not_sealed], [cids[0], cids[2]])
            self.assertEqual(self.opened.call_count, 3)
            b"".join(self.a.read(cids[1]))  # forgotten: read again
            self.assertEqual(self.opened.call_count, 4)

    def test_a_deny_list_change_empties_the_memo(self):
        """Reading the first bytes checks the deny list: once their chunk is denied, a read of a remembered file
        raises Denied as it did before the file was remembered, though the range read doesn't cover it."""
        data = os.urandom(3 * CHUNK_SIZE)
        cid = self.a.add_stream(io.BytesIO(data))["cid"]
        self.assertEqual(b"".join(self.a.read(cid, 2 * CHUNK_SIZE, 4)), data[2 * CHUNK_SIZE:2 * CHUNK_SIZE + 4])
        first = dag.decode(self.a.storage.get(Cid.parse(cid))).children[0].cid
        self.a.deny.add([str(first)])
        with self.assertRaises(Denied):
            b"".join(self.a.read(cid, 2 * CHUNK_SIZE, 4))
        self.assertEqual(self.opened.call_count, 2)
        self.assertEqual(len(self.a._not_sealed), 0)


class PinLsMemoTest(NodeTest):
    """pin_ls remembers each complete pin's type, name from inside and sealed status, rather than reading its
    blocks at every call; what it gives is what reading them gives."""

    def setUp(self):
        """Node A, with an identity, counting _pin_shape's calls (each reads the pin's root block)."""
        super().setUp()
        self.a.identity = RNS.Identity()
        shape = mock.patch.object(self.a, "_pin_shape", wraps=self.a._pin_shape)
        self.shape = shape.start()
        self.addCleanup(shape.stop)

    def fresh(self) -> list[dict]:
        """pin_ls as it is without the memo."""
        with mock.patch.object(node_module, "PIN_MEMO", 0):
            saved, self.a._pin_memo = self.a._pin_memo, {}
            try:
                return self.a.pin_ls()
            finally:
                self.a._pin_memo = saved

    def test_complete_pins_are_read_once(self):
        """A second pin_ls reads no pin's blocks again and answers the same, for files, directories and sealed
        files; pin_ls(cid) uses the memo too."""
        cids = [self.a.add_stream(io.BytesIO(b"small"))["cid"],
                self.a.add_stream(io.BytesIO(os.urandom(2 * CHUNK_SIZE)))["cid"],
                self.a.add_stream(io.BytesIO(b"inside"), name="one.txt")["cid"],
                self.a.add_stream(io.BytesIO(b"secret"), to_self=True)["cid"]]
        first = self.a.pin_ls()
        self.assertEqual(self.shape.call_count, 4)
        self.assertEqual({entry["cid"]: (entry["type"], entry["sealed"]) for entry in first},
                         {cids[0]: ("file", False), cids[1]: ("file", False), cids[2]: ("directory", False),
                          cids[3]: ("file", True)})
        self.assertEqual(self.a.pin_ls(), first)
        self.assertEqual(self.a.pin_ls(cids[2]), [entry for entry in first if entry["cid"] == cids[2]])
        self.assertEqual(self.shape.call_count, 4)
        self.assertEqual(first, self.fresh())

    def test_released_pins_are_forgotten(self):
        """pin_ls forgets the pins released since the last."""
        keep, drop = (self.a.add_stream(io.BytesIO(b"%d" % i))["cid"] for i in range(2))
        self.a.pin_ls()
        self.assertEqual({str(c) for c in self.a._pin_memo}, {keep, drop})
        self.a.pin_rm(drop)
        self.assertEqual([entry["cid"] for entry in self.a.pin_ls()], [keep])
        self.assertEqual({str(c) for c in self.a._pin_memo}, {keep})

    def test_incomplete_pins_are_read_each_time(self):
        """A pin missing blocks isn't remembered: once they arrive it is seen complete, its sealed status known."""
        data = os.urandom(2 * CHUNK_SIZE)
        cid = Cid.parse(self.b.add_stream(io.BytesIO(data))["cid"])
        self.a.storage.put(cid, self.b.storage.get(cid))
        self.a.storage.pin(cid, "user")
        for _ in range(2):
            self.assertEqual([(e["complete"], e["type"], e["sealed"]) for e in self.a.pin_ls()], [(False, "file", None)])
        self.assertEqual(self.a._pin_memo, {})
        for child in dag.decode(self.b.storage.get(cid)).child_cids:
            self.a.storage.put(child, self.b.storage.get(child))
        self.assertEqual([(e["complete"], e["type"], e["sealed"]) for e in self.a.pin_ls()], [(True, "file", False)])
        self.assertIn(cid, self.a._pin_memo)

    def test_a_pin_that_loses_a_block_is_read_again(self):
        """A remembered pin whose first chunk is found damaged (and deleted) is incomplete again: its sealed
        status is unknown, as reading its blocks says."""
        cid = Cid.parse(self.a.add_stream(io.BytesIO(os.urandom(2 * CHUNK_SIZE)))["cid"])
        self.assertEqual([(e["complete"], e["sealed"]) for e in self.a.pin_ls()], [(True, False)])
        first = dag.decode(self.a.storage.get(cid)).child_cids[0]
        path = self.a.storage.blocks.path(first)
        os.chmod(path, 0o600)
        with open(path, "r+b") as f:
            f.write(b"damaged")
        self.assertIsNone(self.a.storage.get(first, verify=True))  # deleted
        self.assertEqual([(e["complete"], e["type"], e["sealed"]) for e in self.a.pin_ls()], [(False, "file", None)])
        self.assertEqual(self.a.pin_ls(), self.fresh())

    def test_the_memo_is_bounded(self):
        """At most PIN_MEMO pins are remembered (the first met); the others are read each time, answering the
        same."""
        for i in range(3):
            self.a.add_stream(io.BytesIO(b"%d" % i))
        with mock.patch.object(node_module, "PIN_MEMO", 2):
            first = self.a.pin_ls()
            self.assertEqual(len(self.a._pin_memo), 2)
            self.assertEqual(self.a.pin_ls(), first)
            self.assertEqual(self.shape.call_count, 4)  # 3, then the one past the bound again
        self.assertEqual(first, self.fresh())


class BackendTest(NodeTest):
    """What the request server asks of the node."""

    def test_find(self):
        key = Cid.for_block(RAW, b"k").key
        self.a.records.store(key, N1)
        self.a.records.store(key, B_ID)
        self.a.routing.heard(N1)
        self.a.routing.ban(B_ID)
        providers, nodes = self.a.backend.find(key)
        self.assertEqual((providers, nodes), ([N1], [N1]))

    def test_provide_only_in_server_mode_and_not_from_banned(self):
        key = Cid.for_block(RAW, b"k").key
        self.assertTrue(self.a.backend.provide(N1, [key], []))
        self.a.routing.ban(B_ID)
        self.assertFalse(self.a.backend.provide(B_ID, [key], []))
        client = self.make_node("c", bytes([0xC] * 16), dht_mode="client")
        self.assertFalse(client.backend.provide(N1, [key], []))
        self.assertFalse(client.backend.is_dht_server())

    def test_served_block_only_from_provided_dags(self):
        cid = Cid.parse(self.a.add_stream(io.BytesIO(os.urandom(3 * CHUNK_SIZE)))["cid"])
        self.assertIsNotNone(self.a.backend.served_block(cid))
        cached = self.a.storage
        other = Cid.for_block(RAW, b"cached only")
        cached.put(other, b"cached only")
        self.assertIsNone(self.a.backend.served_block(other))
        self.assertEqual(self.a.backend.served_block_size(cid), len(self.a.backend.served_block(cid)))
        self.assertIsNone(self.a.backend.served_block_size(other))  # sizes only of what is served
        self.a.pin_rm(str(cid))
        self.assertIsNone(self.a.backend.served_block(cid))
        self.assertIsNone(self.a.backend.served_block_size(cid))

    def test_damaged_block_not_served(self):
        cid = Cid.parse(self.a.add_stream(io.BytesIO(b"will be damaged"))["cid"])
        with open(self.a.storage.blocks.path(cid), "wb") as f:
            f.write(b"damaged")
        self.assertIsNone(self.a.backend.served_block(cid))
        self.a.maintain(time.monotonic(), {})
        self.assertEqual(self.a.publisher.provided(), set())

    def test_is_member(self):
        node = self.make_node("p", bytes([0xD] * 16), private=True, members=[IDENTITY.hex()])
        self.assertTrue(node.backend.is_member(IDENTITY))
        self.assertFalse(node.backend.is_member(bytes(16)))

    def test_identity_checks_dont_mark_destinations_used(self):
        # Checks, not contacts (C2): recalled with _no_use, which skips RNS's "used" RPC on a shared instance
        node = self.make_node("p", bytes([0xD] * 16), private=True, members=[IDENTITY.hex()])
        member = mock.Mock(hash=IDENTITY)
        with mock.patch.object(RNS.Identity, "recall", return_value=member) as recall:
            self.assertTrue(node._is_member_node(A_ID))
            self.assertTrue(node_module._rns_is_known(B_ID))
        self.assertEqual(recall.call_args_list, [mock.call(A_ID, _no_use=True), mock.call(B_ID, _no_use=True)])

    def wait_for(self, condition, timeout=10):
        """Poll until condition() is true."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return True
            time.sleep(0.05)
        return False

    def test_remote_pin_within_quota(self):
        cid = self.b.add_stream(io.BytesIO(os.urandom(2 * CHUNK_SIZE)))["cid"]
        self.b.records.store(Cid.parse(cid).key, B_ID)
        self.a.routing.heard(B_ID)
        self.assertTrue(self.a.backend.remote_pin(IDENTITY, Cid.parse(cid), True))
        source = remote_source(IDENTITY)
        self.assertTrue(self.wait_for(lambda: source in self.a.storage.pins().get(Cid.parse(cid), set())))
        # The worker refreshes the publisher just after pinning
        self.assertTrue(self.wait_for(lambda: Cid.parse(cid) in self.a.publisher.provided()))
        self.assertTrue(self.a.backend.remote_pin(IDENTITY, Cid.parse(cid), False))
        self.assertEqual(self.a.storage.pins(), {})

    def test_remote_pin_over_quota_dropped(self):
        self.a.config.remote_pin_quota = CHUNK_SIZE
        cid = self.b.add_stream(io.BytesIO(os.urandom(2 * CHUNK_SIZE)))["cid"]
        self.b.records.store(Cid.parse(cid).key, B_ID)
        self.a.routing.heard(B_ID)
        self.a.backend.remote_pin(IDENTITY, Cid.parse(cid), True)
        self.assertTrue(self.wait_for(lambda: not self.a._remote_reserved.get(remote_source(IDENTITY))))
        self.assertEqual(self.a.storage.pins(), {})
        # Only the root block was fetched before the quota showed it couldn't fit
        self.assertEqual(self.a.storage.block_count(), 1)

    def test_publish_and_withdraw_through_the_node(self):
        cid = self.b.add_stream(io.BytesIO(b"published by B"))["cid"]
        self.b.publisher.publish_due()
        self.assertEqual(self.a.records.providers(Cid.parse(cid).key), [B_ID])
        self.b.pin_rm(cid)
        self.b.publisher.publish_due()
        self.assertEqual(self.a.records.providers(Cid.parse(cid).key), [])

    def test_pin_remote_sends_request(self):
        self.assertTrue(self.b.pin_remote(A_ID, str(Cid.for_block(RAW, b"x")), True))
        self.assertEqual(self.net.pins[0][0], A_ID)


class SecurityTest(NodeTest):
    """Regression tests for the node's part of security findings B3-B6, B8, B11 and B12."""

    def wait_for(self, condition, timeout: float = 5.0) -> bool:
        """Poll until condition() is true."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return True
            time.sleep(0.05)
        return False

    def empty_files(self, count: int) -> str:
        """A directory on disk of count empty files with long names: size 0, but kilobytes of directory nodes."""
        root = os.path.join(self.tmp.name, "empties")
        os.mkdir(root)
        for i in range(count):
            # Names short enough that the paths stay within Windows' 260-character limit
            open(os.path.join(root, f"{i:04d}" + "n" * 120), "wb").close()
        return root

    @unittest.skipUnless(POSIX_MODES, "Unix permission bits")
    def test_rnfs_dir_is_private(self):
        loose = os.path.join(self.tmp.name, "loose")
        os.mkdir(loose)
        os.chmod(loose, 0o775)
        node = self.make_node("loose", N1)
        for directory in (node.config.root, node.config.state_dir, node.config.blocks_dir,
                          os.path.dirname(node.config.identity_path), node.config.names_dir):
            self.assertEqual(stat.S_IMODE(os.stat(directory).st_mode), 0o700, directory)
        self.assertEqual(stat.S_IMODE(os.stat(self.b.config.root).st_mode), 0o700)

    @unittest.skipUnless(POSIX_MODES, "Unix owners")
    def test_rnfs_dir_of_another_user_is_refused(self):
        theirs = os.path.join(self.tmp.name, "theirs")
        os.mkdir(theirs)
        with mock.patch("os.geteuid", return_value=os.geteuid() + 1):
            with self.assertRaisesRegex(RnfsError, "belongs to another user"):
                node_module._make_private_dirs(theirs)
        self.assertFalse(os.path.exists(os.path.join(theirs, "state")))  # nothing written there

    @unittest.skipUnless(POSIX_MODES, "Unix permission bits")
    def test_rnfs_dir_that_cant_be_made_private_is_refused(self):
        loose = os.path.join(self.tmp.name, "loose")
        os.mkdir(loose)
        os.chmod(loose, 0o777)
        with mock.patch("os.chmod", side_effect=PermissionError("not permitted")):
            with self.assertRaisesRegex(RnfsError, "couldn't make .* private"):
                node_module._make_private_dirs(loose)

    def test_no_permission_fix_on_windows(self):
        # Windows' st_mode always shows others allowed: no chmod and no notice at every start
        loose = os.path.join(self.tmp.name, "loose")
        os.mkdir(loose)
        with mock.patch.object(fsutil, "IS_WINDOWS", True), mock.patch("os.chmod") as chmod, \
                mock.patch.object(node_module, "log") as log, \
                mock.patch("os.stat", return_value=mock.Mock(st_mode=0o40777)):
            node_module._make_private_dirs(loose)
        chmod.assert_not_called()
        log.assert_not_called()

    def test_fetch_succeeds_when_pins_fill_max_storage(self):
        small = self.make_node("small", N1, max_storage=256 * 1024)
        small.routing.heard(A_ID)
        mine = small.add_stream(io.BytesIO(os.urandom(300 * 1024)))["cid"]  # pins alone fill max_storage
        cached = self.a.add_stream(io.BytesIO(os.urandom(100 * 1024)))["cid"]
        big = self.a.add_stream(io.BytesIO(os.urandom(1024 * 1024)))["cid"]
        for cid in (cached, big):
            self.advertise(cid)
        b"".join(small.read(cached, for_gateway=True))
        data = b"".join(small.read(big))
        self.assertEqual(len(data), 1024 * 1024)
        out = os.path.join(self.tmp.name, "got")
        small.get(big, out)
        self.assertEqual(os.path.getsize(out), 1024 * 1024)
        # Once the fetches ended, eviction brought storage back down: cache gone, the pin kept
        self.assertTrue(small.storage.is_complete(Cid.parse(mine)))
        self.assertEqual(small.storage.cached_fetches(), [])
        self.assertLessEqual(small.storage.used_bytes(), small.storage.pinned_bytes())

    def test_fetch_too_big_for_the_disk_is_refused(self):
        big = self.a.add_stream(io.BytesIO(os.urandom(4 * CHUNK_SIZE)))["cid"]
        self.advertise(big)
        usage = shutil.disk_usage(self.tmp.name)._replace(total=1024 ** 3, free=256 * 1024 * 1024 + CHUNK_SIZE)
        with mock.patch("shutil.disk_usage", return_value=usage), self.assertRaises(StorageFull) as caught:
            b"".join(self.b.read(big, for_gateway=True))
        self.assertIn("not enough free disk space", str(caught.exception))

    def test_cache_records_saved_at_stop(self):
        cid = self.a.add_stream(io.BytesIO(b"cached"))["cid"]
        self.advertise(cid)
        b"".join(self.b.read(cid))
        state = self.b.config.state_dir
        self.assertEqual(state_rows(state, "SELECT target FROM cache"), [])  # not written on every fetch
        self.b.stop()
        self.assertEqual(state_rows(state, "SELECT target FROM cache"), [(Cid.parse(cid).binary,)])

    def test_remote_pin_quota_counts_dag_blocks(self):
        self.a.config.remote_pin_quota = 50_000
        cid = self.b.add_path(self.empty_files(400))["cid"]
        self.assertEqual(self.b.info(cid)["size"], 0)
        self.b.records.store(Cid.parse(cid).key, B_ID)
        self.a.routing.heard(B_ID)
        self.a.backend.remote_pin(IDENTITY, Cid.parse(cid), True)
        self.assertTrue(self.wait_for(lambda: not self.a._remote_reserved.get(remote_source(IDENTITY))))
        self.assertEqual(self.a.storage.pins(), {})

    def test_remote_pin_quota_counts_dag_blocks_already_stored(self):
        """A DAG over the quota is refused even when this node already holds all of it (a user pin here)."""
        self.a.config.remote_pin_quota = 50_000
        cid = self.b.add_path(self.empty_files(400))["cid"]
        self.b.records.store(Cid.parse(cid).key, B_ID)
        self.a.routing.heard(B_ID)
        self.a.pin_add(cid)
        self.assertGreater(self.a.storage.dag_bytes(Cid.parse(cid)), 50_000)
        self.a.backend.remote_pin(IDENTITY, Cid.parse(cid), True)
        self.assertTrue(self.wait_for(lambda: not self.a._remote_reserved.get(remote_source(IDENTITY))))
        self.assertEqual(self.a.storage.pins(), {Cid.parse(cid): {"user"}})

    def test_remote_pins_of_one_source_are_fetched_one_at_a_time(self):
        # Two DAGs of empty files, each within the quota but not both: fetched at once, each was checked
        # without the other's blocks and both were pinned
        self.a.config.remote_pin_quota = 30_000
        cids = []
        for count in (120, 119):  # about 20 kB of blocks each
            root = self.empty_files(count)
            os.rename(root, root + str(count))
            cids.append(Cid.parse(self.b.add_path(root + str(count))["cid"]))
            self.b.records.store(cids[-1].key, B_ID)
        self.a.routing.heard(B_ID)
        running, most = [0], [0]
        real = self.net.blocks

        def counted(node_id, cids_, on_progress=None):
            """Count /blocks requests in flight at once."""
            running[0] += 1
            most[0] = max(most[0], running[0])
            try:
                time.sleep(0.01)
                return real(node_id, cids_, on_progress)
            finally:
                running[0] -= 1

        self.net.blocks = counted
        for cid in cids:
            self.assertTrue(self.a.backend.remote_pin(IDENTITY, cid, True))
        self.assertTrue(self.wait_for(lambda: not self.a._remote_reserved.get(remote_source(IDENTITY))))
        self.assertEqual(most[0], 1)
        self.assertLessEqual(self.a.storage.source_usage(remote_source(IDENTITY)), 30_000)
        self.assertEqual(list(self.a.storage.pins()), cids[:1])

    def test_remote_pins_waiting_are_capped(self):
        # The worker's fetches are held until all five /pin requests are in: these CIDs have no provider, so
        # an unheld fetch can fail at its lookup and free a place between two requests on a slow machine
        go = threading.Event()
        real_fetch = self.a._remote_pin_fetch

        def held_fetch(cid, source):
            """Wait for the test, then fetch as the node does."""
            go.wait(10)
            return real_fetch(cid, source)

        with mock.patch.object(node_module, "REMOTE_PIN_QUEUE", 3), \
                mock.patch.object(self.a, "_remote_pin_fetch", held_fetch):
            accepted = [self.a.backend.remote_pin(IDENTITY, Cid.for_block(RAW, b"%d" % i), True) for i in range(5)]
            go.set()
        self.assertEqual(accepted, [True, True, True, False, False])
        self.assertTrue(self.wait_for(lambda: not self.a._remote_reserved.get(remote_source(IDENTITY)), 30))

    def test_remote_pin_released_while_fetching_isnt_pinned(self):
        cid = self.b.add_stream(io.BytesIO(os.urandom(2 * CHUNK_SIZE)))["cid"]
        self.b.records.store(Cid.parse(cid).key, B_ID)
        self.a.routing.heard(B_ID)
        asked, go = threading.Event(), threading.Event()
        real = self.net.blocks

        def held(node_id, cids, on_progress=None):
            """Hold every request until the test lets it go."""
            asked.set()
            go.wait(5)
            return real(node_id, cids, on_progress)

        self.net.blocks = held
        self.a.backend.remote_pin(IDENTITY, Cid.parse(cid), True)
        self.assertTrue(asked.wait(5))
        self.assertTrue(self.a.backend.remote_pin(IDENTITY, Cid.parse(cid), False))
        go.set()
        self.assertTrue(self.wait_for(lambda: not self.a._remote_reserved.get(remote_source(IDENTITY))))
        self.assertEqual(self.a.storage.pins(), {})

    def test_get_refuses_what_wont_fit_on_disk(self):
        cid = self.a.add_stream(io.BytesIO(os.urandom(3 * CHUNK_SIZE)))["cid"]
        self.advertise(cid)
        out = os.path.join(self.tmp.name, "out")
        real = shutil.disk_usage

        def destination_full(path):
            """Little free space where the output goes; the blocks' filesystem is untouched."""
            usage = real(path)
            return usage._replace(free=CHUNK_SIZE) if os.path.abspath(path) == self.tmp.name else usage

        with mock.patch("shutil.disk_usage", destination_full):
            with self.assertRaises(TooLarge):
                self.b.get(cid, out)
            with self.assertRaises(TooLarge):
                self.a.export(cid, out)
        self.assertFalse(os.path.lexists(out))
        self.assertEqual(self.b.storage.block_count(), 1)  # refused before fetching below the root

    def test_export_keeps_a_file_created_during_the_fetch(self):
        cid = self.a.add_stream(io.BytesIO(b"data"))["cid"]
        out = os.path.join(self.tmp.name, "precious")
        real = fetcher.Fetch.fetch_dag

        def racing(fetch, *args, **kwargs):
            """Someone else creates the output while the fetch runs."""
            with open(out, "w", encoding="utf-8") as f:
                f.write("someone else's file")
            return real(fetch, *args, **kwargs)

        with mock.patch.object(fetcher.Fetch, "fetch_dag", racing), self.assertRaises(ValueError):
            self.a.export(cid, out)
        with open(out, encoding="utf-8") as f:
            self.assertEqual(f.read(), "someone else's file")



class RestartTest(NodeTest):
    """State a node finds at startup."""

    def restart(self, node: RnfsNode, name: str, node_id: bytes, **settings) -> RnfsNode:
        """Stop node and start another over its directory."""
        node.stop()
        return self.make_node(name, node_id, **settings)

    def test_failed_saves_dont_stop_stopping(self):
        """Saves failing at stop (a full disk) are logged; the state database is still closed, and the node's
        directory can be used again."""
        node = self.make_node("full", N1)
        failing = mock.Mock(side_effect=statedb.StateDbError("disk full"))
        with mock.patch.object(node.records, "save", failing), mock.patch.object(node.publisher, "save", failing), \
                mock.patch.object(node.names, "_save", failing), mock.patch("rnfs.node.log") as log:
            node.stop()
        self.assertGreaterEqual(failing.call_count, 3)
        self.assertEqual(sum("saving state failed" in call.args[0] for call in log.call_args_list), 3)
        self.assertIsNone(node._db._database)
        self.assertIsNone(node.storage._db._database)
        self.make_node("full", N1).stop()

    def test_deleted_name_key_releases_its_pin_at_startup(self):
        for strategy in ("roots", "pinned"):
            with self.subTest(strategy=strategy):
                node = self.make_node(strategy, N1, provide_strategy=strategy)
                cid = node.add_stream(io.BytesIO(b"site " + strategy.encode()), pin=False)["cid"]
                node.name_create("site")
                node.name_publish("site", cid)
                kept = bytes.fromhex(node.name_create("kept")["name"])
                node.name_publish("kept", cid)
                os.remove(os.path.join(node.config.names_dir, "site"))
                node = self.restart(node, strategy, N1, provide_strategy=strategy)  # used to fail under roots
                self.assertEqual(node.storage.pins(), {Cid.parse(cid): {"name:kept"}})
                # Released after the features were set up: the name still kept isn't withdrawn
                self.assertIn(name_key(kept), node.publisher.provided_keys())
                self.assertFalse(any(name_key(kept) in keys for keys in node.publisher._withdrawals.values()))

    def test_own_records_arent_limited_per_publisher(self):
        self.assertEqual(self.a.records.own_id, A_ID)

    def test_prune_reads_cached_targets_under_the_lock(self):
        # Else a fetch recorded between the read and the prune is forgotten, and never provided under roots
        held = []
        real = self.a.storage.cached_targets

        def cached_targets():
            """Note whether the node's lock is held."""
            held.append(self.a._lock.locked())
            return real()

        with mock.patch.object(self.a.storage, "cached_targets", cached_targets):
            self.a._prune_fetched()
        self.assertEqual(held, [True])

    def test_fetched_targets_are_saved_with_the_state_and_kept_at_a_restart(self):
        cid = self.a.add_stream(io.BytesIO(b"fetched for the user"))["cid"]
        self.advertise(cid)
        self.b.get(cid, os.path.join(self.tmp.name, "out"))
        state = self.b.config.state_dir
        self.assertEqual(state_rows(state, "SELECT cid FROM fetched"), [])  # not written on each fetch, under the
        self.b._save_state()  # node's lock, but by the next state save (maintenance, gc and stop)
        self.assertEqual(state_rows(state, "SELECT cid FROM fetched"), [(Cid.parse(cid).binary,)])
        self.b = self.restart(self.b, "b", B_ID)
        self.assertEqual(self.b._eligible_targets(), {Cid.parse(cid)})
        self.assertIn(Cid.parse(cid), self.b.publisher.provided())

    def test_failed_fetched_save_is_tried_again(self):
        target = Cid.for_block(RAW, b"target")
        self.b._note_fetched(target)
        with failing_commits("database or disk is full"):
            with self.assertRaises(OSError):
                self.b._save_fetched()
        self.b._save_fetched()
        self.assertEqual(state_rows(self.b.config.state_dir, "SELECT cid FROM fetched"), [(target.binary,)])
        # Only the change is written: one forgotten target is one row deleted
        self.b._note_fetched(Cid.for_block(RAW, b"other"))
        self.b._save_fetched()
        with self.b._lock:
            self.b._fetched.discard(target)
            self.b._fetched_dirty = True
        self.b._save_fetched()
        self.assertEqual(state_rows(self.b.config.state_dir, "SELECT cid FROM fetched"),
                         [(Cid.for_block(RAW, b"other").binary,)])


class StopTest(NodeTest):
    """stop() runs every step whatever raises, and waits for the threads it ends (audit XC5)."""

    def test_a_raising_service_still_closes_the_state_and_releases_the_destination(self):
        """An embedded node whose announcer fails to stop: its state database is still closed and its rnfs.dht
        destination still released, so a later node in the process can use both."""
        node = self.make_node("embedded", N1)
        node.embedded = True
        node.destination = mock.Mock(links=[])
        node.announcer = mock.Mock(**{"stop.side_effect": RuntimeError("announcer broke")})
        with mock.patch.object(RNS.Transport, "deregister_destination") as deregister, \
                mock.patch("rnfs.node.log") as log:
            node.stop()
        deregister.assert_called_once_with(node.destination)
        self.assertIsNone(node._db._database)
        self.assertTrue(any("announcer broke" in call.args[0] for call in log.call_args_list))
        self.make_node("embedded", N1).stop()

    def test_links_close_before_the_threads_are_waited_for(self):
        """A background thread stuck on the network (the publisher connecting to a node that has gone) is freed by
        closing the links, and its last save lands while the state database is still open (found by the interop
        harness: `publishing failed: StateDbError(... is closed)` at stop)."""
        node = self.make_node("stuck", N1)
        freed = threading.Event()
        saved = []

        def stuck_publisher():
            """Waits on the network until the links close, then saves, as the publisher's loop does."""
            freed.wait(10)
            node.publisher.save()
            saved.append(node._db._database is not None)

        node.links = mock.Mock(**{"close_all.side_effect": freed.set})
        thread = threading.Thread(target=stuck_publisher, daemon=True)
        thread.start()
        node.threads.append(thread)
        with mock.patch("rnfs.node.log") as log:
            node.stop()
        self.assertEqual(saved, [True])
        self.assertFalse([call for call in log.call_args_list if "failed" in call.args[0]])

    def test_a_raising_feature_or_save_skips_nothing(self):
        """A feature's stop and a store's save raising something other than OSError: the other stores are still
        saved and the state closed."""
        node = self.make_node("broken", N1)
        cid = node.add_stream(io.BytesIO(b"cached"), pin=False)["cid"]
        node._note_fetched(Cid.parse(cid))
        with mock.patch.object(node.feature("follow"), "stop", side_effect=RuntimeError("follow broke")), \
                mock.patch.object(node.storage, "save", side_effect=ValueError("bad row")), \
                mock.patch("rnfs.node.log") as log:
            node.stop()
        logged = [call.args[0] for call in log.call_args_list]
        self.assertTrue(any("follow broke" in line for line in logged), logged)
        self.assertTrue(any("bad row" in line for line in logged), logged)
        self.assertEqual(state_rows(node.config.state_dir, "SELECT cid FROM fetched"), [(Cid.parse(cid).binary,)])
        self.assertIsNone(node._db._database)

    def test_stop_cancels_remote_pin_fetches_and_waits_for_their_workers(self):
        """A remote pin being fetched when the node stops: its fetch watches stop_event, stop waits for its worker,
        and the pin is saved as waiting, for the next start."""
        cid = Cid.parse(self.a.add_stream(io.BytesIO(os.urandom(2 * CHUNK_SIZE)))["cid"])
        self.advertise(str(cid))
        asked, go = threading.Event(), threading.Event()
        self.addCleanup(go.set)

        def held(node_id, cids, on_progress=None):
            """Hold every /blocks until the test ends, then fail it."""
            asked.set()
            go.wait(30)
            return BlocksResult("failed", unsent=list(cids))

        self.net.blocks = held
        before = set(threading.enumerate())
        self.assertTrue(self.b.backend.remote_pin(IDENTITY, cid, True))
        self.assertTrue(asked.wait(10))
        workers = [t for t in threading.enumerate() if t not in before and t.name == "rnfs-remote-pin"]
        self.assertEqual(len(workers), 1)
        started = time.monotonic()
        self.b.stop()
        self.assertFalse(workers[0].is_alive(), "stop didn't wait for the remote-pin worker")
        self.assertLess(time.monotonic() - started, node_module.STOP_TIMEOUT)
        self.assertEqual(state_rows(self.b.config.state_dir, "SELECT cid FROM remote_pins"), [(cid.binary,)])


class MakeRoomTest(NodeTest):
    """Eviction for room, as each stored response batch asks for."""

    def test_fetched_targets_are_pruned_only_when_cache_records_went(self):
        cid = Cid.parse(self.a.add_stream(io.BytesIO(os.urandom(5000)), pin=False)["cid"])  # a cached fetch
        with mock.patch.object(self.a, "_prune_fetched", wraps=self.a._prune_fetched) as prune:
            self.a._make_room(0)  # within max_storage: nothing evicted
            prune.assert_not_called()
            self.a.storage.max_storage = 1
            self.a._make_room(0)
            prune.assert_called_once()
            self.a._make_room(0)  # over max_storage still, but with nothing left to evict
            prune.assert_called_once()
        self.assertEqual(self.a._eligible_targets(), set())
        self.assertFalse(self.a.storage.has(cid))


class LookupStartTest(NodeTest):
    """Where the node's lookups start."""

    def test_nodes_that_failed_start_lookups_only_to_make_up_k(self):
        rng = random.Random(7)
        others = [bytes(rng.getrandbits(8) for _ in range(16)) for _ in range(K)]
        for node_id in others:
            self.assertTrue(self.b.routing.heard(node_id))
        failed = others[0]
        self.b.routing.failed(failed)  # the closest node to its own ID, but no longer answering
        start = self.b._lookup(failed)._start_nodes
        self.assertNotIn(failed, start)
        self.assertEqual(len(start), K)  # A and the others that haven't failed
        self.b.routing.remove(others[1])
        self.b.routing.remove(others[2])
        start = self.b._lookup(failed)._start_nodes
        self.assertIn(failed, start)  # fewer than K healthy: made up from the rest
        self.assertEqual(len(start), K - 1)

    def test_unreliable_nodes_start_lookups_only_to_make_up_k(self):
        """A node whose queries keep failing, though it keeps announcing, starts lookups only after every other."""
        rng = random.Random(8)
        others = [bytes(rng.getrandbits(8) for _ in range(16)) for _ in range(K + 1)]
        for node_id in others:
            self.assertTrue(self.b.routing.heard(node_id))
        dead = others[0]
        for _ in range(self.b.config.max_failures):
            self.b.routing.heard(dead)  # announces don't clear its failures
            self.b.routing.failed(dead)
        self.b.routing.heard(dead)
        lookup = self.b._lookup(dead)
        self.assertNotIn(dead, lookup._start_nodes)
        self.assertEqual(len(lookup._start_nodes), K)
        self.assertTrue(lookup.unreliable(dead))
        for node_id in others[1:4]:
            self.b.routing.remove(node_id)
        start = self.b._lookup(dead)._start_nodes
        self.assertEqual(start[-1], dead)  # fewer than K others: made up with it, last
        self.assertEqual(len(start), K - 1)
        self.b._answered(dead)
        self.assertFalse(self.b.routing.unreliable(dead))


class JoinTest(NodeTest):
    """Joining: bootstrap nodes first (the user's, then the defaults), then a lookup; again while too few."""

    SERVERS = [bytes([0x10 + i] * 16) for i in range(4)]
    DEAD = [bytes([0xD0 + i] * 16) for i in range(2)]  # node IDs nobody answers for
    X_ID = bytes([0xE] * 16)

    def setUp(self):
        """Four servers that know each other, and a record of every /find."""
        super().setUp()
        for i, server_id in enumerate(self.SERVERS):
            self.make_node(f"s{i}", server_id)
        for server_id in self.SERVERS:
            for other in self.SERVERS:
                self.net.nodes[server_id].routing.heard(other)
        self.finds: list[bytes] = []
        find = self.net.find
        self.net.find = lambda node_id, key: (self.finds.append(node_id), find(node_id, key))[1]
        self.defaults = mock.patch.object(node_module, "DEFAULT_BOOTSTRAP_NODES",
                                          tuple(n.hex() for n in self.SERVERS[1:]))
        self.defaults.start()
        self.addCleanup(self.defaults.stop)

    def joiner(self, **settings) -> RnfsNode:
        """The node that joins, with an empty routing table."""
        return self.make_node("x", self.X_ID, **settings)

    def test_default_list_is_well_formed(self):
        ids = [parse_node_id(text) for text in DEFAULT_BOOTSTRAP_NODES]
        self.assertTrue(ids)
        self.assertEqual(len(set(ids)), len(ids))

    def test_user_bootstrap_first_then_defaults_until_two_answer(self):
        x = self.joiner(bootstrap_nodes=[self.SERVERS[0].hex()])
        x.join()
        self.assertEqual(self.finds[0], self.SERVERS[0])
        self.assertIn(self.finds[1], self.SERVERS[1:])
        # The lookup doesn't ask the contacted bootstrap nodes again
        self.assertEqual(len(self.finds), len(set(self.finds)))
        self.assertGreaterEqual(x.routing.live_count(), 3)

    def test_unreachable_bootstrap_nodes_dont_stop_the_rest(self):
        # A fresh slow node knows no identities, and its lookups try only one unknown ID (alpha): bootstrap
        # nodes are contacted outside that budget, so two dead ones don't use it up
        x = self.joiner(bootstrap_nodes=[n.hex() for n in self.DEAD + [self.SERVERS[0]]], default_bootstrap=False)
        x.slow = True
        x._is_known = lambda node_id: False
        x.join()
        self.assertEqual(self.finds[:3], self.DEAD + [self.SERVERS[0]])
        self.assertTrue(x.routing.contains(self.SERVERS[0]))

    def test_defaults_skipped_when_turned_off_or_private(self):
        user = [self.DEAD[0].hex()]
        self.assertEqual(self.joiner(bootstrap_nodes=user, default_bootstrap=False).bootstrap_ids(), [self.DEAD[0]])
        private = self.make_node("p", bytes([0xF] * 16), bootstrap_nodes=user, private=True)
        self.assertEqual(private.bootstrap_ids(), [self.DEAD[0]])
        self.assertEqual(sorted(self.make_node("d", bytes([0xC] * 16)).bootstrap_ids()), sorted(self.SERVERS[1:]))

    def test_joins_again_while_too_few_servers_are_known(self):
        x = self.joiner(bootstrap_nodes=[self.SERVERS[0].hex()], default_bootstrap=False)
        first = self.net.nodes.pop(self.SERVERS[0])  # unreachable at startup
        with mock.patch.object(node_module, "REJOIN_DELAY", 0.05):
            thread = threading.Thread(target=x._join, daemon=True)
            thread.start()
            self.addCleanup(thread.join, 5)
            self.addCleanup(x.stop_event.set)
            self.assertTrue(x.joined.wait(5))
            self.assertEqual(x.routing.node_count(), 0)
            self.net.nodes[self.SERVERS[0]] = first
            wait_for(lambda: x.routing.live_count() >= 3, what="a later join filling the routing table")


if __name__ == "__main__":
    unittest.main()
