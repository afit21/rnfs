"""Tests for RnfsNode's facade and backend (rnfs/node.py), on offline nodes with a fake network client."""
import io
import os
import stat
import tempfile
import time
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.content.cid import RAW, Cid
from rnfs.content.storage import USER, remote_source
from rnfs.errors import InvalidDag, NotFound
from rnfs.network.client import OK, BlocksResult
from rnfs.node import RnfsNode

try:
    from tests.test_names import FakeNetwork
except ImportError:  # run from inside tests/
    from test_names import FakeNetwork

A_ID, B_ID, N1 = bytes([0xA] * 16), bytes([0xB] * 16), bytes([1] * 16)
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

    def find(self, node_id, key):
        """Ask a node's backend as /find would."""
        node = self.nodes.get(node_id)
        if node is None or not node.backend.is_dht_server():
            return None
        return node.backend.find(key)

    def provide(self, node_id, keys, withdrawn):
        """Record, and pass to the node's backend as /provide would (as the sender is unknown here, B)."""
        self.provide_calls.append((node_id, list(keys), list(withdrawn)))
        node = self.nodes.get(node_id)
        return node is not None and node.backend.provide(self.sender, keys, withdrawn)

    def blocks(self, node_id, cids):
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
        with open(script, "w") as f:
            f.write("#!/bin/sh\necho hi\n")
        os.chmod(script, 0o755)
        with open(os.path.join(root, "sub", "deeper", "note.txt"), "w") as f:
            f.write("deep")
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
        self.assertEqual((info["type"], info["executable"]), ("file", True))
        names = [(e["name"], e["kind"]) for e in self.a.ls(cid)]
        self.assertEqual(names, [("big.bin", "file"), ("sub", "directory")])
        self.assertEqual(b"".join(self.a.read(f"{cid}/big.bin", offset=70000, length=1000)),
                         self.big[70000:71000])
        self.assertTrue(self.a.is_local(cid))
        self.assertFalse(self.a.is_local(str(Cid.for_block(RAW, b"elsewhere"))))
        self.assertEqual(self.a.pin_ls(), [{"cid": cid, "sources": ["user"], "complete": True}])
        self.assertEqual(self.a.stat(cid)["blocks"], self.a.stat(cid)["local_blocks"])
        self.assertIn(Cid.parse(cid), self.a.publisher.provided())
        self.assertEqual(self.a.status()["pins"], 1)

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
        self.assertTrue(os.stat(os.path.join(out, "sub", "run.sh")).st_mode & stat.S_IXUSR)
        single = os.path.join(self.tmp.name, "note.txt")
        self.b.get(f"{cid}/sub/deeper/note.txt", single)
        with open(single) as f:
            self.assertEqual(f.read(), "deep")
        self.assertEqual(self.b.storage.cached_targets() >= {Cid.parse(cid)}, True)
        # With the pinned strategy, B doesn't provide or serve what it downloaded
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

    def test_invalid_root_reported(self):
        self.b.fetcher.mark_invalid(Cid.for_block(RAW, b"bad"))
        with self.assertRaises(InvalidDag):
            self.b.info(str(Cid.for_block(RAW, b"bad")))


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
        self.a.pin_rm(str(cid))
        self.assertIsNone(self.a.backend.served_block(cid))

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
        self.assertIn(Cid.parse(cid), self.a.publisher.provided())
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


if __name__ == "__main__":
    unittest.main()
