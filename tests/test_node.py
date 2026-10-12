"""Tests for RnfsNode's facade and backend (rnfs/node.py), on offline nodes with a fake network client."""
import io
import os
import shutil
import stat
import tempfile
import threading
import time
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.content.cid import RAW, Cid
from rnfs.content.storage import remote_source
from rnfs import fetcher
from rnfs.errors import InvalidDag, NotFound, RnfsError, StorageFull, TooLarge
from rnfs.features.keepers import name_key
from rnfs.network.client import OK, BlocksResult
from rnfs import fsutil, node as node_module
from rnfs.node import RnfsNode
from tests.support import POSIX_MODES
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
        self.assertEqual(self.a.pin_ls(), [{"cid": cid, "sources": ["user"], "complete": True, "type": "directory",
                                            "name": os.path.abspath(os.path.join(self.tmp.name, "tree"))}])
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
                node_module._make_private_dirs(Config(rnfs_dir=theirs))
        self.assertFalse(os.path.exists(os.path.join(theirs, "state")))  # nothing written there

    @unittest.skipUnless(POSIX_MODES, "Unix permission bits")
    def test_rnfs_dir_that_cant_be_made_private_is_refused(self):
        loose = os.path.join(self.tmp.name, "loose")
        os.mkdir(loose)
        os.chmod(loose, 0o777)
        with mock.patch("os.chmod", side_effect=PermissionError("not permitted")):
            with self.assertRaisesRegex(RnfsError, "couldn't make .* private"):
                node_module._make_private_dirs(Config(rnfs_dir=loose))

    def test_no_permission_fix_on_windows(self):
        # Windows' st_mode always shows others allowed: no chmod and no notice at every start
        loose = os.path.join(self.tmp.name, "loose")
        os.mkdir(loose)
        with mock.patch.object(fsutil, "IS_WINDOWS", True), mock.patch("os.chmod") as chmod, \
                mock.patch.object(node_module, "log") as log, \
                mock.patch("os.stat", return_value=mock.Mock(st_mode=0o40777)):
            node_module._make_private_dirs(Config(rnfs_dir=loose))
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
        path = os.path.join(self.b.config.state_dir, "cache.json")
        self.assertFalse(os.path.exists(path))  # not rewritten on every fetch
        self.b.stop()
        with open(path, encoding="utf-8") as f:
            self.assertIn(cid, f.read())

    def test_remote_pin_quota_counts_dag_blocks(self):
        self.a.config.remote_pin_quota = 50_000
        cid = self.b.add_path(self.empty_files(400))["cid"]
        self.assertEqual(self.b.info(cid)["size"], 0)
        self.b.records.store(Cid.parse(cid).key, B_ID)
        self.a.routing.heard(B_ID)
        self.a.backend.remote_pin(IDENTITY, Cid.parse(cid), True)
        self.assertTrue(self.wait_for(lambda: not self.a._remote_reserved.get(remote_source(IDENTITY))))
        self.assertEqual(self.a.storage.pins(), {})

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
        self.net.blocks = lambda node_id, cids, on_progress=None: time.sleep(0.2) or BlocksResult("failed", unsent=cids)
        with mock.patch.object(node_module, "REMOTE_PIN_QUEUE", 3):
            accepted = [self.a.backend.remote_pin(IDENTITY, Cid.for_block(RAW, b"%d" % i), True) for i in range(5)]
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


if __name__ == "__main__":
    unittest.main()
