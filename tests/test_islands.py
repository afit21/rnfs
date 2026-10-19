"""Tests for local servers, two-level records, partition mode and the heal republish (specification section 6, "LoRa
islands"; rnfs/dht/local.py and their wiring in rnfs/node.py), on offline nodes joined by a fake network client. The
island simulation is in tests/test_topology.py."""
import os
import tempfile
import unittest

import RNS

from rnfs.config import Config
from rnfs.dht.local import LOCAL_FIRST, LOCAL_FIRST_RADIUS, MAX_JUDGED, LocalServers, judge_all
from rnfs.dht.routing import distance
from rnfs.mesh import NO_PATH, PathInfo
from rnfs.node import RnfsNode
from tests.test_names import FakeNetwork
from tests.test_node import NetClient

OWN = bytes([0xA] * 16)
LORA = PathInfo(2, "LoRa", 1200.0, ("LoRa", "RNodeInterface[LoRa]"))
FAR_LORA = PathInfo(5, "LoRa", 1200.0, ("LoRa", "RNodeInterface[LoRa]"))
HUB = PathInfo(1, "Hub", 10_000_000.0, ("Hub", "TCPInterface[Hub]"))

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node's log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


def ident(n: int) -> bytes:
    """A 16-byte ID from n."""
    return n.to_bytes(16, "big")


class LocalServersTest(unittest.TestCase):
    """The local-server set."""

    def test_note_forget_and_closest(self):
        local = LocalServers()
        for n in (1, 2, 3):
            local.note(ident(n), True)
        local.note(ident(4), False)
        self.assertEqual(len(local), 3)
        self.assertEqual((local.judged(ident(1)), local.judged(ident(4)), local.judged(ident(5))), (True, False, None))
        self.assertEqual(local.closest(ident(3), 2), [ident(3), ident(2)])
        self.assertEqual(local.closest(ident(3), 2, exclude=lambda n: n == ident(3)), [ident(2), ident(1)])
        self.assertEqual(local.closest(ident(3), 0), [])
        local.note(ident(1), False)  # its path moved
        local.forget(ident(2))  # it announced client mode
        self.assertEqual(local.nodes(), [ident(3)])
        self.assertIsNone(local.judged(ident(2)))

    def test_a_scan_replaces_the_set(self):
        local = LocalServers()
        local.note(ident(1), True)
        local.note(ident(2), True)
        local.replace({ident(2): True, ident(3): True, ident(4): False})
        self.assertEqual(sorted(local.nodes()), [ident(2), ident(3)])
        self.assertFalse(local.judged(ident(1)))  # no path within the scan any more: not local
        local.clear()
        self.assertEqual((len(local), local.judged(ident(2))), (0, None))

    def test_bounded(self):
        local = LocalServers(max_servers=4, max_judged=6)
        for n in range(10):
            local.note(ident(n), n % 2 == 0)
        self.assertEqual(local.nodes(), [ident(n) for n in (2, 4, 6, 8)])  # the least recently judged forgotten
        self.assertEqual(len(local._judged), 6)
        self.assertGreater(MAX_JUDGED, 1000)

    def test_judge_all_takes_known_servers_only(self):
        paths = {ident(1): LORA, ident(2): HUB, ident(3): LORA}
        judged = judge_all(paths, lambda n: n != ident(3), lambda path: path is LORA)
        self.assertEqual(judged, {ident(1): True, ident(2): False})


class IslandCase(unittest.TestCase):
    """Offline nodes on a NetClient, the first of them able to have local servers, whose paths come from self.paths."""

    def setUp(self):
        """A temporary directory, a network and paths nobody has yet."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = NetClient()
        self.net.sender = OWN
        self.paths: dict[bytes, PathInfo] = {}

    def make_node(self, node_id: bytes = OWN, local: bool = True, **settings) -> RnfsNode:
        """An offline node whose paths are self.paths."""
        settings.setdefault("slow", "no")
        config = Config(rnfs_dir=os.path.join(self.tmp.name, node_id.hex()), lookup_timeout=5, path_timeout=0.5,
                        **settings)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork(), local=local)
        node._path_of = lambda n: self.paths.get(n, NO_PATH)
        node._path_hops = lambda n: self.paths.get(n, NO_PATH).hops
        node._scan_paths = lambda max_hops: {n: p for n, p in self.paths.items()
                                             if max_hops is None or p.hops <= max_hops}
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def server(self, n: int, path: PathInfo) -> bytes:
        """A server (an offline node) with ID ident(n) behind path."""
        node_id = ident(n)
        self.make_node(node_id, local=False)
        self.paths[node_id] = path
        return node_id


class LocalWiringTest(IslandCase):
    """Judging servers from announces and scans; two-level records; the local-first search."""

    def test_judged_from_announces(self):
        node = self.make_node()
        near, far, hub = self.server(1, LORA), self.server(2, FAR_LORA), self.server(3, HUB)
        for n in (near, far, hub):
            node._heard_server(n)
        self.assertEqual(node.local.nodes(), [near])
        self.assertEqual(node.status()["local_servers"], 1)
        node._heard_client(near)
        self.assertEqual(node.local.nodes(), [])

    def test_named_interfaces_have_no_hop_limit(self):
        node = self.make_node(local_interfaces=["LoRa"])
        near, far, hub = self.server(1, LORA), self.server(2, FAR_LORA), self.server(3, HUB)
        for n in (near, far, hub):
            node._heard_server(n)
        self.assertEqual(sorted(node.local.nodes()), [near, far])

    def test_nothing_local_on_a_node_without(self):
        node = self.make_node(local=False)
        near = self.server(1, LORA)
        node._heard_server(near)
        self.assertEqual(len(node.local), 0)
        self.assertEqual(node._local_first(near), [])

    def test_scan_finds_known_servers_only(self):
        node = self.make_node()
        near, other = self.server(1, LORA), self.server(2, LORA)
        self.paths[ident(9)] = LORA  # a destination that isn't a server it knows of
        node.routing.heard(near)
        node.first_seen.heard(other)
        node.check_local()
        self.assertEqual(sorted(node.local.nodes()), [near, other])
        self.paths[near] = FAR_LORA
        node.check_local()  # within SCAN_INTERVAL: not scanned again
        self.assertEqual(len(node.local), 2)
        node.scan_local()
        self.assertEqual(node.local.nodes(), [other])

    def test_scan_failing_leaves_nothing_local(self):
        node = self.make_node()
        node._heard_server(self.server(1, LORA))

        def broken(max_hops):
            """The instance's RPC fails."""
            raise ConnectionError("no instance")

        node._scan_paths = broken
        node.scan_local()
        self.assertEqual(len(node.local), 0)

    def test_local_replicas_stored_and_withdrawn(self):
        """Two-level records: each key also goes to its local_replicas closest local servers, recorded so withdrawals
        reach them."""
        node = self.make_node(local_replicas=2)
        islanders = [self.server(n, LORA) for n in range(1, 5)]
        backbone = [self.server(n, HUB) for n in range(0x80, 0x8c)]
        for n in islanders + backbone:
            node._heard_server(n)
        cid = node.add_path(self.write(b"two levels"))["cid"]
        node.publisher.publish_due()
        key = node.publisher.provided().__iter__().__next__().key
        expected = sorted(islanders, key=lambda n: distance(n, key))[:2]
        stored = {node_id for node_id, keys, _ in self.net.provide_calls if key in keys}
        self.assertLessEqual(set(expected), stored)
        node.pin_rm(cid)
        node.publisher.publish_due()
        withdrawn = {node_id for node_id, _, gone in self.net.provide_calls if key in gone}
        self.assertLessEqual(set(expected), withdrawn)

    def test_no_replicas_when_zero(self):
        node = self.make_node(local_replicas=0)
        self.assertEqual(len(node.publisher._store_parts), 1)  # the senior servers only

    def write(self, data: bytes) -> str:
        """A file holding data."""
        path = os.path.join(self.tmp.name, f"file-{len(data)}")
        with open(path, "wb") as f:
            f.write(data)
        return path

    def asked_first(self, named: list[str]) -> tuple[list[bytes], list[bytes]]:
        """A slow node (alpha 1) on an island of 5 servers, behind which are 4 others, searches a key: (the local
        servers it should ask first, the /find it sent in order)."""
        asked = []
        find = self.net.find

        def recording(node_id, key):
            """/find, recorded."""
            asked.append(node_id)
            return find(node_id, key)

        self.net.find = recording
        node = self.make_node(slow="yes", local_interfaces=named)
        islanders = [self.server(n, LORA) for n in range(1, 6)]
        backbone = [self.server(n, HUB) for n in range(0x80, 0x84)]
        for n in islanders + backbone:
            node._heard_server(n)
            node.routing.heard(n)
        key = ident(0x5555)
        first = sorted(islanders, key=lambda n: distance(n, key))[:LOCAL_FIRST if named else LOCAL_FIRST_RADIUS]
        self.assertEqual(node._local_first(key), first)
        node.lookups.search(key).find_providers(lambda provider: None)
        return first, asked

    def test_search_asks_the_closest_local_server_first(self):
        first, asked = self.asked_first(["LoRa"])
        self.assertEqual(asked[:1], first)

    def test_a_hop_radius_asks_three(self):
        first, asked = self.asked_first([])
        self.assertEqual(asked[:3], first)


if __name__ == "__main__":
    unittest.main()
