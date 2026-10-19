"""Tests for the mesh feature (rnfs/mesh.py, rnfs/features/peers.py and their hooks): provider ranking by
seniority, hops and bitrate; the upload budget, interface rules and quiet hours on /blocks; traffic counters
and `rnfs peers` on an offline node."""
import inspect
import os
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

import RNS

from rnfs import log, mesh
from rnfs.config import Config
from rnfs.constants import BLOCKS_PATH, FIND_PATH, KIB, MAX_RESPONSE_SIZE
from rnfs.features import peers as peers_feature
from rnfs.network import protocol, server
from rnfs.network.announces import DhtAnnounceHandler, encode_mode
from rnfs.network.client import BUSY, FAILED, OK, BlocksResult, PeerClient
from rnfs.node import RnfsNode
from tests.support import FakeClock, wait_for
from tests.test_fetcher import OWN, FakeClient, FetcherTest
from tests.test_names import FakeNetwork
from tests.test_server import FakeBackend, FakeDestination, FakeLink, FakeResource, block_of

P1, P2, P3, P4 = (bytes([i]) * 16 for i in (1, 2, 3, 4))


def recorder(logged: list):
    """A stand-in for rnfs.log.log appending (message, level, the type of the exception being handled) to logged."""
    def record(message, level=log.LOG_INFO):
        """Record the line and whether it was logged inside an except."""
        logged.append((message, level, sys.exc_info()[0]))
    return record

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence rnfs's log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


def paths(table: dict) -> callable:
    """A path_info stub from {node_id: (hops, bitrate)}."""
    def info(node_id: bytes) -> mesh.PathInfo:
        """The stubbed path, or nothing known."""
        if node_id not in table:
            return mesh.NO_PATH
        hops, bitrate = table[node_id]
        return mesh.PathInfo(hops, "if", bitrate)
    return info


# Provider choice

class ProviderRankTest(unittest.TestCase):
    """mesh.provider_rank: seniority, then local score band, then hops, then bitrate (unknown last), then order."""

    def test_order(self):
        cases = {
            "senior far": (True, mesh.PathInfo(hops=5), 9),
            "near fast": (False, mesh.PathInfo(hops=1, bitrate=1e6), 3),
            "near slow": (False, mesh.PathInfo(hops=1, bitrate=1200), 1),
            "near unknown bitrate": (False, mesh.PathInfo(hops=1), 0),
            "far": (False, mesh.PathInfo(hops=2, bitrate=1e9), 2),
            "unknown": (False, mesh.NO_PATH, 4),
        }
        ranked = sorted(cases, key=lambda name: mesh.provider_rank(*cases[name]))
        self.assertEqual(ranked, ["senior far", "near fast", "near slow", "near unknown bitrate", "far", "unknown"])

    def test_score_orders_within_each_seniority_group_before_hops(self):
        near, far = mesh.PathInfo(hops=1), mesh.PathInfo(hops=6)
        cases = {
            "senior far unscored": (True, far, 5, 0),
            "senior near badly scored": (True, near, 0, -2),
            "senior far well scored": (True, far, 4, 3),
            "new near best scored": (False, near, 1, 7),
            "new far scored": (False, far, 2, 1),
            "new near unscored": (False, near, 3, 0),
        }
        ranked = sorted(cases, key=lambda name: mesh.provider_rank(*cases[name]))
        self.assertEqual(ranked, ["senior far well scored", "senior far unscored", "senior near badly scored",
                                  "new near best scored", "new far scored", "new near unscored"])

    def test_nothing_known_keeps_reply_order(self):
        self.assertLess(mesh.provider_rank(False, mesh.NO_PATH, 0), mesh.provider_rank(False, mesh.NO_PATH, 1))


class FetcherOrderTest(FetcherTest):
    """The fetcher ranks providers and open links with the injected path_info."""

    def setUp(self):
        """One block on the network, held by every provider."""
        super().setUp()
        self.block = self.file(b"mesh" * 100)
        for node_id in (P1, P2, P3, P4):
            self.hold(node_id)

    def test_providers_ranked_within_seniority(self):
        self.fetcher.path_info = paths({P1: (3, None), P2: (1, 1200), P3: (1, 1_000_000)})
        self.config.provider_seniority = 0
        self.routing.heard(P4)  # senior, though nothing is known about its path
        with self.fetcher.open(self.block.cid) as fetch:
            for node_id in (P1, P2, P3, P4):
                fetch._new_provider(node_id)
            self.assertEqual([p.node_id for p in fetch._usable()], [P4, P3, P2, P1])

    def test_first_provider_asked_is_the_nearest(self):
        self.config.max_parallel_providers = 1
        self.fetcher.path_info = paths({P1: (4, 1e6), P2: (2, 1e6), P3: (1, 1200)})
        self.runs.append([P1, P2, P3])
        self.fetch_all(self.block.cid)
        self.assertEqual(self.client.requests[0][0], P3)

    def test_open_links_ranked(self):
        self.config.max_parallel_providers = 2
        self.client.links = [P1, P2, P3]  # most recently used first
        self.fetcher.path_info = paths({P1: (3, None), P3: (1, 9600)})
        self.fetch_all(self.block.cid)
        probed = {node for node, _ in self.client.requests[:2]}
        self.assertEqual(probed, {P3, P1})  # P2's path is unknown: last

    def test_open_links_paths_looked_up_together(self):
        """The open links' paths come from one path_infos call (one RPC on a shared instance), ranked as one by one;
        a node it doesn't answer for falls back to path_info, and a failing path_infos to path_info alone."""
        self.config.max_parallel_providers = 2
        self.client.links = [P1, P2, P3]
        one_by_one = paths({P1: (3, None), P3: (1, 9600)})
        calls = []

        def together(node_ids):
            """path_infos, answering for all but P3."""
            calls.append(list(node_ids))
            return {node_id: one_by_one(node_id) for node_id in node_ids if node_id != P3}

        self.fetcher.path_info = one_by_one
        self.fetcher.path_infos = together
        self.fetch_all(self.block.cid)
        self.assertEqual(calls, [[P1, P2, P3]])
        self.assertEqual({node for node, _ in self.client.requests[:2]}, {P3, P1})

    def test_path_infos_errors_fall_back_to_path_info(self):
        def broken(node_ids):
            """A batch lookup that fails."""
            raise RuntimeError("no RNS")
        self.config.max_parallel_providers = 2
        self.client.links = [P1, P2, P3]
        self.fetcher.path_info = paths({P1: (3, None), P3: (1, 9600)})
        self.fetcher.path_infos = broken
        self.fetch_all(self.block.cid)
        self.assertEqual({node for node, _ in self.client.requests[:2]}, {P3, P1})

    def test_path_info_errors_mean_unknown(self):
        def broken(node_id):
            """A lookup that fails."""
            raise RuntimeError("no RNS")
        self.fetcher.path_info = broken
        self.runs.append([P1])
        self.fetch_all(self.block.cid)
        self.assertTrue(self.storage.is_complete(self.block.cid))


HUB = "TCPInterface[Hub/10.0.0.1:4242]"
RADIO = "RNodeInterface[LoRa]"


class FakeInstance:
    """A Reticulum attached to a shared instance: path answers come by RPC, counted in calls."""

    is_connected_to_shared_instance = True

    def __init__(self, names: dict):
        """The instance's next-hop interface name per node ({node_id: str(interface)})."""
        self.names = names
        self.calls = {"next_hop": 0, "path_table": 0, "stats": 0}
        self.table_fails = False

    def get_next_hop_if_name(self, node_id: bytes) -> str:
        """One RPC: str() of the next-hop interface, "None" for a node the instance has no path to."""
        self.calls["next_hop"] += 1
        return self.names.get(node_id, "None")

    def get_path_table(self, max_hops=None) -> list[dict]:
        """One RPC: the instance's whole path table."""
        self.calls["path_table"] += 1
        if self.table_fails:
            raise ConnectionError("rpc failed")
        return [{"hash": node_id, "timestamp": 0, "via": b"\0" * 16, "hops": 2, "expires": 0, "interface": name}
                for node_id, name in self.names.items()]

    def get_interface_stats(self) -> dict:
        """One RPC: the instance's interfaces."""
        self.calls["stats"] += 1
        return {"interfaces": [{"name": HUB, "short_name": "Hub", "bitrate": 10_000_000},
                               {"name": RADIO, "short_name": "LoRa", "bitrate": 1200}]}


class RnsPathsTest(unittest.TestCase):
    """RnsPaths attached to a shared instance: next-hop interfaces cached for at most PATH_CACHE (C1)."""

    def setUp(self):
        """Local paths to P1-P3 (P4 has none), and an instance knowing P1 and P2's interfaces."""
        self.local = {P1: 1, P2: 3, P3: 2}  # node -> hops in this process's own path table
        for name, fake in (("has_path", lambda node_id: node_id in self.local),
                           ("hops_to", lambda node_id: self.local.get(node_id, mesh.PATHFINDER_M))):
            patcher = mock.patch.object(RNS.Transport, name, fake)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.instance = FakeInstance({P1: HUB, P2: RADIO})
        self.clock = FakeClock()
        self.paths = mesh.RnsPaths(self.instance, clock=self.clock)

    def test_info(self):
        self.assertEqual(self.paths.info(P1), mesh.PathInfo(1, "Hub", 10_000_000.0))
        self.assertEqual(self.paths.info(P2), mesh.PathInfo(3, "LoRa", 1200.0))
        self.assertEqual(self.paths.info(P3), mesh.PathInfo(2, None, None))  # the instance has no path
        self.assertEqual(self.paths.info(P4), mesh.NO_PATH)
        self.assertEqual(self.instance.calls["next_hop"], 3)  # none for P4: no local path, no question

    def test_rns_failure_logged_with_its_traceback(self):
        """RNS failing to answer is a bug or an RNS change: logged at LOG_WARNING inside the except (so the debug
        log has the traceback), at most once a minute; the path is unknown."""
        self.paths._failure_log = log.ThrottledLog(60.0, self.clock)
        logged = []
        with mock.patch.object(RNS.Transport, "has_path", side_effect=TypeError("renamed")), \
                mock.patch("rnfs.log.log", recorder(logged)):
            self.assertEqual(self.paths.info(P1), mesh.NO_PATH)
            self.assertEqual(self.paths.info(P2), mesh.NO_PATH)
        self.assertEqual([(level, raised) for _, level, raised in logged], [(RNS.LOG_WARNING, TypeError)])
        self.assertIn("renamed", logged[0][0])

    def test_interface_reused_for_at_most_path_cache(self):
        self.paths.info(P1)
        self.instance.names[P1] = RADIO  # the path moved to another interface
        self.clock.now += mesh.PATH_CACHE
        self.assertEqual(self.paths.info(P1).interface, "Hub")  # stale, within PATH_CACHE
        self.assertEqual(self.instance.calls["next_hop"], 1)
        self.clock.now += 0.001
        self.assertEqual(self.paths.info(P1).interface, "LoRa")  # never older than PATH_CACHE
        self.assertEqual(self.instance.calls["next_hop"], 2)

    def test_path_and_hops_always_fresh(self):
        self.paths.info(P1)
        self.local[P1] = 4
        self.assertEqual(self.paths.info(P1).hops, 4)
        del self.local[P1]
        self.assertEqual(self.paths.info(P1), mesh.NO_PATH)
        self.assertEqual(self.instance.calls["next_hop"], 1)

    def test_cache_bounded(self):
        many = [bytes([5, i]) * 8 for i in range(20)]
        self.local.update(dict.fromkeys(many, 1))
        with mock.patch.object(mesh, "MAX_CACHED_PATHS", 8):
            for node_id in many:
                self.paths.info(node_id)
            self.assertEqual(len(self.paths._names), 8)
            self.assertEqual(list(self.paths._names), many[-8:])  # the oldest are forgotten first
            self.clock.now += mesh.PATH_CACHE + 1
            self.paths.info(P1)
            self.assertEqual(list(self.paths._names), [P1])  # expired entries are dropped too

    def test_infos_reads_one_path_table(self):
        ids = [P1, P2, P3, P4]
        expected = {node_id: mesh.RnsPaths(FakeInstance(dict(self.instance.names))).info(node_id) for node_id in ids}
        self.assertEqual(self.paths.infos(ids), expected)
        # P3 has a local path the instance's table lacks: asked on its own, as info() would
        self.assertEqual(self.instance.calls, {"next_hop": 1, "path_table": 1, "stats": 1})
        self.assertEqual(self.paths.infos(ids), expected)
        self.assertEqual(self.instance.calls, {"next_hop": 1, "path_table": 1, "stats": 1})  # all cached
        self.clock.now += mesh.PATH_CACHE + 1
        self.instance.names[P1] = RADIO
        self.assertEqual(self.paths.infos(ids)[P1].interface, "LoRa")
        self.assertEqual(self.instance.calls["path_table"], 2)

    def test_infos_without_the_table_asks_each_node(self):
        self.instance.table_fails = True
        self.assertEqual(self.paths.infos([P1, P2, P4]),
                         {P1: mesh.PathInfo(1, "Hub", 10_000_000.0), P2: mesh.PathInfo(3, "LoRa", 1200.0),
                          P4: mesh.NO_PATH})
        self.assertEqual(self.instance.calls["next_hop"], 2)

    def test_own_transport_needs_no_rpc(self):
        interface = types.SimpleNamespace(name="LoRa", bitrate=1200)
        self.instance.is_connected_to_shared_instance = False
        with mock.patch.object(RNS.Transport, "next_hop_interface", lambda node_id: interface):
            self.assertEqual(self.paths.infos([P1, P2]), {P1: mesh.PathInfo(1, "LoRa", 1200.0),
                                                         P2: mesh.PathInfo(3, "LoRa", 1200.0)})
        self.assertEqual(self.instance.calls, {"next_hop": 0, "path_table": 0, "stats": 0})

    def test_hops_are_local(self):
        """hops() reads the local path table only: no RPC to the instance, for every node a lookup learns of."""
        self.assertEqual([self.paths.hops(node_id) for node_id in (P1, P2, P3, P4)], [1, 3, 2, None])
        self.local[P1] = mesh.PATHFINDER_M  # not a hop count
        self.assertIsNone(self.paths.hops(P1))
        self.assertEqual(self.instance.calls, {"next_hop": 0, "path_table": 0, "stats": 0})
        with mock.patch.object(RNS.Transport, "has_path", side_effect=TypeError("renamed")), \
                mock.patch("rnfs.log.log", recorder([])):
            self.assertIsNone(self.paths.hops(P2))  # RNS failing: no path known


class LocalPathTest(unittest.TestCase):
    """Local servers (spec section 6, "LoRa islands"): mesh.is_local_path and the path-table scan RnsPaths.scan."""

    setUp = RnsPathsTest.setUp

    def test_named_interfaces(self):
        """A named interface (or prefix) makes a path local whatever its hops or bitrate; nothing else does."""
        lora = mesh.PathInfo(7, "LoRa", 1200.0, ("LoRa", RADIO))
        hub = mesh.PathInfo(1, "Hub", 10_000_000.0, ("Hub", HUB))
        for patterns, expected in ((["LoRa"], (True, False)), (["rnode"], (True, False)), (["Hub"], (False, True))):
            self.assertEqual((mesh.is_local_path(lora, patterns, 3, 100_000),
                              mesh.is_local_path(hub, patterns, 3, 100_000)), expected)
        self.assertFalse(mesh.is_local_path(mesh.NO_PATH, ["LoRa"], 3, 100_000))
        # Without names (a path read before they were), the label still matches
        self.assertTrue(mesh.is_local_path(mesh.PathInfo(2, "LoRa", None), ["LoRa"], 3, 100_000))

    def test_hop_radius_over_a_slow_interface(self):
        """With no interface named: within local_hops over an interface below slow_bitrate."""
        cases = ((mesh.PathInfo(3, "LoRa", 1200.0), True), (mesh.PathInfo(4, "LoRa", 1200.0), False),
                 (mesh.PathInfo(1, "Hub", 10_000_000.0), False), (mesh.PathInfo(1, "LoRa", None), False),
                 (mesh.NO_PATH, False))
        for path, expected in cases:
            self.assertEqual(mesh.is_local_path(path, [], 3, 100_000), expected, path)

    def test_info_gives_the_names_rules_match(self):
        self.assertEqual(self.paths.info(P2).names, ("LoRa", RADIO))
        interface = types.SimpleNamespace(name="LoRa 868", bitrate=1200, parent_interface=None)
        self.instance.is_connected_to_shared_instance = False
        with mock.patch.object(RNS.Transport, "next_hop_interface", lambda node_id: interface):
            self.assertEqual(self.paths.info(P1).names[0], "LoRa 868")

    def test_scan_through_a_shared_instance(self):
        """One path table RPC and one interface stats RPC, whatever the number of nodes."""
        found = self.paths.scan(3)
        self.assertEqual(found, {P1: mesh.PathInfo(2, "Hub", 10_000_000.0), P2: mesh.PathInfo(2, "LoRa", 1200.0)})
        self.assertEqual(found[P2].names, ("LoRa", RADIO))
        self.assertEqual(self.instance.calls, {"next_hop": 0, "path_table": 1, "stats": 1})
        self.instance.table_fails = True
        with self.assertRaises(ConnectionError):
            self.paths.scan(3)

    def test_scan_of_its_own_transport(self):
        lora = types.SimpleNamespace(name="LoRa", bitrate=1200, parent_interface=None)
        table = {P1: [0, b"", 2, 0, [], lora, b""], P2: [0, b"", 5, 0, [], lora, b""],
                 P3: [0, b"", mesh.PATHFINDER_M, 0, [], lora, b""]}
        self.instance.is_connected_to_shared_instance = False
        with mock.patch.object(RNS.Transport, "path_table", table):
            self.assertEqual(self.paths.scan(3), {P1: mesh.PathInfo(2, "LoRa", 1200.0)})
            self.assertEqual(set(self.paths.scan(None)), {P1, P2})
        self.assertEqual(self.instance.calls, {"next_hop": 0, "path_table": 0, "stats": 0})

    def test_settings_checked(self):
        for name, value in (("local_interfaces", "LoRa"), ("local_interfaces", [""]), ("local_hops", 0),
                            ("local_hops", True), ("local_replicas", 6), ("local_replicas", -1)):
            with self.assertRaises(ValueError, msg=name):
                Config(**{name: value}).validate()
        Config(local_interfaces=["LoRa"], local_hops=1, local_replicas=0).validate()


class SlowPathsTest(unittest.TestCase):
    """mesh.SlowPaths and Traffic.slow_link: which servers a node that isn't slow asks last in its lookups."""

    def setUp(self):
        """Traffic on fake clocks, and SlowPaths over it and a hop table."""
        self.wall, self.mono = FakeClock(50000.0), FakeClock(100.0)
        self.traffic = mesh.Traffic(clock=self.wall, monotonic=self.mono)
        self.hops: dict[bytes, int] = {}
        self.slow = mesh.SlowPaths(self.traffic, Config().slow_bitrate, self.hops.get)

    def test_more_than_three_hops(self):
        self.hops.update({P1: 3, P2: 4})
        self.assertEqual([self.slow(n) for n in (P1, P2, P3)], [False, True, False])  # P3: no path, nothing known

    def test_slow_link_needs_both_rtt_and_rate(self):
        """A round trip above 1 s and a rate estimate below slow_bitrate: LoRa. A long round trip at a fast rate (I2P)
        isn't slow, nor one without a rate estimate yet, nor a short round trip at a low rate."""
        for node_id, rtt, rate in ((P1, 2.5, 1200.0), (P2, 2.5, 500_000.0), (P3, 2.5, None), (P4, 0.3, 1200.0)):
            self.traffic.measured(node_id, rtt)
            if rate is not None:
                self.traffic.rated(node_id, rate)
        self.assertEqual([self.slow(n) for n in (P1, P2, P3, P4)], [True, False, False, False])

    def test_only_the_last_link_within_a_day(self):
        self.traffic.measured(P1, 2.5)
        self.traffic.rated(P1, 1200.0)
        self.mono.now += mesh.SLOW_LINK_MEMORY
        self.assertTrue(self.slow(P1))
        self.mono.now += 1
        self.assertFalse(self.slow(P1))  # measured over a day ago: says nothing now
        self.traffic.measured(P1, 2.5)  # a new link: no rate estimate yet
        self.assertFalse(self.slow(P1))
        self.traffic.rated(P1, 1200.0)
        self.assertTrue(self.slow(P1))
        self.traffic.measured(P1, 0.2)
        self.traffic.rated(P1, 1200.0)
        self.assertFalse(self.slow(P1))

    def test_asking_doesnt_make_a_peer_recent(self):
        traffic = mesh.Traffic(max_peers=2)
        traffic.heard(P1, "server")
        traffic.heard(P2, "server")
        self.assertFalse(traffic.slow_link(P1, 100_000))
        self.assertFalse(traffic.slow_link(P3, 100_000))  # unknown: not counted either
        traffic.heard(P3, "server")
        self.assertEqual(list(traffic.peers()), [P2, P3])

    def test_without_traffic_or_hops(self):
        self.assertFalse(mesh.SlowPaths(None, 100_000)(P1))


# Airtime policy

class TokenBucketTest(unittest.TestCase):
    """Continuous refill; a response goes if the bucket holds its size (beyond what is committed) or is full."""

    def test_reserve_and_refill(self):
        clock = FakeClock()
        bucket = mesh.TokenBucket(3600, 3600, clock)  # 1 byte/s
        self.assertTrue(bucket.reserve(3000, 3000))
        self.assertFalse(bucket.reserve(1000, 1000))  # 600 left
        self.assertAlmostEqual(bucket.available(), 600)  # a refusal spends nothing
        clock.now += 400
        self.assertTrue(bucket.reserve(1000, 1000))
        self.assertAlmostEqual(bucket.available(), 0)
        clock.now += 10_000
        self.assertEqual(bucket.available(), 3600)  # never above capacity

    def test_large_response_allowed_when_full(self):
        clock = FakeClock()
        bucket = mesh.TokenBucket(1000, 3600, clock)
        self.assertTrue(bucket.reserve(5000, 5000))  # bigger than the whole budget, but the bucket was full
        self.assertLess(bucket.available(), 0)
        self.assertFalse(bucket.reserve(1, 1))  # in debt
        clock.now += 3600 * 4.9
        self.assertFalse(bucket.reserve(1000, 1000))
        clock.now += 3600 * 1.2
        self.assertTrue(bucket.reserve(5000, 5000))  # full again

    def test_reservation_then_charges(self):
        """Admitted on its whole size, a response is charged only its reservation up front, then as it is sent,
        into debt if need be."""
        clock = FakeClock()
        bucket = mesh.TokenBucket(1000, 3600, clock)
        self.assertTrue(bucket.reserve(800, 100))
        self.assertEqual(bucket.available(), 900)
        bucket.charge(700)
        self.assertEqual(bucket.available(), 200)
        bucket.charge(500)
        self.assertEqual(bucket.available(), -300)
        self.assertFalse(bucket.reserve(1, 1))

    def test_committed_counts_as_gone(self):
        clock = FakeClock()
        bucket = mesh.TokenBucket(1000, 3600, clock)
        self.assertFalse(bucket.reserve(100, 10, committed=950))  # full, but others are owed most of it
        self.assertEqual(bucket.available(), 1000)
        self.assertTrue(bucket.reserve(5000, 10, committed=0))  # full and nothing owed: any one response
        self.assertEqual(bucket.available(), 990)
        self.assertFalse(bucket.reserve(50, 10, committed=950))
        self.assertTrue(bucket.reserve(40, 10, committed=950))

    def test_give_back(self):
        clock = FakeClock()
        bucket = mesh.TokenBucket(1000, 3600, clock)
        bucket.reserve(800, 800)
        bucket.give_back(800)
        self.assertEqual(bucket.available(), 1000)
        bucket.reserve(800, 100)
        bucket.charge(700)
        bucket.give_back(5000)
        self.assertEqual(bucket.available(), 1000)  # never above capacity

    def test_thread_safe(self):
        bucket = mesh.TokenBucket(10_000, 1e12)
        taken = []

        def spend():
            """Reserve 1 byte many times."""
            taken.append(sum(bucket.reserve(1, 1) for _ in range(5000)))

        threads = [threading.Thread(target=spend) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sum(taken), 10_000)


class InterfaceRuleTest(unittest.TestCase):
    """serve_interfaces / no_serve_interfaces matching."""

    def test_matching(self):
        self.assertTrue(mesh.matches(["LoRa 868"], ["lora"]))
        self.assertFalse(mesh.matches(["TCP Server"], ["LoRa"]))
        self.assertFalse(mesh.matches(["LoRa"], [""]))

    def test_allowed(self):
        lora, tcp = ["RNode LoRa"], ["Public TCP"]
        self.assertTrue(mesh.interface_allowed(lora, [], []))
        self.assertFalse(mesh.interface_allowed(lora, [], ["RNode"]))
        self.assertTrue(mesh.interface_allowed(tcp, [], ["RNode"]))
        self.assertTrue(mesh.interface_allowed(tcp, ["Public"], []))
        self.assertFalse(mesh.interface_allowed(lora, ["Public"], []))
        self.assertFalse(mesh.interface_allowed(tcp, ["Public"], ["Public TCP"]))  # exclusion wins
        self.assertFalse(mesh.interface_allowed([], ["Public"], []))  # unknown interface, allow-list set
        self.assertTrue(mesh.interface_allowed([], [], ["RNode"]))

    def test_names_include_the_parent(self):
        parent = types.SimpleNamespace(name="Public TCP", parent_interface=None)
        spawned = types.SimpleNamespace(name="Client on Public TCP", parent_interface=parent)
        names = mesh.interface_names(spawned)
        self.assertIn("Client on Public TCP", names)
        self.assertIn("Public TCP", names)
        self.assertEqual(mesh.interface_label(spawned), "Public TCP")
        self.assertEqual(mesh.interface_names(None), [])
        self.assertIsNone(mesh.interface_label(None))


class QuietHoursTest(unittest.TestCase):
    """quiet_hours parsing and matching."""

    def test_parse(self):
        self.assertEqual(mesh.parse_quiet_hours(""), [])
        self.assertEqual(mesh.parse_quiet_hours("22:00-06:00"), [(1320, 360)])
        self.assertEqual(mesh.parse_quiet_hours("9:30-10:00, 12:00-13:15"), [(570, 600), (720, 795)])
        for bad in ("22-06", "25:00-06:00", "22:00-22:00", "22:00", "aa:bb-cc:dd", "22:00-06:00,", "1:5-2:00"):
            with self.assertRaises(ValueError, msg=bad):
                mesh.parse_quiet_hours(bad)

    def test_wrapping_range(self):
        ranges = mesh.parse_quiet_hours("22:00-06:00")
        self.assertTrue(mesh.in_quiet_hours(ranges, 23 * 60))
        self.assertTrue(mesh.in_quiet_hours(ranges, 0))
        self.assertTrue(mesh.in_quiet_hours(ranges, 5 * 60 + 59))
        self.assertFalse(mesh.in_quiet_hours(ranges, 6 * 60))
        self.assertFalse(mesh.in_quiet_hours(ranges, 12 * 60))

    def test_plain_range(self):
        ranges = mesh.parse_quiet_hours("12:00-13:00")
        self.assertTrue(mesh.in_quiet_hours(ranges, 12 * 60))
        self.assertFalse(mesh.in_quiet_hours(ranges, 13 * 60))


class AirtimePolicyTest(unittest.TestCase):
    """AirtimePolicy decisions, with an injected clock and time of day."""

    def test_defaults_refuse_nothing(self):
        policy = mesh.AirtimePolicy(Config())
        self.assertIsNone(policy.refusal([]))
        self.assertEqual(policy.limit(12345), 12345)
        self.assertTrue(policy.reserve(10**12, 10**12, committed=10**12))
        policy.charge(10**12)
        policy.refund(10**12)
        self.assertIsNone(policy.remaining())

    def test_quiet_hours(self):
        minute = [23 * 60]
        policy = mesh.AirtimePolicy(Config(quiet_hours="22:00-06:00"), minute_of_day=lambda: minute[0])
        self.assertEqual(policy.refusal(["any"]), mesh.REFUSED_QUIET)
        minute[0] = 7 * 60
        self.assertIsNone(policy.refusal(["any"]))

    def test_budget_limit(self):
        clock = FakeClock()
        policy = mesh.AirtimePolicy(Config(upload_budget=100 * KIB), clock)
        self.assertEqual(policy.limit(500 * KIB), 500 * KIB)  # full: any one response may go
        self.assertTrue(policy.reserve(60 * KIB, 60 * KIB))
        self.assertEqual(policy.limit(500 * KIB), 40 * KIB - 1 * KIB)  # what's left, less framing room
        self.assertEqual(policy.limit(500 * KIB, committed=10 * KIB), 30 * KIB - 1 * KIB)  # less what's owed
        self.assertFalse(policy.reserve(41 * KIB, 41 * KIB))
        self.assertTrue(policy.reserve(40 * KIB, 40 * KIB))
        self.assertEqual(policy.remaining(), 0)
        self.assertEqual(policy.refusal([]), mesh.REFUSED_BUDGET)
        clock.now += 36  # 1% of an hour
        self.assertIsNone(policy.refusal([]))

    def test_reserve_charge_refund(self):
        """A response is admitted on its whole size, charged its reservation, then as it is sent; what wasn't
        sent is refunded."""
        clock = FakeClock()
        policy = mesh.AirtimePolicy(Config(upload_budget=100 * KIB), clock)
        self.assertTrue(policy.reserve(60 * KIB, 1 * KIB))
        self.assertEqual(policy.remaining(), 99 * KIB)
        policy.charge(29 * KIB)
        self.assertEqual(policy.remaining(), 70 * KIB)
        policy.charge(0)
        policy.charge(-5)  # nothing: only what was sent is charged
        self.assertEqual(policy.remaining(), 70 * KIB)
        policy.refund(10 * KIB)
        self.assertEqual(policy.remaining(), 80 * KIB)
        self.assertFalse(policy.reserve(50 * KIB, 1 * KIB, committed=40 * KIB))  # 80 KiB less 40 KiB owed
        self.assertEqual(policy.remaining(), 80 * KIB)  # refused: nothing charged

    def test_validate(self):
        for settings in ({"upload_budget": 0}, {"upload_budget": True}, {"upload_budget": 1.5},
                         {"serve_interfaces": "LoRa"}, {"no_serve_interfaces": [""]}, {"quiet_hours": "late"}):
            with self.assertRaises(ValueError, msg=settings):
                Config(**settings).validate()
        Config(upload_budget=10_000, serve_interfaces=["LoRa"], quiet_hours="22:00-06:00").validate()

    def test_shared_instance_warning(self):
        shared = types.SimpleNamespace(is_connected_to_shared_instance=True)
        self.assertIsNone(mesh.shared_instance_warning(Config(), shared))
        self.assertIsNotNone(mesh.shared_instance_warning(Config(no_serve_interfaces=["LoRa"]), shared))
        self.assertIsNone(mesh.shared_instance_warning(Config(no_serve_interfaces=["LoRa"]), None))


class ServerAirtimeTest(unittest.TestCase):
    """RequestServer applies the airtime controls to /blocks only, and counts what it sends."""

    def setUp(self):
        """A server over fake RNS objects serving four 100 KiB blocks."""
        saved = RNS.Resource.AUTO_COMPRESS_MAX_SIZE
        self.addCleanup(setattr, RNS.Resource, "AUTO_COMPRESS_MAX_SIZE", saved)
        self.clock = FakeClock()
        self.big = [block_of(100 * KIB, i) for i in range(4)]
        self.backend = FakeBackend(blocks=dict(self.big))
        self.links = 0

    def make_server(self, **settings):
        """A server with these settings."""
        self.destination = FakeDestination()
        self.server = server.RequestServer(self.destination, Config(**settings), self.backend, clock=self.clock,
                                           start_monitor=False)
        return self.server

    def connect(self, interface_name: str | None = None) -> FakeLink:
        """A new incoming link attached to an interface of that name."""
        self.links += 1
        link = FakeLink(bytes([self.links]) * 16)
        if interface_name is not None:
            link.attached_interface = types.SimpleNamespace(name=interface_name, parent_interface=None)
        self.destination.links.append(link)
        self.destination.established(link)
        return link

    def ask(self, path, data, link, request_id=b"r" * 16):
        """Call path's handler as RNS would."""
        return self.destination.handlers[path][0](path, data, request_id, link.link_id, None, 0.0)

    def blocks(self, link, index=0):
        """/blocks for one 100 KiB block, its Resource then sent and completed."""
        answer = self.ask(BLOCKS_PATH, protocol.encode_blocks(MAX_RESPONSE_SIZE, [self.big[index][0]]), link)
        if answer is not False:
            resource = FakeResource(link, b"r" * 16)
            link.register_outgoing_resource(resource)
            resource.complete()
        return answer

    def test_excluded_interface_refused_but_find_served(self):
        self.make_server(no_serve_interfaces=["RNode"])
        lora, tcp = self.connect("RNode LoRa"), self.connect("TCP Server")
        self.assertIs(self.blocks(lora), False)
        self.assertIsNot(self.blocks(tcp), False)
        self.assertIsNot(self.ask(FIND_PATH, protocol.encode_find(bytes(16)), lora), False)
        snapshot = self.server.traffic.snapshot()
        self.assertEqual(snapshot["refused"][mesh.REFUSED_INTERFACE], 1)
        self.assertEqual(snapshot["totals"]["responses"], 1)
        self.assertGreater(snapshot["interfaces"]["TCP Server"]["sent"], 100 * KIB)
        self.assertEqual(self.server.incoming.in_progress(lora.link_id), (0, 0))  # no slot taken

    def test_allow_list(self):
        self.make_server(serve_interfaces=["TCP"])
        self.assertIs(self.blocks(self.connect("RNode LoRa")), False)
        self.assertIs(self.blocks(self.connect()), False)  # unknown interface: not on the list
        self.assertIsNot(self.blocks(self.connect("TCP Server")), False)

    def test_quiet_hours_refuse_blocks(self):
        srv = self.make_server(quiet_hours="22:00-06:00")
        srv.airtime.minute_of_day = lambda: 23 * 60
        link = self.connect("LoRa")
        self.assertIs(self.blocks(link), False)
        self.assertEqual(srv.traffic.snapshot()["refused"][mesh.REFUSED_QUIET], 1)
        srv.airtime.minute_of_day = lambda: 12 * 60
        self.assertIsNot(self.blocks(link), False)

    def test_upload_budget(self):
        srv = self.make_server(upload_budget=150 * KIB)
        self.assertIsNot(self.blocks(self.connect(), 0), False)  # full bucket
        self.assertIs(self.blocks(self.connect(), 1), False)  # ~50 KiB left: refused
        self.assertEqual(srv.traffic.snapshot()["refused"][mesh.REFUSED_BUDGET], 1)
        self.clock.now += 3600  # refilled
        self.assertIsNot(self.blocks(self.connect(), 1), False)
        self.assertEqual(srv.traffic.snapshot()["totals"]["responses"], 2)

    def test_missing_blocks_cost_nothing(self):
        srv = self.make_server(upload_budget=150 * KIB)
        srv.airtime.bucket.charge(149 * KIB)
        answer = self.ask(BLOCKS_PATH, protocol.encode_blocks(MAX_RESPONSE_SIZE, [block_of(10, 99)[0]]),
                          self.connect())
        self.assertEqual(protocol.parse_blocks_response(answer, 1), [None])


# Counters

class TrafficTest(unittest.TestCase):
    """mesh.Traffic: totals, per peer and interface, bounded."""

    def test_counts(self):
        traffic = mesh.Traffic()
        traffic.sent(P1, "LoRa", 1000)
        traffic.sent(None, "LoRa", 500)
        traffic.request(P1, mesh.ANSWERED, 300, "TCP")
        traffic.request(P1, mesh.FAILED)
        traffic.request(P2, mesh.REFUSED, bad=0)
        traffic.refused(mesh.REFUSED_QUIET)
        traffic.heard(P2, "client")
        peer = traffic.peer(P1)
        self.assertEqual((peer["sent"], peer["received"], peer["requests"], peer["answered"], peer["failed"]),
                         (1000, 300, 2, 1, 1))
        self.assertEqual(traffic.peer(P2)["mode"], "client")
        snapshot = traffic.snapshot()
        self.assertEqual(snapshot["totals"]["sent"], 1500)
        self.assertEqual(snapshot["totals"]["received"], 300)
        self.assertEqual(snapshot["interfaces"], {"LoRa": {"sent": 1500, "received": 0},
                                                  "TCP": {"sent": 0, "received": 300}})
        self.assertEqual(snapshot["refused"][mesh.REFUSED_QUIET], 1)

    def test_bounded(self):
        traffic = mesh.Traffic(max_peers=10, max_interfaces=3)
        for i in range(1000):
            traffic.sent(i.to_bytes(16, "big"), f"if{i}", 1)
        self.assertEqual(len(traffic.peers()), 10)
        self.assertEqual(len(traffic.snapshot()["interfaces"]), 3)
        self.assertIn((999).to_bytes(16, "big"), traffic.peers())  # the most recent are kept
        self.assertEqual(traffic.snapshot()["totals"]["sent"], 1000)

    def test_default_cap(self):
        traffic = mesh.Traffic()
        for i in range(mesh.MAX_PEERS + 50):
            traffic.heard(i.to_bytes(16, "big"), "server")
        self.assertEqual(len(traffic.peers()), mesh.MAX_PEERS)

    def test_first_heard_last_seen_and_rtt(self):
        wall, mono = FakeClock(5000.0), FakeClock(100.0)  # shown times are wall time; decisions monotonic
        traffic = mesh.Traffic(clock=wall, monotonic=mono)
        traffic.heard(P1, "server")  # an announce or a path response: heard, not seen
        wall.now, mono.now = 5010.0, 110.0
        traffic.request(P1, mesh.FAILED)  # no answer: not seen
        peer = traffic.peer(P1)
        self.assertEqual((peer["first_heard"], peer["first_counted"], peer["last_heard"], peer["last_seen"],
                          peer["rtt"], peer["rtt_at"]), (5000.0, 100.0, 5000.0, None, None, None))
        for at, record in ((120.0, lambda: traffic.request(P1, mesh.REFUSED)),  # it answered, if only False
                           (130.0, lambda: traffic.request(P1, mesh.ANSWERED)),
                           (140.0, lambda: traffic.sent(P1, None, 100))):  # it asked
            mono.now = at
            record()
            self.assertEqual(traffic.peer(P1)["last_seen"], at)
        wall.now, mono.now = 5060.0, 160.0
        traffic.measured(P1, 0.25)
        peer = traffic.peer(P1)
        self.assertEqual((peer["rtt"], peer["rtt_at"], peer["last_seen"], peer["first_heard"]),
                         (0.25, 5060.0, 160.0, 5000.0))
        wall.now = 5070.0
        traffic.measured(P2, 1)  # a new peer, first counted by its link
        self.assertEqual((traffic.peer(P2)["first_heard"], traffic.peer(P2)["rtt"]), (5070.0, 1.0))

    def test_announce_counts_by_its_emission_time(self):
        wall, mono = FakeClock(50000.0), FakeClock(100.0)
        traffic = mesh.Traffic(clock=wall, monotonic=mono)
        self.assertTrue(traffic.announced(P1, 50000.0))
        self.assertEqual(traffic.peer(P1)["last_seen"], 100.0)
        mono.now += 10
        self.assertFalse(traffic.announced(P1, 50000.0))  # the same announce again (a replay)
        self.assertFalse(traffic.announced(P1, 49000.0))  # an older one
        self.assertEqual(traffic.peer(P1)["last_seen"], 100.0)
        # Held in an announce queue, or a path answer a shared instance passes on as an announce: 3 h old
        self.assertFalse(traffic.announced(P2, wall.now - 3 * 3600))
        self.assertIsNone(traffic.peer(P2)["last_seen"])
        # Seen when emitted, less the skew allowed: at the window's edge
        self.assertTrue(traffic.announced(P3, wall.now - mesh.EMISSION_SKEW - mesh.ONLINE_WINDOW))
        self.assertEqual(traffic.peer(P3)["last_seen"], mono.now - mesh.ONLINE_WINDOW)
        self.assertTrue(mesh.online(False, traffic.peer(P3)["last_seen"], mono.now))
        self.assertFalse(traffic.announced(P4, wall.now - mesh.EMISSION_SKEW - mesh.ONLINE_WINDOW - 1))
        # A sender's clock ahead of ours: seen now, never later
        self.assertTrue(traffic.announced(P4, wall.now + 3600))
        self.assertEqual(traffic.peer(P4)["last_seen"], mono.now)
        for emitted in (None, float("nan"), float("inf"), True, "50000"):  # unknown: doesn't count
            self.assertFalse(traffic.announced(bytes([9]) * 16, emitted))

    def test_wall_clock_steps_dont_decide(self):
        wall, mono = FakeClock(50000.0), FakeClock(100.0)
        traffic = mesh.Traffic(clock=wall, monotonic=mono)
        traffic.request(P1, mesh.ANSWERED)
        wall.now -= 86400  # the wall clock goes back a day (or forward): online is decided by monotonic time
        self.assertTrue(mesh.online(False, traffic.peer(P1)["last_seen"], mono.now))
        wall.now += 2 * 86400
        self.assertTrue(mesh.online(False, traffic.peer(P1)["last_seen"], mono.now))
        self.assertFalse(mesh.online(False, mono.now + 10, mono.now))  # a negative age is offline

    def test_online_rule(self):
        now = 10000.0
        self.assertTrue(mesh.online(True, None, now))  # an open link
        self.assertFalse(mesh.online(False, None, now))  # never seen
        self.assertTrue(mesh.online(False, now - mesh.ONLINE_WINDOW, now))
        self.assertFalse(mesh.online(False, now - mesh.ONLINE_WINDOW - 1, now))
        # One announce mustn't keep a node online until the next (announce_interval)
        self.assertLess(mesh.ONLINE_WINDOW, Config().announce_interval / 10)
        self.assertGreaterEqual(mesh.ONLINE_WINDOW, Config().link_idle_timeout)


class PeerClientCountingTest(unittest.TestCase):
    """PeerClient.blocks counts outcomes and verified bytes per provider."""

    def test_counts_outcomes(self):
        links = types.SimpleNamespace(shared_peer=lambda node_id: None)
        client = PeerClient(links, Config())
        cid, data = block_of(1000, 1)
        results = iter([BlocksResult(OK, blocks={cid: data}), BlocksResult(BUSY), BlocksResult(FAILED)])
        client._blocks = lambda node_id, cids, on_progress=None: next(results)
        for _ in range(3):
            client.blocks(P1, [cid])
        peer = client.traffic.peer(P1)
        self.assertEqual((peer["requests"], peer["answered"], peer["refused"], peer["failed"], peer["received"]),
                         (3, 1, 1, 1, 1000))


class PeersFeatureTest(unittest.TestCase):
    """`rnfs peers` data on an offline node."""

    def setUp(self):
        """An offline node whose client carries traffic counters."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        client = FakeClient()
        client.traffic = mesh.Traffic()
        client.links = [P3]
        client.rate = 9600.0
        config = Config(rnfs_dir=os.path.join(self.tmp.name, "node"), slow="no", provider_seniority=0,
                        upload_budget=1000)
        self.node = RnfsNode.offline(config, OWN, client, FakeNetwork())
        self.addCleanup(self.node.stop)
        self.feature = self.node.feature(peers_feature.NAME)

    def test_lists_table_links_and_counted_nodes(self):
        self.node.routing.heard(P1)
        self.feature.traffic.request(P2, mesh.ANSWERED, 4096)
        self.feature.traffic.heard(P2, "client")
        self.node.routing.ban(P4)
        self.feature.traffic.request(P4, mesh.FAILED, bad=1)
        listed = {peer["node_id"]: peer for peer in self.feature.peers()}
        self.assertEqual(set(listed), {P1.hex(), P2.hex(), P3.hex(), P4.hex()})
        self.assertEqual(listed[P1.hex()]["mode"], "server")
        self.assertTrue(listed[P1.hex()]["in_table"] and listed[P1.hex()]["senior"])
        self.assertEqual(listed[P2.hex()]["mode"], "client")
        self.assertEqual(listed[P2.hex()]["received"], 4096)
        self.assertIsNotNone(listed[P2.hex()]["last_heard"])
        self.assertTrue(listed[P3.hex()]["link"])
        self.assertEqual(listed[P3.hex()]["rate"], 9600.0)
        self.assertTrue(listed[P4.hex()]["banned"])
        self.assertIsNone(listed[P1.hex()]["hops"])  # offline: no paths known
        lines = peers_feature.format_peers(list(listed.values()), 0)
        self.assertEqual(len(lines), 5)

    def test_listing_failures_logged_with_their_traceback(self):
        """What a listing can't read is left out and logged at LOG_WARNING inside the except (so the debug log has
        the traceback), at most once a minute for the feature."""
        clock = FakeClock()
        self.feature._failure_log = log.ThrottledLog(60.0, clock)
        logged = []

        def broken(*args):
            """A bug or an RNS change."""
            raise TypeError("renamed")
        self.node.client.open_links = broken
        with mock.patch("rnfs.log.log", recorder(logged)):
            self.assertEqual(self.feature.peers(), [])
            self.feature.peers()  # within the minute: counted, not logged
        self.assertEqual([(level, raised) for _, level, raised in logged], [(RNS.LOG_WARNING, TypeError)])
        self.assertIn("open links", logged[0][0])
        del self.node.client.open_links
        self.node.client.rate_estimate = broken
        clock.now += 60
        with mock.patch("rnfs.log.log", recorder(logged)):
            (listed,) = self.feature.peers()
        self.assertIsNone(listed["rate"])
        self.assertEqual([(level, raised) for _, level, raised in logged[1:]], [(RNS.LOG_WARNING, TypeError)])
        self.assertIn("rate estimate", logged[1][0])
        self.assertIn("(1 more since the last)", logged[1][0])

    def test_paths_from_one_lookup(self):
        asked = []

        class Paths:
            """RnsPaths' bulk lookup, recording what it is asked."""

            def infos(self, node_ids):
                """A path to P1 only."""
                asked.append(list(node_ids))
                return {node_id: mesh.PathInfo(2, "LoRa", 1200.0) if node_id == P1 else mesh.NO_PATH
                        for node_id in node_ids}
        self.node.paths = Paths()
        self.node.routing.heard(P1)
        listed = {peer["node_id"]: peer for peer in self.feature.peers()}
        self.assertEqual(asked, [[P1, P3]])
        self.assertEqual([listed[P1.hex()][key] for key in ("hops", "interface", "bitrate")], [2, "LoRa", 1200.0])
        self.assertIsNone(listed[P3.hex()]["hops"])
        self.assertEqual(list(listed), [P1.hex(), P3.hex()])  # nearest first

    def test_announces_counted_once_by_the_node(self):
        # S8: the node's rnfs.dht announce handler counts each announce; the feature registers none of its own
        self.node.reticulum = object()
        self.addCleanup(setattr, self.node, "reticulum", None)
        with mock.patch.object(RNS.Transport, "register_announce_handler") as register:
            self.feature.start()
        register.assert_not_called()
        handler = DhtAnnounceHandler(self.node._heard_server, self.node._heard_client, self.node.routing.is_banned,
                                     self.node.node_id)  # as RnfsNode._start_network wires it
        self.node.routing.ban(P4)
        with mock.patch.object(self.feature.traffic, "heard", wraps=self.feature.traffic.heard) as heard:
            for node_id, server in ((P1, True), (P2, False), (P4, True), (OWN, True)):
                handler.received_announce(node_id, None, encode_mode(server))
        self.assertEqual(heard.call_args_list, [mock.call(P1, "server"), mock.call(P2, "client")])
        listed = {peer["node_id"]: peer for peer in self.feature.peers()}
        self.assertEqual((listed[P1.hex()]["mode"], listed[P1.hex()]["in_table"]), ("server", True))
        self.assertEqual((listed[P2.hex()]["mode"], listed[P2.hex()]["in_table"]), ("client", False))
        self.assertIsNotNone(listed[P2.hex()]["last_heard"])

    def clocks(self) -> tuple[FakeClock, FakeClock]:
        """Fake wall and monotonic clocks for the feature's Traffic, set apart so neither stands in for the other."""
        wall, mono = FakeClock(50000.0), FakeClock(100.0)
        self.feature.traffic.clock, self.feature.traffic.monotonic = wall, mono
        return wall, mono

    def advance(self, wall: FakeClock, mono: FakeClock, seconds: float):
        """Move both clocks on."""
        wall.now += seconds
        mono.now += seconds

    def test_announce_not_path_response_shows_it_up(self):
        # A transport node may answer a path request from its path table long after the node went away
        wall, mono = self.clocks()
        handler = DhtAnnounceHandler(self.node._heard_server, self.node._heard_client, self.node.routing.is_banned,
                                     self.node.node_id, on_announced=self.node._announced)  # as _start_network
        emitted = {P1: wall.now, P2: wall.now, P4: wall.now - 3 * 3600}
        with mock.patch("rnfs.node.RnsNetwork.emitted_time", side_effect=lambda node_id: emitted.get(node_id)):
            handler.received_announce(P1, None, encode_mode(True), b"h" * 32, True)
            self.assertEqual(self.feature.traffic.peer(P1)["mode"], "server")
            self.assertIsNone(self.feature.traffic.peer(P1)["last_seen"])
            handler.received_announce(P2, None, encode_mode(False), b"h" * 32, False)
            self.assertEqual(self.feature.traffic.peer(P2)["last_seen"], mono.now)
            # Attached to a shared instance, a path answer comes as an announce (RNS 1.5.6 passes it on with no
            # context), and an announce queue may deliver one hours late: its emission time tells
            handler.received_announce(P4, None, encode_mode(True), b"h" * 32, False)
            self.assertIsNone(self.feature.traffic.peer(P4)["last_seen"])
            handler.received_announce(P3, None, encode_mode(True), b"h" * 32, False)  # emission unreadable
            self.assertIsNone(self.feature.traffic.peer(P3)["last_seen"])
        # RNS passes both only to a handler with five parameters
        self.assertEqual(len(inspect.signature(handler.received_announce).parameters), 5)

    def test_first_heard_rtt_and_online(self):
        wall, mono = self.clocks()
        traffic = self.feature.traffic
        self.node.routing.heard(P1)  # in the table, never counted
        traffic.heard(P2, "client")
        traffic.announced(P2, wall.now)
        traffic.measured(P4, 0.4)
        self.advance(wall, mono, mesh.ONLINE_WINDOW + 1)
        traffic.request(P2, mesh.ANSWERED)
        listed = {peer["node_id"]: peer for peer in self.feature.peers()}
        p1, p2, p3, p4 = (listed[node_id.hex()] for node_id in (P1, P2, P3, P4))
        self.assertEqual(p1["first_heard"], wall.now - p1["seniority"])  # entered the table then
        self.assertEqual((p1["rtt"], p1["rtt_at"], p1["online"]), (None, None, False))
        self.assertEqual((p2["first_heard"], p2["online"]), (50000.0, True))  # answered just now
        self.assertEqual((p3["first_heard"], p3["online"]), (None, True))  # an open link, nothing counted
        self.assertEqual((p4["first_heard"], p4["rtt"], p4["rtt_at"], p4["online"]),
                         (50000.0, 0.4, 50000.0, False))  # its link closed longer than ONLINE_WINDOW ago
        # Every field before 0.2.0 is still there
        self.assertEqual(set(p1), {"node_id", "mode", "in_table", "seniority", "senior", "hops", "interface",
                                   "bitrate", "link", "rate", "last_heard", "last_active", "banned", "sent",
                                   "received", "requests", "answered", "refused", "failed", "bad",
                                   "first_heard", "rtt", "rtt_at", "online"})

    def test_first_heard_from_rns_cache(self):
        # RNS's announce cache heard it by then, before this run counted it: the earlier time
        self.node.reticulum = object()
        self.addCleanup(setattr, self.node, "reticulum", None)
        self.clocks()
        self.feature.traffic.heard(P2, "client")
        with mock.patch.dict(RNS.Identity.known_destinations, {P2: [40000.0, b"", b"", None]}), \
                mock.patch.object(RNS.Identity, "recall_app_data", return_value=None):
            listed = {peer["node_id"]: peer for peer in self.feature.peers()}
        self.assertEqual(listed[P2.hex()]["first_heard"], 40000.0)

    def peer_events(self) -> list[tuple[str, str, bool]]:
        """The node's "peer" events since the last call, as (node ID, change, online), sorted."""
        found = self.node.events.since(getattr(self, "_seen", 0), kinds=["peer"])
        self._seen = found["next"]
        return sorted((event["node_id"], event["change"], event["online"]) for event in found["events"])

    def test_peer_events(self):
        wall, mono = self.clocks()
        traffic = self.feature.traffic
        P5, P6 = bytes([5]) * 16, bytes([6]) * 16
        self.node.routing.heard(P1)
        traffic.announced(P2, wall.now)
        self.feature.check()  # the first records the states: no flood at start
        self.assertEqual(self.peer_events(), [])
        self.advance(wall, mono, 1)
        self.node.routing.heard(P5)  # as seeded from RNS's announce cache: not heard this run, so no event
        traffic.announced(P4, wall.now)
        traffic.heard(P6, "client")  # a path response only: new, but not shown up
        self.feature.check()
        self.assertEqual(self.peer_events(), [(P4.hex(), "new", True), (P6.hex(), "new", False)])
        wall.now -= 86400  # a wall clock step back decides nothing
        self.feature.check()  # nothing changed
        self.assertEqual(self.peer_events(), [])
        wall.now += 86400
        self.advance(wall, mono, mesh.ONLINE_WINDOW - 0.5)  # P2's window has passed, P4's not yet
        self.feature.check()
        self.assertEqual(self.peer_events(), [(P2.hex(), "offline", False)])
        self.advance(wall, mono, 1)
        traffic.request(P2, mesh.REFUSED)  # it answered
        self.node.client.links = []  # P3's link closed
        self.feature.check()
        self.assertEqual(self.peer_events(), [(P2.hex(), "online", True), (P3.hex(), "offline", False),
                                              (P4.hex(), "offline", False)])

    def test_events_dont_follow_the_listings_cut(self):
        # A routing table beyond MAX_PEERS cuts the listing; an online peer cut from it isn't offline
        wall, mono = self.clocks()
        self.node.client.links = []
        self.feature.traffic.request(P2, mesh.ANSWERED)
        self.feature.check()
        with mock.patch.object(mesh, "MAX_PEERS", 3):
            for i in range(10, 15):
                self.node.routing.heard(bytes([i]) * 16)
            self.assertNotIn(P2.hex(), {peer["node_id"] for peer in self.feature.peers()})
            self.feature.check()
            self.assertEqual(self.peer_events(), [])
            self.advance(wall, mono, mesh.ONLINE_WINDOW + 1)
            self.feature.check()
        self.assertEqual(self.peer_events(), [(P2.hex(), "offline", False)])

    def test_events_thread_runs_and_stops(self):
        with mock.patch.object(peers_feature, "CHECK_INTERVAL", 0.05):
            self.feature.start()
            wait_for(lambda: self.feature._states is not None, what="the first check")
            self.feature.traffic.request(P4, mesh.ANSWERED)
            wait_for(lambda: self.peer_events(), what="the new peer's event")
        self.feature.stop(time.monotonic() + 5)
        self.assertTrue(self.feature._join(time.monotonic()))

    def test_recalled_mode_doesnt_mark_used(self):
        # A listing is a check, not a contact (C2)
        self.node.reticulum = object()
        with mock.patch.object(RNS.Identity, "recall_app_data", return_value=None) as recall:
            self.assertIsNone(self.feature._recalled_mode(P1))
        recall.assert_called_once_with(P1, _no_use=True)

    def test_status_and_stats(self):
        status = self.node.status()
        for key in ("bytes_sent", "bytes_received", "blocks_refused_budget", "blocks_refused_interface",
                    "blocks_refused_quiet_hours"):
            self.assertEqual(status[key], 0, key)
        stats = self.feature.stats()
        self.assertEqual(stats["upload_budget"], 1000)
        self.assertIn("interfaces", stats)
        self.assertTrue(peers_feature.format_stats(stats))

    def test_routes_and_commands(self):
        self.assertEqual(set(peers_feature.FEATURE.api_routes()), {("GET", "/api/peers"), ("GET", "/api/stats")})
        from rnfs import cli
        self.assertTrue(cli.build_parser().parse_args(["peers", "--json"]).json)
        self.assertFalse(cli.build_parser().parse_args(["stats"]).json)

    def test_table_rtt_and_online_columns(self):
        self.feature.traffic.measured(P2, 0.085)
        listed = {peer["node_id"]: peer for peer in self.feature.peers()}
        header, *lines = peers_feature.format_peers(sorted(listed.values(), key=lambda p: p["node_id"]), 0)
        # The older columns keep their places; RTT and ONLINE come last
        self.assertTrue(header.startswith("NODE"))
        self.assertTrue(header.endswith("FAIL     RTT ONLINE"), header)
        self.assertEqual([line.split()[-2:] for line in lines], [["85ms", "yes"], ["-", "yes"]])
        # A daemon before 0.2.0 sends neither field
        old = {key: value for key, value in listed[P3.hex()].items() if key not in ("rtt", "rtt_at", "online")}
        self.assertEqual(peers_feature.format_peers([old], 0)[1].split()[-2:], ["-", "-"])
        self.assertEqual(peers_feature.human_rtt(2.43), "2.4s")
        self.assertEqual(peers_feature.human_rtt(float("nan")), "-")

    def test_human_formats(self):
        self.assertEqual(peers_feature.human_size(None), "-")
        self.assertEqual(peers_feature.human_size(999), "999 B")
        self.assertEqual(peers_feature.human_size(1500), "1.5 KB")
        self.assertEqual(peers_feature.human_rate(1200), "1.2 kbps")
        self.assertEqual(peers_feature.human_age(59), "59s")
        self.assertEqual(peers_feature.human_age(3 * 3600 + 5), "3h")
        self.assertEqual(peers_feature.human_age(2 * 86400), "2d")


if __name__ == "__main__":
    unittest.main()
