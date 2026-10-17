"""Tests for the mesh feature (rnfs/mesh.py, rnfs/features/peers.py and their hooks): provider ranking by
seniority, hops and bitrate; the upload budget, interface rules and quiet hours on /blocks; traffic counters
and `rnfs peers` on an offline node."""
import os
import tempfile
import threading
import types
import unittest
from unittest import mock

import RNS

from rnfs import mesh
from rnfs.config import Config
from rnfs.constants import BLOCKS_PATH, FIND_PATH, KIB, MAX_RESPONSE_SIZE
from rnfs.features import peers as peers_feature
from rnfs.network import protocol, server
from rnfs.network.announces import DhtAnnounceHandler, encode_mode
from rnfs.network.client import BUSY, FAILED, OK, BlocksResult, PeerClient
from rnfs.node import RnfsNode
from tests.support import FakeClock
from tests.test_fetcher import OWN, FakeClient, FetcherTest
from tests.test_names import FakeNetwork
from tests.test_server import FakeBackend, FakeDestination, FakeLink, FakeResource, block_of

P1, P2, P3, P4 = (bytes([i]) * 16 for i in (1, 2, 3, 4))

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
    """mesh.provider_rank: seniority, then hops, then bitrate (unknown last), then order."""

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


# Airtime policy

class TokenBucketTest(unittest.TestCase):
    """Continuous refill; a response goes if the bucket holds its size or is full."""

    def test_spend_and_refill(self):
        clock = FakeClock()
        bucket = mesh.TokenBucket(3600, 3600, clock)  # 1 byte/s
        self.assertTrue(bucket.take(3000))
        self.assertFalse(bucket.take(1000))  # 600 left
        clock.now += 400
        self.assertTrue(bucket.take(1000))
        self.assertAlmostEqual(bucket.available(), 0)
        clock.now += 10_000
        self.assertEqual(bucket.available(), 3600)  # never above capacity

    def test_large_response_allowed_when_full(self):
        clock = FakeClock()
        bucket = mesh.TokenBucket(1000, 3600, clock)
        self.assertTrue(bucket.take(5000))  # bigger than the whole budget, but the bucket was full
        self.assertLess(bucket.available(), 0)
        self.assertFalse(bucket.take(1))  # in debt
        clock.now += 3600 * 4.9
        self.assertFalse(bucket.take(1000))
        clock.now += 3600 * 1.2
        self.assertTrue(bucket.take(5000))  # full again

    def test_give_back(self):
        clock = FakeClock()
        bucket = mesh.TokenBucket(1000, 3600, clock)
        bucket.take(800)
        bucket.give_back(800)
        self.assertEqual(bucket.available(), 1000)

    def test_thread_safe(self):
        bucket = mesh.TokenBucket(10_000, 1e12)
        taken = []

        def spend():
            """Take 1 byte many times."""
            taken.append(sum(bucket.take(1) for _ in range(5000)))

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
        self.assertTrue(policy.spend(10**12))
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
        self.assertTrue(policy.spend(60 * KIB))
        self.assertEqual(policy.limit(500 * KIB), 40 * KIB - 1 * KIB)  # what's left, less framing room
        self.assertTrue(policy.spend(40 * KIB))
        self.assertEqual(policy.refusal([]), mesh.REFUSED_BUDGET)
        clock.now += 36  # 1% of an hour
        self.assertIsNone(policy.refusal([]))

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
        srv.airtime.bucket.take(149 * KIB)
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
