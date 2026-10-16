"""Tests for the mesh feature (rnfs/mesh.py, rnfs/features/peers.py and their hooks): provider ranking by
seniority, hops and bitrate; the upload budget, interface rules and quiet hours on /blocks; traffic counters
and `rnfs peers` on an offline node."""
import os
import tempfile
import threading
import types
import unittest

import RNS

from rnfs import mesh
from rnfs.config import Config
from rnfs.constants import BLOCKS_PATH, FIND_PATH, KIB, MAX_RESPONSE_SIZE
from rnfs.features import peers as peers_feature
from rnfs.network import protocol, server
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
