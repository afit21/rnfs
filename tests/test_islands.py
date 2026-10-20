"""Tests for local servers, two-level records, partition mode and the heal republish (specification section 6, "LoRa
islands"; rnfs/dht/local.py and their wiring in rnfs/node.py), on offline nodes joined by a fake network client. The
island simulation is in tests/test_topology.py."""
import os
import tempfile
import unittest

import RNS

from rnfs import publisher as publisher_module
from rnfs.config import Config
from rnfs.dht.local import (FAILURES_TO_ENTER, GAP_SAMPLES, LOCAL_FIRST, LOCAL_FIRST_RADIUS, MAX_JUDGED, MIN_GAPS,
                            MIN_SILENCE, PROBE_INTERVAL, SILENCE_FACTOR, SLOW_PROBE_INTERVAL, SLOW_PROBE_MAX,
                            LocalServers, PartitionMode, judge_all)
from rnfs.dht.lookups import SLOW_DISJOINT_PATHS
from rnfs.dht.routing import distance
from rnfs.mesh import NO_PATH, PathInfo
from rnfs.node import RnfsNode
from tests.support import FakeClock
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


class PartitionModeTest(unittest.TestCase):
    """PartitionMode's rules, on a fake clock: servers below 0x100 are local."""

    def setUp(self):
        """Not in partition mode; changes recorded."""
        self.clock, self.wall = FakeClock(), FakeClock(1_700_000_000.0)
        self.changes: list[bool] = []
        self.mode = self.make()

    def make(self, slow: bool = False) -> PartitionMode:
        """A PartitionMode on the fake clocks."""
        return PartitionMode(lambda n: int.from_bytes(n, "big") < 0x100, slow, self.changes.append, self.clock,
                             self.wall)

    def test_three_off_island_failures_in_a_row(self):
        """With nothing heard from beyond the island to probe, the third failure in a row enters it at once."""
        for n in range(FAILURES_TO_ENTER - 1):
            self.mode.failed(ident(0x200 + n))
        self.mode.failed(ident(1))  # a local failure doesn't count
        self.mode.answered(ident(2))  # nor does a local answer reset the count
        self.assertFalse(self.mode.active)
        self.mode.failed(ident(0x300))
        self.assertTrue(self.mode.active)
        self.assertEqual(self.changes, [True])
        self.assertEqual(self.mode.state(), {"partition": True, "partition_since": self.wall.now})
        self.assertTrue(self.mode.allows(ident(1)))
        self.assertFalse(self.mode.allows(ident(0x200)))

    def test_three_failures_ask_for_a_probe(self):
        """With an off-island server that answered, the failures ask for a probe of it (on_probe), and partition mode
        starts only if the probe fails."""
        probes = []
        self.mode.on_probe = lambda: probes.append(True)
        self.mode.answered(ident(0x400))
        for n in range(FAILURES_TO_ENTER):
            self.mode.failed(ident(0x200 + n))
        self.assertEqual((self.mode.active, self.mode.probe_due(), probes), (False, True, [True]))
        self.mode.failed(ident(0x210))  # no second request while the first is due
        self.assertEqual(probes, [True])
        self.assertEqual(self.mode.probe_target(), ident(0x400))
        self.mode.probed(ident(0x400), True)
        self.assertFalse(self.mode.active or self.mode.probe_due())
        for n in range(FAILURES_TO_ENTER):
            self.mode.failed(ident(0x200 + n))
        self.mode.probed(self.mode.probe_target(), False)
        self.assertTrue(self.mode.active)
        self.assertEqual(self.changes, [True])

    def test_only_known_servers_failures_count(self):
        mode = PartitionMode(lambda n: False, known=lambda n: n != ident(0x666), clock=self.clock)
        for _ in range(2 * FAILURES_TO_ENTER):
            mode.failed(ident(0x666))
        self.assertFalse(mode.active or mode.probe_due())
        self.assertIsNone(mode.probe_target())

    def test_failures_for_want_of_a_path_dont_count(self):
        pathless = {ident(0x200 + n) for n in range(FAILURES_TO_ENTER)}
        mode = PartitionMode(lambda n: False, clock=self.clock, no_path=pathless.__contains__,
                             on_change=self.changes.append)
        mode.answered(ident(0x400))  # a server to probe
        for node_id in sorted(pathless):
            mode.failed(node_id)
        self.assertFalse(mode.active or mode.probe_due())
        # A link failure counts, as before; and a probe's outcome counts whatever the reason
        for n in range(FAILURES_TO_ENTER):
            mode.failed(ident(0x300 + n))
        self.assertTrue(mode.probe_due())
        mode.probed(mode.probe_target(), False)
        self.assertTrue(mode.active)

    def test_an_off_island_answer_resets_the_count(self):
        self.mode.failed(ident(0x200))
        self.mode.failed(ident(0x201))
        self.mode.answered(ident(0x202))
        self.mode.failed(ident(0x203))
        self.mode.failed(ident(0x204))
        self.assertFalse(self.mode.active or self.mode.probe_due())

    def enter(self, mode: PartitionMode | None = None):
        """Enter partition mode by failures, and the probe they ask for failing if there is one to probe."""
        mode = mode or self.mode
        for n in range(FAILURES_TO_ENTER):
            mode.failed(ident(0x200 + n))
        if not mode.active:
            mode.probed(mode.probe_target(), False)
        self.assertTrue(mode.active)

    def test_ended_by_an_off_island_answer_or_announce(self):
        self.enter()
        self.mode.answered(ident(1))  # local: no
        self.assertTrue(self.mode.active)
        self.mode.answered(ident(0x400))
        self.assertEqual((self.mode.active, self.changes), (False, [True, False]))
        self.assertEqual(self.mode.state(), {"partition": False, "partition_since": None})
        self.enter()
        self.mode.announced(ident(0x401))
        self.assertEqual((self.mode.active, self.changes, self.mode.entered), (False, [True, False, True, False], 2))

    def hear(self, gaps: list[float]):
        """Fresh off-island announces at these gaps."""
        for gap in gaps:
            self.clock.now += gap
            self.mode.announced(ident(0x500))

    def test_silence_counts_over_four_times_the_usual_gap(self):
        self.hear([0.0] + [60.0] * (MIN_GAPS - 1))
        self.assertIsNone(self.mode.silence_limit())  # too few gaps measured
        self.hear([60.0])
        self.assertEqual(self.mode.silence_limit(), MIN_SILENCE)  # 4 × 60 s is under the floor
        self.hear([1200.0] * GAP_SAMPLES)  # sparse: one every 20 minutes
        self.assertEqual(self.mode.silence_limit(), SILENCE_FACTOR * 1200.0)
        self.clock.now += SILENCE_FACTOR * 1200.0
        self.assertFalse(self.mode.probe_due())
        self.clock.now += 1
        self.assertTrue(self.mode.probe_due())
        self.assertFalse(self.mode.active)  # silence alone doesn't enter: a probe must fail

    def test_silence_then_a_probe(self):
        self.hear([0.0] + [600.0] * MIN_GAPS)
        self.clock.now += 4 * 600.0 + 1
        target = self.mode.probe_target()
        self.assertEqual(target, ident(0x500))
        self.mode.probed(target, True)  # answered: connected after all, and the silence starts again
        self.assertFalse(self.mode.active or self.mode.probe_due())
        self.clock.now += 4 * 600.0 + 1
        self.mode.probed(self.mode.probe_target(), False)
        self.assertTrue(self.mode.active)

    def test_gaps_spanning_partition_mode_are_not_usual(self):
        self.hear([0.0] + [600.0] * MIN_GAPS)
        self.enter()
        self.hear([50_000.0])  # ends it; that gap isn't a sample
        self.assertEqual(self.mode.silence_limit(), 4 * 600.0)

    def test_probes_in_partition_mode(self):
        self.mode.announced(ident(0x600))
        self.mode.answered(ident(0x601))
        self.enter()  # the failures' probe went to 0x601, the most recent, and failed
        self.assertFalse(self.mode.probe_due())
        self.clock.now += PROBE_INTERVAL
        self.assertTrue(self.mode.probe_due())
        targets = [self.mode.probe_target() for _ in range(4)]
        # In turn the recently heard or answered, those that failed since last; never 0x200-0x202, which only ever
        # failed (a reply can name made-up IDs)
        self.assertEqual(targets, [ident(0x601), ident(0x600), ident(0x601), ident(0x600)])
        self.mode.probed(targets[0], False)
        self.assertFalse(self.mode.probe_due())
        self.clock.now += PROBE_INTERVAL
        self.assertTrue(self.mode.probe_due())  # fast nodes: a fixed interval
        self.mode.probed(ident(0x202), True)
        self.assertFalse(self.mode.active)

    def test_slow_nodes_probe_rarely(self):
        mode = self.make(slow=True)
        self.enter(mode)
        waits = []
        for _ in range(6):
            start = self.clock.now
            while not mode.probe_due():
                self.clock.now += 60
            waits.append(self.clock.now - start)
            mode.probed(ident(0x200), False)
        self.assertEqual(waits, [SLOW_PROBE_INTERVAL, 2 * SLOW_PROBE_INTERVAL, 4 * SLOW_PROBE_INTERVAL,
                                 SLOW_PROBE_MAX, SLOW_PROBE_MAX, SLOW_PROBE_MAX])

    def test_no_target(self):
        self.hear([0.0] + [600.0] * MIN_GAPS)
        self.mode._recent.clear()
        self.clock.now += 3000
        self.assertIsNone(self.mode.probe_target())
        self.mode.no_target()
        self.assertFalse(self.mode.probe_due())


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

    def test_a_local_server_no_link_reached_is_tried_once_more(self):
        """A local replica is one of few, and over a radio one lost packet loses its store: a local server no link
        could be opened to is tried again at once; an off-island one isn't (K servers, and partition mode counts
        its failures)."""
        node = self.make_node(local_replicas=1)
        islander = self.server(1, LORA)
        backbone = [self.server(n, HUB) for n in range(0x80, 0x8c)]
        for n in [islander] + backbone:
            node._heard_server(n)
        provide = self.net.provide
        lost = {islander}  # its first link request is lost
        broken = backbone[0]  # never reached

        def lossy(node_id, keys, withdrawn):
            """No link to islander the first time, nor ever to broken."""
            if node_id in lost or node_id == broken:
                lost.discard(node_id)
                self.net.provide_calls.append((node_id, list(keys), list(withdrawn)))
                return set()
            return provide(node_id, keys, withdrawn)

        self.net.provide = lossy
        node.add_path(self.write(b"one replica, one loss"))
        node.publisher.publish_due()
        item = next(iter(node.publisher.provided()))
        tries = [node_id for node_id, keys, _ in self.net.provide_calls if item.key in keys]
        self.assertEqual(tries.count(islander), 2)
        self.assertEqual(tries.count(broken), 1)
        recorded = node.publisher.states()[item]["nodes"]
        self.assertIn(islander, recorded)
        self.assertNotIn(broken, recorded)

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


class PartitionWiringTest(IslandCase):
    """Partition mode on a node: entered from its lookups' failures, lookups and publishes local only, ended by a
    probe or an announce."""

    def setUp(self):
        """A slow island node knowing 4 local servers and 8 behind the gateway, all in its routing table."""
        super().setUp()
        self.node = self.make_node(slow="yes", local_interfaces=["LoRa"])
        self.islanders = [self.server(n, LORA) for n in range(1, 5)]
        self.backbone = [self.server(n, HUB) for n in range(0x80, 0x88)]
        for n in self.islanders + self.backbone:
            self.node._heard_server(n)
            self.node.routing.heard(n)
        self.asked: list[bytes] = []
        find = self.net.find

        def recording(node_id, key):
            """/find, recorded."""
            self.asked.append(node_id)
            return find(node_id, key)

        self.net.find = recording
        self.hidden: dict[bytes, RnfsNode] = {}

    def cut(self):
        """The gateway goes down: the backbone's servers can't be reached."""
        for n in self.backbone:
            self.hidden[n] = self.net.nodes.pop(n)

    def heal(self):
        """The gateway is back."""
        self.net.nodes.update(self.hidden)

    def off_island(self) -> int:
        """/find sent to servers behind the gateway."""
        return sum(n in self.backbone for n in self.asked)

    def test_entered_by_failures_then_lookups_stay_local(self):
        self.cut()
        self.node.lookups.search(ident(0x4444)).find_providers(lambda provider: None)
        self.assertTrue(self.node.partition.active)
        # Then no more were asked, but those already out on the fallback's paths when it began (they run at once)
        self.assertGreaterEqual(self.off_island(), FAILURES_TO_ENTER)
        self.assertLessEqual(self.off_island(), FAILURES_TO_ENTER + SLOW_DISJOINT_PATHS)
        status = self.node.status()
        self.assertTrue(status["partition"])
        self.assertIsNotNone(status["partition_since"])
        self.asked.clear()
        self.node.lookups.search(ident(0x9999)).find_providers(lambda provider: None)
        self.assertEqual(self.off_island(), 0)
        self.assertTrue(self.asked)
        self.assertLessEqual(set(self.asked), set(self.islanders))

    def test_publishing_in_partition_mode_stays_on_the_island(self):
        self.cut()
        self.node.lookups.search(ident(0x4444)).find_providers(lambda provider: None)
        self.assertTrue(self.node.partition.active)
        self.net.provide_calls.clear()
        self.node.add_path(self.write(b"cut off"))
        self.node.publisher.publish_due()
        stored = {node_id for node_id, keys, _ in self.net.provide_calls if keys}
        self.assertTrue(stored)
        self.assertLessEqual(stored, set(self.islanders))
        # Withdrawals for servers behind the gateway wait for the partition to end
        self.node.publisher._withdrawals[self.backbone[0]] = {ident(0x77)}
        self.node.publisher.publish_due()
        self.assertNotIn(self.backbone[0], {node_id for node_id, _, _ in self.net.provide_calls})
        self.assertIn(self.backbone[0], self.node.publisher._withdrawals)

    write = LocalWiringTest.write

    def test_not_entered_by_queries_without_a_path(self):
        # Island relays in RNS's default full mode don't forward a request for a path they lack, and a hub drops those
        # over its limit: such failures don't count towards partition mode, but still count in the routing table
        lacked = set(self.backbone)
        self.node._path_lacked = lacked.__contains__
        self.cut()
        self.node.lookups.search(ident(0x4444)).find_providers(lambda provider: None)
        self.assertGreaterEqual(self.off_island(), FAILURES_TO_ENTER)
        self.assertFalse(self.node.partition.active or self.node.partition.probe_due())
        for node_id in set(self.backbone) & set(self.asked):
            self.assertGreaterEqual(self.node.routing.row(node_id)[4], 1)  # its failed queries since last heard
        # Link failures still enter it
        lacked.clear()
        self.node.lookups.search(ident(0x5555)).find_providers(lambda provider: None)
        self.assertTrue(self.node.partition.active or self.node.partition.probe_due())

    def test_servers_closing_links_are_refusals(self):
        # A server with no room for another incoming link closes it (RNS's DESTINATION_CLOSED): a refusal, not a
        # failure, in partition mode, the routing table and the scores alike (PeerClient.closed_by_node)
        closed = set(self.backbone)
        self.node._closed_by = closed.__contains__
        for n in self.backbone:
            self.node._answered(n)  # the backbone answered before its servers filled up: probe targets
        scores = {node_id: self.node.scores.score(node_id) for node_id in self.backbone}
        self.cut()
        self.node.lookups.search(ident(0x4444)).find_providers(lambda provider: None)
        self.assertGreaterEqual(self.off_island(), FAILURES_TO_ENTER)
        self.assertFalse(self.node.partition.active or self.node.partition.probe_due())
        in_table = [node_id for node_id in self.backbone if self.node.routing.contains(node_id)]
        self.assertTrue(in_table)
        for node_id in in_table:
            self.assertEqual(self.node.routing.row(node_id)[4], 0)  # no failed query counted
        for node_id in self.backbone:
            self.assertFalse(self.node.routing.unreliable(node_id))
            self.assertEqual(self.node.scores.score(node_id), scores[node_id])
        # A probe of a server that closes the link reached it: the island isn't cut off
        self.assertIs(self.node.probe_off_island(), False)
        self.assertFalse(self.node.partition.active)
        # Servers that don't answer at all still count
        closed.clear()
        self.node.lookups.search(ident(0x5555)).find_providers(lambda provider: None)
        self.assertTrue(self.node.partition.active or self.node.partition.probe_due())

    def test_a_probe_ends_it(self):
        clock = FakeClock()
        self.node.partition.clock = clock
        for n in self.backbone:
            self.node._answered(n)  # the backbone answered before the gateway went down
        self.cut()
        self.node.lookups.search(ident(0x4444)).find_providers(lambda provider: None)
        # The failures asked for a probe, which the local-servers thread sends at once (woken); it fails
        self.assertFalse(self.node.partition.active)
        self.assertTrue(self.node._local_wake.is_set() and self.node.partition.probe_due())
        self.node.check_local()
        self.assertTrue(self.node.partition.active)
        self.node.check_local()
        self.assertTrue(self.node.partition.active)  # no probe due yet
        self.asked.clear()
        clock.now += SLOW_PROBE_INTERVAL
        self.node.check_local()  # a probe, unanswered
        self.assertEqual(self.off_island(), 1)
        self.assertTrue(self.node.partition.active)
        self.heal()
        clock.now += 2 * SLOW_PROBE_INTERVAL
        self.node.check_local()
        self.assertFalse(self.node.partition.active)

    def test_an_off_island_announce_ends_it(self):
        self.cut()
        self.node.lookups.search(ident(0x4444)).find_providers(lambda provider: None)
        self.node._fresh_announce(self.islanders[0])  # an island server's: says nothing about the gateway
        self.node._fresh_announce(ident(0x7777))  # not a server it judged
        self.assertTrue(self.node.partition.active)
        self.node._fresh_announce(self.backbone[3])
        self.assertFalse(self.node.partition.active)

    def partitioned_publish(self) -> bytes:
        """Enter partition mode, then add and publish a file: its key."""
        self.cut()
        self.node.lookups.search(ident(0x4444)).find_providers(lambda provider: None)
        self.assertTrue(self.node.partition.active)
        self.node.add_path(self.write(b"published while cut off"))
        self.net.provide_calls.clear()
        self.node.publisher.publish_due()
        key = next(iter(self.node.publisher.provided())).key
        self.assertEqual(self.node.publisher.heal_marked(), 1)
        return key

    def stored_beyond(self, key: bytes) -> set[bytes]:
        """The servers behind the gateway sent key since provide_calls was cleared."""
        return {node_id for node_id, keys, _ in self.net.provide_calls if key in keys and node_id in self.backbone}

    def test_heal_republish(self):
        """Keys whose round reached only the island are republished as soon as partition mode ends, not at their next
        republish (3 days on a slow node); then the mark is gone."""
        key = self.partitioned_publish()
        self.assertEqual(self.stored_beyond(key), set())
        self.assertGreater(self.node.publisher.next_due(), 3600)  # published: not due for days
        self.heal()
        self.node._fresh_announce(self.backbone[0])  # the gateway is back: an announce from beyond it
        self.assertFalse(self.node.partition.active)
        self.node.publisher._next_lookup = 0  # past the slow node's pacing of the round before
        self.assertEqual(self.node.publisher.next_due(), 0)
        self.node.publisher.publish_due()
        self.assertTrue(self.stored_beyond(key))
        self.assertEqual(self.node.publisher.heal_marked(), 0)

    def restarted(self) -> RnfsNode:
        """The island node, restarted after publishing while cut off."""
        self.partitioned_publish()
        self.node.stop()
        node = self.make_node(slow="yes", local_interfaces=["LoRa"])
        self.assertEqual(node.publisher.heal_marked(), 1)
        self.assertGreater(node.publisher.next_due(), 3600)
        for n in self.islanders + self.backbone:
            node._heard_server(n)
        return node

    def test_marks_survive_a_restart_during_the_partition(self):
        node = self.restarted()
        node.partition.active = True  # still cut off after the restart, and found so
        node._fresh_announce(self.islanders[0])
        self.assertGreater(node.publisher.next_due(), 3600)
        node._fresh_announce(self.backbone[0])
        self.assertLess(node.publisher.next_due(), 3600)

    def test_marks_healed_at_the_first_contact_after_a_restart(self):
        """The partition ended while the node was down: no partition mode ends after the restart, so its first
        off-island contact republishes what is marked."""
        node = self.restarted()
        node._answered(self.islanders[0])  # local: nothing
        self.assertGreater(node.publisher.next_due(), 3600)
        node._answered(self.backbone[0])
        self.assertLess(node.publisher.next_due(), 3600)

    def test_a_round_that_found_only_the_island_is_marked_while_servers_beyond_are_known(self):
        """A node started while its island was cut off never enters partition mode (its failures are for want of a
        path) and its rounds find local servers only, so no off-island server is in their store sets: over LoRa a
        gateway restarted during an outage stored a new key on its one neighbour and nothing marked it, leaving it on
        the island for days. Known servers beyond the island mark such a round; an island that knows of none, isn't
        cut off from anything, and marks nothing."""
        publisher = self.node.publisher
        self.assertTrue(publisher._off_island_known())  # the backbone's servers are in its routing table
        state = publisher_module._Item()
        island = self.islanders[:1]
        self.assertTrue(publisher._mark_heal(state, island, island, False, publisher._off_island_known()))
        self.assertFalse(publisher._mark_heal(state, island + self.backbone[:1], island + self.backbone[:1], False,
                                              True))  # reaching beyond clears it
        lonely = self.make_node(ident(0xC0), slow="yes", local_interfaces=["LoRa"])
        for n in self.islanders:
            lonely._heard_server(n)
            lonely.routing.heard(n)
        self.assertFalse(lonely.publisher._off_island_known())
        self.assertFalse(lonely.publisher._mark_heal(publisher_module._Item(), island, island, False,
                                                     lonely.publisher._off_island_known()))

    def test_rounds_reaching_beyond_the_island_mark_nothing(self):
        self.node.add_path(self.write(b"connected"))
        self.node.publisher.publish_due()
        self.assertEqual(self.node.publisher.heal_marked(), 0)
        self.assertFalse(self.node.partition.active)

    def test_a_node_without_local_servers_never_enters_it(self):
        node = self.make_node(ident(0xB0), local=False)
        for n in self.backbone:
            node.routing.heard(n)
        self.cut()
        node.lookups.search(ident(0x4444)).find_providers(lambda provider: None)
        self.assertFalse(node.partition.active)
        self.assertGreater(self.off_island(), FAILURES_TO_ENTER)


if __name__ == "__main__":
    unittest.main()
