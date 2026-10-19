"""LoRa islands behind an internet backbone, over the fake network (specification section 6: slow paths, and the
unknown-ID budgets of slow nodes; the model of docs/simulations/topology, strategies PRS and UB).

1,000 servers, 21% of them on 8 islands, as in the study at that size. Servers on the backbone hear every server; the
servers of each island hear their own island and the same 16% of the rest (RNS gives announces 2% of a LoRa channel,
so an island hears few). Nodes outside the network look up and publish as rnfs does (rnfs.dht.lookups, and the store
set of a region's lookup): fast nodes on the backbone, which see the islands' servers behind a slow path (more than 3
hops, or for one island a slow link measured before, through mesh.SlowPaths), and slow publishers on an island, which
know only the identities their island heard (any other costs a path request).

Measured: how many island servers a backbone node's searches ask and its publishes store to, with and without asking
slow paths last (in the study, 97% less airtime from the backbone on the islands); whether backbone searchers still
find content published on an island (98% there); and how many of their true K closest servers a slow publisher's
records reach with the larger path-request budget (4.4 of 10 there instead of 2.7). Here, over 3 worlds of 60 keys
each: island /find per backbone search 0.76 -> 0, island stores per backbone publish 2.47 -> 0.07, an island
publisher's records on 3.6 -> 6.9 of their true 10 closest, found from the backbone 99% either way (3,000 servers,
5.3% heard: 0.93 -> 0, 2.50 -> 0.01, 2.4 -> 6.3, 95% -> 100%). The suite runs fewer keys, in a few seconds.

Islands cut off (IslandPartitionTest; the study's LF2L-m1 and PH, sections 2, 3, 7 and 9): an island's gateway goes
down, and a neighbour of an island publisher (a cold searcher keeping its state from search to search) looks for what
it published before and during the partition, with and without local servers (the island, as local_interfaces names
it), two-level records, the local-first search and partition mode. Island nodes' lookups run on simulated time, a /find
over LoRa taking 8 s and one to a server they can't reach 40 s, within lookup_timeout (600 s). Over 3 worlds of 7
islands, 10 keys each way: found 67% (published before) and 91% (during) -> 100% and 100%; per search 11.7 timeouts and
490 s -> 0.15 and 31 s (the study: 55-72%, about 300 s and 7 timeouts -> 100%, 28 s), the timeouts being the 3 that
enter partition mode, in the first search. What the island published during the partition: found from the backbone 0%
until the publisher's next republish, 99.5% after the heal republish (the study: 1-14% before). A week of partition
mode's own clock, 5 or 20 servers beyond the island each announcing every 6 hours (announces up to 10 minutes late, 10%
lost), the gateway down for 12 hours on the fourth day: entered once in each of 10 runs (12-338 minutes into the cut:
the silence limit, 67-343 minutes, from the last off-island contact), left 0-49 minutes after the gateway came back,
never entered while connected, at the cost of 0-13 probes in the week while connected.
"""
import random
import statistics
import threading
import unittest

from rnfs.config import Config
from rnfs.constants import ID_LENGTH, K
from rnfs.dht.density import SIZE_SAMPLES, SizeEstimate
from rnfs.dht.local import LOCAL_FIRST, SLOW_PROBE_MAX, LocalServers, PartitionMode
from rnfs.dht.lookup import Lookup
from rnfs.dht.lookups import (DISJOINT_PATHS, SLOW_DISJOINT_PATHS, SLOW_PUBLISH_UNKNOWNS, SLOW_SEARCH_UNKNOWNS,
                              Lookups)
from rnfs.dht.routing import RoutingTable
from rnfs.dht.sweep import find_region
from rnfs.mesh import SLOW_PATH_HOPS, SlowPaths, Traffic
from tests.fakes.log import quiet_log
from tests.support import FakeClock
from tests.test_lookup import CONFIG, FakeNetwork

setUpModule, tearDownModule = quiet_log()

BACKBONE = 790  # servers on the backbone
ISLANDS = 8  # LoRa islands, of ISLAND_SIZE servers each: 21% of the servers, as in the study
ISLAND_SIZE = 26
HEARD = 0.16  # the share of the servers an island hears besides its own (the study at 1,000 servers)
DAY = 86400.0
CHECK = 30.0  # seconds between the node's checks of partition mode (node.LOCAL_CHECK)
RTT_ISLAND = ISLANDS - 1  # this island's servers are 3 hops away, but links to them were measured slow
LOCAL_REPLICAS = 3  # the default local_replicas
# An island node's lookups run on simulated time (the study's model): a /find answered over LoRa takes FIND_SECONDS, one
# to a server it can't reach TIMEOUT_SECONDS (link establishment, about 6 s a hop plus grace, or a path request's 30 s),
# within the default lookup_timeout
FIND_SECONDS = 8.0
TIMEOUT_SECONDS = 40.0
LOOKUP_TIMEOUT = 600.0


class Topology:
    """The servers, which islands they are on, and the hop counts backbone nodes see."""

    def __init__(self, seed: int):
        """BACKBONE backbone servers and ISLANDS islands; every server's routing table filled as it would hear."""
        self.rng = random.Random(seed)
        self.net = FakeNetwork()
        self.backbone = [self.rng.randbytes(ID_LENGTH) for _ in range(BACKBONE)]
        self.islands = [[self.rng.randbytes(ID_LENGTH) for _ in range(ISLAND_SIZE)] for _ in range(ISLANDS)]
        self.island_of = {n: index for index, island in enumerate(self.islands) for n in island}
        everyone = self.backbone + list(self.island_of)
        for n in everyone:
            self.net.add(n)
        # What islands hear of the rest: the same stable subset everywhere (announces are queued fewest hops first)
        self.overheard = self.rng.sample(everyone, int(HEARD * len(everyone)))  # a list: the same order every run
        for n in everyone:
            order = self.heard_by(n)
            self.rng.shuffle(order)
            for other in order:
                if other != n:
                    self.net.nodes[n].table.heard(other)
        # Hops from the backbone: 1-3 to backbone servers, 4-6 to island ones (gateway and LoRa hops); the RTT island
        # sits right behind a hub (3 hops), and only its measured links tell
        self.hops = {n: self.rng.randint(1, 3) for n in self.backbone}
        for n, index in self.island_of.items():
            self.hops[n] = SLOW_PATH_HOPS if index == RTT_ISLAND else self.rng.randint(SLOW_PATH_HOPS + 1, 6)
        self.cut: set[int] = set()  # islands whose gateway is down

    def reachable(self, island: int | None, node_id: bytes) -> bool:
        """Whether a node on island (None: the backbone) reaches node_id: always, unless one of them is on an island
        whose gateway is down and they aren't on the same island."""
        other = self.island_of.get(node_id)
        return island == other or (island not in self.cut and other not in self.cut)

    def heard_by(self, n: bytes | None, island: int | None = None) -> list[bytes]:
        """The servers n (or a node on island) hears: everyone on the backbone; its island and the overheard on one."""
        island = self.island_of.get(n, island) if n is not None else island
        if island is None:
            return self.backbone + list(self.island_of)
        return list(dict.fromkeys(self.islands[island] + [m for m in self.overheard if m not in self.islands[island]]))

    def on_island(self, node_id: bytes) -> bool:
        """True for an island server."""
        return node_id in self.island_of

    def true_closest(self, key: bytes) -> list[bytes]:
        """The K servers closest to key."""
        return self.net.true_closest(key)


class Peer:
    """A node outside the network: on the backbone (alpha 3, 3 paths, asking slow paths last if `demotes`) or on an
    island (slow: alpha 1, 2 paths, knowing only the identities its island heard, with the slow nodes' unknown-ID
    budgets if `budgets`)."""

    def __init__(self, world: Topology, island: int | None = None, demotes: bool = True, budgets: bool = True):
        """A node that has heard what its place in world hears."""
        self.world = world
        self.id = world.rng.randbytes(ID_LENGTH)
        self.island = island
        self.slow = island is not None
        self.timeouts = 0  # queries and stores to servers it couldn't reach (each a link timeout on RNS)
        heard = world.heard_by(None, island)
        self.known = set(heard)
        self.table = RoutingTable(self.id, CONFIG, clock=world.net.clock)
        for n in heard:
            self.table.heard(n)
        self.lock = threading.Lock()
        self.island_finds = self.finds = 0
        traffic = Traffic()
        for n in world.islands[RTT_ISLAND]:  # links this node opened to them before: long round trips, low rates
            traffic.measured(n, 2.5)
            traffic.rated(n, 1200.0)
        self.slow_paths = SlowPaths(traffic, Config().slow_bitrate, world.hops.get) if demotes and not self.slow \
            else None
        slow_budgets = {"publish_unknowns": SLOW_PUBLISH_UNKNOWNS, "search_unknowns": SLOW_SEARCH_UNKNOWNS}
        self.lookups = Lookups(self.new_lookup, lambda key: self.table.closest(key, K), SizeEstimate(),
                               lambda key: [], SLOW_DISJOINT_PATHS if self.slow else DISJOINT_PATHS,
                               **(slow_budgets if self.slow and budgets else {}))
        rng = random.Random(self.id)
        for _ in range(SIZE_SAMPLES):  # the size estimate's samples, from complete lookups for other keys
            self.lookups.search(rng.randbytes(ID_LENGTH)).find_closest()

    def query(self, node_id: bytes, key: bytes):
        """A /find, counted; an identity it learns of is known from then on (its path request answered). One to a
        server it can't reach (a gateway down) fails, counted as a timeout."""
        with self.lock:
            self.finds += 1
            self.island_finds += self.world.on_island(node_id)
            self.known.add(node_id)
            if not self.world.reachable(self.island, node_id):
                self.timeouts += 1
                return None
        return self.world.net.query(node_id, key)

    def is_known(self, node_id: bytes) -> bool:
        """Whether RNS would recall node_id's identity."""
        with self.lock:
            return node_id in self.known

    def new_lookup(self, key: bytes, start: list[bytes], **options) -> Lookup:
        """A Lookup as the node makes one (rnfs.node._wired_lookup)."""
        if self.slow_paths is not None:
            options.setdefault("slow_path", self.slow_paths)
        options.setdefault("timeout", 30)
        return Lookup(key, start, self.query, self.id, alpha=1 if self.slow else 3, is_known=self.is_known,
                      poll_interval=0.01, **options)

    def publish(self, key: bytes) -> list[bytes]:
        """Publish key as a round would (a region's lookup over disjoint paths; its K closest and each path's): the
        servers stored to, which record it."""
        region = find_region(self.lookups.publish(key), key, self.id, lambda bits: 1)
        return self.store(key, region.closest_to(key) + region.path_closest(key))

    def store(self, key: bytes, nodes: list[bytes]) -> list[bytes]:
        """/provide key to nodes: those it reaches record it, and are returned."""
        stored = []
        for n in dict.fromkeys(nodes):
            if self.world.reachable(self.island, n):
                self.world.net.nodes[n].store.store(key, self.id)
                stored.append(n)
            else:
                self.timeouts += 1
                self.failed(n)
        return stored

    def failed(self, node_id: bytes):
        """A query or store to node_id failed (nothing to note without partition mode)."""

    def search(self, key: bytes, publisher: bytes) -> bool:
        """Look for publisher's record of key as the fetcher does with fewer providers than max_parallel_providers
        (until the search ends); True if found."""
        return publisher in self.lookups.search(key).find_providers(lambda provider: None)


class IslandPeer(Peer):
    """A slow node on an island, with the island changes (`two_level`: specification section 6, "LoRa islands") or
    without: its local servers (its island's, as local_interfaces names them), two-level records (stored to the
    LOCAL_REPLICAS closest local servers too), the local-first search, partition mode and the heal republish."""

    def __init__(self, world: Topology, island: int, two_level: bool):
        """A node on island."""
        self.two_level = two_level
        self.elapsed = 0.0  # simulated seconds its queries took
        self.local = LocalServers()
        for n in world.islands[island]:
            self.local.note(n, two_level)
        self.partition = PartitionMode(self.local.is_local, slow=True)
        self.marked: set[bytes] = set()  # keys whose last round reached no off-island server
        super().__init__(world, island=island)
        if two_level:
            self.lookups.start = self.start
            self.lookups.local_first = lambda key: self.local.closest(key, LOCAL_FIRST)

    def start(self, key: bytes) -> list[bytes]:
        """Start nodes as the node gives them: in partition mode the closest local servers first."""
        start = self.table.closest(key, K)
        if self.partition.active:
            start = list(dict.fromkeys(self.local.closest(key, K) + [n for n in start if self.local.is_local(n)]))
        return start

    def clock(self) -> float:
        """Simulated time: what its queries took."""
        with self.lock:
            return self.elapsed

    def query(self, node_id: bytes, key: bytes):
        """A /find, taking FIND_SECONDS, or TIMEOUT_SECONDS if it fails."""
        reply = super().query(node_id, key)
        with self.lock:
            self.elapsed += FIND_SECONDS if reply is not None else TIMEOUT_SECONDS
        return reply

    def new_lookup(self, key: bytes, start: list[bytes], **options) -> Lookup:
        """A Lookup as the node makes one, on simulated time, with partition mode's rule and its accounting."""
        options.setdefault("clock", self.clock)
        options.setdefault("timeout", LOOKUP_TIMEOUT)
        if self.two_level:
            options.setdefault("allowed", self.partition.allows)
            options.setdefault("on_answer", self.partition.answered)
            options.setdefault("on_failure", self.partition.failed)
        return super().new_lookup(key, start, **options)

    def publish(self, key: bytes) -> list[bytes]:
        """A round for key: its region's servers, and with the changes its local replicas, in partition mode local
        ones only; marked for the heal republish if it reached no off-island server."""
        region = find_region(self.lookups.publish(key), key, self.id, lambda bits: 1)
        nodes = region.closest_to(key) + region.path_closest(key)
        if self.two_level:
            nodes += self.local.closest(key, LOCAL_REPLICAS)
            nodes = [n for n in nodes if self.partition.allows(n)]
        stored = self.store(key, nodes)
        if self.two_level:
            if any(not self.local.is_local(n) for n in stored):
                self.marked.discard(key)
            else:
                self.marked.add(key)
        return stored

    def failed(self, node_id: bytes):
        """A store failed: counted towards partition mode."""
        if self.two_level:
            self.partition.failed(node_id)

    def probe(self) -> bool:
        """partition mode's probe: one /find to an off-island server; True if answered."""
        target = self.partition.probe_target()
        answered = target is not None and self.query(target, self.id) is not None
        if target is not None:
            self.partition.probed(target, answered)
        return answered



class TopologyTest(unittest.TestCase):
    """Fast nodes spare the islands; slow publishers place their records better."""

    @classmethod
    def setUpClass(cls):
        """One world for the class."""
        cls.world = Topology(11)

    def backbone_load(self, demotes: bool, keys: int = 30) -> tuple[float, float, float]:
        """Backbone nodes publishing and searching backbone nodes' content: (island /find per search, island stores
        per publish, share found)."""
        rng = random.Random(12)
        publisher, searcher = Peer(self.world, demotes=demotes), Peer(self.world, demotes=demotes)
        island_stores, finds, found = [], [], 0
        for _ in range(keys):
            key = rng.randbytes(ID_LENGTH)
            island_stores.append(sum(map(self.world.on_island, publisher.publish(key))))
            before = searcher.island_finds
            found += searcher.search(key, publisher.id)
            finds.append(searcher.island_finds - before)
        return statistics.mean(finds), statistics.mean(island_stores), found / keys

    def test_backbone_nodes_spare_the_islands(self):
        """Asking slow paths last: few island servers asked or stored to (the study: 97% less airtime), and nothing
        lost on the backbone."""
        old_finds, old_stores, old_found = self.backbone_load(demotes=False)
        new_finds, new_stores, new_found = self.backbone_load(demotes=True)
        self.assertGreater(old_finds, 0.5)  # without it, the islands answer a share of every lookup
        self.assertLessEqual(new_finds, 0.2 * old_finds)
        self.assertLessEqual(new_stores, 0.2 * old_stores)
        self.assertGreaterEqual(new_found, 0.97)
        self.assertGreaterEqual(old_found, 0.95)

    def placement(self, budgets: bool, keys: int = 24) -> tuple[float, float]:
        """Slow publishers on the islands publishing: (how many of each key's true K closest got its record, the
        share of backbone searchers (asking slow paths last) that found it)."""
        rng = random.Random(13 if budgets else 14)
        publishers = [Peer(self.world, island=index % ISLANDS, budgets=budgets) for index in range(4)]
        searchers = [Peer(self.world) for _ in range(2)]
        placed, found = [], 0
        for index in range(keys):
            key, publisher = rng.randbytes(ID_LENGTH), publishers[index % len(publishers)]
            placed.append(len(set(publisher.publish(key)) & set(self.world.true_closest(key))))
            found += searchers[index % len(searchers)].search(key, publisher.id)
        return statistics.mean(placed), found / keys

    def test_island_content_found_from_the_backbone(self):
        """A slow publisher with the larger budget reaches more of its keys' true K closest servers, and backbone nodes
        that ask slow paths last still find what it published (98% in the study)."""
        old_placed, _ = self.placement(budgets=False)
        new_placed, new_found = self.placement(budgets=True)
        self.assertGreaterEqual(new_placed, old_placed + 1.0)
        self.assertGreaterEqual(new_found, 0.95)


class IslandPartitionTest(unittest.TestCase):
    """An island whose gateway goes down (the study's sections 2, 3, 7 and 9): with local servers, two-level records,
    the local-first search and partition mode its own content stays findable without minutes of timeouts, and the heal
    republish puts what it published meanwhile beyond it once it reconnects."""

    ISLAND = 3

    @classmethod
    def setUpClass(cls):
        """One world for the class (its own: tests cut its islands off)."""
        cls.world = Topology(21)

    def tearDown(self):
        """Every gateway up again."""
        self.world.cut.clear()

    def partition(self, two_level: bool, keys: int = 20) -> dict:
        """A publisher and a searcher on one island, with or without the island changes. The publisher publishes keys
        while connected, the gateway goes down, it publishes as many more, and the searcher (cold, then keeping its
        state, as a node does) looks for all of them. Returns the shares found (published before and during), the
        timeouts and simulated seconds per search, the timeouts of the first search and the most of a later one, and
        whether the searcher ended in partition mode."""
        rng = random.Random(41 + two_level)
        self.world.cut.clear()
        self.world.rng.seed(71 + self.ISLAND)  # the same nodes whichever tests ran before
        publisher = IslandPeer(self.world, self.ISLAND, two_level)
        searcher = IslandPeer(self.world, self.ISLAND, two_level)
        before = [rng.randbytes(ID_LENGTH) for _ in range(keys)]
        for key in before:
            publisher.publish(key)
        self.world.cut.add(self.ISLAND)
        during = [rng.randbytes(ID_LENGTH) for _ in range(keys)]
        for key in during:
            publisher.publish(key)
        found, timeouts, seconds = {}, [], []
        for key in before + during:
            start, started = searcher.timeouts, searcher.elapsed
            found[key] = searcher.search(key, publisher.id)
            timeouts.append(searcher.timeouts - start)
            seconds.append(searcher.elapsed - started)
        return {"before": statistics.mean(found[key] for key in before),
                "during": statistics.mean(found[key] for key in during),
                "timeouts": statistics.mean(timeouts), "first": timeouts[0], "later": max(timeouts[1:]),
                "total": sum(timeouts), "seconds": statistics.mean(seconds), "entered": searcher.partition.active}

    def test_island_content_found_during_a_partition(self):
        """Found by a neighbour 100% of the time (the study: 55-72% before), with a few timeouts in the first search
        (those that enter partition mode) and none after (the study: ~7 a lookup, ~300 s)."""
        old, new = self.partition(two_level=False), self.partition(two_level=True)
        self.assertLessEqual(old["before"], 0.85)
        self.assertEqual((new["before"], new["during"]), (1.0, 1.0))
        self.assertTrue(new["entered"])
        self.assertGreaterEqual(old["timeouts"], 8)
        self.assertGreaterEqual(old["seconds"], 300)
        # Over every search: 3 enter partition mode, and the fallback's 2 paths may have one out each when it begins
        self.assertLessEqual(new["total"], 5)
        self.assertLessEqual(new["seconds"], 60)

    def test_heal_republish(self):
        """What the island published during the partition sits on its own servers only: the backbone seldom finds it
        until the publisher republishes (3 days on a slow node). Once partition mode ends, the heal republish puts it
        beyond the island at once, and the backbone finds it."""
        rng = random.Random(51)
        publisher = IslandPeer(self.world, self.ISLAND, two_level=True)
        searchers = [Peer(self.world) for _ in range(2)]
        self.world.cut.add(self.ISLAND)
        keys = [rng.randbytes(ID_LENGTH) for _ in range(10)]
        for key in keys:
            publisher.publish(key)
        self.assertTrue(publisher.partition.active)
        self.assertEqual(publisher.marked, set(keys))
        self.world.cut.clear()

        def found() -> float:
            """The share of keys backbone searchers find."""
            return statistics.mean(searchers[index % 2].search(key, publisher.id) for index, key in enumerate(keys))

        unhealed = found()
        self.assertLessEqual(unhealed, 0.5)
        self.assertTrue(publisher.probe())  # the gateway is back: partition mode ends
        self.assertFalse(publisher.partition.active)
        for key in sorted(publisher.marked):
            publisher.publish(key)
        self.assertEqual(publisher.marked, set())
        self.assertGreaterEqual(found(), 0.9)

    def test_partition_mode_neither_flaps_nor_sticks(self):
        """A small connected network whose servers beyond the island announce every 6 hours (here 5 of them, heard
        late or not at all as LoRa's announce queue allows): over a week partition mode is never entered, the silence
        rule's probes being answered; when the gateway goes down for 12 hours it is entered within the silence limit
        and a check, and left within a slow node's longest probe interval of the gateway coming back."""
        rng = random.Random(61)
        clock = FakeClock(0.0)
        mode = PartitionMode(lambda node_id: False, slow=True, clock=clock)
        servers = [rng.randbytes(ID_LENGTH) for _ in range(5)]
        phases = {n: rng.uniform(0, 6 * 3600) for n in servers}
        down = (3 * DAY, 3 * DAY + 12 * 3600)
        announces = sorted((phases[n] + 6 * 3600 * k + rng.uniform(0, 600), n) for n in servers
                           for k in range(29) if rng.random() > 0.1)
        entered, left, probes = [], [], 0
        for step in range(int(7 * DAY / CHECK)):
            clock.now = step * CHECK
            cut = down[0] <= clock.now < down[1]
            while announces and announces[0][0] <= clock.now:
                _, n = announces.pop(0)
                if not cut:
                    was = mode.active
                    mode.announced(n)
                    if was:
                        left.append(clock.now)
            if mode.probe_due():
                probes += 1
                was = mode.active
                target = mode.probe_target()
                mode.probed(target, not cut)
                if mode.active and not was:
                    entered.append(clock.now)
                if was and not mode.active:
                    left.append(clock.now)
        self.assertEqual(len(entered), 1)
        self.assertLessEqual(entered[0] - down[0], mode.silence_limit() + CHECK)
        self.assertEqual(len(left), 1)
        self.assertLessEqual(left[0] - down[1], SLOW_PROBE_MAX + CHECK)
        self.assertLess(probes, 7 * 4 + 20)  # silence probes on the connected days are rare, and partition's paced


if __name__ == "__main__":
    unittest.main()
