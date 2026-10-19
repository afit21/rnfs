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
"""
import random
import statistics
import threading
import unittest

from rnfs.config import Config
from rnfs.constants import ID_LENGTH, K
from rnfs.dht.density import SIZE_SAMPLES, SizeEstimate
from rnfs.dht.lookup import Lookup
from rnfs.dht.lookups import (DISJOINT_PATHS, SLOW_DISJOINT_PATHS, SLOW_PUBLISH_UNKNOWNS, SLOW_SEARCH_UNKNOWNS,
                              Lookups)
from rnfs.dht.routing import RoutingTable
from rnfs.dht.sweep import find_region
from rnfs.mesh import SLOW_PATH_HOPS, SlowPaths, Traffic
from tests.fakes.log import quiet_log
from tests.test_lookup import CONFIG, FakeNetwork

setUpModule, tearDownModule = quiet_log()

BACKBONE = 790  # servers on the backbone
ISLANDS = 8  # LoRa islands, of ISLAND_SIZE servers each: 21% of the servers, as in the study
ISLAND_SIZE = 26
HEARD = 0.16  # the share of the servers an island hears besides its own (the study at 1,000 servers)
RTT_ISLAND = ISLANDS - 1  # this island's servers are 3 hops away, but links to them were measured slow


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
        self.slow = island is not None
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
        """A /find, counted; an identity it learns of is known from then on (its path request answered)."""
        with self.lock:
            self.finds += 1
            self.island_finds += self.world.on_island(node_id)
            self.known.add(node_id)
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
        stored = list(dict.fromkeys(region.closest_to(key) + region.path_closest(key)))
        for n in stored:
            self.world.net.nodes[n].store.store(key, self.id)
        return stored

    def search(self, key: bytes, publisher: bytes) -> bool:
        """Look for publisher's record of key as the fetcher does with fewer providers than max_parallel_providers
        (until the search ends); True if found."""
        return publisher in self.lookups.search(key).find_providers(lambda provider: None)


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


if __name__ == "__main__":
    unittest.main()
