"""Identities placed next to a key, simulated over the fake network (specification section 6, "Identities placed next
to a key"; the model of docs/simulations/eclipse).

Honest servers have real routing tables (tests.test_lookup.FakeNetwork); m identities are generated closer to a key
than every honest server and answer /find for it with each other only, discarding stores. They are new ("fresh"):
honest servers took them into their buckets only where there was room. A publisher stores the key through a real
Publisher round, and searchers look for it; both are either upgraded (the density check, disjoint paths, the
fallback, rnfs.dht.lookups) or as 0.1.x nodes (one lookup each). The announce first-seen log is left out, as it
alone would beat identities this new; size estimates start from 16 lookups for other keys. A slow searcher's
fallback isn't paced here (pacing only delays it).

The suite runs a small version (a few seconds); RNFS_ECLIPSE=1 runs a larger one (2,000 servers, more keys, slow
nodes too) and prints what it measured.
"""
import os
import random
import statistics
import tempfile
import threading
import unittest
from collections.abc import Callable

from rnfs.config import Config
from rnfs.constants import ID_LENGTH, K
from rnfs.content.storage import Storage
from rnfs.dht.density import SIZE_SAMPLES, SizeEstimate
from rnfs.dht.lookup import Lookup
from rnfs.dht.lookups import DISJOINT_PATHS, SLOW_DISJOINT_PATHS, Lookups
from rnfs.dht.providers import ProviderStore
from rnfs.dht.routing import RoutingTable, distance
from rnfs.publisher import Publisher
from tests.fakes.client import FakeClient
from tests.fakes.log import quiet_log
from tests.support import FakeClock
from tests.test_lookup import CONFIG, FakeNetwork

setUpModule, tearDownModule = quiet_log()

HEAVY = os.environ.get("RNFS_ECLIPSE") == "1"
PUBLISH_GAP = 400.0  # seconds between a peer's publish rounds: over SLOW_LOOKUP_PACE × SLOW_ROUND_LOOKUPS


class Peer:
    """A publishing or searching node outside the network: its routing table (every honest server heard), size
    estimate and lookups, upgraded or as a 0.1.x node; alpha 1 and 2 paths as a slow node."""

    def __init__(self, net: FakeNetwork, node_id: bytes, upgraded: bool, slow: bool = False):
        """A node that has heard every server of net."""
        self.net = net
        self.id = node_id
        self.upgraded = upgraded
        self.slow = slow
        self.queries = 0
        self.lock = threading.Lock()
        self.table = RoutingTable(node_id, CONFIG, clock=net.clock)
        order = list(net.nodes)
        random.Random(node_id).shuffle(order)
        for n in order:
            self.table.heard(n)
        self.keys: list[bytes] = []  # published so far
        self._publisher: Publisher | None = None
        self.clock = FakeClock(1_000_000.0)  # the publisher's
        self.client: FakeClient | None = None
        self.sizes = SizeEstimate()
        self.lookups = Lookups(self.new_lookup, self.start, self.sizes, lambda key: [],
                               SLOW_DISJOINT_PATHS if slow else DISJOINT_PATHS)

    def query(self, node_id: bytes, key: bytes):
        """A /find, counted."""
        with self.lock:
            self.queries += 1
        return self.net.query(node_id, key)

    def new_lookup(self, key: bytes, start: list[bytes], **options) -> Lookup:
        """A Lookup over the network, as the node makes one (a 0.1.x node's takes none of the options)."""
        if not self.upgraded:
            options = {}
        options.setdefault("timeout", 30)
        return Lookup(key, start, self.query, self.id, alpha=1 if self.slow else 3, poll_interval=0.01, **options)

    def start(self, key: bytes) -> list[bytes]:
        """The routing table's K closest."""
        return self.table.closest(key, K)

    def warm(self, rng: random.Random):
        """Size samples from SIZE_SAMPLES complete lookups for other keys (the K-th closest honest server, which a
        complete lookup finds)."""
        honest = [n for n in self.net.nodes if n not in self.net.liars]
        for _ in range(SIZE_SAMPLES):
            key = rng.randbytes(ID_LENGTH)
            self.sizes.note(key, sorted(distance(n, key) for n in honest)[K - 1])

    def search(self, key: bytes, publisher: bytes) -> tuple[bool, int]:
        """Look for publisher's record of key as the fetcher does: (found, /find sent)."""
        before = self.queries
        found = threading.Event()
        lookup = self.lookups.search(key) if self.upgraded else self.new_lookup(key, self.start(key))
        lookup.find_providers(lambda provider: found.set() if provider == publisher else None,
                              should_stop=found.is_set)
        return found.is_set(), self.queries - before

    def closest(self, key: bytes) -> tuple[bool, int, int]:
        """An upgraded node's search run to completion (find_closest of its first lookup) and a 0.1.x node's from the
        same routing table: (the density check fired, /find sent, /find a 0.1.x node sent)."""
        before = self.queries
        lookup = self.lookups.search(key).first
        lookup.find_closest()
        checked = self.queries - before
        Lookup(key, self.start(key), self.query, self.id, alpha=1 if self.slow else 3, timeout=30,
               poll_interval=0.01).find_closest()
        return lookup.dense, checked, self.queries - before - checked

    def publisher(self, cleanup: Callable) -> Publisher:
        """The node's Publisher (made at the first publish), providing every key published so far."""
        if self._publisher is None:
            tmp = tempfile.TemporaryDirectory()
            storage = Storage(os.path.join(tmp.name, "blocks"), os.path.join(tmp.name, "state"), 10**9)
            cleanup(tmp.cleanup)
            cleanup(storage.close)
            self.client = FakeClient()
            make_lookup = self.lookups.publish if self.upgraded else lambda k: self.new_lookup(k, self.start(k))
            self._publisher = Publisher(self.id, Config(rnfs_dir=tmp.name), storage, ProviderStore(None), self.client,
                                        make_lookup, self.slow, clock=self.clock)
            if self.upgraded:
                self._publisher.set_widening(self.lookups.flags, self.lookups.widen)
            self._publisher.add_key_source(lambda: set(self.keys))
        return self._publisher

    def publish(self, key: bytes, cleanup: Callable) -> tuple[int, int]:
        """A Publisher round for key (the only key due), its stores recorded by the honest servers they reach: (/find
        sent, servers stored to)."""
        publisher = self.publisher(cleanup)
        # Past a slow node's pacing of the last round, well before anything published comes due again
        self.clock.now += PUBLISH_GAP
        self.keys.append(key)
        publisher.refresh()
        before, calls = self.queries, len(self.client.calls)
        publisher.publish_due()
        finds = self.queries - before
        servers = [node_id for node_id, stored, _ in self.client.calls[calls:] if key in stored]
        for node_id in servers:
            if node_id in self.net.nodes and node_id not in self.net.liars:
                self.net.nodes[node_id].store.store(key, self.id)
        return finds, len(servers)


class EclipseSimulation:
    """One honest network and its peers; attacks placed and removed key by key."""

    def __init__(self, servers: int, seed: int, cleanup: Callable, slow: bool = False):
        """servers honest servers; 3 publishers and 6 searchers of each kind. cleanup(f) runs f when done."""
        self.cleanup = cleanup
        self.net = FakeNetwork(servers, seed=seed)
        self.rng = random.Random(seed + 1)
        self.peers = {kind: ([Peer(self.net, self.rng.randbytes(ID_LENGTH), kind, slow) for _ in range(3)],
                             [Peer(self.net, self.rng.randbytes(ID_LENGTH), kind, slow) for _ in range(6)])
                      for kind in (False, True)}

    def everyone(self) -> list[Peer]:
        """Every peer."""
        return [peer for publishers, searchers in self.peers.values() for peer in publishers + searchers]

    def place(self, key: bytes, count: int) -> list[bytes]:
        """count identities closer to key than every honest server, heard by every server and peer whose bucket has
        room."""
        nearest = min(distance(n, key) for n in self.net.nodes)
        placed = [(int.from_bytes(key, "big") ^ self.rng.randrange(1, nearest)).to_bytes(ID_LENGTH, "big")
                  for _ in range(count)]
        honest = list(self.net.nodes)
        for n in placed:
            self.net.add(n)
            self.net.liars.add(n)
        for n in honest:
            for p in placed:
                self.net.nodes[n].table.heard(p)
        for peer in self.everyone():
            for p in placed:
                peer.table.heard(p)
        return placed

    def remove(self, placed: list[bytes]):
        """Take the identities out again."""
        for p in placed:
            self.net.liars.discard(p)
            del self.net.nodes[p]
        for table in [node.table for node in self.net.nodes.values()] + [peer.table for peer in self.everyone()]:
            for p in placed:
                table.remove(p)

    def findability(self, keys: int, count: int, upgraded: bool, searches: int = 2) -> float:
        """The share of searches finding the publisher's record of keys keys, each attacked by count identities."""
        publishers, searchers = self.peers[upgraded]
        found = total = 0
        for index in range(keys):
            for peer in publishers + searchers:
                peer.warm(self.rng)  # the samples of the attacked keys searched before don't linger
            key = self.rng.randbytes(ID_LENGTH)
            placed = self.place(key, count)
            try:
                publisher = publishers[index % len(publishers)]
                publisher.publish(key, self.cleanup)
                for searcher in self.rng.sample(searchers, searches):
                    found += searcher.search(key, publisher.id)[0]
                    total += 1
            finally:
                self.remove(placed)
        return found / total

    def honest_costs(self, keys: int) -> dict:
        """Without an attack: the density check's false positives and extra /find per search (searches run to
        completion), /find and servers per publish of one key, and the cost of a search for content nobody provides,
        each against 0.1.x nodes'."""
        for peer in self.everyone():
            peer.warm(self.rng)
        result = {}
        fired, extra, plain_cost = 0, [], []
        for _ in range(keys):
            dense, cost, plain = self.rng.choice(self.peers[True][1]).closest(self.rng.randbytes(ID_LENGTH))
            fired += dense
            extra.append(cost - plain)
            plain_cost.append(plain)
        result["false_positives"] = fired / keys
        result["extra_finds"] = statistics.mean(extra)
        result["plain_finds"] = statistics.mean(plain_cost)
        publishes = max(5, keys // 10)
        for kind in (False, True):
            finds, stores = [], []
            for _ in range(publishes):
                f, s = self.rng.choice(self.peers[kind][0]).publish(self.rng.randbytes(ID_LENGTH), self.cleanup)
                finds.append(f)
                stores.append(s)
            result["publish_old" if not kind else "publish_new"] = (statistics.mean(finds), statistics.mean(stores))
        missing_new, missing_old = [], []
        for _ in range(publishes):
            key = self.rng.randbytes(ID_LENGTH)
            missing_new.append(self.rng.choice(self.peers[True][1]).search(key, bytes(ID_LENGTH))[1])
            missing_old.append(self.rng.choice(self.peers[False][1]).search(key, bytes(ID_LENGTH))[1])
        result["missing"] = (statistics.mean(missing_new), statistics.mean(missing_old))
        return result


class EclipseTest(unittest.TestCase):
    """Findability under attack, and what the defences cost without one."""

    SERVERS = 2000 if HEAVY else 400
    KEYS = 60 if HEAVY else 12

    @classmethod
    def setUpClass(cls):
        """One simulation for the class (building the network takes a second or two)."""
        cls.sim = EclipseSimulation(cls.SERVERS, 3, cls.addClassCleanup)

    def report(self, text: str):
        """Print a measurement in the heavy run."""
        if HEAVY:
            print(f"\n[eclipse, {self.SERVERS} servers] {text}", flush=True)

    def test_identities_next_to_a_key_no_longer_hide_it(self):
        """0.1.x nodes almost never find a key 10 or 20 such identities hide (0-4% in the eclipse simulation);
        upgraded publisher and searcher find it (98.9% there, fast nodes)."""
        for count in (10, 20):
            old = self.sim.findability(self.KEYS, count, upgraded=False)
            new = self.sim.findability(self.KEYS, count, upgraded=True)
            self.report(f"{count} identities: found {old:.1%} by 0.1.x nodes, {new:.1%} upgraded")
            self.assertLessEqual(old, 0.25, count)
            self.assertGreaterEqual(new, 0.85, count)

    def test_costs_without_an_attack(self):
        """The density check fires on about 4% of honest searches (+0.3 /find a search on average in the eclipse
        simulation); publishing over disjoint paths costs some 25 more messages; a search for content nobody provides
        pays the fallback, about 2.5-3 times a normal lookup."""
        costs = self.sim.honest_costs(150 if HEAVY else 60)
        new_publish, old_publish = costs["publish_new"], costs["publish_old"]
        self.report(f"false positives {costs['false_positives']:.1%}, extra /find per search "
                    f"{costs['extra_finds']:+.2f} (of {costs['plain_finds']:.1f}); per publish {new_publish[0]:.1f} "
                    f"/find + {new_publish[1]:.1f} stores against {old_publish[0]:.1f} + {old_publish[1]:.1f}; a "
                    f"search for content nobody provides {costs['missing'][0]:.1f} /find against "
                    f"{costs['missing'][1]:.1f}")
        self.assertLessEqual(costs["false_positives"], 0.15)
        self.assertLessEqual(costs["extra_finds"], 1.5)
        self.assertLessEqual(new_publish[0] + new_publish[1] - sum(old_publish), 45)
        self.assertGreater(costs["missing"][0], costs["missing"][1])
        self.assertLess(costs["missing"][0], 5 * costs["missing"][1])


@unittest.skipUnless(HEAVY, "set RNFS_ECLIPSE=1")
class SlowEclipseTest(EclipseTest):
    """The same with slow nodes everywhere (alpha 1, 2 paths): 90.8% found in the eclipse simulation."""

    @classmethod
    def setUpClass(cls):
        """A simulation of slow peers."""
        cls.sim = EclipseSimulation(cls.SERVERS, 4, cls.addClassCleanup, slow=True)

    def test_identities_next_to_a_key_no_longer_hide_it(self):
        """Slow nodes: the threshold is lower (worst cases in the simulation fell to 59%)."""
        for count in (10, 20):
            old = self.sim.findability(self.KEYS, count, upgraded=False)
            new = self.sim.findability(self.KEYS, count, upgraded=True)
            self.report(f"slow: {count} identities: found {old:.1%} by 0.1.x nodes, {new:.1%} upgraded")
            self.assertGreaterEqual(new, 0.7, count)


if __name__ == "__main__":
    unittest.main()
