"""AUD-M1: searches for keys an attacker chose could poison the network size estimate, turning the density check off.

The density check (specification section 6, "Identities placed next to a key") judges the servers near a key against
the network size estimate, (K − 1/3) × 2^128 over the median d_K of the node's last 16 complete lookups
(`SizeEstimate`). Every search fed it (`Lookups.search`'s on_complete), and a search's key is chosen by whoever asks
for the content: a web page through the gateway, a link's request, a keeper query. An attacker grinds one tight cluster
of identities and about 9 keys inside it, then makes the node search those keys: each lookup completes among the
cluster, its d_K is tiny, and the median collapses. In the audit's simulation (400 servers) the estimate went from 400
to 8.6e5-1.2e9 and the density check stopped firing; the inflated estimate also sizes the publisher's sweep regions
(`sweep.size_bits`) and, on slow nodes, turns `provide_entries` off and on (`RnfsNode._known_servers`).

Rebuilt here over the fake network: 300 honest servers, a node whose estimate is warm from honest samples, 10
identities packed a million times closer together than honest servers lie, and 9 searches for keys inside them.

Now only lookups for keys the node chose itself feed the estimate: its publish (region) lookups and the random-key
lookups it runs for the estimate (`Lookups.sample_due`), never a search. And a sample from a lookup the density check
flags is dropped: a provided key whose region's servers are anomalously dense (`Lookups.flags`) loses its sample, and a
widened lookup takes none, so identities placed next to the node's own keys can't feed it either.
"""
import random

from rnfs.constants import ID_LENGTH, K
from rnfs.dht.density import SIZE_SAMPLES, SizeEstimate
from rnfs.dht.lookup import Lookup
from rnfs.dht.lookups import Lookups
from rnfs.dht.routing import RoutingTable, distance
from rnfs.dht.sweep import Region
from tests.fakes.log import quiet_log
from tests.test_lookup import CONFIG, LookupTestCase

SERVERS = 300
TIGHT = 10**6  # how much closer together the cluster's identities lie than honest servers do
KEYS = 9  # keys inside the cluster the node is made to search: a majority of the 16 samples

setUpModule, tearDownModule = quiet_log()


class SizeEstimatePoisoningTest(LookupTestCase):
    """A node with a warm estimate over 300 honest servers, and an attacker's tight clusters of identities."""

    def setUp(self):
        """The network, the node's routing table (every honest server heard), and its estimate from honest samples
        (the K-th closest honest server of random keys, which a complete lookup finds)."""
        super().setUp()
        self.rng = random.Random(61)
        self.net = self.network(SERVERS, seed=60)
        self.own = self.rng.randbytes(ID_LENGTH)
        self.table = RoutingTable(self.own, CONFIG, clock=self.net.clock)
        for n in self.net.nodes:
            self.table.heard(n)
        self.sizes = SizeEstimate()
        for _ in range(SIZE_SAMPLES):
            key = self.rng.randbytes(ID_LENGTH)
            self.sizes.note(key, sorted(distance(n, key) for n in self.net.nodes)[K - 1])
        self.honest = self.sizes.estimate()
        self.lookups = Lookups(self.new_lookup, lambda key: self.table.closest(key, K), self.sizes,
                               lambda key: [], paths=3)

    def new_lookup(self, key: bytes, start: list[bytes], **options) -> Lookup:
        """A Lookup over the network, as the node makes one."""
        options.setdefault("timeout", 10)
        return Lookup(key, start, self.net.query, self.own, poll_interval=0.02, **options)

    def cluster(self, center: bytes, count: int, tight: int) -> list[bytes]:
        """count identities tight times closer to center than its closest honest server, answering /find with each
        other only, heard by every honest server and the node where buckets have room."""
        nearest = min(distance(n, center) for n in self.net.nodes)
        placed = [(int.from_bytes(center, "big") ^ self.rng.randrange(1, max(2, nearest // tight))).to_bytes(
            ID_LENGTH, "big") for _ in range(count)]
        honest = list(self.net.nodes)
        for n in placed:
            self.net.add(n)
            self.net.liars.add(n)
        for n in honest:
            for p in placed:
                self.net.nodes[n].table.heard(p)
        for p in placed:
            self.table.heard(p)
        return placed

    def inside(self, center: bytes, tight: int) -> bytes:
        """A key inside center's cluster."""
        nearest = min(distance(n, center) for n in self.net.nodes if n not in self.net.liars)
        return (int.from_bytes(center, "big") ^ self.rng.randrange(1, max(2, nearest // tight))).to_bytes(
            ID_LENGTH, "big")

    def test_searches_for_chosen_keys_dont_move_the_estimate(self):
        center = self.rng.randbytes(ID_LENGTH)
        self.cluster(center, K, TIGHT)
        for _ in range(KEYS):
            self.lookups.search(self.inside(center, TIGHT)).find_providers(lambda provider: None)
        self.assertLess(self.sizes.estimate() / self.honest, 2)
        # The density check still fires on a key identities are then placed next to
        attacked = self.rng.randbytes(ID_LENGTH)
        self.cluster(attacked, 2 * K, 1)
        search = self.lookups.search(attacked)
        search.first.find_closest()
        self.assertTrue(search.first.dense)

    def test_a_flagged_key_of_the_nodes_own_drops_its_sample(self):
        """A provided key with identities placed next to it: its region's lookup (no check of its own) samples it, and
        the check over the region flags it, which drops the sample; its widened lookup takes none."""
        key = self.rng.randbytes(ID_LENGTH)
        self.cluster(key, 2 * K, TIGHT)
        lookup = self.lookups.publish(key)
        region = Region(key, 0, lookup.find_closest(), lookup.candidates(), paths=lookup.path_candidates())
        self.assertNotEqual(self.sizes.estimate(), self.sizes.estimate(key))  # the region's lookup sampled it
        self.assertTrue(self.lookups.flags(key, region))
        self.assertEqual(self.sizes.estimate(), self.sizes.estimate(key))  # dropped
        self.lookups.widen(key, region)
        self.assertEqual(self.sizes.estimate(), self.sizes.estimate(key))
        self.assertLess(self.sizes.estimate() / self.honest, 2)

    def test_random_keys_warm_the_estimate(self):
        """Without searches feeding it, a node that publishes nothing still gets an estimate: from lookups for random
        keys of its own choosing."""
        cold = SizeEstimate()
        lookups = Lookups(self.new_lookup, lambda key: self.table.closest(key, K), cold, lambda key: [], paths=3)
        for _ in range(SIZE_SAMPLES):
            self.assertTrue(lookups.sample_due())
        self.assertEqual(len(cold), SIZE_SAMPLES)
        self.assertLess(abs(cold.estimate() / SERVERS - 1), 0.6)
        self.assertFalse(lookups.sample_due())  # full, and fresh
