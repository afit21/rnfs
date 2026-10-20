"""REV8: made-up IDs in /find replies could inflate the network size estimate about 3 times.

The density check (specification section 6, "Identities placed next to a key") estimates the network's size from
d_K, the distance from a lookup's key to its K-th closest node, over the node's recent complete lookups
(`SizeEstimate`, fed by `Lookup`'s on_complete). A Lookup took d_K over every node its run knew of, including IDs a
reply named that nobody had queried. A lookup completes once the beta (3) closest known nodes have answered, so a
server that answers with made-up IDs placed just beyond the key's 3rd closest server leaves them unqueried, ranked 4
to 10: d_K shrank to about the 4th closest's distance and the estimate grew about 3 times (288 -> 743 here), which
weakens the density check (identities placed next to a key look less dense against a larger network).

Now a lookup's sample counts only verified nodes, those that answered or whose identity is known (as candidates()
chooses), and only the run of them closest to the key, up to the first node that isn't verified:
d_j of the run's last, scaled to rank K (density.verified_sample). Counting every verified node instead would skip
the real nodes a node doesn't know the identity of too, and a node on a LoRa island, which hears few announces,
underestimated the network about 4 times (searches flagged by the density check 93% of the time in the topology
simulation, against 7%). Rebuilt here: 300 servers, each lookup with one server among its first queried naming 7
made-up IDs between the key's 3rd and 4th closest.
"""
import random
import statistics
import unittest

from rnfs.constants import ID_LENGTH, K
from rnfs.dht.density import SPACE
from rnfs.dht.routing import distance
from tests.test_lookup import LookupTestCase

SERVERS = 300
LOOKUPS = 64
MADE_UP = K - 3  # ranked 4 to 10 among the IDs a lookup knows


def estimate(samples: list[int]) -> float:
    """The size estimate from samples (SizeEstimate's formula, over all of them)."""
    return (K - 1 / 3) * SPACE / statistics.median(samples)


class SizeEstimateInflationTest(LookupTestCase):
    """A network of 300 servers whose lookups each meet one server naming made-up IDs."""

    def setUp(self):
        """The network, and the made-up IDs the attacking server names, by key."""
        super().setUp()
        self.net = self.network(SERVERS, seed=31)
        self.made_up: dict[bytes, list[bytes]] = {}
        self.attacker: bytes | None = None
        query = self.net.query

        def attacked(node_id: bytes, key: bytes):
            """/find: the attacker names the made-up IDs first (a reply holds at most K), then its true nodes."""
            reply = query(node_id, key)
            if node_id != self.attacker or reply is None or key not in self.made_up:
                return reply
            providers, nodes = reply
            return providers, (self.made_up[key] + list(nodes))[:K]

        self.net.query = attacked

    def place(self, key: bytes) -> list[bytes]:
        """MADE_UP IDs between key's 3rd and 4th closest servers (an attacker that knows where they are, the worst
        case), drawn from a generator of their own, so attacked lookups are for the same keys as honest ones."""
        rng = random.Random(key)
        third, fourth = (distance(n, key) for n in self.net.true_closest(key, 4)[2:])
        value = int.from_bytes(key, "big")
        return [(value ^ rng.randrange(third + 1, fourth)).to_bytes(ID_LENGTH, "big") for _ in range(MADE_UP)]

    def samples(self, attacked: bool, known=None, **options) -> dict[bytes, int]:
        """Each of LOOKUPS lookups' sample (key -> d_K or its stand-in), each starting from the attacker and 2 other
        servers if attacked; RNS knows only the real servers' identities (those known() accepts, if given). options
        are further Lookup arguments."""
        rng = random.Random(32)
        servers = sorted(self.net.nodes)
        known = known or self.net.nodes.__contains__
        samples: dict[bytes, int] = {}
        for _ in range(LOOKUPS):
            key = rng.randbytes(ID_LENGTH)
            start = rng.sample(servers, 3)
            if attacked:
                self.attacker = start[0]
                self.made_up[key] = self.place(key)
            lookup = self.lookup(self.net, key, start, on_complete=samples.__setitem__,
                                 is_known=lambda node_id: node_id in self.net.nodes and known(node_id), **options)
            lookup.find_closest()
            self.assertEqual(lookup.ended, "complete")
        return samples

    def test_made_up_ids_dont_inflate_the_estimate(self):
        honest = estimate(list(self.samples(False).values()))
        attacked = estimate(list(self.samples(True).values()))
        self.assertGreater(honest, SERVERS / 2)
        self.assertLess(honest, 2 * SERVERS)
        self.assertLess(attacked, 1.5 * honest)

    def test_a_node_knowing_few_identities_doesnt_underestimate(self):
        """RNS knows the identities of a fifth of the servers (a LoRa island hears few announces; the rest it queries
        once their path requests are answered): the estimate stays near the network's size, not about a quarter of
        it."""
        rng = random.Random(33)
        heard = set(rng.sample(sorted(self.net.nodes), SERVERS // 5))
        few = estimate(list(self.samples(False, known=heard.__contains__, unknowns=SERVERS).values()))
        self.assertGreater(few, SERVERS / 2)
        self.assertLess(few, 2 * SERVERS)


if __name__ == "__main__":
    unittest.main()
