"""Tests for sweep providing (rnfs/dht/sweep.py): regions, the far half's extra /find, the order lookups take keys in,
and placement over a simulated network against per-key lookups and the true closest servers, also for nodes on LoRa
islands and at their first start (test_topology's world)."""
import os
import random
import statistics
import tempfile
import threading
import unittest

from rnfs.config import Config
from rnfs.constants import ID_LENGTH, K
from rnfs.content.storage import Storage
from rnfs.dht.density import SIZE_SAMPLES, SizeEstimate
from rnfs.dht.lookup import FIRST, SLOW_PATH, UNRELIABLE, Lookup
from rnfs.dht.providers import ProviderStore
from rnfs.dht.routing import RoutingTable, distance
from rnfs.dht.sweep import (ID_BITS, MAX_REGION_FINDS, REGION_FIND_TRIES, SIZE_MARGIN, Region, Sweep, explore,
                            find_region, prefix_length, size_bits)
from rnfs.publisher import SLOW_ROUND_LOOKUPS, DhtKey, Publisher
from tests.fakes.client import FakeClient
from tests.fakes.log import quiet_log
from tests.support import FakeClock
from tests.test_lookup import CONFIG, FakeNetwork
from tests.test_publisher import FakeLookup
from tests.test_topology import Peer, Topology

OWN = bytes(ID_LENGTH)

setUpModule, tearDownModule = quiet_log()


def ident(value: int) -> bytes:
    """The ID whose big-endian value is value."""
    return value.to_bytes(ID_LENGTH, "big")


def top(bits: str, rest: int = 0) -> bytes:
    """The ID starting with the bits given as a string of 0s and 1s, then rest in its low bits."""
    return ident(int(bits, 2) << (ID_BITS - len(bits)) | rest)


class PrefixTest(unittest.TestCase):
    """prefix_length and Region membership."""

    def test_prefix_length(self):
        self.assertEqual(prefix_length(ident(5), ident(5)), ID_BITS)
        self.assertEqual(prefix_length(top("0"), top("1")), 0)
        self.assertEqual(prefix_length(top("1010"), top("1011")), 3)
        self.assertEqual(prefix_length(ident(0), ident(1)), ID_BITS - 1)

    def test_region_holds_the_keys_sharing_its_prefix(self):
        region = Region(top("1010", 7), 3, [], {})
        self.assertIn(top("1011"), region)
        self.assertIn(top("101"), region)
        self.assertNotIn(top("100"), region)

    def test_closest_to_ranks_unreliable_nodes_last(self):
        a, b, c = top("1000"), top("1001"), top("1100")
        region = Region(top("0"), 0, [c], {a: True, b: False, c: False})
        self.assertEqual(region.closest_to(top("10"), 2), [b, c])
        self.assertEqual(region.closest_to(top("10"), 3), [b, c, a])
        # The key looked up keeps at least what its lookup found
        region = Region(top("0"), 0, [a, b, c], {a: False, b: False, c: False})
        self.assertEqual(len(region.closest_to(top("0"), 2)), 3)

    def test_closest_to_ranks_by_tier(self):
        """Servers behind a slow path (on a node that isn't slow) after the rest, before unreliable ones."""
        a, b, c, d = top("1000"), top("1001"), top("1010"), top("1100")
        region = Region(top("0"), 0, [], {a: UNRELIABLE, b: SLOW_PATH, c: FIRST, d: FIRST})
        self.assertEqual(region.closest_to(top("10"), 2), [c, d])
        self.assertEqual(region.closest_to(top("10"), 4), [c, d, b, a])


class SweepOrderTest(unittest.TestCase):
    """Sweep: keys taken in the order given, regions covering keys, claims of lookups running."""

    def test_keys_taken_in_order_skipping_covered_ones(self):
        keys = [top("11"), top("00"), top("01"), top("10")]
        sweep = Sweep(keys)
        self.assertEqual(sweep.take(), top("11"))
        self.assertEqual(sweep.members(top("11"), 1), 2)  # 11 and 10
        covered = sweep.cover(Region(top("11"), 1, [ident(1)], {}))
        self.assertEqual(covered, [top("10"), top("11")])  # sorted
        self.assertEqual(sweep.estimate, 1)
        self.assertEqual(sweep.members(top("11"), 0), 2)  # 00 and 01 left
        self.assertEqual(sweep.take(), top("00"))
        sweep.cover(Region(top("00"), 2, [ident(1)], {}))
        self.assertEqual(sweep.take(), top("01"))
        sweep.cover(Region(top("01"), 0, [ident(1)], {}))  # covers only what is left
        self.assertIsNone(sweep.take())
        self.assertEqual(sweep.covered[top("01")].bits, 0)
        self.assertEqual(sweep.covered[top("00")].bits, 2)
        self.assertEqual(len(sweep.regions), 3)

    def test_a_region_without_servers_keeps_the_estimate(self):
        sweep = Sweep([top("1"), top("0")], estimate=5)
        sweep.cover(Region(sweep.take(), 0, [], {}))
        self.assertEqual(sweep.estimate, 5)
        self.assertIsNone(sweep.take())

    def test_first_lookup_runs_alone_then_claims_make_room(self):
        """Without an estimate a lookup claims every key; with one, the keys sharing one bit fewer than it."""
        keys = [top("000"), top("001"), top("010"), top("100")]
        sweep = Sweep(keys)
        first = sweep.take()
        taken = []
        thread = threading.Thread(target=lambda: taken.append(sweep.take()))
        thread.start()
        thread.join(0.2)
        self.assertTrue(thread.is_alive())  # waits: everything is claimed
        sweep.cover(Region(first, 3, [ident(1)], {}))  # covers only itself; estimate 3
        thread.join(5)
        self.assertEqual(taken, [top("001")])
        # It claims the keys sharing 2 bits with it, so the next is outside them
        self.assertEqual(sweep.take(), top("010"))
        self.assertEqual(sweep.take(), top("100"))
        self.assertEqual(sweep.members(top("000"), 0), 3)

    def test_release_and_stop(self):
        sweep = Sweep([top("0"), top("1")])
        key = sweep.take()
        waiting = []
        thread = threading.Thread(target=lambda: waiting.append(sweep.take()))
        thread.start()
        sweep.release(key)
        thread.join(5)
        self.assertEqual(waiting, [key])  # released: taken again
        sweep.stop()
        self.assertIsNone(sweep.take())


class ScriptedLookup(FakeLookup):
    """A FakeLookup whose ask() answers from a table of servers: each server knows those in `knows`, all of them by
    default, and answers with the K it knows closest to the key asked about."""

    def __init__(self, nodes, servers, knows=None, dead=()):
        """A lookup that found nodes, over servers."""
        super().__init__(nodes)
        self.known = {n: False for n in nodes}
        self.servers = list(servers)
        self.knows = knows or {}
        self.dead = set(dead)

    def candidates(self):
        """What it found and what ask() learned."""
        return dict(self.known)

    def ask(self, node_id, key):
        """The K servers node_id knows closest to key (not itself), or None for a dead one."""
        self.asked.append((node_id, key))
        if node_id in self.dead:
            self.known.pop(node_id, None)
            return None
        knows = self.knows.get(node_id, self.servers)
        named = sorted((n for n in knows if n != node_id), key=lambda n: distance(n, key))[:K]
        for n in named:
            self.known.setdefault(n, False)
        return named


class FindRegionTest(unittest.TestCase):
    """find_region and explore over scripted lookups."""

    def test_region_from_the_kth_closest(self):
        key = top("0110", 1)
        found = [top("0110", i) for i in range(2, 11)] + [top("0111")]  # the 10th shares 3 bits
        lookup = ScriptedLookup(found, found)
        region = find_region(lookup, key, OWN, lambda bits: 1)
        self.assertEqual(region.bits, 3)
        self.assertEqual(region.finds, 0)  # a region of one key needs nothing more
        self.assertEqual(lookup.asked, [])
        self.assertEqual(set(region.closest_to(key)), set(found))

    def test_fewer_than_k_found(self):
        """The farthest found sets the region; none found makes every key one region (retried soon)."""
        key = top("0110")
        lookup = ScriptedLookup([top("0100"), top("0111")], [top("0100"), top("0111")])
        self.assertEqual(find_region(lookup, key, OWN, lambda bits: 1).bits, 2)  # 0100 shares 01
        region = find_region(FakeLookup([]), key, OWN, lambda bits: 5)
        self.assertEqual((region.bits, region.finds, region.closest_to(key)), (0, 0, []))

    def test_far_half_asked_about_once(self):
        """The near half is known from the lookup; one /find to the server nearest the far half's centre fills that
        half in, so every key of the region gets its true K closest."""
        rng = random.Random(1)
        near = [top("00", rng.getrandbits(ID_BITS - 2)) for _ in range(5)]
        far = [top("01", rng.getrandbits(ID_BITS - 2)) for _ in range(8)]
        outside = [top("1", rng.getrandbits(ID_BITS - 1)) for _ in range(20)]
        servers = near + far + outside
        key = top("00", rng.getrandbits(ID_BITS - 2))
        found = sorted(servers, key=lambda n: distance(n, key))[:K]  # the lookup's result: 5 near, 5 far
        lookup = ScriptedLookup(found, servers)
        region = find_region(lookup, key, OWN, lambda bits: 3)
        self.assertEqual(region.bits, 1)
        self.assertEqual(region.finds, 1)
        centre = top("011")
        nearest_far = min(found, key=lambda n: distance(n, centre))
        self.assertEqual(lookup.asked, [(nearest_far, centre)])
        for _ in range(50):
            other = top("0", rng.getrandbits(ID_BITS - 1))
            truth = sorted(servers, key=lambda n: distance(n, other))[:K]
            self.assertEqual(set(region.closest_to(other)), set(truth))

    def test_full_reply_splits_the_part(self):
        """A reply naming K servers inside the far half means it may hold more: each half of it is asked about."""
        rng = random.Random(2)
        near = [top("00", rng.getrandbits(ID_BITS - 2)) for _ in range(4)]
        far = [top("01", rng.getrandbits(ID_BITS - 2)) for _ in range(30)]
        servers = near + far
        key = top("00", 5)
        found = sorted(servers, key=lambda n: distance(n, key))[:K]
        lookup = ScriptedLookup(found, servers)
        region = find_region(lookup, key, OWN, lambda bits: 2)
        centres = [about for _, about in lookup.asked]
        self.assertEqual(centres[:3], [top("011"), top("0101"), top("0111")])
        self.assertLessEqual(region.finds, MAX_REGION_FINDS)
        self.assertEqual(region.finds, len(lookup.asked))
        self.assertTrue(set(far) <= set(region.nodes))

    def test_finds_bounded(self):
        """However full the replies, a region sends at most MAX_REGION_FINDS."""
        rng = random.Random(3)
        servers = [top("01", rng.getrandbits(ID_BITS - 2)) for _ in range(2000)]
        key = top("00", 1)
        found = sorted(servers, key=lambda n: distance(n, key))[:K]
        lookup = ScriptedLookup(found, servers)
        self.assertEqual(explore(lookup, key, 1, OWN), MAX_REGION_FINDS)
        self.assertEqual(len(lookup.asked), MAX_REGION_FINDS)

    def test_handed_over_to_a_server_inside_the_part(self):
        """A server outside the far half may know only some of it (its bucket holding the half is full): the server
        it names nearest the centre inside the half is asked too, and knows all of it."""
        far = [top("01", i << 100) for i in range(1, 7)]
        key, outsider = top("00", 1), top("001")
        knows = {outsider: [far[0]], far[0]: far + [outsider]}  # the outsider knows one server of the far half
        lookup = ScriptedLookup([outsider], far + [outsider], knows=knows)
        sent = explore(lookup, key, 1, OWN)
        self.assertEqual([node for node, _ in lookup.asked], [outsider, far[0]])
        self.assertEqual(sent, 2)
        self.assertTrue(set(far) <= set(lookup.candidates()))
        # One that names no server inside the half: the half is empty as far as anyone knows
        lookup = ScriptedLookup([outsider], [outsider], knows={outsider: []})
        self.assertEqual(explore(lookup, key, 1, OWN), 1)

    def test_nothing_sent_once_the_lookup_is_out_of_time(self):
        far = [top("01", i << 100) for i in range(1, 8)]
        lookup = ScriptedLookup(far, far)
        lookup.remaining = lambda: 0.0
        self.assertEqual(explore(lookup, top("00"), 1, OWN), 0)
        self.assertEqual(lookup.asked, [])

    def test_unanswered_servers_tried_up_to_a_limit(self):
        far = [top("01", i << 100) for i in range(1, 8)]
        lookup = ScriptedLookup(far, far, dead=far)
        self.assertEqual(explore(lookup, top("00"), 1, OWN), REGION_FIND_TRIES)


class SizedRegionTest(unittest.TestCase):
    """Regions sized by the network size estimate too: the floor on their prefix, the keys they know well enough, and
    a region of the key alone while there is no estimate."""

    def test_size_bits(self):
        self.assertEqual(size_bits(1000), 5)  # log2(100) = 6.6, less SIZE_MARGIN
        self.assertEqual(size_bits(K * 2 ** 4), 3)
        self.assertEqual(size_bits(5), 0)

    def test_estimate_lengthens_a_short_prefix_and_far_half_stays_as_found(self):
        """A result whose 10th closest shares 1 bit with the key, on a network the estimate says needs 3: the region
        is the keys sharing 3 bits, and the far half asked about is still the one 1 bit gives (the narrower region's
        keys may have some of their K closest there)."""
        rng = random.Random(1)
        near = [top("00", rng.getrandbits(ID_BITS - 2)) for _ in range(5)]
        far = [top("01", rng.getrandbits(ID_BITS - 2)) for _ in range(8)]
        servers = near + far
        key = top("000", rng.getrandbits(ID_BITS - 3))
        found = sorted(servers, key=lambda n: distance(n, key))[:K]
        lookup = ScriptedLookup(found, servers)
        size = float(K * 2 ** (3 + SIZE_MARGIN))
        region = find_region(lookup, key, OWN, lambda bits: 2, lambda: size)
        self.assertEqual(region.bits, 3)
        self.assertEqual((region.size, region.single), (size, False))
        self.assertEqual(lookup.asked[0][1], top("011"))
        self.assertNotIn(top("001"), region)
        # A prefix longer than the estimate's is kept
        lookup = ScriptedLookup(found, servers)
        self.assertEqual(find_region(lookup, key, OWN, lambda bits: 2, lambda: 2.0 * K).bits, 1)

    def test_no_estimate_yet_the_key_alone(self):
        key = top("0110", 1)
        found = [top("0110", i) for i in range(2, 11)] + [top("0111")]
        lookup = ScriptedLookup(found, found)
        region = find_region(lookup, key, OWN, lambda bits: 5, lambda: None)
        self.assertTrue(region.single)
        self.assertEqual((region.bits, region.finds, lookup.asked), (3, 0, []))  # bits as found, nothing explored
        self.assertIn(key, region)
        self.assertNotIn(top("0110", 99), region)
        sweep = Sweep([key, top("0110", 99)], estimate=None)
        self.assertEqual(sweep.take(), key)
        self.assertEqual(sweep.cover(region), [key])
        self.assertIsNone(sweep.estimate)  # a single region's prefix isn't a region's size
        self.assertEqual(sweep.take(), top("0110", 99))

    def test_region_covers_the_keys_it_knows_well_enough(self):
        """Among 1,000 servers a key's K-th closest lies about (K - 1/3) × 2^128 / 1000 away: a region covers a key
        whose K-th closest known lies within SPARSE times that, not one it knows servers only further from."""
        size = 1000.0
        median = (K - 1 / 3) * (1 << ID_BITS) / size
        key, near, sparse = top("0", 1 << 100), top("0", 1 << 101), top("01")

        def around(centre: bytes, spread: float) -> list[bytes]:
            """K servers whose farthest lies spread × the median from centre."""
            value = int.from_bytes(centre, "big")
            return [ident(value ^ int(spread * median * (i + 1) / K)) for i in range(K)]

        nodes = dict.fromkeys(around(key, 1.0) + around(near, 1.2) + around(sparse, 3.0)[:3], FIRST)
        region = Region(key, 1, around(key, 1.0), nodes, size=size)
        self.assertTrue(region.knows(key))
        self.assertTrue(region.knows(near))
        self.assertFalse(region.knows(sparse))
        self.assertTrue(Region(key, 1, [], nodes).knows(sparse))  # no estimate: as before
        sweep = Sweep([key, near, sparse])
        self.assertEqual(sweep.take(), key)
        self.assertEqual(sweep.cover(region), [key, near])
        self.assertEqual(sweep.take(), sparse)  # looked up in turn

    def test_publisher_without_an_estimate_looks_up_each_key(self):
        """A node's first round (no estimate): a lookup per key, and no network size learned from those regions."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        storage = Storage(os.path.join(tmp.name, "blocks"), os.path.join(tmp.name, "state"), 10**9)
        self.addCleanup(storage.close)
        nodes = [ident(n) for n in range(1, K + 1)]  # far from every key: one region holds them all
        publisher = Publisher(ident(0), Config(rnfs_dir=tmp.name), storage, ProviderStore(None), FakeClient(),
                              lambda key: FakeLookup(nodes), clock=FakeClock(1_000_000.0))
        keys = [bytes([n]) * ID_LENGTH for n in (0x40, 0x41, 0x80, 0xc0)]
        publisher.add_key_source(lambda: set(keys))
        publisher.refresh()
        estimates = []
        publisher.set_network_size(lambda key: estimates.append(key))  # None: no estimate yet
        result = publisher.publish_due()
        self.assertEqual((result["lookups"], len(result["published"])), (4, 4))
        self.assertEqual(sorted(estimates), sorted(keys))  # asked leaving out each key's own sample
        self.assertIsNone(publisher._region_bits_mean)


class SimulatedNetworkTest(unittest.TestCase):
    """Sweep providing over a simulated network of 1000 servers (Kademlia routing tables, real Lookups): placement
    against per-key lookups and the true closest, and the lookups and extra /find it costs. Regions are sized by a
    network size estimate too, as the node's are, fed by the lookups."""

    SERVERS = 1000

    @classmethod
    def setUpClass(cls):
        """One network for the class (it takes a few seconds to build), the publisher's routing table, and its size
        estimate with its samples from lookups for other keys."""
        cls.net = FakeNetwork(cls.SERVERS, seed=41)
        cls.table = RoutingTable(OWN, CONFIG, clock=cls.net.clock)
        for node_id in cls.net.nodes:
            cls.table.heard(node_id)
        cls.sizes = SizeEstimate()
        rng = random.Random(40)
        for _ in range(SIZE_SAMPLES):
            cls.make_lookup(rng.randbytes(ID_LENGTH)).find_closest()

    @classmethod
    def make_lookup(cls, key: bytes) -> Lookup:
        """A lookup for key from the publisher's routing table, as the node makes one (its sample noted)."""
        return Lookup(key, cls.table.closest(key, K), cls.net.query, OWN, timeout=30, poll_interval=0.05,
                      on_complete=cls.sizes.note)

    def publish(self, keys: list[bytes], slow: bool = False,
                at_once: int | None = None) -> tuple[dict, dict[bytes, set[bytes]]]:
        """A publisher providing keys (as DhtKeys) runs one round, at_once region lookups at a time (the publisher's
        default if None): its result, and the servers each key was sent to."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        storage = Storage(os.path.join(tmp.name, "blocks"), os.path.join(tmp.name, "state"), 10**9)
        self.addCleanup(storage.close)
        client = FakeClient()
        publisher = Publisher(OWN, Config(rnfs_dir=tmp.name), storage, ProviderStore(None), client, self.make_lookup,
                              slow, clock=FakeClock(1_000_000.0), lookups_at_once=at_once)
        publisher.set_network_size(self.sizes.estimate)
        publisher.add_key_source(lambda: set(keys))
        publisher.refresh()
        result = publisher.publish_due()
        sent: dict[bytes, set[bytes]] = {}
        for node_id, stored, _ in client.calls:
            for key in stored:
                sent.setdefault(key, set()).add(node_id)
        return result, sent

    def test_placement_matches_the_true_closest(self):
        """3000 keys: at least 99% get exactly their true K closest (100% in the runs measured), at least as many as
        per-key lookups do; lookups level off at about servers / 12.5 (80 here) sized by the lookups alone, and a
        region covering only the keys it knows servers near as densely as the size estimate expects adds under half
        again (114 here: a key's K-th closest lies past SPARSE times the median distance by chance, or in a region
        holding few servers), with under 3 extra /find a region.

        The region lookups run one at a time, so the outcome doesn't depend on thread timing: the order answers arrive
        in decides whom a lookup asks next, and with 4 lookups at once under CPU load one sometimes completes without
        learning one of its key's true closest, which its region's 30 or so keys then all miss (0.983 and 0.990
        measured). Lookups at once are test_publisher's to test."""
        rng = random.Random(42)
        keys = [rng.randbytes(ID_LENGTH) for _ in range(3000)]
        result, sent = self.publish(keys, at_once=1)
        self.assertEqual(len(result["published"]), len(keys))
        exact = sum(1 for key in keys if sent[key] == set(self.net.true_closest(key))) / len(keys)
        self.assertGreaterEqual(exact, 0.99)
        self.assertLessEqual(result["lookups"], self.SERVERS / 12.5 * 1.8)
        self.assertGreaterEqual(result["lookups"], self.SERVERS / 12.5 * 0.7)
        self.assertLess(result["finds"] / result["lookups"], 3)
        # Per-key lookups, as before sweep providing, for a sample: no better placed, and mostly the same servers
        sample = keys[:300]
        per_key = {key: set(self.make_lookup(key).find_closest()) for key in sample}
        per_key_exact = sum(1 for key in sample if per_key[key] == set(self.net.true_closest(key))) / len(sample)
        sweep_exact = sum(1 for key in sample if sent[key] == set(self.net.true_closest(key))) / len(sample)
        self.assertGreaterEqual(sweep_exact, per_key_exact)
        self.assertGreaterEqual(sum(1 for key in sample if per_key[key] == sent[key]) / len(sample), 0.9)

    def test_few_keys_need_at_most_a_lookup_each(self):
        rng = random.Random(43)
        keys = [rng.randbytes(ID_LENGTH) for _ in range(40)]
        result, sent = self.publish(keys)
        self.assertLessEqual(result["lookups"], len(keys))
        exact = sum(1 for key in keys if sent[key] == set(self.net.true_closest(key)))
        self.assertGreaterEqual(exact, 0.9 * len(keys))  # a region of one key is as exact as its lookup

    def test_slow_node_counts_region_lookups(self):
        """A slow node's round runs at most SLOW_ROUND_LOOKUPS lookups, but publishes every key of their regions."""
        rng = random.Random(44)
        keys = [rng.randbytes(ID_LENGTH) for _ in range(1000)]
        result, sent = self.publish(keys, slow=True)
        self.assertEqual(result["lookups"], 10)
        self.assertGreater(len(result["published"]), 10 * 5)  # 1000 keys over ~80 regions: ~12 a region
        exact = sum(1 for item in result["published"] if sent[item.key] == set(self.net.true_closest(item.key)))
        self.assertGreaterEqual(exact, 0.99 * len(result["published"]))
        self.assertTrue(all(isinstance(item, DhtKey) for item in result["published"]))


class TopologySweepTest(unittest.TestCase):
    """Sweep providing in test_topology's world (1,000 servers, 21% of them on 8 LoRa islands that hear their own island
    and 16% of the rest), with the real Publisher and Sweep over each node's own lookups (rnfs.dht.lookups: disjoint
    paths, the slow nodes' budgets of unknown IDs), against one lookup per key run by a twin of the node (so neither
    learns from the other): of each key's true K closest servers, how many its round stores to (the region's K closest
    and each path's), and the lookups the round runs. Stores to island servers aren't modelled as failing: an island's
    gateway is up.

    Regions were sized by their lookup alone (docs/simulations/topology section 11.3, item 6, found 3.7 / 2.2 of 10 on
    an island against 7.0 / 6.6 with a lookup per key, at 1,000 / 3,000 servers). Measured here over 5 worlds at 1,000
    servers, before and after regions were sized by the network size estimate too (sweep.find_region's size): an island
    node's round placed 3.8 -> 6.6 of 10 (6.6 -> 1.6 keys a region lookup) against 7.7-7.9 one lookup per key; a
    backbone node's 7.9 either way against 7.8 (island servers rank last there, so about 2 of each key's true 10 are
    left out either way), in 63 -> 79 lookups for 600 keys; a backbone node's first round after its first start
    0.3 -> 6.3 of 10 (3 -> 79 lookups for 100 keys: its few known servers padded the first lookup's result, which
    sized a region holding most of them), its second 1.9 -> 7.5. Over 3 worlds of 3,000 servers (islands hearing 5.3%
    of the rest): island 2.3 -> 4.5 (4.9 -> 1.2 keys a lookup) against 5.1-5.8 a lookup per key, backbone 7.9 against
    8.1 in 175 -> 202 lookups, first start 0.1 -> 4.9 and its second round 0.7 -> 6.8. The suite runs one world, in a
    few seconds."""

    @classmethod
    def setUpClass(cls):
        """One world for the class."""
        cls.world = Topology(7)

    def round(self, peer: Peer, keys: list[bytes]) -> tuple[dict, dict[bytes, set[bytes]]]:
        """One publish round of a Publisher over peer's lookups providing keys, its region lookups one at a time:
        (its result, key -> the servers it stored to)."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        storage = Storage(os.path.join(tmp.name, "blocks"), os.path.join(tmp.name, "state"), 10**9)
        self.addCleanup(storage.close)
        client = FakeClient()
        publisher = Publisher(peer.id, Config(rnfs_dir=tmp.name), storage, ProviderStore(None), client,
                              peer.lookups.publish, peer.slow, clock=FakeClock(1_000_000.0), lookups_at_once=1)
        publisher.set_network_size(peer.lookups.sizes.estimate)  # as the node does
        publisher.add_key_source(lambda: set(keys))
        publisher.refresh()
        result = publisher.publish_due()
        sent: dict[bytes, set[bytes]] = {}
        for node_id, stored, _ in client.calls:
            for key in stored:
                sent.setdefault(key, set()).add(node_id)
        return result, sent

    def placed(self, key: bytes, nodes) -> int:
        """How many of key's true K closest servers are among nodes."""
        return len(set(nodes) & set(self.world.true_closest(key)))

    @staticmethod
    def per_key(peer: Peer, key: bytes) -> set[bytes]:
        """The servers a lookup of key's own (a region of one key) stores it to."""
        region = find_region(peer.lookups.publish(key), key, peer.id, lambda bits: 1)
        return set(region.closest_to(key) + region.path_closest(key))

    def newcomer(self, rng: random.Random) -> Peer:
        """A backbone node at its first start: it knows the identities of 2 bootstrap servers and of those its join's
        own-ID lookup asked, and has no network size estimate yet."""
        peer = Peer(self.world)
        boot = rng.sample(self.world.backbone, 2)
        peer.known = set(boot)
        peer.table = RoutingTable(peer.id, CONFIG, clock=self.world.net.clock)
        for node_id in boot:
            peer.table.heard(node_id)
        peer.lookups.sizes = SizeEstimate()
        peer.new_lookup(peer.id, peer.table.closest(peer.id, K)).find_closest()
        for node_id in list(peer.known):
            peer.table.heard(node_id)
        return peer

    def island_round(self) -> tuple[float, float, float]:
        """Island nodes on two islands each run a round over 200 due keys (SLOW_ROUND_LOOKUPS region lookups):
        (mean of each published key's true K closest stored to, the same with a lookup per key for 30 of them,
        keys published per region lookup)."""
        rng = random.Random(81)
        swept, alone, per_lookup = [], [], []
        for island in (0, 1):
            keys = [rng.randbytes(ID_LENGTH) for _ in range(200)]
            result, sent = self.round(Peer(self.world, island=island), keys)
            published = [item.key for item in result["published"]]
            self.assertEqual(result["lookups"], SLOW_ROUND_LOOKUPS)
            swept += [self.placed(key, sent.get(key, ())) for key in published]
            twin = Peer(self.world, island=island)
            alone += [self.placed(key, self.per_key(twin, key)) for key in published[:30]]
            per_lookup.append(len(published) / result["lookups"])
        return statistics.mean(swept), statistics.mean(alone), statistics.mean(per_lookup)

    def test_island_node(self):
        """An island node's region lookup covers only the keys it knows servers near as densely as the size estimate
        expects (here 6.8-6.9 of 10 against 7.6-7.8 a lookup per key, 1.8 keys a lookup; sized by its padded result
        alone, 4.2 in 6.3 keys a lookup)."""
        swept, alone, per_lookup = self.island_round()
        self.assertGreaterEqual(alone, 6.5)
        self.assertGreaterEqual(swept, alone - 1.5)
        self.assertLessEqual(per_lookup, 3)

    def test_backbone_node(self):
        """A backbone node's round: placed as well as a lookup per key, in under one lookup per 5 keys (here 78; 63 sized
        by its lookups alone)."""
        rng = random.Random(82)
        keys = [rng.randbytes(ID_LENGTH) for _ in range(600)]
        result, sent = self.round(Peer(self.world), keys)
        self.assertEqual(len(result["published"]), len(keys))
        swept = statistics.mean(self.placed(key, sent[key]) for key in keys)
        twin = Peer(self.world)
        alone = statistics.mean(self.placed(key, self.per_key(twin, key)) for key in keys[:60])
        self.assertGreaterEqual(swept, alone - 0.3)
        self.assertLessEqual(result["lookups"], len(keys) / 5)

    def test_first_start(self):
        """A node's first round after its first start, with no size estimate and few servers known: a key a lookup
        until the estimate has its samples, then regions of the keys it knows well enough, which are few (here 6.4-6.7
        of 10 in 81-89 lookups; sized by its lookups alone, 0.4 in 4)."""
        rng = random.Random(83)
        keys = [rng.randbytes(ID_LENGTH) for _ in range(100)]
        result, sent = self.round(self.newcomer(rng), keys)
        self.assertEqual(len(result["published"]), len(keys))
        self.assertGreaterEqual(statistics.mean(self.placed(key, sent.get(key, ())) for key in keys), 5)


class RegionPathsTest(unittest.TestCase):
    """Region.path_closest."""

    def test_each_paths_closest(self):
        a, b, c, d = (ident(n) for n in (1, 2, 3, 4))
        region = Region(ident(0), 0, [a], {a: False, b: False, c: False, d: False},
                        paths=[{a: False, c: False}, {b: True, d: False}])
        self.assertEqual(region.path_closest(ident(0), 1), [a, d])  # b ranks last on its path
        self.assertEqual(Region(ident(0), 0, [a], {a: False}).path_closest(ident(0)), [])


class WideningPublisherTest(unittest.TestCase):
    """The publisher's widened lookups and their stores."""

    def publisher(self, nodes, slow=False, flagged=lambda key: True):
        """A publisher whose lookups find nodes, whose density check flags the keys flagged() says, and whose widened
        lookups give one ring server per key: (publisher, client, widened keys)."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        storage = Storage(os.path.join(tmp.name, "blocks"), os.path.join(tmp.name, "state"), 10**9)
        self.addCleanup(storage.close)
        client = FakeClient()
        publisher = Publisher(ident(0), Config(rnfs_dir=tmp.name), storage, ProviderStore(None), client,
                              lambda key: FakeLookup(nodes), slow, clock=FakeClock(1_000_000.0))
        widened: list[bytes] = []
        publisher.set_widening(lambda key, region: flagged(key),
                               lambda key, region: (widened.append(key), [key[:15] + b"\xff"])[1])
        return publisher, client, widened

    def test_flagged_keys_stored_on_their_rings_too(self):
        nodes = [ident(n) for n in range(1, K + 1)]
        keys = [bytes([n]) * ID_LENGTH for n in (0x40, 0x80)]
        publisher, client, widened = self.publisher(nodes, flagged=lambda key: key == keys[0])
        publisher.add_key_source(lambda: set(keys))
        publisher.refresh()
        result = publisher.publish_due()
        self.assertEqual(widened, [keys[0]])
        self.assertEqual(result["widened"], 1)
        ring = keys[0][:15] + b"\xff"
        stored = {node_id: set(stored) for node_id, stored, _ in client.calls}
        self.assertEqual(stored[ring], {keys[0]})
        self.assertEqual(len(result["published"]), 2)
        published = publisher.states()[next(item for item in result["published"] if item.key == keys[0])]["nodes"]
        self.assertIn(ring, published)  # withdrawn from too, when it stops being provided

    def test_slow_node_counts_widened_lookups_and_defers_the_rest(self):
        """Each key its own region here: half the round's lookups are regions, the other half widened; the keys whose
        widened lookup didn't fit stay due."""
        keys = [bytes([n]) + bytes(ID_LENGTH - 1) for n in range(0, 256, 8)]

        def lookup(key):
            """A lookup finding K servers next to key: a region of its own."""
            return FakeLookup([key[:15] + bytes([key[15] + 1 + i]) for i in range(K)])
        publisher, client, widened = self.publisher([], slow=True)
        publisher.make_lookup = lookup
        publisher.add_key_source(lambda: set(keys))
        publisher.refresh()
        result = publisher.publish_due()
        self.assertEqual(result["lookups"] + result["widened"], SLOW_ROUND_LOOKUPS)
        self.assertEqual(len(result["published"]), result["widened"])
        self.assertEqual(sorted(widened), sorted(item.key for item in result["published"]))
        self.assertTrue(all(publisher._is_due(item, publisher.clock()) for item in publisher._provided
                            if item not in result["published"]))


class SweepSpendTest(unittest.TestCase):
    """Sweep.spend and lookups."""

    def test_spend_within_the_limit(self):
        sweep = Sweep([ident(1), ident(2)])
        sweep.cover(Region(sweep.take(), 0, [ident(9)], {}))
        self.assertEqual(sweep.lookups, 1)
        self.assertTrue(sweep.spend(2))
        self.assertFalse(sweep.spend(2))
        self.assertTrue(sweep.spend(None))
        self.assertEqual((sweep.lookups, sweep.widened), (3, 2))


if __name__ == "__main__":
    unittest.main()
