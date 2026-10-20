"""Tests for sweep providing (rnfs/dht/sweep.py): regions, the far half's extra /find, the order lookups take keys in,
and placement over a simulated network against per-key lookups and the true closest servers."""
import os
import random
import tempfile
import threading
import unittest

from rnfs.config import Config
from rnfs.constants import ID_LENGTH, K
from rnfs.content.storage import Storage
from rnfs.dht.lookup import FIRST, SLOW_PATH, UNRELIABLE, Lookup
from rnfs.dht.providers import ProviderStore
from rnfs.dht.routing import RoutingTable, distance
from rnfs.dht.sweep import (ID_BITS, MAX_REGION_FINDS, REGION_FIND_TRIES, Region, Sweep, explore, find_region,
                            prefix_length)
from rnfs.publisher import SLOW_ROUND_LOOKUPS, DhtKey, Publisher
from tests.fakes.client import FakeClient
from tests.fakes.log import quiet_log
from tests.support import FakeClock
from tests.test_lookup import CONFIG, FakeNetwork
from tests.test_publisher import FakeLookup

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


class SimulatedNetworkTest(unittest.TestCase):
    """Sweep providing over a simulated network of 1000 servers (Kademlia routing tables, real Lookups): placement
    against per-key lookups and the true closest, and the lookups and extra /find it costs."""

    SERVERS = 1000

    @classmethod
    def setUpClass(cls):
        """One network for the class (it takes a few seconds to build), and the publisher's routing table."""
        cls.net = FakeNetwork(cls.SERVERS, seed=41)
        cls.table = RoutingTable(OWN, CONFIG, clock=cls.net.clock)
        for node_id in cls.net.nodes:
            cls.table.heard(node_id)

    def make_lookup(self, key: bytes) -> Lookup:
        """A lookup for key from the publisher's routing table, as the node makes one."""
        return Lookup(key, self.table.closest(key, K), self.net.query, OWN, timeout=30, poll_interval=0.05)

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
        per-key lookups do; lookups level off at about servers / 12.5, with under 3 extra /find a region.

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
        self.assertLessEqual(result["lookups"], self.SERVERS / 12.5 * 1.3)
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
        published = publisher._published[next(item for item in result["published"] if item.key == keys[0])][1]
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
