"""H1 follow-up: the progress-earned displacement protection hurt honest requesters in some setups, and a requester
taking a little of each response could renew its slots.

- Attached to a shared instance (what the spec recommends), every incoming link arrives on the local interface to
  it (RNS LocalClientInterface, 1 Gbit/s), so a LoRa neighbour got a 3 s grace and a 125 kB/s floor: it was
  displaced and cooled down while downloading honestly. Its bitrate is now unknown.
- Hops beyond the first were priced at the node's own interface bitrate, so a TCP node serving a requester behind
  a LoRa segment gave it a 3 s grace where the first part request takes ~15 s. They are now priced at
  unmeasured_bitrate, or slow_bitrate on a path RNS raised the MTU of; and on the node's own slow radio the grace
  also covers a window of parts queued for each other response there.
- The per-interface fairness rule evicted responses from a busy interface at once, even ones just advertised. It
  now waits until a response is past half its grace, and evicts at most once per grace per interface.
- The rule that displaces the slowest moving response older than deadline_slack never cooled anyone down, so a
  requester taking one part of each response could keep its slots young from a spare link. Now it doesn't fire
  for a newcomer whose link or identity already holds a slot in the pool, and a response it displaces that is
  far behind the rate it needs counts as making no progress.
- The fast pool holds fast_blocks_responses (16) responses, so holding it takes many links; blocks_cooldown is
  300 s.

Driven over the fakes (tests/test_server.py, _server_helpers) with a FakeClock.
"""
import unittest

import RNS
from RNS.Interfaces.LocalInterface import LocalClientInterface

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.network import server
from tests.security._server_helpers import LORA_BITRATE, TCP_BITRATE, FakeInterface, PathLink, ServerHarness
from tests.support import FakeClock
from tests.test_server import block_of

LORA_RATE = 125  # bytes/s: what an honest 1 kbit/s LoRa neighbour downloads at


def config_with(**settings) -> Config:
    """A default Config with settings set as attributes (so these tests also run, and fail as they should, on code
    that predates a setting: there it is simply ignored)."""
    config = Config()
    for name, value in settings.items():
        setattr(config, name, value)
    return config


def shared_instance_interface() -> LocalClientInterface:
    """A real RNS LocalClientInterface as LocalClientInterface.connect() leaves it (no socket): the interface every
    link of a process attached to a shared instance arrives on (phase4 shared_instance_path.log)."""
    interface = LocalClientInterface.__new__(LocalClientInterface)
    interface.name = "Local shared instance"
    interface.socket_path = "\0rns/default"
    interface.bitrate = 1_000_000_000
    interface.is_connected_to_shared_instance = True
    return interface


class Download:
    """Drive a response's FakeResource as an honest requester would: its first part request arrives first_part
    seconds after the advertisement, then parts flow at rate bytes/s until it completes."""

    def __init__(self, harness: ServerHarness, link, first_part: float, rate: float):
        self.h = harness
        self.resource = harness.advertise(link)
        self.response = harness.response(link)
        self.first_part = first_part
        self.rate = rate
        self.started = harness.clock.now

    def step(self):
        """Update the Resource's progress for the clock's current time."""
        elapsed = self.h.clock.now - self.started
        if self.resource.status == RNS.Resource.COMPLETE or elapsed < self.first_part:
            return
        self.resource.status = RNS.Resource.TRANSFERRING
        self.resource.progress = min(1.0, (elapsed - self.first_part) * self.rate / self.response.size)
        if self.resource.progress >= 1.0:
            self.resource.complete()

    @property
    def done(self) -> bool:
        return self.resource.status == RNS.Resource.COMPLETE


class HonestAgainstNewcomersMixin:
    """An honest download alongside newcomers on the same interface that ask every 2 s and never download."""

    def run_alongside_newcomers(self, h: ServerHarness, download: Download, newcomer, cid):
        """Run until download completes; assert its response is never cancelled and keeps its slot."""
        t = 0
        while not download.done:
            self.assertLess(t, 3000, "the honest transfer never completed")
            t += 1
            h.clock.now += 1
            download.step()
            h.server.check_responses()
            self.assertFalse(download.resource.cancelled, f"the honest response was cancelled at {t}s")
            if t % 2 == 0:
                link = newcomer()
                if h.blocks(link, [cid]) is not False:
                    h.advertise(link)
            if not download.done:
                self.assertIs(h.response(download.resource.link), download.response,
                              f"the honest response lost its slot at {t}s")
        return t


class SharedInstanceTest(HonestAgainstNewcomersMixin, unittest.TestCase):
    """A shared-instance link's interface bitrate says nothing about its path: it is treated as unknown."""

    def setUp(self):
        (self.cid, self.block), (self.cid2, self.block2) = block_of(CHUNK_SIZE, 1), block_of(CHUNK_SIZE, 2)
        self.local = shared_instance_interface()

    def test_shared_instance_link_gets_the_unknown_bitrate_treatment(self):
        config = Config()
        lora_neighbour = PathLink(b"a" * 16, self.local, 1, mtu=500)
        interface, bitrate, hops = server.link_path(lora_neighbour)
        # H1 regression: fails until a link on the interface to a shared instance has no (1 Gbit/s) bitrate
        self.assertIsNone(bitrate, "the shared instance's local interface bitrate was taken as the path's")
        self.assertIs(interface, self.local)
        self.assertEqual(hops, 1)
        self.assertEqual(server.progress_floor(bitrate, hops, config), config.unmeasured_bitrate / 8)
        self.assertEqual(server.response_grace(bitrate, hops, config, server.fast_path(lora_neighbour)),
                         config.deadline_slack)
        # Any interface RNS tells apart as the one to the shared instance, whatever its class
        flagged = FakeInterface("to the shared instance", 1_000_000_000)
        flagged.is_connected_to_shared_instance = True
        self.assertIsNone(server.link_path(PathLink(b"b" * 16, flagged, 1))[1])
        self.assertEqual(server.link_path(PathLink(b"c" * 16, FakeInterface("TCP", TCP_BITRATE), 1))[1],
                         TCP_BITRATE)

    def test_honest_lora_neighbour_through_a_shared_instance_keeps_its_slot_and_never_cools_down(self):
        """A LoRa neighbour (MTU 500, one hop) downloads at ~125 B/s through the shared instance; newcomers on the
        same local interface ask every 2 s and never download."""
        h = ServerHarness({self.cid: self.block, self.cid2: self.block2})
        honest = h.connect(self.local, hops=1)
        self.assertIsNot(h.blocks(honest, [self.cid]), False)
        # First part request ~8 s after the advertisement: 4 s for the advertisement at 1 kbit/s, and back
        download = Download(h, honest, first_part=8, rate=LORA_RATE)
        self.assertEqual(download.response.pool, server.SLOW_POOL)
        t = self.run_alongside_newcomers(h, download, lambda: h.connect(self.local, hops=1), self.cid2)
        self.assertGreater(t, 500)  # many minutes alongside the newcomers
        # H1 regression: fails until the floor isn't 1/1000 of the local interface's 1 Gbit/s (125 kB/s)
        self.assertFalse(h.server.cooldowns.active(honest.link_id), "the honest LoRa neighbour was cooled down")


class MultiHopGraceTest(HonestAgainstNewcomersMixin, unittest.TestCase):
    """Hops beyond the first are priced at unmeasured_bitrate, unless RNS raised the path's MTU."""

    def setUp(self):
        (self.cid, self.block), (self.cid2, self.block2) = block_of(CHUNK_SIZE, 1), block_of(CHUNK_SIZE, 2)
        self.tcp = FakeInterface("TCP to a transport node", TCP_BITRATE)

    def test_tcp_node_serving_a_three_hop_lora_requester(self):
        """The requester is 3 hops away, behind a transport node with a LoRa segment (so the link's MTU stays 500):
        its first part request arrives ~15 s after the advertisement. Newcomers ask every 2 s from t=2 s."""
        h = ServerHarness({self.cid: self.block, self.cid2: self.block2})
        honest = h.connect(self.tcp, hops=3, mtu=500, mdu=431)
        self.assertIsNot(h.blocks(honest, [self.cid]), False)
        download = Download(h, honest, first_part=15, rate=LORA_RATE)
        # H1 regression: fails until hops past the first aren't priced at the TCP interface's 10 Mbit/s
        self.assertGreaterEqual(download.response.grace, 15 + 2, "the grace doesn't cover the LoRa segment")
        self.run_alongside_newcomers(h, download, lambda: h.connect(self.tcp, hops=1, mtu=500, mdu=431), self.cid2)
        self.assertFalse(h.server.cooldowns.active(honest.link_id))
        # On a path RNS raised the MTU of, there is no slow radio: the grace stays short at any hop count
        self.assertLess(server.response_grace(TCP_BITRATE, 3, h.config, fast=True), 4)
        fast = h.connect(self.tcp, hops=3, mtu=1500, mdu=1400)
        self.assertIsNot(h.blocks(fast, [self.cid]), False)
        self.assertLess(h.response(fast).grace, 4)

    def test_grace_covers_advertisements_queued_on_the_nodes_own_slow_radio(self):
        """On the node's own 1 kbit/s radio, a new advertisement may wait behind a window of another response's
        parts (with the radio's flow control off, RNS can't tell when it actually leaves)."""
        lora = FakeInterface("RNode", LORA_BITRATE)
        h = ServerHarness({self.cid: self.block, self.cid2: self.block2}, Config(deadline_slack=60))
        first = h.connect(lora, hops=1)
        self.assertIsNot(h.blocks(first, [self.cid]), False)
        h.advertise(first)
        alone = h.response(first).grace
        self.assertAlmostEqual(alone, server.GRACE_BASE + 2 * 4)
        second = h.connect(lora, hops=1)
        self.assertIsNot(h.blocks(second, [self.cid2]), False)
        # H1 regression: fails until the grace also covers what may be queued ahead on the node's own radio
        self.assertGreaterEqual(h.response(second).grace, alone + 10 * 500 * 8 / LORA_BITRATE)
        # Not on a fast interface, whose queue drains in no time
        tcp = FakeInterface("TCP", TCP_BITRATE)
        a, b = h.connect(tcp, mtu=1500, mdu=1400), h.connect(tcp, mtu=1500, mdu=1400)
        self.assertIsNot(h.blocks(a, [self.cid]), False)
        self.assertIsNot(h.blocks(b, [self.cid]), False)
        self.assertEqual(h.response(a).grace, h.response(b).grace)


class InterfaceFairnessTest(unittest.TestCase):
    """The per-interface fairness rule (rule 2) spares young responses and fires at most once per grace."""

    GRACE = 10.0
    FLOOR = 100.0

    def setUp(self):
        self.clock = FakeClock()
        self.lora = FakeInterface("LoRa", LORA_BITRATE)
        self.tcp = FakeInterface("TCP", TCP_BITRATE)

    def take(self, pools, name: bytes, interface):
        return pools.reserve(server.FAST_POOL, name, name, grace=self.GRACE, floor=self.FLOOR, interface=interface)

    def test_responses_early_in_their_grace_are_not_evicted(self):
        """A requester on another interface (e.g. TCP, signalling MTU 500 to land in the slow pool) can't evict
        responses the node advertised over LoRa a moment ago."""
        pools = server.ResponsePools(4, 30, self.clock)
        a, _ = self.take(pools, b"a", self.lora)
        b, _ = self.take(pools, b"b", self.lora)
        self.clock.now += self.GRACE / 2 - 0.1
        # H1 regression: fails until rule 2 waits for the first half of a response's grace
        self.assertEqual(self.take(pools, b"c", self.tcp), (None, None), "a response just advertised was evicted")
        self.clock.now += 0.2
        response, displaced = self.take(pools, b"c", self.tcp)
        self.assertIsNotNone(response)
        self.assertIs(displaced, a)
        self.assertFalse(displaced.idle)  # still within its grace: no cooldown

    def test_at_most_one_eviction_per_grace_per_interface(self):
        pools = server.ResponsePools(6, 30, self.clock)  # 3 slots per pool
        held = [self.take(pools, bytes([i]) * 4, self.lora)[0] for i in range(3)]
        self.clock.now += self.GRACE / 2 + 0.1
        response, displaced = self.take(pools, b"tcp1", self.tcp)
        self.assertIs(displaced, held[0])
        # LoRa still holds two, but it has just lost one this way; another interface's newcomer waits
        other = FakeInterface("TCP 2", TCP_BITRATE)
        # H1 regression: fails until rule 2 evicts at most once per grace per interface
        self.assertEqual(self.take(pools, b"tcp2", other), (None, None), "rule 2 evicted twice within one grace")
        self.assertEqual(set(pools.responses()), {held[1], held[2], response})


class SelfRenewalTest(unittest.TestCase):
    """The slowest-response rule (rule 3) can't be used to keep one's own slots young for free."""

    def setUp(self):
        self.blocks = dict(block_of(CHUNK_SIZE, i) for i in range(4))
        self.cids = list(self.blocks)
        self.hub = FakeInterface("TCP hub", TCP_BITRATE)

    def harness(self) -> ServerHarness:
        return ServerHarness(self.blocks, config_with(fast_blocks_responses=2))

    def link(self, h: ServerHarness, hops=2):
        """A link over the TCP hub on a path RNS raised the MTU of, reporting hops hops."""
        return h.connect(self.hub, hops=hops, mtu=1500, mdu=1400)

    def take_one_part(self, h: ServerHarness, link, cid, identity=None):
        """Ask for cid on link, accept the advertisement, and take one part (1400 bytes) only."""
        self.assertIsNot(h.blocks(link, [cid], identity=identity), False)
        resource = h.advertise(link)
        resource.status = RNS.Resource.TRANSFERRING
        resource.progress = 1400 / h.response(link).size
        h.tick(0.25)
        return resource

    def test_slow_requester_renewing_its_slots_from_a_spare_link_cools_down(self):
        h = self.harness()
        a, b, spare = self.link(h), self.link(h), self.link(h)
        held = [self.take_one_part(h, a, self.cids[0]), self.take_one_part(h, b, self.cids[1])]
        # One part of 64 KiB, then nothing: above the 12.5 B/s floor behind two hops for ~110 s, so protected
        h.tick(h.config.deadline_slack + 1)
        self.assertTrue(all(h.response(link) is not None for link in (a, b)))
        self.assertIsNot(h.blocks(spare, [self.cids[2]]), False)  # rule 3: displaces the slowest
        displaced = a if held[0].cancelled else b
        self.assertTrue(held[0].cancelled or held[1].cancelled)
        # H1 residual: fails until a response displaced far behind the rate it needs counts as making no progress
        self.assertTrue(h.server.cooldowns.active(displaced.link_id), "the renewing requester wasn't cooled down")
        self.assertIs(h.blocks(displaced, [self.cids[3]]), False)

    def test_honest_download_displaced_by_the_slowest_rule_does_not_cool_down(self):
        """The counterpart: a response keeping to its schedule, displaced only for being the slowest, doesn't."""
        h = self.harness()
        a, b = self.link(h), self.link(h)
        ra, rb = self.take_one_part(h, a, self.cids[0]), self.take_one_part(h, b, self.cids[1])
        for _ in range(int(h.config.deadline_slack) + 2):
            for resource, rate in ((ra, 1500), (rb, 2000)):  # ~1.5-2 kB/s: a slow but honest fast-path download
                resource.progress = min(0.99, resource.progress + rate / h.response(resource.link).size)
            h.tick(1)
        self.assertIsNot(h.blocks(self.link(h), [self.cids[2]]), False)
        self.assertTrue(ra.cancelled)
        self.assertFalse(h.server.cooldowns.active(a.link_id))

    def test_requester_already_holding_a_slot_cannot_displace_by_the_slowest_rule(self):
        h = self.harness()
        identity = RNS.Identity()
        mine, other = self.link(h), self.link(h)
        rm = self.take_one_part(h, mine, self.cids[0], identity=identity)
        ro = self.take_one_part(h, other, self.cids[1])
        for _ in range(int(h.config.deadline_slack) + 2):
            for resource in (rm, ro):  # both moving well: neither is far behind
                resource.progress = min(0.99, resource.progress + 4000 / h.response(resource.link).size)
            h.tick(1)
        # H1 residual: fails until rule 3 refuses a newcomer whose identity already holds a slot in the pool
        self.assertIs(h.blocks(self.link(h), [self.cids[2]], identity=identity), False,
                      "a requester displaced a response to renew its own slots")
        self.assertFalse(rm.cancelled or ro.cancelled)
        self.assertIsNot(h.blocks(self.link(h), [self.cids[2]], identity=RNS.Identity()), False)


class FastPoolCapacityTest(unittest.TestCase):
    """Many more fast-link slots by default (fast_blocks_responses), the slow pool unchanged, a longer cooldown."""

    def test_defaults(self):
        config = Config()
        # H1: fails until the fast pool holds fast_blocks_responses (16) and blocks_cooldown is 300 s
        self.assertEqual(getattr(config, "fast_blocks_responses", None), 16)
        self.assertEqual(config.blocks_cooldown, 300)
        h = ServerHarness({})
        self.assertEqual(h.server.pools.capacity, {server.SLOW_POOL: 2, server.FAST_POOL: 16})

    def test_sixteen_fast_links_are_served_at_once_and_the_slow_pool_is_unchanged(self):
        cid, block = block_of(CHUNK_SIZE, 1)
        h = ServerHarness({cid: block})
        tcp, lora = FakeInterface("TCP", TCP_BITRATE), FakeInterface("LoRa", LORA_BITRATE)
        served = [h.blocks(h.connect(tcp, mtu=1500, mdu=1400), [cid]) is not False for _ in range(17)]
        self.assertEqual(served, [True] * 16 + [False])
        served = [h.blocks(h.connect(lora), [cid]) is not False for _ in range(3)]
        self.assertEqual(served, [True, True, False])

    def test_setting_is_validated_and_never_shrinks_the_old_split(self):
        for bad in (0, -1, True, "9", 1.5, None):
            with self.assertRaises(ValueError, msg=repr(bad)):
                config_with(fast_blocks_responses=bad).validate()
        config_with(fast_blocks_responses=1).validate()
        capacity = ServerHarness({}, config_with(fast_blocks_responses=1)).server.pools.capacity
        self.assertEqual(capacity, {server.SLOW_POOL: 2, server.FAST_POOL: 2})
        config = config_with(fast_blocks_responses=16)
        config.max_blocks_responses = 60
        self.assertEqual(ServerHarness({}, config).server.pools.capacity, {server.SLOW_POOL: 30, server.FAST_POOL: 30})


if __name__ == "__main__":
    unittest.main()
