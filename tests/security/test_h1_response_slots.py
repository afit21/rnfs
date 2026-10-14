"""H1: one peer could hold every /blocks response slot by never downloading its responses.

A full pool used to protect any response younger than deadline_slack (30 s), even one with nothing sent, so a
requester re-requesting before then kept every slot. Now a response is protected only for a grace the node
measures from the interface and hop count its request came over, and after that only while it makes progress
at a floor; a requester whose response was displaced or cancelled for making no progress cools down, stalled
responses are cancelled and refunded, and a newcomer on an interface holding no slot may displace a response
that has sent nothing from an interface holding two. These tests drive the policy (ResponsePools) and the
RequestServer over fake RNS objects and a FakeClock.
"""
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.network import server
from tests.security._server_helpers import LORA_BITRATE, TCP_BITRATE, FakeInterface, PathLink, ServerHarness
from tests.support import FakeClock
from tests.test_server import block_of


class GraceAndFloorTest(unittest.TestCase):
    """The grace and the progress floor come from the node's own interface bitrate and the hop count."""

    def setUp(self):
        self.config = Config()

    def grace(self, bitrate, hops):
        return server.response_grace(bitrate, hops, self.config)

    def test_grace_grows_with_hops_and_shrinks_with_bitrate(self):
        self.assertLess(self.grace(LORA_BITRATE, 1), self.grace(LORA_BITRATE, 2))
        self.assertLess(self.grace(LORA_BITRATE, 2), self.grace(LORA_BITRATE, 3))
        self.assertGreater(self.grace(LORA_BITRATE, 2), self.grace(2 * LORA_BITRATE, 2))
        # A ~500-byte advertisement over 3 hops at 1 kbit/s: ~4 s per hop each way, so the first part request
        # arrives after ~24 s, inside the grace
        self.assertGreater(self.grace(LORA_BITRATE, 3), 24 + 2)
        # Over TCP the hop count hardly matters: a few seconds
        self.assertLess(self.grace(TCP_BITRATE, 1), 5)
        self.assertLess(self.grace(TCP_BITRATE, 30), 5)

    def test_grace_is_clamped(self):
        self.assertEqual(self.grace(LORA_BITRATE, 10), self.config.deadline_slack)
        # The requester can raise its hop count; that never takes the grace past deadline_slack
        self.assertEqual(self.grace(LORA_BITRATE, 255), self.config.deadline_slack)
        self.assertEqual(self.grace(TCP_BITRATE, 255), self.grace(TCP_BITRATE, server.MAX_GRACE_HOPS))
        self.assertGreaterEqual(self.grace(1e12, 1), server.GRACE_BASE)
        # An unknown bitrate is taken as unmeasured_bitrate: the old 30 s protection
        self.assertEqual(self.grace(None, 1), self.config.deadline_slack)
        self.assertEqual(self.grace(TCP_BITRATE, None), self.grace(TCP_BITRATE, 1))
        self.assertEqual(server.response_grace(TCP_BITRATE, 1, Config(deadline_slack=1)), 1)

    def test_floor(self):
        minimum = self.config.unmeasured_bitrate / 8
        self.assertEqual(server.progress_floor(TCP_BITRATE, 1, self.config), TCP_BITRATE / 1000 / 8)
        # Behind more hops the path may be far slower than our interface: only the minimum applies
        self.assertEqual(server.progress_floor(TCP_BITRATE, 2, self.config), minimum)
        self.assertEqual(server.progress_floor(LORA_BITRATE, 1, self.config), minimum)
        self.assertEqual(server.progress_floor(None, 1, self.config), minimum)

    def test_link_path_reads_rns_attributes_defensively(self):
        lora = FakeInterface("LoRa", LORA_BITRATE)
        self.assertEqual(server.link_path(PathLink(b"a" * 16, lora, 3)), (lora, LORA_BITRATE, 3))
        self.assertEqual(server.link_path(object()), (None, None, None))
        for bitrate in (None, 0, -5, True, "fast", float("nan"), float("inf")):
            self.assertIsNone(server.link_path(PathLink(b"a" * 16, FakeInterface("x", bitrate)))[1], bitrate)
        for hops in (None, -1, True, "3", 2.0):
            self.assertIsNone(server.link_path(PathLink(b"a" * 16, lora, hops))[2], hops)


class ResponsePoolsPolicyTest(unittest.TestCase):
    """Displacement in a full pool: protection is earned by progress."""

    GRACE = 5.0
    FLOOR = 100.0  # bytes/s

    def setUp(self):
        """Pools of 4 slots (2 per pool) with 30 s slack."""
        self.clock = FakeClock()
        self.pools = server.ResponsePools(4, 30, self.clock)
        self.hub = FakeInterface("hub", TCP_BITRATE)

    def take(self, name: bytes, interface=None):
        """Reserve a fast-pool slot for link name."""
        return self.pools.reserve(server.FAST_POOL, name, name, grace=self.GRACE, floor=self.FLOOR,
                                  interface=interface if interface is not None else self.hub)

    def move(self, seconds: float, *moving):
        """Each (response, rate) in moving sends at rate bytes/s for seconds, as the monitor reads it each second."""
        for _ in range(int(seconds)):
            self.clock.now += 1
            for response, rate in moving:
                response.note_progress(response.sent + rate, self.clock.now)

    def test_zero_progress_becomes_displaceable_after_its_grace_not_before(self):
        a, _ = self.take(b"a")
        b, _ = self.take(b"b")
        self.clock.now += self.GRACE
        self.assertEqual(self.take(b"c"), (None, None))
        self.clock.now += 0.1
        response, displaced = self.take(b"c")
        self.assertIsNotNone(response)
        self.assertIn(displaced, (a, b))
        self.assertTrue(displaced.idle)  # its requester cools down

    def test_moving_response_keeps_its_slot_after_grace(self):
        a, _ = self.take(b"a")
        b, _ = self.take(b"b")
        self.move(10, (a, 2 * self.FLOOR))
        response, displaced = self.take(b"c")
        self.assertIs(displaced, b)  # nothing sent: displaced; a is moving
        for i in range(20):  # well past both graces, but not older than deadline_slack
            self.move(1, (a, 2 * self.FLOOR), (response, self.FLOOR))
            self.assertEqual(self.take(bytes([i]) * 4), (None, None), i)
        self.assertEqual(set(self.pools.responses()), {a, response})

    def test_trickle_below_the_floor_is_not_protected(self):
        a, _ = self.take(b"a")
        b, _ = self.take(b"b")
        self.move(10, (a, 2 * self.FLOOR), (b, self.FLOOR / 10))  # b sends something, but far below the floor
        self.assertIs(self.take(b"c")[1], b)

    def test_least_progress_is_displaced_first(self):
        a, _ = self.take(b"a")
        b, _ = self.take(b"b")
        self.clock.now += self.GRACE + 1
        a.note_progress(1, self.clock.now)  # a byte: still below the floor, but more than b
        self.assertIs(self.take(b"c")[1], b)

    def test_link_cannot_displace_its_own_response(self):
        a, _ = self.take(b"a")
        b, _ = self.take(b"b")
        self.move(10, (b, 2 * self.FLOOR))
        # a has sent nothing and is past its grace, but only another link may take its slot
        self.assertEqual(self.take(b"a"), (None, None))
        self.assertIs(self.take(b"c")[1], a)

    def test_moving_responses_follow_the_old_slowest_rule(self):
        a, _ = self.take(b"a")
        b, _ = self.take(b"b")
        self.move(30, (a, 4 * self.FLOOR), (b, 2 * self.FLOOR))
        self.assertEqual(self.take(b"c"), (None, None))  # both moving, not older than deadline_slack
        self.move(1, (a, 4 * self.FLOOR), (b, 2 * self.FLOOR))
        response, displaced = self.take(b"c")
        self.assertIs(displaced, b)
        self.assertFalse(displaced.idle)  # it was moving: no cooldown

    def test_newcomer_from_an_unrepresented_interface_takes_a_slot_from_a_crowded_one(self):
        other = FakeInterface("radio", LORA_BITRATE)
        a, _ = self.take(b"a")
        self.clock.now += 1
        b, _ = self.take(b"b")
        # Both within their grace and nothing sent, both from the hub: a newcomer from the hub is refused
        self.assertEqual(self.take(b"c"), (None, None))
        response, displaced = self.take(b"d", other)
        self.assertIsNotNone(response)
        self.assertIs(displaced, a)  # the oldest
        self.assertFalse(displaced.idle)  # displaced within its grace: no cooldown
        # Now each interface holds a slot: nobody else gets in during the grace
        self.assertEqual(self.take(b"e", other), (None, None))
        self.assertEqual(self.take(b"f", FakeInterface("third", TCP_BITRATE)), (None, None))
        self.assertEqual(set(self.pools.responses()), {b, response})

    def test_crowded_interface_keeps_responses_that_are_moving(self):
        a, _ = self.take(b"a")
        b, _ = self.take(b"b")
        self.clock.now += 1
        a.note_progress(1000, self.clock.now)
        b.note_progress(1000, self.clock.now)
        self.assertEqual(self.take(b"c", FakeInterface("radio", LORA_BITRATE)), (None, None))


class RequestServerSlotsTest(unittest.TestCase):
    """The policy wired to RNS (faked): grace from the link's path, cooldowns, stall cancellation."""

    def setUp(self):
        (self.cid, self.block), (self.cid2, self.block2) = block_of(CHUNK_SIZE, 1), block_of(CHUNK_SIZE, 2)
        self.hub = FakeInterface("TCP hub", TCP_BITRATE)

    def harness(self, config=None) -> ServerHarness:
        h = ServerHarness({self.cid: self.block, self.cid2: self.block2}, config)
        self.server = h.server
        return h

    def tcp(self, h: ServerHarness, hops=1, interface=None) -> PathLink:
        """A fast-pool link over the TCP hub."""
        return h.connect(interface or self.hub, hops, mtu=1500, mdu=1400)

    def take(self, h, link, identity=None):
        """A /blocks request on link that must be accepted, and its Resource advertised."""
        self.assertIsNot(h.blocks(link, [self.cid], identity=identity), False)
        return h.advertise(link)

    def test_honest_slow_multi_hop_transfer_keeps_its_slot_and_completes(self):
        """~1 kbit/s, 3 hops: the first part request arrives ~24 s after the advertisement. Meanwhile and
        afterwards newcomers on the same interface ask every 2 s and never download anything."""
        h = self.harness()
        lora = FakeInterface("LoRa", LORA_BITRATE)
        honest = h.connect(lora, hops=3)
        resource = self.take(h, honest)
        response = h.response(honest)
        self.assertEqual(response.pool, server.SLOW_POOL)
        self.assertGreater(response.grace, 24)
        rate = LORA_BITRATE / 8 * 0.8  # what the channel really delivers
        newcomers = []
        t = 0
        while resource.status != RNS.Resource.COMPLETE:
            self.assertLess(t, 2000, "the honest transfer never completed")
            t += 1
            h.clock.now += 1
            if t >= 24:  # its first part request has arrived: parts flow at the channel's rate
                resource.status = RNS.Resource.TRANSFERRING
                resource.progress = min(1.0, (t - 24) * rate / response.size)
                if resource.progress >= 1.0:
                    resource.complete()
            h.server.check_responses()
            self.assertFalse(resource.cancelled, f"the honest response was cancelled at {t}s")
            if t % 2 == 0:
                link = h.connect(lora, hops=1)
                if h.blocks(link, [self.cid2]) is not False:
                    newcomers.append(h.advertise(link))
            if resource.status != RNS.Resource.COMPLETE:
                self.assertIs(h.response(honest), response, f"the honest response lost its slot at {t}s")
        self.assertIsNone(h.response(honest))
        self.assertGreater(t, 500)  # it really did run for many minutes alongside the newcomers
        # The newcomers did take the other slot in turn, each losing it for making no progress
        self.assertGreater(len(newcomers), 20)
        self.assertGreater(sum(1 for r in newcomers if r.cancelled), 20)

    def test_zero_progress_response_is_displaced_after_its_grace_and_its_link_cools_down(self):
        h = self.harness()
        a, b = self.tcp(h), self.tcp(h)
        held_a, held_b = self.take(h, a), self.take(h, b)
        grace = h.response(a).grace
        self.assertLess(grace, 5)
        h.tick(grace - 0.01)
        c = self.tcp(h)
        self.assertIs(h.blocks(c, [self.cid]), False)  # both still within their grace
        h.tick(0.02)
        self.assertIsNot(h.blocks(c, [self.cid]), False)
        displaced, kept = (a, b) if held_a.cancelled else (b, a)
        self.assertTrue((held_a if displaced is a else held_b).cancelled)
        self.assertIsNotNone(h.response(kept))
        self.assertIsNone(h.response(displaced))
        # The displaced link is refused while it cools down, even with a slot free
        h.server._cancel(h.response(kept), "test: free a slot")
        self.assertEqual(h.server.pools.count(server.FAST_POOL), 1)
        self.assertIs(h.blocks(displaced, [self.cid]), False)
        self.assertEqual(h.server.pools.count(server.FAST_POOL), 1)
        h.tick(h.config.blocks_cooldown - 1)
        self.assertIs(h.blocks(displaced, [self.cid]), False)
        h.tick(1.1)
        self.assertIsNot(h.blocks(displaced, [self.cid]), False)

    def test_identity_cools_down_with_its_link(self):
        h = self.harness()
        identity = RNS.Identity()
        a = self.tcp(h)
        self.take(h, a, identity)
        self.take(h, self.tcp(h))
        h.tick(h.response(a).grace + 0.1)
        h.server.pools.responses()[1].note_progress(10_000, h.clock.now)  # the other one is moving
        self.assertIsNot(h.blocks(self.tcp(h), [self.cid]), False)  # displaces a
        self.assertIsNone(h.response(a))
        self.assertTrue(h.server.cooldowns.active(a.link_id, bytes(identity.hash)))
        h.server.pools.active[server.FAST_POOL].clear()  # free every slot
        self.assertIs(h.blocks(self.tcp(h), [self.cid], identity=identity), False)  # a new link, same identity
        self.assertIsNot(h.blocks(self.tcp(h), [self.cid], identity=RNS.Identity()), False)

    def test_cooldown_zero_turns_it_off(self):
        h = self.harness(Config(blocks_cooldown=0))
        a = self.tcp(h)
        self.take(h, a)
        self.take(h, self.tcp(h))
        h.tick(5)
        self.assertIsNot(h.blocks(self.tcp(h), [self.cid]), False)
        self.assertFalse(h.server.cooldowns.active(a.link_id))

    def refunds(self, h: ServerHarness) -> list[int]:
        """The amounts given back to upload_budget from now on."""
        refunds = []
        refund = h.server.airtime.refund
        h.server.airtime.refund = lambda amount: refunds.append(amount) or refund(amount)
        return refunds

    def test_zero_progress_response_is_cancelled_and_refunded_without_newcomers(self):
        h = self.harness(Config(upload_budget=1000 * 1024))
        refunds = self.refunds(h)
        a = self.tcp(h)
        resource = self.take(h, a)
        response = h.response(a)
        charged = response.charged  # charged as sent: only a reservation so far
        self.assertGreater(response.cost, CHUNK_SIZE)
        h.tick(response.grace + h.config.deadline_slack - 0.01)
        self.assertFalse(resource.cancelled)
        h.tick(0.5)
        self.assertTrue(resource.cancelled)
        self.assertIsNone(h.response(a))
        self.assertEqual(refunds, [charged])  # nothing was sent: all of it
        self.assertTrue(h.server.cooldowns.active(a.link_id))

    def test_stalled_response_is_cancelled_and_refunded(self):
        h = self.harness(Config(upload_budget=1000 * 1024))
        refunds = self.refunds(h)
        a = self.tcp(h, hops=2)
        resource = self.take(h, a)
        resource.status = RNS.Resource.TRANSFERRING
        response = h.response(a)
        for second in range(1, 11):
            resource.progress = 0.03 * second
            h.tick(1)
        h.tick(h.config.stall_timeout)
        self.assertFalse(resource.cancelled)
        h.tick(1)
        self.assertTrue(resource.cancelled)
        self.assertIsNone(h.response(a))
        self.assertEqual(sum(refunds), 0)  # charged as it was sent, so nothing to give back
        self.assertAlmostEqual(response.charged, 0.3 * response.size, delta=2)  # what was sent
        self.assertFalse(h.server.cooldowns.active(a.link_id))  # it was moving well above its floor

    def test_complete_resource_is_concluded_not_cancelled(self):
        """RNS's Resource.cancel() does nothing once a Resource is COMPLETE; a response whose Resource completed
        between two monitor passes, with no progress seen, is concluded, not cancelled."""
        h = self.harness()
        a = self.tcp(h)
        resource = self.take(h, a)
        resource.status = RNS.Resource.COMPLETE
        h.tick(h.response(a).grace + h.config.deadline_slack + 1)
        self.assertFalse(resource.cancelled)
        self.assertIsNone(h.response(a))
        self.assertFalse(h.server.cooldowns.active(a.link_id))

    def test_newcomer_on_another_interface_is_served_while_one_interface_holds_both_slots(self):
        h = self.harness()
        a, b = self.tcp(h), self.tcp(h)
        held_a = self.take(h, a)
        h.tick(1)
        self.take(h, b)
        self.assertIs(h.blocks(self.tcp(h), [self.cid]), False)  # same interface: refused during the grace
        other = self.tcp(h, interface=FakeInterface("TCP to a second hub", TCP_BITRATE))
        self.assertIsNot(h.blocks(other, [self.cid]), False)
        self.assertTrue(held_a.cancelled)  # the oldest response from the crowded interface
        self.assertFalse(h.server.cooldowns.active(a.link_id))  # it was still within its grace
        self.assertIsNotNone(h.response(b))

    def test_cooldown_refusal_is_cheap(self):
        """A link cooling down is refused before any slot is reserved or any block is read."""
        h = self.harness()
        a = self.tcp(h)
        h.server.cooldowns.add(a.link_id)
        reads = []
        served = h.backend.served_block
        h.backend.served_block = lambda cid: reads.append(cid) or served(cid)
        self.assertIs(h.blocks(a, [self.cid]), False)
        self.assertEqual(reads, [])
        self.assertEqual(h.server.pools.responses(), [])
        self.assertEqual(h.server.incoming.in_progress(a.link_id), (0, 0))

    def test_closed_link_cooldown_is_forgotten(self):
        h = self.harness()
        a = self.tcp(h)
        h.server.cooldowns.add(a.link_id)
        a.teardown()
        self.assertNotIn(a.link_id, h.server.cooldowns.until)


class CooldownsTest(unittest.TestCase):
    """The cooldown table: expiry and a bounded memory."""

    def test_expiry_and_bound(self):
        clock = FakeClock()
        cooldowns = server.Cooldowns(60, clock)
        cooldowns.add(b"a", None)
        clock.now += 59
        self.assertTrue(cooldowns.active(b"x", b"a"))
        clock.now += 1
        self.assertFalse(cooldowns.active(b"a"))
        for i in range(server.COOLDOWN_MEMORY + 10):
            cooldowns.add(i.to_bytes(4, "big"))
        self.assertEqual(len(cooldowns.until), server.COOLDOWN_MEMORY)
        self.assertFalse(cooldowns.active((0).to_bytes(4, "big")))
        self.assertTrue(cooldowns.active((server.COOLDOWN_MEMORY + 9).to_bytes(4, "big")))


if __name__ == "__main__":
    unittest.main()
