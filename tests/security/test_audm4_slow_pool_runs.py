"""AUD-M4 (0.2.1 audit, an H1 variant): requesters on a fast path posing as slow held the 2-slot slow pool for good.

The attack: a link is slow (the slow pool) by what its requester decides: a link MTU of 500, or a rate estimate it can
keep low or never let the node take (responses below min_rate_sample give none). Two links over TCP doing that, each
downloading a 15 KiB response in about 4 s at ~30 kbit/s and asking again at once, kept both slow-pool slots holding
young, moving responses. No displacement rule (ResponsePools._displaceable) fired for a newcomer: none was idle (all
moving), the fairness rule takes only responses with nothing sent, and the slowest-response rule only one older than
deadline_slack, which a fresh response never is. The auditor's repro: over 600 simulated seconds a LoRa requester
asking every 7 s got 86 refusals and no slot.

The tests rebuild it on ServerHarness: two attacker links in the slow pool completing a response every 4 s and asking
again at once, and a newcomer on a LoRa interface asking every 7 s. Once with the attackers on a TCP interface (a
known bitrate above slow_bitrate), once on an interface of unknown bitrate (as every link is on a node attached to a
shared instance).

The fix (ResponsePools):
- A slow-pool response's age, for the slowest-response rule, counts from the start of its link's (and identity's)
  back-to-back run of responses: one reserved within deadline_slack of the end of the last on that link or identity
  continues its run (Response.run_start). A response continuing a run older than deadline_slack has had its grace in
  that run, so the rule may take it within its own grace (without a cooldown).
- In the slow pool, a newcomer not on a known-fast interface may displace one of the responses there when every one
  of them is on a known-fast interface (one of known bitrate at least slow_bitrate): the one that has sent least,
  newest on a tie, without a cooldown. So a slot of the pool meant for slow links stays reachable from them.
"""
import unittest

import RNS

from rnfs.network import server
from tests.security._server_helpers import LORA_BITRATE, TCP_BITRATE, FakeInterface, ServerHarness
from tests.test_server import block_of

TRANSFER = 4.0  # seconds each attacker response takes (~30 kbit/s)
STEP = 0.5


class SlowPoolRunTest(unittest.TestCase):
    def setUp(self):
        RNS.loglevel = RNS.LOG_CRITICAL
        self.blocks = [block_of(15 * 1024, i) for i in range(64)]  # under min_rate_sample: never a rate estimate
        self.h = ServerHarness(dict(self.blocks))
        self.lora = FakeInterface("lora", LORA_BITRATE)
        self.state = {}  # link ID -> (its response's resource, when it started)
        self.count = 0

    def ask(self, link, i: int) -> bool:
        """A /blocks for one block on link; True if accepted (its Resource then advertised)."""
        self.count += 1
        request_id = self.count.to_bytes(16, "big")
        if self.h.blocks(link, [self.blocks[i % len(self.blocks)][0]], max_bytes=0, request_id=request_id) is False:
            return False
        self.state[link.link_id] = (self.h.advertise(link, request_id), self.h.clock.now)
        return True

    def attack(self, interface, seconds: float = 600) -> tuple[int, float | None]:
        """Two attacker links on interface keep the slow pool busy; a LoRa newcomer asks every 7 s. (Its refusals,
        seconds until its first admission or None.)"""
        attackers = self.attackers = [self.h.connect(interface, hops=1, mtu=500, mdu=431) for _ in range(2)]
        newcomer = self.h.connect(self.lora, hops=1, mtu=500, mdu=431)
        for n, link in enumerate(attackers):
            self.assertTrue(self.ask(link, n))
        start, refused = self.h.clock.now, 0
        for tick in range(int(seconds / STEP)):
            self.h.tick(STEP)
            for n, link in enumerate(attackers):
                resource, started = self.state.get(link.link_id, (None, 0))
                if resource is None or resource.status in (RNS.Resource.FAILED, RNS.Resource.COMPLETE):
                    self.ask(link, tick + n)  # displaced (or done): straight back for another
                    continue
                elapsed = self.h.clock.now - started
                resource.progress = min(1.0, elapsed / TRANSFER)
                if elapsed >= TRANSFER:
                    resource.complete()
                    self.ask(link, tick + n)  # and at once the next one
            if tick % 14 == 0:
                if self.ask(newcomer, 7):
                    return refused, self.h.clock.now - start
                refused += 1
        return refused, None

    def test_known_fast_attackers_leave_a_slot_for_a_slow_interface(self):
        refused, admitted_after = self.attack(FakeInterface("tcp", TCP_BITRATE))
        self.assertIsNotNone(admitted_after, f"the LoRa requester was refused {refused} times and never admitted")
        self.assertEqual(refused, 0)

    def test_back_to_back_responses_age_as_one_run(self):
        # Unknown bitrate (a shared instance's links): nothing is known fast, so only the run's age can help
        refused, admitted_after = self.attack(FakeInterface("shared", None))
        self.assertIsNotNone(admitted_after, f"the LoRa requester was refused {refused} times and never admitted")
        self.assertLessEqual(admitted_after, self.h.config.deadline_slack + 2 * 7 + TRANSFER)
        # Displaced within its grace, the attacker isn't cooled down: it had no time to show progress
        self.assertFalse(any(self.h.server.cooldowns.active(link.link_id) for link in self.attackers))
        self.assertEqual(self.h.server.pools.count(server.SLOW_POOL), 2)


if __name__ == "__main__":
    unittest.main()
