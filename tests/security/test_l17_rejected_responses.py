"""L17a: a requester could have the node build /blocks responses in a loop by rejecting each one's advertisement
and asking again at once: a rejected Resource concludes immediately, freeing the slot, and no cooldown applied.

Fixed: a response whose Resource is rejected with nothing sent counts as making no progress, so its link (and
identity) cools down for blocks_cooldown, as after one cancelled for making none (H1). These tests drive the
server on a FakeClock with fake Resources concluding as RNS's do on a rejection (status REJECTED, then the
callback): the loop builds one response per cooldown, and honest outcomes (completed, or rejected mid-transfer)
start no cooldown.
"""
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.content.cid import RAW, Cid
from tests.security._server_helpers import ServerHarness

BLOCK = bytes(CHUNK_SIZE)
CID = Cid.for_block(RAW, BLOCK)


def reject(resource, progress: float = 0.0):
    """The requester rejects (or, mid-transfer, cancels) the response: RNS's Resource._rejected on an outgoing
    Resource sets REJECTED, leaves the link's outgoing Resources and runs the callback."""
    resource.progress = progress
    resource.status = RNS.Resource.REJECTED
    resource.conclude()
    if resource.callback:
        resource.callback(resource)


class RejectedResponseTest(unittest.TestCase):
    """Rejected /blocks responses and the cooldown."""

    def setUp(self):
        self.harness = ServerHarness({CID: BLOCK}, Config(blocks_cooldown=60))
        self.clock = self.harness.clock

    def respond(self, link, identity=None):
        """Request a block on link and advertise the response; its Resource, or None if refused."""
        answer = self.harness.blocks(link, [CID], max_bytes=CHUNK_SIZE, identity=identity)
        if answer is False:
            return None
        self.assertEqual(answer, [BLOCK])
        return self.harness.advertise(link)

    def test_request_and_reject_loop_builds_one_response_per_cooldown(self):
        link = self.harness.connect()
        built = 0
        end = self.clock.now + 10
        while self.clock.now < end:  # ask again 10 ms after each rejection
            resource = self.respond(link)
            if resource is not None:
                built += 1
                reject(resource)
            self.clock.now += 0.01
            self.harness.server.check_responses()
        self.assertEqual(built, 1)  # 1,001 before the fix
        self.assertTrue(self.harness.server.cooldowns.active(link.link_id))
        self.clock.now += 60
        self.assertIsNotNone(self.respond(link))  # and again once the cooldown is over

    def test_identity_cools_down_on_new_links(self):
        identity = RNS.Identity()
        reject(self.respond(self.harness.connect(), identity))
        self.assertIsNone(self.respond(self.harness.connect(), identity))
        self.assertIsNotNone(self.respond(self.harness.connect()))  # another requester is unaffected

    def test_rejection_noticed_by_the_monitor_also_counts(self):
        # Without the callback (the monitor finds the Resource concluded), the same applies
        link = self.harness.connect()
        resource = self.respond(link)
        resource.callback = None
        resource.status = RNS.Resource.REJECTED
        resource.conclude()
        self.harness.server.check_responses()
        self.assertIsNone(self.respond(link))

    def test_honest_outcomes_start_no_cooldown(self):
        link = self.harness.connect()
        self.respond(link).complete()
        resource = self.respond(link)
        self.assertIsNotNone(resource)
        # Rejected after some of it was sent (RNS's receiver gives up mid-transfer the same way): it made progress
        reject(resource, progress=0.5)
        self.assertIsNotNone(self.respond(link))
        self.assertFalse(self.harness.server.cooldowns.active(link.link_id))


if __name__ == "__main__":
    unittest.main()
