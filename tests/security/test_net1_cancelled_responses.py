"""NET-1: a requester could have the node build /blocks responses in a loop by cancelling each one after its
first parts arrived and asking again at once. L17 cooled down only a response rejected with nothing sent, so a
cancel after one part (or one window) of parts freed the slot with no cooldown, and each round made the node
read, hash and pack every block, then build and encrypt a whole Resource, for the cost of one window.

Fixed: a response whose Resource the requester rejected or cancelled (on RNS 1.5.6 both arrive as REJECTED)
before a quarter of it was sent (server.NO_PROGRESS_FRACTION) made no real progress, so its link and identity
cool down for blocks_cooldown, flat, as for one rejected unsent. A link closed early (FAILED) starts no
cooldown: an honest reader that gives up closes its links, and in private mode it would cool its own identity
down. If progress can't be read it counts as progress, as before. These tests drive the server on a FakeClock
with fake Resources concluding as RNS's do.
"""
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.content.cid import RAW, Cid
from rnfs.network import protocol
from tests.security._server_helpers import ServerHarness, reject

BLOCK = bytes(CHUNK_SIZE)
CID = Cid.for_block(RAW, BLOCK)


def close_link(link):
    """The requester closes the link mid-transfer: RNS's Link.link_closed cancels the link's outgoing Resources
    (FAILED, then their callbacks) before it runs the link-closed callback."""
    for resource in list(link.outgoing_resources):
        resource.cancel()
    link.teardown()


class CancelledResponseTest(unittest.TestCase):
    """/blocks responses cancelled before real progress, and the cooldown."""

    def setUp(self):
        self.harness = ServerHarness({CID: BLOCK}, Config(blocks_cooldown=60))
        self.clock = self.harness.clock
        self.server = self.harness.server

    def respond(self, link, identity=None):
        """Request a block on link and advertise the response; its Resource, or None if refused."""
        answer = self.harness.blocks(link, [CID], max_bytes=CHUNK_SIZE, identity=identity)
        if answer is False:
            return None
        self.assertEqual(answer, [BLOCK])
        return self.harness.advertise(link)

    def send(self, link, resource, progress: float):
        """RNS sending progress (a fraction) of resource's parts, noticed by one monitor pass."""
        resource.progress = progress
        self.harness.tick(0.1)
        self.assertEqual(self.harness.response(link).sent, progress * self.harness.response(link).size)

    def assert_cools_down(self, progress: float, link=None):
        """A rejection at progress on link (default: a new one) cools down the link and its identity, and not
        another requester."""
        identity = RNS.Identity()
        link = link or self.harness.connect()
        resource = self.respond(link, identity)
        self.send(link, resource, progress)
        reject(resource, progress)
        self.assertTrue(self.server.cooldowns.active(link.link_id))
        self.assertTrue(self.server.cooldowns.active(bytes(identity.hash)))
        self.assertIsNone(self.respond(link, identity))
        self.assertIsNone(self.respond(self.harness.connect(), identity))  # the identity, on a new link
        self.respond(self.harness.connect()).complete()  # another requester is unaffected

    def assert_no_cooldown(self, link, identity):
        """Neither link nor identity is cooling down, and the identity gets a new response at once."""
        self.assertFalse(self.server.cooldowns.active(link.link_id))
        self.assertFalse(self.server.cooldowns.active(bytes(identity.hash)))
        self.respond(self.harness.connect(), identity).complete()  # completed: the slow pool has two slots

    def test_reject_at_one_percent_cools_down(self):
        self.assert_cools_down(0.01)

    def test_reject_after_one_window_cools_down(self):
        # A full window of parts on a slow link (RNS's largest there; its first, RNS.Resource.WINDOW, is smaller)
        # of a 64 KiB block's response: all an attacker has to receive before cancelling
        link = self.harness.connect()
        size = protocol.packed_response_size(link.link_id, protocol.encode_blocks_response([BLOCK]))
        window = RNS.Resource.WINDOW_MAX_SLOW * link.get_mdu() / size
        self.assertLess(window, 0.25)
        self.assert_cools_down(window, link)

    def test_reject_loop_builds_one_response_per_cooldown(self):
        link = self.harness.connect()
        built = 0
        end = self.clock.now + 10
        while self.clock.now < end:  # take 1% of each response, cancel it and ask again 10 ms later
            resource = self.respond(link)
            if resource is not None:
                built += 1
                reject(resource, 0.01)
            self.clock.now += 0.01
            self.server.check_responses()
        self.assertEqual(built, 1)  # 1,001 before the fix
        self.clock.now += 60
        self.assertIsNotNone(self.respond(link))  # and again once the cooldown is over

    def test_reject_noticed_by_the_monitor_also_counts(self):
        # Without the callback (the monitor finds the Resource concluded), the same applies
        link = self.harness.connect()
        resource = self.respond(link)
        resource.callback = None
        resource.progress = 0.01
        resource.status = RNS.Resource.REJECTED
        resource.conclude()
        self.server.check_responses()
        self.assertIsNone(self.respond(link))

    def test_reject_at_a_quarter_or_more_starts_no_cooldown(self):
        for progress in (0.25, 0.5):
            with self.subTest(progress=progress):
                identity = RNS.Identity()
                link = self.harness.connect()
                resource = self.respond(link, identity)
                self.send(link, resource, progress)
                reject(resource, progress)
                self.respond(link, identity).complete()  # the same link may ask again at once
                self.assert_no_cooldown(link, identity)

    def test_progress_seen_by_the_monitor_counts(self):
        # A Resource that reads less at its rejection than the monitor last saw counts what was seen
        identity = RNS.Identity()
        link = self.harness.connect()
        resource = self.respond(link, identity)
        self.send(link, resource, 0.5)
        reject(resource, 0.0)
        self.assert_no_cooldown(link, identity)

    def test_link_closed_early_starts_no_cooldown(self):
        # A reader that gives up early closes its links (an honest 0.1.5 fetcher never cancels a Resource): in
        # private mode a cooldown would hit its own identity, so a FAILED response never starts one
        identity = RNS.Identity()
        link = self.harness.connect()
        resource = self.respond(link, identity)
        self.send(link, resource, 0.01)
        close_link(link)
        self.assertEqual(resource.status, RNS.Resource.FAILED)
        self.assertIsNone(self.harness.response(link))
        self.assert_no_cooldown(link, identity)

    def test_unreadable_progress_starts_no_cooldown(self):
        identity = RNS.Identity()
        link = self.harness.connect()
        resource = self.respond(link, identity)

        def broken():
            """RNS internals changed: progress can't be read."""
            raise AttributeError("no progress")

        resource.get_progress = broken
        reject(resource)
        self.respond(link, identity).complete()
        self.assert_no_cooldown(link, identity)


if __name__ == "__main__":
    unittest.main()
