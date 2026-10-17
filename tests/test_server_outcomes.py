"""How a /blocks response's Resource ended, as the responder logs it: one RNS gave up on (lost in transit, or its
link closed) is logged at debug, since the requester only sees it stop arriving; one the server cancelled itself
is logged once, with its reason, and a completed one not at all."""
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.content.cid import RAW, Cid
from rnfs.log import LOG_DEBUG
from tests.security._server_helpers import ServerHarness

BLOCK = bytes(CHUNK_SIZE)
CID = Cid.for_block(RAW, BLOCK)


class ResponseOutcomeTest(unittest.TestCase):
    """A server on a FakeClock with fake Resources (tests.security._server_helpers)."""

    def setUp(self):
        """A server holding one block, and one response to a request for it, advertised."""
        self.harness = ServerHarness({CID: BLOCK}, Config())
        self.link = self.harness.connect()
        self.assertEqual(self.harness.blocks(self.link, [CID], max_bytes=CHUNK_SIZE), [BLOCK])
        self.resource = self.harness.advertise(self.link)
        self.harness.tick()  # the monitor finds the Resource and hooks its conclusion

    def messages(self, act) -> list[tuple[str, int]]:
        """The server's log lines (and levels) while act() runs."""
        logged = []
        with mock.patch("rnfs.network.server.log", lambda message, level=None: logged.append((message, level))):
            act()
            self.harness.tick()
        return logged

    def fail_resource(self, progress: float):
        """RNS gives up on the Resource part-way, as it does when its transfer is lost: FAILED, then the
        callback."""
        self.resource.progress = progress
        self.resource.status = RNS.Resource.FAILED
        self.resource.conclude()
        if self.resource.callback:
            self.resource.callback(self.resource)

    def test_failure_in_transit_is_logged(self):
        """A Resource RNS failed, which the server didn't cancel, is logged at debug, saying it was lost in
        transit; the slot is freed for the next request."""
        logged = self.messages(lambda: self.fail_resource(0.5))
        failed = [(message, level) for message, level in logged if "failed after" in message]
        self.assertEqual(len(failed), 1, logged)
        self.assertIn("lost in transit", failed[0][0])
        self.assertEqual(failed[0][1], LOG_DEBUG)
        self.assertIsNone(self.harness.response(self.link))

    def test_failure_with_the_link_closed_says_so(self):
        """A Resource that failed because its link closed is logged as such."""
        self.link.status = RNS.Link.CLOSED
        logged = self.messages(lambda: self.fail_resource(0.2))
        self.assertTrue(any("its link closed" in message for message, _ in logged), logged)

    def test_cancelled_here_is_logged_only_with_its_reason(self):
        """A response the server cancels (here, as the monitor would) logs its reason, not a failure too."""
        response = self.harness.response(self.link)
        logged = self.messages(lambda: self.harness.server._cancel(response, "testing"))
        self.assertTrue(any("cancelling a /blocks response: testing" in message for message, _ in logged), logged)
        self.assertFalse(any("failed after" in message for message, _ in logged), logged)

    def test_completed_is_not_logged(self):
        """A completed response logs no failure."""
        logged = self.messages(self.resource.complete)
        self.assertFalse(any("failed after" in message for message, _ in logged), logged)


if __name__ == "__main__":
    unittest.main()
