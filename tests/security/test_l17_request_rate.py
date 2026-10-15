"""L17b: rnfs.dht had no request rate limit, and RNS starts a thread per request packet, so one link could have
the node parse, look up and answer requests as fast as it can send them.

Fixed: RequestServer takes a token from a per-link bucket (REQUEST_BURST, refilled at REQUEST_RATE a second)
before it parses anything. Over the limit, a few requests are answered False (OVER_RATE_REFUSALS, one more a
second) and the rest dropped unanswered (RNS sends nothing for None). These tests drive the handlers on a
FakeClock: honest request patterns are never limited, a flood is, before any parsing, and only on its own link.
"""
import unittest
from unittest import mock

from rnfs.constants import FIND_PATH, MAX_PROVIDE_KEYS, MAX_REQUESTS_PER_LINK, PROVIDE_PATH
from rnfs.network import protocol, server
from tests.security._server_helpers import ServerHarness

KEY = bytes(range(16))


class RequestRateTest(unittest.TestCase):
    """The per-link request rate limit."""

    def setUp(self):
        self.harness = ServerHarness({})
        self.clock = self.harness.clock
        self.link = self.harness.connect()
        self.logged = []
        patcher = mock.patch.object(server, "log", lambda message, level=None: self.logged.append(message))
        patcher.start()
        self.addCleanup(patcher.stop)

    def find(self, link=None):
        """Send /find on link (default: the test's link) as RNS would; the handler's answer."""
        handler = self.harness.destination.handlers[FIND_PATH][0]
        return handler(FIND_PATH, protocol.encode_find(KEY), bytes(16), (link or self.link).link_id, None, 0.0)

    def test_defaults_cover_honest_requesters(self):
        # At most MAX_REQUESTS_PER_LINK requests per round trip: even at a 1 ms round trip, under the rate
        self.assertGreaterEqual(server.REQUEST_RATE, MAX_REQUESTS_PER_LINK / 0.001)
        # A whole 10,000-key /provide, sent back to back, fits the burst
        self.assertGreaterEqual(server.REQUEST_BURST, -(-10_000 // MAX_PROVIDE_KEYS))

    def test_honest_pace_never_limited(self):
        # MAX_REQUESTS_PER_LINK requests every millisecond for 10 s: as fast as rnfs ever asked over loopback
        answers = set()
        for _ in range(10_000):
            for _ in range(MAX_REQUESTS_PER_LINK):
                answers.add(type(self.find()))
            self.clock.now += 0.001
        self.assertEqual(answers, {list})
        self.assertEqual(self.harness.server.over_rate, 0)

    def test_flood_is_refused_then_dropped(self):
        answers = [self.find() for _ in range(server.REQUEST_BURST)]
        self.assertTrue(all(type(answer) is list for answer in answers))
        refused = [self.find() for _ in range(server.OVER_RATE_REFUSALS)]
        self.assertEqual(refused, [False] * server.OVER_RATE_REFUSALS)  # an honest requester backs off from these
        self.assertEqual([self.find() for _ in range(1000)], [None] * 1000)  # no answer: RNS sends nothing
        self.assertEqual(self.harness.server.over_rate, server.OVER_RATE_REFUSALS + 1000)

    def test_bucket_refills_at_the_rate(self):
        for _ in range(server.REQUEST_BURST + server.OVER_RATE_REFUSALS + 10):
            self.find()
        self.assertIsNone(self.find())
        self.clock.now += 1.5 / server.REQUEST_RATE  # one and a half tokens' time
        self.assertIsInstance(self.find(), list)
        self.assertIsNone(self.find())
        self.clock.now += 1.0  # a second: REQUEST_RATE more requests, and one more refusal
        answers = [self.find() for _ in range(int(server.REQUEST_RATE) + 2)]
        self.assertEqual([type(answer) for answer in answers[:int(server.REQUEST_RATE)]],
                         [list] * int(server.REQUEST_RATE))
        self.assertEqual(answers[-2:], [False, None])

    def test_refused_before_any_parsing(self):
        for _ in range(server.REQUEST_BURST + server.OVER_RATE_REFUSALS):
            self.find()
        with mock.patch.object(protocol, "parse_find_request") as parse, \
                mock.patch.object(protocol, "parse_provide_request") as parse_provide, \
                mock.patch.object(self.harness.backend, "find") as find:
            self.assertIsNone(self.find())
            handler = self.harness.destination.handlers[PROVIDE_PATH][0]
            self.assertIsNone(handler(PROVIDE_PATH, [[KEY], []], bytes(16), self.link.link_id, None, 0.0))
        parse.assert_not_called()
        parse_provide.assert_not_called()
        find.assert_not_called()

    def test_other_links_unaffected(self):
        for _ in range(server.REQUEST_BURST + server.OVER_RATE_REFUSALS + 5):
            self.find()
        self.assertIsNone(self.find())
        other = self.harness.connect()
        self.assertIsInstance(self.find(other), list)

    def test_closed_link_still_answered_false(self):
        self.link.teardown()
        self.assertIs(self.find(), False)
        self.assertNotIn(self.link.link_id, self.harness.server.incoming.links)  # not tracked again

    def test_logged_at_most_once_a_minute(self):
        for _ in range(server.REQUEST_BURST + 500):
            self.find()
        self.clock.now += 30
        for _ in range(server.REQUEST_BURST + 500):  # refilled meanwhile: 500 over the limit again
            self.find()
        lines = [line for line in self.logged if "rate limit" in line]
        self.assertEqual(len(lines), 1)
        self.clock.now += server.OVER_RATE_LOG_INTERVAL
        for _ in range(server.REQUEST_BURST + 1):
            self.find()
        lines = [line for line in self.logged if "rate limit" in line]
        self.assertEqual(len(lines), 2)
        self.assertIn("refused or dropped 1000 requests", lines[1])  # all those since the first line


if __name__ == "__main__":
    unittest.main()
