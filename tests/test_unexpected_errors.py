"""Unexpected exceptions caught by a broad except are logged at LOG_WARNING inside it, so the debug log records
their traceback (XC4): the fetcher's /blocks worker, the lookup's routing-table callbacks and LinkPool.request.
The mesh and peers sites are tested in test_mesh, ThrottledLog in test_log."""
import os
import sys
import unittest
from unittest import mock

from rnfs.config import Config
from rnfs.dht.lookup import Lookup
from rnfs.log import LOG_INFO, LOG_WARNING
from rnfs.network import links
from tests.test_fetcher import FetcherTest
from tests.test_links import FakeLink, fake_rns, run

P1, P2 = bytes([1]) * 16, bytes([2]) * 16


def recorder(logged: list):
    """A stand-in for rnfs.log.log appending (message, level, the type of the exception being handled) to logged."""
    def record(message, level=LOG_INFO):
        """Record the line and whether it was logged inside an except."""
        logged.append((message, level, sys.exc_info()[0]))
    return record


def warnings(logged: list) -> list:
    """(message, exception type) of each line logged at LOG_WARNING."""
    return [(message, raised) for message, level, raised in logged if level == LOG_WARNING]


class FetcherWorkerTest(FetcherTest):
    """A /blocks request whose client raises (a bug, not a network failure) is a failed request, logged."""

    def test_client_exception_logged_and_fetched_elsewhere(self):
        built = self.file(os.urandom(3000))

        def broken(cids):
            """A bug in the client."""
            raise TypeError("a bug")
        self.client.behaviour[P1] = broken
        self.hold(P2)
        self.runs.append([P1, P2])
        logged = []
        with mock.patch("rnfs.fetcher.log", recorder(logged)):
            self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        (message, raised), = [line for line in warnings(logged) if "raised" in line[0]]
        self.assertIs(raised, TypeError)
        self.assertIn("a bug", message)


class LookupCallbackTest(unittest.TestCase):
    """A routing-table callback that raises doesn't end the lookup, and is logged."""

    def test_callback_exception_logged(self):
        def on_answer(node_id):
            """A bug in the routing table (private mode's admit, say)."""
            raise TypeError("a bug")
        lookup = Lookup(bytes(16), [P1], lambda node_id, key: ([], []), bytes([9]) * 16, on_answer=on_answer)
        logged = []
        with mock.patch("rnfs.log.log", recorder(logged)):
            self.assertEqual(lookup.find_closest(), [P1])
        (message, raised), = warnings(logged)
        self.assertIs(raised, TypeError)
        self.assertIn("a bug", message)


class LinkPoolSendTest(unittest.TestCase):
    """Link.request raising (it answers False when it can't send) is a failed request, logged."""

    def setUp(self):
        """A LinkPool over fake_rns()."""
        patcher = mock.patch.object(links, "RNS", fake_rns())
        patcher.start()
        self.addCleanup(patcher.stop)
        FakeLink.created = []
        FakeLink.establish_delay = 0.0
        FakeLink.responder = staticmethod(lambda link, path, data: True)
        self.pool = links.LinkPool(object(), Config())
        self.addCleanup(self.pool.close_all)

    def test_request_exception_logged(self):
        peer = run(self, lambda: self.pool.get(P1))

        def broken(*args, **kwargs):
            """A bug in RNS or in what rnfs passed it."""
            raise TypeError("a bug")
        peer.link.request = broken
        logged = []
        with mock.patch.object(links, "log", recorder(logged)):
            exchange = run(self, lambda: self.pool.request(peer, "/find", bytes(16)))
        self.assertFalse(exchange.answered)
        (message, raised), = warnings(logged)
        self.assertIs(raised, TypeError)
        self.assertIn("a bug", message)
        self.assertEqual(peer.requests, 0)  # its slot was freed


if __name__ == "__main__":
    unittest.main()
