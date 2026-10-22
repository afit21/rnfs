"""REL-3 (0.2.0 release audit): a request of the wrong shape raised inside RNS, before any rnfs check.

The attack: RNS 1.5.6's Link.handle_request indexes the decoded request as [requested_at, path_hash, data] unchecked,
on the thread it starts for the packet. A request whose plaintext is an int, a short list, or has a list as its path
raised there, so each such packet left a traceback in the log (the journal, under systemd), and rnfs's per-link rate
limit, which runs in its handler, never saw it.

The test calls the guarded RNS.Link.handle_request with those shapes on a stub link: none raises, and none reaches
RNS's own handling, while a well-formed request does. The fix (rnfs/network/guards.py, guarded_handle_request, put in
by install): such a request is dropped with a throttled verbose line, as no handler could serve it.
"""
import types
import unittest
from unittest import mock

import RNS
from RNS.vendor import umsgpack

from rnfs.network import guards


class RequestShapeTest(unittest.TestCase):

    def setUp(self):
        guards.install()
        self.link = types.SimpleNamespace(status=RNS.Link.ACTIVE,
                                          destination=types.SimpleNamespace(request_handlers={}))

    def test_malformed_requests_are_dropped_before_rns_indexes_them(self):
        self.assertTrue(guards.installed())
        reached = []
        with mock.patch.object(guards, "_original_handle_request",
                               lambda link, request_id, unpacked: reached.append(unpacked)):
            for request in (5, [0.0, b"x" * 10], [0.0, [1], None], {"a": 1}, None):
                with self.subTest(request=request):
                    unpacked = umsgpack.unpackb(umsgpack.packb(request))
                    self.assertIsNone(RNS.Link.handle_request(self.link, b"r" * 16, unpacked))
            self.assertEqual(reached, [])
            RNS.Link.handle_request(self.link, b"r" * 16, [0.0, b"p" * 16, b"data"])
            self.assertEqual(reached, [[0.0, b"p" * 16, b"data"]])

    def test_the_real_handler_takes_a_well_formed_request_for_no_handler_quietly(self):
        # RNS's own code still runs for a well-formed request: one for a path with no handler is ignored
        self.assertIsNone(RNS.Link.handle_request(self.link, b"r" * 16, [0.0, b"p" * 16, None]))


if __name__ == "__main__":
    unittest.main()
