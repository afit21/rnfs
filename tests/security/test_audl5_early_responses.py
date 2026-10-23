"""AUD-L5 (0.2.1 audit): EarlyResponses kept Resource-sized responses under request IDs a responder made up.

The attack: RNS (1.5.6) registers a request's receipt only after Link.request has sent it, so a fast answer can arrive
first; EarlyResponses (rnfs/network/links.py), installed as each outgoing link's handle_response, keeps an answer no
pending request matches and hands it over once its request is registered. It kept any answer, though only one sent
as a single packet can beat the registration: a responder answering with Resources (up to about 1 MiB each; RNS takes
the request ID from inside the Resource's data) under made-up request IDs had up to MAX_EARLY_RESPONSES (16) of them
held on each of the requester's links, until the link closed: the age limit was applied only when another arrived.

The test feeds an EarlyResponses on a stand-in link 16 Resource-sized answers under unmatched IDs, then a packet-sized
one, ages it past EARLY_RESPONSE_LIFE and hands over another request's answer.

The fix (EarlyResponses.on_response, deliver): only an answer whose transfer size fits one packet on the link (its
MDU) is kept, and deliver() also drops kept answers older than EARLY_RESPONSE_LIFE.
"""
import types
import unittest

from rnfs.network import links

MDU = 431


class EarlyResponsesBoundTest(unittest.TestCase):
    def setUp(self):
        self.handled = []
        self.link = types.SimpleNamespace(pending_requests=[], mdu=MDU,
                                          handle_response=lambda *args, **kwargs: self.handled.append(args))
        self.now = [100.0]
        self.early = links.EarlyResponses(self.link, clock=lambda: self.now[0])

    def test_resource_sized_answers_are_not_kept(self):
        big = bytes(1024 * 1024)
        for i in range(links.MAX_EARLY_RESPONSES):
            # As RNS's Link.response_resource_concluded passes one: (request ID, data, total size, transfer size)
            self.link.handle_response(bytes([i]) * 16, big, len(big), len(big) + 64)
        self.assertEqual(len(self.early.kept), 0, "Resource-sized answers to no request were kept")
        # One sent as a packet, as RNS's Link.receive passes it, is still kept and handed over
        self.link.handle_response(b"a" * 16, "answer", 6, 6, update_sizes=True, check_size=True)
        self.assertTrue(self.early.deliver(types.SimpleNamespace(request_id=b"a" * 16)))
        self.assertEqual(self.handled, [(b"a" * 16, "answer", 6, 6)])

    def test_deliver_drops_answers_past_their_life(self):
        self.link.handle_response(b"old" * 5 + b"x", "late", 4, 4)
        self.now[0] += links.EARLY_RESPONSE_LIFE + 1
        self.assertFalse(self.early.deliver(types.SimpleNamespace(request_id=b"b" * 16)))
        self.assertEqual(len(self.early.kept), 0, "an answer past EARLY_RESPONSE_LIFE was still kept")


if __name__ == "__main__":
    unittest.main()
