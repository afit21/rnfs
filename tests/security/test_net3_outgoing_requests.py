"""NET-3: on links rnfs opens, the remote destination has no max_request_size, so RNS msgpack-decodes any request
packet (up to the link MDU) and starts a thread for it before finding there is no handler. Any node rnfs links to
could make it do so.

Fixed: guards.guarded_receive drops a request packet on an rnfs.dht link this node opened (link.initiator)
without calling RNS or decrypting it, mirroring the drop of responses on incoming links. These tests run the
guard with fake links: an initiator rnfs.dht link drops the request unread; an incoming rnfs.dht link (where the
node's server takes requests) and any LXMF link pass it to RNS; and the response check is unchanged.
"""
import types
import unittest
from unittest import mock

import RNS
from RNS.vendor import umsgpack

from rnfs.network import guards

from tests.security.test_l1_response_precheck import DHT, LXMF, REQUEST_ID, ResponseLink, bomb, response_packet


def request_packet(size: int = 600):
    """A request packet of size bytes (its content is never read by the guard)."""
    return types.SimpleNamespace(context=RNS.Packet.REQUEST, packet_type=RNS.Packet.DATA, data=b"\x93" * size)


class OutgoingRequestTest(unittest.TestCase):
    """Request packets by link direction and destination, through the guard to a recorded RNS receive."""

    def setUp(self):
        guards.install()
        self.received = []
        patcher = mock.patch.object(guards, "_original_receive", lambda link, packet: self.received.append(packet))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.logged = {}
        for name in ("_log_refused_response", "_log_unexpected_response", "_log_unexpected_request"):
            self.logged[name] = mock.Mock()
            patcher = mock.patch.object(guards, name, self.logged[name], create=True)
            patcher.start()
            self.addCleanup(patcher.stop)

    def link(self, destination=DHT, initiator=True) -> ResponseLink:
        """A fake link whose decrypt is recorded."""
        link = ResponseLink(destination, initiator=initiator)
        link.decrypt = mock.Mock(side_effect=lambda data: data)
        return link

    def test_request_on_an_initiator_dht_link_dropped_unread(self):
        for size in (100, 16 * 1024):
            with self.subTest(size=size):
                link = self.link()
                self.assertIsNone(RNS.Link.receive(link, request_packet(size)))
                self.assertEqual(len(self.received), 0)  # RNS's receive never called
                link.decrypt.assert_not_called()
        self.logged["_log_unexpected_request"].assert_called_with(mock.ANY, guards.LOG_VERBOSE)

    def test_request_on_an_incoming_dht_link_passed(self):
        # Where this node's server takes requests: RNS's own size limit and handlers apply
        link = self.link(initiator=False)
        packet = request_packet()
        RNS.Link.receive(link, packet)
        self.assertEqual(self.received, [packet])

    def test_request_on_an_lxmf_initiator_link_passed(self):
        for destination in (LXMF, types.SimpleNamespace(name="host.app." + "ab" * 16)):
            with self.subTest(destination.name):
                self.received.clear()
                link = self.link(destination)
                packet = request_packet()
                RNS.Link.receive(link, packet)
                self.assertEqual(self.received, [packet])

    def test_other_packet_types_on_an_initiator_dht_link_passed(self):
        link = self.link()
        proof = types.SimpleNamespace(context=RNS.Packet.REQUEST, packet_type=RNS.Packet.PROOF, data=b"x" * 64)
        plain = types.SimpleNamespace(context=RNS.Packet.NONE, packet_type=RNS.Packet.DATA, data=b"x" * 64)
        for packet in (proof, plain):
            RNS.Link.receive(link, packet)
        self.assertEqual(self.received, [proof, plain])

    def test_response_check_still_applies(self):
        # An honest response on an initiator link passes; a bomb is refused; a response on an incoming link drops
        link = self.link()
        honest = response_packet(umsgpack.packb([REQUEST_ID, True]))
        RNS.Link.receive(link, honest)
        self.assertEqual(self.received, [honest])
        RNS.Link.receive(link, response_packet(bomb(16 * 1024, 0x90)))
        self.assertEqual(self.received, [honest])
        self.assertEqual(link.handled, [(REQUEST_ID, None)])
        incoming = self.link(initiator=False)
        RNS.Link.receive(incoming, response_packet(umsgpack.packb([REQUEST_ID, True])))
        self.assertEqual(self.received, [honest])
        incoming.decrypt.assert_not_called()


if __name__ == "__main__":
    unittest.main()
