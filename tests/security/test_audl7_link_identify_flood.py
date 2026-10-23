"""AUD-L7 (0.2.1 audit): a flood of LINKIDENTIFY packets cost the node a shared-instance RPC each, inside RNS.

The attack: on an incoming link, RNS 1.5.6's Link.receive handles every DATA packet with context LINKIDENTIFY (0xFB):
it decrypts it, validates the Ed25519 signature over link ID and public key, and, for a valid one, asks
Reticulum.is_blackholed about the identity, which through a shared instance is an RPC to rnsd, before it finds the
link already identified (RNS keeps the first identity and ignores the rest). One identify packet is signed over the
link ID only, so the requester can send the same one again and again: each costs the node a decryption, a signature
check and an RPC, at the price of one small packet, and none of it ever goes through rnfs's per-link request limit.

The test sends LINKIDENTIFY packets through the guarded Link.receive (guards.guarded_receive, with RNS's own receive
recorded in its place) on an incoming rnfs.dht link: many on one already identified, and many on one that never
identifies.

The fix (guards.guarded_receive): on an incoming rnfs.dht link, a LINKIDENTIFY packet is dropped unread once the
link has a remote identity, and after MAX_IDENTIFY_PACKETS (4) of them on a link that has none: an honest requester
identifies once (and again only after a refusal, when the first was lost on the way). RNS ignores every identify
after the first one it accepts, so nothing else changes. Outgoing links and other destinations are left to RNS.
"""
import types
import unittest
from unittest import mock

import RNS

from rnfs.network import guards

DHT = types.SimpleNamespace(name=guards.DHT_NAME + "." + "ab" * 16)
LXMF = types.SimpleNamespace(name="lxmf.delivery." + "ab" * 16)


class IdentifyingLink:
    """An incoming (or outgoing) link as far as Link.receive and the guard look at it."""

    def __init__(self, destination=DHT, initiator=False, identity=None):
        """An active link to destination, identified as identity (None: not identified)."""
        self.destination = destination
        self.initiator = initiator
        self.status = RNS.Link.ACTIVE
        self.identity = identity

    def get_remote_identity(self):
        """The identity the requester identified as, or None."""
        return self.identity


def identify_packet():
    """A LINKIDENTIFY packet (RNS 1.5.6 Link.identify: DATA, context LINKIDENTIFY)."""
    return types.SimpleNamespace(context=RNS.Packet.LINKIDENTIFY, packet_type=RNS.Packet.DATA, data=b"x" * 112)


class LinkIdentifyFloodTest(unittest.TestCase):
    def setUp(self):
        guards.install()
        self.received = []
        for name, value in (("_original_receive", lambda link, packet: self.received.append(packet)),
                            ("_log_unexpected_identify", mock.Mock())):
            patcher = mock.patch.object(guards, name, value, create=True)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_the_context_is_rns_link_identify(self):
        # RNS 1.5.6: Packet.LINKIDENTIFY, what Link.identify sends and Link.receive validates
        self.assertEqual(RNS.Packet.LINKIDENTIFY, 0xFB)

    def test_identify_packets_on_an_identified_link_are_dropped(self):
        link = IdentifyingLink(identity=object())
        for _ in range(100):
            RNS.Link.receive(link, identify_packet())
        self.assertEqual(self.received, [], "identify packets reached RNS on a link already identified")

    def test_a_link_never_identifying_gets_a_few_tries(self):
        link = IdentifyingLink()
        for _ in range(100):
            RNS.Link.receive(link, identify_packet())
        self.assertLessEqual(len(self.received), 4)
        self.assertGreaterEqual(len(self.received), 1)  # an honest requester's identify goes through

    def test_outgoing_links_and_other_destinations_untouched(self):
        outgoing = IdentifyingLink(initiator=True, identity=object())
        other = IdentifyingLink(LXMF, identity=object())
        for link in (outgoing, other):
            for _ in range(10):
                RNS.Link.receive(link, identify_packet())
        self.assertEqual(len(self.received), 20)


if __name__ == "__main__":
    unittest.main()
