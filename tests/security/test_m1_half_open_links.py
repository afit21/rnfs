"""M1: link requests that never complete were unbounded: RNS validates every link request (key exchange, a proof,
a watchdog thread) and keeps the link half-open for 6 s × hops + 360 s waiting for the RTT packet, while rnfs
only counted links once established.

Fixed: guards.limit_half_open wraps the destination's incoming_link_request so half-open links (PENDING or
HANDSHAKE) have a capped bucket of their own (max_incoming_links). When it is full, the oldest half-open link on
the interface holding the most is closed, not the new request refused, so a flood can't lock honest initiators
out. Shown with a fake destination that runs incoming_link_request as RNS 1.5.6 does (validate: a new
HANDSHAKE link on the request's interface; append to destination.links).
"""
import types
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.network import guards, server
from tests.support import FakeClock
from tests.test_server import FakeBackend, FakeDestination


class FakeHalfOpenLink:
    """An incoming RNS.Link just validated: HANDSHAKE on the interface its request arrived on."""

    def __init__(self, destination, interface, request_time: float):
        """Waiting for its RTT packet."""
        self.destination = destination
        self.status = RNS.Link.HANDSHAKE
        self.attached_interface = interface
        self.request_time = request_time
        self.closed_by_teardown = False

    def activate(self):
        """The RTT packet arrived (Link.rtt_packet)."""
        self.status = RNS.Link.ACTIVE

    def teardown(self):
        """Close like RNS: CLOSED, then link_closed removes it from its destination's links."""
        if self.status == RNS.Link.CLOSED:
            return
        self.status = RNS.Link.CLOSED
        self.closed_by_teardown = True
        if self in self.destination.links:
            self.destination.links.remove(self)


class FakeLinkDestination:
    """An IN destination whose incoming_link_request does what RNS 1.5.6's does with a valid request."""

    def __init__(self):
        """No links yet."""
        self.links = []
        self.now = 0.0
        self.validated = 0

    def incoming_link_request(self, data, packet):
        """Link.validate_request (a new half-open link), then self.links.append(link)."""
        self.now += 0.01
        self.validated += 1
        link = FakeHalfOpenLink(self, packet.receiving_interface, self.now)
        self.links.append(link)

    def receive(self, packet):
        """Destination.receive for a LINKREQUEST packet: through self.incoming_link_request."""
        self.incoming_link_request(packet.data, packet)

    def request(self, interface) -> FakeHalfOpenLink:
        """A link request arriving on interface; the half-open link it made."""
        self.receive(types.SimpleNamespace(data=b"lr", receiving_interface=interface))
        return self.links[-1]

    def half_open(self) -> list:
        """Links still waiting for their RTT packet."""
        return [link for link in self.links if link.status in (RNS.Link.PENDING, RNS.Link.HANDSHAKE)]


class HalfOpenLimitTest(unittest.TestCase):
    """The half-open bucket: capped, oldest closed first, per interface, established links untouched."""

    def setUp(self):
        self.destination = FakeLinkDestination()
        self.tcp, self.lora = object(), object()

    def test_unguarded_half_open_links_are_unbounded(self):
        # The finding, as RNS behaves on its own
        for _ in range(1000):
            self.destination.request(self.tcp)
        self.assertEqual(len(self.destination.half_open()), 1000)

    def test_flood_stays_within_the_cap(self):
        guards.limit_half_open(self.destination, 16)
        flood = []
        for _ in range(1000):
            flood.append(self.destination.request(self.tcp))
            self.assertLessEqual(len(self.destination.half_open()), 16)
        self.assertEqual(self.destination.validated, 1000)  # every request still answered: none refused
        self.assertEqual(sum(link.closed_by_teardown for link in flood), 1000 - 16)
        self.assertEqual(self.destination.links, flood[-16:])  # the newest kept, the oldest closed

    def test_honest_handshake_completing_within_the_cap_survives_a_flood(self):
        guards.limit_half_open(self.destination, 16)
        for _ in range(50):
            self.destination.request(self.tcp)
        honest = self.destination.request(self.tcp)
        for _ in range(15):  # an honest RTT packet arrives within about one round trip
            self.destination.request(self.tcp)
        honest.activate()
        for _ in range(1000):
            self.destination.request(self.tcp)
        self.assertEqual(honest.status, RNS.Link.ACTIVE)
        self.assertIn(honest, self.destination.links)
        self.assertLessEqual(len(self.destination.half_open()), 16)

    def test_established_links_are_neither_counted_nor_closed(self):
        guards.limit_half_open(self.destination, 4)
        established = []
        for _ in range(40):  # each completes before the next request
            established.append(self.destination.request(self.tcp))
            established[-1].activate()
        # 40 links requested one after another, none closed: established links don't count
        self.assertFalse(any(link.closed_by_teardown for link in established))
        for _ in range(100):
            self.destination.request(self.tcp)
        self.assertTrue(all(link.status == RNS.Link.ACTIVE for link in established))
        self.assertEqual(len(self.destination.half_open()), 4)

    def test_a_flood_on_one_interface_closes_its_own_links_first(self):
        guards.limit_half_open(self.destination, 16)
        honest = self.destination.request(self.lora)  # a slow handshake on another interface
        for _ in range(5000):
            self.destination.request(self.tcp)
        self.assertEqual(honest.status, RNS.Link.HANDSHAKE)  # still waiting, not closed
        self.assertIn(honest, self.destination.half_open())
        self.assertLessEqual(len(self.destination.half_open()), 16)

    def test_the_interface_holding_most_gives_way_to_another_interfaces_newcomer(self):
        guards.limit_half_open(self.destination, 5)
        other = [self.destination.request(self.lora) for _ in range(2)]
        for _ in range(20):  # on a tie the newcomer's own interface gives way, so the two LoRa links stay
            self.destination.request(self.tcp)
        self.assertTrue(all(link.status == RNS.Link.HANDSHAKE for link in other))
        # A new request on the quieter interface, with the bucket full: the busier one gives way
        self.destination.request(self.lora)
        on_lora = [link for link in self.destination.half_open() if link.attached_interface is self.lora]
        self.assertEqual(len(on_lora), 3)

    def test_half_open_to_close_picks_the_oldest_on_the_busiest_interface(self):
        def link(interface, at):
            return FakeHalfOpenLink(self.destination, interface, at)
        a1, b1, b2 = link(self.tcp, 1), link(self.lora, 2), link(self.lora, 3)
        a2, b3 = link(self.tcp, 4), link(self.lora, 5)
        self.assertIs(guards.half_open_to_close([a1, b1, b2, a2, b3]), b1)
        self.assertIs(guards.half_open_to_close([a1, b1, b2, a2, b3], self.tcp), b1)
        self.assertIs(guards.half_open_to_close([a1, b1, a2, b3]), a1)  # a tie: the oldest
        self.assertIs(guards.half_open_to_close([a1, b1, a2, b3], self.lora), b1)  # a tie: the newcomer's own
        self.assertIsNone(guards.half_open_to_close([]))

    def test_idempotent_and_the_limit_can_change(self):
        first = guards.limit_half_open(self.destination, 8)
        second = guards.limit_half_open(self.destination, 4)
        self.assertIs(first, second)
        self.assertEqual(first.limit, 4)
        for _ in range(20):
            self.destination.request(self.tcp)
        self.assertEqual(len(self.destination.half_open()), 4)
        self.assertEqual(first.closed, 16)

    def test_a_destination_without_the_hook_is_left_alone(self):
        destination = FakeDestination()  # tests/test_server.py's: no incoming_link_request (a changed RNS)
        self.assertIsNone(guards.limit_half_open(destination, 8))
        self.assertFalse(hasattr(destination, "incoming_link_request"))

    def test_a_link_that_fails_to_close_does_not_stop_requests(self):
        guard = guards.limit_half_open(self.destination, 2)
        stuck = self.destination.request(self.tcp)

        def broken():
            """An RNS whose teardown fails."""
            raise RuntimeError("RNS changed")
        stuck.teardown = broken
        self.destination.request(self.tcp)
        with mock.patch.object(guards, "log"):
            self.destination.request(self.tcp)
        self.assertEqual(self.destination.validated, 3)  # the new request was still validated
        self.assertEqual(guard.closed, 0)

    def test_request_server_guards_rnfs_dht(self):
        class Destination(FakeDestination, FakeLinkDestination):
            """tests/test_server.py's fake destination, taking link requests as RNS does."""

            def __init__(self):
                FakeDestination.__init__(self)
                FakeLinkDestination.__init__(self)

        destination = Destination()
        saved = RNS.Resource.AUTO_COMPRESS_MAX_SIZE
        self.addCleanup(setattr, RNS.Resource, "AUTO_COMPRESS_MAX_SIZE", saved)
        server.RequestServer(destination, Config(max_incoming_links=8), FakeBackend(), clock=FakeClock(),
                             start_monitor=False)
        for _ in range(100):
            destination.request(self.tcp)
        self.assertEqual(len(destination.half_open()), 8)


if __name__ == "__main__":
    unittest.main()
