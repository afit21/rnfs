"""The RNS internals the /blocks slot fairness fix (H1) and the half-open link guard (M1) rely on are still
present in the installed RNS.

server.py's displacement protection measures a per-response grace from the receiving interface's bitrate and
the requester's hop count, read through server.link_path(). Those are RNS object attributes that the version
number doesn't promise to keep, so, like tests/test_rns_compat.py (another session's file, not edited here),
this checks each one and fails naming what changed rather than letting the grace silently fall back.

guards.limit_half_open replaces a destination's incoming_link_request and reads Destination.links and each
link's status, attached_interface and request_time, and closes links with Link.teardown(); those are checked
the same way, with one functional check against a real (offline) RNS.Destination.

Written against RNS 1.5.6. Nothing here starts Reticulum.
"""
import ast
import inspect
import re
import textwrap
import unittest

import RNS
from RNS.Interfaces.Interface import Interface

from rnfs.config import Config
from rnfs.network import server


def _normal(code: str) -> str:
    """code without whitespace and with single quotes, so source matches however it's laid out."""
    return re.sub(r"\s+", "", code).replace('"', "'")


def _source_assigns(function, attribute: str, value: str) -> bool:
    """Whether function assigns self.<attribute> = <value> (matched as source, ignoring layout)."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name) \
                    and target.value.id == "self" and target.attr == attribute \
                    and _normal(ast.unparse(node.value)) == _normal(value):
                return True
    return False


class RnsSlotFairnessInternalsTest(unittest.TestCase):
    """Each RNS attribute server.link_path() reads is present and set where rnfs expects."""

    def test_link_has_attached_interface_set_from_the_request_packet(self):
        # server.link_path reads Link.attached_interface; RNS sets it on __init__ and from the link request
        self.assertIn("attached_interface", inspect.getsource(RNS.Link.__init__))
        self.assertIn("attached_interface = packet.receiving_interface",
                      _source_or_empty(RNS.Link.validate_request))

    def test_link_has_expected_hops_set_from_the_rtt_packet(self):
        # server.link_path reads Link.expected_hops; RNS sets it on __init__ and from the RTT packet's hop count
        self.assertIn("expected_hops", inspect.getsource(RNS.Link.__init__))
        self.assertIn("self.expected_hops = packet.hops", inspect.getsource(RNS.Link.rtt_packet))

    def test_receiving_interface_hop_count_increases_on_each_interface(self):
        # expected_hops comes from packet.hops, which Transport increments as a packet crosses interfaces, so a
        # requester can only inflate it; link_path and response_grace clamp it to MAX_GRACE_HOPS either way
        self.assertIn("packet.hops += 1", inspect.getsource(RNS.Transport.preprocess_inbound))

    def test_interface_has_a_bitrate(self):
        # server.link_path reads interface.bitrate (node-local config, or the interface's own guess)
        self.assertIn("self.bitrate", inspect.getsource(Interface.__init__))
        interface = Interface.__new__(Interface)
        interface.bitrate = 1_000_000
        self.assertEqual(getattr(interface, "bitrate", None), 1_000_000)

    def test_link_path_reads_them_and_falls_back_when_missing(self):
        config = Config()

        class _Interface:
            bitrate = 1_000

        class _Link:
            attached_interface = _Interface()
            expected_hops = 3

        self.assertEqual(server.link_path(_Link()), (_Link.attached_interface, 1_000, 3))
        # A changed RNS (attributes gone) falls back to the conservative unknowns, never crashing
        interface, bitrate, hops = server.link_path(object())
        self.assertEqual((interface, bitrate, hops), (None, None, None))
        self.assertEqual(server.response_grace(None, None, config), config.deadline_slack)
        self.assertEqual(server.progress_floor(None, None, config), config.unmeasured_bitrate / 8)


class RnsHalfOpenLinkInternalsTest(unittest.TestCase):
    """Each RNS attribute and method guards.limit_half_open (M1) hooks or reads is present and behaves as rnfs
    expects."""

    def test_destination_receive_calls_incoming_link_request_on_the_instance(self):
        # The guard replaces destination.incoming_link_request on the instance; RNS must look it up on self
        self.assertIn("self.incoming_link_request(plaintext,packet)", _normal_source(RNS.Destination.receive))
        self.assertEqual(list(inspect.signature(RNS.Destination.incoming_link_request).parameters),
                         ["self", "data", "packet"])

    def test_incoming_link_request_validates_and_appends_to_links(self):
        # half_open_links reads destination.links, which RNS appends each validated link to, in arrival order
        source = _normal_source(RNS.Destination.incoming_link_request)
        self.assertIn("RNS.Link.validate_request(self,data,packet)", source)
        self.assertIn("self.links.append(link)", source)

    def test_validate_request_leaves_a_half_open_link_with_its_interface_and_request_time(self):
        source = _normal_source(RNS.Link.validate_request)
        for expected in ("link.handshake()", "link.attached_interface=packet.receiving_interface",
                         "link.request_time=time.time()", "link.start_watchdog()"):
            self.assertIn(expected, source)
        # handshake() moves an incoming link from PENDING to HANDSHAKE; the RTT packet makes it ACTIVE
        self.assertIn("self.status=Link.HANDSHAKE", _normal_source(RNS.Link.handshake))
        self.assertIn("self.status=Link.ACTIVE", _normal_source(RNS.Link.rtt_packet))
        states = {RNS.Link.PENDING, RNS.Link.HANDSHAKE, RNS.Link.ACTIVE, RNS.Link.STALE, RNS.Link.CLOSED}
        self.assertEqual(len(states), 5)

    def test_teardown_closes_a_half_open_link_and_removes_it_from_its_destination(self):
        # The guard closes with Link.teardown(): on a HANDSHAKE link it sends the initiator a close packet (the
        # link keys exist after handshake()), marks it CLOSED and runs link_closed, which removes it from
        # destination.links, so the guard's count drops at once
        teardown = _normal_source(RNS.Link.teardown)
        self.assertIn("ifself.status!=Link.PENDINGandself.status!=Link.CLOSED:self.__teardown_packet()", teardown)
        self.assertIn("self.link_closed()", teardown)
        self.assertIn("self.destination.links.remove(self)", _normal_source(RNS.Link.link_closed))
        # Its watchdog thread wakes at least every WATCHDOG_MAX_SLEEP seconds and ends once the link is CLOSED
        self.assertLessEqual(RNS.Link.WATCHDOG_MAX_SLEEP, 5)
        self.assertIn("whilenotself.status==Link.CLOSED:", _normal_source(RNS.Link._Link__watchdog_job))

    def test_guard_runs_for_a_link_request_to_a_real_destination(self):
        from unittest import mock

        from rnfs.network import guards
        with mock.patch.object(RNS.Transport, "register_destination"):
            destination = RNS.Destination(RNS.Identity(), RNS.Destination.IN, RNS.Destination.SINGLE, "rnfs",
                                          "compat")
        guard = guards.limit_half_open(destination, 4)
        calls = []
        original = guard.original
        guard.original = lambda data, packet: calls.append(data) or original(data, packet)
        packet = mock.Mock(packet_type=RNS.Packet.LINKREQUEST, data=b"too short to be a link request",
                           receiving_interface=None)
        destination.receive(packet)
        self.assertEqual(calls, [b"too short to be a link request"])  # through the guard, then RNS's own
        self.assertEqual(destination.links, [])  # RNS rejected the malformed request as before
        self.assertEqual(guards.half_open_links(destination), [])


def _normal_source(function) -> str:
    """function's source without whitespace and with single quotes."""
    return _normal(textwrap.dedent(_source_or_empty(function)))


def _source_or_empty(function) -> str:
    """function's source, or "" if it can't be read."""
    try:
        return inspect.getsource(function)
    except (OSError, TypeError):
        return ""


if __name__ == "__main__":
    unittest.main()
