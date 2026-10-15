"""The RNS internals the /blocks slot fairness fix (H1) and the half-open link guard (M1) rely on are still
present in the installed RNS.

server.py's displacement protection measures a per-response grace from the receiving interface's bitrate and
the requester's hop count, read through server.link_path(). Those are RNS object attributes that the version
number doesn't promise to keep, so, like tests/test_rns_compat.py (another session's file, not edited here),
this checks each one and fails naming what changed rather than letting the grace silently fall back. The
follow-up also relies on how RNS marks the local interface to a shared instance (whose bitrate says nothing about
a link's path), on RNS raising a link's MTU only across interfaces that configure theirs, and on how a Resource's
advertisement and parts reach a slow radio.

guards.limit_half_open replaces a destination's incoming_link_request and reads Destination.links and each
link's status, attached_interface and request_time, and closes links with Link.teardown(); those are checked
the same way, with one functional check against a real (offline) RNS.Destination.

The response pre-check (L1) replaces Link.response_resource_concluded and Link.receive, reads a completed
Resource's data, status, has_metadata, request_id and total_size, decrypts response packets with Link.decrypt
and fails a refused response through Link.handle_response; each is checked, with functional checks on a real
(offline) Link. The request rate limit (L17b) relies on RNS sending nothing for a handler that returns None,
and the cooldown after a rejected response (L17a) on how RNS concludes an outgoing Resource the receiver
rejects.

Written against RNS 1.5.6. Nothing here starts Reticulum.
"""
import ast
import inspect
import io
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


class RnsGracePathInternalsTest(unittest.TestCase):
    """What the H1 follow-up (shared instances, multi-hop paths, the node's own slow radio) relies on: how RNS
    marks the interface to a shared instance, when it raises a link's MTU, and how it queues a Resource's parts."""

    def test_interface_to_a_shared_instance_is_told_by_its_attribute(self):
        # server.via_shared_instance tells the local interface to a shared instance the way RNS does
        self.assertIn("hasattr(interface,'is_connected_to_shared_instance')",
                      _normal_source(RNS.Transport.interface_to_shared_instance))
        from RNS.Interfaces import LocalInterface
        self.assertIn("self.is_connected_to_shared_instance=True",
                      _normal_source(LocalInterface.LocalClientInterface.connect))
        self.assertEqual({LocalInterface.LocalClientInterface.__name__, LocalInterface.LocalServerInterface.__name__},
                         set(server.rates.LOCAL_INTERFACE_TYPES))
        # Packets from the shared instance have a hop taken off, so a direct neighbour's link has expected_hops 1
        self.assertIn("elifTransport.interface_to_shared_instance(packet.receiving_interface):packet.hops-=1",
                      _normal_source(RNS.Transport._inbound))

    def test_link_path_gives_a_shared_instance_link_no_bitrate(self):
        from RNS.Interfaces.LocalInterface import LocalClientInterface
        interface = LocalClientInterface.__new__(LocalClientInterface)
        interface.bitrate = 1_000_000_000
        interface.is_connected_to_shared_instance = True

        class _Link:
            attached_interface = interface
            expected_hops = 1

        self.assertEqual(server.link_path(_Link()), (interface, None, 1))

    def test_link_mtu_is_raised_only_across_interfaces_that_configure_theirs(self):
        # server.fast_path: a link's MTU above 500 means every interface on the path autoconfigures its MTU
        # (at 62.5 kbit/s and up) or has a fixed one; transport nodes drop the MTU signalling otherwise
        self.assertIn("elifnotoutbound_interface.AUTOCONFIGURE_MTUandnotoutbound_interface.FIXED_MTU:",
                      _normal_source(RNS.Transport._inbound))
        self.assertIn("ifpacket.receiving_interface.AUTOCONFIGURE_MTUorpacket.receiving_interface.FIXED_MTU:",
                      _normal_source(RNS.Transport._inbound))
        from RNS.Interfaces.KISSInterface import KISSInterface
        from RNS.Interfaces.RNodeInterface import RNodeInterface
        for radio in (RNodeInterface, KISSInterface):
            self.assertFalse(radio.AUTOCONFIGURE_MTU or radio.FIXED_MTU, radio.__name__)
        interface = Interface.__new__(Interface)
        interface.AUTOCONFIGURE_MTU = True
        for bitrate, mtu in ((62_499, None), (62_500, 1024)):
            interface.bitrate = bitrate
            interface.optimise_mtu()
            self.assertEqual(interface.HW_MTU, mtu, bitrate)
        self.assertTrue(callable(RNS.Link.get_mtu))

    def test_parts_queue_on_a_slow_radio_without_rns_knowing_when_they_leave(self):
        # server.QUEUED_WINDOW_BITS: one window of parts on a slow link (WINDOW_MAX_SLOW) of one packet each
        self.assertEqual(RNS.Resource.WINDOW_MAX_SLOW, 10)
        self.assertEqual(server.QUEUED_WINDOW_BITS, RNS.Resource.WINDOW_MAX_SLOW * RNS.Reticulum.MTU * 8)
        # RNS registers an outgoing Resource (where rnfs restarts its grace) right after handing its
        # advertisement to the interface; an RNode's flow control is off unless configured, so the packet goes
        # to the device at once and RNS can't tell when it is on the air
        source = _normal_source(RNS.Resource._Resource__advertise_job)
        self.assertLess(source.index("self.advertisement_packet.send()"),
                        source.index("self.link.register_outgoing_resource(self)"))
        from RNS.Interfaces.RNodeInterface import RNodeInterface
        self.assertIn("flow_control=c.as_bool('flow_control')if'flow_control'incelseFalse",
                      _normal_source(RNodeInterface.__init__))


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


class RnsResponsePrecheckInternalsTest(unittest.TestCase):
    """Each RNS method guards hooks or calls to check responses before RNS decodes them (L1) is present and
    behaves as rnfs expects."""

    def setUp(self):
        from rnfs.network import guards
        self.guards = guards
        guards.install()

    def test_response_resource_concluded_is_looked_up_on_the_link_and_decodes_the_data(self):
        # RNS passes self.response_resource_concluded (looked up when the advertisement arrives, so the class
        # attribute the guard replaces) as the response Resource's callback, which decodes resource.data
        receive = _normal_source(RNS.Link._Link__receive)
        self.assertIn("RNS.Resource.accept(packet,callback=self.response_resource_concluded,", receive)
        original = _normal_source(self.guards._original_response_resource_concluded)
        for expected in ("ifresource.status==RNS.Resource.COMPLETE:", "ifresource.has_metadata:",
                         "packed_response=resource.data.read()", "umsgpack.unpackb(packed_response)",
                         "self.handle_response(request_id,response_data,resource.total_size,resource.size)"):
            self.assertIn(expected, original)
        self.assertEqual(list(inspect.signature(self.guards._original_response_resource_concluded).parameters),
                         ["self", "resource"])

    def test_completed_resource_data_is_a_seekable_file_and_request_id_is_set(self):
        # assemble() marks the Resource COMPLETE, then hands the callback an open file the guard reads and rewinds
        assemble = _normal_source(RNS.Resource.assemble)
        self.assertIn("self.status=Resource.COMPLETE", assemble)
        self.assertIn("self.data=open(self.storagepath,'rb')", assemble)
        self.assertLess(assemble.index("self.data=open(self.storagepath,'rb')"), assemble.index("self.callback(self)"))
        # Metadata is decoded inside assemble, before any callback: so rnfs refuses it at the advertisement
        self.assertIn("self.metadata=umsgpack.unpackb(metadata_file.read())", assemble)
        accept = _normal_source(self.guards._original_accept)
        self.assertIn("resource=Resource(None,advertisement_packet.link,request_id=request_id)", accept)
        self.assertIn("self.request_id=request_id", _normal_source(RNS.Resource.__init__))
        self.assertIn("resource.has_metadata=True", accept)
        self.assertIn("ifadv.x:", accept)
        self.assertIn("total_size", inspect.getsource(RNS.Resource.__init__) + inspect.getsource(RNS.Resource.accept))

    def test_advertisement_x_flag_is_has_metadata(self):
        from RNS.vendor import umsgpack
        adv = RNS.ResourceAdvertisement.unpack(umsgpack.packb(
            {"t": 100, "d": 90, "n": 1, "h": bytes(32), "r": bytes(4), "o": bytes(32), "i": 1, "l": 1,
             "q": bytes(16), "f": 0x01 | 0x10 | 0x20, "m": bytes(4)}))
        self.assertIs(adv.x, True)
        self.assertIn("self.x=resource.has_metadata", _normal_source(RNS.ResourceAdvertisement.__init__))

    def test_handle_response_hands_the_response_to_the_pending_request(self):
        self.assertEqual(list(inspect.signature(RNS.Link.handle_response).parameters)[:5],
                         ["self", "request_id", "response_data", "response_size", "response_transfer_size"])
        source = _normal_source(RNS.Link.handle_response)
        self.assertIn("ifpending_request.request_id==request_id:", source)
        self.assertIn("pending_request.response_received(response_data,metadata)", source)

    def test_receive_is_called_on_the_link_and_decodes_response_packets(self):
        # Transport hands every link packet to link.receive (the class attribute the guard replaces), which
        # decrypts a RESPONSE packet and decodes it before matching it to a request
        self.assertIn("else:link.receive(packet)", _normal_source(RNS.Transport))
        self.assertEqual(list(inspect.signature(self.guards._original_receive).parameters), ["self", "packet"])
        self.assertIn("self.__receive(packet)", _normal_source(self.guards._original_receive))
        receive = _normal_source(RNS.Link._Link__receive)
        branch = receive[receive.index("elifpacket.context==RNS.Packet.RESPONSE:"):]
        self.assertIn("packed_response=self.decrypt(packet.data)", branch)
        self.assertIn("unpacked_response=umsgpack.unpackb(packed_response)", branch)
        self.assertEqual(list(inspect.signature(RNS.Link.decrypt).parameters), ["self", "ciphertext"])
        self.assertIn("self.initiator", inspect.getsource(RNS.Link.__init__))

    def test_default_mtu_packets_are_below_the_check_size(self):
        # Response packets larger than RNS's default MTU are checked; a default-MTU link never carries one
        self.assertEqual(self.guards.PACKET_CHECK_SIZE, RNS.Reticulum.MTU)
        self.assertLess(RNS.Link.MDU + RNS.Identity.TOKEN_OVERHEAD + 16, self.guards.PACKET_CHECK_SIZE)

    def test_guards_run_on_a_real_link(self):
        from unittest import mock

        from RNS.vendor import umsgpack
        with mock.patch.object(RNS.Transport, "register_destination"):
            dht = RNS.Destination(RNS.Identity(), RNS.Destination.IN, RNS.Destination.SINGLE, "rnfs", "dht")
        link = RNS.Link(owner=dht)  # incoming, as RNS makes one for a link request
        self.assertTrue(self.guards.is_dht_link(link) and not link.initiator)
        bomb = b"\x92\xc4\x10" + bytes(16) + b"\xdd\x00\x01\x00\x00" + b"\x90" * 65536
        with mock.patch.object(umsgpack, "unpackb", wraps=umsgpack.unpackb) as unpackb, \
                mock.patch.object(self.guards, "_log_refused_response"), \
                mock.patch.object(self.guards, "_log_unexpected_response"), \
                mock.patch.object(link, "decrypt") as decrypt:
            link.receive(mock.Mock(context=RNS.Packet.RESPONSE, packet_type=RNS.Packet.DATA, data=bomb))
            decrypt.assert_not_called()  # an incoming link's response packet is dropped unread
            resource = mock.Mock(status=RNS.Resource.COMPLETE, has_metadata=False, data=io.BytesIO(bomb),
                                 request_id=bytes(16), total_size=len(bomb), size=len(bomb))
            link.response_resource_concluded(resource)
            unpackb.assert_not_called()
            resource.data = io.BytesIO(umsgpack.packb([bytes(16), [b"block"]]))
            link.response_resource_concluded(resource)
            unpackb.assert_called_once()  # an honest response reaches RNS's own decoding


class RnsRequestRateInternalsTest(unittest.TestCase):
    """The request rate limit (L17b) answers a request over the limit with None: RNS then sends nothing."""

    def test_none_response_sends_nothing(self):
        source = _normal_source(RNS.Link.handle_request)
        self.assertIn("ifresponse!=None:", source)
        # and RNS starts a thread per request packet before any handler runs (what the limit can't prevent)
        receive = _normal_source(RNS.Link._Link__receive)
        branch = receive[receive.index("elifpacket.context==RNS.Packet.REQUEST:"):]
        self.assertIn("threading.Thread(target=job,daemon=True).start()", branch)


class RnsRejectedResourceInternalsTest(unittest.TestCase):
    """A rejected outgoing Resource concludes as the server expects (L17a): status REJECTED, then the callback,
    with get_progress() counting the parts sent."""

    def test_rejection_sets_rejected_and_runs_the_callback(self):
        receive = _normal_source(RNS.Link._Link__receive)
        branch = receive[receive.index("elifpacket.context==RNS.Packet.RESOURCE_RCL:"):]
        self.assertIn("forresourceinself.outgoing_resources:ifresource_hash==resource.hash:resource._rejected()",
                      branch)
        rejected = _normal_source(RNS.Resource._rejected)
        for expected in ("self.status=Resource.REJECTED", "self.link.cancel_outgoing_resource(self)",
                         "self.callback(self)"):
            self.assertIn(expected, rejected)
        self.assertGreater(RNS.Resource.REJECTED, RNS.Resource.COMPLETE)  # the monitor treats it as concluded

    def test_progress_of_an_outgoing_resource_counts_sent_parts(self):
        progress = _normal_source(RNS.Resource.get_progress)
        self.assertIn("elifself.initiator:ifnotself.split:self.processed_parts=self.sent_parts", progress)


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
