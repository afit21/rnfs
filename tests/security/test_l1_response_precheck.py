"""L1: RNS decodes a whole rnfs.dht response with umsgpack before rnfs can check it, so a payload of one-byte
empty arrays (0x90) or maps (0x80) costs some 70 times its size in Python objects.

Fixed: guards wraps Link.response_resource_concluded and Link.receive so that, on rnfs.dht links, a response's
msgpack headers are checked first (protocol.response_refusal), and refuses Resources with metadata at their
advertisement. These tests run the real RNS methods behind the guards with fake links: every honest response
shape still reaches the requester decoded as before; the bombs are refused with umsgpack.unpackb never called;
and a refused response fails the /blocks request exactly as a malformed response does.
"""
import io
import types
import unittest
from unittest import mock

import RNS
from RNS.vendor import umsgpack

from rnfs.config import Config
from rnfs.constants import ID_LENGTH, K, MAX_BLOCK_SIZE, MIB
from rnfs.content.cid import RAW, Cid
from rnfs.network import client, guards, links, protocol
from rnfs.features import keepers

import tests.test_links as fakes

REQUEST_ID = bytes(range(16))
ID = bytes(ID_LENGTH)
DHT = types.SimpleNamespace(name=guards.DHT_NAME + "." + "ab" * 16)
LXMF = types.SimpleNamespace(name="lxmf.delivery." + "ab" * 16)


def bomb(size: int, code: int) -> bytes:
    """size bytes of response framing whose response is a list of one-byte empty arrays (0x90) or maps (0x80)."""
    count = size - 24
    return b"\x92\xc4\x10" + REQUEST_ID + b"\xdd" + count.to_bytes(4, "big") + bytes([code]) * count


def honest_responses() -> dict:
    """Every shape an honest rnfs.dht response can have, by name."""
    block = bytes(MAX_BLOCK_SIZE)
    record = [bytes(keepers.PUBLIC_KEY_LENGTH), keepers.NAME_HASH, bytes(keepers.RANDOM_HASH_LENGTH),
              bytes(keepers.RATCHET_LENGTH), bytes(keepers.SIGNATURE_LENGTH), bytes(keepers.MAX_APP_DATA)]
    return {
        "True (/provide, /pin)": True,
        "False (a refusal)": False,
        "/find": [[ID] * K, [ID] * K],
        "/find, empty": [[], []],
        "/blocks, empty": [],
        "/blocks, missing": [None],
        "/blocks, one largest block": [block],
        "/blocks, most entries": [None, block] + [b"x" * 100] * (protocol.MAX_BLOCKS_CIDS - 2),
        "/blocks, two blocks": [b"a" * 1000, b"b" * 2000],
        "/name": record,
        "/name, none kept": [],
    }


class ResponseLink:
    """An rnfs.dht (or other) link as far as Link.response_resource_concluded, receive and handle_response use it:
    handle_response records what RNS hands over."""

    def __init__(self, destination=DHT, initiator=True):
        """An active link to destination."""
        self.destination = destination
        self.initiator = initiator
        self.status = RNS.Link.ACTIVE
        self.pending_requests = []
        self.handled = []

    def handle_response(self, request_id, response_data, response_size, response_transfer_size, metadata=None,
                        update_sizes=False, check_size=False):
        """Record the response RNS hands over."""
        self.handled.append((request_id, response_data))

    @staticmethod
    def decrypt(data):
        """No encryption in these tests."""
        return data


def completed(packed: bytes, request_id=REQUEST_ID, has_metadata=False):
    """A response Resource that has completed with packed, as RNS hands it to response_resource_concluded."""
    return types.SimpleNamespace(status=RNS.Resource.COMPLETE, has_metadata=has_metadata, data=io.BytesIO(packed),
                                 request_id=request_id, total_size=len(packed), size=len(packed))


def response_packet(packed: bytes):
    """A response packet carrying packed (unencrypted: ResponseLink.decrypt is the identity)."""
    return types.SimpleNamespace(context=RNS.Packet.RESPONSE, packet_type=RNS.Packet.DATA, data=packed)


class _Guarded(unittest.TestCase):
    """Guards installed, with umsgpack.unpackb counted."""

    def setUp(self):
        guards.install()
        self.unpackb = mock.Mock(wraps=umsgpack.unpackb)
        patcher = mock.patch.object(umsgpack, "unpackb", self.unpackb)
        patcher.start()
        self.addCleanup(patcher.stop)
        for name in ("_log_refused_response", "_log_unexpected_response"):
            patcher = mock.patch.object(guards, name, mock.Mock())
            patcher.start()
            self.addCleanup(patcher.stop)


class ResponseResourceTest(_Guarded):
    """Responses sent as a Resource: RNS's own response_resource_concluded behind the guard."""

    def test_guard_is_installed(self):
        self.assertTrue(guards.installed())
        self.assertIs(RNS.Link.__dict__["response_resource_concluded"], guards.guarded_response_resource_concluded)

    def test_honest_responses_pass_unchanged(self):
        for name, response in honest_responses().items():
            with self.subTest(name):
                link = ResponseLink()
                RNS.Link.response_resource_concluded(link, completed(umsgpack.packb([REQUEST_ID, response])))
                self.assertEqual(link.handled, [(REQUEST_ID, response)])  # decoded by RNS, as before

    def test_bombs_are_refused_without_decoding(self):
        for size in (129 * 1024, MIB - 1):
            for code in (0x90, 0x80):
                with self.subTest(size=size, code=hex(code)):
                    link = ResponseLink()
                    self.unpackb.reset_mock()
                    RNS.Link.response_resource_concluded(link, completed(bomb(size, code)))
                    self.unpackb.assert_not_called()
                    self.assertEqual(link.handled, [(REQUEST_ID, None)])  # the request fails as malformed

    def test_shapes_no_parser_accepts_are_refused(self):
        for response in ([[]] * 5, [{}] * 5, [1], "text", [bytes(MAX_BLOCK_SIZE + 1)], [None] * 27,
                         [[ID] * (K + 1), []], {b"a": b"b"}):
            with self.subTest(response=repr(response)[:40]):
                link = ResponseLink()
                self.unpackb.reset_mock()
                RNS.Link.response_resource_concluded(link, completed(umsgpack.packb([REQUEST_ID, response])))
                self.unpackb.assert_not_called()
                self.assertEqual(link.handled, [(REQUEST_ID, None)])

    def test_metadata_refused_on_rnfs_links(self):
        link = ResponseLink()
        RNS.Link.response_resource_concluded(link, completed(umsgpack.packb([REQUEST_ID, True]), has_metadata=True))
        self.assertEqual(link.handled, [(REQUEST_ID, None)])

    def test_other_destinations_untouched(self):
        # LXMF's links (and any not rnfs.dht) go straight to RNS, even with what rnfs would refuse
        link = ResponseLink(LXMF)
        RNS.Link.response_resource_concluded(link, completed(umsgpack.packb([REQUEST_ID, [[]] * 3])))
        self.assertEqual(link.handled, [(REQUEST_ID, [[], [], []])])
        self.unpackb.assert_called()

    def test_failed_resources_left_to_rns(self):
        link = ResponseLink()
        link.pending_requests = [mock.Mock(request_id=REQUEST_ID)]
        resource = completed(bomb(4096, 0x90))
        resource.status = RNS.Resource.FAILED
        RNS.Link.response_resource_concluded(link, resource)
        link.pending_requests[0].request_timed_out.assert_called_once_with(None)  # RNS's own handling

    def test_check_reads_headers_only(self):
        # The check costs the same for any size: it never reads a bin's contents or builds objects
        for packed in (bomb(MIB - 1, 0x90), umsgpack.packb([REQUEST_ID, [bytes(MAX_BLOCK_SIZE)] * 8])):
            with mock.patch.object(protocol, "_bin_at", wraps=protocol._bin_at) as bins, \
                    mock.patch.object(protocol, "_array_at", wraps=protocol._array_at) as arrays:
                protocol.response_refusal(packed)
            self.assertLessEqual(bins.call_count + arrays.call_count, protocol.MAX_BLOCKS_CIDS + 4)


class ResponsePacketTest(_Guarded):
    """Responses sent as one packet: RNS's own Link.receive behind the guard."""

    def setUp(self):
        super().setUp()
        self.received = []
        patcher = mock.patch.object(guards, "_original_receive", lambda link, packet: self.received.append(packet))
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_guard_is_installed(self):
        self.assertIs(RNS.Link.__dict__["receive"], guards.guarded_receive)

    def test_honest_responses_pass_unchanged(self):
        for name, response in honest_responses().items():
            with self.subTest(name):
                link = ResponseLink()
                packet = response_packet(umsgpack.packb([REQUEST_ID, response]))
                RNS.Link.receive(link, packet)
                self.assertEqual(self.received, [packet])
                self.received.clear()

    def test_bombs_refused_on_links_with_a_large_mtu(self):
        # Only a link that negotiated a large MTU carries such a packet (TCP: 16 KiB and more)
        for code in (0x90, 0x80):
            link = ResponseLink()
            RNS.Link.receive(link, response_packet(bomb(16 * 1024, code)))
            self.assertEqual(self.received, [])
            self.unpackb.assert_not_called()
            self.assertEqual(link.handled, [(REQUEST_ID, None)])

    def test_default_mtu_packets_not_decrypted_twice(self):
        link = ResponseLink()
        link.decrypt = mock.Mock()
        packet = response_packet(bomb(guards.PACKET_CHECK_SIZE, 0x90))
        RNS.Link.receive(link, packet)
        link.decrypt.assert_not_called()
        self.assertEqual(self.received, [packet])  # at most ~500 objects: left to RNS

    def test_responses_on_incoming_links_dropped_unread(self):
        link = ResponseLink(initiator=False)
        link.decrypt = mock.Mock()
        for packed in (umsgpack.packb([REQUEST_ID, True]), bomb(16 * 1024, 0x90)):
            RNS.Link.receive(link, response_packet(packed))
        self.assertEqual(self.received, [])
        link.decrypt.assert_not_called()

    def test_other_packets_and_destinations_untouched(self):
        link = ResponseLink()
        request = types.SimpleNamespace(context=RNS.Packet.REQUEST, packet_type=RNS.Packet.DATA, data=b"x" * 600)
        RNS.Link.receive(link, request)
        lxmf_link = ResponseLink(LXMF, initiator=False)
        response = response_packet(bomb(16 * 1024, 0x90))
        RNS.Link.receive(lxmf_link, response)
        self.assertEqual(self.received, [request, response])


class MetadataAdvertisementTest(unittest.TestCase):
    """RNS decodes a Resource's metadata as it completes, so rnfs.dht refuses metadata at the advertisement."""

    def test_refused_on_rnfs_links_only(self):
        adv = types.SimpleNamespace(t=1100, d=1000, u=False, l=1, i=1, x=True)
        self.assertEqual(guards.refusal(adv, ResponseLink()), "rnfs responses carry no metadata")
        self.assertEqual(guards.refusal(adv, ResponseLink(initiator=False)), "rnfs responses carry no metadata")
        self.assertIsNone(guards.refusal(adv, ResponseLink(LXMF)))
        adv.x = False
        self.assertIsNone(guards.refusal(adv, ResponseLink()))

    def test_real_advertisement_flag(self):
        # RNS packs has_metadata as the x flag; a response with metadata is a file response (an open file)
        adv = RNS.ResourceAdvertisement.unpack(umsgpack.packb(
            {"t": 1100, "d": 1000, "n": 1, "h": bytes(32), "r": bytes(4), "o": bytes(32), "i": 1, "l": 1,
             "q": REQUEST_ID, "f": 0x01 | 0x10 | 0x20, "m": bytes(4)}))
        self.assertTrue(adv.x)
        self.assertEqual(guards.refusal(adv, ResponseLink()), "rnfs responses carry no metadata")


class ProviderFailureTest(_Guarded):
    """A refused response fails the /blocks request (and so the provider) exactly as a malformed one does."""

    def setUp(self):
        super().setUp()
        patcher = mock.patch.object(links, "RNS", fakes.fake_rns())
        patcher.start()
        self.addCleanup(patcher.stop)
        fakes.FakeLink.created = []
        fakes.FakeLink.establish_delay = 0.0
        self.pool = links.LinkPool(object(), Config(stall_timeout=5))
        self.addCleanup(self.pool.close_all)
        self.client = client.PeerClient(self.pool, Config())
        self.cid = Cid.for_block(RAW, b"block")

    def fetch(self, packed: bytes) -> tuple[client.BlocksResult, object]:
        """client.blocks() against a node whose response arrives as packed, through RNS behind the guard: the
        result and how the request was counted in traffic."""
        def respond(link, path, data):
            """What RNS hands the requester for packed: through the guard and RNS's own decoding."""
            rns_link = ResponseLink()
            RNS.Link.response_resource_concluded(rns_link, completed(packed))
            ((_, response),) = rns_link.handled
            return response
        fakes.FakeLink.responder = staticmethod(respond)
        self.client.traffic = mock.Mock()
        result = fakes.run(self, lambda: self.client.blocks(bytes(16), [self.cid]))
        return result, self.client.traffic.request.call_args

    def test_refused_response_fails_like_a_malformed_one(self):
        # Malformed today: decodable, but more entries than CIDs requested, so the parser refuses it
        malformed, malformed_traffic = self.fetch(umsgpack.packb([REQUEST_ID, [b"x", b"y", b"z"]]))
        self.assertEqual((malformed.status, malformed.reason), (client.FAILED, "malformed response"))
        for code in (0x90, 0x80):
            with self.subTest(code=hex(code)):
                refused, refused_traffic = self.fetch(bomb(129 * 1024, code))
                self.assertEqual((refused.status, refused.reason, refused.unsent, refused.blocks),
                                 (malformed.status, malformed.reason, malformed.unsent, malformed.blocks))
                self.assertEqual(refused_traffic, malformed_traffic)


if __name__ == "__main__":
    unittest.main()
