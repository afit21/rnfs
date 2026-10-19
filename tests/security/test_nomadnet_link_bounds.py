"""NomadNet links (`rnfs add <destination>:/page/…`, rnfs/network/nomadnet.py): what a hostile NomadNet node can
send over the link rnfs opens to it, and that rnfs.dht links keep every rule they had.

NomadNet sends compressed Resources of several segments and files with metadata, which rnfs.dht links refuse, while
the daemon's process-wide decompression limit is 0. So the fetch bounds its own link: each segment may decompress
to one segment (SEGMENT_LIMIT, set by its resource_started callback, since RNS checks a decompressed size against
neither the advertised total nor any bound of its own) and must follow the sequence an honest sender keeps (the
first's original hash and total, the next index, the segment count the total gives); what RNS assembles may not
outgrow the total; a file's metadata must be a small flat map, checked before RNS decodes it; a page answer must
be one flat byte string, checked from its msgpack headers before RNS decodes it (the guards' rnfs.dht response
check, extended). These tests run the guards and the fetch's Transfer with fake links, and RNS's own
Resource.assemble for the decompression bound and the metadata: each hostile answer is refused (the transfer
cancelled, the link closed), each honest one passes, and rnfs.dht links still refuse compressed, multi-segment and
metadata Resources in both scopes.
"""
import bz2
import os
import tempfile
import types
import unittest
from unittest import mock

import RNS
from RNS.vendor import umsgpack

from rnfs.errors import NotFound, TooLarge
from rnfs.network import guards, nomadnet as fetching, protocol

from tests.security.test_l1_response_precheck import DHT, REQUEST_ID, ResponseLink, bomb, completed, response_packet
from tests.test_embedded import DHT_LINK_NAME, advertisement, link_named

SEGMENT = RNS.Resource.MAX_EFFICIENT_SIZE
NOMADNET = types.SimpleNamespace(name="nomadnetwork.node." + "cd" * 16, hash=bytes(range(16)))


def nomadnet_link(**attributes):
    """A stand-in link this node opened to a NomadNet node, marked as the fetch marks it."""
    link = link_named(NOMADNET.name, **attributes)
    guards.mark_nomadnet(link)
    return link


def segments(total: int, metadata: bool = False, compressed: bool = True) -> list:
    """The receiving side's Resources (the fields the checks read) of an honest answer of total bytes."""
    count = fetching.expected_segments(total)
    original = os.urandom(32)
    result = []
    for index in range(1, count + 1):
        hash_ = original if index == 1 else os.urandom(32)
        result.append(types.SimpleNamespace(segment_index=index, total_segments=count, total_size=total,
                                            original_hash=original, hash=hash_, has_metadata=metadata,
                                            compressed=compressed, status=RNS.Resource.TRANSFERRING,
                                            max_decompressed_size=0, cancel=mock.Mock()))
    return result


class _Scoped(unittest.TestCase):
    """The guards installed in a scope; the scope and RNS's process-wide limit put back afterwards."""

    def setUp(self):
        saved = guards._scope, guards._compress_limit, RNS.Resource.AUTO_COMPRESS_MAX_SIZE

        def restore():
            """Put the scope and the process-wide limit back as they were."""
            guards._scope, guards._compress_limit, RNS.Resource.AUTO_COMPRESS_MAX_SIZE = saved
        self.addCleanup(restore)
        guards.install()  # wrapped before anything below patches what it wraps
        for name in ("_log_refused_response", "_log_unexpected_response", "_log_unexpected_request"):
            patcher = mock.patch.object(guards, name, mock.Mock())
            patcher.start()
            self.addCleanup(patcher.stop)


# Honest NomadNet advertisements: a compressed file segment with metadata, a middle and a last segment of a
# 2.5 MiB file, and a full incompressible segment (its transfer size is the largest an honest one has)
HONEST = {
    "first of three, compressed, with metadata": dict(transfer=400_000, data=5 * SEGMENT // 2, compressed=True,
                                                      metadata=True, segments=3),
    "full incompressible segment": dict(transfer=SEGMENT + 68, data=5 * SEGMENT // 2, segments=3),
    "one-segment page, compressed": dict(transfer=2_000, data=20_000, compressed=True),
}
# What no honest NomadNet node sends, refused on NomadNet links in both scopes
HOSTILE = {
    "request": dict(request=True),
    "transfer over data": dict(transfer=2_000_000, data=1000),
    "transfer over a segment": dict(transfer=guards.MAX_TRANSFER_SIZE + 1, data=10 * SEGMENT, segments=10),
    "malformed sizes": dict(transfer=-1),
}


class AdvertisementTest(_Scoped):
    """guards.refusal and the guarded Resource.accept on NomadNet links, beside rnfs.dht and other links."""

    def setUp(self):
        super().setUp()
        self.accepted, self.rejected = [], []
        self.resource = mock.Mock(spec=RNS.Resource)
        patches = [mock.patch.object(guards, "_original_accept",
                                     lambda packet, **kwargs: self.accepted.append(packet) or self.resource),
                   mock.patch.object(RNS.Resource, "reject", self.rejected.append)]
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    def accepts(self, link, **kwargs) -> bool:
        """True if the installed guard passes an advertisement on link to RNS."""
        packet = advertisement(link, **kwargs)
        accepted = RNS.Resource.accept(packet) is not None
        self.assertEqual(not accepted, packet in self.rejected)
        return accepted

    def test_honest_nomadnet_resources_pass_in_both_scopes(self):
        for scope in (guards.PROCESS, guards.DHT):
            guards.install(scope)
            for name, kwargs in HONEST.items():
                with self.subTest(scope=scope, advertisement=name):
                    self.resource.max_decompressed_size = SEGMENT + 1  # as the fetch's callback set it
                    self.assertTrue(self.accepts(nomadnet_link(), **kwargs))
                    # Not zeroed as on rnfs.dht links: the fetch bounds it per segment
                    self.assertEqual(self.resource.max_decompressed_size, SEGMENT + 1)

    def test_rnfs_dht_links_still_refuse_them(self):
        for scope in (guards.PROCESS, guards.DHT):
            guards.install(scope)
            for name, kwargs in HONEST.items():
                with self.subTest(scope=scope, advertisement=name):
                    self.assertFalse(self.accepts(link_named(DHT_LINK_NAME), **kwargs))
            for flag, reason in (("compressed", "never compressed"), ("metadata", "no metadata")):
                adv = RNS.ResourceAdvertisement.unpack(advertisement(None, **{flag: True}).plaintext)
                self.assertIn(reason, guards.refusal(adv, link_named(DHT_LINK_NAME)))
            adv = RNS.ResourceAdvertisement.unpack(advertisement(None, segments=2, data=2 * SEGMENT,
                                                                 transfer=SEGMENT).plaintext)
            self.assertIn("one segment", guards.refusal(adv, link_named(DHT_LINK_NAME)))

    def test_hostile_sizes_refused_on_nomadnet_links_in_both_scopes(self):
        for scope in (guards.PROCESS, guards.DHT):
            guards.install(scope)
            for name, kwargs in HOSTILE.items():
                with self.subTest(scope=scope, advertisement=name):
                    self.assertFalse(self.accepts(nomadnet_link(), **kwargs))

    def test_an_embedded_nodes_host_keeps_its_own_nomadnet_links(self):
        # Not marked: a NomadNet client in the host program, whose links stay RNS's alone
        guards.install(guards.DHT)
        for name, kwargs in HOSTILE.items():
            with self.subTest(advertisement=name):
                self.assertTrue(self.accepts(link_named(NOMADNET.name), **kwargs))

    def test_process_wide_limit_stays_zero_in_the_daemon(self):
        RNS.Resource.AUTO_COMPRESS_MAX_SIZE = 64 * 1024 * 1024
        guards._compress_limit = None
        guards.install(guards.PROCESS)
        transfer = fetching.Transfer(nomadnet_link(teardown=mock.Mock()), "unused", None)
        transfer.resource_started(segments(SEGMENT)[0])
        self.assertEqual(RNS.Resource.AUTO_COMPRESS_MAX_SIZE, 0)  # per Resource, never process-wide


class PageAnswerTest(_Scoped):
    """Page answers checked from their msgpack headers before RNS decodes them, as rnfs.dht responses are."""

    def setUp(self):
        super().setUp()
        guards.install()
        self.unpackb = mock.Mock(wraps=umsgpack.unpackb)
        patcher = mock.patch.object(umsgpack, "unpackb", self.unpackb)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.received = []
        patcher = mock.patch.object(guards, "_original_receive", lambda link, packet: self.received.append(packet))
        patcher.start()
        self.addCleanup(patcher.stop)

    @staticmethod
    def link() -> ResponseLink:
        """A NomadNet link, marked, recording what RNS hands over."""
        link = ResponseLink(NOMADNET)
        guards.mark_nomadnet(link)
        return link

    def test_honest_pages_decode_as_before(self):
        for page in (b">Hello\n" * 30_000, "text page" * 100, b"", None, False):
            with self.subTest(page=repr(page)[:30]):
                link = self.link()
                RNS.Link.response_resource_concluded(link, completed(umsgpack.packb([REQUEST_ID, page])))
                self.assertEqual(link.handled, [(REQUEST_ID, page)])

    def test_structures_refused_without_decoding(self):
        for packed in (bomb(129 * 1024, 0x90), bomb(64 * 1024, 0x80), umsgpack.packb([REQUEST_ID, [b"a"] * 10]),
                       umsgpack.packb([REQUEST_ID, {b"a": b"b"}]), umsgpack.packb([REQUEST_ID, 5]),
                       umsgpack.packb([REQUEST_ID, b"page"]) + b"\x90" * 100):
            with self.subTest(packed=packed[:24].hex()):
                link = self.link()
                self.unpackb.reset_mock()
                RNS.Link.response_resource_concluded(link, completed(packed))
                self.unpackb.assert_not_called()
                self.assertEqual(link.handled, [(REQUEST_ID, None)])  # the fetch fails: nothing usable came

    def test_file_answers_handed_over_undecoded(self):
        link = self.link()
        file = completed(b"\x90" * 50_000, has_metadata=True)
        file.metadata = {"name": b"song.mp3"}
        RNS.Link.response_resource_concluded(link, file)
        self.unpackb.assert_not_called()
        self.assertIs(link.handled[0][1], file.data)

    def test_answers_after_a_refusal_dropped_unread(self):
        link = self.link()
        link.status = RNS.Link.CLOSED  # the fetch refused a segment and closed the link
        RNS.Link.response_resource_concluded(link, completed(umsgpack.packb([REQUEST_ID, b"x" * 2_000_000])))
        self.unpackb.assert_not_called()
        self.assertEqual(link.handled, [])

    def test_packets_checked_too(self):
        link = self.link()
        RNS.Link.receive(link, response_packet(bomb(16 * 1024, 0x90)))
        self.assertEqual((self.received, link.handled), ([], [(REQUEST_ID, None)]))
        honest = response_packet(umsgpack.packb([REQUEST_ID, b"p" * 16 * 1024]))
        RNS.Link.receive(link, honest)
        self.assertEqual(self.received, [honest])

    def test_requests_on_nomadnet_links_dropped(self):
        link = self.link()
        RNS.Link.receive(link, types.SimpleNamespace(context=RNS.Packet.REQUEST, packet_type=RNS.Packet.DATA,
                                                     data=b"\x93" * 400))
        self.assertEqual(self.received, [])

    def test_each_check_keeps_to_its_links(self):
        # A page is no rnfs.dht response, and an rnfs.dht /find answer is no page
        dht = ResponseLink(DHT)
        RNS.Link.response_resource_concluded(dht, completed(umsgpack.packb([REQUEST_ID, b"page"])))
        self.assertEqual(dht.handled, [(REQUEST_ID, None)])
        link = self.link()
        RNS.Link.response_resource_concluded(link, completed(umsgpack.packb([REQUEST_ID, [[], []]])))
        self.assertEqual(link.handled, [(REQUEST_ID, None)])

    def test_page_check_reads_only_the_head(self):
        packed = umsgpack.packb([REQUEST_ID, b"x" * 3_000_000])
        self.assertIsNone(protocol.page_response_refusal(packed[:protocol.PAGE_HEAD_SIZE], len(packed)))
        self.assertIsNotNone(protocol.page_response_refusal(packed[:protocol.PAGE_HEAD_SIZE], len(packed) + 1))
        self.assertIsNotNone(protocol.page_response_refusal(packed[:protocol.PAGE_HEAD_SIZE], len(packed) - 1))


class _Transfers(_Scoped):
    """A Transfer on a fake NomadNet link whose teardown is recorded."""

    def setUp(self):
        super().setUp()
        guards.install()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.link = nomadnet_link(teardown=mock.Mock(), status=RNS.Link.ACTIVE, incoming_resources=[],
                                  outgoing_resources=[])
        self.link.destination = NOMADNET
        self.transfer = fetching.Transfer(self.link, os.path.join(self.tmp.name, "copy"), 50 * SEGMENT)

    def assert_refused(self, resource=None):
        """The transfer refused: resource (if given) cancelled, the link closed, the waiter woken."""
        self.assertIsNotNone(self.transfer.refused)
        self.link.teardown.assert_called()
        self.assertTrue(self.transfer.done.is_set())
        if resource is not None:
            resource.cancel.assert_called()

    def start(self, resource):
        """Start resource as RNS does, then mark it complete as its assembly would."""
        self.transfer.resource_started(resource)
        if self.transfer.refused is None:
            resource.status = RNS.Resource.COMPLETE


class SegmentSequenceTest(_Transfers):
    """Transfer.resource_started: each segment's bound and its place in the sequence."""

    def test_honest_sequences_pass(self):
        for total, metadata in ((100, False), (SEGMENT, True), (SEGMENT + 1, True), (5 * SEGMENT // 2, True)):
            with self.subTest(total=total):
                self.setUp()
                answer = segments(total, metadata)
                for resource in answer:
                    self.start(resource)
                    self.assertEqual(resource.max_decompressed_size, fetching.SEGMENT_LIMIT)
                self.assertIsNone(self.transfer.refused)
                self.link.teardown.assert_not_called()
                self.assertEqual(self.transfer.total, total)

    def hostile(self, answer, mutate):
        """Start answer's segments, mutating the last one first; it must be refused."""
        mutate(answer[-1])
        for resource in answer[:-1]:
            self.start(resource)
        self.assertIsNone(self.transfer.refused)
        self.transfer.resource_started(answer[-1])
        self.assert_refused(answer[-1])

    def test_out_of_sequence_segments(self):
        cases = {
            "skips one": lambda r: setattr(r, "segment_index", 3),
            "repeats one": lambda r: setattr(r, "segment_index", 1),
            "another answer's original hash": lambda r: setattr(r, "original_hash", os.urandom(32)),
            "metadata flag flipped": lambda r: setattr(r, "has_metadata", True),
        }
        for name, mutate in cases.items():
            with self.subTest(name):
                self.setUp()
                self.hostile(segments(5 * SEGMENT // 2)[:2], mutate)

    def test_starting_past_the_first(self):
        answer = segments(5 * SEGMENT // 2)
        self.transfer.resource_started(answer[1])
        self.assert_refused(answer[1])

    def test_first_segment_must_be_its_own_original(self):
        answer = segments(100)
        answer[0].original_hash = os.urandom(32)  # would append to another transfer's file in RNS's directory
        self.transfer.resource_started(answer[0])
        self.assert_refused(answer[0])

    def test_wrong_totals(self):
        cases = {
            "total grows": lambda r: setattr(r, "total_size", 50 * SEGMENT),
            "total shrinks": lambda r: setattr(r, "total_size", 2 * SEGMENT - 10),
        }
        for name, mutate in cases.items():
            with self.subTest(name):
                self.setUp()
                self.hostile(segments(5 * SEGMENT // 2)[:2], mutate)

    def test_segment_counts_an_honest_sender_never_sends(self):
        # A small advertised total with endless segments: RNS checks max_response_size against each total only
        for total, count in ((100, 2), (100, 10_000), (5 * SEGMENT // 2, 2), (5 * SEGMENT // 2, 4), (SEGMENT, 2)):
            with self.subTest(total=total, count=count):
                self.setUp()
                answer = segments(total)
                answer[0].total_segments = count
                self.transfer.resource_started(answer[0])
                self.assert_refused(answer[0])

    def test_next_segment_before_the_previous_completes(self):
        answer = segments(5 * SEGMENT // 2)
        self.transfer.resource_started(answer[0])  # still TRANSFERRING
        self.transfer.resource_started(answer[1])
        self.assert_refused(answer[1])

    def test_malformed_fields(self):
        answer = segments(100)
        answer[0].total_size = "100"
        self.transfer.resource_started(answer[0])
        self.assert_refused(answer[0])


def compressed_segment(plain: bytes, limit: int, link, storage: str):
    """A receiving Resource holding plain bz2-compressed, as RNS's accept and receive_part leave it before
    assembly (unencrypted: the fake link has no keys), with max_decompressed_size limit."""
    random_hash = os.urandom(RNS.Resource.RANDOM_HASH_SIZE)
    resource = RNS.Resource.__new__(RNS.Resource)
    hash_ = RNS.Identity.full_hash(plain + random_hash)
    vars(resource).update(
        status=RNS.Resource.TRANSFERRING, parts=[random_hash + bz2.compress(plain)], encrypted=False, compressed=True,
        max_decompressed_size=limit, random_hash=random_hash, hash=hash_, original_hash=hash_, has_metadata=False,
        segment_index=1, total_segments=1, storagepath=storage, meta_storagepath=storage + ".meta", link=link,
        callback=mock.Mock(), advertisement_packet=types.SimpleNamespace(plaintext=b""), next_segment=None)
    resource.prove = mock.Mock()
    return resource


class DecompressionTest(_Transfers):
    """RNS's own Resource.assemble, under the bound the fetch's callback sets."""

    def setUp(self):
        super().setUp()
        self.link.resource_concluded = self.transfer.resource_concluded
        self.link.cancel_incoming_resource = mock.Mock()
        self.storage = os.path.join(self.tmp.name, "rns-resource")
        patcher = mock.patch.object(RNS.Resource, "reject", mock.Mock())  # a corrupt Resource's reject packet
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_bomb_segment_is_refused_before_it_is_held(self):
        resource = compressed_segment(bytes(64 * 1024 * 1024), 0, self.link, self.storage)
        resource.total_size = SEGMENT  # advertised as one honest segment
        self.transfer.resource_started(resource)
        self.assertEqual(resource.max_decompressed_size, fetching.SEGMENT_LIMIT)
        resource.assemble()
        self.assertEqual(resource.status, RNS.Resource.CORRUPT)
        self.assertFalse(os.path.exists(self.storage))  # nothing written
        resource.callback.assert_not_called()
        self.link.teardown.assert_called()  # RNS closes the link itself on a corrupt Resource

    def test_an_honest_full_segment_assembles(self):
        plain = bytes(SEGMENT)
        resource = compressed_segment(plain, 0, self.link, self.storage)
        resource.total_size = SEGMENT
        self.transfer.resource_started(resource)
        resource.assemble()
        self.assertEqual(resource.status, RNS.Resource.COMPLETE)
        self.assertIsNone(self.transfer.refused)
        resource.callback.assert_called_once()

    def test_the_process_wide_limit_alone_would_refuse_it(self):
        resource = compressed_segment(bytes(SEGMENT), 0, self.link, self.storage)  # no callback ran
        resource.assemble()
        self.assertEqual(resource.status, RNS.Resource.CORRUPT)


class AssembledSizeTest(_Transfers):
    """What RNS assembles, and a file answer copied, may not outgrow the advertised total."""

    def concluded(self, size: int, total: int, index: int = 1):
        """Transfer.resource_concluded for a complete segment index of an answer of total bytes, RNS's file holding
        size bytes so far."""
        path = os.path.join(self.tmp.name, "rns-file")
        with open(path, "wb") as f:
            f.write(bytes(size))
        resource = types.SimpleNamespace(status=RNS.Resource.COMPLETE, storagepath=path, meta_storagepath=path + ".m",
                                         has_metadata=False, segment_index=index, window=4, eifr=None, size=1,
                                         started_transferring=0)
        self.transfer.total = total
        self.transfer.resource_concluded(resource)

    def test_within_the_total_passes(self):
        self.concluded(SEGMENT, 5 * SEGMENT // 2)
        self.concluded(5 * SEGMENT // 2, 5 * SEGMENT // 2, index=3)
        self.assertIsNone(self.transfer.refused)

    def test_an_oversized_assembly_is_refused(self):
        # A stale file under the same original hash, or segments larger than their share, append past the total
        self.concluded(5 * SEGMENT // 2 + 1, 5 * SEGMENT // 2, index=3)
        self.assert_refused()

    def test_a_file_answer_longer_than_advertised_is_refused(self):
        self.transfer.total = 1000
        receipt = types.SimpleNamespace(response=open(os.path.join(self.tmp.name, "f"), "w+b"),
                                        metadata={"name": b"x"})
        self.addCleanup(receipt.response.close)
        receipt.response.write(bytes(1001))
        receipt.response.seek(0)
        self.transfer.on_response(receipt)
        self.assertIn("larger than", self.transfer.refused)
        with self.assertRaises(NotFound):
            fetching._outcome(fetching_link(), self.transfer, None)

    def test_a_file_answer_within_it_is_copied(self):
        self.transfer.total = 1000
        receipt = types.SimpleNamespace(response=open(os.path.join(self.tmp.name, "f"), "w+b"),
                                        metadata={"name": b"song.mp3"})
        self.addCleanup(receipt.response.close)
        receipt.response.write(b"z" * 990)
        receipt.response.seek(0)
        self.transfer.on_response(receipt)
        snapshot = fetching._outcome(fetching_link(), self.transfer, None)
        self.assertEqual((snapshot.size, snapshot.sent_name, snapshot.file), (990, b"song.mp3", True))
        with open(snapshot.path, "rb") as f:
            self.assertEqual(f.read(), b"z" * 990)

    def test_an_answer_advertised_over_the_bound_is_reported_as_too_large(self):
        packed = umsgpack.packb({"t": 1000, "d": 51 * SEGMENT, "n": 1, "h": bytes(32), "r": bytes(4),
                                 "o": bytes(32), "i": 1, "l": 51, "q": REQUEST_ID, "f": 0x11, "m": bytes(4)})
        self.link.decrypt = lambda data: data
        received = []
        with mock.patch.object(RNS.Link, "receive", lambda link, packet: received.append(packet)):
            packet = types.SimpleNamespace(context=RNS.Packet.RESOURCE_ADV, data=packed)
            self.transfer.receive(packet)
        self.assertEqual(received, [packet])  # RNS rejects it itself (max_response_size)
        with self.assertRaises(TooLarge):
            fetching._outcome(fetching_link(), self.transfer, 50 * SEGMENT)


def fetching_link():
    """The NomadNet link the fake transfers stand for."""
    from rnfs import nomadnet
    return nomadnet.parse("cd" * 16 + ":/file/song.mp3")


class MetadataTest(_Transfers):
    """A file's metadata, checked when its first segment completes, before RNS decodes it."""

    def segment_with_metadata(self, metadata_packed: bytes):
        """A completed first segment whose metadata RNS stored as metadata_packed."""
        meta = os.path.join(self.tmp.name, "rns.meta")
        with open(meta, "wb") as f:
            f.write(metadata_packed)
        return types.SimpleNamespace(status=RNS.Resource.COMPLETE, storagepath=os.path.join(self.tmp.name, "none"),
                                     meta_storagepath=meta, has_metadata=True, segment_index=1, window=4, eifr=None,
                                     size=1, started_transferring=0)

    def test_nomadnets_metadata_passes(self):
        for metadata in ({"name": b"song.mp3"}, {"name": "song.mp3", "size": 5, "x": None}):
            with self.subTest(metadata):
                self.transfer.resource_concluded(self.segment_with_metadata(umsgpack.packb(metadata)))
                self.assertIsNone(self.transfer.refused)

    def test_abusive_metadata_removed_unread(self):
        deep = b"\x91" * 2000 + b"\xc0"  # nested 2000 deep: RNS's decoder would recurse past Python's limit
        cases = {"nested": umsgpack.packb({"name": [[[]]]}), "deep": deep, "list": umsgpack.packb([b"a"] * 10),
                 "large": umsgpack.packb({"name": b"x" * 100_000}),
                 "many": umsgpack.packb({str(i): i for i in range(40)}),
                 "trailing": umsgpack.packb({"name": b"a"}) + b"\x90" * 50}
        for name, packed in cases.items():
            with self.subTest(name):
                self.setUp()
                resource = self.segment_with_metadata(packed)
                self.transfer.resource_concluded(resource)
                self.assert_refused()
                self.assertFalse(os.path.exists(resource.meta_storagepath))  # so RNS finds none to decode

    def test_rns_never_decodes_refused_metadata(self):
        # RNS's own assembly of a file's only segment: it stores the metadata, calls the link's resource_concluded
        # (the check), and only then decodes the metadata for the callback
        self.link.resource_concluded = self.transfer.resource_concluded
        storage = os.path.join(self.tmp.name, "rns-resource")
        hostile = b"\xdc\xff\xff" + b"\x90" * 0xFFFF  # 64 Ki empty lists
        payload = len(hostile).to_bytes(3, "big") + hostile + b"file data"
        resource = compressed_segment(payload, fetching.SEGMENT_LIMIT, self.link, storage)
        resource.has_metadata = True
        unpackb = mock.Mock(wraps=umsgpack.unpackb)
        with mock.patch.object(umsgpack, "unpackb", unpackb):
            resource.assemble()
        self.assertFalse(any(call.args and call.args[0] == hostile for call in unpackb.call_args_list))
        self.assert_refused()


class OutcomeTest(_Transfers):
    """What the fetch reports for answers that bring nothing to add."""

    def test_nothing_sent(self):
        self.transfer.on_response(types.SimpleNamespace(response=None, metadata=None))
        with self.assertRaises(NotFound):
            fetching._outcome(fetching_link(), self.transfer, None)

    def test_no_answer(self):
        with self.assertRaises(NotFound):
            fetching._outcome(fetching_link(), self.transfer, None)


if __name__ == "__main__":
    unittest.main()
