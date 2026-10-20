"""/range (0.2.0): a requester decompresses a /range answer's bz2 bytes itself, so the bytes another node sends can't
be allowed to expand without bound.

The attack: a provider asked for a range of max_bytes bytes, with compression allowed, answers with a bz2 stream that
decompresses to far more (64 MiB of zeros is about 50 bytes of bz2), hoping the requester inflates it into memory before
checking its length; or with a stream that is cut short, followed by more data, or of the wrong length, hoping the
requester keeps what it got. The test rebuilds those answers and gives them to the parser
(protocol.parse_range_response), counting what BZ2Decompressor.decompress may produce; to the response precheck the
guards run before RNS decodes an answer (protocol.response_refusal); and to PeerClient.block_range over a fake link.

The fix: decompress_range asks the decompressor for at most limit + 1 bytes (limit being the range asked for, at most
MAX_BLOCK_SIZE) and refuses a stream that yields more, isn't complete within that, or has anything after it, so a bomb
costs at most max_bytes + 1 bytes of output, at its first call; the parser then checks the length exactly. An answer
failing a check is malformed: the client reports it FAILED with nothing kept.
"""
import bz2
import unittest
from unittest import mock

from rnfs.constants import KIB, MAX_BLOCK_SIZE, RANGE_BZ2
from rnfs.content.cid import RAW, Cid
from rnfs.network import client, links, protocol
from RNS.vendor import umsgpack

import tests.test_links as fakes
from rnfs.config import Config

NODE = bytes(16)
BOMB = bz2.compress(bytes(64 * 1024 * 1024))  # 64 MiB of zeros in about 50 bytes


def framed(response) -> bytes:
    """response as RNS frames it: [request_id, response]."""
    return umsgpack.packb([bytes(16), response])


class DecompressionBoundTest(unittest.TestCase):
    """protocol.decompress_range and parse_range_response bound the output and refuse anything off."""

    def test_bomb_is_refused_after_at_most_limit_plus_one_bytes(self):
        produced = []
        real = bz2.BZ2Decompressor

        class Counting:
            """A BZ2Decompressor recording how much each call may produce and did."""

            def __init__(self):
                self.inner = real()

            def decompress(self, data, max_length=-1):
                out = self.inner.decompress(data, max_length)
                produced.append((max_length, len(out)))
                return out

            def __getattr__(self, name):
                return getattr(self.inner, name)

        with mock.patch.object(protocol.bz2, "BZ2Decompressor", Counting):
            self.assertIsNone(protocol.decompress_range(BOMB, 16 * KIB))
            answer = protocol.encode_range_response(MAX_BLOCK_SIZE, RANGE_BZ2, BOMB)
            self.assertIsNone(protocol.parse_range_response(answer, 0, 16 * KIB, True))
        self.assertTrue(produced)
        # Never asked to produce more than the range plus one byte, and never did: not 64 MiB
        self.assertTrue(all(0 < max_length <= 16 * KIB + 1 for max_length, _ in produced), produced)
        self.assertTrue(all(length <= 16 * KIB + 1 for _, length in produced), produced)
        self.assertEqual(len(produced), 2)  # one call each: refused at the first output that overflows

    def test_streams_cut_short_or_followed_by_data_are_refused(self):
        data = b"text " * 1000
        stream = bz2.compress(data)
        self.assertEqual(protocol.decompress_range(stream, len(data)), data)
        self.assertIsNone(protocol.decompress_range(stream[:-4], len(data)))  # incomplete
        self.assertIsNone(protocol.decompress_range(stream + b"x", len(data)))  # trailing data
        self.assertIsNone(protocol.decompress_range(stream + stream, 2 * len(data)))  # a second stream
        self.assertIsNone(protocol.decompress_range(b"not bz2 at all", 100))  # corrupt
        self.assertIsNone(protocol.decompress_range(stream, len(data) - 1))  # one byte over the limit

    def test_parsed_length_must_be_exactly_the_range(self):
        data = b"abcdefgh" * 512  # 4096 bytes
        for encoding, payload in ((0, data), (RANGE_BZ2, bz2.compress(data))):
            with self.subTest(encoding=encoding):
                answer = protocol.encode_range_response(10_000, encoding, payload)
                self.assertEqual(protocol.parse_range_response(answer, 0, 4096, True), (10_000, data))
                # Shorter than min(max_bytes, size - offset): refused, not kept
                self.assertIsNone(protocol.parse_range_response(answer, 0, 5000, True))
                # Longer than max_bytes: refused
                self.assertIsNone(protocol.parse_range_response(answer, 0, 4000, True))
                # The range must end inside the block
                self.assertIsNone(protocol.parse_range_response(answer, 8000, 4096, True))

    def test_compression_not_asked_for_is_refused(self):
        data = b"z" * 2048
        answer = protocol.encode_range_response(2048, RANGE_BZ2, bz2.compress(data))
        self.assertIsNone(protocol.parse_range_response(answer, 0, 2048, False))
        self.assertIsNone(protocol.parse_range_response(
            protocol.encode_range_response(2048, 7, data), 0, 2048, True))  # an encoding nobody defined

    def test_size_over_the_largest_block_or_against_the_known_size_is_refused(self):
        self.assertIsNone(protocol.parse_range_response(
            protocol.encode_range_response(MAX_BLOCK_SIZE + 1, 0, b"x"), 0, 1, False))
        answer = protocol.encode_range_response(100, 0, b"x" * 10)
        self.assertIsNotNone(protocol.parse_range_response(answer, 0, 10, False, size=100))
        self.assertIsNone(protocol.parse_range_response(answer, 0, 10, False, size=99))  # a wrong total


class PrecheckTest(unittest.TestCase):
    """The guards' precheck accepts a /range answer only in its exact shape, from headers alone."""

    def test_range_answers_pass_and_misshapen_ones_dont(self):
        self.assertIsNone(protocol.response_refusal(framed(protocol.encode_range_response(5, 1, b"abc"))))
        self.assertIsNone(protocol.response_refusal(framed(protocol.encode_range_response(MAX_BLOCK_SIZE, 0,
                                                                                         bytes(MAX_BLOCK_SIZE)))))
        self.assertIsNone(protocol.response_refusal(framed([])))  # not held
        self.assertIsNotNone(protocol.response_refusal(framed([5, 0, bytes(MAX_BLOCK_SIZE + 1)])))
        self.assertIsNotNone(protocol.response_refusal(framed([5, 0, b"abc", b""])))
        self.assertIsNotNone(protocol.response_refusal(framed([-5, 0, b"abc"])))
        self.assertIsNotNone(protocol.response_refusal(framed([5, [], b"abc"])))
        self.assertIsNotNone(protocol.response_refusal(framed([5, 0, b"abc"]) + b"\x00"))


class ClientRefusesBombsTest(unittest.TestCase):
    """PeerClient.block_range fails a bomb or a misshapen answer as malformed, keeping nothing."""

    def setUp(self):
        patcher = mock.patch.object(links, "RNS", fakes.fake_rns())
        patcher.start()
        self.addCleanup(patcher.stop)
        fakes.FakeLink.created = []
        fakes.FakeLink.establish_delay = 0.0
        fakes.FakeLink.response_size = 10
        self.config = Config(stall_timeout=0.5)
        self.pool = links.LinkPool(object(), self.config)
        self.addCleanup(self.pool.close_all)
        self.client = client.PeerClient(self.pool, self.config)

    def test_bomb_fails_the_request(self):
        cid = Cid.for_block(RAW, bytes(MAX_BLOCK_SIZE))
        fakes.FakeLink.responder = staticmethod(lambda link, path, data: protocol.encode_range_response(
            MAX_BLOCK_SIZE, RANGE_BZ2, BOMB))
        result = fakes.run(self, lambda: self.client.block_range(NODE, cid, b"", 16 * KIB, compress=True))
        self.assertEqual((result.status, result.data, result.reason), (client.FAILED, b"", "malformed response"))


if __name__ == "__main__":
    unittest.main()
