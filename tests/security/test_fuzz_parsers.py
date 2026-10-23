"""Property-based fuzzing of the strict parsers that read untrusted bytes (security tooling, phase 2).

Every parser here reads data from other nodes, archives, sealed files or rnfs links (and, for 0.2.0's stores, the
rows a node saved about other nodes). For each, arbitrary input (raw bytes, arbitrary msgpack values, and structured
near-valid values built from the real format) must either parse or fail with the parser's documented outcome (None,
or its own error type), never another exception. Where an encoder exists, decode(encode(x)) == x, and for canonical
formats encode(decode(b)) == b for every accepted b. Bounds are checked where a parser documents one (a /range
answer is never longer than the range asked for, nor decompressed past it), and where two sides must agree they are
run against each other (the /range server's answers through the requester's parser, prechecks against parsers).

Needs hypothesis, which is a development tool and not one of rnfs's dependencies: without it every test here
is skipped, which includes CI (it installs only the locked dependencies). Run it by hand in an environment
that has hypothesis: `python -m unittest tests.security.test_fuzz_parsers`. The default run is small and
deterministic (derandomized); set RNFS_FUZZ_EXAMPLES (e.g. 3000) for a deeper, randomized run.
"""
import bz2
import io
import math
import os
import shutil
import tempfile
import time
import types
import unittest
from unittest import mock

import RNS
from RNS.vendor import umsgpack

from rnfs import nomadnet_links, ranges, statedb
from rnfs.config import Config
from rnfs.constants import ID_LENGTH, K, KIB, MAX_BLOCK_SIZE, RANGE_BZ2, RANGE_PATH
from rnfs.content import archive, builder, dag, seal, walker
from rnfs.content.cid import DAG, RAW, Cid, from_base36, to_base36
from rnfs.content.dag import Child, DirectoryNode, Entry, FileNode, IndexChild, IndexNode
from rnfs.content.storage import Storage
from rnfs.errors import InvalidDag, StorageFull
from rnfs import uris as lxmf
from rnfs.features import keepers
from rnfs.uris import Link
from rnfs.gateway import GatewayError, parse_host, parse_range
from rnfs.names import encode_record, name_for_identity, parse_name, parse_record
from rnfs.dht.firstseen import FirstSeenLog
from rnfs.dht.routing import RoutingTable
from rnfs.dht.scores import SCORE_LIMIT, Scores
from rnfs.network import client as peer_client
from rnfs.network import guards, protocol
from rnfs.network import nomadnet as nomadnet_fetch
from rnfs.network.announces import MODE_CLIENT, MODE_SERVER, parse_mode
from tests.security._server_helpers import TCP_BITRATE, FakeInterface, ServerHarness

try:
    from hypothesis import HealthCheck, example, given, settings
    from hypothesis import strategies as st
except ImportError:  # not a dependency of rnfs: the module still loads, and every test in it is skipped
    HAVE_HYPOTHESIS = False

    # Stands in for hypothesis's API (decorators, strategies and their combinations) when it is missing. No
    # docstring: the skipped tests are instances of it, and unittest would print it as each one's description
    class _Absent:
        def __getattr__(self, name):
            return self

        def __call__(self, *args, **kwargs):
            return self

        def __or__(self, other):
            return self

    st = _Absent()
    example = given = settings = HealthCheck = _Absent()
else:
    HAVE_HYPOTHESIS = True

needs_hypothesis = unittest.skipUnless(HAVE_HYPOTHESIS, "hypothesis isn't installed (pip install hypothesis)")

DEEP = os.environ.get("RNFS_FUZZ_EXAMPLES")
EXAMPLES = int(DEEP) if DEEP else 60


def fuzz(scale: float = 1.0):
    """Settings for one property: EXAMPLES × scale examples, no deadline, no example database written to the
    working directory, and the same examples on every default run so the suite stays deterministic."""
    if not HAVE_HYPOTHESIS:
        return settings()
    return settings(max_examples=max(5, int(EXAMPLES * scale)), deadline=None, database=None,
                    derandomize=not DEEP, suppress_health_check=list(HealthCheck))


_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence RNS's log (storage logs what it rejects)."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


# Strategies

# What RNS's umsgpack can hand a parser: nil, bool, int, float, str, bin, arrays and maps of them
msgpack_leaves = (st.none() | st.booleans() | st.integers(-2**63, 2**64 - 1) | st.floats()
                  | st.binary(max_size=48) | st.text(max_size=24))
msgpack_values = st.recursive(
    msgpack_leaves,
    lambda children: st.lists(children, max_size=6)
    | st.dictionaries(st.text(max_size=8) | st.integers(0, 255), children, max_size=4),
    max_leaves=16)
ids = st.binary(min_size=ID_LENGTH, max_size=ID_LENGTH)
cids = st.builds(Cid.for_block, st.sampled_from([RAW, DAG]), st.binary(max_size=8))
# Binary fields that are almost right: valid CIDs, IDs, and bytes of CID-like lengths with CID-like prefixes
near_binaries = (cids.map(lambda cid: cid.binary) | ids | st.binary(max_size=48)
                 | st.tuples(cids, st.binary(max_size=2)).map(lambda pair: pair[0].binary[:-1] + pair[1]))
near_values = msgpack_leaves | near_binaries | st.lists(near_binaries, max_size=4)
uint64 = st.integers(0, 2**64 - 1)
valid_names = st.text(min_size=1, max_size=12).filter(dag.is_valid_name)


@st.composite
def mutated(draw, data: bytes) -> bytes:
    """data with one byte changed, removed or inserted, or cut short."""
    kind = draw(st.sampled_from(["flip", "delete", "insert", "truncate"]))
    if not data or kind == "insert":
        index = draw(st.integers(0, len(data)))
        return data[:index] + bytes([draw(st.integers(0, 255))]) + data[index:]
    index = draw(st.integers(0, len(data) - 1))
    if kind == "flip":
        return data[:index] + bytes([data[index] ^ draw(st.integers(1, 255))]) + data[index + 1:]
    if kind == "delete":
        return data[:index] + data[index + 1:]
    return data[:index]


def unpacked(data: bytes):
    """(True, value) if data is msgpack umsgpack decodes, else (False, None): wire bytes as RNS hands them on."""
    try:
        return True, umsgpack.unpackb(data)
    except Exception:  # anything umsgpack rejects never reaches a parser
        return False, None


# Network protocol (rnfs.network.protocol)

cid_binaries = cids.map(lambda cid: cid.binary)
protocol_requests = (
    msgpack_values | near_values
    | st.tuples(uint64, st.lists(cid_binaries, max_size=8)).map(list)
    | st.tuples(st.lists(ids, max_size=12), st.lists(ids, max_size=12)).map(list)
    | st.tuples(cid_binaries, st.booleans()).map(list)
    | st.lists(near_values, min_size=2, max_size=2)
    | st.tuples(uint64 | msgpack_leaves, st.lists(near_binaries, max_size=4)).map(list)
    | st.tuples(st.lists(near_binaries, max_size=12), st.lists(near_binaries, max_size=12)).map(list)
    | st.lists(st.none() | near_binaries | st.binary(max_size=MAX_BLOCK_SIZE // 4096), max_size=6))


@needs_hypothesis
class ProtocolFuzzTest(unittest.TestCase):
    """rnfs.network.protocol: every parse_* returns its documented result or None, never raising."""

    def check_all(self, data):
        """Run every request and response parser on data and check the shape of what they accept."""
        key = protocol.parse_find_request(data)
        self.assertTrue(key is None or (type(key) is bytes and len(key) == ID_LENGTH))
        found = protocol.parse_find_response(data)
        if found is not None:
            self.assertTrue(all(len(ids_) <= K and all(protocol.is_id(i) for i in ids_) for ids_ in found))
        provided = protocol.parse_provide_request(data)
        if provided is not None:
            self.assertTrue(all(protocol.is_id(i) for keys in provided for i in keys))
        blocks = protocol.parse_blocks_request(data)
        if blocks is not None:
            self.assertTrue(type(blocks[0]) is int and blocks[0] >= 0)
            self.assertTrue(all(isinstance(cid, Cid) for cid in blocks[1]))
        for requested in (0, 1, 3, 8):
            entries = protocol.parse_blocks_response(data, requested)
            if entries is not None:
                self.assertLessEqual(len(entries), requested)
                self.assertTrue(all(e is None or (type(e) is bytes and len(e) <= MAX_BLOCK_SIZE) for e in entries))
        pin = protocol.parse_pin_request(data)
        if pin is not None:
            self.assertIsInstance(pin[0], Cid)
            self.assertIs(type(pin[1]), bool)
        self.assertIs(type(protocol.is_accepted(data)), bool)
        cid = protocol.parse_cid(data)
        self.assertTrue(cid is None or cid.binary == data)

    @fuzz()
    @given(protocol_requests)
    def test_any_value(self, data):
        self.check_all(data)

    @fuzz()
    @given(st.binary(max_size=96) | protocol_requests.map(umsgpack.packb).flatmap(mutated))
    def test_wire_bytes(self, data):
        ok, value = unpacked(data)
        if ok:
            self.check_all(value)

    @fuzz(0.5)
    @given(ids, st.lists(ids, max_size=K), st.lists(ids, max_size=K), uint64, st.lists(cids, max_size=8, unique=True),
           st.lists(st.none() | st.binary(max_size=64), max_size=8), cids, st.booleans())
    def test_round_trips(self, key, providers, nodes, max_bytes, wanted, entries, cid, pin):
        def wire(value):
            """value as it arrives: packed by the sender, unpacked by RNS."""
            return umsgpack.unpackb(umsgpack.packb(value))

        self.assertEqual(protocol.parse_find_request(wire(protocol.encode_find(key))), key)
        self.assertEqual(protocol.parse_find_response(wire(protocol.encode_find_response(providers, nodes))),
                         (providers, nodes))
        self.assertEqual(protocol.parse_provide_request(wire(protocol.encode_provide(providers, nodes))),
                         (providers, nodes))
        self.assertEqual(protocol.parse_blocks_request(wire(protocol.encode_blocks(max_bytes, wanted))),
                         (max_bytes, wanted))
        if wanted:  # a CID asked for twice is malformed (parse_blocks_request): requesters drop duplicates
            repeated = protocol.encode_blocks(max_bytes, wanted + wanted[:1])
            self.assertIsNone(protocol.parse_blocks_request(wire(repeated)))
        self.assertEqual(protocol.parse_pin_request(wire(protocol.encode_pin(cid, pin))), (cid, pin))
        # As many entries as CIDs requested is always well-formed
        self.assertEqual(protocol.parse_blocks_response(wire(protocol.encode_blocks_response(entries)), len(entries)),
                         entries)


# /range (rnfs.network.protocol, rnfs 0.2.0)

def wire(value):
    """value as it arrives: packed by the sender, unpacked by RNS."""
    return umsgpack.unpackb(umsgpack.packb(value))


# Blocks that compress well, badly or not at all, up to MAX_BLOCK_SIZE
range_blocks = (st.binary(max_size=600)
                | st.tuples(st.binary(min_size=1, max_size=40), st.integers(0, MAX_BLOCK_SIZE), st.binary(max_size=8))
                .map(lambda t: (t[0] * (t[1] // len(t[0]) + 1))[:t[1]] + t[2])
                .map(lambda block: block[:MAX_BLOCK_SIZE]))
range_fields = st.integers(-2, 700) | st.integers(MAX_BLOCK_SIZE - 1, MAX_BLOCK_SIZE + 1) | uint64 | msgpack_leaves
range_requests = (msgpack_values | near_values
                  | st.tuples(cid_binaries | near_binaries, range_fields, range_fields, range_fields).map(list)
                  | st.lists(cid_binaries | range_fields, max_size=6))
bz2_streams = st.binary(max_size=400).map(protocol.compress_range)
range_payloads = (st.binary(max_size=300) | bz2_streams | bz2_streams.flatmap(mutated)
                  | st.tuples(bz2_streams, bz2_streams | st.binary(min_size=1, max_size=4)).map(b"".join))
range_answers = (msgpack_values | st.just([]) | st.just(False)
                 | st.tuples(st.integers(0, 600) | st.integers(MAX_BLOCK_SIZE - 1, MAX_BLOCK_SIZE + 1) | uint64
                             | msgpack_leaves, st.integers(0, 3) | msgpack_leaves,
                             range_payloads | msgpack_leaves).map(list))
offsets = st.integers(0, 700) | st.integers(0, MAX_BLOCK_SIZE)
lengths = st.integers(1, 700) | st.integers(1, MAX_BLOCK_SIZE)


_bombs: list[bytes] = []


def bz2_bombs() -> list[bytes]:
    """bz2 bombs: streams of a few dozen bytes to a few KiB expanding past any range (the largest to 16 MiB)."""
    if not _bombs:
        _bombs.extend(protocol.compress_range(data) for data in (
            bytes(MAX_BLOCK_SIZE + 1), b"A" * (2 * MAX_BLOCK_SIZE), bytes(16 * 1024 * 1024)))
    return _bombs


def counting_decompressor(produced: list):
    """A BZ2Decompressor class recording, in produced, the max_length of each call and the bytes it gave."""
    real = bz2.BZ2Decompressor

    class Counting:
        """Records each decompress call."""

        def __init__(self):
            self.inner = real()

        def decompress(self, data, max_length=-1):
            out = self.inner.decompress(data, max_length)
            produced.append((max_length, len(out)))
            return out

        def __getattr__(self, name):
            return getattr(self.inner, name)

    return Counting


@needs_hypothesis
class RangeFuzzTest(unittest.TestCase):
    """/range: parse_range_request within its limits, parse_range_response within the range asked for, and
    decompress_range never producing more than its limit (plus the one byte that tells it is over)."""

    def check_request(self, data):
        """parse_range_request never raises, and what it accepts is within its limits and canonical."""
        parsed = protocol.parse_range_request(data)
        if parsed is None:
            return
        cid, offset, max_bytes, flags = parsed
        self.assertIsInstance(cid, Cid)
        self.assertTrue(all(type(field) is int for field in (offset, max_bytes, flags)))
        self.assertTrue(0 <= offset <= MAX_BLOCK_SIZE and 1 <= max_bytes <= MAX_BLOCK_SIZE and flags >= 0)
        self.assertEqual(protocol.encode_range(cid, offset, max_bytes, flags), data)

    @fuzz()
    @given(range_requests)
    def test_request_any_value(self, data):
        self.check_request(data)

    @fuzz()
    @given(st.binary(max_size=64) | range_requests.map(umsgpack.packb).flatmap(mutated))
    def test_request_wire_bytes(self, data):
        ok, value = unpacked(data)
        if ok:
            self.check_request(value)

    @fuzz()
    @given(cids, st.integers(-3, MAX_BLOCK_SIZE + 3) | st.integers(-2**63, 2**64 - 1),
           st.integers(-3, MAX_BLOCK_SIZE + 3) | st.integers(-2**63, 2**64 - 1), st.integers(-3, 2**64 - 1))
    def test_request_limits_and_round_trip(self, cid, offset, max_bytes, flags):
        # Accepted exactly when within the documented limits; reserved flag bits are kept, never refused
        valid = 0 <= offset <= MAX_BLOCK_SIZE and 1 <= max_bytes <= MAX_BLOCK_SIZE and flags >= 0
        self.assertEqual(protocol.parse_range_request(wire(protocol.encode_range(cid, offset, max_bytes, flags))),
                         (cid, offset, max_bytes, flags) if valid else None)

    def check_answer(self, data, offset: int, max_bytes: int, compressed: bool, size):
        """parse_range_response and is_range_unserved never raise; an accepted answer is exactly the range asked for,
        within MAX_BLOCK_SIZE, and passes the precheck run before RNS decodes it."""
        unserved = protocol.is_range_unserved(data)
        self.assertIs(unserved, type(data) is list and not data)
        parsed = protocol.parse_range_response(data, offset, max_bytes, compressed, size)
        if parsed is None:
            return
        self.assertFalse(unserved)
        total, raw = parsed
        self.assertTrue(type(total) is int and offset <= total <= MAX_BLOCK_SIZE)
        self.assertTrue(size is None or total == size)
        self.assertIs(type(raw), bytes)
        self.assertEqual(len(raw), protocol.range_length(total, offset, max_bytes))
        self.assertLessEqual(len(raw), max_bytes)
        encoding, payload = data[1], data[2]
        if encoding == RANGE_BZ2:
            self.assertTrue(compressed)  # never accepted compressed unless asked for
            self.assertEqual(bz2.decompress(payload), raw)
        else:
            self.assertEqual((encoding, payload), (protocol.RANGE_RAW, raw))
        self.assertIsNone(protocol.body_refusal(umsgpack.packb(data)))

    @fuzz()
    @given(range_answers, offsets, lengths, st.booleans(), st.none() | st.integers(0, 600) | st.just(MAX_BLOCK_SIZE))
    def test_answer_any_value(self, data, offset, max_bytes, compressed, size):
        self.check_answer(data, offset, max_bytes, compressed, size)

    @fuzz()
    @given(st.binary(max_size=96) | range_answers.map(umsgpack.packb).flatmap(mutated), offsets, lengths,
           st.booleans(), st.none() | st.integers(0, 600))
    def test_answer_wire_bytes(self, data, offset, max_bytes, compressed, size):
        ok, value = unpacked(data)
        if ok:
            self.check_answer(value, offset, max_bytes, compressed, size)

    @fuzz()
    @given(range_blocks, st.integers(0, 2000) | offsets, lengths, st.booleans(), st.booleans())
    def test_answer_round_trip(self, block, offset, max_bytes, compressed, use_bz2):
        data = block[offset:offset + protocol.range_length(len(block), offset, max_bytes)]
        encoding = RANGE_BZ2 if use_bz2 and compressed else protocol.RANGE_RAW
        payload = protocol.compress_range(data) if encoding == RANGE_BZ2 else data
        answer = wire(protocol.encode_range_response(len(block), encoding, payload))
        expected = (len(block), data) if offset <= len(block) else None
        self.assertEqual(protocol.parse_range_response(answer, offset, max_bytes, compressed), expected)
        self.assertEqual(protocol.parse_range_response(answer, offset, max_bytes, compressed, len(block)), expected)
        self.assertIsNone(protocol.parse_range_response(answer, offset, max_bytes, compressed, len(block) + 1))
        if use_bz2 and not compressed:  # compressed when not asked for: refused
            bz2_answer = wire(protocol.encode_range_response(len(block), RANGE_BZ2, protocol.compress_range(data)))
            self.assertIsNone(protocol.parse_range_response(bz2_answer, offset, max_bytes, False))
        if expected is not None:
            self.assertIsNone(protocol.body_refusal(umsgpack.packb(answer)))
        if expected is not None and len(payload) <= len(data):  # as a responder sends it: compressed only if smaller
            self.assertLessEqual(protocol.packed_response_size(bytes(protocol.REQUEST_ID_LENGTH), answer),
                                 protocol.range_max_response_size(max_bytes))

    @fuzz()
    @given(range_payloads | st.binary(max_size=200), st.integers(0, 1000))
    def test_decompress_any_stream(self, stream, limit):
        data = protocol.decompress_range(stream, limit)
        if data is not None:
            self.assertLessEqual(len(data), limit)
            self.assertEqual(bz2.decompress(stream), data)  # all of it, one complete stream and nothing after

    @fuzz()
    @given(st.binary(max_size=400) | range_blocks, st.integers(0, 1000) | st.integers(0, MAX_BLOCK_SIZE),
           st.binary(min_size=1, max_size=8), st.data())
    def test_decompress_one_complete_stream_only(self, data, limit, extra, choices):
        stream = protocol.compress_range(data)
        self.assertEqual(protocol.decompress_range(stream, limit), data if len(data) <= limit else None)
        self.assertEqual(protocol.decompress_range(stream, len(data)), data)
        cut = choices.draw(st.integers(0, len(stream) - 1))
        self.assertIsNone(protocol.decompress_range(stream[:cut], MAX_BLOCK_SIZE))  # cut short
        self.assertIsNone(protocol.decompress_range(stream + extra, MAX_BLOCK_SIZE))  # anything after it
        self.assertIsNone(protocol.decompress_range(stream + stream, 2 * MAX_BLOCK_SIZE))  # a second stream

    @fuzz(0.5)
    @given(st.integers(0, 2), st.integers(0, MAX_BLOCK_SIZE))
    def test_bombs_are_refused_within_the_limit(self, which, limit):
        produced = []
        with mock.patch.object(protocol.bz2, "BZ2Decompressor", counting_decompressor(produced)):
            self.assertIsNone(protocol.decompress_range(bz2_bombs()[which], limit))
            answer = protocol.encode_range_response(MAX_BLOCK_SIZE, RANGE_BZ2, bz2_bombs()[which])
            self.assertIsNone(protocol.parse_range_response(answer, MAX_BLOCK_SIZE - max(1, limit), max(1, limit),
                                                            True))
        # Never asked for, nor given, more than the limit and the one byte telling it is over
        self.assertTrue(produced)
        self.assertTrue(all(0 < max_length <= max(limit, 1) + 1 and length <= max_length
                            for max_length, length in produced), produced)


# Packed answers checked before RNS decodes them (rnfs.network.protocol, guards)

def is_dht_response(value) -> bool:
    """True if value is shaped as an rnfs.dht response body_refusal may let through: True, False, None, a /find
    answer, a /blocks (or /name, or empty /range) answer, or a /range answer."""
    if value is None or type(value) is bool:
        return True
    if type(value) is not list:
        return False
    if len(value) == 2 and all(protocol.is_id_list(ids_, K) for ids_ in value):
        return True
    if len(value) <= protocol.MAX_BLOCKS_CIDS and all(
            entry is None or (type(entry) is bytes and len(entry) <= MAX_BLOCK_SIZE) for entry in value):
        return True
    return (len(value) == 3 and protocol.is_count(value[0]) and protocol.is_count(value[1])
            and type(value[2]) is bytes and len(value[2]) <= MAX_BLOCK_SIZE)


blocks_entries = (st.none() | st.binary(max_size=64)
                  | st.sampled_from([bytes(MAX_BLOCK_SIZE), bytes(MAX_BLOCK_SIZE + 1)]))
honest_dht_responses = (
    st.sampled_from([True, False, None, []])
    | st.builds(protocol.encode_find_response, st.lists(ids, max_size=K), st.lists(ids, max_size=K))
    | st.lists(st.none() | st.binary(max_size=64), max_size=protocol.MAX_BLOCKS_CIDS).map(
        protocol.encode_blocks_response)
    | st.builds(protocol.encode_range_response, uint64, uint64, st.binary(max_size=300)))
dht_bodies = (st.binary(max_size=96)
              | (honest_dht_responses | range_answers | protocol_requests
                 | st.lists(blocks_entries, max_size=protocol.MAX_BLOCKS_CIDS + 2)).map(umsgpack.packb)
              | (honest_dht_responses | range_answers).map(umsgpack.packb).flatmap(mutated))
pages = (st.none() | st.just(False) | st.just(True) | st.binary(max_size=200) | st.text(max_size=40)
         | st.integers(0, 70_000).map(bytes) | msgpack_values)
page_answers = (st.tuples(ids | st.binary(max_size=20) | msgpack_leaves, pages).map(list) | msgpack_values).map(
    umsgpack.packb)
page_bodies = (st.binary(max_size=64) | page_answers | page_answers.flatmap(mutated)
               # The longest headers a page answer can have: an array32 holding a bin32 request ID
               | st.binary(max_size=40).map(
                   lambda junk: b"\xdd\x00\x00\x00\x02\xc6\x00\x00\x00\x10" + bytes(16) + junk))
metadata_values = (st.none() | st.booleans() | st.integers(-2**63, 2**64 - 1) | st.binary(max_size=40)
                   | st.text(max_size=40) | msgpack_values)
metadata_maps = st.dictionaries(st.text(max_size=8) | st.binary(max_size=8), metadata_values, max_size=18)
metadata_bodies = (st.binary(max_size=64) | metadata_maps.map(umsgpack.packb)
                   | metadata_maps.map(umsgpack.packb).flatmap(mutated)
                   | st.integers(0, 5000).map(lambda n: umsgpack.packb({"name": "x" * n}))
                   | msgpack_values.map(umsgpack.packb))


@needs_hypothesis
class PrecheckFuzzTest(unittest.TestCase):
    """body_refusal, response_refusal, page_response_refusal (and guards' use of it) and metadata_refusal: a reason
    or None, never raising; what they let through decodes to the flat, bounded shapes they document, and every
    honest answer is let through."""

    def check_body(self, packed: bytes):
        """body_refusal agrees with response_refusal on the framed answer, and lets through only decodable
        rnfs.dht response shapes."""
        reason = protocol.body_refusal(packed)
        self.assertTrue(reason is None or type(reason) is str)
        request_id = bytes(range(protocol.REQUEST_ID_LENGTH))
        framed = b"\x92\xc4" + bytes([len(request_id)]) + request_id + packed
        self.assertEqual(protocol.packed_request_id(framed), request_id)
        framed_reason = protocol.response_refusal(framed)
        if packed:
            self.assertEqual(framed_reason, reason)
        else:
            self.assertIsNotNone(framed_reason)
        if reason is None:
            self.assertTrue(is_dht_response(umsgpack.unpackb(packed)))

    @fuzz()
    @given(dht_bodies)
    def test_dht_body(self, packed):
        self.check_body(packed)

    @fuzz()
    @given(honest_dht_responses
           | st.lists(st.none() | st.sampled_from([bytes(MAX_BLOCK_SIZE)]), max_size=protocol.MAX_BLOCKS_CIDS))
    def test_honest_dht_answers_pass(self, response):
        packed = umsgpack.packb(response)
        self.assertIsNone(protocol.body_refusal(packed))
        self.assertIsNone(protocol.response_refusal(umsgpack.packb([bytes(protocol.REQUEST_ID_LENGTH), response])))

    @fuzz()
    @given(st.binary(max_size=200) | page_bodies)
    def test_response_framing(self, framed):
        reason = protocol.response_refusal(framed)
        self.assertTrue(reason is None or type(reason) is str)
        request_id = protocol.packed_request_id(framed)
        self.assertTrue(request_id is None or len(request_id) == protocol.REQUEST_ID_LENGTH)
        if reason is None:
            value = umsgpack.unpackb(framed)
            self.assertTrue(type(value) is list and len(value) == 2 and value[0] == request_id)
            self.assertTrue(is_dht_response(value[1]))

    def check_page(self, packed: bytes):
        """page_response_refusal decides from the first PAGE_HEAD_SIZE bytes as from all of them (and guards reads
        only those), and lets through only [request ID, one flat string or none]."""
        reason = protocol.page_response_refusal(packed, len(packed))
        self.assertTrue(reason is None or type(reason) is str)
        self.assertEqual(protocol.page_response_refusal(packed[:protocol.PAGE_HEAD_SIZE], len(packed)), reason)
        resource = types.SimpleNamespace(has_metadata=False, data=io.BytesIO(packed))
        self.assertEqual(guards._page_refusal(resource), reason)
        if reason is not None:
            return
        try:
            value = umsgpack.unpackb(packed)
        except umsgpack.InvalidStringException:  # a str page that isn't UTF-8: RNS fails to decode one flat string
            return
        self.assertTrue(type(value) is list and len(value) == 2)
        self.assertTrue(type(value[0]) is bytes and len(value[0]) == protocol.REQUEST_ID_LENGTH)
        self.assertTrue(value[1] is None or value[1] is False or type(value[1]) in (bytes, str))

    @fuzz()
    @given(page_bodies | dht_bodies)
    def test_page_answer(self, packed):
        self.check_page(packed)

    @fuzz()
    @given(st.binary(min_size=16, max_size=16), st.none() | st.just(False) | st.binary(max_size=300)
           | st.text(max_size=60) | st.integers(0, 70_000).map(bytes))
    def test_honest_pages_pass(self, request_id, page):
        packed = umsgpack.packb([request_id, page])
        self.assertIsNone(protocol.page_response_refusal(packed, len(packed)))
        self.assertIsNone(protocol.page_response_refusal(packed[:protocol.PAGE_HEAD_SIZE], len(packed)))

    def check_metadata(self, packed: bytes):
        """metadata_refusal lets through only a small flat map of names, numbers and flags."""
        reason = protocol.metadata_refusal(packed)
        self.assertTrue(reason is None or type(reason) is str)
        if reason is not None:
            return
        self.assertLessEqual(len(packed), protocol.MAX_METADATA_SIZE)
        try:
            value = umsgpack.unpackb(packed)
        except (umsgpack.InvalidStringException, umsgpack.DuplicateKeyException):
            return  # RNS fails to decode it, having built a few strings at most
        self.assertTrue(type(value) is dict and len(value) <= protocol.MAX_METADATA_ENTRIES)
        self.assertTrue(all(type(key) in (bytes, str) for key in value))
        self.assertTrue(all(item is None or type(item) in (bool, int, bytes, str) for item in value.values()))
        # What a fetch makes of it: a valid, printable entry name whatever the node sent
        sent = nomadnet_fetch._sent_name(value)
        self.assertTrue(sent is None or type(sent) in (bytes, str))
        name = nomadnet_links.snapshot_name(nomadnet_links.parse("00" * 16 + ":/file/fallback"), sent)
        self.assertTrue(dag.is_valid_name(name) and name.isprintable())

    @fuzz()
    @given(metadata_bodies)
    def test_metadata(self, packed):
        self.check_metadata(packed)

    @fuzz()
    @given(valid_names | st.binary(min_size=1, max_size=255))
    def test_honest_metadata_passes(self, name):
        self.assertIsNone(protocol.metadata_refusal(umsgpack.packb({"name": name})))
        self.assertIsNone(protocol.metadata_refusal(umsgpack.packb({b"name": name})))


# DAG nodes (rnfs.content.dag)

@st.composite
def dag_nodes(draw):
    """A valid DAG node of any type, with sizes that can't overflow 2^64 - 1."""
    node_type = draw(st.sampled_from([dag.FILE, dag.DIRECTORY, dag.INDEX]))
    sizes = st.integers(0, 2**60)
    if node_type == dag.FILE:
        return FileNode([Child(cid, size) for cid, size in draw(st.lists(st.tuples(cids, sizes), max_size=8))])
    names = sorted(draw(st.sets(valid_names, max_size=8)))
    if node_type == dag.DIRECTORY:
        kinds = st.sampled_from(dag.KINDS)
        return DirectoryNode([Entry(name, draw(cids), draw(sizes), draw(kinds)) for name in names])
    return IndexNode([IndexChild(name, draw(cids), draw(sizes)) for name in names])


# Near-valid node structures: right shape, any field values
dag_fields = near_values | valid_names | uint64
dag_structures = st.tuples(
    st.integers(0, 4) | msgpack_leaves,
    st.lists(st.lists(dag_fields, min_size=2, max_size=4), max_size=4) | msgpack_values).map(list)


@needs_hypothesis
class DagFuzzTest(unittest.TestCase):
    """dag.decode: a node or InvalidDag; accepted blocks are exactly what encode writes."""

    def check(self, block: bytes):
        """decode(block) raises only InvalidDag, and anything it accepts re-encodes to block."""
        try:
            node = dag.decode(block)
        except InvalidDag:
            return
        self.assertEqual(dag.encode(node), block)

    @fuzz()
    @given(st.binary(max_size=128))
    def test_any_bytes(self, block):
        self.check(block)

    @fuzz()
    @given(dag_structures.map(umsgpack.packb) | dag_nodes().map(dag.encode).flatmap(mutated))
    def test_structured_and_mutated(self, block):
        self.check(block)

    @fuzz()
    @given(dag_nodes())
    def test_round_trip(self, node):
        self.assertEqual(dag.decode(dag.encode(node)), node)

    @fuzz(0.5)
    @given(dag_nodes())
    def test_non_shortest_headers_rejected(self, node):
        block = dag.encode(node)
        # The outer fixarray of 2 written as an array16: same value, not the encoding writers produce
        self.assertEqual(block[0], 0x92)
        with self.assertRaises(InvalidDag):
            dag.decode(b"\xdc\x00\x02" + block[1:])

    def test_huge_counts_fail_fast(self):
        start = time.monotonic()
        for block in (b"\x92\x01\xdd\xff\xff\xff\xff", b"\x92\x02\xdd\xff\xff\xff\xff" + b"\x94" * 1000,
                      b"\x92\x01\xdd\xff\xff\xff\xff" + b"\x92\xc4\x24" + bytes(36) + b"\x00" * 1000):
            with self.assertRaises(InvalidDag):
                dag.decode(block)
        self.assertLess(time.monotonic() - start, 1.0)


# Name records (rnfs.names)

@needs_hypothesis
class NameRecordFuzzTest(unittest.TestCase):
    """parse_record: (sequence, CID) or None, with one encoding per record; parse_name: 16 bytes or ValueError."""

    def check(self, app_data):
        """parse_record never raises, and anything it accepts is the canonical encoding."""
        record = parse_record(app_data)
        if record is not None:
            sequence, cid = record
            self.assertTrue(type(sequence) is int and 0 <= sequence < 2**64)
            self.assertEqual(encode_record(sequence, cid), bytes(app_data))

    @fuzz()
    @given(st.binary(max_size=80) | msgpack_values)
    def test_any_value(self, app_data):
        self.check(app_data)

    @fuzz()
    @given(st.tuples(uint64 | msgpack_leaves, near_binaries).map(list).map(umsgpack.packb)
           | st.builds(encode_record, uint64, cids).flatmap(mutated))
    def test_structured_and_mutated(self, app_data):
        self.check(app_data)

    @fuzz()
    @given(uint64, cids)
    def test_round_trip(self, sequence, cid):
        self.assertEqual(parse_record(encode_record(sequence, cid)), (sequence, cid))

    @fuzz()
    @given(st.text(max_size=40) | ids.map(bytes.hex)
           | st.tuples(st.sampled_from(["", " ", "<", "\t<"]), ids.map(bytes.hex),
                       st.sampled_from(["", " ", ">", ">\n"])).map("".join))
    def test_parse_name(self, text):
        # Whitespace inside the hex digits is T1 (tests/security/test_t1_parse_name_whitespace.py); this checks
        # the error type for every input and the length for the rest
        try:
            name = parse_name(text)
        except ValueError:
            return
        if not any(c.isspace() for c in text.strip()):
            self.assertEqual(len(name), 16)


# Keepers' signed records (rnfs.features.keepers)

@needs_hypothesis
class KeepersFuzzTest(unittest.TestCase):
    """parse_signed and the /name messages: a verified SignedRecord or None, never raising."""

    @classmethod
    def setUpClass(cls):
        """Two name keys (making identities is the slow part)."""
        cls.identities = [RNS.Identity(), RNS.Identity()]
        cls.names = [name_for_identity(identity) for identity in cls.identities]

    def check(self, name, fields):
        """Every entry point for untrusted records returns a SignedRecord for name, or None."""
        for record in (keepers.parse_signed(name, fields), keepers.parse_response(name, fields)):
            self.assertTrue(record is None or (isinstance(record, keepers.SignedRecord) and record.name == name))
        if type(fields) is bytes:
            for has_ratchet in (False, True):
                record = keepers.from_announce_data(name, fields, has_ratchet)
                self.assertTrue(record is None or record.name == name)
        request = keepers.parse_request(fields)
        self.assertTrue(request is None or len(request) == ID_LENGTH)

    @fuzz()
    @given(st.sampled_from([0, 1]), msgpack_values | st.lists(st.binary(max_size=80), min_size=6, max_size=6)
           | st.binary(max_size=300))
    def test_any_value(self, which, fields):
        self.check(self.names[which], fields)
        self.check(bytes(16), fields)

    @fuzz(0.5)
    @given(st.sampled_from([0, 1]), uint64, cids, st.integers(0, 2**40 - 1), st.data())
    def test_round_trip_and_tampering(self, which, sequence, cid, now, data):
        identity, name = self.identities[which], self.names[which]
        record = keepers.sign_record(identity, name, encode_record(sequence, cid), now)
        wire = umsgpack.unpackb(umsgpack.packb(keepers.encode_response(record)))
        self.assertEqual(keepers.parse_response(name, wire), record)
        self.assertEqual(keepers.parse_signed(name, record.wire()), record)
        self.assertIsNone(keepers.parse_signed(self.names[1 - which], record.wire()))
        announce = record.public_key + keepers.NAME_HASH + record.random_hash + record.signature + record.app_data
        self.assertEqual(keepers.from_announce_data(name, announce, False), record)
        # Any change to any field makes it a different record, which must not verify
        fields = record.wire()
        index = data.draw(st.integers(0, len(fields) - 1))
        fields[index] = data.draw(mutated(fields[index]))
        self.assertIsNone(keepers.parse_signed(name, fields))
        self.assertIsNone(keepers.from_announce_data(name, data.draw(mutated(announce)), False))


# Archives (rnfs.content.archive)

def archive_header(roots: list) -> bytes:
    """An archive's header listing roots (binary CIDs)."""
    return umsgpack.packb(["rnfs-archive", 1, roots])


def raw_archive(blocks: list[bytes]) -> bytes:
    """An archive of raw blocks, the first (if any) as its root."""
    binaries = [Cid.for_block(RAW, block).binary for block in blocks]
    return archive_header(binaries[:1]) + b"".join(umsgpack.packb(list(pair)) for pair in zip(binaries, blocks))


archive_streams = (
    st.binary(max_size=200)
    | st.lists(st.binary(max_size=64), max_size=4).map(raw_archive)
    | st.lists(st.binary(max_size=64), max_size=4).map(raw_archive).flatmap(mutated)
    | st.tuples(st.lists(near_binaries, max_size=3),
                st.lists(st.tuples(near_binaries, st.binary(max_size=64)).map(list) | msgpack_values, max_size=4))
    .map(lambda pair: archive_header(pair[0]) + b"".join(umsgpack.packb(record) for record in pair[1])))


@needs_hypothesis
class ArchiveFuzzTest(unittest.TestCase):
    """read_archive and import_archive: ValueError (or StorageFull) for anything malformed, never another error;
    a root is pinned only when every block of it came verified from the archive."""

    def setUp(self):
        """A storage to import into (shared by a test's examples: imports must hold whatever is already stored)."""
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.storage = Storage(os.path.join(self.tmp, "blocks"), os.path.join(self.tmp, "state"), 10**9)
        self.addCleanup(self.storage.close)

    def read(self, data: bytes) -> list | None:
        """Every record of the archive in data, or None if it is rejected (ValueError)."""
        try:
            roots, records = archive.read_archive(io.BytesIO(data))
            return [roots, list(records)]
        except ValueError:
            return None

    def check_import(self, data: bytes):
        """Import data; complete roots must have every DAG block in the archive, with a matching CID."""
        parsed = self.read(data)
        try:
            result = archive.import_archive(io.BytesIO(data), self.storage)
        except (ValueError, StorageFull):
            return
        verified = {cid for cid, block in (parsed[1] if parsed else []) if cid.verify(block)}
        for root in result["complete"]:
            self.assertLessEqual(self.storage.dag_blocks(root), verified)
            self.assertTrue(self.storage.is_pinned(root))

    @fuzz()
    @given(archive_streams)
    def test_read_any_stream(self, data):
        self.read(data)

    @fuzz(0.5)
    @given(archive_streams)
    def test_import_any_stream(self, data):
        self.check_import(data)

    @fuzz(0.25)
    @given(st.lists(st.tuples(valid_names, st.binary(max_size=3000)), min_size=1, max_size=3,
                    unique_by=lambda entry: entry[0]), st.data())
    def test_round_trip_and_mutations(self, files, data):
        source = Storage(os.path.join(self.tmp, "source"), os.path.join(self.tmp, "source-state"), 10**9)
        self.addCleanup(source.close)
        entries = sorted((Entry(name, builder.build_bytes(content, source.put).cid, len(content), 0)
                          for name, content in files), key=lambda entry: entry.name)
        root = builder.build_directory(entries, source.put).cid
        out = io.BytesIO()
        archive.export_archive([root], source.getter(), out)
        exported = out.getvalue()
        roots, records = self.read(exported)
        self.assertEqual(roots, [root])
        self.assertEqual({cid for cid, _ in records}, set(source.dag_blocks(root)))
        self.check_import(data.draw(mutated(exported)))
        self.assertEqual(archive.import_archive(io.BytesIO(exported), self.storage)["complete"], [root])


# The LXMF link-map field and rnfs: URIs (rnfs.uris)

links = st.builds(
    Link, st.none() | cids, st.none() | ids, st.lists(valid_names, max_size=3).map(tuple),
    st.none() | uint64, st.none() | ids).filter(lambda link: link.cid is not None or link.name is not None)
link_maps = st.dictionaries(st.sampled_from(["cid", "name", "path", "size", "from", "x"]) | st.text(max_size=4),
                            near_values | uint64 | st.lists(valid_names | msgpack_leaves, max_size=3), max_size=6)
lxmf_fields = (msgpack_values
               | st.lists(link_maps, max_size=9).map(lambda data: {lxmf.FIELD_CUSTOM_TYPE: lxmf.CUSTOM_TYPE,
                                                                   lxmf.FIELD_CUSTOM_DATA: data})
               | st.lists(links, min_size=1, max_size=8).map(lxmf.encode_field))
uri_segments = st.text(alphabet="abcAZ09-._~%2Fe/?#&=: é", max_size=10)
uris = (st.text(max_size=60)
        | st.tuples(st.sampled_from(["rnfs:", "RNFS:", "rnfs:name/", ""]), cids.map(str) | ids.map(bytes.hex),
                    st.lists(uri_segments, max_size=4).map("/".join), st.sampled_from(["", "/", "?", "?from=", "#"]),
                    st.sampled_from(["", "0" * 32, "x&from=" + "1" * 32]))
        .map(lambda parts: parts[0] + parts[1] + ("/" + parts[2] if parts[2] else "") + parts[3] + parts[4])
        | links.map(lxmf.format_uri))


@needs_hypothesis
class LinkFuzzTest(unittest.TestCase):
    """decode_field and parse_uri: links or ValueError."""

    @fuzz()
    @given(lxmf_fields)
    def test_decode_field(self, fields):
        for value in (fields, umsgpack.unpackb(umsgpack.packb(fields))):
            try:
                decoded = lxmf.decode_field(value)
            except ValueError:
                continue
            if decoded is not None:
                self.assertTrue(1 <= len(decoded) <= lxmf.MAX_LINKS and all(isinstance(li, Link) for li in decoded))

    @fuzz()
    @given(st.lists(links, min_size=1, max_size=lxmf.MAX_LINKS))
    def test_field_round_trip(self, sent):
        fields = umsgpack.unpackb(umsgpack.packb(lxmf.encode_field(sent)))
        self.assertEqual(lxmf.decode_field(fields), sent)

    @fuzz()
    @given(uris)
    def test_parse_uri(self, text):
        try:
            link = lxmf.parse_uri(text)
        except ValueError:
            return
        # Whatever is accepted has a canonical form that reads back as the same link
        self.assertEqual(lxmf.parse_uri(lxmf.format_uri(link)), link)

    @fuzz()
    @given(links)
    def test_uri_round_trip(self, link):
        bare = Link(link.cid if link.name is None else None, link.name, link.path, None, link.from_node)
        self.assertEqual(lxmf.parse_uri(lxmf.format_uri(bare)), bare)


# Sealed files (rnfs.content.seal)

def raw_reader(data: bytes):
    """A RawReader over data held in memory."""
    def raw(offset: int, length: int):
        yield data[offset:offset + length]
    return raw


@needs_hypothesis
class SealFuzzTest(unittest.TestCase):
    """Sealed-file headers and bodies: SealError (or NotRecipient) for anything malformed or tampered with."""

    @classmethod
    def setUpClass(cls):
        """A recipient and a stranger."""
        cls.alice, cls.mallory = RNS.Identity(), RNS.Identity()

    @fuzz()
    @given(st.binary(max_size=120)
           | st.tuples(st.integers(0, 2**17) | msgpack_leaves,
                       st.lists(st.lists(st.binary(min_size=16, max_size=16) | st.binary(min_size=128, max_size=128)
                                         | msgpack_leaves, min_size=2, max_size=2), max_size=3) | msgpack_values,
                       st.none() | st.binary(max_size=64) | msgpack_leaves).map(list).map(umsgpack.packb))
    def test_header(self, header):
        try:
            recipients, name = seal._decode_header(header)
        except seal.SealError:
            return
        self.assertTrue(recipients)
        self.assertTrue(all(len(h) == 16 and len(k) == seal.WRAPPED_KEY_SIZE for h, k in recipients.items()))
        self.assertTrue(name is None or type(name) is bytes)

    @fuzz()
    @given(st.binary(max_size=300).map(lambda body: seal.MAGIC + body)
           | st.tuples(st.integers(0, 2), st.integers(0, 400), st.binary(max_size=400))
           .map(lambda t: seal.MAGIC + bytes([t[0]]) + t[1].to_bytes(4, "big") + t[2]))
    def test_open_any_file(self, data):
        try:
            sealed = seal.open_sealed(raw_reader(data), len(data))
            if sealed is not None:
                sealed.unlock(self.alice)
                b"".join(sealed.read())
        except (seal.SealError, seal.NotRecipient):
            pass

    @fuzz(0.25)
    @given(st.binary(max_size=200), st.none() | valid_names, st.data())
    def test_round_trip_and_tampering(self, plaintext, name, data):
        sealed_bytes = seal.seal_bytes(plaintext, [self.alice.get_public_key()], name)
        opened = seal.open_sealed(raw_reader(sealed_bytes), len(sealed_bytes))
        opened.unlock(self.alice)
        self.assertEqual(b"".join(opened.read()), plaintext)
        self.assertEqual(opened.name, name)
        with self.assertRaises(seal.NotRecipient):
            opened.unlock(self.mallory)
        # Any change anywhere: no longer sealed (magic), or an error before the whole plaintext is read
        tampered = data.draw(mutated(sealed_bytes))
        with self.assertRaises((seal.SealError, seal.NotRecipient, _NotSealed)):
            opened = seal.open_sealed(raw_reader(tampered), len(tampered))
            if opened is None:
                raise _NotSealed()
            opened.unlock(self.alice)
            b"".join(opened.read())


class _NotSealed(Exception):
    """open_sealed found no magic."""


# Small parsers of untrusted text and announce data

@needs_hypothesis
class SmallParsersFuzzTest(unittest.TestCase):
    """Announce app_data, CIDs (and their gateway host form), targets, gateway hosts and HTTP Range headers."""

    @fuzz()
    @given(st.binary(max_size=16) | msgpack_values.map(umsgpack.packb))
    def test_parse_mode(self, app_data):
        mode = parse_mode(app_data)
        self.assertIn(mode, (MODE_SERVER, MODE_CLIENT))
        if mode == MODE_SERVER:
            stream = io.BytesIO(app_data)
            value = umsgpack.unpack(stream)
            self.assertTrue(type(value) is int and value == 1 and not stream.read())

    @fuzz()
    @given(st.binary(max_size=48) | near_binaries, st.text(max_size=70) | cids.map(str)
           | cids.map(lambda cid: str(cid).upper()))
    def test_cid(self, binary, text):
        try:
            self.assertEqual(Cid.from_bytes(binary).binary, binary)
        except ValueError:
            pass
        try:
            self.assertEqual(str(Cid.parse(text)), text)
        except ValueError:
            pass

    @fuzz()
    @given(st.text(max_size=70) | cids.map(to_base36) | cids.map(lambda cid: to_base36(cid).upper())
           | st.text(alphabet="0123456789abcdefghijklmnopqrstuvwxyzK_ +-\u0663", min_size=53, max_size=61).map(
               lambda digits: "k" + digits)
           | st.tuples(cids.map(to_base36), st.integers(1, 59), st.characters()).map(
               lambda t: t[0][:t[1]] + t[2] + t[0][t[1] + 1:]))
    def test_cid_host_form(self, text):
        try:
            cid = from_base36(text)
        except ValueError:
            return
        self.assertEqual(to_base36(cid), text)  # canonical: the one host form of this CID
        self.assertEqual(Cid.parse(str(cid)), cid)
        self.assertNotEqual(str(cid), text)

    @fuzz()
    @given(st.none() | st.text(max_size=90)
           | st.tuples(st.text(max_size=8) | cids.map(to_base36) | ids.map(bytes.hex),
                       st.sampled_from([".rnfs.localhost:8789", ".name.localhost:8789", ".rnfs.localhost:8788",
                                        ".RNFS.localhost:8789", ".rnfs.localhost.:8789", ".rnfs.localhost", ""]),
                       st.sampled_from(["", " ", ".", "x"])).map("".join))
    def test_gateway_host(self, host):
        try:
            site = parse_host(host, 8789)
        except GatewayError as e:
            self.assertIn(e.status, (400, 403))
            return
        if site is None:
            self.assertIn(host.lower(), ("127.0.0.1:8789", "localhost:8789"))
            return
        kind, value = site
        label = value.hex() if kind == "name" else to_base36(value)
        self.assertEqual(host.lower(), f"{label}.{kind}.localhost:8789")

    @fuzz()
    @given(st.text(max_size=40) | st.tuples(cids.map(str), st.lists(uri_segments, max_size=3)).map(
        lambda t: "/".join([t[0], *t[1]])))
    def test_walker_target(self, text):
        try:
            cid, names = walker.parse_target(text)
        except ValueError:
            return
        self.assertTrue(all(dag.is_valid_name(name) for name in names))

    @fuzz()
    @given(st.none() | st.text(max_size=30)
           | st.tuples(st.sampled_from(["bytes=", "bytes= ", "Bytes=", "bytes=-"]),
                       st.text(alphabet="0123456789-, ٣²", max_size=45)).map("".join),
           st.integers(0, 2**63 - 1))
    @example("bytes=" + "9" * 5000, 10)
    def test_range(self, header, size):
        result = parse_range(header, size)
        if isinstance(result, tuple):
            start, end = result
            self.assertTrue(0 <= start <= end < size)
        else:
            self.assertIn(result, (None, "unsatisfiable"))


# /range end to end: the server's handler (rnfs.network.server) and a requester's client and partial blocks
# (rnfs.network.client, rnfs.ranges)

RANGE_NODE = b"\x07" * ID_LENGTH


class _RangeLinks:
    """Stands in for the LinkPool a PeerClient sends on: one link to every node, up, on which each request is answered
    by answer(path, request), both as they cross the wire (packed by the sender, unpacked by RNS)."""

    def __init__(self, answer):
        """Links answering with answer."""
        self.answer = answer
        self.on_rtt = None
        self.peer = types.SimpleNamespace(rate=None, link=None, is_active=lambda: True,
                                          record_rate=lambda exchange, min_sample: False)

    def get(self, node_id):
        """The link to node_id."""
        return self.peer

    def shared_peer(self, node_id):
        """The link to node_id."""
        return self.peer

    def clock(self) -> float:
        """A clock that doesn't move."""
        return 0.0

    def call(self, peer, path, request, **kwargs):
        """The exchange of one request: answered at once."""
        response = wire(self.answer(path, wire(request)))
        return types.SimpleNamespace(stalled=None, cancelled=False, answered=True, receipt=object(), sent_at=0.0,
                                     response_started=0.0, response=response,
                                     response_size=len(umsgpack.packb(response)))


def range_client(answer) -> peer_client.PeerClient:
    """A PeerClient whose /range requests answer(path, request) answers."""
    return peer_client.PeerClient(_RangeLinks(answer), Config())


def serving(harness: ServerHarness):
    """answer(path, request) for range_client: the request handled by harness's RequestServer, each on a new link
    (nothing in flight on it) a minute after the last (the compression budget refilling)."""
    def answer(path, request):
        """The handler's answer."""
        harness.clock.now += 60
        link = harness.connect(FakeInterface("hub", TCP_BITRATE), mtu=1500, mdu=1400)
        return harness.destination.handlers[path][0](path, request, link.link_id, link.link_id, None, 0.0)
    return answer


def answers_to(block: bytes, request: list, bombs: list):
    """What a provider of block might answer a /range request: the range raw or compressed, the range with a byte
    changed, a wrong size, a bz2 bomb, unserved, refused, or anything at all."""
    offset, max_bytes = request[1], request[2]
    data = block[offset:offset + protocol.range_length(len(block), offset, max_bytes)]
    honest = protocol.encode_range_response(len(block), protocol.RANGE_RAW, data)
    return (st.sampled_from([honest, protocol.encode_range_response(len(block), RANGE_BZ2,
                                                                    protocol.compress_range(data))])
            | mutated(data).map(lambda forged: protocol.encode_range_response(len(block), protocol.RANGE_RAW, forged))
            | st.integers(0, MAX_BLOCK_SIZE + 1).map(lambda size: protocol.encode_range_response(size, 0, data))
            | st.sampled_from(bombs).map(lambda bomb: protocol.encode_range_response(len(block), RANGE_BZ2, bomb))
            | st.sampled_from([[], False, None]) | range_answers)


@needs_hypothesis
class RangeExchangeFuzzTest(unittest.TestCase):
    """The /range handler (RequestServer) on any request, and a requester's ranges (PeerClient.block_range into
    rnfs.ranges' partial blocks) on any answers: bounded, and an honest exchange always completes the block."""

    @classmethod
    def setUpClass(cls):
        """Blocks to serve (empty, small, compressible, the largest) and one that isn't served."""
        text = (b"It is a truth universally acknowledged, that a single man in possession of a good fortune " * 1500)
        cls.blocks = {Cid.for_block(RAW, block): block for block in (
            b"", b"x", os.urandom(1000), text[:40 * KIB], text[:MAX_BLOCK_SIZE], os.urandom(MAX_BLOCK_SIZE))}
        cls.cids = [*cls.blocks, Cid.for_block(RAW, b"not served")]

    def setUp(self):
        """A temporary directory for partial blocks, and a server of the blocks."""
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.server = ServerHarness(dict(self.blocks))

    def store(self) -> tuple[ranges.Ranges, str]:
        """Fresh /range state with its partial blocks in files, and their directory."""
        directory = tempfile.mkdtemp(dir=self.tmp)
        return ranges.Ranges(ranges.RangeSupport(None), ranges.Partials(directory)), directory

    def check_served(self, request, answer):
        """The handler's answer to request: False if malformed or past the block's end, [] if not served, else
        exactly the range, compressed only if asked, as the requester's parser and precheck accept it."""
        parsed = protocol.parse_range_request(request)
        if parsed is None:
            self.assertIs(answer, False)
            return
        cid, offset, max_bytes, flags = parsed
        block = self.blocks.get(cid)
        if block is None:
            self.assertEqual(answer, [])
            self.assertTrue(protocol.is_range_unserved(wire(answer)))
            return
        if offset > len(block):
            self.assertIs(answer, False)
            return
        data = block[offset:offset + protocol.range_length(len(block), offset, max_bytes)]
        self.assertEqual(protocol.parse_range_response(wire(answer), offset, max_bytes, bool(flags & RANGE_BZ2),
                                                       len(block)), (len(block), data))
        self.assertIsNone(protocol.body_refusal(umsgpack.packb(answer)))
        self.assertLessEqual(protocol.packed_response_size(bytes(protocol.REQUEST_ID_LENGTH), answer),
                             protocol.range_max_response_size(max_bytes))

    @fuzz()
    @given(range_requests)
    def test_server_any_request(self, request):
        self.check_served(request, serving(self.server)(RANGE_PATH, request))

    @fuzz()
    @given(st.integers(0, 6), st.integers(0, 1100) | st.integers(0, MAX_BLOCK_SIZE + 1) | uint64,
           st.integers(0, 1100) | st.integers(0, MAX_BLOCK_SIZE + 1) | uint64, st.integers(0, 3) | uint64)
    def test_server_well_formed_requests(self, which, offset, max_bytes, flags):
        request = wire(protocol.encode_range(self.cids[which], offset, max_bytes, flags))
        self.check_served(request, serving(self.server)(RANGE_PATH, request))

    @fuzz(0.25)
    @given(range_blocks, st.integers(1, 12), st.booleans(), st.booleans())
    def test_honest_exchange_completes_the_block(self, block, parts, compress, declared):
        cid = Cid.for_block(RAW, block)
        target = max(1, -(-len(block) // parts))  # the range's size on the wire: the block in at most parts ranges
        client = range_client(serving(ServerHarness({cid: block})))
        store, directory = self.store()
        most = max(1, math.ceil(len(block) / target))  # ranges of at least target bytes each
        ratio = 1.0
        for count in range(1, most + 1):
            result = store.fetch(client, RANGE_NODE, cid, target, ratio, compress, len(block) if declared else None)
            self.assertEqual(result.status, peer_client.OK)
            ratio = result.ratio or ratio
            if result.blocks:
                break
            held = store.partials.length(cid, RANGE_NODE)
            self.assertEqual(result.partial, {cid: held})
            self.assertTrue(0 < held < len(block))
        self.assertEqual(result.blocks, {cid: block})
        if not compress:
            self.assertEqual(count, most)
        self.assertEqual((store.partials.count(), os.listdir(directory)), (0, []))
        self.assertIs(store.support.answers(RANGE_NODE), True)

    @fuzz(0.5)
    @given(range_blocks, st.integers(1, 2000) | st.integers(1, 64 * KIB), st.booleans(), st.booleans(), st.data())
    def test_any_answers_keep_partials_bounded(self, block, target, compress, declared, choices):
        cid = Cid.for_block(RAW, block)
        client = range_client(lambda path, request: choices.draw(answers_to(block, request, bz2_bombs())))
        store, directory = self.store()
        ratio = 1.0
        for _ in range(8):
            result = store.fetch(client, RANGE_NODE, cid, target, ratio, compress, len(block) if declared else None)
            ratio = result.ratio or ratio
            self.assertTrue(all(got == (cid, block) for got in result.blocks.items()))  # verified whole
            held = store.partials.length(cid, RANGE_NODE)
            self.assertLessEqual(held, len(block) if declared else MAX_BLOCK_SIZE)
            if result.partial:
                self.assertEqual(result.partial, {cid: held})
            files = os.listdir(directory)
            self.assertLessEqual(len(files), 1)
            self.assertEqual(os.path.getsize(os.path.join(directory, files[0])) if files else 0, held)
            if result.blocks or result.bad or result.missing:
                self.assertEqual((held, files), (0, []))  # a block completed, forged or not held: nothing kept
            if result.blocks:
                break


# NomadNet answers (rnfs/network/nomadnet.py) and links (rnfs/nomadnet_links.py)

SEGMENT = nomadnet_fetch.SEGMENT_SIZE
segment_fields = st.integers(-1, 6) | st.integers(0, 4 * SEGMENT) | msgpack_leaves
segment_hashes = st.sampled_from([b"h1" * 16, b"h2" * 16])


@st.composite
def nomadnet_segments(draw):
    """A Resource segment as RNS hands it to resource_started: any fields, often near an honest sender's."""
    total = draw(st.integers(0, 4 * SEGMENT) | st.sampled_from([SEGMENT, SEGMENT + 1, 2 * SEGMENT]) | segment_fields)
    honest_count = type(total) is int and total >= 0 and draw(st.booleans())
    return types.SimpleNamespace(
        segment_index=draw(st.integers(0, 5) | segment_fields),
        total_segments=nomadnet_fetch.expected_segments(total) if honest_count else draw(segment_fields),
        total_size=total, hash=draw(segment_hashes), original_hash=draw(segment_hashes | msgpack_leaves),
        has_metadata=draw(st.booleans() | msgpack_leaves),
        status=draw(st.sampled_from([RNS.Resource.COMPLETE, RNS.Resource.TRANSFERRING]) | msgpack_leaves))


def honest_segments(total: int, has_metadata: bool) -> list:
    """The segments an honest sender sends for an answer of total bytes, each complete once the next starts."""
    count = nomadnet_fetch.expected_segments(total)
    return [types.SimpleNamespace(segment_index=index, total_segments=count, total_size=total,
                                  hash=b"h1" * 16 if index == 1 else bytes([index]) * 32, original_hash=b"h1" * 16,
                                  has_metadata=has_metadata, status=RNS.Resource.COMPLETE)
            for index in range(1, count + 1)]


def accepted_segments(test: unittest.TestCase, segments: list) -> list:
    """The segments segment_refusal accepts in turn, as a Transfer does, up to the first refused."""
    first = previous = None
    accepted = []
    for segment in segments:
        reason = nomadnet_fetch.segment_refusal(segment, first, previous)
        test.assertTrue(reason is None or type(reason) is str)
        if reason is not None:
            break
        first = first or segment
        previous = segment
        accepted.append(segment)
    return accepted


nomadnet_link_texts = (st.text(max_size=60)
                       | st.tuples(st.sampled_from(["00" * 16, "aB" * 16, "0" * 31, "g" * 32]),
                                   st.sampled_from([":/page/", ":/file/", ":/", ":/page", ":/other/"]),
                                   st.lists(uri_segments | valid_names | st.sampled_from([".", "..", "", "a`b", "\n"]),
                                            max_size=3).map("/".join)).map("".join))


@needs_hypothesis
class NomadnetFuzzTest(unittest.TestCase):
    """segment_refusal on any sequence of segments, and the name a snapshot is added under whatever the node sends;
    NomadNet links and their names."""

    @fuzz()
    @given(st.lists(nomadnet_segments(), max_size=6))
    def test_any_segments(self, segments):
        accepted = accepted_segments(self, segments)
        for position, segment in enumerate(accepted):
            # Only one answer's segments, in order, each after a complete one, as many as its total needs
            self.assertEqual(segment.segment_index, position + 1)
            self.assertEqual((segment.original_hash, segment.total_size, bool(segment.has_metadata)),
                             (accepted[0].hash, accepted[0].total_size, bool(accepted[0].has_metadata)))
            self.assertEqual(segment.total_segments, nomadnet_fetch.expected_segments(segment.total_size))
            self.assertTrue(position == 0 or accepted[position - 1].status == RNS.Resource.COMPLETE)

    @fuzz()
    @given(st.integers(0, 5 * SEGMENT) | st.sampled_from([0, 1, SEGMENT, SEGMENT + 1, 3 * SEGMENT]), st.booleans(),
           st.data())
    def test_honest_segments_and_one_change(self, total, has_metadata, choices):
        count = nomadnet_fetch.expected_segments(total)
        self.assertTrue(total <= count * SEGMENT and (count == 1 or (count - 1) * SEGMENT < total))
        segments = honest_segments(total, has_metadata)
        self.assertEqual(accepted_segments(self, segments), segments)
        # Any one change to any segment: refused there
        index = choices.draw(st.integers(0, count - 1))
        changes = ["segment_index", "original_hash", "total_segments"]
        if index:
            changes += ["total_size", "has_metadata", "previous"]
        change = choices.draw(st.sampled_from(changes))
        segment = segments[index]
        if change == "segment_index":
            segment.segment_index = choices.draw(st.integers(0, count + 2).filter(lambda i: i != index + 1))
        elif change == "original_hash":
            segment.original_hash = b"h2" * 16
        elif change == "total_segments":
            segment.total_segments = choices.draw(st.integers(0, count + 2).filter(lambda n: n != count))
        elif change == "total_size":
            segment.total_size = choices.draw(st.integers(0, 6 * SEGMENT).filter(lambda size: size != total))
        elif change == "has_metadata":
            segment.has_metadata = not has_metadata
        else:
            segments[index - 1].status = RNS.Resource.TRANSFERRING
        self.assertEqual(len(accepted_segments(self, segments)), index)

    @fuzz()
    @given(metadata_values | metadata_maps, st.sampled_from(["/file/", "/page/"]), valid_names)
    def test_snapshot_name(self, metadata, kind, last):
        link = nomadnet_links.NomadnetLink(bytes(16), kind + "dir/" + last)
        for sent in (nomadnet_fetch._sent_name(metadata), metadata):
            name = nomadnet_links.snapshot_name(link, sent)
            self.assertTrue(dag.is_valid_name(name) and name.isprintable() or name == last)
            if kind == "/page/":
                self.assertEqual(name, last)

    @fuzz()
    @given(nomadnet_link_texts)
    def test_link(self, text):
        looks = nomadnet_links.looks_like(text)
        try:
            link = nomadnet_links.parse(text)
        except ValueError:
            return
        self.assertTrue(looks)
        self.assertEqual(len(link.destination), 16)
        self.assertEqual(nomadnet_links.parse(str(link)), link)
        self.assertTrue(nomadnet_links.is_valid_name(link.last_name) and link.last_name.isprintable())

    @fuzz()
    @given(st.text(max_size=12) | valid_names | st.text(min_size=60, max_size=260)
           | st.text(alphabet="aé😀/.\0", min_size=60, max_size=260)
           | st.sampled_from([".", "..", "", "a" * 255, "a" * 256, "é" * 127, "é" * 128]))
    def test_names_agree_with_dag(self, name):
        # The CLI checks names without loading RNS (rnfs/nomadnet_links.py), so its copy of the rule must be dag's
        self.assertEqual(nomadnet_links.is_valid_name(name), dag.is_valid_name(name))

    @fuzz()
    @given(st.binary(max_size=300) | msgpack_values
           | st.tuples(st.sampled_from(["", " ", "\n", "\r\n"]), st.booleans(), st.sampled_from(["", "\n", " \r\n"]))
           .map(lambda t: (t[0].encode() + (nomadnet_links.NOT_ALLOWED.replace(b"\n", b"\r\n") if t[1]
                                            else nomadnet_links.NOT_ALLOWED) + t[2].encode())))
    def test_not_allowed(self, data):
        allowed = nomadnet_links.is_not_allowed(data)
        self.assertIs(type(allowed), bool)
        if allowed:
            self.assertEqual(bytes(data).replace(b"\r\n", b"\n").strip(), nomadnet_links.NOT_ALLOWED.strip())
        elif type(data) is bytes and data.replace(b"\r\n", b"\n").strip() == nomadnet_links.NOT_ALLOWED.strip():
            self.fail("NomadNet's page not recognised")


# Rows the 0.2.0 stores save about other nodes (rnfs.dht.routing, firstseen, scores; rnfs.ranges)

SAVED_AT = 1.7e9  # Unix time "now" for loading saved rows
sql_values = (st.none() | st.integers(-2**63, 2**63 - 1) | st.floats() | st.text(max_size=8)
              | st.binary(max_size=20))
saved_times = st.floats(-1e12, 1e12) | st.floats(allow_nan=False) | st.integers(-2**63, 2**63 - 1) | sql_values
# Often now or within a few half-lives of it
recent_times = st.just(SAVED_AT) | st.floats(SAVED_AT - 30 * 86400, SAVED_AT) | saved_times
saved_scores = st.sampled_from([-SCORE_LIMIT, SCORE_LIMIT]) | st.floats(-80, 80) | saved_times  # often at the limit
saved_nodes = ids | sql_values
routing_rows = st.lists(st.tuples(saved_nodes, st.integers(-1, 3) | sql_values, saved_times,
                                  saved_times | st.floats(SAVED_AT - 1e6, SAVED_AT + 1e6),
                                  st.integers(-2, 5) | sql_values),
                        max_size=10, unique_by=lambda row: repr(row[0]))


def same_times(a, b) -> bool:
    """True if a and b are the same saved time (or both None), allowing for the wall-to-monotonic round trip."""
    if a is None or b is None:
        return a is b
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-3)


@needs_hypothesis
class SavedRowsFuzzTest(unittest.TestCase):
    """The routing table, the announce first-seen log, local scores and which nodes answer /range, loaded from saved
    rows holding anything SQLite can: never raising, within each store's bounds, and what a store saves loads back
    as the same state."""

    def setUp(self):
        """A temporary directory for state databases."""
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def saved(self, table: str, columns: tuple, rows: list) -> str:
        """A new state directory whose database's table holds rows (those SQLite takes); its path."""
        state_dir = tempfile.mkdtemp(dir=self.tmp)
        db = statedb.connect(state_dir)
        try:
            for row in rows:
                try:
                    with db.transaction() as tx:
                        tx.execute(f"INSERT OR REPLACE INTO {table} ({', '.join(columns)}) "
                                   f"VALUES ({', '.join('?' * len(columns))})", row)
                except OSError:  # a value its column refuses (NULL where NOT NULL): never in the table
                    pass
        finally:
            db.close()
        return state_dir

    @staticmethod
    def nodes(rows: list) -> set:
        """The node IDs among rows' first fields."""
        return {row[0] for row in rows if protocol.is_id(row[0])}

    def table(self) -> RoutingTable:
        """An empty routing table at SAVED_AT."""
        return RoutingTable(bytes(ID_LENGTH), Config(), clock=lambda: 5000.0, wall_clock=lambda: SAVED_AT)

    @fuzz()
    @given(routing_rows)
    def test_routing_rows(self, rows):
        table = self.table()
        table.restore(rows)
        nodes = table.nodes()
        self.assertTrue(all(protocol.is_id(node) and node != bytes(ID_LENGTH) for node in nodes))
        self.assertTrue(all(sum(table.bucket_index(node) == index for node in nodes) <= K for index in set(
            map(table.bucket_index, nodes))))
        saved = [row for row in map(table.row, self.nodes(rows)) if row is not None]
        self.assertTrue(set(nodes) <= {row[0] for row in saved})
        for row in saved:
            self.assertTrue(all(value is None or math.isfinite(value) for value in (row[2], row[3])))
            self.assertTrue(type(row[4]) is int and row[4] >= 0)
        again = self.table()
        again.restore(saved)
        restored = {row[0]: row for row in map(again.row, self.nodes(rows)) if row is not None}
        self.assertEqual(set(restored), {row[0] for row in saved})
        for row in saved:
            other = restored[row[0]]
            self.assertEqual((other[1], other[4]), (row[1], row[4]))
            self.assertTrue(same_times(other[2], row[2]) and same_times(other[3], row[3]))

    @fuzz(0.25)
    @given(st.lists(st.tuples(saved_nodes, saved_times, saved_times), max_size=8))
    def test_first_seen_rows(self, rows):
        state_dir = self.saved("announce_first_seen", ("node", "first_heard", "last_heard"), rows)
        heard = []
        for _ in range(2):  # loaded, saved, and loaded again: the same
            log = FirstSeenLog(state_dir, clock=lambda: SAVED_AT)
            try:
                first = {node: log.first_heard(node) for node in self.nodes(rows)}
                self.assertTrue(all(t is None or (math.isfinite(t) and t <= SAVED_AT) for t in first.values()))
                self.assertTrue(all(protocol.is_id(node) for node in log.closest(bytes(ID_LENGTH))))
                heard.append(first)
                log.save()
            finally:
                log.close()
        self.assertEqual(heard[0], heard[1])

    @fuzz(0.25)
    @given(st.lists(st.tuples(saved_nodes, saved_scores, recent_times, recent_times | st.none(),
                              recent_times | st.none()), max_size=8))
    def test_score_rows(self, rows):
        state_dir = self.saved("peer_scores", ("node", "score", "updated", "useful", "seeded"), rows)
        scores = []
        for _ in range(2):
            store = Scores(state_dir, clock=lambda: SAVED_AT)
            try:
                now = {node: store.score(node) for node in self.nodes(rows)}
                self.assertTrue(all(math.isfinite(s) and abs(s) <= SCORE_LIMIT for s in now.values()), now)
                self.assertTrue(all(type(band) is int for band in store.bands().values()))
                scores.append(now)
                store.save()
            finally:
                store.close()
        self.assertEqual(scores[0].keys(), scores[1].keys())
        self.assertTrue(all(math.isclose(scores[0][node], scores[1][node], abs_tol=1e-9) for node in scores[0]))

    @fuzz(0.25)
    @given(st.lists(st.tuples(saved_nodes, sql_values | st.booleans(), st.integers(-3, 5) | sql_values,
                              saved_times), max_size=8))
    def test_range_support_rows(self, rows):
        state_dir = self.saved("range_support", ("node", "answers", "silences", "at"), rows)
        db = statedb.connect(state_dir)
        try:
            support = ranges.RangeSupport(db, clock=lambda: SAVED_AT)
            for node in self.nodes(rows):
                self.assertIn(support.answers(node), (True, False, None))
                self.assertIs(type(support.silent(node)), bool)  # counting on from what was saved
                self.assertIn(support.answers(node), (False, None))
                support.answered(node)
                self.assertIs(support.answers(node), True)
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
