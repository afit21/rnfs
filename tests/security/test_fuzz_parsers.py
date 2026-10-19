"""Property-based fuzzing of the strict parsers that read untrusted bytes (security tooling, phase 2).

Every parser here reads data from other nodes, archives, sealed files or LXMF messages. For each, arbitrary
input (raw bytes, arbitrary msgpack values, and structured near-valid values built from the real format) must
either parse or fail with the parser's documented outcome (None, or its own error type), never another
exception. Where an encoder exists, decode(encode(x)) == x, and for canonical formats encode(decode(b)) == b
for every accepted b.

Needs hypothesis, which is a development tool and not one of rnfs's dependencies: without it every test here
is skipped, which includes CI (it installs only the locked dependencies). Run it by hand in an environment
that has hypothesis: `python -m unittest tests.security.test_fuzz_parsers`. The default run is small and
deterministic (derandomized); set RNFS_FUZZ_EXAMPLES (e.g. 3000) for a deeper, randomized run.
"""
import io
import os
import shutil
import tempfile
import time
import unittest

import RNS
from RNS.vendor import umsgpack

from rnfs.constants import ID_LENGTH, K, MAX_BLOCK_SIZE
from rnfs.content import archive, builder, dag, seal, walker
from rnfs.content.cid import DAG, RAW, Cid, from_base36, to_base36
from rnfs.content.dag import Child, DirectoryNode, Entry, FileNode, IndexChild, IndexNode
from rnfs.content.storage import Storage
from rnfs.errors import InvalidDag, StorageFull
from rnfs.features import keepers, lxmf
from rnfs.features.lxmf import Incoming, Link
from rnfs.gateway import GatewayError, parse_host, parse_range
from rnfs.names import encode_record, name_for_identity, parse_name, parse_record
from rnfs.network import protocol
from rnfs.network.announces import MODE_CLIENT, MODE_SERVER, parse_mode
from tests import test_lxmf

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
    """Silence RNS's log (the LXMF feature and storage log what they reject)."""
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
    @given(ids, st.lists(ids, max_size=K), st.lists(ids, max_size=K), uint64, st.lists(cids, max_size=8),
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
        self.assertEqual(protocol.parse_pin_request(wire(protocol.encode_pin(cid, pin))), (cid, pin))
        # As many entries as CIDs requested is always well-formed
        self.assertEqual(protocol.parse_blocks_response(wire(protocol.encode_blocks_response(entries)), len(entries)),
                         entries)


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


# LXMF fields, URIs and messages (rnfs.features.lxmf)

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
class LxmfFuzzTest(test_lxmf.FeatureTestCase):
    """decode_field, parse_uri, find_links and received(): links or ValueError (decode_field and parse_uri),
    and received() never raising whatever a validated message carries."""

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
        for parse in (lxmf.parse_uri, lxmf.parse_target):
            try:
                link = parse(text)
            except ValueError:
                continue
            # Whatever is accepted has a canonical form that reads back as the same link
            self.assertEqual(lxmf.parse_uri(lxmf.format_uri(link)), link)

    @fuzz()
    @given(links)
    def test_uri_round_trip(self, link):
        bare = Link(link.cid if link.name is None else None, link.name, link.path, None, link.from_node)
        self.assertEqual(lxmf.parse_uri(lxmf.format_uri(bare)), bare)

    @fuzz()
    @given(st.lists(uris | st.text(max_size=20), max_size=12).map(" \n".join) | st.text(max_size=200))
    def test_find_links(self, text):
        found = lxmf.find_links(text)
        self.assertLessEqual(len(found), lxmf.MAX_LINKS)
        self.assertEqual(len(found), len(set(found)))

    @fuzz(0.5)
    @given(st.binary(max_size=32) | st.text(max_size=8) | st.none(),
           st.binary(max_size=400) | st.lists(uris, max_size=3).map(lambda u: "\n".join(u).encode()) | msgpack_leaves,
           lxmf_fields, msgpack_leaves, msgpack_leaves)
    def test_received(self, title, content, fields, timestamp, display_name):
        if not hasattr(self, "feature"):
            self.feature_node()
            self.count = 0
        self.count += 1
        incoming = Incoming(test_lxmf.SENDER, self.count.to_bytes(32, "big"), timestamp, title, content, fields,
                            "direct", display_name)
        entry_id = self.feature.received(incoming)
        self.assertTrue(entry_id is None or type(entry_id) is int)


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


if __name__ == "__main__":
    unittest.main()
