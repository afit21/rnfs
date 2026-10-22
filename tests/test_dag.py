"""Tests for DAG node encoding and strict decoding (specification section 5)."""
import random
import struct
import unittest
from unittest import mock

from RNS.vendor import umsgpack

from rnfs.constants import MAX_BLOCK_SIZE
from rnfs.content import dag
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.dag import Child, DirectoryNode, Entry, FileNode, IndexChild, IndexNode
from rnfs.errors import InvalidDag

CHUNK = Cid.for_block(RAW, b"chunk")
NODE = Cid.for_block(DAG, b"node")


def pack(value) -> bytes:
    """value as umsgpack writes it."""
    return umsgpack.packb(value)


class EncodingTest(unittest.TestCase):
    """Nodes encode exactly as umsgpack.packb writes them and decode back."""

    def test_round_trips(self):
        nodes = [
            FileNode([Child(CHUNK, 5), Child(NODE, 2**64 - 6)]),
            FileNode([]),
            DirectoryNode([Entry("a", CHUNK, 5, 0), Entry("b", NODE, 300, 1), Entry("été", NODE, 0, 2)]),
            DirectoryNode([]),
            IndexNode([IndexChild("a", NODE, 1), IndexChild("m", NODE, 70000)]),
        ]
        for node in nodes:
            with self.subTest(node):
                block = dag.encode(node)
                self.assertEqual(dag.decode(block), node)

    def test_encoding_is_umsgpack(self):
        self.assertEqual(dag.encode(FileNode([Child(CHUNK, 5)])), pack([1, [[CHUNK.binary, 5]]]))
        self.assertEqual(dag.encode(DirectoryNode([Entry("a", CHUNK, 5, 1)])), pack([2, [["a", CHUNK.binary, 5, 1]]]))
        self.assertEqual(dag.encode(IndexNode([IndexChild("a", NODE, 7)])), pack([3, [["a", NODE.binary, 7]]]))
        self.assertEqual(dag.encode(DirectoryNode([])), b"\x92\x02\x90")

    def test_umsgpack_writes_shortest_forms(self):
        # The rules the decoder relies on: str as str, bytes as bin, shortest headers
        self.assertEqual(pack("a")[:1], b"\xa1")
        self.assertEqual(pack("a" * 32)[:2], b"\xd9\x20")
        self.assertEqual(pack(b"x")[:2], b"\xc4\x01")
        self.assertEqual(pack(127), b"\x7f")
        self.assertEqual(pack(128), b"\xcc\x80")
        self.assertEqual(pack(2**64 - 1), b"\xcf" + b"\xff" * 8)
        self.assertEqual(pack(list(range(16)))[:3], b"\xdc\x00\x10")

    def test_sizes(self):
        self.assertEqual(FileNode([Child(CHUNK, 3), Child(CHUNK, 4)]).size, 7)
        self.assertEqual(DirectoryNode([Entry("a", CHUNK, 3, 0), Entry("b", CHUNK, 4, 0)]).size, 7)
        self.assertEqual(IndexNode([IndexChild("a", NODE, 3)]).size, 3)

    def test_child_cids_and_names(self):
        file = FileNode([Child(CHUNK, 3), Child(NODE, 4)])
        directory = DirectoryNode([Entry("a", CHUNK, 3, 0), Entry("b", NODE, 4, 2)])
        index = IndexNode([IndexChild("a", NODE, 3), IndexChild("m", CHUNK, 4)])
        self.assertEqual((file.child_cids, file.names), ((CHUNK, NODE), ()))
        self.assertEqual((directory.child_cids, directory.names), ((CHUNK, NODE), ("a", "b")))
        self.assertEqual((index.child_cids, index.names), ((NODE, CHUNK), ("a", "m")))


class StrictDecodingTest(unittest.TestCase):
    """Every way a node can break the encoding rules is rejected."""

    def assertInvalid(self, block: bytes):
        """block must not decode."""
        with self.assertRaises(InvalidDag):
            dag.decode(block)

    def test_rejects_wrong_types(self):
        cid = CHUNK.binary
        bad = {
            "bool type tag": b"\x92\xc3\x90",
            "bool size": b"\x92\x01\x91\x92\xc4\x24" + cid + b"\xc3",
            "bool kind": pack([2, [["a", cid, 1, 0]]]).replace(b"\x01\x00", b"\x01\xc2"),
            "negative size": pack([1, [[cid, -1]]]),
            "float size": pack([1, [[cid, 1.0]]]),
            "nil size": pack([1, [[cid, None]]]),
            "CID as str": pack([1, [["x" * 36, 1]]]),
            "name as bin": pack([2, [[b"a", cid, 1, 0]]]),
            "map": pack({1: 2}),
            "map as children": pack([1, {}]),
            "top-level int": pack(1),
            "children as bin": pack([1, b""]),
            "child as map": pack([1, [{"cid": cid}]]),
        }
        for reason, block in bad.items():
            with self.subTest(reason):
                self.assertInvalid(block)

    def test_rejects_wrong_shapes(self):
        cid = CHUNK.binary
        bad = {
            "extra top-level field": pack([1, [], 0]),
            "missing top-level field": pack([1]),
            "extra file child field": pack([1, [[cid, 1, 0]]]),
            "missing entry field": pack([2, [["a", cid, 1]]]),
            "extra index field": pack([3, [["a", NODE.binary, 1, 0]]]),
            "unknown type": pack([4, []]),
            "type 0": pack([0, []]),
            "invalid CID": pack([1, [[b"\x01\x70" + bytes(34), 1]]]),
            "trailing bytes": pack([1, []]) + b"\x00",
            "truncated": pack([1, [[cid, 5]]])[:-1],
            "empty": b"",
            "huge array count": b"\x92\x01\xdd\xff\xff\xff\xff",
        }
        for reason, block in bad.items():
            with self.subTest(reason):
                self.assertInvalid(block)

    def test_rejects_non_shortest_headers(self):
        cid = CHUNK.binary
        good = pack([2, [["abc", cid, 5, 0]]])
        dag.decode(good)
        bad = {
            "int8 for fixint": pack([1, [[cid, 5]]]).replace(b"\x05", b"\xcc\x05"),
            "uint16 for uint8": pack([1, [[cid, 200]]]).replace(b"\xcc\xc8", b"\xcd\x00\xc8"),
            "uint64 for fixint type": b"\x92\xcf" + bytes(7) + b"\x01\x90",
            "str8 for fixstr": good.replace(b"\xa3abc", b"\xd9\x03abc"),
            "array16 for fixarray": b"\x92\x02\xdc\x00\x00",
            "array32 top level": b"\xdd\x00\x00\x00\x02\x02\x90",
            "bin16 for bin8": pack([1, [[cid, 5]]]).replace(b"\xc4\x24", b"\xc5\x00\x24"),
        }
        for reason, block in bad.items():
            with self.subTest(reason):
                self.assertInvalid(block)

    def test_rejects_every_truncation(self):
        # str8, bin8 and uint64 headers, and fixarray, fixstr and fixint ones
        block = pack([2, [["n" * 40, CHUNK.binary, 2**40, 0], ["o", NODE.binary, 7, 2]]])
        dag.decode(block)
        for end in range(len(block)):
            with self.subTest(end=end):
                self.assertInvalid(block[:end])

    def test_rejects_invalid_utf8(self):
        block = pack([2, [["ab", CHUNK.binary, 1, 0]]]).replace(b"\xa2ab", b"\xa2\xff\xfe")
        self.assertInvalid(block)
        # A UTF-8-encoded surrogate isn't valid UTF-8 either
        self.assertInvalid(pack([2, [["ab", CHUNK.binary, 1, 0]]]).replace(b"\xa2ab", b"\xa3\xed\xa0\x80"))

    def test_rejects_bad_names(self):
        for name in ("", ".", "..", "a/b", "a\0b", "x" * 256, "é" * 128):
            with self.subTest(name=name):
                self.assertInvalid(pack([2, [[name, CHUNK.binary, 1, 0]]]))
                self.assertInvalid(pack([3, [[name, NODE.binary, 1]]]))

    def test_rejects_unsorted_or_duplicate_names(self):
        cid = CHUNK.binary
        self.assertInvalid(pack([2, [["b", cid, 1, 0], ["a", cid, 1, 0]]]))
        self.assertInvalid(pack([2, [["a", cid, 1, 0], ["a", cid, 1, 0]]]))
        self.assertInvalid(pack([3, [["b", NODE.binary, 1], ["a", NODE.binary, 1]]]))
        self.assertInvalid(pack([3, [["a", NODE.binary, 1], ["a", NODE.binary, 1]]]))
        # Bytewise UTF-8 order: "Z" < "a" < "é"
        dag.decode(pack([2, [["Z", cid, 1, 0], ["a", cid, 1, 0], ["é", cid, 1, 0]]]))

    def test_rejects_unknown_kind(self):
        self.assertInvalid(pack([2, [["a", CHUNK.binary, 1, 3]]]))

    def test_rejects_size_overflow(self):
        half = 2**63
        self.assertInvalid(pack([1, [[CHUNK.binary, half], [CHUNK.binary, half]]]))
        self.assertInvalid(pack([2, [["a", CHUNK.binary, half, 0], ["b", CHUNK.binary, half, 0]]]))
        dag.decode(pack([1, [[CHUNK.binary, half], [CHUNK.binary, half - 1]]]))

    def test_rejects_oversized_block(self):
        count = MAX_BLOCK_SIZE // 40 + 1
        block = pack([1, [[CHUNK.binary, 1]] * count])
        self.assertGreater(len(block), MAX_BLOCK_SIZE)
        self.assertInvalid(block)


_ARRAYS = [(0x90, 15, ""), (0xDC, 2**16 - 1, ">H"), (0xDD, 2**32 - 1, ">I")]
_UINTS = [(0x00, 127, ""), (0xCC, 2**8 - 1, ">B"), (0xCD, 2**16 - 1, ">H"), (0xCE, 2**32 - 1, ">I"),
          (0xCF, 2**64 - 1, ">Q")]
_STRS = [(0xA0, 31, ""), (0xD9, 2**8 - 1, ">B"), (0xDA, 2**16 - 1, ">H"), (0xDB, 2**32 - 1, ">I")]
_BINS = [(0xC4, 2**8 - 1, ">B"), (0xC5, 2**16 - 1, ">H"), (0xC6, 2**32 - 1, ">I")]


def _pack_with(value, choose) -> bytes:
    """value packed as msgpack, each header's form chosen by choose(fitting), fitting being the forms (first type
    byte, most it carries, struct format of what follows; fixed forms have "") that can carry it, shortest first."""

    def header(forms, number: int) -> bytes:
        tag, _, fmt = choose([form for form in forms if number <= form[1]])
        return bytes([tag | number]) if not fmt else bytes([tag]) + struct.pack(fmt, number)

    if isinstance(value, list):
        return header(_ARRAYS, len(value)) + b"".join(_pack_with(item, choose) for item in value)
    if isinstance(value, int):
        return header(_UINTS, value)
    data = value.encode("utf-8") if isinstance(value, str) else value
    return header(_STRS if isinstance(value, str) else _BINS, len(data)) + data


class ShortestHeaderTest(unittest.TestCase):
    """decode() checks each header is the shortest form instead of encoding the node again: it must accept exactly
    the blocks that a reader taking any header form, followed by a re-encode check, accepts."""

    @staticmethod
    def accepted(block: bytes) -> bool:
        """Whether decode, without its memo, accepts block."""
        try:
            dag._decode(block)
            return True
        except InvalidDag:
            return False

    @staticmethod
    def re_encodes(block: bytes) -> bool:
        """Whether the reader taking any header form (no _SHORTEST minimum) reads block and the node re-encodes to
        it: the check decode made before."""
        with mock.patch.dict(dag._SHORTEST, dict.fromkeys(dag._SHORTEST, 0)):
            try:
                return dag.encode(dag._decode(block)) == block
            except InvalidDag:
                return False

    def assertSame(self, block: bytes):
        """decode accepts block exactly when the re-encode check does."""
        self.assertEqual(self.accepted(block), self.re_encodes(block), block.hex())

    def values(self) -> list:
        """Nodes as plain values, at the edges of each header form: fixint/uint8/16/32/64 sizes, fixstr/str8 names
        (str16 is too long for a name, but its header is read first), fixarray/array16 counts."""
        sizes = [0, 127, 128, 255, 256, 2**16 - 1, 2**16, 2**32 - 1, 2**32]
        return [
            [1, [[CHUNK.binary, size] for size in sizes]],
            [1, [[CHUNK.binary, 2**64 - 1]]],
            [1, [[CHUNK.binary, 1]] * 15],
            [1, [[CHUNK.binary, 1]] * 16],
            [1, []],
            [2, [["n" * 31, CHUNK.binary, 7, 0], ["o" * 32, NODE.binary, 300, 2], ["p" * 255, CHUNK.binary, 0, 1]]],
            [2, [["x" * 256, CHUNK.binary, 1, 0]]],
            [2, [[f"{n:03}", CHUNK.binary, n, 0] for n in range(17)]],
            [3, [["a", NODE.binary, 2**40], ["é", NODE.binary, 128]]],
            [2, []],
        ]

    def test_every_header_form(self):
        """Each node with one header at a time written in each longer form, and with every header's form chosen
        at random: accepted only when all are shortest, and exactly when the re-encode check accepts."""
        rng = random.Random(5)
        for value in self.values():
            with self.subTest(value=str(value)[:60]):
                valid = not (value[0] == 2 and value[1] and len(value[1][0][0]) > 255)  # all but the str16 name
                self.assertEqual(self.accepted(pack(value)), valid)
                headers = []
                _pack_with(value, lambda fitting: headers.append(len(fitting)) or fitting[0])
                for at, forms in enumerate(headers):
                    for form in range(1, forms):
                        seen = []

                        def choose(fitting):
                            """Shortest, but form for header at."""
                            seen.append(None)
                            return fitting[form] if len(seen) - 1 == at else fitting[0]

                        block = _pack_with(value, choose)
                        self.assertFalse(self.accepted(block))
                        self.assertSame(block)
                for _ in range(100):
                    chosen = []
                    block = _pack_with(value, lambda fitting: chosen.append(rng.randrange(len(fitting)))
                                       or fitting[chosen[-1]])
                    self.assertSame(block)
                    if any(chosen):
                        self.assertFalse(self.accepted(block))

    def test_every_byte_changed(self):
        """Every byte of small valid nodes set to every value, and every truncation and extension by a byte."""
        blocks = [pack([2, [["ab", CHUNK.binary, 200, 0], ["c" * 33, NODE.binary, 70000, 2]]]),
                  pack([3, [["a", NODE.binary, 2**33]]]), pack([1, [[CHUNK.binary, 5]]])]
        for block in blocks:
            for at in range(len(block)):
                for byte in range(256):
                    self.assertSame(block[:at] + bytes([byte]) + block[at + 1:])
                self.assertSame(block[:at])
                for byte in range(256):
                    self.assertSame(block[:at] + bytes([byte]) + block[at:])


class NameTest(unittest.TestCase):
    """The name rules."""

    def test_valid(self):
        for name in ("a", ".hidden", "...", "x" * 255, "é" * 127, "with space", "日本"):
            with self.subTest(name=name):
                dag.check_name(name)
                self.assertTrue(dag.is_valid_name(name))

    def test_invalid(self):
        for name in ("", ".", "..", "a/b", "/", "a\0", "x" * 256, "\udcff", b"a", None):
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    dag.check_name(name)
                self.assertFalse(dag.is_valid_name(name))


class MemoTest(unittest.TestCase):
    """decode() remembers the nodes of blocks it decoded, and only those."""

    def setUp(self):
        """An empty memo, restored to the module's own afterwards."""
        saved = dag._memo
        dag._memo = dag._Memo()
        self.addCleanup(setattr, dag, "_memo", saved)

    def node(self, i: int) -> bytes:
        """A valid file node's block, different for each i."""
        return dag.encode(FileNode([Child(Cid.for_block(RAW, b"%d" % i), 10)]))

    def test_same_bytes_give_the_remembered_node(self):
        """A second decode of the same bytes (as any bytes-like object) is the first's node, unchanged."""
        block = self.node(1)
        node = dag.decode(block)
        self.assertIs(dag.decode(bytearray(block)), node)
        self.assertIs(dag.decode(memoryview(block)), node)
        self.assertEqual(node, FileNode([Child(Cid.for_block(RAW, b"1"), 10)]))

    def test_invalid_blocks_are_checked_every_time(self):
        """A block that fails to decode isn't remembered: it raises each time."""
        bad = pack([1, [[Cid.for_block(RAW, b"x").binary, 10]], 0])
        for _ in range(2):
            with self.assertRaises(InvalidDag):
                dag.decode(bad)
        self.assertEqual(len(dag._memo._nodes), 0)

    def test_bounded_by_count_and_bytes(self):
        """At most MEMO_BLOCKS nodes and MEMO_BYTES of blocks, the least recently used forgotten first."""
        blocks = [self.node(i) for i in range(dag.MEMO_BLOCKS + 10)]
        first = dag.decode(blocks[0])
        for block in blocks[1:]:
            dag.decode(block)
        self.assertEqual(len(dag._memo._nodes), dag.MEMO_BLOCKS)
        self.assertIsNot(dag.decode(blocks[0]), first)  # forgotten: decoded again
        self.assertIs(dag.decode(blocks[-1]), dag.decode(blocks[-1]))
        size = len(dag.encode(FileNode([Child(Cid.for_block(RAW, b"0-%d" % i), 10) for i in range(2000)])))
        for n in range(dag.MEMO_BYTES // size + 5):
            big = dag.encode(FileNode([Child(Cid.for_block(RAW, b"%d-%d" % (n, i)), 10) for i in range(2000)]))
            dag.decode(big)
            dag.decode(big)  # the same block again: counted once
        self.assertLessEqual(dag._memo._bytes, dag.MEMO_BYTES)
        self.assertGreater(dag._memo._bytes, dag.MEMO_BYTES - 2 * size)
        self.assertEqual(dag._memo._bytes, sum(len(block) for block in dag._memo._nodes))


if __name__ == "__main__":
    unittest.main()
