"""Tests for archives (specification section 8): export and import of msgpack block streams."""
import io
import os
import tempfile
import unittest
from unittest import mock

from RNS.vendor import umsgpack

from rnfs.content import archive, builder, dag, walker
from rnfs.content.cid import EMPTY_DIRECTORY, RAW, Cid
from rnfs.content.dag import DirectoryNode, Entry
from rnfs.content.storage import USER, Storage
from rnfs.errors import StorageFull, TooLarge


class ArchiveTest(unittest.TestCase):
    """Round trips between two storages."""

    def setUp(self):
        """A source storage with a tree, and an empty destination."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.source = self.storage("source")
        self.destination = self.storage("destination")
        put = self.source.put
        self.shared = builder.build_bytes(b"S" * 70000, put)
        self.other = builder.build_bytes(b"other", put)
        self.root = builder.build_directory(
            [Entry("a", self.shared.cid, 70000, 0), Entry("b", self.shared.cid, 70000, 1),
             Entry("c", self.other.cid, 5, 0)], put)

    def storage(self, name: str) -> Storage:
        """Storage under the temporary directory."""
        return Storage(os.path.join(self.tmp.name, name, "blocks"), os.path.join(self.tmp.name, name, "state"), 10**9)

    def export(self, roots) -> bytes:
        """Export roots from the source."""
        out = io.BytesIO()
        archive.export_archive(roots, self.source.getter(), out)
        return out.getvalue()

    def records(self, data: bytes) -> list:
        """Every msgpack object in an archive."""
        stream = io.BytesIO(data)
        result = []
        while stream.tell() < len(data):
            result.append(umsgpack.unpack(stream))
        return result

    def test_format(self):
        data = self.export([self.root.cid])
        records = self.records(data)
        self.assertEqual(records[0], ["rnfs-archive", 1, [self.root.cid.binary]])
        cids = [Cid.from_bytes(cid) for cid, _ in records[1:]]
        self.assertEqual(cids[0], self.root.cid)  # depth-first from the root
        self.assertEqual(len(cids), len(set(cids)))  # each block once, though "shared" appears twice
        self.assertEqual(set(cids), {e.cid for e, _ in walker.iter_dag(walker.root_expect(self.root.cid),
                                                                       self.source.getter())})
        for cid, block in records[1:]:
            self.assertTrue(Cid.from_bytes(cid).verify(block))
        self.assertTrue(data.startswith(umsgpack.packb(records[0])))

    def test_round_trip_pins_roots(self):
        data = self.export([self.root.cid, self.other.cid])
        result = archive.import_archive(io.BytesIO(data), self.destination)
        self.assertEqual(result["roots"], [self.root.cid, self.other.cid])
        self.assertEqual(result["complete"], [self.root.cid, self.other.cid])
        self.assertEqual(result["incomplete"], [])
        self.assertEqual(result["bad"], 0)
        self.assertEqual(self.destination.pins(), {self.root.cid: {USER}, self.other.cid: {USER}})
        self.assertTrue(self.destination.is_complete(self.root.cid))
        # Blocks shared between roots are written once
        self.assertEqual(result["blocks"], len(self.records(data)) - 1)
        self.assertEqual(len(self.records(self.export([self.root.cid]))), len(self.records(data)))

    def test_inner_target(self):
        target = walker.resolve(self.root.cid, ["b"], self.source.getter())
        data = self.export([target])
        self.assertEqual(self.records(data)[0][2], [self.shared.cid.binary])
        result = archive.import_archive(io.BytesIO(data), self.destination, pin_source=None)
        self.assertEqual(result["complete"], [self.shared.cid])
        self.assertEqual(self.destination.pins(), {})

    def test_incomplete_and_bad_blocks(self):
        records = self.records(self.export([self.root.cid]))
        # Drop one block and damage another
        damaged = [records[0]] + records[1:-2] + [[records[-1][0], b"damaged"]]
        data = b"".join(umsgpack.packb(r) for r in damaged)
        result = archive.import_archive(io.BytesIO(data), self.destination)
        self.assertEqual(result["incomplete"], [self.root.cid])
        self.assertEqual(result["bad"], 1)
        self.assertEqual(self.destination.pins(), {})

    def test_rejects_non_archives(self):
        for data in (b"", umsgpack.packb(["not-an-archive", 1, []]), umsgpack.packb(["rnfs-archive", 2, []]),
                     umsgpack.packb({"a": 1}), b"\x93\xacrnfs-archive\x01"):
            with self.subTest(data=data):
                with self.assertRaises(ValueError):
                    archive.import_archive(io.BytesIO(data), self.destination)

    def test_rejects_malformed_records(self):
        header = umsgpack.packb(["rnfs-archive", 1, [self.other.cid.binary]])
        record = umsgpack.packb([self.other.cid.binary, b"other"])
        for tail in (record[:-1], umsgpack.packb([self.other.cid.binary]), umsgpack.packb([b"x" * 36, b"x"]),
                     b"\x92\xc4\x24" + self.other.cid.binary + b"\xc6\xff\xff\xff\xff"):
            with self.subTest(tail=tail):
                with self.assertRaises(ValueError):
                    archive.import_archive(io.BytesIO(header + tail), self.destination)
        result = archive.import_archive(io.BytesIO(header + record), self.destination)
        self.assertEqual(result["complete"], [self.other.cid])

    def test_export_max_entries(self):
        cid = EMPTY_DIRECTORY
        self.source.put(cid, dag.encode(DirectoryNode([])))
        for _ in range(3):
            cid = builder.build_directory([Entry(f"d{i}", cid, 0, 2) for i in range(10)], self.source.put).cid
        with self.assertRaises(TooLarge):
            archive.export_archive([cid], self.source.getter(), io.BytesIO(), max_entries=1000)
        out = io.BytesIO()
        self.assertEqual(archive.export_archive([cid], self.source.getter(), out, max_entries=1110)["blocks"], 4)

    def test_header_only_archive_does_not_pin_cached_content(self):
        cached = builder.build_bytes(b"downloaded earlier", self.destination.put)  # cache, not pinned
        header = umsgpack.packb(["rnfs-archive", 1, [cached.cid.binary]])
        result = archive.import_archive(io.BytesIO(header), self.destination)
        self.assertEqual(result["complete"], [])
        self.assertEqual(result["incomplete"], [cached.cid])
        self.assertEqual(self.destination.pins(), {})

    def test_partial_archive_does_not_pin_cached_dag(self):
        # The destination holds the whole tree as cache; an archive with only its root block mustn't pin it
        for expect, block in walker.iter_dag(walker.root_expect(self.root.cid), self.source.getter()):
            self.destination.put(expect.cid, block)
        records = self.records(self.export([self.root.cid]))
        data = b"".join(umsgpack.packb(r) for r in records[:2])
        self.assertEqual(Cid.from_bytes(records[1][0]), self.root.cid)
        result = archive.import_archive(io.BytesIO(data), self.destination)
        self.assertEqual(result["incomplete"], [self.root.cid])
        self.assertEqual(self.destination.pins(), {})
        # The whole DAG in the archive pins it, though every block was already stored
        result = archive.import_archive(io.BytesIO(self.export([self.root.cid])), self.destination)
        self.assertEqual(result["complete"], [self.root.cid])
        self.assertEqual(self.destination.pins(), {self.root.cid: {USER}})

    def test_junk_for_stored_blocks_does_not_pin_cached_dag(self):
        # Blocks already held must be verified like any other, or junk would count as supplying them
        for expect, block in walker.iter_dag(walker.root_expect(self.root.cid), self.source.getter()):
            self.destination.put(expect.cid, block)
        records = self.records(self.export([self.root.cid]))
        data = umsgpack.packb(records[0]) + b"".join(umsgpack.packb([cid, b"junk"]) for cid, _ in records[1:])
        result = archive.import_archive(io.BytesIO(data), self.destination)
        self.assertEqual((result["complete"], result["blocks"], result["bad"]), ([], 0, len(records) - 1))
        self.assertEqual(self.destination.pins(), {})
        self.assertTrue(self.destination.is_complete(self.root.cid))  # the stored blocks are untouched

    def test_import_keeps_the_disk_reserve_free(self):
        data = self.export([self.root.cid])
        with mock.patch.object(self.destination, "check_disk", side_effect=StorageFull("full")) as check:
            with self.assertRaises(StorageFull):
                archive.import_archive(io.BytesIO(data), self.destination)
        check.assert_called_once()
        self.assertEqual(self.destination.block_count(), 0)
        self.assertEqual(self.destination.pins(), {})

    def test_export_of_several_roots_writes_each_block_once(self):
        data = self.export([self.root.cid, self.shared.cid, self.other.cid])
        cids = [Cid.from_bytes(cid) for cid, _ in self.records(data)[1:]]
        self.assertEqual(len(cids), len(set(cids)))
        self.assertEqual(set(cids), {e.cid for e, _ in walker.iter_dag(walker.root_expect(self.root.cid),
                                                                       self.source.getter())})

    def test_root_count_is_capped_before_reading_roots(self):
        # An array header claiming MAX_ROOTS + 1 roots, with none following: refused without reading them
        header = umsgpack.packb("rnfs-archive")
        data = b"\x93" + header + b"\x01\xdc" + (archive.MAX_ROOTS + 1).to_bytes(2, "big")
        with self.assertRaisesRegex(ValueError, "roots"):
            archive.import_archive(io.BytesIO(data), self.destination)
        with self.assertRaises(ValueError):
            archive.export_archive([self.other.cid] * (archive.MAX_ROOTS + 1), self.source.getter(), io.BytesIO())
        data = self.export([self.other.cid] * archive.MAX_ROOTS)
        self.assertEqual(len(archive.import_archive(io.BytesIO(data), self.destination)["complete"]),
                         archive.MAX_ROOTS)

    def test_empty_file_root(self):
        empty = builder.build_bytes(b"", self.source.put)
        data = self.export([empty.cid])
        result = archive.import_archive(io.BytesIO(data), self.destination)
        self.assertEqual(result["complete"], [empty.cid])
        self.assertEqual(self.destination.get(empty.cid), b"")
        self.assertEqual(empty.cid, Cid.for_block(RAW, b""))


if __name__ == "__main__":
    unittest.main()
