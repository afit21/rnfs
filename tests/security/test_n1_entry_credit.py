"""N1-R3: fetch_dag's DAG byte bound credited 1 KiB per directory entry met, whatever the entry, so a directory
declaring 0 bytes whose entries all point at the same empty file could carry about 20 times its own bytes of other
blocks (about 100 MB at max_output_entries); and since the credit stopped at max_output_entries entries, an honest
directory of more than about 331,000 long-named entries could no longer be pinned.

B pins a 0-byte directory of 2,600 entries pointing at the empty file plus 20 distinct 118 KB file nodes of empty
chunks: it must be refused with TooLarge after downloading at most the directory, 16 bytes per entry, dag_bound(0)
and about one block. B, with max_output_entries 1,000, pins an honest directory of 4,000 entries with the longest
names (1.2 MB of directory nodes, more than 1,000 credits of 1 KiB): every entry now brings its own bytes, so it is
pinned (get and export of it are still refused by max_output_entries).
"""
import os
import unittest

from rnfs.constants import MAX_BLOCK_SIZE
from rnfs.content import builder, dag
from rnfs.content.cid import EMPTY_FILE
from rnfs.content.storage import USER
from rnfs.errors import TooLarge
from rnfs.fetcher import dag_bound
from tests.security._content_helpers import TwoNodeTest
from tests.security._fetchr2_helpers import PAD, Blocks
from tests.test_node import A_ID

DUPLICATES = 2600
PER_ENTRY = 16  # what each entry may bring beyond its own bytes


class DuplicateEntriesTest(TwoNodeTest):
    """A 0-byte directory whose many entries are the same empty file, beside a few large nodes of empty chunks."""

    def test_pin_is_refused(self):
        blocks = Blocks()
        entries = [dag.Entry(f"{i:05d}", EMPTY_FILE, 0, dag.KIND_FILE) for i in range(DUPLICATES)]
        junk = [blocks.node(dag.FileNode([dag.Child(EMPTY_FILE, 0)] * (PAD - i))) for i in range(20)]
        entries += [dag.Entry(f"z{i:04d}", cid, 0, dag.KIND_FILE) for i, cid in enumerate(junk)]
        directory = blocks.node(dag.DirectoryNode(entries))
        self.serve(blocks.blocks, directory)
        with self.assertRaises(TooLarge, msg="the pin went ahead: the entries' credit paid for the junk") as caught:
            self.b.pin_add(str(directory))
        self.assertIn("refusing it", str(caught.exception))
        allowed = len(blocks.blocks[directory]) + PER_ENTRY * len(entries) + dag_bound(0) + MAX_BLOCK_SIZE
        self.assertLessEqual(self.net.downloaded_bytes, allowed)
        self.assertNotIn(directory, self.b.storage.pins())


class LargeHonestDirectoryTest(TwoNodeTest):
    """A directory with more entries than max_output_entries, each with the longest name."""

    b_settings = {"max_output_entries": 1000}

    def test_pin_goes_ahead(self):
        with self.a.storage.protect() as session:
            empty = builder.build_bytes(b"", session.put).cid
            built = builder.build_directory([dag.Entry(f"{i:05d}".ljust(255, "n"), empty, 0, dag.KIND_FILE)
                                             for i in range(4000)], session.put)
            self.a.storage.pin(built.cid, USER)
        self.a.publisher.refresh()
        self.a.records.store(built.cid.key, A_ID)
        try:
            self.b.pin_add(str(built.cid))
        except TooLarge as e:
            self.fail(f"an honest directory larger than max_output_entries wasn't pinned: {e}")
        self.assertIn(USER, self.b.storage.pins()[built.cid])
        self.assertTrue(self.b.storage.is_complete(built.cid))
        with self.assertRaises(TooLarge):  # more entries than a write-out may have
            self.b.get(str(built.cid), os.path.join(self.tmp.name, "out"))


if __name__ == "__main__":
    unittest.main()
