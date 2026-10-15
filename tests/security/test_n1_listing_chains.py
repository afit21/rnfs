"""N1-R2: fetch_listing (`ls`, the gateway's directory listings) counted index children per level, not in total, so
chains of single-child index nodes down to MAX_DEPTH were never counted: `ls` of 40 entries downloaded 751 KB in
2,441 blocks, and with more chains about 1.9 GB.

A serves a directory of 40 entries, each below a chain of 20 index nodes of one child; B lists it. The listing is
now bounded like a whole-DAG fetch of size 0: its directory and index nodes pay for themselves and bring a few bytes
per entry or child, but an index node of one child costs 48 bytes, so it must be refused with TooLarge having
downloaded at most the root, dag_bound(0) and about one block more. Honest listings (a wide directory with index nodes and the longest names, one of exactly
max_output_entries entries, one of two index levels) still list in full.
"""
import os
import unittest

from rnfs.constants import MAX_BLOCK_SIZE, MAX_CHILDREN
from rnfs.content import builder, dag
from rnfs.content.cid import EMPTY_FILE, Cid
from rnfs.content.storage import USER
from rnfs.errors import TooLarge
from rnfs.fetcher import dag_bound
from tests.security._content_helpers import TwoNodeTest
from tests.security._fetchr2_helpers import Blocks
from tests.test_node import A_ID

CHAINS = 40
LEVELS = 20  # the reviewer used 60 (to MAX_DEPTH); fewer keep the test fast and still far over the bound


def name(i: int, length: int = 255) -> str:
    """A distinct entry name of length bytes."""
    return f"{i:05d}".ljust(length, "n")


class IndexChainListingTest(TwoNodeTest):
    """ls of a directory whose entries each sit below a chain of single-child index nodes."""

    def test_ls_of_index_chains_is_refused(self):
        blocks = Blocks()
        tops = [(name(i), blocks.index_chain(name(i), LEVELS)) for i in range(CHAINS)]
        root = blocks.node(dag.IndexNode([dag.IndexChild(entry, cid, 0) for entry, cid in tops]))
        self.serve(blocks.blocks, root)
        with self.assertRaises(TooLarge, msg="the listing finished: index chains aren't bounded") as caught:
            self.b.ls(str(root))
        self.assertIn("refusing it", str(caught.exception))
        allowed = len(blocks.blocks[root]) + dag_bound(0) + MAX_BLOCK_SIZE
        self.assertLessEqual(self.net.downloaded_bytes, allowed,
                             f"{self.net.downloaded_bytes} bytes downloaded in {self.net.downloaded_blocks} blocks")


class HonestListingTest(TwoNodeTest):
    """Directories the builder writes list in full."""

    b_settings = {"max_output_entries": 3 * MAX_CHILDREN + 1}

    def build(self, count: int, length: int) -> Cid:
        """A directory of count empty files with names of length bytes, built on A and advertised."""
        with self.a.storage.protect() as session:
            empty = builder.build_bytes(b"", session.put).cid
            built = builder.build_directory([dag.Entry(name(i, length), empty, 0, dag.KIND_FILE)
                                             for i in range(count)], session.put)
            self.a.storage.pin(built.cid, USER)
        self.a.publisher.refresh()
        self.a.records.store(built.cid.key, A_ID)
        return built.cid

    def test_wide_directory_with_the_longest_names(self):
        cid = self.build(3 * MAX_CHILDREN + 1, 255)  # four groups under an index node, max_output_entries in all
        self.assertIsInstance(dag.decode(self.a.storage.get(cid)), dag.IndexNode)
        entries = self.b.ls(str(cid))
        self.assertEqual([entry["name"] for entry in entries], [name(i) for i in range(3 * MAX_CHILDREN + 1)])
        self.assertEqual({entry["cid"] for entry in entries}, {str(EMPTY_FILE)})

    def test_small_directories(self):
        for count, length in [(0, 1), (1, 1), (MAX_CHILDREN, 1), (MAX_CHILDREN + 1, 5)]:
            self.assertEqual(len(self.b.ls(str(self.build(count, length)))), count)

    def test_gateway_listing(self):
        path = os.path.join(self.tmp.name, "tree")
        os.makedirs(os.path.join(path, "sub"))
        for i in range(MAX_CHILDREN + 2):
            with open(os.path.join(path, name(i, 40)), "wb") as f:
                f.write(b"%d" % i)
        cid = Cid.parse(self.a.add_path(path)["cid"])
        self.a.records.store(cid.key, A_ID)
        self.assertEqual(len(self.b.ls(str(cid), for_gateway=True)), MAX_CHILDREN + 3)


class TwoIndexLevelsTest(TwoNodeTest):
    """A directory of MAX_CHILDREN^2 + 1 entries: a root index node above two (one of a single child) above the
    groups, all met before any entry pays its share."""

    b_settings = {"max_output_entries": MAX_CHILDREN * MAX_CHILDREN + 1}

    def test_ls_and_pin(self):
        count = MAX_CHILDREN * MAX_CHILDREN + 1
        with self.a.storage.protect() as session:
            empty = builder.build_bytes(b"", session.put).cid
            built = builder.build_directory([dag.Entry(f"{i:06d}", empty, 0, dag.KIND_FILE) for i in range(count)],
                                            session.put)
            self.a.storage.pin(built.cid, USER)
        self.a.publisher.refresh()
        self.a.records.store(built.cid.key, A_ID)
        self.assertEqual(len(self.b.ls(str(built.cid))), count)
        self.b.pin_add(str(built.cid))
        self.assertTrue(self.b.storage.is_complete(built.cid))


if __name__ == "__main__":
    unittest.main()
