"""L3: a file node may list children of size 0, so a file that declares 1 byte could make `get` download an unbounded
DAG of empty file nodes.

B gets, through the real Fetcher and default config limits, a root served by A whose content is b"x" but whose DAG
holds about 2 MB of distinct file nodes made only of empty chunks. Before the fix `get` downloaded every one of them
(and, with a small max_storage, evicted an unrelated cached fetch to make room). fetch_dag now bounds the DAG's
bytes by what the declared size can hold (rnfs.fetcher.dag_bound), so the get is refused with TooLarge after the
root, and reading the same file the way `cat` does still fetches 2 blocks.
"""
import io
import os
import unittest

from rnfs.constants import CHUNK_SIZE
from rnfs.content.cid import Cid
from rnfs.errors import RnfsError, TooLarge
from tests.security._content_helpers import SLACK, TwoNodeTest
from tests.test_node import A_ID


class ZeroSizeChildrenTest(TwoNodeTest):
    """A 1-byte file whose DAG is mostly empty file nodes, fetched by B from A."""

    # Small enough that 2 MB of empty file nodes doesn't fit beside the cached file below
    b_settings = {"max_storage": 1024 * 1024}

    def setUp(self):
        """A serves the bomb, and an unrelated 192 KiB file B has already fetched (a cached fetch)."""
        super().setUp()
        self.bomb = self.serve_bomb()
        self.honest = self.a.add_stream(io.BytesIO(os.urandom(3 * CHUNK_SIZE)))["cid"]
        self.a.records.store(Cid.parse(self.honest).key, A_ID)
        self.b.get(self.honest, os.path.join(self.tmp.name, "honest"))
        self.assertTrue(self.honest_cached())
        self.net.reset()

    def honest_cached(self) -> bool:
        """True while B still holds the unrelated file as a complete cached fetch."""
        cid = Cid.parse(self.honest)
        return cid in self.b.storage.cached_targets() and self.b.storage.is_complete(cid)

    def test_get_is_refused_without_evicting_the_cache(self):
        self.assertEqual(self.b.info(str(self.bomb.root))["size"], 1)  # what info and the gateway show
        out = os.path.join(self.tmp.name, "out")
        with self.assertRaises(TooLarge) as caught:
            self.b.get(str(self.bomb.root), out)
        self.assertIn("refusing it", str(caught.exception))
        self.assertFalse(os.path.lexists(out))
        # Only the root came over the network (for info): none of the empty file nodes below it
        self.assertEqual(self.net.downloaded_blocks, 1)
        self.assertLess(self.net.downloaded_bytes, self.bomb.size + SLACK)
        # Nothing needed room, so the unrelated cached fetch is still there
        self.assertTrue(self.honest_cached())

    def test_read_range_fetches_two_blocks(self):
        self.assertEqual(b"".join(self.b.read(str(self.bomb.root))), b"x")
        self.assertEqual(self.net.downloaded_blocks, 2)  # the root and the 1-byte chunk: empty subtrees skipped
        self.assertTrue(self.honest_cached())

    def test_get_downloads_about_the_declared_size(self):
        out = os.path.join(self.tmp.name, "out")
        try:
            self.b.get(str(self.bomb.root), out)
        except RnfsError:
            pass  # refusing the root is as safe as writing it out lazily
        self.assertLessEqual(self.net.downloaded_bytes, self.bomb.size + SLACK,
                             f"get of a root declaring {self.bomb.size} byte downloaded "
                             f"{self.net.downloaded_bytes} bytes in {self.net.downloaded_blocks} blocks")


if __name__ == "__main__":
    unittest.main()
