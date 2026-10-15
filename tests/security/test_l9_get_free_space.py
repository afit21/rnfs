"""L9: `get` checked the free space of the output's filesystem without the disk reserve, so a get could fill the disk
to nothing free (the block store keeps the reserve; the write-out didn't).

A node gets a file it already holds completely (so nothing is fetched) while the disk, by a mock, has just under the
file's size plus the reserve free: the get is refused, where it used to go ahead and leave 1 byte free. With the
reserve to spare it goes ahead. A node fetching the file first, onto the same disk, also needs room for the blocks
still to fetch: it is refused before fetching anything below the root.
"""
import io
import os
import shutil
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE, KIB
from rnfs.content import storage as storage_module
from rnfs.content.cid import Cid
from rnfs.errors import StorageFull, TooLarge
from rnfs.node import RnfsNode
from tests.security._content_helpers import TwoNodeTest
from tests.test_names import FakeNetwork
from tests.test_node import A_ID, NetClient

FILE_SIZE = 3 * CHUNK_SIZE


class GetFreeSpaceTest(unittest.TestCase):
    """An offline node holding a 192 KiB file, on a 1 GiB disk the test says is nearly full."""

    def setUp(self):
        """The node and its stored file."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        config = Config(rnfs_dir=os.path.join(self.tmp.name, "a"), slow="no", lookup_timeout=1, path_timeout=0.2)
        self.node = RnfsNode.offline(config, A_ID, NetClient(), FakeNetwork())
        self.addCleanup(self.node.stop)
        self.cid = self.node.add_stream(io.BytesIO(os.urandom(FILE_SIZE)))["cid"]
        self.out = os.path.join(self.tmp.name, "out")

    def disk(self, free: int):
        """A mock disk_usage reporting free bytes on a 1 GiB filesystem (reserve: DISK_RESERVE_MIN)."""
        usage = shutil.disk_usage(self.tmp.name)._replace(total=1024 ** 3, free=free)
        return mock.patch("shutil.disk_usage", return_value=usage)

    def test_get_leaving_the_reserve_free_goes_ahead(self):
        with self.disk(FILE_SIZE + storage_module.DISK_RESERVE_MIN):
            self.node.get(self.cid, self.out)
        self.assertEqual(os.path.getsize(self.out), FILE_SIZE)

    def test_get_keeps_the_disk_reserve_free(self):
        with self.disk(FILE_SIZE + storage_module.DISK_RESERVE_MIN - 1):
            with self.assertRaises((TooLarge, StorageFull), msg="get would leave less than the reserve free"):
                self.node.get(self.cid, self.out)
        self.assertFalse(os.path.lexists(self.out))


class FetchingGetFreeSpaceTest(TwoNodeTest):
    """B gets a 192 KiB file from A; its blocks and the output share one (mock, 1 GiB) disk."""

    def setUp(self):
        """A serves the file."""
        super().setUp()
        self.cid = self.a.add_stream(io.BytesIO(os.urandom(FILE_SIZE)))["cid"]
        self.a.records.store(Cid.parse(self.cid).key, A_ID)
        self.out = os.path.join(self.tmp.name, "out")

    def disk(self, free: int):
        """A mock disk_usage reporting free bytes on a 1 GiB filesystem (reserve: DISK_RESERVE_MIN)."""
        usage = shutil.disk_usage(self.tmp.name)._replace(total=1024 ** 3, free=free)
        return mock.patch("shutil.disk_usage", return_value=usage)

    def test_get_counts_the_blocks_still_to_fetch(self):
        # The blocks still to fetch are estimated as the size less the bytes stored (here the root node's)
        with self.disk(2 * FILE_SIZE + storage_module.DISK_RESERVE_MIN - KIB):
            with self.assertRaises(TooLarge):
                self.b.get(self.cid, self.out)
        self.assertFalse(os.path.lexists(self.out))
        self.assertEqual(self.b.storage.block_count(), 1)  # refused before fetching below the root
        with self.disk(2 * FILE_SIZE + storage_module.DISK_RESERVE_MIN):
            self.b.get(self.cid, self.out)
        self.assertEqual(os.path.getsize(self.out), FILE_SIZE)

if __name__ == "__main__":
    unittest.main()
