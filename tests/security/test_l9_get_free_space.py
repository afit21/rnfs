"""L9: `get` checks the free space of the output's filesystem without the disk reserve, so a get can fill the disk to
nothing free (the block store keeps the reserve; the write-out doesn't).

A node gets a file it already holds completely (so nothing is fetched) while the disk, by a mock, has just 1 byte
more free than the file needs: the get goes ahead, although writing the file leaves 1 byte free. The fixed behaviour
refuses a get whose output wouldn't leave the disk reserve free.
"""
import io
import os
import shutil
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.content import storage as storage_module
from rnfs.errors import StorageFull, TooLarge
from rnfs.node import RnfsNode
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

    def test_get_leaves_less_than_the_reserve_free(self):
        with self.disk(FILE_SIZE + 1):
            self.node.get(self.cid, self.out)
        self.assertEqual(os.path.getsize(self.out), FILE_SIZE)  # written, leaving 1 byte free

    # L9: fails until get's free-space check requires the size plus the disk reserve
    @unittest.expectedFailure
    def test_get_keeps_the_disk_reserve_free(self):
        with self.disk(FILE_SIZE + storage_module.DISK_RESERVE_MIN - 1):
            with self.assertRaises((TooLarge, StorageFull), msg="get would leave less than the reserve free"):
                self.node.get(self.cid, self.out)
        self.assertFalse(os.path.lexists(self.out))


if __name__ == "__main__":
    unittest.main()
