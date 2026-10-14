"""I1: a gateway 507 (storage full) carried the StorageFull message as is, which named the local blocks directory
and the disk's free space and reserve.

A real offline node holds a file root whose chunks claim 2**60 bytes each; the gateway's GET of it fails the disk
check before anything is fetched, and the 507's body is read back. The disk usage is fixed by a mock so the sizes in
the message are known. The gateway now sends a generic message for 507, as it does for 500, and the StorageFull
message itself no longer names the directory; it keeps the figures, for the local user's CLI and API.
"""
import os
import shutil
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.content import dag
from rnfs.content import storage as storage_module
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.storage import USER
from rnfs.errors import StorageFull
from rnfs.gateway import GatewayServer
from rnfs.log import size
from rnfs.node import RnfsNode
from tests.test_api import raw_request
from tests.test_names import FakeNetwork
from tests.test_node import A_ID, NetClient

TOTAL = 1024 ** 4  # the mocked filesystem: 1 TiB, 123,456,789,012 bytes free
FREE = 123_456_789_012


class GatewayStorageFullTest(unittest.TestCase):
    """The gateway of an offline node, asked for a file far bigger than its disk."""

    def setUp(self):
        """A node holding a huge root's block, and its gateway on a free port."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.config = Config(rnfs_dir=os.path.join(tmp.name, "a"), slow="no", lookup_timeout=1, path_timeout=0.2)
        self.node = RnfsNode.offline(self.config, A_ID, NetClient(), FakeNetwork())
        self.addCleanup(self.node.stop)
        # Only the root block is needed to know its size: one real chunk, then chunks claiming 2**60 bytes each
        first = b"<p>hi</p>" * 100
        first_cid = Cid.for_block(RAW, first)
        block = dag.encode(dag.FileNode([dag.Child(first_cid, len(first))]
                                        + [dag.Child(Cid.for_block(RAW, b"x"), 2 ** 60)] * 4))
        self.root = Cid.for_block(DAG, block)
        with self.node.storage.protect() as session:
            session.put(self.root, block)
            session.put(first_cid, first)
        self.node.storage.pin(self.root, USER)
        self.server = GatewayServer(self.node, self.config, 0)
        self.server.start()
        self.addCleanup(self.server.stop)
        usage = shutil.disk_usage(tmp.name)._replace(total=TOTAL, free=FREE)
        disk = mock.patch("shutil.disk_usage", return_value=usage)
        disk.start()
        self.addCleanup(disk.stop)
        self.reserve = max(storage_module.DISK_RESERVE_MIN, int(TOTAL * storage_module.DISK_RESERVE_FRACTION))

    def get_root(self) -> tuple[int, str]:
        """(status, body) of the gateway's answer to a GET of the huge root."""
        status, _, body = raw_request(self.server.port, "GET", f"/rnfs/{self.root}")
        return status, body.decode("utf-8", "replace")

    def local_details(self, body: str) -> list[str]:
        """What of the node's directories and disk figures appears in body."""
        details = [self.config.rnfs_dir, self.config.blocks_dir, size(FREE), size(self.reserve)]
        return [detail for detail in details if detail in body]

    def test_507_message_is_generic(self):
        status, body = self.get_root()
        self.assertEqual(status, 507, body)
        self.assertEqual(self.local_details(body), [], f"the 507 body reveals local details: {body!r}")

    def test_storage_full_names_no_path_but_keeps_the_figures(self):
        # What the CLI and API show the local user: free space against the reserve, but not where the blocks are
        with self.assertRaises(StorageFull) as caught:
            self.node.storage.check_disk(2 ** 60)
        message = str(caught.exception)
        self.assertNotIn(self.config.rnfs_dir, message)
        self.assertIn(f"{size(FREE)} free", message)
        self.assertIn(size(self.reserve), message)

if __name__ == "__main__":
    unittest.main()
