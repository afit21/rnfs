"""L9 residual: node._check_free_space returned without a word when it couldn't read the destination's free space
(OSError from disk_usage or stat), so `get` and a sealed write-out went ahead with no check at all: it failed open.

An offline node holding a small file gets it while disk_usage, by a mock, raises PermissionError: the get must be
refused, saying it couldn't check the free space, and leave no output, where it used to write the file.
"""
import io
import os
import shutil
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.node import RnfsNode
from tests.test_names import FakeNetwork
from tests.test_node import A_ID, NetClient


class FreeSpaceUnknownTest(unittest.TestCase):
    """An offline node with a stored file, whose destination filesystem can't be queried."""

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
        self.cid = self.node.add_stream(io.BytesIO(b"hello"))["cid"]
        self.out = os.path.join(self.tmp.name, "out")

    def test_get_is_refused(self):
        real = shutil.disk_usage

        def unreadable(path):
            """disk_usage, failing for the destination's directory."""
            if os.path.abspath(path) == self.tmp.name:
                raise PermissionError(13, "Permission denied")
            return real(path)

        with mock.patch("shutil.disk_usage", unreadable):
            with self.assertRaises(OSError, msg="get went ahead without checking the free space") as caught:
                self.node.get(self.cid, self.out)
        self.assertIn("free space", str(caught.exception))
        self.assertFalse(os.path.lexists(self.out))
        self.assertEqual(os.listdir(self.tmp.name), ["a"])  # no partial output either


if __name__ == "__main__":
    unittest.main()
