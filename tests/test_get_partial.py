"""node.get writes out under a temporary name, so a failed write-out never leaves a partial output behind."""
import os
import tempfile
import unittest
from unittest import mock

from rnfs.config import Config
from rnfs.content import walker
from rnfs.errors import TooLarge
from rnfs.node import RnfsNode


class GetPartialTest(unittest.TestCase):
    """A get that fails part-way leaves neither the output nor its temporary file."""

    def setUp(self):
        """An offline node holding a small directory tree."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name
        self.node = RnfsNode.offline(Config(rnfs_dir=os.path.join(self.dir, "node")), b"\x01" * 16,
                                     client=mock.Mock(), names_network=mock.Mock())
        self.addCleanup(self.node.stop)
        src = os.path.join(self.dir, "src")
        os.makedirs(os.path.join(src, "sub"))
        for i in range(5):
            with open(os.path.join(src, "sub", f"f{i}"), "wb") as f:
                f.write(os.urandom(1000))
        self.cid = self.node.add_path(src)["cid"]
        self.out = os.path.join(self.dir, "out")

    def test_failed_write_out_leaves_nothing(self):
        real = walker.write_out

        def write_then_fail(expect, get, dest, max_entries, **kwargs):
            """Write the whole tree, then fail as a late error would."""
            real(expect, get, dest, max_entries, **kwargs)
            raise TooLarge("failed after writing")

        with mock.patch.object(walker, "write_out", write_then_fail):
            with self.assertRaises(TooLarge):
                self.node.get(self.cid, self.out)
        self.assertEqual(sorted(os.listdir(self.dir)), ["node", "src"])

    def test_get_writes_the_output(self):
        self.node.get(self.cid, self.out)
        self.assertEqual(sorted(os.listdir(os.path.join(self.out, "sub"))), [f"f{i}" for i in range(5)])
        self.assertEqual(sorted(os.listdir(self.dir)), ["node", "out", "src"])

    def test_existing_output_is_refused(self):
        os.mkdir(self.out)
        with self.assertRaises(ValueError):
            self.node.get(self.cid, self.out)
