"""Tests for add --no-pin (pin=False): added content is cache, evicted like a cached fetch."""
import io
import os
import tempfile
import unittest

import RNS

from rnfs.config import Config
from rnfs.content.cid import Cid
from rnfs.content.storage import USER
from rnfs.node import RnfsNode

try:
    from tests.test_names import FakeNetwork
except ImportError:  # run from inside tests/
    from test_names import FakeNetwork


class PinFalseTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)

    def make_node(self, name: str, **settings) -> RnfsNode:
        """An offline node."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, name), slow="no", **settings)
        node = RnfsNode.offline(config, bytes([5] * 16), None, FakeNetwork())
        self.addCleanup(node.stop)
        return node

    def test_stream_is_cache_not_pin(self):
        node = self.make_node("a", provide_strategy="pinned")
        cid = node.add_stream(io.BytesIO(b"temporary"), pin=False)["cid"]
        parsed = Cid.parse(cid)
        self.assertNotIn(parsed, node.storage.pins())
        self.assertIn(parsed, node.storage.cached_targets())
        self.assertEqual(b"".join(node.read(cid)), b"temporary")
        self.assertNotIn(parsed, node.publisher.provided())  # pinned strategy: cache is never provided
        node.gc()
        self.assertFalse(node.storage.has(parsed))

    def test_path_and_pinned_default(self):
        node = self.make_node("a")
        path = os.path.join(self.tmp.name, "f")
        with open(path, "wb") as f:
            f.write(os.urandom(200_000))
        unpinned = Cid.parse(node.add_path(path, pin=False)["cid"])
        self.assertNotIn(unpinned, node.storage.pins())
        self.assertTrue(node.storage.is_complete(unpinned))
        pinned = Cid.parse(node.add_path(path)["cid"])
        self.assertEqual(pinned, unpinned)
        self.assertEqual(node.storage.pins()[pinned], {USER})

    def test_roots_strategy_provides_it_while_cached(self):
        node = self.make_node("r", provide_strategy="roots")
        cid = Cid.parse(node.add_stream(io.BytesIO(b"share while cached"), pin=False)["cid"])
        self.assertIn(cid, node.publisher.provided())
        node.gc()
        self.assertNotIn(cid, node.publisher.provided())

    def test_evicted_by_max_storage(self):
        node = self.make_node("e", max_storage=300_000)
        first = Cid.parse(node.add_stream(io.BytesIO(os.urandom(200_000)), pin=False)["cid"])
        second = Cid.parse(node.add_stream(io.BytesIO(os.urandom(200_000)), pin=False)["cid"])
        self.assertFalse(node.storage.is_complete(first))  # least recently used goes first
        self.assertTrue(node.storage.is_complete(second))


if __name__ == "__main__":
    unittest.main()
