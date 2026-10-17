"""DHT-4: in private mode, name records are still announced network-wide, so outsiders learn members' [sequence,
CID] records and can link a name's changes; only the content stays member-only. Nothing told the user.

The announce itself is unchanged (the wire stays as it is): `name publish` in private mode now warns. The result of
NameManager.publish (and so of node.name_publish and POST /api/name/publish) gains an optional `warning` key, which
the CLI prints to stderr; outside private mode the result is as before, with no such key.
"""
import io
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.content.cid import RAW, Cid
from rnfs.names import NameManager
from rnfs.node import RnfsNode
from tests.support import FakeClock
from tests.test_cli import CliTestCase
from tests.test_names import FakeNetwork, FakePins

CID = Cid.for_block(RAW, b"a member's page")
MEMBER = "ab" * 16


class PrivateNamePublishTest(unittest.TestCase):
    """A NameManager in a temporary rnfs dir, public or private."""

    def setUp(self):
        """The temporary directory; RNS's log silenced."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def publish(self, **settings) -> dict:
        """Create a name and publish CID under it with these settings; the result."""
        config = Config(rnfs_dir=self.tmp.name, path_timeout=0.2, **settings)
        clock = FakeClock(1_000_000.0)
        manager = NameManager(config, FakePins(), network=FakeNetwork(), clock=clock, monotonic=clock)
        self.addCleanup(manager.close)
        manager.create("site")
        return manager.publish("site", CID)

    def test_private_mode_publish_warns(self):
        result = self.publish(private=True, members=[MEMBER])
        self.assertIn("warning", result)
        self.assertIn("private", result["warning"])
        self.assertIn("outside", result["warning"])
        self.assertEqual(result["cid"], str(CID))  # published all the same

    def test_public_publish_result_is_unchanged(self):
        result = self.publish()
        self.assertEqual(set(result), {"name", "cid", "sequence"})

    def test_node_passes_the_warning_on(self):
        config = Config(rnfs_dir=os.path.join(self.tmp.name, "node"), slow="no", private=True, members=[MEMBER])
        node = RnfsNode.offline(config, bytes([0xA] * 16), None, FakeNetwork())
        self.addCleanup(node.stop)
        node.name_create("site")
        cid = node.add_stream(io.BytesIO(b"a member's page"))["cid"]
        self.assertIn("warning", node.name_publish("site", cid))


class PrivateNamePublishCliTest(CliTestCase):
    """`rnfs name publish` prints the result's warning to stderr."""

    def test_cli_prints_the_warning(self):
        real = self.node.name_publish

        def warning_publish(alias, target):
            """FakeNode.name_publish, as a private node answers."""
            return {**real(alias, target), "warning": "name announces are visible outside the private network"}

        cid = self.node.put_file(b"a member's page")
        self.ok("name", "create", "site")
        with mock.patch.object(self.node, "name_publish", warning_publish):
            code, out, err = self.run_cli("name", "publish", "site", cid)
        self.assertEqual(code, 0, err)
        self.assertIn("published", out.decode())
        self.assertIn("rnfs: warning: name announces are visible outside the private network", err)

    def test_cli_prints_no_warning_without_one(self):
        cid = self.node.put_file(b"a member's page")
        self.ok("name", "create", "site")
        code, out, err = self.run_cli("name", "publish", "site", cid)
        self.assertEqual((code, err), (0, ""))


if __name__ == "__main__":
    unittest.main()
