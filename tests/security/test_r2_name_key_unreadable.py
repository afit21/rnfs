"""R-2: a name key file that exists but can't be read at start was treated as deleted, releasing the name's pin
and dropping its saved record (then in names.json).

NameManager skipped a key file RNS couldn't load (no permission, damaged), so its alias had no key; its saved
record then counted as a key "deleted by hand" (release_gone() released the pin and the save dropped the record)
and reconcile_pins released any pin of an alias with no key. Once the file could be read again the name was back
with no target and sequence 0. The fixed manager treats only a missing key file as removed: an unreadable one
keeps its pins, and every save leaves its names_published row as it is.

Offline node (RnfsNode.offline over tests.test_names' FakeNetwork), restarted on the same directory.
"""
import io
import os
import tempfile
import unittest

import RNS

from rnfs.config import Config
from rnfs.content.cid import Cid
from rnfs.node import RnfsNode
from tests.support import IS_ROOT, state_rows
from tests.test_names import FakeNetwork
from tests.test_node import A_ID, NetClient

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class UnreadableNameKeyTest(unittest.TestCase):
    """A node publishes name "site", then restarts while its key file can't be read."""

    def setUp(self):
        """The node, with "site" published at a target pinned only by the name."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = Config(rnfs_dir=os.path.join(self.tmp.name, "a"), slow="no")
        self.node = self.open()
        self.cid = Cid.parse(self.node.add_stream(io.BytesIO(b"site v1"), pin=False)["cid"])
        self.node.names.create("site")
        self.node.names.publish("site", self.cid)
        self.key = self.node.names._key_path("site")
        self.recorded = self.published()["site"]
        self.assertEqual(self.recorded, ("1", self.cid.binary, 0))

    def open(self) -> RnfsNode:
        """The node, opened (again) on its directory."""
        net = NetClient()
        node = RnfsNode.offline(self.config, A_ID, net, FakeNetwork())
        net.nodes[A_ID] = node
        self.addCleanup(node.stop)
        return node

    def restart(self) -> RnfsNode:
        """Stop the node and open it again."""
        self.node.stop()
        self.node = self.open()
        return self.node

    def published(self) -> dict:
        """The names_published rows: alias -> (sequence, cid, imported)."""
        rows = state_rows(self.config.state_dir, "SELECT alias, sequence, cid, imported FROM names_published")
        return {alias: tuple(rest) for alias, *rest in rows}

    def check_kept(self, make_unreadable, make_readable):
        """While the key can't be read the pin and record stay (across a save); once readable, the name is back."""
        make_unreadable()
        node = self.restart()
        # R-2: fails until only a missing key file counts as a removed name
        self.assertEqual(node.storage.pinned_by("name:site"), {self.cid},
                         "an unreadable key file released the name's pin")
        node.names.create("other")  # saves the records
        self.assertEqual(self.published().get("site"), self.recorded, "the name's record was dropped")
        make_readable()
        node = self.restart()
        self.assertEqual(node.storage.pinned_by("name:site"), {self.cid})
        site = next(entry for entry in node.names.list() if entry["alias"] == "site")
        self.assertEqual((site["cid"], site["sequence"]), (str(self.cid), 1))

    @unittest.skipIf(IS_ROOT, "root reads files without permission")
    @unittest.skipIf(os.name == "nt", "chmod can't make a file unreadable on Windows")
    def test_key_without_read_permission(self):
        """The key file's permissions taken away, then given back."""
        self.addCleanup(os.chmod, self.key, 0o600)
        self.check_kept(lambda: os.chmod(self.key, 0), lambda: os.chmod(self.key, 0o600))

    def test_damaged_key(self):
        """The key file's bytes replaced (too short to be a key), then restored."""
        with open(self.key, "rb") as f:
            good = f.read()

        def write(data):
            """Write data as the key file."""
            with open(self.key, "wb") as f:
                f.write(data)

        self.check_kept(lambda: write(b"\x00" * 7), lambda: write(good))

    def test_deleted_key_still_releases(self):
        """A key file deleted by hand is still a removed name: its pin is released and its record dropped."""
        os.remove(self.key)
        node = self.restart()
        self.assertEqual(node.storage.pinned_by("name:site"), set())
        self.assertNotIn("site", self.published())


if __name__ == "__main__":
    unittest.main()
