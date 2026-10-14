"""N1 (core part): a user-path pin (`node.pin_add`, which `rnfs pin add` runs as a job) sets no max_dag_bytes,
unlike a remote /pin or an LXMF auto-fetch, so pinning a root that declares 1 byte downloads and pins its whole DAG.

B pins a root served by A whose content is b"x" but whose DAG holds about 2 MB of distinct file nodes made only of
empty chunks (as in L3): every block is downloaded and kept under the "user" pin. The fixed behaviour bounds the DAG
bytes of a user pin by a cap derived from the declared size, as the remote /pin path already does with its quota.
"""
import unittest

from rnfs.content.storage import USER
from rnfs.errors import RnfsError
from tests.security._content_helpers import SLACK, TwoNodeTest


class UserPinTest(TwoNodeTest):
    """B runs pin_add on a 1-byte file whose DAG is mostly empty file nodes."""

    def setUp(self):
        """A serves the bomb."""
        super().setUp()
        self.bomb = self.serve_bomb()

    def test_pin_add_downloads_and_pins_every_empty_file_node(self):
        self.assertEqual(self.b.info(str(self.bomb.root))["size"], 1)  # what `rnfs stat` and pages check
        self.b.pin_add(str(self.bomb.root))
        self.assertIn(USER, self.b.storage.pins()[self.bomb.root])
        self.assertEqual(self.net.downloaded_blocks, len(self.bomb.blocks))
        self.assertEqual(self.net.downloaded_bytes, self.bomb.total_bytes())
        self.assertGreater(self.net.downloaded_bytes, 8 * (self.bomb.size + SLACK))
        # All of it is kept for good, and provided
        self.assertEqual(self.b.storage.pinned_bytes(), self.bomb.total_bytes())
        self.assertIn(self.bomb.root, self.b.publisher.provided())

    # N1: fails until pin_add caps a pin's DAG bytes by a bound derived from the root's declared size
    @unittest.expectedFailure
    def test_pin_add_dag_bytes_are_bounded_by_the_declared_size(self):
        try:
            self.b.pin_add(str(self.bomb.root))
        except RnfsError:
            pass  # refusing the pin is the expected outcome
        self.assertLessEqual(self.net.downloaded_bytes, self.bomb.size + SLACK,
                             f"pin_add of a root declaring {self.bomb.size} byte downloaded "
                             f"{self.net.downloaded_bytes} bytes in {self.net.downloaded_blocks} blocks")
        self.assertLessEqual(self.b.storage.pinned_bytes(), self.bomb.size + SLACK)


if __name__ == "__main__":
    unittest.main()
