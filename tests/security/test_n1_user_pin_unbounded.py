"""N1 (core part): a user-path pin (`node.pin_add`, which `rnfs pin add` runs as a job) set no max_dag_bytes, unlike
a remote /pin or an LXMF auto-fetch, so pinning a root that declares 1 byte downloaded and pinned its whole DAG.

B pins a root served by A whose content is b"x" but whose DAG holds about 2 MB of distinct file nodes made only of
empty chunks (as in L3). fetch_dag now bounds the DAG bytes of every fetch, a user pin's included, by what the
declared size can hold (rnfs.fetcher.dag_bound), so the pin is refused with TooLarge and nothing is pinned.
"""
import unittest

from rnfs.errors import RnfsError, TooLarge
from tests.security._content_helpers import SLACK, TwoNodeTest


class UserPinTest(TwoNodeTest):
    """B runs pin_add on a 1-byte file whose DAG is mostly empty file nodes."""

    def setUp(self):
        """A serves the bomb."""
        super().setUp()
        self.bomb = self.serve_bomb()

    def test_pin_add_is_refused_and_pins_nothing(self):
        self.assertEqual(self.b.info(str(self.bomb.root))["size"], 1)  # what `rnfs stat` and pages check
        with self.assertRaises(TooLarge) as caught:
            self.b.pin_add(str(self.bomb.root))
        self.assertIn("refusing it", str(caught.exception))
        self.assertNotIn(self.bomb.root, self.b.storage.pins())
        self.assertNotIn(self.bomb.root, self.b.publisher.provided())
        self.assertEqual(self.net.downloaded_blocks, 1)  # the root, for info
        self.assertEqual(self.b.storage.pinned_bytes(), 0)

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
