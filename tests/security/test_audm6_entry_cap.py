"""AUD-M6 (0.2.1 audit): pin add, remote pins and dir set / dir rm fetched DAGs with no entry cap (an N1 variant).

The attack: a directory tree whose every entry is an empty file declares 0 bytes of content, and directory nodes pay
for themselves under the DAG byte bound (rnfs.fetcher.node_credit), so a tree of any number of entries passes it.
get, export and listings stop at max_output_entries (fetch_dag's max_entries), but pin_add, the remote-pin fetch and
dir_set / dir_rm called fetch_dag without it: the audit's repro pinned 200,000 entries, which get refuses at 100,000,
at about 1.4 KB of memory per entry, and a pinned fetch ignores max_storage.

The tests rebuild it with two offline nodes (tests/security/_content_helpers.py): A serves a 0-byte tree of SUBDIRS
directories of PER_DIR directories that each hold one empty file, more entries than B's max_output_entries
(MAX_ENTRIES), each directory a distinct block. B pins it with pin_add, as
a remote pin, and edits it (and with it) with dir_set and dir_rm.

The fix: those fetch_dag calls pass config.max_output_entries (rnfs/node_pins.py: pin_add, _remote_pin_fetch;
rnfs/node.py: dir_set, dir_rm), so each stops with TooLarge once more entries are met, before fetching the rest; a
remote pin refused so is dropped as too_large, not as over its quota (rnfs/fetcher.py, _Walk.met marks the error).
"""
import unittest

from rnfs.content import dag
from rnfs.content.cid import DAG, EMPTY_FILE, Cid
from rnfs.content.storage import remote_source
from rnfs.errors import TooLarge
from rnfs.remote_pins import DROPPED
from tests.security._content_helpers import TwoNodeTest

MAX_ENTRIES = 50  # B's max_output_entries
SUBDIRS = 6
PER_DIR = 40  # directories per subdirectory: 240 in all, each holding one empty file
IDENTITY = bytes(range(16))


class EntryCapTest(TwoNodeTest):
    """A serves a 0-byte tree of SUBDIRS × PER_DIR directories of one empty file; B's max_output_entries is
    MAX_ENTRIES."""

    b_settings = {"max_output_entries": MAX_ENTRIES, "remote_pin_identities": [IDENTITY.hex()]}

    def setUp(self):
        """The tree, served by A."""
        super().setUp()
        self.blocks: dict[Cid, bytes] = {EMPTY_FILE: b""}
        subdirs = [self.node(dag.DirectoryNode([dag.Entry(f"{i}-{j:03d}", self.leaf(f"{i}-{j}"), 0, dag.KIND_DIRECTORY)
                                                for j in range(PER_DIR)])) for i in range(SUBDIRS)]
        self.root = self.node(dag.DirectoryNode([dag.Entry(f"d{i}", cid, 0, dag.KIND_DIRECTORY)
                                                 for i, cid in enumerate(subdirs)]))
        self.serve(self.blocks, self.root)
        self.small = self.node(dag.DirectoryNode([dag.Entry("f", EMPTY_FILE, 0, dag.KIND_FILE)]))
        self.serve(self.blocks, self.small)

    def leaf(self, name: str) -> Cid:
        """A directory holding one empty file, called name (so each is a distinct block)."""
        return self.node(dag.DirectoryNode([dag.Entry(name, EMPTY_FILE, 0, dag.KIND_FILE)]))

    def node(self, node) -> Cid:
        """Encode node and keep its block."""
        block = dag.encode(node)
        cid = Cid.for_block(DAG, block)
        self.blocks[cid] = block
        return cid

    def assert_stopped_early(self):
        """B stopped fetching the tree well before its end: at the second subdirectory, about two of them in."""
        self.assertLess(self.net.downloaded_blocks, 1 + SUBDIRS + 2 * PER_DIR,
                        f"{self.net.downloaded_blocks} of the tree's {len(self.blocks)} blocks downloaded")

    def test_pin_add(self):
        # AUD-M6: fails until pin_add passes max_output_entries (the whole tree was pinned)
        with self.assertRaises(TooLarge) as caught:
            self.b.pin_add(str(self.root))
        self.assertIn("max_output_entries", str(caught.exception))
        self.assertNotIn(self.root, self.b.storage.pins())
        self.assert_stopped_early()

    def test_remote_pin(self):
        source = remote_source(IDENTITY)
        self.b._remote_pin_fetch(self.root, source)
        # AUD-M6: fails until the remote-pin fetch passes max_output_entries (pinned for IDENTITY)
        self.assertNotIn(self.root, self.b.storage.pins())
        self.assert_stopped_early()
        served = [pin for pin in self.b.pin_remote_served()["pins"] if pin["cid"] == str(self.root)]
        self.assertEqual([(pin["state"], pin["reason"]) for pin in served], [(DROPPED, "too_large")])
        self.assertIn("max_output_entries", served[0]["detail"])

    def test_dir_set_and_dir_rm(self):
        for edit in (lambda: self.b.dir_set(str(self.root), "new", str(self.small)),
                     lambda: self.b.dir_set(str(self.small), "big", str(self.root)),
                     lambda: self.b.dir_rm(f"{self.root}/d0")):
            self.net.reset()
            # AUD-M6: fails until dir_set and dir_rm pass max_output_entries (the edit went through)
            with self.assertRaises(TooLarge):
                edit()
            self.assert_stopped_early()
        self.assertEqual(self.b.storage.pins(), {})


if __name__ == "__main__":
    unittest.main()
