"""AUD-M11: `get` checked the output's free space against the target's declared content bytes (RnfsNode.get,
_check_free_space), not what writing it out allocates. Every directory and file written takes at least one allocation
unit (4 KiB on most filesystems, far more on FAT32) whatever its size, so a DAG of many empty directories or tiny files,
up to max_output_entries, passed a check for a few bytes and then filled the output's filesystem below the disk
reserve: 5,000 empty directories were checked as 0 bytes and took 20 MB, 100,000 about 400 MiB. Repeated subtrees make
such a DAG a few KiB to fetch.

The test builds such DAGs on an offline node (empty directories; one-byte files sharing one block; a subdirectory
repeated under many names) and gets them with the disk, by a mock of shutil.disk_usage, holding the reserve and half
of what their entries allocate: the old check passed it and wrote them all. Now, once the DAG is stored and before
anything is written, get counts what the write-out allocates as the sealed unpack does (REV10: tarstream.Extent.written,
each file rounded up to the output's allocation unit, each directory's listing at least one unit, every occurrence
of a repeated subtree) and refuses it (TooLarge) leaving nothing behind; with room for it, it goes ahead.
"""
import os
import shutil
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs import fsutil
from rnfs.config import Config
from rnfs.content import builder, dag
from rnfs.content.cid import EMPTY_DIRECTORY, EMPTY_DIRECTORY_BLOCK, RAW, Cid
from rnfs.content.storage import disk_reserve
from rnfs.errors import TooLarge
from rnfs.node import RnfsNode
from tests.test_names import FakeNetwork
from tests.test_node import A_ID, NetClient

ENTRIES = 2000
TOTAL = 1024 ** 3  # the mock disk's size


class GetAllocationTest(unittest.TestCase):
    """An offline node holding DAGs of many tiny entries, on a 1 GiB disk the test says is nearly full."""

    def setUp(self):
        """The node."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        config = Config(rnfs_dir=os.path.join(self.tmp.name, "a"), slow="no", lookup_timeout=1, path_timeout=0.2)
        self.node = RnfsNode.offline(config, A_ID, NetClient(), FakeNetwork())
        self.addCleanup(self.node.stop)
        self.out = os.path.join(self.tmp.name, "out")  # each subtest's output: this and its name
        self.unit = fsutil.allocation_unit(self.tmp.name)

    def store(self, entries: list[dag.Entry], extra: dict[Cid, bytes]) -> str:
        """A directory of entries built and pinned on the node, the blocks extra stored first; its CID."""
        with self.node.storage.protect() as session:
            for cid, block in extra.items():
                session.put(cid, block)
            built = builder.build_directory(entries, session.put)
            self.node.storage.pin(built.cid, "user")
        return str(built.cid)

    def empty_directories(self) -> str:
        """ENTRIES empty directories."""
        return self.store([dag.Entry(f"d{i:05d}", EMPTY_DIRECTORY, 0, dag.KIND_DIRECTORY) for i in range(ENTRIES)],
                          {EMPTY_DIRECTORY: EMPTY_DIRECTORY_BLOCK})

    def tiny_files(self) -> str:
        """ENTRIES one-byte files, all one block: ENTRIES bytes declared."""
        block = b"x"
        cid = Cid.for_block(RAW, block)
        return self.store([dag.Entry(f"f{i:05d}", cid, 1, dag.KIND_FILE) for i in range(ENTRIES)], {cid: block})

    def repeated_subtree(self) -> str:
        """A directory of 50 empty directories, under ENTRIES // 50 names: a few blocks, ENTRIES entries and more."""
        inner = self.store([dag.Entry(f"d{i:02d}", EMPTY_DIRECTORY, 0, dag.KIND_DIRECTORY) for i in range(50)],
                           {EMPTY_DIRECTORY: EMPTY_DIRECTORY_BLOCK})
        return self.store([dag.Entry(f"r{i:03d}", Cid.parse(inner), 0, dag.KIND_DIRECTORY)
                           for i in range(ENTRIES // 50)], {})

    def disk(self, room: int):
        """A mock disk_usage: a 1 GiB filesystem with its reserve and room bytes more free."""
        usage = shutil.disk_usage(self.tmp.name)._replace(total=TOTAL, free=disk_reserve(TOTAL) + room)
        return mock.patch("shutil.disk_usage", return_value=usage)

    def test_entries_count_what_they_allocate(self):
        for make in (self.empty_directories, self.tiny_files, self.repeated_subtree):
            with self.subTest(make.__name__):
                cid, out = make(), f"{self.out}-{make.__name__}"
                with self.disk(ENTRIES * self.unit // 2), self.assertRaises(TooLarge) as caught:
                    self.node.get(cid, out)
                self.assertIn("free", str(caught.exception))
                self.assertFalse(os.path.lexists(out))
                self.assertEqual([name for name in os.listdir(self.tmp.name) if name.startswith(".rnfs-")], [])

    def test_with_room_for_them_the_get_goes_ahead(self):
        for make in (self.empty_directories, self.tiny_files):
            with self.subTest(make.__name__):
                cid, out = make(), f"{self.out}-{make.__name__}"
                with self.disk(4 * ENTRIES * self.unit):
                    self.node.get(cid, out)
                self.assertEqual(len(os.listdir(out)), ENTRIES)


if __name__ == "__main__":
    unittest.main()
