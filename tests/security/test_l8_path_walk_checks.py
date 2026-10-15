"""L8 (fix): where directories are walked by path (FD_WALK False, as on Windows), each directory is checked again
whenever it is listed and each file opened below it must really be below the walk's root, so a directory swapped for
a link or junction can't lead the walk outside.

Simulated on any POSIX system with FD_WALK forced to False and a symlink standing in for the junction. Each of the two
checks is tested with the other one disabled, so either alone keeps the outside file out; the Windows-only parts
(reparse tags, GetFinalPathNameByHandleW's prefixes) are tested on their pure logic. Not run on Windows.
"""
import os
import tempfile
import unittest
from unittest import mock

from rnfs import fsutil
from rnfs.content import builder
from rnfs.content.builder import NO_PUT
from rnfs.content.cid import RAW, Cid
from tests.support import CAN_SYMLINK

SECRET = b"TOP SECRET: outside the folder being added"


@unittest.skipUnless(CAN_SYMLINK, "creating symlinks isn't allowed here")
class PathWalkTest(unittest.TestCase):
    """files/sub/harmless.txt being added, and private/secret.txt outside it."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.private = os.path.join(self.tmp.name, "private")
        os.mkdir(self.private)
        with open(os.path.join(self.private, "secret.txt"), "wb") as f:
            f.write(SECRET)
        self.files = os.path.join(self.tmp.name, "files")
        self.sub = os.path.join(self.files, "sub")
        os.makedirs(self.sub)
        with open(os.path.join(self.sub, "harmless.txt"), "wb") as f:
            f.write(b"hi")
        fd_walk = mock.patch.object(builder, "FD_WALK", False)
        fd_walk.start()
        self.addCleanup(fd_walk.stop)

    def swap_sub_when_listed(self):
        """A patch that swaps sub for a symlink to private just as the walk lists sub (after it opened it)."""
        real = builder.list_directory

        def list_then_swap(handle):
            """Swap sub on its first listing, then list it as builder does."""
            if handle == self.sub and not os.path.islink(self.sub):
                os.rename(self.sub, self.sub + ".moved")
                os.symlink(self.private, self.sub)
            return real(handle)

        return mock.patch.object(builder, "list_directory", list_then_swap)

    def add(self) -> tuple[set[Cid], list[str]]:
        """Add files, storing every block; the CIDs stored and the warnings."""
        stored = set()
        _, warnings = builder.add_path(self.files, lambda cid, block: stored.add(cid))
        return stored, warnings

    def assert_secret_not_read(self, stored: set[Cid], warnings: list[str]):
        """Nothing from private was stored, and the walk said it skipped what changed."""
        self.assertNotIn(Cid.for_block(RAW, SECRET), stored, "the secret was read from outside the folder")
        self.assertTrue([w for w in warnings if "changed while being added" in w], warnings)

    def test_swapped_directory_is_skipped(self):
        with self.swap_sub_when_listed():
            self.assert_secret_not_read(*self.add())

    def test_directory_check_alone_keeps_it_out(self):
        # Without the final-path check of each file, the directory's own check refuses the swapped listing
        with self.swap_sub_when_listed(), mock.patch.object(builder, "_check_below"):
            self.assert_secret_not_read(*self.add())

    def test_final_path_check_alone_keeps_it_out(self):
        # Without the directory checks (as if the swap fell between a check and the listing), the file opened
        # through the link has its final path outside the folder
        with self.swap_sub_when_listed(), mock.patch.object(builder, "_check_dir_path"):
            self.assert_secret_not_read(*self.add())

    def test_final_path_check_without_system_support(self):
        # Where the system can't name an open file's path, the path resolved again must lead to it, below the root
        with self.swap_sub_when_listed(), mock.patch.object(builder, "_check_dir_path"), \
                mock.patch.object(fsutil, "final_path", return_value=None):
            self.assert_secret_not_read(*self.add())

    def test_unchanged_tree_still_adds(self):
        # The checks don't refuse an honest tree, nor one named through a symlink to it (the user's choice)
        expected = builder.add_path(self.files, NO_PUT)[0]
        link = os.path.join(self.tmp.name, "link")
        os.symlink(self.files, link, target_is_directory=True)
        self.assertEqual(builder.add_path(link, NO_PUT), (expected, []))

    def test_final_path_names_the_open_file(self):
        path = os.path.join(self.sub, "harmless.txt")
        fd = os.open(path, os.O_RDONLY)
        self.addCleanup(os.close, fd)
        final = fsutil.final_path(fd)
        if final is None:
            self.skipTest("this system can't name an open file's path")
        self.assertEqual(final, os.path.realpath(path))


class WindowsPartsTest(unittest.TestCase):
    """The platform-independent logic of the Windows side."""

    def test_name_surrogate_reparse_points_are_links(self):
        st = os.stat(tempfile.gettempdir())
        for tag, links in ((0xA0000003, True),    # junction (mount point)
                           (0xA000000C, True),    # symlink
                           (0xA000001D, True),    # WSL symlink
                           (0x9000001A, False),   # OneDrive (cloud files) placeholder: the directory itself
                           (0, False)):
            with self.subTest(hex(tag)):
                self.assertEqual(builder.is_link(mock.Mock(st_mode=st.st_mode, st_reparse_tag=tag)), links)

    def test_strip_windows_prefix(self):
        self.assertEqual(fsutil.strip_windows_prefix("\\\\?\\C:\\Users\\a\\x"), "C:\\Users\\a\\x")
        self.assertEqual(fsutil.strip_windows_prefix("\\\\?\\UNC\\srv\\share\\x"), "\\\\srv\\share\\x")
        self.assertEqual(fsutil.strip_windows_prefix("C:\\x"), "C:\\x")

    def test_is_below(self):
        root = os.path.join(tempfile.gettempdir(), "root")
        self.assertTrue(fsutil.is_below(root, root))
        self.assertTrue(fsutil.is_below(os.path.join(root, "a", "b"), root))
        self.assertFalse(fsutil.is_below(root + "2", root))  # a sibling sharing the prefix
        self.assertFalse(fsutil.is_below(os.path.dirname(root), root))
        self.assertFalse(fsutil.is_below("relative", root))


if __name__ == "__main__":
    unittest.main()
