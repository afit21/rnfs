"""L8: where directories are walked by path (FD_WALK is False, as on Windows), swapping a checked directory in the
shared folder for a junction during a scan imports files from outside the folder.

Simulated on any POSIX system: builder.FD_WALK is forced to False and a symlink stands in for the junction. The swap
happens at a fixed point of the scan (after the signature walk of a top-level entry, or right after a nested
directory passed its check), so there is no timing race. With the path walk the outside file is imported and pinned
"folder" (so the node provides it); with the descriptor walk (FD_WALK True, the real setting on Linux and macOS)
it never is.
"""
import os
import tempfile
import unittest
from contextlib import contextmanager
from unittest import mock

from rnfs.content import builder, dag, shared_folder, walker
from rnfs.content.cid import RAW, Cid
from rnfs.content.shared_folder import SharedFolder
from rnfs.content.storage import FOLDER, Storage
from tests.support import CAN_SYMLINK

SECRET = b"TOP SECRET: outside the shared folder"


@unittest.skipUnless(CAN_SYMLINK, "creating symlinks isn't allowed here")
class FolderSwapTest(unittest.TestCase):
    """A shared folder holding drop/sub/harmless.txt, and a private directory outside it holding secret.txt."""

    def make_folder(self):
        """Storage, the shared folder and the private directory in a new temporary directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.private = os.path.join(self.tmp.name, "private")
        os.mkdir(self.private)
        with open(os.path.join(self.private, "secret.txt"), "wb") as f:
            f.write(SECRET)
        self.files = os.path.join(self.tmp.name, "files")
        self.drop = os.path.join(self.files, "drop")
        os.makedirs(os.path.join(self.drop, "sub"))
        with open(os.path.join(self.drop, "sub", "harmless.txt"), "wb") as f:
            f.write(b"hi")
        self.storage = Storage(os.path.join(self.tmp.name, "blocks"), os.path.join(self.tmp.name, "state"), 10**9)
        self.folder = SharedFolder(self.files, self.storage, os.path.join(self.tmp.name, "state"))

    def swap(self, path: str):
        """What the attacker does: move the directory at path aside and put a link to the private one there."""
        os.rename(path, path + ".moved")
        os.symlink(self.private, path)

    @contextmanager
    def swap_after_signature(self):
        """Swap drop once the scan has checked it and read its signature, before it is imported."""
        real = shared_folder.open_signature

        def signature_then_swap(handle, st, path):
            """open_signature, then the swap."""
            result = real(handle, st, path)
            if path == self.drop and not os.path.islink(path):
                self.swap(path)
            return result

        with mock.patch.object(shared_folder, "open_signature", signature_then_swap):
            yield

    @contextmanager
    def swap_after_check(self):
        """Swap drop/sub right after the import's walk checked it, before it is listed."""
        real = builder._check_opened
        importing = []
        real_build = builder.build_open

        def build_open(*args, **kwargs):
            """builder.build_open, noting that the import (not the signature walk) is running."""
            importing.append(True)
            return real_build(*args, **kwargs)

        def check_then_swap(path, expected, handle, follow=False):
            """_check_opened, then the swap."""
            real(path, expected, handle, follow)
            sub = os.path.join(self.drop, "sub")
            if importing and path == sub and not os.path.islink(sub):
                self.swap(sub)

        with mock.patch.object(builder, "_check_opened", check_then_swap), \
                mock.patch.object(builder, "build_open", build_open):
            yield

    def scan(self, fd_walk: bool, swapping) -> set[Cid]:
        """The pins of a scan with builder.FD_WALK set to fd_walk and the swap made by swapping."""
        with mock.patch.object(builder, "FD_WALK", fd_walk), swapping():
            return self.folder.scan()

    def imported_files(self, pins: set[Cid]) -> set[str]:
        """Paths of every file in the DAGs of pins, below the pinned directory."""
        get = self.storage.getter()
        paths = set()

        def visit(root: Cid, names: list[str]):
            """Collect the files below the directory <root>/<names>."""
            for entry in walker.list_directory(walker.resolve(root, names, get), get):
                if entry.kind == dag.KIND_DIRECTORY:
                    visit(root, names + [entry.name])
                else:
                    paths.add("/".join(names + [entry.name]))

        for cid in pins:
            visit(cid, [])
        return paths

    def outside_files(self, pins: set[Cid]) -> set[str]:
        """Imported files that came from the private directory."""
        return {path for path in self.imported_files(pins) if path.endswith("secret.txt")}

    def assert_leaked(self, pins: set[Cid]):
        """The secret was read into storage and is in the folder's pinned (so provided) DAG."""
        self.assertTrue(self.storage.has(Cid.for_block(RAW, SECRET)))
        self.assertTrue(self.outside_files(pins))
        self.assertEqual(pins, self.storage.pinned_by(FOLDER))

    def assert_not_leaked(self, pins: set[Cid]):
        """Nothing from outside the folder was read into storage."""
        self.assertFalse(self.storage.has(Cid.for_block(RAW, SECRET)), "the secret's block was stored")
        self.assertEqual(self.outside_files(pins), set(), "imported from outside the folder")

    def test_path_walk_imports_outside_files(self):
        for swapping in (self.swap_after_signature, self.swap_after_check):
            with self.subTest(swapping.__name__):
                self.make_folder()
                pins = self.scan(False, swapping)
                self.assert_leaked(pins)

    @unittest.skipUnless(builder.FD_WALK, "directories opened by descriptor (POSIX)")
    def test_descriptor_walk_stays_in_the_folder(self):
        for swapping in (self.swap_after_signature, self.swap_after_check):
            with self.subTest(swapping.__name__):
                self.make_folder()
                pins = self.scan(True, swapping)
                self.assertTrue(os.path.islink(self.drop) or os.path.islink(os.path.join(self.drop, "sub")))
                self.assert_not_leaked(pins)

    # L8: fails until the path walk re-checks each directory's identity (or walks by handle) so a swapped-in
    # junction can't lead outside the folder
    @unittest.expectedFailure
    def test_path_walk_stays_in_the_folder(self):
        for swapping in (self.swap_after_signature, self.swap_after_check):
            self.make_folder()
            pins = self.scan(False, swapping)
            self.assert_not_leaked(pins)


if __name__ == "__main__":
    unittest.main()
