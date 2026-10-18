"""CONT-2: `get` created a directory's entries by path, after all its siblings, and never checked that the
directory was still the one it had made. Someone who could rename inside the output (a group member writing to a
umask 002 output in a shared setgid directory; on Windows, any user a folder's inherited ACL grants Modify) could
swap `partial/sub` for a symlink or junction between its mkdir and its children's writes, and get then created the
DAG's files, execute bit included, in a directory of their choosing (~/.config/autostart, the Startup folder).

Now, on POSIX, the partial root is made private (0700, setgid kept) and must be empty, and every entry is created
relative to its parent's descriptor: directories are reopened from the root, one component at a time without
following links, and checked against the device and inode recorded when they were made. On Windows the root is
created with a private DACL and each file's final path must be inside it; the output then inherits its parent's
access list again before the rename. node.get checks the partial root is still the one write_out made before
renaming it.

The swaps happen at a fixed point (while writing a file that comes after the directory in name order, so after the
directory's mkdir and before it is reopened for its children), so there is no timing race. The Windows logic runs
on any platform through the fake winacl backend of test_l7_windows_acl, with a symlink standing in for a junction;
the real advapi32 calls aren't exercised here.
"""
import os
import stat
import sys
import tempfile
import unittest
from unittest import mock

from rnfs import fsutil, node as node_module, winacl
from rnfs.config import Config
from rnfs.content import builder, dag, walker
from rnfs.content.cid import EMPTY_DIRECTORY
from rnfs.content.dag import DirectoryNode, Entry
from rnfs.content.walker import root_expect
from rnfs.node import RnfsNode
from tests.security.test_l7_windows_acl import SHARED, USER, FakeAcl
from tests.support import CAN_SYMLINK, POSIX_MODES
from tests.test_walker import Blocks

CHANGED = "output was changed while being written"
HAS_FINAL_PATH = os.name == "nt" or sys.platform.startswith("linux") or sys.platform == "darwin"
EVIL = b"[Desktop Entry]\nExec=sh -c 'echo pwned'\n"


class SwapTestCase(unittest.TestCase):
    """A DAG {sub/evil.desktop, z.txt} in memory, a temporary directory and an attacker's directory in it."""

    def setUp(self):
        """The DAG and the directories."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name
        self.blocks = Blocks()
        evil = builder.build_bytes(EVIL, self.blocks.put, kind=dag.KIND_EXECUTABLE)
        self.z = builder.build_bytes(b"after sub in name order", self.blocks.put)
        sub = builder.wrap("evil.desktop", evil, self.blocks.put)
        self.root = builder.build_directory([Entry("sub", sub.cid, sub.size, dag.KIND_DIRECTORY),
                                             Entry("z.txt", self.z.cid, self.z.size, dag.KIND_FILE)], self.blocks.put)
        self.attacker = os.path.join(self.dir, "autostart")
        os.mkdir(self.attacker)
        self.dest = os.path.join(self.dir, "out")

    def swap_to_link(self, dest: str):
        """What the attacker does: move dest/sub aside and put a symlink (a junction on Windows) to theirs there."""
        os.rename(os.path.join(dest, "sub"), os.path.join(self.dir, "moved"))
        os.symlink(self.attacker, os.path.join(dest, "sub"))

    def swap_to_directory(self, dest: str):
        """Or: move dest/sub aside and put a real directory of their own there."""
        os.rename(os.path.join(dest, "sub"), os.path.join(self.dir, "moved"))
        os.mkdir(os.path.join(dest, "sub"))

    def write_out(self, swap=None, **kwargs) -> list[str]:
        """walker.write_out of the DAG to self.dest, calling swap(dest) once, just before z.txt is created."""
        def check(file):
            """The deny list's hook, called before each file is created: the attacker's moment."""
            if swap is not None and file.cid == self.z.cid:
                swap(self.dest)

        return walker.write_out(root_expect(self.root.cid), self.blocks.get, self.dest, 100, check, **kwargs)


@unittest.skipUnless(CAN_SYMLINK and POSIX_MODES, "needs symlinks and POSIX modes")
class PosixSwapTest(SwapTestCase):
    """The descriptor write-out (Linux, macOS)."""

    def test_swap_to_a_symlink_writes_nothing_outside(self):
        with self.assertRaisesRegex(ValueError, CHANGED):
            self.write_out(self.swap_to_link)
        self.assertEqual(os.listdir(self.attacker), [])
        self.assertEqual(os.listdir(os.path.join(self.dir, "moved")), [])  # nor in the real one, moved away

    def test_swap_to_another_directory_fails_on_its_identity(self):
        with self.assertRaisesRegex(ValueError, CHANGED):
            self.write_out(self.swap_to_directory)
        self.assertEqual(os.listdir(os.path.join(self.dest, "sub")), [])

    def test_unswapped_write_out_is_unchanged(self):
        created = []
        self.assertEqual(self.write_out(created=created), [])
        with open(os.path.join(self.dest, "sub", "evil.desktop"), "rb") as f:
            self.assertEqual(f.read(), EVIL)
        self.assertTrue(os.stat(os.path.join(self.dest, "sub", "evil.desktop")).st_mode & stat.S_IXUSR)
        st = os.lstat(self.dest)
        self.assertEqual(created, [(st.st_dev, st.st_ino)])

    def test_root_is_private_while_written_and_normal_after(self):
        # In a setgid directory, as shared ones are: setgid stays on the root, so what is created below it keeps
        # the group, and the root gets back the mode a plain mkdir gives it
        shared = os.path.join(self.dir, "shared")
        os.mkdir(shared)
        os.chmod(shared, 0o2775)
        os.mkdir(os.path.join(shared, "reference"))
        normal = stat.S_IMODE(os.lstat(os.path.join(shared, "reference")).st_mode)
        self.dest = os.path.join(shared, "out")
        modes = []
        self.write_out(lambda dest: modes.append(stat.S_IMODE(os.lstat(dest).st_mode)))
        self.assertEqual(modes, [0o700 | (normal & stat.S_ISGID)])
        self.assertEqual(stat.S_IMODE(os.lstat(self.dest).st_mode), normal)
        if normal & stat.S_ISGID:  # a filesystem that honours setgid directories
            self.assertTrue(os.lstat(os.path.join(self.dest, "sub")).st_mode & stat.S_ISGID)

    def test_entry_planted_before_the_chmod_aborts(self):
        # Between the root's mkdir and its fchmod to 0700, anyone the parent's mode lets in can create an entry
        # in it (say a symlink named like an entry of the DAG, or a directory they keep writing to)
        real = os.mkdir

        def mkdir_then_plant(path, *args, **kwargs):
            """os.mkdir, then the attacker's entry if it was the root."""
            real(path, *args, **kwargs)
            if path == self.dest:
                os.symlink(self.attacker, os.path.join(path, "sub"))

        with mock.patch("os.mkdir", mkdir_then_plant), self.assertRaisesRegex(ValueError, CHANGED):
            self.write_out()
        self.assertEqual(os.listdir(self.attacker), [])

    def test_root_swapped_for_a_symlink_before_it_is_opened(self):
        real = os.mkdir

        def mkdir_then_swap(path, *args, **kwargs):
            """os.mkdir, then the root moved aside and a symlink to the attacker's directory put in its place."""
            real(path, *args, **kwargs)
            if path == self.dest:
                os.rename(path, os.path.join(self.dir, "moved"))
                os.symlink(self.attacker, path)

        with mock.patch("os.mkdir", mkdir_then_swap), self.assertRaisesRegex(ValueError, CHANGED):
            self.write_out()
        self.assertEqual(os.listdir(self.attacker), [])

    def test_ten_thousand_subdirectories_dont_exhaust_descriptors(self):
        # The stack holds names and identities, not descriptors: a few are open at once, however many directories
        self.blocks[EMPTY_DIRECTORY] = dag.encode(DirectoryNode([]))
        count = 10_000
        empty = [Entry(f"d{i:05}", EMPTY_DIRECTORY, 0, dag.KIND_DIRECTORY) for i in range(count)]
        middle = builder.build_directory(empty, self.blocks.put)
        top = builder.build_directory([Entry("m", middle.cid, 0, dag.KIND_DIRECTORY)], self.blocks.put)
        import resource  # POSIX only, as this class is: imported here so the module loads on Windows
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        fd_dir = "/proc/self/fd" if os.path.isdir("/proc/self/fd") else "/dev/fd"
        highest = max(int(name) for name in os.listdir(fd_dir))
        resource.setrlimit(resource.RLIMIT_NOFILE, (highest + 32, hard))
        try:
            walker.write_out(root_expect(top.cid), self.blocks.get, self.dest, count + 1)
        finally:
            resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
        self.assertEqual(len(os.listdir(os.path.join(self.dest, "m"))), count)


@unittest.skipUnless(CAN_SYMLINK, "needs symlinks")
class NodeGetSwapTest(unittest.TestCase):
    """node.get: a swap inside the partial output, or of the partial output itself, fails the get and leaves
    no output."""

    def setUp(self):
        """An offline node holding {sub/evil.desktop, z.txt}, and an attacker's directory."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name
        self.node = RnfsNode.offline(Config(rnfs_dir=os.path.join(self.dir, "node")), b"\x01" * 16,
                                     client=mock.Mock(), names_network=mock.Mock())
        self.addCleanup(self.node.stop)
        src = os.path.join(self.dir, "src")
        os.makedirs(os.path.join(src, "sub"))
        for name, data in (("sub/evil.desktop", EVIL), ("z.txt", b"z")):
            with open(os.path.join(src, name), "wb") as f:
                f.write(data)
        self.cid = self.node.add_path(src)["cid"]
        self.attacker = os.path.join(self.dir, "autostart")
        os.mkdir(self.attacker)
        self.out = os.path.join(self.dir, "out")

    def partial(self) -> str:
        """The get's partial output, beside out."""
        [name] = [n for n in os.listdir(self.dir) if n.startswith(".rnfs-get-")]
        return os.path.join(self.dir, name)

    def test_swap_inside_the_partial_fails_the_get(self):
        real = walker._write_file

        def write_and_swap(expect, *args, **kwargs):
            """_write_file; before z.txt, the attacker's swap of sub."""
            if expect.size == 1:
                sub = os.path.join(self.partial(), "sub")
                os.rename(sub, os.path.join(self.dir, "moved"))
                os.symlink(self.attacker, sub)
            return real(expect, *args, **kwargs)

        with mock.patch.object(walker, "_write_file", write_and_swap), self.assertRaisesRegex(ValueError, CHANGED):
            self.node.get(self.cid, self.out)
        self.assertEqual(os.listdir(self.attacker), [])
        self.assertFalse(os.path.lexists(self.out))
        self.assertEqual([n for n in os.listdir(self.dir) if n.startswith(".rnfs-get-")], [])

    def test_partial_swapped_after_write_out_is_not_renamed(self):
        real = walker.write_out

        def write_then_swap(expect, get, dest, *args, **kwargs):
            """write_out, then the whole partial output replaced by a directory of the attacker's."""
            warnings = real(expect, get, dest, *args, **kwargs)
            os.rename(dest, os.path.join(self.dir, "moved"))
            os.rename(self.attacker, dest)
            return warnings

        with open(os.path.join(self.attacker, "theirs"), "wb"):
            pass
        with mock.patch.object(walker, "write_out", write_then_swap), self.assertRaisesRegex(ValueError, CHANGED):
            self.node.get(self.cid, self.out)
        self.assertFalse(os.path.lexists(self.out))
        # What was put in the partial's place isn't the get's to delete: it is left where it was put
        self.assertEqual(os.listdir(self.partial()), ["theirs"])

    def test_get_writes_the_output(self):
        self.assertEqual(self.node.get(self.cid, self.out)["warnings"], [])
        with open(os.path.join(self.out, "sub", "evil.desktop"), "rb") as f:
            self.assertEqual(f.read(), EVIL)
        if POSIX_MODES:
            reference = os.path.join(self.dir, "reference")
            os.mkdir(reference)
            self.assertEqual(os.lstat(self.out).st_mode, os.lstat(reference).st_mode)


@unittest.skipUnless(CAN_SYMLINK, "needs symlinks")
class WindowsSwapTest(SwapTestCase):
    """The path write-out of Windows, with a fake winacl backend and a symlink standing in for a junction."""

    def setUp(self):
        super().setUp()
        self.acl = FakeAcl()
        for patch in (mock.patch.object(fsutil, "IS_WINDOWS", True), mock.patch.object(winacl, "backend", self.acl)):
            patch.start()
            self.addCleanup(patch.stop)

    def test_root_is_created_private(self):
        self.assertEqual(self.write_out(), [])
        self.assertEqual(self.acl.created, [self.dest])
        self.assertIsNone(winacl.problem(self.acl.read(self.dest), USER))
        with open(os.path.join(self.dest, "sub", "evil.desktop"), "rb") as f:
            self.assertEqual(f.read(), EVIL)

    @unittest.skipUnless(HAS_FINAL_PATH, "this platform can't give a file's final path")
    def test_file_below_a_swapped_directory_is_refused(self):
        # Windows can't open a directory as a descriptor, so the file is created (empty) through the junction, but
        # its final path gives it away before a byte is written. It isn't removed: by path, that could be redirected
        with self.assertRaisesRegex(ValueError, CHANGED):
            self.write_out(self.swap_to_link)
        self.assertEqual(os.listdir(self.attacker), ["evil.desktop"])
        self.assertEqual(os.path.getsize(os.path.join(self.attacker, "evil.desktop")), 0)

    def test_get_restores_inheritance_before_the_rename(self):
        node = RnfsNode.offline(Config(rnfs_dir=os.path.join(self.dir, "node")), b"\x01" * 16,
                                client=mock.Mock(), names_network=mock.Mock())
        self.addCleanup(node.stop)
        src = os.path.join(self.dir, "src")
        os.makedirs(os.path.join(src, "sub"))
        with open(os.path.join(src, "sub", "f"), "wb") as f:
            f.write(b"f")
        cid = node.add_path(src)["cid"]
        out = os.path.join(self.dir, "got")
        renamed = []
        real = fsutil.rename_new

        def rename_new(src, dst):
            """fsutil.rename_new, noting what had been made to inherit by then."""
            renamed.append(list(self.acl.inherit_calls))
            real(src, dst)

        with mock.patch.object(fsutil, "rename_new", rename_new):
            node.get(cid, out)
        [[partial]] = renamed
        self.assertTrue(os.path.basename(partial).startswith(".rnfs-get-"))
        self.assertEqual(self.acl.read(out), SHARED)  # its parent's access list, as for anything made there
        # A single file is made in a folder the user chose, so it inherits from the start
        self.acl.inherit_calls.clear()
        node.get(cid + "/sub/f", os.path.join(self.dir, "f"))
        self.assertEqual(self.acl.inherit_calls, [])
        # If its access list can't be relaxed, the output stays private (safe) and the get still completes
        again = os.path.join(self.dir, "again")
        with mock.patch.object(self.acl, "inherit", side_effect=PermissionError(5, "Access is denied")), \
                mock.patch.object(node_module, "log") as log:
            node.get(cid, again)
        self.assertIsNone(winacl.problem(self.acl.read(again), USER))
        self.assertIn("stays private to you", log.call_args.args[0])

    def test_empty_acl(self):
        # What the real backend sets, unprotected, to inherit again: an ACL of no entries
        acl = winacl.empty_acl()
        self.assertEqual(int.from_bytes(acl[2:4], "little"), len(acl))
        self.assertEqual(winacl.parse_acl(acl), [])


if __name__ == "__main__":
    unittest.main()
