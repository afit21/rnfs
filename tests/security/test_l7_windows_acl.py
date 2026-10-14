"""L7 (fix): on Windows the API token and rnfs dir relied on inherited ACLs, so with --dir outside the user's profile
other local users could read api_token, the identity key and the name keys. Now the rnfs dir, the token and the key
files get a protected DACL for the user and SYSTEM only, and the CLI and the daemon check it.

Simulated with fakes on any platform: fsutil.IS_WINDOWS is patched and winacl.backend is a FakeAcl that keeps a DACL
per file (by device and inode, so it follows a file across the atomic write's rename) and reports a chosen
"inherited" DACL for files it never set, standing in for a folder that lets every local user read. The binary SID and
ACL formats the real backend reads and writes are tested on their own. The real backend (advapi32 through ctypes) is
not exercised: none of this has been run on Windows.
"""
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs import cli, fsutil, node as node_module, winacl
from rnfs.config import Config
from rnfs.errors import RnfsError
from rnfs.network import identity as identity_module
from rnfs.winacl import ADMINISTRATORS, SYSTEM, Ace, Dacl
from tests.test_names import NamesTestCase

USER = "S-1-5-21-1111-2222-3333-1001"
OTHER = "S-1-5-21-1111-2222-3333-1002"
USERS = "S-1-5-32-545"
EVERYONE = "S-1-1-0"
FULL = winacl.FILE_ALL_ACCESS
READ = 0x120089  # FILE_GENERIC_READ
INHERITED = 0x10  # INHERITED_ACE


def allow(sid: str, mask: int = FULL, flags: int = INHERITED) -> Ace:
    """An inherited allow entry."""
    return Ace(winacl.ACCESS_ALLOWED, flags, mask, sid)


# What a file inherits under the user's profile (rnfs's default dir), and under a folder such as C:\ProgramData
# or another drive's root, where every local user may read
PROFILE = Dacl(USER, [allow(SYSTEM), allow(ADMINISTRATORS), allow(USER)])
SHARED = Dacl(USER, [allow(SYSTEM), allow(ADMINISTRATORS), allow(USER), allow(USERS, READ)])


class FakeAcl:
    """A winacl backend: a DACL per file, set_private's calls, and `inherited` for files never set."""

    def __init__(self, inherited: Dacl = SHARED):
        self.inherited = inherited
        self.dacls: dict[tuple[int, int], Dacl] = {}
        self.set_calls: list[tuple[str, bool]] = []
        self.read_fds: list[int | None] = []

    @staticmethod
    def key(path: str, fd: int | None = None) -> tuple[int, int]:
        """The file's identity, which a rename keeps."""
        st = os.fstat(fd) if fd is not None else os.stat(path)
        return st.st_dev, st.st_ino

    def user_sid(self) -> str:
        return USER

    def read(self, path: str, fd: int | None = None) -> Dacl:
        self.read_fds.append(fd)
        return self.dacls.get(self.key(path, fd), self.inherited)

    def set_private(self, path: str, directory: bool):
        self.set_calls.append((path, directory))
        self.dacls[self.key(path)] = Dacl(USER, winacl.parse_acl(winacl.private_acl(USER, directory)))

    def set(self, path: str, dacl: Dacl):
        """Give path dacl, as someone else (or an older rnfs) left it."""
        self.dacls[self.key(path)] = dacl


class WindowsTestCase(unittest.TestCase):
    """A temporary directory, fsutil.IS_WINDOWS patched on and a FakeAcl as winacl's backend."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.acl = FakeAcl()
        for patch in (mock.patch.object(fsutil, "IS_WINDOWS", True), mock.patch.object(winacl, "backend", self.acl)):
            patch.start()
            self.addCleanup(patch.stop)

    def private(self, path: str) -> bool:
        """The fake says path is private to the user."""
        return winacl.problem(self.acl.read(path), USER) is None


class TokenTest(WindowsTestCase):
    """cli.write_token and cli.read_token."""

    def setUp(self):
        super().setUp()
        self.path = os.path.join(self.tmp.name, cli.TOKEN_FILE)

    def write_plain(self, token: str = "t" * 32):
        """A token file written without an access list of its own, as rnfs 0.1.3 wrote it."""
        with open(self.path, "w") as f:
            f.write(token + "\n")

    def assert_refused(self, message: str):
        """read_token exits with message."""
        with self.assertRaises(SystemExit) as caught:
            cli.read_token(self.path)
        self.assertIn(message, str(caught.exception))

    def test_written_with_a_private_dacl_and_accepted(self):
        cli.write_token(self.path, "secret-token")
        # Set on the temporary file, before it took the token's name: never readable by others meanwhile
        [(temp, directory)] = self.acl.set_calls
        self.assertTrue(os.path.basename(temp).startswith(fsutil.PARTIAL_PREFIX))
        self.assertEqual(os.path.dirname(temp), self.tmp.name)
        self.assertFalse(directory)
        self.assertTrue(self.private(self.path))
        self.assertEqual(cli.read_token(self.path), "secret-token")
        self.assertIsNotNone(self.acl.read_fds[-1], "the DACL checked is the opened file's")

    def test_token_other_users_can_read_is_refused(self):
        # The L7 case: a --dir outside the profile, its token inheriting a DACL that lets Users read
        self.write_plain()
        self.assert_refused("accessible to other users (Users (S-1-5-32-545) may access it)")

    def test_refused_for_any_other_account_null_dacl_or_owner(self):
        for dacl, message in ((Dacl(USER, [allow(USER), allow(OTHER, READ)]), f"{OTHER} may access it"),
                              (Dacl(USER, [allow(USER), allow(EVERYONE, READ, 0x8)]), "Everyone"),  # inherit-only
                              (Dacl(USER, [allow(USER), Ace(0x5, 0, READ, None)]), "can't check (type 5)"),
                              (Dacl(USER, None), "no access list, so everyone may access it"),
                              (Dacl(OTHER, [allow(USER)]), f"it belongs to {OTHER}")):
            with self.subTest(message):
                self.write_plain()
                self.acl.set(self.path, dacl)
                self.assert_refused(message)

    def test_dacl_that_cant_be_read_is_refused(self):
        self.write_plain()
        with mock.patch.object(self.acl, "read", side_effect=PermissionError(5, "Access is denied")):
            self.assert_refused("can't check who may read the API token")

    def test_profile_default_is_accepted(self):
        # A token an older rnfs wrote in the default dir: the user, SYSTEM and Administrators, plus entries that
        # can't let anyone in (deny, audit, an allow with no rights)
        self.write_plain("old-token")
        self.acl.set(self.path, Dacl(ADMINISTRATORS, [*PROFILE.entries, Ace(0x1, 0, FULL, EVERYONE),
                                                      Ace(0x2, 0, FULL, None), allow(USERS, 0)]))
        self.assertEqual(cli.read_token(self.path), "old-token")

    def test_failed_dacl_leaves_no_token(self):
        cli.write_token(self.path, "old")
        with mock.patch.object(self.acl, "set_private", side_effect=PermissionError(5, "Access is denied")):
            with self.assertRaises(PermissionError):
                cli.write_token(self.path, "new")
        self.assertEqual(cli.read_token(self.path), "old")
        self.assertEqual([n for n in os.listdir(self.tmp.name) if n.startswith(fsutil.PARTIAL_PREFIX)], [])


class RnfsDirTest(WindowsTestCase):
    """node._make_private_dirs on Windows."""

    def setUp(self):
        super().setUp()
        self.root = os.path.join(self.tmp.name, "rnfs")
        log = mock.patch("rnfs.log.log")
        self.log = log.start()
        self.addCleanup(log.stop)
        profile = mock.patch.dict(os.environ, {"USERPROFILE": self.tmp.name})  # the rnfs dir is inside it
        profile.start()
        self.addCleanup(profile.stop)

    def messages(self) -> str:
        """Everything logged."""
        return "\n".join(call.args[0] for call in self.log.call_args_list)

    def test_new_dir_gets_a_protected_dacl(self):
        node_module._make_private_dirs(Config(rnfs_dir=self.root))
        self.assertEqual(self.acl.set_calls, [(self.root, True)])
        self.assertTrue(self.private(self.root))
        self.assertEqual(self.messages(), "")

    def test_existing_dir_others_can_read_is_tightened(self):
        os.mkdir(self.root)
        node_module._make_private_dirs(Config(rnfs_dir=self.root))
        self.assertEqual(self.acl.set_calls, [(self.root, True)])
        self.assertTrue(self.private(self.root))
        self.assertIn("made " + self.root + " private to you and SYSTEM (Users (S-1-5-32-545) may access it)",
                      self.messages())

    def test_existing_private_dir_is_left_alone(self):
        # The default ~/.rnfs, inheriting the profile's DACL: nothing changed, nothing logged at every start
        os.mkdir(self.root)
        self.acl.set(self.root, PROFILE)
        node_module._make_private_dirs(Config(rnfs_dir=self.root))
        self.assertEqual(self.acl.set_calls, [])
        self.assertEqual(self.messages(), "")

    def test_dir_of_another_account_is_refused(self):
        os.mkdir(self.root)
        self.acl.set(self.root, Dacl(OTHER, [allow(OTHER), allow(USER)]))
        with self.assertRaisesRegex(RnfsError, f"couldn't make the rnfs dir .* private to you: .*belongs to {OTHER}"):
            node_module._make_private_dirs(Config(rnfs_dir=self.root))
        self.assertEqual(self.acl.set_calls, [])
        self.assertFalse(os.path.exists(os.path.join(self.root, "state")))

    def test_dir_that_cant_be_made_private_is_refused(self):
        os.mkdir(self.root)
        with mock.patch.object(self.acl, "set_private", side_effect=PermissionError(5, "Access is denied")):
            with self.assertRaisesRegex(RnfsError, "couldn't make the rnfs dir"):
                node_module._make_private_dirs(Config(rnfs_dir=self.root))
        # Nor one whose DACL doesn't take (e.g. a filesystem without ACLs)
        with mock.patch.object(self.acl, "set_private"):
            with self.assertRaisesRegex(RnfsError, "still not private"):
                node_module._make_private_dirs(Config(rnfs_dir=self.root))

    def test_dir_outside_the_profile_is_warned_about(self):
        with mock.patch.dict(os.environ, {"USERPROFILE": os.path.join(self.tmp.name, "profile")}):
            node_module._make_private_dirs(Config(rnfs_dir=self.root))
        self.assertTrue(self.private(self.root))
        self.assertIn("is outside your user profile", self.messages())


class KeyFileTest(WindowsTestCase):
    """The identity file (load_or_create_identity) on Windows."""

    def setUp(self):
        super().setUp()
        self.path = os.path.join(self.tmp.name, "storage", "identity")

    def test_new_identity_gets_a_private_dacl(self):
        identity_module.load_or_create_identity(self.path)
        self.assertTrue(self.private(self.path))
        [(temp, _)] = self.acl.set_calls
        self.assertTrue(os.path.basename(temp).startswith(fsutil.PARTIAL_PREFIX))

    def test_existing_identity_others_can_read_is_tightened(self):
        os.makedirs(os.path.dirname(self.path))
        RNS.Identity().to_file(self.path)  # as rnfs 0.1.3 left it outside the profile
        with mock.patch.object(identity_module, "log") as log:
            identity_module.load_or_create_identity(self.path)
        self.assertTrue(self.private(self.path))
        self.assertIn("restricted it to you and SYSTEM", log.call_args.args[0])

    def test_private_identity_is_left_alone(self):
        identity_module.load_or_create_identity(self.path)
        self.acl.set_calls.clear()
        with mock.patch.object(identity_module, "log") as log:
            identity_module.load_or_create_identity(self.path)
        self.assertEqual(self.acl.set_calls, [])
        log.assert_not_called()


class NameKeyTest(NamesTestCase):
    """Name keys on Windows."""

    def test_name_key_gets_a_private_dacl(self):
        acl = FakeAcl()
        with mock.patch.object(fsutil, "IS_WINDOWS", True), mock.patch.object(winacl, "backend", acl):
            self.manager.create("site")
            self.assertEqual(winacl.problem(acl.read(os.path.join(self.config.names_dir, "site")), USER), None)


class PosixUnchangedTest(unittest.TestCase):
    """Outside Windows nothing here calls the backend: the mode-bit checks stay what they were."""

    def test_no_backend_calls(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(fsutil, "IS_WINDOWS", False), \
                mock.patch.object(winacl, "backend", mock.Mock(side_effect=AssertionError)) as backend:
            node_module._make_private_dirs(Config(rnfs_dir=os.path.join(directory, "rnfs")))
            token = os.path.join(directory, "rnfs", cli.TOKEN_FILE)
            cli.write_token(token, "t")
            self.assertEqual(cli.read_token(token), "t")
            identity_module.load_or_create_identity(os.path.join(directory, "rnfs", "storage", "identity"))
            self.assertFalse(winacl.outside_profile("/"))
        self.assertEqual(backend.mock_calls, [])


class FormatTest(unittest.TestCase):
    """The binary SID and ACL formats the real backend reads and writes."""

    def test_sid_round_trip(self):
        for sid in (USER, SYSTEM, ADMINISTRATORS, EVERYONE):
            with self.subTest(sid):
                self.assertEqual(winacl.sid_to_str(winacl.sid_from_str(sid)), sid)
        # S-1-5-18 as Windows stores it
        self.assertEqual(winacl.sid_from_str(SYSTEM), bytes.fromhex("010100000000000512000000"))
        for bad in ("S-1", "X-1-5", "S-2-5-18"):
            with self.subTest(bad), self.assertRaises(ValueError):
                winacl.sid_from_str(bad)

    def test_private_acl(self):
        for directory, flags in ((False, 0), (True, winacl.OBJECT_INHERIT | winacl.CONTAINER_INHERIT)):
            with self.subTest(directory=directory):
                acl = winacl.private_acl(USER, directory)
                self.assertEqual(int.from_bytes(acl[2:4], "little"), len(acl))  # AclSize
                self.assertEqual(winacl.parse_acl(acl), [Ace(0, flags, FULL, USER), Ace(0, flags, FULL, SYSTEM)])
                self.assertIsNone(winacl.problem(Dacl(USER, winacl.parse_acl(acl)), USER))

    def test_parse_acl_refuses_malformed(self):
        acl = winacl.private_acl(USER, False)
        for bad in (acl[:6], acl[:12], acl[:8] + b"\x00\x00\x04\x00" + acl[12:]):
            with self.subTest(bad.hex()), self.assertRaises(ValueError):
                winacl.parse_acl(bad)


if __name__ == "__main__":
    unittest.main()
