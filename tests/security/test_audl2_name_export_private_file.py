"""AUD-L2 (0.2.1 audit): on Windows `rnfs name export <alias> -o FILE` wrote the private name key readable by others.

run_name_export created FILE with os.open(..., 0o600). Windows ignores that mode, so the key file inherited its
folder's access list: in a folder every local user can read (C:\\ProgramData, another drive's root, a shared folder)
any of them could copy the key and publish the name for good. `rnfs app add -o` already wrote its token through
fsutil.write_atomic(private=True), which gives the file a protected access list (the user and SYSTEM) before it takes
its name.

The test rebuilds it as tests/security/test_l7_windows_acl.py does, with fsutil.IS_WINDOWS patched on and its FakeAcl
standing in for a folder whose files every user may read: the exported key must come out private, set on the
temporary file before it took FILE's name. Now run_name_export writes through fsutil.write_atomic(mode=0o600,
private=True, exclusive=True), so it still never overwrites a file, and an existing FILE is refused as before.
"""
import os
from unittest import mock

from rnfs import cli, fsutil, winacl
from tests.security.test_l7_windows_acl import FakeAcl, USER
from tests.test_cli import CliTestCase


class NameExportWindowsTest(CliTestCase):
    """The CLI's name export with Windows' access lists faked (FakeAcl: files inherit a DACL any user may read)."""

    def setUp(self):
        super().setUp()
        self.acl = FakeAcl()
        for patch in (mock.patch.object(fsutil, "IS_WINDOWS", True), mock.patch.object(winacl, "backend", self.acl),
                      mock.patch.object(winacl, "_warned_outside", set())):
            patch.start()
            self.addCleanup(patch.stop)
        cli.write_token(self.token_file, self.server.token)  # private under the fake, so the CLI accepts it
        self.acl.set_calls.clear()

    def test_exported_key_is_private(self):
        self.ok("name", "create", "site")
        key_file = os.path.join(self.tmp, "site.key")
        self.ok("name", "export", "site", "-o", key_file)
        with open(key_file, "rb") as f:
            self.assertEqual(f.read(), self.node.keys["site"])
        self.assertIsNone(winacl.problem(self.acl.read(key_file), USER), "the key file is readable by others")
        [(temp, directory)] = self.acl.set_calls  # on the temporary file, before it took the key's name
        self.assertTrue(os.path.basename(temp).startswith(fsutil.PARTIAL_PREFIX))
        self.assertFalse(directory)

    def test_existing_file_is_never_overwritten(self):
        self.ok("name", "create", "site")
        key_file = self.write("site.key", b"something else")
        code, _, err = self.run_cli("name", "export", "site", "-o", key_file)
        self.assertEqual((code, err.strip()), (1, f"rnfs: {key_file} already exists"))
        with open(key_file, "rb") as f:
            self.assertEqual(f.read(), b"something else")
        self.assertEqual([n for n in os.listdir(self.tmp) if n.startswith(fsutil.PARTIAL_PREFIX)], [])
