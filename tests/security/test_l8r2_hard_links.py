"""L8-R2 (fix): the shared folder imported regular files that have other hard links, so anyone who may write to
files/ could hard link a file of the owner's (a key, a document elsewhere on the same filesystem) into it and have
it published, even though links are otherwise skipped there. Now a folder scan skips a regular file whose link
count is above one, at the top level and inside shared directories, with a log line; once its other link is gone
it is imported at the next scan. `rnfs add` is unchanged (the user names what to add).

Reproduced with real hard links (os.link) in a temporary shared folder; the link count is read from the opened
file, as on Windows (NTFS hard links), but this has only been run on Linux.
"""
import os
import tempfile
import unittest
from unittest import mock

from rnfs.content import builder, dag, shared_folder
from rnfs.content.shared_folder import SharedFolder
from rnfs.content.storage import FOLDER, Storage

SECRET = b"the owner's private key, kept outside the shared folder\n"


def can_hard_link(directory: str) -> bool:
    """True if directory's filesystem allows hard links."""
    probe = os.path.join(directory, "probe")
    with open(probe, "wb"):
        pass
    try:
        os.link(probe, probe + "-link")
    except OSError:
        return False
    finally:
        for path in (probe, probe + "-link"):
            if os.path.exists(path):
                os.remove(path)
    return True


class HardLinkTest(unittest.TestCase):
    """A shared folder, and a secret file of the owner's outside it on the same filesystem."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        if not can_hard_link(self.tmp.name):
            self.skipTest("no hard links on this filesystem")
        self.files = os.path.join(self.tmp.name, "files")
        os.mkdir(self.files)
        self.secret = os.path.join(self.tmp.name, "secret.key")
        with open(self.secret, "wb") as f:
            f.write(SECRET)
        self.storage = Storage(os.path.join(self.tmp.name, "blocks"), os.path.join(self.tmp.name, "state"), 10**9)
        self.addCleanup(self.storage.close)
        self.folder = SharedFolder(self.files, self.storage, os.path.join(self.tmp.name, "state"))
        self.addCleanup(self.folder.close)
        self.secret_cid = builder.add_path(self.secret, builder.NO_PUT)[0].cid
        log = mock.patch.object(shared_folder, "log")
        self.log = log.start()
        self.addCleanup(log.stop)

    def messages(self) -> str:
        return "\n".join(call.args[0] for call in self.log.call_args_list)

    def stored(self) -> bool:
        """True if the secret's content made it into the block store."""
        return self.storage.has(self.secret_cid)

    def test_top_level_hard_link_is_skipped(self):
        os.link(self.secret, os.path.join(self.files, "notes.txt"))
        self.assertEqual(self.folder.scan(), set())
        self.assertNotIn(self.secret_cid, self.storage.pinned_by(FOLDER), "a hard-linked file was shared")
        self.assertFalse(self.stored())
        self.assertIn("notes.txt: it has 1 other hard link", self.messages())

    def test_hard_link_inside_a_shared_directory_is_skipped(self):
        os.makedirs(os.path.join(self.files, "site"))
        with open(os.path.join(self.files, "site", "index.mu"), "wb") as f:
            f.write(b">Home\n")
        os.link(self.secret, os.path.join(self.files, "site", "backup.txt"))
        [pin] = self.folder.scan()
        self.assertFalse(self.stored(), "a hard-linked file inside a shared directory was imported")
        self.assertEqual([entry.name for entry in dag.decode(self.storage.get(pin)).entries], ["index.mu"])
        self.assertIn("backup.txt: it has 1 other hard link", self.messages())

    def test_imported_once_the_other_link_is_gone(self):
        """A file that only had a second link (say, a backup tool's) is shared once that link is removed."""
        os.link(self.secret, os.path.join(self.files, "notes.txt"))
        self.folder.scan()
        os.remove(self.secret)
        self.assertEqual(self.folder.scan(), {self.secret_cid})

    def test_existing_pin_of_a_file_linked_later_is_released(self):
        shared = os.path.join(self.files, "notes.txt")
        with open(shared, "wb") as f:
            f.write(b"public notes\n")
        [cid] = self.folder.scan()
        os.link(shared, os.path.join(self.tmp.name, "elsewhere"))
        self.assertEqual(self.folder.scan(), set())
        self.assertNotIn(cid, self.storage.pinned_by(FOLDER))

    def test_add_of_a_hard_linked_file_is_unchanged(self):
        """`rnfs add` builds what the user names, links or not."""
        os.link(self.secret, os.path.join(self.tmp.name, "linked"))
        built, warnings = builder.add_path(os.path.join(self.tmp.name, "linked"), builder.NO_PUT)
        self.assertEqual((built.cid, warnings), (self.secret_cid, []))


if __name__ == "__main__":
    unittest.main()
