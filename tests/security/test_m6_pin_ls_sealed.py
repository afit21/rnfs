"""M6 (core): `pin ls` didn't say which pins are sealed files, so callers that show pins to others couldn't
tell which names and contents to keep back.

A sealed add records the plaintext path or seal_name as the pin's local name (pin_names), which is useful to
the node's own user and stays local; the leak was companion pages publishing it. Without a marker, a page had
to read each file's first bytes itself to tell. The fixed pin_ls() gives every pin "sealed": True for a file
with the sealed-file magic, False for other files and directories, None when the stored blocks can't tell
(the root or the first chunk isn't stored); `rnfs pin ls` prints "(sealed)" too, and --json carries the
field. The node is an offline node with an RNS identity (so it can seal to itself); the CLI part reuses
tests.test_cli's CliTestCase, whose FakeNode's pin_ls is given these entries.
"""
import io
import json
import os
import tempfile
import unittest

import RNS

from rnfs.content.cid import Cid
from tests.security import _pages_helpers as helpers
from tests.test_cli import CliTestCase

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class PinLsSealedTest(unittest.TestCase):
    """pin_ls() of sealed and plain pins on an offline node."""

    def setUp(self):
        """One offline node with an identity, and a provider it can fetch from."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        _, self.provider, self.node = helpers.make_nodes(self.tmp.name)
        self.addCleanup(self.provider.stop)
        self.addCleanup(self.node.stop)
        helpers.give_identity(self.node)

    def entry(self, cid: str) -> dict:
        """cid's pin_ls() entry."""
        return self.node.pin_ls(cid)[0]

    def test_sealed_pins_are_marked(self):
        """Sealed files (from a path or stdin) are marked sealed; the local name stays for the local user."""
        path = os.path.join(self.tmp.name, "Q3-layoffs.xlsx")
        with open(path, "wb") as f:
            f.write(b"secret data" * 10)
        from_path = self.node.add_path(path, to_self=True)["cid"]
        from_stdin = self.node.add_stream(io.BytesIO(b"minutes"), to_self=True, seal_name="board-minutes.txt")["cid"]
        # M6: fails until pin_ls marks sealed files
        self.assertEqual(self.entry(from_path).get("sealed"), True, "pin ls doesn't mark a sealed file")
        self.assertEqual(self.entry(from_stdin).get("sealed"), True)
        self.assertEqual(self.entry(from_path)["name"], os.path.abspath(path))  # kept, local only

    def test_plain_pins_are_not_marked(self):
        """Plain files, tiny files and directories are marked not sealed."""
        plain = self.node.add_stream(io.BytesIO(b"plain notes" * 100))["cid"]
        tiny = self.node.add_stream(io.BytesIO(b"rnfs"))["cid"]  # shorter than the magic
        folder = os.path.join(self.tmp.name, "maps")
        os.makedirs(folder)
        with open(os.path.join(folder, "north.png"), "wb") as f:
            f.write(b"png")
        directory = self.node.add_path(folder)["cid"]
        # M6: fails until pin_ls has the sealed field
        self.assertEqual([self.entry(cid).get("sealed", "missing") for cid in (plain, tiny, directory)],
                         [False, False, False])

    def test_unknown_when_the_first_chunk_isnt_stored(self):
        """A pin whose file content isn't stored yet (a remote pin still fetching) is None, not False."""
        cid = self.provider.add_stream(io.BytesIO(os.urandom(300_000)))["cid"]  # several chunks
        root = Cid.parse(cid)
        self.node.storage.put(root, self.provider.storage.get(root))  # the root block only
        self.node.storage.pin(root, "remote:" + "ab" * 16)
        self.assertEqual(self.entry(cid)["type"], "file")
        # M6: fails until pin_ls says when it can't tell
        self.assertIsNone(self.entry(cid).get("sealed", "missing"))


class PinLsCliTest(CliTestCase):
    """`rnfs pin ls` output for sealed entries."""

    def setUp(self):
        """A FakeNode whose pin_ls returns a sealed and a plain entry."""
        super().setUp()
        entries = [{"cid": "b" + "a" * 60, "sources": ["user"], "complete": True, "type": "file", "sealed": True,
                    "name": "/home/me/Q3-layoffs.xlsx"},
                   {"cid": "b" + "c" * 60, "sources": ["user"], "complete": True, "type": "file", "sealed": False,
                    "name": "/home/me/notes.txt"}]
        self.node.pin_ls = lambda cid=None: entries

    def test_text_output_marks_sealed(self):
        """The text listing says which pins are sealed."""
        lines = self.ok("pin", "ls").splitlines()
        self.assertIn("(sealed)", lines[0])
        self.assertNotIn("(sealed)", lines[1])

    def test_json_output_carries_the_field(self):
        """--json passes "sealed" through for scripts and pages."""
        pins = json.loads(self.ok("pin", "ls", "--json"))
        self.assertEqual([pin["sealed"] for pin in pins], [True, False])


if __name__ == "__main__":
    unittest.main()
