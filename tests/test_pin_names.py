"""`pin ls` names: where each pin came from on this machine, or what a pinned directory holds."""
import io
import json
import os
import tempfile
import unittest
from unittest import mock

from rnfs.cli import display_name
from rnfs.config import Config
from rnfs.content.cid import Cid
from rnfs.node import RnfsNode
from rnfs.pin_names import PinNames


class PinNamesStoreTest(unittest.TestCase):
    """The store itself: record, keep the first for pin add, prune, persist, ignore damage."""

    def setUp(self):
        """A store in a temporary state directory."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name
        self.cid = Cid.for_block("raw", b"x")
        self.other = Cid.for_block("raw", b"y")

    def test_record_prune_and_persist(self):
        names = PinNames(self.dir)
        names.record(self.cid, "/home/u/a.txt")
        names.record_if_unknown(self.cid, "bafy.../a.txt")  # a later pin add keeps the local path
        names.record(self.other, "(stdin)")
        self.assertEqual(PinNames(self.dir).get(self.cid), "/home/u/a.txt")
        names.prune({self.cid})
        again = PinNames(self.dir)
        self.assertEqual((again.get(self.cid), again.get(self.other)), ("/home/u/a.txt", None))

    def test_damaged_file_is_ignored(self):
        with open(os.path.join(self.dir, "pin_names.json"), "w", encoding="utf-8") as f:
            json.dump({"not a cid": "x", str(self.cid): 5, str(self.other): "ok"}, f)
        names = PinNames(self.dir)
        self.assertEqual((names.get(self.cid), names.get(self.other)), (None, "ok"))


class PinLsNamesTest(unittest.TestCase):
    """The node's pin_ls reports type and name for each way a pin can be made."""

    def setUp(self):
        """An offline node and a working directory of files to add."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name
        self.node = RnfsNode.offline(Config(rnfs_dir=os.path.join(self.dir, "node")), b"\x02" * 16,
                                     client=mock.Mock(), names_network=mock.Mock())
        self.addCleanup(self.node.stop)

    def entry(self, cid: str) -> dict:
        """pin_ls's entry for cid."""
        return self.node.pin_ls(cid)[0]

    def write(self, name: str, data: bytes) -> str:
        """A file in the working directory."""
        path = os.path.join(self.dir, name)
        with open(path, "wb") as f:
            f.write(data)
        return path

    def test_added_file_shows_its_path(self):
        path = self.write("notes.txt", b"hello")
        cid = self.node.add_path(path)["cid"]
        self.assertEqual((self.entry(cid)["type"], self.entry(cid)["name"]), ("file", os.path.abspath(path)))

    def test_stdin_and_named_stream(self):
        anon = self.node.add_stream(io.BytesIO(b"anonymous"))["cid"]
        named = self.node.add_stream(io.BytesIO(b"named"), name="report.pdf")["cid"]
        self.assertEqual(self.entry(anon)["name"], "(stdin)")
        self.assertEqual(self.entry(named)["name"], "report.pdf")

    def test_no_pin_adds_record_nothing(self):
        path = self.write("tmp.bin", b"cache only")
        cid = self.node.add_path(path, pin=False)["cid"]
        self.assertEqual(self.node.pin_ls(), [])
        self.assertIsNone(self.node.pin_names.get(Cid.parse(cid)))

    def test_directory_without_a_recorded_name_shows_what_it_holds(self):
        path = self.write("one.txt", b"wrapped")
        wrapped = self.node.add_path(path, wrap=True)["cid"]
        self.node.pin_names.prune(set())  # as if it was pinned before names were recorded
        self.assertEqual((self.entry(wrapped)["type"], self.entry(wrapped)["name"]), ("directory", "one.txt"))
        tree = os.path.join(self.dir, "tree")
        os.mkdir(tree)
        for i in range(3):
            self.write(os.path.join("tree", f"f{i}"), bytes([i]))
        cid = self.node.add_path(tree)["cid"]
        self.node.pin_names.prune(set())
        self.assertEqual(self.entry(cid)["name"], "(directory: 3 entries)")

    def test_unpinned_names_are_forgotten(self):
        path = self.write("gone.txt", b"bye")
        cid = self.node.add_path(path)["cid"]
        self.node.pin_rm(cid)
        self.node.pin_ls()
        self.assertIsNone(self.node.pin_names.get(Cid.parse(cid)))

    def test_shared_folder_entries_show_their_names(self):
        os.makedirs(self.node.config.files_dir, exist_ok=True)
        with open(os.path.join(self.node.config.files_dir, "shared.pdf"), "wb") as f:
            f.write(b"shared content")
        cids = self.node.shared_folder.scan()
        (cid,) = cids
        self.assertEqual(self.entry(str(cid))["name"], "shared.pdf")


class DisplayNameTest(unittest.TestCase):
    """The CLI shortens the home directory and escapes control characters."""

    def test_home_shortened_and_escaped(self):
        home = os.path.expanduser("~")
        self.assertEqual(display_name(os.path.join(home, "a.txt")), os.path.join("~", "a.txt"))
        self.assertEqual(display_name(home + "other/x"), home + "other/x")  # a sibling of home isn't under it
        self.assertNotIn("\x1b", display_name("evil\x1b[2Jname"))
