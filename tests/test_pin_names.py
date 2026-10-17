"""`pin ls` names: where each pin came from on this machine, or what a pinned directory holds."""
import io
import os
import tempfile
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from unittest import mock

from rnfs.cli import display_name
from rnfs.config import Config
from rnfs.content.cid import Cid
from rnfs.node import RnfsNode
from rnfs.pin_names import MAX_NAME_LENGTH, PinNames
from tests.support import failing_commits, state_execute, state_rows, write_legacy


@contextmanager
def recorded_transactions(db, label: str, opened: list | None = None) -> Iterator[list]:
    """Within the with block, each transaction opened through the StateDb reference db appends (label, durable)
    to opened (a new list if None), which is yielded."""
    opened = [] if opened is None else opened
    real = db.transaction

    def transaction(durable: bool = False):
        """The real transaction, noted."""
        opened.append((label, durable))
        return real(durable)

    with mock.patch.object(db, "transaction", transaction):
        yield opened


class PinNamesStoreTest(unittest.TestCase):
    """The store itself: record, keep the first for pin add, prune, persist, ignore damage."""

    def setUp(self):
        """A store in a temporary state directory."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = tmp.name
        self.cid = Cid.for_block("raw", b"x")
        self.other = Cid.for_block("raw", b"y")

    def open(self) -> PinNames:
        """A store on the state directory, closed when the test ends."""
        names = PinNames(self.dir)
        self.addCleanup(names.close)
        return names

    def rows(self) -> dict:
        """What the pin_names table holds: CID -> stored name text."""
        return {Cid.from_bytes(cid): name for cid, name in state_rows(self.dir, "SELECT cid, name FROM pin_names")}

    def test_record_prune_and_persist(self):
        """Names are kept across a restart, a later pin add keeps the first, and pruned ones are deleted."""
        names = self.open()
        names.record(self.cid, "/home/u/a.txt")
        names.record_if_unknown(self.cid, "bafy.../a.txt")  # a later pin add keeps the local path
        names.record(self.other, "(stdin)")
        self.assertEqual(self.open().get(self.cid), "/home/u/a.txt")
        names.prune({self.cid})
        self.assertEqual(self.rows(), {self.cid: '"/home/u/a.txt"'})
        again = self.open()
        self.assertEqual((again.get(self.cid), again.get(self.other)), ("/home/u/a.txt", None))

    def test_names_that_are_not_unicode_round_trip(self):
        """A name with a lone surrogate is saved and loaded as it was; a long one is cut to MAX_NAME_LENGTH."""
        names = self.open()
        odd = "/home/u/caf\udce9"  # a file name that isn't UTF-8, as os.fsdecode gives it
        names.record(self.cid, odd)
        names.record(self.other, "x" * (MAX_NAME_LENGTH + 10))
        again = self.open()
        self.assertEqual((again.get(self.cid), again.get(self.other)), (odd, "x" * MAX_NAME_LENGTH))

    def test_damaged_file_is_ignored(self):
        """pin_names.json's entries that don't parse are skipped when it is imported; the rest are kept."""
        write_legacy(self.dir, "pin_names.json", {"not a cid": "x", str(self.cid): 5, str(self.other): "ok"})
        names = self.open()
        self.assertEqual((names.get(self.cid), names.get(self.other)), (None, "ok"))

    def test_damaged_rows_are_deleted(self):
        """Rows that don't parse are skipped at load and deleted; the others are kept."""
        self.open().record(self.other, "ok")
        for cid, name in ((b"not a cid", '"x"'), (self.cid.binary, "not json"), (self.cid.binary + b"!", "5")):
            state_execute(self.dir, "INSERT INTO pin_names (cid, name) VALUES (?, ?)", (cid, name))
        names = self.open()
        self.assertEqual((names.get(self.cid), names.get(self.other)), (None, "ok"))
        self.assertEqual(self.rows(), {self.other: '"ok"'})

    def test_failed_save_is_written_with_the_next_change(self):
        """A change whose commit failed stays in memory, as before, and is saved with the next change."""
        names = self.open()
        names.record(self.cid, "first")
        with failing_commits(), self.assertRaises(OSError):
            names.prune(set())
        self.assertIsNone(names.get(self.cid))  # forgotten in memory, as before
        self.assertEqual(self.rows(), {self.cid: '"first"'})
        names.record(self.other, "second")
        self.assertEqual(self.rows(), {self.other: '"second"'})

    def test_failed_save_is_written_by_save(self):
        """save() writes a name whose write failed (the node saves its stores at stop)."""
        names = self.open()
        with failing_commits(), self.assertRaises(OSError):
            names.record(self.cid, "first")
        self.assertEqual(self.rows(), {})
        names.save()
        self.assertEqual(self.rows(), {self.cid: '"first"'})

    def test_names_are_written_at_once_but_not_durably(self):
        """Each change is committed as it is made, with synchronous=NORMAL (S3): the pin it names is durable."""
        names = self.open()
        with recorded_transactions(names._db, "name") as opened:
            names.record(self.cid, "a")
            self.assertEqual(self.rows(), {self.cid: '"a"'})
            names.prune(set())
            self.assertEqual(self.rows(), {})
        self.assertEqual(opened, [("name", False), ("name", False)])


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

    def test_an_add_commits_durably_once_pin_first(self):
        """S3: an add makes one durable commit, the pin's; the name is written after it, not durably, so a power
        loss can lose at most the name, never the pin."""
        with recorded_transactions(self.node.storage._db, "pin") as opened, \
                recorded_transactions(self.node.pin_names._db, "name", opened):
            cid = self.node.add_path(self.write("notes.txt", b"hello"))["cid"]
            self.node.add_stream(io.BytesIO(b"streamed"), name="report.pdf")
        self.assertEqual(opened, [("pin", True), ("name", False)] * 2)
        self.assertEqual(self.entry(cid)["name"], os.path.abspath(os.path.join(self.dir, "notes.txt")))

    def test_stop_saves_a_name_whose_write_failed(self):
        """The node saves pin names at stop: a name whose write failed is written then."""
        cid = Cid.for_block("raw", b"x")
        with failing_commits(), self.assertRaises(OSError):
            self.node.pin_names.record(cid, "/home/u/a.txt")
        self.node.stop()
        rows = state_rows(self.node.config.state_dir, "SELECT cid, name FROM pin_names")
        self.assertEqual(rows, [(cid.binary, '"/home/u/a.txt"')])

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
