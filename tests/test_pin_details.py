"""Tests for rnfs 0.2.0's pin details and the cache (rnfs/node.py pin_ls, pin_label, cache_ls, cache_rm;
rnfs/pin_names.py labels; rnfs/content/storage.py pin times, uncache and its listeners), on offline nodes, and the
"pin" and "cache" events they publish (rnfs/events.py)."""
import io
import os
import time
import unittest
from unittest import mock

from rnfs import constants, events, pin_names
from rnfs.content import dag
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.errors import NotFound
from tests.support import state_execute, state_rows
from tests.test_node import A_ID, NodeTest


class PinDetailsTest(NodeTest):
    """pin_ls's fields of 0.2.0: label, origin, size, entries and added."""

    def test_file_and_directory_fields(self):
        """A file has its size and no entries; a directory its entry count; the origin is where it came from."""
        file_cid = self.a.add_stream(io.BytesIO(b"x" * 1000), name=None)["cid"]
        tree = self.tree()
        dir_cid = self.a.add_path(tree)["cid"]
        pins = {pin["cid"]: pin for pin in self.a.pin_ls()}
        self.assertEqual((pins[file_cid]["type"], pins[file_cid]["size"], pins[file_cid]["entries"]),
                         ("file", 1000, None))
        self.assertEqual(pins[file_cid]["origin"], "(stdin)")
        self.assertEqual((pins[dir_cid]["type"], pins[dir_cid]["entries"]), ("directory", 2))
        self.assertEqual(pins[dir_cid]["size"], self.a.stat(dir_cid)["size"])
        self.assertEqual(pins[dir_cid]["origin"], os.path.abspath(tree))
        self.assertIsNone(pins[dir_cid]["label"])

    def test_large_directory_counts_its_index(self):
        """A directory past MAX_CHILDREN entries (an index node at its root) still gets its entry count; its name
        stays "(directory)", as before 0.2.0."""
        root = os.path.join(self.tmp.name, "many")
        os.makedirs(root)
        for n in range(11):
            with open(os.path.join(root, f"f{n:02}"), "wb") as f:
                f.write(bytes([n]))
        with mock.patch.object(constants, "MAX_CHILDREN", 4):
            cid = self.a.add_path(root)["cid"]
        self.a.pin_names.prune(set())  # no origin, so the name comes from inside
        [pin] = self.a.pin_ls()
        self.assertEqual((pin["type"], pin["entries"], pin["name"]), ("directory", 11, "(directory)"))
        self.assertEqual(pin["cid"], cid)

    def test_crafted_index_is_not_walked_exponentially(self):
        """An index listing the same child many times (which decode accepts, and a valid index never does) gets
        no entry count, after reading each node once; a count past max_output_entries isn't made either."""
        leaf = dag.DirectoryNode([dag.Entry("a", Cid.for_block(RAW, b"x"), 1, 0)])
        leaf_cid = Cid.for_block(DAG, dag.encode(leaf))
        middle = dag.IndexNode([dag.IndexChild(f"n{n:03}", leaf_cid, 1) for n in range(200)])
        middle_cid = Cid.for_block(DAG, dag.encode(middle))
        top = dag.IndexNode([dag.IndexChild(f"m{n:03}", middle_cid, 200) for n in range(200)])
        for node, cid in ((leaf, leaf_cid), (middle, middle_cid)):
            self.a.storage.put(cid, dag.encode(node))
        with mock.patch.object(self.a.storage, "get", wraps=self.a.storage.get) as get:
            self.assertIsNone(self.a._index_entries(top))
        self.assertLess(get.call_count, 10)
        honest = dag.IndexNode([dag.IndexChild("a", leaf_cid, 1)])
        self.assertEqual(self.a._index_entries(honest), 1)
        self.a.config.max_output_entries = 0
        self.assertIsNone(self.a._index_entries(honest))

    def test_added_time_is_kept_and_first_source_wins(self):
        """added is when the CID was first pinned by any source; another source pinning it later keeps it; it
        survives a restart and goes with the last source."""
        cid = self.a.add_stream(io.BytesIO(b"timed"))["cid"]
        first = self.a.pin_ls(cid)[0]["added"]
        self.assertLessEqual(abs(first - time.time()), 60)
        with mock.patch("time.time", return_value=first + 1000):
            self.a.storage.pin(Cid.parse(cid), "folder")
        self.assertEqual(self.a.pin_ls(cid)[0]["added"], first)
        self.a.stop()
        again = self.make_node("a", A_ID)
        self.assertEqual(again.pin_ls(cid)[0]["added"], first)
        again.storage.unpin(Cid.parse(cid), "folder")
        again.storage.unpin(Cid.parse(cid), "user")
        self.assertEqual(state_rows(again.config.state_dir, "SELECT * FROM pin_times"), [])

    def test_pin_from_before_0_1_7_has_no_time(self):
        """A pin whose row predates pin_times (no time recorded) lists added as None, and a time left without its
        pin is deleted at the next start."""
        cid = self.a.add_stream(io.BytesIO(b"old"))["cid"]
        state = self.a.config.state_dir
        self.a.stop()
        state_execute(state, "DELETE FROM pin_times")
        state_execute(state, "INSERT INTO pin_times (cid, added) VALUES (?, ?)", (b"not a cid", 1.0))
        again = self.make_node("a", A_ID)
        self.assertIsNone(again.pin_ls(cid)[0]["added"])
        self.assertEqual(state_rows(state, "SELECT * FROM pin_times"), [])


class PinLabelTest(NodeTest):
    """pin_label: a name the user gives a pin, apart from its origin."""

    def test_label_set_clear_and_persist(self):
        """A label becomes the name (the origin stays), survives a restart, and None or "" clears it."""
        cid = self.a.add_stream(io.BytesIO(b"labelled"), name="data.bin")["cid"]
        entry = self.a.pin_label(cid, "  My data  ")
        self.assertEqual((entry["label"], entry["name"], entry["origin"]), ("My data", "My data", "data.bin"))
        self.a.stop()
        again = self.make_node("a", A_ID)
        self.assertEqual(again.pin_ls(cid)[0]["label"], "My data")
        self.assertIsNone(again.pin_label(cid, "")["label"])
        self.assertEqual(again.pin_ls(cid)[0]["name"], "data.bin")
        again.pin_label(cid, "back")
        self.assertIsNone(again.pin_label(cid, None)["label"])
        self.assertEqual(state_rows(again.config.state_dir, "SELECT * FROM pin_labels"), [])

    def test_label_refusals(self):
        """Not pinned: NotFound; not a string or too long: ValueError; nothing is stored."""
        cid = self.a.add_stream(io.BytesIO(b"x"))["cid"]
        with self.assertRaises(NotFound):
            self.a.pin_label(str(Cid.for_block(RAW, b"elsewhere")), "x")
        for bad in (5, ["x"], "x" * (pin_names.MAX_LABEL_LENGTH + 1)):
            with self.assertRaises(ValueError):
                self.a.pin_label(cid, bad)
        self.assertIsNone(self.a.pin_ls(cid)[0]["label"])

    def test_label_goes_with_the_pin(self):
        """Once the CID is no longer pinned by anything its label is forgotten at once (no listing needed), so
        pinning it again starts without one; another source still pinning it keeps it."""
        cid = self.a.add_stream(io.BytesIO(b"temporary"))["cid"]
        self.a.pin_label(cid, "soon gone")
        self.a.storage.pin(Cid.parse(cid), "folder")
        self.a.pin_rm(cid)
        self.assertEqual(self.a.pin_ls(cid)[0]["label"], "soon gone")
        self.a.storage.unpin(Cid.parse(cid), "folder")
        self.a.storage.pin(Cid.parse(cid), "user")
        self.assertIsNone(self.a.pin_ls(cid)[0]["label"])

    def test_listing_never_drops_a_new_label(self):
        """pin_ls prunes names from a snapshot of the pins, never labels: a pin and label made after the snapshot
        keep their label."""
        cid = self.a.add_stream(io.BytesIO(b"raced"))["cid"]
        self.a.pin_label(cid, "mine")
        self.a.pin_names.prune(set())
        self.assertEqual(self.a.pin_names.label(Cid.parse(cid)), "mine")

    def test_a_label_not_saved_is_not_kept(self):
        """A label whose save fails is rolled back, so a retry writes it and publishes its event."""
        cid = self.a.add_stream(io.BytesIO(b"full disk"))["cid"]
        store = self.a.pin_names._label_store
        with mock.patch.object(store, "_write_dirty", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.a.pin_label(cid, "lost")
        self.assertIsNone(self.a.pin_names.label(Cid.parse(cid)))
        store._dirty.clear()
        self.assertEqual(self.a.pin_label(cid, "lost")["label"], "lost")
        labels = [event for event in self.a.events.since(0)["events"] if event.get("change") == "label"]
        self.assertEqual(len(labels), 1)


class CacheTest(NodeTest):
    """cache_ls and cache_rm: downloads that aren't pinned."""

    def fetched(self) -> str:
        """A tree A adds and B gets (so B caches it); its CID."""
        cid = self.a.add_path(self.tree())["cid"]
        self.advertise(cid)
        self.b.get(cid, os.path.join(self.tmp.name, "out"))
        return cid

    def test_cache_ls_lists_downloads(self):
        """A download is listed, complete and provided (the roots strategy), with its size; a path fetch too."""
        cid = self.a.add_path(self.tree())["cid"]
        self.advertise(cid)
        b"".join(self.b.read(f"{cid}/sub/deeper/note.txt"))  # a path fetch, then the whole target
        self.b.get(cid, os.path.join(self.tmp.name, "out"))
        b"".join(self.b.read(f"{cid}/sub/deeper/note.txt"))  # used again: the most recently used
        listing = self.b.cache_ls()
        whole = [entry for entry in listing if entry["cid"] == cid]
        self.assertEqual(len(whole), 1)
        self.assertEqual((whole[0]["root"], whole[0]["path"], whole[0]["complete"], whole[0]["pinned"],
                          whole[0]["type"]), (cid, [], True, False, "directory"))
        self.assertTrue(whole[0]["provided"])
        self.assertEqual(whole[0]["size"], self.a.stat(cid)["size"])
        note = [entry for entry in listing if entry["path"] == ["sub", "deeper", "note.txt"]]
        self.assertEqual(len(note), 1)
        self.assertEqual(note[0]["size"], 4)
        self.assertIs(listing[0], note[0])  # most recently used first

    def test_cache_rm_frees_and_withdraws(self):
        """cache rm drops the records leading to the CID, deletes its blocks and stops providing it; a second
        rm is NotFound."""
        cid = self.fetched()
        before = self.b.storage.used_bytes()
        result = self.b.cache_rm(cid)
        self.assertEqual(result["removed_records"], 1)
        self.assertGreater(result["removed_blocks"], 0)
        self.assertEqual(self.b.storage.used_bytes(), before - result["freed_bytes"])
        self.assertEqual(self.b.cache_ls(), [])
        self.assertNotIn(Cid.parse(cid), self.b.publisher.provided())
        self.assertFalse(self.b.storage.has(Cid.parse(cid)))
        with self.assertRaises(NotFound):
            self.b.cache_rm(cid)

    def test_cache_rm_keeps_a_pin_of_the_same_content(self):
        """Content both cached and pinned keeps its blocks: only the cache record goes."""
        cid = self.fetched()
        self.b.storage.pin(Cid.parse(cid), "user")
        self.assertTrue(self.b.cache_ls()[0]["pinned"])
        result = self.b.cache_rm(cid)
        self.assertEqual((result["removed_records"], result["removed_blocks"]), (1, 0))
        self.assertTrue(self.b.storage.is_complete(Cid.parse(cid)))

    def test_cache_rm_leaves_other_records(self):
        """Another cached download stays, blocks and all."""
        cid = self.fetched()
        other = self.a.add_stream(io.BytesIO(os.urandom(3000)))["cid"]
        self.advertise(other)
        self.b.get(other, os.path.join(self.tmp.name, "other"))
        self.b.cache_rm(cid)
        self.assertEqual([entry["cid"] for entry in self.b.cache_ls()], [other])
        self.assertTrue(self.b.storage.is_complete(Cid.parse(other)))


class StorageEventsTest(NodeTest):
    """The "pin" and "cache" events the node publishes for storage's changes."""

    def kinds(self, node, kind: str) -> list[dict]:
        """Every event of kind node has published."""
        return [event for event in node.events.since(0)["events"] if event["kind"] == kind]

    def test_pin_events(self):
        """An add, a label and a release each publish a "pin" event naming the CID."""
        cid = self.a.add_stream(io.BytesIO(b"evented"))["cid"]
        self.a.pin_label(cid, "x")
        self.a.pin_rm(cid)
        changes = [(event["change"], event["cids"], event["source"]) for event in self.kinds(self.a, "pin")]
        self.assertEqual(changes, [("added", [cid], "user"), ("label", [cid], None), ("removed", [cid], "user")])
        self.assertEqual(self.kinds(self.a, "pin")[1]["label"], "x")

    def test_cache_events(self):
        """A download publishes a "cache" added event, cache rm a removed one; a gc of many reports the count."""
        cid = self.a.add_path(self.tree())["cid"]
        self.advertise(cid)
        self.b.get(cid, os.path.join(self.tmp.name, "out"))
        self.b.cache_rm(cid)
        changes = [(event["change"], event["cids"]) for event in self.kinds(self.b, "cache")]
        self.assertEqual(changes, [("added", [cid]), ("removed", [cid])])

    def test_many_cids_are_counted_not_listed(self):
        """An event about more than MAX_EVENT_CIDS pins lists that many and counts them all."""
        cids = [Cid.for_block(RAW, bytes([n, m])) for n in range(2) for m in range(80)]
        self.a.storage.pin_many(cids, "user")
        [event] = [event for event in self.kinds(self.a, "pin") if event["count"] == len(cids)]
        self.assertEqual(len(event["cids"]), events.MAX_EVENT_CIDS)


if __name__ == "__main__":
    unittest.main()
