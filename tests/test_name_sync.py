"""Tests for name sync (rnfs.name_sync, specification section 9): a name kept pointing at a folder's content."""
import hashlib
import os
import shutil
import tempfile
import threading
import unittest
from unittest import mock

import RNS

from rnfs import name_sync, statedb
from rnfs.config import Config
from rnfs.content import builder, chunker, shared_folder
from rnfs.content.cid import Cid
from rnfs.errors import NotFound
from rnfs.name_sync import PAUSED, PUBLISH_INTERVAL, SETTLE_TIME, SETTLING, SYNCED, WAITING
from rnfs.names import PIN_SOURCE_PREFIX
from rnfs.node import RnfsNode
from tests.support import CAN_SYMLINK, FakeClock, state_rows, wait_for
from tests.test_names import FakeNetwork

NODE_ID = bytes([9] * 16)


class NameSyncTest(unittest.TestCase):
    """An offline node syncing names to folders under a temporary directory, its clocks moved by hand."""

    def setUp(self):
        """A node with a name "site" and a folder "site" holding one file."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        self.wall = FakeClock(1_700_000_000.0)
        self.mono = FakeClock(1000.0)
        self.node = self.make_node()
        self.node.name_create("site")
        self.folder = os.path.join(self.tmp.name, "site")
        self.write("index.html", b"<h1>hello</h1>")

    def make_node(self, **settings) -> RnfsNode:
        """An offline node on the test's rnfs dir, its name sync on the test's clocks; stopped at the end."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, "node"), slow="no", **settings)
        node = RnfsNode.offline(config, NODE_ID, None, FakeNetwork())
        self.addCleanup(node.stop)
        node.folder_syncs.clock, node.folder_syncs.monotonic = self.wall, self.mono
        return node

    def write(self, relative: str, data: bytes, folder: str | None = None) -> str:
        """Write a file in the folder (the test's by default), creating directories."""
        path = os.path.join(folder or self.folder, relative)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
        return path

    def target(self, alias: str = "site") -> str | None:
        """What the name alias points at."""
        return next(name for name in self.node.name_list() if name["alias"] == alias)["cid"]

    def sequence(self, alias: str = "site") -> int:
        """The name alias's sequence."""
        return next(name for name in self.node.name_list() if name["alias"] == alias)["sequence"]

    def state(self, alias: str = "site") -> dict:
        """The sync of alias, as GET /api/name/syncs lists it."""
        return next(entry for entry in self.node.name_syncs() if entry["alias"] == alias)

    def tick(self, seconds: float = 0.0):
        """Move both clocks on by seconds, then let the sync scan what is due."""
        self.wall.now += seconds
        self.mono.now += seconds
        self.node.folder_syncs.tick()

    def folder_cid(self, folder: str | None = None) -> str:
        """The CID `add --chunker fastcdc` gives the folder."""
        return self.node.add_path(folder or self.folder, only_hash=True, chunker=chunker.FASTCDC)["cid"]

    def synced(self) -> str:
        """Sync "site" to the folder and let it publish (a scan, then another once settled); the CID published."""
        self.node.name_sync("site", self.folder)
        self.tick()
        self.tick(SETTLE_TIME)
        self.assertEqual(self.state()["state"], SYNCED)
        return self.target()

    def test_publishes_the_folder_once_settled(self):
        """The first scan imports the folder and waits; a scan SETTLE_TIME later finding it unchanged publishes it,
        pinned with the name's source, as `name publish` does."""
        entry = self.node.name_sync("site", self.folder)
        self.assertEqual((entry["alias"], entry["path"], entry["state"], entry["published"]),
                         ("site", self.folder, "new", None))
        self.tick()
        self.assertEqual(self.state()["state"], SETTLING)
        self.assertEqual(self.state()["root"], self.folder_cid())
        self.assertIsNone(self.target())
        self.tick(SETTLE_TIME - 1)
        self.assertIsNone(self.target())
        self.tick(1)
        cid = self.folder_cid()
        self.assertEqual(self.target(), cid)
        self.assertEqual(self.state()["published"], cid)
        self.assertEqual(self.state()["published_at"], self.wall.now)
        self.assertEqual(self.node.storage.pins()[Cid.parse(cid)], {PIN_SOURCE_PREFIX + "site"})

    def test_fastcdc_is_the_chunker(self):
        """A synced folder's files are cut with FastCDC: the root is the folder's FastCDC CID, not the fixed one."""
        self.write("big.bin", os.urandom(600_000))
        cid = self.synced()
        self.assertEqual(cid, self.folder_cid())
        fixed = self.node.add_path(self.folder, only_hash=True)["cid"]
        self.assertNotEqual(cid, fixed)

    def test_republishes_on_change_and_releases_the_old_target(self):
        """A changed folder is published again (once settled, an hour after the last publish); the previous target's
        name pin is released, so only the latest version is kept."""
        first = self.synced()
        self.write("index.html", b"<h1>changed</h1>")
        self.tick(PUBLISH_INTERVAL)
        self.tick(SETTLE_TIME)
        second = self.target()
        self.assertNotEqual(second, first)
        self.assertEqual(second, self.folder_cid())
        self.assertEqual(self.sequence(), 2)
        pins = self.node.storage.pins()
        self.assertNotIn(Cid.parse(first), pins)
        self.assertEqual(pins[Cid.parse(second)], {PIN_SOURCE_PREFIX + "site"})

    def test_no_publish_while_unchanged(self):
        """Scans of an unchanged folder publish nothing more."""
        self.synced()
        for _ in range(5):
            self.tick(PUBLISH_INTERVAL)
        self.assertEqual(self.sequence(), 1)
        self.assertEqual(self.state()["state"], SYNCED)

    def test_settle_time_restarts_with_each_change(self):
        """A folder changing between scans (being copied into) isn't published until a scan SETTLE_TIME after the
        last change finds it unchanged."""
        self.node.name_sync("site", self.folder)
        self.tick()
        for step in range(3):
            self.write(f"part{step}", b"x" * (step + 1))
            self.tick(SETTLE_TIME)
            self.assertIsNone(self.target())
            self.assertEqual(self.state()["state"], SETTLING)
        self.tick(SETTLE_TIME)
        self.assertEqual(self.target(), self.folder_cid())

    def test_settling_is_scanned_without_waiting_for_scan_interval(self):
        """With a scan_interval longer than SETTLE_TIME, the scan that settles a change comes SETTLE_TIME after it."""
        self.node.config.scan_interval = 3600
        self.node.name_sync("site", self.folder)
        self.tick()
        self.assertEqual(self.node.folder_syncs.tick(), SETTLE_TIME)
        self.tick(SETTLE_TIME)
        self.assertEqual(self.target(), self.folder_cid())
        self.assertEqual(self.node.folder_syncs.tick(), 3600)

    def test_at_most_one_publish_an_hour(self):
        """A change settled within PUBLISH_INTERVAL of the last publish waits for it (state waiting, next_publish
        set), and is published once it has passed."""
        first = self.synced()
        published_at = self.state()["published_at"]
        self.write("index.html", b"<h1>changed soon</h1>")
        self.tick(self.node.config.scan_interval)
        self.tick(SETTLE_TIME)
        self.assertEqual(self.target(), first)
        self.assertEqual(self.state()["state"], WAITING)
        self.assertEqual(self.state()["next_publish"], published_at + PUBLISH_INTERVAL)
        # It is scanned again as soon as the hour is up, even with a longer scan_interval
        self.node.config.scan_interval = 2 * PUBLISH_INTERVAL
        self.assertAlmostEqual(self.node.folder_syncs.tick(), published_at + PUBLISH_INTERVAL - self.wall.now)
        self.tick(published_at + PUBLISH_INTERVAL - self.wall.now - 1)
        self.assertEqual(self.target(), first)
        self.tick(1)
        self.assertEqual(self.target(), self.folder_cid())
        self.assertEqual(self.sequence(), 2)

    def test_unchanged_files_are_not_read_again(self):
        """An unchanged folder is recognised by its signature and nothing in it is read; in a changed one only the
        file that changed is read (the shared folder's per-file cache, for FastCDC)."""
        self.write("other.bin", os.urandom(200_000))
        self.synced()
        reads = []
        real = builder.build_file

        def counting(stream, put, size=None, **kwargs):
            """builder.build_file, noting each file read."""
            reads.append(size)
            return real(stream, put, size, **kwargs)

        with mock.patch.object(builder, "build_file", counting):
            self.tick(PUBLISH_INTERVAL)
            self.assertEqual(reads, [])
            self.write("index.html", b"<h1>edited</h1>")
            self.tick(SETTLE_TIME)
            self.tick(SETTLE_TIME)
        self.assertEqual(len(reads), 1)
        self.assertEqual(self.target(), self.folder_cid())

    def test_unsync_keeps_the_target(self):
        """unsync stops the sync and leaves the name where it last pointed, pinned; later changes aren't
        published."""
        cid = self.synced()
        self.node.name_unsync("site")
        self.assertEqual(self.node.name_syncs(), [])
        self.write("index.html", b"<h1>after unsync</h1>")
        self.tick(PUBLISH_INTERVAL)
        self.tick(SETTLE_TIME)
        self.assertEqual(self.target(), cid)
        self.assertIn(PIN_SOURCE_PREFIX + "site", self.node.storage.pins()[Cid.parse(cid)])
        self.assertEqual(state_rows(self.node.config.state_dir, "SELECT alias FROM name_syncs"), [])
        with self.assertRaises(NotFound):
            self.node.name_unsync("site")

    def test_unsync_while_settling_publishes_nothing(self):
        """A sync stopped while a change settles never publishes it."""
        self.node.name_sync("site", self.folder)
        self.tick()
        self.node.name_unsync("site")
        self.tick(SETTLE_TIME)
        self.assertIsNone(self.target())

    def test_name_rm_stops_the_sync(self):
        """Removing the name stops its sync (and releases its pin, as name rm does)."""
        cid = self.synced()
        self.node.name_rm("site")
        self.assertEqual(self.node.name_syncs(), [])
        self.assertNotIn(Cid.parse(cid), self.node.storage.pins())
        self.assertEqual(state_rows(self.node.config.state_dir, "SELECT alias FROM name_syncs"), [])
        self.tick(PUBLISH_INTERVAL)  # nothing left to scan

    def test_vanished_folder_pauses_without_publishing(self):
        """A folder that disappears pauses the sync (state and reason) and publishes nothing, the name staying
        where it was; once the folder is back the sync resumes, publishing it if it changed."""
        cid = self.synced()
        moved = self.folder + ".moved"
        os.rename(self.folder, moved)
        self.tick(PUBLISH_INTERVAL)
        self.tick(SETTLE_TIME)
        entry = self.state()
        self.assertEqual(entry["state"], PAUSED)
        self.assertIn("missing", entry["reason"])
        self.assertEqual(self.target(), cid)
        self.assertEqual(self.sequence(), 1)
        os.rename(moved, self.folder)
        self.tick(SETTLE_TIME)
        self.assertEqual((self.state()["state"], self.state()["reason"]), (SYNCED, None))
        self.assertEqual(self.sequence(), 1)  # unchanged: nothing to publish

    def test_emptied_folder_is_not_published(self):
        """An empty folder (as an unmounted drive's mount point is) pauses the sync rather than being published."""
        cid = self.synced()
        os.remove(os.path.join(self.folder, "index.html"))
        self.tick(PUBLISH_INTERVAL)
        self.tick(SETTLE_TIME)
        self.assertEqual(self.state()["state"], PAUSED)
        self.assertIn("empty", self.state()["reason"])
        self.assertEqual(self.target(), cid)

    def test_unreadable_folder_pauses(self):
        """A folder that can't be read pauses the sync; one changing while it is read only delays it (in flux), and
        pauses it if it can't be read whole twice running."""
        cid = self.synced()
        self.write("index.html", b"<h1>being copied</h1>")
        failing = mock.patch.object(shared_folder, "open_signature", side_effect=OSError(5, "Input/output error"))
        with failing:
            self.tick(PUBLISH_INTERVAL)
            self.assertEqual((self.state()["state"], self.state()["reason"]), (SETTLING, None))
            self.tick(SETTLE_TIME)
            self.assertEqual(self.state()["state"], PAUSED)
            self.assertIn("Input/output error", self.state()["reason"])
        self.assertEqual(self.target(), cid)
        self.tick(SETTLE_TIME)
        self.tick(SETTLE_TIME)
        self.assertEqual(self.target(), self.folder_cid())

    def test_refusals_as_the_shared_folder_makes_them(self):
        """Links and files with other hard links are left out of the synced folder, as the shared folder leaves
        them out."""
        outside = self.write("secret", b"not to be shared", folder=os.path.join(self.tmp.name, "outside"))
        if CAN_SYMLINK:
            os.symlink(outside, os.path.join(self.folder, "link"))
        if hasattr(os, "link"):
            os.link(outside, os.path.join(self.folder, "hardlink"))
        cid = self.synced()
        names = [entry["name"] for entry in self.node.ls(cid)]
        self.assertEqual(names, ["index.html"])

    def test_folder_inside_the_rnfs_dir_is_refused(self):
        """A folder that is or leads into the rnfs dir, or one of its key and state folders, can't be synced; one
        that comes to lead there (a link retargeted) pauses the sync."""
        root = self.node.config.root
        for path in (root, os.path.join(root, "names"), os.path.join(root, "state")):
            with self.subTest(path=path), self.assertRaises(ValueError):
                self.node.name_sync("site", path)
        if not CAN_SYMLINK:
            return
        link = os.path.join(self.tmp.name, "link")
        os.symlink(self.folder, link)
        self.node.name_sync("site", link)
        self.tick()
        self.tick(SETTLE_TIME)
        cid = self.target()
        os.remove(link)
        os.symlink(os.path.join(root, "names"), link)
        self.write("index.html", b"<h1>changed</h1>")
        self.tick(PUBLISH_INTERVAL)
        self.tick(SETTLE_TIME)
        self.assertEqual(self.state()["state"], PAUSED)
        self.assertIn("keys and state", self.state()["reason"])
        self.assertEqual(self.target(), cid)

    def test_sync_refuses_what_it_cant_sync(self):
        """No such name or folder: NotFound; a relative path, or a file: ValueError."""
        with self.assertRaises(NotFound):
            self.node.name_sync("nope", self.folder)
        with self.assertRaises(NotFound):
            self.node.name_sync("site", os.path.join(self.tmp.name, "missing"))
        with self.assertRaises(ValueError):
            self.node.name_sync("site", "site")
        with self.assertRaises(ValueError):
            self.node.name_sync("site", os.path.join(self.folder, "index.html"))
        self.assertEqual(self.node.name_syncs(), [])

    def test_restart_resumes_without_reading_or_publishing_again(self):
        """Syncs are saved: after a restart a sync goes on from its saved state, recognising the unchanged folder
        without reading it and without publishing again; a change made while stopped is published."""
        cid = self.synced()
        self.node.stop()
        self.node = self.make_node()
        self.assertEqual(self.state()["published"], cid)
        with mock.patch.object(builder, "build_file", side_effect=AssertionError("a file was read")):
            self.tick()
        self.assertEqual((self.state()["state"], self.sequence()), (SYNCED, 1))
        self.node.stop()
        self.write("index.html", b"<h1>changed while stopped</h1>")
        self.node = self.make_node()
        self.tick(PUBLISH_INTERVAL)
        self.tick(SETTLE_TIME)
        self.assertEqual(self.target(), self.folder_cid())
        self.assertEqual(self.sequence(), 2)

    def test_restart_keeps_the_hourly_limit(self):
        """The time of the last publish is saved, so a restart doesn't let a sync publish sooner."""
        self.synced()
        self.write("index.html", b"<h1>changed</h1>")
        self.node.stop()
        self.node = self.make_node()
        self.tick()
        self.tick(SETTLE_TIME)
        self.assertEqual((self.state()["state"], self.sequence()), (WAITING, 1))

    def test_two_names_may_sync_one_folder(self):
        """One folder synced by two names: each publishes the same root, pinned by both names."""
        self.node.name_create("mirror")
        self.node.name_sync("site", self.folder)
        self.node.name_sync("mirror", self.folder)
        self.tick()
        self.tick(SETTLE_TIME)
        cid = self.folder_cid()
        self.assertEqual((self.target("site"), self.target("mirror")), (cid, cid))
        self.assertEqual(self.node.storage.pins()[Cid.parse(cid)],
                         {PIN_SOURCE_PREFIX + "site", PIN_SOURCE_PREFIX + "mirror"})

    def test_sync_to_another_folder_replaces_it(self):
        """Syncing a name again to another folder publishes that one instead (the hourly limit still counted)."""
        self.synced()
        other = os.path.join(self.tmp.name, "other")
        self.write("page.html", b"other", folder=other)
        self.assertEqual(self.node.name_sync("site", other)["path"], other)
        self.assertEqual(len(self.node.name_syncs()), 1)
        self.tick()
        self.tick(SETTLE_TIME)
        self.assertEqual(self.state()["state"], WAITING)
        self.tick(PUBLISH_INTERVAL)
        self.assertEqual(self.target(), self.folder_cid(other))

    def test_name_already_pointing_at_the_folder_isnt_published_again(self):
        """A name already published to the folder's content is synced without a new publish."""
        cid = self.folder_cid()
        self.node.add_path(self.folder, chunker=chunker.FASTCDC)
        self.node.name_publish("site", cid)
        self.node.name_sync("site", self.folder)
        self.tick()
        self.assertEqual((self.state()["state"], self.state()["published"], self.sequence()), (SYNCED, cid, 1))

    def test_private_mode_warns_once(self):
        """In private mode sync's result warns that each publish's announce reaches outsiders, and the sync logs it
        once, not at every publish."""
        self.node.stop()
        self.node = self.make_node(private=True, members=["ab" * 16])
        self.assertIn("warning", self.node.name_sync("site", self.folder))
        with mock.patch.object(name_sync, "log") as logged:
            self.tick()
            self.tick(SETTLE_TIME)
            self.write("index.html", b"<h1>changed</h1>")
            self.tick(PUBLISH_INTERVAL)
            self.tick(SETTLE_TIME)
        self.assertEqual(self.sequence(), 2)
        warnings = [call for call in logged.call_args_list if "private mode" in call.args[0]]
        self.assertEqual(len(warnings), 1)

    def test_denied_content_pauses(self):
        """A folder holding a file the deny list denies by hash isn't published: the sync pauses saying why."""
        cid = self.synced()
        data = b"denied content"
        self.write("bad.txt", data)
        self.node.deny.add([f"sha256:{hashlib.sha256(data).hexdigest()}"])
        self.tick(PUBLISH_INTERVAL)
        self.tick(SETTLE_TIME)
        self.assertEqual(self.state()["state"], PAUSED)
        self.assertIn("deny list", self.state()["reason"])
        self.assertEqual(self.target(), cid)

    def test_invalid_rows_are_dropped_at_load(self):
        """A saved sync that doesn't parse is logged and deleted when the node starts."""
        self.node.stop()
        statedb_rows = [("bad alias!", statedb.dump(self.folder)), ("rel", statedb.dump("relative"))]
        db = statedb.connect(self.node.config.state_dir)
        try:
            with db.transaction() as tx:
                tx.executemany("INSERT INTO name_syncs (alias, path, added) VALUES (?, ?, 0)", statedb_rows)
        finally:
            db.close()
        self.node = self.make_node()
        self.assertEqual(self.node.name_syncs(), [])
        self.assertEqual(state_rows(self.node.config.state_dir, "SELECT alias FROM name_syncs"), [])


class SyncThreadTest(unittest.TestCase):
    """run(), the node's rnfs-name-sync thread: a sync started is scanned at once, and the thread ends when told."""

    def test_thread_publishes_and_stops(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        node = RnfsNode.offline(Config(rnfs_dir=os.path.join(tmp, "node"), slow="no"), NODE_ID, None, FakeNetwork())
        self.addCleanup(node.stop)
        node.name_create("site")
        folder = os.path.join(tmp, "site")
        os.makedirs(folder)
        with open(os.path.join(folder, "a.txt"), "wb") as f:
            f.write(b"a")
        stop = threading.Event()
        thread = threading.Thread(target=node.folder_syncs.run, args=(stop,), daemon=True)
        with mock.patch.object(name_sync, "SETTLE_TIME", 0.0):
            thread.start()
            node.name_sync("site", folder)
            wait_for(lambda: node.name_list()[0]["cid"] is not None, what="the sync's publish")
            stop.set()
            node.folder_syncs.wake()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(node.name_syncs()[0]["state"], SYNCED)


class FolderGoneTest(unittest.TestCase):
    """A synced folder removed for good stays paused: never published empty."""

    def test_removed_folder_stays_paused(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        node = RnfsNode.offline(Config(rnfs_dir=os.path.join(tmp, "node"), slow="no"), NODE_ID, None, FakeNetwork())
        self.addCleanup(node.stop)
        mono = FakeClock()
        node.folder_syncs.monotonic = mono
        node.name_create("site")
        folder = os.path.join(tmp, "site")
        os.makedirs(folder)
        node.name_sync("site", folder)
        shutil.rmtree(folder)
        for _ in range(3):
            node.folder_syncs.tick()
            mono.now += SETTLE_TIME
        self.assertEqual(node.name_syncs()[0]["state"], PAUSED)
        self.assertIsNone(node.name_list()[0]["cid"])


if __name__ == "__main__":
    unittest.main()
