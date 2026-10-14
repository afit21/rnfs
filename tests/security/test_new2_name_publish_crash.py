"""NEW-2: a crash or a failed save inside `name publish` left a `name:` pin nothing could remove.

publish() pinned the new target, released the old one (each saved to pins.json at once) and only then saved
names.json. A crash in between left names.json pointing at the old target, now unpinned (gc deleted it while the
name was announced at it), and the new target pinned by `name:<alias>`, which `pin rm` refuses and `name rm` and
later publishes never released. A full disk at names.json had the same effect without a crash. These tests crash
an offline node at each step of a publish (a BaseException from a wrapped pin store, as the reviewer's script
does), restart it on the same directory and check that the name pins match names.json, that gc keeps the
recorded target and that `name rm` leaves no pin; and they make the saves fail with OSError.
"""
import errno
import io
import os
import tempfile
import unittest

import RNS

from rnfs.config import Config
from rnfs.content.cid import Cid
from rnfs.node import RnfsNode
from tests.test_names import FakeNetwork
from tests.test_node import NetClient

A_ID = bytes([0xA] * 16)
SOURCE = "name:site"

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node's log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class Crash(BaseException):
    """Stands in for the process dying."""


class FailingPins:
    """The NameManager's pins, failing at a chosen step: crash or raise OSError before or after pin or unpin."""

    def __init__(self, pins, fail: str, error: BaseException):
        """fail is "after pin", "before unpin" or "" (never); error is what is raised there."""
        self.pins = pins
        self.fail = fail
        self.error = error

    def pin(self, cid, source):
        """Pin, then fail if asked to."""
        self.pins.pin(cid, source)
        if self.fail == "after pin":
            self.fail = ""
            raise self.error

    def unpin(self, cid, source):
        """Fail if asked to, else unpin."""
        if self.fail == "before unpin":
            self.fail = ""
            raise self.error
        return self.pins.unpin(cid, source)


class NamePublishCrashTest(unittest.TestCase):
    """An offline node publishing a name at one cached target, then at another."""

    def setUp(self):
        """A temporary directory for the node."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def open_node(self, root: str) -> RnfsNode:
        """An offline node on root (a restart when root was used before)."""
        net = NetClient()
        net.sender = A_ID
        node = RnfsNode.offline(Config(rnfs_dir=root, slow="no", lookup_timeout=1, path_timeout=0.2), A_ID, net,
                                FakeNetwork())
        net.nodes[A_ID] = node
        return node

    def first_publish(self, root: str) -> tuple[RnfsNode, Cid, Cid]:
        """A node with two cached (unpinned) targets, "site" published at the first."""
        node = self.open_node(root)
        cid1 = Cid.parse(node.add_stream(io.BytesIO(os.urandom(5000)), pin=False)["cid"])
        cid2 = Cid.parse(node.add_stream(io.BytesIO(os.urandom(5000)), pin=False)["cid"])
        node.name_create("site")
        node.name_publish("site", str(cid1))
        return node, cid1, cid2

    @staticmethod
    def name_pins(node: RnfsNode) -> set[Cid]:
        """The CIDs the name's pin source holds."""
        return node.storage.pinned_by(SOURCE)

    def test_crash_at_each_step_leaves_pins_matching_names_json(self):
        """After the restart, the name's pins are exactly its recorded target, which gc keeps and `name rm`
        releases, wherever the publish crashed."""
        problems = []
        for step in ("after pin", "in save", "before unpin"):
            with self.subTest(step=step):
                root = os.path.join(self.tmp.name, step.replace(" ", "-"))
                a, cid1, cid2 = self.first_publish(root)
                a.names._pins = FailingPins(a.names._pins, step, Crash())
                if step == "in save":
                    def crash():
                        raise Crash()
                    a.names._save = crash
                with self.assertRaises(Crash):
                    a.name_publish("site", str(cid2))
                # a is abandoned without stop(), as in a real crash: only what it wrote survives

                b = self.open_node(root)
                recorded = Cid.parse(b.name_list()[0]["cid"])
                if self.name_pins(b) != {recorded}:
                    problems.append(f"{step}: name pins {sorted(map(str, self.name_pins(b)))} but names.json "
                                    f"records {recorded}")
                b.gc()
                if not b.storage.is_complete(recorded):
                    problems.append(f"{step}: gc deleted the recorded target {recorded}")
                b.name_rm("site")
                if self.name_pins(b):
                    problems.append(f"{step}: `name rm` left {sorted(map(str, self.name_pins(b)))} pinned")
                b.stop()
        # NEW-2: fails until publish saves names.json between pinning the new target and releasing the old one,
        # and the node reconciles name pins with names.json at start
        self.assertEqual(problems, [])

    def test_failed_save_rolls_the_publish_back(self):
        """names.json can't be written (ENOSPC): the publish fails and nothing changes, in memory or on disk."""
        a, cid1, cid2 = self.first_publish(os.path.join(self.tmp.name, "a"))
        save = a.names._save

        def full():
            a.names._save = save
            raise OSError(errno.ENOSPC, "No space left on device")
        a.names._save = full
        with self.assertRaises(OSError):
            a.name_publish("site", str(cid2))
        listed = a.name_list()[0]
        # NEW-2: fails until a failed names.json save rolls back the publish's pin changes
        self.assertEqual(self.name_pins(a), {cid1})
        self.assertEqual((listed["cid"], listed["sequence"]), (str(cid1), 1))
        self.assertEqual(a.name_publish("site", str(cid2))["sequence"], 2)  # trying again works
        self.assertEqual(self.name_pins(a), {cid2})
        a.stop()

    def test_failed_release_of_the_old_target_is_retried(self):
        """Releasing the old target fails (pins.json can't be written): the publish stands, and the next one
        releases it."""
        a, cid1, cid2 = self.first_publish(os.path.join(self.tmp.name, "a"))
        a.names._pins = FailingPins(a.names._pins, "before unpin", OSError(errno.EIO, "Input/output error"))
        try:
            result = a.name_publish("site", str(cid2))
        except OSError as e:
            # NEW-2: fails until a release that fails after names.json was saved is kept for later, not raised
            self.fail(f"the publish failed after pinning its new target: {e}")
        self.assertEqual(result["sequence"], 2)
        self.assertEqual(self.name_pins(a), {cid1, cid2})  # cid1 still held: nothing was lost
        cid3 = Cid.parse(a.add_stream(io.BytesIO(os.urandom(5000)), pin=False)["cid"])
        a.name_publish("site", str(cid3))
        self.assertEqual(self.name_pins(a), {cid3})
        a.stop()

    def test_name_pins_nothing_records_are_released_at_start(self):
        """Pins left by an earlier version's crash, or of an alias with no key, are released when the node starts;
        a recorded target missing its pin is pinned again."""
        root = os.path.join(self.tmp.name, "a")
        a, cid1, cid2 = self.first_publish(root)
        a.storage.pin(cid2, SOURCE)  # what the old order left after a crash in its save
        a.storage.unpin(cid1, SOURCE)
        a.storage.pin(cid2, "name:gone")  # an alias whose key and record are both gone
        a.stop()
        b = self.open_node(root)
        # NEW-2: fails until name pins are reconciled with names.json at start
        self.assertEqual(self.name_pins(b), {cid1})
        self.assertEqual(b.storage.pinned_by("name:gone"), set())
        b.stop()


if __name__ == "__main__":
    unittest.main()
