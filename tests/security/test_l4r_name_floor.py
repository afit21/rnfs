"""L4-R: after the L4 fix, the floor of names resolved for the gateway (persist=False) was saved only by the
re-announce loop, at most a minute later, so a crash soon after a resolve lost it; and it forgot the least recently
resolved names once it held 4 × name_table_size, so that many resolves of other names, however recent, let a
replayed old announce of a name back in.

A NameManager over FakeNetwork (as tests/security/test_l4_name_rollback.py) resolves name N at sequence 5 for the
gateway. A "crash" is a second manager on the same state directory without stop(). The floor must be on disk at once
(within FLOOR_SAVE_INTERVAL of the last save), names resolved within FLOOR_KEEP_AGE must outlast churn up to a hard
bound, and the file must stay bounded.
"""
import json
import os
import tempfile
import unittest

import RNS

from rnfs import names
from rnfs.config import Config
from rnfs.content.cid import RAW, Cid
from rnfs.names import NameManager, encode_record
from tests.support import FakeClock
from tests.test_names import FakeNetwork, FakePins

N = bytes([0x4E] * 16)
OLD = Cid.for_block(RAW, b"old")
NEW = Cid.for_block(RAW, b"new")
HARD_FACTOR = 2  # rnfs.names.FLOOR_HARD_FACTOR: recent names are kept up to twice the usual floor


def other_name(i: int) -> bytes:
    """The i-th other name, never N."""
    return (0x10 ** 30 + i).to_bytes(16, "big")


class FloorTest(unittest.TestCase):
    """A started manager with a table of 3 names (so a floor of 12), which resolved N at sequence 5."""

    def setUp(self):
        """The manager, and N resolved for the gateway."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.config = Config(rnfs_dir=tmp.name, path_timeout=0.05, name_table_size=3)
        self.network = FakeNetwork()
        self.clock = FakeClock(1_000_000.0)
        self.manager = self.start_manager()
        self.network.announce(N, encode_record(5, NEW), self.clock.now)
        self.assertEqual(self.manager.resolve(N, persist=False)["sequence"], 5)

    def start_manager(self) -> NameManager:
        """A manager on the state directory, started."""
        manager = NameManager(self.config, FakePins(), network=self.network, clock=self.clock, monotonic=self.clock)
        manager.start()
        self.addCleanup(manager.stop)
        return manager

    def resolve_other(self, i: int):
        """A gateway resolve of another name, announced at sequence 1."""
        self.network.announce(other_name(i), encode_record(1, OLD), self.clock.now)
        self.manager.resolve(other_name(i), persist=False)

    def replayed_resolve(self, manager: NameManager):
        """N's older announce arrives again; the gateway resolves N with manager."""
        self.clock.now += 10
        self.network.app_data.clear()
        self.network.announce(N, encode_record(3, OLD), self.clock.now)
        return manager.resolve(N, persist=False)

    def saved_names(self) -> set[bytes]:
        """The names in name-floor.json (none if there is no such file)."""
        try:
            with open(os.path.join(self.config.state_dir, names.FLOOR_FILE), encoding="utf-8") as f:
                return {bytes.fromhex(entry[0]) for entry in json.load(f)["floor"]}
        except FileNotFoundError:
            return set()

    def test_floor_survives_a_crash_right_after_the_resolve(self):
        crashed = self.start_manager()  # the first one never stopped, nor reached announce_due
        result = self.replayed_resolve(crashed)
        # L4-R: fails until a resolve that raises the floor saves it promptly
        self.assertTrue(result is None or result["sequence"] >= 5, f"resolve after a crash went back from 5: {result}")

    def test_saves_are_rate_limited(self):
        """A second raise within FLOOR_SAVE_INTERVAL of a save waits; the first after it saves both."""
        # L4-R: fails until a resolve that raises the floor saves it promptly
        self.assertIn(N, self.saved_names(), "the floor wasn't saved by the resolve")
        self.resolve_other(1)
        self.assertNotIn(other_name(1), self.saved_names())
        self.clock.now += names.FLOOR_SAVE_INTERVAL
        self.resolve_other(2)
        self.assertTrue({N, other_name(1), other_name(2)} <= self.saved_names())

    def test_recent_names_outlast_churn(self):
        for i in range(names.FLOOR_FACTOR * 3 + 5):
            self.resolve_other(i)
        result = self.replayed_resolve(self.manager)
        # L4-R: fails until names resolved within FLOOR_KEEP_AGE are kept past FLOOR_FACTOR × name_table_size
        self.assertTrue(result is None or result["sequence"] >= 5, f"gateway resolve went back from 5: {result}")

    def test_floor_stays_bounded(self):
        hard = HARD_FACTOR * names.FLOOR_FACTOR * 3
        for i in range(hard + 5):
            self.resolve_other(i)
        # L4-R: fails until recent names are kept up to the hard bound (the old floor held FLOOR_FACTOR × 3)
        self.assertEqual(len(self.manager._floor), hard)
        self.assertEqual(names.FLOOR_HARD_FACTOR, HARD_FACTOR)
        self.assertNotIn(N, self.manager._floor)  # the least recently resolved went first
        # Once they are older than FLOOR_KEEP_AGE, the next resolve brings the floor back to its usual size
        self.clock.now += names.FLOOR_KEEP_AGE
        self.resolve_other(hard + 5)
        self.assertEqual(len(self.manager._floor), names.FLOOR_FACTOR * 3)
        self.manager.stop()
        self.assertEqual(len(self.saved_names()), names.FLOOR_FACTOR * 3)


if __name__ == "__main__":
    unittest.main()
