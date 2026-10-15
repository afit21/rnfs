"""L4: announces of many other names push a gateway-resolved name out of the bounded name table, after which a
replayed, validly signed older announce of it is accepted and a gateway resolve rolls back to it.

A NameManager over FakeNetwork resolves name N for the gateway (persist=False) at sequence 5. Then fresh names, as many
as the default name_table_size (10,000; about 0.35 s here), are announced, and N's older announce at sequence 3 is
replayed. The next gateway resolve must not return less than 5. A control shows the replay is refused when no flood
has evicted N. Further tests pin down the fix: unasked announces take only places of names that were only heard, and
the highest sequence of names resolved for the gateway is a saved floor that outlasts table churn and restarts.
"""
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


def flood_name(i: int) -> bytes:
    """The i-th name of the flood, never N."""
    return (0x10 ** 30 + i).to_bytes(16, "big")


class GatewayRollbackTest(unittest.TestCase):
    """A started NameManager, with no keepers, that has resolved N at sequence 5 for the gateway."""

    def setUp(self):
        """The manager, N's current announce heard, and one gateway resolve of it."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.config = Config(rnfs_dir=tmp.name, path_timeout=0.05)
        self.assertEqual(self.config.name_table_size, 10_000)
        self.network = FakeNetwork()
        self.clock = FakeClock(1_000_000.0)
        self.manager = NameManager(self.config, FakePins(), network=self.network, clock=self.clock,
                                   monotonic=self.clock)
        self.manager.start()
        self.addCleanup(self.manager.stop)
        self.network.announce(N, encode_record(5, NEW), self.clock.now)
        self.assertEqual(self.manager.resolve(N, persist=False)["sequence"], 5)

    def replay_old(self):
        """N's older announce, validly signed, arrives again."""
        self.clock.now += 10
        self.network.announce(N, encode_record(3, OLD), self.clock.now)

    def test_control_replay_refused_without_flood(self):
        self.replay_old()
        result = self.manager.resolve(N, persist=False)
        self.assertEqual((result["sequence"], result["cid"]), (5, str(NEW)))

    def test_no_rollback_after_table_churn(self):
        for i in range(self.config.name_table_size):
            self.clock.now += 0.01
            self.network.announce(flood_name(i), encode_record(1, OLD), self.clock.now)
        self.replay_old()
        result = self.manager.resolve(N, persist=False)
        self.assertGreaterEqual(result["sequence"], 5, "gateway resolve went back from sequence 5")

    def test_flood_takes_only_heard_places(self):
        """Announces of names never resolved here fill only the table places not held by resolved names."""
        for i in range(2 * self.config.name_table_size):
            self.network.announce(flood_name(i), encode_record(1, OLD), self.clock.now)
        self.assertIn(N, self.manager._table)
        self.assertEqual(len(self.manager._table), self.config.name_table_size)

    def test_no_rollback_after_restart(self):
        """The floor is saved: after a restart the old announce is still refused, a newer one accepted."""
        self.manager.stop()
        self.network.app_data.clear()
        restarted = NameManager(self.config, FakePins(), network=self.network, clock=self.clock, monotonic=self.clock)
        restarted.start()
        self.addCleanup(restarted.stop)
        self.replay_old()
        self.assertIsNone(restarted.resolve(N, persist=False))  # the record itself isn't kept, only the floor
        self.network.announce(N, encode_record(6, NEW), self.clock.now)
        self.assertEqual(restarted.resolve(N, persist=False)["sequence"], 6)


class FloorChurnTest(unittest.TestCase):
    """Gateway resolves of other names push N out of a small table, not out of the larger floor."""

    def setUp(self):
        """A manager with a table of 3 names, which resolved N at sequence 5."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.config = Config(rnfs_dir=tmp.name, path_timeout=0.05, name_table_size=3)
        self.network = FakeNetwork()
        self.clock = FakeClock(1_000_000.0)
        self.manager = NameManager(self.config, FakePins(), network=self.network, clock=self.clock,
                                   monotonic=self.clock)
        self.manager.start()
        self.addCleanup(self.manager.stop)
        self.network.announce(N, encode_record(5, NEW), self.clock.now)
        self.assertEqual(self.manager.resolve(N, persist=False)["sequence"], 5)

    def test_resolve_churn(self):
        for i in range(5):
            self.network.announce(flood_name(i), encode_record(1, OLD), self.clock.now)
            self.manager.resolve(flood_name(i), persist=False)
        self.assertNotIn(N, self.manager._table)
        self.network.app_data.clear()
        self.network.announce(N, encode_record(3, OLD), self.clock.now)
        result = self.manager.resolve(N, persist=False)
        self.assertTrue(result is None or result["sequence"] >= 5, f"gateway resolve went back from 5: {result}")

    def test_floor_bounded(self):
        """Names resolved over FLOOR_KEEP_AGE ago are forgotten past FLOOR_FACTOR × name_table_size."""
        for i in range(names.FLOOR_FACTOR * 3 + 5):
            self.clock.now += names.FLOOR_KEEP_AGE
            self.network.announce(flood_name(i), encode_record(1, OLD), self.clock.now)
            self.manager.resolve(flood_name(i), persist=False)
        self.assertEqual(len(self.manager._floor), names.FLOOR_FACTOR * 3)

    def test_damaged_floor_file_is_set_aside(self):
        self.manager.stop()
        path = os.path.join(self.config.state_dir, names.FLOOR_FILE)
        with open(path, "w", encoding="utf-8") as f:
            f.write("{not json")
        restarted = NameManager(self.config, FakePins(), network=self.network, clock=self.clock, monotonic=self.clock)
        self.assertEqual(len(restarted._floor), 0)
        self.assertTrue(any(entry.startswith(names.FLOOR_FILE + ".damaged") for entry in os.listdir(self.config.state_dir)))


if __name__ == "__main__":
    unittest.main()
