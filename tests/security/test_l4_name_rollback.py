"""L4: announces of many other names push a gateway-resolved name out of the bounded name table, after which a
replayed, validly signed older announce of it is accepted and a gateway resolve rolls back to it.

A NameManager over FakeNetwork resolves name N for the gateway (persist=False) at sequence 5. Then fresh names, as many
as the default name_table_size (10,000; about 0.35 s here), are announced, and N's older announce at sequence 3 is
replayed. The next gateway resolve must not return less than 5. A control shows the replay is refused when no flood
has evicted N.
"""
import tempfile
import unittest

import RNS

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

    # L4: fails until announces only update names this node resolved or keeps, or a floor survives eviction
    @unittest.expectedFailure
    def test_no_rollback_after_table_churn(self):
        for i in range(self.config.name_table_size):
            self.clock.now += 0.01
            self.network.announce(flood_name(i), encode_record(1, OLD), self.clock.now)
        self.replay_old()
        result = self.manager.resolve(N, persist=False)
        self.assertGreaterEqual(result["sequence"], 5, "gateway resolve went back from sequence 5")


if __name__ == "__main__":
    unittest.main()
