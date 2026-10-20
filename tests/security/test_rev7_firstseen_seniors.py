"""REV7: a flood of fresh identities kept honest servers out of the announce first-seen log's seniors.

The log (specification section 6, "Announce first-seen log") keeps at most 65,536 servers and forgot the least
recently heard first. Honest servers announce every 6 hours (`announce_interval`), so about 3 announces a second of
fresh identities (65,536 in 6 hours) pushed every honest server out between its announces: each announce found it
new again, its first-heard time reset to now, and none ever reached `provider_seniority`, which publishers' and
searchers' senior rule needs (`FirstSeenLog.closest(min_age=...)`, rnfs.node's `_senior_servers`).

Now a full log forgets first a server silent for 2 × `announce_interval` (counted from the later of its last announce
and the log's load), then the server not yet senior that was first heard most recently (the newcomer itself, in a
flood), and a live senior server never while a newer one can go. Rebuilt here at a tenth of the scale: a log of 6,553
rows, 200 honest servers announcing every 6 hours, and a fresh identity every 3.3 s (a tenth of 3 a second), for 2
days.
"""
import os
import random
import tempfile
import unittest

from rnfs.config import Config
from rnfs.constants import ID_LENGTH, K
from rnfs.dht.firstseen import MAX_FIRST_SEEN, FirstSeenLog
from tests.fakes.log import quiet_log
from tests.support import FakeClock
from tests.test_names import FakeNetwork
from tests.test_node import NetClient

setUpModule, tearDownModule = quiet_log()

WALL = 1_700_000_000.0
HOUR = 3600.0
DAY = 24 * HOUR
ANNOUNCE_INTERVAL = 6 * HOUR  # the default announce_interval
SENIORITY = DAY  # the default provider_seniority
ROWS = MAX_FIRST_SEEN // 10
HONEST = 200
FLOOD_EVERY = 6 * HOUR / (1.5 * ROWS)  # fresh identities: 1.5 times the log's rows every announce interval


def flood(log: FirstSeenLog, clock: FakeClock, days: float, seed: int = 7) -> list[bytes]:
    """Honest servers announcing every ANNOUNCE_INTERVAL for days, and from their first round on (the log has heard
    each once) a fresh identity every FLOOD_EVERY seconds: the honest servers' IDs."""
    rng = random.Random(seed)
    honest = [rng.randbytes(ID_LENGTH) for _ in range(HONEST)]
    phases = {n: rng.uniform(0, ANNOUNCE_INTERVAL) for n in honest}
    events = [(WALL + phases[n] + k * ANNOUNCE_INTERVAL, n) for n in honest
              for k in range(int(days * DAY / ANNOUNCE_INTERVAL))]
    events += [(WALL + ANNOUNCE_INTERVAL + k * FLOOD_EVERY, None)
               for k in range(int((days * DAY - ANNOUNCE_INTERVAL) / FLOOD_EVERY))]
    for when, n in sorted(events, key=lambda event: event[0]):
        clock.now = when
        log.heard(rng.randbytes(ID_LENGTH) if n is None else n)
    clock.now = WALL + days * DAY
    return honest


class FirstSeenSeniorsTest(unittest.TestCase):
    """The flood against a log with the node's settings."""

    def test_honest_servers_reach_seniority_under_a_flood(self):
        clock = FakeClock(WALL)
        log = FirstSeenLog(clock=clock, max_rows=ROWS, seniority=SENIORITY, silence=2 * ANNOUNCE_INTERVAL)
        honest = flood(log, clock, days=2)
        self.assertEqual(len(log), ROWS)  # the flood kept it full
        senior = [n for n in honest if log.first_heard(n) is not None and log.first_heard(n) <= clock.now - SENIORITY]
        self.assertEqual(len(senior), HONEST)
        # The senior rule finds them: every key's K closest senior servers heard lately are honest
        rng = random.Random(3)
        for _ in range(20):
            found = log.closest(rng.randbytes(ID_LENGTH), K, min_age=SENIORITY, heard_within=2 * ANNOUNCE_INTERVAL)
            self.assertEqual(len(found), K)
            self.assertLessEqual(set(found), set(honest))

    def test_the_node_gives_the_log_its_settings(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Config(rnfs_dir=os.path.join(tmp, "node"), provider_seniority=2 * DAY, announce_interval=HOUR)
            from rnfs.node import RnfsNode
            node = RnfsNode.offline(config, bytes([0xA] * ID_LENGTH), NetClient(), FakeNetwork())
            try:
                self.assertEqual((node.first_seen.seniority, node.first_seen.silence), (2 * DAY, 2 * HOUR))
            finally:
                node.stop()


if __name__ == "__main__":
    unittest.main()
