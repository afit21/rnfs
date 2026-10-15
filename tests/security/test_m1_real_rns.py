"""M1 over a real Reticulum network: a burst of link requests that never complete stays bounded at a real rnfs
daemon, while an honest link still establishes and is served.

Same loopback topology as test_h1_real_rns (a TCP hub, a real rnfs daemon serving one 64 KiB public CID, every
other party a separate OS process), with max_incoming_links = 16, which is also the half-open cap. A flooder
process sends genuine RNS link requests at FLOOD_RATE per second for FLOOD_SECONDS and never answers a proof
(see _m1_real_helpers). Each half-open link costs the daemon a watchdog thread, so its thread count shows how
many it holds: without the guard it grows by one per request (FLOOD_RATE × FLOOD_SECONDS, RNS keeps them for
366 s); with it, by at most the cap plus the links closed in the last WATCHDOG_MAX_SLEEP (5 s), whose watchdog
threads haven't woken to exit yet. Linux only (/proc).

Opt in with RNFS_E2E=1 (real daemons and RNS networking), like tests/test_e2e.py.
"""
import os
import sys
import threading
import time
import unittest
from unittest import mock

from tests import support
from tests.security import _h1_real_helpers as helpers
from tests.security import _m1_real_helpers
from tests.security import test_h1_real_rns as h1  # not the class itself, which discovery would run again

CAP = 16
FLOOD_RATE = 10  # link requests per second
FLOOD_SECONDS = 30
LINGER = 6  # seconds a closed half-open link's watchdog thread may still be asleep (WATCHDOG_MAX_SLEEP = 5)
SLACK = 30  # other threads the daemon may start meanwhile


def threads(pid: int) -> int | None:
    """Number of threads of process pid, or None if it can't be read."""
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("Threads:"):
                    return int(line.split()[1])
    except OSError:
        return None
    return None


@unittest.skipUnless(os.environ.get("RNFS_E2E") == "1", "set RNFS_E2E=1 to run")
@unittest.skipUnless(os.path.exists(f"/proc/{os.getpid()}/status"), "needs /proc to count threads")
class M1RealRnsTest(unittest.TestCase):
    """Half-open links at a real rnfs daemon stay within their cap under a flood of link requests."""

    _spawn = h1.H1RealRnsTest._spawn
    _status = h1.H1RealRnsTest._status
    _stop_all = h1.H1RealRnsTest._stop_all
    _honest = h1.H1RealRnsTest._honest

    def setUp(self):
        with mock.patch.object(helpers, "VICTIM_TOML", helpers.VICTIM_TOML + f"max_incoming_links = {CAP}\n"):
            h1.H1RealRnsTest.setUp(self)
        self.victim = next(p for p in self.procs if "daemon" in p.args)

    def test_link_requests_that_never_complete_stay_bounded(self):
        baseline = self._honest("honest_baseline", 4)
        self.assertGreater(baseline["served"], 0, f"baseline should serve blocks: {baseline}")
        before = threads(self.victim.pid)
        self.assertIsNotNone(before)

        flood_dir = os.path.join(self.base, "flood")
        helpers.write(os.path.join(flood_dir, "config"), helpers.CLIENT_CONFIG.format(port=self.tcp_port))
        flood_log = os.path.join(self.base, "flood.log")
        self._spawn([sys.executable, _m1_real_helpers.__file__, "flood", flood_dir, self.node_id,
                     str(FLOOD_RATE), str(FLOOD_SECONDS)], "flood", env={**os.environ, "PYTHONUNBUFFERED": "1"})

        def logged(marker):
            with open(flood_log, "rb") as f:
                return marker in f.read()
        support.wait_for(lambda: logged(b"FLOOD_READY"), 90, "the flooder to find the node", interval=0.5)

        peak = [before]
        stop = threading.Event()

        def sample():
            while not stop.is_set():
                count = threads(self.victim.pid)
                if count is not None:
                    peak[0] = max(peak[0], count)
                stop.wait(0.2)
        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        try:
            time.sleep(FLOOD_SECONDS / 2)
            # Mid-flood, an honest requester still establishes a link and is served
            attack = self._honest("honest_attack", 6)
            support.wait_for(lambda: logged(b"FLOOD_DONE"), FLOOD_SECONDS + 60, "the flood to finish", interval=0.5)
        finally:
            stop.set()
            sampler.join(5)

        unguarded = FLOOD_RATE * FLOOD_SECONDS
        bound = CAP + LINGER * FLOOD_RATE + SLACK
        grew = peak[0] - before
        print(f"\nM1 real RNS: victim threads {before} -> peak {peak[0]} (+{grew}); bound +{bound}; "
              f"unguarded up to +{unguarded}; honest during the flood: {attack}", file=sys.stderr)
        self.assertGreater(attack["served"], 0, f"an honest requester was locked out by the flood: {attack}")
        self.assertLessEqual(grew, bound, f"M1: half-open links weren't bounded: the daemon gained {grew} threads "
                                          f"from {unguarded} link requests that never completed")


if __name__ == "__main__":
    unittest.main()
