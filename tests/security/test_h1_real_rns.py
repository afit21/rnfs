"""H1: one peer can hold every /blocks response slot, so honest requesters are refused.

Reproduced over a REAL Reticulum network (RNS 1.5.6) between separate OS processes: a loopback TCP hub, a
real rnfs daemon serving one public 64 KiB CID with the default config, an attacker process that holds the
response slots, and an honest requester process issuing the same /blocks requests rnfs's own client sends.

The attacker opens a handful of links, inflates each link's rtt (so a response it never downloads stays
ADVERTISED, holding its slot, since RNS times an advertised Resource out after link.rtt * 6), and keeps
firing /blocks for 15 copies of the CID (a ~960 KiB Resource -- duplicates are kept, see L2). With only two
fast-pool slots by default (max_blocks_responses = 4), both fill and every honest request is refused with
"every fast-link response slot is busy".

This test asserts the SAFE behaviour -- that an honest requester keeps being served while the attacker runs
-- so it fails until H1 is fixed. See @unittest.expectedFailure below.

Opt in with RNFS_E2E=1 (real daemons and RNS networking), like tests/test_e2e.py.
"""
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

from tests import support
from tests.security import _h1_real_helpers as helpers

HELPER = helpers.__file__
ROOT = helpers.ROOT
MAIN = helpers.MAIN
# H1 is a timing race the attacker wins by polling; the safe refusal rate is near 0, the attacked one near 1.
SAFE_REFUSAL_RATE = 0.3


@unittest.skipUnless(os.environ.get("RNFS_E2E") == "1", "set RNFS_E2E=1 to run")
class H1RealRnsTest(unittest.TestCase):
    """A real rnfs daemon's /blocks service is denied to an honest process by one attacker process."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmp.cleanup)
        self.base = self.tmp.name
        self.procs = []
        self.logs = []
        self.addCleanup(self._stop_all)

        tcp_port = helpers.free_port()
        # Hub: a plain RNS transport node.
        hub_dir = os.path.join(self.base, "hub")
        helpers.write(os.path.join(hub_dir, "config"), helpers.HUB_CONFIG.format(port=tcp_port))
        self._spawn([sys.executable, HELPER, "hub", hub_dir], "hub")
        time.sleep(3)

        # Victim: a real rnfs daemon with the default config, serving one public 64 KiB CID.
        self.victim_dir = os.path.join(self.base, "victim")
        helpers.write(os.path.join(self.victim_dir, "config"), helpers.CLIENT_CONFIG.format(port=tcp_port))
        helpers.write(os.path.join(self.victim_dir, "rnfs.toml"), helpers.VICTIM_TOML)
        self.api_port = helpers.free_port()
        gw_port = helpers.free_port()
        self._spawn([sys.executable, MAIN, "--dir", self.victim_dir, "--api-port", str(self.api_port),
                     "daemon", "--rnsconfig", self.victim_dir, "--gateway-port", str(gw_port)], "victim",
                    env={**os.environ, "PYTHONUNBUFFERED": "1"})
        self.node_id = support.wait_for(self._status, 60, "victim daemon to start")["node_id"]

        blob = os.path.join(self.base, "public.bin")
        with open(blob, "wb") as f:
            f.write(os.urandom(64 * 1024))
        self.cid = subprocess.run([sys.executable, MAIN, "--dir", self.victim_dir, "--api-port",
                                   str(self.api_port), "add", blob],
                                  capture_output=True, timeout=60).stdout.decode().strip()
        self.tcp_port = tcp_port

    def _spawn(self, argv, name, env=None):
        log = open(os.path.join(self.base, f"{name}.log"), "wb")
        self.logs.append(log)
        proc = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, env=env)
        self.procs.append(proc)
        return proc

    def _status(self):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.api_port}/api/status", timeout=2) as r:
                return json.load(r)
        except OSError:
            return None

    def _stop_all(self):
        for p in self.procs:
            if p.poll() is None:
                p.send_signal(signal.SIGINT)
        time.sleep(2)
        for p in self.procs:
            if p.poll() is None:
                p.kill()
        for log in self.logs:
            try:
                log.close()
            except Exception:
                pass

    def _honest(self, name, duration, interval=1.0):
        """Run an honest requester process for duration seconds; return its outcome counts."""
        cfg_dir = os.path.join(self.base, name)
        helpers.write(os.path.join(cfg_dir, "config"), helpers.CLIENT_CONFIG.format(port=self.tcp_port))
        out = os.path.join(self.base, f"{name}.json")
        with open(os.path.join(self.base, f"{name}.log"), "wb") as log:
            subprocess.run([sys.executable, HELPER, "honest", cfg_dir, self.node_id, self.cid,
                            str(duration), str(interval), out], timeout=duration + 120, check=True,
                           stdout=log, stderr=subprocess.STDOUT)
        with open(out) as f:
            return json.load(f)

    def test_one_peer_denies_the_blocks_service(self):
        # Baseline: honest alone is always served.
        baseline = self._honest("honest_baseline", 6)
        self.assertEqual(baseline["refused"], 0, f"baseline should never be refused: {baseline}")
        self.assertGreater(baseline["served"], 0, f"baseline should serve blocks: {baseline}")

        # Attacker: hold the slots.
        atk_dir = os.path.join(self.base, "attacker")
        helpers.write(os.path.join(atk_dir, "config"), helpers.CLIENT_CONFIG.format(port=self.tcp_port))
        atk_log = os.path.join(self.base, "attacker.log")
        self._spawn([sys.executable, HELPER, "attacker", atk_dir, self.node_id, self.cid], "attacker",
                    env={**os.environ, "PYTHONUNBUFFERED": "1"})

        def ready():
            if not os.path.exists(atk_log):
                return False
            with open(atk_log, "rb") as f:
                return b"ATTACKER_READY" in f.read()
        support.wait_for(ready, 90, "attacker to hold the slots", interval=0.5)
        time.sleep(8)  # let the held responses settle into both fast-pool slots

        # Safe behaviour: the honest requester should still be served. It is not -- H1.
        attack = self._honest("honest_attack", 20)
        self.assertLessEqual(
            attack["refusal_rate"], SAFE_REFUSAL_RATE,
            f"H1: one attacker denied the /blocks service to an honest peer: {attack}")


if __name__ == "__main__":
    unittest.main()
