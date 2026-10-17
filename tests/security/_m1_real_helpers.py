"""Helpers for test_m1_real_rns: a process that floods a node's rnfs.dht with link requests that never complete.

Run as its own OS process (python3 _m1_real_helpers.py flood CONFIGDIR NODE_ID RATE SECONDS) over the loopback
topology of _h1_real_helpers. Each link request is a genuine RNS one; the flooder never processes the proofs
(Link.validate_proof is replaced), so it never sends the RTT packet and every link stays half-open at the node.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from tests.security import _h1_real_helpers as h


def role_flood(configdir, node_id_hex, rate, seconds):
    """Send link requests to the node at rate per second for seconds, never completing one."""
    RNS = h._rns()
    RNS.Link.validate_proof = lambda self, packet: None  # never answer the proof: no RTT packet is sent
    RNS.Reticulum(configdir=os.path.abspath(configdir))
    dest = h.resolve(node_id_hex)
    print("FLOOD_READY", flush=True)
    sent = 0
    start = time.monotonic()
    while time.monotonic() - start < float(seconds):
        RNS.Link(dest)
        sent += 1
        time.sleep(max(0.0, start + sent / float(rate) - time.monotonic()))
    print("FLOOD_DONE", sent, flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    if sys.argv[1] != "flood":
        raise SystemExit(f"unknown role {sys.argv[1]}")
    role_flood(*sys.argv[2:6])
