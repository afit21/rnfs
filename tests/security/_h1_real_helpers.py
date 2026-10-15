"""Helpers for test_h1_real_rns: a loopback TCP hub, an H1 attacker, and an honest requester, each run
as its own process over real RNS 1.5.6.

This module is both a library (config templates, process orchestration) and a small CLI dispatched by role
(hub | attacker | honest), so the test can spawn separate OS processes -- H1 is about one node refusing a
*different* process's /blocks requests, so it can only be shown across real processes, not in one interpreter.

Nothing here touches rnfs/ code. The attacker and honest clients speak the same /blocks wire format rnfs's
own client sends (rnfs/network/client.py), using rnfs only for its pure protocol constants.
"""
import json
import os
import socket
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

MAIN = os.path.join(ROOT, "rnfs.py")

HUB_CONFIG = """[reticulum]
  enable_transport = Yes
  share_instance = No
  panic_on_interface_error = No
[logging]
  loglevel = 3
[interfaces]
  [[TCP Server]]
    type = TCPServerInterface
    enabled = yes
    listen_ip = 127.0.0.1
    listen_port = {port}
"""

CLIENT_CONFIG = """[reticulum]
  enable_transport = No
  share_instance = No
  panic_on_interface_error = No
[logging]
  loglevel = 3
[interfaces]
  [[TCP Client]]
    type = TCPClientInterface
    enabled = yes
    target_host = 127.0.0.1
    target_port = {port}
"""

# Default rnfs config except for short intervals so the node starts and announces quickly; the H1 knobs
# (max_blocks_responses = 4, deadline_slack = 30, unmeasured_bitrate = 100) are all left at their defaults.
VICTIM_TOML = """default_bootstrap = false
announce_interval = 10
startup_announce_delay = 2
scan_interval = 3
republish_interval = 600
path_timeout = 10
lookup_timeout = 30
"""


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)


# --- raw RNS client helpers (run inside the attacker and honest processes) ---

def _rns():
    import RNS
    return RNS


def resolve(node_id_hex, timeout=60.0):
    """Request a path to the victim's rnfs.dht destination and build an OUT destination to it."""
    RNS = _rns()
    from rnfs.constants import APP_NAME, DHT_ASPECT
    node_id = bytes.fromhex(node_id_hex)
    deadline = time.monotonic() + timeout
    identity = RNS.Identity.recall(node_id)
    i = 0
    while identity is None and time.monotonic() < deadline:
        if i % 15 == 0:  # a path can be cached before its identity is recalled; re-request to pull it
            RNS.Transport.request_path(node_id)
        time.sleep(0.2)
        i += 1
        identity = RNS.Identity.recall(node_id)
    if identity is None:
        raise TimeoutError(f"no path/identity for {node_id_hex}")
    dest = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE, APP_NAME, DHT_ASPECT)
    if dest.hash != node_id:
        raise ValueError("recalled identity's rnfs.dht is not the node ID asked for")
    return dest


def open_link(dest, timeout=30.0, attempts=3):
    """Open a real RNS link and wait until ACTIVE, retrying a few times over a busy loopback hub."""
    RNS = _rns()
    for _ in range(attempts):
        up = threading.Event()
        link = RNS.Link(dest, established_callback=lambda _l: up.set())
        up.wait(timeout)
        if link.status == RNS.Link.ACTIVE:
            return link
        try:
            link.teardown()
        except Exception:
            pass
    raise TimeoutError("link did not become ACTIVE")


def inflate_rtt(link, seconds):
    """Make the victim's link.rtt large (its ADVERTISED-Resource timeout is link.rtt * 6), by sending it an
    LRRTT packet with an inflated value: the victim does link.rtt = max(measured_rtt, value)."""
    RNS = _rns()
    from RNS.vendor import umsgpack
    RNS.Packet(link, umsgpack.packb(float(seconds)), context=RNS.Packet.LRRTT).send()


def send_blocks(link, cid_binaries, max_bytes, timeout=8.0, wait=True):
    """Send one /blocks request (the same shape rnfs's client sends) and return its outcome holder."""
    from rnfs.constants import BLOCKS_PATH
    state = {"response": None, "done": threading.Event()}

    def on_response(receipt):
        state["response"] = receipt.get_response()
        state["done"].set()

    def on_failed(receipt):
        state["done"].set()

    receipt = link.request(BLOCKS_PATH, [int(max_bytes), list(cid_binaries)],
                           response_callback=on_response, failed_callback=on_failed, timeout=timeout)
    if receipt is None:
        state["done"].set()
    if wait:
        state["done"].wait(timeout)
    return state["response"]


def cid_binary(cid_text):
    from rnfs.content.cid import Cid
    return Cid.parse(cid_text).binary


# --- role entry points (each becomes its own OS process) ---

def role_hub(configdir):
    RNS = _rns()
    RNS.Reticulum(configdir=os.path.abspath(configdir))
    while True:
        time.sleep(3600)


def role_attacker(configdir, node_id_hex, cid, n_links=8, rtt_seconds=3600.0, poll=0.03):
    """Hold the victim's /blocks slots: never download the advertised responses, inflate rtt, keep firing."""
    RNS = _rns()
    RNS.Resource.accept = staticmethod(lambda *a, **k: None)  # never download a response Resource
    RNS.Reticulum(configdir=os.path.abspath(configdir))
    dest = resolve(node_id_hex)
    cids = [cid_binary(cid)]  # one CID, a 64 KiB Resource: a request repeating a CID is refused as malformed (L2)
    links = []
    for _ in range(int(n_links)):
        link = open_link(dest)
        inflate_rtt(link, rtt_seconds)
        links.append(link)
    print("ATTACKER_READY", flush=True)

    def hammer(link):
        while True:
            try:
                send_blocks(link, cids, 1 << 40, timeout=2.0, wait=False)
            except Exception:
                pass
            time.sleep(poll)

    for link in links:
        threading.Thread(target=hammer, args=(link,), daemon=True).start()
    while True:
        time.sleep(3600)


def role_honest(configdir, node_id_hex, cid, duration, interval, out_json):
    """Issue honest /blocks requests and record served / refused (victim answered False) / failed."""
    RNS = _rns()
    RNS.Reticulum(configdir=os.path.abspath(configdir))
    dest = resolve(node_id_hex)
    cidb = cid_binary(cid)
    counts = {"served": 0, "refused": 0, "failed": 0}
    link = open_link(dest)
    end = time.time() + float(duration)
    while time.time() < end:
        if link.status != RNS.Link.ACTIVE:
            link = open_link(dest)
        response = send_blocks(link, [cidb], 64 * 1024, timeout=8.0)
        if response is False:
            counts["refused"] += 1
        elif isinstance(response, list):
            counts["served"] += 1
        else:
            counts["failed"] += 1
        time.sleep(float(interval))
    total = sum(counts.values())
    result = {**counts, "total": total, "refusal_rate": counts["refused"] / total if total else 0.0}
    with open(out_json, "w") as f:
        json.dump(result, f)
    print("HONEST_DONE", json.dumps(result), flush=True)


def _main():
    role = sys.argv[1]
    if role == "hub":
        role_hub(sys.argv[2])
    elif role == "attacker":
        role_attacker(sys.argv[2], sys.argv[3], sys.argv[4])
    elif role == "honest":
        role_honest(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7])
    else:
        raise SystemExit(f"unknown role {role}")


if __name__ == "__main__":
    _main()
