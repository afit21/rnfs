"""Real daemons over local TCP, driven through the CLI as a user would (opt in with RNFS_E2E=1).

A hub with transport enabled and three nodes connected to it: A and B (DHT servers) and C (a DHT client).
Each has its own --dir, API port and gateway port, and short intervals in its rnfs.toml. Takes about a minute.
"""
import filecmp
import functools
import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.request

from tests import support
from tests.support import free_port

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN = os.path.join(ROOT, "rnfs.py")
MISSING_CID = "bafkreidhtnhwyqkmbhxvp2mrery66f4kpdsfbxew3qrfqrbtwbs6zb5zce"  # content no node has
WINDOWS = os.name == "nt"
# Stopping a daemon as Ctrl+C would. Windows can't send SIGINT to one process: it gets CTRL_BREAK_EVENT, sent to
# a process group of its own, which the daemon handles (as SIGBREAK) like Ctrl+C
POPEN_OPTIONS = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if WINDOWS else {}
INTERRUPT = signal.CTRL_BREAK_EVENT if WINDOWS else signal.SIGINT

HUB_CONFIG = """
[reticulum]
  enable_transport = Yes
  share_instance = No
[logging]
  loglevel = 4
[interfaces]
  [[TCP Server]]
    type = TCPServerInterface
    enabled = yes
    listen_ip = 127.0.0.1
    listen_port = {port}
"""

NODE_CONFIG = """
[reticulum]
  enable_transport = No
  share_instance = No
[logging]
  loglevel = 4
[interfaces]
  [[TCP Client]]
    type = TCPClientInterface
    enabled = yes
    target_host = 127.0.0.1
    target_port = {port}
"""

# Short intervals so the test runs in seconds; republish_interval must stay below PROVIDER_TTL / 2
SETTINGS = """
default_bootstrap = false
announce_interval = 3600
startup_announce_delay = 2
scan_interval = 2
republish_interval = 600
path_timeout = 10
lookup_timeout = 30
"""


# Every poll here runs the CLI or calls a daemon's API, so poll every half second
wait_for = functools.partial(support.wait_for, interval=0.5)


def same_tree(a: str, b: str) -> bool:
    """True if two directory trees have the same names, file contents and execute bits."""
    comparison = filecmp.dircmp(a, b)
    if comparison.left_only or comparison.right_only or comparison.funny_files:
        return False
    for name in comparison.common_files:
        left, right = os.path.join(a, name), os.path.join(b, name)
        if not filecmp.cmp(left, right, shallow=False):
            return False
        if bool(os.stat(left).st_mode & 0o100) != bool(os.stat(right).st_mode & 0o100):
            return False  # always equal on Windows, which has no execute bit
    return all(same_tree(os.path.join(a, name), os.path.join(b, name)) for name in comparison.common_dirs)


class Daemon:
    """One running daemon: its directory, ports, node ID and log."""

    def __init__(self, name: str, base: str, config: str, tcp_port: int, *extra: str):
        """Start the daemon and wait for its API."""
        self.name = name
        self.dir = os.path.join(base, name)
        os.makedirs(self.dir, exist_ok=True)
        with open(os.path.join(self.dir, "config"), "w", encoding="utf-8") as f:
            f.write(config.format(port=tcp_port))
        with open(os.path.join(self.dir, "rnfs.toml"), "w", encoding="utf-8") as f:
            f.write(SETTINGS)
        self.api_port = free_port()
        self.gateway_port = free_port()
        self.log_path = os.path.join(base, f"{name}.log")
        with open(self.log_path, "wb") as log:
            self.proc = subprocess.Popen(
                [sys.executable, MAIN, "--dir", self.dir, "--api-port", str(self.api_port), "daemon",
                 "--rnsconfig", self.dir, "--gateway-port", str(self.gateway_port), *extra],
                stdout=log, stderr=subprocess.STDOUT, env={**os.environ, "PYTHONUNBUFFERED": "1"}, **POPEN_OPTIONS)
        self.node_id = wait_for(self.status, 60, f"{name} daemon to start")["node_id"]

    def status(self) -> dict | None:
        """The daemon's status (with its API token: without it, status says only the version), or None while
        its API is not up yet."""
        try:
            with open(os.path.join(self.dir, "api_token"), encoding="ascii") as f:
                token = f.read().strip()
            request = urllib.request.Request(f"http://127.0.0.1:{self.api_port}/api/status",
                                             headers={"X-Rnfs-Api": token})
            with urllib.request.urlopen(request, timeout=2) as response:
                return json.load(response)
        except OSError:
            if self.proc.poll() is not None:
                raise AssertionError(f"{self.name} daemon exited") from None
            return None

    def cli(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        """Run a CLI command against this daemon; with check, fail unless it succeeds."""
        result = subprocess.run([sys.executable, MAIN, "--dir", self.dir, "--api-port", str(self.api_port), *args],
                                capture_output=True, timeout=180)
        if check and result.returncode != 0:
            raise AssertionError(f"{self.name}: {' '.join(args)} failed: {result.stderr.decode('utf-8', 'replace')}")
        return result

    def out(self, *args: str) -> str:
        """A successful command's stdout, stripped."""
        return self.cli(*args).stdout.decode("utf-8", "replace").strip()

    def gateway(self, path: str, headers: dict | None = None):
        """(status, headers, body) of a gateway GET, without following redirects."""
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                return None

        opener = urllib.request.build_opener(NoRedirect)
        request = urllib.request.Request(f"http://127.0.0.1:{self.gateway_port}{path}", headers=headers or {})
        try:
            with opener.open(request, timeout=60) as response:
                return response.status, response.headers, response.read()
        except urllib.error.HTTPError as e:
            return e.code, e.headers, e.read()

    def interrupt(self):
        """Ask the daemon to shut down, as Ctrl+C would, if it is still running."""
        if self.proc.poll() is not None:
            return
        try:
            self.proc.send_signal(INTERRUPT)
        except OSError:
            self.proc.terminate()  # Windows without a console to deliver the event through: stop it outright

    def stop(self):
        """Interrupt like Ctrl-C; kill if it doesn't exit in time."""
        self.interrupt()
        try:
            self.proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()


@unittest.skipUnless(os.environ.get("RNFS_E2E") == "1", "set RNFS_E2E=1 to run")
class NetworkTest(unittest.TestCase):
    """A hub, two DHT servers and a DHT client sharing content through the DHT, the CLI and the gateway."""

    def setUp(self):
        """Start the daemons."""
        # A daemon killed on Windows may not have released its files yet: don't fail the test over cleanup
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmp.cleanup)
        self.daemons: list[Daemon] = []
        self.addCleanup(self.stop_all)
        tcp_port = free_port()
        base = self.tmp.name
        try:
            self.hub = self.start("hub", HUB_CONFIG, tcp_port)
            bootstrap = ("--bootstrap", self.hub.node_id)
            self.a = self.start("a", NODE_CONFIG, tcp_port, *bootstrap)
            self.b = self.start("b", NODE_CONFIG, tcp_port, *bootstrap)
            self.c = self.start("c", NODE_CONFIG, tcp_port, *bootstrap, "--dht-mode", "client")
        except BaseException:
            self.print_logs()
            raise
        self.base = base

    def start(self, name: str, config: str, tcp_port: int, *extra: str) -> Daemon:
        """Start one daemon, remembered for cleanup."""
        daemon = Daemon(name, self.tmp.name, config, tcp_port, *extra)
        self.daemons.append(daemon)
        return daemon

    def stop_all(self):
        """Interrupt every daemon at once, then wait for each (killing any that hangs)."""
        for daemon in self.daemons:
            daemon.interrupt()
        for daemon in self.daemons:
            daemon.stop()

    def print_logs(self):
        """Print every daemon's log, to diagnose a failure."""
        for daemon in self.daemons:
            with open(daemon.log_path, encoding="utf-8", errors="replace") as f:
                print(f"\n===== {daemon.name} log =====\n{f.read()}")

    def path(self, *parts: str) -> str:
        """A path in the test's temporary directory."""
        return os.path.join(self.base, *parts)

    def make_tree(self) -> str:
        """A nested directory with a multi-chunk file and an executable."""
        root = self.path("tree")
        os.makedirs(os.path.join(root, "sub", "deeper"))
        with open(os.path.join(root, "big.bin"), "wb") as f:
            f.write(os.urandom(300 * 1024))
        # Written in binary, so Windows' text mode doesn't turn "\n" into "\r\n" in what the gateway serves
        with open(os.path.join(root, "index.txt"), "wb") as f:
            f.write(b"hello from rnfs\n")
        script = os.path.join(root, "sub", "run.sh")
        with open(script, "wb") as f:
            f.write(b"#!/bin/sh\necho hi\n")
        os.chmod(script, 0o755)
        with open(os.path.join(root, "sub", "deeper", "note.txt"), "wb") as f:
            f.write(b"deep inside")
        return root

    def test_network(self):
        try:
            self.run_scenario()
        except BaseException:
            self.print_logs()
            raise

    def run_scenario(self):
        """Every step, in order, on the running network."""
        a, b, c, hub = self.a, self.b, self.c, self.hub

        # Add a multi-chunk file and a nested directory on A
        big = self.path("big.bin")
        content = os.urandom(300 * 1024)
        with open(big, "wb") as f:
            f.write(content)
        file_cid = a.out("add", big)
        tree = self.make_tree()
        dir_cid = a.out("add", tree)

        # B finds A as a provider once A has published
        def providers_of(daemon, cid):
            """The provider IDs a daemon finds for cid."""
            result = daemon.cli("providers", cid, check=False)
            return result.stdout.decode("utf-8", "replace").split() if result.returncode == 0 else []

        wait_for(lambda: a.node_id in providers_of(b, file_cid), 60, "B to find A providing the file")
        wait_for(lambda: a.node_id in providers_of(b, dir_cid), 60, "B to find A providing the directory")

        # get by CID and by <cid>/path, with identical bytes and execute bits
        b.cli("get", file_cid, "-o", self.path("b-file"))
        with open(self.path("b-file"), "rb") as f:
            self.assertEqual(f.read(), content)
        b.cli("get", dir_cid, "-o", self.path("b-tree"))
        self.assertTrue(same_tree(tree, self.path("b-tree")))
        b.cli("get", f"{dir_cid}/sub/run.sh", "-o", self.path("b-run.sh"))
        if not WINDOWS:  # no execute bit there: add records kind 0 and get sets none
            self.assertTrue(os.stat(self.path("b-run.sh")).st_mode & 0o100)
        b.cli("get", f"{dir_cid}/sub/deeper/note.txt", "-o", self.path("b-note"))
        with open(self.path("b-note"), "rb") as f:
            self.assertEqual(f.read(), b"deep inside")

        # cat with --offset and --length, ls, stat
        with open(os.path.join(tree, "big.bin"), "rb") as f:
            tree_big = f.read()
        part = b.cli("cat", f"{dir_cid}/big.bin", "--offset", "70000", "--length", "100000").stdout
        self.assertEqual(part, tree_big[70000:170000])
        listing = b.out("ls", dir_cid).splitlines()
        self.assertEqual([line.split()[-1] for line in listing], ["big.bin", "index.txt", "sub/"])
        self.assertIn("local_blocks", b.out("stat", dir_cid))

        # The gateway on B: headers, ranges, directories
        status, headers, body = b.gateway(f"/rnfs/{dir_cid}/index.txt")
        self.assertEqual((status, body), (200, b"hello from rnfs\n"))
        self.assertTrue(headers["Content-Type"].startswith("text/plain"))
        self.assertIn("sandbox", headers["Content-Security-Policy"])
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
        index_cid = [line.split()[2] for line in listing if line.endswith("index.txt")][0]
        self.assertEqual(headers["ETag"], f'"{index_cid}"')
        status, headers, body = b.gateway(f"/rnfs/{dir_cid}/big.bin", {"Range": "bytes=65530-65545"})
        self.assertEqual((status, body), (206, tree_big[65530:65546]))
        self.assertEqual(headers["Content-Range"], f"bytes 65530-65545/{len(tree_big)}")
        status, headers, _ = b.gateway(f"/rnfs/{dir_cid}/sub")
        self.assertEqual(status, 301)
        self.assertTrue(headers["Location"].endswith("/sub/"))
        status, headers, body = b.gateway(f"/rnfs/{dir_cid}/sub/")
        self.assertEqual(status, 200)
        self.assertIn(b"run.sh", body)
        self.assertIn(b"deeper", body)

        # A DHT client can fetch too
        c.cli("get", file_cid, "-o", self.path("c-file"))
        with open(self.path("c-file"), "rb") as f:
            self.assertEqual(f.read(), content)
        self.assertEqual(json.loads(json.dumps(c.status()))["dht_mode"], "client")

        # pin rm and gc on B: pinning keeps blocks, gc then removes everything unpinned
        self.assertEqual(b.out("pin", "add", dir_cid), dir_cid)
        self.assertIn(dir_cid, b.out("pin", "ls"))
        b.cli("pin", "rm", dir_cid)
        self.assertNotIn(dir_cid, b.out("pin", "ls"))
        self.assertIn("removed", b.out("gc"))
        self.assertEqual(b.status()["blocks"], 0)

        # dir set on A
        new_root = a.out("dir", "set", dir_cid, "copy.bin", file_cid)
        self.assertIn("copy.bin", a.out("ls", new_root))
        self.assertEqual(a.out("dir", "rm", f"{new_root}/copy.bin"), dir_cid)

        # export on A, import on the hub
        archive = self.path("tree.rnfs")
        self.assertIn("exported", a.out("export", dir_cid, "-o", archive))
        self.assertEqual(hub.out("import", archive), dir_cid)
        self.assertIn(dir_cid, hub.out("pin", "ls"))

        # A name created and published on A resolves on B, and through B's gateway
        name = a.out("name", "create", "site")
        self.assertIn(dir_cid, a.out("name", "publish", "site", dir_cid))
        resolved = wait_for(lambda: b.cli("name", "resolve", name, check=False).stdout.decode("utf-8", "replace").strip(),
                            60, "B to resolve A's name")
        self.assertEqual(resolved, dir_cid)
        status, _, body = b.gateway(f"/name/{name}/index.txt")
        self.assertEqual((status, body), (200, b"hello from rnfs\n"))

        # A damaged block on A: A re-checks before serving, so it answers None rather than bad data; B then
        # fails over to the hub, which imported the archive and provides it too
        with open(os.path.join(a.dir, "blocks", index_cid), "wb") as f:
            f.write(b"damaged on disk")
        b.cli("gc")  # B cached index.txt through its gateway above
        failed = b.cli("get", f"{dir_cid}/index.txt", "-o", self.path("b-index-a"), "--from", a.node_id, check=False)
        self.assertNotEqual(failed.returncode, 0)
        wait_for(lambda: hub.node_id in providers_of(b, dir_cid), 60, "B to find the hub providing the directory")
        b.cli("get", f"{dir_cid}/index.txt", "-o", self.path("b-index"))
        with open(self.path("b-index"), "rb") as f:
            self.assertEqual(f.read(), b"hello from rnfs\n")
        self.assertEqual(b.cli("verify").returncode, 0)

        # A CID nobody has
        missing = b.cli("get", MISSING_CID, "-o", self.path("missing"), check=False)
        self.assertNotEqual(missing.returncode, 0)
        self.assertIn(b"no provider", missing.stderr)

        # Status shows the network
        self.assertGreaterEqual(b.status()["known_nodes"], 2)
        self.assertGreater(hub.status()["records"], 0)

        # --- mesh: peers, traffic counters and stats ---
        peers = {peer["node_id"]: peer for peer in json.loads(b.out("peers", "--json"))}
        self.assertIn(a.node_id, peers)
        self.assertGreater(peers[a.node_id]["received"], 0)
        self.assertIsNotNone(peers[a.node_id]["hops"])
        # b fetched from a over a link it opened: RNS measured its round trip, and a answered within the window
        self.assertGreaterEqual(peers[a.node_id]["rtt"], 0)
        self.assertTrue(peers[a.node_id]["online"])
        self.assertLessEqual(peers[a.node_id]["first_heard"], peers[a.node_id]["rtt_at"])
        self.assertIn(a.node_id, b.out("peers"))
        self.assertGreater(b.status()["bytes_received"], 0)
        self.assertGreater(a.status()["bytes_sent"], 0)
        self.assertIn("received:", b.out("stats"))
        # --- end mesh ---

        # --- transfers: a background get, followed with rnfs jobs ---
        job_id = c.out("get", file_cid, "-o", self.path("c-background"), "--background")
        self.assertIn(job_id, c.out("jobs"))
        self.assertEqual(c.out("jobs", "wait", job_id), self.path("c-background"))
        with open(self.path("c-background"), "rb") as f:
            self.assertEqual(f.read(), content)
        self.assertIn("state: done", c.out("jobs", "show", job_id))
        self.assertIn("removed", c.out("jobs", "clear"))
        # --- end transfers ---

        # --- content: sealed files for B, FastCDC, --no-pin, trustless gateway formats ---
        secret = os.urandom(150 * 1024)
        with open(self.path("secret.bin"), "wb") as f:
            f.write(secret)
        added = wait_for(lambda: (lambda r: r.stdout.decode().strip() if r.returncode == 0 else None)(
            a.cli("add", self.path("secret.bin"), "--to", b.node_id, "--to-self", check=False)), 60,
            "A to know B's identity and seal a file for it")
        wait_for(lambda: a.node_id in providers_of(b, added), 60, "B to find A providing the sealed file")
        self.assertEqual(b.cli("cat", added).stdout, secret)
        self.assertEqual(b.cli("cat", added, "--offset", "70000", "--length", "1000").stdout, secret[70000:71000])
        b.cli("get", added, "-o", self.path("b-secret"))
        with open(self.path("b-secret"), "rb") as f:
            self.assertEqual(f.read(), secret)
        self.assertTrue(b.cli("cat", added, "--raw").stdout.startswith(b"rnfs-sealed\n"))
        refused = c.cli("cat", added, check=False)
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn(b"sealed for", refused.stderr)
        status, headers, body = b.gateway(f"/rnfs/{added}")
        self.assertEqual((status, headers["Content-Type"]), (200, "application/octet-stream"))
        self.assertTrue(body.startswith(b"rnfs-sealed\n"))
        cdc_cid = a.out("add", big, "--chunker", "fastcdc", "--no-pin")
        self.assertNotIn(cdc_cid, a.out("pin", "ls"))
        self.assertEqual(a.cli("cat", cdc_cid).stdout, content)
        status, headers, body = b.gateway(f"/rnfs/{file_cid}?format=raw")
        self.assertEqual((status, headers["Content-Type"]), (200, "application/vnd.rnfs.raw"))
        self.assertEqual(body, b.cli("cat", file_cid, "--block").stdout)
        status, headers, body = b.gateway(f"/rnfs/{dir_cid}?format=archive")
        self.assertEqual((status, headers["Content-Type"]), (200, "application/vnd.rnfs.archive"))
        self.assertEqual(len(body), int(headers["Content-Length"]))
        # --- end content ---

        # --- lxmf: A sends B a link over LXMF; B fetches it from its inbox ---
        b_address = b.out("lxmf", "address").splitlines()[0]
        self.assertEqual(b.status()["lxmf_address"], b_address)
        wait_for(lambda: a.cli("send", b_address, file_cid, "-m", "hello B", check=False).returncode == 0, 60,
                 "A to send B an LXMF message")
        inbox = wait_for(lambda: (lambda out: out if file_cid in out else None)(b.out("inbox")), 90,
                         "the message in B's inbox")
        self.assertIn("hello B", inbox)
        self.assertIn(f"from={a.node_id}", inbox)  # the sender's node, as a provider hint
        wait_for(lambda: "delivered" in a.out("outbox"), 60, "A's outbox to show the message delivered")
        number = inbox.split()[0].lstrip("#")
        fetched = b.out("inbox", "get", number, "-o", self.path("b-lxmf")).split()
        if fetched[0] == "job":  # with rnfs/features/jobs.py the fetch runs as a background job
            b.out("jobs", "wait", fetched[1].rstrip(":"))
        with open(self.path("b-lxmf"), "rb") as f:
            self.assertEqual(f.read(), content)
        self.assertIn("removed 1", b.out("inbox", "rm", number))
        # --- end lxmf ---

        # --- names: keepers, contacts, follow. The hub keeps A's name; once A is gone, C gets the signed
        # record from the hub (only a keeper's /name answer carries the signature C keeps) ---
        self.assertIn(name, a.out("name", "kept"))  # a node keeps its own published names
        hub.cli("name", "keep", name)
        wait_for(lambda: dir_cid in hub.out("name", "kept"), 90, "the hub to fetch the signed record from A")
        a.stop()

        def c_keeps():
            """C asks keepers again (keep is idempotent) until it holds the signed record."""
            c.cli("name", "keep", name, check=False)
            return dir_cid in c.out("name", "kept")

        wait_for(c_keeps, 120, "C to get A's record from the hub, a keeper, after A stopped")
        self.assertEqual(c.out("name", "resolve", name), dir_cid)
        # Contacts: a name contact's nick starts a target; a node contact's nick works as a node ID
        c.cli("contacts", "add", "alice", name, "--kind", "name")
        self.assertIn(f"alice name {name}", c.out("contacts", "ls"))
        self.assertEqual(c.cli("cat", "alice/index.txt").stdout, b"hello from rnfs\n")
        self.assertIn("index.txt", c.out("ls", "alice"))
        self.assertEqual(c.out("contacts", "add", "thehub", hub.node_id).split()[1], "node")
        self.assertIn("round-trip time", c.out("ping", "thehub"))
        # Following pins the name's target with source follow:<name>
        c.cli("name", "follow", "alice")
        wait_for(lambda: f"{name} {dir_cid}" in c.out("name", "following"), 120, "C to pin the followed name")
        self.assertIn(f"follow:{name}", c.out("pin", "ls"))
        c.cli("name", "unfollow", "alice")
        self.assertNotIn(f"follow:{name}", c.out("pin", "ls"))
        # --- end names ---


if __name__ == "__main__":
    unittest.main()
