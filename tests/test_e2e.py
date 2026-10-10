"""Real daemons over local TCP, driven through the CLI as a user would (opt in with RNFS_E2E=1).

A hub with transport enabled and three nodes connected to it: A and B (DHT servers) and C (a DHT client).
Each has its own --dir, API port and gateway port, and short intervals in its rnfs.toml. Takes about a minute.
"""
import filecmp
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN = os.path.join(ROOT, "rnfs.py")
MISSING_CID = "bafkreidhtnhwyqkmbhxvp2mrery66f4kpdsfbxew3qrfqrbtwbs6zb5zce"  # content no node has

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
announce_interval = 3600
startup_announce_delay = 2
scan_interval = 2
republish_interval = 600
path_timeout = 10
lookup_timeout = 30
"""


def free_port() -> int:
    """A local TCP port that is currently free."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(condition, timeout: float, what: str):
    """Poll condition until it returns something truthy and return that; fail after timeout seconds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


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
            return False
    return all(same_tree(os.path.join(a, name), os.path.join(b, name)) for name in comparison.common_dirs)


class Daemon:
    """One running daemon: its directory, ports, node ID and log."""

    def __init__(self, name: str, base: str, config: str, tcp_port: int, *extra: str):
        """Start the daemon and wait for its API."""
        self.name = name
        self.dir = os.path.join(base, name)
        os.makedirs(self.dir, exist_ok=True)
        with open(os.path.join(self.dir, "config"), "w") as f:
            f.write(config.format(port=tcp_port))
        with open(os.path.join(self.dir, "rnfs.toml"), "w") as f:
            f.write(SETTINGS)
        self.api_port = free_port()
        self.gateway_port = free_port()
        self.log_path = os.path.join(base, f"{name}.log")
        with open(self.log_path, "w") as log:
            self.proc = subprocess.Popen(
                [sys.executable, MAIN, "--dir", self.dir, "--api-port", str(self.api_port), "daemon",
                 "--rnsconfig", self.dir, "--gateway-port", str(self.gateway_port), *extra],
                stdout=log, stderr=subprocess.STDOUT, env={**os.environ, "PYTHONUNBUFFERED": "1"})
        self.node_id = wait_for(self.status, 30, f"{name} daemon to start")["node_id"]

    def status(self) -> dict | None:
        """The daemon's status, or None while its API is not up yet."""
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.api_port}/api/status", timeout=2) as response:
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
            raise AssertionError(f"{self.name}: {' '.join(args)} failed: {result.stderr.decode()}")
        return result

    def out(self, *args: str) -> str:
        """A successful command's stdout, stripped."""
        return self.cli(*args).stdout.decode().strip()

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

    def stop(self):
        """Interrupt like Ctrl-C; kill if it doesn't exit in time."""
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
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
        self.tmp = tempfile.TemporaryDirectory()
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
        """Stop every daemon, checking each exits cleanly on SIGINT."""
        for daemon in self.daemons:
            if daemon.proc.poll() is None:
                daemon.proc.send_signal(signal.SIGINT)
        for daemon in self.daemons:
            daemon.stop()

    def print_logs(self):
        """Print every daemon's log, to diagnose a failure."""
        for daemon in self.daemons:
            with open(daemon.log_path, errors="replace") as f:
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
        with open(os.path.join(root, "index.txt"), "w") as f:
            f.write("hello from rnfs\n")
        script = os.path.join(root, "sub", "run.sh")
        with open(script, "w") as f:
            f.write("#!/bin/sh\necho hi\n")
        os.chmod(script, 0o755)
        with open(os.path.join(root, "sub", "deeper", "note.txt"), "w") as f:
            f.write("deep inside")
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
            return result.stdout.decode().split() if result.returncode == 0 else []

        wait_for(lambda: a.node_id in providers_of(b, file_cid), 60, "B to find A providing the file")
        wait_for(lambda: a.node_id in providers_of(b, dir_cid), 60, "B to find A providing the directory")

        # get by CID and by <cid>/path, with identical bytes and execute bits
        b.cli("get", file_cid, "-o", self.path("b-file"))
        with open(self.path("b-file"), "rb") as f:
            self.assertEqual(f.read(), content)
        b.cli("get", dir_cid, "-o", self.path("b-tree"))
        self.assertTrue(same_tree(tree, self.path("b-tree")))
        b.cli("get", f"{dir_cid}/sub/run.sh", "-o", self.path("b-run.sh"))
        self.assertTrue(os.stat(self.path("b-run.sh")).st_mode & 0o100)
        b.cli("get", f"{dir_cid}/sub/deeper/note.txt", "-o", self.path("b-note"))
        with open(self.path("b-note")) as f:
            self.assertEqual(f.read(), "deep inside")

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
        resolved = wait_for(lambda: b.cli("name", "resolve", name, check=False).stdout.decode().strip(), 60,
                            "B to resolve A's name")
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


if __name__ == "__main__":
    unittest.main()
