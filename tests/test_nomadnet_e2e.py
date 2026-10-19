"""`rnfs add <destination>:/page/…` and `:/file/…` against a NomadNet node over real RNS on local TCP (opt in with
RNFS_E2E=1): a daemon connected to a minimal NomadNet node (tests/fakes/nomadnet_node.py) answering as NomadNet
does, a compressed file of three segments included, through the CLI as a user would. Takes about a minute."""
import json
import os
import subprocess
import sys
import tempfile
import unittest

from tests.support import free_port
from tests.test_e2e import NODE_CONFIG, Daemon

FAKE_NODE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fakes", "nomadnet_node.py")
PAGE = b">Hello from NomadNet\n\nA page served over RNS.\n"
SECRET = b">Members only\n"


@unittest.skipUnless(os.environ.get("RNFS_E2E") == "1", "set RNFS_E2E=1 to run")
class NomadnetTest(unittest.TestCase):
    """A daemon adding pages and files from a NomadNet node."""

    def setUp(self):
        """The NomadNet node, then a daemon connected to it."""
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(self.tmp.cleanup)
        self.pages = os.path.join(self.tmp.name, "pages")
        os.makedirs(self.pages)
        self.files = {"index.mu": PAGE, "secret.mu": SECRET,
                      # 2.5 MiB that compress well: a compressed Resource of three segments, with metadata
                      "big.log": b"".join(b"line %07d of a log file\n" % n for n in range(105_000))}
        for name, data in self.files.items():
            with open(os.path.join(self.pages, name), "wb") as f:
                f.write(data)
        port = free_port()
        self.node_log = open(os.path.join(self.tmp.name, "nomadnet.log"), "wb")
        self.addCleanup(self.node_log.close)
        self.node = subprocess.Popen([sys.executable, FAKE_NODE, os.path.join(self.tmp.name, "nn"), str(port),
                                      self.pages], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=self.node_log)
        self.addCleanup(self.stop_node)
        self.destination = self.node.stdout.readline().decode().strip()
        self.assertEqual(len(self.destination), 32, "the NomadNet node didn't start")
        self.daemon = Daemon("a", self.tmp.name, NODE_CONFIG, port)
        self.addCleanup(self.daemon.stop)

    def stop_node(self):
        """Stop the NomadNet node."""
        try:
            self.node.stdin.close()
            self.node.wait(timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            self.node.kill()
            self.node.wait()
        self.node.stdout.close()

    def link(self, path: str) -> str:
        """The NomadNet link to path on the node."""
        return f"{self.destination}:{path}"

    def local_cid(self, data: bytes, *options: str) -> str:
        """The CID `rnfs add` gives data as a local file."""
        path = os.path.join(self.tmp.name, "local")
        with open(path, "wb") as f:
            f.write(data)
        return self.daemon.out("add", path, "--only-hash", *options)

    def fails(self, *args: str) -> str:
        """The error of a command that must fail."""
        result = self.daemon.cli(*args, check=False)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        return result.stderr.decode("utf-8", "replace")

    def test_nomadnet(self):
        try:
            self.run_scenario()
        except BaseException:
            for name in ("a.log", "nomadnet.log"):
                with open(os.path.join(self.tmp.name, name), encoding="utf-8", errors="replace") as f:
                    print(f"\n===== {name} =====\n{f.read()}")
            raise

    def run_scenario(self):
        """Every step, in order."""
        daemon = self.daemon

        # A page, pinned with its link as origin
        cid = daemon.out("add", self.link("/page/index.mu"))
        self.assertEqual(cid, self.local_cid(PAGE))
        pins = {pin["cid"]: pin for pin in json.loads(daemon.out("pin", "ls", "--json"))}
        self.assertEqual(pins[cid]["origin"], self.link("/page/index.mu"))

        # A compressed file of three segments, under the name the node sent
        big = self.files["big.log"]
        wrapped = daemon.out("add", self.link("/file/big.log"), "--wrap")
        self.assertIn("big.log", daemon.out("ls", wrapped))
        self.assertEqual(daemon.cli("cat", wrapped + "/big.log").stdout, big)
        self.assertEqual(daemon.out("add", self.link("/file/big.log"), "--only-hash"), self.local_cid(big))

        # A restricted page: refused anonymously and to an identity it doesn't list, then served to the one it does
        with open(os.path.join(self.pages, "secret.mu.allowed"), "w", encoding="ascii") as f:
            f.write("00" * 16 + "\n")
        self.assertIn("Request Not Allowed", self.fails("add", self.link("/page/secret.mu")))
        self.assertIn("Request Not Allowed", self.fails("add", self.link("/page/secret.mu"), "--identify"))
        identity_path = os.path.join(daemon.dir, "storage", "nomadnet_identity")
        self.assertTrue(os.path.isfile(identity_path))
        import RNS
        identity = RNS.Identity.from_file(identity_path)
        self.assertNotEqual(identity.hash.hex(), daemon.status()["identity_hash"])  # never the node's identity
        with open(os.path.join(self.pages, "secret.mu.allowed"), "w", encoding="ascii") as f:
            f.write(identity.hash.hex() + "\n")
        self.assertEqual(daemon.out("add", self.link("/page/secret.mu"), "--identify"), self.local_cid(SECRET))

        # Hostile metadata is refused, and the daemon carries on
        self.assertIn("metadata", self.fails("add", self.link("/file/nested")))
        self.assertEqual(daemon.out("add", self.link("/page/index.mu")), cid)

        # A path the node doesn't serve: it never answers
        self.assertIn("didn't answer", self.fails("add", self.link("/page/missing.mu")))


if __name__ == "__main__":
    unittest.main()
