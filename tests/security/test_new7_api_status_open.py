"""NEW-7: GET /api/status needed no token and returned the node's identifiers and traffic counters.

Status stays open so health checks and the CLI's proof check (verify_daemon, which only reads the
X-Rnfs-Proof header) work before the token is sent. But the open response was the whole status document:
node_id, identity_hash, lxmf_address and the traffic counters, to any local process of any user, while
/api/lxmf/address, which returns the same LXMF address, needs the token. Without the token the fixed API
answers only {"version"}; the full document needs the token, which `rnfs status` now sends.

The API tests run a real ApiServer over a real offline node (its LXMF network replaced by a stand-in holding
only an address, as an offline node has none); the CLI tests reuse tests.test_cli's CliTestCase.
"""
import hmac
import json
import os
import tempfile
import types
import unittest

import RNS

from rnfs import __version__, cli
from rnfs.api import ApiServer
from rnfs.config import Config
from rnfs.node import RnfsNode
from tests.test_api import raw_request
from tests.test_cli import CliTestCase
from tests.test_names import FakeNetwork
from tests.test_node import NetClient

LXMF_ADDRESS = bytes([0x5C] * 16)
PRIVATE_FIELDS = ("node_id", "identity_hash", "lxmf_address", "bytes_sent", "bytes_received", "pins", "records")

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the node log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class ApiStatusTest(unittest.TestCase):
    """GET /api/status on a real ApiServer and offline node."""

    def setUp(self):
        """An offline node with a stand-in LXMF address, behind an ApiServer."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.node = RnfsNode.offline(Config(rnfs_dir=tmp.name, slow="no"), bytes([0xA] * 16), NetClient(),
                                     FakeNetwork())
        self.addCleanup(self.node.stop)
        lxmf = self.node.features.get("lxmf")
        if lxmf is not None:
            lxmf.network = types.SimpleNamespace(address=LXMF_ADDRESS)
        self.server = ApiServer(self.node, 0)
        self.server.start()
        self.addCleanup(self.server.stop)

    def status(self, headers=None):
        """(HTTP status, response headers, JSON body) of GET /api/status with these headers."""
        code, response_headers, body = raw_request(self.server.port, "GET", "/api/status", None, headers or {})
        return code, response_headers, json.loads(body)

    def test_without_the_token_only_the_version(self):
        """A local process without the token learns that a daemon runs and its version, nothing else."""
        code, _, body = self.status()
        self.assertEqual(code, 200)  # health checks still work
        # NEW-7: fails until the open status answers only the version
        leaked = {field: body[field] for field in PRIVATE_FIELDS if field in body}
        self.assertEqual(leaked, {}, "GET /api/status without the token returned the node's private fields")
        self.assertEqual(body, {"version": __version__})

    def test_with_the_token_the_full_document(self):
        """With the token, status is the node's whole summary, as before."""
        code, _, body = self.status({"X-Rnfs-Api": self.server.token})
        self.assertEqual(code, 200)
        self.assertEqual(body["node_id"], self.node.node_id.hex())
        self.assertIn("records", body)
        if self.node.features.get("lxmf") is not None:
            self.assertEqual(body.get("lxmf_address"), LXMF_ADDRESS.hex())

    def test_wrong_token_gets_the_open_answer(self):
        """A wrong token is treated as none: only the version."""
        _, _, body = self.status({"X-Rnfs-Api": self.server.token + "x"})
        self.assertEqual(body, {"version": __version__})

    def test_proof_still_answered_without_the_token(self):
        """The CLI's proof check uses the open status: its proof header doesn't need the token."""
        challenge = "ab" * 16
        _, headers, _ = self.status({cli.CHALLENGE_HEADER: challenge})
        expected = cli.api_proof(self.server.token, self.server.port, challenge)
        self.assertTrue(hmac.compare_digest(headers[cli.PROOF_HEADER], expected))


class CliStatusTest(CliTestCase):
    """`rnfs status` against a FakeNode's ApiServer."""

    def test_status_sends_the_token_and_prints_everything(self):
        """`rnfs status` sends the token (after the daemon's proof), so it still prints the node's fields."""
        out = self.ok("status")
        self.assertIn(f"node_id: {self.node.node_id.hex()}", out)

    def test_status_without_a_token_prints_the_version(self):
        """Without a readable token, `rnfs status` still works as a health check, and says why it is short."""
        os.unlink(self.token_file)
        code, out, err = self.run_cli("status")
        self.assertEqual(code, 0, err)
        self.assertEqual(out.decode(), f"version: {__version__}\n")
        self.assertIn("no readable API token", err)


if __name__ == "__main__":
    unittest.main()
