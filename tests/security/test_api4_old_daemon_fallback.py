"""API-4: a daemon before 0.1.5 closes the connection its proof was made on, so the client sent the request (and the
token) on a new connection. It did that for any daemon that closed it, which kept the old race for every daemon: the
daemon could stop between the proof and the new connection, and a program taking the port then got the token.

From 0.1.5 to 0.1.6 the client reconnected only for a daemon whose proved version was below 0.1.5; any other daemon
that closed the proof's connection got ConnectionFailed. Since 0.2.0 that fallback is gone: a daemon proved older
than 0.1.5 is refused before the token is sent (check_daemon), and every daemon that closes the proof's connection
gets nothing more.
"""
import json
import os
import tempfile
import unittest

from rnfs import apiclient, cli
from rnfs.apiclient import ApiConnection, ConnectionFailed
from tests.security.test_api2_proof_first import TOKEN, FakeListener, response


def closing_daemon(version):
    """A daemon that knows TOKEN, says version (None: says none) and closes every connection after its answer."""
    def respond(listener, request):
        """The proof and version for a proof request, a status for the request after it."""
        if "x-rnfs-challenge" in request:
            body = json.dumps({} if version is None else {"version": version}).encode()
            return response("200 OK", body, {apiclient.PROOF_HEADER: listener.proof(request), "Connection": "close",
                                             "Content-Type": "application/json"}), False
        return response("200 OK", b'{"node_id": "ab"}', {"Connection": "close"}), False
    return respond


class OldDaemonFallbackTest(unittest.TestCase):
    """A client whose token file holds TOKEN, against daemons that close the proof's connection."""

    def setUp(self):
        """The token file."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        cli.write_token(os.path.join(self.tmp, "api_token"), TOKEN)

    def test_newer_daemon_closing_the_connection_gets_nothing_more(self):
        # API-4: fails until only a daemon proved older than 0.1.5 gets the request on a new connection
        for version in ("0.1.5", "0.1.6", "0.2.0", "1.0", "0.1.5.dev1", "garbage", "", None, 15):
            listener = FakeListener(self, closing_daemon(version))
            api = ApiConnection(self.tmp, port=listener.port)
            with self.assertRaises(ConnectionFailed, msg=version) as raised:
                api.status()
            self.assertIn("closed the connection", str(raised.exception), version)
            self.assertEqual(listener.tokens(), [None], version)  # only the proof request: no token sent

    def test_older_daemon_is_refused_before_the_token(self):
        # 0.2.0: a pre-0.1.5 daemon is refused, saying why, and gets neither the token nor a second connection
        for version in ("0.1.4", "0.1.0", "0.1.4.post1", "0.0.9"):
            listener = FakeListener(self, closing_daemon(version))
            api = ApiConnection(self.tmp, port=listener.port)
            with self.assertRaises(apiclient.ApiError, msg=version) as raised:
                api.status()
            self.assertIn("too old for this client", str(raised.exception), version)
            self.assertEqual(api.daemon_version, version)
            self.assertEqual(listener.tokens(), [None], version)


if __name__ == "__main__":
    unittest.main()
