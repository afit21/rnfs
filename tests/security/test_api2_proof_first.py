"""API-2: before the proof, a program listening on the API port chose the client's error. verify_daemon mapped the
status of its proof request (error_for) before comparing X-Rnfs-Proof, so while the daemon was down a listener on
its port could answer 451, 404 or 507 with any text, and rnfs.Client raised Denied, NotFound or StorageFull carrying
that text instead of AuthFailed. The proof exchange also had no timeout, and its body was read whole: a listener
that never answered, or sent an endless body, hung the client.

Now the proof is checked first: without a valid one, a 503 is Busy with the client's own message and every other
status AuthFailed, and the listener's text is never shown. The proof's response is read up to 64 KiB, and connecting
and proving take at most PROOF_TIMEOUT seconds, after which the request gets the client's own timeout. A proved
daemon's error statuses keep their meaning (error_for is still the one mapping).
"""
import json
import os
import socketserver
import tempfile
import threading
import time
import unittest
from unittest import mock

from rnfs import apiclient, cli
from rnfs.api import ApiServer
from rnfs.apiclient import ApiConnection, AuthFailed, Busy, ConnectionFailed, Denied, NotFound
from tests.test_api import FakeNode

TOKEN = "the daemon's token " + "7" * 32
LISTENER_TEXT = "your files were deleted: run rnfs-repair"  # what the listener would have the user read


def response(status: str, body: bytes = b"", headers: dict | None = None, length: int | None = None) -> bytes:
    """A raw HTTP/1.1 response: status ("451 Unavailable"), headers, and body, with a Content-Length of length (default:
    the body's)."""
    lines = [f"HTTP/1.1 {status}", f"Content-Length: {len(body) if length is None else length}",
             *(f"{name}: {value}" for name, value in (headers or {}).items())]
    return ("\r\n".join(lines) + "\r\n\r\n").encode() + body


def error_body(message: str = LISTENER_TEXT) -> bytes:
    """An error body as the API sends one."""
    return json.dumps({"error": message}).encode()


class FakeListener:
    """A program listening on a local port, answering each request it reads with respond(listener, headers), which
    returns (raw response, keep the connection open), or None to never answer. requests holds (request line,
    headers) of everything received, in order."""

    def __init__(self, test: unittest.TestCase, respond):
        """Start listening on a free port; stopped when test ends."""
        self.respond = respond
        self.requests = []
        self.release = threading.Event()  # set at the end of the test, so connections never answered are closed
        listener = self

        class Handler(socketserver.StreamRequestHandler):
            def handle(self):
                """Read requests (line and headers; the client sends no body to a proof) and answer them."""
                try:
                    self.serve()
                except OSError:
                    pass  # the client closed the connection

            def serve(self):
                """Answer requests until the connection ends or one is answered for the last time."""
                while True:
                    line = self.rfile.readline().decode("latin-1").strip()
                    if not line:
                        return
                    headers = {}
                    for header in iter(self.rfile.readline, b"\r\n"):
                        if not header:
                            return
                        name, _, value = header.decode("latin-1").partition(":")
                        headers[name.strip().lower()] = value.strip()
                    listener.requests.append((line, headers))
                    answer = listener.respond(listener, headers)
                    if answer is None:
                        listener.release.wait(30)
                        return
                    data, keep_open = answer
                    self.wfile.write(data)
                    self.wfile.flush()
                    if not keep_open:
                        return

        self.server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, args=(0.05,), daemon=True).start()
        test.addCleanup(self.server.server_close)
        test.addCleanup(self.server.shutdown)
        test.addCleanup(self.release.set)

    def proof(self, headers: dict, token: str = TOKEN) -> str:
        """The proof for the challenge in headers, as a daemon holding token gives it."""
        return apiclient.api_proof(token, self.port, headers.get("x-rnfs-challenge", ""))

    def tokens(self) -> list:
        """The X-Rnfs-Api header of every request received (None where there was none)."""
        return [headers.get("x-rnfs-api") for _, headers in self.requests]


class ProofFirstTest(unittest.TestCase):
    """A client whose token file holds TOKEN, against listeners on the API port."""

    def setUp(self):
        """The token file."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        cli.write_token(os.path.join(self.tmp, "api_token"), TOKEN)

    def client(self, listener: FakeListener, timeout: float | None = None) -> ApiConnection:
        """A client of the API on listener's port."""
        return ApiConnection(self.tmp, port=listener.port, timeout=timeout)

    def finishes(self, call, within: float = 5.0) -> BaseException | None:
        """What call() raised (None if it returned), failing the test if it is still running after within seconds."""
        outcome = []

        def run():
            """Call, keeping what it raised."""
            try:
                call()
                outcome.append(None)
            except BaseException as e:  # whatever it raised is the result
                outcome.append(e)
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(within)
        self.assertFalse(thread.is_alive(), f"the client still waits after {within} s")
        return outcome[0]

    def test_unproved_statuses_are_auth_failed(self):
        # API-2: fails until a listener that can't prove it knows the token gets AuthFailed whatever it answers
        for status in ("400 Bad Request", "403 Forbidden", "404 Not Found", "413 Too Large", "422 Invalid",
                       "451 Unavailable", "500 Internal Server Error", "502 Bad Gateway", "507 Insufficient Storage",
                       "200 OK"):
            for proof in (None, "00" * 32, "not hex"):
                headers = {"Content-Type": "application/json", **({apiclient.PROOF_HEADER: proof} if proof else {})}
                listener = FakeListener(self, lambda listener, request, status=status, headers=headers: (
                    response(status, error_body(), headers), False))
                api = self.client(listener)
                with self.assertRaises(AuthFailed) as raised:
                    api.call("POST", "/api/name/import", params={"alias": "stolen"}, body=b"K" * 64)
                context = (status, proof)
                self.assertIs(type(raised.exception), AuthFailed, context)
                self.assertNotIn(LISTENER_TEXT, str(raised.exception), context)
                self.assertIn("couldn't prove it knows the API token", str(raised.exception), context)
                self.assertEqual(listener.tokens(), [None], context)  # only the proof request, without the token
                self.assertTrue(listener.requests[0][0].startswith("GET /api/status "), context)

    def test_unproved_503_is_busy_with_the_clients_message(self):
        # A full daemon answers 503 before reading the request, so without a proof: Busy, in the client's words
        listener = FakeListener(self, lambda listener, request: (
            response("503 Service Unavailable", error_body(), {"Retry-After": "10"}), False))
        with self.assertRaises(Busy) as raised:
            self.client(listener).status()
        self.assertNotIn(LISTENER_TEXT, str(raised.exception))
        self.assertEqual(raised.exception.status, 503)
        self.assertIn(f"API port {listener.port}", str(raised.exception))
        self.assertEqual(listener.tokens(), [None])

    def test_proved_statuses_keep_their_meaning(self):
        # The daemon itself (it knows the token) answering the proof request with an error: error_for, its message
        for status, expected in (("451 Unavailable", Denied), ("404 Not Found", NotFound),
                                 ("503 Service Unavailable", Busy)):
            listener = FakeListener(self, lambda listener, request, status=status: (
                response(status, error_body("the daemon's own words"),
                         {apiclient.PROOF_HEADER: listener.proof(request)}), False))
            with self.assertRaises(expected) as raised:
                self.client(listener).status()
            self.assertEqual(str(raised.exception), "the daemon's own words")
            self.assertEqual(raised.exception.status, int(status.split()[0]))
            self.assertEqual(listener.tokens(), [None])

    def test_a_listener_that_never_answers(self):
        # API-2: fails until the proof exchange has a timeout of its own, even for a client without one
        listener = FakeListener(self, lambda listener, request: None)
        api = self.client(listener)
        with mock.patch.object(apiclient, "PROOF_TIMEOUT", 0.5, create=True):
            raised = self.finishes(api.status)
        self.assertIsInstance(raised, ConnectionFailed)
        self.assertEqual(listener.tokens(), [None])

    def test_a_listener_that_never_ends_its_body(self):
        # API-2: fails until the proof's response is read only up to 64 KiB, and its proof checked first
        def endless(listener, request, proved):
            """200 announcing 100 MiB, then 70 KiB of them, then nothing more."""
            headers = {apiclient.PROOF_HEADER: listener.proof(request)} if proved else {}
            return response("200 OK", b"{" + b" " * (70 * 1024), headers, length=100 << 20), True
        for proved in (False, True):
            listener = FakeListener(self, lambda listener, request, proved=proved: endless(listener, request, proved))
            api = self.client(listener)
            with mock.patch.object(apiclient, "PROOF_TIMEOUT", 30, create=True):
                raised = self.finishes(api.status)
            self.assertIsInstance(raised, AuthFailed if not proved else ConnectionFailed, proved)
            self.assertEqual(listener.tokens(), [None], proved)

    def test_the_request_gets_the_clients_timeout(self):
        """Once the daemon has proved itself, the request may take longer than PROOF_TIMEOUT (fetches can take
        hours): the client's own timeout (none here) applies again."""
        node = FakeNode()
        server = ApiServer(node, 0)
        server.start()
        self.addCleanup(server.stop)
        cli.write_token(os.path.join(self.tmp, "api_token"), server.token)
        cid = node.put_file(b"abc")
        original = node.stat

        def slow(target):
            """The stat, after a while."""
            time.sleep(1.0)
            return original(target)
        with mock.patch.object(apiclient, "PROOF_TIMEOUT", 0.3, create=True), \
                mock.patch.object(node, "stat", slow):
            self.assertEqual(ApiConnection(self.tmp, port=server.port).stat(cid)["cid"], cid)


class ProofTimeoutTest(unittest.TestCase):
    """The constants the plan fixes."""

    def test_values(self):
        """15 s to connect and prove; at most 64 KiB of the proof's response read."""
        self.assertEqual((apiclient.PROOF_TIMEOUT, apiclient.PROOF_READ_LIMIT), (15, 64 * 1024))


if __name__ == "__main__":
    unittest.main()
