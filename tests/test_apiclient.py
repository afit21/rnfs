"""Tests for the client library of the local API (rnfs/apiclient.py), against a live ApiServer with a FakeNode."""
import http.server
import io
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

from rnfs import apiclient, cli
from rnfs.api import ApiServer
from rnfs.apiclient import (ApiError, AuthFailed, BadRequest, Busy, Client, ConnectionFailed, DaemonNotRunning, Denied,
                            InvalidDag, NotFound, ServerError, SettingsError, StorageFull, TokenError, TooLarge)
from rnfs.config import DEFAULT_API_PORT
from rnfs import errors
from tests.support import POSIX_MODES, free_port
from tests.test_api import FakeNode

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class FakeJobs:
    """The job manager's interface, recording what it is asked."""

    def __init__(self):
        """No jobs yet."""
        self.calls = []

    def submit(self, kind, target, output, from_node, **flags):
        """A queued job."""
        self.calls.append(("submit", kind, target, output, from_node, flags))
        return {"id": "0123abcd", "kind": kind, "state": "queued"}

    def list(self):
        """Every job."""
        return [{"id": "0123abcd", "state": "queued"}]

    def get(self, job_id, wait):
        """One job, after waiting wait seconds."""
        self.calls.append(("get", job_id, wait))
        if job_id != "0123abcd":
            raise errors.NotFound(f"no job {job_id}")
        return {"id": job_id, "state": "done"}

    def cancel(self, job_id):
        """A cancelled job."""
        return {"id": job_id, "state": "cancelled"}

    def clear(self, job_id):
        """How many finished jobs were forgotten."""
        self.calls.append(("clear", job_id))
        return 2


class NodeWithJobs(FakeNode):
    """FakeNode with a job manager."""

    def __init__(self):
        """A fake node and its jobs."""
        super().__init__()
        self.jobs = FakeJobs()

    def feature(self, name):
        """The jobs feature; KeyError for any other."""
        if name != "jobs":
            raise KeyError(name)
        return self.jobs


class ClientTestCase(unittest.TestCase):
    """Base: a NodeWithJobs behind an ApiServer, its token in <tmp>/api_token, and a Client for it."""

    def setUp(self):
        """Start the server and write its token."""
        self.node = NodeWithJobs()
        self.server = ApiServer(self.node, 0)
        self.server.start()
        self.addCleanup(self.server.stop)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.token_file = os.path.join(self.tmp, "api_token")
        cli.write_token(self.token_file, self.server.token)
        self.api = Client(self.tmp, port=self.server.port)

    def write(self, name: str, data: bytes) -> str:
        """A file in tmp holding data."""
        path = os.path.join(self.tmp, name)
        with open(path, "wb") as f:
            f.write(data)
        return path

    def tokens_seen(self):
        """A list that collects (path, X-Rnfs-Api header) of every request the server handles from now on."""
        seen = []
        original = self.server.server.RequestHandlerClass.dispatch

        def spy(handler):
            """Record the request, then handle it."""
            seen.append((handler.path.split("?")[0], handler.headers["X-Rnfs-Api"]))
            original(handler)
        patcher = mock.patch.object(self.server.server.RequestHandlerClass, "dispatch", spy)
        patcher.start()
        self.addCleanup(patcher.stop)
        return seen


class ImportTest(unittest.TestCase):
    """Apps importing the library don't pay for loading RNS."""

    def test_does_not_import_rns(self):
        """Importing rnfs.apiclient and making a Client loads no RNS module."""
        code = ("import sys; from rnfs import apiclient; apiclient.Client(port=1); "
                "print(any(name == 'RNS' or name.startswith('RNS.') for name in sys.modules))")
        result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, timeout=60,
                                encoding="utf-8", errors="replace")
        self.assertEqual(result.stdout.strip(), "False", result.stderr)


class DirectoryTest(unittest.TestCase):
    """Where the client finds the token and the port, as the CLI does."""

    def setUp(self):
        """An empty rnfs dir."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name

    def settings(self, text: str):
        """Write rnfs.toml."""
        with open(os.path.join(self.tmp, "rnfs.toml"), "w", encoding="utf-8") as f:
            f.write(text)

    def test_default_dir_and_token(self):
        """The default rnfs dir is ~/.rnfs/ (the home directory on every OS) and the token is api_token in it."""
        self.assertEqual(apiclient.DEFAULT_DIR, "~/.rnfs/")
        self.assertEqual(apiclient.token_path(), os.path.join(os.path.expanduser("~/.rnfs/"), "api_token"))
        self.assertEqual(Client(port=1).token_file, apiclient.token_path())
        self.assertEqual(Client(self.tmp, port=1).token_file, os.path.join(self.tmp, "api_token"))
        self.assertEqual(Client(port=1, token_file="/x/t").token_file, "/x/t")

    def test_port_from_settings(self):
        """The port is api_port in rnfs.toml, else the default; one given wins."""
        self.assertEqual(Client(self.tmp).port, DEFAULT_API_PORT)
        self.settings("api_port = 9123\n")
        self.assertEqual(Client(self.tmp).port, 9123)
        self.assertEqual(Client(self.tmp, port=9124).port, 9124)

    def test_bad_settings(self):
        """Settings that can't be read raise SettingsError, saying why."""
        self.settings("api_port = 99999\n")
        with self.assertRaises(SettingsError) as raised:
            Client(self.tmp)
        self.assertIn("api_port must be a whole number", str(raised.exception))
        self.assertIsNone(raised.exception.status)
        Client(self.tmp, port=1)  # not read when the port is given


class OperationsTest(ClientTestCase):
    """The main operations, each one request answered as the API documents."""

    def test_status_and_version(self):
        """status() needs the token; version() is the open status and sends none, even without a token file."""
        self.assertEqual(self.api.status()["node_id"], bytes(range(16)).hex())
        os.unlink(self.token_file)
        seen = self.tokens_seen()
        api = Client(self.tmp, port=self.server.port)
        self.assertEqual(api.version(), self.api.daemon_version)
        self.assertEqual(seen, [("/api/status", None)])

    def test_add_and_cat(self):
        """add_path (made absolute), add_stream of bytes and of a file sent chunked, and cat's ranges."""
        path = self.write("f.bin", b"hello world")
        os.chdir(self.tmp)
        self.addCleanup(os.chdir, ROOT)
        cid = self.api.add_path("f.bin", pin=False, chunker="fastcdc")["cid"]
        self.assertEqual(self.node.called("add_path")[0][0], (path,))
        self.assertEqual(self.node.called("add_path")[0][1]["pin"], False)
        self.assertEqual(self.node.called("add_path")[0][1]["chunker"], "fastcdc")
        self.assertEqual(self.api.add_stream(b"hello world")["cid"], cid)
        self.api.add_stream(io.BytesIO(b"from a file"), name="n.txt")
        self.assertEqual(self.node.called("add_stream")[1][1]["name"], "n.txt")
        with self.api.cat(cid) as stream:
            self.assertEqual(stream.length, 11)
            self.assertEqual(b"".join(stream), b"hello world")
        self.assertEqual(self.api.cat(cid, offset=6, length=3).read(), b"wor")

    def test_fetch_info_ls_stat(self):
        """fetch writes the target out at an absolute path; info, ls and stat answer as the facade."""
        file_cid = self.node.put_file(b"abc")
        directory = self.node.put_dir({"a.txt": file_cid})
        output = os.path.join(self.tmp, "out")
        self.assertEqual(self.api.fetch(directory, output)["cid"], directory)
        with open(os.path.join(output, "a.txt"), "rb") as f:
            self.assertEqual(f.read(), b"abc")
        self.assertEqual(self.api.info(directory)["type"], "directory")
        self.assertEqual(self.api.ls(directory), [{"name": "a.txt", "kind": "file", "size": 3, "cid": file_cid}])
        self.assertEqual(self.api.stat(f"{directory}/a.txt")["size"], 3)

    def test_pins(self):
        """pin_add, pin_ls and pin_rm; removing it again is NotFound."""
        cid = self.node.put_file(b"abc")
        self.assertEqual(self.api.pin_add(cid), {"cid": cid})
        self.assertEqual(self.api.pin_ls(cid), [{"cid": cid, "sources": ["user"], "complete": True}])
        self.api.pin_rm(cid)
        with self.assertRaises(NotFound):
            self.api.pin_rm(cid)

    def test_names(self):
        """name_create, name_publish, name_resolve and name_list."""
        cid = self.node.put_file(b"abc")
        name = self.api.name_create("site")["name"]
        self.assertEqual(self.api.name_publish("site", cid)["sequence"], 1)
        self.assertEqual(self.api.name_resolve(name)["cid"], cid)
        self.assertEqual([entry["alias"] for entry in self.api.name_list()], ["site"])
        with self.assertRaises(NotFound):
            self.api.name_resolve("ab" * 16)

    def test_jobs(self):
        """submit_job, job, jobs, cancel_job and clear_jobs reach the job manager."""
        cid = self.node.put_file(b"abc")
        job = self.api.submit_job("get", cid, "out", from_node="ab" * 16, sealed_name=True)
        self.assertEqual(job["id"], "0123abcd")
        self.assertEqual(self.node.jobs.calls[0],
                         ("submit", "get", cid, os.path.abspath("out"), "ab" * 16, {"sealed_name": True,
                                                                                    "fallback_dht": False}))
        self.assertEqual(self.api.job("0123abcd", wait=2)["state"], "done")
        self.assertEqual(self.node.jobs.calls[1], ("get", "0123abcd", 2))
        self.assertEqual(self.api.jobs(), [{"id": "0123abcd", "state": "queued"}])
        self.assertEqual(self.api.cancel_job("0123abcd")["state"], "cancelled")
        self.assertEqual(self.api.clear_jobs(), 2)
        with self.assertRaises(NotFound):
            self.api.job("0123abce")
        with self.assertRaises(BadRequest):
            self.api.job("nope")

    def test_used_from_several_threads(self):
        """Concurrent first requests prove the daemon once, and all send the token."""
        seen = self.tokens_seen()
        results = []
        threads = [threading.Thread(target=lambda: results.append(self.api.status()["node_id"])) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(results), 8)
        self.assertEqual(seen.count(("/api/status", None)), 1)
        self.assertEqual(seen.count(("/api/status", self.server.token)), 8)


class ProofTest(ClientTestCase):
    """The token is sent only to a listener that proves it knows it."""

    def impostor(self, received: list) -> int:
        """Start a server answering everything 200 with a guessed proof; its port. What it receives goes in
        received as (path, headers, body)."""

        class Impostor(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                """Quiet."""

            def answer(self):
                """200 with a guessed proof."""
                length = int(self.headers.get("Content-Length") or 0)
                received.append((self.path, dict(self.headers), self.rfile.read(length) if length else b""))
                data = b'{"version": "1", "name": "x"}'
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.send_header(apiclient.PROOF_HEADER, "00" * 32)
                self.end_headers()
                self.wfile.write(data)
            do_GET = do_POST = answer

        server = http.server.HTTPServer(("127.0.0.1", 0), Impostor)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server.server_address[1]

    def test_impostor_gets_neither_token_nor_body(self):
        """A listener that can't prove it knows the token gets only the challenge: AuthFailed, nothing sent."""
        received = []
        api = Client(self.tmp, port=self.impostor(received))
        with self.assertRaises(AuthFailed) as raised:
            api.call("POST", "/api/name/import", params={"alias": "stolen"}, body=b"K" * 64)
        self.assertIn("couldn't prove it knows the API token", str(raised.exception))
        self.assertEqual(len(received), 1)
        path, headers, body = received[0]
        self.assertTrue(path.startswith("/api/status"))
        self.assertNotIn("X-Rnfs-Api", headers)
        self.assertIn("X-Rnfs-Challenge", headers)
        self.assertEqual(body, b"")
        self.assertIsNone(api.token)

    def test_stale_token_is_never_sent(self):
        """A token the daemon doesn't hold (an old daemon's) fails the proof and is never sent."""
        cli.write_token(self.token_file, "an old daemon's token")
        seen = self.tokens_seen()
        with self.assertRaises(AuthFailed):
            self.api.status()
        self.assertEqual(seen, [("/api/status", None)])

    def test_version_check_in_one_place(self):
        """The proof request gives the daemon's version to check_daemon, which can refuse before the token goes."""
        refused = []

        class Strict(Client):
            def check_daemon(self, version):
                """Refuse every daemon, noting its version."""
                refused.append(version)
                raise ApiError(f"daemon {version} is too old")

        version = self.api.version()
        seen = self.tokens_seen()
        with self.assertRaises(ApiError):
            Strict(self.tmp, port=self.server.port).post("/api/gc")
        self.assertEqual(refused, [version])
        self.assertEqual(seen, [("/api/status", None)])
        self.assertEqual(self.node.called("gc"), [])
        self.api.status()
        self.assertEqual(self.api.daemon_version, refused[0])


class ErrorTest(ClientTestCase):
    """Every failure is a typed ApiError with the message the CLI prints."""

    def test_no_daemon(self):
        """Nothing on the port: DaemonNotRunning, with no status."""
        with self.assertRaises(DaemonNotRunning) as raised:
            Client(self.tmp, port=free_port()).version()
        self.assertIn("no daemon is running", str(raised.exception))
        self.assertIsNone(raised.exception.status)

    def test_missing_token(self):
        """No token file: TokenError, and the request isn't sent."""
        os.unlink(self.token_file)
        with self.assertRaises(TokenError) as raised:
            self.api.post("/api/gc")
        self.assertIn("no API token", str(raised.exception))
        self.assertEqual(self.node.called("gc"), [])

    @unittest.skipUnless(POSIX_MODES, "Unix permissions")
    def test_token_readable_by_others(self):
        """A token file others can read isn't trusted: TokenError."""
        os.chmod(self.token_file, 0o644)
        with self.assertRaises(TokenError) as raised:
            self.api.status()
        self.assertIn("accessible to other users", str(raised.exception))

    def test_api_statuses(self):
        """Each error the node raises arrives as its class, with the API's message and status."""
        cid = self.node.put_file(b"abc")
        cases = ((ValueError("v"), BadRequest, 400), (errors.NotFound("n"), NotFound, 404),
                 (errors.TooLarge("t"), TooLarge, 413), (errors.InvalidDag("i"), InvalidDag, 422),
                 (errors.Denied("d"), Denied, 451),
                 (errors.Busy("b"), Busy, 503), (errors.StorageFull("s"), StorageFull, 507),
                 (errors.RnfsError("r"), ServerError, 500))
        for error, expected, status in cases:
            with mock.patch.object(self.node, "stat", side_effect=error), mock.patch("rnfs.api.log"):
                with self.assertRaises(expected) as raised:
                    self.api.stat(cid)
            self.assertEqual((str(raised.exception), raised.exception.status), (str(error), status))
            self.assertIsInstance(raised.exception, ApiError)

    def test_refused_without_the_token(self):
        """A protected endpoint asked without the token is AuthFailed (403); an unknown one NotFound."""
        with self.assertRaises(AuthFailed) as raised:
            self.api.call("POST", "/api/gc", authenticated=False)
        self.assertEqual(raised.exception.status, 403)
        with self.assertRaises(NotFound) as raised:
            self.api.get("/api/nothing")
        self.assertIn("no such API endpoint", str(raised.exception))
        self.assertIsInstance(apiclient.error_for(405, "x"), ApiError)
        self.assertNotIsInstance(apiclient.error_for(405, "x"), ServerError)
        self.assertIsInstance(apiclient.error_for(502, "x"), ServerError)

    def test_cut_short_and_invalid_responses(self):
        """A body ending before its Content-Length raises ConnectionFailed at its last read; a body that isn't
        JSON where JSON is expected too."""
        cid = self.node.put_file(b"x" * 100)

        def failing(*args, **kwargs):
            """Ten bytes, then the fetch fails."""
            yield b"x" * 10
            raise errors.NotFound("provider vanished")
        with mock.patch.object(self.node, "read", failing), mock.patch("rnfs.api.log"):
            stream = self.api.cat(cid)
            self.assertEqual(stream.read(10), b"x" * 10)
            with self.assertRaises(ConnectionFailed) as raised:
                stream.read(10)
            stream.close()
        self.assertIn("the transfer stopped after 10 of 100 bytes", str(raised.exception))
        with self.assertRaises(ConnectionFailed) as raised:
            self.api.call("GET", "/api/cat", params={"target": cid})
        self.assertEqual(str(raised.exception), "the daemon sent an invalid response")


if __name__ == "__main__":
    unittest.main()
