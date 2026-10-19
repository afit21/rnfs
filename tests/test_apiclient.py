"""Tests for the client library of the local API (rnfs/apiclient.py), against a live ApiServer with a FakeNode."""
import http.server
import inspect
import io
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from rnfs import apiclient, cli
from rnfs.api import ApiHandler, ApiServer
from rnfs.apiclient import (ApiConnection, ApiError, AuthFailed, BadRequest, Busy, ConnectionFailed, DaemonNotRunning,
                            Denied, InvalidDag, NotFound, ServerError, SettingsError, StorageFull, TokenError, TooLarge)
from rnfs.config import DEFAULT_API_PORT
from rnfs import errors
from tests.support import POSIX_MODES, free_port, real
from tests.test_fsutil import hold_lock
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
    """Base: a NodeWithJobs behind an ApiServer, its token in <tmp>/api_token, and an ApiConnection for it."""

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
        self.api = ApiConnection(self.tmp, port=self.server.port)

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
        """Importing rnfs.apiclient and making an ApiConnection loads no RNS module."""
        code = ("import sys; from rnfs import apiclient; apiclient.ApiConnection(port=1); "
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
        self.assertEqual(ApiConnection(port=1).token_file, apiclient.token_path())
        self.assertEqual(ApiConnection(self.tmp, port=1).token_file, os.path.join(self.tmp, "api_token"))
        self.assertEqual(ApiConnection(port=1, token_file="/x/t").token_file, "/x/t")

    def test_port_from_settings(self):
        """The port is api_port in rnfs.toml, else the default; one given wins."""
        self.assertEqual(ApiConnection(self.tmp).port, DEFAULT_API_PORT)
        self.settings("api_port = 9123\n")
        self.assertEqual(ApiConnection(self.tmp).port, 9123)
        self.assertEqual(ApiConnection(self.tmp, port=9124).port, 9124)

    def test_bad_settings(self):
        """Settings that can't be read raise SettingsError, saying why."""
        self.settings("api_port = 99999\n")
        with self.assertRaises(SettingsError) as raised:
            ApiConnection(self.tmp)
        self.assertIn("api_port must be a whole number", str(raised.exception))
        self.assertIsNone(raised.exception.status)
        ApiConnection(self.tmp, port=1)  # not read when the port is given


class DaemonFileTest(unittest.TestCase):
    """daemon.json: what a running daemon writes about itself, which clients read before rnfs.toml."""

    def setUp(self):
        """An empty rnfs dir."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.path = apiclient.daemon_info_path(self.tmp)

    def write(self, info, mode: int = 0o600):
        """Write daemon.json as the daemon does (cli.write_daemon_info), then set its mode."""
        cli.write_daemon_info(self.path, info)
        os.chmod(self.path, mode)

    def test_port_comes_from_the_running_daemon(self):
        """A daemon.json of a running process gives the port, even where rnfs.toml can't be read (a newer
        setting); the port given still wins."""
        with open(os.path.join(self.tmp, "rnfs.toml"), "w", encoding="utf-8") as f:
            f.write("a_setting_from_the_future = 1\n")
        self.write({"pid": os.getpid(), "api_port": 9555, "version": "0.2.0"})
        self.assertEqual(apiclient.read_daemon_info(self.tmp)["version"], "0.2.0")
        self.assertEqual(ApiConnection(self.tmp).port, 9555)
        self.assertEqual(ApiConnection(self.tmp, port=1).port, 1)
        self.assertEqual(ApiConnection(self.tmp).daemon_info()["api_port"], 9555)

    def test_unusable_files_are_ignored(self):
        """A daemon.json that doesn't parse, lacks the port, is too large or names a process that has ended is
        ignored, and rnfs.toml gives the port."""
        for info in ({"pid": os.getpid()}, {"pid": os.getpid(), "api_port": "9555"},
                     {"pid": os.getpid(), "api_port": 70000}, ["not", "an", "object"]):
            with self.subTest(info=info):
                self.write(info)
                self.assertIsNone(apiclient.read_daemon_info(self.tmp))
                self.assertEqual(ApiConnection(self.tmp).port, DEFAULT_API_PORT)
        with open(self.path, "wb") as f:
            f.write(b"{" + b" " * apiclient.MAX_DAEMON_FILE + b"}")
        self.assertIsNone(apiclient.read_daemon_info(self.tmp))
        if POSIX_MODES:
            ended = subprocess.run([sys.executable, "-c", "import os; print(os.getpid())"], capture_output=True,
                                   text=True, timeout=60).stdout.strip()
            self.write({"pid": int(ended), "api_port": 9555})
            self.assertIsNone(apiclient.read_daemon_info(self.tmp))

    @unittest.skipUnless(POSIX_MODES, "POSIX permission bits")
    def test_a_file_others_can_write_is_not_trusted(self):
        """One other users could have written, or a symlink, is ignored."""
        self.write({"pid": os.getpid(), "api_port": 9555}, mode=0o622)
        self.assertIsNone(apiclient.read_daemon_info(self.tmp))
        os.chmod(self.path, 0o600)
        os.rename(self.path, self.path + ".real")
        os.symlink(self.path + ".real", self.path)
        self.assertIsNone(apiclient.read_daemon_info(self.tmp))

    def test_windows_process_check(self):
        """On Windows a process is alive unless OpenProcess finds none (ERROR_INVALID_PARAMETER) or it has exited;
        one that can't be opened (another user's) or a failing call counts as alive. Run with kernel32 stood in."""
        import ctypes

        class Kernel32:
            """kernel32's three calls, answering as a process in state does."""

            def __init__(self, state):
                self.state, self.closed = state, []
                self.OpenProcess, self.GetExitCodeProcess, self.CloseHandle = (
                    mock.Mock(side_effect=self.open), mock.Mock(side_effect=self.exit_code),
                    mock.Mock(side_effect=self.closed.append))

            def open(self, access, inherit, pid):
                """A handle, or 0 with the last error set."""
                if self.state in ("none", "denied"):
                    Kernel32.error = 87 if self.state == "none" else 5
                    return 0
                return 1234

            def exit_code(self, handle, code):
                """STILL_ACTIVE (259) while running, else 0."""
                if self.state == "failing":
                    return 0
                code._obj.value = 259 if self.state == "running" else 0
                return 1

        for state, alive in (("running", True), ("exited", False), ("none", False), ("denied", True),
                             ("failing", True)):
            with self.subTest(state=state):
                kernel32 = Kernel32(state)
                with mock.patch.object(ctypes, "WinDLL", create=True, return_value=kernel32), \
                        mock.patch.object(ctypes, "get_last_error", create=True, side_effect=lambda: Kernel32.error):
                    self.assertIs(apiclient._windows_process_alive(4321), alive)
                if state not in ("none", "denied"):
                    self.assertEqual(kernel32.closed, [1234])  # the handle is always closed
        with mock.patch.object(ctypes, "WinDLL", create=True, side_effect=OSError("no kernel32")):
            self.assertTrue(apiclient._windows_process_alive(4321))

    def test_removed_only_by_its_own_daemon(self):
        """cli.remove_daemon_info leaves a daemon.json another process wrote."""
        self.write({"pid": os.getpid() + 1, "api_port": 9555})
        cli.remove_daemon_info(self.path, os.getpid())
        self.assertTrue(os.path.exists(self.path))
        self.write({"pid": os.getpid(), "api_port": 9555})
        cli.remove_daemon_info(self.path, os.getpid())
        self.assertFalse(os.path.exists(self.path))


class AppSupportTest(ClientTestCase):
    """The client side of what 0.2.0 added for apps: error codes, capabilities, events and shutdown."""

    def test_error_codes(self):
        """An error carries the daemon's code; an unknown endpoint is NoSuchEndpoint (a NotFound), as is an older
        daemon's, told by its message; something the daemon can't do is Unsupported."""
        with self.assertRaises(apiclient.NoSuchEndpoint) as raised:
            self.api.get("/api/nope")
        self.assertEqual((raised.exception.status, raised.exception.code), (404, "no_such_endpoint"))
        self.assertIsInstance(raised.exception, NotFound)
        with self.assertRaises(NotFound) as raised:
            self.api.stat("bafkreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        self.assertNotIsInstance(raised.exception, apiclient.NoSuchEndpoint)
        old = apiclient.error_for(404, "no such API endpoint: /api/events")
        self.assertIsInstance(old, apiclient.NoSuchEndpoint)
        self.assertIsNone(apiclient.error_for(404, "no provider found").code)
        self.assertIsInstance(apiclient.error_for(404, "x", "unsupported"), errors.Unsupported)
        with self.assertRaises(apiclient.Unsupported):
            self.api.shutdown()

    def test_capabilities_and_events(self):
        """capabilities() lists the daemon's; {} from one without the endpoint. events() long-polls the log."""
        self.assertIn("events", self.api.capabilities()["capabilities"])
        with mock.patch.object(self.api, "get", side_effect=apiclient.NoSuchEndpoint("no such API endpoint", 404)):
            self.assertEqual(self.api.capabilities(), {})
        start = self.api.events()
        self.node.events.publish("cache", change="removed", cids=[], count=0)
        result = self.api.events(after=start["next"], kinds=["cache"], wait=1)
        self.assertEqual([event["kind"] for event in result["events"]], ["cache"])
        self.assertEqual(result["epoch"], start["epoch"])

    def test_shutdown_waits_until_stopped(self):
        """shutdown(wait=) returns True once the API no longer answers; False while it still does."""
        stopping = threading.Event()
        server = ApiServer(self.node, 0, token=self.server.token, shutdown=stopping.set)
        server.start()
        self.addCleanup(server.stop)
        api = ApiConnection(self.tmp, port=server.port)
        threading.Thread(target=lambda: stopping.wait(10) and server.stop(), daemon=True).start()
        self.assertTrue(api.shutdown(wait=10))
        self.assertTrue(stopping.is_set())
        stopping.clear()
        still = ApiServer(self.node, 0, token=self.server.token, shutdown=stopping.set)
        still.start()
        self.addCleanup(still.stop)
        self.assertFalse(ApiConnection(self.tmp, port=still.port).shutdown(wait=0.5))

    def test_shutdown_waits_for_the_process(self):
        """When daemon.json names the port, shutdown(wait=) also waits for that process to end (it saves its state
        and releases the rnfs dir after its API stops), so a daemon started next can take the dir."""
        daemon = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        self.addCleanup(daemon.wait)
        self.addCleanup(daemon.kill)
        stopping = threading.Event()
        server = ApiServer(self.node, 0, token=self.server.token, shutdown=stopping.set)
        server.start()
        self.addCleanup(server.stop)
        cli.write_daemon_info(apiclient.daemon_info_path(self.tmp), {"pid": daemon.pid, "api_port": server.port})
        threading.Thread(target=lambda: stopping.wait(10) and server.stop(), daemon=True).start()
        api = ApiConnection(self.tmp, port=server.port)
        self.assertFalse(api.shutdown(wait=0.5))  # the API stopped; the process runs on
        self.assertFalse(api._stopped(daemon.pid, 1))
        daemon.kill()
        daemon.wait()  # reaped, so the process is gone
        self.assertTrue(api._stopped(daemon.pid, 1))

    def test_a_silent_listener_cannot_hold_shutdown(self):
        """Each status poll is bounded by what is left of wait: a listener that accepts and never answers can't
        hold shutdown(wait=) past it."""
        stopping = threading.Event()
        server = ApiServer(self.node, 0, token=self.server.token, shutdown=stopping.set)
        server.start()
        self.addCleanup(server.stop)
        api = ApiConnection(self.tmp, port=server.port)
        silent = socket.socket()
        silent.bind(("127.0.0.1", 0))
        silent.listen()
        self.addCleanup(silent.close)
        real_call = api.call

        def call(method, path, *args, **kwargs):
            """The shutdown itself reaches the daemon; every poll after it a listener that never answers."""
            if path == "/api/shutdown":
                return real_call(method, path, *args, **kwargs)
            api.port = silent.getsockname()[1]
            return real_call(method, path, *args, **kwargs)

        with mock.patch.object(api, "call", side_effect=call):
            began = time.monotonic()
            api.shutdown(wait=1)
        self.assertLess(time.monotonic() - began, 5)

    def test_a_stalled_body_is_connection_failed(self):
        """A body that stops arriving within the call's timeout raises ConnectionFailed, never a bare
        TimeoutError (Stream.read and read_all)."""
        response = mock.Mock(getheader=lambda name: "10")
        response.read.side_effect = TimeoutError("timed out")
        with self.assertRaises(ConnectionFailed):
            apiclient.Stream(response).read(5)
        with self.assertRaises(ConnectionFailed):
            apiclient.read_all(response)


class OperationsTest(ClientTestCase):
    """The main operations, each one request answered as the API documents."""

    def test_status_and_version(self):
        """status() needs the token; version() is the open status and sends none, even without a token file."""
        self.assertEqual(self.api.status()["node_id"], bytes(range(16)).hex())
        os.unlink(self.token_file)
        seen = self.tokens_seen()
        api = ApiConnection(self.tmp, port=self.server.port)
        self.assertEqual(api.version(), self.api.daemon_version)
        self.assertEqual(seen, [("/api/status", None)])

    def test_add_and_cat(self):
        """add_path (made absolute), add_stream of bytes and of a file sent chunked, and cat's ranges."""
        path = self.write("f.bin", b"hello world")
        os.chdir(self.tmp)
        self.addCleanup(os.chdir, ROOT)
        cid = self.api.add_path("f.bin", pin=False, chunker="fastcdc")["cid"]
        sent, = self.node.called("add_path")[0][0]
        self.assertTrue(os.path.isabs(sent))
        self.assertEqual(real(sent), real(path))  # macOS: getcwd() is /private/var, tempfile's /var
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

    def test_directories_archives_and_maintenance(self):
        """dir_set and dir_rm, export and import_archive (paths made absolute), gc and verify (ND4)."""
        file_cid = self.node.put_file(b"abc")
        directory = self.node.put_dir({"a": file_cid})
        edited = self.api.dir_set(directory, "b", file_cid, executable=True)["cid"]
        self.assertEqual(self.node.called("dir_set")[0], ((directory, "b", file_cid), {"executable": True}))
        self.assertEqual(self.api.dir_rm(f"{edited}/b"), {"cid": directory})
        with self.assertRaises(BadRequest):
            self.api.dir_rm(directory)
        os.chdir(self.tmp)
        self.addCleanup(os.chdir, ROOT)
        self.assertEqual(self.api.export(directory, "archive")["roots"], [directory])
        sent = self.node.called("export")[0][0][1]
        self.assertEqual(real(sent), real(os.path.join(self.tmp, "archive")))
        self.assertEqual(len(self.api.import_archive("archive")["roots"]), 1)
        self.assertTrue(os.path.isabs(self.node.called("import_archive")[0][0][0]))
        self.assertEqual(self.api.gc(), {"removed_blocks": 3, "freed_bytes": 300})
        self.assertEqual(self.api.verify()["bad"], [])

    def test_remote_pins_providers_and_ping(self):
        """pin_remote, providers and ping, node IDs in hex (ND4)."""
        cid = self.node.put_file(b"abc")
        self.assertIs(self.api.pin_remote("ab" * 16, cid), True)
        self.assertIs(self.api.pin_remote("ff" * 16, cid, pin=False), False)
        self.assertEqual(self.node.called("pin_remote"), [((bytes.fromhex("ab" * 16), cid, True), {}),
                                                          ((bytes.fromhex("ff" * 16), cid, False), {})])
        self.node.provider_map[cid] = [bytes.fromhex("cd" * 16)]
        self.assertEqual(self.api.providers(cid), ["cd" * 16])
        self.assertEqual(self.api.ping("cd" * 16), 0.25)
        self.assertIsNone(self.api.ping("ff" * 16))

    def test_name_keys(self):
        """name_export, name_rm and name_import (the key as the body) (ND4)."""
        name = self.api.name_create("site")["name"]
        key = self.api.name_export("site")
        self.assertEqual(key, self.node.keys["site"])
        self.api.name_rm("site")
        with self.assertRaises(NotFound):
            self.api.name_rm("site")
        self.assertEqual(self.api.name_import("again", memoryview(key)), {"alias": "again", "name": name})

    def test_every_core_route_has_a_method(self):
        """Each of the API's own routes is sent by one of ApiConnection's methods, so neither rnfs.Client nor
        another program writes a path by hand (ND4); the features' routes are reached through get() and post()."""
        source = inspect.getsource(ApiConnection)
        for method, path in ApiHandler.ROUTES:
            with self.subTest(f"{method} {path}"):
                self.assertRegex(source, rf'self\.(call|request)\("{method}", "{path}"|'
                                         rf'self\._?{method.lower()}\("{path}"')

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
        """Concurrent requests each prove the daemon on their own connection, and all send the token."""
        seen = self.tokens_seen()
        results = []
        threads = [threading.Thread(target=lambda: results.append(self.api.status()["node_id"])) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(results), 8)
        self.assertEqual(seen.count(("/api/status", None)), 8)
        self.assertEqual(seen.count(("/api/status", self.server.token)), 8)


class ProofTest(ClientTestCase):
    """The token is sent only to a listener that proves it knows it."""

    def impostor(self, received: list, port: int = 0) -> int:
        """Start a server on port (0: any) answering everything 200 with a guessed proof; its port. What it
        receives goes in received as (path, headers, body)."""

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

        server = http.server.HTTPServer(("127.0.0.1", port), Impostor)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server.server_address[1]

    def test_impostor_gets_neither_token_nor_body(self):
        """A listener that can't prove it knows the token gets only the challenge: AuthFailed, nothing sent."""
        received = []
        api = ApiConnection(self.tmp, port=self.impostor(received))
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

    def test_impostor_after_the_daemon_stops(self):
        """A client that has used the daemon sends nothing to a listener taking its port once it stops."""
        self.api.status()
        self.server.stop()
        received = []
        self.impostor(received, self.server.port)
        with self.assertRaises(AuthFailed):
            self.api.call("POST", "/api/name/import", params={"alias": "stolen"}, body=b"K" * 64)
        self.assertEqual(len(received), 1)
        path, headers, body = received[0]
        self.assertTrue(path.startswith("/api/status"))
        self.assertNotIn("X-Rnfs-Api", headers)
        self.assertEqual(body, b"")

    def test_request_goes_on_the_proved_connection(self):
        """The proof and the request it allows are one connection, which the daemon then closes."""
        clients = []
        original = self.server.server.RequestHandlerClass.dispatch

        def spy(handler):
            """Record which connection each request came on."""
            clients.append((handler.path.split("?")[0], handler.client_address))
            original(handler)
        with mock.patch.object(self.server.server.RequestHandlerClass, "dispatch", spy):
            self.api.post("/api/gc")
            self.api.post("/api/gc")
        self.assertEqual([path for path, _ in clients], ["/api/status", "/api/gc"] * 2)
        self.assertEqual(clients[0][1], clients[1][1])
        self.assertEqual(clients[2][1], clients[3][1])
        self.assertNotEqual(clients[0][1], clients[2][1])

    def test_daemon_before_0_1_5_is_refused(self):
        """A daemon before 0.1.5 (which closes the proof's connection) is refused before the token is sent: since
        0.2.0 the request no longer goes on a new connection."""
        seen = self.tokens_seen()
        with mock.patch.object(self.server.server.RequestHandlerClass, "PROOF_ROUTE", None), \
                mock.patch("rnfs.api.__version__", "0.1.4"):
            with self.assertRaises(ApiError) as raised:
                self.api.status()
        self.assertIn("too old for this client", str(raised.exception))
        self.assertEqual(seen, [("/api/status", None)])

    def test_newer_daemon_closing_the_connection(self):
        """A daemon of 0.1.5 or later that closes the proof's connection gets nothing more (API-4)."""
        seen = self.tokens_seen()
        with mock.patch.object(self.server.server.RequestHandlerClass, "PROOF_ROUTE", None), \
                mock.patch("rnfs.api.__version__", "0.1.5"):
            with self.assertRaises(ConnectionFailed) as raised:
                self.api.status()
        self.assertIn("closed the connection it proved itself on", str(raised.exception))
        self.assertEqual(seen, [("/api/status", None)])

    def test_version_numbers(self):
        """The numbers a version starts with, for the fallback's comparison."""
        for version, expected in (("0.1.4", (0, 1, 4)), ("0.1.5.dev1", (0, 1, 5)), ("0.1.4.post1", (0, 1, 4)),
                                  ("1", (1,)), ("10.0rc1", (10, 0)), ("", None), ("v0.1.4", None), (None, None),
                                  (15, None)):
            self.assertEqual(apiclient.version_numbers(version), expected, version)

    def test_daemon_restarted(self):
        """A client used before the daemon restarted reads its new token and goes on working."""
        self.api.status()
        self.server.stop()
        restarted = ApiServer(self.node, self.server.port)
        restarted.start()
        self.addCleanup(restarted.stop)
        cli.write_token(self.token_file, restarted.token)
        self.assertEqual(self.api.status()["node_id"], self.node.status()["node_id"])
        self.assertEqual(self.api.token, restarted.token)

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

        class Strict(ApiConnection):
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


class TimeoutTest(ClientTestCase):
    """Timeouts per call (timeout=) and per thread (using_timeout), over the connection's own."""

    def slow_jobs(self, delay: float = 1.0):
        """Make GET /api/jobs/get answer delay seconds late (the server's log quiet: answering a client that timed
        out fails)."""
        original = self.node.jobs.get

        def slow(job_id, wait):
            """The job, late."""
            time.sleep(delay)
            return original(job_id, wait)
        for patcher in (mock.patch.object(self.node.jobs, "get", slow), mock.patch("rnfs.api.log")):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_a_calls_own_timeout(self):
        """A call's timeout= bounds it alone: the slow answer raises ConnectionFailed for it, while a call without
        one (the connection has none) waits for it."""
        self.slow_jobs()
        started = time.monotonic()
        with self.assertRaises(ConnectionFailed) as raised:
            self.api.get("/api/jobs/get", id="0123abcd", timeout=0.3)
        self.assertLess(time.monotonic() - started, 0.9)
        self.assertIn("didn't answer within its timeout (0.3 seconds)", str(raised.exception))
        self.assertEqual(self.api.get("/api/jobs/get", id="0123abcd")["state"], "done")
        with self.assertRaises(ConnectionFailed):
            self.api.call("GET", "/api/jobs/get", params={"id": "0123abcd"}, timeout=0.3)

    def test_none_lifts_the_connections_timeout(self):
        """A connection's timeout bounds every call but one given timeout=None, or made inside
        using_timeout(None)."""
        self.slow_jobs()
        api = ApiConnection(self.tmp, port=self.server.port, timeout=0.3)
        with self.assertRaises(ConnectionFailed):
            api.job("0123abcd")
        self.assertEqual(api.get("/api/jobs/get", id="0123abcd", timeout=None)["state"], "done")
        with api.using_timeout(None):
            self.assertEqual(api.job("0123abcd")["state"], "done")
        self.assertEqual(api.effective_timeout(), 0.3)

    def test_using_timeout_is_the_threads_own(self):
        """using_timeout bounds this thread's requests only: another thread's, sent meanwhile, waits for its
        answer. It nests, the outer timeout back on leaving the inner block, and none after the outer."""
        self.slow_jobs()
        inside, other = threading.Event(), []

        def request():
            """A request sent while the main thread is inside using_timeout."""
            inside.wait(5)
            other.append(self.api.job("0123abcd")["state"])
        thread = threading.Thread(target=request)
        thread.start()
        with self.api.using_timeout(0.3):
            inside.set()
            with self.assertRaises(ConnectionFailed):
                self.api.job("0123abcd")
            with self.api.using_timeout(None):
                self.assertEqual(self.api.job("0123abcd")["state"], "done")
            self.assertEqual(self.api.effective_timeout(), 0.3)
            self.assertEqual(self.api.effective_timeout(5), 5)  # a call's own comes first
        thread.join(10)
        self.assertEqual(other, ["done"])
        self.assertIsNone(self.api.effective_timeout())

    def test_proof_within_the_timeout(self):
        """Connecting and proving take at most the call's timeout, when shorter than PROOF_TIMEOUT: a listener
        that never answers holds a call only that long."""
        listener = socket.socket()
        self.addCleanup(listener.close)
        listener.bind(("127.0.0.1", 0))
        listener.listen(4)  # connections complete in the backlog; nothing is ever read or answered
        api = ApiConnection(self.tmp, port=listener.getsockname()[1])
        started = time.monotonic()
        with self.assertRaises(ConnectionFailed):
            api.get("/api/status", timeout=0.5)
        with api.using_timeout(0.5), self.assertRaises(ConnectionFailed):
            api.status()
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual((api._proof_timeout(None), api._proof_timeout(100), api._proof_timeout(2)),
                         (apiclient.PROOF_TIMEOUT, apiclient.PROOF_TIMEOUT, 2))

    def test_timeouts_are_checked(self):
        """A timeout is None or a positive number: anything else is ValueError, before anything is sent."""
        for bad in (0, -1, True, "5", float("inf"), float("nan")):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    self.api.get("/api/status", timeout=bad)
                with self.assertRaises(ValueError), self.api.using_timeout(bad):
                    pass


class ErrorTest(ClientTestCase):
    """Every failure is a typed ApiError with the message the CLI prints."""

    def test_no_daemon(self):
        """Nothing on the port: DaemonNotRunning, with no status."""
        with self.assertRaises(DaemonNotRunning) as raised:
            ApiConnection(self.tmp, port=free_port()).version()
        self.assertIn("no daemon is running", str(raised.exception))
        self.assertIsNone(raised.exception.status)
        self.assertNotIsInstance(raised.exception, apiclient.DaemonStarting)

    def test_daemon_starting(self):
        """Nothing on the port, but a process holds the rnfs dir's lock (a daemon opening a large block store
        before its API listens): DaemonStarting, a DaemonNotRunning, naming that process."""
        holder = hold_lock(os.path.join(self.tmp, "daemon.lock"))
        try:
            with self.assertRaises(apiclient.DaemonStarting) as raised:
                ApiConnection(self.tmp, port=free_port()).version()
            self.assertIsInstance(raised.exception, DaemonNotRunning)
            self.assertEqual(raised.exception.pid, holder.pid)
            self.assertIn(f"the daemon (process {holder.pid}) is starting", str(raised.exception))
            self.assertEqual(apiclient.dir_holder(self.tmp), holder.pid)
        finally:
            holder.stdin.close()
            holder.wait(30)
            holder.stdout.close()
        # Once it has gone, the lock file is left, naming it: no daemon is running
        self.assertIsNone(apiclient.dir_holder(self.tmp))
        with self.assertRaises(DaemonNotRunning) as raised:
            ApiConnection(self.tmp, port=free_port()).version()
        self.assertNotIsInstance(raised.exception, apiclient.DaemonStarting)

    def test_lock_of_a_process_gone_isnt_tested(self):
        """A lock file naming a process that has ended isn't tested (testing it could make a daemon starting at that
        moment fail), nor is one naming this process (its own Node)."""
        for pid in (os.getpid(), 2**22 + 12345):
            with self.subTest(pid=pid):
                with open(os.path.join(self.tmp, "daemon.lock"), "w", encoding="ascii") as f:
                    f.write(f"{pid}\n")
                with mock.patch.object(apiclient.fsutil, "lock_held", side_effect=AssertionError("tested")), \
                        mock.patch.object(apiclient, "_process_alive", return_value=pid == os.getpid()):
                    self.assertIsNone(apiclient.dir_holder(self.tmp))

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
                 (errors.RnfsError("r"), ServerError, 500), (errors.BlockMismatch("m"), BadRequest, 400),
                 (errors.SealError("s"), BadRequest, 400), (errors.NotRecipient("n"), BadRequest, 400))
        for error, expected, status in cases:
            with mock.patch.object(self.node, "stat", side_effect=error), mock.patch("rnfs.api.log"):
                with self.assertRaises(expected) as raised:
                    self.api.stat(cid)
            self.assertEqual((str(raised.exception), raised.exception.status), (str(error), status))
            self.assertIsInstance(raised.exception, ApiError)

    def test_status_table_follows_the_node_errors(self):
        """STATUS_ERRORS comes from rnfs.errors.STATUSES (XC4): each status's error is an ApiError and a subclass of
        the node's class for it, so one except clause catches a Node's error and a Client's; 403 is AuthFailed."""
        self.assertEqual(set(apiclient.STATUS_ERRORS), {status for _, status in errors.STATUSES} | {403})
        for error_class, status in errors.STATUSES:
            with self.subTest(status):
                client_class = apiclient.STATUS_ERRORS[status]
                self.assertTrue(issubclass(client_class, ApiError) and issubclass(client_class, error_class))
                self.assertIs(type(apiclient.error_for(status, "x")), client_class)
        self.assertIs(apiclient.STATUS_ERRORS[403], AuthFailed)

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
        with mock.patch.object(self.node, "read_file", lambda *args, **kwargs: (
                {"type": "file", "size": 100, "cid": cid}, failing())), mock.patch("rnfs.api.log"):
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
