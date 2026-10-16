"""Tests for the command line (rnfs/cli.py): parsing, and each command against a live ApiServer with a FakeNode."""
import contextlib
import http.server
import io
import os
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

from rnfs import cli, fsutil
from rnfs.api import ApiServer
from rnfs.content.cid import RAW, Cid
from tests.support import CAN_SYMLINK, POSIX_MODES, free_port, real
from tests.test_api import FakeNode

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class CliTestCase(unittest.TestCase):
    """Base: a FakeNode behind an ApiServer whose token is in <tmp>/api_token, and run_cli() to call cli.main."""

    def setUp(self):
        self.node = FakeNode()
        self.server = ApiServer(self.node, 0)
        self.server.start()
        self.addCleanup(self.server.stop)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.token_file = os.path.join(self.tmp, "api_token")
        cli.write_token(self.token_file, self.server.token)

    def run_cli(self, *argv, stdin: bytes = b"", port=None, tty=False):
        """Run cli.main(argv) with --api-port and --dir tmp; returns (exit code, stdout bytes, stderr text)."""
        stdout_bytes = io.BytesIO()
        # newline="\n": compare output the same way on every OS (Windows would write \r\n)
        stdout = io.TextIOWrapper(stdout_bytes, encoding="utf-8", newline="\n", write_through=True)
        stdout.isatty = lambda: tty
        stdin_stream = io.TextIOWrapper(io.BytesIO(stdin), encoding="utf-8")
        stderr = io.StringIO()
        code = 0
        with mock.patch("sys.stdout", stdout), mock.patch("sys.stdin", stdin_stream), \
                contextlib.redirect_stderr(stderr):
            try:
                cli.main([*argv, "--api-port", str(port or self.server.port), "--dir", self.tmp])
            except SystemExit as e:
                if isinstance(e.code, str):
                    print(e.code, file=sys.stderr)
                    code = 1
                else:
                    code = e.code or 0
            stdout.flush()
        return code, stdout_bytes.getvalue(), stderr.getvalue()

    def ok(self, *argv, **kwargs) -> str:
        """stdout (text) of a command that must succeed."""
        code, out, err = self.run_cli(*argv, **kwargs)
        self.assertEqual(code, 0, err)
        return out.decode()

    def write(self, name: str, data: bytes) -> str:
        path = os.path.join(self.tmp, name)
        with open(path, "wb") as f:
            f.write(data)
        return path


class ParsingTest(unittest.TestCase):

    def test_common_options_before_or_after_the_command(self):
        parser = cli.build_parser()
        self.assertEqual(parser.parse_args(["--api-port", "5", "status"]).api_port, 5)
        self.assertEqual(parser.parse_args(["status", "--api-port", "6"]).api_port, 6)
        self.assertEqual(parser.parse_args(["pin", "ls", "--api-port", "7"]).api_port, 7)
        self.assertIsNone(parser.parse_args(["status"]).api_port)
        self.assertEqual(parser.parse_args(["--dir", "/x", "status"]).dir, "/x")

    def test_daemon_options(self):
        args = cli.build_parser().parse_args(["daemon", "--bootstrap", "aa" * 16, "bb" * 16, "--dht-mode", "client",
                                              "--slow", "yes", "--private", "-vv", "--gateway-port", "0"])
        self.assertEqual(args.bootstrap, ["aa" * 16, "bb" * 16])
        self.assertEqual((args.dht_mode, args.slow, args.private, args.verbose, args.gateway_port),
                         ("client", "yes", True, 2, 0))
        args = cli.build_parser().parse_args(["daemon"])
        self.assertEqual((args.bootstrap, args.private, args.dht_mode), (None, None, None))
        self.assertIsNone(args.debug_log)  # rnfs.toml's debug_log then decides
        self.assertTrue(cli.build_parser().parse_args(["daemon", "--debug-log"]).debug_log)

    def test_every_spec_command_parses(self):
        cid = str(Cid.for_block(RAW, b""))
        node = "ab" * 16
        for argv in (["add", "x", "--wrap", "--only-hash"], ["add", "-"], ["get", cid, "-o", "o", "--from", node],
                     ["cat", cid, "--offset", "1", "--length", "2"], ["ls", cid], ["stat", cid],
                     ["dir", "set", cid, "n", cid, "--exec"], ["dir", "rm", f"{cid}/n"], ["pin", "add", cid],
                     ["pin", "rm", cid], ["pin", "ls"], ["pin", "ls", cid], ["pin", "remote", "add", node, cid],
                     ["pin", "remote", "rm", node, cid], ["gc"], ["verify"], ["providers", cid], ["ping", node],
                     ["name", "create", "a"], ["name", "rm", "a"], ["name", "export", "a"], ["name", "import", "a"],
                     ["name", "list"], ["name", "publish", "a", cid], ["name", "resolve", node],
                     ["export", cid, "-o", "f"], ["import", "f"], ["status"], ["daemon"]):
            self.assertTrue(callable(cli.build_parser().parse_args(argv).run), argv)

    def test_default_output(self):
        cid = str(Cid.for_block(RAW, b""))
        self.assertEqual(cli.default_output(cid), cid)
        self.assertEqual(cli.default_output(f"{cid}/dir/file.txt"), "file.txt")
        self.assertEqual(cli.default_output(f"{cid}/dir/"), "dir")
        # On Windows a name with "\\" would write outside the current directory, and ":" to a stream
        with mock.patch.object(fsutil, "IS_WINDOWS", True):
            for name in ("..\\..\\evil.txt", "a:b", "con"):
                self.assertEqual(cli.default_output(f"{cid}/{name}"), cid, name)
            self.assertEqual(cli.default_output(f"{cid}/file.txt"), "file.txt")
        # Names are the DAG maker's choice: no dotfiles in the current directory, no terminal escapes
        for name in (".bash_login", ".npmrc", "\x1b]0;title\x07", "a\x1b[2Jb", "..", "a\\b"):
            self.assertEqual(cli.default_output(f"{cid}/{name}"), cid, repr(name))
        self.assertEqual(cli.default_output(f"{cid}/dir/rapport été.pdf"), "rapport été.pdf")

    def test_sealed_output_without_job_manager(self):
        base = tempfile.gettempdir()  # nothing is written: the daemon is a mock
        output = os.path.join(base, "CID")
        args = cli.build_parser().parse_args(["get", "CID", "--from", "ab" * 16])
        args.from_node = "ab" * 16
        api = mock.Mock()
        with mock.patch.object(cli, "client", return_value=api):
            for name, expected in (("report.pdf", "report.pdf"), (".bashrc", "CID"), ("\x1b[31mred", "CID"),
                                   (None, "CID")):
                api.get.return_value = {"sealed": True, "name": name}
                self.assertEqual(cli.sealed_output(args, output), os.path.join(base, expected), name)
        # --from reaches the info request: without it the daemon would look the root up in the DHT first
        self.assertEqual(api.get.call_args, mock.call("/api/info", target="CID", **{"from": "ab" * 16}))

    def test_printable(self):
        self.assertEqual(cli.printable("ok name"), "ok name")
        self.assertEqual(cli.printable("evil\x1b[2Jname\n"), "evil\\x1b[2Jname\\n")

    def test_client_commands_do_not_import_rns(self):
        code = ("import sys; from rnfs import cli; cli.build_parser().parse_args(['status']); "
                "print('RNS' in sys.modules)")
        result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, timeout=60,
                                encoding="utf-8", errors="replace")
        self.assertEqual(result.stdout.strip(), "False", result.stderr)

    def test_entry_points(self):
        for argv in ([sys.executable, "rnfs.py", "--help"], [sys.executable, "-m", "rnfs", "--help"]):
            result = subprocess.run(argv, cwd=ROOT, capture_output=True, timeout=60, encoding="utf-8", errors="replace")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("daemon", result.stdout)


class ContentCommandTest(CliTestCase):

    def test_add_path(self):
        path = self.write("f.txt", b"hello")
        cid = str(Cid.for_block(RAW, b"hello"))
        with contextlib.chdir(self.tmp):
            self.assertEqual(self.ok("add", "f.txt"), cid + "\n")
        (added,), options = self.node.called("add_path")[0]
        self.assertEqual((real(added), options), (real(path), {"wrap": False, "only_hash": False}))
        code, out, err = self.run_cli("add", path, "--wrap", "--only-hash")
        self.assertEqual(code, 0)
        self.assertIn("warning: skipped a symlink", err)
        self.assertEqual(self.node.called("add_path")[1][1], {"wrap": True, "only_hash": True})
        code, _, err = self.run_cli("add", os.path.join(self.tmp, "missing"))
        self.assertEqual(code, 1)
        self.assertIn("no such file", err)

    def test_add_stdin(self):
        self.assertEqual(self.ok("add", "-", stdin=b"from stdin"), str(Cid.for_block(RAW, b"from stdin")) + "\n")
        data, kwargs = self.node.called("add_stream")[0]
        self.assertEqual(data, (b"from stdin",))
        self.assertEqual(kwargs, {"size": None, "name": None, "only_hash": False})  # sent chunked
        self.ok("add", "-", "--wrap", "--name", "n.txt", stdin=b"x")
        self.assertEqual(self.node.called("add_stream")[1][1]["name"], "n.txt")
        self.assertEqual(self.run_cli("add", "-", "--wrap", stdin=b"x")[0], 1)

    def test_get(self):
        cid = self.node.put_file(b"content")
        directory = self.node.put_dir({"file.txt": cid})
        with contextlib.chdir(self.tmp):
            self.ok("get", f"{directory}/file.txt")
            self.ok("get", directory, "--from", "ab" * 16)
            code, _, err = self.run_cli("get", directory)
        with open(os.path.join(self.tmp, "file.txt"), "rb") as f:
            self.assertEqual(f.read(), b"content")
        self.assertTrue(os.path.isfile(os.path.join(self.tmp, directory, "file.txt")))
        (target, output), options = self.node.called("get")[1]
        self.assertEqual((target, real(output), options),
                         (directory, real(os.path.join(self.tmp, directory)), {"from_node": bytes.fromhex("ab" * 16)}))
        self.assertEqual(code, 1)
        self.assertIn("already exists", err)
        out = os.path.join(self.tmp, "chosen")
        self.assertEqual(self.ok("get", cid, "-o", out), out + "\n")

    def test_cat(self):
        cid = self.node.put_file(b"0123456789")
        self.assertEqual(self.run_cli("cat", cid)[1], b"0123456789")
        self.assertEqual(self.run_cli("cat", cid, "--offset", "3", "--length", "4")[1], b"3456")
        code, _, err = self.run_cli("cat", "not-a-cid")
        self.assertEqual(code, 1)
        self.assertIn("rnfs: invalid CID", err)

    def test_ls_and_stat(self):
        cid = self.node.put_file(b"abc")
        directory = self.node.put_dir({"a file": cid, "evil\x1bname": cid, "sub": self.node.put_dir({})})
        lines = self.ok("ls", directory).splitlines()
        self.assertEqual(lines[0], f"file                  3 {cid} a file")
        self.assertIn("evil\\x1bname", lines[1])
        self.assertTrue(lines[2].startswith("directory") and lines[2].endswith(" sub/"))
        out = self.ok("stat", directory)
        self.assertIn(f"cid: {directory}", out)
        self.assertIn("size: 6", out)

    def test_dir_commands(self):
        cid = self.node.put_file(b"abc")
        directory = self.node.put_dir({})
        new = self.ok("dir", "set", directory, "x", cid, "--exec").strip()
        self.assertEqual(self.node.called("dir_set")[0], ((directory, "x", cid), {"executable": True}))
        self.assertEqual(self.ok("dir", "rm", f"{new}/x").strip(), directory)
        code, _, err = self.run_cli("dir", "rm", directory)
        self.assertEqual(code, 1)


class OtherCommandTest(CliTestCase):

    def test_pins(self):
        cid = self.node.put_file(b"abc")
        self.assertEqual(self.ok("pin", "add", cid), cid + "\n")
        self.assertEqual(self.ok("pin", "ls"), f"{cid} user\n")
        self.assertEqual(self.ok("pin", "ls", cid), f"{cid} user\n")
        self.ok("pin", "rm", cid)
        self.assertEqual(self.ok("pin", "ls"), "")
        code, _, err = self.run_cli("pin", "rm", cid)
        self.assertEqual((code, err.strip()), (1, f"rnfs: {cid} is not pinned"))
        self.assertEqual(self.ok("pin", "remote", "add", "ab" * 16, cid), "accepted\n")
        self.ok("pin", "remote", "rm", "ab" * 16, cid)
        self.assertEqual([args for args, _ in self.node.called("pin_remote")],
                         [(bytes.fromhex("ab" * 16), cid, True), (bytes.fromhex("ab" * 16), cid, False)])
        self.assertEqual(self.run_cli("pin", "remote", "add", "ff" * 16, cid)[0], 1)

    def test_maintenance_and_network(self):
        cid = self.node.put_file(b"abc")
        self.assertEqual(self.ok("gc"), "removed 3 blocks, freed 300 bytes\n")
        self.assertIn("checked 1 blocks", self.ok("verify"))
        self.node.provider_map[cid] = [bytes.fromhex("cd" * 16)]
        self.assertEqual(self.ok("providers", cid), "cd" * 16 + "\n")
        self.assertEqual(self.run_cli("providers", self.node.put_file(b"other"))[0], 1)
        self.assertEqual(self.ok("ping", "cd" * 16), "round-trip time 250 ms\n")
        self.assertEqual(self.run_cli("ping", "ff" * 16)[0], 1)
        self.assertIn(f"node_id: {bytes(range(16)).hex()}", self.ok("status"))

    def test_names(self):
        cid = self.node.put_file(b"abc")
        name = self.ok("name", "create", "site").strip()
        self.assertEqual(len(name), 32)
        # Export refuses a terminal, writes to stdout when redirected, and creates -o files privately
        self.assertEqual(self.run_cli("name", "export", "site", tty=True)[0], 1)
        key = self.run_cli("name", "export", "site")[1]
        self.assertEqual(key, self.node.keys["site"])
        key_file = os.path.join(self.tmp, "key")
        self.ok("name", "export", "site", "-o", key_file)
        if POSIX_MODES:
            self.assertEqual(os.stat(key_file).st_mode & 0o777, 0o600)
        with open(key_file, "rb") as f:
            self.assertEqual(f.read(), self.node.keys["site"])  # written in binary, also on Windows
        self.assertEqual(self.run_cli("name", "export", "site", "-o", key_file)[0], 1)  # never overwrites
        self.assertEqual(self.ok("name", "import", "copy", key_file).strip(), name)
        self.assertEqual(self.ok("name", "import", "copy2", stdin=key).strip(), name)
        self.assertIn(f"published {cid} as {name} (sequence 1)", self.ok("name", "publish", "site", cid))
        self.assertEqual(self.ok("name", "resolve", name), cid + "\n")
        self.assertIn(f"site\t{name}\t{cid}\t1", self.ok("name", "list"))
        self.ok("name", "rm", "copy")
        self.assertNotIn("copy\t", self.ok("name", "list"))

    def test_archives(self):
        cid = self.node.put_file(b"abc")
        with contextlib.chdir(self.tmp):
            self.assertIn("exported 1 blocks", self.ok("export", cid, "-o", "a.rnfs"))
            exported, output = self.node.called("export")[0][0]
            self.assertEqual((exported, real(output)), (cid, real(os.path.join(self.tmp, "a.rnfs"))))
            self.assertEqual(len(self.ok("import", "a.rnfs").split()), 1)
        self.assertEqual(self.run_cli("import", os.path.join(self.tmp, "missing"))[0], 1)

    def test_no_daemon(self):
        code, _, err = self.run_cli("status", port=free_port())
        self.assertEqual(code, 1)
        self.assertIn("no daemon is running", err)

    def test_sends_api_header_and_host(self):
        # The server would answer 403 otherwise; check the exact header anyway
        seen = []
        original = self.server.server.RequestHandlerClass.dispatch

        def spy(handler):
            seen.append((handler.path, handler.headers["Host"], handler.headers["X-Rnfs-Api"],
                         handler.headers[cli.CHALLENGE_HEADER] is not None))
            original(handler)
        with mock.patch.object(self.server.server.RequestHandlerClass, "dispatch", spy):
            self.ok("gc")
            self.ok("status")
        host = f"127.0.0.1:{self.server.port}"
        self.assertEqual(seen, [("/api/status", host, None, True),  # the daemon proves it knows the token first
                                ("/api/gc", host, self.server.token, False),
                                # status sends it too, after the proof: without it the daemon only says its version
                                ("/api/status", host, None, True),
                                ("/api/status", host, self.server.token, False)])

    def test_printed_fields_are_escaped(self):
        # Whatever the daemon answers is printed with control characters escaped
        evil = "bafy\x1b]0;owned\x07\x1b[2J"
        escaped = cli.printable(evil)
        cid = self.node.put_file(b"abc")
        directory = self.node.put_dir({"a": cid})
        for method, argv, value in (("dir_set", ("dir", "set", directory, "x", cid), {"cid": evil}),
                                    ("dir_rm", ("dir", "rm", f"{directory}/a"), {"cid": evil}),
                                    ("name_create", ("name", "create", "site"), {"alias": "site", "name": evil}),
                                    ("name_resolve", ("name", "resolve", "ab" * 16), {"name": "ab" * 16, "cid": evil}),
                                    ("verify", ("verify",), {"checked": evil, "bad": []})):
            with mock.patch.object(self.node, method, return_value=value):
                out = self.ok(*argv)
            self.assertNotIn("\x1b", out, argv)
            self.assertIn(escaped, out, argv)
        entries = [{"name": "n", "kind": evil, "size": 1, "cid": evil}]
        with mock.patch.object(self.node, "ls", return_value=entries):
            out = self.ok("ls", directory)
        self.assertNotIn("\x1b", out)
        self.assertEqual(out.count(escaped), 2)
        with mock.patch.object(self.node, "verify", return_value={"checked": 1, "bad": [evil]}):
            code, out, _ = self.run_cli("verify")
        self.assertEqual(code, 1)
        self.assertIn(f"bad block deleted: {escaped}", out.decode())

    def test_port_numbers_checked(self):
        for argv in (["--api-port", "99999", "status"], ["status", "--api-port", "0"],
                     ["daemon", "--gateway-port", "65536"], ["status", "--api-port", "x"]):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                cli.build_parser().parse_args(argv)

    def test_bad_settings_fail_cleanly(self):
        # A badly typed setting is a clean error, for client commands and for the daemon (before RNS starts)
        with open(os.path.join(self.tmp, "rnfs.toml"), "w", encoding="utf-8") as f:
            f.write('api_port = 99999\n')
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as raised:
            cli.main(["--dir", self.tmp, "status"])
        self.assertIn("api_port must be a whole number", str(raised.exception.code))
        with open(os.path.join(self.tmp, "rnfs.toml"), "w", encoding="utf-8") as f:
            f.write('private = "no"\n')
        with contextlib.redirect_stderr(stderr), self.assertRaises(SystemExit) as raised:
            cli.main(["--dir", self.tmp, "daemon"])
        self.assertIn("invalid settings: private must be true or false", str(raised.exception.code))

    def test_node_that_cant_start_fails_cleanly(self):
        # An error building the node (e.g. an rnfs dir that isn't private) is a message, not a traceback
        from rnfs.errors import RnfsError
        with mock.patch("rnfs.node.RnfsNode", side_effect=RnfsError("the rnfs dir isn't private")), \
                contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
            cli.main(["--dir", self.tmp, "daemon"])
        self.assertEqual(raised.exception.code, "rnfs: the rnfs dir isn't private")


class TokenTest(CliTestCase):

    @unittest.skipUnless(POSIX_MODES and CAN_SYMLINK, "Unix permissions, symlinks")
    def test_write_token(self):
        path = os.path.join(self.tmp, "t")
        os.symlink(os.path.join(self.tmp, "elsewhere"), path)  # replaced, never written through
        umask = os.umask(0)  # the file is 0600 whatever the umask
        try:
            cli.write_token(path, "first")
        finally:
            os.umask(umask)
        cli.write_token(path, "second")
        self.assertFalse(os.path.islink(path))
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "elsewhere")))
        self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
        with open(path, encoding="ascii") as f:
            self.assertEqual(f.read(), "second\n")
        self.assertEqual(sorted(os.listdir(self.tmp)), ["api_token", "t"])  # no temporary files left

    def test_write_token_replaces(self):
        path = os.path.join(self.tmp, "t")
        cli.write_token(path, "first")
        cli.write_token(path, "second")
        with open(path, encoding="ascii") as f:
            self.assertEqual(f.read().strip(), "second")
        self.assertEqual(sorted(os.listdir(self.tmp)), ["api_token", "t"])  # no temporary files left

    def test_token_mode_not_checked_on_windows(self):
        # Windows' st_mode always shows others allowed, so the check would refuse every token there
        os.chmod(self.token_file, 0o644)
        with mock.patch.object(fsutil, "IS_WINDOWS", True):
            code, _, err = self.run_cli("gc")
        self.assertEqual(code, 0, err)

    def test_remove_token_only_if_ours(self):
        cli.remove_token(self.token_file, "another daemon's")
        self.assertTrue(os.path.exists(self.token_file))
        cli.remove_token(self.token_file, self.server.token)
        self.assertFalse(os.path.exists(self.token_file))
        cli.remove_token(self.token_file, self.server.token)  # already gone: no error

    def test_missing_token(self):
        os.unlink(self.token_file)
        code, _, err = self.run_cli("gc")
        self.assertEqual(code, 1)
        self.assertIn("no API token", err)
        self.assertEqual(self.node.called("gc"), [])
        self.assertEqual(self.run_cli("status")[0], 0)  # health checks still work

    @unittest.skipUnless(POSIX_MODES, "Unix permissions")
    def test_unreadable_token(self):
        os.chmod(self.token_file, 0)
        if os.access(self.token_file, os.R_OK):
            self.skipTest("running as root")
        code, _, err = self.run_cli("gc")
        self.assertEqual(code, 1)
        self.assertIn("another user", err)

    @unittest.skipUnless(POSIX_MODES, "Unix permissions")
    def test_token_readable_by_others_is_refused(self):
        os.chmod(self.token_file, 0o644)
        code, _, err = self.run_cli("gc")
        self.assertEqual(code, 1)
        self.assertIn("accessible to other users", err)
        self.assertEqual(self.node.called("gc"), [])

    def test_stale_token_is_never_sent(self):
        # The daemon can't prove it knows this token, so the CLI doesn't send it
        cli.write_token(self.token_file, "an old daemon's token")
        tokens = []
        original = self.server.server.RequestHandlerClass.dispatch

        def spy(handler):
            tokens.append(handler.headers["X-Rnfs-Api"])
            original(handler)
        with mock.patch.object(self.server.server.RequestHandlerClass, "dispatch", spy):
            code, _, err = self.run_cli("gc")
        self.assertEqual(code, 1)
        self.assertIn("couldn't prove it knows the API token", err)
        self.assertEqual(tokens, [None])
        self.assertEqual(self.node.called("gc"), [])

    def test_impostor_on_the_api_port_gets_neither_token_nor_key(self):
        # Another local user listening on the API port while the daemon is down, answering everything 200
        received = []

        class Impostor(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def answer(self):
                length = int(self.headers.get("Content-Length") or 0)
                received.append((self.path, dict(self.headers), self.rfile.read(length) if length else b""))
                data = b'{"name": "x", "cid": "y", "version": "1"}'
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.send_header(cli.PROOF_HEADER, "00" * 32)  # a guess
                self.end_headers()
                self.wfile.write(data)
            do_GET = do_POST = answer

        impostor = http.server.HTTPServer(("127.0.0.1", 0), Impostor)
        threading.Thread(target=impostor.serve_forever, daemon=True).start()
        self.addCleanup(impostor.server_close)
        self.addCleanup(impostor.shutdown)
        key = self.write("key", b"K" * 64)
        code, out, err = self.run_cli("name", "import", "stolen", key, port=impostor.server_address[1])
        self.assertEqual(code, 1)
        self.assertIn("couldn't prove", err)
        self.assertEqual(len(received), 1)  # only the challenge
        path, headers, body = received[0]
        self.assertTrue(path.startswith("/api/status"))
        self.assertNotIn("X-Rnfs-Api", headers)
        self.assertEqual(body, b"")

    def test_proof_is_bound_to_the_port(self):
        # A program on another port can't pass on the daemon's answer: the proof covers the daemon's port
        proof = cli.api_proof("token", 8788, "ab" * 16)
        self.assertNotEqual(proof, cli.api_proof("token", 8789, "ab" * 16))
        self.assertNotEqual(proof, cli.api_proof("other", 8788, "ab" * 16))
        self.assertNotEqual(proof, cli.api_proof("token", 8788, "cd" * 16))


if __name__ == "__main__":
    unittest.main()
