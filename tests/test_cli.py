"""Tests for the command line (rnfs/cli.py): parsing, and each command against a live ApiServer with a FakeNode."""
import contextlib
import http.server
import io
import json
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
from tests.test_fsutil import hold_lock

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

    def test_pin_label_and_cache(self):
        """pin label sets and clears a label; cache ls lists downloads (names escaped), cache rm evicts them."""
        cid = self.node.put_file(b"abc")
        self.ok("pin", "add", cid)
        self.assertEqual(self.ok("pin", "label", cid, "my file"), f"{cid}  my file\n")
        self.assertEqual(self.ok("pin", "label", cid), f"{cid}: label cleared\n")
        self.assertEqual([args for args, _ in self.node.called("pin_label")], [(cid, "my file"), (cid, None)])
        self.node.cached.add(cid)
        self.assertEqual(self.ok("cache", "ls"), f"{cid} 1 B\n")
        self.assertEqual(json.loads(self.ok("cache", "ls", "--json"))[0]["cid"], cid)
        self.assertEqual(self.ok("cache", "rm", cid), f"{cid}: removed 1 blocks, freed 1 bytes\n")
        code, _, err = self.run_cli("cache", "rm", cid)
        self.assertEqual((code, err.strip()), (1, f"rnfs: {cid} is not in the cache"))

    def test_url(self):
        """url prints a target's site URL, from the base32 CID or a name, for the running daemon's gateway port
        or else rnfs.toml's; nicks and invalid targets are refused. It never calls the daemon."""
        from rnfs.content.cid import to_base36
        raw = str(Cid.for_block(RAW, b"x"))
        node = self.node.put_dir({"a b": raw})
        label, dag_label = to_base36(Cid.parse(raw)), to_base36(Cid.parse(node))
        self.assertEqual(self.ok("url", raw), f"http://{label}.rnfs.localhost:8789/\n")
        self.assertEqual(self.ok("url", f"{node}/a b/c?#%"),
                         f"http://{dag_label}.rnfs.localhost:8789/a%20b/c%3F%23%25\n")
        self.assertEqual(self.ok("url", f"{node}/sub/"), f"http://{dag_label}.rnfs.localhost:8789/sub/\n")
        self.assertEqual(self.ok("url", "AB" * 16 + "/index.html"),
                         f"http://{'ab' * 16}.name.localhost:8789/index.html\n")
        for target in ("mysite", "b", label, f"{raw}//x", "ab" * 15):
            code, out, err = self.run_cli("url", target)
            self.assertEqual((code, out), (1, b""), target)
            self.assertTrue(err.startswith("rnfs: "), err)
        # rnfs.toml's port, then a running daemon's (daemon.json)
        with open(os.path.join(self.tmp, "rnfs.toml"), "w") as f:
            f.write("gateway_port = 9000\n")
        self.assertEqual(self.ok("url", raw), f"http://{label}.rnfs.localhost:9000/\n")
        cli.write_daemon_info(os.path.join(self.tmp, "daemon.json"), {"pid": os.getpid(), "api_port": 9001,
                                                                       "gateway_port": 9002})
        self.assertEqual(self.ok("url", raw), f"http://{label}.rnfs.localhost:9002/\n")
        os.remove(os.path.join(self.tmp, "daemon.json"))
        with open(os.path.join(self.tmp, "rnfs.toml"), "w") as f:
            f.write("gateway_port = 0\n")
        code, _, err = self.run_cli("url", raw)
        self.assertEqual(code, 1)
        self.assertIn("gateway_port is 0", err)
        self.assertEqual(self.node.calls, [])

    def test_stop(self):
        """stop asks the daemon to stop and waits until its API no longer answers; a daemon that can't be
        stopped through its API is reported."""
        code, _, err = self.run_cli("stop", "--wait", "1")
        self.assertEqual(code, 1)
        self.assertIn("can't be stopped through its API", err)
        stopping = threading.Event()
        server = ApiServer(self.node, 0, token=self.server.token, shutdown=stopping.set)
        server.start()
        self.addCleanup(server.stop)
        threading.Thread(target=lambda: stopping.wait(10) and server.stop(), daemon=True).start()
        with mock.patch("rnfs.api.log"):  # the daemon's own log line, which goes to stdout here
            self.assertEqual(self.ok("stop", port=server.port), "rnfs: the daemon stopped\n")

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

    def test_remote_pin_status(self):
        """pin remote ls lists the pins asked for (filtered, --check passed on, --json), --served those served and
        each identity's usage."""
        cid = self.node.put_file(b"abc")
        out = self.ok("pin", "remote", "ls")
        self.assertRegex(out, rf"^{'ab' * 16}  bafkreie  waiting  asked \d+d ago\n$")
        out = self.ok("pin", "remote", "ls", "ab" * 16, cid, "--check")
        self.assertRegex(out, rf"^{'ab' * 16}  {cid}  done  asked \d+d ago, checked \d+d ago\n$")
        self.assertEqual(self.node.called("pin_remote_ls"),
                         [((None, None), {}), ((bytes.fromhex("ab" * 16), cid), {"check": True})])
        self.assertEqual(json.loads(self.ok("pin", "remote", "ls", "--json"))[0]["state"], "waiting")
        out = self.ok("pin", "remote", "ls", "--served").splitlines()
        self.assertEqual(out[0], f"{'cd' * 16}: 5 B of 100 B used, 1 pinned, 0 waiting")
        self.assertRegex(out[1], rf"^{'cd' * 16}  bafkreie  dropped \(quota\)  \d+d ago: too big$")
        self.assertEqual(json.loads(self.ok("pin", "remote", "ls", "--served", "--json"))["quota"], 100)
        self.assertEqual(self.run_cli("pin", "remote", "ls", "--served", "--check")[0], 1)

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

    def test_daemon_starting(self):
        # The daemon locks its rnfs dir before opening the block store, and its API listens only after
        holder = hold_lock(os.path.join(self.tmp, "daemon.lock"))
        try:
            for command in (("status",), ("pin", "ls")):
                with self.subTest(command=command):
                    code, _, err = self.run_cli(*command, port=free_port())
                    self.assertEqual(code, 1)
                    self.assertIn(f"the daemon (process {holder.pid}) is starting", err)
                    self.assertNotIn("no daemon is running", err)
        finally:
            holder.stdin.close()
            holder.wait(30)
            holder.stdout.close()

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
        """An error building the node (e.g. an rnfs dir that isn't private) is a message, not a traceback, and
        the daemon.lock is let go."""
        from rnfs.errors import RnfsError
        with mock.patch("rnfs.node.RnfsNode", side_effect=RnfsError("the rnfs dir isn't private")), \
                contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
            cli.main(["--dir", self.tmp, "daemon"])
        self.assertEqual(raised.exception.code, "rnfs: the rnfs dir isn't private")
        self.assert_unlocked()

    def test_debug_log_that_cant_open_fails_cleanly(self):
        """A debug log that can't be written is a message, and the daemon.lock taken before it is let go."""
        with mock.patch("rnfs.debuglog.start", side_effect=OSError("disk full")), \
                contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
            cli.main(["--dir", self.tmp, "daemon", "--debug-log"])
        self.assertEqual(raised.exception.code, "rnfs: can't write the debug log: disk full")
        self.assert_unlocked()

    def assert_unlocked(self):
        """The failed daemon closed its daemon.lock: this process can lock it again (an open descriptor would
        hold the lock, and on Windows keep the temp dir from being removed)."""
        fd = fsutil.lock_exclusive(os.path.join(self.tmp, cli.LOCK_FILE))
        os.close(fd)


class AppCommandTest(CliTestCase):
    """rnfs app add, ls and rm."""

    def setUp(self):
        """The FakeNode gets a real app registry."""
        super().setUp()
        from rnfs.apps import AppRegistry
        self.node.apps = AppRegistry(os.path.join(self.tmp, "state"))
        self.addCleanup(self.node.apps.close)
        quiet = mock.patch("rnfs.apps.log")  # the daemon's own log lines, which go to stdout here
        quiet.start()
        self.addCleanup(quiet.stop)

    def test_add_prints_the_token_once(self):
        """app add prints only the token on stdout (for scripts) and on stderr that the app can do anything the
        CLI can but manage apps; the token works as the app's."""
        code, out, err = self.run_cli("app", "add", "wiki")
        self.assertEqual(code, 0, err)
        token = out.decode().strip()
        self.assertEqual(out.decode(), token + "\n")
        self.assertTrue(self.node.apps.verify("wiki", token))
        self.assertIn("can do anything this command line can", err)
        self.assertIn("except manage apps", err)
        self.assertIn("rnfs app rm wiki", err)

    def test_add_writes_the_token_to_a_new_private_file(self):
        """-o writes the token to a new file only this user can read, which read_token accepts; an existing one is
        refused before any app is added."""
        from rnfs import apiclient
        path = os.path.join(self.tmp, "wiki_token")
        out = self.ok("app", "add", "wiki", "-o", path)
        self.assertEqual(out, f"app wiki: its token is in {path}\n")
        token = apiclient.read_token(path, "wiki")
        self.assertTrue(self.node.apps.verify("wiki", token))
        if POSIX_MODES:
            self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), 0o600)
        code, _, err = self.run_cli("app", "add", "notes", "-o", path)
        self.assertEqual((code, err.strip()), (1, f"rnfs: {path} already exists"))
        self.assertNotIn("notes", self.node.apps)

    def test_unwritable_output_leaves_no_app(self):
        """An -o that can't be written removes the app again: nobody would have its token."""
        path = os.path.join(self.tmp, "no such dir", "token")
        code, _, err = self.run_cli("app", "add", "wiki", "-o", path)
        self.assertEqual(code, 1)
        self.assertIn("app wiki was not added", err)
        self.assertEqual(self.node.apps.list(), [])

    def test_ls_and_rm(self):
        """app ls lists names and when added (--json too); app rm revokes one; errors are the daemon's."""
        self.node.apps.add("wiki")
        self.node.apps.add("notes")
        lines = self.ok("app", "ls").splitlines()
        self.assertEqual([line.split("\t")[0] for line in lines], ["notes", "wiki"])
        self.assertEqual([app["name"] for app in json.loads(self.ok("app", "ls", "--json"))], ["notes", "wiki"])
        self.assertEqual(self.ok("app", "rm", "wiki"), "removed app wiki, releasing 0 pins\n")
        code, _, err = self.run_cli("app", "rm", "wiki")
        self.assertEqual((code, err.strip()), (1, "rnfs: no app called wiki"))
        code, _, err = self.run_cli("app", "add", "Bad")
        self.assertEqual(code, 1)
        self.assertIn("invalid app name", err)


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



class DaemonLockTest(unittest.TestCase):
    """A second daemon on an rnfs dir another daemon holds (its daemon.lock) stops before touching anything."""

    def test_second_daemon_stops_at_once(self):
        """With the lock held, `rnfs daemon` exits with an error naming the holder, before writing state or
        rotating the debug log."""
        from rnfs.node import _make_private_dirs
        from tests.test_fsutil import hold_lock

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        rnfs_dir = os.path.join(tmp.name, "rnfs")
        _make_private_dirs(rnfs_dir)  # as the first daemon left it
        debug_log = os.path.join(rnfs_dir, "debug.log")
        with open(debug_log, "w", encoding="utf-8") as f:
            f.write("the first daemon's log\n")
        holder = hold_lock(os.path.join(rnfs_dir, cli.LOCK_FILE))
        try:
            result = subprocess.run([sys.executable, "rnfs.py", "--dir", rnfs_dir, "--api-port", str(free_port()),
                                     "daemon", "--gateway-port", str(free_port()), "--debug-log"],
                                    cwd=ROOT, capture_output=True, timeout=60, encoding="utf-8", errors="replace")
        finally:
            holder.stdin.close()
            holder.wait(30)
            holder.stdout.close()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn(f"another rnfs daemon (process {holder.pid}) is using", result.stderr)
        with open(debug_log, encoding="utf-8") as f:
            self.assertEqual(f.read(), "the first daemon's log\n")
        self.assertFalse(os.path.exists(debug_log + ".1"))
        self.assertFalse(os.path.exists(os.path.join(rnfs_dir, "state", "rnfs.db")))


if __name__ == "__main__":
    unittest.main()
