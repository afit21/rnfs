"""Tests for the command line (rnfs/cli.py): parsing, and each command against a live ApiServer with a FakeNode."""
import contextlib
import io
import os
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from rnfs import cli
from rnfs.api import ApiServer
from rnfs.content.cid import RAW, Cid

try:
    from tests.test_api import FakeNode
except ImportError:  # run from inside tests/
    from test_api import FakeNode

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port() -> int:
    """A local port nothing is listening on."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class CliTestCase(unittest.TestCase):
    """Base: a FakeNode behind an ApiServer, and run() to call cli.main capturing output."""

    def setUp(self):
        self.node = FakeNode()
        self.server = ApiServer(self.node, 0)
        self.server.start()
        self.addCleanup(self.server.stop)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name

    def run_cli(self, *argv, stdin: bytes = b"", port=None, tty=False):
        """Run cli.main(argv) with --api-port; returns (exit code, stdout bytes, stderr text)."""
        stdout_bytes = io.BytesIO()
        stdout = io.TextIOWrapper(stdout_bytes, encoding="utf-8", write_through=True)
        stdout.isatty = lambda: tty
        stdin_stream = io.TextIOWrapper(io.BytesIO(stdin), encoding="utf-8")
        stderr = io.StringIO()
        code = 0
        with mock.patch("sys.stdout", stdout), mock.patch("sys.stdin", stdin_stream), \
                contextlib.redirect_stderr(stderr):
            try:
                cli.main([*argv, "--api-port", str(port or self.server.port)])
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

    def test_printable(self):
        self.assertEqual(cli.printable("ok name"), "ok name")
        self.assertEqual(cli.printable("evil\x1b[2Jname\n"), "evil\\x1b[2Jname\\n")

    def test_client_commands_do_not_import_rns(self):
        code = ("import sys; from rnfs import cli; cli.build_parser().parse_args(['status']); "
                "print('RNS' in sys.modules)")
        result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.stdout.strip(), "False", result.stderr)

    def test_entry_points(self):
        for argv in ([sys.executable, "main.py", "--help"], [sys.executable, "-m", "rnfs", "--help"]):
            result = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("daemon", result.stdout)


class ContentCommandTest(CliTestCase):

    def test_add_path(self):
        path = self.write("f.txt", b"hello")
        cid = str(Cid.for_block(RAW, b"hello"))
        with contextlib.chdir(self.tmp):
            self.assertEqual(self.ok("add", "f.txt"), cid + "\n")
        self.assertEqual(self.node.called("add_path")[0], ((path,), {"wrap": False, "only_hash": False}))
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
        self.assertEqual(self.node.called("get")[1], ((directory, os.path.join(os.path.realpath(self.tmp), directory)),
                                                      {"from_node": bytes.fromhex("ab" * 16)}))
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
        self.assertEqual(os.stat(key_file).st_mode & 0o777, 0o600)
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
            self.assertEqual(self.node.called("export")[0][0], (cid, os.path.join(os.path.realpath(self.tmp),
                                                                                  "a.rnfs")))
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
            seen.append((handler.headers["Host"], handler.headers["X-Rnfs-Api"]))
            original(handler)
        with mock.patch.object(self.server.server.RequestHandlerClass, "dispatch", spy):
            self.ok("gc")
        self.assertEqual(seen, [(f"127.0.0.1:{self.server.port}", "1")])


if __name__ == "__main__":
    unittest.main()
