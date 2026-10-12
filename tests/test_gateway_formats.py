"""Tests for the gateway's trustless responses (?format=raw|archive, rnfs/gateway_formats.py) and sealed files
served as opaque bytes, against an offline RnfsNode."""
import io
import os
import socket
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.content import archive, seal
from rnfs.content.cid import RAW, Cid
from rnfs.content.storage import Storage
from rnfs.errors import NotFound
from rnfs.gateway import CSP_SCRIPTS, GatewayServer
from rnfs.node import RnfsNode

try:
    from tests.test_api import raw_request, read_until_closed
    from tests.test_names import FakeNetwork
except ImportError:  # run from inside tests/
    from test_api import raw_request, read_until_closed
    from test_names import FakeNetwork


class FormatTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        self.config = Config(rnfs_dir=os.path.join(self.tmp.name, "n"), slow="no", gateway_fetches=1,
                             max_output_entries=50)
        self.node = RnfsNode.offline(self.config, bytes([4] * 16), None, FakeNetwork())
        self.node.identity = RNS.Identity()
        self.addCleanup(self.node.stop)
        self.server = GatewayServer(self.node, self.config, 0)
        self.server.start()
        self.addCleanup(self.server.stop)
        tree = os.path.join(self.tmp.name, "tree")
        os.makedirs(os.path.join(tree, "sub"))
        self.big = os.urandom(300_000)
        with open(os.path.join(tree, "sub", "big.bin"), "wb") as f:
            f.write(self.big)
        with open(os.path.join(tree, "a.txt"), "wb") as f:
            f.write(b"hello")
        self.root = self.node.add_path(tree)["cid"]

    def get(self, path, headers=None, method="GET"):
        """(status, headers, body) of a gateway request."""
        return raw_request(self.server.port, method, path, None, headers)

    def assert_common(self, headers):
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(headers["Content-Security-Policy"], CSP_SCRIPTS)

    def test_raw_block(self):
        status, headers, body = self.get(f"/rnfs/{self.root}?format=raw")
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "application/vnd.rnfs.raw")
        self.assertEqual(body, self.node.storage.get(Cid.parse(self.root)))
        self.assertTrue(Cid.parse(self.root).verify(body))
        self.assertEqual(headers["ETag"], f'"{self.root}.raw"')
        self.assertIn("immutable", headers["Cache-Control"])
        self.assert_common(headers)
        status, headers, _ = self.get(f"/rnfs/{self.root}?format=raw", {"If-None-Match": f'"{self.root}.raw"'})
        self.assertEqual(status, 304)
        self.assertEqual(headers["ETag"], f'"{self.root}.raw"')
        # The plain ETag doesn't revalidate the raw form
        self.assertEqual(self.get(f"/rnfs/{self.root}?format=raw", {"If-None-Match": f'"{self.root}"'})[0], 200)

    def test_raw_refusals(self):
        self.assertEqual(self.get(f"/rnfs/{self.root}/a.txt?format=raw")[0], 400)
        self.assertEqual(self.get(f"/rnfs/{self.root}?format=car")[0], 400)
        self.assertEqual(self.get(f"/name/{'00' * 16}?format=raw")[0], 400)
        status, headers, _ = self.get("/rnfs/bnotacid?format=raw")
        self.assertEqual(status, 400)
        self.assert_common(headers)

    def test_archive(self):
        status, headers, body = self.get(f"/rnfs/{self.root}?format=archive")
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "application/vnd.rnfs.archive")
        self.assertEqual(headers["Content-Disposition"], f'attachment; filename="{self.root}.rnfs-archive"')
        self.assertEqual(headers["ETag"], f'"{self.root}.archive"')
        self.assertEqual(int(headers["Content-Length"]), len(body))
        self.assert_common(headers)
        # Byte-identical to `rnfs export`, and importable elsewhere
        exported = os.path.join(self.tmp.name, "exported")
        self.node.export(self.root, exported)
        with open(exported, "rb") as f:
            self.assertEqual(f.read(), body)
        other = Storage(os.path.join(self.tmp.name, "o", "blocks"), os.path.join(self.tmp.name, "o", "state"), 10**9)
        stats = archive.import_archive(io.BytesIO(body), other)
        self.assertEqual([str(cid) for cid in stats["roots"]], [self.root])

    def test_archive_of_a_path_and_head(self):
        target = self.node.info(f"{self.root}/sub")["cid"]
        status, headers, body = self.get(f"/rnfs/{self.root}/sub?format=archive")
        self.assertEqual(status, 200)
        self.assertEqual(headers["ETag"], f'"{target}.archive"')
        other = Storage(os.path.join(self.tmp.name, "o", "blocks"), os.path.join(self.tmp.name, "o", "state"), 10**9)
        self.assertEqual([str(c) for c in archive.import_archive(io.BytesIO(body), other)["roots"]], [target])
        status, head_headers, head_body = self.get(f"/rnfs/{self.root}/sub?format=archive", method="HEAD")
        self.assertEqual((status, head_body), (200, b""))
        self.assertEqual(head_headers["Content-Length"], headers["Content-Length"])
        self.assertEqual(self.get(f"/rnfs/{self.root}/sub?format=archive",
                                  {"If-None-Match": f'"{target}.archive"'})[0], 304)

    def test_archive_limits(self):
        many = os.path.join(self.tmp.name, "many")
        os.mkdir(many)
        for i in range(60):
            open(os.path.join(many, f"f{i}"), "wb").close()
        cid = self.node.add_path(many)["cid"]
        status, headers, _ = self.get(f"/rnfs/{cid}?format=archive")
        self.assertEqual(status, 413)  # max_output_entries
        self.assert_common(headers)

    def test_cors_rule(self):
        own = {"Referer": f"http://127.0.0.1:{self.server.port}/rnfs/{self.root}/"}
        for fmt in ("raw", "archive"):
            self.assertEqual(self.get(f"/rnfs/{self.root}?format={fmt}", own)[1]["Access-Control-Allow-Origin"], "*")
            self.assertIsNone(self.get(f"/rnfs/{self.root}?format={fmt}",
                                       {"Referer": "http://example.com/"})[1]["Access-Control-Allow-Origin"])

    def test_gateway_fetches_limit(self):
        missing = str(Cid.for_block(RAW, b"not stored anywhere"))
        limiter = self.server.server.RequestHandlerClass.limiter
        self.assertTrue(limiter.try_acquire())  # the only slot is busy
        try:
            status, headers, _ = self.get(f"/rnfs/{missing}?format=raw", {"Sec-Fetch-Site": "cross-site"})
            self.assertEqual(status, 503)
            self.assert_common(headers)
            # Stored content is served without limit
            self.assertEqual(self.get(f"/rnfs/{self.root}?format=raw", {"Sec-Fetch-Site": "cross-site"})[0], 200)
        finally:
            limiter.release()

    def test_archives_of_unstored_content_need_a_users_get(self):
        """A web page's request or a HEAD gets an archive only of content stored completely: anything else
        would fetch a whole DAG for nobody (409, with the common headers); the user's own GET still fetches."""
        missing = str(Cid.for_block(RAW, b"not stored anywhere"))
        calls = []
        real = self.node.export_stream
        self.node.export_stream = lambda *args, **kwargs: calls.append(args) or real(*args, **kwargs)
        for headers, method in (({"Sec-Fetch-Site": "cross-site"}, "GET"), ({"Sec-Fetch-Site": "same-origin"}, "HEAD"),
                                ({}, "HEAD"), ({"Sec-Fetch-Site": "none"}, "HEAD")):
            status, response, body = self.get(f"/rnfs/{missing}?format=archive", headers, method)
            self.assertEqual(status, 409, (headers, method))
            self.assert_common(response)
        self.assertEqual(calls, [])
        self.assertIn(b"stored", self.get(f"/rnfs/{missing}?format=archive", {"Sec-Fetch-Site": "cross-site"})[2])
        # The user's own GET tries the fetch; stored content is served to anyone, HEAD too
        def not_found(*args, **kwargs):
            """The fetch finds no provider (this node has no network)."""
            calls.append(args)
            raise NotFound("no provider found")

        self.node.export_stream = not_found
        self.assertEqual(self.get(f"/rnfs/{missing}?format=archive", {"Sec-Fetch-Site": "none"})[0], 404)
        self.assertEqual(len(calls), 1)
        self.node.export_stream = real
        for headers, method in (({"Sec-Fetch-Site": "cross-site"}, "GET"), ({"Sec-Fetch-Site": "cross-site"}, "HEAD")):
            self.assertEqual(self.get(f"/rnfs/{self.root}?format=archive", headers, method)[0], 200)

    def test_archive_fetch_is_cancelled_when_the_client_leaves(self):
        """The fetch behind a user's archive GET runs under an observer that cancels it once the client is gone."""
        import socket
        import threading
        import time
        from rnfs import fetcher
        from rnfs.gateway_formats import ClientWatch

        outcome = {}
        entered = threading.Event()

        def export_stream(target, start, for_gateway=False):
            """Stand in for a long fetch: wait until the observer this thread runs under cancels it."""
            observer = fetcher._observers.current
            entered.set()
            deadline = time.monotonic() + 5
            while not observer.cancelled():
                if time.monotonic() > deadline:
                    outcome["result"] = "never cancelled"
                    return {}
                time.sleep(0.02)
            outcome["result"] = "cancelled"
            raise fetcher.Cancelled("client gone")

        self.node.export_stream = export_stream
        self.node.is_local = lambda target: False
        sock = socket.create_connection(("127.0.0.1", self.server.port))
        sock.sendall(f"GET /rnfs/{self.root}?format=archive HTTP/1.1\r\nHost: 127.0.0.1:{self.server.port}\r\n\r\n"
                     .encode())
        self.assertTrue(entered.wait(5))
        time.sleep(0.2)
        self.assertNotIn("result", outcome)  # still connected: not cancelled
        sock.close()
        deadline = time.monotonic() + 5
        while "result" not in outcome and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertEqual(outcome.get("result"), "cancelled")
        # The watch itself: open, a pipelined byte, then closed by the peer
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        watch = ClientWatch(left)
        self.assertFalse(watch.cancelled())
        right.sendall(b"G")
        self.assertFalse(watch.cancelled())
        right.close()
        left.recv(1)
        self.assertTrue(watch.cancelled())

    def test_client_leaving_mid_archive_is_not_an_error(self):
        # A web page can abort archive downloads at will: a write failing once the headers are out isn't logged,
        # unless the client is still there (then it is an error of the node's, e.g. reading a block)
        def export_stream(target, start, for_gateway=False):
            """Send the headers and part of the body, then fail as a write to a closed connection does."""
            start(1000).write(b"x" * 10)
            raise BrokenPipeError(32, "Broken pipe")

        self.node.export_stream = export_stream
        for gone, logged in ((True, False), (False, True)):
            with mock.patch("rnfs.gateway_formats.client_gone", return_value=gone), \
                    mock.patch("rnfs.gateway.log") as log:
                with socket.create_connection(("127.0.0.1", self.server.port), timeout=10) as sock:
                    sock.sendall(f"GET /rnfs/{self.root}?format=archive HTTP/1.1\r\n"
                                 f"Host: 127.0.0.1:{self.server.port}\r\n\r\n".encode())
                    data, _ = read_until_closed(sock)
            self.assertTrue(data.startswith(b"HTTP/1.0 200 ") and b"Content-Length: 1000\r\n" in data, data[:200])
            self.assertTrue(data.endswith(b"\r\n\r\n" + b"x" * 10))  # cut short, as nothing else can be said
            self.assertEqual(log.called, logged, gone)

    def test_client_watch_works_beyond_fd_1024(self):
        # select.select() can't take a file descriptor of 1024 or more; poll() can
        import resource
        from rnfs.gateway_formats import client_gone
        if not hasattr(os, "dup2") or not hasattr(resource, "RLIMIT_NOFILE"):
            self.skipTest("no fd limits here")
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if hard != resource.RLIM_INFINITY and hard < 1200:
            self.skipTest("can't open a file descriptor over 1024")
        if soft != resource.RLIM_INFINITY and soft < 1200:
            resource.setrlimit(resource.RLIMIT_NOFILE, (1200, hard))
            self.addCleanup(resource.setrlimit, resource.RLIMIT_NOFILE, (soft, hard))
        left, right = socket.socketpair()
        self.addCleanup(right.close)
        high = os.dup2(left.fileno(), 1100)
        left.close()
        sock = socket.socket(fileno=high)
        self.addCleanup(sock.close)
        self.assertFalse(client_gone(sock))
        right.close()
        self.assertTrue(client_gone(sock))

    def test_head_and_later_ranges_fetch_no_first_chunk(self):
        """A HEAD, or a range not covering byte 0, doesn't fetch the first chunk just to sniff or check for a
        sealed file: it uses it only if stored (else the name's type, or application/octet-stream)."""
        from rnfs.content.cid import RAW as RAW_CODEC

        target = f"{self.root}/sub/big.bin"
        file_cid = Cid.parse(self.node.info(target)["cid"])
        storage = self.node.storage
        first = next(cid for cid in storage.dag_blocks(file_cid)
                     if cid.codec == RAW_CODEC and self.big.startswith(storage.get(cid)))
        real_get, real_size = storage.get, storage.block_size
        # The first chunk isn't stored (evicted, or never fetched); the node has no network to fetch it with
        storage.get = lambda cid, verify=False: None if cid == first else real_get(cid, verify)
        storage.block_size = lambda cid: None if cid == first else real_size(cid)
        self.addCleanup(setattr, storage, "get", real_get)
        self.addCleanup(setattr, storage, "block_size", real_size)
        status, headers, body = self.get(f"/rnfs/{target}", method="HEAD")
        self.assertEqual((status, body), (200, b""))
        self.assertEqual(headers["Content-Length"], str(len(self.big)))
        status, headers, body = self.get(f"/rnfs/{target}?filename=x.txt", {"Range": "bytes=-100"})
        self.assertEqual((status, body), (206, self.big[-100:]))
        self.assertEqual(headers["Content-Type"], "text/plain")
        self.assertEqual(self.get(f"/rnfs/{target}", {"Range": "bytes=-100"})[1]["Content-Type"],
                         "application/octet-stream")
        # Stored first bytes are still used: a sealed file stays opaque on a HEAD and a later range too
        sealed = self.node.add_stream(io.BytesIO(b"<html>" + bytes(200_000)), to_self=True)["cid"]
        for headers, method in (({}, "HEAD"), ({"Range": "bytes=-10"}, "GET")):
            response = self.get(f"/rnfs/{sealed}?filename=page.html", headers, method)[1]
            self.assertEqual(response["Content-Type"], "application/octet-stream", method)

    def test_sealed_files_are_opaque(self):
        data = b"<html><script>alert(1)</script></html>"
        cid = self.node.add_stream(io.BytesIO(data), to_self=True)["cid"]
        self.assertEqual(b"".join(self.node.read(cid)), data)  # the node itself can open it
        status, headers, body = self.get(f"/rnfs/{cid}?filename=page.html")
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "application/octet-stream")
        self.assertTrue(seal.is_sealed(body))
        self.assertNotIn(data, body)


if __name__ == "__main__":
    unittest.main()
