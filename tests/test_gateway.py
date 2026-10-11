"""Tests for the gateway (rnfs/gateway.py), run against the in-memory FakeNode with no RNS."""
import socket
import threading
import time
import unittest
from unittest import mock

from rnfs.config import Config
from rnfs.errors import NotFound
from rnfs.gateway import (CSP_NO_SCRIPTS, CSP_SCRIPTS, IMMUTABLE, GatewayServer, etag_matches, listing_html,
                          parse_range, sniff)

try:
    from tests.test_api import FakeNode, held_connections, raw_request, read_until_closed
except ImportError:  # run from inside tests/
    from test_api import FakeNode, held_connections, raw_request, read_until_closed

SPEC_CSP = ("sandbox allow-scripts allow-forms allow-popups; default-src 'self' 'unsafe-inline' 'unsafe-eval' "
            "data: blob:; form-action 'self'")
PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 20


class GatewayTestCase(unittest.TestCase):
    """Base: a GatewayServer on a free port for a FakeNode with a small DAG."""

    config_overrides: dict = {}

    def setUp(self):
        self.node = FakeNode()
        self.config = Config(gateway_fetches=2, **self.config_overrides)
        self.start()
        self.text = self.node.put_file(b"hello gateway")
        self.page = self.node.put_file(b"<p>index</p>")
        self.sub = self.node.put_dir({"index.html": self.page, "a b.txt": self.text})
        self.root = self.node.put_dir({"<script>alert(1)</script>": self.text, "sub": self.sub,
                                       "javascript:x": self.text, "data.bin": self.text, "pic": self.node.put_file(PNG)})

    def start(self):
        """(Re)start the gateway with self.config."""
        if hasattr(self, "server"):
            self.server.stop()
        self.server = GatewayServer(self.node, self.config, 0)
        self.server.start()
        self.addCleanup(self.server.stop)
        self.port = self.server.port

    def get(self, path, headers=None, method="GET", host="default"):
        """(status, headers, body) of a gateway request."""
        return raw_request(self.port, method, path, None, headers, host)

    def assert_common_headers(self, headers, csp=SPEC_CSP):
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(headers["Content-Security-Policy"], csp)


class HeaderTest(GatewayTestCase):

    def test_spec_csp_constant(self):
        self.assertEqual(CSP_SCRIPTS, SPEC_CSP)

    def test_every_response_carries_csp_and_nosniff(self):
        responses = {
            "file": self.get(f"/rnfs/{self.text}"),
            "listing": self.get(f"/rnfs/{self.root}/"),
            "redirect": self.get(f"/rnfs/{self.root}"),
            "not modified": self.get(f"/rnfs/{self.text}", {"If-None-Match": f'"{self.text}"'}),
            "range": self.get(f"/rnfs/{self.text}", {"Range": "bytes=0-1"}),
            "unsatisfiable": self.get(f"/rnfs/{self.text}", {"Range": "bytes=999-"}),
            "bad cid": self.get("/rnfs/nope"),
            "missing": self.get(f"/rnfs/{self.root}/missing"),
            "unknown path": self.get("/elsewhere"),
            "bad host": self.get(f"/rnfs/{self.text}", host="evil.example"),
            "post": self.get(f"/rnfs/{self.text}", method="POST"),
            "head": self.get(f"/rnfs/{self.text}", method="HEAD"),
        }
        expected = {"file": 200, "listing": 200, "redirect": 301, "not modified": 304, "range": 206,
                    "unsatisfiable": 416, "bad cid": 400, "missing": 404, "unknown path": 404, "bad host": 403,
                    "post": 405, "head": 200}
        for name, (status, headers, _) in responses.items():
            self.assertEqual(status, expected[name], name)
            self.assert_common_headers(headers)

    def test_host_check(self):
        for host in (f"127.0.0.1:{self.port}", f"localhost:{self.port}"):
            self.assertEqual(self.get(f"/rnfs/{self.text}", host=host)[0], 200)
        self.node.calls.clear()
        for host in ("evil.example", f"evil.example:{self.port}", f"127.0.0.1:{self.port + 1}", None):
            self.assertEqual(self.get(f"/rnfs/{self.text}", host=host)[0], 403, host)
        self.assertEqual(self.node.called("info"), [])

    def test_cors_only_for_own_referer(self):
        path = f"/rnfs/{self.text}"
        for referer in (f"http://127.0.0.1:{self.port}/rnfs/{self.root}/page.html", f"http://localhost:{self.port}/"):
            for method in ("GET", "HEAD"):
                headers = self.get(path, {"Referer": referer}, method=method)[1]
                self.assertEqual(headers["Access-Control-Allow-Origin"], "*", referer)
            # Errors too, so a page can tell a missing file from a blocked one
            self.assertEqual(self.get(f"/rnfs/{self.root}/missing", {"Referer": referer})[1]
                             ["Access-Control-Allow-Origin"], "*")
        for referer in (None, "http://evil.example/", f"http://127.0.0.1:{self.port + 1}/",
                        f"https://127.0.0.1:{self.port}/", f"http://127.0.0.1:{self.port}.evil.example/", "garbage"):
            headers = self.get(path, {"Referer": referer} if referer else {})[1]
            self.assertNotIn("Access-Control-Allow-Origin", headers, referer)
        self.assertEqual(self.get(path)[1]["Vary"], "Referer")

    def test_scripts_off(self):
        self.config.gateway_scripts = False
        self.start()
        headers = self.get(f"/rnfs/{self.text}")[1]
        self.assert_common_headers(headers, CSP_NO_SCRIPTS)
        csp = headers["Content-Security-Policy"]
        self.assertNotIn("allow-scripts", csp)
        self.assertNotIn("unsafe-eval", csp)
        self.assertIn("script-src 'none'", csp)
        self.assertTrue(csp.startswith("sandbox allow-forms allow-popups;"))


class PrivateModeTest(GatewayTestCase):
    config_overrides = {"private": True}

    def test_never_cors_in_private_mode(self):
        headers = self.get(f"/rnfs/{self.text}", {"Referer": f"http://127.0.0.1:{self.port}/"})[1]
        self.assertNotIn("Access-Control-Allow-Origin", headers)


class FileTest(GatewayTestCase):

    def test_file_with_cache_headers(self):
        status, headers, body = self.get(f"/rnfs/{self.text}")
        self.assertEqual((status, body), (200, b"hello gateway"))
        self.assertEqual(headers["ETag"], f'"{self.text}"')
        self.assertEqual(headers["Cache-Control"], IMMUTABLE)
        self.assertIn("immutable", IMMUTABLE)
        self.assertEqual(headers["Accept-Ranges"], "bytes")
        self.assertEqual(headers["Content-Length"], "13")
        self.assertEqual(headers["Content-Type"], "text/plain; charset=utf-8")  # sniffed

    def test_etag_is_the_target(self):
        headers = self.get(f"/rnfs/{self.root}/sub/a%20b.txt")[1]
        self.assertEqual(headers["ETag"], f'"{self.text}"')

    def test_if_none_match(self):
        for header in (f'"{self.text}"', f'W/"{self.text}"', f'"other", "{self.text}"', "*"):
            status, headers, body = self.get(f"/rnfs/{self.text}", {"If-None-Match": header})
            self.assertEqual((status, body), (304, b""), header)
            self.assertEqual(headers["ETag"], f'"{self.text}"')
        # A bare CID is answered without asking the node anything
        self.assertEqual(self.node.called("info"), [])
        self.assertEqual(self.get(f"/rnfs/{self.root}/data.bin", {"If-None-Match": f'"{self.text}"'})[0], 304)
        self.assertEqual(self.get(f"/rnfs/{self.text}", {"If-None-Match": '"other"'})[0], 200)

    def test_content_type(self):
        self.assertEqual(self.get(f"/rnfs/{self.text}?filename=x.html")[1]["Content-Type"], "text/html")
        self.assertIn("x.html", self.get(f"/rnfs/{self.text}?filename=x.html")[1]["Content-Disposition"])
        self.assertEqual(self.get(f"/rnfs/{self.root}/pic")[1]["Content-Type"], "image/png")  # sniffed
        self.assertEqual(self.get(f"/rnfs/{self.root}/sub/a%20b.txt")[1]["Content-Type"], "text/plain")
        # ?filename= wins over the path's name
        self.assertEqual(self.get(f"/rnfs/{self.root}/data.bin?filename=a.json")[1]["Content-Type"],
                         "application/json")

    def test_ranges(self):
        path = f"/rnfs/{self.text}"  # b"hello gateway", 13 bytes
        for header, body, content_range in (("bytes=0-4", b"hello", "bytes 0-4/13"),
                                            ("bytes=6-", b"gateway", "bytes 6-12/13"),
                                            ("bytes=-3", b"way", "bytes 10-12/13"),
                                            ("bytes=10-100", b"way", "bytes 10-12/13"),
                                            ("bytes=-100", b"hello gateway", "bytes 0-12/13")):
            status, headers, data = self.get(path, {"Range": header})
            self.assertEqual((status, data), (206, body), header)
            self.assertEqual(headers["Content-Range"], content_range)
            self.assertEqual(headers["Content-Length"], str(len(body)))
        status, headers, _ = self.get(path, {"Range": "bytes=13-"})
        self.assertEqual((status, headers["Content-Range"]), (416, "bytes */13"))
        # Multiple or malformed ranges: the whole file
        for header in ("bytes=0-1,3-4", "bytes=5-2", "items=0-1", "bytes=a-b"):
            self.assertEqual(self.get(path, {"Range": header})[:3:2], (200, b"hello gateway"), header)
        # If-Range with another validator: the whole file
        self.assertEqual(self.get(path, {"Range": "bytes=0-1", "If-Range": '"old"'})[0], 200)
        self.assertEqual(self.get(path, {"Range": "bytes=0-1", "If-Range": f'"{self.text}"'})[0], 206)
        reads = [kwargs for _, kwargs in self.node.called("read") if kwargs["offset"] == 6]
        self.assertEqual(reads[0]["length"], 7)  # only the range is read

    def test_head(self):
        status, headers, body = self.get(f"/rnfs/{self.text}", method="HEAD")
        self.assertEqual((status, body, headers["Content-Length"]), (200, b"", "13"))

    def test_path_decoding(self):
        self.assertEqual(self.get(f"/rnfs/{self.root}/%3Cscript%3Ealert(1)%3C%2Fscript%3E")[0], 400)  # %2F
        self.assertEqual(self.get(f"/rnfs/{self.root}/sub/a%20b.txt")[2], b"hello gateway")
        for path in (f"/rnfs/{self.root}/sub//a", f"/rnfs/{self.root}/%2e%2e", f"/rnfs/{self.root}/a%00",
                     f"/rnfs/{self.root}/%ff"):
            self.assertEqual(self.get(path)[0], 400, path)
        self.assertEqual(self.get(f"/rnfs/{self.root}/javascript%3Ax")[2], b"hello gateway")

    def test_errors(self):
        self.assertEqual(self.get("/rnfs/")[0], 404)
        self.assertEqual(self.get(f"/rnfs/{self.text.upper()}")[0], 400)
        with mock.patch.object(self.node, "info", side_effect=RuntimeError("boom")), mock.patch("rnfs.gateway.log"):
            status, headers, body = self.get(f"/rnfs/{self.text}")
        self.assertEqual(status, 500)
        self.assertEqual(headers["Content-Type"], "text/plain; charset=utf-8")
        self.assertNotIn("ETag", headers)

    def test_always_for_gateway(self):
        self.get(f"/rnfs/{self.root}/")
        self.get(f"/rnfs/{self.root}/sub/")
        self.get(f"/rnfs/{self.text}", {"Range": "bytes=1-2"})
        for method in ("info", "read", "ls"):
            calls = self.node.called(method)
            self.assertTrue(calls, method)
            for _, kwargs in calls:
                self.assertIs(kwargs["for_gateway"], True, method)


class DirectoryTest(GatewayTestCase):

    def test_redirect_to_slash(self):
        status, headers, _ = self.get(f"/rnfs/{self.root}")
        self.assertEqual((status, headers["Location"]), (301, f"/rnfs/{self.root}/"))
        status, headers, _ = self.get(f"/rnfs/{self.root}/sub?x=1")
        self.assertEqual((status, headers["Location"]), (301, f"/rnfs/{self.root}/sub/?x=1"))

    def test_index_html(self):
        status, headers, body = self.get(f"/rnfs/{self.root}/sub/")
        self.assertEqual((status, body), (200, b"<p>index</p>"))
        self.assertEqual(headers["Content-Type"], "text/html")
        self.assertEqual(headers["ETag"], f'"{self.sub}"')

    def test_escaped_listing(self):
        status, headers, body = self.get(f"/rnfs/{self.root}/")
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "text/html; charset=utf-8")
        page = body.decode()
        self.assertNotIn("<script>", page)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", page)
        self.assertIn('href="./%3Cscript%3Ealert%281%29%3C%2Fscript%3E"', page)
        self.assertIn('href="./javascript%3Ax"', page)
        self.assertIn('href="./sub/"', page)
        self.assertNotIn('href="../"', page)  # the root has no parent
        self.assertIn(self.text, page)

    def test_listing_html_escapes_everything(self):
        page = listing_html('"><img src=x>', [{"name": "\"'&<>", "kind": "file", "size": 1, "cid": "b<x>"}],
                            True).decode()
        self.assertNotIn("<img", page)
        self.assertNotIn("<x>", page)
        self.assertIn('href="../"', page)


class NameTest(GatewayTestCase):

    def setUp(self):
        super().setUp()
        self.name = "ab" * 16
        self.node.records[self.name] = {"cid": self.root, "sequence": 3}

    def test_name_paths(self):
        status, headers, body = self.get(f"/name/{self.name}/sub/a%20b.txt")
        self.assertEqual((status, body), (200, b"hello gateway"))
        self.assertNotIn("ETag", headers)
        self.assertNotIn("immutable", headers.get("Cache-Control", ""))
        self.assertEqual(self.node.called("name_resolve")[0], ((self.name,), {"for_gateway": True}))
        self.assertEqual(self.get(f"/name/{self.name}")[1]["Location"], f"/name/{self.name}/")
        self.assertEqual(self.get(f"/name/{'cd' * 16}/")[0], 404)
        self.assertEqual(self.get("/name/xyz/")[0], 400)

    def test_resolutions_are_reused_briefly(self):
        for _ in range(3):
            self.assertEqual(self.get(f"/name/{self.name}/sub/")[0], 200)
        self.assertEqual(len(self.node.called("name_resolve")), 1)


class FetchLimitTest(GatewayTestCase):
    """Sec-Fetch-Site other than none: at most gateway_fetches (2) network fetches at a time."""

    def setUp(self):
        super().setUp()
        self.node.gate = threading.Event()
        self.addCleanup(self.node.gate.set)
        self.remote = [self.node.put_file(f"remote {i}".encode(), remote=True) for i in range(4)]
        self.results = {}

    def fetch_in_background(self, key, path, headers):
        """Start a request on a thread; its result lands in self.results[key]."""
        thread = threading.Thread(target=lambda: self.results.__setitem__(key, self.get(path, headers)), daemon=True)
        thread.start()
        return thread

    def wait_for_fetching(self, count):
        """Wait until count requests are inside a simulated network fetch."""
        deadline = time.monotonic() + 5
        while self.node.fetching < count:
            self.assertLess(time.monotonic(), deadline, "fetches didn't start")
            time.sleep(0.01)

    def test_limit(self):
        cross = {"Sec-Fetch-Site": "cross-site"}
        threads = [self.fetch_in_background(i, f"/rnfs/{self.remote[i]}", cross) for i in range(2)]
        self.wait_for_fetching(2)
        # A third limited fetch is refused while two are running
        status, headers, _ = self.get(f"/rnfs/{self.remote[2]}", {"Sec-Fetch-Site": "same-origin"})
        self.assertEqual(status, 503)
        self.assertIn("Retry-After", headers)
        self.assert_common_headers(headers)
        # Stored content is served without limit
        self.assertEqual(self.get(f"/rnfs/{self.text}", cross)[0], 200)
        self.assertEqual(self.get(f"/rnfs/{self.root}/", cross)[0], 200)
        # The user's own navigation, and tools that send no Sec-Fetch-Site, aren't limited
        threads.append(self.fetch_in_background("navigation", f"/rnfs/{self.remote[2]}", {"Sec-Fetch-Site": "none"}))
        threads.append(self.fetch_in_background("curl", f"/rnfs/{self.remote[3]}", {}))
        self.wait_for_fetching(4)
        self.node.gate.set()
        for thread in threads:
            thread.join(5)
        for key in (0, 1, "navigation", "curl"):
            self.assertEqual(self.results[key][0], 200, key)
        # Slots are released: a limited fetch works again
        self.node.remote.add(self.remote[0])
        self.assertEqual(self.get(f"/rnfs/{self.remote[0]}", cross)[0], 200)

    def test_name_resolution_counts(self):
        name = "ab" * 16
        self.node.records[name] = {"cid": self.root, "sequence": 1}
        cross = {"Sec-Fetch-Site": "cross-site"}
        threads = [self.fetch_in_background(i, f"/rnfs/{self.remote[i]}", cross) for i in range(2)]
        self.wait_for_fetching(2)
        self.assertEqual(self.get(f"/name/{name}/", cross)[0], 503)
        self.assertEqual(self.node.called("name_resolve"), [])
        self.node.gate.set()
        for thread in threads:
            thread.join(5)
        self.assertEqual(self.get(f"/name/{name}/", cross)[0], 200)


class ConnectionTest(GatewayTestCase):
    """Time limits and the connection cap, with short limits so the tests are quick."""

    def setUp(self):
        patcher = mock.patch.multiple(GatewayServer, request_timeout=0.5, response_timeout=0.5, max_connections=2)
        patcher.start()
        self.addCleanup(patcher.stop)
        super().setUp()

    def hold(self, count: int) -> list[socket.socket]:
        """count connections with unfinished requests, closed at cleanup."""
        held = held_connections(self.port, count, b"GET /rnfs/")
        for sock in held:
            self.addCleanup(sock.close)
        time.sleep(0.1)  # let the server accept them
        return held

    def test_idle_connection_is_closed(self):
        sock, = held_connections(self.port, 1, b"")
        self.addCleanup(sock.close)
        data, elapsed = read_until_closed(sock)
        self.assertEqual(data, b"")
        self.assertGreater(elapsed, 0.3)
        self.assertLess(elapsed, 3)

    def test_connection_cap(self):
        held = self.hold(2)
        for scripts, csp in ((True, SPEC_CSP), (False, CSP_NO_SCRIPTS)):
            self.config.gateway_scripts = scripts
            status, headers, data = self.get(f"/rnfs/{self.text}")
            self.assertEqual(status, 503)
            self.assertIn(b"too many", data)
            self.assertEqual(headers["Retry-After"], "10")
            self.assert_common_headers(headers, csp)
        self.assertEqual(self.node.called("info"), [])
        for sock in held:
            read_until_closed(sock)
        time.sleep(0.1)
        self.assertEqual(self.get(f"/rnfs/{self.text}")[0], 200)

    def test_clients_that_stop_reading_are_dropped(self):
        self.node.piece = 1024 * 1024
        big = self.node.put_file(bytes(64 * 1024 * 1024))
        for _ in range(2):  # fill the server with responses nobody reads
            sock = socket.socket()
            self.addCleanup(sock.close)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            sock.connect(("127.0.0.1", self.port))
            sock.sendall(f"GET /rnfs/{big} HTTP/1.1\r\nHost: 127.0.0.1:{self.port}\r\n\r\n".encode())
        time.sleep(0.2)
        self.assertEqual(self.get(f"/rnfs/{self.text}")[0], 503)
        # Writing to the stalled clients times out, which frees their slots
        deadline = time.monotonic() + 5
        while self.get(f"/rnfs/{self.text}")[0] != 200:
            self.assertLess(time.monotonic(), deadline, "the stalled responses kept their slots")
            time.sleep(0.1)

    def test_log_escapes_the_path(self):
        with mock.patch.object(self.node, "info", side_effect=RuntimeError("boom")), \
                mock.patch("rnfs.gateway.log") as log:
            sock = socket.create_connection(("127.0.0.1", self.port))
            self.addCleanup(sock.close)
            sock.sendall(f"GET /rnfs/{self.root}/a\x1b[2Jb HTTP/1.1\r\nHost: 127.0.0.1:{self.port}\r\n\r\n"
                         .encode("latin-1"))
            data, _ = read_until_closed(sock)
        self.assertTrue(data.startswith(b"HTTP/1.0 500"), data)
        (message, _), = [call.args for call in log.call_args_list]
        self.assertIn("/a\\x1b[2Jb", message)
        self.assertNotIn("\x1b", message)


class HelperTest(unittest.TestCase):

    def test_sniff(self):
        self.assertEqual(sniff(PNG), "image/png")
        self.assertEqual(sniff(b"\xff\xd8\xff\xe0rest"), "image/jpeg")
        self.assertEqual(sniff(b"GIF89a..."), "image/gif")
        self.assertEqual(sniff(b"RIFF\x00\x00\x00\x00WEBPVP8 "), "image/webp")
        self.assertEqual(sniff(b"%PDF-1.7"), "application/pdf")
        self.assertEqual(sniff(b"  \n<!DOCTYPE html><html>"), "text/html")
        self.assertEqual(sniff(b"\xef\xbb\xbf<html>"), "text/html")
        self.assertEqual(sniff(b"<p>hi</p>"), "text/html")
        self.assertEqual(sniff(b'<?xml version="1.0"?>\n<svg xmlns="http://www.w3.org/2000/svg">'), "image/svg+xml")
        self.assertEqual(sniff(b"<base64 is not html>"), "text/plain; charset=utf-8")
        self.assertEqual(sniff("plain text, ünïcode".encode()), "text/plain; charset=utf-8")
        self.assertEqual(sniff("cut ü".encode()[:-1]), "text/plain; charset=utf-8")  # cut mid-character
        self.assertEqual(sniff(b""), "text/plain; charset=utf-8")
        self.assertEqual(sniff(b"\x00\x01\x02binary"), "application/octet-stream")
        self.assertEqual(sniff(b"\xff\xfe\xfd"), "application/octet-stream")

    def test_parse_range(self):
        self.assertEqual(parse_range("bytes=0-0", 10), (0, 0))
        self.assertEqual(parse_range("bytes=-0", 10), "unsatisfiable")
        self.assertEqual(parse_range("bytes=0-", 0), "unsatisfiable")
        self.assertEqual(parse_range("bytes=-5", 0), "unsatisfiable")
        self.assertIsNone(parse_range(None, 10))
        self.assertIsNone(parse_range("bytes=-", 10))

    def test_etag_matches(self):
        self.assertTrue(etag_matches('W/"a", "b"', '"a"'))
        self.assertFalse(etag_matches('"ab"', '"a"'))
        self.assertFalse(etag_matches(None, '"a"'))

    def test_not_found_maps_to_404(self):
        node = FakeNode()
        server = GatewayServer(node, Config(), 0)
        server.start()
        self.addCleanup(server.stop)
        with mock.patch.object(node, "info", side_effect=NotFound("no provider found")):
            status, _, body = raw_request(server.port, "GET", f"/rnfs/{node.put_file(b'x')}")
        self.assertEqual((status, body), (404, b"no provider found\n"))


if __name__ == "__main__":
    unittest.main()
