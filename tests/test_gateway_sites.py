"""Tests for the gateway's subdomain hosts (rnfs/gateway.py, specification section 11): an origin per site.

http://<k + base36 CID>.rnfs.localhost:<port>/<path> serves <cid>/<path> and http://<name>.name.localhost:<port>/
the name's target, with a CSP keeping the page's origin, against the in-memory FakeNode with no RNS.
"""
import threading
import time
import unittest
from unittest import mock

from rnfs import api
from rnfs.content.cid import Cid, to_base36
from rnfs.errors import Denied
from rnfs.gateway import (CSP_NO_SCRIPTS, CSP_SCRIPTS, CSP_SITE_NO_SCRIPTS, CSP_SITE_SCRIPTS, GatewayError,
                          GatewayHandler, parse_host)

from tests.test_gateway import PNG, SPEC_CSP, GatewayTestCase

SPEC_SITE_CSP = ("sandbox allow-same-origin allow-scripts allow-forms allow-popups; default-src 'self' "
                 "'unsafe-inline' 'unsafe-eval' data: blob:; form-action 'self'; frame-ancestors 'self'")
SPEC_SITE_CSP_NO_SCRIPTS = ("sandbox allow-same-origin allow-forms allow-popups; default-src 'self' "
                            "'unsafe-inline' data: blob:; script-src 'none'; form-action 'self'; "
                            "frame-ancestors 'self'")
NAME = "ab" * 16


class ParseHostTest(unittest.TestCase):
    """parse_host: the path gateway's two hosts, the two site forms, and nothing else."""

    port = 8789

    def setUp(self):
        self.raw = Cid.for_block("raw", b"x")
        self.dag = Cid.for_block("dag", b"x")

    def refused(self, host, status=403):
        """Assert host is refused with status."""
        with self.assertRaises(GatewayError, msg=host) as caught:
            parse_host(host, self.port)
        self.assertEqual(caught.exception.status, status, host)

    def test_path_gateway_hosts(self):
        for host in ("127.0.0.1:8789", "localhost:8789", "LOCALHOST:8789", "LocalHost:8789"):
            self.assertIsNone(parse_host(host, self.port), host)

    def test_cid_sites(self):
        for cid in (self.raw, self.dag):
            label = to_base36(cid)
            for host in (f"{label}.rnfs.localhost:8789", f"{label}.RNFS.LOCALHOST:8789", f"{label.upper()}.rnfs"
                         ".localhost:8789", f"{label}.rnfs.Localhost:8789"):
                kind, value = parse_host(host, self.port)
                self.assertEqual((kind, value), ("rnfs", cid), host)
                self.assertIsInstance(value, Cid)
                self.assertEqual(str(value), str(cid))  # base32 from here on

    def test_name_sites(self):
        for host in (f"{NAME}.name.localhost:8789", f"{NAME.upper()}.name.localhost:8789",
                     f"{NAME}.NAME.LOCALHOST:8789"):
            self.assertEqual(parse_host(host, self.port), ("name", bytes.fromhex(NAME)), host)

    def test_other_hosts_refused(self):
        label = to_base36(self.raw)
        refused = [
            None, "", "evil.example", "evil.example:8789", "127.0.0.1", "localhost", "127.0.0.1:8790",
            "localhost:80", "[::1]:8789", "0.0.0.0:8789", "127.1:8789", "localhost.:8789",
            # The site domains themselves, other ports, no port, extra or missing labels, trailing dots
            "rnfs.localhost:8789", "name.localhost:8789", f"{label}.rnfs.localhost", f"{label}.rnfs.localhost:8790",
            f"{label}.rnfs.localhost:08789", f"{label}.rnfs.localhost:8789:8789", f"{label}.rnfs.localhost.:8789",
            f"a.{label}.rnfs.localhost:8789", f"{label}.a.rnfs.localhost:8789", f"{label}.rnfs.localhost.evil:8789",
            f"{label}.rnfs.evil.example:8789", f"{label}.localhost:8789", f"{label}.rnfs.127.0.0.1:8789",
            f"{label}.ipfs.localhost:8789", f"{NAME}.names.localhost:8789", f"{label}.rnfs.localhost:8789/x",
            f"{label}.rnfs.localhost :8789", f"{label}.rnfs.localhost:8789 ", f"{label}.rnfs.localhost:+8789",
            # Non-ASCII that lowercases to ASCII (KELVIN SIGN to "k"), or looks like it
            f"K{label[1:]}.rnfs.localhost:8789", f"{label}.rnfs.locaĺhost:8789",
        ]
        for host in refused:
            self.refused(host)

    def test_invalid_cid_or_name_in_a_site_host(self):
        raw, node = to_base36(self.raw), to_base36(self.dag)
        bad_labels = ["", "k", str(self.raw), str(self.dag), raw[:-1], raw + "0", node[:-1], node + "0",
                      "k0" + raw[1:-1], raw[:-1] + "_", "k" + "z" * 55, "z" + raw[1:], "-" + raw[1:]]
        for label in bad_labels:
            self.refused(f"{label}.rnfs.localhost:8789", 400)
        bad_names = ["", "ab" * 15, "ab" * 17, "xy" * 16, " " + "ab" * 15 + "a", "ab" * 15 + "a ", "<" + "ab" * 16 + ">",
                     to_base36(self.raw), "nick", "0x" + "ab" * 15]
        for label in bad_names:
            self.refused(f"{label}.name.localhost:8789", 400)

    def test_api_host_check_unchanged(self):
        # The API's own check is exact and doesn't take site hosts
        self.assertTrue(api.host_allowed("127.0.0.1:8788", 8788))
        self.assertTrue(api.host_allowed("localhost:8788", 8788))
        for host in (f"{to_base36(self.raw)}.rnfs.localhost:8788", f"{NAME}.name.localhost:8788",
                     "x.localhost:8788", "rnfs.localhost:8788"):
            self.assertFalse(api.host_allowed(host, 8788), host)


class SiteTestCase(GatewayTestCase):
    """Base: the GatewayTestCase DAG, with helpers to request it on its site hosts."""

    def setUp(self):
        super().setUp()
        self.node.records[NAME] = {"cid": self.root, "sequence": 3}

    def site_host(self, cid: str) -> str:
        """The Host of cid's site."""
        return f"{to_base36(Cid.parse(cid))}.rnfs.localhost:{self.port}"

    def name_host(self) -> str:
        """The Host of NAME's site."""
        return f"{NAME}.name.localhost:{self.port}"

    def on_site(self, cid, path, headers=None, method="GET"):
        """(status, headers, body) of a request on cid's site."""
        return self.get(path, headers, method=method, host=self.site_host(cid))

    def assert_site_headers(self, headers, csp=SPEC_SITE_CSP):
        """The headers every site response carries, and no CORS header."""
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(headers["Content-Security-Policy"], csp)
        self.assertEqual(headers["Origin-Agent-Cluster"], "?1")
        self.assertEqual([name for name in headers.keys() if name.lower().startswith("access-control")], [])


class SiteServingTest(SiteTestCase):

    def test_spec_csp_constants(self):
        self.assertEqual(CSP_SITE_SCRIPTS, SPEC_SITE_CSP)
        self.assertEqual(CSP_SITE_NO_SCRIPTS, SPEC_SITE_CSP_NO_SCRIPTS)
        # The path gateway's are unchanged
        self.assertEqual(CSP_SCRIPTS, SPEC_CSP)
        self.assertNotIn("allow-same-origin", CSP_NO_SCRIPTS)

    def test_cid_site(self):
        status, headers, body = self.on_site(self.root, "/sub/a%20b.txt")
        self.assertEqual((status, body), (200, b"hello gateway"))
        self.assertEqual(headers["ETag"], f'"{self.text}"')  # base32, the target's
        self.assertIn("immutable", headers["Cache-Control"])
        self.assertEqual(self.node.called("info")[-1][0], (f"{self.root}/sub/a b.txt",))
        # The root's index.html, and the root of a site that is a file
        status, _, body = self.on_site(self.sub, "/")
        self.assertEqual((status, body), (200, b"<p>index</p>"))
        status, headers, body = self.on_site(self.text, "/")
        self.assertEqual((status, body), (200, b"hello gateway"))
        self.assertEqual(self.on_site(self.root, "/pic")[1]["Content-Type"], "image/png")
        self.assertEqual(self.on_site(self.root, "/missing")[0], 404)
        # Everything is asked for the gateway, as on the path gateway: cache, never provided
        for method in ("info", "read", "ls"):
            self.assertTrue(all(kwargs.get("for_gateway") for _, kwargs in self.node.called(method)), method)

    def test_revalidation(self):
        status, headers, _ = self.on_site(self.root, "/", {"If-None-Match": f'"{self.root}"'})
        self.assertEqual((status, headers["ETag"]), (304, f'"{self.root}"'))
        status, headers, _ = self.on_site(self.root, "/sub/a%20b.txt", {"If-None-Match": f'"{self.text}"'})
        self.assertEqual(status, 304)
        self.assert_site_headers(headers)

    def test_name_site(self):
        status, headers, body = self.get("/sub/a%20b.txt", host=self.name_host())
        self.assertEqual((status, body), (200, b"hello gateway"))
        self.assertNotIn("ETag", headers)
        self.assertNotIn("immutable", headers.get("Cache-Control", ""))
        self.assertEqual(self.node.called("name_resolve")[0], ((NAME,), {"for_gateway": True}))
        self.assert_site_headers(headers)
        # Resolved names are reused as on the path gateway; an unknown name is a 404
        self.get("/", host=self.name_host())
        self.assertEqual(len(self.node.called("name_resolve")), 1)
        self.assertEqual(self.get("/", host=f"{'cd' * 16}.name.localhost:{self.port}")[0], 404)

    def test_listing_shows_base32(self):
        status, _, body = self.on_site(self.root, "/")
        self.assertEqual(status, 200)
        page = body.decode()
        self.assertIn(f"<title>/rnfs/{self.root}/</title>", page)
        self.assertNotIn(to_base36(Cid.parse(self.root)), page)
        self.assertIn('href="./sub/"', page)
        self.assertIn('href="./%3Cscript%3Ealert%281%29%3C%2Fscript%3E"', page)
        page = self.get("/", host=self.name_host())[2].decode()
        self.assertIn(f"<title>/name/{NAME}/</title>", page)

    def test_redirect_to_slash(self):
        status, headers, _ = self.on_site(self.root, "/sub")
        self.assertEqual((status, headers["Location"]), (301, "/sub/"))
        self.assert_site_headers(headers)
        status, headers, _ = self.on_site(self.root, "/sub?x=1")
        self.assertEqual((status, headers["Location"]), (301, "/sub/?x=1"))
        status, headers, _ = self.get("/sub", host=self.name_host())
        self.assertEqual((status, headers["Location"]), (301, "/sub/"))

    def test_redirect_stays_on_the_site(self):
        # Directory names a browser could read as another host or scheme are percent-encoded whole
        inner = self.node.put_dir({"f": self.text})
        root = self.node.put_dir({"\\evil.example": inner, "evil.example": inner, "javascript:x": inner,
                                  "a b?c#d": inner, "%2e": inner})
        expected = {"/%5Cevil.example": "/%5Cevil.example/", "/\\evil.example": "/%5Cevil.example/",
                    "/evil.example": "/evil.example/", "/javascript:x": "/javascript%3Ax/",
                    "/a%20b%3Fc%23d": "/a%20b%3Fc%23d/", "/%252e": "/%252e/"}
        for path, location in expected.items():
            status, headers, _ = self.on_site(root, path)
            self.assertEqual((status, headers["Location"]), (301, location), path)

    def test_empty_segments_and_bad_names(self):
        for path in ("/sub//a%20b.txt", "/sub//", "/./sub/", "/sub/../sub/", "/%2e%2e/", "/a%2Fb", "/a%00b",
                     "/%ff", "/sub/%2F/"):
            status, headers, _ = self.on_site(self.root, path)
            self.assertEqual(status, 400, path)
            self.assert_site_headers(headers)
        self.assertEqual(self.node.called("info"), [])
        # A path not starting with "/" (absolute-form aside, http.server passes the request target as it came)
        self.assertEqual(self.on_site(self.root, "sub/")[0], 400)

    def test_path_gateway_prefixes_are_plain_names(self):
        # /rnfs/<cid> on a site host is a path in the site's DAG, not another root
        status, _, _ = self.on_site(self.root, f"/rnfs/{self.text}")
        self.assertEqual(status, 404)
        self.assertEqual(self.node.called("info")[-1][0], (f"{self.root}/rnfs/{self.text}",))

    def test_sealed_files_are_never_decrypted(self):
        sealed = self.node.put_file(b"rnfs-sealed\n" + bytes(64))
        root = self.node.put_dir({"page.html": sealed})
        status, headers, body = self.on_site(root, "/page.html")
        self.assertEqual((status, headers["Content-Type"], body), (200, "application/octet-stream",
                                                                   b"rnfs-sealed\n" + bytes(64)))

    def test_deny_list(self):
        with mock.patch.object(self.node, "info", side_effect=Denied("on the deny list")):
            status, headers, body = self.on_site(self.root, "/")
        self.assertEqual(status, 451)
        self.assertNotIn(b"on the deny list", body)  # a node error's own text is never shown
        self.assert_site_headers(headers)

    def test_methods(self):
        status, headers, _ = self.on_site(self.root, "/", method="POST")
        self.assertEqual((status, headers["Allow"]), (405, "GET, HEAD"))
        self.assert_site_headers(headers)
        status, headers, body = self.on_site(self.root, "/sub/a%20b.txt", method="HEAD")
        self.assertEqual((status, body, headers["Content-Length"]), (200, b"", "13"))


class SiteHeaderTest(SiteTestCase):

    def responses(self, host: str) -> dict:
        """name -> response, for every kind of response on host."""
        return {
            "file": self.get("/sub/a%20b.txt", host=host),
            "listing": self.get("/", host=host),
            "redirect": self.get("/sub", host=host),
            "range": self.get("/sub/a%20b.txt", {"Range": "bytes=1-2"}, host=host),
            "unsatisfiable": self.get("/sub/a%20b.txt", {"Range": "bytes=999-"}, host=host),
            "missing": self.get("/missing", host=host),
            "bad name": self.get("/a%00b", host=host),
            "format": self.get("/?format=raw", host=host),
            "service worker": self.get("/sw.js", {"Service-Worker": "script"}, host=host),
            "head": self.get("/", method="HEAD", host=host),
            "post": self.get("/", method="POST", host=host),
        }

    def test_every_site_response_carries_the_site_headers(self):
        expected = {"file": 200, "listing": 200, "redirect": 301, "range": 206, "unsatisfiable": 416, "missing": 404,
                    "bad name": 400, "format": 400, "service worker": 403, "head": 200, "post": 405}
        for host in (self.site_host(self.root), self.name_host()):
            for name, (status, headers, _) in self.responses(host).items():
                self.assertEqual(status, expected[name], (host, name))
                self.assert_site_headers(headers)

    def test_scripts_off(self):
        self.config.gateway_scripts = False
        self.start()
        for host in (self.site_host(self.root), self.name_host()):
            headers = self.get("/", host=host)[1]
            self.assert_site_headers(headers, SPEC_SITE_CSP_NO_SCRIPTS)
            csp = headers["Content-Security-Policy"]
            self.assertNotIn("allow-scripts", csp)
            self.assertNotIn("unsafe-eval", csp)
            self.assertIn("script-src 'none'", csp)
        # The path gateway's, unchanged
        self.assert_common_headers(self.get(f"/rnfs/{self.root}/")[1], CSP_NO_SCRIPTS)

    def test_path_gateway_unchanged(self):
        for host in ("default", f"localhost:{self.port}"):
            status, headers, _ = self.get(f"/rnfs/{self.root}/sub/", host=host)
            self.assertEqual(status, 200)
            self.assert_common_headers(headers)
            self.assertNotIn("Origin-Agent-Cluster", headers)
            # Service-Worker isn't refused there: its pages have an opaque origin and can't register a worker
            self.assertEqual(self.get(f"/rnfs/{self.text}", {"Service-Worker": "script"}, host=host)[0], 200)

    def test_refused_hosts_get_the_path_gateways_headers(self):
        for host, status in (("evil.example", 403), (f"{self.root}.rnfs.localhost:{self.port}", 400),
                             (f"x.{self.site_host(self.root)}", 403), (f"nick.name.localhost:{self.port}", 400)):
            response_status, headers, _ = self.get("/", host=host)
            self.assertEqual(response_status, status, host)
            self.assert_common_headers(headers)
            self.assertNotIn("Origin-Agent-Cluster", headers)
        self.assertEqual(self.node.called("info"), [])
        self.assertEqual(self.node.called("name_resolve"), [])

    def test_service_worker_refused(self):
        for value in ("script", "SCRIPT", ""):
            for host in (self.site_host(self.root), self.name_host()):
                status, headers, body = self.get("/sub/a%20b.txt", {"Service-Worker": value}, host=host)
                self.assertEqual(status, 403, (value, host))
                self.assertIn(b"service worker", body)
        self.assertEqual(self.node.called("info"), [])
        self.assertEqual(self.node.called("name_resolve"), [])

    def test_format_refused(self):
        for query in ("?format=raw", "?format=archive", "?format=", "?format=car"):
            for method in ("GET", "HEAD"):
                status, _, _ = self.on_site(self.text, "/" + query, {"Sec-Fetch-Site": "none"}, method=method)
                self.assertEqual(status, 400, (query, method))
            self.assertEqual(self.get("/" + query, {"Sec-Fetch-Site": "none"}, host=self.name_host())[0], 400)
        self.assertEqual(self.node.called("info"), [])
        # The path gateway still answers them
        self.assertEqual(self.get(f"/rnfs/{self.text}?format=raw", {"Sec-Fetch-Site": "cross-site"})[0], 409)


class SiteOriginTest(SiteTestCase):
    """A site's page reads its own DAG with fetch() (same origin); other origins still can't (no CORS header)."""

    def test_same_origin_fetch(self):
        host = self.site_host(self.root)
        origin = f"http://{host}"
        for headers in ({"Origin": origin, "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors",
                         "Referer": f"{origin}/", "User-Agent": "Mozilla/5.0"},
                        # A same-origin GET may carry no Origin at all
                        {"Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors", "User-Agent": "Mozilla/5.0"}):
            for path, expected in (("/sub/a%20b.txt", b"hello gateway"), ("/pic", PNG)):
                status, response, body = self.get(path, headers, host=host)
                self.assertEqual((status, body), (200, expected), headers)
                self.assert_site_headers(response)
        status, response, body = self.get("/sub/a%20b.txt", {"Origin": f"http://{self.name_host()}",
                                                              "Sec-Fetch-Site": "same-origin"}, host=self.name_host())
        self.assertEqual((status, body), (200, b"hello gateway"))

    def test_other_origins_get_no_cors_header(self):
        host = self.site_host(self.root)
        others = ["null", f"http://{self.site_host(self.text)}", f"http://{self.name_host()}",
                  f"http://127.0.0.1:{self.port}", f"http://localhost:{self.port}", "http://evil.example"]
        for origin in others:
            for method in ("GET", "HEAD", "OPTIONS"):
                headers = {"Origin": origin, "Sec-Fetch-Site": "same-site", "Sec-Fetch-Mode": "cors",
                           "Access-Control-Request-Method": "GET"}
                status, response, _ = self.get("/sub/a%20b.txt", headers, method=method, host=host)
                self.assertEqual(status, 405 if method == "OPTIONS" else 200, (origin, method))
                self.assert_site_headers(response)

    def test_fetch_limit_applies(self):
        # A site page's fetches of content not stored are held to gateway_fetches as on the path gateway
        self.node.gate = threading.Event()
        self.addCleanup(self.node.gate.set)
        remote = [self.node.put_file(f"remote {i}".encode(), remote=True) for i in range(3)]
        root = self.node.put_dir({str(i): cid for i, cid in enumerate(remote)})
        page = {"Sec-Fetch-Site": "same-origin", "User-Agent": "Mozilla/5.0"}
        results = {}
        threads = [threading.Thread(target=lambda i=i: results.__setitem__(i, self.on_site(root, f"/{i}", page)),
                                    daemon=True) for i in range(2)]
        for thread in threads:
            thread.start()
        deadline = time.monotonic() + 5
        while self.node.fetching < 2:
            self.assertLess(time.monotonic(), deadline, "fetches didn't start")
            time.sleep(0.01)
        status, headers, _ = self.on_site(root, "/2", page)
        self.assertEqual(status, 503)
        self.assertIn("Retry-After", headers)
        self.assert_site_headers(headers)
        self.assertEqual(self.on_site(self.root, "/sub/a%20b.txt", page)[0], 200)  # stored: no limit
        self.node.gate.set()
        for thread in threads:
            thread.join(5)
        self.assertEqual([results[i][0] for i in range(2)], [200, 200])


class SiteLogTest(SiteTestCase):
    """Logs name a site's CID in base32, never in the host form."""

    @mock.patch("rnfs.gateway.log")
    def test_logged_in_base32(self, log):
        self.on_site(self.root, "/missing")
        self.on_site(self.root, "/a%00b")
        lines = " ".join(str(call.args[0]) for call in log.call_args_list)
        self.assertIn(f"rnfs site {self.root} /missing", lines)
        self.assertNotIn(to_base36(Cid.parse(self.root)), lines)

    def test_debug_log_host(self):
        handler = GatewayHandler.__new__(GatewayHandler)
        handler.server = mock.Mock(server_address=("127.0.0.1", self.port))
        handler.headers = {"Host": self.site_host(self.root)}
        self.assertEqual(handler.host_for_log(), f"{self.root}.rnfs.localhost (subdomain)")
        handler.headers = {"Host": self.name_host()}
        self.assertEqual(handler.host_for_log(), f"{NAME}.name.localhost (subdomain)")
        handler.headers = {"Host": "evil.example"}
        self.assertEqual(handler.host_for_log(), "evil.example")


if __name__ == "__main__":
    unittest.main()
