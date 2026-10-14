"""Gateway CORS: no gateway response carries an Access-Control-* header, whatever the request's Origin or Referer.

The rule that sent Access-Control-Allow-Origin for a Referer on the gateway never fired in browsers (sandboxed
pages have an opaque origin and send no Referer), and its obvious repair, allowing `Origin: null`, would let any
site's sandboxed frame or data: page read gateway responses (I1, L6). The rule is gone; these tests send every
kind of request the gateway answers, with a gateway Referer, `Origin: null`, another site's Origin and a CORS
preflight, and check that no response, errors and refusals included, carries a CORS header.
"""
import socket
import unittest
from unittest import mock

from tests.test_api import read_until_closed
from tests.test_gateway import GatewayTestCase

# Request headers a page could send: none, a gateway page's Referer (127.0.0.1 and localhost), the Origin of a
# sandboxed frame or data: page, another site's Origin, and a preflight's
HEADER_SETS = {
    "plain": {},
    "gateway referer": {"Referer": "http://127.0.0.1:{port}/rnfs/{root}/page.html", "Sec-Fetch-Site": "same-origin"},
    "localhost referer": {"Referer": "http://localhost:{port}/", "Sec-Fetch-Site": "same-site"},
    "origin null": {"Origin": "null", "Sec-Fetch-Site": "cross-site", "Sec-Fetch-Mode": "cors"},
    "origin null and gateway referer": {"Origin": "null", "Referer": "http://127.0.0.1:{port}/rnfs/{root}/"},
    "other site": {"Origin": "http://evil.example", "Referer": "http://evil.example/", "Sec-Fetch-Site": "cross-site"},
    "preflight": {"Origin": "null", "Access-Control-Request-Method": "GET",
                  "Access-Control-Request-Headers": "range"},
}


class NoCorsTest(GatewayTestCase):
    """Every gateway response, for every kind of request and every Origin or Referer, lacks CORS headers."""

    def requests(self) -> dict:
        """name → (method, path) covering every kind of response: content, redirects, 304, ranges, errors."""
        return {
            "file": ("GET", f"/rnfs/{self.text}"),
            "head": ("HEAD", f"/rnfs/{self.text}"),
            "listing": ("GET", f"/rnfs/{self.root}/"),
            "index": ("GET", f"/rnfs/{self.sub}/"),
            "redirect": ("GET", f"/rnfs/{self.root}"),
            "range": ("GET", f"/rnfs/{self.text}?range"),
            "missing": ("GET", f"/rnfs/{self.root}/missing"),
            "bad cid": ("GET", "/rnfs/nope"),
            "unknown path": ("GET", "/elsewhere"),
            "raw": ("GET", f"/rnfs/{self.text}?format=raw"),
            "archive": ("GET", f"/rnfs/{self.root}?format=archive"),
            "archive head": ("HEAD", f"/rnfs/{self.root}?format=archive"),
            "archive not stored": ("HEAD", f"/rnfs/{self.node.put_file(b'not stored', remote=True)}?format=archive"),
            "name": ("GET", f"/name/{'00' * 16}/"),
            "options": ("OPTIONS", f"/rnfs/{self.text}"),
            "post": ("POST", f"/rnfs/{self.text}"),
        }

    def assert_no_cors(self, headers, context):
        cors = [name for name in headers.keys() if name.lower().startswith("access-control")]
        self.assertEqual(cors, [], context)

    @mock.patch("rnfs.gateway.log")  # the fake node has no block() or export_stream(): those are 500s, logged
    def test_no_cors_header_in_any_response(self, _log):
        statuses = set()
        for set_name, template in HEADER_SETS.items():
            headers = {name: value.format(port=self.port, root=self.root) for name, value in template.items()}
            for name, (method, path) in self.requests().items():
                extra = dict(headers)
                if name == "range":
                    path, extra["Range"] = path.removesuffix("?range"), "bytes=2-"
                status, response, _ = self.get(path, extra, method=method)
                statuses.add(status)
                self.assert_no_cors(response, (set_name, name, status))
            # A revalidation and an unsatisfiable range too
            for extra in ({"If-None-Match": f'"{self.text}"'}, {"Range": "bytes=999-"}):
                status, response, _ = self.get(f"/rnfs/{self.text}", {**headers, **extra})
                statuses.add(status)
                self.assert_no_cors(response, (set_name, extra, status))
            status, response, _ = self.get(f"/rnfs/{self.text}", headers, host="evil.example")
            statuses.add(status)
            self.assert_no_cors(response, (set_name, "bad host", status))
        # The requests above did reach every kind of response
        self.assertTrue({200, 206, 301, 304, 400, 403, 404, 405, 409, 416} <= statuses, statuses)

    def test_no_cors_in_private_mode_either(self):
        self.config.private = True
        self.start()
        status, response, _ = self.get(f"/rnfs/{self.text}", {"Origin": "null",
                                                              "Referer": f"http://127.0.0.1:{self.port}/"})
        self.assertEqual(status, 200)
        self.assert_no_cors(response, "private")

    def test_no_cors_on_responses_sent_without_a_handler(self):
        # A malformed request line (answered before the headers are parsed) and the over-capacity 503
        with socket.create_connection(("127.0.0.1", self.port), timeout=10) as sock:
            sock.sendall(f"GARBAGE\r\nHost: 127.0.0.1:{self.port}\r\nOrigin: null\r\n\r\n".encode())
            data, _ = read_until_closed(sock)
        self.assertTrue(data.startswith(b"HTTP/1.0 400 "), data[:100])
        self.assertNotIn(b"access-control", data.lower())
        busy = self.server.server.RequestHandlerClass.busy_response()
        self.assertNotIn(b"access-control", busy.lower())


if __name__ == "__main__":
    unittest.main()
