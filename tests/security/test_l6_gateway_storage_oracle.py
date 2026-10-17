"""L6: the gateway told web pages which CIDs the node stores: `?format=archive` answered a web page's request or a HEAD
with 200 for a stored target and 409 for one not stored, `?format=raw` answered a web page with 200 (a download, which
fires no load event) for a stored block and a network fetch for one not stored (an error page, or 503 once the page
held every gateway_fetches slot), and a HEAD's Content-Type was sniffed from stored first bytes but
application/octet-stream when they weren't.

Two gateways serve the same content (the same CIDs) from two fake nodes, one storing it and one that has to fetch
every block from its "network" each time (its first bytes are never stored, as for a big file whose first chunk
isn't). Every request a web page can start (Sec-Fetch-Site other than "none") and every HEAD of a ?format= must get
the same status, headers and body from both, also for ?format= while the fetch slots are full or the network doesn't
have the content. The user's own requests (no Sec-Fetch-Site, or "none") may differ: only the user sees them. A last
test documents what still differs for plain paths while gateway_fetches is full.
"""
import json
import unittest

from rnfs.config import Config
from rnfs.errors import NotFound
from rnfs.gateway import GatewayServer

from tests.test_api import FakeNode, raw_request

PAGE = b"<!doctype html><p>a page</p>\n" * 30  # sniffed as text/html, and longer than the sniffed prefix
PNG = b"\x89PNG\r\n\x1a\n" + bytes(600)

# What a web page's requests carry: a cross-site page's subresource or frame, a gateway page's (opaque origin,
# so cross-site too), and the other values a browser may send
PAGE_REQUESTS = {
    "cross-site image": {"Sec-Fetch-Site": "cross-site", "Sec-Fetch-Mode": "no-cors", "Sec-Fetch-Dest": "image"},
    "cross-site iframe": {"Sec-Fetch-Site": "cross-site", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Dest": "iframe"},
    "cross-site fetch": {"Sec-Fetch-Site": "cross-site", "Sec-Fetch-Mode": "no-cors", "Sec-Fetch-Dest": "empty",
                         "Origin": "null"},
    "same-site": {"Sec-Fetch-Site": "same-site", "Sec-Fetch-Mode": "no-cors"},
    "same-origin": {"Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors"},
}


class NetworkNode(FakeNode):
    """A FakeNode whose remote CIDs stay remote: every read of one is a network fetch (counted in `fetches`), and
    nothing of them is ever stored. stored_head, block and export_stream answer as RnfsNode's do."""

    def __init__(self):
        super().__init__()
        self.fetches = 0
        self.findable = True  # False: no provider on the network has the remote CIDs

    def _fetch(self, cid: str):
        if cid in self.remote:
            with self.lock:
                self.fetches += 1
            if not self.findable:
                raise NotFound(f"no provider found for {cid}")

    def stored_head(self, target, length):
        self.record("stored_head", target, length)
        try:
            path = self._path(target)
        except Exception:  # "invalid" is None too, as for RnfsNode
            return None
        obj = self.objects.get(path[-1])
        if obj is None or obj[0] != "file" or set(path) & self.remote:
            return None
        return obj[1][:length]

    def block(self, cid, for_gateway=False):
        self.record("block", cid, for_gateway=for_gateway)
        obj = self._object(cid)
        return obj[1] if obj[0] == "file" else json.dumps(sorted(obj[1].items())).encode()

    def export_stream(self, target, start, for_gateway=False):
        self.record("export_stream", target, for_gateway=for_gateway)
        data = b"archive of " + self._resolve(target).encode()
        out = start(len(data))
        if out is not None:
            out.write(data)
        return {}


class StorageOracleTest(unittest.TestCase):
    """The same requests to a gateway whose node stores the content and to one whose node has to fetch it."""

    def setUp(self):
        self.gateways = {}
        for kind in ("stored", "unstored"):
            node = NetworkNode()
            remote = kind == "unstored"
            page = node.put_file(PAGE, remote=remote)
            data = node.put_file(PNG, remote=remote)
            site = node.put_dir({"index.html": page, "page.html": page, "data": data}, remote=remote)
            server = GatewayServer(node, Config(gateway_fetches=2), 0)
            server.start()
            self.addCleanup(server.stop)
            self.gateways[kind] = (node, server)
        self.page, self.data, self.site = page, data, site  # the same CIDs on both nodes

    def paths(self) -> list[str]:
        """Every kind of /rnfs/ path: bare files, files by name and sniffed, a site, and every format."""
        return [f"/rnfs/{self.page}", f"/rnfs/{self.data}", f"/rnfs/{self.site}/", f"/rnfs/{self.site}/page.html",
                f"/rnfs/{self.site}/data", f"/rnfs/{self.site}", f"/rnfs/{self.page}?filename=x.bin",
                *self.format_paths(), f"/rnfs/{self.site}/data?format=raw"]

    def format_paths(self) -> list[str]:
        """Every valid ?format= path: a file's and a directory's block, and archives of a root, a path and a file."""
        return [f"/rnfs/{self.page}?format=raw", f"/rnfs/{self.data}?format=raw", f"/rnfs/{self.site}?format=raw",
                f"/rnfs/{self.site}?format=archive", f"/rnfs/{self.site}/page.html?format=archive",
                f"/rnfs/{self.page}?format=archive"]

    def hold_fetch_slots(self):
        """A page holds every gateway_fetches slot of both gateways with fetches that never finish."""
        for node, server in self.gateways.values():
            limiter = server.server.RequestHandlerClass.limiter
            for _ in range(limiter.limit):
                self.assertTrue(limiter.try_acquire())
            self.addCleanup(lambda limiter=limiter: [limiter.release() for _ in range(limiter.limit)])

    def both(self, method: str, path: str, headers: dict) -> dict:
        """kind → (status, headers without Date, body) of the request to each gateway."""
        responses = {}
        for kind, (_, server) in self.gateways.items():
            status, response, body = raw_request(server.port, method, path, None, headers)
            responses[kind] = (status, sorted((k, v) for k, v in response.items() if k != "Date"), body)
        return responses

    def assert_same(self, method: str, path: str, headers: dict, context):
        responses = self.both(method, path, headers)
        self.assertEqual(responses["stored"], responses["unstored"], (context, method, path))
        return responses["stored"]

    def test_page_requests_dont_depend_on_storage(self):
        for name, headers in PAGE_REQUESTS.items():
            for path in self.paths():
                for method in ("GET", "HEAD"):
                    self.assert_same(method, path, headers, name)
            # A media element's seek: a range not starting at byte 0, so its first bytes aren't sent
            for method in ("GET", "HEAD"):
                status = self.assert_same(method, f"/rnfs/{self.page}", {**headers, "Range": "bytes=600-"}, name)[0]
                self.assertEqual(status, 206)
        self.assertGreater(self.gateways["unstored"][0].fetches, 0)  # the unstored node really did fetch

    def test_a_pages_head_has_the_sniffed_type(self):
        # Sniffed from the first bytes (fetched if need be), as its GET is: not octet-stream for content not stored
        for path, expected in ((f"/rnfs/{self.page}", "text/html"), (f"/rnfs/{self.data}", "image/png"),
                               (f"/rnfs/{self.site}/data", "image/png")):
            status, headers, body = self.assert_same("HEAD", path, PAGE_REQUESTS["cross-site image"], path)
            self.assertEqual((status, dict(headers)["Content-Type"], body), (200, expected, b""), path)

    def test_formats_for_pages_and_heads_are_always_409(self):
        # L6: fails until every ?format= (raw as well as archive) answers a page's request and every HEAD with 409
        users = {"no Sec-Fetch-Site": {}, "Sec-Fetch-Site none": {"Sec-Fetch-Site": "none"}}
        for path in self.format_paths():
            for name, headers in PAGE_REQUESTS.items():
                for method in ("GET", "HEAD"):
                    self.assertEqual(self.assert_same(method, path, headers, name)[0], 409, (name, method, path))
            for name, headers in users.items():  # the user's own HEAD
                self.assertEqual(self.assert_same("HEAD", path, headers, name)[0], 409, (name, path))
        for (node, _) in self.gateways.values():
            self.assertEqual((node.called("block"), node.called("export_stream")), ([], []))
        self.assertEqual(self.gateways["unstored"][0].fetches, 0)  # nothing was fetched for them either

    def test_formats_dont_depend_on_storage_while_fetch_slots_are_full(self):
        # L6: fails until a page's ?format=raw no longer gets a download if stored and a 503 if not
        self.hold_fetch_slots()
        for name, headers in PAGE_REQUESTS.items():
            for path in self.format_paths():
                for method in ("GET", "HEAD"):
                    self.assert_same(method, path, headers, name)

    def test_formats_dont_depend_on_storage_when_the_network_lacks_the_content(self):
        # L6: fails until a page's ?format=raw no longer gets a download if stored and an error page if not
        self.gateways["unstored"][0].findable = False
        for name, headers in PAGE_REQUESTS.items():
            for path in self.format_paths():
                for method in ("GET", "HEAD"):
                    self.assert_same(method, path, headers, name)

    def test_users_own_archive_get_still_works(self):
        # The user's own navigation (Sec-Fetch-Site: none) or tool (no header) gets the archive, fetching it if needed
        fetches = self.gateways["unstored"][0].fetches
        for headers in ({}, {"Sec-Fetch-Site": "none"}):
            status, _, body = self.assert_same("GET", f"/rnfs/{self.site}?format=archive", headers, headers)
            self.assertEqual((status, body), (200, f"archive of {self.site}".encode()))
        self.assertGreater(self.gateways["unstored"][0].fetches, fetches)

    def test_users_own_raw_get_still_works(self):
        # The user's own navigation or tool gets the block, fetching it if needed, even while a page holds every slot
        self.hold_fetch_slots()
        for headers in ({}, {"Sec-Fetch-Site": "none"}):
            for cid, block in ((self.page, PAGE), (self.data, PNG)):
                status, response, body = self.assert_same("GET", f"/rnfs/{cid}?format=raw", headers, headers)
                self.assertEqual((status, dict(response)["Content-Type"], body),
                                 (200, "application/vnd.rnfs.raw", block))
        self.assertEqual(self.gateways["unstored"][0].fetches, 4)

    def test_full_fetch_slots_still_reveal_storage(self):
        """Accepted residual (specification.md, gateway section): while a page holds every gateway_fetches slot,
        stored content is still served and content that needs a fetch gets 503. Closing it would break pages or
        move the leak into timing; this test pins the documented behaviour so a change to it is noticed."""
        self.hold_fetch_slots()
        responses = self.both("GET", f"/rnfs/{self.data}", PAGE_REQUESTS["cross-site image"])
        self.assertEqual((responses["stored"][0], responses["unstored"][0]), (200, 503))

if __name__ == "__main__":
    unittest.main()
