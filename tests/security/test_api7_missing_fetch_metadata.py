"""API-7: the gateway counted every request without Sec-Fetch-Site as the user's own (a local tool such as curl).
Browsers without Fetch Metadata (Safari before 16.4, Firefox before 90) send none either, so in them any web page's
request was the user's own: it got ?format= downloads (reopening L6's storage oracle: a download for stored content, a fetch
for the rest) and wasn't held to gateway_fetches.

Now (decision Q7) a request without Sec-Fetch-Site is the user's own only if its User-Agent doesn't start with
"Mozilla/", which every browser's does. Such a browser is treated as a web page's: ?format= gets 409 and network use
counts against gateway_fetches, whether or not the user typed the address. curl, scripts and other tools keep the
user's own access; so does a browser's own navigation that says so (Sec-Fetch-Site: none).
"""
import unittest

from rnfs.config import Config
from rnfs.gateway import GatewayServer
from tests.security.test_l6_gateway_storage_oracle import PAGE, PNG, NetworkNode
from tests.test_api import raw_request

# Browsers without Fetch Metadata: no Sec-Fetch-Site on any request, whoever started it
OLD_BROWSERS = {
    "Safari 16.3": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) "
                   "Version/16.3 Safari/605.1.15",
    "Firefox 89": "Mozilla/5.0 (X11; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/ only": "Mozilla/",
    "lower case": "mozilla/5.0 (compatible)",
    "leading space": " Mozilla/5.0",
}
# The user's own tools: no Sec-Fetch-Site and a User-Agent that isn't a browser's (or none)
TOOLS = {"curl": "curl/8.5.0", "wget": "Wget/1.21.4", "python": "Python-urllib/3.12", "rnfs": "rnfs",
         "no User-Agent": None, "empty": "", "Mozilla later": "MyTool/1.0 (like Mozilla/5.0)"}


class MissingFetchMetadataTest(unittest.TestCase):
    """Two gateways, one whose node stores the content and one whose node has to fetch it."""

    def setUp(self):
        self.gateways = {}
        for kind in ("stored", "unstored"):
            node = NetworkNode()
            remote = kind == "unstored"
            page = node.put_file(PAGE, remote=remote)
            data = node.put_file(PNG, remote=remote)
            site = node.put_dir({"index.html": page, "data": data}, remote=remote)
            server = GatewayServer(node, Config(gateway_fetches=2), 0)
            server.start()
            self.addCleanup(server.stop)
            self.gateways[kind] = (node, server)
        self.page, self.data, self.site = page, data, site

    def format_paths(self) -> list[str]:
        """A block and an archive."""
        return [f"/rnfs/{self.data}?format=raw", f"/rnfs/{self.site}?format=archive"]

    def request(self, kind: str, method: str, path: str, user_agent: str | None, extra: dict | None = None) -> tuple:
        """(status, body) of the request to the gateway of kind, with that User-Agent (None: none sent)."""
        headers = {**({"User-Agent": user_agent} if user_agent is not None else {}), **(extra or {})}
        status, _, body = raw_request(self.gateways[kind][1].port, method, path, None, headers)
        return status, body

    def hold_fetch_slots(self):
        """A page holds every gateway_fetches slot of both gateways with fetches that never finish."""
        for _, server in self.gateways.values():
            limiter = server.server.RequestHandlerClass.limiter
            for _ in range(limiter.limit):
                self.assertTrue(limiter.try_acquire())
            self.addCleanup(lambda limiter=limiter: [limiter.release() for _ in range(limiter.limit)])

    def test_old_browsers_get_no_formats(self):
        # API-7: fails until a request without Sec-Fetch-Site from a browser gets 409 for ?format=, stored or not
        for name, user_agent in OLD_BROWSERS.items():
            for path in self.format_paths():
                for kind in self.gateways:
                    status, _ = self.request(kind, "GET", path, user_agent)
                    self.assertEqual(status, 409, (name, kind, path))
        for node, _ in self.gateways.values():
            self.assertEqual((node.called("block"), node.called("export_stream")), ([], []))
        self.assertEqual(self.gateways["unstored"][0].fetches, 0)

    def test_old_browsers_are_held_to_gateway_fetches(self):
        # API-7: fails until a browser's request without Sec-Fetch-Site counts against gateway_fetches
        self.hold_fetch_slots()
        for name, user_agent in OLD_BROWSERS.items():
            self.assertEqual(self.request("unstored", "GET", f"/rnfs/{self.data}", user_agent)[0], 503, name)
            self.assertEqual(self.request("stored", "GET", f"/rnfs/{self.data}", user_agent),
                             (200, PNG), name)  # stored content is served without limit, as for any page
        self.assertEqual(self.gateways["unstored"][0].fetches, 0)

    def test_tools_keep_the_users_own_access(self):
        # curl and scripts still get ?format= (fetching what isn't stored) and aren't held to gateway_fetches
        self.hold_fetch_slots()
        for name, user_agent in TOOLS.items():
            for kind in self.gateways:
                status, body = self.request(kind, "GET", f"/rnfs/{self.site}?format=archive", user_agent)
                self.assertEqual((status, body), (200, f"archive of {self.site}".encode()), (name, kind))
                self.assertEqual(self.request(kind, "GET", f"/rnfs/{self.data}?format=raw", user_agent),
                                 (200, PNG), (name, kind))
                self.assertEqual(self.request(kind, "GET", f"/rnfs/{self.data}", user_agent), (200, PNG), (name, kind))

    def test_a_browsers_own_navigation_still_works(self):
        # A browser with Fetch Metadata says Sec-Fetch-Site: none for the user's own navigation: the User-Agent
        # doesn't matter then, nor for a page's request (cross-site stays limited whatever the User-Agent)
        for user_agent in (*OLD_BROWSERS.values(), *TOOLS.values()):
            status, _ = self.request("unstored", "GET", f"/rnfs/{self.site}?format=archive", user_agent,
                                     {"Sec-Fetch-Site": "none"})
            self.assertEqual(status, 200, user_agent)
            status, _ = self.request("unstored", "GET", f"/rnfs/{self.site}?format=archive", user_agent,
                                     {"Sec-Fetch-Site": "cross-site"})
            self.assertEqual(status, 409, user_agent)


if __name__ == "__main__":
    unittest.main()
