"""Gateway site hosts: one origin per root, and no way for one host to serve or share another's.

The subdomain gateway (0.2.0) gives each CID and name its own origin, <k + base36 CID>.rnfs.localhost:<port> and
<name>.name.localhost:<port>, and lets pages there keep it (CSP sandbox with allow-same-origin), so a page can
read its own DAG with fetch(). That is safe only while the origin really names the content served: these tests
rebuild the ways a request could confuse them and check each is refused.

- A request on A's host naming B in its target (absolute-form request target, /rnfs/<B>, dot segments,
  encoded slashes) never makes the node read anything but A's DAG (`GatewayHandler.serve_site`).
- One CID has one host: a non-canonical base36 label (a leading zero, base32, other lengths) is refused
  (`content.cid.from_base36`), so the same content can't be put in a second origin to escape its storage or
  a frame-ancestors check. Case variants are the same host to a browser, which lowercases them.
- The host form is accepted nowhere else: not by the path gateway, nor by the API's CID parsers (`Cid.parse`).
- The path gateway's hosts (127.0.0.1, localhost) never get allow-same-origin or Origin-Agent-Cluster, so every
  CID served there stays in an opaque origin rather than sharing one origin between all of them.
- Name hosts take 32 hex characters only, never a contact nick (`parse_host`).
- Service worker scripts are refused on site hosts, so a worker can't keep serving a name's old content.
"""
import unittest

from rnfs import api
from rnfs.content.cid import Cid, to_base36

from tests.test_api import raw_request
from tests.test_gateway import GatewayTestCase


class SiteHostConfusionTest(GatewayTestCase):

    def setUp(self):
        super().setUp()
        self.other = self.node.put_dir({"secret.txt": self.node.put_file(b"another site's file")})

    def host(self, cid: str) -> str:
        """The Host of cid's site."""
        return f"{to_base36(Cid.parse(cid))}.rnfs.localhost:{self.port}"

    def roots_read(self) -> set[str]:
        """The root CIDs of every target the node was asked about."""
        return {args[0].split("/")[0] for name, args, _ in self.node.calls if args and isinstance(args[0], str)}

    def test_a_site_serves_only_its_own_dag(self):
        host = self.host(self.root)
        paths = [f"http://{self.host(self.other)}/secret.txt", f"http://127.0.0.1:{self.port}/rnfs/{self.other}/",
                 f"/rnfs/{self.other}/secret.txt", f"/../{self.other}/secret.txt", f"/%2e%2e/{self.other}/",
                 f"/{self.other}/secret.txt", f"/sub%2F..%2F..%2F{self.other}/", f"/sub/../../{self.other}/",
                 f"/?cid={self.other}", f"/sub/?format=raw&cid={self.other}"]
        for path in paths:
            status, _, body = raw_request(self.port, "GET", path, host=host)
            if path.startswith("http://"):
                self.assertEqual(status, 400, path)  # absolute-form targets are refused outright
            self.assertNotIn(b"another site's file", body, path)
        self.assertEqual(self.roots_read(), {self.root})

    def test_one_cid_one_host(self):
        label = to_base36(Cid.parse(self.root))
        for other_label in ("k0" + label[1:-1], label + "0", label[:-1], self.root, "b" + label[1:]):
            status, _, _ = self.get("/", host=f"{other_label}.rnfs.localhost:{self.port}")
            self.assertEqual(status, 400, other_label)
        self.assertEqual(self.node.calls, [])

    def test_host_form_accepted_nowhere_else(self):
        label = to_base36(Cid.parse(self.text))
        for path in (f"/rnfs/{label}", f"/rnfs/{label}/", f"/rnfs/{label}?format=raw", f"/name/{label}/"):
            self.assertEqual(self.get(path, {"Sec-Fetch-Site": "none"})[0], 400, path)
        for parser in (Cid.parse, api.parse_cid, api.parse_target):
            with self.assertRaises(ValueError):
                parser(label)
        self.assertEqual(self.node.calls, [])

    def test_path_gateway_never_shares_an_origin(self):
        for host in ("default", f"localhost:{self.port}", f"LOCALHOST:{self.port}"):
            for path in (f"/rnfs/{self.root}/", f"/rnfs/{self.root}/sub/", f"/rnfs/{self.text}", "/elsewhere",
                         f"/rnfs/{self.root}"):
                for extra in ({}, {"Service-Worker": "script"}, {"Origin": f"http://{self.host(self.root)}"}):
                    _, headers, _ = self.get(path, extra, host=host)
                    self.assertNotIn("allow-same-origin", headers["Content-Security-Policy"], (host, path))
                    self.assertNotIn("Origin-Agent-Cluster", headers)

    def test_name_hosts_never_expand_nicks(self):
        for label in ("mysite", "abc", "ab" * 15 + "zz", "-" * 32):
            status, _, _ = self.get("/", host=f"{label}.name.localhost:{self.port}")
            self.assertEqual(status, 400, label)
        self.assertEqual(self.node.called("name_resolve"), [])

    def test_service_worker_refused_on_sites(self):
        name = "ab" * 16
        self.node.records[name] = {"cid": self.root, "sequence": 1}
        for host in (self.host(self.root), f"{name}.name.localhost:{self.port}"):
            for path in ("/sub/a%20b.txt", "/", "/sw.js"):
                self.assertEqual(self.get(path, {"Service-Worker": "script"}, host=host)[0], 403, (host, path))
        self.assertEqual(self.node.calls, [])


if __name__ == "__main__":
    unittest.main()
