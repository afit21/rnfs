"""REV6: a file put into a site's DAG can't be made to render as a page in that site's origin.

On a site host (<k + base36 CID>.rnfs.localhost, <name>.name.localhost) the page keeps a real origin (CSP sandbox
with allow-same-origin), so whatever is served there as HTML, SVG or XML runs script with that origin's storage. A
site that holds files other people supplied (uploads, a mirrored forum, attachments) used to let an attacker do
just that with a file of theirs:

- `?filename=x.html` set the Content-Type, so a link to `/uploads/note.txt?filename=x.html` rendered the text
  file as HTML in the site's origin (`GatewayHandler.serve_target`, `serve_file`);
- a file whose path name gives no type (`/uploads/1234`) was sniffed (`sniff`), and one starting `<html>` or
  `<svg` became text/html or image/svg+xml.

On site hosts `?filename=` now only names a download (`Content-Disposition: attachment`), the type comes from the
path's own name, and a file reached by a path inside the site's DAG is sniffed only to passive types (images,
audio, video, fonts, wasm, plain text; anything else is application/octet-stream). A site's own index.html and
files its DAG names .html or .svg still render: that is the site. A site whose root is itself a file is sniffed
as on the path gateway, HTML included: that file is its origin's only content. The path gateway, whose pages run
in an opaque origin, keeps its behaviour.
"""
import unittest

from rnfs.content.cid import Cid, to_base36

from tests.test_gateway import PNG, GatewayTestCase

NAME = "cd" * 16
SCRIPT_PAGE = b"<html><script>localStorage.getItem('secret')</script></html>"
SCRIPT_SVG = b'<svg xmlns="http://www.w3.org/2000/svg"><script>alert(document.domain)</script></svg>'
SCRIPT_XML_SVG = b'<?xml version="1.0"?>\n' + SCRIPT_SVG
RENDERED = ("text/html", "image/svg+xml", "application/xhtml+xml", "text/xml", "application/xml")


class SiteUploadRenderTest(GatewayTestCase):

    def setUp(self):
        super().setUp()
        self.note = self.node.put_file(b"just a note")
        self.html_upload = self.node.put_file(SCRIPT_PAGE)
        uploads = self.node.put_dir({
            "note.txt": self.note, "1234": self.html_upload, "5678": self.node.put_file(SCRIPT_SVG),
            "9012": self.node.put_file(SCRIPT_XML_SVG), "avatar": self.node.put_file(PNG),
            "plain": self.note, "remote": self.node.put_file(SCRIPT_PAGE + b" ", remote=True)})
        self.site = self.node.put_dir({"index.html": self.node.put_file(b"<p>the site</p>"),
                                       "about.html": self.node.put_file(b"<p>about</p>"),
                                       "logo.svg": self.node.put_file(SCRIPT_SVG), "uploads": uploads})
        self.node.records[NAME] = {"cid": self.site, "sequence": 1}

    def hosts(self) -> list[str]:
        """The site's two hosts: by CID and by name."""
        return [f"{to_base36(Cid.parse(self.site))}.rnfs.localhost:{self.port}", f"{NAME}.name.localhost:{self.port}"]

    def content_type(self, path, host, headers=None, method="GET") -> str:
        """The Content-Type of a successful request."""
        status, response, _ = self.get(path, headers, method=method, host=host)
        self.assertIn(status, (200, 206), (host, path))
        return response["Content-Type"]

    def test_filename_only_names_a_download(self):
        for host in self.hosts():
            for path, expected in (("/uploads/note.txt", "text/plain"), ("/uploads/1234", "application/octet-stream"),
                                   ("/uploads/plain", "text/plain; charset=utf-8")):
                for filename in ("x.html", "x.svg", "x.xhtml", "x.xml"):
                    status, headers, _ = self.get(f"{path}?filename={filename}", host=host)
                    self.assertEqual((status, headers["Content-Type"]), (200, expected), (host, path, filename))
                    self.assertEqual(headers["Content-Disposition"], f"attachment; filename*=UTF-8''{filename}")
                    self.assertEqual(headers["X-Content-Type-Options"], "nosniff")

    def test_filename_ignored_for_the_type_without_reading(self):
        # The user's own HEAD of a file not stored, or a range past byte 0: the type comes from the name alone
        for host in self.hosts():
            for headers, method in (({}, "HEAD"), ({"Range": "bytes=2-"}, "GET")):
                self.assertEqual(self.content_type("/uploads/remote?filename=x.html", host, headers, method),
                                 "application/octet-stream", (host, method))
                self.assertEqual(self.content_type("/uploads/note.txt?filename=x.svg", host, headers, method),
                                 "text/plain", (host, method))

    def test_never_sniffed_as_a_page(self):
        for host in self.hosts():
            for name in ("1234", "5678", "9012", "remote"):
                for headers in ({}, {"Sec-Fetch-Site": "same-origin"}):
                    content_type = self.content_type(f"/uploads/{name}", host, headers)
                    self.assertEqual(content_type, "application/octet-stream", (host, name, headers))
                    self.assertNotIn(content_type.split(";")[0], RENDERED)

    def test_a_site_whose_root_is_a_file(self):
        # The file is its origin's only content, so sniffing it as a page gains an attacker nothing (their own CID's
        # site would do the same), and a name pointing straight at a one-page HTML file keeps working
        other = "ef" * 16
        self.node.records[other] = {"cid": self.html_upload, "sequence": 1}
        for host in (f"{to_base36(Cid.parse(self.html_upload))}.rnfs.localhost:{self.port}",
                     f"{other}.name.localhost:{self.port}"):
            status, headers, body = self.get("/", host=host)
            self.assertEqual((status, headers["Content-Type"], body), (200, "text/html", SCRIPT_PAGE), host)
            self.assertNotIn("Content-Disposition", headers)
            # ?filename= still only names a download there, and never sets the type
            status, headers, _ = self.get("/?filename=x.svg", host=host)
            self.assertEqual((status, headers["Content-Type"]), (200, "text/html"), host)
            self.assertEqual(headers["Content-Disposition"], "attachment; filename*=UTF-8''x.svg")
            status, headers, _ = self.get("/?filename=x.txt", host=host)
            self.assertEqual(headers["Content-Disposition"], "attachment; filename*=UTF-8''x.txt")

    def test_passive_types_still_sniffed(self):
        for host in self.hosts():
            self.assertEqual(self.content_type("/uploads/avatar", host), "image/png")
            self.assertEqual(self.content_type("/uploads/plain", host), "text/plain; charset=utf-8")

    def test_the_sites_own_pages_still_render(self):
        for host in self.hosts():
            status, headers, body = self.get("/", host=host)
            self.assertEqual((status, headers["Content-Type"], body), (200, "text/html", b"<p>the site</p>"))
            self.assertNotIn("Content-Disposition", headers)
            self.assertEqual(self.content_type("/about.html", host), "text/html")
            self.assertEqual(self.content_type("/logo.svg", host), "image/svg+xml")

    def test_path_gateway_unchanged(self):
        # Pages there run in an opaque origin: ?filename= still sets the type, inline, and HTML is still sniffed
        status, headers, _ = self.get(f"/rnfs/{self.site}/uploads/note.txt?filename=x.html")
        self.assertEqual((status, headers["Content-Type"]), (200, "text/html"))
        self.assertEqual(headers["Content-Disposition"], "inline; filename*=UTF-8''x.html")
        self.assertNotIn("allow-same-origin", headers["Content-Security-Policy"])
        self.assertEqual(self.get(f"/rnfs/{self.site}/uploads/1234")[1]["Content-Type"], "text/html")
        self.assertEqual(self.get(f"/name/{NAME}/uploads/5678")[1]["Content-Type"], "image/svg+xml")


if __name__ == "__main__":
    unittest.main()
