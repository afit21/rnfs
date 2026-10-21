"""REL-9 (0.2.0 release audit): gateway documents were cached as immutable, though their headers are the node's policy.

The attack: every /rnfs/ response carried "Cache-Control: public, max-age=31536000, immutable", yet its
Content-Security-Policy depends on gateway_scripts. A page cached while scripts were allowed kept running script in the
browser for up to a year after the user turned them off (an immutable response isn't revalidated, not even on reload),
and a later fix to these headers never reached cached copies.

The test asks for an HTML file, an SVG, a directory listing and an index.html (each revalidated: "public, no-cache",
with the ETag), a text file (still immutable), and checks that a 304 carries no Cache-Control, so what the browser
stored is kept while the current CSP reaches it. The fix: rnfs/gateway.py (for_type, DOCUMENT_TYPES,
send_not_modified).
"""
import unittest

from rnfs.gateway import IMMUTABLE, REVALIDATE
from tests.test_gateway import GatewayTestCase

SVG = b'<svg xmlns="http://www.w3.org/2000/svg"><script>alert(1)</script></svg>'


class DocumentCacheTest(GatewayTestCase):

    def test_documents_are_revalidated(self):
        svg = self.node.put_file(SVG)
        for path in (f"/rnfs/{self.page}", f"/rnfs/{svg}?filename=a.svg", f"/rnfs/{self.root}/",
                     f"/rnfs/{self.root}/sub/"):
            with self.subTest(path=path):
                status, headers, _ = self.get(path)
                self.assertEqual(status, 200)
                self.assertEqual(headers["Cache-Control"], REVALIDATE)
                self.assertIn("ETag", headers)

    def test_other_files_stay_immutable(self):
        self.assertEqual(self.get(f"/rnfs/{self.text}")[1]["Cache-Control"], IMMUTABLE)

    def test_a_revalidation_keeps_what_was_stored_and_brings_the_current_policy(self):
        status, headers, _ = self.get(f"/rnfs/{self.page}", {"If-None-Match": f'"{self.page}"'})
        self.assertEqual(status, 304)
        self.assertNotIn("Cache-Control", headers)
        self.assertIn("Content-Security-Policy", headers)


if __name__ == "__main__":
    unittest.main()
