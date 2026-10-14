"""Response headers that keep the gateway and the local API safe, which no other test pins.

- The gateway percent-encodes ?filename= in Content-Disposition, so a CR/LF in it can't add a header.
- Every API response, including a name's exported private key, carries Cache-Control: no-store and
  X-Content-Type-Options: nosniff, on success and on error alike.
"""
import unittest

from tests.test_api import ApiTestCase
from tests.test_gateway import GatewayTestCase


class ContentDispositionTest(GatewayTestCase):
    """?filename= can't inject headers or break out of the filename* parameter."""

    def test_filename_is_percent_encoded(self):
        for name in ("a\r\nSet-Cookie: pwned=1", "a\nX-Injected: 1", 'x"; filename="evil.html', "a b;c=d"):
            path = f"/rnfs/{self.text}?filename=" + "".join(f"%{b:02X}" for b in name.encode())
            status, headers, _ = self.get(path)
            self.assertEqual(status, 200, name)
            self.assertNotIn("Set-Cookie", headers, name)
            self.assertNotIn("X-Injected", headers, name)
            value = headers["Content-Disposition"]
            self.assertTrue(value.startswith("inline; filename*=UTF-8''"), value)
            encoded = value[len("inline; filename*=UTF-8''"):]
            # Only RFC 5987 attr-chars and percent-escapes: no space, quote, semicolon, CR or LF
            self.assertRegex(encoded, r"\A(?:[A-Za-z0-9!#$&+\-.^_`|~]|%[0-9A-F]{2})+\Z", name)


class ApiHeadersTest(ApiTestCase):
    """no-store and nosniff on every kind of API response."""

    def assert_safe_headers(self, headers, context):
        self.assertEqual(headers["Cache-Control"], "no-store", context)
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff", context)

    def test_name_export(self):
        self.ok("POST", "/api/name/create", {"alias": "site"})
        status, headers, _ = self.call("POST", "/api/name/export", {"alias": "site"})
        self.assertEqual(status, 200)
        self.assert_safe_headers(headers, "name export")

    def test_other_responses(self):
        for method, path, body, api in (("GET", "/api/status", None, False),  # open to anyone
                                        ("GET", "/api/name/list", None, True),
                                        ("GET", "/api/name/list", None, False),  # refused: no token
                                        ("POST", "/api/name/export", {"alias": "missing"}, True),  # 404
                                        ("GET", "/api/nothing-here", None, True)):
            status, headers, _ = self.call(method, path, body, api=api)
            self.assert_safe_headers(headers, (method, path, api, status))


if __name__ == "__main__":
    unittest.main()
