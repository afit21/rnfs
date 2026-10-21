"""REL-17 (0.2.0 release audit): what fuzzing the gateway, the API, rnfs.toml and tar streams found.

- A web page could set any Content-Type, control characters included, with `?filename=data:<type>,`: the gateway
  looked the type up with mimetypes.guess_type of the whole name, which takes a data: URL's media type as written;
  one that couldn't be encoded raised after the 200 status line was buffered, and the client got two status lines.
  The type now comes from the name's extension alone, and an error drops a half-built response's buffer.
- A control character in a request's query was echoed into a redirect's Location header: now percent-encoded.
- A request line of four words ending HTTP/0.9 was answered as HTTP/0.9, with no status line or headers (no CSP,
  no nosniff): refused like any HTTP/0.9 request.
- A deeply nested JSON body answered 500 and was logged as an error (RecursionError): now 400.
- A huge whole number (10**400) in a rate or duration setting raised OverflowError (the API's 500, an embedded node's
  undocumented error), and upload_budget = 10**400 passed validation, then stopped the node starting: settings hold
  64-bit whole numbers, as rnfs.toml does.
- A tar member refused by name, declaring 2**88 bytes, held a tar add's reader for good (tarfile skips a member by
  reading its size, and reads nothing for ever at the input's end); a size past what a seek takes raised
  OverflowError. Both are refused as invalid tar streams now.
"""
import io
import socket
import tarfile
import time
import unittest

from rnfs.config import Config
from rnfs.content import tarstream
from tests.test_api import ApiTestCase
from tests.test_gateway import GatewayTestCase


class GatewayFindingsTest(GatewayTestCase):

    def test_a_data_url_filename_sets_no_content_type(self):
        for name in ("data:a/%C4%80,", "data:a/b%01,", "data:text/html;x,", "data:image/svg%2Bxml,"):
            with self.subTest(name=name):
                status, headers, body = self.get(f"/rnfs/{self.text}?filename={name}")
                self.assertEqual((status, body), (200, b"hello gateway"))
                self.assertEqual(headers["Content-Type"], "text/plain; charset=utf-8")  # sniffed, as without a name

    def test_a_redirect_location_holds_no_control_character(self):
        # Sent raw: http.client won't send a control character in a URL, but anything else on the machine may
        with socket.create_connection(("127.0.0.1", self.port), timeout=10) as connection:
            connection.sendall(f"GET /rnfs/{self.root}/sub?\x01 HTTP/1.1\r\nHost: 127.0.0.1:{self.port}\r\n"
                               "Connection: close\r\n\r\n".encode("latin-1"))
            answer = b""
            while data := connection.recv(65536):
                answer += data
        head = answer.split(b"\r\n\r\n", 1)[0]
        self.assertTrue(head.startswith(b"HTTP/1.0 301"), head)
        self.assertNotIn(b"\x01", head)
        self.assertIn(b"/?%01\r\n", head)


class ApiFindingsTest(ApiTestCase):

    def test_a_deeply_nested_body_is_a_bad_request(self):
        status, _, _ = self.call("POST", "/api/pin/add", ("[" * 100_000).encode())
        self.assertEqual(status, 400)


class SettingsBoundTest(unittest.TestCase):

    def test_a_huge_whole_number_is_a_value_error(self):
        for name in ("lookup_timeout", "max_upload_rate", "upload_budget"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                Config(**{name: 10 ** 400}).validate()


class TarSizeTest(unittest.TestCase):

    @staticmethod
    def tar_of(name: str, size: int) -> bytes:
        """A GNU tar of one member called name, declaring size bytes (base-256 past what octal holds)."""
        info = tarfile.TarInfo(name)
        info.size = size
        return info.tobuf(tarfile.GNU_FORMAT) + b"\0" * 1024

    def test_a_huge_skipped_member_is_refused_at_once(self):
        for name in ("../x", "a/../b"):
            for size in (2 ** 40, 2 ** 88 - 1):  # the largest is what the 11-byte base-256 field holds
                data = self.tar_of(name, size)  # built outside assertRaises: its ValueError mustn't count
                with self.subTest(name=name, size=size):
                    started = time.monotonic()
                    with self.assertRaises(ValueError):
                        tarstream.build_tar(io.BytesIO(data), lambda cid, block: None,
                                            lambda text: None, 100)
                    with self.assertRaises(ValueError):
                        tarstream.scan_tar(io.BytesIO(data), 100)
                    self.assertLess(time.monotonic() - started, 5)


if __name__ == "__main__":
    unittest.main()
