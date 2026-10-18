"""API-5: the user's own HEAD, or a range not starting at byte 0, takes a file's Content-Type from its first bytes only
if they are stored (node_content.stored_head), and stored_head read them past the deny list: a HEAD sniffed a
denied stored chunk (its type leaked), and a HEAD of a stored file whose hash is denied answered 200 where GET
answered 451.

Now stored_head raises Denied for a denied CID it meets, and while the deny list has file hashes it checks the file
(CONT-1's bounded check) against stored blocks only: a HEAD of a stored denied file answers 451, like GET. A file
not wholly stored is still never fetched for a HEAD, and `pin ls` is unchanged.
"""
import hashlib
import io
import os
import time
import unittest
from unittest import mock

from rnfs.constants import CHUNK_SIZE
from rnfs.content import dag
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.storage import USER
from rnfs.errors import Denied
from rnfs.gateway import GatewayServer
from tests.security._content_helpers import TwoNodeTest
from tests.security.test_cont1_deny_hash_amplification import repeated
from tests.test_api import raw_request

PAGE = b"<!doctype html><title>a page</title>" + b"<p>text</p>" * 20_000  # several chunks, sniffed as HTML
DENIED_MESSAGE = b"unavailable: this node's deny list blocks this content\n"


class HeadDenyTest(TwoNodeTest):
    """B's gateway answers the user's own HEAD of what B stores."""

    def setUp(self):
        super().setUp()
        server = GatewayServer(self.b, self.b.config, 0)
        server.start()
        self.addCleanup(server.stop)
        self.port = server.port

    def deny(self, *entries):
        """Add entries to B's local list, its warnings silenced."""
        with mock.patch("rnfs.node.log"):
            self.b.deny.add([str(entry) for entry in entries])

    def stored(self, data: bytes) -> Cid:
        """A file B added (pinned "user", so a deny-list change keeps it stored)."""
        return Cid.parse(self.b.add_stream(io.BytesIO(data))["cid"])

    def request(self, method: str, cid: Cid, headers: dict | None = None) -> tuple[int, str | None, bytes]:
        """(status, Content-Type, body) of the user's own request (no Sec-Fetch-Site) for cid."""
        status, response, body = raw_request(self.port, method, f"/rnfs/{cid}", None, headers)
        return status, response.get("Content-Type"), body

    def test_head_of_a_hash_denied_file(self):
        """A stored file whose content hash is denied: HEAD and a later range answer 451, as GET does."""
        cid = self.stored(PAGE)
        self.assertEqual(self.request("HEAD", cid)[:2], (200, "text/html"))
        self.deny("sha256:" + hashlib.sha256(PAGE).hexdigest())
        status, _, body = self.request("GET", cid)
        self.assertEqual((status, body), (451, DENIED_MESSAGE))
        self.b.deny._known.clear()  # not remembered from the GET: the HEAD hashes it itself
        self.assertEqual(self.request("HEAD", cid)[0], 451)
        self.assertEqual(self.request("GET", cid, {"Range": f"bytes={CHUNK_SIZE}-{CHUNK_SIZE + 9}"})[0], 451)
        self.assertEqual(self.request("HEAD", cid)[0], 451)  # with the hash remembered

    def test_head_of_a_denied_first_chunk(self):
        """A stored file whose first chunk's CID is denied isn't sniffed: HEAD answers 451."""
        cid = self.stored(PAGE)
        first = Cid.for_block(RAW, PAGE[:CHUNK_SIZE])
        self.assertTrue(self.b.storage.has(first))
        self.deny(first)
        self.assertTrue(self.b.storage.has(first), "a user pin's blocks stay stored")
        status, content_type, _ = self.request("HEAD", cid)
        self.assertEqual(status, 451)
        self.assertNotEqual(content_type, "text/html")
        with self.assertRaises(Denied):
            self.b.stored_head(str(cid), 512)

    def test_head_of_the_bounded_poc(self):
        """A stored file repeating one chunk 2^40 times answers 451 to a HEAD at once, its chunk unread."""
        blocks, root, _ = repeated(os.urandom(CHUNK_SIZE), 5)
        with self.b.storage.protect() as session:
            for cid, block in blocks.items():
                session.put(cid, block)
        self.b.storage.pin(root, USER)
        self.deny("sha256:" + hashlib.sha256(b"something else").hexdigest())
        get = self.b.storage.get
        raw_reads = []
        with mock.patch.object(self.b.storage, "get", lambda cid, verify=False: (
                raw_reads.append(cid) if cid.is_raw else None) or get(cid, verify)):
            started = time.monotonic()
            self.assertEqual(self.request("HEAD", root)[0], 451)
        self.assertLess(time.monotonic() - started, 10)
        self.assertEqual(raw_reads, [])

    def test_allowed_and_not_wholly_stored(self):
        """With hash entries, a stored file that isn't denied is still sniffed. One not wholly stored can't be
        checked without fetching it, which a HEAD never does: it is typed by name only."""
        self.deny("sha256:" + hashlib.sha256(b"something else").hexdigest())
        cid = self.stored(PAGE)
        self.assertEqual(self.request("HEAD", cid)[:2], (200, "text/html"))
        # A file whose root and first chunk B stores, but not the rest: A has it all
        rest = os.urandom(CHUNK_SIZE)
        chunks = [PAGE[:CHUNK_SIZE], rest]
        cids = [Cid.for_block(RAW, chunk) for chunk in chunks]
        block = dag.encode(dag.FileNode([dag.Child(c, len(chunk)) for c, chunk in zip(cids, chunks)]))
        partial = Cid.for_block(DAG, block)
        self.serve({partial: block, **dict(zip(cids, chunks))}, partial)
        with self.b.storage.protect() as session:
            session.put(partial, block)
            session.put(cids[0], chunks[0])
        self.b.storage.pin(partial, USER)
        self.net.reset()
        status, content_type, _ = self.request("HEAD", partial)
        self.assertEqual((status, content_type), (200, "application/octet-stream"))
        self.assertEqual(self.net.downloaded_blocks, 0)
        self.assertFalse(self.b.storage.has(cids[1]))

    def test_pin_ls_is_unchanged(self):
        """pin ls still reports whether a pinned file is sealed, denied or not, without hashing it."""
        cid = self.stored(PAGE)
        self.deny("sha256:" + hashlib.sha256(PAGE).hexdigest(), Cid.for_block(RAW, PAGE[:CHUNK_SIZE]))
        with mock.patch.object(self.b, "_check_file", side_effect=AssertionError("hashed")):
            listed = {pin["cid"]: pin for pin in self.b.pin_ls()}
        self.assertIs(listed[str(cid)]["sealed"], False)


if __name__ == "__main__":
    unittest.main()
