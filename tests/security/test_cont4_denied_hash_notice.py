"""CONT-4 / API-6: a `sha256:` deny entry is checked where a file's content is assembled (get, cat, the gateway, add,
import), so it doesn't stop a multi-chunk file's blocks being served one by one (/blocks, the gateway's
?format=raw of a chunk) or provided. Specification section 8 documents that, and says to deny the CID for it.

Kept as documented (turning remembered hashes into CID denies is deferred), but the node now says so where it
matters: when _check_file finds a denied hash, it logs a NOTICE, once per file CID while it runs, suggesting
`rnfs deny add <file cid>`. Doing that stops the blocks being served.
"""
import hashlib
import io
import os
from unittest import mock

from rnfs.constants import CHUNK_SIZE
from rnfs.content import dag
from rnfs.content.cid import Cid
from rnfs.errors import Denied
from rnfs.log import LOG_NOTICE
from tests.test_node import NodeTest


class DeniedHashNoticeTest(NodeTest):
    """B holds a three-chunk file of its own, pinned and provided, whose content hash it then denies."""

    def setUp(self):
        super().setUp()
        self.data = os.urandom(3 * CHUNK_SIZE)
        self.cid = Cid.parse(self.b.add_stream(io.BytesIO(self.data))["cid"])
        self.chunks = [child.cid for child in dag.decode(self.b.storage.get(self.cid)).children]
        self.assertEqual(len(self.chunks), 3)
        with mock.patch("rnfs.node.log"):
            self.b.deny.add(["sha256:" + hashlib.sha256(self.data).hexdigest()])

    def read(self) -> list:
        """Read the file on B as cat does, expecting Denied; what B logged meanwhile, as (message, level)."""
        logged = []
        with mock.patch("rnfs.node.log", lambda message, level=None: logged.append((message, level))):
            with self.assertRaises(Denied):
                next(self.b.read(str(self.cid)))
        return logged

    def test_notice_suggests_denying_the_cid_once(self):
        """The first refusal logs a NOTICE naming `rnfs deny add <cid>`; the next one doesn't repeat it."""
        notices = [message for message, level in self.read() if level == LOG_NOTICE]
        self.assertEqual(len(notices), 1, notices)
        self.assertIn(f"rnfs deny add {self.cid}", notices[0])
        self.assertEqual([message for message, level in self.read() if level == LOG_NOTICE], [])

    def test_blocks_still_served_until_the_cid_is_denied(self):
        """What the notice is for: with only the hash denied, the file's blocks are still served to /blocks;
        once its CID is denied, none of them are."""
        for cid in [self.cid, *self.chunks]:
            self.assertEqual(self.b.backend.served_block(cid), self.b.storage.get(cid))
        with mock.patch("rnfs.node.log"):
            self.b.deny.add([str(self.cid)])
        for cid in [self.cid, *self.chunks]:
            self.assertIsNone(self.b.backend.served_block(cid))
