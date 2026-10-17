"""FEAT-5: a name link quietly fell back to the CID its sender put in the message.

An rnfs link in an LXMF message's field carries both `name` and `cid` (the CID the name pointed at when sent),
and `rnfs inbox` showed only `rnfs:name/<name>/...`. When the name couldn't be resolved (owner offline, no
keeper reachable: common on a mesh), `inbox get` used the sender's CID without a word, so any sender, a stranger
included, could pose as a well-known name and have the user save the sender's file under the name's file name.

Node B runs the lxmf feature on a stand-in network (tests/test_lxmf.py's FeatureTestCase): node A holds the
sender's file (`self.cid`, with `file.txt` in it), and the message links name NAME, sent as that CID. The name is
made unresolvable, or resolvable to other content, by replacing B's name table's `resolve`.

Fixed: `inbox get` fails (NotFound, naming `--use-sent-cid`) before writing anything or starting a job, unless the
user asks for the sent CID, and then each result says so in `warnings`; the listing shows the sent CID next to a
name link; auto-fetch (trusted senders) keeps the fallback but records `"sent_cid": true`, which the listing
shows.
"""
import http.client
import io
import json
import os
import unittest
from types import SimpleNamespace
from unittest import mock

from rnfs.api import ApiServer
from rnfs.content.cid import Cid
from rnfs.errors import NotFound
from rnfs.features import lxmf
from rnfs.features.lxmf import Link
from tests import test_lxmf
from tests.test_lxmf import A_ID, NAME, SENDER, FeatureTestCase


def setUpModule():
    """Silence the node's log."""
    test_lxmf.setUpModule()


def tearDownModule():
    """Restore RNS's loglevel."""
    test_lxmf.tearDownModule()


class NameLinkFallbackTest(FeatureTestCase):
    """A message from SENDER linking name NAME, sent as A's directory holding the sender's file.txt."""

    def setUp(self):
        """The name link, an empty output directory, and the name unresolvable until a test says otherwise."""
        super().setUp()
        self.name_link = Link(self.cid, NAME, ("file.txt",), from_node=A_ID)
        self.out = os.path.join(self.tmp.name, "out")
        os.makedirs(self.out)
        self.resolved = None  # what B's name table answers for NAME: None (unresolvable) or a record

    def resolve(self, name, persist, max_age=0.0):
        """B's name table: the record the test set for NAME, else None."""
        return self.resolved if name == NAME else None

    def receive(self, **settings) -> int:
        """Node B (with settings) receives the name link; its inbox id. B's names resolve as self.resolved."""
        feature = self.feature_node(**settings)
        patch = mock.patch.object(self.b.names, "resolve", side_effect=self.resolve)
        patch.start()
        self.addCleanup(patch.stop)
        return feature.received(self.link_message(self.name_link))

    def name_points_elsewhere(self) -> bytes:
        """Make NAME resolve to another directory with its own file.txt; that file's content."""
        data = b"the name owner's file"
        other = Cid.parse(self.a.add_stream(io.BytesIO(data), name="file.txt")["cid"])
        self.resolved = {"name": NAME.hex(), "cid": str(other), "sequence": 1, "age": 0}
        return data

    def test_unresolvable_name_fails_without_writing_or_starting_anything(self):
        entry_id = self.receive()
        with self.assertRaisesRegex(NotFound, "--use-sent-cid"):
            self.feature.inbox_get(entry_id, directory=self.out)
        self.assertEqual(os.listdir(self.out), [], "inbox get wrote the sender's file for an unresolved name")
        # Through the jobs feature: no job is submitted either
        jobs = mock.Mock()
        jobs.submit.return_value = {"id": "0123abcd"}
        self.b.features["jobs"] = jobs
        with self.assertRaises(NotFound):
            self.feature.inbox_get(entry_id, output=os.path.join(self.out, "x"))
        jobs.submit.assert_not_called()

    def test_use_sent_cid_writes_the_file_with_a_warning(self):
        entry_id = self.receive()
        result = self.feature.inbox_get(entry_id, directory=self.out, use_sent_cid=True)[0]
        with open(os.path.join(self.out, "file.txt"), "rb") as f:
            self.assertEqual(f.read(), self.data)
        self.assertTrue(any(NAME.hex() in w and str(self.cid) in w for w in result["warnings"]), result)
        # A job's result carries the warning too
        jobs = mock.Mock()
        jobs.submit.return_value = {"id": "0123abcd"}
        self.b.features["jobs"] = jobs
        result = self.feature.inbox_get(entry_id, output=os.path.join(self.out, "y"), use_sent_cid=True)[0]
        self.assertEqual(result["job"], {"id": "0123abcd"})
        self.assertTrue(any(str(self.cid) in w for w in result["warnings"]), result)

    def test_resolvable_name_wins(self):
        entry_id = self.receive()
        data = self.name_points_elsewhere()
        for number, flag in enumerate((False, True)):
            output = os.path.join(self.out, f"f{number}")
            result = self.feature.inbox_get(entry_id, output=output, use_sent_cid=flag)[0]
            with open(output, "rb") as f:
                self.assertEqual(f.read(), data)
            self.assertEqual(result["warnings"], [])

    def call(self, server, body):
        """(status, decoded JSON) of POST /api/lxmf/inbox/get on server."""
        connection = http.client.HTTPConnection("127.0.0.1", server.port, timeout=30)
        headers = {"Host": f"127.0.0.1:{server.port}", "X-Rnfs-Api": server.token,
                   "Content-Type": "application/json"}
        connection.request("POST", "/api/lxmf/inbox/get", body=json.dumps(body).encode(), headers=headers)
        response = connection.getresponse()
        raw = response.read()
        connection.close()
        return response.status, json.loads(raw)

    def test_api_answers_404_without_use_sent_cid(self):
        entry_id = self.receive()
        server = ApiServer(self.b, 0)
        server.start()
        self.addCleanup(server.stop)
        status, body = self.call(server, {"id": entry_id, "directory": self.out})
        self.assertEqual(status, 404, body)
        self.assertEqual(os.listdir(self.out), [])
        status, body = self.call(server, {"id": entry_id, "directory": self.out, "use_sent_cid": True})
        self.assertEqual(status, 200, body)
        self.assertTrue(body["results"][0]["warnings"], body)
        self.assertEqual(os.listdir(self.out), ["file.txt"])

    def test_cli_passes_use_sent_cid(self):
        client = mock.Mock()
        client.post.return_value = {"results": [{"output": "/x/file.txt", "warnings": ["name unresolved"]}]}
        err = io.StringIO()
        with mock.patch("rnfs.cli.client", return_value=client), mock.patch("sys.stdout", io.StringIO()), \
                mock.patch("sys.stderr", err):
            lxmf._run_inbox_get(SimpleNamespace(output=None, n=1, link=None, use_sent_cid=True))
        self.assertTrue(client.post.call_args.kwargs["use_sent_cid"])
        self.assertIn("warning: name unresolved", err.getvalue())

    def listing(self, entry: dict) -> str:
        """What `rnfs inbox` prints for entry."""
        client = mock.Mock()
        client.get.return_value = {"messages": [entry]}
        out = io.StringIO()
        with mock.patch("rnfs.cli.client", return_value=client), mock.patch("sys.stdout", out):
            lxmf._run_inbox(SimpleNamespace())
        return out.getvalue()

    def test_listing_shows_the_sent_cid(self):
        entry_id = self.receive()
        listed = self.listing(self.feature.inbox.get(entry_id))
        self.assertIn(f"rnfs:name/{NAME.hex()}/file.txt", listed)
        self.assertIn(f"sent as {self.cid}", listed, "the inbox listing hides the CID a name link was sent as")

    def test_auto_fetch_marks_the_sent_cid(self):
        entry_id = self.receive(lxmf_auto_fetch_from=[SENDER.hex()])
        self.feature.process_auto_fetches()
        result = self.feature.inbox.get(entry_id)["auto"][0]
        self.assertEqual((result["state"], result.get("sent_cid")), ("pinned", True), result)
        self.assertIn("auto-fetch pinned (sent CID; name unresolved)", self.listing(self.feature.inbox.get(entry_id)))
        # A resolvable name: no mark
        self.name_points_elsewhere()
        second = self.feature.received(self.link_message(self.name_link, number=2))
        self.feature.process_auto_fetches()
        result = self.feature.inbox.get(second)["auto"][0]
        self.assertEqual(result["state"], "pinned")
        self.assertNotIn("sent_cid", result)
        self.assertNotIn("sent CID", self.listing(self.feature.inbox.get(second)))


if __name__ == "__main__":
    unittest.main()
