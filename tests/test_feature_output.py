"""Feature CLI commands escape what the daemon sends before printing it (follow, keepers, contacts, peers):
whatever answers on the API port mustn't drive the terminal."""
import io
import unittest
from types import SimpleNamespace
from unittest import mock

from rnfs.features import contacts, follow, keepers, peers

EVIL = "ab\x1b]0;owned\x07\x1b[2J"


class FeatureCliOutputTest(unittest.TestCase):
    """Every field of a response is printed escaped."""

    def output(self, run, response, args=None) -> str:
        """What run prints when the daemon answers response to any request."""
        client = mock.Mock()
        client.get.return_value = response
        client.post.return_value = response
        out = io.StringIO()
        with mock.patch("rnfs.cli.client", return_value=client), mock.patch("sys.stdout", out):
            run(args or SimpleNamespace(name="x", nick="x", id="x", kind=None))
        self.assertTrue(out.getvalue())
        return out.getvalue()

    def assertEscaped(self, text: str):
        """No control character reaches the terminal."""
        self.assertNotIn("\x1b", text)
        self.assertNotIn("\x07", text)

    def test_follow(self):
        self.assertEscaped(self.output(follow.run_follow, {"name": EVIL}))
        self.assertEscaped(self.output(follow.run_following, {"following": [
            {"name": EVIL, "pinned": EVIL, "latest": EVIL, "state": EVIL}]}))

    def test_keepers(self):
        self.assertEscaped(self.output(keepers.run_keep, {"name": EVIL, "sequence": EVIL, "cid": EVIL}))
        self.assertEscaped(self.output(keepers.run_kept, {"kept": [
            {"name": EVIL, "sequence": EVIL, "cid": EVIL, "reasons": [EVIL]}]}))

    def test_contacts(self):
        self.assertEscaped(self.output(contacts.run_add, {"nick": EVIL, "kind": EVIL, "id": EVIL}))

    def test_peers(self):
        peer = {"node_id": EVIL, "mode": EVIL, "seniority": 0, "in_table": False, "last_heard": None, "rate": 0,
                "link": False, "interface": EVIL, "banned": False, "bad": 0, "hops": None, "sent": 0,
                "received": 0, "failed": 0}
        self.assertEscaped("\n".join(peers.format_peers([peer], 0.0)))


if __name__ == "__main__":
    unittest.main()
