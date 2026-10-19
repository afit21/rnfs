"""Tests for app tokens (rnfs/apps.py): the registry in the state database, and the node's app_add, app_ls and app_rm,
which releases the app's pins."""
import io
import os
import tempfile
import unittest

import RNS

from rnfs import statedb
from rnfs.apiclient import api_proof, app_proof_key, app_verifier
from rnfs.apps import AppRegistry
from rnfs.config import Config
from rnfs.content.cid import Cid
from rnfs.content.storage import USER, app_source
from rnfs.errors import NotFound
from rnfs.node import RnfsNode
from tests.support import failing_commits, state_execute, state_rows
from tests.test_names import FakeNetwork
from tests.test_node import NetClient

NODE_ID = bytes(range(16))
_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class AppRegistryTest(unittest.TestCase):
    """An AppRegistry on a temporary state directory."""

    def setUp(self):
        """An empty state directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = os.path.join(self.tmp.name, "state")
        self.apps = self.open()

    def open(self) -> AppRegistry:
        """A registry on the state directory (closed before it is removed)."""
        apps = AppRegistry(self.state)
        self.addCleanup(apps.close)
        return apps

    def test_add_gives_the_token_once_and_keeps_its_verifier(self):
        """add() returns the token; the registry and the database keep only its verifier, which checks it and keys
        the app's proof."""
        added = self.apps.add("wiki")
        token = added["token"]
        self.assertEqual(added["name"], "wiki")
        self.assertGreaterEqual(len(token), 43)  # 256 bits, URL-safe base64
        self.assertEqual(self.apps.list(), [{"name": "wiki", "created": added["created"]}])
        self.assertIn("wiki", self.apps)
        self.assertTrue(self.apps.verify("wiki", token))
        self.assertFalse(self.apps.verify("wiki", token + "x"))
        self.assertFalse(self.apps.verify("other", token))
        self.assertEqual(self.apps.proof_key("wiki"), app_proof_key(token))
        self.assertIsNone(self.apps.proof_key("other"))
        rows = state_rows(self.state, "SELECT name, verifier, created FROM apps")
        self.assertEqual(rows, [("wiki", app_verifier(token), added["created"])])
        with open(os.path.join(self.state, statedb.DB_FILE), "rb") as f:
            self.assertNotIn(token.encode(), f.read())

    def test_verifier_and_proof(self):
        """The verifier is SHA-256 of a fixed label and the token; the proof an app checks is api_proof keyed by its
        hex, not by the token."""
        import hashlib
        self.assertEqual(app_verifier("abc"), hashlib.sha256(b"rnfs app token abc").digest())
        self.assertNotEqual(api_proof(app_proof_key("abc"), 1, "00" * 16), api_proof("abc", 1, "00" * 16))

    def test_apps_outlive_a_restart(self):
        """Apps are kept in the state database: a registry opened again knows them and their tokens."""
        token = self.apps.add("wiki")["token"]
        self.apps.add("gallery")
        again = self.open()
        self.assertEqual([app["name"] for app in again.list()], ["gallery", "wiki"])
        self.assertTrue(again.verify("wiki", token))

    def test_names_are_checked_and_unique(self):
        """An invalid name, or one taken, is refused."""
        self.apps.add("wiki")
        for name in ("wiki", "Wiki", "", "a:b", "a b", "x" * 65, None):
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    self.apps.add(name)
        self.assertEqual([app["name"] for app in self.apps.list()], ["wiki"])

    def test_remove(self):
        """remove() stops the token passing before release runs, returns what release returns and deletes the row;
        an app there isn't is NotFound."""
        token = self.apps.add("wiki")["token"]
        seen = []

        def release():
            seen.append(self.apps.verify("wiki", token))
            return 3

        self.assertEqual(self.apps.remove("wiki", release), 3)
        self.assertEqual(seen, [False])
        self.assertNotIn("wiki", self.apps)
        self.assertEqual(state_rows(self.state, "SELECT name FROM apps"), [])
        self.assertEqual(self.open().list(), [])
        with self.assertRaises(NotFound):
            self.apps.remove("wiki")

    def test_failed_release_keeps_the_app(self):
        """If release fails, the app is put back, token and row, to be removed again."""
        token = self.apps.add("wiki")["token"]

        def release():
            raise OSError("disk failed")

        with self.assertRaises(OSError):
            self.apps.remove("wiki", release)
        self.assertTrue(self.apps.verify("wiki", token))
        self.assertEqual(state_rows(self.state, "SELECT name FROM apps"), [("wiki",)])
        self.apps.remove("wiki")
        self.assertEqual(self.apps.list(), [])

    def test_failed_writes(self):
        """An add whose row can't be written isn't added; a remove whose delete fails keeps the app, and a later
        change writes the rows as they then are."""
        with failing_commits():
            with self.assertRaises(OSError):
                self.apps.add("wiki")
        self.assertEqual(self.apps.list(), [])
        token = self.apps.add("gallery")["token"]
        with failing_commits():
            with self.assertRaises(OSError):
                self.apps.remove("gallery")
        self.assertTrue(self.apps.verify("gallery", token))
        self.apps.add("notes")
        self.assertEqual(sorted(state_rows(self.state, "SELECT name FROM apps")), [("gallery",), ("notes",)])

    def test_invalid_rows_are_deleted(self):
        """A row with an invalid name, verifier or time is logged and deleted at load; the rest are kept."""
        self.apps.add("wiki")
        self.apps.close()
        for values in (("Bad", bytes(32), 1.0), ("short", b"x", 1.0), ("notime", bytes(32), "soon"),
                       ("ok", bytes(32), 2)):
            state_execute(self.state, "INSERT INTO apps (name, verifier, created) VALUES (?, ?, ?)", values)
        again = self.open()
        self.assertEqual([app["name"] for app in again.list()], ["ok", "wiki"])
        self.assertEqual(sorted(state_rows(self.state, "SELECT name FROM apps")), [("ok",), ("wiki",)])


class NodeAppsTest(unittest.TestCase):
    """An offline node's app facade."""

    def setUp(self):
        """An offline node holding one file."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.node = self.make_node()
        self.cid = Cid.parse(self.node.add_stream(io.BytesIO(b"x" * 3000), name="f")["cid"])
        self.other = Cid.parse(self.node.add_stream(io.BytesIO(b"y" * 3000), pin=False)["cid"])

    def make_node(self, **settings) -> RnfsNode:
        """An offline node on the test's directory."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, "node"), slow="no", **settings)
        config.validate()
        node = RnfsNode.offline(config, NODE_ID, NetClient(), FakeNetwork())
        self.addCleanup(node.stop)
        return node

    def test_add_list_remove(self):
        """app_add issues a token, app_ls lists the apps without tokens, app_rm removes one."""
        added = self.node.app_add("wiki")
        self.assertEqual(set(added), {"name", "created", "token"})
        self.assertEqual(self.node.app_ls(), [{"name": "wiki", "created": added["created"]}])
        self.assertEqual(self.node.app_rm("wiki"), {"released": 0})
        self.assertEqual(self.node.app_ls(), [])
        with self.assertRaises(NotFound):
            self.node.app_rm("wiki")
        with self.assertRaises(ValueError):
            self.node.app_add("Wiki")

    def test_remove_releases_every_pin_of_the_app(self):
        """app_rm releases app:<name> and every app:<name>:<key> pin, and no other source's (another app's, one
        whose name starts the same, the user's)."""
        self.node.app_add("wiki")
        self.node.app_add("wikis")
        storage = self.node.storage
        for source in (app_source("wiki"), app_source("wiki", "a"), app_source("wiki", "b:c")):
            storage.pin(self.other, source)
        storage.pin(self.cid, app_source("wiki", "a"))
        storage.pin(self.cid, app_source("wikis"))
        self.assertEqual(self.node.app_rm("wiki"), {"released": 4})
        self.assertEqual(storage.pins(), {self.cid: {USER, "app:wikis"}})
        self.assertEqual([app["name"] for app in self.node.app_ls()], ["wikis"])

    def test_apps_outlive_the_node(self):
        """An app added is there after a restart."""
        token = self.node.app_add("wiki")["token"]
        self.node.stop()
        again = self.make_node()
        self.assertTrue(again.apps.verify("wiki", token))


if __name__ == "__main__":
    unittest.main()
