"""Tests for app tokens (rnfs/apps.py): the registry in the state database, and the node's app_add, app_ls and app_rm,
which releases the app's pins."""
import io
import os
import tarfile
import tempfile
import unittest

import RNS

from rnfs import cli, statedb
from rnfs.api import ApiServer
from rnfs.apiclient import PIN_KEY_HEADER, ApiConnection, BadRequest, api_proof, app_proof_key, app_verifier
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

    def test_lxmf_pins_stay_until_an_app_called_lxmf_is_removed(self):
        """The app:lxmf:<sender> pins the LXMF feature of rnfs before 0.2.0 left (its upgrade rewrote lxmf:<sender>)
        stay through restarts, as no part of rnfs releases them since LXMF left it: an app called lxmf holds them.
        Adding that app (an LXMF app adopting them, or a user wanting them gone) is never refused now, and removing
        it releases them, and only them."""
        sender = "a0" * 16
        source = app_source("lxmf", sender)
        self.node.storage.pin(self.other, source)
        self.node.storage.pin(self.cid, source)
        self.node.stop()
        self.node = self.make_node()
        self.assertEqual(self.node.storage.pins(), {self.cid: {USER, source}, self.other: {source}})
        self.node.app_add("lxmf")
        self.node.stop()
        self.node = self.make_node()
        self.assertEqual(self.node.storage.pinned_by(source), {self.cid, self.other})  # the app's now: kept
        self.assertEqual(self.node.app_rm("lxmf"), {"released": 2})
        self.assertEqual(self.node.storage.pins(), {self.cid: {USER}})
        self.node.stop()
        self.assertEqual(self.make_node().storage.pins(), {self.cid: {USER}})



class AppPinsTest(unittest.TestCase):
    """An offline node behind its API: the routes that pin or release for the caller use the app's source when an
    app's token calls (app:<name>, or app:<name>:<key> inside pin_key), "user" for the CLI's."""

    def setUp(self):
        """The node, its API, a connection with the CLI's token and one as app "wiki"."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        config = Config(rnfs_dir=os.path.join(self.tmp.name, "node"), slow="no")
        config.validate()
        self.node = RnfsNode.offline(config, NODE_ID, NetClient(), FakeNetwork())
        self.addCleanup(self.node.stop)
        self.server = ApiServer(self.node, 0)
        self.server.start()
        self.addCleanup(self.server.stop)
        token_file = os.path.join(self.tmp.name, "api_token")
        cli.write_token(token_file, self.server.token)
        self.cli = ApiConnection(self.tmp.name, port=self.server.port, token_file=token_file)
        self.token = self.node.app_add("wiki")["token"]
        self.app = ApiConnection(self.tmp.name, port=self.server.port, app="wiki", token=self.token)

    def sources(self, cid: str) -> set[str]:
        """The sources pinning cid now."""
        return self.node.storage.pins().get(Cid.parse(cid), set())

    def test_every_pinning_route_uses_the_apps_source(self):
        """add (a path, a stream, a tar), dir set, dir rm, pin add, import (a path, a stream) all pin app:wiki for
        the app, app:wiki:<key> inside pin_key; the CLI's still pin user."""
        path = os.path.join(self.tmp.name, "file.txt")
        with open(path, "wb") as f:
            f.write(b"from a path")
        tar = io.BytesIO()
        with tarfile.open(fileobj=tar, mode="w") as archive:
            info = tarfile.TarInfo("inside.txt")
            info.size = 3
            archive.addfile(info, io.BytesIO(b"tar"))
        directory = self.app.add_stream(b"entry", name="f")["cid"]
        child = self.app.add_stream(b"child")["cid"]
        cached = self.cli.add_stream(b"cached", pin=False)["cid"]
        exported = os.path.join(self.tmp.name, "out.rnfs-archive")
        self.node.export(child, exported)
        made = {"add_path": self.app.add_path(path)["cid"], "add_stream": directory, "child": child,
                "add_tar": self.app.add_tar(tar.getvalue())["cid"],
                "dir_set": self.app.dir_set(directory, "g", child)["cid"],
                "dir_rm": self.app.dir_rm(f"{directory}/f")["cid"], "pin_add": self.app.pin_add(cached)["cid"]}
        for what, cid in made.items():
            self.assertEqual(self.sources(cid), {"app:wiki"}, what)
        self.assertEqual(self.app.import_archive(exported)["roots"], [child])
        _, chunks = self.node.export_chunks(directory)
        archive_bytes = b"".join(chunks)
        with self.app.pin_key("photos:2026"):
            keyed = self.app.add_stream(b"keyed")["cid"]
            self.assertEqual(self.app.import_stream(archive_bytes)["roots"], [directory])
        self.assertEqual(self.sources(keyed), {"app:wiki:photos:2026"})
        self.assertEqual(self.sources(directory), {"app:wiki", "app:wiki:photos:2026"})
        self.assertEqual(self.sources(self.cli.add_stream(b"cli")["cid"]), {USER})
        listed = {pin["cid"]: pin["sources"] for pin in self.cli.pin_ls()}
        self.assertEqual(listed[keyed], ["app:wiki:photos:2026"])

    def test_pin_rm_releases_only_the_callers_source(self):
        """An app's pin rm releases its own source (its pin key's inside pin_key), never user's or another app's;
        the CLI's releases only user's."""
        cid = self.cli.add_stream(b"shared")["cid"]
        self.app.pin_add(cid)
        with self.app.pin_key("g"):
            self.app.pin_add(cid)
        other = self.node.app_add("notes")["token"]
        notes = ApiConnection(self.tmp.name, port=self.server.port, app="notes", token=other)
        notes.pin_add(cid)
        self.assertEqual(self.sources(cid), {USER, "app:wiki", "app:wiki:g", "app:notes"})
        self.app.pin_rm(cid)
        self.assertEqual(self.sources(cid), {USER, "app:wiki:g", "app:notes"})
        with self.assertRaises(NotFound):
            self.app.pin_rm(cid)  # app:wiki's is gone: neither user's nor another's is touched
        with self.app.pin_key("g"):
            self.app.pin_rm(cid)
        self.cli.pin_rm(cid)
        self.assertEqual(self.sources(cid), {"app:notes"})
        with self.assertRaises(NotFound):
            self.cli.pin_rm(cid)  # the CLI's pin rm releases user's only

    def test_pin_keys_are_checked(self):
        """A pin key with the CLI's token is refused (400), as is one with characters a key can't have."""
        cid = self.cli.add_stream(b"x", pin=False)["cid"]
        with self.assertRaises(BadRequest):
            self.cli.request("POST", "/api/pin/add", json_body={"target": cid}, extra_headers={PIN_KEY_HEADER: "g"})
        with self.assertRaises(ValueError):
            with self.cli.pin_key("g"):
                pass
        for key in ("a b", "k" * 129, "a/b"):
            with self.subTest(key=key), self.assertRaises(BadRequest), self.app.pin_key(key):
                self.app.pin_add(cid)
        self.assertEqual(self.sources(cid), set())

    def test_client_as_an_app(self):
        """rnfs.Client as an app: pin and unpin use its source, pin_key a group."""
        import rnfs
        cid = self.cli.add_stream(b"for the client", pin=False)["cid"]
        with rnfs.Client(self.tmp.name, port=self.server.port, app="wiki", token=self.token) as client:
            client.pin(cid)
            with client.pin_key("k"):
                client.pin(cid)
            self.assertEqual(self.sources(cid), {"app:wiki", "app:wiki:k"})
            client.unpin(cid)
            self.assertEqual(self.sources(cid), {"app:wiki:k"})
            with self.assertRaises(ValueError):
                with rnfs.Client(self.tmp.name, port=self.server.port).pin_key("k"):
                    pass

    def test_jobs_keep_the_source(self):
        """Pin, add and import jobs queued with an app's token pin with its source (its pin key's too), saved in
        the job's record so a resumed job pins the same; app rm cancels the app's queued jobs."""
        jobs = self.node.feature("jobs")
        cid = self.cli.add_stream(b"job", pin=False)["cid"]
        with self.app.pin_key("k"):
            queued = self.app.submit_job("pin", cid)
        self.assertEqual(queued["source"], "app:wiki:k")
        self.assertNotIn("source", self.cli.submit_job("pin", cid))  # a user job's record is as before
        record = statedb.load(state_rows(self.node.config.state_dir, "SELECT record FROM jobs WHERE id = ?",
                                         (queued["id"],))[0][0])
        self.assertEqual(record["source"], "app:wiki:k")
        jobs.start()
        self.assertEqual(self.app.job(queued["id"], wait=10)["state"], "done")
        self.assertEqual(self.sources(cid), {USER, "app:wiki:k"})
        jobs.stop()
        held = self.app.submit_job("pin", cid)
        self.assertEqual(self.node.app_rm("wiki"), {"released": 1})
        self.assertEqual(self.cli.job(held["id"])["state"], "cancelled")
        self.assertEqual(self.sources(cid), {USER})

    def test_job_sources_are_checked(self):
        """A job's source is user's or an app's, and only pin, add and import jobs have one (a saved record too)."""
        from rnfs.features.jobs import Job
        for kind, source in (("pin", "folder"), ("pin", "app:Bad"), ("get", "app:wiki")):
            record = {"id": "0123abcd", "kind": kind, "target": str(Cid.for_block("raw", b"x")),
                      "output": None if kind == "pin" else os.path.abspath("out"), "state": "queued",
                      "source": source}
            with self.subTest(kind=kind, source=source), self.assertRaises(ValueError):
                Job.from_record(record)
        record = {"id": "0123abcd", "kind": "pin", "target": str(Cid.for_block("raw", b"x")), "output": None,
                  "state": "queued", "source": "app:wiki:k"}
        self.assertEqual(Job.from_record(record).to_record()["source"], "app:wiki:k")
        del record["source"]
        job = Job.from_record(record)
        self.assertEqual((job.source, "source" in job.to_record()), (USER, False))

    def test_pins_of_an_app_removed_meanwhile_are_released(self):
        """A request let in before its app was removed doesn't leave a pin: the facade releases it and says so."""
        cid = self.cli.add_stream(b"late", pin=False)["cid"]
        self.node.app_rm("wiki")
        with self.assertRaises(NotFound):
            self.node.pin_add(cid, source="app:wiki")
        self.assertEqual(self.sources(cid), set())
        for source in ("folder", "name:x", "remote:" + "00" * 16, "app:"):
            with self.subTest(source=source), self.assertRaises(ValueError):
                self.node.pin_add(cid, source=source)


if __name__ == "__main__":
    unittest.main()
