"""Tests for the address book (rnfs/features/contacts.py): nicks, kinds, persistence, and nick targets and
node IDs end to end through the node facade, the local API (including jobs) and the CLI helpers."""
import http.client
import json
import os
import tempfile
import time
import unittest
from unittest import mock

import RNS

from rnfs import api
from rnfs.api import ApiServer
from rnfs.config import Config
from rnfs.content.cid import Cid
from rnfs.content.storage import USER
from rnfs.errors import NotFound
from rnfs.features import contacts
from rnfs.features.contacts import check_nick, is_nick, node_arg
from rnfs.node import RnfsNode

from tests.support import failing_commits, state_execute, state_rows, write_legacy
from tests.test_names import FakeNetwork
from tests.test_node import NetClient

A_ID, B_ID = bytes([0xA] * 16), bytes([0xB] * 16)

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the nodes' log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class NickTest(unittest.TestCase):
    """Nick syntax."""

    def test_valid(self):
        for nick in ("alice", "a", "bob-2", "x_y.z", "0day", "z" * 32, "b" * 31):
            self.assertEqual(check_nick(nick), nick)

    def test_invalid(self):
        cid = str(Cid.for_block("raw", b"x"))
        for nick in ("", "Alice", "a" * 33, "-a", ".", "..", "_x", "a/b", "a b", "ä", "ab" * 16, "0" * 32,
                     cid, None, 5, b"alice"):
            self.assertFalse(is_nick(nick), nick)
            with self.assertRaises(ValueError):
                check_nick(nick)

    def test_api_parse_target_accepts_nicks(self):
        self.assertEqual(api.parse_target("alice/docs/a.txt"), "alice/docs/a.txt")
        cid = str(Cid.for_block("raw", b"x"))
        self.assertEqual(api.parse_target(cid + "/x"), cid + "/x")
        for bad in ("Alice/x", "", "/x", "bnotacid" + "a" * 60):
            with self.assertRaises(ValueError):
                api.parse_target(bad)


class ContactsNodeTest(unittest.TestCase):
    """Owner A publishes a name over a directory; B has contacts for it and for A."""

    def setUp(self):
        """Two offline nodes; A provides the directory its name points at."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.net = NetClient()
        self.net.sender = B_ID
        self.a = self.make_node("a", A_ID)
        self.b = self.make_node("b", B_ID)
        self.b.routing.heard(A_ID)
        tree = os.path.join(self.tmp.name, "tree")
        os.makedirs(os.path.join(tree, "docs"))
        with open(os.path.join(tree, "docs", "report.txt"), "wb") as f:
            f.write(b"quarterly report")
        self.root = Cid.parse(self.a.add_path(tree)["cid"])
        self.a.records.store(self.root.key, A_ID)
        self.name = bytes.fromhex(self.a.name_create("site")["name"])
        self.a.name_publish("site", str(self.root))
        # B heard A's announce
        self.b.names.offer(self.name, 1, self.root, time.time())
        self.book = self.b.feature("contacts")

    def make_node(self, label: str, node_id: bytes) -> RnfsNode:
        """An offline node with its own directory."""
        config = Config(rnfs_dir=os.path.join(self.tmp.name, label), slow="no", lookup_timeout=5, path_timeout=0.2)
        node = RnfsNode.offline(config, node_id, self.net, FakeNetwork())
        self.net.nodes[node_id] = node
        self.addCleanup(node.stop)
        return node

    def reopen(self) -> contacts.ContactsFeature:
        """B's address book loaded again from its state, closed when the test ends."""
        book = contacts.ContactsFeature(self.b)
        self.addCleanup(book.close)
        return book

    def rows(self) -> list[tuple]:
        """What B's contacts table holds, by nick."""
        return state_rows(self.b.config.state_dir, "SELECT nick, kind, id FROM contacts ORDER BY nick")

    def test_add_list_remove_persist(self):
        """Contacts are saved as added and removed, and loaded again."""
        self.book.add("alice", self.name.hex(), "name")
        self.book.add("anode", "<" + A_ID.hex().upper() + ">", "node")
        self.assertEqual(self.book.list(), [{"nick": "alice", "kind": "name", "id": self.name.hex()},
                                            {"nick": "anode", "kind": "node", "id": A_ID.hex()}])
        self.assertEqual(self.book.lookup("anode", "node"), A_ID)
        self.assertIsNone(self.book.lookup("anode", "lxmf"))
        self.assertIsNone(self.book.lookup("nobody"))
        self.assertIsNone(self.book.lookup("Bad Nick"))
        self.assertEqual(self.reopen().list(), self.book.list())
        self.book.remove("anode")
        with self.assertRaises(NotFound):
            self.book.remove("anode")
        self.assertEqual(self.rows(), [("alice", "name", self.name)])

    def test_invalid_input(self):
        for nick, ident, kind in (("Alice", self.name.hex(), "name"), ("alice", "xyz", "name"),
                                  ("alice", self.name.hex()[:-2], "name"), ("alice", self.name.hex(), "email")):
            with self.assertRaises(ValueError):
                self.book.add(nick, ident, kind)

    def test_malformed_file_entries_skipped(self):
        """contacts.json's malformed entries are skipped when it is imported."""
        write_legacy(self.b.config.state_dir, "contacts.json",
                     {"version": 1, "contacts": {"ok": {"kind": "node", "id": A_ID.hex()},
                                                 "BAD": {"kind": "node", "id": A_ID.hex()},
                                                 "worse": {"kind": "spaceship", "id": A_ID.hex()},
                                                 "worst": "nope"}})
        self.assertEqual([c["nick"] for c in self.reopen().list()], ["ok"])

    def test_malformed_rows_deleted(self):
        """Rows that aren't a valid contact, and any past MAX_CONTACTS, are skipped at load and deleted."""
        self.book.add("ok", A_ID.hex(), "node")
        self.book.add("zed", B_ID.hex(), "node")
        for nick, kind, ident in (("BAD", "node", A_ID), ("worse", "spaceship", A_ID), ("short", "node", b"\x01"),
                                  ("text", "node", A_ID.hex()), ("ab" * 16, "node", A_ID)):
            state_execute(self.b.config.state_dir, "INSERT INTO contacts (nick, kind, id) VALUES (?, ?, ?)",
                          (nick, kind, ident))
        with mock.patch.object(contacts, "MAX_CONTACTS", 1):
            self.assertEqual([c["nick"] for c in self.reopen().list()], ["ok"])
        self.assertEqual(self.rows(), [("ok", "node", A_ID)])

    def test_failed_save_is_written_with_the_next_change(self):
        """A change whose commit failed raises and stays in memory, as before; the next change saves it too."""
        with failing_commits(), self.assertRaises(OSError):
            self.book.add("anode", A_ID.hex(), "node")
        self.assertEqual(self.book.lookup("anode", "node"), A_ID)
        self.assertEqual(self.rows(), [])
        self.book.add("bnode", B_ID.hex(), "node")
        self.assertEqual(self.rows(), [("anode", "node", A_ID), ("bnode", "node", B_ID)])
        with failing_commits(), self.assertRaises(OSError):
            self.book.remove("anode")
        self.book.remove("bnode")
        self.assertEqual(self.rows(), [])

    def test_kind_detection(self):
        self.assertEqual(self.book.add("alice", self.name.hex())["kind"], "name")  # a record B holds
        self.assertEqual(self.book.add("anode", A_ID.hex())["kind"], "node")  # in B's routing table
        self.assertEqual(self.a.feature("contacts").add("me", self.name.hex())["kind"], "name")  # own name
        with self.assertRaises(ValueError):
            self.book.add("mystery", bytes([7] * 16).hex())

    def test_kind_from_rns_known_destination(self):
        identity = RNS.Identity()
        lxmf = RNS.Destination.hash(identity, "lxmf", "delivery")
        with mock.patch.object(RNS.Identity, "recall", lambda ident, *args, **kwargs: identity
                               if ident == lxmf else None):
            self.assertEqual(self.book.add("carol", lxmf.hex())["kind"], "lxmf")

    def test_nick_targets_through_the_facade(self):
        self.book.add("alice", self.name.hex(), "name")
        self.book.add("anode", A_ID.hex(), "node")
        entries = self.b.ls("alice")
        self.assertEqual([e["name"] for e in entries], ["docs"])
        self.assertEqual(b"".join(self.b.read("alice/docs/report.txt")), b"quarterly report")
        self.assertEqual(self.b.stat("alice/docs")["cid"], self.b.info(f"{self.root}/docs")["cid"])
        out = os.path.join(self.tmp.name, "out.txt")
        self.b.get("alice/docs/report.txt", out)
        with open(out, "rb") as f:
            self.assertEqual(f.read(), b"quarterly report")
        self.assertEqual(self.b.pin_add("alice")["cid"], str(self.root))
        self.assertIn(USER, self.b.storage.pins()[self.root])
        self.b.name_create("mirror")
        self.assertEqual(self.b.name_publish("mirror", "alice")["cid"], str(self.root))
        with self.assertRaises(ValueError):
            self.b.ls("anode")  # a node, not a name
        with self.assertRaises(ValueError):
            self.b.ls("nobody")
        with self.assertRaises(ValueError):
            self.b.info("alice", for_gateway=True)  # the gateway never reaches the address book

    def test_unresolvable_name_contact(self):
        self.book.add("ghost", bytes([9] * 16).hex(), "name")
        with self.assertRaises(NotFound):
            self.b.ls("ghost")

    def test_resolve_id(self):
        self.book.add("anode", A_ID.hex(), "node")
        self.assertEqual(self.book.resolve_id(A_ID.hex(), "node"), A_ID)
        self.assertEqual(self.book.resolve_id("anode", "node"), A_ID)
        for text, kind in (("anode", "name"), ("nobody", "node"), ("Bad!", "node")):
            with self.assertRaises(ValueError):
                self.book.resolve_id(text, kind)


class ContactsApiTest(ContactsNodeTest.__base__):
    """Nick targets and contacts through the local API of an offline node, jobs included."""

    setUp = ContactsNodeTest.setUp
    make_node = ContactsNodeTest.make_node

    def start_api(self):
        """An API server for B, with B's job manager running."""
        self.b.feature("jobs").start()
        server = ApiServer(self.b, 0)
        server.start()
        self.addCleanup(server.stop)
        self.server = server

    def call(self, method: str, path: str, body=None):
        """(status, decoded JSON) of an API request."""
        connection = http.client.HTTPConnection("127.0.0.1", self.server.port, timeout=30)
        headers = {"Host": f"127.0.0.1:{self.server.port}", "X-Rnfs-Api": self.server.token}
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        connection.request(method, path, body=data, headers=headers)
        response = connection.getresponse()
        raw = response.read()
        connection.close()
        try:
            return response.status, json.loads(raw)
        except ValueError:
            return response.status, raw

    def test_contacts_endpoints(self):
        self.start_api()
        status, contact = self.call("POST", "/api/contacts/add", {"nick": "alice", "id": self.name.hex()})
        self.assertEqual((status, contact["kind"]), (200, "name"))
        self.assertEqual(self.call("GET", "/api/contacts/get?nick=alice")[1]["id"], self.name.hex())
        self.assertEqual(self.call("GET", "/api/contacts")[1]["contacts"][0]["nick"], "alice")
        self.assertEqual(self.call("GET", "/api/contacts/get?nick=bob")[0], 404)
        self.assertEqual(self.call("POST", "/api/contacts/add", {"nick": "x", "id": "zz"})[0], 400)
        self.assertEqual(self.call("POST", "/api/contacts/add", {"nick": "x", "id": A_ID.hex(), "kind": 5})[0], 400)
        self.assertEqual(self.call("POST", "/api/contacts/rm", {"nick": "alice"})[0], 200)

    def test_nick_targets_end_to_end(self):
        self.book.add("alice", self.name.hex(), "name")
        self.start_api()
        status, listing = self.call("GET", "/api/ls?target=alice/docs")
        self.assertEqual((status, [e["name"] for e in listing["entries"]]), (200, ["report.txt"]))
        self.assertEqual(self.call("GET", "/api/cat?target=alice/docs/report.txt"), (200, b"quarterly report"))
        self.assertEqual(self.call("GET", "/api/stat?target=alice")[0], 200)
        self.assertEqual(self.call("GET", "/api/info?target=alice")[1]["cid"], str(self.root))
        out = os.path.join(self.tmp.name, "via-api.txt")
        self.assertEqual(self.call("POST", "/api/get", {"target": "alice/docs/report.txt", "output": out})[0], 200)
        archive = os.path.join(self.tmp.name, "a.rnfs")
        self.assertEqual(self.call("POST", "/api/export", {"target": "alice", "output": archive})[0], 200)
        file_cid = self.b.info("alice/docs/report.txt")["cid"]
        status, made = self.call("POST", "/api/dir/set", {"target": "alice/docs", "name": "copy.txt", "cid": file_cid})
        self.assertEqual(status, 200)
        self.assertEqual(self.call("POST", "/api/dir/rm", {"target": "alice/docs/report.txt"})[0], 200)
        self.b.name_create("mirror")
        self.assertEqual(self.call("POST", "/api/name/publish", {"alias": "mirror", "target": "alice"})[0], 200)
        self.assertEqual(self.call("GET", "/api/ls?target=nobody")[0], 400)
        # Jobs: the nick is resolved when the job is queued, so the job transfers a fixed root
        status, job = self.call("POST", "/api/jobs", {"kind": "pin", "target": "alice/docs"})
        self.assertEqual(status, 200, job)
        self.assertTrue(job["target"].startswith(str(self.root)))
        done = self.call("GET", f"/api/jobs/get?id={job['id']}&wait=20")[1]
        self.assertEqual(done["state"], "done", done)
        out2 = os.path.join(self.tmp.name, "via-job.txt")
        status, job = self.call("POST", "/api/jobs", {"kind": "get", "target": "alice/docs/report.txt",
                                                      "output": out2})
        self.assertEqual(status, 200, job)
        self.assertEqual(self.call("GET", f"/api/jobs/get?id={job['id']}&wait=20")[1]["state"], "done")
        with open(out2, "rb") as f:
            self.assertEqual(f.read(), b"quarterly report")
        self.assertEqual(self.call("POST", "/api/jobs", {"kind": "pin", "target": "nobody"})[0], 400)


class NodeArgTest(unittest.TestCase):
    """The CLI's node-ID arguments accept a node contact's nick."""

    def test_hex_passes_through_without_the_daemon(self):
        with mock.patch("rnfs.cli.client", side_effect=AssertionError("no API call expected")):
            self.assertEqual(node_arg(None, "ab" * 16), "ab" * 16)
            self.assertIsNone(node_arg(None, None))
            self.assertEqual(node_arg(None, "Not A Nick!"), "Not A Nick!")  # the API reports it

    def test_nick_looked_up(self):
        api_client = mock.Mock()
        api_client.get.return_value = {"nick": "relay", "kind": "node", "id": "cd" * 16}
        with mock.patch("rnfs.cli.client", return_value=api_client):
            self.assertEqual(node_arg(None, "relay"), "cd" * 16)
        api_client.get.assert_called_once_with("/api/contacts/get", nick="relay")
        api_client.get.return_value = {"nick": "alice", "kind": "name", "id": "cd" * 16}
        with mock.patch("rnfs.cli.client", return_value=api_client):
            with self.assertRaises(SystemExit):
                node_arg(None, "alice")


if __name__ == "__main__":
    unittest.main()
