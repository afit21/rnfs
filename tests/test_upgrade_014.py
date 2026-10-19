"""Upgrading from rnfs 0.1.4: the current code started on an rnfs directory exactly as 0.1.4 wrote it
(tests/fixtures/rnfs-0.1.4, every one of its 17 JSON state files populated; see its README) holds every record.

Each store's state in memory is compared with the fixture's files read here with plain json, so the expectation
comes from the fixture itself. The only differences allowed are those the 0.1.4 load made too: outbox messages
not finished are failed (LXMF doesn't persist its queue), and the node's own provider records are stored again
(the publisher records itself for everything it provides); and 0.2.0's own: the departed and evicted publishers
providers.json remembers aren't imported, and the newcomer pool's records are no longer pooled (0.2.0's record
budget and storage tiers replaced both). Then: each file is kept as <name>.migrated, none is
set aside as damaged, the migrated table counts each file's records, a second start changes nothing, and a
restart keeps everything.

The clock is the fixture's T0 plus a day (generate.py), so no record has expired."""
import functools
import importlib.util
import io
import json
import os
import shutil
import tempfile
import time
import unittest
from unittest import mock

import RNS

from rnfs import node as node_module
from rnfs import statedb
from rnfs.config import Config
from rnfs.content import builder
from rnfs.content.blockstore import matches
from rnfs.content.cid import Cid
from rnfs.content.storage import upgraded_source
from rnfs.features import lxmf
from rnfs.node import RnfsNode
from tests.support import FakeClock, pinned_entries, state_rows
from tests.test_names import FakeNetwork
from tests.test_node import NetClient

FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "rnfs-0.1.4")
T0 = 1792054800.0  # the fake clock generate.py wrote the fixture at (records span T0 - 3 days to T0 + 1 hour)
NOW = T0 + 86400.0
NODE_ID = bytes.fromhex("4e0de1d04e0de1d04e0de1d04e0de1d0")  # generate.py's NODE_ID
# generate.py's SETTINGS: nothing in the fixture is filtered out by configuration. max_records is retired in 0.2.0:
# start() leaves it out with a warning, as a start does with an rnfs.toml holding it
SETTINGS = {"remote_pin_identities": ["0123456789abcdef0123456789abcdef"],
            "lxmf_auto_fetch_from": ["a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"], "max_records": 40, "slow": "no",
            "path_timeout": 2, "lookup_timeout": 2}
LEGACY = [item.file for item in statedb.LEGACY]
UNFINISHED_ERROR = "the daemon stopped before it was delivered"

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the log while these tests run."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


def fixture(name: str):
    """A state file of the fixture, as plain JSON."""
    with open(os.path.join(FIXTURE, "state", name), encoding="utf-8") as f:
        return json.load(f)


def _content():
    """The fixture's fixture_content module: the files 0.1.4 added to it."""
    spec = importlib.util.spec_from_file_location("fixture_content", os.path.join(FIXTURE, "fixture_content.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FlatBlockStore:
    """Blocks as rnfs 0.1.6 and earlier stored them: one file per CID directly in the blocks directory,
    blocks/<cid>. The upgrade tests write their blocks so, rather than through the current BlockStore, so that
    they start on a directory exactly as those versions left it."""

    def __init__(self, directory: str):
        """A store writing into directory (created if missing)."""
        self.directory = directory
        os.makedirs(directory, exist_ok=True)

    def path(self, cid: Cid) -> str:
        """Where 0.1.6 stored the block for cid."""
        return os.path.join(self.directory, str(cid))

    def put(self, cid: Cid, block: bytes):
        """Store block under cid, as 0.1.6's BlockStore.put left it (its temporary file renamed away)."""
        if not matches(cid, block):
            raise ValueError(f"block does not match {cid}")
        with open(self.path(cid), "wb") as f:
            f.write(block)


def make_blocks(blocks_dir: str, scratch: str) -> set[Cid]:
    """Store in blocks_dir, in 0.1.6's flat layout (FlatBlockStore), the blocks of everything 0.1.4 added to the
    fixture, built by the current code (the fixture keeps no blocks); the roots built. The CIDs must be those
    0.1.4 recorded, as no CID changed."""
    content, store, roots = _content(), FlatBlockStore(blocks_dir), set()
    for data, name in content.STREAMS:
        roots.add(builder.add_stream(io.BytesIO(data), store.put, len(data), name).cid)

    def write(path: str, tree: dict):
        """Create the directory path holding tree."""
        os.makedirs(path)
        for entry, value in tree.items():
            if isinstance(value, dict):
                write(os.path.join(path, entry), value)
            else:
                with open(os.path.join(path, entry), "wb") as f:
                    f.write(value)

    for n, tree in enumerate(content.TREES):
        write(os.path.join(scratch, f"tree-{n}"), tree)
        roots.add(builder.add_path(os.path.join(scratch, f"tree-{n}"), store.put)[0].cid)
    notes = os.path.join(scratch, content.NOTES[0])
    with open(notes, "wb") as f:
        f.write(content.NOTES[1])
    roots.add(builder.add_path(notes, store.put)[0].cid)
    return roots


def cids(texts) -> list[Cid]:
    """CIDs from their text."""
    return [Cid.parse(text) for text in texts]


def unfinished(message: dict) -> bool:
    """An outbox message not finished when the daemon stopped (as LxmfFeature tells them at start)."""
    return message["state"] not in lxmf.FINAL_STATES and not (message["method"] == "propagated"
                                                                and message["state"] == "sent")


def loadable_jobs() -> list[dict]:
    """The fixture's jobs this platform loads: a get's output must be absolute, and /home/... isn't on Windows
    (Python 3.13 and later), so there 0.1.4 and the import both skip it."""
    return [job for job in fixture("jobs.json")["jobs"] if job["output"] is None or os.path.isabs(job["output"])]


class UpgradeTest(unittest.TestCase):
    """A copy of the fixture under a temporary directory, started on with the clock at NOW."""

    def setUp(self):
        """Copy the fixture; fake the wall clock, also for the stores that take it as an argument."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = os.path.join(self.tmp.name, "rnfs")
        for part in ("state", "names", "files"):
            shutil.copytree(os.path.join(FIXTURE, part), os.path.join(self.root, part))
        os.makedirs(os.path.join(self.tmp.name, "scratch"))
        self.built = make_blocks(os.path.join(self.root, "blocks"), os.path.join(self.tmp.name, "scratch"))
        self.state = os.path.join(self.root, "state")
        self.clock = FakeClock(NOW)
        # ProviderStore, Publisher and NameManager bind time.time as a default argument, so patching it isn't enough
        for patch in (mock.patch.object(time, "time", self.clock),
                      *(mock.patch.object(node_module, cls, functools.partial(getattr(node_module, cls),
                                                                              clock=self.clock))
                        for cls in ("ProviderStore", "Publisher", "NameManager"))):
            patch.start()
            self.addCleanup(patch.stop)

    def start(self) -> RnfsNode:
        """An offline node on the copy (stopped before the directory is removed)."""
        config = Config.combine(self.root, SETTINGS)
        self.assertEqual([warning.split(":")[0] for warning in config.warnings],
                         ["ignoring max_records in rnfs.toml"])
        node = RnfsNode.offline(config, NODE_ID, NetClient(), FakeNetwork())
        self.addCleanup(node.stop)
        return node

    def tables(self) -> dict[str, list[tuple]]:
        """Every row of every table of the state database, sorted."""
        names = [row[0] for row in state_rows(self.state, "SELECT name FROM sqlite_master WHERE type = 'table'")]
        return {name: sorted(state_rows(self.state, f"SELECT * FROM {name}"), key=repr) for name in names}

    # Each store's state against the fixture

    def check_pins(self, node: RnfsNode):
        """Every pin with every source, and each complete: the current code built the same CIDs (make_blocks). The
        two lxmf:<address> pins (an auto-fetch sender's) become app:lxmf:<address> since rnfs 0.2.0, the LXMF
        wrapper app's source (storage.upgraded_source, as the schema step "lxmf_app_pins" for the pins table)."""
        expected = {Cid.parse(text): {upgraded_source(source) for source in sources}
                    for text, sources in fixture("pins.json")["pins"].items()}
        self.assertEqual(sum(source.startswith("app:lxmf:") for sources in expected.values() for source in sources), 2)
        self.assertEqual(node.storage.pins(), expected)
        roots = {Cid.parse(root) for root, _, _ in fixture("cache.json")["fetches"]}
        self.assertLessEqual(roots, self.built)
        for cid in expected:
            self.assertTrue(node.storage.is_complete(cid), cid)

    def check_cache(self, node: RnfsNode):
        """Every cached fetch, least recently used first, non-ASCII path names included."""
        expected = [(Cid.parse(root), tuple(names), Cid.parse(target))
                    for root, names, target in fixture("cache.json")["fetches"]]
        self.assertEqual(node.storage.cached_fetches(), expected)

    def check_pin_names(self, node: RnfsNode):
        """Every pin name, the lone surrogate of a name that isn't UTF-8 included."""
        expected = {Cid.parse(text): name for text, name in fixture("pin_names.json").items()}
        self.assertIn("\udcff", "".join(expected.values()))
        self.assertEqual({cid: node.pin_names.get(cid) for cid in expected}, expected)
        self.assertEqual(len(node.pin_names._names), len(expected))

    def check_fetched(self, node: RnfsNode):
        """The targets fetched for the user, which the roots strategy provides."""
        self.assertEqual(node._eligible_targets(), set(cids(fixture("fetched.json"))))

    def check_remote_pins(self, node: RnfsNode):
        """The remote pins waiting, in the order accepted."""
        self.assertEqual(node._load_remote_pins(), [(source, Cid.parse(text))
                                                    for source, text in fixture("remote_pins.json")])

    def check_invalid(self, node: RnfsNode):
        """The roots found invalid, oldest first."""
        self.assertEqual(list(node.fetcher._invalid), cids(fixture("invalid.json")))

    def check_published(self, node: RnfsNode):
        """Last rounds (CIDs and kept names' keys), earlier rounds' nodes and withdrawals queued, as written."""
        expected = fixture("published.json")
        self.assertTrue(any(item.startswith("key:") for item in expected["published"]))
        publisher = node.publisher
        self.assertEqual({str(item): [when, [n.hex() for n in nodes]]
                          for item, (when, nodes) in publisher._published.items()}, expected["published"])
        self.assertEqual({str(item): {n.hex(): when for n, when in nodes.items()}
                          for item, nodes in publisher._earlier.items() if nodes}, expected["earlier"])
        self.assertEqual({n.hex(): sorted(k.hex() for k in keys) for n, keys in publisher._withdrawals.items()
                          if keys}, expected["withdraw"])
        # What 0.1.4 provided is still provided: every published CID and key; and since 0.2.0 (provide_entries, on
        # by default) the files and folders inside its complete pins too, not published yet
        entries = pinned_entries(node.storage) - {Cid.parse(text) for text in expected["published"]
                                                   if not text.startswith("key:")}
        self.assertTrue(entries)
        self.assertEqual(publisher.provided_entries(), entries)
        self.assertEqual({str(c) for c in publisher.provided()} | {"key:" + k.hex() for k in publisher.provided_keys()},
                         set(expected["published"]) | {str(c) for c in entries})

    def check_providers(self, node: RnfsNode):
        """Every publisher's first-seen time and every record, those of 0.1.4's newcomer pool included (no longer
        pooled), each publisher with a standing built from them; the node's own records are stored again at start
        (last stored now), in their own tier, with one for each entry it provides since 0.2.0 (provide_entries). The
        departed and evicted publishers 0.1.4 remembered are not imported: 0.2.0 keeps no such memories (spec section
        6)."""
        expected = fixture("providers.json")
        records = node.records
        with records._lock:
            held, standings = records._merged()
        self.assertEqual({p.hex(): standing.first_seen for p, standing in standings.items()}, expected["publishers"])
        own = NODE_ID.hex()
        records_expected = {(k, p): [first, NOW if p == own else last] for k, p, first, last, _ in expected["records"]}
        entries = {(cid.key.hex(), own): [NOW, NOW] for cid in pinned_entries(node.storage)}
        self.assertTrue(set(entries) - set(records_expected))
        self.assertEqual({(k.hex(), p.hex()): [first, last] for (k, p), (first, last) in held.items()},
                         entries | records_expected)
        self.assertTrue(any(pooled for *_, pooled in expected["records"]))
        self.assertTrue(expected["departed"] and expected["evicted"])
        self.assertEqual(state_rows(self.state, "SELECT * FROM provider_departed UNION ALL"
                                                " SELECT * FROM provider_evicted"), [])
        for publisher, standing in standings.items():
            self.assertEqual(standing.records, sum(p == publisher for _, p in held), publisher.hex())
        self.assertEqual(records.standing(NODE_ID)["tier"], "own")
        records._check_consistency()

    def check_names(self, node: RnfsNode):
        """Names published (one imported, never published here) with their keys, and the records held."""
        expected = fixture("names.json")
        names = node.names
        self.assertEqual({alias: {"sequence": p.sequence, "cid": None if p.cid is None else str(p.cid),
                                  "imported": p.imported} for alias, p in names._published.items()},
                         expected["published"])
        self.assertEqual([entry["alias"] for entry in names.list()], sorted(expected["published"]))
        self.assertEqual({name.hex(): {"sequence": r.sequence, "cid": str(r.cid), "heard": r.heard}
                          for name, r in names._held.items()}, expected["held"])

    def check_floor(self, node: RnfsNode):
        """The floor of names resolved for the gateway, least recently resolved first, 2^64 - 1 included."""
        self.assertEqual([[name.hex(), sequence, at] for name, (sequence, at) in node.names._floor.items()],
                         fixture("name-floor.json")["floor"])

    def check_shared_folder(self, node: RnfsNode):
        """The shared folder's scanned entries."""
        self.assertEqual({name: [signature, str(cid)] for name, (signature, cid) in node.shared_folder._known.items()},
                         fixture("shared_folder.json")["entries"])

    def check_jobs(self, node: RnfsNode):
        """Every job as saved, in creation order: the finished get with its result and warnings (but see
        loadable_jobs), the failed pin, and the queued pin, queued again to resume."""
        expected = loadable_jobs()
        jobs = node.feature("jobs")._jobs
        self.assertEqual([job.to_record() for job in jobs.values()], expected)
        self.assertEqual({job.state for job in jobs.values()}, {job["state"] for job in expected})
        self.assertIn("queued", {job.state for job in jobs.values()})

    def check_keepers(self, node: RnfsNode):
        """Kept names and why, with their signed records (verified again at load), one with none yet."""
        expected = fixture("keepers.json")["names"]
        store = node.feature("keepers").store
        self.assertEqual({name.hex(): {"reasons": sorted(reasons),
                                       "record": [f.hex() for f in store._records[name].wire()]
                                       if name in store._records else None}
                          for name, reasons in store._reasons.items()}, expected)

    def check_follow(self, node: RnfsNode):
        """Followed names and the target pinned for each."""
        self.assertEqual({name.hex(): {"pinned": None if cid is None else str(cid)}
                          for name, cid in node.feature("follow")._pinned.items()}, fixture("follow.json")["names"])

    def check_contacts(self, node: RnfsNode):
        """Contacts of each kind."""
        self.assertEqual({nick: {"kind": kind, "id": ident.hex()}
                          for nick, (kind, ident) in node.feature("contacts")._contacts.items()},
                         fixture("contacts.json")["contacts"])

    def check_inbox(self, node: RnfsNode):
        """Every inbox message with its exact text (non-BMP characters included) and auto-fetch results, and
        the next id."""
        expected = fixture("lxmf_inbox.json")
        inbox = node.feature("lxmf").inbox
        self.assertEqual(inbox.entries(), expected["messages"])
        self.assertEqual(inbox._next_id, expected["next_id"])

    def check_outbox(self, node: RnfsNode):
        """Every outbox message with its exact text; those not finished are failed now."""
        expected = fixture("lxmf_outbox.json")
        messages = [{**message, "state": "failed", "error": UNFINISHED_ERROR} if unfinished(message) else message
                    for message in expected["messages"]]
        self.assertTrue(any(map(unfinished, expected["messages"])))
        outbox = node.feature("lxmf").outbox
        self.assertEqual(outbox.entries(), messages)
        self.assertEqual(outbox._next_id, expected["next_id"])

    def check_all(self, node: RnfsNode):
        """Every store's check."""
        for name in dir(self):
            if name.startswith("check_") and name != "check_all":
                with self.subTest(check=name):
                    getattr(self, name)(node)

    # Tests

    def test_pins(self):
        """pins.json: every source kind is kept, as names.json, follow.json and the inbox account for them."""
        self.check_pins(self.start())

    def test_cache(self):
        """cache.json: every record, in order."""
        self.check_cache(self.start())

    def test_pin_names(self):
        """pin_names.json: every name."""
        self.check_pin_names(self.start())

    def test_fetched(self):
        """fetched.json: every target."""
        self.check_fetched(self.start())

    def test_remote_pins(self):
        """remote_pins.json: every pin waiting."""
        self.check_remote_pins(self.start())

    def test_invalid(self):
        """invalid.json: every root."""
        self.check_invalid(self.start())

    def test_published(self):
        """published.json: every section."""
        self.check_published(self.start())

    def test_providers(self):
        """providers.json: every record, publisher, departure and eviction."""
        self.check_providers(self.start())

    def test_names(self):
        """names.json and the name keys: every name and record."""
        self.check_names(self.start())

    def test_floor(self):
        """name-floor.json: every entry."""
        self.check_floor(self.start())

    def test_shared_folder(self):
        """shared_folder.json: every entry."""
        self.check_shared_folder(self.start())

    def test_jobs(self):
        """jobs.json: every job."""
        self.check_jobs(self.start())

    def test_keepers(self):
        """keepers.json: every kept name and record."""
        self.check_keepers(self.start())

    def test_follow(self):
        """follow.json: every followed name."""
        self.check_follow(self.start())

    def test_contacts(self):
        """contacts.json: every contact."""
        self.check_contacts(self.start())

    def test_inbox(self):
        """lxmf_inbox.json: every message."""
        self.check_inbox(self.start())

    def test_outbox(self):
        """lxmf_outbox.json: every message."""
        self.check_outbox(self.start())

    def test_files_are_migrated(self):
        """Every state file is imported and kept as <name>.migrated, unchanged; none is set aside as damaged, and
        the migrated table counts each file's records."""
        self.start()
        for name in LEGACY:
            self.assertFalse(os.path.exists(os.path.join(self.state, name)), name)
            with open(os.path.join(self.state, name + statedb.MIGRATED_SUFFIX), "rb") as kept, \
                    open(os.path.join(FIXTURE, "state", name), "rb") as original:
                self.assertEqual(kept.read(), original.read(), name)
        self.assertEqual([name for name in os.listdir(self.state) if ".damaged" in name], [])
        published = fixture("published.json")
        names = fixture("names.json")
        counts = {
            "pins.json": sum(1 for sources in fixture("pins.json")["pins"].values() if sources),
            "cache.json": len(fixture("cache.json")["fetches"]),
            "pin_names.json": len(fixture("pin_names.json")),
            "fetched.json": len(fixture("fetched.json")),
            "remote_pins.json": len(fixture("remote_pins.json")),
            "invalid.json": len(fixture("invalid.json")),
            # Rows: one per item published, per (item, earlier node) and per (node, key withdrawn)
            "published.json": len(published["published"]) + sum(map(len, published["earlier"].values()))
            + sum(map(len, published["withdraw"].values())),
            "providers.json": len(fixture("providers.json")["records"]),
            "names.json": len(names["published"]) + len(names["held"]),
            "name-floor.json": len(fixture("name-floor.json")["floor"]),
            "shared_folder.json": len(fixture("shared_folder.json")["entries"]),
            "jobs.json": len(loadable_jobs()),
            "keepers.json": len(fixture("keepers.json")["names"]),
            "follow.json": len(fixture("follow.json")["names"]),
            "contacts.json": len(fixture("contacts.json")["contacts"]),
            "lxmf_inbox.json": len(fixture("lxmf_inbox.json")["messages"]),
            "lxmf_outbox.json": len(fixture("lxmf_outbox.json")["messages"]),
        }
        self.assertEqual(sorted(counts), sorted(LEGACY))
        self.assertTrue(all(counts.values()), counts)  # every file has records
        self.assertEqual(dict(state_rows(self.state, "SELECT file, records FROM migrated")), counts)

    def test_second_start_changes_nothing(self):
        """A second start imports nothing again and changes no row, nor anything in the state directory."""
        self.start().stop()
        tables, listing = self.tables(), sorted(os.listdir(self.state))
        self.start().stop()
        self.assertEqual(self.tables(), tables)
        self.assertEqual(sorted(os.listdir(self.state)), listing)

    def test_restart_keeps_everything(self):
        """Stopped and started again, the node holds every store's state as it did after the upgrade."""
        self.start().stop()
        self.check_all(self.start())


if __name__ == "__main__":
    unittest.main()
