"""Write the tests/fixtures/rnfs-0.1.4 fixture: an rnfs directory exactly as rnfs 0.1.4 writes it, every state
file populated (see README.md there).

Run it with the rnfs 0.1.4 source tree (git archive v0.1.4, which includes its tests/ package), never the current
one: it puts that tree first on sys.path and checks rnfs is imported from there. From the repository root:

    mkdir /tmp/rnfs-0.1.4 && git archive v0.1.4 | tar -x -C /tmp/rnfs-0.1.4
    python3 tests/fixtures/rnfs-0.1.4/generate.py /tmp/rnfs-0.1.4 /tmp/rnfs-fixture-out
    # then replace tests/fixtures/rnfs-0.1.4/{state,names,files} with those in
    # /tmp/rnfs-fixture-out/rnfs-0.1.4

Everything is done through 0.1.4's own objects: an offline RnfsNode (RnfsNode.offline, with the fakes of 0.1.4's
own tests: tests.test_node.NetClient, tests.test_names.FakeNetwork, tests.test_lxmf.FakeLxmf), its stores' public
methods where they exist, and otherwise their in-memory state set as their own code sets it, then saved by the
store's own save. The wall clock is a fake (time.time patched): records span three days before T0 to about an
hour after it, so the test knows how old every record is. Keys, signatures, job IDs and message hashes are
random, so each run writes different bytes.
"""
import io
import json
import os
import random
import shutil
import sys
from unittest import mock

V014, OUT = (os.path.abspath(arg) for arg in sys.argv[1:3])
SCRATCH = os.path.join(OUT, "scratch")  # directories add_path imports
sys.path.insert(0, V014)
sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)))  # fixture_content, next to this file

import RNS  # noqa: E402

import rnfs  # noqa: E402

assert rnfs.__file__.startswith(V014 + os.sep), rnfs.__file__
assert rnfs.__version__ == "0.1.4", rnfs.__version__

from rnfs.config import Config  # noqa: E402
from rnfs.content.cid import RAW, Cid  # noqa: E402
from rnfs.content.storage import remote_source  # noqa: E402
from rnfs.features import keepers as keepers_module  # noqa: E402
from rnfs.features.lxmf import Incoming, Link, encode_field  # noqa: E402
from rnfs.names import encode_record, name_for_identity  # noqa: E402
from rnfs.node import RnfsNode  # noqa: E402
from tests.test_lxmf import FakeLxmf  # noqa: E402
from tests.test_names import FakeNetwork  # noqa: E402
from tests.test_node import NetClient  # noqa: E402

import fixture_content as content  # noqa: E402

DAY, HOUR = 86400.0, 3600.0
T0 = 1792054800.0  # 2026-10-15 09:00 UTC, just after 0.1.4 was tagged: the fake wall clock's start
NODE_ID = bytes.fromhex("4e0de1d04e0de1d04e0de1d04e0de1d0")
REMOTE = "0123456789abcdef0123456789abcdef"  # an identity on remote_pin_identities
AUTO_SENDER = bytes.fromhex("a0a1a2a3a4a5a6a7a8a9aaabacadaeaf")  # LXMF sender on lxmf_auto_fetch_from
OTHER_SENDER = bytes.fromhex("b0b1b2b3b4b5b6b7b8b9babbbcbdbebf")  # an LXMF sender not trusted
LXMF_ADDRESS = bytes.fromhex("c0c1c2c3c4c5c6c7c8c9cacbcccdcecf")  # this node's LXMF address
PROPAGATION_NODE = bytes.fromhex("d0d1d2d3d4d5d6d7d8d9dadbdcdddedf")
PEERS = [bytes([0x10 + i]) * 16 for i in range(4)]  # DHT nodes published to
# Settings the test starts the node with too (none of them is written to rnfs.toml)
SETTINGS = {"remote_pin_identities": [REMOTE], "lxmf_auto_fetch_from": [AUTO_SENDER.hex()], "max_records": 40,
            "slow": "no", "path_timeout": 2, "lookup_timeout": 2}


class Clock:
    """The fake wall clock (time.time), moved by hand."""

    def __init__(self, now: float):
        """Start at now."""
        self.now = now

    def __call__(self) -> float:
        """The current time."""
        return self.now


def build(root: str):
    """Create and populate the rnfs directory root with an offline 0.1.4 node."""
    clock = Clock(T0)
    with mock.patch("time.time", clock):
        config = Config(rnfs_dir=root, **SETTINGS)
        config.validate()
        network = FakeNetwork()
        node = RnfsNode.offline(config, NODE_ID, NetClient(), network)
        # These took time.time as a default argument, before it was patched (they loaded nothing: a new directory)
        node.records.clock = node.publisher.clock = node.names._clock = clock
        try:
            populate(node, network, clock)
        finally:
            node.stop()


def providers_phase(node: RnfsNode, clock: Clock):
    """providers.json: other publishers' records, stored over the three days before T0 (the clock moves only
    forward, as the store clamps times in the future back to now): seniors in the main store, one departed,
    newcomers in the newcomer pool and one evicted from it. The node's own records are stored later, at T0, by
    the publisher (evicting records of the newest publisher outside the pool, as own records always get in)."""
    records = node.records
    records.rng = random.Random(14)
    clock.now = T0 - 3 * DAY
    publishers = [bytes([0x30 + i]) + bytes(15) for i in range(12)]

    def key(i: int) -> bytes:
        """A DHT key."""
        return bytes([0x80, i]) + bytes(14)

    for i in range(8):
        records.store(key(i), publishers[0])  # a senior
    for i in range(4, 10):
        records.store(key(i), publishers[1])
    records.store(key(20), publishers[2])  # departs
    clock.now = T0 - 1.5 * DAY
    records.withdraw(key(20), publishers[2])
    assert publishers[2] in records._departed
    filler, i = 3, 30
    while records._main_count < records.main_size:
        clock.now += 60
        if not records.store(key(i), publishers[filler]):
            filler += 1
        i += 1
    clock.now = T0 - HOUR
    newcomers = publishers[filler + 1:filler + 1 + records.pool_size + 1]
    for n, newcomer in enumerate(newcomers):
        clock.now += 60
        assert records.store(key(100 + n), newcomer)
    assert records._evicted and records._pool_count == records.pool_size, (records._evicted, records._pool_count)
    clock.now = T0
    return lambda: records.store(key(0), publishers[0])  # refreshed at the end: first stored 3 days before


def populate(node: RnfsNode, network: FakeNetwork, clock: Clock):
    """Every state file, through the node's stores."""
    refresh_senior = providers_phase(node, clock)
    storage, names = node.storage, node.names
    jobs, keepers, follow = node.feature("jobs"), node.feature("keepers"), node.feature("follow")
    contacts, lxmf = node.feature("contacts"), node.feature("lxmf")

    def tick(seconds: float = 60.0):
        """Let time pass."""
        clock.now += seconds

    def add(data: bytes, name: str | None = None, pin: bool = True) -> Cid:
        """add_stream (add of stdin, wrapped as name if given)."""
        tick()
        return Cid.parse(node.add_stream(io.BytesIO(data), name=name, pin=pin)["cid"])

    def add_tree(tree: dict, pin: bool) -> Cid:
        """add_path of a directory holding tree ({name: bytes or subtree})."""
        tick()
        directory = os.path.join(SCRATCH, f"tree-{clock.now:.0f}")

        def write(path: str, entries: dict):
            """Create path holding entries."""
            os.makedirs(path)
            for entry, value in entries.items():
                if isinstance(value, dict):
                    write(os.path.join(path, entry), value)
                else:
                    with open(os.path.join(path, entry), "wb") as f:
                        f.write(value)

        write(directory, tree)
        return Cid.parse(node.add_path(directory, pin=pin)["cid"])

    def child(root: Cid, *path: str) -> Cid:
        """The CID at root/path."""
        return Cid.parse(node.stat("/".join([str(root), *path]))["cid"])

    # pins.json: user, with pin_names.json
    user = add(*content.USER)  # pin name: the name it was wrapped as
    plain = add(*content.PLAIN)
    picture = add(*content.PICTURE)
    storage.pin(picture, "user")
    node.pin_names.record(picture, "/home/alice/Pictures/Fête à Montréal/photo \U0001f389.jpg")
    # add of a file whose name isn't UTF-8: its path holds a lone surrogate (os.fsdecode's surrogateescape)
    odd = add(*content.ODD)
    node.pin_names.record(odd, "/home/alice/Downloads/b\udcffd name.txt")

    # folder: a real file in files/, scanned
    os.makedirs(node.config.files_dir, exist_ok=True)
    with open(os.path.join(node.config.files_dir, content.NOTES[0]), "wb") as f:
        f.write(content.NOTES[1])
    tick()
    node.shared_folder.scan()
    node.publisher.refresh()

    # name:<alias>, names.json, a key in names/photos, keepers' own record
    album = add_tree(content.ALBUM, pin=False)
    names.create("photos")
    tick()
    node.name_publish("photos", str(album))
    imported = RNS.Identity()
    names.import_key("imported", imported.get_private_key())  # imported, never published here

    # remote:<hex>, and remote pins waiting (remote_pins.json)
    remote = add(*content.REMOTE_PIN, pin=False)
    storage.pin(remote, remote_source(REMOTE))
    waiting = [Cid.for_block(RAW, b"waiting %d" % i) for i in range(2)]
    with node._lock:
        node._remote_reserved[remote_source(REMOTE)] = {cid: None for cid in waiting}
        node._remote_pins_dirty = True
    node._save_remote_pins()

    # cache.json and fetched.json: add --no-pin records, a path into a directory with non-ASCII names
    docs = add_tree(content.DOCS, pin=False)
    storage.record_fetch(docs, ["日本語", "メモ.md"], child(docs, "日本語", "メモ.md"))
    storage.record_fetch(docs, ["Ελληνικά", "αρχείο.txt"], child(docs, "Ελληνικά", "αρχείο.txt"))
    node._note_fetched(child(docs, "Ελληνικά", "αρχείο.txt"))
    # a gateway fetch: cached, never noted as fetched for the user (so never provided)
    storage.record_fetch(docs, ["caf\u00e9.txt"], child(docs, "caf\u00e9.txt"))
    storage.touch(album, [])  # used again: most recently used
    tick()

    # invalid.json
    for i in range(3):
        node.fetcher.mark_invalid(Cid.for_block(RAW, b"invalid %d" % i))

    # keepers.json: a user-kept name with a record signed by its owner, one with none yet
    kept_owner = RNS.Identity()
    kept_name = name_for_identity(kept_owner)
    kept_target = add(*content.KEPT_TARGET, pin=False)
    keepers.keep(kept_name, "user")
    tick()
    keepers._store(keepers_module.sign_record(kept_owner, kept_name, encode_record(7, kept_target)))
    unanswered = name_for_identity(RNS.Identity())
    keepers.keep(unanswered, "user")

    # follow:<hex>, follow.json; the followed name's record is kept and held (names.json "held")
    followed_owner = RNS.Identity()
    followed = name_for_identity(followed_owner)
    followed_target = add(*content.FOLLOWED_TARGET, pin=False)
    tick()
    follow.follow(followed)
    keepers._store(keepers_module.sign_record(followed_owner, followed, encode_record(12, followed_target)))
    tick()
    assert follow.check(followed, network=False)

    # names.json "held": a name resolved through the CLI; name-floor.json: one resolved for the gateway
    resolved_owner_target = add(*content.RESOLVED_TARGET, pin=False)
    held = name_for_identity(RNS.Identity())
    tick()
    network.store(held, encode_record(3, resolved_owner_target), clock.now)
    assert names.resolve(held, persist=True) is not None
    for i, sequence in enumerate((5, 2**64 - 1)):
        floor_name = name_for_identity(RNS.Identity())
        tick()
        network.store(floor_name, encode_record(sequence, plain), clock.now)
        assert names.resolve(floor_name, persist=False) is not None

    # contacts.json: one of each kind
    contacts.add("alice", followed.hex(), kind="name")
    contacts.add("relay.1", PEERS[0].hex(), kind="node")
    contacts.add("bob", OTHER_SENDER.hex(), kind="lxmf")

    # lxmf_inbox.json: a message from an auto-fetch sender, both links fetched and pinned (lxmf:<hex>)
    feed = add(*content.FEED, pin=False)
    lxmf.attach(FakeLxmf(address=LXMF_ADDRESS, propagation_node=PROPAGATION_NODE))
    links = [Link(feed, size=36, from_node=PEERS[1]), Link(docs, path=("café.txt",))]
    text = "Fotos vom Wochenende \U0001f4f7\n\n" + "\n".join(link.uri() for link in links)
    tick()
    first = lxmf.received(Incoming(AUTO_SENDER, os.urandom(32), clock.now - 30, "Grüße 🌻".encode(),
                                   text.encode(), encode_field(links), "direct", "Zoë"))
    lxmf.process_auto_fetches()
    assert all(a["state"] == "pinned" for a in lxmf.inbox.get(first)["auto"]), lxmf.inbox.get(first)
    # ... and two from a sender not trusted: a name link in the text only, and non-BMP characters
    name_link = Link(name=names.name("photos"), path=("2026", "été.jpg"))
    tick()
    lxmf.received(Incoming(OTHER_SENDER, os.urandom(32), clock.now - 5, "Привет".encode(),
                           f"Посмотри это: {name_link.uri()}".encode(), {}, "propagated", "Bøb \U0001f98a"))
    tick()
    lxmf.received(Incoming(OTHER_SENDER, os.urandom(32), None, b"",
                           f"\U0001d11e music \U0001f3b5 rnfs:{user}".encode(), {}, "opportunistic", None))

    # lxmf_outbox.json: delivered, propagated and sent, and three unfinished: a direct one that fell back to
    # propagation, one outbound, one sending (0.1.4 marks them failed when it next starts)
    tick()
    delivered = lxmf.send(OTHER_SENDER.hex(), str(user), text="Voilà le fichier ✅")
    lxmf.outbound_changed(bytes.fromhex(delivered["hash"]), "delivered", 1.0)
    tick()
    propagated = lxmf.send("bob", f"name/{names.name('photos').hex()}/2026",
                           text="via the propagation node \U0001f4e1", propagate=True)
    lxmf.outbound_changed(bytes.fromhex(propagated["hash"]), "sent", None)
    tick()
    fallback = lxmf.send(OTHER_SENDER.hex(), str(picture), text="")
    lxmf.outbound_changed(bytes.fromhex(fallback["hash"]), "failed", None)  # resent through the propagation node
    tick()
    lxmf.send(AUTO_SENDER.hex(), f"{docs}/日本語", text="not delivered yet \u23f3")
    tick()
    sending = lxmf.send(AUTO_SENDER.hex(), str(plain), text="half way \U0001f69a")
    lxmf.outbound_changed(bytes.fromhex(sending["hash"]), "sending", 0.4567)
    lxmf.save()

    # jobs.json: a finished get with a result and warnings, a failed pin, and a queued pin (to resume)
    tick()
    got = jobs.submit("get", f"{docs}/café.txt", output="/home/alice/Downloads/café \U0001f600.txt")
    job = jobs._job(got["id"])
    job.begin_run()
    job.total(7)
    job.block(child(docs, "café.txt"), 7, True)
    tick(5)
    jobs._finish(job, "done", None, {"cid": str(child(docs, "café.txt")), "size": 7, "warnings": [
        "skipped symlink café/link → elsewhere", "kept \U0001f600 as is"],
        "output": job.output})
    tick()
    failed = jobs._job(jobs.submit("pin", str(Cid.for_block(RAW, b"nobody has this")))["id"])
    failed.begin_run()
    jobs._finish(failed, "failed", "no providers found for this CID", None)
    tick()
    jobs.submit("pin", f"{docs}/Ελληνικά")

    # published.json: everything provided was published to two peers; earlier rounds' nodes for some; a pin
    # released since leaves withdrawals queued for the nodes it was published to
    dropped = add(*content.DROPPED)
    publisher = node.publisher
    publisher.refresh()
    tick()
    with publisher._lock:
        for i, item in enumerate(sorted(publisher._provided, key=str)):
            publisher._published[item] = (clock.now - i * HOUR, [PEERS[i % 4], PEERS[(i + 1) % 4]])
            if i % 2 == 0:
                publisher._earlier[item] = {PEERS[(i + 2) % 4]: clock.now - 3 * DAY}
    storage.unpin(dropped, "user")
    publisher.refresh()  # saves: withdrawals of dropped's key queued for PEERS
    assert publisher._withdrawals, "no withdrawals queued"
    tick()
    refresh_senior()


def check(root: str):
    """Start 0.1.4 again on a copy of root, a day later: it must load every record (the fixture is consistent:
    nothing is reconciled away). Its clocks are faked as the test fakes the new code's."""
    import functools

    from rnfs import node as node_module

    copy = root + "-check"
    shutil.copytree(root, copy)
    clock = Clock(T0 + DAY)

    def state(name: str):
        """A state file of the fixture."""
        with open(os.path.join(root, "state", name), encoding="utf-8") as f:
            return json.load(f)

    with mock.patch("time.time", clock), \
            mock.patch.object(node_module, "ProviderStore", functools.partial(node_module.ProviderStore,
                                                                               clock=clock)), \
            mock.patch.object(node_module, "Publisher", functools.partial(node_module.Publisher, clock=clock)), \
            mock.patch.object(node_module, "NameManager", functools.partial(node_module.NameManager, clock=clock)):
        config = Config(rnfs_dir=copy, **SETTINGS)
        node = RnfsNode.offline(config, NODE_ID, NetClient(), FakeNetwork())
        try:
            pins = {Cid.parse(c): set(s) for c, s in state("pins.json")["pins"].items()}
            assert node.storage.pins() == pins, (node.storage.pins(), pins)
            assert all(node.storage.is_complete(cid) for cid in pins)
            assert len(node.storage.cached_fetches()) == len(state("cache.json")["fetches"])
            assert {str(c) for c in node._eligible_targets()} == set(state("fetched.json"))
            assert len(node._load_remote_pins()) == len(state("remote_pins.json"))
            assert {str(c) for c in node.publisher._published} == set(state("published.json")["published"])
            assert {n.hex() for n in node.publisher._withdrawals} == set(state("published.json")["withdraw"])
            stored = {(k, p) for k, p, *_ in state("providers.json")["records"]}
            loaded = {(k.hex(), p.hex()) for k, ps in node.records._records.items() for p in ps}
            assert loaded == stored, (loaded ^ stored)
            assert len(node.records._departed) == len(state("providers.json")["departed"])
            assert len(node.records._evicted) == len(state("providers.json")["evicted"])
            assert {n.hex() for n in node.names._held} == set(state("names.json")["held"])
            assert len(node.names._floor) == len(state("name-floor.json")["floor"])
            assert len(node.shared_folder._known) == len(state("shared_folder.json")["entries"])
            jobs = node.feature("jobs")._jobs
            assert [j for j in jobs] == [j["id"] for j in state("jobs.json")["jobs"]]
            keepers = node.feature("keepers").store
            assert {n.hex() for n in keepers.names()} == set(state("keepers.json")["names"])
            assert len(keepers._records) == sum(1 for e in state("keepers.json")["names"].values() if e["record"])
            assert {n.hex() for n in node.feature("follow")._pinned} == set(state("follow.json")["names"])
            assert set(node.feature("contacts")._contacts) == set(state("contacts.json")["contacts"])
            lxmf = node.feature("lxmf")
            assert lxmf.inbox.clean and len(lxmf.inbox) == len(state("lxmf_inbox.json")["messages"])
            assert len(lxmf.outbox) == len(state("lxmf_outbox.json")["messages"])
        finally:
            node.stop()
    shutil.rmtree(copy)


def prune(root: str):
    """Drop what the fixture doesn't need: LXMF router state, the node identity (rnfs makes one), the API token,
    logs, and the blocks (the test makes them again from fixture_content)."""
    for name in ("lxmf", "api_token", "debug.log", "storage", "blocks"):
        path = os.path.join(root, name)
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)


if __name__ == "__main__":
    RNS.loglevel = RNS.LOG_CRITICAL
    root = os.path.join(OUT, "rnfs-0.1.4")
    for path in (root, SCRATCH):
        if os.path.exists(path):
            shutil.rmtree(path)
    os.makedirs(SCRATCH)
    build(root)
    check(root)  # with the blocks, before they are pruned
    prune(root)
    shutil.rmtree(SCRATCH)
    print(root)
