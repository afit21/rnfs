"""Write the tests/fixtures/rnfs-0.1.5 fixture: an rnfs directory exactly as rnfs 0.1.5 leaves it, its state
database holding a row in every table (see README.md there).

Run it with the rnfs 0.1.5 source tree (git archive v0.1.5, which includes its tests/ package and the rnfs-0.1.4
fixture), never the current one: it puts that tree first on sys.path and checks rnfs is imported from there. From
the repository root:

    mkdir /tmp/rnfs-0.1.5 && git archive v0.1.5 | tar -x -C /tmp/rnfs-0.1.5
    python3 tests/fixtures/rnfs-0.1.5/generate.py /tmp/rnfs-0.1.5 /tmp/rnfs-fixture-out
    # then replace tests/fixtures/rnfs-0.1.5/{state,names,files,expected.json} with those in
    # /tmp/rnfs-fixture-out/rnfs-0.1.5

How: 0.1.5 starts, offline, on a copy of the rnfs-0.1.4 fixture, with that fixture's blocks made and its clock
and settings, as 0.1.5's own tests/test_upgrade_014.py does. That imports the 17 JSON state files. Then 0.1.5's
own objects add what only 0.1.5 has: local deny-list entries (a CID and a file hash), a deny list followed through
the deny_lists setting (accepted, its blocks pinned "deny"), and a file's content hash remembered (deny_hashes).
The node stops (which saves and checkpoints), and the database is VACUUMed. 0.1.5 then starts again on a copy and
must change no row. expected.json holds every table's rows, read with plain sqlite3.
"""
import io
import json
import os
import shutil
import sqlite3
import sys
from unittest import mock

V015, OUT = (os.path.abspath(arg) for arg in sys.argv[1:3])
HERE = os.path.dirname(os.path.abspath(__file__))
SCRATCH = os.path.join(OUT, "scratch")
sys.path.insert(0, V015)
sys.path.insert(1, HERE)  # fixture_content, next to this file

import RNS  # noqa: E402

import rnfs  # noqa: E402

assert rnfs.__file__.startswith(V015 + os.sep), rnfs.__file__
assert rnfs.__version__ == "0.1.5", rnfs.__version__

from rnfs import statedb  # noqa: E402
from rnfs.config import Config  # noqa: E402
from rnfs.content import builder  # noqa: E402
from rnfs.content.blockstore import BlockStore  # noqa: E402
from rnfs.content.cid import Cid  # noqa: E402
from rnfs.node import RnfsNode  # noqa: E402
from tests import test_upgrade_014 as v014  # noqa: E402  (0.1.5's own upgrade test: its fixture, clock, settings)
from tests.test_names import FakeNetwork  # noqa: E402
from tests.test_node import NetClient  # noqa: E402

import fixture_content as content  # noqa: E402

NOW = v014.NOW  # T0 + 1 day: the 0.1.4 fixture's records haven't expired


def tables(db_path: str) -> dict[str, list]:
    """Every row of every table, sorted, as JSON: a BLOB is {"blob": hex}."""
    conn = sqlite3.connect(db_path)
    try:
        names = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")]
        return {name: sorted(([{"blob": v.hex()} if isinstance(v, bytes) else v for v in row]
                              for row in conn.execute(f"SELECT * FROM {name}")), key=repr) for name in names}
    finally:
        conn.close()


def patched(clock):
    """The patches 0.1.5's upgrade test starts its node under: time.time, and the stores taking it as a default
    argument."""
    return [
        mock.patch("time.time", clock),
        *(mock.patch.object(v014.node_module, cls, v014.functools.partial(getattr(v014.node_module, cls),
                                                                           clock=clock))
          for cls in ("ProviderStore", "Publisher", "NameManager"))]


def start(root: str, settings: dict) -> RnfsNode:
    """An offline 0.1.5 node on root."""
    config = Config(rnfs_dir=root, **settings)
    config.validate()
    return RnfsNode.offline(config, v014.NODE_ID, NetClient(), FakeNetwork())


def build(root: str) -> dict:
    """Copy the 0.1.4 fixture to root, make its blocks and upgrade it with 0.1.5, then add 0.1.5's rows. The
    settings used."""
    for part in ("state", "names", "files"):
        shutil.copytree(os.path.join(v014.FIXTURE, part), os.path.join(root, part))
    v014.make_blocks(os.path.join(root, "blocks"), SCRATCH)
    store = BlockStore(os.path.join(root, "blocks"))
    deny_list = builder.add_stream(io.BytesIO(content.DENY_LIST[0]), store.put, len(content.DENY_LIST[0])).cid
    settings = {**v014.SETTINGS, "deny_lists": [str(deny_list)]}
    clock = v014.FakeClock(NOW)
    patches = patched(clock)
    for patch in patches:
        patch.start()
    try:
        node = start(root, settings)
        try:
            for name in v014.LEGACY:
                assert os.path.exists(os.path.join(root, "state", name + statedb.MIGRATED_SUFFIX)), name
            # deny_entries
            assert node.deny.add([content.DENIED_CID, content.DENIED_HASH])
            # deny_lists, and its blocks pinned "deny"
            assert node.feature("deny").check(str(deny_list)), node.feature("deny").lists()
            # deny_hashes: a file of several blocks read while file hashes are denied
            data, name = content.LARGE
            large = Cid.parse(node.add_stream(io.BytesIO(data), name=name)["cid"])
            info, chunks = node.read_file(f"{large}/{name}")
            assert b"".join(chunks) == data
            assert node.deny.known_hash(Cid.parse(info["cid"])) is not None
        finally:
            node.stop()
    finally:
        for patch in reversed(patches):
            patch.stop()
    db_path = os.path.join(root, "state", statedb.DB_FILE)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("VACUUM")
    finally:
        conn.close()
    for suffix in ("-wal", "-shm"):
        assert not os.path.exists(db_path + suffix), suffix
    return settings


def check(root: str, settings: dict):
    """Start 0.1.5 again on a copy of root (with the blocks), at the same time: it must change no row, so the test
    can expect every row as it is."""
    copy = root + "-check"
    shutil.copytree(root, copy)
    before = tables(os.path.join(copy, "state", statedb.DB_FILE))
    patches = patched(v014.FakeClock(NOW))
    for patch in patches:
        patch.start()
    try:
        start(copy, settings).stop()
    finally:
        for patch in reversed(patches):
            patch.stop()
    after = tables(os.path.join(copy, "state", statedb.DB_FILE))
    assert after == before, {t: (before[t], after[t]) for t in before if before[t] != after.get(t)}
    empty = [t for t, rows in before.items() if not rows]
    assert not empty, empty  # every table has rows
    shutil.rmtree(copy)


def prune(root: str):
    """Drop what the fixture doesn't need, as the 0.1.4 fixture does: LXMF router state, the node identity, the
    API token, logs, and the blocks (the test makes them again from the two fixture_content modules)."""
    for name in ("lxmf", "api_token", "debug.log", "debug.log.1", "storage", "blocks"):
        path = os.path.join(root, name)
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)


if __name__ == "__main__":
    RNS.loglevel = RNS.LOG_CRITICAL
    root = os.path.join(OUT, "rnfs-0.1.5")
    for path in (root, SCRATCH):
        if os.path.exists(path):
            shutil.rmtree(path)
    os.makedirs(SCRATCH)
    settings = build(root)
    check(root, settings)
    prune(root)
    shutil.rmtree(SCRATCH)
    expected = {"now": NOW, "settings": settings, "user_version": len(statedb.SCHEMA),
                "tables": tables(os.path.join(root, "state", statedb.DB_FILE))}
    with open(os.path.join(root, "expected.json"), "w", encoding="utf-8") as f:
        json.dump(expected, f, indent=1, ensure_ascii=False, sort_keys=True)
        f.write("\n")
    print(root)
