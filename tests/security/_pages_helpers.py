"""Shared harness for the companion NomadNet page findings (not a test module).

The two pages are Python scripts NomadNet runs as CGI-like pages; they shell out to the ``rnfs`` CLI
(``companion-nomadnet-sites/display-mu-file/index.mu`` and ``.../file-index-node/index.mu``). These helpers
run the pages' own code against an offline ``RnfsNode`` (real node, fetcher, storage and publisher; the
transport is the ``NetClient`` double from ``tests.test_node``). The CLI the pages call is stood in for by a
fake ``subprocess.run`` that drives the in-process node, so the pages' real control flow, escaping and
``entry()`` logic run against real pin data. Each page keeps its allowlist, lock, rate state, caches and log
in a data dir, which the helpers point at a temporary directory through ``RNFS_PAGE_DIR``. The slow,
full-stack variants (a real daemon driven through the real CLI) live in the test modules behind ``RNFS_E2E``.
"""
import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.content import dag
from rnfs.content.cid import DAG, EMPTY_FILE, RAW, Cid
from rnfs.node import RnfsNode
from tests.test_names import FakeNetwork
from tests.test_node import A_ID, B_ID, NetClient

WORKTREE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PAGES_DIR = os.path.join(WORKTREE_ROOT, "companion-nomadnet-sites")
DISPLAY_PAGE = os.path.join(PAGES_DIR, "display-mu-file", "index.mu")
INDEX_PAGE = os.path.join(PAGES_DIR, "file-index-node", "index.mu")


def make_nodes(tmp, settings=None, test=None):
    """A provider node A and a victim node B joined by a NetClient, as tests.test_node does; stopped (which
    closes their state databases) when test, a TestCase, cleans up."""
    net = NetClient()
    net.sender = B_ID
    nodes = {}
    for name, node_id in (("a", A_ID), ("b", B_ID)):
        config = Config(rnfs_dir=os.path.join(tmp, name), slow="no", lookup_timeout=5, path_timeout=0.5,
                        **(settings or {}))
        node = RnfsNode.offline(config, node_id, net, FakeNetwork())
        net.nodes[node_id] = node
        nodes[name] = node
        if test is not None:
            test.addCleanup(node.stop)
    nodes["b"].routing.heard(A_ID)
    return net, nodes["a"], nodes["b"]


def give_identity(node):
    """Give an offline node an RNS identity, so it can seal to_self (sealed adds)."""
    node.identity = RNS.Identity()
    return node


def advertise(provider, finder, cid):
    """Make provider a provider of cid, in both its own records and the finder's, so a lookup finds it."""
    key = Cid.parse(cid).key if isinstance(cid, str) else cid.key
    provider.records.store(key, provider.node_id)
    finder.records.store(key, provider.node_id)


# --- the file-index-node page's own functions, lifted from its source ---


def index_page_namespace():
    """The file-index-node page's module-level defs (escape, display_name, entry, section)."""
    with open(INDEX_PAGE, encoding="utf-8") as f:
        source = f.read()
    # The page renders only under __main__, so running its source here just defines them
    namespace = {"__name__": "index_page"}
    exec(compile(source, INDEX_PAGE, "exec"), namespace)
    return namespace


@contextlib.contextmanager
def page_dir(path=None):
    """RNFS_PAGE_DIR set to path, or to a fresh temporary directory removed afterwards; yields the path."""
    with contextlib.ExitStack() as stack:
        if path is None:
            path = stack.enter_context(tempfile.TemporaryDirectory())
        stack.enter_context(mock.patch.dict(os.environ, {"RNFS_PAGE_DIR": path}))
        yield path


def render_index(node, calls=None, data_dir=None):
    """What the file-index-node page's entry() prints for each of node's complete pins (as the page does:
    pin_ls JSON, round-tripped, through the page's own entry()). The rnfs commands entry() runs (its sealed
    check) are served from node by fake_rnfs and recorded in calls; data_dir is the page's RNFS_PAGE_DIR."""
    pins = json.loads(json.dumps(node.pin_ls()))  # the CLI's pin ls --json, then the page's json.loads
    with page_dir(data_dir), mock.patch.object(subprocess, "run", fake_rnfs(node, [] if calls is None else calls)):
        namespace = index_page_namespace()
        return [namespace["entry"](pin) for pin in pins if pin.get("complete")]


def exec_index_page(run, data_dir=None):
    """Run the whole file-index-node script with subprocess.run replaced by run; return what it printed.
    data_dir is the page's data dir (RNFS_PAGE_DIR; a fresh one if None)."""
    with open(INDEX_PAGE, encoding="utf-8") as f:
        source = f.read()
    output = io.StringIO()
    with page_dir(data_dir), mock.patch.object(subprocess, "run", run), mock.patch.object(sys, "stdout", output):
        exec(compile(source, INDEX_PAGE, "exec"), {"__name__": "__main__"})
    return output.getvalue()


# --- driving the display-mu-file page's whole script against an in-process node ---


class _Completed:
    """A subprocess.CompletedProcess stand-in: the three attributes the page reads."""

    def __init__(self, returncode, stdout=b"", stderr=""):
        """returncode, stdout bytes and stderr text (as the page's rnfs() expects)."""
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr.encode("utf-8") if isinstance(stderr, str) else stderr


def fake_rnfs(node, calls, timeout_on=()):
    """A subprocess.run stand-in for the page: it runs the rnfs subcommand against node in-process.

    calls records each argument list the page runs. A subcommand named in timeout_on raises
    TimeoutExpired, as the real subprocess.run does when the CLI outlives its timeout.
    """
    from rnfs.errors import NotFound

    def cat(target, options):
        """`rnfs cat target [--raw] [--offset N] [--length N]`, as the API's cat serves it (api.py api_cat)."""
        raw = {"raw": True} if "--raw" in options else {}
        offset = int(options[options.index("--offset") + 1]) if "--offset" in options else 0
        length = int(options[options.index("--length") + 1]) if "--length" in options else None
        info = node.info(target, **raw)
        if info["type"] != "file":
            return _Completed(1, stderr=f"rnfs: {target} is a directory")
        available = max(info["size"] - offset, 0)
        length = available if length is None else min(length, available)
        return _Completed(0, b"".join(node.read(target, offset=offset, length=length, **raw)))

    def run(argv, *args, **kwargs):
        """Serve the page's rnfs(...) call from the node."""
        words = list(argv[1:])  # argv[0] is the rnfs executable path
        calls.append(words)
        if words and words[0] in timeout_on:
            raise subprocess.TimeoutExpired(argv, kwargs.get("timeout"))
        try:
            if words[0] == "stat":
                lines = "".join(f"{name}: {value}\n" for name, value in node.stat(words[1]).items())
                return _Completed(0, lines.encode("utf-8"))
            if words[0] == "ls":
                node.ls(words[1])
                return _Completed(0, b"<listing>\n")
            if words[:2] == ["pin", "add"]:
                return _Completed(0, (node.pin_add(words[2])["cid"] + "\n").encode("utf-8"))
            if words[0] == "cat":
                return cat(words[1], words[2:])
            if words == ["status"]:
                from rnfs.cli import print_fields
                with contextlib.redirect_stdout(io.StringIO()) as printed:
                    print_fields(node.status())
                return _Completed(0, printed.getvalue().encode("utf-8"))
            if words[:3] == ["pin", "ls", "--json"]:
                return _Completed(0, json.dumps(node.pin_ls(*words[3:4])).encode("utf-8"))
        except ValueError as error:  # e.g. "<target> is not a directory", which the page keys off
            return _Completed(1, stderr=str(error))
        except NotFound as error:
            return _Completed(1, stderr=str(error))
        return _Completed(1, stderr="unknown command")

    return run


def display_page_namespace(data_dir):
    """The display-mu-file page's module-level names (its constants and functions), without running it, for
    the page data dir data_dir."""
    with open(DISPLAY_PAGE, encoding="utf-8") as f:
        source = f.read()
    namespace = {"__name__": "display_page"}
    with page_dir(data_dir):
        exec(compile(source, DISPLAY_PAGE, "exec"), namespace)
    return namespace


def exec_display_page(run, cid, data_dir=None):
    """Run the real display-mu-file script for var_cid=cid with subprocess.run replaced by run; return
    whatever the page printed. data_dir is the page's data dir (RNFS_PAGE_DIR; a fresh one if None)."""
    with open(DISPLAY_PAGE, encoding="utf-8") as f:
        source = f.read()
    output = io.StringIO()
    with page_dir(data_dir), \
            mock.patch.object(subprocess, "run", run), \
            mock.patch.dict(os.environ, {"var_cid": cid}), \
            mock.patch.object(sys, "stdout", output):
        try:
            exec(compile(source, DISPLAY_PAGE, "exec"), {"__name__": "__main__"})
        except SystemExit:
            pass  # the page stops with SystemExit on every error path
    return output.getvalue()


def run_display_page(node, cid, timeout_on=(), data_dir=None):
    """Run the real display-mu-file script for var_cid=cid against node; return (output, calls)."""
    calls = []
    output = exec_display_page(fake_rnfs(node, calls, timeout_on), cid, data_dir)
    return output, calls


def allow(data_dir, *cids):
    """Write the display page's allowlist file in data_dir, listing cids."""
    with open(os.path.join(data_dir, "allowlist.txt"), "w", encoding="utf-8") as f:
        f.write("# CIDs the operator lets the page fetch and render\n" + "".join(f"{cid}\n" for cid in cids))


# --- the N1 "1-byte" DAG bomb ---


def bomb_dag(fanout=3, children=3000):
    """A file root that declares 1 byte but carries fanout large file-node blocks of zero-size children.

    Returns (root_cid, {cid: block}). Each level-1 node lists `children` empty chunks (size 0), so the root's
    declared size is 1 (the single b"x" leaf) while the DAG's blocks total far more than that.
    """
    blocks = {EMPTY_FILE: b""}

    def store(node):
        """Encode node, remember its block, return its CID."""
        block = dag.encode(node)
        cid = Cid.for_block(DAG, block)
        blocks[cid] = block
        return cid

    level1 = [store(dag.FileNode([dag.Child(EMPTY_FILE, 0)] * (children + i))) for i in range(fanout)]
    leaf = Cid.for_block(RAW, b"x")
    blocks[leaf] = b"x"
    root = store(dag.FileNode([dag.Child(cid, 0) for cid in level1] + [dag.Child(leaf, 1)]))
    return root, blocks


def serve_blocks(provider, finder, root, blocks):
    """Store blocks on provider, pin+provide root so provider serves them, and advertise root to finder."""
    with provider.storage.protect() as session:
        for cid, block in blocks.items():
            session.put(cid, block)
    from rnfs.content.storage import USER
    provider.storage.pin(root, USER)
    provider.publisher.refresh()
    advertise(provider, finder, root)
