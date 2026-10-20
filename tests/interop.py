#!/usr/bin/env python3
"""Cross-version interop: daemons of this tree against daemons of older releases, over local TCP.

Not a unittest module (discovery runs test_*.py only) and not run by the suite: a script run by hand before a
release (the release checklist's "Interop" item). For each old tree given with --old (a `git archive` extract of a
release tag), it starts a network like tests/test_e2e.py's, a hub with transport enabled and nodes connected to
it, where every daemon runs `python3 <tree>/rnfs.py daemon` with its tree first on PYTHONPATH and is driven by
that tree's own CLI:

- hub     this tree; the TCP server and the bootstrap node.
- old/new one node of each version, default settings.
- old-cap/new-cap one node of each version with max_upload_rate set (CAP_RATE), the serving side of the paced
          transfer, and later the name publishers of the keeper scenario.

Every scenario runs in both directions, "A -> B" meaning A serves (or publishes) and B fetches (or resolves):

- version    each daemon's `rnfs --version`, /api/status and imported rnfs come from the tree it was given.
- providers  B finds A among the providers of a CID A added.
- fetch      a multi-chunk file and a directory tree added on A: `get`, `cat --offset --length` and a path's
             `cat` on B give the same bytes, execute bits included.
- bulk       a BULK_SIZE file with default settings: identical bytes, and no refusals, failures or bad blocks
             in B's counters for A, no fetch failures in B's log and no "refused" lines in A's (NET-1).
- small      the same checks for a tree of SMALL_FILES small files, whose /blocks responses fit one packet each
             (a local TCP link's MTU is large).
- bulk-cap,  bulk and small from the *-cap node of A's version, whose max_upload_rate paces every /blocks
  small-cap  response, single-packet ones included since 0.1.6 (NET-2): an honest fetcher must see no refusals.
- ranges     (rnfs 0.2.0's /range) a node of this tree set slow = "yes" (new-slow, so it asks for chunks in
             ranges over every link) fetches text files: from old, each of two fetches has one /range left
             unanswered and falls back to /blocks, with no refusal, failure or ban on either side; the second
             silence in a row marks the node as not answering, so a third fetch, after a restart (the answer is
             persisted), asks no /range of it; from new, it gets the chunks in compressed ranges.
- names      a name published on A resolves on B, and B's gateway serves a file through it.
- keepers    publisher -> keeper -> resolver across versions (old-cap -> new -> a fresh old node, new-cap -> old
             -> a fresh new node): the keeper keeps the name, the publisher stops, then the resolver starts (so
             it never heard the name's announce) and must get the signed record by asking keepers, its log
             showing the keeper query that returned it.
There is no LXMF scenario: this tree has no LXMF since 0.2.0, which moved it out of rnfs into an app of its own
(the old releases still have it, but there is nothing to test between them and this tree). The rnfs link field
an LXMF message carries is checked against the old releases' bytes by tests/test_wire.py.

Usage, from a checkout (the Python that runs rnfs, with RNS installed):

    git archive v0.1.5 | (mkdir -p /tmp/rnfs-0.1.5 && tar -x -C /tmp/rnfs-0.1.5)
    git archive v0.1.4 | (mkdir -p /tmp/rnfs-0.1.4 && tar -x -C /tmp/rnfs-0.1.4)
    python3 tests/interop.py --old /tmp/rnfs-0.1.5 --old /tmp/rnfs-0.1.4

It prints a PASS/FAIL table per old tree and exits non-zero on any FAIL. Each old tree takes about five minutes.
Daemon directories and logs go to a temporary directory, kept (and named) when something fails or with --keep.
"""
import argparse
import json
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # run from a plain checkout

from tests import support
from tests.test_e2e import HUB_CONFIG, INTERRUPT, NODE_CONFIG, POPEN_OPTIONS, SETTINGS, WINDOWS, same_tree

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KIB, MIB = 1024, 1024 * 1024
BULK_SIZE = 3 * MIB // 2
SMALL_FILES, SMALL_SIZE = 64, 4 * KIB
TEXT_SIZE = 200 * KIB  # the ranges scenario's text files: about four chunks each
UNANSWERED = "doesn't answer /range"  # what a fetching node of this tree logs as it learns a node doesn't
SILENT_ONCE = "left /range unanswered once"  # ... and as a node leaves its first /range unanswered
CAP_RATE = 2_000_000  # bits/s: max_upload_rate on the *-cap nodes, so BULK_SIZE takes seconds (4 to 6)
# Lines in a fetching node's log that mean a provider let it down, and in a serving node's log that mean it
# refused (or cooled down) a requester. Both need RNS's loglevel at 5 (verbose), which every node here runs at
FETCHER_TROUBLE = re.compile(r"rnfs .*(failed for |banning |every provider of |refus|cooldown)", re.IGNORECASE)
SERVER_TROUBLE = re.compile(r"rnfs .*(refus|cooldown|no progress)", re.IGNORECASE)


def wait_for(condition, timeout: float, what: str):
    """support.wait_for, polling every half second (each poll runs the CLI or calls an API)."""
    return support.wait_for(condition, timeout, what, interval=0.5)


def tree_version(tree: str) -> str:
    """The __version__ a source tree's rnfs/__init__.py declares."""
    with open(os.path.join(tree, "rnfs", "__init__.py"), encoding="utf-8") as f:
        match = re.search(r'(?m)^__version__ = "([^"]+)"', f.read())
    if match is None:
        raise SystemExit(f"{tree}: no __version__ in rnfs/__init__.py")
    return match.group(1)


def current_label() -> str:
    """This tree's name in the tables: its git commit, and its version."""
    try:
        commit = subprocess.run(["git", "-C", ROOT, "rev-parse", "--short", "HEAD"], capture_output=True,
                                text=True, timeout=10).stdout.strip()
    except OSError:
        commit = ""
    return f"current ({commit or 'checkout'}, {tree_version(ROOT)})"


class Daemon:
    """One running daemon of one source tree: its directory, ports, node ID and log."""

    def __init__(self, name: str, tree: str, label: str, base: str, config: str, tcp_port: int,
                 settings: str = SETTINGS, *extra: str):
        """Start the daemon and wait for its API."""
        self.name, self.tree, self.label = name, tree, label
        self.main = os.path.join(tree, "rnfs.py")
        self.dir = os.path.join(base, name)
        os.makedirs(self.dir, exist_ok=True)
        # loglevel 5 (verbose): rnfs logs refusals, cooldowns and failed providers at that level
        with open(os.path.join(self.dir, "config"), "w", encoding="utf-8") as f:
            f.write(config.format(port=tcp_port).replace("loglevel = 4", "loglevel = 5"))
        with open(os.path.join(self.dir, "rnfs.toml"), "w", encoding="utf-8") as f:
            f.write(settings)
        self.env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONPATH": tree}
        self.api_port = support.free_port()
        self.gateway_port = support.free_port()
        self.log_path = os.path.join(base, f"{name}.log")
        with open(self.log_path, "wb") as log:
            self.proc = subprocess.Popen(
                [sys.executable, self.main, "--dir", self.dir, "--api-port", str(self.api_port), "daemon",
                 "--rnsconfig", self.dir, "--gateway-port", str(self.gateway_port), *extra],
                stdout=log, stderr=subprocess.STDOUT, env=self.env, cwd=base, **POPEN_OPTIONS)
        self.node_id = wait_for(self.status, 60, f"{name} daemon to start")["node_id"]

    def status(self) -> dict | None:
        """The daemon's status (with its API token), or None while its API is not up yet."""
        try:
            with open(os.path.join(self.dir, "api_token"), encoding="ascii") as f:
                token = f.read().strip()
            request = urllib.request.Request(f"http://127.0.0.1:{self.api_port}/api/status",
                                             headers={"X-Rnfs-Api": token})
            with urllib.request.urlopen(request, timeout=2) as response:
                return json.load(response)
        except OSError:
            if self.proc.poll() is not None:
                raise AssertionError(f"{self.name} daemon exited") from None
            return None

    def run(self, *args: str, timeout: float = 300) -> subprocess.CompletedProcess:
        """This tree's CLI with args, as is (no --dir)."""
        return subprocess.run([sys.executable, self.main, *args], capture_output=True, timeout=timeout,
                              env=self.env, cwd=os.path.dirname(self.dir))

    def cli(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        """A CLI command against this daemon; with check, fail unless it succeeds."""
        result = self.run("--dir", self.dir, "--api-port", str(self.api_port), *args)
        if check and result.returncode != 0:
            raise AssertionError(f"{self.name}: {' '.join(args)} failed: "
                                 f"{result.stderr.decode('utf-8', 'replace').strip()}")
        return result

    def out(self, *args: str) -> str:
        """A successful command's stdout, stripped."""
        return self.cli(*args).stdout.decode("utf-8", "replace").strip()

    def providers(self, cid: str) -> list[str]:
        """The provider IDs this node finds for cid (none if the lookup fails)."""
        result = self.cli("providers", cid, check=False)
        return result.stdout.decode("utf-8", "replace").split() if result.returncode == 0 else []

    def peer(self, node_id: str) -> dict:
        """This node's traffic counters for node_id (all zero if it has none)."""
        for peer in json.loads(self.out("peers", "--json")):
            if peer.get("node_id") == node_id:
                return peer
        return {}

    def log_size(self) -> int:
        """The log's length now, to read only what is logged after it."""
        return os.path.getsize(self.log_path)

    def log_since(self, offset: int) -> list[str]:
        """The log's lines from offset on."""
        with open(self.log_path, "rb") as f:
            f.seek(offset)
            return f.read().decode("utf-8", "replace").splitlines()

    def gateway(self, path: str) -> tuple[int, bytes]:
        """(status, body) of a gateway GET."""
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.gateway_port}{path}", timeout=120) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    def interrupt(self):
        """Ask the daemon to shut down, as Ctrl+C would, if it is still running."""
        if self.proc.poll() is not None:
            return
        try:
            self.proc.send_signal(INTERRUPT)
        except OSError:
            self.proc.terminate()

    def stop(self):
        """Interrupt like Ctrl-C; kill if it doesn't exit in time."""
        self.interrupt()
        try:
            self.proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()


class Network:
    """A hub and the four nodes of one run (this tree against one old tree), and the scenarios run on it."""

    def __init__(self, old_tree: str, base: str):
        """Nothing started yet; old_tree is the old release's source tree, base the run's directory."""
        self.old_tree = os.path.abspath(old_tree)
        self.old_label = f"v{tree_version(self.old_tree)}"
        self.new_label = current_label()
        self.base = base
        self.daemons: list[Daemon] = []
        self.results: list[tuple[str, str, bool, str]] = []
        self.counter = 0

    # Running

    def start(self):
        """Start the hub, then the four nodes, all bootstrapping from the hub."""
        self.tcp_port = tcp_port = support.free_port()
        capped = SETTINGS + f"max_upload_rate = {CAP_RATE}\n"
        self.hub = self.daemon("hub", ROOT, self.new_label, HUB_CONFIG, tcp_port)
        bootstrap = ("--bootstrap", self.hub.node_id)
        self.old = self.daemon("old", self.old_tree, self.old_label, NODE_CONFIG, tcp_port, SETTINGS, *bootstrap)
        self.new = self.daemon("new", ROOT, self.new_label, NODE_CONFIG, tcp_port, SETTINGS, *bootstrap)
        self.old_cap = self.daemon("old-cap", self.old_tree, self.old_label, NODE_CONFIG, tcp_port, capped,
                                   *bootstrap)
        self.new_cap = self.daemon("new-cap", ROOT, self.new_label, NODE_CONFIG, tcp_port, capped, *bootstrap)

    def daemon(self, name: str, tree: str, label: str, config: str, tcp_port: int, settings: str = SETTINGS,
               *extra: str) -> Daemon:
        """Start one daemon, remembered for stopping."""
        daemon = Daemon(name, tree, label, self.base, config, tcp_port, settings, *extra)
        self.daemons.append(daemon)
        return daemon

    def stop_all(self):
        """Interrupt every daemon at once, then wait for each (killing any that hangs)."""
        for daemon in self.daemons:
            daemon.interrupt()
        for daemon in self.daemons:
            daemon.stop()

    def check(self, scenario: str, direction: str, step, *args):
        """Run one scenario step, recording PASS (with the detail it returns) or FAIL (with the error)."""
        print(f"--- {scenario}: {direction}", flush=True)
        started = time.monotonic()
        try:
            detail = step(*args) or ""
            ok = True
        except Exception as e:
            detail = f"{type(e).__name__}: {e}"
            if not isinstance(e, AssertionError):
                detail += "\n" + traceback.format_exc()
            ok = False
        detail = f"{detail} [{time.monotonic() - started:.0f}s]".strip()
        print(f"    {'PASS' if ok else 'FAIL'} {detail}", flush=True)
        self.results.append((scenario, direction, ok, detail))
        return ok

    def run(self):
        """Every scenario, in both directions."""
        pairs = [(self.old, self.new), (self.new, self.old)]
        self.check("version", "every daemon", self.versions)
        added = {}

        def add(a: Daemon) -> str:
            added[a.name] = self.add_content(a)
            return f"file {added[a.name]['file']}, dir {added[a.name]['dir']}"

        for a, _ in pairs:
            self.check("add", a.label.split(" (")[0], add, a)
        for a, b in pairs:
            self.check("providers", self.direction(a, b), self.providers, a, b, added[a.name])
        for a, b in pairs:
            self.check("fetch", self.direction(a, b), self.fetch, a, b, added[a.name])
        for a, b in pairs:
            self.check("bulk", self.direction(a, b), self.bulk, a, b)
            self.check("small", self.direction(a, b), self.small, a, b)
        # small-cap first: on a link with no rate estimate yet, only 0.1.6 paces single-packet responses (NET-2)
        for a, b in [(self.old_cap, self.new), (self.new_cap, self.old)]:
            self.check("small-cap", self.direction(a, b), self.small, a, b)
            self.check("bulk-cap", self.direction(a, b), self.bulk, a, b)
        new_slow = self.daemon("new-slow", ROOT, self.new_label, NODE_CONFIG, self.tcp_port,
                               SETTINGS + 'slow = "yes"\n', "--bootstrap", self.hub.node_id)
        self.check("ranges", self.direction(self.old, new_slow) + " (slow)", self.ranges_old, self.old, new_slow)
        self.check("ranges", self.direction(self.new, new_slow) + " (slow)", self.ranges_new, self.new, new_slow)
        for a, b in pairs:
            self.check("names", self.direction(a, b), self.names, a, b, added[a.name])
        # Last: the publishers (the *-cap nodes) are stopped on the way
        for publisher, keeper, resolver in [(self.old_cap, self.new, self.old), (self.new_cap, self.old, self.new)]:
            self.check("keepers", self.direction(publisher, keeper, resolver), self.keepers, publisher, keeper,
                       resolver.tree, resolver.label)

    def direction(self, *daemons: Daemon) -> str:
        """The direction's label: each daemon's version, in order."""
        return " -> ".join(daemon.label.split(" (")[0] for daemon in daemons)

    def path(self, *parts: str) -> str:
        """A path in the run's directory."""
        return os.path.join(self.base, *parts)

    def unique(self, prefix: str) -> str:
        """A path in the run's directory not used before."""
        self.counter += 1
        return self.path(f"{prefix}-{self.counter}")

    def random_file(self, size: int) -> tuple[str, bytes]:
        """A new file of size random bytes: (path, content)."""
        content = os.urandom(size)
        path = self.unique("data")
        with open(path, "wb") as f:
            f.write(content)
        return path, content

    # Scenarios

    def versions(self) -> str:
        """Each daemon runs its own tree's code and reports that tree's version."""
        for daemon in self.daemons:
            expected = tree_version(daemon.tree)
            reported = daemon.status()["version"]
            cli = daemon.run("--version").stdout.decode().strip()
            probe = subprocess.run([sys.executable, "-c", "import rnfs; print(rnfs.__file__)"], capture_output=True,
                                   text=True, env=daemon.env, cwd=self.base, timeout=60).stdout.strip()
            if reported != expected or cli != f"rnfs {expected}":
                raise AssertionError(f"{daemon.name} ({daemon.tree}): status says {reported!r}, --version says "
                                     f"{cli!r}, expected {expected}")
            if os.path.commonpath([os.path.realpath(probe), os.path.realpath(daemon.tree)]) != \
                    os.path.realpath(daemon.tree):
                raise AssertionError(f"{daemon.name}: rnfs imports from {probe}, not {daemon.tree}")
        return ", ".join(f"{daemon.name}={daemon.status()['version']}" for daemon in self.daemons)

    def add_content(self, a: Daemon) -> dict:
        """A multi-chunk file and a directory tree, added on a."""
        path, content = self.random_file(300 * KIB)
        root = self.unique(f"tree-{a.name}")
        os.makedirs(os.path.join(root, "sub", "deeper"))
        with open(os.path.join(root, "big.bin"), "wb") as f:
            f.write(os.urandom(200 * KIB))
        with open(os.path.join(root, "index.txt"), "wb") as f:
            f.write(f"hello from {a.label}\n".encode())
        script = os.path.join(root, "sub", "run.sh")
        with open(script, "wb") as f:
            f.write(b"#!/bin/sh\necho hi\n")
        os.chmod(script, 0o755)
        with open(os.path.join(root, "sub", "deeper", "note.txt"), "wb") as f:
            f.write(b"deep inside")
        return {"file": a.out("add", path), "content": content, "dir": a.out("add", root), "tree": root}

    def providers(self, a: Daemon, b: Daemon, added: dict) -> str:
        """b finds a providing the file and the directory."""
        wait_for(lambda: a.node_id in b.providers(added["file"]), 90, f"{b.name} to find {a.name} providing a file")
        wait_for(lambda: a.node_id in b.providers(added["dir"]), 90, f"{b.name} to find {a.name} providing a dir")
        return "file and directory"

    def fetch(self, a: Daemon, b: Daemon, added: dict) -> str:
        """b gets and cats what a added, byte for byte."""
        out = self.unique("got")
        b.cli("get", added["file"], "-o", out)
        with open(out, "rb") as f:
            if f.read() != added["content"]:
                raise AssertionError("get: the file's bytes differ")
        out = self.unique("got-tree")
        b.cli("get", added["dir"], "-o", out)
        if not same_tree(added["tree"], out):
            raise AssertionError("get: the directory tree differs")
        with open(os.path.join(added["tree"], "big.bin"), "rb") as f:
            big = f.read()
        part = b.cli("cat", f"{added['dir']}/big.bin", "--offset", "70000", "--length", "100000").stdout
        if part != big[70000:170000]:
            raise AssertionError("cat --offset --length: the bytes differ")
        if b.cli("cat", f"{added['dir']}/sub/deeper/note.txt").stdout != b"deep inside":
            raise AssertionError("cat <dir>/<path>: the bytes differ")
        if not WINDOWS:
            out = self.unique("got-run")
            b.cli("get", f"{added['dir']}/sub/run.sh", "-o", out)
            if not os.stat(out).st_mode & 0o100:
                raise AssertionError("get <dir>/<path>: the execute bit is lost")
        return "get file, get dir, cat range, cat path"

    def bulk(self, a: Daemon, b: Daemon) -> str:
        """b gets a BULK_SIZE file from a, with no refusal, failure or bad block on either side."""
        path, content = self.random_file(BULK_SIZE)
        cid = a.out("add", path)

        def check(out):
            with open(out, "rb") as f:
                if f.read() != content:
                    raise AssertionError("the bytes differ")

        return self.honest_transfer(a, b, cid, check, BULK_SIZE, f"{BULK_SIZE / MIB:.1f} MiB")

    def small(self, a: Daemon, b: Daemon) -> str:
        """b gets a tree of SMALL_FILES small files from a, with no refusal, failure or bad block on either side.

        Over local TCP a link's MTU is large, so these responses go as single packets: the ones NET-2 paces when
        max_upload_rate is set."""
        root = self.unique(f"small-{a.name}")
        for i in range(SMALL_FILES):
            os.makedirs(os.path.join(root, f"d{i % 4}"), exist_ok=True)
            with open(os.path.join(root, f"d{i % 4}", f"f{i}.bin"), "wb") as f:
                f.write(os.urandom(SMALL_SIZE))
        cid = a.out("add", root)

        def check(out):
            if not same_tree(root, out):
                raise AssertionError("the directory tree differs")

        return self.honest_transfer(a, b, cid, check, SMALL_FILES * SMALL_SIZE,
                                    f"{SMALL_FILES} files of {SMALL_SIZE // KIB} KiB")

    def honest_transfer(self, a: Daemon, b: Daemon, cid: str, check, size: int, what: str) -> str:
        """b gets cid from a, which check(path) then checks; no refusal, failure or bad block may show in b's
        counters for a, b's log or a's log (NET-1, NET-2)."""
        wait_for(lambda: a.node_id in b.providers(cid), 90, f"{b.name} to find {a.name} providing {cid}")
        before = b.peer(a.node_id)
        b_log, a_log = b.log_size(), a.log_size()
        out = self.unique("transfer")
        started = time.monotonic()
        b.cli("get", cid, "-o", out)
        took = time.monotonic() - started
        after = b.peer(a.node_id)
        check(out)
        counts = {key: after.get(key, 0) - before.get(key, 0) for key in ("requests", "answered", "refused",
                                                                            "failed", "bad")}
        fetcher_lines = [line for line in b.log_since(b_log) if FETCHER_TROUBLE.search(line)]
        server_lines = [line for line in a.log_since(a_log) if SERVER_TROUBLE.search(line)]
        detail = (f"{what} in {took:.1f}s ({size * 8 / took / 1e6:.2f} Mbit/s); {b.name}'s counters for {a.name}: "
                  + ", ".join(f"{k} {v}" for k, v in counts.items()))
        if counts["refused"] or counts["failed"] or counts["bad"] or fetcher_lines or server_lines:
            lines = "\n".join([f"  {b.name}.log: {line}" for line in fetcher_lines[:10]] +
                              [f"  {a.name}.log: {line}" for line in server_lines[:10]])
            raise AssertionError(f"refusals or failures in an honest transfer: {detail}\n{lines}")
        return detail

    def text_file(self) -> tuple[str, bytes]:
        """A new file of TEXT_SIZE bytes of English-like text, which bz2 compresses about as books do: (path,
        content)."""
        rng = random.Random(self.counter)
        words = ("the of and to a in that it was he his her with as had for you not be she which at on by all my "
                 "have from is this but they were so one said what would there been their could more when no "
                 "Elizabeth Darcy Bennet Jane Bingley sister mother letter house ball evening").split()
        lines = []
        while sum(len(line) + 1 for line in lines) < TEXT_SIZE:
            lines.append(" ".join(rng.choice(words) for _ in range(rng.randrange(6, 14))).capitalize() + ".")
        content = "\n".join(lines).encode()[:TEXT_SIZE]
        path = self.unique("text")
        with open(path, "wb") as f:
            f.write(content)
        return path, content

    def text_transfer(self, a: Daemon, b: Daemon) -> tuple[str, list[str], list[str]]:
        """b gets a text file a added, as honest_transfer checks it: (its detail, b's log lines and a's log lines
        written meanwhile)."""
        path, content = self.text_file()
        cid = a.out("add", path)

        def check(out):
            with open(out, "rb") as f:
                if f.read() != content:
                    raise AssertionError("the bytes differ")

        b_log, a_log = b.log_size(), a.log_size()
        detail = self.honest_transfer(a, b, cid, check, TEXT_SIZE, f"{TEXT_SIZE // KIB} KiB of text")
        return detail, b.log_since(b_log), a.log_since(a_log)

    def ranges_old(self, a: Daemon, b: Daemon) -> str:
        """b (this tree, slow) fetches from a (an old release) twice: in each, one /range is left unanswered and the
        rest comes with /blocks, nothing refused, failed or banned on either side (checked by honest_transfer); the
        second silence in a row teaches b that a doesn't answer /range, and after b restarts no /range goes to a."""
        detail, b_lines, _ = self.text_transfer(a, b)
        once = [line for line in b_lines if SILENT_ONCE in line]
        learnt = [line for line in b_lines if UNANSWERED in line]
        if (len(once), len(learnt)) != (1, 0):
            raise AssertionError(f"{b.name}'s first fetch logged {len(once)} lines saying {SILENT_ONCE!r} and "
                                 f"{len(learnt)} saying {UNANSWERED!r}, not 1 and 0: {detail}")
        again, b_lines, _ = self.text_transfer(a, b)
        learnt = [line for line in b_lines if UNANSWERED in line]
        if len(learnt) != 1 or any(SILENT_ONCE in line for line in b_lines):
            raise AssertionError(f"{b.name}'s second fetch logged {len(learnt)} lines saying {UNANSWERED!r}, not 1")
        b.stop()
        restarted = self.daemon(b.name, b.tree, b.label, NODE_CONFIG, self.tcp_port, SETTINGS + 'slow = "yes"\n',
                                "--bootstrap", self.hub.node_id)
        after, b_lines, _ = self.text_transfer(a, restarted)
        asked = [line for line in b_lines if "/range" in line]
        if asked:
            raise AssertionError(f"{b.name} asked {a.name} /range again after a restart: {asked[:3]}")
        return f"learnt after 2 silences ({learnt[0].split('rnfs ', 1)[-1][:60]}...); {detail}"

    def ranges_new(self, a: Daemon, b: Daemon) -> str:
        """b (this tree, slow) fetches a text file from a (this tree): a sends the chunks as /range answers, bz2
        compressed, and b never takes a for a node that doesn't answer /range."""
        b = next(daemon for daemon in reversed(self.daemons) if daemon.name == b.name)  # restarted by ranges_old
        detail, b_lines, a_lines = self.text_transfer(a, b)
        sent = [line for line in a_lines if "/range: sending" in line]
        compressed = [line for line in sent if "bz2" in line]
        if not compressed:
            raise AssertionError(f"{a.name} sent {len(sent)} /range answers, {len(compressed)} compressed: {detail}")
        if any(UNANSWERED in line for line in b_lines):
            raise AssertionError(f"{b.name} took {a.name} for a node that doesn't answer /range")
        return f"{len(sent)} /range answers, {len(compressed)} in bz2; {detail}"

    def names(self, a: Daemon, b: Daemon, added: dict) -> str:
        """A name a publishes resolves on b, and b's gateway serves a file through it."""
        alias = f"site-{self.counter}"
        self.counter += 1
        name = a.out("name", "create", alias)
        if added["dir"] not in a.out("name", "publish", alias, added["dir"]):
            raise AssertionError("publish didn't print the target")
        resolved = wait_for(lambda: b.cli("name", "resolve", name, check=False).stdout.decode().strip(), 90,
                            f"{b.name} to resolve {a.name}'s name")
        if resolved != added["dir"]:
            raise AssertionError(f"{b.name} resolved {resolved!r}, not {added['dir']}")
        status, body = b.gateway(f"/name/{name}/index.txt")
        if (status, body) != (200, f"hello from {a.label}\n".encode()):
            raise AssertionError(f"gateway /name/<name>/index.txt: {status} {body[:200]!r}")
        return f"resolve and gateway /name/ for {name[:12]}..."

    def keepers(self, publisher: Daemon, keeper: Daemon, resolver_tree: str, resolver_label: str) -> str:
        """keeper keeps a name publisher published; with publisher stopped, a node of resolver_tree started
        afterwards (so it never heard the name's announce, which it could take the signed record from) gets the
        record by asking keepers, and resolves it."""
        path, _ = self.random_file(10 * KIB)
        cid = publisher.out("add", path)
        alias = f"kept-{self.counter}"
        self.counter += 1
        name = publisher.out("name", "create", alias)
        publisher.cli("name", "publish", alias, cid)
        if name not in publisher.out("name", "kept"):
            raise AssertionError(f"{publisher.name} doesn't keep its own name")

        def keeper_has():
            keeper.cli("name", "keep", name, check=False)
            return cid in keeper.out("name", "kept")

        wait_for(keeper_has, 120, f"{keeper.name} to fetch the signed record from {publisher.name}")
        publisher.stop()
        resolver = self.daemon(f"resolver-{self.counter}", resolver_tree, resolver_label, NODE_CONFIG,
                               self.tcp_port, SETTINGS, "--bootstrap", self.hub.node_id)
        self.counter += 1
        start = resolver.log_size()

        def resolver_has():
            """resolver asks keepers again (keep is idempotent) until it holds the signed record."""
            resolver.cli("name", "keep", name, check=False)
            return cid in resolver.out("name", "kept")

        wait_for(resolver_has, 240, f"{resolver.name} to get the record from {keeper.name} with "
                                    f"{publisher.name} stopped")
        # Every version logs each keeper query's outcome; the record must have come from one (the keeper being
        # the only one left), not from an announce
        asked = re.compile(rf"rnfs keepers: (asked \d+ of \d+ keepers of name {name}: [1-9]\d* verifying records?)")
        evidence = wait_for(lambda: next((m.group(1) for line in resolver.log_since(start)
                                          if (m := asked.search(line))), None), 180,
                            f"{resolver.name}'s log to show a keeper query that got the record")
        resolved = resolver.out("name", "resolve", name)
        if resolved != cid:
            raise AssertionError(f"{resolver.name} resolved {resolved!r}, not {cid}")
        return f"{keeper.name} kept it; with {publisher.name} stopped, a fresh {resolver.name} {evidence}"

    # Reporting

    def table(self) -> str:
        """The results as a text table."""
        rows = [("scenario", "direction", "result", "detail")]
        rows += [(scenario, direction, "PASS" if ok else "FAIL", detail.splitlines()[0])
                 for scenario, direction, ok, detail in self.results]
        widths = [max(len(row[i]) for row in rows) for i in range(3)]
        lines = [f"{row[0]:<{widths[0]}}  {row[1]:<{widths[1]}}  {row[2]:<{widths[2]}}  {row[3]}" for row in rows]
        lines.insert(1, "-" * min(120, max(len(line) for line in lines)))
        return "\n".join(lines)


def run_one(old_tree: str, keep: bool) -> bool:
    """Run every scenario against one old tree; True if all passed."""
    base = tempfile.mkdtemp(prefix=f"rnfs-interop-{tree_version(old_tree)}-")
    network = Network(old_tree, base)
    print(f"=== {network.new_label} against {network.old_label} ({os.path.abspath(old_tree)}); files in {base}",
          flush=True)
    try:
        if network.check("startup", "every daemon", network.start):
            network.run()
    finally:
        network.stop_all()
    passed = all(ok for _, _, ok, _ in network.results)
    print(f"\n=== {network.new_label} against {network.old_label}\n{network.table()}\n", flush=True)
    for scenario, direction, ok, detail in network.results:
        if not ok and "\n" in detail:
            print(f"{scenario} {direction}:\n{detail}\n")
    if passed and not keep:
        shutil.rmtree(base, ignore_errors=True)
    else:
        print(f"daemon directories and logs kept in {base}")
    return passed


def main():
    parser = argparse.ArgumentParser(description="Run this tree's daemons against older releases' daemons.")
    parser.add_argument("--old", action="append", required=True, metavar="DIR",
                        help="an old release's source tree (a `git archive vX.Y.Z` extract); repeat for several")
    parser.add_argument("--keep", action="store_true", help="keep the daemon directories and logs even on success")
    args = parser.parse_args()
    for tree in args.old:
        if not os.path.isfile(os.path.join(tree, "rnfs.py")):
            parser.error(f"{tree}: no rnfs.py; give a release's source tree")
    results = [run_one(tree, args.keep) for tree in args.old]
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
