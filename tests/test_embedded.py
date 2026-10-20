"""An rnfs node inside another program (design.md, "Embedded mode"): the guards' scope, the rnfs dir's lock, and
a two-process test over real RNS (RNFS_E2E=1) of a host program running a node beside its own destinations.

That test runs this file as a script in two subprocesses with their own Reticulum, over local TCP: a plain RNS
peer, and a host program with an embedded node. The host's destinations must send and receive what RNS alone
allows (compressed Resources, requests carried as Resources, multi-segment Resources) while the node runs, and
its rnfs.dht links must still refuse each of those.
"""
import json
import os
import subprocess
import sys
import random
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import RNS  # noqa: E402
from RNS.vendor import umsgpack  # noqa: E402

from rnfs import fsutil  # noqa: E402
from rnfs.config import LOCK_FILE, Config  # noqa: E402
from rnfs.content.cid import RAW, Cid  # noqa: E402
from rnfs.network import client, guards, links  # noqa: E402
from rnfs.node import DirInUse, lock_rnfs_dir  # noqa: E402

import tests.test_links as fakes  # noqa: E402  (a module, so its test cases don't run twice)
from tests.support import free_port  # noqa: E402

DHT_LINK_NAME = "rnfs.dht." + "ab" * 16  # as RNS.Destination.expand_name names an rnfs.dht destination
HOLD_LOCK = ("import sys; from rnfs.config import Config; from rnfs.node import lock_rnfs_dir; "
             "lock_rnfs_dir(Config(rnfs_dir=sys.argv[1])); print('locked', flush=True); sys.stdin.read()")


def link_named(name: str, **attributes):
    """A stand-in RNS.Link whose destination has name."""
    return types.SimpleNamespace(destination=types.SimpleNamespace(name=name), pending_requests=[], **attributes)


def advertisement(link, transfer: int = 1052, data: int = 1000, request: bool = False, compressed: bool = False,
                  metadata: bool = False, segments: int = 1):
    """A packet carrying a crafted, packed ResourceAdvertisement (flags as RNS 1.5.6 packs them)."""
    flags = (0x01 | int(compressed) << 1 | int(segments > 1) << 2 | int(request) << 3 | int(not request) << 4
             | int(metadata) << 5)
    packed = umsgpack.packb({"t": transfer, "d": data, "n": 1, "h": bytes(32), "r": bytes(4), "o": bytes(32),
                             "i": 1, "l": segments, "q": b"q" * 16, "f": flags, "m": bytes(4)})
    return types.SimpleNamespace(plaintext=packed, link=link)


# What a hostile or careless peer may advertise, and whether the daemon (PROCESS) refuses it on another program's
# link; on rnfs.dht links both scopes refuse every one
HOSTILE = {
    "request": (dict(request=True), True),
    "transfer over data": (dict(transfer=2_000_000, data=1000), True),
    "transfer over the largest response": (dict(transfer=guards.MAX_TRANSFER_SIZE + 1,
                                                data=guards.MAX_TRANSFER_SIZE + 1), True),
    "malformed sizes": (dict(transfer=-1), True),
    "multi-segment": (dict(segments=3), False),
    "metadata": (dict(metadata=True), False),
    "compressed": (dict(compressed=True), False),  # the daemon's process-wide limit of 0 fails it instead
}


class GuardScopeTest(unittest.TestCase):
    """guards.install(scope): every link in the daemon, rnfs.dht links only in an embedded node."""

    def setUp(self):
        """Record what the guard passes to RNS and rejects; restore the scope and RNS's limit afterwards."""
        saved = guards._scope, guards._compress_limit, RNS.Resource.AUTO_COMPRESS_MAX_SIZE

        def restore():
            """Put the scope and the process-wide limit back as they were."""
            guards._scope, guards._compress_limit, RNS.Resource.AUTO_COMPRESS_MAX_SIZE = saved
        self.addCleanup(restore)
        self.accepted, self.rejected = [], []
        self.resource = mock.Mock(spec=RNS.Resource)
        self.resource.max_decompressed_size = 123
        patches = [mock.patch.object(guards, "_original_accept",
                                     lambda packet, **kwargs: self.accepted.append(packet) or self.resource),
                   mock.patch.object(RNS.Resource, "reject", self.rejected.append)]
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    def accepts(self, link, **kwargs) -> bool:
        """True if the installed guard passes an advertisement on link to RNS."""
        packet = advertisement(link, **kwargs)
        accepted = RNS.Resource.accept(packet) is not None
        self.assertEqual(accepted, packet in self.accepted)
        self.assertEqual(not accepted, packet in self.rejected)
        return accepted

    def test_rnfs_links_refuse_the_same_in_both_scopes(self):
        """On rnfs.dht links both scopes refuse every hostile advertisement and accept an honest one."""
        for scope in (guards.PROCESS, guards.DHT):
            guards.install(scope)
            for name, (kwargs, _) in HOSTILE.items():
                with self.subTest(scope=scope, advertisement=name):
                    self.assertFalse(self.accepts(link_named(DHT_LINK_NAME), **kwargs))
            with self.subTest(scope=scope, advertisement="honest"):
                self.assertTrue(self.accepts(link_named(DHT_LINK_NAME)))

    def test_daemon_guards_every_link(self):
        """The daemon's scope refuses on other links what it always did."""
        guards.install(guards.PROCESS)
        for name, (kwargs, refused) in HOSTILE.items():
            with self.subTest(advertisement=name):
                self.assertEqual(self.accepts(link_named("hostapp.chat"), **kwargs), not refused)

    def test_embedded_node_leaves_other_links_to_rns(self):
        """An embedded node's scope refuses nothing on other links."""
        guards.install(guards.DHT)
        for name, (kwargs, _) in HOSTILE.items():
            with self.subTest(advertisement=name):
                self.assertTrue(self.accepts(link_named("hostapp.chat"), **kwargs))
        self.assertTrue(self.accepts(None, request=True))  # a packet without a link isn't rnfs's either

    def test_daemon_messages_are_unchanged(self):
        """A compressed advertisement that something else already refused is logged for that, as before."""
        guards.install(guards.PROCESS)
        adv = RNS.ResourceAdvertisement.unpack(advertisement(None, request=True, compressed=True).plaintext)
        reason = guards.refusal(adv, link_named(DHT_LINK_NAME))
        self.assertEqual(reason, "requests are never sent as Resources")

    def test_compression_limit_follows_the_scope(self):
        """PROCESS sets RNS's limit to 0; DHT puts the program's back."""
        RNS.Resource.AUTO_COMPRESS_MAX_SIZE = 64 * 1024 * 1024
        guards._compress_limit = None
        guards.install(guards.PROCESS)
        self.assertEqual(RNS.Resource.AUTO_COMPRESS_MAX_SIZE, 0)
        guards.install()  # RequestServer and LinkPool keep the scope the node set
        self.assertEqual((guards.scope(), RNS.Resource.AUTO_COMPRESS_MAX_SIZE), (guards.PROCESS, 0))
        guards.install(guards.DHT)  # the host program's value comes back
        self.assertEqual(RNS.Resource.AUTO_COMPRESS_MAX_SIZE, 64 * 1024 * 1024)
        guards.install()
        self.assertEqual((guards.scope(), RNS.Resource.AUTO_COMPRESS_MAX_SIZE), (guards.DHT, 64 * 1024 * 1024))
        with self.assertRaises(ValueError):
            guards.install("everything")

    def test_rnfs_resources_accepted_decompress_nothing(self):
        """An accepted rnfs.dht Resource may decompress nothing; another link's keeps its limit."""
        for scope in (guards.PROCESS, guards.DHT):
            guards.install(scope)
            with self.subTest(scope=scope):
                self.resource.max_decompressed_size = 123
                self.assertTrue(self.accepts(link_named(DHT_LINK_NAME)))
                self.assertEqual(self.resource.max_decompressed_size, 0)
        self.resource.max_decompressed_size = 123
        self.assertTrue(self.accepts(link_named("hostapp.chat")))
        self.assertEqual(self.resource.max_decompressed_size, 123)

    def test_response_checks_pass_other_links_through(self):
        """Another link's response packets go to RNS undecrypted."""
        received = []
        with mock.patch.object(guards, "_original_receive", lambda link, packet: received.append(packet)):
            for scope in (guards.PROCESS, guards.DHT):
                guards.install(scope)
                packet = types.SimpleNamespace(context=RNS.Packet.RESPONSE, packet_type=RNS.Packet.DATA,
                                               data=bytes(guards.PACKET_CHECK_SIZE + 1))

                def decrypt(data):
                    """The host's link is never decrypted by rnfs."""
                    raise AssertionError("decrypted")
                guards.guarded_receive(link_named("hostapp.chat", initiator=False, decrypt=decrypt), packet)
                self.assertEqual(received[-1:], [packet])


class OnePacketRequestTest(unittest.TestCase):
    """LinkPool never sends a request that would go as a Resource (which RNS compresses unless told not to)."""

    def setUp(self):
        """A LinkPool over fake RNS."""
        patcher = mock.patch.object(links, "RNS", fakes.fake_rns())
        patcher.start()
        self.addCleanup(patcher.stop)
        fakes.FakeLink.created = []
        fakes.FakeLink.establish_delay = 0.0
        fakes.FakeLink.responder = staticmethod(lambda link, path, data: True)
        self.pool = links.LinkPool(object(), Config())
        self.addCleanup(self.pool.close_all)

    def test_oversized_request_is_not_sent(self):
        """A request over one packet isn't sent; one that fits is."""
        peer = fakes.run(self, lambda: self.pool.get(bytes(16)))
        exchange = fakes.run(self, lambda: self.pool.request(peer, "/find", os.urandom(1000)))
        self.assertIsNone(exchange.receipt)
        self.assertEqual(peer.link.requests, [])
        exchange = fakes.run(self, lambda: self.pool.request(peer, "/find", bytes(16)))
        self.assertIsNotNone(exchange.receipt)
        self.assertEqual([path for path, _, _ in peer.link.requests], ["/find"])


class DirLockTest(unittest.TestCase):
    """lock_rnfs_dir: one daemon or embedded node per rnfs dir, in this process or another."""

    def setUp(self):
        """A temporary rnfs dir."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.config = Config(rnfs_dir=os.path.join(tmp.name, "rnfs"))

    def test_second_lock_in_this_process_is_refused(self):
        """A second lock in this process is DirInUse, naming it."""
        fd = lock_rnfs_dir(self.config)
        try:
            with self.assertRaises(DirInUse) as caught:
                lock_rnfs_dir(self.config)
        finally:
            fsutil.unlock(fd)
        self.assertEqual(caught.exception.holder, os.getpid())
        self.assertIn(f"another rnfs daemon or node (process {os.getpid()}) is using {self.config.root}",
                      str(caught.exception))
        fsutil.unlock(lock_rnfs_dir(self.config))  # released once unlocked

    def test_lock_held_by_another_process_is_refused(self):
        """A lock another process holds is DirInUse, naming it."""
        env = {**os.environ, "PYTHONPATH": ROOT}
        holder = subprocess.Popen([sys.executable, "-c", HOLD_LOCK, self.config.rnfs_dir], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, encoding="utf-8", env=env)
        self.addCleanup(holder.wait, 10)
        self.addCleanup(holder.stdin.close)
        self.addCleanup(holder.stdout.close)
        self.assertEqual(holder.stdout.readline().strip(), "locked")
        with self.assertRaises(DirInUse) as caught:
            lock_rnfs_dir(self.config)
        self.assertEqual(caught.exception.holder, holder.pid)
        self.assertEqual(fsutil.lock_holder(os.path.join(self.config.root, LOCK_FILE)), holder.pid)


# Two processes over real RNS: a plain RNS peer, and a host program with an embedded node

PEER_CONFIG = """
[reticulum]
  enable_transport = No
  share_instance = No
[logging]
  loglevel = 2
[interfaces]
  [[TCP Server]]
    type = TCPServerInterface
    enabled = yes
    listen_ip = 127.0.0.1
    listen_port = {port}
"""

HOST_CONFIG = """
[reticulum]
  enable_transport = No
  share_instance = No
[logging]
  loglevel = 2
[interfaces]
  [[TCP Client]]
    type = TCPClientInterface
    enabled = yes
    target_host = 127.0.0.1
    target_port = {port}
"""

ALONE_CONFIG = """
[reticulum]
  enable_transport = No
  share_instance = No
[logging]
  loglevel = 2
[interfaces]
"""

APP = "rnfstest"  # the host programs' own app name
BIG = 3_000_000  # over RNS's 1 MiB segment: a multi-segment Resource, and very compressible (zeros)
REQUEST = 600_000  # a request over the link MDU, so RNS sends it as a Resource (one segment, compressible)
DHT_REQUEST = 800  # over the link MDU, within rnfs.dht's max_request_size
# Blocks the peer's fake rnfs.dht server answers /blocks with: random (RNS sends it uncompressed, the control),
# and zeros (RNS compresses it); a request for BIG_CID gets BIG bytes, a multi-segment Resource
RANDOM_BLOCK = random.Random(7).randbytes(60_000)
ZERO_BLOCK = bytes(60_000)
BIG_CID = Cid.for_block(RAW, b"big")
WAIT = 60.0  # seconds any one exchange may take over local TCP


class Recorder:
    """What arrives on a program's own links: every Resource RNS starts receiving (its compressed flag and
    segment), and the plain Resources completed."""

    def __init__(self):
        """Nothing seen yet."""
        self.started: list[dict] = []
        self.completed: list[int] = []

    def watch(self, link):
        """Record link's incoming Resources, accepting plain ones as RNS's ACCEPT_ALL does."""
        link.set_resource_strategy(RNS.Link.ACCEPT_ALL)
        # The only one-segment Resource each side receives is the /echo request; the multi-segment ones are the
        # /echo response and the plain Resource (RNS sets no request or response flag before this is called)
        link.set_resource_started_callback(
            lambda resource: self.started.append({"compressed": bool(resource.compressed),
                                                  "hash": resource.original_hash.hex(),
                                                  "segment": resource.segment_index,
                                                  "segments": resource.total_segments}))
        link.set_resource_concluded_callback(self._concluded)

    def _concluded(self, resource):
        """A plain Resource completed: record its size."""
        if resource.status == RNS.Resource.COMPLETE:
            data = resource.data
            self.completed.append(len(data.read()) if hasattr(data, "read") else len(data))

    def summary(self) -> dict:
        """What was seen, for the test to check."""
        return {"started": self.started, "completed": self.completed}


def app_destination(recorder: Recorder) -> "RNS.Destination":
    """The program's own destination: /echo answers any request with BIG zero bytes, RNS compressing it."""
    identity = RNS.Identity()
    destination = RNS.Destination(identity, RNS.Destination.IN, RNS.Destination.SINGLE, APP, "echo")
    destination.register_request_handler("/echo",
                                         response_generator=lambda path, data, request_id, identity, at: bytes(BIG),
                                         allow=RNS.Destination.ALLOW_ALL)  # auto_compress: RNS's default
    destination.set_link_established_callback(recorder.watch)
    return destination


def open_link(destination_hash: bytes, app: str, aspect: str, recorder: Recorder | None = None):
    """An active link to destination_hash's app.aspect destination, or None."""
    RNS.Transport.request_path(destination_hash)
    deadline = time.monotonic() + WAIT
    while RNS.Identity.recall(destination_hash) is None and time.monotonic() < deadline:
        time.sleep(0.1)
    identity = RNS.Identity.recall(destination_hash)
    if identity is None:
        return None
    link = RNS.Link(RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE, app, aspect))
    if recorder is not None:
        recorder.watch(link)
    while link.status != RNS.Link.ACTIVE and time.monotonic() < deadline:
        time.sleep(0.1)
    return link if link.status == RNS.Link.ACTIVE else None


def request(link, path: str, data):
    """Send a request on link and wait: the response's length (bytes) or the response itself (anything else),
    or None if it failed or timed out."""
    receipt = link.request(path, data)
    deadline = time.monotonic() + WAIT
    while receipt and receipt.get_status() not in (RNS.RequestReceipt.READY, RNS.RequestReceipt.FAILED) \
            and time.monotonic() < deadline:
        time.sleep(0.1)
    if not receipt or receipt.get_status() != RNS.RequestReceipt.READY:
        return None
    response = receipt.response
    if hasattr(response, "read"):
        response = response.read()
    return len(response) if isinstance(response, (bytes, bytearray)) else response


def send_resource(link, data: bytes, timeout: float = WAIT) -> bool:
    """Send data as a plain Resource on link and wait for its last segment's outcome; True if it completed."""
    concluded, outcome = threading.Event(), []
    RNS.Resource(data, link, callback=lambda resource: (outcome.append(resource.status), concluded.set()))
    concluded.wait(timeout)
    return outcome == [RNS.Resource.COMPLETE]


def exercise(target_hash: bytes, recorder: Recorder) -> dict:
    """What this program's links can do with another's /echo destination: a request carried as a compressed
    Resource answered by a compressed multi-segment one, and a compressed multi-segment plain Resource."""
    link = open_link(target_hash, APP, "echo", recorder)
    if link is None:
        return {"linked": False}
    result = {"linked": True, "echo": request(link, "/echo", bytes(REQUEST)), "resource": send_resource(link,
                                                                                                          bytes(BIG))}
    link.teardown()
    return result


def run_peer(configdir: str):
    """The plain RNS peer: its /echo destination, and a fake rnfs.dht server answering /find with Resources a
    node refuses; then, given the host's destinations on stdin, what it can do with them. Prints JSON."""
    RNS.Reticulum(configdir=configdir)
    recorder = Recorder()
    own = app_destination(recorder)
    dht = RNS.Destination(RNS.Identity(), RNS.Destination.IN, RNS.Destination.SINGLE, "rnfs", "dht")
    blocks = {Cid.for_block(RAW, RANDOM_BLOCK).binary: RANDOM_BLOCK, Cid.for_block(RAW, ZERO_BLOCK).binary: ZERO_BLOCK}

    def answer_blocks(path, data, request_id, identity, at):
        """[block, ...] for the CIDs asked for; BIG bytes for BIG_CID."""
        cids = data[1]
        if cids == [BIG_CID.binary]:
            return bytes(BIG)
        return [blocks.get(binary) for binary in cids]
    dht.register_request_handler("/blocks", response_generator=answer_blocks,
                                 allow=RNS.Destination.ALLOW_ALL)  # RNS's default auto_compress
    own.announce()
    dht.announce()
    print("PEER", own.hash.hex(), dht.hash.hex(), flush=True)
    host_echo, host_dht = (bytes.fromhex(text) for text in sys.stdin.readline().split()[1:3])
    results = exercise(host_echo, recorder)
    # The embedded node's rnfs.dht: a request carried as a Resource is refused, a one-packet one answered
    link = open_link(host_dht, "rnfs", "dht")
    results["dht_linked"] = link is not None
    if link is not None:
        results["dht_small"] = request(link, "/find", bytes(16))
        # Over one packet on a default-MTU link (this TCP one negotiated a larger MTU, so its MDU is lowered here)
        # and within rnfs.dht's max_request_size, which RNS checks itself: rnfs's guard is what refuses it
        link.mdu = RNS.Link.MDU
        results["dht_request_resource"] = request(link, "/find", os.urandom(DHT_REQUEST))
        results["dht_resource"] = send_resource(link, bytes(BIG), timeout=15)  # RNS's own ACCEPT_NONE, as ever
        link.teardown()
    print("EXERCISED", flush=True)
    sys.stdin.readline()  # the host has finished with this process's destinations
    print("RESULT", json.dumps({**results, "seen": recorder.summary()}), flush=True)
    os._exit(0)


def run_host(configdir: str, rnfs_dir: str, peer_echo_hex: str, peer_dht_hex: str):
    """The host program: its own Reticulum and /echo destination with an embedded node beside them. Prints
    its destinations, then what its links could do with the peer's and what the node's links refused."""
    import rnfs

    reticulum = RNS.Reticulum(configdir=configdir)
    recorder = Recorder()
    own = app_destination(recorder)
    refused = []
    refusal = guards.refusal

    def recording_refusal(adv, link=None):
        """guards.refusal, recording each refusal's reason."""
        reason = refusal(adv, link)
        if reason is not None:
            refused.append(reason)
        return reason
    guards.refusal = recording_refusal
    # The DHT mode is left to rnfs.Node's default: client
    settings = {"default_bootstrap": False, "startup_announce_delay": 2, "path_timeout": WAIT}
    # A start failing late leaves nothing behind (its rnfs.dht destination above all), so the next one works
    from rnfs.node import RnfsNode
    with mock.patch.object(RnfsNode, "_start_network", side_effect=RuntimeError("failing on purpose")):
        try:
            rnfs.Node(rnfs_dir, reticulum=reticulum, settings=settings)
        except RuntimeError:
            pass
    node = rnfs.Node(rnfs_dir, reticulum=reticulum, settings=settings)
    own.announce()
    print("HOST", own.hash.hex(), node.node_id.hex(), flush=True)
    results = exercise(bytes.fromhex(peer_echo_hex), recorder)
    peer_dht = bytes.fromhex(peer_dht_hex)
    results["blocks"] = {name: node.internal.client.blocks(peer_dht, [cid]).status for name, cid in (
        ("random", Cid.for_block(RAW, RANDOM_BLOCK)), ("zeros", Cid.for_block(RAW, ZERO_BLOCK)), ("big", BIG_CID))}
    results["compress_limit"] = RNS.Resource.AUTO_COMPRESS_MAX_SIZE
    results["lxmf_imported"] = "LXMF" in sys.modules
    results["dht_mode"] = node.status().dht_mode
    print("EXERCISED", flush=True)
    sys.stdin.readline()  # the peer has finished with this process's destinations
    results["refused"] = refused
    results["seen"] = recorder.summary()
    node.close()
    # Reticulum is the host's, and still running: its links work as before
    results["reticulum_kept"] = RNS.Reticulum.get_instance() is reticulum
    link = open_link(bytes.fromhex(peer_echo_hex), APP, "echo")
    results["after_stop"] = request(link, "/echo", b"x") if link is not None else None
    print("RESULT", json.dumps(results), flush=True)
    os._exit(0)


def run_links_gate(configdir: str, rnfs_dir: str):
    """A node on a Reticulum of its own without interfaces: whether its rnfs.dht destination accepted links while it
    was set up, and once it had started. Prints RESULT and a JSON object."""
    import rnfs
    from rnfs.node import RnfsNode

    reticulum = RNS.Reticulum(configdir=configdir)
    seen = {}
    setup = RnfsNode._setup_or_close

    def recording(node, *args, **kwargs):
        """RnfsNode._setup_or_close, noting whether links were accepted meanwhile."""
        seen["during_setup"] = node.destination.accepts_links()
        return setup(node, *args, **kwargs)

    with mock.patch.object(RnfsNode, "_setup_or_close", recording):
        node = rnfs.Node(rnfs_dir, reticulum=reticulum, settings={"default_bootstrap": False})
    seen["started"] = node.internal.destination.accepts_links()
    node.close()
    print("RESULT", json.dumps(seen), flush=True)
    os._exit(0)


class LinkGateTest(unittest.TestCase):
    """A node accepts links on its rnfs.dht destination only once it can answer requests on them."""

    def test_no_links_while_the_node_is_set_up(self):
        """RNS accepts a link to a destination as soon as it exists and drops requests without a handler: through a
        node's setup (a minute on a large store, minutes at an upgrade's first start), every request other nodes sent
        waited out its timeout. Links are refused until the request server is in place."""
        directory = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(directory.cleanup)
        configdir = os.path.join(directory.name, "rns")
        os.makedirs(configdir)
        with open(os.path.join(configdir, "config"), "w", encoding="utf-8") as f:
            f.write(ALONE_CONFIG)
        env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONPATH": ROOT}
        done = subprocess.run([sys.executable, os.path.abspath(__file__), "--links-gate", configdir,
                               os.path.join(directory.name, "rnfs")], capture_output=True, encoding="utf-8",
                              errors="replace", env=env, timeout=120)
        lines = [line for line in done.stdout.splitlines() if line.startswith("RESULT ")]
        self.assertTrue(lines, done.stdout + done.stderr)
        self.assertEqual(json.loads(lines[-1][len("RESULT "):]), {"during_setup": False, "started": True})


@unittest.skipUnless(os.environ.get("RNFS_E2E") == "1", "set RNFS_E2E=1 to run")
class HostProgramTest(unittest.TestCase):
    """A host program's own destinations beside an embedded node, against a plain RNS peer."""

    @staticmethod
    def stop(proc: subprocess.Popen):
        """End a subprocess (it exits when its stdin closes) and close its pipes."""
        try:
            if not proc.stdin.closed:
                proc.stdin.close()
            proc.wait(timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            proc.kill()
            proc.wait()
        proc.stdout.close()

    @staticmethod
    def read_line(proc: subprocess.Popen, prefix: str, output: list) -> str | None:
        """What follows prefix on the next line of proc's output starting with it (output collects every line
        read, for failure messages); None if proc ends first."""
        for line in iter(proc.stdout.readline, ""):
            output.append(line)
            if line.split(" ", 1)[0].strip() == prefix:
                return line[len(prefix):].strip()
        return None

    @staticmethod
    def send(proc: subprocess.Popen, line: str):
        """Write line to proc's stdin."""
        proc.stdin.write(line + "\n")
        proc.stdin.flush()

    def start(self, *args) -> subprocess.Popen:
        """Run this file as a script with args."""
        env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONPATH": ROOT}
        proc = subprocess.Popen([sys.executable, os.path.abspath(__file__), *args], stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8",
                                errors="replace", env=env)
        self.addCleanup(self.stop, proc)
        return proc

    def test_host_destinations_beside_an_embedded_node(self):
        """The host's destinations work as with RNS alone while the node's rnfs.dht links refuse what they always
        did."""
        # Removed after the subprocesses stop (their cleanups, registered later, run first); one killed on Windows
        # may not have released its files yet: don't fail the test over cleanup
        directory = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(directory.cleanup)
        tmp = directory.name
        port = free_port()
        dirs = {}
        for name, template in (("peer", PEER_CONFIG), ("host", HOST_CONFIG)):
            dirs[name] = os.path.join(tmp, name)
            os.makedirs(dirs[name])
            with open(os.path.join(dirs[name], "config"), "w", encoding="utf-8") as f:
                f.write(template.format(port=port))
        peer_output, host_output = [], []
        peer = self.start("--peer", dirs["peer"])
        peer_hashes = (self.read_line(peer, "PEER", peer_output) or "").split()
        self.assertEqual(len(peer_hashes), 2, "".join(peer_output))
        host = self.start("--host", dirs["host"], os.path.join(tmp, "rnfs"), *peer_hashes)
        host_hashes = self.read_line(host, "HOST", host_output)
        self.assertIsNotNone(host_hashes, "".join(host_output))
        # Each exercises the other's destinations at once, then waits to be told to report
        self.send(peer, f"HOST {host_hashes}")
        self.assertIsNotNone(self.read_line(peer, "EXERCISED", peer_output), "".join(peer_output))
        self.assertIsNotNone(self.read_line(host, "EXERCISED", host_output), "".join(host_output))
        # The host first: after stopping its node it still uses the peer's destination
        self.send(host, "")
        host_result = self.read_line(host, "RESULT", host_output)
        self.assertIsNotNone(host_result, "".join(host_output))
        self.send(peer, "")
        peer_result = self.read_line(peer, "RESULT", peer_output)
        self.assertIsNotNone(peer_result, "".join(peer_output))
        peer_results, host_results = json.loads(peer_result), json.loads(host_result)
        detail = json.dumps({"peer": peer_results, "host": host_results}, indent=1)

        # The host's own destinations, with the node running, behave as with RNS alone, both ways
        for side in (peer_results, host_results):
            self.assertTrue(side["linked"], detail)
            self.assertEqual(side["echo"], BIG, detail)  # a request carried as a Resource, a multi-segment answer
            self.assertTrue(side["resource"], detail)  # a multi-segment plain Resource
            self.assertIn(BIG, side["seen"]["completed"], detail)
            started = side["seen"]["started"]
            # Compressed: the request Resource arriving at /echo, and every segment of the response and of the
            # plain Resource
            self.assertTrue(all(r["compressed"] for r in started), detail)
            self.assertEqual(sum(r["segments"] == 1 for r in started), 1, detail)
            multi = {r["hash"]: r["segments"] for r in started if r["segments"] > 1}
            self.assertEqual(len(multi), 2, detail)
            for original, segments in multi.items():
                self.assertEqual(sorted(r["segment"] for r in started if r["hash"] == original),
                                 list(range(1, segments + 1)), detail)
        self.assertNotEqual(host_results["compress_limit"], 0, detail)  # the host's process-wide setting is its own

        # The node's rnfs.dht links still refuse each of those
        self.assertTrue(peer_results["dht_linked"], detail)
        self.assertIsNotNone(peer_results["dht_small"], detail)  # the link works for a one-packet request
        self.assertIsNone(peer_results["dht_request_resource"], detail)
        self.assertFalse(peer_results["dht_resource"], detail)
        self.assertEqual(host_results["blocks"]["random"], client.OK, detail)  # the control: an honest answer
        self.assertNotEqual(host_results["blocks"]["zeros"], client.OK, detail)  # compressed, one segment
        self.assertNotEqual(host_results["blocks"]["big"], client.OK, detail)  # compressed, multi-segment
        self.assertIn("requests are never sent as Resources", host_results["refused"], detail)
        self.assertIn("rnfs Resources are never compressed", host_results["refused"], detail)
        self.assertTrue(all(reason in ("requests are never sent as Resources", "rnfs Resources are never compressed")
                            or reason.startswith("rnfs responses are one segment")
                            for reason in host_results["refused"]), detail)  # nothing of the host's refused

        # rnfs has no LXMF since 0.2.0: not even the module is loaded; the host's Reticulum survives the node's stop
        self.assertFalse(host_results["lxmf_imported"], detail)
        self.assertEqual(host_results["dht_mode"], "client", detail)
        self.assertTrue(host_results["reticulum_kept"], detail)
        self.assertEqual(host_results["after_stop"], BIG, detail)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--peer"]:
        run_peer(sys.argv[2])
    elif sys.argv[1:2] == ["--host"]:
        run_host(*sys.argv[2:6])
    elif sys.argv[1:2] == ["--links-gate"]:
        run_links_gate(*sys.argv[2:4])
    else:
        unittest.main()
