"""Tests for fetching targets (rnfs/fetcher.py; specification sections 6 and 7) over a fake client and lookup."""
import os
import shutil
import random
import tempfile
import threading
import time
import unittest
from collections import deque
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE, MAX_BLOCK_SIZE, MAX_REQUEST_SIZE, MAX_RESPONSE_SIZE, RESPONSE_OVERHEAD
from rnfs.content import builder, dag, walker
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.dag import Child, Entry, FileNode
from rnfs.content import storage as storage_module
from rnfs.content.storage import Storage
from rnfs.dht.routing import RoutingTable
from rnfs.errors import InvalidDag, NotFound, StorageFull, TooLarge
from rnfs import fetcher as fetcher_module
from rnfs.fetcher import Fetcher
from rnfs.network import protocol, server
from rnfs.network.client import BUSY, FAILED, OK, STALLED, BlocksResult
from tests.support import failing_commits, state_execute, state_rows

OWN = bytes(16)
P1, P2, P3, P4 = (bytes([i]) * 16 for i in (1, 2, 3, 4))
BATCH = 4  # CIDs the fake client sends per request, like a packet limit

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the fetcher's log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class FakeClient:
    """Stands in for PeerClient: each node holds a dict of blocks, or behaves as a test function says."""

    def __init__(self):
        """No nodes, no open links."""
        self.holdings: dict[bytes, dict[Cid, bytes]] = {}
        self.behaviour: dict[bytes, object] = {}  # node -> f(cids) -> BlocksResult, or None for the default
        self.links: list[bytes] = []
        self.rate: float | None = None
        self.requests: list[tuple[bytes, list[Cid]]] = []
        self.closed: list[bytes] = []
        self.lock = threading.Lock()

    def open_links(self) -> list[bytes]:
        """Nodes with an open link."""
        return list(self.links)

    def rate_estimate(self, node_id: bytes):
        """The same estimate for every link."""
        return self.rate

    def close(self, node_id: bytes):
        """Close the link to node_id (noted)."""
        with self.lock:
            self.closed.append(node_id)

    def blocks(self, node_id: bytes, cids: list[Cid], on_progress=None) -> BlocksResult:
        """Serve what node_id holds, BATCH CIDs per request."""
        with self.lock:
            self.requests.append((node_id, list(cids)))
        self.on_progress = on_progress
        behaviour = self.behaviour.get(node_id)
        if behaviour is not None:
            result = behaviour(cids)
            if result is not None:
                return result
        held = self.holdings.get(node_id, {})
        result = BlocksResult(OK)
        for cid in cids[:BATCH]:
            if cid in held:
                result.blocks[cid] = held[cid]
            else:
                result.missing.append(cid)
        result.unsent = list(cids[BATCH:])
        return result

    def asked(self, node_id: bytes) -> list[Cid]:
        """Every CID sent to node_id."""
        return [cid for node, cids in self.requests if node == node_id for cid in cids[:BATCH]]


class FakeLookup:
    """A provider lookup whose runs report fixed providers."""

    def __init__(self, runs: list[list[bytes]]):
        """runs[i] is what the i-th run of find_providers reports."""
        self.runs = runs
        self.count = 0

    def find_providers(self, on_provider, should_stop=lambda: False):
        """Report this run's providers."""
        run = self.runs[self.count] if self.count < len(self.runs) else []
        self.count += 1
        for provider in run:
            on_provider(provider)
        return list(run)

    def stop(self):
        """Nothing to wake."""

    def allow_unknown(self, node_id: bytes) -> bool:
        """Every identity is known."""
        return True

    def remaining(self) -> float:
        """Time is never up."""
        return 100.0


class FetcherTest(unittest.TestCase):
    """A Fetcher over temporary storage, a FakeClient and FakeLookups."""

    def setUp(self):
        """Empty storage; the content of the 'network' is built into self.network."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = Config(rnfs_dir=self.tmp.name, max_parallel_providers=3, slow="no")
        self.storage = Storage(os.path.join(self.tmp.name, "blocks"), os.path.join(self.tmp.name, "state"), 10**9)
        self.addCleanup(self.storage.close)
        self.routing = RoutingTable(OWN, self.config)
        self.client = FakeClient()
        self.network: dict[Cid, bytes] = {}
        self.lookups: list[FakeLookup] = []
        self.runs: list[list[bytes]] = []
        self.fetcher = self.make_fetcher()

    def make_fetcher(self, slow: bool = False) -> Fetcher:
        """A fetcher whose lookups report self.runs."""
        def make_lookup(key):
            """A new FakeLookup, remembered."""
            lookup = FakeLookup(self.runs)
            self.lookups.append(lookup)
            return lookup
        fetcher = Fetcher(self.storage, self.client, self.config, OWN, self.routing, make_lookup, slow,
                          os.path.join(self.tmp.name, "state"), busy_delay=0.01)
        self.addCleanup(fetcher.close)
        return fetcher

    def put(self, cid: Cid, block: bytes):
        """Builder callback storing into the network."""
        self.network[cid] = block

    def file(self, data: bytes) -> builder.Built:
        """A file on the network."""
        return builder.build_bytes(data, self.put)

    def hold(self, node_id: bytes, cids=None):
        """node_id holds every network block (or just cids)."""
        self.client.holdings[node_id] = dict(self.network) if cids is None else {c: self.network[c] for c in cids}

    def fetch_all(self, root: Cid, names=(), **kwargs) -> walker.Expect:
        """Fetch a whole DAG."""
        with self.fetcher.open(root, names, **kwargs) as fetch:
            target = fetch.resolve()
            fetch.fetch_dag(target)
        return target

    def read(self, root: Cid) -> bytes:
        """A stored file's bytes."""
        return b"".join(walker.read_range(walker.root_expect(root), self.storage.getter()))


class ProviderDiscoveryTest(FetcherTest):
    """Open links first, then the lookup; --from bypasses both."""

    def test_fetch_from_lookup_provider(self):
        data = os.urandom(5 * CHUNK_SIZE + 10)
        built = self.file(data)
        self.hold(P1)
        self.runs.append([P1])
        self.fetch_all(built.cid)
        self.assertEqual(self.read(built.cid), data)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual(len(self.lookups), 1)

    def test_open_link_nodes_are_asked_first(self):
        built = self.file(os.urandom(3 * CHUNK_SIZE))
        self.hold(P1)
        self.client.links = [P1]
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual(self.lookups, [])  # no lookup needed

    def test_open_link_without_the_root_falls_back_to_lookup(self):
        built = self.file(os.urandom(3 * CHUNK_SIZE))
        self.hold(P2)
        self.client.links = [P1]  # holds nothing
        self.runs.append([P2])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual(self.client.asked(P1), [built.cid])  # asked only for the root
        self.assertFalse(self.routing.is_banned(P1))

    def test_from_node_bypasses_the_dht(self):
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        self.hold(P3)
        self.client.links = [P1]
        self.fetch_all(built.cid, from_node=P3)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual(self.lookups, [])
        self.assertEqual(self.client.asked(P1), [])

    def test_from_node_failure_is_not_found(self):
        built = self.file(b"x" * 100)
        with self.assertRaises(NotFound):
            self.fetch_all(built.cid, from_node=P3)

    def test_no_providers(self):
        built = self.file(b"nobody has this")
        self.runs.append([])
        with self.assertRaises(NotFound) as caught:
            self.fetch_all(built.cid)
        self.assertIn("no provider found", str(caught.exception))

    def test_lookup_runs_again_when_every_provider_failed(self):
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        self.hold(P2)
        self.runs.extend([[P1], [P2]])  # P1 has nothing: it fails, then a second run finds P2
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual(self.lookups[0].count, 2)

    def test_seniority_orders_providers(self):
        self.routing.heard(P2)
        self.config.provider_seniority = 0  # everything in the routing table counts as long-known
        with self.fetcher.open(self.file(b"y").cid) as fetch:
            for node_id in (P1, P2, P3):
                fetch._add_provider(node_id)
            self.assertEqual([p.node_id for p in fetch._usable()], [P2, P1, P3])


class FailureRuleTest(FetcherTest):
    """When providers fail, are banned, and are retried."""

    def test_none_for_a_block_fails_the_provider(self):
        self.config.max_parallel_providers = 1
        built = self.file(os.urandom(4 * CHUNK_SIZE))
        root_block_only = [built.cid]
        self.hold(P1, root_block_only)  # advertises the root but lacks the chunks
        self.hold(P2)
        self.runs.append([P1, P2])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        requests_to_p1 = [cids for node, cids in self.client.requests if node == P1]
        self.assertEqual(len(requests_to_p1), 2)  # the root, then one batch answered None: not asked again

    def test_busy_retried_then_fails_after_max_failures(self):
        built = self.file(b"z" * 3000)
        self.client.behaviour[P1] = lambda cids: BlocksResult(BUSY, unsent=list(cids))
        self.runs.append([P1])
        with self.assertRaises(NotFound):
            self.fetch_all(built.cid)
        self.assertEqual(len([n for n, _ in self.client.requests if n == P1]), self.config.max_failures)

    def test_busy_then_ok_keeps_the_provider(self):
        built = self.file(b"w" * 3000)
        self.hold(P1)
        answers = [BlocksResult(BUSY, unsent=[built.cid])]
        self.client.behaviour[P1] = lambda cids: answers.pop() if answers else None
        self.runs.append([P1])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))

    def test_cancelled_response_is_retried_like_busy(self):
        built = self.file(b"k" * 3000)
        self.hold(P1)
        answers = [BlocksResult(BUSY, unsent=[built.cid], reason="cancelled")]
        self.client.behaviour[P1] = lambda cids: answers.pop() if answers else None
        self.runs.append([P1])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual(len(self.client.requests), 2)

    def test_stalls_twice_fails(self):
        self.config.max_parallel_providers = 1
        built = self.file(b"s" * 3000)
        self.hold(P2)
        self.client.behaviour[P1] = lambda cids: BlocksResult(STALLED, unsent=list(cids), reason="stalled")
        self.runs.append([P1, P2])
        self.fetch_all(built.cid)
        self.assertEqual(len([n for n, _ in self.client.requests if n == P1]), 2)

    def test_link_failure_fails_at_once(self):
        self.config.max_parallel_providers = 1
        built = self.file(b"f" * 3000)
        self.hold(P2)
        self.client.behaviour[P1] = lambda cids: BlocksResult(FAILED, unsent=list(cids), reason="unreachable")
        self.runs.append([P1, P2])
        self.fetch_all(built.cid)
        self.assertEqual(len([n for n, _ in self.client.requests if n == P1]), 1)

    def test_bad_block_bans_the_provider(self):
        self.config.max_parallel_providers = 1
        built = self.file(b"b" * 3000)
        self.hold(P2)
        self.client.behaviour[P1] = lambda cids: BlocksResult(OK, bad=[cids[0]], unsent=list(cids[1:]))
        self.runs.append([P1, P2])
        self.fetch_all(built.cid)
        self.assertTrue(self.routing.is_banned(P1))
        self.assertTrue(self.storage.is_complete(built.cid))
        # A later fetch doesn't use the banned node, even if a lookup reports it
        other = self.file(b"c" * 3000)
        self.hold(P2)
        self.runs[:] = [[P1, P2]]
        self.client.requests.clear()
        self.fetch_all(other.cid)
        self.assertEqual(self.client.asked(P1), [])


class PacingTest(FetcherTest):
    """Mirroring the responder's upload pacing (section 3), and refusals soon after serving not failing a provider."""

    def setUp(self):
        """A fetch of a three-chunk file with P1 as its provider, its results handled by hand."""
        super().setUp()
        self.now = 1000.0
        self.fetcher.clock = lambda: self.now
        self.fetcher.busy_delay = 2.0
        self.built = self.file(os.urandom(3 * CHUNK_SIZE))
        self.fetch = self.fetcher.open(self.built.cid).__enter__()
        self.addCleanup(self.fetch.__exit__, None, None, None)
        self.provider = self.fetch._new_provider(P1)

    def result(self, status: str, reason: str = "", sent_at: float | None = None, rate: float | None = None):
        """Handle P1's answer, now, to a request sent at sent_at with that rate estimate (blocks served if OK)."""
        cid = self.built.cid
        result = BlocksResult(status, reason=reason)
        if status == OK:
            result.blocks[cid] = self.network[cid]
        self.fetch._handle(P1, [cid], result, False, {}, {}, set(), sent_at, rate)

    def test_slow_link_waits_before_asking_again(self):
        self.result(OK, sent_at=self.now - 40, rate=50_000)  # 40 s for that request
        # At the default cap, half the rate estimate, the next response may start 2 × 40 s after the last one
        self.assertEqual(self.provider.busy_until, self.now + 40)
        self.assertGreaterEqual(self.fetch._progress_at, self.now + 40)  # waiting isn't counted as hanging

    def test_fast_link_isnt_paced(self):
        self.result(OK, sent_at=self.now - 40, rate=200_000)
        self.assertEqual(self.provider.busy_until, 0)
        self.result(OK, sent_at=self.now - 40, rate=None)
        self.assertEqual(self.provider.busy_until, 0)

    def test_refusals_soon_after_serving_are_not_failures(self):
        self.result(OK, sent_at=self.now - 40, rate=50_000)
        self.now += 40
        for _ in range(2 * self.config.max_failures):
            self.result(BUSY, "busy")
            self.assertEqual(self.provider.busy_until, self.now + 5)  # an eighth of the 40 s it took
            self.now = self.provider.busy_until
        self.assertFalse(self.provider.failed)
        self.assertEqual(self.provider.busy, 0)
        # Past four times that since it was sent, refusals count as busy as usual
        self.now = 1000 - 40 + 4 * 40 + 1
        for _ in range(self.config.max_failures):
            self.result(BUSY, "busy")
        self.assertTrue(self.provider.failed)

    def test_cancelled_responses_count_even_after_serving(self):
        self.result(OK, sent_at=self.now - 40, rate=50_000)
        for _ in range(self.config.max_failures):
            self.result(BUSY, "cancelled")
        self.assertTrue(self.provider.failed)

    def test_paced_provider_isnt_dropped_meanwhile(self):
        # Only provider, waiting out the pacing: the fetch neither gives up nor reruns the lookup
        self.result(OK, sent_at=self.now - 40, rate=50_000)
        self.assertTrue(self.fetch._waiting_for_busy())

    def sent(self, elapsed: float | None):
        """Send P1 a request that spends 30 s opening the link, then 40 s from sending it to its answer (reported
        as elapsed), and handle the result it queues."""
        cid = self.built.cid
        self.client.rate = 50_000

        def answer(cids):
            """Open the link, then send and get the answer."""
            self.now += 30 + 40
            return BlocksResult(OK, blocks={cid: self.network[cid]}, elapsed=elapsed)
        self.client.behaviour[P1] = answer
        self.fetch._send(P1, [cid])
        _, node_id, cids, result, sent_at, rate = self.fetch._events.get(timeout=5)
        self.fetch._handle(node_id, cids, result, False, {}, {}, set(), sent_at, rate)

    def test_pacing_counts_from_when_the_request_was_sent(self):
        self.sent(40)
        # As long again as the request took once sent: not the link opening before it
        self.assertEqual(self.provider.busy_until, self.now + 40)
        self.assertEqual(self.provider.pacing_until, self.now - 40 + 4 * 40)
        self.assertEqual(self.provider.pacing_retry, 5)

    def test_pacing_without_a_send_time_counts_the_whole_call(self):
        self.sent(None)
        self.assertEqual(self.provider.busy_until, self.now + 70)
        self.assertEqual(self.provider.pacing_until, self.now - 70 + 4 * 70)


def old_take_batch(fetch, pending, node_id, wanted, mdu):
    """Fetch._take_batch as it was, packing each prefix in turn to see if it fits, with the cap on the bytes
    reserved that it has now: the reference the single count over pending must match."""
    budget = fetcher_module.rates.requester_max_bytes(fetch.client.rate_estimate(node_id), fetch.config)
    carried = min(budget, MAX_RESPONSE_SIZE - RESPONSE_OVERHEAD)
    room = fetch._room(wanted)
    batch, total = [], 0
    while pending and len(batch) < fetcher_module.MAX_BATCH:
        cid = pending[0]
        known = wanted[cid][0].size if cid.is_raw else None
        guess = known if known is not None else (CHUNK_SIZE if cid.is_raw else fetcher_module.DAG_NODE_GUESS)
        if batch and total + guess > carried:
            break
        if batch and protocol.blocks_batch_size(budget, batch + [cid], mdu) <= len(batch):
            break
        if room is not None:
            room = fetch._reserve(cid, wanted, room)
            if not fetch._fits(room) and (batch or fetch._in_flight):
                break
        batch.append(pending.popleft())
        total += guess
    return batch


class BatchTest(FetcherTest):
    """What one request asks for: no more than a response can carry, or the provider's link can send."""

    def setUp(self):
        """An open fetch, and the expectations of 40 raw chunks of 64 KiB."""
        super().setUp()
        self.fetch = self.fetcher.open(self.file(b"root").cid).__enter__()
        self.addCleanup(self.fetch.__exit__, None, None, None)
        self.chunks = [walker.Expect(Cid.for_block(RAW, bytes([i]) * 8), CHUNK_SIZE, walker.FILE_TYPES, 2)
                       for i in range(40)]
        self.wanted = {expect.cid: [expect] for expect in self.chunks}

    def take(self) -> list[Cid]:
        """The batch for P1 from every chunk."""
        return self.fetch._take_batch(deque(self.wanted), P1, self.wanted)

    def test_capped_at_what_a_response_carries(self):
        # At 1 Gbit/s max_bytes asks for ~7.5 GB, but a response carries at most MAX_RESPONSE_SIZE less overhead
        self.client.rate = 1e9
        batch = self.take()
        max_bytes = fetcher_module.rates.requester_max_bytes(self.client.rate, self.config)
        served = server.select_blocks(batch, lambda cid: bytes(CHUNK_SIZE),
                                      server.blocks_budget(max_bytes, self.client.rate, None, 60))
        self.assertEqual(len(batch), (MAX_RESPONSE_SIZE - RESPONSE_OVERHEAD) // CHUNK_SIZE)
        self.assertEqual(len(served), len(batch))  # every CID reserved comes back

    def test_sized_to_the_open_links_packet(self):
        self.client.rate = 1e9
        self.wanted = {expect.cid: [walker.Expect(expect.cid, 10, walker.FILE_TYPES, 2)] for expect in self.chunks}
        max_bytes = fetcher_module.rates.requester_max_bytes(self.client.rate, self.config)
        # No link known: the largest request any link allows
        self.assertEqual(len(self.take()), protocol.blocks_batch_size(max_bytes, list(self.wanted), MAX_REQUEST_SIZE))
        self.client.link_mdu = lambda node_id: None
        self.assertEqual(len(self.take()), protocol.blocks_batch_size(max_bytes, list(self.wanted), MAX_REQUEST_SIZE))
        self.client.link_mdu = lambda node_id: 431
        self.assertEqual(len(self.take()), protocol.blocks_batch_size(max_bytes, list(self.wanted), 431))
        self.assertLess(len(self.take()), protocol.blocks_batch_size(max_bytes, list(self.wanted), MAX_REQUEST_SIZE))

    def test_one_count_matches_packing_each_prefix(self):
        rng = random.Random(7)
        for trial in range(300):
            expects = []
            for i in range(rng.randrange(0, 45)):
                if rng.random() < 0.7:
                    size = rng.choice([None, 0, 10, 1000, CHUNK_SIZE, rng.randrange(1, CHUNK_SIZE)])
                    expects.append(walker.Expect(Cid.for_block(RAW, rng.randbytes(8)), size, walker.FILE_TYPES, 2))
                else:
                    expects.append(walker.Expect(Cid.for_block(DAG, rng.randbytes(8)), None, walker.ANY_TYPES, 2))
            wanted = {expect.cid: [expect] for expect in expects}
            self.client.rate = rng.choice([None, 1200, 50_000, 1e6, 1e9, rng.uniform(1, 1e10)])
            mdu = rng.choice([None, 0, 120, 300, 431, 1000, 1500, 9000])
            self.client.link_mdu = lambda node_id: mdu
            self.fetch.max_dag_bytes = rng.choice([None, None, 0, CHUNK_SIZE, 5 * MAX_BLOCK_SIZE])
            with self.subTest(trial=trial):
                old, new = deque(wanted), deque(wanted)
                expected = old_take_batch(self.fetch, old, P1, wanted, mdu or MAX_REQUEST_SIZE)
                self.assertEqual(self.fetch._take_batch(new, P1, wanted), expected)
                self.assertEqual(list(new), list(old))


class ValidityTest(FetcherTest):
    """Reader checks, max_download_size, space."""

    def test_invalid_dag_marks_the_root_invalid(self):
        chunk = b"abc"
        chunk_cid = Cid.for_block(RAW, chunk)
        self.network[chunk_cid] = chunk
        # The node says its child holds 5 bytes; the chunk has 3: correctly hashed, but an invalid DAG
        lying = dag.encode(FileNode([Child(chunk_cid, 5), Child(chunk_cid, 3)]))
        root = Cid.for_block(DAG, lying)
        self.network[root] = lying
        self.hold(P1)
        self.runs.append([P1])
        with self.assertRaises(InvalidDag):
            self.fetch_all(root)
        self.assertTrue(self.fetcher.is_invalid(root))
        self.assertFalse(self.routing.is_banned(P1))  # the author's fault, not the provider's
        with self.assertRaises(InvalidDag):
            self.fetcher.open(root)
        # Remembered across restarts, once saved (the node saves periodically and at shutdown)
        self.assertFalse(self.make_fetcher().is_invalid(root))
        self.fetcher.save()
        self.assertTrue(self.make_fetcher().is_invalid(root))

    def test_blocks_damaged_on_disk_are_fetched_again(self):
        """A stored block no longer matching its CID (e.g. left empty by a power loss) is local damage: it is
        deleted and fetched again, and the root isn't marked invalid."""
        data = os.urandom(3 * CHUNK_SIZE)
        built = builder.build_bytes(data, self.storage.put)
        self.hold(P1, list(self.storage_cids()))
        chunk = next(cid for cid in self.network if cid.is_raw)
        for cid, damage in ((built.cid, b""), (chunk, b"junk")):
            with open(self.storage.blocks.path(cid), "wb") as f:
                f.write(damage)
        # As after a restart, whose scan records the damaged files' lengths
        self.storage = Storage(os.path.join(self.tmp.name, "blocks"), os.path.join(self.tmp.name, "state"), 10**9)
        self.addCleanup(self.storage.close)
        self.fetcher = self.make_fetcher()
        self.runs.append([P1])
        self.fetch_all(built.cid)
        self.assertFalse(self.fetcher.is_invalid(built.cid))
        self.assertEqual(self.read(built.cid), data)
        self.assertEqual(set(self.client.asked(P1)), {built.cid, chunk})

    def test_chunk_damaged_during_a_session_is_fetched_again_when_read(self):
        data = os.urandom(3 * CHUNK_SIZE)
        built = builder.build_bytes(data, self.storage.put)
        self.hold(P1, list(self.storage_cids()))
        chunk = next(cid for cid in self.network if cid.is_raw)
        with open(self.storage.blocks.path(chunk), "wb"):
            pass  # empty, though storage still records its length
        self.runs.append([P1])
        with self.fetcher.open(built.cid) as fetch:
            self.assertEqual(b"".join(fetch.read_range(fetch.resolve())), data)
        self.assertFalse(self.fetcher.is_invalid(built.cid))
        self.assertEqual(self.client.asked(P1), [chunk])

    def storage_cids(self):
        """Every stored block, also put on the network."""
        for cid, _ in self.storage.blocks.scan():
            self.network[cid] = self.storage.get(cid)
            yield cid

    def test_max_download_size_checked_before_fetching_below(self):
        self.config.max_download_size = 2 * CHUNK_SIZE
        built = self.file(os.urandom(4 * CHUNK_SIZE))
        self.hold(P1)
        self.runs.append([P1])
        with self.assertRaises(TooLarge):
            self.fetch_all(built.cid)
        self.assertEqual(self.client.asked(P1), [built.cid])  # only the root block

    def test_pins_filling_max_storage_dont_refuse_a_fetch(self):
        self.storage.max_storage = 10
        pinned = builder.build_bytes(b"p" * 100, self.storage.put)
        self.storage.pin(pinned.cid, "user")
        built = self.file(b"q" * 100)
        self.hold(P1)
        self.runs.append([P1])
        self.fetch_all(built.cid)  # max_storage is soft: storage goes over it while nothing is evictable
        self.assertTrue(self.storage.has(built.cid))
        self.assertTrue(self.storage.is_complete(pinned.cid))


class TransferTest(FetcherTest):
    """Resume, byte ranges, spreading, fetching each block once."""

    def test_interrupted_fetch_resumes(self):
        data = os.urandom(8 * CHUNK_SIZE)
        built = self.file(data)
        chunks = [c for c in self.network if c.is_raw]
        self.hold(P1, [built.cid] + chunks[:3])
        self.runs.append([P1])
        with self.assertRaises(NotFound):
            self.fetch_all(built.cid)
        self.assertEqual(sum(self.storage.has(c) for c in chunks), 3)
        self.hold(P2)
        self.runs[:] = [[P2]]
        self.client.requests.clear()
        self.fetch_all(built.cid)
        self.assertEqual(self.read(built.cid), data)
        self.assertEqual(set(self.client.asked(P2)), set(chunks[3:]))  # only what was missing

    def test_byte_range_fetches_only_covering_chunks(self):
        data = os.urandom(10 * CHUNK_SIZE)
        built = self.file(data)
        self.hold(P1)
        self.runs.append([P1])
        offset, length = 3 * CHUNK_SIZE + 5, CHUNK_SIZE
        with self.fetcher.open(built.cid) as fetch:
            target = fetch.resolve()
            got = b"".join(fetch.read_range(target, offset, length))
        self.assertEqual(got, data[offset:offset + length])
        node = dag.decode(self.network[built.cid])
        covering = {node.children[3].cid, node.children[4].cid}
        self.assertEqual(set(self.client.asked(P1)), {built.cid} | covering)

    def test_requests_spread_over_providers(self):
        built = self.file(os.urandom(40 * CHUNK_SIZE))
        barrier = threading.Barrier(2, timeout=5)
        seen: set[bytes] = set()

        def slow_answer(node_id):
            """Hold the first request until a second provider is asked too."""
            def answer(cids):
                seen.add(node_id)
                if len(seen) < 2:
                    try:
                        barrier.wait()
                    except threading.BrokenBarrierError:
                        pass
                elif not barrier.broken and barrier.n_waiting:
                    barrier.wait()
                return None
            return answer

        for node_id in (P1, P2, P3):
            self.hold(node_id)
            self.client.behaviour[node_id] = slow_answer(node_id)
        self.runs.append([P1, P2, P3])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertGreaterEqual(len({node for node, _ in self.client.requests}), 2)

    def test_slow_node_uses_one_provider(self):
        self.fetcher = self.make_fetcher(slow=True)
        self.config.max_parallel_providers = None
        built = self.file(os.urandom(20 * CHUNK_SIZE))
        for node_id in (P1, P2, P3):
            self.hold(node_id)
        self.runs.append([P1, P2, P3])
        self.fetch_all(built.cid)
        self.assertEqual({node for node, _ in self.client.requests}, {P1})

    def test_each_block_fetched_once(self):
        # Identical chunks and a directory listing the same file twice
        repeated = self.file(b"r" * (4 * CHUNK_SIZE))
        root = builder.build_directory([Entry("one", repeated.cid, repeated.size, 0),
                                        Entry("two", repeated.cid, repeated.size, 0)], self.put)
        self.hold(P1)
        self.runs.append([P1])
        self.fetch_all(root.cid)
        asked = self.client.asked(P1)
        self.assertEqual(len(asked), len(set(asked)))
        self.assertEqual(set(asked), set(self.network))

    def test_path_target(self):
        inner = self.file(b"inner file")
        other = self.file(os.urandom(3 * CHUNK_SIZE))
        sub = builder.build_directory([Entry("f", inner.cid, inner.size, dag.KIND_EXECUTABLE)], self.put)
        root = builder.build_directory([Entry("sub", sub.cid, sub.size, 2), Entry("big", other.cid, other.size, 0)],
                                       self.put)
        self.hold(P1)
        self.runs.append([P1])
        target = self.fetch_all(root.cid, ["sub", "f"])
        self.assertEqual(target.cid, inner.cid)
        self.assertEqual(target.kind, dag.KIND_EXECUTABLE)
        self.assertFalse(self.storage.has(other.cid))  # the rest of the root wasn't fetched
        with self.assertRaises(NotFound):
            self.fetch_all(root.cid, ["nope"])

    def test_blocks_already_stored_are_not_fetched(self):
        built = builder.build_bytes(os.urandom(3 * CHUNK_SIZE), self.storage.put)
        self.fetch_all(built.cid)
        self.assertEqual(self.client.requests, [])


class LimitTest(FetcherTest):
    """Bounds on what a hostile DAG or provider can make a fetch cost (security findings B1-B3, B5, B7, B9)."""

    def bomb(self, levels: int) -> tuple[Cid, int]:
        """A file of 256^levels one-byte chunks, made of levels + 1 blocks on the network."""
        leaf = b"x"
        cid, size = Cid.for_block(RAW, leaf), 1
        self.network[cid] = leaf
        for _ in range(levels):
            block = dag.encode(FileNode([Child(cid, size)] * 256))
            cid, size = Cid.for_block(DAG, block), size * 256
            self.network[cid] = block
        return cid, size

    def test_range_read_is_lazy_and_small_chunks_are_bounded(self):
        self.config.max_output_entries = 1000
        root, size = self.bomb(2)
        self.hold(P1)
        self.runs.append([P1])
        expanded = []
        real = walker.range_children

        def counting(*args):
            """range_children, counted."""
            expanded.append(args[0].cid)
            return real(*args)

        with mock.patch.object(walker, "range_children", counting), self.fetcher.open(root) as fetch:
            pieces = fetch.read_range(fetch.resolve())
            self.assertEqual(next(pieces), b"x")
            self.assertLessEqual(len(expanded), 3)  # one node per level, not all 257 before the first byte
            with self.assertRaises(TooLarge):
                for _ in pieces:
                    pass

    def test_honest_large_range_isnt_limited_by_max_output_entries(self):
        self.config.max_output_entries = 0
        data = os.urandom(40 * CHUNK_SIZE + 7)
        built = self.file(data)
        self.hold(P1)
        self.runs.append([P1])
        with self.fetcher.open(built.cid) as fetch:
            self.assertEqual(b"".join(fetch.read_range(fetch.resolve(), 5)), data[5:])

    def test_failed_fetch_closes_links_with_requests_in_flight(self):
        self.config.max_parallel_providers = 2
        bad_block = b"\x93\x01\x02\x03"  # correctly hashed, not a valid DAG node
        bad = Cid.for_block(DAG, bad_block)
        self.network[bad] = bad_block
        self.hold(P1)
        self.hold(P2)
        release = threading.Event()
        self.addCleanup(release.set)
        self.client.behaviour[P2] = lambda cids: (release.wait(5), None)[1]  # still sending when the fetch fails
        self.client.links = [P1, P2]  # both asked for the root at once
        with self.assertRaises(InvalidDag):
            self.fetch_all(bad)
        self.assertEqual(self.client.closed, [P2])
        # A fetch that succeeds leaves links alone
        self.client.closed.clear()
        release.set()
        fine = self.file(b"fine")
        self.hold(P1)
        self.hold(P2)
        self.fetch_all(fine.cid)
        self.assertEqual(self.client.closed, [])

    def test_hung_request_ends_the_fetch(self):
        self.config.lookup_timeout, self.config.stall_timeout = 0.3, 0.2
        built = self.file(b"never arrives")
        self.hold(P1)
        self.client.behaviour[P1] = lambda cids: (time.sleep(5), BlocksResult(FAILED, unsent=list(cids)))[1]
        self.runs.append([P1])
        start = time.monotonic()
        with self.assertRaises(NotFound):
            self.fetch_all(built.cid)
        self.assertLess(time.monotonic() - start, 3)

    def test_response_progress_keeps_a_slow_fetch_alive(self):
        self.config.lookup_timeout, self.config.stall_timeout = 0.2, 0.2
        built = self.file(b"slowly")
        self.hold(P1)

        def slow(cids):
            """Report progress for a while, then answer normally."""
            for _ in range(10):
                self.client.on_progress(0.5)
                time.sleep(0.1)
            return None

        self.client.behaviour[P1] = slow
        self.runs.append([P1])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.has(built.cid))

    def test_unpinned_fetch_evicts_cache_rather_than_overshooting(self):
        cached = builder.build_bytes(os.urandom(3 * CHUNK_SIZE), self.storage.put)
        self.storage.record_fetch(cached.cid, [], cached.cid)
        self.storage.max_storage = 5 * CHUNK_SIZE
        built = self.file(os.urandom(4 * CHUNK_SIZE))
        self.hold(P1)
        self.runs.append([P1])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertFalse(self.storage.has(cached.cid))
        self.assertLessEqual(self.storage.used_bytes(), self.storage.max_storage)

    def disk(self, free: int):
        """A mock disk_usage reporting free bytes on a 1 GiB filesystem (reserve: DISK_RESERVE_MIN)."""
        usage = shutil.disk_usage(self.tmp.name)._replace(total=1024 ** 3, free=free)
        return mock.patch("shutil.disk_usage", return_value=usage)

    def test_fetch_bigger_than_free_disk_is_refused(self):
        built = self.file(os.urandom(8 * CHUNK_SIZE))
        self.hold(P1)
        self.runs.append([P1])
        reserve = storage_module.DISK_RESERVE_MIN
        for pinned in (False, True):
            with self.subTest(pinned=pinned), self.disk(reserve + 4 * CHUNK_SIZE):
                with self.assertRaises(StorageFull):  # its verified size shows it can't fit
                    self.fetch_all(built.cid, pinned=pinned)
                with self.assertRaises(StorageFull), self.fetcher.open(built.cid) as fetch:
                    b"".join(fetch.read_range(fetch.resolve(), 0, 6 * CHUNK_SIZE))
                with self.fetcher.open(built.cid) as fetch:  # a range that fits is fine
                    b"".join(fetch.read_range(fetch.resolve(), 0, 2 * CHUNK_SIZE))
        self.assertEqual(len(self.client.asked(P1)), 3)  # the root and two chunks, nothing more
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))

    def test_large_dag_nodes_stop_at_the_disk_reserve(self):
        root = self.empty_files(600)
        self.hold(P1)
        self.runs.append([P1])
        with self.disk(storage_module.DISK_RESERVE_MIN + 30_000), self.assertRaises(StorageFull):
            self.fetch_all(root.cid)  # its size is 0, but its directory nodes are not

    def empty_files(self, count: int, length: int = 200) -> builder.Built:
        """A directory of count empty files with long names: size 0, but kilobytes of directory nodes."""
        empty = self.file(b"")
        return builder.build_directory([Entry(f"{i:05d}" + "n" * length, empty.cid, 0, 0) for i in range(count)],
                                       self.put)

    def test_dag_nodes_count_against_max_download_size(self):
        root = self.empty_files(600)
        self.assertEqual(root.size, 0)
        self.config.max_download_size = 50_000
        self.hold(P1)
        self.runs.append([P1])
        with self.assertRaises(TooLarge):
            self.fetch_all(root.cid)
        self.assertFalse(self.fetcher.is_invalid(root.cid))
        self.config.max_download_size = None
        self.fetch_all(root.cid)

    def test_listing_and_write_out_fetches_stop_at_max_output_entries(self):
        with mock.patch("rnfs.constants.MAX_CHILDREN", 4):
            root = self.empty_files(200, 10)
        self.assertIsInstance(dag.decode(self.network[root.cid]), dag.IndexNode)
        self.config.max_output_entries = 20
        self.hold(P1)
        self.runs.append([P1])
        with self.assertRaises(TooLarge), self.fetcher.open(root.cid) as fetch:
            fetch.fetch_listing(fetch.resolve())
        asked = len(self.client.asked(P1))
        self.assertLess(asked, len(self.network) // 2)
        with self.assertRaises(TooLarge), self.fetcher.open(root.cid) as fetch:
            fetch.fetch_dag(fetch.resolve(), max_entries=20)
        with self.fetcher.open(root.cid) as fetch:
            fetch.fetch_listing(fetch.resolve(), max_entries=200)
            fetch.fetch_dag(fetch.resolve(), max_entries=200)
        self.assertTrue(self.storage.is_complete(root.cid))

    def test_max_dag_bytes(self):
        root = self.empty_files(600)
        self.hold(P1)
        self.runs.append([P1])
        with self.assertRaises(TooLarge), self.fetcher.open(root.cid, pinned=True) as fetch:
            fetch.max_dag_bytes = 50_000
            fetch.fetch_dag(fetch.resolve())
        self.assertLess(self.storage.used_bytes(), 50_000 + 2 * MAX_BATCH_BYTES)

    def test_nested_fetch_marks_only_its_own_root_invalid(self):
        own = builder.build_bytes(b"mine", self.storage.put)
        bad_block = b"\x93\x01\x02\x03"  # correctly hashed, not a valid DAG node
        bad = Cid.for_block(DAG, bad_block)
        self.network[bad] = bad_block
        self.hold(P1)
        self.runs.append([P1])
        with self.assertRaises(InvalidDag):
            with self.fetcher.open(own.cid, pinned=True) as fetch, self.fetcher.open(bad, pinned=True) as inner:
                fetch.fetch_dag(fetch.resolve())
                inner.fetch_dag(inner.resolve())
        self.assertTrue(self.fetcher.is_invalid(bad))
        self.assertFalse(self.fetcher.is_invalid(own.cid))
        # Nor does opening an invalid root inside another fetch mark the outer one
        with self.assertRaises(InvalidDag):
            with self.fetcher.open(own.cid) as fetch, self.fetcher.open(bad):
                fetch.resolve()
        self.assertFalse(self.fetcher.is_invalid(own.cid))

    def test_invalid_roots_are_capped_and_saved_in_batches(self):
        state = os.path.join(self.tmp.name, "state")
        with mock.patch.object(fetcher_module, "MAX_INVALID", 3), \
                mock.patch.object(self.fetcher._db, "transaction", wraps=self.fetcher._db.transaction) as writes:
            roots = [Cid.for_block(RAW, b"%d" % i) for i in range(5)]
            for root in roots:
                self.fetcher.mark_invalid(root)
            self.assertEqual(writes.call_count, 0)
            self.assertEqual(state_rows(state, "SELECT cid FROM invalid"), [])
            self.fetcher.save()
            self.fetcher.save()
            self.assertEqual(writes.call_count, 1)
            self.assertEqual([self.fetcher.is_invalid(r) for r in roots], [False, False, True, True, True])
            self.assertEqual([self.make_fetcher().is_invalid(r) for r in roots], [False, False, True, True, True])
            # The oldest goes when another is marked; only the change is written
            self.fetcher.mark_invalid(roots[0])
            self.fetcher.save()
            self.assertEqual([self.make_fetcher().is_invalid(r) for r in roots], [True, False, False, True, True])
            self.assertEqual(state_rows(state, "SELECT cid FROM invalid ORDER BY position"),
                             [(roots[3].binary,), (roots[4].binary,), (roots[0].binary,)])

    def test_failed_invalid_save_is_tried_again(self):
        root = Cid.for_block(RAW, b"bad")
        self.fetcher.mark_invalid(root)
        with failing_commits():
            with self.assertRaises(OSError):
                self.fetcher.save()
        self.fetcher.save()
        self.assertTrue(self.make_fetcher().is_invalid(root))

    def test_invalid_rows_are_skipped_and_deleted(self):
        root = Cid.for_block(RAW, b"bad")
        self.fetcher.mark_invalid(root)
        self.fetcher.save()
        state = os.path.join(self.tmp.name, "state")
        state_execute(state, "INSERT INTO invalid (cid, position) VALUES (?, 7)", (str(root),))
        fetcher = self.make_fetcher()
        self.assertTrue(fetcher.is_invalid(root))
        fetcher.save()
        self.assertEqual(state_rows(state, "SELECT cid FROM invalid"), [(root.binary,)])


MAX_BATCH_BYTES = BATCH * 80 * 1024  # what one fake response can hold at most


if __name__ == "__main__":
    unittest.main()
