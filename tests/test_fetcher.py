"""Tests for fetching targets (rnfs/fetcher.py; specification sections 6 and 7) over a fake client and lookup."""
import os
import tempfile
import threading
import unittest

import RNS

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE
from rnfs.content import builder, dag, walker
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.dag import Child, Entry, FileNode
from rnfs.content.storage import Storage
from rnfs.dht.routing import RoutingTable
from rnfs.errors import InvalidDag, NotFound, StorageFull, TooLarge
from rnfs.fetcher import Fetcher
from rnfs.network.client import BUSY, FAILED, OK, STALLED, BlocksResult

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
        self.lock = threading.Lock()

    def open_links(self) -> list[bytes]:
        """Nodes with an open link."""
        return list(self.links)

    def rate_estimate(self, node_id: bytes):
        """The same estimate for every link."""
        return self.rate

    def blocks(self, node_id: bytes, cids: list[Cid]) -> BlocksResult:
        """Serve what node_id holds, BATCH CIDs per request."""
        with self.lock:
            self.requests.append((node_id, list(cids)))
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
        return Fetcher(self.storage, self.client, self.config, OWN, self.routing, make_lookup, slow,
                       os.path.join(self.tmp.name, "state"), busy_delay=0.01)

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
        # Remembered across restarts
        self.assertTrue(self.make_fetcher().is_invalid(root))

    def test_max_download_size_checked_before_fetching_below(self):
        self.config.max_download_size = 2 * CHUNK_SIZE
        built = self.file(os.urandom(4 * CHUNK_SIZE))
        self.hold(P1)
        self.runs.append([P1])
        with self.assertRaises(TooLarge):
            self.fetch_all(built.cid)
        self.assertEqual(self.client.asked(P1), [built.cid])  # only the root block

    def test_storage_full_refuses_unpinned_fetch(self):
        self.storage.max_storage = 10
        pinned = builder.build_bytes(b"p" * 100, self.storage.put)
        self.storage.pin(pinned.cid, "user")
        built = self.file(b"q" * 100)
        self.hold(P1)
        self.runs.append([P1])
        with self.assertRaises(StorageFull):
            self.fetch_all(built.cid)
        self.fetch_all(built.cid, pinned=True)  # a pinned fetch is allowed
        self.assertTrue(self.storage.has(built.cid))


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


if __name__ == "__main__":
    unittest.main()
