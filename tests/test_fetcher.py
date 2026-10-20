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

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE, K, MAX_BLOCK_SIZE, MAX_REQUEST_SIZE, MAX_RESPONSE_SIZE, RESPONSE_OVERHEAD
from rnfs.content import builder, dag, walker
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.dag import Child, Entry, FileNode
from rnfs.content import storage as storage_module
from rnfs.content.storage import Storage
from rnfs.dht import scores as scoring
from rnfs.dht.routing import RoutingTable
from rnfs.errors import InvalidDag, NotFound, StorageFull, TooLarge
from rnfs import fetcher as fetcher_module, mesh
from rnfs.fetcher import Fetcher
from rnfs.network import client as client_module, links, protocol, server
from rnfs.network.client import BUSY, FAILED, LOST, OK, STALLED, BlocksResult
from tests.fakes.client import BATCH, FakeClient
from tests.fakes.log import quiet_log
from tests.support import FakeClock, failing_commits, state_execute, state_rows
import tests.test_links as link_fakes  # a module, so its test cases don't run twice

OWN = bytes(16)
P1, P2, P3, P4 = (bytes([i]) * 16 for i in (1, 2, 3, 4))
setUpModule, tearDownModule = quiet_log()


class FakeLookup:
    """A provider lookup whose runs report fixed providers."""

    def __init__(self, runs: list[list[bytes]], listed: dict[bytes, set[bytes]] | None = None):
        """runs[i] is what the i-th run of find_providers reports; listed[provider], the nodes whose answers listed
        it (none by default)."""
        self.runs = runs
        self.count = 0
        self.listed = listed or {}

    def find_providers(self, on_provider, should_stop=lambda: False):
        """Report this run's providers."""
        run = self.runs[self.count] if self.count < len(self.runs) else []
        self.count += 1
        for provider in run:
            on_provider(provider)
        return list(run)

    def stop(self):
        """Nothing to wake."""

    def listers(self, provider: bytes) -> set[bytes]:
        """The nodes said to have listed provider."""
        return set(self.listed.get(provider, ()))

    def allow_unknown(self, node_id: bytes) -> bool:
        """Every identity is known."""
        return True

    def remaining(self) -> float:
        """Time is never up."""
        return 100.0

    def closest_answered(self) -> list[bytes]:
        """Some node answered (the search reached the DHT), unless a test said none did."""
        return [] if self.unanswered else [P4]

    unanswered = False  # set by a test: no node answered (the node is offline)


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
        self.listed: dict[bytes, set[bytes]] = {}  # provider -> the nodes the lookups say listed it
        self.fetcher = self.make_fetcher()

    def make_fetcher(self, slow: bool = False) -> Fetcher:
        """A fetcher whose lookups report self.runs."""
        def make_lookup(key):
            """A new FakeLookup, remembered."""
            lookup = FakeLookup(self.runs, self.listed)
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

    def test_a_probe_that_brings_the_block_isnt_asked_for_it_again(self):
        built = self.file(os.urandom(3 * CHUNK_SIZE))
        self.hold(P1)
        self.client.links = [P1]
        self.fetch_all(built.cid)
        asked = self.client.asked(P1)
        self.assertEqual(asked[0], built.cid)
        self.assertEqual(len(asked), len(set(asked)))  # the root once: the probe brought it
        self.assertEqual(sorted(map(str, asked)), sorted(map(str, self.network)))

    def test_a_probe_that_misses_leaves_the_block_to_the_lookup(self):
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        self.hold(P3)
        self.client.links = [P1, P2]  # neither holds anything
        self.runs.append([P3])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual(self.client.asked(P1), [built.cid])
        self.assertEqual(self.client.asked(P2), [built.cid])
        self.assertEqual(self.client.asked(P3)[0], built.cid)

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

    def test_scores_order_providers_within_seniority_groups(self):
        self.routing.heard(P2)
        self.config.provider_seniority = 0  # everything in the routing table counts as long-known
        for _ in range(8):
            self.fetcher.scores.served(P3)  # well scored, but new: still after P2
        self.fetcher.scores.served(P1)
        self.fetcher.scores.served(P1)
        four = bytes([4] * 16)
        with self.fetcher.open(self.file(b"y").cid) as fetch:
            for node_id in (four, P1, P2, P3):
                fetch._add_provider(node_id)
            self.assertEqual([p.node_id for p in fetch._usable()], [P2, P3, P1, four])


class RememberTest(FetcherTest):
    """Providers that served a root are asked first when it is fetched again; a search that found none isn't repeated
    by a fetch that isn't explicit (section 7, Fetching a target, step 1)."""

    LORA = mesh.PathInfo(1, "LoRa", 1200)

    def setUp(self):
        """A file of a few chunks; the fetcher's clock set by hand."""
        super().setUp()
        self.built = self.file(os.urandom(3 * CHUNK_SIZE))
        self.now = [1000.0]
        self.fetcher.clock = lambda: self.now[0]

    def resolve(self, root: Cid, **kwargs):
        """Fetch the root block only."""
        with self.fetcher.open(root, **kwargs) as fetch:
            fetch.resolve()

    def test_a_provider_that_served_is_asked_first_without_a_lookup(self):
        self.hold(P1)
        self.runs.append([P1])
        self.resolve(self.built.cid)  # the root, from the provider a lookup found
        self.assertEqual(self.fetcher.remembered(self.built.cid), [P1])
        self.client.requests.clear()
        self.fetch_all(self.built.cid)  # its links closed since: no open link to ask
        self.assertTrue(self.storage.is_complete(self.built.cid))
        self.assertEqual(len(self.lookups), 1)  # no second lookup
        self.assertEqual(self.client.requests[0][0], P1)

    def test_only_providers_that_served_are_remembered(self):
        self.hold(P2)
        self.runs.append([P1, P3, P2])  # a /find reply's word isn't enough: P1 and P3 hold nothing
        self.fetch_all(self.built.cid)
        self.assertEqual(self.fetcher.remembered(self.built.cid), [P2])

    def test_a_remembered_provider_that_answers_none_is_forgotten(self):
        self.hold(P1)
        self.runs.append([P1])
        self.resolve(self.built.cid)
        self.client.holdings[P1] = {}  # it unpinned it
        self.hold(P2)
        self.runs[:] = [[P2]]
        self.fetch_all(self.built.cid)
        self.assertTrue(self.storage.is_complete(self.built.cid))
        self.assertEqual(len(self.lookups), 2)
        self.assertEqual(self.fetcher.remembered(self.built.cid), [P2])
        self.assertFalse(self.routing.is_banned(P1))  # not penalised: it was only asked first

    def test_a_remembered_provider_that_fails_is_forgotten_and_a_busy_one_kept(self):
        for status, kept in ((FAILED, False), (LOST, False), (BUSY, True)):
            with self.subTest(status=status):
                built = self.file(os.urandom(2 * CHUNK_SIZE) + status.encode())
                self.client.behaviour.clear()
                self.hold(P1)
                self.runs[:] = [[P1]]
                self.resolve(built.cid)
                self.client.behaviour[P1] = lambda cids, status=status: BlocksResult(status, unsent=list(cids))
                self.hold(P2)
                self.runs[:] = [[P2]]
                self.fetch_all(built.cid)
                self.assertEqual(P1 in self.fetcher.remembered(built.cid), kept)

    def test_a_provider_that_fails_during_a_fetch_is_forgotten(self):
        self.hold(P1)
        self.runs.append([P1])
        self.resolve(self.built.cid)
        root = self.network[self.built.cid]
        self.client.holdings[P1] = {self.built.cid: root}  # the root, but none of its chunks
        self.hold(P2)
        self.client.links = [P2]
        self.fetch_all(self.built.cid)
        self.assertNotIn(P1, self.fetcher.remembered(self.built.cid))

    def test_remembered_for_a_few_hours_and_never_once_banned(self):
        self.fetcher.remember(self.built.cid, P1)
        self.now[0] += fetcher_module.REMEMBER_FOR - 1
        self.fetcher.remember(self.built.cid, P2)
        self.assertEqual(self.fetcher.remembered(self.built.cid), [P2, P1])  # the most recent to serve first
        self.now[0] += 1
        self.assertEqual(self.fetcher.remembered(self.built.cid), [P2])
        self.routing.ban(P2)
        self.assertEqual(self.fetcher.remembered(self.built.cid), [])

    def test_bounded(self):
        for i in range(K + 5):
            self.now[0] += 1
            self.fetcher.remember(self.built.cid, bytes([i + 10]) * 16)
        remembered = self.fetcher.remembered(self.built.cid)
        self.assertEqual(remembered, [bytes([i + 10]) * 16 for i in reversed(range(5, K + 5))])  # the latest K
        roots = [Cid.for_block(RAW, bytes([i])) for i in range(4)]
        with mock.patch.object(fetcher_module, "MAX_REMEMBERED", 3), \
                mock.patch.object(fetcher_module, "MAX_UNFOUND", 3):
            for root in roots:
                self.fetcher.remember(root, P1)
                self.fetcher.note_unfound(root)
            self.fetcher.remembered(roots[1])  # used: kept over roots[2]
            self.fetcher.remember(self.built.cid, P1)
            self.assertEqual([root for root in roots if self.fetcher.remembered(root)], [roots[1], roots[3]])
            self.assertEqual([root for root in roots if self.fetcher.unfound(root) is not None], roots[1:])

    def test_the_hint_then_remembered_providers_then_open_links(self):
        for node_id in (P1, P2, P3):
            self.hold(node_id)
        self.fetcher.remember(self.built.cid, P2)
        self.client.links = [P3, P2]
        self.config.max_parallel_providers = 2
        with self.fetcher.open(self.built.cid, hint=P4) as fetch:
            fetch.resolve()
        self.assertEqual(sorted(node for node, _ in self.client.requests[:2]), [P2, P4])  # P3 left out
        self.assertEqual(self.client.asked(P3), [])

    def test_a_fast_node_asks_no_remembered_provider_over_a_slow_link_first(self):
        self.hold(P1)
        self.hold(P2)
        self.fetcher.remember(self.built.cid, P1)
        self.fetcher.path_info = lambda node_id: self.LORA if node_id == P1 else mesh.NO_PATH
        self.runs.append([P2])
        self.fetch_all(self.built.cid)
        self.assertEqual(self.client.asked(P1), [])  # the lookup over fast links first
        self.assertEqual(len(self.lookups), 1)
        self.assertIn(P1, self.fetcher.remembered(self.built.cid))  # kept: it wasn't asked

    def test_a_slow_node_asks_its_one_remembered_provider_before_an_open_link(self):
        self.config.max_parallel_providers = None  # its default: 1 on a slow node
        self.fetcher = self.make_fetcher(slow=True)
        self.fetcher.clock = lambda: self.now[0]
        self.hold(P1)
        self.hold(P2)
        self.fetcher.remember(self.built.cid, P1)
        self.client.links = [P2]
        self.fetcher.path_info = lambda node_id: self.LORA
        self.resolve(self.built.cid)
        self.assertEqual(self.client.requests, [(P1, [self.built.cid])])
        self.assertEqual(self.lookups, [])

    def test_a_search_that_found_nothing_isnt_repeated_by_a_fetch_that_isnt_explicit(self):
        self.runs.append([])
        with self.assertRaises(NotFound):
            self.resolve(self.built.cid, explicit=False)
        self.assertEqual(len(self.lookups), 1)
        self.now[0] += fetcher_module.UNFOUND_FOR - 1
        with self.assertRaises(NotFound) as caught:
            self.resolve(self.built.cid, explicit=False)  # a gateway reload
        self.assertEqual(len(self.lookups), 1)
        self.assertIn("a search 599s ago found none", str(caught.exception))
        with self.assertRaises(NotFound):
            self.resolve(self.built.cid)  # the user asks again: searched
        self.assertEqual(len(self.lookups), 2)
        self.now[0] += fetcher_module.UNFOUND_FOR
        with self.assertRaises(NotFound):
            self.resolve(self.built.cid, explicit=False)
        self.assertEqual(len(self.lookups), 3)

    def test_nodes_asked_first_are_still_asked(self):
        self.runs.append([])
        with self.assertRaises(NotFound):
            self.resolve(self.built.cid, explicit=False)
        self.hold(P1)
        self.client.links = [P1]  # an open link that has it now
        self.fetch_all(self.built.cid, explicit=False)
        self.assertTrue(self.storage.is_complete(self.built.cid))
        self.assertEqual(len(self.lookups), 1)
        self.assertIsNone(self.fetcher.unfound(self.built.cid))  # it was found after all

    def test_only_a_search_that_was_answered_and_found_nobody_counts(self):
        FakeLookup.unanswered = True  # offline: no node answered
        self.addCleanup(setattr, FakeLookup, "unanswered", False)
        self.runs.append([])
        with self.assertRaises(NotFound):
            self.resolve(self.built.cid, explicit=False)
        self.assertIsNone(self.fetcher.unfound(self.built.cid))
        FakeLookup.unanswered = False
        self.runs[:] = [[P1]]  # a provider found, which fails: someone provides it
        with self.assertRaises(NotFound):
            self.resolve(self.built.cid, explicit=False)
        self.assertIsNone(self.fetcher.unfound(self.built.cid))
        self.assertEqual(len(self.lookups), 2)


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

    def test_lost_request_is_retried_on_a_new_link(self):
        built = self.file(os.urandom(3 * CHUNK_SIZE))
        self.hold(P1)
        answers = [BlocksResult(LOST, unsent=[built.cid], reason="no answer")]
        self.client.behaviour[P1] = lambda cids: answers.pop() if answers else None
        self.runs.append([P1])
        with self.fetcher.open(built.cid) as fetch:
            fetch.fetch_dag(fetch.resolve())
            provider = fetch._providers[P1]
            self.assertEqual((provider.failed, provider.lost), (False, 0))
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual(self.client.closed, [P1])  # closed, so the retry goes out on a new link
        self.assertEqual(self.client.asked(P1)[:2], [built.cid, built.cid])

    def test_lost_max_failures_times_in_a_row_fails(self):
        built = self.file(b"l" * 3000)
        self.client.behaviour[P1] = lambda cids: BlocksResult(LOST, unsent=list(cids), reason="no answer")
        self.runs.append([P1])
        with self.assertRaises(NotFound):
            self.fetch_all(built.cid)
        self.assertEqual(len(self.client.requests), self.config.max_failures)
        self.assertEqual(self.client.closed, [P1] * self.config.max_failures)

    def test_another_provider_is_asked_while_a_lost_request_waits(self):
        self.config.max_parallel_providers = 1
        built = self.file(b"m" * 3000)
        self.hold(P2)
        self.client.behaviour[P1] = lambda cids: BlocksResult(LOST, unsent=list(cids), reason="no answer")
        self.runs.append([P1, P2])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual([node for node, _ in self.client.requests], [P1, P2])

    def test_an_answer_resets_the_lost_count(self):
        self.config.max_parallel_providers = 1
        built = self.file(os.urandom(6 * CHUNK_SIZE))
        self.hold(P1)
        # Two lost of every three requests: never max_failures in a row
        count = [0]

        def lossy(cids):
            """Lose the first two requests of every three."""
            count[0] += 1
            return BlocksResult(LOST, unsent=list(cids), reason="no answer") if count[0] % 3 else None
        self.client.behaviour[P1] = lossy
        self.runs.append([P1])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))

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


class ScoreTest(FetcherTest):
    """Local scores (section 6): raised by verified blocks and the answers that led to them, lowered by failures."""

    def setUp(self):
        """Scores on a clock that stands still, so none decays while a slow runner works through a fetch."""
        super().setUp()
        self.fetcher.scores.clock = FakeClock()

    def test_verified_blocks_raise_the_provider_and_once_per_fetch_its_listers(self):
        self.config.max_parallel_providers = 1
        built = self.file(os.urandom(5 * CHUNK_SIZE))
        self.hold(P1)
        self.runs.append([P1])
        self.listed[P1] = {P2, P3}
        self.fetch_all(built.cid)
        responses = len([n for n, _ in self.client.requests if n == P1])
        self.assertGreater(responses, 1)
        scores = self.fetcher.scores
        self.assertAlmostEqual(scores.score(P1), responses * scoring.SERVED, places=4)
        self.assertAlmostEqual(scores.score(P2), scoring.USEFUL_ANSWER, places=4)
        self.assertAlmostEqual(scores.score(P3), scoring.USEFUL_ANSWER, places=4)

    def test_failing_providers_and_bad_blocks_lower_scores(self):
        self.config.max_parallel_providers = 1
        built = self.file(b"f" * 3000)
        self.hold(P3)
        self.client.behaviour[P1] = lambda cids: BlocksResult(FAILED, unsent=list(cids), reason="unreachable")
        self.client.behaviour[P2] = lambda cids: BlocksResult(OK, bad=[cids[0]], unsent=list(cids[1:]))
        self.runs.append([P1, P2, P3])
        self.listed[P2] = {P1}  # P1 listed P2, which sent nothing verified: no credit for that
        self.fetch_all(built.cid)
        scores = self.fetcher.scores
        self.assertAlmostEqual(scores.score(P1), scoring.FAILED, places=4)
        self.assertAlmostEqual(scores.score(P2), scoring.BAD_BLOCK + scoring.FAILED, places=4)
        self.assertAlmostEqual(scores.score(P3), scoring.SERVED, places=4)

    def test_scores_given_are_used(self):
        scores = scoring.Scores()
        fetcher = Fetcher(self.storage, self.client, self.config, OWN, self.routing, lambda key: None, False, None,
                          scores=scores)
        self.assertIs(fetcher.scores, scores)


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
        self.fetch._handle(P1, [cid], result, False, {}, {}, set(), self.fetch.budget, sent_at, rate)

    def test_slow_link_waits_before_asking_again(self):
        self.result(OK, sent_at=self.now - 40, rate=50_000)  # 40 s for that request
        # At the default cap, half the rate estimate, the next response may start 2 × 40 s after the last one
        self.assertEqual(self.provider.busy_until, self.now + 40)
        self.assertGreaterEqual(self.fetch._progress_at, self.now + 40)  # waiting isn't counted as hanging
        # ...nor leaving the link idle: the sweep would close it, and its rate estimate (and this pacing) with it
        self.assertEqual(self.client.held, [(P1, 40)])

    def test_response_progress_doesnt_cut_a_pacing_wait_short(self):
        self.result(OK, sent_at=self.now - 40, rate=50_000)
        paced = self.fetch._progress_at
        self.now += 1
        self.client.behaviour[P1] = lambda cids: self.client.on_progress(0.5)  # then served as usual
        self.fetch._send(P1, [self.built.cid])
        self.fetch._events.get(timeout=5)
        # The request thread only notes when it heard progress; the deadline counts from the later of the two
        self.assertEqual((self.fetch._progress_at, self.fetch._heard_at), (paced, self.now))

    def test_fast_link_isnt_paced(self):
        self.result(OK, sent_at=self.now - 40, rate=200_000)
        self.assertEqual(self.provider.busy_until, 0)
        self.result(OK, sent_at=self.now - 40, rate=None)
        self.assertEqual(self.provider.busy_until, 0)
        self.assertEqual(self.client.held, [])

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

    def test_closed_links_count_even_after_serving(self):
        # A provider closing the link (no room for another incoming link) is busy, not pacing: counted, and failed only
        # after max_failures in a row, each retried after the busy delay
        self.result(OK, sent_at=self.now - 40, rate=50_000)
        self.result(BUSY, "closed")
        self.assertEqual((self.provider.busy, self.provider.failed), (1, False))
        for _ in range(self.config.max_failures - 1):
            self.result(BUSY, "closed")
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
        event = self.fetch._events.get(timeout=5)
        self.fetch._handle(event.node_id, event.cids, event.result, False, {}, {}, set(), self.fetch.budget,
                           event.sent_at, event.rate)

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
    room = fetch.budget.room((cid for cids, _ in fetch._in_flight.values() for cid in cids), wanted)
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
            room = fetch.budget.reserve(cid, wanted, room)
            if not fetch.budget.fits(room) and (batch or fetch._in_flight):
                break
        batch.append(pending.popleft())
        total += guess
    return batch


class SlowLinkTest(FetcherTest):
    """Parallelism, probes and duplication follow each provider's link, not the node (section 7, item 7): a node
    with a fast and a slow interface (a gateway) isn't slow, but its providers behind the slow one share one request
    at a time, and nothing is probed or duplicated over a slow link."""

    LORA = mesh.PathInfo(1, "LoRa", 1200)
    OTHER_LORA = mesh.PathInfo(1, "LoRa 2", 1200)
    TCP = mesh.PathInfo(1, "TCP", 10_000_000)

    def paths(self, table: dict):
        """Have path_info answer from table (nothing known for the rest)."""
        self.fetcher.path_info = lambda node_id: table.get(node_id, mesh.NO_PATH)

    def concurrency(self, groups: dict[bytes, str], company: bool = False) -> dict[str, int]:
        """Serve every provider as usual, a little slowly, recording the most requests each group (by node) had in
        flight at once. With company, a request waits (5 s at most) until its group has had two in flight at once,
        so a slow runner starting the request threads late can't hide that they may overlap."""
        lock = threading.Condition()
        now: dict[str, int] = {}
        most: dict[str, int] = {}

        def serve(node_id):
            """A behaviour counting node_id's group in flight while it answers."""
            group = groups[node_id]

            def behaviour(cids):
                """Count, wait a little (or for company), and serve as usual."""
                with lock:
                    now[group] = now.get(group, 0) + 1
                    most[group] = max(most.get(group, 0), now[group])
                    lock.notify_all()
                    if company:
                        lock.wait_for(lambda: most[group] >= 2, timeout=5)
                time.sleep(0.02)  # how long the provider takes to answer
                with lock:
                    now[group] -= 1
                return None
            return behaviour
        for node_id in groups:
            self.hold(node_id)
            self.client.behaviour[node_id] = serve(node_id)
        return most

    def test_providers_behind_one_slow_interface_share_one_request(self):
        built = self.file(os.urandom(24 * CHUNK_SIZE))
        self.paths({P1: self.LORA, P2: self.LORA, P3: self.TCP, P4: self.OTHER_LORA})
        most = self.concurrency({P1: "lora", P2: "lora", P3: "tcp", P4: "lora 2"})
        self.config.max_parallel_providers = 4
        self.runs.append([P1, P2, P3, P4])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual(most["lora"], 1)  # P1 and P2: one at a time
        self.assertEqual(set(most), {"lora", "tcp", "lora 2"})  # each interface was used

    def test_a_slow_rate_estimate_shares_the_slot_of_an_unknown_interface(self):
        built = self.file(os.urandom(12 * CHUNK_SIZE))
        self.client.rate = 50_000  # every link measured slow; no path known
        most = self.concurrency({P1: "all", P2: "all", P3: "all"})
        self.runs.append([P1, P2, P3])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual(most["all"], 1)

    def test_a_fast_rate_estimate_is_its_own_slot(self):
        built = self.file(os.urandom(24 * CHUNK_SIZE))
        self.client.rate = 10_000_000  # measured fast, whatever the interface says
        self.paths({P1: self.LORA, P2: self.LORA, P3: self.LORA})
        most = self.concurrency({P1: "all", P2: "all", P3: "all"}, company=True)
        self.storage.put(built.cid, self.network[built.cid])  # so the first request isn't the root's, alone
        self.runs.append([P1, P2, P3])
        self.fetch_all(built.cid)
        self.assertGreater(most["all"], 1)

    def test_a_slow_path_shares_a_slot(self):
        built = self.file(os.urandom(12 * CHUNK_SIZE))
        self.fetcher.slow_path = lambda node_id: node_id in (P1, P2)  # e.g. more than 3 hops away
        most = self.concurrency({P1: "far", P2: "far", P3: "near"})
        self.runs.append([P1, P2, P3])
        self.fetch_all(built.cid)
        self.assertEqual(most["far"], 1)

    def test_open_links_over_slow_links_are_not_probed(self):
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        for node_id in (P1, P2, P3):
            self.hold(node_id)
        self.client.links = [P1, P2]
        self.paths({P1: self.LORA, P2: self.TCP})
        self.fetch_all(built.cid)
        self.assertEqual(self.client.asked(P1), [])
        self.assertEqual(self.client.requests[0], (P2, [built.cid]))
        # Only slow links open: no probe, the lookup at once
        self.client.links = [P1]
        self.client.requests.clear()
        other = self.file(os.urandom(CHUNK_SIZE))
        self.hold(P3)
        self.runs.append([P3])
        self.fetch_all(other.cid)
        self.assertEqual(self.client.asked(P1), [])
        self.assertEqual(len(self.lookups), 1)

    def test_a_slow_node_probes_one_open_link(self):
        # Every link of a slow node is slow; one probe that hits saves a whole lookup over the radio
        self.fetcher = self.make_fetcher(slow=True)
        self.config.max_parallel_providers = None  # a slow node's default: one at a time
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        for node_id in (P1, P2):
            self.hold(node_id)
        self.client.links = [P1, P2]  # most recently used first
        self.paths({P1: self.LORA, P2: self.LORA})
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual(self.client.requests[0], (P1, [built.cid]))  # the one probe, which found the root
        self.assertEqual(self.client.asked(P2), [])
        self.assertEqual(self.lookups, [])

    def test_nothing_is_duplicated_over_a_slow_link(self):
        self.config.deadline_slack = 0.1
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        dripped = walker.expand(walker.root_expect(built.cid), self.network[built.cid])[0].cid  # P1 is asked first
        released = threading.Event()
        self.addCleanup(released.set)
        for node_id in (P1, P2):
            self.hold(node_id)

        def drip(cids):
            """Take a while over the dripped chunk."""
            if dripped in cids:
                released.wait(1.0)
            return None
        self.client.behaviour[P1] = drip
        self.paths({P1: self.TCP, P2: self.LORA})  # the idle provider is behind the radio
        self.runs.append([P1, P2])
        self.fetch_all(built.cid)
        self.assertEqual(self.client.asked(P1) + self.client.asked(P2),
                         list(dict.fromkeys(self.client.asked(P1) + self.client.asked(P2))))  # nothing asked twice

    def test_a_batch_on_a_measured_link_is_duplicated_after_one_and_a_half_times_its_expected_time(self):
        self.config.deadline_slack = 1
        self.client.rate = 1_000_000  # fast: a chunk takes about 0.52 s
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        chunk = walker.expand(walker.root_expect(built.cid), self.network[built.cid])[0]
        now = [1000.0]
        self.fetcher.clock = lambda: now[0]
        release = threading.Event()
        self.addCleanup(release.set)
        for node_id in (P1, P2):
            self.hold(node_id)
            self.client.behaviour[node_id] = lambda cids: (release.wait(5), None)[1]
        with self.fetcher.open(built.cid) as fetch:
            providers = [fetch._new_provider(P1), fetch._new_provider(P2)]
            wanted = {chunk.cid: [chunk]}
            pending = fetcher_module._Pending([chunk.cid])
            fetch._assign(pending, fetch._in_flight, {chunk.cid}, wanted, fetch.budget)
            self.assertEqual(list(fetch._in_flight), [P1])
            expected = fetch._expected[P1]
            self.assertAlmostEqual(expected, chunk.size * 8 / 1_000_000)
            # 10 s expected: duplicated after 15 s, not after deadline_slack
            fetch._expected[P1] = 10.0
            now[0] += 14
            fetch._duplicate(providers, fetch._in_flight, 1, now[0])
            self.assertEqual(list(fetch._in_flight), [P1])
            now[0] += 2
            fetch._duplicate(providers, fetch._in_flight, 1, now[0])
            self.assertEqual(sorted(fetch._in_flight), [P1, P2])
            # Without a rate estimate, deadline_slack alone
            self.assertEqual(fetch._duplicate_after(P3), 1)
            release.set()


TEXT = (b"It is a truth universally acknowledged, that a single man in possession of a good fortune, must be in want "
        b"of a wife. However little known the feelings or views of such a man may be on his first entering a "
        b"neighbourhood, this truth is so well fixed in the minds of the surrounding families. ")


def text_of(size: int) -> bytes:
    """size bytes of English-like text, varied enough that bz2 doesn't make it vanish."""
    rng = random.Random(size)
    words = TEXT.split()
    out = bytearray()
    while len(out) < size:
        out += b" ".join(rng.choice(words) for _ in range(12)) + b"\n"
    return bytes(out[:size])


class RangeTransferTest(FetcherTest):
    """/range (rnfs 0.2.0): large chunks over slow links come in ranges, compressed, into a partial block that a stall
    doesn't lose; fast links keep /blocks; an older node's silence is learnt once, persisted, and costs one request."""

    def ranges_of(self, node_id: bytes, cid: Cid) -> list[tuple[int, int, bool]]:
        """(offset, max_bytes, compress) of every range node_id was asked for of cid."""
        return [(offset, max_bytes, compress) for node, c, offset, max_bytes, compress in self.client.ranges
                if node == node_id and c == cid]

    def chunks(self, built) -> list[walker.Expect]:
        """The chunk expectations under a built file's root."""
        return walker.expand(walker.root_expect(built.cid), self.network[built.cid])

    def test_a_slow_link_fetches_large_chunks_in_compressed_ranges(self):
        self.fetcher = self.make_fetcher(slow=True)
        data = text_of(3 * CHUNK_SIZE)
        built = self.file(data)
        self.hold(P1)
        self.runs.append([P1])
        self.fetch_all(built.cid)
        self.assertEqual(self.read(built.cid), data)
        first = self.ranges_of(P1, self.chunks(built)[0].cid)
        self.assertGreater(len(first), 1)  # several ranges, not one whole chunk
        self.assertEqual(first[0][:2], (0, self.config.min_rate_sample))  # no rate: what gives an estimate
        for chunk in self.chunks(built):
            self.assertTrue(all(compress for _, _, compress in self.ranges_of(P1, chunk.cid)))
        # The root, a DAG node, came with /blocks; the later chunks' ranges were scaled by the compression achieved
        self.assertEqual(self.ranges_of(P1, built.cid), [])
        last = self.ranges_of(P1, self.chunks(built)[-1].cid)
        self.assertGreater(last[0][1], 2 * self.config.min_rate_sample)
        self.assertEqual(self.fetcher.ranges.partials.count(), 0)  # every partial dropped once its block is stored

    def test_a_fast_link_keeps_blocks(self):
        built = self.file(os.urandom(3 * CHUNK_SIZE))
        self.hold(P1)
        self.runs.append([P1])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertEqual(self.client.ranges, [])

    def test_small_chunks_and_dag_nodes_keep_blocks_on_a_slow_link(self):
        self.fetcher = self.make_fetcher(slow=True)
        files = [(f"f{i}", builder.build_bytes(os.urandom(1000), self.put)) for i in range(5)]
        built = builder.build_directory([Entry(name, file.cid, file.size, 0) for name, file in files], self.put)
        self.hold(P1)
        self.runs.append([P1])
        self.fetch_all(built.cid)
        self.assertEqual(self.client.ranges, [])

    def test_a_stall_mid_chunk_resumes_where_it_stopped(self):
        """A range that stalls loses only itself: the chunk resumes from the bytes held, where /blocks would send the
        whole chunk again."""
        self.fetcher = self.make_fetcher(slow=True)
        data = os.urandom(2 * CHUNK_SIZE)  # random: no compression, so the ranges are exactly min_rate_sample each
        built = self.file(data)
        chunk = self.chunks(built)[0].cid
        self.hold(P1)
        stalled = []

        def stall_once(cid, offset, max_bytes, compress):
            """The third range of the chunk stalls once (the link closed under it)."""
            if cid == chunk and offset == 2 * self.config.min_rate_sample and not stalled:
                stalled.append(offset)
                return client_module.RangeResult(STALLED, reason="no progress for 120s")
            return None
        self.client.range_behaviour[P1] = stall_once
        self.runs.append([P1])
        self.fetch_all(built.cid)
        self.assertEqual(self.read(built.cid), data)
        step = self.config.min_rate_sample
        self.assertEqual([offset for offset, _, _ in self.ranges_of(P1, chunk)],
                         [0, step, 2 * step, 2 * step, 3 * step])
        # Bytes asked for again after the stall: the stalled range only (16 KiB), not the 32 KiB already held
        offsets = [offset for offset, _, _ in self.ranges_of(P1, chunk)]
        self.assertEqual(len(offsets) - len(set(offsets)), 1)

    def test_an_old_providers_silence_costs_one_request_a_fetch_and_is_learnt_after_two(self):
        self.fetcher = self.make_fetcher(slow=True)
        first, second = self.file(os.urandom(3 * CHUNK_SIZE)), self.file(os.urandom(2 * CHUNK_SIZE))
        self.hold(P1)
        self.client.no_range.add(P1)  # an rnfs 0.1.x node
        self.runs.extend([[P1], [P1], [P1]])
        self.fetch_all(first.cid)
        self.assertEqual(len(self.client.ranges), 1)  # asked once, then /blocks for the rest of the fetch
        self.assertTrue(self.storage.is_complete(first.cid))
        self.assertIsNone(self.fetcher.ranges.support.answers(P1))  # one silence: maybe a lost packet
        self.assertFalse(self.routing.is_banned(P1))  # not penalised: its silence isn't a failure
        self.fetch_all(second.cid)
        self.assertEqual(len(self.client.ranges), 2)  # the second silence in a row: learnt
        self.assertTrue(self.storage.is_complete(second.cid))
        self.assertIs(self.fetcher.ranges.support.answers(P1), False)
        # Persisted: a restarted fetcher doesn't ask it again
        self.fetcher.close()
        self.fetcher = self.make_fetcher(slow=True)
        self.assertIs(self.fetcher.ranges.support.answers(P1), False)
        third = self.file(os.urandom(2 * CHUNK_SIZE))
        self.hold(P1)
        self.fetch_all(third.cid)
        self.assertEqual(len(self.client.ranges), 2)

    def test_one_lost_range_doesnt_mark_a_node_that_answers(self):
        self.fetcher = self.make_fetcher(slow=True)
        first, second = self.file(os.urandom(2 * CHUNK_SIZE)), self.file(os.urandom(2 * CHUNK_SIZE))
        self.hold(P1)
        lost = []

        def lose_one(cid, offset, max_bytes, compress):
            """The first /range is lost on the way (RNS times it out with the link up)."""
            if not lost:
                lost.append(cid)
                return client_module.RangeResult(LOST, silent=True, reason="no answer")
            return None
        self.client.range_behaviour[P1] = lose_one
        self.runs.extend([[P1], [P1]])
        self.fetch_all(first.cid)
        self.assertTrue(self.storage.is_complete(first.cid))
        self.assertIsNone(self.fetcher.ranges.support.answers(P1))
        self.fetch_all(second.cid)  # ranges again, answered: the silence is forgotten
        self.assertTrue(self.storage.is_complete(second.cid))
        self.assertGreater(len(self.client.ranges), 1)
        self.assertIs(self.fetcher.ranges.support.answers(P1), True)

    def test_a_provider_that_stalled_gets_ranges_on_a_fast_link(self):
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        chunk = self.chunks(built)[0].cid
        self.hold(P1)
        answers = []

        def stall_first(cids):
            """The first /blocks with the chunk stalls."""
            if chunk in cids and not answers:
                answers.append(1)
                return BlocksResult(STALLED, unsent=list(cids), reason="stalled")
            return None
        self.client.behaviour[P1] = stall_first
        self.runs.append([P1])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        asked = self.ranges_of(P1, chunk)
        self.assertTrue(asked)
        self.assertTrue(all(not compress and max_bytes <= fetcher_module.STALLED_RANGE
                            for _, max_bytes, compress in asked))

    def test_a_block_held_in_part_is_left_to_its_provider(self):
        self.config.max_parallel_providers = 2
        self.fetcher = self.make_fetcher()
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        chunk = self.chunks(built)[0].cid
        self.hold(P1)
        self.hold(P2)
        with self.fetcher.open(built.cid) as fetch:
            fetch._new_provider(P1)
            fetch._new_provider(P2)
            fetch._owners[chunk] = P1
            pending = fetcher_module._Pending([chunk, self.chunks(built)[1].cid])
            self.assertEqual(fetch._others_owned(pending, P2), [chunk])  # P2 takes what follows
            self.assertEqual(list(pending), [self.chunks(built)[1].cid])
            pending.push_front([chunk])
            self.assertEqual(fetch._others_owned(pending, P1), [])  # its owner takes it
            fetch._fail(fetch._providers[P1], "test")
            self.assertEqual(fetch._others_owned(pending, P2), [])  # a failed owner's blocks go to the others

    def test_a_mismatching_block_from_ranges_bans_its_provider(self):
        self.fetcher = self.make_fetcher(slow=True)
        data = os.urandom(2 * CHUNK_SIZE)
        built = self.file(data)
        chunk = self.chunks(built)[0].cid
        forged = bytearray(self.network[chunk])
        forged[-1] ^= 1
        self.hold(P1)
        self.client.holdings[P1][chunk] = bytes(forged)
        self.hold(P2)
        self.paths({})
        self.runs.append([P1, P2])
        self.config.max_parallel_providers = 1
        self.fetch_all(built.cid)
        self.assertEqual(self.read(built.cid), data)
        self.assertTrue(self.routing.is_banned(P1))

    def paths(self, table: dict):
        """Have path_info answer from table (nothing known for the rest)."""
        self.fetcher.path_info = lambda node_id: table.get(node_id, mesh.NO_PATH)


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
        return self.fetch._take_batch(deque(self.wanted), P1, self.wanted, self.fetch.budget)

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
                self.assertEqual(self.fetch._take_batch(new, P1, wanted, self.fetch.budget), expected)
                self.assertEqual(list(new), list(old))


class RequeueTest(FetcherTest):
    """What a result puts back in the pending queue, and where, with a large file's chunks pending (A3)."""

    def test_requeue_keeps_the_walk_order_with_a_large_pending_queue(self):
        fetch = self.fetcher.open(self.file(b"root").cid).__enter__()
        self.addCleanup(fetch.__exit__, None, None, None)
        chunks = [i.to_bytes(8, "big") for i in range(20_000)]
        expects = [walker.Expect(Cid.for_block(RAW, data), len(data), walker.FILE_TYPES, 2) for data in chunks]
        blocks = {expect.cid: data for expect, data in zip(expects, chunks)}
        wanted = {expect.cid: [expect] for expect in expects}
        walk = list(wanted)
        outstanding = set(wanted)
        pending = fetcher_module._Pending(wanted)
        for node_id in (P1, P2):
            fetch._new_provider(node_id)
        first = [pending.popleft() for _ in range(32)]
        second = [pending.popleft() for _ in range(32)]
        in_flight = {P1: (first, False), P2: (second, False)}

        def result(node_id: bytes, cids: list[Cid], outcome: BlocksResult):
            """Handle node_id's result for cids."""
            fetch._handle_event(fetcher_module.ResultEvent(node_id, cids, outcome, None, None), pending, in_flight,
                                outstanding, wanted, {}, fetch.budget)

        # P2's budget cut its response after 10 blocks: the rest go back to the front
        result(P2, second, BlocksResult(OK, blocks={cid: blocks[cid] for cid in second[:10]}, unsent=second[10:]))
        self.assertEqual(list(pending), walk[42:])
        # A probe asks for first[0] (still in flight with P1) and the next pending CID (still pending)
        in_flight[P3] = ([first[0], pending[0]], True)
        result(P1, first, BlocksResult(BUSY))  # first[0] stays out: the probe still has it
        self.assertEqual(list(pending), first[1:] + walk[42:])
        result(P3, [first[0], walk[42]], BlocksResult(FAILED, unsent=[first[0], walk[42]]))
        # Back in the walk's order, each CID once, and only the ones not stored
        self.assertEqual(list(pending), walk[:32] + walk[42:])
        self.assertEqual(len(pending), len(walk) - 10)
        self.assertNotIn(second[0], pending)
        self.assertIn(walk[-1], pending)
        drained = [pending.popleft() for _ in range(len(pending))]
        self.assertEqual(drained, walk[:32] + walk[42:])
        self.assertFalse(any(cid in pending for cid in walk))


class DagBudgetTest(unittest.TestCase):
    """_DagBudget, the pure bookkeeping of a fetch's DAG byte limits, and the budgets of its read_range calls."""

    def setUp(self):
        """A fetch's own budget, with no limit yet, a DAG node and a raw chunk."""
        self.budget = fetcher_module._DagBudget(Cid.for_block(RAW, b"root"))
        self.node = Cid.for_block(DAG, b"node")
        self.chunk = Cid.for_block(RAW, b"chunk")

    def test_nothing_is_counted_without_a_limit(self):
        self.assertIsNone(self.budget.limit())
        self.assertIsNone(self.budget.room([self.node], {}))
        self.budget.count(self.node, 1000)
        self.assertEqual(self.budget.used, 0)

    def test_a_read_range_budget_counts_against_the_fetchs_too(self):
        self.budget.max_dag_bytes = 10_000
        first = self.budget.for_range(3000, 1)
        first.count(self.node, 2000)
        first.count(self.node, 2000)  # once per call
        first.count(self.chunk, 5000)  # a raw chunk counts against the fetch's limits only
        self.assertEqual((first.range_used, self.budget.used), (2000, 7000))
        second = self.budget.for_range(3000, 1)
        second.count(self.node, 2000)  # each call counts the nodes it meets; the fetch, each block once
        self.assertEqual((second.range_used, self.budget.used), (2000, 7000))
        with self.assertRaises(TooLarge):
            second.count(Cid.for_block(DAG, b"other"), 1001)
        with self.assertRaises(TooLarge):
            self.budget.count(Cid.for_block(RAW, b"large"), 3001)

    def test_room_reserves_what_is_in_flight(self):
        self.budget.max_dag_bytes = 100 * MAX_BLOCK_SIZE
        budget = self.budget.for_range(10 * MAX_BLOCK_SIZE, 1)
        wanted = {self.chunk: [walker.Expect(self.chunk, 10, walker.FILE_TYPES, 2)]}
        # A node not received may be MAX_BLOCK_SIZE; a raw chunk is its declared size, and nothing to the range
        room = budget.room([self.node, self.chunk], wanted)
        self.assertEqual(room, (None, 99 * MAX_BLOCK_SIZE - 10, 9 * MAX_BLOCK_SIZE))
        self.assertTrue(budget.fits(room))
        self.assertFalse(budget.fits((None, -1, 0)))


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


class ReadAheadTest(FetcherTest):
    """read_range asks for the next window while the reader takes this one (A1); a reader closing early closes
    the links still bringing it, and never cancels a response (NET-1)."""

    def setUp(self):
        """A file of four windows of WINDOW (6) chunks, held by three providers. Each request carries one chunk (no
        rate estimate: CHUNK_SIZE), and one for a chunk of window k + 1 is answered only once the reader has had
        the first bytes of window k (self.gates[k] set), so what is in flight when it gets them is seen."""
        super().setUp()
        patcher = mock.patch.object(fetcher_module, "WINDOW", 6)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.data = os.urandom(4 * 6 * CHUNK_SIZE)
        self.built = self.file(self.data)
        self.chunks = dag.decode(self.network[self.built.cid]).children
        self.window_of = {child.cid: i // 6 for i, child in enumerate(self.chunks)}
        self.gates = [threading.Event() for _ in range(4)]
        for gate in self.gates:
            self.addCleanup(gate.set)  # so no request thread outlives the test

        def gated(cids):
            """Serve as usual once the reader has had the previous window's first bytes."""
            window = min(self.window_of.get(cid, 0) for cid in cids)
            if window:
                self.gates[window - 1].wait(5)
            return None
        for node_id in (P1, P2, P3):
            self.hold(node_id)
            self.client.behaviour[node_id] = gated
        self.runs.append([P1, P2, P3])

    def test_every_provider_is_busy_across_window_boundaries(self):
        busy = []
        pieces = []
        with self.fetcher.open(self.built.cid) as fetch:
            for i, piece in enumerate(fetch.read_range(fetch.resolve())):
                if i % 6 == 0:
                    # The reader has this window's first bytes: the next window's requests are already out
                    busy.append(sorted(node for node, (_, probe) in fetch._in_flight.items() if not probe))
                    self.gates[i // 6].set()
                pieces.append(piece)
            self.assertEqual(fetch._expects, {})  # nothing left over once read
        self.assertEqual(b"".join(pieces), self.data)
        self.assertEqual(busy, [[P1, P2, P3]] * 3 + [[]])  # the last window has none after it
        asked = [cid for node in (P1, P2, P3) for cid in self.client.asked(node)]
        self.assertEqual(sorted(asked), sorted([self.built.cid] + [child.cid for child in self.chunks]))

    def test_early_closing_reader_closes_its_links(self):
        def read():
            """What node.read does: the fetch ends when the reader closes its iterator."""
            with self.fetcher.open(self.built.cid) as fetch:
                self.fetch = fetch
                yield from fetch.read_range(fetch.resolve())

        reader = read()
        self.assertEqual(next(reader), self.data[:CHUNK_SIZE])
        in_flight = sorted(self.fetch._in_flight)
        self.assertEqual(in_flight, [P1, P2, P3])
        reader.close()
        self.assertEqual(sorted(self.client.closed), in_flight)
        # It fetched at most the window after the one it read
        asked = [cid for node in (P1, P2, P3) for cid in self.client.asked(node)]
        self.assertLessEqual(len(asked), 1 + 2 * 6)
        self.assertNotIn(self.chunks[2 * 6].cid, asked)

    def test_a_reader_left_unread_closes_its_links_when_the_fetch_ends(self):
        with self.fetcher.open(self.built.cid) as fetch:
            reader = fetch.read_range(fetch.resolve())
            next(reader)
            in_flight = sorted(fetch._in_flight)
        self.assertEqual(sorted(self.client.closed), in_flight)
        self.assertTrue(in_flight)
        reader.close()

    def test_a_block_arriving_after_its_call_is_stored(self):
        first, second, third = walker.expand(walker.root_expect(self.built.cid), self.network[self.built.cid])[:3]
        gate = threading.Event()
        self.addCleanup(gate.set)

        def held(cids):
            """Serve second only once the gate opens."""
            if second.cid in cids:
                gate.wait(5)
            return None
        for node_id in (P1, P2, P3):
            self.client.behaviour[node_id] = held
        with self.fetcher.open(self.built.cid) as fetch:
            fetch.resolve()
            fetch.ensure([first], ahead=[second])  # returns with second still in flight
            self.assertFalse(self.storage.has(second.cid))
            gate.set()
            deadline = time.monotonic() + 5
            while fetch._events.empty() and time.monotonic() < deadline:
                time.sleep(0.01)
            fetch.ensure([third])  # second's result arrives during a call that doesn't want it
            self.assertTrue(self.storage.has(second.cid))
        asked = [cid for node in (P1, P2, P3) for cid in self.client.asked(node)]
        self.assertEqual(asked.count(second.cid), 1)
        self.assertEqual(self.client.closed, [])


class RangeWindowTest(FetcherTest):
    """A byte-range read's window follows the providers' links: WINDOW chunks while one is over a fast link, else
    SLOW_WINDOW (one chunk, one more asked for ahead), so the first byte over LoRa doesn't wait for 32 chunks."""

    def setUp(self):
        """A file of eight chunks, held by P1."""
        super().setUp()
        self.data = os.urandom(8 * CHUNK_SIZE)
        self.built = self.file(self.data)
        self.chunks = [child.cid for child in dag.decode(self.network[self.built.cid]).children]
        self.hold(P1)
        self.runs.append([P1])

    def first_piece(self, path: mesh.PathInfo) -> list[Cid]:
        """Read the file with P1 behind path; the CIDs asked for by the time the reader has its first bytes (and the
        whole file read after)."""
        self.fetcher.path_info = lambda node_id: path
        with self.fetcher.open(self.built.cid) as fetch:
            reader = fetch.read_range(fetch.resolve())
            pieces = [next(reader)]
            asked = self.client.asked(P1)
            pieces.extend(reader)
        self.assertEqual(b"".join(pieces), self.data)
        return asked

    def test_a_slow_link_reads_one_chunk_at_a_time_one_ahead(self):
        asked = self.first_piece(mesh.PathInfo(1, "LoRa", 1200))
        self.assertEqual(asked, [self.built.cid] + self.chunks[:2])

    def test_a_fast_link_reads_a_full_window(self):
        asked = self.first_piece(mesh.PathInfo(1, "TCP", 10_000_000))
        self.assertEqual(sorted(asked), sorted([self.built.cid] + self.chunks))  # fewer than WINDOW: all of them

    def test_with_no_provider_known_yet_the_first_window_is_small(self):
        self.storage.put(self.built.cid, self.network[self.built.cid])  # the root stored: no provider asked for it
        with self.fetcher.open(self.built.cid) as fetch:
            reader = fetch.read_range(fetch.resolve())
            pieces = [next(reader)]
            self.assertEqual(self.client.asked(P1), self.chunks[:2])
            pieces.extend(reader)  # P1 is known, over a link not known to be slow: the rest in one window
            self.assertEqual(fetch._window(), fetcher_module.WINDOW)
        self.assertEqual(b"".join(pieces), self.data)


class ReadAheadLinkTest(unittest.TestCase):
    """An early-closing reader over the real PeerClient and LinkPool (RNS faked as in test_links): its link is torn
    down, which stops the response, unless another fetch has a request on it; it never cancels the response's
    Resource (a responder would cool it down)."""

    def setUp(self):
        """A fetcher using a PeerClient whose links are FakeLinks; one provider serving a file of three windows of
        two chunks, which holds back its answers past the first window until self.release is set (5 s at most),
        and another file of as many chunks, which it serves at once."""
        patcher = mock.patch.object(links, "RNS", link_fakes.fake_rns())
        patcher.start()
        self.addCleanup(patcher.stop)
        window = mock.patch.object(fetcher_module, "WINDOW", 2)
        window.start()
        self.addCleanup(window.stop)
        link_fakes.FakeLink.created = []
        link_fakes.FakeLink.establish_delay = 0.0
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        config = Config(rnfs_dir=tmp.name, slow="no")
        storage = Storage(os.path.join(tmp.name, "blocks"), os.path.join(tmp.name, "state"), 10**9)
        self.addCleanup(storage.close)
        self.pool = links.LinkPool(object(), config)
        self.addCleanup(self.pool.close_all)
        network: dict[Cid, bytes] = {}
        self.data = os.urandom(6 * CHUNK_SIZE)
        self.root = builder.build_bytes(self.data, network.__setitem__).cid
        self.other_data = os.urandom(6 * CHUNK_SIZE)
        self.other = builder.build_bytes(self.other_data, network.__setitem__).cid
        held = {child.cid for child in dag.decode(network[self.root]).children[2:]}
        self.release = threading.Event()
        self.addCleanup(self.release.set)

        def respond(link, path, data):
            """Serve at once, but the first file's chunks past its first window only once released, as a slow
            response would arrive."""
            cids = [Cid.from_bytes(binary) for binary in data[1]]
            if held.intersection(cids):
                self.release.wait(5)
            return [network[cid] for cid in cids]
        link_fakes.FakeLink.responder = staticmethod(respond)
        self.fetcher = Fetcher(storage, client_module.PeerClient(self.pool, config), config, OWN,
                               RoutingTable(OWN, config), lambda key: FakeLookup([[P1]]))
        self.addCleanup(self.fetcher.close)
        self.fetches = []

    def read(self, root: Cid):
        """What node.read does: the fetch ends when the reader closes its iterator."""
        with self.fetcher.open(root) as fetch:
            self.fetches.append(fetch)
            yield from fetch.read_range(fetch.resolve())

    def test_closing_the_reader_tears_the_link_down(self):
        reader = self.read(self.root)
        self.assertEqual(next(reader), self.data[:CHUNK_SIZE])
        link = link_fakes.FakeLink.created[0]
        response = mock.Mock()  # the next window's response Resource, arriving
        link.incoming_resources = [response]
        self.assertEqual(link.status, link_fakes.FakeLink.ACTIVE)
        reader.close()
        self.assertEqual(link.status, link_fakes.FakeLink.CLOSED)
        response.cancel.assert_not_called()

    def test_closing_a_reader_leaves_a_link_another_fetch_uses(self):
        other = self.read(self.other)
        self.assertEqual(next(other), self.other_data[:CHUNK_SIZE])  # its provider found, its link open
        reader = self.read(self.root)
        self.assertEqual(next(reader), self.data[:CHUNK_SIZE])  # its next window now holds the link's /blocks slot
        rest = []
        thread = threading.Thread(target=lambda: rest.append(b"".join(other)), daemon=True)
        thread.start()
        peer = self.pool.shared_peer(P1)
        deadline = time.monotonic() + 5
        while len(peer.users) < 2 and time.monotonic() < deadline:  # the other read's request waits for the slot
            time.sleep(0.01)
        self.assertEqual(len(peer.users), 2)
        link = link_fakes.FakeLink.created[0]
        response = mock.Mock()
        link.incoming_resources = [response]
        reader.close()  # gives up mid-window: its request is left to finish, the other's isn't failed
        self.assertEqual(link.status, link_fakes.FakeLink.ACTIVE)
        self.release.set()
        thread.join(10)
        self.assertEqual(rest, [self.other_data[CHUNK_SIZE:]])
        provider = self.fetches[0]._providers[P1]
        self.assertEqual((provider.failed, provider.stalls, provider.busy), (False, 0, 0))
        self.assertEqual(len(link_fakes.FakeLink.created), 1)  # the one link, never closed
        self.assertEqual(link.status, link_fakes.FakeLink.ACTIVE)
        response.cancel.assert_not_called()


class LossyLinkTest(unittest.TestCase):
    """1 MiB fetches from one provider over the real PeerClient and LinkPool (RNS faked as in test_links), on a path
    losing 5% of packets. RNS resends neither a request nor a link's request or proof: a lost request times out, a
    lost link request or proof leaves the link unestablished. Before 0.2.0 the first lost request failed the only
    provider, and with it more than half of these fetches (16 chunks and the root: 1 - 0.95^17); now a lost request
    is retried on a new link and the provider fails only after max_failures in a row."""

    LOSS = 0.05
    TRIALS = 30

    def setUp(self):
        """A fetcher using a PeerClient whose links are FakeLinks; P1 serves whatever it is asked for, but each
        request, and each link's request and proof, is lost with probability LOSS (a seeded draw, one at a time:
        with one provider, one request is in flight at a time, so the draws come in the same order every run)."""
        patcher = mock.patch.object(links, "RNS", link_fakes.fake_rns())
        patcher.start()
        self.addCleanup(patcher.stop)
        link_fakes.FakeLink.created = []
        link_fakes.FakeLink.establish_delay = 0.0
        self.random = random.Random(5)
        self.lock = threading.Lock()
        self.network: dict[Cid, bytes] = {}

        def lost() -> bool:
            """One packet's fate."""
            with self.lock:
                return self.random.random() < self.LOSS

        def respond(link, path, data):
            """Serve every CID asked for, unless the request is lost: RNS then times it out."""
            if lost():
                return link_fakes.FAIL
            return [self.network.get(Cid.from_bytes(binary)) for binary in data[1]]
        link_fakes.FakeLink.responder = staticmethod(respond)
        link_fakes.FakeLink.establishes = staticmethod(lambda link: not lost() and not lost())  # request, proof
        self.addCleanup(setattr, link_fakes.FakeLink, "establishes", None)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        config = Config(rnfs_dir=tmp.name, slow="no")
        self.storage = Storage(os.path.join(tmp.name, "blocks"), os.path.join(tmp.name, "state"), 10**9)
        self.addCleanup(self.storage.close)
        self.pool = links.LinkPool(object(), config)
        self.addCleanup(self.pool.close_all)
        self.fetcher = Fetcher(self.storage, client_module.PeerClient(self.pool, config), config, OWN,
                               RoutingTable(OWN, config), lambda key: FakeLookup([[P1]]), busy_delay=0.01)
        self.addCleanup(self.fetcher.close)

    def test_a_lossy_link_fetches_a_mebibyte_from_one_provider(self):
        failed = 0
        for _ in range(self.TRIALS):
            root = builder.build_bytes(os.urandom(1024 * 1024), self.network.__setitem__).cid
            try:
                with self.fetcher.open(root) as fetch:
                    fetch.fetch_dag(fetch.resolve())
            except NotFound:
                failed += 1
                continue
            self.assertTrue(self.storage.is_complete(root))
        # About 2% fail now (three losses in a row, each retry on a new link that may itself not be established);
        # 56-58% did
        self.assertLessEqual(failed, 2, f"{failed} of {self.TRIALS} fetches failed")
        self.assertGreater(len(link_fakes.FakeLink.created), 1, "nothing was lost")


class WalkTest(FetcherTest):
    """fetch_dag and fetch_listing queue a block's children as soon as it is met (A4), keeping parents before their
    children, so an interrupted fetch resumes from what it stored."""

    def setUp(self):
        """A directory of three subdirectories of four small files each; each request carries one DAG node (as
        if each were a full block), so the subdirectories go to three providers."""
        super().setUp()
        patcher = mock.patch.object(fetcher_module, "DAG_NODE_GUESS", CHUNK_SIZE)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.subdirs, self.files = [], []
        for i in range(3):
            files = [self.file(os.urandom(100 + 10 * i + k)) for k in range(4)]
            self.files.append([built.cid for built in files])
            self.subdirs.append(builder.build_directory([Entry(f"f{k}", built.cid, built.size, 0)
                                                         for k, built in enumerate(files)], self.put))
        self.root = builder.build_directory([Entry(f"s{i}", sub.cid, sub.size, 2)
                                             for i, sub in enumerate(self.subdirs)], self.put)

    def test_children_are_asked_for_before_the_level_ends(self):
        asked_below = threading.Event()
        seen = []
        early = {cid for files in self.files[:2] for cid in files}

        def answer(cids):
            """Hold the last subdirectory until files below the others are asked for (up to 3 s)."""
            if early.intersection(cids):
                asked_below.set()
            if self.subdirs[2].cid in cids:
                seen.append(asked_below.wait(3))
            return None
        for node_id in (P1, P2, P3):
            self.hold(node_id)
            self.client.behaviour[node_id] = answer
        self.runs.append([P1, P2, P3])
        self.fetch_all(self.root.cid)
        self.assertTrue(self.storage.is_complete(self.root.cid))
        self.assertEqual(seen, [True], "nothing below the level was asked for while its last block was held")
        asked = [cid for node_id in (P1, P2, P3) for cid in self.client.asked(node_id)]
        self.assertEqual(sorted(asked), sorted(self.network))  # each block once

    def test_interrupted_walk_stores_no_child_without_its_parent(self):
        self.hold(P1, [cid for cid in self.network if cid != self.subdirs[1].cid])
        self.runs.append([P1])
        with self.assertRaises(NotFound):  # P1 fails once it answers None for the second subdirectory
            self.fetch_all(self.root.cid)
        self.assertFalse(any(self.storage.has(cid) for cid in self.files[1]))
        missing = [cid for cid in self.network if not self.storage.has(cid)]
        self.hold(P2)
        self.runs[:] = [[P2]]
        self.fetch_all(self.root.cid)
        self.assertTrue(self.storage.is_complete(self.root.cid))
        self.assertEqual(sorted(self.client.asked(P2)), sorted(missing))  # resumed from what it stored

    def test_listing_fetches_index_nodes_as_they_are_met(self):
        with mock.patch("rnfs.constants.MAX_CHILDREN", 4):
            empty = self.file(b"")
            root = builder.build_directory([Entry(f"{i:04d}", empty.cid, 0, 0) for i in range(100)], self.put)
        self.hold(P1)
        self.hold(P2)
        self.runs.append([P1, P2])
        with self.fetcher.open(root.cid) as fetch:
            fetch.fetch_listing(fetch.resolve())
            entries = list(walker.list_directory(fetch.target, fetch.get, 1000))
        self.assertEqual([entry.name for entry in entries], [f"{i:04d}" for i in range(100)])
        asked = self.client.asked(P1) + self.client.asked(P2)
        self.assertEqual(len(asked), len(set(asked)))
        self.assertNotIn(empty.cid, asked)  # the entries' contents aren't fetched


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
        release = threading.Event()  # set as the test ends, so its request thread doesn't outlive it
        self.addCleanup(release.set)
        self.client.behaviour[P1] = lambda cids: (release.wait(5), BlocksResult(FAILED, unsent=list(cids)))[1]
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

    def test_unpinned_fetch_makes_room_by_disk_allocation(self):
        # On FAT32 each block takes a 32 KiB cluster: cache that is little in bytes but much on disk makes room
        self.storage.allocation_unit = 32 * 1024  # nothing is stored yet, so there's no total to redo
        small = [builder.build_bytes(b"%d" % i * 10, self.storage.put) for i in range(20)]
        cached = builder.build_directory([Entry(f"{i:02d}", f.cid, f.size, 0) for i, f in enumerate(small)],
                                         self.storage.put)
        self.storage.record_fetch(cached.cid, [], cached.cid)
        self.assertLess(self.storage.used_bytes(), 2000)
        self.storage.max_storage = 25 * 32 * 1024  # the cache takes 21 clusters, the file 9 more
        built = self.file(os.urandom(4 * CHUNK_SIZE))
        self.hold(P1)
        self.runs.append([P1])
        self.fetch_all(built.cid)
        self.assertTrue(self.storage.is_complete(built.cid))
        self.assertFalse(self.storage.has(cached.cid))
        self.assertLess(self.storage.used_bytes() + 2000, self.storage.max_storage)  # bytes alone would have fit
        self.assertLessEqual(self.storage.allocated_bytes(), self.storage.max_storage)

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
