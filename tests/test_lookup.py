"""Tests for iterative DHT lookups over simulated in-memory networks of RoutingTable/ProviderStore nodes."""
import random
import threading
import time
import unittest
from unittest import mock

from rnfs.config import Config
from rnfs.constants import ID_LENGTH, K
from rnfs.dht.lookup import FIRST, MAX_QUERIES, SLOW_PATH, STALL_WINDOWS, UNRELIABLE, Lookup
from rnfs.dht.providers import ProviderStore
from rnfs.dht.routing import AVOID_AFTER, RoutingTable, distance
from tests.support import FakeClock, wait_for

CONFIG = Config()


def ident(n: int) -> bytes:
    """The 16-byte ID whose big-endian value is n."""
    return n.to_bytes(ID_LENGTH, "big")


class FakeNode:
    """A DHT server: its own routing table and provider store, answering /find as section 6 says."""

    def __init__(self, node_id: bytes, clock):
        """Empty server node_id."""
        self.node_id = node_id
        self.table = RoutingTable(node_id, CONFIG, clock=clock)
        self.store = ProviderStore(None, clock=clock, rng=random.Random(node_id))

    def find(self, key: bytes) -> tuple[list[bytes], list[bytes]]:
        """The /find reply: providers, then the servers it knows closest to key."""
        return self.store.for_reply(key), self.table.closest(key, K, for_reply=True)


class FakeNetwork:
    """In-memory nodes reachable by ID. IDs not in `nodes` are unreachable.

    `liars` answer with no providers and only other liars as nodes; `hang` nodes block until released.
    """

    def __init__(self, count: int = 0, seed: int = 0):
        """count servers with random IDs, each having heard every other (full buckets keep the first K)."""
        self.rng = random.Random(seed)
        self.now = 1_700_000_000.0
        self.nodes: dict[bytes, FakeNode] = {}
        self.queried: list[bytes] = []
        self.liars: set[bytes] = set()
        self.hang: set[bytes] = set()
        self.release = threading.Event()
        self.lock = threading.Lock()
        ids = [self.rng.randbytes(ID_LENGTH) for _ in range(count)]
        for n in ids:
            self.add(n)
        for n in ids:
            order = ids[:]
            self.rng.shuffle(order)
            for other in order:
                self.nodes[n].table.heard(other)

    def clock(self) -> float:
        """The network's shared fake wall clock."""
        return self.now

    def add(self, n: bytes, knows: tuple[bytes, ...] = ()) -> FakeNode:
        """Add server n that has heard the nodes in `knows`."""
        node = self.nodes[n] = FakeNode(n, self.clock)
        for other in knows:
            node.table.heard(other)
        return node

    def query(self, n: bytes, key: bytes):
        """/find sent to n: its reply, or None if unreachable."""
        with self.lock:
            self.queried.append(n)
        if n in self.hang:
            self.release.wait()
            return None
        node = self.nodes.get(n)
        if node is None:
            return None
        if n in self.liars:
            return [], sorted(self.liars - {n}, key=lambda m: distance(m, key))[:K]
        return node.find(key)

    def true_closest(self, key: bytes, count: int = K) -> list[bytes]:
        """The count reachable nodes closest to key, by brute force."""
        return sorted(self.nodes, key=lambda n: distance(n, key))[:count]

    def publish(self, key: bytes, publisher: bytes):
        """Store publisher as a provider of key at the K nodes closest to it."""
        for n in self.true_closest(key):
            self.nodes[n].store.store(key, publisher)


class LookupTestCase(unittest.TestCase):
    """Base: releases hung queries after each test, so no worker thread outlives it."""

    def setUp(self):
        """Nothing shared yet; tests build their own networks."""
        self.networks: list[FakeNetwork] = []

    def network(self, *args, **kwargs) -> FakeNetwork:
        """A FakeNetwork whose hung queries tearDown releases."""
        net = FakeNetwork(*args, **kwargs)
        self.networks.append(net)
        return net

    def tearDown(self):
        """Release hung queries."""
        for net in self.networks:
            net.release.set()

    def lookup(self, net: FakeNetwork, key: bytes, start: list[bytes], own_id: bytes = ident(0), **kwargs) -> Lookup:
        """A Lookup over net with short time budgets suited to tests."""
        kwargs.setdefault("timeout", 5)
        kwargs.setdefault("poll_interval", 0.05)
        return Lookup(key, start, net.query, own_id, **kwargs)


class ConvergenceTest(LookupTestCase):
    """Lookups over larger networks find the nodes closest to the key in few queries."""

    def test_find_closest_converges_in_200_nodes(self):
        net = self.network(200, seed=1)
        rng = random.Random(2)
        ids = list(net.nodes)
        for _ in range(20):
            key = rng.randbytes(ID_LENGTH)
            before = len(net.queried)
            result = self.lookup(net, key, rng.sample(ids, 3)).find_closest()
            truth = net.true_closest(key)
            self.assertEqual(result[:3], truth[:3])  # the beta closest are always exact
            self.assertGreaterEqual(len(set(result) & set(truth)), 8)
            self.assertEqual(result, sorted(result, key=lambda n: distance(n, key)))
            self.assertLess(len(net.queried) - before, 60)

    def test_converges_with_alpha_1(self):
        net = self.network(100, seed=3)
        key = random.Random(4).randbytes(ID_LENGTH)
        result = self.lookup(net, key, list(net.nodes)[:1], alpha=1).find_closest()
        self.assertEqual(result[:3], net.true_closest(key, 3))

    def test_publish_then_find_providers(self):
        net = self.network(150, seed=5)
        rng = random.Random(6)
        ids = list(net.nodes)
        key, publisher = rng.randbytes(ID_LENGTH), rng.randbytes(ID_LENGTH)
        closest = self.lookup(net, key, rng.sample(ids, 2), own_id=publisher).find_closest()
        for n in closest:
            net.nodes[n].store.store(key, publisher)

        found = []
        result = self.lookup(net, key, rng.sample(ids, 2)).find_providers(found.append)
        self.assertEqual(found, [publisher])
        self.assertEqual(result, [publisher])

    def test_bootstrap_join_fills_routing_table(self):
        net = self.network(100, seed=7)
        own = random.Random(8).randbytes(ID_LENGTH)
        table = RoutingTable(own, CONFIG, clock=net.clock)
        bootstrap = list(net.nodes)[:1]
        self.lookup(net, own, bootstrap, own_id=own, on_answer=table.heard, on_failure=table.failed).find_closest()
        # Only nodes that answered are added: at least the beta closest, which are exactly the true ones
        self.assertGreaterEqual(table.node_count(), 3)
        for n in net.true_closest(own, 3):
            self.assertIn(n, table.nodes())


class TerminationTest(LookupTestCase):
    """beta termination, and termination when nothing new turns up."""

    def test_complete_once_beta_closest_answered(self):
        net = self.network()
        key = ident(0x1000)
        near = [ident(0x1001), ident(0x1002), ident(0x1003)]
        far = [ident(0x8000 + i) for i in range(20)]
        for n in near:
            net.add(n, knows=far)  # they only point further away
        for n in far:
            net.add(n)
        result = self.lookup(net, key, near).find_closest()
        self.assertEqual(sorted(net.queried), near)  # far nodes never queried
        self.assertEqual(result[:3], near)

    def test_closer_node_found_later_must_answer(self):
        net = self.network()
        key = ident(0x1000)
        start = [ident(0x1100), ident(0x1200), ident(0x1300)]
        closer = ident(0x1001)
        net.add(start[0], knows=[closer])
        net.add(start[1])
        net.add(start[2])
        net.add(closer)
        result = self.lookup(net, key, start).find_closest()
        self.assertIn(closer, net.queried)
        self.assertEqual(result[0], closer)

    def test_ends_when_every_known_node_queried(self):
        net = self.network()
        a, b = ident(1), ident(2)
        net.add(a, knows=[b])
        net.add(b, knows=[a])
        result = self.lookup(net, ident(100), [a]).find_closest()
        self.assertEqual(sorted(result), [a, b])
        self.assertEqual(sorted(net.queried), [a, b])  # each queried once

    def test_reports_each_query_and_how_the_run_ended(self):
        net = self.network()
        a, b, gone = ident(1), ident(2), ident(3)
        net.add(a, knows=[b, gone])
        net.add(b, knows=[a])
        reports: list[str] = []
        lock = threading.Lock()

        def report(text):
            """Collect reports (query threads report too)."""
            with lock:
                reports.append(text)

        self.lookup(net, ident(100), [a], report=report).find_closest()
        key = ident(100).hex()
        self.assertTrue(reports[0].startswith(f"lookup for {key}: 1 start nodes"), reports[0])
        self.assertTrue(any(f"<{a.hex()}> answered in" in text for text in reports), reports)
        self.assertTrue(any(f"<{gone.hex()}> failed after" in text for text in reports), reports)
        self.assertIn("asked 3, 2 answered, 1 failed", reports[-1])

    def test_no_start_nodes(self):
        net = self.network()
        self.assertEqual(self.lookup(net, ident(5), []).find_closest(), [])
        self.assertEqual(self.lookup(net, ident(5), []).find_providers(lambda p: None), [])

    def test_never_queries_itself(self):
        net = self.network()
        own, other = ident(7), ident(1)
        net.add(other, knows=[own])
        net.add(own)
        self.lookup(net, ident(100), [other, own], own_id=own).find_closest()
        self.assertNotIn(own, net.queried)

    def test_slow_far_node_does_not_delay_completion(self):
        net = self.network()
        key = ident(0x1000)
        near = [ident(0x1001), ident(0x1002), ident(0x1003)]
        slow = ident(0x9000)
        for n in near:
            net.add(n)
        net.add(slow)
        net.hang.add(slow)
        started = time.monotonic()
        # alpha 4 so the slow node is queried in the first round alongside the near ones
        result = self.lookup(net, key, near + [slow], alpha=4, timeout=30).find_closest()
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(result, near)


class FailureTest(LookupTestCase):
    """Nodes that don't answer are dropped from the lookup and reported."""

    def test_unreachable_dropped_and_reported(self):
        net = self.network()
        alive, dead = ident(1), ident(2)
        net.add(alive, knows=[dead])
        answered, failed = [], []
        result = self.lookup(net, ident(3), [alive], on_answer=answered.append,
                             on_failure=failed.append).find_closest()
        self.assertEqual(result, [alive])
        self.assertEqual(answered, [alive])
        self.assertEqual(failed, [dead])

    def test_exceptions_and_malformed_replies_count_as_failures(self):
        good, broken, garbage = ident(1), ident(2), ident(3)

        def query(n, key):
            """broken raises, garbage replies with something that isn't a /find reply."""
            if n == broken:
                raise RuntimeError("boom")
            if n == garbage:
                return "nonsense"
            return [b"bad id", ident(50)], [b"x" * 3, ident(1)]

        failed, found = [], []
        lookup = Lookup(ident(9), [good, broken, garbage], query, ident(0), on_failure=failed.append,
                        poll_interval=0.05)
        self.assertEqual(lookup.find_providers(found.append), [ident(50)])
        self.assertEqual(sorted(failed), [broken, garbage])
        self.assertEqual(lookup.closest_answered(), [good])

    def test_failures_drop_nodes_from_routing_table(self):
        net = self.network()
        own = ident(0)
        table = RoutingTable(own, Config(max_failures=1), clock=net.clock)
        dead = ident(5)
        table.heard(dead)
        self.lookup(net, ident(6), table.closest(ident(6)), own_id=own, on_failure=table.failed).find_closest()
        # Reported from the worker thread; it has finished by the time the lookup saw the failure
        self.assertFalse(table.contains(dead))

    def test_excluded_nodes_not_queried(self):
        net = self.network()
        a, banned = ident(1), ident(2)
        net.add(a, knows=[banned])
        net.add(banned)
        self.lookup(net, ident(3), [a], exclude=lambda n: n == banned).find_closest()
        self.assertEqual(net.queried, [a])


class UsefulAnswerTest(LookupTestCase):
    """on_useful (local scores): an answer naming a node closer to the key that then answers proved useful."""

    def test_answers_leading_closer_are_useful_once_per_lookup(self):
        net = self.network()
        key = ident(0x100)
        far, mid, near, made_up = ident(0xF000), ident(0x1F00), ident(0x101), ident(0x102)
        net.add(far, knows=[mid, made_up])
        net.add(mid, knows=[near, far])
        net.add(near)
        useful = []
        lookup = self.lookup(net, key, [far], own_id=ident(1), on_useful=useful.append, alpha=1)
        lookup.find_closest()
        # far named mid and made_up: mid answered and is closer, so far counts (once, though it named two);
        # mid named near, which answered. made_up never answered, and near named nobody
        self.assertEqual(sorted(useful), sorted([far, mid]))
        lookup.find_closest()  # a second run of the same lookup credits nobody again
        self.assertEqual(sorted(useful), sorted([far, mid]))

    def test_naming_only_farther_nodes_is_not_useful(self):
        net = self.network()
        key = ident(0x100)
        near, farther = ident(0x101), ident(0xF000)
        net.add(near, knows=[farther])
        net.add(farther)
        useful = []
        self.lookup(net, key, [near], own_id=ident(1), on_useful=useful.append).find_closest()
        self.assertEqual(useful, [])

    def test_a_failing_callback_does_not_end_the_lookup(self):
        net = self.network()
        key = ident(0x100)
        far, near = ident(0xF000), ident(0x101)
        net.add(far, knows=[near])
        net.add(near)

        def broken(node_id: bytes):
            """A callback with a bug."""
            raise RuntimeError("boom")

        with mock.patch("rnfs.log.log") as log:
            result = self.lookup(net, key, [far], own_id=ident(1), on_useful=broken).find_closest()
        self.assertEqual(result, [near, far])
        self.assertIn("RuntimeError('boom')", log.call_args.args[0])


class UnknownIdTest(LookupTestCase):
    """IDs whose identity isn't known cost a path request: at most alpha are tried per lookup."""

    def test_at_most_alpha_unknown_ids_tried(self):
        net = self.network()
        honest = ident(1)
        fakes = [ident(0x100 + i) for i in range(20)]  # mentioned in a reply, never announced
        net.add(honest, knows=[])

        def query(n, key):
            """honest lists 10 fake IDs; fakes are unreachable."""
            net.queried.append(n)
            return ([], fakes[:K]) if n == honest else None

        lookup = Lookup(ident(0x100), [honest], query, ident(0), alpha=3,
                        is_known=lambda n: n not in fakes, poll_interval=0.05)
        lookup.find_closest()
        tried = [n for n in net.queried if n in fakes]
        self.assertEqual(len(tried), 3)

    def test_budget_larger_than_alpha(self):
        """unknowns sets the budget (slow nodes' publishing and provider lookups), never below alpha."""
        fakes = [ident(0x100 + i) for i in range(20)]

        def query(n, key):
            """ident(1) lists 10 fake IDs; fakes are unreachable."""
            tried.append(n)
            return ([], fakes[:K]) if n == ident(1) else None

        for alpha, unknowns, expected in ((1, 10, 10), (1, None, 1), (3, 2, 3)):
            tried: list[bytes] = []
            lookup = Lookup(ident(0x100), [ident(1)], query, ident(0), alpha=alpha, unknowns=unknowns,
                            is_known=lambda n: n not in fakes, poll_interval=0.05)
            lookup.find_closest()
            self.assertEqual(len([n for n in tried if n in fakes]), expected, (alpha, unknowns))

    def test_named_tells_who_named_each_unknown_id_queried(self):
        """named(id, by) before a query costs a path request: the node scopes it to the path to by (paths.named)."""
        first, second, known_one = ident(1), ident(2), ident(3)
        fakes = [ident(0x100 + i) for i in range(4)]
        replies = {first: [second, fakes[0], known_one], second: fakes[1:]}

        def query(n, key):
            """first names second, a fake and a known node; second names the other fakes; fakes don't answer."""
            return ([], replies[n]) if n in replies else (([], []) if n == known_one else None)

        told: list[tuple[bytes, bytes]] = []
        lookup = Lookup(ident(0x100), [first], query, ident(0), alpha=1, unknowns=10, poll_interval=0.05,
                        is_known=lambda n: n in (first, known_one), named=lambda n, by: told.append((n, by)))
        lookup.find_closest()
        # Unknown IDs only, each with the node whose answer first named it; the known node isn't told
        self.assertEqual(dict(told), {second: first, fakes[0]: first, **{n: second for n in fakes[1:]}})
        self.assertEqual(len(told), len(dict(told)))
        # A callback that raises doesn't end the lookup
        lookup = Lookup(ident(0x100), [first], query, ident(0), alpha=1, unknowns=10, poll_interval=0.05,
                        is_known=lambda n: n in (first, known_one), named=lambda n, by: 1 / 0)
        with mock.patch("rnfs.log.log"):
            self.assertIn(known_one, lookup.find_closest())

    def test_budget_shared_with_providers(self):
        lookup = Lookup(ident(1), [], lambda n, k: None, ident(0), alpha=2, is_known=lambda n: n == ident(9))
        self.assertTrue(lookup.allow_unknown(ident(9)))  # known: free
        self.assertTrue(lookup.allow_unknown(ident(5)))
        self.assertTrue(lookup.allow_unknown(ident(5)))  # charged once
        self.assertTrue(lookup.allow_unknown(ident(6)))
        self.assertFalse(lookup.allow_unknown(ident(7)))
        self.assertTrue(lookup.allow_unknown(ident(9)))


class PatienceTest(LookupTestCase):
    """A query unanswered past patience stops holding an alpha place and ranks with the unreliable: over LoRa two
    servers RNS had long paths to held a search 6 minutes, each for its link's whole establishment timeout."""

    def setUp(self):
        """The two servers closest to the key never answer; three more beyond them do."""
        super().setUp()
        self.net = self.network()
        self.key = ident(0x1000)
        self.dead = [ident(0x1001), ident(0x1002)]
        self.alive = [ident(0x1010), ident(0x1020), ident(0x1030)]
        everyone = self.dead + self.alive
        for n in everyone:
            self.net.add(n, knows=[m for m in everyone if m != n])
        self.net.hang.update(self.dead)

    def run_search(self, patience):
        """find_providers from every server, alpha 3, with patience; (how it ended, seconds it took)."""
        lookup = self.lookup(self.net, self.key, self.dead + self.alive, timeout=3, alpha=3, beta=3,
                             patience=patience)
        started = time.monotonic()
        lookup.find_providers(lambda provider: None)
        return lookup.ended, time.monotonic() - started

    def test_servers_that_dont_answer_dont_hold_a_search(self):
        """It asks the others meanwhile, and once nobody holds a record, waits for them only HOLD_PATIENCE times
        patience, not its whole time."""
        ended, took = self.run_search(0.2)
        self.assertEqual(ended, "complete")
        self.assertLess(took, 1.5)
        self.assertTrue(set(self.alive) <= set(self.net.queried))

    def test_a_search_that_found_providers_waits_for_nobody_slow(self):
        self.net.nodes[self.alive[0]].store.store(self.key, ident(0x7777))
        ended, took = self.run_search(0.2)
        self.assertEqual(ended, "complete")
        self.assertLess(took, 0.6)  # within HOLD_PATIENCE x patience: it didn't wait for them

    def test_without_patience_it_waits_for_them(self):
        ended, took = self.run_search(None)
        self.assertEqual(ended, "out of time")
        self.assertGreaterEqual(took, 2.9)

    def test_finding_the_closest_waits_for_them(self):
        """For publishing, where records are stored matters more than how soon: patience doesn't apply."""
        lookup = self.lookup(self.net, self.key, self.dead + self.alive, timeout=1, patience=0.2)
        lookup.find_closest()
        self.assertEqual(lookup.ended, "out of time")

    def test_an_answer_after_patience_still_gives_its_providers(self):
        """A server slow to answer, not dead, holding the only record: a search that has found nobody waits for it
        before it ends, so its provider still comes."""
        self.net.hang.clear()
        slow, gate = self.dead[0], threading.Event()
        self.net.nodes[slow].store.store(self.key, ident(0x7777))
        query = self.net.query

        def slowly(node_id, key):
            if node_id == slow:
                gate.wait(5)
            return query(node_id, key)
        lookup = Lookup(self.key, self.dead + self.alive, slowly, ident(0), timeout=5, poll_interval=0.05,
                        patience=0.2)
        threading.Timer(0.6, gate.set).start()
        started = time.monotonic()
        self.assertEqual(lookup.find_providers(lambda provider: None), [ident(0x7777)])
        self.assertLess(time.monotonic() - started, 3)
        self.assertIn(lookup.ended, ("complete", "no node left to ask"))


class UnreliableNodeTest(LookupTestCase):
    """Nodes whose queries keep failing are asked last, and a retry of one is never waited for."""

    def setUp(self):
        """A key with two unreliable nodes closest to it, which hang, and three reliable ones beyond them."""
        super().setUp()
        self.net = self.network()
        self.key = ident(0x1000)
        self.dead = [ident(0x1001), ident(0x1002)]
        self.near = [ident(0x1010), ident(0x1020), ident(0x1030)]
        for n in self.dead + self.near:
            self.net.add(n)
        self.net.hang.update(self.dead)

    def test_not_asked_while_enough_others_answer(self):
        """The lookup completes from the reliable nodes without asking or waiting for the unreliable ones."""
        started = time.monotonic()
        result = self.lookup(self.net, self.key, self.dead + self.near, timeout=30,
                             unreliable=self.dead.__contains__).find_closest()
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(result, self.near + self.dead)  # not asked: after every other node, to make up k
        self.assertFalse(set(self.net.queried) & set(self.dead))

    def test_unreliable_nodes_named_in_replies_too(self):
        """Unreliable nodes a reply names rank last as well."""
        for n in self.near:
            self.net.nodes[n].table.heard(self.dead[0])
        started = time.monotonic()
        result = self.lookup(self.net, self.key, self.near, timeout=30,
                             unreliable=self.dead.__contains__).find_closest()
        self.assertLess(time.monotonic() - started, 2)
        self.assertNotIn(self.dead[0], self.net.queried)
        # Known but not asked: it comes after every other node in the result
        self.assertEqual(result, self.near + [self.dead[0]])

    def test_retry_is_sent_but_not_waited_for(self):
        """A due retry is asked on top of alpha, and the lookup finishes without its answer."""
        retries = []

        def may_retry(n: bytes) -> bool:
            """Every unreliable node is due, once."""
            retries.append(n)
            return True

        started = time.monotonic()
        lookup = self.lookup(self.net, self.key, self.dead + self.near, alpha=1, timeout=30,
                             unreliable=self.dead.__contains__, may_retry=may_retry)
        result = lookup.find_closest()
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual(result, self.near)
        self.assertEqual(sorted(retries), self.dead)
        wait_for(lambda: set(self.dead) <= set(self.net.queried), what="the retries")

    def test_not_retried_once_disallowed(self):
        """A due retry of a node the lookup may no longer query (partition mode began) isn't sent."""
        allowed = set(self.dead + self.near)
        retries = []

        def may_retry(n: bytes) -> bool:
            """Every unreliable node is due; partition mode begins as the first is asked."""
            retries.append(n)
            allowed.difference_update(self.dead)
            return True

        result = self.lookup(self.net, self.key, self.dead + self.near, alpha=1, unreliable=self.dead.__contains__,
                             may_retry=may_retry, allowed=allowed.__contains__).find_closest()
        self.assertEqual(result, self.near)
        self.assertEqual(len(retries), 1)  # the second was dropped before its retry was claimed

    def test_asked_and_waited_for_when_needed(self):
        """With too few other nodes, unreliable ones are asked and waited for like any node."""
        self.net.hang.clear()
        result = self.lookup(self.net, self.key, self.dead + self.near[:1], beta=3,
                             unreliable=self.dead.__contains__).find_closest()
        self.assertEqual(result, sorted(self.dead + self.near[:1], key=lambda n: distance(n, self.key)))
        self.assertEqual(set(self.net.queried), set(self.dead + self.near[:1]))


class SlowPathTest(LookupTestCase):
    """Servers behind a slow path (on a node that isn't slow: LoRa islands) are asked after every other but unreliable
    ones, and only when the lookup has too few others; ordering only."""

    def setUp(self):
        """A key with two slow-path servers closest to it, three servers on fast paths beyond them, and an unreliable
        one beyond those; every server knows all of them."""
        super().setUp()
        self.net = self.network()
        self.key = ident(0x1000)
        self.island = [ident(0x1001), ident(0x1002)]
        self.near = [ident(0x1010), ident(0x1020), ident(0x1030)]
        self.dead = ident(0x1040)
        everyone = self.island + self.near + [self.dead]
        for n in everyone:
            self.net.add(n, knows=[m for m in everyone if m != n])
        self.asked: list[bytes] = []

    def slow_path(self, node_id: bytes) -> bool:
        """The island's servers are behind a slow path; counts each question."""
        self.asked.append(node_id)
        return node_id in self.island

    def test_not_asked_while_enough_others_answer(self):
        lookup = self.lookup(self.net, self.key, self.near[:1], slow_path=self.slow_path,
                             unreliable={self.dead}.__contains__)
        result = lookup.find_closest()
        self.assertFalse(set(self.net.queried) & set(self.island))
        # After the others, before the unreliable one: stores reach them only to make up k
        self.assertEqual(result, self.near + self.island + [self.dead])
        self.assertEqual(lookup.candidates(), {**dict.fromkeys(self.near, FIRST), **dict.fromkeys(self.island, SLOW_PATH),
                                               self.dead: UNRELIABLE})
        # Asked once per node, however often replies name it
        self.assertEqual(sorted(self.asked), sorted(self.island + self.near + [self.dead]))

    def test_asked_when_too_few_others_and_first_once_answered(self):
        """With too few others they are asked and waited for like any node; once they answer they rank by distance
        with the rest."""
        net = self.network()
        nodes = self.island + self.near[:1]
        for n in nodes:
            net.add(n, knows=[m for m in nodes if m != n])
        lookup = self.lookup(net, self.key, self.near[:1], slow_path=self.slow_path)
        result = lookup.find_closest()
        self.assertEqual(set(net.queried), set(nodes))
        self.assertEqual(result, nodes)
        self.assertEqual(lookup.candidates()[self.island[0]], FIRST)
        self.assertEqual(lookup.ended, "complete")

    def test_providers_found_on_fast_paths_first(self):
        for n in self.near + self.island:
            self.net.nodes[n].store.store(self.key, ident(0x7777))
        lookup = self.lookup(self.net, self.key, self.island + self.near, slow_path=self.slow_path)
        self.assertEqual(lookup.find_providers(lambda p: None), [ident(0x7777)])
        self.assertFalse(set(self.net.queried) & set(self.island))
        asked = len(self.asked)
        lookup.find_providers(lambda p: None)  # a second run: slow_path isn't asked again
        self.assertEqual(len(self.asked), asked)

    def test_found_slow_once_its_path_is_known(self):
        """A server whose path was unknown when it was ranked is checked before it is asked: found slow, it is ranked
        again and not asked while others remain. Nothing is sent to it, and nothing counts against it."""
        late = ident(0x1003)  # ranked first: its path, and so its hops, are unknown until the query requests one
        self.net.add(late, knows=self.near)
        checked, failed = [], []

        def check_path(node_id: bytes) -> bool | None:
            """late's path request finds a slow path; the others are fine."""
            checked.append(node_id)
            return True if node_id == late else None

        lookup = self.lookup(self.net, self.key, [late] + self.near, slow_path=self.slow_path, check_path=check_path,
                             on_failure=failed.append)
        result = lookup.find_closest()
        self.assertNotIn(late, self.net.queried)
        self.assertEqual(sorted(checked), sorted([late] + self.near))  # each first-tier node asked, once
        self.assertEqual(result, self.near + [self.dead] + self.island + [late])
        self.assertEqual(lookup.candidates()[late], SLOW_PATH)
        self.assertEqual(failed, [])
        self.assertEqual(lookup.sent, len(self.net.queried))

    def test_asked_at_once_when_it_still_ranks_among_the_nodes_to_query(self):
        net = self.network()
        late = ident(0x1003)
        for n in (late, self.near[0]):
            net.add(n)
        checked = []

        def check_path(node_id: bytes) -> bool | None:
            """late's path turns out slow."""
            checked.append(node_id)
            return node_id == late or None

        lookup = self.lookup(net, self.key, [late, self.near[0]], check_path=check_path, alpha=1)
        result = lookup.find_closest()
        self.assertEqual(net.queried.count(late), 1)  # too few others: asked, without checking it again
        self.assertEqual(checked, [late, self.near[0]])
        self.assertEqual(result, [late, self.near[0]])  # it answered: first tier
        self.assertEqual(lookup.sent, 2)

    def test_no_path_is_a_failed_query(self):
        failed = []
        lookup = self.lookup(self.net, self.key, self.near, check_path=lambda n: False if n == self.near[0] else None,
                             on_failure=failed.append)
        lookup.find_closest()
        self.assertEqual(failed, [self.near[0]])
        self.assertNotIn(self.near[0], self.net.queried)

    def test_checked_only_in_the_first_tier(self):
        checked = []
        self.lookup(self.net, self.key, self.island + self.near[:1], slow_path=self.slow_path,
                    unreliable={self.near[0]}.__contains__, check_path=lambda n: checked.append(n)).find_closest()
        self.assertTrue(set(self.net.queried) & set(self.island))  # asked: too few others
        self.assertFalse(set(checked) & set(self.island + self.near[:1]))


class LocalFirstTest(LookupTestCase):
    """Nodes asked first (local servers) and nodes a lookup may not query (partition mode)."""

    def world(self):
        """A key, three servers near it, and a far local server that knows them and holds a provider."""
        net = self.network()
        key = ident(0x1000)
        near = [ident(0x1001), ident(0x1002), ident(0x1003)]
        local = ident(0x9000)
        for n in near:
            net.add(n)
        net.add(local, knows=near).store.store(key, ident(0x7777))
        return net, key, near, local

    def test_first_nodes_asked_before_any_other_and_seed_the_run(self):
        net, key, near, local = self.world()
        found = []
        lookup = self.lookup(net, key, [], alpha=1, first=[local])
        lookup.find_providers(found.append)
        self.assertEqual(net.queried[0], local)
        self.assertEqual(found, [ident(0x7777)])
        self.assertEqual(sorted(net.queried[1:]), near)  # its answer named them: the run went on from there

    def test_first_nodes_dropped_once_a_reply_lists_a_provider(self):
        net, key, near, local = self.world()
        other = ident(0xa000)
        net.add(other)
        lookup = self.lookup(net, key, near, alpha=1, first=[local, other])
        lookup.find_providers(lambda provider: None)
        self.assertEqual(net.queried[0], local)
        self.assertNotIn(other, net.queried)  # far from the key, and no longer asked first

    def test_only_the_first_run(self):
        net, key, near, local = self.world()
        lookup = self.lookup(net, key, near, first=[local])
        lookup.find_closest()
        net.queried.clear()
        lookup.find_providers(lambda provider: None)
        self.assertEqual(sorted(net.queried), near)

    def test_disallowed_nodes_never_queried_nor_in_the_result(self):
        net, key, near, local = self.world()
        allowed = {local, near[2]}
        result = self.lookup(net, key, near + [local], allowed=allowed.__contains__).find_closest()
        self.assertEqual(set(net.queried), allowed)
        self.assertEqual(set(result), allowed)

    def test_disallowed_mid_lookup_dropped_when_its_turn_comes(self):
        net, key, near, local = self.world()
        partitioned = threading.Event()

        def allowed(node_id: bytes) -> bool:
            """Everything until the first answer; then only local."""
            return not partitioned.is_set() or node_id == local

        lookup = self.lookup(net, key, [local] + near, alpha=1, allowed=allowed, first=[local],
                             on_answer=lambda node_id: partitioned.set())
        result = lookup.find_closest()
        self.assertEqual(net.queried, [local])
        self.assertEqual(result, [local])

    def test_providers_reported_whoever_they_are(self):
        net, key, near, local = self.world()
        found = []
        self.lookup(net, key, [local], allowed=lambda node_id: node_id == local).find_providers(found.append)
        self.assertEqual(found, [ident(0x7777)])


class UnreliableRoutingTest(LookupTestCase):
    """Lookups over a routing table: a far node that keeps announcing but never connects."""

    def test_avoided_retried_and_restored(self):
        """Asked by every lookup until max_failures, then avoided (announces don't change that), asked again
        after the backoff, and asked like any node once it answers."""
        net = self.network()
        clock = FakeClock()
        own = ident(0)
        table = RoutingTable(own, Config(max_failures=3), clock=clock)
        key = ident(0x1000)
        dead = ident(0x1001)  # closest to the key, and unreachable
        near = [ident(0x1010), ident(0x1020), ident(0x1030), ident(0x1040)]
        for n in near:
            net.add(n)
            table.heard(n)

        outcomes: list[bytes] = []  # every node whose query's outcome reached the table

        def count(record):
            """record (the table's failed or answered), noting the outcome."""
            return lambda n: (record(n), outcomes.append(n))

        def run() -> bool:
            """Hear dead's announce, run a lookup from the table, and say whether it asked dead."""
            table.heard(dead)
            before = net.queried.count(dead)
            lookup = self.lookup(net, key, table.closest(key), own_id=own, on_failure=count(table.failed),
                                 on_answer=count(table.answered), unreliable=table.unreliable,
                                 may_retry=table.may_retry)
            self.assertLessEqual(set(near[:3]), set(lookup.find_closest()))
            # The outcome of a retry may arrive after the run, which doesn't wait for it
            wait_for(lambda: outcomes.count(dead) == net.queried.count(dead), what="the query's outcome")
            return net.queried.count(dead) > before

        self.assertEqual([run() for _ in range(3)], [True, True, True])
        self.assertTrue(table.unreliable(dead))
        self.assertFalse(run())
        clock.now += AVOID_AFTER
        self.assertTrue(run())  # retried, failing again
        self.assertFalse(run())
        net.add(dead)  # back
        clock.now += 2 * AVOID_AFTER
        self.assertTrue(run())
        wait_for(lambda: not table.unreliable(dead), what="the answer")
        self.assertTrue(run())


class TimeoutTest(LookupTestCase):
    """The time budget bounds a lookup even when every query hangs."""

    def test_hung_queries_do_not_block_past_deadline(self):
        net = self.network()
        nodes = [ident(i) for i in range(1, 6)]
        for n in nodes:
            net.add(n)
            net.hang.add(n)
        started = time.monotonic()
        result = self.lookup(net, ident(100), nodes, timeout=0.2).find_closest()
        self.assertLess(time.monotonic() - started, 1)
        # Queried nodes that never answered are left out; the rest are known but untried
        self.assertEqual(len(net.queried), 3)
        self.assertEqual(result, [n for n in nodes if n not in net.queried])

    def test_partial_result_at_timeout(self):
        net = self.network()
        answering, hung = ident(1), ident(0x10)
        net.add(answering, knows=[hung])
        net.add(hung)
        net.hang.add(hung)
        result = self.lookup(net, ident(0x10), [answering], timeout=0.2).find_closest()
        self.assertEqual(result, [answering])

    def test_rerun_shares_the_deadline(self):
        clock = [0.0]
        lookup = Lookup(ident(1), [ident(2)], lambda n, k: ([], []), ident(0), timeout=10,
                        clock=lambda: clock[0])
        self.assertEqual(lookup.find_closest(), [ident(2)])
        self.assertEqual(lookup.remaining(), 10)
        clock[0] = 11
        calls = []
        lookup.query = lambda n, k: calls.append(n)
        lookup.find_providers(lambda p: None)
        self.assertEqual(calls, [])  # budget spent: nothing sent
        self.assertEqual(lookup.remaining(), 0)


class CandidatesTest(LookupTestCase):
    """candidates() and ask(): what sweep providing reads off a finished lookup, and its extra /find."""

    def test_candidates_are_what_find_closest_chooses_from(self):
        net = self.network(200, seed=31)
        rng = random.Random(32)
        ids = list(net.nodes)
        for _ in range(10):
            key = rng.randbytes(ID_LENGTH)
            lookup = self.lookup(net, key, rng.sample(ids, 3))
            result = lookup.find_closest()
            candidates = lookup.candidates()
            self.assertGreater(len(candidates), K)
            self.assertLessEqual(set(result), set(candidates))
            # The K closest of the candidates are at least as close as find_closest's (answers that came after the
            # run ended can only add nodes)
            ranked = sorted(candidates, key=lambda n: distance(n, key))[:K]
            self.assertLessEqual([distance(n, key) for n in ranked], [distance(n, key) for n in result])
            self.assertFalse(any(candidates.values()))  # none unreliable

    def test_answers_after_the_run_ended_are_candidates(self):
        """A run completes with a query still out: find_closest leaves that node out, but once its answer comes,
        candidates() has it and the nodes it named."""
        key = ident(0x1000)
        fast = [ident(0x1001), ident(0x1002), ident(0x1003)]
        slow, named = ident(0x1100), ident(0x1004)
        answer = threading.Event()

        def query(node_id, about):
            """The fast nodes name nothing new; the slow one answers once told to, naming another."""
            if node_id == slow:
                answer.wait(5)
                return [], [named]
            return [], fast

        lookup = Lookup(key, fast + [slow], query, ident(0), alpha=4, timeout=5, poll_interval=0.05)
        self.assertEqual(lookup.find_closest(), fast)
        self.assertEqual(set(lookup.candidates()), set(fast))
        answer.set()
        wait_for(lambda: slow in lookup.candidates(), what="the late answer")
        self.assertEqual(lookup.candidates(), {n: False for n in fast + [slow, named]})

    def test_candidates_leave_out_failed_and_unanswered_nodes(self):
        """Hung or failed queries aren't candidates; unreliable nodes not asked are, ranked last."""
        net = self.network()
        key = ident(0x1000)
        dead, hung = ident(0x1001), ident(0x1002)  # unreachable; hangs
        near = [ident(0x1010), ident(0x1020), ident(0x1030)]
        for n in [hung] + near:
            net.add(n)
        net.hang.add(hung)
        far = ident(0x8000)  # unreliable and never asked: there are enough others
        # The lookup waits out the hung node: long enough that the near nodes' answers, on threads, are in by then
        lookup = self.lookup(net, key, [dead, hung, far] + near, timeout=2, unreliable={far}.__contains__)
        lookup.find_closest()
        self.assertEqual(lookup.candidates(), {**{n: FIRST for n in near}, far: UNRELIABLE})

    def test_ask_merges_the_named_nodes(self):
        """ask() sends one /find about another key: its answer is counted and the nodes it names become candidates,
        returned as named (at most K)."""
        net = self.network(300, seed=33)
        rng = random.Random(34)
        ids = list(net.nodes)
        key = rng.randbytes(ID_LENGTH)
        answered, failed = [], []
        lookup = self.lookup(net, key, rng.sample(ids, 3), on_answer=answered.append, on_failure=failed.append)
        lookup.find_closest()
        before = set(lookup.candidates())
        other = bytes(b ^ 0xFF for b in key)  # the far side of the key space
        asked = min(before, key=lambda n: distance(n, other))
        named = lookup.ask(asked, other)
        self.assertEqual(named, net.nodes[asked].find(other)[1])
        self.assertLessEqual(len(named), K)
        self.assertEqual(net.queried[-1], asked)
        self.assertEqual(answered[-1], asked)
        self.assertLessEqual(set(named) | before, set(lookup.candidates()))
        self.assertEqual(failed, [])

    def test_ask_of_a_node_that_fails(self):
        """A node that doesn't answer gives None, is counted as failed and is no longer a candidate."""
        net = self.network()
        key = ident(0x1000)
        near = [ident(0x1010), ident(0x1020)]
        for n in near:
            net.add(n)
        failed = []
        lookup = self.lookup(net, key, near, on_failure=failed.append)
        lookup.find_closest()
        del net.nodes[near[0]]  # gone since
        self.assertIsNone(lookup.ask(near[0], ident(0x2000)))
        self.assertEqual(failed, [near[0]])
        self.assertEqual(lookup.candidates(), {near[1]: False})
        self.assertIsNone(lookup.ask(near[0], ident(0x2000)))  # failed: not asked again
        self.assertEqual(net.queried.count(near[0]), 2)

    def test_ask_within_the_deadline(self):
        """A hung node gives None once the lookup's time budget is spent."""
        net = self.network()
        answering, hung = ident(1), ident(2)
        net.add(answering)
        net.add(hung)
        lookup = self.lookup(net, ident(3), [answering, hung], timeout=0.3)
        lookup.find_closest()
        net.hang.add(hung)
        started = time.monotonic()
        self.assertIsNone(lookup.ask(hung, ident(4)))
        self.assertLess(time.monotonic() - started, 1)
        asked = len(net.queried)
        self.assertIsNone(lookup.ask(answering, ident(4)))  # the budget is spent: nothing is sent
        self.assertEqual(len(net.queried), asked)


class FindProvidersTest(LookupTestCase):
    """Provider search: reports as found, doesn't stop early, resists hiding, stops when told."""

    def setUp(self):
        """A 100-node network and a key."""
        super().setUp()
        self.net = self.network(100, seed=11)
        self.key = random.Random(12).randbytes(ID_LENGTH)
        self.closest = self.net.true_closest(self.key)
        self.start = [n for n in self.net.nodes if n not in self.closest][:3]

    def test_does_not_stop_at_first_reply_listing_providers(self):
        # Each provider is recorded at a different near node, so no single reply lists them all
        providers = [ident(0x500 + i) for i in range(3)]
        for node, provider in zip(self.closest[:3], providers):
            self.net.nodes[node].store.store(self.key, provider)
        found = []
        self.lookup(self.net, self.key, self.start).find_providers(found.append)
        self.assertEqual(sorted(found), providers)

    def test_a_provider_answered_after_the_run_completed_is_late(self):
        """A query still out when the run completes (its node no longer among the beta closest) can still list a
        provider: late_providers reports it, once, with its lister; not this node, nor one excluded."""
        net = self.network()
        key = ident(0x1000)
        near = [ident(0x1001), ident(0x1002), ident(0x1003)]
        start, straggler = ident(0x9000), ident(0x9001)
        for n in near:
            net.add(n, knows=near)
        net.add(start, knows=near)
        net.add(straggler)
        for provider in (ident(0x7777), ident(0), ident(0x6666)):
            net.nodes[straggler].store.store(key, provider)
        release = threading.Event()

        def query(node_id, about):
            if node_id == straggler:
                release.wait(5)
            return net.query(node_id, about)
        lookup = Lookup(key, [start, straggler], query, ident(0), timeout=5, poll_interval=0.05, alpha=3, beta=3,
                        exclude={ident(0x6666)}.__contains__)
        self.assertEqual(lookup.find_providers(lambda p: None), [])
        self.assertEqual(lookup.ended, "complete")
        self.assertEqual(lookup.late_providers(), [])  # nothing came late yet
        release.set()
        late = []
        wait_for(lambda: late.extend(lookup.late_providers()) or late, what="the straggler's late answer")
        self.assertEqual(late, [ident(0x7777)])
        self.assertEqual(lookup.listers(ident(0x7777)), {straggler})
        self.assertEqual(lookup.late_providers(), [])  # reported once

    def test_on_provider_called_as_soon_as_found(self):
        provider = ident(0x500)
        self.net.publish(self.key, provider)
        events = []
        lookup = self.lookup(self.net, self.key, self.start)
        lookup.find_providers(lambda p: events.append(("provider", len(self.net.queried))))
        self.assertEqual(len(events), 1)
        self.assertLess(events[0][1], len(self.net.queried))  # reported before the lookup finished

    def test_closest_node_hiding_content_does_not_stop_search(self):
        provider = ident(0x500)
        self.net.publish(self.key, provider)
        liar = self.closest[0]
        self.net.liars.add(liar)
        self.net.nodes[liar].store.withdraw(self.key, provider)
        found = []
        self.lookup(self.net, self.key, self.start).find_providers(found.append)
        self.assertIn(liar, self.net.queried)
        self.assertEqual(found, [provider])

    def test_several_liars_near_key_do_not_hide_content(self):
        provider = ident(0x500)
        self.net.publish(self.key, provider)
        for liar in self.closest[:2]:  # fewer than beta
            self.net.liars.add(liar)
            self.net.nodes[liar].store.withdraw(self.key, provider)
        found = []
        self.lookup(self.net, self.key, self.start).find_providers(found.append)
        self.assertEqual(found, [provider])

    def test_should_stop_ends_search(self):
        for i in range(3):
            self.net.publish(self.key, ident(0x500 + i))
        found = []
        self.lookup(self.net, self.key, self.start).find_providers(found.append, should_stop=lambda: len(found) >= 1)
        self.assertGreaterEqual(len(found), 1)
        queries = len(self.net.queried)
        full = []
        self.lookup(self.net, self.key, self.start).find_providers(full.append)
        self.assertLess(queries, len(self.net.queried) - queries)  # stopped earlier than a full search

    def test_stop_wakes_a_waiting_search(self):
        hung = ident(0x7)
        self.net.add(hung)
        self.net.hang.add(hung)
        done = threading.Event()
        lookup = Lookup(self.key, [hung], self.net.query, ident(0), timeout=30, poll_interval=30)
        started = time.monotonic()

        def stop_soon():
            """Flip should_stop from another thread, as the fetcher does when the root arrives."""
            time.sleep(0.05)
            done.set()
            lookup.stop()

        threading.Thread(target=stop_soon).start()
        lookup.find_providers(lambda p: None, should_stop=done.is_set)
        self.assertLess(time.monotonic() - started, 2)

    def test_excluded_providers_not_reported(self):
        good, banned = ident(0x500), ident(0x501)
        self.net.publish(self.key, good)
        self.net.publish(self.key, banned)
        found = []
        self.lookup(self.net, self.key, self.start, exclude=lambda n: n == banned).find_providers(found.append)
        self.assertEqual(found, [good])

    def test_rerun_gives_fresh_random_half(self):
        many = [ident(0x1000 + i) for i in range(40)]
        holder = self.closest[0]
        for i, provider in enumerate(many):
            self.net.nodes[holder].store.store(self.key, provider)
            self.net.now += 1  # distinct first-stored times
        lookup = self.lookup(self.net, self.key, self.start)
        first = lookup.find_providers(lambda p: None)
        self.assertEqual(len(first), K)
        second = lookup.find_providers(lambda p: None)
        self.assertTrue(second)  # new providers from the random half
        self.assertFalse(set(first) & set(second))  # already reported ones aren't reported again
        self.assertIn(holder, self.net.queried[len(self.net.queried) - 20:])  # re-queried


class BoundsTest(LookupTestCase):
    """Nodes that keep answering with ever closer IDs (made up, or ground for the key) can't hold a run open: it
    ends at the query cap, or after STALL_WINDOWS windows of answers in a row that bring the key no nearer."""

    def grinding(self, next_distance) -> tuple[list[bytes], object]:
        """A query function for key ident(0), and the nodes it was asked: every node answers with K fresh IDs at
        distances next_distance(n) + 0..K-1, n counting the answers so far."""
        asked: list[bytes] = []
        lock = threading.Lock()

        def query(node_id, key):
            """A made-up answer, nearer the key as next_distance says."""
            with lock:
                asked.append(node_id)
                distance_ = next_distance(len(asked))
            return [], [ident(distance_ + i) for i in range(K)]
        return asked, query

    def run_grinding(self, next_distance, start: int = 1 << 120, **kwargs) -> tuple[list[bytes], str]:
        """find_closest over grinding nodes from three start nodes at distance start; the nodes asked, and how the
        run ended."""
        asked, query = self.grinding(next_distance)
        reports: list[str] = []
        lookup = Lookup(ident(0), [ident(start + i) for i in range(3)], query, ident(1 << 127), timeout=30,
                        poll_interval=0.05, report=reports.append, **kwargs)
        lookup.find_closest()
        ends = [text for text in reports if "s; asked" in text]
        self.assertEqual(len(ends), 1, reports)
        return asked, ends[0]

    def test_ever_closer_answers_end_at_the_query_cap(self):
        asked, end = self.run_grinding(lambda n: 1 << max(8, 119 - n))  # each answer a bit nearer
        self.assertIn(": query cap after", end)
        self.assertEqual(len(asked), MAX_QUERIES)
        self.assertEqual(MAX_QUERIES, 8 * K)

    def test_query_cap_is_a_parameter(self):
        asked, end = self.run_grinding(lambda n: 1 << max(8, 119 - n), max_queries=10)
        self.assertIn(": query cap after", end)
        self.assertEqual(len(asked), 10)

    def test_answers_that_bring_the_key_no_nearer_end_the_run(self):
        """Each answer is nearer, but within the same power of two: the beta-th closest distance keeps its bit
        length, so the run ends after 3 windows (9 answers), with at most alpha more queries out."""
        start = (1 << 120) + (1 << 119)
        asked, end = self.run_grinding(lambda n: start - 1000 * n, start=start)
        self.assertIn(": no progress after", end)
        self.assertGreaterEqual(len(asked), STALL_WINDOWS * 3)
        self.assertLessEqual(len(asked), STALL_WINDOWS * 3 + 3)

    def test_progress_resets_the_count(self):
        """A bit nearer every 5th answer is progress enough: such a run goes on to the query cap."""
        asked, end = self.run_grinding(lambda n: (1 << max(8, 119 - n // 5)) + (1 << max(7, 118 - n // 5)) - n)
        self.assertIn(": query cap after", end)
        self.assertEqual(len(asked), MAX_QUERIES)


class HonestBoundsTest(LookupTestCase):
    """The bounds never end an honest lookup: over a 2000-node network every run completes, as it did before them, so
    its result is unchanged (only completion, a deadline, or running out of nodes ends a run the bounds don't)."""

    @classmethod
    def setUpClass(cls):
        """One 2000-node network for the class (it takes seconds to build)."""
        cls.net = FakeNetwork(2000, seed=21)

    def test_lookups_in_2000_nodes_complete(self):
        rng = random.Random(22)
        ids = list(self.net.nodes)
        for alpha, beta in ((3, 3), (1, 3), (5, 3), (3, 1)):
            for _ in range(25):
                key = rng.randbytes(ID_LENGTH)
                reports: list[str] = []
                lock = threading.Lock()

                def report(text, reports=reports, lock=lock):
                    """Collect reports (query threads report too)."""
                    with lock:
                        reports.append(text)

                result = Lookup(key, rng.sample(ids, 3), self.net.query, ident(0), alpha=alpha, beta=beta, timeout=30,
                                poll_interval=0.05, report=report).find_closest()
                with lock:
                    ends = [text for text in reports if "s; asked" in text]
                self.assertEqual(len(ends), 1)
                self.assertIn(": complete after", ends[0], f"alpha {alpha}, beta {beta}")
                self.assertEqual(result[:beta], self.net.true_closest(key, beta))


if __name__ == "__main__":
    unittest.main()
