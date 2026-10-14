"""Tests for RequestServer and its policy: /blocks budget, response pools, deadlines, displacement,
incoming-link eviction, upload pacing, and every handler's refusals. RNS objects are faked."""
import random
import threading
import time
import types
import unittest
from unittest import mock

import RNS
from RNS.vendor import umsgpack

from rnfs.config import Config
from rnfs.constants import (BLOCKS_PATH, FIND_PATH, ID_LENGTH, K, KIB, MAX_REQUEST_SIZE, MAX_RESPONSE_SIZE, PIN_PATH,
                            PROVIDE_PATH, RESPONSE_OVERHEAD)
from rnfs.content.cid import RAW, Cid
from rnfs.network import guards, protocol, server
from rnfs.network.identity import node_id_for_identity
from tests.support import FakeClock

KEY = bytes(range(ID_LENGTH))


def block_of(size: int, seed: int = 0) -> tuple[Cid, bytes]:
    """A raw block of size bytes and its CID."""
    data = bytes([seed % 256]) * size
    return Cid.for_block(RAW, data), data


# Pure policy

class BudgetTest(unittest.TestCase):
    """The /blocks budget (section 7)."""

    def test_spec_example(self):
        (c1, b1), (c2, _), (c3, b3) = block_of(40_000, 1), block_of(10, 2), block_of(30_000, 3)
        held = {c1: b1, c3: b3}
        limit = server.blocks_budget(60_000, None, None, 60)
        self.assertEqual(server.select_blocks([c1, c2, c3], held.get, limit), [b1, None])

    def test_first_served_block_always_included(self):
        (c1, b1), (c2, b2) = block_of(100_000, 1), block_of(10, 2)
        held = {c1: b1, c2: b2}
        self.assertEqual(server.select_blocks([c1, c2], held.get, 0), [b1])

    def test_leading_missing_blocks_are_none(self):
        (c1, _), (c2, b2) = block_of(10, 1), block_of(100_000, 2)
        self.assertEqual(server.select_blocks([c1, c2], {c2: b2}.get, 0), [None, b2])

    def test_capped_by_max_response_size(self):
        self.assertEqual(server.blocks_budget(10 * 1024 * KIB, 1e9, None, 60), MAX_RESPONSE_SIZE - RESPONSE_OVERHEAD)

    def test_capped_by_response_target_at_rate_estimate(self):
        self.assertEqual(server.blocks_budget(1_000_000, 8000, None, 60), 60_000)

    def test_upload_cap_lowers_rate(self):
        self.assertEqual(server.blocks_budget(1_000_000, 8000, 4000, 60), 30_000)
        self.assertEqual(server.blocks_budget(1_000_000, 8000, 16000, 60), 60_000)

    def test_max_bytes(self):
        self.assertEqual(server.blocks_budget(5000, 1e9, None, 60), 5000)


class ResponsePoolsTest(unittest.TestCase):
    """Slow and fast pools, displacement and refusal."""

    def setUp(self):
        """Pools of 4 slots (2 slow, 2 fast) with 30 s slack."""
        self.clock = FakeClock()
        self.pools = server.ResponsePools(4, 30, self.clock)

    def take(self, pool, name: bytes):
        """Reserve a slot for link name."""
        return self.pools.reserve(pool, name, name)

    def test_capacity_split(self):
        for total, slow, fast in ((2, 1, 1), (4, 2, 2), (5, 2, 3), (7, 3, 4)):
            with self.subTest(total=total):
                self.assertEqual(server.ResponsePools(total, 30).capacity, {"slow": slow, "fast": fast})

    def test_full_pool_refuses_young_responses(self):
        self.take(server.SLOW_POOL, b"a")
        self.take(server.SLOW_POOL, b"b")
        self.clock.now += 30  # not older than deadline_slack yet
        self.assertEqual(self.take(server.SLOW_POOL, b"c"), (None, None))

    def test_pools_are_independent(self):
        self.take(server.SLOW_POOL, b"a")
        self.take(server.SLOW_POOL, b"b")
        response, displaced = self.take(server.FAST_POOL, b"c")
        self.assertIsNotNone(response)
        self.assertIsNone(displaced)

    def test_displaces_lowest_throughput_old_response(self):
        fast, _ = self.take(server.FAST_POOL, b"a")
        slow, _ = self.take(server.FAST_POOL, b"b")
        self.clock.now += 31
        fast.sent, slow.sent = 100_000, 1000
        response, displaced = self.take(server.FAST_POOL, b"c")
        self.assertIs(displaced, slow)
        self.assertEqual(response.pool, server.FAST_POOL)
        self.assertEqual(set(self.pools.responses()), {fast, response})

    def test_slowest_response_too_young_means_refusal(self):
        # The slowest response overall is the candidate; an older, faster one is never taken instead
        old, _ = self.take(server.FAST_POOL, b"a")
        self.clock.now += 31
        young, _ = self.take(server.FAST_POOL, b"b")
        old.sent, young.sent = 100_000, 0
        self.assertEqual(self.take(server.FAST_POOL, b"c"), (None, None))
        self.assertEqual(set(self.pools.responses()), {old, young})

    def test_release(self):
        response, _ = self.take(server.SLOW_POOL, b"a")
        self.assertTrue(self.pools.release(response))
        self.assertFalse(self.pools.release(response))
        self.assertEqual(self.pools.count(server.SLOW_POOL), 0)


class IncomingLinksTest(unittest.TestCase):
    """Per-link request limits and eviction when max_incoming_links is reached."""

    def setUp(self):
        """Room for 3 links."""
        self.clock = FakeClock()
        self.links = server.IncomingLinks(3, self.clock, random.Random(1))

    def test_per_link_limits(self):
        self.links.admit(b"a")
        self.assertTrue(self.links.start_request(b"a", blocks=True))
        self.assertFalse(self.links.start_request(b"a", blocks=True))
        self.assertTrue(self.links.start_request(b"a", blocks=False))
        self.assertFalse(self.links.start_request(b"a", blocks=False))
        self.links.finish_request(b"a", blocks=True)
        self.assertTrue(self.links.start_request(b"a", blocks=False))

    def test_evicts_link_idle_longest(self):
        for name in (b"a", b"b", b"c"):
            self.links.admit(name)
            self.clock.now += 10
        self.links.start_request(b"a", blocks=False)  # busy, though oldest
        self.assertEqual(self.links.admit(b"d"), (True, b"b"))

    def test_evicts_random_link_without_blocks_when_all_busy(self):
        for name in (b"a", b"b", b"c"):
            self.links.admit(name)
            self.links.start_request(name, blocks=name != b"b")
        self.assertEqual(self.links.admit(b"d"), (True, b"b"))

    def test_refuses_when_all_have_blocks_in_progress(self):
        for name in (b"a", b"b", b"c"):
            self.links.admit(name)
            self.links.start_request(name, blocks=True)
        self.assertEqual(self.links.admit(b"d"), (False, None))
        self.assertEqual(self.links.count(), 3)

    def test_room_left(self):
        self.assertEqual(self.links.admit(b"a"), (True, None))

    def test_links_not_kept_are_evicted_first(self):
        # Private mode: a member's link idle longest stays; an outsider's goes, even busy with a /find
        for name in (b"a", b"b", b"c"):
            self.links.admit(name)
            self.clock.now += 10
        self.links.start_request(b"c", blocks=False)
        self.assertEqual(self.links.admit(b"d", keep={b"a", b"b"}), (True, b"c"))
        # With only kept links left to choose from, the usual rule applies among them
        self.assertEqual(self.links.admit(b"e", keep={b"a", b"b", b"d"}), (True, b"a"))

    def test_closed_link_is_not_counted_again(self):
        self.links.admit(b"a")
        self.links.remove(b"a")
        self.assertFalse(self.links.start_request(b"a", blocks=True))
        self.assertEqual(self.links.count(), 0)
        self.assertTrue(self.links.start_request(b"b", blocks=False))  # an untracked open link still counts


class UploadPacerTest(unittest.TestCase):
    """The next response on a link waits bytes × 8 / cap after the previous one started."""

    def test_wait(self):
        clock = FakeClock()
        pacer = server.UploadPacer(clock)
        pacer.started(b"a", 1000, 800)  # 10 s at 800 bit/s
        clock.now += 4
        self.assertAlmostEqual(pacer.wait_time(b"a"), 6)
        clock.now += 7
        self.assertEqual(pacer.wait_time(b"a"), 0)

    def test_no_cap(self):
        pacer = server.UploadPacer(FakeClock())
        pacer.started(b"a", 1000, None)
        self.assertEqual(pacer.wait_time(b"a"), 0)


# RequestServer over fake RNS objects

class FakeResource:
    """Stands in for an outgoing RNS.Resource."""

    def __init__(self, link, request_id: bytes, status=RNS.Resource.TRANSFERRING):
        """A response Resource in progress on link."""
        self.link = link
        self.request_id = request_id
        self.status = status
        self.callback = None
        self.progress = 0.0
        self.cancelled = False

    def get_progress(self):
        """Fraction sent."""
        return self.progress

    def cancel(self):
        """Cancel like RNS: fail, then run the callback."""
        self.cancelled = True
        self.status = RNS.Resource.FAILED
        self.conclude()
        if self.callback:
            self.callback(self)

    def complete(self):
        """Finish like RNS: complete, leave the link's outgoing Resources, then run the callback."""
        self.status = RNS.Resource.COMPLETE
        self.conclude()
        if self.callback:
            self.callback(self)

    def conclude(self):
        """Leave the link's outgoing Resources, as Link.resource_concluded does."""
        if self in self.link.outgoing_resources:
            self.link.outgoing_resources.remove(self)


class FakeLink:
    """Stands in for an incoming RNS.Link."""

    def __init__(self, link_id: bytes, mtu=500, mdu=431, watchable=True):
        """An active link; unwatchable ones lack register_outgoing_resource (changed RNS internals)."""
        self.link_id = link_id
        self.status = RNS.Link.ACTIVE
        self.rtt = 0.05
        self.mtu, self.mdu = mtu, mdu
        self.expected_rate = None
        self.outgoing_resources = []
        self.closed_callback = None
        self.remote_identity = None
        if not watchable:
            self.register_outgoing_resource = None

    def register_outgoing_resource(self, resource):
        """Called by a Resource once advertised, as in RNS."""
        self.outgoing_resources.append(resource)

    def get_mtu(self):
        """MTU."""
        return self.mtu

    def get_mdu(self):
        """MDU."""
        return self.mdu

    def get_expected_rate(self):
        """Rate set by tests."""
        return self.expected_rate

    def get_remote_identity(self):
        """The identity the requester identified as, or None."""
        return self.remote_identity

    def set_link_closed_callback(self, callback):
        """Store the closed callback."""
        self.closed_callback = callback

    def teardown(self):
        """Close and run the closed callback synchronously, like RNS."""
        if self.status == RNS.Link.CLOSED:
            return
        self.status = RNS.Link.CLOSED
        if self.closed_callback:
            self.closed_callback(self)


class FakeDestination:
    """Stands in for the rnfs.dht IN destination."""

    def __init__(self):
        """No handlers yet."""
        self.handlers = {}
        self.links = []
        self.max_request_size = None
        self.established = None

    def set_max_request_size(self, size):
        """Store it."""
        self.max_request_size = size

    def set_link_established_callback(self, callback):
        """Store it."""
        self.established = callback

    def register_request_handler(self, path, response_generator, allow, auto_compress):
        """Store the handler and its options."""
        self.handlers[path] = (response_generator, allow, auto_compress)


class FakeBackend:
    """A trivial backend recording what it's asked."""

    def __init__(self, blocks=None, dht_server=True, members=()):
        """Serve blocks (a dict CID -> bytes)."""
        self.blocks = blocks or {}
        self.dht_server = dht_server
        self.members = set(members)
        self.provided = []
        self.pins = []

    def is_dht_server(self):
        """Whether this node is a DHT server."""
        return self.dht_server

    def find(self, key):
        """Providers and nodes for key."""
        return [KEY] * (K + 2), [KEY]

    def provide(self, publisher_id, keys, withdrawn):
        """Record publisher_id for keys."""
        self.provided.append((publisher_id, keys, withdrawn))
        return True

    def served_block(self, cid):
        """The served block for cid, if any."""
        return self.blocks.get(cid)

    def remote_pin(self, identity_hash, cid, pin):
        """Accept or refuse a remote pin."""
        self.pins.append((identity_hash, cid, pin))
        return True

    def is_member(self, identity_hash):
        """Whether the identity is a member."""
        return identity_hash in self.members


class RequestServerTest(unittest.TestCase):
    """Handlers and limits, driven directly with fake links."""

    def setUp(self):
        """A server with a fake destination and clock, its monitor driven by hand."""
        saved = RNS.Resource.AUTO_COMPRESS_MAX_SIZE
        self.addCleanup(setattr, RNS.Resource, "AUTO_COMPRESS_MAX_SIZE", saved)
        self.identity = RNS.Identity()
        self.clock = FakeClock()
        self.big = [block_of(100 * KIB, i) for i in range(4)]
        self.backend = FakeBackend(blocks=dict(self.big))
        self.make_server(Config(remote_pin_identities=[self.identity.hash.hex()]))

    def make_server(self, config):
        """(Re)create the server under test."""
        self.config = config
        self.destination = FakeDestination()
        self.server = server.RequestServer(self.destination, config, self.backend, clock=self.clock,
                                           start_monitor=False)

    def connect(self, name: bytes, **kwargs) -> FakeLink:
        """A new incoming link, announced through the established callback."""
        link = FakeLink(name.ljust(16, b"\0"), **kwargs)
        self.destination.links.append(link)
        self.destination.established(link)
        return link

    def ask(self, path, data, link, identity=None, request_id=None):
        """Call path's handler as RNS would."""
        handler = self.destination.handlers[path][0]
        return handler(path, data, request_id or bytes(16), link.link_id, identity, 0.0)

    def blocks(self, link, cids=None, max_bytes=MAX_RESPONSE_SIZE, request_id=b"r" * 16):
        """Send a /blocks request (by default for one 100 KiB block)."""
        cids = [self.big[0][0]] if cids is None else cids
        return self.ask(BLOCKS_PATH, protocol.encode_blocks(max_bytes, cids), link, request_id=request_id)

    @staticmethod
    def send(link, request_id=b"r" * 16) -> FakeResource:
        """RNS starting the response Resource for request_id: it registers itself on the link."""
        resource = FakeResource(link, request_id)
        link.register_outgoing_resource(resource)
        return resource

    def test_registration(self):
        self.assertEqual(RNS.Resource.AUTO_COMPRESS_MAX_SIZE, 0)
        self.assertEqual(self.destination.max_request_size, MAX_REQUEST_SIZE)
        self.assertEqual(set(self.destination.handlers), {FIND_PATH, PROVIDE_PATH, BLOCKS_PATH, PIN_PATH})
        for _, allow, auto_compress in self.destination.handlers.values():
            self.assertEqual(allow, RNS.Destination.ALLOW_ALL)
            self.assertIs(auto_compress, False)

    def test_malformed_requests_get_false(self):
        link = self.connect(b"a")
        for path, data in ((FIND_PATH, b"short"), (PROVIDE_PATH, [KEY]), (BLOCKS_PATH, [True, []]),
                           (PIN_PATH, [b"x", True])):
            with self.subTest(path=path):
                self.assertIs(self.ask(path, data, link, self.identity), False)

    def test_find(self):
        link = self.connect(b"a")
        providers, nodes = self.ask(FIND_PATH, KEY, link)
        self.assertEqual(len(providers), K)
        self.assertEqual(nodes, [KEY])

    def test_client_mode_refuses_find_and_provide(self):
        self.backend.dht_server = False
        link = self.connect(b"a")
        self.assertIs(self.ask(FIND_PATH, KEY, link), False)
        self.assertIs(self.ask(PROVIDE_PATH, [[KEY], []], link, self.identity), False)

    def test_provide_records_identified_requester(self):
        link = self.connect(b"a")
        self.assertIs(self.ask(PROVIDE_PATH, [[KEY], []], link), False)
        self.assertIs(self.ask(PROVIDE_PATH, [[KEY], []], link, self.identity), True)
        self.assertEqual(self.backend.provided, [(node_id_for_identity(self.identity), [KEY], [])])

    def test_pin_needs_listed_identity(self):
        link = self.connect(b"a")
        cid = self.big[0][0]
        self.assertIs(self.ask(PIN_PATH, [cid.binary, True], link), False)
        self.assertIs(self.ask(PIN_PATH, [cid.binary, True], link, RNS.Identity()), False)
        self.assertIs(self.ask(PIN_PATH, [cid.binary, True], link, self.identity), True)
        self.assertEqual(self.backend.pins, [(self.identity.hash, cid, True)])

    def test_private_mode_refuses_non_members(self):
        self.backend.members = {self.identity.hash}
        self.make_server(Config(private=True))
        link = self.connect(b"a")
        self.assertIs(self.ask(FIND_PATH, KEY, link), False)
        self.assertIs(self.ask(FIND_PATH, KEY, link, RNS.Identity()), False)
        self.assertNotEqual(self.ask(FIND_PATH, KEY, link, self.identity), False)

    def test_small_response_is_not_tracked(self):
        link = self.connect(b"a")
        missing = block_of(10, 99)[0]
        self.assertEqual(self.blocks(link, [missing]), [None])
        self.assertEqual(self.server.pools.responses(), [])
        self.assertEqual(self.server.incoming.in_progress(link.link_id), (0, 0))

    def test_response_is_tracked_until_complete_and_gives_rate(self):
        link = self.connect(b"a", mtu=1500, mdu=1400)
        self.server.rates.record(link.link_id, 64 * KIB, 1_000_000)  # measured, so more than one block fits
        (c1, b1), (c2, b2) = self.big[:2]
        self.assertEqual(self.blocks(link, [c1, c2]), [b1, b2])
        self.assertEqual(self.server.incoming.in_progress(link.link_id), (1, 1))
        (response,) = self.server.pools.responses()
        self.assertEqual(response.pool, server.FAST_POOL)
        self.assertFalse(response.untracked)

        resource = self.send(link)
        self.assertIs(response.resource, resource)  # found as RNS registered it, no polling needed
        link.expected_rate = 50_000
        resource.complete()
        self.assertEqual(self.server.pools.responses(), [])
        self.assertEqual(self.server.incoming.in_progress(link.link_id), (0, 0))
        self.assertEqual(self.server.rates.get(link.link_id), 50_000)

    def test_fast_transfer_concluded_before_the_monitor_looks(self):
        link = self.connect(b"a")
        self.blocks(link)
        self.send(link).complete()
        self.assertEqual(self.server.pools.responses(), [])

    def test_budget_uses_rate_estimate(self):
        link = self.connect(b"a")
        self.server.rates.record(link.link_id, 64 * KIB, 8000 * 8)  # 8 kB/s: 480 kB in 60 s
        cids = [cid for cid, _ in self.big]
        # Slow link (64 kbit/s): upload cap is half the estimate, so 240 kB: two 100 KiB blocks
        self.assertEqual(len(self.blocks(link, cids)), 2)

    def test_second_blocks_refused_until_previous_concludes(self):
        link = self.connect(b"a")
        self.blocks(link)
        # Not yet registered (e.g. queued in RNS), then transferring: refused either way
        self.assertIs(self.blocks(link, request_id=b"s" * 16), False)
        resource = self.send(link)
        self.assertIs(self.blocks(link, request_id=b"s" * 16), False)
        # All parts sent, proof withheld: still refused (after a short grace), and the slots stay held
        resource.status = RNS.Resource.AWAITING_PROOF
        self.assertIs(self.blocks(link, request_id=b"s" * 16), False)
        self.assertEqual(len(self.server.pools.responses()), 1)
        # The proof arriving while the next request waits: accepted
        threading.Timer(0.1, resource.complete).start()
        self.assertNotEqual(self.blocks(link, request_id=b"s" * 16), False)

    def test_only_one_request_per_link_waits_for_a_withheld_proof(self):
        link = self.connect(b"a")
        self.blocks(link)
        self.send(link).status = RNS.Resource.AWAITING_PROOF  # all parts sent, proof withheld
        # A flood of /blocks on that link: one waits out the grace, the rest are refused at once
        finished, results = [], []

        def ask(i):
            """One /blocks request, noting when it was answered."""
            results.append(self.blocks(link, request_id=bytes([i]) * 16))
            finished.append(time.monotonic())

        start = time.monotonic()
        threads = [threading.Thread(target=ask, args=(i,)) for i in range(1, 9)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
        self.assertEqual(results, [False] * 8)
        quick = sum(1 for t in finished if t - start < 0.5)  # the grace here is 2 × RTT + 1 s
        self.assertEqual(quick, 7)
        self.assertEqual(len(self.server.pools.responses()), 1)
        self.assertEqual(self.server.incoming.in_progress(link.link_id), (1, 1))
        # The waiter's mark is cleared, so the next request may wait for the proof again
        self.assertTrue(self.server.incoming.start_waiting(link.link_id))

    def test_deadline_cancels_response(self):
        link = self.connect(b"a")
        self.blocks(link)
        resource = self.send(link)
        (response,) = self.server.pools.responses()
        # Slow link, no estimate: 3 × 100 KiB at 100 bit/s + 30 s
        self.assertAlmostEqual(response.deadline - response.started, 3 * response.size * 8 / 100 + 30)
        self.clock.now = response.deadline + 1
        self.server.check_responses()
        self.assertTrue(resource.cancelled)
        self.assertEqual(self.server.pools.responses(), [])
        self.assertEqual(link.status, RNS.Link.ACTIVE)

    def test_untracked_response_cancelled_by_closing_link(self):
        link = self.connect(b"a", watchable=False)  # RNS internals unavailable
        del link.outgoing_resources
        self.blocks(link)
        self.server.check_responses()
        (response,) = self.server.pools.responses()
        self.assertTrue(response.untracked)
        self.clock.now = response.deadline + 1
        self.server.check_responses()
        self.assertEqual(link.status, RNS.Link.CLOSED)
        self.assertEqual(self.server.pools.responses(), [])

    def test_unregistered_response_keeps_slots(self):
        link = self.connect(b"a")
        self.blocks(link)
        ahead = FakeResource(link, b"x" * 16)
        link.outgoing_resources.append(ahead)  # something else is ahead of it in RNS's queue
        self.clock.now += server.REGISTER_WINDOW + 1
        self.server.check_responses()
        self.assertEqual(len(self.server.pools.responses()), 1)
        self.assertEqual(link.status, RNS.Link.ACTIVE)
        # With nothing ahead and still never registered, RNS failed to send it: the link is closed
        link.outgoing_resources.clear()
        self.server.check_responses()
        self.assertEqual(self.server.pools.responses(), [])
        self.assertEqual(link.status, RNS.Link.CLOSED)

    def test_full_slow_pool_refuses_then_displaces(self):
        links = [self.connect(bytes([i])) for i in range(3)]
        resources = []
        for i, link in enumerate(links[:2]):
            self.assertNotEqual(self.blocks(link, [self.big[i][0]], request_id=bytes([i]) * 16), False)
            resources.append(self.send(link, bytes([i]) * 16))
        self.assertIs(self.blocks(links[2], [self.big[2][0]], request_id=b"\2" * 16), False)

        self.clock.now += 31
        resources[0].progress = 0.5
        self.server.check_responses()
        self.assertNotEqual(self.blocks(links[2], [self.big[2][0]], request_id=b"\2" * 16), False)
        self.assertTrue(resources[1].cancelled)
        self.assertFalse(resources[0].cancelled)
        self.assertEqual(self.server.incoming.in_progress(links[1].link_id), (0, 0))
        self.assertEqual(self.server.incoming.in_progress(links[0].link_id), (1, 1))

    def test_upload_pacing_between_responses(self):
        self.make_server(Config(max_upload_rate=80_000))
        link = self.connect(b"a", mtu=1500, mdu=1400)
        bound = server.MAX_PACING_WAIT
        sleeps = []
        with mock.patch.object(self.server, "_pace", lambda response, wait: sleeps.append(wait)):
            self.blocks(link)  # no rate estimate yet: no cap, no pacing
            self.send(link).complete()
            self.assertEqual(self.server.pacer.wait_time(link.link_id), 0)
            self.server.rates.record(link.link_id, 64 * KIB, 1_000_000)
            self.assertNotEqual(self.blocks(link, request_id=b"s" * 16), False)
            self.send(link, b"s" * 16).complete()
            # ~100 KiB at 80 kbit/s is ~10 s: longer than the requester would wait, so refused at once
            self.assertGreater(self.server.pacer.wait_time(link.link_id), bound)
            self.assertIs(self.blocks(link, request_id=b"t" * 16), False)
            self.assertEqual(sleeps, [])
            self.clock.now += 6
            wait = self.server.pacer.wait_time(link.link_id)
            self.assertNotEqual(self.blocks(link, request_id=b"t" * 16), False)
        self.assertEqual(len(sleeps), 1)
        self.assertAlmostEqual(sleeps[0], wait)

    def test_pacing_bound_ignores_the_link_rtt(self):
        """The requester can inflate the RTT RNS reports, so it doesn't lengthen how long a response is held."""
        link = self.connect(b"a")
        link.rtt = 200.0
        self.server.pacer.started(link.link_id, 2000, 1000)  # next response in 16 s
        self.assertIs(self.blocks(link), False)
        self.assertEqual(self.server.pools.responses(), [])

    def test_pacing_wait_ends_when_its_link_closes(self):
        link = self.connect(b"a")
        self.server.pacer.started(link.link_id, 500, 1000)  # next response in 4 s
        results = []
        thread = threading.Thread(target=lambda: results.append(self.blocks(link)))
        start = time.monotonic()
        thread.start()
        while not self.server.pools.responses() and time.monotonic() - start < 2:
            time.sleep(0.01)
        link.teardown()
        thread.join(5)
        self.assertLess(time.monotonic() - start, 2)
        self.assertEqual(results, [False])
        self.assertEqual(self.server.pools.responses(), [])
        # Nothing is tracked again for the closed link
        self.assertNotIn(link.link_id, self.server.incoming.links)
        self.assertNotIn(link.link_id, self.server.pacer.next_start)
        self.assertIs(self.ask(FIND_PATH, KEY, link), False)
        self.assertNotIn(link.link_id, self.server.incoming.links)

    def test_pacing_wait_ends_when_displaced(self):
        self.make_server(Config(max_blocks_responses=2))  # one slow-link slot
        link, other = self.connect(b"a"), self.connect(b"b")
        self.server.pacer.started(link.link_id, 500, 1000)
        results = []
        thread = threading.Thread(target=lambda: results.append(self.blocks(link)))
        thread.start()
        start = time.monotonic()
        while not self.server.pools.responses() and time.monotonic() - start < 2:
            time.sleep(0.01)
        self.clock.now += 31  # older than deadline_slack: the next request displaces it
        self.assertIsNot(self.blocks(other, request_id=b"s" * 16), False)
        thread.join(5)
        self.assertLess(time.monotonic() - start, 2)
        self.assertEqual(results, [False])
        self.assertEqual([r.link_id for r in self.server.pools.responses()], [other.link_id])
        self.assertEqual(self.server.incoming.in_progress(link.link_id), (0, 0))

    def test_blocks_on_a_closed_link_tracks_nothing(self):
        link = self.connect(b"a")
        link.teardown()  # still listed by the destination, as RNS may for a moment
        self.assertIs(self.blocks(link), False)
        self.assertNotIn(link.link_id, self.server.links)
        self.assertNotIn(link.link_id, self.server.incoming.links)

    def test_pacing_refusal_does_not_take_a_slot(self):
        self.make_server(Config(max_upload_rate=1000))
        link = self.connect(b"a")
        self.server.pacer.started(link.link_id, 10_000, 1000)  # next response in 80 s
        self.assertIs(self.blocks(link), False)
        self.assertEqual(self.server.pools.responses(), [])
        self.assertEqual(self.server.incoming.in_progress(link.link_id), (0, 0))

    def budget_server(self, budget: int):
        """A server with an upload_budget of budget bytes per hour (and response slots to spare)."""
        self.make_server(Config(upload_budget=budget, max_blocks_responses=20))
        return self.server.airtime

    def test_failed_responses_refund_what_they_did_not_send(self):
        """A Resource cancelled (the requester rejected it, or never fetched its parts), or a link closed
        mid-transfer, gives the budget back what wasn't sent, so one requester can't drain it for nothing."""
        airtime = self.budget_server(1000 * KIB)
        capacity = airtime.remaining()
        cids = [cid for cid, _ in self.big]
        link = self.connect(b"a")
        self.server.rates.record(link.link_id, 64 * KIB, 1_000_000)  # measured, so more than one block fits
        self.assertIsNot(self.blocks(link, cids), False)
        response = self.server.pools.responses()[0]
        charged = capacity - airtime.remaining()
        self.assertGreater(charged, 300 * KIB)
        resource = self.send(link)
        resource.progress = 0.25
        resource.cancel()  # RNS: rejected or timed out, then the callback
        self.assertAlmostEqual(capacity - airtime.remaining(), 0.25 * response.size, delta=2)
        # A link closed mid-transfer refunds the rest too
        before = airtime.remaining()
        other = self.connect(b"b")
        self.server.rates.record(other.link_id, 64 * KIB, 1_000_000)
        self.assertIsNot(self.blocks(other, cids[:2]), False)
        self.send(other).progress = 0.5
        packed = before - airtime.remaining()
        other.teardown()
        self.assertAlmostEqual(before - airtime.remaining(), packed / 2, delta=2)
        # A response cancelled before its Resource started (missed deadline) is refunded whole
        before = airtime.remaining()
        late = self.connect(b"c")
        self.assertIsNot(self.blocks(late, cids[:1]), False)
        self.server._cancel(self.server.pools.responses()[0], "test")
        self.assertAlmostEqual(airtime.remaining(), before, delta=1)

    def test_unreadable_progress_keeps_the_charge(self):
        airtime = self.budget_server(1000 * KIB)
        capacity = airtime.remaining()
        link = self.connect(b"a")
        self.assertIsNot(self.blocks(link), False)
        charged = capacity - airtime.remaining()
        resource = self.send(link)

        def unreadable():
            """RNS internals changed."""
            raise AttributeError("no progress")
        resource.get_progress = unreadable
        resource.cancel()
        self.assertEqual(self.server.pools.responses(), [])
        self.assertEqual(capacity - airtime.remaining(), charged)

    def test_completed_responses_keep_their_charge(self):
        airtime = self.budget_server(1000 * KIB)
        capacity = airtime.remaining()
        link = self.connect(b"a")
        self.assertIsNot(self.blocks(link), False)
        charged = capacity - airtime.remaining()
        resource = self.send(link)
        resource.progress = 1.0
        resource.complete()
        link.teardown()
        self.assertEqual(capacity - airtime.remaining(), charged)
        self.assertEqual(self.server.pools.responses(), [])

    def test_one_requester_holds_at_most_half_the_budget_in_flight(self):
        """Withholding proofs on several links, one identity's responses in flight hold at most about half
        the bucket; another requester is still served."""
        airtime = self.budget_server(800 * KIB)
        cids = [cid for cid, _ in self.big]
        greedy = RNS.Identity()
        answers = []
        for i in range(3):
            link = self.connect(bytes([0x10 + i]))
            self.server.rates.record(link.link_id, 64 * KIB, 1_000_000)  # measured, so more than one block fits
            answers.append(self.ask(BLOCKS_PATH, protocol.encode_blocks(MAX_RESPONSE_SIZE, cids), link, greedy,
                                    request_id=bytes([i]) * 16))
        self.assertIsNot(answers[0], False)
        served = [entry for entry in protocol.parse_blocks_response(answers[0], 4) if entry is not None]
        self.assertEqual(len(served), 3)  # cut to half the bucket
        self.assertIs(answers[2], False)
        self.assertEqual(self.server.traffic.snapshot()["refused"]["budget"], 1)
        in_flight = sum(response.charged for response in self.server.pools.responses())
        self.assertLessEqual(in_flight, 400 * KIB + 2 * 64 * KIB)
        self.assertIsNot(self.ask(BLOCKS_PATH, protocol.encode_blocks(MAX_RESPONSE_SIZE, cids[:1]),
                                  self.connect(b"z"), RNS.Identity()), False)
        self.assertGreater(airtime.remaining(), 0)

    def test_link_close_releases_responses(self):
        link = self.connect(b"a")
        self.blocks(link, [self.big[0][0]])
        link.teardown()
        self.assertEqual(self.server.pools.responses(), [])
        self.assertEqual(self.server.incoming.count(), 0)

    def test_incoming_link_eviction(self):
        self.make_server(Config(max_incoming_links=2))
        first, second = self.connect(b"a"), self.connect(b"b")
        self.clock.now += 5
        self.ask(FIND_PATH, KEY, first)  # first is now the more recently active
        third = self.connect(b"c")
        self.assertEqual(second.status, RNS.Link.CLOSED)
        self.assertEqual(first.status, RNS.Link.ACTIVE)
        self.assertEqual(third.status, RNS.Link.ACTIVE)

    def test_private_mode_evicts_outsiders_first(self):
        member = RNS.Identity()
        self.backend.members = {member.hash}
        self.make_server(Config(private=True, max_incoming_links=2))
        first = self.connect(b"a")
        first.remote_identity = member
        self.clock.now += 5
        outsider = self.connect(b"b")  # unidentified, and more recently active than the member's link
        self.connect(b"c")
        self.assertEqual(first.status, RNS.Link.ACTIVE)
        self.assertEqual(outsider.status, RNS.Link.CLOSED)

    def test_request_on_unknown_link(self):
        link = FakeLink(b"z" * 16)
        self.destination.links.append(link)  # established before the callback was set
        self.assertEqual(self.blocks(link, [block_of(5, 77)[0]]), [None])
        self.assertIn(link.link_id, self.server.watched)
        self.assertIs(self.ask(BLOCKS_PATH, [0, []], FakeLink(b"y" * 16)), False)  # no such link

    def test_backend_error_answers_false(self):
        def broken(key):
            """A backend that fails."""
            raise RuntimeError("boom")
        self.backend.find = broken
        self.assertIs(self.ask(FIND_PATH, KEY, self.connect(b"a")), False)


class ResourceGuardTest(unittest.TestCase):
    """The guard on incoming Resource advertisements (oversized-transfer bombs)."""

    def setUp(self):
        """Install the guard, recording what it passes to RNS and what it rejects."""
        guards.install()
        self.accepted, self.rejected = [], []
        patches = [mock.patch.object(guards, "_original_accept",
                                     lambda packet, **kwargs: self.accepted.append(packet) or "resource"),
                   mock.patch.object(RNS.Resource, "reject", self.rejected.append)]
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    @staticmethod
    def advertisement(transfer: int, data: int, request: bool = False, response: bool = True, link=None,
                      segment: int = 1, segments: int = 1):
        """A packet carrying a crafted, packed ResourceAdvertisement."""
        flags = 0x01 | (int(request) << 3) | (int(response) << 4) | (int(segments > 1) << 2)
        packed = umsgpack.packb({"t": transfer, "d": data, "n": 1, "h": bytes(32), "r": bytes(4), "o": bytes(32),
                                 "i": segment, "l": segments, "q": b"q" * 16 if request or response else None,
                                 "f": flags, "m": bytes(4)})
        return types.SimpleNamespace(plaintext=packed, link=link)

    def test_installed_once(self):
        guards.install()
        self.assertTrue(guards.installed())
        self.assertIs(RNS.Resource.__dict__["accept"].__func__, guards.guarded_accept)

    def test_accepts_honest_response(self):
        packet = self.advertisement(100_000 + 52, 100_000)
        self.assertEqual(RNS.Resource.accept(packet, callback=None), "resource")
        self.assertEqual(self.accepted, [packet])

    def test_rejects_transfer_larger_than_data(self):
        pending = mock.Mock(request_id=b"q" * 16)
        link = types.SimpleNamespace(pending_requests=[pending])
        packet = self.advertisement(2_000_000, 1000, link=link)  # under RNS's own 3 MiB cap
        self.assertIsNone(RNS.Resource.accept(packet))
        self.assertEqual((self.accepted, self.rejected), ([], [packet]))
        pending.response_rejected.assert_called_once()  # the requester fails at once, not at a timeout

    def test_rejects_transfer_over_largest_response(self):
        packet = self.advertisement(guards.MAX_TRANSFER_SIZE + 1, guards.MAX_TRANSFER_SIZE)
        self.assertIsNone(RNS.Resource.accept(packet))
        self.assertEqual(self.rejected, [packet])

    def test_rejects_request_resources(self):
        packet = self.advertisement(600, 600, request=True, response=False)
        self.assertIsNone(RNS.Resource.accept(packet))
        self.assertEqual(self.rejected, [packet])

    def test_rejects_multi_segment_resources_on_rnfs_links_only(self):
        """Each segment passes the size checks, so a 500 MiB response would be accepted segment by segment:
        on rnfs.dht links (every rnfs response is one segment) multi-segment Resources are refused, while
        other destinations' (LXMF's) are left alone."""
        identity = RNS.Identity()
        names = {}
        for app, aspect in (("rnfs", "dht"), ("lxmf", "delivery"), ("lxmf", "propagation")):
            names[app, aspect] = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE, app,
                                                 aspect).name
        outgoing = types.SimpleNamespace(destination=types.SimpleNamespace(name=names["rnfs", "dht"]),
                                         pending_requests=[])
        incoming = types.SimpleNamespace(destination=None, owner=types.SimpleNamespace(name=names["rnfs", "dht"]))
        lxmf = [types.SimpleNamespace(destination=types.SimpleNamespace(name=names[key]))
                for key in (("lxmf", "delivery"), ("lxmf", "propagation"))]
        self.assertTrue(guards.is_dht_link(outgoing) and guards.is_dht_link(incoming))
        self.assertFalse(any(guards.is_dht_link(link) for link in lxmf))
        size_ = 1_000_000
        for link in (outgoing, incoming):
            for segment, segments in ((1, 500), (2, 500), (2, 2)):
                packet = self.advertisement(size_ + 52, 500 * size_, link=link, segment=segment, segments=segments)
                self.assertIsNone(RNS.Resource.accept(packet))
                self.assertIn(packet, self.rejected)
            packet = self.advertisement(size_ + 52, size_, link=link)  # one segment: fine
            self.assertEqual(RNS.Resource.accept(packet), "resource")
        for link in lxmf:
            packet = self.advertisement(size_ + 52, 500 * size_, link=link, segment=3, segments=500)
            self.assertEqual(RNS.Resource.accept(packet), "resource")
        self.assertIn("one segment", guards.refusal(RNS.ResourceAdvertisement.unpack(
            self.advertisement(100, 200, segments=2).plaintext), outgoing))

    def test_unreadable_advertisement_left_to_rns(self):
        packet = types.SimpleNamespace(plaintext=b"\xc1", link=None)
        RNS.Resource.accept(packet)
        self.assertEqual(self.accepted, [packet])

    def test_unsupported_rns_refused_with_one_error(self):
        """An RNS whose unpacked advertisements have no t and d (renamed fields) is still refused, failing
        closed, and says so once per process at LOG_ERROR rather than refusing every Resource silently."""
        import rnfs
        lines = []
        for patcher in (mock.patch.object(guards, "_unsupported_reported", False),
                        mock.patch.object(guards, "log", lambda message, level: lines.append((message, level))),
                        mock.patch.object(RNS.ResourceAdvertisement, "unpack",
                                          lambda data: types.SimpleNamespace(transfer_size=100, data_size=100))):
            patcher.start()
            self.addCleanup(patcher.stop)
        packets = [self.advertisement(100, 100) for _ in range(3)]
        for packet in packets:
            self.assertIsNone(RNS.Resource.accept(packet))
        self.assertEqual(guards.refusal(types.SimpleNamespace(d=100)), "malformed sizes")
        self.assertEqual((self.accepted, self.rejected), ([], packets))
        errors = [message for message, level in lines if level == RNS.LOG_ERROR]
        self.assertEqual(errors, [f"this RNS version ({RNS.__version__}) isn't supported by rnfs "
                                  f"{rnfs.__version__}: incoming Resources are being refused"])
        self.assertEqual(len(lines), 4)  # and the usual warning per refused Resource

    def test_invalid_sizes_are_not_an_unsupported_rns(self):
        lines = []
        for patcher in (mock.patch.object(guards, "_unsupported_reported", False),
                        mock.patch.object(guards, "log", lambda message, level: lines.append((message, level)))):
            patcher.start()
            self.addCleanup(patcher.stop)
        for transfer, data in ((None, 100), ("100", 100), (-1, 100), (100, True)):
            self.assertEqual(guards.refusal(types.SimpleNamespace(t=transfer, d=data)), "malformed sizes")
        self.assertEqual(lines, [])


class CompressionSettingTest(unittest.TestCase):
    """RNS.Resource.AUTO_COMPRESS_MAX_SIZE = 0 refuses compressed Resources and stops compression."""

    def test_rns_decompression_limit_rejects_compressed_payloads(self):
        import bz2
        # What Resource.assemble does with max_decompressed_size = AUTO_COMPRESS_MAX_SIZE = 0
        decompressor = bz2.BZ2Decompressor()
        decompressor.decompress(bz2.compress(b"x" * 1000), max_length=0)
        self.assertFalse(decompressor.eof)

    def test_resource_reads_limit_at_creation(self):
        # Resource.__init__ copies the class attribute, so setting it once covers every later Resource
        import inspect
        source = inspect.getsource(RNS.Resource.__init__)
        self.assertIn("self.max_decompressed_size = Resource.AUTO_COMPRESS_MAX_SIZE", source)
        self.assertIn("self.auto_compress_limit = Resource.AUTO_COMPRESS_MAX_SIZE", source)


if __name__ == "__main__":
    unittest.main()
