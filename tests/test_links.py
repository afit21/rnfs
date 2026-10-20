"""Tests for LinkPool: shared and identified links, idle close, per-link request limits and retry, with a fake RNS."""
import threading
import time
import types
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.network import links
from tests.support import wait_for

NODE = bytes(16)
OTHER = bytes([1]) * 16


class FakeReceipt:
    """Stands in for RNS.RequestReceipt."""

    def __init__(self, link):
        """Receipt for a request on link."""
        self.link = link
        self.request_id = bytes([len(link.requests) % 256]) * 16  # a link may carry more than 256 requests
        self.response = None
        self.response_size = None
        self.progress = 0.0

    def get_response(self):
        """The response."""
        return self.response

    def get_progress(self):
        """Progress receiving the response."""
        return self.progress


class FakeLink:
    """Stands in for RNS.Link: becomes active immediately; answers requests with FakeLink.responder."""

    PENDING, ACTIVE, CLOSED = 0, 2, 4
    TRAFFIC_TIMEOUT_FACTOR = 6
    establish_delay = 0.0
    establishes = None  # function(link) -> False to have establishment time out, as a lost link request does
    responder = None  # function(link, path, data) -> response, SILENT, FAIL or a Stall
    response_size = 10  # what receipts report as the response's size
    created: list["FakeLink"] = []

    def __init__(self, destination, established_callback, closed_callback):
        """Link that establishes after establish_delay, as RNS.Link's constructor is called."""
        self.closed_callback = closed_callback
        self.status = FakeLink.PENDING
        self.rtt = 0.01
        self.establishment_timeout = 1
        self.identities = []
        self.requests = []
        self.idle = 0.0
        self.silence = 0.0
        self.expected_rate = None
        FakeLink.created.append(self)

        def establish():
            """Become active and report it."""
            time.sleep(FakeLink.establish_delay)
            if FakeLink.establishes is not None and not FakeLink.establishes(self):
                self.teardown_reason = links.LINK_TIMEOUT
                self.teardown()
                return
            if self.status == FakeLink.PENDING:
                self.status = FakeLink.ACTIVE
                established_callback(self)
        threading.Thread(target=establish, daemon=True).start()

    def identify(self, identity):
        """Record the identity."""
        self.identities.append(identity)

    def teardown(self):
        """Close the link; like RNS, the closed callback runs synchronously inside teardown."""
        if self.status == FakeLink.CLOSED:
            return
        self.status = FakeLink.CLOSED
        self.closed_callback(self)

    def no_data_for(self):
        """Seconds without data, set by tests."""
        return self.idle

    def no_inbound_for(self):
        """Seconds since anything arrived on the link, set by tests (by default just now)."""
        return self.silence

    def get_mdu(self):
        """Default MDU."""
        return 431 if self.status == FakeLink.ACTIVE else None

    def get_mtu(self):
        """Default MTU."""
        return 500 if self.status == FakeLink.ACTIVE else None

    def get_expected_rate(self):
        """Rate set by tests."""
        return self.expected_rate

    def request(self, path, data, response_callback=None, failed_callback=None, progress_callback=None,
                timeout=None, max_response_size=None):
        """Answer on another thread with FakeLink.responder."""
        assert timeout is None, "requesters use RNS's RTT-based timeout"
        receipt = FakeReceipt(self)
        self.requests.append((path, data, max_response_size))

        def answer():
            """Call back like RNS would."""
            response = FakeLink.responder(self, path, data)
            if response is SILENT:
                return
            if response is FAIL:
                receipt.status = links.RECEIPT_FAILED  # as RNS marks a request it timed out
                failed_callback(receipt)
                return
            if isinstance(response, Stall):
                # The response starts arriving, then never progresses again
                receipt.response_size = response.size
                receipt.progress = 0.1
                progress_callback(receipt)
                return
            receipt.response = response
            receipt.response_size = FakeLink.response_size
            receipt.progress = 1.0
            progress_callback(receipt)
            response_callback(receipt)
        threading.Thread(target=answer, daemon=True).start()
        return receipt


class RacingLink(FakeLink):
    """FakeLink whose requests go as RNS 1.5.6's Link.request sends them, with RNS's own handle_response and
    RequestReceipt: the packet first, the receipt registered in pending_requests after. FakeLink.responder's answer
    reaches handle_response in between, as a fast link's answer can while the requesting thread waits for the GIL."""

    handle_response = RNS.Link.handle_response
    timeout = 1.0  # seconds before a request still pending fails, as RNS's own timeout (11 s at least) would

    def __init__(self, *args, **kwargs):
        """A link with RNS's pending requests."""
        self.pending_requests = []
        super().__init__(*args, **kwargs)

    def request(self, path, data, response_callback=None, failed_callback=None, progress_callback=None,
                timeout=None, max_response_size=None):
        """Answer before the receipt is registered, then register it."""
        self.requests.append((path, data, max_response_size))
        request_id = len(self.requests).to_bytes(16, "big")
        self.handle_response(request_id, FakeLink.responder(self, path, data), 10, 10, update_sizes=True,
                             check_size=True)
        packet_receipt = types.SimpleNamespace(truncated_hash=request_id, set_timeout_callback=lambda callback: None,
                                               status=None, proved=False, concluded_at=None,
                                               callbacks=types.SimpleNamespace(delivery=None))
        receipt = RNS.RequestReceipt(self, packet_receipt=packet_receipt, response_callback=response_callback,
                                     failed_callback=failed_callback, progress_callback=progress_callback,
                                     timeout=RacingLink.timeout, max_response_size=max_response_size)
        # RNS's timeout, shortened: it fails a request still pending then (an answered one is left as it is)
        timer = threading.Timer(RacingLink.timeout, receipt.request_timed_out, args=(None,))
        timer.daemon = True
        timer.start()
        return receipt


SILENT = object()  # responder result: never answer
FAIL = object()  # responder result: RNS reports the request failed


class Stall:
    """Responder result: a response of size bytes starts arriving and then stops."""

    def __init__(self, size: int):
        """A stalling response of size bytes."""
        self.size = size


class FakeDestination:
    """Stands in for RNS.Destination: an identity's rnfs.dht hash is its .node_id (see fake_rns's recall)."""

    OUT, SINGLE = 2, 0

    def __init__(self, identity, direction, kind, app_name, *aspects):
        """The destination of identity; its hash is the node ID recall() made it for, or identity.dht_hash."""
        self.hash = getattr(identity, "dht_hash", identity.node_id)


def fake_rns():
    """A stand-in for the RNS module where every node is reachable and links are FakeLinks."""
    return types.SimpleNamespace(
        Link=FakeLink,
        Destination=FakeDestination,
        Identity=types.SimpleNamespace(recall=lambda node_id: types.SimpleNamespace(node_id=node_id)),
        Transport=types.SimpleNamespace(await_path=lambda node_id, timeout: True, has_path=lambda node_id: True,
                                        request_path=lambda node_id: None),
        log=lambda *args: None, prettyhexrep=lambda b: b.hex(), LOG_INFO=4, LOG_VERBOSE=5, loglevel=4,
    )


def run(test, function, timeout=5):
    """Return function(), run on a thread so a deadlock fails the test instead of hanging it."""
    result = []
    thread = threading.Thread(target=lambda: result.append(function()), daemon=True)
    thread.start()
    thread.join(timeout)
    test.assertFalse(thread.is_alive(), "deadlocked")
    return result[0]


class LinkPoolTest(unittest.TestCase):
    """LinkPool behaviour with RNS replaced by fake_rns()."""

    def setUp(self):
        """Patch RNS in the links module and create an empty LinkPool."""
        patcher = mock.patch.object(links, "RNS", fake_rns())
        patcher.start()
        self.addCleanup(patcher.stop)
        FakeLink.created = []
        FakeLink.establish_delay = 0.0
        FakeLink.response_size = 10
        FakeLink.responder = staticmethod(lambda link, path, data: True)
        self.identity = object()
        self.now = [1000.0]  # the pool's clock, which sweep() compares with when get() last handed a link out
        self.pool = links.LinkPool(self.identity, Config(link_idle_timeout=300), clock=lambda: self.now[0])
        self.addCleanup(self.pool.close_all)
        # The tests here call sweep() themselves, at the times they set on the pool's clock. The sweeper thread would
        # sweep too, whenever get() or a request's end wakes it: on a loaded machine after a test has moved the clock
        # on, closing a link the test means to see kept, handed out again or closed by its own sweep() (rightly so,
        # by the clock it reads). test_sweeper_closes_a_link_when_it_is_due tests the thread, on a pool of its own.
        self.pool._start_sweeper = lambda: None

    def test_reuses_shared_link(self):
        first = run(self, lambda: self.pool.get(NODE))
        self.assertIs(run(self, lambda: self.pool.get(NODE)), first)
        self.assertTrue(self.pool.has_link(NODE))
        self.assertEqual(self.pool.open_nodes(), [NODE])

    def test_shared_link_does_not_identify(self):
        peer = run(self, lambda: self.pool.get(NODE))
        self.assertFalse(peer.identified)
        self.assertEqual(peer.link.identities, [])

    def test_private_mode_identifies_every_link(self):
        # Private mode links only to members: NODE's recalled identity is one
        member = types.SimpleNamespace(node_id=NODE, hash=bytes([7]) * 16)
        links.RNS.Identity = types.SimpleNamespace(recall=lambda node_id: member)
        pool = links.LinkPool(self.identity, Config(private=True, members=[member.hash.hex()]))
        self.addCleanup(pool.close_all)
        peer = run(self, lambda: pool.get(NODE))
        self.assertEqual(peer.link.identities, [self.identity])

    def test_concurrent_gets_open_one_link(self):
        FakeLink.establish_delay = 0.2
        results = []
        threads = [threading.Thread(target=lambda: results.append(self.pool.get(NODE))) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
        self.assertEqual(len(FakeLink.created), 1)
        self.assertEqual(len({id(peer) for peer in results}), 1)

    def test_identified_link_is_separate(self):
        shared = run(self, lambda: self.pool.get(NODE))
        identified = run(self, lambda: self.pool.open_identified(NODE))
        self.assertIsNot(identified, shared)
        self.assertEqual(identified.link.identities, [self.identity])
        self.pool.close_link(identified)
        self.assertEqual(identified.link.status, FakeLink.CLOSED)
        self.assertIs(self.pool.get(NODE), shared)

    def test_close_tears_down_and_forgets(self):
        peer = run(self, lambda: self.pool.get(NODE))
        self.pool.close(NODE)
        self.assertEqual(peer.link.status, FakeLink.CLOSED)
        self.assertFalse(self.pool.has_link(NODE))
        self.pool.close(NODE)  # closing again is harmless

    def test_closed_link_is_forgotten(self):
        peer = run(self, lambda: self.pool.get(NODE))
        peer.link.teardown()
        self.assertFalse(self.pool.has_link(NODE))
        self.assertIsNot(run(self, lambda: self.pool.get(NODE)), peer)

    def test_unreachable(self):
        links.RNS.Transport.has_path = lambda node_id: False
        links.RNS.Transport.await_path = lambda node_id, timeout: False
        self.assertIsNone(run(self, lambda: self.pool.get(NODE)))

    def test_await_path(self):
        """A path a lookup waits for before its query (to see its hops): requested only if RNS has none."""
        waited = []
        links.RNS.Transport.await_path = lambda node_id, timeout: waited.append((node_id, timeout)) or True
        self.assertTrue(self.pool.await_path(NODE))
        self.assertEqual(waited, [])  # there is one
        links.RNS.Transport.has_path = lambda node_id: False
        self.assertTrue(self.pool.await_path(NODE))
        self.assertEqual(waited, [(NODE, self.pool.config.path_timeout)])
        # Another node: NODE's path was just requested, so it isn't asked again within PATH_REQUEST_MI (test_paths)
        links.RNS.Transport.await_path = lambda node_id, timeout: False
        self.assertFalse(self.pool.await_path(OTHER))

    def test_await_path_with_a_shorter_wait(self):
        """A lookup candidate's measured wait (PathRequests.candidate_wait): used, and not measured itself."""
        measured = []
        self.pool.paths = mock.Mock(await_path=lambda node_id, timeout, cancel, measure: measured.append(
            (timeout, measure)) or True)
        links.RNS.Transport.has_path = lambda node_id: False
        self.assertTrue(self.pool.await_path(NODE, 15.0))
        self.assertTrue(self.pool.await_path(NODE))
        self.assertEqual(measured, [(15.0, False), (self.pool.config.path_timeout, True)])

    def test_awaited_identity_marked_used_once(self):
        # Polls for an identity still on its way don't mark the destination used; contacting it does, once (C2)
        calls = []

        def recall(node_id, _no_use=False):
            """Unknown for the first call and two polls, then known."""
            calls.append(_no_use)
            return types.SimpleNamespace(node_id=node_id) if len(calls) > 3 else None
        links.RNS.Identity.recall = recall
        self.assertIsNotNone(run(self, lambda: self.pool.get(NODE)))
        self.assertEqual(calls, [False, True, True, True, False])

    def test_round_trip_reported_for_every_link_opened(self):
        # rnfs peers' rtt: the shared link, a one-shot (ping's) and an identified one each report theirs
        reported = []
        self.pool.on_rtt = lambda node_id, rtt: reported.append((node_id, rtt))
        run(self, lambda: self.pool.get(NODE))
        run(self, lambda: self.pool.get(NODE))  # reused: not established again
        self.pool.close_link(run(self, lambda: self.pool.open_identified(OTHER)))
        self.assertEqual(reported, [(NODE, 0.01), (OTHER, 0.01)])
        for rtt in (None, float("nan"), float("inf"), -1.0, True, "0.1"):  # not a usable measurement: not reported
            self.pool._report_rtt(NODE, rtt)
        self.assertEqual(len(reported), 2)

    def test_ping_records_the_round_trip(self):
        from rnfs.network.client import PeerClient
        client = PeerClient(self.pool, Config())
        self.assertEqual(run(self, lambda: client.ping(OTHER)), 0.01)
        peer = client.traffic.peer(OTHER)
        self.assertEqual(peer["rtt"], 0.01)
        self.assertIsNotNone(peer["rtt_at"])
        self.assertIsNotNone(peer["last_seen"])  # a link established: it is up
        self.assertFalse(self.pool.has_link(OTHER))  # ping's link is closed again

    def test_path_described_only_for_a_kept_debug_message(self):
        # The description costs RNS lookups: built only when a LOG_DEBUG message goes somewhere (C2)
        with mock.patch.object(links, "path_description", return_value="") as describe:
            run(self, lambda: self.pool.get(NODE))
            describe.assert_not_called()
            links.RNS.loglevel = links.LOG_DEBUG
            run(self, lambda: self.pool.get(OTHER))
        describe.assert_called_once_with(OTHER)

    def test_no_link_to_another_destination_of_the_identity(self):
        # The identity recalled for NODE has another rnfs.dht hash: NODE is some other destination of it
        links.RNS.Identity.recall = lambda node_id: types.SimpleNamespace(node_id=node_id, dht_hash=OTHER)
        self.assertIsNone(run(self, lambda: self.pool.get(NODE)))
        self.assertIsNone(run(self, lambda: self.pool.open_identified(NODE)))
        self.assertEqual(FakeLink.created, [])

    def test_sweep_closes_only_idle_links(self):
        idle = run(self, lambda: self.pool.get(NODE))
        busy = run(self, lambda: self.pool.get(OTHER))
        idle.link.idle = busy.link.idle = 301
        self.now[0] += 301
        busy.acquire(blocks=False)
        self.assertEqual(self.pool.sweep(), 1)
        self.assertEqual(idle.link.status, FakeLink.CLOSED)
        self.assertEqual(busy.link.status, FakeLink.ACTIVE)

    def test_sweep_keeps_recent_links(self):
        peer = run(self, lambda: self.pool.get(NODE))
        peer.link.idle = 299
        self.now[0] += 301
        self.assertEqual(self.pool.sweep(), 0)

    def test_sweep_keeps_a_link_just_handed_out(self):
        # A link quiet for link_idle_timeout that get() has just returned: its caller is about to use it
        peer = run(self, lambda: self.pool.get(NODE))
        peer.link.idle = 301
        self.now[0] += 301
        self.assertIs(run(self, lambda: self.pool.get(NODE)), peer)
        self.assertEqual(self.pool.sweep(), 0)
        self.assertEqual(peer.link.status, FakeLink.ACTIVE)
        self.now[0] += 301  # not used after all
        self.assertEqual(self.pool.sweep(), 1)

    def test_sweep_keeps_a_held_link(self):
        # A requester waiting out the provider's upload pacing (9 minutes for a LoRa chunk) holds its link
        self.assertFalse(self.pool.hold(NODE, 600))  # no link yet
        peer = run(self, lambda: self.pool.get(NODE))
        self.assertTrue(self.pool.hold(NODE, 600))
        self.assertTrue(self.pool.hold(NODE, 10))  # a shorter hold doesn't cut a longer one short
        peer.link.idle = 590
        self.now[0] += 590
        self.assertEqual(self.pool.sweep(), 0)
        self.assertIs(run(self, lambda: self.pool.get(NODE)), peer)  # handed out meanwhile: still held
        peer.link.idle = 600 + 299
        self.now[0] += 10 + 299  # link_idle_timeout after the hold ends, less a second
        self.assertEqual(self.pool.sweep(), 0)
        peer.link.idle += 2
        self.now[0] += 2
        self.assertEqual(self.pool.sweep(), 1)
        self.assertEqual(peer.link.status, FakeLink.CLOSED)

    def test_idle_limit_closes_before_the_first_keepalive(self):
        # RNS 1.5.6's interval: max(5 s, RTT × 360 / 1.75), at most 360 s
        backbone = 0.05 * 360 / 1.75  # 10.3 s on a 50 ms path
        self.assertAlmostEqual(links.idle_limit(300, backbone, 0.05, False), backbone - 1 - 0.1)
        self.assertAlmostEqual(links.idle_limit(300, 5, 0.002, False), 5 - 1 - 0.004)  # the 5 s floor, on a LAN
        self.assertEqual(links.idle_limit(300, 360, 2.0, False), 300)  # radio: no keepalive within 5 minutes anyway
        self.assertEqual(links.idle_limit(300, backbone, 0.05, True), 300)  # a rate estimate keeps the link
        self.assertEqual(links.idle_limit(300, None, 0.05, False), 300)  # no keepalive interval known
        self.assertEqual(links.idle_limit(3, backbone, 0.05, False), 3)  # a shorter link_idle_timeout wins
        self.assertEqual(links.idle_limit(300, 1.0, 0.4, False), links.KEEPALIVE_MARGIN)  # never below the margin

    def test_sweep_closes_a_link_without_a_rate_estimate_before_its_keepalive(self):
        peer = run(self, lambda: self.pool.get(NODE))
        peer.link.keepalive, peer.link.rtt = 10.3, 0.05
        peer.link.idle = 5.0
        self.now[0] += 5
        self.assertEqual(self.pool.sweep(), 0)
        self.assertAlmostEqual(self.pool.next_sweep, 10.3 - 1 - 0.1 - 5)  # when it is due
        peer.link.idle = 9.3
        self.now[0] += 4.3
        self.assertEqual(self.pool.sweep(), 1)
        self.assertEqual(peer.link.status, FakeLink.CLOSED)

    def test_sweep_keeps_a_link_with_a_rate_estimate_for_link_idle_timeout(self):
        # Closing it would reset the provider's estimate (its next response held to one chunk) and RNS's window
        peer = run(self, lambda: self.pool.get(NODE))
        peer.link.keepalive, peer.link.rtt = 10.3, 0.05
        peer.rate = 1_000_000
        peer.link.idle = 299
        self.now[0] += 299
        self.assertEqual(self.pool.sweep(), 0)
        self.assertAlmostEqual(self.pool.next_sweep, 1)
        peer.link.idle = 301
        self.now[0] += 2
        self.assertEqual(self.pool.sweep(), 1)

    def test_sweep_keeps_a_short_lived_link_while_handed_out_or_held(self):
        peer = run(self, lambda: self.pool.get(NODE))
        peer.link.keepalive, peer.link.rtt = 10.3, 0.05
        peer.link.idle = 60
        self.now[0] += 5  # handed out 5 s ago: its caller may yet send a request
        self.assertEqual(self.pool.sweep(), 0)
        self.assertAlmostEqual(self.pool.next_sweep, 10.3 - 1 - 0.1 - 5)
        self.now[0] += 5
        self.assertTrue(self.pool.hold(NODE, 30))  # a pacing wait holds it for link_idle_timeout after it
        self.now[0] += 30 + 299
        self.assertEqual(self.pool.sweep(), 0)
        self.now[0] += 2
        self.assertEqual(self.pool.sweep(), 1)

    def test_sweep_keeps_a_busy_link(self):
        peer = run(self, lambda: self.pool.get(NODE))
        peer.link.keepalive, peer.link.rtt = 10.3, 0.05
        peer.link.idle = 60
        self.now[0] += 60
        peer.acquire(blocks=True)  # a response still on its way
        self.assertEqual(self.pool.sweep(), 0)
        peer.release(blocks=True)
        self.assertEqual(self.pool.sweep(), 1)

    def test_sweeper_closes_a_link_when_it_is_due(self):
        # The sweeper's longest wait is 30 s here; a link due in about a second is closed then, not at the next one
        pool = links.LinkPool(self.identity, Config(link_idle_timeout=300))
        self.addCleanup(pool.close_all)
        peer = run(self, lambda: pool.get(NODE))
        used = time.monotonic()
        peer.link.keepalive, peer.link.rtt = 2.0, 0.01  # closed after 0.98 s without data
        peer.link.no_data_for = lambda: time.monotonic() - used
        run(self, lambda: pool.request(peer, "/find", b"x"))  # ends with a wake-up: the sweep sees the link due soon
        wait_for(lambda: peer.link.status == FakeLink.CLOSED, timeout=10, what="the idle link closed")
        self.assertLess(time.monotonic() - used, 10)
        self.assertFalse(pool.has_link(NODE))

    def test_sweep_rechecks_under_the_lock(self):
        # get() hands the link out after sweep() found it idle but before it is closed
        peer = run(self, lambda: self.pool.get(NODE))
        peer.link.idle = 301
        self.now[0] += 301
        original = peer.busy

        def busy_then_handed_out():
            """Report not busy, then let get() hand the peer out, as another thread would."""
            result = original()
            peer.handed_out = self.now[0]
            return result
        peer.busy = busy_then_handed_out
        self.assertEqual(self.pool.sweep(), 0)
        self.assertEqual(peer.link.status, FakeLink.ACTIVE)
        self.assertTrue(self.pool.has_link(NODE))

    def test_request_answers(self):
        FakeLink.responder = staticmethod(lambda link, path, data: [path, data])
        peer = run(self, lambda: self.pool.get(NODE))
        exchange = run(self, lambda: self.pool.request(peer, "/find", b"k"))
        self.assertTrue(exchange.answered)
        self.assertEqual(exchange.response, ["/find", b"k"])

    def test_answer_arriving_before_rns_registers_the_request(self):
        # RNS drops a response no pending request matches: without EarlyResponses this request would wait out RNS's
        # timeout (RacingLink's, shortened) and end unanswered
        links.RNS.Link = RacingLink
        FakeLink.responder = staticmethod(lambda link, path, data: [path, data])
        peer = run(self, lambda: self.pool.get(NODE))
        exchange = run(self, lambda: self.pool.request(peer, "/find", b"k"))
        self.assertTrue(exchange.answered)
        self.assertEqual(exchange.response, ["/find", b"k"])
        self.assertEqual(peer.link.pending_requests, [])
        self.assertEqual(len(peer.early.kept), 0)

    def test_request_failure(self):
        FakeLink.responder = staticmethod(lambda link, path, data: FAIL)
        peer = run(self, lambda: self.pool.get(NODE))
        self.assertFalse(run(self, lambda: self.pool.request(peer, "/find", b"k")).answered)

    def test_a_link_silent_through_an_unanswered_request_is_closed(self):
        """Nothing came back on the link while a request waited out RNS's timeout: the other end lost the link (its
        rnsd restarted), which RNS would notice only at its keepalive. Closed, so the next request opens a new one."""
        FakeLink.responder = staticmethod(lambda link, path, data: FAIL)
        peer = run(self, lambda: self.pool.get(NODE))
        peer.link.silence = 10_000.0
        self.assertFalse(run(self, lambda: self.pool.request(peer, "/find", b"k")).answered)
        self.assertEqual(peer.link.status, FakeLink.CLOSED)
        FakeLink.responder = staticmethod(lambda link, path, data: True)
        fresh = run(self, lambda: self.pool.get(NODE))
        self.assertIsNot(fresh, peer)
        self.assertTrue(run(self, lambda: self.pool.request(fresh, "/find", b"k")).answered)

    def test_a_link_heard_from_meanwhile_is_kept_after_an_unanswered_request(self):
        # The request or its answer was lost on the way (RNS resends neither); the link itself works
        FakeLink.responder = staticmethod(lambda link, path, data: FAIL)
        peer = run(self, lambda: self.pool.get(NODE))
        self.assertFalse(run(self, lambda: self.pool.request(peer, "/find", b"k")).answered)
        self.assertEqual(peer.link.status, FakeLink.ACTIVE)
        self.assertIs(run(self, lambda: self.pool.get(NODE)), peer)

    def test_a_silent_link_another_request_uses_is_left_to_it(self):
        FakeLink.responder = staticmethod(lambda link, path, data: FAIL)
        peer = run(self, lambda: self.pool.get(NODE))
        peer.link.silence = 10_000.0
        peer.users[-1] = 1  # another thread's request in progress on it
        self.assertFalse(run(self, lambda: self.pool.request(peer, "/find", b"k")).answered)
        self.assertEqual(peer.link.status, FakeLink.ACTIVE)

    def test_request_outcomes_are_logged_with_the_reason(self):
        FakeLink.responder = staticmethod(lambda link, path, data: [path, data])
        peer = run(self, lambda: self.pool.get(NODE))
        with mock.patch.object(links, "log") as logged:
            run(self, lambda: self.pool.request(peer, "/find", b"k"))
        self.assertTrue(any("answered /find in" in call.args[0] and call.args[1] == links.LOG_DEBUG
                            for call in logged.call_args_list), logged.call_args_list)
        FakeLink.responder = staticmethod(lambda link, path, data: FAIL)
        with mock.patch.object(links, "log") as logged:
            run(self, lambda: self.pool.request(peer, "/find", b"k"))
        message = logged.call_args_list[-1].args[0]
        self.assertIn("/find to", message)
        self.assertIn("not answered after", message)

    def test_close_reason_names_the_end_that_closed_the_link(self):
        def link(initiator, reason):
            """A closed link as RNS leaves it."""
            return types.SimpleNamespace(initiator=initiator, teardown_reason=reason)

        # RNS records the role of the end that closed it: for a link opened by the other node, the roles swap
        self.assertEqual(links.close_reason(link(True, links.INITIATOR_CLOSED)), "closed by this node")
        self.assertEqual(links.close_reason(link(True, links.DESTINATION_CLOSED)), "closed by the other node")
        self.assertEqual(links.close_reason(link(False, links.INITIATOR_CLOSED)), "closed by the other node")
        self.assertEqual(links.close_reason(link(False, links.DESTINATION_CLOSED)), "closed by this node")
        self.assertEqual(links.close_reason(link(False, links.LINK_TIMEOUT)), "timed out")
        self.assertEqual(links.close_reason(types.SimpleNamespace()), "closed")

    def test_request_gives_up_when_link_closes(self):
        FakeLink.responder = staticmethod(lambda link, path, data: SILENT)
        peer = run(self, lambda: self.pool.get(NODE))
        threading.Timer(0.3, peer.link.teardown).start()
        exchange = run(self, lambda: self.pool.request(peer, "/find", b"k"))
        self.assertFalse(exchange.answered)
        self.assertEqual(peer.requests, 0)

    def test_watchdog_stops_waiting(self):
        FakeLink.responder = staticmethod(lambda link, path, data: SILENT)
        peer = run(self, lambda: self.pool.get(NODE))
        exchange = run(self, lambda: self.pool.request(peer, "/blocks", [], blocks=True,
                                                         watchdog=lambda ex, now: "stuck"))
        self.assertEqual(exchange.stalled, "stuck")
        self.assertEqual(peer.blocks, 0)

    def test_false_on_identified_link_is_retried_once_after_identifying(self):
        FakeLink.responder = staticmethod(lambda link, path, data: False)
        peer = run(self, lambda: self.pool.open_identified(NODE))
        exchange = run(self, lambda: self.pool.call(peer, "/provide", [[], []]))
        self.assertIs(exchange.response, False)
        self.assertEqual(len(peer.link.requests), 2)
        self.assertEqual(peer.link.identities, [self.identity, self.identity])

    def test_retry_succeeds(self):
        answers = iter([False, True])
        FakeLink.responder = staticmethod(lambda link, path, data: next(answers))
        peer = run(self, lambda: self.pool.open_identified(NODE))
        self.assertIs(run(self, lambda: self.pool.call(peer, "/provide", [[], []])).response, True)

    def test_false_on_unidentified_link_is_not_retried(self):
        FakeLink.responder = staticmethod(lambda link, path, data: False)
        peer = run(self, lambda: self.pool.get(NODE))
        self.assertIs(run(self, lambda: self.pool.call(peer, "/blocks", [0, []])).response, False)
        self.assertEqual(len(peer.link.requests), 1)


class EarlyResponsesTest(unittest.TestCase):
    """EarlyResponses on a stand-in link: what it keeps and hands over."""

    def setUp(self):
        """A link with no pending request, whose own handle_response records what it is given."""
        self.handled = []
        self.link = types.SimpleNamespace(pending_requests=[],
                                          handle_response=lambda *args, **kwargs: self.handled.append(args))
        self.now = [100.0]
        self.early = links.EarlyResponses(self.link, clock=lambda: self.now[0])

    def test_a_pending_request_is_answered_at_once(self):
        self.link.pending_requests.append(types.SimpleNamespace(request_id=b"a"))
        self.link.handle_response(b"a", "answer", 6, 6)
        self.assertEqual(self.handled, [(b"a", "answer", 6, 6)])
        self.assertFalse(self.early.deliver(types.SimpleNamespace(request_id=b"a")))

    def test_an_early_answer_is_kept_until_its_request_is_registered(self):
        self.link.handle_response(b"a", "answer", 6, 6)
        self.assertEqual(self.handled, [])
        self.assertFalse(self.early.deliver(types.SimpleNamespace(request_id=b"b")))
        self.assertTrue(self.early.deliver(types.SimpleNamespace(request_id=b"a")))
        self.assertEqual(self.handled, [(b"a", "answer", 6, 6)])
        self.assertFalse(self.early.deliver(types.SimpleNamespace(request_id=b"a")))  # handed over once

    def test_unmatched_answers_are_kept_within_bounds(self):
        for i in range(links.MAX_EARLY_RESPONSES + 5):
            self.link.handle_response(bytes([i]), "late", 4, 4)
        self.assertEqual(len(self.early.kept), links.MAX_EARLY_RESPONSES)
        self.assertNotIn(bytes([0]), self.early.kept)  # the oldest go first
        self.now[0] += links.EARLY_RESPONSE_LIFE + 1
        self.link.handle_response(b"new", "late", 4, 4)
        self.assertEqual(list(self.early.kept), [b"new"])

    def test_installed_only_on_a_link_with_rns_pending_requests(self):
        self.assertIsNone(links.EarlyResponses.install(types.SimpleNamespace(handle_response=lambda *args: None)))
        self.assertIsNotNone(links.EarlyResponses.install(types.SimpleNamespace(pending_requests=[],
                                                                                handle_response=lambda *a: None)))


class PeerLinkSlotsTest(unittest.TestCase):
    """Requesters keep within MAX_REQUESTS_PER_LINK, at most one of them /blocks."""

    def setUp(self):
        """A PeerLink over an active fake link."""
        patcher = mock.patch.object(links, "RNS", fake_rns())
        patcher.start()
        self.addCleanup(patcher.stop)
        self.peer = links.PeerLink(NODE, identified=False, shared=True)
        self.peer.link = types.SimpleNamespace(status=FakeLink.ACTIVE)

    def test_two_requests_at_most(self):
        self.assertTrue(self.peer.acquire(False))
        self.assertTrue(self.peer.acquire(False))
        self.assertFalse(self.peer.acquire(False, timeout=0.1))
        self.peer.release(False)
        self.assertTrue(self.peer.acquire(False, timeout=0.1))

    def test_one_blocks_at_most(self):
        self.assertTrue(self.peer.acquire(True))
        self.assertFalse(self.peer.acquire(True, timeout=0.1))
        self.assertTrue(self.peer.acquire(False, timeout=0.1))

    def test_waiter_gets_slot_when_released(self):
        self.peer.acquire(True)
        got = []
        thread = threading.Thread(target=lambda: got.append(self.peer.acquire(True, timeout=5)))
        with mock.patch.object(self.peer.cond, "wait", wraps=self.peer.cond.wait) as wait:
            thread.start()
            self.assertTrue(wait_for(lambda: wait.called))  # it waits for the slot
        self.peer.release(True)
        thread.join(5)
        self.assertEqual(got, [True])

    def test_waiter_fails_when_link_closes(self):
        self.peer.acquire(True)
        got = []
        thread = threading.Thread(target=lambda: got.append(self.peer.acquire(True)))
        with mock.patch.object(self.peer.cond, "wait", wraps=self.peer.cond.wait) as wait:
            thread.start()
            self.assertTrue(wait_for(lambda: wait.called))  # it waits for the slot
        self.peer.mark_closed()
        thread.join(5)
        self.assertEqual(got, [False])

    def test_rate_recorded_from_large_responses_only(self):
        exchange = links.Exchange(time.monotonic)
        exchange.response_size, exchange.expected_rate = 1000, 5000.0
        self.assertFalse(self.peer.record_rate(exchange, 16384))
        exchange.response_size = 20000
        self.assertTrue(self.peer.record_rate(exchange, 16384))
        self.assertEqual(self.peer.rate, 5000.0)


if __name__ == "__main__":
    unittest.main()
