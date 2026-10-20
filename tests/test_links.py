"""Tests for LinkPool: shared and identified links, idle close, per-link request limits and retry, with a fake RNS."""
import threading
import time
import types
import unittest
from unittest import mock

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

    def test_request_failure(self):
        FakeLink.responder = staticmethod(lambda link, path, data: FAIL)
        peer = run(self, lambda: self.pool.get(NODE))
        self.assertFalse(run(self, lambda: self.pool.request(peer, "/find", b"k")).answered)

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
