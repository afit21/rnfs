"""The node's path-request budget (rnfs/network/paths.py, specification section 3, "Path requests"), over a fake
RNS.Transport: requests spaced, the wait outside path_timeout, one wait per destination, nothing sent again within
PATH_REQUEST_MI, failed links charged, the busiest interface's outgoing estimate as a ceiling, and which waits ended
without a path."""
import threading
import types
import unittest

from rnfs.network import paths
from tests.fakes.log import quiet_log
from tests.support import FakeClock, wait_for

A, B, C = bytes([1]) * 16, bytes([2]) * 16, bytes([3]) * 16
setUpModule, tearDownModule = quiet_log()


class FakeTransport:
    """RNS.Transport as the budget uses it: has_path from .paths, await_path answering with answer(destination) (default:
    a path arrives at once) and recording each request with the clock's time."""

    def __init__(self, clock):
        """No paths known."""
        self.clock = clock
        self.paths: set[bytes] = set()
        self.requests: list[tuple[float, bytes]] = []  # (when, destination)
        self.timeouts: list[float] = []
        self.answer = lambda destination: True

    def has_path(self, destination):
        """True if a path to destination is known."""
        return destination in self.paths

    def request_path(self, destination, on_interface=None, tag=None):
        """Record a path request."""
        self.requests.append((self.clock(), destination))

    def await_path(self, destination, timeout=None, on_interface=None):
        """RNS's await_path: a request, then the answer (a path, or none after timeout)."""
        self.request_path(destination, on_interface)
        self.timeouts.append(timeout)
        if self.answer(destination):
            self.paths.add(destination)
            return True
        return False


def stats(*interfaces) -> dict:
    """Reticulum.get_interface_stats() of interfaces given as dicts."""
    return {"interfaces": [{"type": "TCPClientInterface", "status": True, **entry} for entry in interfaces]}


class BudgetTest(unittest.TestCase):
    """PathRequests in virtual time."""

    def setUp(self):
        """A budget over a fake transport, its waits advancing a fake clock."""
        self.clock = FakeClock()
        self.transport = FakeTransport(self.clock)
        self.stats = stats()
        reticulum = types.SimpleNamespace(get_interface_stats=lambda: self.stats, is_connected_to_shared_instance=False)
        self.slept = []

        def sleep(seconds):
            """Advance virtual time."""
            self.slept.append(seconds)
            self.clock.now += seconds
        self.budget = paths.PathRequests(transport=lambda: self.transport, reticulum=lambda: reticulum,
                                         clock=self.clock, sleep=sleep)

    def times(self) -> list[float]:
        """When each request went, from the first."""
        start = self.transport.requests[0][0]
        return [round(when - start, 6) for when, _ in self.transport.requests]

    def test_requests_are_spaced(self):
        destinations = [bytes([i]) * 16 for i in range(1, 6)]
        for destination in destinations:
            self.assertTrue(self.budget.await_path(destination, 30))
        self.assertEqual(self.times(), [0, 1, 2, 3, 4])
        self.assertEqual(self.budget.requests, 5)
        self.assertAlmostEqual(self.budget.waited, 4)
        # A path already known needs no request, and no turn
        self.assertTrue(self.budget.await_path(A, 30))
        self.assertEqual(len(self.transport.requests), 5)

    def test_a_quiet_budget_sends_at_once(self):
        self.budget.await_path(A, 30)
        self.clock.now += 5
        self.budget.await_path(B, 30)
        self.assertEqual(self.times(), [0, 5])

    def test_the_wait_for_a_turn_is_not_part_of_path_timeout(self):
        self.transport.answer = lambda destination: False
        for destination in (A, B, C):
            self.assertFalse(self.budget.await_path(destination, 30))
        # Each request waited for its turn first, and then had the whole path_timeout
        self.assertEqual(self.transport.timeouts, [30, 30, 30])

    def test_a_path_heard_while_waiting_sends_nothing(self):
        self.budget.await_path(A, 30)
        # B's path arrives (an announce) while B waits for its turn
        original = self.budget.sleep
        self.budget.sleep = lambda seconds: (original(seconds), self.transport.paths.add(B))
        self.assertTrue(self.budget.await_path(B, 30))
        self.assertEqual([destination for _, destination in self.transport.requests], [A])

    def test_nothing_sent_again_within_path_request_mi(self):
        self.transport.answer = lambda destination: False
        self.assertFalse(self.budget.await_path(A, 5))
        self.clock.now += 1
        # Within 20 s of A's request: the answer may still come, so it is waited for, not asked again
        self.transport.answer = lambda destination: True
        original = self.budget.sleep
        self.budget.sleep = lambda seconds: (original(seconds), self.transport.paths.add(A))
        self.assertTrue(self.budget.await_path(A, 5))
        self.assertEqual(len(self.transport.requests), 1)
        self.assertEqual(self.budget.reused, 1)
        # Later it is asked again
        self.budget.sleep = original
        self.transport.paths.clear()
        self.clock.now += paths.PATH_REQUEST_MI
        self.assertTrue(self.budget.await_path(A, 5))
        self.assertEqual(len(self.transport.requests), 2)

    def test_a_reused_wait_still_has_its_timeout(self):
        self.transport.answer = lambda destination: False
        self.budget.await_path(A, 5)
        started = self.clock.now
        self.assertFalse(self.budget.await_path(A, 5))
        self.assertAlmostEqual(self.clock.now - started, 5)

    def test_a_failed_link_costs_a_turn(self):
        self.budget.await_path(A, 30)
        self.budget.charge(B)  # RNS asks for B's path again by itself
        self.budget.await_path(C, 30)
        self.assertEqual(self.times(), [0, 2])
        # And B's path isn't asked again within PATH_REQUEST_MI of RNS's own request
        self.transport.paths.discard(B)
        self.transport.answer = lambda destination: False
        self.assertFalse(self.budget.await_path(B, 3))
        self.assertEqual(len(self.transport.requests), 2)
        self.assertEqual(self.budget.charged, 1)

    def test_identity_wait(self):
        # A path but no identity: requested in its turn, then the identity polled for
        self.transport.paths.add(A)
        self.budget.await_path(B, 30)

        def known() -> bool:
            """A's identity arrives 0.3 s after its path request."""
            asked = [when for when, destination in self.transport.requests if destination == A]
            return bool(asked) and self.clock.now >= asked[0] + 0.3
        self.assertTrue(self.budget.await_known(A, known, 30))
        self.assertEqual(self.times(), [0, 1])
        self.assertFalse(self.budget.await_known(C, lambda: False, 2))
        self.assertEqual(len(self.transport.requests), 3)

    def test_lacked_tells_a_failure_for_want_of_a_path(self):
        self.transport.answer = lambda destination: destination != A
        self.assertFalse(self.budget.await_path(A, 5))
        self.assertTrue(self.budget.await_path(B, 5))
        self.assertTrue(self.budget.lacked(A))
        self.assertFalse(self.budget.lacked(B) or self.budget.lacked(C))
        self.transport.paths.add(A)  # heard since: no longer lacked
        self.assertTrue(self.budget.await_path(A, 5))
        self.assertFalse(self.budget.lacked(A))
        self.assertFalse(self.budget.await_known(C, lambda: False, 2))  # an identity that never came
        self.assertTrue(self.budget.lacked(C))

    def test_names_take_their_turn(self):
        self.budget.await_path(A, 30)
        self.assertTrue(self.budget.send(B))
        self.assertTrue(self.budget.send(B))  # a name's request isn't a wait: no PATH_REQUEST_MI rule
        self.assertEqual(self.times(), [0, 1, 2])

    def test_cancel_ends_a_wait_for_a_turn(self):
        self.budget.await_path(A, 30)
        cancel = threading.Event()
        original = self.budget.sleep
        self.budget.sleep = lambda seconds: (original(seconds), cancel.set())
        self.assertFalse(self.budget.await_path(B, 30, cancel=cancel))
        self.assertFalse(self.budget.send(C, cancel=cancel))
        self.assertEqual(len(self.transport.requests), 1)

    def test_ceiling_holds_requests_while_the_busiest_interface_is_busy(self):
        self.stats = stats({"outgoing_pr_frequency": 0.4}, {"outgoing_pr_frequency": 4.0})
        cleared = self.clock.now + 3.5

        def busy_until_cleared(seconds):
            """Virtual time passes; the other application's burst ends at cleared."""
            self.clock.now += seconds
            if self.clock.now >= cleared:
                self.stats = stats({"outgoing_pr_frequency": 0.4})
        self.budget.sleep = busy_until_cleared
        self.assertTrue(self.budget.await_path(A, 30))
        self.assertEqual(self.transport.requests[0][0], 1004.0)  # the first turn after the burst ended
        self.assertEqual(self.budget.held, 1)

    def test_ceiling_holds_a_request_for_ceiling_hold_at_most(self):
        self.stats = stats({"outgoing_pr_frequency": 9.0})
        started = self.clock.now
        self.assertTrue(self.budget.await_path(A, 30))
        self.assertAlmostEqual(self.transport.requests[0][0] - started, paths.CEILING_HOLD)

    def test_stats_that_cant_be_read_hold_nothing(self):
        self.budget._reticulum = lambda: types.SimpleNamespace(get_interface_stats=lambda: 1 / 0)
        self.assertTrue(self.budget.await_path(A, 30))
        self.budget._reticulum = lambda: None
        self.assertTrue(self.budget.await_path(B, 30))
        self.assertEqual(self.times(), [0, 1])


class BusiestTest(unittest.TestCase):
    """busiest_pr_frequency over get_interface_stats()."""

    def test_counts_interfaces_a_request_leaves_by(self):
        server = {"hash": b"s", "type": "TCPServerInterface", "outgoing_pr_frequency": 40.0}  # a copy per client
        clients = [{"hash": bytes([i]), "parent_interface_hash": b"s", "outgoing_pr_frequency": 1.0 + i / 10}
                   for i in range(3)]
        others = [{"type": "LocalServerInterface", "outgoing_pr_frequency": 9.0},
                  {"type": "LocalClientInterface", "outgoing_pr_frequency": 9.0},
                  {"status": False, "outgoing_pr_frequency": 9.0},
                  {"outgoing_pr_frequency": float("nan")}, {"outgoing_pr_frequency": True},
                  {"outgoing_pr_frequency": "9"}, {}]
        self.assertAlmostEqual(paths.busiest_pr_frequency(stats(server, *clients, *others)), 1.2)
        for broken in (None, {}, {"interfaces": None}, {"interfaces": ["x", 3]}):
            self.assertEqual(paths.busiest_pr_frequency(broken), 0.0)


class ConcurrentTest(unittest.TestCase):
    """PathRequests across threads, in real time with a short spacing."""

    def test_one_wait_per_destination(self):
        transport = FakeTransport(lambda: 0.0)
        arrived = threading.Event()
        transport.answer = lambda destination: arrived.wait(5)
        budget = paths.PathRequests(transport=lambda: transport, reticulum=lambda: None, spacing=0.01)
        results = []
        threads = [threading.Thread(target=lambda: results.append(budget.await_path(A, 5))) for _ in range(4)]
        for thread in threads:
            thread.start()
        wait_for(lambda: budget.joined == 3, what="the later waits to join the first")
        arrived.set()
        for thread in threads:
            thread.join(5)
        self.assertEqual(results, [True] * 4)
        self.assertEqual(len(transport.requests), 1)

    def test_a_burst_is_spaced(self):
        times = []
        lock = threading.Lock()
        transport = FakeTransport(lambda: 0.0)

        def await_path(destination, timeout=None, on_interface=None):
            """Record when each request went."""
            import time
            with lock:
                times.append(time.monotonic())
            return True
        transport.await_path = await_path
        budget = paths.PathRequests(transport=lambda: transport, reticulum=lambda: None, spacing=0.05)
        threads = [threading.Thread(target=budget.await_path, args=(bytes([i]) * 16, 5)) for i in range(1, 7)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        times.sort()
        self.assertEqual(len(times), 6)
        self.assertTrue(all(later - earlier >= 0.045 for earlier, later in zip(times, times[1:])), times)


if __name__ == "__main__":
    unittest.main()
