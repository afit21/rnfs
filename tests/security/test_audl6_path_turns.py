"""AUD-L6 (0.2.1 audit): path-request turns given up by their waiters were never given back.

The attack (or just an announce burst): PathRequests._take_turn reserves each waiter a turn, moving _next_turn a spacing
on, and a waiter whose path arrives meanwhile (an announce) leaves without sending anything, but its turn stayed
reserved. So every caller after it waited behind turns nobody would take: in the auditor's repro 30 waiters that left
delayed a new caller by 5.6 s at 0.2 s spacing (about 28 s at the default 1 s), and anyone able to have the node wait
for paths it then hears announced (a /find reply naming announced nodes) could stretch every later path request.

The tests rebuild it twice: in virtual time, waiters one after another whose paths are announced as soon as they
start waiting for their turn; and with threads and a short spacing, as the auditor did, all waiting at once and all
leaving when their paths arrive together. Either way a new caller must then get its turn within one spacing.

The fix (PathRequests._take_turn, _give_back): each reservation is kept (_turns, in order) until it is taken or given
up. A waiter leaving before taking its turn gives it up, and while the last reservation is one given up, _next_turn
goes back to its time, so turns given up at the end of the queue are free again. A turn given up between two that are
kept leaves its gap until the later ones are taken or given up: each kept turn still goes no sooner than it would have.
"""
import threading
import time
import types
import unittest

from rnfs.network import paths
from tests.fakes.log import quiet_log
from tests.support import FakeClock, wait_for
from tests.test_paths import FakeTransport

setUpModule, tearDownModule = quiet_log()


class AbandonedTurnsTest(unittest.TestCase):
    def test_turns_given_up_one_after_another(self):
        clock = FakeClock()
        transport = FakeTransport(clock)
        reticulum = types.SimpleNamespace(get_interface_stats=lambda: {}, is_connected_to_shared_instance=False)
        waiting = []

        def sleep(seconds):
            """A waiter's path is announced while it waits for its turn; once none is left, time passes."""
            if waiting:
                transport.paths.add(waiting.pop())
            else:
                clock.now += seconds
        budget = paths.PathRequests(transport=lambda: transport, reticulum=lambda: reticulum, clock=clock,
                                    sleep=sleep)
        self.assertTrue(budget.await_path(bytes([255]) * 16, 30))  # takes the first turn
        for i in range(30):
            destination = bytes([i]) * 16
            waiting.append(destination)
            self.assertTrue(budget.await_path(destination, 30))  # its path came: nothing sent
        self.assertEqual(len(transport.requests), 1)
        start = clock.now
        self.assertTrue(budget.await_path(bytes([254]) * 16, 30))
        self.assertLessEqual(transport.requests[-1][0] - start, budget.spacing,
                             "a new path request waited behind turns given up")

    def test_turns_given_up_together(self):
        class Transport:
            """RNS.Transport whose paths arrive when the test says."""

            def __init__(self):
                """No paths known, no requests sent."""
                self.known = set()
                self.sent = []

            def has_path(self, destination):
                """True once destination's path has arrived."""
                return destination in self.known

            def request_path(self, destination, **kwargs):
                """Record a path request."""
                self.sent.append((time.monotonic(), destination))

            def await_path(self, destination, timeout=None, **kwargs):
                """A request, then the path if it arrives within timeout."""
                self.request_path(destination)
                return wait_for(lambda: destination in self.known, timeout)

        transport = Transport()
        spacing = 0.05
        budget = paths.PathRequests(transport=lambda: transport, reticulum=lambda: None, spacing=spacing)
        waiters = [bytes([i]) * 16 for i in range(30)]
        threads = [threading.Thread(target=budget.await_path, args=(destination, 2), daemon=True)
                   for destination in waiters]
        for thread in threads:
            thread.start()
        wait_for(lambda: budget._next_turn - time.monotonic() > 20 * spacing, what="the waiters' turns reserved")
        transport.known.update(waiters)  # an announce burst: every waiter still queued leaves
        for thread in threads:
            thread.join(10)
        self.assertLess(len(transport.sent), len(waiters))
        start = time.monotonic()
        budget.await_path(b"\xee" * 16, 0.01)
        sent_at = transport.sent[-1][0]
        self.assertEqual(transport.sent[-1][1], b"\xee" * 16)
        self.assertLess(sent_at - start, 5 * spacing, "a new path request waited behind turns given up")


if __name__ == "__main__":
    unittest.main()
