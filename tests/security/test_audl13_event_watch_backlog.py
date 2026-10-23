"""AUD-L13: an embedded node's event watch queued events without bound behind a slow callback.

rnfs.Node's events.watch hands each event to the event thread under a key of its own (Rnfs._deliver_event,
_Events.post), never coalesced, so a callback slower than the events let them pile up: a closure and an Event each,
with no bound. Remote announces drive some kinds (peers coming online, names heard, fetches cached), so other nodes
could make the host program's memory grow for as long as its callback lagged. The daemon's own event log keeps
MAX_EVENTS (1024) and answers "lost" to a client further behind (GET /api/events), which a Client's watch delivers
as a "lost" Event.

The test blocks a watch's callback on its first event, publishes several times MAX_EVENTS events on an offline
node's log, and counts what waits on the event thread. The fixed watch keeps at most MAX_EVENTS deliveries queued:
past that its oldest are dropped and a "lost" Event (seq: the last one dropped, fields: the run's epoch) waits in
their place, as a Client's watch gets when it falls behind; then the newest events follow, in order.
"""
import os
import tempfile
import threading
import unittest

import RNS

from rnfs.config import Config
from rnfs.events import MAX_EVENTS
from rnfs.node import RnfsNode
from tests.support import wait_for
from tests.test_names import FakeNetwork
from tests.test_node import N1, NetClient
from tests.test_public import OfflineNode

PUBLISHED = 3 * MAX_EVENTS  # events published while the callback is blocked


class EventWatchBacklogTest(unittest.TestCase):
    """An event watch on an offline node whose callback blocks on its first event."""

    def setUp(self):
        """The node, rnfs.Node over it, and RNS's log silenced."""
        saved = RNS.loglevel
        RNS.loglevel = RNS.LOG_CRITICAL
        self.addCleanup(setattr, RNS, "loglevel", saved)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.node = RnfsNode.offline(Config(rnfs_dir=os.path.join(tmp.name, "n"), slow="no"), N1, NetClient(),
                                     FakeNetwork())
        self.addCleanup(self.node.stop)
        self.rnfs = OfflineNode(self.node)
        self.addCleanup(self.rnfs.close)

    def publish(self, index: int) -> dict:
        """A "peer" event, as a peer's announce brings."""
        return self.node.events.publish("peer", node_id=f"{index:032x}", change="new", online=True)

    def test_a_slow_callback_gets_lost_then_the_newest(self):
        gate = threading.Event()
        self.addCleanup(gate.set)
        heard = []

        def callback(event):
            """Record the event; block on the first until the test lets it go."""
            heard.append(event)
            if len(heard) == 1:
                gate.wait(30)

        self.rnfs.events.watch(callback, kinds="peer")
        self.publish(0)
        wait_for(lambda: heard, what="the first event")
        last = [self.publish(index)["seq"] for index in range(1, PUBLISHED + 1)][-1]
        queued = len(self.rnfs._events._pending)
        # AUD-L13: fails until a watch's queue is bounded
        self.assertLessEqual(queued, MAX_EVENTS, "the watch's queue grew without bound")
        gate.set()
        wait_for(lambda: heard[-1].seq == last, what="the last event")
        lost = heard[1]
        self.assertTrue(lost.lost)
        self.assertEqual(lost.fields["epoch"], self.node.events.epoch)
        after = [event.seq for event in heard[2:]]
        self.assertFalse(any(event.lost for event in heard[2:]))
        self.assertEqual(after, list(range(lost.seq + 1, last + 1)))  # the newest, in order, none missing
        self.assertEqual(len(heard), 1 + MAX_EVENTS)  # the first, the marker and MAX_EVENTS - 1 events

    def test_a_watch_keeping_up_loses_nothing(self):
        """A callback that keeps up gets every event, no "lost"."""
        heard = []
        self.rnfs.events.watch(heard.append, kinds="peer")
        last = [self.publish(index)["seq"] for index in range(MAX_EVENTS // 4)][-1]
        wait_for(lambda: heard and heard[-1].seq == last, what="the last event")
        self.assertFalse(any(event.lost for event in heard))
        self.assertEqual(len(heard), MAX_EVENTS // 4)


if __name__ == "__main__":
    unittest.main()
