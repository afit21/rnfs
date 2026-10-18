"""Tests for the node's event log (rnfs/events.py), which GET /api/events serves."""
import threading
import time
import unittest

from rnfs import events
from rnfs.events import EventLog


class EventLogTest(unittest.TestCase):
    """EventLog.since: level-triggered, numbered, bounded, and honest about what it no longer holds."""

    def test_baseline_then_events_in_order(self):
        """after None gives where to start; then every event after it, in order, numbered from 1."""
        log = EventLog(clock=lambda: 5.0)
        self.assertEqual(log.since(None), {"events": [], "next": 0, "lost": False, "epoch": log.epoch})
        log.publish("pin", change="added")
        log.publish("job", id="a")
        result = log.since(0)
        self.assertEqual([(event["seq"], event["kind"], event["time"]) for event in result["events"]],
                         [(1, "pin", 5.0), (2, "job", 5.0)])
        self.assertEqual((result["next"], result["lost"]), (2, False))
        self.assertEqual(log.since(2)["events"], [])
        self.assertEqual([event["seq"] for event in log.since(1, kinds=["job"])["events"]], [2])

    def test_unknown_kind_refused(self):
        """Only the documented kinds are published."""
        with self.assertRaises(ValueError):
            EventLog().publish("config")

    def test_falling_behind_is_reported(self):
        """A client after a sequence the log no longer holds gets lost and everything held."""
        log = EventLog(max_events=3)
        for n in range(5):
            log.publish("peer", n=n)
        result = log.since(1)
        self.assertTrue(result["lost"])
        self.assertEqual([event["seq"] for event in result["events"]], [3, 4, 5])
        self.assertFalse(log.since(2)["lost"])  # 3 onwards is all held

    def test_restart_is_reported(self):
        """A sequence beyond the log's, or another epoch, means the daemon restarted: lost, and all held."""
        log = EventLog()
        log.publish("inbox", id=1)
        self.assertTrue(log.since(10)["lost"])
        result = log.since(1, epoch="another run")
        self.assertTrue(result["lost"])
        self.assertEqual(len(result["events"]), 1)
        self.assertFalse(log.since(1, epoch=log.epoch)["lost"])

    def test_wait_wakes_on_publish(self):
        """A waiting call returns as soon as an event comes, and with none after its wait."""
        log = EventLog()
        threading.Timer(0.2, lambda: log.publish("name", name="x")).start()
        began = time.monotonic()
        self.assertEqual(len(log.since(0, wait=5)["events"]), 1)
        self.assertLess(time.monotonic() - began, 4)
        began = time.monotonic()
        self.assertEqual(log.since(1, wait=0.2)["events"], [])
        self.assertGreaterEqual(time.monotonic() - began, 0.15)

    def test_listeners_and_publish_helper(self):
        """Listeners hear every event; one that raises doesn't stop the others; events.publish is a no-op for a
        node without a log and never raises."""
        log = EventLog()
        heard = []
        log.add_listener(lambda event: 1 / 0)
        log.add_listener(heard.append)
        log.publish("cache", change="removed")
        self.assertEqual([event["kind"] for event in heard], ["cache"])

        class Node:
            """A node with a log."""
            events = log

        events.publish(object(), "pin")
        events.publish(Node(), "not a kind")  # refused, never raised
        events.publish(Node(), "pin", change="added")
        self.assertEqual(log.last, 2)

    def test_listeners_hear_events_in_order(self):
        """Publishers on several threads at once: a listener hears every event, in sequence order (a Node's event
        watch skips a sequence lower than one it heard)."""
        log = EventLog()
        heard = []
        log.add_listener(lambda event: heard.append(event["seq"]))
        threads = [threading.Thread(target=lambda: [log.publish("job", n=n) for n in range(500)]) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(heard, list(range(1, 4001)))

    def test_lost_only_for_the_kinds_asked(self):
        """A client asking only for some kinds isn't told it lost events of other kinds the log dropped."""
        log = EventLog(max_events=2)
        log.publish("job", n=1)
        for n in range(5):
            log.publish("pin", n=n)
        self.assertTrue(log.since(0, kinds=["job"])["lost"])
        self.assertFalse(log.since(1, kinds=["job"])["lost"])
        self.assertTrue(log.since(1)["lost"])

    def test_cid_fields(self):
        """Many CIDs: the first MAX_EVENT_CIDS listed, all counted."""
        fields = events.cid_fields(range(events.MAX_EVENT_CIDS + 5))
        self.assertEqual((len(fields["cids"]), fields["count"]), (events.MAX_EVENT_CIDS, events.MAX_EVENT_CIDS + 5))


if __name__ == "__main__":
    unittest.main()
