"""LXMF auto-fetch fairness (earlier review, code-read): one worker ran every auto-fetch in arrival order from one
shared 64-entry queue, so one allowlisted sender with many links delayed every other sender's auto-fetches, and
once it had filled the queue their links were skipped.

Node B runs the lxmf feature on a stand-in network (tests/test_lxmf.py's FeatureTestCase) with two senders in
lxmf_auto_fetch_from. The worker's fetch is replaced by a recorder, so the tests see the order fetches would run
in. A sender's first link must run right after the fetch in progress, not after the other sender's backlog
(round-robin between senders), and a sender with nothing queued must get a place in a full queue, from the sender
holding the most.
"""
import unittest
from unittest import mock

from rnfs.features import lxmf
from tests import test_lxmf
from tests.test_lxmf import OTHER, SENDER


def setUpModule():
    """Silence the node's log."""
    test_lxmf.setUpModule()


def tearDownModule():
    """Restore RNS's loglevel."""
    test_lxmf.tearDownModule()


class AutoFetchFairnessTest(test_lxmf.FeatureTestCase):
    """SENDER and OTHER both in lxmf_auto_fetch_from; fetches recorded, not run."""

    def setUp(self):
        """B's feature, its fetches recorded by sender."""
        super().setUp()
        self.feature = self.feature_node(lxmf_auto_fetch_from=[SENDER.hex(), OTHER.hex()])
        self.ran: list[str] = []
        self.feature.auto_fetch = lambda entry_id, index: self.ran.append(self.feature.inbox.get(entry_id)["from"])

    def test_one_senders_backlog_does_not_delay_another(self):
        for number in range(10):
            self.feature.received(self.link_message(source=SENDER, number=number))
        self.feature.received(self.link_message(source=OTHER, number=99))
        self.feature.process_auto_fetches()
        # Fails until auto-fetches are taken round-robin by sender
        self.assertEqual(self.ran.index(OTHER.hex()), 1,
                         f"OTHER's link waited behind {self.ran.index(OTHER.hex())} of SENDER's: {self.ran}")
        self.assertEqual(self.ran.count(SENDER.hex()), 10)

    def test_full_queue_makes_room_for_another_sender(self):
        with mock.patch.object(lxmf, "AUTO_QUEUE_LIMIT", 4):
            ids = [self.feature.received(self.link_message(source=SENDER, number=n)) for n in range(6)]
            other = self.feature.received(self.link_message(source=OTHER, number=99))
        states = [self.feature.inbox.get(i)["auto"][0]["state"] for i in ids]
        self.assertEqual(states[4:], ["skipped", "skipped"])  # one sender alone still gets the whole queue
        # Fails until a sender with fewer links queued takes a place from the one holding the most
        self.assertEqual(self.feature.inbox.get(other)["auto"][0]["state"], "queued",
                         "a full queue of one sender's links skipped another sender's")
        self.assertEqual(states[:4], ["queued", "queued", "queued", "skipped"])  # SENDER's newest gave way
        self.feature.process_auto_fetches()
        self.assertEqual(sorted(self.ran), sorted([SENDER.hex()] * 3 + [OTHER.hex()]))

    def test_restart_requeues_round_robin(self):
        for number in range(3):
            self.feature.received(self.link_message(source=SENDER, number=number))
        self.feature.received(self.link_message(source=OTHER, number=99))
        self.feature._auto_queue.clear()
        self.feature._requeue_auto_fetches()
        self.feature.process_auto_fetches()
        self.assertEqual(self.ran[:2], [SENDER.hex(), OTHER.hex()])


if __name__ == "__main__":
    unittest.main()
