"""L13-R: after the L13 fix, a claimed sender got at most HOLD_PER_SENDER places in the queue of messages from
senders whose identity isn't known yet, and once forgeries claiming a sender held them all, that sender's real
message was dropped (LXMF has already told the sender it was delivered).

An LxmfNetwork with a stand-in router and feature, as tests/security/test_l13_lxmf_hold_queue.py sets it up. Forged
messages claiming sender S fill S's places; S's real message must then take the place of the oldest of them.
"""
import os
import unittest

from rnfs.features import lxmf
from tests import test_lxmf
from tests.security import test_l13_lxmf_hold_queue as l13


def setUpModule():
    """Silence the node's log."""
    test_lxmf.setUpModule()


def tearDownModule():
    """Restore RNS's loglevel."""
    test_lxmf.tearDownModule()


class HoldReplaceTest(l13.HoldQueueTest):
    """The hold queue of HoldQueueTest; only the test below runs here."""

    def test_real_message_takes_the_place_of_the_oldest_forgery(self):
        claimed = os.urandom(16)
        for i in range(lxmf.HOLD_PER_SENDER):
            self.deliver(claimed, b"forged%d" % i)
        self.deliver(claimed, b"REAL")
        # L13-R: fails until a claimed sender's newest message takes the place of its oldest held one
        self.assertIn(b"REAL", self.tags(), "forgeries claiming the sender kept its real message out")
        self.assertNotIn(b"forged0", self.tags())
        self.assertEqual(len(self.network._held), lxmf.HOLD_PER_SENDER)


for _name in [name for name in dir(l13.HoldQueueTest) if name.startswith("test_")]:
    setattr(HoldReplaceTest, _name, None)  # they run in their own module, not again here


if __name__ == "__main__":
    unittest.main()
