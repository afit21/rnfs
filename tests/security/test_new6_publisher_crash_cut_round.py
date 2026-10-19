"""NEW-6 (publisher half): records stored during a publish round cut short by a crash were never withdrawn.

Where a round stored a CID was only saved at the round's end, so after a crash mid-round nothing listed those
nodes, and when the CID later stopped being provided they kept the record for up to PROVIDER_TTL (section 6
SHOULD: withdraw from nodes of a round cut short). These tests crash a round from the fake client after some
nodes accepted, restart the publisher on the same state directory, stop providing the CID, and check that every
node saved as accepted (every SAVE_EVERY_NODES nodes or SAVE_EVERY_SECONDS) gets a withdrawal.
"""
import os
import tempfile
import unittest

import RNS

from rnfs import publisher as publisher_module
from rnfs.config import Config
from rnfs.content import builder
from rnfs.content.storage import USER, Storage
from rnfs.dht.providers import ProviderStore
from rnfs.publisher import Publisher
from tests.support import FakeClock
from tests.test_publisher import FakeClient, FakeLookup

OWN = bytes(16)
# Read this way so the tests also run (and fail for the stated reason) on code from before the fix
SAVE_EVERY_NODES = getattr(publisher_module, "SAVE_EVERY_NODES", 10)
SAVE_EVERY_SECONDS = getattr(publisher_module, "SAVE_EVERY_SECONDS", 30.0)

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the publisher's log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class Crash(BaseException):
    """Stands in for the process dying in the middle of a round."""


class CrashingClient(FakeClient):
    """A FakeClient that dies before request number crash_at, advancing the clock by step per request."""

    def __init__(self, clock: FakeClock, crash_at: int, step: float = 0.0):
        """Crash before the crash_at'th request (0-based)."""
        super().__init__()
        self.clock = clock
        self.crash_at = crash_at
        self.step = step

    def provide(self, node_id, keys, withdrawn):
        """Record the request, or crash."""
        if len(self.calls) == self.crash_at:
            raise Crash()
        self.clock.now += self.step
        return super().provide(node_id, keys, withdrawn)


class CrashCutRoundTest(unittest.TestCase):
    """A round over many nodes, crashed part way, then a restart that stops providing the CID."""

    def setUp(self):
        """Storage with one pinned CID; a temporary state directory."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = os.path.join(self.tmp.name, "state")
        self.config = Config(rnfs_dir=self.tmp.name, republish_interval=1000)
        self.storage = Storage(os.path.join(self.tmp.name, "blocks"), self.state, 10**9)
        self.addCleanup(self.storage.close)
        self.clock = FakeClock(1_000_000.0)
        self.cid = builder.build_bytes(b"some content", self.storage.put).cid
        self.storage.pin(self.cid, USER)

    def publisher(self, client, nodes) -> Publisher:
        """A publisher on the shared state directory whose lookups find nodes (closed when the test ends)."""
        publisher = Publisher(OWN, self.config, self.storage, ProviderStore(None), client,
                              lambda key: FakeLookup(nodes), False, self.state, clock=self.clock)
        self.addCleanup(publisher.close)
        return publisher

    def crash_then_unpin(self, nodes, crash_at: int, step: float = 0.0) -> set[bytes]:
        """Crash the first round before request crash_at; restart, unpin, publish; the nodes withdrawn from."""
        first = self.publisher(CrashingClient(self.clock, crash_at, step), nodes)
        first.refresh()
        with self.assertRaises(Crash):
            first.publish_due()
        # The crashed publisher is abandoned: only what it saved survives
        client = FakeClient()
        again = self.publisher(client, nodes)
        again.refresh()
        self.storage.unpin(self.cid, USER)
        again.refresh()
        again.publish_due()
        return {node_id for node_id, _, withdrawn in client.calls if self.cid.key in withdrawn}

    def test_nodes_saved_every_few_nodes_are_withdrawn_from_after_a_crash(self):
        """Crash after SAVE_EVERY_NODES + 2 nodes accepted: at least the first SAVE_EVERY_NODES are withdrawn from."""
        nodes = [bytes([i]) * 16 for i in range(1, SAVE_EVERY_NODES + 6)]
        withdrawn = self.crash_then_unpin(nodes, crash_at=SAVE_EVERY_NODES + 2)
        # NEW-6: fails until accepted nodes are recorded (and saved) as each /provide returns
        self.assertLessEqual(set(nodes[:SAVE_EVERY_NODES]), withdrawn)
        self.assertLessEqual(withdrawn, set(nodes[:SAVE_EVERY_NODES + 2]))  # never a node it didn't store on

    def test_nodes_saved_after_some_seconds_are_withdrawn_from_after_a_crash(self):
        """A slow round (each /provide takes over SAVE_EVERY_SECONDS) crashed after two nodes: both are kept."""
        nodes = [bytes([i]) * 16 for i in range(1, 5)]
        withdrawn = self.crash_then_unpin(nodes, crash_at=2, step=SAVE_EVERY_SECONDS + 1)
        # NEW-6: fails until accepted nodes are saved at least every SAVE_EVERY_SECONDS during a round
        self.assertEqual(withdrawn, set(nodes[:2]))


if __name__ == "__main__":
    unittest.main()
