"""NEW-1: a publish round could withdraw a CID from the nodes that had just stored it.

publish_due snapshots the pending withdrawals and then sends without the lock. A refresh() during the sends that
stops providing a CID queues withdrawals of its key; if the CID is provided again before the round ends, the round
records it as freshly published and the next round sends those withdrawals to the nodes holding the fresh records
(breaking section 6's MUST), and the node's own local record is gone too. These tests force the interleavings
through the fake client: each node holds the records its /provide requests leave (withdrawals, then stores), and
after the rounds settle every node the publisher thinks holds the CID must hold it, and so must the node itself.
"""
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.content import builder
from rnfs.content.storage import USER, Storage
from rnfs.dht.providers import ProviderStore
from rnfs import publisher as publisher_module
from rnfs.publisher import Publisher
from tests.support import FakeClock
from tests.test_publisher import FakeClient, FakeLookup

OWN = bytes(16)
NODES = [bytes([i]) * 16 for i in (1, 2, 3)]

_saved_loglevel = RNS.loglevel


def setUpModule():
    """Silence the publisher's log."""
    RNS.loglevel = RNS.LOG_CRITICAL


def tearDownModule():
    """Restore RNS's loglevel."""
    RNS.loglevel = _saved_loglevel


class HoldingClient(FakeClient):
    """A FakeClient whose nodes keep the records its requests leave, and which runs a hook before a request."""

    def __init__(self):
        """No records held, no hook."""
        super().__init__()
        self.held: dict[bytes, set[bytes]] = {}
        self.before = lambda index: None  # called with the request's index in self.calls, before it is applied
        self.before_node = lambda node_id: None  # called with the node a request goes to, before it is applied

    def provide(self, node_id, keys, withdrawn):
        """Run the hooks, then apply the request as a responder does: withdrawals first, then stores."""
        self.before(len(self.calls))
        self.before_node(node_id)
        recorded = super().provide(node_id, keys, withdrawn)
        held = self.held.setdefault(node_id, set())
        held -= set(withdrawn)
        held |= recorded
        return recorded


class WithdrawRaceTest(unittest.TestCase):
    """Unpinning and re-pinning a CID while a republish round sends its /provide requests."""

    def setUp(self):
        """A publisher over real storage and a provider store; a pinned CID published once to NODES."""
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        state = os.path.join(self.tmp.name, "state")
        config = Config(rnfs_dir=self.tmp.name, republish_interval=1000)
        self.storage = Storage(os.path.join(self.tmp.name, "blocks"), state, 10**9)
        self.addCleanup(self.storage.close)
        self.records = ProviderStore(None)
        self.client = HoldingClient()
        self.clock = FakeClock(1_000_000.0)
        self.publisher = Publisher(OWN, config, self.storage, self.records, self.client,
                                   lambda key: FakeLookup(NODES), False, state, clock=self.clock)
        self.addCleanup(self.publisher.close)
        self.cid = builder.build_bytes(b"some content", self.storage.put).cid
        self.storage.pin(self.cid, USER)
        self.publisher.refresh()
        self.publisher.publish_due()  # round 1: stored on every node
        self.clock.now += 1001  # republish due

    def unpin(self):
        """Stop providing the CID, as `pin rm` does."""
        self.storage.unpin(self.cid, USER)
        self.publisher.refresh()

    def repin(self):
        """Provide the CID again, as `pin add` does."""
        self.storage.pin(self.cid, USER)
        self.publisher.refresh()

    def settle(self):
        """Run rounds until nothing is due, as the publish loop does (at most a few)."""
        for _ in range(5):
            if self.publisher.next_due() > 0:
                return
            self.publisher.publish_due()
        self.fail("the publisher never settled")

    def problems(self) -> list[str]:
        """What is wrong once settled with the CID provided: records the publisher counts on that are gone."""
        key = self.cid.key
        found = []
        state = self.publisher.states().get(self.cid)  # the servers of its last round (none if cut short)
        for node_id in (state["nodes"] if state else []):
            if key not in self.client.held.get(node_id, set()):
                found.append(f"{node_id.hex()[:4]} was sent a withdrawal after storing it")
        if OWN not in self.records.providers(key):
            found.append("the node's own record is gone")
        return found

    # The interleavings below are of requests sent one at a time (as slow nodes send them), so a request's index
    # says what has been sent; test_unpin_and_repin_while_sending_in_parallel covers other nodes' parallel sends
    @mock.patch.object(publisher_module, "PROVIDE_PARALLEL", 1, create=True)
    def test_unpin_and_repin_during_the_first_send(self):
        """The reviewer's interleaving: both happen just before the round's first /provide."""
        def before(index):
            if index == len(NODES):  # the first request of round 2
                self.unpin()
                self.repin()
        self.client.before = before
        self.publisher.publish_due()  # round 2
        self.client.before = lambda index: None
        self.settle()
        self.assertIn(self.cid, self.publisher.provided())
        # NEW-1: fails until refresh() cancels the pending withdrawals of a key it provides again and re-records
        # the node's own record
        self.assertEqual(self.problems(), [])

    @mock.patch.object(publisher_module, "PROVIDE_PARALLEL", 1, create=True)
    def test_every_interleaving_of_unpin_and_repin_with_the_sends(self):
        """Unpin before request i and re-pin before request j >= i of round 2, or after it ends (j == end)."""
        first = len(NODES)  # requests of round 1
        failures = []
        for i in range(len(NODES)):
            for j in range(i, len(NODES) + 1):
                with self.subTest(unpin=i, repin=j):
                    self.setUp()

                    def before(index, i=i, j=j):
                        if index == first + i:
                            self.unpin()
                        if index == first + j:
                            self.repin()
                    self.client.before = before
                    self.publisher.publish_due()  # round 2
                    self.client.before = lambda index: None
                    if j == len(NODES):
                        self.repin()  # after round 2 ended
                    self.settle()
                    self.assertIn(self.cid, self.publisher.provided())
                    found = self.problems()
                    if found:
                        failures.append(f"unpin before send {i}, repin before {j}: {found}")
        # NEW-1: fails until the invariant holds for every interleaving
        self.assertEqual(failures, [])

    def test_unpin_and_repin_while_sending_in_parallel(self):
        """Every node's request in flight at once (other nodes send up to PROVIDE_PARALLEL at once): unpinning and
        re-pinning just before any one node's request still leaves every node the publisher counts on holding it."""
        failures = []
        for target in NODES:
            with self.subTest(before=target.hex()[:4]):
                self.setUp()

                def before_node(node_id, target=target):
                    if node_id == target:
                        self.unpin()
                        self.repin()
                self.client.before_node = before_node
                with mock.patch.object(publisher_module, "PROVIDE_PARALLEL", len(NODES), create=True):
                    self.publisher.publish_due()  # round 2
                self.client.before_node = lambda node_id: None
                self.settle()
                self.assertIn(self.cid, self.publisher.provided())
                found = self.problems()
                if found:
                    failures.append(f"unpin and repin before {target.hex()[:4]}: {found}")
        self.assertEqual(failures, [])

    @mock.patch.object(publisher_module, "PROVIDE_PARALLEL", 1, create=True)
    def test_no_withdrawal_follows_a_store_to_the_same_node(self):
        """Section 6's MUST, read off the requests: once settled with the CID provided, no node got a withdrawal
        of its key in a later request than a store of it."""
        def before(index):
            if index == len(NODES) + 1:  # round 2's second request: the first node already has the fresh record
                self.unpin()
                self.repin()
        self.client.before = before
        self.publisher.publish_due()
        self.client.before = lambda index: None
        self.settle()
        key = self.cid.key
        late = []
        for node_id in NODES:
            stored_at = [n for n, (target, keys, _) in enumerate(self.client.calls) if target == node_id and key in keys]
            withdrawn_at = [n for n, (target, keys, withdrawn) in enumerate(self.client.calls)
                            if target == node_id and key in withdrawn and key not in keys]
            if withdrawn_at and stored_at and max(withdrawn_at) > max(stored_at):
                late.append(node_id.hex()[:4])
        # NEW-1: fails until a withdrawal queued before the CID was provided again is cancelled
        self.assertEqual(late, [], "withdrawn after the last store while still provided")


if __name__ == "__main__":
    unittest.main()
