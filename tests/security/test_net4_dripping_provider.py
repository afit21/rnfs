"""NET-4: a provider could hold one of our fetches for hours by dripping a /blocks response. A response that keeps
making progress never stalls, and its deadline, with no rate estimate, assumes unmeasured_bitrate (100 bit/s): 64 KiB
takes about 4.4 h. Once every other block had arrived the fetch waited for it, its other providers idle.

Fixed with end-game duplication, requester-side (spec section 7, item 7, a MAY): once nothing is left to ask for and
a provider is idle, a batch in flight for longer than deadline_slack is asked of it too. The first verified copy
wins; the slow request's link is then closed, unless another fetch has a request on it (then the request is left
to finish), and its outcome doesn't count against its provider (no stall, no failure). The remote load stays bounded: nothing is asked twice before deadline_slack, while blocks are still to be asked for, or
on a slow node (one provider at a time), and each request is duplicated at most once.
"""
import os
import threading
import time
import unittest

from rnfs.constants import CHUNK_SIZE
from rnfs.content import builder, walker
from rnfs.content.dag import Entry
from rnfs.network.client import FAILED, STALLED, BlocksResult
from tests.fakes.client import FakeClient
import tests.test_fetcher as fetcher_tests  # a module, so its test cases don't run twice

P1, P2 = fetcher_tests.P1, fetcher_tests.P2
DRIP = 5.0  # seconds the dripping provider takes over its response, unless its link is closed first


class DrippingProviderTest(fetcher_tests.FetcherTest):
    """P1 drips its response for one file's first chunk; P2 serves everything at once (each request carries one
    chunk: no rate estimate, so CHUNK_SIZE)."""

    def setUp(self):
        """A directory of two files of eight chunks each, held by P1 and P2; P1 drips the first file's first chunk
        until DRIP passes or its link is closed (then it answers as a torn-down link does)."""
        super().setUp()
        self.config.deadline_slack = 0.3
        self.closing = threading.Event()
        self.addCleanup(self.closing.set)  # so no request thread outlives the test
        first = self.file(os.urandom(8 * CHUNK_SIZE))
        second = self.file(os.urandom(8 * CHUNK_SIZE))
        self.root = builder.build_directory([Entry("a", first.cid, first.size, 0),
                                             Entry("b", second.cid, second.size, 0)], self.put)
        self.dripped = walker.expand(walker.root_expect(first.cid), self.network[first.cid])[0].cid
        for node_id in (P1, P2):
            self.hold(node_id)
        self.runs.append([P1, P2])

        def close(node_id):
            """FakeClient.close, which also ends a dripping response: the link is gone."""
            FakeClient.close(self.client, node_id)
            if node_id == P1:
                self.closing.set()
        self.client.close = close

        def drip(cids):
            """Take DRIP seconds over the dripped chunk, as a response trickling in would."""
            if self.dripped in cids:
                self.closing.wait(DRIP)
                if P1 in self.client.closed:
                    return BlocksResult(FAILED, unsent=list(cids), reason="no answer")
            return None
        self.client.behaviour[P1] = drip

    def files(self, fetch) -> list[walker.Expect]:
        """The root directory's two entries, the root fetched."""
        root = fetch.resolve()
        return fetch.ensure([root])[root]

    def test_a_dripping_response_is_asked_of_an_idle_provider(self):
        start = time.monotonic()
        with self.fetcher.open(self.root.cid) as fetch:
            first, second = self.files(fetch)
            fetch.fetch_dag(first)
            took = time.monotonic() - start
            self.assertTrue(self.storage.is_complete(first.cid))
            self.assertLess(took, DRIP / 2, "the fetch waited for the dripping provider")
            # The first copy won: P1's slow request was ended by closing its link...
            self.assertEqual(self.client.closed, [P1])
            # ...which doesn't count against P1: its answer (the link gone) arrives, and P1 is used again
            deadline = time.monotonic() + 5
            while fetch._events.empty() and time.monotonic() < deadline:
                time.sleep(0.01)
            fetch.fetch_dag(second)
            provider = fetch._providers[P1]
            self.assertEqual((provider.failed, provider.stalls, provider.busy), (False, 0, 0))
        self.assertTrue(self.storage.is_complete(second.cid))
        second_chunks = {child.cid for child in walker.expand(second, self.network[second.cid])}
        self.assertTrue(second_chunks & set(self.client.asked(P1)), "P1 wasn't used again")
        # Only the dripped chunk was asked twice, and of P2 only once nothing else was left to ask for
        asked = self.client.asked(P1) + self.client.asked(P2)
        self.assertEqual(sorted(cid for cid in set(asked) if asked.count(cid) > 1), [self.dripped])
        first_cids = [cid for cid in self.client.asked(P2) if cid not in second_chunks and cid != second.cid]
        self.assertEqual(first_cids[-1], self.dripped)

    def test_a_link_another_fetch_uses_is_left_open(self):
        self.client.users[P1] = {-1: 1}  # another fetch's thread has a request on P1's link

        def drip(cids):
            """Drip, then end as stalled: an outcome that would count, if it were counted."""
            if self.dripped in cids:
                self.closing.wait(DRIP)
                return BlocksResult(STALLED, unsent=list(cids), reason="no progress")
            return None
        self.client.behaviour[P1] = drip
        start = time.monotonic()
        with self.fetcher.open(self.root.cid) as fetch:
            first, second = self.files(fetch)
            fetch.fetch_dag(first)
            self.assertLess(time.monotonic() - start, DRIP / 2, "the fetch waited for the dripping provider")
            self.assertEqual((self.client.closed, self.client.kept_open), ([], [P1]))  # left to finish
            self.closing.set()
            deadline = time.monotonic() + 5
            while fetch._events.empty() and time.monotonic() < deadline:
                time.sleep(0.01)
            fetch.fetch_dag(second)  # P1's answer is handled first: ignored
            provider = fetch._providers[P1]
            self.assertEqual((provider.failed, provider.stalls, provider.busy), (False, 0, 0))
        self.assertEqual(self.client.closed, [])

    def test_nothing_is_asked_twice_within_deadline_slack(self):
        self.config.deadline_slack = 30
        with self.fetcher.open(self.root.cid) as fetch:
            first, _ = self.files(fetch)
            threading.Timer(1.0, self.closing.set).start()  # P1 finishes after a second
            fetch.fetch_dag(first)
        asked = self.client.asked(P1) + self.client.asked(P2)
        self.assertEqual(len(asked), len(set(asked)))
        self.assertEqual(self.client.closed, [])

    def test_a_slow_node_never_asks_twice(self):
        self.fetcher = self.make_fetcher(slow=True)
        self.config.max_parallel_providers = None  # so one provider at a time
        with self.fetcher.open(self.root.cid) as fetch:
            first, _ = self.files(fetch)
            threading.Timer(1.0, self.closing.set).start()
            fetch.fetch_dag(first)
        asked = self.client.asked(P1) + self.client.asked(P2)
        self.assertEqual(len(asked), len(set(asked)))
        self.assertEqual(self.client.closed, [])


if __name__ == "__main__":
    unittest.main()
