"""AUD-L10 (0.2.1 audit): a /range provider that reneged with a malformed answer, rather than [], lost only a plain
failure.

The attack (a REL-20 variant): a provider sends ranges of a block, whose bytes are checked only once it is whole,
then answers the next range malformed (say, a size other than the one it gave before). The requester drops the
partial, so the airtime is spent, as when the provider disowns the block with []. REL-20 scores that answer as a block
not matching its CID (BAD_BLOCK), but a malformed one was scored as an ordinary failure (-1), so a provider could keep
doing it, asked first again in every later fetch.

The test has a provider send the first range of a two-range chunk, then answer the next malformed: its score falls by
what a bad block costs, and it isn't banned (as for REL-20). A malformed first range, with nothing of the block sent
before, still costs only a failure.

The fix: rnfs/ranges.py (Ranges._fetch marks a malformed answer reneged when part of the block was held from that
provider) and rnfs/fetcher.py (Fetch._handle scores a failed request that reneged as REL-20's does).
"""
import os
import unittest

from rnfs.constants import CHUNK_SIZE
from rnfs.content import walker
from rnfs.dht.scores import BAD_BLOCK
from rnfs.errors import NotFound
from rnfs.network.client import FAILED, RangeResult
from tests.test_fetcher import P1, P2, FetcherTest


class MalformedRenegeTest(FetcherTest):
    """A slow node fetches a two-chunk file in ranges from P1."""

    def setUp(self):
        """The file, held by P1, and its first chunk."""
        super().setUp()
        self.fetcher = self.make_fetcher(slow=True)
        self.built = self.file(os.urandom(2 * CHUNK_SIZE))
        self.chunk = walker.expand(walker.root_expect(self.built.cid), self.network[self.built.cid])[0].cid
        self.hold(P1)
        self.runs.append([P1])

    def malformed_after(self, offset: int):
        """P1 answers ranges of the first chunk from offset on malformed, the rest as FakeClient serves them."""
        def answer(cid, held, max_bytes, compress):
            """Malformed past offset."""
            if cid == self.chunk and held >= offset:
                return RangeResult(FAILED, reason="malformed response")
            return None
        self.client.range_behaviour[P1] = answer

    def test_a_malformed_range_after_sending_ranges_costs_a_bad_blocks_score(self):
        self.malformed_after(1)
        with self.assertRaises(NotFound):
            self.fetch_all(self.built.cid)
        self.assertIn((P1, self.chunk, 0), [request[:3] for request in self.client.ranges])  # one range was sent
        # AUD-L10: fails until a malformed answer after ranges is scored as reneging (it cost -1)
        self.assertLess(self.fetcher.scores.score(P1), BAD_BLOCK / 2)
        self.assertFalse(self.fetcher.routing.is_banned(P1))

    def test_a_malformed_first_range_costs_only_a_failure(self):
        self.malformed_after(0)
        self.hold(P2)
        self.runs[:] = [[P1, P2]]
        self.fetch_all(self.built.cid)
        self.assertGreater(self.fetcher.scores.score(P1), BAD_BLOCK / 2)


if __name__ == "__main__":
    unittest.main()
