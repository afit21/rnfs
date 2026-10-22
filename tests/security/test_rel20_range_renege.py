"""REL-20 (0.2.0 release audit): a /range provider could spend a block's airtime and lose only an ordinary failure.

The attack: a provider sends ranges of a block, the bytes checked only once it is whole, and just before the end
answers [] (it doesn't hold the block). The requester drops the partial: the airtime is spent, and the provider failed
for the fetch as one that simply didn't have a block, so it could do the same in every later fetch it was asked first.

The test has a provider send one range of a two-range chunk, then disown it: the fetch fails that provider, and its
score falls by what a block not matching its CID costs (it isn't banned, as content unpinned and collected while it
was sent does the same). The fix: rnfs/ranges.py (BlocksResult.reneged), rnfs/fetcher.py (Fetch._handle).
"""
import os
import unittest

from rnfs.constants import CHUNK_SIZE
from rnfs.content import walker
from rnfs.dht.scores import BAD_BLOCK
from rnfs.errors import NotFound
from rnfs.network.client import OK, RangeResult
from tests.test_fetcher import P1, P2, FetcherTest


class RangeRenegeTest(FetcherTest):

    def test_disowning_a_block_after_sending_ranges_of_it_costs_a_bad_blocks_score(self):
        self.fetcher = self.make_fetcher(slow=True)
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        chunk = walker.expand(walker.root_expect(built.cid), self.network[built.cid])[0].cid
        self.hold(P1)
        sent = []

        def renege(cid, offset, max_bytes, compress):
            """The first range comes as FakeClient serves it; asked for more of the chunk, it disowns it."""
            if cid == chunk and offset > 0:
                return RangeResult(OK, missing=True)
            sent.append(offset)
            return None
        self.client.range_behaviour[P1] = renege
        self.runs.append([P1])
        before = self.fetcher.scores.score(P1)
        with self.assertRaises(NotFound):
            self.fetch_all(built.cid)
        self.assertIn(0, sent)
        self.assertLess(self.fetcher.scores.score(P1), before + BAD_BLOCK / 2)  # scores decay as time passes
        self.assertFalse(self.fetcher.routing.is_banned(P1))

    def test_an_honest_none_costs_only_a_failure(self):
        self.fetcher = self.make_fetcher(slow=True)
        built = self.file(os.urandom(2 * CHUNK_SIZE))
        self.hold(P2)  # P1 holds nothing of it, and says so at once
        self.runs.append([P1, P2])
        self.fetch_all(built.cid)
        self.assertGreater(self.fetcher.scores.score(P1), BAD_BLOCK / 2)


if __name__ == "__main__":
    unittest.main()
