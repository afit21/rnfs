"""REL-1 (0.2.0 release audit): a provider whose results alternate between "unreachable" and "link not established"
kept its fetch alive for ever.

The attack: a node providing a CID only it claims ignores link requests and answers every other path request. RNS
drops the path after a link fails to establish, so the victim's requests come back "unreachable" (no path: retried
once after UNREACHABLE_RETRY) and "link not established" (lost: retried up to max_failures times) in turn. Each reset
the other's counter, and each unreachable retry moved the fetch's no-progress deadline on, so the fetch, its thread and
(on a node that isn't slow) a lookup every 10 minutes lived as long as the attacker liked.

The test alternates the two results from the one provider and expects NotFound within 2 × max_failures tries. The fix
(rnfs/fetcher.py, Fetch._handle): only an answer, or an outright failure, resets the lost count; a missing path doesn't.
"""
import os
import threading
import unittest
from unittest import mock

from rnfs import fetcher as fetcher_module
from rnfs.constants import CHUNK_SIZE
from rnfs.errors import NotFound
from rnfs.network.client import FAILED, LOST, BlocksResult
from tests.test_fetcher import P1, FetcherTest


class AlternatingUnreachableTest(FetcherTest):

    def test_alternating_unreachable_and_lost_fails_the_provider(self):
        self.config.lookup_timeout = 0.3
        self.config.stall_timeout = 0.2
        self.fetcher = self.make_fetcher()
        built = self.file(os.urandom(CHUNK_SIZE))
        tries = []

        def alternate(cids):
            """No path, then a link that doesn't establish, in turn."""
            tries.append(1)
            if len(tries) % 2:
                return BlocksResult(FAILED, unsent=list(cids), reason="unreachable")
            return BlocksResult(LOST, unsent=list(cids), reason="link not established")
        self.client.behaviour[P1] = alternate
        self.runs.append([P1])
        outcome = []

        def run():
            """Fetch, noting how it ended."""
            try:
                self.fetch_all(built.cid)
                outcome.append("fetched")
            except NotFound:
                outcome.append("not found")
        with mock.patch.object(fetcher_module, "UNREACHABLE_RETRY", 0.1):
            thread = threading.Thread(target=run, daemon=True)
            thread.start()
            thread.join(10.0)
        self.assertEqual(outcome, ["not found"])
        self.assertLessEqual(len(tries), 2 * self.config.max_failures)


if __name__ == "__main__":
    unittest.main()
