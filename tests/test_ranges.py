"""Tests for rnfs.ranges: which nodes answer /range (learnt once, persisted, asked again weekly), the partial blocks
(bounded, cleaned at start, one per block and provider), and one range fetched into a partial block."""
import os
import tempfile
import unittest

from rnfs import ranges, statedb
from rnfs.constants import DAY, KIB
from rnfs.content.cid import RAW, Cid
from rnfs.network import client as peer_client
from tests.fakes.client import FakeClient

P1, P2 = b"\x01" * 16, b"\x02" * 16


def raw(data: bytes) -> tuple[Cid, bytes]:
    """A raw block and its CID."""
    return Cid.for_block(RAW, data), data


class RangeSupportTest(unittest.TestCase):
    """RangeSupport: answers, records, persistence, recheck and the cap."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = [1_000_000.0]

    def support(self) -> ranges.RangeSupport:
        """A RangeSupport over the state database in the temporary directory."""
        db = statedb.connect(self.tmp.name)
        self.addCleanup(db.close)
        return ranges.RangeSupport(db, clock=lambda: self.now[0])

    def test_unknown_then_learnt(self):
        support = self.support()
        self.assertIsNone(support.answers(P1))
        support.record(P1, False)
        support.record(P2, True)
        self.assertIs(support.answers(P1), False)
        self.assertIs(support.answers(P2), True)

    def test_persisted_across_restarts(self):
        support = self.support()
        support.record(P1, False)
        support.record(P2, True)
        again = self.support()
        self.assertIs(again.answers(P1), False)
        self.assertIs(again.answers(P2), True)

    def test_a_node_not_answering_is_asked_again_after_a_week(self):
        support = self.support()
        support.record(P1, False)
        support.record(P2, True)
        self.now[0] += ranges.RECHECK - 1
        self.assertIs(support.answers(P1), False)
        self.now[0] += 1
        self.assertIsNone(support.answers(P1))  # asked once more
        self.assertIs(support.answers(P2), True)  # a node that answers stays known
        self.assertIsNone(self.support().answers(P1))  # the same after a restart

    def test_written_only_when_it_changes_or_a_day_on(self):
        support = self.support()
        writes = []
        real = support.db.transaction

        def counting(*args, **kwargs):
            writes.append(1)
            return real(*args, **kwargs)
        support.db.transaction = counting
        support.record(P1, True)
        support.record(P1, True)
        support.record(P1, True)
        self.assertEqual(len(writes), 1)
        self.now[0] += DAY
        support.record(P1, True)
        self.assertEqual(len(writes), 2)
        support.record(P1, False)
        self.assertEqual(len(writes), 3)

    def test_capped(self):
        support = self.support()
        cap = 5
        original = ranges.MAX_NODES
        ranges.MAX_NODES = cap
        self.addCleanup(setattr, ranges, "MAX_NODES", original)
        for i in range(cap + 3):
            self.now[0] += 1
            support.record(bytes([i]) * 16, False)
        self.assertEqual(len(support.records), cap)
        self.assertIsNone(support.answers(bytes([0]) * 16))  # the least recently checked went first
        rows = support.db.query("SELECT COUNT(*) FROM range_support")
        self.assertEqual(rows, [(cap,)])

    def test_in_memory_without_a_database(self):
        support = ranges.RangeSupport(None)
        support.record(P1, False)
        self.assertIs(support.answers(P1), False)


class PartialsTest(unittest.TestCase):
    """Partials: per (block, provider), files cleaned at start, bounded, held by one request at a time."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = os.path.join(self.tmp.name, ranges.PARTIAL_DIR)

    def test_append_read_and_files(self):
        partials = ranges.Partials(self.dir)
        cid = raw(b"x" * 100)[0]
        partial = partials.acquire(cid, P1)
        self.assertEqual(partials.read(partial), b"")
        self.assertTrue(partials.append(partial, 100, b"abc"))
        self.assertTrue(partials.append(partial, 100, b"def"))
        self.assertEqual(partials.read(partial), b"abcdef")
        self.assertEqual(partials.length(cid, P1), 6)
        self.assertEqual(partials.length(cid, P2), 0)  # another provider's is its own
        self.assertEqual(len(os.listdir(self.dir)), 1)
        partials.release(partial)
        partials.drop(cid)
        self.assertEqual(os.listdir(self.dir), [])
        self.assertEqual(partials.count(), 0)

    def test_left_over_files_are_deleted_at_start(self):
        os.makedirs(self.dir)
        for name in ("a" * 32 + ranges.PARTIAL_SUFFIX, "b" * 32 + ranges.PARTIAL_SUFFIX):
            with open(os.path.join(self.dir, name), "wb") as f:
                f.write(b"left by an earlier run")
        ranges.Partials(self.dir)
        self.assertEqual(os.listdir(self.dir), [])

    def test_one_request_at_a_time(self):
        partials = ranges.Partials(None)
        cid = raw(b"y")[0]
        held = partials.acquire(cid, P1)
        self.assertIsNone(partials.acquire(cid, P1))  # held by another request
        self.assertIsNotNone(partials.acquire(cid, P2))
        partials.release(held)
        self.assertIsNotNone(partials.acquire(cid, P1))

    def test_bounded_least_recently_written_first(self):
        partials = ranges.Partials(self.dir, limit=2)
        cids = [raw(bytes([i]) * 10)[0] for i in range(3)]
        first = partials.acquire(cids[0], P1)
        partials.append(first, 10, b"0")
        partials.release(first)
        second = partials.acquire(cids[1], P1)
        partials.append(second, 10, b"1")
        partials.release(second)
        third = partials.acquire(cids[2], P1)  # the first goes
        self.assertIsNotNone(third)
        self.assertEqual(partials.length(cids[0], P1), 0)
        self.assertEqual(partials.length(cids[1], P1), 1)
        self.assertEqual(len(os.listdir(self.dir)), 1)  # the third has no file until written
        # Every partial held: no room for another
        held = partials.acquire(cids[1], P1)
        self.assertIsNone(partials.acquire(cids[0], P2))
        partials.release(held)
        partials.release(third)

    def test_an_unreadable_file_empties_the_partial(self):
        partials = ranges.Partials(self.dir)
        cid = raw(b"z" * 10)[0]
        partial = partials.acquire(cid, P1)
        partials.append(partial, 10, b"abc")
        os.remove(partial.path)
        self.assertEqual(partials.read(partial), b"")
        self.assertEqual(partial.length, 0)


class RangesFetchTest(unittest.TestCase):
    """Ranges.fetch: one range into a partial block, then the block verified whole."""

    def setUp(self):
        self.client = FakeClient()
        self.ranges = ranges.Ranges(ranges.RangeSupport(None), ranges.Partials(None))

    def test_ranges_build_the_block_and_verify_it(self):
        cid, block = raw(os.urandom(40 * KIB))
        self.client.holdings[P1] = {cid: block}
        results = []
        while True:
            result = self.ranges.fetch(self.client, P1, cid, 16 * KIB, 1.0, False, len(block))
            results.append(result)
            if result.blocks:
                break
        self.assertEqual([r.partial.get(cid) for r in results], [16 * KIB, 32 * KIB, None])
        self.assertEqual(results[-1].blocks, {cid: block})
        self.assertEqual([(offset, max_bytes) for _, _, offset, max_bytes, _ in self.client.ranges],
                         [(0, 16 * KIB), (16 * KIB, 16 * KIB), (32 * KIB, 8 * KIB)])
        self.assertEqual(self.ranges.partials.count(), 0)  # forgotten once complete
        self.assertIs(self.ranges.support.answers(P1), True)

    def test_compression_scales_the_range_and_stops_for_a_block_that_doesnt_compress(self):
        text = (b"It is a truth universally acknowledged, that a single man in possession of a good fortune, "
                * 1500)[:128 * KIB]
        cid, block = raw(text)
        self.client.holdings[P1] = {cid: block}
        result = self.ranges.fetch(self.client, P1, cid, 16 * KIB, 3.0, True, len(block))
        self.assertEqual(self.client.ranges[-1][3], 48 * KIB)  # three times the bytes on the wire
        self.assertGreater(result.ratio, 3.0)
        noise_cid, noise = raw(os.urandom(64 * KIB))
        self.client.holdings[P1][noise_cid] = noise
        self.ranges.fetch(self.client, P1, noise_cid, 16 * KIB, 3.0, True, len(noise))
        self.ranges.fetch(self.client, P1, noise_cid, 16 * KIB, 3.0, True, len(noise))
        asked = [(compress, max_bytes) for _, c, _, max_bytes, compress in self.client.ranges if c == noise_cid]
        self.assertEqual(asked, [(True, 48 * KIB), (False, 16 * KIB)])  # it came back raw: the rest asked raw

    def test_an_old_nodes_silence_is_learnt_once(self):
        cid, block = raw(os.urandom(20 * KIB))
        self.client.holdings[P1] = {cid: block}
        self.client.no_range.add(P1)
        result = self.ranges.fetch(self.client, P1, cid, 16 * KIB, 1.0, False, len(block))
        self.assertEqual((result.status, result.unsent), (peer_client.UNSUPPORTED, [cid]))
        self.assertIs(self.ranges.support.answers(P1), False)
        self.assertFalse(self.ranges.usable(P1))

    def test_silence_from_a_node_known_to_answer_is_a_lost_request(self):
        cid, block = raw(os.urandom(20 * KIB))
        self.client.holdings[P1] = {cid: block}
        self.ranges.support.record(P1, True)
        self.client.no_range.add(P1)
        result = self.ranges.fetch(self.client, P1, cid, 16 * KIB, 1.0, False, len(block))
        self.assertEqual(result.status, peer_client.LOST)
        self.assertIs(self.ranges.support.answers(P1), True)

    def test_a_partial_in_use_falls_back_to_blocks(self):
        cid, block = raw(os.urandom(20 * KIB))
        held = self.ranges.partials.acquire(cid, P1)
        self.assertIsNone(self.ranges.fetch(self.client, P1, cid, 16 * KIB, 1.0, False, len(block)))
        self.ranges.partials.release(held)

    def test_mismatch_and_malformed_drop_the_partial(self):
        cid, block = raw(os.urandom(20 * KIB))
        forged = bytearray(block)
        forged[-1] ^= 1
        self.client.holdings[P1] = {cid: bytes(forged)}
        self.ranges.fetch(self.client, P1, cid, 16 * KIB, 1.0, False, len(block))
        result = self.ranges.fetch(self.client, P1, cid, 16 * KIB, 1.0, False, len(block))
        self.assertEqual(result.bad, [cid])
        self.assertEqual(self.ranges.partials.length(cid, P1), 0)
        # A malformed answer (a wrong total, say) drops it too
        self.client.holdings[P1] = {cid: block}
        self.ranges.fetch(self.client, P1, cid, 16 * KIB, 1.0, False, len(block))
        self.client.range_behaviour[P1] = lambda c, offset, max_bytes, compress: peer_client.RangeResult(
            peer_client.FAILED, reason="malformed response")
        result = self.ranges.fetch(self.client, P1, cid, 16 * KIB, 1.0, False, len(block))
        self.assertEqual(result.status, peer_client.FAILED)
        self.assertEqual(self.ranges.partials.length(cid, P1), 0)

    def test_unserved_drops_the_partial(self):
        cid, block = raw(os.urandom(20 * KIB))
        self.client.holdings[P1] = {cid: block}
        self.ranges.fetch(self.client, P1, cid, 16 * KIB, 1.0, False, len(block))
        self.client.holdings[P1] = {}
        result = self.ranges.fetch(self.client, P1, cid, 16 * KIB, 1.0, False, len(block))
        self.assertEqual(result.missing, [cid])
        self.assertEqual(self.ranges.partials.count(), 0)


if __name__ == "__main__":
    unittest.main()
