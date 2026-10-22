"""REV5: a NomadNet page answer is read whole into memory, so its size is bounded at its first segment.

The attack: `rnfs add <destination>:/page/<path>` asks a NomadNet node for a page. The node answers with a Resource
without metadata (what RNS sends for a response that isn't a file), advertising a total size as large as the
requester allows: the request's max_response_size, which is the free space above the disk reserve unless
max_download_size is set (unset by default). Every segment passes the fetch's checks (the sequence, one segment's
decompression each, the assembly no larger than the total), RNS appends them to a file, and when the last completes
RNS 1.5.6's Link.response_resource_concluded reads that file whole (`resource.data.read()`) and decodes it with
umsgpack into another copy, before rnfs sees the answer. A "page" as large as the free disk exhausts a Raspberry
Pi's memory. A file answer (a Resource with metadata) is handed over as an open file and copied in chunks, so its
size only costs disk.

The test starts the first segment of answers on a fake NomadNet link through the fetch's own resource_started
callback (Transfer.resource_started, which RNS runs before it requests any part), with the helpers of
test_nomadnet_link_bounds: an answer without metadata advertising more than 16 MiB must be refused there (the
segment cancelled, the link closed) and reported as too large, while a page of 16 MiB and a file of any size within
max_response_size pass as before, and later segments can't change the total or the metadata flag (segment_refusal
already requires both to match the first's).

The fix: network/nomadnet.MAX_PAGE_SIZE (16 MiB); Transfer.resource_started refuses an answer's first segment when
it has no metadata and its advertised total size exceeds it, and _outcome reports that as TooLarge.
"""
import unittest

from rnfs.errors import TooLarge
from rnfs.network import nomadnet as fetching

from tests.security.test_nomadnet_link_bounds import SEGMENT, _Transfers, fetching_link, segments

PAGE_LIMIT = 16 * 1024 * 1024  # the bound the fix sets, written out so the test fails on code without it


class PageMemoryTest(_Transfers):
    """Transfer.resource_started on the first segment of page and file answers."""

    def test_the_bound_is_16_mib(self):
        self.assertEqual(getattr(fetching, "MAX_PAGE_SIZE", None), PAGE_LIMIT)

    def test_a_page_advertised_over_16_mib_is_refused_at_its_first_segment(self):
        for total in (PAGE_LIMIT + 1, 40 * SEGMENT):  # both within the transfer's max_size (50 segments)
            with self.subTest(total=total):
                self.setUp()
                first = segments(total, metadata=False)[0]
                self.transfer.resource_started(first)
                self.assert_refused(first)  # cancelled before RNS requested any part, the link closed
                with self.assertRaises(TooLarge):
                    fetching._outcome(fetching_link(), self.transfer, 50 * SEGMENT)

    def test_a_page_of_16_mib_passes(self):
        answer = segments(PAGE_LIMIT, metadata=False)
        for resource in answer:
            self.start(resource)
        self.assertIsNone(self.transfer.refused)
        self.link.teardown.assert_not_called()

    def test_files_keep_their_bound(self):
        # Written to disk and copied in chunks: only max_response_size (here 50 segments) bounds them
        answer = segments(40 * SEGMENT, metadata=True)
        for resource in answer:
            self.start(resource)
        self.assertIsNone(self.transfer.refused)
        self.link.teardown.assert_not_called()

    def test_later_segments_cant_drop_the_metadata(self):
        # A file's first segment, then segments claiming to be a page: refused as another answer's
        answer = segments(40 * SEGMENT, metadata=True)[:2]
        self.start(answer[0])
        answer[1].has_metadata = False
        self.transfer.resource_started(answer[1])
        self.assert_refused(answer[1])


if __name__ == "__main__":
    unittest.main()
