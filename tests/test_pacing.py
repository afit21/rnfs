"""Upload pacing end to end (section 3, audit H1): a real RequestServer answering a real Fetch's /blocks requests
over a simulated slow link, in virtual time. An honest single provider must never be dropped for pacing."""
import os
import tempfile
import unittest
from unittest import mock

import RNS

from rnfs.config import Config
from rnfs.constants import BLOCKS_PATH, CHUNK_SIZE
from rnfs.content import builder, walker
from rnfs.content.storage import Storage
from rnfs.dht.routing import RoutingTable
from rnfs.fetcher import Fetcher
from rnfs.network import protocol, rates, server
from rnfs.network.client import BUSY, OK, BlocksResult
from tests.fakes.log import quiet_log
from tests.support import FakeClock
from tests.test_server import FakeBackend, FakeDestination, FakeLink, FakeResource

OWN, PROVIDER = bytes(16), bytes([1]) * 16
setUpModule, tearDownModule = quiet_log()


class PacingTest(unittest.TestCase):
    """One provider on a slow link, serving with the default upload cap (half its rate estimate)."""

    def setUp(self):
        """Temporary storage; RNS settings the server changes are restored afterwards."""
        saved = RNS.Resource.AUTO_COMPRESS_MAX_SIZE
        self.addCleanup(setattr, RNS.Resource, "AUTO_COMPRESS_MAX_SIZE", saved)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name

    def fetch(self, rate: float, rtt: float, chunks: int, server_config: Config | None = None,
              server_measures: float = 1.0, mtu: int = 500, mdu: int = 431) -> tuple[int, float]:
        """Fetch a file of chunks chunks from one provider over a link of rate bits/s and rtt seconds, whose
        MTU and MDU are mtu and mdu bytes.

        The responder measures server_measures × the rate (RNS's sender-side span includes a round trip).
        Returns (refusals, virtual seconds taken); fails the test if the provider is dropped.
        """
        clock = FakeClock()
        start = clock.now
        network = {}
        built = builder.build_bytes(os.urandom(chunks * CHUNK_SIZE), network.__setitem__)
        destination = FakeDestination()
        responder = server.RequestServer(destination, server_config or Config(), FakeBackend(blocks=network),
                                         clock=clock, start_monitor=False)

        def pace(response, wait):
            """The responder holding a response back, in virtual time."""
            clock.now += wait
        responder._pace = pace
        link = FakeLink(b"L" * 16, mtu=mtu, mdu=mdu)
        link.rtt = rtt
        destination.links.append(link)
        destination.established(link)
        handler = destination.handlers[BLOCKS_PATH][0]

        config = Config(rnfs_dir=self.tmp, slow="yes")
        storage = Storage(os.path.join(self.tmp, "blocks"), os.path.join(self.tmp, "state"), 10**9)
        self.addCleanup(storage.close)
        requester_rate = None
        client = mock.Mock(rate_estimate=lambda node_id: requester_rate)
        fetcher = Fetcher(storage, client, config, OWN, RoutingTable(OWN, config), lambda key: None,
                          state_dir=os.path.join(self.tmp, "state"), clock=clock)
        self.addCleanup(fetcher.close)
        refusals = 0
        with fetcher.open(built.cid, from_node=PROVIDER) as fetch:
            provider = fetch._providers[PROVIDER]
            root = walker.root_expect(built.cid)
            wanted = {built.cid: [root]}
            wanted.update({child.cid: [child] for child in walker.expand(root, network[built.cid])})
            outstanding, children = set(wanted), {}
            pending = [built.cid] + [cid for cid in wanted if cid != built.cid]
            request = 0
            while outstanding:
                self.assertFalse(provider.failed, f"provider dropped after {refusals} refusals")
                clock.now = max(clock.now, provider.busy_until)
                sent_at, sent_rate = clock.now, requester_rate
                max_bytes = rates.requester_max_bytes(sent_rate, config)
                batch = pending[:max(1, protocol.blocks_batch_size(max_bytes, pending, link.get_mdu()))]
                request += 1
                request_id = request.to_bytes(16, "big")
                clock.now += rtt / 2
                body = handler(BLOCKS_PATH, protocol.encode_blocks(max_bytes, batch), request_id, link.link_id,
                               None, 0.0)
                if body is False:
                    refusals += 1
                    clock.now += rtt / 2
                    result = BlocksResult(BUSY, unsent=batch, reason="busy")
                else:
                    entries = protocol.parse_blocks_response(body, len(batch))
                    packed = protocol.packed_response_size(request_id, body)
                    transfer = packed * 8 / rate
                    clock.now += rtt / 2 + transfer
                    if packed > link.get_mdu():
                        resource = FakeResource(link, request_id)
                        link.register_outgoing_resource(resource)
                        link.expected_rate = packed * 8 / (transfer + rtt) * server_measures
                        resource.complete()
                    if packed >= config.min_rate_sample:
                        requester_rate = packed * 8 / transfer
                    result = BlocksResult(OK, unsent=batch[len(entries):])
                    for cid, entry in zip(batch, entries):
                        result.blocks[cid] = entry
                fetch._handle(PROVIDER, batch, result, False, wanted, children, outstanding, sent_at, sent_rate)
                pending = [cid for cid in pending if cid in outstanding]
            self.assertTrue(storage.is_complete(built.cid))
        return refusals, clock.now - start

    def test_lora(self):
        refusals, taken = self.fetch(1200, 2.0, 4)
        self.assertEqual(refusals, 0)
        # Each 64 KiB chunk takes ~440 s at 1200 bit/s; the cap halves that rate
        self.assertLess(taken, 2.2 * 4 * CHUNK_SIZE * 8 / 1200)

    def test_slow_links(self):
        for rate, rtt in ((50_000, 0.5), (50_000, 2.0), (90_000, 0.3), (20_000, 1.0)):
            with self.subTest(rate=rate, rtt=rtt):
                refusals, taken = self.fetch(rate, rtt, 30)
                self.assertEqual(refusals, 0)
                self.assertLess(taken, 2.2 * 30 * CHUNK_SIZE * 8 / rate)

    def test_fast_link_isnt_paced(self):
        refusals, taken = self.fetch(200_000, 0.1, 30)
        self.assertEqual(refusals, 0)
        self.assertLess(taken, 1.2 * 30 * CHUNK_SIZE * 8 / 200_000)

    def test_responder_measuring_a_slower_rate(self):
        # Rate estimates differ between the ends: a few refusals, which don't drop the provider
        for rate, rtt in ((1200, 2.0), (50_000, 0.5)):
            with self.subTest(rate=rate, rtt=rtt):
                self.fetch(rate, rtt, 6, server_measures=0.8)

    def test_responder_with_a_lower_upload_cap(self):
        # max_upload_rate set on the responder, which the requester can't know: refused, but not dropped
        refusals, _ = self.fetch(50_000, 0.5, 12, Config(max_upload_rate=12_500))
        self.assertGreater(refusals, 0)

    def test_large_mtu_single_packet_responses_are_paced(self):
        # A fast path whose MTU holds a whole response (RNS: 256 KiB at 750 Mbit/s): every response goes as one
        # packet, so the responder never gets a rate estimate, but an explicit max_upload_rate applies all the
        # same (NET-2); its waits are within the bound, so they are held, not refused
        cap = 2_000_000
        refusals, taken = self.fetch(10_000_000, 0.05, 30, Config(max_upload_rate=cap), mtu=262144, mdu=262000)
        self.assertEqual(refusals, 0)
        self.assertGreater(taken, 0.9 * 30 * CHUNK_SIZE * 8 / cap)


if __name__ == "__main__":
    unittest.main()
