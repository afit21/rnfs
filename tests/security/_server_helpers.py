"""Helpers for the /blocks server security tests (H1, L2): fake links that know their RNS path, and a
RequestServer over tests/test_server.py's fakes driven by a FakeClock."""
import RNS

from rnfs.config import Config
from rnfs.constants import BLOCKS_PATH
from rnfs.network import protocol, server
from tests.support import FakeClock
from tests.test_server import FakeBackend, FakeDestination, FakeLink, FakeResource

TCP_BITRATE = 10_000_000  # what RNS's TCP interfaces assume (TCPClientInterface.BITRATE_GUESS)
LORA_BITRATE = 1_000  # a slow LoRa channel


class FakeInterface:
    """Stands in for an RNS Interface: a name and its configured bitrate."""

    def __init__(self, name: str, bitrate):
        """An interface called name, at bitrate bits/s."""
        self.name = name
        self.bitrate = bitrate

    def __str__(self):
        return self.name


class PathLink(FakeLink):
    """A FakeLink that also has the interface its request arrived on and the requester's hop count."""

    def __init__(self, link_id: bytes, interface=None, hops=1, **kwargs):
        """A link over interface, hops hops away."""
        super().__init__(link_id, **kwargs)
        self.attached_interface = interface
        self.expected_hops = hops


class ServerHarness:
    """A RequestServer over fake RNS objects and a FakeClock, with its monitor driven by hand."""

    def __init__(self, blocks: dict, config: Config | None = None):
        """Serve blocks (CID -> bytes) with config (default settings unless given)."""
        self.clock = FakeClock()
        self.backend = FakeBackend(blocks=blocks)
        self.config = config or Config()
        self.destination = FakeDestination()
        saved = RNS.Resource.AUTO_COMPRESS_MAX_SIZE
        self.server = server.RequestServer(self.destination, self.config, self.backend, clock=self.clock,
                                           start_monitor=False)
        RNS.Resource.AUTO_COMPRESS_MAX_SIZE = saved  # process-wide; leave it as other tests expect
        self.count = 0

    def connect(self, interface=None, hops=1, mtu=500, mdu=431) -> PathLink:
        """A new incoming link over interface, announced through the established callback."""
        self.count += 1
        link = PathLink(self.count.to_bytes(16, "big"), interface, hops, mtu=mtu, mdu=mdu)
        self.destination.links.append(link)
        self.destination.established(link)
        return link

    def blocks(self, link, cids, max_bytes=64 * 1024, identity=None, request_id=None):
        """Send a /blocks request on link as RNS would; the handler's answer."""
        handler = self.destination.handlers[BLOCKS_PATH][0]
        request_id = request_id or link.link_id
        return handler(BLOCKS_PATH, protocol.encode_blocks(max_bytes, cids), request_id, link.link_id, identity,
                       0.0)

    @staticmethod
    def advertise(link, request_id=None) -> FakeResource:
        """RNS advertising the response Resource for request_id (default: the link's own ID) on link."""
        resource = FakeResource(link, request_id or link.link_id, status=RNS.Resource.ADVERTISED)
        link.register_outgoing_resource(resource)
        return resource

    def response(self, link):
        """The response holding a slot for link, or None."""
        for response in self.server.pools.responses():
            if response.link_id == link.link_id:
                return response
        return None

    def tick(self, seconds: float = 0.0):
        """Move the clock on and run one monitor pass."""
        self.clock.now += seconds
        self.server.check_responses()
