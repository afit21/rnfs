"""Rate estimates, slow links and slow nodes (specification section 2), and the /blocks size rules that follow
from them (sections 3 and 7). Used by both server and client.

A link's rate estimate is what RNS measured (Link.get_expected_rate()) for the last response of at least
min_rate_sample on it, recorded when that response completes, since RNS overwrites the value with every
Resource transfer. Rates are never derived from round-trip times, which the remote end can inflate.
"""
import threading

from rnfs.config import Config
from rnfs.constants import CHUNK_SIZE, MAX_RESPONSE_SIZE, RESPONSE_OVERHEAD
from rnfs.log import LOG_WARNING, log

SLOW_MTU = 500  # a link whose MTU RNS never raised above this crosses an interface that doesn't support more
LOCAL_INTERFACE_TYPES = ("LocalServerInterface", "LocalClientInterface")  # the shared instance and its clients


def sample(response_size: int | None, expected_rate: float | None, min_rate_sample: int) -> float | None:
    """The rate estimate a completed response gives, in bits/s, or None if it's too small to count."""
    if response_size is None or response_size < min_rate_sample:
        return None
    if expected_rate is None or expected_rate <= 0:
        return None
    return float(expected_rate)


class RateEstimates:
    """Rate estimates of links, keyed by link ID. Thread-safe."""

    def __init__(self, min_rate_sample: int):
        """No estimates yet; responses under min_rate_sample bytes are ignored."""
        self.min_rate_sample = min_rate_sample
        self.rates: dict[bytes, float] = {}
        self.samples = 0  # estimates recorded so far, over every link (read by tests)
        self.lock = threading.Lock()

    def record(self, link_id: bytes, response_size: int | None, expected_rate: float | None) -> bool:
        """Record what Link.get_expected_rate() said as a response of response_size completed. True if kept."""
        rate = sample(response_size, expected_rate, self.min_rate_sample)
        if rate is None:
            return False
        with self.lock:
            self.rates[link_id] = rate
            self.samples += 1
        return True

    def get(self, link_id: bytes) -> float | None:
        """The link's rate estimate in bits/s, or None if it has none."""
        with self.lock:
            return self.rates.get(link_id)

    def forget(self, link_id: bytes):
        """Drop a closed link's estimate."""
        with self.lock:
            self.rates.pop(link_id, None)


def is_slow_link(rate_estimate: float | None, mtu: int | None, slow_bitrate: float) -> bool:
    """True if a link is slow: its rate estimate is below slow_bitrate, or it has none and a small MTU.

    A link whose MTU isn't known (not active) counts as slow, which only makes it cautious.
    """
    if rate_estimate is not None:
        return rate_estimate < slow_bitrate
    return mtu is None or mtu <= SLOW_MTU


def link_is_slow(link, rate_estimate: float | None, config: Config) -> bool:
    """is_slow_link for an RNS link, reading its MTU with Link.get_mtu()."""
    try:
        mtu = link.get_mtu()
    except Exception:  # noqa: BLE001 - RNS internals: MTU unknown if they fail
        mtu = None
    return is_slow_link(rate_estimate, mtu, config.slow_bitrate)


def transfer_seconds(size: int, rate: float) -> float:
    """Seconds to move size bytes at rate bits/s."""
    return size * 8 / rate


def transfer_bytes(seconds: float, rate: float) -> int:
    """Bytes moved in seconds at rate bits/s."""
    return int(seconds * rate / 8)


def response_deadline(size: int, rate_estimate: float | None, config: Config) -> float:
    """Seconds a /blocks response of size bytes may take (section 3), used by both ends."""
    rate = rate_estimate if rate_estimate else config.unmeasured_bitrate
    return 3 * transfer_seconds(size, rate) + config.deadline_slack


def requester_max_bytes(rate_estimate: float | None, config: Config) -> int:
    """max_bytes for a /blocks request: response_target of transfer at the rate estimate, else CHUNK_SIZE."""
    if rate_estimate is None:
        # Small enough to finish on any link, and large enough that its completion gives an estimate
        return CHUNK_SIZE
    return transfer_bytes(config.response_target, rate_estimate)


# The /blocks size rules both ends share (section 7): the responder enforces them (server.blocks_budget), the
# requester sizes its batches by them (fetcher), so changing one end's rule can't leave the other's estimate wrong

def response_capacity(max_bytes: int, rate_estimate: float | None, upload_cap: float | None,
                      response_target: float) -> int:
    """Total block bytes a /blocks response may hold after its first served block: max_bytes, at most what one
    response carries, and response_target of transfer at the link's rate (capped by upload_cap, if any); CHUNK_SIZE
    at most while the link has no rate estimate."""
    limit = min(max_bytes, MAX_RESPONSE_SIZE - RESPONSE_OVERHEAD)
    if rate_estimate is None:
        # What a requester asks for until the link has a rate estimate (section 7), enforced: otherwise one
        # small request could ask for a ~1 MiB response with a deadline of many hours
        return min(limit, CHUNK_SIZE)
    rate = min(rate_estimate, upload_cap) if upload_cap else rate_estimate
    return min(limit, transfer_bytes(response_target, rate))


# Without max_upload_rate, a slow link's upload cap is its rate estimate divided by this (section 3). The responder
# then starts the next /blocks response on the link no sooner than this many times its transfer time at the rate
# estimate after the last one started, which the requester waits out (fetcher's _pace)
UPLOAD_CAP_DIVISOR = 2


def default_upload_cap(slow_link: bool, rate_estimate: float | None) -> float | None:
    """The upload cap of a link in bits/s when max_upload_rate isn't set: a slow link's rate estimate divided by
    UPLOAD_CAP_DIVISOR. None (no cap) on a fast link, or one without a rate estimate."""
    if slow_link and rate_estimate is not None:
        return rate_estimate / UPLOAD_CAP_DIVISOR
    return None


# Slow nodes

def counted_bitrates(stats: dict) -> list[float]:
    """Bitrates of the interfaces that decide whether a node is slow, from Reticulum.get_interface_stats().

    Shared-instance and local-client interfaces, offline ones, and ones reporting no bitrate are ignored.
    """
    bitrates = []
    for interface in (stats or {}).get("interfaces", []) or []:
        if not isinstance(interface, dict):
            continue
        if str(interface.get("type", "")) in LOCAL_INTERFACE_TYPES:
            continue
        if not interface.get("status"):
            continue
        bitrate = interface.get("bitrate")
        if not isinstance(bitrate, (int, float)) or isinstance(bitrate, bool) or bitrate <= 0:
            continue
        bitrates.append(float(bitrate))
    return bitrates


def slow_from_stats(stats: dict, slow_bitrate: float) -> bool:
    """True if every counted interface is below slow_bitrate; False if none are counted."""
    bitrates = counted_bitrates(stats)
    return bool(bitrates) and all(bitrate < slow_bitrate for bitrate in bitrates)


def has_slow_interface(reticulum, slow_bitrate: float) -> bool:
    """Whether one of the interfaces counted_bitrates counts runs below slow_bitrate: a node with one may have local
    servers (specification section 6, "LoRa islands") without naming local_interfaces. False if the stats can't be
    read (then nothing is local)."""
    try:
        bitrates = counted_bitrates(reticulum.get_interface_stats())
    except Exception as e:  # noqa: BLE001 - then nothing is local; logged
        log(f"could not read interface stats to look for a slow interface: {e}", LOG_WARNING)
        return False
    return any(bitrate < slow_bitrate for bitrate in bitrates)


def slowest_bitrate(reticulum) -> float | None:
    """The bitrate of the slowest interface counted_bitrates counts (bits/s), or None if none reports one or the
    stats can't be read: what a slow node's republish airtime is estimated at (publisher.republish_airtime)."""
    try:
        bitrates = counted_bitrates(reticulum.get_interface_stats())
    except Exception as e:  # noqa: BLE001 - the estimate then assumes the reference LoRa rate; logged
        log(f"could not read interface stats for the slowest interface's bitrate: {e}", LOG_WARNING)
        return None
    return min(bitrates, default=None)


def is_slow_node(config: Config, reticulum=None) -> bool:
    """Whether this node is slow (section 2): `slow` yes/no, or for auto, judged from its RNS interfaces.

    reticulum is the running RNS.Reticulum instance (default: the process's). get_interface_stats() also
    works through a shared instance, where it returns the shared instance's interfaces.
    """
    if config.slow == "yes":
        return True
    if config.slow == "no":
        return False
    try:
        if reticulum is None:
            import RNS
            reticulum = RNS.Reticulum.get_instance()
        stats = reticulum.get_interface_stats()
    except Exception as e:  # noqa: BLE001 - not slow if the stats can't be read; logged
        log(f"could not read interface stats to decide whether this node is slow: {e}", LOG_WARNING)
        return False
    return slow_from_stats(stats, config.slow_bitrate)
