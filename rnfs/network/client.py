"""Requests to other nodes (sections 3, 6, 7): /find, /provide, /blocks and /range with stall detection, /pin, and
ping.

Every method blocks until it has a result, so never call one from an RNS callback thread. Lookups and
downloads use the node's shared unidentified link; /provide and /pin use a separate identified link,
closed when done.
"""
import threading
from dataclasses import dataclass, field
from typing import Callable

from rnfs.config import Config
from rnfs.constants import BLOCKS_PATH, FIND_PATH, PIN_PATH, PROVIDE_PATH, RANGE_BZ2, RANGE_PATH
from rnfs.content.cid import Cid
from rnfs.log import LOG_VERBOSE, LOG_WARNING, log, node_name, plural, size
from rnfs.network import protocol, rates
from rnfs.network.links import Exchange, LinkPool, closed_by_other, timed_out
from rnfs import mesh

# BlocksResult statuses
OK = "ok"  # the node answered; see blocks, missing, unsent and bad
BUSY = "busy"  # refused (False) under the node's limits, or the node cancelled the response (displaced or
#               past its deadline, .reason "cancelled"), or closed the link (.reason "closed": no room for another
#               incoming link, say); retry after a short delay. The link stays open unless the node closed it
FAILED = "failed"  # no link (no path, or closed by this node), or a malformed answer
# No answer the way a lossy link loses one: RNS timed the request out (the request or its answer lost), the response
# transfer failed, the link timed out under it, or a new link to a node with a path wasn't established. Retry it on
# a new link after a short delay (the fetcher closes this one)
LOST = "lost"
STALLED = "stalled"  # the response stopped making progress or missed its deadline; the link was closed
# The node left a /range unanswered, as nodes before 0.2.0 do (they have no handler for it): the fetcher asks for the
# block with /blocks instead, at once, and remembers that the node doesn't answer /range (rnfs.ranges)
UNSUPPORTED = "unsupported"


@dataclass
class BlocksResult:
    """Outcome of one /blocks request. Every block in .blocks has been verified against its CID."""

    status: str
    blocks: dict[Cid, bytes] = field(default_factory=dict)
    missing: list[Cid] = field(default_factory=list)  # answered None: not held or not served
    unsent: list[Cid] = field(default_factory=list)  # not answered: cut by the batch size or budget, or not OK
    bad: list[Cid] = field(default_factory=list)  # blocks that didn't match their CID: ban the provider
    reason: str = ""  # why the request stalled or failed, for logs
    elapsed: float | None = None  # seconds from sending the request to its answer, if OK (local, not on the wire)
    # A /range request's (rnfs.ranges): the blocks it brought part of, with the bytes now held of each, and the
    # compression its answer achieved (bytes / bytes on the wire), if it came compressed
    partial: dict[Cid, int] = field(default_factory=dict)
    ratio: float | None = None


@dataclass
class RangeResult:
    """Outcome of one /range request (rnfs 0.2.0). Its bytes passed protocol.parse_range_response's checks; a block in
    .block, the range completing it, has been verified whole against its CID."""

    status: str  # OK, BUSY, FAILED, LOST or STALLED, as for /blocks
    size: int | None = None  # the block's length, from an OK answer
    data: bytes = b""  # the range's bytes, decompressed
    block: bytes | None = None  # the whole block, verified, if this range completed it
    bad: bool = False  # this range completed the block, and it didn't match its CID: ban the provider
    missing: bool = False  # answered []: the node doesn't hold or serve the block
    wire: int = 0  # bytes of the answer, as RNS reported them (compressed, framing included)
    compressed: bool = False  # the bytes came as bz2
    silent: bool = False  # RNS timed it out with the link up, nothing of an answer seen: as an older node leaves /range
    reason: str = ""
    elapsed: float | None = None  # seconds from sending the request to its answer, if OK


class PeerClient:
    """Sends rnfs requests to other nodes over links from a LinkPool and checks their answers."""

    def __init__(self, links: LinkPool, config: Config):
        """Client sending over links."""
        self.links = links
        self.config = config
        self.traffic = mesh.Traffic()  # requests and bytes per peer (mesh); the node shares it with the server
        # Every link opened (shared, ping's, identified) records its round trip, and that the node is up, there
        links.on_rtt = self.traffic.measured
        # Per thread, (node ID, whether the node closed the link) of the last /find it sent (closed_by_node): a lookup
        # tells its failure callback on the thread that queried
        self._last_find = threading.local()

    def closed_by_node(self, node_id: bytes) -> bool:
        """True if this thread's last /find went to node_id and failed because node_id closed the link (a refusal:
        busy, or going away), not because it didn't answer: it then doesn't count against the node in the routing
        table, its score or partition mode (specification section 6)."""
        return getattr(self._last_find, "outcome", None) == (node_id, True)

    def _closed(self, node_id: bytes, peer) -> bool:
        """True if node_id closed the link a request went on (peer), or the new one get() tried to open (peer None)."""
        if peer is None:
            return self.links.refused(node_id)
        return not peer.is_active() and peer.link is not None and closed_by_other(peer.link)

    # Links

    def has_link(self, node_id: bytes) -> bool:
        """True if there's an open outgoing link to node_id (the fetcher asks those nodes first)."""
        return self.links.has_link(node_id)

    def open_links(self) -> list[bytes]:
        """Node IDs with an open outgoing link, most recently used first."""
        return self.links.open_nodes()

    def rate_estimate(self, node_id: bytes) -> float | None:
        """The rate estimate (bits/s) of the open link to node_id, or None."""
        peer = self.links.shared_peer(node_id)
        return peer.rate if peer is not None else None

    def link_mdu(self, node_id: bytes) -> int | None:
        """The packet MDU of the open link to node_id, which bounds the requests sent on it, or None."""
        peer = self.links.shared_peer(node_id)
        return peer.mdu() if peer is not None and peer.is_active() else None

    def hold_link(self, node_id: bytes, seconds: float) -> bool:
        """Count the shared link to node_id as in use for seconds more (LinkPool.hold): the fetcher waiting out the
        node's upload pacing, so the idle sweep doesn't close the link, and with it the rate estimate the next
        request is sized and paced by. False if there is no open link."""
        return self.links.hold(node_id, seconds)

    def close(self, node_id: bytes):
        """Close the shared link to node_id, cancelling any transfer on it."""
        self.links.close(node_id)

    def close_unless_used(self, node_id: bytes, own: set[int]) -> bool:
        """Close the shared link to node_id, as close() does, unless a request made from a thread not in own
        (thread idents: the caller's request threads) is in progress on it or waiting for a slot, so one fetch
        giving up on its request doesn't fail another's. True if closed (or there was no link); False if left
        open, its request left to finish. Never cancels a Resource: closing the link stops it (section 3)."""
        return self.links.close_unless_used(node_id, own)

    def await_path(self, node_id: bytes, timeout: float | None = None) -> bool:
        """Wait up to timeout (path_timeout if None) for a path to node_id, requesting one if RNS has none: True once
        there is one (LinkPool.await_path)."""
        return self.links.await_path(node_id, timeout)

    # /find

    def find(self, node_id: bytes, key: bytes) -> tuple[list[bytes], list[bytes]] | None:
        """Ask node_id for providers of key and the servers it knows closest to it.

        (providers, nodes), or None if the node couldn't be reached, didn't answer, refused (a DHT client
        answers False) or answered something malformed.
        """
        self._last_find.outcome = (node_id, False)
        peer = self.links.get(node_id)
        exchange = None
        if peer is not None:
            # Honest answers fit one packet; a larger one is refused at its advertisement (and links.py gives up on
            # one that is trickled)
            exchange = self.links.call(peer, FIND_PATH, protocol.encode_find(key),
                                       max_response_size=protocol.MAX_FIND_RESPONSE_SIZE)
        if exchange is None or not exchange.answered:
            closed = self._closed(node_id, peer)
            self._last_find.outcome = (node_id, closed)
            if peer is not None:
                log(f"{node_name(node_id)} closed the link before answering /find" if closed
                    else f"no answer from {node_name(node_id)} to /find", LOG_VERBOSE)
            self.traffic.request(node_id, mesh.REFUSED if closed else mesh.FAILED)
            return None
        result = protocol.parse_find_response(exchange.response)
        if result is None:
            outcome = "refused" if exchange.response is False else "sent a malformed answer to"
            log(f"{node_name(node_id)} {outcome} /find for {key.hex()}", LOG_VERBOSE)
            self.traffic.request(node_id, mesh.REFUSED if exchange.response is False else mesh.FAILED)
            return None
        self.traffic.request(node_id, mesh.ANSWERED)
        providers, nodes = result
        log(f"asked {node_name(node_id)} about {key.hex()}: {plural(len(providers), 'provider')}, "
            f"{plural(len(nodes), 'node')}", LOG_VERBOSE)
        return result

    # /provide

    def provide(self, node_id: bytes, keys: list[bytes], withdrawn: list[bytes]) -> set[bytes]:
        """Tell node_id we provide keys and no longer provide withdrawn, over one identified link.

        Sent in as few requests as fit one packet each, one at a time; stops at the first batch that isn't
        recorded (section 6: not retried until the next republish). Returns the keys node_id may hold a
        record of: every key if each batch was recorded, else those of the batches sent, including the one
        refused or unanswered (a responder can record some keys of a batch it refuses, e.g. up to a limit),
        so they are withdrawn from it later. Empty if no identified link could be opened.
        """
        if not keys and not withdrawn:
            return set()
        peer = self.links.open_identified(node_id)
        if peer is None:
            return set()
        try:
            batches = protocol.provide_batches(keys, withdrawn, peer.mdu())
            sent: set[bytes] = set()
            for batch_keys, batch_withdrawn in batches:
                sent.update(batch_keys)
                exchange = self.links.call(peer, PROVIDE_PATH, protocol.encode_provide(batch_keys, batch_withdrawn),
                                           max_response_size=protocol.MAX_ACCEPT_RESPONSE_SIZE)
                if not (exchange.answered and protocol.is_accepted(exchange.response)):
                    outcome = "refused" if exchange.answered else "didn't answer"
                    log(f"{node_name(node_id)} {outcome} /provide of {plural(len(batch_keys), 'key')}"
                        + (f" ({plural(len(keys) - len(sent), 'key')} not sent)" if len(sent) < len(keys) else ""),
                        LOG_VERBOSE)
                    return sent
            log(f"{node_name(node_id)} recorded {plural(len(keys), 'key')}"
                + (f", withdrew {len(withdrawn)}" if withdrawn else ""), LOG_VERBOSE)
            return set(keys)
        finally:
            self.links.close_link(peer)

    # /pin

    def pin(self, node_id: bytes, cid: Cid, pin: bool) -> bool:
        """Ask node_id to pin cid (pin=True) or release our pin of it, over an identified link. True if accepted."""
        peer = self.links.open_identified(node_id)
        if peer is None:
            return False
        try:
            exchange = self.links.call(peer, PIN_PATH, protocol.encode_pin(cid, pin),
                                       max_response_size=protocol.MAX_ACCEPT_RESPONSE_SIZE)
            return exchange.answered and protocol.is_accepted(exchange.response)
        finally:
            self.links.close_link(peer)

    # ping

    def ping(self, node_id: bytes) -> float | None:
        """Open a new link to node_id and return its round-trip time in seconds, or None if unreachable. The pool
        records it in traffic as it does for every link (LinkPool.on_rtt)."""
        peer = self.links.open_oneshot(node_id, identify=False)
        if peer is None:
            return None
        try:
            return getattr(peer.link, "rtt", None)
        finally:
            self.links.close_link(peer)

    # /blocks

    def blocks(self, node_id: bytes, cids: list[Cid],
               on_progress: Callable[[float], None] | None = None) -> BlocksResult:
        """_blocks(), counted in traffic (mesh): the outcome, verified bytes received and bad blocks."""
        result = self._blocks(node_id, cids, on_progress)
        if result.status != OK or result.blocks or result.missing or result.bad:
            outcome = {OK: mesh.ANSWERED, BUSY: mesh.REFUSED}.get(result.status, mesh.FAILED)
            self.traffic.request(node_id, outcome, sum(len(block) for block in result.blocks.values()),
                                 self._interface(node_id), len(result.bad))
        return result

    def _interface(self, node_id: bytes) -> str | None:
        """The name of the interface the shared link to node_id runs over, or None."""
        peer = self.links.shared_peer(node_id)
        return mesh.interface_label(getattr(getattr(peer, "link", None), "attached_interface", None))

    def _blocks(self, node_id: bytes, cids: list[Cid],
                on_progress: Callable[[float], None] | None = None) -> BlocksResult:
        """Ask node_id for blocks over its shared link, verifying each against its CID.

        Sends as many of cids (duplicates dropped) as fit one packet; the rest come back in .unsent.
        on_progress(fraction) is called from this thread as the response arrives.
        """
        wanted = list(dict.fromkeys(cids))
        if not wanted:
            return BlocksResult(OK)
        peer = self.links.get(node_id)
        if peer is None:
            if self.links.unestablished(node_id):
                return BlocksResult(LOST, unsent=wanted, reason="link not established")
            if self._closed(node_id, None):
                return BlocksResult(BUSY, unsent=wanted, reason="closed")
            return BlocksResult(FAILED, unsent=wanted, reason="unreachable")

        rate = peer.rate
        max_bytes = rates.requester_max_bytes(rate, self.config)
        count = protocol.blocks_batch_size(max_bytes, wanted, peer.mdu())
        batch, rest = wanted[:count], wanted[count:]
        if not batch:
            return BlocksResult(FAILED, unsent=wanted, reason="request doesn't fit a packet")

        exchange = self.links.call(peer, BLOCKS_PATH, protocol.encode_blocks(max_bytes, batch), blocks=True,
                                   max_response_size=protocol.max_response_size(max_bytes),
                                   watchdog=self._stall_watchdog(rate), on_progress=on_progress)
        name = node_name(node_id)
        if exchange.stalled:
            self._close_stalled(peer, f"/blocks from {name} stalled ({exchange.stalled})")
            return BlocksResult(STALLED, unsent=wanted, reason=exchange.stalled)
        if exchange.cancelled:
            # The responder gave the slot to another request or ran out of time, or the transfer was lost in
            # transit: like a refusal, not a stall
            log(f"{name}'s /blocks response stopped arriving (cancelled by it, or lost in transit)", LOG_VERBOSE)
            return BlocksResult(BUSY, unsent=wanted, reason="cancelled")
        if not exchange.answered and self._closed(node_id, peer):
            # Busy (no room for another incoming link) or going away: a refusal, retried on a new link after the busy
            # delay, max_failures times in a row at most
            log(f"{name} closed the link before answering /blocks", LOG_VERBOSE)
            return BlocksResult(BUSY, unsent=wanted, reason="closed")
        if not exchange.answered:
            log(f"no answer from {name} to /blocks", LOG_VERBOSE)
            return BlocksResult(LOST if self._lost(peer, exchange) else FAILED, unsent=wanted, reason="no answer")
        if exchange.response is False:
            log(f"{name} is busy and refused /blocks", LOG_VERBOSE)
            return BlocksResult(BUSY, unsent=wanted, reason="busy")
        entries = protocol.parse_blocks_response(exchange.response, len(batch))
        if entries is None:
            # Including an empty answer: counted as a failure, so the fetcher doesn't ask again forever
            log(f"{name} sent a malformed /blocks response", LOG_WARNING)
            return BlocksResult(FAILED, unsent=wanted, reason="malformed response")

        if peer.record_rate(exchange, self.config.min_rate_sample):
            log(f"link to {name} measured at {size(peer.rate / 8)}/s", LOG_VERBOSE)
            self.traffic.rated(node_id, peer.rate)  # the last link's estimate, a slow path's sign (mesh.SlowPaths)
        result = self._check_blocks(name, batch, entries, rest)
        # From when it was sent: not opening the link or waiting for its /blocks slot
        result.elapsed = max(0.0, self.links.clock() - exchange.sent_at)
        return result

    # /range (rnfs 0.2.0)

    def block_range(self, node_id: bytes, cid: Cid, held: bytes = b"", max_bytes: int = 0, compress: bool = False,
                    block_size: int | None = None, on_progress: Callable[[float], None] | None = None,
                    answers: bool | None = None) -> RangeResult:
        """Ask node_id for at most max_bytes of cid's block after the held bytes (the start of the block, received
        from node_id before), compressed if compress, over its shared link; counted in traffic (mesh). answers: whether
        node_id is known to answer /range (rnfs.ranges.RangeSupport); one not known to may leave it unanswered on a
        link that works, which is then kept for the /blocks that follows.

        The answer is checked (protocol.parse_range_response: its size against block_size, if known, and the length
        of its bytes, decompressed within that bound); a range that completes the block has the whole block, held
        and data, verified against cid. on_progress(fraction) is called from this thread as the answer arrives.
        """
        result = self._range(node_id, cid, held, max_bytes, compress, block_size, on_progress, answers is not True)
        if result.status != OK or result.block is not None or result.missing or result.bad:
            outcome = {OK: mesh.ANSWERED, BUSY: mesh.REFUSED}.get(result.status, mesh.FAILED)
            if not result.silent:  # an older node's silence isn't its failure (rnfs.ranges)
                self.traffic.request(node_id, outcome, len(result.block or b""), self._interface(node_id),
                                     int(result.bad))
        else:
            self.traffic.request(node_id, mesh.ANSWERED, 0, self._interface(node_id))
        return result

    def _range(self, node_id: bytes, cid: Cid, held: bytes, max_bytes: int, compress: bool, block_size: int | None,
               on_progress: Callable[[float], None] | None, silence_expected: bool = False) -> RangeResult:
        """block_range(), uncounted."""
        peer = self.links.get(node_id)
        if peer is None:
            if self.links.unestablished(node_id):
                return RangeResult(LOST, reason="link not established")
            if self._closed(node_id, None):
                return RangeResult(BUSY, reason="closed")
            return RangeResult(FAILED, reason="unreachable")
        rate = peer.rate
        offset = len(held)
        request = protocol.encode_range(cid, offset, max_bytes, RANGE_BZ2 if compress else 0)
        exchange = self.links.call(peer, RANGE_PATH, request, blocks=True,
                                   max_response_size=protocol.range_max_response_size(max_bytes),
                                   watchdog=self._stall_watchdog(rate), on_progress=on_progress,
                                   silence_expected=silence_expected)
        name = node_name(node_id)
        if exchange.stalled:
            self._close_stalled(peer, f"/range from {name} stalled ({exchange.stalled})")
            return RangeResult(STALLED, reason=exchange.stalled)
        if exchange.cancelled:
            log(f"{name}'s /range answer stopped arriving (cancelled by it, or lost in transit)", LOG_VERBOSE)
            return RangeResult(BUSY, reason="cancelled")
        if not exchange.answered and self._closed(node_id, peer):
            log(f"{name} closed the link before answering /range", LOG_VERBOSE)  # a refusal, as for /blocks
            return RangeResult(BUSY, reason="closed")
        if not exchange.answered:
            # Nothing at all came back, the link up: not an answer that started and failed on the way
            silent = peer.is_active() and exchange.receipt is not None and exchange.response_started is None
            log(f"no answer from {name} to /range", LOG_VERBOSE)
            return RangeResult(LOST if self._lost(peer, exchange) else FAILED, silent=silent, reason="no answer")
        if exchange.response is False:
            log(f"{name} is busy and refused /range", LOG_VERBOSE)
            return RangeResult(BUSY, reason="busy")
        if protocol.is_range_unserved(exchange.response):
            return RangeResult(OK, missing=True)
        parsed = protocol.parse_range_response(exchange.response, offset, max_bytes, compress, block_size)
        if parsed is None:
            log(f"{name} sent a malformed /range answer", LOG_WARNING)
            return RangeResult(FAILED, reason="malformed response")
        if peer.record_rate(exchange, self.config.min_rate_sample):
            log(f"link to {name} measured at {size(peer.rate / 8)}/s", LOG_VERBOSE)
            self.traffic.rated(node_id, peer.rate)
        total, data = parsed
        result = RangeResult(OK, size=total, data=data, wire=exchange.response_size or 0,
                             compressed=exchange.response[1] == RANGE_BZ2,
                             elapsed=max(0.0, self.links.clock() - exchange.sent_at))
        if offset + len(data) == total:
            block = held + data
            if cid.verify(block):
                result.block = block
            else:
                log(f"{name} sent ranges of {cid} not matching it", LOG_WARNING)
                result.bad = True
        return result

    @staticmethod
    def _lost(peer, exchange: Exchange) -> bool:
        """True if a /blocks request sent on peer went unanswered the way a lossy link loses one: RNS gave up on it
        with the link still up (it timed out: the request or the response's advertisement was lost, or the response
        transfer ran out of retries), or the link itself timed out under it. Not when it wasn't sent, or the link
        was closed by either end. A response RNS rejected at its advertisement (too large) fails the same way and
        can't be told from a lost one, so it is retried too: no more than a busy refusal costs (section 6). Also when
        this node closed the link for it, as nothing came back on it meanwhile (Exchange.went_dead): over LoRa a lone
        lost request looks just so, and failing the provider for it ended a fetch from its one provider."""
        if exchange.went_dead:
            return True
        if exchange.receipt is None and not peer.is_active():
            return True  # never sent: the link was closed while it waited for its slot (another request's stall)
        if peer.is_active():
            return exchange.receipt is not None
        return timed_out(peer.link)

    def _close_stalled(self, peer, what: str):
        """Close peer's link after a response on it stalled, which cancels the transfer so it doesn't compete with the
        next provider; unless other requests use it or wait for its slot, which a close would fail outright (REL-7):
        they have it now, and if it is dead they come back lost and are asked again on a new link."""
        if peer.used_by_others({threading.get_ident()}):
            log(f"{what}; leaving the link to the other requests on it", LOG_WARNING)
            return
        log(f"{what}; closing the link", LOG_WARNING)
        self.links.close_link(peer)

    def _stall_watchdog(self, rate: float | None) -> Callable[[Exchange, float], str | None]:
        """Stall test for a /blocks response on a link with this rate estimate (section 7, step 5)."""
        def watchdog(exchange: Exchange, now: float) -> str | None:
            """Why the response has stalled, or None while it's fine."""
            if now - exchange.last_progress > self.config.stall_timeout:
                return f"no progress for {self.config.stall_timeout:g}s"
            if exchange.response_started is not None and exchange.response_size:
                # The same deadline the responder uses for a response of this size
                allowed = rates.response_deadline(exchange.response_size, rate, self.config)
                if now - exchange.response_started > allowed:
                    return f"missed its {allowed:.0f}s deadline"
            return None
        return watchdog

    @staticmethod
    def _check_blocks(name: str, batch: list[Cid], entries: list, rest: list[Cid]) -> BlocksResult:
        """Sort a parsed response's entries into verified blocks, missing ones and hash mismatches."""
        result = BlocksResult(OK)
        for cid, block in zip(batch, entries):
            if block is None:
                result.missing.append(cid)
            elif cid.verify(block):
                result.blocks[cid] = block
            else:
                result.bad.append(cid)
        if result.bad:
            log(f"{name} sent {plural(len(result.bad), 'block')} not matching the CID requested", LOG_WARNING)
        result.unsent = batch[len(entries):] + rest
        return result
