"""Requests to other nodes (sections 3, 6, 7): /find, /provide, /blocks with stall detection, /pin, and ping.

Every method blocks until it has a result, so never call one from an RNS callback thread. Lookups and
downloads use the node's shared unidentified link; /provide and /pin use a separate identified link,
closed when done.
"""
from dataclasses import dataclass, field
from typing import Callable

from rnfs.config import Config
from rnfs.constants import BLOCKS_PATH, FIND_PATH, PIN_PATH, PROVIDE_PATH
from rnfs.content.cid import Cid
from rnfs.log import LOG_VERBOSE, LOG_WARNING, log, node_name, plural, size
from rnfs.network import protocol, rates
from rnfs.network.links import Exchange, LinkPool
from rnfs import mesh

# BlocksResult statuses
OK = "ok"  # the node answered; see blocks, missing, unsent and bad
BUSY = "busy"  # refused (False) under the node's limits, or the node cancelled the response (displaced or
#               past its deadline, .reason "cancelled"); retry after a short delay. The link stays open
FAILED = "failed"  # no link, no answer, or a malformed answer
STALLED = "stalled"  # the response stopped making progress or missed its deadline; the link was closed


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


class PeerClient:
    """Sends rnfs requests to other nodes over links from a LinkPool and checks their answers."""

    def __init__(self, links: LinkPool, config: Config):
        """Client sending over links."""
        self.links = links
        self.config = config
        self.traffic = mesh.Traffic()  # requests and bytes per peer (mesh); the node shares it with the server
        # Every link opened (shared, ping's, identified) records its round trip, and that the node is up, there
        links.on_rtt = self.traffic.measured

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

    def close(self, node_id: bytes):
        """Close the shared link to node_id, cancelling any transfer on it."""
        self.links.close(node_id)

    def close_unless_used(self, node_id: bytes, own: set[int]) -> bool:
        """Close the shared link to node_id, as close() does, unless a request made from a thread not in own
        (thread idents: the caller's request threads) is in progress on it or waiting for a slot, so one fetch
        giving up on its request doesn't fail another's. True if closed (or there was no link); False if left
        open, its request left to finish. Never cancels a Resource: closing the link stops it (section 3)."""
        return self.links.close_unless_used(node_id, own)

    # /find

    def find(self, node_id: bytes, key: bytes) -> tuple[list[bytes], list[bytes]] | None:
        """Ask node_id for providers of key and the servers it knows closest to it.

        (providers, nodes), or None if the node couldn't be reached, didn't answer, refused (a DHT client
        answers False) or answered something malformed.
        """
        peer = self.links.get(node_id)
        if peer is None:
            self.traffic.request(node_id, mesh.FAILED)
            return None
        # Honest answers fit one packet; a larger one is refused at its advertisement (and links.py gives up
        # on one that is trickled)
        exchange = self.links.call(peer, FIND_PATH, protocol.encode_find(key),
                                   max_response_size=protocol.MAX_FIND_RESPONSE_SIZE)
        if not exchange.answered:
            log(f"no answer from {node_name(node_id)} to /find", LOG_VERBOSE)
            self.traffic.request(node_id, mesh.FAILED)
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
            log(f"/blocks from {name} stalled ({exchange.stalled}); closing the link", LOG_WARNING)
            # Closing the link cancels the transfer, so it doesn't compete with the next provider
            self.links.close_link(peer)
            return BlocksResult(STALLED, unsent=wanted, reason=exchange.stalled)
        if exchange.cancelled:
            # The responder gave the slot to another request or ran out of time, or the transfer was lost in
            # transit: like a refusal, not a stall
            log(f"{name}'s /blocks response stopped arriving (cancelled by it, or lost in transit)", LOG_VERBOSE)
            return BlocksResult(BUSY, unsent=wanted, reason="cancelled")
        if not exchange.answered:
            log(f"no answer from {name} to /blocks", LOG_VERBOSE)
            return BlocksResult(FAILED, unsent=wanted, reason="no answer")
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
        result = self._check_blocks(name, batch, entries, rest)
        # From when it was sent: not opening the link or waiting for its /blocks slot
        result.elapsed = max(0.0, self.links.clock() - exchange.sent_at)
        return result

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
