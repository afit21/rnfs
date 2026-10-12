"""Outgoing RNS links to other nodes (specification section 3).

- Requests to a node share one outgoing link, opened on demand and closed after link_idle_timeout without
  traffic (Link.no_data_for()) or being handed out by LinkPool.get(). These links don't identify, so lookups
  and downloads don't reveal who asks.
- A link goes only to a destination whose hash is the node ID asked for (rnfs.dht of the identity RNS
  recalls for it), so one identity can't pose as many node IDs by announcing other destinations.
- Publishing and /pin use a separate link that identifies, closed when done.
- In private mode every link identifies.
- Requesters stay within the per-link limits: at most MAX_REQUESTS_PER_LINK requests in progress on a link,
  at most one of them /blocks. A request waits for a free slot.
- RNS stops a request's timeout once a response Resource starts arriving, so a node could trickle an answer
  for hours while holding a slot. Requests other than /blocks therefore have a hard deadline from when they
  were sent (and wait no longer than that for a slot); /blocks has the fetcher's stall detection instead.
- A request answered False on an identified link is retried once after identifying again, since
  Link.identify() is unacknowledged.

Everything here blocks its caller, so never call it from an RNS callback thread.
"""
import threading
import time
from typing import Callable

import RNS

from rnfs.config import Config
from rnfs.constants import APP_NAME, DHT_ASPECT, MAX_REQUESTS_PER_LINK
from rnfs.log import LOG_VERBOSE, LOG_WARNING, log, node_name
from rnfs.network import guards, protocol, rates

POLL_INTERVAL = 0.25  # how often a waiting request checks its link, progress and stall conditions
CANCEL_GRACE = 2.0  # seconds a started response may be missing from the link's incoming Resources
ESTABLISH_GRACE = 5  # seconds past RNS's own establishment timeout before giving up on a new link
DEADLINE_FACTOR = 2  # a request other than /blocks may take this many times RNS's own request timeout
# What RNS adds to rtt × traffic_timeout_factor for its request timeout (Link.request with timeout=None)
RESPONSE_GRACE = protocol.REQUEST_TIMEOUT_WITHOUT_RTT


class Exchange:
    """One request on a link: RNS callbacks fill it in while the requesting thread waits on it."""

    def __init__(self, clock: Callable[[], float]):
        """A request not yet sent."""
        self.clock = clock
        self.done = threading.Event()
        self.answered = False  # a response arrived; it is in .response (which may be False, a refusal)
        self.response = None
        self.response_size: int | None = None  # bytes RNS reported for the response
        self.expected_rate: float | None = None  # Link.get_expected_rate() as the response completed
        self.progress = 0.0
        self.sent_at = clock()
        self.last_progress = self.sent_at
        self.response_started: float | None = None  # when the first part of a Resource response arrived
        self.stalled: str | None = None  # why the requester gave up waiting, if it did
        self.cancelled = False  # the responder cancelled the response Resource (e.g. displaced it)
        self.receipt = None  # the RNS RequestReceipt, once sent

    # RNS callbacks: they run on RNS threads, so they only record and never block

    def on_response(self, receipt):
        """Record the response and the link's measured rate, which the next Resource would overwrite."""
        try:
            self.expected_rate = receipt.link.get_expected_rate()
        except Exception:
            self.expected_rate = None
        self.response_size = receipt.response_size
        self.response = receipt.get_response()
        self.progress = 1.0
        self.answered = True
        self.done.set()

    def on_failed(self, receipt):
        """The request timed out, its response was refused as too large, or the response transfer failed."""
        self.done.set()

    def on_progress(self, receipt):
        """Note progress receiving the response, for stall detection."""
        progress = receipt.get_progress()
        now = self.clock()
        if self.response_started is None:
            self.response_started = now
        if receipt.response_size:
            self.response_size = receipt.response_size
        if progress > self.progress:
            self.progress = progress
            self.last_progress = now


class PeerLink:
    """One outgoing link to a node and the requests in progress on it."""

    def __init__(self, node_id: bytes, identified: bool, shared: bool):
        """A link not yet opened; LinkPool sets .link once RNS.Link is created."""
        self.node_id = node_id
        self.identified = identified
        self.shared = shared
        self.link = None
        self.rate: float | None = None  # rate estimate in bits/s (section 2)
        self.requests = 0
        self.blocks = 0
        self.closed = False
        self.handed_out: float | None = None  # when LinkPool.get() last returned it (the pool's clock)
        self.cond = threading.Condition()

    @property
    def link_id(self) -> bytes | None:
        """RNS link ID, or None before the link exists."""
        return getattr(self.link, "link_id", None)

    def is_active(self) -> bool:
        """True while the link is up."""
        return not self.closed and self.link is not None and self.link.status == RNS.Link.ACTIVE

    def mdu(self) -> int | None:
        """The link's packet MDU, which bounds request sizes."""
        try:
            return self.link.get_mdu()
        except Exception:
            return None

    def mtu(self) -> int | None:
        """The link's MTU, which classifies it as slow before it has a rate estimate."""
        try:
            return self.link.get_mtu()
        except Exception:
            return None

    def acquire(self, blocks: bool, timeout: float | None = None) -> bool:
        """Wait for a request slot (and the /blocks slot if blocks). False if the link closed or timeout passed."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self.cond:
            while self.requests >= MAX_REQUESTS_PER_LINK or (blocks and self.blocks >= 1):
                if not self.is_active():
                    return False
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                # Bounded waits, so a link that dies without a closed callback is still noticed
                self.cond.wait(POLL_INTERVAL if remaining is None else min(remaining, POLL_INTERVAL))
            if not self.is_active():
                return False
            self.requests += 1
            if blocks:
                self.blocks += 1
            return True

    def release(self, blocks: bool):
        """Free the slot taken by acquire."""
        with self.cond:
            self.requests -= 1
            if blocks:
                self.blocks -= 1
            self.cond.notify_all()

    def busy(self) -> bool:
        """True if a request is in progress."""
        with self.cond:
            return self.requests > 0

    def mark_closed(self):
        """The link closed: wake everyone waiting for a slot."""
        with self.cond:
            self.closed = True
            self.cond.notify_all()

    def record_rate(self, exchange: Exchange, min_rate_sample: int) -> bool:
        """Keep the rate RNS measured for a completed response if it was large enough. True if kept."""
        rate = rates.sample(exchange.response_size, exchange.expected_rate, min_rate_sample)
        if rate is None:
            return False
        self.rate = rate
        return True


class LinkPool:
    """Outgoing links: one shared link per node, plus one-shot identified links. Thread-safe."""

    def __init__(self, identity: RNS.Identity, config: Config, clock: Callable[[], float] = time.monotonic):
        """Empty pool; links identify as identity when they identify."""
        self.identity = identity
        self.config = config
        self.clock = clock
        self.shared: dict[bytes, PeerLink] = {}
        self.oneshot: set[PeerLink] = set()
        self.connecting: dict[bytes, threading.Event] = {}  # one thread opens a node's shared link at a time
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.sweeper: threading.Thread | None = None
        # Process-wide: reject response advertisements whose transfer outgrows their size
        guards.install()

    # Opening links

    def get(self, node_id: bytes) -> PeerLink | None:
        """The shared link to node_id, opened if needed; None if the node can't be reached.

        Blocks for up to path_timeout plus RNS's link establishment timeout.
        """
        with self.lock:
            peer = self.shared.get(node_id)
            if peer is not None and peer.is_active():
                peer.handed_out = self.clock()  # under the lock, so sweep() sees it before closing the link
                return peer
            if peer is not None:
                del self.shared[node_id]
            event = self.connecting.get(node_id)
            opener = event is None
            if opener:
                event = self.connecting[node_id] = threading.Event()

        if not opener:
            # Another thread is opening this link: use its result rather than opening a second link
            event.wait(self.config.path_timeout + 120)
            with self.lock:
                peer = self.shared.get(node_id)
                if peer is None or not peer.is_active():
                    return None
                peer.handed_out = self.clock()
                return peer

        peer = PeerLink(node_id, identified=self.config.private, shared=True)
        opened = False
        try:
            opened = self._open(peer)
        finally:
            with self.lock:
                if opened and peer.is_active():
                    peer.handed_out = self.clock()
                    self.shared[node_id] = peer
                self.connecting.pop(node_id, None)
            event.set()
        if not (opened and peer.is_active()):
            return None
        self._start_sweeper()
        return peer

    def open_identified(self, node_id: bytes) -> PeerLink | None:
        """A new link to node_id that identifies, for publishing or /pin. Close it with close_link when done."""
        return self.open_oneshot(node_id, identify=True)

    def open_oneshot(self, node_id: bytes, identify: bool) -> PeerLink | None:
        """A new link to node_id outside the shared pool (identifying if asked, or in private mode)."""
        peer = PeerLink(node_id, identified=identify or self.config.private, shared=False)
        with self.lock:
            self.oneshot.add(peer)
        if not self._open(peer):
            with self.lock:
                self.oneshot.discard(peer)
            return None
        return peer

    def _open(self, peer: PeerLink) -> bool:
        """Find a path and the identity for peer's node, establish the link, and identify if peer should."""
        node_id = peer.node_id
        name = node_name(node_id)
        if not RNS.Transport.has_path(node_id):
            log(f"requesting a path to {name}", LOG_VERBOSE)
        if not RNS.Transport.await_path(node_id, timeout=self.config.path_timeout):
            log(f"no path to {name} after {self.config.path_timeout:g}s")
            return False

        identity = self._recall(node_id)
        if identity is None:
            log(f"have a path to {name} but not its identity")
            return False

        destination = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE, APP_NAME, DHT_ASPECT)
        if destination.hash != node_id:
            # node_id is another destination of that identity, not its rnfs.dht: whoever holds it could
            # otherwise answer for many node IDs (ground near target keys) or dodge a ban on its real one
            log(f"the identity known for {name} isn't that node's (its rnfs.dht is "
                f"{node_name(destination.hash)}); not connecting", LOG_VERBOSE)
            return False
        # Set on close too, so a failed link doesn't wait out the full timeout
        settled = threading.Event()

        def on_closed(closed_link):
            """Forget the link; RNS runs this synchronously inside teardown, so it only takes our lock."""
            self._forget(peer)
            settled.set()
            log(f"link to {name} closed", LOG_VERBOSE)

        log(f"connecting to {name}", LOG_VERBOSE)
        link = RNS.Link(destination, established_callback=lambda _: settled.set(), closed_callback=on_closed)
        peer.link = link
        timeout = getattr(link, "establishment_timeout", None) or self.config.path_timeout
        settled.wait(timeout + ESTABLISH_GRACE)
        if link.status != RNS.Link.ACTIVE:
            log(f"could not connect to {name}")
            link.teardown()
            peer.mark_closed()
            return False

        if peer.identified:
            link.identify(self.identity)
        rtt = getattr(link, "rtt", None)
        log(f"connected to {name}" + (f" (round trip {rtt * 1000:.0f} ms)" if rtt else ""), LOG_VERBOSE)
        return True

    def _recall(self, node_id: bytes):
        """The node's identity from RNS, asking for a path again (whose response carries it) if needed."""
        identity = RNS.Identity.recall(node_id)
        if identity is not None:
            return identity
        RNS.Transport.request_path(node_id)
        deadline = time.monotonic() + self.config.path_timeout
        while identity is None and time.monotonic() < deadline:
            time.sleep(0.1)
            identity = RNS.Identity.recall(node_id)
        return identity

    def identify(self, peer: PeerLink):
        """Identify again on peer's link (unacknowledged, so it may need repeating)."""
        if peer.is_active():
            peer.link.identify(self.identity)

    # Closing links

    def _forget(self, peer: PeerLink):
        """Remove a closed link from the pool."""
        peer.mark_closed()
        with self.lock:
            if self.shared.get(peer.node_id) is peer:
                del self.shared[peer.node_id]
            self.oneshot.discard(peer)

    def close_link(self, peer: PeerLink | None):
        """Tear down peer's link, which also cancels transfers running over it."""
        if peer is None:
            return
        self._forget(peer)
        # Outside the lock: teardown runs the closed callback synchronously, and that takes the lock
        if peer.link is not None:
            peer.link.teardown()

    def close(self, node_id: bytes):
        """Tear down the shared link to node_id, if any (e.g. after a stalled transfer)."""
        with self.lock:
            peer = self.shared.get(node_id)
        self.close_link(peer)

    def close_all(self):
        """Stop the idle sweeper and tear down every link."""
        self.stop_event.set()
        with self.lock:
            peers = list(self.shared.values()) + list(self.oneshot)
        for peer in peers:
            self.close_link(peer)

    def sweep(self) -> int:
        """Close shared links with no request in progress, no data and not handed out by get() for
        link_idle_timeout. Returns how many."""
        timeout = self.config.link_idle_timeout
        with self.lock:
            peers = list(self.shared.values())
        idle = []
        for peer in peers:
            try:
                quiet = peer.link.no_data_for()
            except Exception:
                continue
            if not peer.busy() and quiet >= timeout:
                idle.append(peer)
        closing = []
        for peer in idle:
            with self.lock:
                # get() may have handed it out since (its caller is about to send a request): it marks the peer
                # under this lock, and once the peer is out of the pool here no get() can return it
                if peer.handed_out is not None and self.clock() - peer.handed_out < timeout:
                    continue
                if self.shared.get(peer.node_id) is peer:
                    del self.shared[peer.node_id]
            closing.append(peer)
        for peer in closing:
            log(f"closing idle link to {node_name(peer.node_id)}", LOG_VERBOSE)
            self.close_link(peer)
        return len(closing)

    def _start_sweeper(self):
        """Start the thread closing idle links, once."""
        with self.lock:
            if self.sweeper is not None or self.stop_event.is_set():
                return
            self.sweeper = threading.Thread(target=self._sweep_loop, name="rnfs-link-sweeper", daemon=True)
        self.sweeper.start()

    def _sweep_loop(self):
        """Sweep idle links until close_all."""
        interval = max(1.0, min(30.0, self.config.link_idle_timeout / 4))
        while not self.stop_event.wait(interval):
            try:
                self.sweep()
            except Exception as e:
                log(f"error closing idle links: {e}", LOG_WARNING)

    # Queries

    def has_link(self, node_id: bytes) -> bool:
        """True if there's an open shared link to node_id."""
        with self.lock:
            peer = self.shared.get(node_id)
        return peer is not None and peer.is_active()

    def open_nodes(self) -> list[bytes]:
        """Node IDs with an open shared link, most recently used first."""
        with self.lock:
            peers = [peer for peer in self.shared.values() if peer.is_active()]

        def quiet_for(peer: PeerLink) -> float:
            """Seconds since data crossed the link."""
            try:
                return peer.link.no_data_for()
            except Exception:
                return float("inf")

        return [peer.node_id for peer in sorted(peers, key=quiet_for)]

    # Requests

    def request(self, peer: PeerLink, path: str, data, blocks: bool = False, max_response_size: int | None = None,
                watchdog: Callable[[Exchange, float], str | None] | None = None,
                on_progress: Callable[[float], None] | None = None) -> Exchange:
        """Send one request on peer's link and wait for its outcome, within the per-link limits.

        watchdog(exchange, now) may return a reason to stop waiting (a stall). on_progress(fraction) is
        called from this thread, never an RNS one. A request other than /blocks gives up (.stalled set) once
        request_deadline has passed since it was sent, or if it waited that long for a slot.
        """
        exchange = Exchange(self.clock)
        deadline = None if blocks else self.request_deadline(peer)
        if not peer.acquire(blocks, timeout=deadline):
            if deadline is not None and peer.is_active():
                exchange.stalled = "no free request slot"
                log(f"no free request slot for {path} to {node_name(peer.node_id)} within {deadline:.0f}s",
                    LOG_VERBOSE)
            exchange.done.set()
            return exchange
        try:
            try:
                # timeout=None: RNS's RTT-based request timeout (section 3)
                receipt = peer.link.request(path, data, response_callback=exchange.on_response,
                                            failed_callback=exchange.on_failed,
                                            progress_callback=exchange.on_progress,
                                            timeout=None, max_response_size=max_response_size)
            except Exception as e:
                log(f"could not send {path} to {node_name(peer.node_id)}: {e}", LOG_VERBOSE)
                return exchange
            if not receipt:
                log(f"could not send {path} to {node_name(peer.node_id)}", LOG_VERBOSE)
                return exchange
            exchange.receipt = receipt
            exchange.sent_at = exchange.last_progress = self.clock()

            reported = 0.0
            missing_since = None
            while not exchange.done.wait(POLL_INTERVAL):
                if not peer.is_active():
                    break
                # A response Resource cancelled by the responder never fails the receipt (RNS ignores the
                # timeout once a response is being received), so notice that it's gone from the link
                if exchange.response_started is not None and not exchange.answered:
                    if self._receiving(peer.link, getattr(receipt, "request_id", None)):
                        missing_since = None
                    elif missing_since is None:
                        missing_since = self.clock()
                    elif self.clock() - missing_since > CANCEL_GRACE:
                        exchange.cancelled = True
                        break
                if on_progress is not None and exchange.progress > reported:
                    reported = exchange.progress
                    on_progress(reported)
                if watchdog is not None:
                    reason = watchdog(exchange, self.clock())
                    if reason:
                        exchange.stalled = reason
                        break
                if deadline is not None and self.clock() - exchange.sent_at > deadline:
                    # Only reachable while RNS is receiving a response Resource: otherwise its own timeout
                    # fails the request first. Honest answers to these requests fit one packet.
                    exchange.stalled = f"no answer within {deadline:.0f}s"
                    log(f"{node_name(peer.node_id)} took over {deadline:.0f}s to answer {path}; giving up",
                        LOG_VERBOSE)
                    break
            if exchange.answered and on_progress is not None and reported < 1.0:
                on_progress(1.0)
            return exchange
        finally:
            peer.release(blocks)

    @staticmethod
    def request_deadline(peer: PeerLink) -> float:
        """Seconds a request other than /blocks may take from being sent: DEADLINE_FACTOR × RNS's own
        RTT-based request timeout for peer's link."""
        link = peer.link
        rtt = getattr(link, "rtt", None) or 0
        factor = getattr(link, "traffic_timeout_factor", None) or RNS.Link.TRAFFIC_TIMEOUT_FACTOR
        return DEADLINE_FACTOR * (rtt * factor + RESPONSE_GRACE)

    @staticmethod
    def _receiving(link, request_id) -> bool:
        """True if link has an incoming Resource for the response to request_id (or we can't tell)."""
        incoming = getattr(link, "incoming_resources", None)
        if incoming is None or request_id is None:
            return True
        return any(getattr(resource, "request_id", None) == request_id for resource in list(incoming))

    def call(self, peer: PeerLink, path: str, data, **kwargs) -> Exchange:
        """request(), retried once after identifying again if an identified link answers False (section 3)."""
        exchange = self.request(peer, path, data, **kwargs)
        if exchange.answered and exchange.response is False and peer.identified and peer.is_active():
            log(f"{node_name(peer.node_id)} refused {path} on an identified link; identifying again and retrying",
                LOG_VERBOSE)
            self.identify(peer)
            exchange = self.request(peer, path, data, **kwargs)
        return exchange
