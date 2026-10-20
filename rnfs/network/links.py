"""Outgoing RNS links to other nodes (specification section 3).

- Requests to a node share one outgoing link, opened on demand and closed once idle: no request in progress, no data
  (Link.no_data_for()) and not handed out by LinkPool.get() for its idle limit (idle_limit), nor held (LinkPool.hold: a
  requester waiting out the provider's upload pacing, so the next response keeps the link's rate estimate). The idle
  limit is link_idle_timeout for a link holding a rate estimate; any other closes just before RNS would send its first
  keepalive, if that is sooner, since reopening it costs less than two keepalive pairs. These links don't identify, so
  lookups and downloads don't reveal who asks.
- A link goes only to a destination whose hash is the node ID asked for (rnfs.dht of the identity RNS
  recalls for it), so one identity can't pose as many node IDs by announcing other destinations.
- Publishing and /pin use a separate link that identifies, closed when done.
- In private mode every link identifies, and links go only to members (identities on `members`).
- Requesters stay within the per-link limits: at most MAX_REQUESTS_PER_LINK requests in progress on a link,
  at most one of them /blocks. A request waits for a free slot.
- RNS stops a request's timeout once a response Resource starts arriving, so a node could trickle an answer
  for hours while holding a slot. Requests other than /blocks therefore have a hard deadline from when they
  were sent (and wait no longer than that for a slot); /blocks has the fetcher's stall detection instead.
- A request answered False on an identified link is retried once after identifying again, since
  Link.identify() is unacknowledged.
- A shared link that has a path and identity but isn't established is remembered (LinkPool.unestablished): on a
  lossy path its link request or proof may have been lost, so the fetcher tries again rather than give up on the
  node (specification section 6, unanswered requests). One the node closes (closed_by_other: RNS's teardown_reason) is
  a refusal (LinkPool.refused), not a failure.
- A request that wouldn't fit one packet on its link isn't sent: RNS would send it as a Resource, which nodes
  refuse (network/guards.py) and RNS may compress.
- Every path request goes through the node's budget (LinkPool.paths, network/paths.py), whose wait doesn't count
  towards path_timeout; a link that fails to establish costs a turn of it, as RNS then requests the path again.
- A response that arrives before RNS has registered its request is kept and handed over once it is (EarlyResponses):
  RNS (1.5.6) adds a request's RequestReceipt to the link's pending requests only after Link.request has sent the
  packet, and drops a response no pending request matches, so on a fast link (a 1 ms round trip, a busy process)
  an answer could be lost and its request wait out RNS's whole timeout, 11 s at least.

Everything here blocks its caller, so never call it from an RNS callback thread.
"""
import threading
import time
from collections import OrderedDict
from typing import Callable

import RNS

from rnfs.config import Config
from rnfs.constants import APP_NAME, DHT_ASPECT, MAX_REQUESTS_PER_LINK
from rnfs import log as rnfs_log
from rnfs.log import LOG_DEBUG, LOG_VERBOSE, LOG_WARNING, log, node_name, size
from rnfs.network import guards, protocol, rates
from rnfs.network.paths import PathRequests

POLL_INTERVAL = 0.25  # how often a waiting request checks its link, progress and stall conditions
CANCEL_GRACE = 2.0  # seconds a started response may be missing from the link's incoming Resources
ESTABLISH_GRACE = 5  # seconds past RNS's own establishment timeout before giving up on a new link
DEADLINE_FACTOR = 2  # a request other than /blocks may take this many times RNS's own request timeout
# What RNS adds to rtt × traffic_timeout_factor for its request timeout (Link.request with timeout=None)
RESPONSE_GRACE = protocol.REQUEST_TIMEOUT_WITHOUT_RTT
# Link.teardown_reason: a timeout, or which end closed it (the one that opened it, or the other)
LINK_TIMEOUT, INITIATOR_CLOSED, DESTINATION_CLOSED = (RNS.Link.TIMEOUT, RNS.Link.INITIATOR_CLOSED,
                                                       RNS.Link.DESTINATION_CLOSED)
RECEIPT_FAILED = RNS.RequestReceipt.FAILED  # a request RNS gave up on
MAX_NOT_ESTABLISHED = 1024  # nodes LinkPool remembers a link not established to (LinkPool.unestablished)
# An idle link without a rate estimate is closed this many seconds, plus two round trips, before RNS's keepalive
# interval has passed (idle_limit): room for the sweep's own timing
KEEPALIVE_MARGIN = 1.0
MIN_SWEEP_WAIT = 0.25  # the idle sweep runs at most this often, however soon a link is due
# Responses no pending request matched (EarlyResponses): kept this many seconds, at most this many per link. One
# arriving early is handed over within moments; the rest are late answers to requests RNS already gave up on
EARLY_RESPONSE_LIFE = 30.0
MAX_EARLY_RESPONSES = 16


def close_reason(link) -> str:
    """Why link closed, as the log says it: timed out, or closed by this node or the other. RNS names the end
    that closed it by its role, so for a link the other node opened (link.initiator false) they swap."""
    reason = getattr(link, "teardown_reason", None)
    if reason == LINK_TIMEOUT:
        return "timed out"
    if closed_by_other(link):
        return "closed by the other node"
    if reason in (INITIATOR_CLOSED, DESTINATION_CLOSED):
        return "closed by this node"
    return "closed"


def closed_by_other(link) -> bool:
    """True if the other node closed link (RNS's teardown_reason, which names the end by its role): for a link this
    node opened, a server refusing it (no room for another incoming link) or going away. A refusal, not a failure
    (specification section 6): the node is up."""
    theirs = DESTINATION_CLOSED if getattr(link, "initiator", True) else INITIATOR_CLOSED
    return getattr(link, "teardown_reason", None) == theirs


def idle_limit(link_idle_timeout: float, keepalive, rtt, rated: bool) -> float:
    """Seconds without data after which the idle sweep closes a shared link with nothing in progress.

    The node that opens a link sends a keepalive once RNS's keepalive interval (RNS 1.5.6: max(5 s, RTT × 360 / 1.75),
    at most 360 s; Link.keepalive) has passed without a packet either way, and the other node answers: on a 50 ms path a
    pair every ~10 s, about 58 packets in link_idle_timeout (5 minutes), while reopening the link costs about 4. So a
    link closes just before its first keepalive, if that is sooner than link_idle_timeout: KEEPALIVE_MARGIN and two
    round trips early, since RNS counts the interval from the earlier of the last packet in and out (the request, for a
    link that was waiting for an answer) and no_data_for from the later (the answer). A link holding a rate estimate
    (rated: it carried /blocks responses) keeps link_idle_timeout, as does one whose keepalive interval isn't known:
    closing it would take the estimate, and the provider's for its next response, with it (section 3, Links).
    """
    if rated or not _positive(keepalive):
        return link_idle_timeout
    early = keepalive - KEEPALIVE_MARGIN - 2 * (rtt if _positive(rtt) else 0.0)
    return max(KEEPALIVE_MARGIN, min(link_idle_timeout, early))


def _positive(value) -> bool:
    """True if value is a finite positive number (not a bool)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and 0 < value < float("inf")


def timed_out(link) -> bool:
    """True if link closed because it timed out (RNS heard nothing on it for too long), not closed by either end."""
    return getattr(link, "teardown_reason", None) == LINK_TIMEOUT


def _debug_logged() -> bool:
    """True if a LOG_DEBUG message is kept anywhere: RNS logs at debug level, or the debug log (which keeps
    every message, whatever RNS's loglevel) is on."""
    return RNS.loglevel >= LOG_DEBUG or rnfs_log._debug_sink is not None


def _reticulum():
    """The running Reticulum instance as this module's RNS knows it (None if none runs): the path-request budget's
    ceiling reads its interface stats."""
    reticulum = getattr(RNS, "Reticulum", None)
    return reticulum.get_instance() if reticulum is not None else None


def path_description(node_id: bytes) -> str:
    """How far RNS's path to node_id goes and through which interface, for the log ("" if it can't tell)."""
    try:
        hops = RNS.Transport.hops_to(node_id)
        interface = RNS.Transport.next_hop_interface(node_id)
    except Exception:  # noqa: BLE001 - RNS internals: no description if they fail
        return ""
    if hops >= RNS.Transport.PATHFINDER_M:
        return ""
    return f"{hops} hop{'s' if hops != 1 else ''}" + (f" via {interface}" if interface is not None else "")


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
        # The response Resource left the link unfinished: the responder cancelled it (e.g. displaced it), or RNS
        # lost it in transit; the requester can't tell which
        self.cancelled = False
        self.receipt = None  # the RNS RequestReceipt, once sent

    # RNS callbacks: they run on RNS threads, so they only record and never block

    def on_response(self, receipt):
        """Record the response and the link's measured rate, which the next Resource would overwrite."""
        try:
            self.expected_rate = receipt.link.get_expected_rate()
        except Exception:  # noqa: BLE001 - RNS internals: no rate if they fail
            self.expected_rate = None
        # RNS sizes a one-packet response as its msgpack length less 2, so one of a byte ([], False) is -1
        self.response_size = None if receipt.response_size is None else max(0, receipt.response_size)
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


class EarlyResponses:
    """Responses to requests on one RNS link that arrived before RNS registered the request (see the module
    docstring): installed in place of the link's handle_response, which RNS looks up on the link for every response.
    Thread-safe."""

    def __init__(self, link, clock: Callable[[], float] = time.monotonic):
        """Take over link's handle_response."""
        self.link = link
        self.clock = clock
        self.lock = threading.Lock()
        self.kept: OrderedDict[bytes, tuple[float, tuple, dict]] = OrderedDict()  # request ID -> (when, arguments)
        self.handle = link.handle_response  # RNS's own
        link.handle_response = self.on_response

    @classmethod
    def install(cls, link) -> "EarlyResponses | None":
        """An EarlyResponses for link, or None if link isn't one (a test's stand-in without RNS's pending requests)."""
        if not isinstance(getattr(link, "pending_requests", None), list) or not callable(
                getattr(link, "handle_response", None)):
            return None
        return cls(link)

    def on_response(self, request_id, *args, **kwargs):
        """Link.handle_response: handled by RNS if a pending request matches it, else kept for deliver()."""
        with self.lock:
            # Under the lock: deliver() looks for a kept response only once its request is pending, so a response
            # is either seen pending here or kept before deliver() looks
            if not any(getattr(pending, "request_id", None) == request_id
                       for pending in list(self.link.pending_requests)):
                now = self.clock()
                self.kept[request_id] = (now, args, kwargs)
                self.kept.move_to_end(request_id)
                while self.kept and (len(self.kept) > MAX_EARLY_RESPONSES
                                     or now - next(iter(self.kept.values()))[0] > EARLY_RESPONSE_LIFE):
                    self.kept.popitem(last=False)
                return None
        return self.handle(request_id, *args, **kwargs)

    def deliver(self, receipt) -> bool:
        """Hand RNS the response kept for receipt's request, which Link.request has just registered: True if there
        was one (its callbacks then run on this thread)."""
        request_id = getattr(receipt, "request_id", None)
        with self.lock:
            kept = self.kept.pop(request_id, None)
        if kept is None:
            return False
        self.handle(request_id, *kept[1], **kept[2])
        return True


class PeerLink:
    """One outgoing link to a node and the requests in progress on it."""

    def __init__(self, node_id: bytes, identified: bool, shared: bool):
        """A link not yet opened; LinkPool sets .link once RNS.Link is created."""
        self.node_id = node_id
        self.identified = identified
        self.shared = shared
        self.link = None
        self.early: EarlyResponses | None = None  # the link's responses that came before their request was registered
        self.rate: float | None = None  # rate estimate in bits/s (section 2)
        self.requests = 0
        self.blocks = 0
        # Threads with a request in progress on the link or waiting for a slot -> how many each: who would lose a
        # request if the link were closed now (PeerClient.close_unless_used)
        self.users: dict[int, int] = {}
        self.closed = False
        # The node had a path and its identity, but the link wasn't established: its request or proof may have been
        # lost on the way (LinkPool._open), rather than the node being out of reach
        self.unestablished = False
        self.refused = False  # the node closed the link before it was established at this end (closed_by_other)
        self.handed_out: float | None = None  # when LinkPool.get() last returned it (the pool's clock)
        self.held_until: float | None = None  # LinkPool.hold: in use until then, as if handed out then
        self.cond = threading.Condition()

    def is_active(self) -> bool:
        """True while the link is up."""
        return not self.closed and self.link is not None and self.link.status == RNS.Link.ACTIVE

    def mdu(self) -> int | None:
        """The link's packet MDU, which bounds request sizes."""
        try:
            return self.link.get_mdu()
        except Exception:  # noqa: BLE001 - RNS internals: no MDU if they fail
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

    def enter(self):
        """The calling thread is about to wait for a slot and send a request (LinkPool.request)."""
        ident = threading.get_ident()
        with self.cond:
            self.users[ident] = self.users.get(ident, 0) + 1

    def leave(self):
        """The calling thread's request is over, sent or not."""
        ident = threading.get_ident()
        with self.cond:
            if self.users.get(ident, 0) <= 1:
                self.users.pop(ident, None)
            else:
                self.users[ident] -= 1

    def used_by_others(self, own: set[int]) -> bool:
        """True if a thread not in own (thread idents) has a request in progress on the link or waiting for a slot."""
        with self.cond:
            return any(ident not in own for ident in self.users)

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

    def __init__(self, identity: RNS.Identity, config: Config, clock: Callable[[], float] = time.monotonic,
                 paths: PathRequests | None = None):
        """Empty pool; links identify as identity when they identify. paths is the node's path-request budget (default:
        a new one over this module's RNS, which tests replace)."""
        self.identity = identity
        self.config = config
        self.clock = clock
        self.paths = paths if paths is not None else PathRequests(transport=lambda: RNS.Transport, reticulum=_reticulum,
                                                                  slow_bitrate=config.slow_bitrate)
        # Private mode links only to members: every link identifies, and an outsider mustn't learn who we are
        self.members = config.member_set
        self.shared: dict[bytes, PeerLink] = {}
        self.oneshot: set[PeerLink] = set()
        self.connecting: dict[bytes, threading.Event] = {}  # one thread opens a node's shared link at a time
        # Nodes whose last shared link get() tried to open had a path and identity but wasn't established
        self.not_established: set[bytes] = set()
        # Nodes that closed the last shared link get() tried to open before it was established here (refused)
        self.closed_by: set[bytes] = set()
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.sweeper: threading.Thread | None = None
        # Set to run the idle sweep now: a request ended or a link was handed out, so a link may be due sooner than
        # the sweep's next run (next_sweep), which the last sweep worked out from the links it kept
        self.wake = threading.Event()
        self.next_sweep = self._sweep_interval()
        # Called with (node ID, seconds) for the round trip RNS measured on each link established, shared, one-shot
        # (ping's) or identified, on the thread that opened it; PeerClient sets its traffic counters' (`rnfs peers`)
        self.on_rtt: Callable[[bytes, float], None] | None = None
        # Reject response advertisements whose transfer outgrows their size (in the scope the node set)
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
                self.wake.set()  # it may now be due sooner than the sweep's next run
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
                for marked, flag in ((self.not_established, peer.unestablished), (self.closed_by, peer.refused)):
                    if flag:
                        if len(marked) >= MAX_NOT_ESTABLISHED:
                            marked.clear()  # only the latest attempts matter; keeps it bounded
                        marked.add(node_id)
                    else:
                        marked.discard(node_id)
                self.connecting.pop(node_id, None)
            event.set()
        if not (opened and peer.is_active()):
            return None
        self._start_sweeper()
        self.wake.set()  # a new link may be due before the sweep's next run
        return peer

    def unestablished(self, node_id: bytes) -> bool:
        """True if the last time get() tried to open a shared link to node_id (and returned None), the node had a path
        and its identity but the link wasn't established: on a lossy path its request or proof may have been lost,
        so the next try may well succeed (PeerClient counts it as a lost request)."""
        with self.lock:
            return node_id in self.not_established

    def refused(self, node_id: bytes) -> bool:
        """True if the last time get() tried to open a shared link to node_id (and returned None), the node closed it:
        a refusal (closed_by_other), not a failure."""
        with self.lock:
            return node_id in self.closed_by

    def await_path(self, node_id: bytes, timeout: float | None = None) -> bool:
        """Wait up to timeout (path_timeout if None) for a path to node_id, requesting one in its turn if RNS has none
        (as opening a link would): True once there is one. A lookup asks it before a query, to see how many hops the
        path takes (rnfs.mesh.SlowPaths), on a node that isn't slow for PathRequests.candidate_wait. A wait shorter
        than path_timeout isn't measured for it.
        """
        if RNS.Transport.has_path(node_id):
            return True
        wait = self.config.path_timeout if timeout is None else timeout
        log(f"requesting a path to {node_name(node_id)}", LOG_VERBOSE)
        if self.paths.await_path(node_id, wait, cancel=self.stop_event, measure=wait >= self.config.path_timeout):
            return True
        log(f"no path to {node_name(node_id)} after {wait:g}s")
        return False

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
        if not self.paths.await_path(node_id, self.config.path_timeout, cancel=self.stop_event):
            log(f"no path to {name} after {self.config.path_timeout:g}s")
            return False
        if _debug_logged():  # RNS path lookups, only for a message that will be kept
            log(f"path to {name}: {path_description(node_id) or 'known'}", LOG_DEBUG)

        identity = self._recall(node_id)
        if identity is None:
            log(f"have a path to {name} but not its identity (none after {self.config.path_timeout:g}s)")
            return False

        destination = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE, APP_NAME, DHT_ASPECT)
        if destination.hash != node_id:
            # node_id is another destination of that identity, not its rnfs.dht: whoever holds it could
            # otherwise answer for many node IDs (ground near target keys) or dodge a ban on its real one
            log(f"the identity known for {name} isn't that node's (its rnfs.dht is "
                f"{node_name(destination.hash)}); not connecting", LOG_VERBOSE)
            return False
        if self.config.private and identity.hash.hex() not in self.members:
            log(f"{name} isn't a member; not connecting in private mode", LOG_VERBOSE)
            return False
        # Set on close too, so a failed link doesn't wait out the full timeout
        settled = threading.Event()

        def on_closed(closed_link):
            """Forget the link; RNS runs this synchronously inside teardown, so it only takes our lock."""
            self._forget(peer)
            settled.set()
            log(f"link to {name} {close_reason(closed_link)}", LOG_VERBOSE)

        log(f"connecting to {name}", LOG_VERBOSE)
        started = time.monotonic()
        link = RNS.Link(destination, established_callback=lambda _: settled.set(), closed_callback=on_closed)
        peer.early = EarlyResponses.install(link)
        peer.link = link
        timeout = getattr(link, "establishment_timeout", None) or self.config.path_timeout
        settled.wait(timeout + ESTABLISH_GRACE)
        if link.status == RNS.Link.CLOSED and closed_by_other(link):
            # The node refused it (no room for another incoming link, say): it is up, and RNS doesn't request the path
            # again for a link that was established at its end
            log(f"{name} closed the link as it was established", LOG_VERBOSE)
            peer.mark_closed()
            peer.refused = True
            return False
        if link.status != RNS.Link.ACTIVE:
            route = path_description(node_id)
            why = (close_reason(link) if link.status == RNS.Link.CLOSED
                   else f"not established within {time.monotonic() - started:.0f}s")
            log(f"could not connect to {name}: {why}" + (f" ({route})" if route else ""))
            link.teardown()
            peer.mark_closed()
            peer.unestablished = True
            self.paths.charge(node_id)  # RNS now requests the path again by itself
            return False

        if peer.identified:
            link.identify(self.identity)
        rtt = getattr(link, "rtt", None)
        log(f"connected to {name}" + (f" (round trip {rtt * 1000:.0f} ms)" if rtt else ""), LOG_VERBOSE)
        self._report_rtt(node_id, rtt)
        return True

    def _report_rtt(self, node_id: bytes, rtt):
        """Hand on_rtt the round trip of a link just established to node_id, if it is a finite, non-negative number
        (RNS measures it on a link this node opens: the time from its request to the other node's proof)."""
        listener = self.on_rtt
        if listener is None or not isinstance(rtt, (int, float)) or isinstance(rtt, bool):
            return
        if 0 <= rtt < float("inf"):
            listener(node_id, float(rtt))

    def _recall(self, node_id: bytes):
        """The node's identity from RNS, asking for a path again (whose response carries it) in its turn if needed.

        A recall marks the destination used, which keeps it in RNS's known destinations for longer (and is
        an RPC of its own on a shared instance): right for a node being contacted, but not for each of the
        polls while its identity is awaited, so those don't mark it, and one recall marks it once it is there.
        """
        identity = RNS.Identity.recall(node_id)
        if identity is not None:
            return identity
        if self.paths.await_known(node_id, lambda: RNS.Identity.recall(node_id, _no_use=True) is not None,
                                  self.config.path_timeout, cancel=self.stop_event):
            return RNS.Identity.recall(node_id) or RNS.Identity.recall(node_id, _no_use=True)
        return None

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

    def shared_peer(self, node_id: bytes) -> PeerLink | None:
        """The shared link to node_id (whatever its state), or None."""
        with self.lock:
            return self.shared.get(node_id)

    def hold(self, node_id: bytes, seconds: float) -> bool:
        """Count the shared link to node_id as in use for seconds more (a requester waiting out a provider's upload
        pacing before its next request): sweep() closes it only link_idle_timeout after that, whatever its idle limit.
        False if there is no such link."""
        with self.lock:
            peer = self.shared.get(node_id)
            if peer is None:
                return False
            until = self.clock() + max(0.0, seconds)
            peer.held_until = until if peer.held_until is None else max(peer.held_until, until)
            return True

    def close(self, node_id: bytes):
        """Tear down the shared link to node_id, if any (e.g. after a stalled transfer)."""
        self.close_link(self.shared_peer(node_id))

    def close_unless_used(self, node_id: bytes, own: set[int]) -> bool:
        """Tear down the shared link to node_id unless a thread not in own (thread idents) has a request in
        progress on it or waiting for a slot. True if there was nothing else on it (the link, if any, is closed)."""
        peer = self.shared_peer(node_id)
        if peer is not None and peer.used_by_others(own):
            return False
        self.close_link(peer)
        return True

    def close_all(self):
        """Stop the idle sweeper and tear down every link."""
        self.stop_event.set()
        self.wake.set()
        with self.lock:
            peers = list(self.shared.values()) + list(self.oneshot)
        for peer in peers:
            self.close_link(peer)

    def idle_limit(self, peer: PeerLink) -> float:
        """Seconds without data after which sweep() closes peer's link if nothing is in progress on it (idle_limit():
        link_idle_timeout with a rate estimate, else at most until just before RNS's first keepalive)."""
        link = peer.link
        return idle_limit(self.config.link_idle_timeout, getattr(link, "keepalive", None), getattr(link, "rtt", None),
                          peer.rate is not None)

    def sweep(self) -> int:
        """Close shared links with no request in progress that have had no data and not been handed out by get() for
        their idle limit (idle_limit), nor been held (hold) within link_idle_timeout. Returns how many. Sets
        next_sweep: seconds until the next link kept may be due, at least MIN_SWEEP_WAIT and at most the sweep
        interval (a request ending or a link handed out wakes the sweeper sooner)."""
        timeout = self.config.link_idle_timeout
        wait = self._sweep_interval()
        with self.lock:
            peers = list(self.shared.values())
        idle = []
        for peer in peers:
            try:
                quiet = peer.link.no_data_for()
            except Exception:  # noqa: BLE001 - a link closing meanwhile is skipped
                continue
            if peer.busy():
                continue  # its request ending wakes the sweeper
            limit = self.idle_limit(peer)
            if quiet >= limit:
                idle.append((peer, limit))
            else:
                wait = min(wait, limit - quiet)
        closing = []
        for peer, limit in idle:
            with self.lock:
                # get() may have handed it out since (its caller is about to send a request): it marks the peer
                # under this lock, and once the peer is out of the pool here no get() can return it
                now = self.clock()
                if peer.handed_out is not None and now - peer.handed_out < limit:
                    wait = min(wait, peer.handed_out + limit - now)
                    continue
                if peer.held_until is not None and now - peer.held_until < timeout:
                    wait = min(wait, peer.held_until + timeout - now)
                    continue
                if self.shared.get(peer.node_id) is peer:
                    del self.shared[peer.node_id]
            closing.append(peer)
        for peer in closing:
            log(f"closing idle link to {node_name(peer.node_id)}", LOG_VERBOSE)
            self.close_link(peer)
        self.next_sweep = max(MIN_SWEEP_WAIT, wait)
        return len(closing)

    def _sweep_interval(self) -> float:
        """The longest the sweeper waits between sweeps: a quarter of link_idle_timeout, within 1 to 30 s."""
        return max(1.0, min(30.0, self.config.link_idle_timeout / 4))

    def _start_sweeper(self):
        """Start the thread closing idle links, once."""
        with self.lock:
            if self.sweeper is not None or self.stop_event.is_set():
                return
            self.sweeper = threading.Thread(target=self._sweep_loop, name="rnfs-link-sweeper", daemon=True)
        self.sweeper.start()

    def _sweep_loop(self):
        """Sweep idle links until close_all: when the last sweep said the next link is due, or when woken."""
        wait = self._sweep_interval()
        while True:
            self.wake.wait(wait)
            # Cleared before sweeping: a request ending during the sweep wakes the next wait at once
            self.wake.clear()
            if self.stop_event.is_set():
                return
            try:
                self.sweep()
                wait = self.next_sweep
            except Exception as e:  # noqa: BLE001 - the sweeper thread keeps running; logged
                log(f"error closing idle links: {e}", LOG_WARNING)
                wait = self._sweep_interval()

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
            except Exception:  # noqa: BLE001 - a link closing meanwhile sorts last
                return float("inf")

        return [peer.node_id for peer in sorted(peers, key=quiet_for)]

    # Requests

    def request(self, peer: PeerLink, path: str, data, blocks: bool = False, max_response_size: int | None = None,
                watchdog: Callable[[Exchange, float], str | None] | None = None,
                on_progress: Callable[[float], None] | None = None) -> Exchange:
        """Send one request on peer's link and wait for its outcome, within the per-link limits.

        watchdog(exchange, now) may return a reason to stop waiting (a stall). on_progress(fraction) is
        called from this thread, never an RNS one. A request other than /blocks gives up (.stalled set) once
        request_deadline has passed since it was sent, or if it waited that long for a slot. A request larger
        than the link's packet MDU isn't sent (the exchange is returned unanswered).
        """
        peer.enter()
        try:
            return self._request(peer, path, data, blocks, max_response_size, watchdog, on_progress)
        finally:
            peer.leave()

    def _request(self, peer: PeerLink, path: str, data, blocks: bool, max_response_size: int | None,
                 watchdog: Callable[[Exchange, float], str | None] | None,
                 on_progress: Callable[[float], None] | None) -> Exchange:
        """request(), with the calling thread counted among the link's users."""
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
            mdu = peer.mdu()
            if mdu is not None and protocol.request_size(path, data) > mdu:
                # Link.request would send it as a Resource, which nodes refuse, and which RNS compresses unless
                # the daemon's process-wide limit is set: every rnfs request fits one packet (section 3)
                log(f"not sending {path} to {node_name(peer.node_id)}: it doesn't fit one packet", LOG_WARNING)
                return exchange
            try:
                # timeout=None: RNS's RTT-based request timeout (section 3)
                receipt = peer.link.request(path, data, response_callback=exchange.on_response,
                                            failed_callback=exchange.on_failed,
                                            progress_callback=exchange.on_progress,
                                            timeout=None, max_response_size=max_response_size)
            except Exception as e:  # noqa: BLE001 - a failed send is a failed request; logged
                # Link.request answers False when it can't send (a closed link too), so an exception is a bug
                log(f"could not send {path} to {node_name(peer.node_id)}: {e!r}", LOG_WARNING)
                return exchange
            if not receipt:
                log(f"could not send {path} to {node_name(peer.node_id)}", LOG_VERBOSE)
                return exchange
            exchange.receipt = receipt
            exchange.sent_at = exchange.last_progress = self.clock()
            if peer.early is not None and peer.early.deliver(receipt):
                log(f"{node_name(peer.node_id)} answered {path} before RNS had registered the request; handed the "
                    f"answer over", LOG_VERBOSE)
            try:
                self._wait(peer, path, exchange, receipt, deadline, watchdog, on_progress)
            finally:
                self._log_outcome(peer, path, exchange)
            if self._went_dead(peer, exchange):
                # Nothing at all came back on the link while the request waited out RNS's timeout: the other end
                # most likely lost it (its rnsd or a transport node on the way restarted), which RNS notices only at
                # its keepalive, about 10 minutes on LoRa; meanwhile every request on it waited out its timeout too.
                # Closed, unless another request is using it, so the next one opens a link that works or fails fast
                if self.close_unless_used(peer.node_id, {threading.get_ident()}):
                    log(f"closed the link to {node_name(peer.node_id)}: nothing came back on it while {path} waited",
                        LOG_VERBOSE)
            return exchange
        finally:
            peer.release(blocks)
            if peer.shared:
                self.wake.set()  # the link may be idle now: the sweep works out when it is due

    def _wait(self, peer: PeerLink, path: str, exchange: Exchange, receipt, deadline: float | None,
              watchdog: Callable[[Exchange, float], str | None] | None, on_progress: Callable[[float], None] | None):
        """request()'s wait for the outcome of a request sent."""
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

    def _went_dead(self, peer: PeerLink, exchange: Exchange) -> bool:
        """True if exchange went unanswered on peer's shared link (RNS gave up on it, nothing of an answer started)
        while the link, still up at this end, received nothing at all from the other end since it was sent, not even a
        keepalive: a link lost at the other end, rather than a request or answer lost on the way."""
        if not peer.shared or exchange.answered or exchange.response_started is not None or exchange.sent_at is None:
            return False
        if getattr(exchange.receipt, "status", None) != RECEIPT_FAILED or not peer.is_active():
            return False
        try:
            silent = peer.link.no_inbound_for()
        except Exception:  # noqa: BLE001 - RNS internals: not known to be dead if they fail
            return False
        return silent > self.clock() - exchange.sent_at

    def _log_outcome(self, peer: PeerLink, path: str, exchange: Exchange):
        """Log, at LOG_DEBUG, how a request sent ended: answered (in what time, how large) or why not."""
        name = node_name(peer.node_id)
        took = self.clock() - exchange.sent_at
        if exchange.answered:
            answer = "refused" if exchange.response is False else "answered"
            log(f"{name} {answer} {path} in {took:.2f}s"
                + (f" ({size(exchange.response_size)})" if exchange.response_size else ""), LOG_DEBUG)
            return
        if exchange.stalled:
            why = exchange.stalled
        elif exchange.cancelled:
            why = "its response stopped arriving (the responder cancelled it, or it was lost in transit)"
        elif not peer.is_active():
            why = "the link " + (close_reason(peer.link) if peer.link is not None else "closed")
        elif getattr(exchange.receipt, "status", None) == RECEIPT_FAILED:
            why = "RNS gave up on it (timed out, or the response failed or was too large)"
        else:
            why = "no outcome"
        log(f"{path} to {name} not answered after {took:.1f}s: {why}", LOG_DEBUG)

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
