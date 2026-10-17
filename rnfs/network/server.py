"""Answering other nodes' requests on rnfs.dht, within every section 3 limit, and the /blocks budget (section 7).

RNS calls each request handler on a thread of its own and sends back whatever it returns. Malformed and
refused requests get False straight away, never silence, except requests over MAX_REQUEST_SIZE, which RNS
drops before any handler runs, and a flood over a link's request rate limit (REQUEST_BURST, REQUEST_RATE),
which after a few refusals gets no answer.

The policy parts (response pools, deadlines and displacement, the /blocks budget, incoming-link eviction,
upload pacing) are plain classes with an injected clock, so they can be tested without RNS. RequestServer
wires them to RNS.

A handler gets no handle on the Resource RNS sends its response in. Responses are matched by request_id
to the link's outgoing Resources as RNS registers them (Link.register_outgoing_resource, wrapped per link;
Link.outgoing_resources is polled as a backup), followed to conclusion through the Resource's callback, and
cancelled with Resource.cancel(). A response keeps its slots until its Resource concludes. These are RNS
1.5.6 internals; if they are missing, a response is cancelled by closing its link.
"""
import math
import random
import threading
import time
from collections import Counter, OrderedDict
from typing import Callable, Protocol

import RNS

from rnfs.config import Config
from rnfs.constants import (BLOCKS_PATH, CHUNK_SIZE, FIND_PATH, MAX_REQUEST_SIZE, MAX_REQUESTS_PER_LINK,
                            MAX_RESPONSE_SIZE, PIN_PATH, PROVIDE_PATH, RESPONSE_OVERHEAD)
from rnfs.content.cid import Cid
from rnfs.log import LOG_DEBUG, LOG_ERROR, LOG_VERBOSE, LOG_WARNING, log, node_name, plural, size
from rnfs.network import guards, protocol, rates
from rnfs.network.identity import node_id_for_identity
from rnfs.network.links import close_reason
from rnfs import mesh

SLOW_POOL = "slow"
FAST_POOL = "fast"
REGISTER_WINDOW = 30.0  # seconds for RNS to build and register a response's Resource with nothing ahead of it
PROOF_GRACE = 5.0  # longest a new /blocks waits for the previous response's proof to be processed
MONITOR_FAST_TICK = 0.02  # monitor interval while a response's Resource hasn't been found yet
MONITOR_TICK = 0.25
# Longest a /blocks response is held back for upload pacing: half of RNS's request timeout with no RTT, so the
# requester doesn't give up meanwhile. Not RTT-based: the requester can inflate the link's RTT
MAX_PACING_WAIT = protocol.REQUEST_TIMEOUT_WITHOUT_RTT / 2
CLOSED_MEMORY = 1024  # closed incoming links remembered, so a late request can't track them again

# Displacement protection (section 3): a response is protected for a grace the node measures itself, then only
# while it makes progress. Never from link.rtt, which the requester sets
GRACE_BASE = 3.0  # seconds: handling at both ends and a round trip on a fast link, before the first part request
# A Resource advertisement fills at most one packet at RNS's default MTU (ResourceAdvertisement.HASHMAP_MAX_LEN is
# sized to Link.MDU), so it takes at most this long per hop at the interface's bitrate
ADVERTISEMENT_BITS = 500 * 8
MAX_GRACE_HOPS = 32  # the requester can raise the hop count it reports; the grace is capped anyway
# What may be queued ahead of an advertisement on the node's own slow radio, per other response on that interface:
# one window of parts (RNS.Resource.WINDOW_MAX_SLOW, 10, of at most one 500-byte packet each). With the radio's
# default flow control off, RNS hands packets to the device at once, so when one actually leaves can't be read
QUEUED_WINDOW_BITS = 10 * 500 * 8
# Progress floor on a directly connected link: 1/1000 of the interface's bitrate (1.25 kB/s on a 10 Mbit/s TCP
# interface). Far below what an honest transfer gets from a direct neighbour, but a requester trickling below it
# loses its protection. Behind more hops the path may be much slower than our interface, so only the
# unmeasured_bitrate floor applies there
FLOOR_FRACTION = 1 / 1000
# A response displaced for a newcomer while making progress at less than this fraction of the rate it needs to
# be done when it is due (Response.due) counts as making none: its requester cools down
FAR_BEHIND = 1 / 4
# A response whose Resource its requester rejects or cancels before this fraction of it was sent made no real
# progress: its requester cools down (section 3). Otherwise one could take the first window of parts of each
# response, cancel it and ask again, having the node read, hash, pack and encrypt a whole response per window
# (on a slow link, a window of RNS.Resource.WINDOW_MAX_SLOW parts is about 7% of a 64 KiB response). An honest
# 0.1.5 fetcher never cancels a Resource (a reader giving up closes the link), so only RNS running out of
# retries on a lossy path does
NO_PROGRESS_FRACTION = 1 / 4
COOLDOWN_MEMORY = 4096  # links and identities on a /blocks cooldown remembered at most
# Requests per incoming link (section 3): a token bucket of REQUEST_BURST, refilled at REQUEST_RATE a second.
# An honest requester waits for its answers, with at most MAX_REQUESTS_PER_LINK in progress, so it sends at most
# that many per round trip. Over real RNS on loopback (the shortest round trip there is), a 10,000-key /provide
# (182 requests, one at a time) ran at ~900 a second and two threads sending /find back to back at 2,100-3,600;
# the busiest link in the end-to-end tests saw 13 in a second. So the limit binds only on a link sending faster
# than any rnfs requester over a real network (or a tight loop like that last one, slowed to the rate).
# Requests over it are answered False while the link has refusals left (OVER_RATE_REFUSALS, one more a
# second), so a requester that gets there backs off as from any refusal, and dropped unanswered after that.
# RNS has already decrypted and decoded each request and started its thread by then; what the limit saves is
# the handler's work and the answer (about 40% of a /find's cost to the node)
REQUEST_BURST = 2000
REQUEST_RATE = 2000.0
OVER_RATE_REFUSALS = 2 * MAX_REQUESTS_PER_LINK
OVER_RATE_LOG_INTERVAL = 60.0  # seconds between log lines about requests over the rate limit
# IncomingLinks.take_token outcomes
REQUEST_OK, REQUEST_REFUSED, REQUEST_DROPPED = "ok", "refused", "dropped"


class Backend(Protocol):
    """What RequestServer needs from the node. Called on RNS request threads; must be thread-safe."""

    def is_dht_server(self) -> bool:
        """True if this node runs the DHT as a server (else /find and /provide are refused)."""

    def find(self, key: bytes) -> tuple[list[bytes], list[bytes]]:
        """(providers of key, servers closest to key) for a /find reply; the server truncates each to K."""

    def provide(self, publisher_id: bytes, keys: list[bytes], withdrawn: list[bytes]) -> bool:
        """Record publisher_id as a provider of keys and withdraw it from withdrawn. True if recorded."""

    def served_block(self, cid: Cid) -> bytes | None:
        """cid's block if it's in the DAG of a CID this node provides, re-verified; else None."""

    def served_block_size(self, cid: Cid) -> int | None:
        """Optional (looked up with getattr): the stored length of the block served_block(cid) would return, or
        None if it isn't served (or isn't known). Lets /blocks stop at the block over its budget unread."""

    def remote_pin(self, identity_hash: bytes, cid: Cid, pin: bool) -> bool:
        """Accept (True) or refuse a /pin from the identity with this 16-byte hash."""

    def is_member(self, identity_hash: bytes) -> bool:
        """In private mode, True if the identity with this 16-byte hash is a member."""

    def feature_requests(self) -> dict:
        """Optional (looked up with getattr): request paths added by features, {path: handler(data,
        remote_identity)}, served within the same per-link limits; a path rnfs serves itself is ignored."""


# Pure policy

def blocks_budget(max_bytes: int, rate_estimate: float | None, upload_cap: float | None,
                  response_target: float) -> int:
    """Total block bytes a /blocks response may hold after its first served block (section 7)."""
    limit = min(max_bytes, MAX_RESPONSE_SIZE - RESPONSE_OVERHEAD)
    if rate_estimate is None:
        # What a requester asks for until the link has a rate estimate (section 7), enforced: otherwise one
        # small request could ask for a ~1 MiB response with a deadline of many hours
        return min(limit, CHUNK_SIZE)
    rate = min(rate_estimate, upload_cap) if upload_cap else rate_estimate
    return min(limit, rates.transfer_bytes(response_target, rate))


def select_blocks(cids: list[Cid], served_block: Callable[[Cid], bytes | None], limit: int,
                  served_size: Callable[[Cid], int | None] | None = None) -> list[bytes | None]:
    """Response entries for cids: each block or None, stopping before the first block over the budget.

    The first served block is always included, even if it alone exceeds limit. served_size(cid), if given, is
    the length of the block served_block(cid) would return (None if unknown): the block over the budget is then
    left out without being read and hashed.
    """
    entries: list[bytes | None] = []
    total = 0
    served_any = False
    for cid in cids:
        if served_any and served_size is not None:
            known = served_size(cid)
            if known is not None and total + known > limit:
                break
        block = served_block(cid)
        if block is None:
            entries.append(None)
            continue
        if served_any and total + len(block) > limit:
            break
        entries.append(block)
        total += len(block)
        served_any = True
    return entries


def link_path(link) -> tuple[object | None, float | None, int | None]:
    """(receiving interface, its bitrate in bits/s, hop count) of an incoming link; None for what is unknown.

    RNS 1.5.6 dependency: Link.attached_interface (the interface the link request arrived on), Interface.bitrate
    (the node's own configuration, or the interface's own guess) and Link.expected_hops (the hop count of the
    requester's RTT packet, which the requester can raise but transport nodes only increase). Read with
    defaults, so a changed RNS falls back to the conservative values the callers use for unknowns. Attached to a
    shared instance, every link arrives on the local interface to it, whose bitrate (1 Gbit/s) says nothing
    about the path, so the bitrate is unknown (via_shared_instance).
    """
    interface = getattr(link, "attached_interface", None)
    bitrate = getattr(interface, "bitrate", None)
    if type(bitrate) not in (int, float) or not math.isfinite(bitrate) or bitrate <= 0 \
            or via_shared_instance(interface):
        bitrate = None
    hops = getattr(link, "expected_hops", None)
    if type(hops) is not int or hops < 0:
        hops = None
    return interface, bitrate, hops


def via_shared_instance(interface) -> bool:
    """Whether interface is a local one between RNS instances on this machine: the client end of the connection
    to a shared instance (or, in the instance, a local client's), not a link to the network.

    RNS 1.5.6 dependency: Transport.interface_to_shared_instance tells the interface to the shared instance by
    its is_connected_to_shared_instance attribute (LocalClientInterface sets it; the spawned server-side ends set
    it False). The class names are a fallback.
    """
    return interface is not None and (hasattr(interface, "is_connected_to_shared_instance")
                                      or type(interface).__name__ in rates.LOCAL_INTERFACE_TYPES)


def fast_path(link) -> bool:
    """Whether RNS raised the link's MTU above rates.SLOW_MTU, which it does only if every interface on the path
    autoconfigures its MTU (from 62.5 kbit/s up, Interface.optimise_mtu) or has a fixed one: no LoRa or other
    KISS radio on it. A requester can lower its link's MTU, never raise it past what the path allows."""
    try:
        mtu = link.get_mtu()
    except Exception:  # noqa: BLE001 - RNS internals: not a fast link if they fail
        return False
    return type(mtu) is int and mtu > rates.SLOW_MTU


def response_grace(bitrate: float | None, hops: int | None, config: Config, fast: bool = False,
                   queued_bits: float = 0) -> float:
    """Seconds a new response is protected from displacement before it must show progress (section 3).

    Enough for the advertisement to reach the requester and its first part request to come back over hops hops,
    plus GRACE_BASE, at most deadline_slack. The first hop is priced at the receiving interface's bitrate; the
    others, which may be much slower, at slow_bitrate on a fast path (fast_path) and unmeasured_bitrate
    otherwise, as is an unknown bitrate. queued_bits is what may be queued ahead of the advertisement on the
    node's own interface. E.g. 1 hop at 1 kbit/s: 3 + 2 × 4 s = 11 s; 3 hops behind a TCP interface with a LoRa
    segment, or anything at an unknown bitrate off a fast path: deadline_slack; any hop count on a fast path
    over TCP: 3-5 s.
    """
    beyond = config.slow_bitrate if fast else config.unmeasured_bitrate
    rate = bitrate if bitrate else beyond
    hops = min(max(1, hops or 1), MAX_GRACE_HOPS)
    one_way = ADVERTISEMENT_BITS / rate + (hops - 1) * ADVERTISEMENT_BITS / min(rate, beyond)
    return min(config.deadline_slack, GRACE_BASE + 2 * one_way + queued_bits / rate)


def progress_floor(bitrate: float | None, hops: int | None, config: Config) -> float:
    """Bytes/s a response must keep up, once its grace is over, to stay protected (section 3).

    unmeasured_bitrate (which every honest path is assumed to beat), or for a directly connected requester
    FLOOR_FRACTION of the interface's bitrate if that is more.
    """
    floor = config.unmeasured_bitrate / 8
    if bitrate and hops == 1:
        floor = max(floor, bitrate * FLOOR_FRACTION / 8)
    return floor


class Response:
    """A /blocks response in progress, holding a slot in one of the pools.

    Threads and locks. A response is touched by its request's handler thread (RNS starts one per request), the
    next request's handler on the same link (_previous_done), the monitor thread (check_responses), RNS's threads
    (the outgoing-Resource registration hook on the packet thread; the Resource's callback on the packet thread,
    on a Resource's watchdog, on the thread RNS starts for a rejection's callback, or on whichever thread
    cancelled it) and link-closed callbacks (_link_closed, on the thread that tore the link down). Python
    assigns attributes atomically; what matters is who may write a field, under which lock, and in what order:
    - link_id, request_id, pool and started: set at construction, never changed.
    - grace, floor, interface and identity: set by ResponsePools.reserve under ResponsePools.lock, before the
      response is visible to any other thread. grace is set again, with no lock, by the handler thread at
      handover, dropping the pacing wait it covered until then: a reader sees one value or the other.
    - size, deadline, due, find_until and untracked: written once each, with no lock, by the handler thread at
      handover, size first and the others after it, before it returns the body (so before RNS can build the
      Resource) and before it wakes the monitor. Readers take size None to mean not yet handed over and skip
      the response (check_responses), so a reader that sees size set but an older deadline (inf) or
      find_until (inf) only waits one more pass. requester is likewise set once by the handler, before _admit.
    - sent and first_progress: written by the monitor thread only (note_progress), with no lock. Other threads
      read them as a snapshot: a stale value only makes a response look slower than it is.
    - grace_start and progress_at: moved forward only (max), with no lock, by the handler at handover, by
      _attach (on the packet thread or the monitor) and, for progress_at, by the monitor. A lost update loses
      at most the gap between two near-simultaneous "now"s.
    - resource: set by _attach, with no lock, from the registration hook (packet thread) or the monitor's
      poll, whichever sees the Resource first; both check it is still None and would attach the same
      Resource, so a race only runs _hook twice, which installs the callback once (and _concluded runs once).
    - idle: set under ResponsePools.lock when a newcomer displaces it (_displaceable), or with no lock by the
      monitor (_check_progress) just before it cancels it, and read by the _cancel that follows. A displaced
      response has left the pool, so only a monitor pass whose snapshot predates that can still reach it; both
      then cancel it, which _finish makes harmless, and either one's idle decides the cooldown.
    - finished and concluded: check-and-set under RequestServer.lock (_finish, _concluded), so each runs once
      whichever thread gets there first. Read without the lock elsewhere as a hint: neither ever goes back to
      False, so a stale False only means that thread's later _finish (or _concluded) call returns early.
    - cost, charged and settled: written under RequestServer.lock (_admit, _charge_progress, _settle, and the
      handler's settled=True for single-packet and untracked responses). Read without it by commitment() and
      _committed() outside _admit, where a stale value only shifts an admission estimate.
    - done: a threading.Event, set by _finish.
    NW2's later steps (in a later release) give a response its own lock; until then, keep to these owners.
    """

    def __init__(self, link_id: bytes, request_id: bytes, pool: str, started: float):
        """A response that has just taken a slot; its size and deadline follow once it's built."""
        self.link_id = link_id
        self.request_id = request_id
        self.pool = pool
        self.started = started
        self.size: int | None = None  # packed bytes, once built
        self.deadline = float("inf")  # absolute time by which it must have finished
        # When it should be done judging from its path: its deadline, or for one with no rate estimate on a fast
        # path (fast_path), the deadline it would have at slow_bitrate, which every interface on that path beats
        self.due = float("inf")
        self.sent = 0.0  # bytes sent so far, from the Resource's progress
        self.resource = None  # the RNS Resource carrying it, once found
        self.find_until = float("inf")
        self.untracked = False  # RNS internals unavailable: can only be cancelled by closing the link
        self.concluded = False  # its Resource's conclusion has been handled
        self.finished = False
        self.done = threading.Event()  # set once finished, to cut short a pacing wait
        # upload_budget (section 3): charged as it is sent, not up front
        self.cost = 0  # packed bytes it will cost the budget if all of it is sent (0: not charged)
        self.charged = 0  # bytes charged so far: a small reservation, then what has been sent
        self.settled = False  # its charge is final: it concluded, or how much it sends can't be followed
        self.requester: bytes | None = None  # identity hash (else link ID) whose budget share it counts against
        self.identity: bytes | None = None  # the requester's identity hash, if it identified
        # Displacement protection (section 3)
        self.grace = float("inf")  # seconds from grace_start it is protected whatever its progress
        self.grace_start = started  # restarted when it is handed to RNS and when its Resource is advertised
        self.floor = 0.0  # bytes/s it must keep up after its grace to stay protected
        self.interface = None  # the RNS interface its request arrived on, if known
        self.progress_at = started  # when sent last grew (or it was handed to RNS)
        self.first_progress = None  # when sent was first seen above 0
        self.idle = False  # displaced or cancelled for making no progress: its requester cools down

    def throughput(self, now: float) -> float:
        """Bytes per second sent so far."""
        return self.sent / max(now - self.started, 1e-6)

    def note_progress(self, sent: float, now: float):
        """Record the bytes sent so far, as read from its Resource."""
        if sent > self.sent:
            self.progress_at = now
            if self.first_progress is None:
                self.first_progress = now
        self.sent = sent

    def progress_rate(self, now: float) -> float:
        """Bytes per second since it started moving: a slow start inside the grace doesn't count against it."""
        if self.sent <= 0:
            return 0.0
        since = self.first_progress if self.first_progress is not None else self.started
        return self.sent / max(now - since, 1.0)

    def in_grace(self, now: float) -> bool:
        """Whether it is still within its grace."""
        return now - self.grace_start <= self.grace

    def moving(self, now: float) -> bool:
        """Whether it is making progress at or above its floor (always, if its progress can't be read)."""
        return self.untracked or (self.sent > 0 and self.progress_rate(now) >= self.floor)

    def protected(self, now: float) -> bool:
        """Whether no newcomer may displace it for making no progress: within its grace, or moving."""
        return self.in_grace(now) or self.moving(now)

    def needed_rate(self, now: float) -> float:
        """Bytes per second it must send from now on to be done when it is due (0 if its size isn't known yet)."""
        if self.size is None or self.untracked:
            return 0.0
        return max(0.0, self.size - self.sent) / max(min(self.due, self.deadline) - now, 1.0)

    def far_behind(self, now: float) -> bool:
        """Whether it is moving at less than FAR_BEHIND of the rate it needs to be done when it is due."""
        return self.progress_rate(now) < FAR_BEHIND * self.needed_rate(now)

    def commitment(self, now: float) -> int:
        """Budget bytes it may still send that haven't been charged (upload_budget, section 3): its uncharged rest
        while it is within its grace; after that, at most what it sends at its progress rate before its deadline,
        and nothing if it is below its floor (or has sent nothing)."""
        rest = max(0, self.cost - self.charged)
        if self.in_grace(now):
            return rest
        if not self.moving(now):
            return 0
        return int(min(rest, self.progress_rate(now) * max(0.0, self.deadline - now)))


class ResponsePools:
    """Slots for /blocks responses: max_responses // 2 for slow links, the rest (or fast_responses, if more) for
    others. Thread-safe.

    Each pool competes only with itself. When a pool is full, a new response may displace (section 3), never
    a response on its own link:
    - the response making least progress among those past their grace and below their progress floor (it is
      marked idle, and its requester cools down);
    - else, if the newcomer's interface holds no slot in the pool, the oldest response with nothing sent from
      an interface holding two or more that is past half its grace, at most once per grace per interface;
    - else, if neither the newcomer's link nor its identity holds a slot in the pool, the pool's response with the
      lowest throughput, if it is older than deadline_slack and past its grace (marked idle if it is far behind
      the rate it needs to be done when it is due).
    Otherwise it is refused.
    """

    def __init__(self, max_responses: int, deadline_slack: float, clock: Callable[[], float] = time.monotonic,
                 fast_responses: int | None = None):
        """Empty pools."""
        slow = max_responses // 2
        self.capacity = {SLOW_POOL: slow, FAST_POOL: max(max_responses - slow, fast_responses or 0)}
        self.deadline_slack = deadline_slack
        self.clock = clock
        self.active: dict[str, list[Response]] = {SLOW_POOL: [], FAST_POOL: []}
        # id(interface) -> when it last lost a response to a newcomer from another interface
        self.fairness_at: dict[int, float] = {}
        self.lock = threading.Lock()

    def reserve(self, pool: str, link_id: bytes, request_id: bytes, grace: float | None = None, floor: float = 0.0,
                interface=None, identity: bytes | None = None) -> tuple[Response | None, Response | None]:
        """Take a slot in pool: (new response, or None if refused; displaced response to cancel, or None).

        grace (default deadline_slack), floor, interface and identity (its requester's identity hash, if it
        identified) are the new response's (section 3).
        """
        with self.lock:
            now = self.clock()
            members = self.active[pool]
            displaced = None
            if len(members) >= self.capacity[pool]:
                displaced = self._displaceable(members, link_id, interface, identity, now)
                if displaced is None:
                    return None, None
                members.remove(displaced)
            response = Response(link_id, request_id, pool, now)
            response.grace = self.deadline_slack if grace is None else grace
            response.floor = floor
            response.interface = interface
            response.identity = identity
            members.append(response)
            return response, displaced

    def _displaceable(self, members: list[Response], link_id: bytes, interface, identity: bytes | None,
                      now: float) -> Response | None:
        """The member a newcomer on link_id (of identity) arriving on interface may displace, or None. Caller
        holds the lock."""
        others = [r for r in members if r.link_id != link_id]
        idle = [r for r in others if not r.protected(now)]
        if idle:
            displaced = min(idle, key=lambda r: (r.progress_rate(now), r.sent, r.started))
            displaced.idle = True
            return displaced
        if interface is not None and all(r.interface is not interface for r in members):
            displaced = self._fairness_pick(others, now)
            if displaced is not None:
                return displaced
        # The slowest moving response, for a newcomer holding nothing in the pool: a requester can't keep its own
        # responses young by displacing them from its own link or identity
        if any(r.link_id == link_id or (identity is not None and r.identity == identity) for r in members):
            return None
        slowest = min(members, key=lambda r: r.throughput(now))
        if now - slowest.started <= self.deadline_slack or slowest.in_grace(now):
            return None
        # Far behind what it needs to finish in time: no better than making no progress, so its requester cools
        # down (else one taking a little of each response could renew its slots this way from a spare link)
        slowest.idle = slowest.far_behind(now)
        return slowest

    def _fairness_pick(self, others: list[Response], now: float) -> Response | None:
        """For a newcomer from an interface holding no slot: the oldest response with nothing sent from an
        interface holding two or more, once it is past half its grace and that interface hasn't lost one this
        way within the response's grace. Caller holds the lock."""
        self.fairness_at = {key: at for key, at in self.fairness_at.items() if now - at <= self.deadline_slack}
        held = Counter(id(r.interface) for r in others if r.interface is not None)
        crowded = [r for r in others if r.interface is not None and held[id(r.interface)] >= 2
                   and r.sent <= 0 and not r.untracked and now - r.grace_start > r.grace / 2
                   and now - self.fairness_at.get(id(r.interface), -math.inf) > r.grace]
        if not crowded:
            return None
        displaced = min(crowded, key=lambda r: r.started)
        self.fairness_at[id(displaced.interface)] = now
        return displaced

    def release(self, response: Response) -> bool:
        """Free response's slot. True if it held one."""
        with self.lock:
            members = self.active[response.pool]
            if response in members:
                members.remove(response)
                return True
            return False

    def responses(self) -> list[Response]:
        """Every response holding a slot."""
        with self.lock:
            return self.active[SLOW_POOL] + self.active[FAST_POOL]

    def count(self, pool: str) -> int:
        """Slots in use in pool."""
        with self.lock:
            return len(self.active[pool])


class _LinkState:
    """Requests in progress on one incoming link, and its request rate limit."""

    __slots__ = ("requests", "blocks", "last_active", "waiting", "tokens", "refusals", "refilled_at")

    def __init__(self, now: float):
        """A link with nothing in progress, active now, with full request and refusal buckets."""
        self.requests = 0
        self.blocks = 0
        self.last_active = now
        self.waiting = False  # a /blocks request is waiting for the previous response's proof
        self.tokens = float(REQUEST_BURST)
        self.refusals = float(OVER_RATE_REFUSALS)
        self.refilled_at = now


class IncomingLinks:
    """Incoming links and their requests in progress: per-link limits and eviction. Thread-safe.

    A /blocks request counts as in progress until its response has been sent.
    """

    def __init__(self, max_links: int, clock: Callable[[], float] = time.monotonic, rng: random.Random | None = None):
        """No links yet; at most max_links at once."""
        self.max_links = max_links
        self.clock = clock
        self.rng = rng or random.Random()
        self.links: dict[bytes, _LinkState] = {}
        self.closed: OrderedDict[bytes, None] = OrderedDict()  # recently closed links, oldest first
        self.lock = threading.Lock()

    def admit(self, link_id: bytes, keep: set[bytes] = frozenset()) -> tuple[bool, bytes | None]:
        """Add a new link: (admitted, ID of a link to close to make room, or None).

        When full, closes the link idle longest with no request in progress, or else a random one with no
        /blocks response in progress; if there's neither, the new link isn't admitted. Links not in keep (in
        private mode, those not identified as a member) are chosen first.
        """
        with self.lock:
            if link_id in self.links:
                return True, None
            victim = None
            if len(self.links) >= self.max_links:
                others = [lid for lid in self.links if lid not in keep]
                victim = self._choose_victim(others) or self._choose_victim(list(self.links))
                if victim is None:
                    return False, None
                del self.links[victim]
            self.links[link_id] = _LinkState(self.clock())
            return True, victim

    def _choose_victim(self, candidates: list[bytes]) -> bytes | None:
        """The link among candidates to close to make room, or None. Caller holds the lock."""
        states = [(link_id, self.links[link_id]) for link_id in candidates]
        idle = [(state.last_active, link_id) for link_id, state in states if state.requests == 0]
        if idle:
            return min(idle)[1]
        no_blocks = [link_id for link_id, state in states if state.blocks == 0]
        return self.rng.choice(no_blocks) if no_blocks else None

    def start_request(self, link_id: bytes, blocks: bool) -> bool:
        """Count a new request on link_id. False if it would break the per-link limits or the link has closed."""
        with self.lock:
            if link_id in self.closed:
                return False
            state = self.links.get(link_id)
            if state is None:
                # A link established before we were tracking; count it from now
                state = self.links[link_id] = _LinkState(self.clock())
            if state.requests >= MAX_REQUESTS_PER_LINK or (blocks and state.blocks >= 1):
                return False
            state.requests += 1
            if blocks:
                state.blocks += 1
            state.last_active = self.clock()
            return True

    def take_token(self, link_id: bytes) -> str:
        """Count a request arriving on link_id against the link's rate limit: REQUEST_OK, or, over the limit
        (REQUEST_BURST at once, REQUEST_RATE a second after that), REQUEST_REFUSED while the link has refusals
        left and REQUEST_DROPPED after that. A closed link isn't tracked again: its requests are let through,
        to be refused by the handlers."""
        with self.lock:
            if link_id in self.closed:
                return REQUEST_OK
            now = self.clock()
            state = self.links.get(link_id)
            if state is None:
                # A link established before we were tracking; count it from now
                state = self.links[link_id] = _LinkState(now)
            elapsed = max(0.0, now - state.refilled_at)
            state.refilled_at = now
            state.tokens = min(float(REQUEST_BURST), state.tokens + elapsed * REQUEST_RATE)
            state.refusals = min(float(OVER_RATE_REFUSALS), state.refusals + elapsed)
            if state.tokens >= 1:
                state.tokens -= 1
                return REQUEST_OK
            if state.refusals >= 1:
                state.refusals -= 1
                return REQUEST_REFUSED
            return REQUEST_DROPPED

    def start_waiting(self, link_id: bytes) -> bool:
        """Let one /blocks request on link_id wait for the previous response to conclude. False if one already is."""
        with self.lock:
            state = self.links.get(link_id)
            if state is None or state.waiting:
                return False
            state.waiting = True
            return True

    def stop_waiting(self, link_id: bytes):
        """The waiting /blocks request on link_id has stopped waiting."""
        with self.lock:
            state = self.links.get(link_id)
            if state is not None:
                state.waiting = False

    def finish_request(self, link_id: bytes, blocks: bool):
        """A request (for /blocks: its response) on link_id has finished."""
        with self.lock:
            state = self.links.get(link_id)
            if state is None:
                return
            state.requests = max(0, state.requests - 1)
            if blocks:
                state.blocks = max(0, state.blocks - 1)
            state.last_active = self.clock()

    def remove(self, link_id: bytes):
        """Forget a closed link (a request arriving for it afterwards is refused)."""
        with self.lock:
            self.links.pop(link_id, None)
            self.closed[link_id] = None
            while len(self.closed) > CLOSED_MEMORY:
                self.closed.popitem(last=False)

    def count(self) -> int:
        """Incoming links tracked."""
        with self.lock:
            return len(self.links)

    def in_progress(self, link_id: bytes) -> tuple[int, int]:
        """(requests, /blocks requests) in progress on link_id."""
        with self.lock:
            state = self.links.get(link_id)
            return (state.requests, state.blocks) if state else (0, 0)


class Cooldowns:
    """Links and identities refused /blocks for a while, after a response of theirs was displaced or cancelled
    for making no progress (section 3). Thread-safe, and cheap to ask."""

    def __init__(self, duration: float, clock: Callable[[], float] = time.monotonic):
        """No one cooling down; each cooldown lasts duration seconds (0: none)."""
        self.duration = duration
        self.clock = clock
        self.until: OrderedDict[bytes, float] = OrderedDict()  # key -> end, soonest first (one duration for all)
        self.lock = threading.Lock()

    def add(self, *keys: bytes | None):
        """Start a cooldown for each key given (None is skipped)."""
        if self.duration <= 0:
            return
        with self.lock:
            now = self.clock()
            for key in keys:
                if key is not None:
                    self.until.pop(key, None)
                    self.until[key] = now + self.duration
            self._prune(now)

    def active(self, *keys: bytes | None) -> bool:
        """Whether any key given is cooling down."""
        with self.lock:
            now = self.clock()
            self._prune(now)
            return any(key is not None and key in self.until for key in keys)

    def forget(self, key: bytes):
        """Drop key (a closed link's ID: it never comes back)."""
        with self.lock:
            self.until.pop(key, None)

    def _prune(self, now: float):
        """Drop ended cooldowns, and the oldest beyond COOLDOWN_MEMORY. Caller holds the lock."""
        while self.until and (len(self.until) > COOLDOWN_MEMORY or next(iter(self.until.values())) <= now):
            self.until.popitem(last=False)


class UploadPacer:
    """max_upload_rate between responses: a link's next response starts no sooner than bytes × 8 / cap
    seconds after the previous one started. Thread-safe."""

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        """No responses yet."""
        self.clock = clock
        self.next_start: dict[bytes, float] = {}
        self.lock = threading.Lock()

    def started(self, link_id: bytes, size: int, cap: float | None):
        """A response of size bytes started on link_id now, under an upload cap of cap bits/s (None: none)."""
        if not cap:
            return
        with self.lock:
            self.next_start[link_id] = self.clock() + size * 8 / cap

    def wait_time(self, link_id: bytes) -> float:
        """Seconds until the next response on link_id may start."""
        with self.lock:
            next_start = self.next_start.get(link_id)
        return max(0.0, next_start - self.clock()) if next_start is not None else 0.0

    def forget(self, link_id: bytes):
        """Forget a closed link."""
        with self.lock:
            self.next_start.pop(link_id, None)


# RNS glue

class RequestServer:
    """Registers rnfs.dht's request handlers and answers them within every section 3 limit."""

    def __init__(self, destination: RNS.Destination, config: Config, backend: Backend,
                 clock: Callable[[], float] = time.monotonic, start_monitor: bool = True,
                 traffic: mesh.Traffic | None = None):
        """Start answering requests to destination from backend."""
        self.destination = destination
        self.config = config
        self.backend = backend
        self.clock = clock
        self.pools = ResponsePools(config.max_blocks_responses, config.deadline_slack, clock,
                                   config.fast_blocks_responses)
        self.incoming = IncomingLinks(config.max_incoming_links, clock)
        self.pacer = UploadPacer(clock)
        self.cooldowns = Cooldowns(config.blocks_cooldown, clock)
        # mesh: airtime controls on /blocks and the traffic counters (shared with the client by the node)
        self.airtime = mesh.AirtimePolicy(config, clock)
        self.traffic = traffic if traffic is not None else mesh.Traffic()
        warning = mesh.shared_instance_warning(config, RNS.Reticulum.get_instance())
        if warning:
            log(warning, LOG_WARNING)
        self.rates = rates.RateEstimates(config.min_rate_sample)
        self.links: dict[bytes, RNS.Link] = {}
        self.watched: set[bytes] = set()  # links whose outgoing Resource registrations we see
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.wake = threading.Event()
        self.pin_identities = {h.strip().strip("<>").lower() for h in config.remote_pin_identities}
        self.over_rate = 0  # requests over their link's rate limit, in all (refused or dropped)
        self.over_rate_logged_at: float | None = None
        self.over_rate_logged = 0

        # Reject request Resources, compressed ones and advertisements whose transfer outgrows their size: on
        # every link in the daemon, which also turns RNS's compression off process-wide, or on rnfs.dht links
        # in an embedded node (guards.install, in the scope the node set)
        guards.install()
        # Links that never finish their handshake are kept in a bucket of their own, below max_incoming_links
        guards.limit_half_open(destination, config.max_incoming_links)
        destination.set_max_request_size(MAX_REQUEST_SIZE)
        destination.set_link_established_callback(self._link_established)
        handlers = {
            FIND_PATH: self._handle_find,
            PROVIDE_PATH: self._handle_provide,
            BLOCKS_PATH: self._handle_blocks,
            PIN_PATH: self._handle_pin,
        }
        # Paths added by features (rnfs.features: Feature.requests), within the same per-link limit
        for path, handler in getattr(backend, "feature_requests", dict)().items():
            if path not in handlers:
                handlers[path] = self._feature_handler(handler)
        for path, handler in handlers.items():
            # ALLOW_ALL: private mode is checked inside the handler, since ALLOW_LIST drops refusals silently
            destination.register_request_handler(path, response_generator=self._guarded(handler),
                                                 allow=RNS.Destination.ALLOW_ALL, auto_compress=False)

        self.monitor = threading.Thread(target=self._monitor_loop, name="rnfs-response-monitor", daemon=True)
        if start_monitor:
            self.monitor.start()

    def stop(self):
        """Stop the response monitor."""
        self.stop_event.set()
        self.wake.set()

    # Incoming links

    def _link_established(self, link):
        """Track a new incoming link, closing one to make room if max_incoming_links is reached.

        RNS calls this on its packet-handling thread, so it only does quick work.
        """
        self._track(link)
        log(f"incoming link {RNS.prettyhexrep(link.link_id)} established"
            + (f" on {link.attached_interface}" if getattr(link, "attached_interface", None) is not None else ""),
            LOG_DEBUG)
        # Private mode: outsiders' links are closed first, so they can't push members' idle links out
        keep = self._member_links() if self.config.private else frozenset()
        admitted, victim = self.incoming.admit(link.link_id, keep)
        if victim is not None:
            with self.lock:
                victim_link = self.links.get(victim)
            if victim_link is not None:
                log("too many incoming links: closing one to make room", LOG_VERBOSE)
                # Outside the lock: teardown runs the closed callback synchronously
                victim_link.teardown()
        if not admitted:
            log("too many incoming links, all busy: refusing a new one", LOG_VERBOSE)
            link.teardown()

    def _member_links(self) -> set[bytes]:
        """IDs of the incoming links identified as a member (private mode)."""
        with self.lock:
            links = list(self.links.values())
        members = set()
        for link in links:
            try:
                identity = link.get_remote_identity()
            except Exception:  # noqa: BLE001 - RNS internals: no identity if they fail
                identity = None
            if self._is_member(identity):
                members.add(link.link_id)
        return members

    def _track(self, link):
        """Remember an incoming link and get told when it closes."""
        with self.lock:
            self.links[link.link_id] = link
        link.set_link_closed_callback(self._link_closed)
        self._watch_resources(link)

    def _link_closed(self, link):
        """Forget a closed link and free everything its responses held."""
        link_id = link.link_id
        opened = getattr(link, "activated_at", None)
        log(f"incoming link {RNS.prettyhexrep(link_id)} {close_reason(link)}"
            + (f" after {time.time() - opened:.0f}s" if isinstance(opened, (int, float)) else ""), LOG_DEBUG)
        with self.lock:
            self.links.pop(link_id, None)
            self.watched.discard(link_id)
        for response in self.pools.responses():
            if response.link_id == link_id:
                self._finish(response)
        self.incoming.remove(link_id)
        self.cooldowns.forget(link_id)
        self.pacer.forget(link_id)
        self.rates.forget(link_id)

    def _find_link(self, link_id: bytes):
        """The incoming link with this ID (RNS gives handlers only the ID), or None."""
        with self.lock:
            link = self.links.get(link_id)
        if link is not None:
            return link
        for candidate in list(getattr(self.destination, "links", [])):
            # Not a closed one: its closed callback has already run, so it would stay tracked for good
            if getattr(candidate, "link_id", None) == link_id and candidate.status != RNS.Link.CLOSED:
                self._track(candidate)
                return candidate
        return None

    # Handlers

    def _guarded(self, handler):
        """Wrap handler with the link's rate limit (over it: False, or None, which RNS doesn't answer), the
        private-mode membership check (False for a non-member), and an answer of False for an unexpected error
        rather than leaving the requester waiting."""
        def response_generator(path, data, request_id, link_id, remote_identity, requested_at):
            """RNS's six-argument request handler signature."""
            # First, before any parsing: the link's rate limit. None: RNS sends no answer
            allowed = self.incoming.take_token(link_id)
            if allowed != REQUEST_OK:
                self._over_rate()
                return False if allowed == REQUEST_REFUSED else None
            try:
                if self.config.private and not self._is_member(remote_identity):
                    log(f"refused {path} from a non-member", LOG_VERBOSE)
                    return False
                return handler(path, data, request_id, link_id, remote_identity)
            except Exception as e:  # noqa: BLE001 - runs on RNS's request thread: refuse and log
                log(f"error answering {path}: {e}", LOG_ERROR)
                return False
        return response_generator

    def _over_rate(self):
        """Count a request over its link's rate limit, logging at most once a minute."""
        with self.lock:
            self.over_rate += 1
            now = self.clock()
            if self.over_rate_logged_at is not None and now - self.over_rate_logged_at < OVER_RATE_LOG_INTERVAL:
                return
            dropped, self.over_rate_logged, self.over_rate_logged_at = (self.over_rate - self.over_rate_logged,
                                                                        self.over_rate, now)
        log(f"refused or dropped {plural(dropped, 'request')} over the per-link rate limit ({REQUEST_BURST} at "
            f"once, {REQUEST_RATE:g} a second)", LOG_WARNING)

    def _is_member(self, remote_identity) -> bool:
        """True if the requester identified as a member (private mode)."""
        return remote_identity is not None and bool(self.backend.is_member(remote_identity.hash))

    def _handle_find(self, path, data, request_id, link_id, remote_identity):
        """Answer /find with providers of the key and the servers closest to it."""
        key = protocol.parse_find_request(data)
        if key is None or not self.backend.is_dht_server():
            return False
        if not self.incoming.start_request(link_id, blocks=False):
            return False
        try:
            providers, nodes = self.backend.find(key)
            log(f"/find {key.hex()}: {plural(len(providers), 'provider')}, {plural(len(nodes), 'node')}",
                LOG_VERBOSE)
            return protocol.encode_find_response(list(providers), list(nodes))
        finally:
            self.incoming.finish_request(link_id, blocks=False)

    def _handle_provide(self, path, data, request_id, link_id, remote_identity):
        """Record the identified requester as a provider of the keys, and withdraw the withdrawn ones."""
        # Only the identified requester is ever recorded, so a node can only advertise itself
        if remote_identity is None or not self.backend.is_dht_server():
            return False
        parsed = protocol.parse_provide_request(data)
        if parsed is None or not self.incoming.start_request(link_id, blocks=False):
            return False
        try:
            keys, withdrawn = parsed
            publisher = node_id_for_identity(remote_identity)
            recorded = self.backend.provide(publisher, keys, withdrawn) is True
            log(f"/provide from {node_name(publisher)}: {plural(len(keys), 'key')}, {len(withdrawn)} withdrawn, "
                f"{'recorded' if recorded else 'refused'}", LOG_VERBOSE)
            return recorded
        finally:
            self.incoming.finish_request(link_id, blocks=False)

    def _feature_handler(self, handler):
        """A handler for a feature's path: counted against MAX_REQUESTS_PER_LINK, and never answering None."""
        def handle(path, data, request_id, link_id, remote_identity):
            """Run the feature's handler(data, remote_identity) inside the per-link request limit."""
            if not self.incoming.start_request(link_id, blocks=False):
                return False
            try:
                response = handler(data, remote_identity)
                return False if response is None else response
            finally:
                self.incoming.finish_request(link_id, blocks=False)
        return handle

    def _handle_pin(self, path, data, request_id, link_id, remote_identity):
        """Pass a /pin from an identity on the remote-pin list to the backend."""
        if remote_identity is None or remote_identity.hash.hex() not in self.pin_identities:
            return False
        parsed = protocol.parse_pin_request(data)
        if parsed is None or not self.incoming.start_request(link_id, blocks=False):
            return False
        try:
            cid, pin = parsed
            return self.backend.remote_pin(remote_identity.hash, cid, pin) is True
        finally:
            self.incoming.finish_request(link_id, blocks=False)

    def _handle_blocks(self, path, data, request_id, link_id, remote_identity):
        """Answer /blocks within the per-link limit, the response pools, upload pacing and the budget."""
        parsed = protocol.parse_blocks_request(data)
        link = self._find_link(link_id)
        if parsed is None or link is None:
            return False
        identity = bytes(remote_identity.hash) if remote_identity is not None else None
        if self.cooldowns.active(link_id, identity):
            log("refused /blocks: that requester's last response made no progress", LOG_VERBOSE)
            return False
        # mesh airtime controls: refused (False) at once, so the requester tries another provider
        refusal = self.airtime.refusal(mesh.interface_names(getattr(link, "attached_interface", None)))
        if refusal is not None:
            self.traffic.refused(refusal)
            log(f"refused /blocks: {refusal.replace('_', ' ')}", LOG_VERBOSE)
            return False
        max_bytes, cids = parsed
        if not self.incoming.start_request(link_id, blocks=True) and not self._after_previous(link, link_id):
            log("refused /blocks: a response is still in progress on that link", LOG_VERBOSE)
            return False

        response = None
        handed_over = False
        try:
            # Refusals come first and at once: pacing, then the pools; only an accepted request waits
            wait = self.pacer.wait_time(link_id)
            if wait > MAX_PACING_WAIT:
                log(f"refused /blocks: the link's upload cap allows the next response in {wait:.0f}s", LOG_VERBOSE)
                return False
            rate = self.rates.get(link_id)
            slow = rates.link_is_slow(link, rate, self.config)
            interface, bitrate, hops = link_path(link)
            fast = fast_path(link)
            grace = response_grace(bitrate, hops, self.config, fast, self._queued_bits(interface, bitrate))
            # Until it is handed to RNS, the grace also covers the pacing wait
            response, displaced = self.pools.reserve(SLOW_POOL if slow else FAST_POOL, link_id, request_id,
                                                     grace=min(self.config.deadline_slack, grace + wait),
                                                     floor=progress_floor(bitrate, hops, self.config),
                                                     interface=interface, identity=identity)
            if displaced is not None:
                self._cancel(displaced, "made no progress; displaced by a new request" if displaced.idle
                             else "displaced by a new request")
            if response is None:
                log(f"refused /blocks: every {'slow' if slow else 'fast'}-link response slot is busy", LOG_VERBOSE)
                return False
            if wait > 0:
                self._pace(response, wait)
                if response.finished:
                    return False  # displaced, or its link closed, while it waited

            # The upload cap applies once the link has a rate estimate (section 3)
            cap = self.config.effective_max_upload_rate(slow, rate) if rate is not None else None
            limit = self.airtime.limit(blocks_budget(max_bytes, rate, cap, self.config.response_target),
                                       self._committed(response))
            response.requester = identity if identity is not None else link_id
            share = self._budget_share_left(response)
            if share is not None:
                if share <= 0:
                    self.traffic.refused(mesh.REFUSED_BUDGET)
                    log("refused /blocks: that requester's responses in flight hold half of upload_budget", LOG_VERBOSE)
                    return False
                limit = min(limit, max(0, share - RESPONSE_OVERHEAD))
            entries = select_blocks(cids, self.backend.served_block, limit,
                                    getattr(self.backend, "served_block_size", None))
            body = protocol.encode_blocks_response(entries)
            packed = protocol.packed_response_size(request_id, body)
            mdu = link.get_mdu() or protocol.DEFAULT_MDU
            single = packed <= mdu
            untracked = link_id not in self.watched
            # The upload budget is charged for responses carrying blocks (answers of only None are tiny), as
            # they are sent: up front only a reservation of about one packet (its advertisement), or all of it
            # if it goes as one packet or its progress can't be followed
            if any(entry is not None for entry in entries) and not self._admit(
                    response, packed, packed if single or untracked else min(packed, mdu)):
                self.traffic.refused(mesh.REFUSED_BUDGET)
                log(f"refused /blocks: {size(packed)} is over what is left of upload_budget", LOG_VERBOSE)
                return False
            if response.finished:
                return False  # displaced while it was being built (_admit charged nothing, or _finish settled)
            with self.lock:
                if link_id in self.links:  # a closed link's entry would never be forgotten
                    self.pacer.started(link_id, packed, cap)
            served = sum(1 for entry in entries if entry is not None)
            log(f"/blocks: sending {plural(served, 'block')} of {len(cids)} requested ({size(packed)})", LOG_VERBOSE)
            self.traffic.sent(node_id_for_identity(remote_identity) if remote_identity is not None else None,
                              mesh.interface_label(getattr(link, "attached_interface", None)), packed)
            if single:
                with self.lock:
                    response.settled = True  # sent as a single packet once we return: the charge stands
                return body  # nothing to track
            response.size = packed
            response.deadline = response.started + rates.response_deadline(packed, rate, self.config)
            response.due = response.started + rates.response_deadline(
                packed, rate or (self.config.slow_bitrate if fast else None), self.config)
            response.grace = grace
            self._restart_grace(response)
            response.find_until = self.clock() + REGISTER_WINDOW
            response.untracked = untracked
            if untracked:
                with self.lock:
                    response.settled = True  # charged in full: how much is sent can't be followed
            # From here the slots are freed when the Resource concludes, is cancelled, or the link closes
            handed_over = True
            self.wake.set()
            return body
        finally:
            if not handed_over:
                if response is not None:
                    self._finish(response)  # frees the pool slot and the link's /blocks slot exactly once
                else:
                    self.incoming.finish_request(link_id, blocks=True)

    def _queued_bits(self, interface, bitrate: float | None) -> float:
        """What may be queued ahead of a new response's advertisement on the node's own interface, if it is a slow
        one (below slow_bitrate): a window of parts for each response in flight on it (QUEUED_WINDOW_BITS)."""
        if interface is None or not bitrate or bitrate >= self.config.slow_bitrate:
            return 0
        return QUEUED_WINDOW_BITS * sum(1 for r in self.pools.responses() if r.interface is interface)

    @staticmethod
    def _pace(response: Response, wait: float):
        """Hold an accepted response back wait seconds (at most MAX_PACING_WAIT) for upload pacing, or until
        it finishes (displaced, or its link closed), so it doesn't keep its slot and thread for nothing."""
        response.done.wait(wait)

    def _after_previous(self, link, link_id: bytes) -> bool:
        """Take the link's /blocks slot once its previous response has concluded. False if refused.

        The per-link limit is checked first, so only the request that found the slot taken gets here, and
        only one request per link at a time may wait for a proof: any other is refused at once, so a
        requester withholding proofs can't hold many handler threads in the grace wait.
        """
        if not self.incoming.start_waiting(link_id):
            return False
        try:
            return self._previous_done(link, link_id) and self.incoming.start_request(link_id, blocks=True)
        finally:
            self.incoming.stop_waiting(link_id)

    def _previous_done(self, link, link_id: bytes) -> bool:
        """Whether the link's previous /blocks response, if any, has finished, so a new one may start.

        A response's slots are kept until its Resource has concluded (or been cancelled), so a requester
        withholding proofs can't queue responses. Since RNS handles a proof and the next request on separate
        threads, a Resource whose parts have all been sent is given a short grace to conclude.
        """
        previous = [r for r in self.pools.responses() if r.link_id == link_id and not r.finished]
        if not previous:
            return True
        deadline = time.monotonic() + min(PROOF_GRACE, 2 * (getattr(link, "rtt", None) or 0) + 1)
        for response in previous:
            if response.untracked:
                # Without RNS internals, the requester asking again is the only sign the response arrived
                self.rates.record(link_id, response.size, self._expected_rate(link))
                self._finish(response)
                continue
            resource = response.resource
            if resource is None or getattr(resource, "status", 0) < RNS.Resource.AWAITING_PROOF:
                return False  # not yet sent, or still transferring
            while not response.finished and time.monotonic() < deadline:
                if getattr(resource, "status", 0) >= RNS.Resource.COMPLETE:
                    self._concluded(response, link, resource)
                    break
                # Its Resource's callback finishes it as the proof is processed (_hook); the status is a backup
                response.done.wait(0.01)
            if not response.finished:
                return False
        return True

    # Tracking responses

    def _watch_resources(self, link):
        """Get told when RNS registers an outgoing Resource on link, by wrapping the link's
        register_outgoing_resource (an RNS internal). Without it, responses are tracked as untracked."""
        original = getattr(link, "register_outgoing_resource", None)
        if not callable(original):
            return
        if getattr(original, "rnfs_watch", False):
            self.watched.add(link.link_id)
            return

        def register_outgoing_resource(resource, *args, **kwargs):
            """RNS registering a Resource it has just advertised: register it, then match it to a response."""
            result = original(resource, *args, **kwargs)
            try:
                self._registered(link, resource)
            except Exception as e:  # noqa: BLE001 - runs inside RNS's Resource code; logged
                log(f"error tracking a /blocks response: {e}", LOG_WARNING)
            return result

        register_outgoing_resource.rnfs_watch = True
        try:
            link.register_outgoing_resource = register_outgoing_resource
        except Exception:  # noqa: BLE001 - a link that can't be watched isn't
            return
        self.watched.add(link.link_id)

    def _registered(self, link, resource):
        """An outgoing Resource appeared on link: if it carries one of our responses, follow it."""
        request_id = getattr(resource, "request_id", None)
        for response in self.pools.responses():
            if response.link_id == link.link_id and response.request_id == request_id and response.resource is None:
                self._attach(response, link, resource)
                return

    def _attach(self, response: Response, link, resource):
        """Follow resource, just advertised, as the response's: its grace starts again from here."""
        response.resource = resource
        self._restart_grace(response)
        self._hook(response, link, resource)

    def _restart_grace(self, response: Response):
        """Start the response's grace (and its progress clock) now: it was just handed to RNS, or advertised."""
        now = self.clock()
        response.grace_start = max(response.grace_start, now)
        response.progress_at = max(response.progress_at, now)

    def _finish(self, response: Response):
        """Free a response's pool slot and its link's /blocks slot, and settle its budget charge, once."""
        with self.lock:
            if response.finished:
                return
            response.finished = True
        response.done.set()
        self._settle(response)
        self.pools.release(response)
        self.incoming.finish_request(response.link_id, blocks=True)

    def _admit(self, response: Response, cost: int, reservation: int) -> bool:
        """Charge upload_budget reservation of a response that will cost it cost bytes, if the budget allows
        the response. False if it must be refused.

        Admissions are taken one at a time, each counting what the others are still committed to send, so
        responses admitted together can't overrun what the bucket holds.
        """
        with self.lock:
            if response.finished:
                return True  # displaced meanwhile: nothing to charge, and the caller sees it finished
            if not self.airtime.reserve(cost, reservation, self._committed(response)):
                return False
            response.cost, response.charged = cost, reservation
            return True

    def _committed(self, response: Response | None = None) -> int:
        """Budget bytes the responses in flight (other than response) are committed to send but haven't been
        charged (Response.commitment): each one's unsent rest while it is within its grace, and after that what
        it can send at its progress rate before its deadline. One past its grace with nothing sent (its requester
        isn't fetching it) or crawling below its floor commits nothing, so a few such responses can't hold
        admission while spending nothing."""
        if self.airtime.bucket is None:
            return 0
        now = self.clock()
        return sum(other.commitment(now) for other in self.pools.responses()
                   if other is not response and not other.finished and not other.settled and other.cost)

    def _charge_progress(self, response: Response):
        """Charge upload_budget for what response has sent since it was last charged."""
        with self.lock:
            if response.settled or not response.cost:
                return
            sent = min(response.cost, int(response.sent))
            extra = sent - response.charged
            if extra <= 0:
                return
            response.charged = sent
        self.airtime.charge(extra)

    def _settle(self, response: Response):
        """Make a concluded response's budget charge final, once: what it sent (its whole cost if it completed,
        or if how much it sent can't be read), with any reservation it didn't use given back.

        So a requester that rejects (or never fetches) its responses' Resources costs the node-wide budget
        nothing beyond them being in flight, and a cancelled one costs what was sent.
        """
        with self.lock:
            if response.settled or not response.cost:
                response.settled = True
                return
            response.settled = True
            charged = response.charged
        resource = response.resource
        final = response.cost
        if getattr(resource, "status", None) != RNS.Resource.COMPLETE:
            try:
                sent = resource.get_progress() * (response.size or 0) if resource is not None else response.sent
                final = min(response.cost, int(max(0.0, sent)))
            except Exception:  # noqa: BLE001 - progress unknown: charged in full
                pass  # progress unknown: it is charged in full
        with self.lock:
            response.charged = final
        if final > charged:
            self.airtime.charge(final - charged)
        elif final < charged:
            self.airtime.refund(charged - final)

    def _budget_share_left(self, response: Response) -> int | None:
        """Budget bytes response's requester may still have in flight (None without upload_budget).

        One requester's responses in flight may cost at most half the bucket's capacity in all, so one requester
        can't size its responses to take the whole budget at once and leave others none. Unidentified links
        each count as their own requester and fresh identities are free, so this is fairness among requesters,
        not a bound on a determined one: what bounds that is that the budget is only charged as bytes are sent.
        """
        bucket = self.airtime.bucket
        if bucket is None:
            return None
        responses = self.pools.responses()
        with self.lock:
            in_flight = sum(other.cost for other in responses
                            if other is not response and other.requester == response.requester and not other.finished)
        return int(bucket.capacity // 2) - in_flight

    def _concluded(self, response: Response, link, resource):
        """The response's Resource concluded: record the rate if it completed, start a cooldown if its requester
        rejected or cancelled it before it made real progress (_made_progress), and free the slots."""
        with self.lock:
            # The Resource's callback, the monitor and the next request may all notice; handle it once
            first, response.concluded = not response.concluded, True
            cancelled = response.finished  # finished before its Resource concluded: _cancel did it, and logged why
        status = getattr(resource, "status", None)
        if first and status == RNS.Resource.COMPLETE:
            self.rates.record(response.link_id, response.size, self._expected_rate(link))
        elif first and status == RNS.Resource.REJECTED and not self._made_progress(response, resource):
            # Turned down at its advertisement or cancelled by its requester (on RNS 1.5.6 both arrive as
            # REJECTED) before a quarter of it was sent: no honest 0.1.5 fetcher does that, it closes the link.
            # So no real progress, and its requester cools down as for a response cancelled for making none,
            # rather than having the node build another at once (section 3)
            log(f"a /blocks response ({size(response.size or 0)}) was rejected before a quarter of it was sent: "
                "its requester cools down", LOG_VERBOSE)
            self.cooldowns.add(response.link_id, response.identity)
        elif first and not cancelled and status in (RNS.Resource.FAILED, RNS.Resource.CORRUPT):
            # Not cancelled here: RNS gave up on the transfer, or the requester closed the link, so the requester
            # sees it stop arriving. No cooldown, however little was sent: an honest reader that gives up early
            # closes its links, and in private mode a cooldown would hold back its own identity
            log(f"a /blocks response ({size(response.size or 0)}) failed after {size(int(response.sent))} sent: "
                f"{'its link closed' if getattr(link, 'status', None) == RNS.Link.CLOSED else 'lost in transit'}",
                LOG_DEBUG)
        self._finish(response)  # charges what was sent: all of a completed Resource

    @staticmethod
    def _made_progress(response: Response, resource) -> bool:
        """Whether at least NO_PROGRESS_FRACTION of response was sent: the more of what the monitor last saw and
        what its Resource reads now. True if its size or the Resource's progress can't be read."""
        if response.size is None:
            return True
        try:
            sent = max(response.sent, resource.get_progress() * response.size)
        except Exception:  # noqa: BLE001 - progress unknown counts as progress
            return True
        return sent >= NO_PROGRESS_FRACTION * response.size

    def _cancel(self, response: Response, reason: str):
        """Stop sending a response: cancel its Resource, or close its link if we can't reach the Resource."""
        with self.lock:
            link = self.links.get(response.link_id)
        resource = response.resource
        if resource is None and link is not None and not response.untracked:
            resource = self._find_resource(link, response.request_id)
        log(f"cancelling a /blocks response: {reason}", LOG_VERBOSE)
        if response.idle:
            # Its requester made no progress with the slot: it can't take one again for a while (section 3)
            self.cooldowns.add(response.link_id, response.identity)
        self._finish(response)
        if resource is not None and callable(getattr(resource, "cancel", None)):
            resource.cancel()
        elif link is not None:
            link.teardown()

    @staticmethod
    def _find_resource(link, request_id: bytes):
        """The outgoing Resource carrying the response to request_id, or None."""
        for resource in list(getattr(link, "outgoing_resources", None) or []):
            if getattr(resource, "request_id", None) == request_id:
                return resource
        return None

    @staticmethod
    def _expected_rate(link) -> float | None:
        """Link.get_expected_rate(), or None if unavailable."""
        try:
            return link.get_expected_rate()
        except Exception:  # noqa: BLE001 - RNS internals: no rate if they fail
            return None

    def _hook(self, response: Response, link, resource):
        """Get told when the Resource concludes, so the rate is read before another transfer overwrites it."""
        if getattr(resource, "callback", None) is None:
            def concluded(resource):
                """Resource finished (RNS calls this right after updating the link's expected rate)."""
                self._concluded(response, link, resource)

            try:
                resource.callback = concluded
            except Exception:  # noqa: BLE001 - RNS internals: the deadline still applies
                pass
        if getattr(resource, "status", 0) >= RNS.Resource.COMPLETE:
            # It concluded before the hook was in place
            self._concluded(response, link, resource)

    def check_responses(self) -> bool:
        """One monitor pass: note progress, finish concluded responses, cancel overdue or lost ones.

        Returns True if a response's Resource is still being looked for (so the next pass should be soon).
        """
        searching = False
        now = self.clock()
        for response in self.pools.responses():
            if response.finished or response.size is None:
                continue
            with self.lock:
                link = self.links.get(response.link_id)
            if link is None or link.status == RNS.Link.CLOSED:
                self._finish(response)
                continue

            if response.resource is None and not response.untracked:
                # Normally the registration hook finds it; polling is a backup
                resource = self._find_resource(link, response.request_id)
                if resource is not None:
                    self._attach(response, link, resource)
                elif now > response.find_until and not getattr(link, "outgoing_resources", None):
                    # Never registered although nothing on the link is ahead of it: RNS failed to send it.
                    # Its slots are only freed by closing the link, which certainly ends it.
                    self._cancel(response, "its Resource never started")
                    continue
                else:
                    searching = True

            resource = response.resource
            if resource is not None and not response.finished:
                status = getattr(resource, "status", None)
                if status is not None and status >= RNS.Resource.COMPLETE:
                    self._concluded(response, link, resource)
                    continue
                try:
                    response.note_progress(resource.get_progress() * response.size, now)
                except Exception:  # noqa: BLE001 - progress unknown: only the deadline applies
                    resource = None  # progress unknown: only the deadline applies
                else:
                    self._charge_progress(response)
                if resource is not None and self._check_progress(response, now):
                    continue

            if not response.finished and now > response.deadline:
                self._cancel(response, f"missed its deadline ({size(response.size)})")
        return searching

    def _check_progress(self, response: Response, now: float) -> bool:
        """Cancel (and refund) a response whose Resource makes no progress (section 3). True if cancelled.

        One with nothing sent is cancelled deadline_slack after its grace ended (by then a newcomer could have
        displaced it anyway; this also gives its budget charge back when nobody else asks), and any response
        with no progress for stall_timeout. Either way, its requester cools down unless it was moving.
        """
        idle_for = now - response.progress_at
        if response.sent <= 0 and now - response.grace_start > response.grace + self.config.deadline_slack:
            reason = f"nothing sent {idle_for:.0f}s after it was advertised"
        elif idle_for > self.config.stall_timeout:
            reason = f"no progress for {idle_for:.0f}s"
        else:
            return False
        response.idle = not response.moving(now)
        self._cancel(response, reason)
        return True

    def _monitor_loop(self):
        """Run check_responses until stopped; with no response handed to RNS, wait for one (or stop)."""
        while not self.stop_event.is_set():
            try:
                searching = self.check_responses()
                # check_responses only looks at responses handed to RNS, and each handover sets wake
                idle = not any(r.size is not None and not r.finished for r in self.pools.responses())
            except Exception as e:  # noqa: BLE001 - the monitor thread keeps running; logged
                log(f"error tracking /blocks responses: {e}", LOG_WARNING)
                searching = idle = False
            self.wake.wait(None if idle else MONITOR_FAST_TICK if searching else MONITOR_TICK)
            self.wake.clear()
