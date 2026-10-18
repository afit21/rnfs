"""Guards on RNS: on incoming Resource advertisements and responses (sections 3 and 10: oversized-response and
decoding bombs, compression), and one per destination on half-open incoming links (section 3: link exhaustion).

Their scope is set by install(scope). The daemon owns its process (PROCESS): the advertisement checks below
cover every link, and RNS's process-wide decompression limit (Resource.AUTO_COMPRESS_MAX_SIZE) is 0, which also
stops RNS compressing anything the process sends. A node embedded in another program (DHT: rnfs.Node) guards
only rnfs.dht links and leaves that limit as the program has it, so the program's own requests, Resources and
compressed transfers behave as with RNS alone; on rnfs.dht links every check still applies, and a compressed
Resource is refused there just the same (below). rnfs never compresses what it sends on rnfs.dht links in
either scope: responses are registered with auto_compress=False, which RNS passes to a response Resource, and
LinkPool sends no request that wouldn't fit one packet, so no request Resource either.

RNS checks Destination.set_max_request_size and a request's max_response_size only against an
advertisement's data size (d), but receives, buffers and allocates by its transfer size (t). RNS 1.5.6 caps t
at 3 × Resource.MAX_EFFICIENT_SIZE (~3 MiB) in ResourceAdvertisement.unpack, so a 1 KiB request or a small
response could still cost ~3 MiB per link. So RNS.Resource.accept is wrapped once to reject, before anything is
allocated (on every link with PROCESS, on rnfs.dht links only with DHT):
- every request sent as a Resource: rnfs requests always fit in one packet;
- any advertisement whose transfer size exceeds its data size plus encryption overhead, or exceeds
  MAX_RESPONSE_SIZE plus that overhead (the largest response rnfs accepts is one Resource segment);
- on rnfs.dht links, any multi-segment Resource: the checks above see one segment at a time, so a
  response of many segments (hundreds of MiB, later read wholly into memory) would pass them, while every
  legitimate rnfs response fits one. Other destinations' Resources are told apart by the link's destination
  name, which this node chose itself (LXMF's delivery and propagation node links refuse several segments in
  features/lxmf.py, which also covers an embedded node);
- on rnfs.dht links, any Resource carrying metadata: RNS decodes metadata with umsgpack as the Resource
  completes, before anyone can look at it, and rnfs responses never have any;
- on rnfs.dht links, any compressed Resource (the advertisement's compressed flag): RNS would decompress it as
  it completes, before any rnfs check, and rnfs never compresses. Refused before any part is transferred, and
  each accepted rnfs.dht Resource also gets a decompression limit of 0, so one that passed couldn't be
  decompressed either.

RNS also decodes a whole response with umsgpack before handing it to rnfs, and a payload of one-byte empty
arrays or maps decodes to some 70 times its size in Python objects (1 MiB: ~70 MB and ~0.3 s of CPU). So on
rnfs.dht links two more RNS methods are wrapped, to check the packed response's msgpack headers first
(protocol.response_refusal, which costs the same for any size) and refuse what can't be an rnfs response:
- Link.response_resource_concluded, for responses sent as a Resource;
- Link.receive, for responses sent as one packet. A packet larger than RNS's default MTU (only possible on a
  link that negotiated a larger one, up to hundreds of KiB) is decrypted and checked first, so such a link
  decrypts an honest one twice. An rnfs.dht server sends no requests, so a response packet arriving on an
  incoming link is dropped unread: RNS would otherwise decode it before finding no request it answers.
Link.receive likewise drops, unread, a request packet arriving on an rnfs.dht link this node opened: the remote
destination of such a link has no request handlers and no max_request_size, so RNS would decode any request up
to the link's MDU and start a thread for it before finding nothing to run it. The node's server takes requests
on incoming links only.
A refused response answers its request with None, which every rnfs.dht parser refuses as malformed, so the
requester fails it at once, as it would a malformed response.

It also bounds half-open incoming links (section 3, link exhaustion). For every link request, RNS's
Destination.incoming_link_request runs Link.validate_request, which does the key exchange, sends a proof,
registers the link and starts a watchdog thread; the link then waits up to 6 s × hops + 360 s for the RTT
packet that makes it ACTIVE, and only then tells rnfs (the link established callback, where
max_incoming_links applies). limit_half_open wraps a destination's incoming_link_request so that, before a new
request is validated, links still waiting (PENDING or HANDSHAKE) are kept below a cap of their own: when it is
reached, the oldest half-open link on the interface holding the most is closed, rather than the new one
refused. An honest handshake completes within about one round trip, so a flood can't lock honest initiators
out by keeping the cap full, and a flood on one interface closes its own links first.
"""
import threading
import time
from collections import Counter

import RNS

from rnfs import __version__
from rnfs.constants import APP_NAME, DHT_ASPECT, MAX_RESPONSE_SIZE
from rnfs.log import LOG_ERROR, LOG_VERBOSE, LOG_WARNING, ThrottledLog, log, plural
from rnfs.network import protocol

# Resource data is a 4-byte random hash plus the payload, encrypted as an RNS Token: 16-byte IV, 32-byte HMAC
# and up to 16 bytes of PKCS7 padding. 128 bytes leaves room without letting the transfer outgrow the data.
TRANSFER_OVERHEAD = 128
MAX_TRANSFER_SIZE = MAX_RESPONSE_SIZE + TRANSFER_OVERHEAD

PROCESS = "process"  # install()'s scopes: every link of the process (the daemon) ...
DHT = "dht"  # ... or rnfs.dht links only (a node embedded in another program)

_lock = threading.Lock()
_scope = PROCESS  # what the advertisement checks cover; set by install()
_compress_limit = None  # Resource.AUTO_COMPRESS_MAX_SIZE before a PROCESS install set it to 0, if one did
_original_accept = None
_original_response_resource_concluded = None
_original_receive = None
_unsupported_reported = False  # the error for an RNS whose advertisements lack t and d, logged once per process
_ABSENT = object()


DHT_NAME = f"{APP_NAME}.{DHT_ASPECT}"


def is_dht_link(link) -> bool:
    """True if link (an RNS.Link, either end) belongs to rnfs.dht: its destination's name is "rnfs.dht"
    plus the identity's hex hash, as RNS.Destination.expand_name builds it."""
    # An outgoing link has the remote destination; an incoming one ours, set (with owner) by
    # Link.validate_request before the link is registered, so from its first packet on
    destination = getattr(link, "destination", None) or getattr(link, "owner", None)
    name = getattr(destination, "name", None)
    return isinstance(name, str) and (name == DHT_NAME or name.startswith(DHT_NAME + "."))


def _report_unsupported():
    """Log, once per process, that RNS's advertisements no longer have the fields the guard reads."""
    global _unsupported_reported
    with _lock:
        if _unsupported_reported:
            return
        _unsupported_reported = True
    log(f"this RNS version ({getattr(RNS, '__version__', 'unknown')}) isn't supported by rnfs {__version__}: "
        f"incoming Resources are being refused", LOG_ERROR)


def scope() -> str:
    """What the advertisement checks cover: PROCESS or DHT (install())."""
    return _scope


def refusal(adv, link=None) -> str | None:
    """Why an unpacked ResourceAdvertisement arriving on link must be rejected, or None if it may be accepted."""
    dht = link is not None and is_dht_link(link)
    if not dht and _scope == DHT:
        return None  # an embedded node's host program: its links are RNS's alone
    transfer, data = getattr(adv, "t", _ABSENT), getattr(adv, "d", _ABSENT)
    if transfer is _ABSENT or data is _ABSENT:
        # Not a malformed advertisement but an RNS that renamed the fields: every Resource is refused (failing
        # open would undo the guard), so say why, loudly
        _report_unsupported()
        return "malformed sizes"
    if type(transfer) is not int or type(data) is not int or transfer < 0 or data < 0:
        return "malformed sizes"
    if getattr(adv, "u", False):
        return "requests are never sent as Resources"
    if transfer > data + TRANSFER_OVERHEAD:
        return f"transfer size {transfer} exceeds data size {data}"
    if transfer > MAX_TRANSFER_SIZE:
        return f"transfer size {transfer} exceeds the largest response"
    if dht and (getattr(adv, "l", 1) != 1 or getattr(adv, "i", 1) != 1):
        return f"rnfs responses are one segment, not {getattr(adv, 'l', '?')}"
    if dht and getattr(adv, "x", False):
        return "rnfs responses carry no metadata"
    if dht and getattr(adv, "c", False):  # last: whatever else the daemon refused is logged as before
        return "rnfs Resources are never compressed"
    return None


def _fail_pending_request(packet, adv):
    """Fail the request a rejected response belongs to, so the requester doesn't wait out a timeout."""
    link = getattr(packet, "link", None)
    request_id = getattr(adv, "q", None)
    if link is None or request_id is None or not getattr(adv, "p", False):
        return
    for pending in list(getattr(link, "pending_requests", [])):
        if getattr(pending, "request_id", None) == request_id:
            try:
                pending.response_rejected()
            except Exception:  # noqa: BLE001 - RNS internals: best effort
                pass


def guarded_accept(advertisement_packet, callback=None, progress_callback=None, request_id=None):
    """RNS.Resource.accept, but rejecting advertisements refusal() objects to. An rnfs.dht Resource accepted
    decompresses nothing (refusal() already refused a compressed one)."""
    link = getattr(advertisement_packet, "link", None)
    try:
        adv = RNS.ResourceAdvertisement.unpack(advertisement_packet.plaintext)
    except Exception:  # noqa: BLE001 - unreadable advertisements are left to RNS
        adv = None  # let RNS handle an advertisement it can't read, as it always has
    reason = refusal(adv, link) if adv is not None else None
    if reason is not None:
        log(f"rejected an incoming Resource: {reason}", LOG_WARNING)
        RNS.Resource.reject(advertisement_packet)
        _fail_pending_request(advertisement_packet, adv)
        return None
    resource = _original_accept(advertisement_packet, callback=callback, progress_callback=progress_callback,
                                request_id=request_id)
    if isinstance(resource, RNS.Resource) and link is not None and is_dht_link(link):
        resource.max_decompressed_size = 0  # what Resource.assemble decompresses at most
    return resource


# Responses checked before RNS decodes them

PACKET_CHECK_SIZE = RNS.Reticulum.MTU  # response packets larger than RNS's default MTU are checked first
REFUSAL_LOG_INTERVAL = 60.0  # seconds between log lines about refused responses, after the first

# Whoever sends refused or unexpected responses or requests can't flood the log
_log_refused_response = ThrottledLog(REFUSAL_LOG_INTERVAL)
_log_unexpected_response = ThrottledLog(REFUSAL_LOG_INTERVAL)
_log_unexpected_request = ThrottledLog(REFUSAL_LOG_INTERVAL)


def _answer_as_malformed(link, request_id, size: int):
    """Answer the request request_id on link with None, which every rnfs.dht parser refuses as malformed."""
    if request_id is None:
        return
    try:
        link.handle_response(request_id, None, size, size)
    except Exception as e:  # noqa: BLE001 - runs inside RNS's packet handling; logged
        log(f"error failing a request whose response was refused: {e}", LOG_WARNING)


def _read_resource(resource) -> bytes | None:
    """A completed response Resource's data (an open file, which RNS reads next), or None if unreadable."""
    data = getattr(resource, "data", None)
    try:
        packed = data.read()
        data.seek(0)
    except Exception:  # noqa: BLE001 - RNS's stream: unreadable means not checked here
        return None
    return packed if isinstance(packed, (bytes, bytearray)) else None


def guarded_response_resource_concluded(link, resource):
    """RNS.Link.response_resource_concluded, but on rnfs.dht links refusing a completed response that can't be
    an rnfs response before RNS decodes it."""
    if is_dht_link(link) and getattr(resource, "status", None) == RNS.Resource.COMPLETE:
        reason = "rnfs responses carry no metadata" if getattr(resource, "has_metadata", False) else None
        packed = _read_resource(resource) if reason is None else None
        if packed is not None:
            reason = protocol.response_refusal(packed)
        if reason is not None:
            _log_refused_response(f"refused a response before decoding it: {reason}", LOG_WARNING)
            _answer_as_malformed(link, getattr(resource, "request_id", None),
                                 getattr(resource, "total_size", None) or len(packed or b""))
            return None
    return _original_response_resource_concluded(link, resource)


def guarded_receive(link, packet):
    """RNS.Link.receive, but on rnfs.dht links checking a response packet before RNS decodes it, and dropping a
    request packet on a link this node opened."""
    if getattr(packet, "context", None) == RNS.Packet.REQUEST and getattr(link, "initiator", False) is True \
            and getattr(packet, "packet_type", None) == RNS.Packet.DATA and is_dht_link(link):
        # The remote destination of a link this node opened has no max_request_size and no handlers, so RNS
        # would decode the request (up to the link's MDU) and start a thread for it before finding nothing to run
        _log_unexpected_request("dropped a request arriving on an outgoing link, which serves none", LOG_VERBOSE)
        return None
    if getattr(packet, "context", None) == RNS.Packet.RESPONSE and is_dht_link(link) \
            and getattr(packet, "packet_type", None) == RNS.Packet.DATA:
        if not getattr(link, "initiator", False):
            _log_unexpected_response("dropped a response arriving on an incoming link, which has no requests",
                                     LOG_VERBOSE)
            return None
        data = getattr(packet, "data", b"")
        if len(data) > PACKET_CHECK_SIZE:
            try:
                packed = link.decrypt(data)
            except Exception:  # noqa: BLE001 - undecryptable is left to RNS, as before
                packed = None
            reason = protocol.response_refusal(packed) if packed is not None else None
            if reason is not None:
                _log_refused_response(f"refused a response before decoding it: {reason}", LOG_WARNING)
                _answer_as_malformed(link, protocol.packed_request_id(packed), len(packed))
                return None
    return _original_receive(link, packet)


def _wrapped(owner, name: str, guard) -> bool:
    """True if owner.name (set on owner itself, plain or as a staticmethod) is guard."""
    current = owner.__dict__.get(name)
    return getattr(current, "__func__", current) is guard


def _wrap(owner, name: str, guard, staticmethod_: bool = False):
    """Replace owner.name with guard unless it is already; the original it replaced."""
    if _wrapped(owner, name, guard):
        return None
    original = getattr(owner, name)
    setattr(owner, name, staticmethod(guard) if staticmethod_ else guard)
    return original


def install(scope: str | None = None):
    """Wrap RNS.Resource.accept, Link.response_resource_concluded and Link.receive with the guards. Idempotent;
    called by the node, RequestServer and LinkPool.

    scope is PROCESS (the daemon: the advertisement checks cover every link, and RNS's process-wide
    decompression limit is set to 0, which also stops it compressing what this process sends) or DHT (an
    embedded node: rnfs.dht links only, and the limit is left as the host program has it, or put back if a
    PROCESS install changed it); None keeps the scope already set (PROCESS at first).
    """
    global _original_accept, _original_response_resource_concluded, _original_receive, _scope, _compress_limit
    with _lock:
        if scope is not None:
            if scope not in (PROCESS, DHT):
                raise ValueError(f"unknown guard scope {scope!r}")
            _scope = scope
        if _scope == PROCESS:
            # Refuses compressed incoming Resources (RNS decompresses at most this many bytes, so any compressed
            # payload fails) and stops RNS compressing outgoing ones (section 3)
            if _compress_limit is None:
                _compress_limit = RNS.Resource.AUTO_COMPRESS_MAX_SIZE
            RNS.Resource.AUTO_COMPRESS_MAX_SIZE = 0
        elif _compress_limit is not None:
            RNS.Resource.AUTO_COMPRESS_MAX_SIZE, _compress_limit = _compress_limit, None
        original = _wrap(RNS.Resource, "accept", guarded_accept, staticmethod_=True)
        if original is not None:
            _original_accept = original
        original = _wrap(RNS.Link, "response_resource_concluded", guarded_response_resource_concluded)
        if original is not None:
            _original_response_resource_concluded = original
        original = _wrap(RNS.Link, "receive", guarded_receive)
        if original is not None:
            _original_receive = original


def installed() -> bool:
    """True if the guards are in place."""
    return (_wrapped(RNS.Resource, "accept", guarded_accept)
            and _wrapped(RNS.Link, "response_resource_concluded", guarded_response_resource_concluded)
            and _wrapped(RNS.Link, "receive", guarded_receive))


# Half-open incoming links

HALF_OPEN_LOG_INTERVAL = 60.0  # seconds between log lines about closing half-open links


def half_open_links(destination) -> list:
    """destination's incoming links not yet ACTIVE (PENDING or HANDSHAKE: waiting for the RTT packet), oldest
    first."""
    waiting = (RNS.Link.PENDING, RNS.Link.HANDSHAKE)
    links = [link for link in list(getattr(destination, "links", None) or [])
             if getattr(link, "status", None) in waiting]
    # destination.links is in arrival order; Link.request_time (set by validate_request) confirms it
    return sorted(links, key=lambda link: getattr(link, "request_time", None) or 0.0)


def half_open_to_close(links: list, interface=None):
    """Of half-open links (oldest first), the one to close to make room for a request arriving on interface:
    the oldest on the interface holding the most (the request's own on a tie), so a flood arriving on one
    interface closes its own links before anyone else's."""
    if not links:
        return None
    held = Counter(id(getattr(link, "attached_interface", None)) for link in links)
    most = max(held.values())
    busiest = id(interface) if held.get(id(interface)) == most else None
    return next(link for link in links if held[id(getattr(link, "attached_interface", None))] == most
                and busiest in (None, id(getattr(link, "attached_interface", None))))


class HalfOpenLimit:
    """Wraps one destination's incoming_link_request to keep its half-open links below limit (see the module)."""

    def __init__(self, destination, limit: int, clock=time.monotonic):
        """Guard destination at limit half-open links (at least 1), wrapping its current incoming_link_request."""
        self.destination = destination
        self.limit = max(1, int(limit))
        self.clock = clock
        self.original = destination.incoming_link_request
        self.lock = threading.Lock()
        self.closed = 0  # half-open links closed to make room, in all
        self.logged_at = None
        self.logged_closed = 0

    def incoming_link_request(self, data, packet):
        """RNS's Destination.incoming_link_request, after closing the oldest half-open links over the limit.

        Runs on RNS's inbound packet thread, so it only does quick work: nothing here waits on established
        links' processing.
        """
        interface = getattr(packet, "receiving_interface", None)
        with self.lock:
            waiting = half_open_links(self.destination)
            while len(waiting) >= self.limit:
                link = half_open_to_close(waiting, interface)
                waiting.remove(link)
                self._close(link)
            return self.original(data, packet)

    def _close(self, link):
        """Close a half-open link. RNS's teardown() sends the initiator a close packet, so an honest one whose
        handshake was cut short learns at once and can try again."""
        try:
            link.teardown()
        except Exception as e:  # noqa: BLE001 - runs on the guard's timer thread; logged
            log(f"error closing a half-open link: {e}", LOG_WARNING)
            return
        self.closed += 1
        now = self.clock()
        if self.logged_at is None or now - self.logged_at >= HALF_OPEN_LOG_INTERVAL:
            log(f"too many incoming links waiting to complete their handshake: closed "
                f"{plural(self.closed - self.logged_closed, 'half-open link')}, oldest first", LOG_WARNING)
            self.logged_at, self.logged_closed = now, self.closed


def limit_half_open(destination, limit: int) -> HalfOpenLimit | None:
    """Keep destination's half-open incoming links below limit (see the module). Idempotent: a destination
    already guarded keeps its guard, with the new limit. None if destination has no incoming_link_request (an
    RNS that changed, or a test's fake)."""
    current = getattr(destination, "incoming_link_request", None)
    guard = getattr(current, "__self__", None)
    if isinstance(guard, HalfOpenLimit):
        guard.limit = max(1, int(limit))
        return guard
    if not callable(current):
        return None
    guard = HalfOpenLimit(destination, limit)
    destination.incoming_link_request = guard.incoming_link_request
    return guard
