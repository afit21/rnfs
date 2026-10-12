"""A process-wide guard on incoming Resource advertisements (sections 3 and 10: oversized-response bombs).

RNS checks Destination.set_max_request_size and a request's max_response_size only against an
advertisement's data size (d), but receives, buffers and allocates by its transfer size (t). RNS 1.5.6 caps t
at 3 × Resource.MAX_EFFICIENT_SIZE (~3 MiB) in ResourceAdvertisement.unpack, so a 1 KiB request or a small
response could still cost ~3 MiB per link. rnfs runs in its own process (section 3), so it wraps
RNS.Resource.accept once to reject, before anything is allocated:
- every request sent as a Resource: rnfs requests always fit in one packet;
- any advertisement whose transfer size exceeds its data size plus encryption overhead, or exceeds
  MAX_RESPONSE_SIZE plus that overhead (the largest response rnfs accepts is one Resource segment);
- on rnfs.dht links, any multi-segment Resource: the checks above see one segment at a time, so a
  response of many segments (hundreds of MiB, later read wholly into memory) would pass them, while every
  legitimate rnfs response fits one. Other destinations' Resources (LXMF's, whose messages may span
  segments) are told apart by the link's destination name, which this node chose itself.
"""
import threading

import RNS

from rnfs import __version__
from rnfs.constants import APP_NAME, DHT_ASPECT, MAX_RESPONSE_SIZE
from rnfs.log import LOG_ERROR, LOG_WARNING, log

# Resource data is a 4-byte random hash plus the payload, encrypted as an RNS Token: 16-byte IV, 32-byte HMAC
# and up to 16 bytes of PKCS7 padding. 128 bytes leaves room without letting the transfer outgrow the data.
TRANSFER_OVERHEAD = 128
MAX_TRANSFER_SIZE = MAX_RESPONSE_SIZE + TRANSFER_OVERHEAD

_lock = threading.Lock()
_original_accept = None
_unsupported_reported = False  # the error for an RNS whose advertisements lack t and d, logged once per process
_ABSENT = object()


DHT_NAME = f"{APP_NAME}.{DHT_ASPECT}"


def is_dht_link(link) -> bool:
    """True if link (an RNS.Link, either end) belongs to rnfs.dht: its destination's name is "rnfs.dht"
    plus the identity's hex hash, as RNS.Destination.expand_name builds it."""
    # An outgoing link has the remote destination; an incoming one has none, and our own as its owner
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


def refusal(adv, link=None) -> str | None:
    """Why an unpacked ResourceAdvertisement arriving on link must be rejected, or None if it may be accepted."""
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
    if link is not None and is_dht_link(link) and (getattr(adv, "l", 1) != 1 or getattr(adv, "i", 1) != 1):
        return f"rnfs responses are one segment, not {getattr(adv, 'l', '?')}"
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
            except Exception:
                pass


def guarded_accept(advertisement_packet, callback=None, progress_callback=None, request_id=None):
    """RNS.Resource.accept, but rejecting advertisements refusal() objects to."""
    try:
        adv = RNS.ResourceAdvertisement.unpack(advertisement_packet.plaintext)
    except Exception:
        adv = None  # let RNS handle an advertisement it can't read, as it always has
    reason = refusal(adv, getattr(advertisement_packet, "link", None)) if adv is not None else None
    if reason is not None:
        log(f"rejected an incoming Resource: {reason}", LOG_WARNING)
        RNS.Resource.reject(advertisement_packet)
        _fail_pending_request(advertisement_packet, adv)
        return None
    return _original_accept(advertisement_packet, callback=callback, progress_callback=progress_callback,
                            request_id=request_id)


def install():
    """Wrap RNS.Resource.accept with the guard. Idempotent; called by RequestServer and LinkPool."""
    global _original_accept
    with _lock:
        current = RNS.Resource.__dict__.get("accept")
        function = getattr(current, "__func__", current)
        if function is guarded_accept:
            return
        _original_accept = RNS.Resource.accept
        RNS.Resource.accept = staticmethod(guarded_accept)


def installed() -> bool:
    """True if the guard is in place."""
    current = RNS.Resource.__dict__.get("accept")
    return getattr(current, "__func__", current) is guarded_accept
