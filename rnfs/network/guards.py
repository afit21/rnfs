"""A process-wide guard on incoming Resource advertisements (sections 3 and 10: oversized-response bombs).

RNS checks Destination.set_max_request_size and a request's max_response_size only against an
advertisement's data size (d), but receives, buffers and allocates by its transfer size (t). RNS 1.5.6 caps t
at 3 × Resource.MAX_EFFICIENT_SIZE (~3 MiB) in ResourceAdvertisement.unpack, so a 1 KiB request or a small
response could still cost ~3 MiB per link. rnfs runs in its own process (section 3), so it wraps
RNS.Resource.accept once to reject, before anything is allocated:
- every request sent as a Resource: rnfs requests always fit in one packet;
- any advertisement whose transfer size exceeds its data size plus encryption overhead, or exceeds
  MAX_RESPONSE_SIZE plus that overhead (the largest response rnfs accepts is one Resource segment).
"""
import threading

import RNS

from rnfs.constants import MAX_RESPONSE_SIZE
from rnfs.log import LOG_WARNING, log

# Resource data is a 4-byte random hash plus the payload, encrypted as an RNS Token: 16-byte IV, 32-byte HMAC
# and up to 16 bytes of PKCS7 padding. 128 bytes leaves room without letting the transfer outgrow the data.
TRANSFER_OVERHEAD = 128
MAX_TRANSFER_SIZE = MAX_RESPONSE_SIZE + TRANSFER_OVERHEAD

_lock = threading.Lock()
_original_accept = None


def refusal(adv) -> str | None:
    """Why an unpacked ResourceAdvertisement must be rejected, or None if it may be accepted."""
    transfer, data = getattr(adv, "t", None), getattr(adv, "d", None)
    if type(transfer) is not int or type(data) is not int or transfer < 0 or data < 0:
        return "malformed sizes"
    if getattr(adv, "u", False):
        return "requests are never sent as Resources"
    if transfer > data + TRANSFER_OVERHEAD:
        return f"transfer size {transfer} exceeds data size {data}"
    if transfer > MAX_TRANSFER_SIZE:
        return f"transfer size {transfer} exceeds the largest response"
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
    reason = refusal(adv) if adv is not None else None
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
