"""Waiting for content that can't be found yet (rnfs 0.2.0; specification sections 8 and 11).

A remote pin, or a get, pin or export job, whose content no provider could supply (NotFound) isn't given up at once:
content just added elsewhere is found only once its provider's records are out, which on LoRa can take a while. It
waits and is tried again after RETRY_FIRST, the wait doubling up to RETRY_MAX, until RETRY_FOR has passed since it
was first not found; then it fails as before. Each wait is RETRY_FIRST plus the time since the first failure, which
is the sum of the waits before it: 10 minutes, then 20, 40, 80 ... (plus the time each try took), never more than
RETRY_MAX. Times are Unix times, so the waits are kept across a restart. Only time spent waiting counts towards
RETRY_FOR: a try that comes more than LATE after it was due (the node was off, or the clock stepped forward, as a
Raspberry Pi's does when NTP corrects the time it booted with) moves the first failure on by the excess
(counted_since), so it isn't given up untried (REL-11).

Standard library only: the jobs feature (which the CLI imports) uses it.
"""
RETRY_FIRST = 600.0  # seconds before the first retry
RETRY_MAX = 6 * 3600.0  # the longest wait between tries
RETRY_FOR = 3 * 24 * 3600.0  # how long after it was first not found content is tried again
LATE = RETRY_MAX  # a try later than this after its due time: the time between wasn't spent waiting


def counted_since(since: float, now: float, due: float | None) -> float:
    """When content was first not found (since), for the schedule of a try now that was due at due (None: none was):
    moved on by however much more than LATE the try came after it was due."""
    if due is None:
        return since
    late = now - due
    return since + late if late > LATE else since


def next_retry(since: float, now: float) -> float | None:
    """When to try again content first not found at since (Unix time) and not found again at now; None once
    RETRY_FOR has passed since, when it is given up. The last retry is at since + RETRY_FOR at the latest."""
    waited = max(0.0, now - since)
    if waited >= RETRY_FOR:
        return None
    return min(now + min(RETRY_MAX, RETRY_FIRST + waited), since + RETRY_FOR)
