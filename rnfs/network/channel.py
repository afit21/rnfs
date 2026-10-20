"""Pacing by the radio's measured load (specification section 3, "Measured airtime").

RNodes measure their own airtime and the load of the channel they sit on, over 15 s and over an hour, and RNS reports
those figures with each interface's stats (Reticulum.get_interface_stats: airtime_short, airtime_long,
channel_load_short, channel_load_long, percentages; through a shared instance too, by RPC). An RNode reports 0.0 until
its first measurement, so 0.0 counts as unknown, as does an interface without the figures (every interface but an
RNode's). A slow node reads them (ChannelLoad) at most every POLL seconds, and only when paced work asks: a path
request taking its turn, a publishing round's pacing, a /blocks request arriving, a refresh choosing entries. It:

- stretches its pacing while the channel is busy: above BUSY_LOAD, the spacing of its path requests
  (rnfs.network.paths) and of its publishing rounds' lookups, the heal republish's included (rnfs.publisher), grows in
  proportion to the load, up to MAX_STRETCH times. A round's stretch never lets a record's republish slip past
  PROVIDER_TTL / 2 (stretch_wait's slack, which the publisher works out);
- refuses /blocks (an airtime control, rnfs.mesh.AirtimePolicy) while its own airtime over the last hour is above
  AIRTIME_BUDGET: a single paced transfer stays under it (a slow link's upload cap is half its rate estimate,
  rnfs.network.rates.UPLOAD_CAP_DIVISOR), so only a node serving several at once, or busy with other traffic, refuses;
- doesn't turn provide_entries on by default while its own airtime over the last hour is above the publisher's
  ENTRY_AIRTIME_ON (airtime_share): the estimate covers republishing alone, the radio everything it sent.

The channel figures are the busiest RNode's (online, not a shared instance's local interface); the short one is used
while known, being the load now, else the long one. Other nodes, and interfaces without the figures, keep today's
estimates.
"""
import math
import threading
import time
from dataclasses import dataclass
from typing import Callable

from rnfs.log import LOG_WARNING, ThrottledLog
from rnfs.network.rates import LOCAL_INTERFACE_TYPES

POLL = 15.0  # seconds a reading of the interface stats is used for (an RNode's short figures span 15 s)
BUSY_LOAD = 25.0  # percent: a channel more loaded than this is busy
MAX_STRETCH = 4.0  # a busy channel stretches pacing in proportion to its load, up to this many times (at 100%)
AIRTIME_BUDGET = 50.0  # percent of the last hour this node may have transmitted and still answer /blocks
FIGURES = ("airtime_short", "airtime_long", "channel_load_short", "channel_load_long")


@dataclass(frozen=True)
class Load:
    """What the radios measured, in percent; None where unknown."""

    airtime_short: float | None = None  # this node's transmissions over the last 15 s
    airtime_long: float | None = None  # ... and over the last hour
    channel_load_short: float | None = None  # the channel's load (every node's transmissions) over the last 15 s
    channel_load_long: float | None = None  # ... and over the last hour

    @property
    def channel(self) -> float | None:
        """The channel's load now: the short figure while known, else the long one."""
        return self.channel_load_short if self.channel_load_short is not None else self.channel_load_long


UNKNOWN = Load()


def _percent(value) -> float | None:
    """value as a measured percentage: a finite number above 0 and at most 100 (0.0 is what an RNode reports before
    its first measurement), else None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value) if 0 < value <= 100 else None


def measured_load(stats) -> Load:
    """The highest of each figure among the interfaces of Reticulum.get_interface_stats() that report it, online and
    not local (to or of a shared instance)."""
    highest: dict[str, float] = {}
    for entry in (stats or {}).get("interfaces") or []:
        if not isinstance(entry, dict) or str(entry.get("type", "")) in LOCAL_INTERFACE_TYPES or not entry.get("status"):
            continue
        for figure in FIGURES:
            value = _percent(entry.get(figure))
            if value is not None:
                highest[figure] = max(value, highest.get(figure, value))
    return Load(**highest)


def stretch_for(load: float | None) -> float:
    """How many times longer paced work is spaced at a channel load (percent; None: unknown): 1 up to BUSY_LOAD, then
    in proportion to the load, at most MAX_STRETCH."""
    if load is None or load <= BUSY_LOAD:
        return 1.0
    return min(MAX_STRETCH, load / BUSY_LOAD)


def _rns_instance():
    """The running RNS.Reticulum instance, or None."""
    import RNS
    return RNS.Reticulum.get_instance()


class ChannelLoad:
    """A slow node's view of its radios' measured load, read at most every poll seconds. Thread-safe.

    reticulum() gives the running Reticulum instance (None: nothing is known); clock is monotonic time."""

    def __init__(self, reticulum: Callable[[], object] = _rns_instance, clock: Callable[[], float] = time.monotonic,
                 poll: float = POLL):
        """Nothing read yet: the first question reads."""
        self._reticulum = reticulum
        self.clock = clock
        self.poll = poll
        self._load = UNKNOWN
        self._read_at = -math.inf
        self._lock = threading.Lock()
        self._failure_log = ThrottledLog()
        self.reads = 0  # readings of the interface stats so far (tests count them)

    def load(self) -> Load:
        """The radios' figures, read again if the last reading is poll seconds old (one caller reads; the others use
        the last reading meanwhile). UNKNOWN if the stats can't be read."""
        with self._lock:
            now = self.clock()
            if 0 <= now - self._read_at < self.poll:
                return self._load
            self._read_at = now
        try:
            reticulum = self._reticulum()
            load = UNKNOWN if reticulum is None else measured_load(reticulum.get_interface_stats())
        except Exception as e:  # noqa: BLE001 - RNS internals or the instance's RPC: nothing known then; logged
            self._failure_log(f"couldn't read the radios' airtime and channel load: {e!r}", LOG_WARNING)
            load = UNKNOWN
        with self._lock:
            self._load = load
            self.reads += 1
        return load

    def stretch(self) -> float:
        """How many times longer paced work is spaced now (stretch_for the channel's load)."""
        return stretch_for(self.load().channel)

    def stretch_wait(self, wait: float, slack: float) -> float:
        """The seconds to pace by instead of wait while the channel is busy: wait stretched, but never past slack (how
        long the work may slip), and never less than wait."""
        stretch = self.stretch()
        return wait if stretch <= 1.0 else max(wait, min(wait * stretch, slack))

    def over_budget(self) -> bool:
        """True while this node's own airtime over the last hour is above AIRTIME_BUDGET (False if unknown)."""
        airtime = self.load().airtime_long
        return airtime is not None and airtime > AIRTIME_BUDGET

    def airtime_share(self) -> float | None:
        """The share of the channel this node's own transmissions took over the last hour (0-1), or None if unknown."""
        airtime = self.load().airtime_long
        return None if airtime is None else airtime / 100
