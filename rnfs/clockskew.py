"""How far this node's clock seems to be from the network's, estimated from the emission times of the announces it
hears (specification section 9, "Announces and the clock").

Every RNS announce carries the time its sender emitted it (by the sender's clock, signed). An announce heard live
was emitted moments ago, so its emission time less the time it arrived is the sender's clock against this node's,
less the delay on the way. The estimate is the median of that over the senders heard recently, one value each (its
least delayed announce in the window), so a few wrong or lying clocks don't move it: they need more than half of the
senders. A node with many names has one clock, so names' announces count for fewer senders than nodes' do, except
to make up the MIN_SENDERS an estimate needs.

Late announces and replayed old ones can only make this node's clock look ahead, never behind, and RNS delivers
many late: it holds the announces arriving on an interface that just came up (a laptop waking, a node starting)
and releases them one every few seconds (Interface.held_announces), and each transport node queues announces past
its 2% cap. A clock that looks ahead therefore can't be told from announces arriving late, so only "behind" is
reported: as a warning (rnfs.names logs it) when it passes WARN_OFFSET, and in `status` (clock_offset, 0 when the
estimate says ahead). Behind is also the direction that matters: a clock behind a name's last announce is what makes
RNS drop the name's announces (rnfs.names), and only senders whose clocks are ahead can make this node's look so.
It never changes the clock or what the node does. No RNS here: the CLI reads the texts.
"""
import math
import statistics
import threading
import time
from collections import OrderedDict

WARN_OFFSET = 300.0  # seconds: an estimate further behind than this is reported as a warning
CLEAR_OFFSET = 240.0  # ... and once warned, one back within this is reported as fixed (not again at each sample)
MIN_SENDERS = 5  # distinct senders heard within the window before there is an estimate
MAX_SENDERS = 128  # senders kept per kind (nodes, names), the least recently heard forgotten first


def duration_text(seconds: float) -> str:
    """seconds as a short text: "45 s", "12 min", "3 h 5 min", "2 d 4 h"."""
    seconds = int(round(abs(seconds)))
    if seconds < 60:
        return f"{seconds} s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} min"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours} h {minutes} min" if minutes else f"{hours} h"
    days, hours = divmod(hours, 24)
    return f"{days} d {hours} h" if hours else f"{days} d"


def offset_text(offset: float) -> str:
    """An offset (the network's clock less this node's) in words: "5 min behind" or "2 h ahead"."""
    return f"{duration_text(offset)} {'behind' if offset > 0 else 'ahead'}"


class ClockSkew:
    """The estimate, from samples the announce handlers give (node_announced, name_announced). Thread-safe.

    A sample is kept as its emission time less the monotonic time it arrived, so a step of the wall clock (an NTP
    sync) moves every sample's offset with it: the estimate is always against the clock as it is now.
    """

    def __init__(self, window: float, clock=time.time, monotonic=time.monotonic, report=None):
        """Samples count for window seconds after they arrived (2 x announce_interval: every sender announces
        within it). report(text, warning) is told when the estimate passes WARN_OFFSET (warning True) and when it
        comes back within CLEAR_OFFSET (False)."""
        self.window = float(window)
        self._report = report
        self._clock = clock
        self._monotonic = monotonic
        self._lock = threading.Lock()
        # sender -> [emitted less the monotonic arrival of its least delayed announce in the window, that arrival],
        # least recently heard first
        self._nodes: OrderedDict[bytes, list] = OrderedDict()
        self._names: OrderedDict[bytes, list] = OrderedDict()
        self._warned = False

    def node_announced(self, node_id: bytes, emitted) -> None:
        """An rnfs.dht announce of node_id, emitted at emitted (None if unknown), arrived live (not a path response)."""
        self._sample(self._nodes, node_id, emitted)

    def name_announced(self, name: bytes, emitted) -> None:
        """An rnfs.name announce of name, emitted at emitted (None if unknown), arrived live, unasked."""
        self._sample(self._names, name, emitted)

    def _sample(self, senders: OrderedDict, sender: bytes, emitted):
        """Keep sender's sample, then report it if the estimate passed WARN_OFFSET (or came back)."""
        if type(emitted) not in (int, float) or not math.isfinite(emitted) or emitted <= 0:
            return
        sender = bytes(sender)
        with self._lock:
            now = self._monotonic()
            value = float(emitted) - now
            entry = senders.get(sender)
            # Its least delayed announce in the window: delays only ever make a clock look ahead
            if entry is None or now - entry[1] > self.window or value >= entry[0]:
                senders[sender] = [value, now]
            senders.move_to_end(sender)
            while len(senders) > MAX_SENDERS:
                senders.popitem(last=False)
            offset, count = self._estimate(now)
            message = None
            # Only behind: ahead can't be told from announces delivered late (the module's docstring)
            if offset is not None and not self._warned and offset > WARN_OFFSET:
                self._warned = True
                message = (f"this node's clock seems to be {offset_text(offset)}, going by the median of {count} "
                           "senders' recent announces; set it right: while the clock is behind a name's last "
                           "announce, the name's announces are held, since RNS would drop them")
            elif offset is not None and self._warned and offset < CLEAR_OFFSET:
                self._warned = False
                message = "this node's clock is no longer behind the network's"
            warned = self._warned
        if message is not None and self._report is not None:
            self._report(message, warned)

    def _estimate(self, now: float) -> tuple[float | None, int]:
        """(offset, senders) as estimate() gives them, at monotonic time now. Lock held."""
        nodes = [value for value, at in self._nodes.values() if now - at <= self.window]
        names = sorted(((at, value) for value, at in self._names.values() if now - at <= self.window), reverse=True)
        # Names count for fewer senders than nodes, or for as many as make up MIN_SENDERS
        names = [value for _, value in names[:max(len(nodes) - 1, MIN_SENDERS - len(nodes), 0)]]
        values = nodes + names
        if len(values) < MIN_SENDERS:
            return None, len(values)
        base = self._clock() - now  # the wall time at monotonic 0, as the clock says now
        return statistics.median(values) - base, len(values)

    def estimate(self) -> tuple[float | None, int]:
        """(the network's clock less this node's in seconds, positive when this node's is behind, or None before
        MIN_SENDERS senders were heard within the window; how many senders it is the median of)."""
        with self._lock:
            return self._estimate(self._monotonic())

    def behind(self) -> float | None:
        """How far behind the network's this node's clock seems, in seconds: the estimate, or 0 when it says ahead
        (which can't be told from announces arriving late); None before there is an estimate."""
        offset, _ = self.estimate()
        return None if offset is None else max(0.0, offset)

    def status(self) -> dict:
        """{"clock_offset": behind() in whole seconds, or None}: a part of the node's status."""
        behind = self.behind()
        return {"clock_offset": None if behind is None else round(behind)}
