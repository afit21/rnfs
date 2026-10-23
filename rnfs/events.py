"""The node's event log for local clients: what changed (jobs, pins, the cache, names, peers, remote pins; from
0.2.1 the settings, name syncs, apps and the status), so an app learns of it through one long poll (GET /api/events,
rnfs/api.py) instead of polling every list. No RNS.

Every event is a dict: "seq" (1, 2, ... for the node's run), "kind" (EVENT_KINDS), "time" (Unix time) and the
kind's own fields (design.md, "Events"). The log keeps the last MAX_EVENTS; a client that asks for events after a
sequence the log no longer holds, of a kind it asks for (it fell behind), or of another run (the daemon restarted:
"epoch" changed) gets "lost": true and every event held, and reloads its lists. Level-triggered like GET /api/name/watch: a client that asks again with
the "next" it was given loses nothing between calls.

publish() is called from anywhere, some callers holding their own locks (the names lock, storage's): it only
appends under the log's condition, the innermost lock, and never calls out. Listeners (an embedded rnfs.Node's
events thread) are told with that condition released, in sequence order (publishers take turns, _order), and must
only queue.
"""
import secrets
import threading
import time
from collections import deque

MAX_EVENTS = 1024  # events kept; a client further behind gets "lost"
MAX_EVENT_CIDS = 100  # CIDs one event lists; "count" says how many changed (a gc may uncache thousands)
# Every kind published, for GET /api/events?kinds= and the capability list
EVENT_KINDS = ("job", "pin", "cache", "name", "peer", "remote_pin", "config", "name_sync", "apps", "status")
RESERVED_FIELDS = frozenset({"seq", "kind", "time"})  # every event's own


class EventLog:
    """The events of one node run, numbered from 1 (see the module docstring). Thread-safe."""

    def __init__(self, clock=time.time, max_events: int = MAX_EVENTS):
        """An empty log; clock gives each event's time."""
        self.clock = clock
        self.epoch = secrets.token_hex(8)  # this run's: a client seeing another knows the daemon restarted
        self._cond = threading.Condition()
        # Held by a publisher from numbering its event until its listeners are told, so listeners hear events in
        # sequence order (two publishers at once would otherwise race to them, and a listener tracking the last
        # sequence it heard would skip the lower one). Taken before _cond, never under it; reentrant, for a
        # listener that publishes
        self._order = threading.RLock()
        self._events: deque[dict] = deque(maxlen=max_events)
        self._last = 0  # the last sequence given
        self._dropped: dict[str, int] = {}  # kind -> the last sequence of that kind the log no longer holds
        self._listeners: tuple = ()

    def publish(self, kind: str, /, **fields) -> dict:
        """Append an event of kind with fields, wake the waiting calls and tell the listeners; the event. Raises
        ValueError for an unknown kind, or a field named as one of the event's own (seq, kind, time)."""
        if kind not in EVENT_KINDS:
            raise ValueError(f"unknown event kind {kind!r}")
        if fields.keys() & RESERVED_FIELDS:
            raise ValueError(f"an event's fields can't be named {', '.join(sorted(fields.keys() & RESERVED_FIELDS))}")
        with self._order:
            with self._cond:
                self._last += 1
                event = {"seq": self._last, "kind": kind, "time": self.clock(), **fields}
                if len(self._events) == self._events.maxlen:
                    oldest = self._events[0]
                    self._dropped[oldest["kind"]] = oldest["seq"]
                self._events.append(event)
                self._cond.notify_all()
                listeners = self._listeners
            for listener in listeners:
                try:
                    listener(event)
                except Exception:  # noqa: BLE001 - a listener only queues; one failing mustn't stop the publisher
                    pass
        return event

    def add_listener(self, listener):
        """Call listener(event) for every event published from now on (it must only queue)."""
        with self._cond:
            self._listeners = (*self._listeners, listener)

    def remove_listener(self, listener):
        """Stop calling listener."""
        with self._cond:
            self._listeners = tuple(item for item in self._listeners if item != listener)

    @property
    def last(self) -> int:
        """The sequence of the last event published (0 before any)."""
        with self._cond:
            return self._last

    def since(self, after: int | None, kinds=None, wait: float = 0.0, epoch: str | None = None) -> dict:
        """{"events", "next", "lost", "epoch"}: the events held after sequence after (of kinds, if given), once
        there is one or after wait seconds. after None: none, only "next" (where to start from now). "lost" is
        True when events after after of kinds are no longer held, or epoch (the one a client was given) isn't this run's:
        the events held are all returned, and the client should reload what it shows."""
        wanted = frozenset(kinds) if kinds else None
        deadline = time.monotonic() + wait
        with self._cond:
            if after is None:
                return {"events": [], "next": self._last, "lost": False, "epoch": self.epoch}
            lost = (epoch is not None and epoch != self.epoch) or after > self._last
            if lost:
                after = 0
            while True:
                # Lost: an event of a kind asked for, after after, that the log dropped (others don't matter here)
                lost = lost or any(seq > after for kind, seq in self._dropped.items()
                                   if wanted is None or kind in wanted)
                found = [event for event in self._events
                         if event["seq"] > after and (wanted is None or event["kind"] in wanted)]
                remaining = deadline - time.monotonic()
                if found or lost or remaining <= 0:
                    return {"events": found, "next": self._last, "lost": lost, "epoch": self.epoch}
                self._cond.wait(remaining)


def cid_fields(cids) -> dict:
    """{"cids": the first MAX_EVENT_CIDS of cids written out, "count": how many}: an event about many pins or
    cached fetches stays small, and a client seeing count above len(cids) reloads the list."""
    cids = list(cids)
    return {"cids": [str(cid) for cid in cids[:MAX_EVENT_CIDS]], "count": len(cids)}


def publish(node, kind: str, /, **fields):
    """Publish an event on node's log, if it has one (a stand-in node in tests may not). Never raises: an event
    lost is a client reloading later, never a failed operation."""
    log = getattr(node, "events", None)
    if log is None:
        return
    try:
        log.publish(kind, **fields)
    except Exception:  # noqa: BLE001 - see above
        pass
