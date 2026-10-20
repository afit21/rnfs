"""The node's path requests: one budget for all of them (specification section 3, "Path requests").

Since RNS 1.2.5 every interface limits the path requests arriving on it: above 8 a second by RNS's estimate (3 while
the interface is under 2 hours old, as a hub's interface for a client that just connected is), later requests wait in
a short queue behind all other traffic or are dropped, and those let through aren't forwarded to find an unknown path.
The estimate counts every application's requests, so a burst from rnfs costs other users of the next hop their path
resolution too. A node therefore paces its own:

- **One budget per node** (PathRequests; LinkPool makes it and the node hands it to names and NomadNet fetches): every
  path request rnfs sends (link opens, so lookups, stores, fetches and keeper queries; a lookup's path check; name
  resolves; NomadNet adds) takes a turn, at least `spacing` (SPACING, 1 s) after the last. Requests 1 s apart peak at
  an estimate of 1.5 a second at the next hop, half a new interface's limit. Waiting for a turn isn't a failure and
  isn't part of `path_timeout`, which starts once the request is sent. Turns are taken in order of asking.
- **A ceiling**: before a request goes, the busiest interface's `outgoing_pr_frequency` (RNS's estimate of the path
  requests this instance sends there, other applications' included; Reticulum.get_interface_stats, which works through
  a shared instance too) must be at most CEILING, else the request takes a later turn, for at most CEILING_HOLD. Not
  RNS's `pr_burst_active`: that comes from other nodes' requests, so on a shared channel every node would wait on the
  same signal and resume together.
- **One wait per destination at a time** (RNS's Transport.await_path doesn't join concurrent callers): a second caller
  waits for the first's request. Nothing is sent again for a destination within PATH_REQUEST_MI (RNS's minimum
  interval for its own requests, 20 s) of the last request: a caller then waits `timeout` for the answer still on
  its way.
- **A link that fails to establish costs a turn** (charge): RNS then expires the path and requests it again by itself
  (RNS 1.5.6 Transport.jobs, and rnsd for its local clients' links), which also counts as a request for its
  destination.

Blocking: never call it from an RNS callback thread.
"""
import math
import threading
import time
from collections import OrderedDict
from typing import Callable

import RNS

from rnfs.log import LOG_DEBUG, LOG_WARNING, ThrottledLog, log, node_name
from rnfs.network.rates import LOCAL_INTERFACE_TYPES

SPACING = 1.0  # seconds between two path requests of a node
CEILING = 1.5  # path requests a second the busiest interface may already be sending (RNS's estimate) for one to go
CEILING_HOLD = 15.0  # seconds the ceiling holds a request back at most (RNS's IC_BURST_HOLD: a limit lasts that long)
PATH_REQUEST_MI = 20  # RNS.Transport.PATH_REQUEST_MI: nothing is sent again for a destination within it
POLL = 0.1  # seconds between checks while waiting for a turn or an identity
MAX_TRACKED = 4096  # destinations the table of requests sent remembers at most


def _rns_transport():
    """RNS.Transport, the default transport."""
    return RNS.Transport


def _rns_instance():
    """The running RNS.Reticulum instance, or None."""
    return RNS.Reticulum.get_instance()


def busiest_pr_frequency(stats) -> float:
    """The highest outgoing_pr_frequency among the interfaces of Reticulum.get_interface_stats() a path request leaves
    by (0.0 if none says): local interfaces (to or of a shared instance), offline ones, and a parent of spawned
    interfaces (a hub's server interface, an AutoInterface), which counts a copy of every request each of its spawned
    interfaces sends, are left out."""
    entries = [entry for entry in ((stats or {}).get("interfaces") or []) if isinstance(entry, dict)]
    parents = {entry.get("parent_interface_hash") for entry in entries} - {None}
    busiest = 0.0
    for entry in entries:
        if str(entry.get("type", "")) in LOCAL_INTERFACE_TYPES or not entry.get("status"):
            continue
        if entry.get("hash") is not None and entry.get("hash") in parents:
            continue
        frequency = entry.get("outgoing_pr_frequency")
        if isinstance(frequency, (int, float)) and not isinstance(frequency, bool) and math.isfinite(frequency):
            busiest = max(busiest, float(frequency))
    return busiest


class PathRequests:
    """A node's path requests under one budget (see the module docstring). Thread-safe.

    transport() and reticulum() give RNS.Transport and the running Reticulum instance (callers whose RNS tests replace
    pass their own); clock and sleep are monotonic time and waiting, replaced by tests."""

    def __init__(self, transport: Callable[[], object] = _rns_transport,
                 reticulum: Callable[[], object] = _rns_instance, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], object] = time.sleep, spacing: float = SPACING):
        """No request sent yet: the first takes its turn at once."""
        self._transport = transport
        self._reticulum = reticulum
        self.clock = clock
        self.sleep = sleep
        self.spacing = spacing
        self._lock = threading.Lock()
        self._next_turn = -math.inf  # the earliest time the next turn may be
        self._waits: dict[bytes, threading.Event] = {}  # destination -> set when its owner's wait ends
        self._sent: OrderedDict[bytes, float] = OrderedDict()  # destination -> when last requested, oldest first
        self._failure_log = ThrottledLog()
        # Counts, for tests and measurements
        self.requests = 0  # path requests sent
        self.charged = 0  # links that failed to establish, each a request RNS sends by itself
        self.joined = 0  # waits that joined another caller's for the same destination
        self.reused = 0  # waits within PATH_REQUEST_MI of the destination's last request, which sent nothing
        self.held = 0  # requests the ceiling held back at least once
        self.waited = 0.0  # seconds spent waiting for turns, in all

    # Waiting for a path or an identity

    def await_path(self, destination: bytes, timeout: float, cancel: threading.Event | None = None) -> bool:
        """Wait for a path to destination, requesting one in its turn if RNS has none: True once there is one. timeout
        (path_timeout) counts from when the request is sent; cancel, once set, ends a wait for a turn or another
        caller's request (False)."""
        transport = self._transport()
        if transport.has_path(destination):
            return True
        return self._wait(destination, transport, lambda: bool(transport.has_path(destination)), timeout, cancel,
                          rns_wait=True)

    def await_known(self, destination: bytes, known: Callable[[], bool], timeout: float,
                    cancel: threading.Event | None = None) -> bool:
        """Request a path to destination in its turn (its answer carries the identity) and wait until known() (polled
        every POLL) holds: True if it did within timeout of the request. For a node RNS has a path to but no identity
        for; as await_path otherwise."""
        return self._wait(destination, self._transport(), known, timeout, cancel, rns_wait=False)

    def send(self, destination: bytes, cancel: threading.Event | None = None) -> bool:
        """Request a path to destination in its turn on every interface, without waiting for the answer (names: the
        answer is the name's announce). False if cancel was set first."""
        transport = self._transport()
        if not self._take_turn(destination, lambda: False, cancel):
            return False
        self._note_sent(destination)
        transport.request_path(destination)
        return True

    # What else counts

    def charge(self, destination: bytes):
        """A link to destination failed to establish: RNS expires the path and requests it again by itself, which takes
        a turn now and counts as a request for destination (none is sent again within PATH_REQUEST_MI)."""
        with self._lock:
            now = self.clock()
            self._next_turn = max(now, self._next_turn) + self.spacing
            self._remember_sent(destination, now)
            self.charged += 1

    # Internals

    def _wait(self, destination: bytes, transport, ready: Callable[[], bool], timeout: float,
              cancel: threading.Event | None, rns_wait: bool) -> bool:
        """The wait of await_path (rns_wait: RNS's await_path waits for the path once the turn comes) or await_known,
        joining a wait already running for destination."""
        with self._lock:
            event = self._waits.get(destination)
            owner = event is None
            if owner:
                event = self._waits[destination] = threading.Event()
            else:
                self.joined += 1
        if not owner:
            while not event.wait(POLL):
                if ready():
                    return True
                if cancel is not None and cancel.is_set():
                    return False
            return bool(ready())
        try:
            return self._own_wait(destination, transport, ready, timeout, cancel, rns_wait)
        finally:
            with self._lock:
                self._waits.pop(destination, None)
            event.set()

    def _own_wait(self, destination: bytes, transport, ready: Callable[[], bool], timeout: float,
                  cancel: threading.Event | None, rns_wait: bool) -> bool:
        """Request destination's path in its turn (unless one was requested within PATH_REQUEST_MI) and wait."""
        with self._lock:
            last = self._sent.get(destination)
            recent = last is not None and 0 <= self.clock() - last < PATH_REQUEST_MI
            if recent:
                self.reused += 1
        if recent:
            return self._poll(ready, timeout, cancel)  # its answer may still be on its way
        if not self._take_turn(destination, ready, cancel):
            return bool(ready())
        self._note_sent(destination)
        if rns_wait:
            return bool(transport.await_path(destination, timeout=timeout))
        transport.request_path(destination)
        return self._poll(ready, timeout, cancel)

    def _take_turn(self, destination: bytes, ready: Callable[[], bool], cancel: threading.Event | None) -> bool:
        """Wait for a turn to request destination's path: True when it may go, False if ready() held meanwhile (no
        request needed) or cancel was set."""
        started = self.clock()
        held_since: float | None = None
        try:
            while True:
                with self._lock:
                    turn = max(self.clock(), self._next_turn)
                    self._next_turn = turn + self.spacing
                while (now := self.clock()) < turn:
                    if ready() or (cancel is not None and cancel.is_set()):
                        return False
                    self.sleep(min(POLL, turn - now))
                if ready() or (cancel is not None and cancel.is_set()):
                    return False
                if not self._over_ceiling():
                    return True
                if held_since is None:
                    held_since = self.clock()
                    with self._lock:
                        self.held += 1
                elif self.clock() - held_since >= CEILING_HOLD:
                    return True
        finally:
            waited = self.clock() - started
            with self._lock:
                self.waited += waited
            if waited >= self.spacing:
                log(f"path request for {node_name(destination)} waited {waited:.0f}s for its turn", LOG_DEBUG)

    def _over_ceiling(self) -> bool:
        """True if the busiest interface already sends more than CEILING path requests a second (False if the stats
        can't be read)."""
        try:
            reticulum = self._reticulum()
            if reticulum is None:
                return False
            return busiest_pr_frequency(reticulum.get_interface_stats()) > CEILING
        except Exception as e:  # noqa: BLE001 - RNS internals or the instance's RPC: no ceiling then; logged
            self._failure_log(f"couldn't read the interfaces' path request rates: {e!r}", LOG_WARNING)
            return False

    def _poll(self, ready: Callable[[], bool], timeout: float, cancel: threading.Event | None) -> bool:
        """Wait up to timeout for ready() (checked every POLL): True once it holds."""
        deadline = self.clock() + timeout
        while True:
            if ready():
                return True
            now = self.clock()
            if now >= deadline or (cancel is not None and cancel.is_set()):
                return False
            self.sleep(min(POLL, deadline - now))

    def _note_sent(self, destination: bytes):
        """A request for destination is being sent now."""
        with self._lock:
            self._remember_sent(destination, self.clock())
            self.requests += 1

    def _remember_sent(self, destination: bytes, now: float):
        """Record destination as requested at now, forgetting requests older than PATH_REQUEST_MI. Lock held."""
        self._sent[destination] = now
        self._sent.move_to_end(destination)
        while self._sent:
            oldest = next(iter(self._sent.values()))
            if len(self._sent) <= MAX_TRACKED and now - oldest < PATH_REQUEST_MI:
                break
            self._sent.popitem(last=False)
