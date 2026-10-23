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
  isn't part of `path_timeout`, which starts once the request is sent. Turns are taken in order of asking. A waiter
  that leaves before its turn (its path or identity arrived, or it was cancelled) gives the turn up, and turns given up
  at the end of the queue are free again, so callers after them don't wait for nothing (AUD-L6). On a slow
  node the spacing stretches while the radio's channel is busy (stretch: rnfs.network.channel.ChannelLoad.stretch,
  which the node sets), up to 4 s.
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
- **Scoping** (named): with its own RNS instance, the path request for a node a /find reply named (its identity
  unknown) goes only on the interface the path to the node that named it uses (Transport.next_hop_interface,
  request_path's on_interface), if that interface is fast (its bitrate at least slow_bitrate), so lookups over the
  internet side don't flood the node's radio with requests for internet nodes. A node named by one reached over a
  slow interface (or one of unknown bitrate) is asked on every interface: it may be an island server, reachable only
  over the radio, or an internet one, reachable only over the other side.
  A spawned interface (a hub's interface for one client, an AutoInterface's for one peer) stands for its parent's
  spawned interfaces: the request goes on each of them with one tag, as RNS itself forwards a local client's request
  interface by interface. A lookup has other candidates, so a miss isn't asked again elsewhere. Joins, names and links
  to known IDs ask on every interface. Through a shared instance nothing is scoped: the instance forwards a client's
  request on all its interfaces (Transport.path_request), so scoping there needs an RNS change. RNS documents
  on_interface as not for normal use, but sending a path request filters interfaces by nothing else (no interface
  mode applies to a request going out: Transport._outbound), so it is safe on every mode; interfaces a request can't
  leave by (offline, not outgoing, local, a parent of spawned ones) are never chosen.
- **Path-response times** (candidate_wait): the time from each request this node sent to its answer (the path, or the
  identity it carries), over waits as long as path_timeout, the last MAX_SAMPLES of them. Once MIN_SAMPLES are
  measured, a lookup's path check on a node that isn't slow waits their 95th percentile, at least RNS's
  PATH_REQUEST_TIMEOUT (15 s) and at most path_timeout, instead of path_timeout; such shortened waits aren't measured,
  so they can't pull the figure down to their own length. Everything else (names, links to a known or named target,
  stores, fetches, a slow node's lookups) waits path_timeout.
- lacked(destination): whether the last wait for its path (or identity) ended without it, so a query that failed
  for want of a path can be told from one whose link failed (partition mode leaves the former out,
  rnfs.dht.local.PartitionMode).

Blocking: never call it from an RNS callback thread.
"""
import math
import os
import threading
import time
from collections import OrderedDict, deque
from typing import Callable

import RNS

from rnfs.log import LOG_DEBUG, LOG_VERBOSE, LOG_WARNING, ThrottledLog, log, node_name
from rnfs.network.rates import LOCAL_INTERFACE_TYPES

SPACING = 1.0  # seconds between two path requests of a node
CEILING = 1.5  # path requests a second the busiest interface may already be sending (RNS's estimate) for one to go
CEILING_HOLD = 15.0  # seconds the ceiling holds a request back at most (RNS's IC_BURST_HOLD: a limit lasts that long)
PATH_REQUEST_MI = 20  # RNS.Transport.PATH_REQUEST_MI: nothing is sent again for a destination within it
POLL = 0.1  # seconds between checks while waiting for a turn or an identity
HINT_LIFE = 300.0  # seconds a node named by a /find reply keeps its scope for its path request
MAX_TRACKED = 4096  # destinations each table (requests sent, paths lacked, scopes) remembers at most
PATH_REQUEST_TIMEOUT = 15  # RNS.Transport.PATH_REQUEST_TIMEOUT: RNS's own wait for a path, the shortest candidate_wait
MIN_SAMPLES = 20  # path-response times measured before candidate_wait shortens anything
MAX_SAMPLES = 100  # the most recent path-response times kept
PERCENTILE = 0.95  # candidate_wait waits as long as this share of the path responses measured took


class _Turn:
    """A turn reserved for a path request, until it is taken or given up (PathRequests._turns)."""

    __slots__ = ("at", "given_up")

    def __init__(self, at: float):
        """A turn at at, still wanted."""
        self.at = at
        self.given_up = False


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


def _sendable(interface, transport) -> bool:
    """True if a path request can go out on interface itself: online, outgoing, not detached, not local (to or of a
    shared instance, or a local client's) and not a parent of spawned interfaces (whose own sending does nothing)."""
    return (bool(getattr(interface, "OUT", False)) and bool(getattr(interface, "online", False))
            and not getattr(interface, "detached", False) and not getattr(interface, "spawned_interfaces", None)
            and type(interface).__name__ not in LOCAL_INTERFACE_TYPES
            and not any(interface is local for local in (getattr(transport, "local_client_interfaces", None) or ())))


def _fast(interface, slow_bitrate: float) -> bool:
    """True if interface's bitrate is known and at least slow_bitrate (as rnfs.network.rates judges interfaces)."""
    bitrate = getattr(interface, "bitrate", None)
    return (isinstance(bitrate, (int, float)) and not isinstance(bitrate, bool) and math.isfinite(bitrate)
            and bitrate >= slow_bitrate)


class PathRequests:
    """A node's path requests under one budget (see the module docstring). Thread-safe.

    transport() and reticulum() give RNS.Transport and the running Reticulum instance (callers whose RNS tests replace
    pass their own); clock and sleep are monotonic time and waiting, replaced by tests."""

    def __init__(self, transport: Callable[[], object] = _rns_transport,
                 reticulum: Callable[[], object] = _rns_instance, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], object] = time.sleep, spacing: float = SPACING,
                 slow_bitrate: float | None = None):
        """No request sent yet: the first takes its turn at once. slow_bitrate (bits/s, the node's): interfaces below it
        are slow, and only a request named over a fast interface is scoped (None: nothing is)."""
        self._transport = transport
        self.slow_bitrate = slow_bitrate
        self._reticulum = reticulum
        self.clock = clock
        self.sleep = sleep
        self.spacing = spacing
        # How many times longer the spacing is now: 1 but on a slow node, whose radio's channel load stretches it
        # (rnfs.network.channel.ChannelLoad.stretch, set by the node; it reads the radios at most every 15 s)
        self.stretch: Callable[[], float] = lambda: 1.0
        self._lock = threading.Lock()
        self._next_turn = -math.inf  # the earliest time the next turn may be
        self._last_went = -math.inf  # when the last turn was actually taken (a request went)
        self._turns: list[_Turn] = []  # turns reserved and not yet taken, in order (_take_turn, _give_back)
        self._kept_until = -math.inf  # _next_turn never goes back before this: the end of the last turn charged
        self._waits: dict[bytes, threading.Event] = {}  # destination -> set when its owner's wait ends
        self._sent: OrderedDict[bytes, float] = OrderedDict()  # destination -> when last requested, oldest first
        self._lacked: OrderedDict[bytes, None] = OrderedDict()  # destinations whose last wait found no path
        self._named: OrderedDict[bytes, tuple[bytes, float]] = OrderedDict()  # node -> (who named it, when)
        self._failure_log = ThrottledLog()
        self._responses: deque[float] = deque(maxlen=MAX_SAMPLES)  # seconds from a request to its answer
        # Counts, for tests and measurements
        self.requests = 0  # path requests sent (one scoped to several interfaces counts once)
        self.charged = 0  # links that failed to establish, each a request RNS sends by itself
        self.joined = 0  # waits that joined another caller's for the same destination
        self.reused = 0  # waits within PATH_REQUEST_MI of the destination's last request, which sent nothing
        self.held = 0  # requests the ceiling held back at least once
        self.scoped = 0  # requests sent on only some interfaces
        self.waited = 0.0  # seconds spent waiting for turns, in all

    # Waiting for a path or an identity

    def await_path(self, destination: bytes, timeout: float, cancel: threading.Event | None = None,
                   measure: bool = True) -> bool:
        """Wait for a path to destination, requesting one in its turn if RNS has none: True once there is one. timeout
        (path_timeout) counts from when the request is sent; cancel, once set, ends a wait for a turn or another
        caller's request (False). measure: the answer's time counts towards candidate_wait (not for a shortened
        wait)."""
        transport = self._transport()
        if transport.has_path(destination):
            self._settle(destination, True)
            return True
        found = self._wait(destination, transport, lambda: bool(transport.has_path(destination)), timeout, cancel,
                           rns_wait=True, measure=measure)
        self._settle(destination, found)
        return found

    def await_known(self, destination: bytes, known: Callable[[], bool], timeout: float,
                    cancel: threading.Event | None = None, measure: bool = True) -> bool:
        """Request a path to destination in its turn (its answer carries the identity) and wait until known() (polled
        every POLL) holds: True if it did within timeout of the request. For a node RNS has a path to but no identity
        for; as await_path otherwise."""
        found = self._wait(destination, self._transport(), known, timeout, cancel, rns_wait=False, measure=measure)
        self._settle(destination, found)
        return found

    def candidate_wait(self, path_timeout: float) -> float:
        """How long a lookup's path check on a node that isn't slow waits for a path: path_timeout until MIN_SAMPLES
        path-response times are measured, then the PERCENTILE of the last MAX_SAMPLES, within PATH_REQUEST_TIMEOUT and
        path_timeout."""
        with self._lock:
            samples = sorted(self._responses)
        if len(samples) < MIN_SAMPLES:
            return path_timeout
        measured = samples[min(len(samples) - 1, math.ceil(PERCENTILE * len(samples)) - 1)]
        return min(path_timeout, max(PATH_REQUEST_TIMEOUT, measured))

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
        spacing = self._spacing()
        with self._lock:
            now = self.clock()
            self._next_turn = self._kept_until = max(now, self._next_turn) + spacing
            self._remember_sent(destination, now)
            self.charged += 1

    def named(self, node_id: bytes, by: bytes):
        """node_id, whose identity isn't known, was named in by's /find reply: its next path request, if sent within
        HINT_LIFE, goes only where the path to by goes (see the module docstring)."""
        with self._lock:
            self._named[node_id] = (by, self.clock())
            self._named.move_to_end(node_id)
            while len(self._named) > MAX_TRACKED:
                self._named.popitem(last=False)

    def lacked(self, destination: bytes) -> bool:
        """True if the last wait for destination's path (or identity) ended without it."""
        with self._lock:
            return destination in self._lacked

    # Internals

    def _wait(self, destination: bytes, transport, ready: Callable[[], bool], timeout: float,
              cancel: threading.Event | None, rns_wait: bool, measure: bool = True) -> bool:
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
            return self._own_wait(destination, transport, ready, timeout, cancel, rns_wait, measure)
        finally:
            with self._lock:
                self._waits.pop(destination, None)
            event.set()

    def _own_wait(self, destination: bytes, transport, ready: Callable[[], bool], timeout: float,
                  cancel: threading.Event | None, rns_wait: bool, measure: bool) -> bool:
        """Request destination's path in its turn (unless one was requested within PATH_REQUEST_MI) and wait. The time
        from the request sent to its answer is recorded for candidate_wait if measure."""
        with self._lock:
            last = self._sent.get(destination)
            recent = last is not None and 0 <= self.clock() - last < PATH_REQUEST_MI
            if recent:
                self.reused += 1
        if recent:
            return self._poll(ready, timeout, cancel)  # its answer may still be on its way
        if not self._take_turn(destination, ready, cancel):
            return bool(ready())
        interfaces = self._scope(destination, transport)
        self._note_sent(destination)
        sent_at = self.clock()
        if rns_wait and len(interfaces) <= 1:
            options = {"on_interface": interfaces[0]} if interfaces else {}
            found = bool(transport.await_path(destination, timeout=timeout, **options))
        else:
            if interfaces:
                tag = os.urandom(16)  # one request, as a broadcast is: every copy carries the same tag
                for interface in interfaces:
                    transport.request_path(destination, on_interface=interface, tag=tag)
            else:
                transport.request_path(destination)
            found = self._poll(ready, timeout, cancel)
        if found and measure:
            with self._lock:
                self._responses.append(max(0.0, self.clock() - sent_at))
        return found

    def _take_turn(self, destination: bytes, ready: Callable[[], bool], cancel: threading.Event | None) -> bool:
        """Wait for a turn to request destination's path: True when it may go, False if ready() held meanwhile (no
        request needed) or cancel was set."""
        started = self.clock()
        held_since: float | None = None
        reserved: _Turn | None = None
        went = False
        try:
            while True:
                spacing = self._spacing()  # outside the lock: it may read the interface stats
                with self._lock:
                    if reserved is not None:
                        self._turns.remove(reserved)  # its time has passed: the ceiling held it back
                    turn = max(self.clock(), self._next_turn)
                    self._next_turn = turn + spacing
                    reserved = _Turn(turn)
                    self._turns.append(reserved)
                while (now := self.clock()) < turn:
                    if ready() or (cancel is not None and cancel.is_set()):
                        return False
                    self.sleep(min(POLL, turn - now))
                if ready() or (cancel is not None and cancel.is_set()):
                    return False
                if not self._over_ceiling():
                    went = self._go(spacing, ready, cancel)
                    return went
                if held_since is None:
                    held_since = self.clock()
                    with self._lock:
                        self.held += 1
                elif self.clock() - held_since >= CEILING_HOLD:
                    went = self._go(spacing, ready, cancel)
                    return went
        finally:
            self._give_back(reserved, went)
            waited = self.clock() - started
            with self._lock:
                self.waited += waited
            if waited >= self.spacing:
                log(f"path request for {node_name(destination)} waited {waited:.0f}s for its turn", LOG_DEBUG)

    def _give_back(self, reserved: _Turn | None, went: bool):
        """A turn reserved is over: taken (went), or given up by a waiter leaving before it. While the last turn
        reserved is one given up, _next_turn goes back to its time, so turns nobody will take don't hold later callers
        back (a turn given up between two kept ones stays a gap until those are over)."""
        if reserved is None:
            return
        with self._lock:
            if went:
                self._turns.remove(reserved)
                return
            reserved.given_up = True
            while self._turns and self._turns[-1].given_up:
                self._next_turn = max(self._turns.pop().at, self._kept_until)

    def _go(self, spacing: float, ready: Callable[[], bool], cancel: threading.Event | None) -> bool:
        """Take the turn: at least spacing after the last one taken, whatever the turns reserved say. A thread that
        slept past its turn (a loaded machine oversleeps: by 80 ms on a CI runner) would otherwise go with the next
        one's, two requests at once. True when it may go, False if ready() held or cancel was set meanwhile."""
        while True:
            with self._lock:
                now = self.clock()
                gap = self._last_went + spacing - now
                if gap <= 0:
                    self._last_went = now
                    return True
            if ready() or (cancel is not None and cancel.is_set()):
                return False
            self.sleep(min(POLL, gap))

    def _spacing(self) -> float:
        """The spacing of the next turn: spacing, stretched (at least 1 time) while a slow node's channel is busy."""
        try:
            stretch = float(self.stretch())
        except Exception as e:  # noqa: BLE001 - a broken reading must not stop path requests; logged
            self._failure_log(f"couldn't read the channel load: {e!r}", LOG_WARNING)
            stretch = 1.0
        return self.spacing * (stretch if math.isfinite(stretch) and stretch > 1.0 else 1.0)

    def _over_ceiling(self) -> bool:
        """True if the busiest interface already sends more than CEILING path requests a second (False if the stats
        can't be read)."""
        try:
            reticulum = self._reticulum()
            if reticulum is None:
                return False
            return busiest_pr_frequency(reticulum.get_interface_stats()) > CEILING
        except (OSError, EOFError) as e:
            # The shared instance's RPC refused or cut: its rnsd restarting, most often, which needs no traceback
            self._failure_log(f"couldn't read the interfaces' path request rates from the shared instance: {e!r}",
                              LOG_VERBOSE)
            return False
        except Exception as e:  # noqa: BLE001 - RNS internals: no ceiling then; logged
            self._failure_log(f"couldn't read the interfaces' path request rates: {e!r}", LOG_WARNING)
            return False

    def _scope(self, destination: bytes, transport) -> list:
        """The interfaces destination's path request goes on: [] for every interface (no scope, a shared instance, or
        the scope covering every interface anyway, or the path to the node that named it on a slow interface or one of
        unknown bitrate), else those of the path to the node that named it."""
        with self._lock:
            hint = self._named.pop(destination, None)
        if hint is None or not 0 <= self.clock() - hint[1] <= HINT_LIFE or self.slow_bitrate is None:
            return []
        try:
            reticulum = self._reticulum()
            # Through a shared instance, the instance forwards the request on all its interfaces: nothing to scope
            if reticulum is None or getattr(reticulum, "is_connected_to_shared_instance", True):
                return []
            interface = transport.next_hop_interface(hint[0])
            usable = [candidate for candidate in list(transport.interfaces) if _sendable(candidate, transport)]
            if interface is None or not any(candidate is interface for candidate in usable):
                return []
            if not _fast(interface, self.slow_bitrate):
                return []  # named over the radio: an island server or an internet one, so asked everywhere
            parent = getattr(interface, "parent_interface", None)
            chosen = [interface] if parent is None else [
                candidate for candidate in usable if getattr(candidate, "parent_interface", None) is parent]
        except Exception as e:  # noqa: BLE001 - RNS internals: asked on every interface then; logged
            self._failure_log(f"couldn't read the interface of the path to {node_name(hint[0])}: {e!r}", LOG_WARNING)
            return []
        if len(chosen) == len(usable):
            return []
        with self._lock:
            self.scoped += 1
        return chosen

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

    def _settle(self, destination: bytes, found: bool):
        """Record how a wait for destination ended (lacked)."""
        with self._lock:
            if found:
                self._lacked.pop(destination, None)
                return
            self._lacked[destination] = None
            self._lacked.move_to_end(destination)
            while len(self._lacked) > MAX_TRACKED:
                self._lacked.popitem(last=False)
