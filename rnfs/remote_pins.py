"""Remote pin status (specification section 8, "Remote pinning"; rnfs 0.2.0): what this node records about the remote
pins it asked other nodes for (AskedPins) and about those other identities asked of it (ServedPins). Local only:
/pin is unchanged, and nothing here is sent anywhere.

Asked. Each /pin this node sends is recorded by (node, CID): whether it asked to pin or to release, when, and
whether the node accepted. Whether an accepted pin is done is worked out here, by looking up the CID's providers
and finding the node among them (the node's `providing`, given at construction): the requester learns of a remote
pin only that way (section 8), so "done" can lag the node's publishing and is only as good as the DHT. A check that
doesn't find the node within GRACE of asking leaves the pin waiting (it may still be fetching it); one after that
shows it missing (dropped, over quota or not found, or never fetched). Checks run in the background, paced
(CHECKS_PER_HOUR, CHECKS_AT_ONCE: on slow nodes a few an hour, one at a time), each pin at most every
WAITING_RECHECK or SETTLED_RECHECK, and on demand (check_now: at most ON_DEMAND_CHECKS a call, none of a pin checked
in the last FRESH seconds, sharing the same CHECKS_AT_ONCE). At most MAX_ASKED entries are kept: past it the oldest
released or refused entries go first, then the oldest done, then the oldest of the rest. A release is kept as an
entry of its own state ("released"), never checked.

Served. The pins other identities asked of this node live in storage (pinned, source remote:<identity>) and in the
node's queue (waiting, fetching); what became of each one that isn't pinned (dropped and why, refused, released)
was only logged before 0.2.0. ServedPins keeps the latest outcome of each (identity, CID), at most MAX_SERVED of
them: past it the oldest outcome that ended goes first, then the oldest pinned one (storage still knows the pin);
pins waiting or fetching are never forgotten (they are bounded by the node's queue).

Both are saved to the state database (rnfs.statedb: remote_pins_asked and remote_pins_served), not durably and in
snapshots, like the remote pins waiting: a crash may lose the last changes, never part of one.
"""
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from typing import Callable

from rnfs import statedb
from rnfs.content.cid import Cid
from rnfs.log import LOG_ERROR, LOG_WARNING, log, plural

ASKED_TABLE, SERVED_TABLE = "remote_pins_asked", "remote_pins_served"
MAX_ASKED = 10_000  # asked entries kept; past this the oldest released, refused or done go first
MAX_SERVED = 1_000  # served outcomes kept; past this the oldest that ended go first
MAX_DETAIL = 400  # characters of an outcome's detail (an error message) kept

# Asked states. waiting: accepted, not yet found providing (or not within GRACE); done: the last check found the node
# providing; missing: a check GRACE or more after asking didn't; refused: not accepted (refused, or no answer);
# released: a release the node accepted
WAITING, DONE, MISSING, REFUSED, RELEASED = "waiting", "done", "missing", "refused", "released"
ASKED_STATES = (WAITING, DONE, MISSING, REFUSED, RELEASED)
# Served states (RELEASED as above: released by its identity), and the reasons a pin was dropped or refused
FETCHING, PINNED, DROPPED = "fetching", "pinned", "dropped"
SERVED_STATES = (WAITING, FETCHING, PINNED, DROPPED, REFUSED, RELEASED)
REASONS = ("quota", "not_found", "denied", "invalid_dag", "too_large", "storage_full", "failed", "queue_full",
           "identity_removed", "lost")

# A remote node's republish interval by default (section 12): a check this long after asking that doesn't find the
# node shows the pin missing. A big pin on a slow link may take longer; a later check finding it shows it done
GRACE = 24 * 3600.0
# Pacing, by whether this node is slow: (fast, slow)
CHECKS_PER_HOUR = (60, 4)  # background checks started in any hour, at most
CHECKS_AT_ONCE = (4, 1)  # lookups running at once, background and on demand together
ON_DEMAND_CHECKS = (32, 4)  # pins one check_now() checks, at most (the least recently checked first)
FRESH = (60.0, 600.0)  # check_now() skips a pin checked this recently
FIRST_CHECK = (600.0, 3600.0)  # seconds after asking before the first background check
WAITING_RECHECK = (3600.0, 6 * 3600.0)  # between background checks of a waiting pin
SETTLED_RECHECK = (24 * 3600.0, 3 * 24 * 3600.0)  # between background checks of a done or missing one
LOOP_INTERVAL = 30.0  # seconds between the background checker's passes


@dataclass
class _Asked:
    """One asked entry (see AskedPins)."""

    pin: bool  # asked to pin (False: to release)
    accepted: bool
    asked: float  # Unix time of the latest ask
    tried: float | None = None  # the last check started (any outcome), for pacing
    checked: float | None = None  # the last conclusive check
    found: bool | None = None  # what it found (None: never checked conclusively)
    seen: float | None = None  # the last time a check found the node providing

    def state(self) -> str:
        """The entry's state (ASKED_STATES)."""
        if not self.accepted:
            return REFUSED
        if not self.pin:
            return RELEASED
        if self.found:
            return DONE
        if self.found is False and self.checked is not None and self.checked - self.asked >= GRACE:
            return MISSING
        return WAITING

    def checkable(self) -> bool:
        """Whether checks apply: an accepted pin."""
        return self.pin and self.accepted

    def due_at(self, slow: bool) -> float:
        """When the next background check of this entry is due (Unix time); only for checkable entries."""
        if self.tried is None:
            return self.asked + FIRST_CHECK[slow]
        state = self.state()
        return self.tried + (WAITING_RECHECK if state == WAITING else SETTLED_RECHECK)[slow]


def _forget_rank(state: str) -> int:
    """Which asked entries go first past MAX_ASKED: released and refused, then done, then the rest."""
    return 0 if state in (RELEASED, REFUSED) else 1 if state == DONE else 2


class AskedPins:
    """The remote pins this node asked for, and whether each is done. Thread-safe.

    providing(node_id, cid) -> True (the node provides cid), False (a lookup completed without finding it) or None
    (inconclusive: no node answered); it may block for a lookup, and should return soon once stopping is set.
    on_change(node_id, cid, state) is told of every new state, outside the lock (the node publishes an event).
    """

    def __init__(self, state_dir: str, providing: Callable[[bytes, Cid], bool | None], slow: bool = False,
                 on_change: Callable[[bytes, Cid, str], None] | None = None, clock: Callable[[], float] = time.time,
                 monotonic: Callable[[], float] = time.monotonic, cap: int = MAX_ASKED):
        """Load the entries saved in state_dir; a row that doesn't parse is logged and deleted."""
        self.providing = providing
        self.slow = bool(slow)
        self.on_change = on_change
        self.clock = clock
        self.monotonic = monotonic
        self.cap = cap
        self._lock = threading.Lock()
        self._entries: OrderedDict[tuple[bytes, Cid], _Asked] = OrderedDict()  # oldest asked first
        self._checking: set[tuple[bytes, Cid]] = set()  # checks running
        self._started: deque[float] = deque()  # monotonic times of the checks started in the last hour
        self._slots = threading.BoundedSemaphore(CHECKS_AT_ONCE[self.slow])  # lookups running at once
        self._on_demand = threading.Lock()  # one check_now at a time
        self._db = statedb.open_store(state_dir, self._load)

    def _load(self, db: statedb.StateDb):
        """Read the remote_pins_asked table, oldest asked first."""
        self._store = statedb.RowStore(db, ASKED_TABLE, ("node", "cid"), durable=False, mode=statedb.SNAPSHOT,
                                       lock=self._lock, row=self._row, key_values=lambda key: (key[0], key[1].binary))
        bad = []
        rows = db.query(f"SELECT node, cid, pin, accepted, asked, tried, checked, found, seen FROM {ASKED_TABLE} "
                        "ORDER BY asked")
        for node, binary, pin, accepted, asked, tried, checked, found, seen in rows:
            try:
                if not isinstance(node, bytes) or len(node) != 16:
                    raise ValueError("not a node ID")
                if not isinstance(asked, (int, float)):
                    raise TypeError("no time asked")
                for value in (tried, checked, seen):
                    if value is not None and not isinstance(value, (int, float)):
                        raise TypeError("not a time")
                entry = _Asked(bool(pin), bool(accepted), float(asked), tried, checked,
                               None if found is None else bool(found), seen)
                self._entries[(node, Cid.from_bytes(binary))] = entry
            except (ValueError, TypeError):
                bad.append((node, binary))
        if bad:
            log(f"remote pins: ignoring {plural(len(bad), 'invalid asked pin')}",
                LOG_WARNING)
        statedb.delete_rows(db, ASKED_TABLE, ("node", "cid"), bad)

    def _row(self, key: tuple[bytes, Cid]) -> tuple | None:
        """key's row, or None once forgotten (lock held)."""
        entry = self._entries.get(key)
        if entry is None:
            return None
        return (key[0], key[1].binary, int(entry.pin), int(entry.accepted), entry.asked, entry.tried, entry.checked,
                None if entry.found is None else int(entry.found), entry.seen)

    def record(self, node_id: bytes, cid: Cid, pin: bool, accepted: bool):
        """Record that this node asked node_id to pin (or release) cid, and whether it accepted: a new entry for
        (node_id, cid), replacing any earlier one. Saved by the next save()."""
        key = (bytes(node_id), cid)
        with self._lock:
            self._entries.pop(key, None)
            self._entries[key] = entry = _Asked(bool(pin), bool(accepted), self.clock())
            self._store.changed(key)
            forgotten = self._cap(key)
        if forgotten:
            log(f"remote pins: forgot {plural(forgotten, 'asked pin')} past {self.cap}")
        self._tell(key, entry.state())

    def _cap(self, keep: tuple[bytes, Cid]) -> int:
        """Forget entries past the cap, never keep (just recorded): released and refused first, then done, then the
        rest, each oldest asked first (lock held). Returns how many."""
        excess = len(self._entries) - self.cap
        if excess <= 0:
            return 0
        candidates = sorted((_forget_rank(entry.state()), entry.asked, n)
                            for n, (key, entry) in enumerate(self._entries.items()) if key != keep)
        keys = list(self._entries)
        gone = [keys[n] for _, _, n in candidates[:excess]]
        for key in gone:
            del self._entries[key]
        self._store.changed(*gone)
        return len(gone)

    def _tell(self, key: tuple[bytes, Cid], state: str):
        """Tell on_change of key's state (outside the lock); a failing listener is logged, never raised."""
        if self.on_change is None:
            return
        try:
            self.on_change(key[0], key[1], state)
        except Exception as e:  # noqa: BLE001 - a listener only publishes an event; the change is made
            log(f"remote pins: a change listener failed: {e!r}", LOG_ERROR)

    def entries(self, node_id: bytes | None = None, cid: Cid | None = None) -> list[dict]:
        """The entries (node_id's, cid's, or every one), most recently asked first: {"node" (hex), "cid", "pin",
        "accepted", "asked", "state", "done", "checked" (the last conclusive check, or None), "seen" (the last time
        one found the node providing, or None)}."""
        with self._lock:
            return [{"node": key[0].hex(), "cid": str(key[1]), "pin": entry.pin, "accepted": entry.accepted,
                     "asked": entry.asked, "state": entry.state(), "done": entry.state() == DONE,
                     "checked": entry.checked, "seen": entry.seen}
                    for key, entry in reversed(self._entries.items())
                    if (node_id is None or key[0] == node_id) and (cid is None or key[1] == cid)]

    def __len__(self) -> int:
        """The number of entries."""
        with self._lock:
            return len(self._entries)

    # Checks

    def _claim(self, keys: list[tuple[bytes, Cid]]) -> list[tuple[bytes, Cid]]:
        """Mark keys as being checked, noting each start for the hourly budget (lock held); the keys."""
        now = self.monotonic()
        for key in keys:
            self._checking.add(key)
            self._started.append(now)
        return keys

    def _budget(self) -> int:
        """Background checks that may start now within CHECKS_PER_HOUR (lock held)."""
        hour_ago = self.monotonic() - 3600.0
        while self._started and self._started[0] <= hour_ago:
            self._started.popleft()
        return max(0, CHECKS_PER_HOUR[self.slow] - len(self._started))

    def due(self) -> list[tuple[bytes, Cid]]:
        """Claim the background checks due now, the most overdue first, within the hourly budget and
        CHECKS_AT_ONCE less the checks running."""
        now = self.clock()
        with self._lock:
            room = min(self._budget(), CHECKS_AT_ONCE[self.slow] - len(self._checking))
            if room <= 0:
                return []
            due = sorted((entry.due_at(self.slow), n, key) for n, (key, entry) in enumerate(self._entries.items())
                         if entry.checkable() and key not in self._checking and entry.due_at(self.slow) <= now)
            return self._claim([key for _, _, key in due[:room]])

    def check_due(self) -> int:
        """One background pass: run the checks due (due()), returning once they end; how many ran."""
        keys = self.due()
        self._run(keys)
        return len(keys)

    def check_now(self, node_id: bytes | None = None, cid: Cid | None = None) -> int:
        """Check the accepted pins (node_id's, cid's, or every one) now, on demand: the least recently checked
        first, at most ON_DEMAND_CHECKS, leaving out those checked in the last FRESH seconds or being checked;
        returns once they end, with how many ran. One call at a time; lookups share CHECKS_AT_ONCE with the
        background checks (on a slow node, one at a time)."""
        with self._on_demand:
            now = self.clock()
            with self._lock:
                fresh = FRESH[self.slow]
                candidates = sorted(
                    (entry.tried if entry.tried is not None else float("-inf"), n, key)
                    for n, (key, entry) in enumerate(self._entries.items())
                    if entry.checkable() and key not in self._checking
                    and (node_id is None or key[0] == node_id) and (cid is None or key[1] == cid)
                    and (entry.tried is None or now - entry.tried >= fresh))
                keys = self._claim([key for _, _, key in candidates[:ON_DEMAND_CHECKS[self.slow]]])
            self._run(keys)
            return len(keys)

    def _run(self, keys: list[tuple[bytes, Cid]]):
        """Check keys (claimed), at most CHECKS_AT_ONCE at a time, returning once every check has ended."""
        if not keys:
            return
        pending = list(keys)
        lock = threading.Lock()

        def work():
            """Check pending keys until none is left."""
            while True:
                with lock:
                    if not pending:
                        return
                    key = pending.pop(0)
                self._check(key)

        workers = min(CHECKS_AT_ONCE[self.slow], len(keys))
        threads = []
        for _ in range(workers - 1):
            thread = threading.Thread(target=work, name="rnfs-remote-pin-check", daemon=True)
            try:
                thread.start()
            except RuntimeError as e:  # can't start a new thread: this one does the work alone
                log(f"remote pins: couldn't start a check thread: {e}", LOG_WARNING)
                break
            threads.append(thread)
        work()
        for thread in threads:
            thread.join()

    def _check(self, key: tuple[bytes, Cid]):
        """Check one claimed key: is its node among the CID's providers? Records the result."""
        try:
            with self._slots:
                with self._lock:
                    entry = self._entries.get(key)
                    if entry is not None:
                        entry.tried = self.clock()
                        self._store.changed(key)
                found = self.providing(key[0], key[1]) if entry is not None else None
        except Exception as e:  # noqa: BLE001 - a check that fails is inconclusive; logged
            log(f"remote pins: checking {key[1]} on {key[0].hex()} failed: {e!r}", LOG_ERROR)
            found = None
        with self._lock:
            self._checking.discard(key)
            entry = self._entries.get(key)
            if entry is None or found is None or not entry.checkable():
                return  # forgotten, asked again as a release meanwhile, or inconclusive: as it was
            before = entry.state()
            entry.checked = self.clock()
            entry.found = bool(found)
            if found:
                entry.seen = entry.checked
            self._store.changed(key)
            state = entry.state()
        if state != before:
            self._tell(key, state)

    def run(self, stop: threading.Event, ready: threading.Event | None = None):
        """The background checker: once ready is set (the node joined), a pass (check_due) every LOOP_INTERVAL
        until stop is set."""
        while ready is not None and not ready.is_set():
            if stop.wait(0.5):
                return
        while not stop.is_set():
            try:
                self.check_due()
            except Exception as e:  # noqa: BLE001 - the checker keeps running; logged
                log(f"remote pins: checking failed: {e!r}", LOG_ERROR)
            stop.wait(LOOP_INTERVAL)

    def save(self):
        """Write the entries changed since the last save. Raises OSError (they are written by the next one)."""
        self._store.save()

    def close(self):
        """Release the state database (once saved, at stop)."""
        self._db.close()


@dataclass(frozen=True)
class Outcome:
    """What became of a remote pin served: its state (SERVED_STATES), why (REASONS) for one dropped or refused,
    when (Unix time), and a detail (an error message) if any."""

    state: str
    reason: str | None
    at: float
    detail: str | None = None


def _ended_rank(state: str) -> int:
    """Which served outcomes go first past MAX_SERVED: those that ended, then pinned ones; never one waiting or
    being fetched (2)."""
    return 2 if state in (WAITING, FETCHING) else 1 if state == PINNED else 0


class ServedPins:
    """The latest outcome of each remote pin another identity asked of this node. Thread-safe.

    on_change(identity, cid, outcome) is told of every outcome recorded, outside the lock."""

    def __init__(self, state_dir: str, on_change: Callable[[bytes, Cid, Outcome], None] | None = None,
                 clock: Callable[[], float] = time.time, cap: int = MAX_SERVED):
        """Load the outcomes saved in state_dir; one being fetched when the node stopped is waiting again, and a row
        that doesn't parse is logged and deleted."""
        self.on_change = on_change
        self.clock = clock
        self.cap = cap
        self._lock = threading.Lock()
        self._outcomes: OrderedDict[tuple[bytes, Cid], Outcome] = OrderedDict()  # oldest first
        self._db = statedb.open_store(state_dir, self._load)

    def _load(self, db: statedb.StateDb):
        """Read the remote_pins_served table, oldest first."""
        self._store = statedb.RowStore(db, SERVED_TABLE, ("identity", "cid"), durable=False, mode=statedb.SNAPSHOT,
                                       lock=self._lock, row=self._row, key_values=lambda key: (key[0], key[1].binary))
        bad = []
        for identity, binary, state, reason, at, detail in db.query(
                f"SELECT identity, cid, state, reason, at, detail FROM {SERVED_TABLE} ORDER BY at"):
            try:
                if not isinstance(identity, bytes) or len(identity) != 16:
                    raise ValueError("not an identity hash")
                if state not in SERVED_STATES or (reason is not None and reason not in REASONS):
                    raise ValueError("unknown state or reason")
                if not isinstance(at, (int, float)):
                    raise TypeError("no time")
                text = None if detail is None else statedb.load(detail)
                if text is not None and not isinstance(text, str):
                    raise TypeError("detail not a string")
                key = (identity, Cid.from_bytes(binary))
            except (ValueError, TypeError):
                bad.append((identity, binary))
                continue
            if state == FETCHING:  # the node stopped while fetching it: resumed (if at all) as waiting
                state = WAITING
                self._store.changed(key)
            self._outcomes[key] = Outcome(state, reason, float(at), text)
        if bad:
            log(f"remote pins: ignoring {plural(len(bad), 'invalid served outcome')}", LOG_WARNING)
        statedb.delete_rows(db, SERVED_TABLE, ("identity", "cid"), bad)

    def _row(self, key: tuple[bytes, Cid]) -> tuple | None:
        """key's row, or None once forgotten (lock held)."""
        outcome = self._outcomes.get(key)
        if outcome is None:
            return None
        detail = None if outcome.detail is None else statedb.dump(outcome.detail)
        return key[0], key[1].binary, outcome.state, outcome.reason, outcome.at, detail

    def record(self, identity: bytes, cid: Cid, state: str, reason: str | None = None, detail: str | None = None):
        """Record what became of identity's pin of cid now (SERVED_STATES, REASONS), replacing its earlier outcome.
        Saved by the next save(). Raises ValueError for an unknown state or reason (a bug)."""
        if state not in SERVED_STATES or (reason is not None and reason not in REASONS):
            raise ValueError(f"unknown remote pin outcome {state!r} {reason!r}")
        key = (bytes(identity), cid)
        outcome = Outcome(state, reason, self.clock(), None if detail is None else detail[:MAX_DETAIL])
        with self._lock:
            self._outcomes.pop(key, None)
            self._outcomes[key] = outcome
            self._store.changed(key)
            self._cap()
        if self.on_change is not None:
            try:
                self.on_change(key[0], cid, outcome)
            except Exception as e:  # noqa: BLE001 - a listener only publishes an event; the outcome is recorded
                log(f"remote pins: a change listener failed: {e!r}", LOG_ERROR)

    def _cap(self):
        """Forget outcomes past the cap: those that ended, then pinned ones, each oldest first; never one waiting
        or being fetched (lock held)."""
        excess = len(self._outcomes) - self.cap
        if excess <= 0:
            return
        gone = []
        for rank in (0, 1):
            for key, outcome in self._outcomes.items():
                if len(gone) >= excess:
                    break
                if _ended_rank(outcome.state) == rank:
                    gone.append(key)
        for key in gone:
            del self._outcomes[key]
        self._store.changed(*gone)

    def outcomes(self) -> dict[tuple[bytes, Cid], Outcome]:
        """Every outcome kept, by (identity, CID), oldest first (a copy)."""
        with self._lock:
            return OrderedDict(self._outcomes)

    def get(self, identity: bytes, cid: Cid) -> Outcome | None:
        """identity's latest outcome for cid, or None."""
        with self._lock:
            return self._outcomes.get((bytes(identity), cid))

    def save(self):
        """Write the outcomes changed since the last save. Raises OSError (they are written by the next one)."""
        self._store.save()

    def close(self):
        """Release the state database (once saved, at stop)."""
        self._db.close()
