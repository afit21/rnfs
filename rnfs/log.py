"""Logging through RNS's logger, so rnfs messages share its output, timestamps and loglevel.

With `debug_log` on, every message also goes to the debug log file (rnfs/debuglog.py), whatever RNS's loglevel.
"""
import threading
import time
from typing import Callable

import RNS

# Re-exported so callers only need this module for logging
from RNS import LOG_DEBUG, LOG_ERROR, LOG_INFO, LOG_NOTICE, LOG_VERBOSE, LOG_WARNING  # noqa: F401

from rnfs.fsutil import printable
from rnfs.units import human_size

# Warnings a result keeps (a write-out skipping 50,000 entries mustn't make a 16 MB response or saved job)
MAX_WARNINGS = 100

# sink(level, text) given every message as well, at any level: the debug log's (set_debug_sink), or None
_debug_sink = None
# Set while an rnfs message is inside RNS.log, so the debug log's copy of RNS's own output can skip it
_passing = threading.local()


def log(message: str, level: int = LOG_INFO):
    """Log message with an "rnfs" prefix at the given RNS loglevel, non-printable characters escaped."""
    # Messages carry remote-chosen names (DAG entries, request paths): escaping here, once,
    # keeps escape sequences and fake newlines out of every log line without each caller remembering
    text = printable(message)
    sink = _debug_sink
    if sink is None:
        RNS.log(f"rnfs {text}", level)
        return
    _passing.active = True
    try:
        RNS.log(f"rnfs {text}", level)
    finally:
        _passing.active = False
    sink(level, text)


def set_debug_sink(sink):
    """Give every message from now on to sink(level, text) too (None: stop)."""
    global _debug_sink
    _debug_sink = sink


def passing_through() -> bool:
    """True while this thread's rnfs message is inside RNS.log (it reaches the debug log by its sink)."""
    return getattr(_passing, "active", False)


class ThrottledLog:
    """Log lines about one kind of event at most one every interval seconds, counting the events left out, so
    whatever causes them (often another node) can't flood the log. Thread-safe.

    Called inside an except block, a line logged at LOG_WARNING or more severe carries the exception's traceback
    to the debug log, like any other.
    """

    def __init__(self, interval: float = 60.0, clock: Callable[[], float] = time.monotonic):
        """Nothing logged yet."""
        self.interval = interval
        self.clock = clock
        self.lock = threading.Lock()
        self.logged_at: float | None = None
        self.skipped = 0

    def due(self) -> int | None:
        """For an event now: how many were left out since the last line, if a line is due (the next is then due
        interval seconds from now); None, counting this one as left out, if not."""
        with self.lock:
            now = self.clock()
            if self.logged_at is not None and now - self.logged_at < self.interval:
                self.skipped += 1
                return None
            skipped, self.skipped, self.logged_at = self.skipped, 0, now
            return skipped

    def __call__(self, message: str, level: int):
        """Log message at level, with how many were left out since the last line, if a line is due."""
        skipped = self.due()
        if skipped is not None:
            log(message + (f" ({skipped} more since the last)" if skipped else ""), level)


class SlowStep:
    """A step of a start that takes minutes on a large node, said in the log while it runs (a context manager): once
    it has taken `after` seconds, what it is doing, then every `interval` seconds that it is still at it, and at the
    end how long it took. A quick one logs nothing. So a start busy with it isn't taken for a hung one, as one silent
    for minutes was (a node with 857k blocks spent 80 s working out what it provides, before its API answered)."""

    def __init__(self, doing: str, done: str, after: float = 10.0, interval: float = 30.0,
                 level: int = LOG_NOTICE, clock: Callable[[], float] = time.monotonic):
        """doing: what the step does, said once it has taken after seconds ("working out what this node
        provides"); done: what it did, said with the time it took ("worked out what this node provides")."""
        self.doing, self.done, self.after, self.interval, self.level, self.clock = \
            doing, done, after, interval, level, clock
        self._finished = threading.Event()
        self._thread: threading.Thread | None = None
        self.said = False  # set once doing was logged (read after the step)
        self._started = 0.0

    def __enter__(self):
        """Start timing the step."""
        self._started = self.clock()
        self._thread = threading.Thread(target=self._watch, name="rnfs-slow-step", daemon=True)
        self._thread.start()
        return self

    def _watch(self):
        """Say what the step does after `after` seconds, and that it still is every interval, until it ends."""
        if self._finished.wait(self.after):
            return
        self.said = True
        log(f"{self.doing} (on a large node this takes a while)", self.level)
        while not self._finished.wait(self.interval):
            log(f"still {self.doing}: {self.clock() - self._started:.0f} s so far", self.level)

    def __exit__(self, *exc):
        """The step ended: stop watching, and say how long it took if it was said to be under way."""
        self._finished.set()
        if self._thread is not None:
            self._thread.join()
        if self.said and exc[0] is None:
            log(f"{self.done} in {self.clock() - self._started:.0f} s", self.level)
        return False


def node_name(node_id: bytes | None) -> str:
    """Printable form of a node ID, e.g. "<9e0a158a...>", or a placeholder for an unknown node."""
    return RNS.prettyhexrep(node_id) if node_id is not None else "an unidentified node"


def plural(count: int, noun: str) -> str:
    """count with noun, pluralised unless count is 1: "1 node", "3 nodes"."""
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def size(num_bytes: float) -> str:
    """Human-readable size, e.g. "3.20 MB" (units.human_size)."""
    return human_size(num_bytes)


def cap_warnings(warnings: list) -> dict:
    """{"warnings": the first MAX_WARNINGS, and if there were more, "warnings_total": how many}."""
    if len(warnings) <= MAX_WARNINGS:
        return {"warnings": list(warnings)}
    rest = len(warnings) - MAX_WARNINGS
    return {"warnings": [*warnings[:MAX_WARNINGS], f"... and {plural(rest, 'more warning')}"],
            "warnings_total": len(warnings)}
