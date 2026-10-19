"""Counts of recent events, for the node's status (specification section 11): how often something happened over the
last hour and the last day, in bounded memory. Local only: kept in memory, never sent, and started afresh by a restart.

Events are counted per minute (STEP), and a slot is dropped once it is older than the horizon (a day), so a counter
holds at most 1,440 slots however many events there are, and counting a window sums at most that many numbers.
"""
import threading
import time
from collections import deque
from collections.abc import Callable

from rnfs.constants import DAY, HOUR

STEP = 60  # seconds per slot: windows are counted to the minute
HORIZON = DAY  # the longest window counted; older slots are dropped


class RecentCounts:
    """Events over the last HORIZON seconds, counted per STEP. Thread-safe. clock is monotonic seconds."""

    def __init__(self, clock: Callable[[], float] = time.monotonic, step: float = STEP, horizon: float = HORIZON):
        """A counter with no events yet."""
        self.clock = clock
        self.step = step
        self.horizon = horizon
        self._slots: deque[list[int]] = deque()  # [slot number, events], oldest first, at most horizon / step
        self._lock = threading.Lock()

    def add(self, count: int = 1):
        """count more events now."""
        if count <= 0:
            return
        slot = self._slot()
        with self._lock:
            if self._slots and self._slots[-1][0] >= slot:
                self._slots[-1][1] += count  # this minute's; a clock that went back counts with the latest, in order
            else:
                self._slots.append([slot, count])
                self._drop(slot)

    def count(self, seconds: float = HORIZON) -> int:
        """Events in the last seconds (at most the horizon), to the step: the current slot and the whole slots before
        it that fit."""
        slot = self._slot()
        first = slot - max(1, round(min(seconds, self.horizon) / self.step)) + 1
        with self._lock:
            self._drop(slot)
            total = 0
            for number, events in reversed(self._slots):  # newest first: a short window stops early
                if number < first:
                    break
                total += events
            return total

    def hour_and_day(self) -> tuple[int, int]:
        """Events in the last hour and in the last day."""
        return self.count(HOUR), self.count(DAY)

    def _slot(self) -> int:
        """The current slot's number."""
        return int(self.clock() // self.step)

    def _drop(self, slot: int):
        """Drop the slots older than the horizon, as of slot. Lock held."""
        oldest = slot - round(self.horizon / self.step) + 1
        while self._slots and self._slots[0][0] < oldest:
            self._slots.popleft()
