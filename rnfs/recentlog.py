"""The daemon's recent log lines, kept in memory for GET /api/log (rnfs 0.2.1), so an app shows the log of a daemon it
didn't start (one a service runs, or another app), with no debug log or journal to read.

LINES holds the last MAX_LINES lines the console shows: rnfs's own messages at Reticulum's loglevel (rnfs.log adds
them) and, once capture() has run (the daemon calls it), Reticulum's own output, its level read from the line as
the debug log reads it. Each line is {"seq" (1, 2, ... in the process's run), "time" (Unix), "level" (RNS's
number), "level_name", "source" ("rnfs" or "RNS"), "text"}, its text escaped as the console's is and cut at
MAX_TEXT characters. Nothing is written anywhere: a restart starts afresh. No RNS at module level.
"""
import threading
import time
from collections import deque

MAX_LINES = 2000
MAX_TEXT = 4096
MAX_LIMIT = 1000  # lines one GET /api/log answers at most


class RecentLines:
    """The last max_lines log lines, numbered. Thread-safe; add() never blocks for long (it may run under any of
    rnfs's locks, as logging does)."""

    def __init__(self, max_lines: int = MAX_LINES, clock=time.time):
        """None yet."""
        self._lines: deque[dict] = deque(maxlen=max_lines)
        self._last = 0
        self._lock = threading.Lock()
        self.clock = clock

    def add(self, level: int, source: str, text: str):
        """Keep a line (text already escaped)."""
        from rnfs.debuglog import LEVEL_NAMES
        with self._lock:
            self._last += 1
            self._lines.append({"seq": self._last, "time": self.clock(), "level": level,
                                "level_name": LEVEL_NAMES.get(level, str(level)), "source": source,
                                "text": text[:MAX_TEXT]})

    def since(self, after: int | None = None, limit: int = MAX_LIMIT, max_level: int | None = None) -> dict:
        """{"lines": those after sequence after (all held if None), the last limit of them, of max_level or more
        severe if given; "next": the sequence to ask after next; "dropped": True if lines after after are no longer
        held (more were logged since than are kept)}."""
        with self._lock:
            held = list(self._lines)
            last = self._last
        first = held[0]["seq"] if held else last + 1
        after = 0 if after is None else after
        lines = [line for line in held if line["seq"] > after
                 and (max_level is None or line["level"] <= max_level)]
        return {"lines": lines[-limit:] if limit else [], "next": last,
                "dropped": after + 1 < first and after < last}


LINES = RecentLines()


def capture():
    """Keep Reticulum's own output in LINES too, still printing it as before: its log callback becomes one that
    calls the callback set up before (the debug log's) or prints, then keeps the line unless it is an rnfs message
    (rnfs.log keeps those itself). Idempotent."""
    import RNS

    from rnfs import debuglog, fsutil
    from rnfs import log as rnfs_log

    if getattr(RNS.logcall, "_rnfs_recent", False) and RNS.logdest == RNS.LOG_CALLBACK:
        return
    before = RNS.logcall if RNS.logdest == RNS.LOG_CALLBACK else None
    if before is None and RNS.logdest != RNS.LOG_STDOUT:
        return  # a file someone set up: left alone

    def output(logstring: str):
        """RNS's log output: passed on as before, then kept."""
        if before is not None:
            before(logstring)
        elif threading.main_thread().is_alive():  # as RNS's own console output does
            try:
                print(logstring)
            except Exception:  # noqa: BLE001 - as RNS: a closed stdout mustn't break logging
                pass
        if rnfs_log.passing_through():
            return
        match = debuglog.RNS_LINE.fullmatch(logstring)
        if match and match.group(1) in debuglog.LEVEL_NUMBERS:
            LINES.add(debuglog.LEVEL_NUMBERS[match.group(1)], "RNS", fsutil.printable(match.group(2)))
        else:
            LINES.add(RNS.LOG_INFO, "RNS", fsutil.printable(logstring))

    output._rnfs_recent = True
    RNS.logcall = output
    RNS.logdest = RNS.LOG_CALLBACK
