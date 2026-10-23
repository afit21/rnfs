"""The debug log file (setting `debug_log`, or `daemon --debug-log`): <rnfs dir>/debug.log, for diagnosing a daemon.

- It begins with the daemon's version, then the Python and RNS versions, the platform, and the settings
  that differ from the defaults. Each start moves the previous run's file to debug.log.1; past MAX_SIZE the file
  is moved there too and a new one begun, also with the version first, so the two hold at most 2 × MAX_SIZE.
- Every rnfs message at every level, whatever verbosity the console shows (rnfs.log hands each one here), with
  the thread that logged it, and, for a warning or error logged while handling an exception, its traceback.
- Reticulum's own messages at Info and more severe (RNS_LEVEL) that its loglevel lets through: interfaces
  failing to connect, sockets dropping and reconnecting. Its Verbose and Debug detail (handshakes, ratchets,
  table cleaning, other nodes' announces) isn't copied, and rnfs logs what matters of links itself. The console
  still gets all of RNS's output as before. Also threads that die of an exception, and a status line every
  STATUS_INTERVAL: the node's counts, its open links and each interface's state.
- Gateway requests (rnfs/gateway.py) are numbered, and the lines logged on a request's thread carry its number.

The file names what the node looked up, fetched and served, so it is created readable by its owner only (on
Windows, the rnfs dir's access list) and nothing in it is sent anywhere. Every line has non-printable characters
escaped, as the console's do. A file that can't be written stops the debug log, with one message on stderr.
"""
import dataclasses
import os
import platform
import re
import sys
import threading
import time
import traceback
from contextlib import contextmanager

from rnfs import __version__, fsutil, winacl
from rnfs import log as rnfs_log
from rnfs.fsutil import printable

DEBUG_LOG_FILE = "debug.log"
MAX_SIZE = 16 * 1024 * 1024  # bytes per file; the one before is kept as debug.log.1
STATUS_INTERVAL = 300.0  # seconds between status lines
RNS_LEVEL = 4  # RNS.LOG_INFO: Reticulum's own messages this severe or more are copied
# RNS's loglevels by number, named as its log lines name them
LEVEL_NAMES = {0: "Critical", 1: "Error", 2: "Warning", 3: "Notice", 4: "Info", 5: "Verbose", 6: "Debug",
               7: "Pathing", 8: "Extra"}
# RNS's own log line: "[time] [Level] message" (its compact format has neither, and is copied as is)
RNS_LINE = re.compile(r"\[[^\]]*\] \[(\w+)\]\s*(.*)", re.DOTALL)
LEVEL_NUMBERS = {name: number for number, name in LEVEL_NAMES.items()}

_active: "DebugLog | None" = None
_previous_hook = None  # threading.excepthook before start()


def current() -> "DebugLog | None":
    """The debug log, if the daemon runs with one."""
    return _active


def path_in(rnfs_root: str) -> str:
    """Where the debug log of the rnfs dir rnfs_root is."""
    return os.path.join(rnfs_root, DEBUG_LOG_FILE)


class DebugLog:
    """The open file: whole lines, written as they come from any thread, each flushed at once."""

    def __init__(self, path: str, settings: dict, max_size: int = MAX_SIZE):
        """Move any earlier file at path aside and start one with the header. Raises OSError."""
        self.path = path
        self.settings = settings
        self.max_size = max_size
        self._lock = threading.Lock()
        self._local = threading.local()  # .tag: what this thread's lines are about (a gateway request)
        self._fd: int | None = None
        self._size = 0
        self._broken = False
        with self._lock:
            self._begin(continued=False)

    def _begin(self, continued: bool):
        """Move the current file to path.1 and start a new one, version first (lock held). Raises OSError."""
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        if os.path.lexists(self.path):
            fsutil.replace(self.path, self.path + ".1")
        # O_EXCL and O_NOFOLLOW: never append to a file or symlink put in its place
        self._fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | fsutil.O_NOFOLLOW | fsutil.O_BINARY,
                           0o600)
        winacl.make_private(self.path)  # Windows ignores the mode; elsewhere this does nothing
        self._size = 0
        now = time.strftime("%Y-%m-%d %H:%M:%S %z")
        for line in (f"rnfs {__version__} debug log, " + (f"continued {now} (earlier lines: {DEBUG_LOG_FILE}.1)"
                                                         if continued else f"started {now} (pid {os.getpid()})"),
                     f"Python {platform.python_version()} on {platform.platform()}; RNS {_package_version('rns')}",
                     f"settings other than the defaults: {self.settings}"):
            self._put(printable(line) + "\n")

    def _put(self, text: str):
        """Write text, already escaped (lock held). Raises OSError."""
        data = text.encode("utf-8", "backslashreplace")
        os.write(self._fd, data)
        self._size += len(data)

    def write(self, level: int, source: str, text: str, trace: str | None = None):
        """One line, from source ("rnfs", "RNS", "status"…) at level, with a traceback below it if given."""
        stamp = time.time()
        when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stamp)) + f".{int(stamp % 1 * 1000):03d}"
        where = getattr(self._local, "tag", None) or threading.current_thread().name
        line = f"[{when}] [{LEVEL_NAMES.get(level, level)}] [{printable(where)}] {source}: {printable(text)}\n"
        if trace:
            line += "".join(f"    | {printable(part)}\n" for part in trace.rstrip("\n").split("\n"))
        with self._lock:
            if self._broken:
                return
            try:
                if self._size + len(line) > self.max_size:
                    self._begin(continued=True)
                self._put(line)
            except OSError as e:
                self._broken = True  # not logged: that would come back here
                print(f"rnfs: the debug log {self.path} stopped: {e}", file=sys.stderr, flush=True)

    def note(self, text: str, level: int = rnfs_log.LOG_DEBUG):
        """A line for the debug log only (not the console), from rnfs."""
        self.write(level, "rnfs", text)

    @contextmanager
    def tag(self, label: str):
        """Mark the lines this thread writes in the with block as about label (e.g. "gateway #12")."""
        previous = getattr(self._local, "tag", None)
        self._local.tag = label
        try:
            yield
        finally:
            self._local.tag = previous

    def close(self):
        """Close the file; later lines are dropped."""
        with self._lock:
            self._broken = True
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None


def _package_version(name: str) -> str | None:
    """The installed version of a distribution, or None."""
    try:
        from importlib.metadata import version
        return version(name)
    except Exception:  # noqa: BLE001 - not installed, or a bundle without metadata
        return None


def _rnfs_message(level: int, text: str):
    """rnfs.log's sink: every rnfs message; a warning or error with the traceback of the exception being handled,
    if any (others logged in an except block are expected outcomes, such as a gateway refusal)."""
    debug = _active
    if debug is None:
        return
    trace = traceback.format_exc() if level <= rnfs_log.LOG_WARNING and sys.exc_info()[0] is not None else None
    debug.write(level, "rnfs", text, trace)


def _rns_output(logstring: str):
    """RNS's log output (LOG_CALLBACK): printed as RNS prints it, and copied here unless it's an rnfs message."""
    if threading.main_thread().is_alive():  # as RNS's own console output does
        try:
            print(logstring)
        except Exception:  # noqa: BLE001 - as RNS: a closed stdout mustn't break logging
            pass
    debug = _active
    if debug is None or rnfs_log.passing_through():
        return
    match = RNS_LINE.fullmatch(logstring)
    if match and match.group(1) in LEVEL_NUMBERS:
        level = LEVEL_NUMBERS[match.group(1)]
        if level <= RNS_LEVEL:
            debug.write(level, "RNS", match.group(2))
    else:
        debug.write(rnfs_log.LOG_INFO, "RNS", logstring)  # a format without levels: kept, as it can't be told


def start(config) -> DebugLog:
    """Open the debug log of config's rnfs dir and send it everything described above. Raises OSError."""
    global _active, _previous_hook
    import RNS
    defaults = type(config)()
    settings = {field.name: getattr(config, field.name) for field in dataclasses.fields(config)
                if getattr(config, field.name) != getattr(defaults, field.name)}
    debug = DebugLog(path_in(config.root), settings)
    _active = debug
    rnfs_log.set_debug_sink(_rnfs_message)
    if RNS.logdest == RNS.LOG_STDOUT:  # the daemon's own; a file or callback someone set up is left alone
        RNS.logcall = _rns_output
        RNS.logdest = RNS.LOG_CALLBACK
    previous_hook = _previous_hook = threading.excepthook

    def thread_died(args):
        """Log a thread's uncaught exception, then report it as before."""
        if args.exc_type is not SystemExit:
            name = args.thread.name if args.thread is not None else "a thread"
            trace = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
            debug.write(rnfs_log.LOG_ERROR, "rnfs", f"{name} died of {args.exc_type.__name__}", trace)
        previous_hook(args)

    threading.excepthook = thread_died
    return debug


def stop():
    """Stop sending anything to the debug log and close it (RNS's output goes back to the console alone)."""
    global _active, _previous_hook
    import RNS
    debug = _active
    if debug is None:
        return
    _active = None
    rnfs_log.set_debug_sink(None)
    if _previous_hook is not None:
        threading.excepthook, _previous_hook = _previous_hook, None
    if RNS.logdest == RNS.LOG_CALLBACK and RNS.logcall is _rns_output:
        RNS.logdest = RNS.LOG_STDOUT
    debug.close()


def status_line(node) -> str:
    """The node's status (as `rnfs status` gives it), its open links, and each RNS interface's state."""
    import RNS
    interfaces = []
    for interface in list(getattr(RNS.Transport, "interfaces", [])):
        online = getattr(interface, "online", None)
        interfaces.append(f"{interface} {'up' if online else 'down' if online is not None else '?'}"
                          f" rx {getattr(interface, 'rxb', 0)} B tx {getattr(interface, 'txb', 0)} B")
    links = getattr(node, "links", None)
    open_links = len(links.open_nodes()) if links is not None else 0
    return f"status {node.status()}; {open_links} open links; interfaces: " + ("; ".join(interfaces) or "none")
