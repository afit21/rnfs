"""Shared test helpers (not a test module): what the platform running the tests can do, for skipping tests that need
POSIX features, and small doubles and helpers several test modules use."""
import os
import socket
import tempfile
import time

# Unix permission bits (modes, umask, the execute bit): Windows has none, and its st_mode is synthesised
POSIX_MODES = os.name != "nt"
HAS_FIFO = hasattr(os, "mkfifo")


def _can_symlink() -> bool:
    """True if this process may create symlinks (on Windows that takes Developer Mode or an admin)."""
    with tempfile.TemporaryDirectory() as directory:
        target = os.path.join(directory, "target")
        open(target, "wb").close()
        try:
            os.symlink(target, os.path.join(directory, "link"))
        except (OSError, NotImplementedError):
            return False
    return True


CAN_SYMLINK = _can_symlink()


def real(path: str) -> str:
    """path in one canonical form, for comparing paths the CLI resolved with paths a test built.

    macOS reaches the temp directory through a symlink (/var is /private/var), and Windows may name it by its
    8.3 short name (RUNNER~1), so the same directory can be spelled differently by the CLI and the test.
    """
    return os.path.normcase(os.path.realpath(path))


class FakeClock:
    """A settable clock tests move by hand (clock.now = ...), used for wall or monotonic time."""

    def __init__(self, now: float = 1000.0):
        """Start at now."""
        self.now = now

    def __call__(self) -> float:
        """The current time."""
        return self.now


def free_port() -> int:
    """A local TCP port that is free now (nothing is listening on it)."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(condition, timeout: float = 10.0, what: str | None = None, interval: float = 0.02):
    """Poll condition until it returns something truthy and return that.

    After timeout seconds, fail with an AssertionError naming what if it's given; otherwise return condition's
    last (falsy) value, for self.assertTrue(wait_for(...), message).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(interval)
    result = condition()
    if not result and what is not None:
        raise AssertionError(f"timed out waiting for {what}")
    return result
