"""Shared test helpers (not a test module): what the platform running the tests can do, for skipping tests that need
POSIX features, and small doubles and helpers several test modules use."""
import json
import os
import socket
import sqlite3
import tempfile
import time
from contextlib import contextmanager
from unittest import mock

from rnfs import statedb

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


# The state database (rnfs.statedb)


def state_rows(state_dir: str, sql: str, params=()) -> list[tuple]:
    """The rows a read of the state database in state_dir gives, through a connection of its own (closed again),
    so it sees only what was committed: for tests checking what a store saved."""
    conn = sqlite3.connect(os.path.join(state_dir, statedb.DB_FILE))
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def state_execute(state_dir: str, sql: str, params=()):
    """Run one write on the state database in state_dir and commit it, through a connection of its own: for tests
    planting rows a store must reject at load."""
    conn = sqlite3.connect(os.path.join(state_dir, statedb.DB_FILE))
    try:
        with conn:
            conn.execute(sql, params)
    finally:
        conn.close()


def write_legacy(state_dir: str, name: str, value=None, text: str | None = None) -> str:
    """Write a JSON state file as rnfs 0.1.4 did (value as JSON, or text as is) into state_dir; its path."""
    os.makedirs(state_dir, exist_ok=True)
    path = os.path.join(state_dir, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text if text is not None else json.dumps(value))
    return path


@contextmanager
def failing_commits(error: str = "disk I/O error"):
    """Within the with block, every state database commit fails as a full or failing disk makes it fail."""
    with mock.patch.object(statedb._Database, "_commit", side_effect=sqlite3.OperationalError(error)):
        yield
