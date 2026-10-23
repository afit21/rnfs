"""How the daemon was started, and restarting it in place (POST /api/restart; rnfs 0.2.1). No RNS.

daemon.json says what started the daemon (started_by), so an app needn't ask systemctl or the Task Scheduler:
- "systemd": it runs in a systemd service (its cgroup is a .service unit; INVOCATION_ID alone isn't enough, as a
  terminal's shell may inherit it from the terminal's own service), "service" naming the unit;
- "launchd": on macOS, launchd is its parent (a LaunchAgent such as contrib/io.github.afit21.rnfs.plist);
- "windows_task": contrib/install-windows-task.ps1's wrapper, which sets RNFS_STARTED_BY to say so;
- whatever RNFS_STARTED_BY says otherwise (an app starting its own daemon, "app:<name>"), at most MAX_STARTED_BY
  printable characters;
- "terminal" if its standard input is a terminal, else "unknown".
"sandbox" is "flatpak" inside a Flatpak (FLATPAK_ID), else None.

Restarting (can_restart): on POSIX the daemon stops as SIGTERM stops it, then executes its own command line again
(restart_argv), keeping its process ID, so a service manager sees no exit and a terminal keeps it in the
foreground; a newer rnfs installed meanwhile is what starts. Windows has no such exec: there only the task wrapper's
daemon can restart, by exiting with EXIT_RESTART, which the wrapper restarts as any failed exit.
"""
import os
import sys

STARTED_BY_VARIABLE = "RNFS_STARTED_BY"  # what an app or wrapper starting the daemon sets to say so
MAX_STARTED_BY = 64
WINDOWS_TASK = "windows_task"
EXIT_RESTART = 75  # EX_TEMPFAIL: the task wrapper restarts a daemon that exits with it (as with any failure)


def _systemd_unit(cgroup_text: str | None) -> str | None:
    """The systemd service unit a /proc/self/cgroup names (its unified hierarchy's line), or None."""
    for line in (cgroup_text or "").splitlines():
        hierarchy, _, path = line.partition("::")
        if hierarchy == "0" and path:
            unit = path.rstrip("/").rpartition("/")[2]
            return unit if unit.endswith(".service") else None
    return None


def _read_cgroup() -> str | None:
    """/proc/self/cgroup, or None where there is none (not Linux)."""
    try:
        with open("/proc/self/cgroup", encoding="utf-8", errors="replace") as f:
            return f.read(64 * 1024)
    except OSError:
        return None


def how_started(environ=None, platform: str | None = None, parent: int | None = None,
                cgroup_text: str | None = None, interactive: bool | None = None) -> dict:
    """{"started_by", "service", "sandbox", "can_restart"} for daemon.json (the module docstring). Each argument
    defaults to this process's own (os.environ, sys.platform, its parent's PID, /proc/self/cgroup, whether stdin
    is a terminal); tests pass them."""
    environ = os.environ if environ is None else environ
    platform = sys.platform if platform is None else platform
    windows = platform.startswith("win")
    service = None
    given = environ.get(STARTED_BY_VARIABLE, "").strip()
    if given and len(given) <= MAX_STARTED_BY and given.isprintable():
        started_by = given
    elif platform.startswith("linux") and environ.get("INVOCATION_ID") \
            and (service := _systemd_unit(_read_cgroup() if cgroup_text is None else cgroup_text)) is not None:
        started_by = "systemd"
    elif platform == "darwin" and (os.getppid() if parent is None else parent) == 1:
        started_by = "launchd"
        service = environ.get("XPC_SERVICE_NAME") or None
    else:
        if interactive is None:
            try:
                interactive = sys.stdin is not None and sys.stdin.isatty()
            except (OSError, ValueError):
                interactive = False
        started_by = "terminal" if interactive else "unknown"
    return {"started_by": started_by, "service": service,
            "sandbox": "flatpak" if environ.get("FLATPAK_ID") else None,
            "can_restart": started_by == WINDOWS_TASK if windows else True}


def restart_argv() -> tuple[str, list[str]]:
    """(the program, its arguments) that start this process again as it was started: a frozen build's own
    executable and argv; else the interpreter with the command line it was given (sys.orig_argv: `python -m rnfs
    daemon`, or a launcher script's)."""
    if getattr(sys, "frozen", False):
        return sys.executable, list(sys.argv)
    return sys.executable, list(getattr(sys, "orig_argv", None) or [sys.executable, *sys.argv])
