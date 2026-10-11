"""Logging through RNS's logger, so rnfs messages share its output, timestamps and loglevel."""
import RNS

from rnfs.fsutil import printable

# Warnings a result keeps (a write-out skipping 50,000 entries mustn't make a 16 MB response or jobs.json)
MAX_WARNINGS = 100

# Re-exported so callers only need this module for logging
from RNS import LOG_ERROR, LOG_INFO, LOG_NOTICE, LOG_VERBOSE, LOG_WARNING  # noqa: F401


def log(message: str, level: int = LOG_INFO):
    """Log message with an "rnfs" prefix at the given RNS loglevel, non-printable characters escaped."""
    # Messages carry remote-chosen names (DAG entries, LXMF links, request paths): escaping here, once,
    # keeps escape sequences and fake newlines out of every log line without each caller remembering
    RNS.log(f"rnfs {printable(message)}", level)


def node_name(node_id: bytes | None) -> str:
    """Printable form of a node ID, e.g. "<9e0a158a...>", or a placeholder for an unknown node."""
    return RNS.prettyhexrep(node_id) if node_id is not None else "an unidentified node"


def plural(count: int, noun: str) -> str:
    """count with noun, pluralised unless count is 1: "1 node", "3 nodes"."""
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def size(num_bytes: float) -> str:
    """Human-readable size, e.g. "3.00 MB"."""
    return RNS.prettysize(num_bytes)


def cap_warnings(warnings: list) -> dict:
    """{"warnings": the first MAX_WARNINGS, and if there were more, "warnings_total": how many}."""
    if len(warnings) <= MAX_WARNINGS:
        return {"warnings": list(warnings)}
    rest = len(warnings) - MAX_WARNINGS
    return {"warnings": [*warnings[:MAX_WARNINGS], f"... and {plural(rest, 'more warning')}"],
            "warnings_total": len(warnings)}
