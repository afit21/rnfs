"""Logging through RNS's logger, so rnfs messages share its output, timestamps and loglevel."""
import RNS

# Re-exported so callers only need this module for logging
from RNS import LOG_ERROR, LOG_INFO, LOG_NOTICE, LOG_VERBOSE, LOG_WARNING  # noqa: F401


def log(message: str, level: int = LOG_INFO):
    """Log message with an "rnfs" prefix at the given RNS loglevel."""
    RNS.log(f"rnfs {message}", level)


def node_name(node_id: bytes | None) -> str:
    """Printable form of a node ID, e.g. "<9e0a158a...>", or a placeholder for an unknown node."""
    return RNS.prettyhexrep(node_id) if node_id is not None else "an unidentified node"


def plural(count: int, noun: str) -> str:
    """count with noun, pluralised unless count is 1: "1 node", "3 nodes"."""
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def size(num_bytes: float) -> str:
    """Human-readable size, e.g. "3.00 MB"."""
    return RNS.prettysize(num_bytes)
