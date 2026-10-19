"""The LXMF inbox and outbox records (the state database's lxmf_messages table) and their import from rnfs 0.1.4.

rnfs 0.1.4 kept its LXMF inbox and outbox in <state>/lxmf_inbox.json and lxmf_outbox.json; statedb.migrate imports
them once into the lxmf_messages table (one row per record, in its box: "inbox" or "outbox", with each box's
next id and whether it was read whole in the meta table), checked and bounded as the LXMF feature always loaded
them: import_inbox and import_outbox.

Standard library only at module level (rnfs.statedb and rnfs.log, which load RNS, are imported when importing).
"""
import json

from rnfs import ids

INBOX, OUTBOX = "inbox", "outbox"  # the boxes: the lxmf_messages table's box, and their meta store
INBOX_LIMIT = 500  # inbox records kept
OUTBOX_LIMIT = 200  # outbox records kept
MAX_INBOX_BYTES = 4 * 1024 * 1024  # every inbox record as JSON
MAX_ID = 2 ** 63 - 2  # largest record id: the next id must still fit SQLite's integers


def _log(message: str, level: int):
    """Log through rnfs.log (imported here: it loads RNS), as the LXMF feature did."""
    from rnfs import log as rnfs_log
    rnfs_log.log(f"lxmf: {message}", level)


def json_bytes(value) -> int:
    """Bytes value takes as UTF-8 JSON, as the state database stores a record (statedb.dump)."""
    return len(json.dumps(value, ensure_ascii=False).encode("utf-8", "backslashreplace"))


def records(entries: list, limit: int, max_bytes: int | None, valid) -> tuple[list[dict], int]:
    """The records a log keeps of entries (loaded from the database, or a JSON file), oldest first, and how many
    were malformed: not a dict with an id (an int, 1 to MAX_ID) no earlier record has, that valid() accepts if
    given. Of the others, the oldest past limit are dropped, then the oldest past max_bytes as JSON (one is kept
    whatever its size)."""
    kept, seen, bad = [], set(), 0
    for entry in entries:
        if isinstance(entry, dict) and type(entry.get("id")) is int and 0 < entry["id"] <= MAX_ID \
                and entry["id"] not in seen and (valid is None or valid(entry)):
            kept.append(entry)
            seen.add(entry["id"])
        else:
            bad += 1
    kept = kept[-limit:]
    sizes = [json_bytes(e) for e in kept]
    total, first = sum(sizes), 0
    while max_bytes is not None and len(kept) - first > 1 and total > max_bytes:
        total -= sizes[first]
        first += 1
    return kept[first:], bad


def next_id(saved, entries: list[dict]) -> int:
    """The id the next record gets: the saved next id (if it is an int that fits), past every record's."""
    return max([saved if type(saved) is int and saved <= MAX_ID else 1] + [e["id"] + 1 for e in entries])


def _str_or_none(value) -> bool:
    """Whether value is a string or None."""
    return value is None or isinstance(value, str)


def _valid_links(value) -> bool:
    """Whether value is a list of link records (dicts, rnfs.uris.Link.to_json's form)."""
    return isinstance(value, list) and all(isinstance(link, dict) for link in value)


def valid_inbox_entry(entry: dict) -> bool:
    """Whether an inbox record has the shape the LXMF feature gave a message it received."""
    auto = entry.get("auto")
    return (isinstance(entry.get("hash"), str) and isinstance(entry.get("from"), str)
            and ids.HEX16.fullmatch(entry["from"]) is not None and _valid_links(entry.get("links"))
            and (auto is None or (isinstance(auto, list) and len(auto) == len(entry["links"])
                                  and all(isinstance(a, dict) and _str_or_none(a.get("state")) for a in auto)))
            and all(_str_or_none(entry.get(key)) for key in ("display_name", "title", "text", "method")))


def valid_outbox_entry(entry: dict) -> bool:
    """Whether an outbox record has the shape the LXMF feature gave a message it sent."""
    return (all(isinstance(entry.get(key), str) for key in ("hash", "to", "state", "method"))
            and _valid_links(entry.get("links")) and all(_str_or_none(entry.get(key)) for key in ("text", "error")))


def _import_log(box: str, value, tx, limit: int, max_bytes: int | None, valid, file: str) -> int:
    """Import a message log file of rnfs 0.1.4 (statedb.migrate): its records replace box's, checked and bounded
    as it loaded them, and it is clean only if every record was read. Returns the number of records imported."""
    from rnfs import statedb
    from rnfs.log import LOG_WARNING
    tx.execute("DELETE FROM lxmf_messages WHERE box = ?", (box,))
    entries, clean, following = [], False, 1
    if isinstance(value, dict):
        messages = value.get("messages")
        entries, bad = records(messages if isinstance(messages, list) else [], limit, max_bytes, valid)
        bad += not isinstance(messages, list)
        if bad:
            _log(f"dropped {bad} malformed record(s) from {file}", LOG_WARNING)
        clean = not bad
        following = next_id(value.get("next_id"), entries)
    tx.executemany("INSERT INTO lxmf_messages (box, id, record) VALUES (?, ?, ?)",
                   [(box, entry["id"], statedb.dump(entry)) for entry in entries])
    statedb.set_meta(tx, box, "next_id", following)
    statedb.set_meta(tx, box, "clean", int(clean))
    return len(entries)


def import_inbox(value, tx) -> int:
    """Import lxmf_inbox.json (statedb.migrate); the number of records imported."""
    return _import_log(INBOX, value, tx, INBOX_LIMIT, MAX_INBOX_BYTES, valid_inbox_entry, "lxmf_inbox.json")


def import_outbox(value, tx) -> int:
    """Import lxmf_outbox.json (statedb.migrate); the number of records imported."""
    return _import_log(OUTBOX, value, tx, OUTBOX_LIMIT, None, valid_outbox_entry, "lxmf_outbox.json")
