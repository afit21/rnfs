"""The objects rnfs.Node and rnfs.Client return (design.md, "Public API"; provisional until 0.2). No RNS.

Each is a frozen dataclass made from the local API's JSON form (from_dict, which the facade's dicts share) and
turned back into it (to_dict: CIDs as their written form, node IDs, names and hashes as hex, nested objects as
their own form). str() gives the CID, name or path where an object has one obvious text form. A field whose JSON
key is a Python keyword ("from") is named otherwise (sender, from_node). Cid is rnfs.content.cid's.
"""
from dataclasses import MISSING, dataclass, field, fields

from rnfs.content.cid import Cid

__all__ = ["Cid", "Added", "Written", "Entry", "Info", "Stat", "Pin", "Record", "Status", "Collected", "Verified",
           "Exported", "Imported", "DenyState", "Peer", "Stats", "Contact", "Kept", "Followed", "LxmfAddress",
           "MessageLink", "Message", "Outgoing", "Sent", "Synced", "Cached", "Uncached", "Event", "EventBatch",
           "Capabilities", "Setting", "Settings", "SettingsChange", "RemotePin", "ServedPin", "RemotePinUsage",
           "ServedPins", "FolderSync"]


def _cid(text) -> Cid:
    """A Cid from its written form (a Cid is kept as it is)."""
    return text if isinstance(text, Cid) else Cid.parse(text)


def _hex(value) -> bytes | None:
    """bytes from hex (None stays None)."""
    return None if value is None else bytes.fromhex(value) if isinstance(value, str) else bytes(value)


def _dump(value):
    """A field's JSON form: CIDs written out, bytes as hex, tuples as lists, objects as their to_dict()."""
    if isinstance(value, Cid):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, tuple):
        return [_dump(item) for item in value]
    if isinstance(value, frozenset):
        return sorted(_dump(item) for item in value)
    if isinstance(value, _Object):
        return value.to_dict()
    return value


def _field(load=None, default=MISSING, key: str | None = None):
    """A dataclass field whose JSON value from_dict turns into its type with load; key is its JSON key, if not
    the field's name."""
    metadata = {"load": load} if load is not None else {}
    if key is not None:
        metadata["key"] = key
    if default is MISSING:
        return field(metadata=metadata)
    return field(default=default, metadata=metadata)


def _key(f) -> str:
    """A field's JSON key."""
    return f.metadata.get("key", f.name)


def _json_fields(cls) -> list:
    """cls's fields that are JSON fields of their own (not those marked metadata={"json": False})."""
    return [f for f in fields(cls) if f.metadata.get("json", True)]


def _cids(values) -> tuple[Cid, ...]:
    """A tuple of Cids from written forms."""
    return tuple(_cid(value) for value in values)


class _Object:
    """to_dict and from_dict for the dataclasses below."""

    def to_dict(self) -> dict:
        """The local API's JSON form."""
        return {_key(f): _dump(getattr(self, f.name)) for f in _json_fields(self)}

    @classmethod
    def from_dict(cls, data: dict):
        """The object for a JSON form (the API's, or the facade's dict); fields missing take their defaults, and
        keys it doesn't know (a newer daemon's) are left out."""
        values = {}
        for f in _json_fields(cls):
            if _key(f) in data:
                load = f.metadata.get("load")
                value = data[_key(f)]
                values[f.name] = load(value) if load is not None and value is not None else value
        return cls(**values)


@dataclass(frozen=True)
class Added(_Object):
    """What add() (or add_tar()) stored: the root's CID and size, warnings (entries a directory add or a tar
    skipped; warnings_total counts them past the first 100, where an add_tar caps them), whether sealed."""

    cid: Cid = _field(_cid)
    size: int = 0
    warnings: tuple[str, ...] = _field(tuple, ())
    sealed: bool = False
    warnings_total: int | None = None

    def __str__(self) -> str:
        """The CID's written form."""
        return str(self.cid)


@dataclass(frozen=True)
class Written(_Object):
    """What get() wrote: the target's CID, the output path, warnings (warnings_total past the first 100), and for
    a sealed file whether it was one and its sealed name (only a name fit to be an output name); for a sealed folder
    unpacked (extracted), the name of the directory written instead."""

    cid: Cid = _field(_cid)
    output: str | None = None
    warnings: tuple[str, ...] = _field(tuple, ())
    warnings_total: int | None = None
    sealed: bool = False
    name: str | None = None
    extracted: bool = False

    def __str__(self) -> str:
        """The output path."""
        return self.output or str(self.cid)


@dataclass(frozen=True)
class Entry(_Object):
    """A directory entry: name, kind ("file", "executable" or "directory"), content size and CID."""

    name: str
    kind: str
    size: int
    cid: Cid = _field(_cid)

    @property
    def is_dir(self) -> bool:
        """True for a directory."""
        return self.kind == "directory"

    def __str__(self) -> str:
        """The entry's name."""
        return self.name


@dataclass(frozen=True)
class Info(_Object):
    """A target's own CID, type ("file" or "directory"), size and executable bit; for a sealed file, sealed,
    recipient (this node can open it), stored_size, and its plaintext size and sealed name if a recipient, and
    whether it is a sealed folder get would unpack (sealed_folder; False from a daemon before 0.2.0)."""

    cid: Cid = _field(_cid)
    type: str = "file"
    size: int = 0
    executable: bool = False
    sealed: bool = False
    recipient: bool | None = None
    stored_size: int | None = None
    name: str | None = None
    sealed_folder: bool = False


@dataclass(frozen=True)
class Stat(_Object):
    """A target's size, its DAG's block count, and how many of those blocks (and bytes) are stored here."""

    cid: Cid = _field(_cid)
    size: int = 0
    blocks: int = 0
    local_blocks: int = 0
    local_bytes: int = 0


@dataclass(frozen=True)
class Pin(_Object):
    """A pin: its CID, sources ("user", "folder", "name:<alias>", ...), whether complete, its type and whether a
    sealed file (None when its stored blocks can't tell), and a local name for it if one is known. From 0.2.0 (None
    from an older daemon, or where not known): the label its user gave it (label_pin), where it was added from on
    this machine (origin), its content size (once its root block is stored), a directory's entry count, and when
    it was first pinned (Unix time; None for a pin made before 0.2.0)."""

    cid: Cid = _field(_cid)
    sources: tuple[str, ...] = _field(tuple, ())
    complete: bool = False
    type: str | None = None
    sealed: bool | None = None
    name: str | None = None
    label: str | None = None
    origin: str | None = None
    size: int | None = None
    entries: int | None = None
    added: float | None = None

    def __str__(self) -> str:
        """The CID's written form."""
        return str(self.cid)


@dataclass(frozen=True)
class Record(_Object):
    """A name's record: the name (16 bytes), the CID it points at, its sequence, and how many seconds ago it was
    heard (None when not known)."""

    name: bytes = _field(_hex)
    cid: Cid = _field(_cid)
    sequence: int = 0
    age: float | None = None


@dataclass(frozen=True)
class Status(_Object):
    """The node's status; features holds every other field (traffic, jobs, LXMF, ...)."""

    version: str
    node_id: bytes | None = _field(_hex, None)
    identity_hash: bytes | None = _field(_hex, None)
    dht_mode: str | None = None
    slow: bool | None = None
    private: bool | None = None
    known_nodes: int | None = None
    records: int | None = None
    pins: int | None = None
    provided: int | None = None
    blocks: int | None = None
    used_bytes: int | None = None
    max_storage: int | None = None
    names: int | None = None
    unexported_names: list | None = None  # aliases whose key was never exported or imported (rnfs 0.2.0)
    features: dict = field(default_factory=dict, repr=False, metadata={"json": False})

    def to_dict(self) -> dict:
        """The API's form: the fields above with the features' merged in."""
        return {**super().to_dict(), **self.features}

    @classmethod
    def from_dict(cls, data: dict) -> "Status":
        """From the API's form, every field not above going into features."""
        status = super().from_dict(data)
        known = {f.name for f in fields(cls)}
        return cls(**{f.name: getattr(status, f.name) for f in _json_fields(cls)},
                   features={key: value for key, value in data.items() if key not in known})


@dataclass(frozen=True)
class Collected(_Object):
    """What gc() removed."""

    removed_blocks: int = 0
    freed_bytes: int = 0


@dataclass(frozen=True)
class Verified(_Object):
    """What verify() checked, and the CIDs of the bad blocks it deleted."""

    checked: int = 0
    bad: tuple[Cid, ...] = _field(_cids, ())


@dataclass(frozen=True)
class Exported(_Object):
    """What export() wrote: the archive's roots and its block count."""

    roots: tuple[Cid, ...] = _field(_cids, ())
    blocks: int = 0


@dataclass(frozen=True)
class Imported(_Object):
    """What import_archive() read: its roots, those not complete (so not pinned), and its block count."""

    roots: tuple[Cid, ...] = _field(_cids, ())
    incomplete: tuple[Cid, ...] = _field(_cids, ())
    blocks: int = 0


@dataclass(frozen=True)
class DenyState(_Object):
    """The deny list: the local entries, and each deny_lists source with the version in use and its state."""

    entries: tuple[str, ...] = _field(tuple, ())
    lists: tuple[dict, ...] = _field(tuple, ())


@dataclass(frozen=True)
class Peer(_Object):
    """A node this one knows (peers()): its node ID, DHT mode ("server", "client", None unknown), whether in the
    routing table and for how many seconds (seniority; senior once past provider_seniority), the path's hops,
    interface and bitrate, whether a link is open and its rate estimate (bits/s), when it was last heard and last
    active (Unix times), whether banned, the traffic counted since the node started (bytes sent and received,
    requests made, answered, refused, failed, and bad blocks), and from 0.2.0 when it was first heard, the
    round-trip seconds of the last link to it (rtt, measured at rtt_at) and whether it is online now (None from a
    daemon that predates them)."""

    node_id: bytes = _field(_hex)
    mode: str | None = None
    in_table: bool = False
    seniority: float | None = None
    senior: bool = False
    hops: int | None = None
    interface: str | None = None
    bitrate: float | None = None
    link: bool = False
    rate: float | None = None
    last_heard: float | None = None
    last_active: float | None = None
    banned: bool = False
    sent: int = 0
    received: int = 0
    requests: int = 0
    answered: int = 0
    refused: int = 0
    failed: int = 0
    bad: int = 0
    first_heard: float | None = None
    rtt: float | None = None
    rtt_at: float | None = None
    online: bool | None = None

    def __str__(self) -> str:
        """The node ID in hex."""
        return self.node_id.hex()


@dataclass(frozen=True)
class Stats(_Object):
    """The node's traffic since it started (stats()): since (Unix time), totals ({"sent", "received",
    "responses", "requests", "answered", "refused", "failed"}), the /blocks refusals by reason, the bytes sent and
    received per interface ({name: {"sent", "received"}}), and the upload budget per hour and what is left of it
    (None without one)."""

    since: float | None = None
    totals: dict = field(default_factory=dict)
    refused: dict = field(default_factory=dict)
    interfaces: dict = field(default_factory=dict)
    upload_budget: int | None = None
    upload_budget_left: int | None = None


@dataclass(frozen=True)
class Contact(_Object):
    """An address book entry (contacts): its nick, its kind ("name", "node" or "lxmf") and the hash it names (16
    bytes)."""

    nick: str
    kind: str
    id: bytes = _field(_hex)

    def __str__(self) -> str:
        """The nick."""
        return self.nick


@dataclass(frozen=True)
class Kept(_Object):
    """A name this node keeps (names.keep): the name (16 bytes), why it is kept ("user", "follow", "own"), and the
    signed record held, if any: its sequence, CID and emission time (Unix)."""

    name: bytes = _field(_hex)
    reasons: tuple[str, ...] = _field(tuple, ())
    sequence: int | None = None
    cid: Cid | None = _field(_cid, None)
    emitted: float | None = None

    def __str__(self) -> str:
        """The name in hex."""
        return self.name.hex()


@dataclass(frozen=True)
class Followed(_Object):
    """A name this node follows (names.follow): the name (16 bytes), the target pinned for it (None until the
    first is complete), the latest target and sequence known, and what the last check did (state: "waiting",
    "current", "unresolved", "fetching <cid>", or why it failed)."""

    name: bytes = _field(_hex)
    pinned: Cid | None = _field(_cid, None)
    latest: Cid | None = _field(_cid, None)
    sequence: int | None = None
    state: str = "waiting"

    def __str__(self) -> str:
        """The name in hex."""
        return self.name.hex()


@dataclass(frozen=True)
class FolderSync(_Object):
    """A name of this node synced to a folder (names.sync): its alias, the folder (a path on the node's machine),
    the sync's state ("new": not scanned yet, "settling": a change waiting to stay unchanged a minute, "waiting":
    for an hour since its last publish, "synced", "paused": reason says why), the folder's root as last imported,
    the root the sync last published and when (Unix time), and when the hourly limit next lets it publish (None:
    it holds nothing back)."""

    alias: str
    path: str
    state: str = "new"
    reason: str | None = None
    root: Cid | None = _field(_cid, None)
    published: Cid | None = _field(_cid, None)
    published_at: float | None = None
    next_publish: float | None = None

    def __str__(self) -> str:
        """The alias."""
        return self.alias


@dataclass(frozen=True)
class LxmfAddress(_Object):
    """This node's LXMF identity (lxmf.address()): its address (16 bytes), display name, the propagation node set
    (lxmf_propagation_node) and the nearest one heard (None: none)."""

    address: bytes = _field(_hex)
    display_name: str | None = None
    propagation_node: bytes | None = _field(_hex, None)
    nearest_propagation_node: bytes | None = _field(_hex, None)

    def __str__(self) -> str:
        """The address in hex."""
        return self.address.hex()


@dataclass(frozen=True)
class MessageLink(_Object):
    """An rnfs link in an LXMF message (section 13): its rnfs: URI, the CID (a name link's: what it pointed at when
    sent) or the name (16 bytes) and the path below it, the size and provider hint (from_node) as the sender gave
    them (untrusted: fetches verify)."""

    uri: str
    cid: Cid | None = _field(_cid, None)
    name: bytes | None = _field(_hex, None)
    path: tuple[str, ...] = _field(tuple, ())
    size: int | None = None
    from_node: bytes | None = _field(_hex, None, key="from")

    def __str__(self) -> str:
        """The rnfs: URI."""
        return self.uri


def _links(values) -> tuple[MessageLink, ...]:
    """A message's links from their JSON form."""
    return tuple(MessageLink.from_dict(value) for value in values)


@dataclass(frozen=True)
class Message(_Object):
    """A message in the LXMF inbox (lxmf.inbox()): its inbox id, hash, sender's LXMF address (sender, the API's
    "from") and display name (the sender's own word: trusted and contact say what this node knows of it), when
    sent (the sender's clock; None if implausible) and received, title and text, delivery method, its rnfs links,
    each link's auto-fetch result ({"state", "cid" or "error"}; None unless the sender is in lxmf_auto_fetch_from),
    whether the sender is trusted now, and the nick of the LXMF contact with its address."""

    id: int
    hash: bytes = _field(_hex)
    sender: bytes = _field(_hex, key="from")
    display_name: str | None = None
    sent: float | None = None
    received: float | None = None
    title: str = ""
    text: str = ""
    method: str = "unknown"
    links: tuple[MessageLink, ...] = _field(_links, ())
    auto: tuple[dict, ...] | None = _field(tuple, None)
    trusted: bool = False
    contact: str | None = None


@dataclass(frozen=True)
class Outgoing(_Object):
    """A message in the LXMF outbox (lxmf.outbox()): its outbox id, hash, recipient, when queued, its links and
    text, method ("direct" or "propagated"), state ("outbound", "sending", "sent", "delivered", "failed", ...) and
    progress, the error of a failed propagation fallback, and for a direct message sent again through the
    propagation node, the direct attempt's hash (direct_hash) and fallback True."""

    id: int
    hash: bytes = _field(_hex)
    to: bytes = _field(_hex)
    time: float | None = None
    links: tuple[MessageLink, ...] = _field(_links, ())
    text: str = ""
    method: str = "direct"
    state: str = "outbound"
    progress: float | None = None
    error: str | None = None
    direct_hash: bytes | None = _field(_hex, None)
    fallback: bool = False


@dataclass(frozen=True)
class Sent(_Object):
    """A message lxmf.send() queued: its outbox id, hash, recipient's address and method; the outbox says how it
    went."""

    id: int
    hash: bytes = _field(_hex)
    to: bytes = _field(_hex)
    method: str = "direct"


@dataclass(frozen=True)
class Synced(_Object):
    """What lxmf.sync() did: the propagation node asked (None: none set or heard) and whether a download
    started."""

    propagation_node: bytes | None = _field(_hex, None)
    started: bool = False


@dataclass(frozen=True)
class Cached(_Object):
    """A cached fetch (cache()): a download kept until evicted, not pinned. cid is the target, reached from root
    through path (() for a bare CID); whether the same content is pinned too, complete, its type and size (None
    until its root block is stored), and whether it is provided to the network."""

    cid: Cid = _field(_cid)
    root: Cid | None = _field(_cid, None)
    path: tuple[str, ...] = _field(tuple, ())
    pinned: bool = False
    complete: bool = False
    type: str | None = None
    size: int | None = None
    provided: bool = False

    def __str__(self) -> str:
        """The CID's written form."""
        return str(self.cid)


@dataclass(frozen=True)
class RemotePin(_Object):
    """A remote pin this node asked for (remote_pins()): the node asked (16 bytes) and the CID; whether it asked to pin
    it (False: to release it) and whether the node accepted; when (Unix time); its state ("waiting", "done",
    "missing", "refused", "released") and done; when last checked conclusively and when the node was last found
    providing it (None if never)."""

    node: bytes = _field(_hex)
    cid: Cid = _field(_cid)
    pin: bool = True
    accepted: bool = False
    asked: float | None = None
    state: str = "waiting"
    done: bool = False
    checked: float | None = None
    seen: float | None = None

    def __str__(self) -> str:
        """The CID's written form."""
        return str(self.cid)


@dataclass(frozen=True)
class ServedPin(_Object):
    """A remote pin another identity asked of this node (served_pins()): the identity hash (16 bytes), the CID, its
    state ("waiting", "fetching", "pinned", "dropped", "refused", "released"), why for one dropped or refused
    ("quota", "not_found", "denied", "queue_full", ...), when (Unix time; None where not recorded) and a detail."""

    identity: bytes = _field(_hex)
    cid: Cid = _field(_cid)
    state: str = "waiting"
    reason: str | None = None
    at: float | None = None
    detail: str | None = None

    def __str__(self) -> str:
        """The CID's written form."""
        return str(self.cid)


@dataclass(frozen=True)
class RemotePinUsage(_Object):
    """An identity's remote pins on this node: bytes used against its quota (remote_pin_quota), how many are pinned
    and waiting, and whether it is still on remote_pin_identities."""

    identity: bytes = _field(_hex)
    used: int = 0
    quota: int = 0
    pinned: int = 0
    waiting: int = 0
    allowed: bool = False


def _served(values) -> tuple[ServedPin, ...]:
    """ServedPins from their JSON form."""
    return tuple(ServedPin.from_dict(value) for value in values)


def _usage(values) -> tuple[RemotePinUsage, ...]:
    """RemotePinUsages from their JSON form."""
    return tuple(RemotePinUsage.from_dict(value) for value in values)


@dataclass(frozen=True)
class ServedPins(_Object):
    """served_pins(): the remote pins served, most recent first, each identity's usage, and remote_pin_quota."""

    pins: tuple[ServedPin, ...] = _field(_served, ())
    usage: tuple[RemotePinUsage, ...] = _field(_usage, ())
    quota: int = 0


@dataclass(frozen=True)
class Uncached(_Object):
    """What uncache() evicted: the cache records, and the blocks (and bytes) nothing else needed."""

    removed_records: int = 0
    removed_blocks: int = 0
    freed_bytes: int = 0


@dataclass(frozen=True)
class Event(_Object):
    """Something that changed on the node (events): seq (1, 2, ... in the node's run), kind ("job", "pin",
    "cache", "inbox", "name", "peer", "remote_pin"; design.md, "Events", lists each kind's fields), time (Unix), and
    the kind's own fields in fields. kind "lost" (lost is True) is no node's: events.watch delivers it when events
    were missed (the watch fell behind, or the daemon restarted), so what the app shows should be read again; its
    fields hold the run's "epoch"."""

    seq: int
    kind: str
    time: float | None = None
    fields: dict = field(default_factory=dict, metadata={"json": False})

    @property
    def lost(self) -> bool:
        """True for the "lost" marker."""
        return self.kind == "lost"

    def to_dict(self) -> dict:
        """The API's form: seq, kind and time with the kind's fields merged in."""
        return {**self.fields, **super().to_dict()}

    @classmethod
    def from_dict(cls, data: dict) -> "Event":
        """From the API's form, every key but seq, kind and time going into fields."""
        known = {"seq", "kind", "time"}
        return cls(data["seq"], data["kind"], data.get("time"),
                   {key: value for key, value in data.items() if key not in known})


def _events(values) -> tuple[Event, ...]:
    """Events from their JSON form."""
    return tuple(Event.from_dict(value) for value in values)


@dataclass(frozen=True)
class EventBatch(_Object):
    """What events.since() answered: the events, the sequence to ask after next, whether events were missed
    (lost: the node's run changed, or they are no longer held; events then holds every one held, and the app should
    read again what it shows), and the run's epoch to pass back."""

    events: tuple[Event, ...] = _field(_events, ())
    next: int = 0
    lost: bool = False
    epoch: str | None = None


def _routes(values) -> tuple[tuple[str, str], ...]:
    """Routes ([method, path] pairs) as tuples."""
    return tuple(tuple(route) for route in values)


@dataclass(frozen=True)
class Capabilities(_Object):
    """What the node offers (capabilities()): its version, its capabilities (api.CAPABILITIES: "events",
    "pin_label", "config", ...), the API routes ((method, path)), the features loaded, the event kinds, the
    error codes and the deprecated features (they still work, but a later release removes them: "lxmf"). Empty
    from a daemon before 0.2.0, which can't say: every has() is False."""

    version: str | None = None
    capabilities: frozenset[str] = _field(frozenset, frozenset())
    routes: tuple[tuple[str, str], ...] = _field(_routes, ())
    features: tuple[str, ...] = _field(tuple, ())
    event_kinds: tuple[str, ...] = _field(tuple, ())
    error_codes: tuple[str, ...] = _field(tuple, ())
    deprecated: tuple[str, ...] = _field(tuple, ())

    def has(self, capability: str) -> bool:
        """Whether the node offers capability."""
        return capability in self.capabilities

    def is_deprecated(self, feature: str) -> bool:
        """Whether feature is deprecated: it still works, but a later release removes it."""
        return feature in self.deprecated


def _frozen(value):
    """A setting's JSON value with lists made tuples (so a frozen Setting compares and reads alike from either
    class: the API's JSON has lists where a Node may have either)."""
    if isinstance(value, (list, tuple)):
        return tuple(_frozen(item) for item in value)
    return value


@dataclass(frozen=True)
class Setting(_Object):
    """One setting (config()): its schema (rnfs.config.settings_schema: name, group, type, choices, nullable,
    default, unit, minimum, maximum, exclusive_minimum, description, live: a change applies without a restart),
    the running value, rnfs.toml's (file; None where it doesn't set it), whether the command line (a Node's
    settings) overrides rnfs.toml, and whether a restart would run another value (pending); deprecated: why the
    setting is going away though it still works (the lxmf_* settings), else None. Lists are tuples."""

    name: str
    group: str | None = None
    type: str | None = None
    choices: tuple | None = _field(tuple, None)
    nullable: bool = False
    default: object = _field(_frozen, None)
    unit: str | None = None
    minimum: float | None = None
    maximum: float | None = None
    exclusive_minimum: bool = False
    description: str | None = None
    live: bool = False
    value: object = _field(_frozen, None)
    file: object = _field(_frozen, None)
    overridden: bool = False
    pending: bool = False
    deprecated: str | None = None  # last, after the fields a caller may give by position

    def __str__(self) -> str:
        """The setting's name."""
        return self.name


def _settings(values) -> tuple[Setting, ...]:
    """Settings from their JSON form."""
    return tuple(Setting.from_dict(value) for value in values)


@dataclass(frozen=True)
class Settings(_Object):
    """The node's settings (config()): its rnfs.toml's path, every setting in the schema's order (settings[name]
    gives one, iterating gives each), why a restart would refuse rnfs.toml (file_error, None if it wouldn't), and
    the warnings reading it gave."""

    path: str | None = None
    settings: tuple[Setting, ...] = _field(_settings, ())
    file_error: str | None = None
    warnings: tuple[str, ...] = _field(tuple, ())

    def __getitem__(self, name: str) -> Setting:
        """The setting called name. KeyError if there is none."""
        for setting in self.settings:
            if setting.name == name:
                return setting
        raise KeyError(name)

    def __iter__(self):
        """Each setting."""
        return iter(self.settings)


@dataclass(frozen=True)
class SettingsChange(_Object):
    """What config_set() did to rnfs.toml (path): the settings applied to the running node now, those whose new
    value applies at the next start (restart_required), those the command line overrides (overridden, which keeps
    winning), and the warnings reading the file gave."""

    path: str | None = None
    applied: tuple[str, ...] = _field(tuple, ())
    restart_required: tuple[str, ...] = _field(tuple, ())
    overridden: tuple[str, ...] = _field(tuple, ())
    warnings: tuple[str, ...] = _field(tuple, ())
