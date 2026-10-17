"""The objects rnfs.Node and rnfs.Client return (design.md, "Public API"; provisional until 0.2). No RNS.

Each is a frozen dataclass made from the local API's JSON form (from_dict, which the facade's dicts share) and
turned back into it (to_dict: CIDs as their written form, node IDs and names as hex). str() gives the CID, name
or path where an object has one obvious text form. Cid is rnfs.content.cid's.
"""
from dataclasses import MISSING, dataclass, field, fields

from rnfs.content.cid import Cid

__all__ = ["Cid", "Added", "Written", "Entry", "Info", "Stat", "Pin", "Record", "Status", "Collected", "Verified",
           "Exported", "Imported", "DenyState"]


def _cid(text) -> Cid:
    """A Cid from its written form (a Cid is kept as it is)."""
    return text if isinstance(text, Cid) else Cid.parse(text)


def _hex(value) -> bytes | None:
    """bytes from hex (None stays None)."""
    return None if value is None else bytes.fromhex(value) if isinstance(value, str) else bytes(value)


def _dump(value):
    """A field's JSON form: CIDs written out, bytes as hex, tuples as lists."""
    if isinstance(value, Cid):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    if isinstance(value, tuple):
        return [_dump(item) for item in value]
    return value


def _field(load=None, default=MISSING):
    """A dataclass field whose JSON value from_dict turns into its type with load."""
    metadata = {"load": load} if load is not None else {}
    if default is MISSING:
        return field(metadata=metadata)
    return field(default=default, metadata=metadata)


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
        return {f.name: _dump(getattr(self, f.name)) for f in _json_fields(self)}

    @classmethod
    def from_dict(cls, data: dict):
        """The object for a JSON form (the API's, or the facade's dict); fields missing take their defaults."""
        values = {}
        for f in _json_fields(cls):
            if f.name in data:
                load = f.metadata.get("load")
                value = data[f.name]
                values[f.name] = load(value) if load is not None and value is not None else value
        return cls(**values)


@dataclass(frozen=True)
class Added(_Object):
    """What add() stored: the root's CID and size, warnings (entries a directory add skipped), whether sealed."""

    cid: Cid = _field(_cid)
    size: int = 0
    warnings: tuple[str, ...] = _field(tuple, ())
    sealed: bool = False

    def __str__(self) -> str:
        """The CID's written form."""
        return str(self.cid)


@dataclass(frozen=True)
class Written(_Object):
    """What get() wrote: the target's CID, the output path, warnings (warnings_total past the first 100), and for
    a sealed file whether it was one and its sealed name (only a name fit to be an output name)."""

    cid: Cid = _field(_cid)
    output: str | None = None
    warnings: tuple[str, ...] = _field(tuple, ())
    warnings_total: int | None = None
    sealed: bool = False
    name: str | None = None

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
    recipient (this node can open it), stored_size, and its plaintext size and sealed name if a recipient."""

    cid: Cid = _field(_cid)
    type: str = "file"
    size: int = 0
    executable: bool = False
    sealed: bool = False
    recipient: bool | None = None
    stored_size: int | None = None
    name: str | None = None


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
    sealed file (None when its stored blocks can't tell), and a local name for it if one is known."""

    cid: Cid = _field(_cid)
    sources: tuple[str, ...] = _field(tuple, ())
    complete: bool = False
    type: str | None = None
    sealed: bool | None = None
    name: str | None = None

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
