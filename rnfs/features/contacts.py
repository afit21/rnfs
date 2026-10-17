"""Contacts (feature "contacts"): a local address book of nicknames for names, nodes and LXMF addresses.

- `rnfs contacts add <nick> <hex> [--kind name|node|lxmf]`, `rnfs contacts rm <nick>`, `rnfs contacts ls`.
  Without --kind the daemon tells what the hash is when it can: one of its own or a known name, a node in
  its routing table, or an RNS destination it has heard (rnfs.name, rnfs.dht or lxmf.delivery).
- A nick is 1-32 characters of lowercase letters, digits, '-', '_' and '.', starting with a letter or
  digit. It can never be read as something else: CIDs are longer, and 32 hex digits (a node ID or name)
  aren't allowed.
- A target may start with a name contact's nick: `rnfs get alice/docs/report.pdf` resolves alice's name to
  its CID first (RnfsNode._parse_target calls expand_target; never for the gateway, so web pages can't use
  or probe the address book). Wherever the CLI takes a node ID (`get --from`, `ping`, `pin remote`), a node
  contact's nick works too (node_arg). The LXMF feature resolves a recipient's nick with resolve_id(nick, "lxmf"),
  trusts senders that has(address, "lxmf") finds, and names them in the inbox listing with nick_of(address,
  "lxmf"). What other features may call is rnfs.features.ContactsApi (tests/test_contacts.py checks it).
- Stored in the state database's contacts table (rnfs.statedb), each change written at once, durably; at most
  MAX_CONTACTS entries.

No RNS at module level (the CLI imports this module).
"""
import re
import threading
import time

from rnfs import ids
from rnfs.content.cid import is_valid as is_valid_cid
from rnfs.errors import NotFound
from rnfs.features import Feature, _common
from rnfs.features._common import LOG_INFO, LOG_WARNING
from rnfs.fsutil import printable

NAME = "contacts"
KINDS = ("name", "node", "lxmf")
MAX_CONTACTS = 10_000  # an address book is typed in by hand; this only bounds the saved state
EXPAND_AGE = 60.0  # seconds a nick's resolved CID is reused when a target starts with a contact's nick
_NICK = re.compile(r"[a-z0-9][a-z0-9._-]{0,31}")
# RNS destination aspects a hash is checked against to tell its kind
_KIND_ASPECTS = {"name": ("rnfs", "name"), "node": ("rnfs", "dht"), "lxmf": ("lxmf", "delivery")}
# Each kind in resolve_id's errors: (article, noun, the word before "contact")
_KIND_WORDS = {"name": ("a", "name", "name"), "node": ("a", "node", "node"), "lxmf": ("an", "LXMF address", "LXMF")}


def _log(message: str, level: int = LOG_INFO):
    """Log with this feature's prefix."""
    _common.log(f"contacts: {message}", level)


def check_nick(nick) -> str:
    """nick if it is a valid contact nick; ValueError otherwise."""
    if not isinstance(nick, str) or not _NICK.fullmatch(nick) or ids.HEX16_LOWER.fullmatch(nick) or is_valid_cid(nick):
        raise ValueError(f"invalid nick {nick!r}: use 1-32 lowercase letters, digits, '-', '_' or '.', starting "
                         "with a letter or digit (and not 32 hex digits)")
    return nick


def is_nick(text) -> bool:
    """True if text is shaped like a nick (whether or not such a contact exists)."""
    try:
        check_nick(text)
        return True
    except ValueError:
        return False


def parse_hex(text) -> bytes:
    """A 16-byte destination hash from 32 hex characters (RNS's <angle brackets> allowed). ValueError."""
    return ids.parse_hex16(text, "hash", brackets=True)


def check_kind(kind) -> str:
    """kind if it is a contact kind; ValueError otherwise."""
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {', '.join(KINDS)}")
    return kind


def import_contacts(value, tx) -> int:
    """Import contacts.json (statedb.migrate): its contacts replace the contacts table, each checked as rnfs 0.1.4
    loaded it (a malformed one is logged and skipped), at most MAX_CONTACTS. Anything but an object with a
    "contacts" object imports nothing. Returns the number imported."""
    tx.execute("DELETE FROM contacts")
    entries = value.get("contacts") if isinstance(value, dict) else None
    if not isinstance(entries, dict):
        return 0
    count = 0
    for nick, entry in entries.items():
        try:
            check_nick(nick)
            tx.execute("INSERT INTO contacts (nick, kind, id) VALUES (?, ?, ?)",
                       (nick, check_kind(entry["kind"]), parse_hex(entry["id"])))
            count += 1
        except (KeyError, TypeError, ValueError):
            _log(f"ignoring malformed contact {nick!r} in contacts.json", LOG_WARNING)
        if count >= MAX_CONTACTS:
            break
    return count


class ContactsFeature(Feature):
    """The address book. Thread-safe."""

    name = NAME
    optional = ("keepers",)  # a name it keeps is a name (detect_kind)

    def __init__(self, node):
        """Load the address book from the state database."""
        from rnfs import statedb
        super().__init__(node)
        self._lock = threading.Lock()
        self._contacts: dict[str, tuple[str, bytes]] = {}  # nick -> (kind, hash)
        self._nicks: dict[tuple[str, bytes], set[str]] = {}  # (kind, hash) -> the nicks naming it, for has()
        # name -> (CID, monotonic time of the resolve that found it), reused for EXPAND_AGE
        self._expanded: dict[bytes, tuple[str, float]] = {}
        self._db = statedb.open_store(node.config.state_dir, self._load)

    # State

    def _load(self, db):
        """Read the contacts table, by nick; malformed rows are logged and deleted, as are any past MAX_CONTACTS."""
        from rnfs import statedb
        # Written through, durably: a change is on disk when it returns; one whose write failed is written with the
        # next change, or by save()
        self._store = statedb.RowStore(db, "contacts", ("nick",), durable=True, lock=self._lock, row=self._row)
        bad = []
        for nick, kind, ident in db.query("SELECT nick, kind, id FROM contacts ORDER BY nick"):
            if len(self._contacts) >= MAX_CONTACTS:
                bad.append((nick,))  # never added (the address book is full): dropped, as from contacts.json
                continue
            try:
                check_nick(nick)
                if not isinstance(ident, bytes) or len(ident) != 16:
                    raise ValueError("not a 16-byte hash")
                self._set(nick, (check_kind(kind), ident))
            except (TypeError, ValueError):
                _log(f"ignoring malformed contact {nick!r}", LOG_WARNING)
                bad.append((nick,))
        statedb.delete_rows(db, "contacts", ("nick",), bad)

    def _row(self, nick: str) -> tuple | None:
        """nick's contacts row, or None if there is no such contact (lock held)."""
        entry = self._contacts.get(nick)
        return None if entry is None else (nick, *entry)

    def _set(self, nick: str, value: tuple[str, bytes] | None):
        """Set or (None) delete a contact, keeping _nicks in step (lock held, or loading)."""
        old = self._contacts.pop(nick, None)
        if old is not None:
            self._nicks[old].discard(nick)
            if not self._nicks[old]:
                del self._nicks[old]
        if value is not None:
            self._contacts[nick] = value
            self._nicks.setdefault(value, set()).add(nick)

    def _save(self, nick: str):
        """Write nick's row, and those of earlier changes whose save failed, durably (lock held). Raises OSError if
        it fails (the change stays in memory); they are then written with the next change, or by save()."""
        self._store.changed(nick)

    def save(self):
        """Write the contacts whose write failed, if any (each change is written as it is made)."""
        self._store.save()

    def close(self):
        """Release the state database (the node calls this once it has saved everything at shutdown)."""
        self._db.close()

    # The address book

    def add(self, nick: str, ident_hex: str, kind: str | None = None) -> dict:
        """Add or replace a contact; kind is detected when None. Returns the contact. ValueError if invalid."""
        check_nick(nick)
        ident = parse_hex(ident_hex)
        if kind is None:
            kind = self.detect_kind(ident)
            if kind is None:
                raise ValueError(f"can't tell whether {ident.hex()} is a name, a node or an LXMF address: "
                                 "give --kind")
        check_kind(kind)
        with self._lock:
            if nick not in self._contacts and len(self._contacts) >= MAX_CONTACTS:
                raise ValueError(f"the address book is full ({MAX_CONTACTS} contacts)")
            self._set(nick, (kind, ident))
            self._save(nick)
        _log(f"added {nick}: {kind} {ident.hex()}")
        return {"nick": nick, "kind": kind, "id": ident.hex()}

    def remove(self, nick: str):
        """Delete a contact. NotFound if there is none."""
        check_nick(nick)
        with self._lock:
            if nick not in self._contacts:
                raise NotFound(f"no contact {nick}")
            self._set(nick, None)
            self._save(nick)

    def list(self) -> list[dict]:
        """Every contact, by nick."""
        with self._lock:
            return [{"nick": nick, "kind": kind, "id": ident.hex()}
                    for nick, (kind, ident) in sorted(self._contacts.items())]

    def lookup(self, nick: str, kind: str | None = None) -> bytes | None:
        """The hash of contact nick (of that kind, if given), or None. For other features (e.g. LXMF's send)."""
        if not is_nick(nick):
            return None
        with self._lock:
            entry = self._contacts.get(nick)
        if entry is None or (kind is not None and entry[0] != kind):
            return None
        return entry[1]

    def has(self, ident: bytes, kind: str) -> bool:
        """Whether some contact of that kind has hash ident (for other features: LXMF trusts its contacts)."""
        with self._lock:
            return (kind, bytes(ident)) in self._nicks

    def nick_of(self, ident: bytes, kind: str) -> str | None:
        """The nick of the contact of that kind with hash ident (the first by nick if several name it), or None
        (for other features: the LXMF inbox listing names its senders' contacts)."""
        with self._lock:
            nicks = self._nicks.get((kind, bytes(ident)))
            return min(nicks) if nicks else None

    def get(self, nick: str) -> dict:
        """One contact. NotFound if there is none; ValueError for an invalid nick."""
        check_nick(nick)
        with self._lock:
            entry = self._contacts.get(nick)
        if entry is None:
            raise NotFound(f"no contact {nick}")
        return {"nick": nick, "kind": entry[0], "id": entry[1].hex()}

    def resolve_id(self, text: str, kind: str) -> bytes:
        """32 hex characters as bytes, or the hash of a contact of kind with that nick. ValueError otherwise.

        Other features call it too (keepers' and follow's names, LXMF's recipients), so they all word it alike.
        """
        try:
            return parse_hex(text)
        except ValueError:
            pass
        article, noun, word = _KIND_WORDS.get(kind, ("a", kind, kind))
        if is_nick(text):
            ident = self.lookup(text, kind)
            if ident is not None:
                return ident
            known = self.lookup(text)
            if known is not None:
                raise ValueError(f"contact {text} is not {article} {noun}")
            raise ValueError(f"no {word} contact {text}")
        raise ValueError(f"invalid {noun} {text!r}: give 32 hex characters or a contact's nick")

    # Kinds

    def detect_kind(self, ident: bytes) -> str | None:
        """What ident is, if this node can tell: "name", "node", "lxmf", or None."""
        node = self.node
        names = node.names
        if any(entry["name"] == ident.hex() for entry in names.list()) or names.record(ident) is not None:
            return "name"
        keepers = self.dependency("keepers")
        if keepers is not None and keepers.is_kept(ident):
            return "name"
        if ident == node.node_id or node.routing.contains(ident):
            return "node"
        return self._recalled_kind(ident)

    @staticmethod
    def _recalled_kind(ident: bytes) -> str | None:
        """The kind of an RNS destination whose announce RNS has heard (its public key is then known)."""
        try:
            import RNS
            identity = RNS.Identity.recall(ident)
            if identity is None:
                return None
            for kind, (app, *aspects) in _KIND_ASPECTS.items():
                if RNS.Destination.hash(identity, app, *aspects) == ident:
                    return kind
        except Exception:  # noqa: BLE001 - detection is a convenience; the user can give --kind
            return None
        return None

    # Targets

    def expand_target(self, target):
        """target with a leading name contact's nick replaced by the CID its name points at now.

        Anything that doesn't start with a nick is returned unchanged (walker.parse_target checks it).
        Raises ValueError for a nick that isn't a name contact, NotFound if the name can't be resolved.
        """
        if not isinstance(target, str):
            return target
        first, slash, rest = target.partition("/")
        if not is_nick(first):
            return target
        ident = self.lookup(first, "name")
        if ident is None:
            if self.lookup(first) is not None:
                raise ValueError(f"contact {first} is not a name")
            raise ValueError(f"{first!r} is neither a CID nor a contact's nick")
        return self._resolve_name(first, ident) + slash + rest

    def _resolve_name(self, nick: str, ident: bytes) -> str:
        """The CID ident points at now, reusing one resolved within EXPAND_AGE. NotFound if unresolved.

        Timed from the resolve, not from when the record was heard: a keeper's record carries its old
        emission time, so with the owner offline every use of the nick would otherwise go to the network
        (cat expands the same target twice: info, then read).
        """
        now = time.monotonic()
        with self._lock:
            cached = self._expanded.get(ident)
            if cached is not None and now - cached[1] < EXPAND_AGE:
                return cached[0]
        # Persisted like the CLI's resolve
        result = self.node.names.resolve(ident, persist=True, max_age=EXPAND_AGE)
        if result is None:
            raise NotFound(f"the name of contact {nick} ({ident.hex()}) could not be resolved")
        with self._lock:
            # Expired entries go first, so the cache stays as small as the names used in the last minute
            self._expanded = {key: value for key, value in self._expanded.items() if now - value[1] < EXPAND_AGE}
            self._expanded[ident] = (result["cid"], now)
        return result["cid"]

    def status(self) -> dict:
        """Number of contacts."""
        with self._lock:
            return {"contacts": len(self._contacts)}

    # API and CLI

    @classmethod
    def api_routes(cls) -> dict:
        """The address book endpoints (all require the token; the gateway never sees them)."""
        return {("GET", "/api/contacts"): _api_list, ("GET", "/api/contacts/get"): _api_get,
                ("POST", "/api/contacts/add"): _api_add, ("POST", "/api/contacts/rm"): _api_rm}

    @classmethod
    def cli_commands(cls, commands, command):
        """`rnfs contacts add|rm|ls`."""
        contacts = commands.add_parser("contacts", help="address book: nicks for names, nodes and LXMF addresses"
                                       ).add_subparsers(dest="contacts_command", required=True, metavar="command")
        add = command(contacts, "add", run_add, "add or replace a contact")
        add.add_argument("nick")
        add.add_argument("id", metavar="HEX", help="the name, node ID or LXMF address (32 hex characters)")
        add.add_argument("--kind", choices=KINDS, help="what it is (default: detected by the daemon)")
        command(contacts, "rm", run_rm, "delete a contact").add_argument("nick")
        command(contacts, "ls", run_ls, "list contacts: nick, kind, hash")


FEATURE = ContactsFeature


def _feature(handler) -> ContactsFeature:
    """The node's address book. Raises NotFound if the daemon has none."""
    return _common.feature(handler, NAME, "this daemon has no address book")


def _api_list(handler):
    """GET /api/contacts: {"contacts": [{"nick", "kind", "id"}]}."""
    handler.send_json(200, {"contacts": _feature(handler).list()})


def _api_get(handler):
    """GET /api/contacts/get?nick=: one contact (404 if none)."""
    handler.send_json(200, _feature(handler).get(handler.param("nick")))


def _api_add(handler):
    """POST /api/contacts/add {"nick", "id", "kind" (optional)}: the contact."""
    body = handler.json_body()
    kind = body.get("kind")
    handler.send_json(200, _feature(handler).add(body.get("nick"), body.get("id"),
                                                 None if kind is None else check_kind(kind)))


def _api_rm(handler):
    """POST /api/contacts/rm {"nick"}: {}."""
    _feature(handler).remove(handler.json_body().get("nick"))
    handler.send_json(200, {})


# CLI (no RNS here)


def run_add(args):
    """Add a contact; print it."""
    import rnfs.cli
    contact = rnfs.cli.client(args).post("/api/contacts/add", nick=args.nick, id=args.id, kind=args.kind)
    print(f"{printable(contact['nick'])} {printable(contact['kind'])} {printable(contact['id'])}")


def run_rm(args):
    """Delete a contact."""
    import rnfs.cli
    rnfs.cli.client(args).post("/api/contacts/rm", nick=args.nick)


def run_ls(args):
    """Print every contact: nick, kind, hash."""
    import rnfs.cli
    for contact in rnfs.cli.client(args).get("/api/contacts")["contacts"]:
        print(f"{printable(contact['nick'])} {printable(contact['kind'])} {printable(contact['id'])}")


def node_arg(args, text: str | None) -> str | None:
    """A node ID argument as 32 hex characters: unchanged if it is one, else a node contact's ID (via the API).

    For the CLI's --from, ping and pin remote. Fails the command for an unknown nick.
    """
    if text is None or ids.is_hex16(text, brackets=True) or not is_nick(text):
        return text  # hex, or something the API will reject with its own message
    import rnfs.cli
    contact = rnfs.cli.client(args).get("/api/contacts/get", nick=text)  # exits with "no contact <nick>"
    if contact.get("kind") != "node":
        rnfs.cli.fail(f"contact {text} is a {printable(contact.get('kind'))}, not a node")
    return contact["id"]
