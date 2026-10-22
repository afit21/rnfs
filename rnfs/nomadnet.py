"""NomadNet links for `rnfs add <destination>:/page/<path>` and `:/file/<path>` (specification section 11, "Adding from
NomadNet"): their syntax, the name a snapshot is added under, and NomadNet's "Request Not Allowed" page. No RNS
(the CLI imports this module); the fetch itself is rnfs/network/nomadnet.py.

A NomadNet node serves pages and files as link requests on its `nomadnetwork.node` destination: `/page/<path>`
answered with the page's bytes (an executable page's output, rendered for that request), `/file/<path>` with a
Resource carrying the file and `{"name": <file name>}` metadata. A page or file the visitor isn't allowed (a
`.allowed` list not naming its identity) is answered with NomadNet's own "Request Not Allowed" page instead, as
bytes, never as an error: an add recognises it (is_not_allowed) and fails rather than pinning it.
"""
import re
from typing import NamedTuple

from rnfs.entry_names import is_valid_name  # an entry name, as the DAG has it

APP_NAME, ASPECT = "nomadnetwork", "node"  # the destination NomadNet nodes serve pages and files on
PAGE, FILE = "page", "file"
MAX_PATH_LENGTH = 1024  # characters of a link's path; NomadNet sends only its hash, but rnfs keeps it as the origin

_LINK = re.compile(r"([0-9A-Fa-f]{32}):(/(page|file)/.*)", re.DOTALL)

# What NomadNet's Node.py (DEFAULT_NOTALLOWED, unchanged since its first releases) answers a visitor whose
# identity a page's or file's .allowed list doesn't name
NOT_ALLOWED = b">Request Not Allowed\n\nYou are not authorised to carry out the request.\n"


class NomadnetLink(NamedTuple):
    """A NomadNet link: the node's destination hash and the request path ("/page/index.mu", "/file/song.mp3")."""

    destination: bytes
    path: str

    @property
    def kind(self) -> str:
        """PAGE or FILE."""
        return PAGE if self.path.startswith("/page/") else FILE

    @property
    def last_name(self) -> str:
        """The path's last component: a page's name, and a file's when the node sends none fit to use."""
        return self.path.rsplit("/", 1)[-1]

    def __str__(self) -> str:
        """The link as written: <32 hex>:<path>."""
        return f"{self.destination.hex()}:{self.path}"


def looks_like(text) -> bool:
    """True if text is written as a NomadNet link (<32 hex>:/page/… or :/file/…), well-formed or not: `rnfs add`
    then reads it as one, and a local file named like that is added as ./<name>."""
    return isinstance(text, str) and _LINK.fullmatch(text) is not None


def parse(text) -> NomadnetLink:
    """The NomadNet link text writes, or ValueError saying what is wrong with it.

    The path is /page/<path> or /file/<path>: components separated by single slashes, each a valid entry name
    (is_valid_name: no NUL, not . or ..), printable, at most MAX_PATH_LENGTH characters in all. Request data (a
    page's `var_` values or form fields, written after a backtick) isn't supported.
    """
    match = _LINK.fullmatch(text) if isinstance(text, str) else None
    if match is None:
        raise ValueError(f"not a NomadNet link: {text!r} (expected <32 hex>:/page/<path> or <32 hex>:/file/<path>)")
    path = match.group(2)
    if "`" in path:
        raise ValueError("NomadNet request data (a page's fields or var_ values, after a backtick) isn't supported")
    if len(path) > MAX_PATH_LENGTH:
        raise ValueError(f"a NomadNet path may have at most {MAX_PATH_LENGTH} characters")
    if not path.isprintable():
        raise ValueError(f"NomadNet path {path!r} has a non-printable character")
    for name in path.split("/")[2:]:
        if not is_valid_name(name):
            raise ValueError(f"NomadNet path {path!r} has an empty, . or .. component")
    return NomadnetLink(bytes.fromhex(match.group(1)), path)


def snapshot_name(link: NomadnetLink, sent_name=None) -> str:
    """The name a snapshot of link is added under (with --wrap, and sealed inside): for a file the name the node
    sent in its metadata (bytes or text) if it is a valid entry name, else the path's last component; for a page
    always the path's last component."""
    if link.kind == FILE and isinstance(sent_name, (bytes, bytearray)):
        try:
            sent_name = bytes(sent_name).decode("utf-8")
        except UnicodeDecodeError:
            sent_name = None
    if link.kind == FILE and isinstance(sent_name, str) and is_valid_name(sent_name) and sent_name.isprintable():
        return sent_name
    return link.last_name


def is_not_allowed(data: bytes) -> bool:
    """True if data is NomadNet's "Request Not Allowed" page (line endings and surrounding whitespace aside)."""
    if not isinstance(data, (bytes, bytearray)) or len(data) > 4 * len(NOT_ALLOWED):
        return False
    return bytes(data).replace(b"\r\n", b"\n").strip() == NOT_ALLOWED.strip()
