"""Errors shared across rnfs. The API maps them to HTTP statuses (see design.md)."""


class RnfsError(Exception):
    """Base class for rnfs errors that are reported to the user rather than crashing."""


class InvalidDag(RnfsError):
    """A block is correctly hashed but isn't a valid DAG node, or the DAG breaks a reader check (section 5).

    The content author's fault, not the provider's: the root being fetched is marked invalid locally.
    """


class NotFound(RnfsError):
    """Content, a path inside a DAG, a name or an alias doesn't exist, or no provider could supply it."""


class TooLarge(RnfsError):
    """A fetch would exceed max_download_size, or writing out a DAG exceeded max_output_entries."""


class StorageFull(RnfsError):
    """Pins alone fill max_storage, so a fetch that isn't being pinned is refused (section 8)."""


class Busy(RnfsError):
    """A local limit (e.g. gateway_fetches) is reached; try again later."""


class Denied(RnfsError):
    """The content is on this node's deny list (section 8): it isn't fetched, read, stored, served or provided."""
