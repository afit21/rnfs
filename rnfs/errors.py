"""Errors shared across rnfs, and raised by its public API (rnfs.Node, rnfs.Client), which exports each as
rnfs.<name>. The API maps them to HTTP statuses by STATUSES (see design.md); rnfs.apiclient's errors for those
statuses are subclasses of these, so one except clause catches either class's.

Standard library only: rnfs.apiclient, which never loads RNS, derives its table from STATUSES.
"""


class RnfsError(Exception):
    """Base class for rnfs errors that are reported to the user rather than crashing."""


class InvalidDag(RnfsError):
    """A block is correctly hashed but isn't a valid DAG node, or the DAG breaks a reader check (section 5).

    The content author's fault, not the provider's: the root being fetched is marked invalid locally.
    """


class NotFound(RnfsError):
    """Content, a path inside a DAG, a name or an alias doesn't exist, or no provider could supply it."""


class TooLarge(RnfsError):
    """A fetch or read would exceed a bound: max_download_size, max_output_entries, the DAG byte or byte-range
    bound (section 7), a remote pin's or auto-fetch's quota, or the free space an output needs."""


class StorageFull(RnfsError):
    """Storing more would leave less than the disk reserve free (section 8; max_storage itself never refuses)."""


class Busy(RnfsError):
    """A local limit (e.g. gateway_fetches) is reached; try again later."""


class Denied(RnfsError):
    """The content is on this node's deny list (section 8): it isn't fetched, read, stored, served or provided."""


class Cancelled(RnfsError):
    """A fetch stopped because its observer asked it to (a job cancelled, or the node stopping)."""


class Closed(RnfsError):
    """The rnfs.Node or rnfs.Client was closed."""


class BlockMismatch(RnfsError, ValueError):
    """A block handed to the block store doesn't match its CID (or is over MAX_BLOCK_SIZE).

    The add and import paths only store blocks they built or checked, so there it is a bug, not a bad request:
    the API logs it at WARNING. Still a ValueError (400), as before, so every `except ValueError` still holds.
    """


class SealError(RnfsError, ValueError):
    """A sealed file is malformed or has been tampered with (rnfs.content.seal). A ValueError (400), as before."""


class NotRecipient(ValueError):
    """This node's identity isn't among a sealed file's recipients (rnfs.content.seal), or it has none. A
    ValueError (400)."""


# Error class -> the local API's HTTP status (design.md, "Node facade"), checked in order: an error gets the status
# of the first class it is an instance of, and anything else 500. One class per status: rnfs.api maps errors by
# it (BlockMismatch, SealError and NotRecipient are ValueErrors: 400), rnfs.apiclient each status back to its
# error, and rnfs.gateway has a fixed message for each
STATUSES = ((ValueError, 400), (NotFound, 404), (TooLarge, 413), (Denied, 451), (InvalidDag, 422), (Busy, 503),
            (StorageFull, 507))
