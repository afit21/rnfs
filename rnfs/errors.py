"""Errors shared across rnfs, and raised by its public API (rnfs.Node, rnfs.Client), which exports each as
rnfs.<name>. The API maps them to HTTP statuses (see design.md); rnfs.apiclient's errors for those statuses are
subclasses of these, so one except clause catches either class's."""


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
