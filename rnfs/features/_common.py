"""Helpers shared by the feature modules. No RNS at module level (the CLI imports every feature module)."""
from rnfs.errors import NotFound

# RNS's loglevels (RNS.LOG_ERROR ...), defined here so feature modules can name them without loading RNS
LOG_ERROR, LOG_WARNING, LOG_NOTICE, LOG_INFO, LOG_VERBOSE = 1, 2, 3, 4, 5


def log(message: str, level: int = LOG_VERBOSE):
    """Log through rnfs.log (imported here: it loads RNS)."""
    from rnfs import log as rnfs_log
    rnfs_log.log(message, level)


def feature(handler, name: str, missing: str | None = None):
    """The feature called name of the handler's node. NotFound (with missing, if given) if the daemon has none."""
    try:
        return handler.node.feature(name)
    except (AttributeError, KeyError):
        raise NotFound(missing or f"this daemon has no {name} feature") from None
