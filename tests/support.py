"""What the platform running the tests can do, for skipping tests that need POSIX features (not a test module)."""
import os
import tempfile

# Unix permission bits (modes, umask, the execute bit): Windows has none, and its st_mode is synthesised
POSIX_MODES = os.name != "nt"
HAS_FIFO = hasattr(os, "mkfifo")


def _can_symlink() -> bool:
    """True if this process may create symlinks (on Windows that takes Developer Mode or an admin)."""
    with tempfile.TemporaryDirectory() as directory:
        target = os.path.join(directory, "target")
        open(target, "wb").close()
        try:
            os.symlink(target, os.path.join(directory, "link"))
        except (OSError, NotImplementedError):
            return False
    return True


CAN_SYMLINK = _can_symlink()
