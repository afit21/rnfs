"""Filesystem operations that behave alike on POSIX and Windows. No RNS dependency, so the CLI can use it.

Windows can't delete or replace a file while any process holds it open: Python opens files without
FILE_SHARE_DELETE, and virus scanners and search indexers briefly open new files too. There, replace() and
remove() retry for about a second while the file is in use; elsewhere they are plain os.replace and os.remove.
"""
import os
import time

# The one platform switch for rnfs's Windows behaviour; tests patch it to exercise that behaviour anywhere
IS_WINDOWS = os.name == "nt"

# Opening with os.open on Windows gives a text-mode descriptor (CRLF translation, Ctrl-Z as end of file)
# unless O_BINARY is set; elsewhere the flag doesn't exist and isn't needed
O_BINARY = getattr(os, "O_BINARY", 0)

# Seconds slept between attempts while Windows reports a file in use: about 1 s in all
RETRY_DELAYS = (0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.2, 0.25)

# Device names Windows reserves in every directory, with or without an extension (also as superscript digits)
_RESERVED_NAMES = {"CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$",
                   *(f"{port}{digit}" for port in ("COM", "LPT") for digit in "123456789¹²³")}


def _retrying(operation, *args):
    """operation(*args), retried on Windows while it raises PermissionError; the last attempt's error is raised."""
    if not IS_WINDOWS:
        return operation(*args)
    for delay in RETRY_DELAYS:
        try:
            return operation(*args)
        except PermissionError:
            time.sleep(delay)  # most likely another handle on the file, closed in a moment
    return operation(*args)


def replace(src: str, dst: str):
    """os.replace(src, dst), retried on Windows while either file is in use. Raises OSError."""
    _retrying(os.replace, src, dst)


def rename_new(src: str, dst: str):
    """Rename src (a file or directory) to dst, which must not exist: FileExistsError if it does.

    os.rename alone would replace an existing file (or empty directory) on POSIX. The check and the rename
    aren't atomic, which is fine for outputs only this user's processes write to.
    """
    if os.path.lexists(dst):
        raise FileExistsError(f"{dst} already exists")
    _retrying(os.rename, src, dst)


def remove(path: str) -> bool:
    """Delete the file at path (missing is fine); False if Windows still has it in use after retrying.

    The caller can try again later. Other errors, and PermissionError on other platforms, raise OSError.
    """
    try:
        _retrying(os.remove, path)
    except FileNotFoundError:
        pass
    except PermissionError:
        if IS_WINDOWS:
            return False
        raise
    return True


def windows_name_reason(name: str) -> str | None:
    """Why Windows can't take name as a file name in a directory, or None if it can (on any platform)."""
    if ":" in name:
        return "it contains ':' (a drive or stream on Windows)"
    if any(c in '<>"|?*\\' or ord(c) < 32 for c in name) or name[-1:] in (".", " "):
        return "Windows can't represent it"
    # Windows ignores trailing dots and spaces before matching a device name, so "nul .txt" is NUL too
    if name.split(".")[0].rstrip(" ").upper() in _RESERVED_NAMES:
        return "it is a reserved device name on Windows"
    return None


def printable(text) -> str:
    """text with non-printable characters escaped, so names from other people's DAGs can't drive a terminal or log."""
    text = str(text)
    if text.isprintable():
        return text
    return "".join(c if c.isprintable() else c.encode("unicode_escape").decode("ascii") for c in text)


def unsafe_name_reason(name: str) -> str | None:
    """Why the local filesystem can't take name as a plain file name (it would be read as a path), or None."""
    if name in (".", "..") or "/" in name or "\0" in name:
        return "it would be interpreted as a path"
    if os.sep in name or (os.altsep and os.altsep in name):
        return "it contains a path separator"
    if IS_WINDOWS:
        reason = windows_name_reason(name)
        if reason is not None:
            return reason
    try:
        os.fsencode(name)
    except UnicodeEncodeError:
        return "the filesystem encoding can't represent it"
    return None


def safe_default_name(name, fallback):
    """name if it is fit to be an output name chosen without asking the user, else fallback.

    The name comes from someone else (a DAG entry, a sealed name, an LXMF link), so beyond being one
    plain path component here it must be printable and not hidden: a dotfile such as .bash_login or
    .npmrc written into the current directory unnoticed could change how other programs behave.
    """
    if (not isinstance(name, str) or not name or not name.isprintable() or name.startswith(".")
            or "\\" in name or unsafe_name_reason(name) is not None):
        return fallback
    return name
