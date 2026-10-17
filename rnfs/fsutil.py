"""Filesystem operations that behave alike on POSIX and Windows. No RNS dependency, so the CLI can use it.

Windows can't delete or replace a file while any process holds it open: Python opens files without
FILE_SHARE_DELETE, and virus scanners and search indexers briefly open new files too. There, replace() and
remove() retry for about a second while the file is in use; elsewhere they are plain os.replace and os.remove.
"""
import errno
import os
import secrets
import stat
import sys
import time

# The one platform switch for rnfs's Windows behaviour; tests patch it to exercise that behaviour anywhere
IS_WINDOWS = os.name == "nt"

# Opening with os.open on Windows gives a text-mode descriptor (CRLF translation, Ctrl-Z as end of file)
# unless O_BINARY is set; elsewhere the flag doesn't exist and isn't needed
O_BINARY = getattr(os, "O_BINARY", 0)
O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)  # Windows lacks it

# Temporary files of atomic writes start with this, so they can't be taken for what they replace (the
# blockstore deletes its own when opened)
PARTIAL_PREFIX = ".partial-"

# Seconds slept between attempts while Windows reports a file in use: about 1 s in all
RETRY_DELAYS = (0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.2, 0.25)

_REPARSE_JUNCTION = getattr(stat, "IO_REPARSE_TAG_MOUNT_POINT", 0xA0000003)
# Reparse tags with this bit are name surrogates: they stand for another file or directory (symlinks,
# junctions, WSL symlinks), unlike placeholders such as OneDrive's that hold the file itself
_REPARSE_NAME_SURROGATE = 0x20000000

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


LOCK_OFFSET = 1 << 30  # Windows: the byte locked, far past what the lock file holds, so its text stays readable


def lock_exclusive(path: str) -> int:
    """Open path (created 0600 if missing) and lock it for this process, without waiting; the descriptor, whose
    lock lasts until it is closed or the process ends (also by a crash), with this process's ID written in the
    file. Raises BlockingIOError if another process holds the lock, OSError if path can't be opened or locked.
    Release it with unlock().

    POSIX: flock (advisory: only processes that lock it too are kept out). Windows: msvcrt.locking of one byte at
    LOCK_OFFSET, past the text.
    """
    fd = os.open(path, os.O_RDWR | os.O_CREAT | O_NOFOLLOW | O_BINARY, 0o600)
    try:
        if IS_WINDOWS:
            import msvcrt
            os.lseek(fd, LOCK_OFFSET, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            except OSError as e:
                # PermissionError (EACCES) when another process holds it (EDEADLOCK in the waiting modes);
                # anything else (a file system that can't lock, say) is no holder's doing
                if e.errno not in (errno.EACCES, errno.EDEADLK):
                    raise
                raise BlockingIOError(errno.EAGAIN, f"{path} is locked by another process") from e
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)  # BlockingIOError when another process holds it
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, f"{os.getpid()}\n".encode("ascii"))
    except BaseException:
        os.close(fd)
        raise
    return fd


def unlock(fd: int):
    """Release lock_exclusive()'s lock on fd, then close fd. Windows may release a closed file's locks only some
    time later, so a lock given up here can be taken again at once; one that won't unlock is left to the close.
    Raises OSError if fd can't be closed."""
    try:
        if IS_WINDOWS:
            import msvcrt
            os.lseek(fd, LOCK_OFFSET, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_UN)
    except OSError:
        pass  # closing releases it too
    finally:
        os.close(fd)


def lock_holder(path: str) -> int | None:
    """The process ID lock_exclusive() wrote in path, or None if it can't be read."""
    try:
        with open(path, "rb") as f:
            text = f.read(32)
        return int(text.strip())
    except (OSError, ValueError):
        return None


def rename_new(src: str, dst: str):
    """Rename src (a file or directory) to dst, which must not exist: FileExistsError if it does.

    os.rename alone would replace an existing file (or empty directory) on POSIX. Atomic where the platform
    allows: Windows' rename never replaces, Linux has renameat2(RENAME_NOREPLACE), and a file can be hard
    linked then unlinked elsewhere. Only where none of these works (a directory outside Linux, a filesystem
    without either) is it a check then a rename, which is fine for outputs only this user's processes write to.
    """
    try:
        if not IS_WINDOWS:
            if _rename_noreplace(src, dst):
                return
            if not os.path.isdir(src):
                try:
                    os.link(src, dst)  # fails if dst exists, atomically
                except FileExistsError:
                    raise
                except OSError:
                    pass  # no hard links here (e.g. FAT): check, then rename
                else:
                    os.remove(src)
                    return
        if os.path.lexists(dst):
            raise FileExistsError
        _retrying(os.rename, src, dst)
    except FileExistsError:
        raise FileExistsError(f"{dst} already exists") from None


_RENAME_NOREPLACE = 1
_AT_FDCWD = -100
_renameat2 = None  # libc's renameat2 once looked up, or False where there is none


def _rename_noreplace(src: str, dst: str) -> bool:
    """renameat2(src, dst, RENAME_NOREPLACE) on Linux; False if the kernel, filesystem or libc lacks it."""
    global _renameat2
    if _renameat2 is None:
        _renameat2 = False
        if sys.platform.startswith("linux"):
            try:
                import ctypes
                function = ctypes.CDLL(None, use_errno=True).renameat2
                function.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
                function.restype = ctypes.c_int
                _renameat2 = function
            except (OSError, AttributeError):
                pass  # glibc before 2.28, or a libc without it
    if not _renameat2:
        return False
    import ctypes
    if _renameat2(_AT_FDCWD, os.fsencode(src), _AT_FDCWD, os.fsencode(dst), _RENAME_NOREPLACE) == 0:
        return True
    error = ctypes.get_errno()
    if error in (errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP):
        return False  # this kernel or filesystem doesn't support the flag
    raise OSError(error, os.strerror(error), src, None, dst)  # FileExistsError for EEXIST


def sync_directory(directory: str):
    """Make renames into directory durable (fsync it), where the filesystem allows. Nothing on Windows, which
    can't open a directory. Raises OSError for a real failure (EIO), not for a filesystem that can't do it."""
    if IS_WINDOWS:
        return
    try:
        fd = os.open(directory, os.O_RDONLY)
    except PermissionError:
        return  # a directory we may write to but not read: the rename stands, just less durably
    try:
        os.fsync(fd)
    except OSError as e:
        if e.errno not in (errno.EINVAL, errno.EOPNOTSUPP, errno.EBADF):
            raise
    finally:
        os.close(fd)


def final_path(fd: int) -> str | None:
    """The path the system holds for the file open as fd, every link and junction resolved; None where the
    platform can't say. Raises OSError if it can but fails.

    What was opened, not where its path leads now: a directory swapped for a link after the open can't change
    it. Linux reads /proc/self/fd, macOS asks fcntl(F_GETPATH) and Windows GetFinalPathNameByHandleW (by the
    real platform, not IS_WINDOWS, which tests patch).
    """
    if os.name == "nt":
        return _windows_final_path(fd)
    if sys.platform.startswith("linux"):
        try:
            path = os.readlink(f"/proc/self/fd/{fd}")
        except FileNotFoundError:
            return None  # no /proc mounted
        return path if path.startswith("/") else None  # e.g. "anon_inode:[...]": not a file at a path
    if sys.platform == "darwin":
        import fcntl
        if hasattr(fcntl, "F_GETPATH"):
            return os.fsdecode(fcntl.fcntl(fd, fcntl.F_GETPATH, bytes(1024)).split(b"\0", 1)[0])
    return None


def strip_windows_prefix(path: str) -> str:
    r"""A path from GetFinalPathNameByHandleW in the form os.path.realpath gives: \\?\C:\x as C:\x and
    \\?\UNC\server\share\x as \\server\share\x."""
    if path.startswith("\\\\?\\UNC\\"):
        return "\\\\" + path[8:]
    if path.startswith("\\\\?\\"):
        return path[4:]
    return path


def _windows_final_path(fd: int) -> str:
    """final_path on Windows: GetFinalPathNameByHandleW on the descriptor's handle, in drive-letter form."""
    import ctypes
    import msvcrt
    from ctypes import wintypes
    function = ctypes.WinDLL("kernel32", use_last_error=True).GetFinalPathNameByHandleW
    function.argtypes = (wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD)
    function.restype = wintypes.DWORD
    handle = msvcrt.get_osfhandle(fd)
    size = 1024
    while True:
        buffer = ctypes.create_unicode_buffer(size)
        length = function(handle, buffer, size, 0)  # 0: FILE_NAME_NORMALIZED | VOLUME_NAME_DOS
        if length == 0:
            raise ctypes.WinError(ctypes.get_last_error())
        if length < size:
            return strip_windows_prefix(buffer.value)
        size = length + 1  # the buffer was too small, and length is what it needs


def is_link(st: os.stat_result) -> bool:
    """True for a symlink, a Windows directory junction (which stat reports as a directory, yet it links) or any
    other Windows reparse point that stands for another file (a name surrogate); st is from lstat."""
    tag = getattr(st, "st_reparse_tag", 0)
    return stat.S_ISLNK(st.st_mode) or tag == _REPARSE_JUNCTION or bool(tag & _REPARSE_NAME_SURROGATE)


def is_below(path: str, root: str) -> bool:
    """True if path is root or inside it; both should be real (resolved) paths. Case-insensitive on Windows."""
    path, root = os.path.normcase(path), os.path.normcase(root)
    try:
        return os.path.commonpath([root, path]) == root
    except ValueError:
        return False  # on different drives, or one relative


def _temp_path(directory: str) -> str:
    """A new, unpredictable name for a temporary file in directory."""
    return os.path.join(directory, PARTIAL_PREFIX + secrets.token_hex(8))


def write_atomic(path: str, data: bytes, *, mode: int = 0o644, durable: bool = True, exclusive: bool = False,
                 private: bool = False):
    """Replace path with data, so a crash or power loss leaves the old or the new file whole. Raises OSError.

    data goes to a new temporary file in path's directory (O_EXCL, plus O_NOFOLLOW where it exists, i.e. not on
    Windows: never an existing file or symlink; created with mode, which the umask can only narrow), synced to
    disk and renamed over path (retried on Windows while path is in use); a symlink at path is replaced, not
    written through. If durable, the
    directory is then synced so the rename survives a power loss too. With exclusive, an existing path is kept
    and FileExistsError raised instead. With private (for keys and the API token, written with mode 0600), on
    Windows, where mode sets no access list, the file also gets a protected DACL for this user and SYSTEM only
    (winacl.make_private) before it takes path's place. On any failure before the rename the temporary file is
    removed and path is left as it was.
    """
    directory = os.path.dirname(path) or "."
    temp = _temp_path(directory)
    # Outside the try: if this fails, whatever is at temp isn't ours to remove
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | O_NOFOLLOW | O_BINARY, mode)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())  # else a power loss can leave path empty once renamed
        if private and IS_WINDOWS:
            from rnfs import winacl
            # By path, once closed (the handle written through can't change its DACL), retried while a virus
            # scanner has the new file open
            _retrying(winacl.make_private, temp)
        if exclusive:
            rename_new(temp, path)
        else:
            replace(temp, path)
    except BaseException:
        try:
            remove(temp)
        except OSError:
            pass
        raise
    if durable:
        sync_directory(directory)


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
    .npmrc written into the current directory unnoticed could change how other programs behave. Nor may
    it start with "-", which a later shell command would read as an option.
    """
    if (not isinstance(name, str) or not name or not name.isprintable() or name.startswith((".", "-"))
            or "\\" in name or unsafe_name_reason(name) is not None):
        return fallback
    return name
