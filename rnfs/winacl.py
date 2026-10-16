"""Owner-only access on Windows: an explicit, protected DACL for the rnfs dir and its secret files (audit L7).

POSIX systems get this from mode bits (a 0700 rnfs dir, 0600 key and token files, checked by
node._make_private_dirs and apiclient.read_token), and there everything here does nothing. Windows ignores mode bits,
so a new file just inherits its directory's access list: under the user's profile that is the user,
Administrators and SYSTEM, but elsewhere (another drive, C:\\ProgramData) it usually lets every local user read
the API token, the node's identity and the name keys. So on Windows:

- the daemon creates a new rnfs dir and its subdirectories with a protected DACL (nothing inherited from the
  parent) that grants full control to the current user and SYSTEM only, inherited by everything created below
  it. The DACL is part of the CreateDirectoryW call, so there is no moment when the parent's DACL applies and
  another account could put a file in it; a dir that is new yet not empty is refused all the same. An existing
  rnfs dir that lets anyone else in gets the same DACL (which Windows passes on to what is below it), and one
  that is a junction or symlink is refused;
- the API token, identity and name key files get that DACL explicitly when written (fsutil.write_atomic's
  private); an existing identity or settings file (rnfs.toml) is checked through the handle it is read from
  before use: one owned by another account is refused, one that lets others in is tightened, and a DACL that
  can't be read or tightened stops rnfs;
- every CLI command checks the token file's DACL before trusting it, and the daemon the rnfs dir's at start.

A DACL counts as private if it allows nobody but the current user, SYSTEM and Administrators (who can read any
file anyway); deny entries and audit entries don't matter. The owner must be one of those too, since an owner can
always change the DACL. Reading or setting a DACL that fails is an error, never a pass.

The Windows calls (advapi32 through ctypes) sit behind a small backend so tests can swap in a fake: they patch
fsutil.IS_WINDOWS and `backend`. The ACL and SID formats are parsed and built here in Python, so that part is
tested on any platform. No RNS dependency, so the CLI can use it.
"""
import os
import struct
from typing import NamedTuple

from rnfs import fsutil

SYSTEM = "S-1-5-18"
ADMINISTRATORS = "S-1-5-32-544"
CREATOR_OWNER = "S-1-3-0"  # only a placeholder in inheritable entries: it never matches an account
OWNER_RIGHTS = "S-1-3-4"   # rights of whoever owns the object, which is checked separately
_NAMES = {"S-1-1-0": "Everyone", "S-1-5-11": "Authenticated Users", "S-1-5-32-545": "Users",
          "S-1-5-32-546": "Guests", "S-1-5-7": "Anonymous", "S-1-5-4": "Interactive"}

# ACE types (winnt.h). Allowed entries carry their SID right after the mask; deny, audit and label entries
# restrict or only log, so they can't let anyone in
ACCESS_ALLOWED = 0x0
ACCESS_ALLOWED_CALLBACK = 0x9
_ALLOW_TYPES = {ACCESS_ALLOWED, ACCESS_ALLOWED_CALLBACK}
_HARMLESS_TYPES = {0x1, 0x2, 0x3, 0x6, 0x7, 0x8, 0xA, 0xC, 0xD, 0xE, 0xF, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15}
OBJECT_INHERIT = 0x1
CONTAINER_INHERIT = 0x2
INHERIT_ONLY = 0x8
FILE_ALL_ACCESS = 0x001F01FF
ACL_REVISION = 2
# Security descriptor control bits (winnt.h)
SE_DACL_PRESENT = 0x0004
SE_DACL_PROTECTED = 0x1000
SE_SELF_RELATIVE = 0x8000
_DESCRIPTOR_HEADER = 20  # SECURITY_DESCRIPTOR_RELATIVE: revision, padding, control, four offsets


class Ace(NamedTuple):
    """One access control entry: its type, flags, access mask and SID (None if it carries none rnfs reads)."""

    type: int
    flags: int
    mask: int
    sid: str | None


class Dacl(NamedTuple):
    """An object's owner SID and its DACL's entries; entries None for a NULL DACL (which allows everyone)."""

    owner: str | None
    entries: list[Ace] | None


# The SID and ACL formats (winnt.h): parsing what Windows returns, and building the private DACL

def sid_to_str(data: bytes) -> str:
    """The S-1-... form of a binary SID. Raises ValueError if data isn't one."""
    if len(data) < 8 or data[0] != 1:
        raise ValueError("not a SID")
    count = data[1]
    if len(data) < 8 + 4 * count:
        raise ValueError("truncated SID")
    authority = int.from_bytes(data[2:8], "big")
    subs = struct.unpack_from(f"<{count}I", data, 8)
    return "S-1-" + "-".join(str(n) for n in (authority, *subs))


def sid_length(data: bytes) -> int:
    """Bytes in the binary SID at the start of data."""
    return 8 + 4 * data[1]


def sid_from_str(text: str) -> bytes:
    """The binary form of an S-1-... SID string. Raises ValueError if it isn't one."""
    parts = text.split("-")
    if len(parts) < 3 or parts[0] != "S" or parts[1] != "1" or len(parts) > 3 + 15:
        raise ValueError(f"not a SID: {text}")
    authority, subs = int(parts[2]), [int(p) for p in parts[3:]]
    return bytes([1, len(subs)]) + authority.to_bytes(6, "big") + struct.pack(f"<{len(subs)}I", *subs)


def parse_acl(data: bytes) -> list[Ace]:
    """The entries of a binary ACL. Raises ValueError if it is malformed."""
    if len(data) < 8:
        raise ValueError("truncated ACL")
    _, _, size, count, _ = struct.unpack_from("<BBHHH", data, 0)
    data = data[:size]
    entries = []
    offset = 8
    for _ in range(count):
        if offset + 8 > len(data):
            raise ValueError("truncated ACL")
        kind, flags, ace_size, mask = struct.unpack_from("<BBHI", data, offset)
        if ace_size < 8 or offset + ace_size > len(data):
            raise ValueError("bad ACE size")
        sid = None
        if kind in _ALLOW_TYPES or kind in (0x1, 0xA):  # the plain allow and deny layouts: header, mask, SID
            sid = sid_to_str(data[offset + 8:offset + ace_size])
        entries.append(Ace(kind, flags, mask, sid))
        offset += ace_size
    return entries


def private_acl(user_sid: str, directory: bool) -> bytes:
    """A binary ACL granting full control to user_sid and SYSTEM only, inherited below it if directory."""
    flags = OBJECT_INHERIT | CONTAINER_INHERIT if directory else 0
    entries = b""
    for sid in (user_sid, SYSTEM):
        body = sid_from_str(sid)
        entries += struct.pack("<BBHI", ACCESS_ALLOWED, flags, 8 + len(body), FILE_ALL_ACCESS) + body
    return struct.pack("<BBHHH", ACL_REVISION, 0, 8 + len(entries), 2, 0) + entries


def private_descriptor(user_sid: str, directory: bool) -> bytes:
    """A self-relative security descriptor whose DACL is private_acl, marked protected (nothing inherited from
    the parent), with no owner or group (the creating process's defaults apply), for CreateDirectoryW."""
    acl = private_acl(user_sid, directory)
    control = SE_SELF_RELATIVE | SE_DACL_PRESENT | SE_DACL_PROTECTED
    return struct.pack("<BBHIIII", 1, 0, control, 0, 0, 0, _DESCRIPTOR_HEADER) + acl


def _name(sid: str) -> str:
    """A SID for messages, with the name of a well-known group."""
    return f"{_NAMES[sid]} ({sid})" if sid in _NAMES else sid


def owner_problem(dacl: Dacl, user_sid: str) -> str | None:
    """Why dacl's owner could let others in (it isn't the user, SYSTEM or Administrators), or None."""
    if dacl.owner is None:
        return "it has no owner"
    if dacl.owner not in (user_sid, SYSTEM, ADMINISTRATORS):
        return f"it belongs to {_name(dacl.owner)}"
    return None


def access_problem(dacl: Dacl, user_sid: str) -> str | None:
    """Why dacl lets someone other than the user, SYSTEM and Administrators in, or None if it doesn't.

    Inherit-only entries count too: on a directory they would let others into what is created in it.
    """
    if dacl.entries is None:
        return "it has no access list, so everyone may access it"
    trusted = {user_sid, SYSTEM, ADMINISTRATORS, CREATOR_OWNER, OWNER_RIGHTS}
    for entry in dacl.entries:
        if entry.type in _HARMLESS_TYPES or entry.mask == 0:
            continue
        if entry.type not in _ALLOW_TYPES:
            return f"it has an access entry rnfs can't check (type {entry.type})"
        if entry.sid not in trusted:
            return f"{_name(entry.sid)} may access it"
    return None


def problem(dacl: Dacl, user_sid: str) -> str | None:
    """Why the object with dacl isn't private to user_sid, or None if it is."""
    return owner_problem(dacl, user_sid) or access_problem(dacl, user_sid)


# The Windows calls

class _Windows:
    """advapi32 through ctypes: the current user's SID, an object's owner and DACL, and setting the private DACL."""

    SE_FILE_OBJECT = 1
    OWNER_SECURITY_INFORMATION = 0x1
    DACL_SECURITY_INFORMATION = 0x4
    PROTECTED_DACL_SECURITY_INFORMATION = 0x80000000
    TOKEN_QUERY = 0x8
    TOKEN_USER = 1
    ERROR_ALREADY_EXISTS = 183

    def __init__(self):
        import ctypes
        from ctypes import wintypes
        self.ctypes = ctypes
        advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        p = ctypes.c_void_p
        pp = ctypes.POINTER(ctypes.c_void_p)
        self.get_named = advapi32.GetNamedSecurityInfoW
        self.get_named.argtypes = (wintypes.LPCWSTR, ctypes.c_int, wintypes.DWORD, pp, pp, pp, pp, pp)
        self.get_named.restype = wintypes.DWORD
        self.get_handle = advapi32.GetSecurityInfo
        self.get_handle.argtypes = (wintypes.HANDLE, ctypes.c_int, wintypes.DWORD, pp, pp, pp, pp, pp)
        self.get_handle.restype = wintypes.DWORD
        self.set_named = advapi32.SetNamedSecurityInfoW
        self.set_named.argtypes = (wintypes.LPWSTR, ctypes.c_int, wintypes.DWORD, p, p, p, p)
        self.set_named.restype = wintypes.DWORD
        self.open_token = advapi32.OpenProcessToken
        self.open_token.argtypes = (wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE))
        self.open_token.restype = wintypes.BOOL
        self.token_info = advapi32.GetTokenInformation
        self.token_info.argtypes = (wintypes.HANDLE, ctypes.c_int, p, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD))
        self.token_info.restype = wintypes.BOOL
        self.current_process = kernel32.GetCurrentProcess
        self.current_process.restype = wintypes.HANDLE
        self.close_handle = kernel32.CloseHandle
        self.close_handle.argtypes = (wintypes.HANDLE,)
        self.local_free = kernel32.LocalFree
        self.local_free.argtypes = (p,)
        self.local_free.restype = p

        class SecurityAttributes(ctypes.Structure):
            """SECURITY_ATTRIBUTES (minwinbase.h)."""
            _fields_ = (("nLength", wintypes.DWORD), ("lpSecurityDescriptor", p), ("bInheritHandle", wintypes.BOOL))

        self.SecurityAttributes = SecurityAttributes
        self.create_directory = kernel32.CreateDirectoryW
        self.create_directory.argtypes = (wintypes.LPCWSTR, ctypes.POINTER(SecurityAttributes))
        self.create_directory.restype = wintypes.BOOL
        self._user_sid: str | None = None

    def _error(self, code: int, path: str | None = None) -> OSError:
        """An OSError for a Win32 error code."""
        error = self.ctypes.WinError(code)
        if path is not None:
            error.filename = path
        return error

    def _sid_at(self, address: int) -> str:
        """The S-1-... form of the binary SID at address."""
        head = self.ctypes.string_at(address, 8)
        return sid_to_str(self.ctypes.string_at(address, sid_length(head)))

    def user_sid(self) -> str:
        """The SID of the user this process runs as (its token's user, not its owner default)."""
        if self._user_sid is None:
            from ctypes import wintypes
            ctypes = self.ctypes
            token = wintypes.HANDLE()
            if not self.open_token(self.current_process(), self.TOKEN_QUERY, ctypes.byref(token)):
                raise self._error(ctypes.get_last_error())
            try:
                needed = wintypes.DWORD()
                self.token_info(token, self.TOKEN_USER, None, 0, ctypes.byref(needed))
                buffer = ctypes.create_string_buffer(needed.value)
                if not self.token_info(token, self.TOKEN_USER, buffer, needed, ctypes.byref(needed)):
                    raise self._error(ctypes.get_last_error())
            finally:
                self.close_handle(token)
            # TOKEN_USER starts with SID_AND_ATTRIBUTES, which starts with a pointer to the SID
            self._user_sid = self._sid_at(ctypes.c_void_p.from_buffer(buffer).value)
        return self._user_sid

    def read(self, path: str, fd: int | None = None) -> Dacl:
        """The owner and DACL of the open file fd (if given, so the very file read is checked) or of path."""
        ctypes = self.ctypes
        owner, dacl, descriptor = ctypes.c_void_p(), ctypes.c_void_p(), ctypes.c_void_p()
        info = self.OWNER_SECURITY_INFORMATION | self.DACL_SECURITY_INFORMATION
        refs = (ctypes.byref(owner), None, ctypes.byref(dacl), None, ctypes.byref(descriptor))
        if fd is not None:
            import msvcrt
            code = self.get_handle(msvcrt.get_osfhandle(fd), self.SE_FILE_OBJECT, info, *refs)
        else:
            code = self.get_named(path, self.SE_FILE_OBJECT, info, *refs)
        if code != 0:
            raise self._error(code, path)
        try:
            owner_sid = self._sid_at(owner.value) if owner.value else None
            if not dacl.value:
                return Dacl(owner_sid, None)
            size = struct.unpack("<H", ctypes.string_at(dacl.value + 2, 2))[0]
            return Dacl(owner_sid, parse_acl(ctypes.string_at(dacl.value, size)))
        finally:
            self.local_free(descriptor)

    def set_private(self, path: str, directory: bool):
        """Give path the protected DACL of private_acl (for a directory, Windows applies it below too)."""
        acl = self.ctypes.create_string_buffer(private_acl(self.user_sid(), directory))
        info = self.DACL_SECURITY_INFORMATION | self.PROTECTED_DACL_SECURITY_INFORMATION
        code = self.set_named(path, self.SE_FILE_OBJECT, info, None, None, acl, None)
        if code != 0:
            raise self._error(code, path)

    def create_private_dir(self, path: str) -> bool:
        """Create the directory path with the protected DACL of private_acl from the start (CreateDirectoryW
        with it in its SECURITY_ATTRIBUTES); False if something already exists at path. Raises OSError."""
        ctypes = self.ctypes
        descriptor = ctypes.create_string_buffer(private_descriptor(self.user_sid(), True))
        attributes = self.SecurityAttributes(ctypes.sizeof(self.SecurityAttributes),
                                             ctypes.cast(descriptor, ctypes.c_void_p), False)
        if self.create_directory(path, ctypes.byref(attributes)):
            return True
        code = ctypes.get_last_error()
        if code == self.ERROR_ALREADY_EXISTS:
            return False
        raise self._error(code, path)


backend = None  # the _Windows backend, made at first use; tests replace it with a fake


def _backend():
    """The backend: None outside Windows, which has nothing to call (fsutil.IS_WINDOWS patched by a test that
    gave no fake), so nothing is done there; on Windows itself always the real one."""
    global backend
    if backend is None and os.name == "nt":
        backend = _Windows()
    return backend


# What the rest of rnfs calls: each does nothing outside Windows (fsutil.IS_WINDOWS)

def make_private(path: str, directory: bool = False):
    """On Windows, give path a protected DACL for the current user and SYSTEM only. Raises OSError."""
    acl = _backend() if fsutil.IS_WINDOWS else None
    if acl is not None:
        acl.set_private(path, directory)


def make_private_dir(path: str) -> bool:
    """Create the directory path (its parent must exist), on Windows with a protected DACL for the current user
    and SYSTEM only from the start, elsewhere with mode 0700; False if something already exists at path.
    Raises OSError."""
    acl = _backend() if fsutil.IS_WINDOWS else None
    if acl is not None:
        return acl.create_private_dir(path)
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        return False
    return True


def check_private(path: str, fd: int | None = None) -> str | None:
    """On Windows, why the file open as fd (or at path) isn't private to the current user, or None if it is (and
    always None elsewhere). Raises OSError if its DACL can't be read."""
    acl = _backend() if fsutil.IS_WINDOWS else None
    if acl is None:
        return None
    return problem(acl.read(path, fd), acl.user_sid())


def tighten(path: str, directory: bool = False, fd: int | None = None) -> str | None:
    """On Windows, make an existing path private if its DACL lets others in; what was wrong (None if nothing).

    With fd (path opened for reading), the owner and DACL checked, before and after, are the open file's, so a
    file swapped in at path after the open can't pass for it. Raises PermissionError if it belongs to someone
    else (its owner could open it again) or is still not private, OSError if its DACL can't be read or set.
    """
    acl = _backend() if fsutil.IS_WINDOWS else None
    if acl is None:
        return None
    user = acl.user_sid()
    dacl = acl.read(path, fd)
    reason = owner_problem(dacl, user)
    if reason is not None:
        raise PermissionError(f"{path}: {reason}")
    reason = access_problem(dacl, user)
    if reason is None:
        return None
    acl.set_private(path, directory)
    after = problem(acl.read(path, fd), user)
    if after is not None:
        raise PermissionError(f"{path} is still not private after setting its access list: {after}")
    return reason


def secure_settings(path: str, fd: int):
    """On Windows, check the settings file (rnfs.toml) open as fd before its settings are used: one that another
    account owns is refused, one that lets others in is made private (with a warning, as they could have
    changed it). Its settings choose the Reticulum config, and with it programs rnfs starts, and the API port
    the CLI sends its token to. Raises PermissionError or OSError if it can't be trusted."""
    reason = tighten(path, fd=fd)
    if reason is not None:
        from rnfs.log import LOG_WARNING, log
        log(f"the settings file {path} was accessible to other users ({reason}); made it private to you and "
            f"SYSTEM. Check that its settings are yours", LOG_WARNING)


def outside_profile(path: str) -> bool:
    """On Windows, True if path is outside the user's profile (%USERPROFILE%), whose ACL keeps other users out."""
    if not fsutil.IS_WINDOWS:
        return False
    profile = os.environ.get("USERPROFILE")
    if not profile:
        return False
    return not fsutil.is_below(os.path.realpath(path), os.path.realpath(profile))


def secure_rnfs_dir(root: str, subdirs: tuple[str, ...] = ()):
    """On Windows, node._make_private_dirs: create the rnfs dir root and subdirs (below it) private from the
    start, or check and tighten an existing root, and warn about a root outside the user's profile.

    A new root is created with the protected owner-only DACL in the same call (make_private_dir), and must
    then be empty: anything in it was put there by someone else. An existing one is made private if others
    may access it. A root that is a junction, symlink or other link-like reparse point is refused, as the
    access list checked and set would be its target's. Missing subdirectories get the same DACL when created.

    Raises RnfsError if root is a link, belongs to another account, can't be made private, or was found
    non-empty when new.
    """
    if not fsutil.IS_WINDOWS:
        return
    from rnfs.errors import RnfsError
    from rnfs.log import LOG_NOTICE, LOG_WARNING, log
    path = os.path.abspath(root)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        created = make_private_dir(path)
        if fsutil.is_link(os.lstat(path)):
            raise RnfsError(f"the rnfs dir {root} is a junction or symlink; rnfs uses only a real directory there "
                            f"(give its target as --dir)")
        if created:
            entries = os.listdir(path)
            if entries:
                raise RnfsError(f"the rnfs dir {root} was just created, yet it already holds {len(entries)} "
                                f"entries (e.g. {entries[0]!r}) that someone else put there; delete it and "
                                f"keep it in a folder only you can change")
            reason = check_private(path)
            if reason is not None:
                raise PermissionError(f"{root} is still not private after setting its access list: {reason}")
        else:
            reason = tighten(path, directory=True)
            if reason is not None:
                log(f"made {root} private to you and SYSTEM ({reason})", LOG_NOTICE)
        for directory in subdirs:
            _make_private_below(path, os.path.abspath(directory))
    except OSError as e:
        raise RnfsError(f"couldn't make the rnfs dir {root} private to you: {e}") from None
    if outside_profile(root):
        log(f"the rnfs dir {root} is outside your user profile. rnfs has made it private to you, but accounts "
            f"that can change its parent folder could replace it: keep it in a folder only you can change",
            LOG_WARNING)


def _make_private_below(root: str, directory: str):
    """Create directory (below root) and any missing directories between root and it with make_private_dir.
    Raises OSError (FileExistsError if something other than a directory is in the way)."""
    path = root
    for part in os.path.relpath(directory, root).split(os.sep):
        if part == ".":
            continue
        path = os.path.join(path, part)
        if not make_private_dir(path) and not os.path.isdir(path):
            raise FileExistsError(f"{path} is in the way of a directory")
