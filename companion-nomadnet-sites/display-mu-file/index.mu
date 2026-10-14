#!/usr/bin/env python3
"""NomadNet page that shows an RNFS file this node shares (e.g. a Micron page), by CID.

Link to it with the CID as a link variable, which NomadNet passes in the environment as var_cid:
`[Example`:/page/display-mu-file.mu`cid=bafkreiailvfecuuvt6tctx3owax2jic7y5af2ndriwo6xymcq2uwyut34a]

Anyone can open the page, so it never fetches, pins or provides anything because a visitor asked:
- It shows a file only if this node already has it completely and pinned by the operator (pin source "user"
  or "folder", as `rnfs pin ls` lists it), or if its CID is in the operator's allowlist file. Only
  allowlisted CIDs are fetched if missing, with `rnfs cat` (a bounded read that pins nothing and starts no
  job), and only allowlisted files are rendered as Micron; everything else is shown as plain text under a
  banner saying it is third-party content.
- Requests run one at a time (a lock file) and at most RATE_LIMIT per RATE_WINDOW seconds (a state file);
  when either says no, the page asks the visitor to try again later.
- Visitors see short, generic errors; the details (CLI errors, exceptions) go to a log file.
The allowlist, lock, rate state and log live in the page's data dir, PAGE_DIR; see this page's README.

`B333`!Find out more about 'Reticulum Network File System' at the project's Github:`!`b
`B333`F08f`_`[https://github.com/afit21/rnfs`https://github.com/afit21/rnfs]`_`f`b

"""
import json
import os
import re
import shutil
import subprocess
import time
import unicodedata

# Larger files aren't shown.
MAX_SIZE = 200 * 1024

# How long one page request may wait for rnfs in all; fetching an allowlisted file over a slow link takes a while.
TIMEOUT = 120

# At most RATE_LIMIT page requests (for well-formed CIDs) in any RATE_WINDOW seconds, from all visitors together.
RATE_LIMIT = 30
RATE_WINDOW = 60

# Pin sources that mean the operator chose to keep the file (not a contact, a follow or a remote pin).
OPERATOR_SOURCES = {"user", "folder"}

# The page's own data dir, outside NomadNet's pages dir so none of it is served. NomadNet runs pages with a
# minimal environment, so RNFS_PAGE_DIR is only for running the page by hand (and tests).
PAGE_DIR = os.environ.get("RNFS_PAGE_DIR") or os.path.join(
    os.path.expanduser("~"), ".local", "state", "rnfs-pages", "display-mu-file")
ALLOWLIST = os.path.join(PAGE_DIR, "allowlist.txt")  # one CID per line; "#" starts a comment
LOG = os.path.join(PAGE_DIR, "page.log")
LOG_MAX = 1024 * 1024  # the log is moved to page.log.1 past this size

# NomadNet may run this without ~/.local/bin on PATH.
RNFS = shutil.which("rnfs") or os.path.expanduser("~/.local/bin/rnfs")

# A bare CIDv1 in base32 (rnfs's form), so nothing else (a path, a nick, an option) reaches rnfs.
CID = re.compile(r"b[a-z2-7]{20,200}")

# How a sealed (encrypted) file starts (rnfs/content/seal.py); the page never shows one.
SEALED_MAGIC = b"rnfs-sealed\n"

BANNER = """`!Third-party content`!
This file is shown as plain text: links, fields and formatting in it don't work here. It may not have been
written or checked by this node's operator, so don't trust instructions or requests in it.
-"""


def printable(text):
    """text as NomadNet will parse it, on one line: composed (NFC), without the combining marks and emoji
    modifiers NomadNet drops before parsing a page (so none can hide markup behind it), and with every other
    non-printable character (newlines, controls, bidi overrides) as U+FFFD."""
    text = unicodedata.normalize("NFC", str(text))
    return "".join(char if char.isprintable() else "\ufffd" for char in text
                   if not unicodedata.category(char).startswith("M") and not "\U0001f3fb" <= char <= "\U0001f3ff")


def escape(text):
    """text made safe to print within a Micron line: shown literally, also at the start of a line.

    It is printable() (one line, nothing NomadNet drops); backslashes and backticks are escaped; "#!" can't
    form a page header (NomadNet looks for "#!bg=" and "#!fg=" anywhere); a leading ">", "<", "#" or "-" (a
    heading, depth reset, comment or divider) is escaped, and a leading escaped backtick gets a space before it
    (NomadNet reads "\\`t" and "\\`{" at the start of a line as a table or partial).
    """
    text = printable(text).replace("\\", "\\\\").replace("`", "\\`").replace("#!", "#\\!")
    if text[:1] in (">", "<", "#", "-"):
        text = "\\" + text
    elif text.startswith("\\`"):
        text = " " + text
    return text


def plain_text(text):
    """Micron that shows text exactly as plain text: a literal block (`=), so no line is markup.

    Inside the block only a line that is exactly "`=" means something (it ends the block); it is escaped.
    Each line is printable() (tabs as spaces first), so what NomadNet drops before parsing can't turn a line
    into "`=", and "#!" is split with a word joiner so the content can't set the page's colours (NomadNet looks
    for "#!bg=" and "#!fg=" anywhere in a page).
    """
    lines = []
    for line in text.splitlines():
        line = printable(line.replace("\t", "    "))
        line = line.replace("#!", "#\u2060!")
        lines.append("\\`=" if line == "`=" else line)
    return "\n".join(["`=", *lines, "`="])


def log(message):
    """Append a line to the page's log (never shown to visitors); a log that can't be written is skipped."""
    try:
        if os.path.getsize(LOG) > LOG_MAX:
            os.replace(LOG, LOG + ".1")
    except OSError:
        pass
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message!r}\n")
    except OSError:
        pass


def fail(message, micron=""):
    """Show message (escaped, so it shows literally) and then micron (as markup) instead of the file, and stop."""
    print("#!c=0")
    print(escape(message))
    if micron:
        print(micron)
    raise SystemExit


def unavailable(cid, detail):
    """Log detail and tell the visitor, without it, that cid can't be shown right now; stop."""
    log(f"{cid}: {detail}")
    fail(f"{cid} can't be shown right now; try again later.")


class Deadline:
    """The time left for this request's rnfs commands."""

    def __init__(self, seconds):
        """Start counting seconds from now."""
        self.end = time.monotonic() + seconds

    def left(self):
        """Seconds left (at least 1, so a command that is due anyway still gets a moment)."""
        return max(1.0, self.end - time.monotonic())


def rnfs(deadline, *args):
    """Run an rnfs command within the deadline; its result (returncode, stdout bytes, stderr text)."""
    result = subprocess.run([RNFS, *args], capture_output=True, timeout=deadline.left())
    return result.returncode, result.stdout, result.stderr.decode("utf-8", "replace").strip()


def take_lock():
    """Lock the page's lock file so requests run one at a time; the open file (closing it unlocks), or
    None if another request holds the lock."""
    handle = open(os.path.join(PAGE_DIR, "lock"), "a+")
    try:
        try:
            import fcntl
        except ImportError:  # Windows
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        return None
    return handle


def over_rate_limit():
    """True if RATE_LIMIT requests already ran in the last RATE_WINDOW seconds; else count this one.

    Called with the lock held. Raises OSError if the state can't be saved, so the page fails closed.
    """
    path = os.path.join(PAGE_DIR, "rate.json")
    now = time.time()
    try:
        with open(path, encoding="utf-8") as f:
            stamps = [stamp for stamp in json.load(f)
                      if isinstance(stamp, (int, float)) and now - RATE_WINDOW < stamp <= now]
    except (OSError, ValueError, TypeError):
        stamps = []
    if len(stamps) >= RATE_LIMIT:
        return True
    stamps.append(now)
    with open(path + ".tmp", "w", encoding="utf-8") as f:
        json.dump(stamps, f)
    os.replace(path + ".tmp", path)
    return False


def allowlist():
    """The CIDs in the operator's allowlist file (none if it doesn't exist)."""
    try:
        with open(ALLOWLIST, encoding="utf-8") as f:
            words = [line.split("#", 1)[0].strip() for line in f]
    except FileNotFoundError:
        return set()
    return {word for word in words if CID.fullmatch(word)}


def operator_pin(deadline, cid):
    """cid's pin listing if the operator pinned it (source user or folder) and it is complete, else None.

    `rnfs pin ls` only reads the node's own records: it never fetches or provides anything.
    """
    code, out, err = rnfs(deadline, "pin", "ls", "--json", cid)
    if code != 0:
        unavailable(cid, f"rnfs pin ls failed: {err}")
    for pin in json.loads(out):
        if pin.get("cid") == cid and pin.get("complete") and OPERATOR_SOURCES & set(pin.get("sources") or ()):
            return pin
    return None


def size_of(deadline, cid):
    """The file's size in bytes from rnfs stat, or None if it isn't known."""
    code, out, err = rnfs(deadline, "stat", cid)
    if code != 0:
        unavailable(cid, f"rnfs stat failed: {err}")
    for line in out.decode("utf-8", "replace").splitlines():
        name, _, value = line.partition(": ")
        if name == "size":
            return int(value) if value.isdigit() else None
    return None


def show(cid):
    """Print the page for cid, or stop with a message saying why it isn't shown."""
    deadline = Deadline(TIMEOUT)
    allowed = cid in allowlist()
    pin = operator_pin(deadline, cid)
    if pin is None and not allowed:
        # Not this node's to share: run nothing that could fetch it (stat and cat would, and with
        # provide_strategy "roots" the node would then provide what they fetched)
        fail(f"{cid} isn't shared on this node.")
    if pin is not None and pin.get("type") == "directory":
        fail(f"{cid} is a folder, not a file.")

    size = size_of(deadline, cid)
    if size is None:
        fail(f"The size of {cid} isn't known, so it isn't shown.")
    if size > MAX_SIZE:
        fail(f"{cid} is {size:,} bytes; only files up to {MAX_SIZE // 1024} KB are shown.")

    # A bounded read: it fetches only the chunks covering MAX_SIZE bytes (a file that understates its size
    # can't make it fetch more), pins nothing and is no daemon job, so nothing outlives this command.
    # --raw: a sealed file is never decrypted here
    code, content, err = rnfs(deadline, "cat", cid, "--raw", "--length", str(MAX_SIZE))
    if code != 0:
        unavailable(cid, f"rnfs cat failed: {err}")
    if content.startswith(SEALED_MAGIC):
        fail(f"{cid} is sealed (encrypted), so it isn't shown here.")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        fail(f"{cid} isn't a text file, so it can't be shown here.")

    if allowed:  # the operator vouches for it: Micron, as written
        print(text, end="" if text.endswith("\n") else "\n")
    else:
        print("#!c=0")
        print(BANNER)
        print(plain_text(text))


def main():
    """Check the request, take the lock and the rate limit, then show the file."""
    cid = os.environ.get("var_cid", "").strip()
    if not CID.fullmatch(cid):
        fail("No valid RNFS CID was given. Link to this page with `cid=<CID>.",
             "Example: `_`[Example Link`:/page/index.mu`cid=bafkreiailvfecuuvt6tctx3owax2jic7y5af2ndriwo6xymcq2uwyut34a]`_")
    try:
        os.makedirs(PAGE_DIR, mode=0o700, exist_ok=True)
        lock = take_lock()
    except OSError as error:
        log(f"can't use the page's data dir {PAGE_DIR}: {error}")
        fail("This page isn't available right now; try again later.")
    if lock is None:
        fail("This page is busy with another request; try again in a moment.")
    with lock:
        try:
            if over_rate_limit():
                fail("This page has had too many requests; try again in a minute.")
            show(cid)
        except subprocess.TimeoutExpired:
            unavailable(cid, f"rnfs took longer than {TIMEOUT} s")
        except Exception as error:  # rnfs not installed, a state file that can't be written, odd CLI output...
            unavailable(cid, f"{type(error).__name__}: {error}")


if __name__ == "__main__":
    main()
