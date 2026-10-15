#!/usr/bin/env python3
"""NomadNet page listing the files this node has pinned (seeding) via RNFS, with optional descriptions.

Anyone can open the page, so it lists only what the operator chose to share and nothing else about the node:
- only complete pins with source "user" or "folder" (pins made from contacts, follows or remote pin
  requests are left off);
- never a sealed (encrypted) file, whose name is meant to stay private: the page checks each file pin's
  first bytes once (`rnfs cat --raw`, a local read) and remembers the answer in its data dir;
- names and descriptions are escaped, so they show literally on one line and can't add Micron markup;
- visitors see a generic message if the list isn't available; the details go to a log in the data dir.
See this page's README.
"""
import json
import os
import shutil
import subprocess
import time
import unicodedata

# Descriptions shown under each file, keyed by CID (a CID never changes, unlike a file name).
DESCRIPTIONS = {
    "bagaibqabciqkq6rth6jhr2vkh7c2ip7p6bsmdxmslbia47rjbmwpcz6wwu6veya": "Marx and Engels, 1848. PDF.",
    "bagaibqabciqh5e4vsvwazopdfypoiflpfjvbfqionjko7cvxsmzmumsdoppzixy": "Alpine Linux 3.24.2 ISOs.",
    "bagaibqabciqaiphoe7qtscjuqprvwpr2ifr3hgnvuk5rgeh53mpk4ntapwutr3a": "MeshChat v2.4.0 release files",
    "bagaibqabciqospcwo756sqjp7yl3h2vukd3ijkvffy4nyh2umljg4yluwp2ce6a": "The Original SCP-106 image",
    "bagaibqabciqd6s6yrwpp6eemv27sa7ud4l3r7rjcyu5oh4fgziopqa432htdxma": "The Reticulum source code taken from GitHub on 11/10/2026",
}

# CIDs to leave off the page.
HIDDEN = {
}

# Pins with no name and no description (e.g. pinned before rnfs recorded names) are left off unless True.
SHOW_UNNAMED = False

# Pin sources that mean the operator chose to share the file (not a contact, a follow or a remote pin).
OPERATOR_SOURCES = {"user", "folder"}

# The page's own data dir, outside NomadNet's pages dir so none of it is served. NomadNet runs pages with a
# minimal environment, so RNFS_PAGE_DIR is only for running the page by hand (and tests).
PAGE_DIR = os.environ.get("RNFS_PAGE_DIR") or os.path.join(
    os.path.expanduser("~"), ".local", "state", "rnfs-pages", "file-index-node")
SEALED_CACHE = os.path.join(PAGE_DIR, "sealed.json")  # {cid: sealed?}, a CID's content never changes
LOG = os.path.join(PAGE_DIR, "page.log")
LOG_MAX = 1024 * 1024  # the log is moved to page.log.1 past this size

# How a sealed (encrypted) file starts (rnfs/content/seal.py).
SEALED_MAGIC = b"rnfs-sealed\n"

# NomadNet may run this without ~/.local/bin on PATH.
RNFS = shutil.which("rnfs") or os.path.expanduser("~/.local/bin/rnfs")


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


def log(message):
    """Append a line to the page's log (never shown to visitors); a log that can't be written is skipped."""
    try:
        os.makedirs(PAGE_DIR, mode=0o700, exist_ok=True)
        if os.path.getsize(LOG) > LOG_MAX:
            os.replace(LOG, LOG + ".1")
    except OSError:
        pass
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message!r}\n")
    except OSError:
        pass


def rnfs(*args):
    """Run an rnfs command; its stdout bytes. Raises RuntimeError (with the CLI's error) if it fails."""
    result = subprocess.run([RNFS, *args], capture_output=True, timeout=20)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode("utf-8", "replace").strip() or f"rnfs {args[0]} failed")
    return result.stdout


def load_pins():
    """This node's complete pins, from the running rnfs daemon."""
    return [pin for pin in json.loads(rnfs("pin", "ls", "--json")) if pin.get("complete")]


_sealed = None  # the sealed-check cache, loaded on first use


def is_sealed(pin):
    """True if the pin is a sealed file, or if that can't be told (then it is left off, and checked again
    next time). A complete pin is stored here, so reading its first bytes never fetches anything."""
    global _sealed
    if pin.get("type") == "directory":
        return False  # rnfs seals single files only; a directory's entry names are in the clear anyway
    if _sealed is None:
        try:
            with open(SEALED_CACHE, encoding="utf-8") as f:
                _sealed = {cid: value for cid, value in json.load(f).items() if isinstance(value, bool)}
        except (OSError, ValueError, AttributeError):
            _sealed = {}
    cid = pin["cid"]
    if cid not in _sealed:
        try:
            _sealed[cid] = rnfs("cat", cid, "--raw", "--length", str(len(SEALED_MAGIC))).startswith(SEALED_MAGIC)
        except Exception as error:
            log(f"checking whether {cid} is sealed failed: {type(error).__name__}: {error}")
            return True
        try:
            os.makedirs(PAGE_DIR, mode=0o700, exist_ok=True)
            with open(SEALED_CACHE + ".tmp", "w", encoding="utf-8") as f:
                json.dump(_sealed, f)
            os.replace(SEALED_CACHE + ".tmp", SEALED_CACHE)
        except OSError as error:
            log(f"saving {SEALED_CACHE} failed: {error}")
    return _sealed[cid]


def shown(pin):
    """True if the pin may be listed: the operator's own (user or folder), not hidden, not sealed."""
    return (bool(OPERATOR_SOURCES & set(pin.get("sources") or ())) and pin["cid"] not in HIDDEN
            and not is_sealed(pin))


def display_name(pin):
    """The pin's name without any local path, so the page doesn't reveal this machine's folders."""
    name = pin.get("name")
    return os.path.basename(name.rstrip("/")) if name else None


def entry(pin):
    """The Micron lines for one pin, or None to leave it off."""
    cid = pin["cid"]
    name = display_name(pin)
    description = DESCRIPTIONS.get(cid)
    if (not name and not description and not SHOW_UNNAMED) or not shown(pin):
        return None
    lines = [f"`!{escape(name or '(unnamed)')}`!"]
    if description:
        lines.append(f"  {escape(description)}")
    lines.append(f"  `F888{escape(cid)}`f")
    return "\n".join(lines)


print("""#!c=0
        ~+                            
                                      
                 *       +            
           '                  |       
       ()    .-.,="''"=.    - o -     
             '=/_       \\     |      
          *   |  '=._    |            
               \\     '=./',        ' 
            .   '=.__.=' ''='      *
   +                         +
        O      *        '       .



This is a NomadNet page listing the files this node currently has pinned (seeding) via `!RNFS (Reticulum Network File System)`!.

RNFS is a decentralized file system built for Reticulum & based on IPFS (InterPlanetary File System).

`B333`!Find out more about 'Reticulum Network File System' at the project's Github:`!`b
`B333`F08f`_`[https://github.com/afit21/rnfs`https://github.com/afit21/rnfs]`_`f`b

---

Any of the following files or folders can be freely accessed using RNFS:
`F888rnfs get <CID> -o <file name>`f
""")

def section(title, pins):
    """Print a heading and its entries, sorted by name; nothing if no entry is shown."""
    entries = [entry(pin) for pin in sorted(pins, key=lambda pin: (display_name(pin) or "").lower())]
    entries = [text for text in entries if text]
    if entries:
        print(f">{title}")
        print("\n\n".join(entries) + "\n")
    return bool(entries)


try:
    pins = load_pins()
except Exception as error:  # the daemon isn't running, rnfs isn't installed, ...
    log(f"listing pins failed: {type(error).__name__}: {error}")
    print("The file list isn't available right now; try again later.")
else:
    folders = [pin for pin in pins if pin.get("type") == "directory"]
    files = [pin for pin in pins if pin.get("type") != "directory"]
    listed = section("Folders", folders)
    listed = section("Files", files) or listed
    if not listed:
        print("Nothing is shared yet.")
