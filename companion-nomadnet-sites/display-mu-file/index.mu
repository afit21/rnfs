#!/usr/bin/env python3
"""NomadNet page that shows an RNFS file: pins it on this node and prints its content (e.g. a Micron page).

Link to it with the CID as a link variable, which NomadNet passes in the environment as var_cid:
`[Example`:/page/display-mu-file.mu`cid=bafkreiailvfecuuvt6tctx3owax2jic7y5af2ndriwo6xymcq2uwyut34a]


`B333`!Find out more about 'Reticulum Network File System' at the project's Github:`!`b
`B333`F08f`_`[https://github.com/afit21/rnfs`https://github.com/afit21/rnfs]`_`f`b

"""
import os
import re
import shutil
import subprocess

# Larger files aren't pinned or shown.
MAX_SIZE = 200 * 1024

# How long to wait for each rnfs command; a fetch over a slow link can take a while.
TIMEOUT = 120

# NomadNet may run this without ~/.local/bin on PATH.
RNFS = shutil.which("rnfs") or os.path.expanduser("~/.local/bin/rnfs")

# A bare CIDv1 in base32 (rnfs's form), so nothing else (a path, a nick, an option) reaches rnfs.
CID = re.compile(r"b[a-z2-7]{20,200}")


def escape(text):
    """Escape Micron's formatting characters so text shows literally."""
    return text.replace("\\", "\\\\").replace("`", "\\`")


def rnfs(*args):
    """Run an rnfs command; its result (returncode, stdout bytes, stderr text)."""
    result = subprocess.run([RNFS, *args], capture_output=True, timeout=TIMEOUT)
    return result.returncode, result.stdout, result.stderr.decode("utf-8", "replace").strip()


def fail(message, micron=""):
    """Show message (escaped, so it shows literally) and then micron (as markup) instead of the file, and stop."""
    print("#!c=0")
    print(escape(message))
    if micron:
        print(micron)
    raise SystemExit


def size_of(cid):
    """The file's size in bytes from rnfs stat, or None if it isn't known."""
    code, out, err = rnfs("stat", cid)
    if code != 0:
        fail(f"Couldn't find {cid} ({err or 'rnfs stat failed'}).")
    for line in out.decode("utf-8", "replace").splitlines():
        name, _, value = line.partition(": ")
        if name == "size":
            return int(value) if value.isdigit() else None
    return None


def is_directory(cid):
    """True if the CID is a directory (rnfs ls lists it; for a file it fails with "is not a directory")."""
    code, _, err = rnfs("ls", cid)
    if code != 0 and "is not a directory" not in err:
        fail(f"Couldn't read {cid} ({err or 'rnfs ls failed'}).")
    return code == 0


cid = os.environ.get("var_cid", "").strip()
if not CID.fullmatch(cid):
    fail("No valid RNFS CID was given. Link to this page with `cid=<CID>.",
         "Example: `_`[Example Link`:/page/index.mu`cid=bafkreiailvfecuuvt6tctx3owax2jic7y5af2ndriwo6xymcq2uwyut34a]`_")

try:
    size = size_of(cid)
    if size is None:
        fail(f"The size of {cid} isn't known, so it isn't shown.")
    if size > MAX_SIZE:
        fail(f"{cid} is {size:,} bytes; only files up to {MAX_SIZE // 1024} KB are shown.")
    if is_directory(cid):
        fail(f"{cid} is a folder, not a file.")

    code, _, err = rnfs("pin", "add", cid)
    if code != 0:
        fail(f"Couldn't pin {cid} ({err or 'rnfs pin add failed'}).")

    code, content, err = rnfs("cat", cid)
    if code != 0:
        fail(f"Couldn't read {cid} ({err or 'rnfs cat failed'}).")
except subprocess.TimeoutExpired:
    fail(f"Fetching {cid} took too long; try again later.")
except OSError as error:
    fail(f"RNFS isn't available on this node ({error}).")

try:
    text = content.decode("utf-8")
except UnicodeDecodeError:
    fail(f"{cid} isn't a text file, so it can't be shown here.")
print(text, end="" if text.endswith("\n") else "\n")
