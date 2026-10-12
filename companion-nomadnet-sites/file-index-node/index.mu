#!/usr/bin/env python3
"""NomadNet page listing the files this node has pinned (seeding) via RNFS, with optional descriptions."""
import json
import os
import shutil
import subprocess

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

# NomadNet may run this without ~/.local/bin on PATH.
RNFS = shutil.which("rnfs") or os.path.expanduser("~/.local/bin/rnfs")


def escape(text):
    """Escape Micron's formatting characters so names and descriptions show literally."""
    return text.replace("\\", "\\\\").replace("`", "\\`")


def load_pins():
    """This node's complete pins, from the running rnfs daemon."""
    result = subprocess.run([RNFS, "pin", "ls", "--json"], capture_output=True, text=True, timeout=20)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "rnfs pin ls failed")
    return [pin for pin in json.loads(result.stdout) if pin.get("complete")]


def display_name(pin):
    """The pin's name without any local path, so the page doesn't reveal this machine's folders."""
    name = pin.get("name")
    return os.path.basename(name.rstrip("/")) if name else None


def entry(pin):
    """The Micron lines for one pin, or None to leave it off."""
    cid = pin["cid"]
    name = display_name(pin)
    description = DESCRIPTIONS.get(cid)
    if cid in HIDDEN or (not name and not description and not SHOW_UNNAMED):
        return None
    lines = [f"`!{escape(name or '(unnamed)')}`!"]
    if description:
        lines.append(f"  {escape(description)}")
    lines.append(f"  `F888{cid}`f")
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
except Exception as error:
    print(f"The file list isn't available right now ({escape(str(error))}).")
else:
    folders = [pin for pin in pins if pin.get("type") == "directory"]
    files = [pin for pin in pins if pin.get("type") != "directory"]
    shown = section("Folders", folders)
    shown = section("Files", files) or shown
    if not shown:
        print("Nothing is shared yet.")
