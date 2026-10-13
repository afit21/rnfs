<div align="center">

# 🌍 RNFS - Reticulum Network File System


**A decentralized file system built for [Reticulum](https://reticulum.network) & based on [IPFS](https://ipfs.tech/) (InterPlanetary File System).**

Share files over LoRa radio, Wi-Fi, local networks or the internet without relying on a an authorative server for file hosting or peer lookups.

Access any file on RNFS by it's **'CID'**.

***Example:*** The Communist Manifesto PDF: ```bagaibqabciqkq6rth6jhr2vkh7c2ip7p6bsmdxmslbia47rjbmwpcz6wwu6veya```

[![License: MIT](https://img.shields.io/badge/License-MIT-cyan.svg)](LICENSE)
[![Python 3.8+](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://python.org)
[![Reticulum](https://img.shields.io/badge/Reticulum-Mesh_Network-purple.svg)](https://reticulum.network/)

**Contents**

[Install](#install) · [Sample files](#sample-files) · [Getting started](#getting-started) ·
[How rnfs adapts IPFS](#how-rnfs-adapts-ipfs) · [Settings](#settings) · [Commands](#commands) ·
[Example use cases](#example-use-cases) · [Running several nodes](#running-several-nodes) ·
[Tests](#tests) · [Known limitations](#known-limitations) ·
[specification.md](specification.md) (the protocol) · [design.md](design.md) (the code)

</div>

## Why Use RNFS?

RNFS provides a number of benefits compared to other 'torrenting for RNS' or file hosting projects.

### Key Features

***📃 Share File With A Single Line (CID) -*** To fetch a file, all someone needs it's a 'CID'. For example: The communist Manifesto PDF - ```bagaibqabciqkq6rth6jhr2vkh7c2ip7p6bsmdxmslbia47rjbmwpcz6wwu6veya```

<p align="center">
<img width="500" alt="rnfs-cid" src="https://github.com/user-attachments/assets/01a4b717-9647-4df1-a924-254f92031141" />
</p>

* ***🔖 Names (links that can change) -*** A CID is permanent, but a name can point to a new version. Others can follow a name to keep the latest version pinned, and keepers keep it resolvable while you're offline.
* ***⚡ Multi-source, resumable downloads -*** Pulls pieces from several seeders at once, preferring nearby, fast and long-standing nodes, and picks up where it left off after an interruption.
* ***🌐 Global Namespace -*** CIDs are deterministic hashes of the actual files. No tracker is required like in traditional torrents
* ***🗺️ Distributed Hash Table (Kademlia) -*** The Kademlia algorithm is used to build a distributed dictionary of who has which files. This means you only need to know the CID of a file to download it. Because the global 'phone book' is decentralized, there is zero reliance on trackers, or for the torrent file to be up to date as long as at least one other node has pinned (is seeding) the file.
* ***📃 Nodes automatically split up lookup data between themselves -*** Lookup 'Kademlia' for more in depth information, however all nodes on the RNFS network will take responsibility for remembering a part of the overall 'address book' of nodes and a list of which files they host. Assignment of the information is automatic.
* ***🌱 Automatically find seeders -*** No file of known seeders is needed, RNFS will automatically ask it's peers "who has this file". Kademlia is very efficient at doing this. In a 1 million node network, this would only take 6 hops to find a seeder.

## Quick start

### 1 - Install

Different package options available at [latest release](https://github.com/afit21/rnfs/releases/latest)

**Windows:**

[`rnfs-0.1.3-windows-x64.exe`](https://github.com/afit21/rnfs/releases/download/v0.1.3/rnfs-0.1.3-windows-x64.exe), saved as `rnfs.exe`
  somewhere on your PATH ([how](#windows)).

**Linux:**

One liner for install script:

`curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh`

Or installers for ([Debian & Ubuntu](#debian-and-ubuntu)).  [Flatpak](#other-linux-flatpak)

**macOS:**

`pipx install https://github.com/afit21/rnfs/releases/download/v0.1.3/rnfs-0.1.3-py3-none-any.whl` ([details](#macos)).

### 2 - Start rnfs and leave it running in its own terminal:

```sh
rnfs daemon
```

### 3 - Connect to other nodes. Computers on the same local network or Wi-Fi find each other automatically.
To reach nodes further away, see [Connect to a network](#2-connect-to-a-network).

### 4 - Download a file, for example one of the [sample files](#sample-files):

```sh
rnfs get bagaibqabciqkq6rth6jhr2vkh7c2ip7p6bsmdxmslbia47rjbmwpcz6wwu6veya -o manifesto.pdf
```

Or open it in your browser:
<http://127.0.0.1:8789/rnfs/bagaibqabciqkq6rth6jhr2vkh7c2ip7p6bsmdxmslbia47rjbmwpcz6wwu6veya?filename=manifesto.pdf>

### 5 - Share your own file:

```sh
rnfs add holiday.jpg        # prints its CID: give that to anyone who should have the file
```

That's all you need for the basics. [Getting started](#getting-started) goes a step further (folders,
links that can change, sharing in the background), and [Commands](#commands) lists everything.


## Install

rnfs runs on Linux, macOS and Windows. The easiest way to install it is a file from the
[latest release](https://github.com/afit21/rnfs/releases/latest):

| System | Download | Install |
|---|---|---|
| Windows (64-bit) | [`rnfs-0.1.3-windows-x64.exe`](https://github.com/afit21/rnfs/releases/download/v0.1.3/rnfs-0.1.3-windows-x64.exe) | [Windows](#windows) |
| Debian 12+, Ubuntu 24.04+, Raspberry Pi OS (64-bit) | [`rnfs_0.1.3_amd64.deb`](https://github.com/afit21/rnfs/releases/download/v0.1.3/rnfs_0.1.3_amd64.deb) or [`rnfs_0.1.3_arm64.deb`](https://github.com/afit21/rnfs/releases/download/v0.1.3/rnfs_0.1.3_arm64.deb) | [Debian and Ubuntu](#debian-and-ubuntu) |
| Other Linux (x86_64) | [`rnfs-0.1.3-x86_64.flatpak`](https://github.com/afit21/rnfs/releases/download/v0.1.3/rnfs-0.1.3-x86_64.flatpak) | [Other Linux](#other-linux-flatpak) |
| macOS, or any system with Python 3.11+ | [`rnfs-0.1.3-py3-none-any.whl`](https://github.com/afit21/rnfs/releases/download/v0.1.3/rnfs-0.1.3-py3-none-any.whl) | [macOS](#macos) |

Every package includes Reticulum and LXMF, gives you one `rnfs` command, and keeps your data in the same
places. The release also has the source archive (`rnfs-0.1.3.tar.gz`) and `SHA256SUMS`, to
[check your download](#checking-a-download). These links are for 0.1.3; for a newer version, take the
same files from the [latest release](https://github.com/afit21/rnfs/releases/latest).

### Windows

Download [`rnfs-0.1.3-windows-x64.exe`](https://github.com/afit21/rnfs/releases/download/v0.1.3/rnfs-0.1.3-windows-x64.exe). It is a single file that
includes Python, Reticulum and LXMF; nothing else needs installing. In PowerShell, from your download
folder, put it on your PATH as `rnfs`:

```powershell
$bin = "$env:USERPROFILE\.local\bin"
New-Item -ItemType Directory -Force $bin | Out-Null
Move-Item .\rnfs-0.1.3-windows-x64.exe "$bin\rnfs.exe" -Force
Unblock-File "$bin\rnfs.exe"
[Environment]::SetEnvironmentVariable("Path", [Environment]::GetEnvironmentVariable("Path", "User") + ";$bin", "User")
# open a new terminal, then:
rnfs daemon
```

- **SmartScreen:** the exe isn't code-signed, so Windows may warn you the first time. Choose
  "More info", then "Run anyway".
- **Update:** stop the daemon and replace `rnfs.exe` with the newer release's exe.
- **Uninstall:** delete `rnfs.exe`. Your data in `%USERPROFILE%\.rnfs` stays.
- **Start at logon:** the Task Scheduler script
  [contrib/install-windows-task.ps1](contrib/install-windows-task.ps1) runs the daemon at logon. It
  expects `rnfs.exe` in `%USERPROFILE%\.local\bin`, as above, and logs to
  `%USERPROFILE%\.rnfs\daemon.log`. Add `-Uninstall` to remove the task:
  ```powershell
  powershell -ExecutionPolicy Bypass -File install-windows-task.ps1
  ```

Windows Firewall may ask about network access the first time Reticulum opens an interface. rnfs's own API
and gateway listen only on 127.0.0.1. Windows has no execute bit, so a directory containing executable
scripts gets a different CID when added on Windows than on Linux or macOS. Windows support is newer than
Linux and macOS support, so please report problems.

### Debian and Ubuntu

For Debian 12+, Ubuntu 24.04+, Raspberry Pi OS (64-bit) and other Debian-based systems with Python 3.11
or newer. Ubuntu 22.04 ships Python 3.10, so it isn't supported; use the [install script](#install-script)
there instead. Pick `amd64` or `arm64` to match `dpkg --print-architecture`:

```sh
curl -LO https://github.com/afit21/rnfs/releases/download/v0.1.3/rnfs_0.1.3_amd64.deb
sudo apt install ./rnfs_0.1.3_amd64.deb
rnfs daemon                             # or run it in the background:
systemctl --user enable --now rnfs      # a systemd user service; logs: journalctl --user -u rnfs -f
```

The package bundles Reticulum and LXMF (the tested versions, pinned in `packaging/requirements.lock`) in
`/opt/rnfs`, and installs the `/usr/bin/rnfs` command and a systemd user unit. To keep the service running
while you're logged out: `loginctl enable-linger $USER`.

- **Update:** install the newer release's `.deb` the same way, then `systemctl --user restart rnfs`.
- **Uninstall:** `systemctl --user disable --now rnfs`, then `sudo apt remove rnfs`. Your data in
  `~/.rnfs` stays.

### Other Linux (Flatpak)

For x86_64 Linux distributions without `.deb` support. The Flatpak uses the Flathub
`org.freedesktop.Platform` runtime:

```sh
curl -LO https://github.com/afit21/rnfs/releases/download/v0.1.3/rnfs-0.1.3-x86_64.flatpak
flatpak remote-add --user --if-not-exists flathub https://dl.flathub.org/repo/flathub.flatpakrepo
flatpak install --user rnfs-0.1.3-x86_64.flatpak
flatpak run io.github.afit21.rnfs daemon
alias rnfs='flatpak run io.github.afit21.rnfs'      # add to ~/.bashrc to type just "rnfs"
```

The app has these permissions:

- network access;
- your home folder, for the files you add or get, plus `~/.rnfs` and `~/.reticulum`;
- all devices, for RNode and other serial interfaces. If you don't use serial interfaces, remove this
  with `flatpak override --user --nodevice=all io.github.afit21.rnfs`.

To reach files outside your home folder, run
`flatpak override --user --filesystem=/mnt/data io.github.afit21.rnfs`.

- **Update:** install the newer release's `.flatpak` the same way.
- **Uninstall:** `flatpak uninstall --user io.github.afit21.rnfs`.

On other architectures (e.g. a 32-bit Raspberry Pi), use the [install script](#install-script).

### macOS

Install the wheel from the release with [pipx](https://pipx.pypa.io), which gives rnfs its own environment
and puts the `rnfs` command on your PATH. Homebrew also installs a recent Python:

```sh
brew install pipx
pipx ensurepath                 # then open a new terminal
pipx install https://github.com/afit21/rnfs/releases/download/v0.1.3/rnfs-0.1.3-py3-none-any.whl     # includes Reticulum and LXMF
rnfs daemon
```

The same command works on any system with Python 3.11+, including Linux and Windows.

- **Update:** `pipx install --force <the newer release's .whl URL>`.
- **Uninstall:** `pipx uninstall rnfs`. Your data in `~/.rnfs` stays.
- **Start at login:** install the launchd agent. It runs `~/.local/bin/rnfs daemon` and logs to
  `~/.rnfs/daemon.log`:
  ```sh
  curl -fsSL -o ~/Library/LaunchAgents/io.github.afit21.rnfs.plist \
    https://raw.githubusercontent.com/afit21/rnfs/main/contrib/io.github.afit21.rnfs.plist
  launchctl load -w ~/Library/LaunchAgents/io.github.afit21.rnfs.plist
  ```
  To remove it: `launchctl unload -w ~/Library/LaunchAgents/io.github.afit21.rnfs.plist`.

pipx installs Reticulum and LXMF at the newest versions rnfs allows, not the exact tested versions the
other packages bundle.

### Checking a download

Each release's `SHA256SUMS` lists the checksum of every file. Download it next to your file, then:

```sh
sha256sum -c SHA256SUMS --ignore-missing          # Linux
shasum -a 256 rnfs-0.1.3-py3-none-any.whl         # macOS: compare with the line in SHA256SUMS
```

```powershell
Get-FileHash .\rnfs-0.1.3-windows-x64.exe          # Windows: compare with the line in SHA256SUMS
```

### Other ways to install

The install script installs the latest release; pipx from GitHub and a source checkout install the latest
code from the main branch, which may be newer than the latest release.

#### Install script

Installs the latest release on any Linux distribution. You need Python 3.11 or newer with venv:

- Debian/Ubuntu: `sudo apt install python3 python3-venv`
- Fedora: `sudo dnf install python3`
- Arch: `sudo pacman -S python`

Git isn't needed.

```sh
curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh
```

This installs the latest rnfs release, with Reticulum and LXMF, into its own virtual environment at
`~/.local/share/rnfs/venv`. Dependencies are installed at the versions pinned in the source's
`packaging/requirements.lock`, with their sha256 hashes checked. It links the `rnfs` command into
`~/.local/bin` and tells you if that folder isn't on your PATH. Options go after `sh -s --`:

```sh
curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh -s -- --service    # also run the daemon as a systemd user service
curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh -s -- --update     # update to the latest release
curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh -s -- --main       # install the main branch instead
curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh -s -- --uninstall  # remove it (keeps ~/.rnfs)
```

- **Update:** run the script again, with or without `--update`. It re-installs rnfs from the latest
  release (or with `--main`, the main branch), with the dependency versions pinned there. If you installed
  the service, it restarts.
- **Main branch:** `--main` installs the latest code from the main branch, which may be newer than the
  latest release. Give it again each time you update, or the next update goes back to the latest release.
- **Service:** `--service` installs `~/.config/systemd/user/rnfs.service` and starts it. To see its logs:
  `journalctl --user -u rnfs -f`. To keep it running while you're logged out:
  `loginctl enable-linger $USER`.
- **For all users:** run it as root with `--system`
  (`curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sudo sh -s -- --system`).
  This installs to `/opt/rnfs` and `/usr/local/bin/rnfs`.
- `--python /usr/bin/python3.12` chooses the Python to use.
- `--force` replaces an existing `~/.local/bin/rnfs`, for example one installed by pipx.
- `RNFS_SOURCE=<dir, .zip, .tar.gz, .whl or URL>` installs something other than the latest release (it
  overrides `--main` too). A `.whl` or other pip requirement has no lock file, so its dependencies are
  installed unpinned (newest), with a warning.

#### pipx from GitHub

```sh
pipx install git+https://github.com/afit21/rnfs                                  # Linux, macOS
pipx install https://github.com/afit21/rnfs/archive/refs/heads/main.zip          # Windows (no git needed)
```

Update with `pipx upgrade rnfs`; uninstall with `pipx uninstall rnfs`. Plain `pip install` works too.

#### From source

```sh
git clone https://github.com/afit21/rnfs && cd rnfs
pip install -e .                    # the rnfs command, running your working copy, with Reticulum and LXMF
rnfs daemon
```

To get the tested, hash-checked versions of Reticulum and LXMF, run
`pip install --require-hashes -r packaging/requirements.lock` first. Without `pip install -e .`, install them
that way and use `python3 rnfs.py` (on Windows, `py rnfs.py`) wherever this README says `rnfs`.

**Building the packages yourself.** `.github/workflows/release.yml` does this for releases. By hand:

```sh
python3 -m build                                   # wheel and sdist
sh packaging/build-deb.sh --arch amd64             # or arm64; installs the lock's pinned, hash-checked wheels
pip install --require-hashes -r packaging/requirements.lock && pip install --no-deps . pyinstaller==6.22.3
pyinstaller --clean packaging/pyinstaller/rnfs.spec          # one-file binary for this OS (needs LXMF installed)
python3 -I packaging/update-lock.py rns==X lxmf==Y ...       # bump the pinned dependencies, then:
python3 -I packaging/flatpak/generate-sources.py             # refresh the Flatpak's wheels from the lock
flatpak-builder --user --install --force-clean build-dir packaging/flatpak/io.github.afit21.rnfs.yml
python3 packaging/smoke_test.py dist/rnfs
```

`packaging/requirements.lock` pins every runtime dependency (rns, lxmf, cryptography, cffi, pycparser,
pyserial) with the sha256 of each file PyPI has for it. `update-lock.py` with no arguments picks the newest
versions `pyproject.toml` allows; commit the lock and the regenerated Flatpak sources together.

### Reticulum and where things live

rnfs uses Reticulum's own config directory, the same one `rnsd` and other RNS programs use. That is the
first of `/etc/reticulum`, `~/.config/reticulum` or `~/.reticulum` that exists; on Windows it is
`%USERPROFILE%\.reticulum`. If `rnsd` is running, rnfs attaches to it as a shared instance. Otherwise rnfs
starts Reticulum itself from that config. To use a different config, pass `rnfs daemon --rnsconfig DIR` or
set `reticulum_dir`. Your network interfaces (LAN, TCP, RNode/LoRa…) are set up in Reticulum's `config`
file. See the [Reticulum manual](https://reticulum.network/manual/interfaces.html).

Everything else is in the rnfs directory (`--dir`, default `~/.rnfs/`; on Windows `%USERPROFILE%\.rnfs`).
Uninstalling never deletes it.

| Path | Contents |
|---|---|
| `rnfs.toml` | your settings ([Settings](#settings)) |
| `storage/identity` | the node's RNS identity; your node ID comes from it |
| `storage/lxmf_identity` | a separate identity for your LXMF address |
| `blocks/` | stored blocks |
| `state/` | pins, cache records, provider records, names' sequences, jobs, kept and followed names, contacts, LXMF inbox and outbox |
| `names/` | name keys (back them up: `rnfs name export`) |
| `files/` | the shared folder: anything put here is added, pinned and shared automatically |
| `api_token` | the running daemon's API token |

## Sample files

Some files & Folders that I host as examples for everyone to download. Fetch them with a running daemon that is
connected to a network ([Getting started](#getting-started)):

The Communist Manifesto - bagaibqabciqkq6rth6jhr2vkh7c2ip7p6bsmdxmslbia47rjbmwpcz6wwu6veya (PDF, 715 KB)

Original Doge Photo - bafkreigqte4wbglswc74766w445b67scla7kd2wvrqtp6t7a2ofw32znny

MeshChatv2.4.0 (folder) - bagaibqabciqaiphoe7qtscjuqprvwpr2ifr3hgnvuk5rgeh53mpk4ntapwutr3a

Nasa Apolo 11 'One Small Step' audio mp3 - bagaibqabciqbe75h7g7s4zqozhwrbxbica6krk5iggn7itaoh6t2fh7lnhqpjeq

Reticulum github zip taken on 11/10/2026 - bagaibqabciqd6s6yrwpp6eemv27sa7ud4l3r7rjcyu5oh4fgziopqa432htdxma

Apline Linux 3.24.2 ISOs (folder) - bagaibqabciqh5e4vsvwazopdfypoiflpfjvbfqionjko7cvxsmzmumsdoppzixy

SCP-106 Original Image - bagaibqabciqospcwo756sqjp7yl3h2vukd3ijkvffy4nyh2umljg4yluwp2ce6a

rockyou.txt.zip - bagaibqabciqp26agunewwmavivqgwv3c53xvnxdn7vpz4rdknjlfai72zszhdza

```sh
rnfs get bagaibqabciqkq6rth6jhr2vkh7c2ip7p6bsmdxmslbia47rjbmwpcz6wwu6veya -o ManifestoOftheCommunistParty.pdf
```

The OG SCP-106 Image - bagaibqabciqospcwo756sqjp7yl3h2vukd3ijkvffy4nyh2umljg4yluwp2ce6a (JPEG, 227 KB)

```sh
rnfs get bagaibqabciqospcwo756sqjp7yl3h2vukd3ijkvffy4nyh2umljg4yluwp2ce6a -o scp-106.jpg
```

rockyou.txt in a .zip file - bagaibqabciqp26agunewwmavivqgwv3c53xvnxdn7vpz4rdknjlfai72zszhdza (53 MB; too
big for LoRa)

```sh
rnfs get bagaibqabciqp26agunewwmavivqgwv3c53xvnxdn7vpz4rdknjlfai72zszhdza -o rockyou.txt.zip
```

Or open one in your browser through the local gateway. `?filename=` sets the type and the download name:

```
http://127.0.0.1:8789/rnfs/bagaibqabciqkq6rth6jhr2vkh7c2ip7p6bsmdxmslbia47rjbmwpcz6wwu6veya?filename=ManifestoOftheCommunistParty.pdf
```

## Getting started

### 1. Start the daemon

```sh
rnfs daemon
```

Leave it running, or install it as a service (see [Install](#install)). Every other command talks to it.
`rnfs status` shows your node ID and how many other nodes your node knows (`known_nodes`).

### 2. Connect to a network

rnfs nodes find each other through Reticulum, so your node needs Reticulum interfaces that reach other
rnfs nodes:

- **Local network:** Reticulum's default config has an `AutoInterface`, which finds other Reticulum
  nodes on the same LAN or Wi-Fi with no setup.
- **Over the Internet:** add a `TCPClientInterface` to a Reticulum transport node in
  `~/.reticulum/config`, then restart the daemon:

  ```ini
  [[rnfs entry point]]
    type = TCPClientInterface
    enabled = yes
    target_host = <HOST>        # placeholder: the project's public entry point, once there is one
    target_port = <PORT>
  ```

- **Radio:** add an `RNodeInterface` for LoRa. See the
  [Reticulum manual](https://reticulum.network/manual/interfaces.html).

Your node then learns about other rnfs nodes from their announces, and joins the DHT through the
project's **bootstrap nodes**, which are built in. It contacts them at every start, and again whenever it
knows fewer than 3 nodes, so it also gets in where no announces reach it (for example behind a LoRa access
point), as long as Reticulum can reach them. To join through nodes you choose as well, such as a friend's
(`rnfs status` shows its node ID), give them first:

```sh
rnfs daemon --bootstrap <NODE_ID>          # or bootstrap_nodes = ["<NODE_ID>"] in ~/.rnfs/rnfs.toml
```

`default_bootstrap = false` in `rnfs.toml` stops rnfs contacting the built-in nodes. Private mode never
contacts them.

`rnfs peers` lists the nodes yours knows, nearest first.

### 3. Get a file

```sh
rnfs get bagaibqabciqospcwo756sqjp7yl3h2vukd3ijkvffy4nyh2umljg4yluwp2ce6a -o scp-106.jpg
```

A progress bar shows the rate and the time left. Over LoRa a large file can take hours. Press Ctrl+C to
cancel, or add `--background` to leave it running in the daemon (`rnfs jobs` shows it). Interrupted
transfers continue where they stopped. `rnfs cat <cid>` writes to stdout instead, and `rnfs ls <cid>` lists
a directory.

**In a browser:** open `http://127.0.0.1:8789/rnfs/<cid>`. Directories show a listing, or their
`index.html`, so a folder with a website in it works as a site. Paths work too:
`http://127.0.0.1:8789/rnfs/<cid>/docs/report.pdf`.

If you know a node that has the file, `rnfs get <cid> --from <node ID>` asks it directly, without the DHT.

### 4. Add and share a file

```sh
rnfs add report.pdf             # prints its CID
rnfs add photos/                # a whole directory
rnfs add report.pdf --wrap      # in a directory, so the link keeps its name: <cid>/report.pdf
```

`add` stores the file in your node, **pins** it (keeps it) and announces to the DHT that you have it. Give
the CID to others. They can get it while your node, or any other node that pinned it, is reachable. Files
you drop into `~/.rnfs/files/` are added and shared automatically. `rnfs pin rm <cid>` stops sharing a file,
and `rnfs gc` frees space used by downloads you didn't pin.

### 5. Names: a link that can change

A CID always means the same content. A **name** is a fixed ID that you can point at new CIDs, like a
website address:

```sh
rnfs name create site                   # prints the name (32 hex characters)
rnfs name publish site <cid>            # point it at a CID; again later for a new version
rnfs name resolve <name>                # anyone: what does it point at now?
rnfs contacts add site <name>           # a nickname, so you can type "site" instead of the hex
rnfs get site/index.html                # whatever the name points at now
```

In a browser: `http://127.0.0.1:8789/name/<name>/`. Others can `rnfs name follow <name>` to keep the latest
version pinned. Back up your name key with `rnfs name export site -o site.key`, because only that key can
update the name.

## How rnfs adapts IPFS

rnfs follows IPFS where its design helps, uses Reticulum's own features in place of libp2p, and drops what
only makes sense on IP networks. It is **not** compatible with IPFS. The full comparison is in
[specification.md, Appendix A](specification.md#appendix-a-comparison-with-ipfs).

| IPFS | rnfs |
|---|---|
| CIDs | Same idea. A raw block or one rnfs DAG codec, SHA-256, base32 only. A file of up to 64 KiB is one raw block, with the same CID as that raw block in IPFS. |
| UnixFS files and directories | 64 KiB chunks (optionally FastCDC) in balanced trees, and sorted directory indexes in place of HAMT sharding. The execute bit is kept; mtimes and symlinks are dropped. |
| PeerID, multiaddrs, transports, NAT traversal | A node ID is a Reticulum destination hash. Reticulum routes by hash and encrypts every link. |
| Kademlia DHT | Kademlia for provider records only, with 128-bit keys and `K` = 10. Server and client modes are kept. Peers are found through Reticulum announces, and Sybil resistance comes from seniority. |
| Bitswap | `/blocks` requests over Reticulum Resources. Downloads are multi-source, resumable and verified. Providers are ranked by seniority, then hops and link speed. Responses are sized to the link's measured rate. |
| IPNS | Names are Reticulum announces with a signed record. **Keepers** (any node, with `name keep`) store and re-serve a name's signed record, so it outlives its owner's announces. |
| Pinning, remote pinning, GC | The same, with pin sources (user, folder, name, remote, follow, LXMF). `add --no-pin` adds a file as evictable cache. |
| Reprovider strategies | `roots` (default: what you added or pinned, plus your complete downloads while cached) or `pinned` (only what you added or pinned). |
| CAR files | rnfs archives: `export` / `import`. |
| HTTP gateway | Local gateway with sandboxed pages, plus trustless `?format=raw` and `?format=archive`. |
| MFS | `dir set` / `dir rm` make edited copies of a directory; use a name for the mutable root. |
| Pubsub | Dropped: floods are unaffordable on radio. Use `name follow`, and send links over LXMF (`rnfs send`). |
| Swarm keys (private networks) | Reticulum interface access codes, plus rnfs private mode (members only). |
| DNSLink | Dropped: Reticulum has no DNS. |

## Settings

Settings go in `~/.rnfs/rnfs.toml` (or `<--dir>/rnfs.toml`). Times are in seconds, sizes in bytes and rates
in bits/s. Command-line options override the file. An unknown setting is an error, and every setting is
optional.

```toml
# ~/.rnfs/rnfs.toml
bootstrap_nodes = ["<node ID>"]
dht_mode = "server"               # "client" on phones, laptops and battery nodes
max_storage = 21474836480         # 20 GiB
max_download_size = 1073741824    # refuse fetches over 1 GiB
upload_budget = 5_000_000         # upload at most ~5 MB per hour
no_serve_interfaces = ["RNode"]   # don't upload over the LoRa radio
quiet_hours = "22:00-06:00"
lxmf_auto_fetch_from = ["<a friend's LXMF address>"]
```

**Network and DHT**

| Setting | Default | Description |
|---|---|---|
| `reticulum_dir` | RNS's own | Reticulum config directory (`daemon --rnsconfig`) |
| `bootstrap_nodes` | `[]` | Node IDs to join the DHT through, contacted before the built-in ones (`daemon --bootstrap`) |
| `default_bootstrap` | `true` | Also join through the project's built-in bootstrap nodes. Never used in private mode |
| `dht_mode` | `"server"` | `"server"` stores DHT records for others; `"client"` only asks (for nodes often offline or short of bandwidth) |
| `slow` | `"auto"` | `"yes"`, `"no"` or `"auto"`: a slow node uses one provider at a time and fewer parallel queries. `auto` is slow when every interface is under `slow_bitrate` |
| `slow_bitrate` | `100000` | Links and interfaces below this (bits/s) count as slow |
| `min_rate_sample` | `16384` | Smallest response used to measure a link's rate |
| `unmeasured_bitrate` | `100` | Rate assumed for deadlines before a link is measured; lower it on very slow multi-hop LoRa |
| `alpha` | 3; 1 on slow nodes | Parallel DHT queries per lookup |
| `beta` | `3` | Closest nodes that must answer before a lookup is complete |
| `announce_interval` | `21600` (6 h) | How often the node (and its names) announce; keep it ≥ 1 h |
| `startup_announce_delay` | `30` | Second announce after startup |
| `republish_interval` | 24 h; 3 days on slow nodes | How often provider records are republished; must be under 3.5 days |
| `provide_strategy` | `"roots"` | `"roots"`: share pins and complete downloads while they're cached (up to `max_storage`); `"pinned"`: share only pins, so downloads stay private |
| `provider_seniority` | `86400` (1 day) | Providers known this long are tried first |
| `lookup_timeout` | `600` | Time budget of a DHT lookup |
| `path_timeout` | `30` | Wait for a Reticulum path |
| `max_failures` | `3` | Unanswered queries before a node is dropped; busy refusals before a provider is skipped |
| `link_idle_timeout` | `300` | Close idle links after this |
| `max_incoming_links` | `128` | Incoming links at once |
| `max_records` | `100000` | Provider records stored for others |
| `ban_time` | `3600` | How long a node that sent a bad block is ignored |
| `verbosity` | `0` | Extra logging: 1 per-query detail, 2 RNS debug output (`daemon -v`, `-vv`) |

**Storage**

| Setting | Default | Description |
|---|---|---|
| `max_storage` | `10737418240` (10 GiB) | Soft limit. Cached downloads are evicted (oldest use first) above it, and pins are never evicted. The disk keeps max(256 MiB, 2%) free in any case |
| `max_download_size` | none | Refuse fetches larger than this |
| `max_output_entries` | `100000` | Most directory entries a listing, `get` or export may write |
| `scan_interval` | `60` | How often the shared folder `files/` is scanned |
| `remote_pin_identities` | `[]` | Identity hashes allowed to ask this node to pin (`pin remote add`); a node's is the `identity_hash` in its `rnfs status` |
| `remote_pin_quota` | `1073741824` (1 GiB) | Storage each of those identities may use |

**Transfers**

| Setting | Default | Description |
|---|---|---|
| `max_parallel_providers` | 3; 1 on slow nodes | Providers downloaded from at once |
| `response_target` | `60` | Transfer time each block response aims for |
| `deadline_slack` | `30` | Added to response deadlines |
| `stall_timeout` | `120` | A response with no progress for this long is abandoned |
| `max_blocks_responses` | `4` | Uploads in progress at once (half reserved for slow links; ≥ 2) |
| `max_upload_rate` | none; half the measured rate on slow links | Upload cap per link (bits/s) |
| `max_jobs` | 2; 1 on slow nodes | Transfers (`get`, `pin add`, `export`) running at once; set 1 on a slow radio |
| `max_finished_jobs` | `100` | Finished jobs remembered by `rnfs jobs` (at least 1). A finished job is kept for at least 60 s whatever this says, so a command waiting for it always sees the result |

**Mesh and airtime.** These limit only uploads of content. Lookups, announces and publishing keep working.

| Setting | Default | Description |
|---|---|---|
| `upload_budget` | none | Bytes per hour your node uploads, in total. One requester may hold at most half of it in responses still being sent, and what a failed or cancelled response didn't send is given back |
| `serve_interfaces` | `[]` (all) | Upload only on these Reticulum interfaces (names or prefixes, any case) |
| `no_serve_interfaces` | `[]` | Never upload on these interfaces; wins over `serve_interfaces` |
| `quiet_hours` | `""` (off) | Local times with no uploads, e.g. `"22:00-06:00"` or `"01:00-05:00,12:00-13:00"` |

**Names**

| Setting | Default | Description |
|---|---|---|
| `name_table_size` | `10000` | Records remembered for names you haven't resolved yourself |
| `keep_max_names` | `100` | Names kept or followed for others (your own don't count). Each is a DHT key your node republishes (every 3 days or more) and, if followed, a pin |
| `keeper_query` | `"fallback"` | Ask keepers only when the network had no answer (`"fallback"`), or on every resolve (`"always"`). `always` costs about 10 times the airtime per resolve: keep `fallback` on slow links |

**Content.** No settings: chunking, sealing and `--no-pin` are chosen per `rnfs add`.

**LXMF** (the `lxmf` package is installed with rnfs)

| Setting | Default | Description |
|---|---|---|
| `lxmf_enabled` | on if LXMF is installed | `false` turns LXMF off (`true` or `false` only) |
| `lxmf_display_name` | `"rnfs node"` | Name shown to LXMF contacts (1–64 printable characters). The default says nothing about your node ID |
| `lxmf_send_from_hint` | `true` | Links you send carry `from=<your node ID>`, so recipients can fetch straight from your node when the DHT doesn't know it. This tells everyone who reads the message which rnfs node your LXMF address belongs to; `false` leaves it out |
| `lxmf_propagation_node` | none | Propagation node (32 hex) for `send --propagate`, for resending direct messages that failed, and for the periodic downloads. Without it none of these happen automatically; a propagation node only heard announcing itself is used only with `--nearest` |
| `lxmf_sync_interval` | `3600` | How often waiting messages are downloaded from `lxmf_propagation_node`; 0 never |
| `lxmf_accept_from` | `[]` (everyone) | Only keep messages from these LXMF addresses |
| `lxmf_auto_fetch_from` | `[]` | Senders whose links are fetched and pinned automatically |
| `lxmf_auto_fetch_quota` | `1073741824` (1 GiB) | Storage each auto-fetch sender may use |

The inbox keeps at most 500 messages (4 MiB in all), and at most 50 from any one sender not in
`lxmf_auto_fetch_from`. Messages holding auto-fetched pins aren't pushed out by other people's messages.

**Gateway and API**

| Setting | Default | Description |
|---|---|---|
| `api_port` | `8788` | Local API port (127.0.0.1). Also `--api-port` on every command |
| `gateway_port` | `8789` | Gateway port (127.0.0.1) (`daemon --gateway-port`) |
| `gateway_scripts` | `true` | Let gateway pages run scripts (`daemon --no-gateway-scripts` turns it off) |
| `gateway_fetches` | `2` | Network fetches at once for requests that web pages make (not your own navigation) |

<a id="privacy"></a>**Privacy**

| Setting | Default | Description |
|---|---|---|
| `private` | `false` | Private mode: every link identifies and only `members` are served (`daemon --private`) |
| `members` | `[]` | Identity hashes of the members. `rnfs status` shows a node's `identity_hash` (so does `rnid -i ~/.rnfs/storage/identity`) |

## Commands

Every command except `daemon` talks to the running daemon through its local API. Two global options work
before the command or at the end of it (after the subcommand, for groups such as `name list` or
`contacts ls`):

- `--dir DIR` uses another rnfs directory (default `~/.rnfs`);
- `--api-port N` talks to the daemon on another API port.

`rnfs --version` prints the version. Run commands as the daemon's user: the CLI reads the daemon's secret
token from `<rnfs dir>/api_token`.

A **target** is `<cid>[/path]`, or `<contact>/path` for a name contact (`alice/docs/report.pdf`). Where a
node ID is expected, a node contact's nick works too.

**Daemon and status**

| Command | Description |
|---|---|
| `rnfs daemon [--bootstrap NODE_ID…] [--rnsconfig DIR] [--gateway-port N] [--dht-mode server\|client] [--slow yes\|no\|auto] [--private] [--provide-strategy pinned\|roots] [--no-gateway-scripts] [-v\|-vv]` | Run a node with its API and gateway |
| `rnfs status` | Node ID, identity hash (what others list in `members` and `remote_pin_identities`), known nodes, records, storage, traffic, jobs, LXMF address |
| `rnfs peers [--json]` | Known nodes: DHT mode, seniority, hops, interface, link rate, last heard, traffic, bans |
| `rnfs stats [--json]` | Traffic since start: totals, uploads refused by your limits, per interface |
| `rnfs ping NODE_ID` | Round-trip time to a node |
| `rnfs providers CID` | Find nodes providing a CID |

**Adding and getting content**

| Command | Description |
|---|---|
| `rnfs add PATH\|- [--wrap] [--only-hash] [--name NAME] [--no-pin] [--to RECIPIENT…] [--to-self] [--no-sealed-name] [--chunker fixed\|fastcdc]` | Add a file, directory or stdin (`-`), pin it and print its CID |
| `rnfs get TARGET [-o OUT] [--from NODE_ID] [--background] [--raw]` | Fetch and write out a file or directory (default name: below) |
| `rnfs cat TARGET [--offset N] [--length N] [--raw] [--block]` | Write a file, or part of one, to stdout (fetching only what's needed) |
| `rnfs ls TARGET` | List a directory: kind, size, CID, name |
| `rnfs stat TARGET` | Size, block count and how much is stored locally |
| `rnfs dir set TARGET NAME CID [--exec]` | Make a new directory with an entry added or replaced; prints its CID |
| `rnfs dir rm TARGET/NAME` | Make a new directory with an entry removed |
| `rnfs export TARGET -o FILE [--background]` | Write an archive (for USB sticks, sneakernet) |
| `rnfs import FILE` | Import an archive, pinning its complete roots |

`add` options:

- `--wrap` puts the file in a directory, so links keep its name.
- `--only-hash` prints the CID without storing anything.
- `--name` names stdin, wrapping it in a directory.
- `--no-pin` keeps the file as cache that can be evicted.
- `--to` seals the file so only those recipients can read it. A recipient is a node ID, an LXMF address or
  identity hash that RNS has heard, a 128-hex public key, or a contact. Put `--to` after the path, since it
  takes several values. Sealing works on single files: put a directory in a tar file first.
- `--to-self` also seals the file for you.
- `--no-sealed-name` leaves the file name out of the sealed file.
- `--chunker fastcdc` uses content-defined chunks, so a later edited version shares most blocks.

`get` without `-o` writes into the current directory under the target's last path name, but only if that
name is printable, doesn't start with `.` and is a valid file name here; otherwise under its root CID. So a
link to `.bash_login`, or a name with terminal escapes, is written as `./<cid>`. A sealed file you can open
is written under its sealed name by the same rule. `get` writes under a hidden temporary name beside the
output (`.rnfs-get-….partial`, or `.rnfs-job-<id>.partial` for a job) and renames it when complete, so an
interrupted transfer never leaves a half-written file under the real name. Names from other nodes are
printed and logged with control characters escaped.

`get`/`cat --raw` give a sealed file's stored (encrypted) bytes. `cat --block` gives a CID's own block.

**Pins and storage**

| Command | Description |
|---|---|
| `rnfs pin add TARGET [--background]` | Fetch completely and pin (share) |
| `rnfs pin rm CID` | Remove your pin |
| `rnfs pin ls [CID] [--json]` | Pins, their sources (`user`, `folder`, `name:…`, `remote:…`, `follow:…`, `lxmf:…`) and where each came from: the local path you added it from (`~/notes.txt`, `(stdin)`, `archive …`, the target given to `pin add`), the shared-folder file name, or for a directory its single entry's name or entry count. These names are kept only on your machine; pins made before this was added show none for plain files |
| `rnfs pin remote add NODE_ID CID` / `rnfs pin remote rm NODE_ID CID` | Ask another node to pin or unpin (it must list your identity in `remote_pin_identities`) |
| `rnfs gc` | Evict every cached download |
| `rnfs verify` | Re-check every stored block against its CID |

**Background jobs**

| Command | Description |
|---|---|
| `rnfs jobs` / `rnfs jobs list` | Every transfer: ID, state, progress, rate, time left |
| `rnfs jobs show ID` | Everything about one job |
| `rnfs jobs wait ID` | Wait for a job with a progress bar; fails if the job fails. A get's result keeps the first 100 warnings and their total |
| `rnfs jobs cancel ID` | Cancel a queued or running job |
| `rnfs jobs clear [ID]` | Forget finished jobs (or one) |

```
$ rnfs get bafy…/maps.tar -o maps.tar --background
3fa2b1c9
$ rnfs jobs
3fa2b1c9  running   get    [######--------------]  31%  310 KB of 1.00 MB  280 B/s  ETA 41m04s  bafy…/maps.tar -> /home/me/maps.tar
$ rnfs jobs wait 3fa2b1c9
```

**Names**

| Command | Description |
|---|---|
| `rnfs name create ALIAS` | Create a name key; prints the name |
| `rnfs name list` | Your names, with what they point at |
| `rnfs name publish ALIAS TARGET` | Point a name at a CID (pins it) |
| `rnfs name resolve NAME` | What a name points at |
| `rnfs name rm ALIAS` | Delete a name key and release its pin |
| `rnfs name export ALIAS [-o FILE]` | Write a name's private key (to a file with mode 0600, or a redirect) |
| `rnfs name import ALIAS [FILE]` | Import a name key from a file or stdin |
| `rnfs name keep NAME` / `rnfs name unkeep NAME` | Store and serve someone's name record, so it still resolves while they're offline |
| `rnfs name kept` | Kept names: sequence, CID, why kept |
| `rnfs name follow NAME` / `rnfs name unfollow NAME` | Keep the latest version a name points at pinned, fetched in the background |
| `rnfs name following` | Followed names: pinned CID, latest CID, state |

A `NAME` is 32 hex characters or a name contact's nick. A followed name is checked every
`announce_interval`; a failed check is retried after 5 minutes, then 10, 20 and so on up to
`announce_interval`.

**Contacts**

| Command | Description |
|---|---|
| `rnfs contacts add NICK HEX [--kind name\|node\|lxmf]` | Nickname a name, node ID or LXMF address (kind detected if omitted) |
| `rnfs contacts rm NICK` | Delete a contact |
| `rnfs contacts ls` | List contacts |

Nicks are 1–32 characters of `a-z 0-9 - _ .`, starting with a letter or digit.

**LXMF messaging**

| Command | Description |
|---|---|
| `rnfs lxmf address` | Your LXMF address, to give to friends |
| `rnfs send ADDRESS\|CONTACT TARGET [-m TEXT] [--propagate] [--nearest]` | Send an `rnfs:` link (and text). The target may also be `name/<name>[/path]` or an `rnfs:` URI. `--propagate` sends through `lxmf_propagation_node` for offline recipients. `--nearest`, without `lxmf_propagation_node`, uses the nearest propagation node heard (for `--propagate`, or if direct delivery fails) |
| `rnfs inbox` | Received messages with rnfs links |
| `rnfs inbox get N [--link K] [-o OUT]` | Fetch what message N links to (only its K-th link with `--link`; `-o` for one link only), asking the sender's node first, then the DHT. Default names follow the `get` rule above |
| `rnfs inbox rm N` / `rnfs inbox rm --all` | Remove messages, releasing what they auto-fetched |
| `rnfs outbox` | Sent messages and their delivery state |
| `rnfs lxmf sync [--nearest]` | Download waiting messages from `lxmf_propagation_node` now (`--nearest`: from the nearest one heard, if none is set) |

**Gateway** (`http://127.0.0.1:8789`, browser only, not a command):

- `/rnfs/<cid>[/path]` and `/name/<name>[/path]` serve files and directories (with `index.html` or a
  listing), with byte ranges.
- `?filename=` sets the type and the download name.
- `/rnfs/<cid>?format=raw` returns the block, and `/rnfs/<cid>[/path]?format=archive` returns an archive
  you can `rnfs import`. You can check both against the CID without trusting the gateway. A web page's
  request or a `HEAD` gets an archive only of content stored completely on your node (409 otherwise);
  opening the URL yourself fetches it first.
- Pages are sandboxed. Sealed files are never decrypted by the gateway.

## Example use cases

**Community library over LoRa.** A few well-placed nodes run as DHT servers. The librarian keeps a
directory of documents and maps behind a name and updates it with `dir set` / `name publish`. Each library
node follows the name, so it always holds and shares the latest edition:

```sh
rnfs contacts add library <name>
rnfs name follow library
rnfs ls library                        # what's in it
rnfs get library/maps/region.pdf
```

**Sharing with a friend via LXMF.** Send a link to your friend's Sideband, NomadNet or MeshChat address.
Any client shows it as text, and an rnfs node recognises it:

```sh
rnfs send <their LXMF address> <cid>/report.pdf -m "the report, as promised"
rnfs inbox                              # on their side
rnfs inbox get 1                        # writes ./report.pdf, asking your node first
```

To have a friend's links fetched and pinned automatically, add their address to `lxmf_auto_fetch_from`
(limited by `lxmf_auto_fetch_quota`). For friends who are often offline, set `lxmf_propagation_node` to a
propagation node you trust: messages that can't be delivered directly are then sent through it, and your
node collects messages waiting there every `lxmf_sync_interval`. The links you send name your node
(`from=`), so your friend can fetch from it even if the DHT doesn't know the content yet; set
`lxmf_send_from_hint = false` if your LXMF address shouldn't be linked to your node ID.

<a id="private-group"></a>**Private group.** Seal a file so only chosen people can read it, while any node
can still carry the encrypted blocks:

```sh
rnfs add minutes.pdf --to <alice node ID> bob --to-self
rnfs get <cid>              # recipients get the decrypted file under its original name (or the CID)
```

For a fully closed group, run every node with `private = true` and the same `members` list (their identity
hashes). Members then answer, store records for and serve only each other.

**Offline sneakernet.** Carry content where there's no link at all:

```sh
rnfs export <cid> -o bundle.rnfs        # on a connected node
rnfs import bundle.rnfs                 # on the offline node: verified and pinned
```

**Mirroring a publisher.** Help keep someone's site available even when they're offline. Following pins
the content, and keeping the name means it still resolves through your node:

```sh
rnfs name follow <their name>           # implies keep
rnfs name kept
```

**Field kit with an airtime budget.** A battery node on a shared LoRa channel: a DHT client, one transfer at
a time, limited uploads and silent nights:

```toml
dht_mode = "client"
max_jobs = 1
upload_budget = 2_000_000
quiet_hours = "22:00-06:00"
no_serve_interfaces = ["RNode"]         # upload over TCP/Wi-Fi only
```

Note: when rnfs is attached to a shared `rnsd`, every link looks like it arrives on the local interface,
so interface rules can't tell your radios apart. Let rnfs run Reticulum itself if you need them.

**A website on the mesh.** Put a static site in a directory, add it and publish it under a name:

```sh
rnfs add mysite/                        # prints <cid>
rnfs name create site && rnfs name publish site <cid>
```

Visitors open `http://127.0.0.1:8789/name/<name>/` on their own node. Links inside the site work, and a
new `name publish` updates it for everyone. NomadNet pages can't open `rnfs:` links, so show the link as
text along with the command and gateway URL to use:

```
rnfs:name/<name>/index.html
rnfs get <cid>/index.html --from <your node ID>   or   http://127.0.0.1:8789/name/<name>/
```

## Running several nodes

To run several daemons on one machine, give each its own `--dir`, `--api-port`, `--gateway-port` and
Reticulum config (`--rnsconfig`). Then connect their Reticulum instances:

- The first node's Reticulum `config` has a `TCPServerInterface` with `enable_transport = Yes`.
- The others have a `TCPClientInterface` to it and `share_instance = No`, and start with
  `--bootstrap <first node ID>`.

Pass `--api-port` to talk to a particular daemon:

```sh
rnfs --dir /tmp/a --api-port 9001 daemon --rnsconfig /tmp/a --gateway-port 9101 --bootstrap <hub id>
rnfs --dir /tmp/a --api-port 9001 add somefile
rnfs --dir /tmp/b --api-port 9002 get <cid> -o out
```

ery thread's stack.

## Known limitations

- A name resolves only while its owner, a transport node that remembers its announce, or a keeper is
  reachable. A name can be updated about once an hour.
- Sybil attacks can make a single CID hard to find; `get --from` bypasses the DHT. Data can never be
  forged.
- Sealed files hide content and file name, not size or recipients, and don't prove who sealed them. `get` of
  a directory leaves the sealed files inside it encrypted.
- LXMF: messages over 256 kB are refused, and unsent messages are lost if the daemon restarts. Offline
  delivery and downloads need `lxmf_propagation_node` (or `--nearest` each time).
