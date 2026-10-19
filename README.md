<div align="center">

# 🌍 RNFS - Reticulum Network File System


**A decentralized file system built for [Reticulum](https://reticulum.network) & based on [IPFS](https://ipfs.tech/) (InterPlanetary File System).**

Share files over LoRa radio, Wi-Fi, local networks or the internet without relying on a an authoritative server for file hosting or peer lookups.

Access any file on RNFS by it's **'CID'**.

***Example:*** Moby Dick book .txt file: ```bagaibqabciqfweclrlsfdkqgkhtmbavijftqfkiogrprcntnew5jalakjsykq4q```

[![License: MIT](https://img.shields.io/badge/License-MIT-cyan.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://python.org)
[![Reticulum](https://img.shields.io/badge/Reticulum-Mesh_Network-purple.svg)](https://reticulum.network/)

<hr>

### Publish files that anyone can fetch over Reticulum by it's hash

No centralized servers

<img width="700" alt="rnfs demo" src="https://raw.githubusercontent.com/afit21/rnfs/refs/heads/main/rnfsgifcompressed.gif" />

<hr>

**Contents**

[Install](#install) · [Sample files (with CIDs)](#sample-files) · [Getting started](#getting-started) ·
[How rnfs adapts IPFS](#how-rnfs-adapts-ipfs) · [Settings](#settings) · [Commands](#commands) ·
[Building apps on rnfs](#building-apps-on-rnfs) · [Keeping content off your node](#keeping-content-off-your-node) ·
[Example use cases](#example-use-cases) · [Running several nodes](#running-several-nodes) ·
[Known limitations](#known-limitations) ·
[specification.md](specification.md) (the protocol) · [design.md](design.md) (the code)
</div>

## Why Use RNFS?

RNFS provides a number of benefits compared to other 'torrenting for RNS' or file hosting projects.

### Key Features

***📃 Share File With A Single Line (CID) -*** To fetch a file, all someone needs it's a 'CID'. For example: Moby Dick book .txt file - ```bagaibqabciqfweclrlsfdkqgkhtmbavijftqfkiogrprcntnew5jalakjsykq4q```

<p align="center">
<img width="500" alt="rnfs-cid" src="https://github.com/user-attachments/assets/01a4b717-9647-4df1-a924-254f92031141" />
</p>

* ***🔖 Names (links that can change) -*** A CID is permanent, but a name can point to a new version. Others can follow a name to keep the latest version pinned, and keepers keep it resolvable while you're offline.
* ***⚡ Multi-source, resumable downloads -*** Pulls pieces from several seeders at once, preferring nearby, fast and long-standing nodes, and picks up where it left off after an interruption.
* ***🌐 Global Namespace -*** CIDs are deterministic hashes of the actual files. No tracker is required like in traditional torrents
* ***🗺️ Distributed Hash Table (Kademlia) -*** The Kademlia algorithm is used to build a distributed dictionary of who has which files. This means you only need to know the CID of a file to download it. Because the global 'phone book' is decentralized, there is zero reliance on trackers, or for the torrent file to be up to date as long as at least one other node has pinned (is seeding) the file.
* ***📃 Nodes automatically split up lookup data between themselves -*** Lookup 'Kademlia' for more in depth information, however all nodes on the RNFS network will take responsibility for remembering a part of the overall 'address book' of nodes and a list of which files they host. Assignment of the information is automatic.
* ***🌱 Automatically find seeders -*** No file of known seeders is needed, RNFS will automatically ask it's peers "who has this file". Kademlia is very efficient at doing this. In a 1 million node network, this would only take 6 hops to find a seeder.

# Ways To Use RNFS

* ***GUI Application (Planned)*** - User friendly interface for rnfs coming soon
* ***Standard Install (Command line + HTML gateway)*** - See the [Quick Start](#quick-start) or [Getting started](#getting-started) sections to install this project and get started with RNFS.
* ***Host a NomadNet Gateway*** - See [RNFS NomadNet Gateway Project](https://github.com/afit21/rnfs-nomadnet-gateway)
* ***Project Gutenberg NomadNet Mirror*** - Visit `78d812594409fc12489f3d392974bec9:/page/index.mu` on NomadNet. This is essentially a site that indexes a mirror of Project Gutenberg (libary of over 70k public domain books). It serves files through a gateway rather than RNFS but also provides CIDs.
* ***Public RNFS NomadNet Gateway*** - Access file on RNFS via the nomadnet gateway at `14c5fc21e231343073ff9e64a26d6f5d:/page/index.mu`

# Quick start

This section covers the basics to get you using rnfs.

## 1 - Install

> [!WARNING]
> If you don't already have Reticulum running on your machine, you'll need to configure some interfaces to
> connect to the backbone: see [Connecting to the network](https://reticulum.network/connect.html).

Different package options available at [latest release](https://github.com/afit21/rnfs/releases/latest)

**Windows:**

[`rnfs-0.1.6-windows-x64.exe`](https://github.com/afit21/rnfs/releases/download/v0.1.6/rnfs-0.1.6-windows-x64.exe), saved as `rnfs.exe`
  somewhere on your PATH ([how](#windows)).

**Linux:**

One liner for install script:

`curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh`

To install as a systemctl service, add the service flag:

`curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh -s -- --service`

Or installers for ([Debian & Ubuntu](#debian-and-ubuntu)).  [Flatpak](#other-linux-flatpak)

**macOS:**

`pipx install https://github.com/afit21/rnfs/releases/download/v0.1.6/rnfs-0.1.6-py3-none-any.whl` ([details](#macos)).

## 2 - Start rnfs and leave it running in its own terminal:

```sh
rnfs daemon
```

## 3 - Download a file
for example one of the [sample files](#sample-files):

```sh
rnfs get bagaibqabciqkq6rth6jhr2vkh7c2ip7p6bsmdxmslbia47rjbmwpcz6wwu6veya -o manifesto.pdf
```

Or open it in your browser:
<http://127.0.0.1:8789/rnfs/bagaibqabciqkq6rth6jhr2vkh7c2ip7p6bsmdxmslbia47rjbmwpcz6wwu6veya?filename=manifesto.pdf>

## 4 - Share your own file:

```sh
rnfs add holiday.jpg        # prints its CID: give that to anyone who should have the file
```

That's all you need for the basics. [Getting started](#getting-started) goes a step further (folders,
links that can change, sharing in the background), and [Commands](#commands) lists everything.

# Install

rnfs runs on Linux, macOS and Windows. Every package includes Reticulum and LXMF and gives you one `rnfs`
command. Your data in `~/.rnfs` is never touched by an update or uninstall.

| System | Download |
|---|---|
| Windows (64-bit) | [`rnfs-0.1.6-windows-x64.exe`](https://github.com/afit21/rnfs/releases/download/v0.1.6/rnfs-0.1.6-windows-x64.exe) ([how](#windows)) |
| Debian 12+, Ubuntu 24.04+, Raspberry Pi OS (64-bit) | [`rnfs_0.1.6_amd64.deb`](https://github.com/afit21/rnfs/releases/download/v0.1.6/rnfs_0.1.6_amd64.deb) or [`rnfs_0.1.6_arm64.deb`](https://github.com/afit21/rnfs/releases/download/v0.1.6/rnfs_0.1.6_arm64.deb) ([how](#debian-and-ubuntu)) |
| Other Linux (x86_64) | [`rnfs-0.1.6-x86_64.flatpak`](https://github.com/afit21/rnfs/releases/download/v0.1.6/rnfs-0.1.6-x86_64.flatpak) ([how](#other-linux-flatpak)), or the [install script](#install-script) |
| macOS, or any system with Python 3.11+ | [`rnfs-0.1.6-py3-none-any.whl`](https://github.com/afit21/rnfs/releases/download/v0.1.6/rnfs-0.1.6-py3-none-any.whl) ([how](#macos)) |

These links are for 0.1.6; for a newer version, take the same files from the
[latest release](https://github.com/afit21/rnfs/releases/latest), which also has the source archive
(`rnfs-0.1.6.tar.gz`) and `SHA256SUMS` ([checking a download](#checking-a-download)).

## Windows

The exe is a single file with Python, Reticulum and LXMF inside. In PowerShell, from your download folder:

```powershell
$bin = "$env:USERPROFILE\.local\bin"
New-Item -ItemType Directory -Force $bin | Out-Null
Move-Item .\rnfs-0.1.6-windows-x64.exe "$bin\rnfs.exe" -Force
Unblock-File "$bin\rnfs.exe"
[Environment]::SetEnvironmentVariable("Path", [Environment]::GetEnvironmentVariable("Path", "User") + ";$bin", "User")
# open a new terminal, then:
rnfs daemon
```

- The exe isn't code-signed: if SmartScreen warns you, choose "More info", then "Run anyway".
- **Update:** stop the daemon and replace `rnfs.exe`. **Uninstall:** delete it.
- **Start at logon:** download [contrib/install-windows-task.ps1](contrib/install-windows-task.ps1) and run
  `powershell -ExecutionPolicy Bypass -File install-windows-task.ps1` (`-Uninstall` removes it). It logs to
  `%USERPROFILE%\.rnfs\daemon.log`.
- Windows has no execute bit, so a directory with executable scripts gets a different CID than on Linux or
  macOS. Windows support is the newest, so please report problems.

## Debian and Ubuntu

Pick `amd64` or `arm64` to match `dpkg --print-architecture`. Ubuntu 22.04 has Python 3.10, which is too old;
use the [install script](#install-script) there.

```sh
curl -LO https://github.com/afit21/rnfs/releases/download/v0.1.6/rnfs_0.1.6_amd64.deb
sudo apt install ./rnfs_0.1.6_amd64.deb
systemctl --user enable --now rnfs      # run it as a service; logs: journalctl --user -u rnfs -f
loginctl enable-linger $USER            # keep it running while you're logged out
```

- **Update:** install the newer `.deb` the same way, then `systemctl --user restart rnfs`.
- **Uninstall:** `systemctl --user disable --now rnfs && sudo apt remove rnfs`.

## Other Linux (Flatpak)

```sh
curl -LO https://github.com/afit21/rnfs/releases/download/v0.1.6/rnfs-0.1.6-x86_64.flatpak
flatpak remote-add --user --if-not-exists flathub https://dl.flathub.org/repo/flathub.flatpakrepo
flatpak install --user rnfs-0.1.6-x86_64.flatpak
alias rnfs='flatpak run io.github.afit21.rnfs'      # add to ~/.bashrc
rnfs daemon
```

The app can use the network, `~/.rnfs` and `~/.reticulum`, and your Downloads folder: not the rest of your home
folder, nor any device. Grant more with `flatpak override --user --device=all io.github.afit21.rnfs` (RNode and
other serial interfaces) or `flatpak override --user --filesystem=<folder> io.github.afit21.rnfs` (another
folder, or `~/.config/reticulum` if your Reticulum config is there); `--reset` takes them back. Until 0.1.6 the app had your whole home folder and every
device: after updating, grant the ones you used again. Update by installing the newer `.flatpak`; uninstall
with `flatpak uninstall --user io.github.afit21.rnfs`.

## macOS

```sh
brew install pipx && pipx ensurepath          # then open a new terminal
pipx install https://github.com/afit21/rnfs/releases/download/v0.1.6/rnfs-0.1.6-py3-none-any.whl
rnfs daemon
```

- **Update:** `pipx install --force <the newer .whl URL>`. **Uninstall:** `pipx uninstall rnfs`.
- **Start at login:**
  ```sh
  curl -fsSL -o ~/Library/LaunchAgents/io.github.afit21.rnfs.plist \
    https://raw.githubusercontent.com/afit21/rnfs/main/contrib/io.github.afit21.rnfs.plist
  launchctl load -w ~/Library/LaunchAgents/io.github.afit21.rnfs.plist
  ```
- pipx (and plain `pip install`) resolves rnfs's dependencies unpinned, at the newest Reticulum, LXMF and
  cryptography rnfs allows, not at the tested versions and hashes in `packaging/requirements.lock` that the
  other packages and the install script use.

## Install script

For any Linux with Python 3.11+ and venv (`sudo apt install python3 python3-venv` on Debian/Ubuntu). It
installs the latest release into `~/.local/share/rnfs/venv`, with the dependency versions and hashes pinned in
the release, and checks the download against `SHA256SUMS` and, if the GitHub CLI is installed, its signed
provenance for that release's tag. If `gh` is installed but not logged in, it stops: run `gh auth login`, or set
`RNFS_REQUIRE_ATTESTATION=0` to check the checksum only. Without `gh` it warns (`RNFS_REQUIRE_ATTESTATION=1` makes
it stop): `curl | sh` without `gh` trusts whoever can write the release's files, since `SHA256SUMS` comes from
the same place.

```sh
curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh                   # install or update
curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh -s -- --service    # also run it as a systemd user service
curl -fsSL https://raw.githubusercontent.com/afit21/rnfs/main/install.sh | sh -s -- --uninstall  # remove it (keeps ~/.rnfs)
```

- Running it again updates rnfs and restarts the service if you installed it.
- `--main` installs the main branch instead (no checksum to check; give it on every update).
- `--system`, as root, installs for all users in `/opt/rnfs` and `/usr/local/bin/rnfs`.
- `--python PATH` picks the Python; `--force` replaces another `rnfs` command (e.g. pipx's);
  `RNFS_SOURCE=<dir, archive, .whl or URL>` installs something other than the latest release (nothing checks
  it; a plain `http://` URL gets a warning).
- With the service, `loginctl enable-linger $USER` keeps it running while you're logged out. More sandboxing
  (`ProtectSystem`, `PrivateTmp`) needs unprivileged user namespaces, so it is an optional drop-in, for this
  unit or the `.deb`'s: see [`contrib/rnfs.service.d/hardening.conf`](contrib/rnfs.service.d/hardening.conf).

## Checking a download

Each release's `SHA256SUMS` lists every file's checksum. With it next to your download:

```sh
sha256sum -c SHA256SUMS --ignore-missing                  # Linux; macOS: shasum -a 256 <file>
gh attestation verify rnfs_0.1.6_amd64.deb --repo afit21/rnfs
```

A matching checksum shows the file is whole, not who made it. The signed build provenance, checked with the
[GitHub CLI](https://cli.github.com/) (2.49 or newer, logged in), shows it was built by this repository's
release workflow. On Windows, `Get-FileHash .\rnfs-0.1.6-windows-x64.exe` gives the checksum.

## From source

```sh
git clone https://github.com/afit21/rnfs && cd rnfs
pip install --require-hashes -r packaging/requirements.lock   # the tested Reticulum and LXMF (optional)
pip install -e .
rnfs daemon
python3 -m unittest discover tests                             # the tests
```

`pipx install git+https://github.com/afit21/rnfs` installs the main branch without a checkout. The release
packages are built by `.github/workflows/release.yml`; the scripts it uses are in `packaging/`. To work on rnfs
itself (the tests and their options, lint, the locks, adding a schema step or an API route, releasing), see
[CONTRIBUTING.md](CONTRIBUTING.md).

## Reticulum and where things live

rnfs uses Reticulum's own config directory (`/etc/reticulum`, `~/.config/reticulum` or `~/.reticulum`;
`%USERPROFILE%\.reticulum` on Windows), and attaches to `rnsd` if it is running. Your network interfaces
(LAN, TCP, RNode/LoRa…) are set up in its `config` file: see the
[Reticulum manual](https://reticulum.network/manual/interfaces.html). `daemon --rnsconfig DIR` uses another.

Everything else is in the rnfs directory (`--dir`, default `~/.rnfs/`), which only you can read:

| Path | Contents |
|---|---|
| `rnfs.toml` | your [settings](#settings) |
| `storage/identity`, `storage/lxmf_identity` | the node's identity (your node ID comes from it) and your LXMF identity |
| `blocks/` | stored blocks, one file per block in 1,024 subdirectories (`blocks/<xy>/<cid>`) |
| `state/rnfs.db` | pins, records, jobs, names' sequences, contacts, the LXMF inbox and outbox, deny lists, the stored blocks' sizes (SQLite; an older version's JSON files are imported once and kept as `*.migrated`) |
| `names/` | name keys: back them up with `rnfs name export` |
| `files/` | the shared folder: anything put here is added, pinned and shared |
| `api_token`, `daemon.lock` | the running daemon's API token and lock |
| `debug.log` | with `debug_log` on, this run's debug log (`debug.log.1`: the previous run's) |

Keep `files/` writable only by you. rnfs skips links and files with several hard links in it, so copy files in
rather than linking them. rnfs refuses an identity or `rnfs.toml` owned by another account.

# Sample files

Some files & folders that I host as examples for everyone to download. Fetch them with a running daemon that is
connected to a network ([Getting started](#getting-started)):

*Moby Dick book .txt file* - bagaibqabciqfweclrlsfdkqgkhtmbavijftqfkiogrprcntnew5jalakjsykq4q

*The Communist Manifesto* - bagaibqabciqkq6rth6jhr2vkh7c2ip7p6bsmdxmslbia47rjbmwpcz6wwu6veya (PDF, 715 KB)

*Original Doge Photo* - bafkreigqte4wbglswc74766w445b67scla7kd2wvrqtp6t7a2ofw32znny

*MeshChatv2.4.0 (folder)* - bagaibqabciqaiphoe7qtscjuqprvwpr2ifr3hgnvuk5rgeh53mpk4ntapwutr3a

*Nasa Apolo 11 'One Small Step' audio mp3* - bagaibqabciqbe75h7g7s4zqozhwrbxbica6krk5iggn7itaoh6t2fh7lnhqpjeq

*Reticulum github zip taken on 11/10/2026* - bagaibqabciqd6s6yrwpp6eemv27sa7ud4l3r7rjcyu5oh4fgziopqa432htdxma

*Apline Linux 3.24.2 ISOs (folder)* - bagaibqabciqh5e4vsvwazopdfypoiflpfjvbfqionjko7cvxsmzmumsdoppzixy

*SCP-106 Original Image* - bagaibqabciqospcwo756sqjp7yl3h2vukd3ijkvffy4nyh2umljg4yluwp2ce6a

*rockyou.txt.zip* - bagaibqabciqp26agunewwmavivqgwv3c53xvnxdn7vpz4rdknjlfai72zszhdza

Fetch one with `rnfs get <cid> -o <output file name>`, or open it in your browser through the local gateway
(`?filename=` sets the type and the download name):

```sh
rnfs get bagaibqabciqospcwo756sqjp7yl3h2vukd3ijkvffy4nyh2umljg4yluwp2ce6a -o scp-106.jpg
```

```
http://127.0.0.1:8789/rnfs/bagaibqabciqkq6rth6jhr2vkh7c2ip7p6bsmdxmslbia47rjbmwpcz6wwu6veya?filename=ManifestoOftheCommunistParty.pdf
```

# Getting started

### 1. Run the daemon and connect it

`rnfs daemon` runs your node; leave it running (or install it as a service), since every other command
talks to it. Your node needs Reticulum interfaces that reach other nodes:

- **Local network:** Reticulum's default `AutoInterface` finds nodes on the same LAN or Wi-Fi with no setup.
- **Internet:** add an interface to a public transport node to your Reticulum `config`; see
  [reticulum.network/connect.html](https://reticulum.network/connect.html) for the public backbone.
- **Radio:** add an `RNodeInterface` for LoRa ([Reticulum manual](https://reticulum.network/manual/interfaces.html)).

Your node then joins the DHT through the project's built-in bootstrap nodes and the rnfs nodes it hears
announcing. `rnfs status` shows your node ID and `known_nodes`; `rnfs peers` lists the nodes it knows. To also
join through a node you choose, run `rnfs daemon --bootstrap <NODE_ID>` (or set `bootstrap_nodes`);
`default_bootstrap = false` stops it contacting the built-in ones.

### 2. Get files

```sh
rnfs get <cid> -o out.pdf               # a progress bar; Ctrl+C cancels, and a later get resumes
rnfs get <cid>/docs/ --background       # leave it running in the daemon: rnfs jobs
rnfs ls <cid>                           # list a directory; rnfs cat <cid> writes to stdout
rnfs get <cid> --from <node ID>         # ask a node you know has it, without the DHT
```

In a browser, `http://127.0.0.1:8789/rnfs/<cid>/` shows a directory's listing or its `index.html`, so a folder
with a website in it works as a site. `rnfs url <cid>` prints its address on a subdomain of its own,
`http://k….rnfs.localhost:8789/`, where its pages can also read its files with `fetch()` (names:
`http://<name>.name.localhost:8789/`).

### 3. Share files

```sh
rnfs add report.pdf             # prints its CID; give that to others
rnfs add photos/                # a whole directory
rnfs add report.pdf --wrap      # in a directory, so the link keeps its name: <cid>/report.pdf
```

`add` stores and **pins** the file and tells the DHT you have it. Files dropped into `~/.rnfs/files/` are
shared automatically. `rnfs pin rm <cid>` stops sharing, and `rnfs gc` frees space used by downloads you
didn't pin. By default your node also shares what you download while it's cached, so others can see what you
fetched; `provide_strategy = "pinned"` shares only what you pin. Every file and folder inside what you pin is also
findable by its own CID (`provide_entries`), so a file you shared in a folder can be fetched without the folder's
CID; on a slow radio node, only while there are few of them.

### 4. Names: a link that can change

A CID always means the same content. A **name** is a fixed ID you can point at new CIDs:

```sh
rnfs name create site                   # prints the name (32 hex characters)
rnfs name publish site <cid>            # again later for a new version
rnfs name sync site ~/site              # or keep it pointing at a folder, published as it changes
rnfs contacts add site <name>           # a nickname, so others can type "site"
rnfs get site/index.html                # whatever the name points at now
rnfs name follow <name>                 # keep the latest version pinned
```

In a browser: `http://127.0.0.1:8789/name/<name>/`. Back up the key with `rnfs name export site -o site.key`:
only it can update the name.

# How rnfs adapts IPFS

rnfs follows IPFS where its design helps, uses Reticulum in place of libp2p, and drops what only makes sense on
IP networks. It is **not** compatible with IPFS. The full comparison is in
[specification.md, Appendix A](specification.md#appendix-a-comparison-with-ipfs).

| IPFS | rnfs |
|---|---|
| CIDs | Same idea: a raw block or one rnfs DAG codec, SHA-256, base32. A file of up to 64 KiB has the same CID as in IPFS |
| UnixFS | 64 KiB chunks (optionally FastCDC) in balanced trees, sorted directory indexes. The execute bit is kept; mtimes and symlinks aren't |
| PeerID, transports, NAT traversal | A node ID is a Reticulum destination hash; Reticulum routes and encrypts |
| Kademlia DHT | For provider records only, `K` = 10, server and client modes, Sybil resistance from seniority |
| Bitswap | Multi-source, resumable, verified `/blocks` requests over Reticulum, sized to each link's measured rate |
| IPNS | Signed records in Reticulum announces, re-served by **keepers** so a name outlives its owner's announces |
| Pinning, remote pinning, GC | The same, with pin sources (user, folder, name, remote, follow, LXMF) |
| CAR files, HTTP gateway | `export` / `import` archives; a local gateway with sandboxed pages and trustless `?format=raw` / `archive` |
| MFS | `dir set` / `dir rm` make edited copies; a name is the mutable root |
| Pubsub, DNSLink | Dropped (floods are unaffordable on radio; Reticulum has no DNS). Use `name follow` and LXMF links |
| Private networks | Reticulum interface access codes, plus rnfs private mode (members only) |

# Settings

Settings go in `~/.rnfs/rnfs.toml`; command-line options override them. Times are in seconds, sizes in bytes,
rates in bits/s, and an unknown setting is an error (one a newer version retired, such as `max_records`, is ignored
with a warning). These are the ones you're most likely to change; every
setting and its default is in [specification.md, section 12](specification.md#12-constants).

```toml
# ~/.rnfs/rnfs.toml
dht_mode = "client"               # on phones, laptops and battery nodes
max_storage = 21474836480         # 20 GiB
provide_strategy = "pinned"       # don't share your downloads
upload_budget = 5_000_000         # upload at most ~5 MB per hour
no_serve_interfaces = ["RNode"]   # don't upload over the LoRa radio
quiet_hours = "22:00-06:00"
lxmf_accept_from = ["<a friend's LXMF address>"]
```

| Setting | Default | What it does |
|---|---|---|
| `dht_mode` | `"server"` | `"client"` only asks the DHT, for nodes often offline or short of bandwidth |
| `bootstrap_nodes`, `default_bootstrap` | `[]`, `true` | Node IDs to join through first; whether to also use the built-in ones |
| `slow` | `"auto"` | A slow node uses one provider at a time and fewer parallel queries; `auto` when every interface is under `slow_bitrate` (100 kbit/s) |
| `announce_interval` | `21600` (6 h) | How often the node and its names announce; keep it ≥ 1 h |
| `provide_strategy` | `"roots"` | `"roots"` shares pins and every complete download while it's cached (even a `cat` or `stat`), so **anyone can learn what you downloaded**; `"pinned"` shares only pins |
| `provide_entries` | unset | Also share every file and folder inside your pins under its own CID (not inside downloads). Unset: on, for up to 100,000 of them, oldest pins first; on a slow node only while republishing them would take under 2% of the radio channel (about 40 keys in all on a large network: `rnfs status` says when they're off and why). `true` or `false` overrides it. Each one tells anyone who looks that your node has that file |
| `max_storage` | 10 GiB | Cached downloads are evicted above this; pins never are. The disk keeps max(256 MiB, 2%) free |
| `max_download_size` | none | Refuse fetches larger than this |
| `record_budget` | 1 GiB | Disk the provider records your node stores for others (as a DHT server) may take; when full, your node contacts and publishers seen for days keep their place, and strangers share a small part |
| `max_jobs` | 2; 1 on slow nodes | Transfers running at once |
| `upload_budget` | none | Bytes per hour your node uploads in total |
| `serve_interfaces`, `no_serve_interfaces` | all, none | Reticulum interfaces to upload on, or never upload on (names or prefixes) |
| `quiet_hours` | off | Local times with no uploads, e.g. `"22:00-06:00"` |
| `remote_pin_identities`, `remote_pin_quota` | `[]`, 1 GiB | Identities allowed to ask this node to pin (`pin remote add`), and the storage each may use |
| `deny_lists` | `[]` | [Deny lists](#keeping-content-off-your-node) to apply |
| `keep_max_names` | `100` | Names you keep or follow for others |
| `follow_quota` | 1 GiB | Storage each followed name's target may use; a larger one isn't fetched and the previous one stays pinned (up to twice this while it changes). `0` stops fetching followed names |
| `lxmf_accept_from` | everyone | Only keep LXMF messages from these addresses (recommended if you only expect links from people you know). Every `lxmf_*` setting is deprecated: [LXMF is moving to its own app](#lxmf-is-moving-out) |
| `lxmf_auto_fetch_from`, `lxmf_auto_fetch_quota` | `[]`, 1 GiB | Senders whose links are fetched and pinned automatically, and the storage each may use |
| `lxmf_propagation_node` | none | Propagation node for offline delivery (`send --propagate`) and periodic downloads |
| `lxmf_send_from_hint` | `true` | Links you send carry your node ID, so recipients can fetch from you; this links your LXMF address to your node |
| `api_port`, `gateway_port` | `8788`, `8789` | Local API and gateway ports (127.0.0.1 only) |
| `gateway_scripts` | `true` | Let gateway pages run scripts |
| `debug_log` | `false` | Write `~/.rnfs/debug.log` (readable by you only) for diagnosing problems (`daemon --debug-log`) |
| `verbosity` | `0` | Extra logging: 1 per-query detail, 2 RNS debug output (`daemon -v`, `-vv`) |

**Changing settings while the daemon runs.** `rnfs config` lists every setting with the value the daemon runs
with (`*` set in `rnfs.toml`, `!` changed there but waiting for a restart, `^` set on the daemon's command line,
which wins); `rnfs config get <name>` describes one. `rnfs config set <name> <value>` (a TOML value: `true`,
`10`, `"text"`, `["a", "b"]`; a bare word is text) and `rnfs config unset <name>` edit `rnfs.toml` in place,
keeping your comments, after checking the whole file as a start would. Some apply at once: `max_storage` (enforced
then), `max_download_size`, `max_jobs`, `quiet_hours`, the serve interfaces, the quotas, the LXMF sender lists
and a few more (`config get` says which); the rest at the next start. Apps do
the same through `GET /api/config` and `POST /api/config/set`, which carry each setting's type, range, unit,
default and description.

<a id="privacy"></a>**Private mode.** `private = true` with a `members` list (identity hashes, from each
node's `rnfs status`) makes the node link to, serve and keep records for members only. A name's announce still
reaches everyone, so outsiders can see which CID it points at and when it changes (not fetch it), and
`rnfs name publish` warns of this in private mode.

LXMF senders you've named (in `lxmf_accept_from`, `lxmf_auto_fetch_from`, or as an LXMF contact) are trusted.
Anyone can make new LXMF addresses for free, so messages from everyone else only push out each other's, within
fixed limits; set `lxmf_accept_from` if strangers' messages aren't wanted.

<a id="lxmf-is-moving-out"></a>**LXMF is moving out of rnfs.** Sending and receiving links over LXMF (`send`,
`inbox`, `outbox`, `lxmf address`, `lxmf sync`, the `lxmf_*` settings, the `/api/lxmf/` routes and the library's
`lxmf` methods) is deprecated since 0.2.0: it moves to a separate app built on rnfs's local API and library, and a
later release removes it from rnfs. It works as before until then, and says so each time: the commands print a
warning on stderr (`RNFS_NO_DEPRECATION_WARNINGS=1` hides it; stdout is unchanged), and the daemon logs it at
start. `rnfs:` links stay part of rnfs, and the pins of links fetched automatically already belong to the app
(`app:lxmf:<address>`). Don't use LXMF? `lxmf_enabled = false` turns it off now.

# Commands

Every command except `daemon` and `url` talks to the running daemon, so run them as the daemon's user.
`--dir DIR` and `--api-port N` pick another daemon; `rnfs <command> --help` shows every option. A **target** is
`<cid>[/path]` or `<contact>/path`; where a node ID is expected, a node contact's nick works too.

| Command | Description |
|---|---|
| `rnfs daemon` | Run a node with its API and gateway (`--bootstrap`, `--rnsconfig`, `--private`, `--debug-log`, `-v`…) |
| `rnfs status` / `peers` / `stats` | Node ID and identity hash, storage, jobs, LXMF address / known nodes, their round-trip time and whether each is online / traffic |
| `rnfs ping NODE_ID` / `providers CID` | Round-trip time to a node / nodes providing a CID |
| `rnfs add PATH\|-` | Add a file, directory or stdin, pin it and print its CID (options below) |
| `rnfs add DEST:/page/PATH` / `add DEST:/file/PATH` | Add a snapshot of a NomadNet page or file (`DEST` is the NomadNet node's 32-hex address), pin it and print its CID |
| `rnfs get TARGET [-o OUT] [--from NODE_ID] [--background]` | Fetch and write out a file or directory |
| `rnfs cat TARGET [--offset N] [--length N]` | Write a file, or part of one, to stdout |
| `rnfs ls TARGET` / `stat TARGET` | List a directory / size and how much is stored locally |
| `rnfs url TARGET` | The target's gateway site URL (`http://k….rnfs.localhost:8789/`; a name's too); needs no daemon |
| `rnfs dir set TARGET NAME CID` / `dir rm TARGET/NAME` | Make a copy of a directory with an entry added, replaced or removed |
| `rnfs export TARGET -o FILE` / `import FILE` | Write or import an archive (USB sticks, sneakernet) |
| `rnfs pin add TARGET` / `pin rm CID` / `pin ls` | Fetch and pin / unpin / list pins, their sources and where they came from |
| `rnfs pin remote add\|rm NODE_ID CID` | Ask another node to pin or unpin (it must list you in `remote_pin_identities`) |
| `rnfs pin remote ls [NODE_ID] [CID] [--check]` / `pin remote ls --served` | The remote pins you asked for and whether each is done (the node found among the CID's providers; `--check` looks now) / those others asked of your node, what became of each and their use of `remote_pin_quota` |
| `rnfs gc` / `verify` | Evict cached downloads / re-check every stored block |
| `rnfs deny add\|rm ENTRY…` / `deny ls` | Edit your deny list / show it and the lists in use |
| `rnfs jobs [list\|show\|wait\|cancel\|clear] [ID]` | Background transfers: progress, rate, time left |
| `rnfs name create\|list\|publish\|resolve\|rm` | Your names and what they point at |
| `rnfs name export\|import ALIAS` | Back up or restore a name's private key |
| `rnfs name keep\|unkeep\|kept NAME` | Store and serve someone's name record, so it resolves while they're offline |
| `rnfs name follow\|unfollow\|following NAME` | Keep the latest version a name points at pinned |
| `rnfs name sync ALIAS FOLDER` / `name unsync ALIAS` / `name syncs` | Keep your name pointing at a folder's current content: published when it changes, once unchanged a minute, at most once an hour / stop (the name keeps its target) / list the syncs and how they stand |
| `rnfs contacts add NICK HEX` / `rm` / `ls` | Nicknames for names, node IDs and LXMF addresses |
| `rnfs lxmf address` / `lxmf sync` | (deprecated: [moving to its own app](#lxmf-is-moving-out)) Your LXMF address / download waiting messages now |
| `rnfs send ADDRESS TARGET [-m TEXT] [--propagate]` | (deprecated) Send an `rnfs:` link over LXMF |
| `rnfs inbox` / `inbox get N` / `inbox rm N` / `outbox` | (deprecated) Received links (each line starts with the sender's address and whether it is `untrusted`, `trusted` or a contact; the display name the sender chose comes last, quoted), fetching them (a name link whose name can't be resolved needs `--use-sent-cid`), sent messages |

`add` options: `--wrap` keeps the file's name in the link; `--only-hash` prints the CID without storing;
`--no-pin` keeps it as evictable cache; `--chunker fastcdc` lets an edited version share most blocks;
`--to RECIPIENT…` (after the path) seals a file so only those node IDs, LXMF addresses or contacts can read it,
and `--to-self` adds you; a folder is sealed as one file, a tar named `<folder>.tar`, hiding its names too.

A NomadNet link (`rnfs add e166ecdbc405bcc9762b6767617fc949:/page/index.mu`) is fetched from that node as it serves
it now and added like a file, with the link shown in `pin ls`; run it again for a newer snapshot. The same options
apply, and `--name N` wraps it under that name. Requests are anonymous; `--identify` identifies with rnfs's own
NomadNet identity (`storage/nomadnet_identity`, not your node's) for pages the node serves only to identities it
lists. A local file whose name looks like such a link is added as `./NAME`.

`get` without `-o` uses the target's name only if it is a safe file name (printable, not starting with `.`),
otherwise its CID, and writes under a temporary name until the transfer is complete. It decrypts sealed files
you can open, and unpacks a sealed folder to its directory (`--no-extract` keeps the `.tar`); `--raw` gives the
stored bytes, which is what scripts that show content to others should use.

**Gateway** (`http://127.0.0.1:8789`): `/rnfs/<cid>[/path]` and `/name/<name>[/path]` serve files and
directories; `?filename=` sets the type and download name; `?format=raw` and `?format=archive` return data you
can check against the CID. Pages are sandboxed and can't read the gateway's responses, and sealed files are
never decrypted there. Each CID and name also has a site of its own, `http://<k-cid>.rnfs.localhost:8789/[path]`
(`rnfs url <cid>` prints it) and `http://<name>.name.localhost:8789/[path]`, whose pages can read their own
files; browsers resolve `*.localhost` themselves, other tools should use the paths above.

# Building apps on rnfs

Python programs use rnfs the way they use RNS, through one of two classes with the same methods, objects and
errors:

- **`rnfs.Node`** runs a node inside your program, beside its own Reticulum, with its own storage and identity.
- **`rnfs.Client`** uses the user's running daemon (their content, pins and names). It never loads Reticulum,
  so it starts fast.

**The API is provisional until rnfs 0.2** (`rnfs.API_STATUS`).

```python
import RNS, rnfs

RNS.Reticulum()                                   # your program's Reticulum, as with any RNS app
with rnfs.Node("~/.myapp/rnfs") as node:          # or: rnfs.Client(), for the daemon (no RNS.Reticulum())
    added = node.add("photos/")                   # rnfs.Added: .cid, .size, .warnings
    for entry in node.ls(added.cid):              # rnfs.Entry(name, kind, size, cid)
        print(entry.name, entry.size)
    with node.open(f"{added.cid}/cat.jpg") as f:  # seekable: a read after a seek fetches only what it covers
        data = f.read(4096)
    site = node.names.create("site")
    site.publish(added.cid)
    node.names.watch(site, lambda record: print("now", record.cid))
    node.jobs.get(f"rnfs:name/{site.hex}", "/tmp/copy").wait()
```

- An embedded node is a DHT client with LXMF off unless you pass `settings={"dht_mode": "server", ...}`. It
  never stops your Reticulum. Never point it at the daemon's `~/.rnfs/`: a directory is used by one node at a
  time.
- Callbacks run on one event thread; keep them short.
- Beyond content, pins, names and jobs: `peers()` and `stats()`, the address book (`contacts.add/get/list/remove`),
  keeping and following names (`names.keep/unkeep/kept`, `names.follow/unfollow/following`), and LXMF
  (`lxmf.address/send/inbox/get/remove/remove_all/outbox/sync`; off in an embedded node unless
  `settings={"lxmf_enabled": True}`; deprecated: each warns `DeprecationWarning`, as
  [LXMF is moving to its own app](#lxmf-is-moving-out)).
- To follow what changes instead of polling lists: `events.watch(callback, kinds=...)` (jobs, pins, the cache, the
  inbox, names, peers; an event with `event.lost` means "read everything again"), or `events.since(...)`. Also
  `capabilities()`, pin labels (`label_pin`), the cache (`cache()`, `uncache(cid)`), settings (`config()`,
  `config_set({...})`), tars and archives as streams (`add_tar`, `get_tar`, `export_stream`, `import_stream`), and
  for a `Client` `shutdown()` and `daemon_info()`.
- A `Client` can have a token of its own, so the user sees and removes what your app pinned: the user runs
  `rnfs app add myapp -o ~/.myapp/token`, and you use `rnfs.Client(app="myapp", token_file="~/.myapp/token")`.
  Its pins get the source `app:myapp` (`with client.pin_key("inbox"): ...` makes them `app:myapp:inbox`'s, a group
  your app can hold and release on its own); `client.unpin()` releases only your app's, and `rnfs app rm myapp`
  revokes the token and releases them all. The token isn't scoped: your app can do anything the user's `rnfs`
  command can, but manage apps.
- A `Client` waits as long as each request takes unless given `timeout=`; `with client.timeout(5): ...` bounds
  just the calls your thread makes inside it (a `Node` accepts it too, and does nothing).
- `examples/rnfs_share.py` is a runnable example using either class; design.md's "Public API" section is the
  reference. Other languages can use the local API's JSON endpoints: `docs/openapi.json` is an OpenAPI 3.1
  reference for every route (parameters, answers, error codes, the token headers and the events), which OpenAPI
  tools can read or turn into a client.

# Keeping content off your node

A deny list keeps chosen content off a public gateway or provider node. Entries are CIDs or file hashes
(`sha256:<hex>`, as `sha256sum` prints it):

```sh
rnfs deny add bafkreidhtnhwyqkmbhxvp2mrery66f4kpdsfbxew3qrfqrbtwbs6zb5zce
rnfs deny add sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
```

To use a list someone maintains (a text file, one entry per line, `#` for comments), add its CID, or
`name/<name>/path` to follow its updates, to `deny_lists` in `rnfs.toml`.

A denied CID is never fetched, stored or served, whether asked for directly or found inside other content
(the gateway answers 451). Cached copies are deleted and remote pins released; your own pins are kept but not
served, and the log names them so you can remove them. A file hash is only known once a file is downloaded,
so it stops the file being handed out or added, not its blocks being fetched: deny the CID for that (the first
time rnfs refuses a file by its hash, the log names the command). While there are file hashes, a file that
repeats its content more than 16 times beyond 64 MiB can't be read or imported (451): it can't be checked
cheaply.

# Example use cases

**Community library over LoRa.** A librarian publishes a directory of documents under a name and updates it
with `dir set` and `name publish`. Library nodes `rnfs name follow` it, so each always holds the latest
edition, and readers `rnfs get library/maps/region.pdf`.

**Sharing with a friend over LXMF.** `rnfs send <their LXMF address> <cid>/report.pdf -m "as promised"`.
Sideband, NomadNet and MeshChat show the link as text; their rnfs node lists it in `rnfs inbox` and fetches it
with `rnfs inbox get 1`, asking your node first. Add friends to `lxmf_auto_fetch_from` to fetch their links
automatically. (Deprecated: [LXMF is moving to its own app](#lxmf-is-moving-out).)

<a id="private-group"></a>**Private group.** `rnfs add minutes.pdf --to <alice's node ID> bob --to-self` seals
a file so only those people can read it, while any node can carry it. For a fully closed group, run every
node with `private = true` and the same `members`.

**Offline sneakernet.** `rnfs export <cid> -o bundle.rnfs` on a connected node, `rnfs import bundle.rnfs` on
the offline one: it's verified and pinned.

**Mirroring a publisher.** `rnfs name follow <their name>` keeps their latest content pinned and their name
resolvable through your node while they're offline.

**Field kit with an airtime budget.** A battery node on a shared LoRa channel:
`dht_mode = "client"`, `max_jobs = 1`, `upload_budget = 2_000_000`, `quiet_hours = "22:00-06:00"` and
`no_serve_interfaces = ["RNode"]`. Interface rules need rnfs to run Reticulum itself: attached to a shared
`rnsd`, every link looks like it arrives on the local interface.

**A website on the mesh.** `rnfs add mysite/`, then `rnfs name create site && rnfs name publish site <cid>`.
Visitors open `http://127.0.0.1:8789/name/<name>/` on their own node, and each `name publish` updates it; or
`rnfs name sync site mysite/` publishes the folder whenever it changes (at most once an hour, and only once it has
been unchanged for a minute, so a half-copied site isn't published).
NomadNet pages can't open `rnfs:` links, so show the link as text: `rnfs:name/<name>/index.html`.

# Running several nodes

Give each daemon its own `--dir`, `--api-port`, `--gateway-port` and Reticulum config (`--rnsconfig`). The
first node's Reticulum config has a `TCPServerInterface` with `enable_transport = Yes`; the others have a
`TCPClientInterface` to it and `share_instance = No`, and start with `--bootstrap <first node ID>`:

```sh
rnfs --dir /tmp/a --api-port 9001 daemon --rnsconfig /tmp/a --gateway-port 9101 --bootstrap <hub id>
rnfs --dir /tmp/a --api-port 9001 add somefile
rnfs --dir /tmp/b --api-port 9002 get <cid> -o out
```

# Known limitations

- A name resolves only while its owner, a transport node that remembers its announce, or a keeper is
  reachable, and can be updated about once an hour.
- Sybil attacks can make a CID hard to find (`get --from` bypasses the DHT), but data can never be forged.
- Sealed files hide content and file name, not size or recipients, and don't prove who sealed them. `get` of a
  directory leaves sealed files inside it encrypted.
- Under the default `provide_strategy = "roots"`, anyone can learn what your node downloaded. With
  `provide_entries` (on by default), anyone can learn which files your pinned folders hold by looking up each
  file's CID, without knowing the folder.
- LXMF messages over 256 kB are refused, unsent messages are lost if the daemon restarts, and offline delivery
  needs `lxmf_propagation_node`. A message rnfs can't keep (a stranger's during a flood) is lost, though the
  sender's app shows it as delivered.
