# rnfs

Content-addressed file sharing over [Reticulum](https://reticulum.network) (RNS), with the functional
features of IPFS: content IDs, chunked files and directories, a provider DHT, verified multi-source
transfer, pinning, mutable names and a local HTTP gateway. It is built to work over slow, shared links
such as LoRa as well as over TCP. [specification.md](specification.md) describes the protocol and
[design.md](design.md) describes the code layout.

## Setup

Requires Python 3.11+ and RNS 1.5.6:

```
pip install -r requirements.txt
python3 main.py daemon            # or: python3 -m rnfs daemon
```

The daemon keeps everything in its rnfs directory (`--dir`, default `~/.rnfs/`):

| Path | Contents |
|---|---|
| `config` | Reticulum config. RNS writes a default config on first start. If `share_instance` is on and `rnsd` is running, rnfs attaches to it |
| `rnfs.toml` | rnfs settings (see below) |
| `storage/identity` | the node's RNS identity; the node ID is derived from it |
| `blocks/`, `state/` | block store; pins, cache records, provider records, publish state, names' sequences |
| `names/` | name keys |
| `files/` | the shared folder: anything placed here is added, pinned (source `folder`) and provided |

## Commands

Every command except `daemon` talks to a running daemon through its local API (`--api-port`, default 8788).
Paths are resolved by the CLI and read or written by the daemon.

```
rnfs daemon [--bootstrap NODE_ID…] [--gateway-port N] [--dht-mode server|client] [--slow yes|no|auto]
            [--private] [--provide-strategy pinned|roots] [--no-gateway-scripts] [-v|-vv]
rnfs add PATH|- [--wrap] [--only-hash] [--name NAME]   # import and pin; prints the CID
rnfs get CID[/PATH] [-o OUT] [--from NODE_ID]          # fetch and write out a file or directory
rnfs cat CID[/PATH] [--offset N] [--length N]          # fetch (only the chunks needed) to stdout
rnfs ls CID[/PATH]                                     # kind, size, CID, name
rnfs stat CID[/PATH]                                   # size, blocks, how much is stored locally
rnfs dir set CID[/PATH] NAME CID [--exec]              # new directory with an entry set; prints its CID
rnfs dir rm CID/PATH                                   # new directory with an entry removed
rnfs pin add CID[/PATH] | pin rm CID | pin ls [CID]    # fetch and pin; unpin; pins and their sources
rnfs pin remote add|rm NODE_ID CID                     # ask another node to pin (it must allow you)
rnfs gc | rnfs verify                                  # evict cached fetches | re-check every block
rnfs providers CID | rnfs ping NODE_ID
rnfs name create|rm|export|import ALIAS  rnfs name list
rnfs name publish ALIAS CID[/PATH]   rnfs name resolve NAME
rnfs export CID[/PATH] -o FILE | rnfs import FILE      # archives, for moving content without a network
rnfs status
```

The gateway (default `http://127.0.0.1:8789`) serves `/rnfs/<cid>[/path]` and `/name/<name>[/path]`.
It supports byte ranges and directory listings, and serves a directory's `index.html` when it has one.
Pages are sandboxed by a Content-Security-Policy. Content fetched for the gateway is cached but never
provided.

## Sample Files

Some files that I host as examples for everyone to download :)

The Communist Manifesto - bagaibqabciqkq6rth6jhr2vkh7c2ip7p6bsmdxmslbia47rjbmwpcz6wwu6veya

## Settings

`<rnfs dir>/rnfs.toml` sets any local setting from specification section 12 (times in seconds, sizes in
bytes, rates in bits/s). Command-line options override it. For example:

```toml
api_port = 8788
gateway_port = 8789
dht_mode = "server"
provide_strategy = "pinned"      # or "roots": also provide complete downloads while cached
max_storage = 10737418240
max_download_size = 1073741824
remote_pin_identities = ["<32 hex identity hash>"]
remote_pin_quota = 1073741824
members = []                      # with private = true: only these identities
announce_interval = 21600
republish_interval = 86400        # must be < PROVIDER_TTL / 2 (3.5 days)
```

## Running several nodes on one machine

Give each daemon its own `--dir`, `--api-port` and `--gateway-port`, and connect their Reticulum instances.
For example, start one daemon whose `config` has a `TCPServerInterface` with `enable_transport = Yes`. Start
the others with a `TCPClientInterface` to it, `share_instance = No`, and `--bootstrap <first node ID>`
(`status` prints a node ID). Then pass `--api-port` to target a particular daemon:

```
python3 main.py --dir /tmp/a --api-port 9001 daemon --gateway-port 9101 --bootstrap <hub id>
python3 main.py --dir /tmp/a --api-port 9001 add somefile
python3 main.py --dir /tmp/b --api-port 9002 get <cid> -o out
```

`tests/test_e2e.py` sets up exactly this, with short intervals in each node's `rnfs.toml`.