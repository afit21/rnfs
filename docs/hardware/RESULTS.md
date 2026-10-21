# Hardware tests: LoRa and cloud nodes (October 2026)

Real radios and real servers, not simulations: rnfs 0.2.0 in development (the `dev` branch, from `ce3e25d`) on two
computers linked by LoRa and seven cloud nodes, five of them upgraded from 0.1.6 during the tests. What each test
found, and the fixes it led to (commits named), are below; the fixes are in the changelog's 0.2.0 section.

## Setup

- **PC** (Linux Mint 22.1, 12 threads): `rnsd` 1.5.6 as a shared instance with transport on; an AutoInterface, a TCP
  link to a backbone hub, and an RNodeInterface. rnfs with about 883k blocks. Its radio is the island's gateway.
- **Laptop** (Linux Mint 22.3, 8 threads): `rnsd` 1.5.4, an RNodeInterface only (its backbone link disabled), so a
  slow node (`slow: True`) whose every path goes over the radio and through the PC. About 27k blocks.
- **Radios**: two Heltec LoRa32 V3 with RNode firmware 1.86 (SX1262), 925.875 MHz, 22 dBm, a few metres apart. First
  SF9, 250 kHz, CR 4/5 (3.52 kbit/s), then SF10, 125 kHz (977 bit/s).
- **Cloud**: five Google Cloud VMs (2 vCPU, 4 GB) and two AWS Lightsail instances, rnfs as a systemd user service;
  each runs its own Reticulum (transport on, several public backbones) but the Sydney gateway node, which attaches to
  an `rnsd`. Montreal, US central, Madrid, Johannesburg and the Sydney gateway node (which also runs the public
  NomadNet gateway) were upgraded from 0.1.6; Stockholm and the other Sydney node stayed on 0.1.6 for interop.

## LoRa at 3.52 kbit/s

| Test | Result |
|---|---|
| Laptop joins the DHT over the radio | joined in 46 s, 14 servers known; link round trips 1–2 s |
| 8 KB random file, PC → laptop | 40.5 s (the first request, to the PC's open link, brought it) |
| 24 KB Gutenberg text, PC → laptop, before `92cd81d` | 88 s, sent whole with `/blocks` (bz2 makes it 9.8 KB) |
| 24 KB text, PC → laptop, `/range` | 68 s of transfer in two compressed ranges, 10.2 KB on the air |
| 24 KB text, 0.1.6 node in Sydney → laptop | 107 s: the `/range` left unanswered 17.5 s, then `/blocks` 77.5 s, as specified |
| 12 KB, laptop → 0.1.6 node in Sydney | 43 s once the laptop's records were out (a fetch started 10 s after `add` found none) |
| Names: laptop publishes, PC, Sydney (0.1.6) and Montreal resolve | about 1 s each, from the announce |
| Names: PC republishes, laptop sees the new sequence | under 6 s |
| Laptop follows the PC's name; the PC points it at a folder | fetched and pinned 42 s after the publish |
| Laptop's HTTP gateway: a PC folder's listing, then a 2 KB file in it | 6.3 s, then 10.4 s |
| Remote pin: laptop asks the PC to pin a file only the laptop holds | accepted in 9 s, pinned 38 s later (fetched from the asker over the radio) |
| 200 KB text (4 chunks) PC → laptop, the PC's daemon restarted 30 s in | 386 s in all, about 50 s of it with the PC down; the interrupted chunk resumed from the 16.4 KB held, nothing sent twice; about 4.1 kbit/s of file per 3.52 kbit/s of channel |
| The laptop's `rnsd` restarted under a running daemon | requests out work at once; the PC reached the laptop again after about 65 s (RNS's re-announce of client destinations) |
| Airtime during an hour of these tests | laptop 5.5 %, PC 8.6 % |

## LoRa at 977 bit/s

SF10, 125 kHz, both `rnsd` restarted (so every link and the laptop's paths were new); the laptop's publisher was
republishing at the same time, as it would in use.

| Test | Result |
|---|---|
| 16 KB Gutenberg text, PC → laptop | 184 s: a 25 s search (no record held: the PC's local replica failed while the laptop's `rnsd` was coming back), then one 6.1 KB bz2 range in 131 s |
| 16 KB random file, PC → laptop (the first request, to the open link) | 287 s, one 16 KiB range (no rate estimate yet: `min_rate_sample`), uncompressed |
| Link round trips | 12–22 s while the channel was busy (1–2 s at 3.52 kbit/s) |
| The laptop's `/provide` links meanwhile | about half timed out (RNS's per-hop establishment timeout against 20 s round trips) |
| Channel load | 80–100 % over 15 s on both radios during a fetch; laptop airtime 52 % over 15 s at its peak |

The hour after these fetches, with nothing asked of either node: the laptop transmitted 6.6 % of the time and the PC
7.3 %, the channel busy 18–19 %. Nearly all of the laptop's was two publishing rounds storing the roots of what it
had just fetched (2 keys, then 1, batched per server: about 25 identified links, to every one of the network's 12–13
servers); the PC's was its serving, its own records and the announces it relays from the backbone.

With `auto` (`d95a2d8`), the same test: the laptop fetched two 6 KB files and the following hour it made no publishing
link at all (about 25 before) and transmitted 4.65 % of the time, all of it serving other nodes: 42 incoming links,
26 `/provide` and 21 `/find`, from cloud nodes publishing their own keys (during the cloud feature tests). From
Madrid the laptop is 3 hops away with no rate estimate, so it isn't a slow path, and on a network of 15 servers it
is among a key's 10 closest (and closest senior servers) about two times in three; on a large network a slow server
holds few records. Not changed: a long round trip alone is kept out of the slow-path rule (I2P), and ranking slow
servers last among the senior servers would put records outside the set a searcher asks.

At this rate one node's own work fills the channel: a fetch, its search running on until the root arrives, and the
publisher storing the roots of what was just fetched (`provide_strategy = "roots"`: each cached download is
published, an identified link to each of about 10 servers, about 6.4 s of airtime each at 1 kbit/s by the load
study's figures). `provide_strategy = "pinned"` avoids the last; since `d95a2d8` it is what a slow node does unless
set (the new default, `auto`).

## Partition: the island cut off from the backbone

The PC's backbone and AutoInterface were disabled and its `rnsd` restarted, leaving the PC and the laptop alone on
the radio. In a real outage the TCP interface would stay configured (down), so its paths would remain and the PC would
not count itself slow; here they were removed, which made some of what follows harsher than an outage.

- The laptop entered partition mode 2 min 40 s after the cut, from its failures to reach off-island servers.
- A search during the partition found nothing in 3.5 minutes although the PC held the record: every `/find` to the PC
  went over a link that died at the PC's end when its `rnsd` restarted, each waiting out RNS's request timeout
  (21–23 s), and RNS would notice the dead link only at its keepalive, about 12 minutes on a radio path. Fixed by
  `0af1e51`: a link that received nothing while a request on it went unanswered is closed. Measured after the fix,
  with the PC's `rnsd` restarted again under an open link: closed after 17 s, a new link at once, the file fetched.
- The same file then came in 57 s during the partition, from the PC, which held its own record.
- A file whose record the laptop itself held as the PC's local replica still waited 51 s for a search to find the
  provider that record named. Fixed by `be69c5f`: a fetch starts its search with the providers of the records held
  here.
- The PC's store of a new key's local replica on the laptop failed on one lost link request (both radios
  transmitting), and a failed store waits for the next republish. Fixed by `a92fb5e`: a local server no link could be
  opened to is tried once more.
- Both daemons restarted during the partition: a node's request to the other, during that node's setup, was dropped
  (RNS accepts links to a destination as soon as it exists; rnfs registered its handlers only after setting up).
  Fixed by `6b269aa`: links are refused until the request server is in place.
- The PC, restarted while cut off, joined only after asking 13 candidates one at a time (30 s each for a path), about
  6.5 minutes, and published nothing until then; it never entered partition mode (every failure was for want of a
  path, which by design doesn't count, and no off-island announce was heard to measure silence by). What it then
  published went to the laptop alone and wasn't marked for the heal republish, so once the backbone was back nothing
  republished it beyond the island. Fixed by `cb89212`: a round that reached no off-island server is marked while
  off-island servers are known.
- With the default `local_hops` (3), the laptop counts servers on the PC's backbone hub (2–3 hops away, through the
  PC) as local, and its local-first search asked one of those before the PC. The specification names this limit
  ("Limits of a hop radius") and advises `local_interfaces` or a lower `local_hops` on such nodes. The default stays
  3, which the topology simulation measured on meshes of relays (nothing in RNS's path data tells a radio hop from an
  internet one beyond the first); the README now says to set `local_hops = 1` on a node one radio hop from its
  gateway.

## Upgrading cloud nodes from 0.1.6

| Node | Blocks | First start to API | Notes |
|---|---|---|---|
| Montreal (GCE, 2 vCPU) | 857,325 (43 GB) | 353 s | listing 19 s, move 110 s, then 3.5 min with nothing in the log (block sizes, then 80 s working out what it provides); the next start 148 s (0.1.6's own last start: 165 s) |
| Sydney gateway (Lightsail) | 742,586 | 274 s | attached to an `rnsd`: listing 8 s, move 244 s, block sizes 20 s, the rest 8 s |
| US central (GCE) | 33,826 | 331 s | 265 s of it Reticulum's own start (below); move 45 s |
| Madrid (GCE) | 33,826 | 222 s | about 170 s of it Reticulum's start; move 34 s |
| Johannesburg (GCE) | 21 | 41 s | |

State database upgrades took 0.1–2.2 s. Under systemd none of the progress lines reached the journal until the
daemon stopped: Python fills a pipe's buffer in 8 KiB blocks, and on a quiet node that is hours (`1248979`). The
silent steps after the move now report themselves (`1a4701d`), which the Sydney gateway node's upgrade showed live.

Reticulum's own start took 3–4.5 minutes on two of the GCE nodes, rnfs having stopped cleanly: with transport on, RNS
rebuilds its path table by reading each entry's cached announce, a file each, from a 120 MB packet cache. It isn't
rnfs's to change; the API answers only after it.

## Interoperability

- A 300 KB file each way between Montreal (development) and Stockholm (0.1.6): 3–4 s, bytes identical.
- A 30 MB file from Montreal to three development nodes at once: US central 26.6 s (about 1.2 MB/s), Madrid 1,715 s
  and Johannesburg 2,903 s, bytes identical. The slow two are Reticulum's routes: Madrid reached Montreal in 4 hops
  through the Broken Hill gateway in Australia, Johannesburg through Cape Town at a 3.2 s round trip. US central,
  done in 26 s and providing the file from then on (2 hops from Madrid), wasn't used: a fetch searched again only
  once every provider it had failed, not when the one it had was slow. Fixed by `17bc360`: a long fetch looks for
  more providers.
- `tests/interop.py` against 0.1.6, with every fix here (`cb89212`): every scenario passes both ways (providers,
  fetch, bulk and small with and without an upload cap, ranges learnt after two silences, names, keepers).
- The NomadNet gateway's `rnfs.Client` (0.1.5) against a development daemon: `node_id`, `ls`, `get(raw=True)`,
  `pins`, `pin`, `unpin`, `unpin` of a CID not pinned (`NotFound`) and a malformed CID (`ApiError`) all behave as
  the gateway expects. The public gateway node served its index page after its daemon's upgrade.

## Other findings and fixes

- `92cd81d`: the first request of a fetch (to an open link, a remembered provider or a remote pin's asker) always used
  `/blocks`; on a radio that request most often brings a single-chunk file, which so never got `/range`'s
  compression or resumption.
- `10763e3`: one-byte answers were logged as "-1 B" (RNS sizes a one-packet response as its msgpack length less 2).
- A request packet can be lost when both radios transmit at once (seen twice): RNS doesn't resend a request, so it
  times out. rnfs's retries cover it; the link fix above keeps a working link that heard from the other end meanwhile.

## Second round (October 21)

The fixes above were deployed (laptop, PC, five cloud nodes) and the untested ones re-checked on the hardware; two
agents ran feature and packaging tests meanwhile.

- **LoRa, 3.52 kbit/s:** the laptop, slow, came up with `provide_strategy` `auto` and withdrew the records of its
  cached downloads (12 keys a server, paced). A probe to an open link not known to answer `/range` used `/blocks`;
  one to the PC, known to, brought a 12 KB text as one 4.8 KB bz2 range in 18.8 s, with no search. From the 0.1.6 node
  in Sydney: the provider came from a record the laptop held ("a record held here", no search wait), its second
  `/range` silence marked it as not answering, and `/blocks` followed on the same link (`0dafaa6`); 10 KB in 59 s.
- **LoRa, 977 bit/s:** a fetch from the PC while its `rnsd` restarted failed after 338 s (fixed by `7bd50b7`: no path
  once is asked again a minute later); after the fix the same sequence logged "no path …; asking it again in 60s" and
  fetched the file 163 s in.
- **NomadNet gateway:** a NomadNet-style client (RNS link to its destination, `/page/get.mu` then `/file/get` with
  `var_cid`) got a 12 KB file the gateway's upgraded daemon had just fetched from the PC, bytes identical.
- **Cloud features** (four development nodes, a gateway node and two 0.1.6 nodes): sealed files and folders
  (recipients, a non-recipient, a 0.1.6 recipient getting a folder as `.tar`), names (publish, keep, `name sync`
  publishing an edit within two minutes), remote pins with status (`--check`, `--served`, quotas, denied), deny lists,
  app tokens and their pins, events, tar streams, cache, settings applied live, the path gateway with ranges and site
  hosts, and interop with 0.1.6 all passed. Searching again (`17bc360`): with only Montreal holding a 20 MB file, Madrid
  ran at 10–18 KB/s until it found US central and Johannesburg, which had fetched it meanwhile, then at 23–53 KB/s:
  554 s, against about 1,230 s at its first rate. Found and fixed: a `sha256:` deny entry didn't stop a one-block file
  being served or provided (`7958129`); the first sealed-folder unpack after a start on Montreal held its result 3
  minutes in a collection that read the whole store (`00660d7`); `cache rm` counted an unpinned add's blocks as the
  first CID's (`2b3318c`); the `Server` header ended in a space (`52e40c1`).
- **Packaging** (Docker): `install.sh` from the development sdist on Ubuntu 24.04, Debian 12 and 13 (user and
  `--system`), the `.deb` built and installed on all three with its smoke test, pipx, Python 3.11 accepted and 3.10
  refused, and a real 0.1.6 `~/.rnfs` (pins, names, contacts, retired settings, an LXMF pin) upgraded with every
  upgrade note holding, 0.1.6 then refusing the upgraded state database cleanly. Fixed: a missing local `RNFS_SOURCE`
  printed a traceback, Ubuntu 22.04's Python advice named a package it doesn't have (`6760409`), `rnfs stop` waited out
  its timeout for a daemon that had exited unreaped (`2f422ec`), and `config unset` said "saved" (`e67395d`).
- **Starting a large node** (`5da2c3c`): each DAG node is read once (the entries walk takes what the served set's
  walk collects) and decoded without the canonical re-encode (an equivalent shortest-header check); on a synthetic
  565k-block store the first refresh went from 6.26 s to 3.81 s and 177.6 MB to 159.5 MB at its peak.


## Third round (October 21)

All on LoRa at 3.52 kbit/s (SF9, 250 kHz), the PC with its backbone, the laptop on the radio only.

- **Resume after a crash:** the laptop's daemon, SIGKILLed 46% into a background `get` of 140 KB, listed every stored
  block on start, resumed the job and finished it, bytes identical. It showed `0 B/s  ETA ?` for 90 s meanwhile, with
  56 KB of its next chunk in hand: a job's progress counted a block only once whole, and a `get` didn't know its
  total until the sealed check had read the first chunk. Fixed (`6ca2a04`, `2458391`): the same fetch now steps
  46%, 74%, 93% as ranges arrive.
- **`rnsd` restarted under a running daemon:** the node carried on (a fetch 12 s after `rnsd` was back), but logged a
  warning with a traceback each time it read the radios' load meanwhile; now a verbose line (`d5779bf`).
- **Cancel:** `rnfs jobs cancel` partway through a chunk ended the job at once and closed the link; the PC's
  response stopped with it.
- **Gateway range:** `curl -H 'Range: bytes=250000-251999'` on the laptop's gateway for a 300 KB file held only by
  the PC: `206`, the right 2,000 bytes in 83 s, one chunk of six fetched.
- **Both ways at once:** the PC and the laptop each fetching the other's new file failed both ways. The PC's search
  completed 29 ms before the laptop (one hop away, but ranked last behind a slow path) answered with itself as
  provider; the answer was dropped. The laptop's only provider was failed by one lost `/range`: the link, silent
  meanwhile, was closed for it, and that counted as a failure, not a loss. Fixed (`51e07a2`): late answers'
  providers are used, and such a request counts as lost. Again with new files: 53 KB in 164 s to the PC, 100 KB in
  259 s to the laptop, at the same time, bytes identical.
- **Internet to island:** Montreal fetched a 12 KB file only the laptop held, over the PC's radio, in 65 s.

## Release audit (October 21)

Before 0.2.0: reviews of every subsystem, new fuzzers, and a soak of the cloud nodes, then the LoRa scenarios again on
the fixed build. The findings are REL-1 to REL-19 in CHANGELOG.md (the attacks also in `tests/security/README.md`).

- **Soak:** a random file of 20 KB to 2 MB added on one development node every minute or two and fetched by
  another, or by a 0.1.6 node, its bytes compared. It found:
  - REL-12: a provider refused for a minute after a large response, as its upload cap had been set from a rate
    estimate small `/range` responses left low, and the requester failed it.
  - REL-19: a route that cut every 64 KiB response from Montreal at the same point, three times in a row, failing
    the provider, where ranges would come through.
  - The CLI telling a starting daemon's user there was no API token.
  Of the rest:
  - Most failures were fetches 20 s after the add, before the publisher's records were out. Its publish lookup
    waited up to 24 s on a server that didn't answer, more often just after every node restarted at once. Asked
    again a minute later, they succeeded.
  - The others were the route between Montreal and Sydney, which lost most of what crossed it: whole responses,
    and ranges too.
- **LoRa at 3.52 kbit/s, on the fixed build:**
  - Fetches both ways at once (139 s and 339 s).
  - SIGKILL during a fetch, then a resume with nothing left behind (REL-10).
  - Cancel.
  - A gateway byte range: 206, the right bytes.
  - `rnsd` restarted under the daemon, with no warnings logged (d5779bf).
  - A remote pin, done in 70 s.
  - A second round, on the final build, passed the same way (155 s and 257 s both ways, a remote pin in 50 s) but for
    the gateway range: its search ran 600 s, curl's limit, and found no provider. The same far server cost 217 s
    twice, and three others failed meanwhile.
  - One finding was left as is. A search from the laptop asked internet servers within 3 hops first, as its local
    servers: every path from the laptop goes through the PC, the known single-gateway case. Two of its three query
    slots then waited 127 s and 217 s on servers that never answered, one of them 34 hops away by RNS's path. Letting
    a query that has waited long give up its slot while its answer still counts would speed such searches; it
    changes the lookup, so it is left for after 0.2.0.
