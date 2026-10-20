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

At this rate one node's own work fills the channel: a fetch, its search running on until the root arrives, and the
publisher storing the roots of what was just fetched (`provide_strategy = "roots"`: each cached download is
published, an identified link to each of about 10 servers, about 6.4 s of airtime each at 1 kbit/s by the load
study's figures). `provide_strategy = "pinned"` avoids the last; the specification leaves the default to `roots` for
every node.

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
  ("Limits of a hop radius") and advises `local_interfaces` or a lower `local_hops` on such nodes.

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
  done in 26 s and providing the file from then on (2 hops from Madrid), wasn't used: a fetch searches again only
  once every provider it has failed, not when the one it has is slow.
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
