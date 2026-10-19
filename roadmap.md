# 0.1.5 (released)

## CID denylist (done)

- Ability to point to a CID or name as a CID deny list (`deny_lists`, by CID or `name/<name>`)

- Refuse to fetch, provide or store a CID.

- Deny lists can contain file hashes and/or CIDs

## Sqlite3 migration (done)

- State is kept in one SQLite database, `<rnfs dir>/state/rnfs.db`

- The JSON stores of earlier versions are imported at the first start, and kept as `<name>.migrated`

## Client Library (done)

* `rnfs.Client` (the user's running daemon, through the local API) and `rnfs.Node` (a node inside a program),
  with the same methods and objects; provisional until 0.2 (`rnfs.API_STATUS`)

## Change notifications for other applications to subscribe to (done)

* Name changes: `names.watch` in the library, `GET /api/name/watch` in the local API
* Job progress and completion: job callbacks in the library

# 0.1.6

Security fixes, maintainability work and performance work, with no wire-format or CID changes (see
CHANGELOG.md).

# 0.2.0

The feature-complete release, shipped as one release after 0.1.6: the work planned as 0.1.7 is part of it, and
there are no 0.1.x releases in between. Nodes of 0.1.3 and later keep working with 0.2.0 nodes: no CID changes,
wire changes additive only, and every state change gets an upgrade migration from 0.1.6.

## Local API and library for companion apps (done; planned as 0.1.7, which wasn't released)

What companion apps need from the daemon, most impactful first.

* Discovery file: the daemon writes `<rnfs dir>/daemon.json` at start with its PID, API ports, version, start time
  and the Reticulum config it uses, so a client finds the daemon without parsing `rnfs.toml` (an older client
  can't find the port today if `rnfs.toml` has a newer setting)
* `POST /api/shutdown`: a background daemon can't be stopped cleanly on Windows without it (the app ships a second
  console program to send Ctrl+Break through a hidden console)
* Capability list and machine-readable error codes: apps string-match error messages today, and a 404 can mean
  either "no such endpoint" or "content not found"
* Push endpoint (`GET /api/events`) for jobs, pins, the LXMF inbox, names and peers, replacing the apps' polling
* Richer `pin ls`: size, time added, folder entry counts as fields (not "(directory: 28 entries)" strings), and a
  renamable label kept separate from the added-from path (nothing can set a file's local name today)
* `cache ls` and `cache rm`: nothing lists downloads that aren't pinned, short of reading `blocks/` on disk
* Config API that carries its own schema (so apps don't keep a copy of the setting metadata), with some settings
  applied without a restart (every setting needs one today)
* Add and import as jobs with progress, and directory streaming for upload and export, so a sandboxed (Flatpak)
  daemon works fully
* Peers: `first_heard`, the last link RTT the node already measured, and an online state worked out by the daemon
* `rnfs.Client`: typed methods for peers, stats, contacts, follow, keep and LXMF, and timeouts per call instead
  of per connection


## Providing at scale

Today a node can advertise about 1000 roots on a small network (100 while it is new): each server stores at most
`max_records / 100` records per publisher, and with 10 or fewer servers every server holds every key. Each root
also costs its own closest-nodes lookup at every republish: on a fast node a round for 1M keys takes about 83 h,
so it never finishes within a day. Every record is also held in memory (about 500 bytes each). Simulated in
[docs/simulations/load](docs/simulations/load/RESULTS.md).

* Sweep providing: sort the due keys; look up the first one not yet covered; treat every key sharing its first
  p bits as one region (p = the common prefix of the key and its 10th-closest server, so a region always holds
  at least 10 servers); if the region has 2 or more due keys, send one `/find` to the known node nearest the
  centre of the region's far half (and to each half again if that reply is full); give every key in the region
  its 10 closest from what was learned, then batch `/provide` per server (22 keys a packet at the default MTU,
  55 at most). It placed 100% of records correctly for about 1.6 extra `/find` per region. Lookups per round
  level off at about servers ÷ 12.5 (1M keys on 10k servers: 801 lookups instead of 1M); a fast node's round
  for 1M keys drops to about 1.1 h with `/provide` sent to 8–16 servers in parallel
* Republish jitter per region rather than per key, so a region's keys stay due together; slow nodes pace per
  region, not per key; withdrawals ride in the same batches, and are paced on slow nodes too (unpinning 1,000
  keys costs ~2.9 h of 1 kbit/s airtime at 1k servers today)
* Slow nodes can't publish much at scale, sweep or not. At 1 kbit/s, keeping to 5% of the channel with a 72 h
  republish: about 4,500 keys on a 100-server network, 590 at 1,000 servers, 111 at 10,000. Past that, a slow
  node hands its content to a fast node with remote pinning (the existing `/pin`): the fast node stores,
  pins and provides it, so the slow node's airtime no longer depends on how many keys it shares
* `K` stays 10: `K` = 20 evens out server load by only 10–25% and doubles every per-key cost. The busiest
  server carries 1.6× (1k servers) to 2.4× (100k) the mean load, so record budgets need about 2× headroom
* Provider records on disk: records, publishers' first-seen times and tiers live in the state database and are
  read per `/find` through an index, with a small cache, instead of loaded whole into memory (117 bytes per
  record on disk). Inserts and deletes are batched into one transaction every ~30 s; a refresh is written only
  when its last-stored time moves by more than ~1 h; eviction uses an index rather than scanning a publisher's
  keys; and a full store refuses rather than evicts where the tiers allow (a refusal writes nothing), which
  keeps SD-card writes at 0.03–2 per second even under a flood (today's policy: up to 100 per second)
* A record budget of its own instead of a record count: `max_records` goes, and records may use a share of the
  disk (default e.g. 1 GiB, about 9M records), never the disk reserve that blocks need, so a flood of records
  can't stop the node storing content. A `max_records` left in `rnfs.toml` is ignored with a notice (an unknown
  setting stops the daemon today). The cap of 1000 publishers per key stays, so a `/find` stays cheap
* Persist the routing table, with each entry's first-seen time and score (below), so a restart doesn't re-flood
  path requests on radio to rejoin, and peers keep the standing they earned
* Benchmark: one node publishing a large collection (e.g. the ~70k-file Gutenberg mirror) over rnfs, kept
  findable through republishing

## Resisting floods

RNS identities are free and RNS has no IP addresses, so no cap keyed on an identity bounds an attacker, and no
deployed DHT's defence (IP diversity, a directory authority) carries over. The model: **share freely while there
is room; when there isn't, publishers this server has seen for days keep their place and strangers compete for a
small, fast-expiring pool.** Background research: [docs/sybil-resistant-publish-caps.md](docs/sybil-resistant-publish-caps.md);
simulated in [docs/simulations/storage](docs/simulations/storage/RESULTS.md).

* Local scores: each node keeps a private score for the peers it deals with, raised by useful work it can
  check itself: blocks served that verified against their CIDs, `/find` answers that led to real providers or
  closer servers, and refreshing records on distinct days. Nothing is gossiped or published. Scores are a few
  numbers on rows that already exist (routing entries, publishers of stored records), in a capped table that
  forgets the least recently useful peer first; a peer without a row scores 0, so forgetting is always safe.
  State grows with the node's own activity, not with the size of the network. Scores order providers and the
  servers a join tries; the storage tiers use only the refresh-day test below
* Bootstrap nodes start with a seeded score, so a fresh node has someone to lean on from its first minute. They
  keep it only by earning it like any other peer (failures, silence and bad blocks lower it, bans still apply).
  `default_bootstrap = false` drops both their joining role and their seeded score. A user's own
  `bootstrap_nodes` get the same seed, which matters most on meshes without the internet
* Storage tiers, applied only once the record budget is full (anyone stores freely while there is room):

  | Tier | Who | Floor | Records expire after | Eviction inside the tier |
  |---|---|---|---|---|
  | Known | Contacts, private-mode members, seeded bootstrap nodes | 30% | `PROVIDER_TTL` (7 days) | Largest holder first |
  | Earned | Held a record and refreshed it on at least 3 distinct UTC days | 60% | `PROVIDER_TTL` (7 days) | **None: Earned publishers never evict each other** |
  | Newcomer | Everyone else, including 0.1.x publishers new to this server | 10% | 4.5 days | Largest holder first; one holding as many as the largest is refused |

  When the budget is full, a store reclaims first from the lowest tier below the storer's that is above its
  floor (from its largest holder); if the storer's own tier is below its floor, from a higher tier that is above
  its floor; otherwise only within its own tier. Floors therefore hold in both directions: read literally, "a
  lower tier never evicts a higher one" lets Earned growth take the Newcomer floor for good. A full key (1000
  publishers) also gives way lowest tier first
* Why "never evict each other" inside Earned: with fair-share eviction there, a flood earns Earned status after
  3 days and its identities then take per-identity shares, pushing established honest records below 50% within
  2 days once it has 100 identities, and from the start if pre-aged. With no eviction inside Earned, established
  records stayed at 90% or more for 10 to 10,000 attacking identities pre-aged up to 180 days (except a fresh
  flood on a store already ~90% full of honest records: 69–84%), publishers joining during a flood kept 88–100%,
  and peacetime was unchanged (a mirror worth 30% of the budget joining a small network: 98–99% stored within a
  day, against 1–3% under today's per-publisher cap). Ordering Earned by seniority instead protects established
  publishers but drops those younger than the attacker to 1%
* Newcomer expiry of 4.5 days: the longest honest gap between republishes is 3.55 days (kept names: 72 h + 10%
  jitter), and in simulation 4 days lapsed honest records once a slow node's republish was delayed by a day,
  while 4.5 days lapsed none. Expiry barely changes recovery after a flood
* Not adopted, as none helped any honest publisher in simulation and each cost newcomers or joining mirrors:
  admission tickets that wait as the store fills, caps or daily insert limits for newcomers, a limit on
  promotions into Earned, weighting by age, a separate Senior tier, and requiring 5 days rather than 3
* What remains: a pre-aged attacker can take free space and the Newcomer floor, absorb space that honest
  publishers leaving the network free (about 6–10% of large publishers' records over three weeks at 1% churn a
  day), and crowd honest newcomers. Recovery after a flood ends: at once for pre-aged floods on stores below
  ~85% use, 4.5–5.5 days for fresh floods on a full store, 7–9 days for identities already Earned

## Finding content when identities are placed next to a key

Generating identities closer to a key than every honest server costs about 4 s of one core each, even with 10,000
servers, and 10 of them hide content completely today (found 0–4% of the time). Simulated in
[docs/simulations/eclipse](docs/simulations/eclipse/RESULTS.md); all local, nothing on the wire changes.

* Density check: a node estimates the network's size (median distance to the `K`-th closest node over its last
  ~16 lookups for other keys) and, when the nodes found near a key are packed more than 2× denser than that
  allows, widens in two rings: the nodes after the dense core, and the nodes beyond the radius where `K` honest
  servers would be expected. Publishers store `K` more per ring; searchers continue until 5 nodes of each ring
  have answered. About 4% false positives, costing +0.3 `/find` per search on average. Alone it raised
  findability under attack from 47% to 90% on average
* Disjoint lookup paths (S/Kademlia: lookups that share no node): publishers always use them (3 on fast nodes,
  2 on slow), at about +25 messages per publish on fast nodes, once per republish round; searchers run the
  density-checked lookup and fall back to disjoint paths only when it finds no provider. A search for content
  nobody provides pays the fallback, about 2.5–3× a normal lookup; slow nodes pace it like publish lookups
* Together: content found 98.9% of the time under attack on fast nodes (worst case 93%) and 90.8% on slow ones
  (worst 59%), against 35–42% today, for +0.36 `/find` on a normal search. A publisher's upgrade matters most:
  popular content, with many providers, is protected as soon as one of them has upgraded
* Announce first-seen log: a node records when it first heard each server's announce (persisted, capped like
  the routing table's other state); publishers also store to the `K` closest servers it first heard at least
  `provider_seniority` ago, and searchers continue until the closest such have answered. Free without an
  attack; total against newly generated identities (a new publisher beats them 88% of the time even for old
  searchers, against 61% without it), useless against pre-aged ones
* Not adopted: storing to the closest servers this node scores well (rarely the same set for publisher and
  searcher once there are 300+ servers), storing to 20 nodes, searching to 2`K`, and records under several keys
  (useful only with the density check, and it doubles the records stored)
* Joining without bootstrap nodes: a node still joins from the server announces it hears, as today. Once it has
  joined once, its saved routing table is its own bootstrap list: a join tries saved servers, best scored
  first, before the defaults. A node behind an RNS access-point interface hears no announces from beyond it,
  so its first join needs one reachable node ID (a default or its user's)

## LoRa meshes and the backbone

Kademlia ignores where nodes are. Internet nodes' lookups route through LoRa islands, a mesh cut off from the
internet can't find its own content, and records published while it was cut off sit on the wrong nodes after it
reconnects. Simulated in [docs/simulations/topology](docs/simulations/topology/RESULTS.md) (300–3,000 servers,
~21% of them on islands of 10–50 nodes); all local behaviour.

* Fast nodes ask slow-path servers last: servers more than 3 hops away, or whose link round trip rnfs measured
  above ~1 s, are ranked after all others in lookups, as unreliable nodes are now. Today an island answering 10
  lookups a day from each internet node would need 135–141% of its channel; this cuts the airtime internet
  lookups cost islands by 97% (27 s to 0.6 s each), while internet nodes still find island content 98% of the
  time
* A larger path-request budget on slow nodes: up to 10 per publish lookup and 3 per provider lookup (today 1).
  An island publisher's records otherwise reach only 1–3 of the 10 servers they belong on; with it, internet
  nodes find island content 99% of the time instead of 68% (3,000 servers), for 11–19% more publish airtime
* Two-level records: a publisher also stores to the 3 closest local servers (5 at most: +12% to +25% publish
  airtime), and a searcher asks the closest local server first, its reply seeding the global lookup. Local means
  the interfaces named in `local_interfaces`, or else within `local_hops` (3) over a slow interface (4 hops
  starts counting backbone nodes behind the gateway). Content stays findable during a partition (55–72% today,
  100%), and island-to-island lookups use 42% less airtime
* Partition mode: after 3 failures in a row reaching off-island servers, or ~10 minutes without an off-island
  announce, lookups and publishes use local servers only; any off-island announce or answer ends it, and a
  periodic off-island probe keeps it from sticking. Lookups during a partition take under a minute instead of
  ~300 s. On reconnecting, a node republishes (paced) the keys whose last round reached no off-island server, so
  records are found in hours instead of after the next republish (~3 days)
* Not adopted: choosing routing-table entries by proximity alone (fast, but lets an attacker one hop away take
  83–99% of a node's table; a version limited to half of each bucket's free slots, never displacing a healthy
  entry, is an option for later), preferring nodes with a known path (saves under 0.4 path requests a lookup),
  and a gateway that runs lookups for its island (a new request, and trust in the gateway)
* Upgrading helps in proportion: with half the nodes upgraded, content published before a partition is found 78%
  of the time (55% today). Two-level records need both publisher and searcher upgraded; the other changes help
  as soon as either side has

## Less announce traffic

Every node announces `rnfs.dht` every 6 hours and every name its own destination, and RNS floods announces across
the network. A LoRa channel's 2% announce allowance carries about 1,150 a day at 1 kbit/s, enough for about 290
rnfs nodes; at 100k nodes announces approach RNS's per-interface ingress limit on backbone links.

* Clients stop announcing `rnfs.dht`: they never enter routing tables, and RNS still answers path requests for
  them, so they stay reachable for `/blocks`. Servers keep announcing every 6 hours while 0.1.x nodes remain
  (an older node drops a server it hasn't heard from in 12 hours)
* Names resolve through keepers first, and a name is announced when it changes rather than every
  `announce_interval`; its owner still answers path requests, so older resolvers keep working

## Providing files inside folders

* A `provide_entries` setting that also provides the CID of every file and folder inside a complete pin, so
  each can be found by its own CID rather than only as `<root>/<path>`, and the same file in two folders can be
  found from either. It works with either `provide_strategy`; cached downloads only ever provide their root.
  Chunks and intermediate file/index nodes are not provided
* On by default on fast nodes, up to 100k entry keys per node. With record storage no longer capped, the cost
  left is the publisher's republishing: about 165 bytes per CID per round (16 bytes to each of `K` servers,
  plus request overhead), which sweep providing doesn't reduce, and entries multiply a node's keys 3–5× for
  folders of a few files and over 100× for large ones. A 70k-file folder is ~11.6 MB a day from a fast node
* On slow nodes only while the estimated republish airtime stays under ~2% of the channel (in practice about
  1,500 keys on a 100-server network, about 50 at 1,000 servers or more): one mid-size folder already exceeds
  what a 1 kbit/s node can republish. `provide_entries = true` or `false` overrides the default either way
* Unpinning a folder withdraws every entry it provided

## Gateway: an origin per site

* Subdomain gateway: `<cid>.rnfs.localhost:8789` and `<name>.name.localhost:8789`, so a page can `fetch()`
  files of its own DAG; the path gateway (`/rnfs/<cid>`, `/name/<name>`) stays
* rnfs-dag CIDs are 64 characters in base32, over DNS's 63-character label limit, so the subdomain form needs
  a shorter encoding (base36, as IPFS uses for subdomains). This is a gateway-only form; CIDs everywhere else
  stay base32
* Each origin keeps the current rules: no CORS headers, `Origin: null` refused, Host checks, sandbox CSP
  reviewed for the new origins

## Pin by NomadNet address

`rnfs add` recognises a NomadNet link and adds what the NomadNet node returns:

```sh
rnfs add e166ecdbc405bcc9762b6767617fc949:/page/index.mu   # the page as the node renders it
rnfs add e166ecdbc405bcc9762b6767617fc949:/file/song.mp3   # a file the node serves
```

* Syntax: `<32 hex destination>:/page/<path>` or `:/file/<path>`. A local file literally named like that is
  added with `./` in front. The same through the API and the library (add from a NomadNet URL); not through the
  gateway, so web pages can't make the node fetch
* Fetch: a link to the `nomadnetwork.node` destination with that hash, then a request for the path. Pages
  come back as bytes (an executable `.mu` page as rendered for this request); files as a Resource carrying a
  `name`. Request data (form fields, `var_` values) is not supported
* One-off snapshot: the result is added like a file (chunked, deny-list checked, pinned with source `user`;
  the usual `add` options apply, e.g. `--no-pin`, `--name`, `--background`) and the pin is labelled with the
  link. Running it again takes a new snapshot
* Anonymous by default. A NomadNet node answers a restricted page's anonymous visitor with its "Request Not
  Allowed" page rather than an error, so rnfs detects that page and fails instead of pinning it. `--identify`
  identifies with a dedicated identity (`storage/nomadnet_identity`, not the node's), so allowlisted pages can
  be fetched without tying the request to the node ID
* Limits: `max_download_size` and the disk reserve apply. NomadNet compresses files up to 32 MB and may send
  multi-segment Resources, while the daemon sets RNS's decompression limit to 0 process-wide; these links need
  their own bounded decompression and size checks, without loosening the rules on `rnfs.dht` links

## LXMF moves out of core

LXMF becomes a separate wrapper project built on the local API and the library.

* Users are never without the feature: if the wrapper's first release is ready with 0.2.0, 0.2.0 removes LXMF;
  if not, 0.2.0 keeps it, deprecated (a warning whenever LXMF features are used: CLI, API, settings), and the
  release after the wrapper ships removes it
* Contacts of kind `lxmf` stay in core's address book; the wrapper reads them through the API to decide which
  senders it trusts

* Removed from rnfs: `rnfs/features/lxmf.py`, `send`, `inbox`, `outbox`, `lxmf address` and `lxmf sync`, their
  API routes and library methods, the LXMF inbox events, the `lxmf_*` settings, and the LXMF dependency in
  every package
* Kept: `rnfs:` URIs (section 13's syntax) and the LXMF link-map field definition, as the interop contract
  the wrapper and other clients use; the message, receiving and auto-fetch rules move to the wrapper's spec
* Upgrade: `lxmf:<address>` pins become `app:lxmf:<address>` pins (below), which the wrapper owns and releases;
  `storage/lxmf_identity` and `lxmf/` are left on disk for the wrapper to adopt, so its LXMF address doesn't
  change. `lxmf_*` settings left in `rnfs.toml` are ignored with a notice (an unknown setting stops the daemon
  today)

## Per-app API tokens and pin sources

* `rnfs app add <name>` issues a labelled token for an application; `rnfs app ls` and `rnfs app rm <name>`
  list and revoke them. The current `api_token` stays as the full-access token for the CLI
* App tokens are not scoped: an app can do anything the CLI can, including exporting name keys, changing
  settings and stopping the daemon. Issue them only to apps you trust that far
* Pins made with an app's token get the source `app:<name>` (or `app:<name>:<key>`, so an app can hold and
  release its own groups), shown by `pin ls`, and released only by that app or by removing the app

## API reference

* No API stability is promised until 1.0.0: `rnfs.API_STATUS` stays provisional, and the local API and the
  library may change in 0.2.x releases (`/api/capabilities` says what a daemon has)
* An OpenAPI reference for the local API (request and response shapes, error codes), checked against the
  code by tests as `rnfs/api.py`'s route list is today

## Startup Scan

Bind the API before the scan, or log "scanning N blocks…", so status can say "starting" instead of "no daemon is running". This is the cheapest fix for the confusing message.

Store block sizes in rnfs.db and update them as blocks change, so startup doesn't rescan the directory. This removes the delay itself.


## Block storage layout

* Blocks move from one flat `blocks/` directory into subdirectories named by the second- and third-to-last
  characters of the CID (1,024 of them: the leading characters are the same for every CID of a codec, and the
  last carries only 2–3 bits of the digest). One directory of millions
  of files is slow to list and scan, and FAT32, common on the USB drives Pi users add, allows about 65,000
  entries per directory. The first start of 0.2.0 moves existing blocks (a rename within the filesystem), and
  goes with the Startup Scan work above

## Network status

* `rnfs status` (and `GET /api/status`) reports what the node can see of the network: the estimated number of
  DHT servers (the density check's size estimate), how often the density check fired recently, partition mode
  and how long it has lasted, records stored against the record budget and how full each storage tier is, and
  how many newcomers were refused or evicted recently. It is the cheapest way to learn how big the network is
  and to notice a flood or an attack on a key, and needs nothing on the wire

## Encrypted folders

* `add --to` accepts a folder: rnfs packs it as one tar stream (the existing writer of `GET /api/get/stream`)
  and seals that as a single sealed file, so the folder's names and structure are hidden along with its
  content. Sealed files are unchanged: the sealed name is the folder's name plus `.tar`, so a recipient on 0.1.x
  gets a tar file it can unpack itself
* `get` of such a file, by a recipient, decrypts it and unpacks the tar under the same rules as the tar reader
  of `POST /api/add/stream` and the write-out rules of `get` (regular files and directories only, no absolute
  or `..` paths, entry-count and depth limits, written under a temporary name and renamed when complete);
  `--no-extract` writes the `.tar` instead. `cat` and the gateway still give the sealed bytes

## Name key backups

* Losing a name's key loses the name for good: nothing can move it to a new key. `name create` and `name import`
  say so and print the `name export` command; the node remembers which keys were exported since they were
  created, and `rnfs status` lists names whose key has never been exported

## Publishing a folder to a name

* `rnfs name sync <alias> <folder>` keeps a name pointing at a folder's current content: every `scan_interval`
  the folder is imported as the shared folder is (unchanged files recognised by their metadata, not re-read;
  the same refusal of links, files with other hard links, and folders inside the rnfs directory), and when its
  root CID changes the name is published to it. The previous target's `name:` pin is released as with any
  publish, so only the latest version is kept
* Publishes are debounced: at most one an hour per name (RNS rate-limits a destination's announces to about
  that), and only once the folder has been unchanged for a short settle time, so a half-copied folder isn't
  published. FastCDC is the default chunker for synced folders, so an edited file shares most of its blocks
  with the version before. `rnfs name unsync <alias>` stops it and keeps the name where it last pointed. Also
  through the API and the library

## Remote pin status

* `rnfs pin remote ls` lists the remote pins this node asked for: node, CID, when it asked, and whether it is
  done, worked out locally by looking up the CID's providers and finding that node among them (paced on slow
  nodes, refreshed on demand with `--check`). A pin the node accepted but later dropped (quota, or the CID
  couldn't be found) shows as not done after a while; `/pin` itself is unchanged
* `rnfs pin remote ls --served` lists the pins other identities asked of this node: identity, CID, state
  (waiting, fetching, pinned, dropped and why) and each identity's use of `remote_pin_quota`. Both through the
  API and the library

# After 0.2.0

Held back until there is evidence they are needed.

## Not planned

* A global `PROVIDER_TTL` of 48 hours (as in IPFS) would clear floods faster, but older nodes republish every 3
  days (slow nodes, kept names) and would lose their records: it breaks compatibility. The 4.5-day newcomer
  expiry gets most of the benefit locally
