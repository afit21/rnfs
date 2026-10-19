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
also costs its own closest-nodes lookup at every republish, which limits a slow node to about 8,600 roots.
Every record is also held in memory (about 590 bytes each: ~59 MB at the default 100k).

* Sweep providing: group the keys due for republishing (and withdrawals) by keyspace region, run one
  closest-nodes lookup per region, and send the keys in batched `/provide` requests (up to 55 keys each).
  Slow nodes keep their pacing, counted in lookups rather than keys
* Provider records on disk: records, publishers' first-seen times and the newcomer pool live in the state
  database and are read per `/find` through an index, with a small cache, instead of loaded whole into memory.
  Writes from republishing are grouped into one transaction every ~30 s (about 12 rows/s for 1M records
  refreshed daily). Eviction uses an index rather than scanning a publisher's keys
* No fixed record count: `max_records` goes, and the store is bounded by disk space instead. It is *full* when
  it would leave less than the disk reserve free (max(256 MiB, 2%), as for blocks). While it has room, every
  publisher may store as much as it likes; the tiers below decide what is evicted once it is full. The cap of
  1000 publishers per key stays, so a `/find` stays cheap. A `max_records` left in `rnfs.toml` is ignored with a
  notice (an unknown setting stops the daemon today)
* Persist the routing table, with each entry's first-seen time and score (below), so a restart doesn't re-flood
  path requests on radio to rejoin, and peers keep the standing they earned
* Benchmark: one node publishing a large collection (e.g. the ~70k-file Gutenberg mirror) over rnfs, kept
  findable through republishing

## Resisting floods and eclipses

RNS identities are free and RNS has no IP addresses, so no cap keyed on an identity bounds an attacker, and no
deployed DHT's defence (IP diversity, a directory authority) carries over. The model: **share freely while there
is room; when there isn't, nodes this node knows keep their guaranteed space and strangers compete for a small,
fast-expiring pool.** Background research: [docs/sybil-resistant-publish-caps.md](docs/sybil-resistant-publish-caps.md).

* Local scores: each node keeps a private score for the peers it deals with, raised by useful work it can
  check itself: blocks served that verified against their CIDs, `/find` answers that led to real providers or
  closer servers, and refreshing records on several distinct days. Nothing is gossiped or published. Scores
  are a few numbers on rows that already exist (routing entries, publishers of stored records), in a capped
  table that forgets the least recently useful peer first; a peer without a row scores 0, as a newcomer, so
  forgetting is always safe. State grows with the node's own activity, not with the size of the network
* Bootstrap nodes start with a seeded score, so a fresh node has someone to lean on from its first minute. They
  keep it only by earning it like any other peer (failures, silence and bad blocks lower it, bans still apply).
  `default_bootstrap = false` drops both their joining role and their seeded score. A user's own
  `bootstrap_nodes` get the same seed, which matters most on meshes without the internet
* Storage tiers, applied only once the store is full (floors are illustrative, to be tuned by simulation):

  | Tier | Who | Guaranteed share | Records expire after |
  |---|---|---|---|
  | Known | Contacts, private-mode members, seeded bootstrap nodes | ~30% | `PROVIDER_TTL` (7 days) |
  | Earned | Publishers with a high local score | ~60% | `PROVIDER_TTL` (7 days) |
  | Newcomer | Everyone else, including 0.1.x publishers new to this server | ~10% | 4 days |

  Records are reclaimed from the lowest tier first, and within a tier from the publisher furthest over its fair
  share; a lower tier never evicts a higher one. A full key (1000 publishers) also gives way lowest tier
  first. Newcomer records expire after 4 days, above the slowest honest republish interval (3.3 days: 3 days +
  10% jitter for slow nodes and kept names), so a flood clears in 4 days rather than 7 while no honest record
  lapses
* Lookups, against eclipse (identities generated next to a key, which costs about one key generation per
  server in the network):
  * Density check: if the `K` closest nodes to a key are far closer than the network's size allows, widen the
    lookup and store to the next ring out too
  * Alongside the `K` closest nodes, store to and ask the closest few servers this node itself scores well, so
    newly generated identities never displace them
  * Disjoint lookup paths (S/Kademlia): 3–4 lookups sharing no node on fast nodes, 1 on slow nodes
* Joining without bootstrap nodes: a node still joins from the server announces it hears, as today. Once it has
  joined once, its saved routing table is its own bootstrap list: a join tries saved servers, best scored
  first, before the defaults. A node behind an RNS access-point interface hears no announces from beyond it,
  so its first join needs one reachable node ID (a default or its user's)
* What this can't do: a patient attacker who contributes real work for days can earn standing, and a node with
  no history relies on the density check and disjoint paths alone. Content can never be forged under any of it

## Providing files inside folders

* A `provide_entries` setting that also provides the CID of every file and folder inside a complete pin, so
  each can be found by its own CID rather than only as `<root>/<path>`, and the same file in two folders can be
  found from either. It works with either `provide_strategy`; cached downloads only ever provide their root.
  Chunks and intermediate file/index nodes are not provided
* On by default, except on slow nodes. With record storage no longer capped, the cost left is the publisher's
  republishing: about 165 bytes per CID per round (16 bytes to each of `K` servers, plus request overhead),
  which sweep providing doesn't reduce. A 70k-file folder is ~11.6 MB a day from a fast node, but on a slow
  node ~3.9 MB every 3 days, about a third of a LoRa channel permanently. `provide_entries = true` or `false`
  overrides the default either way
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

# After 0.2.0

Held back until there is evidence they are needed.

## Publisher stamps

* A weekly proof of work that lets an unknown heavy publisher earn storage beyond the newcomer tier without
  anyone vouching for it: a nonce for scrypt (N=2^14, r=8, 16 MiB) over `rnfs context ‖ node ID ‖ epoch`, the
  epoch being 7 days to match `PROVIDER_TTL` (current and previous accepted). A one-time stamp would bound only
  how fast an attacker gains identities, not how many it holds
* Sent lazily with a new `/stamp` request (additive) only when a `/provide` is refused, answered with the share
  granted; checked once per publisher per epoch, cached, and rate-limited per link and overall
* A price, not a barrier: a rented GPU buys a server's worth of stamped allowance for about a cent a week, so
  stamps get their own bounded tier and can never evict known or earned publishers. Before adopting, time
  scrypt verification on a Pi Zero 2 and a Pi 4

## Records under several keys

* Provide each CID under 2–3 independent keys (`H(i ‖ cid)`) besides its usual one, so hiding it means eclipsing
  several unrelated parts of the keyspace. Older nodes keep using the usual key. Costs publish airtime: 1 key
  on slow nodes

## Shorter provider record lifetime

* A global `PROVIDER_TTL` of 48 hours (as in IPFS) would clear floods faster, but slow nodes republish every 3
  days and kept names every 3 days or more, so it needs them to republish more than daily first (cheaper with
  sweep providing) and is a protocol change: a later major version
