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
there are no 0.1.x releases in between. It is the last foreseeable feature release: everything planned is listed
here, and later 0.2.x releases are for fixes, the items below still to do, and those that wait on others (older
nodes leaving the network, Reticulum). Nodes of 0.1.3 and later keep working with 0.2.0 nodes: no CID changes,
wire changes additive only, and every state change gets an upgrade migration from 0.1.6.

## Local API and library for companion apps (implemented)

What companion apps need from the daemon, most impactful first. Planned as 0.1.7, which wasn't released.

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
* As implemented: everything above, planned as 0.1.7 (which wasn't released). The push endpoint's `inbox` events went
  with LXMF's removal; `rnfs.Client`'s LXMF methods too.

## Providing at scale (implemented)

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
* Republish jitter per region rather than per key, so a region's keys stay due together: kept names' fixed
  ±10% jitter is derived from the key's first byte instead of the whole key (256 slots, and a region's keys
  share their leading bits). Slow nodes pace per region, not per key; withdrawals ride in the same batches, and
  are paced on slow nodes too, one region lookup's worth of budget per server they alone reach (unpinning 1,000
  keys costs ~2.9 h of 1 kbit/s airtime at 1k servers today)
* One publish round does everything below that adds stores, per region:
  1. Due keys (with `provide_entries`' entry keys) and queued withdrawals; slow nodes budget region lookups
  2. The region's lookup runs as disjoint paths (3 fast, 2 slow; up to 10 path requests on slow nodes; local
     servers only in partition mode), then the region is found and its extra `/find` sent as above
  3. Each key's store set, from what the region learned: its `K` closest; if the density check flags the key
     on the region's nodes, a widened lookup of its own and `K` more per ring (counted in the round's lookup
     budget); the `K` closest servers of the announce first-seen log heard at least `provider_seniority` ago;
     the 3 closest local servers. The union, without duplicates
  4. Store sets grouped by server, each server's queued withdrawals first in its batches, one identified link
     per server; every server stored to is recorded, so withdrawals reach ring, senior and local stores too
  5. Keys whose round reached no off-island server are marked for the heal republish

  On a 1 kbit/s node a region costs about 34–44 s (2 disjoint lookups plus the extra `/find`), a key about
  3.9 s (13–15 stores batched; about 7 s when the density check widens it), and each server contacted 6.4 s
  for its link: with these extras a slow node keeps about two thirds of the key counts in the next bullet
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
* A record budget of its own instead of a record count: `max_records` goes, and records may use
  `record_budget` of disk (default 1 GiB, about 9M records at 117 bytes; the size per record is measured from the
  database, not assumed), never the disk reserve that blocks need, so a flood of records can't stop the node
  storing content. A `max_records` left in `rnfs.toml` is ignored with a notice (an unknown setting stops the
  daemon today): `config.py` gets a list of retired settings, dropped with a warning, which the `lxmf_*`
  settings join later. The per-publisher caps, the newcomer pool and the departed/evicted memories go; their
  tables are migrated by a schema step. The cap of 1000 publishers per key stays, so a `/find` stays cheap
* Persist the routing table, with each entry's first-seen time and score (below), so a restart doesn't re-flood
  path requests on radio to rejoin, and peers keep the standing they earned. Times are stored as wall clock
  (clamped like provider records'), so `/find` replies still leave out nodes silent for 2 × `announce_interval`
* Benchmark: one node publishing a large collection (e.g. the ~70k-file Gutenberg mirror) over rnfs, kept
  findable through republishing
* As implemented: sweep providing as designed (`rnfs/dht/sweep.py`): over a simulated network with real lookups, 100% of
  keys got their true 10 closest (per-key lookups 97–98%), lookups per round level off at servers ÷ 12–13, and a region
  costs 1.5–1.6 extra `/find`. A server outside a region that names one inside it is asked too, which took accuracy from
  98.3–100% to 100%. `/provide` goes to 16 servers at once on fast nodes. Slow nodes run 10 region lookups a round; a
  server only withdrawals reach counts as one of them, with up to 5 of the 10 kept for withdrawals while any are queued.
  Kept names' jitter comes from the key's first byte, as planned; on networks of up to about 2,500 servers a region
  still spans several slots, so its kept names come due in up to 256 rounds per interval. The store set is built from
  parts (`add_store_part`): the region's 10 closest, each disjoint path's 10 closest, the density rings, the first-seen
  log's senior servers (heard in the last 2 × `announce_interval` too, so departed ones cost nothing) and the local
  servers. Provider records are on disk as planned; measured on disk they take about 150 bytes each, 240 when each has a
  publisher of its own, so the default 1 GiB `record_budget` holds about 7 million. The budget is measured as the whole
  state database's size (SQLite's per-table statistics are optional and read every page), so other state counts against
  it too, mostly the block-size table: a node holding about 10M blocks may want a larger budget. A flood of 300,000
  fresh identities into a full 200,000-record store cost about 70 µs a store. The routing table is persisted with
  wall-clock times. Not done: the Gutenberg mirror benchmark, which needs the mirror and real hardware.

## Resisting floods (implemented)

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
  State grows with the node's own activity, not with the size of the network. Scores order providers within
  each seniority group (spec section 6: seniority MUST stay the first key, and a score can be earned quickly by
  serving popular blocks) and the servers a join tries; the storage tiers use only the refresh-day test below
* Bootstrap nodes start with a seeded score, so a fresh node has someone to lean on from its first minute. They
  keep it only by earning it like any other peer (failures, silence and bad blocks lower it, bans still apply).
  `default_bootstrap = false` drops both their joining role and their seeded score. A user's own
  `bootstrap_nodes` get the same seed, which matters most on meshes without the internet
* Storage tiers, applied only once the record budget is full (anyone stores freely while there is room):

  | Tier | Who | Floor | Records expire after | Eviction inside the tier |
  |---|---|---|---|---|
  | Known | Contacts of kind `node`, private-mode members, seeded bootstrap nodes | 30% | `PROVIDER_TTL` (7 days) | Largest holder first |
  | Earned | Held a record and refreshed it on at least 3 distinct UTC days | 60% | `PROVIDER_TTL` (7 days) | **None: Earned publishers never evict each other** |
  | Newcomer | Everyone else, including 0.1.x publishers new to this server | 10% | 4.5 days (while full) | Largest holder first; one holding as many as the largest is refused |

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
* The shorter expiry applies, like the tiers, only while the record budget is full: it is local server policy,
  one more way a full store makes room (spec section 6 states it next to eviction). In peacetime every record
  lives `PROVIDER_TTL`, so a record still survives one missed republish (the reason `republish_interval` MUST be
  below `PROVIDER_TTL / 2`) and nothing changes for 0.1.x publishers; a flood fills the budget, so recovery is as
  simulated
* Not adopted, as none helped any honest publisher in simulation and each cost newcomers or joining mirrors:
  admission tickets that wait as the store fills, caps or daily insert limits for newcomers, a limit on
  promotions into Earned, weighting by age, a separate Senior tier, and requiring 5 days rather than 3
* What remains: a pre-aged attacker can take free space and the Newcomer floor, absorb space that honest
  publishers leaving the network free (about 6–10% of large publishers' records over three weeks at 1% churn a
  day), and crowd honest newcomers. Recovery after a flood ends: at once for pre-aged floods on stores below
  ~85% use, 4.5–5.5 days for fresh floods on a full store, 7–9 days for identities already Earned
* As implemented: the tiers as tabled. Earned counts refresh days after the day of a first store. The Known tier also
  refuses a storer already holding as many records as its largest holder, so two Known publishers can't churn each
  other. A full key evicts a random member of the lowest tier present, never one of the `K/2` oldest records. At the
  upgrade, a 0.1.x publisher starts Earned only if it was first seen at least 3 days before its latest store and had
  refreshed a record outside the old newcomer pool a day or more after storing it. Scores: +1 for a `/blocks` response
  whose blocks verified, +0.5 for a useful `/find` answer, −1 for a failure, −32 for a block not matching its CID; they
  halve every 7 days without new work, stay within ±64 and are compared in power-of-two bands; at most 4,096 peers are
  scored. Bootstrap nodes are seeded with 6. A join asks up to 6 saved servers first; after such a join a fast node
  still asks the default bootstrap nodes (at most 2), so one that saved only an island reaches the rest; a slow node
  doesn't.

## Finding content when identities are placed next to a key (implemented)

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
* As implemented: all four mechanisms (`rnfs/dht/density.py`, `disjoint.py`, `lookups.py`). The size estimate is kept in
  the state database, so a restart has it at once; the density check waits for 8 lookups. Searchers wait for the 3
  closest of the log's senior servers rather than all 10 (all 10 cost about 5 more `/find` per search). A fallback
  search gets what is left of the first lookup's time and of its 80 `/find`, so a search still sends at most 80. Over
  the fake network of `tests/test_eclipse.py` (2,000 servers, 10 or 20 identities placed closer to a key than every
  honest server): found 100% of the time by upgraded fast nodes and 99.2–100% by slow ones, 0% by 0.1.x nodes; 2–6%
  false positives; +0.2 to +0.5 `/find` per search; about +17 `/find` and +3 stores to publish one key; a search for
  content nobody provides about 4× a normal one. Over 1,000 servers with sweep providing, 0.6% of keys needed a widened
  lookup. Fast nodes keep up to about 36 `/find` in flight while publishing.

## LoRa meshes and the backbone (implemented)

Kademlia ignores where nodes are. Internet nodes' lookups route through LoRa islands, a mesh cut off from the
internet can't find its own content, and records published while it was cut off sit on the wrong nodes after it
reconnects. Simulated in [docs/simulations/topology](docs/simulations/topology/RESULTS.md) (300–3,000 servers,
~21% of them on islands of 10–50 nodes); all local behaviour.

* Fast nodes ask slow-path servers last. Today an island answering 10 lookups a day from each internet node
  would need 135–141% of its channel; this cuts the airtime internet lookups cost islands by 97% (27 s to 0.6 s
  each), while internet nodes still find island content 98% of the time. Rules:
  * Slow path: RNS has a path of more than 3 hops, or this node's own last link to the server (within 24 h) had
    a round trip above ~1 s **and** a rate estimate below `slow_bitrate` (RTT alone would demote I2P nodes,
    which cost no radio airtime). No path and no measurement: ranked normally
  * Ordering only, in lookup queries (closest nodes and providers): answered or fast/unknown first, then slow
    path, then unreliable, by distance within each. Still asked when too few others remain, still fill `K`
    for stores. Never touches the routing table, `/find` replies, provider ranking or failure counts
  * Only a transport node on the path can raise a hop count (announce signatures don't cover it) or delay a
    link to inflate its RTT, and such a node could already drop the traffic; a hub could demote the nodes
    behind it, but not remove them
* A larger path-request budget on slow nodes: up to 10 per publish lookup and 3 per provider lookup (today 1).
  An island publisher's records otherwise reach only 1–3 of the 10 servers they belong on; with it, internet
  nodes find island content 99% of the time instead of 68% (3,000 servers), for 11–19% more publish airtime
* Two-level records: a publisher also stores to the 3 closest local servers (5 at most: +12% to +25% publish
  airtime), and a searcher asks the closest local server first, its reply seeding the global lookup. Content
  stays findable during a partition (55–72% today, 100%), and island-to-island lookups use 42% less airtime
* Local is judged from the path to a node, never from the interface a request arrived on: the next-hop
  interface is named in `local_interfaces`, or else the node is within `local_hops` (3) over a slow interface.
  Attached to a shared instance (rnsd), hop counts are exact but the next-hop interface is the local one, so
  the real interface comes from the instance (`get_next_hop_if_name`, `get_path_table`, which already back
  provider ranking); `local_interfaces` names the instance's interfaces as rnsd's config names them, matched
  like `serve_interfaces`, and the local servers are found with one `get_path_table(max_hops=local_hops)` at
  start, refreshed periodically (a client's own path table starts empty). If the instance can't be asked,
  nothing is local. A node one LoRa hop from its gateway reaches servers on the gateway's internet hub in 3
  hops, so behind a big public hub set `local_interfaces` or a lower `local_hops`
* Partition mode: after 3 failures in a row reaching off-island servers, or off-island announce silence over
  4× the usual gap between off-island announces this node measured (at least 10 minutes: with servers
  announcing every 6 h, an island hearing 20 of them hears one every ~18 minutes), lookups and publishes use
  local servers only; any off-island announce or answer ends it, and a periodic off-island probe keeps it from
  sticking. Lookups during a partition take under a minute instead of
  ~300 s. On reconnecting, a node republishes (paced) the keys whose last round reached no off-island server, so
  records are found in hours instead of after the next republish (~3 days)
* Not adopted: choosing routing-table entries by proximity alone (fast, but lets an attacker one hop away take
  83–99% of a node's table; a version limited to half of each bucket's free slots, never displacing a healthy
  entry, is an option for later), preferring nodes with a known path (saves under 0.4 path requests a lookup),
  and a gateway that runs lookups for its island (a new request, and trust in the gateway)
* Upgrading helps in proportion: with half the nodes upgraded, content published before a partition is found 78%
  of the time (55% today). Two-level records need both publisher and searcher upgraded; the other changes help
  as soon as either side has
* As implemented: as planned, with lookup candidates ranked in three tiers (first, slow path, unreliable). A server
  whose path was unknown is judged again once its path request is answered, before a link is opened. On slow nodes a
  publishing lookup's 10 path requests are split over its 2 disjoint paths, and a search gets 3, plus 3 over its
  fallback. Silence alone only triggers a probe of an off-island server; partition mode starts only if that probe fails,
  which is what keeps small connected networks from flapping. Probes go every 5 minutes, or on slow nodes from 15
  minutes doubling up to 2 hours. A path-table scan runs at start and every 10 minutes, and at most 1,024 local servers
  are kept. Heal marks are persisted (`published_heal`). Over the fake network of `tests/test_topology.py` (1,000
  servers, 21% on 8 islands): island `/find` per internet search 0.76 → 0; island stores per internet publish 2.47 →
  0.07; an island publisher's records on 6.9 of their true 10 closest instead of 3.6. During a partition, a neighbour
  found what an island node published before it 100% of the time instead of 67%, in 31 s of simulated time per search
  instead of 490 s. The internet found what was published during the partition 0% of the time until the heal republish,
  99.5% after it. Not tested over a real island.

## Less announce traffic (implemented)

Every node announces `rnfs.dht` every 6 hours and every name its own destination, and RNS floods announces across
the network. A LoRa channel's 2% announce allowance carries about 1,150 a day at 1 kbit/s, enough for about 290
rnfs nodes; at 100k nodes announces approach RNS's per-interface ingress limit on backbone links.

* Clients that only fetch stop announcing `rnfs.dht` periodically (they still announce at startup, after
  `startup_announce_delay` and on a mode change): nothing needs to reach them. A client that provides anything
  (records out, kept names), accepts remote pins or is in private mode keeps announcing every
  `announce_interval`. A destination that never announces is not reachable beyond one hop on most networks: a
  transport node with no path forwards a path request only on access-point, gateway, roaming or boundary
  interfaces or with `recursive_prs` (RNS 1.5.6 `Transport.path_request`), and the default mode is `full`. And
  private-mode servers of 0.1.4–0.1.6 drop at start the records of publishers whose identity RNS doesn't recall
* Servers keep announcing every 6 hours while 0.1.x nodes remain (an older node drops a server it hasn't heard
  from in 12 hours)
* Names keep announcing every `announce_interval`, and on every change: a resolver of 0.1.5 or later counts an
  announce as current for only 2 × `announce_interval` after it was emitted (spec section 9), so a name
  announced only on change would add a keeper query to every resolve of an unchanged name (ten to twenty times
  a path-answered resolve's airtime), and transport nodes answer path requests from their cached, old announce
  anyway
* As implemented: as written. Whether a client needs reaching is checked every minute, and it announces at once
  when it starts to. Servers and names announce as before.

## Pacing path requests (to do)

Since RNS 1.2.5 every interface limits the path requests arriving on it, and the release notes say applications
that send many will see their users lose path resolution, by design. In RNS 1.5.6 (`Interface.should_ingress_limit_pr`,
`Transport.inbound`) an interface, each TCP client a hub spawns included, estimates the rate of incoming path
requests from its last few; above 8 a second, or 3 while the interface is under 2 hours old, later ones wait in a
queue of 8 behind all other traffic, the rest are dropped, and those let through aren't forwarded to discover an
unknown path. The limit holds for at least 15 s after the rate falls. It counts every application's path
requests on that interface, so an rnfs burst on a shared radio costs other users their path resolution too.

rnfs paces nothing node-wide: every link it opens to a node without a known path sends one (`links.py` `_open`),
publishing runs 16 stores at once (`PROVIDE_PARALLEL`), lookups several queries, and slow publishers now up to 10
per lookup. A query that failed because its path request was dropped counts as the node's failure (`node.py`
`_query_failed`), towards partition mode included, so a node newly connected to a hub could enter partition mode
by publishing. All local; no wire change.

* Spec section 3 describes RNS's path-request limits, as it does the 2% announce cap
* One node-wide budget for path requests: lookups, link opens, name resolves, keeper queries, NomadNet adds and
  in-core LXMF while it remains. Requests are spaced at least 1 s apart, so RNS's estimate stays under 1.5 a
  second, below a new interface's 3 with room for other traffic. A request waiting for the budget is not a
  failure, and its time doesn't count towards `path_timeout`
* While a lookup waits for the budget, it queries the next candidates with a known path instead of idling. This
  revisits "preferring nodes with a known path", not adopted above when a path request cost only airtime
* A query that failed for want of a path doesn't count towards partition mode: during a real partition paths to
  off-island servers stay in RNS's table and fail as links, and announce silence detects the rest. It still
  counts in the routing table, where a wrongly dropped node regains its seniority within 2 × `announce_interval`
* Re-run the topology simulation with the limit modelled, before the slow nodes' larger budget and sweep
  providing ship: path requests a publish round and a join send, and how often each limit trips at a gateway and
  a public hub
* Spec section 6 (or Appendix A) says what announces carry and why provider records aren't announced: servers
  are found from `rnfs.dht` announces and names live in their own, but a 1 kbit/s channel's 2% allowance (about
  1,150 announces a day) covers node presence for about 290 nodes, not an announce for each CID

## Less link and lookup traffic (to do)

Traffic rnfs sends that does no work. Counts are from RNS 1.5.6's code, not yet measured. All local; no wire change.

* Close idle links before RNS keeps them alive. The opener of a link sends a keepalive after `max(5 s, RTT × 206)`
  of quiet, capped at 360 s, and the other end answers (`Link.__update_keepalive`, `Link.__watchdog_job`); rnfs
  closes an outgoing link only after `link_idle_timeout` (5 min) without data, which keepalives don't count as. On
  a 50 ms backbone path that is a pair every ~10 s, about 58 packets per idle link, against about 6 for the lookup
  that opened it, and every transport node on the path relays them. On radio paths (RTT ≥ 1.75 s) the interval
  is 360 s, so none are sent. An outgoing link with nothing in progress closes once it has been idle for its
  keepalive interval or `link_idle_timeout`, whichever is shorter: reopening costs about 4 packets, less than two
  keepalive pairs. Measure how often links are reopened within 5 minutes before and after (spec section 3, Links)
* Path requests only where the node can be. A path request without an interface goes out on every interface
  (`Transport.request_path`), so a node with a LoRa and a TCP interface sends its lookups' path requests for
  internet nodes over its radio too. A node running its own RNS instance sends the path request for an ID named
  in a `/find` reply only on the interface its path to the replying node uses (`Transport.next_hop_interface`,
  `request_path(on_interface=...)`); a lookup has other candidates, so a miss isn't retried on every interface.
  Joins, names and links to known IDs ask on every interface, as now. Through a shared instance nothing changes:
  `rnsd` forwards a client's path request on all its interfaces (`Transport.path_request`), so scoping it there
  needs an RNS change. RNS documents `on_interface` as not for normal use: check its behaviour on each interface
  mode before relying on it
* Remember providers. Nothing is kept between fetches, so fetching a CID again once its links have closed (a
  gateway reload, `pin add` after `cat`) repeats its lookup, and a search for content nobody provides (about 13.5
  messages) repeats on every retry. A node keeps the providers a lookup found for each CID for a few hours
  (records live 7 days; bounded, least recently used first) and asks them in step 1 of fetching, with the nodes
  it has open links to, before looking up; one that answers `None` is forgotten for that CID. A lookup that found
  no providers isn't repeated for the same CID for 10 minutes, except when the user asks again explicitly (spec
  section 7, Fetching a target)

Open (owner's decision):
* One link for publishing. A publish looks up servers over the shared anonymous link, then opens a separate
  identified link to each server it stores to: about 5 more packets per stored server each round, on radio for a
  slow publisher. Identifying the publishing lookup's links from the start and storing over them saves that, but
  every server the lookup queries, not just those that store the record, learns which identity publishes which
  key
* Republishing less often on fast nodes: 24 h today, 3 days on slow nodes, and it MUST stay under 3.5 days
  (`PROVIDER_TTL / 2`). Two or three days cuts fast nodes' stores 2–3×; records lost to departed servers stay lost
  longer. Measure with the storage and topology simulations, and against the 4.5-day newcomer expiry
* Compressed `/blocks` responses. Text (the Gutenberg mirror) would take about 2.5–3.5× less airtime, with CIDs
  unchanged. `/blocks` can't change shape, so it needs a new optional request; a node without a handler for it
  drops the request unanswered (`Link.handle_request`), so a requester must learn per node whether it is
  supported, at the cost of a timeout to each older node. Decompression must be bounded before any data is kept,
  as the NomadNet add's links are

## Providing files inside folders (implemented)

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
* As implemented: the 100,000-key cap takes pins oldest first, each pin's entries all or none, so a new pin never takes
  entries from an older one. On a slow node the airtime estimate counts about 2 senior-server stores per key on top of
  `K`, so the thresholds are about 1,300 keys at 100 servers and 40 from 1,000 servers at 1 kbit/s; once on, entries
  stay on until the estimate passes 2.5%. The network size comes from the publish rounds' regions and the density
  check's estimate, whichever is larger. `rnfs status` gains `provided_entries`, `provide_entries_off` and
  `provide_entries_capped`, and `provided` counts entries.

## Gateway: an origin per site (implemented)

* Subdomain gateway: `<cid>.rnfs.localhost:8789` and `<name>.name.localhost:8789`, so a page can `fetch()`
  files of its own DAG; the path gateway (`/rnfs/<cid>`, `/name/<name>`) stays
* rnfs-dag CIDs are 64 characters in base32, over DNS's 63-character label limit, so the subdomain form needs
  a shorter encoding: `k` + base36 of the binary CID, as IPFS uses for subdomains (56 characters raw, 60
  rnfs-dag). It is a host-only encoding, accepted only by the gateway's host parser (exact length, canonical),
  and converted to the base32 CID before any other use: spec section 4 says so next to its rule that a CID has
  exactly one written form, which stays true everywhere else. The API's exact Host check isn't widened
* Today's CSP sandboxes every page without `allow-same-origin`, which gives it an opaque origin, so its own
  `fetch()` is cross-origin and refused: subdomain responses get `sandbox allow-same-origin allow-scripts
  allow-forms allow-popups` (scripts only with `gateway_scripts`), `Origin-Agent-Cluster: ?1` and
  `frame-ancestors 'self'`; the path gateway keeps today's CSP. Otherwise the current rules: no CORS headers,
  `Origin: null` refused, nosniff
* Requests for a service worker script (`Service-Worker: script`) are refused: on `<name>.name.localhost` a
  worker could keep serving old content after the name moves. `?format=` is refused on subdomain hosts, and
  empty path segments still give 400 (no protocol-relative redirect)
* Documented: `localhost` isn't a public suffix, so every `*.rnfs.localhost` site is one *site* and they share
  cookies (localStorage and IndexedDB are per origin); not every resolver maps `*.localhost` to loopback
  (browsers do), so the path gateway stays the fallback; links between sites must be absolute subdomain URLs
* As implemented: as written, plus `rnfs url <cid>|<name>[/path]`, which prints a target's site URL. Every
  `*.rnfs.localhost` site shares cookies, as does every `*.name.localhost` site; neither the gateway nor the API uses
  cookies. Service workers stay refused.

## Pin by NomadNet address (implemented)

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
  multi-segment Resources, while the daemon sets RNS's decompression limit to 0 process-wide. That limit is
  copied into each Resource when it is created (`max_decompressed_size`, RNS 1.5.6) and a link's
  `resource_started` callback runs before any data arrives (rnfs's LXMF feature used both), so these
  links are bounded per link, and `rnfs.dht` links keep every rule they have:
  * requests carry `max_response_size` = min(`max_download_size`, free space above the disk reserve)
  * each segment gets `max_decompressed_size` = one segment (`MAX_EFFICIENT_SIZE` + 1), and must carry the same
    original hash and total size, the next segment index, and the segment count an honest sender computes from
    the size; otherwise the transfer is cancelled and the link torn down (RNS checks neither the sequence nor
    the decompressed size against the advertised one)
  * the assembled file must be no larger than advertised, and is copied into storage inside the response
    callback (RNS deletes it after); a page response must be one byte string, checked from its msgpack headers
    before RNS decodes it, as `rnfs.dht` responses are
* As implemented: as written, modelled on NomadNet 1.4.4's own `Node.py`. The link is recorded as the pin's origin,
  which `pin ls` shows when the pin has no label, rather than as the label, so it never overwrites a label the user
  chose. Through the API, `POST /api/add` takes `nomadnet` instead of `path`, and add jobs take it too. A pin made with
  an app's token gets that app's source. A "Request Not Allowed" answer on an identified link is retried once after
  identifying again.

## LXMF moves out of core (implemented)

LXMF becomes a separate wrapper project built on the local API and the library. The plan kept LXMF, deprecated,
until the wrapper's first release; the owner decided to remove it in 0.2.0 without waiting for the wrapper.

* Removed from rnfs: `rnfs/features/lxmf.py`, `send`, `inbox`, `outbox`, `lxmf address` and `lxmf sync`, their
  API routes and library methods, the LXMF inbox events, the `lxmf_*` settings, and the LXMF dependency in
  every package and lock
* Kept: `rnfs:` URIs (section 13's syntax) and the LXMF link-map field definition, as the interop contract
  the wrapper and other clients use (`rnfs/uris.py`); the message, receiving and auto-fetch rules move to the
  wrapper's spec. Contacts of kind `lxmf` stay in core's address book; the wrapper reads them through the API to
  decide which senders it trusts
* Upgrade: `lxmf:<address>` pins become `app:lxmf:<address>` pins (a schema step), which the wrapper, as an app
  named `lxmf`, owns and releases; until then `rnfs app add lxmf` followed by `rnfs app rm lxmf` releases them.
  `storage/lxmf_identity`, `lxmf/` and the `lxmf_messages` table are left as they are for the wrapper to adopt,
  so its LXMF address doesn't change; nothing in rnfs reads or deletes them. `lxmf_*` settings left in
  `rnfs.toml` are ignored with a warning
* As implemented: the deprecation path built first (warnings, a `Deprecation` header, schema and capability
  fields) was never released and went with LXMF. 0.1.4's LXMF state files are still imported into
  `lxmf_messages` at an upgrade (`rnfs/lxmf_messages.py`). `tests/interop.py` no longer tests LXMF between
  versions

## Per-app API tokens and pin sources (implemented)

* `rnfs app add <name>` issues a labelled token for an application; `rnfs app ls` and `rnfs app rm <name>`
  list and revoke them. The current `api_token` stays as the full-access token for the CLI
* App tokens are not scoped: an app can do anything the CLI can, including exporting name keys, changing
  settings and stopping the daemon. Issue them only to apps you trust that far
* Pins made with an app's token get the source `app:<name>` (or `app:<name>:<key>`, so an app can hold and
  release its own groups), shown by `pin ls`, and released only by that app or by removing the app
* How: app tokens are kept in rnfs.db (they outlive restarts, unlike `api_token`); the API's token check
  returns who called (the CLI or an app), with an `X-Rnfs-App` header naming the app whose token keys the
  challenge proof; every route that pins with source `user` today (add, keep, dir set/rm, pin add/rm, import,
  and the jobs that replay them) takes the caller's source. `storage.check_source` accepts `app:` sources before
  anything writes one (pin rows failing it are deleted at load)
* As implemented: the daemon keeps only a verifier of each token (SHA-256 of "rnfs app token " + the token) in rnfs.db's
  `apps` table, which also keys the app's proof. `rnfs app add` prints the token once (or writes it to a file with
  `-o`). `X-Rnfs-Pin-Key: <key>` picks `app:<name>:<key>`. The `/api/apps` routes accept only the CLI's token; an app's
  gets 403 `forbidden`. Removing an app cancels its jobs and releases all its pins.

## API reference (implemented)

* No API stability is promised until 1.0.0: `rnfs.API_STATUS` stays provisional, and the local API and the
  library may change in 0.2.x releases (`/api/capabilities` says what a daemon has)
* An OpenAPI reference for the local API (request and response shapes, error codes), checked against the
  code by tests as `rnfs/api.py`'s route list is today
* As implemented: `docs/openapi.json` (OpenAPI 3.1, in the sdist too), checked by `tests/test_openapi.py` against the
  routes, error codes, event kinds and capabilities, and against the fields live answers carry, over a real offline
  node. Writing it turned up 7 places where the docs said something other than the code; the docs were fixed. The status
  schema is split into one part per producer, so new status fields have an obvious home.

## Block storage and startup (implemented)

* Startup lists every block (twice: once for leftover `.partial-` files, once for sizes) before the API binds,
  so `rnfs status` says "no daemon is running" while a large store is scanned. The directory lock is taken
  first, so the CLI reports "starting (PID n)" when the API doesn't answer and the lock is held: the cheapest fix
* Block sizes live in rnfs.db, updated as blocks change and committed at the existing durability point (after
  the blocks' fsync), so startup doesn't list the directory; it rescans after an unclean shutdown, and a row
  whose file is missing counts as a damaged block
* Blocks move from one flat `blocks/` directory into subdirectories named by the second- and third-to-last
  characters of the CID (1,024 of them: the leading characters are the same for every CID of a codec, and the
  last carries only 2–3 bits of the digest; both are lowercase base32, safe on case-insensitive filesystems).
  One directory of millions of files is slow to list and scan, and FAT32, common on the USB drives Pi users
  add, holds about 10,900 such files per directory (a 59–64-character name takes 6 entries of its 65,536)
* The move is resumable rather than one-shot: every start moves any block still in `blocks/` itself into its
  subdirectory (a rename within the filesystem; if the target exists, the flat copy is deleted, being the same
  content), then syncs the directories. A block whose rename fails (Windows antivirus) stays readable where it
  is until a later start moves it. Scans never follow symlinks in `blocks/`
* As implemented: the layout is `blocks/<cid[-3:-1]>/<cid>`. A subdirectory that is a link, isn't a directory or belongs
  to another user stops the start. The library raises `DaemonStarting`, a `DaemonNotRunning`, while the daemon starts.
  On a desktop NVMe disk with a warm cache, opening 1M small blocks took 1.2 s after a clean stop (5.4 s in 0.1.6; 7.2 s
  after an unclean stop), and the first start, which moves 0.1.6's flat layout, took 74 s. Adding blocks costs one
  directory sync per subdirectory written (16 at a time), +40% for a 64 MiB add (1.02 → 1.41 s). Not measured yet: a
  Raspberry Pi's SD card (`tests/bench_startup.py`, `tests/bench_block_writes.py`), the Windows paths, and FAT32 with
  the private-directory checks.

## Network status (implemented)

* `rnfs status` (and `GET /api/status`) reports what the node can see of the network: the estimated number of
  DHT servers (the density check's size estimate), how often the density check fired recently, partition mode
  and how long it has lasted, records stored against the record budget and how full each storage tier is, and
  how many newcomers were refused or evicted recently. It is the cheapest way to learn how big the network is
  and to notice a flood or an attack on a key, and needs nothing on the wire
* As implemented: `status.network` gives `size_estimate` and how many searches and published keys the density check ran
  on and fired on in the last hour and day. `status.record_store` gives the records' bytes against `record_budget`, the
  capacity, whether it is full, and for each tier its records, share, floor, and the records refused and evicted in the
  last hour and day. `rnfs status` sums these up in a few lines. The counts are kept in memory, per minute over the last
  day, and `status()` reads nothing from the state database.

## Encrypted folders (implemented)

* `add --to` accepts a folder: rnfs packs it as one tar stream and seals that as a single sealed file, so the
  folder's names and structure are hidden along with its content. Sealed files are unchanged: the sealed name
  is the folder's name plus `.tar` (a folder name over 251 bytes is refused), so a recipient on 0.1.x gets a tar
  file it can unpack itself
* Packing needs a small writer of its own: `GET /api/get/stream`'s tar writer walks a stored DAG, not a local
  folder. It reuses that writer's header code, walks the folder with `add`'s rules (no links, the hard-link
  rules, depth and entry limits), streams into the sealer (which already takes a stream of unknown length), and
  puts everything under one top directory `<folder>/`, so `tar xf` doesn't spill into the current directory
* `get` of such a file, by a recipient, decrypts it and unpacks it only if the sealed name ends in `.tar` and the
  content is a valid tar that starts with rnfs's sealed-folder marker (a pax global header holding only `comment=rnfs
  sealed folder`, which extractors ignore), so a real `.tar` someone sealed as a file stays a file, reusing both rule
  sets as they are: the tar reader of `POST /api/add/stream` builds it into the block store (regular files and
  directories only, no absolute or `..` paths, entry-count and depth limits), `get`'s write-out writes it (temporary
  name, renamed when complete, unsafe names skipped), and the plaintext blocks are collected afterwards; the free-space
  check counts both. `--no-extract` writes the `.tar` instead. `cat` gives the decrypted tar, as for any sealed file;
  the gateway and `--raw` give the sealed bytes
* As implemented: as written. `--no-sealed-name` with a folder is refused, since the sealed name is what makes a
  recipient's `get` unpack it. `GET /api/info` tells a recipient `sealed_folder`. `POST /api/add/stream?format=tar`
  still can't seal.

## Name key backups (implemented)

* Losing a name's key loses the name for good: nothing can move it to a new key. `name create` and `name import`
  say so and print the `name export` command; the node remembers which keys were exported since they were
  created, and `rnfs status` lists names whose key has never been exported
* As implemented: a key imported from a file counts as backed up, and keys made before 0.2.0 count as never exported
  until `rnfs name export` runs. A feature reading a key, as keepers do, doesn't count as an export. `name list` entries
  and `rnfs.Name` gain `exported`; `rnfs status` lists `unexported_names`.

## Publishing a folder to a name (implemented)

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
* As implemented: the settle time is 60 s. The hourly limit follows RNS transport interfaces' default
  `announce_rate_target` of 3600 s. A folder that is missing, unreadable, empty (an unmounted drive looks empty) or
  fails to import pauses the sync, which never publishes it. `rnfs name syncs` lists the syncs and how they stand; `name
  rm` stops a sync. Syncs resume after a restart.

## Remote pin status (implemented)

* `rnfs pin remote ls` lists the remote pins this node asked for: node, CID, when it asked, and whether it is
  done, worked out locally by looking up the CID's providers and finding that node among them (paced on slow
  nodes, refreshed on demand with `--check`). A pin the node accepted but later dropped (quota, or the CID
  couldn't be found) shows as not done after a while; `/pin` itself is unchanged
* `rnfs pin remote ls --served` lists the pins other identities asked of this node: identity, CID, state
  (waiting, fetching, pinned, dropped and why) and each identity's use of `remote_pin_quota`. Both through the
  API and the library
* As implemented: asked pins are `waiting`, `done`, `missing` (not found 24 h after asking), `refused` (including no
  answer) or `released`. Checks run in the background at most 60 an hour, 4 at once, or on slow nodes 4 an hour, one at
  a time; `--check` checks at most 32 a call (4 on a slow node). At most 10,000 asked entries and 1,000 served outcomes
  are kept. Served outcomes carry a reason (quota, not found, denied, queue full, ...). `remote_pin` events report both
  sides.

## Fewer name announces (to do, once 0.1.5 and 0.1.6 resolvers are gone)

* Announce a name only when it changes, and resolve through keepers first, once resolvers of 0.1.5 and 0.1.6
  are gone: they count an announce as current for only 2 × `announce_interval`. Keepers first is in effect
  `keeper_query = always`, so measure its airtime on LoRa against the announces it saves before adopting it.
  Paths at transport nodes lapse a week after a name's last announce, so even then a name needs a keepalive
  announce more often than that

## Waiting on Reticulum (to do, once Reticulum offers them)

* Traffic classes: RNS 1.5's inbound queues have classes (data, announce, path request, ingress limited), but an
  application can't set one. If RNS lets applications mark traffic, mark `/blocks` responses as bulk so DHT
  messages, names and other applications go first on a shared link
* LXMF content destinations, on LXMF's roadmap since 2022 with no design published: if they ship, look at
  whether rnfs CIDs map onto them, so LXMF clients and propagation nodes can fetch rnfs content

## Not planned

* A global `PROVIDER_TTL` of 48 hours (as in IPFS) would clear floods faster, but older nodes republish every 3
  days (slow nodes, kept names) and would lose their records: it breaks compatibility. The 4.5-day newcomer
  expiry gets most of the benefit locally
