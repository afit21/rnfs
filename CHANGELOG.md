# Changelog

[specification.md](specification.md) is the authority on behaviour.

## 0.2.0 (unreleased)

The feature-complete release: the local API and the library for companion apps; providing at scale (sweep
providing, provider records on disk within a byte budget, storage tiers against floods); finding content when
identities are generated next to its key (the density check, disjoint lookup paths, the announce first-seen
log); LoRa islands (fast nodes sparing them, two-level records, partition mode); and new features (per-app API
tokens, encrypted folders, NomadNet add, gateway sites, name sync, name key backups, remote pin status, network
status, files inside folders provided by their own CIDs). LXMF messaging moved out of rnfs into an app of its own
(Removed, below). No wire-format or CID changes: these nodes, 0.1.6, 0.1.5, 0.1.4 and 0.1.3 nodes work together
(`tests/interop.py` against 0.1.6 passes). The local API stays compatible but for LXMF: every route, field and
status of 0.1.6 is unchanged but LXMF's, which are gone, and error bodies gain a field. The upgrade of an
`<rnfs dir>` is automatic and one-way.

Upgrade notes:
- **State:** the state database gains tables (`pin_times`, `pin_labels`, `name_exports`, `apps`, `block_sizes`,
  `routing`, `peer_scores`, `announce_first_seen`, `provider_standing`, `remote_pins_asked`, `remote_pins_served`,
  `name_syncs`, `name_sync_files`, `published_heal`, `name_announces`); an older rnfs can't open it afterwards. Pins made before the
  `name_syncs`, `name_sync_files`, `published_heal`, `remote_pin_retries`); an older rnfs can't open it
  afterwards. Pins made before the
  upgrade have no `added` time, name keys made before it count as never exported (`rnfs status` lists them until
  `rnfs name export` is run), and remote pins asked before it have no status entry.
- **LXMF:** rnfs no longer sends or receives LXMF (Removed, below). Pins of links auto-fetched over LXMF change
  source from `lxmf:<address>` to `app:lxmf:<address>` (`pin ls` shows the new one), an app called `lxmf`'s, so
  they stay pinned for an LXMF app to take over; nothing is released by the change, and an older `pins.json`
  imported at the upgrade gets the same rewrite. `storage/lxmf_identity`, `lxmf/` and the inbox and outbox in
  `rnfs.db` are kept, untouched. `lxmf_*` settings in `rnfs.toml` are ignored with a warning. If you rely on
  `rnfs send` and `rnfs inbox`, don't upgrade before an LXMF app is there: the upgrade can't be undone.
- **Provider records:** the upgrade gives each publisher of the records a node stores a standing
  (`provider_standing`, and indexes on `provider_records`): Earned if it was first seen at least 3 days before its
  latest store and refreshed a record outside the newcomer pool a day or more after storing it, else Newcomer. The
  newcomer pool's records are no longer pooled, and the remembered departed and evicted publishers are dropped.
- **`max_records` is retired:** a `max_records` left in `rnfs.toml` (or given to `rnfs.Node`) is ignored with a
  warning instead of stopping the daemon; `record_budget` (bytes) replaces it. `rnfs config set max_records` is
  refused as an unknown setting; `rnfs config unset max_records` removes it.
- **Blocks:** they move from `blocks/<cid>` into 1,024 subdirectories, `blocks/<xy>/<cid>` (the CID's third- and
  second-to-last characters), at the first start, which takes a while on a large store (a rename per block; the
  API answers once it is done). Each start moves whatever is left, so an interrupted upgrade loses nothing, and
  a block Windows won't let rnfs move stays readable until a later start moves it. An older rnfs doesn't find
  the moved blocks. A subdirectory of `blocks/` that is a link, not a directory or another user's stops the
  start. The state database gains a `block_sizes` table, every stored block's size, which the first start
  fills by listing the blocks.
- **`<rnfs dir>/daemon.json`:** the daemon writes it while it runs. Clients from 0.2.0 on read the API port from
  it first, so one that can't read a newer daemon's `rnfs.toml` still finds it.
- **Settings:** `rnfs.toml` may now be rewritten by the daemon (`rnfs config set`, `POST /api/config/set`). Only the
  assignments changed are replaced; comments and other lines are kept. Settings whose `api_port` equals a
  non-zero `gateway_port` are now refused at start (such a daemon could never listen on both).
- **Routing state:** the state database gains tables for the routing table (`routing`), which is now kept across
  restarts, for local scores (`peer_scores`) and for the announce first-seen log (`announce_first_seen`).
- **Files and folders inside pins are provided** (`provide_entries`, below, on by default): after the upgrade a node
  that isn't slow also publishes the CID of every file and folder inside its complete pins, up to 100,000, so its
  first rounds publish more keys. Set `provide_entries = false` to keep 0.1.6's behaviour.

Security:
- **NomadNet links are bounded per link** (specification sections 3 and 11), with no process-wide change: the answer
  may be no larger than `max_download_size` and the free space above the disk reserve; each Resource segment may
  decompress to one segment, and must follow the sequence an honest sender keeps (same original hash and total,
  the next index, the segment count the total gives), else the transfer is cancelled and the link closed; what is
  assembled may not outgrow the advertised total; a file's metadata must be a small flat map, checked before RNS
  decodes it; a page must be one flat byte string, checked from its msgpack headers before RNS decodes it. The
  daemon's process-wide decompression limit stays 0, and `rnfs.dht` links keep every rule they have.
- **A NomadNet page answer may be at most 16 MiB** (specification section 11): RNS reads an answer without metadata
  (a page) whole into memory and decodes it, so a "page" as large as the free disk could exhaust a small node's
  memory. One advertised larger is refused at its first segment, before any of it is transferred (the add fails
  with too large); files, written to disk, keep the bound above.
- **Gateway sites don't render uploads as pages** (specification section 11, Sites). A site's pages run script in
  its own origin, so a file someone else got into a site's DAG could be made to run script there: on site hosts
  `?filename=` now only names a download (`Content-Disposition: attachment`; the type comes from the path's own
  name), and a file inside the site's DAG whose name gives no type is sniffed only to an image, audio, video, font,
  wasm or plain text, else served as `application/octet-stream`, never as HTML, SVG or PDF. A site's `index.html`,
  the files its DAG names `.html` or `.svg`, and a site whose root is itself a file render as before; the path
  gateway is unchanged.
- **App names stay private** (specification section 11): a request naming an app that doesn't exist in
  `X-Rnfs-App` gets an `X-Rnfs-Proof` too, keyed by an HMAC of a secret made at each start and the name, which no
  client can check, instead of none; so every answer, the open `GET /api/status` and the 403s included, looks the
  same whether or not the app exists, and anyone who can reach the API port can no longer list the installed apps.
- **What an app's token can do, said plainly** (specification section 11, README): an app can do anything the CLI
  can, managing apps included in effect. The `/api/apps` routes refuse its token, but it reads and writes files as
  the daemon's user, which reaches the CLI's token; `rnfs app add` now says so. Give a token only to software you
  would trust with the CLI.
- **Unpacking a sealed folder counts what its entries take on disk** (REV10; specification section 5,
  Unpacking): the tar's headers are read first, before anything is stored, and every file and directory counts
  at least one filesystem allocation unit (4 KiB or the filesystem's own, if larger) in the block store and
  again as written, where the plaintext's bytes were counted before. A sender could seal a folder of many tiny
  files that passed the check and then took 4 to 8 times as much, twice, filling the recipient's disk past the
  reserve (1,000 one-byte files: 10.9 MB written where 2 MB was checked). Such a get is now refused (413, naming
  `--no-extract`).
- **An unpack the process died in no longer leaves the plaintext stored** (REV10): its decrypted blocks used to
  stay in the block store, unpinned, until a gc or an eviction. An unpack now marks the state database while it
  runs, and a start that finds the mark deletes every block nothing reaches before serving anything (the
  unrecorded blocks of a fetch the same crash interrupted too: a retry fetches them again).
- **Documented: a sealed file is only as safe as the recipient's node identity** (REV12), which is online while
  the node runs and stored unencrypted in the rnfs dir (`storage/identity`): sealing protects against the
  network, not against someone who can read the recipient's disk (README, Known limitations; specification
  section 5).
- **`add` refuses the rnfs dir** (specification section 5): `rnfs add ~` pinned and provided the node identity, the
  name keys, `api_token` and `rnfs.db`, each findable by its own CID with `provide_entries`, and an app token could
  read the CLI's token through `add`. `add` (the CLI, the API, add jobs, sealed folders) now refuses the rnfs dir, a
  directory holding it, anything inside `storage/`, `names/`, `state/`, `blocks/` or `lxmf/`, and anything else in
  the rnfs dir but outside `files/`. Every walk (`add`, the shared folder, `name sync`, sealed folders) also skips,
  with a warning, a directory that is the rnfs dir or one of those subdirectories by device and inode, which catches
  a bind mount of them; one given as the root is refused.
- **Provider records have a write budget** (REV2; specification section 6, "Provider records"): with no
  per-publisher cap left, one identity storing and withdrawing records in a loop made the store write about
  100,000 rows a second in-process, and 9.5 MB/s at 100 requests a second. New records and withdrawals now spend a
  budget of 60 rows every 30 s open to all, plus 30 each kept for Earned and Known publishers, carried over for up
  to 6 hours; past it new records are refused (`False`), withdrawals never are, and refreshes cost nothing. A
  withdrawal followed by a store of the same key before the next save is a refresh, and a refresh is written once
  it moves a record's last-stored time by 6 hours rather than 1. A Newcomer flood now makes the store write at most
  about 150 kB/s (fresh identities, inserted and withdrawn, on a 1M-record database), a hundredth of what it did.
- **A full key can't be taken over** (REV3; specification section 6): Earned publishers never gave way to each other
  at a full key, so about 1000 identities that each refreshed one key on 3 UTC days (from about 48 hours) held all
  its 1000 publishers, and every new provider of it but Known ones was refused. Now Earned records give way to
  Earned and Known storers (a random one outside the `K/2` oldest), and Newcomers keep a tenth of the key: below it
  a Newcomer evicts a random Earned record, past it Newcomers give way first, so a flood of fresh identities holds
  at most that share.
- **Provider standings without records are capped** (REV11; specification section 6): a publisher left without
  records keeps its refresh days a week, and nothing else bounded those rows, so identities that each refreshed one
  record a day later and withdrew it grew the state database (and took room from the record budget, which measures
  it). At most 50,000 are kept (about 5 MB); past that the hourly pass forgets those with the fewest refresh days
  first, so Earned publishers that left outlast such a flood.

Removed:
- **LXMF messaging left rnfs** for an app of its own, built on the local API and the library (roadmap "LXMF moves
  out of core"): rnfs is the core (content, the DHT, names, pinning, the local API, the library), and messaging is
  a companion app's. Gone: the commands `rnfs send`, `inbox` (`inbox get`, `inbox rm`), `outbox`, `lxmf address` and
  `lxmf sync`; the routes `GET /api/lxmf/address`, `POST /api/lxmf/send`, `GET /api/lxmf/inbox`,
  `POST /api/lxmf/inbox/get`, `/api/lxmf/inbox/rm`, `GET /api/lxmf/outbox` and `POST /api/lxmf/sync` (now 404
  `no_such_endpoint`); `lxmf` among `GET /api/capabilities`' features; the `lxmf_address`, `lxmf_inbox` and
  `untrusted_dropped` fields of `GET /api/status` and `rnfs status`; the `lxmf` dependency, from `pyproject.toml`,
  the lock and every package (the `.deb`, the Flatpak, the Windows exe, the install script). This is a deliberate
  break of the local API for LXMF only: every other route, field and status of 0.1.6 is unchanged. The
  library's `lxmf` methods, their objects and the `inbox` events, added during 0.2.0's development, never shipped,
  nor did the deprecation path first planned for this release (warnings, a `Deprecation` header).
- **What stays:** nothing on the network changes, and `rnfs:` URIs and the LXMF link map (the field an LXMF message
  carries rnfs links in) stay part of the specification (section 13) as the contract an LXMF app and other clients
  use; `rnfs.Node` and `rnfs.Client` still take an `rnfs:` URI as a target (`rnfs/uris.py`). Contacts of kind
  `lxmf` stay in the address book, for an app to read which senders are trusted. On disk, nothing is deleted:
  `storage/lxmf_identity` and `lxmf/` are left as they are (rnfs no longer creates or checks `lxmf/`), so an LXMF
  app can adopt them and keep the node's LXMF address; the inbox and outbox stay in `rnfs.db`'s `lxmf_messages`
  table (0.1.4's `lxmf_inbox.json` and `lxmf_outbox.json` are still imported into it), which nothing reads,
  changes or deletes, for the app to import.
- **Pins of links fetched automatically over LXMF** stay pinned, as `app:lxmf:<address>` (upgrade notes, above):
  an app called `lxmf` holds them, and nothing else releases them. An LXMF app registered under that name
  (`rnfs app add lxmf`) takes them over; to release them without one, run `rnfs app add lxmf` then
  `rnfs app rm lxmf`.
- **Retired settings:** `lxmf_enabled`, `lxmf_display_name`, `lxmf_propagation_node`, `lxmf_send_from_hint`,
  `lxmf_sync_interval`, `lxmf_accept_from`, `lxmf_auto_fetch_from` and `lxmf_auto_fetch_quota`. One left in
  `rnfs.toml` (or given to `rnfs.Node`) is ignored with a warning, as `max_records` is, instead of stopping the
  daemon; `rnfs config set` refuses one as unknown, and `rnfs config unset` removes it.
- **If you used `rnfs send` or `rnfs inbox`:** use an LXMF app built on rnfs once one is released. Meanwhile an
  `rnfs:` link can go through any LXMF client (Sideband, NomadNet, MeshChat) as text, and the recipient fetches
  `rnfs:<cid>/<path>?from=<node>` with `rnfs get <cid>/<path> --from <node>`. If you rely on rnfs's own inbox
  and auto-fetch, stay on 0.1.6 until then: an `<rnfs dir>` 0.2.0 has upgraded can't go back to it.

Added, for apps:
- **App tokens:** an app can have a token of its own, kept across restarts (`rnfs app add`, below), sent as
  `X-Rnfs-Api` with `X-Rnfs-App: <name>`; the proof a client checks before sending it is keyed by the token's
  verifier, which is all the daemon keeps of it (`rnfs.db`'s new `apps` table). `rnfs.Client(app=..., token=...
  or token_file=...)` and `ApiConnection(app=, token=)`. The `api_token` stays the CLI's, unchanged, and a
  request without `X-Rnfs-App` is checked exactly as before.
- **Managing apps:** `rnfs app add <name> [-o file]` issues an app a token, printed once (alone on stdout, for
  scripts) or written to a new file only you can read, and says on stderr that the app can do anything the CLI
  can (export name keys, change settings, stop the daemon), managing apps included in effect; `rnfs app ls [--json]`;
  `rnfs app rm <name>` revokes the token and releases every pin the app made. Routes `GET /api/apps`,
  `POST /api/apps/add`, `POST /api/apps/rm`, for the CLI's token only (an app's gets 403 with the new error code
  `forbidden`); capability `apps`. An app called `lxmf` holds the `app:lxmf:` pins rnfs's own LXMF left
  (Removed, below).
- **An app's pins:** what an app adds, pins, imports or makes with `dir set`/`dir rm` (and its pin, add and import
  jobs) is pinned with the source `app:<name>`, or `app:<name>:<key>` with `X-Rnfs-Pin-Key: <key>`
  (`client.pin_key(key)`), shown by `pin ls`. Its `pin rm` releases only its own source; the CLI's still
  releases only `user`'s, and a pin key with the CLI's token is refused (400). Removing the app releases them all
  and cancels its jobs.
- **Discovery:** `<rnfs dir>/daemon.json`, written atomically (0600) once the API listens and removed at stop: the
  daemon's PID, API and gateway ports, version, start time, rnfs dir, Reticulum config directory, whether it
  attached to a shared instance, and node ID. `rnfs.apiclient.read_daemon_info()` reads it, refusing a symlink,
  a file others could have written, or one whose process has ended.
- **Stopping:** `POST /api/shutdown` stops the daemon as SIGTERM does, after answering; `rnfs stop [--wait S]`
  uses it and waits until the API stops answering. Windows has no other clean way to stop a background daemon.
- **Error codes:** every API error is `{"error", "code"}`, the code machine-readable (`rnfs.errors.CODES`): the
  node's errors by status (`bad_request`, `not_found`, `too_large`, `denied`, `invalid_dag`, `busy`,
  `storage_full`, `internal`) and the API's own refusals (`no_such_endpoint`, `method_not_allowed`,
  `unauthorized`, `bad_host`, `timeout`, `length_required`, `body_too_large`, `too_many_connections`,
  `unsupported`). So a 404 for an endpoint a daemon lacks (`no_such_endpoint`) is told from one for missing
  content (`not_found`). `ApiError.code`, and `NoSuchEndpoint` and `Unsupported` (both `NotFound`s) in the
  library, which also recognises an older daemon's unknown-endpoint 404 by its message.
- **No API stability until 1.0.0:** the library (`rnfs.Node`, `rnfs.Client`) and the local API stay provisional
  (`rnfs.API_STATUS`), and may change in 0.2.x releases; `GET /api/capabilities` says what a daemon offers.
- **An OpenAPI reference for the local API:** `docs/openapi.json` (OpenAPI 3.1) describes every route, its
  parameters and bodies (streamed ones too), its answers, the error codes, the token headers and the event
  kinds, for apps in any language; tests check it against the code (routes, codes, kinds,
  capabilities, and the fields that live answers carry). It is in the sdist too.
- **Capabilities:** `GET /api/capabilities` lists the daemon's routes, features, event kinds, error codes and
  capabilities (`events`, `pin_details`, `cache`, `config`, `jobs_add`, `add_tar`, ...).
- **Events:** `GET /api/events?after=&kinds=&epoch=&wait=`, a long poll on the node's events (`rnfs/events.py`),
  replacing an app's polling of each list: `job` (every state change, and a running job's progress at most once
  a second), `pin` (added, removed, labelled), `cache` (added, removed), `name` (a newer record of a name the node holds
  or watches) and `peer` (a peer first heard, coming online, going offline). Events are numbered within a run;
  the last 1024 are kept, and a client that falls behind, or asks of an earlier run (`epoch`), is told `lost`
  and reloads. At most 8 polls wait at once.
- **Pins:** `pin ls --json` and `GET /api/pin/ls` give each pin's `label`, `origin` (where it was added from,
  kept apart from the label), `size`, `entries` (a directory's entry count, as a number) and `added` (when first
  pinned). `rnfs pin label <cid> [label]` / `POST /api/pin/label` give a pin a label, which becomes its listed
  name. `name` is otherwise unchanged.
- **The cache:** `rnfs cache ls [--json]` / `GET /api/cache/ls` list the cached fetches (downloads not pinned),
  most recently used first, with size, completeness and whether the same content is pinned or provided;
  `rnfs cache rm <cid>…` / `POST /api/cache/rm` evict those leading to a CID now. Apps no longer need to look in
  `blocks/`.
- **Settings:** `GET /api/config` gives every setting with its schema (type, choices, default, unit, range,
  description, group, whether it applies without a restart), the running value, the value in `rnfs.toml`, and
  whether the command line overrides it or a restart is pending; `POST /api/config/set` changes `rnfs.toml`
  (validated as a start would, nothing written otherwise; `null` removes a setting), keeping its comments and
  other lines, and applies at once what can be. `rnfs config [list|get|set|unset]`. `rnfs.config.settings_schema()`
  needs no daemon. Settings that now apply without a restart: `max_storage` (enforced at once),
  `max_download_size`, `max_output_entries`, `max_upload_rate`, `remote_pin_quota`, `follow_quota`, `max_jobs`
  (queued jobs start if the new limit allows), `max_finished_jobs`, `serve_interfaces`, `no_serve_interfaces`,
  `quiet_hours`, `keep_max_names`, `keeper_query` and `scan_interval`.
- **Add and import as jobs:** `POST /api/jobs` takes `kind` `add` (`path` and `POST /api/add`'s options) and
  `import` (`path`), with progress (`total_bytes` to read, `done_bytes` read, `rate`, `eta`), cancellation at
  the next read, and the result `POST /api/add` / `/api/import` would give; persisted, and run again from the
  start if interrupted. `rnfs add --background`, `rnfs import --background`.
- **Streaming, so a daemon that can't see the app's files (Flatpak) works fully:** `POST /api/add/stream?format=tar`
  (a tar stream becomes a directory; links, devices, absolute or `..` paths and invalid names are skipped with
  warnings; bounded by `max_output_entries` and the DAG depth limit; extended headers over 64 KiB, global headers
  over 4 KiB in all, data after the tar's end, and compressed or malformed tars refused),
  `GET /api/get/stream?format=tar` (a target as a tar, fetched first, every limit checked before the response
  starts, with an exact length; files as stored; entries whose names hold `\`, `＼` or `／`, or start as a Windows
  drive does, `C:`, are skipped and counted),
  `GET /api/export/stream` (an archive, byte-identical to `rnfs export`) and `POST /api/import/stream`.
- **Peers:** `GET /api/peers` and `rnfs peers` gain `first_heard`, `rtt` and `rtt_at` (the round trip RNS measured
  on the last link to the peer, `ping` included) and `online`, worked out by the daemon: an rnfs link is open, or
  within 15 minutes of its last sign of life — an answer, a request served, a link, or an announce emitted within
  that time (a path answer, or an announce relayed late, doesn't count). Decided on the monotonic clock, so a
  wall-clock step changes nothing.
  `rnfs peers` gains RTT and ONLINE columns.
- **rnfs.Client and rnfs.Node:** typed methods for what was only reachable through `Client.api`: `peers()`,
  `stats()`, `contacts.*`, `names.keep/unkeep/kept` and `names.follow/unfollow/following`, with objects (`Peer`,
  `Stats`, `Contact`, `Kept`, `Followed`); `jobs.add` and `jobs.import_archive`; and the additions above (events,
  capabilities, pin labels, the cache, settings, streams). A Node raises what a Client gets back.
- **Starting:** while a daemon opens its block store, before its API listens, `rnfs status` and the other commands
  say it is starting, naming its process, rather than that no daemon is running; the library raises
  `DaemonStarting` (`.pid`), a `DaemonNotRunning`, so code catching that is unchanged. Told by the rnfs dir's
  lock, which the daemon takes first (`apiclient.dir_holder`).
- **Per-call timeouts:** `with client.timeout(seconds):` bounds the calling thread's requests only, long polls
  fitting inside it (a Node accepts it and does nothing); `ApiConnection.request/call/get/post(timeout=)`.
- **Gateway sites, an origin per root:** `http://<k-cid>.rnfs.localhost:8789/<path>` serves `<cid>/<path>` and
  `http://<name>.name.localhost:8789/<path>` the name's target, so a page can `fetch()` files of its own DAG.
  `<k-cid>` is the CID as `k` + base36 (56 or 60 characters: an rnfs-dag CID's usual form is too long for a
  DNS label), accepted in these hosts only (`rnfs.content.cid.to_base36`); ETags, listings and logs still show
  the usual base32 CID. Site responses keep the page's origin (`sandbox allow-same-origin …`), can be framed
  only by their own origin, and carry `Origin-Agent-Cluster: ?1`; service worker scripts (403) and `?format=`
  (400) are refused there, and paths are DAG paths only. Browsers resolve `*.localhost` themselves; other tools
  should keep using the path gateway, which is unchanged. All `*.rnfs.localhost` sites share cookies (as do all
  `*.name.localhost` sites): neither the gateway nor the API uses them. `rnfs url <cid>|<name>[/path]` prints a
  target's site URL.

Added:
- **Name key backups:** losing a name's key loses the name for good. `rnfs name create` and `rnfs name import`
  say so and print the `rnfs name export` command (the API's results carry it as `warning`); the node remembers
  which keys were exported by `name export` or imported, and `rnfs status` (`GET /api/status`,
  `Status.unexported_names`) lists the names whose key never was. `name list` entries and `rnfs.Name` gain
  `exported`.
- **Clock problems are noticed:** RNS drops an announce emitted (by its sender's clock) no later than the one it took
  a path from, so a Raspberry Pi without a clock that came back on an older time lost its name updates silently, for
  up to a week. The node now remembers each name's last announce (`name_announces`) and holds the name's announces
  while its clock is behind it: a publish is committed as usual, its result says why the announce is held
  (`announce_held`, printed by `rnfs name publish`, a `RuntimeWarning` from `Name.publish`), and the announce goes
  out once the clock passes the last one, or after a week held in all. `GET /api/status` (`Status`) gains
  `announces_held` and `clock_offset`, the clock's offset from the network's estimated from the emission times of
  the announces heard (the median over senders, so a few wrong clocks don't move it); a warning is logged, and
  printed by `rnfs status`, while it is over 5 minutes off. The estimate never changes the clock or what the node
  does. Name syncs publish through the same path.
- **Pin by NomadNet address:** `rnfs add <destination>:/page/<path>` (a page as the node renders it) or
  `<destination>:/file/<path>` (a file it serves) fetches it from that NomadNet node over a link of rnfs's own and adds
  it like a file, pinned `user` (with an app's token, that app's source) with the link as its origin in `pin ls`; the
  usual `add` options apply (`--no-pin`, `--wrap`, `--name`, `--to`, `--chunker`, `--only-hash`, `--background`), and it
  runs as an `add` job with progress. Each run takes a new snapshot. The link is anonymous unless `--identify`, which
  identifies with an identity of its own (`storage/nomadnet_identity`, made on first use), never the node's. A page the
  node answers with its "Request Not Allowed" page fails rather than being pinned. A local file named like such a link
  is added as `./<name>`. `POST /api/add` takes `nomadnet` (with `name` and `identify`) instead of `path`, as do add
  jobs (`POST /api/jobs`); capability `add_nomadnet`; `rnfs.Node`/`rnfs.Client` `add_nomadnet()` and
  `jobs.add_nomadnet()`. Request data (a page's fields or `var_` values) isn't supported.
- **Encrypted folders:** `rnfs add --to <recipient>… <folder>` (and `--to-self`, `--background`, `POST /api/add` with
  `to`) seals a folder as one sealed file: the folder is packed as a tar, everything under one top directory
  `<folder>/`, and sealed under the name `<folder>.tar`, so its file names and structure are hidden along with its
  content (not its approximate total size). The sealed-file format is unchanged: a recipient on 0.1.x gets a tar it can
  unpack itself. The tar starts with a marker, a pax global header holding only the POSIX `comment` `rnfs sealed
  folder`, which extractors ignore. The folder is walked as `add` walks one (symlinks, devices and invalid names skipped
  with warnings; names holding `\`, `＼` or `／`, or starting as a Windows drive does, skipped too); more than
  `max_output_entries` entries or a tree deeper than `MAX_DEPTH` is refused, and so are a folder name over 251 bytes and
  `--no-sealed-name` with a folder (the name is what makes a recipient's `get` unpack it).
- **Unpacking sealed folders:** `rnfs get` of a sealed folder (a sealed file whose sealed name ends in `.tar` and whose
  content is a valid tar starting with the marker) unpacks it, by a recipient. A tar sealed as a file (by 0.1.x, or
  `rnfs add --to bob backup.tar` now) has no marker and is still written as that file, byte for byte; `GET /api/info`
  gives a recipient `sealed_folder`. The decrypted tar is read with `POST /api/add/stream?format=tar`'s rules (links,
  devices, absolute or `..` paths skipped; `max_output_entries` and `MAX_DEPTH`) and written with `get`'s (temporary
  name, unsafe names skipped). Without `-o` the output is named after the tar's top directory. The free-space check
  counts the content twice (its blocks, then the files), and the decrypted blocks are deleted afterwards, never pinned
  or provided. A marked `.tar` whose content isn't a valid tar is written as the file, with a warning; one past a limit
  is refused. `rnfs get --no-extract` writes the `.tar` instead; `POST /api/get`, `POST /api/jobs` (kept with the job),
  `Node/Client.get` and `jobs.get` take `no_extract`, and `Written` gains `extracted`. `POST /api/get` without
  `no_extract` unpacks a sealed folder; content sealed before 0.2.0 is written as it always was. `GET /api/capabilities`
  lists `sealed_folders`. `cat` still gives the decrypted tar, and the gateway and `--raw` the sealed bytes. `POST
  /api/add/stream?format=tar` still can't seal (400).
- **Remote pin status** (local only; `/pin` and the wire are unchanged): a node records the remote pins it asks
  for (node, CID, pin or release, when, whether accepted) and works out whether each accepted one is done by
  looking the node up among the CID's providers: paced (in the background at most 60 lookups an hour, 4 at
  once, each pin hourly while waiting and daily after; on slow nodes 4 an hour, one at a time), waiting for up
  to 24 hours after asking, then missing if not found. So "done" lags the node's publishing and is only as good as
  the DHT. A node serving remote pins records what became of each (waiting, fetching, pinned, dropped with why:
  quota, not found, denied, ...; refused because denied or its queue was full; released), which was only logged.
  At most 10 000 asked entries and 1 000 served outcomes are kept. `remote_pin` events report both.
  `rnfs pin remote ls [NODE_ID] [CID] [--check] [--json]` lists the pins asked for (`--check` looks them up now:
  one lookup each, at most 32 a call, 4 on a slow node); `rnfs pin remote ls --served [--json]` lists those served
  and each identity's use of `remote_pin_quota`. `GET /api/pin/remote?node=&cid=&check=1` and
  `GET /api/pin/remote/served`; `ApiConnection.pin_remote_ls` / `pin_remote_served`; `Node/Client.remote_pins()`
  and `served_pins()` (`RemotePin`, `ServedPins`, `ServedPin`, `RemotePinUsage`); capability
  `remote_pin_status`.
- **Publishing a folder to a name** (local behaviour built on `name publish`; nothing on the wire changes):
  `rnfs name sync <alias> <folder>` keeps a name pointing at a folder's current content. Every `scan_interval` the
  folder is imported as the shared folder is (unchanged files recognised by their metadata, not read again; links,
  special files and files with other hard links left out; a folder in the rnfs directory refused), with FastCDC,
  so an edited file shares most of its blocks with the version before. When its root changes the name is
  published to it, as `name publish` does (the previous version's `name:` pin released), once the folder has
  stayed unchanged a minute and at most once an hour per name (RNS's transport nodes stop rebroadcasting a
  destination announcing faster than that). A folder that is missing, unreadable, empty or whose import fails
  pauses the sync, logged once, and nothing is published. `rnfs name unsync <alias>` stops it, the name keeping
  its target; `rnfs name rm` stops it too; `rnfs name syncs [--json]` lists the syncs and how they stand. Syncs
  are kept in the state database and resume after a restart. In private mode `name sync` warns, as `name publish`
  does, and the sync logs it once. `POST /api/name/sync {"alias", "path"}`, `POST /api/name/unsync {"alias"}`,
  `GET /api/name/syncs`; `ApiConnection.name_sync`, `name_unsync`, `name_syncs`; `Node/Client.names.sync`,
  `unsync` and `syncs()` (`FolderSync`); capability `name_sync`. An app's token may start and stop a sync, as it
  may publish; the name's pin stays the name's.
- **Files and folders inside pins are provided** (new setting `provide_entries`; an entry is an ordinary provider
  record of its CID): every file and directory a directory entry names inside a complete pin, at every depth, can
  be found by its own CID rather than only as `<root>/<path>`, and the same file in two folders from either. Not
  chunks or other intermediate nodes, and never inside cached downloads, which provide only their root; serving is
  unchanged (an entry's blocks are its pin's). Unset (the default), it is on up to 100,000 entry keys: pins
  oldest first, each pin's entries all or none, a pin that would pass the cap left out (logged). On a slow node
  it is on only while republishing every key is estimated to take at most 2% of the channel (from the slowest
  interface's bitrate, the keys, and the network size the publish rounds' regions show: about 1,300 keys at 100
  servers and 40 from 1,000 at 1 kbit/s), and none are provided past that; once on, they stay on up to 2.5%.
  `true` or `false` overrides the default; it applies at once when changed through the API. Unpinning a folder
  withdraws its entries, except those still inside another pin. `rnfs status` (and `GET /api/status`,
  `Status`) gains `provided_entries`, `provide_entries_off` (why none are provided) and `provide_entries_capped`;
  `provided` counts entries too. Providing entries tells anyone looking up a file's CID that the node has it.
- **Network status** (spec section 11, "Network status"): `rnfs status` (`GET /api/status`, the library's
  `Status.network` and `Status.record_store`) reports what the node sees of the network, with nothing sent to learn
  it: `network` gives the number of DHT servers the density check estimates (`size_estimate`, null until 8 lookups
  completed; `known_nodes` is the routing table's, to compare) and how many searches and published keys the check ran
  on and fired on in the last hour and day; `record_store` gives the provider records' bytes against `record_budget`,
  the capacity, whether it is full, and for each storage tier (Known, Earned, Newcomer) its records and share against
  its floor and the new records refused and records evicted in the last hour and day. `rnfs status` sums these up in
  a few lines after the other fields (`network_size`, `density_check`, `partition_for` while in partition mode,
  `record_store`, `record_tiers`, `records_refused`, `records_evicted`); every existing field and line is unchanged.
  The counts are kept in memory (per minute over the last day; a restart starts them afresh), and the status reads
  nothing more from the state database, so apps can call it as often as before.

The DHT and provider records (local behaviour: nothing on the wire changes):
- The routing table is saved and loaded at start, so a restart doesn't make every server new again: servers keep
  their seniority (which ranks providers and keepers), bans last their time, and `/find` replies still leave out
  servers silent for over 2 × `announce_interval`, the time the node was down included.
- Local scores: each node keeps a private score per peer, earned by work it checks itself (blocks served that
  verified, `/find` answers that led to providers who served or to closer servers that answered) and lost by
  failures, bad blocks and silence (scores halve each week). Bootstrap nodes start with a small seed they keep
  only by earning it. Nothing is sent; at most 4096 peers are scored.
- A join (at start, and again while too few servers are live) asks up to 6 servers of the saved routing table
  first, best scored first, and bootstrap nodes only if fewer than two of those answer: a node that has joined
  once is its own bootstrap list, so a restart doesn't flood path requests for bootstrap nodes on radio. After
  such a join, a node that isn't slow still asks the default bootstrap nodes (until one answers, at most 2), so
  one that saved only an island of servers reaches the rest of the network; a slow node no longer does.
- Choosing providers: within each seniority group (seniority stays first), providers with a better local score
  are tried first, before fewer hops: a provider whose blocks verified before is preferred to an unknown one.
- The announce first-seen log: each node records when it first heard each server's announce (at most 65,536
  servers, the least recently heard forgotten first; about 12 MB when full), kept across restarts, and can give
  the closest servers to a key among those heard for at least `provider_seniority`, which identities generated
  next to a key can't be. Publishing and searching use it (below).
- **Sweep providing:** republishing many keys takes far fewer lookups. One closest-nodes lookup now serves every
  due key of its region (the keys sharing as many leading bits as the key looked up shares with its 10th-closest
  server), with about 1.6 more `/find` a region to fill in its far half; each key still gets its own 10 closest
  servers (100% of keys on their true 10 closest over a simulated 2000-server network, against about 97% with a
  lookup per key). A round needs about servers ÷ 12.5 lookups however many keys are due: 1,000 keys on a
  1,000-server network take about 80 lookups instead of 1,000. Slow nodes' 10 lookups a round now count regions,
  not keys, so a round publishes every due key of up to 10 regions.
- **Stores to long-known servers too:** each key is also stored on the 10 servers closest to it among those the
  node first heard at least `provider_seniority` (1 day) ago and has heard in the last 2 × `announce_interval`.
  Without an attack they are mostly the key's 10 closest already, so this adds few stores; identities generated
  next to a key can't push the record off them. Withdrawals reach them too.
- **Content is far harder to hide by placing identities next to its key** (spec section 6, "Identities placed next
  to a key"). Ten identities generated closer to a key than every honest server (about 4 s of one core each among
  10,000 servers) hid it almost completely from 0.1.x nodes (found 0–4% of the time); with an upgraded publisher
  and searcher it is found 99–100% of the time over the fake network of `tests/test_eclipse.py` (98.9% on fast
  nodes and 90.8% on slow ones across every attack of the eclipse simulation). Four local mechanisms, nothing on
  the wire changed:
  - a network size estimate: the median distance to the 10th-closest server over the node's last 16 complete
    lookups for other keys, kept in the state database's `meta` table so a restart has it at once (the density
    check waits for 8);
  - the density check: when the servers found near a key are packed more than twice as densely as the estimate
    allows (from the 10th on), a search waits until 5 servers of each of two rings beyond them have answered (those
    ranked after the dense core, and those beyond where 10 honest servers would be expected), and a publisher gives
    the key a widened lookup of its own and stores it on 10 servers of each ring too;
  - disjoint lookup paths: a publisher's region lookup runs as 3 lookups that share no node (2 on slow nodes) and
    stores each key on each path's 10 closest too; a search that finds no provider at all falls back to them, in
    what is left of the same `lookup_timeout` and of its 80 `/find` (a search still sends at most 80 in all);
  - searches also wait for the 3 closest of the 10 servers nearest the key that the announce first-seen log heard
    at least `provider_seniority` ago (none extra without an attack: they are the closest anyway).

  The cost: a little more traffic per publish (over the fake network of 2,000 servers, +17 `/find` and +3 stores
  to publish one key, once per republish; with sweep providing about 0.65 `/find` and 13 stores a key instead of
  0.22 and 10), +0.2 to +0.5 `/find` per search on average (the density check fires on 2–6% of searches without an
  attack), and a search for content nobody provides now costs about 4 times as much (30 `/find` instead of 7: the
  fallback). A slow node paces that fallback like its publish lookups (it counts as 2 of a round's lookups and runs
  only if the pacing allows within the lookup's time), and counts widened lookups among its round's 10 (a flagged
  key that doesn't fit waits for the next round).
- **Fast nodes spare LoRa islands most lookup traffic** (spec section 6, "Slow paths"; nothing on the wire changes):
  a node that isn't slow asks servers behind a slow path (an RNS path of more than 3 hops, or its own last link to
  the server within 24 hours had a round trip above 1 s and a rate estimate below `slow_bitrate`) after every other
  server but unreliable ones, in searches and publish lookups, and stores to them only to make up 10. They are still
  asked when a lookup has too few others; the routing table, `/find` replies, provider ranking and failure counts are
  unchanged. In the topology simulation this cut the airtime internet nodes' lookups cost islands by 97% (27 s to
  0.6 s each; a 30-server island needed 135–141% of its channel for light internet use before), and internet nodes
  still found island content 98% of the time. Over the fake network of `tests/test_topology.py` (1,000 servers,
  21% on islands): island `/find` per internet search 0.76 to 0, island stores per internet publish 2.5 to 0.07. A
  server whose path was unknown is judged again once the query's path request is answered, before a link is opened.
  `rnfs peers` is unchanged; the traffic counters now keep each peer's last link's rate estimate too (in memory).
- **Slow nodes place records better:** a slow node's lookups may make more path requests than its `alpha` (1): up to
  10 per publishing lookup (its 2 disjoint paths 5 each, and a widened lookup 10) and 3 per search (and 3 more over
  the fallback's paths). A slow node hears only some servers' announces, so a publish lookup stopped at the closest
  servers it knew: in the topology simulation an island publisher's records reached 1.3–2.7 of their 10 closest
  servers, and internet nodes found island content 68–91% of the time; with the larger budget 3.7–4.4 and 99%, for
  11–19% more airtime per publish. Over the fake network of `tests/test_topology.py` (1,000 servers): 3.6 to 6.9 of
  the 10 closest. Nodes that aren't slow are unchanged (`alpha` per lookup), as is the keeper query's own budget.
- **LoRa islands keep their own content findable** (spec section 6, "LoRa islands"; nothing on the wire changes): a
  node with a slow interface, or naming `local_interfaces`, keeps a set of *local* servers, those whose RNS path goes
  out on a named interface, or (none named) within `local_hops` (3) hops over an interface below `slow_bitrate`,
  judged from the path to each server as its announces arrive and from one read of the path table every 10 minutes
  (through `rnsd`, one RPC: a client's own path table starts empty). It also stores each key it provides on its
  `local_replicas` (3, at most 5) closest local servers (*two-level records*: ordinary `/provide`s, withdrawn like
  the rest), and a search asks the closest local server first (3 of them with a hop radius), its answer seeding the
  rest of the lookup. In the topology simulation an island neighbour found content during a partition 100% of the
  time instead of 55–72%, and island-to-island lookups took 42% less airtime, for 12% more publish airtime. Fast
  nodes with no slow interface and no `local_interfaces` are unchanged. New settings `local_interfaces`,
  `local_hops`, `local_replicas`; `rnfs status` gains `local_servers`.
- **Partition mode** (spec section 6, "LoRa islands"): a node with local servers that fails 3 times in a row to reach
  off-island servers, or hears no off-island announce or answer for over 4 times the usual gap between off-island
  announces (at least 10 minutes) and then gets no answer from one it probes, uses its local servers only for
  lookups and publishing, so a search during a gateway outage ends in the time its island takes instead of minutes
  of link timeouts. Any off-island answer or fresh announce ends it, and a probe of one off-island server every 5
  minutes (on slow nodes after 15 minutes, doubling up to 2 hours) keeps it from sticking. Silence alone never enters
  it, so a small connected network with sparse announces doesn't flap. `rnfs status` gains `partition` and
  `partition_since`.
- **The heal republish:** keys whose last publish round reached no server beyond the island (during a partition) are
  marked, and republished as soon as partition mode ends, paced as a slow node's rounds are, so the rest of the
  network finds them within hours instead of after their next republish (3 days on a slow node). The marks are kept
  across restarts (a new table, `published_heal`).

  Together, over the fake network of `tests/test_topology.py` (1,000 servers, 21% on 8 islands, an island's gateway
  down): a neighbour found what an island node published before the partition 100% of the time instead of 67%, and
  what it published during it 100% instead of 91%, with 0.15 timeouts and 31 s of simulated time per search instead
  of 11.7 and 490 s; the backbone found what was published during the partition 0% of the time until the heal
  republish, 99.5% after it. Over a simulated week with 5 or 20 servers beyond the island announcing every 6 hours,
  partition mode was entered only while the gateway was down, and left within 49 minutes of its coming back.
- **`/provide` to 16 servers at once:** a node that isn't slow sends a publish round's `/provide`s to up to 16
  servers at once, each still over its own identified link with its batches in order (withdrawals first, stopping
  at a refusal), so a round over many servers takes a fraction of the time. Slow nodes still send one at a time.
- **Slow nodes pace withdrawals:** unpinning many keys no longer sends every withdrawal at once over radio. A slow
  node sends withdrawals along with its round's stores to the same servers, and each server only withdrawals
  reach counts as one of the round's 10 lookups (up to 5 kept for them while any are queued), so the round waits
  30 s for it before the next. Other nodes still send withdrawals at once.
- Kept names' keys take their fixed ±10% republish jitter from the key's first byte rather than the whole key: 256
  slots still spread them out, and keys sharing their leading bits (the same servers are closest to them) come due
  together. A key's interval may change once, by at most a fifth, at the upgrade; it stays below 3.5 days.
- **On disk:** records are read from the state database per `/find` through an index, with a small cache, instead
  of all held in memory (about 500 bytes each before): memory no longer grows with the records or their publishers.
  Changes are written together every 30 s; a republish is written only when it moves a record's last-stored time by
  more than 6 hours, and a refused store writes nothing. A flood of 300 000 fresh identities into a full store of
  200 000 records cost about 70 µs a store and 112 transactions in all, on a desktop NVMe disk.
- **A byte budget instead of a record count:** records may take `record_budget` of disk (new, default 1 GiB: about
  7 million records at the 150 bytes a record takes, measured from the database), never the free disk the blocks'
  reserve needs. `max_records` (100 000 records) goes.
- **No per-publisher caps while there is room:** anyone stores freely until the budget is full, so a large
  publisher (a mirror) is no longer held to 1% of the store, nor a new one to a tenth of that. The newcomer pool and
  the memories of departed and evicted publishers go too.
- **Storage tiers once the budget is full:** Known (contacts of kind `node`, private-mode members, bootstrap
  nodes; 30% floor), Earned (publishers that refreshed a record on 3 distinct UTC days; 60%) and Newcomer (everyone
  else; 10%). A store to a full node takes a record from a tier's largest holder: the lowest tier below the storer's
  above its floor first, then, if the storer's own tier is below its floor, a higher tier above its own; else
  within its own tier, refused for one holding as many as the largest, and for Earned publishers, which never evict
  each other. While full, a Newcomer's record expires 4.5 days after it was last stored (otherwise every record
  still lives `PROVIDER_TTL`, 7 days). A key still keeps at most 1000 publishers, now giving way by tier, with a
  share kept for Newcomers (Security, above).

Changed:
- A pacing wait no longer closes the link: after a response on a slow link the fetcher waits as long again before
  asking that provider again (the responder's upload pacing), and on LoRa a 64 KiB chunk takes about 9 minutes, so
  the wait outlasted `link_idle_timeout` (5 minutes) and the idle link was closed, taking its rate estimate with it:
  the next response was sized and paced as on a new link. The link now counts as in use while its provider is
  waited for.
- Slow links are judged per provider, not per node: a gateway with a TCP and a LoRa interface isn't a slow node,
  so it ran 3 parallel `/blocks`, probed open links for the root and duplicated any batch out for 30 s, all over
  the radio. Now a provider is reached over a slow link when the link's rate estimate is below `slow_bitrate` (or,
  without one, the next-hop interface is, or the provider is behind a slow path); providers over slow links
  behind one next-hop interface share one request at a time, a node that isn't slow probes no open link over a
  slow link (a slow node still probes one, as before: a hit saves a lookup over the radio), nothing is duplicated
  onto a slow link, and on fast links a batch is duplicated only once it has taken 1.5 times its blocks' transfer
  time at the rate estimate (and `deadline_slack`, as before).
- A lost packet no longer drops a provider: a `/blocks` request RNS times out (its packet or its answer's lost,
  which RNS doesn't resend), whose link times out under it, or whose new link isn't established, failed its
  provider for the rest of the fetch at once, so at 5% packet loss most 1 MiB fetches from one provider failed.
  Now its link is closed (which starts no cooldown at the provider) and the request asked again on a new link
  after the busy delay; the provider fails after `max_failures` such requests in a row. A link closed by either
  end, no path, or a malformed answer still fails it at once. Measured over the fake link of
  `tests/test_fetcher.py`'s `LossyLinkTest` (one provider, each request lost with probability 5%), 1 MiB fetches
  succeeded 39% of the time before and 99.7% after (300 fetches each), or 97.6% when each new link's request and
  proof are lost at 5% too.
- A DHT client that only fetches (it provides nothing, owns no names, accepts no remote pins and isn't in private
  mode) announces `rnfs.dht` only at startup, no longer every `announce_interval`; nothing needs to reach it. It
  announces again at once when it starts to provide. Servers, and clients that provide, announce as before.
- Blocks are stored in subdirectories (`blocks/<xy>/<cid>`): one directory of millions of files is slow to list,
  and FAT32, common on the USB drives added to a Raspberry Pi, holds only about 10,900 such names per directory
  (a 59–64 character name takes 6 of its 65,536 entries). The cost is at the durability points: storing blocks
  syncs each subdirectory written to (up to 1,024, 16 at a time) where it synced one directory. Adding 64 MiB
  (1,029 blocks) to a new store on a desktop NVMe disk: 1.02 → 1.41 s; a tree of 1,024 8 KiB files 0.95 → 1.43 s;
  fetching them (each block synced on its own) unchanged at about 13.5 s (`tests/bench_block_writes.py`,
  `today`, median of 3). Raspberry Pi SD-card numbers are still to be measured.
- `rnfs.apiclient` (the CLI, `rnfs.Client`) refuses a daemon older than 0.1.5 before sending it the token, rather
  than sending the request on a second connection as it did for them until now (the fallback planned to go in
  0.2.0): stop such a daemon and start the new one. Every daemon of 0.1.5 or later works as before.
- `POST /api/import` (and `rnfs import`) refuse anything but a regular file, without waiting on a FIFO.
- `Config.validate` refuses `api_port` equal to a non-zero `gateway_port`.
- A request that times out raises `ConnectionFailed` saying the daemon "didn't answer within its timeout".
- `Config.load` reports invalid TOML as "<path> isn't valid TOML: …" (still a `ValueError`).
- A fetch to pin that fails keeps what it got: `pin add`, `dir set` and `dir rm`, remote pins, followed names and
  deny lists record it as a cached fetch, as `get` and `cat` already did, so trying again resumes rather than
  starting over (a 4 MiB pin over LoRa failing at 90% fetched all of it again). Not when it failed on denied
  content, an invalid DAG or a size bound or quota (`TooLarge`), which a retry can't get past. The record is cache
  like any other (evictable, removed by `gc`), and never provided as a download of the user's.
- Remote pins don't give up on content not yet published. The node asked to pin asks the node that asked first
  (its node ID follows from the identity of the `/pin`'s link), which serves what it added before its provider
  records are out. A pin whose content can't be found waits and is tried again after 10 minutes, doubling up to 6
  hours, for up to 3 days, instead of being dropped at once; `pin remote ls --served` shows it waiting
  (`not_found`) with when it is tried again (`retry_at`, a new field of `GET /api/pin/remote/served`). Pins
  accepted after it aren't held up, and a release ends the wait at once.
- A `get`, `pin` or `export` job whose content can't be found is queued again the same way, keeping its blocks,
  rather than failed: `retry_at` (and `retry_since` while it waits) are new fields of a job, and `rnfs jobs` shows
  it waiting. Not for a path its directory shows doesn't exist, nor for a job submitted with the new `retry: false`
  (`POST /api/jobs`; `retry=False` in the library's `jobs.get`, `pin` and `export`), which fails as in 0.1.6. A
  foreground `rnfs get`, `pin add` or `export` submits so, and still fails at once; `--background` jobs wait.

Faster:
- Range reads (`cat`, the gateway, sealed reads) size their window by the providers' links: they fetched 32 chunks
  (2 MiB) before handing over the first byte, about 5 hours at LoRa's 1 kbit/s. While no provider known is reached
  over a fast link (or none is known yet), the window is one chunk with the next asked for ahead, so the first
  byte comes after one chunk (about 9 minutes at 1 kbit/s); over fast links it stays 32.
- A start no longer lists every stored block before the API answers: their sizes are kept in the state database
  (`block_sizes`) and read from there after a clean stop. After a crash or power loss the start lists them all,
  as before, and corrects the table. Opening storage on a desktop NVMe disk (warm cache), small blocks: 100 000
  of them 0.50 → 0.13 s after a clean stop (0.65 s after an unclean one), a million 5.4 → 1.2 s (7.2 s after an
  unclean one: 1,024 directories listed and the table compared). The first start, moving them out of 0.1.6's flat
  layout, took 4.2 s and 74 s. A cold cache and a slow disk should gain more, as the table is one file read in
  order: Raspberry Pi SD-card numbers are still to be measured (`tests/bench_startup.py`, `--cold` for a cache
  dropped as after a reboot).

## 0.1.6

Security fixes, maintainability work and performance work. No wire-format or CID changes: these nodes, 0.1.5,
0.1.4 and 0.1.3 nodes work together. The upgrade of an `<rnfs dir>` is automatic and one-way.

Upgrade notes (each is described in full below):
- **Settings and the rnfs dir:** a symlinked `rnfs.toml`, or one or a subdirectory owned by another user, stops the
  start; a loose one (such as a 0664 `rnfs.toml` or 0775 `blocks/`) is made owner-only, with a warning.
- **The shared folder:** one that leads into the rnfs dir (or a directory holding it) is refused, and its
  pins with source `folder` are released.
- **Following names:** a followed name's target may take at most `follow_quota` (new, default 1 GiB).
- **Deny lists:** while the deny list has `sha256:` entries, a file repeating its content more than 16 times
  (beyond 64 MiB) can't be read or imported (451).
- **Local API:** `POST /api/lxmf/inbox/get` answers 404 for a name link whose name can't be resolved, unless the
  request sets `use_sent_cid`.
- **Gateway:** browsers that send no `Sec-Fetch-Site` (Safari before 16.4, Firefox before 90) are treated as web
  pages, so `?format=` downloads give them 409.
- **`rnfs inbox`:** its message line is reordered (address and trust first, the display name last, quoted).
- **install.sh:** it stops when the GitHub CLI can check provenance but isn't logged in; log in with
  `gh auth login`, or set `RNFS_REQUIRE_ATTESTATION=0`.
- **Flatpak:** after updating, the Flatpak no longer sees the rest of your home folder or any device. Grant what
  you used again: `flatpak override --user --device=all io.github.afit21.rnfs` for an RNode or other serial
  interface, and `flatpak override --user --filesystem=<folder> io.github.afit21.rnfs` for other folders,
  `~/.config/reticulum` included if your Reticulum config is there.
- **State:** the state database gains a table (the shared folder's file cache); an older rnfs can't open it
  afterwards.

Security:
- The release workflow's Flatpak build no longer uses the GitHub Actions cache. Any workflow run on the default
  branch can write that cache, and a poisoned one would have gone into the attested `.flatpak` bundle.
- `rnfs inbox get` no longer quietly fetches the CID a sender put in a name link when the name can't be
  resolved. The sender chose that CID and nothing ties it to the name, so any sender could pose as a
  well-known name. It now fails before writing anything, unless you pass `--use-sent-cid`; then the result
  carries a warning. Auto-fetch (trusted senders only) keeps the fallback, and records it.
- A `/blocks` requester that rejects or cancels a response before a quarter of it was sent now cools down for
  `blocks_cooldown`, per link and per identity, as one that rejected it unsent already did. Before, a requester
  could take the first window of each response, cancel it and ask again at once, so the node built a whole
  response per window. Closing the link still starts no cooldown, so an honest reader that gives up early isn't
  held back.
- While the deny list has file hashes (`sha256:` entries), a file that declares more than 16 times the bytes of
  its distinct chunks, plus 64 MiB, is refused (451) before it is hashed. Hashing reads every repeat of
  repeated content, so six blocks (128 KB) declaring 2^56 bytes made a one-byte `cat`, a gateway range request,
  `?format=archive` or an `import` hash for about a year, holding a gateway slot. `cat` and the gateway now fetch
  a file's whole DAG before checking it (unless its hash is already known), so `max_download_size` and the disk
  reserve apply as they do for `get`.
- A gateway HEAD (or a range not starting at byte 0) of a stored file the deny list blocks answers 451, like
  GET. Before, it took its Content-Type from a denied stored chunk, and a HEAD of a file with a denied hash
  answered 200.
- A name whose owner is offline can still be resolved through its keepers when Sybil nodes near its keeper key
  stall the DHT lookup. They could answer `/find` slowly with ever closer IDs, so the search used all of
  `lookup_timeout` and the keepers then had one second to answer. Each batch of `/name` queries now has its own
  budget (87.5 s at the defaults); the search for keepers stops once some are known and `lookup_timeout` / 5 has
  passed, and resumes if their answers don't settle the name.
- A neighbour replaying an older, validly signed name announce no longer makes its record look current: rnfs
  reads the replayed announce's own emission time from RNS's announce cache, treats one older than the path's
  (or carrying another record) as a suspected replay, gives its real age and asks keepers.
- A provider can no longer hold a fetch for hours by dripping a `/blocks` response: once nothing is left to
  request and a provider is idle, a batch in flight longer than `deadline_slack` is asked of it too, the first
  verified copy wins, and the slow request's link is closed (left to finish if another fetch uses it) without
  counting as a stall. Simulated: 20.6 → 3.3 s.
- `get` of a directory can no longer be redirected by someone who can rename inside the output swapping one of
  its directories for a link or junction (or another directory) while it is written. On Linux and macOS the
  output being written is private (0700, setgid kept) and must start empty, every entry is created relative to
  its parent's descriptor without following links, and each directory is checked by device and inode before
  anything is written into it. On Windows it gets a private access list while it is written, and each file's
  final path must be inside it. A swap fails the get with "output was changed while being written".
- On Linux and macOS, `rnfs.toml` is now checked before its settings are used, as on Windows: a symlink, a FIFO
  or anything else that isn't a regular file is refused, and so is a file another user owns; one others may
  access is made 0600, with a warning; at most 1 MiB of it is read. Its settings choose the Reticulum config, and
  with it programs rnfs starts, so anyone who could write it could run code as you. Reticulum's `config` gets the
  same check when `reticulum_dir` is the rnfs dir.
- The rnfs dir's subdirectories (`state`, `blocks`, `storage`, `names`, `lxmf`) are checked at every start: a
  symlink, a non-directory or one another user owns stops the start before Reticulum starts; a loose one is made
  0700, with a notice; and if others could write to it, entries another user owns are reported. Before, a user
  who could once write in a loose rnfs dir kept their access to `blocks/` and `names/` after it was tightened.
- The shared folder is no longer scanned when `files/` leads into the rnfs dir. Before, a symlinked `files/` that
  led to `storage/` or `names/` (or that someone who could write to a directory above it retargeted there)
  imported, pinned and provided the node's identity or the name keys, and one that led to the rnfs dir or a
  directory above it shared everything in it. A folder that resolves, as opened, to the rnfs dir, a directory
  holding it, or anything inside `storage/`, `names/`, `state/`, `blocks/` or `lxmf/` is now refused with a
  warning and scanned as empty: nothing is imported, and every pin with source `folder` is released, so keys a
  0.1.5 node already shared that way stop being provided at the first scan after the upgrade. On Linux and macOS,
  a directory above the folder that other users can change gets a warning, once.
- A node no longer reads request packets sent to it on `rnfs.dht` links it opened. That end of a link has no
  request handlers, yet RNS decoded each request (up to the link's packet size) and started a thread for it
  before finding nothing to run, for any node rnfs linked to. LXMF and host-program links are unchanged.
- An explicit `max_upload_rate` now applies to every `/blocks` response, including one that fits in one packet.
  Such responses never give the link a rate estimate, and the cap applied only once there was one, so on a
  large-MTU path (RNS's TCP interfaces) a requester asking only for single-packet answers was never paced. Waits
  of up to 5.6 s are held and longer ones refused as busy, as for larger responses. The default cap (half the
  rate estimate, on slow links) is unchanged.
- The wait for a previous `/blocks` response's proof uses the round-trip time the requester claims only if it is
  a finite, non-negative number, and still at most 5 s. Before, a negative claim gave an honest proof no grace,
  and a value that wasn't a number made the request handler fail.
- `install.sh` ties a release install to the tag it asked for: where `gh` supports them, the provenance check
  passes `--source-ref=refs/tags/<tag>` and `--deny-self-hosted-runners`, and only an `rnfs-<version>-*.whl` of
  that tag's version is installed. Before, an older release's genuinely attested sdist, renamed to the new
  release's file name, passed both checks.
- `install.sh` stops when the GitHub CLI has `gh attestation` but isn't logged in (root installs usually hit
  this), instead of quietly checking `SHA256SUMS` only. Log in with `gh auth login`, or set
  `RNFS_REQUIRE_ATTESTATION=0` to install with the checksum only. Without `gh` it still warns, and the README now
  says that `curl | sh` without `gh` trusts whoever can write the release's files.
- The Flatpak's sandbox is narrowed to `~/.rnfs`, `~/.reticulum` and the Downloads folder, with no devices
  (before: your whole home folder and every device, so a compromised daemon could write `~/.bashrc`). See the
  upgrade note below.
- `install.sh` warns when `RNFS_SOURCE` is, or names, a plain `http://` URL, and the `.deb`'s postinst
  byte-compiles with `python3 -I`.
- The file-index-node NomadNet page renders at most once a minute, one visit at a time, and serves its cached
  page meanwhile (before, every visit ran `rnfs pin ls`).
- `examples/rnfs_share.py` and `rnfs.Client`'s log escape names and daemon messages, so a name can't drive the
  terminal.
- `rnfs.Client` and the CLI check the daemon's proof before believing anything else the API port answers: while
  the daemon is down, a program listening on its port can no longer make the client raise `Denied`, `NotFound`
  or `StorageFull` with its own text. Without a valid proof a 503 is `Busy` and anything else `AuthFailed`, both
  in the client's own words. Connecting and proving time out after 15 s (or the client's timeout, if shorter),
  and at most 64 KiB of the proof response is read.
- After the proof, the client sends the request on a new connection only to a daemon that proved a version below
  0.1.5 (those close every connection); a newer daemon that closes the proved connection gets
  `ConnectionFailed`, and nothing is sent. The fallback for 0.1.4 daemons goes in 0.2.0.
- A followed name can no longer fill the follower's disk: its new target is fetched only if its size, and the
  bytes of its DAG's blocks, fit in `follow_quota` (default 1 GiB); otherwise the check fails, the follow's state
  says why, and the previous target stays pinned. While one target replaces another both are held, so a followed
  name can use up to twice the quota for a time. Targets already pinned over the quota stay.
- An auto-fetch sender can no longer push another auto-fetch sender's pinned messages out of the inbox.
  Re-linking a CID it already pins costs no quota, so about 500 such messages filled the inbox with protected
  records, and each further message dropped the oldest record of anyone, releasing its pin. Each of the n senders
  in `lxmf_auto_fetch_from` now keeps at most 1/n of the inbox's messages and bytes, messages holding pins
  included, and when every other message holds pins, a new one may push out only its own sender's.
- Auto-fetches still queued for a sender removed from `lxmf_auto_fetch_from` are refused at the next start (with
  LXMF off too) and when their turn comes. Before, up to 64 of them were fetched and pinned after a restart.
- `rnfs inbox` shows each sender's address before its display name, marked `untrusted`, `trusted` or
  `contact <nick>`; the display name, which the sender chooses, comes last, quoted and escaped. Before, a display
  name like `alice <trusted address>` came first and pushed the real address off to the right.
- A node's own provider records can no longer be evicted from its own store: about 1000 Sybil stores to a key it
  provides pushed its record out, and a full newcomer pool could take it too. Its own records don't count toward
  the publishers a key keeps and are never chosen for eviction.

Changed:
- `POST /api/lxmf/inbox/get` answers 404 for a name link whose name can't be resolved, unless the request sets
  `use_sent_cid` (before, it fetched the sender's CID).
- While the deny list has file hashes, a highly repetitive file over 64 MiB (a zero-filled disk image, say) can't
  be read or imported (451). The user's own gateway HEAD of a file that isn't wholly stored is then typed by its
  name only, not by its first bytes.
- On Windows, a directory `get` output is private to the user while it is written, then inherits its folder's
  access list again before it gets its final name (if that fails, a warning is logged and it stays private).
- A symlinked `rnfs.toml` is refused: dotfile setups that symlink it into the rnfs dir stop working (put the
  file itself there). A loose `rnfs.toml` (such as 0664) or subdirectory (such as 0775) is tightened at the next
  start, with a warning; it isn't refused.
- The daemon and `rnfs.Node` check, in order, the rnfs dir, the settings, the subdirectories, then take the lock.
  The CLI prints settings warnings, and `rnfs.Client` raises them as `RuntimeWarning`. On Windows, a loose
  `rnfs.toml` no longer makes the CLI or `rnfs.Client` load RNS.
- When a file is refused because its SHA-256 is on the deny list, the log suggests `rnfs deny add <file cid>`
  (once per file): a `sha256:` entry doesn't stop the file's blocks being served one by one, and denying its CID
  does.
- The README says plainly that pipx and plain `pip install` resolve dependencies unpinned, not at the versions in
  `packaging/requirements.lock`.
- The gateway treats a request without `Sec-Fetch-Site` as the user's own only if its `User-Agent` doesn't start
  with `Mozilla/`. Browsers without Fetch Metadata (Safari before 16.4, Firefox before 90) are now treated like a
  web page on every request: `?format=` downloads get 409, and their network fetches count against
  `gateway_fetches`. curl, scripts and other tools keep the user's own access, and so does a browser's own
  navigation (`Sec-Fetch-Site: none`).
- `rnfs name publish` in private mode warns that the name's announce (its sequence and target CID) is visible
  outside the private network; only the content is served to members only. `POST /api/name/publish` then carries
  the warning as an optional `warning` key, and `rnfs.Node`/`rnfs.Client` raise it as a `RuntimeWarning`; outside
  private mode nothing changes.
- The specification documents that the API's proof keep-open lets one connection hold an API slot about twice as
  long as one request, and the measured cost of grinding node IDs near a key (raising it needs a change to how
  node IDs are derived, so it is deferred).
- `rnfs inbox` prints each message as number, time, address, trust, method, then the quoted display name (the
  text format isn't a stable interface; scripts should use the API). Display names are also escaped for
  backslashes, blank filler characters (U+115F, U+1160, U+3164, U+FFA0, U+2800), stacked or leading combining
  marks, and names of only spaces.
- The specification documents that many LXMF identities can keep the strangers' bucket empty; contacts and other
  trusted senders are unaffected.
- The request server checks a feature path's response (keepers' `/name`) as requesters do, and answers `False`,
  logging an error, rather than send one they would refuse. Unexpected errors at five places that catch broadly
  (the fetcher's `/blocks` worker, the lookup's routing-table callbacks, sending a request on a link, path
  lookups, the peers listing) are logged as warnings with their traceback in the debug log (the last two at most
  once a minute).
- Stopping the node is exception-safe: every step (features, announcer, request server, names, saves, closing
  the state database, releasing an embedded node's destination) runs even if an earlier one fails, so a failing
  step no longer leaves the database open or the `rnfs.dht` destination registered in a host program. Stopping
  waits at most 5 s in all for the features' threads and the node's own (LXMF and jobs no longer add 2 s each),
  and remote-pin fetches stop with the node (their pins are fetched at the next start).
- A pin name or contact whose save failed (a full disk, say) is written again by the node's next save, at the
  latest at stop, rather than only at the next change.
- Gateway and other resolves with a `max_age` (60 s on the gateway) reuse a network refresh of the name for that
  long, including one that found nothing, and simultaneous resolves of a name share one refresh: a page with many
  `/name/<n>/…` assets whose owner is offline costs one path request and one keeper query, not one each. A name
  republished moments ago may show its older record on the gateway for up to 60 s. CLI resolves always refresh.
- GitHub releases take their notes from the version's section of this file instead of generated notes, and the
  release workflow refuses a tag whose section is still marked unreleased. The check that the Flatpak's wheels
  match `packaging/requirements.lock` runs on every push and pull request, and the release builds (Flatpak,
  Windows exe, arm64 .deb, Debian 12 smoke test) also run, without publishing, on pull requests that touch
  packaging and weekly. The Flatpak build no longer copies local debris from a checkout.
- A block that doesn't match its CID raises `BlockMismatch`, and a damaged sealed file `SealError`: both are
  `RnfsError`s and still `ValueError`s, so the API keeps answering 400 (and logs a `BlockMismatch` as a warning,
  as it means a bug rather than a bad request). A shared-folder entry that fails with an unexpected `ValueError`
  is logged and tried again after 10 minutes rather than skipped until the file changes. `dir rm` refuses a
  target without a path in the node itself, before fetching anything.
- `rnfs jobs` (`--background`, `cancel`, `clear` and the progress line), `rnfs peers` and `rnfs stats` escape every
  field the daemon sends, as the other feature commands already did. Internally, features declare the features
  they use, and the node checks those and every feature's API routes and request paths when it loads them.
- 16-byte IDs written as hex (node IDs, names, identity hashes, LXMF addresses) are parsed in one place with the
  same rules everywhere; the only input refused that was accepted before is a `bootstrap_nodes` entry with
  whitespace between its hex digits, now skipped with the warning other malformed entries get.
- A DHT lookup run also ends after 80 `/find` queries, or after 9 answers in a row (3 × max(`alpha`, `beta`))
  that bring the closest nodes no nearer the key. Honest lookups end as before (in a simulated network of 2000
  nodes they complete within 12 queries); peers see fewer `/find` requests.
- The specification now says a requester that gives up on a `/blocks` response closes its link, and never
  cancels the Resource (0.1.5 and earlier already do so).
- `rnfs inbox` shows the CID each name link was sent as ("sent as `<cid>`"), and marks an auto-fetch that
  pinned the sent CID because the name didn't resolve ("auto-fetch pinned (sent CID; name unresolved)";
  `"sent_cid": true` in the inbox record).

Faster:
- `cat`, gateway and range reads ask for the next 32-chunk window before handing the reader the current one, so
  every provider stays busy across window boundaries: a 48 MiB read over 3 simulated 40 Mbit/s providers went
  from 8.06 to 5.95 s (links 57% → 77% busy; 6.12 → 4.73 s without fsync). A reader that stops early has fetched
  at most one extra window; closing it closes its links, except one another fetch is using (that request is left
  to finish), and never cancels a Resource.
- `get`, `pin add` and `ls` ask for a block's children as soon as the block arrives instead of one tree level at a
  time, and count `max_output_entries` per block (small gains in simulation: up to about 4%, one request fewer on
  a single LoRa provider).
- The fetcher keeps a set beside its queue of blocks to fetch, so requeueing a refused or budget-cut request's
  blocks no longer scans the queue: 22 ms → 0.01 ms per answer with 16 000 blocks pending (1 GiB), 242 → 0.015
  ms at 10 GiB.
- `open()` on `rnfs.Client` or `rnfs.Node` reads ahead while reads are sequential, doubling each refill from 64
  KiB up to 4 MiB; a seek that moves starts again with a read of only what it covers. Sequential 64 KiB reads: 33
  → 560 MB/s through `rnfs.Client`, 79 → 1219 MB/s on `rnfs.Node`; `read()` of a whole file 310 → 589 MB/s.
- An add makes one durable commit instead of two: the pin is committed durably first, and the name `pin ls` shows
  for it is written after it without a sync of its own, so a power loss can lose at most the latest names, never
  a pin. 4 KiB adds 12.9 → 10.5 ms, 1 MiB adds 18.9 → 16.4 ms on a local SSD.
- The shared folder re-reads only the files that changed inside a changed entry: a per-file cache keyed by
  device, inode, size, modification time and execute bit reuses a file's CID while all its blocks are stored and
  none is denied, and its remembered content hash still goes through the deny list. Rescanning a 5000-file, 1 GiB
  folder after a 4 KiB change: 1.20 → 0.10 s (256 MiB: 0.33 → 0.10 s). The cache is kept in the state database (a
  new `shared_folder_files` table, added by the upgrade), so it holds right after a restart too.
- `dir set` and `dir rm` no longer walk the whole tree to check its depth: subtree heights are remembered, so an
  edit reads only the directories it rebuilds. On a 16 GiB tree, an edit after the first went from about 0.9 s to
  0.3 ms (30 edits: 27.5 → 0.89 s).
- Storage no longer walks complete DAGs again under its lock: eviction takes a DAG known to be complete as a
  whole, `dag_bytes` sums it once, and `is_complete` remembers an incomplete DAG's missing block until it
  arrives. With about 550 000 blocks an eviction pass holds the lock for about 240 ms instead of 900 ms;
  `is_complete` on a 4 GiB file missing its first chunk went from 530 ms per call to 0.002 ms after the first.
- Expiring provider records no longer checks every record for a time in the future unless the clock went back
  since a time was taken: one expiry pass at 100 000 records went from 16.6 to 8.7 ms, held under the store's
  lock.
- `cat`, `info` and `GET /api/cat` remember which files aren't sealed (at most 4096; forgotten when the deny
  list changes), so reading one again doesn't read its first bytes to check, and `GET /api/cat` opens a sealed
  file once instead of twice: a 64 KiB read of a stored file 0.32 → 0.19 ms (through the API 0.47 → 0.21 ms).
- `pin ls` remembers each complete pin's type, name and whether it is sealed instead of reading its blocks again
  at every call: 68 → 3.8 ms at 2000 pins after the first.
- The publisher's refresh after an add reads a cached set of pinned CIDs instead of copying every pin, and
  gathers the keys of queued withdrawals once: the first refresh with 10 000 CIDs and withdrawals queued for 300
  nodes 71 → 14.5 ms; a refresh after a pin at 3000 pins 2.25 → 1.69 ms.
- Attached to a shared Reticulum instance, path lookups no longer cost an RPC per node each time (about 41 ms each
  over TCP, which macOS and Windows use for it): each node's next-hop interface is kept for 15 s (hops and whether
  there is a path are still read fresh), and `rnfs peers` and the start of a fetch read every interface they need
  from one path-table RPC. A 1000-peer `rnfs peers` went from 41.4 s to 95 ms, and the path lookups of 20 open
  links at the start of a fetch from 825 ms to one RPC (under 0.1 ms within 15 s of the last).
- Read-only identity checks no longer mark destinations as used in RNS (the peers listing, the DHT lookup's
  known-identity check, private-mode routing admission, the link opener's polls while it waits for an identity):
  on a shared instance each mark was an RPC. A link actually opened still marks its destination.
- The peers feature no longer registers a second `rnfs.dht` announce handler: the node's own records what `rnfs
  peers` shows, which halves the threads RNS starts per announce (2 → 1; about 130 → 62 µs of dispatch each).
- Opening a node scans its block directory about twice as fast: block file names are checked for canonical CID
  form without being encoded again (10.4 → 4.5 µs per block, about 6 s less per million blocks).
- Hearing a DHT announce no longer scans every remembered ban and dropped node: 201 → 0.9 µs with 3000 dropped
  nodes (247 → 0.9 µs with 3000 bans as well).
- A background job nobody polls no longer keeps a rate sample for every second it runs (about 10 MB a day): it
  keeps the last 5 minutes' (301 samples), and reports the same rate.
- An LXMF auto-fetch checks whether its sender already pins a target with one lookup instead of copying every
  pin: 1.96 ms → 0.24 µs per link at 10 000 pins.
- An idle node no longer wakes every second: the LXMF loop polls only while a message is being sent or held or a
  propagation-node download runs (a newly queued message is polled at once), and on Linux and macOS the daemon's
  main thread waits for SIGINT/SIGTERM with no timeout (Windows still wakes each second to run signal
  handlers): 120 fewer wakeups a minute on an idle daemon.

Added:
- `rnfs inbox get --use-sent-cid`, and an optional `use_sent_cid` on `POST /api/lxmf/inbox/get`.
- `trusted` and `contact` fields in `GET /api/lxmf/inbox` records, and an `untrusted_dropped` counter in
  `status` (messages from untrusted senders dropped by the rate limits since the start).
- `rnfs.BlockMismatch`, `rnfs.SealError` and `rnfs.NotRecipient`, to catch these errors by class. `ApiConnection`
  has a typed method for every core API route (`dir_set`, `dir_rm`, `pin_remote`, `providers`, `ping`,
  `name_import`, `name_export`, `name_rm`, `export`, `import_archive`, `gc`, `verify`).
- `tests/security/README.md`: an index from each security finding's audit ID to its regression tests, the code they
  guard and the commits that fixed it, grouped by subsystem (a test checks that every security test is listed).
- CONTRIBUTING.md: running the tests and their options, lint, the dependency locks, adding a schema step or an
  API route, and releasing. `specification.md` names the version it describes and that the wire format is
  byte-identical since 0.1.3. Dependabot updates the workflows' pinned actions. Tests check that the supported
  Python versions agree across CI, `pyproject.toml`, the lock scripts, `build-deb.sh` and `install.sh`, and that
  this file's version is `rnfs.__version__`'s.
- The `follow_quota` setting: bytes a followed name's target may take (default 1 GiB; `0` stops fetching
  followed names).
- `contrib/rnfs.service.d/hardening.conf`: an opt-in systemd drop-in with `ProtectSystem=full` and
  `PrivateTmp=yes`, for systems with unprivileged user namespaces. The default unit sets neither, nor `UMask`.

## 0.1.5

Performance work, a debug log, state kept in one SQLite database, deny lists, and a public API for apps. No
wire-format or CID changes: these nodes, 0.1.4 and 0.1.3 nodes work together. The upgrade of an `<rnfs dir>` is
automatic and one-way (an older version can't read it afterwards).

Changed:
- State is kept in one SQLite database, `<rnfs dir>/state/rnfs.db` (Python's standard `sqlite3`; no new dependency,
  but a Python built without its `_sqlite3` module now stops at startup, saying so), instead of 17 JSON files. At
  the first start the JSON files of 0.1.4 or 0.1.3 (pins, cache records, pin names, fetched targets, remote pins
  waiting, invalid roots, publish state, provider records, names and the name floor, the shared folder, jobs,
  keepers, follows, contacts, the LXMF inbox and outbox) are imported, each in one transaction, with every check
  the old loaders applied, and kept as `<name>.migrated` (a node that skips versions upgrades the same way). A
  damaged file is moved aside as `.damaged` as before; an unreadable `pins.json` or a damaged `names.json` still
  stops startup. Nothing changes on the network: request paths, messages, CIDs, DAG bytes, provider and name
  records are as before.
- Saves write only what changed: a pin or release is one row, about 2.5 ms whatever the number of pins
  (before: `pins.json` rewritten whole, 5 ms at 100 pins and 14 ms at 20 000); a provider-records save writes
  the records changed since the last one (one change at 100 000 records: 0.02 ms, from 155 ms); a cache save
  the records touched (0.02 ms, from 13 ms at 10 000 records). Startup with 20 000 pins and 100 000 provider
  records loads in 350 ms, from 460.
- Pins, names, jobs and the other state saved on each change are on disk when the change returns, as before.
  State saved in batches (cache records, fetched targets, invalid roots, remote pins waiting, provider records,
  publish state, the name floor, keepers, the LXMF inbox and outbox) is no longer synced to disk at each save:
  a power loss (not a crash) may lose the last of those saves as well as what changed since, never part of
  one, and never one without every earlier save.
- A `providers.json` that can't be parsed is now kept as `.damaged` (before, the node started without its records
  and overwrote it). Node IDs and keys in `published.json` must be 16 bytes (rnfs never wrote others).
- The database and its `-wal` and `-shm` files are readable by their owner only. rnfs refuses to start on a
  damaged database (it runs SQLite's `quick_check` at startup) or one written by a newer version, and says how
  to recover: the `.migrated` files are kept for that. A state directory it can't write to now stops startup
  too, saying so (before, the node started and every save failed).
- Three more built-in bootstrap nodes (Johannesburg, Montreal and Stockholm; eight in all), which every node
  contacts when joining unless `default_bootstrap = false` or it is in private mode.
- A compressed Resource advertised on an `rnfs.dht` link is now refused before any of it is transferred (before,
  it was transferred, then failed as corrupt and the link torn down), and a request that wouldn't fit one packet
  is refused locally instead of being sent as a Resource for the peer to refuse. Honest peers see no change.

Added:
- A debug log file, for diagnosing problems: `debug_log = true` in `rnfs.toml`, or `rnfs daemon --debug-log`,
  writes `<rnfs dir>/debug.log`. It begins with the daemon's version (then the Python, RNS and LXMF versions, the
  platform and the settings changed from the defaults), and has every rnfs message at every level whatever
  `verbosity` is; Reticulum's own messages at Info and more severe that its loglevel shows, such as an interface
  that can't connect or a socket that drops (its Verbose and Debug detail stays on the console only); each gateway
  request, numbered, with the headers that decide it, what was decided (stored or fetched, type, range) and its
  status, size and time; errors with tracebacks, and threads that die; and a status line every 5 minutes (counts,
  open links, each interface up or down). New detail, at debug level: each DHT lookup's queries (which node
  answered or failed, and how the lookup ended), the path to each node (hops, interface), why a link closed or
  couldn't be established, each request's outcome or why it went unanswered, and incoming links. The file is
  readable by its owner only and never sent; each start keeps the previous run's as `debug.log.1`, and past 16 MiB
  the file moves there too, so the two stay under 32 MiB.
- A public API for programs, provisional until 0.2 (`rnfs.API_STATUS`): `rnfs.Node` runs an rnfs node inside a
  program, beside the program's own Reticulum, and `rnfs.Client` uses the user's running daemon through its local
  API. Both have the same methods (add, get, a seekable `open()` that fetches only what it reads, `ls`, `info`,
  pins, archives, names with `publish` and `watch`, jobs with progress callbacks, the deny list), return the same
  typed objects (`rnfs.Cid`, `Entry`, `Record`, `Job`, `Name`, ...) and raise the same errors, so a program
  switches between them on one line. `import rnfs` stays light: `rnfs.Client` never loads Reticulum. See "Building
  apps on rnfs" in the README and `examples/rnfs_share.py` (the source archive includes `examples/`).
- An embedded `rnfs.Node` applies rnfs's Resource guards (no compression, no requests carried as Resources, one
  segment, no metadata, transfer sizes) to its own `rnfs.dht` links only, so the program's own requests,
  transfers and compressed Resources work as with RNS alone. It defaults to a DHT client with LXMF off, never
  stops the program's Reticulum, and locks its rnfs dir as the daemon does, so the two can't share one.
- `rnfs.apiclient`, the local API's protocol layer under `rnfs.Client` and the CLI: it finds the rnfs
  directory, port and token as the CLI does, makes whatever listens on the API port prove it knows the token
  before sending it, on the connection carrying each request, and raises a typed error for each failure (each also a subclass of the node's matching
  error). Standard library only. The CLI now uses it, with the same output.
- `GET /api/name/watch`, a long poll for newer records of names, and a `from` parameter on `GET /api/cat`
  (specification section 11).
- Deny lists, to keep chosen content off a node (a public gateway or provider node): `rnfs deny add|rm|ls` edits a
  local list of CIDs and file hashes (`sha256:<hex>`) (or `GET /api/deny`, `POST /api/deny/add|rm`), and
  `deny_lists` in `rnfs.toml` names lists to apply, by CID or `name/<name>` (followed as the name changes; each
  version is checked, at most 4 MiB and 100 000 entries, and pinned while in use). A denied CID is never fetched,
  read, stored, served or provided, as a root or anywhere inside a DAG; the API and gateway answer 451. Records
  already published are withdrawn, cached copies are deleted and remote pins holding it released, and a remote pin
  of it is refused; your own pins are kept but not served or read, and the log names them. File hashes are checked
  where a file is assembled (`get`, `cat`, the gateway, `add`, the shared folder, `import`), after its blocks have
  been downloaded; the SHA-256 of each file checked is remembered (10 000 files, least recently used forgotten
  first) and kept across restarts, so a file isn't downloaded and hashed again before its first byte after a
  restart (a stored 64 MiB file's first byte: 6.5 ms, from 60 ms). `rnfs status` shows `deny_entries` and
  `deny_lists`, and `pin ls` the lists' own blocks under the source `deny`. Nothing changes on the network: a peer
  asking for a denied block is told it isn't held.

Fixes:
- Anyone able to reach the node's LXMF delivery destination could pass the 256 kB message limit by sending a
  Resource in many segments, each within the limit: RNS appended them to one file under its storage, kept
  until the last arrived, and LXMF read the whole Resource into memory before checking its signature. A
  delivery Resource of more than one segment is now refused before any of it is transferred (a message within
  the limit always fits one), in an embedded node too.
- The local API's clients proved the daemon once, then trusted the port: a program kept running across a daemon
  restart (an `rnfs.Client`, a long `rnfs get` or job wait) sent the token and request bodies, name keys being
  imported among them, to whatever listened on the port next, and took its answers. The daemon now proves itself
  on the connection carrying each request (it keeps a proof request's connection open for one more request), and
  a client reads the token again when the proof fails, so it also goes on working after the daemon restarts
  (before, it got 403 until restarted itself). A client of this version still works with an older daemon.
- A neighbour repeating a name's old announce could keep a node that missed the newer one on the old record
  indefinitely: RNS remembers a repeated announce as just heard, so the record looked current, the checks of
  followed names and deny lists skipped the network, and resolves never asked keepers. An announce now shows
  its record current at most 2 × `announce_interval` after it was emitted (its signed time, which repeating it
  doesn't change); past that, resolves ask keepers.
- A far node that keeps announcing but never accepts a link cost every lookup its connection timeout (60 s or
  more at 9–10 hops), and held lookups open long after they had their answer: its announces reset the failure
  count that would have dropped it, and other nodes kept naming it in their replies once it was dropped. A node
  whose last 3 (`max_failures`) queries failed is now asked only when a lookup has too few others, whatever
  its announces; it is asked again after 5 minutes, doubling up to an hour, without the lookup waiting for
  its answer, and an answer restores it. Nothing changes on the network.
- A gateway or LXMF auto-fetch resolve that found a higher sequence for a name it had resolved before raised
  the name's floor without saving it until the next re-announce pass (up to a minute later) or shutdown, and
  so did an announce raising it with no resolve. Every raise is now saved within 5 s, as documented.
- `gc` (and eviction) could delete blocks of a complete pin. `stat` of a DAG only partly stored remembered the
  nodes it didn't find as listing nothing, so once the rest was fetched and pinned, collection didn't see what
  those nodes reach. Until a restart, which cleared it.
- `import` pins the roots an archive supplies with one save of the pins; if that save fails, none of them is
  pinned (before, those ahead of the failure were).
- A second daemon started on an rnfs dir already in use loaded and saved the same state and announced the same
  node ID before stopping on "address already in use". It now stops at once, saying which process uses the
  directory: the daemon (or an embedded `rnfs.Node`) holds a lock on `<rnfs dir>/daemon.lock`.
- With `-v`, a requester no longer says a provider "cancelled" a `/blocks` response that may have been lost in
  transit (it can't tell those apart): it says the response stopped arriving. The provider now logs, at debug
  level, a response whose transfer RNS gave up on (lost in transit, or its link closed).
- A remote pin or LXMF auto-fetch of content this node already held in full (a pin of your own, a cached
  download) was accepted however many bytes of blocks it held, past `remote_pin_quota` or
  `lxmf_auto_fetch_quota`: the quota was only counted for blocks fetched. It is now refused as when fetched.
- A failed save at shutdown (a full disk) stopped the shutdown there, leaving the state database, links and an
  embedded node's destination open; it is now logged and the rest of the shutdown goes on.
- `rnfs pin ls` waited for a shared-folder scan in progress to finish importing (minutes for large files).
- A shared-folder file whose import failed for a reason that passes (a full disk, a pin that couldn't be saved)
  wasn't tried again until it changed or the daemon restarted; it is now tried again 10 minutes later. A failed
  pin of an unchanged entry no longer ends the scan.
- An interrupted write of a name key could leave a `names/.partial-*` file holding the private key; these are now
  deleted at start, as the older `.tmp` leftovers were.
- Names: a publish racing `name rm` and `name create` of the same alias could save the record under the old
  name (it now fails); `name create` or `name import` over an alias whose key file is a dangling symlink
  overwrote that alias's saved record (now refused as an existing alias); a failed save while applying a heard
  record skipped waking resolves, telling keepers and watchers, and was never retried (it is now logged and
  saved by the next save); a name key deleted by hand is released once at startup, with one warning instead of
  two; a name resolved for the user no longer keeps a stale floor entry; `name create` no longer fails if the
  name is removed at that moment.
- Private mode: the routing table asked RNS whether a newly heard node is a member while holding the table's lock,
  so a slow answer (from `rnsd`, through a shared instance) held up `/find` replies and lookups; and a node banned
  while it was being asked about could still be added. It is now asked without the lock, and the table checked
  again after.
- The local API's own HTTP error pages (400 for HTTP/0.9 or a malformed request, 414, 431, 501) now carry
  `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`, like every other API response.

Faster:
- `add`, the shared folder and `import` write blocks on up to 16 threads, so the sync to disk each block needs
  before its rename overlaps (about 11x on an SSD: 64 MB added at 165–190 MB/s, from 13–17). Every block is
  still synced before it is named, and before anything pins it.
- A CID's written form and DHT key are computed once. They showed up in every save of the pins, pin names and
  publish state, and in `pin ls`.
- The targets fetched for the user (for `roots`) are saved with the cache records (every 30 s, at `gc` and at
  stop), not rewritten and synced under the node's lock on each fetch. Jobs are saved once per change, not twice.
- The publish state, provider records and name floor are written outside the publisher, provider-store and
  names locks, from a copy taken under them, so `/find`, `/provide`, resolves and name announces don't wait for
  the write. `verify` and the size queries (`stat`, quotas, pinned bytes) read DAG
  nodes without the storage lock, as eviction and `gc` already did.
- Recomputing what is provided reuses the last result when no provided DAG changed (about 30 ms to 5 ms at
  5000 pins).
- The DHT routing table finds the closest nodes by walking buckets, not by sorting every node (about 350 µs to
  4 µs per `/find` on a full table). Lookups start from nodes that haven't failed or gone silent, with the
  others only to make up K, as `/find` replies do.
- Fetches: a slow provider is paced by how long its response took from when the request was sent, not counting
  opening the link or waiting for the link's `/blocks` slot. A request reserves only what one response can carry,
  sized to the open link's packet size, so idle providers get the rest; and the batch is sized in one pass.
- Serving `/blocks` no longer reads and hashes the block it then leaves out for the budget; finished responses
  are noticed at once rather than within 10 ms; the response monitor sleeps while nothing is being sent.
- DAG nodes decode about 15% faster (struct formats compiled once), accepting and rejecting exactly as before.
- A DAG node decoded recently isn't decoded again (the last 256 nodes, at most 2 MiB of blocks, are remembered):
  a read met the same file and directory nodes many times. `GET /api/cat` also resolves its target and reads in
  one fetch, where it fetched the target's info first, then read it in a second. Reading a stored 8 MiB file
  through `rnfs.Client`'s `open()` (one request per 64 KiB) takes 0.18 s, from 1.0 s; a 4 KiB `cat` 1.3 ms, from
  8 ms.
- Republishing runs up to 4 closest-nodes lookups at once instead of one after another, so a round of many due
  keys takes about a quarter of the time (200 keys whose lookups take 0.2 s each: 10 s, from 40). `/provide`
  requests are batched per node as before and the same messages are sent. Slow nodes keep their pacing: one
  lookup at a time, at most 10 per round, 30 s per lookup.
- Working out what the node provides, done after every pin, fetch, eviction, name change or deny-list change, is
  skipped when nothing it depends on changed: 1 µs instead of 7 ms with 10 000 pins. Withdrawals are still sent
  at once.

## 0.1.4

Security release from the fifth audit: fixes for denial of service against `/blocks` serving, unbounded downloads,
DHT keeper and provider-store takeover, name rollback, gateway privacy leaks, the companion pages, LXMF floods and
Windows permissions. No wire-format changes: 0.1.4 and 0.1.3 nodes work together.

Fixes:
- A fetch of a whole DAG (`get`, `pin add`, `export`, `dir set`/`dir rm`, a followed name's new target, gateway
  archives, remote pins and LXMF auto-fetches) is refused with "too large" once the DAG's blocks hold more than
  its declared size can: the size plus 64 bytes per 4 KiB, 1 KiB, and what its directory nodes take plus 16
  bytes per entry (index nodes: their own bytes plus 16 per child, less 64). A file node may list empty
  children, so a file declaring 1 byte could make `get` or `pin add` download and keep any amount of data.
  Content built by `add` (either chunker) stays far inside the bound, and fetches of it ask for blocks as
  before; there is no limit on the entries of a directory you pin (`get` and `export` still stop at
  `max_output_entries`). Requests under the bound never ask for more than it leaves, so a refused fetch
  downloads at most about one block too many.
- A byte-range read (`cat`, the gateway's file responses and Range requests, `/api/cat`, and reading or
  decrypting a sealed file, as `get`, `cat` and `info` do for one sealed to this node) is refused with "too
  large" once the file nodes it meets hold more than the range can need: its own bound as above plus two full
  file nodes (13 KB each) per level a file of the declared size can have (one up to 1 MiB, seven at most).
  These reads never ran the whole-DAG bound, so `cat` of a file declaring 1 byte could download 7 MB, and
  `get` of a 12 KB file sealed to you 29 MB; now at most about 184 KB plus one block. A small read that ends
  inside two file nodes may take one more request than before.
- `ls` and the gateway's directory listings are bounded the same way, as a fetch of 0 bytes: chains of index
  nodes of one child each used to be listed level by level without limit (751 KB for 40 entries).
- `get` and `export` refuse to write when they can't read the free space at the destination (a missing directory, say),
  instead of writing without the disk reserve check.
- `get` and `export` refuse output that would leave less than the disk reserve (max(256 MiB, 2%)) free, and
  `get` counts the blocks it still has to fetch when they go to the same disk. They used to check only that
  the output fits, so a `get` could fill the disk.
- Names given as text must be exactly 32 hex digits (optionally in `<angle brackets>`, with spaces around):
  whitespace between the digits used to be skipped, giving a name shorter than 16 bytes. Node IDs and names
  given to the local API and the gateway are checked the same way (spaces around allowed, not between).
- A remote `/pin` accepted just as the previous one of that identity finished was answered "accepted" and
  never fetched (sending it again didn't help): the fetch thread now deregisters in the same step that finds
  nothing left. If the thread can't be started the `/pin` is refused, where the identity used to be stuck
  until a restart, its pins queuing up and then being refused.
- Remote pins accepted but not yet fetched survive a restart: they are saved in `state/remote_pins.json` and
  fetched again at start, unless their identity has left `remote_pin_identities`. They used to be dropped
  silently. An entry of the file that isn't a pair of strings is skipped with a warning; it used to stop the
  node from starting.

Local API and privacy:
- `GET /api/status` without the API token answers only `{"version": ...}`, enough for health checks; the node
  ID, identity hash, LXMF address and traffic counters need the token, as `/api/lxmf/address` already did.
  `rnfs status` sends the token (without a readable one it prints just the version and says why).
- `pin ls` says which pins are sealed files: `(sealed)` in the listing and `"sealed": true` in `--json`
  (`false` for other files and directories, `null` when the stored blocks can't tell). The local name of a
  sealed add is still the path or name you gave, kept on your machine only; anything that shows pins to
  others must leave sealed entries out.
- The README and specification now say plainly that under the default `provide_strategy = "roots"` every
  complete download, `cat` and `stat` of other nodes' content included, is provided and served, so anyone
  can learn what the node fetched, and that `"pinned"` avoids it; and that scripts showing content to others
  must use `cat --raw`, since `cat` decrypts sealed files this node can open. Behaviour is unchanged.

Platform and packaging:
- Where directories are walked by path (Windows), adding a directory or scanning the shared folder checks
  each directory again whenever it lists it (same file ID, not a junction, symlink or other name-surrogate
  reparse point, real path still inside the folder) and each file's final path, from its open handle, before
  reading it. A directory swapped for a junction during a scan used to import files from outside the shared
  folder and share them; now what changed is skipped with a warning. The README says the shared folder must
  not be writable by accounts you don't trust. Not yet tested on Windows itself.
- On Windows the rnfs dir, the API token, the node and LXMF identities and name keys get a protected access
  list allowing only the user and SYSTEM; they used to inherit their folder's, which with `--dir` outside the
  user profile (another drive, `C:\ProgramData`) usually let every local user read the token and the keys.
  An existing rnfs dir or identity file that lets others in is tightened (with a notice), the daemon refuses
  an rnfs dir owned by another account, and every command refuses a token file others can read, as on POSIX.
  A `--dir` outside `%USERPROFILE%` gets a warning. Not yet tested on Windows itself.
- On Windows a new rnfs dir and its subdirectories get that access list in the call that creates them. A new
  `--dir` used to have its parent's access list until rnfs set its own, so another local account could put a
  file in it first (an `rnfs.toml` naming a Reticulum config of theirs, whose interfaces can run programs, or
  an identity); a new rnfs dir that already holds anything is now refused. `rnfs.toml` and the identity are
  checked before use, through the handle they are read from: one owned by another account is refused, one
  others may access is made private with a warning, and one that can't be made private stops rnfs (for the
  identity this was a warning). An rnfs dir that is a junction or symlink is refused. Not yet tested on
  Windows itself.
- The shared folder skips files that have other hard links, with a line in the log, also inside shared
  directories: someone who may write to `files/` could hard link a file of yours from elsewhere on the same
  disk into it and have it shared. A file is imported once its other links are gone. `add` is unchanged.
- `install.sh` installs the latest release from its sdist (`rnfs-X.Y.Z.tar.gz`, a release file) instead of
  GitHub's archive of the tag, which neither `SHA256SUMS` nor the build provenance covers and which a moved tag
  would change unnoticed. The sdist must match the release's `SHA256SUMS`; where the GitHub CLI (2.49 or
  newer) is installed and logged in, its signed provenance is checked too (`gh attestation verify`, signed by
  the release workflow), and a mismatch or failed check stops the install. Without `gh` it warns that only
  the checksum was checked; `RNFS_REQUIRE_ATTESTATION=1` makes that an error. The README's "Checking a
  download" shows `gh attestation verify`.
- The release workflow's Debian 12 smoke test mounts the checkout read-only (it installs a copy of the
  `.deb`), runs an image pinned by digest with `no-new-privileges`, and runs the smoke test as `nobody`. The
  container could change the `.deb` just before it was uploaded, checksummed and attested.
- The release job runs in a `release` environment, so publishing can be gated by that environment's
  protection rules (set in the repository's settings).
- The systemd user unit (`contrib/rnfs.service` and the one `install.sh` writes) adds
  `SystemCallFilter=@system-service` and `SystemCallArchitectures=native`.
- The identity file's permission fix no longer follows a symlink, and the daemon refuses to start, with an
  error, if the identity file is a symlink, belongs to another user or isn't a regular file. A symlink used
  to be loaded with a warning, and a FIFO there made the start hang. The file is checked, tightened and read
  through one descriptor opened with `O_NOFOLLOW`, so a file swapped in after the check isn't the one loaded.

Companion NomadNet pages:
- `display-mu-file` shows only allowlisted CIDs when the node runs in private mode: it used to show any
  unsealed file the operator pinned (up to 200 KB) to every visitor who knew its CID. Its rate limit is now per
  visitor where NomadNet says who that is: 10 requests a minute for each visitor who identified, 30 for all who
  didn't together, and 60 for everyone. One visitor's requests used to lock everyone out for a minute.
  Identities cost nothing to make, so someone who makes several can still reach the overall limit.

Gateway privacy:
- Errors from the node other than not-found (`400`, `413`, `422`, `503`, `507`, as well as `500`) get a fixed
  message for their status; the detail goes to the daemon's log. A `507` used to show the local blocks
  directory, the free disk space and the reserve. The storage-full message no longer names the directory
  anywhere (the CLI and API still show free space against the reserve).
- The gateway sends no CORS headers at all. The rule that allowed pages to `fetch()` files in their own DAG
  (`Access-Control-Allow-Origin` for a gateway `Referer`) never worked in browsers, since sandboxed pages
  send no `Referer`, and it is dropped from the specification. Pages still load their DAG's files as
  images, scripts, style sheets and frames. Per-CID origins may bring reading back later.
- `?format=archive` answers every web page's request and every `HEAD` with `409`, stored or not. It used to
  answer `200` for content stored on the node, which let any web site tell what the user had downloaded or
  viewed (an iframe of a download fires no load event). The user's own `GET` is unchanged.
- `?format=raw` answers every web page's request and every `HEAD` with `409` too, stored or not, and so never
  takes a `gateway_fetches` slot. A page used to get the block, a download, if it was stored, but a network
  fetch otherwise (an error page if nobody had it, or `503` once the page held every slot), which told it the
  same. The user's own `GET` is unchanged.
- A web page's `HEAD`, or range not starting at byte 0, takes its Content-Type from the file's first bytes,
  fetched if need be, as its `GET` does. It used to sniff them only if stored (else
  `application/octet-stream`), which a page may be able to observe (e.g. through opaque-response blocking).

Serving `/blocks` under load:
- Displacement protection is now earned by progress, so one peer can no longer hold every `/blocks` response
  slot almost for free. A response is protected from displacement only while it is within a short grace the
  node measures from the path its request came over (never from the requester-controlled `link.rtt`), or while
  it is moving data at or above a throughput floor. The grace prices the first hop at the receiving
  interface's bitrate and further hops as slow ones unless RNS raised the link's MTU (so a requester behind a
  LoRa segment gets enough time however fast the node's own interface is), and on the node's own slow radio it
  also covers parts queued there for other responses. Attached to a shared instance, where every link arrives
  on the local interface to it, the bitrate is taken as unknown, so LoRa neighbours aren't held to a 1 Gbit/s
  interface's grace and floor. A response that transfers nothing is cancelled and its upload-budget charge
  refunded once its grace passes, and any response with no progress for `stall_timeout` is cancelled too.
- A link (and its identity, if it identified) whose response was displaced or cancelled for making no progress
  is refused new `/blocks` for `blocks_cooldown` (new setting, default 300 s). So is one whose response was
  displaced as the slowest while far behind the rate it needs to be done in time, and a requester whose link
  or identity already holds a slot can't displace that way, so taking a little of each response no longer
  keeps one's slots young. A newcomer on an interface that holds no slot in a full pool may displace a
  zero-progress response from an interface holding two or more, once that response is past half its grace,
  at most once per grace per interface.
- Links that aren't slow get 16 `/blocks` slots (new setting `fast_blocks_responses`; it used to be 2, the
  other half of `max_blocks_responses`), so holding them takes many links each actually downloading. Slow
  links keep their 2. At most about 40 MiB is held for responses in flight on fast links.
- A `/blocks` request that repeats a CID is refused as malformed, so a node can't be made to read and serve
  the same block several times in one response.
- On a link with no rate estimate a `/blocks` response holds at most `CHUNK_SIZE` of blocks after the first
  (which is always served): what requesters already ask for there. It used to be whatever `max_bytes` the
  requester sent, so one packet could ask for a ~1 MiB response with a deadline of many hours.
- `upload_budget` is charged as a response is sent, from its Resource's progress, instead of its full size up
  front with a refund at the end. A response is charged about one packet when it starts, and the rest of it is
  set aside only while it is within its grace, then only what it sends at its rate before its deadline, and
  nothing below its floor, so responses a requester never downloads or crawls through (each unidentified link
  counting as its own requester) no longer hold the budget. A completed response costs its
  size and a cancelled one what it sent, as before. The half-per-requester rule now caps what one requester's
  responses in flight may cost.
- Link requests that never complete their handshake are counted in a bucket of their own, of
  `max_incoming_links`, on `rnfs.dht` and the LXMF delivery destination. When it is full the oldest one on the
  busiest interface is closed. RNS keeps such links (each with a thread) for at least 366 s and rnfs only saw
  links once established, so a flood of link requests could exhaust the node's memory or threads. A flood on
  one interface still shortens the time an honest handshake over that interface has to complete.
- A link (and its identity) that rejects a `/blocks` response's advertisement, so that none of it is sent,
  cools down for `blocks_cooldown` as after a response that made no progress. A rejected Resource frees its
  slot at once, so a requester could have the node build a response, reject it and ask again in a loop (a
  response of up to 128 KiB on a link with no rate estimate, about 1 ms of CPU each); now it gets one per
  cooldown. No honest requester rejects a response.
- Each incoming link may send 2000 requests at once and 2000 a second after that. Requests over the limit are
  refused before anything is parsed, and after 4 refusals (one more a second) dropped unanswered. RNS has
  already decoded a request and started a thread for it by then, so this only removes rnfs's share of a
  flood's cost (about 40% of the node's CPU per request, measured); honest requesters, which wait for their
  answers, stay far below it.
- Known limit: over a shared interface, many unidentified links that download slowly but on schedule, or a
  steady stream of fresh links, can still keep the fast pool busy (specification section 10).

Fetching from other nodes:
- On `rnfs.dht` links a response's msgpack headers are checked before RNS decodes it, and one that can't be
  an rnfs response is refused: the request fails as for a malformed response. RNS decoded every response
  whole first, and a response of one-byte empty arrays or maps cost about 70 times its size in memory (a
  1 MiB response: ~70 MB and ~0.3 s of CPU). The check reads only headers, so it costs the same for any size.
  Response packets are checked when larger than RNS's default MTU (links that negotiated a larger one), and a
  response packet arriving on an incoming `rnfs.dht` link, which has no requests, is dropped unread. Resources
  carrying metadata, which RNS also decodes first and rnfs never sends, are refused on `rnfs.dht` links.
  Other destinations (LXMF) are unaffected.

DHT and names:
- A full provider store keeps a tenth of its room as a newcomer pool, with random eviction inside it, so a
  publisher it hasn't seen can still store records; about 100 identities filling the store used to lock out
  every new publisher. Publishers seen before still displace the newest one outside the pool, and the node's
  own records are always accepted (they were refused once the store was full). A publisher's share of the
  store grows with age: a tenth of it until it has been seen for `provider_seniority`. First-seen times are
  kept for a week after a publisher's last record goes, so one that was away for a while isn't new again.
- Resolving through keepers no longer asks only the first `alpha` keepers found, oldest record first: a few
  identities that stored provider records for a name's key before its real keepers (and, on a slow node, the
  departed owner's own record) could hide every keeper. Keepers recorded locally no longer skip the DHT search,
  which now looks for 3 × `alpha` keepers listed by more than one node; they are ranked by routing-table
  seniority (live servers first, then those that failed or went silent, then unknown IDs), at random within
  each group, and asked a batch at a time. Another batch is asked while no answer reaches the sequence already
  known or only one keeper answered, at most 3 batches (2 on slow nodes, where a second `/name` is spent only
  if there's no known sequence to check the first answer against).
- Keeper queries can no longer be steered by identities that only answer `/find`. Keepers are ranked on the
  routing table as it stood when the query started, so nodes the lookup meets don't rank above keepers recorded
  here; the search ends early only once two established servers (in the table for `provider_seniority` and
  healthy) listed keepers, and records stored here count as a source only if an established publisher stored
  one; each batch first takes keepers listed by a source no earlier pick was listed by; and when the node knows
  no sequence for the name, only answers from established keepers settle it. Unknown keepers get a budget of
  `alpha` path requests of their own, which `/find` replies naming unknown nodes can no longer use up. On a slow
  node a query is still at most two `/name` requests; a fast node may ask up to 9 keepers, and its lookup may
  run to completion, where established servers are scarce (a young network).
- A full newcomer pool in the provider store evicts from the publisher holding the most pooled records instead
  of at random, so a flood of new identities leaves an honest newcomer about a fair share of the pool (16 of its
  100 records against 600 identities, where it used to keep 1). A newcomer whose last record the pool evicted
  keeps its first-seen time (at most `max_records / 100` of them), so it still ages into a full share.
- Gateway and LXMF auto-fetch name resolves can no longer be rolled back by flooding announces of new names
  and replaying an old one: an announce of a name the node isn't resolving or keeping only takes the table
  place of another name that was only heard, and the highest sequence of each name resolved this way is saved
  in `state/name-floor.json` (up to 4 × `name_table_size` names), so no older record is accepted after table
  churn or a restart.
- The name floor is saved by the resolve that raises it (at most every 5 s), where a crash up to a minute after a
  resolve used to lose it, and names resolved in the last 7 days are kept past 4 × `name_table_size`, up to
  8 ×, so that many resolves of other names no longer let a replayed old record of a recent one back in.
- The specification no longer claims that a node's names can't be linked to it or to each other: the node
  keeps its own names and provides their targets under its node ID, so its names are linked to it, as
  providing content reveals the provider in IPFS. Behaviour is unchanged.
- A `/provide` carrying more than 55 keys (what fits `MAX_REQUEST_SIZE`) is refused as malformed. rnfs's own
  client never sends more.
- A `/provide` from a publisher already holding its share of the provider store is refused cheaply: its
  expired records are looked for at most once per batch, and not at all while none can have expired. Each
  refused key used to rescan all of the publisher's records (about 5 ms of work per request under the
  store's lock at the default size).
- Private mode drops the provider records of non-members when the node starts (those stored while it was
  public, or by a member since removed from `members`), and `/find` replies leave out any non-member's
  record; such records used to be handed to members for up to a week. A private node also links only to
  members, so it never identifies itself to an outsider.

Publishing, names and storage:
- A CID unpinned and pinned again while a publish round was sending (or evicted and fetched again) could be
  withdrawn by the next round from the nodes that had just stored it, and the node's own record of it was
  gone too, until its next republish. Providing a key again now cancels its queued withdrawals and records
  the node itself again, and a round sends only the withdrawals still queued.
- Where a publish round stored each key is saved as the round goes (every 10 nodes or 30 s), so records stored
  by a round cut short by a crash are withdrawn later; they used to stay until they expired.
- `name publish` is crash-consistent: it pins the new target, saves `names.json`, then releases the old
  target, and a failed save (a full disk) undoes the publish. A crash or failed save used to leave the name
  recorded at a target no longer pinned (which `gc` deleted) and the new one pinned by `name:<alias>`, which
  `pin rm` refused and `name rm` didn't release. At start the node now makes `name:` pins match `names.json`,
  releases `follow:` pins of names no longer followed and `lxmf:` pins no saved inbox message records, which
  a crash inside `name unfollow` or before the inbox was saved used to leave for good. Those two are released
  only if `follow.json` or the inbox file was read whole: when it is missing, damaged (moved aside) or has a
  malformed entry, the pins are kept and recorded again, the `follow:` pin's name being followed again and a
  sender's `lxmf:` pins listed in a new inbox message, whose removal releases them. A followed name's check
  releases every other target its pin source holds. A pin or unpin whose `pins.json` save fails no longer
  stays changed in memory.
- A name key file that is there but can't be read at start (no permission, damaged) no longer counts as a
  deleted key: the name's pins and its `names.json` entry are kept, so it is back as it was once the file can
  be read. Its pins used to be released and its entry dropped, leaving the name at sequence 0 with no target.
- Eviction and `gc` read the DAG nodes they walk before taking the storage lock, so serving `/blocks` no longer
  waits while the first eviction after a start reads every stored DAG node (about 0.4 s with 20 000 blocks);
  what to delete is still decided under the lock, so nothing that became reachable meanwhile is deleted.

LXMF:
- Senders in `lxmf_accept_from` or `lxmf_auto_fetch_from` and LXMF contacts are trusted. Their inbox messages
  are dropped only to make room for each other's, after every other sender's message without pins. With
  `lxmf_accept_from` empty (the default), about ten new LXMF identities, which cost nothing, could push every
  message not holding pins out of the inbox, including contacts' messages and auto-fetches that had failed.
- New inbox messages from senders not trusted are rate limited node-wide: 32 at once, then one a minute. The
  README now recommends setting `lxmf_accept_from`.
- Each sender not trusted also has an allowance of its own, 4 new inbox messages at once, then one per 15
  minutes, spent before the node-wide limit, so one sender writing once a minute can no longer keep every
  other stranger's messages out.
- A message from a sender whose identity isn't known yet, arriving when 8 held messages already claim that
  sender, now takes the place of the oldest of them instead of being dropped, so forgeries sent first can't
  keep the sender's real message out.
- Messages from senders whose identity isn't known yet: at most 8 of the 32 held may claim one sender, a held
  message is dropped 30 s after an unanswered path request instead of waiting 2 minutes, a message claiming
  a trusted sender takes the place of one held for an unknown sender, and trusted senders get path requests
  first. 32 forged messages used to fill the queue, so a real first message from a new sender was dropped.
  LXMF has already told the sender such a message was delivered, so it can still be lost; trusted senders
  are now protected.
- Auto-fetches take senders in turn, and a sender with few links waiting gets a place in a full queue from
  the sender with the most. One sender's backlog used to delay every other sender's auto-fetches, and once
  it filled the 64-link queue their links were skipped.

## 0.1.3

Joining the DHT:
- Built-in bootstrap nodes (`DEFAULT_BOOTSTRAP_NODES`): every node contacts them when joining, after its own
  `bootstrap_nodes`, so a new node gets in even where no announces reach it (e.g. behind a LoRa access
  point). `default_bootstrap = false` turns them off; private mode never uses them.
- Bootstrap nodes are asked one at a time until two answer, outside the lookup's budget for unknown node IDs:
  a slow node used to try only one of them, and gave up if that one was down.
- The node joins again while it knows fewer than 3 live DHT servers, 5 minutes after the last join and
  doubling up to `announce_interval`. It used to join only at startup.

## 0.1.2

Maintenance release from the fourth audit: fixes, release guardrails and internal consolidation. No format,
protocol, CLI or config changes.

Fixes:
- `POST /api/add` refuses a `seal_name` that isn't `true` or `false` (a string like `"false"` used to seal the
  name anyway). `/api/add/stream`, where `seal_name` is the name, is unchanged.
- `names.json`, name keys and the API token are written durably (file and directory synced, unique temp
  file), like the other state files.
- The README's Tests and Known limitations sections are restored (0.1.1 shipped them inside a merge
  conflict), and its download links point at the current release.

Dependencies and install:
- rns is limited to <1.6 and lxmf to <1.3; a test checks every RNS/LXMF internal rnfs relies on, and the
  daemon logs an error if incoming Resource advertisements no longer look as rnfs expects.
- install.sh installs the latest release instead of the main branch (`--main` for the old behaviour).
- requirements.txt is gone: install from source with `pip install -e .`, or hash-checked from
  packaging/requirements.lock.
- Python 3.14 is tested in CI and covered by the build locks.

Development:
- CI lints with ruff and rejects merge-conflict markers; releases run the full test matrix before
  publishing.
- Shared test fakes live in tests/support.py; a contract test keeps the API tests' fake node in step with
  RnfsNode.

## 0.1.1

Fixes from the third audit.

### Security fixes

Changed behaviour:
- Settings are type- and range-checked: `private = "no"` or `gateway_scripts = "false"` are errors (they
  used to mean on), as are counts given as strings, negative counts and out-of-range ports. Invalid settings
  stop the daemon and client commands with a message, not a traceback.
- The daemon refuses to start if its rnfs dir belongs to another user or can't be made 0700 (a warning
  before), and if a state file can't be read (permissions, I/O errors) instead of setting it aside and
  starting without it. Unparsable or wrongly shaped state files are still set aside, as `.damaged`,
  `.damaged.1`, … so earlier copies are kept.
- The CLI makes the daemon prove it knows the API token (`X-Rnfs-Challenge` / `X-Rnfs-Proof`) before sending
  it, so a program listening on the API port while the daemon is down gets neither the token nor an imported
  name key. Restart a running daemon after upgrading.

Network and names:
- On slow links, single-provider downloads no longer fail because of the responder's upload pacing: the
  requester waits as long again as a `/blocks` took before asking again, and a refusal soon after a served
  response isn't counted as a failure. The responder's pacing wait no longer depends on the link's RTT
  (which the requester can inflate) and ends when the link closes or the response is displaced.
- In private mode, outsiders' incoming links are evicted before members'. Outgoing links check that the
  destination hash is the node ID asked for.
- A key full of publishers evicts a random non-senior one instead of refusing newcomers, so early identities
  can't lock later providers out. A node's own records aren't limited per publisher, and keys of partly
  recorded `/provide` batches are withdrawn later. Withdrawals also go to earlier rounds' nodes; a stopped
  publish round no longer marks unsent CIDs as published; withdrawals queued during lookups go in the same
  round.
- The first publish of an imported key, or of one found without state, asks the keepers for the highest
  sequence. A keeper's record below the sequence the node knows isn't kept. Restarting after deleting a name
  key file works.
- A source's remote pins are fetched one at a time (at most 64 waiting or in progress), so they can't exceed its quota.
- Wall-clock times in the future (e.g. after a clock that ran ahead is corrected) are clamped.
- Cancelled or failed fetches close the links of their in-flight `/blocks`.

Storage:
- Archive import verifies every block, including ones already stored (junk could pin a cached download), and
  keeps the disk reserve free.
- Blocks and state files are fsynced, and block names are synced before pins and cache records are saved.
- A stored DAG node that no longer matches its CID stops collection below it (nothing is deleted until
  `rnfs verify` removes it), and a damaged stored block is fetched again instead of marking the root invalid.
- `get` skips and reports names the destination filesystem refuses (FAT, eCryptfs) instead of failing.
  Export holds much less memory per block. Default output names never start with `-`; output renames never
  replace a file created meanwhile.

LXMF and features:
- Auto-fetches still queued when the node stops are queued again at the next start; removing the message or
  stopping the node cancels an auto-fetch in progress.
- With `lxmf_accept_from` set, messages from unknown senders outside it and `lxmf_auto_fetch_from` aren't
  held. Messages arriving while held ones are retried count against the limit.
- Links read from message text: a link on a line of its own is read whole (a last name ending in `.` was
  cut), links past the 4000 characters kept are found, and query values may contain `=`.
- `inbox get` checks every output before starting any, and reports later failures per link.
- Follow keeps checking a name after an unexpected error. Inbox, outbox and provider records are saved under
  a lock, so an older snapshot can't be written last.

Local API and gateway:
- Every field the CLI and feature commands print is escaped.
- The gateway ignores malformed `Range` headers, puts no error details in 500 bodies, doesn't log aborted
  archive downloads as errors, and watches for disconnects with `poll` (fds over 1024). Both servers refuse
  HTTP/0.9 with the usual headers; chunked request bodies need plain hex sizes and at most 32 trailer lines.
- The node identity file is created without racing a second instance.

### Packaging and CI hardening

- The build backend is locked: `packaging/build-backend.lock` pins setuptools with hashes, and `install.sh`,
  `build-deb.sh` and the workflows build rnfs with it without pip's build isolation (which fetched the
  newest setuptools from PyPI, as root with `install.sh --system`). `pyproject.toml` bounds setuptools
  (`<85`), and the sdist includes the backend lock.
- The release toolchain is locked: `packaging/build-tools.lock` (build, twine, PyInstaller, setuptools and
  their dependencies, for Linux and Windows) is installed with `--require-hashes`; `update-lock.py --build`
  regenerates both build locks. The tests install the hash-locked dependencies too (they used `pip install .`).
- `install.sh` installs dependencies as wheels only (`RNFS_ALLOW_SOURCE_BUILDS=1` builds them from their
  hash-checked source where PyPI has no wheel, e.g. cffi on 32-bit ARM), runs `pip check`, uses umask 022
  and no pip cache in system mode, never replaces or removes a systemd unit it didn't write (`--force`
  replaces one), tells system-mode users to restart running daemons after an update, and on a system-mode
  uninstall removes users' `default.target.wants` links to the unit.
- `build-deb.sh` fails instead of silently leaving out a Python version's cffi backend (a .deb without it
  can't import Reticulum's crypto), checks every bundled backend is present, limits `Depends` to the
  bundled Pythons (`python3 (<< 3.15)`), and writes `DEBIAN/md5sums` and a DEP-5 copyright file.
- The systemd units get `NoNewPrivileges`, `RestrictAddressFamilies` (Unix, IP and netlink sockets),
  `LockPersonality`, `RestrictRealtime` and `RestrictSUIDSGID`, and stop retrying after 5 failed starts in
  10 minutes; the launchd agent doesn't restart when `rnfs` is missing.
- Windows task script: stopping or uninstalling now also ends the daemon (not just the hidden PowerShell),
  a crashed daemon is restarted (Task Scheduler's restart settings never did that), the log is written as
  UTF-8 bytes rather than UTF-16 with wrapped stderr, and paths with `[` `]` work. New `-Stop` switch.
- Release workflow: a manual run never publishes, even on a tag; files already attached to a release are
  never replaced (no `--clobber`); every file gets a signed build provenance attestation
  (`gh attestation verify`). Checkouts don't keep the token (`persist-credentials: false`).
- README: the Python badge says 3.11+; the Windows PATH snippet adds the folder once and keeps the PATH
  value's registry type.

## 0.1.0

First release.

### Core

- `pin ls` shows where each pin came from (the local path it was added from, the shared-folder file name, or
  what a pinned directory holds), recorded locally in `state/pin_names.json`; `--json` prints everything.
- Downloads are re-shared by default: `provide_strategy` defaults to `roots`, so complete downloads are
  provided while they stay in the cache (up to `max_storage`). Set `pinned` to share only what you add or pin.
- Content IDs: raw blocks and the rnfs-dag codec, SHA-256, base32 (specification section 4).
- Files and directories: 64 KiB chunks in balanced trees, sorted directory indexes and strict DAG decoding
  (section 5).
- A Kademlia provider DHT over Reticulum, with server and client modes, seniority-based Sybil resistance,
  batched publishing and withdrawal (section 6).
- `/blocks` transfers: multi-source, resumable and verified, sized to each link's measured rate, with stall
  detection (section 7).
- Storage: pins with sources, an LRU cache, GC, a shared folder, archives (`export`/`import`) and remote
  pinning (section 8).
- Names: self-certifying mutable pointers carried in signed Reticulum announces, with persisted sequences
  against rollback (section 9).
- Private mode for closed groups (section 3).
- A local API and a sandboxed HTTP gateway (section 11).

### Features

- **Background jobs.** `get`, `pin add` and `export` now run as daemon-owned jobs with a progress bar
  (rate, ETA). `--background` returns at once; `rnfs jobs list|show|wait|cancel|clear` manage jobs. Jobs
  survive restarts without fetching stored blocks again. New settings: `max_jobs`, `max_finished_jobs`.
- **Mesh-aware transfers.**
  - Providers are ranked by seniority, then hops, then next-hop interface bitrate.
  - New airtime controls for uploads: `upload_budget` (bytes per hour), `serve_interfaces` /
    `no_serve_interfaces`, and `quiet_hours`.
  - New commands `rnfs peers` and `rnfs stats`, and traffic counters in `rnfs status`.
- **Keepers.** `rnfs name keep|unkeep|kept`: any node can store and serve a name's signed announce over the
  new `/name` request, so names outlive their owner's announces. Your own names are always kept. New
  settings: `keep_max_names` (default 100), `keeper_query`.
- **Follow.** `rnfs name follow|unfollow|following` keeps a name's latest target pinned (pin source
  `follow:<name>`).
- **Contacts.** `rnfs contacts add|rm|ls`: nicknames for names, nodes and LXMF addresses, usable in targets
  (`alice/docs/report.pdf`) and as node IDs.
- **Content.**
  - `add --no-pin` adds as evictable cache.
  - **Sealed files:** `add --to <recipients> [--to-self] [--no-sealed-name]` encrypts a file for chosen RNS
    identities. `get` and `cat` decrypt it for recipients, and `--raw` gives the stored bytes.
  - **FastCDC chunking:** `add --chunker fastcdc`, fully specified, with a test vector.
  - `cat --block`.
  - Trustless gateway responses: `?format=raw` and `?format=archive`.
- **LXMF.**
  - `rnfs send`, `inbox` (`get`, `rm`), `outbox`, and `lxmf address|sync` share `rnfs:` links with any
    LXMF client: Sideband, NomadNet, MeshChat.
  - Auto-fetch from trusted senders (`lxmf_auto_fetch_from`, `lxmf_auto_fetch_quota`, pin source
    `lxmf:<address>`).
  - Propagation-node fallback and sync through a configured `lxmf_propagation_node`; `send --nearest` and
    `lxmf sync --nearest` use the nearest one heard. New setting `lxmf_send_from_hint`.
  - The `rnfs:` URI syntax (section 13).

### Security hardening

- The local API requires a per-start secret token (`<rnfs dir>/api_token`, mode 0600) on every request except
  `GET /api/status`. The rnfs directory and identity files are owner-only.
- The API and gateway time-limit request reads and cap concurrent connections (32 and 64).
- Requests to other nodes have bounded response sizes and deadlines. Empty or malformed `/blocks` answers
  count as failures. A process-wide guard rejects oversized Resource advertisements before allocation.
- Range reads walk files lazily with bounded prefetch. Zero-size DAG bombs count downloaded block bytes
  against `max_download_size` and remote-pin quotas. Listing stops at `max_output_entries`. A free-disk
  reserve is enforced, and destination space is checked before writing out.
- Provider records are capped per publisher (max(10, `max_records` / 100)) and per key (1000 publishers).
- Archive import pins only roots whose every block was in the archive, at most 1024 roots.
- The shared folder and `add` walk directories by file descriptor (`O_NOFOLLOW`, type and inode checks), so
  swapped symlinks, FIFOs and devices are skipped.
- Cache and invalid-root records are capped and saved in batches. Eviction runs one collection per pass.
- Sealed files are never decrypted by the gateway, and the gateway never uses contacts.

### Security hardening (second audit)

Changed defaults:
- `keep_max_names` is 100 (was 1000): every kept or followed name is a provider record this node
  republishes, and a pin if followed.
- `lxmf_display_name` defaults to `rnfs node` (was `rnfs ` + the first 8 hex digits of the node ID), so the
  LXMF announce no longer links the LXMF address to the node ID. New setting `lxmf_send_from_hint` (default
  `true`) controls the `from=<node ID>` hint in sent links, which still does.
- No propagation node is used automatically unless `lxmf_propagation_node` is set: neither for the periodic
  downloads nor for resending failed direct messages. `send --nearest` and `lxmf sync --nearest` use the
  nearest one heard; `send --propagate` without either fails with an explanation.
- `max_finished_jobs` must be at least 1 (was 0), and a finished job is kept for at least 60 s whatever it
  says. `max_jobs`, `max_finished_jobs`, `keep_max_names`, `lxmf_enabled` and `lxmf_send_from_hint` reject
  values of the wrong type (`true` is no longer a count, `0`/`1` are no longer booleans).

Network and names:
- `upload_budget`: a response that fails, is cancelled or displaced, never starts, or whose link closes
  gives back what it didn't send; one requester may hold at most half the bucket in responses in flight.
- Multi-segment Resource advertisements are refused on `rnfs.dht` links and on links to LXMF propagation
  nodes (which are also limited to 1 MB and 64 messages per download).
- Kept names' keys are republished every max(`republish_interval`, 3 days) with ±10% jitter per key, slow
  nodes pace their publishing lookups (at most 10 per round, 30 s each on average), and a restart no longer
  withdraws kept names' keys. `/provide` batches send withdrawals before stores, so a later batch can't
  delete what an earlier one stored.
- Kept records are a floor when resolving, so a replayed older announce of a kept name is never accepted,
  even after a restart. Kept names without a record are retried round robin.
- Followed names: periodic checks use the record held unless nothing was heard for 2 × `announce_interval`,
  and failed checks back off exponentially from 5 minutes up to `announce_interval`.
- A contact nick's resolved CID is reused for 60 s from the resolve, also when the record is old.

LXMF:
- The inbox keeps at most 500 messages, 4 MiB in all, 50 per sender not in `lxmf_auto_fetch_from` and 16 KiB
  per message; messages holding auto-fetched pins are never pushed out by others' messages, so a flood can't
  release a trusted sender's pins. Field links longer than 4096 characters as URIs are rejected.
- Messages from unknown senders: at most 32 held, further ones dropped; path requests for them at most one
  per sender per 2 minutes and 3 at once, then one per 10 s node-wide.
- `inbox get` always asks the link's `from` node first, then the DHT (get job option `fallback_dht`).
- Auto-fetched `name/` links no longer grow `names.json`. A pin-recording race that could leak an `lxmf:`
  pin is fixed. Malformed inbox and outbox records are dropped at startup; the files are written as UTF-8.

Local output and gateway:
- A default output name chosen by someone else (a DAG's last path name, a sealed name, an LXMF link name) is
  used only if it is printable, doesn't start with `.` and is one valid file name here; otherwise the CID.
  `rnfs get <cid>/.bash_login` now writes `./<cid>`.
- Every log line, and the paths and job results the CLI prints, escape non-printable characters.
- `get` (and get jobs) write under a hidden `.rnfs-get-….partial` / `.rnfs-job-<id>.partial` name beside the
  output and rename it when complete, so an interrupted write-out never leaves a partial output (which used
  to make a resumed job fail with "already exists").
- Get results keep the first 100 warnings plus `warnings_total`.
- New get-job option `sealed_name`: `get --from` without `-o` no longer does a DHT lookup first, and
  `get --background` returns at once. `GET /api/info` takes `from`.
- `rnfs status` shows `identity_hash`, the value other nodes list in `members` and `remote_pin_identities`.
- `rnfs name resolve` accepts a name contact's nick.
- Gateway `?format=archive`: a web page's request or a `HEAD` gets an archive only of content stored
  completely (409 otherwise); the user's own request's fetch is cancelled when the client disconnects. A
  `HEAD` or a range not starting at 0 no longer fetches a file's first chunk to recognise sealed files.
- Sealed files: a final segment of more than 65536 plaintext bytes is rejected; an invalid sealed name is
  ignored instead of making the file unreadable, and no partial output is left. Sealed get jobs report a
  total size.

Packaging:
- `lxmf` is a regular dependency: `pipx install` of rnfs includes LXMF (no more `pipx inject rnfs lxmf`).
- New hash-locked `packaging/requirements.lock` (regenerated by `packaging/update-lock.py`), used with
  `pip --require-hashes` by `install.sh`, the `.deb` build, the Windows exe and the tests; the Flatpak's
  wheels are generated from it, and release packages bundle rns 1.5.6. It covers the runtime dependencies
  only: the build backend and the release tools were locked later (in 0.1.1).
- `install.sh` runs Python with `-I` from `/` (a planted `venv.py` or `pip/` in the current directory was
  imported, as root with `--system`), installs the locked versions, no longer upgrades pip from PyPI, and
  quotes the systemd unit's `ExecStart` (homes with spaces or `%` work). `contrib/rnfs.service` is quoted too,
  and the Windows task script handles apostrophes in paths.
- CI: read-only token permissions for tests, actions pinned to commit SHAs, the Flatpak container pinned by
  digest, and workflow values passed to scripts only through `env:`.

### Packaging

- `install.sh` for Linux: a venv in `~/.local/share/rnfs/venv` (or `/opt/rnfs` with `--system`) with the
  dependency versions of `packaging/requirements.lock`, plus `--service` (systemd user unit), `--update` and
  `--uninstall`.
- `rns` and `lxmf` are both regular dependencies.
- `.deb` packages for amd64 and arm64, an x86_64 Flatpak (`io.github.afit21.rnfs`), a self-contained
  Windows x64 `rnfs.exe`, and a wheel and sdist. All are built and smoke-tested by
  `.github/workflows/release.yml` from a `vX.Y.Z` tag and published to GitHub Releases with `SHA256SUMS`.
- Background-service files: `contrib/rnfs.service` (systemd), `contrib/io.github.afit21.rnfs.plist`
  (launchd), `contrib/install-windows-task.ps1` (Task Scheduler).
- Windows support: no execute bit, retried file replaces, reserved-name handling and exclusive ports.
- CI runs the tests on Linux, macOS and Windows.
