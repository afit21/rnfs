# Changelog

[specification.md](specification.md) is the authority on behaviour.

## 0.2.0 (unreleased)

The local API and the library for companion apps. No wire-format or CID changes: these nodes, 0.1.6, 0.1.5, 0.1.4
and 0.1.3 nodes work together. The local API stays compatible: every route, field and status of 0.1.6 is
unchanged, and error bodies gain a field. The upgrade of an `<rnfs dir>` is automatic and one-way.

Upgrade notes:
- **State:** the state database gains two tables (`pin_times`, `pin_labels`); an older rnfs can't open it
  afterwards. Pins made before the upgrade have no `added` time.
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

Added, for apps:
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
- **Capabilities:** `GET /api/capabilities` lists the daemon's routes, features, event kinds, error codes and
  capabilities (`events`, `pin_details`, `cache`, `config`, `jobs_add`, `add_tar`, ...).
- **Events:** `GET /api/events?after=&kinds=&epoch=&wait=`, a long poll on the node's events (`rnfs/events.py`),
  replacing an app's polling of each list: `job` (every state change, and a running job's progress at most once
  a second), `pin` (added, removed, labelled), `cache` (added, removed), `inbox` (messages received, removed and
  auto-fetched; outbox messages queued and their delivery state), `name` (a newer record of a name the node holds
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
  `max_download_size`, `max_output_entries`, `max_upload_rate`, `remote_pin_quota`, `follow_quota`,
  `lxmf_auto_fetch_quota`, `max_jobs` (queued jobs start if the new limit allows), `max_finished_jobs`,
  `serve_interfaces`, `no_serve_interfaces`, `quiet_hours`, `keep_max_names`, `keeper_query`,
  `lxmf_accept_from`, `lxmf_auto_fetch_from`, `lxmf_send_from_hint` and `scan_interval`.
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
  `stats()`, `contacts.*`, `names.keep/unkeep/kept`, `names.follow/unfollow/following` and `lxmf.*` (address,
  send, inbox, get, remove, outbox, sync), with objects (`Peer`, `Stats`, `Contact`, `Kept`, `Followed`,
  `Message`, ...); `jobs.add` and `jobs.import_archive`; and the additions above (events, capabilities, pin
  labels, the cache, settings, streams). A Node raises what a Client gets back.
- **Per-call timeouts:** `with client.timeout(seconds):` bounds the calling thread's requests only, long polls
  fitting inside it (a Node accepts it and does nothing); `ApiConnection.request/call/get/post(timeout=)`.

Changed:
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

Faster:
- A start no longer lists every stored block before the API answers: their sizes are kept in the state database
  (`block_sizes`) and read from there after a clean stop. After a crash or power loss the start lists them all,
  as before, and writes the table again. 100 000 small blocks on a desktop NVMe disk (warm cache): 0.65 s to
  index them before, 0.19 s now after a clean stop, 0.98 s after an unclean one (1,024 directories listed and the
  table rewritten); moving them out of 0.1.6's flat layout at the first start took 5.9 s. A cold cache and a
  slow disk should gain more, as the table is one file read in order: Raspberry Pi SD-card numbers are still to
  be measured (`tests/bench_startup.py`, `--cold` for a cache dropped as after a reboot).

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
