# Changelog

[specification.md](specification.md) is the authority on behaviour.

## Unreleased

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
