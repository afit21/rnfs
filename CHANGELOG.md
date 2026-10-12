# Changelog

[specification.md](specification.md) is the authority on behaviour.

## Unreleased (0.1.2)

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
