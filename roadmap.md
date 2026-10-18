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

# 0.1.7

## Local API and library for companion apps (done)

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

# 0.1.8

## Faster block writes on SD cards (plan item B, then A2)
Sync blocks to disk at the durability point (before a pin or cache save) instead of once per block, with one
`syncfs()` on Linux; keep the per-block fsync on other platforms and on stacked filesystems (ecryptfs, overlayfs),
where `syncfs()` may write nothing back. A block left damaged by a power loss is found and deleted at the next
start (a `blocks/.dirty` marker). Measure on a Pi SD card first (`tests/bench_block_writes.py`); then move block
writes off the fetch thread (A2). Deferred from 0.1.6: see 0.1.6-plan.md, item B.

# Later

## Alternate Pin Sources

* List if a file was pinned by an application via an API. `pin ls` shows each pin's sources (`user`, the shared
  folder, a name, a deny list, ...), but not which application pinned it

## API versioning and reference

* The public library stays provisional until 0.2 (`rnfs.API_STATUS`)
* The local API's routes are listed in `rnfs/api.py`'s docstring (checked against the code by
  `tests/test_api.py`); a reference with request and response shapes is still to come

## Per-app API tokens

* Destination.hash(identity, "rnfs", "api")
* Whitelist and/or API key for auth
