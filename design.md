# rnfs implementation design

How the code is organised to implement [specification.md](specification.md). The spec is the
authority on behaviour; this file fixes module boundaries and the interfaces between them.

## Shared foundation

- `rnfs/constants.py` — protocol constants (spec section 12), request paths, aspects, DHT modes.
- `rnfs/config.py` — `Config`: every local setting (section 12), paths under `<rnfs dir>`, and the
  `effective_*` helpers for settings that depend on slow nodes or links. Loaded from `<rnfs dir>/rnfs.toml`
  plus command-line overrides; `validate()` checks every core setting's type and range (`WHOLE_NUMBERS`,
  `RATES`, `DURATIONS`, `FLAGS`, `IDENTITY_LISTS`), and the daemon exits on an invalid one. Reticulum is
  started from `reticulum_dir`, by default RNS's own config directory, so the node attaches to `rnsd` with the
  matching RPC key.
- `rnfs/errors.py` — `RnfsError` and subclasses `InvalidDag`, `NotFound`, `TooLarge`, `StorageFull`, `Busy`.
- `rnfs/content/cid.py` — `Cid` (binary form `.binary`, `.codec` `RAW`/`DAG`, `.key` DHT key, `.verify(block)`,
  `str()` written form, `Cid.parse`, `Cid.from_bytes`, `Cid.for_block`). Code passes `Cid` objects everywhere;
  strings only at the user edge (CLI, API, gateway, logs) and binary only on the wire.
- `rnfs/log.py` — logging through RNS's logger. `log()` escapes non-printable characters in every message
  (remote names can't inject terminal escapes or fake lines); `cap_warnings` keeps a result's first
  `MAX_WARNINGS` (100) warnings plus a count line and `warnings_total`.
- `rnfs/fsutil.py` — the one platform switch (`IS_WINDOWS`), `O_BINARY`, and `replace`/`remove` that retry for
  about a second on Windows while another handle holds the file; `rename_new` (rename onto a path that must not
  exist; atomic through `renameat2(RENAME_NOREPLACE)` on Linux or a hard link, else check then rename);
  `sync_directory` (fsync a directory after a rename into it); `printable` (Python escapes for non-printables,
  used by the log, the API's log lines and the CLI); `unsafe_name_reason` (one plain path component on this OS;
  `walker` uses it) and `safe_default_name(name, fallback)`, which every default output name chosen by someone
  else goes through (DAG last names, sealed names, LXMF link names): printable, no leading `.` or `-`, no `\`,
  a safe component, else the fallback (the CID). Text files are opened with `encoding="utf-8"`.

Node IDs, DHT keys and names are 16-byte `bytes`. Identity hashes (for `members`, `remote_pin_identities`,
pin source `remote:<identity>`) are RNS identity hashes, and LXMF addresses (`lxmf_*` settings, pin source
`lxmf:<address>`) are `lxmf.delivery` destination hashes, all written as 32 hex characters.

Persistent state is JSON (or msgpack) under `config.state_dir`, written atomically and durably
(`storage.write_json`: temp file fsynced, `os.replace`, then the directory synced). `storage.read_json(path,
shape)` treats only a file that isn't JSON, fails `shape` or is a directory as damaged: it is moved to
`.damaged` (`.damaged.1`, … keeping earlier copies) and the node starts without it; any other `OSError` stops
startup, since starting without `pins.json` would lose every pin. Everything shared between threads locks
internally.

## Layout and ownership

```
rnfs.py, rnfs/__main__.py   entry points (python3 rnfs.py, python3 -m rnfs); the installed command is rnfs.cli:main
rnfs/
  constants.py config.py errors.py log.py          shared foundation
  fsutil.py           cross-platform file operations: replace/remove retried on Windows, O_BINARY, IS_WINDOWS
  content/            no networking; uses RNS's bundled msgpack and RNS logging, so importing it loads RNS
    cid.py            CIDs (section 4)
    dag.py            DAG node encoding, strict decoding, names (section 5)
    chunker.py        fixed and FastCDC chunkers; chunker.using(name) picks one per context (section 5)
    builder.py        building trees: files, directories, wrap, only-hash (section 5)
    seal.py           sealed files: SealingReader, open_sealed/SealedFile, recipients (section 5, Sealed files)
    walker.py         Expect, reader checks, path lookup, byte ranges, listing, writing out (sections 5, 7)
    edit.py           dir set / dir rm (section 11)
    blockstore.py     one file per CID, atomic, verified (section 8)
    storage.py        Storage: blocks + pins (sources) + cached fetches + protection + eviction/GC (section 8)
    archive.py        export/import stream; archive_size for an exact Content-Length (section 8)
    shared_folder.py  importing <rnfs dir>/files/ with pin source "folder" (section 8)
  dht/                no RNS dependency
    routing.py        RoutingTable: buckets, seniority, failures, bans (section 6)
    providers.py      ProviderStore: records, TTL, max_records eviction, /find reply selection (section 6)
    lookup.py         iterative lookups with alpha/beta, time budget, unknown-ID budget (section 6)
  network/            everything that talks to RNS
    protocol.py       message encoding/validation, batch sizes (sections 3, 6, 7, 8)
    rates.py          rate estimates, slow links, slow node detection (section 2)
    links.py          LinkPool: shared unidentified links, identified one-shot links, idle close (section 3)
    guards.py         process-wide guard rejecting oversized or request Resource advertisements (3, 10)
    server.py         RequestServer: handlers, every section 3 limit, airtime policy, feature paths (3, 7)
    client.py         PeerClient: /find, /provide, /blocks (with stall detection), /pin, ping (sections 3, 6, 7)
    announces.py      rnfs.dht announces carrying the DHT mode, and the announce handler (section 3)
    identity.py       node identity file, node ID helpers
  mesh.py             provider ranking (PathInfo, provider_rank, RnsPaths), airtime policy, Traffic (3, 6)
  names.py            NameManager: name keys, records, publish, resolve, sequences, keeper hook (section 9)
  fetcher.py          fetching a target from providers; observing() for job progress (sections 6, 7)
  publisher.py        provide strategy, batched /provide, republish, withdrawal, extra key sources (section 6)
  node.py             RnfsNode: wires everything; the facade below
  node_content.py     node side of --no-pin, sealing/unsealing, raw blocks and archive streams
  api.py              local API, 127.0.0.1:8788 (section 11)
  gateway.py          gateway, 127.0.0.1:8789 (section 11)
  gateway_formats.py  ?format=raw|archive (section 11)
  cli.py              command line (section 11)
  features/           optional features, one module each (below)
    __init__.py       Feature base class, FEATURE_MODULES, feature API routes
    peers.py          rnfs peers, rnfs stats
    jobs.py           background jobs (section 11)
    keepers.py        /name, kept names (section 9)
    follow.py         followed names (section 9)
    contacts.py       address book (section 11)
    lxmf.py           rnfs: URIs, LXMF send/inbox/outbox/auto-fetch (section 13)
install.sh            Linux installer of the latest release (or --main): venv, command, optional systemd user
                      unit; --update, --uninstall
contrib/              rnfs.service (systemd user), io.github.afit21.rnfs.plist (launchd), install-windows-task.ps1
packaging/            requirements.lock (hash-pinned runtime dependencies), update-lock.py (regenerates it),
                      build-deb.sh, flatpak/ (manifest, wheels generated from the lock), pyinstaller/ (one-file exe),
                      smoke_test.py
.github/workflows/    tests.yml (Linux, macOS, Windows), release.yml (builds, smoke-tests and publishes releases)
```

## Content layer interfaces

`dag.py`
- `FILE, DIRECTORY, INDEX = 1, 2, 3`; `KIND_FILE, KIND_EXECUTABLE, KIND_DIRECTORY = 0, 1, 2`.
- Frozen dataclasses `Child(cid, size)`, `Entry(name, cid, size, kind)`, `IndexChild(first_name, cid, size)`;
  nodes `FileNode(children)`, `DirectoryNode(entries)`, `IndexNode(children)`.
- `encode(node) -> bytes`; `decode(block) -> node`, strict (types, shortest headers, re-encodes identically),
  raising `InvalidDag`. `check_name(name)` raises `ValueError`.

`walker.py` — reader checks are driven by an **expectation** of what a child must be:
- `Expect(cid, size: int | None, types: frozenset[str], depth: int, first_name: str | None, below: str | None)`,
  frozen. `types` ⊆ {"raw", "file", "directory", "index"}; `first_name` is the name the first name below must
  equal; `below` an exclusive upper bound for every name below. `root_expect(cid)` allows any type at depth 1.
- `expand(expect, block) -> list[Expect]` — check a verified block against its expectation (type, size, names,
  index bounds, depth) and return its children's expectations. Raises `InvalidDag`. Raw blocks have none.
- `step(expect, block, name) -> Expect | None` — one path step in a directory or index node (for an index, the
  child that would hold `name`; keep stepping until a directory entry is reached).
- `range_children(expect, block, offset, length) -> list[tuple[Expect, int, int]]` — children of a file node
  overlapping a byte range, with the range relative to each child.
- `parse_target(text) -> tuple[Cid, list[str]]` for `<cid>[/name…]` (ValueError if malformed).
- Local operations over a blocking `get(expect) -> bytes` (raise `NotFound` when absent):
  `resolve(root, names, get) -> Expect`, `iter_dag(expect, get)` (each `(expect, block)`, each CID once,
  depth-first), `read_range(expect, get, offset, length) -> Iterator[bytes]`,
  `list_directory(expect, get) -> Iterator[Entry]`, `write_out(expect, get, dest, max_entries) -> warnings`.

`storage.py` — `Storage(blocks_dir, state_dir, max_storage)`, thread-safe:
- blocks: `has(cid)`, `get(cid, verify=False) -> bytes | None` (with `verify`, a block no longer matching its
  CID is deleted), `put(cid, block)` (verifies, even a block already stored; atomic, the file fsynced),
  `used_bytes()`. `BlockStore.sync()` syncs the blocks directory; `Storage` calls it before saving pins or
  cache records, so they never refer to block names a power loss could undo.
- pins: `pin(cid, source)`, `unpin(cid, source) -> bool`, `pins() -> dict[Cid, set[str]]`, `is_complete(cid)`.
- cache: `record_fetch(root, names, target)` (keeps the path's nodes and the target's DAG), LRU touched on use.
- protection: `with storage.protect() as session:` — blocks stored or listed in `session.add(cid)` are kept as
  if pinned until the block exits (fetches, adds, imports in progress).
- `check_disk(nbytes)` raises `StorageFull` when storing `nbytes` would leave less than the disk reserve
  (max(256 MiB, 2% of the filesystem)) free. `max_storage` itself never refuses a fetch: it is enforced by
  eviction once fetches end.
- `evict() -> set[Cid]` enforce `max_storage` (LRU cached fetches); `gc() -> stats` evicts every cached fetch;
  both delete unreachable blocks and report targets no longer complete, so the node can withdraw them.
- `verify() -> stats` re-checks every block, deleting bad ones.
- A stored DAG node that no longer matches its CID (`DAMAGED`) has unknown children: while anything live
  reaches one, `evict()` and `gc()` delete nothing and log it once (`_held_back`) until `verify()` removes it or
  a fetch replaces it. Fetches treat such a block as not stored: deleted and fetched again.
- `dag_blocks(cid) -> set[Cid]` the blocks of a complete DAG (memoised), used for "what a node serves".

## DHT layer interfaces

`routing.py` — `RoutingTable(own_id, config, clock=time.monotonic)`:
`heard(node_id) -> bool` (proven server: announce in server mode, or answered `/find`), `failed(node_id) -> bool`
(True if dropped), `remove(node_id)`, `ban(node_id)`, `is_banned(node_id)`, `closest(key, count=K, for_reply=False)`,
`seniority(node_id) -> float | None` (seconds in the table), `contains(node_id)`, `node_count()`.

`providers.py` — `ProviderStore(max_records, state_path, ttl=PROVIDER_TTL, clock=time.time, rng=random)`:
`store(key, publisher) -> bool`, `withdraw(key, publisher)`, `for_reply(key, exclude=lambda id: False) -> list[bytes]`,
`expire()`, `record_count()`, `save()`. Local limits against flooding: one publisher holds at most max(10,
`max_records` // 100) records (not the node's own, `own_id`), and a key keeps at most 1000 publishers: a
newcomer to a full key evicts a random one, never one of the `K/2` oldest records. Times later than now (a
clock corrected backwards) are clamped to now at load and at each `expire()`.

`lookup.py` — network access is injected:
`FindQuery = Callable[[node_id, key], tuple[list[bytes], list[bytes]] | None]` returning (providers, nodes).
`Lookup(key, start_nodes, query, own_id, alpha, beta, k, timeout, is_known, on_answer, on_failure)` with
`find_closest() -> list[bytes]` and `find_providers(on_provider, should_stop) -> list[bytes]`. `is_known(id)`
says whether RNS knows its identity; at most `alpha` unknown IDs are tried per lookup (`allow_unknown(id)`, also
used by the fetcher for provider IDs).

## Network layer interfaces

`server.py` — `RequestServer(destination, config, backend)`. `backend` provides:
`is_dht_server() -> bool`, `find(key) -> (providers, nodes)`, `provide(publisher_id, keys, withdrawn) -> bool`,
`served_block(cid) -> bytes | None` (only blocks in the DAG of a currently provided CID, re-verified),
`remote_pin(identity_hash, cid, pin) -> bool`, `is_member(identity_hash) -> bool` (private mode).

`client.py` — `PeerClient(links, config)`:
`find(node_id, key) -> (providers, nodes) | None`, `provide(node_id, keys, withdrawn) -> set[bytes]` (identified
link, batched; the keys the node may have recorded: every key, or those of the batches up to and including the
first refused or unanswered one), `blocks(node_id, cids, on_progress=None) -> BlocksResult` (status OK / BUSY /
FAILED / STALLED; blocks verified against their CIDs; `.blocks: dict[Cid, bytes]`, `.missing` answered None,
`.unsent` cut by the budget, `.bad` hash mismatches), `pin(node_id, cid, pin) -> bool`, `ping(node_id) -> float |
None`, `has_link(node_id)`, `close(node_id)`. `links.py` links only to the `rnfs.dht` destination of the identity
RNS recalls, and only if its hash is the node ID asked for; the idle sweep spares a link `LinkPool.get()` handed
out within `link_idle_timeout`.

`announces.py` — `DhtAnnouncer(destination, config)` sets `app_data` to the mode and runs the announce schedule;
`DhtAnnounceHandler(on_server, on_client, is_banned)`.

## Node facade (used by api.py, gateway.py and cli.py)

`RnfsNode(config)` exposes `node_id: bytes`, `config`, and these methods. Targets are strings
`<cid>[/name…]`; CIDs are returned as strings. Return values are JSON-ready dicts.

| Method | Returns |
|---|---|
| `add_path(path, wrap=False, only_hash=False, pin=True, to=None, to_self=False, seal_name=True, chunker=None)` | `{"cid", "size", "warnings": [str]}` (+ `"sealed"`); file or directory, pinned `user` (or cached with `pin=False`) |
| `add_stream(stream, size=None, name=None, only_hash=False, …)` | as above; `name` wraps it as that entry (sealed: the sealed name) |
| `get(target, output, from_node=None, raw=False)` | `{"cid", "warnings"}` (+ `"warnings_total"` past 100; + `"sealed", "name"`, the name only if `safe_default_name` accepts it); fetches and writes the file or directory under `.rnfs-get-<random>.partial` beside `output`, then renames it to `output` (`rename_new`), decrypting a sealed file unless `raw` |
| `info(target, for_gateway=False, from_node=None, raw=False)` | `{"cid", "type": "file"\|"directory", "size", "executable"}` (+ `"sealed", "recipient", "stored_size", "name"`; the sealed name is None unless `safe_default_name` accepts it) |
| `stored_head(target, length)` | a file's first `length` bytes if stored, else None; never fetches (gateway `HEAD` and ranges not at 0) |
| `read(target, offset=0, length=None, for_gateway=False, from_node=None, raw=False)` | iterator of bytes of a file (range fetched only; sealed files decrypted unless `raw` or `for_gateway`) |
| `block(cid, for_gateway=False)` | the CID's own block, verified (`cat --block`, `?format=raw`) |
| `export_stream(target, start, for_gateway=False)` | fetches the DAG, then writes its archive to `start(exact length)` (`?format=archive`; `None` for HEAD). The gateway calls it for a limited request or a HEAD only when `is_local(target)`, and otherwise inside `fetcher.observing(ClientWatch)`, which cancels the fetch once the client disconnects |
| `ls(target, for_gateway=False)` | `[{"name", "kind": "file"\|"executable"\|"directory", "size", "cid"}]` |
| `stat(target)` | `{"cid", "size", "blocks", "local_blocks", "local_bytes"}` |
| `is_local(target)` | `bool`: everything needed is stored, so no network fetch |
| `dir_set(target, name, child_cid, executable=False)`, `dir_rm(target)` | `{"cid"}`, pinned `user` |
| `pin_add(target)` | `{"cid"}` once complete |
| `pin_rm(cid)` | `None`; NotFound if no `user` source |
| `pin_ls(cid=None)` | `[{"cid", "sources": [str], "complete": bool, "type": "file" \| "directory" \| None, "name": str \| None}]`; `type` and `name` only when the root block is stored, `name` where the pin was added from (pin_names), its shared-folder entry, or a directory's single entry or entry count |
| `pin_remote(node_id, cid, pin)` | `bool` accepted |
| `gc()` / `verify()` | `{"removed_blocks", "freed_bytes"}` / `{"checked", "bad": [cid]}` |
| `providers(cid)` | `[node_id bytes]` |
| `ping(node_id)` | round-trip seconds, or None |
| `name_create(alias)`, `name_import(alias, key_bytes)` | `{"alias", "name"}` |
| `name_rm(alias)`, `name_export(alias)` | `None`, key bytes |
| `name_list()` | `[{"alias", "name", "cid" or None, "sequence"}]` |
| `name_publish(alias, target)` | `{"name", "cid", "sequence"}` |
| `name_resolve(name, for_gateway=False)` | `{"name", "cid", "sequence", "age"}`; NotFound if unresolved |
| `export(target, output)` / `import_archive(path)` | `{"roots", "blocks"}` / `{"roots", "incomplete", "blocks"}` |
| `status()` | `{"version", "node_id", "identity_hash", "dht_mode", "slow", "private", "known_nodes", "records", "pins", "provided", "blocks", "used_bytes", "max_storage", "names"}` plus every feature's `status()` fields (traffic, jobs, LXMF); `identity_hash` is the RNS identity hash (None on offline test nodes) |

`RnfsNode.offline(config, node_id, client, names_network)` builds a node without Reticulum or background
threads, for tests (tests/test_node.py). The request server's backend is `node.backend` (`NodeBackend`).
`fetcher.py`: `Fetcher.open(root, names, from_node, pinned)` gives a `Fetch` context (a protection session)
with `resolve()`, `fetch_dag()`, `fetch_listing()`, `read_range()`; `publisher.py`: `Publisher.refresh()`
(recompute the provide set, withdraw at once), `publish_due()`, `serves(cid)`, `run()`. The node calls the
first `refresh()` only after the features have registered their key sources, so a restart doesn't withdraw
kept names' keys. `publish_due` republishes `DhtKey`s every max(effective `republish_interval`, 3 days)
with a per-key ±10% jitter (capped below `PROVIDER_TTL / 2`), drops a withdrawal of any key it provides to
the same node in that round, and on slow nodes runs at most 10 lookups per round, waiting 30 s × the
round's lookups before the next (never-published items first, then the longest unpublished).
`protocol.provide_batches` puts withdrawals before stores, and a key in both lists is only stored.

Errors: `ValueError` → 400, `NotFound` → 404, `TooLarge` → 413, `InvalidDag` → 422, `Busy` → 503,
`StorageFull` → 507, other `RnfsError` → 500. Paths passed to `add_path`, `get`, `export` and `import_archive`
are absolute paths on the daemon's machine (the CLI resolves them); stdin is uploaded as a request body.

**Core API routes** (`rnfs/api.py` `ROUTES`): `GET /api/status` (open), `/api/info`, `/api/cat`, `/api/ls`,
`/api/stat`, `/api/pin/ls`, `/api/providers`, `/api/ping`, `/api/name/list`, `/api/name/resolve`;
`POST /api/add`, `/api/add/stream`, `/api/get`, `/api/dir/set`, `/api/dir/rm`, `/api/pin/add`, `/api/pin/rm`,
`/api/pin/remote`, `/api/gc`, `/api/verify`, `/api/name/create`, `/api/name/rm`, `/api/name/export`,
`/api/name/import`, `/api/name/publish`, `/api/export`, `/api/import`. `GET /api/info` and `POST /api/get` take
an optional `from` (node ID); `GET /api/name/resolve?name=` takes a name or a name contact's nick.

The API requires `X-Rnfs-Api: <token>` on every request except `GET /api/status`. The daemon writes a new random
token to `<rnfs dir>/api_token` (mode 0600, in the 0700 rnfs dir) at each start and the CLI reads it, so other
local users and processes can't drive the API. `_make_private_dirs` refuses to start (RnfsError) if the rnfs dir
belongs to another user or can't be made 0700 (not on Windows). A response to a request with
`X-Rnfs-Challenge: <32 hex>` carries `X-Rnfs-Proof: cli.api_proof(token, port, challenge)` (HMAC-SHA256 of
`rnfs api proof <port> <challenge>`); `ApiClient.verify_daemon` checks it with `GET /api/status` before first
sending the token. Both HTTP servers (`TimedHandler`) time out slow requests, cap concurrent connections (API
32, gateway 64) and refuse HTTP/0.9 with a 400 carrying the usual headers. Gateway 500s say only "internal
error" (the detail goes to the log); malformed `Range` headers are ignored.

## Content additions

- `content/chunker.py` — `chunks(stream, size)` yields chunks from the chunker chosen with the context
  manager `chunker.using(name)` (a ContextVar, so the shared-folder scan on its own thread keeps fixed
  chunks); `builder.build_file` takes its chunks from it. FastCDC computes gear hashes for 8 KiB at a time on
  a big integer of 9-byte lanes (≈ 39 MB/s in CPython 3.12, against ≈ 15 MB/s for the byte-by-byte reference
  `cut_point_simple` it is tested against, and ≈ 2 GB/s for fixed chunking).
- `content/seal.py` — the sealed-file format: `SealingReader` (file-like, one segment in memory),
  `open_sealed(raw, total)` → `SealedFile` (`unlock(identity)`, `size`, `name`, `read(offset, length)`
  decrypting only the covering segments), `parse_recipient`. AES-256-CBC and HKDF come from
  `RNS.Cryptography`, HMAC/SHA-256 from the standard library: no new dependency.
- `node_content.py` — `keep_added` (a `user` pin, or for `--no-pin` a cached fetch recorded in
  `fetched.json` for the roots strategy), `seal_recipients` (`RNS.Identity.recall` by destination hash, then
  identity hash; the node's own identity; contacts), `open_sealed` over a Fetch's `read_range`,
  `sealed_info`, `write_sealed`, `block`, `export_stream`, and the API option parser `add_options`.
- `gateway_formats.py` — `?format=raw|archive`; `archive.archive_size` gives the exact length, so the archive
  is sent with a Content-Length before streaming. An archive for a web page's request or a HEAD is made only
  of a target stored completely (else 409), so archives never take a `gateway_fetches` slot; a bare-CID
  `If-None-Match` revalidation answers 304 first.
- `content/seal.py` rejects a final segment of more than 65536 plaintext bytes, and `SealedFile.name` is None
  for a name that fails its tag, isn't UTF-8 or isn't a valid name. `write_sealed` reads the name before
  creating the output, so nothing raised there leaves a file. `open_sealed` reports the stored (ciphertext)
  size as a job's total, which is what its done bytes count.
- Cost: `get`, `cat` and `info` of a file read its first 12 bytes to detect sealing (`--raw` skips this), so
  the first chunk is fetched first even for a `cat --offset` deep in a file. The gateway does so only for a
  GET from byte 0; a HEAD, or a range not at 0, uses the first bytes only if already stored
  (`stored_head`), else takes the Content-Type from the name, or `application/octet-stream`.

## Mesh

`mesh.py` (no RNS at module level): `PathInfo`, `provider_rank` and `RnsPaths` (hops and next-hop
interfaces from `RNS.Transport`, or through a shared instance's RPC with its interface stats cached for 30 s);
the airtime policy (`TokenBucket`, `AirtimePolicy`, interface matching, quiet-hours parsing, all with
injected clocks); and `Traffic`, thread-safe counters per peer (LRU, at most 1000), per interface (at most 64)
and in total, never persisted.
- `Fetcher.path_info` (default: nothing known) is set by the node to `RnsPaths.info`; `Fetch._rank` orders
  both providers and open-link probes, and each provider caches its `PathInfo`.
- `RequestServer.airtime` checks interface rules, quiet hours and an empty budget before taking any slot,
  caps the block budget, and charges the bucket once the response's size is known. Each `Response` records
  its charge and its requester (identity hash, else link ID); one requester's unsettled charges may reach at
  most half the bucket's capacity (`_budget_share_left` cuts the budget, or refuses). When a response
  concludes (`_finish`), a failed, cancelled, displaced or never-started Resource, or one whose link closed,
  refunds `charged − progress × size`; completed, single-packet and untracked responses, and ones whose
  progress can't be read, keep their charge.
  `RequestServer.traffic` and `PeerClient.traffic` share one `Traffic`.
- `network/guards.py` also rejects, on `rnfs.dht` links only (recognised by the link's destination or owner
  name `rnfs.dht.<hex>`), any Resource advertisement with more than one segment.

## Features

`rnfs/features/__init__.py` defines `Feature` and `FEATURE_MODULES` (in start order: peers, jobs, keepers,
follow, contacts, lxmf). The node builds every feature in its constructor (`node.feature(name)`), starts them
after its own threads and stops them, in reverse, before its own services. A feature may override:
`start()`, `stop()`, `save()` (called with the node's periodic saves), `status()` (fields merged into
`rnfs status`), `requests()` (new `rnfs.dht` paths; the request server applies private mode and the per-link
limit, and turns `None` or an exception into `False`), `api_routes()` (all token-protected) and
`cli_commands()`. Feature modules don't import RNS at module level (the CLI imports them to build its parser)
and their constructors work on `RnfsNode.offline` test nodes.

| Feature | Module | What it does | Hooks in shared code |
|---|---|---|---|
| `peers` | `peers.py` | `rnfs peers`, `rnfs stats`; its own `rnfs.dht` announce handler, so client-mode nodes' mode and last-heard time are known | reads `Traffic`, the routing table, links, paths |
| `jobs` | `jobs.py` | `JobsFeature` (the job manager), `Job`, CLI progress formatting | `fetcher.observing(observer)`: a thread-local observer every `Fetch` picks up (`total`, `block`, `cancelled` checked at each step, raising `fetcher.Cancelled`) |
| `keepers` | `keepers.py` | signed records (`SignedRecord`, `parse_signed`, `sign_record`, `from_announce_data`), `/name`, `KeeperClient`, `KeeperStore` | `Feature.requests`; `publisher.DhtKey` and `Publisher.add_key_source` (non-CID keys published like CIDs); `NameManager.keepers` (`published`, `heard`, `query`, `always_ask`), `NameManager.offer()`/`record()`; the 5-argument announce handler (packet hash) |
| `follow` | `follow.py` | followed names: each check and fetch one at a time, `fetcher.open(pinned=True)`, pin inside the fetch's protection, old pin released after | keepers' `FOLLOW` reason |
| `contacts` | `contacts.py` | address book; `expand_target`, `lookup(nick, kind)`, `node_arg` | `RnfsNode._parse_target` (never for the gateway), `JobsFeature.submit` |
| `lxmf` | `lxmf.py` | pure parts (`Link`, `parse_uri`, `format_uri`, `find_links`, `encode_field`/`decode_field`, `MessageLog`); `LxmfFeature` (send with direct→propagated fallback, inbox, auto-fetch, inbox get, pin release); `LxmfNetwork`, made in `start()`, holds every RNS/LXMF object | `lxmf.validate_settings` from `Config.validate`; `LXMF_PREFIX` in `storage.check_source` |

`LxmfNetwork` makes an `LXMF.LXMRouter` with storage `<rnfs dir>/lxmf/` and `signal.signal` stubbed (LXMRouter
installs SIGINT/SIGTERM handlers that exit at once), a 256 kB delivery limit, announce data without
`SF_COMPRESSION`, and incoming delivery Resources (and propagation-node downloads) allowed to decompress up to
that limit, since rnfs sets RNS's global limit to 0. It keeps a table of up to 64 propagation nodes from
announces (forgotten after 7 days unheard), but uses one automatically only if it is `lxmf_propagation_node`;
the nearest heard is used only for a request with `nearest`, and without a configured node the feature logs
once (notice) that downloads and the fallback are off. On a propagation-node link it cancels any response
Resource over 1 MB (4 × `DELIVERY_LIMIT_KB`) or of more than one segment and tears the link down, and asks for
at most 64 messages per download. Messages from unknown senders are held (at most 32; further ones dropped, and
none from senders `accepts()` would ignore) while their identity is requested: one path request per sender per
2 minutes, from a node-wide token bucket (burst 3, then one per 10 s). A held message stays in the queue while
it is retried, so it still counts as a duplicate and against the limit. The inbox (`MessageLog`) holds at most
500 records and 4 MiB as JSON, 50 per sender not in `lxmf_auto_fetch_from`, 16 KiB per record (links beyond it
dropped, then text cut), and never evicts a record holding auto-fetched pins for one that holds none; the pin's
"pinned" state is recorded in the same locked section that pins. Auto-fetched `name/` links resolve with
`persist=False`; each auto-fetch runs under a `fetcher.observing` observer cancelled when the node stops or the
message is removed, and links still "queued" in the inbox are queued again at `start()`
(`_requeue_auto_fetches`, within the 64-entry queue). Links without the field are read from the first
`MAX_SCAN_CHARS` (16 384) characters of the content. `inbox get` (through the jobs feature) checks every output
before starting any (two links with one output are refused), then submits a get job per link with its `from`
and `fallback_dht`; once one has started, a later link's failure is reported in its result (`error`) rather
than raised. An output without `-o` is named with `fsutil.safe_default_name`. Sent links carry `from` only with
`lxmf_send_from_hint`. The inbox and outbox files are fsynced before being renamed into place.

**Feature API routes** (all need the token): `POST /api/jobs`, `GET /api/jobs`, `GET /api/jobs/get?id=&wait=`,
`POST /api/jobs/cancel`, `POST /api/jobs/clear`; `GET /api/peers`, `GET /api/stats`; `POST /api/name/keep`,
`/api/name/unkeep`, `GET /api/name/kept`; `POST /api/name/follow`, `/api/name/unfollow`,
`GET /api/name/following`; `GET /api/contacts`, `GET /api/contacts/get?nick=`, `POST /api/contacts/add`,
`/api/contacts/rm`; `GET /api/lxmf/address`, `POST /api/lxmf/send`, `GET /api/lxmf/inbox`,
`POST /api/lxmf/inbox/get`, `/api/lxmf/inbox/rm`, `GET /api/lxmf/outbox`, `POST /api/lxmf/sync`. `POST /api/jobs`
takes, for a get, the JSON booleans `sealed_name` (write a sealed file this node opens under its safe sealed
name in the output's directory; the CLI sets it for `rnfs get <bare cid>` without `-o`, so it no longer calls
`GET /api/info` first) and `fallback_dht` (a get with `from` that fails, other than by cancellation,
`TooLarge` or `StorageFull`, is retried once through the DHT). `POST /api/lxmf/send` and `/api/lxmf/sync` take
`nearest`; `GET /api/lxmf/address` returns `propagation_node` (the configured one only) and
`nearest_propagation_node`. Core routes gained options: `POST /api/add` `pin`, `to`, `to_self`, `seal_name`,
`chunker` (and the same as query parameters on `/api/add/stream`); `POST /api/get` `raw`; `GET /api/cat`
`raw=1`, `block=1`; `GET /api/info` `from`.

## Threads

| Thread | Owner | Does |
|---|---|---|
| `rnfs-join`, `rnfs-publisher`, `rnfs-names`, `rnfs-maintenance` | node | joining, publishing, name announces, periodic saves and eviction |
| `rnfs-announcer`, `rnfs-response-monitor`, `rnfs-link-sweeper` | network | rnfs.dht announces, `/blocks` deadlines and displacement, idle links |
| `rnfs-lookup`, `rnfs-fetch`, `rnfs-find-providers`, `rnfs-remote-pin` | per operation | lookups, fetch workers, provider search, remote-pin fetches (one thread per source with pins waiting) |
| `rnfs-job-<id>` | jobs | one per running job, at most `max_jobs`; `stop()` waits ≤ 2 s |
| `rnfs-keepers`, `rnfs-keeper-query` | keepers | pending refreshes (≤ 64 queued), own-name sync, saves, re-asking every `announce_interval`; parallel `/name` queries |
| `rnfs-follow` | follow | every check and fetch, one at a time |
| `rnfs-lxmf`, `rnfs-lxmf-fetch` | lxmf | announces, propagation-node downloads, polling outbound states each second; auto-fetch, one at a time (LXMF runs its own job thread) |

The API and gateway run one thread per connection, capped (32 and 64).

Locks are taken in this order, never the reverse: `names._lock` → `publisher._lock` → `node._lock` →
`storage._lock`. `NameManager.publish`, `remove` and `release_gone` pin and unpin, and so call
`publisher.refresh()`, while holding the names lock; `Publisher.refresh` recomputes what is provided under its
own lock, reading the node's fetched targets (`node._lock`) and walking the pins' DAGs (`storage._lock`); the
node reads storage under its lock (`_prune_fetched`). This is how the code behaves today, not a design goal.
Slow work that runs while a lock is held:

- `Publisher.refresh` walks every pin's DAG (memoised) under the publisher lock, behind the names lock when a
  name changes, so an RNS thread delivering a name announce can wait behind a full recompute.
- `Storage.evict`, `gc` and `verify` check every pin and cached target for completeness (`dag_blocks`,
  memoised, reading blocks from disk) under the storage lock; `evict` and `gc` also delete blocks under it.
  `verify` re-reads the blocks themselves outside it.
- `RnfsNode._note_fetched` and `_prune_fetched` write `fetched.json`, synced to disk, under the node lock.

## State files

Under `<rnfs dir>/state/`, JSON written atomically from a temp file synced to disk: `pins.json`, `cache.json`
(storage); `pin_names.json` (rnfs/pin_names.py: where each pin was added from, local only, pruned to the pin
list); `providers.json` (provider records); `published.json` (publisher: last rounds, earlier rounds' nodes,
pending withdrawals); `fetched.json` (targets fetched for the user, for `roots`); `names.json` (sequences, name
table); `invalid.json` (invalid roots); `shared_folder.json`; `jobs.json` (results keep at most 100 warnings
plus `warnings_total`; older results are capped at load); `keepers.json` (records re-verified at load);
`follow.json`; `contacts.json`; `lxmf_inbox.json` (500 messages, 4 MiB, 50 per untrusted sender, 16 KiB per
record) and `lxmf_outbox.json` (200; unfinished messages are marked failed at a restart), both UTF-8 JSON with
non-ASCII characters written as is. Elsewhere: `storage/identity`, `storage/lxmf_identity` (0600),
`names/<alias>` (name keys), `lxmf/` (LXMF's ratchets, delivered-message IDs, stamp costs), `api_token` (while
the daemon runs). Malformed records in feature state files (wrong types, duplicate IDs) are dropped at load
with a warning. Outputs being written live beside the output as `.rnfs-get-<random>.partial` (`RnfsNode.get`)
or `.rnfs-job-<id>.partial` (get jobs) until renamed; a failed, cancelled or shut-down job removes its partial,
and a resumed job removes one left by a crash.

## Tests

How to run the tests is in [README.md, Tests](README.md#tests). Feature tests live in their own files
(`test_jobs.py`, `test_mesh.py`, `test_names_keep.py`, `test_contacts.py`, `test_seal.py`, `test_chunker.py`,
`test_pin_false.py`, `test_gateway_formats.py`, `test_lxmf.py`), and `test_e2e.py` has a marked block per
feature. CI (`.github/workflows/tests.yml`) runs them on Linux, macOS and Windows; `packaging/smoke_test.py`
checks a built package (`--version`, `--help`, daemon and status).

## Status

Everything in the specification is implemented, with these simplifications and additions:
- The routing table isn't persisted. At startup it is seeded from the rnfs.dht server announces that RNS
  cached within 2 × `announce_interval` (`Identity.known_destinations`, an RNS internal), then the join runs.
- Publishing runs one closest-nodes lookup per due CID, one after another. Lookups aren't shared between keys
  that have the same closest nodes, but `/provide` requests are batched per node. Slow nodes pace these
  lookups (at most 10 per round, 30 s per lookup on average), which only delays republishing; withdrawals
  are sent at once.
- Withdrawals are sent once, with any provides due for the same node. One that fails isn't retried, since the
  record expires after `PROVIDER_TTL`. A CID is withdrawn from the nodes of its last round and from nodes of
  earlier rounds, or of a round cut short, that may still hold its record (`Publisher._earlier`, persisted in
  `published.json`, forgotten after `PROVIDER_TTL`). A round stopped before every node was sent its keys
  leaves its CIDs due, and persisted times later than now are clamped to now.
- A CID whose lookup found no nodes to publish to is retried after 30 s rather than at the next republish.
- When every provider has failed, the provider lookup runs again only if its previous run found a new
  provider (and time is left).
- `max_download_size` is checked against the target's verified size, or the range's size, before anything
  below the target is fetched, and the DAG-node bytes a fetch downloads count against it too, so directories
  of empty files can't get around it. Listing and writing out also stop fetching at `max_output_entries`.
- `max_storage` is soft. An unpinned fetch evicts other cached fetches (LRU) to make room for each response
  and goes above `max_storage` when nothing is evictable; when its session ends the node evicts again, which
  may evict what it fetched if pins alone fill `max_storage`. Any fetch is refused with `StorageFull` ("not
  enough free disk space") if its remaining size, the range's size, or a response would leave less than
  max(256 MiB, 2%) of the blocks' filesystem free (`Storage.check_disk`, `DISK_RESERVE_MIN`/`_FRACTION`).
  `get` and `export` also check free disk space at the destination first.
- Roots found invalid are remembered in `state/invalid.json` (at most 10 000, oldest dropped), and there is
  no command to clear them. Cache records are capped at 10 000 (LRU). Both are saved every 30 s and at stop.
- A remote pin's usage is the larger of its content size and its DAG's block bytes. It is dropped once the
  root block shows the quota would be exceeded, or once the blocks fetched exceed it. A source's remote pins
  are fetched one at a time, oldest first (`_remote_pin_worker`), so the quota check counts every earlier pin;
  at most 64 (`REMOTE_PIN_QUEUE`) may be waiting or fetching, and further `/pin`s are refused.
- Byte-range reads (`cat`, gateway) walk the file lazily and fetch 32 chunks at a time ahead of what has been
  written out. A read may meet at most `max_output_entries` pieces much shorter than `CHUNK_SIZE`, which bounds
  files built from repeated tiny or empty subtrees without limiting honest large files.
- Archive import pins only roots whose every block was in the archive, and reads at most 1024 roots. It
  verifies every block, including ones already stored, and stops with `StorageFull` like a fetch when a block
  not yet stored would leave less than the disk reserve free (`Storage.check_disk`).
- Upload pacing: `RequestServer` holds an accepted `/blocks` back at most `MAX_PACING_WAIT` (5.625 s, half of
  RNS's request timeout without RTT) and refuses one that would wait longer. After a response on a slow link,
  `Fetch._pace` waits as long again as the request took before asking that provider again, and a refusal within
  4 × that time of the request being sent isn't counted towards `max_failures` (retried after an eighth of it,
  at least `busy_delay`). A `Fetch` exiting with an error (or cancelled) closes the links of its requests still
  in flight. In private mode, incoming links not identified as a member's are closed first to make room.
- The shared-folder scan and `add` walk directories by file descriptor (`O_NOFOLLOW`, `O_NONBLOCK`, type and
  inode checks), so entries swapped for symlinks, FIFOs or devices mid-scan are skipped. Hard links inside the
  shared folder are real entries and are shared.
- `get` (`walker.write_out`) skips and reports names the destination filesystem refuses to create (EINVAL,
  ENAMETOOLONG, EILSEQ: e.g. `?` on FAT, long names on eCryptfs) rather than failing the whole write-out.
- On Windows (`rnfs/fsutil.py`, `IS_WINDOWS`): there is no execute bit, so `add` records every file as kind 0
  and `get` sets none; Unix permission checks are skipped (the rnfs dir relies on the ACL it inherits from the
  user's profile). Directories are walked by path rather than descriptor, each entry's type and file ID checked
  before use, and directory junctions are skipped like symlinks. Replacing or deleting a file another handle
  has open is retried for about a second; a block still in use then stays stored and indexed and is deleted by
  a later collection. `get` skips names Windows can't represent (`:`, device names, trailing dots), and the
  API and gateway bind their ports exclusively. The daemon stops cleanly on Ctrl+C or Ctrl+Break (SIGBREAK).

- **Jobs.** The `gc` command keeps the blocks of queued and running jobs (their protection sessions). A daemon
  without the job manager (older versions) is detected through `GET /api/status` (no `jobs_queued`):
  foreground commands then fall back to one blocking request (using `GET /api/info?from=` for a sealed name),
  and `--background` fails. A finished job is kept for at least 60 s (`FINISHED_GRACE`) whatever
  `max_finished_jobs` says. `rnfs jobs show` shows `sealed_name` and `fallback_dht` when set. The CLI prints
  output paths and job results through `printable`.
- **Keepers.** RNS keeps no announce signatures outside its on-disk packet cache, which only the instance
  owning the interfaces writes (rnsd, with a shared instance) and which is cleaned when the path expires, so
  capturing a newer announce from it is best effort; otherwise keepers ask other keepers over `/name`. RNS
  sends nothing for a `None` response, so "not kept" is `[]`. Kept records are offered to `NameManager` at
  startup and on every resolve (the rollback floor). Kept names without a record are retried round robin, at
  most 64 per `announce_interval`, continuing after the last one asked. `_store` refuses a record below the
  sequence `NameManager` holds for the name.
- **Names.** A key imported, or found without an entry in `names.json`, is marked `imported`: its first publish
  offers kept records, makes a path request and queries keepers before choosing a sequence. The pins of name
  keys deleted by hand are released by `release_gone()`, once the features are set up.
- **Follow** isn't built on jobs (job pins use source `user`); `pin add --follow` doesn't exist. Checks use
  the record held unless nothing was heard for 2 × `announce_interval`; a failed check (any exception) is
  retried after 300 s, doubling up to `announce_interval`, and a failed fetch is retried with the record held
  (no path request).
- **Contacts.** Every `<name>` argument (`name resolve|keep|unkeep|follow|unfollow`) takes a name contact's
  nick; the gateway never expands nicks. A nick at the start of a target reuses the CID resolved for it for
  60 s (`EXPAND_AGE`) from the time of the resolve, not from when the record was heard.
- **Sealed files.** `get` of a *directory* containing sealed files writes them as stored (get each with
  `get <cid>/<path>`). Sealing hides content and name but not size, recipients or wrapper names, and gives no
  sender authentication. A sealed file's 65600-byte segments don't align with 64 KiB chunks, so a
  one-segment range read may fetch two chunks. Jobs (`get` without `--raw`) decrypt as plain `get` does.
- **Mesh.** Attached to a shared `rnsd` instance, every incoming link arrives on the local shared-instance
  interface, so `serve_interfaces`/`no_serve_interfaces` can't tell radios apart (a warning is logged); run
  rnfs as the instance to use them. Traffic counters aren't persisted.
- **LXMF.** Propagation delivery and downloads aren't covered by the e2e test (it has no propagation node).
  Stamps a recipient demands are computed by LXMF (CPU, in subprocesses). Messages over 256 kB are refused.
  The outbound queue is lost at a restart (LXMF doesn't keep it). A message from a sender whose identity
  can't be found within 2 minutes is lost (LXMF has already marked it delivered). Links sent carry the node
  ID as `from` unless `lxmf_send_from_hint = false`; the default display name (`rnfs node`) doesn't. No
  propagation node is used unless configured (or asked for with `--nearest`).
- **Packaging.** Release artifacts (`.deb` for amd64/arm64, x86_64 Flatpak, Windows x64 exe, wheel and sdist)
  are built by `.github/workflows/release.yml`. Only a pushed `vX.Y.Z` tag matching `rnfs/__init__.py`'s
  `__version__` publishes them, once the whole of `tests.yml` (which release.yml calls) has passed, with
  `SHA256SUMS` and signed build provenance (`gh attestation verify`), never replacing files already attached;
  a manual run only builds. The Windows exe isn't code-signed, and unpacks itself on each start.
  - `lxmf>=1.2.0,<1.3` is a regular dependency next to `rns>=1.5.6,<1.6` (`pyproject.toml`), so every
    install includes LXMF; the PyInstaller spec fails the build without it. The upper bounds keep unpinned
    installs off RNS and LXMF releases rnfs hasn't been tested with, since it relies on some of their internals.
  - `packaging/requirements.lock` pins the tested versions (rns 1.5.6, lxmf 1.2.0, cryptography 50.0.2,
    cffi 2.1.1, pycparser 3.0, pyserial 3.5) with the sha256 of every file PyPI has for each.
    `python3 -I packaging/update-lock.py [pkg==ver …]` regenerates it (no arguments: the newest versions
    `pyproject.toml` allows), then `packaging/flatpak/generate-sources.py` regenerates the Flatpak's
    `python3-deps.json` from it; both are committed. `install.sh`, `build-deb.sh`, the tests and the release's
    Windows exe install from it with `pip --require-hashes --only-binary=:all:` (then `pip check`), and the
    release's flatpak-sources step fails if regenerating changes the committed files. The sdist includes the
    lock.
  - `update-lock.py --build pyinstaller==<ver>` writes `build-backend.lock` (setuptools, within
    `pyproject.toml`'s `<85` bound) and `build-tools.lock` (build, twine, PyInstaller, setuptools, ruff and
    their dependencies), hashed the same way. Every build installs these with `--require-hashes` and builds
    without build isolation, so nothing unpinned is fetched for a build; the Flatpak's `build-sources.json` comes from
    `build-backend.lock`. `build-deb.sh` fails if a supported Python has no cffi backend, caps `python3` below
    3.15, and writes a DEP-5 `copyright` and `md5sums`.
  - `install.sh` installs the latest release's source archive (`archive/refs/tags/<tag>.zip`, the tag taken
    from where `releases/latest` redirects, so no GitHub API), the main branch's with `--main`, or
    `RNFS_SOURCE`. Since the script on `main` installs older release trees, it must keep working with every
    release's layout from 0.1.1 on (`packaging/requirements.lock`, `packaging/build-backend.lock`).
    It unpacks an archive source itself (with Python, refusing absolute or `..` entries) to find its
    locks; a `.whl` or other requirement has none, so it warns and installs dependencies unpinned. It installs
    wheels only (`RNFS_ALLOW_SOURCE_BUILDS=1` builds missing ones from hashed sources with unpinned build
    tools), builds rnfs in a separate venv with the locked setuptools, sets `umask 022` in system mode, and
    replaces or removes only a systemd unit it wrote (marked in its first line; `--force` replaces another). It
    doesn't upgrade the venv's pip, runs every Python with `-I` from `/` once its paths are absolute (so
    modules in the caller's directory are never imported), refuses a relative `HOME`, ignores relative
    `XDG_DATA_HOME`/`XDG_CONFIG_HOME`, and writes the systemd unit's `ExecStart` quoted with `%` as `%%`
    (refusing a launcher path with a quote, backslash or control character). `contrib/rnfs.service` is quoted
    the same way, sandboxed (`NoNewPrivileges`, `RestrictAddressFamilies`, …) and gives up after 5 failed
    starts in 10 minutes; `contrib/install-windows-task.ps1` doubles quotes in paths, refuses `%` in them, runs
    the daemon through a wrapper script that restarts it on errors (likewise giving up), and
    `-Stop`/`-Uninstall` stop the task's whole process tree.
  - CI: `tests.yml` has `permissions: contents: read`; both workflows pin actions to commit SHAs, the release's
    Flatpak container is pinned by digest, workflow values reach `run:` scripts only through `env:`, checkout
    doesn't persist credentials, and build tools come from `build-tools.lock`. The install.sh job checks that a
    planted `venv.py`/`pip` in the caller's directory isn't imported and that rns came from the lock.
