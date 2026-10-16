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
  `MAX_WARNINGS` (100) warnings plus a count line and `warnings_total`. With a debug sink set
  (`set_debug_sink`), every message also goes to it, whatever RNS's loglevel.
- `rnfs/debuglog.py` — the debug log file (`debug_log`, `daemon --debug-log`), `<rnfs dir>/debug.log` (0600),
  started by the daemon before the node: version first, then every rnfs message (warnings and errors logged in
  an `except` block with their traceback), RNS's own log lines at Info or more severe (`RNS_LEVEL`; copied from
  `RNS.logdest = LOG_CALLBACK`, which still prints all of them), threads that die (`threading.excepthook`), a status line every 5 minutes (`status_line`:
  the node's status, open links, each interface up or down), and gateway requests, numbered (`tag`). Each start
  and every 16 MiB moves the file to `debug.log.1`. Lookups report their queries through `Lookup(report=)` (dht
  stays free of RNS), links their path, close reason and each request's outcome, all at `LOG_DEBUG`.
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
  debuglog.py         the debug log file, <rnfs dir>/debug.log (debug_log)
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
    guards.py         process-wide guards rejecting oversized or request Resource advertisements and, on
                      rnfs.dht links, responses that fail a msgpack header check before RNS decodes them
                      (3, 10); the half-open incoming link cap per destination (3)
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
  `used_bytes()`. Adds, the shared folder and archive import store through `with session.writer() as put:`,
  which writes blocks on up to 16 threads (`WRITE_THREADS`, at most 32 queued) so their fsyncs overlap, and
  has stored every one, or raised the first error, before the block exits: nothing is pinned before its blocks
  are. It is handed only blocks built from, or checked against, their CIDs. `BlockStore.sync()` syncs the blocks directory; `Storage` calls it before saving pins or
  cache records, so they never refer to block names a power loss could undo.
- pins: `pin(cid, source)`, `pin_many(cids, source)` (one save for them all; archive import),
  `unpin(cid, source) -> bool`, `pins() -> dict[Cid, set[str]]`, `pin_count()`, `is_complete(cid)`.
- cache: `record_fetch(root, names, target)` (keeps the path's nodes and the target's DAG), LRU touched on use.
  `cache_generation()` grows whenever records are removed or retargeted, so the node prunes its fetched
  targets only then.
- protection: `with storage.protect() as session:` — blocks stored or listed in `session.add(cid)` are kept as
  if pinned until the block exits (fetches, adds, imports in progress).
- `check_disk(nbytes)` raises `StorageFull` when storing `nbytes` would leave less than the disk reserve
  (max(256 MiB, 2% of the filesystem)) free. `max_storage` itself never refuses a fetch: it is enforced by
  eviction once fetches end.
- `evict() -> set[Cid]` enforce `max_storage` (LRU cached fetches); `gc() -> stats` evicts every cached fetch;
  both delete unreachable blocks and report targets no longer complete, so the node can withdraw them. Both
  first read, without the lock, the DAG nodes they will walk (`_warm`, filling the `_links` and `_steps`
  memos; completeness via `dag_blocks`), then decide and delete under the lock from the pins, sessions and
  records current then, so `get` (and `/blocks` serving) waits only for that in-memory walk and the deletions,
  and a block that became reachable meanwhile is kept. The size queries (`pinned_bytes`, `dag_bytes`,
  `source_usage`, `stat`) read their DAG nodes the same way first. `_links` memoises only stored DAG nodes: a
  node not stored yet may be stored later, and a raw block lists nothing. `pins.json` is still written under the lock on every
  pin and unpin: a pin must be on disk when `pin()` returns (names' crash consistency and `rnfs pin add`
  depend on it), and before a collection can delete what an `unpin()` released.
- `verify() -> stats` re-checks every block, deleting bad ones.
- A stored DAG node that no longer matches its CID (`DAMAGED`) has unknown children: while anything live
  reaches one, `evict()` and `gc()` delete nothing and log it once (`_held_back`) until `verify()` removes it or
  a fetch replaces it. Fetches treat such a block as not stored: deleted and fetched again.
- `dag_blocks(cid) -> set[Cid]` the blocks of a complete DAG (memoised), used for "what a node serves".

## DHT layer interfaces

`routing.py` — `RoutingTable(own_id, config, clock=time.monotonic)`:
`heard(node_id) -> bool` (proven server: announce in server mode, or answered `/find`), `failed(node_id) -> bool`
(True if dropped), `remove(node_id)`, `ban(node_id)`, `is_banned(node_id)`, `closest(key, count=K, for_reply=False)`,
`seniority(node_id) -> float | None` (seconds in the table), `is_senior(node_id, min_age)`, `standing(node_id,
min_age) -> int` (0 senior and healthy, 1 healthy, 2 stale, 3 absent: ranks keepers), `standings(min_age) ->
dict` (every node's standing now: the keeper query's snapshot from before its lookup), `contains(node_id)`,
`node_count()`.

`providers.py` — `ProviderStore(max_records, state_path, ttl=PROVIDER_TTL, clock=time.time, rng=random,
own_id=None, seniority=0)`: `store(key, publisher) -> bool`, `withdraw(key, publisher)`, `provide(publisher,
keys, withdrawn) -> int`, `retain(keep) -> int` (drops every record of publishers `keep` rejects: private mode's
non-members), `for_reply(key, exclude=lambda id: False) -> list[bytes]`, `expire()`, `record_count()`,
`allowance(publisher)`, `save()`. Local limits against flooding: one publisher holds at most max(10,
`max_records` // 100) records once seen for `seniority` (the node passes `provider_seniority`) and a tenth of
that (at least 10) before, the node's own (`own_id`) excepted. A publisher at its limit has its expired records
swept at most once per batch, and not at all before `sweep_after` (the earliest its oldest record can expire,
from its last sweep), so each further key of a refused `/provide` costs one check; a key keeps at most 1000 publishers: a newcomer to a full key evicts a random
one, never one of the `K/2` oldest records. A tenth of the store is a newcomer pool evicting by fair share: each
publisher's pooled keys sit in an ordered dict (oldest pooled first), and publishers in lists by how many they
hold (`_holders`, with each one's index, and `_most_pooled`), so a full pool picks one of the biggest holders at
random in O(1); one already holding the most is refused. The newest publisher with records outside the pool comes
from a heap of (first seen, arrival) with stale entries dropped lazily. First-seen times of departed publishers
(seen for `seniority`) are kept for a TTL, at most `max_records` // 10, in departure order; those of younger
publishers whose last record the pool evicted (`_evicted`) for a TTL too, at most `pool_size` // 10 (at least
10), forgetting the one first seen most recently when full (an O(n) scan, only on such evictions). Times later than now
(a clock corrected backwards) are clamped to now at load and at each `expire()`.

`lookup.py` — network access is injected:
`FindQuery = Callable[[node_id, key], tuple[list[bytes], list[bytes]] | None]` returning (providers, nodes).
`Lookup(key, start_nodes, query, own_id, alpha, beta, k, timeout, is_known, on_answer, on_failure)` with
`find_closest() -> list[bytes]` and `find_providers(on_provider, should_stop) -> list[bytes]`. `is_known(id)`
says whether RNS knows its identity; at most `alpha` unknown IDs are tried per lookup (`allow_unknown(id)`, also
used by the fetcher for provider IDs). `listing_count()` is how many nodes have answered with at least one
provider, `listing_nodes()` which, and `listers(provider)` which listed a given provider, so the keeper search
can tell who vouches for its candidates.

## Network layer interfaces

`server.py` — `RequestServer(destination, config, backend)`. `backend` provides:
`is_dht_server() -> bool`, `find(key) -> (providers, nodes)`, `provide(publisher_id, keys, withdrawn) -> bool`,
`served_block(cid) -> bytes | None` (only blocks in the DAG of a currently provided CID, re-verified),
optionally `served_block_size(cid) -> int | None` (so `/blocks` stops at the block over its budget unread),
`remote_pin(identity_hash, cid, pin) -> bool`, `is_member(identity_hash) -> bool` (private mode).
`NodeBackend.find` leaves out providers `RnfsNode.excluded_provider` rejects: banned, and in private mode
non-members (`_is_member_publisher`: this node, a publisher whose `/provide` the server let through as a member
this run, or a node ID whose recalled identity is a member, as routing admission checks). In private mode the
node also drops non-members' records at start (`ProviderStore.retain`).

`client.py` — `PeerClient(links, config)`:
`find(node_id, key) -> (providers, nodes) | None`, `provide(node_id, keys, withdrawn) -> set[bytes]` (identified
link, batched; the keys the node may have recorded: every key, or those of the batches up to and including the
first refused or unanswered one), `blocks(node_id, cids, on_progress=None) -> BlocksResult` (status OK / BUSY /
FAILED / STALLED; blocks verified against their CIDs; `.blocks: dict[Cid, bytes]`, `.missing` answered None,
`.unsent` cut by the budget, `.bad` hash mismatches, `.elapsed` from sending to the answer, which the fetcher
paces by), `link_mdu(node_id)` (an open link's, which the fetcher sizes batches by), `pin(node_id, cid, pin) -> bool`, `ping(node_id) -> float |
None`, `has_link(node_id)`, `close(node_id)`. `links.py` links only to the `rnfs.dht` destination of the identity
RNS recalls, and only if its hash is the node ID asked for (in private mode, also only if that identity is on
`members`); the idle sweep spares a link `LinkPool.get()` handed out within `link_idle_timeout`.

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
| `stored_head(target, length)` | a file's first `length` bytes if stored, else None; never fetches (the user's own gateway `HEAD` and ranges not at 0) |
| `read(target, offset=0, length=None, for_gateway=False, from_node=None, raw=False)` | iterator of bytes of a file (range fetched only; sealed files decrypted unless `raw` or `for_gateway`) |
| `block(cid, for_gateway=False)` | the CID's own block, verified (`cat --block`, `?format=raw`: the gateway calls it only for the user's own GET) |
| `export_stream(target, start, for_gateway=False)` | fetches the DAG, then writes its archive to `start(exact length)` (`?format=archive`; `None` for HEAD). The gateway calls it only for the user's own GET (never for a limited request or a HEAD, stored or not), inside `fetcher.observing(ClientWatch)`, which cancels the fetch once the client disconnects |
| `ls(target, for_gateway=False)` | `[{"name", "kind": "file"\|"executable"\|"directory", "size", "cid"}]` |
| `stat(target)` | `{"cid", "size", "blocks", "local_blocks", "local_bytes"}` |
| `is_local(target)` | `bool`: everything needed is stored, so no network fetch |
| `dir_set(target, name, child_cid, executable=False)`, `dir_rm(target)` | `{"cid"}`, pinned `user` |
| `pin_add(target)` | `{"cid"}` once complete |
| `pin_rm(cid)` | `None`; NotFound if no `user` source |
| `pin_ls(cid=None)` | `[{"cid", "sources": [str], "complete": bool, "type": "file" \| "directory" \| None, "sealed": bool \| None, "name": str \| None}]`; `type` and `name` only when the root block is stored, `name` where the pin was added from (pin_names; for a sealed add, the local path or the name given, kept for the local user), its shared-folder entry, or a directory's single entry or entry count; `sealed` from the file's first bytes (`seal.MAGIC`), False for directories, None if the root or first chunk isn't stored. Callers that show pins to others must skip `sealed` entries' names and content |
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
| `status()` | (API: without the token, only `{"version"}`) `{"version", "node_id", "identity_hash", "dht_mode", "slow", "private", "known_nodes", "records", "pins", "provided", "blocks", "used_bytes", "max_storage", "names"}` plus every feature's `status()` fields (traffic, jobs, LXMF); `identity_hash` is the RNS identity hash (None on offline test nodes) |

`RnfsNode.offline(config, node_id, client, names_network)` builds a node without Reticulum or background
threads, for tests (tests/test_node.py). The request server's backend is `node.backend` (`NodeBackend`).
`fetcher.py`: `Fetcher.open(root, names, from_node, pinned)` gives a `Fetch` context (a protection session)
with `resolve()`, `fetch_dag()`, `fetch_listing()`, `read_range()`; `publisher.py`: `Publisher.refresh()`
(recompute the provide set, withdraw at once), `publish_due()`, `serves(cid)`, `run()`. The node calls the
first `refresh()` only after the features have registered their key sources, so a restart doesn't withdraw
kept names' keys. `publish_due` republishes `DhtKey`s every max(effective `republish_interval`, 3 days)
with a per-key ±10% jitter (capped below `PROVIDER_TTL / 2`), drops a withdrawal of any key it provides to
the same node in that round, and on slow nodes runs at most 10 lookups per round, waiting 30 s × the
round's lookups before the next (never-published items first, then the longest unpublished). When
`refresh()` finds a key provided again, it cancels that key's queued withdrawals (their nodes join its earlier
nodes) and re-records the node's own record, and a round sends only withdrawals still queued when it reaches
each node, so a withdrawal queued while the key was briefly unprovided never follows a store of it.
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

The API requires `X-Rnfs-Api: <token>` on every request except `GET /api/status`, which without a valid token
answers only `{"version"}` (health checks, and `verify_daemon`, which reads only the proof header); the CLI sends
the token with every other request, `rnfs status` included (without a readable token it prints the version
and says why). The daemon writes a new random
token to `<rnfs dir>/api_token` (mode 0600, in the 0700 rnfs dir) at each start and the CLI reads it, so other
local users and processes can't drive the API. `_make_private_dirs` refuses to start (RnfsError) if the rnfs dir
belongs to another user or can't be made 0700. On Windows (`rnfs/winacl.py`), mode bits set no ACL, so a new
rnfs dir and its subdirectories are created with a protected DACL allowing only the user and SYSTEM (inherited
below it) in the CreateDirectoryW call itself (`winacl.make_private_dir`, a self-relative descriptor built by
`private_descriptor`), so the parent's DACL never applies to them; a new root that already holds anything, and
a root that is a junction or symlink (`fsutil.is_link` on its lstat), are refused. An existing one that allows
anyone else is given the DACL, and one owned by another account is refused; the token, identity and name key
files get the same DACL before they are renamed into place (`fsutil.write_atomic(private=True)`), and
`read_token` checks the opened token's DACL (Administrators tolerated). `rnfs.toml` (`Config.load`,
`winacl.secure_settings`) and the identity (`network/identity.py`) are checked through the handle they are
read from (`winacl.tighten(fd=...)`): another account's is refused, a loose one tightened with a warning, and
a failure is fatal. A `--dir` outside `%USERPROFILE%` is warned about. The advapi32 and kernel32 calls are
behind `winacl.backend`, which tests replace with a fake. A response to a request with
`X-Rnfs-Challenge: <32 hex>` carries `X-Rnfs-Proof: cli.api_proof(token, port, challenge)` (HMAC-SHA256 of
`rnfs api proof <port> <challenge>`); `ApiClient.verify_daemon` checks it with `GET /api/status` before first
sending the token. Both HTTP servers (`TimedHandler`) time out slow requests, cap concurrent connections (API
32, gateway 64) and refuse HTTP/0.9 with a 400 carrying the usual headers. Gateway errors raised by the node
(anything but `GatewayError` and `NotFound`: 400, 413, 422, 503, 507, 500) say only a fixed message per status
(`gateway.ERROR_MESSAGES`; the detail goes to the log); malformed `Range` headers are ignored.

Neither server sends CORS headers. Gateway pages run sandboxed in an opaque origin, so they load their DAG's
files as subresources but can't read any gateway response with `fetch()`; an earlier rule allowing a gateway
`Referer` never fired, as browsers send no `Referer` from an opaque origin, and allowing `Origin: null` instead
would let any site's sandboxed frame read the gateway. A separate origin per root (a subdomain per CID) is the
possible future way to let pages read their own DAG.

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
  is sent with a Content-Length before streaming. For every format, a web page's request or a HEAD always
  gets 409, stored or not (a 200 download for stored content only told pages what is stored: L6), before the
  node is asked anything, so formats never take a `gateway_fetches` slot; a bare-CID `If-None-Match`
  revalidation answers 304 first.
- `content/seal.py` rejects a final segment of more than 65536 plaintext bytes, and `SealedFile.name` is None
  for a name that fails its tag, isn't UTF-8 or isn't a valid name. `write_sealed` reads the name before
  creating the output, so nothing raised there leaves a file. `open_sealed` reports the stored (ciphertext)
  size as a job's total, which is what its done bytes count.
- Cost: `get`, `cat` and `info` of a file read its first 12 bytes to detect sealing (`--raw` skips this), so
  the first chunk is fetched first even for a `cat --offset` deep in a file. The gateway does so only for a
  GET from byte 0 or a web page's request (`limited()`, so its type never depends on what is stored); the
  user's own HEAD, or range not at 0, uses the first bytes only if already stored (`stored_head`), else takes
  the Content-Type from the name, or `application/octet-stream`.

## Mesh

`mesh.py` (no RNS at module level): `PathInfo`, `provider_rank` and `RnsPaths` (hops and next-hop
interfaces from `RNS.Transport`, or through a shared instance's RPC with its interface stats cached for 30 s);
the airtime policy (`TokenBucket`, `AirtimePolicy`, interface matching, quiet-hours parsing, all with
injected clocks); and `Traffic`, thread-safe counters per peer (LRU, at most 1000), per interface (at most 64)
and in total, never persisted.
- `Fetcher.path_info` (default: nothing known) is set by the node to `RnsPaths.info`; `Fetch._rank` orders
  both providers and open-link probes, and each provider caches its `PathInfo`.
- `ResponsePools` has `max_blocks_responses // 2` slow-link slots and `fast_blocks_responses` (or the rest, if
  more) for others. It protects a response from displacement only while it is within a grace it measures from
  the link's path (`link_path`, `fast_path`, `response_grace`, never from `link.rtt`), or moving at or above a
  floor (`progress_floor`). `link_path` treats the interface to a shared instance (`via_shared_instance`: RNS's
  `is_connected_to_shared_instance` attribute, else the `LocalClientInterface` class name) as of unknown
  bitrate. `response_grace` prices the first hop at the interface's bitrate and the others at
  `unmeasured_bitrate`, or `slow_bitrate` when `fast_path` (the link's MTU is above 500, which RNS allows only
  across interfaces that configure their MTU), and adds `RequestServer._queued_bits` on the node's own slow
  interface (a window of parts per response in flight there). A full pool displaces the least-progressing
  unprotected response; else (`_fairness_pick`), for a newcomer whose interface holds no slot, the oldest
  zero-progress response past half its grace from an interface holding two or more, at most once per grace
  per interface (`fairness_at`); else, for a newcomer whose link and identity hold no slot, the slowest
  response older than `deadline_slack` and past its grace, marked idle if `Response.far_behind` (below a
  quarter of `needed_rate`, measured against `Response.due`). Never one on the newcomer's own link.
  `check_responses` cancels and refunds a response that has sent nothing once its grace plus `deadline_slack`
  has passed, or that makes no progress for `stall_timeout`. `Cooldowns` then refuses `/blocks` from a link
  (and its identity, if identified) whose response was displaced or cancelled idle, for `blocks_cooldown`,
  cheaply and before any slot is taken. `_concluded` starts the same
  cooldown for a response whose Resource the requester rejected (`REJECTED`) with nothing sent (`_sent_any`:
  `Response.sent`, else the Resource's `get_progress()`).
- `IncomingLinks.take_token` keeps a per-link token bucket (`REQUEST_BURST`, `REQUEST_RATE`) and a smaller
  one of refusals (`OVER_RATE_REFUSALS`, one a second) in `_LinkState`. `RequestServer._guarded` asks it first
  for every path: over the limit it answers `False` while refusals are left, else `None`, which RNS sends
  nothing for; `_over_rate` counts them and logs at most once a minute.
- `RequestServer.airtime` checks interface rules, quiet hours and an empty budget before taking any slot,
  caps the block budget, and charges the bucket as responses are sent. Each `Response` records its `cost`
  (packed size), what has been `charged` so far and its requester (identity hash, else link ID). `_admit`
  (one admission at a time) allows a response if the bucket holds its cost beyond `_committed()` (the sum of
  other responses' `Response.commitment`: the uncharged rest within the grace; after it, what the progress
  rate sends before the deadline, or nothing below the floor), and charges a
  reservation of at most the link's MDU; single-packet and untracked responses are charged in full.
  `check_responses` charges progress as it reads it (`_charge_progress`), and `_settle` (from `_finish`)
  makes the charge final: the cost if completed or progress is unreadable, else progress × size, refunding
  an unused reservation. One requester's responses in flight may cost at most half the bucket's capacity
  (`_budget_share_left` cuts the budget, or refuses).
  `RequestServer.traffic` and `PeerClient.traffic` share one `Traffic`.
- `network/guards.py` also rejects, on `rnfs.dht` links only (recognised by the link's destination or owner
  name `rnfs.dht.<hex>`), any Resource advertisement with more than one segment or with metadata.
- `guards.install()` also replaces `RNS.Link.response_resource_concluded` and `RNS.Link.receive` (class
  attributes; RNS looks both up on the link). On `rnfs.dht` links, `guarded_response_resource_concluded` reads
  a completed response Resource's data (an open file, rewound for RNS) and `guarded_receive` decrypts a
  response packet larger than `PACKET_CHECK_SIZE` (RNS's default MTU), and both pass it to
  `protocol.response_refusal`, which walks msgpack headers (`_array_at`, `_bin_at`) without building
  objects: `[16-byte bin, response]`, the response `True`, `False`, `nil`, a `/find` answer or at most
  `MAX_BLOCKS_CIDS` (26) entries of `bin` up to `MAX_BLOCK_SIZE` or `nil`, and nothing after it. A refused
  response is handed to `Link.handle_response` as `None`, which every parser refuses, so the request fails as
  malformed; refusals are logged at most once a minute. `guarded_receive` drops a response packet on an
  incoming `rnfs.dht` link without decrypting it. Everything else goes to RNS's own methods unchanged.
- `guards.limit_half_open(destination, limit)` replaces a destination's `incoming_link_request` (on the
  instance; RNS's `Destination.receive` calls it for each link request) with `HalfOpenLimit`, which, before
  RNS validates a request, closes (`Link.teardown()`) the oldest `PENDING`/`HANDSHAKE` link in
  `destination.links` on the interface holding the most until fewer than `limit` remain. `RequestServer`
  applies it to `rnfs.dht` and the LXMF feature to its delivery destination, both at `max_incoming_links`.

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
at most 64 messages per download. `LxmfFeature.trusted()` is true for senders in `lxmf_accept_from` or
`lxmf_auto_fetch_from` and LXMF contacts (`ContactsFeature.has(address, "lxmf")`, an index kept beside the
address book). Messages from unknown senders are held (`_hold`: at most 32, 8 per claimed sender, a ninth
claiming one taking the place of its oldest; further ones dropped, and none from senders `accepts()` would
ignore, but a trusted sender's takes the place of the newest held for the untrusted sender holding the most) while their identity is requested: one path request per sender
per 2 minutes, from a node-wide token bucket (burst 3, then one per 10 s), trusted senders' held messages
retried first. A held message is dropped 30 s (`HOLD_PATH_WAIT`) after its sender's path request, or its own
arrival if later, with no identity known, and after 2 minutes in any case. A held message stays in the queue
while it is retried, so it still counts as a duplicate and against the limit. The inbox (`MessageLog`) holds at
most 500 records and 4 MiB as JSON, 50 per sender not in `lxmf_auto_fetch_from`, 16 KiB per record (links
beyond it dropped, then text cut), and never evicts a record holding auto-fetched pins for one that holds none;
`add(reserved=)` keeps trusted senders' records from being evicted except for each other's (untrusted ones go
first), so free identities can't push them out, and untrusted senders' new records come from a per-sender
allowance (`SENDER_BURST` 4, then one per `SENDER_INTERVAL` 15 min; `_sender_tokens`, the last
`SENDER_ALLOWANCES` 256 senders) and then a node-wide token bucket (`UNTRUSTED_BURST` 32, then one per
`UNTRUSTED_INTERVAL` 60 s), both on `LxmfFeature.clock`; a message refused by either takes from neither. The pin's
"pinned" state is recorded in the same locked section that pins. Auto-fetched `name/` links resolve with
`persist=False`; each auto-fetch runs under a `fetcher.observing` observer cancelled when the node stops or the
message is removed, and links still "queued" in the inbox are queued again at `start()`
(`_requeue_auto_fetches`, within the 64-entry queue). The queue (`_RoundRobin`) keeps one FIFO per sender and
takes senders in turn; when it is full, a sender with at least two fewer waiting than the busiest takes the
busiest one's newest place (that link is marked skipped). Links without the field are read from the first
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
| `rnfs-join`, `rnfs-publisher`, `rnfs-names`, `rnfs-maintenance` | node | joining (and joining again while under `MIN_SERVERS`), publishing, name announces, periodic saves and eviction |
| `rnfs-announcer`, `rnfs-response-monitor`, `rnfs-link-sweeper` | network | rnfs.dht announces, `/blocks` deadlines and displacement, idle links |
| `rnfs-lookup`, `rnfs-fetch`, `rnfs-find-providers`, `rnfs-remote-pin` | per operation | lookups, fetch workers, provider search, remote-pin fetches (one thread per source with pins waiting) |
| `rnfs-write` | storage | an add's, shared-folder import's or archive import's block writes (`Session.writer()`), up to 16 at once, ended with it |
| `rnfs-job-<id>` | jobs | one per running job, at most `max_jobs`; `stop()` waits ≤ 2 s |
| `rnfs-keepers`, `rnfs-keeper-query` | keepers | pending refreshes (≤ 64 queued), own-name sync, saves, re-asking every `announce_interval`; parallel `/name` queries |
| `rnfs-follow` | follow | every check and fetch, one at a time |
| `rnfs-lxmf`, `rnfs-lxmf-fetch` | lxmf | announces, propagation-node downloads, polling outbound states each second; auto-fetch, one at a time, senders in turn (LXMF runs its own job thread) |

The API and gateway run one thread per connection, capped (32 and 64).

Locks are taken in this order, never the reverse: `names._lock` → `publisher._lock` → `node._lock` →
`storage._lock`. `NameManager.publish`, `remove` and `release_gone` pin and unpin, and so call
`publisher.refresh()`, while holding the names lock; `Publisher.refresh` recomputes what is provided under its
own lock, reading the node's fetched targets (`node._lock`) and walking the pins' DAGs (`storage._lock`); the
node reads storage under its lock (`_prune_fetched`). This is how the code behaves today, not a design goal.
State files written in full are written outside these locks, from a copy taken under them, one save at a time
behind a save lock taken first (`Storage.save`, `Publisher.save`, `ProviderStore.save`, the name floor,
`fetched.json`, `remote_pins.json`), so an older copy never lands last. Slow work that runs while a lock is
held:

- `Publisher.refresh` checks every pin's DAG (`dag_blocks`, memoised) under the publisher lock, behind the
  names lock when a name changes. It builds the set of blocks served again only when a provided CID went or its
  DAG's blocks changed (CIDs only added are unioned in), so a refresh that changed nothing doesn't recompute it.
- `Storage.evict` and `gc` decide what to delete, and delete it, under the storage lock (after reading the DAG
  nodes outside it); `verify` checks completeness and re-reads the blocks outside it.
- `pins.json` is written, synced to disk, under the storage lock on every pin and unpin (see `storage.py`).

## State files

Under `<rnfs dir>/state/`, JSON written atomically from a temp file synced to disk: `pins.json`, `cache.json`
(storage); `pin_names.json` (rnfs/pin_names.py: where each pin was added from, local only, pruned to the pin
list); `providers.json` (provider records); `published.json` (publisher: last rounds, earlier rounds' nodes,
pending withdrawals); `fetched.json` (targets fetched for the user, for `roots`); `remote_pins.json` (remote pins accepted and
not yet fetched, `[[source, cid], ...]` in order); `names.json` (sequences, name
table); `name-floor.json` (`[[name, sequence, resolved], ...]`: highest sequence of names resolved for the
gateway or auto-fetch and when, at most 4 × `name_table_size`, or 8 × for names resolved within 7 days; saved by
a resolve raising it, at most every 5 s, and by the re-announce loop when changed; entries without the time load
as resolved long ago); `invalid.json` (invalid roots); `shared_folder.json`; `jobs.json` (results keep at most 100 warnings
plus `warnings_total`; older results are capped at load); `keepers.json` (records re-verified at load);
`follow.json`; `contacts.json`; `lxmf_inbox.json` (500 messages, 4 MiB, 50 per non-auto-fetch sender, 16 KiB per
record) and `lxmf_outbox.json` (200; unfinished messages are marked failed at a restart), both UTF-8 JSON with
non-ASCII characters written as is. Elsewhere: `storage/identity`, `storage/lxmf_identity` (0600),
`names/<alias>` (name keys), `lxmf/` (LXMF's ratchets, delivered-message IDs, stamp costs), `api_token` (while
the daemon runs). Malformed records in feature state files (wrong types, duplicate IDs) are dropped at load
with a warning. Outputs being written live beside the output as `.rnfs-get-<random>.partial` (`RnfsNode.get`)
or `.rnfs-job-<id>.partial` (get jobs) until renamed; a failed, cancelled or shut-down job removes its partial,
and a resumed job removes one left by a crash.

## Tests

The commands CI runs the tests with are in `.github/workflows/tests.yml`. Feature tests live in their own files
(`test_jobs.py`, `test_mesh.py`, `test_names_keep.py`, `test_contacts.py`, `test_seal.py`, `test_chunker.py`,
`test_pin_false.py`, `test_gateway_formats.py`, `test_lxmf.py`), and `test_e2e.py` has a marked block per
feature. CI (`.github/workflows/tests.yml`) runs them on Linux, macOS and Windows; `packaging/smoke_test.py`
checks a built package (`--version`, `--help`, daemon and status).

## Status

Everything in the specification is implemented, with these simplifications and additions:
- The routing table isn't persisted. At startup it is seeded from the rnfs.dht server announces that RNS
  cached within 2 × `announce_interval` (`Identity.known_destinations`, an RNS internal), then the join runs.
- Joining (`RnfsNode._join`, `join`): bootstrap nodes are asked one at a time with `PeerClient.find`, outside
  any `Lookup`, until `BOOTSTRAP_ANSWERS` (2) answer; the lookup that follows excludes them. The thread then
  checks `RoutingTable.live_count()` every `REJOIN_DELAY` (300 s) and joins again while it is under
  `MIN_SERVERS` (3), doubling the wait up to `announce_interval`. `DEFAULT_BOOTSTRAP_NODES` lives in
  `rnfs/constants.py` and changes only with a release; the e2e test turns it off (`default_bootstrap = false`).
- Publishing runs one closest-nodes lookup per due CID, one after another. Lookups aren't shared between keys
  that have the same closest nodes, but `/provide` requests are batched per node. Slow nodes pace these
  lookups (at most 10 per round, 30 s per lookup on average), which only delays republishing; withdrawals
  are sent at once.
- Withdrawals are sent once, with any provides due for the same node. One that fails isn't retried, since the
  record expires after `PROVIDER_TTL`. A CID is withdrawn from the nodes of its last round and from nodes of
  earlier rounds, or of a round cut short, that may still hold its record (`Publisher._earlier`, persisted in
  `published.json`, forgotten after `PROVIDER_TTL`). Each node that accepts a key is added there as its
  `/provide` returns, and `published.json` is saved every 10 nodes or 30 s of a round
  (`SAVE_EVERY_NODES`, `SAVE_EVERY_SECONDS`), so a crash loses at most that much of where a round stored.
  A round stopped before every node was sent its keys leaves its CIDs due, and persisted times later than now
  are clamped to now.
- A CID whose lookup found no nodes to publish to is retried after 30 s rather than at the next republish.
- When every provider has failed, the provider lookup runs again only if its previous run found a new
  provider (and time is left).
- `max_download_size` is checked against the target's verified size, or the range's size, before anything
  below the target is fetched, and the DAG-node bytes a fetch downloads count against it too, so directories
  of empty files can't get around it. Listing and writing out also stop fetching at `max_output_entries`.
- **DAG byte bound** (`fetcher.dag_bound`, `Fetch.dag_limit`). `fetch_dag` counts the bytes of the distinct
  blocks of its target's DAG, stored or downloaded, against `dag_bound(size)` = size + `DAG_GRANULE_OVERHEAD`
  (64) per `DAG_GRANULE` (4 KiB, `SmallLeaves`' threshold) of the declared size + `DAG_SLACK` (1 KiB), plus
  what the counted directory and index nodes bring (`node_credit`): a directory node its own bytes and
  `DAG_ENTRY_OVERHEAD` (16) per entry, an index node its own bytes and 16 per child less `INDEX_NODE_COST`
  (64), never below 0; and `max_dag_bytes` (a remote pin's or auto-fetch's quota) if lower. Over it is
  `TooLarge`, not `InvalidDag`. Every caller gets it, since it lives in `fetch_dag`: `get`, `pin_add`,
  `dir_set`/`dir_rm`, `export`, `export_stream`, follow, remote pins and LXMF auto-fetches. Why the builder's
  output never comes near it: a file node child is at most 51 bytes (`[cid, size]`: 39-byte CID, 9-byte size)
  and a node header at most 7; fixed chunks are 64 KiB and FastCDC chunks over 16 KiB except a file's last,
  so with the levels above (at most n/255 + 8 nodes for n chunks) a file takes under 13 bytes per 4 KiB plus
  under 600 bytes (its last chunk and up to 8 levels). A file with a node has two chunks or more, the first
  at least 16 KiB, so its 64 bytes per 4 KiB (at least 256 per chunk but its last) cover both, inside a
  directory as for the target itself. A directory node pays for itself, so an entry costs nothing whatever
  its name. An index node is met before the entries below it, which is why its children bring their 16 bytes
  in advance: one of 4 children or more pays for itself, and the tree rule fills every index node but the
  last of its level, so an honest directory meets at most 48 bytes of index nodes per level (at most 7 levels)
  before its entries arrive, inside `DAG_SLACK`, and about one index node per 65,536 entries after that. An
  empty file is the 0-byte raw block and an empty directory 3 bytes, the same block each time. Counting is
  incremental: a node's credit is added when its block is counted, before anything below it, and the chunks
  are counted against the declared size, so the bound holds at every block, not only at the end
  (tests/security/test_dag_caps.py fetches large, FastCDC, deep, wide and empty trees: they use at most 8%
  of the bound's margin, the part neither the content nor nodes paying for themselves take). What it
  refuses: zero-size children (audit L3), chains of index nodes (48 bytes a level), duplicate entries
  carrying other blocks (each brings 16 bytes beyond its own: a 0-byte directory carries at most that much
  of other nodes, where 1 KiB per entry used to let it carry 20 times its own bytes), and DAGs of chunks well
  under 4 KiB, which no writer following the tree rule makes. The credit isn't capped at
  `max_output_entries` entries, so a pin of an honest directory of any size is allowed (`get` and `export`
  still stop at `max_output_entries` entries); a directory's blocks are then about what an honest directory
  of as many entries takes, and `max_download_size` and quotas bound them. To keep the overshoot to about
  one block, `_take_batch` asks only for what fits the room left after the requests in flight (`_room`,
  `_reserve`): a raw chunk's declared size, anything else `MAX_BLOCK_SIZE`, except that under the
  size-derived bound (not under a quota, which entries don't raise) a directory or index node takes nothing,
  since it costs at most 64 bytes (a request of `MAX_BATCH` of them overshoots by at most 2 KiB); and always
  one CID when nothing is in flight. Honest fetches are not held back by this: the content still to come
  leaves room for many file nodes, and directory nodes cost nothing.
- **Listings** (`fetch_listing`: `ls`, the gateway's directory pages) fetch only directory and index nodes,
  and are bounded as `fetch_dag` is for a declared size of 0 (`_declared` = 0): `DAG_SLACK` plus the nodes'
  credit. It used to count index children per level only, so 40 entries below chains of single-child index
  nodes down to `MAX_DEPTH` took 751 KB; now a single-child index node costs 48 bytes, and those chains are
  refused after 24 KB.
- **Byte-range reads** (`read_range`: `cat`, the gateway's file responses, `/api/cat`, a sealed file's
  header and its decryption by `get`, `cat` and `info`, which never run `fetch_dag`) count the bytes of the
  distinct file nodes each call meets, stored or downloaded, against `range_bound(size, length)` =
  `dag_bound(length)` + 2 × `file_levels(size)` × `FILE_NODE_MAX` (13,061 bytes: a header of 5 and 256
  children of 51). Nodes wholly inside the range cost no more than in a whole file of `length` bytes; on each
  level the range ends inside at most two more; and an honest file of `size` bytes, cut into chunks of 4 KiB
  or more, has `file_levels(size)` levels: enough 256-wide levels for size / 4 KiB + 1 chunks, one up to
  1 MiB, two up to 256 MiB, seven for any size. Raw chunks aren't counted: they hold the range and at most
  two chunks more. A file node padded with empty children made `cat` of a file declaring 1 byte download
  7.3 MB (and a sealed `get` of 12 KB 29 MB); now the read stops at about 27 KB plus one block, and at most
  about 184 KB plus one block for any declared size. The budget (`_RangeBudget`) is set only while
  `_range_leaves` ensures a node's children, since another read of the same fetch may run between a
  generator's steps; `_reserve` counts a file node not yet received as `MAX_BLOCK_SIZE` against it, so a
  small honest range ending inside two nodes of a level asks for them in two requests instead of one.
- `max_storage` is soft. An unpinned fetch evicts other cached fetches (LRU) to make room for each response
  and goes above `max_storage` when nothing is evictable; when its session ends the node evicts again, which
  may evict what it fetched if pins alone fill `max_storage`. Any fetch is refused with `StorageFull` ("not
  enough free disk space") if its remaining size, the range's size, or a response would leave less than
  max(256 MiB, 2%) of the blocks' filesystem free (`Storage.check_disk`, `DISK_RESERVE_MIN`/`_FRACTION`).
  `get` and `export` also check free disk space at the destination first (`node._check_free_space`): the
  output must leave the same reserve free there, and when the destination is on the blocks' filesystem, `get`
  (and a sealed file's write-out) counts what it still has to fetch too (the verified size less the bytes of
  the target's DAG already stored). If the destination's free space can't be read, the check raises the
  `OSError` (same errno, saying so) instead of letting the write go ahead unchecked.
- Roots found invalid are remembered in `state/invalid.json` (at most 10 000, oldest dropped), and there is
  no command to clear them. Cache records are capped at 10 000 (LRU). Both are saved every 30 s and at stop.
- A remote pin's usage is the larger of its content size and its DAG's block bytes. It is dropped once the
  root block shows the quota would be exceeded, or once the blocks fetched exceed it. A source's remote pins
  are fetched one at a time, oldest first (`_remote_pin_worker`), so the quota check counts every earlier pin;
  at most 64 (`REMOTE_PIN_QUEUE`) may be waiting or fetching, and further `/pin`s are refused. A worker leaves
  `_remote_workers` in the same critical section in which it finds its queue empty, so a `/pin` arriving as it
  exits starts a new one; if `Thread.start()` fails the pin is given back and refused. The pins waiting are
  saved to `state/remote_pins.json` by maintenance (when changed) and at stop, and `_resume_remote_pins` queues
  them again at start (skipping those already pinned or whose identity left `remote_pin_identities`); workers
  wait for the first join before fetching, and a fetch cut short by stopping stays queued.
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
  inode checks), so entries swapped for symlinks, FIFOs or devices mid-scan are skipped. The scan (not `add`)
  skips regular files whose opened descriptor has `st_nlink > 1` (`builder.hard_links`, `build_open`'s
  `skip_hard_links`); such a file's signature is marked, so it is imported once its other links are gone.
- The identity file (`network/identity.py`) is opened with `O_NOFOLLOW | O_NONBLOCK` after an lstat that must
  show a regular file, not a link; the descriptor must be that same regular file (device and inode), owned by
  the user, and is tightened through `fchmod` if loose. The key is read from it (`RNS.Identity.from_bytes`),
  never by path again. Anything else raises `IdentityFileError` (an RnfsError), which stops the daemon with
  its message.
- `get` (`walker.write_out`) skips and reports names the destination filesystem refuses to create (EINVAL,
  ENAMETOOLONG, EILSEQ: e.g. `?` on FAT, long names on eCryptfs) rather than failing the whole write-out.
- On Windows (`rnfs/fsutil.py`, `IS_WINDOWS`): there is no execute bit, so `add` records every file as kind 0
  and `get` sets none; Unix permission checks are replaced by access-list ones (`winacl`, see the API
  section). Directories are walked by path rather than descriptor, each entry's type and file ID checked
  before use, and directory junctions (and other name-surrogate reparse points) are skipped like symlinks.
  Each directory is checked again whenever it is listed (`builder.list_directory`: same file ID, not a link,
  real path inside the walk's root), and each file opened below it must have its final path
  (`fsutil.final_path`, GetFinalPathNameByHandleW) inside that root, so a directory swapped for a junction
  mid-walk can't lead outside it (audit L8). Replacing or deleting a file another handle
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
  sequence `NameManager` holds for the name. `ask_keepers` takes `RoutingTable.standings` before its lookup and
  uses the worse of that and the current standing (`_standing`), so nodes the lookup adds to the table don't
  gain rank; "established" is standing 0. It searches for 3 × alpha candidates from two established sources
  (`Lookup.listing_nodes`, plus this node if an established publisher stored a record here), ranks them
  (`rank_keepers(candidates, before)`: standing, then a random tie-break), and asks batches of alpha: each
  batch first takes, in rank order, candidates with a source no earlier pick had (`vouched`: established
  listers from `Lookup.listers`, this node for local records, else the shared `_UNVOUCHED`), then fills in
  rank order. Unknown keepers come from a budget of alpha of the query's own (`may_ask`). Batches go on until
  an answer reaches the highest known sequence (`NameManager.record` or the kept record) from two keepers
  (two established ones when no sequence is known), at most `keeper_rounds()` batches (3; 2 on slow nodes,
  where one current answer is enough once a sequence is known). On a slow node that is still at most two
  `/name`; a fast node with no known sequence and no established keepers asks all 3 batches (up to 9), and the
  lookup runs to completion unless two established nodes list keepers.
- **Names.** A key imported, or found without an entry in `names.json`, is marked `imported`: its first publish
  offers kept records, makes a path request and queries keepers before choosing a sequence. `publish()` pins
  the new target, saves `names.json` (the commit point; a failed save rolls the record back and releases the
  new pin) and only then releases the old target, so `names.json`'s target is pinned at every step. Once the
  features are set up, `reconcile_pins(storage.pins())` releases the pins of name keys deleted by hand
  (`release_gone()`), pins a recorded target again if its pin is missing, and releases `name:` pins nothing
  records; a key without a `names.json` entry keeps its pins until its next publish or removal (`_stale`,
  where a release that failed with `OSError` also waits). A key file that is there but can't be loaded is
  `_unreadable`: its pins are kept and its `names.json` entry is written back as loaded. `Storage.pin`/`pin_many`/
  `unpin` undo their change in memory if saving `pins.json` fails. Follow and LXMF do the same for their sources at
  start (`FollowFeature._reconcile`, `_release_unrecorded`), but release only if their file loaded whole
  (`MessageLog.clean`); otherwise unrecorded pins are recorded again (the name followed again, or an inbox
  message per sender listing them), so a later start keeps them too. The name table marks
  entries that were only heard (`_heard_only`, its own LRU, so finding one to evict is O(1)); an announce with
  no waiting resolve, of a name without a kept record, may only evict one of those. A resolve with
  `persist=False` (gateway, LXMF auto-fetch) raises the name's floor (`_floor`: name -> (sequence, wall time
  last resolved), in resolve order), which `_offer` checks. A raise (or a new name) saves `name-floor.json`
  at once if the last save was `FLOOR_SAVE_INTERVAL` (5 s) ago, else from a one-shot `threading.Timer`;
  renewing only the time marks it for the next save after an hour. `_trim_floor` drops the least recently
  resolved past `FLOOR_FACTOR` × `name_table_size`, but stops at one resolved within `FLOOR_KEEP_AGE` (7 days)
  until the floor holds `FLOOR_HARD_FACTOR` (2) times that.
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
  can't be found within 2 minutes (30 s after an unanswered path request) is lost, as is one the hold queue
  or the rate limit on untrusted senders has no room for: LXMF has already recorded it as delivered and the
  sender has its delivery proof. A steady stream of forged messages can still crowd a first message from an
  untrusted sender out of the hold queue, and messages with rnfs links from new identities can use up the
  untrusted senders' rate limit; trusted senders (contacts, `lxmf_accept_from`, `lxmf_auto_fetch_from`) are
  not held back by either, though forgeries claiming one of them can take its 8 hold places until its
  identity is known. One auto-fetch runs at a time, so a long one still delays every sender's next. Links
  sent carry the node ID as `from` unless `lxmf_send_from_hint = false`; the default display name
  (`rnfs node`) doesn't. No propagation node is used unless configured (or asked for with `--nearest`).
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
