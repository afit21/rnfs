# rnfs implementation design

How the code is organised to implement [specification.md](specification.md). The spec is the
authority on behaviour; this file fixes module boundaries and the interfaces between them.

## Shared foundation

- `rnfs/constants.py` — protocol constants (spec section 12), request paths, aspects, DHT modes.
- `rnfs/config.py` — `Config`: every local setting (section 12), paths under `<rnfs dir>`, and the `effective_*`
  helpers for settings that depend on slow nodes or links. Loaded from `<rnfs dir>/rnfs.toml` plus command-line
  overrides (and `defaults` beneath the file: an embedded node's); `validate()` checks every core setting's type and
  range (`WHOLE_NUMBERS`, `RATES`, `DURATIONS`, `FLAGS`, `IDENTITY_LISTS`), and the daemon exits on an invalid one.
  Reticulum is started from `reticulum_dir`, by default RNS's own config directory, so the node attaches to `rnsd`
  with the matching RPC key.
- `rnfs/errors.py` — `RnfsError` and subclasses `InvalidDag`, `NotFound`, `TooLarge`, `StorageFull`, `Busy`,
  `Denied`, `Cancelled` (a fetch stopped by its observer), `Closed` (an `rnfs.Node` or `rnfs.Client` used after
  `close()`).
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

- `rnfs/statedb.py` — the state database, `<rnfs dir>/state/rnfs.db` (standard `sqlite3`, WAL mode,
  `busy_timeout`; created 0600, which SQLite copies to its `-wal` and `-shm` files; on Windows the state
  directory's access list covers them). `connect(state_dir)` hands each store a reference (`StateDb`) to one
  shared connection per directory, the last `close()` closing it; opening runs `quick_check` and refuses a
  damaged database, or one whose `user_version` is past the schema steps this code knows (`SCHEMA`, applied
  one transaction each), saying what to do. `transaction(durable)` is the only way to write: `durable` commits
  with `synchronous=FULL` (on disk when it returns), otherwise `NORMAL` (a power loss may lose the last commits,
  never part of one or one without those before it); a sqlite failure raises `StateDbError`, an `OSError`.
  Text that may hold lone surrogates (file names, remote strings) goes through `dump`/`load` (JSON text);
  sequences are decimal text, since they pass SQLite's 64-bit integers. Without `_sqlite3` the daemon stops
  with a message saying so.

Persistent state lives in the state database. Each store keeps its state in memory, which is all reads use,
loads it once at startup (checking every row as the JSON loaders checked every record: a malformed row is
logged, skipped and deleted) and then writes only what changed: a pin is one row inserted or deleted, batched
stores write the records changed since their last save. At every `connect`, `statedb.migrate` first imports
the JSON state files of 0.1.4 and earlier (`LEGACY`): each through its owner's importer, which checks every
record as the old loader did, in one transaction that also records the file in the `migrated` table; then the
file is renamed `<name>.migrated` and the directory synced. A file the table already lists (a crash between
the commit and the rename) is only renamed, so none is imported twice or by half. The files are read with
`statedb.read_json(path, shape)`, which treats only a file that isn't JSON, fails `shape` or is a directory as
damaged: it is moved to `.damaged` (`.damaged.1`, … keeping earlier copies) and the node starts without it; any
other `OSError` stops startup, since starting without `pins.json` would lose every pin (`names.json` stops it
when damaged too, as it always did: starting afresh would allow rollback). Everything shared between threads
locks internally.

## Layout and ownership

```
rnfs.py, rnfs/__main__.py   entry points (python3 rnfs.py, python3 -m rnfs); the installed command is rnfs.cli:main
rnfs/
  constants.py config.py errors.py log.py          shared foundation
  debuglog.py         the debug log file, <rnfs dir>/debug.log (debug_log)
  fsutil.py           cross-platform file operations: replace/remove retried on Windows, O_BINARY, IS_WINDOWS
  statedb.py          the state database, <rnfs dir>/state/rnfs.db: schema, transactions, JSON state migration
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
    routing.py        RoutingTable: buckets, seniority, failures, unreliable nodes, bans (section 6)
    providers.py      ProviderStore: records, TTL, max_records eviction, /find reply selection (section 6)
    lookup.py         iterative lookups with alpha/beta, time budget, unknown-ID budget (section 6)
  network/            everything that talks to RNS
    protocol.py       message encoding/validation, batch sizes (sections 3, 6, 7, 8)
    rates.py          rate estimates, slow links, slow node detection (section 2)
    links.py          LinkPool: shared unidentified links, identified one-shot links, idle close (section 3)
    guards.py         guards rejecting oversized, compressed or request Resource advertisements (on every
                      link in the daemon, on rnfs.dht links only in an embedded node) and, on rnfs.dht links,
                      responses that fail a msgpack header check before RNS decodes them (3, 10); the
                      half-open incoming link cap per destination (3)
    server.py         RequestServer: handlers, every section 3 limit, airtime policy, feature paths (3, 7)
    client.py         PeerClient: /find, /provide, /blocks (with stall detection), /pin, ping (sections 3, 6, 7)
    announces.py      rnfs.dht announces carrying the DHT mode, and the announce handler (section 3)
    identity.py       node identity file, node ID helpers
  mesh.py             provider ranking (PathInfo, provider_rank, RnsPaths), airtime policy, Traffic (3, 6)
  names.py            NameManager: name keys, records, publish, resolve, sequences, keeper hook (section 9)
  denylist.py         DenyList: denied CIDs and file hashes, local entries, accepted lists (section 8)
  fetcher.py          fetching a target from providers; observing() for job progress (sections 6, 7)
  publisher.py        provide strategy, batched /provide, republish, withdrawal, extra key sources (section 6)
  node.py             RnfsNode: wires everything; the facade below
  node_content.py     node side of --no-pin, sealing/unsealing, raw blocks and archive streams
  api.py              local API, 127.0.0.1:8788 (section 11)
  apiclient.py        the local API's protocol layer (ApiConnection) under rnfs.Client and the CLI: token, proof
                      check, typed errors (section 11)
  objects.py          the public API's typed objects (Added, Entry, Info, Record, ...); no RNS
  interface.py        the public API both classes share (Rnfs, Names, Jobs, Deny, File, Watch); no RNS
  embedded.py         rnfs.Node: a node inside a program (imports RNS)
  remote.py           rnfs.Client: the daemon through its API (no RNS)
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
    deny.py           deny_lists sources: fetched, checked, followed; rnfs deny (section 8)
examples/             rnfs_share.py: the public API with either class
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
  raising `InvalidDag`. Nodes are immutable and decoding is a pure function of the bytes, so `decode` remembers
  the nodes of the blocks it decoded last (`MEMO_BLOCKS` 256, `MEMO_BYTES` 2 MiB of blocks, least recently used
  forgotten first; a block that fails is checked again each time): a read meets the same nodes many times.
  `check_name(name)` raises `ValueError`.

`walker.py` — reader checks are driven by an **expectation** of what a child must be:
- `Expect(cid, size: int | None, types: frozenset[str], depth: int, first_name: str | None, below: str | None,
  kind: int | None)`, frozen. `types` ⊆ {"raw", "file", "directory", "index"}; `first_name` is the name the first
  name below must equal; `below` an exclusive upper bound for every name below; `kind` a directory entry's kind
  (`dag.KIND_*`), so `write_out` can give an executable target the execute bit. `root_expect(cid)` allows any
  type at depth 1.
- `expand(expect, block) -> list[Expect]` — check a verified block against its expectation (type, size, names,
  index bounds, depth) and return its children's expectations. Raises `InvalidDag`. Raw blocks have none.
- `step(expect, block, name) -> Expect | None` — one path step in a directory or index node (for an index, the
  child that would hold `name`; keep stepping until a directory entry is reached).
- `range_children(expect, block, offset, length) -> list[tuple[Expect, int, int]]` — children of a file node
  overlapping a byte range, with the range relative to each child.
- `parse_target(text) -> tuple[Cid, list[str]]` for `<cid>[/name…]` (ValueError if malformed).
- Local operations over a blocking `get(expect) -> bytes` (raise `NotFound` when absent):
  `step_name(expect, name, get) -> Expect | None` (one name, stepping through index nodes),
  `resolve(root, names, get) -> Expect`, `iter_dag(expect, get)` (each `(expect, block)`, each CID once,
  depth-first), `read_range(expect, get, offset, length) -> Iterator[bytes]`, `leaf_bytes(expect, get, stop_at)`
  (the bytes of a file's distinct chunks, from its file nodes only: the deny list's hash bound),
  `list_directory(expect, get, max_entries=None) -> Iterator[Entry]` (`TooLarge` past
  `max_entries`), `write_out(expect, get, dest, max_entries, check=None, created=None) -> warnings`
  (`created` gets a directory output's `(st_dev, st_ino)`, for `check_unchanged(dest, identity)` before it is
  renamed; a directory replaced while written raises `ValueError("output was changed while being written")`).

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
  targets only then. `changes()` grows with every block stored or deleted, pin or release, and record added,
  removed or retargeted: while it is unchanged, so are `pins()`, `cached_targets()` and every `dag_blocks()`
  answer (the publisher's refresh skips recomputing then).
- protection: `with storage.protect() as session:` — blocks stored or listed in `session.add(cid)` are kept as
  if pinned until the block exits (fetches, adds, imports in progress).
- deny list: `storage.deny` (`denylist.NONE` until the node sets its `DenyList`; the fetcher and publisher read it
  there too). `_put` raises `Denied` for a denied CID, before anything is written, even one already stored.
  `drop_denied(denied)` (binary CIDs, the `DenyList`'s `cids`) drops the cached fetches whose root, path or stored
  DAG holds one and collects as `gc` does; `denied_roots(roots, denied)` says which roots reach one through stored
  nodes; `collect()` deletes whatever nothing reaches (what a refused add or import left).
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
  node not stored yet may be stored later, and a raw block lists nothing. A pin's or unpin's row is still
  committed durably under the lock: a pin must be on disk when `pin()` returns (names' crash consistency and
  `rnfs pin add` depend on it), and before a collection can delete what an `unpin()` released. If the commit
  fails, `pin`/`pin_many`/`unpin` undo their change in memory and raise. Cache records track the keys changed
  since the last `save()`; those still cached are always the most recently used, so a save writes them with
  new `used` values (the table's LRU order) and deletes the others.
- `verify() -> stats` re-checks every block, deleting bad ones.
- A stored DAG node that no longer matches its CID (`DAMAGED`) has unknown children: while anything live
  reaches one, `evict()` and `gc()` delete nothing and log it once (`_held_back`) until `verify()` removes it or
  a fetch replaces it. Fetches treat such a block as not stored: deleted and fetched again.
- `dag_blocks(cid) -> frozenset[Cid]` the blocks of a complete DAG (memoised), used for "what a node serves".

`denylist.py` — `parse_entry(text) -> (canonical, binary CID | None, digest | None)`, `parse_list(bytes) -> (binary
CIDs, digests)` (ValueError naming the line; `MAX_LIST_BYTES` 4 MiB, `MAX_LIST_ENTRIES` 100 000).
`DenyList(state_dir)`: `cids` and `hashes` (frozensets swapped whole, read without a lock: every check is one set
lookup), `denies(cid)`, `denies_hash(digest)`; the local list `entries()`, `add(texts)`, `remove(texts)`
(`deny_entries`, at most `MAX_LOCAL_ENTRIES`); sources `accept(source, cid, entries)` (saved, then applied),
`forget(source)`, `accepted()`, `in_use(source)`, `entry_count(source)` (`deny_lists`: the CID each source last
accepted; `reload(read)` reads every one back from storage at start); `known_hash(cid)` / `remember_hash` (a file
CID's content SHA-256, `MAX_KNOWN_HASHES` LRU, kept across restarts in `deny_hashes`: `save()`, called by the node's
`_save_state`, writes the rows changed since the last one outside the lock, behind a save lock); `add_listener(fn)`,
called after each change outside its lock; `DENY_HASH_RATIO` and `DENY_HASH_SLACK` bound what `_check_file` hashes.
`walker.write_out(..., check)` and `walker.iter_files` and
`builder.checking(check)` (a ContextVar: `build_file` hashes each file and passes the digest before the file's root
node is stored) are its hooks in the content layer; `archive.import_archive(..., check)` runs on the complete roots
before they are pinned.

## DHT layer interfaces

`routing.py` — `RoutingTable(own_id, config, clock=time.monotonic, admit=None)` (`admit(node_id)`: private mode's
member check, asked only for a node not in the table and without the table's lock held, since it may block on
RNS; the table is checked again once it answers):
`heard(node_id) -> bool` (proven server: announce in server mode, or answered `/find`), `answered(node_id) -> bool`
(`heard`, and reliable again), `failed(node_id) -> bool` (True if dropped), `unreliable(node_id)` (its last
`max_failures` queries or more failed, in the table or not; announces don't clear it: `_unanswered`, at most
`MAX_UNRELIABLE` (4096) nodes, least recently failed forgotten first), `may_retry(node_id)` (due again after
`AVOID_AFTER` (300 s), doubling per further failure up to `AVOID_MAX` (3600 s); True claims the retry, pushing
the next one back a further backoff, so concurrent lookups don't all retry it), `remove(node_id)`, `ban(node_id)`,
`is_banned(node_id)`, `closest(key, count=K, for_reply=False)`,
`seniority(node_id) -> float | None` (seconds in the table), `is_senior(node_id, min_age)`, `standing(node_id,
min_age) -> int` (0 senior and healthy, 1 healthy, 2 stale, 3 absent: ranks keepers), `standings(min_age) ->
dict` (every node's standing now: the keeper query's snapshot from before its lookup), `contains(node_id)`,
`nodes()`, `node_count()`, `live_count()` (nodes `/find` replies would include).

`providers.py` — `ProviderStore(max_records, state_dir, ttl=PROVIDER_TTL, clock=time.time, rng=random,
max_per_publisher=None, max_per_key=1000, own_id=None, seniority=0)` (`state_dir` holds the state database): `store(key, publisher) -> bool`, `withdraw(key, publisher)`, `provide(publisher,
keys, withdrawn) -> int`, `retain(keep) -> int` (drops every record of publishers `keep` rejects: private mode's
non-members), `for_reply(key, exclude=lambda id: False) -> list[bytes]`, `providers(key, exclude) -> list[bytes]`, `expire()`,
`record_count()`, `allowance(publisher)`, `save()`, `close()`. Local limits against flooding: one publisher holds at most max(10,
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
`Lookup(key, start_nodes, query, own_id, alpha, beta, k, timeout, is_known, on_answer, on_failure, exclude, report,
unreliable, may_retry, max_queries)` with `find_closest() -> list[bytes]` and `find_providers(on_provider, should_stop)
-> list[bytes]`. Besides completion and the deadline, a run (`_iterate`) ends once it has sent `max_queries`
(`MAX_QUERIES`, 8 × K) queries and their answers are in ("query cap"), or after `STALL_WINDOWS` (3) × max(alpha,
beta) answers in a row that leave the bit length of the beta-th closest known distance (`_reach`) unchanged ("no
progress"). A run ranks the nodes `unreliable` named when it learnt of them (`doubtful`) after every other, for
its queries, its termination and `find_closest`'s result; one that distance alone would rank within the query
window is asked again when `may_retry` says so, as a query beyond `alpha` (`retrying`) that the run waits for
only if the node comes to rank within the window by `rank` (it is then counted in flight). `is_known(id)`
says whether RNS knows its identity; at most `alpha` unknown IDs are tried per lookup (`allow_unknown(id)`, also
used by the fetcher for provider IDs). `listing_nodes()` is the nodes that have answered with at least one
provider, and `listers(provider)` those that listed a given provider, so the keeper search can tell who vouches
for its candidates.

## Network layer interfaces

`server.py` — `RequestServer(destination, config, backend)`. `backend` provides:
`is_dht_server() -> bool`, `find(key) -> (providers, nodes)`, `provide(publisher_id, keys, withdrawn) -> bool`,
`served_block(cid) -> bytes | None` (only blocks in the DAG of a currently provided CID, re-verified),
optionally `served_block_size(cid) -> int | None` (so `/blocks` stops at the block over its budget unread),
`remote_pin(identity_hash, cid, pin) -> bool`, `is_member(identity_hash) -> bool` (private mode), and
optionally `feature_requests() -> {path: handler(data, remote_identity)}` (features' request paths, served within
the same per-link limits).
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

Programs use `rnfs.Node` or `rnfs.Client` (Public API, below). The CLI, and `rnfs.Client` underneath, reach the
daemon through the local API with `rnfs/apiclient.py` (`ApiConnection`), which imports neither RNS nor anything
that does. `ApiConnection(rnfs_dir, port, token_file, timeout)` takes the port from `rnfs.toml`
(`api_port()`, `SettingsError`) and the token from `<rnfs dir>/api_token` (`read_token`, `TokenError`); its
every request needing the token first proves the daemon on its own connection (`_prove`, `verify_daemon`),
which the daemon keeps open for that request only, so a client used for a long time sends nothing to a program
that took the port after the daemon stopped; the token is read again when a proof with it fails (a restarted
daemon's). The proof also reads the daemon's version for `check_daemon`, the one place a version
check goes. `request()` maps failures to `DaemonNotRunning`, `ConnectionFailed`, `AuthFailed` (a failed proof,
or 403) and the API's statuses (`STATUS_ERRORS`, `error_for`); `Stream` reads a body, raising
`ConnectionFailed` if it ends short. Its methods (`status`, `add_path`, `add_stream`, `fetch`, `cat`, `info`,
`ls`, `stat`, `pin_*`, `name_*` (with `name_watch`), `submit_job`, `job`, `jobs`, `cancel_job`, `clear_jobs`,
`deny*`) send only the parameters given; `ApiError` is an `RnfsError`, and each status's error a subclass of the
node's own too (`NotFound` of `errors.NotFound`, `BadRequest` of `ValueError`, ...). The CLI's `ApiClient`
subclasses it, turning each `ApiError` into `fail(str(e))`, so its messages are the library's.

`RnfsNode(config, reticulum=None, embedded=False)` exposes `node_id: bytes`, `config`, and these methods. Targets are strings
`<cid>[/name…]`; CIDs are returned as strings. Return values are JSON-ready dicts.

| Method | Returns |
|---|---|
| `add_path(path, wrap=False, only_hash=False, pin=True, to=None, to_self=False, seal_name=True, chunker=None)` | `{"cid", "size", "warnings": [str]}` (+ `"sealed"`); file or directory, pinned `user` (or cached with `pin=False`) |
| `add_stream(stream, size=None, name=None, only_hash=False, …)` | as above; `name` wraps it as that entry (sealed: the sealed name) |
| `get(target, output, from_node=None, raw=False)` | `{"cid", "warnings"}` (+ `"warnings_total"` past 100; + `"sealed", "name"`, the name only if `safe_default_name` accepts it); fetches and writes the file or directory under `.rnfs-get-<random>.partial` beside `output`, then renames it to `output` (`rename_new`), decrypting a sealed file unless `raw`; a directory output is first checked to still be the one `write_out` made (`walker.check_unchanged`) and, on Windows, made to inherit its folder's access list (`winacl.inherit_from_parent`; if that fails it stays private, with a warning logged) |
| `info(target, for_gateway=False, from_node=None, raw=False)` | `{"cid", "type": "file"\|"directory", "size", "executable"}` (+ `"sealed", "recipient", "stored_size", "name"`; the sealed name is None unless `safe_default_name` accepts it) |
| `stored_head(target, length)` | a file's first `length` bytes if stored, else None; never fetches (the user's own gateway `HEAD` and ranges not at 0); `Denied` as a read: a denied CID met, or while there are file hashes a file that fails `_check_file` (one not wholly stored gives None) |
| `read(target, offset=0, length=None, for_gateway=False, from_node=None, raw=False)` | iterator of bytes of a file (range fetched only; sealed files decrypted unless `raw` or `for_gateway`) |
| `read_file(target, offset=0, length=None, from_node=None, raw=False)` | `(info, chunks)`: `info()` and `read()` in one fetch, resolving the target once; `chunks` reads `[offset, offset + length)` cut to the info's size only once iterated (a directory's raises `ValueError`), and closing it ends the fetch (`GET /api/cat`, which used to call `info` and `read`: two fetches) |
| `block(cid, for_gateway=False)` | the CID's own block, verified (`cat --block`, `?format=raw`: the gateway calls it only for the user's own GET) |
| `export_stream(target, start, for_gateway=False)` | fetches the DAG, then writes its archive to `start(exact length)` (`?format=archive`; a `start` returning `None` writes nothing). The gateway calls it only for the user's own GET (never for a limited request or a HEAD, stored or not), inside `fetcher.observing(ClientWatch)`, which cancels the fetch once the client disconnects |
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
(recompute the provide set, withdraw at once; skipped when the node's generation, the deny list, the key
sources' keys and the strategy are as at the last one), `publish_due()`, `serves(cid)`, `run()`. The node calls the
first `refresh()` only after the features have registered their key sources, so a restart doesn't withdraw
kept names' keys. `publish_due` republishes `DhtKey`s every max(effective `republish_interval`, 3 days)
with a per-key ±10% jitter (capped below `PROVIDER_TTL / 2`), drops a withdrawal of any key it provides to
the same node in that round, runs up to `PUBLISH_LOOKUPS` (4) lookups at once, keeping the batches one at a
time would build, and on slow nodes runs one lookup at a time, at most 10 per round, waiting 30 s × the
round's lookups before the next (never-published items first, then the longest unpublished). When
`refresh()` finds a key provided again, it cancels that key's queued withdrawals (their nodes join its earlier
nodes) and re-records the node's own record, and a round sends only withdrawals still queued when it reaches
each node, so a withdrawal queued while the key was briefly unprovided never follows a store of it.
`protocol.provide_batches` puts withdrawals before stores, and a key in both lists is only stored.

Errors: `ValueError` → 400, `NotFound` → 404, `TooLarge` → 413, `InvalidDag` → 422, `Busy` → 503,
`StorageFull` → 507, `Denied` → 451, other `RnfsError` → 500. Paths passed to `add_path`, `get`, `export` and `import_archive`
are absolute paths on the daemon's machine (the CLI resolves them); stdin is uploaded as a request body.

**Core API routes** (`rnfs/api.py` `ROUTES`): `GET /api/status` (open), `/api/info`, `/api/cat`, `/api/ls`,
`/api/stat`, `/api/pin/ls`, `/api/providers`, `/api/ping`, `/api/name/list`, `/api/name/resolve`,
`/api/name/watch` (a long poll for newer records: Public API, API additions);
`POST /api/add`, `/api/add/stream`, `/api/get`, `/api/dir/set`, `/api/dir/rm`, `/api/pin/add`, `/api/pin/rm`,
`/api/pin/remote`, `/api/gc`, `/api/verify`, `/api/name/create`, `/api/name/rm`, `/api/name/export`,
`/api/name/import`, `/api/name/publish`, `/api/export`, `/api/import`. `GET /api/info` and `POST /api/get` take
an optional `from` (node ID), as does `GET /api/cat`; `GET /api/name/resolve?name=` takes a name or a name
contact's nick.

The API requires `X-Rnfs-Api: <token>` on every request except `GET /api/status`, which without a valid token
answers only `{"version"}` (health checks, and `verify_daemon`, which reads the proof header, then the
version); the CLI sends the token with every other request, `rnfs status` included (without a readable token it
prints the version and says why). The daemon writes a new random token to `<rnfs dir>/api_token` (mode 0600, in the 0700 rnfs dir) at each start and the CLI reads it, so other
local users and processes can't drive the API. Before anything else uses the rnfs dir (the debug log, the
node), the daemon locks `<rnfs dir>/daemon.lock` (`fsutil.lock_exclusive`: `flock`, or `msvcrt.locking` of a byte
past the text on Windows; the file holds its process ID) and keeps the lock until it exits, so a second daemon
on the same dir stops at once, naming the first (`lock_holder`). A start runs in this order, in the daemon
(`cli.run_daemon`) and in `rnfs.Node` alike: the root (`node.make_private_root`), then the settings
(`Config.load`), then the subdirectories and the lock (`lock_rnfs_dir`: `_make_private_dirs(root)`, then
`lock_exclusive`), so each step relies only on what an earlier one secured. `make_private_root` refuses to start
(RnfsError) if the rnfs dir belongs to another user or can't be made 0700. `_make_private_dirs` (audit API-1)
opens each of `node.PRIVATE_SUBDIRS` (`state`, `blocks`, `storage`, `names`, `lxmf`) with `O_DIRECTORY |
O_NOFOLLOW`, creating it 0700 if missing: a symlink, a non-directory or one another user owns is refused with an
RnfsError naming `chown`, before Reticulum starts; a loose one is `fchmod`ed to 0700 with a notice, and if its old
mode let others write, one `lstat` pass (`_warn_foreign_entries`) warns about entries another user owns (a
planted raw block would be trusted by its length). `rnfs.toml`, and RNS's `config` when `reticulum_dir` is the
rnfs dir, are opened by `config.open_checked` (`O_NOFOLLOW | O_NONBLOCK`, so a symlink is refused and a FIFO
never waited on) and checked through the descriptor: a regular file, the user's own, and if group or other bits
are set `fchmod`ed to 0600 (`fsutil.check_owned_fd`, which the identity's check shares); at most
`MAX_SETTINGS_SIZE` (1 MiB) is read. What was changed goes to `config.warnings` (a plain attribute, not a field,
so `rnfs.toml` can't set it), never to `rnfs.log`, which imports RNS: the daemon prints them (`cli.warn`),
`rnfs.Node` logs them, the CLI client prints them and `rnfs.Client` (`apiclient.api_port`) raises them as
`RuntimeWarning`s. On Windows (`rnfs/winacl.py`), mode bits set no ACL, so a new
rnfs dir and its subdirectories are created with a protected DACL allowing only the user and SYSTEM (inherited
below it) in the CreateDirectoryW call itself (`winacl.make_private_dir`, a self-relative descriptor built by
`private_descriptor`), so the parent's DACL never applies to them; a new root that already holds anything, and
a root that is a junction or symlink (`fsutil.is_link` on its lstat), are refused. An existing root or
subdirectory that allows anyone else is given the DACL (`winacl.tighten(d, directory=True)`), one owned by
another account is refused, and so is a subdirectory that is a junction or symlink; the token, identity and name key
files get the same DACL before they are renamed into place (`fsutil.write_atomic(private=True)`), and
`read_token` checks the opened token's DACL (Administrators tolerated). `rnfs.toml` (`Config.load`) and the
identity (`network/identity.py`) are checked through the handle they are read from (`fsutil.check_owned_fd`,
which calls `winacl.tighten(fd=...)` there): another account's is refused, a loose one tightened with a warning,
and a failure is fatal. A `--dir` outside `%USERPROFILE%` is warned about. The advapi32 and kernel32 calls are
behind `winacl.backend`, which tests replace with a fake. A response to a request with
`X-Rnfs-Challenge: <32 hex>` carries `X-Rnfs-Proof: apiclient.api_proof(token, port, challenge)` (HMAC-SHA256 of
`rnfs api proof <port> <challenge>`); `apiclient.ApiConnection.verify_daemon` checks it with `GET /api/status` on the connection
that then carries the token: `ApiHandler.dispatch` keeps one open after a proof request asking for
`keep-alive` (`keep_open`, once per connection: `proved`), giving the next request a new request deadline. Both HTTP servers (`TimedHandler`) time out slow requests, cap concurrent connections (API
32, gateway 64) and refuse HTTP/0.9 with a 400 carrying the usual headers (both add them in `end_headers`, so
http.server's own error pages, 400, 414, 431 and 501, carry them too). Gateway errors raised by the node
(anything but `GatewayError` and `NotFound`: 400, 413, 422, 503, 507, 500) say only a fixed message per status
(`gateway.ERROR_MESSAGES`; the detail goes to the log); malformed `Range` headers are ignored.

Neither server sends CORS headers. Gateway pages run sandboxed in an opaque origin, so they load their DAG's
files as subresources but can't read any gateway response with `fetch()`; an earlier rule allowing a gateway
`Referer` never fired, as browsers send no `Referer` from an opaque origin, and allowing `Origin: null` instead
would let any site's sandboxed frame read the gateway. A separate origin per root (a subdomain per CID) is the
possible future way to let pages read their own DAG.

## Public API (rnfs.Node, rnfs.Client)

**Provisional until 0.2.** Names, signatures and objects may change after use in a real app. Said in the
module docstring of `rnfs/__init__.py`, in `rnfs.API_STATUS == "provisional"`, and in README.md.

Two classes with one interface (`rnfs.interface.Rnfs`): **`rnfs.Node`** runs a node inside the app's process,
as RNS runs inside it; **`rnfs.Client`** drives the user's running daemon through its local API. Same methods,
objects, exceptions and callback rules, so an app switches by changing the one line that constructs it. Nothing
chooses for the app. The JSON API is unchanged for other languages.

```python
import RNS, rnfs

RNS.Reticulum()                                    # the app's own
with rnfs.Node("~/.myapp/rnfs") as node:           # or: rnfs.Client() for the user's daemon
    added = node.add("photos/")                    # rnfs.Added; added.cid is an rnfs.Cid
    for entry in node.ls(added.cid):               # rnfs.Entry(name, kind, size, cid)
        print(entry.name, entry.size)
    with node.open(f"{added.cid}/cat.jpg") as f:   # seekable; reads fetch only the chunks they cover
        f.seek(1_000_000); data = f.read(4096)
    name = node.names.create("site")               # rnfs.Name
    name.publish(added.cid)
    watch = node.names.watch(other, lambda record: print(record.cid))
    job = node.jobs.get("rnfs:name/3f2a…/site", "/tmp/site", on_progress=lambda job: print(job.done_bytes))
    job.wait()
```

### Package layout

```
rnfs/__init__.py   __version__, API_STATUS; every public name loaded on first use (PEP 562 __getattr__, __all__,
                   __dir__), so `import rnfs` imports nothing more and the CLI's start-up is unchanged
rnfs/objects.py    the typed objects (Cid re-exported from rnfs.content.cid); no RNS
rnfs/interface.py  Rnfs, Names, Name, Jobs, Job, Deny, Watch, File, the event thread: shared by both; no RNS
rnfs/embedded.py   Node: imports rnfs.node, so RNS, only when rnfs.Node is first used
rnfs/remote.py     Client, over rnfs.apiclient; no RNS
rnfs/errors.py     gains Cancelled (moved from fetcher, which imports it from there) and Closed
rnfs/apiclient.py  the API protocol layer (token, proof, typed errors, Stream); its class becomes ApiConnection
```

`Rnfs` implements every method once, `open` included, over a *backend* offering the facade's operations in the
local API's argument and result forms (JSON-ready dicts; CIDs written out, node IDs and names as hex): for
`Node`, `embedded._Backend`, an adapter over the `RnfsNode`; for `Client`, `remote._Backend(api, long_wait)`, over
an `ApiConnection` whose methods already mirror the facade (`add_path`, `ls`, `info`, `pin_add`, …). Both turn the
same dicts into the same objects (`Entry.from_dict`), so they can't drift apart; only lifecycle, how name records
are heard, and the escape hatches differ per class. `rnfs.Client`, its objects
and errors import neither RNS nor anything that does (tested, as the CLI's start-up is). The CLI keeps using
`apiclient` directly (its `ApiClient` subclasses `ApiConnection`), so its output can't change.

### Construction and lifecycle

`Node(rnfs_dir, *, reticulum=None, settings=None)`
- `rnfs_dir` (str or path) is required: the app's own node directory (identity, blocks, state, optional
  `rnfs.toml`), never guessed. Created private as the daemon's is, in the same order (`make_private_root`,
  `Config.load`, whose `warnings` it logs, then `lock_rnfs_dir`).
- `reticulum`: the running `RNS.Reticulum`, which must be `RNS.Reticulum.get_instance()` (RNS allows one per
  process; anything else is ValueError). None: the running instance if there is one, else the node starts one
  from the `reticulum_dir` setting, as the daemon does (so it attaches to `rnsd` if one runs). The node never
  stops Reticulum or calls `RNS.exit()`, whether it started it or not: RNS can't restart within a process, and
  its exit handler saves its tables when the process ends.
- `settings`: `Config` fields as a dict, over `rnfs.toml`'s (`Config.load(rnfs_dir, defaults=EMBEDDED_DEFAULTS,
  **settings)`), validated (ValueError naming the setting). Defaults for an embedded node where neither sets them
  (`embedded.EMBEDDED_DEFAULTS`, which `Config.load`'s `defaults` puts under the file): `dht_mode = "client"`
  (an app's node is often short-lived; section 6 says such nodes SHOULD be clients) and `lxmf_enabled = false`.
  `{"dht_mode": "server"}` and `{"lxmf_enabled": True}` turn them on. `api_port`, `gateway_port` and
  `debug_log` belong to the daemon and are ignored (no API, gateway or debug log runs in the app);
  `verbosity` applies only when the node starts Reticulum.
- The identity is the rnfs dir's (`storage/identity`); an app's identity can't be handed in (decision 4).
- The constructor starts the node (state loaded, threads running, join in the background) and raises if it
  can't: `RnfsError` for the dir in use or not private, `ValueError` for settings, `OSError`, `StateDbError`.
  At most one `Node` per process at a time (`embedded._running`; RnfsError otherwise), checked before the lock.
- It takes `<rnfs dir>/daemon.lock` before anything else touches the dir, through the helper the daemon now calls
  too, `node.lock_rnfs_dir(config)` (`_make_private_dirs`, then `fsutil.lock_exclusive`); held, it raises
  `node.DirInUse` (an RnfsError with `.holder`, the process ID): "another rnfs daemon or node (process N) is using
  DIR; stop it first, or use another rnfs dir". The daemon catches it and prints its own message, unchanged.
  `close()` releases the lock once the node has stopped, as a start that fails does (`fsutil.unlock`: unlocked, then
  closed, since Windows may free a closed file's locks only some time later).

`Client(rnfs_dir=None, *, port=None, token_file=None, timeout=None)`: as `apiclient.ApiConnection` (default
`~/.rnfs/`, the `api_port` in its `rnfs.toml`, the token from `<rnfs dir>/api_token`, proved before it is sent).
Nothing is sent until the first call. Raises `SettingsError` if `rnfs.toml` can't be read or trusted; one made
private on the way is reported as a `RuntimeWarning` (`warnings.warn`), without loading RNS.

Both: `close()` (idempotent; both end their event and job-watch threads, then a Node stops its features and
threads, saves, closes the state database, releases its rnfs.dht destination and the lock; a Client's name
poller ends with the long poll in flight, within 30 s), `closed`, `with … as node:` (closes on
exit), `wait_joined(timeout=None) -> bool` (Node: the first join attempt has finished; Client: True). A call after
`close()` raises `Closed`. Both may be used from several threads at once.

### Targets and objects

A *target* is a `Cid`, a string `<cid>[/name…]` (whose first part may be a name contact's nick, as in the CLI),
or an `rnfs:` URI (section 13): `rnfs:<cid>/path`, or `rnfs:name/<name>/path`, whose name is resolved first
(persisted, as `names.resolve`), with `?from=<node ID>` used as `from_node` where the method takes one. Node IDs
and names are `bytes` (16), or 32 hex characters where accepted; returned as `bytes`.

Every object is a frozen dataclass with `to_dict()` (the API's JSON form, CIDs as strings) and
`from_dict(d)` (from that form):

| Object | Fields | `str()` |
|---|---|---|
| `Cid` | `rnfs/content/cid.py` as is: `.binary`, `.codec`, `.digest`, `.key`, `parse`, `from_bytes` | written form |
| `Added` | `cid`, `size`, `warnings: tuple[str]`, `sealed: bool` | the CID |
| `Written` | `cid`, `output`, `warnings`, `warnings_total`, `sealed`, `name` (get) | the output path |
| `Entry` | `name`, `kind` ("file", "executable", "directory"), `size`, `cid` | the name |
| `Info` | `cid`, `type` ("file", "directory"), `size`, `executable`, `sealed`, `recipient`, `stored_size`, `name` | — |
| `Stat` | `cid`, `size`, `blocks`, `local_blocks`, `local_bytes` | — |
| `Pin` | `cid`, `sources: tuple[str]`, `complete`, `type`, `sealed`, `name` | the CID |
| `Record` | `name: bytes`, `cid`, `sequence`, `age` (seconds since heard, or None) | — |
| `Status` | `version`, `node_id`, `identity_hash`, `dht_mode`, `slow`, `private`, `known_nodes`, `records`, `pins`, `provided`, `blocks`, `used_bytes`, `max_storage`, `names`, `features: dict` (every other field) | — |
| `Collected` / `Verified` | `removed_blocks`, `freed_bytes` / `checked`, `bad: tuple[Cid]` | — |
| `Exported` / `Imported` | `roots: tuple[Cid]`, `blocks` / and `incomplete` | — |
| `DenyState` | `entries: tuple[str]`, `lists: tuple[dict]` | — |

`Name`, `Job`, `Watch` and `File` are bound to the `Node` or `Client` that made them (below).

### Methods

Exceptions, as both classes raise them (`→` the errors besides those every call may raise: `Closed`, and for a
Client the connection errors below). "Fetch errors" = `NotFound` (no such content or path, no provider),
`TooLarge`, `InvalidDag`, `Denied`, `StorageFull`, `Busy`. Facade calls are on `RnfsNode`; routes in
`rnfs/api.py` or the feature named.

| Method → returns | Raises | Facade / API route |
|---|---|---|
| `status() -> Status` | | `status()` / `GET /api/status` |
| `node_id -> bytes` (property) | | `node_id` / status, read once |
| `add(source, *, name=None, wrap=False, pin=True, only_hash=False, to=None, to_self=False, seal_name=True, chunker=None) -> Added` | `ValueError` (options: `name` with a path, `wrap` without `name`, a source neither a path, bytes nor a binary file), `NotFound` (path), `Denied` (a file's hash), `StorageFull`, `OSError` | a path (str or PathLike): `add_path` / `POST /api/add`; bytes or a binary file: `add_stream(name=name)` / `POST /api/add/stream`, sealing with `name` as the sealed name unless `seal_name` is False |
| `get(target, output, *, from_node=None, raw=False) -> Written` | fetch errors, `ValueError` (output exists), `OSError` | `get` / `POST /api/get` |
| `open(target, *, from_node=None, raw=False) -> File` | fetch errors, `ValueError` (a directory) | `info`, then `read(target, offset, length)` per refill / `GET /api/info`, then `GET /api/cat?offset=&length=&from=` per refill |
| `cat(target, *, offset=0, length=None, from_node=None, raw=False) -> Iterator[bytes]` | fetch errors, `ValueError` | `read` / `GET /api/cat` |
| `block(cid) -> bytes` | fetch errors | `block` / `GET /api/cat?block=1` |
| `ls(target) -> list[Entry]` | fetch errors, `ValueError` (not a directory) | `ls` / `GET /api/ls` |
| `info(target, *, from_node=None, raw=False) -> Info` | fetch errors | `info` / `GET /api/info` (which has no `raw`: a Client gives a sealed file's `stored_size` as its size) |
| `stat(target) -> Stat` | fetch errors | `stat` / `GET /api/stat` |
| `dir_set(target, name, child, *, executable=False) -> Cid`, `dir_rm(target) -> Cid` | fetch errors, `ValueError` | `dir_set`, `dir_rm` / `POST /api/dir/set`, `/api/dir/rm` |
| `pin(target) -> Cid` (once complete) | fetch errors | `pin_add` / `POST /api/pin/add` |
| `unpin(cid)` | `NotFound` (no `user` pin) | `pin_rm` / `POST /api/pin/rm` |
| `pins(cid=None) -> list[Pin]` | | `pin_ls` / `GET /api/pin/ls` |
| `pin_remote(node_id, cid, pin=True) -> bool` | | `pin_remote` / `POST /api/pin/remote` |
| `providers(cid) -> list[bytes]`, `ping(node_id) -> float \| None` | | `providers`, `ping` / `GET /api/providers`, `/api/ping` |
| `export(target, output) -> Exported`, `import_archive(path) -> Imported` | fetch errors, `ValueError`, `OSError` | `export`, `import_archive` / `POST /api/export`, `/api/import` |
| `gc() -> Collected`, `verify() -> Verified` | | `gc`, `verify` / `POST /api/gc`, `/api/verify` |
| `names.create(alias) -> Name`, `names.import_key(alias, key) -> Name` | `ValueError` (alias invalid or taken, key malformed) | `name_create`, `name_import` / `POST /api/name/create`, `/api/name/import` |
| `names.get(alias) -> Name`, `names.list() -> list[Name]` | `NotFound` | `name_list` / `GET /api/name/list` |
| `names.remove(alias)` | `NotFound` | `name_rm` / `POST /api/name/rm` |
| `names.resolve(name) -> Record` (bytes, hex, `Name` or a contact's nick) | `NotFound` (unresolved), `ValueError` | `name_resolve` / `GET /api/name/resolve` |
| `names.watch(name, callback) -> Watch` | `ValueError`; a Client: `NotFound` from a daemon without the endpoint | below / `GET /api/name/watch` |
| `Name`: `alias`, `name: bytes`, `cid`, `sequence`, `str()` hex; `publish(target) -> Record`, `export_key() -> bytes`, `remove()`, `refresh() -> Name` | `publish`: `NotFound` (path not stored), `ValueError`, `RnfsError` (highest sequence) | `name_publish`, `name_export`, `name_rm` / `POST /api/name/publish`, `/api/name/export`, `/api/name/rm` |
| `jobs.get(target, output, *, from_node=None, sealed_name=False, fallback_dht=False, on_progress=None) -> Job`, `jobs.pin(target, *, on_progress=None) -> Job`, `jobs.export(target, output, *, on_progress=None) -> Job` | `ValueError` (output exists or in use), `Busy` (1000 active) | `JobsFeature.submit` / `POST /api/jobs` |
| `jobs.job(job_id) -> Job`, `jobs.list() -> list[Job]`, `jobs.clear(job_id=None) -> int` | `NotFound`, `ValueError` (not finished) | `JobsFeature.get`, `list`, `clear` / `GET /api/jobs/get`, `GET /api/jobs`, `POST /api/jobs/clear` |
| `Job`: `id`, `kind`, `target`, `output`, `from_node`, `sealed_name`, `fallback_dht`, `state`, `error`, `result: dict`, `total_bytes`, `done_bytes`, `downloaded_bytes`, `blocks`, `blocks_total`, `rate`, `eta`, `created`, `started`, `finished`, `runs`, `done` (finished); `refresh() -> Job`, `wait(timeout=None) -> Job`, `cancel() -> Job`, `watch(callback) -> Watch` | `wait`: `TimeoutError`; `NotFound` once cleared | `JobsFeature.get(wait=)`, `cancel` / `GET /api/jobs/get?wait=`, `POST /api/jobs/cancel` |
| `deny.list() -> DenyState`, `deny.add(entries) -> list[str]`, `deny.remove(entries) -> list[str]` | `ValueError` (malformed entry, too many), `NotFound` (remove: not on it) | `node.deny.entries`/`add`/`remove`, `DenyFeature.lists` / `GET /api/deny`, `POST /api/deny/add`, `/api/deny/rm` |

`File` (from `open`, an `io.BufferedIOBase`): `read`, `read1`, `readinto`, `readline`, iteration, `seek`, `tell`,
`seekable()`/`readable()` True, `size`, `cid` (the file's own CID), `info`, `close()`, context manager;
`read()` of the rest is one read. Reads
go by the target as opened, root and path, since providers announce roots and the file's own CID may have none;
an `rnfs:name/` URI is resolved once, at `open`, and a contact nick is expanded by the node (which reuses its
CID for 60 s). It is an `io.BufferedReader` of 64 KiB over `_FileReader`, whose every `readinto` is one `read`
(or one `GET /api/cat`) of `[position, position + its length)`, so only the chunks covering it are fetched; a sealed file reads its plaintext unless `raw`. Raises fetch errors as reads happen. Not
thread-safe, like any file object. `Job`, `Name` and every object are snapshots as of their last `refresh()`,
`wait()` or callback.

Job targets are what the jobs feature stores (`<cid>[/path]`): an `rnfs:name/` URI is resolved by `jobs.*`
before submitting, as the CLI resolves a nick. Jobs are persisted, resumed at the next start, and seen by
`rnfs jobs` (a Client's) or only by the app (a Node's, in its own rnfs dir).

**Escape hatches, unsupported:** `Node.internal` (the `RnfsNode`) and `Client.api` (the `ApiConnection`, whose
`get(path, **params)` and `post(path, **fields)` reach every endpoint: contacts, peers, stats, keep, follow,
LXMF). Not in the typed interface for 0.2: keep/follow, contacts, peers/stats, LXMF.

**Errors.** One hierarchy in `rnfs.errors`, every class importable as `rnfs.<Name>`: `RnfsError` ⊃
`NotFound`, `TooLarge`, `InvalidDag`, `StorageFull`, `Busy`, `Denied`, `Cancelled`, `Closed`, `ApiError`.
`ApiError` (with `.status`) ⊃ the Client's own: `DaemonNotRunning`, `ConnectionFailed`, `TokenError`,
`AuthFailed`, `SettingsError`, `ServerError` (500, or another 5xx), and the API statuses, each also a subclass
of the node's class: `apiclient.NotFound(ApiError, errors.NotFound)` and so on, `BadRequest(ApiError,
ValueError)`. So `except rnfs.NotFound` and `except ValueError` catch both classes' errors. A Node raises
`OSError` where a Client gets `ServerError` (the API answers 500 for it).

### Events and callbacks

| Callback | Called with | Source in a Node | Source in a Client |
|---|---|---|---|
| `names.watch(name, callback)` | `Record`, each time a record of `name` newer than the last delivered (or than the one held when `watch` was called) is accepted under the sequence rules (section 9): announces, path responses, keepers' answers, this node's own publishes | `NameManager.watch(name)` (its announces count as asked in `_heard`, so a full name table never refuses them) and a record listener (`add_record_listener`), which `_offer` calls when it accepts a record and `publish` when it commits one, for watched names, with the names lock held; it only queues the record. Records heard before the watch knows its baseline wait and are compared with it (`_NameWatch.begin`). The keepers listener (`KeepersFeature.add_listener`) isn't used: its records reach `NameManager.offer` too | the `GET /api/name/watch` long poll, on the thread `rnfs-client-name-watch` |
| `on_progress` / `Job.watch(callback)` | `Job`, at most twice a second (Node) or once a second (Client) while it changes, and once at its final state | the job-watch thread (`rnfs-job-watch`) polls `JobsFeature.get(id)` (a snapshot made under the jobs lock) every `fetcher.POLL` (0.5 s); `fetcher.observing` and the `Observer` (the `Job`, on the job's thread) stay the jobs feature's | `rnfs-client-job-watch` polls `GET /api/jobs/get?id=` once a second; `wait()` loops `GET /api/jobs/get?id=&wait=30` |
| LXMF links received | not offered in 0.2 (decision 5) | | |

Rules, the same for both:
- Every callback runs on the instance's event thread (`rnfs-events` in a Node, `rnfs-client-events` in a
  Client), one at a time, in the order events arrived. Events are coalesced per name and per job, so a slow
  callback gets the latest state, not a backlog.
- None of rnfs's locks is held: hooks inside rnfs only enqueue (`_Events.post`, whose condition is held just to
  queue, innermost), and the thread calls app code with nothing held. A callback may call any method of the Node or Client.
- A callback must not block for long: it delays every other callback of that instance. Long work (a fetch,
  `job.wait()`) belongs on the app's own thread or in a job. `close()` from a callback is allowed and doesn't
  wait for the event thread.
- An exception in a callback is logged (a Node: rnfs's log; a Client: Python's `logging`, logger `rnfs`) and
  dropped. `Watch.cancel()` (also a context manager) stops further calls; one already running finishes.
- Watching only listens: it sends nothing. To check now, `names.resolve()`.

### API additions (backwards compatible)

- `GET /api/name/watch?names=<hex>[,<hex>…]&after=<sequence or ->[,…]&wait=<s>` → `{"records": [{"name", "cid",
  "sequence", "age"}]}`: the records held for the listed names whose sequence is above the matching `after` (`-`:
  any), answered as soon as there is one or after `wait` seconds (at most 30, as jobs' `wait=`) with an empty list.
  Level-triggered, so nothing heard between polls is lost: the Client's first call (`wait=0`, made inside `watch()`,
  so a daemon without it raises `NotFound` there) sets the baseline, later ones run on its name-watch thread with
  every watched name in one call. Each call marks its names watched (`NameManager.watch`) until `WATCH_GRACE` (60 s)
  after it ends (`api.NameWatches`, one per `ApiServer`), at most `MAX_WATCHED` (1024) names, the soonest to end
  dropped first. At most 64 names per call and 4 calls waiting at once (503 beyond; a `wait=0` call never waits, so
  it is always answered), so watchers can't take the API's 32 connections. Token required; sends nothing to the
  network. A name watched while a call waits joins the next call, within 30 s; nothing heard meanwhile is lost.
- `GET /api/cat` gains `from` (a node ID), as `GET /api/info` has, for `open(from_node=)` and `cat`. A daemon
  that predates it ignores it (unknown parameters are ignored): the fetch still works, without the hint.

### Parity gaps

| Gap | How it is handled |
|---|---|
| A Node's events come from in-process hooks; a Client's from polling (jobs, 1 s) and the long poll (names) | Documented; the same callbacks and rules. A daemon older than the endpoint can't watch names (`NotFound` at `watch()`) |
| Paths (`add`, `get`, `export`, `import_archive`, a job's output) are opened by the daemon for a Client | Same machine and user (the token requires it); made absolute on the app's side, as now |
| `open()` costs one API request per buffer refill for a Client | Accepted; the 64 KiB buffer bounds the requests |
| Settings, DHT mode, LXMF, lifecycle: a Node's own; the daemon's for a Client | Documented: a Client can't change them; `close()` only ends the Client |
| No gateway or API in a Node | Not offered in 0.2 (decision 6) |
| The API's `GET /api/info` has no `raw` | A Client's `info(raw=True)` (and `open(raw=True)`) takes a sealed file's `stored_size` as its size |
| Keep/follow, contacts, peers, stats, LXMF only through the escape hatches | Typed methods later, once an app needs them |
| `OSError` in a Node is `ServerError` from a Client | Documented |
| A Client's connection errors (`DaemonNotRunning`, …) | Client only, all `ApiError` |

### What happens to `rnfs/apiclient.py`

It stays the protocol layer: `Client` was renamed `ApiConnection` (unreleased; the CLI's `ApiClient` and
`tests/test_apiclient.py` changed mechanically), keeping `request`, `call`, `get`, `post`, `Stream`, the token,
proof and version check, and its dict-returning methods, which become `rnfs.Client`'s backend. Its errors are
re-based as above. Its module docstring and README point apps to `rnfs.Client`. The CLI's output can't change:
it calls the same methods with the same parameters. It gained `name_watch` and `cat(from_node=)`.

### Example app

`examples/rnfs_share.py`, runnable as `python3 examples/rnfs_share.py --node DIR PATH` (an embedded node in
DIR, with the app's own Reticulum) or `--daemon PATH` (the user's daemon): adds PATH, lists it, reads 4 KiB
from the middle of its first file through `open`, publishes it under a name and watches that name, then gets it
back with a job showing progress. The one line constructing `Node` or `Client` is the only difference.
`MANIFEST.in` includes `examples/`; `tests/test_public.py` runs its `share()` through both classes.

### Embedded mode: how a Node differs from the daemon

The daemon keeps its process-wide behaviour. A Node (`RnfsNode(config, reticulum=None, embedded=True)`) differs
in these, each tested (tests/test_embedded.py: the guards in both scopes, and a host program over real RNS):
- Reticulum: it attaches to the running instance (or starts one if none runs); `stop()` never stops it, as it
  never did (the daemon's `RNS.exit()` is in `cli.run_daemon`). At `stop()` an embedded node also deregisters its
  rnfs.dht destination and closes the links to it (`_release_destination`), so the host's Reticulum stops
  answering for it and a later Node in the process can register it again.
- Compression: the daemon keeps `AUTO_COMPRESS_MAX_SIZE = 0`. A Node leaves it alone; rnfs's responses are
  already never compressed (`register_request_handler(auto_compress=False)`, which RNS 1.5.6 passes to the
  response `Resource`), `LinkPool.request` refuses locally a request that wouldn't fit one packet (so rnfs never
  sends a request Resource), and `guards.refusal` refuses on `rnfs.dht` links any advertisement with the
  compressed flag (`adv.c`) before anything is transferred, with `max_decompressed_size = 0` set on each
  accepted `rnfs.dht` Resource as a backstop.
- Guards: `guards.install(scope)` with `"process"` (the daemon: as today) or `"dht"` (a Node: the request,
  transfer-size and segment checks apply only where `is_dht_link`). `guarded_receive` and
  `guarded_response_resource_concluded` are already scoped; the host's links reach RNS's own methods.
- LXMF off by default: no `LXMRouter`, no LXMF identity, no `LXMF` import. With it on, the router is rnfs's own,
  and its links (not rnfs.dht) get RNS's stock compression, bounded on receipt by the feature's per-Resource
  limits; LXMF can't shut its router down, so it runs until the process ends.
- The lock, as above. Not done in a Node: the debug log, `threading.excepthook`, `faulthandler`, signal
  handlers, `RNS.logdest`, `RNS.exit()`.
- Threads added: `rnfs-events` and `rnfs-job-watch` (Node), `rnfs-client-events`, `rnfs-client-job-watch` and
  `rnfs-client-name-watch` (Client), each started on first use. Locks added: `_Events`' and `_JobPoller`'s
  conditions (innermost: held only to queue or take work); `Rnfs._watch_lock`, taken after the names lock (the
  record listener runs under it), never before; `NameManager._watched`, under `names._lock`;
  `api.NameWatches.lock`, taken before the names lock, never under it; and three leaves, never held while calling
  out: `remote.Client._poll_cond` (the name poller's wake-up), `interface._NameWatch.lock` (a watch's baseline and
  last sequence) and `interface.Watch._lock` (`cancel()`'s once-only check). Callbacks run with no rnfs lock held.

### Decisions taken at review

1. Compressed advertisements on `rnfs.dht` links are refused before anything is transferred, by the daemon too
   (before, it transferred one, then failed it as corrupt and tore the link down); honest nodes never send one.
2. `close()` on both classes, not the sketch's `stop()`.
3. `add()` returns `Added`, which carries a directory add's warnings.
4. A Node uses its rnfs dir's identity; the app's can't be handed in (0.2).
5. No LXMF callback and no handing over the app's `LXMRouter` (later).
6. A Node serves neither the API nor the gateway (0.2).
7. A Node defaults to `dht_mode = "client"` and `lxmf_enabled = false`.
8. `apiclient.Client` becomes `ApiConnection`, so `rnfs.Client` is the only client.

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
  the fetched targets for the roots strategy), `seal_recipients` (`RNS.Identity.recall` by destination hash, then
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
  the Content-Type from the name, or `application/octet-stream`. `stored_head` applies the deny list as a read
  does (`node_content.denying` wraps its getter; with file hashes, `_check_file` over stored blocks once the file
  is wholly stored), so such a HEAD of a denied file answers 451 like GET; `pin ls` (`_pin_sealed`) passes
  `deny=False`.

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
  `is_connected_to_shared_instance` attribute, else a `LocalServerInterface` or `LocalClientInterface` class
  name, `rates.LOCAL_INTERFACE_TYPES`) as of unknown
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
  cooldown for a response whose Resource the requester rejected or cancelled (both `REJECTED` on RNS 1.5.6)
  before `NO_PROGRESS_FRACTION` (a quarter) of it was sent (`_made_progress`: the more of `Response.sent` and
  the Resource's `get_progress()` × size; progress counts if either can't be read). A `FAILED` response (the
  link closed, or RNS gave up) starts none, so a reader that gives up early must close its links and never
  call `Resource.cancel()` on a response, or it would cool itself down at its providers. `Cooldowns` is flat:
  every cooldown lasts `blocks_cooldown`, per link and per identity. The `Response` docstring records which
  thread or lock owns each of its fields.
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
  name `rnfs.dht.<hex>`; an incoming link's destination is set by `Link.validate_request` before it is
  registered), any Resource advertisement with more than one segment, with metadata or compressed, and gives
  each `rnfs.dht` Resource it accepts a decompression limit of 0. Its scope (`install(PROCESS | DHT)`, set by
  the node first, kept by later calls) decides whether its other checks cover every link (the daemon, which also
  sets RNS's process-wide `AUTO_COMPRESS_MAX_SIZE` to 0) or `rnfs.dht` links only (an embedded node, which leaves
  that limit, restoring it if a PROCESS install changed it). `LinkPool.request` sends nothing that wouldn't fit
  one packet, so rnfs never sends a request Resource (which RNS would compress unless the limit is 0).
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

`rnfs/features/__init__.py` defines `Feature` and `FEATURE_MODULES` (in start order: peers, jobs, keepers, follow,
contacts, lxmf, deny). The node builds every feature in its constructor (`node.feature(name)`), starts them after
its own threads and stops them, in reverse, before its own services. A feature may override: `start()`, `stop()`,
`save()` (called with the node's periodic saves), `close()` (releases the state database at shutdown, after `stop()`
and the last save), `status()` (fields merged into `rnfs status`), `requests()` (new `rnfs.dht` paths; the request
server applies private mode and the per-link limit, and turns `None` or an exception into `False`; a response must
be a shape `protocol.response_refusal` accepts, True/False/None, a `/find` answer, or at most `MAX_BLOCKS_CIDS` bins
or nils, since nodes since 0.1.4 refuse any other), `api_routes()` (all token-protected) and `cli_commands()`.
Feature modules don't import RNS at module level (the CLI imports them to build its parser) and their constructors
work on `RnfsNode.offline` test nodes.

| Feature | Module | What it does | Hooks in shared code |
|---|---|---|---|
| `peers` | `peers.py` | `rnfs peers`, `rnfs stats`; its own `rnfs.dht` announce handler, so client-mode nodes' mode and last-heard time are known | reads `Traffic`, the routing table, links, paths |
| `jobs` | `jobs.py` | `JobsFeature` (the job manager), `Job`, CLI progress formatting | `fetcher.observing(observer)`: a thread-local observer every `Fetch` picks up (`total`, `block`, `cancelled` checked at each step, raising `fetcher.Cancelled`) |
| `keepers` | `keepers.py` | signed records (`SignedRecord`, `parse_signed`, `sign_record`, `from_announce_data`), `/name`, `KeeperClient`, `KeeperStore` | `Feature.requests`; `publisher.DhtKey` and `Publisher.add_key_source` (non-CID keys published like CIDs); `NameManager.keepers` (`published`, `heard`, `query`, `always_ask`, `local_record`), `NameManager.offer()`/`record()`; the 5-argument announce handler (packet hash) |
| `follow` | `follow.py` | followed names: each check and fetch one at a time, `fetcher.open(pinned=True)`, pin inside the fetch's protection, old pin released after | keepers' `FOLLOW` reason |
| `contacts` | `contacts.py` | address book; `expand_target`, `lookup(nick, kind)`, `node_arg` | `RnfsNode._parse_target` (never for the gateway), `JobsFeature.submit` |
| `lxmf` | `lxmf.py` | pure parts (`Link`, `parse_uri`, `format_uri`, `find_links`, `encode_field`/`decode_field`); `MessageLog` (the inbox and outbox, in the state database); `LxmfFeature` (send with direct→propagated fallback, inbox, auto-fetch, inbox get, pin release); `LxmfNetwork`, made in `start()`, holds every RNS/LXMF object | `lxmf.validate_settings` from `Config.validate`; `LXMF_PREFIX` in `storage.check_source` |
| `deny` | `deny.py` | `deny_lists` sources (`parse_source`, `validate_settings` from `Config.validate`): `check(source)` resolves a name (persisted), fetches the target pinned, checks it is a file within `MAX_LIST_BYTES`, parses it, pins it `deny` and `accept`s it, then releases unused `deny` pins; `run_due` on `rnfs-deny`, after the join (`CHECK_INTERVAL` for names, `retry_delay` backoff); sources no longer configured are forgotten at start | `node.deny` (core); `DENY` in `storage.check_source` |

`LxmfNetwork` makes an `LXMF.LXMRouter` with storage `<rnfs dir>/lxmf/` and `signal.signal` stubbed (LXMRouter
installs SIGINT/SIGTERM handlers that exit at once), a 256 kB delivery limit, announce data without
`SF_COMPRESSION`, and incoming delivery Resources (and propagation-node downloads) allowed to decompress up to
that limit, since the daemon sets RNS's global limit to 0 (an embedded node leaves it, and the limit bounds them
all the same). `delivery_resource_advertised` is wrapped to refuse a delivery Resource of more than one segment
(LXMF checks only each segment's own size). It keeps a table of up to 64 propagation nodes from
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
than raised. Names are resolved while planning: `_resolve(link, persist, allow_sent_cid)` returns
`(cid, names, fell_back)` and raises NotFound (naming `--use-sent-cid`) for an unresolvable name unless
`allow_sent_cid`, so without `use_sent_cid` nothing is written and no job submitted; with it, a link that fell
back to its sent CID gets a `warnings` entry in its result. Auto-fetch passes `allow_sent_cid=True` (its senders
are trusted) and records `"sent_cid": true` with such a pin; `_print_links` shows it, and a name link's sent CID.
An output without `-o` is named with `fsutil.safe_default_name`. Sent links carry `from` only with
`lxmf_send_from_hint`. The inbox and outbox are rows of the `lxmf_messages` table, saved in batches (NORMAL).

**Feature API routes** (all need the token): `POST /api/jobs`, `GET /api/jobs`, `GET /api/jobs/get?id=&wait=`,
`POST /api/jobs/cancel`, `POST /api/jobs/clear`; `GET /api/peers`, `GET /api/stats`; `POST /api/name/keep`,
`/api/name/unkeep`, `GET /api/name/kept`; `POST /api/name/follow`, `/api/name/unfollow`,
`GET /api/name/following`; `GET /api/contacts`, `GET /api/contacts/get?nick=`, `POST /api/contacts/add`,
`/api/contacts/rm`; `GET /api/lxmf/address`, `POST /api/lxmf/send`, `GET /api/lxmf/inbox`,
`POST /api/lxmf/inbox/get`, `/api/lxmf/inbox/rm`, `GET /api/lxmf/outbox`, `POST /api/lxmf/sync`; `GET /api/deny`,
`POST /api/deny/add`, `/api/deny/rm` (`{"entries": [...]}`). `POST /api/jobs`
takes, for a get, the JSON booleans `sealed_name` (write a sealed file this node opens under its safe sealed
name in the output's directory; the CLI sets it for `rnfs get <bare cid>` without `-o`, so it no longer calls
`GET /api/info` first) and `fallback_dht` (a get with `from` that fails, other than by cancellation,
`TooLarge` or `StorageFull`, is retried once through the DHT). `POST /api/lxmf/send` and `/api/lxmf/sync` take
`nearest`; `POST /api/lxmf/inbox/get` takes `use_sent_cid` (without it, an unresolvable name link is 404);
`GET /api/lxmf/address` returns `propagation_node` (the configured one only) and `nearest_propagation_node`.
Core routes gained options: `POST /api/add` `pin`, `to`, `to_self`, `seal_name`,
`chunker` (and the same as query parameters on `/api/add/stream`); `POST /api/get` `raw`; `GET /api/cat`
`raw=1`, `block=1`; `GET /api/info` `from`.

## Threads

| Thread | Owner | Does |
|---|---|---|
| `rnfs-join`, `rnfs-publisher`, `rnfs-names`, `rnfs-maintenance` | node | joining (and joining again while under `MIN_SERVERS`), publishing, name announces, periodic saves and eviction |
| `rnfs-announcer`, `rnfs-response-monitor`, `rnfs-link-sweeper` | network | rnfs.dht announces, `/blocks` deadlines and displacement, idle links |
| `rnfs-lookup`, `rnfs-fetch`, `rnfs-find-providers`, `rnfs-remote-pin`, `rnfs-publish-lookup` | per operation | lookups, fetch workers, provider search, remote-pin fetches (one thread per source with pins waiting), a publishing round's lookups (up to 4 at once, ended with the round; none on slow nodes) |
| `rnfs-write` | storage | an add's, shared-folder import's or archive import's block writes (`Session.writer()`), up to 16 at once, ended with it |
| `rnfs-job-<id>` | jobs | one per running job, at most `max_jobs`; `stop()` waits ≤ 2 s |
| `rnfs-keepers`, `rnfs-keeper-query` | keepers | pending refreshes (≤ 64 queued), own-name sync, saves, re-asking every `announce_interval`; parallel `/name` queries |
| `rnfs-follow` | follow | every check and fetch, one at a time |
| `rnfs-deny` | deny | every deny-list source's check and fetch, one at a time, once joined |
| `rnfs-events`, `rnfs-job-watch` | rnfs.Node | callbacks into the program, one at a time; polling watched jobs (both started on first use) |
| `rnfs-client-events`, `rnfs-client-job-watch`, `rnfs-client-name-watch` | rnfs.Client (in the program) | callbacks; polling watched jobs; the `GET /api/name/watch` long poll |
| `rnfs-lxmf`, `rnfs-lxmf-fetch` | lxmf | announces, propagation-node downloads, polling outbound states each second; auto-fetch, one at a time, senders in turn (LXMF runs its own job thread) |

The API and gateway run one thread per connection, capped (32 and 64).

Locks are taken in this order, never the reverse: `names._lock` → `publisher._lock` → `node._lock` → `storage._lock`
→ the state database's lock (`statedb`, innermost: a store opens a transaction only once it holds every lock the
change needs, and inside it takes no other lock and does no network I/O). `node._deny_lock` (one deny-list change
applied at a time, `_apply_deny`) is taken before the publisher's lock and never with the names lock;
`DenyList._lock` holds only the state database's. Feature locks held across `storage.pin`/`unpin`/`protect` and
`publisher.refresh()` (`JobsFeature._lock`, `FollowFeature._lock`, `LxmfFeature._lock`) come before the publisher's
and storage's. The public API's locks come after these: `Rnfs._watch_lock` after the names lock (NameManager's
record listeners run under it, and only queue), the event and job-watch conditions innermost; callbacks into the
program run with none of them held. `api.NameWatches`' lock is taken before the names lock (it watches and unwatches
in NameManager, one watch per name it holds) and never under it. `NameManager.publish`, `remove`, `release_gone` and
`reconcile_pins` pin and unpin, and so call `publisher.refresh()`, while holding the names lock; `Publisher.refresh`
recomputes what is provided under its own lock, reading the node's fetched targets (`node._lock`) and walking the
pins' DAGs (`storage._lock`); the node reads storage under its lock (`_prune_fetched`). This is how the code behaves
today, not a design goal. Batched saves write outside these locks, from the changes collected under them, one save
at a time behind a save lock taken first (`Storage.save`, `Publisher.save`, `ProviderStore.save`, `DenyList.save`,
the name floor, the fetched targets, the remote pins waiting), so an older snapshot never lands last. Slow work that
runs while a lock is held:

- `Publisher.refresh` checks every pin's DAG (`dag_blocks`, memoised) under the publisher lock, behind the
  names lock when a name changes, but only when something it is computed from changed: the node's generation
  (`Storage.changes()` and the fetched targets' change count, read under `node._lock` then `storage._lock`), the
  deny list's CID set, the key sources' keys or the strategy. A refresh that changed none of them returns at once
  (about 1 µs, from 7 ms at 10 000 pins); the generation is read before computing, so a change made meanwhile
  makes the next refresh compute. When it does compute, it builds the set of blocks served again only when a
  provided CID went or its DAG's blocks changed (CIDs only added are unioned in).
- `Storage.evict` and `gc` decide what to delete, and delete it, under the storage lock (after reading the DAG
  nodes outside it); `verify` checks completeness and re-reads the blocks outside it.
- Every pin and unpin commits its row durably (`synchronous=FULL`, after the blocks are synced) under the
  storage lock (see `storage.py`).

## State files

`<rnfs dir>/state/rnfs.db` (rnfs/statedb.py), one table set per store; "FULL" commits are on disk when the
change returns, "NORMAL" ones are batched saves a power loss may lose the last of:
- storage: `pins` (one row per source; each pin and release FULL, after the blocks are synced) and `cache`
  (cached fetches, LRU order in `used`, at most 10 000; NORMAL every 30 s and at stop, changed records only);
- `pin_names` (rnfs/pin_names.py: where each pin was added from, local only, pruned to the pin list; FULL);
- node: `fetched` (targets fetched for the user, for `roots`; with the cache records) and `remote_pins` (remote
  pins accepted and not yet fetched, in order; about once a second and at stop, rewritten whole); fetcher:
  `invalid` (invalid roots, oldest first; with the cache records);
- `published`, `published_earlier`, `withdrawals` (publisher: last rounds, earlier rounds' nodes, pending
  withdrawals; NORMAL every 10 nodes or 30 s of a round and at stop, changed items only);
- `provider_publishers`, `provider_records`, `provider_departed`, `provider_evicted` (NORMAL every 300 s and at
  stop, changed rows only);
- `names_published`, `names_held` (sequences as decimal text, up to 2^64 − 1; FULL, changed rows only; a
  malformed row stops startup, as a malformed `names.json` did, since skipping it could allow rollback) and
  `name_floor` (highest sequence of names resolved for the gateway or auto-fetch and when, in `used` order, at
  most 4 × `name_table_size`, or 8 × for names resolved within 7 days; NORMAL, saved by a resolve or record
  raising it, at most every 5 s, and by the re-announce loop when changed);
- `shared_folder` (FULL); `jobs` (record per job in creation order; FULL, including progress saves; results keep
  at most 100 warnings plus `warnings_total`, older results capped at load); `keepers` (records re-verified at
  load; NORMAL); `follow` (FULL); `contacts` (FULL); `lxmf_messages` (inbox: 500 messages, 4 MiB, 50 per
  non-auto-fetch sender, 16 KiB per record; outbox 200, unfinished messages marked failed at a restart; NORMAL,
  changed messages only; ids and `next_id` stay below 2^63);
- `deny_entries` (the local deny list, canonical text; FULL), `deny_lists` (each source as `dump()`ed text, the
  CID of the version it last accepted; FULL) and `deny_hashes` (the content SHA-256 of files checked against
  `sha256:` entries, by binary CID, LRU order in `used`, at most 10 000; NORMAL with the periodic saves and at
  stop, changed rows only; a CID's content never changes, so a row never goes stale);
- `meta` (per-store values: follow's and each box's `clean`, set by every save, which the reconcile at start
  requires; each box's `next_id`) and `migrated` (JSON files imported).

Free text that may hold lone surrogates (file names, LXMF records, cache paths) is stored as JSON text
(`statedb.dump`). Rows that fail the checks the JSON loaders applied are dropped at load with a warning and
deleted. The JSON files of 0.1.4 and earlier, once imported, stay as `<name>.migrated`. Elsewhere:
`storage/identity`, `storage/lxmf_identity` (0600), `names/<alias>` (name keys), `lxmf/` (LXMF's ratchets,
delivered-message IDs, stamp costs), `api_token` (while the daemon runs), `daemon.lock` (locked while the daemon, or an embedded `rnfs.Node`, uses the
dir: `node.lock_rnfs_dir`).
Outputs being written live beside the output as `.rnfs-get-<random>.partial` (`RnfsNode.get`) or `.rnfs-job-<id>.partial` (get jobs) until renamed; a failed, cancelled or shut-down job removes its partial,
and a resumed job removes one left by a crash. A get job runs `RnfsNode.get` with its `.rnfs-job-<id>.partial` as
the output, so it is written in a `.rnfs-get-<random>.partial` beside that and renamed twice.
A directory partial is private while written (audit CONT-2), so nobody can swap one of its directories for a
link and have the DAG's files created elsewhere: on POSIX (`walker._FdOutput`, where `FD_WRITE`) it is made with
`mkdir`, opened `O_DIRECTORY | O_NOFOLLOW`, checked to be this user's, `fchmod`ed to 0700 (setgid kept, so
entries still get a shared directory's group) and must then be empty; entries are created relative to their
parent's descriptor (`mkdir(dir_fd=)`, files `O_CREAT | O_EXCL | O_NOFOLLOW`), and a directory is reopened from
the root, one component at a time, checked against the device and inode recorded when it was made. The stack
of directories still to write holds those names and identities, not descriptors, so only a few are open
however many directories a DAG has. The root gets its original mode back when complete. On Windows
(`walker._PathOutput`) the root is made with `winacl.make_private_dir`, entries by path, and each file's final
path must be inside the root; `RnfsNode.get` then restores inheritance (`winacl.inherit_from_parent`) before
the rename.

## Tests

The commands CI runs the tests with are in `.github/workflows/tests.yml`. Feature tests live in their own files
(`test_jobs.py`, `test_mesh.py`, `test_names_keep.py`, `test_contacts.py`, `test_seal.py`, `test_chunker.py`,
`test_pin_false.py`, `test_gateway_formats.py`, `test_lxmf.py`), and `test_e2e.py` has a marked block per
feature. The public API has `test_public.py` (every interface test run on an embedded node and on a Client of an
ApiServer, the name-watch endpoint, the objects, `import rnfs`'s weight) and `test_embedded.py` (the guards'
scopes, the dir lock, and with `RNFS_E2E=1` a host program over real RNS beside an embedded node). The state database has `test_statedb.py`, each store's migration a `test_migrate_*.py`, and
`test_upgrade_014.py` loads `tests/fixtures/rnfs-0.1.4/`, a node directory 0.1.4 wrote (its `generate.py`
writes it again). CI runs them on Linux, macOS and Windows; `packaging/smoke_test.py` checks a built package
(`--version`, `--help`, daemon and status, an add, and that the state database holds the pin and imported a
0.1.4 state file).

## Status

Everything in the specification is implemented, with these simplifications and additions:
- The routing table isn't persisted. At startup it is seeded from the rnfs.dht server announces that RNS
  cached within 2 × `announce_interval` (`Identity.known_destinations`, an RNS internal), then the join runs.
- Lookups (`RnfsNode._lookup`) start from the closest nodes `/find` replies would give, then others to make up K,
  then unreliable ones if still short; answers go to `RoutingTable.answered`, failures to `failed`, and the
  join's lookup is wired the same way. A failed `/provide` or `/blocks` isn't counted there.
- Joining (`RnfsNode._join`, `join`): bootstrap nodes are asked one at a time with `PeerClient.find`, outside
  any `Lookup`, until `BOOTSTRAP_ANSWERS` (2) answer; the lookup that follows excludes them. The thread then
  checks `RoutingTable.live_count()` every `REJOIN_DELAY` (300 s) and joins again while it is under
  `MIN_SERVERS` (3), doubling the wait up to `announce_interval`. `DEFAULT_BOOTSTRAP_NODES` lives in
  `rnfs/constants.py` and changes only with a release; the e2e test turns it off (`default_bootstrap = false`).
- Publishing runs up to 4 closest-nodes lookups at once (`PUBLISH_LOOKUPS`, on `rnfs-publish-lookup` threads);
  slow nodes run them one at a time. Lookups aren't shared between keys that have the same closest nodes, but
  `/provide` requests are batched per node, in the same batches one at a time would build. The airtime controls
  apply to `/blocks` only, and overlapping lookups send the same `/find` messages. Slow nodes pace these
  lookups (at most 10 per round, 30 s per lookup on average), which only delays republishing; withdrawals
  are sent at once.
- Withdrawals are sent once, with any provides due for the same node. One that fails isn't retried, since the
  record expires after `PROVIDER_TTL`. A CID is withdrawn from the nodes of its last round and from nodes of
  earlier rounds, or of a round cut short, that may still hold its record (`Publisher._earlier`, persisted in
  `published_earlier`, forgotten after `PROVIDER_TTL`). Each node that accepts a key is added there as its
  `/provide` returns, and the publish state is saved every 10 nodes or 30 s of a round
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
  `dir_set`/`dir_rm`, `export`, `export_stream`, follow, remote pins and LXMF auto-fetches. A target whose DAG
  is stored complete already isn't walked: it is checked whole against the deny list and, with
  `max_dag_bytes`, against the quota (`Storage.dag_bytes`), so a remote pin or auto-fetch of a DAG held already
  meets the same quota as one fetched. Why the builder's
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
  header and its decryption by `get`, `cat` and `info`, which run `fetch_dag` only to check a file against the deny
  list's file hashes) count the bytes of the
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
- Roots found invalid are remembered in the state database's `invalid` table (at most 10 000, oldest dropped), and there is
  no command to clear them. Cache records are capped at 10 000 (LRU). Both are saved every 30 s and at stop.
- A remote pin's usage is the larger of its content size and its DAG's block bytes. It is dropped once the
  root block shows the quota would be exceeded, or once the blocks fetched exceed it. A source's remote pins
  are fetched one at a time, oldest first (`_remote_pin_worker`), so the quota check counts every earlier pin;
  at most 64 (`REMOTE_PIN_QUEUE`) may be waiting or fetching, and further `/pin`s are refused. A worker leaves
  `_remote_workers` in the same critical section in which it finds its queue empty, so a `/pin` arriving as it
  exits starts a new one; if `Thread.start()` fails the pin is given back and refused. The pins waiting are
  saved to the `remote_pins` table by maintenance (when changed; rewritten whole, a few rows) and at stop, and `_resume_remote_pins` queues
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
  in flight. It never cancels a response's Resource (`Resource.cancel()`), and nothing in the fetcher may: on
  RNS 1.5.6 that reaches the provider as a rejection, which before a quarter of the response is sent cools the
  requester down there (`RequestServer._concluded`), while a closed link doesn't. In private mode, incoming
  links not identified as a member's are closed first to make room.
- A shared-folder entry that fails to import because of its content (an invalid name, the deny list, too large)
  isn't tried again until it changes; one that fails for a reason that may pass (`OSError`, including a pin the
  state database couldn't save, or `StorageFull`) is tried again `RETRY_AFTER` (600 s) later, and an unchanged
  entry whose pin fails is logged without ending the scan.
- The shared-folder scan and `add` walk directories by file descriptor (`O_NOFOLLOW`, `O_NONBLOCK`, type and
  inode checks), so entries swapped for symlinks, FIFOs or devices mid-scan are skipped. The scan (not `add`)
  skips regular files whose opened descriptor has `st_nlink > 1` (`builder.hard_links`, `build_open`'s
  `skip_hard_links`); such a file's signature is marked, so it is imported once its other links are gone.
- Each scan resolves the shared folder it opened (`shared_folder.opened_path`: `fsutil.final_path` of its
  descriptor, the path walk's checked root, or else its real path with the same device and inode) and refuses one
  that is the rnfs dir (`SharedFolder`'s `rnfs_dir`, `config.root` in the node), a directory holding it, or inside
  one of `PRIVATE_SUBDIRS` (`storage`, `names`, `state`, `blocks`, `lxmf`, each resolved): a warning while it
  stays refused, and the scan treats it as empty (`current = {}`): nothing imported, every `folder` pin released
  and the shared_folder table emptied, so pins a 0.1.5 node made from such a folder go at the first scan after the
  upgrade. The walk then reads that descriptor, so a link retargeted after
  the check can't redirect it. On POSIX, the first directory above it owned by another user (not root) or writable
  by group or others (unless sticky and what is below it is the user's or root's) is warned about once
  (`loose_ancestor`).
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
  `/name`; a fast node with no known sequence and no established keepers asks all 3 batches (up to 9). The
  query's state is a `_KeeperQuery` (`search`, `next_batch`, `ask` on a thread per keeper, `settled`, `resume`).
  Its search stops (`should_stop`) once enough candidates are found, or once some are and `lookup_timeout` /
  `SEARCH_SHARE` (5) has passed; candidates aren't asked while it runs. Each batch is joined for
  max(`Lookup.remaining()`, `name_wait(config)`), the latter 87.5 s at the defaults. When the candidates run out
  unsettled with batches left, `resume` runs `find_providers` again, but only if the last search was stopped
  early (`cut_short`) and some of its candidates were asked, so resumes stay within the batch bound and the
  lookup's deadline.
- **Names.** A key imported, or found without a `names_published` row, is marked `imported`: its first publish
  offers kept records, makes a path request and queries keepers before choosing a sequence. `publish()` pins the new
  target, saves its row (the commit point; a failed commit rolls the record back and releases the new pin) and only
  then releases the old target, so the saved target is pinned at every step. Once the features are set up,
  `reconcile_pins(storage.pins())` releases the pins of name keys deleted by hand (`release_gone()`, whose releases
  the rest of the pass skips), pins a recorded target again if its pin is missing, and releases `name:` pins nothing
  records; a key without a saved record keeps its pins until its next publish or removal (`_stale`, where a release
  that failed with `OSError` also waits). A key file that is there but can't be loaded is `_unreadable`: its pins
  are kept and its row is never touched by saves. `Storage.pin`/`pin_many`/`unpin` undo their change in memory if
  the commit fails. Follow and LXMF do the same for their sources at start (`FollowFeature._reconcile`,
  `_release_unrecorded`), but release only if their state loaded whole (the `clean` meta value, set by every save;
  `MessageLog.clean`); otherwise unrecorded pins are recorded again (the name followed again, or an inbox message
  per sender listing them), so a later start keeps them too. The name table marks entries that were only heard
  (`_heard_only`, its own LRU, so finding one to evict is O(1)); an announce with no waiting resolve, of a name
  neither kept nor watched, may only evict one of those (`watch()` takes a name out of `_heard_only`). A name
  resolved for the user, or published here, leaves the table and the floor (`_hold`). A resolve with `persist=False`
  (gateway, LXMF auto-fetch) raises the name's floor (`_floor`: name -> (sequence, wall time last resolved), in
  resolve order), which `_offer` checks. A raise (or a new name) saves the floor at once if the last save was
  `FLOOR_SAVE_INTERVAL` (5 s) ago, else from a one-shot `threading.Timer`; a raise by a record heard for a name in
  the table (`_store`: an announce, a keeper's record, or one a resolve's refresh heard) is always the timer's,
  started at once when a save is due, since its callers hold the lock; renewing only the time marks it for the next
  save after an hour. `_trim_floor` drops the least recently resolved past `FLOOR_FACTOR` × `name_table_size`, but
  stops at one resolved within `FLOOR_KEEP_AGE` (7 days) until the floor holds `FLOOR_HARD_FACTOR` (2) times that.
- **Deny lists.** `RnfsNode._setup` makes the `DenyList` right after storage, gives it to storage (`storage.deny`),
  and `reload`s the accepted lists from their pinned blocks (`_read_deny_list`), so the publisher's first refresh
  already leaves out denied content. Fetches check every expectation in `_ensure` and the root in `Fetcher.open`,
  and a complete stored DAG in `fetch_dag`'s early return (`dag_blocks`). `NodeBackend.served_block`/`_size` answer
  None for a denied CID; `Publisher._compute` drops CIDs whose `dag_blocks` hold one (`_denied_dags`, memoised per
  DAG and deny set). `_apply_deny` (the list's listener, and maintenance when `deny.cids` changed, as at start)
  refreshes the publisher, `drop_denied`s the cache, releases `remote:` pins of `denied_roots` and collects, and
  warns about the other pins it keeps. File hashes: `_hash_check` (for `builder.checking`, in adds, the shared
  folder and imports, under `_refusing`, which collects what a refused add or import stored) and
  `_check_file(expect, get)` (get's write-out check, a sealed get, `read` before its first byte,
  `node_content.block` for a raw block, `export_stream` for every file before `start` and `import_archive` for the
  complete roots; a raw file's digest is its CID's, others are hashed once and remembered, across restarts:
  `deny_hashes`; a refusal by hash logs a NOTICE suggesting `rnfs deny add <cid>`, since the file's blocks are still
  served, once per file CID in `_hash_noticed`, cleared at `HASH_NOTICES` (1024)). `get` serves the file's whole DAG, stored and checked: `_check_fetched` (a sealed get, `read`)
  runs `fetch_dag` first unless the hash is known, which also brings `max_download_size` and `check_disk`. Before
  hashing, a file declaring more than `DENY_HASH_RATIO` (16) × `walker.leaf_bytes` + `DENY_HASH_SLACK` (64 MiB) is
  refused as `Denied`, so `_refusing` collects what a refused import stored: hashing reads every occurrence of
  repeated content, and six blocks (one chunk, five nodes each listing the one below 256 times) declare 2^56 bytes.
  `leaf_bytes(expect, get, stop_at)` reads each distinct file node once and adds each distinct raw child's size
  once, never reading a raw block (a size over `MAX_BLOCK_SIZE` is `InvalidDag`), and stops at the bytes needed,
  so the check costs the DAG's nodes at most and the hash at most 16 × the distinct chunks plus 64 MiB. A failed `_apply_deny` leaves
  `_deny_applied` as it was, so maintenance tries again after `DENY_RETRY` (60 s). `remote_pin` refuses a denied
  CID; jobs don't fall back to the DHT on `Denied`, and LXMF auto-fetch records it as refused.
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
