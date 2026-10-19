# rnfs implementation design

How the code is organised to implement [specification.md](specification.md). The spec is the
authority on behaviour; this file fixes module boundaries and the interfaces between them.

## Shared foundation

- `rnfs/constants.py` — protocol constants (spec section 12), request paths, aspects, DHT modes, and
  `MISSED_ANNOUNCES` (2): the announces a node or name may miss before the routing table, name resolves, follows
  and deny lists count it gone or stale (`2 × announce_interval` in the spec).
- `rnfs/config.py` — `Config`: every local setting (section 12), paths under `<rnfs dir>`, and the `effective_*`
  helpers for settings that depend on slow nodes or links. Loaded from `<rnfs dir>/rnfs.toml` plus command-line
  overrides (and `defaults` beneath the file: an embedded node's); `validate()` checks every core setting's type and
  range (`WHOLE_NUMBERS`, `RATES`, `DURATIONS`, `FLAGS`, `IDENTITY_LISTS`), and the daemon exits on an invalid one.
  An unknown setting is refused too, but a retired one (`RETIRED_SETTINGS`, name -> what replaced it: `max_records`
  since 0.2.0) is left out by `Config.combine`, wherever it comes from, with a warning in `Config.warnings` (an
  embedded node's own defaults silently), so an `rnfs.toml` an older version accepted still starts the daemon; the
  settings schema doesn't list it, so `rnfs config set` refuses it and `unset` removes it.
  Reticulum is started from `reticulum_dir`, by default RNS's own config directory, so the node attaches to `rnsd`
  with the matching RPC key. `member_set` and `remote_pin_set` are `members` and `remote_pin_identities` as
  frozensets of lowercase hex, normalised once there for the node, the link pool and the request server.
- `rnfs/ids.py` — stdlib only: the one parser of 16-byte IDs written as 32 hex digits (node IDs, names, identity
  hashes, LXMF addresses). `parse_hex16(text, what, *, brackets=False, upper=True)` takes whitespace around the
  digits, RNS's `<angle brackets>` only where `brackets`, and nothing between them (`bytes.fromhex` alone would skip
  whitespace there); `is_hex16` is its check and `normal_hex16` the lowercase form. `api.parse_hex_id`,
  `names.parse_name`, `identity.parse_node_id`, `contacts.parse_hex`, `lxmf.parse_address` and `Config.validate`
  use it. Persisted rows are checked against its patterns (`HEX16`, `HEX16_LOWER`) as they are, unstripped, so
  what loaded before still loads.
- `rnfs/errors.py` — `RnfsError` and subclasses `InvalidDag`, `NotFound`, `TooLarge`, `StorageFull`, `Busy`,
  `Denied`, `Cancelled` (a fetch stopped by its observer), `Closed` (an `rnfs.Node` or `rnfs.Client` used after
  `close()`).
- `rnfs/content/cid.py` — `Cid` (binary form `.binary`, `.codec` `RAW`/`DAG`, `.key` DHT key, `.verify(block)`,
  `str()` written form, `Cid.parse`, `Cid.from_bytes`, `Cid.for_block`). Code passes `Cid` objects everywhere;
  strings only at the user edge (CLI, API, gateway, logs) and binary only on the wire. `Cid.from_block_name` is the
  block store's scan at startup: it accepts exactly what `parse` accepts, but checks the canonical form with one
  regular expression and decodes with `int(..., 32)` instead of re-encoding, keeping the name as the written form.
  `to_base36` and `from_base36` are the gateway's host form (`k` + base36, section 4), which only the gateway's
  host parser (`gateway.parse_host`) reads: exact length and digits by a regular expression (`int(..., 36)` alone
  would also take `_`, signs, whitespace and non-ASCII digits), then `Cid.from_bytes`, then the canonical check.
- `rnfs/log.py` — logging through RNS's logger. `log()` escapes non-printable characters in every message
  (remote names can't inject terminal escapes or fake lines); `cap_warnings` keeps a result's first
  `MAX_WARNINGS` (100) warnings plus a count line and `warnings_total`. With a debug sink set
  (`set_debug_sink`), every message also goes to it, whatever RNS's loglevel. `ThrottledLog(interval, clock)`
  logs one line about a kind of event at most every interval (60 s), with a count of those left out; `due()` is
  the check alone. The response guards, the request rate limit (`RequestServer._over_rate`), `RnsPaths` and the
  peers listing use it. An unexpected exception caught by a broad `except` (a `# noqa: BLE001` reason says why
  it's caught) is logged at `LOG_WARNING` inside the `except`, so the debug log gets its traceback: among them the
  fetcher's `/blocks` worker, the lookup's routing-table callbacks and `LinkPool.request`.
- `rnfs/debuglog.py` — the debug log file (`debug_log`, `daemon --debug-log`), `<rnfs dir>/debug.log` (0600),
  started by the daemon before the node: version first, then every rnfs message (warnings and errors logged in
  an `except` block with their traceback), RNS's own log lines at Info or more severe (`RNS_LEVEL`; copied from
  `RNS.logdest = LOG_CALLBACK`, which still prints all of them), threads that die (`threading.excepthook`), a status line every 5 minutes (`status_line`:
  the node's status, open links, each interface up or down), and gateway requests, numbered (`tag`). Each start
  and every 16 MiB moves the file to `debug.log.1`. Lookups report their queries through `Lookup(report=)` (dht
  stays free of RNS at import), links their path, close reason and each request's outcome, all at `LOG_DEBUG`.
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
`app:lxmf:<address>`, `storage.LXMF_PREFIX`) are `lxmf.delivery` destination hashes, all written as 32 hex
characters.

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
  with a message saying so. `connect_memory(stores)` makes a private in-memory database with only the tables of
  the schema steps of those labels (a memory-only `ProviderStore`).

Persistent state lives in the state database. Each store but the provider store (which reads its records per key,
`providers.py` below) keeps its state in memory, which is all reads use,
loads it once at startup (checking every row as the JSON loaders checked every record: a malformed row is
logged, skipped and deleted) and then writes only what changed: a pin is one row inserted or deleted, batched
stores write the records changed since their last save. The shared parts are in statedb: `open_store(state_dir,
load)` connects and loads, releasing the reference if the load fails; `delete_rows` deletes the rows a load found
malformed; and a `RowStore(db, table, key_cols, *, durable, mode)` keeps the keys a store changed (`changed(key)`,
under the store's lock) and not yet written, and writes their rows (`row(key)`, None once gone) in one
transaction: `write_through` at once, under the store's lock, raising if it fails; `snapshot` at `save()`, taking
the rows under the lock and writing them outside it, one save at a time so an older snapshot never lands last.
Either way a failed write keeps the keys for the next one, and the change stays in memory. pin_names, contacts,
jobs and the publisher are built on it (the other stores move over later). `reconcile_pins` is the start-up
check of a feature's pins against its saved state (released if it was saved whole, recorded again if not).
At every `connect`, `statedb.migrate` first imports
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
  constants.py config.py ids.py errors.py log.py   shared foundation
  debuglog.py         the debug log file, <rnfs dir>/debug.log (debug_log)
  fsutil.py           cross-platform file operations: replace/remove retried on Windows, O_BINARY, IS_WINDOWS
  statedb.py          the state database, <rnfs dir>/state/rnfs.db: schema, transactions, JSON state migration
  content/            no networking; uses RNS's bundled msgpack and RNS logging, so importing it loads RNS
    cid.py            CIDs (section 4)
    dag.py            DAG node encoding, strict decoding, names (section 5)
    chunker.py        fixed and FastCDC chunkers (section 5)
    builder.py        building trees: files, directories, wrap, only-hash; BuildOptions (section 5)
    seal.py           sealed files: SealingReader, open_sealed/SealedFile, recipients (section 5, Sealed files)
    walker.py         Expect, reader checks, path lookup, byte ranges, listing, writing out (sections 5, 7)
    edit.py           dir set / dir rm (section 11)
    blockstore.py     one file per CID in 1,024 subdirectories, atomic, verified (section 8)
    storage.py        Storage: blocks + pins (sources) + cached fetches + protection + eviction/GC (section 8)
    archive.py        export/import stream; archive_size for an exact Content-Length (section 8)
    tarstream.py      directories as tar streams: build_tar (an upload), plan/length/chunks (a download),
                      folder_chunks (a local folder, for sealing)
    shared_folder.py  importing <rnfs dir>/files/ with pin source "folder" (section 8)
  dht/                no RNS dependency
    routing.py        RoutingTable: buckets, seniority, failures, unreliable nodes, bans (section 6)
    routing_state.py  RoutingState: the routing table saved in the state database (section 6)
    scores.py         Scores: local scores per peer, decay, bootstrap seeds, the capped saved table (section 6)
    firstseen.py      FirstSeenLog: when each server's announce was first heard; closest old servers (section 6)
    providers.py      ProviderStore: records on disk, TTL, record budget and storage tiers, /find replies (section 6)
    lookup.py         iterative lookups with alpha/beta, time budget, unknown-ID budget (section 6)
  network/            everything that talks to RNS
    protocol.py       message encoding/validation, batch sizes (sections 3, 6, 7, 8)
    rates.py          rate estimates, slow links, slow node detection (section 2); the /blocks size rules both
                      ends share: response_capacity, default_upload_cap (sections 3, 7)
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
  apps.py             AppRegistry: the apps rnfs app add issued tokens to, their verifiers (section 11)
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
- `FILE, DIRECTORY, INDEX = 1, 2, 3`; `KIND_FILE, KIND_EXECUTABLE, KIND_DIRECTORY = 0, 1, 2`, and `KIND_NAMES`
  (`file`, `executable`, `directory`: how `ls` shows them).
- Frozen dataclasses `Child(cid, size)`, `Entry(name, cid, size, kind)`, `IndexChild(first_name, cid, size)`;
  nodes `FileNode(children)`, `DirectoryNode(entries)`, `IndexNode(children)`, each with `child_cids` (the CIDs it
  lists, in order) and `names` (its entries' names or children's first names; none for a file node).
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
  child that would hold `name`; keep stepping until a directory entry is reached). `index_child(node, name)` is
  that index rule (the child with the greatest first name <= `name`), shared with `Storage`'s path walk.
- `range_children(expect, block, offset, length) -> list[tuple[Expect, int, int]]` — children of a file node
  overlapping a byte range, with the range relative to each child.
- `parse_target(text) -> tuple[Cid, list[str]]` for `<cid>[/name…]` (ValueError if malformed).
- Local operations over a blocking `get(expect) -> bytes` (raise `NotFound` when absent):
  `step_name(expect, name, get) -> Expect | None` (one name, stepping through index nodes),
  `resolve(root, names, get) -> Expect`, `iter_dag(expect, get)` (each `(expect, block)`, each CID once,
  depth-first), `read_range(expect, get, offset, length) -> Iterator[bytes]`, `leaf_bytes(expect, get, stop_at)`
  (the bytes of a file's distinct chunks, from its file nodes only: the deny list's hash bound),
  `list_directory(expect, get, max_entries=None) -> Iterator[Entry]` (`TooLarge` past
  `max_entries`), `height(expect, get)` (DAG nodes on the longest path, checking every node; `dir set` and
  `dir rm` check the edited tree's depth with it, and it remembers each subtree it measured by its expectation,
  `HEIGHT_MEMO` 32768 least recently used, so an edit reads only the directories it rebuilt), `write_out(expect, get, dest, max_entries, check=None, created=None) -> warnings`
  (`created` gets a directory output's `(st_dev, st_ino)`, for `check_unchanged(dest, identity)` before it is
  renamed; a directory replaced while written raises `ValueError("output was changed while being written")`).

`storage.py` — `Storage(blocks_dir, state_dir, max_storage)`, thread-safe:
- blocks: `has(cid)`, `get(cid, verify=False) -> bytes | None` (with `verify`, a block no longer matching its
  CID is deleted), `put(cid, block)` (verifies, even a block already stored; atomic, the file fsynced),
  `used_bytes()`. Adds, the shared folder and archive import store through `with session.writer() as put:`,
  which writes blocks on up to 16 threads (`WRITE_THREADS`, at most 32 queued) so their fsyncs overlap, and
  has stored every one, or raised the first error, before the block exits: nothing is pinned before its blocks
  are. It is handed only blocks built from, or checked against, their CIDs. `BlockStore.sync()` syncs the
  subdirectories blocks were renamed into since the last sync (16 at a time, `SYNC_THREADS`: they share the
  filesystem's journal commits), and `blocks/` itself when a subdirectory was made; `Storage` calls it before
  saving pins or cache records, so they never refer to block names a power loss could undo.
- block layout (`blockstore.py`): `blocks/<xy>/<cid>`, `xy = shard_of(name)`, the written CID's `[-3:-1]`. `put`
  writes its temporary file in the block's subdirectory, made 0700 on first use (`_shard_dir`, under a small
  lock, since writers run in parallel). Opening the store lists `blocks/` itself only: `.partial-` files there
  are deleted, each subdirectory is checked (`_check_shard`: `lstat`, then on POSIX an `O_NOFOLLOW` descriptor's
  `fstat` for the owner and an `fchmod` to 0700; a link, non-directory or another user's raises RnfsError and
  stops the start), and every canonical-CID file there (0.1.6's flat layout) is moved into its subdirectory
  (`fsutil.replace`; if the target exists, the flat copy is deleted) before the subdirectories and `blocks/` are
  synced. A move that fails leaves the CID in `_flat`, where `path()` looks first, until a start moves it, or
  `delete` removes it. `scan(tidy)` lists every subdirectory (no links followed; a CID in another block's
  subdirectory is ignored) and with `tidy` deletes the `.partial-` files it meets, which only a start may do.
- pins: `pin(cid, source)`, `pin_many(cids, source)` (one save for them all; archive import),
  `unpin(cid, source) -> bool`, `pins() -> dict[Cid, set[str]]` (a copy), `pinned_cids() -> frozenset[Cid]` (the
  same set until pins change: the publisher's refresh reads it after every add), `has_pin(cid, source)`,
  `pin_count()`, `is_complete(cid)`.
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
  memos, its path walk the one `_path_cids` makes under the lock, `_walk_path`; completeness via `dag_blocks`),
  then decide and delete under the lock from the pins, sessions and
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
- The block index, `_sizes` (CID -> size), is also the `block_sizes` table (schema step `block_sizes`). `_put`
  and `_delete` note each change in `_sizes_dirty` (a size, or None); `_save_pins` writes them in the pin's durable
  transaction and `save()` with the cache records, each taking and writing them under the lock (`save()` syncs
  the blocks first, outside it), so an older batch never lands after a newer one. The table is trusted only after
  a clean stop: `close()` marks the store closed (`_put` raises `Closed`, `_delete` deletes nothing), syncs the
  blocks, writes what is left and sets the meta value `(block_sizes, clean)` to 1 in one durable transaction,
  unless a put is still writing (`_writing`, a thread that outlived the node's stop) or `BlockStore.untidy` (a
  partial file it couldn't delete). At the next start `_index` clears the mark durably before the block store
  is opened (so the move of flat blocks, or anything after, can't be trusted to a stale table), then loads the
  table, adding the flat blocks `BlockStore` found (`found_flat`: an older rnfs may have stored some since); with
  no mark, a 0, or a row that doesn't parse, it lists every block (`scan(tidy=True)`) and writes the rows that
  differ from the table (all of them after a bad row). A row whose file is missing is found when the block is
  read, as a damaged block is (`get` deletes it from the index). A million small blocks (NVMe, warm cache) are
  indexed in 1.2 s from the table against 5.4 s for 0.1.6's listing; a start after an unclean stop takes 7.2 s
  (1,024 directories listed, the table read and compared).
- A stored DAG node that no longer matches its CID (`DAMAGED`) has unknown children: while anything live
  reaches one, `evict()` and `gc()` delete nothing and log it once (`_held_back`) until `verify()` removes it or
  a fetch replaces it. Fetches treat such a block as not stored: deleted and fetched again.
- `dag_blocks(cid) -> frozenset[Cid]` the blocks of a complete DAG (memoised), used for "what a node serves".
  For an incomplete DAG it remembers the block it found missing (`_missing`, `MISSING_MEMO` 10 000, oldest
  forgotten first): a CID's content never changes, so the DAG stays incomplete while that block isn't stored,
  and `is_complete` is a lookup until it arrives. The walks under the lock take a DAG memoised as complete
  whole (`_reach`: the memo's set, less what is already kept) rather than walking it, except one holding a path
  node eviction kept without its other children, which is walked as before; `dag_bytes` of a complete DAG is
  summed once (`_complete_bytes`). `_delete` forgets the complete memos a deleted block breaks, which is how
  every deletion (eviction, gc, verify, `drop_denied`) keeps them exact.

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
`builder.BuildOptions(chunker, check)` (`build_file` hashes each file and passes the digest to check before the
file's root node is stored) are its hooks in the content layer; `archive.import_archive(..., check)` runs on the complete roots
before they are pinned.

## DHT layer interfaces

`routing.py` — `RoutingTable(own_id, config, clock=time.monotonic, admit=None, wall_clock=time.time)`
(`admit(node_id)`: private mode's member check, asked only for a node not in the table and without the table's
lock held, since it may block on RNS; the table is checked again once it answers):
`heard(node_id) -> bool` (proven server: announce in server mode, or answered `/find`), `answered(node_id) -> bool`
(`heard`, and reliable again), `failed(node_id) -> bool` (True if dropped), `unreliable(node_id)` (its last
`max_failures` queries or more failed, in the table or not; announces don't clear it: `_unanswered`, at most
`MAX_UNRELIABLE` (4096) nodes, least recently failed forgotten first), `may_retry(node_id)` (due again after
`AVOID_AFTER` (300 s), doubling per further failure up to `AVOID_MAX` (3600 s); True claims the retry, pushing
the next one back a further backoff, so concurrent lookups don't all retry it), `remove(node_id)`, `ban(node_id)`,
`is_banned(node_id)`, `closest(key, count=K, for_reply=False)`,
`seniority(node_id) -> float | None` (seconds in the table), `join_order()` (the servers a join asks first, before
the node orders them by score), `is_senior(node_id, min_age)`, `standing(node_id,
min_age) -> int` (0 senior and healthy, 1 healthy, 2 stale, 3 absent: ranks keepers), `standings(min_age) ->
dict` (every node's standing now: the keeper query's snapshot from before its lookup), `contains(node_id)`,
`nodes()`, `node_count()`, `live_count()` (nodes `/find` replies would include).
Every `heard` forgets expired bans and dropped nodes' seniority, looking only at the front of each: both are kept
in expiry order (an entry set again is moved to the end), and their readers check expiry too, so an entry out of
order (after a clock step or a changed setting) only lingers in memory.
Persistence (0.2.0): the table runs on the monotonic clock and converts to wall clock (`wall_clock() - clock()` as
the offset) only for saved rows. `row(node_id)` gives `(node, IN_TABLE, first seen, last heard, failures)`,
`(node, DROPPED, first seen, when dropped, 0)` or `(node, BANNED, None, ban end, 0)`, or None; `track(changed)`
sets the listener told, under the table's lock (`lock`, an RLock), each node whose row changed; `restore(rows)`
loads saved rows into an unused table (times in the future clamped to now, a ban to at most `ban_time` ahead;
expired drops and bans, malformed rows, our own ID, nodes `admit` now rejects and a full bucket's youngest left
out) and returns the nodes whose saved rows must be written again or deleted. `routing_state.py` —
`RoutingState(table, state_dir)`, a `node.Store`: loads the `routing` table into the table, then saves the rows
changed through a SNAPSHOT `RowStore` taking the table's lock; it imports the state database so `routing.py`,
which lookups import, needn't.

`providers.py` — `ProviderStore(state_dir, record_budget=1 GiB, *, ttl=PROVIDER_TTL, clock=time.time, rng=random,
max_per_key=1000, own_id=None, known=lambda id: False, free_space=None, record_bytes=None)` (`state_dir` holds the
state database; None: a private in-memory one, `statedb.connect_memory`, so tests run the same code):
`store(key, publisher) -> bool`, `withdraw(key, publisher)`, `provide(publisher, keys, withdrawn) -> int`,
`retain(keep) -> int` (drops every record and the standing of publishers `keep` rejects: private mode's
non-members), `for_reply(key, exclude=lambda id: False) -> list[bytes]`, `providers(key, exclude) -> list[bytes]`,
`record(key, publisher)`, `expire()`, `maintain()`, `is_full()`, `record_count()`, `publisher_count()`,
`tier_counts()`, `first_seen(publisher)`, `standing(publisher)`, `save()`, `close()`.

Records live on disk and are read per key. Nothing in memory grows with the records or publishers:
- `_cache`: the records of recently used keys (publisher -> (first stored, last stored)), LRU, at most
  `CACHE_KEYS` (4096) keys and `CACHE_ROWS` (50 000) rows, read through `provider_records`' primary key (joined
  with `provider_standing`, so a record of a publisher without a standing, or malformed, is dropped and deleted).
- `_dirty`: the records changed since the last save (None: deleted), by key, and `_dirty_by_publisher`; a key read
  from disk gets them laid over it, so the cache can drop any key. `_pinned_standings` holds the changed standings
  until the save, `_standings` an LRU of others (`STANDING_CACHE`, 4096).
- `save()` writes all of it in one transaction (NORMAL), then measures (`_measure`): the node's `maintain()`
  call, every 30 s (`RECORDS_INTERVAL`), expires and saves; a store saves itself once `FLUSH_CHANGES` (20 000)
  changes wait, and past `MAX_CHANGES` (saves failing) refuses others' new records rather than hold more. A refresh
  writes nothing unless it moves the last-stored time by more than an hour (`REFRESH_WRITE`), or its day is a new
  refresh day; a refusal writes nothing.

Each publisher's standing (`_Standing`: first seen, records held, tier, refresh days and the last of them) keeps
the counts the budget and tiers need: `_tier_records` (records per tier, this node's in a tier of its own, `OWN`)
and `_publishers`, read from `provider_standing` at load (`SUM ... GROUP BY tier`), and changed with each record
(`_count_records`). A publisher left without records keeps its standing only if it has refresh days, until a TTL
after the last (`maintain`'s hourly `_forget_departed`). The tier is worked out when the publisher stores
(`_tier_of`: `own_id`, then `known(publisher)`, then `EARNED_DAYS` refresh days); a contact added or removed since
is noticed when the publisher stores, when it is picked to evict from (`_largest` checks and moves it), or by the
hourly check of those written as Known.

Room: `_capacity()` is `record_budget` over the bytes a record takes, or the records held at the last measure plus
the free disk beyond the reserve (`free_space()`; the node passes `shutil.disk_usage` of the state directory less
`storage.disk_reserve`) over it, whichever is less. The bytes a record takes are measured at each save as the
database's pages in use over the records (at least `MEASURE_RECORDS`, 10 000: below that the database is mostly
the rest of the state), so indexes, standings, fragmentation and the other state count; `record_bytes` fixes it
(tests). A full store (`_full`) first sweeps for expired records, at most once a minute; then `_reclaim` applies
the tiers' rules (FLOORS 10/60/30%), evicting from a tier's largest holder the record it refreshed least recently
(`_oldest_key`, by `provider_records_publisher`). A tier's largest holder (`_largest`) is the larger of the changed
standings' heap (`_heaps`, stale entries dropped as they surface) and the first standing on disk, by
`provider_standing_tier`, not changed since (`_disk_top`, a page of 64 at a time, kept until the save changes the
table); both, and `_oldest_key`'s position in each publisher's records, are reset by a save. So a flood's store
costs a few reads and O(log n) in memory: about 70 µs on a desktop, a third of them SQLite's. Once a batch's key is
refused for room, `provide` refuses its other new keys without looking again (NEW-8).

A key keeps at most `max_per_key` publishers (`_key_room`): the K/2 oldest records stay, and a random one of the
lowest tier among the rest gives way if that tier is below the storer's, or is the storer's and not Earned.
This node's records are never counted there, never evicted and never refused. Expiry (`_expire`) reads expired
records through `provider_records_last`, `SWEEP_BATCH` at a time with the lock let go between batches, and, while
full, Newcomers' records last stored 4.5 days ago (`_NEWCOMERS_EXPIRED`, joined with their standing; the tier
checked again before each goes). Times later than now (a clock corrected backwards) are clamped to now at load
(if the latest last-stored time is in the future) and at the first `expire()` after the clock went backwards: the
store notes the latest clock reading any time it holds came from (`_latest`, via `_now()`), and an `expire()`
with the clock not behind it skips the clamping pass. `_check_consistency()` recomputes the counts and standings
from the records on disk and in memory (`_merged`) and checks the caches against them; test_providers runs it
after every test.

`lookup.py` — network access is injected:
`FindQuery = Callable[[node_id, key], tuple[list[bytes], list[bytes]] | None]` returning (providers, nodes).
`Lookup(key, start_nodes, query, own_id, alpha, beta, k, timeout, is_known, on_answer, on_failure, exclude, report,
unreliable, may_retry, max_queries, on_useful)` with `find_closest() -> list[bytes]` and
`find_providers(on_provider, should_stop) -> list[bytes]`. Besides completion and the deadline, a run (`_iterate`)
ends once it has sent `max_queries` (`MAX_QUERIES`, 8 × K) queries and their answers are in ("query cap"), or after
`STALL_WINDOWS` (3) × max(alpha, beta) answers in a row that leave the bit length of the beta-th closest known
distance (`_reach`) unchanged ("no progress"). A run ranks the nodes `unreliable` named when it learnt of them
(`doubtful`) after every other, for its queries, its termination and `find_closest`'s result; one that distance
alone would rank within the query window is asked again when `may_retry` says so, as a query beyond `alpha`
(`retrying`) that the run waits for only if the node comes to rank within the window by `rank` (it is then counted
in flight). `is_known(id)` says whether RNS knows its identity; at most `alpha` unknown IDs are tried per lookup
(`allow_unknown(id)`, also used by the fetcher for provider IDs). `listing_nodes()` is the nodes that have answered
with at least one provider, and `listers(provider)` those that listed a given provider, so the keeper search can
tell who vouches for its candidates (and the fetcher whose answers led to a provider that served). `on_useful(id)`
(local scores): a run notes which node's answer first named each node (`introduced`); when a node answers that is
closer to the key than the node that named it, that node is passed to `on_useful`, once per lookup (`_useful`,
`_credit`).

`scores.py` — `Scores(state_dir=None, seeded=(), clock=time.time, max_rows=MAX_SCORES)`, a `node.Store`:
`score(node_id)` (0 without a row), `band(node_id)`, `bands()`, and `served`, `useful_answer`, `failed`,
`bad_block` adding `SERVED` (1), `USEFUL_ANSWER` (0.5), `FAILED` (−1) and `BAD_BLOCK` (−32). A row holds the score
as of `updated` (it decays by half each `SCORE_HALF_LIFE`, 7 days, computed when read or changed: nothing runs
on a timer), `useful` (last raised) and `seeded`. Rows sit in an `OrderedDict`, least recently useful first: a
raise moves a row to the end, a row made by lowering goes to the front, and past `MAX_SCORES` (4096) the front is
forgotten. `seeded` (the node's `_bootstrap_set()`: `bootstrap_nodes` and the defaults in use) gives each without a
row `BOOTSTRAP_SEED` (6); a row seeded before whose node isn't a bootstrap node any more has the seed's decayed
share taken off (exact, as every contribution decays alike). Saved in `peer_scores` through a SNAPSHOT `RowStore`.
The node wires it: lookups' `on_failure` is `_query_failed` (the routing table's `failed`, then `scores.failed`),
`on_useful` is `scores.useful_answer`, and the join's unanswered bootstrap nodes go through `_query_failed` too;
the fetcher (`Fetcher(scores=)`, in memory only if not given) calls `served` for each response with verified
blocks and `useful_answer` once per fetch for each of its lookup's `listers` of that provider (`Fetch._served`),
`failed` in `_fail` and `bad_block` with the ban.

`firstseen.py` — `FirstSeenLog(state_dir=None, clock=time.time, max_rows=MAX_FIRST_SEEN)`, a `node.Store`:
`heard(node_id)` (the node's `_heard_server`, for every server-mode announce or path response, before the routing
table), `forget(node_id)` (`_heard_client`), `first_heard(node_id)`, `len()`, and `closest(key, count=K, min_age=0,
exclude)`: the count closest to key of the servers first heard at least min_age ago (publishers and searchers will
pass `provider_seniority`), not excluded (`exclude` runs under the log's lock). Memory: an `OrderedDict` node ID ->
first heard, least recently heard first (a heard moves to the end; past `MAX_FIRST_SEEN`, 65,536, the front is
forgotten), and the same IDs in a sorted list (`bisect.insort`, a memory move: about 7 µs for a new server at
65,536, 1 µs for one heard before, 17 µs when evicting). The IDs sharing the key's first p bits are a contiguous run
of the sorted list, all closer to the key than any outside it: `closest` binary-searches the narrowest run holding
count IDs (`_run`, two bisects), then widens it a bit at a time until it holds count old enough, and ranks only that
run (about 20 µs at 65,536). Saved in `announce_first_seen` through a SNAPSHOT `RowStore`: an announce only marks
its row, and a row's `last_heard` is the time of the save writing it (within 30 s of the announce), which only
orders forgetting after a restart.

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

`announces.py` — `DhtAnnouncer(destination, config, needs_reach=None)` sets `app_data` to the mode and runs the
announce schedule; a client announces periodically only while `needs_reach()` (the node's `_needs_reach`:
provides, owns names, accepts remote pins, private mode) holds, checked every `REACH_CHECK` (60 s), announcing at
once when it starts to;
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
check goes. `verify_daemon` checks `X-Rnfs-Proof` before anything else of the answer: without a valid proof a
`503` is `Busy` and every other status `AuthFailed`, both in the client's own words, so a program on the port
chooses neither the error nor its text; it reads at most `PROOF_READ_LIMIT` (64 KiB), and connecting and proving
take at most `PROOF_TIMEOUT` (15 s, or the client's timeout if shorter), after which the request gets the client's
own timeout. A daemon that closes the proved connection gets nothing more (`ConnectionFailed`): a new connection could reach
another listener. A daemon proved older than 0.1.5 (`OLDEST_DAEMON`; such daemons close every connection) is
refused by `check_daemon` before the token is sent; until 0.2.0 the client sent it the request on a new connection. `request()` maps failures to `DaemonNotRunning`, `ConnectionFailed` (`_exchange`), `AuthFailed` (a
failed proof, or 403) and the API's statuses (`_raise_for`: `STATUS_ERRORS`, `error_for`, the one mapping);
`Stream` reads a body, raising
`ConnectionFailed` if it ends short. `ApiConnection(..., app=, token=)` (0.2.0) is an app's: its token is given
(never read again) or in `token_file` (read and checked as `api_token` is, `read_token`), every request carries
`X-Rnfs-App` (`APP_HEADER`, `_identity_headers`), and `verify_daemon` checks the proof keyed by
`app_proof_key(token)`; `rnfs.Client(app=, token=)` passes them on. Its methods (`status`, `add_path`, `add_stream`, `fetch`, `cat`, `info`,
`ls`, `stat`, `dir_set`, `dir_rm`, `export`, `import_archive`, `add_tar`, `get_tar`, `export_stream`,
`import_stream` (uploads bytes or a binary file, chunked when their length is unknown; downloads a `Stream`), `gc`,
`verify`, `pin_*` (with `pin_remote`, `pin_remote_ls`, `pin_remote_served`),
`providers`, `ping`, `name_*` (with `name_watch`, `name_import`, `name_export`, `name_rm`), `submit_job`, `job`,
`jobs`, `cancel_job`, `clear_jobs`, `deny*`) cover every core route (`ApiHandler.ROUTES`; checked by
test_apiclient), so `remote.py` writes no path itself, and send only the parameters given; `ApiError` is an `RnfsError`, and each status's error a subclass of the
node's own too (`NotFound` of `errors.NotFound`, `BadRequest` of `ValueError`, ...). The CLI's `ApiClient`
subclasses it, turning each `ApiError` into `fail(str(e))`, so its messages are the library's.

`RnfsNode(config, reticulum=None, embedded=False)` exposes `node_id: bytes`, `config`, and these methods. Targets are strings
`<cid>[/name…]`; CIDs are returned as strings. Return values are JSON-ready dicts.

| Method | Returns |
|---|---|
| `add_path(path, wrap=False, only_hash=False, pin=True, to=None, to_self=False, seal_name=True, chunker=None)` | `{"cid", "size", "warnings": [str]}` (+ `"sealed"`); file or directory, pinned `user` (or cached with `pin=False`); with `to`/`to_self` a directory is packed as one tar and sealed as `<name>.tar` (`node_content.add_sealed_path`, `tarstream.folder_chunks`) |
| `add_stream(stream, size=None, name=None, only_hash=False, …)` | as above; `name` wraps it as that entry (sealed: the sealed name) |
| `add_tar(stream, name=None, only_hash=False, pin=True, chunker=None)` | as above (+ `"warnings_total"` past 100): the directory a tar stream holds (`POST /api/add/stream?format=tar`), built by `tarstream.build_tar` as it is read; at most `max_output_entries` files and directories; recorded in pin_names as `name` or `(tar upload)` |
| `get(target, output, from_node=None, raw=False, no_extract=False)` | `{"cid", "warnings"}` (+ `"warnings_total"` past 100; + `"sealed", "name"`, the name only if `safe_default_name` accepts it; + `"extracted"` for a sealed folder unpacked, `name` then the directory's); fetches and writes the file or directory under `.rnfs-get-<random>.partial` beside `output`, then renames it to `output` (`rename_new`), decrypting a sealed file unless `raw` and unpacking a sealed folder unless `no_extract` (`node_content.get_sealed`); a directory output is first checked to still be the one `write_out` made (`walker.check_unchanged`) and, on Windows, made to inherit its folder's access list (`winacl.inherit_from_parent`; if that fails it stays private, with a warning logged) |
| `info(target, for_gateway=False, from_node=None, raw=False)` | `{"cid", "type": "file"\|"directory", "size", "executable"}` (+ `"sealed", "recipient", "stored_size", "name"`, and for a recipient `"sealed_folder"`; the sealed name is None unless `safe_default_name` accepts it) |
| `stored_head(target, length)` | a file's first `length` bytes if stored, else None; never fetches (the user's own gateway `HEAD` and ranges not at 0); `Denied` as a read: a denied CID met, or while there are file hashes a file that fails `_check_file` (one not wholly stored gives None) |
| `read(target, offset=0, length=None, for_gateway=False, from_node=None, raw=False)` | iterator of bytes of a file (range fetched only; sealed files decrypted unless `raw` or `for_gateway`) |
| `read_file(target, offset=0, length=None, from_node=None, raw=False)` | `(info, chunks)`: `info()` and `read()` in one fetch, resolving the target once; `chunks` reads `[offset, offset + length)` cut to the info's size only once iterated (a directory's raises `ValueError`), and closing it ends the fetch (`GET /api/cat`, which used to call `info` and `read`: two fetches) |
| `block(cid, for_gateway=False)` | the CID's own block, verified (`cat --block`, `?format=raw`: the gateway calls it only for the user's own GET) |
| `export_stream(target, start, for_gateway=False)` | fetches the DAG, then writes its archive to `start(exact length)` (`?format=archive`; a `start` returning `None` writes nothing). The gateway calls it only for the user's own GET (never for a limited request or a HEAD, stored or not), inside `fetcher.observing(ClientWatch)`, which cancels the fetch once the client disconnects. Built on `export_chunks` |
| `export_chunks(target, for_gateway=False)`, `get_tar(target)` | `(info, chunks)`, as `read_file`: the DAG fetched and checked first (`node_content._fetched_dag`: `max_download_size`, `max_output_entries`, the deny list's file hashes), then `info` (`{"roots", "blocks", "length"}` / `{"cid", "length", "skipped"}`) and an iterator of the archive's or tar's bytes, exactly `length` of them; closing it ends the fetch (`GET /api/export/stream`, `GET /api/get/stream?format=tar`, which run the fetch inside `observing(ClientWatch)` as the gateway does) |
| `ls(target, for_gateway=False)` | `[{"name", "kind": "file"\|"executable"\|"directory", "size", "cid"}]` |
| `stat(target)` | `{"cid", "size", "blocks", "local_blocks", "local_bytes"}` |
| `is_local(target)` | `bool`: everything needed is stored, so no network fetch |
| `dir_set(target, name, child_cid, executable=False)`, `dir_rm(target)` | `{"cid"}`, pinned `user`; `dir_rm` refuses a target that names no entry (`edit.check_entry_target`: ValueError, before fetching anything), so the API and `rnfs.Node` don't check it themselves |
| `pin_add(target, source="user")` | `{"cid"}` once complete |
| `pin_rm(cid, source="user")` | `None`; NotFound if `source` doesn't hold it |
| `source=` (0.2.0) | taken by `add_path`, `add_stream`, `add_tar`, `dir_set`, `dir_rm`, `pin_add`, `pin_rm`, `import_archive` and `import_stream`: the source their caller pins and releases with, `user` (the default: the CLI, an embedded node) or an app's `app:<name>[:<key>]` (`ApiHandler.pin_source()`; anything else is ValueError, `node._caller_source`). After pinning with an app's source the node checks the app still exists (`_app_still_there`): `app_rm` forgets an app before it looks for its pins, so a pin a request already let in makes after that is released there and then, with NotFound |
| `pin_ls(cid=None)` | `[{"cid", "sources": [str], "complete": bool, "type": "file" \| "directory" \| None, "sealed": bool \| None, "name": str \| None}]`; `type` and `name` only when the root block is stored, `name` where the pin was added from (pin_names; for a sealed add, the local path or the name given, kept for the local user), its shared-folder entry, or a directory's single entry or entry count; `sealed` from the file's first bytes (`seal.MAGIC`), False for directories, None if the root or first chunk isn't stored. A complete pin's `type`, name from inside and `sealed` are read from its blocks once, then remembered while it stays complete (`_pin_details`, `PIN_MEMO` 16 384, forgotten when released). Callers that show pins to others must skip `sealed` entries' names and content |
| `pin_remote(node_id, cid, pin)` | `bool` accepted; recorded (`AskedPins.record`) |
| `pin_remote_ls(node_id=None, cid=None, check=False)` | `[{"node", "cid", "pin", "accepted", "asked", "state", "done", "checked", "seen"}]`, most recently asked first; `check` runs `AskedPins.check_now` first (Remote pin status, below) |
| `pin_remote_served()` | `{"pins": [{"identity", "cid", "state", "reason", "at", "detail"}], "usage": [{"identity", "used", "quota", "pinned", "waiting", "allowed"}], "quota"}` |
| `gc()` / `verify()` | `{"removed_blocks", "freed_bytes"}` / `{"checked", "bad": [cid]}` |
| `providers(cid)` | `[node_id bytes]` |
| `ping(node_id)` | round-trip seconds, or None |
| `name_create(alias)`, `name_import(alias, key_bytes)` | `{"alias", "name"}` |
| `name_rm(alias)`, `name_export(alias)` | `None`, key bytes |
| `name_list()` | `[{"alias", "name", "cid" or None, "sequence"}]` |
| `name_publish(alias, target)` | `{"name", "cid", "sequence"}`, plus `"warning"` in private mode (the name's announce reaches outsiders: `names.PRIVATE_NAME_WARNING`) |
| `name_resolve(name, for_gateway=False)` | `{"name", "cid", "sequence", "age"}`; NotFound if unresolved |
| `export(target, output)` / `import_archive(path, progress=None)` | `{"roots", "blocks"}` / `{"roots", "incomplete", "blocks"}`; `import_archive` opens `path` with `O_NONBLOCK` and refuses (ValueError) anything but a regular file, so a FIFO can't hold it (or an import job) |
| `import_stream(stream, label="(archive upload)", progress=None)` | as `import_archive`, of an archive read from `stream` to its end (`POST /api/import/stream`; `import_archive` opens the file and calls it). `progress` here and in `add_path` is told each read's length (`builder.BuildOptions.progress`, `builder.counted`) and may raise to stop: an add or import job's `Job.read` |
| `app_add(name)`, `app_ls()`, `app_rm(name)` | `{"name", "created", "token"}` / `[{"name", "created"}]` / `{"released"}`: `AppRegistry` (rnfs/apps.py); `app_add` refuses `lxmf` while the LXMF feature is on (`in_use()`); `app_rm` revokes the token, calls every feature's `app_removed(name)`, releases every `app:<name>[:<key>]` pin, then deletes the row |
| `status()` | (API: without the token, only `{"version"}`) `{"version", "node_id", "identity_hash", "dht_mode", "slow", "private", "known_nodes", "records", "pins", "provided", "blocks", "used_bytes", "max_storage", "names", "unexported_names"}` plus every feature's `status()` fields (traffic, jobs, LXMF); `identity_hash` is the RNS identity hash (None on offline test nodes) |

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

Errors (`errors.STATUSES`, the one table: `api.ERROR_STATUSES` is it, `apiclient.STATUS_ERRORS` and the gateway's
`ERROR_MESSAGES` follow it): `ValueError` → 400 (with `BlockMismatch`, `SealError` and `NotRecipient`, which
subclass it), `NotFound` → 404, `TooLarge` → 413, `InvalidDag` → 422, `Busy` → 503, `StorageFull` → 507, `Denied`
→ 451, other `RnfsError` → 500. The API logs a 500 as an error and a `BlockMismatch` (a block the node itself
handed the store not matching its CID: a bug, not a bad request) as a warning. Paths passed to `add_path`, `get`,
`export` and `import_archive` are absolute paths on the daemon's machine (the CLI resolves them); stdin is uploaded
as a request body.

**Core API routes** (`rnfs/api.py` `ROUTES`): `GET /api/status` (open), `/api/info`, `/api/cat`, `/api/ls`,
`/api/stat`, `/api/pin/ls`, `/api/pin/remote` (`node`, `cid`, `check`), `/api/pin/remote/served`,
`/api/providers`, `/api/ping`, `/api/name/list`, `/api/name/resolve`,
`/api/name/watch` (a long poll for newer records: Public API, API additions);
`POST /api/add`, `/api/add/stream`, `/api/get`, `/api/dir/set`, `/api/dir/rm`, `/api/pin/add`, `/api/pin/rm`,
`/api/pin/remote`, `/api/gc`, `/api/verify`, `/api/name/create`, `/api/name/rm`, `/api/name/export`,
`/api/name/import`, `/api/name/publish`, `/api/export`, `/api/import`; app tokens (`ApiHandler.CLI_ROUTES`, the
CLI's token only: an app's gets 403 `forbidden`) `GET /api/apps`, `POST /api/apps/add`, `/api/apps/rm`
(`app_ls`, `app_add`, `app_rm`; `ApiConnection.apps`, `app_add`, `app_rm`, which neither `rnfs.Node` nor
`rnfs.Client` has: managing apps is the CLI's, `rnfs app`); for clients that share no files with the
daemon (a sandboxed app), `GET /api/get/stream?format=tar`, `GET /api/export/stream`, `POST /api/import/stream`,
and `POST /api/add/stream?format=tar`. `GET /api/info` and `POST /api/get` take
an optional `from` (node ID), as does `GET /api/cat`; `GET /api/name/resolve?name=` takes a name or a name
contact's nick.

The API requires `X-Rnfs-Api: <token>` on every request except `GET /api/status`, which without a valid token
answers only `{"version"}` (health checks, and `verify_daemon`, which reads the proof header, then the
version); the CLI sends the token with every other request, `rnfs status` included (without a readable token it
prints the version and says why). The daemon writes a new random token to `<rnfs dir>/api_token` (mode 0600, in the 0700 rnfs dir) at each start and the CLI reads it, so other
local users and processes can't drive the API. Before anything else uses the rnfs dir (the debug log, the
node), the daemon locks `<rnfs dir>/daemon.lock` (`fsutil.lock_exclusive`: `flock`, or `msvcrt.locking` of a byte
past the text on Windows; the file holds its process ID) and keeps the lock until it exits, so a second daemon
on the same dir stops at once, naming the first (`lock_holder`). The lock is taken before the node opens its block
store, and the API listens only after, so a client whose connection is refused checks the lock
(`apiclient.dir_holder`: the process `lock_holder` names, if `_process_alive`, and then `fsutil.lock_held`, a
shared `flock` (or the byte's lock on Windows) taken and released at once, writing nothing) and raises
`DaemonStarting(pid)`, a `DaemonNotRunning`, in place of "no daemon is running": `rnfs status` and every other
command say the daemon is starting. The CLI's `ApiClient` is given `--dir` for it. A start runs in this order, in the daemon
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
(anything but `GatewayError` and `NotFound`: 400, 413, 422, 451, 503, 507, 500) say only a fixed message per status
(`gateway.ERROR_MESSAGES`, which has one for every status of `errors.STATUSES`; the detail goes to the log);
malformed `Range` headers are ignored.

Neither server sends CORS headers. Gateway pages run sandboxed in an opaque origin, so they load their DAG's
files as subresources but can't read any gateway response with `fetch()`; an earlier rule allowing a gateway
`Referer` never fired, as browsers send no `Referer` from an opaque origin, and allowing `Origin: null` instead
would let any site's sandboxed frame read the gateway.

**Gateway sites** (0.2.0) give each root an origin of its own, so its pages can read their own DAG.
`GatewayHandler.handle_request` starts with `gateway.parse_host`, which takes the path gateway's hosts through
`api.host_allowed` (unchanged: the API's check stays exact) and otherwise only `<label>.rnfs.localhost:<port>` and
`<label>.name.localhost:<port>`, after lowercasing (ASCII hosts only, so no other character folds onto an ASCII
one); it returns None, `("rnfs", Cid)` (`cid.from_base36`, so from then on the CID is a `Cid` written in base32)
or `("name", bytes)` (`ids.HEX16_LOWER`), and raises a `GatewayError` (403 for other hosts, 400 for a bad label).
The result is `handler.site`; `end_headers` picks the CSP by it (`CSP_SITE_SCRIPTS`/`CSP_SITE_NO_SCRIPTS`, with
`allow-same-origin` and `frame-ancestors 'self'`, plus `Origin-Agent-Cluster: ?1`), so errors carry the site's
headers too, and a host that fails to parse gets the path gateway's (as does the over-capacity 503, sent before
the request is read). `serve` hands a site request to
`serve_site`, which refuses `Service-Worker` (403), an absolute-form target (400) and `?format=` (400), decodes
every segment with `decode_segment` (an empty one is 400), resolves a name through `resolve_name` (the same cache
and `gateway_fetches` slot) and calls `serve_target`, the path gateway's own serving, with a redirect location
rebuilt from the decoded names (`quote(name, safe="")`), so it is always a path on the site. Logs name the site
by its base32 CID or name (`path_for_log`, and `host_for_log` for the debug log's Host). No Origin header is
checked: a page's `fetch()` of its own site is same-origin and needs no CORS header, and every other origin is
refused by the browser for want of one, as on the path gateway. `rnfs url` (`cli.site_url`) prints a target's site
URL without asking the daemon: the gateway port from `daemon.json`, else `rnfs.toml`.

## Local API for apps (0.2.0)

What a companion app needs from the daemon beyond the facade calls (specification section 11, "For apps").

- **App tokens.** `rnfs/apps.py`'s `AppRegistry` (`RnfsNode.apps`, made before the features) keeps each app's
  name, its token's verifier (`apiclient.app_verifier`: SHA-256 of `rnfs app token ` and the token) and when it
  was added, in the `apps` table; `add` returns the token, the only time it exists outside the app. The API's
  `ApiHandler.caller()` answers `api.CLI` for the API token (no `X-Rnfs-App`), `"app:<name>"` for an app's token
  with `X-Rnfs-App` naming it (`AppRegistry.verify`, constant time, also for names there aren't), None
  otherwise; `dispatch` keeps it in `principal`. `proof_key()` keys `X-Rnfs-Proof` with the API token, or
  (`X-Rnfs-App`) with `AppRegistry.proof_key` (the verifier in hex), None for an app there isn't. A node without
  `apps` (a stand-in) lets no app in.
- **Discovery.** `cli.run_daemon` writes `<rnfs dir>/daemon.json` (`apiclient.DAEMON_FILE`; `cli.write_daemon_info`,
  `fsutil.write_atomic`, 0600, private) once its servers listen and its token is written, and removes it when it
  stops if it still names its PID (`remove_daemon_info`). `apiclient.read_daemon_info` reads it without following a
  symlink, refuses one other users could have written, one over 64 KiB, one without an integer `pid` and a valid
  `api_port`, and (POSIX) one whose process has ended; `api_port()` and the CLI's `client()` use its port before
  `rnfs.toml`'s, so an older client finds a daemon whose settings it can't parse. Only the port is used: the proof
  check is unchanged, so a stale or planted file at worst points at a port where nothing proves itself.
- **Error codes.** `rnfs.errors.STATUS_CODES` (the node's errors, by status), `API_CODES` (the API's own
  refusals: `bad_host`, `unauthorized`, `no_such_endpoint`, `method_not_allowed`, `timeout`, `length_required`,
  `body_too_large`, `unsupported`, `too_many_connections`, and from 0.2.0's app tokens `forbidden`) and `error_code(error, status)`, which prefers an
  exception's own `code` (`errors.Unsupported`: a 404 `unsupported`, for something this daemon can't do).
  `ApiHandler.respond_error` sends `{"error", "code"}`; `ApiRequestError` carries its code. The client
  (`apiclient.error_for`) keeps the code on every `ApiError` (`.code`) and raises `NoSuchEndpoint` for
  `no_such_endpoint` (or, from a daemon before 0.2.0, a 404 whose message starts "no such API endpoint") and
  `Unsupported` for `unsupported`, both `NotFound` subclasses, so existing `except NotFound` clauses still hold.
  `http.server`'s own error pages (a malformed request line) have no code.
- **Capabilities.** `GET /api/capabilities`: `api.CAPABILITIES` (what a route list can't say: `events`,
  `pin_details`, `sealed_folders` (add seals a folder, get unpacks one and takes `no_extract`), ...), every core
  and feature route, the loaded features, `events.EVENT_KINDS` and
  `errors.CODES`. `ApiConnection.capabilities()` answers `{}` from an older daemon.
- **Events.** `rnfs/events.py`: `EventLog`, one per node (`RnfsNode.events`, made first in `_setup`), holding the
  last `MAX_EVENTS` (1024) events, each `{"seq", "kind", "time", ...}` numbered from 1 per run, with a random
  `epoch` per run. `publish()` appends under the log's condition (the innermost lock: callers hold the names lock
  or storage's) and tells listeners (an embedded Node's event thread) without it; `events.publish(node, ...)` is
  the never-raising form for code that may run on a stand-in node. `since(after, kinds, wait, epoch)` is
  level-triggered: `after` absent answers `next` at once; events after `after`, or after `wait` none; `lost` (and
  every event held) when `after` is no longer held, is beyond the log (a restart) or `epoch` differs. Kinds and
  their fields: `pin` (`change`: added, removed or label; `source`; `cids`, at most `MAX_EVENT_CIDS` (100), and
  `count`: storage's `pin_listener`, after the pins are saved; a label from `pin_label`), `cache` (`change`: added
  or removed, `cids`, `count`: storage's `cache_listener`, from `_set_record` and `_drop_records`), `name`
  (`name`, `cid`, `sequence`: `NameManager.held_listener`, for a newer record accepted for a name held — this
  node's own, kept, followed or resolved — or watched), `remote_pin` (`side` "asked": `node`, `cid`, `state`, as
  a remote pin this node asked for is recorded or a check changes its state; `side` "served": `identity`, `cid`,
  `state`, `reason`, for each outcome of one another identity asked of it: Remote pin status, below), and `job`,
  `inbox` and `peer` from their features. `GET /api/events` waits at most 30 s, `MAX_EVENT_WAITERS` (8) calls at once (`ApiHandler.event_waiters`, a
  semaphore per server; 503 beyond).
- **Shutdown.** `ApiServer(shutdown=)`: the daemon passes its `stopping.set`, the event its signal handlers set,
  so `POST /api/shutdown` answers `{"stopping": true}`, flushes, then stops the daemon exactly as SIGTERM does
  (the servers stop, `daemon.json` and the token are removed, the node saves and stops). Without one (tests) it is
  `unsupported`. `ApiConnection.shutdown(wait=)` polls the open status until the port stops answering; the CLI's
  `rnfs stop` uses it.
- **Pins and the cache.** `pin_ls` adds `label`, `origin`, `size`, `entries` and `added` (`_PinDetails`, read
  from stored blocks and remembered for complete pins as before; a large directory's entry count walks its stored
  index and group nodes, None while one is missing); `name` is unchanged but for a label coming first.
  `pin_label` (`POST /api/pin/label`) stores the label in `PinNames` (`pin_labels`). `cache_ls` lists
  `storage.cached_fetches()` newest first; `cache_rm` is `Storage.uncache(cids)`, which drops the records leading
  to them and collects as `gc` does (warming the DAGs that stay without the lock), then the publisher withdraws
  what is no longer complete.

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
rnfs/interface.py  Rnfs, Names, Name, Jobs, Job, Deny, Contacts, Lxmf, Fetched, Events, Watch, File, the event
                   thread: shared by both; no RNS
rnfs/embedded.py   Node: imports rnfs.node, so RNS, only when rnfs.Node is first used
rnfs/remote.py     Client, over rnfs.apiclient; no RNS
rnfs/errors.py     gains Cancelled (moved from fetcher, which imports it from there) and Closed
rnfs/apiclient.py  the API protocol layer (token, proof, typed errors, Stream); its class becomes ApiConnection
```

`Rnfs` implements every method once, `open` included, over a *backend* offering the facade's operations in the
local API's argument and result forms (JSON-ready dicts; CIDs written out, node IDs and names as hex): for
`Node`, `embedded._Backend`, an adapter over the `RnfsNode`; for `Client`, `remote._Backend(api)`, over
an `ApiConnection` whose methods already mirror the facade (`add_path`, `ls`, `info`, `pin_add`, …). Both turn the
same dicts into the same objects (`Entry.from_dict`), so they can't drift apart; only lifecycle, how name records
are heard, timeouts and the escape hatches differ per class. A feature's methods (peers, contacts, keep, follow,
LXMF) go, in a Node, through the feature as its API route calls it: the route's own lookup (each feature module's
`_feature(handler)`, or `_common.feature`, given the backend, which has the handler's `node` attribute), so a
feature that isn't loaded is the route's `NotFound`, with its message, then the route's checks of its arguments
(`check_kind`, `parse_name_arg`, lxmf's `_int`), in its order. `rnfs.Client`, its objects
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

`Client(rnfs_dir=None, *, port=None, token_file=None, timeout=None, app=None, token=None)`: as
`apiclient.ApiConnection` (default `~/.rnfs/`, the `api_port` in its `rnfs.toml`, the token from
`<rnfs dir>/api_token`, proved before it is sent). With `app` (0.2.0) it is that app's (`rnfs app add`), its
token given or in `token_file`, and `client.pin_key(key)` (`ApiConnection.pin_key`, thread-local like
`timeout()`) gives the calls inside it `X-Rnfs-Pin-Key`.
Nothing is sent until the first call. Raises `SettingsError` if `rnfs.toml` can't be read or trusted; one made
private on the way is reported as a `RuntimeWarning` (`warnings.warn`), without loading RNS.

**Timeouts.** A Client's `timeout` bounds each request (none by default: fetches can take hours). Rather than a
`timeout=` on every method of the shared interface, `with client.timeout(seconds):` (`Rnfs.timeout`, None: no
bound) replaces it for the requests the calling thread makes inside the block, nesting and restoring the outer one
on exit. It is thread-local (`ApiConnection.using_timeout`, a `threading.local` per connection), so other threads,
and the Client's own event, job-watch and name-watch threads, keep the Client's. A request that outlasts it raises
`ConnectionFailed`; connecting and proving take at most the smaller of it and `PROOF_TIMEOUT` (15 s). The long
polls on the calling thread fit it as they fit the Client's: `Job.wait` asks `GET /api/jobs/get` to wait at most
`remote.long_poll_wait(timeout)` (`LONG_POLL_MARGIN`, 2 s, less, whole seconds; with none left, it polls every
second), read from `ApiConnection.effective_timeout()` when each request is sent; the name poller, on its own
thread, keeps the Client's. A Node makes no requests: its `timeout()` checks `seconds` (None or a positive
number, else `ValueError`, as a Client's) and does nothing, so code using it runs on either class. Underneath,
`ApiConnection.request`, `call`, `get` and `post` take `timeout=` for one call (`apiclient.DEFAULT`: the thread's,
else the connection's; None: none, even over the connection's).

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
| `Written` | `cid`, `output`, `warnings`, `warnings_total`, `sealed`, `name`, `extracted` (get) | the output path |
| `Entry` | `name`, `kind` ("file", "executable", "directory"), `size`, `cid` | the name |
| `Info` | `cid`, `type` ("file", "directory"), `size`, `executable`, `sealed`, `recipient`, `stored_size`, `name`, `sealed_folder` | — |
| `Stat` | `cid`, `size`, `blocks`, `local_blocks`, `local_bytes` | — |
| `Pin` | `cid`, `sources: tuple[str]`, `complete`, `type`, `sealed`, `name`, and (0.2.0; None from an older daemon) `label`, `origin`, `size`, `entries`, `added` | the CID |
| `Cached` / `Uncached` | `cid`, `root`, `path: tuple[str]`, `pinned`, `complete`, `type`, `size`, `provided` / `removed_records`, `removed_blocks`, `freed_bytes` | the CID / — |
| `Event` | `seq`, `kind` (`EVENT_KINDS`, or "lost": `lost` True), `time`, `fields: dict` (the kind's own; merged back by `to_dict`) | — |
| `EventBatch` | `events: tuple[Event]`, `next`, `lost`, `epoch` | — |
| `Capabilities` | `version`, `capabilities: frozenset[str]` (`has(name)`), `routes: tuple[(method, path)]`, `features`, `event_kinds`, `error_codes`; empty from a daemon before 0.2.0 | — |
| `Setting` | `rnfs.config.settings_schema()`'s `name`, `group`, `type`, `choices`, `nullable`, `default`, `unit`, `minimum`, `maximum`, `exclusive_minimum`, `description`, `live`, and `value`, `file`, `overridden`, `pending` (lists as tuples) | the name |
| `Settings` / `SettingsChange` | `path`, `settings: tuple[Setting]` (`settings["max_jobs"]`, iteration), `file_error`, `warnings` / `path`, `applied`, `restart_required`, `overridden`, `warnings` | — |
| `Record` | `name: bytes`, `cid`, `sequence`, `age` (seconds since heard, or None) | — |
| `Status` | `version`, `node_id`, `identity_hash`, `dht_mode`, `slow`, `private`, `known_nodes`, `records`, `pins`, `provided`, `blocks`, `used_bytes`, `max_storage`, `names`, `features: dict` (every other field) | — |
| `Collected` / `Verified` | `removed_blocks`, `freed_bytes` / `checked`, `bad: tuple[Cid]` | — |
| `Exported` / `Imported` | `roots: tuple[Cid]`, `blocks` / and `incomplete` | — |
| `DenyState` | `entries: tuple[str]`, `lists: tuple[dict]` | — |
| `Peer` | `node_id`, `mode`, `in_table`, `seniority`, `senior`, `hops`, `interface`, `bitrate`, `link`, `rate`, `last_heard`, `last_active`, `banned`, `sent`, `received`, `requests`, `answered`, `refused`, `failed`, `bad`, and (None from a daemon before 0.2.0) `first_heard`, `rtt`, `rtt_at`, `online` | node ID hex |
| `Stats` | `since`, `totals: dict`, `refused: dict`, `interfaces: dict`, `upload_budget`, `upload_budget_left` | — |
| `Contact` | `nick`, `kind` ("name", "node", "lxmf"), `id: bytes` | the nick |
| `Kept` | `name: bytes`, `reasons: tuple[str]` ("user", "follow", "own"), `sequence`, `cid`, `emitted` (the record held, or None) | name hex |
| `Followed` | `name: bytes`, `pinned`, `latest` (CIDs or None), `sequence`, `state` | name hex |
| `LxmfAddress` | `address: bytes`, `display_name`, `propagation_node`, `nearest_propagation_node` | address hex |
| `MessageLink` | `uri`, `cid`, `name`, `path: tuple[str]`, `size`, `from_node` (JSON `"from"`) | the URI |
| `Message` (inbox) | `id`, `hash`, `sender` (JSON `"from"`), `display_name`, `sent`, `received`, `title`, `text`, `method`, `links: tuple[MessageLink]`, `auto: tuple[dict] \| None`, `trusted`, `contact` | — |
| `Outgoing` (outbox) | `id`, `hash`, `to`, `time`, `links`, `text`, `method`, `state`, `progress`, `error`, `direct_hash`, `fallback` | — |
| `Sent` / `Synced` | `id`, `hash`, `to`, `method` / `propagation_node`, `started` | — |

A JSON key that is a Python keyword is a field of another name (`_field(key="from")`); `from_dict` ignores keys
it doesn't know, so a newer daemon's additions don't break an older library.

`Name`, `Job`, `Fetched` (`lxmf.get`'s per link: `target`, `output`, `job: Job | None`, `cid`, `warnings`,
`error`), `Watch` and `File` are bound to the `Node` or `Client` that made them (below).

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
| `get(target, output, *, from_node=None, raw=False, no_extract=False) -> Written` | fetch errors, `ValueError` (output exists), `OSError` | `get` / `POST /api/get` (`no_extract` sent only when set) |
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
| `remote_pins(node_id=None, cid=None, *, check=False) -> list[RemotePin]`, `served_pins() -> ServedPins` (`pins: tuple[ServedPin]`, `usage: tuple[RemotePinUsage]`, `quota`) | `ValueError`; a Client: `NoSuchEndpoint` (an older daemon: its 405 for the GET is turned into it) | `pin_remote_ls`, `pin_remote_served` / `GET /api/pin/remote`, `GET /api/pin/remote/served` |
| `providers(cid) -> list[bytes]`, `ping(node_id) -> float \| None` | | `providers`, `ping` / `GET /api/providers`, `/api/ping` |
| `export(target, output) -> Exported`, `import_archive(path) -> Imported` | fetch errors, `ValueError`, `OSError` | `export`, `import_archive` / `POST /api/export`, `/api/import` |
| `add_tar(source, *, name=None, pin=True, only_hash=False, chunker=None) -> Added` (bytes or a binary file, read up to the tar's end; `Added.warnings_total`) | `ValueError` (a malformed tar, a chunker), `Denied`, `TooLarge`, `StorageFull` | `add_tar` / `POST /api/add/stream?format=tar` |
| `get_tar(target) -> Iterator[bytes]`, `export_stream(target) -> Iterator[bytes]` (sent once iterated, as `cat`; the whole DAG fetched first; `close()` stops early) | fetch errors, `ValueError`, possibly once iterated | `get_tar`, `export_chunks` (the chunks, closed when done) / `GET /api/get/stream?format=tar`, `/api/export/stream` |
| `import_stream(source) -> Imported` (bytes or a binary file, read to its end) | `InvalidDag`, `Denied`, `StorageFull`, `ValueError` | `import_stream` / `POST /api/import/stream` |
| `label_pin(cid, label) -> Pin` (None or "" clears it) | `NotFound` (not pinned), `ValueError` | `pin_label` / `POST /api/pin/label` |
| `cache() -> list[Cached]`, `uncache(cid) -> Uncached` | `uncache`: `NotFound` (no cached fetch leads to it) | `cache_ls`, `cache_rm` / `GET /api/cache/ls`, `POST /api/cache/rm` |
| `capabilities() -> Capabilities` | | `api.capabilities(node, without=embedded.NODE_LACKS)` (a Node: not "shutdown" or "daemon_json") / `GET /api/capabilities` (`{}` from an older daemon) |
| `shutdown(wait=None) -> bool`, `daemon_info() -> dict \| None` | `shutdown`: `Unsupported` (a Node: `close()` stops it; a daemon without a way to stop it), `NoSuchEndpoint` (an older daemon) | a Node: refused / None; a Client: `ApiConnection.shutdown`, `daemon_info` (`daemon.json`, read locally) |
| `config() -> Settings`, `config_set(settings) -> SettingsChange` | `ValueError` (unknown, refused or malformed settings), `Busy` (rnfs.toml changed meanwhile), `NotFound` (no settings feature) | `SettingsFeature.describe`, `set` (a Node: through JSON, as the API carries them) / `GET /api/config`, `POST /api/config/set` |
| `events.watch(callback, kinds=None) -> Watch`, `events.since(after=None, kinds=None, wait=0, epoch=None) -> EventBatch` (epoch "" is None; a Client's wait fits its timeout, pausing up to a second on an empty answer when that leaves none) | `ValueError` (an unknown kind, after or wait not a whole number ≥ 0); a Client: `NoSuchEndpoint` (an older daemon), `since`: `Busy` (8 polls waiting) | `EventLog.add_listener`, `since` / `GET /api/events` (below) |
| `gc() -> Collected`, `verify() -> Verified` | | `gc`, `verify` / `POST /api/gc`, `/api/verify` |
| `names.create(alias) -> Name`, `names.import_key(alias, key) -> Name` | `ValueError` (alias invalid or taken, key malformed) | `name_create`, `name_import` / `POST /api/name/create`, `/api/name/import` |
| `names.get(alias) -> Name`, `names.list() -> list[Name]` | `NotFound` | `name_list` / `GET /api/name/list` |
| `names.remove(alias)` | `NotFound` | `name_rm` / `POST /api/name/rm` |
| `names.resolve(name) -> Record` (bytes, hex, `Name` or a contact's nick) | `NotFound` (unresolved), `ValueError` | `name_resolve` / `GET /api/name/resolve` |
| `names.watch(name, callback) -> Watch` | `ValueError`; a Client: `NotFound` from a daemon without the endpoint | below / `GET /api/name/watch` |
| `Name`: `alias`, `name: bytes`, `cid`, `sequence`, `str()` hex; `publish(target) -> Record`, `export_key() -> bytes`, `remove()`, `refresh() -> Name` | `publish`: `NotFound` (path not stored), `ValueError`, `RnfsError` (highest sequence) | `name_publish`, `name_export`, `name_rm` / `POST /api/name/publish`, `/api/name/export`, `/api/name/rm` |
| `jobs.get(target, output, *, from_node=None, sealed_name=False, fallback_dht=False, on_progress=None, no_extract=False) -> Job`, `jobs.pin(target, *, on_progress=None) -> Job`, `jobs.export(target, output, *, on_progress=None) -> Job`, `jobs.add(path, *, wrap=False, pin=True, only_hash=False, to=None, to_self=False, seal_name=True, chunker=None, on_progress=None) -> Job`, `jobs.import_archive(path, *, on_progress=None) -> Job` | `ValueError` (output exists or in use, an option), `NotFound` (add, import: no such path), `Busy` (1000 active) | `JobsFeature.submit` / `POST /api/jobs` |
| `jobs.job(job_id) -> Job`, `jobs.list() -> list[Job]`, `jobs.clear(job_id=None) -> int` | `NotFound`, `ValueError` (not finished) | `JobsFeature.get`, `list`, `clear` / `GET /api/jobs/get`, `GET /api/jobs`, `POST /api/jobs/clear` |
| `Job`: `id`, `kind`, `target` (None for add and import), `output`, `from_node`, `sealed_name`, `fallback_dht`, `no_extract`, `path`, `options` (add and import), `state`, `error`, `result: dict`, `total_bytes`, `done_bytes`, `downloaded_bytes`, `blocks`, `blocks_total`, `rate`, `eta`, `created`, `started`, `finished`, `runs`, `done` (finished); `refresh() -> Job`, `wait(timeout=None) -> Job`, `cancel() -> Job`, `watch(callback) -> Watch` | `wait`: `TimeoutError`; `NotFound` once cleared | `JobsFeature.get(wait=)`, `cancel` / `GET /api/jobs/get?wait=`, `POST /api/jobs/cancel` |
| `deny.list() -> DenyState`, `deny.add(entries) -> list[str]`, `deny.remove(entries) -> list[str]` | `ValueError` (malformed entry, too many), `NotFound` (remove: not on it) | `node.deny.entries`/`add`/`remove`, `DenyFeature.lists` / `GET /api/deny`, `POST /api/deny/add`, `/api/deny/rm` |
| `timeout(seconds) -> context manager` | `ValueError` (not None or a positive number) | a Client: `ApiConnection.using_timeout`; a Node: nothing (above) |
| `peers() -> list[Peer]`, `stats() -> Stats` | `NotFound` (no peers feature) | `PeersFeature.peers`, `stats` / `GET /api/peers`, `/api/stats` |
| `contacts.list() -> list[Contact]`, `contacts.get(nick) -> Contact`, `contacts.add(nick, id, kind=None) -> Contact` (id: bytes, hex or a `Name`; kind None: the node tells), `contacts.remove(nick)` | `ValueError` (nick, id or kind invalid, a kind it can't tell, full), `NotFound` (no such contact; no contacts feature) | `ContactsFeature.list`, `get`, `add`, `remove` / `GET /api/contacts`, `/api/contacts/get`, `POST /api/contacts/add`, `/api/contacts/rm` |
| `names.keep(name) -> Kept`, `names.unkeep(name)`, `names.kept() -> list[Kept]` (name: bytes, hex, `Name` or a name contact's nick) | `ValueError` (malformed, unknown nick, `keep_max_names`), `NotFound` (unkeep: not kept by the user; no keepers feature) | `KeepersFeature.keep`, `unkeep`, `kept` / `POST /api/name/keep`, `/api/name/unkeep`, `GET /api/name/kept` |
| `names.follow(name) -> Followed`, `names.unfollow(name)`, `names.following() -> list[Followed]` | as keep; `NotFound` (unfollow: not followed; no follow feature) | `FollowFeature.follow`, `unfollow`, `following` / `POST /api/name/follow`, `/api/name/unfollow`, `GET /api/name/following` |
| `lxmf.address() -> LxmfAddress`, `lxmf.send(to, target, message=None, *, propagate=False, nearest=False) -> Sent` (to: bytes, hex or an LXMF contact's nick; target a Cid, path or `rnfs:` URI, a name URI sent as a name link), `lxmf.sync(nearest=False) -> Synced` | `ValueError` (LXMF off, as by default in a Node; recipient or target invalid; no propagation node), `NotFound` (send: recipient never heard; no lxmf feature) | `LxmfFeature.address`, `send`, `sync` / `GET /api/lxmf/address`, `POST /api/lxmf/send`, `/api/lxmf/sync` |
| `lxmf.inbox() -> list[Message]`, `lxmf.outbox() -> list[Outgoing]`, `lxmf.get(id, *, link=None, output=None, directory=None, use_sent_cid=False) -> list[Fetched]`, `lxmf.remove(id) -> int`, `lxmf.remove_all() -> int` | get: `NotFound` (no such message; a name unresolved without `use_sent_cid`), `ValueError` (id or link not a positive integer, no or a clashing output); remove: `NotFound` | `LxmfFeature.inbox_list`, `outbox_list`, `inbox_get`, `inbox_rm` / `GET /api/lxmf/inbox`, `/api/lxmf/outbox`, `POST /api/lxmf/inbox/get`, `/api/lxmf/inbox/rm` |

`File` (from `open`, an `io.BufferedIOBase`): `read`, `read1`, `readinto`, `readline`, iteration, `seek`, `tell`,
`seekable()`/`readable()` True, `size`, `cid` (the file's own CID), `info`, `close()`, context manager;
`read()` of the rest is one read. Reads
go by the target as opened, root and path, since providers announce roots and the file's own CID may have none;
an `rnfs:name/` URI is resolved once, at `open`, and a contact nick is expanded by the node (which reuses its
CID for 60 s). It is an `io.BufferedReader` of 64 KiB (`FILE_BUFFER`) over `_FileReader`, whose refill after
`open` or a seek is one `read` (or one `GET /api/cat`) of `[position, position + its length)`, so only the chunks
covering it are fetched. While reads are sequential (a refill starting where the last read ended), each refill
reads ahead twice as far as the last, up to `READ_AHEAD` (4 MiB), and keeps the rest for the next `readinto`s: a
sequential reader makes few requests and fetches beyond what it reads at most about what it has read already. A
seek that moves the position drops what was read ahead and starts again at 64 KiB. A sealed file reads its
plaintext unless `raw`. Raises fetch errors as reads happen. Not
thread-safe, like any file object. `Job`, `Name` and every object are snapshots as of their last `refresh()`,
`wait()` or callback.

Job targets are what the jobs feature stores (`<cid>[/path]`): an `rnfs:name/` URI is resolved by `jobs.*`
before submitting, as the CLI resolves a nick. Jobs are persisted, resumed at the next start, and seen by
`rnfs jobs` (a Client's) or only by the app (a Node's, in its own rnfs dir).

**Escape hatches, unsupported:** `Node.internal` (the `RnfsNode`) and `Client.api` (the `ApiConnection`, whose
`get(path, **params)` and `post(path, **fields)` reach every endpoint, an app's own feature's included).

**Errors.** One hierarchy in `rnfs.errors`, every class importable as `rnfs.<Name>`: `RnfsError` ⊃
`NotFound`, `TooLarge`, `InvalidDag`, `StorageFull`, `Busy`, `Denied`, `Cancelled`, `Closed`, `ApiError`, and
`BlockMismatch` and `SealError` (a sealed file malformed or tampered with), which are `ValueError`s too, as is
`NotRecipient` (this node can't open a sealed file).
`ApiError` (with `.status`) ⊃ the Client's own: `DaemonNotRunning` (⊃ `DaemonStarting`, with `.pid`: a process holds the
rnfs dir's lock, as a daemon does while it opens its block store), `ConnectionFailed`, `TokenError`,
`AuthFailed`, `SettingsError`, `ServerError` (500, or another 5xx), and the API statuses, each also a subclass
of the node's class: `apiclient.NotFound(ApiError, errors.NotFound)` and so on, `BadRequest(ApiError,
ValueError)`. So `except rnfs.NotFound` and `except ValueError` catch both classes' errors. A Node raises
`OSError` where a Client gets `ServerError` (the API answers 500 for it).

### Events and callbacks

| Callback | Called with | Source in a Node | Source in a Client |
|---|---|---|---|
| `names.watch(name, callback)` | `Record`, each time a record of `name` newer than the last delivered (or than the one held when `watch` was called) is accepted under the sequence rules (section 9): announces, path responses, keepers' answers, this node's own publishes | `NameManager.watch(name)` (its announces count as asked in `_heard`, so a full name table never refuses them) and a record listener (`add_record_listener`), which `_offer` calls when it accepts a record and `publish` when it commits one, for watched names, with the names lock held; it only queues the record. Records heard before the watch knows its baseline wait and are compared with it (`_NameWatch.begin`). The keepers listener (`KeepersFeature.add_listener`) isn't used: its records reach `NameManager.offer` too | the `GET /api/name/watch` long poll, on the thread `rnfs-client-name-watch` |
| `on_progress` / `Job.watch(callback)` | `Job`, at most twice a second (Node) or once a second (Client) while it changes, and once at its final state | the job-watch thread (`rnfs-job-watch`) polls `JobsFeature.get(id)` (a snapshot made under the jobs lock) every `fetcher.POLL` (0.5 s); `fetcher.observing` and the `Observer` (the `Job`, on the job's thread) stay the jobs feature's | `rnfs-client-job-watch` polls `GET /api/jobs/get?id=` once a second; `wait()` loops `GET /api/jobs/get?id=&wait=30` |
| `events.watch(callback, kinds=None)` | `Event`, one call per event of `kinds` published after `watch()`, in order and never coalesced (an event says what changed, not a state); a "lost" `Event` (`event.lost`) where events were missed, after which the app reads again what it shows | the event log's listener (`EventLog.add_listener`, added at the first watch, removed at `close()`), called after `publish` releases the log's condition, perhaps under the names or storage lock: it only queues. The baseline is the log's `last` and `epoch`; a Node never misses events | `GET /api/events` long-polled for every kind on `rnfs-client-event-watch`, from the first watch's baseline (`after` absent, inside `watch()`, so an older daemon raises `NoSuchEndpoint` there), passing back `next` and `epoch`; on `lost` (fell over 1024 events behind, or the daemon restarted) each watch gets the marker and polling goes on from `next`. Idle, forgetting its position, while no watch is left; a failed poll is reported and retried after 5 s |
| LXMF links received | not offered in 0.2 (decision 5); `events.watch(kinds="inbox")` says when the inbox changes | | |

Rules, the same for both:
- Every callback runs on the instance's event thread (`rnfs-events` in a Node, `rnfs-client-events` in a
  Client), one at a time, in the order events arrived. Events are coalesced per name and per job, so a slow
  callback gets the latest state, not a backlog; `events.watch`'s are not (each is posted under a key of its own),
  so a callback slower than the events lets them queue. Each event watch (`interface._EventWatch`) keeps its
  position, the run's epoch and the last sequence it passed: events heard before its baseline is known wait in it
  and only those after it are delivered, and a "lost" marker moves it to the new run unless it began after it.
  Its deliveries are queued under its lock, so they reach the event thread in the order its position passed them,
  whichever threads heard them; `EventLog.publish` tells listeners in sequence order (its `_order` lock).
  A Client takes one baseline at a time (`_baseline_lock`, the request and the position together, so two first
  watches can't leave the poller past events the older one hasn't had) and sets the poller's position only when it
  has none: a baseline from a restarted daemon leaves it on the old run, whose next poll answers "lost" and moves
  every older watch on (the new watch began in the new run and skips that marker).
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
| `open()` costs one API request per buffer refill for a Client | Accepted; the 64 KiB buffer bounds the requests, and sequential reads read ahead up to 4 MiB a request |
| Settings, DHT mode, LXMF, lifecycle: a Node's own; the daemon's for a Client | Documented: a Client can't change them; `close()` only ends the Client |
| No gateway or API in a Node | Not offered in 0.2 (decision 6) |
| The API's `GET /api/info` has no `raw` | A Client's `info(raw=True)` (and `open(raw=True)`) takes a sealed file's `stored_size` as its size |
| A Client's requests can be bounded (`timeout`, `timeout()`); a Node's calls make none | A Node's `timeout()` checks its argument and does nothing; `Job.wait(timeout)` bounds a wait on both |
| LXMF is off in a Node unless `lxmf_enabled` | `lxmf.address`, `send` and `sync` raise `ValueError` saying so, as a daemon with it off answers (400) |
| `OSError` in a Node is `ServerError` from a Client | Documented |
| A Client can be an app's (`app=`), its pins `app:<name>[:<key>]` (`pin_key()`); a Node's are always `user`'s | Client only: an embedded node is its app's own node, with no token to tell callers apart |
| A Client's connection errors (`DaemonNotRunning`, …) | Client only, all `ApiError` |

### What happens to `rnfs/apiclient.py`

It stays the protocol layer: `Client` was renamed `ApiConnection` (unreleased; the CLI's `ApiClient` and
`tests/test_apiclient.py` changed mechanically), keeping `request`, `call`, `get`, `post`, `Stream`, the token,
proof and version check, and its dict-returning methods, which become `rnfs.Client`'s backend. Its errors are
re-based as above. Its module docstring and README point apps to `rnfs.Client`. The CLI's output can't change:
it calls the same methods with the same parameters. It gained `name_watch` and `cat(from_node=)`, and in 0.2.0
the feature methods `rnfs.Client` uses (`peers`, `stats`, `contacts`, `contact_get`/`_add`/`_rm`,
`name_keep`/`_unkeep`/`_kept`/`_follow`/`_unfollow`/`_following`, `lxmf_address`/`_send`/`_inbox`/`_inbox_get`/
`_inbox_rm`/`_outbox`/`_sync`) and per-call timeouts (`timeout=`, `using_timeout`, `effective_timeout`).

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
- Threads added: `rnfs-events` and `rnfs-job-watch` (Node), `rnfs-client-events`, `rnfs-client-job-watch`,
  `rnfs-client-name-watch` and `rnfs-client-event-watch` (Client), each started on first use. Locks added:
  `_Events`' and `_JobPoller`'s conditions (innermost: held only to queue or take work); `Rnfs._watch_lock`, taken
  after the names lock and storage's (the record and event listeners run under them), never before;
  `NameManager._watched`, under `names._lock`; `api.NameWatches.lock`, taken before the names lock, never under
  it; `remote.Client._poll_cond` (the name and event pollers' wake-up and the event poller's position), taken
  before `_watch_lock`, never under it, and never held while calling out; `remote.Client._baseline_lock`, taken
  first, around one event baseline request and then `_poll_cond`; `interface._EventWatch.lock` (a watch's
  baseline and position), held only while it queues deliveries (`_Events`' condition, under it); and two leaves:
  `interface._NameWatch.lock` (a watch's baseline and last sequence) and `interface.Watch._lock` (`cancel()`'s
  once-only check). Callbacks run with no rnfs lock held.

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

- `content/chunker.py` — `fixed_chunks` and `fastcdc_stream` cut a stream into chunks. Which one a build uses
  is `builder.BuildOptions.chunker`, frozen with the deny check (`check`) and passed explicitly down through
  `add_path`, `add_stream`, `build_open`, `build_file` and `SharedFolder.scan`: a ContextVar isn't copied to a
  worker thread, so a build moved onto one would silently get other CIDs and skip the check. `chunker.using(name)`
  and `builder.checking(check)` remain as wrappers setting them for the current context, which a build given no
  options takes (`builder.current_options()`). FastCDC computes gear hashes for 8 KiB at a time on
  a big integer of 9-byte lanes (≈ 39 MB/s in CPython 3.12, against ≈ 15 MB/s for the byte-by-byte reference
  `cut_point_simple` it is tested against, and ≈ 2 GB/s for fixed chunking).
- `content/seal.py` — the sealed-file format: `SealingReader` (file-like, one segment in memory),
  `open_sealed(raw, total)` → `SealedFile` (`unlock(identity)`, `size`, `name`, `read(offset, length)`
  decrypting only the covering segments), `parse_recipient`. AES-256-CBC and HKDF come from
  `RNS.Cryptography`, HMAC/SHA-256 from the standard library: no new dependency.
- `node_content.py` — `keep_added` (a `user` pin, or for `--no-pin` a cached fetch recorded in
  the fetched targets for the roots strategy), `seal_recipients` (`RNS.Identity.recall` by destination hash, then
  identity hash; the node's own identity; contacts), `open_sealed` over a Fetch's `read_range`,
  `sealed_info`, `write_sealed`, `get_sealed`/`unpack_sealed`, `block`, `export_stream`, and the API option
  parser `add_options`.
- Recognising a sealed folder (`is_sealed_folder`): the sealed name ends in `.tar` and the plaintext's first
  `MARKER_PEEK` (1024) bytes, from the first segment alone, are the marker `tarstream.folder_marker()` writes:
  `tarstream.is_marked` parses the first header (`TarInfo.frombuf`) and needs typeflag `g` with exactly the one
  pax record `comment=rnfs sealed folder`, parsed rather than compared byte for byte so another writer's header
  layout still counts. An unmarked tar, which every tar sealed as a file is, never reaches `build_tar`: it is
  written by `write_sealed` with no warning, byte for byte as 0.1.6 does. `sealed_info` gives recipients
  `sealed_folder` (one more first-segment read, only for `.tar` names), which the CLI uses to name outputs before a
  get it can't hand to a job's rename (a daemon without jobs; a path target ending in `.tar`). `build_tar` takes
  the marker as one global header (its 47 bytes well within `MAX_GLOBAL_BYTES`), which `tarfile` keeps as the
  stream's `pax_headers`; a marker with nothing after it but the end blocks isn't a tar to `tarfile` (it fails
  at offset 0), but packing always writes the top directory after it.
- Unpacking a sealed folder (`get_sealed`, for a sealed folder unless `no_extract`): `unpack_sealed` checks the free space for the plaintext twice where the blocks and the output share a
  filesystem (`_check_free_space` with the plaintext added to what is still to fetch, and `Storage.check_disk`
  for the blocks' own), then, inside a protection session of its own (within the fetch's), feeds the
  decrypted segments (`IteratorReader` over `SealedFile.read()`) to `tarstream.build_tar` through a
  `Session.writer()`, with the deny list's hash check, and writes out with `walker.write_out` from the stored
  blocks (`Storage.getter()`). The target is the root's one entry if it is a directory, else the root. A
  `ValueError` from `build_tar` (not `SealError` or `BlockMismatch`) means the plaintext isn't a valid tar:
  `NotATar`, and `get_sealed` writes the file with `write_sealed` and a warning (only a marked tar gets here). `TooLarge` is raised again
  naming `--no-extract`. In every case the session ends and `storage.collect()` deletes what nothing else
  reaches, so the plaintext is never pinned, recorded as a fetch or provided.
- Sealed folders (`add_sealed_path` of a directory): `folder_sealed_name` refuses a name `<name>.tar` couldn't
  carry (over 251 bytes, invalid) and `seal_name=False` before anything is read, rather than sealing unnamed as
  a file with an invalid name is. `tarstream.folder_chunks` is a generator writing the tar as it walks
  (`builder.open_path`/`open_child`/`list_directory`, the same descriptor-relative opening and `Changed` checks
  as `_build_tree`, which it can't reuse since that builds a DAG); `tarstream.IteratorReader` turns it into the
  stream `SealingReader` reads, so one segment of plaintext is held at a time. It starts with the sealed-folder
  marker (`folder_marker`, `TarInfo.create_pax_global_header`). Its headers are `_header_of`,
  shared with the writer of stored DAGs. `Changed` is only raised by opening and listing, before an entry's
  header is written, so a skipped entry never leaves half a member. The entry limit is the sender's
  `max_output_entries` (the recipient's unpack has its own), and the depth check is exact: `fixed_height` and
  `directory_height` compute the height `builder` gives each file and directory with fixed chunks (as the
  recipient's `build_tar` builds them), checked against `MAX_DEPTH` one level up for the tar's root; a refusal
  can come once part of the folder has been sealed, and what was stored is then unreferenced, for the next
  collection. A file's size is its open `fstat`'s; a file growing meanwhile is cut there, one shrinking fails
  the add.
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
  the first chunk is fetched first even for a `cat --offset` deep in a file. `info` and `read` (so `cat`) remember
  the targets found not to be sealed (`RnfsNode._open_sealed`: by resolved expectation, `NOT_SEALED_MEMO` 4096,
  least recently used forgotten first; emptied when the deny list's CIDs change, since that read meets them), so
  reading one again doesn't read its first bytes; `read_file` (`GET /api/cat`) hands the `SealedFile` its info
  opened to its read rather than opening it twice. The gateway does so only for a
  GET from byte 0 or a web page's request (`limited()`, so its type never depends on what is stored); the
  user's own HEAD, or range not at 0, uses the first bytes only if already stored (`stored_head`), else takes
  the Content-Type from the name, or `application/octet-stream`. `stored_head` applies the deny list as a read
  does (`node_content.denying` wraps its getter; with file hashes, `_check_file` over stored blocks once the file
  is wholly stored), so such a HEAD of a denied file answers 451 like GET; `pin ls` (`_pin_sealed`) passes
  `deny=False`.

## Mesh

`mesh.py` (no RNS at module level): `PathInfo`, `provider_rank` and `RnsPaths` (hops and next-hop
interfaces from `RNS.Transport`, or through a shared instance's RPC with its interface stats cached for 30 s
and each node's next-hop interface name for 15 s, `PATH_CACHE`, in an LRU of at most 1024 nodes; `infos()` fills
the names of many nodes from one `get_path_table` RPC, for `rnfs peers`);
the airtime policy (`TokenBucket`, `AirtimePolicy`, interface matching, quiet-hours parsing, all with
injected clocks); and `Traffic`, thread-safe counters per peer (LRU, at most 1000), per interface (at most 64)
and in total, never persisted.
- Per peer, `Traffic` also keeps `first_heard` (wall time, when first counted), `rtt` and `rtt_at` (`measured`:
  `LinkPool` calls its `on_rtt`, which `PeerClient` sets to `traffic.measured`, with `Link.rtt` for every link
  `_open` establishes, shared, one-shot (`ping`) or identified, only a finite non-negative number) and, on
  `Traffic.monotonic` (so a wall-clock step decides nothing), `first_counted` and `last_seen`, the last sign the
  peer was up: a request it answered (`ANSWERED` or `REFUSED`, not `FAILED`), a `/blocks` response sent to it
  (`sent` with its node ID), a link established, or its own announce (`announced`). `DhtAnnounceHandler` takes
  RNS's five-parameter call and runs `on_announced` (the node's `_announced`) only when `is_path_response` is
  false, but that isn't enough: attached to a shared instance, RNS 1.5.6 passes path answers on to its clients as
  plain announces, and an announce queue can deliver one hours late. So `_announced` reads the announce's emission
  time (its signed random hash: the path table's newest, `RnsNetwork.emitted_time`, which is this announce's since
  RNS calls handlers only for an announce that takes the path, after writing it; in memory, unlike the announce
  cache) and `Traffic.announced` counts it only if it is later than the last one counted (`last_emitted`) and at
  most `ONLINE_WINDOW` old, allowing `EMISSION_SKEW` (300 s) of clock difference, seeing the peer at its emission
  time (never later than now). An unreadable emission doesn't count. Path responses and stale announces still
  update `last_heard` and the mode (`heard`). `mesh.online(link, last_seen, now)` is the one online rule: an open
  shared link, or `last_seen` within `ONLINE_WINDOW` (900 s: three `link_idle_timeout`s, far below
  `announce_interval`) on the monotonic clock, a negative age being offline.
- The peers feature (`features/peers.py`) lists them (`first_heard` is the earliest of `Traffic`'s, the routing
  table's entry (now − seniority) and RNS's `known_destinations` time, `_earliest`) and publishes `peer` events
  (`{"node_id", "change": "new" | "online" | "offline", "online"}`) from its `rnfs-peers` thread: `check()` every
  `CHECK_INTERVAL` (30 s) computes `mesh.online` for every peer `Traffic` counts or with an open link (both
  bounded; not the listing, which a routing table beyond `MAX_PEERS` cuts, and whose routing-only peers are never
  online) and diffs it with the last check's. The first check only records, and `new` needs a `Traffic` entry
  first counted after it, so the start and the routing table seeded from RNS's cache publish nothing; a peer
  online that is no longer counted nor linked gets `offline`.
- `Fetcher.path_info` (default: nothing known) is set by the node to `RnsPaths.info`; `Fetch._rank` orders
  both providers and open-link probes (`mesh.provider_rank`: seniority, then the local score band,
  `Scores.band`, then hops, bitrate and order), and each provider caches its `PathInfo`.
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
  thread or lock owns each of its fields. A new `/blocks` arriving while the link's previous response has
  sent all its parts waits, on `RequestServer.clock`, up to `_proof_wait(link)` (2 × RTT + 1 s, at most
  `PROOF_GRACE`, 5 s) for its proof (`_previous_done`). That is the server's only use of `link.rtt`, which on an
  incoming link is what the requester claims: anything but a finite, non-negative number counts as no RTT, and
  any future use must go through it, capped.
- `IncomingLinks.take_token` keeps a per-link token bucket (`REQUEST_BURST`, `REQUEST_RATE`) and a smaller
  one of refusals (`OVER_RATE_REFUSALS`, one a second) in `_LinkState`. `RequestServer._guarded` asks it first
  for every path: over the limit it answers `False` while refusals are left, else `None`, which RNS sends
  nothing for; `_over_rate` counts them and logs at most once a minute. Every handler but `/blocks` runs its
  work through `RequestServer._in_slot(link_id, answer)` after its own checks (so a refusal by those takes no
  slot): it takes one of the link's `MAX_REQUESTS_PER_LINK` slots (`IncomingLinks.start_request`), answers
  `False` if none is free, and frees it when `answer()` returns or raises. A `/blocks` slot outlives its handler
  (until the response's Resource concludes), so `_handle_blocks` keeps its own.
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
  incoming `rnfs.dht` link, and a request packet on one this node opened (`link.initiator`), without
  decrypting it. Everything else goes to RNS's own methods unchanged.
- `guards.limit_half_open(destination, limit)` replaces a destination's `incoming_link_request` (on the
  instance; RNS's `Destination.receive` calls it for each link request) with `HalfOpenLimit`, which, before
  RNS validates a request, closes (`Link.teardown()`) the oldest `PENDING`/`HANDSHAKE` link in
  `destination.links` on the interface holding the most until fewer than `limit` remain. `RequestServer`
  applies it to `rnfs.dht` and the LXMF feature to its delivery destination, both at `max_incoming_links`.

## Features

`rnfs/features/__init__.py` defines `Feature` and `FEATURE_MODULES` (in start order: peers, jobs, keepers, follow,
contacts, lxmf, deny, settings). The node builds every feature in its constructor (`node.feature(name)`), starts them after
its own threads and stops them, in reverse, before its own services. A feature may override: `start()`,
`stop(deadline=None)` (asks its threads to end, then waits for them until `deadline`, a `time.monotonic()` value:
the node passes its stop budget's, so `STOP_TIMEOUT`, 5 s, bounds the features' threads and its own together;
`None` waits `features.STOP_WAIT`; the base's `_spawn(target, name)` starts a thread that `_join(deadline)` waits
for; the node calls a `stop()` with no `deadline` parameter as before, `features.stop_feature`),
`save()` (called with the node's periodic saves), `close()` (releases the state database at shutdown, after `stop()`
and the last save), `status()` (fields merged into `rnfs status`), `requests()` (new `rnfs.dht` paths; the request
server applies private mode and the per-link limit, and turns `None` or an exception into `False`; a response must
be a shape `protocol.response_refusal` accepts, True/False/None, a `/find` answer, or at most `MAX_BLOCKS_CIDS` bins
or nils, since nodes since 0.1.4 refuse any other: the server checks each with `protocol.body_refusal` and answers
`False`, logged as an error, instead of one they'd refuse), `api_routes()` (all token-protected) and `cli_commands()`.
Feature modules don't import RNS at module level (the CLI imports them to build its parser) and their constructors
work on `RnfsNode.offline` test nodes.

A feature declares the features it uses: `requires` (it can't work without them; each must come before it in
`FEATURE_MODULES`) and `optional` (used when loaded). The node loads them through `features.checked_classes()`,
which raises `features.FeatureError` before making any if a required feature is missing or later, or two features
share a name; a feature reaches another only with `self.dependency(name)`, which raises `FeatureError` for a name it
didn't declare and returns `None` for an optional one not loaded. Today follow requires keepers; jobs, keepers,
follow and lxmf use contacts, contacts uses keepers, lxmf uses jobs, and settings uses jobs, keepers and lxmf (to
hand them live settings), all optionally. What the others call on
contacts is the `features.ContactsApi` Protocol (`lookup`, `has`, `nick_of`, `resolve_id`, `expand_target`), which
types `node.feature("contacts")`, `self.dependency("contacts")` and `features.find(node, "contacts")`;
`tests/test_contacts.py` checks `ContactsFeature` against it, so a renamed method fails there instead of silently
turning off LXMF's contact trust. LXMF resolves a recipient's nick with `ContactsFeature.resolve_id(nick, "lxmf")`.

The node checks the features' names too when it loads them, with one error style (`FeatureError`, "feature
<name>: <problem>"), so nothing is shadowed or dropped silently: `checked_classes()` checks every API route
(`features.api_routes()`: GET or POST of a path matching `API_PATH`, under `/api/`, with a handler, defined neither
by `ApiHandler.ROUTES` nor by another feature), and once the features are made, `features.request_handlers()`
checks their request paths (`/` and a name, `REQUEST_PATH`, with a handler, neither one of rnfs's own,
`CORE_REQUEST_PATHS`, nor another feature's). `NodeBackend.feature_requests()` hands the request server the paths
checked then. A feature's constructor raising stops the node from being made (those made before it are closed);
its `start()` raising is logged and the others still start (`RnfsNode._start_features`).

| Feature | Module | What it does | Hooks in shared code |
|---|---|---|---|
| `peers` | `peers.py` | `rnfs peers`, `rnfs stats`; client-mode nodes' mode and last-heard time come from the node's `rnfs.dht` announce handler, which counts every announce in `Traffic`; `first_heard`, `rtt`, `online` and `peer` events (Mesh) | reads `Traffic`, the routing table, links, paths |
| `jobs` | `jobs.py` | `JobsFeature` (the job manager), `Job` (a pin, add or import job keeps its `source`, saved in its record only when it isn't `user`, so other records are as 0.1.6 wrote them; `app_removed` cancels an app's), CLI progress formatting | `fetcher.observing(observer)`: a thread-local observer every `Fetch` picks up (`total`, `block`, `cancelled` checked at each step, raising `fetcher.Cancelled`) |
| `keepers` | `keepers.py` | signed records (`SignedRecord`, `parse_signed`, `sign_record`, `from_announce_data`), `/name`, `KeeperClient`, `KeeperStore` | `Feature.requests`; `publisher.DhtKey` and `Publisher.add_key_source` (non-CID keys published like CIDs); `NameManager.keepers` (`published`, `heard`, `query`, `always_ask`, `local_record`), `NameManager.offer()`/`record()`; the 5-argument announce handler (packet hash) |
| `follow` | `follow.py` | followed names: each check and fetch one at a time, `fetcher.open(pinned=True)`, pin inside the fetch's protection, old pin released after | keepers' `FOLLOW` reason |
| `contacts` | `contacts.py` | address book; `expand_target`, `lookup(nick, kind)`, `resolve_id(text, kind)`, `node_arg` | `RnfsNode._parse_target` (never for the gateway), `JobsFeature.submit` |
| `lxmf` | `lxmf.py` | pure parts (`Link`, `parse_uri`, `format_uri`, `find_links`, `encode_field`/`decode_field`); `MessageLog` (the inbox and outbox, in the state database); `LxmfFeature` (send with direct→propagated fallback, inbox, auto-fetch, inbox get, pin release; `inbox` events with `box` "inbox" — `received` {id, from}, `removed` {ids}, `auto` {id, index, state} — or "outbox" — `queued` {id}, `state` {id, state}, `removed` {ids} — published under its lock by `_event`); `LxmfNetwork`, made in `start()`, holds every RNS/LXMF object | `lxmf.validate_settings` from `Config.validate`; `storage.LXMF_PREFIX` (`app:lxmf:`, an app source to `storage.check_source`) |
| `deny` | `deny.py` | `deny_lists` sources (`parse_source`, `validate_settings` from `Config.validate`): `check(source)` resolves a name (persisted), fetches the target pinned, checks it is a file within `MAX_LIST_BYTES`, parses it, pins it `deny` and `accept`s it, then releases unused `deny` pins; `run_due` on `rnfs-deny`, after the join (`CHECK_INTERVAL` for names, `retry_delay` backoff); sources no longer configured are forgotten at start | `node.deny` (core); `DENY` in `storage.check_source` |
| `settings` | `settings.py` | `rnfs config`, `GET /api/config`, `POST /api/config/set`: rnfs.toml read and rewritten, `LIVE` settings applied to the running node | `config.settings_schema`, `Config.combine`, `config.edit_settings`; `Config.overrides`/`given_defaults` (set by `Config.load`); the live hooks below |

**Settings.** The schema is `config.settings_schema()` (standard library only, so `rnfs.Client` apps and the CLI
read it without a daemon): one entry per `Config` field but `rnfs_dir`, its type and nullability from the field's
annotation, ranges from the tables `Config.validate` checks (`WHOLE_NUMBERS`, `OPTIONAL_WHOLE_NUMBERS`, `RATES`,
`DURATIONS`, `CHOICES`, and `FEATURE_LIMITS` for what features check), units, groups (`SECTIONS`, the markers in
`Config`), a one-line `DESCRIPTIONS` entry and `live` (`LIVE_SETTINGS`); `tests/test_settings.py` fails for a
field without one, and checks each entry against `validate`. `Config.load` records what it was given over
rnfs.toml (`overrides`: the daemon's command line, an embedded node's settings) and under it (`given_defaults`),
so `GET /api/config` can say for each setting the running value, rnfs.toml's (`file`, null where it isn't set:
TOML has no null), `overridden` and `pending` (a restart with the same command line would run another value),
plus `file_error` when a restart would refuse the file. `POST /api/config/set` runs under the feature's lock: it
reads rnfs.toml through `open_checked` (a symlink or another user's file is refused, 400), applies the changes to
its values (a name that isn't a setting, or `rnfs_dir`, may only be removed, and only if the file has it), and
checks them with `Config.combine` and `Config.check_reticulum_config` (`restart_config`) both alone (a start
without the command line, and clients reading the port, must accept the file) and with the overrides; then
`config.edit_settings` rewrites just the changed assignments, every statement of a changed name (a dotted key, an
inline table) included: lines end at `\n` only, as TOML's do, and each statement's extent comes from a small
lexer (`_statement_extent`: strings, multi-line ones too, and bracket depth), then one `tomllib` parse, so
editing is linear in the file's size; a comment ending it is kept; new settings are appended. Strings are written
with control characters, NEL, U+2028 and U+2029 escaped. The
result is checked to parse to exactly the intended values and to fit `MAX_SETTINGS_SIZE`, the file is re-read
(changed meanwhile: 503 busy, nothing written) and replaced with `fsutil.write_atomic` (0600, private, a symlink
at the path replaced, not followed). Last, each `LIVE` setting not overridden is assigned on `node.config`, the one
`Config` the node's parts share, and its hook, if any, hands it to what copied it at start: `Storage.max_storage`
(then `_enforce_storage` at once), `JobsFeature._schedule` (queued jobs a higher `max_jobs` allows),
`RequestServer.airtime`'s `serve`/`no_serve`/`quiet` (each replaced whole), `KeeperStore.max_names`, and
`LxmfFeature.accept_from`/`auto_fetch_from` (sets replaced whole). The others in `LIVE` are read from
`node.config` at each use. Not live, deliberately: identity, ports, Reticulum, `dht_mode`, private mode and
`members`, and `remote_pin_identities` (a removed identity's waiting remote pins are dropped only at a start),
`upload_budget` (its token bucket holds charges of responses in flight), `gateway_fetches` (the gateway isn't the
node's), `verbosity` (RNS clamps its loglevel, so a change can't be undone exactly) and `lxmf_sync_interval`
(read only after each download).

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
beyond it dropped, then text cut), and never evicts a record holding auto-fetched pins for one that holds none,
nor for another sender's (when all others hold pins, the new record's sender's own goes, or it is refused). An
auto-fetch sender's record is added with `share=` 1/n of the record and byte limits (`_auto_share`, n senders in
`lxmf_auto_fetch_from`, at least one record and 16 KiB), counting its protected records; past it, `_own_victim`
drops its own unprotected records first, then those whose holdings (`holds=_held`: pinned CIDs, queued links)
another of its records also holds, then the oldest;
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
`POST /api/deny/add`, `/api/deny/rm` (`{"entries": [...]}`); `GET /api/config`, `POST /api/config/set`
(`{"settings": {name: value or null}}`). `POST /api/jobs`
takes, for a get, the JSON booleans `sealed_name` (write a sealed file this node opens under its safe sealed
name in the output's directory; the CLI sets it for `rnfs get <bare cid>` without `-o`, so it no longer calls
`GET /api/info` first; an unpacked folder is then named after its directory),
`fallback_dht` (a get with `from` that fails, other than by cancellation, `TooLarge` or `StorageFull`, is retried
once through the DHT) and `no_extract` (a sealed folder written as its tar; kept in the job's record only when set,
so other records stay as 0.1.6 wrote them); for `kind` `add`, `path` and `POST /api/add`'s options (flat, as `/api/add` takes them; kept in the job as `options`, only those set), for `import`, `path`.
Add and import jobs run `add_path`/`import_archive` with `progress=Job.read`, which counts `done_bytes` (and the
rate, of bytes read) and raises `Cancelled` once cancelled or stopping; their `total_bytes` is `builder.tree_size`
(measured first, cancellable between entries) or the archive's size. They hold no protection session (they fetch
nothing) and an interrupted one runs again from the start. Every job state change, removal (`removed`: true) and,
at most every `EVENT_INTERVAL` (1 s) on the job's thread, its progress is a `job` event (`Job.event`: `id`,
`job_kind`, `state`, `error` and the progress fields; never the result). `POST /api/lxmf/send` and `/api/lxmf/sync` take
`nearest`; `POST /api/lxmf/inbox/get` takes `use_sent_cid` (without it, an unresolvable name link is 404);
`GET /api/lxmf/address` returns `propagation_node` (the configured one only) and `nearest_propagation_node`.
`GET /api/lxmf/inbox` adds to each record, computed when listed, `trusted` (`LxmfFeature._trusted_hex`) and
`contact` (`ContactsFeature.nick_of(address, "lxmf")`, the first nick naming it, or null); `rnfs inbox` prints the
address, then `contact <nick>`, `trusted` or `untrusted` (`?` without the fields), the method, and last the
display name quoted and escaped with `fsutil.printable_label`.
Core routes gained options: `POST /api/add` `pin`, `to`, `to_self`, `seal_name`,
`chunker` (and the same as query parameters on `/api/add/stream`); `POST /api/get` `raw`, `no_extract`; `GET /api/cat`
`raw=1`, `block=1`; `GET /api/info` `from`.

## Threads

| Thread | Owner | Does |
|---|---|---|
| `rnfs-join`, `rnfs-publisher`, `rnfs-names`, `rnfs-maintenance` | node | joining (and joining again while under `MIN_SERVERS`), publishing, name announces, periodic saves and eviction |
| `rnfs-remote-pin-checks`, `rnfs-remote-pin-check` | node | checking whether the remote pins asked for are done: a pass every 30 s once joined, its provider lookups on up to 4 threads at once (1 on slow nodes, shared with on-demand checks) |
| `rnfs-announcer`, `rnfs-response-monitor`, `rnfs-link-sweeper` | network | rnfs.dht announces, `/blocks` deadlines and displacement, idle links |
| `rnfs-lookup`, `rnfs-fetch`, `rnfs-find-providers`, `rnfs-remote-pin`, `rnfs-publish-lookup` | per operation | lookups, fetch workers, provider search, remote-pin fetches (one thread per source with pins waiting), a publishing round's lookups (up to 4 at once, ended with the round; none on slow nodes) |
| `rnfs-write` | storage | an add's, shared-folder import's or archive import's block writes (`Session.writer()`), up to 16 at once, ended with it |
| `rnfs-job-<id>` | jobs | one per running job, at most `max_jobs`; `stop()` waits for them until the node's stop deadline |
| `rnfs-keepers`, `rnfs-keeper-query` | keepers | pending refreshes (≤ 64 queued), own-name sync, saves, re-asking every `announce_interval`; parallel `/name` queries |
| `rnfs-follow` | follow | every check and fetch, one at a time |
| `rnfs-peers` | peers | every 30 s, the peers' online states diffed into `peer` events (no network I/O) |
| `rnfs-deny` | deny | every deny-list source's check and fetch, one at a time, once joined |
| `rnfs-events`, `rnfs-job-watch` | rnfs.Node | callbacks into the program, one at a time; polling watched jobs (both started on first use) |
| `rnfs-client-events`, `rnfs-client-job-watch`, `rnfs-client-name-watch`, `rnfs-client-event-watch` | rnfs.Client (in the program) | callbacks; polling watched jobs; the `GET /api/name/watch` and `GET /api/events` long polls |
| `rnfs-lxmf`, `rnfs-lxmf-fetch` | lxmf | announces, propagation-node downloads, polling outbound states each second while a message is followed or held or a download runs (otherwise it sleeps until the next announce or download; `LxmfNetwork.send`, `_hold` and `sync` wake it); auto-fetch, one at a time, senders in turn (LXMF runs its own job thread) |

The API and gateway run one thread per connection, capped (32 and 64). The daemon's main thread waits for
SIGINT, SIGTERM or SIGBREAK (`cli.wait_until_stopping`): on POSIX a signal interrupts the wait, so it has no
timeout and wakes only for the debug log's status line; on Windows a wait isn't interrupted for signal handlers,
so it wakes each second.

Locks are taken in this order, never the reverse: `names._lock` → `publisher._lock` → `node._lock` → `storage._lock`
→ the state database's lock (`statedb`, innermost: a store opens a transaction only once it holds every lock the
change needs, and inside it takes no other lock and does no network I/O). `node._deny_lock` (one deny-list change
applied at a time, `_apply_deny`) is taken before the publisher's lock and never with the names lock;
`DenyList._lock` holds only the state database's, as does `AppRegistry._lock` (`app_rm`'s release of the app's
pins runs without it). Feature locks held across `storage.pin`/`unpin`/`protect` and
`publisher.refresh()` (`JobsFeature._lock`, `FollowFeature._lock`, `LxmfFeature._lock`) come before the publisher's
and storage's. The public API's locks come after these: `Rnfs._watch_lock` after the names lock (NameManager's
record listeners run under it, and only queue), the event and job-watch conditions innermost; callbacks into the
program run with none of them held. `api.NameWatches`' lock is taken before the names lock (it watches and unwatches
in NameManager, one watch per name it holds) and never under it. `NameManager.publish`, `remove`, `release_gone` and
`reconcile_pins` pin and unpin, and so call `publisher.refresh()`, while holding the names lock; `Publisher.refresh`
recomputes what is provided under its own lock, reading the node's fetched targets (`node._lock`) and walking the
pins' DAGs (`storage._lock`); the node reads storage under its lock (`_prune_fetched`). This is how the code behaves
today, not a design goal. Batched saves write outside these locks, from the changes collected under them, one save
at a time behind a save lock taken first (`Storage.save`, `Publisher.save`, `DenyList.save`, the name floor, the
fetched targets, the remote pins waiting), so an older snapshot never lands last. `ProviderStore` is the exception:
its records are read from the database under its lock, so it writes under it too (a `/find` that misses its cache
waits for a save, at most `FLUSH_CHANGES` rows); it asks the node's `_known_publisher` under it, which takes the
contacts feature's lock, never the other way round. Slow work that runs while a lock is held:

- `Publisher.refresh` checks every pin's DAG (`dag_blocks`, memoised) under the publisher lock, behind the
  names lock when a name changes, but only when something it is computed from changed: the node's generation
  (`Storage.changes()` and the fetched targets' change count, read under `node._lock` then `storage._lock`), the
  deny list's CID set, the key sources' keys or the strategy. A refresh that changed none of them returns at once
  (about 1 µs, from 7 ms at 10 000 pins); the generation is read before computing, so a change made meanwhile
  makes the next refresh compute. When it does compute, it builds the set of blocks served again only when a
  provided CID went or its DAG's blocks changed (CIDs only added are unioned in). It takes the pins as
  `Storage.pinned_cids()`, not a copy of every pin's sources, and gathers the keys withdrawals are queued for once,
  so an item newly provided searches the queues only when its key is among them (a mass re-pin, or the first
  refresh after a start with withdrawals pending).
- `Storage.evict` and `gc` decide what to delete, and delete it, under the storage lock (after reading the DAG
  nodes outside it); `verify` checks completeness and re-reads the blocks outside it.
- Every pin and unpin commits its row durably (`synchronous=FULL`, after the blocks are synced) under the
  storage lock (see `storage.py`).

## State files

`<rnfs dir>/state/rnfs.db` (rnfs/statedb.py), one table set per store; "FULL" commits are on disk when the
change returns, "NORMAL" ones are batched saves a power loss may lose the last of:
- storage: `pins` (one row per source; each pin and release FULL, after the blocks are synced), `pin_times`
  (0.2.0: when each pinned CID was first pinned by any source, written in the same transaction as the pin that
  made it pinned and the release that ended it; pins of earlier versions have none, and a row whose pin is gone
  is deleted at start; a table of its own so the released `pins` stays as 0.1.5 made it) and `cache` (cached
  fetches, LRU order in `used`, at most 10 000; NORMAL every 30 s and at stop, changed records only);
- `pin_names` (rnfs/pin_names.py: where each pin was added from, local only, pruned to the pin list; NORMAL,
  each change written at once, after the pin it names was committed FULL: a power loss can lose a name, never the
  pin, and an add costs one durable commit, not two) and `pin_labels` (0.2.0: the label its user gave a pin,
  apart from where it came from; FULL, each change written at once; pruned with the names);
- node: `fetched` (targets fetched for the user, for `roots`; with the cache records) and `remote_pins` (remote
  pins accepted and not yet fetched, in order; about once a second and at stop, rewritten whole); fetcher:
  `invalid` (invalid roots, oldest first; with the cache records);
- `remote_pins_asked` and `remote_pins_served` (0.2.0, rnfs/remote_pins.py: the remote pins this node asked for and
  their checks, at most 10 000, saved after each ask and with the batched saves; the latest outcome of each remote
  pin served, at most 1 000, saved with the remote pins waiting; NORMAL, changed rows only; tables of their own, so
  `remote_pins` keeps its released meaning);
- `published`, `published_earlier`, `withdrawals` (publisher: last rounds, earlier rounds' nodes, pending
  withdrawals; NORMAL every 10 nodes or 30 s of a round and at stop, changed items only);
- `provider_records` (read per key; indexed by publisher and last-stored time for eviction and expiry; `pooled` is
  always 0 since 0.2.0), `provider_publishers` (first-seen times) and `provider_standing` (0.2.0: each publisher's
  record count, tier, refresh days and last refresh day; indexed by tier and count), NORMAL every 30 s, when 20 000
  changes wait, and at stop, changed rows only; `provider_departed` and `provider_evicted` are 0.1.x's, emptied by
  0.2.0's step (`providers_on_disk`, which also builds the standings, `statedb.PROVIDER_STANDING`) and unused;
- `routing` (0.2.0: the routing table, `RoutingState`: one row per node in the table, dropped with its seniority
  kept, or banned, times as Unix time; NORMAL with the periodic saves and at stop, changed rows only) and
  `peer_scores` (0.2.0: local scores, `Scores`: score, when it was set, last raised, seeded; at most 4096 rows;
  NORMAL with the periodic saves and at stop, changed rows only) and `announce_first_seen` (0.2.0: the announce
  first-seen log, `FirstSeenLog`: first heard and about when last heard; at most 65,536 rows; NORMAL with the
  periodic saves and at stop, changed rows only);
- `names_published`, `names_held` (sequences as decimal text, up to 2^64 − 1; FULL, changed rows only; a
  malformed row stops startup, as a malformed `names.json` did, since skipping it could allow rollback) and
  `name_floor` (highest sequence of names resolved for the gateway or auto-fetch and when, in `used` order, at
  most 4 × `name_table_size`, or 8 × for names resolved within 7 days; NORMAL, saved by a resolve or record
  raising it, at most every 5 s, and by the re-announce loop when changed); `name_exports` (keys known to exist
  outside this node: exported by `name export` (`NameManager.mark_exported`, called by the node's
  `name_export`, not by `export_key`, which keepers also use) or imported, and when; FULL, at each);
- `shared_folder` (FULL) and `shared_folder_files` (the shared folder's per-file cache, `FolderFiles`: one row per
  file per top-level entry, the `dump()`ed entry name and `[device, inode, size, mtime_ns, execute bit]` as text,
  the CID, its height and the content SHA-256 if one was computed; a `RowStore` in SNAPSHOT mode, NORMAL with the
  periodic saves and at stop, changed rows only: a power loss costs at most reading some files again);
- `jobs` (record per job in creation order; FULL, including progress saves; results keep
  at most 100 warnings plus `warnings_total`, older results capped at load); `keepers` (records re-verified at
  load; NORMAL); `follow` (FULL); `contacts` (FULL); `lxmf_messages` (inbox: 500 messages, 4 MiB, 50 per
  non-auto-fetch sender, 16 KiB per record; outbox 200, unfinished messages marked failed at a restart; NORMAL,
  changed messages only; ids and `next_id` stay below 2^63);
- `deny_entries` (the local deny list, canonical text; FULL), `deny_lists` (each source as `dump()`ed text, the
  CID of the version it last accepted; FULL) and `deny_hashes` (the content SHA-256 of files checked against
  `sha256:` entries, by binary CID, LRU order in `used`, at most 10 000; NORMAL with the periodic saves and at
  stop, changed rows only; a CID's content never changes, so a row never goes stale);
- `apps` (0.2.0, rnfs/apps.py: each app's name, the SHA-256 verifier of its token (`apiclient.app_verifier`; the
  token itself is never stored) and when it was added; FULL, each change written at once; a row that doesn't
  parse is deleted at load);
- `meta` (per-store values: follow's and each box's `clean`, set by every save, which the reconcile at start
  requires; each box's `next_id`) and `migrated` (JSON files imported).
- Schema steps of 0.2.0 that rewrite rows: `lxmf_app_pins` turns every valid `lxmf:<address>` pin into
  `app:lxmf:<address>` (a case-sensitive `GLOB` spelt out, so an invalid one is still deleted at load;
  `storage.import_pins` does the same to an older `pins.json`, imported after the steps).

The node lists its stores as `_setup` makes them (`RnfsNode._add_store`; the `node.Store` protocol: `save()`
writes what changed since the last save, raising `OSError` and keeping the change if the database fails, and
`close()` releases the store's reference to the state database). A store is batched (`_save_state`: every 30 s
and at gc) or saves on a schedule of its own (provider records, the publisher, the remote pins waiting, the
write-through stores); the features are stores too, batched. Stop saves every store in the order made, then
`_close_state` closes them in reverse, the node's own reference last; after a `_setup` that failed, only those it
made. A new store needs only its `_add_store` call.

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
writes it again). Test doubles several modules share live in `tests/fakes/` (`FakeClient`, the `PeerClient`
stand-in, and the quiet-log fixture); `test_fake_contract.py` checks the fakes against what they stand in for
(`FakeNode` against `RnfsNode`, `FakeClient` and the node tests' `NetClient` against `PeerClient`, `FakeBackend`
against `server.Backend`), so production code needn't look up with `getattr` a method a fake might lack. CI runs them on Linux, macOS and Windows; `packaging/smoke_test.py` checks a built package
(`--version`, `--help`, daemon and status, an add, and that the state database holds the pin and imported a
0.1.4 state file).

## Status

Everything in the specification is implemented, with these simplifications and additions:
- The routing table is saved in the state database (`routing_state.RoutingState`) and loaded at start. Before
  the join it is also seeded from the rnfs.dht server announces that RNS cached within 2 × `announce_interval`
  (`Identity.known_destinations`, an RNS internal). The nodes' failed queries in a row (`unreliable`) aren't
  saved.
- Lookups (`RnfsNode._lookup`) start from the closest nodes `/find` replies would give, then others to make up K,
  then unreliable ones if still short; answers go to `RoutingTable.answered`, failures to `failed`, and the
  join's lookup is wired the same way. A failed `/provide` or `/blocks` isn't counted there.
- Joining (`RnfsNode._join`, `join`): the join's candidates (`join_candidates`: up to `SAVED_JOIN_TRIES` (6)
  servers of the routing table, from `RoutingTable.join_order()`, those not failed since last heard, longest known
  first, sorted stably by score band, best first; then `bootstrap_ids()`) are asked one at a time with
  `PeerClient.find`, outside any `Lookup`, until `BOOTSTRAP_ANSWERS` (2) answer; the lookup that follows excludes
  them. `join()` returns the nodes contacted when a saved server answered and no default bootstrap node was asked;
  `_join` then calls `check_defaults` (after `joined` is set, and after each rejoin): unless the node is slow, it
  asks the defaults (`bootstrap_ids()`'s, not contacted yet) one at a time until one answers, at most
  `DEFAULT_CHECKS` (2), and runs a lookup for its own ID from the servers that one listed. With
  `default_bootstrap` off or in private mode there are none. The thread then
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
- **DAG byte bound** (`fetcher.dag_bound`, `Fetch.dag_limit`; the bookkeeping is `fetcher._DagBudget`, pure,
  the fetch's own in `Fetch.budget`). `fetch_dag` counts the bytes of the distinct
  blocks of its target's DAG, stored or downloaded, against `dag_bound(size)` = size + `DAG_GRANULE_OVERHEAD`
  (64) per `DAG_GRANULE` (4 KiB, `SmallLeaves`' threshold) of the declared size + `DAG_SLACK` (1 KiB), plus
  what the counted directory and index nodes bring (`node_credit`): a directory node its own bytes and
  `DAG_ENTRY_OVERHEAD` (16) per entry, an index node its own bytes and 16 per child less `INDEX_NODE_COST`
  (64), never below 0; and `max_dag_bytes` (a remote pin's, auto-fetch's or follow's quota) if lower. Over it is
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
  one block, `_take_batch` asks only for what fits the room left after the requests in flight
  (`_DagBudget.room`, `reserve`): a raw chunk's declared size, anything else `MAX_BLOCK_SIZE`, except that under the
  size-derived bound (not under a quota, which entries don't raise) a directory or index node takes nothing,
  since it costs at most 64 bytes (a request of `MAX_BATCH` of them overshoots by at most 2 KiB); and always
  one CID when nothing is in flight. Honest fetches are not held back by this: the content still to come
  leaves room for many file nodes, and directory nodes cost nothing.
- **Listings** (`fetch_listing`: `ls`, the gateway's directory pages) fetch only directory and index nodes,
  and are bounded as `fetch_dag` is for a declared size of 0 (`declare(0)`): `DAG_SLACK` plus the nodes'
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
  about 184 KB plus one block for any declared size. Each call's budget (`_DagBudget.for_range`, which
  counts every block against the fetch's own budget too) is passed explicitly to the `ensure` of a node's
  children by `_range_leaves`, never kept on the fetch, since another read of the same fetch may run between a
  generator's steps; `reserve` counts a file node not yet received as `MAX_BLOCK_SIZE` against it, so a
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
  wait for the first join before fetching, and a fetch cut short by stopping stays queued. Stop cancels their
  fetches (an observer bound to `stop_event`, so a fetch ends at its next step) and waits for the workers
  (`_remote_workers` maps each source to its thread) within its budget.
- **Remote pin status** (0.2.0, `rnfs/remote_pins.py`; local only, `/pin` unchanged). `AskedPins` records each
  `/pin` this node sends (`RnfsNode.pin_remote`): one entry per (node, CID), replaced by a later ask, with pin or
  release, whether accepted, when, and its checks (`tried`, the last attempt, for pacing; `checked` and `found`,
  the last conclusive one; `seen`, the last finding the node). Its state is worked out when listed: `refused` (not
  accepted, which includes no answer: `PeerClient.pin` can't tell them apart), `released`, `done` (the last
  conclusive check found the node), `missing` (it didn't, and it ran `GRACE`, 24 h, or more after the ask) or
  `waiting`. A check is `RnfsNode._providing`: the provider records held here first, else a provider lookup that
  stops once the node is among the providers (`find_providers`' `should_stop`, also bound to `stop_event`);
  inconclusive (None, nothing recorded but `tried`) when no node answered it (`Lookup.closest_answered()` empty) or
  the node is stopping. The `rnfs-remote-pin-checks` thread (`AskedPins.run`, after the first join) makes a pass
  every 30 s: the accepted pins due (first `FIRST_CHECK` after asking, then every `WAITING_RECHECK` while waiting,
  `SETTLED_RECHECK` once done or missing), most overdue first, within `CHECKS_PER_HOUR` (a deque of the last
  hour's starts, on-demand checks counted too) and `CHECKS_AT_ONCE` less those running; values are (fast, slow):
  10 min / 1 h, 1 h / 6 h, 1 day / 3 days, 60 / 4 an hour, 4 / 1 at once. `check_now` (`pin_remote_ls(check=True)`)
  runs one call at a time, the least recently tried first, at most `ON_DEMAND_CHECKS` (32 / 4), skipping pins tried
  within `FRESH` (1 / 10 min) or being checked. Every lookup, background or on demand, holds a slot of one
  semaphore of `CHECKS_AT_ONCE`, so a slow node never runs two; a pass runs its checks on up to that many
  `rnfs-remote-pin-check` threads and waits for them. Entries are kept to `MAX_ASKED` (10 000): past it the oldest
  released or refused, then done, then the rest go, never the one just recorded. `ServedPins` keeps the latest
  outcome of each (identity, CID) another identity asked of this node, recorded by `remote_pin` (waiting, refused
  `denied`, `queue_full` or `failed`, released), `_remote_pin_fetch` (fetching, pinned, dropped `quota` or by its
  error's class, `_FAILURE_REASONS`; a fetch failing or cancelled while stopping stays waiting), `_apply_denied`
  (dropped `denied`) and `_load_remote_pins` / `_resume_remote_pins` (dropped `identity_removed`, or `failed` when
  no thread starts), with a detail (a log line's reason, at most 400 characters); at most `MAX_SERVED` (1 000),
  those that ended forgotten first, then pinned ones, never one waiting or fetching. `RnfsNode.pin_remote_served`
  joins them with what is current, which wins: storage's `remote:<identity>` pins are `pinned`, the queue's
  `waiting` or `fetching`; an outcome claiming one is current that isn't (a crash lost it) shows `dropped`, reason
  `lost`; usage per identity is `Storage.source_usage`, for every identity listed in `remote_pin_identities` or
  holding pins. Both stores are `RowStore`s in SNAPSHOT mode, NORMAL: asked entries saved after each ask and with
  the batched saves, served outcomes with the remote pins waiting (every second, in `maintain`); each change tells
  the node, which publishes a `remote_pin` event.
- Byte-range reads (`cat`, gateway) walk the file lazily and fetch 32 chunks (`WINDOW`) at a time ahead of what
  has been written out. The next window is asked for before the current one is handed to the reader, so every
  provider stays busy across window boundaries (a block that arrives after the call that asked for it is stored,
  not dropped). A reader that stops early has fetched at most one window more than it read; when its fetch ends,
  the links it still has requests on are closed (`PeerClient.close_unless_used`) unless another fetch is using one,
  whose request is then left to finish: never a `Resource.cancel()`, which a 0.1.6 server cools down (NET-1). A
  read may meet at most `max_output_entries` pieces much shorter than `CHUNK_SIZE`, which bounds files built from
  repeated tiny or empty subtrees without limiting honest large files.
- `fetch_dag` and `fetch_listing` are one `ensure()` over a `_Walk`: a block's children are queued as soon as it is
  stored or downloaded, not one tree level at a time, and `max_entries` is counted per block. A parent is stored
  before its children are asked for, so an interrupted fetch resumes from what it stored, as before.
- End game (NET-4): when nothing is left to ask for and a provider is idle, a batch in flight longer than
  `deadline_slack` is asked of it too (once, not on slow nodes, within `max_parallel`); the first verified copy
  wins, and the slower request's link is closed as above, without counting as a stall.
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
- A shared-folder entry that fails to import because of its content (an `RnfsError`: the deny list, too large or
  too deep) isn't tried again until it changes; one that fails for a reason that may pass (`OSError`, including a
  pin the state database couldn't save, or `StorageFull`) is tried again `RETRY_AFTER` (600 s) later, as is one
  failing with a `ValueError` (a bug, such as a `BlockMismatch`: invalid names and special files inside an entry
  are skipped, so the content doesn't cause one), logged with its type; an unchanged entry whose pin fails is
  logged without ending the scan.
- Inside an entry whose signature changed, the scan reads only the files that changed (audit D1):
  `SharedFolder.files` (a `FolderFiles`) maps each file's `builder.file_state` (device, inode, size, `mtime_ns`,
  execute bit; the open file's `fstat`, taken before it is read) to the `Built` it gave, grouped by top-level
  entry so an entry imported again keeps only what it holds now and one gone takes its files along; a file moved
  to another entry is found too. `build_open(..., cache=)` asks it (`builder.FileCache`) before reading each
  regular file: a known file stands in only while `is_complete` holds for its CID (protected in the import's
  session first) and no block of it is denied (`denied_roots`; else it is read again and its put refuses the
  import, as before), and its remembered content hash still goes to `BuildOptions.check`; one remembered without
  a hash (built while the deny list had none) is read again when there is a check. A file whose state changed
  while it was read isn't remembered. Only fixed-chunker builds use it. It sits behind CONT-3's target check: a
  refused folder is scanned as empty, which empties it. It is a store of its own (`shared_folder_files`, saved
  with the node's batched stores, `RnfsNode._add_store`), so a restart doesn't read the folder's files again.
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
  (gateway, LXMF auto-fetch) raises the name's floor (`_floor`, a `_NameFloor`: name -> (sequence, wall time last
  resolved, used), in resolve order, `used` being its row's place; it owns its trim, load, save snapshot and timer, and
  reads as name -> (sequence, resolved)), which `_offer` checks. A raise (or a new name) saves the floor at once if the
  last save was `FLOOR_SAVE_INTERVAL` (5 s) ago, else from a one-shot `threading.Timer`; a raise by a record heard for a
  name in the table (`_store`: an announce, a keeper's record, or one a resolve's refresh heard) is always the timer's,
  started at once when a save is due, since its callers hold the lock; renewing only the time marks it for the next save
  after an hour. `_NameFloor.trim` drops the least recently resolved past `FLOOR_FACTOR` × `name_table_size`, but stops
  at one resolved within `FLOOR_KEEP_AGE` (7 days) until the floor holds `FLOOR_HARD_FACTOR` (2) times that. A resolve's
  network refresh (`_shared_refresh` around `_refresh_name`) is remembered per name (`_refreshed`: when it ended,
  monotonic, and whether it left a record; at most `REFRESHED_SIZE`, least recent dropped), so a resolve within its
  `max_age` skips it unless that record has left the table or the refresh left a suspected replay (`_absorb_recalled`);
  resolves of a name being refreshed wait on the leader's `Event` in `_refreshing`, then each runs only the local step
  (persist, floor).
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
  (no path request). The fetch checks the resolved target's size against `follow_quota` and sets it as
  `max_dag_bytes` (as remote pins do with their quota, but per name rather than per source: the old target's
  pin isn't counted, so a swap peaks at twice the quota); 0 refuses before opening the fetch.
- **Contacts.** Every `<name>` argument (`name resolve|keep|unkeep|follow|unfollow`) takes a name contact's
  nick; the gateway never expands nicks. A nick at the start of a target reuses the CID resolved for it for
  60 s (`EXPAND_AGE`) from the time of the resolve, not from when the record was heard.
- **Sealed folders.** Unpacking stores the plaintext as blocks before writing the files (both rule sets
  reused as they are, rather than a second tar extractor), so it needs the space twice on one disk; they are
  collected by `storage.collect()`, which walks every pin, right after. A marked sealed `.tar` that isn't a
  valid tar is decrypted twice (once into `build_tar`, once to the file); one with a malformed header after the
  marker fails at once, before anything is stored. Where the output's name must be chosen before the get (a
  daemon without the job manager, or a path target ending in `.tar`, whose job keeps its visible name), the CLI
  asks `GET /api/info` first and drops `.tar` only when it says `sealed_folder`; an older daemon doesn't say
  it, and doesn't unpack. `POST /api/add/stream?format=tar`
  can't seal (a tar uploaded by an app would have to be repacked under one top directory): sealed folders are
  added from a path.
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
  a manual run only builds, as do the runs for a pull request touching `packaging/**`, `pyproject.toml` or
  `.github/**` and the weekly scheduled run. The Windows exe isn't code-signed, and unpacks itself on each start.
  - `lxmf>=1.2.0,<1.3` is a regular dependency next to `rns>=1.5.6,<1.6` (`pyproject.toml`), so every
    install includes LXMF; the PyInstaller spec fails the build without it. The upper bounds keep unpinned
    installs off RNS and LXMF releases rnfs hasn't been tested with, since it relies on some of their internals.
  - `packaging/requirements.lock` pins the tested versions (rns 1.5.6, lxmf 1.2.0, cryptography 50.0.2,
    cffi 2.1.1, pycparser 3.0, pyserial 3.5) with the sha256 of every file PyPI has for each.
    `python3 -I packaging/update-lock.py [pkg==ver …]` regenerates it (no arguments: the newest versions
    `pyproject.toml` allows), then `packaging/flatpak/generate-sources.py` regenerates the Flatpak's
    `python3-deps.json` from it; both are committed. `install.sh`, `build-deb.sh`, the tests and the release's
    Windows exe install from it with `pip --require-hashes --only-binary=:all:` (then `pip check`), and
    `tests.yml`'s flatpak-sources job fails if regenerating changes the committed files. The sdist includes the
    lock.
  - `update-lock.py --build pyinstaller==<ver>` writes `build-backend.lock` (setuptools, within
    `pyproject.toml`'s `<85` bound) and `build-tools.lock` (build, twine, PyInstaller, setuptools, ruff and
    their dependencies), hashed the same way. Every build installs these with `--require-hashes` and builds
    without build isolation, so nothing unpinned is fetched for a build; the Flatpak's `build-sources.json` comes from
    `build-backend.lock`. `build-deb.sh` fails if a supported Python has no cffi backend, caps `python3` below
    3.15, and writes a DEP-5 `copyright` and `md5sums`.
  - `install.sh` installs the latest release's sdist (`rnfs-X.Y.Z.tar.gz`, the tag taken from where
    `releases/latest` redirects, so no GitHub API), the main branch's archive with `--main`, or `RNFS_SOURCE`
    (with a warning for a plain `http://` URL). A release's sdist must match the release's `SHA256SUMS` and,
    where the GitHub CLI (2.49 or newer) can check it, its signed build provenance for that tag
    (`--source-ref=refs/tags/<tag>` and `--deny-self-hosted-runners` when this `gh` has them); a `gh` with the
    attestation command that isn't logged in stops the install unless `RNFS_REQUIRE_ATTESTATION=0`, and without
    `gh` it warns. Only a wheel of the tag's version is installed from it. Since the script on `main` installs older release trees, it must keep working with every
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
