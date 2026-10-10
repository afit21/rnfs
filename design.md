# rnfs implementation design

How the code is organised to implement [specification.md](specification.md). The spec is the
authority on behaviour; this file fixes module boundaries and the interfaces between them.

## Shared foundation

- `rnfs/constants.py` — protocol constants (spec section 12), request paths, aspects, DHT modes.
- `rnfs/config.py` — `Config`: every local setting (section 12), paths under `<rnfs dir>`, and the
  `effective_*` helpers for settings that depend on slow nodes or links. Loaded from
  `<rnfs dir>/rnfs.toml` plus command-line overrides. Reticulum is started from `reticulum_dir`, by default
  RNS's own config directory, so the node attaches to `rnsd` with the matching RPC key.
- `rnfs/errors.py` — `RnfsError` and subclasses `InvalidDag`, `NotFound`, `TooLarge`, `StorageFull`, `Busy`.
- `rnfs/content/cid.py` — `Cid` (binary form `.binary`, `.codec` `RAW`/`DAG`, `.key` DHT key, `.verify(block)`,
  `str()` written form, `Cid.parse`, `Cid.from_bytes`, `Cid.for_block`). Code passes `Cid` objects everywhere;
  strings only at the user edge (CLI, API, gateway, logs) and binary only on the wire.
- `rnfs/log.py` — logging through RNS's logger.

Node IDs and DHT keys are 16-byte `bytes`. Identity hashes (for `members`, `remote_pin_identities`,
pin source `remote:<identity>`) are RNS identity hashes, written as 32 hex characters.

Persistent state is JSON (or msgpack) under `config.state_dir`, written atomically (temp file + `os.replace`).
Everything shared between threads locks internally.

## Layout and ownership

```
rnfs/
  constants.py config.py errors.py log.py          shared foundation
  content/            no RNS dependency
    cid.py            CIDs (section 4)
    dag.py            DAG node encoding, strict decoding, names (section 5)
    builder.py        building trees: files, directories, wrap, only-hash (section 5)
    walker.py         Expect, reader checks, path lookup, byte ranges, listing, writing out (sections 5, 7)
    edit.py           dir set / dir rm (section 11)
    blockstore.py     one file per CID, atomic, verified (section 8)
    storage.py        Storage: blocks + pins (sources) + cached fetches + protection + eviction/GC (section 8)
    archive.py        export/import stream (section 8)
    shared_folder.py  importing <rnfs dir>/files/ with pin source "folder" (section 8)
  dht/                no RNS dependency
    routing.py        RoutingTable: buckets, seniority, failures, bans (section 6)
    providers.py      ProviderStore: records, TTL, max_records eviction, /find reply selection (section 6)
    lookup.py         iterative lookups with alpha/beta, time budget, unknown-ID budget (section 6)
  network/            everything that talks to RNS
    protocol.py       message encoding/validation, batch sizes (sections 3, 6, 7, 8)
    rates.py          rate estimates, slow links, slow node detection (section 2)
    links.py          LinkPool: shared unidentified links, identified one-shot links, idle close (section 3)
    server.py         RequestServer: handlers and every section 3 limit; /blocks budget (sections 3, 7)
    client.py         PeerClient: /find, /provide, /blocks (with stall detection), /pin, ping (sections 3, 6, 7)
    announces.py      rnfs.dht announces carrying the DHT mode, and the announce handler (section 3)
    identity.py       node identity file, node ID helpers
  names.py            NameManager: name keys, records, publish, resolve, sequences (section 9)
  fetcher.py          fetching a target from providers (sections 6, 7)
  publisher.py        provide strategy, batched /provide, republish, withdrawal (section 6)
  node.py             RnfsNode: wires everything; the facade below
  api.py              local API, 127.0.0.1:8788 (section 11)
  gateway.py          gateway, 127.0.0.1:8789 (section 11)
  cli.py              command line (section 11)
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
- blocks: `has(cid)`, `get(cid) -> bytes | None`, `put(cid, block)` (verifies, atomic), `used_bytes()`.
- pins: `pin(cid, source)`, `unpin(cid, source) -> bool`, `pins() -> dict[Cid, set[str]]`, `is_complete(cid)`.
- cache: `record_fetch(root, names, target)` (keeps the path's nodes and the target's DAG), LRU touched on use.
- protection: `with storage.protect() as session:` — blocks stored or listed in `session.add(cid)` are kept as
  if pinned until the block exits (fetches, adds, imports in progress).
- `check_space(pinned: bool)` raises `StorageFull` when pins alone fill `max_storage` and the fetch isn't pinned.
- `evict() -> set[Cid]` enforce `max_storage` (LRU cached fetches); `gc() -> stats` evicts every cached fetch;
  both delete unreachable blocks and report targets no longer complete, so the node can withdraw them.
- `verify() -> stats` re-checks every block, deleting bad ones.
- `dag_blocks(cid) -> set[Cid]` the blocks of a complete DAG (memoised), used for "what a node serves".

## DHT layer interfaces

`routing.py` — `RoutingTable(own_id, config, clock=time.monotonic)`:
`heard(node_id) -> bool` (proven server: announce in server mode, or answered `/find`), `failed(node_id) -> bool`
(True if dropped), `remove(node_id)`, `ban(node_id)`, `is_banned(node_id)`, `closest(key, count=K, for_reply=False)`,
`seniority(node_id) -> float | None` (seconds in the table), `contains(node_id)`, `node_count()`.

`providers.py` — `ProviderStore(max_records, state_path, ttl=PROVIDER_TTL, clock=time.time, rng=random)`:
`store(key, publisher) -> bool`, `withdraw(key, publisher)`, `for_reply(key, exclude=lambda id: False) -> list[bytes]`,
`expire()`, `record_count()`, `save()`.

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
`find(node_id, key) -> (providers, nodes) | None`, `provide(node_id, keys, withdrawn) -> bool` (identified link,
batched), `blocks(node_id, cids, on_progress=None) -> BlocksResult` (status OK / BUSY / FAILED / STALLED; blocks
verified against their CIDs; `.blocks: dict[Cid, bytes]`, `.missing` answered None, `.unsent` cut by the budget,
`.bad` hash mismatches), `pin(node_id, cid, pin) -> bool`, `ping(node_id) -> float | None`, `has_link(node_id)`.

`announces.py` — `DhtAnnouncer(destination, config)` sets `app_data` to the mode and runs the announce schedule;
`DhtAnnounceHandler(on_server, on_client, is_banned)`.

## Node facade (used by api.py, gateway.py and cli.py)

`RnfsNode(config)` exposes `node_id: bytes`, `config`, and these methods. Targets are strings
`<cid>[/name…]`; CIDs are returned as strings. Return values are JSON-ready dicts.

| Method | Returns |
|---|---|
| `add_path(path, wrap=False, only_hash=False)` | `{"cid", "size", "warnings": [str]}`; file or directory, pinned `user` |
| `add_stream(stream, size=None, name=None, only_hash=False)` | as above; `name` wraps it as that entry |
| `get(target, output, from_node=None)` | `{"cid", "warnings"}`; fetches and writes the file or directory to `output` |
| `info(target, for_gateway=False, from_node=None)` | `{"cid", "type": "file"\|"directory", "size", "executable"}` |
| `read(target, offset=0, length=None, for_gateway=False, from_node=None)` | iterator of bytes of a file (range fetched only) |
| `ls(target, for_gateway=False)` | `[{"name", "kind": "file"\|"executable"\|"directory", "size", "cid"}]` |
| `stat(target)` | `{"cid", "size", "blocks", "local_blocks", "local_bytes"}` |
| `is_local(target)` | `bool`: everything needed is stored, so no network fetch |
| `dir_set(target, name, child_cid, executable=False)`, `dir_rm(target)` | `{"cid"}`, pinned `user` |
| `pin_add(target)` | `{"cid"}` once complete |
| `pin_rm(cid)` | `None`; NotFound if no `user` source |
| `pin_ls(cid=None)` | `[{"cid", "sources": [str], "complete": bool}]` |
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
| `status()` | `{"node_id", "dht_mode", "slow", "private", "known_nodes", "records", "pins", "provided", "blocks", "used_bytes", "max_storage", "names"}` |

`RnfsNode.offline(config, node_id, client, names_network)` builds a node without Reticulum or background
threads, for tests (tests/test_node.py). The request server's backend is `node.backend` (`NodeBackend`).
`fetcher.py`: `Fetcher.open(root, names, from_node, pinned)` gives a `Fetch` context (a protection session)
with `resolve()`, `fetch_dag()`, `fetch_listing()`, `read_range()`; `publisher.py`: `Publisher.refresh()`
(recompute the provide set, withdraw at once), `publish_due()`, `serves(cid)`, `run()`.

Errors: `ValueError` → 400, `NotFound` → 404, `TooLarge` → 413, `InvalidDag` → 422, `Busy` → 503,
`StorageFull` → 507, other `RnfsError` → 500. Paths passed to `add_path`, `get`, `export` and `import_archive`
are absolute paths on the daemon's machine (the CLI resolves them); stdin is uploaded as a request body.

## Tests

`python3 -m unittest discover tests` runs unit tests with no network. `RNFS_E2E=1 python3 -m unittest tests.test_e2e`
starts real daemons over local TCP. Debugging: `kill -USR1 <daemon pid>` prints every thread's stack.

## Status

Everything in the specification is implemented, with these simplifications and additions:
- The routing table isn't persisted. At startup it is seeded from the rnfs.dht server announces that RNS
  cached within 2 × `announce_interval` (`Identity.known_destinations`, an RNS internal), then the join runs.
- Publishing runs one closest-nodes lookup per due CID, one after another. Lookups aren't shared between keys
  that have the same closest nodes, but `/provide` requests are batched per node.
- Withdrawals are sent once, with any provides due for the same node. One that fails isn't retried, since the
  record expires after `PROVIDER_TTL`.
- A CID whose lookup found no nodes to publish to is retried after 30 s rather than at the next republish.
- When every provider has failed, the provider lookup runs again only if its previous run found a new
  provider (and time is left).
- `max_download_size` is checked against the target's verified size, or the range's size, before anything
  below the target is fetched. Bytes downloaded aren't counted separately.
- Roots found invalid are remembered in `state/invalid.json`, and there is no command to clear them.
- A remote pin is dropped once its root block shows its declared size would exceed the quota. That size is
  verified as the DAG arrives. Concurrent remote-pin fetches aren't limited.
- Byte-range reads (`cat`, gateway) fetch 32 chunks at a time ahead of what has been written out.

