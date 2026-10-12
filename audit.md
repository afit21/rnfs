# rnfs maintainability audit

*Date: 2026-10-13 · Revision: `d0aa468` (branch `dev`) · Scope: the whole repository (`rnfs/`, `tests/`, packaging, CI, docs)*

This audit looks at how hard the code is to change safely. It is not a bug hunt. Two incidental bugs found along the way are noted where relevant.

## Summary

rnfs is in good shape for a project of its size: about 40k lines, 1,020 unit tests plus a 28-test end-to-end suite, all passing in about 2 minutes.

**Strengths:**
- Docstrings explain *why* code exists and cite the spec.
- Policy code is pure and takes an injected clock.
- Wire and DAG parsers are strict.
- `specification.md` and `design.md` match the code.
- Supply-chain discipline is above average: hash-locked deps, SHA-pinned actions, and a 3 OS × 3 Python CI matrix.

The risks to future change are not in any single module. They come from six **cross-cutting patterns**:

1. **RNS and LXMF internals are patched and read in many places.** Upstream versions are not capped, and nothing checks for these accesses at startup.
2. **Low-level helpers are written several times over.** Atomic file writes exist 7 times, hex-ID parsing about 8 times, path resolution 4 times, and the feature utility functions 3–5 times each. The copies have already drifted.
3. **Layer boundaries leak.** The API imports the CLI. Core modules and `Config` name specific features. Features use node private members. The content store knows feature pin-source prefixes.
4. **A few classes do too much.** `RnfsNode` (~840 lines), `features/lxmf.py` (1,885 lines), `RequestServer` (~540 lines) and `Fetch` (~650 lines).
5. **Lock ownership and ordering are implicit.** Several objects have fields guarded by two different locks or none, and slow work runs while locks are held.
6. **There are no automated hygiene checks.** No lint, type-check or coverage in CI. Merge-conflict markers shipped in the 0.1.1 README, and releases are not gated on the test suite.

None of these is urgent on its own. Together they make each new feature or RNS upgrade more expensive than it needs to be. The [roadmap](#suggested-roadmap) orders the fixes by cost against payoff.

### Severity overview

| Area | High | Medium | Low |
|---|---|---|---|
| Cross-cutting | 3 | 3 | – |
| Core (node, api, cli, config, names, publisher) | 1 | 5 | 7 |
| Content layer, fetcher, gateway | 1 | 5 | 7 |
| Network and DHT | 1 | 4 | 6 |
| Features | 1 | 5 | 8 |
| Tests, CI, packaging, docs | 1 | 6 | 6 |

The cross-cutting findings in the next section are counted once, there. The per-area sections only add what is specific to that area.

---

## Method

The review was split into five areas, each read in full by a separate reviewer:
- core node, API and CLI
- content layer
- network and DHT
- features
- tests, packaging and docs

Every High finding, and a sample of the others, was then checked by hand against the source. Supporting metrics came from an AST scan of `rnfs/`:

| Metric | Value |
|---|---|
| Functions/methods | 1,313 |
| Functions > 60 lines / > 100 lines | 8 / 2 (`cli.build_parser` 136, `Publisher.publish_due` 102) |
| Broad `except Exception` / bare `except` | 95 (most in `features/lxmf.py` 13, `node.py` 10, `network/server.py` 9, `names.py` 8) |
| Function-local imports | 125+ (most in `features/lxmf.py` 32, `features/jobs.py` 27, `cli.py` 16, `features/keepers.py` 15) |
| `TODO` / `FIXME` markers | 0 |
| Unit tests / runtime | 1,020 tests, 58 s (Python 3.12, rns 1.5.6) |
| E2E tests / runtime | 28 tests, 57 s |

Most functions are short. The problems are at the module and class level, not inside individual functions.

---

## 1. Cross-cutting themes (highest leverage)

### X1 · High: RNS/LXMF internals are used throughout, with no upper version bound

**Process-wide mutations:**
- `RNS.Resource.AUTO_COMPRESS_MAX_SIZE = 0` is set twice (`node.py:214`, `network/server.py:360`).
- `guards.install()` is called three times (`node.py:215`, `network/server.py:362`, `network/links.py:190`).
- `features/lxmf.py:1540-1552` temporarily replaces `signal.signal` for the whole process while `LXMRouter` is constructed. Any other thread calling `signal.signal` in that window is silently ignored.

**Private RNS attributes read:**
- Resource advertisement fields `t/d/u/l/i/q/p` (`network/guards.py:46-56`)
- `Link.pending_requests` (`guards.py:66`)
- `Link.incoming_resources` (`links.py:502`)
- `Link.register_outgoing_resource`, which is wrapped per link (`server.py:684-705`)
- `RNS.Identity.known_destinations` entry layout (`node.py:396-411`, `names.py:175-179`, `features/peers.py:133`)
- `Transport.get_cached_packet` (`features/keepers.py:548`)
- `_no_use=True` private kwarg (`node_content.py:63-67`)

**Private LXMF attributes and methods:**
- `LxmfNetwork.__init__` (`features/lxmf.py:1576-1633`) replaces three `LXMRouter` methods.
- It reads `propagation_transfer_state`, `outbound_propagation_link` and `delivery_destinations`.
- It sets an attribute on a foreign `Link`.

**Version bounds:** `pyproject.toml:13` is `rns>=1.5.6`, `lxmf>=1.2.0`. Neither has an upper bound.

**Why it matters:** an RNS point release that renames `adv.t` makes `guards.refusal()` reject **every** incoming Resource in the process, LXMF's included. Nothing fails at startup to say why.

**Recommendation:**
- Create `rnfs/network/rns_compat.py` (plus an LXMF equivalent) to hold every patch and every private-attribute access, each with a comment naming the upstream version it was written against.
- Call it exactly once from node startup. It should check that the attributes it needs exist and log loudly, or refuse to start, when they don't.
- Add a unit test that imports RNS/LXMF and asserts the attributes exist, so a dependency bump fails in CI.
- Cap the versions: `rns>=1.5.6,<1.6`, `lxmf>=1.2,<1.3`.

### X2 · High: core primitives are written several times and have drifted

| Primitive | Copies | Drift |
|---|---|---|
| Atomic file write | `content/blockstore.py:59-89`, `content/storage.py:81-97` (`write_json`), `names.py:747-762`, `cli.py:56-69` (`write_token`), `network/identity.py:45-65`, `dht/providers.py:346-360`, `features/lxmf.py:424-444` | `storage.write_json` fsyncs the file and its directory. `names._write_atomic` never syncs the directory and uses a fixed temp name. `write_token` doesn't fsync at all. `design.md` promises "atomically and durably" for all state. |
| 32-hex ID / identity-hash parsing | `api.parse_hex_id` (`api.py:149`), `identity.parse_node_id` / `parse_identity_hash` (`network/identity.py:88-101`), `protocol.is_id` (`protocol.py:52`), `lookup._valid_id` (`dht/lookup.py:41`), `routing.check_id` (`dht/routing.py:20`) plus an inline copy (`routing.py:205`), `names.parse_name` (`names.py:78`), `config._HEX_ID` (`config.py:38`), `contacts.parse_hex`, `lxmf._HEX32`, `jobs._NODE_ID`, inline `.strip().strip("<>").lower()` (`node.py:264`, `server.py:356`) | Rules differ on letter case and on whether `<…>` brackets are allowed. |
| Feature utility helpers | `human_size` ×3 (`lxmf.py:165`, `jobs.py:710`, `peers.py:169`, plus `log.size`), `_printable` ×3 (`contacts.py:322`, `follow.py:339`, `peers.py:237`, plus `lxmf.py:111` and `fsutil.printable`), `_log` ×5, `_feature(handler)` ×5 | Three different size formats. Two `_log` level conventions (`"info"` vs `"LOG_INFO"`). `_feature` sometimes raises `NotFound` and sometimes doesn't. |
| Path resolution | `walker.resolve` (`walker.py:241-252`), `edit._directory_chain` (`edit.py:16-31`), `Fetch._resolve` (`fetcher.py:326-340`), `Storage._path_cids` (`storage.py:480-506`) | `_path_cids` re-implements the spec's "greatest `first_name` ≤ name" index rule separately from `walker.step`. |
| Strict msgpack reader | `dag._Reader` (`dag.py:145-224`), `archive._StreamReader` (`archive.py:56-142`) | Same tag tables. `archive_size` (`archive.py:43-53`) re-derives the encoding byte by byte. |
| Archive export | `node.export` (`node.py:993-1015`), `node_content.export_stream` (`node_content.py:194-208`) | `export_stream` omits `max_output_entries`. |
| "Read exactly n bytes" | `builder._read_full` (unused), `chunker.read_full`/`_read_limited`, `seal._read_segment`, `archive._take` | – |
| Constants | `PARTIAL_SUFFIX` (`node.py:100`, `jobs.py:60`), the `".partial-"` literal (`storage.py:84`, `lxmf.py:429`) instead of `blockstore.PARTIAL_PREFIX`, `RESPONSE_MAX_GRACE_TIME * 1.125` (`links.py:36`, `server.py:42`) | – |

**Recommendation:**
- Add `fsutil.write_atomic(path, data, *, mode=0o644, durable=True)` and route all seven writers through it.
- Add one `rnfs/ids.py` for ID parsing.
- Add one `rnfs/features/_common.py` (or use `fsutil`/`log` directly) for the feature helpers.
- Add one `walker.walk_path()` generator used by `resolve`, `edit`, `fetcher` and `storage`.

These are mostly mechanical changes, and they remove a whole class of "fixed it in one copy" bugs.

### X3 · High: layer boundaries leak in both directions

The intended layering in `design.md` is content/network/dht → node → api/cli, with features as plugins. These imports and accesses go against it:

| Leak | Where |
|---|---|
| The API server imports the CLI | `api.py:75` imports `CHALLENGE_HEADER`, `PROOF_HEADER` and `api_proof` from `rnfs.cli`. |
| The gateway depends on the API module for its HTTP framework | `gateway.py:46`. `TimedHandler`, `BoundedHTTPServer`, `LocalServer` and `BodyReader` (~290 lines) live in `api.py:194-481`. |
| Core modules name specific features | `api.py:135` (`contacts.is_nick`), `api.py:816` (`keepers.parse_name_arg`), `node.py:631-633` (`contacts.expand_target`), `cli.py:444/462/511/661` (`jobs.run_transfer`, `jobs.StreamProgress`) |
| `Config` owns every feature's settings | Fields at `config.py:96-119`. `Config.validate` imports `features.lxmf` (`config.py:206`). This contradicts the comment at `config.py:19`. |
| The content store knows feature pin sources | `storage.check_source` hard-codes `"follow:"` and `lxmf:` (`storage.py:52-67`) |
| Features use node and name-manager private members | `node._lookup`, `node._is_known` (`keepers.py:637,646`), `node.names.keepers = self` (`keepers.py:382`), argparse's `_actions`/`_SubParsersAction` (`keepers.py:714-715`) |
| Content helpers use node private members | `node._note_fetched`, `_prune_fetched`, `_fetching` (`node_content.py:29-31,186,198`) |
| The network layer imports a top-level module | `network/client.py:16` and `network/server.py:32` import `rnfs.mesh` |
| Hidden import cycle | `gateway_formats` ↔ `gateway`, hidden by a function-local import (`gateway_formats.py:33`) |

**Why it matters:** adding a feature currently means editing `config.py`, `api.py`, and sometimes `node.py` and `storage.py`. The `Feature` contract in `features/__init__.py` exists precisely to avoid that.

**Recommendation:**
- Move the token and proof helpers to `rnfs/auth.py`.
- Move the HTTP framework to `rnfs/httpserver.py`.
- Add `Feature` hooks: `settings()`, `validate_config()`, `expand_target()`, `resolve_name_arg()` and `pin_source_prefixes()`.
- Make `_lookup`/`_is_known` public.
- Give `NameManager` a `set_keepers()` method.
- Move `StreamProgress`/`ProgressLine` out of `features/jobs.py`; `rnfs cat` uses them and has nothing to do with jobs.

### X4 · Medium: a few classes carry too many responsibilities

| Unit | Size | Responsibilities mixed |
|---|---|---|
| `RnfsNode` (`node.py:205-1044`) | ~840 lines | Startup wiring, DHT join and seeding, maintenance scheduling, fetched-target bookkeeping, a remote-pin work queue (`node.py:823-915`), and the public facade |
| `features/lxmf.py` | 1,885 lines | URI and field codec (175-414), `MessageLog` persistence (416-685), `LxmfFeature` (691-1323), API handlers, CLI printing, `LxmfNetwork` RNS adapter (1537-1885) |
| `RequestServer` (`network/server.py:330-870`) | ~540 lines | Handler dispatch, link admission, RNS Resource tracking and monkeypatching, budget settlement |
| `Fetch` (`fetcher.py:220-872`) | ~650 lines | Traversal policy, provider ranking, pacing, lookup threads, event loop |

**Recommendation:**
- Extract `RemotePinQueue` and `FetchedSet` from `RnfsNode`.
- Turn `features/lxmf.py` into a package: `uri.py`, `messagelog.py`, `feature.py`, `network.py`, `cli.py`.
- Extract `ResponseTracker` from `RequestServer`.
- Extract a provider scheduler from `Fetch`, and replace its positional string-tagged event tuples with `NamedTuple`s.

### X5 · Medium: lock ownership and ordering are implicit

**Fields guarded by more than one lock, or by none:**
- **`Response`:** `charged` and `finished` are guarded by `RequestServer.lock`. `resource` (`server.py:712,836`) and `sent` (`:853`) are written without a lock, and `finished` is read without one (`:572,597,824`).
- **`Job`:** `state` is written under both `JobsFeature._lock` (`jobs.py:507,586`) and `Job._lock` (`:135`). `total_bytes` is written with no lock (`:109`).
- **`Exchange`:** fields are written on RNS threads and polled without a lock (`links.py:60-86`). This relies on the GIL and on write order, which is not documented.

**Implicit lock order:** the order is names → publisher → node → storage, and it isn't written down anywhere:
- `NameManager.publish/remove/release_gone` pins and calls `publisher.refresh()` while holding `names._lock` (`names.py:288,315,365-367`).
- `Publisher.refresh` walks every pin's DAG under its lock and takes `node._lock` (`publisher.py:252-258`).
- RNS announce threads can therefore block behind a full provide-set recompute.

**Slow work under locks:**
- `storage.evict/gc/verify` run full DAG walks with disk reads under the storage RLock (`storage.py:706,721,738`). The module docstring (`storage.py:16`) says long walks happen outside the lock.
- `_note_fetched` does an fsync'd write under `node._lock` (`node.py:513-519`).

**Busy-wait:** `server.py:670-674` polls with `time.sleep(0.01)` for up to 5 s on an RNS request thread.

**Recommendation:**
- Document the lock order in `design.md` § Threads.
- Give each mutable field exactly one owning lock, and say which in a comment.
- Move pin and refresh calls out of the names lock.
- Snapshot targets under the storage lock and walk them outside it.
- Wait on `response.done` instead of polling.

### X6 · Medium: no automated hygiene checks in CI

- **No lint, format, type-check or coverage.** Neither `pyproject.toml` nor `.github/workflows/tests.yml` configures any. The code is fully annotated and already has `# noqa` comments, so the groundwork is there.
- **Things a basic check would have caught:**
  - merge-conflict markers in the released README (T-H1)
  - a stray `#yo` comment (`rnfs.py:3`)
  - unused code: `builder._read_full`, `announces.read_mode`, `PeerLink.mtu`, `guards.installed`, `constants.REQUEST_FRAMING`, and the unused `when` at `follow.py:178`
- **95 broad exception handlers.** Many are deliberate best-effort guards around RNS. Some swallow errors without logging (`fetcher.py:652`, `seal.py:387`).

**Recommendation:** add a fast ubuntu-only `lint` job:
- `ruff check` with the pyflakes, isort and bugbear rule sets
- `ruff format --check`, if wanted
- `! git grep -nE '^(<<<<<<<|>>>>>>>) '`

Optionally, add `mypy rnfs` with a permissive baseline that is tightened over time, and `coverage run` with a report. Coverage would also make the test gaps in T-L3 visible.

---

## 2. Core: node, API, CLI, config, names, publisher

**C-H1 · High: `RnfsNode` is used as a bag of components.** See X4 for its size. On top of that, callers drive its internals directly:
- Features call `node.publisher.refresh()` 7 times and `node.storage.pin/unpin` directly.
- The "pin, then `publisher.refresh()`" sequence is repeated 17 times inside `node.py`, and again in `_NamePins` (`node.py:145-154`).

*Fix:* add public `node.pin(cid, source)`, `node.unpin(...)`, `node.fetching(...)` and `node.note_fetched(...)` methods that refresh internally, so components stop being reached through the node.

**C-M1 · Medium: add options are encoded and decoded in two places, and `seal_name` changes type.**
- `cli.add_options` (`cli.py:348-365`) and `node_content.add_options` (`node_content.py:214-237`) are paired only by convention.
- `seal_name` is a `bool` in `add_path` (`node.py:566`) but a `str` in `add_stream` (`node.py:594`).
- *Incidental bug:* `node_content.py:233` applies `parse_bool` only to values that are *already* bools, so `POST /api/add {"seal_name": "false"}` passes the string `"false"` to `add_path`. A non-empty string is truthy, so the name is sealed anyway. That is the safe direction, but it shows the type confusion.

*Fix:* split the parameter into `seal_name: bool` and `sealed_name: str | None`, and define one shared options schema.

**C-M2 · Medium: the offline node is half-built, so the facade is probed with `getattr`.**
- `RnfsNode.offline()` uses `cls.__new__` (`node.py:251`) and never sets `identity`, `destination` or `paths`.
- Callers therefore probe with `getattr(node, "identity", None)` (`node_content.py:52,115,130`, `node.py:1036`, `gateway.py:541`).
- `node_content.py:63-67` catches `AttributeError` to detect the offline case.

*Fix:* initialise every attribute (as `None`) in `_setup`, describe the facade as a `typing.Protocol`, and inject identity recall the same way `is_known` is already injected.

**C-M3 · Medium: config is the extension point for every feature.** See X3. *Fix:* per-feature settings dataclasses, or `Feature.settings()`/`validate()` hooks.

**C-M4 · Medium: `maintain()` schedules work with an ad-hoc dict.** The `last` dict (`node.py:461-484`) mixes `-inf` with `setdefault(now)` to mean "run on the first pass". *Fix:* a small `Periodic(interval, first_run=…)` helper.

**C-M5 · Medium: the HTTP server framework lives in `api.py`.** See X3. Timeout defaults are also duplicated between `TimedHandler` and `LocalServer` (`api.py:289-291` vs `454-456`).

**Low:**
- **Long functions:** `cli.build_parser` (136 lines, `cli.py:688`), `Publisher.publish_due` (102 lines, `publisher.py:318`), `cli.run_daemon` (80 lines, `cli.py:263`).
- **Mode strings vs constants:** the literals `"server"`/`"client"` (`node.py:166`, `announces.py:47`, `features/peers.py`) sit alongside the `MODE_SERVER`/`MODE_CLIENT` ints in `constants.py`.
- **Misleading type:** `Publisher._provided: frozenset[Cid]` also holds `DhtKey` (`publisher.py:129`). Use a type alias.
- **Re-exported helper:** `fsutil.printable` is re-exported as `api.loggable` (`api.py:114`) and `cli.printable` (`cli.py:38`), and `jobs` imports it from `cli`.
- **Repeated call:** `_make_private_dirs` is called twice (`node.py:212,265`).
- **Untyped hook:** `node.names.keepers = self` is an untyped attribute set from outside, with `None` checks scattered through `names.py`.
- **Stale doc:** `design.md`'s facade table says `pin_ls` returns `{cid, sources, complete}`. The code also returns `type` and `name` (`node.py:795`).

## 3. Content layer, fetcher, gateway

**N-H1 · High: path resolution is implemented four times.** See X2. Lookup rules are the most likely part of the format to change, and they live in two separate implementations.

**N-M1 · Medium: the content layer loads all of RNS.**
- *Measured:* importing `rnfs.content.dag`, `builder` or `storage` loads 46 `RNS.*` modules, `RNS.Transport` among them. Only `content/cid.py` loads none.
- *Cause:* `from RNS.vendor import umsgpack` runs `RNS/__init__`, and `rnfs.log` does `import RNS`.
- The package docstring does acknowledge "bundled msgpack (and logging)". The practical effect, though, is that the lazy-import workarounds in `seal.py:25,71-75,172` and `walker.py:433` don't avoid loading RNS.

*Fix:* depend on standalone `msgpack`/`umsgpack` and a logging shim with no RNS import. Add a test asserting `"RNS" not in sys.modules` after importing `rnfs.content.*`.

**N-M2 · Medium: generic JSON persistence lives in `content/storage.py`.**
- `read_json`/`write_json` (`storage.py:81-139`) are imported by 12 modules across every layer, mostly through function-local imports.
- Partial files in `state_dir` are never cleaned up; only the blocks directory is (`blockstore.py:31-34`).

*Fix:* add `rnfs/jsonstate.py` on top of `fsutil.write_atomic` (see X2), with startup cleanup.

**N-M3 · Medium: the store knows pin-source prefixes, and drifts from the spec.**
- `storage.check_source` hard-codes `follow:` and `lxmf:` (`storage.py:52-67`).
- The module docstring (`storage.py:3`) lists four sources.
- `name:` accepts any non-empty string, but spec §8 requires 32 hex characters.

*Fix:* a registry of prefixes and validators that features register into.

**N-M4 · Medium: `Fetch` uses implicit protocols.**
- Events are positional tuples unpacked by index (`fetcher.py:284,604-613,719,760-763`).
- `InvalidDag.root` is monkey-patched onto exception objects (`187-188,292,303-304`).
- The context manager is driven by hand (`264-265,294`).
- `describe()` reads `fetch._providers` (`878`).
- `Fetch.get` duplicates `Storage.getter` (`fetcher.py:312-317` vs `storage.py:309-319`).

*Fix:* `NamedTuple` events, a real `root` field on `InvalidDag`, and `contextlib.ExitStack`.

**N-M5 · Medium: the chunker is chosen by ambient `ContextVar` state.** The output CID of `build_file` depends on hidden context (`chunker.py:56,67-78`, `builder.py:136-137`), and threads don't inherit the `ContextVar`. *Fix:* pass `chunker` explicitly.

**N-M6 · Medium: `Session` reaches into storage privates.** It uses `storage._lock` and `storage._put` (`storage.py:160,166`). *Fix:* a narrow internal API.

**Low:**
- **Confusing names:** `dag.MAX_SIZE` (2⁶⁴−1) and `chunker.MAX_SIZE` (128 KiB) share a name.
- **Four "type" vocabularies:** dag ints, walker strings, `kind_of` strings and `Built.kind` ints.
- **Mixed concerns in `builder.py`:** it holds the secure filesystem-walk code (`O_NOFOLLOW`, junctions, `FD_WALK`, `builder.py:211-337`) that `shared_folder` also uses. Move it to `fsutil` or `content/fswalk.py`.
- **Repeated validity check:** `len(block) <= MAX_BLOCK_SIZE and cid.verify(block)` appears 4 times (`blockstore.py:109`, `storage.py:261,288,437`).
- **Gateway duplication:** the 304 helpers are duplicated (`gateway.py:481-486`, `gateway_formats.py:129-134`). `gateway.decode_segment` (`gateway.py:453-461`) re-implements part of `dag.check_name` and skips the 255-byte limit.
- **Scattered normative constants:** `MAX_ROOTS`, `MAX_NAME_BYTES`, the CID prefixes, the FastCDC parameters and the seal format all live outside `constants.py`, which says it holds the protocol constants.
- **Silent broad excepts:** `fetcher.py:652` and `seal.py:387`.

## 4. Network and DHT

**W-H1 · High: RNS patching is scattered.** See X1.

**W-M1 · Medium: request handlers repeat boilerplate, and adding a message type touches five places.**
- The `start_request` / `try` / `finally: finish_request(...)` pattern is copied in four handlers (`server.py:483-491, 499-509, 515-521, 529-535`).
- `_handle_blocks` (`server.py:537-626`, 90 lines) ends with a three-branch `finally` that includes a no-op `if handed_over: pass` (621-622).
- A new message type touches constants, protocol encode/parse, a server handler, a `Backend` method and a client method.
- `Backend` (`server.py:46`) omits `feature_requests`, which is instead duck-typed at `server.py:372`.

*Fix:* a `with self.incoming.slot(link_id):` context manager, and add `feature_requests` to the `Backend` protocol.

**W-M2 · Medium: the clock is injected only partly.** Classes take a `clock`, yet these paths call `time.monotonic()` and `time.sleep()` directly:
- `_previous_done` (`server.py:658,670-674`)
- `PeerLink.acquire` (`links.py:130,135`)
- `_recall` (`links.py:306-308`)
- `announces.read_mode` (`announces.py:134-139`)

They can't be driven by a fake clock in tests.

**W-M3 · Medium: timing constants are scattered.**
- There are six different poll intervals: 0.25, 0.5, 0.02, 0.1, 0.1 and 0.01 s.
- `path_timeout + 120` (`links.py:213`) is unnamed.
- The 5.625 s pacing cap is derived twice (see X2).

*Fix:* name them in `protocol.py` or `rns_compat`.

**W-M4 · Medium: `PeerClient` reaches into `LinkPool`.** It takes `links.lock` and reads `links.shared` (`client.py:59-60,173-174`). *Fix:* add a `LinkPool.peer(node_id)` accessor.

**Low:**
- **Dead code:** `announces.read_mode`, `PeerLink.mtu`, `guards.installed` and `constants.REQUEST_FRAMING`.
- **Understated timeout:** the `LinkPool.get` docstring (`links.py:197`) gives a smaller worst case than the code: `await_path` plus `_recall` plus establishment plus 5 s.
- **Wire format split across modules:** the `/name` message is missing from the `protocol.py` table and is encoded in `features/keepers.py:45`.
- **Duplicate helper:** `_never` is defined in both `lookup.py:36` and `providers.py:36`.
- **Mode checked twice:** `server.py:496` and `node.py:177`.
- **Unbounded threads:** `Lookup._send` starts one daemon thread per query, and hung threads are abandoned (`lookup.py:268`).

## 5. Features

**F-H1 · High: `features/lxmf.py` is six modules in one file.** It also carries most of the LXMF monkeypatching. See X1 and X4.

**F-M1 · Medium: features look each other up in four different ways.**
- The four patterns:
  - `getattr(node, "features", {}).get("contacts")` plus `hasattr(..., "lookup")` (`lxmf.py:847,859,1145`, `jobs.py:532`)
  - `node.features.get(...) if hasattr(node, "features")` (`keepers.py:723`)
  - `node.features.get("keepers")` (`contacts.py:194`)
  - `node.feature("keepers")` (`follow.py:73`)
- Feature start order is enforced only by list order in `FEATURE_MODULES` (`features/__init__.py:20`), with a comment as the only guard. Reordering the list causes a `KeyError` at startup.

*Fix:* a declared `requires = ("keepers",)` that `feature_classes()` validates, plus a `Feature.optional(name)` helper.

**F-M2 · Medium: persisted state has no shared format or versioning.**
- contacts, follow and keepers write `"version": 1` (`contacts.py:107`, `follow.py:98`, `keepers.py:282`), but no `_load` ever reads it.
- `jobs.json` and the lxmf inbox/outbox files have no version at all.

*Fix:* one `load_versioned(path, version, migrate)` helper, and a small base class for the repeated "dirty flag + save lock + snapshot" pattern.

**F-M3 · Medium: lock ownership in jobs is unclear.** See X5. `KeeperStore.save` also re-marks the store dirty without holding its lock (`keepers.py:291`).

**F-M4 · Medium: shutdown behaviour differs per feature.**
- `jobs` joins its threads within `STOP_WAIT`.
- `lxmf` joins with a hard-coded 2.0 s per thread (`lxmf.py:765`).
- `keepers` (`keepers.py:399`) and `follow` (`follow.py:113`) never join their threads, and `follow` doesn't save on `stop()`.
- The background loops of keepers, follow and lxmf are nearly identical.

*Fix:* a shared `BackgroundLoop` helper (stop event, wake event, join with a time budget).

**F-M5 · Medium: the CLI progress widgets live in `features/jobs.py`.** `StreamProgress`/`ProgressLine` (`jobs.py:798-856`) are used by core `rnfs cat`. See X3.

**Low:**
- **Bare state strings:** auto-fetch states such as `"queued"`, `"pinned"` and `"skipped"` are bare strings across `lxmf.py:639,984,1030,1159,1238`. `jobs.py:48` already shows the better pattern.
- **Constructor does real work:** `keepers.sync_own()` imports RNS and signs records (`keepers.py:390,512-519`), which strains the "constructors must be cheap" rule in `features/__init__.py:10`.
- **API handler skips the feature:** `_api_sync` calls `_require_network()` directly (`lxmf.py:1398`), bypassing the feature's own logging.
- **Magic number:** `len(name) == 16` at `follow.py:92`, where `ID_LENGTH` is used elsewhere.
- **Re-stated defaults:** `getattr(config, "max_jobs", None)` re-states defaults that `Config` already guarantees (`jobs.py:283,344`).
- **Docstring contradicts packaging:** the `lxmf.py:25` docstring says LXMF is optional, but `pyproject.toml` requires it.

## 6. Tests, CI, packaging, docs

**T-H1 · High: unresolved merge-conflict markers shipped in the 0.1.1 README.**
- *Where:* `README.md:869` (`<<<<<<< HEAD`), `:870` (`=======`), `:895` (`>>>>>>> 3003517 …`).
- *Effect:* the "Tests" and "Known limitations" sections render inside the conflict block on GitHub and PyPI.

*Fix:* resolve the block, and add the marker check from X6 to CI.

**T-M1 · Medium: test fakes are copied and imported across test modules.**
- *Copied fakes:*
  - `Clock`/`FakeClock` ×5
  - `FakeDestination` ×3
  - `FakeLink`, `FakeNetwork`, `FakeClient`, `FakeNode` ×2 each
  - `free_port()` ×4
- *Cross-imports:* about 17 test modules import fakes from *other test modules*. For example, `test_names_airtime.py:31-34` pulls from four of them.
- *Dead fallback:* each of these imports sits behind a `try/except ImportError` fallback that can never work, because `from tests.support import …` runs unguarded first.

*Fix:* move the shared fakes into `tests/support.py` or a `tests/fakes/` package, add `tests/__init__.py`, and delete the fallbacks.

**T-M2 · Medium: the facade fake can drift silently.**
- `tests/test_api.py:24-376` re-implements the `RnfsNode` facade in about 350 lines, and test_cli, test_gateway and test_jobs reuse it.
- Nothing in the suite uses `spec=`/`autospec`, so a change to the real facade won't break the fake.
- Tests also patch many private attributes:
  - test_lxmf: `_held` ×8, `_delivered` ×7
  - test_node: `_remote_reserved` ×5
  - elsewhere: `keepers._store`, `follow._due`, `storage._delete`

*Fix:* a contract test that compares the fake's public method signatures with `RnfsNode` via `inspect`, plus small test hooks for the most-patched internals.

**T-M3 · Medium: tests synchronise with fixed sleeps.** About 30 `time.sleep` calls stand in for synchronisation (e.g. `test_api.py:747,765,852`, `test_gateway.py:378`, `test_links.py:365,375`, `test_jobs.py:216`). They are likely to flake on loaded macOS and Windows runners; `d0aa468` already had to fix cross-platform failures. *Fix:* move `wait_for()` from `test_e2e.py:74` into `tests/support.py` and use it.

**T-M4 · Medium: the end-to-end suite is one long test.** `test_e2e.test_network` runs about 220 lines of sequential scenario (`test_e2e.py:247-468`), so an early failure hides every later check. *Fix:* split it into test methods that share a class-level daemon fixture.

**T-M5 · Medium: releases aren't gated on tests, and the installer installs unreleased code.**
- The `release` job in `.github/workflows/release.yml:215` depends only on the build jobs, so a tag can be released without the test suite having passed.
- `install.sh:35` installs `main.zip` by default, so the README's `curl | sh` one-liner installs unreleased code.
- The packaging checks are copied between `release.yml` and `tests.yml`.

*Fix:* require the test matrix in `release.yml`, have `install.sh` default to the latest release tag, and move the shared packaging checks into one script.

**T-M6 · Medium: the README is duplicated and partly stale.**
- **Mismatched versions:** the Quick start links to v0.1.0 (`README.md:55,68`), while the Install table links to v0.1.1. The version appears about 17 times.
- **Broken markup:** headings contain stray `**` (`:70,76,79,88`), and there is a typo, "availible" (`:50`).
- **Duplicate walkthroughs:** two parallel five-step walkthroughs, "Quick start" (`:47`) and "Getting started" (`:386`).
- **Duplicate test docs:** test instructions appear in both the README and `design.md:391`, and the README's e2e command omits `tests.test_client`, which CI runs.

*Fix:* link to `releases/latest`, merge the walkthroughs, and keep test instructions in one place.

**Low:**
- **T-L1: five copies of the dependency list.** It lives in `requirements.txt`, `pyproject.toml`, `design.md:498`, the lock header and `update-lock.py`. `__version__` is also parsed four separate ways. Drop `requirements.txt`, or test it like the lock.
- **T-L2: CI misses Python 3.14 and two packages.** The matrix is 3.11–3.13. The arm64 `.deb` and the Flatpak are never actually exercised; the Flatpak step is `continue-on-error`.
- **T-L3: test layout doesn't mirror the package.**
  - `features/keepers.py` (775 lines) and `features/follow.py` have no dedicated test files.
  - `node_content.py` has no direct tests.
  - Several tiny files are split off by feature.
- **T-L4: no contributor docs.** There is no `CONTRIBUTING.md` covering dev setup, running a single test, updating the locks, or the CHANGELOG convention.
- **T-L5: CHANGELOG and companion material lag behind.**
  - There's no "Unreleased" entry for `d0aa468`/`22688db`.
  - `Planned-companion-applications.md` is only headings.
  - `companion-nomadnet-sites/` is undocumented and untested, yet depends on the `pin ls --json` output format.
- **T-L6: stray comment.** `rnfs.py:3` contains `#yo`.

**Spot-checks that passed:**
- All 37 commands and subcommands in `rnfs --help` match the README command tables.
- Every flag the README mentions exists in the source.
- Spec §12 constants match `rnfs/constants.py` exactly.
- `Config` defaults match the spec settings table.
- `design.md` "Layout" lists every module.

---

## Strengths to preserve

These are worth protecting during any refactor:

1. **Pure, clock-injected policy objects.** `ResponsePools`, `IncomingLinks`, `UploadPacer`, the whole of `rnfs/dht/` and `mesh.py` don't import RNS and are easy to unit-test. The DHT has no circular imports and no network dependency.
2. **Strict parsers at every trust boundary.**
   - Wire messages: `protocol.py`
   - DAG nodes: `dag.decode` re-encodes to check the input is canonical.
   - Signed records: `keepers.parse_signed`
   - LXMF fields: `lxmf.decode_field`, `parse_uri`

   These reject booleans posing as ints and over-long lists, and do cheap checks before expensive ones.
3. **One error taxonomy.** `errors.py` is mapped to HTTP statuses in one table (`api.error_status`) and shared by the gateway.
4. **Builders take only a `put(cid, block)` callback.** This keeps tree building independent of storage; `NO_PUT` gives hash-only adds.
5. **A clear `Feature` contract.** `features/__init__.py` documents the plugin lifecycle; start and stop failures are isolated per feature, and duplicate routes are rejected.
6. **Docs that are accurate.** `specification.md` matches the code on every constant and format checked, and docstrings explain *why*, citing spec sections and the attacks they defend against.
7. **Supply-chain and platform discipline.**
   - Hash-locked runtime and build dependencies come from a single generator.
   - Actions are pinned to SHAs, and builds run without build isolation.
   - CI runs a 3 OS × 3 Python matrix with real package smoke tests.
   - `test_packaging.py` keeps the locks and workflow in sync.
8. **Careful persistence.** Damaged state files are moved aside, and a corrupt `names.json` is refused, which prevents rollback attacks.

---

## Suggested roadmap

### Quick wins (≤ 1 day total, low risk)
1. Resolve the README conflict block, fix the v0.1.0 links and stray `**`, and remove `#yo` (T-H1, T-M6, T-L6).
2. Add a CI `lint` job: `ruff check` plus the conflict-marker grep (X6). Fix or `noqa` what it finds, and delete the dead code it reports.
3. Add upper bounds to `rns` and `lxmf` in `pyproject.toml` and regenerate the locks (X1).
4. Make the `release` job require the test matrix (T-M5).
5. Fix the `seal_name` parsing in `node_content.add_options` (C-M1).

### Consolidation (a few days, mechanical)
6. `fsutil.write_atomic` replaces the seven atomic-write copies; `jsonstate.py` moves out of `content/storage.py` (X2, N-M2).
7. `rnfs/ids.py` for ID parsing; `features/_common.py` for `human_size`/`_printable`/`_log`/`_feature` (X2).
8. `tests/fakes/` for the shared test doubles, plus a facade contract test (T-M1, T-M2).
9. `rns_compat.py`, which centralises every RNS/LXMF patch and check and is called once at startup with an attribute self-test (X1).

### Structural (plan as separate pieces of work)
10. Split `api.py` into `httpserver.py`, `auth.py` and the routes, removing `api → cli` and `gateway → api` (X3).
11. Add `Feature` hooks for settings, validation, target expansion and pin-source registration, then remove feature names from `config.py`, `api.py`, `node.py` and `storage.py` (X3, F-M1).
12. Break up `features/lxmf.py` into a package; extract `RemotePinQueue` from `RnfsNode`, `ResponseTracker` from `RequestServer`, and a provider scheduler from `Fetch` (X4).
13. Write down lock ownership and ordering in `design.md`, then move slow work out from under the locks (X5).
14. Have one path-walk generator in `walker` used by all four resolvers (N-H1).
