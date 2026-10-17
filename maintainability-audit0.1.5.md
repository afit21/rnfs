# 0.1.5 maintainability audit

This audit covers rnfs at v0.1.5 (`dev` @ 5bd3075, the same tree as the tag). It comes from six read-only reviews on
2026-10-17: content; fetcher and network; DHT and names; node, API and CLI; features; tests, packaging and docs. A
mechanical pass (function size, complexity, broad excepts, churn) was also run. All line references were checked
against the tree. A sample of each review's claims was re-checked by hand before writing this up.

It is a companion to [0.1.6-plan.md](0.1.6-plan.md). That plan is about speed and this audit is about the cost of
change, so items already in the plan are not repeated. Where a refactor here makes a plan item easier or safer, it
says so (see [Order of work](#order-of-work)).

**Rules for every item** (the same as the plan's):
- No wire changes: request paths, messages, CIDs, DAG bytes, chunk boundaries, and provider and name records stay
  byte-identical.
- The local HTTP API stays compatible, including status codes.
- Local state may change only with an automatic one-way upgrade.
- rnfs is the core, and user-facing features belong in companion apps.

**Sizes:** S is under a day, M is a few days, L is a week or more. **Risk** is the chance of a regression if the
change is made with the existing tests.

---

## Summary

The codebase is in good shape for its size. The docs are unusually accurate: ten spot checks of the README, the spec
and design.md against the code all matched. Each wire format has its encoder and strict decoder side by side. Clocks,
networks and RNG are injected through most of the core. The test suite is large (2001 tests) and the security tests
are thorough.

The maintainability risk is concentrated in three places:

1. **Seven files over 1,000 lines hold most of the churn.** `node.py`, `server.py`, `lxmf.py`, `names.py`,
   `storage.py`, `fetcher.py` and `cli.py` are also the files changed most often this year. Each one has more than
   one job and shares state across those jobs by hand.
2. **The same patterns are copied instead of shared.** Examples are the "dirty rows, snapshot, save, restore on
   failure" store (about 11 copies), 32-hex ID parsing (about 8), DAG path walking (4), msgpack header tables (3),
   schedulers, log throttles and token buckets. Several copies already differ.
3. **Some guarantees rest on conventions nothing checks.** Released schema steps are never edited, and four
   real-RNS test classes are assumed to run in CI but don't. Wire bytes are assumed unchanged, but the protocol
   tests only round-trip. Shutdown is assumed to release everything, and the plugin boundary is assumed to hold.
   Each of these will break silently.

The cheapest wins are in the tests and CI (TS1–TS3). Those make every later refactor safer, so they come first.

### Numbers
| Measure | Value |
|---|---|
| Source | 28,013 lines in 61 modules under `rnfs/` |
| Tests | 37,794 lines; 2001 tests, 38 skipped; 3 m 29 s wall but only ~34 s CPU (Python 3.12) |
| Functions over 60 / 100 lines | 14 / 6 (of 1998). Longest: `keepers.ask_keepers` 138, `cli.build_parser` 138, `publisher.publish_due` 127, `lookup._iterate` 125, `cli.run_daemon` 123, `server._handle_blocks` 111 |
| Largest classes | `RnfsNode` 88 methods / 1336 lines; `Storage` 59 / 891; `NameManager` 52 / 963; `ApiHandler` 48; `Fetch` 46; `LxmfFeature` 43 |
| Broad `except Exception` / bare | 153 in `rnfs/`; 111 have no `noqa` rationale (the BLE001 rule isn't enabled, so the 43 `noqa: BLE001` comments aren't checked either) |
| Function-level imports | 172, mostly in features and cli (some to keep RNS off the CLI path, some to dodge import cycles) |
| TODO / FIXME | none |
| Coverage measurement | none |

---

## Cross-cutting themes

These came up in two or more reviews, so they're best fixed once rather than area by area.

### XC1. The core/feature boundary leaks in both directions (L, med)
- **Core imports features:**
  - [api.py:150](rnfs/api.py#L150) imports `contacts.is_nick` and [api.py:939](rnfs/api.py#L939) imports `keepers.parse_name_arg`.
  - [node.py:1047](rnfs/node.py#L1047) calls `features.get("contacts")`.
  - [embedded.py:259-282](rnfs/embedded.py#L259-L282) uses keepers and jobs.
  - The core `get`, `pin` and `export` commands in cli.py run on `jobs.run_transfer` ([cli.py:377](rnfs/cli.py#L377), 392, 457, 491, 528, 607), so they can't work without the jobs feature.
- **Core hard-codes feature details:**
  - Config holds the `lxmf_*` settings and calls `lxmf.validate_settings` and `deny.validate_settings` by name ([config.py:122-130](rnfs/config.py#L122-L130), [210-223](rnfs/config.py#L210-L223)).
  - `storage.check_source` hard-codes the `"follow:"` and `"lxmf:"` pin sources ([storage.py:68-83](rnfs/content/storage.py#L68-L83)). The `"name:"` and `"follow:"` prefixes are defined again in names.py and follow.py.
  - statedb's `SCHEMA` and `LEGACY` hold every feature's tables and importers ([statedb.py:105-121](rnfs/statedb.py#L105-L121), [464-469](rnfs/statedb.py#L464-L469)).
- **keepers is core protocol under a feature label:**
  - It serves the `/name` wire path, and names.py calls back into it.
  - It reaches node privates (`node._lookup`, `node._is_known` at [keepers.py:730](rnfs/features/keepers.py#L730) and [772](rnfs/features/keepers.py#L772)).
  - It wires itself in with `node.names.keepers = self` ([keepers.py:446](rnfs/features/keepers.py#L446)).
- **Why it matters:** [features/__init__.py](rnfs/features/__init__.py) promises that a new feature doesn't mean editing the node, the API and the CLI. In practice a new stateful feature, or moving lxmf or contacts out to a companion app, means editing config.py, statedb.py, storage.py, api.py and cli.py.
- **Change:**
  1. Classify explicitly. Move core-like code out of `features/`: the keepers record, store and query logic (into e.g. `rnfs/names_keepers.py`), and `run_transfer` and `StreamProgress` (into a CLI module). Keep `features/` for app-like code.
  2. Replace named imports with hooks: `Feature.validate_settings(config)` called in a loop, a target-expander hook in place of `features.get("contacts")`, and feature-declared schema entries. Existing tables and `LEGACY` strings stay as they are, so the state is unchanged.
  3. Make `lookup()` and `is_known()` public on the node, and replace `names.keepers = …` with a `set_keepers()` method plus a `typing.Protocol` that a test checks the real feature against.
- **Don't** let features register pin-source prefixes at run time. `Storage._load` durably deletes rows that fail `check_source` ([storage.py:293-296](rnfs/content/storage.py#L293-L296)), so a node started without a feature would erase that feature's pins. Keep one static `{prefix: validator}` table in storage, holding every prefix constant, and have names, follow and lxmf import from it.

### XC2. The persisted-store pattern is copied about 11 times, and the copies already differ (M, low–med)
- **Where:** the same structure (connect, load and validate rows, delete bad ones, keep dirty sets, snapshot under the lock, write outside it, restore the dirty sets on failure) is written separately in:
  - providers ([providers.py:732-780](rnfs/dht/providers.py#L732-L780)), publisher ([publisher.py:338-371](rnfs/publisher.py#L338-L371)), denylist ([denylist.py:344-366](rnfs/denylist.py#L344-L366)), names (two different ways, [names.py:1288-1346](rnfs/names.py#L1288-L1346)) and pin_names ([pin_names.py:99-108](rnfs/pin_names.py#L99-L108));
  - the features: lxmf `MessageLog`, jobs, keepers, follow and contacts ([lxmf.py:641-662](rnfs/features/lxmf.py#L641-L662), [jobs.py:350-379](rnfs/features/jobs.py#L350-L379), [keepers.py:333-350](rnfs/features/keepers.py#L333-L350), [follow.py:164-180](rnfs/features/follow.py#L164-L180), [contacts.py:148-157](rnfs/features/contacts.py#L148-L157));
  - the node's remote-pin queue and fetched-targets store.
- **How the copies differ:**
  - Some catch `BaseException` and re-raise, names catches only `OSError` and logs, and pin_names writes under the lock.
  - jobs, follow and contacts save durably; keepers and lxmf don't.
  - contacts keeps the in-memory change *and* raises, while the others re-queue it.
  - The "clean meta plus orphan-pin reconcile" algorithm is written twice ([lxmf.py:849-877](rnfs/features/lxmf.py#L849-L877), [follow.py:125-162](rnfs/features/follow.py#L125-L162)).
- **Scenario:** a fix to the "an older snapshot must never land last" ordering lands in one store and not the other ten. Or a new store copies whichever version happens to be nearest.
- **Change:** a small `statedb.RowStore` helper that owns the save lock, the dirty keys and restore-on-failure, takes `snapshot()` and `write(tx, rows)` callbacks, and has an explicit `durable` flag. Add an `open_store(state_dir, load)` helper and a `reconcile_pins(prefix, recorded, clean, adopt, release)` helper. Table layouts don't change. Move stores over one at a time, starting with the features.

### XC3. 32-hex ID parsing exists about 8 times with different rules (S–M, low)
- **Where:**
  - The private `config._HEX_ID` ([config.py:43](rnfs/config.py#L43)) is imported by api.py, names.py and interface.py.
  - jobs.py:63, contacts.py:34, storage.py:60 and lxmf.py:106 have their own regexes, some lowercase-only.
  - `identity.parse_node_id` uses bare `bytes.fromhex` ([identity.py:148-153](rnfs/network/identity.py#L148-L153)), which accepts whitespace between digits. names.py and api.py carry comments guarding against exactly that.
  - `api.parse_hex_id` rejects `<bracketed>` IDs, while `parse_node_id` and `parse_name` accept them.
  - The member set is normalised separately in [node.py:397](rnfs/node.py#L397) and [links.py:206](rnfs/network/links.py#L206). The remote-pin identity set is normalised separately in node.py:1413 and server.py:684.
- **Scenario:** a case, bracket or whitespace fix goes into one parser and the others keep accepting different input. In private mode, the node and the link pool could disagree about who is a member.
- **Change:** one stdlib-only `rnfs/ids.py` with `parse_hex16(text, what, *, brackets=…)`, plus `Config` properties that hold the normalised member and remote-pin sets as frozensets. Narrowing `parse_node_id` only affects odd `bootstrap_nodes` values, which are already warned about and skipped.

### XC4. Errors: ValueError means two things, the status mapping is in four tables, and broad excepts hide bugs (M, med)
- **ValueError is overloaded:**
  - The API maps ValueError to 400 ([api.py:103](rnfs/api.py#L103)) and logs only 500s ([api.py:772-774](rnfs/api.py#L772-L774)).
  - Content code also raises ValueError for internal invariants, such as a block/CID mismatch on the add path ([blockstore.py:202-204](rnfs/content/blockstore.py#L202-L204), [storage.py:380](rnfs/content/storage.py#L380)). A bug there becomes a silent "bad request".
  - `SealError` and `NotRecipient` subclass ValueError ([seal.py:59-64](rnfs/content/seal.py#L59-L64)), so a tampered sealed file is a 400 while a damaged DAG is a 422. Neither is exported, so programs can only catch ValueError.
  - `SharedFolder._import` treats any ValueError as a permanent content fault ([shared_folder.py:251-254](rnfs/content/shared_folder.py#L251-L254)), so a bug there silently parks entries.
- **Four status tables:** `api.ERROR_STATUSES`, `apiclient.STATUS_ERRORS`, `gateway.ERROR_MESSAGES` and the hand-listed cases in test_apiclient.py:406-410. A new error class needs four coordinated edits.
- **Bugs reported as network failures:**
  - [fetcher.py:963-964](rnfs/fetcher.py#L963-L964) turns *any* exception from `client.blocks`, including a TypeError, into `BlocksResult(FAILED)`. It is logged at VERBOSE outside the `except`, so the debug log never gets the traceback, and the user sees "no provider could supply X".
  - [lookup.py:352-355](rnfs/dht/lookup.py#L352-L355) is `except Exception: pass` around the routing table's `on_answer` and `on_failure`. If private mode's `admit` raises, that node is never added and nothing is logged.
  - The same pattern appears in links.py:487-489, mesh.py:99-100 and peers.py:75/87/125/136.
- **Change:**
  1. Add specific ValueError subclasses (`BlockMismatch`, and make `SealError` an `RnfsError` too), keeping each one's current HTTP status, and export them. Have the shared folder treat only content-caused errors as permanent.
  2. Keep one status table in errors.py (stdlib-only, so apiclient can import it) and derive the other three from it.
  3. Log unexpected exceptions at WARNING *inside* the `except`, so the debug log records the traceback. Narrow the catch where the expected failures are known.
  4. Enable ruff BLE001 (see TS10) so each broad except carries a reason.

### XC5. Shutdown is not exception-safe, and threads outlive `stop()` (M, med)
- **Where:**
  - [node.py:492-528](rnfs/node.py#L492-L528): `announcer.stop`, `announce_handler.deregister` and `server.stop` are not guarded. If one raises in an embedded node, `_close_state` and `_release_destination` are skipped. The destination stays registered and the database stays open, so the next `rnfs.Node` in the process fails.
  - The stores are named in three hand-written lists, in `_close_state` ([node.py:544](rnfs/node.py#L544)), `_save_state` and `stop`, plus a fourth schedule in `maintain`. A store added to `_setup` but missed in the `_close_state` tuple keeps its statedb reference. The reference-counted registry ([statedb.py:374-383](rnfs/statedb.py#L374-L383)) then reuses the old connection on the next start.
  - The remote-pin workers ([node.py:1348](rnfs/node.py#L1348)) aren't in `self.threads`, and their fetches don't watch `stop_event`.
  - follow, deny and keepers `stop()` only set an event and never join. lxmf and jobs join with their own hard-coded 2 s ([lxmf.py:946](rnfs/features/lxmf.py#L946), [jobs.py:57](rnfs/features/jobs.py#L57)), and the node's 5 s budget is never passed down.
- **Scenario:** follow's thread is still inside `check()` when the node closes the state, so its `_save` raises StateDbError after `storage.pin` already ran. Correctness then rests on the next start's reconcile, and shutdown logs spurious errors.
- **Change:**
  - Register stores as `_setup` builds them (`self._stores.append(x)`), behind a `save()` / `close()` protocol, and loop over that list.
  - Write `stop()` as an ordered list of individually guarded steps.
  - Change the feature contract to `Feature.stop(deadline)`, with base-class `_spawn` and `_join(deadline)` helpers.
  - Track the remote-pin workers and run their fetches under an observer bound to `stop_event`.

### XC6. Clocks are injected, but waits are real (M, low)
- **Where:**
  - `server._previous_done` uses `time.monotonic()` in spite of `self.clock` ([server.py:1044](rnfs/network/server.py#L1044), 1054).
  - `PeerLink.acquire`, `_open` and `_recall` use `time.monotonic` ([links.py:152-339](rnfs/network/links.py#L152-L339)), while `LinkPool.request` compares against `self.clock()`.
  - `Lookup._iterate` blocks on a real `results.get(timeout=…)` ([lookup.py:292](rnfs/dht/lookup.py#L292)), and `fetcher._download` does the same.
  - `NameManager._refresh` waits on a real `time.monotonic()` for `path_timeout` ([names.py:858-866](rnfs/names.py#L858-L866)). That alone is 7 of the 13 seconds test_names takes.
- **Effect:**
  - About 84% of the suite's wall time is idle.
  - 27 tests assert wall-clock bounds (`assertLess(elapsed, …)`). One already had to be widened for CI (2f2b629), and `test_fetcher.py:755` leaves a thread sleeping 5 s after the test ends.
- **Change:** use the injected clock everywhere and add an injectable waiter or thread starter where code blocks, following `test_pacing.py`, which already runs in virtual time. Drop wall-clock upper bounds where a counter already proves the point, e.g. [test_publisher.py:618](tests/test_publisher.py#L618), where line 614's `running.peak == PUBLISH_LOOKUPS` makes the timing check redundant.

### XC7. Test fakes: shared but kept in test modules, duplicated and unchecked (M, low)
- **Where:**
  - [tests/support.py](tests/support.py) is 117 lines. The real shared doubles live in big test modules and are imported across the suite: `from tests.test_names` 36 times, `test_node` 27, `test_api` 13 and `test_server` 11. The security helpers import from test modules too.
  - Duplicate fakes: FakeDestination (3 copies), FakeLink (`test_server.py:265`, `test_links.py:36`), FakeClient (`test_fetcher.py:46`, `test_publisher.py:39`), FakeLookup and FakeNetwork.
  - Production code bends to incomplete fakes. [fetcher.py:927-930](rnfs/fetcher.py#L927-L930) does `getattr(self.client, "link_mdu", None)` because "test fakes may not know links", and `server.Backend` marks two methods "optional (looked up with getattr)" because `FakeBackend` lacks them. So tests run a different code path from production.
  - `test_fake_contract.py` checks only FakeNode against RnfsNode.
  - `LxmfNetwork` tests build the object with `__new__` and hand-assign up to 12 private attributes ([test_lxmf.py:929-934](tests/test_lxmf.py#L929-L934), 1323-1333), so any new field breaks them.
  - The same `setUpModule` / `tearDownModule` log-quieting pair is repeated in 45 files.
- **Change:**
  - Add a `tests/fakes/` package (network, node, client, rns_link, lxmf) and one shared quiet-log fixture.
  - Make `link_mdu`, `served_block_size` and `feature_requests` required, and add them to the fakes.
  - Extend the contract test to FakeClient↔PeerClient, FakeBackend↔Backend and FakeLink↔the RNS.Link surface used.

### XC8. Small rules restated locally
Each of these is S and low risk. Fix them as the surrounding code is touched.
- **"Two missed announces":** `routing._silence_limit`, `names.ANNOUNCE_VALIDITY = 2` and `follow.STALE_ANNOUNCES = 2` (and deny's copy). Use one constant in constants.py.
- **60 s log throttles:** written three times ([server.py:820-830](rnfs/network/server.py#L820-L830), [guards.py:182-201](rnfs/network/guards.py#L182-L201), [guards.py:388-393](rnfs/network/guards.py#L388-L393)). Move `guards._ThrottledLog` into `rnfs.log` and reuse it.
- **Token buckets:** lxmf hand-rolls three ([lxmf.py:1218-1240](rnfs/features/lxmf.py#L1218-L1240), 2132-2150), while `mesh.TokenBucket` already has an injectable clock and locking.
- **Formatting helpers:** `human_size` is written three times and they disagree ("1.5 MB", "1.50 MB", and "?" vs "-" for None), and the timestamp formats differ. Use one no-RNS `rnfs/fmt.py`.
- **Block validity check:** `len(block) <= MAX_BLOCK_SIZE and cid.verify(block)` appears five times in blockstore.py and storage.py. Add a `blockstore.matches(cid, block)` helper.
- **`chunker.MAX_SIZE`:** it is commented "= MAX_BLOCK_SIZE" but not derived from it, it decides fastcdc CIDs, and it shares its name with the unrelated `dag.MAX_SIZE`. Rename it `FASTCDC_MAX` and assert `<= MAX_BLOCK_SIZE`.
- **"file" / "directory" literals:** compared as plain strings in edit.py, node.py, fetcher.py, deny.py and node_content.py, although walker.py defines `FILE_TYPE` / `DIRECTORY_TYPE`. `KIND_NAMES` lives in node.py, not next to `dag.KIND_*`.

---

## Tests, CI and docs (TS)

### TS1. Four real-RNS test classes never run in CI (S, low): do first
- **Where:** [tests.yml:41-44](.github/workflows/tests.yml#L41-L44) sets `RNFS_E2E=1` only for `tests.test_e2e tests.test_client`. These classes are gated on the same variable and never selected:
  - [test_embedded.py:464](tests/test_embedded.py#L464) `HostProgramTest`, the only real-RNS test of `rnfs.Node`;
  - [security/test_h1_real_rns.py:37](tests/security/test_h1_real_rns.py#L37);
  - [security/test_m1_real_rns.py:45](tests/security/test_m1_real_rns.py#L45);
  - [security/test_h2_display_page_pins.py:306](tests/security/test_h2_display_page_pins.py#L306).
- **Why:** these are security regressions plus the embedded mode, and design.md:982-985 implies CI runs them.
- **Change:** run `python -m unittest discover tests` with `RNFS_E2E=1` in the E2E step. Or list every gated module and add a test that each `RNFS_E2E` gate's module appears in tests.yml.

### TS2. Nothing protects upgrades from 0.1.5 state (S–M, low): do before any 0.1.6 schema change
- **Where:**
  - `tests/fixtures/` holds only `rnfs-0.1.4/`, which is JSON. There is no `rnfs.db` written by 0.1.5, the first SQLite release.
  - "Steps are only ever appended" ([statedb.py:43-44](rnfs/statedb.py#L43-L44)) is just a comment. [test_statedb.py:94-102](tests/test_statedb.py#L94-L102) upgrades from the *current* code's `SCHEMA[:2]`, so editing a released step in place passes.
  - Fresh-DB tests can't see that kind of edit. Upgraded databases miss the change, and every test still passes.
  - Two branches that each append a step collide on the same index with no error.
  - Schema steps can only be SQL, so the first migration that needs Python has no hook.
- **Change:**
  - Add a `tests/fixtures/rnfs-0.1.5/` fixture made by v0.1.5's own code (the way `generate.py` made 0.1.4's) and a `test_upgrade_015.py`.
  - Add a golden test that pins a digest of `SCHEMA[:16]` as "released, never edit".
  - Allow `(label, callable(tx))` steps.
- This also gives the plan's release-checklist item "a 0.1.5 `<rnfs dir>` starts cleanly" something to run against.

### TS3. No known-answer tests for the wire format (M, low): do before the plan's fetcher work
- **Where:** [test_protocol.py:79-203](tests/test_protocol.py#L79-L203) only encodes then decodes with the same code. The only literal vectors are for DAG and archive bytes, chunker CIDs and *malformed* name records.
- **Scenario:** a refactor changes a msgpack int width, a list/tuple choice or field order. It still round-trips, every e2e node runs the same code, and interop with 0.1.3 breaks silently.
- **Change:** add `tests/fixtures/wire/*.hex` covering valid `/find`, `/provide`, `/blocks`, `/pin` and `/name` requests and responses, announce app_data, name records and LXMF fields. Generate them once from v0.1.3 and v0.1.4. Assert both `decode(vector) == value` and `encode(value) == vector`.

### TS4. No coverage measurement (S, low)
- Coverage is not in `packaging/build-tools.lock` and no CI step runs it.
- Modules with no dedicated test file, 3,240 lines in all, so their coverage is unknown: `features/keepers.py` (969), `winacl.py` (463), `features/follow.py` (442), `network/guards.py` (409), `features/deny.py` (309), `features/peers.py` (260), `node_content.py` (255) and `network/announces.py` (133).
- **Change:** lock `coverage` and add one Linux job that runs `coverage run -m unittest discover tests` with `RNFS_E2E=1`. Report only at first, with no threshold.

### TS5. Several build paths run only when a tag is pushed (S, low)
- **Where:** the Flatpak build and its sources-match check ([release.yml:121-139](.github/workflows/release.yml#L121-L139)), the Windows exe, the arm64 .deb and the Debian 12 smoke test run only in release.yml.
- **Scenario:** a lock bump leaves the Flatpak sources stale. It's caught only after `vX.Y.Z` is pushed, and because [release.yml:37](.github/workflows/release.yml#L37) requires the tag to equal `__version__`, the fix means moving the tag or bumping the version.
- **Change:** move the cheap Flatpak sources diff into tests.yml. Run the release build jobs on PRs that touch `packaging/**`, `pyproject.toml` or `.github/**`, and weekly.

### TS6. No developer or release guide (S, low)
- **What exists:**
  - How to run the tests is one sentence (design.md:980).
  - `RNFS_FUZZ_EXAMPLES`, `RNFS_BENCH` and the full `RNFS_E2E` list are documented only in module docstrings.
  - The release checklist exists only in the untracked 0.1.6-plan.md.
  - Adding an API route touches about six places (see ND4) and is written down nowhere.
  - Nothing checks that CHANGELOG's first `## X.Y.Z` equals `__version__`.
  - Release notes use `--generate-notes` ([release.yml:264](.github/workflows/release.yml#L264)) rather than the CHANGELOG.
- **Change:** a CONTRIBUTING.md (or a "Development" section) covering tests, lint, locks, releasing, adding a schema step and adding an API route. Add a CHANGELOG == `__version__` test, and feed that CHANGELOG section into the release notes.

### TS7. The supported-Python list is in 7+ places with no cross-check (S, low)
- **Where:** `pyproject.toml:12`, `tests.yml:21`, `packaging/update-lock.py:42,60`, `flatpak/generate-sources.py:28`, `build-deb.sh:33,36` and the interpreter names `install.sh:154` tries. README.md:340 also hard-codes `pyinstaller==6.22.3`.
- **Scenario:** adding 3.15 but missing `BUILD_PYTHONS` leaves build-tools.lock without wheels for the new CI cell.
- **Change:** a `test_packaging` test that checks these lists agree with the tests.yml matrix.

### TS8. Pinned actions and the install sequence are repeated, and nothing updates them (S, low)
- `actions/checkout` is pinned 9 times and `setup-python` 7 times, each with an "update together" comment.
- The locked install sequence (requirements.lock, then the build lock, then `pip install --no-deps --no-build-isolation .`, then `pip check`) is pasted 4 times.
- There is no `.github/dependabot.yml`, although [test_rns_compat.py:5](tests/test_rns_compat.py#L5) names Dependabot as what would trigger it.
- **Change:** Dependabot for `github-actions`, which handles SHA pins with version comments; a composite `.github/actions/install-locked`; and a scheduled job that reports newer rns or lxmf releases within the pyproject bounds.

### TS9. Security tests are organised by external audit IDs (M, low)
- The 73 `tests/security/test_*.py` files are named by IDs (`m3r2`, `new6`, …) that appear nowhere else in the repo. Finding every test for `keepers.py` means searching five files.
- Seven docstrings cite `file:line` locations that have since moved (e.g. `test_l10_index_page_injection.py:2`, `test_l14_page_local_paths.py:3`).
- Small files overlap the main ones: `test_pin_false.py` (4 tests), `test_get_partial.py`, `test_server_outcomes.py` and `test_migrate_small.py`, which is grouped by file size rather than by store.
- **Change:** group the security tests by subsystem, one class per finding with its ID in the docstring, or keep the files and add an ID → module → fix-commit index. Cite function names, not line numbers.

### TS10. Lint rules don't cover the `noqa` comments in the code (S, low–med)
- [pyproject.toml:41](pyproject.toml#L41) selects F, E9, B006, B008 and B904. The code carries 43 `noqa: BLE001` and 30 `noqa: E402` comments for rules that aren't enabled, so nothing checks them. There is also a `noqa: B902`, a flake8 code ruff doesn't implement.
- The companion pages (`companion-nomadnet-sites/*/index.mu`, 554 lines of Python) are never linted.
- **Change:** enable BLE001 (start with per-file ignores and triage the 111 sites), E402, RUF100 (flags stale `noqa`) and the rest of B. Add `extend-include` for the `.mu` scripts.

### TS11. Stale planning docs, and no reference for the local API (S, low)
- [roadmap.md](roadmap.md) lists 0.1.5's deny-list and SQLite work as not done. It puts "Client Library" under 0.1.6, but it shipped in 0.1.5 (CHANGELOG.md:57), and "change notifications" shipped as `/api/name/watch`.
- The local API must stay compatible, but its routes exist only as a prose list (design.md:383-389), with no request or response shapes.
- [specification.md](specification.md) doesn't say which version it describes, nor that the wire format is byte-identical since 0.1.3.
- **Change:** update roadmap.md. Generate a route table from `api.ROUTES` plus each feature's `api_routes()`, with a test that the documented list matches. Add a version and compatibility note to the spec.

### TS12. Smaller hygiene items (S, low)
- The Flatpak manifest copies `../..` with a skip list that doesn't skip `*.egg-info`, `.ruff_cache` or `.venv`, so a local flatpak-builder run picks up dev debris.
- `companion-nomadnet-sites/file-index-node/index.mu:21-26` ships the maintainer's own CIDs as defaults.
- The plist comment says "adjust the path (both places)", but the path appears three times.
- `rnfsgifcompressed.gif` (427 KB) is tracked at the root, so every clone carries it. It could move to a release asset or `docs/`.
- `.gitignore` is correct: `__pycache__`, `rnfs.egg-info` and `.ruff_cache` are ignored, not tracked.

---

## Content layer (CT): `rnfs/content/`, `objects.py`

### CT1. Storage has its own DAG walking and path stepping, written twice inside itself (M, med): do before plan D3
- **Where:**
  - The index step rule ("the child with the greatest first_name <= name") is in [walker.py:162-169](rnfs/content/walker.py#L162-L169) and again in [storage.py:609-611](rnfs/content/storage.py#L609-L611).
  - The path walk over `_step_in` is written twice with different stop conditions: `_path_cids` under the lock ([storage.py:647-670](rnfs/content/storage.py#L647-L670)) and `_warm` without it ([712-724](rnfs/content/storage.py#L712-L724)).
  - Child listing is done separately in `Storage._listed`, `walker.children`, `dag._check_node` and `walker.check_node`.
  - `Storage.dag_blocks` ([storage.py:841-859](rnfs/content/storage.py#L841-L859)) re-does `walker.iter_dags`' walk, and `Storage.stat` is another copy of `_reach`.
  - `MISSING` means both "the block isn't stored" and "no memo entry".
- **Scenario:**
  - D3 changes `_reach` and `dag_blocks`, and any change to path semantics has to land in four places.
  - If `_warm` and `_path_cids` drift apart, warming silently stops covering what the locked walk reads, and disk reads under the lock come back (the problem `security/test_new5_storage_lock_walks.py` guards against).
  - `archive_size` ([node_content.py:213-214](rnfs/node_content.py#L213-L214)) assumes `dag_blocks` returns exactly what `iter_dags` writes.
- **Change:** add `names` and `child_cids` properties to the `dag` node classes. Put one `step_in(node, name)` in walker.py, used by both modules. Write one path-walk generator over a "node or unavailable" callback, shared by `_path_cids` and `_warm`. Use a separate `_NO_MEMO` sentinel.

### CT2. A CID-deciding option and the deny check travel as ContextVars (M, low–med)
- **Where:** `chunker._active` ([chunker.py:56](rnfs/content/chunker.py#L56)) decides fastcdc CIDs, and `builder._check` ([builder.py:120](rnfs/content/builder.py#L120)) is the deny list's file-hash check. Both are set two layers up (node.py:750, 979, 1003, 1030), and `build_file` takes neither as a parameter.
- **Scenario:** `ThreadPoolExecutor` doesn't copy contextvars. Moving directory hashing onto worker threads, a natural speed-up, would silently do two things:
  - fall back to the fixed chunker, so `--chunker fastcdc` adds get different CIDs;
  - skip the deny check.

  A new `SharedFolder.scan()` caller not wrapped in `builder.checking` would also skip the check.
- **Change:** pass an explicit `BuildOptions(chunker, check)` down through `build_file`, `build_open`, `add_path` and `add_stream`. Keep `using()` and `checking()` as thin wrappers during the move.

### CT3. The rule for which local files are included is written three times (M, med)
- **Where:** `builder._build_tree` ([builder.py:472-503](rnfs/content/builder.py#L472-L503)), `shared_folder._scan_item` ([shared_folder.py:196-222](rnfs/content/shared_folder.py#L196-L222)) and `open_signature`'s `add` ([shared_folder.py:67-89](rnfs/content/shared_folder.py#L67-L89)).
- **They already differ:**
  - The depth bound is `depth > MAX_DEPTH` in the builder but `MAX_DEPTH + 1` in the signature.
  - The hard-link count comes from the listed stat in one and from `fstat` in the other.
- **Scenario:** a new skip rule added to the builder doesn't trigger a re-import when that attribute changes, unless the signature is updated too. The `"linked"` marker exists only to patch this coupling.
- **Change:** one walk generator in the fs layer that applies the policy and yields `(relative, st, handle | None, skip_reason)`. The builder, the signature and the top-level scan all consume it.

### CT4. Cache and pin bookkeeping is spread across five removal sites (M, low–med)
- **Where:** cache records are removed by `_cap_cache`, `_evict_to`, `drop_denied` and `gc`, and retargeted by `record_fetch`. Each updates a different subset of `_cache_dirty`, `_garbage` and `_cache_generation` ([storage.py:494-513](rnfs/content/storage.py#L494-L513), 820-824, 909-915, 1033-1036).
  - The cache row key is built in four places.
  - Records are validated separately by `import_*` and `_load`.
  - The "what stopped being complete" sequence is copied in `evict`, `gc`, `drop_denied` and `verify`.
- **Scenario:**
  - A new removal path that forgets `_cache_generation += 1` leaves `_prune_fetched` unrun.
  - One that forgets `_cache_dirty` resurrects the row at restart.
  - The D3 memo change has to update every copy of the completeness sequence.
- **Change:** `_drop_records(keys)` and `_set_record(key, target)` helpers that do all the bookkeeping; `_cache_row` / `_parse_cache_row`; shared validators; and `_newly_incomplete(targets, action)`. Later, split a `CacheRecords` class out of the ~900-line `Storage`.

### CT5. msgpack is hand-parsed in three modules (M, med)
- **Where:**
  - [dag.py:155-236](rnfs/content/dag.py#L155-L236) parses over bytes.
  - [archive.py:56-138](rnfs/content/archive.py#L56-L138) parses over a stream.
  - [network/protocol.py:277-310](rnfs/network/protocol.py#L277-L310) parses responses.
  - `archive_size` ([archive.py:47-53](rnfs/content/archive.py#L47-L53)) hand-computes header lengths apart from the encoder.
- **Change:** a small `rnfs/msgpack_headers.py` holding the type tables plus `bin_header_size(n)` and `array_header_size(n)`. Share only the tables; leave `dag._decode`'s validation alone. Add a test that `archive_size == len(export bytes)` at the 255/256 and 65535/65536 boundaries.

### CT6. Smaller content items
- **Untyped results:** operation results are untyped dicts that are re-mapped by hand at three layers (`Storage.gc`, `verify` and `stat`; `node.py:836-849`; `objects.py`). Use internal frozen dataclasses and keep the JSON form unchanged. (M, low)
- **builder.py has two jobs:** about 270 lines of it ([builder.py:238-510](rnfs/content/builder.py#L238-L510)) are a secure local-filesystem layer. Move it to `content/localfs.py`. Two unrelated functions are both called `list_directory` (builder's and walker's). (S–M, low)
- **Tests reach into internals:** test_storage patches `_reach`, `_children`, `_complete_sets` and others. D3 will break those tests or quietly make them test nothing. The security tests call CPython's private `RLock._is_owned()`. Add an injectable block-read hook and a lock wrapper with `held_by_current_thread()`. (S, low)
- **Wrong docstrings:** "No RNS dependency" is false for builder, walker, storage, dag, edit, shared_folder and archive, which load RNS through `RNS.vendor.umsgpack` or `rnfs.log`. Only cid, blockstore and chunker are RNS-free. Fix the docstrings and add a test that those three don't import RNS. (S, low)

---

## Fetcher and network (NW): `fetcher.py`, `network/`, `mesh.py`

### NW1. `Fetch` does too many jobs (L, med): pair with plan A1–A4
- **Where:** [fetcher.py:357-1121](rnfs/fetcher.py#L357-L1121), 46 methods. One class handles:
  - tree walking;
  - DAG byte-budget bookkeeping;
  - provider scheduling and failure rules;
  - lookups;
  - storage.
- **State is passed around loosely:**
  - Per-download state goes around as 5–6 parameters (`_handle` takes 9). `in_flight` is both a parameter and `self._in_flight` (:806).
  - The range budget reaches `_count_dag` and `_room` through `self._range`, set and cleared around `ensure()` (:644-648).
  - `room` is a positional `(bound, quota, budget)` tuple whose parts may be None.
  - Events are positional string tuples (`("result", node_id, cids, result, sent_at, rate)`).
  - The module docstring says only the fetch thread changes state, but `progress()` writes `_progress_at` from request threads (:951-953).
- **Scenario:** a new limit has to be added to the tuple, `_reserve`, `_fits`, `_count_dag` and `dag_limit` together. A new read path that forgets `_range` silently skips the range budget. A1/A4 pipelining will add more state to this same class.
- **Change:** extract a pure `_DagBudget` (absorbing `_RangeBudget`, passed to `ensure` explicitly), a `_Providers` policy object (failure, busy and pacing rules and ranking, with a clock only) and a `_Download` state object. Use NamedTuple events. The tests already call about 15 of these private methods, which shows where the seams are.

### NW2. `Response` is shared mutable state with three locking styles (M, high: security-relevant)
- **Where:** `Response` ([server.py:233-319](rnfs/network/server.py#L233-L319)) has about 25 mutable fields, touched by RNS request threads, the RNS packet thread, the monitor thread and link-closed callbacks.
  - `idle` is set under `ResponsePools.lock` in some places and with no lock in `_check_progress` (:1350).
  - `finished`, `settled`, `charged` and `concluded` use `RequestServer.lock`.
  - `size`, `deadline`, `due`, `grace`, `untracked` and `resource` use no lock.
  - `watched` is changed without the lock, discarded under it, and read without it.
- **Why:** correctness rests on idempotence flags and the order fields are assigned in, and neither is written down. The next person to touch upload pacing has to reconstruct the model from scratch.
- **Change:** first, document which thread or lock owns each field. Then give `Response` its own lock and move its state changes (attach, conclude, finish, settle) into methods on it.

### NW3. `_handle_blocks` is 111 lines with about 9 return points (M, med)
- **Where:** [server.py:895-1005](rnfs/network/server.py#L895-L1005). The steps run in this order: cooldown, airtime, slot, pacing, grace, pool reserve, pace wait, budget, select and encode, admit, pacer, traffic, handover. One `handed_over` flag and a `finally` free the slots exactly once.
- **Change:** split it into `_admit_request`, `_reserve_slot`, `_build_body` and `_hand_over`, each returning a refusal or a value, and keep the single `finally` owner.

### NW4. Per-request slot bookkeeping is copied into four handlers (S, low)
- **Where:** `start_request` / `try` / `finally: finish_request` is written out at [server.py:841-849](rnfs/network/server.py#L841-L849), 857-867, 873-879 and 887-893.
- **Scenario:** a new handler that forgets the `finally` leaks one of a link's two slots (`MAX_REQUESTS_PER_LINK`), and that link is then refused forever.
- **Change:** a `_with_slot(link_id, fn)` helper, or have `_guarded` count the slot for every path other than /blocks.

### NW5. The /blocks size rules are copied between client and server (S, low)
- **Where:** `server.blocks_budget`, `rates.requester_max_bytes`, [fetcher.py:923-925](rnfs/fetcher.py#L923-L925) and `protocol.max_response_size`. `fetcher._pace` also hard-codes the server's default cap of half the rate estimate, copied from `config.effective_max_upload_rate`.
- **Scenario:** changing the server's cap leaves the client's batching estimates silently wrong. The wire is unaffected either way.
- **Change:** shared `rates.response_capacity()` and `default_upload_cap()` functions, called by both ends.

### NW6. Smaller network items
- **`LinkPool.get` timeout:** a waiting thread gives up after the magic `path_timeout + 120` ([links.py:237](rnfs/network/links.py#L237)). The opener can take two path timeouts plus establishment plus grace, so with a large LoRa `path_timeout` waiting threads report "unreachable" while the opener is still connecting. Add one `open_budget()` and one path-wait helper; the poll loop is also duplicated in `announces.read_mode`. (S, low)
- **Feature response shapes:** these are enforced only by a docstring. `_feature_handler` returns whatever the handler gives, and a feature that returns a dict would be refused by every 0.1.4+ peer. Run `protocol.response_refusal` on the server's own feature responses, and list `/name` in protocol.py's table. (S, low)
- **RNS internals:** these are read directly at many call sites. For example, `getattr(link, "attached_interface", None)` appears six times in server.py, client.py and guards.py. A thin accessor module registered in `test_rns_compat.INTERNALS` would turn an RNS rename into a one-place edit. (M, low)
- **Dead code that is a trap:** `AirtimePolicy.spend` and `TokenBucket.take` ([mesh.py:241-248](rnfs/mesh.py#L241-L248), 311-313) are used only by tests. They predate the reserve/charge/refund model and bypass its commitment bookkeeping. Delete them and move their tests onto `reserve` / `charge`. (S, low)

---

## DHT, names and publisher (DH)

### DH1. names.py is five modules in one, and the floor is a hand-synced bundle (L, med): do the floor part before plan E1
- **Where:** [names.py](rnfs/names.py) holds the record codec (76-167), the legacy JSON import (172-292), `RnsNetwork` (298-357), the announce handler and `NameManager` (384-1346). `NameManager.__init__` sets about 35 attributes, 9 of them for the floor (422-432).
- **The floor dicts:** `_floor` and `_floor_used` must keep the same keys, and four sites maintain that by hand. `_save_floor` indexes `_floor_used[name]` for every floor name (:1300), so one missed update is a KeyError on the floor timer's thread.
- **Duplicated refresh:** `publish` and `resolve` each have their own "refresh, then ask the keepers" block (634-641, 814-824). E1 will add caching to `resolve` and leave `publish`'s copy behind.
- **Lock scope:**
  - `_offer` does a durable save and calls listeners while holding the names lock, on RNS's announce thread (994-1002).
  - Public methods re-enter under the RLock.
  - The lock order is written only in comments.
- **Change:**
  1. Extract `_refresh_name(name, ask_keepers)` *before* E1.
  2. Extract a `_NameFloor` class with one value per name, `(sequence, resolved, used)`, which owns trim, load, save and the timer, and a `_RecordTable` class.
  3. Move the codec, `parse_name` and `check_alias` to `name_record.py`, and the JSON importers to `rnfs/legacy/`. Keep the `rnfs.names` re-exports, because statedb's LEGACY strings point at them.
  4. Queue listener calls under the lock and make them after releasing it. Write the lock order down in one place.

### DH2. ProviderStore keeps about 10 indexes in step by hand, with no consistency check (M, low)
- **Where:**
  - [providers.py:214-236](rnfs/dht/providers.py#L214-L236) holds `_records`, `entry.keys`, `entry.main`, `_count`, `_main_count`, `_pool_count`, `_pooled`, `_holders`, `_holder_index`, `_most_pooled` and the `_newest` heap. They change in five methods.
  - `_load` (787-870) repeats `store`'s insert and an inline copy of `_add_main`.
  - Tests check only `record_count()` and `publisher_count()`.
- **Scenario:** plan item S5, or a new limit, adds a counter in `_add_main` but not in `_load`. The counts drift after a restart and only show up later as wrong eviction choices.
- **Change:** one `_insert(key, publisher, record)` used by `store` and `_load`. Add a `_check_consistency()` that recomputes every index from `_records`, call it in test_providers' `tearDown`, and add one randomized store/withdraw/expire/reload sequence test.

### DH3. `Publisher.publish_due` is 127 lines in five phases under four lock windows (M, med)
- **Where:** [publisher.py:570-696](rnfs/publisher.py#L570-L696). About 8 locals cross the phases, and the "still provided?" check is repeated after each re-lock.
- **Wrong type hints:** `_provided: frozenset[Cid]` and `_published: dict[Cid, …]` also hold `DhtKey`s, which is why `provided()` has to filter. A `cid.binary` added in `refresh` would break only on nodes running keepers.
- **Change:** a `_Round` dataclass with `_plan_round`, `_send_to` and `_settle_round` methods; test_publisher already covers the behaviour. Add a `ProvidedItem = Cid | DhtKey` alias.

### DH4. `Lookup._iterate` is 125 lines over nine interlocking local sets (M, med)
- **Where:**
  - [lookup.py:211-335](rnfs/dht/lookup.py#L211-L335). Retries move in and out of `in_flight` and launch on top of alpha, so the comment "at most alpha run at a time" (:359) is wrong.
  - The constructor takes 17 parameters, four of which are callables from one `RoutingTable` ([node.py:582-587](rnfs/node.py#L582-L587)).
- **Change:** a `_Run` object with `fill()`, `retry_due()`, `merge(reply)` and `done()`, and a `NodeHealth` Protocol in place of the four callables. Also fix the swallowed callback exceptions (XC4).

### DH5. Smaller DHT and names items
- **State-row validators** are duplicated with slightly different rules, all S and low:
  - Time: `providers._number` accepts ±inf, `publisher._time` rejects it, and `names._held_row` checks only the type.
  - Integers: denylist.py:160 accepts `bool` as an int.
  - **Change:** shared `finite_time`, `int_column` and `id_column` helpers in statedb. Keep the providers' inf behaviour explicitly if it matters.
- **Legacy importers** sit next to live store code in providers, names, publisher and pin_names. `providers.read_records` re-implements `statedb.read_json`, and the `shape` parameter is unused. Move them to `rnfs/legacy/`; test_upgrade_014 already verifies every LEGACY string. (M, low)
- **The `names.keepers` collaborator contract** is a comment mirrored by `FakeKeepers`, so renaming `KeepersFeature.always_ask` passes test_names. This is covered by XC1 step 3.

---

## Node, API and CLI (ND)

### ND1. `RnfsNode` does too many jobs; split out the remote-pin queue and fetched targets (M–L, med)
- **Where:** [node.py:303-1639](rnfs/node.py#L303-L1639), 88 methods and 48 attributes. Besides wiring and the facades, it runs the DHT join, private-mode membership, the maintenance scheduler and deny-list application. It also owns two persisted stores:
  - a remote-pin queue with its own workers and quota checks (89-109, 446-451, 1296-1502; about 230 lines);
  - the fetched-targets store (75-86, 434-445, 872-922).
- **Why:** both stores share `self._lock` ([node.py:433](rnfs/node.py#L433)) with nothing else in common, and storage locks are taken under it. node.py is the most-changed file in the repo.
- **Change:** a `RemotePinQueue` class (state, workers, save and load, importer, `close()`) and a `FetchedTargets` store, each owning its lock and table, the way `PinNames` and `SharedFolder` already do. Once XC5's store registry exists, both plug into it.

### ND2. node_content.py and gateway_formats.py are free-function "mixins" with import cycles (S–M, low)
- **node_content.py:**
  - Its functions take the node and reach into its privates (`_note_fetched`, `_fetching`, `_check_file`).
  - It lazily imports from rnfs.node, and `rnfs.api.parse_bool`, while api.py imports it.
  - `add_options` ([node_content.py:222-255](rnfs/node_content.py#L222-L255)) is HTTP parameter parsing living in the node layer.
- **gateway_formats.py** lazily imports gateway.py for the same circular reason.
- **Scenario:** renaming `_fetching` breaks a module that a grep for `self._fetching` won't find.
- **Change:** fold the node-side functions into a `ContentOps` component the node owns, move `add_options` into api.py and the output-file helpers into fsutil, and pass gateway_formats what it needs.

### ND3. The facade doesn't check its own inputs (S, low)
- **Where:**
  - The `dir_rm` rule exists three times with three messages ([api.py:1000](rnfs/api.py#L1000), [embedded.py:189-190](rnfs/embedded.py#L189-L190), edit.py:69-70). edit.py's copy only runs after the whole DAG is fetched.
  - Name nicknames are resolved by both callers before `node.name_resolve`.
  - The "pin is a bool" check exists only in the API.
- **Change:** do these checks in `RnfsNode`'s facade methods, before any fetch, and drop the copies. A third caller then behaves the same.

### ND4. The API surface is written out in 6+ places and the client covers only part of it (M, low)
- **Route table:** `ApiHandler.ROUTES` ([api.py:611-640](rnfs/api.py#L611-L640)) dispatches by name through `getattr`, so a typo fails only when that route is requested. The routes are restated in api.py's docstring and design.md, which match today but aren't checked.
- **Stale handler docstrings:** they list old parameter sets (`api_add` :965, `api_add_stream` :972, `api_get` :981).
- **Partial client:** `ApiConnection` has no typed methods for dir set/rm, remote pin, gc, verify, providers, ping, name rm/export/import, export or import. So remote.py, cli.py (18 sites) and deny.py hand-write paths and parameters.
- **Add options are encoded three times:** in `cli.add_options`, in `apiclient.add_path` / `add_stream`, and in `node_content.add_options` on the server, each handling the JSON-vs-query differences its own way.
- **Scenario:** a new `/api/add` option needs edits in 5+ files, and the CLI's copy is the one most likely to drift.
- **Change:** complete `ApiConnection` and route cli.py, remote.py and the feature CLIs through it. Add a test that every ROUTES name resolves and that the docstring table equals ROUTES. Delete the parameter lists from per-handler docstrings.

### ND5. statedb steps can't be protected or written in Python (S, low)
See TS2. The fix belongs in [statedb.py](rnfs/statedb.py): a golden digest test of the released steps, and `(label, callable(tx))` steps. Optionally record the version that added each step.

### ND6. Smaller node, API and CLI items
- **Missing attributes in `offline()` nodes:** the offline/test node built by `cls.__new__` ([node.py:367-379](rnfs/node.py#L367-L379)) never sets `identity`, `destination` or `paths`. So `getattr(node, "identity", None)` appears in node.py and node_content.py, and the features use `reticulum is None` as an implicit "offline" flag. Have `offline()` set every attribute explicitly and expose `node.online`. (S, low)
- **Config defaults:**
  - The jobs default lives in jobs.py rather than an `effective_*` helper.
  - `"~/.rnfs/"` appears twice in config.py, and cli.py:664 hard-codes "default 8789".
  - Unknown override keys are checked in embedded.py rather than in `Config.load`.
  - Use constants and one check. (S, low)
- **`winacl` and RNS:** winacl.py says "No RNS dependency, so the CLI can use it", but it lazily imports rnfs.log, which imports RNS ([winacl.py:388](rnfs/winacl.py#L388), 422). So on Windows a loose `rnfs.toml` loads RNS inside the CLI and `rnfs.Client`. Return the warnings to the caller instead. (S, low)
- **api.py doubles as the HTTP framework** for the gateway (`LocalServer`, `TimedHandler`, `host_allowed`, …), and keeps test-only aliases (`api.loggable`, `cli.printable`, `cli.read_token`). Split out `rnfs/httpserver.py`. cli.py:8 calls `rnfs.apiclient` "the library companion apps use", which contradicts design.md:593: `rnfs.Client` is the public library. (S, low)
- **`cli.build_parser` and `cli.run_daemon`** are 138 and 123 lines. They are low priority, because they're flat, but `run_daemon` is the place to make startup steps individually guarded, like XC5's `stop()`. (S, low)

---

## Features (FT): `rnfs/features/`

### FT1. lxmf.py (2188 lines) is seven modules, and its constructor runs before it is fully built (M, low): fix the constructor order now
- **Seams:**
  - URI and field codec, which is the wire format (182-415);
  - `MessageLog` persistence, the 0.1.4 import and validators (417-698);
  - `_RoundRobin` (746-799);
  - `LxmfFeature` (804-1569);
  - API handlers (1574-1647);
  - CLI rendering (1649-1780);
  - `LxmfNetwork`, the RNS/LXMF glue that monkey-patches LXMRouter (1782-2188).
- **Constructor order:** `LxmfFeature.__init__` calls `_release_unrecorded()` at [lxmf.py:832](rnfs/features/lxmf.py#L832), which can go on to `_record_kept()` and the inbox. That happens *before* `_resend`, `_auto_queue`, `clock`, the rate-limit state, `_stop` and `_threads` are assigned (836-847). It works today only because that path doesn't touch them. A change there would raise AttributeError only on the rarely tested unclean-inbox path.
- **Change:** now, assign every attribute before calling any method (S). Later, make lxmf a package:
  - `codec.py`, the frozen wire format with its own known-answer tests (TS3);
  - `store.py`;
  - `feature.py`;
  - `network.py`;
  - `cli.py`;
  - an `__init__` that re-exports `FEATURE` and the names statedb's LEGACY strings use.

  Replace the positional tuples (`_resend`, `_outbound`, `_held`, whose source is re-sliced as `p[ID_LENGTH:2*ID_LENGTH]` eight times) with small dataclasses, so the tests no longer need `__new__`.

### FT2. keepers: `ask_keepers` is 138 lines of closures over shared mutable state (M, med)
- **Where:**
  - [keepers.py:702-839](rnfs/features/keepers.py#L702-L839) has six nested closures sharing `found`, `unknown`, `used`, `results` and `answered`. It also starts unjoined per-keeper threads (:816).
  - `name_subcommands` (908-914) uses argparse's private `_actions` and `_SubParsersAction`, and follow.py depends on it.
- **Change:** a small `_KeeperQuery` class (`next_batch`, `ask`, `settled`), and have the CLI's `command` helper hand out named subparsers. Moving the record, store and query logic into core is XC1.

### FT3. jobs: the state machine is implicit, and a kind is dispatched in five places (M, med)
- **Where:**
  - Transitions happen in `Job.begin_run`, `_load`, `_run`, `_finish` and `cancel`. `cancel` repeats `_finish`'s release, `ended.set`, cap and save sequence inline ([jobs.py:643-655](rnfs/features/jobs.py#L643-L655)).
  - `job.state` is guarded by two different locks.
  - Adding a kind touches `KINDS`, `check_job_params`, `_execute`, `api_start` and `print_result`.
  - About 275 lines of CLI rendering, and `StreamProgress` (used by `rnfs cat`), live in the same file.
- **Change:** one `_transition(job, state, …)` with an allowed-transitions table under one lock, a per-kind spec (validator, executor, printer), and the formatting moved to a CLI module.

### FT4. Features depend on each other through string lookups and duck typing (S–M, low)
- **Where:**
  - `getattr(self.node, "features", {}).get(...)` plus `hasattr(x, "lookup")` in lxmf (4×) and jobs.
  - follow requires keepers through a `KeyError`, while names treats keepers as optional.
  - `lxmf._recipient` duplicates `contacts.resolve_id` down to the error wording, and `lxmf._contact_target` is a variant of `contacts.expand_target`.
- **Scenario:** renaming `contacts.lookup` silently disables LXMF contact trust and sending by nickname, because `hasattr` returns False and nothing fails.
- **Change:** declare dependencies (`requires = ("contacts",)`, `optional = (...)`) and check them at load time. Call `contacts.resolve_id` instead of copying it.

### FT5. Smaller feature items
- **Copy-pasted schedulers:** follow and deny have the same `run_due` and `_loop` code, and duplicate `STALE_ANNOUNCES`, `FAR_FUTURE` and a magic `min(delay, 60.0)` ([follow.py:335-377](rnfs/features/follow.py#L335-L377), [deny.py:190-231](rnfs/features/deny.py#L190-L231)). Add a `_common.DueScheduler`. (S, low)
- **Name collisions are handled three ways:**
  - Duplicate feature API routes raise, but a core route silently shadows a feature route ([api.py:741-742](rnfs/api.py#L741-L742)).
  - A feature request path that collides with a core path is silently dropped, and two features with the same path silently overwrite each other ([server.py:704-706](rnfs/network/server.py#L704-L706), [node.py:299-300](rnfs/node.py#L299-L300)).

  Validate all three namespaces at load time with one error style. Add lifecycle tests with `DummyFeature` (`requests()`, save/close order, `start()` failure isolation versus `__init__` failure aborting the node). (S, low)
- **Uneven escaping:** daemon-supplied fields are printed raw in jobs.py (962, 1017-1028) and peers.py (216, 224-228), although test_feature_output's docstring says feature commands escape what the daemon sends. Extend that test to jobs, deny, lxmf and `format_stats`. (S, low)
- **Dead or stale code:**
  - `_RoundRobin.clear` is never called.
  - lxmf.py:1336 has a redundant import.
  - The lxmf docstring calls LXMF optional, but pyproject requires it.
  - `_api_sync` calls `_require_network()` directly, unlike every other handler.
  - jobs.py:313 does `getattr(config, "max_jobs", None)` on a field Config declares.
  - contacts uses a `@staticmethod _log` where every other module uses a module-level one.

  (S, low)

---

## Order of work

These phases fit around the 0.1.6 plan's.

1. **Safety nets first (S, low risk, about a week in total).** These make every later refactor safer, and none changes behaviour:
   - TS1: run every `RNFS_E2E` test in CI.
   - TS2 / ND5: the 0.1.5 state fixture and the released-schema digest. Do this before the plan's D1 adds a table.
   - TS3: wire known-answer vectors. Do this before the plan's A1/A2/A4 fetcher work.
   - TS4: report-only coverage.
   - TS10: enable BLE001, E402 and RUF100 with per-file ignores.
2. **Quick correctness-adjacent fixes (S):**
   - FT1: the constructor order.
   - XC4 step 3: log swallowed exceptions in lookup and fetcher.
   - NW4: the slot helper.
   - XC5: guarded `stop()` steps and the store registry.
   - XC6: drop the redundant wall-clock bound in test_publisher.
3. **Refactors that the 0.1.6 plan items touch.** Do each just before, or as part of, the plan item:

   | Before plan item | Do |
   |---|---|
   | D3 (storage walks under the lock) | CT1, CT4's `_newly_incomplete`, CT6's test hooks |
   | E1 (gateway name-resolve cache) | DH1 step 1 (`_refresh_name`) and the `_NameFloor` extraction |
   | A1, A2, A4 (fetch pipelining) | NW1's `_DagBudget`, typed events and the `progress()` thread fix |
   | B (block write durability) | XC8's `blockstore.matches` helper |
   | S5 (provider store) | DH2's `_insert` and consistency check |
4. **Shared helpers (M), one at a time, with no behaviour change:**
   - XC3: `rnfs/ids.py`.
   - XC2: `statedb.RowStore`, starting with the features.
   - XC7: the `tests/fakes/` package.
   - XC4 steps 1–2: error types and one status table.
   - CT5: msgpack headers.
   - NW5: shared size rules.
5. **Structural splits (L), after 0.1.6, each in its own release:**
   - ND1: remote pins and fetched targets out of `RnfsNode`.
   - FT1: lxmf as a package.
   - DH1: the rest of names.py.
   - NW2 / NW3: the `Response` lock and `_handle_blocks`.
   - XC1: the feature boundary. This is the one that decides how easily features can move to companion apps.

## Looked at and not recommended
- **Removing the 0.1.1 temp-file cleanup** ([names.py:1182-1197](rnfs/names.py#L1182-L1197)). It is old, but those files can hold a private key, and an install upgraded straight from 0.1.1 would keep them. It's a few lines, so keep it.
- **Runtime registration of pin-source prefixes:** see XC1. It would let a node started without a feature erase that feature's pins.
- **Merging `embedded._Backend` and `remote._Backend`.** They have 39 parallel methods each, but that is deliberate (in-process versus HTTP behind `interface.Rnfs`), and test_public runs the whole public interface over both.
- **Splitting `cli.build_parser`:** it is long but flat, a declarative table that is easy to read.
- **Restructuring `dag._decode`'s validation** while sharing msgpack tables (CT5): the plan already marks it as needing fuzzing first.

## What is already done well
- **Wire formats:** each has its encoder and strict decoder side by side, documented against its spec section. Decoders re-encode to reject non-canonical input. `protocol.response_refusal` checks a response's header only.
- **Injected dependencies:** clocks, networks, RNG and a `put` callback are injected through most of the core. routing, lookup, providers, cid, blockstore and chunker have no RNS dependency, so most behaviour is unit-tested without Reticulum.
- **Drift guards:**
  - `test_fake_contract.py` checks FakeNode against RnfsNode.
  - `test_rns_compat.py`'s `INTERNALS` lists every RNS and LXMF internal rnfs relies on.
  - `test_packaging.py` checks the locks, the README release links and that install.sh agrees with `contrib/rnfs.service`.
  - The 0.1.4 upgrade fixture is written by 0.1.4's own code.
- **statedb:** upgrades are one-way, a newer schema refuses to start, durability is chosen per transaction and migrations are crash-safe.
- **Policy classes:** the server's (`ResponsePools`, `IncomingLinks`, `Cooldowns`, `UploadPacer`, `TokenBucket`, `AirtimePolicy`) are pure and take injected clocks. Request dispatch is a handler table, not an if/elif chain.
- **Docs and comments:** they match the code. Comments explain *why*, including the attack each rule defends against.
- **Supply chain:** builds are hash-locked, actions are SHA-pinned, and releases are signed with provenance.
