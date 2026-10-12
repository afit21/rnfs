# rnfs audit — 2026-10-12

Scope: every file in the repository at `4b8e932` (branch `dev`): `rnfs/**` (~17.5k lines), `install.sh`,
`packaging/`, `contrib/`, `.github/workflows/`, project metadata. No code was changed.

Baseline: `python3 -m unittest discover tests` — 886 tests, all pass (2 skipped).

**Verification key:** **R** = reproduced with a script; **T** = traced through the code (confirmed by reading);
**P** = plausible (traced, depends on timing/environment, not reproduced). Findings marked ✔ were re-checked
independently after the initial review. A second full verification pass re-checked every finding against
the code; it corrected line references, numbers and wording, moved the Flatpak finding from Medium to Low,
and moved three Low items to Info (one of them, about the pip cache, was wrong as a Low).

## Summary

No Critical issues. The core security controls hold up: block/CID verification, Ed25519 name-record
verification and rollback protection, strict msgpack/DAG decoding, Resource guards, API token handling
(256-bit, `compare_digest`, `O_EXCL|O_NOFOLLOW` 0600), Host-header checks, gateway CSP sandbox + nosniff,
sealed-file crypto (encrypt-then-MAC, MAC checked before unpadding, constant-time compare), LXMF signature
validation before allowlists, path-safe write-out, and pinned GitHub Actions.

The most important problems are one **availability bug on slow links** (the primary target), several
**data-loss paths** around state files and damaged blocks, a **privacy check that's bypassed** in archive import,
a **startup failure**, and **supply-chain gaps** in the build/install toolchain.

| Severity | Count |
|---|---|
| High | 1 |
| Medium | 11 |
| Low | 42 |
| Info | 24 |

---

## High

### H1. `/blocks` pacing refuses honest slow-link requesters until the fetcher drops the provider ✔ R
- **Where:** `rnfs/network/server.py:526-529` (refusal), `server.py:593-598` (`_max_pacing_wait`),
  `rnfs/config.py:145-151` (default cap = rate/2 on slow links), `rnfs/fetcher.py:781-784` (3 busy → fail).
- **Problem:** On a slow link with a rate estimate, the default upload cap is half the measured rate, so the
  pacer's interval between responses is ~2× a response's transfer time. The requester doesn't pace itself: it
  asks again as soon as a response arrives, so the server's pacing wait is ~one transfer time (up to ~30 s, half
  the default `response_target` of 60 s, when a response fills its budget; a single 64 KiB chunk's full transfer
  time below ~17.5 kbit/s). The server refuses outright when the wait exceeds `_max_pacing_wait` (half of RNS's
  request timeout: (rtt·6 + 11.25 s)/2 = 3·rtt + 5.6 s, i.e. 5.6–11.6 s for RTTs up to 2 s).
  The fetcher counts each refusal as BUSY, retries after 2 s, and fails the provider after `max_failures` (3).
  The spec says the next response "starts no sooner than" the interval; it doesn't say to refuse.
- **Impact:** 1200 bit/s LoRa, rtt 2 s: chunks 0 and 1 are served, then chunk 2 is refused 3× (wait 437 s vs.
  limit 11.6 s) and the provider is dropped. A single-provider fetch of more than ~2 responses fails over most
  slow links, including 50 kbit/s with rtt 0.5 s (waits 20.5/18/15.5 s vs. limit 7.1 s) and 90 kbit/s with rtt
  0.3 s. Only where the transfer time is close to the limit (e.g. 50 kbit/s with rtt 2 s) does it get through,
  at the cost of two wasted refusals per response.
- **Fix:** pace on the requester side (mirror the cap and wait before the next `/blocks`); or don't count
  pacing refusals toward `max_failures`; or return a "retry after N s" hint the fetcher honours.

---

## Medium

### M1. Archive import accepts junk bytes for blocks already stored, so cached downloads get pinned and provided ✔ R
- **Where:** `rnfs/content/storage.py:242-246` (`_put` returns early for a CID already indexed, *before*
  `blocks.put` verifies), reached via `Session.put` from `rnfs/content/archive.py:192-198`.
- **Problem:** `import_archive` adds every CID whose `session.put` didn't raise to `supplied`. For a CID the node
  already holds, the archive's bytes are never checked. The docstring and spec §8 promise that a root is never
  pinned on the strength of blocks already held locally, and that every block is verified.
- **Impact:** An attacker who gets the victim to import an archive (330 bytes in the repro) containing root R
  and `[cid_i, b"junk"]` for R's DAG turns the victim's unpinned cached download R into a permanent `user` pin.
  With `provide_strategy = "pinned"`, R is then provided on the DHT, revealing that the victim had downloaded it;
  under the default `roots` it was already provided while cached, but now it is never evicted and stays
  provided. On a node without R the same archive reports
  `bad=6`, so the result is also an oracle on the cache. Import counts are wrong too (`blocks` 6, `bad` 0).
- **Fix:** always `cid.verify(block)` (and size-check) before the "already stored" early return, or verify
  explicitly in `import_archive`.

### M2. Daemon can't start after a name key file is deleted (default `roots` strategy) ✔ R
- **Where:** `rnfs/names.py:698`, `rnfs/node.py:269-272`.
- **Problem:** `NameManager._load_state` handles a key file deleted by hand by calling `self._pins.unpin(...)`,
  which runs `publisher.refresh()` while `RnfsNode._setup` is still constructing `NameManager`, before
  `node._lock`/`node._fetched` exist. Under `roots`, `Publisher._compute` → `node._eligible_targets()` raises
  `AttributeError`, which `_load_state` reports as `RnfsError("malformed …names.json: 'RnfsNode' object has no
  attribute '_lock'")`.
- **Impact:** Publish a name, stop, `rm ~/.rnfs/names/<alias>`, restart: the node refuses to start, every time,
  with a misleading error. (With `provide_strategy = "pinned"` it starts, but there the early `refresh()` runs before
  the keepers feature registers its key source, so kept-name DHT keys are queued for withdrawal, which is
  exactly what the comment at `node.py:290` tries to prevent.)
- **Fix:** defer the releases (collect them in `_load_state` and apply them at the end of `_setup`, after the
  features exist). Creating `_lock`/`_fetched` before `NameManager` fixes the crash but not the early
  withdrawal. `NameManager` can't move after the features, which use it at construction.

### M3. Queued LXMF auto-fetches are lost on restart, stay "queued" forever and block inbox eviction ✔ R
- **Where:** `rnfs/features/lxmf.py:944` (records saved as `{"state": "queued"}`), `:603` (`_holds_pins` treats
  "queued" as holding pins), `:680` / `:693-715` (`_auto_queue` is memory-only; `start()` never refills it).
- **Problem:** The auto-fetch queue isn't persisted or rebuilt from the inbox on start.
- **Impact:** A trusted sender's message arrives and the daemon stops before its fetch runs. After restart the
  link is never fetched, `inbox` shows "auto-fetch queued" forever, and the record is never dropped to make
  room for other senders' messages (the fetch running at shutdown is left `queued` too). Each restart can
  strand up to `AUTO_QUEUE_LIMIT`+1 (65) records; once enough accumulate (500 records or 4 MiB), every new
  message from an untrusted sender is refused ("inbox is full of messages holding auto-fetched pins").
- **Fix:** at `start()`, re-queue every `queued` (entry, index) (respecting `AUTO_QUEUE_LIMIT`, marking the
  rest `skipped`), or mark them `interrupted` on load.

### M4. Per-key publisher cap lets early Sybil identities lock every later provider out of a key ✔ R
- **Where:** `rnfs/dht/providers.py:28` (`MAX_PUBLISHERS_PER_KEY = 1000`), `:162-166`.
- **Problem:** Once a key has 1000 live publishers, every new publisher is refused. The spec's protection
  against early squatters (the random second half of each `/find` reply) only works if newcomers get stored.
- **Impact:** An attacker `/provide`s a popular CID, or a name's keeper key, from 1000 cheap identities to the K
  closest servers and keeps republishing. Honest providers that start later are refused. As the original
  honest providers drop off, `/find` returns only attackers and the content becomes unfindable.
- **Fix:** at the cap, evict a publisher (e.g. the newest-first-seen, or a random non-senior one) instead of
  refusing; or keep a random reservoir for the newcomer half.

### M5. Nodes providing more than ~1000 keys are partly unfindable, and their stored records are never withdrawn ✔ R
- **Where:** `rnfs/dht/providers.py:26-27, 158-161`; `rnfs/node.py:168-172`; `rnfs/network/client.py:114-120`;
  `rnfs/publisher.py:316-322, 341`.
- **Problem:** `max_per_publisher` defaults to `max_records // 100` (1000). The server answers `False` unless
  every key in a batch is recorded; `client.provide` stops at the first refused batch; the publisher then
  records the node for *none* of the CIDs, including those that were stored. The node's own local
  self-records are capped the same way.
- **Impact:** A node pinning/caching 1500 CIDs on a small mesh: ~500 keys are never stored anywhere, and the
  ~1000 that were are never withdrawn after unpin (the node isn't in `_published[cid]`), staying advertised as
  stale for up to 7 days.
- **Fix:** have `/provide` report per-key success (or record the accepted prefix); exempt own-ID records from
  the cap; make the cap a documented setting.

### M6. Publishing an imported name key ignores keepers, so the sequence can go backwards ✔ R
- **Where:** `rnfs/names.py:328-338` (also `:427-428`).
- **Problem:** On the first publish of an imported key, `publish()` only does a path request and
  `_absorb_recalled`; it never queries keepers or offers the kept record. Spec §9 step 3 requires a sequence
  above "the highest it can resolve", which includes keepers.
- **Impact:** A key moved to a new machine after its old announces have expired, while keepers still hold
  sequence N, publishes sequence 1. Keepers and resolvers that persisted N refuse it, and since transport nodes
  rate-limit announces (about one an hour sustained) and no command sets the sequence, recovery needs ~N
  publishes. Losing the whole state directory (`names.json` and `keepers.json`) with the key kept behaves the
  same, without even the path request. Losing only `names.json` is covered, because the node's own kept record
  is offered at startup (`keepers.py:386-389`).
- **Fix:** for an imported key (or missing state), run `_offer_kept` and `keepers.query(name)` before choosing
  the sequence.

### M7. A damaged interior DAG node makes GC delete the pinned blocks below it (R for the effects; P for the trigger)
- **Where:** `rnfs/content/blockstore.py:55-81` (temp+rename, no fsync), `storage.py:149` (startup scan
  indexes files unverified), `storage.py:382-399` (`_children` treats an undecodable node as childless),
  `blockstore.py:40-46` (`get` doesn't verify); `rnfs/fetcher.py:256-263` (marks root invalid).
- **Problem:** After a power loss a just-renamed block can be zero-length under its CID name. That file is
  trusted: `has()` is true so it's never refetched; reads raise `InvalidDag`, which persistently marks the root
  invalid (nothing ever clears it; `rnfs verify` deletes the empty block but not the mark); and `_reach` stops
  there, so `gc()`/eviction delete the pinned blocks below it, possibly the only copy of content the user added.
- **Repro:** a simulated truncated interior node made `gc()` delete 44 of 303 blocks of a pinned DAG.
- **Fix:** fsync block and directory around the rename; treat an undecodable stored node as "unknown
  children" and skip GC below it (or verify before collecting); verify length/hash on read; never mark a root
  invalid because of a local read error.

### M8. `read_json` treats any `OSError` as damage; losing `pins.json` loses every pin and GC then deletes pinned content ✔ R
- **Where:** `rnfs/content/storage.py:95-112` (read), `:79-92` (write, no fsync), `:167`.
- **Problem:** Any `OSError` opening `pins.json`, including transient ones (EACCES, EMFILE, EIO), moves it to
  `.damaged` and the node starts with no pins. The same happens to a file left empty by a crash (no fsync;
  ext4/btrfs mostly avoid this on rename-over, XFS does not). A later damage event overwrites the earlier
  `.damaged`.
- **Repro:** with `chmod 000 pins.json`, the file was moved aside, `is_pinned` returned False, and `gc`
  deleted the formerly pinned block.
- **Fix:** only treat `ValueError` (bad JSON) as damage and re-raise other `OSError`s; fsync in `write_json`;
  don't overwrite an existing `.damaged`.

### M9. The build backend (setuptools) is fetched unpinned and unhashed, despite claims to the contrary ✔ T
- **Where:** `install.sh:307,315`; `packaging/build-deb.sh:69`; `.github/workflows/release.yml:43,173`;
  `tests.yml:26,49,83`; `pyproject.toml:2` (`setuptools>=64`).
- **Problem:** `pip wheel` / `pip install .` / `python -m build` use PEP 517 build isolation, which pulls the
  newest `setuptools` from PyPI. `--require-hashes` doesn't cover the isolated build environment. So
  `packaging/build-deb.sh:78` ("pip's hash-checking mode then installs nothing unpinned") and the hash-locking
  claims in `install.sh:22-24,307` and `CHANGELOG.md:141-145` overstate it: they cover runtime dependencies
  only, not the build backend.
- **Impact:** A malicious or broken setuptools release runs as root during `curl … | sudo sh -s -- --system`,
  and shapes the published wheel and .deb.
- **Fix:** lock setuptools with hashes, install it with `--require-hashes`, build with
  `--no-build-isolation`; add an upper bound.

### M10. The release toolchain is unhashed (T)
- **Where:** `release.yml:42,174`; `tests.yml:46,84`.
- **Problem:** `pip install build twine` is unpinned; `pyinstaller==6.22.3` has no hash and its dependencies
  (pyinstaller-hooks-contrib, altgraph, pefile, …) are unpinned.
- **Impact:** These tools produce the published wheel, sdist and Windows exe, so a compromised or changed
  version alters release artifacts. Builds aren't reproducible.
- **Fix:** a hashed `packaging/build-tools.lock` installed with `--require-hashes`.

### M11. `build-deb.sh` silently skips missing cffi backends, producing a .deb that crashes on import ✔ R
- **Where:** `packaging/build-deb.sh:92-106`.
- **Problem:** Per-Python `pip download` runs with `2>/dev/null` inside an `if`; any network/PyPI error just
  prints "skipped" and the build succeeds. Without `_cffi_backend`, importing cryptography's ciphers raises
  `ModuleNotFoundError`. RNS has already chosen the pyca provider (its fallback only guards `import
  cryptography`), so `import RNS` fails and the daemon dies; client commands, which don't import RNS, still
  work. Hosts with `python3-cffi-backend` installed mask this, because the launcher's `-I` still sees system
  dist-packages.
- **Impact:** A transient error while building arm64 (never smoke-tested in CI) ships a deb that is dead on
  Ubuntu 24.04 / Python 3.12. Python 3.13/3.14 aren't tested on either architecture.
- **Fix:** make a failed download fatal (skip only on explicit "no matching distribution"); assert every
  expected `_cffi_backend.cpython-3XX-*.so` exists after the loop.

---

## Low

### Core daemon, network and DHT

- **L1. Core settings aren't type-checked; security settings can fail open** ✔ R:
  `rnfs/config.py:153-183`. Feature settings are validated, but `gateway_scripts = "false"` is truthy, so the
  gateway serves the scripts-allowed CSP. `private = "no"` turns private mode **on**. `members = "abcd…"`
  becomes a set of single characters. `max_storage = "10GiB"` makes every `add` and every unpinned fetch fail
  with a TypeError (500), and maintenance logs it every 60 s; `gateway_fetches = "2"` makes every limited
  gateway request that needs the network return 500. Negative `gateway_fetches` / `max_output_entries` are
  accepted. `api_port = 99999` raises OverflowError, which isn't caught, so the daemon crashes with a traceback
  without stopping the node. A wrongly typed `max_blocks_responses` (e.g. `"9"`) raises TypeError inside
  `validate()` itself, which `run_daemon`/`client()` don't catch either. `unmeasured_bitrate = 0` → ZeroDivisionError in
  `rates.response_deadline`; `max_incoming_links <= 0` refuses every link. **Fix:** `type() is` checks plus
  range checks for every field in `validate()`.
- **L2. Pacing bound uses an RTT the requester can inflate** (R): `server.py:593-598, 538-539`.
  RNS sets `link.rtt = max(measured, initiator's claim)`, so a peer claiming a huge RTT makes `_max_pacing_wait`
  unbounded; the handler then `time.sleep()`s (819 s in one repro, 434 s in another), holding a slow-pool slot
  until it is displaced (after `deadline_slack`) or the link closes, and holding the RNS handler thread for the
  whole sleep, even after the link is torn down. The spec avoids the RTT for rates because the remote end can
  inflate it (§2); the same reasoning applies here. **Fix:** bound by a
  constant or by the locally measured RTT; skip the sleep once displaced.
- **L3. In private mode, outsiders can evict members' idle incoming links** (R):
  `server.py:217-223, 376-392`. Eviction ignores membership. **Fix:** evict unidentified or non-member links
  first; tear down links that don't identify as a member within a grace period.
- **L4. Link target isn't checked against the requested node ID** (P): `rnfs/network/links.py:253-258`.
  `_open` recalls the identity for `node_id` and links to its `rnfs.dht` destination without checking that the
  resulting hash equals `node_id`. One identity can answer under many announced destination hashes (ground
  near target keys), fill routing buckets via `heard()`, and dodge bans on its real ID. **Fix:** require
  `destination.hash == node_id`.
- **L5. Idle sweeper can close a shared link just after `LinkPool.get()` hands it out** (P):
  `links.py:195-198, 335-350`. The result counts as a routing failure or drops a provider. **Fix:** touch/mark
  the peer in `get()`.
- **L6. Identity creation race** (P): `rnfs/network/identity.py:15-56`. Check-then-`os.replace` lets two
  instances starting on a fresh directory overwrite each other's key; no directory fsync. **Fix:**
  `os.link(tmp, path)` (fails if it exists), then reload; fsync the directory.
- **L7. Remote `/pin` spawns unbounded fetch threads and can exceed the quota** (T): `rnfs/node.py:810-852`.
  Each accepted `/pin` for a new CID starts a raw thread (bypassing `max_jobs`). In-flight pins reserve only
  their declared content size, but each may fetch up to `quota − used` DAG bytes, so K concurrent pins of DAGs
  with many empty files can store K × quota. Only identities in `remote_pin_identities` can do this.
  **Fix:** route through the jobs queue or cap in-flight pins per source; reserve `max(content, max_dag_bytes)`.
- **L8. `_prune_fetched` can drop a just-recorded download from the provide set** (P):
  `node.py:491-498`. `cached_targets()` is snapshotted outside `self._lock`, so a concurrent
  `record_fetch` + `_note_fetched` can be lost and the download is never provided under `roots`.
- **L9. Publish round stopped mid-way marks unsent CIDs as published** ✔ R:
  `publisher.py:310-341`. On shutdown after lookups, CIDs are saved as published with `[]` and aren't
  republished for 24 h (3 days on slow nodes).
- **L10. Stale provider records are never withdrawn from earlier rounds' nodes** (T): `publisher.py:341`.
  Each round replaces the node list, so nodes that dropped out of the closest set keep a stale record for up to
  7 days (this matches the spec's literal wording).
- **L11. Wall-clock times in the future (persisted, and slow-node pacing's in-memory `_next_lookup`) are never
  clamped** (R for ProviderStore): `providers.py:173,205,261,267,344`; `publisher.py:250,267,270,295`;
  `names.py:426`. After a clock that ran ahead is corrected, provider records outlive their TTL, nothing is
  republished, slow-node pacing stalls, and the gateway treats a held name as always fresh.
- **L12. Cancelled or failed fetches leave `/blocks` responses running** (P): `fetcher.py:256-265, 654-668`.
  Up to ~1 MiB keeps using LoRa airtime, and the next fetch from that provider waits behind it (one `/blocks`
  per link).
- **L13. `MessageLog.save` and `ProviderStore.save` lack a save lock** (P; forced interleaving reproduced): `lxmf.py:560-572`,
  `providers.py:298-316`. An older snapshot can be written last with `_dirty` already cleared (shutdown
  overlaps the maintenance thread). `KeeperStore` already uses `_save_lock` for this.

### Content and storage

- **L14. State JSON that parses but has the wrong shape crashes startup** ✔ R:
  `storage.py:168,178`; `shared_folder.py:91`; `publisher.py:157-158`; `jobs.py:203-206,291` (a 400-digit
  integer passes `_number` and then `float()` raises OverflowError, uncaught). **Fix:** `isinstance(…, dict)`
  checks falling back to the damaged-file path; catch `OverflowError`.
- **L15. Archive import never checks the disk reserve** (T): `archive.py:189-199`. An untrusted archive can fill
  the filesystem; `check_disk` (used by fetches) is never called.
- **L16. `write_out` aborts the whole `get` on one entry's `OSError`** (P): `walker.py:457-466`. A name allowed
  on ext4 but rejected by vfat/exFAT (e.g. `:`, `?`, `"`) or ecryptfs (names over ~143 bytes) makes the whole
  directory impossible to `get` there. Spec §5 says `get` "reports names it cannot represent", and
  `write_out`'s docstring promises such entries are skipped and reported.
- **L17. `iter_dag` keeps per-block state for a whole export** (R): `walker.py:251-280`. `seen_expects`,
  `seen_cids` and `raw_lengths` hold an entry for every block (raw leaves included), and `nodes` holds every
  DAG node's bytes. Measured ~480 bytes per 64 KiB chunk, so roughly 8 GB for a 1 TiB file (index-node bytes
  add ~0.75 GB); DAGs with tiny chunks are worse.
- **L18. Most state files and blocks are never fsynced** (P): `storage.write_json` (pins, cache, invalid
  roots, publisher, shared folder, jobs, keepers, follow, contacts), `lxmf._write_json` and `BlockStore.put`;
  only `names._write_atomic`, `ProviderStore.save` and identity creation fsync.
  Cross-cutting cause of M7/M8. After power loss, an emptied inbox file leaves `lxmf:` pins nothing will
  release (permanent quota use), and an emptied keepers file loses the rollback floor.

### Local API, gateway and CLI

- **L19. rnfs dir that can't be made private only logs a warning** (T): `node.py:61-79`. If the dir is owned
  by another user (e.g. a pre-created `--dir /tmp/rnfs`), the daemon writes its identity, name keys and token
  into a directory the attacker can rename entries in (e.g. plant an `rnfs.toml` that adds their
  `remote_pin_identities`, or swap in their own identity before the next start).
  **Fix:** refuse to start unless the root is owned by the euid and 0700.
- **L20. CLI trusts whatever listens on the API port** (P): `cli.py:109-146`. If another local user binds
  127.0.0.1:8788 while the daemon is down after a crash or kill (a clean stop deletes the token file, and the
  CLI refuses to send without one), `rnfs name import` sends the private key to them. Separately, many success
  paths print response fields without `printable()` (e.g. `run_add`, `run_dir_set`, `run_dir_rm`, `run_pin_add`,
  `run_providers`, `run_name_create`/`import`/`publish`/`resolve`, `run_import`, `ls`'s kind/CID columns, and
  several feature commands), allowing terminal-escape injection. **Fix:** have the daemon prove it
  knows the token (e.g. an HMAC reply header); `printable()` every printed field.
- **L21. Malformed `Range` returns 400 with the parser message instead of being ignored** (R):
  `gateway.py:132-144`. `str.isdigit()` accepts `¹²³` but `int()` doesn't; 5000-digit values hit Python's
  int-string limit. **Fix:** `re.fullmatch(r"[0-9]{1,18}")`.
- **L22. Aborted `?format=archive` downloads log at ERROR** (R): `gateway_formats.py:83-88` /
  `gateway.py:368-372`. A web page can trigger this repeatedly. **Fix:** treat `OSError` after headers as a
  client disconnect.
- **L23. `client_gone` uses `select.select`, which fails for fd ≥ 1024** (R for the mechanism):
  `gateway_formats.py:91-102`. The `ValueError` is read as "client gone", so once the process has ≥1024 fds
  open (possible only where the soft `RLIMIT_NOFILE` exceeds 1024, e.g. containers; systemd's default is 1024)
  every archive request that fetches is cancelled at its first check.
  **Fix:** `poll`/`selectors` or `recv(MSG_PEEK|MSG_DONTWAIT)`.

### Features

- **L24. Follow stops checking a name after any non-`RnfsError` exception** ✔ R:
  `follow.py:223, 255-256`. The due time is set to `FAR_FUTURE` before `check()`, and an `OSError` (e.g. disk
  full while saving) skips the reschedule. Both versions stay pinned (the old pin is only released at restart
  or unfollow) and periodic checks stop until a new record for the name is heard (`_changed`) or it is
  followed again.
- **L25. Forged-source LXMF messages fill the hold queue even when `lxmf_accept_from` is set** (R):
  `lxmf.py:1719-1742`. Messages from unknown sources are held (up to 32) with path requests sent, before
  allowlists are considered. An attacker can keep the queue full so that messages from allowlisted-but-not-yet-known
  senders are dropped after LXMF has marked them delivered. **Fix:** when `accept_from` is set, don't hold
  sources outside `accept_from` ∪ `auto_fetch_from` (the same test as `received()` at `:916`).
- **L26. Reading links from message text strips trailing characters that belong to the link** (R):
  `lxmf.py:93-94, 306`. A trailing `.` of a link rnfs writes (e.g. a last name `file.` or `...`) is stripped,
  so the link resolves to something else (`file`, or the parent directory). In hand-written links from other
  clients, unencoded trailing `!`, `)`, `,`, `;`, `:` are stripped too, and a `'` ends the link wherever it
  appears. This applies when the link field is missing or invalid. **Fix:** don't strip a link that fills its
  whole line (as `message_text` writes it), or try the unstripped form first when the text ends there. The spec
  requires `.` to be written unencoded, so `format_uri` can't encode it.
- **L27. An auto-fetch in progress can't be cancelled** (T): `lxmf.py:1102-1111, 1132-1166, 1178-1200`.
  `inbox rm`/`stop()` don't interrupt it, and it blocks every other auto-fetch (they run one at a time).
  **Fix:** run `auto_fetch` under `fetcher.observing()` with an observer whose `cancelled()` is
  `self._stop.is_set()` or the inbox entry being gone.
- **L28. A keeper stores records below the node's known sequence** (P): `keepers.py:323-330, 608-619`. A
  malicious keeper can supply an old, validly signed record that this node then re-serves until a higher
  announce arrives (local resolution is unaffected).
- **L29. `inbox get` on several links with the same last name fails partway** (T): `lxmf.py:1026-1075`. The
  first job starts, then the second fails on a duplicate output, and the CLI never prints the first job ID.

### Packaging, install and CI

- **L30. A manual dispatch on a tag publishes and clobbers the release** ✔ T: `release.yml:8,192,216`. The
  release job's `if:` is only `github.ref_type == 'tag'`, contrary to the "without releasing" comment, and it
  uploads with `--clobber`, replacing assets and SHA256SUMS users may already have verified. **Fix:** also
  require `github.event_name == 'push'`.
- **L31. Release artifacts are unsigned and have no provenance**: `release.yml:203-207`. SHA256SUMS comes over
  the same channel as the files. **Fix:** `actions/attest-build-provenance`, or sign SHA256SUMS.
- **L32. `install.sh` system mode doesn't set `umask 022`** (P): on hosts with umask 077, `/opt/rnfs/venv` is
  0700 and `/usr/local/bin/rnfs` fails for non-root users (the root-run `--version` check passes), and
  `/etc/systemd/user/rnfs.service` is written 0600, so users' systemd can't read it.
- **L33. `install.sh` overwrites or deletes a systemd unit it didn't create**: `install.sh:335-336, 363-367`,
  unlike the `ours()` check used for the launcher.
- **L34. `install.sh` doesn't check that the lock covers rnfs's dependencies** (rnfs is installed with
  `--no-deps`): run `pip check` after install.
- **L35. The Flatpak sandbox is effectively disabled** (T): `packaging/flatpak/io.github.afit21.rnfs.yml:19,23`
  grant `--filesystem=home` (read-write home) and `--device=all`. If the network daemon, which parses
  untrusted remote data, were exploited, it could write `~/.bashrc`, `~/.config/autostart` or
  `~/.local/share/flatpak/overrides/` and escape persistently. This is deliberate and documented (manifest
  comments at `:17-22`; README.md:178-183 shows `flatpak override --nodevice=all`), and the pip, install.sh and
  .deb installs have no sandbox at all, so it is a defence-in-depth gap. **Fix:** `--filesystem=~/.rnfs:create`,
  `~/.reticulum:create`, `xdg-download`; let users widen it with `flatpak override`; make `--device=all`
  opt-in (this changes documented behaviour: `add`/`get` outside those paths would need `flatpak override`).
- **L36. System-mode update/uninstall ignore users' running services**: no restart notice; uninstall leaves
  dangling `default.target.wants` symlinks.
- **L37. Lock includes sdist hashes**: on platforms without wheels (musl, 32-bit Pi), cffi/cryptography build
  from source with unpinned build deps (see M9). **Fix:** `--only-binary=:all:`.
- **L38. `build-deb.sh`**: `Depends: python3 (>= 3.11)` has no upper bound even though backends are bundled
  only for 3.11–3.14 (`:179`); `cd` without `CDPATH=''` (`:24`); the `file://$WHEEL` URL isn't percent-encoded
  (`:80`).
- **L39. Windows task script** (P): `contrib/install-windows-task.ps1:21,27,43,49`. `Stop-ScheduledTask`
  stops powershell.exe, not the rnfs child (uninstall leaves the daemon running); `-RestartCount` doesn't restart on crash; `*>>` in PS 5.1 writes UTF-16 and wraps
  stderr; `Test-Path` without `-LiteralPath` breaks on `[`/`]`.
- **L40. systemd unit has no hardening** (`NoNewPrivileges`, `ProtectSystem=strict` + `ReadWritePaths`,
  `PrivateTmp`, `RestrictAddressFamilies`, …).
- **L41. Version/release inconsistency** ✔ T: tag `v0.1.0` predates `b8e5ea8` (the BoundedHTTPServer fix) but
  `main` still reports `0.1.0`; CHANGELOG says "0.1.0 (unreleased)"; local-only tags `v1.0` and `test-0.1.0`
  exist (pushing `v1.0` would trigger a failing release); README badge says **Python 3.8+** at `4b8e932`
  (origin/main 012118c fixed the badge image, but its alt text still says 3.8+), but
  `requires-python = ">=3.11"`.
- **L42. README's Windows PATH snippet** (`README.md:119`) adds a duplicate entry on every run and flattens
  `REG_EXPAND_SZ` entries.

---

## Info

- `server.py:705-710`: `_refund` refunds from the last seen progress when `get_progress()` raises; the spec
  says such responses keep their charge (design.md reads "can't be read" as untracked only, which the code
  does honour).
- `server.py:229-231, 563`: handler threads can re-create `_LinkState`/pacer entries for closed links; pacer
  entries leak. `_find_link` (`:420-423`) can likewise `_track` a closed link still in `destination.links`,
  re-adding an entry whose closed callback never fires.
- `gateway.py:368-372`: 500 bodies are `str(e)` with ACAO for gateway referers, so untrusted gateway JS can read
  messages like `[Errno 28] … '/home/<user>/.rnfs/blocks/…'` (username/path disclosure). **Fix:** generic 500 body.
- `gateway.py`: an HTTP/0.9 request line gets a raw body with no CSP/nosniff headers (not sendable by browsers);
  likewise any malformed request line, whose http.server 400 is sent without headers.
- `gateway.py:388-391`: no `Sec-Fetch-Site` (Safari < 16.4) is treated as the user's own request, so
  `gateway_fetches` doesn't apply.
- `gateway.py:285-288`: ACAO is also sent on HEAD (deliberately mirroring GET, as RFC 9110 recommends); spec
  §11 mentions only `GET`, so the spec could say so explicitly.
- `fsutil.rename_new` (`fsutil.py:42-50`) is check-then-rename (not atomic): a file created at the output path
  in that window is silently replaced (POSIX `rename` overwrites); only a racing directory raises. Consider
  `renameat2(RENAME_NOREPLACE)` (link+unlink works only for file outputs).
- `safe_default_name` accepts names starting with `-` (argument injection risk in later shell use).
- `api.py:198-213`: chunk sizes accept `0x`, `+`, `_`; unlimited trailer lines (token required).
- `walker.py:443,463,485-487`: write-out creates directories and sets the execute bit by path (`os.mkdir`,
  then `os.stat`/`os.chmod`, which follow symlinks): a local TOCTOU only for someone who can already write in
  the output's parent directory (where the `.partial` directory is created). `builder.py` is fine: it opens
  entries fd-relative with `O_NOFOLLOW` and identity checks.
- `lxmf.py:1764-1791`: messages arriving while `_retry_held` runs can be dropped silently (R, stubbed network).
  It empties `_held` (`:1766-1768`) before checking each held message outside the lock, so `_delivered` sees an
  empty queue and accepts up to 32 more (logging "holding", spending path-request tokens, missing duplicates of
  held messages); then the kept ones are put back in front and the list is cut to `HOLD_LIMIT` (`:1789-1791`)
  with no log line. The number dropped matches the spec's "newest are dropped" rule. **Fix:** retry in place,
  or count `keep` against the limit in `_delivered`.
- `pin_names.py:43-48`: `record_if_unknown` checks and writes without holding the lock throughout; a concurrent
  `record` can be overwritten (affects only the origin label `pin ls` shows).
- `lxmf.py:91`: a query value containing `=` makes the whole link invalid, although the spec's grammar allows
  `=` in a value and readers "MUST ignore other parameters".
- `lxmf.py:922, 305`: text is cut to 4000 chars before link scanning, so `MAX_SCAN_CHARS` (16,384 chars) is never
  used; links after 4000 chars are lost when the field is absent.
- `lxmf.py:881-885`: an outbox record evicted before completion leaks its `_resend` entry and skips fallback.
- `publisher.py:288-292`: non-slow nodes run all due lookups before any `/provide`; withdrawals queued
  meanwhile wait until the round ends.
- `fetcher.py:563`: a probe still in flight when `_download` returns can deliver its result into a later
  `_download` (shared `_events`, per-call `in_flight`) and pop that call's entry for the same node.
- `routing.py`: seniority isn't persisted, so for a day after every restart provider ranking ignores it.
- Workflows: `actions/checkout` keeps `persist-credentials: true` while third-party build code runs (the
  token is read-only in those jobs).
- `install.sh`'s default download/unpack path has no CI coverage (`RNFS_SOURCE` is always set);
  `curl | sh` installs moving `main`.
- CHANGELOG claims tests use the hash-locked deps; `tests.yml` does `pip install .` unpinned.
- `.deb`: missing `Copyright:` field and `DEBIAN/md5sums`, native version, placeholder maintainer.
- launchd plist/Windows task: `daemon.log` is never rotated. launchd plist and systemd units: a missing binary
  restarts every 10 s forever (`RestartSec=10` never hits systemd's default start limit).
- `install.sh` system mode under `sudo -E`: pip disables its cache (it won't write a non-root-owned path as
  root) and prints a warning; `--no-cache-dir` avoids it.

## Checked and found sound

Block verification against CIDs (with bans on mismatch; but see M1 and M7); strict DAG/msgpack decoding with re-encode check;
index-node bounds, depth limit, size checks; keeper/name record verification (name hash, destination hash,
canonical record, Ed25519 over exactly the RNS-signed fields) and sequence floors; domain-separated DHT keys;
`/provide` records only the identified requester; bounded lookups; Resource advertisement guards; per-link and
pool limits with exactly-once refunds; strict request parsing (bool-as-int, str/bytes, negatives); API token
generation, storage and comparison; Host checks; no CORS on the API; gateway CSP sandbox + nosniff on every
response including errors; `Content-Disposition` encoding and listing escaping; exclusive-create output files;
sealed-file construction; safe write-out names (`..`, separators, Windows reserved names/ADS, case and
normalization collisions); shared-folder walk (no symlink following, no FIFO blocking); LXMF signature
validation before allowlists, strict `rnfs:` URI parsing (300k-case fuzz); capped inbox/outbox/jobs/contacts;
all GitHub Actions pinned to SHAs that match their tags; no `pull_request_target` or `${{ }}` in `run:`.
