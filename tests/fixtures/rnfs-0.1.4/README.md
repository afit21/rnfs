# rnfs 0.1.4 directory fixture

An rnfs directory exactly as rnfs **0.1.4** (commit `ffd23976e07ce41abc3e603d066472531a98b0cb`) writes it, with
every one of its 17 JSON state files populated. `tests/test_upgrade_014.py` copies it, starts the current code on
the copy and checks that the state database holds everything the files held.

It was written by `generate.py` (in this directory), running 0.1.4's own code: an offline `RnfsNode` with the
fakes of 0.1.4's own tests, its stores' public methods where they exist, and otherwise their in-memory state set as
their own code sets it, then saved by the store's own save. The generator checks that 0.1.4 loads the result
again without dropping or reconciling away any record.

## What is in it

- `state/`: the 17 state files. Notable cases: pins of every source kind (`user`, `folder`, `name:photos`,
  `remote:<hex>`, `follow:<hex>`, `lxmf:<hex>`), each matching the file that accounts for it (names.json,
  follow.json, the inbox message that auto-fetched it); cache records with Japanese and Greek path names, in
  least-recently-used order; a pin name holding a lone surrogate (the path of a file whose name isn't UTF-8);
  published, earlier and withdraw sections, with kept names' DHT keys (`key:<hex>`); provider records in the main
  store and in the newcomer pool, with departed and evicted publishers; a published name, an imported one never
  published, held records and a floor (one at the highest sequence, 2^64 - 1); kept names with signed records
  (own, followed, user) and one without; jobs done (with a result and warnings), failed and queued; inbox and
  outbox messages with non-ASCII and non-BMP text, outbox messages delivered, sent through a propagation node,
  and unfinished (outbound, sending, and a direct one that fell back to propagation).
- `names/photos`, `names/imported`: name keys. **Throwaway test keys, generated for this fixture: never use them
  for anything else.**
- `files/notes.txt`: the shared folder's one entry (shared_folder.json records its scan).
- `fixture_content.py`: the files 0.1.4 added (data only). There is no `blocks/`: the test adds the same files
  with the current code to make the blocks, so every pin is complete and provided, and checks the CIDs it gets
  are those 0.1.4 recorded.

Left out: the node identity (`storage/`, which a node creates), `lxmf/` router state, `api_token` and logs. No
`rnfs.toml`: the settings the test starts with are `SETTINGS` in generate.py (a remote pin identity, an LXMF
auto-fetch sender and `max_records = 40`, so the newcomer pool is reachable with a few records).

## Times

The wall clock was faked: records were written between 3 days before and an hour after T0 = 1792054800
(2026-10-15 09:00 UTC). The test runs the new code at T0 + 1 day, so no provider record or earlier publish
round has expired.

## Regenerating

From the repository root (the generator inserts the 0.1.4 tree first on `sys.path` and checks that is where
`rnfs` comes from):

    mkdir /tmp/rnfs-0.1.4 && git archive v0.1.4 | tar -x -C /tmp/rnfs-0.1.4
    python3 tests/fixtures/rnfs-0.1.4/generate.py /tmp/rnfs-0.1.4 /tmp/rnfs-fixture-out

then replace `state/`, `names/` and `files/` here with those in `/tmp/rnfs-fixture-out/rnfs-0.1.4` (the generator
checks 0.1.4 loads everything, with the blocks, before it drops them).
Keys, signatures, job IDs and message hashes are random, so every run writes different bytes; the test reads
its expectations from the files, so it needs no change.

The fixture is test data only: the sdist prunes `tests/` (MANIFEST.in) and the wheel packages only `rnfs*`.
