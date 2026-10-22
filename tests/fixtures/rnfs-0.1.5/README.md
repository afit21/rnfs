# rnfs 0.1.5 directory fixture

An rnfs directory exactly as rnfs **0.1.5** (commit `5bd3075`, tag `v0.1.5`) leaves it: its state database,
`state/rnfs.db`, with rows in every one of its 27 tables, and the 17 JSON files 0.1.5 imported from 0.1.4, kept as
`<name>.migrated`. `tests/test_upgrade_015.py` copies it, starts the current code on the copy and checks that
every row is still there.

It was written by `generate.py` (in this directory), running 0.1.5's own code:

1. 0.1.5 starts, offline, on a copy of the [rnfs-0.1.4 fixture](../rnfs-0.1.4/README.md), with that fixture's
   blocks, clock and settings, as 0.1.5's own `tests/test_upgrade_014.py` does. That imports the JSON files.
2. Through 0.1.5's own objects it then adds what only 0.1.5 has: two local deny-list entries (a CID and a file
   hash), a deny list followed through the `deny_lists` setting (accepted, its blocks pinned `deny`), and a
   file's content hash remembered (`deny_hashes`), from reading a file of several blocks while file hashes are
   denied.
3. The node stops, which saves every store and checkpoints the WAL, and the database is `VACUUM`ed.
4. 0.1.5 starts again on a copy, at the same time, and must change no row.

`expected.json` holds every table's rows as plain `sqlite3` reads them (a BLOB is `{"blob": "<hex>"}`), the
schema version (`user_version`, 16), the time the test runs at (`now`) and the settings it starts with.

## What is in it

- `state/rnfs.db`: everything the rnfs-0.1.4 fixture holds (see its README), plus the 0.1.5 rows above. The
  deny-list entries match nothing else in the fixture, so the upgrade test sees every pin and record as 0.1.5
  kept it.
- `state/*.json.migrated`: the 0.1.4 fixture's state files, unchanged.
- `names/photos`, `names/imported`: name keys. **Throwaway test keys, generated for the 0.1.4 fixture: never use
  them for anything else.**
- `files/notes.txt`: the shared folder's one entry.
- `fixture_content.py`: the files 0.1.5 added on top of the 0.1.4 fixture's (data only). There is no `blocks/`:
  the test adds both fixtures' files with the current code to make the blocks, so every pin is complete, and
  writes them in the flat layout 0.1.6 and earlier used (`blocks/<cid>`).

Left out, as in the 0.1.4 fixture: the node identity (`storage/`), `lxmf/` router state, `api_token` and logs.
No `rnfs.toml`: the settings are in `expected.json`.

## Times

The fake wall clock is the 0.1.4 fixture's T0 plus a day, `now` in `expected.json` (1792141200), so no provider
record or publish round has expired. Rows 0.1.5 timed with the real clock (the deny-list entries' `added`, the
deny list's `accepted`) hold the time the generator ran.

## Regenerating

From the repository root:

    mkdir /tmp/rnfs-0.1.5 && git archive v0.1.5 | tar -x -C /tmp/rnfs-0.1.5
    python3 tests/fixtures/rnfs-0.1.5/generate.py /tmp/rnfs-0.1.5 /tmp/rnfs-fixture-out

then replace `state/`, `names/`, `files/` and `expected.json` here with those in
`/tmp/rnfs-fixture-out/rnfs-0.1.5`. Keys, signatures, job IDs and message hashes are random, so every run writes
different bytes; the test reads its expectations from `expected.json`, so it needs no change.

The fixture is test data only: the sdist prunes `tests/` (MANIFEST.in) and the wheel packages only `rnfs*`.
