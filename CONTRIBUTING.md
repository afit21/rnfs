# Contributing to rnfs

[specification.md](specification.md) is the authority on behaviour, [design.md](design.md) describes how this
implementation works, and [CHANGELOG.md](CHANGELOG.md) records what each release changed.

rnfs is the core: content, the DHT, names, pinning, the local API and the public library. Features for users
beyond that belong in companion apps built on `rnfs.Client` or `rnfs.Node` (README, "Building apps on rnfs").

Compatibility rules every change keeps:
- **The wire format doesn't change** without a deliberate decision: request paths, messages, CIDs, DAG bytes,
  chunk boundaries, provider and name records and announce app_data stay byte-identical, so nodes of different
  versions work together. `tests/test_wire.py` checks this against vectors made by 0.1.3 and 0.1.4.
- **The local HTTP API stays compatible**, status codes included.
- **State upgrades are automatic and one way.** A newer rnfs upgrades an `<rnfs dir>` an older one wrote;
  downgrades need not work.

## Setting up

```sh
git clone https://github.com/afit21/rnfs && cd rnfs
python3 -m venv .venv && . .venv/bin/activate
pip install --require-hashes --only-binary=:all: -r packaging/requirements.lock   # rns, lxmf and theirs
pip install --require-hashes --only-binary=:all: -r packaging/build-tools.lock    # ruff, coverage, build, ...
pip install --no-deps --no-build-isolation -e .
```

The library and the CLI use the standard library besides RNS and LXMF; `rnfs.apiclient`, `rnfs.remote`
(rnfs.Client) and the modules they import never import RNS. Keep it that way.

## Tests

The suite uses `unittest` (pytest isn't needed) and runs on Linux, macOS and Windows, Python 3.11 to 3.14:

```sh
python3 -m unittest discover tests                                   # everything (a few minutes)
python3 -m unittest tests.test_storage tests.test_fetcher            # some modules
python3 -m unittest discover -s tests/security -p "test_*.py"        # the security regressions
```

`.github/workflows/tests.yml` holds the exact commands CI runs. Some tests are opt-in through environment
variables:

| Variable | What it turns on |
|---|---|
| `RNFS_E2E=1` | Real daemons and real RNS over local TCP, driven through the CLI or the library. Run them with `RNFS_E2E=1 python3 -m unittest tests.test_e2e tests.test_client tests.test_embedded tests.security.test_h1_real_rns tests.security.test_m1_real_rns tests.security.test_h2_display_page_pins` (3 to 4 minutes). CI's end-to-end step lists every module gated on it, and `tests/test_packaging.py` fails if a new one is missing from that list. |
| `RNFS_FUZZ_EXAMPLES=N` | A deeper, randomized run of `tests/security/test_fuzz_parsers.py` (e.g. 3000 examples per parser). That module needs hypothesis, which isn't one of rnfs's dependencies: without it every test there is skipped, as in CI. |
| `RNFS_BENCH=1` | Prints the FastCDC chunking rate measured by `tests/test_chunker.py` (the test itself only checks a floor). |

`tests/bench_block_writes.py` is a benchmark, not a test: run it by hand on the disks that matter (its docstring
says how, including on a Raspberry Pi SD card). Performance changes record their before and after numbers in
CHANGELOG.md.

`tests/interop.py` is a manual cross-version check, not a test either: it runs this tree's daemons against daemons
of older releases over local TCP, in both directions, and prints a PASS/FAIL table (providers, get and cat,
transfers with and without `max_upload_rate` checked for refusals, names, keepers, LXMF). Run it before a release
with each supported old release extracted by `git archive`:

    mkdir -p /tmp/rnfs-0.1.5 && git archive v0.1.5 | tar -x -C /tmp/rnfs-0.1.5
    python3 tests/interop.py --old /tmp/rnfs-0.1.5 --old /tmp/rnfs-0.1.4

It takes about five minutes per release; logs are kept on failure or with `--keep`.

Tests that guard compatibility, and must stay green:
- `tests/test_wire.py`: wire known-answer vectors (`tests/fixtures/wire`).
- `tests/test_upgrade_014.py`, `tests/test_upgrade_015.py`: the current code started on an `<rnfs dir>` exactly
  as 0.1.4 and 0.1.5 left it (`tests/fixtures/rnfs-0.1.4`, `tests/fixtures/rnfs-0.1.5`) keeps every row.
- `tests/test_statedb.py`'s released-schema digest (below).
- `tests/test_rns_compat.py`: the RNS internals rnfs relies on.
- `tests/test_packaging.py`: the workflows, the locks, install.sh, build-deb.sh, the Flatpak manifest, the
  supported Pythons, and the CHANGELOG's version against `rnfs.__version__`.

Never weaken or delete a test to make a change pass. If a change deliberately alters behaviour a test encodes,
change the test in the same commit and say why in the commit message.

### Security regression tests

Every security fix comes with a regression test in `tests/security/test_<id>_<what>.py` (for example
`test_cont1_deny_hash_amplification.py`), written first and seen failing before the fix. Rebuild the attack from
its description with the fakes and helpers in `tests/` and `tests/security/` (see the sibling tests), cite
function names rather than line numbers, and add the test to the index in `tests/security/README.md` (ID,
module, fix). A test that needs real RNS is gated on `RNFS_E2E` and added to CI's end-to-end step.

## Lint

ruff, at the version `packaging/build-tools.lock` pins, with the rules in `pyproject.toml`:

```sh
ruff check .
```

A broad `except Exception` in `rnfs/` carries `# noqa: BLE001 - <why it must catch everything>`, an import that
can't be at the top `# noqa: E402`, and RUF100 flags a `noqa` that suppresses nothing. The companion NomadNet
pages (`companion-nomadnet-sites/*/index.mu`) are linted as Python. CI also fails on merge-conflict markers.

Style: match the surrounding code. Every function has a docstring, comments say why rather than what, and lines
are at most 120 columns.

## Dependency and build locks

Every install path (install.sh, build-deb.sh, the Flatpak, the PyInstaller builds, the tests) uses hash-locked
versions:

| File | What | Regenerate with |
|---|---|---|
| `packaging/requirements.lock` | rnfs's runtime dependencies (rns, lxmf and theirs), every file PyPI has for each | `python3 -I packaging/update-lock.py [rns==X lxmf==Y ...]` (no arguments: the newest versions `pyproject.toml` allows) |
| `packaging/build-backend.lock`, `packaging/build-tools.lock` | setuptools; and build, twine, PyInstaller, ruff, coverage and their dependencies | `python3 -I packaging/update-lock.py --build pyinstaller==X ...` (run it on Linux with Python 3.12) |
| `packaging/flatpak/python3-deps.json`, `build-sources.json` | the Flatpak's wheels, from the locks | `python3 -I packaging/flatpak/generate-sources.py` |

After changing a lock, regenerate the Flatpak sources and commit them together: tests.yml's flatpak-sources job
fails if regenerating changes them. The setuptools in `build-backend.lock` must fit `pyproject.toml`'s
`[build-system] requires`, and README's `pyinstaller==` the version `build-tools.lock` pins (both are checked).

**Supported Pythons.** The list is in several places, and `tests/test_packaging.py` checks they agree: the
tests.yml matrix, `requires-python` in `pyproject.toml`, `BUILD_PYTHONS` and the `--python-version` of
`RESOLVE_ARGS` in `packaging/update-lock.py`, `RUNTIME_PYTHONS` in `packaging/flatpak/generate-sources.py`,
`EXTRA_PYTHONS` and `PYTHON_BELOW` in `packaging/build-deb.sh`, and the interpreters `install.sh`'s
`find_python` tries.

## Adding a schema step

Everything rnfs persists is in one SQLite database (`rnfs/statedb.py`). Its schema is the list of steps
`statedb.SCHEMA`; `PRAGMA user_version` counts those applied, and a database is upgraded step by step at start.

- **Append only.** A store whose layout changes gets a new step (`ALTER TABLE`, or a new table and an
  `INSERT ... SELECT`; a callable taking the connection for what SQL alone can't express), labelled with the
  store. Never edit a released step: a database that release wrote already has it, so the edit would never reach
  it.
- The released steps are pinned by a digest in `tests/test_statedb.py` (`RELEASED_0_1_5`, the first 16). Never
  change it; when a release adds steps, pin those with a digest of their own.
- `tests/test_upgrade_015.py` starts the current code on `tests/fixtures/rnfs-0.1.5` (see its README): new
  tables and rows are fine, but none of 0.1.5's rows may change or go. A step that rewrites old rows needs the
  test to say what they become.
- The old JSON state files are imported once (`statedb.LEGACY`, `migrate`); a store that still reads one keeps
  its `tests/test_migrate_*.py`.

## Adding an API route

The local API (`rnfs/api.py`, specification section 11) must stay compatible, status codes included. A new
route touches these places:

1. `ApiHandler.ROUTES` (`(method, path)` to the handler's name) and its `api_*` method in `rnfs/api.py`; a
   feature's route goes in that feature's `api_routes()` (`rnfs/features/`) instead. GET routes never change
   pins, names or content. Errors map to statuses through the error types (`ValueError` 400, `NotFound` 404,
   ...), not by hand.
2. The route tables in `rnfs/api.py`'s module docstring (core or feature table): `tests/test_api.py`'s
   `RouteTableTest` checks that they list exactly the routes, and that each name in `ROUTES` is a method.
3. design.md's list of core routes ("Core API routes", under "Node facade") and, for new behaviour,
   specification.md section 11.
4. The client: an `ApiConnection` method in `rnfs/apiclient.py`, and if it is part of the public library, the
   same method on `rnfs.Node` and `rnfs.Client` (`rnfs/interface.py`, `rnfs/embedded.py`, `rnfs/remote.py`),
   tested on both by `tests/test_public.py`.
5. The CLI command, if any (`rnfs/cli.py`, or the feature's own commands), through `ApiConnection` rather than
   a hand-written path.
6. Tests of the handler (`tests/test_api.py` or the feature's test file), and a CHANGELOG line.

## Commits

One change per commit, with a plain summary line (ending with the plan or audit item ID in parentheses where
there is one) and a body saying what changed and why. Behaviour-neutral refactors go in their own commits, with
the suite green, before the change that relies on them. CHANGELOG.md gets a line for every user-visible change,
under the next version's `## X.Y.Z (unreleased)` heading.

## Releasing

A release is published by pushing a tag `vX.Y.Z`: `.github/workflows/release.yml` runs the whole tests workflow,
builds every package (wheel and sdist, `.deb` for amd64 and arm64, the x86_64 Flatpak, the Windows exe),
smoke-tests them, and (through the `release` environment, which may require a reviewer) publishes a GitHub Release with
`SHA256SUMS` and signed build provenance. The release notes are CHANGELOG.md's section for the version.

The same builds run, without publishing, on pull requests that touch `packaging/**`, `pyproject.toml` or
`.github/**`, weekly, and when the workflow is started by hand, so check that they pass before tagging.

Steps:
1. Go through the checklist below.
2. Set `__version__` in `rnfs/__init__.py` to X.Y.Z, change CHANGELOG.md's first heading from
   `## X.Y.Z (unreleased)` to `## X.Y.Z`, and update README's release links and file names
   (`tests/test_packaging.py` checks the three agree).
3. Commit, let CI pass on `main`, then push the tag: `git tag vX.Y.Z && git push origin vX.Y.Z`. The workflow
   refuses a tag that isn't `v` + `__version__`, and a CHANGELOG whose section for it is still marked
   unreleased.
4. If the `release` environment requires a reviewer, approve its deployment once the builds pass.
5. Open the next version's section: `## X.Y.(Z+1) (unreleased)` at the top of CHANGELOG.md (`__version__` stays
   X.Y.Z until that release), and name that version in the note at the top of specification.md (checked too).

### Release checklist

- [ ] Every decision the release's plan needed is answered, and the maintainer-only actions (GitHub settings,
      PyPI, benchmarks on real hardware) are done or deliberately left.
- [ ] Wire: `tests/test_wire.py`'s 0.1.3 and 0.1.4 vectors pass byte for byte.
- [ ] Interop: the new version against the previous releases' nodes (fixtures in `tests/fixtures/`,
      `tests/test_rns_compat.py`). Fetch, provide, names and keepers behave the same, and a previous release's
      fetcher sees no new refusals in an honest transfer.
- [ ] Upgrade: `tests/test_upgrade_015.py` (and `test_upgrade_014.py`) pass, including any table the release
      adds, and the released-schema digest is unchanged.
- [ ] Every security fix has its `tests/security/test_<id>_*.py` regression test, and `tests/security/README.md`
      lists it.
- [ ] CI runs every `RNFS_E2E` module. Lint passes.
- [ ] Before and after numbers for the performance changes (a desktop NVMe disk and a Raspberry Pi SD card),
      recorded in CHANGELOG.md.
- [ ] specification.md updated for every change in behaviour, with its version and compatibility note at the top.
- [ ] design.md updated for every change in how things work.
- [ ] CHANGELOG.md's entry states whether the wire format or CIDs changed ("no wire-format or CID changes"),
      has a **Security** section, and lists every user-visible change (changed defaults, new refusals, changed
      formats, new settings).
- [ ] CHANGELOG's version equals `__version__` (`tests/test_packaging.py`).

For 0.1.6 the plan (`0.1.6-plan.md`, "Release checklist") adds: the power-loss recovery path of item B is
tested (a leftover `.dirty`, a truncated recent block, an old block left alone); the numbers cover A1, A2, B, C1,
D1 and D3; and the CHANGELOG's user-visible changes include FEAT-5's API default, API-1's refusals, API-7,
SUP-3, SUP-5, FEAT-2's inbox format and `follow_quota`.
