# 0.1.5 (released)

## CID denylist (done)

- Ability to point to a CID or name as a CID deny list (`deny_lists`, by CID or `name/<name>`)

- Refuse to fetch, provide or store a CID.

- Deny lists can contain file hashes and/or CIDs

## Sqlite3 migration (done)

- State is kept in one SQLite database, `<rnfs dir>/state/rnfs.db`

- The JSON stores of earlier versions are imported at the first start, and kept as `<name>.migrated`

## Client Library (done)

* `rnfs.Client` (the user's running daemon, through the local API) and `rnfs.Node` (a node inside a program),
  with the same methods and objects; provisional until 0.2 (`rnfs.API_STATUS`)

## Change notifications for other applications to subscribe to (done)

* Name changes: `names.watch` in the library, `GET /api/name/watch` in the local API
* Job progress and completion: job callbacks in the library

# 0.1.6 (in progress)

Security fixes, maintainability work and performance work, with no wire-format or CID changes (see
CHANGELOG.md).

# Later

## Alternate Pin Sources

* List if a file was pinned by an application via an API. `pin ls` shows each pin's sources (`user`, the shared
  folder, a name, a deny list, ...), but not which application pinned it

## API versioning and reference

* The public library stays provisional until 0.2 (`rnfs.API_STATUS`)
* The local API's routes are listed in `rnfs/api.py`'s docstring (checked against the code by
  `tests/test_api.py`); a reference with request and response shapes is still to come

## Per-app API tokens

* Destination.hash(identity, "rnfs", "api")
* Whitelist and/or API key for auth
