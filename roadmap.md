# 0.1.3

## Default Bootstrap Node List
DEFAULT_BOOTSTRAP_NODES global variable: node IDs of official, always-on DHT servers, giving new nodes a first contact when no announces reach them (e.g. behind a LoRa access point)
* Used in the startup join alongside `bootstrap_nodes` (the user's own are tried first), as IPFS does
* Used again, with backoff, whenever fewer than about 3 servers are known, so isolated nodes and small islands rejoin
* Tried in random order, with their own path-request budget, so one offline node doesn't block the rest
* Skipped in private mode, or when a setting turns them off
* After the join they are ordinary DHT servers, with no extra trust

# 0.1.4

## CID denylist

- Ability to point to a CID or name as a CID deny list

- Refuse to fetch, provide or store a CID.

- Deny lists can contain file hashes and/or CIDs

## Migrate to JSON stores to sqlite3

- If JSON store exists, convert to the sqlite3 stor

## Alternate Pin Sources
* List if a file was pinned by an application via an API

## Change notifications for other applications to subscribe too
* New file pinned by daemon


# 0.1.5 

## Client Library

## API versioning and reference

## Per-app API tokens

## RNS API

* Destination.hash(identity, "rnfs", "api")
* Whitelist and/or API key for auth