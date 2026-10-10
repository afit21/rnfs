# rnfs Specification

rnfs is content-addressed file sharing over [Reticulum](https://reticulum.network) (RNS), with the functional
features of IPFS: content IDs, chunked files, directories, a provider DHT, verified multi-source transfer,
pinning, mutable names and a gateway — built on what RNS already provides.

**Principles**
- No compatibility with IPFS or libp2p is required. Follow IPFS where its design helps; drop what exists only
  because of IP networks or legacy.
- Use RNS's native features rather than reimplementing them.
- Interfaces may be slow and shared (LoRa at ~1 kbit/s, packet radio), so every message and periodic task must
  be cheap, and limits adapt to link speed.
- Nothing received from another node is trusted until verified.

The key words MUST, SHOULD and MAY are used as in RFC 2119. Names in `UPPER_CASE` are protocol constants and
names in `lower_case` are local settings; both are listed in section 12. Appendix A compares rnfs with IPFS.

## 1. Overview

- **Add.** A file is cut into chunks; chunks and directories are encoded as blocks, each named by a CID (the
  hash of its bytes). The root CID names the whole file or directory. The node stores the blocks and pins
  the root.
- **Provide.** The node finds the nodes whose IDs are closest to the root's key (`/find`) and tells them it
  has the root (`/provide`), repeating this periodically.
- **Get.** Another node asks the DHT (`/find`) who provides the root, fetches the root block from a provider
  (`/blocks`), walks it, and fetches the rest from one or more providers, checking every block against its
  CID.
- **Names.** A name is a key pair whose RNS announces carry "this name points at CID X". Anyone can resolve it
  through RNS's own announce caching; nobody but the owner can change it.

## 2. Terms

- **Block:** bytes whose SHA-256 is a CID's digest. At most `MAX_BLOCK_SIZE`.
- **CID:** content identifier of a block (section 4).
- **DAG node:** a block encoding part of a file or directory, listing *children* by CID (section 5).
- **Child:** an entry in a DAG node: a child block's CID and the number of file content bytes below it.
- **Below:** reachable by following children. A CID's **DAG** is its own block plus every block below it; a
  file or directory is the DAG of its root.
- **Root:** the CID a user adds, gets, pins or publishes. **Target:** the CID a path `<root>/<path>` leads to
  (the root itself when there is no path).
- **Complete:** a node holds a CID completely when it holds every block of its DAG.
- **Provider:** a node that holds a CID completely and has told the DHT so.
- **Pin / cache:** pinned blocks are kept until unpinned; other stored blocks are cache and may be deleted.
- **Node ID:** a node's address in the DHT (section 3).
- **Link:** always an RNS Link.
- **Rate estimate** of a link: the rate RNS measured for the last response of at least `min_rate_sample` on
  it, in bits/s (divide by 8 for bytes). It is recorded when that response completes, from
  `Link.get_expected_rate()`, since RNS overwrites that value with every Resource transfer, including small
  ones.
  Smaller responses are dominated by round-trip time and aren't used. A link has no rate estimate until then.
  Rates are never derived from the round-trip time, which the remote end can inflate.
- **Slow link:** a link whose rate estimate is below `slow_bitrate`. A link with no rate estimate is slow if
  its MTU (`Link.get_mtu()`) is 500 bytes or less: RNS raises a link's MTU only when every hop supports it,
  which RNode, serial, KISS, AX.25, UDP, I2P and pipe interfaces don't. Some fast links therefore start out
  slow until measured, which only makes them cautious.
- **Slow node:** a node with `slow` set to `yes`, or set to `auto` (the default) with every interface of its
  RNS instance below `slow_bitrate`. Interfaces are read with `Reticulum.get_interface_stats()`, which also
  works through a shared instance. Shared-instance and local-client interfaces, offline interfaces, and
  interfaces reporting no bitrate (or 0) are ignored; if none remain, the node is not slow. Node-wide settings
  (`alpha`, `republish_interval`, `max_parallel_providers`) depend on the node; per-link values (budgets,
  deadlines, `max_upload_rate`) on the link.

## 3. Use of Reticulum

**Process, identity and destination.**
- rnfs MUST run in its own process, since one of its limits (below) changes a process-wide RNS setting, and
  SHOULD attach to a shared RNS instance (`rnsd`).
- Each node has one RNS identity, kept at `<rnfs dir>/storage/identity`.
- Each node has one IN/SINGLE destination, `rnfs.dht`, which receives every rnfs request.
- A node's **node ID** is the hash of that destination: `Destination.hash(identity, "rnfs", "dht")`, 16 bytes.
  The DHT keyspace is therefore 128 bits.
- **Versioning.** A breaking protocol change uses a new aspect (e.g. `rnfs.dht2`), so incompatible nodes never
  talk to each other. Messages carry no version field.

**Announces and paths.**
- A node announces `rnfs.dht` at startup, again after `startup_announce_delay` (interfaces may not have been
  up), then every `announce_interval`. Ratchets are not enabled: links are already forward secret.
- The announce's `app_data` is the node's DHT mode as a msgpack integer: `1` server, `0` client (section 6).
  - Anything other than msgpack `1` means client. A node re-announces when its mode changes.
  - It is set with `Destination.set_default_app_data()`, so path responses carry it too.
  - Nodes read it with an announce handler for `rnfs.dht` registered with `receive_path_responses=True`, or with
    `Identity.recall_app_data()` after a path request.
- Reaching a node needs only its node ID: RNS finds a path (`Transport.request_path`) and the public key
  (`Identity.recall`, filled from announces and path responses).

**Links.**
- Requests to a node share one outgoing link, opened on demand and closed after `link_idle_timeout` without
  traffic (`Link.no_data_for()`). These links don't identify, so lookups and downloads don't reveal who is
  asking.
- A node identifies (`Link.identify()`) only:
  - on a separate link opened to publish (section 6) or to send `/pin` (section 8), closed when done; or
  - on every link, in private mode.
- `Link.identify()` is unacknowledged. A request answered `False` on an identified link is retried once after
  identifying again. RNS keeps the first identity a link proves and ignores later ones, so a link can't
  switch identities.

**Messages.** Every message is an RNS link request (`Destination.register_request_handler` / `Link.request`),
encoded with msgpack by RNS. Requesters use RNS's RTT-based request timeout (`timeout=None`).

| Path | Request | Response | Section |
|---|---|---|---|
| `/find` | `key` | `[[provider IDs], [node IDs]]` | 6 |
| `/provide` | `[[key, …], [withdrawn key, …]]` | `True` if recorded | 6 |
| `/blocks` | `[max_bytes, [cid, …]]` | `[block or None, …]` | 7 |
| `/pin` | `[cid, pin]` | `True` if accepted | 8 |

- Field types: keys and node IDs are 16-byte `bin`; CIDs are binary CIDs as `bin`; blocks are `bin`;
  `max_bytes` is a non-negative integer; `pin` is a boolean.
- Any request of another shape, or one refused under the limits below, is answered `False` at once — never
  left unanswered, except a request over `MAX_REQUEST_SIZE`, which RNS drops unanswered.
- Every request fits in one packet: at most `min(Link.get_mdu(), MAX_REQUEST_SIZE)` bytes including RNS's
  request framing (~28 bytes). Batch sizes are computed from that (e.g. 22 keys per `/provide` at the default
  MTU, counting both lists).

**Limits.** These keep a node usable under load.
- Requests are at most `MAX_REQUEST_SIZE` (`Destination.set_max_request_size`).
- Per link: at most `MAX_REQUESTS_PER_LINK` requests in progress, of which at most one is `/blocks`.
  Requesters MUST stay within this.
- At most `max_blocks_responses` `/blocks` responses are in progress in total, split between two pools:
  responses on slow links (`max_blocks_responses` // 2 slots) and on other links (the rest), so each pool
  has at least one slot. A response stays in the pool it started in, even if its link's class changes. Each
  pool competes only with itself, so neither kind of client can crowd out the other.
  - A response's **deadline** is 3 × its size at the link's rate estimate (at `unmeasured_bitrate` if it has
    none), plus `deadline_slack`. A response not finished by then is cancelled.
  - When a pool is full, a new `/blocks` request for it displaces that pool's in-progress response with the
    lowest throughput, if that response is older than `deadline_slack`; otherwise the new request is refused.
  - A handler gets no direct handle on the response Resource RNS sends for it. Responses are tracked through
    the link's outgoing Resources (`Link.outgoing_resources`, matched by `request_id`) and cancelled with
    `Resource.cancel()`. These are RNS internals that may change between versions; if they're unavailable, a
    response is cancelled by closing its link.
- At most `max_incoming_links` incoming links. When full, one is closed to make room: the link idle longest
  with no request in progress, or else a random one with no `/blocks` response in progress.
- rnfs neither sends nor accepts compressed Resources. Handlers are registered with `auto_compress=False`,
  and nodes MUST set RNS's maximum decompressed Resource size (`RNS.Resource.AUTO_COMPRESS_MAX_SIZE` in
  RNS 1.5.6) to 0, since RNS decompresses before any rnfs check runs. Setting it to 0 also stops RNS
  compressing what the node sends.
- `max_upload_rate` caps bytes sent for `/blocks` on a link once it has a rate estimate. RNS paces a Resource
  by its receiver, so the cap is applied between responses: the response budget uses the lower of the rate
  estimate and `max_upload_rate`, and the next response on that link starts no sooner than
  `bytes × 8 / max_upload_rate` seconds after the previous one started.

**Private mode.** RNS Interface Access Codes keep outsiders off an interface. To restrict rnfs itself
across shared interfaces and transport nodes, a node MAY run in private mode:
- every link identifies;
- handlers answer `False` to identities not on the member list (checked inside the handler, since RNS's
  `ALLOW_LIST` drops refused requests silently);
- the routing table admits only members.

## 4. Content identifiers

A CID is 36 (raw) or 39 (rnfs-dag) bytes, written as `b` + lowercase unpadded base32 for people (CLI, API,
gateway, logs). On the wire CIDs are always binary.

| Codec | Binary CID | Block contents |
|---|---|---|
| raw | `01 55 12 20` + digest | Bytes of a whole file, or one chunk of a file |
| rnfs-dag | `01 80 80 c0 01 12 20` + digest | A DAG node (section 5) |

- The digest is the SHA-256 of the block. `0x300000` (rnfs-dag) is from multicodec's private-use range, so
  rnfs CIDs can't be mistaken for an IPFS codec.
- Nothing else is a valid CID. A string is valid only if it equals `encode(decode(string))`, so every CID has
  exactly one written form.
- A file of at most `CHUNK_SIZE` bytes is a single raw block; its CID is byte-for-byte the IPFS CID of that
  raw block.
- **DHT key** of a CID: `SHA-256(binary CID)[0:16]`, in the node ID space. Hashing the whole CID keeps a raw
  block and a DAG node with identical bytes apart.

## 5. Files and directories

**Encoding.** DAG nodes are msgpack arrays with exactly the fields shown, and each field has exactly the
msgpack type shown:
- type tags, sizes and kinds are non-negative integers, never booleans;
- CIDs are `bin`;
- names are `str` holding valid UTF-8.

Every header uses its shortest form, and no other msgpack types appear. Writers produce exactly this (RNS's
bundled `umsgpack.packb` does). Readers MUST reject a node that breaks these rules or doesn't re-encode to
identical bytes.

| Node | Encoding | Children |
|---|---|---|
| File | `[1, [[cid, size], …]]` | Raw chunks or file nodes, in order |
| Directory | `[2, [[name, cid, size, kind], …]]` | Entries sorted by name (bytewise UTF-8), names unique |
| Directory index | `[3, [[first_name, cid, size], …]]` | Directory or index nodes, in name order; `first_name` is the first name below that child |

- **size** is the number of file content bytes below a child: a raw block's length, or the sum of a node's
  child sizes. A size above 2^64 − 1 makes the DAG invalid; implementations MUST check for overflow.
- **kind** of a directory entry: `0` file, `1` executable file, `2` directory.
- **Building a tree** (writers MUST follow this, so the same content always has the same CID):
  1. Start with the list of children: a file's chunks (consecutive `CHUNK_SIZE` pieces; the last may be
     shorter, never empty), or a directory's sorted entries.
  2. While the list has more than `MAX_CHILDREN` children:
     - split it into consecutive groups of `MAX_CHILDREN` (the last group may be smaller);
     - make one node per group (files: type 1; directories: type 2 for groups of entries, type 3 above that);
     - continue with the list of children pointing at those nodes.
  3. The final list becomes the root node.

  Special cases: a file with one chunk is just that raw block; an empty file is a zero-byte raw block; an
  empty directory is `[2, []]`.
- **Reader checks.** For every node it fetches, a reader MUST check:
  - each child is of a type the table allows: a file node's children are raw blocks or file nodes; an index's
    are directory or index nodes; a directory entry of kind 0 or 1 is a raw block or file node, and of kind 2
    a directory or index node;
  - each child's size against the child, once the child arrives;
  - names: valid, unique, and sorted within the node;
  - indexes: `first_name`s strictly increasing; each equal to the first name below that child; and every name
    below a child less than the next child's `first_name`;
  - depth.

  A node failing a check makes the root being fetched invalid.
- **Path lookup** in an index: a name is in the child with the greatest `first_name` that is ≤ the name
  (bytewise); if it isn't there, it doesn't exist.
- **Names** are UTF-8, 1–255 bytes, with no `/` or NUL, and not `.` or `..`. Names are stored exactly as
  read, without Unicode normalisation.
  - `add` skips, with a warning, names that break these rules or aren't valid UTF-8.
  - `get` MUST NOT write a name the local filesystem would interpret as a path, and reports names it cannot
    represent.
- **Paths:** `<cid>/<name>/<name>…` walks directory and index nodes. Commands and the gateway accept paths,
  and `<root>/<path>` is the shareable form of anything inside a DAG.
- **Depth** — DAG nodes from the root down, all types counted — is at most `MAX_DEPTH`; `add` fails on a
  deeper tree.
- `add` includes hidden files, and skips symlinks, devices and sockets with a warning. Modification times and
  other metadata are not stored, so identical content always gets an identical CID.

## 6. DHT

Kademlia, used only for provider records. The distance between IDs `a` and `b` is `a XOR b` as a 128-bit
big-endian integer; "closest" means smallest distance.

**Servers and clients.** As in IPFS, a node runs the DHT as a *server* or a *client* (`dht_mode`):
- A server answers `/find` and `/provide`, stores provider records, and appears in routing tables.
- A client only makes requests: it answers `/find` and `/provide` with `False`, and is never added to routing
  tables. A client still serves `/blocks` and `/pin`, and publishes its own provider records, so it can share
  content.
- Nodes that are often offline, mobile, battery-powered or very short of bandwidth SHOULD run as clients, so
  others don't waste queries on them. A mesh needs some servers; on an all-radio mesh, the more stable nodes
  should stay servers. A node that knows no servers can still fetch from nodes it has an open link to, or with
  `get --from`.

**Routing table.** Bucket `i` (0–127) holds up to `K` servers at distance `[2^i, 2^(i+1))` from our own ID.
- A node is added only once it has proven to be a reachable server: we heard its announce with server mode,
  or it answered a `/find`. IDs merely mentioned in a reply are not stored.
- A full bucket keeps its nodes. A newcomer replaces the least recently heard node only if that node failed a
  query since it was last heard, or has been silent for over `2 × announce_interval`. Long-lived nodes are
  more reliable and harder for an attacker to displace.
- A node is dropped after `max_failures` queries in a row go unanswered; hearing from it resets the count.
  Lossy links can drop a live server this way, so a dropped node's first-seen time is kept for
  `2 × announce_interval`, and it regains its seniority if it comes back within that time.
- **Joining.** At startup a node queries any bootstrap node IDs it was given (`daemon --bootstrap`) with
  `/find` for its own node ID, adds those that answer, and continues that lookup, which fills its table with
  its neighbours.
- Because RNS floods announces, on small meshes (a few dozen nodes) the routing table holds most of the
  network, so lookups usually finish in one or two rounds.

**`/find`.** The responder answers with:
- up to `K` providers of `key`, leaving out expired and banned ones: the `K/2` with the oldest records, listed
  first, then `K/2` chosen at random from the rest. Old records favour long-standing providers; the random
  half stops early squatters holding every slot;
- up to `K` servers it knows closest to `key`, not including itself, nodes that have failed a query since it
  last heard from them, or nodes silent for over `2 × announce_interval`. Without IPFS's periodic table
  refresh, this keeps departed nodes out of replies at no airtime cost.

**Lookups.** Query the `alpha` closest known nodes not yet queried with `/find`, merge the nodes they return,
and repeat.
- A returned node or provider ID with no known identity (`Identity.recall` fails) costs a path request; at most
  `alpha` such IDs are tried per lookup.
- Nodes that fail to answer are dropped from the lookup and counted in the routing table.
- Every lookup has a time budget, `lookup_timeout`.
- **Termination.** As in IPFS, a lookup is complete once the `beta` closest nodes it knows of have all
  answered, or once every known node has been queried without finding new ones.
- **Finding the closest nodes** (for publishing) runs until the lookup is complete. The result is the `K`
  closest nodes known; the stores sent to them also show which are reachable.
- **Finding providers.** Downloading starts with the first provider found. The search ends when
  `max_parallel_providers` providers are known and one has delivered the verified root block, when the lookup
  is complete, or at `lookup_timeout`. It does not stop at the first reply that lists providers, so one node
  near the key can't hide content.
- **Choosing providers.** Providers are tried in this order: those that have been in the routing table for over
  `provider_seniority`, then the rest in reply order. A provider *fails* when its link fails, it answers
  `None` for a block of the root it advertised, it answers `False` (busy) `max_failures` times in a row, or it
  stalls twice (section 7). A `False` is retried after a short delay. Failed providers are removed and not
  retried during that fetch. When no providers are left, `/find` runs again for a fresh random half, within
  the same `lookup_timeout`.

**Publishing.**
- For each key, find the `K` closest nodes. Then send each node the keys it should store, in as few `/provide`
  requests as the batch size allows (section 3), one at a time over a separate identified link closed
  afterwards.
- `/provide` is answered `True` if the keys were recorded, or `False` if the request was refused (the link
  isn't identified, or a section 3 limit applied). Apart from the one re-identify retry of section 3, a
  refused or failed store isn't retried until the next republish.
- To stop providing a CID (unpinned, deleted), a node SHOULD send it as a withdrawn key to the nodes it last
  published it to.

**Provider records.**
- `/provide` records the identified requester's node ID as a provider; a node can only advertise itself.
  Records carry no addresses, since RNS routes by node ID.
- A record expires `PROVIDER_TTL` after it was last stored, keeps its first-stored time across republishes,
  and is removed on withdrawal.
- A node keeps at most `max_records`. When full, it evicts a record of the publisher it first saw most
  recently, so a flood of new identities displaces only itself. First-seen times are persisted, and dropped
  once a publisher has no records left.
- Providers republish every `republish_interval`, which MUST be less than `PROVIDER_TTL / 2`. Providers also
  record themselves locally.
- **Provide strategy** (`provide_strategy`):
  - `pinned` (default) provides every complete pin.
  - `roots` also provides every complete fetched target while it is cached.

  `pinned` uses less airtime and doesn't publish what a user downloads; `roots` helps popular content spread.
  Nothing else inside a DAG is provided separately: share `<root>/<path>`, or pin the inner CID to provide
  it.

## 7. Transfer

**`/blocks`.**
- **Request:** `[max_bytes, [cid, …]]`, with as many CIDs as fit in one packet.
- **Response:** a list in request order, one entry per CID until the budget is reached. Each entry is the
  block, or `None` if the node doesn't hold it or doesn't serve it. Entries after the last returned were not
  sent because of the budget.
- **Budget:** the first served block is always included, even if it exceeds `max_bytes`. After it, a block is
  added only if the response's total block bytes stay within:
  - `max_bytes`;
  - `MAX_RESPONSE_SIZE − 1 KiB`, so the encoded response stays within `MAX_RESPONSE_SIZE`;
  - if the link has a rate estimate, `response_target` of transfer at the lower of that estimate and
    `max_upload_rate`.
- **What a node serves:** only blocks in the DAG of a CID it currently provides (the same reachability GC
  computes). With `pinned`, cached downloads answer `None`, so a node's download history can't be read by
  asking for it.
- **Before serving**, a node SHOULD re-check the block against its CID, so a damaged block on disk doesn't get
  it banned.
- **Example.** Request `[60000, [c1, c2, c3]]`; the node holds `c1` (40 000 bytes) and `c3` (30 000 bytes), but
  not `c2`. Response `[b1, None]`: `c2` isn't held, and `c3` would take the total past 60 000, so it was left
  for a later request.

**Requester.**
- Sets `max_bytes` to `response_target` of transfer at the link's rate estimate. Until the link has a rate
  estimate, `max_bytes` is `CHUNK_SIZE`: small enough to finish on any link, and large enough that its
  completion gives the link a rate estimate.
- Passes `max_response_size = min(max(max_bytes, MAX_BLOCK_SIZE) + 1 KiB, MAX_RESPONSE_SIZE)` to RNS.
- MUST verify every block against its CID before storing or using it.

**Fetching a target** (`get`, `cat`, `pin add`, `export`):
1. Ask up to `max_parallel_providers` nodes with an open outgoing link for the root with `/blocks`; if none
   has it, find providers (section 6).
2. Fetch the root block and decode it. Fetch the blocks not already stored: those along the requested path,
   then every block of the target's DAG — or, for a byte range (`cat --offset`, gateway Range requests), only
   the chunks covering it and the nodes above them. Each block is fetched once, even if it appears several
   times in the DAG.
3. `max_download_size`, if set, limits the bytes a fetch may download. Before fetching below a node, its
   total size (for a byte range, the range's size) is checked against it. Sizes are verified as blocks arrive
   (section 5), so a DAG can't hide its size.
4. Writing out a DAG (`get`, `ls`, the gateway, `export`) stops with an error after `max_output_entries`
   entries, counting every occurrence. This catches DAGs that repeat empty subtrees, which have size 0.
5. Spread requests over up to `max_parallel_providers` providers (1 on slow nodes, where parallel transfers
   only contend for the same channel). A response *stalls* if it makes no progress for `stall_timeout`, or
   misses the same deadline the responder uses (section 3). A stalled provider's link is closed, which cancels
   the transfer, and its blocks are requested again from another provider; providers that keep failing are
   handled as in section 6.
6. Blocks are stored as they arrive, so an interrupted fetch resumes where it stopped. An RNS Resource is only
   delivered when complete, so a response lost mid-transfer yields nothing and its blocks are requested again.
7. When the target is complete, the node provides it if its strategy says so (section 6).

## 8. Storage

- Blocks are stored in `<rnfs dir>/blocks/`, one file per CID, written atomically after verification.

**Pins.**
- Pins are recursive: a pinned CID keeps every block of its DAG.
- A pin is held by one or more *sources*: `user`, `folder`, `name:<alias>` and `remote:<identity>`. It lasts
  while any source holds it.
- `pin rm` removes only the `user` source. Other sources are released by whatever created them: the folder
  scan, `name publish` or `name rm`, and `/pin` with `false` or a remote quota being exceeded.
- A pin is provided only once it is complete. Blocks of fetches, adds and imports in progress are protected as
  if pinned.

**Cache** is everything else.
- Each cached fetch is recorded as the `<root>/<path>` it fetched, which keeps the path's nodes as well as the
  target's DAG.
- When storage exceeds `max_storage`, cached fetches are evicted least recently used first; `gc` evicts them
  all.
- A block is deleted only if no pin and no remaining cached fetch reaches it.
- The node stops providing anything it no longer holds completely, and withdraws it (section 6).
- Pins are never evicted. If pins alone fill `max_storage`, new fetches that aren't being pinned are refused.

**Shared folder.** Every `scan_interval`, files and directories in `<rnfs dir>/files/` are imported as with
`add` (copied into the block store) and pinned with source `folder`. A changed or removed file releases its
old pin.

**Archives** move content without a network (the equivalent of IPFS CAR files).
- `export` fetches the target if needed, then writes a msgpack stream: `["rnfs-archive", 1, [root CIDs]]`,
  then `[cid, block]` for every block of every root's DAG, depth-first from each root, each block once. For
  `export <cid>/<path>`, the archive's root is the target.
- `import` verifies every block, pins each complete root with source `user`, and reports any root that isn't
  complete.

**Remote pinning.**
- A node MAY accept `/pin` from identities on its remote-pin list.
  - `[cid, true]` is answered `True` once accepted. The node then fetches the CID and pins it with source
    `remote:<identity>`.
  - `[cid, false]` releases that source.
- Each remote source has a storage quota, `remote_pin_quota`, counting the full size of every CID it pins. A
  pin that would exceed it is dropped, as soon as the CID's verified size shows it.
- The requester learns that a remote pin is complete when the node shows up as a provider (`providers`).
- `rnfs pin remote add|rm <node ID> <cid>` sends these requests over an identified link.

## 9. Names

A **name** is a mutable pointer to a CID, owned by a key pair: the IPNS equivalent.

**Name keys.**
- Each name has its own RNS identity, stored at `<rnfs dir>/names/<alias>`.
- The name is the hash of that identity's `rnfs.name` destination: 16 bytes, shown as 32 hex characters.
- Separate keys mean names can't be linked to each other or to the node, and a name can be moved to another
  machine by exporting its key.

**Record.**
- The name destination's announce `app_data` is `[sequence, cid]` (msgpack: unsigned integer, binary CID).
  RNS signs it as part of the announce.
- The publisher MUST set it with `Destination.set_default_app_data()`, so the announces RNS sends in answer to
  path requests carry it too.

**Publish** (`name publish <alias> <cid>[/path]`):
1. Resolve any path to a CID locally.
2. Pin the target with source `name:<alias>`, and release that source from the previous target.
3. Set the record with `sequence` one higher than both the publisher's stored sequence and the highest it can
   resolve, and persist it.
4. Announce, and re-announce every `announce_interval`.

**Resolve** (`name resolve <name>`):
- The daemon registers one announce handler for `rnfs.name` (with `receive_path_responses=True`).
- To refresh, call `Transport.request_path(name)`, wait up to `path_timeout`, then read
  `Identity.recall_app_data(name)`.
- Return the record with the highest sequence held, and how long ago it was heard.

**Sequence.** RNS stores any validly signed announce, including a replayed old one, so persisted sequences are
what prevent rollback.
- For every name this node has published, or resolved through the CLI or API, it MUST persist the highest
  sequence seen, and never accept a lower one from any source.
- At equal sequence, the record already held is kept.
- Records for other names (heard, or resolved by the gateway) are kept only in a table of at most
  `name_table_size` entries, least recently used evicted first.

**Limits.**
- A name resolves only while its owner is online or a reachable transport node still holds its announce.
  Paths last 1 week on normal interfaces, less on access-point and roaming ones.
- RNS transport nodes rate-limit rebroadcasts per destination (by default one an hour, after a burst of about
  6), so a name can be updated about once an hour sustained.
- RNS orders announces by a signed emission timestamp from the publisher's clock, so a publisher whose clock
  goes backwards can't update its name until old paths expire.
- A resolver's first resolution of a name can't detect a replayed old record.
- The same name key used on two machines can produce two records with one sequence; each resolver keeps the
  one it heard first.

## 10. Protection against misbehaving nodes

Nodes assume others may be faulty or hostile. Data integrity is guaranteed; availability is protected as far
as is cheap. Rules live in the sections referenced.

| Threat | Protection |
|---|---|
| Forged or corrupted data | Every block verified against its CID (7) |
| Size lies, repeated or empty-subtree bombs, deep trees | Verified sizes, `max_download_size`, `max_output_entries`, `MAX_DEPTH`, each block fetched once (5, 7) |
| Malformed DAG nodes | Strict msgpack types, re-encoding and per-node checks (5). The tree rule gives honest writers one CID per content; readers don't enforce it |
| Advertising on someone else's behalf | Records name only the identified requester (6) |
| Advertising content it doesn't have, or stale records | Failed providers removed; the search runs again; withdrawals (6, 7) |
| Squatting or flooding provider records | Oldest-plus-random replies; long-known providers tried first; eviction of the newest publishers; no identity switching on a link (3, 6) |
| Hiding content near a key | Provider search doesn't stop at the first reply that lists providers (6) |
| Routing table takeover with new identities | Only proven-reachable nodes stored; full buckets keep long-lived nodes (6) |
| Fake node IDs in replies | Unknown IDs never stored; at most `alpha` path requests for them per lookup; lookup time budget (6) |
| Slow or stalled transfers (both directions) | Rates measured from transfers, never from round-trip time; response deadlines; displacement of the slowest response within separate slow and fast pools; requester stall detection (2, 3, 7) |
| Request flooding, link exhaustion | Request size, per-link and global limits; eviction of idle incoming links (3) |
| Decompression and oversized-response bombs | Compressed Resources refused; response sizes capped (3, 7) |
| Reading another node's download history | Only provided content is served (7) |
| Name rollback or forgery | Self-certifying names; RNS-verified signatures; persisted highest sequence (9) |

**Penalties.** A provider that sends a block that doesn't match the CID requested from it is banned for
`ban_time`: dropped from the routing table, and its announces and records ignored. Nothing else bans:
- timeouts and malformed replies only count as failures, since they also come from lossy links and version
  mismatches;
- a correctly hashed block that isn't a valid DAG node is the content author's fault, not the provider's, so
  the root being fetched is marked invalid locally and the fetch fails.

RNS's own `blackhole_identity` is not used: it is instance-wide and persistent.

**Privacy** (outside private mode, where every link identifies).
- Lookups and downloads use unidentified links, so the nodes asked learn only that *some* node wants a key.
- Publishing uses a separate identified link, never reused.
- With the default `pinned` strategy, downloads are neither published nor served.
- DHT keys don't hide which CID is wanted from someone who already knows the CID.
- Transport nodes on the path see traffic volume and timing.

**Limits.**
- RNS identities are free to create. A determined attacker can surround one key with responsive nodes and
  make that key hard to find (a Sybil attack); it can't forge data. `get <cid> --from <node ID>` bypasses
  the DHT when a provider is known.
- Patient attackers that keep identities alive for days defeat the preference for old records and long-known
  providers.
- Bans are evaded by switching identity; they only make an attacker pay for a new one.
- A provider can lower its own measured rate, and so slow a fetch from it; on slow nodes, which use one
  provider at a time, this can delay a fetch for a long time.
- Mass announces from new identities compete for RNS's announce bandwidth, which RNS itself caps.
- Any web page can make the local gateway fetch content (a plain request to `127.0.0.1` passes the Host
  check), and can time responses to guess what is cached. The gateway's own limits (section 11) bound the
  airtime this costs.
- Unidentified links can still be tied to their sender by path and timing, e.g. a publisher's `/find` just
  before its identified `/provide`.
- A requester that already holds the blocks it asks for can complete responses unusually fast and inflate its
  link's rate estimate at the responder; the effect is limited to that link.
- An attacker that behaves exactly like a slow-link client competes fairly for the slow pool's slots, and
  can't be told apart from one.
- An outside web site can frame a gateway page and receive public content from it by `postMessage` (outside
  private mode, only content public by CID).

## 11. Local interface

`rnfs daemon [--bootstrap <node ID>…]` runs a node. Other commands use its local API. The API and the gateway
are local conveniences, not part of the protocol.

| Command | Does |
|---|---|
| `add <path>\|-` | Import a file, a directory or stdin, pin it with source `user`, print its CID. `--wrap` puts it in a directory; `--only-hash` just prints the CID |
| `get <cid>[/path] [-o out] [--from <node ID>]` | Fetch and write out a file or directory |
| `cat <cid>[/path] [--offset N] [--length N]` | Fetch and write a file, or part of one, to stdout |
| `ls <cid>[/path]` | List a directory: name, kind, size, CID |
| `stat <cid>[/path]` | Size, block count, and how much is held locally |
| `dir set <cid>[/path] <name> <cid> [--exec]`, `dir rm <cid>/<path>` | Make a new directory with an entry added, replaced or removed, and pin it with source `user` (fetching the complete directory, file contents included) |
| `pin add\|rm\|ls [<cid>]` | Manage pins; `ls` shows their sources |
| `pin remote add\|rm <node ID> <cid>` | Remote pinning |
| `gc`, `verify` | Evict all cached fetches; re-check every stored block against its CID |
| `providers <cid>` | Find nodes providing a CID |
| `ping <node ID>` | Open a link and report its round-trip time (`rnprobe` can't probe `rnfs.dht`, which doesn't send delivery proofs) |
| `name create\|rm\|export\|import <alias>`, `name list` | Manage name keys; `rm` releases the name's pin |
| `name publish <alias> <cid>[/path]`, `name resolve <name>` | Mutable names |
| `export <cid>[/path] -o file`, `import file` | Archives |
| `status` | Node ID, known nodes, records held, storage use |

**API** (`127.0.0.1:8788`) and **gateway** (`127.0.0.1:8789`) are separate ports, so pages served by the
gateway can never reach the API.
- Both MUST reject requests whose `Host` header isn't `127.0.0.1:<port>` or `localhost:<port>`, which blocks
  DNS rebinding.
- API requests that change state MUST carry an `X-Rnfs-Api: 1` header, which blocks cross-site requests.
- The API never sends CORS headers.

**Gateway:**
- Paths are `/rnfs/<cid>[/path]` and `/name/<name>[/path]`.
- **Files.**
  - Content-Type comes from the path's file name or `?filename=`, or else from sniffing the content.
  - Range requests are supported.
  - `/rnfs/` responses carry `ETag: "<cid>"` and `Cache-Control: immutable`.
- **Directories.**
  - A directory path without a trailing slash redirects to the slash form.
  - It is served as its `index.html` if present, otherwise as an HTML-escaped listing.
- **Every response** carries `X-Content-Type-Options: nosniff` and `Content-Security-Policy: sandbox
  allow-scripts allow-forms allow-popups; default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob:;
  form-action 'self'`. Pages run in an opaque origin and can't load resources from other servers;
  navigation and WebRTC can still reveal the viewer. A node MAY disable scripts (`gateway_scripts`).
- `GET` responses carry `Access-Control-Allow-Origin: *` only when the request's `Referer` has the gateway's
  own origin (`127.0.0.1:8789` or `localhost:8789`), so pages can `fetch()` files in their own DAG despite
  their opaque origin, while other web sites can't read rnfs content directly. In private mode the header is
  never sent.
- Content fetched for the gateway is cache and is never provided. Names it resolves go only into the capped
  table (section 9).
- Requests that aren't the user's own navigation (`Sec-Fetch-Site` other than `none`) may start at most
  `gateway_fetches` network fetches or name resolutions at a time. Content already stored is served without
  limit.

## 12. Constants

**Protocol constants.** All nodes MUST use these values. `CHUNK_SIZE` and `MAX_CHILDREN` decide CIDs; the
rest decide validity and interoperability.

| Name | Value | Notes |
|---|---|---|
| `CHUNK_SIZE` | 64 KiB | IPFS: 256 KiB. A block is the unit a download resumes from; 64 KiB is ~7 min at 1.2 kbit/s |
| `MAX_CHILDREN` | 256 | A directory node of 256 maximum-length names is ~80 KiB |
| `MAX_BLOCK_SIZE` | 128 KiB | |
| `MAX_DEPTH` | 64 | |
| `K` | 10 | Bucket size, reply size and replication. IPFS: 20. A `/find` reply of 20 IDs fits one packet |
| `PROVIDER_TTL` | 7 days | IPFS: 48 h. Allows infrequent republishing on slow networks |
| `MAX_REQUEST_SIZE` | 1 KiB | Including RNS request framing |
| `MAX_REQUESTS_PER_LINK` | 2 | At most one of them `/blocks` |
| `MAX_RESPONSE_SIZE` | 1 MiB − 1 | Encoded response; one RNS Resource segment |

**Local settings.** Each node may change these.

| Name | Default | Notes |
|---|---|---|
| `slow` | `auto` | `yes`, `no` or `auto` (section 2) |
| `slow_bitrate` | 100 kbit/s | Below this, links and nodes are slow |
| `min_rate_sample` | 16 KiB | Smallest response used for a rate estimate |
| `unmeasured_bitrate` | 100 bit/s | Rate assumed for deadlines before a link has a rate estimate; below typical slow LoRa paths (multi-hop SF12 can be slower: set it lower there) |
| `dht_mode` | `server` | `server` or `client` (section 6) |
| `alpha` | 3; 1 on slow nodes | Parallel lookup queries. IPFS: 10 |
| `beta` | 3 | Closest nodes that must answer before a lookup is complete. IPFS: 3 |
| `republish_interval` | 24 h; 3 days on slow nodes | MUST be < `PROVIDER_TTL / 2` |
| `announce_interval` | 6 h | SHOULD NOT be under 1 h (RNS's per-destination announce rate limit) |
| `startup_announce_delay` | 30 s | Second announce after startup |
| `max_parallel_providers` | 3; 1 on slow nodes | |
| `provider_seniority` | 1 day | Providers in the routing table this long are tried first |
| `response_target` | 60 s | Transfer time a `/blocks` response aims for |
| `deadline_slack` | 30 s | Added to response deadlines; minimum age before a response can be displaced |
| `stall_timeout` | 120 s | |
| `lookup_timeout` | 10 min | |
| `path_timeout` | 30 s | Waiting for an RNS path |
| `max_failures` | 3 | Unanswered queries in a row before a node is dropped; busy refusals in a row before a provider fails |
| `link_idle_timeout` | 5 min | |
| `max_incoming_links` | 128 | |
| `max_blocks_responses` | 4 | `/blocks` responses in progress, in total; half for slow links. At least 2 |
| `max_upload_rate` | none; half the rate estimate on slow links that have one | |
| `max_records` | 100 000 | |
| `ban_time` | 1 h | |
| `max_storage` | 10 GiB | |
| `max_download_size` | none | |
| `max_output_entries` | 100 000 | |
| `name_table_size` | 10 000 | Records kept for names this node hasn't resolved through the CLI or API |
| `remote_pin_quota` | 1 GiB per remote identity | |
| `remote_pin_identities` | empty | Identities allowed to send `/pin` |
| `members` | empty | Private mode's member identities |
| `scan_interval` | 60 s | Shared folder |
| `provide_strategy` | `pinned` | `pinned` or `roots` |
| `gateway_scripts` | on | |
| `gateway_fetches` | 2 | Network fetches at a time for requests that aren't the user's own navigation |

## Appendix A. Comparison with IPFS

**What RNS replaces**

| IPFS / libp2p | rnfs |
|---|---|
| PeerID | Node ID: RNS destination hash of `rnfs.dht` |
| Multiaddrs, transports, NAT traversal, relays, peer routing | RNS routing by destination hash |
| Noise / TLS, peer authentication | RNS Links (encrypted, forward secret); `Link.identify()` |
| Protocol negotiation, protobuf RPC | RNS link requests and msgpack; versions are destination aspects |
| Bitswap | `/blocks` over RNS Resources |
| Bootstrap and peer discovery | RNS announces, plus optional bootstrap node IDs (`daemon --bootstrap`) |
| IPNS records, DHT PUT/GET | RNS announces with signed `app_data`, cached by transport nodes |
| Swarm key (private networks) | RNS Interface Access Codes; rnfs private mode |
| Resource manager limits | RNS request and response size limits, plus rnfs's link and request caps |

**Features**

| IPFS feature | rnfs |
|---|---|
| CIDs | Section 4. One DAG codec (plus raw) instead of several; base32 only |
| UnixFS files and directories | Section 5. Fixed-size chunks and balanced trees, sorted index instead of HAMT sharding; executable bit kept, mtimes and symlinks dropped |
| Kademlia DHT, providers | Section 6. 128-bit keys; `K` 10; servers and clients as in IPFS, with the mode carried in the announce; `beta` termination as in IPFS; one find message; batched publishing with withdrawal; no PUT/GET values |
| DHT routing table refresh, IP diversity filter | Replaced by RNS announces and failure counting; no IP addresses exist to diversify on, so Sybil resistance rests on seniority (section 6) |
| DHT key (SHA-256 of the multihash) | SHA-256 of the whole binary CID, truncated to 128 bits, so a raw block and a DAG node with identical bytes stay apart |
| Reprovider strategies | `pinned` (default), `roots`; `all` dropped to save airtime |
| Bitswap, multi-source and resumable download | Section 7 |
| Pinning, remote pinning, GC | Section 8 |
| CAR import/export | Archives (section 8) |
| IPNS, `key` commands | Names (section 9) |
| DNSLink | Not applicable: RNS has no DNS. A gateway on the Internet could resolve a DNS TXT record to a name, outside this spec |
| MFS / `object patch` | `dir set` / `dir rm`, with names for a mutable root. Unlike MFS, an edit fetches the whole directory, since only complete DAGs can be provided |
| Gateway | Section 11 |
| `ping`, `routing findprovs`, `repo verify`, `dag stat` | `ping`, `providers`, `verify`, `stat` |
| Pubsub | Not included: gossip floods are unaffordable on shared radio interfaces, and RNS has no routed multicast. Use LXMF for messaging |
