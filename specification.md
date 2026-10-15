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
  through RNS's own announce caching, or from *keepers* that store its signed announce; nobody but the owner
  can change it. A node can *follow* a name, keeping its latest target pinned.
- **Sharing.** Content can be sealed for chosen identities (section 5) and linked from LXMF messages with
  `rnfs:` URIs (section 13). Neither changes the protocol.

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
- Through a shared instance, every incoming link is attached to the local shared-instance interface, so the
  interface rules of the airtime controls (below) can't tell the real interfaces apart. A node SHOULD warn
  when interface rules are set while it is attached to a shared instance. Hops and next-hop interfaces for
  provider ranking (section 6) are read through the instance's RPC.
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
  (`Identity.recall`, filled from announces and path responses). A node MUST link only to the `rnfs.dht`
  destination of the recalled identity, and only if its hash is the node ID asked for, so one identity can't
  answer for other node IDs through its other destinations.

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
encoded with msgpack by RNS. Requesters use RNS's RTT-based request timeout (`timeout=None`). RNS stops that
timeout once a response Resource starts arriving, so requests other than `/blocks` (which has stall detection,
section 7) SHOULD also have a hard deadline from when they were sent (rnfs: twice RNS's request timeout), and
pass a `max_response_size` just above their largest honest answer.

| Path | Request | Response | Section |
|---|---|---|---|
| `/find` | `key` | `[[provider IDs], [node IDs]]` | 6 |
| `/provide` | `[[key, …], [withdrawn key, …]]` | `True` if recorded | 6 |
| `/blocks` | `[max_bytes, [cid, …]]` | `[block or None, …]` | 7 |
| `/pin` | `[cid, pin]` | `True` if accepted | 8 |
| `/name` | `name` | `[public_key, name_hash, random_hash, ratchet, signature, app_data]`, or `[]` | 9 |

- Field types: keys, node IDs and names are 16-byte `bin`; CIDs are binary CIDs as `bin`; blocks are `bin`;
  `max_bytes` is a non-negative integer; `pin` is a boolean; every `/name` response field is `bin`.
- Any request of another shape, or one refused under the limits below, is answered `False` at once — never
  left unanswered, except a request over `MAX_REQUEST_SIZE`, which RNS drops unanswered, and a flood over the
  per-link request rate (below), which after a few refusals is dropped unanswered.
- Every request fits in one packet: at most `min(Link.get_mdu(), MAX_REQUEST_SIZE)` bytes including RNS's
  request framing (~28 bytes). Batch sizes are computed from that (e.g. 22 keys per `/provide` at the default
  MTU, counting both lists). A `/provide` carries at most 55 keys, counting both lists (the most that fit
  `MAX_REQUEST_SIZE`); a longer one is malformed.

**Limits.** These keep a node usable under load.
- Requests are at most `MAX_REQUEST_SIZE` (`Destination.set_max_request_size`).
- Per link: at most `MAX_REQUESTS_PER_LINK` requests in progress, of which at most one is `/blocks`.
  Requesters MUST stay within this.
- **Request rate per link.** RNS decrypts and decodes every request packet and starts a thread for it before
  any handler runs, so nothing in rnfs stops that work; a node SHOULD at least keep a flood from costing it
  the handler's work and an answer. rnfs takes a token from a per-link bucket (2000 requests, refilled at 2000
  a second) before parsing anything. A request over it is answered `False` while the link has refusals left
  (4, one more a second), so a requester that gets there backs off as from any refusal, and dropped
  unanswered after that. An honest requester waits for its answers, `MAX_REQUESTS_PER_LINK` at a time, so it
  stays far below this over a real network (over loopback a 10,000-key `/provide` runs at about 900 requests
  a second). Measured over real RNS, the limit cuts the node's CPU per flood request by about 40% (the rest is
  RNS's own work), so the flood rate one link needs to saturate the node about doubles.
- `/blocks` responses in progress are split between two pools: responses on slow links
  (`max_blocks_responses` // 2 slots) and on other links (`fast_blocks_responses` slots, or the rest of
  `max_blocks_responses` if that is more), so each pool has at least one slot. A response stays in the pool it
  started in, even if its link's class changes. Each pool competes only with itself, so neither kind of client
  can crowd out the other. A response in flight holds its encoded size about 2.4 times over in RNS (measured
  with RNS 1.5.6: the encrypted data and its parts), and at most `CHUNK_SIZE` plus one block on a link with no
  rate estimate, so at the defaults the fast pool holds at most about 40 MiB (16 responses of 1 MiB on links
  with a rate estimate; about 7 MiB with none) and the slow pool a few MiB.
  - A response's **deadline** is 3 × its size at the link's rate estimate (at `unmeasured_bitrate` if it has
    none), plus `deadline_slack`. A response not finished by then is cancelled.
  - **Displacement protection is earned by progress**, so a response that holds a slot without transferring
    can't keep honest requests out. A response is protected from displacement only while it is within a
    per-response **grace** the node measures itself, or while it is moving data at or above a **throughput
    floor**. The grace is `base + 2 × (time for the advertisement to cross the path) + queue`, clamped to
    `[grace_min, deadline_slack]`. The advertisement (one packet, `advertisement_bits`) crosses the first hop at
    the receiving interface's configured bitrate (`Link.attached_interface`'s `bitrate`) and each further hop
    of the request's hop count (`Link.expected_hops`, clamped, since a requester can inflate it) at
    `unmeasured_bitrate`, or at `slow_bitrate` if RNS raised the link's MTU above 500, which it does only
    across interfaces that configure their MTU (none of them slow radios); a hop behind a transport node may
    be a LoRa segment however fast the node's own interface is. An unknown bitrate is priced like a further
    hop. Attached to a shared instance, every link arrives on the local interface to it (RNS
    `LocalClientInterface`, recognised as RNS does by its `is_connected_to_shared_instance` attribute), whose
    bitrate says nothing about the path, so the bitrate is unknown there. On the node's own slow interface
    (below `slow_bitrate`), `queue` is one window of parts (10 packets of 500 bytes) at its bitrate for each
    response in flight on it: the advertisement may wait behind them, and RNS can't tell when it actually
    leaves the radio. The grace is never derived from `link.rtt`, which the requester controls. The floor is
    `unmeasured_bitrate / 8` bytes/s, or a small fraction of the interface's bitrate (if known) for a directly
    connected requester.
  - When a pool is full, a new `/blocks` request displaces one of the pool's responses, never one on its own
    link: the response making least progress among those that are unprotected (past their grace and below the
    floor); else, if the newcomer's receiving interface holds no slot in the pool, the oldest response with
    nothing sent from an interface that holds two or more, once it is past half its grace and at most one
    within a grace from each interface; else, if neither the newcomer's link nor its identity holds a slot in
    the pool, the response with the lowest throughput, if it is older than `deadline_slack` and past its
    grace. Otherwise the request is refused.
  - A response that makes **no progress** is cancelled and its budget charge refunded: one with nothing sent,
    once its grace plus `deadline_slack` has passed; any response with no progress for `stall_timeout`.
  - **Cooldown.** A link (and its identity, if it identified) whose response was displaced or cancelled for
    making no progress, or whose response Resource it rejected before any of it was sent (no honest requester
    does: otherwise one could have the node build a response, reject it and ask again in a loop), is refused
    new `/blocks` for `blocks_cooldown`. A response displaced as the slowest counts as making no progress if
    it was moving at less than a quarter of the rate it needs to be done when due: by its deadline, or, on a
    link with no rate estimate whose MTU RNS raised, by the deadline it would have at `slow_bitrate`. So a
    requester taking a little of each response can't keep its slots young by displacing its own responses
    from a spare link; an honest one keeping to its schedule is displaced without a cooldown. The refusal is
    answered at once, before any slot is reserved or block read.
  - A handler gets no direct handle on the response Resource RNS sends for it. Responses are tracked through
    the link's outgoing Resources (`Link.outgoing_resources`, matched by `request_id`) and cancelled with
    `Resource.cancel()` (which acts only while the Resource is not yet complete). These are RNS internals that
    may change between versions; if they're unavailable, a response is cancelled by closing its link.
- At most `max_incoming_links` incoming links. When full, one is closed to make room: the link idle longest
  with no request in progress, or else a random one with no `/blocks` response in progress. In private mode,
  links not identified as a member's are chosen first.
- **Half-open links.** RNS validates every link request (a key exchange, a proof, a watchdog thread) and keeps
  the link waiting for the initiator's RTT packet for `6 s × hops + 360 s`; only then is it established and
  counted above. So, on `rnfs.dht` and on the LXMF delivery destination, links still waiting (RNS `PENDING`
  or `HANDSHAKE`) are counted in a bucket of their own, also of `max_incoming_links`. When a link request
  arrives with the bucket full, the oldest waiting link on the interface holding the most of them (the
  request's own interface on a tie) is closed, rather than the request refused. So a flood can't fill the
  bucket and keep everyone out, and a flood arriving on one interface closes its own links first. It does
  shorten every handshake on that interface: at R link requests a second, a link waiting there is closed after
  about `max_incoming_links` / R seconds (1.3 s at 100 a second, at the default), so an honest initiator
  reaching the node over the same interface whose handshake takes longer is cut off (RNS tells it at once, and
  it may try again), while initiators on other interfaces are not affected. Each request still costs the node
  the key exchange and a proof; RNS's own long responder timeout is the root cause.
- rnfs neither sends nor accepts compressed Resources. Handlers are registered with `auto_compress=False`,
  and nodes MUST set RNS's maximum decompressed Resource size (`RNS.Resource.AUTO_COMPRESS_MAX_SIZE` in
  RNS 1.5.6) to 0, since RNS decompresses before any rnfs check runs. Setting it to 0 also stops RNS
  compressing what the node sends.
- RNS checks request and response size limits against a Resource's data size but allocates by its transfer
  size, so a node SHOULD reject, before accepting it, any incoming Resource advertisement that is a request
  (rnfs requests always fit one packet), or whose transfer size exceeds its data size or `MAX_RESPONSE_SIZE`
  by more than the encryption overhead (rnfs: 128 bytes). RNS checks those limits per segment, so on
  `rnfs.dht` links (either end) a node SHOULD also reject any advertisement of more than one segment: every
  rnfs response fits one (`MAX_RESPONSE_SIZE`). On `rnfs.dht` links it SHOULD also reject one with metadata,
  which RNS decodes as the Resource completes and rnfs responses never carry. Other protocols' Resources in
  the same process (LXMF) are not affected.
- RNS decodes a whole response with msgpack before handing it over, and one-byte empty arrays or maps decode
  to some 70 times their size in objects (a 1 MiB response: ~70 MB and ~0.3 s of CPU). So on `rnfs.dht`
  links a node SHOULD check a response's msgpack headers before RNS decodes it, without building objects, and
  refuse one that can't be an rnfs response: anything but `[request_id, response]` with a 16-byte `bin`
  request ID and a response that is `True`, `False`, `nil`, a `/find` answer, or a list of at most 26 entries
  (the most CIDs a `/blocks` request can carry), each a `bin` of at most `MAX_BLOCK_SIZE` or `nil`, with
  nothing after it. rnfs checks responses sent as a Resource, and those sent as one packet larger than RNS's
  default MTU (only links that negotiated a larger MTU carry them; smaller ones decode to little). A refused
  response fails its request as a malformed one does. A responder sends no requests, so a response packet on
  an incoming `rnfs.dht` link is dropped unread.
- `max_upload_rate` caps bytes sent for `/blocks` on a link once it has a rate estimate. RNS paces a Resource
  by its receiver, so the cap is applied between responses: the response budget uses the lower of the rate
  estimate and `max_upload_rate`, and the next response on that link starts no sooner than
  `bytes × 8 / max_upload_rate` seconds after the previous one started.
  - A responder holds an accepted request back for this at most 5.625 s (half of RNS's request timeout
    without RTT, 1.125 × `RNS.Resource.RESPONSE_MAX_GRACE_TIME` (10 s), so the requester doesn't give up
    meanwhile; not RTT-based, since the requester can inflate the RTT), and refuses it at once if it would
    have to wait longer.
  - So a requester on a slow link SHOULD, after a response, wait as long again as that request took before
    sending its next `/blocks` to that node (at the default cap, half the rate estimate, the next response may
    start twice its transfer time after the last one started).

**Airtime controls.** A node MAY limit the airtime it spends answering `/blocks` with the settings below.
Each limit answers `/blocks` with `False`, which the requester treats as busy (section 6), moving on to
another provider after `max_failures` refusals in a row. These limits MUST NOT apply to `/find`, `/provide`,
`/pin` or `/name`, whose answers are tiny and which the DHT needs, and don't affect announces. A request
refused by them SHOULD be answered at once, before a response slot is reserved or any block is read.
- **Interfaces** (`serve_interfaces`, `no_serve_interfaces`): interface names or name prefixes, matched
  without regard to case against the configured name and `str()` of the interface the request's link is
  attached to (`Link.attached_interface`), and of its parent for spawned interfaces such as TCP server
  clients. A request on an interface matched by `no_serve_interfaces` MUST be refused. If
  `serve_interfaces` is not empty, a request on an interface it doesn't match, or on an unknown one, MUST be
  refused.
- **Quiet hours** (`quiet_hours`): one or more local-time ranges, `HH:MM-HH:MM[,HH:MM-HH:MM]`, start
  inclusive and end exclusive; a range may wrap past midnight. During a range `/blocks` MUST be refused.
- **Upload budget** (`upload_budget`, bytes per hour, node-wide): a token bucket of `upload_budget` bytes,
  refilled continuously at `upload_budget / 3600` bytes per second, starting full.
  - A response carrying at least one block costs its packed size, **charged as it is sent** (from its
    Resource's progress), not up front. It is allowed if the bucket holds at least that size beyond what the
    responses in flight are still committed to send, or if the bucket is full and nothing is committed, so one
    honest response larger than what is left is delayed but never starved; the bucket may go into debt this
    way. A response refused for its size is not charged.
  - **Reservation and commitment.** When it is allowed, a response is charged a reservation of at most one
    packet (the link's MDU: its advertisement). The rest of it counts as committed, when admitting other
    responses, while it is within its grace (Displacement, above), so responses admitted together can't
    overrun the bucket. After its grace a response commits at most what it sends at its progress rate before
    its deadline, and nothing while it is below its floor (or has sent nothing): responses a requester never
    downloads, or crawls through, hold at most their reservations, so a few of them can't hold admission while
    spending nothing. A single-packet response, and one whose progress can't be followed, is charged in full
    when it is allowed.
  - **Per requester.** One requester (its identity; each unidentified link counts as its own) may have
    responses in flight costing at most half the bucket's capacity. Its response's block budget is cut to
    what is left of that share (the first block is always included, section 7), and a requester at its share
    is refused. So with a full bucket one response is at most about `upload_budget / 2`, and one requester
    can't size its responses to take the whole budget at once. Identities are free, so this shares the bucket
    among requesters rather than bounding a determined one; what keeps a requester that withholds downloads
    from holding the budget is that only bytes sent are charged.
  - **Settlement.** When a response concludes (it completed, failed, was cancelled, missed its deadline, was
    displaced, never started, or its link closed), its charge is made final: its size if it completed or its
    progress can't be read, else its progress times its size. A reservation it didn't use is given back.
  - While the bucket is not full, or responses in flight are committed, the response's block budget (section
    7) is cut to what the bucket holds less what is committed, less 1 KiB.
  - A request arriving while the bucket is empty or in debt is refused before its response is built.
  - Responses of only `None` entries are not charged.

**Private mode.** RNS Interface Access Codes keep outsiders off an interface. To restrict rnfs itself
across shared interfaces and transport nodes, a node MAY run in private mode:
- every link identifies;
- handlers answer `False` to identities not on the member list (checked inside the handler, since RNS's
  `ALLOW_LIST` drops refused requests silently);
- the routing table admits only members;
- provider records are kept and handed out only for members: at start the node drops the records of every
  publisher whose node ID it can't tie to a member identity (stored while it was public, or by a member since
  removed from the list), and `/find` replies leave out any such record;
- outgoing links go only to nodes whose identity is a member, so the node never identifies itself to an
  outsider (one listed as a provider anywhere, say).

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
- **kind** of a directory entry: `0` file, `1` executable file, `2` directory. On systems without an execute
  bit (Windows), add records every file as kind 0.
- **Building a tree** (writers MUST follow this, so the same content always has the same CID):
  1. Start with the list of children: a file's chunks (consecutive `CHUNK_SIZE` pieces, the last possibly
     shorter but never empty; or FastCDC chunks, below), or a directory's sorted entries.
  2. While the list has more than `MAX_CHILDREN` children:
     - split it into consecutive groups of `MAX_CHILDREN` (the last group may be smaller);
     - make one node per group (files: type 1; directories: type 2 for groups of entries, type 3 above that);
     - continue with the list of children pointing at those nodes.
  3. The final list becomes the root node.

  Special cases: a file with one chunk is just that raw block; an empty file is a zero-byte raw block; an
  empty directory is `[2, []]`.
- **Content-defined chunking** (optional, `add --chunker fastcdc`). A writer MAY cut a file's chunks with
  FastCDC instead of consecutive `CHUNK_SIZE` pieces. Only the chunk list of step 1 changes; steps 2 and 3
  and the special cases are the same, and readers need no change (the reader checks accept any raw chunk of
  1 to `MAX_BLOCK_SIZE` bytes). Writers using FastCDC MUST cut exactly as follows, so the same content always
  gets the same CID:
  - `MIN_SIZE` = 16 KiB, `AVG_SIZE` = 64 KiB, `MAX_SIZE` = 128 KiB (= `MAX_BLOCK_SIZE`).
  - `GEAR[i]` for i = 0…255 is the first 8 bytes, read big-endian, of SHA-256 of the ASCII string
    `rnfs-fastcdc-gear` followed by the single byte i (`GEAR[0]` = `0x61304b5b3811834c`).
  - `MASK_S` = `0xFFFFC00000000000` (the top 18 bits), `MASK_L` = `0xFFFC000000000000` (the top 14 bits).
  - Each chunk starts where the previous one ended (the first at 0). If at most `MIN_SIZE` bytes remain, they
    are the last chunk. Otherwise let h = 0 and, for chunk offsets i = `MIN_SIZE`, `MIN_SIZE` + 1, … below
    min(`MAX_SIZE`, bytes remaining): h = ((h << 1) + `GEAR[byte i]`) mod 2^64; the chunk ends after byte i
    if h & `MASK_S` = 0 when i < `AVG_SIZE`, or h & `MASK_L` = 0 when i ≥ `AVG_SIZE`. If no offset matches,
    the chunk is `MAX_SIZE` bytes (or all that remains).
  - Test vector: the 1 MiB made by concatenating SHA-256(`rnfs-chunker-test` ‖ uint32-BE(n)) for
    n = 0, 1, … (truncated to 1 048 576 bytes) cuts into chunks of 68012, 67020, 76641, 76118, 65772, 99649,
    18749, 68089, 81228, 74064, 66952, 86699, 75467, 67594, 56522 bytes; its CID is
    `bagaibqabciqmb3zqfyjuzmxwezantnxahoiwklo2i7b7lamhpv4psa3lhbzg6da` (fixed chunking:
    `bagaibqabciqalfywj6ff5m6wfytxzirtijmhx6licv3utbiq2d5wzyzzcide3kq`).

  The same file added with different chunkers gets different CIDs. FastCDC keeps most chunk boundaries when
  bytes are inserted or removed, so an edited file shares most of its blocks with the old version.
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

### Sealed files (a content convention)

A *sealed file* is a file whose content is encrypted for chosen RNS identities (`add --to`). To the rest of
rnfs it is an ordinary file: CIDs, the DHT, transfer, pinning and storage treat it like any other bytes.
Readers recognise it by its first bytes. Version 1:

```
sealed file = prefix || segment_0 || … || segment_(n-1)          n ≥ 1
prefix      = MAGIC || VERSION || header_length || header
  MAGIC         = "rnfs-sealed\n" (12 bytes: 72 6e 66 73 2d 73 65 61 6c 65 64 0a)
  VERSION       = 0x01 (1 byte)
  header_length = 4 bytes, big-endian, 1 … 65536
  header        = msgpack [segment_size, [[identity_hash, wrapped_key], …], sealed_name]
segment_i   = iv || AES-256-CBC(enc_key, iv, PKCS7(plaintext_i)) || tag_i
  iv            = 16 random bytes
  tag_i         = HMAC-SHA256(mac_key, SHA-256(prefix) || uint64-BE(i) || final_i || iv || ciphertext_i)
  final_i       = 0x01 for the last segment, 0x00 for every other (1 byte)
```

- **Header.** msgpack with exactly these types: `segment_size` an integer, which MUST be 65536 in version 1;
  `identity_hash` `bin` of 16 bytes (the recipient's RNS identity hash, SHA-256 of its 64-byte public key
  truncated to 16 bytes); `wrapped_key` `bin` of 128 bytes; `sealed_name` `bin` or nil. Entries are sorted by
  `identity_hash` bytewise, unique, 1 to 256 of them. The header MUST re-encode to identical bytes (shortest
  forms), as for DAG nodes.
- **Keys.** The writer draws a random 32-byte content key K. `mac_key` ‖ `enc_key` = HKDF-SHA256(input K, no
  salt, info `rnfs-seal-v1 segments`, 64 bytes): the first 32 bytes are `mac_key`, the last 32 `enc_key`.
  The name's keys are derived the same way with info `rnfs-seal-v1 name`.
- **Wrapped key** = `RNS.Identity.encrypt(K)` to the recipient's public key: a 32-byte ephemeral X25519
  public key, then RNS's token (16-byte IV, AES-256-CBC of PKCS7(K), HMAC-SHA256) keyed by
  HKDF-SHA256(X25519 shared secret, salt = the recipient's identity hash, 64 bytes). Ratchets are not used:
  files outlive ratchets.
- **Segments.** Every segment but the last holds exactly 65536 plaintext bytes; the last holds 1 to 65536
  bytes, or 0 when the whole file is empty. A full segment is therefore 65600 bytes. Readers compute n from
  the stored size: n = ⌈(size − prefix length) / 65600⌉; the last segment's length must be at least 64 and a
  multiple of 16 plus 48, else the file is malformed.
- **Sealed name** (optional) = iv ‖ AES-256-CBC(name enc key, iv, PKCS7(UTF-8 name)) ‖ HMAC-SHA256(name mac
  key, iv ‖ ciphertext). A sealed name whose tag doesn't verify, that isn't valid UTF-8, or that breaks the
  name rules above is ignored as invalid: the file reads as one without a name.
- **Reading.** A reader finds the entry with its own identity hash, unwraps K with its private key, and MUST
  check each segment's tag (constant-time compare) before decrypting it, and that the padding is valid PKCS7.
  A segment failing either, a non-last segment that isn't 65536 bytes, a last segment of 0 bytes in a
  non-empty file, or one of more than 65536 bytes (PKCS7 lets a writer fit up to 65551) makes the file
  invalid. Binding the prefix hash, the index and the final flag into each tag
  detects any change to the header, reordering, splicing from another file, and truncation (the new last
  segment's tag was made with final = 0). The plaintext size is (n − 1) · 65536 plus the last segment's
  plaintext length, so finding it decrypts the last segment.
- **Ranges.** A plaintext range [o, o + l) needs only segments ⌊o / 65536⌋ … ⌊(o + l − 1) / 65536⌋.
- **What it protects.** The confidentiality of the content and, if sealed, of the file name. It does not hide
  the recipients' identity hashes, the approximate size, or (with `--wrap`) the wrapper's entry name, and it
  says nothing about who sealed it: anyone with a recipient's public key can seal a file for them, and any
  recipient can seal different content for the others.
- `add --to` seals single files only: sealing each file of a directory would leave its names and tree
  visible. Sealing is randomised, so a sealed file's CID can't be predicted (`--only-hash` is refused).
- **Gateways MUST NOT decrypt** sealed files (the gateway is reachable by web pages); they serve the stored
  bytes as `application/octet-stream`.

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
  should stay servers. A node that knows no servers can still fetch from nodes it has an open link to (ranked
  by the same keys as providers, below, most recently used breaking ties), or with `get --from`.

**Routing table.** Bucket `i` (0–127) holds up to `K` servers at distance `[2^i, 2^(i+1))` from our own ID.
- A node is added only once it has proven to be a reachable server: we heard its announce with server mode,
  or it answered a `/find`. IDs merely mentioned in a reply are not stored.
- A full bucket keeps its nodes. A newcomer replaces the least recently heard node only if that node failed a
  query since it was last heard, or has been silent for over `2 × announce_interval`. Long-lived nodes are
  more reliable and harder for an attacker to displace.
- A node is dropped after `max_failures` queries in a row go unanswered; hearing from it resets the count.
  Lossy links can drop a live server this way, so a dropped node's first-seen time is kept for
  `2 × announce_interval`, and it regains its seniority if it comes back within that time.
- **Joining.** At startup a node sends `/find` for its own node ID to bootstrap nodes, one at a time, until
  two have answered: first those it was given (`bootstrap_nodes`, `daemon --bootstrap`) in their order,
  then, unless `default_bootstrap` is off or it is in private mode, the implementation's default bootstrap
  nodes (rnfs: `DEFAULT_BOOTSTRAP_NODES`) in random order. It adds those that answer, then runs a lookup for
  its own ID from the servers they listed and its routing table, without asking the contacted nodes again;
  this fills its table with its neighbours.
  - Bootstrap nodes are contacted outside the lookup's budget of `alpha` unknown IDs, so unreachable ones
    don't stop the rest being tried.
  - A node behind an RNS access-point interface hears no announces from beyond it, so bootstrap nodes are
    its only way in.
  - While fewer than 3 servers are known that `/find` replies would include (not failed since last heard,
    nor silent for over `2 × announce_interval`), the node joins again, 5 minutes after the last join and
    doubling up to `announce_interval`: on radio every path request is flooded. A node that knows only a
    small island of servers still contacts the defaults at every start.
  - Bootstrap nodes get no special trust: once answered, they are ordinary routing-table entries.
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
- **Choosing providers.** A node SHOULD rank providers by these keys, in this order:
  1. **Seniority.** Providers that have been in the routing table for over `provider_seniority` come first.
     This key MUST stay first: an attacker can create fresh identities that sit close in hops, but can't make
     them old.
  2. **Hops.** Fewer hops (`RNS.Transport.hops_to`) first; a provider whose path is unknown goes after every
     provider with a known path.
  3. **Next-hop interface bitrate.** A faster outbound interface on the path first
     (`RNS.Transport.next_hop_interface(id).bitrate`; through a shared instance, the bitrate of the interface
     `Reticulum.get_next_hop_if_name` names); an unknown bitrate goes last.
  4. **Reply order.**

  With no hops or bitrates known, this is seniority, then reply order. Path information is read once per
  provider per fetch, when the provider is found.

  A provider *fails* when its link fails, it answers `None` for a block of the root it advertised, it answers
  `False` (busy or refused) `max_failures` times in a row, or it stalls twice (section 7). A `False` is retried
  after a short delay. A `False` soon after the provider served a response (within 4 × that request's round
  trip, counted from when it was sent) is taken as upload pacing (section 3) and not counted; it is retried
  after an eighth of that round trip, or the usual delay if longer. Failed providers are removed and not
  retried during that fetch. When no providers are left, `/find` runs again for a fresh random half, within the
  same `lookup_timeout`.

**Publishing.**
- For each key, find the `K` closest nodes. Then send each node the keys it should store, in as few `/provide`
  requests as the batch size allows (section 3), one at a time over a separate identified link closed
  afterwards.
- `/provide` is answered `True` if the keys were recorded, or `False` if the request was refused (the link
  isn't identified, or a section 3 limit applied). Apart from the one re-identify retry of section 3, a
  refused or failed store isn't retried until the next republish. A responder may have recorded some keys of
  a request it refused (e.g. up to a local limit), so the publisher SHOULD count every key it sent to that
  node, up to and including that request, as possibly recorded there.
- To stop providing a CID (unpinned, deleted), a node SHOULD send it as a withdrawn key to the nodes it last
  published it to, and to nodes of earlier rounds (or of a round cut short, including by a crash) whose record
  may not have expired: the closest nodes change as nodes come and go. So it SHOULD persist where a round
  stored as the round goes, not only when it ends (rnfs: every 10 nodes or 30 s).
- A responder applies a `/provide`'s withdrawals before its keys, so a key in both lists is stored. A
  publisher MUST NOT send a withdrawal of a key in a later request to the same node than a store of it (that
  would delete what it just stored): it sends withdrawals in the first batches, doesn't withdraw a key it
  is providing to that node in the same round, and cancels the withdrawals still queued for a key when it
  provides that key again (it may stop and start providing it while a round is sending).
- A slow node SHOULD pace its closest-nodes lookups, since each is several `/find` exchanges at radio rates
  (rnfs: at most 10 per publish round, and after a round of n lookups the next waits 30 × n s; items never
  published go first, then those unpublished longest). Withdrawals are not delayed by this.

**Provider records.**
- `/provide` records the identified requester's node ID as a provider; a node can only advertise itself.
  Records carry no addresses, since RNS routes by node ID.
- A record expires `PROVIDER_TTL` after it was last stored, keeps its first-stored time across republishes,
  and is removed on withdrawal. These times are wall-clock and persisted; one later than the current time (a
  clock since corrected backwards) counts as now, so a record can't outlive its TTL.
- A node keeps at most `max_records`, a tenth of them in a *newcomer pool*. When the rest is full, a
  publisher first seen before the most recent one there evicts a record of that most recent publisher (the
  one it stored least recently), so a flood of new identities can't displace older publishers. Any other
  publisher (one not seen before, or the most recent one) stores into the pool, evicting a pooled record
  chosen at random when the pool is full, but never one of its own: so a new publisher can always store
  something, and a flood churns only the pool. A pooled record leaves the pool when it is republished while
  the rest has room. The node's own records are always accepted.
- First-seen times are persisted, and kept for `PROVIDER_TTL` after a publisher's last record is gone
  (rnfs: for publishers seen for at least `provider_seniority`, at most `max_records / 10` of them, the
  oldest departure forgotten first), so a publisher that was away for a while isn't new again.
- A node SHOULD limit how many records one publisher holds, its own excepted (rnfs: `max_records / 100`, at
  least 10, once the publisher has been seen for `provider_seniority`, and a tenth of that, at least 10,
  before), so one identity can't fill the store and a fresh one gets little.
- Providers republish every `republish_interval`, which MUST be less than `PROVIDER_TTL / 2`. Providers also
  record themselves locally. Keys that aren't CIDs' (kept names, section 9) are republished every
  max(`republish_interval`, 3 days), each with a fixed jitter of ±10% derived from the key, so many keys
  don't come due at once; the jittered interval stays below `PROVIDER_TTL / 2`.
- **Provide strategy** (`provide_strategy`):
  - `roots` (default) provides every complete pin, and every complete fetched target while it is cached
    (so at most `max_storage` of downloads at a time). That is every complete download, whatever the command:
    `get`, `cat`, and even `stat`, `info` or `ls` of another node's content when what they fetched is the
    whole target (a small file is one block). Anyone can look up the provider records, or ask the node for
    the blocks (section 7), and so learn what it fetched.
  - `pinned` provides every complete pin only, so downloads are never published or served.

  `roots` helps popular content spread; `pinned` uses less airtime and doesn't publish what a user
  downloads. Gateway fetches are never provided under either.
  Nothing else inside a DAG is provided separately: share `<root>/<path>`, or pin the inner CID to provide
  it. Keepers also provide the keys of the names they keep (section 9), under any strategy.

## 7. Transfer

**`/blocks`.**
- **Request:** `[max_bytes, [cid, …]]`, with as many CIDs as fit in one packet. The CIDs MUST be distinct; a
  request that repeats a CID is malformed and refused, so a node can't be made to read and serve the same
  block several times in one response.
- **Response:** a list in request order, one entry per CID until the budget is reached. Each entry is the
  block, or `None` if the node doesn't hold it or doesn't serve it. Entries after the last returned were not
  sent because of the budget.
- **Budget:** the first served block is always included, even if it exceeds `max_bytes`. After it, a block is
  added only if the response's total block bytes stay within:
  - `max_bytes`;
  - `MAX_RESPONSE_SIZE − 1 KiB`, so the encoded response stays within `MAX_RESPONSE_SIZE`;
  - if the link has a rate estimate, `response_target` of transfer at the lower of that estimate and
    `max_upload_rate`; if it has none, `CHUNK_SIZE` (what a requester asks for then, enforced);
  - with an upload budget whose bucket isn't full or with responses in flight committed, what the bucket holds
    less what they are committed to send, less 1 KiB, and in any case what is left of the requester's half
    share (section 3, Airtime controls).
- **What a node serves:** only blocks in the DAG of a CID it currently provides (the same reachability GC
  computes). With `pinned`, cached downloads answer `None`, so a node's download history can't be read by
  asking for it. With the default `roots`, complete cached downloads are provided and served, so they can.
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
5. Fetching a target's whole DAG (`get`, `pin add`, `export`, `dir set` and `dir rm`, a followed name's new
   target, the gateway's archives, remote pins and LXMF auto-fetches) is bounded by what its verified size can
   hold. The bytes of its distinct blocks, stored or downloaded, may not exceed the size, plus 64 bytes per
   4 KiB of it (rounded up), plus 1 KiB, plus 1 KiB per directory entry or index child met (at most
   `max_output_entries` of them); a remote pin's or auto-fetch's quota applies too, whichever is lower. A file
   node may list children of size 0, so without this a file that declares 1 byte could hold any number of
   distinct nodes. Every DAG built by the tree rule (section 5) stays far inside the bound: its file nodes take
   under 13 bytes per 4 KiB of content, and each entry, with the nodes above it and the levels of a file
   below it, under 1 KiB. A fetch that would exceed it fails ("too large") without marking the root invalid.
   While the bound applies, a requester asks only for blocks that fit the room it leaves, counting a DAG node
   not yet received as `MAX_BLOCK_SIZE` (under the size-derived bound, a directory or index node as its header
   only, since its entries bring more room than they take; under a quota, in full), and always for at least
   one block when none is in flight, so a fetch downloads at most about one block beyond either. Byte-range reads are lazy and skip empty children instead.
6. Spread requests over up to `max_parallel_providers` providers (1 on slow nodes, where parallel transfers
   only contend for the same channel). A response *stalls* if it makes no progress for `stall_timeout`, or
   misses the same deadline the responder uses (section 3). A stalled provider's link is closed, which cancels
   the transfer, and its blocks are requested again from another provider; providers that keep failing are
   handled as in section 6. A fetch that fails or is cancelled closes the links of its requests still in
   flight, so their responses don't keep using the link and hold up the next `/blocks` to those nodes.
7. Blocks are stored as they arrive, so an interrupted fetch resumes where it stopped. An RNS Resource is only
   delivered when complete, so a response lost mid-transfer yields nothing and its blocks are requested again.
8. When the target is complete, the node provides it if its strategy says so (section 6).

## 8. Storage

- Blocks are stored in `<rnfs dir>/blocks/`, one file per CID, written atomically after verification, and
  synced to disk before any pin or cache record that refers to them is saved, so a power loss can't leave a
  pin without its blocks or an empty file under a CID name.

**Pins.**
- Pins are recursive: a pinned CID keeps every block of its DAG.
- A pin is held by one or more *sources*: `user`, `folder`, `name:<alias>`, `remote:<identity>`,
  `follow:<name>` (the latest target of a followed name, section 9) and `lxmf:<address>` (links auto-fetched
  from that LXMF sender, section 13); names, identities and addresses are 32 hex characters. It lasts while
  any source holds it.
- `pin rm` removes only the `user` source. Other sources are released by whatever created them: the folder
  scan; `name publish` or `name rm`; `/pin` with `false` or a remote quota being exceeded; `name unfollow`, or
  a followed name's newer target once that is complete; removing the LXMF messages that hold it.
- Pins are saved at once; the state that records why a source holds a pin (`names.json`, `follow.json`, the
  LXMF inbox) is saved separately, so a crash can leave them disagreeing. At start the node makes `name:` pins
  match `names.json` (pinning a recorded target again, releasing the rest), and releases `follow:` pins of
  names no longer followed and `lxmf:` pins no saved inbox message records (a queued auto-fetch is fetched
  again). A followed name's check releases every other target its source holds.
- A pin is provided only once it is complete. Blocks of fetches, adds and imports in progress, and of queued or
  running jobs (section 11), are protected as if pinned.

**Cache** is everything else.
- Each cached fetch is recorded as the `<root>/<path>` it fetched, which keeps the path's nodes as well as the
  target's DAG.
- When storage exceeds `max_storage`, cached fetches are evicted least recently used first; `gc` evicts them
  all.
- `add --no-pin` stores what it adds as a cached fetch of its CID instead of pinning it: evicted least recently
  used like any other, removed by `gc`, never provided under `pinned`, and under `roots` provided while it is
  cached (it counts as a target the user fetched).
- A block is deleted only if no pin and no remaining cached fetch reaches it. A stored DAG node that no
  longer matches its CID hides what it reaches, so while anything kept reaches one, nothing is deleted
  (`verify` removes it, or a fetch replaces it).
- The node stops providing anything it no longer holds completely, and withdraws it (section 6).
- Pins are never evicted. `max_storage` never refuses a fetch: while one runs, other cached fetches are
  evicted to make room, and if nothing is evictable (e.g. pins alone fill `max_storage`) storage goes above it.
  Once the fetch ends, eviction brings storage back to `max_storage`, which may evict what was just fetched.
- The disk is the hard bound: a fetch (pinned or not) is refused if what it still has to store — its verified
  size, or the range's size, before downloading, then each response — would leave less than
  max(256 MiB, 2% of the filesystem) free on the filesystem holding `<rnfs dir>/blocks/`.
- `get` and `export` write their output only if it leaves the same reserve (of the output's filesystem) free.
  When that is the filesystem holding the blocks, what `get` still has to fetch counts too: its verified size
  less the bytes of the target's DAG already stored.

**Shared folder.** Every `scan_interval`, files and directories in `<rnfs dir>/files/` are imported as with
`add` (copied into the block store) and pinned with source `folder`. A changed or removed file releases its
old pin.

**Archives** move content without a network (the equivalent of IPFS CAR files).
- `export` fetches the target if needed, then writes a msgpack stream: `["rnfs-archive", 1, [root CIDs]]`,
  then `[cid, block]` for every block of every root's DAG, depth-first from each root, each block once. For
  `export <cid>/<path>`, the archive's root is the target.
- `import` verifies every block, including blocks already stored, pins with source `user` each root whose
  every block was in the archive, and reports the other roots. A root is never pinned on the strength of
  blocks already held locally, so an archive listing guessed CIDs can't turn cached downloads into provided
  pins. An archive lists at most 1024 roots. Like a fetch, an import stops with "not enough free disk space"
  when a block not yet stored would leave less than the disk reserve free; blocks stored before are cache.

**Remote pinning.**
- A node MAY accept `/pin` from identities on its remote-pin list.
  - `[cid, true]` is answered `True` once accepted. The node then fetches the CID and pins it with source
    `remote:<identity>`.
  - `[cid, false]` releases that source.
- Each remote source has a storage quota, `remote_pin_quota`, counting the full size of every CID it pins. A
  pin that would exceed it is dropped, as soon as the CID's verified size shows it. A source's pins are
  fetched one at a time, in the order accepted, so each is checked with the earlier ones counted; a node MAY
  refuse `/pin` from a source with many pins waiting (rnfs: 64).
- The requester learns that a remote pin is complete when the node shows up as a provider (`providers`).
- `/pin` is best effort: an accepted pin may still be dropped (quota, or the CID can't be found). rnfs saves the
  pins waiting (`state/remote_pins.json`, about once a second and at stop) and resumes them after a restart,
  unless their identity has left `remote_pin_identities`; a crash may lose those accepted in the last second.
- `rnfs pin remote add|rm <node ID> <cid>` sends these requests over an identified link.

## 9. Names

A **name** is a mutable pointer to a CID, owned by a key pair: the IPNS equivalent.

**Name keys.**
- Each name has its own RNS identity, stored at `<rnfs dir>/names/<alias>`.
- The name is the hash of that identity's `rnfs.name` destination: 16 bytes, shown as 32 hex characters.
- A separate key per name means a name can be moved to another machine by exporting its key, and that the name,
  its announces and its signed records carry nothing that identifies the node or the node's other names.
- Publishing a name does link it to the node, as providing content does in IPFS (a provider record reveals
  the PeerID providing a CID). The owner keeps its own names (Keepers, below), so it provides each name's key
  under its node ID, usually with the oldest record; and it pins and provides each name's target. Anyone who
  knows a name can look up its keepers and its target's providers and find the owner's node ID there, so the
  names one node publishes can be linked to that node and to each other. Only names published from different
  nodes are unlinked.

**Record.**
- The name destination's announce `app_data` is `[sequence, cid]` (msgpack: unsigned integer, binary CID).
  RNS signs it as part of the announce.
- The publisher MUST set it with `Destination.set_default_app_data()`, so the announces RNS sends in answer to
  path requests carry it too.

**Publish** (`name publish <alias> <cid>[/path]`):
1. Resolve any path to a CID locally.
2. Pin the target with source `name:<alias>`.
3. Set the record with `sequence` one higher than both the publisher's stored sequence and the highest it can
   resolve, and persist it. If that fails, the publish fails and a pin step 2 added is released.
4. Release that source from the previous target (only now, so a crash leaves the target the persisted record
   names pinned; any other pin of the source is released at the next start, section 8).
5. Announce, and re-announce every `announce_interval`.

Before the first publish of a key imported from elsewhere, or found without stored state, the node resolves
the name through the network and asks keepers (below), since the key's earlier records may be held only there.

**Resolve** (`name resolve <name>`):
- The daemon registers one announce handler for `rnfs.name` (with `receive_path_responses=True`).
- To refresh, call `Transport.request_path(name)`, wait up to `path_timeout`, then read
  `Identity.recall_app_data(name)`.
- If the path request heard nothing within `path_timeout` (or always, with `keeper_query = always`), ask
  keepers (below). Storing a provider record is free, so neither a record's age nor the order a reply lists it
  in may decide which keepers are asked:
  - Find providers of the name's key (section 6). Providers recorded locally are candidates, never a reason
    to skip the search, which runs until 3 × `alpha` are known and more than one source listed some (this
    node's own records count as one source), or until the lookup is complete.
  - Rank the candidates by routing-table seniority, as providers are (section 6): servers in the routing table
    for over `provider_seniority` that haven't failed a query since last heard nor been silent for
    2 × `announce_interval` first, then other such servers, then servers that failed a query or went silent
    (an owner that has left), then IDs not in the table; at random within each group.
  - Ask them with `/name`, a batch of `alpha` at a time, and offer every verifying record to the sequence
    rules, so the highest valid sequence wins. Ask another batch while no answer reaches the highest sequence
    already known for the name (from an announce, an earlier resolve or a kept record), or while fewer than two
    keepers have answered; at most 3 batches. A slow node (`alpha` 1) asks at most 2 batches, and asks the
    second for want of a second answer only when it knows no sequence to compare the first against.

  A keeper query (a DHT lookup plus a few requests) costs roughly ten to twenty times the airtime of a
  resolve a path response answers, so `always` SHOULD NOT be used on slow links.
- Return the record with the highest sequence held, and how long ago it was heard (for a record from a
  keeper, from its signed emission time, never in the future).

**Sequence.** RNS stores any validly signed announce, including a replayed old one, so persisted sequences are
what prevent rollback.
- For every name this node has published, or resolved through the CLI or API, it MUST persist the highest
  sequence seen, and never accept a lower one from any source.
- At equal sequence, the record already held is kept.
- Records for other names (heard, or resolved by the gateway or LXMF auto-fetch) are kept only in a table of
  at most `name_table_size` entries, least recently used evicted first. An announce of a name the node isn't
  resolving or keeping takes a place only if one is free or held by another name that was only heard, so a
  flood of announces of new names can't push out the names the node resolved.
- For names resolved without being persisted (gateway, auto-fetch), the node SHOULD persist the highest
  sequence it returned, as a floor below which no record of that name is accepted, so such a resolve never
  goes back after table churn or a restart. The record itself isn't persisted. (rnfs: up to
  4 × `name_table_size` names, least recently resolved forgotten first, saved at least every minute and at
  shutdown.)
- Records from keepers are subject to the same persisted-sequence and no-rollback rules as announces.
- A record this node keeps (below) is a floor for resolving that name: it MUST be offered to the sequence
  rules at startup and on every resolve, so an older validly signed announce is never accepted for a kept
  name, even after a restart or after the name left the table.

**Keepers.** A keeper stores the latest validly signed announce of a name and serves it, so the name
resolves after its owner's announce has expired from the network.
- **Signed record.** Exactly the fields RNS signs in the name destination's announce: the name key's public
  key (64 bytes), the name hash of `rnfs.name` (10 bytes, `SHA-256("rnfs.name")[:10]`), the random hash
  (10 bytes; its last 5 are the signed emission time), the ratchet (empty, or 32 bytes), the signature
  (64 bytes) and `app_data` (the record, at most 64 bytes).
- A record MUST be accepted only if every field has exactly that type and length; `name_hash` is that of
  `rnfs.name`; `SHA-256(name_hash + SHA-256(public_key)[:16])[:16]` (the RNS destination hash) equals the
  name asked for; `app_data` is a canonical record (Record, above); and the signature validates with the
  public key over `name + public_key + name_hash + random_hash + ratchet + app_data`.
- **`/name`.** Request: the name. Response: the record's fields in the order above, or `[]` if the node keeps
  no record of the name (RNS sends nothing for a nil response). Malformed requests and refusals (private
  mode, limits) are answered `False`. The largest response is 276 bytes as RNS packs it, so it always fits
  one packet; requesters pass that as `max_response_size`. Requests count against `MAX_REQUESTS_PER_LINK`.
- **Advertising.** A keeper provides the DHT key `SHA-256(b"rnfs-name" + name)[:16]` with `/provide`
  (batched and withdrawn like a CID's key, and republished at the slower, jittered interval of section 6)
  once it holds a record of the name. The
  prefix keeps these keys apart from CID keys (SHA-256 of a 36- or 39-byte binary CID).
- **What is kept.** A node keeps the names its user keeps (`name keep`) or follows, at most `keep_max_names`,
  and MUST keep its own published names, with records it signs itself, so they resolve through it. Keeping
  advertises the node's ID under the name's key, which links the node to every name it keeps or publishes
  (Name keys, above). Each kept name costs a provider record republished by this node (and, if followed, a
  pin), hence the small default.
  Kept names still without a record are asked for again every `announce_interval`, in turn (rnfs: at most 64
  per round, continuing after the last one asked).
- **Refreshing.** A keeper replaces its record when it gets a verifying record with a higher sequence, or the
  same sequence and CID with a later emission time (a fresher signature of the same record); never one with a
  lower sequence, nor one below the highest sequence the node already holds for the name (Sequence, above),
  which it would otherwise serve until a newer one is heard. On hearing a higher
  sequence for a kept name it SHOULD take the signed announce from RNS's packet cache if it can, else ask
  other keepers.

**Following.** A node MAY *follow* a name (`name follow`): it keeps the name and pins its latest target with
source `follow:<name>`. When the name moves, the new target is fetched completely in the background and
pinned, and only then is the previous target's follow pin released, so a complete copy is always held. The
fetch has the usual limits: `max_download_size`, the DAG byte bound of its new target's size (section 7) and
the disk reserve (section 8). A
followed name is checked when a newer record is heard and every `announce_interval`.
- The periodic check uses the record held, unless nothing was heard for the name for
  2 × `announce_interval`; only then does it resolve through the network (a path request, then keepers).
- A failed check is retried with exponential backoff: after 300 s, then 600 s, 1200 s and so on, at most
  `announce_interval` (never less than 300 s). The backoff restarts on success, or when the user follows
  the name again. A failed fetch is retried with the record held, without a path request. A name whose owner
  is gone for good therefore costs a flooded path request at most every `announce_interval`.

**Limits.**
- A name resolves only while its owner is online, a reachable transport node still holds its announce, or a
  keeper of it is reachable. Paths last 1 week on normal interfaces, less on access-point and roaming ones.
- A keeper can withhold a newer record but can't forge one; asking several keepers, and the owner (which
  keeps its own names), limits withholding.
- RNS transport nodes rate-limit rebroadcasts per destination (by default one an hour, after a burst of about
  6), so a name can be updated about once an hour sustained.
- RNS orders announces by a signed emission timestamp from the publisher's clock, so a publisher whose clock
  goes backwards can't update its name until old paths expire.
- A resolver's first resolution of a name, through an announce or a keeper, can't detect a replayed old
  record.
- The same name key used on two machines can produce two records with one sequence; each resolver keeps the
  one it heard first.

## 10. Protection against misbehaving nodes

Nodes assume others may be faulty or hostile. Data integrity is guaranteed; availability is protected as far
as is cheap. Rules live in the sections referenced.

| Threat | Protection |
|---|---|
| Forged or corrupted data | Every block verified against its CID (7) |
| Size lies, repeated or empty-subtree bombs, deep trees | Verified sizes, `max_download_size`, `max_output_entries`, the DAG byte bound of whole-DAG fetches, `MAX_DEPTH`, each block fetched once (5, 7) |
| Malformed DAG nodes | Strict msgpack types, re-encoding and per-node checks (5). The tree rule gives honest writers one CID per content; readers don't enforce it |
| Advertising on someone else's behalf | Records name only the identified requester (6) |
| Advertising content it doesn't have, or stale records | Failed providers removed; the search runs again; withdrawals (6, 7) |
| Squatting or flooding provider records | Oldest-plus-random replies; long-known providers tried first; eviction of the newest publishers; no identity switching on a link (3, 6) |
| Hiding content near a key | Provider search doesn't stop at the first reply that lists providers (6) |
| Routing table takeover with new identities | Only proven-reachable nodes stored; full buckets keep long-lived nodes (6) |
| Fake node IDs in replies | Unknown IDs never stored; at most `alpha` path requests for them per lookup; lookup time budget (6) |
| Slow or stalled transfers (both directions) | Rates measured from transfers, never from round-trip time; response deadlines; displacement protection earned by progress (a per-response grace, then a throughput floor), so a response that transfers nothing can't hold a slot; no-progress and stall cancellation with refund; a cooldown on a link whose response made no progress, was rejected unsent, or fell far behind its schedule; many fast-link slots; requester stall detection (2, 3, 7) |
| Request flooding, link exhaustion | Request size, per-link and global limits; a per-link request rate limit; eviction of idle incoming links; half-open links capped in their own bucket, oldest closed first (3) |
| Decompression and oversized-response bombs | Compressed Resources refused; response sizes capped; multi-segment Resources refused on `rnfs.dht` links and on links to LXMF propagation nodes; `rnfs.dht` responses' msgpack headers checked before RNS decodes them (3, 7, 13) |
| Reading another node's download history | Only provided content is served (7). Under the default `roots`, complete cached downloads are provided; `pinned` keeps them private |
| Name rollback or forgery | Self-certifying names; RNS-verified signatures; persisted highest sequence (9) |
| Forged or withheld kept names | Records verified against the name's key; highest persisted sequence; several keepers asked (9) |
| Tampered or spliced sealed files | Per-segment HMAC bound to the header, index and final flag, checked before decrypting (5) |
| Unsolicited LXMF links, inbox flooding | Strict field parsing, size caps per link, per message and for the whole inbox; at most 50 kept messages per untrusted sender; auto-fetched pins never pushed out by others' messages; signature-validated senders only; rate-limited path requests for unknown senders; `lxmf_accept_from`, per-sender `lxmf_auto_fetch_quota` (13) |
| Uploads exhausting a shared channel | `upload_budget` (charged for bytes sent, at most half of it per requester in flight), interface rules and quiet hours, applied to `/blocks` only (3) |
| Names and paths chosen by others reaching the local user | Default output names only if printable, not hidden, not starting with `-` and one valid file name, else the CID; non-printable characters escaped in output and logs (11) |
| Republishing and retries draining airtime | Small `keep_max_names`; slower, jittered republishing of name keys; paced lookups on slow nodes; follow backoff (6, 9) |

**Penalties.** A provider that sends a block that doesn't match the CID requested from it is banned for
`ban_time`: dropped from the routing table, and its announces and records ignored. Nothing else bans:
- timeouts and malformed replies only count as failures, since they also come from lossy links and version
  mismatches;
- a correctly hashed block that isn't a valid DAG node is the content author's fault, not the provider's, so
  the root being fetched is marked invalid locally and the fetch fails;
- a stored block that no longer matches its CID is local damage: it is deleted and fetched again, and doesn't
  make the root invalid.

RNS's own `blackhole_identity` is not used: it is instance-wide and persistent.

**Privacy** (outside private mode, where every link identifies).
- Lookups and downloads use unidentified links, so the nodes asked learn only that *some* node wants a key.
- Publishing uses a separate identified link, never reused.
- With the default `roots` strategy, complete downloads (by any command, `cat` and `stat` included) are
  published and served while cached, so anyone can learn what the node fetched. With
  `pinned`, downloads are neither published nor served. Gateway fetches never are.
- DHT keys don't hide which CID is wanted from someone who already knows the CID.
- Provider records are public: anyone can look up which node IDs provide a CID, or keep a name. A node's own
  names are linked to it this way (section 9).
- Transport nodes on the path see traffic volume and timing.
- Content is public by CID. Sealed files (section 5) keep their content and name from everyone but their
  recipients, not their size or recipient list.
- A node's LXMF address uses a separate identity, and its default display name (`rnfs node`) says nothing
  about the node, so its announce can't be linked to its node ID (section 13). The links it sends do carry
  its node ID as the `from` hint, so whoever reads them (and anyone who sees a propagated message) can link
  the two, unless `lxmf_send_from_hint` is `false`.
- The gateway never uses the address book (contacts) or decrypts sealed files, so web pages can't probe or
  read either.

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
- RNS decrypts and decodes every request packet and starts a thread for it before rnfs sees it, so a link
  sending requests as fast as its interface carries them still costs the node that work (about 0.14 ms of CPU
  each over loopback): the request rate limit (section 3) removes only rnfs's share. Likewise a node rejecting
  `/blocks` responses only after RNS has received them can be made to build one per cooldown per link, and
  RNS spends a key exchange on every new link.
- Any web page can make the local gateway fetch content (a plain request to `127.0.0.1` passes the Host
  check), and can time responses to guess what is cached. The gateway's own limits (section 11) bound the
  airtime this costs: `gateway_fetches`, and no `?format=` response (raw block or archive) at all for a web
  page's request or a `HEAD`.
- Every local user can use the gateway, and can make it or the API busy by holding connections up to the
  cap (section 11); the API itself needs the token.
- Unidentified links can still be tied to their sender by path and timing, e.g. a publisher's `/find` just
  before its identified `/provide`.
- A requester that already holds the blocks it asks for can complete responses unusually fast and inflate its
  link's rate estimate at the responder; the effect is limited to that link.
- An attacker that behaves exactly like a slow-link client competes fairly for the slow pool's slots, and
  can't be told apart from one.
- Displacement protection and the cooldown stop a requester holding a response slot without transferring, and
  the cooldown binds a link and its identity. But each unidentified link is its own requester, and where links
  all arrive over one interface (a single upstream transport node, or every link of a node attached to a
  shared instance), nothing at admission tells them from honest requesters on it: a newcomer with no history
  looks the same either way. So a pool can still be kept busy in two ways (Medium). Measured with the test
  fakes over 10 simulated minutes, an honest requester fetching every ~10 s (three tries 2 s apart) and the
  attacker asking every 50 ms over the same TCP interface, reporting two hops:
  - **Slow downloaders.** Links that each download their responses at about the rate they need to be done when
    due keep their slots: 18 links pulling ~1.3 kB/s each (~21 kB/s in all) held the 16 fast slots, and the
    honest requester was refused 95% of the time. At 300 B/s each, or taking one part of each response and
    renewing it from a spare link, they were cooled down (refused 4–7%; before, 3 links taking one part each,
    ~90 B/s in all, held the then two fast slots: 98%).
  - **Fresh-link churn.** A new unidentified link for each request, holding a slot for a grace (or, taking one
    part, until displaced as the slowest after `deadline_slack`) and never asking again: one new link a second
    taking one part each (~0.7 kB/s) left the honest requester refused 35% of the time, two a second 63%, and
    ten a second taking nothing 81%.
  A fixed set of links used in turn needs about `slots × (blocks_cooldown + hold) / hold` links (176 for the
  fast pool at the defaults, with a 30 s hold), more than `max_incoming_links` admits; 40 links taking one
  part each were refused 11% of the time. Limits on young links or on new links per interface would hold back
  honest peers reaching the node through one transport node; a waiting list giving freed slots to the
  requester refused longest ago only shares slots per link, which the attacker multiplies (measured over real
  RNS: no change, honest refusal 0.95–1.0 either way). The hold is not free: the attacker must keep
  downloading, or keep opening links. An operator under such an attack can raise `fast_blocks_responses`, or
  `blocks_cooldown`.
- An outside web site can frame a gateway page and receive public content from it by `postMessage` (outside
  private mode, only content public by CID).

## 11. Local interface

`rnfs daemon [--bootstrap <node ID>…]` runs a node. Other commands use its local API. The API and the gateway
are local conveniences, not part of the protocol.

| Command | Does |
|---|---|
| `add <path>\|- [--wrap] [--only-hash] [--name N] [--no-pin] [--to <recipient>…] [--to-self] [--no-sealed-name] [--chunker fixed\|fastcdc]` | Import a file, a directory or stdin, pin it with source `user`, print its CID. `--wrap` puts it in a directory; `--only-hash` just prints the CID; `--no-pin` keeps it as cache (section 8); `--to` seals it for recipients (section 5, Sealed files); `--chunker fastcdc` uses content-defined chunks (section 5) |
| `get <cid>[/path] [-o out] [--from <node ID>] [--background] [--raw]` | Fetch and write out a file or directory. Without `-o` the output is, in the current directory, the target's last path name if it is printable, not hidden (no leading `.`), doesn't start with `-` (which a later command would read as an option) and is a valid file name here, else its root CID. A sealed file this node is a recipient of is decrypted, and without `-o` (for a bare CID) written under its sealed name by the same rule; `--raw` writes the stored bytes |
| `cat <cid>[/path] [--offset N] [--length N] [--raw] [--block]` | Fetch and write a file, or part of one, to stdout; sealed files are decrypted as by `get` (only the segments covering the range); `--raw` writes the stored (encrypted) bytes, which is what scripts that show content to others must use; `--block` writes the CID's own block |
| `ls <cid>[/path]` | List a directory: name, kind, size, CID |
| `stat <cid>[/path]` | Size, block count, and how much is held locally |
| `dir set <cid>[/path] <name> <cid> [--exec]`, `dir rm <cid>/<path>` | Make a new directory with an entry added, replaced or removed, and pin it with source `user` (fetching the complete directory, file contents included) |
| `pin add <cid>[/path] [--background]`, `pin rm <cid>`, `pin ls [<cid>] [--json]` | Manage pins; `ls` shows their sources, whether each is a sealed file (`sealed`: true, false, or null when its stored blocks can't tell), and a local name: where each was added from on this machine (recorded locally, never sent; for a sealed file too), the shared-folder entry, or what a stored directory holds |
| `pin remote add\|rm <node ID> <cid>` | Remote pinning |
| `gc`, `verify` | Evict all cached fetches; re-check every stored block against its CID |
| `providers <cid>` | Find nodes providing a CID |
| `ping <node ID>` | Open a link and report its round-trip time (`rnprobe` can't probe `rnfs.dht`, which doesn't send delivery proofs) |
| `name create\|rm\|export\|import <alias>`, `name list` | Manage name keys; `rm` releases the name's pin |
| `name publish <alias> <cid>[/path]`, `name resolve <name>` | Mutable names. Every `<name>` argument is 32 hex digits or a name contact's nick |
| `name keep\|unkeep <name>`, `name kept` | Keep a name's signed record and serve it (section 9) |
| `name follow\|unfollow <name>`, `name following` | Keep a name's latest target pinned (section 9) |
| `export <cid>[/path] -o file [--background]`, `import file` | Archives |
| `jobs [list]`, `jobs show\|wait\|cancel <id>`, `jobs clear [<id>]` | Background jobs (below) |
| `contacts add <nick> <hex> [--kind name\|node\|lxmf]`, `contacts rm <nick>`, `contacts ls` | Contacts (below) |
| `send <address> <target> [-m text] [--propagate] [--nearest]`, `inbox`, `inbox get <n> [--link K] [-o out]`, `inbox rm <n>\|--all`, `outbox`, `lxmf address`, `lxmf sync [--nearest]` | LXMF (section 13). `--nearest` uses the nearest propagation node heard when `lxmf_propagation_node` isn't set |
| `peers [--json]`, `stats [--json]` | Known nodes with hops, interface and traffic; traffic since start, refusals by cause, per interface |
| `status` | Node ID, RNS identity hash (the value other nodes list in `members` and `remote_pin_identities`), known nodes, records held, storage use, traffic, jobs, LXMF address (only the version without a readable API token) |

Wherever a command takes a target, it may start with a name contact's nick (`get alice/docs/report.pdf`);
where it takes a node ID (`get --from`, `ping`, `pin remote`), a node contact's nick works too.

**Output from other nodes.** Names, paths and text chosen by other nodes (DAG names, sealed names, LXMF
links and messages) MUST be printed and logged with non-printable characters escaped, so they can't inject
terminal escapes or fake log lines. A name chosen by someone else is used as a default output name only under
the `get` rule above. `get` writes out under a temporary hidden name in the output's directory and renames
it to the output once complete (refusing if the output appeared meanwhile), so a failed or interrupted
write-out never leaves a partial output under the requested name.

**API** (`127.0.0.1:8788`) and **gateway** (`127.0.0.1:8789`) are separate ports, so pages served by the
gateway can never reach the API.
- Both MUST reject requests whose `Host` header isn't `127.0.0.1:<port>` or `localhost:<port>`, which blocks
  DNS rebinding.
- Every API request except `GET /api/status` MUST carry `X-Rnfs-Api: <token>`, compared in constant time.
  The token is random (at least 128 bits), made anew at each daemon start and written to
  `<rnfs dir>/api_token` with mode 0600 (on Windows, with a protected access list allowing only the user and
  SYSTEM); the CLI reads it from there and refuses a file other users can access. Every local process, of every user, can connect to `127.0.0.1`, and the API reads and writes
  files as the daemon's user, so only users who can read the rnfs directory may use it. Web pages can't
  learn the token, and the custom header needs a CORS preflight, which fails: this also blocks cross-site
  requests. `GET /api/status` stays open for health checks and the proof check below, but without the
  token it MUST say no more than the daemon's version (rnfs: `{"version": ...}`); the full status (node ID,
  identity hash, LXMF address, traffic) needs the token like every other request.
- The daemon MUST refuse to start if the rnfs directory belongs to another user or can't be made accessible
  to its owner only (on Windows: a protected access list allowing only the user and SYSTEM, which the files
  below it inherit; Administrators are tolerated where an older version left them).
- An API request carrying `X-Rnfs-Challenge: <32 hex digits>` is answered with
  `X-Rnfs-Proof: hex(HMAC-SHA256(token, "rnfs api proof <port> <challenge>"))`, `<port>` being the API's.
  Before it first sends the token, the CLI checks this proof with a `GET /api/status`, so a program another
  user runs on the port while the daemon is down learns neither the token nor a name key being imported.
- The API never sends CORS headers.
- Both servers SHOULD time-limit reading a request (rnfs: 15 s for the request line, headers and small
  bodies; 60 s between reads of an upload), but not the node's work, and SHOULD cap concurrent
  connections (rnfs: 32 for the API, 64 for the gateway), answering others `503` at once, so held-open
  connections can't exhaust threads or file descriptors.
- Both refuse HTTP/0.9 requests with a `400` carrying the usual headers, since an HTTP/0.9 response has none.

**Gateway:**
- Paths are `/rnfs/<cid>[/path]` and `/name/<name>[/path]`.
- **Files.**
  - Content-Type comes from the path's file name or `?filename=`, or else from sniffing the content.
  - Range requests for one `bytes=` range are supported; another or malformed `Range` header is ignored and
    the whole file sent.
  - `/rnfs/` responses carry `ETag: "<cid>"` and `Cache-Control: immutable`.
  - Sealed files are served as `application/octet-stream`, whatever their name says, and never decrypted;
    the gateway reads a file's first bytes to recognise them. The user's own `HEAD`, or range not starting
    at byte 0, uses those bytes (to recognise a sealed file, or to sniff) only if they are already stored,
    and never fetches them: its Content-Type then comes from the name, or is `application/octet-stream`. A
    request that isn't the user's own navigation (below) always reads them, fetching them if need be, so its
    Content-Type doesn't depend on what the node stores.
- **Trustless responses** (`/rnfs/` only; `/name/` answers `?format=` with 400):
  - `/rnfs/<cid>?format=raw` returns the CID's own block, re-verified, as `application/vnd.rnfs.raw`; a path
    is refused (400).
  - `/rnfs/<cid>[/path]?format=archive` returns an archive (section 8) of the target, as
    `application/vnd.rnfs.archive` with `Content-Disposition: attachment; filename="<target cid>.rnfs-archive"`
    and an exact `Content-Length`; it is byte-identical to `rnfs export`. Its length must be known before
    its first byte, so the user's own `GET` fetches the whole DAG first, within `max_download_size` and
    `max_output_entries` (413 otherwise), and the fetch is cancelled if the client disconnects.
  - Every format is answered only to the user's own `GET`, which fetches what isn't stored. A request that
    isn't the user's own navigation (`Sec-Fetch-Site` present and not `none`) and any `HEAD` get
    `409 Conflict`, whether the target is stored or not, without the node fetching anything. A browser
    downloads both types, and a frame of a download fires no load event while an error page does, so an
    answer that depended on what is stored (a download for stored content, a fetch that fails or a `503` for
    the rest) would tell web pages what the node stores. A bare-CID `If-None-Match` revalidation is answered
    304 before anything else. Formats therefore never use `gateway_fetches`. Unknown formats get 400.
  - Both carry `ETag: "<target cid>.raw"` or `"<target cid>.archive"` and `Cache-Control: immutable`
    (`If-None-Match` answers 304), and every other gateway rule (the CSP, nosniff, no CORS headers) applies.
- **Directories.**
  - A directory path without a trailing slash redirects to the slash form.
  - It is served as its `index.html` if present, otherwise as an HTML-escaped listing.
- **Every response** carries `X-Content-Type-Options: nosniff` and `Content-Security-Policy: sandbox
  allow-scripts allow-forms allow-popups; default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob:;
  form-action 'self'`. Pages run in an opaque origin and can't load resources from other servers;
  navigation and WebRTC can still reveal the viewer. A node MAY disable scripts (`gateway_scripts`).
- The gateway's own refusals (a bad path or CID, a method other than GET or HEAD, a `409` or `416`…) and a `404`
  say what went wrong. Any other error from the node (`400`, `413`, `422`, `503`, `507` or `500`) gets only a
  fixed message for its status; the details, which may name local paths, disk space or limits, go to the log.
- No response carries `Access-Control-*` headers, whatever the request's `Origin` or `Referer`. Pages run in an
  opaque origin, so they can load files of their own DAG as subresources (images, scripts, style sheets,
  frames) but can't read gateway responses with `fetch()` or `XMLHttpRequest`, and neither can other web
  sites. A gateway MUST NOT allow `Origin: null`: every site's sandboxed frames and `data:` pages send it.
  A separate origin per root (e.g. a subdomain per CID) would let pages read their own DAG; that is possible
  future work.
- Content fetched for the gateway is cache and is never provided. Names it resolves go only into the capped
  table (section 9).
- Requests that aren't the user's own navigation (`Sec-Fetch-Site` other than `none`) may start at most
  `gateway_fetches` network fetches or name resolutions at a time. Content already stored is served without
  limit. Browsers send `Sec-Fetch-Site` to `127.0.0.1` and `localhost`, and `none` only for a navigation the
  user started (address bar, bookmark); a request without it comes from a local tool the user runs.
- Apart from timing, the `gateway_fetches` limit and content no provider has, a web page's request gets the
  same answer whether the node stores the content or has to fetch it. The limit is a known, accepted leak: while a
  page keeps every slot busy with fetches, stored content is still served and content that needs a fetch gets 503,
  so the page can tell them apart. Like the timing difference, closing it would cost more than it protects.
  None of these applies to `?format=`, which a web page's request never gets (409, above).
- The gateway never expands contact nicks (`/name/<nick>/` is not supported), so web pages can't use or probe
  the address book.

**Background jobs.** A node MAY run long transfers as *jobs* it owns, so a transfer doesn't depend on one API
request or terminal staying open. A job has a kind — `get` (fetch a target and write it to an output path,
optionally from one node), `pin` (fetch a target completely and pin it with source `user`) or `export`
(write an archive of a target to an output path) — and a state: `queued`, `running`, `done`, `failed` or
`cancelled`. Its result, when done, MUST be what the equivalent API call returns (`POST /api/get`,
`/api/pin/add`, `/api/export`), and its error, when failed, the message that call would have reported.
- At most `max_jobs` jobs run at once; queued jobs MUST be started oldest first.
- Jobs MUST be persisted in the state directory, written atomically. When the node starts, jobs that were
  queued or running MUST be queued again. A resumed job MUST NOT fetch again blocks that are still stored.
- While a job is queued or running, the blocks stored below its root MUST be kept as if pinned, including
  across a restart, so a resumed job continues where it stopped. A failed or cancelled job's blocks are
  ordinary cache (`get`, `export`) or unreferenced (`pin`), and a new job for the same target fetches again
  whatever was evicted meanwhile.
- Cancelling a queued job MUST end it at once; cancelling a running one MUST stop its fetch at the fetch's next
  step (within 0.5 s), not at the end of a request in flight, whose link is then closed (section 7). Writing
  out (`get`) or the archive (`export`) after the fetch is local and is not interrupted.
- Stopping the node MUST NOT wait for jobs beyond a short bound (rnfs: 2 s); a job interrupted by the
  shutdown is saved as queued and resumes at the next start.
- At most `max_finished_jobs` (at least 1) finished jobs are kept, the oldest finished forgotten first, but
  a finished job is kept for at least 60 s whatever the cap, so a client following a fast job always finds
  its result. At most 1000 jobs may be queued or running (more are refused as busy). Two queued or running
  jobs MUST NOT share an output path, and a job's output path MUST NOT exist when it is queued.
- A `get` job writes to a temporary hidden name beside its output (rnfs: `.rnfs-job-<id>.partial`) and renames
  it when complete; if the output appeared meanwhile, the job fails. A failed, cancelled or interrupted job's
  temporary output is removed (a resumed job removes one left by a crash).
- A `get` job MAY ask (`sealed_name`) that a sealed file this node opens be written under its sealed name, if
  that passes the `get` default-name rule, in the output's directory; the final path is then the job's output.
  A `get` job with a `from` node MAY ask (`fallback_dht`) to be retried once with ordinary DHT discovery if it
  fails for any reason other than cancellation, a size limit or a full disk.
- A get's result (and a job's) keeps at most the first 100 warnings, then one saying how many more there were,
  plus `warnings_total`.
- Progress reported for a job: `total_bytes` (the target's verified content size, once its block is
  checked), `done_bytes` (content bytes of the target's chunks stored, whether downloaded by this run or
  already stored), `blocks` (distinct blocks met by this run), `blocks_total` (blocks of its DAG, once
  complete), `downloaded_bytes` (bytes downloaded by this run, DAG nodes included), `rate` (bytes/s
  downloaded over the last 300 s of the run, falling while nothing arrives) and `eta`
  (`(total_bytes - done_bytes) / rate`).

**Contacts.** A node MAY keep a local address book of nicks for names, node IDs and LXMF addresses. A nick is
1–32 characters of `a-z`, `0-9`, `-`, `_` and `.`, starting with a letter or digit, and not 32 hex digits, so
it can never be read as a node ID, name or CID. Contacts are local: they are never sent to other nodes.

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

The FastCDC parameters (`MIN_SIZE`, `AVG_SIZE`, `MAX_SIZE`, `GEAR`, the masks) and the sealed-file format's
constants are fixed in section 5; they matter only to writers that choose them and to readers of sealed files.

**Local settings.** Each node may change these. Settings have exact types: whole numbers are integers (`true`
is not 1), flags are `true` or `false`, rates (bits/s) and times (seconds, at most 30 days) are finite numbers.
Ports are 1–65535 (`gateway_port` 0: any free port); `alpha`, `beta`, `max_parallel_providers`,
`max_failures`, `max_incoming_links`, `fast_blocks_responses` and `min_rate_sample` are at least 1; other
sizes and counts at least 0; rates above 0; times above 0, except `startup_announce_delay`,
`provider_seniority`, `deadline_slack`, `blocks_cooldown` and `ban_time`, which may be 0. `members` and
`remote_pin_identities` are lists of identity hashes (32 hex digits). A node MUST NOT start with an invalid
setting.

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
| `provider_seniority` | 1 day | Providers (and keepers) in the routing table this long are tried first; a publisher seen this long gets its full share of the provider store |
| `response_target` | 60 s | Transfer time a `/blocks` response aims for |
| `deadline_slack` | 30 s | Added to response deadlines; minimum age before a response can be displaced |
| `stall_timeout` | 120 s | A response with no progress for this long is cancelled and refunded |
| `blocks_cooldown` | 300 s | A link (and its identity) whose response made no progress is refused `/blocks` this long |
| `lookup_timeout` | 10 min | |
| `path_timeout` | 30 s | Waiting for an RNS path |
| `max_failures` | 3 | Unanswered queries in a row before a node is dropped; busy refusals in a row before a provider fails |
| `link_idle_timeout` | 5 min | |
| `max_incoming_links` | 128 | |
| `max_blocks_responses` | 4 | `/blocks` responses in progress on slow links: half of this; the rest for other links, unless `fast_blocks_responses` is more. At least 2 |
| `fast_blocks_responses` | 16 | `/blocks` responses in progress on links that aren't slow, if more than the rest of `max_blocks_responses` |
| `max_upload_rate` | none; half the rate estimate on slow links that have one | |
| `max_records` | 100 000 | |
| `ban_time` | 1 h | |
| `max_storage` | 10 GiB | |
| `max_download_size` | none | |
| `max_output_entries` | 100 000 | |
| `name_table_size` | 10 000 | Records kept for names this node hasn't resolved through the CLI or API (four times as many sequence floors, section 9) |
| `remote_pin_quota` | 1 GiB per remote identity | |
| `remote_pin_identities` | empty | Identities allowed to send `/pin` |
| `private` | off | Private mode (section 3) |
| `members` | empty | Private mode's member identities |
| `scan_interval` | 60 s | Shared folder |
| `provide_strategy` | `roots` | `roots` (pins and complete cached downloads) or `pinned` (pins only) |
| `gateway_scripts` | on | |
| `gateway_fetches` | 2 | Network fetches at a time for requests that aren't the user's own navigation |
| `api_port`, `gateway_port` | 8788, 8789 | Local API and gateway ports, on 127.0.0.1 (section 11) |
| `bootstrap_nodes` | empty | Node IDs contacted first when joining (section 6) |
| `default_bootstrap` | on | Also contact the default bootstrap nodes when joining; never in private mode (section 6) |
| `reticulum_dir` | RNS's own | RNS config directory, the one `rnsd` uses by default |
| `verbosity` | 0 | Extra logging: 1 per-query detail, 2 RNS debug output |
| `upload_budget` | none (unlimited) | Bytes per hour of `/blocks` responses, node-wide; a token bucket charged for bytes sent, at most half of it per requester in flight (section 3) |
| `serve_interfaces` | empty (all) | Serve `/blocks` only on these RNS interfaces (names or prefixes, case-insensitive; a spawned interface also matches its parent's name) |
| `no_serve_interfaces` | empty | Never serve `/blocks` on these interfaces; takes precedence over `serve_interfaces` |
| `quiet_hours` | none | Local time ranges such as `22:00-06:00` or `01:00-05:00,12:00-13:00`, during which `/blocks` is refused |
| `keep_max_names` | 100 | Names kept (keep and follow) for others; this node's own names don't count. A whole number |
| `keeper_query` | `fallback` | `fallback`: ask keepers when a path request heard nothing; `always`: on every network resolve (about 10× the airtime) |
| `max_jobs` | 2; 1 on slow nodes | Jobs running at once (a whole number ≥ 1). Foreground `get`, `pin add` and `export` run as jobs too, so they queue beyond this |
| `max_finished_jobs` | 100 | Finished jobs kept (a whole number ≥ 1); each is kept at least 60 s regardless (section 11) |
| `lxmf_enabled` | on if the LXMF package is installed | A boolean. `false` turns LXMF off; `true` without the package logs a warning and stays off |
| `lxmf_display_name` | `rnfs node` | Announced with the LXMF address; 1–64 printable characters |
| `lxmf_send_from_hint` | `true` | Sent links carry `from=<node ID>` as a provider hint; this tells recipients which rnfs node the LXMF address belongs to. `false` omits it |
| `lxmf_propagation_node` | none | Propagation node (32 hex digits) used for `--propagate`, the fallback of failed direct messages and the periodic downloads. One only heard announcing itself is used only with `--nearest` |
| `lxmf_sync_interval` | 1 h | Downloads from `lxmf_propagation_node`; 0: never |
| `lxmf_accept_from` | empty: everyone | LXMF addresses whose messages are kept |
| `lxmf_auto_fetch_from` | empty | LXMF addresses whose links are fetched and pinned (`lxmf:<address>`) |
| `lxmf_auto_fetch_quota` | 1 GiB per sender | Storage the pins of one auto-fetch sender may use |

## 13. rnfs URIs and LXMF

rnfs links can be sent and received as [LXMF](https://github.com/markqvist/LXMF) messages, so content can be
shared with people using Sideband, NomadNet, MeshChat or any other LXMF client. This is optional and not part
of the `rnfs.dht` protocol: a node without the LXMF package (or with `lxmf_enabled = false`) does none of it.

**Identity.** A node that uses LXMF has a separate RNS identity for it, kept at
`<rnfs dir>/storage/lxmf_identity` (owner-only, like the node identity), so its LXMF address can't be linked
to its node ID by key. Its LXMF address is the hash of that identity's `lxmf.delivery` destination. LXMF's
own state is kept in `<rnfs dir>/lxmf/`. The node announces `lxmf.delivery` with its display name
(`lxmf_display_name`, by default `rnfs node`, which says nothing about the node ID) at startup, again after `startup_announce_delay`, then every `announce_interval`, and
never more often. The announce lists no supported functions, so senders don't compress messages to it (rnfs
refuses compressed Resources, section 3).

**rnfs URIs.** A link to rnfs content is written:

```
rnfs-uri  = "rnfs:" ( cid / "name/" name ) *( "/" segment ) [ "/" ] [ "?" query ]
cid       = the written form of a CID (section 4)
name      = 32 hex digits (section 9)
segment   = 1*( unreserved / pct-encoded / "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "=" / ":" / "@" )
query     = param *( "&" param )
param     = key "=" value      ; key: 1 or more segment characters other than "&" and "="
                               ; value: 0 or more segment characters other than "&", or "/"
unreserved, pct-encoded: as in RFC 3986
```

- Each segment is one path name (section 5), encoded as UTF-8 with percent-encoding. A writer MUST
  percent-encode every byte other than ASCII letters, digits, `-`, `.`, `_` and `~`, with upper-case hex
  digits, so a link has exactly one written form; it MUST write the scheme and hex digits in lower case, and
  no trailing slash.
- A reader MUST accept the scheme and hex digits (including percent-encodings) in either case, and one
  trailing slash. It MUST reject a link that has an empty segment, a segment that doesn't decode to valid
  UTF-8 or to a valid name, more than `MAX_DEPTH` names, a fragment (`#`), spaces, non-ASCII or non-printable
  characters, an invalid CID, or more than 4096 characters.
- `from=<node ID>` (32 hex digits) is a hint: a node that has the content, normally the sender's. A reader
  MUST reject a link with a malformed or repeated `from`, and MUST ignore other parameters, so later versions
  can add some. A node adds its own node ID as `from` to the links it sends unless `lxmf_send_from_hint` is
  `false`; the hint tells every reader which rnfs node the LXMF address belongs to.
- `rnfs:name/<name>[/path]` refers to whatever the name currently points at (section 9).
- Examples: `rnfs:bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq`,
  `rnfs:bagaib…/photos/caf%C3%A9%20night.jpg?from=9e0a158a1c3c4e8f9d2b7a6e5f4c3b2a`,
  `rnfs:name/1f2e3d4c5b6a79881f2e3d4c5b6a7988/index.html`.

**Messages.** A message sharing rnfs content carries:
- in its content: optional text, then each link's URI on a line of its own, so people and clients that know
  nothing of rnfs see the links;
- in its fields, for rnfs-aware clients: `FIELD_CUSTOM_TYPE` (`0xFB`) = the string `"rnfs"`, and
  `FIELD_CUSTOM_DATA` (`0xFC`) = an array of 1–8 *link maps*, msgpack maps with string keys:

  | Key | Type | |
  |---|---|---|
  | `cid` | bin, a binary CID | The target's root CID; for a name link, the CID the name pointed at when sent |
  | `name` | bin, 16 bytes | A name (section 9) |
  | `path` | array of str | Path names below the root, if any |
  | `size` | non-negative integer < 2^64 | Content bytes of the target, if the sender knows it (a hint: fetches verify sizes) |
  | `from` | bin, 16 bytes | Node ID of a node that has it (the sender's) |

  A link map has `cid`, `name` or both. These are LXMF's fields for embedding other protocols. An
  incompatible change of the link map uses a new `FIELD_CUSTOM_TYPE` value.
- A reader MUST reject the field (and then read links from the content instead) if `FIELD_CUSTOM_DATA` is not
  an array of 1–8 maps, a map has a non-string key or more than 16 keys, a value has another type than shown
  (booleans are not integers; `str` is not `bin`), a CID or name is invalid, a map has neither `cid` nor
  `name`, or a link, written as an `rnfs:` URI, would be longer than 4096 characters (as for URIs in the
  text). Unknown keys MUST be ignored.
- Messages are sent for direct delivery (over a link) unless the user asks for propagation. A direct message
  that fails is sent again through `lxmf_propagation_node`, if set. A node with `lxmf_propagation_node` set
  downloads the messages waiting there every `lxmf_sync_interval`, at most 64 per download. A propagation
  node only heard announcing itself (the nearest: fewest hops, then most recently heard node announcing
  `lxmf.propagation` with propagation enabled) is used only when the user asks (`--nearest`): it could be
  anyone's, and gets the messages sent through it. Without `lxmf_propagation_node`, `send --propagate` without
  `--nearest` fails, and the node logs once that downloads and the fallback are off.
- On the link to a propagation node, a response Resource larger than 1 MB (4 × the 256 kB delivery limit,
  which LXMF asks the node to respect) or of more than one segment is refused and the link torn down;
  decompression is bounded to 1 MB.

**Receiving.**
- Senders in `lxmf_accept_from` or `lxmf_auto_fetch_from`, and LXMF contacts (section 11), are *trusted*.
  Anyone else can make as many LXMF identities as they like, so the bounds below protect trusted senders'
  messages from them.
- A message is accepted only once LXMF has validated its signature with the sender's identity. A message from
  a sender whose identity isn't known yet is held while a path request fetches it, then validated: at most
  32, at most 8 claiming one sender, for up to 2 minutes, and only 30 seconds after its path request was sent
  (or after it arrived, if later) if no answer has made the identity known. When 32 are held, a further one
  is dropped, so held ones are not pushed out, unless it claims a trusted sender: then it takes the place of
  the newest message held for the untrusted sender holding the most. The source of such a message is
  unauthenticated until its identity is known, so path requests for held senders are sent at most once per
  sender per 2 minutes, and at most 3 at once, then one per 10 seconds node-wide, trusted senders first.
- If `lxmf_accept_from` is not empty, messages from other senders (except those in `lxmf_auto_fetch_from`)
  are ignored, and those from unknown senders are not held. Messages without rnfs links are ignored.
- The links come from the field if it is valid, else from the first 16 384 characters of the content. A link
  on a line of its own is read whole; elsewhere, trailing punctuation (`.,;:!?)]}'"`) is taken as the
  sentence's. At most 8 links, 4000 characters of text, 256 of title and 64 of display name are kept. Text
  from messages MUST be shown with control characters escaped.
- At most 500 messages and 4 MiB (as JSON) are kept, and at most 50 from one sender not in
  `lxmf_auto_fetch_from`. To make room, the oldest message holding no auto-fetched pins (none pinned or
  queued) is dropped; a message holding pins is not dropped for others' messages: if every kept message holds
  pins, a new message without any is not kept (and one with pins pushes out the oldest). A trusted sender's
  message is dropped only to make room for another trusted sender's, and only once no untrusted sender's
  message without pins is left; a new message from an untrusted sender that would need one dropped is not
  kept. New messages from untrusted senders are also rate limited node-wide: 32 at once, then one per minute;
  others are not kept. Each message's record is at most 16 KiB as JSON: links beyond that are dropped (the
  first is always kept), then text is cut.
- **Auto-fetch.** Links from a sender in `lxmf_auto_fetch_from` are fetched completely and pinned with source
  `lxmf:<sender address>`, one fetch at a time, taking senders in turn. At most 64 links wait: when that many
  do, a new link is skipped, unless its sender has at least two fewer waiting than the sender with the most,
  whose newest waiting link is skipped instead. Auto-fetches
  still waiting when the node stops are queued again, oldest first, at the next start; stopping the node or
  removing the message stops a fetch in progress. The fetch is refused if the target's verified size plus what
  that source already pins would exceed `lxmf_auto_fetch_quota`, or if the blocks of its DAG would (as for
  remote pins, section 8); `max_download_size` applies too. A `name/` link is resolved without persisting
  the name's sequence (only the capped table of section 9 holds it), so names arriving unasked don't grow
  the node's state. Removing a message from the inbox (`inbox rm`) releases its pins unless another inbox
  message from the same sender holds the same CID.
- Fetching a link (auto-fetch or `inbox get`) asks the `from` node first, unless it is the node itself, and
  the DHT if that fails. `inbox get` without `-o` names its output by the `get` rule (section 11): the link's
  last path name only if it is printable, not hidden, doesn't start with `-` and is a valid file name, else
  the root CID.

## Appendix A. Comparison with IPFS

**What RNS replaces**

| IPFS / libp2p | rnfs |
|---|---|
| PeerID | Node ID: RNS destination hash of `rnfs.dht` |
| Multiaddrs, transports, NAT traversal, relays, peer routing | RNS routing by destination hash |
| Noise / TLS, peer authentication | RNS Links (encrypted, forward secret); `Link.identify()` |
| Protocol negotiation, protobuf RPC | RNS link requests and msgpack; versions are destination aspects |
| Bitswap | `/blocks` over RNS Resources |
| Bootstrap and peer discovery | RNS announces, plus bootstrap node IDs: built-in defaults, as in IPFS, and your own (`daemon --bootstrap`) |
| IPNS records, DHT PUT/GET | RNS announces with signed `app_data`, cached by transport nodes and served by keepers (`/name`) |
| Swarm key (private networks) | RNS Interface Access Codes; rnfs private mode |
| Resource manager limits | RNS request and response size limits, plus rnfs's link and request caps |

**Features**

| IPFS feature | rnfs |
|---|---|
| CIDs | Section 4. One DAG codec (plus raw) instead of several; base32 only |
| UnixFS files and directories | Section 5. Fixed-size chunks and balanced trees, sorted index instead of HAMT sharding; executable bit kept, mtimes and symlinks dropped |
| Chunkers (`--chunker rabin`, `buzhash`) | Fixed (default) or one fully specified FastCDC (section 5), so every writer cuts the same content alike |
| No built-in encryption | Sealed files: a content convention encrypting a file for chosen RNS identities (section 5) |
| Kademlia DHT, providers | Section 6. 128-bit keys; `K` 10; servers and clients as in IPFS, with the mode carried in the announce; `beta` termination as in IPFS; one find message; batched publishing with withdrawal; no PUT/GET values |
| DHT routing table refresh, IP diversity filter | Replaced by RNS announces and failure counting; no IP addresses exist to diversify on, so Sybil resistance rests on seniority (section 6) |
| DHT key (SHA-256 of the multihash) | SHA-256 of the whole binary CID, truncated to 128 bits, so a raw block and a DAG node with identical bytes stay apart |
| Reprovider strategies | `roots` (default), `pinned`; `all` dropped to save airtime |
| Bitswap, multi-source and resumable download | Section 7; providers ranked by seniority, then hops and next-hop bitrate (section 6) |
| Bandwidth limits | Upload budget, interface rules and quiet hours for `/blocks` (section 3) |
| `swarm peers`, `stats bw` | `peers`, `stats` |
| Pinning, remote pinning, GC, `add --pin=false` | Section 8 (`add --no-pin`) |
| CAR import/export | Archives (section 8) |
| IPNS, `key` commands | Names (section 9) |
| IPNS records republished by other DHT peers | Keepers: any node can store and serve a name's signed announce (`/name`, section 9) |
| IPFS Cluster follower | `name follow`: pin a name's latest target (section 9) |
| DNSLink | Not applicable: RNS has no DNS. A gateway on the Internet could resolve a DNS TXT record to a name, outside this spec |
| MFS / `object patch` | `dir set` / `dir rm`, with names for a mutable root. Unlike MFS, an edit fetches the whole directory, since only complete DAGs can be provided |
| Gateway, trustless gateway (`?format=raw`, `car`) | Section 11 (`?format=raw`, `archive`) |
| `ipfs://` URIs | `rnfs:` URIs (section 13) |
| Long-running `get`/`pin add` in the foreground | Background jobs that survive restarts (section 11) |
| `ping`, `routing findprovs`, `repo verify`, `dag stat` | `ping`, `providers`, `verify`, `stat` |
| Pubsub | Not included: gossip floods are unaffordable on shared radio interfaces, and RNS has no routed multicast. rnfs links travel in LXMF messages instead (section 13), and `name follow` replaces subscriptions |
