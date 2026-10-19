# rnfs Specification

rnfs is content-addressed file sharing over [Reticulum](https://reticulum.network) (RNS), with the functional
features of IPFS: content IDs, chunked files, directories, a provider DHT, verified multi-source transfer,
pinning, mutable names and a gateway — built on what RNS already provides.

**Version.** This document describes rnfs 0.2.0. The wire format (request paths and messages, CIDs and DAG
bytes, chunk boundaries, provider and name records, announce app_data and the LXMF field) is byte-identical
since rnfs 0.1.3, so nodes of 0.1.3 and later work together. `tests/test_wire.py` checks the current code
against vectors that 0.1.3's and 0.1.4's own code produced (`tests/fixtures/wire`).

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
  `rnfs:` URIs (section 13). Neither changes the protocol. rnfs's own LXMF messaging is deprecated in 0.2.0: it
  moves to a separate app, while `rnfs:` URIs and the LXMF link-map field stay part of this specification.

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
  - A client (section 6) MAY skip the periodic announces while nothing needs other nodes to reach it, but MUST
    announce while it provides anything (section 6), owns names (it answers `/name` for them, section 9),
    accepts remote pins (section 8) or is in private mode, and SHOULD announce at once when that starts. A
    transport node forwards a path request only for a destination it has a path to, unless the request arrived
    on an access-point, gateway, roaming or boundary interface, or one with `recursive_prs` (RNS 1.5.6
    `Transport.path_request`); interfaces default to `full` mode, so a destination that never announces is
    usually reachable from one hop away only. And private-mode servers of rnfs 0.1.4 to 0.1.6 drop at start the
    records of publishers whose identity RNS doesn't recall. (rnfs: checked every minute; servers always
    announce.)
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
    making no progress, or whose response Resource it rejected or cancelled before a quarter of it was sent, is
    refused new `/blocks` for `blocks_cooldown`. Otherwise a requester could have the node build a response,
    take its first window of parts, reject it and ask again in a loop: each round costs the node reading,
    hashing, packing and encrypting the whole response, and the requester one window. On RNS 1.5.6 a receiver
    cancelling a Resource, at its advertisement or mid-transfer, reaches the sender as a rejection
    (`REJECTED`); the part sent is the more of the progress last read and the Resource's progress then, and
    counts as a quarter or more if it can't be read. A requester that gives up on a response (a stall, a
    reader that stops early) MUST close the link, so the Resource fails (`FAILED`), and MUST NOT cancel or
    reject the Resource itself (`Resource.cancel()`), or it would cool itself down at its providers. A link
    closing starts no cooldown however little was sent: in private mode that would hold back the member's own
    identity. So the only honest cancel is RNS's receiver running out of retries on a lossy path; that
    requester's next `/blocks` to the node are refused (`False`) until the cooldown ends, and it moves to
    another provider after `max_failures` of them (section 6). A response displaced as the slowest counts as
    making no progress if it was moving at less than a quarter of the rate it needs to be done when due: by
    its deadline, or, on a link with no rate estimate whose MTU RNS raised, by the deadline it would have at
    `slow_bitrate`. So a requester taking a little of each response can't keep its slots young by displacing
    its own responses from a spare link; an honest one keeping to its schedule is displaced without a cooldown.
    Every cooldown lasts `blocks_cooldown`, flat: it doesn't grow with repeats. The refusal is answered at
    once, before any slot is reserved or block read.
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
- Links a node opens to NomadNet nodes to add what they serve (section 11, Adding from NomadNet) carry compressed
  Resources of several segments and files with metadata: they are bounded per link as section 11 says, and none of
  the rules below that are for `rnfs.dht` links applies to them, nor do they change any.
- rnfs neither sends nor accepts compressed Resources on `rnfs.dht` links. Handlers are registered with
  `auto_compress=False`, requests are never sent unless they fit one packet, and a node MUST reject, before
  accepting it, any Resource advertisement on an `rnfs.dht` link that has the compressed flag set, since RNS
  decompresses before any rnfs check runs. A node that has its process to itself (the daemon) also sets RNS's
  maximum decompressed Resource size (`RNS.Resource.AUTO_COMPRESS_MAX_SIZE` in RNS 1.5.6) to 0, which also
  stops RNS compressing anything the process sends; a node embedded in another program leaves that setting to
  the program.
- RNS checks request and response size limits against a Resource's data size but allocates by its transfer
  size, so a node SHOULD reject, before accepting it, any incoming Resource advertisement that is a request
  (rnfs requests always fit one packet), or whose transfer size exceeds its data size or `MAX_RESPONSE_SIZE`
  by more than the encryption overhead (rnfs: 128 bytes). RNS checks those limits per segment, so on
  `rnfs.dht` links (either end) a node SHOULD also reject any advertisement of more than one segment: every
  rnfs response fits one (`MAX_RESPONSE_SIZE`). On `rnfs.dht` links it SHOULD also reject one with metadata,
  which RNS decodes as the Resource completes and rnfs responses never carry. Other protocols' Resources in
  the same process (LXMF) are not affected by these two. A node embedded in another program applies every
  check in this item to its `rnfs.dht` links only, so the program's own links behave as with RNS alone.
- RNS decodes a whole response with msgpack before handing it over, and one-byte empty arrays or maps decode
  to some 70 times their size in objects (a 1 MiB response: ~70 MB and ~0.3 s of CPU). So on `rnfs.dht`
  links a node SHOULD check a response's msgpack headers before RNS decodes it, without building objects, and
  refuse one that can't be an rnfs response: anything but `[request_id, response]` with a 16-byte `bin`
  request ID and a response that is `True`, `False`, `nil`, a `/find` answer, or a list of at most 26 entries
  (the most CIDs a `/blocks` request can carry), each a `bin` of at most `MAX_BLOCK_SIZE` or `nil`, with
  nothing after it. rnfs checks responses sent as a Resource, and those sent as one packet larger than RNS's
  default MTU (only links that negotiated a larger MTU carry them; smaller ones decode to little). A refused
  response fails its request as a malformed one does. A responder sends no requests, so a response packet on
  an incoming `rnfs.dht` link is dropped unread. Likewise a node serves requests only on incoming links, so it
  SHOULD drop, unread, a request packet on an `rnfs.dht` link it opened: the remote destination of such a link
  has no request size limit or handlers, and RNS would decode the request and start a thread for it first.
- `max_upload_rate` caps bytes sent for `/blocks` on a link. An explicit one applies to every `/blocks`
  response, including one sent as a single packet (which never gives a rate estimate: on a link with a large
  MTU every response may be one); the default, half the rate estimate on slow links, applies once the link has
  one. RNS paces a Resource by its receiver, so the cap is applied between responses: the response budget uses
  the lower of the rate estimate and `max_upload_rate`, and the next response on that link starts no sooner
  than `bytes × 8 / max_upload_rate` seconds after the previous one started.
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

Private mode restricts rnfs's links, not RNS announces. A member's names are announced as anywhere else (section
9), and RNS carries those announces to every node that hears them, so outsiders learn each name's
`[sequence, CID]` record and can follow its changes; only the content stays member-only. `name publish` in
private mode warns of this (rnfs: the result carries a `warning`, section 9).

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
- **Gateway host form.** DNS labels hold at most 63 characters and an rnfs-dag CID's written form has 64, so the
  gateway's subdomain hosts (section 11) write a CID as `k` + lowercase base36 of the binary CID (big-endian,
  no padding), as IPFS does for subdomains: 56 characters for a raw CID, 60 for an rnfs-dag one, the same for
  every digest. It is a host-only encoding: only the gateway's host parser accepts it, with exactly one of
  those lengths, digits `0-9a-z` only, and canonical (`encode(decode(label)) == label`, the decoded bytes a
  valid CID as above). The CID is converted to its written form at once, before any other use; responses
  (`ETag`, `Location`, listings, errors) and logs show the written form. Everywhere else, `k…` is not a CID.
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
- **Folders.** `add --to` of a folder seals it as one sealed file, never file by file (which would leave its
  names and tree visible): the folder is packed as one plain tar (POSIX pax) and that tar is sealed with the
  sealed name `<folder>.tar`. The format above is unchanged, so a reader that only decrypts (rnfs 0.1.x) gets a
  tar file it can unpack itself.
  - **Marker.** The tar's first header MUST be a pax global extended header (typeflag `g`) holding exactly one
    record, the POSIX keyword `comment` with the value `rnfs sealed folder` (the record
    `30 comment=rnfs sealed folder\n`). This is how a sealed folder is recognised: a tar sealed as a file (by
    rnfs 0.1.x, or `add --to backup.tar` now) has no such first header and stays a file. Extractors ignore
    `comment`, so the marker adds no file and no warning (checked with GNU tar and Python's `tarfile`).
  - Every member is under one top directory named after the folder (`<folder>/…`), so extracting the tar
    creates nothing else in the directory it is extracted in. Each directory comes before what it holds.
  - The folder is walked with `add`'s rules: hidden files included; symlinks, devices, sockets, FIFOs, names
    that break the name rules and entries that change while being opened skipped with a warning; a file with
    other hard links is an ordinary file (members are regular files and directories only, never links). Names
    holding `\`, `＼` or `／`, or starting as a Windows drive does (`C:`), are skipped with a warning too, as
    `GET /api/get/stream` skips them (section 11).
  - Files are mode 0644 (0755 if executable), directories 0755, owned by uid and gid 0 without user or group
    names, every member dated the time of packing.
  - The tar must unpack where it is sent: more than `max_output_entries` files and directories (the top one
    included), or a tree deeper than `MAX_DEPTH` once built as `add --wrap` builds the folder with fixed
    chunks, is refused (rnfs: `TooLarge`, 413). A file that gets shorter while it is read is an error (its
    member's size was already written).
  - A folder whose name breaks the name rules, or is over 251 bytes (`<name>.tar` must pass them), is refused,
    not sealed without a name, and so is `--no-sealed-name` with a folder: the name is what tells a recipient's
    `get` to unpack it. With `--wrap` the wrapper's entry is `<folder>.tar`.
  - What it hides: the folder's file names, its structure and each file's size. What it doesn't: the
    recipients, and the approximate total size (the tar's: the files' sizes rounded up to 512 bytes, plus at
    least 512 bytes per entry), which says roughly how much the folder holds.
  - **Unpacking.** `get` by a recipient unpacks a sealed file only if its sealed name ends in `.tar` (after at
    least one character), its plaintext starts with the marker (as its first header; a marker anywhere else,
    or with other records, doesn't count) and its plaintext is a valid tar. Anything else is written as the
    file, as rnfs 0.1.6 writes any sealed file, byte for byte: an unmarked tar (the normal case for a sealed tar
    file) without a warning, a marked one that isn't a valid tar with one. `info` tells a recipient whether a
    sealed file is a sealed folder (rnfs: `sealed_folder`). Unpacking
    applies both existing rule sets unchanged: the plaintext is read as `POST /api/add/stream?format=tar` reads
    an upload (regular files and directories only; symlinks, hard links, devices, FIFOs, absolute paths, `..`
    components and invalid names skipped with a warning; a later member replacing an earlier one of the same
    path; `max_output_entries`, `MAX_DEPTH` and the header bounds of section 11), then written as `get` writes a
    directory (a temporary name renamed when complete, names the local filesystem would read as a path or can't
    represent skipped). A file is executable only by its owner-execute bit; no other mode, owner or time is
    taken from the tar. A tar past a limit is refused (rnfs: 413, naming `--no-extract`) rather than written in
    part. The output is the tar's one top directory, or the whole tar as a directory when it holds anything else;
    without `-o` it is named by `get`'s default-name rule (section 11) after that directory (or the sealed name
    without `.tar`). The node MUST check that its free space holds the plaintext twice where its blocks and the
    output share a filesystem (rnfs builds the tar into its block store, then writes the files), and MUST NOT
    pin, cache or provide those plaintext blocks: rnfs holds them in a protection session (section 8) while
    unpacking and collects them at once after, so none stays stored unless something else keeps the same block.
    `--no-extract` writes the decrypted `.tar` instead, as `get` of a 0.1.x node does.

  Sealing is randomised, so a sealed file's CID can't be predicted (`--only-hash` is refused).
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
- **Saved across restarts.** A node SHOULD save its routing table: each entry's first-seen and last-heard times
  and whether it failed a query since, the first-seen times of dropped nodes until they expire, and bans until
  they end. Times are saved as wall clock; one later than the current time when loaded (a clock since corrected
  backwards) counts as now, and a loaded ban ends at most `ban_time` from now. A loaded entry keeps its
  seniority, and `/find` replies leave it out while it has been silent for over `2 × announce_interval`, the
  time the node was down included, until it is heard again. So a restart doesn't make every server new again,
  nor send path requests to find servers it already knew (rnfs: in the state database, the rows changed written
  every 30 s and at stop).
- **Joining.** At startup a node sends `/find` for its own node ID, one node at a time, until two have answered:
  first servers of its saved routing table, then bootstrap nodes: those it was given (`bootstrap_nodes`,
  `daemon --bootstrap`) in their order, then, unless `default_bootstrap` is off or it is in private mode, the
  implementation's default bootstrap nodes (rnfs: `DEFAULT_BOOTSTRAP_NODES`) in random order. It adds those that
  answer, then runs a lookup for its own ID from the servers they listed and its routing table, without asking
  the contacted nodes again; this fills its table with its neighbours.
  - Saved servers are asked best scored first (local scores, below), the longest known first between equals
    (rnfs: by score band, at most 6 of them, leaving out those that failed a query since they were last heard;
    those silent for long are asked too, as after a long downtime all are). A node that has joined once is its
    own bootstrap list, so a restart doesn't flood path requests for bootstrap nodes on radio.
  - Saved servers and bootstrap nodes are contacted outside the lookup's budget of `alpha` unknown IDs, so
    unreachable ones don't stop the rest being tried.
  - A node behind an RNS access-point interface hears no announces from beyond it, so bootstrap nodes are
    its only way in.
  - While fewer than 3 servers are known that `/find` replies would include (not failed since last heard,
    nor silent for over `2 × announce_interval`), the node joins again, 5 minutes after the last join and
    doubling up to `announce_interval`: on radio every path request is flooded.
  - After a join that saved servers answered (so no default bootstrap node was asked), a node that isn't slow
    SHOULD still ask the default bootstrap nodes, one at a time in random order until one answers (rnfs: at
    most 2), and look up its own ID from the servers it lists, once the join is done so it isn't delayed: a node
    that saved only a small island of servers still reaches the rest of the network. A slow node skips them, as
    every path request on radio is flooded, and stays on such an island until it hears announces from beyond
    it, or joins again with too few servers left. With `default_bootstrap` off or in private mode there are no
    defaults to ask.
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
  `alpha` such IDs are tried per lookup. (A keeper query asks unknown keepers from a budget of its own, also
  `alpha`, so nodes named in `/find` replies can't use it up, section 9.)
- Nodes that fail to answer are dropped from the lookup and counted in the routing table.
- **Unreliable nodes.** An announce shows a node is alive, not that a link to it can be established: a far
  node whose announces keep arriving may never accept a link, and each query to it then costs RNS's whole
  link establishment timeout (about 6 s per hop). A node SHOULD keep counting the failed queries of such a
  node however often it is heard, also when it isn't in the routing table (it is named in other nodes'
  replies), and SHOULD NOT let it hold lookups open. In rnfs a node whose last `max_failures` queries all
  failed is *unreliable* until it answers one:
  - it starts lookups, is queried, counts for termination and is in a lookup's result only after every other
    node known, so it is asked (and waited for) only when the lookup has too few others;
  - it is asked again 5 minutes after it became unreliable, doubling with each further failure up to an
    hour, by one lookup at a time, if it would otherwise rank among the nodes the lookup queries. That query
    is sent on top of the `alpha` in flight and nothing waits for its answer; an answer makes it reliable
    again.

  Its queries aren't shortened: on a long radio path a link may take the whole timeout to establish, and
  only an answer shows the node is back. Its routing-table entry, `/find` replies and the join's count of
  live servers are unchanged.
- Every lookup has a time budget, `lookup_timeout`.
- **Termination.** As in IPFS, a lookup is complete once the `beta` closest nodes it knows of have all
  answered, or once every known node has been queried without finding new ones. Nodes near a key can keep
  answering with ever closer IDs, generated or ground for it, so a lookup also ends, without being complete:
  - once it has sent 8 × `K` (80) `/find` queries in one run (counting retries of unreliable nodes), and the
    answers to those still out are in;
  - after 3 × max(`alpha`, `beta`) answers in a row (9 at the defaults) that don't shorten the bit length of
    the distance from the key to the `beta`-th closest node it knows.

  Neither bound ends an honest lookup: over a simulated network of 2000 nodes, each of 2500 lookups (`alpha`
  and `beta` 3) completed within 12 queries and at most 8 such answers in a row.
- **Finding the closest nodes** (for publishing) runs until the lookup is complete. The result is the `K`
  closest nodes known; the stores sent to them also show which are reachable.
- **Finding providers.** Downloading starts with the first provider found. The search ends when
  `max_parallel_providers` providers are known and one has delivered the verified root block, when the lookup
  is complete, or at `lookup_timeout`. It does not stop at the first reply that lists providers, so one node
  near the key can't hide content.
- **Choosing providers.** A node SHOULD rank providers by these keys, in this order:
  1. **Seniority.** Providers that have been in the routing table for over `provider_seniority` come first.
     This key MUST stay first: an attacker can create fresh identities that sit close in hops, or earn a score
     quickly by serving popular blocks, but can't make them old.
  2. **Local score.** Within each seniority group, a provider with a better local score (below) first (rnfs: by
     score band; a provider without a score counts as 0).
  3. **Hops.** Fewer hops (`RNS.Transport.hops_to`) first; a provider whose path is unknown goes after every
     provider with a known path.
  4. **Next-hop interface bitrate.** A faster outbound interface on the path first
     (`RNS.Transport.next_hop_interface(id).bitrate`; through a shared instance, the bitrate of the interface
     `Reticulum.get_next_hop_if_name` names); an unknown bitrate goes last.
  5. **Reply order.**

  With no scores, hops or bitrates known, this is seniority, then reply order. Path information is read once per
  provider per fetch, when the provider is found. Through a shared instance, a node MAY reuse the instance's
  answer for a node's next-hop interface for up to 15 seconds (each answer is an RPC to it); hops, and whether
  there is a path at all, are read fresh.

  A provider *fails* when its link fails, it answers `None` for a block of the root it advertised, it answers
  `False` (busy or refused) `max_failures` times in a row, or it stalls twice (section 7). A `False` is retried
  after a short delay. A `False` soon after the provider served a response (within 4 × that request's round
  trip, counted from when it was sent) is taken as upload pacing (section 3) and not counted; it is retried
  after an eighth of that round trip, or the usual delay if longer. Failed providers are removed and not
  retried during that fetch. When no providers are left, `/find` runs again for a fresh random half, within the
  same `lookup_timeout`.

**Local scores.** A node MAY keep a private score for each peer it deals with, earned by work it checked itself.
Scores are local state: they are never sent or published, nothing on the wire changes, and nodes may score
differently. A score can be earned quickly by serving popular blocks, so it never outranks seniority. rnfs:
- raises a peer's score by 1 for each `/blocks` response that delivered blocks verified against their CIDs, and
  by 0.5 for a `/find` answer that proved useful, once per fetch or lookup: it listed a provider that then
  served verified blocks, or named a server closer to the key than the answering node that then answered;
- lowers it by 1 for a failed query or provider (a `/find` unanswered, a provider failing in a fetch) and by 32
  for a block not matching its CID (besides the ban);
- halves every score each 7 days without new work, towards 0 from either side (silence lowers standing, and a bad
  score is forgiven in time), and keeps it within ±64;
- starts bootstrap nodes (`bootstrap_nodes`, and the defaults unless `default_bootstrap` is off or in private
  mode) at 6 when they have no score, so a fresh node has someone to lean on. The seed decays like any score, so
  they keep standing only by earning it; a node no longer a bootstrap node loses what is left of its seed at the
  next start; bans apply to them as to anyone;
- compares scores in bands a power of two wide (0 within ±1, else ±(1 + ⌊log₂ |score|⌋)), so peers of about the
  same standing are ordered by the next key;
- keeps scores for at most 4096 peers, forgetting the least recently raised first (a peer only ever lowered goes
  before any raised); a peer without a score scores 0, so forgetting is always safe;
- saves them, with wall-clock times, a time in the future when loaded counting as now.

**Announce first-seen log.** A node SHOULD record when it first heard each server's `rnfs.dht` announce in server
mode, whether or not the server enters its routing table, and keep the record across restarts (wall-clock times; one
in the future when loaded counts as now). Identities generated next to a key can't be old ones, so among the servers
near a key, those first heard at least `provider_seniority` ago were there before any such identity was made for it;
the log gives the `K` closest of them to a key, which publishers also store to (below). It is local state: nothing
about it is sent. A server that announces client mode is forgotten. rnfs keeps at most 65,536 servers, forgetting
the least recently heard first (about 12 MB of memory when full), so a flood of new identities pushes out only
servers silent longer than every one of them; announces are recorded in memory and written together every 30 s.

**Publishing.**
- For each key, find the `K` closest nodes, by a lookup per key or by sweep providing (below). Then send each node
  the keys it should store, in as few `/provide` requests as the batch size allows (section 3), one at a time
  over a separate identified link closed afterwards. A publisher MAY send to several nodes at once, each over its
  own link and in its own order (rnfs: up to 16 on nodes that aren't slow; a slow node one at a time).
- **The store set.** A node SHOULD store each key on its `K` closest found as above, and also on the `K` servers
  closest to the key in its announce first-seen log that it first heard at least `provider_seniority` ago (rnfs:
  and has heard within 2 × `announce_interval`, as `/find` replies leave out servers silent that long; not banned;
  in private mode, members only). Without an attack these are mostly the same servers, so it costs little; against
  identities generated next to a key it keeps the record where they can't take its place. A key's store set is the
  union, without duplicates; every server stored to is recorded, so withdrawals reach all of them. A key whose
  lookup found no server isn't stored this round (rnfs: retried 30 s later).
- **Sweep providing** finds the `K` closest of many keys with one lookup per *region*. It is local behaviour:
  only which `/find` queries a publisher sends changes, and how it groups its `/provide`s. Take the due keys in
  turn; for the first not yet covered, x, run the closest-nodes lookup. Let p be the length of the prefix x shares
  with the farthest of the `K` closest found (of all found with fewer; 0 with none): the region is every due key
  sharing x's first p bits. Every server closer to x than that one shares those bits, so a region holds at least
  `K` servers and each of its keys has its `K` closest inside it. The lookup knows the half of the region holding
  x; if the region holds 2 or more due keys, ask the known server nearest the centre of the other half about that
  centre (`/find`), and while a reply names `K` servers inside the part asked about, ask about each half of it
  too. Each key of the region then takes the `K` closest of every server learned, ranked as the lookup ranks them.
  rnfs sends at most 7 such `/find` a region, and a server outside the part it was asked about hands over to the
  one it names nearest the centre inside it (its routing table may hold only some of the part). Over a simulated
  network of 2000 servers (Kademlia routing tables), this placed every record on its true `K` closest, against
  about 97% for a lookup per key, for about 1.6 extra `/find` a region; a round needs about servers ÷ 12.5 lookups
  however many keys are due (1M keys on 10,000 servers: about 800 lookups instead of 1M). rnfs takes the due keys
  never published first, then those unpublished longest, and on nodes that aren't slow runs up to 4 lookups at
  once, each claiming the keys sharing one bit fewer than the last region's p so that lookups running together
  seldom find the same region.
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
  (rnfs: at most 10 region lookups per publish round, each publishing every due key of its region, and after a
  round of n lookups the next waits 30 × n s; items never published go first, then those unpublished longest).
  It SHOULD pace withdrawals too: each server they alone reach costs an identified link and a request, and
  unpinning many keys reaches many servers (1,000 keys at 1,000 servers: about 2.9 h of airtime at 1 kbit/s).
  rnfs sends a slow node's withdrawals along with the round's stores to the same servers, and counts each server
  only withdrawals reach as one of the round's 10 lookups (those with the most keys queued first; while any are
  queued, up to 5 of the 10 are kept for them). Other nodes send withdrawals at once.

**Provider records.**
- `/provide` records the identified requester's node ID as a provider; a node can only advertise itself.
  Records carry no addresses, since RNS routes by node ID.
- A record expires `PROVIDER_TTL` after it was last stored, keeps its first-stored time across republishes,
  and is removed on withdrawal. These times are wall-clock and persisted; one later than the current time (a
  clock since corrected backwards) counts as now, so a record can't outlive its TTL.
- How much a node stores for others, and whose records it keeps when that is full, is local server policy:
  nothing on the wire depends on it, and a refused store is only a `False`. RNS identities are free and carry no
  address, so no limit keyed on an identity bounds a flood; rnfs shares freely while there is room, and once
  there isn't, keeps the places of the publishers it has seen for days. In rnfs:
  - **Record budget.** Records may take `record_budget` of disk, measured as the state database's size over the
    records it holds (so their indexes, their publishers' standing and storage overhead count: about 150 bytes a
    record, up to 250 when each has a publisher of its own; 7 million records in the default 1 GiB), and never
    the free disk the blocks' reserve needs (section 8). The store is *full* when either is reached. While it
    isn't, any publisher stores freely: there is no per-publisher limit.
  - **Storage tiers**, applied only while the store is full. *Known*: contacts of kind `node`, private mode's
    members, and the bootstrap nodes a join contacts (`bootstrap_nodes`, and the defaults unless
    `default_bootstrap` is off). *Earned*: publishers that held a record here and refreshed it on at least 3
    distinct UTC days (the day of a first store not counted). *Newcomer*: everyone else, 0.1.x publishers new
    to this node included. Each tier has a floor, a share of the budget: Known 30%, Earned 60%, Newcomer 10%.
  - **Making room.** A new record to a full store evicts a record of a tier's largest holder (the record it
    refreshed least recently): from the lowest tier below the storer's that holds more than its floor; else,
    if the storer's own tier holds less than its floor, from a tier above it that holds more than its floor;
    else from the storer's own tier, unless the storer already holds as many records as that tier's largest
    holder (so it can't churn its records), or the tier is Earned: Earned publishers never evict each other.
    Otherwise the store is refused. Floors hold in both directions: a lower tier can't take a higher one's
    floor, nor a higher one the lower one's for good.
  - **Expiry while full.** While the store is full, a Newcomer's record last stored 4.5 days ago counts as
    expired: one more way a full store makes room. The longest honest gap between republishes is just under
    3.5 days (kept names, below), so a record republished on time never lapses this way. A store that isn't
    full keeps every record `PROVIDER_TTL`, so a record survives one missed republish.
  - A publisher's refresh days are persisted with its first-seen time, and kept `PROVIDER_TTL` after the last
    of them once its last record is gone, so one that was away for a while is still Earned; a publisher that
    never refreshed a record leaves nothing behind, so a flood of fresh identities grows no state.
  - A key keeps at most 1000 publishers besides this node, so `/find` replies stay cheap: a newcomer to a full
    key evicts a random one of the lowest tier there (never one of the `K/2` oldest records, which replies list
    first), if that tier is below its own, or is its own and not Earned; otherwise it is refused. Refusing every
    newcomer would let early Sybil identities hold the key, and the random half of `/find` replies relies on
    newcomers getting stored.
- The node's own records are always accepted and never evicted to make room for others' (rnfs: nor counted
  toward the 1000 publishers of a key; they take room in the budget), so Sybil stores can't push a node's own
  record out of its own store.
- Providers republish every `republish_interval`, which MUST be less than `PROVIDER_TTL / 2`. Providers also
  record themselves locally. Keys that aren't CIDs' (kept names, section 9) are republished every
  max(`republish_interval`, 3 days), each with a fixed jitter of ±10% derived from the key's first byte, so
  many keys don't come due at once (256 slots) while keys sharing their leading bits come due together (the
  same servers are closest to them); the jittered interval stays below `PROVIDER_TTL / 2`.
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
   4 KiB of it (rounded up), plus 1 KiB, plus what its directory and index nodes bring: a directory node its
   own bytes and 16 bytes per entry, an index node its own bytes and 16 bytes per child less 64. A remote pin's
   or auto-fetch's quota applies too, whichever is lower. A file node may list children of size 0, and index
   nodes of one child can be chained, so without this a file that declares 1 byte, or a directory of empty
   files, could hold any number of distinct nodes. Every DAG built by the tree rule (section 5) stays far
   inside the bound: its file nodes take under 13 bytes per 4 KiB of content plus under 600 bytes, which the
   64 bytes per 4 KiB cover for any file of more than one chunk; its index nodes cost under 2 bytes per entry,
   and at most 48 bytes a level before the entries below them arrive. A fetch that would exceed it
   fails ("too large") without marking the root invalid. While the bound applies, a requester asks only for
   blocks that fit the room it leaves, counting a DAG node not yet received as `MAX_BLOCK_SIZE` (under the
   size-derived bound, a directory or index node as nothing, since it costs at most 64 bytes; under a quota,
   in full), and always for at least one block when none is in flight, so a fetch downloads at most about one
   block beyond either. Listing a directory (`ls`, the gateway's listings) fetches only its directory and
   index nodes, under the same bound for a size of 0.
6. A byte-range read (`cat`, the gateway's file responses and Range requests, a sealed file's header and its
   decryption by `get`, `cat` and `info`) is lazy: it fetches only the chunks covering the range and the file
   nodes above them, and skips empty children. The file nodes it meets, stored or downloaded, may not exceed
   what the range's size can hold (as in item 5, for the range's size), plus two file nodes of 256 children
   (13,061 bytes) for each level a file of the target's verified size can have under the tree rule with
   chunks of 4 KiB or more (one level up to 1 MiB, seven for any size): a range ends inside at most two nodes
   of a level. Otherwise it fails ("too large"); a requester counts a file node not yet received as
   `MAX_BLOCK_SIZE`, as in item 5, so a small range ending in two nodes of a level asks for them one at a
   time. The chunks are fetched a window (32 chunks) at a time, and the next window's are asked for before the
   reader is handed the current one's bytes, so the providers don't wait for the reader; a reader that stops
   early has had at most one window fetched that it didn't read.
7. Spread requests over up to `max_parallel_providers` providers (1 on slow nodes, where parallel transfers only
   contend for the same channel). A response *stalls* if it makes no progress for `stall_timeout`, or misses the
   same deadline the responder uses (section 3). A stalled provider's link is closed, which cancels the transfer,
   and its blocks are requested again from another provider; providers that keep failing are handled as in
   section 6. A fetch that fails or is cancelled, or whose reader stops early (item 6), closes the links of its
   requests still in flight, so their responses don't keep using the link and hold up the next `/blocks` to those
   nodes, unless another fetch has a request on the same link: closing it would fail that one, so the request is
   left to finish. It never cancels their Resources (section 3, Cooldown). When nothing is left to ask for and a
   provider is idle, a requester MAY ask it too for a batch that has been in flight for longer than
   `deadline_slack` (end-game duplication), so a response that keeps making just enough progress not to stall
   doesn't hold up the fetch. The first verified copy wins; the request still sending blocks that have arrived
   then has its link closed (left to finish, under the same rule), which doesn't count as a stall of its
   provider. rnfs duplicates each request at most once, and not on slow nodes, which use one provider at a time.
8. Blocks are stored as they arrive, so an interrupted fetch resumes where it stopped. An RNS Resource is only
   delivered when complete, so a response lost mid-transfer yields nothing and its blocks are requested again.
9. When the target is complete, the node provides it if its strategy says so (section 6).

## 8. Storage

- Blocks are stored in `<rnfs dir>/blocks/<xy>/<cid>`, one file per CID, written atomically after
  verification, and synced to disk before any pin or cache record that refers to them is saved, so a power loss
  can't leave a pin without its blocks or an empty file under a CID name. `<xy>` is the CID's third- and
  second-to-last characters, in that order: 1,024 subdirectories, named in lowercase base32 for both codecs (a
  CID's leading characters are the same for every CID of its codec, and its last carries only 2 or 3 bits).
- Blocks rnfs 0.1.6 and earlier stored directly in `blocks/` are moved into their subdirectories at the start
  (a rename within the filesystem; one already there makes the flat copy redundant, and it is deleted), and the
  directories synced. Every start moves what is left, so an interrupted move loses nothing, and a block whose
  move fails (on Windows, a file another program holds open) stays readable where it is until a later start.
- rnfs stores blocks only in real directories of its own: a subdirectory of `blocks/` that is a link (or a
  Windows junction), isn't a directory or belongs to another user stops the start; a loose one is made 0700.
  Links inside `blocks/` are never followed.
- Every stored block's size is kept in the state database too, written whenever pins or cache records are saved,
  so a start needn't list every block: after a clean stop rnfs reads them from there. After a stop that wasn't
  clean (a crash, a power loss, or the first start of 0.2.0) it lists every block, deleting partial files a crash
  left, and writes the sizes again. A block whose file turns out to be missing counts as a damaged block: deleted
  from the index once read, and fetched again if needed.
- rnfs keeps the rest of its state (pins, cache records, names, provider records, jobs, ...) in one SQLite
  database, `<rnfs dir>/state/rnfs.db`, readable by its owner only. The JSON state files earlier versions wrote
  under `<rnfs dir>/state/` are imported into it at the first start, each once and whole, and kept as
  `<name>.migrated`; the upgrade is one-way. A damaged database, or one written by a newer version, stops the
  node rather than being started over.

**Pins.**
- Pins are recursive: a pinned CID keeps every block of its DAG.
- A pin is held by one or more *sources*: `user`, `folder`, `name:<alias>`, `remote:<identity>`,
  `follow:<name>` (the latest target of a followed name, section 9), `deny` (the deny lists in use, below), and
  `app:<app>` or `app:<app>:<key>` (pins an app made through the local API with its own token, section 11),
  among them `app:lxmf:<address>` (links auto-fetched from that LXMF sender, section 13: the LXMF wrapper app's,
  made by the LXMF built into rnfs while it remains); names, identities and addresses are 32 hex characters.
  Until rnfs 0.2.0 LXMF's pins were `lxmf:<address>`: the upgrade to 0.2.0 rewrites each as
  `app:lxmf:<address>` (and those of an older version's `pins.json` as it is imported). An app's name is 1–64 characters of `a-z`, `0-9`, `.`, `_` and `-`,
  starting with a letter or digit (so never `:`), and a key 1–128 of `A-Z`, `a-z`, `0-9`, `.`, `_`, `:` and `-`.
  A pin lasts while any source holds it.
- `pin rm` removes only its caller's source: `user` for the CLI, and for an app only its own (`app:<app>`, or
  `app:<app>:<key>` with its pin key; section 11). Other sources are released by whatever created them: the folder
  scan; `name publish` (a name's sync too, section 9) or `name rm`; `/pin` with `false` or a remote quota being
  exceeded; `name unfollow`, or a followed name's newer target once that is complete; removing the LXMF messages
  that hold it; removing the app (`app rm`), which releases every `app:<app>` and `app:<app>:<key>` pin.
- Pins are saved at once; the state that records why a source holds a pin (the names' records, the followed
  names, the LXMF inbox) is saved separately, so a crash can leave them disagreeing. At start the node makes
  `name:` pins match the names' records (pinning a recorded target again, releasing the rest), and releases
  `follow:` pins of
  names no longer followed and `app:lxmf:<address>` pins no saved inbox message records (a queued auto-fetch is fetched
  again). A followed name's check releases every other target its source holds.
- That release needs the state read whole. If the followed names or the inbox were never saved (or were
  imported from a file that was damaged and moved aside as `.damaged`), or hold a malformed entry, such pins
  are kept and recorded again: a `follow:` pin's name is followed again, and a sender's `app:lxmf:` pins get a new
  inbox message listing them, whose removal releases them. A name key file that is there but can't be read
  keeps its name's pins and records; only a missing key file counts as a removed name.
- A pin is provided only once it is complete. Blocks of fetches, adds and imports in progress, and of queued or
  running jobs (section 11), are protected as if pinned.

**Cache** is everything else.
- Each cached fetch is recorded as the `<root>/<path>` it fetched, which keeps the path's nodes as well as the
  target's DAG.
- When storage exceeds `max_storage`, cached fetches are evicted least recently used first; `gc` evicts them
  all, and `cache rm <cid>` those leading to one CID (a pin of the same content keeps its blocks).
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
  less the bytes of the target's DAG already stored. If the free space there can't be read (the directory is
  missing, say), they fail without writing.

**Shared folder.** Every `scan_interval`, files and directories in `<rnfs dir>/files/` are imported as with
`add` (copied into the block store) and pinned with source `folder`. A changed or removed file releases its
old pin. Unlike `add`, a scan MUST skip (with a log line) a regular file that has other hard links, at the
top level and inside shared directories: it is also a file elsewhere, which whoever may write to the folder
could have linked in. It is imported at the next scan after its other links are gone. A scan recognises
unchanged content by its metadata rather than by reading it: an entry whose files' names, sizes, modification
times and execute bits are as before isn't read, nor, inside an entry that changed, a file imported before whose
device, inode, size, modification time and execute bit are as they were (its CID is reused while every block of it
is still stored and none is denied, and its content hash still goes through the deny list). Content rewritten in
place keeping all of these goes unnoticed until one changes. `files/` may be a
symlink, but a scan MUST NOT import from a folder that resolves, as opened, to the rnfs dir, to a directory
holding it, or into `storage/`, `names/`, `state/`, `blocks/` or `lxmf/`, which hold the node's keys and state:
rnfs logs a warning (once, while it stays so) and scans it as an empty folder: nothing is imported and every pin
with source `folder` is released (including those an earlier version made from it). rnfs also warns, once, when
a directory above the folder can be changed by other users, who could replace the folder with another.

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
  pins waiting (in its state database, about once a second and at stop) and resumes them after a restart,
  unless their identity has left `remote_pin_identities`; a crash may lose those accepted in the last second.
- `rnfs pin remote add|rm <node ID> <cid>` sends these requests over an identified link; `rnfs pin remote ls`
  shows their status (below, and section 11).
- **Status** (rnfs 0.2.0; local only: `/pin` and every other message are unchanged).
  - A node records each `/pin` it sends: the node, the CID, pin or release, when, and whether the node accepted
    it, one entry per (node, CID), replaced by a later request. An accepted pin is *done* once the node is among
    the CID's providers, as a provider lookup (or a record held locally) finds them: done is inferred from provider
    records, so it lags the node's publishing and is only as good as the DHT. A pin not found within a grace
    after asking (rnfs: 24 hours, a republish interval by default) is *waiting*, as it may still be fetched; a
    check after it that doesn't find the node shows it *missing*: dropped (quota, not found), never fetched, or
    no longer provided. A node that refused or didn't answer gives *refused*; an accepted release, *released*
    (never checked). Pins asked before rnfs 0.2.0 have no entry.
  - Checks are paced. rnfs checks in the background at most 60 pins an hour, 4 lookups at once, each pin first 10
    minutes after asking, then hourly while waiting and daily once done or missing; on a slow node at most 4 an
    hour, one lookup at a time, after an hour, then every 6 hours and every 3 days. An on-demand check
    (`pin remote ls --check`) is one lookup per pin, at most 32 pins a call (4 on a slow node), least recently
    checked first, skipping pins checked in the last minute (10 minutes on a slow node), its lookups sharing the
    same limit at once. rnfs keeps at most 10 000 entries: past that the oldest released or refused go first,
    then the oldest done, then the oldest of the rest.
  - A node serving remote pins records what became of each pin an identity asked of it: *waiting*, *fetching*,
    *pinned*, *dropped* (`quota`; the fetch failing: `not_found`, `denied`, `invalid_dag`, `too_large`,
    `storage_full`, `failed`; released by the deny list, `denied`; waiting at a start from an identity no longer
    on `remote_pin_identities`, `identity_removed`; recorded as current but lost in a crash, `lost`), *refused*
    (`denied`, `queue_full`, `failed`) or *released* by its identity, with when. Its pins and the pins queued
    are listed as it holds them, whatever was recorded; rnfs keeps the latest outcome of each pin, at most 1 000
    (past that those that ended go first, then pinned ones, oldest first; never one waiting or fetching). It
    reports each identity's use of `remote_pin_quota`, counted as the quota counts it.

**Deny lists.** A node MAY refuse chosen content, so operators of public gateways and provider nodes can keep it
off their node. Nothing on the wire changes.
- Entries are CIDs and file hashes, `sha256:<64 hex digits>`: the SHA-256 of a file's whole content. A deny
  list is UTF-8 text, one entry per line; blank lines and everything after `#` are ignored. The entries in use
  are those of the local list (`deny add|rm`) and of every source in `deny_lists`: `<cid>[/path]`, a fixed
  list, or `name/<name>[/path]`, one that follows the name.
- A list is untrusted input. One over 4 MiB or 100 000 entries, not UTF-8, with a line that isn't an entry, or
  that is a directory, is refused with a warning, and its source keeps the version it had. A version in use
  stays pinned (source `deny`), so a restart applies it before anything is fetched or provided; the version
  a newer one replaces is released. rnfs checks a name source every 10 minutes from the record held, through
  the network only when nothing was heard for 2 × `announce_interval`; a failed check is retried after 5
  minutes, doubling up to `announce_interval`.
- A denied CID is never fetched or read, whether it is the root or anywhere in the DAG being fetched, stored
  here or not: `get`, `cat`, `ls`, `stat`, `pin add`, `dir set`, `export`, the gateway, remote pins, followed
  names, LXMF auto-fetch and jobs fail, saying the content is on the deny list (the API and the gateway answer
  `451`). It is never stored (an add or import holding it fails and pins nothing), never served (`/blocks`
  answers it as a block not held), and nothing whose DAG holds it is provided: records already published are
  withdrawn. A `/pin` of it is refused.
- When the entries change, and at start: cached fetches reaching a denied block are dropped, and the blocks
  nothing else reaches deleted; remote pins reaching one are released, as they are kept for others, and their
  blocks deleted. The node's other pins (`user`, `folder`, `name:`, `follow:`, `app:`, `deny`) are kept,
  stored but neither served, provided nor read, with a warning naming each, until they are removed. Every
  deletion and release is logged.
- A file's hash is only known once its content is read, so file-hash entries are checked where content is
  assembled: before `get` writes a file out (nothing is left), before `cat` or the gateway sends any of a file
  (so a range fetches the whole file first; its hash is then remembered; the gateway's `?format=raw` and
  `?format=archive` too), and as `add`, the shared folder or `import` read it (the add or import fails). By then the file's blocks have been downloaded. A file-hash entry
  doesn't stop those blocks being fetched (by `pin add`, `export` to a file, a remote pin or a followed name), stored,
  served or provided (a file of several chunks is still served chunk by chunk, by `/blocks` and by the gateway's
  `?format=raw` of each block's CID): deny the CID for that. The first time a node refuses a file by its hash it
  logs a notice naming the command that does (`rnfs deny add <file cid>`). A file of one block has its content's hash as its CID's digest,
  so either entry stops it; a sealed file matches by its sealed bytes, as stored, not by what it decrypts to.
- Hashing a file reads every occurrence of content its DAG repeats, so a few small blocks can declare a file of
  2^56 bytes. While there are file-hash entries, a file whose hash isn't known yet and whose declared size is
  more than 16 times the bytes of its distinct chunks plus 64 MiB (`DENY_HASH_RATIO`, `DENY_HASH_SLACK`,
  section 12) can't be checked, and is refused as if denied, before any chunk is read (`451`; an add or import
  fails as above). The chunks are counted from the file nodes, so a range read (`cat`, the gateway) fetches the
  whole DAG first. Honest files repeat less, except highly repetitive ones over 64 MiB such as a zero-filled disk
  image.
- Provider records other nodes store here for a denied CID are kept and handed out as usual (`/find` sees
  only keys).

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
5. Announce, and re-announce every `announce_interval`. In private mode the announce reaches outsiders all the
   same (section 3), and the publish warns of it: rnfs adds a `warning` to the result (`POST /api/name/publish`
   and `rnfs name publish`, which prints it to stderr).

Before the first publish of a key imported from elsewhere, or found without stored state, the node resolves
the name through the network and asks keepers (below), since the key's earlier records may be held only there.

**Resolve** (`name resolve <name>`):
- The daemon registers one announce handler for `rnfs.name` (with `receive_path_responses=True`).
- To refresh, call `Transport.request_path(name)`, wait up to `path_timeout`, then read
  `Identity.recall_app_data(name)`.
- If the path request heard nothing within `path_timeout`, or heard only an announce emitted more than
  2 × `announce_interval` ago or a suspected replay (below), or always with `keeper_query = always`, ask keepers
  (below). Storing a provider record is free, and so is answering `/find`, so neither a record's age,
  nor the order a reply lists it in, nor how many nodes list it may decide which keepers are asked. Standing
  is read from the routing table as it was when the query started: a node the lookup meets doesn't gain rank
  from answering it, and one that fails a query meanwhile loses rank. An *established* node is one that was
  in the table for over `provider_seniority` then, and hasn't failed a query since last heard nor been silent
  for 2 × `announce_interval`.
  - Find providers of the name's key (section 6). Providers recorded locally are candidates, never a reason
    to skip the search, which runs until 3 × `alpha` are known and two established sources listed some, until
    some are known and `lookup_timeout` / 5 has passed, or until the lookup ends. A source is a node whose
    `/find` answer listed providers, or this node if it holds a record stored by an established publisher.
    Candidates are asked only once the search has stopped, never as they arrive, so reply order doesn't choose
    them.
  - Rank the candidates by routing-table seniority, as providers are (section 6): established servers first,
    then other servers in the table, then servers that failed a query or went silent (an owner that has
    left), then IDs not in the table; at random within each group.
  - Ask them with `/name`, a batch of `alpha` at a time, and offer every verifying record to the sequence
    rules, so the highest valid sequence wins. Each batch takes first, in rank order, candidates listed by a
    source no earlier candidate of the query was listed by (this node holding a record, an established node,
    or, counted as one source, none of these), then the rest in rank order. A candidate whose identity isn't
    known (each costs a path request) is asked within a budget of `alpha` such candidates per query, apart
    from the lookup's (section 6). Ask another batch while no answer reaches the highest sequence already
    known for the name (from an announce, an earlier resolve or a kept record), or while fewer than two
    keepers have answered; when no sequence is known, only established keepers' answers count toward those
    two. At most 3 batches. A slow node (`alpha` 1) asks at most 2 batches, and asks the second for want of a
    second answer only when it knows no sequence to compare the first against.
  - Each batch is waited for up to the lookup's time left or, if longer, a `/name` budget of its own:
    `path_timeout` + 5 s + 5 hops × RNS's link establishment timeout per hop (6 s) + 2 × RNS's request timeout
    without a measured round trip (11.25 s), 87.5 s at the defaults. A search that used all of
    `lookup_timeout` still leaves keepers time to set up a link and answer.
  - If the candidates run out before the answers settle the name, with batches left, and the search was
    stopped early (not ended by the lookup) and some of its candidates asked, the search resumes within the
    lookup's time left, stopping by the same rules for the candidates it newly finds, which are then asked.

  A keeper query (a DHT lookup plus a few requests) costs roughly ten to twenty times the airtime of a
  resolve a path response answers, so `always` SHOULD NOT be used on slow links.
- Return the record with the highest sequence held, and how long ago it was heard (for a record from a
  keeper, from its signed emission time, never in the future). RNS remembers a repeated old announce as just
  heard, so a record is counted as heard through an announce no later than 2 × `announce_interval` after the
  newest emission time (signed) RNS holds for the name, which repeating an old announce doesn't change (rnfs:
  from RNS's path table). A neighbour repeating an old announce can then keep a replaced record current for
  at most that long, plus the age a caller accepts (a follow's or deny list's 2 × `announce_interval`).
- An announce RNS stored that was emitted before the newest one its path to the name holds, or that carries
  another record than the announce the path was taken from, is a *suspected replay* (rnfs: read from RNS's
  announce cache, which holds the announces paths were taken from). Its record is heard no later than its own
  emission time, or the path's if its own is no longer known, and the resolve asks keepers. Without a path
  nothing is compared.
- A resolve that accepts a record up to some age (the gateway's 60 s, a follow's or deny list's
  2 × `announce_interval`) skips the refresh while the record held was heard within that age, or while a
  refresh of the name ended within it and the record it left is still held (rnfs: one that found nothing
  counts too, a short negative cache; one that left a suspected replay doesn't). With the owner offline a
  keeper's record keeps its old emission time, so it would otherwise go to the network every time. A resolve
  of a name being refreshed waits for that refresh, whatever age it accepts, and takes the record it leaves.
  A name republished moments ago may so show its older record on the gateway for up to 60 s.

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
  4 × `name_table_size` names, least recently resolved forgotten first, except that names resolved in the
  last 7 days are kept up to 8 × `name_table_size`; saved within 5 s of a resolve, announce or keeper record
  raising the floor, otherwise at least every minute and at shutdown.)
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
the disk reserve (section 8). It also has `follow_quota` (section 12), since a pinned fetch never stops at
`max_storage`: a new target whose verified size, or whose DAG's distinct blocks (stored or downloaded), would
exceed it is refused (a failed fetch, so the old target stays pinned), and `follow_quota` = 0 fetches nothing.
While one target replaces another both are held, so a followed name can use up to 2 × `follow_quota` for a
time. A target pinned before the quota was lowered stays pinned. A
followed name is checked when a newer record is heard and every `announce_interval`.
- The periodic check uses the record held, unless nothing was heard for the name for
  2 × `announce_interval`; only then does it resolve through the network (a path request, then keepers).
- A failed check is retried with exponential backoff: after 300 s, then 600 s, 1200 s and so on, at most
  `announce_interval` (never less than 300 s). The backoff restarts on success, or when the user follows
  the name again. A failed fetch is retried with the record held, without a path request. A name whose owner
  is gone for good therefore costs a flooded path request at most every `announce_interval`.

**Syncing a folder** (rnfs 0.2.0; local behaviour built on Publish: nothing on the wire changes). A node MAY keep
one of its names pointing at a local folder's current content (rnfs: `name sync <alias> <folder>`).
- Every `scan_interval` the folder is imported as the shared folder's entries are (section 8), the whole folder
  as one directory: unchanged content is recognised by its metadata rather than read, links, devices, sockets and
  files with other hard links are left out, and a folder that resolves, as opened, to the rnfs dir, a directory
  holding it, or into the folders holding its keys and state is refused. Files are cut with FastCDC (section 5),
  so an edited file shares most of its blocks with the version before: the root is the CID
  `add --chunker fastcdc` gives the folder.
- When the root differs from the one the sync last published and from the name's target, the name is published to
  it as `name publish` does (Publish, above): pinned with source `name:<alias>`, the previous target's pin
  released, so only the latest version is kept. The publish waits until:
  - a scan at least 60 s after the new root was first seen finds the folder unchanged, so a folder still being
    copied into isn't published half-copied (rnfs scans again then, if `scan_interval` is longer; a folder that
    changes while it is read counts as changing). A copy that stalls for longer than that can still be published
    part-way, and is published whole once it ends;
  - an hour has passed since the sync last published the name (rnfs: the time is saved, so a restart doesn't
    shorten it). RNS transport nodes count the announces of a destination arriving less than an hour apart
    (`announce_rate_target`, 3600 s by default on transport instances) and, after more than
    `announce_rate_grace` (5) of them, stop rebroadcasting it until an hour after the last one they let through
    (Limits, below): publishing faster would spend airtime on announces that don't get through.
- A folder that is missing, can't be read, is refused, isn't a folder or is empty (an unmounted drive's mount point
  looks empty), or whose import fails (the deny list, a full disk), pauses the sync: nothing is published, the
  name keeps its target, and rnfs logs why once and shows it in the sync's state. The sync resumes by itself once
  the folder is back.
- Stopping the sync (rnfs: `name unsync <alias>`) leaves the name where it last pointed, pinned; removing the name
  (`name rm`) stops it too. A name published by hand while synced keeps that target until the folder next
  changes. Two names may sync one folder: each imports it and publishes the same root, pinned by both. rnfs saves
  its syncs (the name, the folder, its last import, and the root last published and when), so they resume after a
  restart.
- In private mode each publish's announce reaches outsiders (section 3): starting a sync warns of it as
  `name publish` does (rnfs: a `warning` in `POST /api/name/sync`'s result, which `rnfs name sync` prints to
  stderr), and rnfs logs it once per sync rather than at each publish.
- Any caller of the local API may start or stop a name's sync, an app's token included, as any may publish the
  name; the name's pin is always the name's own, `name:<alias>`, never the caller's.

**Limits.**
- A name resolves only while its owner is online, a reachable transport node still holds its announce, or a
  keeper of it is reachable. Paths last 1 week on normal interfaces, less on access-point and roaming ones.
- A keeper can withhold a newer record but can't forge one; asking several keepers, and the owner (which
  keeps its own names), limits withholding.
- RNS transport nodes rate-limit rebroadcasts per destination (by default one an hour, after a burst of about
  6), so a name can be updated about once an hour sustained.
- RNS orders announces by a signed emission timestamp from the publisher's clock, so a publisher whose clock
  goes backwards can't update its name until old paths expire.
- A resolver can't detect a replayed old record when RNS holds no newer path to the name (a first resolution
  with no path, or after the path expired), nor when RNS's announce cache can't be read (a shared instance's
  client whose cache isn't the instance's). A node that missed a name's newer announces and is next to someone
  repeating an older one then gets the newer record from keepers only once the repeated announce is
  2 × `announce_interval` old (above).
- The same name key used on two machines can produce two records with one sequence; each resolver keeps the
  one it heard first.

## 10. Protection against misbehaving nodes

Nodes assume others may be faulty or hostile. Data integrity is guaranteed; availability is protected as far
as is cheap. Rules live in the sections referenced.

| Threat | Protection |
|---|---|
| Forged or corrupted data | Every block verified against its CID (7) |
| Size lies, repeated or empty-subtree bombs, deep trees | Verified sizes, `max_download_size`, `max_output_entries`, the DAG byte bounds of whole-DAG fetches, listings and byte-range reads, `MAX_DEPTH`, each block fetched once; while the deny list has file hashes, a file repeating its content more than `DENY_HASH_RATIO` times (beyond `DENY_HASH_SLACK`) is refused rather than hashed (5, 7, 8) |
| Malformed DAG nodes | Strict msgpack types, re-encoding and per-node checks (5). The tree rule gives honest writers one CID per content; readers don't enforce it |
| Advertising on someone else's behalf | Records name only the identified requester (6) |
| Advertising content it doesn't have, or stale records | Failed providers removed; the search runs again; withdrawals (6, 7) |
| Squatting or flooding provider records | Oldest-plus-random replies; long-known providers tried first; a record budget whose storage tiers, once it is full, keep the places of known nodes and of publishers seen refreshing for days, while strangers share a small, fast-expiring tier; no identity switching on a link (3, 6) |
| Hiding content near a key | Provider search doesn't stop at the first reply that lists providers (6) |
| Routing table takeover with new identities | Only proven-reachable nodes stored; full buckets keep long-lived nodes (6) |
| Fake node IDs in replies | Unknown IDs never stored; at most `alpha` path requests for them per lookup; lookup time budget (6) |
| Slow or stalled transfers (both directions) | Rates measured from transfers, never from round-trip time; response deadlines; displacement protection earned by progress (a per-response grace, then a throughput floor), so a response that transfers nothing can't hold a slot; no-progress and stall cancellation with refund; a cooldown on a link whose response made no progress, was rejected or cancelled before a quarter of it was sent, or fell far behind its schedule; many fast-link slots; requester stall detection, and end-game duplication of a response still arriving after `deadline_slack` (2, 3, 7) |
| Request flooding, link exhaustion | Request size, per-link and global limits; a per-link request rate limit; eviction of idle incoming links; half-open links capped in their own bucket, oldest closed first (3) |
| Decompression and oversized-response bombs | Compressed Resources refused; response sizes capped; multi-segment Resources refused on `rnfs.dht` links, LXMF delivery links and links to LXMF propagation nodes; `rnfs.dht` responses' msgpack headers checked before RNS decodes them (3, 7, 13) |
| Reading another node's download history | Only provided content is served (7). Under the default `roots`, complete cached downloads are provided; `pinned` keeps them private |
| Name rollback or forgery | Self-certifying names; RNS-verified signatures; persisted highest sequence (9) |
| Forged or withheld kept names | Records verified against the name's key; highest persisted sequence; several keepers asked, ranked by routing-table standing from before the query and drawn from different sources; answers from established keepers only settle a name with no known sequence (9) |
| Tampered or spliced sealed files | Per-segment HMAC bound to the header, index and final flag, checked before decrypting (5) |
| Unsolicited LXMF links, inbox flooding | Strict field parsing, size caps per link, per message and for the whole inbox; at most 50 kept messages per untrusted sender; new ones from untrusted senders rate limited per sender, then node-wide; auto-fetched pins never pushed out by others' messages, and each auto-fetch sender held to 1/n of the inbox; signature-validated senders only; rate-limited path requests for unknown senders; `lxmf_accept_from`, per-sender `lxmf_auto_fetch_quota` (13) |
| Unwanted content on a public gateway or provider node | Deny lists: denied CIDs never fetched, stored, served or provided, anywhere in a DAG; file hashes checked where content is assembled; lists size-capped and checked before use (8) |
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
  the DHT when a provider is known. Grinding IDs near a key is cheap: one CPU core makes about 20,000-30,000
  identities a second, each with its node ID (measured for rnfs 0.1.6), and getting `K` of them closer to a
  key than the `K` closest of n honest nodes takes about n tries, so a fraction of a second against 10,000
  nodes and under a minute against a million. Raising this cost means changing how node IDs are derived, a
  protocol change left for a later version.
- Patient attackers that keep identities alive for days defeat the preference for old records and long-known
  providers. Such identities, established in a resolver's routing table near a name's keeper key, can also
  answer its keeper query with a replayed older record of the name, unless the resolver knows a newer
  sequence or reaches a keeper they don't list (one recorded locally, or listed by another established node).
- A flood of new identities that fills the record budget shares the Newcomer tier with honest newcomers: each
  keeps about as many records as any flooding identity (the largest holders give way first), and their records
  expire 4.5 days after they were last stored while the store is full. Identities kept refreshing records for 3
  days become Earned: they then keep what they hold, though they can't take others' (Earned publishers never
  evict each other), so space they hold is lost to honest newcomers until they stop. A flood can also take the
  free space and the Newcomer floor, and absorb space honest publishers leaving the network free.
- Private mode doesn't hide names: a member's names are announced over RNS like any other, so outsiders who
  hear them learn each `[sequence, CID]` record and when it changes, though members won't serve them the content
  (section 3). `name publish` in private mode warns of this.
- Bans are evaded by switching identity; they only make an attacker pay for a new one.
- A provider can lower its own measured rate, and so slow a fetch from it, or drip a response just fast enough
  not to stall: without a rate estimate its deadline assumes `unmeasured_bitrate`, hours for one 64 KiB block.
  Once nothing else is left to ask for, another provider is asked for its blocks after `deadline_slack`
  (section 7); on slow nodes, which use one provider at a time, this can delay a fetch for a long time.
- Mass announces from new identities compete for RNS's announce bandwidth, which RNS itself caps.
- Many LXMF identities can keep the strangers' bucket empty (one message a minute, each from a new identity,
  is enough), so no other untrusted sender's message is kept; contacts and other trusted senders are unaffected
  (section 13). The node counts the messages dropped this way (`status`'s `untrusted_dropped`), so apps can
  suggest adding the senders the user expects as contacts. LXMF stamps (proof of work) would make each message
  cost the sender, but announcing a stamp cost changes the announce and refuses senders that haven't heard it.
- RNS decrypts and decodes every request packet and starts a thread for it before rnfs sees it, so a link
  sending requests as fast as its interface carries them still costs the node that work (about 0.14 ms of CPU
  each over loopback): the request rate limit (section 3) removes only rnfs's share. Likewise a node rejecting
  `/blocks` responses only after RNS has received them can be made to build one per cooldown per link, and
  RNS spends a key exchange on every new link.
- A `/blocks` requester that takes a response and cancels it still has the node build the whole response (read,
  hash, pack and encrypt it) each round, and the cooldown binds it only below a quarter sent (section 3):
  - cancelling at a quarter or more starts no cooldown, so it costs the requester at least a quarter of each
    response's bytes. Where a single window of parts is that much (a link whose MTU RNS raised, or a small
    response), cancelling after the first window is not told apart from it;
  - closing the link instead starts no cooldown however little was sent, deliberately: an honest reader that
    gives up closes its links, and in private mode a cooldown would hold back its own identity. Each round
    then costs a new link (a key exchange at both ends, and a place among `max_incoming_links`);
  - an honest requester whose Resource RNS cancels after running out of retries on a lossy path, before a
    quarter of it arrived, is cooled down at that node for `blocks_cooldown` and fetches from other providers
    meanwhile.
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
- An outside web site can frame a path gateway page and receive public content from it by `postMessage`
  (outside private mode, only content public by CID). A site's pages (section 11) can be framed only by pages
  of their own origin.
- Gateway sites share cookies: `localhost` is not on the Public Suffix List, so a browser takes every
  `*.rnfs.localhost` host as one *site* (`rnfs.localhost`), and every `*.name.localhost` host as another, and
  cookies ignore ports. A site's script can read and set cookies for its whole domain, which every other site
  of the same kind receives. Neither the gateway nor the API uses cookies, so this gives no access, but cookies
  large enough can make a browser's requests to every site of that kind too big to be answered (`431`) until
  the user clears them. Storage that matters (localStorage, IndexedDB, caches) is per origin, so per root.

## 11. Local interface

`rnfs daemon [--bootstrap <node ID>…]` runs a node. Other commands use its local API. The API and the gateway
are local conveniences, not part of the protocol.

| Command | Does |
|---|---|
| `add <path>\|- [--wrap] [--only-hash] [--name N] [--no-pin] [--to <recipient>…] [--to-self] [--no-sealed-name] [--chunker fixed\|fastcdc] [--background]` | Import a file, a directory or stdin, pin it with source `user`, print its CID. `--wrap` puts it in a directory; `--only-hash` just prints the CID; `--no-pin` keeps it as cache (section 8); `--to` seals it for recipients (section 5, Sealed files; a directory as one tar named `<directory>.tar`); `--chunker fastcdc` uses content-defined chunks (section 5); `--background` adds a path as an `add` job (below) and prints its ID |
| `add <destination>:/page/<path>\|<destination>:/file/<path> [the options above] [--identify]` | Add a snapshot of a page or file a NomadNet node serves, as a file (Adding from NomadNet, below); `--name N` wraps it as entry N; `--identify` identifies to the NomadNet node. A local file named like that is added as `./<name>` |
| `get <cid>[/path] [-o out] [--from <node ID>] [--background] [--raw] [--no-extract]` | Fetch and write out a file or directory. Without `-o` the output is, in the current directory, the target's last path name if it is printable, not hidden (no leading `.`), doesn't start with `-` (which a later command would read as an option) and is a valid file name here, else its root CID. A sealed file this node is a recipient of is decrypted, and without `-o` (for a bare CID) written under its sealed name by the same rule; a sealed folder (a sealed `.tar` starting with the sealed-folder marker, section 5) is unpacked to a directory (section 5, Unpacking), named without `-o` (for a bare CID, or a path ending in `.tar`) after the folder by the same rule, while a tar sealed as a file is written as that file; `--no-extract` writes it as the decrypted `.tar` instead; `--raw` writes the stored bytes |
| `cat <cid>[/path] [--offset N] [--length N] [--raw] [--block]` | Fetch and write a file, or part of one, to stdout; sealed files are decrypted as by `get` (only the segments covering the range), a sealed folder giving its tar (never unpacked); `--raw` writes the stored (encrypted) bytes, which is what scripts that show content to others must use; `--block` writes the CID's own block |
| `ls <cid>[/path]` | List a directory: name, kind, size, CID |
| `stat <cid>[/path]` | Size, block count, and how much is held locally |
| `url <cid>\|<name>[/path]` | Print the target's URL on its gateway site (`http://<k-cid>.rnfs.localhost:<port>/<path>` or `http://<name>.name.localhost:<port>/<path>`, below), for the running daemon's gateway port or else the configured one; needs no daemon |
| `dir set <cid>[/path] <name> <cid> [--exec]`, `dir rm <cid>/<path>` | Make a new directory with an entry added, replaced or removed, and pin it with source `user` (fetching the complete directory, file contents included) |
| `pin add <cid>[/path] [--background]`, `pin rm <cid>`, `pin ls [<cid>] [--json]`, `pin label <cid> [<label>]` | Manage pins; `ls` shows their sources, whether each is a sealed file (`sealed`: true, false, or null when its stored blocks can't tell), and a local name: the pin's label if it has one, else where it was added from on this machine (recorded locally, never sent; for a sealed file too), the shared-folder entry, or what a stored directory holds. `--json` also gives each pin's `label`, `origin` (where it was added from, kept apart from the label), `size` (as its root block states it), `entries` (a directory's entry count) and `added` (when first pinned, by any source; null for pins made before rnfs 0.2.0). `label` sets a pin's label (local only, lasting while the CID is pinned); without a label it clears it |
| `cache ls [--json]`, `cache rm <cid>…` | The cached fetches (downloads not pinned, section 8), most recently used first, with their size, completeness and whether the same content is pinned; `rm` evicts those leading to a CID now, deleting the blocks nothing else needs, as `gc` does for all of them |
| `pin remote add\|rm <node ID> <cid>`, `pin remote ls [<node ID>] [<cid>] [--check] [--json]`, `pin remote ls --served [--json]` | Remote pinning; `ls` lists the remote pins this node asked for (rnfs 0.2.0: node, CID, state, when asked and last checked; section 8, Status), `--check` checking them first, and `--served` those other identities asked of it, with each identity's use of `remote_pin_quota` |
| `gc`, `verify` | Evict all cached fetches; re-check every stored block against its CID |
| `providers <cid>` | Find nodes providing a CID |
| `ping <node ID>` | Open a link and report its round-trip time (`rnprobe` can't probe `rnfs.dht`, which doesn't send delivery proofs) |
| `name create\|rm\|export\|import <alias>`, `name list` | Manage name keys; `rm` releases the name's pin. A name can't move to another key, so losing its key loses the name for good: `create` and `import` say so and give the `export` command, the node remembers which keys were exported (by `name export`) or imported, and `status` lists the others (`unexported_names`; keys made before rnfs 0.2.0 count as never exported) |
| `name publish <alias> <cid>[/path]`, `name resolve <name>` | Mutable names. Every `<name>` argument is 32 hex digits or a name contact's nick |
| `name keep\|unkeep <name>`, `name kept` | Keep a name's signed record and serve it (section 9) |
| `name follow\|unfollow <name>`, `name following` | Keep a name's latest target pinned (section 9) |
| `name sync <alias> <folder>`, `name unsync <alias>`, `name syncs [--json]` | Keep a name pointing at a folder's current content, published at most once an hour once it has been unchanged a minute (section 9, Syncing a folder); `syncs` lists each sync's folder, state (`new`, `settling`, `waiting`, `synced`, `paused` and why), the root last imported and the one last published and when (rnfs: `POST /api/name/sync {"alias", "path"}`, `POST /api/name/unsync {"alias"}`, `GET /api/name/syncs`) |
| `export <cid>[/path] -o file [--background]`, `import file [--background]` | Archives; `import --background` runs as an `import` job |
| `jobs [list]`, `jobs show\|wait\|cancel <id>`, `jobs clear [<id>]` | Background jobs (below) |
| `contacts add <nick> <hex> [--kind name\|node\|lxmf]`, `contacts rm <nick>`, `contacts ls` | Contacts (below) |
| `deny add\|rm <entry>…`, `deny ls` | The local deny list (CIDs, `sha256:<hex>` file hashes); `ls` also shows each `deny_lists` source, the version in use and its state (section 8) |
| `send <address> <target> [-m text] [--propagate] [--nearest]`, `inbox`, `inbox get <n> [--link K] [-o out] [--use-sent-cid]`, `inbox rm <n>\|--all`, `outbox`, `lxmf address`, `lxmf sync [--nearest]` | LXMF (section 13), deprecated in 0.2.0: each prints a warning on stderr (stdout unchanged; `RNFS_NO_DEPRECATION_WARNINGS=1` silences it). `--nearest` uses the nearest propagation node heard when `lxmf_propagation_node` isn't set; `--use-sent-cid` gets a name link's sent CID when the name can't be resolved |
| `peers [--json]`, `stats [--json]` | Known nodes with hops, interface, traffic, the last link round-trip time and whether each is online (Peers, below); traffic since start, refusals by cause, per interface |
| `config [list] [--json]`, `config get <name>`, `config set <name> <value>`, `config unset <name>` | The settings (section 12): each with the value running, marked if `rnfs.toml` sets it, if a restart would change it, or if the daemon's command line sets it (which wins); `set` takes a TOML value (a bare word is text) and `unset` removes the setting from `rnfs.toml`, back to its default. Both edit `rnfs.toml` in place (other lines and comments kept), only once the whole result is valid, and say whether the change applied at once or needs a restart |
| `app add <name> [-o file]`, `app ls [--json]`, `app rm <name>` | Tokens for apps (below): `add` issues a token to an app and prints it once (only it, on stdout), or writes it to a new file only the user can read, saying on stderr what it lets the app do; `rm` revokes it and releases every pin the app made. Only the CLI's token may run these |
| `stop [--wait S]` | Ask the daemon to stop, as Ctrl+C or SIGTERM would (it saves its state first), and wait until its API no longer answers (rnfs: 30 s) |
| `status` | Node ID, RNS identity hash (the value other nodes list in `members` and `remote_pin_identities`), known nodes, records held, storage use, traffic, jobs, deny-list entries and sources, LXMF address, inbox size and untrusted senders' messages dropped by the rate limits (`untrusted_dropped`, section 13) (only the version without a readable API token). While the daemon starts, before its API listens, it and every other command say the daemon is starting, with its process ID (below) |

Wherever a command takes a target, it may start with a name contact's nick (`get alice/docs/report.pdf`);
where it takes a node ID (`get --from`, `ping`, `pin remote`), a node contact's nick works too.

**Output from other nodes.** Names, paths and text chosen by other nodes (DAG names, sealed names, LXMF
links and messages) MUST be printed and logged with non-printable characters escaped, so they can't inject
terminal escapes or fake log lines. A name chosen by someone else is used as a default output name only under
the `get` rule above. `get` writes out under a temporary hidden name in the output's directory and renames
it to the output once complete (refusing if the output appeared meanwhile), so a failed or interrupted
write-out never leaves a partial output under the requested name. A directory's entries MUST end up inside
it even if someone who can write beside or inside it swaps one of its directories for a link or another
directory while it is written; such a swap fails the `get`. rnfs: on POSIX the temporary directory is made
mode 0700 (setgid kept) and must still be empty once it is, every entry is created relative to its parent's
open directory without following links, and each directory is reopened from the root, component by
component, and checked against the device and inode it had when made; the temporary directory gets back the
mode it was created with when complete. On Windows it is created with the protected access list of the rnfs
directory (the user and SYSTEM only), every file's final path must be inside it, and once complete it
inherits its folder's access list again. Either way, it must still be the directory made before it is
renamed. A single file is created directly in the output's directory.

**API** (`127.0.0.1:8788`) and **gateway** (`127.0.0.1:8789`) are separate ports, so pages served by the
gateway can never reach the API.
- Both MUST reject requests whose `Host` header isn't `127.0.0.1:<port>` or `localhost:<port>`, which blocks
  DNS rebinding. The gateway also accepts its site hosts (below), matched as exactly; the API accepts nothing
  else.
- Every API request except `GET /api/status` MUST carry `X-Rnfs-Api: <token>`, compared in constant time.
  The token is random (at least 128 bits), made anew at each daemon start and written to
  `<rnfs dir>/api_token` with mode 0600 (on Windows, with a protected access list allowing only the user and
  SYSTEM); clients (the CLI, and apps through `rnfs.Client`, built on `rnfs.apiclient`) read it from there and
  refuse a file other users can access. Every local process, of every user, can connect to `127.0.0.1`, and
  the API reads and writes files as the daemon's user, so only users who can read the rnfs directory may use it. Web pages can't
  learn the token, and the custom header needs a CORS preflight, which fails: this also blocks cross-site
  requests. `GET /api/status` stays open for health checks and the proof check below, but without the
  token it MUST say no more than the daemon's version (rnfs: `{"version": ...}`); the full status (node ID,
  identity hash, LXMF address, traffic) needs the token like every other request.
- The daemon MUST refuse to start if the rnfs directory belongs to another user or can't be made accessible
  to its owner only (on Windows: a protected access list allowing only the user and SYSTEM, which the files
  below it inherit; Administrators are tolerated where an older version left them). On Windows a new rnfs
  directory and its subdirectories MUST get that access list when they are created (not after), the daemon
  MUST refuse a new one that already holds anything, and one that is a junction or symlink. Its existing
  subdirectories (rnfs: `state`, `blocks`, `storage`, `names` and `lxmf`) MUST be real directories (not
  symlinks or junctions) owned by the user, checked without following a link, and one others may access is
  made owner-only (rnfs: 0700, with a notice; if others could write to it, entries in it owned by another
  user are reported, as a warning). Anything else stops the daemon with an error before Reticulum starts.
  The settings file (`rnfs.toml`), the identity, and Reticulum's `config` when `reticulum_dir` is the rnfs
  directory MUST be checked before use, through the handle they are read from: one owned by another user
  (account) is refused, one others may access is made private (rnfs: 0600 on POSIX; with a warning), and one
  that can't be made private stops rnfs. Each MUST also be a regular file (not a symlink, FIFO or device),
  opened without following a link or waiting on a FIFO; anything else is refused. A symlinked `rnfs.toml` is
  refused too. rnfs reads at most 1 MiB of `rnfs.toml`. A start checks the rnfs directory, then reads the
  settings, then checks the subdirectories, then takes the directory's lock, in the daemon and in an embedded
  node alike; clients (the CLI, `rnfs.Client`) check `rnfs.toml` the same way when they read it, and show
  what was changed (the CLI prints it, `rnfs.Client` raises a `RuntimeWarning`, an embedded node logs it).
- **App tokens** (rnfs 0.2.0). Besides that token, which stays the CLI's, the node MAY issue applications
  tokens of their own (rnfs: `rnfs app add <name>`), each labelled with the app's name (section 8's app-name
  rule). A request naming an app in `X-Rnfs-App: <name>` MUST carry that app's token in `X-Rnfs-Api`, and passes
  only with it; a request without `X-Rnfs-App` passes only with the CLI's token, so an app's token is never
  taken for another's. App tokens outlive restarts. The node needn't keep the token itself: rnfs keeps
  `SHA-256("rnfs app token " + token)` (the app's *verifier*), from which the token can't be found, and checks
  a presented token against it. App tokens are not scoped: an app can do anything the CLI can (export name keys,
  change settings, stop the daemon), except manage apps, so an app's token is to be given only to an app trusted
  that far. Removing an app revokes its token at once, cancels its queued and running jobs and releases every
  pin it holds (a pin a request let in before the removal makes after it is released as it is made). Only the
  CLI's token manages apps (rnfs: `GET /api/apps`, `POST /api/apps/add {"name"}`, which answers the token, the
  only time it is given, and `POST /api/apps/rm {"name"}`); an app's token gets `403` with code `forbidden`
  there. While the LXMF built into rnfs is on, rnfs refuses an app called `lxmf`, whose pins (`app:lxmf:`) it
  would share (section 13).
- **An app's pins.** What a request with an app's token pins or releases for its caller (rnfs: `POST /api/add`,
  `/api/add/stream`, `/api/dir/set`, `/api/dir/rm`, `/api/pin/add`, `/api/pin/rm`, `/api/import`,
  `/api/import/stream`, and `pin`, `add` and `import` jobs, below) uses the source `app:<app>` instead of
  `user`, or `app:<app>:<key>` when the request carries `X-Rnfs-Pin-Key: <key>` (section 8's key rule; 400
  otherwise), so an app can hold and release groups of its own. A job keeps the source it was queued with, also
  across a restart. With the CLI's token a pin key is refused (400): its pins are always `user`'s. `pin ls`
  shows every pin's sources to every caller.
- An API request carrying `X-Rnfs-Challenge: <32 hex digits>` is answered with
  `X-Rnfs-Proof: hex(HMAC-SHA256(token, "rnfs api proof <port> <challenge>"))`, `<port>` being the API's.
  Before it sends the token, a client SHOULD check this proof with a `GET /api/status` on the connection
  that will carry the request (the CLI and `rnfs.apiclient` do, for every request), so a program another user
  runs on the port while the daemon is down learns neither the token nor a name key being imported, nor can
  answer in the daemon's place. The API keeps a connection open after a status request carrying a valid
  challenge and `Connection: keep-alive` (and no body), answering it with `Connection: keep-alive`, for one
  more request, which is its last; every other connection is closed after its request. Any caller can ask for
  this (the proof needs no token), and the second request gets a new read deadline, so one connection can hold
  a connection slot (below) about twice as long as a single request could (rnfs: about 2 × 15 s). This is
  accepted: holding every slot was already possible by opening connections, and the keep-open is bounded to
  exactly one more request. A request carrying `X-Rnfs-App: <name>` gets its proof keyed by that app's verifier
  (above), written as 64 lowercase hex digits, instead of the token: an app computes it from its own token, and
  checks the proof the same way before sending that token. No proof is sent for an app the node doesn't have.
  Whoever can read the node's state could answer an app's proof while the daemon is down, and so be sent that
  app's token; they can read the CLI's token already.
- The API never sends CORS headers.
- **Starting** (rnfs 0.2.0). The daemon locks `<rnfs dir>/daemon.lock`, which holds its process ID, before it
  opens its block store, and its API listens only once that is done, which takes a while for a large store after
  an unclean stop or at the upgrade (section 8). When nothing answers on the API port but a running process holds
  that lock, a client reports the daemon as starting, naming that process, rather than as not running (it may
  also be stopping, or an app's own node, which serves no API). The lock is tested only while the process the file
  names runs, by taking a shared lock for an instant: a daemon trying to lock the dir at that very instant would
  fail as if another held it.
- **For apps** (rnfs 0.2.0). While it runs, the daemon describes itself in `<rnfs dir>/daemon.json` (written
  atomically after its API listens, mode 0600 like the token, removed when it stops): its process ID, API and
  gateway ports, version, start time, rnfs directory, Reticulum config directory, whether it attached to a shared
  instance, and node ID. Clients read the port there first (refusing a symlink, a file other users could have
  written, or one whose process has ended), so a client finds the daemon even when it can't read a newer
  daemon's `rnfs.toml`; nothing else in it is trusted, and the proof above still comes first. Errors are
  `{"error": message, "code": code}`, the code machine-readable and never renamed once released, so a client
  tells "no such endpoint" (`no_such_endpoint`, a daemon that predates it) from "no such content" (`not_found`),
  both `404`; `GET /api/capabilities` lists the daemon's routes, features, event kinds, error codes and
  capabilities, and the features that are deprecated (`deprecated`: they still work, but a later release removes
  them; rnfs 0.2.0: `lxmf`, section 13). Every response of a deprecated feature's routes, errors included,
  carries `Deprecation: @<Unix time>` (RFC 9745, the time it was deprecated; rnfs's `/api/lxmf/` routes:
  `@1792368000`, 2026-10-19), and is otherwise unchanged. `POST /api/shutdown` stops the daemon as SIGTERM does,
  after answering (Windows has no other clean way for a program to stop a daemon running in the background). `GET /api/events` is a long poll on the
  node's events — jobs, pins, cached fetches, the LXMF inbox, names it holds, peers and remote pins (those it asked
  for and those it serves, section 8) — numbered within a run:
  answered as soon as there is an event after the sequence the client gives, or after a wait (at most 30 s),
  level-triggered like the name watch below, so a client asking again with what it was given loses nothing. A
  node keeps a bounded number of events (rnfs: 1024) and says `lost` when the client asks for some it no longer
  holds, or of another run (`epoch`): the client then reloads its lists. It SHOULD cap the calls waiting at once
  (rnfs: 8; more get `503`).
- Both servers SHOULD time-limit reading a request (rnfs: 15 s for the request line, headers and small
  bodies; 60 s between reads of an upload), but not the node's work, and SHOULD cap concurrent
  connections (rnfs: 32 for the API, 64 for the gateway), answering others `503` at once, so held-open
  connections can't exhaust threads or file descriptors.
- Both refuse HTTP/0.9 requests with a `400` carrying the usual headers, since an HTTP/0.9 response has none.

**Gateway:**
- Paths are `/rnfs/<cid>[/path]` and `/name/<name>[/path]` (the *path gateway*).
- **Sites (subdomain hosts).** `http://<k-cid>.rnfs.localhost:<port>/[path]` serves `<cid>/[path]` and
  `http://<name>.name.localhost:<port>/[path]` serves the name's target, so each root has an origin of its own
  and its pages can read its own DAG with `fetch()`. The path gateway stays as it is, and is the fallback.
  - `<k-cid>` is the CID's host form (section 4: `k` + base36), the only place it is accepted; it is converted
    to the written form at once, so `ETag`, listings, errors and logs show that. `<name>` is 32 hex characters,
    never a contact nick. Hosts are compared after lowercasing (browsers lowercase them), and must be exactly
    `<label>.rnfs.localhost:<port>` or `<label>.name.localhost:<port>`: no other or further label, no trailing dot,
    the gateway's port. Any other host gets `403` as before; an invalid CID or name in a site host gets `400`.
  - The path is a path in the root's DAG and nothing else (no `/rnfs/` or `/name/` prefix), in origin form
    (an absolute-form request target gets `400`); every segment must be a valid name, so an empty one gets `400`
    and a redirect to the slash form, rebuilt from the decoded names, stays on the site.
  - `?format=` gets `400` (formats are on the path gateway). A request carrying `Service-Worker` (the request
    for a service worker script) gets `403`: a worker on a name's site could go on serving old content after
    the name moves.
  - Every response, errors included, carries `X-Content-Type-Options: nosniff`, `Origin-Agent-Cluster: ?1`
    and `Content-Security-Policy: sandbox allow-same-origin allow-scripts allow-forms allow-popups; default-src
    'self' 'unsafe-inline' 'unsafe-eval' data: blob:; form-action 'self'; frame-ancestors 'self'`; with
    `gateway_scripts` off, `allow-scripts` and `'unsafe-eval'` are dropped and `script-src 'none'` added, as on
    the path gateway. `allow-same-origin` keeps the page's own origin (the sandbox still stops it navigating
    its top window or opening unsandboxed popups), and only its own origin's pages may frame it.
  - Everything else is as on the path gateway: no CORS headers (a page's `fetch()` of its own site is
    same-origin and needs none, and every other origin is refused for want of one), sealed files never
    decrypted, the deny list, sizes, `gateway_fetches` and the caching rules.
  - Browsers map `*.localhost` to loopback themselves; other resolvers may not, so tools should use the path
    gateway. Links between sites must be absolute (`http://<other>.rnfs.localhost:<port>/`), and the cookie
    caveat of section 10 applies.
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
  - The deny list (section 8) applies to those bytes as to any read: a `HEAD` of a stored file that is denied,
    by a CID read or by its content hash, answers `451` like `GET`. While the deny list has file hashes, the
    stored bytes are used only once the whole file is stored (its hash is checked first, never fetching).
- **Trustless responses** (`/rnfs/` only; `/name/` answers `?format=` with 400):
  - `/rnfs/<cid>?format=raw` returns the CID's own block, re-verified, as `application/vnd.rnfs.raw`; a path
    is refused (400).
  - `/rnfs/<cid>[/path]?format=archive` returns an archive (section 8) of the target, as
    `application/vnd.rnfs.archive` with `Content-Disposition: attachment; filename="<target cid>.rnfs-archive"`
    and an exact `Content-Length`; it is byte-identical to `rnfs export`. Its length must be known before
    its first byte, so the user's own `GET` fetches the whole DAG first, within `max_download_size` and
    `max_output_entries` (413 otherwise), and the fetch is cancelled if the client disconnects.
  - Every format is answered only to the user's own `GET`, which fetches what isn't stored. A request that
    isn't the user's own (`Sec-Fetch-Site` present and not `none`, or absent with a browser's `User-Agent`;
    below) and any `HEAD` get
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
- **Every path gateway response** carries `X-Content-Type-Options: nosniff` and `Content-Security-Policy: sandbox
  allow-scripts allow-forms allow-popups; default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob:;
  form-action 'self'`. Pages run in an opaque origin and can't load resources from other servers;
  navigation and WebRTC can still reveal the viewer. A node MAY disable scripts (`gateway_scripts`).
- The gateway's own refusals (a bad path or CID, a method other than GET or HEAD, a `409` or `416`…) and a `404`
  say what went wrong. Any other error from the node (`400`, `413`, `422`, `451` for content the deny list
  refuses, `503`, `507` or `500`) gets only a fixed message for its status; the details, which may name local paths, disk space or limits, go to the log.
- No response carries `Access-Control-*` headers, whatever the request's `Origin` or `Referer`. Pages run in an
  opaque origin, so they can load files of their own DAG as subresources (images, scripts, style sheets,
  frames) but can't read gateway responses with `fetch()` or `XMLHttpRequest`, and neither can other web
  sites. A gateway MUST NOT allow `Origin: null`: every site's sandboxed frames and `data:` pages send it.
  Pages that need to read their own DAG are served on their site's host (above).
- Content fetched for the gateway is cache and is never provided. Names it resolves go only into the capped
  table (section 9).
- Requests that aren't the user's own navigation (`Sec-Fetch-Site` other than `none`) may start at most
  `gateway_fetches` network fetches or name resolutions at a time. Content already stored is served without
  limit. Browsers send `Sec-Fetch-Site` to `127.0.0.1` and `localhost` (and `*.localhost`, so to sites too: a
  site page's own `fetch()` is `same-origin`, so limited), and `none` only for a navigation the user started
  (address bar, bookmark). A request without it is the user's own only if its `User-Agent`
  doesn't start with `Mozilla/` (rnfs ignores case and leading spaces): then it comes from a local tool the
  user runs, such as curl. Every browser's `User-Agent` starts with `Mozilla/`, and browsers without Fetch
  Metadata (Safari before 16.4, Firefox before 90) send no `Sec-Fetch-Site` for a web page's requests either,
  so all their requests are treated as a web page's: in them even the user's own navigation gets `409` for
  `?format=` and is held to `gateway_fetches`.
- Apart from timing, the `gateway_fetches` limit and content no provider has, a web page's request gets the
  same answer whether the node stores the content or has to fetch it. The limit is a known, accepted leak: while a
  page keeps every slot busy with fetches, stored content is still served and content that needs a fetch gets 503,
  so the page can tell them apart. Like the timing difference, closing it would cost more than it protects.
  None of these applies to `?format=`, which a web page's request never gets (409, above).
- The gateway never expands contact nicks (`/name/<nick>/` is not supported), so web pages can't use or probe
  the address book.

**Background jobs.** A node MAY run long transfers as *jobs* it owns, so a transfer doesn't depend on one API
request or terminal staying open. A job has a kind — `get` (fetch a target and write it to an output path,
optionally from one node), `pin` (fetch a target completely and pin it with source `user`, or the app's that
queued it: App tokens, above), `export`
(write an archive of a target to an output path), `add` (add a file or directory at a path on the node's
machine, or a NomadNet link's snapshot (below), with `POST /api/add`'s options) or `import` (import an archive
at such a path) — and a state: `queued`, `running`, `done`, `failed` or `cancelled`. Its result, when done,
MUST be what the equivalent API call returns (`POST /api/get`, `/api/pin/add`, `/api/export`, `/api/add`, `/api/import`), and its error, when failed, the
message that call would have reported.
- At most `max_jobs` jobs run at once; queued jobs MUST be started oldest first.
- Jobs MUST be persisted in the state directory, each change saved atomically and durably. When the node starts, jobs that were
  queued or running MUST be queued again. A resumed job MUST NOT fetch again blocks that are still stored.
- While a job is queued or running, the blocks stored below its root MUST be kept as if pinned, including
  across a restart, so a resumed job continues where it stopped. A failed or cancelled job's blocks are
  ordinary cache (`get`, `export`) or unreferenced (`pin`), and a new job for the same target fetches again
  whatever was evicted meanwhile.
- Cancelling a queued job MUST end it at once; cancelling a running one MUST stop its fetch at the fetch's next
  step (within 0.5 s), not at the end of a request in flight, whose link is then closed (section 7). Writing
  out (`get`) or the archive (`export`) after the fetch is local and is not interrupted. A running `add` or
  `import` MUST stop at its next read of the file or archive; what it stored by then is unreferenced, for the
  next collection. A running `add` of a NomadNet link stops its fetch within 0.5 s, closing its link.
- `add` and `import` jobs fetch nothing and hold no blocks while queued: one interrupted (or queued at a
  restart) runs again from the start, which is safe because adding and importing are idempotent (the same
  content gives the same CIDs; a sealed add seals afresh, so its CID is the finishing run's; an add of a NomadNet
  link takes a new snapshot). Their path MUST exist when queued (404 otherwise); they have no target or output, so
  the output rules don't apply.
- Stopping the node MUST NOT wait for jobs beyond a short bound (rnfs: the node's stop budget, 5 s for every
  thread it ends); a job interrupted by the shutdown is saved as queued and resumes at the next start.
- At most `max_finished_jobs` (at least 1) finished jobs are kept, the oldest finished forgotten first, but
  a finished job is kept for at least 60 s whatever the cap, so a client following a fast job always finds
  its result. At most 1000 jobs may be queued or running (more are refused as busy). Two queued or running
  jobs MUST NOT share an output path, and a job's output path MUST NOT exist when it is queued.
- A `get` job writes to a temporary hidden name beside its output (rnfs: `.rnfs-job-<id>.partial`) and renames
  it when complete; if the output appeared meanwhile, the job fails. A failed, cancelled or interrupted job's
  temporary output is removed (a resumed job removes one left by a crash).
- A `get` job MAY ask (`sealed_name`) that a sealed file this node opens be written under its sealed name, if
  that passes the `get` default-name rule, in the output's directory; the final path is then the job's output.
  For a sealed folder unpacked that name is its top directory's (the result's `name`). A `get` job MAY ask
  (`no_extract`, kept with the job) that a sealed folder be written as its tar rather than unpacked.
  A `get` job with a `from` node MAY ask (`fallback_dht`) to be retried once with ordinary DHT discovery if it
  fails for any reason other than cancellation, a size limit or a full disk.
- A get's result (and a job's) keeps at most the first 100 warnings, then one saying how many more there were,
  plus `warnings_total`.
- Progress reported for a job: `total_bytes` (the target's verified content size, once its block is
  checked), `done_bytes` (content bytes of the target's chunks stored, whether downloaded by this run or
  already stored), `blocks` (distinct blocks met by this run), `blocks_total` (blocks of its DAG, once
  complete), `downloaded_bytes` (bytes downloaded by this run, DAG nodes included), `rate` (bytes/s
  downloaded over the last 300 s of the run, falling while nothing arrives) and `eta`
  (`(total_bytes - done_bytes) / rate`). For `add` and `import`: `total_bytes` is what they will read (the
  sizes of the regular files an add reads, measured before reading, or the archive's size), `done_bytes` what
  they have read, and `rate` bytes/s read.
- A node with an event log (rnfs: `GET /api/events`) publishes a `job` event at every change of a job's state,
  when it is forgotten (`removed`: true), and at most once a second for a running job's progress, with its `id`,
  `job_kind`, `state`, `error` and progress fields but not its result.

**Adding from NomadNet.** A NomadNet node serves pages and files as link requests on its `nomadnetwork.node`
destination. A node MAY add what one serves, as a one-off snapshot (rnfs: `add <destination>:/page/<path>` or
`:/file/<path>`; `POST /api/add` with `nomadnet` instead of `path`; an `add` job with `nomadnet`; `add_nomadnet` in
the library). Never through the gateway: web pages must not make the node fetch.
- **Syntax.** 32 hex digits (the `nomadnetwork.node` destination hash), `:`, then `/page/<path>` or `/file/<path>`:
  components separated by single slashes, each a valid entry name (section 5: not empty, `.` or `..`), printable,
  at most 1024 characters in all. Request data (a page's form fields or `var_` values, written after a backtick)
  is not supported. On the command line a local file named like a link is added as `./<name>`.
- **Fetch.** The node finds a path to the destination and recalls its identity (each within `path_timeout`), links
  only to the `nomadnetwork.node` destination of that identity whose hash is the one given, waits for the link
  within RNS's establishment timeout (rnfs: plus 5 s), and sends one request for the path with no data. A page
  comes back as bytes (an executable page as rendered for this request), a file as a Resource whose metadata
  carries its `name`. Until an answer starts, RNS's request timeout applies (and rnfs's hard deadline, twice it);
  once it does, the fetch fails if it makes no progress for `stall_timeout` or isn't complete within its size at
  32 bytes/s. The link is closed when the fetch ends. Nothing answering (NomadNet sends nothing for a path it
  doesn't serve or an error) fails the add.
- **Identity.** The link is anonymous unless the user asks to identify (rnfs: `--identify`); it then identifies
  with an identity kept for this alone (rnfs: `<rnfs dir>/storage/nomadnet_identity`, created on first use, private
  like the node's), never the node's, so allowlisted pages can be fetched without tying them to the node ID.
  NomadNet answers a visitor its `.allowed` list doesn't name with its own "Request Not Allowed" page rather than
  an error: an answer that is that page (line endings and surrounding whitespace aside) fails the add instead of
  being added. On an identified link it is retried once after identifying again (`Link.identify` is
  unacknowledged).
- **The snapshot** is added as data is (a file, chunked, checked against the deny list, pinned with the caller's
  source, `user` or an app's, unless `--no-pin`, sealed with `--to`), and the pin's origin (`pin ls`) is the link. Its name, used with `--wrap`
  and sealed inside, is for a file the name the node sent if it is a valid entry name, else the path's last
  component; for a page always the path's last component. `--name` wraps it under that name instead. Each add
  takes a new snapshot. An `add` job's progress is the answer's bytes received of its advertised size; one
  interrupted fetches again from the start.
- **Limits, per link.** NomadNet compresses files up to 32 MB and sends anything over one Resource segment
  (`RNS.Resource.MAX_EFFICIENT_SIZE`) as several, while the daemon's process-wide decompression limit is 0 and
  RNS checks neither a segment's decompressed size against the advertised one nor the segments' sequence. So the
  node bounds the link it opened, and nothing else (section 3: `rnfs.dht` links keep every rule, the daemon's
  process-wide limit stays 0, an embedded node leaves its host's settings alone):
  - the request carries `max_response_size` = the smaller of `max_download_size` (if set) and the free space above
    the disk reserve where the answer is copied;
  - a `resource_started` callback on the link gives each segment `max_decompressed_size` = `MAX_EFFICIENT_SIZE` + 1
    and requires the first to be segment 1 whose original hash is its own hash, and each to carry the first's
    original hash, total size and metadata flag, the segment count an honest sender computes from that total
    (1 up to `MAX_EFFICIENT_SIZE` bytes, else ⌊(total − 1) / `MAX_EFFICIENT_SIZE`⌋ + 1), and the index after the
    previous segment's, which must be complete; anything else cancels the transfer and closes the link;
  - as each segment completes, what RNS has assembled must be no larger than the advertised total, and a file's
    metadata (stored by RNS before it decodes it) must be a map of at most 16 entries whose keys are strings and
    whose values are strings, integers, booleans or nil, at most 4 KiB in all; otherwise it is removed unread and
    the link closed;
  - a page answer must be `[request_id, page]` with the page one `bin` or `str` (or `nil` or `False`, which fail the
    add), checked from its msgpack headers before RNS decodes it, as `rnfs.dht` responses are (section 3); a file
    answer is copied, no larger than the advertised total, inside the response callback (RNS deletes it after);
  - the advertisement checks of section 3 that cover every link in the daemon (no request Resources, transfer size
    within data size and one segment's) apply to these links in an embedded node too; the `rnfs.dht`-only ones
    (one segment, no metadata, no compression) don't.

**Streams.** For a client that can't share files with the node (a sandboxed app), the API also carries content
in its requests and responses (rnfs: these routes):
- `POST /api/add/stream?format=tar`: the body is a plain tar, read once in order and never held whole. Its
  regular files and directories become a directory (wrapped as `name` if given) under the rules of adding a
  directory: members that are symlinks, hard links, devices or FIFOs, whose path is absolute or has a `..`
  component, or whose names break the entry-name rules are skipped with a warning; directories a path implies
  are made; a later file replaces an earlier one of the same path, and a member conflicting with what is
  there as the other type is skipped with a warning. A file is executable if its mode has the owner's execute
  bit. Sealing is refused (400): a sealed folder is added from a path (`POST /api/add` with `to`), where the node
  packs the folder itself (section 5). The node MUST bound what a tar can make it hold: rnfs refuses (413) more than
  `max_output_entries` files and directories or nesting deeper than `MAX_DEPTH`, refuses (400) extended headers
  over 64 KiB, more than 8 in a row, global headers of more than 4 KiB in all, and sparse files, and keeps 100
  warnings (counting the rest), each naming at most 300 characters of the member. A malformed header anywhere,
  anything but zeros after the tar's end (as a zeroed header in its middle leaves), or a body ending inside the
  tar, is an error (400), never a shorter directory. The result is `/api/add`'s, the root pinned `user` (or cached with `pin=0`).
- `GET /api/get/stream?target=&format=tar`: the target as a tar, its DAG fetched first within
  `max_download_size` and `max_output_entries` (413) and checked against the deny list (451) like `get`, then
  sent with an exact Content-Length, so a client can tell a transfer cut short. A directory's entries are the
  tar's top level, each directory before its contents; a file is one member named by the target's last path
  name (its CID for a bare CID). Member paths are relative and never hold `..` (DAG names hold no `/`); an entry
  whose name holds a character some extractors read as a separator (rnfs: `\`, and the fullwidth `＼` and
  `／`), or starts as a Windows drive does (`C:`), is skipped (with what is below it), counted and logged (rnfs: the first 10 by
  name). Every limit, small chunks included, is checked before the response starts. Files are their stored bytes (a sealed file stays sealed, as in a directory's `get`).
- `GET /api/export/stream?target=`: the target's archive, byte-identical to `export`, with the same limits and
  an exact Content-Length; `POST /api/import/stream`: the body is an archive, imported as `/api/import` does.

**Watching names.** A node MAY let local clients wait for newer records of names (rnfs: `GET /api/name/watch`,
which rnfs.Client's `names.watch` uses). It answers from the records the node holds, under the sequence rules
of section 9, with those above a sequence the client gives, as soon as there is one or after a wait (rnfs: at
most 30 s), so a client that asks again with what it last received loses nothing between calls. A watched
name's announces count as asked for (section 9: a full table makes room for them), during each call and for a
while after it (rnfs: 60 s); watching sends nothing to the network. A node SHOULD cap the names per call
(rnfs: 64), the calls waiting at once (rnfs: 4, so watchers can't take the API's connections; more get `503`)
and the names watched (rnfs: 1024). `GET /api/cat` takes `from`, a node to fetch from, as `GET /api/info` does.

**Peers.** `GET /api/peers` lists the nodes this one knows (in its routing table, with an open link, or met since
it started) with what it already knows of each, gathered without network I/O. Besides the path and traffic fields,
each has `first_heard` (when this node first heard of it, as far as it knows: rnfs keeps nothing of its peers
across a restart, so it is since the start unless the routing table or Reticulum's own announce cache knows an
earlier time; null if nothing does), `rtt` and `rtt_at` (the round trip measured on the last link this node
established to it, a ping's included, in seconds, and when; null if none) and `online`, decided by the node so
every client applies the same rule: online while a link to it is open, or within a window (rnfs: 15 minutes) of
the last sign it was up — an answer to a request (a refusal too), a request of its own served, a link established,
or an announce of its own emitted within the window (by the emission time signed in it, allowing some clock
difference, rnfs: 5 minutes, and later than the last one counted). A path response doesn't count, nor an announce
delivered late (a transport node may answer a path request long after the node went away, an announce queue may
hold an announce for hours, and a shared instance passes path answers on to its clients as announces). The window
is measured on a monotonic clock, so a step of the wall clock changes nothing. A node up but silent therefore shows
offline (`ping` tells for sure). A node SHOULD publish `peer` events (`{"node_id", "change", "online"}`) when a
peer is first heard or contacted (`new`) and when its online state changes (`online`, `offline`, checked
periodically: rnfs every 30 s, over the peers it met this run or has a link to); rnfs publishes none for the peers
it knows when it starts, so a client lists them once and then follows the events, taking `new` as add-or-update.

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
setting. A setting an earlier version had and this one retired is ignored with a warning, so settings an older
version accepted still start the node (rnfs: `max_records`, replaced by `record_budget` in 0.2.0); it can't be set
any more, only removed.

A node MAY let local clients change its settings while it runs (rnfs: `rnfs config`, `POST /api/config/set`,
with each setting's type, range, unit, default, description and, for a setting that still works but is going
away, why (`deprecated`: rnfs 0.2.0's `lxmf_*` settings, section 13) from `GET /api/config`, which also warns
about the deprecated settings the file sets, as setting one through the API does). A change MUST be refused,
leaving the settings file as it was, unless the whole resulting file is valid by itself and with the node's
command-line options, under the same checks as at start (ownership, no symlink, size). Options given on the
command line keep winning over the file. rnfs applies these at once, the rest at the next start: `max_storage`
(enforced at once), `max_download_size`, `max_output_entries`, `max_upload_rate`, `remote_pin_quota`,
`scan_interval`, `max_jobs`, `max_finished_jobs`, `serve_interfaces`, `no_serve_interfaces`, `quiet_hours`,
`keep_max_names` (names kept beyond a lower cap stay, as at a start), `keeper_query`, `follow_quota`,
`lxmf_accept_from`, `lxmf_auto_fetch_from`, `lxmf_auto_fetch_quota` and `lxmf_send_from_hint`. Settings of
identity, ports, Reticulum, the DHT mode and private mode never apply before a restart.

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
| `provider_seniority` | 1 day | Providers (and keepers) in the routing table this long are tried first |
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
| `max_upload_rate` | none; half the rate estimate on slow links that have one | An explicit value applies to every `/blocks` response, with or without a rate estimate |
| `record_budget` | 1 GiB | Disk the provider records stored for others may take, never the free disk the blocks' reserve needs; past it, the storage tiers decide whose records stay (section 6). Replaces 0.1.x's `max_records` (a record count), which a node ignores with a warning |
| `ban_time` | 1 h | |
| `max_storage` | 10 GiB | |
| `max_download_size` | none | |
| `max_output_entries` | 100 000 | |
| `name_table_size` | 10 000 | Records kept for names this node hasn't resolved through the CLI or API (four times as many sequence floors, section 9) |
| `remote_pin_quota` | 1 GiB per remote identity | |
| `remote_pin_identities` | empty | Identities allowed to send `/pin` |
| `private` | off | Private mode (section 3) |
| `members` | empty | Private mode's member identities |
| `scan_interval` | 60 s | Shared folder, and folders synced to names (section 9) |
| `provide_strategy` | `roots` | `roots` (pins and complete cached downloads) or `pinned` (pins only) |
| `gateway_scripts` | on | |
| `gateway_fetches` | 2 | Network fetches at a time for requests that aren't the user's own navigation |
| `api_port`, `gateway_port` | 8788, 8789 | Local API and gateway ports, on 127.0.0.1 (section 11) |
| `bootstrap_nodes` | empty | Node IDs contacted first when joining (section 6) |
| `default_bootstrap` | on | Also contact the default bootstrap nodes when joining; never in private mode (section 6) |
| `reticulum_dir` | RNS's own | RNS config directory, the one `rnsd` uses by default |
| `verbosity` | 0 | Extra logging: 1 per-query detail, 2 RNS debug output |
| `debug_log` | off | Also write every rnfs log message (at every level), RNS's own at Info and more severe, and each gateway request to a debug log file in the rnfs dir, readable by its owner only. Local only: nothing in it is sent |
| `upload_budget` | none (unlimited) | Bytes per hour of `/blocks` responses, node-wide; a token bucket charged for bytes sent, at most half of it per requester in flight (section 3) |
| `serve_interfaces` | empty (all) | Serve `/blocks` only on these RNS interfaces (names or prefixes, case-insensitive; a spawned interface also matches its parent's name) |
| `no_serve_interfaces` | empty | Never serve `/blocks` on these interfaces; takes precedence over `serve_interfaces` |
| `quiet_hours` | none | Local time ranges such as `22:00-06:00` or `01:00-05:00,12:00-13:00`, during which `/blocks` is refused |
| `keep_max_names` | 100 | Names kept (keep and follow) for others; this node's own names don't count. A whole number |
| `keeper_query` | `fallback` | `fallback`: ask keepers when a path request heard nothing; `always`: on every network resolve (about 10× the airtime) |
| `follow_quota` | 1 GiB per followed name | Bytes a followed name's new target may hold, its size and its DAG's blocks; 0: followed names aren't fetched. A swap holds the old and new targets, so up to twice this (section 9) |
| `max_jobs` | 2; 1 on slow nodes | Jobs running at once (a whole number ≥ 1). Foreground `get`, `pin add` and `export` run as jobs too, so they queue beyond this |
| `max_finished_jobs` | 100 | Finished jobs kept (a whole number ≥ 1); each is kept at least 60 s regardless (section 11) |
| `lxmf_enabled` | on if the LXMF package is installed | A boolean. `false` turns LXMF off; `true` without the package logs a warning and stays off. Like every `lxmf_*` setting, deprecated in 0.2.0 (section 13) |
| `lxmf_display_name` | `rnfs node` | Announced with the LXMF address; 1–64 printable characters |
| `lxmf_send_from_hint` | `true` | Sent links carry `from=<node ID>` as a provider hint; this tells recipients which rnfs node the LXMF address belongs to. `false` omits it |
| `lxmf_propagation_node` | none | Propagation node (32 hex digits) used for `--propagate`, the fallback of failed direct messages and the periodic downloads. One only heard announcing itself is used only with `--nearest` |
| `lxmf_sync_interval` | 1 h | Downloads from `lxmf_propagation_node`; 0: never |
| `lxmf_accept_from` | empty: everyone | LXMF addresses whose messages are kept |
| `lxmf_auto_fetch_from` | empty | LXMF addresses whose links are fetched and pinned (`app:lxmf:<address>`; `lxmf:<address>` before 0.2.0) |
| `lxmf_auto_fetch_quota` | 1 GiB per sender | Storage the pins of one auto-fetch sender may use |
| `deny_lists` | empty | Deny-list sources: `<cid>[/path]` or `name/<name>[/path]`, each a file (section 8) |

**Local limits.** Fixed by this implementation rather than set; other implementations may choose their own.

| Name | Value | Notes |
|---|---|---|
| `DENY_HASH_RATIO` | 16 | While the deny list has file hashes, a file declaring more than this times the bytes of its distinct chunks, plus `DENY_HASH_SLACK`, is refused unhashed (section 8) |
| `DENY_HASH_SLACK` | 64 MiB | Any file up to this size can be hashed, however much it repeats |

## 13. rnfs URIs and LXMF

**Deprecated (rnfs 0.2.0).** rnfs's own LXMF messaging (sending, receiving, the inbox and outbox, auto-fetch,
the `lxmf_*` settings, and the commands, API routes, library methods and `inbox` events for them) is deprecated:
it is moving to a separate app built on the local API (section 11) and rnfs's library, and a later release
removes it from rnfs. Until then it works as this section says, and says it is deprecated wherever it is used:
the node logs so at start while LXMF runs or an `lxmf_*` setting is given; each command warns on stderr; every
response of the `/api/lxmf/` routes carries a `Deprecation` header and `GET /api/capabilities` lists `lxmf` under
`deprecated` (section 11); the settings schema marks each `lxmf_*` setting `deprecated`; and the library's
methods warn `DeprecationWarning`. Nothing on the network changes. The `rnfs:` URIs and the link-map field below
stay part of this specification: they are the interop contract that app, other clients and rnfs itself (a target
may be an `rnfs:` URI) keep using. The rules for messages, receiving and auto-fetch move to that app's
specification once rnfs drops LXMF; its pins already use the source `app:lxmf:<address>` (section 8), which the
app owns.

rnfs links can be sent and received as [LXMF](https://github.com/markqvist/LXMF) messages, so content can be
shared with people using Sideband, NomadNet, MeshChat or any other LXMF client. This is optional and not part
of the `rnfs.dht` protocol: a node without the LXMF package (or with `lxmf_enabled = false`) does none of it.
Nor does rnfs while an app called `lxmf` exists (section 11, app tokens): that is an LXMF wrapper built on the
local API, which owns the `app:lxmf:` pins and the LXMF identity, so rnfs's own LXMF then neither starts nor
releases any of those pins at start.

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
- On a delivery link, a Resource of more than one segment is refused before any of it is transferred: LXMF
  checks only the size each segment advertises for itself, and a message within the 256 kB delivery limit
  always fits one segment.

**Receiving.**
- Senders in `lxmf_accept_from` or `lxmf_auto_fetch_from`, and LXMF contacts (section 11), are *trusted*.
  Anyone else can make as many LXMF identities as they like, so the bounds below protect trusted senders'
  messages from them.
- A message is accepted only once LXMF has validated its signature with the sender's identity. A message from
  a sender whose identity isn't known yet is held while a path request fetches it, then validated: at most
  32, at most 8 claiming one sender, for up to 2 minutes, and only 30 seconds after its path request was sent
  (or after it arrived, if later) if no answer has made the identity known. A ninth message claiming one
  sender takes the place of the oldest of its 8, so forgeries sent first can't keep the sender's real message
  out. When 32 are held, a further one is dropped, so held ones are not pushed out, unless it claims a trusted
  sender: then it takes the place of the newest message held for the untrusted sender holding the most. The source of such a message is
  unauthenticated until its identity is known, so path requests for held senders are sent at most once per
  sender per 2 minutes, and at most 3 at once, then one per 10 seconds node-wide, trusted senders first.
- If `lxmf_accept_from` is not empty, messages from other senders (except those in `lxmf_auto_fetch_from`)
  are ignored, and those from unknown senders are not held. Messages without rnfs links are ignored.
- The links come from the field if it is valid, else from the first 16 384 characters of the content. A link
  on a line of its own is read whole; elsewhere, trailing punctuation (`.,;:!?)]}'"`) is taken as the
  sentence's. At most 8 links, 4000 characters of text, 256 of title and 64 of display name are kept. Text
  from messages MUST be shown with control characters escaped.
- A sender's display name is its own word, so it MUST NOT be shown where it could pass for the sender's address
  or trust. `inbox` shows each message's sender address first, then `untrusted`, `trusted` or `contact <nick>`
  (the API's computed `trusted` and `contact` fields; `?` from a daemon that doesn't give them), and the display
  name last, quoted, with backslashes, quotes, blank fillers (U+115F, U+1160, U+3164, U+FFA0, U+2800), a
  combining mark that follows nothing printed or is the third in a row, and a name of only spaces escaped too.
- At most 500 messages and 4 MiB (as JSON) are kept, and at most 50 from one sender not in
  `lxmf_auto_fetch_from`. To make room, the oldest message holding no auto-fetched pins (none pinned or
  queued) is dropped; a message holding pins is not dropped for others' messages: if every kept message holds
  pins, a new message without any is not kept, and one with pins pushes out its own sender's oldest message
  (or, if that sender has none, is not kept). Each of the n senders in `lxmf_auto_fetch_from` keeps at most
  1/n of the 500 messages and of the 4 MiB (at least one message and 16 KiB), messages holding pins included:
  past its share, a new message from it drops its own messages, first those without pins, then those whose
  pinned CIDs another of its messages also holds (dropping them releases nothing), then the oldest. A trusted
  sender's message is dropped only to make room for another trusted sender's, and only once no untrusted sender's
  message without pins is left; a new message from an untrusted sender that would need one dropped is not
  kept. New messages from untrusted senders are also rate limited: each sender has an allowance of 4 at once,
  then one per 15 minutes, and within it messages are taken from a node-wide bucket of 32 at once, then one
  per minute; others are not kept, and counted (`untrusted_dropped` in `status`). So one sender can't keep the
  node-wide bucket empty for everyone else (many can: section 10, Limits). Each message's record is at most 16
  KiB as JSON: links beyond that are dropped (the
  first is always kept), then text is cut.
- **Auto-fetch.** Links from a sender in `lxmf_auto_fetch_from` are fetched completely and pinned with source
  `app:lxmf:<sender address>` (`lxmf:` before rnfs 0.2.0, section 8), one fetch at a time, taking senders in
  turn. At most 64 links wait: when that many
  do, a new link is skipped, unless its sender has at least two fewer waiting than the sender with the most,
  whose newest waiting link is skipped instead. Auto-fetches
  still waiting when the node stops are queued again, oldest first, at the next start; stopping the node or
  removing the message stops a fetch in progress. A waiting link whose sender is no longer in
  `lxmf_auto_fetch_from` is refused, not fetched: at the next start (with LXMF off too) and when its turn comes. The fetch is refused if the target's verified size plus what
  that source already pins would exceed `lxmf_auto_fetch_quota`, or if the blocks of its DAG would (as for
  remote pins, section 8); `max_download_size` applies too. A `name/` link is resolved without persisting
  the name's sequence (only the capped table of section 9 holds it), so names arriving unasked don't grow
  the node's state. Removing a message from the inbox (`inbox rm`) releases its pins unless another inbox
  message from the same sender holds the same CID.
- Fetching a link (auto-fetch or `inbox get`) asks the `from` node first, unless it is the node itself, and
  the DHT if that fails. `inbox get` without `-o` names its output by the `get` rule (section 11): the link's
  last path name only if it is printable, not hidden, doesn't start with `-` and is a valid file name, else
  the root CID.
- A `name/` link is fetched at what the name resolves to. Its `cid` is only the sender's word: nothing ties
  it to the name, so any sender could pose as a well-known name with it. If the name can't be resolved,
  `inbox get` fails (404 through the API) before writing anything or starting a job, unless the user passes
  `--use-sent-cid` (API: `use_sent_cid`): then it fetches the sent CID and that link's result carries a
  warning saying so. A name that resolves is always used. Auto-fetch, whose senders are trusted, falls back
  to the sent CID and records `"sent_cid": true` with the pin. `inbox` shows a name link's sent CID ("sent
  as `<cid>`") and such a pin ("auto-fetch pinned (sent CID; name unresolved)").

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
| CIDs | Section 4. One DAG codec (plus raw) instead of several; base32 only, except base36 (`k`) in gateway subdomain hosts |
| UnixFS files and directories | Section 5. Fixed-size chunks and balanced trees, sorted index instead of HAMT sharding; executable bit kept, mtimes and symlinks dropped |
| Chunkers (`--chunker rabin`, `buzhash`) | Fixed (default) or one fully specified FastCDC (section 5), so every writer cuts the same content alike |
| No built-in encryption | Sealed files: a content convention encrypting a file for chosen RNS identities (section 5) |
| Kademlia DHT, providers | Section 6. 128-bit keys; `K` 10; servers and clients as in IPFS, with the mode carried in the announce; `beta` termination as in IPFS; one find message; batched publishing with withdrawal; no PUT/GET values |
| DHT routing table refresh, IP diversity filter | Replaced by RNS announces and failure counting; no IP addresses exist to diversify on, so Sybil resistance rests on seniority (section 6) |
| DHT key (SHA-256 of the multihash) | SHA-256 of the whole binary CID, truncated to 128 bits, so a raw block and a DAG node with identical bytes stay apart |
| Reprovider strategies | `roots` (default), `pinned`; `all` dropped to save airtime |
| Bitswap, multi-source and resumable download | Section 7; providers ranked by seniority, then local score, hops and next-hop bitrate (section 6) |
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
