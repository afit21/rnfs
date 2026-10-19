# Topology-aware lookups for rnfs: simulation study

Status: complete (all experiments below were run; numbers are means over 900 trials = 300 x 3 seeds unless noted).

## Summary

### What topology-blind Kademlia does on backbone + LoRa islands (0.1.x behaviour)
1. **Announce-starved islands publish to the wrong nodes.** RNS's 2% announce cap lets an island hear only a
   small, stable subset of backbone announces (f ~ 0.16 at S = 1000, 0.05 at S = 3000). A slow node may make one
   path request per lookup (alpha = 1), so its closest-nodes lookup ends at the closest nodes it *knows*: only
   2.7 (S = 1000) / 1.3 (S = 3000) of the true 10 closest get the record, and backbone searchers then find island
   content 91% / 68% of the time (100% at S = 300). Island neighbours still find it (they see the same subset).
2. **Partitions hurt island-local findability and are slow.** Gateway down: a neighbour on the same island finds
   content 55-72% of the time, after ~250-310 s and ~7 timeouts per lookup; publishing during the partition takes
   ~630 s, reaches 3 nodes and suffers 14 timeouts. After 12 h (2 x announce_interval) replies drop off-island
   entries and buckets refill with island nodes, so it partly self-heals (69-72% for records from before).
3. **After the heal, content published during the partition is lost to the backbone** (found 1-14%) and mostly to
   island neighbours too (33-42%), until the slow publisher's next republish: ~2-3 days on average.
4. **The backbone loads the islands.** Island servers sit in backbone routing tables and among the K closest of
   ~1/5 of keys: each backbone lookup costs ~27-33 s of LoRa airtime, each backbone publish ~70 s. With light use
   (10 CIDs + 10 lookups per backbone node per day) a 30-node island needs 135-141% of its channel for other
   people's traffic. This is the biggest topology effect at scale.

### Headline comparison

**S = 1000** (all nodes upgraded)

| strategy | connected: island->island found / LoRa s / latency s | connected: backbone finds island content | foreign LoRa s per backbone lookup | partition early, published before: found / s | published during: found / s | partition late, published before: found | after heal, island searcher | after heal, backbone searcher (before republish) | island publish LoRa s |
|---|---|---|---|---|---|---|---|---|---|
| BASE | 97% / 45 / 47 | 91% | 27.1 | 55% / 310 | 63% / 278 | 72% | 33% | 11% | 358 |
| PNS | 100% / 26 / 28 | 94% | 13.3 | 92% / 92 | 98% / 59 | 93% | 73% | 11% | 346 |
| UB | 100% / 45 / 46 | 99% | 27.1 | 53% / 318 | 62% / 282 | 69% | 19% | 11% | 397 |
| LF2L-m1+PH | 100% / 26 / 28 | 92% | 27.1 | 100% / 28 | 100% / 28 | 100% | 100% | 14% | 443 |
| REC | 100% / 26 / 28 | 99% | 22.0 | 100% / 28 | 100% / 28 | 100% | 100% | 12% | 466 |
| REC-S | 100% / 26 / 28 | 100% | 0.0 | 100% / 28 | 100% / 28 | 100% | 100% | 1% | 455 |
| REC+PRS | 100% / 26 / 28 | 98% | 0.6 | 100% / 28 | 100% / 28 | 100% | 100% | 12% | 466 |

**S = 3000** (all nodes upgraded)

| strategy | connected: island->island found / LoRa s / latency s | connected: backbone finds island content | foreign LoRa s per backbone lookup | partition early, published before: found / s | published during: found / s | partition late, published before: found | after heal, island searcher | after heal, backbone searcher (before republish) | island publish LoRa s |
|---|---|---|---|---|---|---|---|---|---|
| BASE | 85% / 59 / 62 | 68% | 33.2 | 56% / 311 | 72% / 251 | 69% | 33% | 5% | 383 |
| PNS | 99% / 29 / 31 | 78% | 13.9 | 93% / 98 | 97% / 68 | 94% | 76% | 5% | 370 |
| UB | 98% / 65 / 65 | 99% | 33.2 | 51% / 331 | 72% / 253 | 64% | 20% | 5% | 457 |
| LF2L-m1+PH | 100% / 27 / 30 | 68% | 33.2 | 100% / 30 | 100% / 30 | 100% | 100% | 6% | 474 |
| REC | 100% / 27 / 30 | 99% | 26.2 | 100% / 30 | 100% / 30 | 100% | 100% | 6% | 535 |
| REC-S | 100% / 27 / 30 | 100% | 0.0 | 100% / 30 | 100% / 30 | 100% | 100% | 0% | 513 |
| REC+PRS | 100% / 27 / 30 | 98% | 0.2 | 100% / 30 | 100% / 30 | 100% | 100% | 6% | 535 |

(Strategy names: see "Strategies" below. Latencies are lookup time to first provider; LoRa s = airtime summed
over every LoRa hop.)

### Recommended strategy set (all local; wire unchanged)
1. **UB - path-request budget for slow publishers:** a slow node's closest-nodes lookup (publishing) may make up
   to 10 path requests (provider lookups: 3) instead of alpha = 1. Placement 1.3 -> 3.7 of 10 true closest at
   S = 3000; backbone findability of island content 68% -> 99% (91% -> 99% at 1000). Cost +11% (S = 1000) / +19%
   (S = 3000) island publish airtime. Works for 0.1.x searchers (it is the publisher that upgrades).
2. **Two-level local replicas + local-first search (LF2L-m1):** a node on a slow interface also stores each record
   on the 3-5 closest *island-local* servers (plain /provide), and a searcher first asks the 1 closest local server
   (3 if "local" is a hop radius), whose reply also seeds the global lookup. Partition findability 55-72% ->
   100% at ~28 s; island->island fetch airtime 45 -> 26 s, latency 47 -> 28 s; no extra cost when fetching
   backbone content with m = 1. Extra publish airtime +12% (3 local replicas) / +25% (5). Best "local" = same
   island (configured interfaces, or hops <= 3 over a slow interface with m = 3: 98-100%; hops <= 4
   misclassifies backbone nodes behind the gateway as local and drops to 63-81%).
3. **PH - partition mode + heal republish:** after 3 consecutive failures to off-island nodes (or ~10 min without
   off-island announces) use island-local nodes only for lookups and publishing; clear on any off-island announce
   or answer (keep a periodic off-island probe so it can't stick). Cuts partition lookup latency 300 -> 45-130 s,
   publish timeouts 14 -> 3 per publish. On heal, republish (paced) the keys whose last round reached no
   off-island node: recovery in hours instead of ~3 days while <= ~5 keys per node are affected (channel-bound
   beyond that).
4. **Slow-path demotion on fast nodes (PRS):** a fast node ranks servers behind a slow path (hops > 3; better, the
   RTT rnfs already measures on links it opens, > ~1 s) after all others in lookups and in the publish result,
   exactly like unreliable nodes. Foreign LoRa load -97% (27 -> 0.6-0.8 s per backbone lookup); with UB the
   backbone still finds island content 98%; island fetch of backbone content costs +30% airtime and 1-2 points of
   findability. Its benefit is proportional to the fraction of upgraded *backbone* nodes.
   *Alternative* for islands that want zero foreign load: **SCOPE** (island servers announce client mode and serve
   /find + /provide to their own island only) - same effect, but old island nodes then lose local servers.
5. **Optional: PNS50** (choose by hops only when a bucket slot opens, at most K/2 such slots per bucket): island
   fetch airtime -20%, foreign lookup load -20%; without LF2L it alone lifts partition findability from 55-72% to
   88-98% (latency 82-149 s), because island nodes then fill the far buckets of island tables; raises eclipse
   exposure from a nearby attacker (sec. 10). Never displace healthy incumbents for proximity.

Not recommended: **PA** (path-aware skipping of unknown IDs): saves <0.4 path requests per lookup and slightly
worsens placement - the alpha budget already bounds path requests. **PNS (full)**: best latency but eclipsable.
**Additive optional, only if announce coverage is very poor (f <= 0.03):** a delegated lookup request to an
upgraded gateway (PROXY) gives perfect placement (9.9/10) and ~25 s island lookups, but needs a new request path
plus capability discovery (an unknown request path is silently dropped by 0.1.x, so blind probing costs a timeout)
and trust in the gateway (it can hide content; fall back to an own lookup on a miss). Stores still cross LoRa.

### Mixed populations (S = 1000, fraction u of all nodes upgraded)
| u | REC: partition, published before (found) | REC: during | REC: backbone finds island content | REC+PRS: foreign LoRa s / backbone lookup | REC-S partition, before |
|---|---|---|---|---|---|
| 0 | 55% | 63% | 91% | 27.1 | 55% |
| 0.25 | 67% | 74% | 93% | 20.6 | 60% |
| 0.5 | 78% | 83% | 95% | 14.4 | 65% |
| 0.75 | 86% | 89% | 96% | 7.0 | 73% |
| 1 | 100% | 100% | 99% | 0.6 | 100% |

Gains grow roughly linearly with u: two-level needs an upgraded publisher *and* searcher (local replicas are plain
records, so old searchers still benefit when their lookup happens to reach them), UB needs an upgraded publisher,
PRS an upgraded backbone. SCOPE at partial upgrade is worse in partitions than REC because old island nodes see
the scoped servers as clients.

### Changes needed in rnfs (all local behaviour; no new messages, K, TTL or ID rules unchanged)
- lookup.py: per-lookup unknown-ID budget configurable by purpose (closest: 10 on slow nodes; providers: 3);
  optional demotion predicate (like `unreliable`) for slow-path nodes on fast nodes.
- routing / node: a small "local servers" set (servers within `local_hops` (3) over a slow interface, or on
  configured `local_interfaces`), kept from announces; local-first /find before the global lookup with its replies
  as start nodes; partition flag from consecutive off-island failures / off-island announce silence.
- publisher.py: extra stores to the `local_replicas` (3-5) closest local servers each round (counted in the
  persisted per-round store list so withdrawals reach them); remember whether a round reached any off-island node
  and republish (paced) those that didn't when off-island contact returns.
- Optional: bucket fill by hops only for opened slots, capped at K/2 per bucket; SCOPE as a `dht_mode` value
  ("local") that announces client.

### Main caveats
Simplified model: one gateway per island, fixed per-hop airtime constants (link 6.4 s, /find 4 s per LoRa hop),
no collisions/queueing in latency, no churn or packet loss, routing tables as steady-state snapshots, announce
coverage as a stable random subset; all island nodes are servers; open links not modelled (searchers start cold).
The foreign-load numbers scale linearly with the assumed backbone activity. PRS's hop threshold assumes
backbone paths of <= 3 hops; deeper backbones need the RTT signal instead.

# Details

Code: `sim.py` (world, lookup engine mirroring rnfs/dht/lookup.py and node.py start nodes), `experiments.py` (scenarios; `python experiments.py connected|partition|mixed|fsweep|local|prs 300`), `learning.py`, `analysis.py` (load, recovery, PNS exposure), `tables.py`.
Raw outputs: `out/*.json`, `out/*.txt`.

## Model (summary)
- S servers: backbone (TCP hubs) + LoRa islands of 10-50 nodes (random geometric mesh, one gateway each).
- Island = slow nodes (alpha=1, max_parallel_providers=1); backbone = alpha 3. K=10, beta=3, 80-query cap,
  stall window 9, lookup_timeout 600 s, path_timeout 30 s, link establishment timeout 6 s/hop + 5 s grace,
  max_failures 3, unreliable backoff 300 s doubling.
- LoRa airtime per hop (1 kbit/s): link setup 6.4 s, /find 1.6 + 0.24 s per returned ID (4.0 s at 10 IDs),
  path request 0.6 s per rebroadcast (flooded over the island), path response 1.4 s per hop, identify 1.5 s,
  /provide 2.0 s, link close 0.5 s. Backbone: 30 ms per hop.

### World parameters (3 seeds per size)
| S | islands | island servers | announce coverage f on islands | hubs | mean LoRa hops to gateway | max island diameter |
|---|---|---|---|---|---|---|
| 300 | 2-4 | 64-76 (~23%) | 0.56 | 3 | 1.8-2.7 | 6-7 |
| 1000 | 6-9 | 200-222 (~21%) | 0.16 | 7-8 | 1.9-2.1 | 6-8 |
| 3000 | 18-21 | 621-630 (~21%) | 0.053 | 23 | 2.0-2.3 | 8-9 |

- Announce coverage: RNS caps announces at 2% of an interface's bitrate (~318 announces per 6 h at 1 kbit/s,
  queue served fewest-hops first, so the subset that gets through is stable rather than rotating); half of that
  share is assumed to be rnfs. Island nodes know (identity + path) every node of their island and the same
  random fraction f of off-island nodes (minus 10% per-node loss); backbone nodes and gateways know everyone.
  Identities/paths learned by path requests are kept.
- Every server is a DHT server (as today); records stored at time of publish; searchers start cold
  (no open links). 300 trials x 3 seeds per cell. Island searcher = another non-gateway node on the publisher's island.

### Strategies (all local unless noted)
| name | meaning |
|---|---|
| BASE | rnfs 0.1.x behaviour |
| PNS / PNS50 | proximity neighbour selection: in every bucket all (PNS) or half (PNS50) of the slots go to the candidates with fewest hops (then fast next hop); the rest first-heard. PNS50 approximates "PNS only when a slot opens" (incumbents kept). |
| PA1 / PA2 | path-aware: skip an unknown-path ID if a known one not yet queried is within 1 / 2 bits of distance |
| UB | slow nodes may spend up to 10 path requests per closest-nodes lookup (publish) and 3 per provider lookup (today: alpha = 1) |
| PRS | fast nodes rank nodes more than 3 hops away after all others (like unreliable nodes) |
| LF2L / LF2L-m1 | two-level: publisher also stores to the 5 closest island-local servers; searcher first asks the 3 (LF2L) or 1 (m1) closest local servers, their replies seeding the global lookup |
| PH / PHp | partition handling: after 3 consecutive failures to off-island nodes (PH) or from announce silence (PHp, "passive", already known) the node uses island-local nodes only for lookups/publish; any off-island answer clears it |
| SCOPE | local-scope servers: upgraded island nodes announce *client* mode (so 0.1.x and backbone nodes never add them, nothing changes on the wire) and answer /find and /provide for their own island only; upgraded island nodes find them via the local table (LF2L-m1 included) |
| PROXY | **additive request**: island node asks its (upgraded) gateway server to run the lookup and return the result |
| REC | PNS50 + UB + LF2L-m1 + PH |
| REC-S | REC + SCOPE |

## 1. Connected network

Columns: pub_* = island node publishes island content (acc = how many of the true 10 closest got the record;
air = LoRa airtime in s summed over all hops; lat = s). fi_* = another node on the same island fetches it
(found = provider found by the lookup). fb_* = random backbone node fetches it. fbb_air = LoRa airtime caused
by a backbone node fetching *backbone* content; bpub_air = LoRa airtime caused by a backbone node publishing.
fib_* = island node fetches backbone content. Latency = time to first provider (lookup only, not the transfer).

| S | strat | pub_acc | pub_air | pub_lat | fi_found | fi_air | fi_lat | fi_preqs | fb_found | fb_air | fb_lat | fbb_air | bpub_air | fib_found | fib_air | fib_lat |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 300 | BASE | 6.6 | 363 | 347 | 100% | 33.3 | 36.2 | 0.1 | 100% | 30.2 | 1.7 | 30.5 | 80.6 | 100% | 34.8 | 37.5 |
| 300 | PNS | 6.6 | 359 | 342 | 100% | 26.0 | 28.4 | 0.0 | 100% | 15.8 | 1.0 | 17.5 | 69.5 | 100% | 29.4 | 32.2 |
| 300 | PNS50 | 6.6 | 359 | 343 | 100% | 29.1 | 31.5 | 0.1 | 100% | 25.7 | 1.3 | 26.3 | 77.2 | 100% | 31.7 | 34.4 |
| 300 | PA1 | 6.2 | 356 | 343 | 100% | 33.1 | 36.5 | 0.0 | 99% | 30.2 | 2.1 | 30.5 | 80.6 | 100% | 34.1 | 37.3 |
| 300 | PA2 | 6.0 | 354 | 342 | 100% | 33.0 | 36.5 | 0.0 | 98% | 30.2 | 2.3 | 30.5 | 80.6 | 100% | 33.9 | 37.2 |
| 300 | UB | 7.1 | 374 | 355 | 100% | 32.9 | 35.7 | 0.1 | 100% | 30.2 | 1.5 | 30.5 | 80.6 | 100% | 34.8 | 37.5 |
| 300 | PRS | 6.6 | 363 | 347 | 100% | 33.3 | 36.2 | 0.1 | 99% | 1.6 | 0.7 | 1.6 | 6.4 | 99% | 43.3 | 46.4 |
| 300 | PNS+PRS | 6.6 | 359 | 342 | 100% | 26.0 | 28.4 | 0.0 | 99% | 0.9 | 0.6 | 0.9 | 5.6 | 98% | 43.8 | 47.5 |
| 300 | LF2L | 6.6 | 445 | 421 | 100% | 25.8 | 28.4 | 0.0 | 100% | 30.2 | 1.7 | 30.5 | 80.6 | 100% | 52.4 | 57.6 |
| 300 | LF2L-m1 | 6.6 | 445 | 421 | 100% | 25.8 | 28.4 | 0.0 | 100% | 30.2 | 1.7 | 30.5 | 80.6 | 100% | 35.7 | 39.2 |
| 300 | SCOPE | 6.1 | 451 | 425 | 100% | 25.8 | 28.4 | 0.0 | 100% | 0.0 | 0.6 | 0.0 | 0.0 | 100% | 49.5 | 54.0 |
| 300 | PROXY | 9.8 | 372 | 306 | 100% | 45.6 | 26.1 | 0.0 | 100% | 30.2 | 1.3 | 30.5 | 80.6 | 100% | 45.5 | 26.1 |
| 300 | REC | 7.1 | 451 | 424 | 100% | 25.8 | 28.4 | 0.0 | 100% | 25.8 | 1.0 | 26.3 | 77.2 | 100% | 35.3 | 38.7 |
| 300 | REC-S | 6.6 | 464 | 435 | 100% | 25.8 | 28.4 | 0.0 | 100% | 0.0 | 0.6 | 0.0 | 0.0 | 100% | 49.5 | 53.9 |
| 300 | REC+PROXY | 9.9 | 472 | 397 | 100% | 25.8 | 28.4 | 0.0 | 100% | 25.8 | 0.9 | 26.3 | 77.2 | 100% | 39.8 | 36.9 |
| 1000 | BASE | 2.7 | 358 | 342 | 97% | 44.9 | 47.5 | 0.3 | 91% | 27.7 | 5.2 | 27.1 | 69.8 | 99% | 52.8 | 54.1 |
| 1000 | PNS | 2.8 | 346 | 329 | 100% | 25.6 | 27.9 | 0.0 | 94% | 13.0 | 3.2 | 13.3 | 56.6 | 98% | 44.0 | 46.6 |
| 1000 | PNS50 | 2.7 | 345 | 328 | 99% | 36.6 | 38.9 | 0.2 | 93% | 22.3 | 4.5 | 22.0 | 65.1 | 98% | 48.2 | 49.9 |
| 1000 | PA1 | 2.7 | 357 | 341 | 98% | 43.0 | 45.9 | 0.2 | 94% | 27.7 | 5.0 | 27.1 | 69.8 | 100% | 52.5 | 54.5 |
| 1000 | PA2 | 2.6 | 355 | 340 | 98% | 41.7 | 44.9 | 0.2 | 92% | 27.7 | 5.3 | 27.1 | 69.8 | 99% | 52.8 | 55.5 |
| 1000 | UB | 4.4 | 397 | 368 | 100% | 44.7 | 46.1 | 0.4 | 99% | 27.8 | 2.1 | 27.1 | 69.8 | 100% | 53.0 | 54.1 |
| 1000 | PRS | 2.7 | 358 | 342 | 97% | 44.9 | 47.5 | 0.3 | 84% | 0.8 | 1.2 | 0.8 | 2.6 | 92% | 61.4 | 63.1 |
| 1000 | PNS+PRS | 2.8 | 346 | 329 | 100% | 25.6 | 27.9 | 0.0 | 89% | 0.4 | 0.9 | 0.4 | 2.1 | 95% | 54.0 | 56.7 |
| 1000 | LF2L | 2.7 | 443 | 419 | 100% | 25.5 | 28.1 | 0.0 | 92% | 27.7 | 5.2 | 27.1 | 69.8 | 99% | 83.1 | 89.9 |
| 1000 | LF2L-m1 | 2.7 | 443 | 419 | 100% | 25.5 | 28.1 | 0.0 | 92% | 27.7 | 5.2 | 27.1 | 69.8 | 98% | 50.4 | 53.9 |
| 1000 | SCOPE | 2.5 | 419 | 394 | 100% | 25.5 | 28.1 | 0.0 | 94% | 0.0 | 1.0 | 0.0 | 0.0 | 98% | 53.7 | 56.8 |
| 1000 | PROXY | 9.9 | 407 | 313 | 100% | 40.0 | 25.4 | 0.0 | 100% | 27.8 | 1.7 | 27.1 | 69.8 | 100% | 42.7 | 25.6 |
| 1000 | REC | 4.4 | 466 | 431 | 100% | 25.5 | 28.1 | 0.0 | 99% | 22.4 | 1.5 | 22.0 | 65.1 | 100% | 50.5 | 53.7 |
| 1000 | REC-S | 4.2 | 455 | 417 | 100% | 25.5 | 28.1 | 0.0 | 100% | 0.0 | 0.8 | 0.0 | 0.0 | 100% | 53.9 | 56.8 |
| 1000 | REC+PROXY | 9.9 | 527 | 418 | 100% | 25.5 | 28.1 | 0.0 | 100% | 22.5 | 1.2 | 22.0 | 65.1 | 100% | 56.5 | 45.3 |
| 3000 | BASE | 1.3 | 383 | 364 | 85% | 59.3 | 61.8 | 0.4 | 68% | 33.7 | 9.5 | 33.2 | 74.3 | 84% | 74.7 | 75.4 |
| 3000 | PNS | 1.4 | 370 | 350 | 99% | 28.5 | 31.0 | 0.1 | 78% | 15.0 | 6.0 | 13.9 | 58.0 | 89% | 67.7 | 68.9 |
| 3000 | PNS50 | 1.4 | 373 | 355 | 94% | 40.8 | 43.3 | 0.2 | 75% | 26.3 | 8.2 | 26.2 | 66.8 | 86% | 71.6 | 72.7 |
| 3000 | PA1 | 1.4 | 384 | 365 | 85% | 58.4 | 61.1 | 0.4 | 71% | 33.7 | 9.2 | 33.2 | 74.3 | 89% | 74.2 | 75.2 |
| 3000 | PA2 | 1.4 | 383 | 365 | 85% | 57.7 | 60.6 | 0.4 | 75% | 33.7 | 8.8 | 33.2 | 74.3 | 90% | 74.2 | 75.7 |
| 3000 | UB | 3.7 | 457 | 421 | 98% | 65.1 | 64.5 | 0.8 | 99% | 34.0 | 2.6 | 33.2 | 74.3 | 98% | 76.7 | 75.3 |
| 3000 | PRS | 1.3 | 383 | 364 | 85% | 59.3 | 61.8 | 0.4 | 60% | 0.2 | 1.5 | 0.2 | 0.7 | 74% | 81.1 | 82.1 |
| 3000 | PNS+PRS | 1.4 | 370 | 350 | 99% | 28.5 | 31.0 | 0.1 | 73% | 0.2 | 1.2 | 0.2 | 0.6 | 84% | 73.2 | 74.2 |
| 3000 | LF2L | 1.3 | 474 | 446 | 100% | 27.1 | 29.8 | 0.0 | 68% | 33.7 | 9.4 | 33.2 | 74.3 | 88% | 123 | 131 |
| 3000 | LF2L-m1 | 1.3 | 474 | 446 | 100% | 27.1 | 29.8 | 0.0 | 68% | 33.7 | 9.4 | 33.2 | 74.3 | 88% | 77.5 | 80.1 |
| 3000 | SCOPE | 1.3 | 451 | 422 | 100% | 27.1 | 29.8 | 0.0 | 73% | 0.0 | 1.3 | 0.0 | 0.0 | 93% | 73.2 | 74.5 |
| 3000 | PROXY | 9.9 | 476 | 356 | 100% | 47.7 | 27.5 | 0.0 | 100% | 34.0 | 2.0 | 33.2 | 74.3 | 100% | 48.4 | 27.7 |
| 3000 | REC | 3.6 | 535 | 492 | 100% | 27.1 | 29.8 | 0.0 | 99% | 26.5 | 2.0 | 26.2 | 66.8 | 98% | 80.7 | 82.4 |
| 3000 | REC-S | 3.6 | 513 | 466 | 100% | 27.1 | 29.8 | 0.0 | 100% | 0.0 | 1.0 | 0.0 | 0.0 | 100% | 74.3 | 74.9 |
| 3000 | REC+PROXY | 9.9 | 605 | 473 | 100% | 27.1 | 29.8 | 0.0 | 100% | 26.6 | 1.3 | 26.2 | 66.8 | 100% | 69.9 | 54.5 |


## 2. Partitioned island (gateway down)

PB = record published while connected, island neighbour fetches during the partition; PD = published during the
partition. *early* = partition just started (tables unchanged, nodes not yet marked unreliable, searcher cold);
*late* = more than 2 x announce_interval (12 h) in: off-island entries are silent, so /find replies omit them,
lookups start from fresh entries, and buckets have refilled with island nodes. pub_stored = stores that succeeded;
pub_to / fi_to = timeouts (path or link establishment) suffered.

*Scenario PB_early*

| S | strat | pub_stored | pub_acc | pub_air | pub_lat | pub_to | fi_found | fi_air | fi_lat | fi_to |
|---|---|---|---|---|---|---|---|---|---|---|
| 300 | BASE | 10.0 | 6.6 | 363 | 347 | 0.0 | 65% | 69.1 | 280 | 7.0 |
| 300 | PNS | 10.0 | 6.6 | 359 | 342 | 0.0 | 86% | 41.4 | 120 | 2.7 |
| 300 | UB | 10.0 | 7.1 | 374 | 355 | 0.0 | 64% | 70.2 | 284 | 7.2 |
| 300 | LF2L-m1 | 13.3 | 6.6 | 445 | 421 | 0.0 | 100% | 25.8 | 28.4 | 0.0 |
| 300 | PH | 10.0 | 6.6 | 363 | 347 | 0.0 | 68% | 49.1 | 124 | 2.4 |
| 300 | PHp | 10.0 | 6.6 | 363 | 347 | 0.0 | 68% | 37.0 | 40.7 | 0.0 |
| 300 | LF2L-m1+PH | 13.3 | 6.6 | 445 | 421 | 0.0 | 100% | 25.8 | 28.4 | 0.0 |
| 300 | SCOPE+PH | 14.9 | 6.1 | 451 | 425 | 0.0 | 100% | 25.8 | 28.4 | 0.0 |
| 300 | REC | 13.3 | 7.1 | 451 | 424 | 0.0 | 100% | 25.8 | 28.4 | 0.0 |
| 300 | REC-S | 14.9 | 6.6 | 464 | 435 | 0.0 | 100% | 25.8 | 28.4 | 0.0 |
| 1000 | BASE | 10.0 | 2.7 | 358 | 342 | 0.0 | 55% | 74.9 | 310 | 7.4 |
| 1000 | PNS | 10.0 | 2.8 | 346 | 329 | 0.0 | 92% | 35.6 | 92.4 | 1.8 |
| 1000 | UB | 10.0 | 4.4 | 397 | 368 | 0.0 | 53% | 77.6 | 318 | 7.7 |
| 1000 | LF2L-m1 | 13.6 | 2.7 | 443 | 419 | 0.0 | 100% | 25.5 | 28.1 | 0.0 |
| 1000 | PH | 10.0 | 2.7 | 358 | 342 | 0.0 | 57% | 53.3 | 133 | 2.4 |
| 1000 | PHp | 10.0 | 2.7 | 358 | 342 | 0.0 | 57% | 41.4 | 45.6 | 0.0 |
| 1000 | LF2L-m1+PH | 13.6 | 2.7 | 443 | 419 | 0.0 | 100% | 25.5 | 28.1 | 0.0 |
| 1000 | SCOPE+PH | 14.9 | 2.5 | 419 | 394 | 0.0 | 100% | 25.5 | 28.1 | 0.0 |
| 1000 | REC | 13.2 | 4.4 | 466 | 431 | 0.0 | 100% | 25.5 | 28.1 | 0.0 |
| 1000 | REC-S | 14.9 | 4.2 | 455 | 417 | 0.0 | 100% | 25.5 | 28.1 | 0.0 |
| 3000 | BASE | 10.0 | 1.3 | 383 | 364 | 0.0 | 56% | 83.1 | 311 | 7.0 |
| 3000 | PNS | 10.0 | 1.4 | 370 | 350 | 0.0 | 93% | 37.5 | 98.4 | 1.9 |
| 3000 | UB | 10.0 | 3.7 | 457 | 421 | 0.0 | 51% | 90.0 | 331 | 7.5 |
| 3000 | LF2L-m1 | 13.5 | 1.3 | 474 | 446 | 0.0 | 100% | 27.1 | 29.8 | 0.0 |
| 3000 | PH | 10.0 | 1.3 | 383 | 364 | 0.0 | 59% | 62.7 | 146 | 2.4 |
| 3000 | PHp | 10.0 | 1.3 | 383 | 364 | 0.0 | 59% | 49.7 | 54.6 | 0.0 |
| 3000 | LF2L-m1+PH | 13.5 | 1.3 | 474 | 446 | 0.0 | 100% | 27.1 | 29.8 | 0.0 |
| 3000 | SCOPE+PH | 14.9 | 1.3 | 451 | 422 | 0.0 | 100% | 27.1 | 29.8 | 0.0 |
| 3000 | REC | 13.2 | 3.6 | 535 | 492 | 0.0 | 100% | 27.1 | 29.8 | 0.0 |
| 3000 | REC-S | 14.9 | 3.6 | 513 | 466 | 0.0 | 100% | 27.1 | 29.8 | 0.0 |

*Scenario PD_early*

| S | strat | pub_stored | pub_acc | pub_air | pub_lat | pub_to | fi_found | fi_air | fi_lat | fi_to |
|---|---|---|---|---|---|---|---|---|---|---|
| 300 | BASE | 3.0 | 2.8 | 212 | 634 | 14.1 | 67% | 64.4 | 262 | 6.6 |
| 300 | PNS | 6.2 | 5.5 | 271 | 606 | 11.8 | 97% | 27.2 | 60.1 | 1.2 |
| 300 | UB | 3.0 | 2.8 | 217 | 645 | 14.6 | 67% | 65.1 | 264 | 6.7 |
| 300 | LF2L-m1 | 5.6 | 5.4 | 275 | 691 | 14.1 | 100% | 25.8 | 28.4 | 0.0 |
| 300 | PH | 3.5 | 3.1 | 172 | 263 | 3.0 | 69% | 44.8 | 118 | 2.3 |
| 300 | PHp | 3.5 | 3.1 | 156 | 154 | 0.0 | 69% | 33.1 | 36.4 | 0.0 |
| 300 | LF2L-m1+PH | 6.0 | 5.6 | 232 | 317 | 3.0 | 100% | 25.8 | 28.4 | 0.0 |
| 300 | SCOPE+PH | 5.0 | 5.0 | 143 | 220 | 3.0 | 100% | 25.8 | 28.4 | 0.0 |
| 300 | REC | 7.1 | 6.2 | 250 | 333 | 3.1 | 100% | 25.8 | 28.4 | 0.0 |
| 300 | REC-S | 5.2 | 5.1 | 150 | 226 | 3.0 | 100% | 25.8 | 28.4 | 0.0 |
| 1000 | BASE | 3.0 | 2.8 | 202 | 633 | 14.0 | 63% | 66.0 | 278 | 6.7 |
| 1000 | PNS | 6.4 | 5.6 | 264 | 606 | 11.7 | 98% | 26.9 | 59.1 | 1.0 |
| 1000 | UB | 2.9 | 2.7 | 208 | 651 | 14.7 | 62% | 67.3 | 282 | 6.9 |
| 1000 | LF2L-m1 | 5.6 | 5.4 | 263 | 688 | 14.0 | 100% | 25.5 | 28.1 | 0.0 |
| 1000 | PH | 3.5 | 3.1 | 169 | 265 | 3.1 | 67% | 45.7 | 122 | 2.3 |
| 1000 | PHp | 3.6 | 3.1 | 154 | 153 | 0.0 | 67% | 34.5 | 37.9 | 0.0 |
| 1000 | LF2L-m1+PH | 6.0 | 5.5 | 226 | 317 | 3.1 | 100% | 25.5 | 28.1 | 0.0 |
| 1000 | SCOPE+PH | 5.0 | 5.0 | 138 | 215 | 3.0 | 100% | 25.5 | 28.1 | 0.0 |
| 1000 | REC | 7.1 | 6.2 | 243 | 333 | 3.1 | 100% | 25.5 | 28.1 | 0.0 |
| 1000 | REC-S | 5.2 | 5.1 | 144 | 220 | 3.0 | 100% | 25.5 | 28.1 | 0.0 |
| 3000 | BASE | 3.2 | 3.0 | 224 | 656 | 13.5 | 72% | 65.4 | 251 | 5.7 |
| 3000 | PNS | 6.4 | 5.5 | 277 | 617 | 11.0 | 97% | 29.9 | 68.4 | 1.2 |
| 3000 | UB | 3.2 | 3.0 | 229 | 670 | 14.1 | 72% | 66.6 | 253 | 5.8 |
| 3000 | LF2L-m1 | 5.6 | 5.4 | 286 | 713 | 13.5 | 100% | 27.1 | 29.8 | 0.0 |
| 3000 | PH | 3.8 | 3.3 | 191 | 289 | 3.1 | 76% | 48.0 | 122 | 2.2 |
| 3000 | PHp | 3.9 | 3.4 | 175 | 172 | 0.0 | 76% | 36.7 | 40.3 | 0.0 |
| 3000 | LF2L-m1+PH | 6.1 | 5.6 | 249 | 341 | 3.1 | 100% | 27.1 | 29.8 | 0.0 |
| 3000 | SCOPE+PH | 5.1 | 5.0 | 149 | 230 | 3.0 | 100% | 27.1 | 29.8 | 0.0 |
| 3000 | REC | 7.3 | 6.2 | 263 | 356 | 3.2 | 100% | 27.1 | 29.8 | 0.0 |
| 3000 | REC-S | 5.1 | 5.1 | 154 | 233 | 3.0 | 100% | 27.1 | 29.8 | 0.0 |

*Scenario PB_late*

| S | strat | pub_stored | pub_acc | pub_air | pub_lat | pub_to | fi_found | fi_air | fi_lat | fi_to |
|---|---|---|---|---|---|---|---|---|---|---|
| 300 | BASE | 10.0 | 6.6 | 363 | 347 | 0.0 | 83% | 36.9 | 40.5 | 0.0 |
| 300 | PNS | 10.0 | 6.6 | 359 | 342 | 0.0 | 89% | 31.4 | 34.5 | 0.0 |
| 300 | UB | 10.0 | 7.1 | 374 | 355 | 0.0 | 82% | 37.5 | 41.2 | 0.0 |
| 300 | LF2L-m1 | 13.3 | 6.6 | 445 | 421 | 0.0 | 100% | 25.8 | 28.4 | 0.0 |
| 300 | PH | 10.0 | 6.6 | 363 | 347 | 0.0 | 83% | 36.9 | 40.5 | 0.0 |
| 300 | PHp | 10.0 | 6.6 | 363 | 347 | 0.0 | 83% | 36.9 | 40.5 | 0.0 |
| 300 | LF2L-m1+PH | 13.3 | 6.6 | 445 | 421 | 0.0 | 100% | 25.8 | 28.4 | 0.0 |
| 300 | SCOPE+PH | 14.9 | 6.1 | 451 | 425 | 0.0 | 100% | 20.5 | 23.0 | 0.0 |
| 300 | REC | 13.3 | 7.1 | 451 | 424 | 0.0 | 100% | 25.8 | 28.4 | 0.0 |
| 300 | REC-S | 14.9 | 6.6 | 464 | 435 | 0.0 | 100% | 20.5 | 23.0 | 0.0 |
| 1000 | BASE | 10.0 | 2.7 | 358 | 342 | 0.0 | 72% | 42.8 | 47.1 | 0.0 |
| 1000 | PNS | 10.0 | 2.8 | 346 | 329 | 0.0 | 93% | 28.1 | 30.9 | 0.0 |
| 1000 | UB | 10.0 | 4.4 | 397 | 368 | 0.0 | 69% | 44.8 | 49.3 | 0.0 |
| 1000 | LF2L-m1 | 13.6 | 2.7 | 443 | 419 | 0.0 | 100% | 25.5 | 28.1 | 0.0 |
| 1000 | PH | 10.0 | 2.7 | 358 | 342 | 0.0 | 72% | 42.8 | 47.1 | 0.0 |
| 1000 | PHp | 10.0 | 2.7 | 358 | 342 | 0.0 | 72% | 42.8 | 47.1 | 0.0 |
| 1000 | LF2L-m1+PH | 13.6 | 2.7 | 443 | 419 | 0.0 | 100% | 25.5 | 28.1 | 0.0 |
| 1000 | SCOPE+PH | 14.9 | 2.5 | 419 | 394 | 0.0 | 100% | 20.2 | 22.7 | 0.0 |
| 1000 | REC | 13.2 | 4.4 | 466 | 431 | 0.0 | 100% | 25.5 | 28.1 | 0.0 |
| 1000 | REC-S | 14.9 | 4.2 | 455 | 417 | 0.0 | 100% | 20.2 | 22.7 | 0.0 |
| 3000 | BASE | 10.0 | 1.3 | 383 | 364 | 0.0 | 69% | 50.9 | 56.2 | 0.0 |
| 3000 | PNS | 10.0 | 1.4 | 370 | 350 | 0.0 | 94% | 30.1 | 33.3 | 0.0 |
| 3000 | UB | 10.0 | 3.7 | 457 | 421 | 0.0 | 64% | 54.6 | 60.4 | 0.0 |
| 3000 | LF2L-m1 | 13.5 | 1.3 | 474 | 446 | 0.0 | 100% | 27.1 | 29.8 | 0.0 |
| 3000 | PH | 10.0 | 1.3 | 383 | 364 | 0.0 | 69% | 50.9 | 56.2 | 0.0 |
| 3000 | PHp | 10.0 | 1.3 | 383 | 364 | 0.0 | 69% | 50.8 | 55.9 | 0.0 |
| 3000 | LF2L-m1+PH | 13.5 | 1.3 | 474 | 446 | 0.0 | 100% | 27.1 | 29.8 | 0.0 |
| 3000 | SCOPE+PH | 14.9 | 1.3 | 451 | 422 | 0.0 | 100% | 21.4 | 24.1 | 0.0 |
| 3000 | REC | 13.2 | 3.6 | 535 | 492 | 0.0 | 100% | 27.1 | 29.8 | 0.0 |
| 3000 | REC-S | 14.9 | 3.6 | 513 | 466 | 0.0 | 100% | 21.5 | 24.2 | 0.0 |

*Scenario PD_late*

| S | strat | pub_stored | pub_acc | pub_air | pub_lat | pub_to | fi_found | fi_air | fi_lat | fi_to |
|---|---|---|---|---|---|---|---|---|---|---|
| 300 | BASE | 10.0 | 9.6 | 325 | 310 | 0.0 | 100% | 25.7 | 28.2 | 0.0 |
| 300 | PNS | 10.0 | 9.6 | 325 | 310 | 0.0 | 100% | 22.7 | 25.0 | 0.0 |
| 300 | UB | 10.0 | 9.6 | 325 | 310 | 0.0 | 100% | 25.7 | 28.2 | 0.0 |
| 300 | LF2L-m1 | 10.0 | 9.6 | 325 | 310 | 0.0 | 100% | 25.8 | 28.4 | 0.0 |
| 300 | PH | 10.0 | 9.6 | 325 | 310 | 0.0 | 100% | 25.7 | 28.2 | 0.0 |
| 300 | PHp | 10.0 | 9.6 | 325 | 310 | 0.0 | 100% | 25.7 | 28.2 | 0.0 |
| 300 | LF2L-m1+PH | 10.0 | 9.6 | 325 | 310 | 0.0 | 100% | 25.8 | 28.4 | 0.0 |
| 300 | SCOPE+PH | 9.0 | 7.0 | 292 | 323 | 1.6 | 100% | 20.5 | 23.0 | 0.0 |
| 300 | REC | 10.0 | 9.6 | 325 | 310 | 0.0 | 100% | 25.8 | 28.4 | 0.0 |
| 300 | REC-S | 9.0 | 7.0 | 265 | 297 | 1.6 | 100% | 20.5 | 23.0 | 0.0 |
| 1000 | BASE | 10.0 | 9.6 | 316 | 302 | 0.0 | 100% | 25.2 | 27.7 | 0.0 |
| 1000 | PNS | 10.0 | 9.6 | 316 | 301 | 0.0 | 100% | 22.0 | 24.2 | 0.0 |
| 1000 | UB | 10.0 | 9.6 | 316 | 302 | 0.0 | 100% | 25.2 | 27.7 | 0.0 |
| 1000 | LF2L-m1 | 10.0 | 9.6 | 316 | 302 | 0.0 | 100% | 25.5 | 28.1 | 0.0 |
| 1000 | PH | 10.0 | 9.6 | 316 | 302 | 0.0 | 100% | 25.2 | 27.7 | 0.0 |
| 1000 | PHp | 10.0 | 9.6 | 316 | 302 | 0.0 | 100% | 25.2 | 27.7 | 0.0 |
| 1000 | LF2L-m1+PH | 10.0 | 9.6 | 316 | 302 | 0.0 | 100% | 25.5 | 28.1 | 0.0 |
| 1000 | SCOPE+PH | 9.1 | 7.0 | 286 | 317 | 1.5 | 100% | 20.2 | 22.7 | 0.0 |
| 1000 | REC | 10.0 | 9.6 | 316 | 301 | 0.0 | 100% | 25.5 | 28.1 | 0.0 |
| 1000 | REC-S | 9.1 | 7.0 | 258 | 290 | 1.5 | 100% | 20.2 | 22.7 | 0.0 |
| 3000 | BASE | 10.0 | 9.7 | 339 | 323 | 0.0 | 100% | 27.0 | 29.9 | 0.0 |
| 3000 | PNS | 10.0 | 9.6 | 339 | 323 | 0.0 | 100% | 23.3 | 25.8 | 0.0 |
| 3000 | UB | 10.0 | 9.7 | 339 | 323 | 0.0 | 100% | 27.0 | 29.9 | 0.0 |
| 3000 | LF2L-m1 | 10.0 | 9.7 | 339 | 323 | 0.0 | 100% | 27.1 | 29.8 | 0.0 |
| 3000 | PH | 10.0 | 9.7 | 339 | 323 | 0.0 | 100% | 27.0 | 29.9 | 0.0 |
| 3000 | PHp | 10.0 | 9.7 | 339 | 323 | 0.0 | 100% | 26.9 | 29.6 | 0.0 |
| 3000 | LF2L-m1+PH | 10.0 | 9.7 | 339 | 323 | 0.0 | 100% | 27.1 | 29.8 | 0.0 |
| 3000 | SCOPE+PH | 9.3 | 7.0 | 310 | 337 | 1.4 | 100% | 21.4 | 24.1 | 0.0 |
| 3000 | REC | 10.0 | 9.7 | 339 | 323 | 0.0 | 100% | 27.1 | 29.8 | 0.0 |
| 3000 | REC-S | 9.3 | 7.0 | 278 | 307 | 1.4 | 100% | 21.5 | 24.2 | 0.0 |


**PNS50 in the partition scenarios** (all nodes upgraded; compare BASE above)

| S | PB_early found / lat s / LoRa s | PD_early | PB_late | H_early (island searcher) |
|---|---|---|---|---|
| 1000 | 89% / 143 / 44.8 | 98% / 82 / 30.0 | 92% / 35 / 31.5 | 55% / 69 / 66.7 |
| 3000 | 88% / 149 / 48.3 | 97% / 90 / 32.7 | 92% / 38 / 34.4 | 60% / 67 / 65.3 |

## 3. Partition heals

Records published during the partition (early: just after it started; late: >12 h in), then the gateway comes back
and searchers look 1 h later, before any republish. fi = island neighbour, fb = backbone node.

*Scenario H_early*

| S | strat | pub_stored | fi_found | fi_air | fi_lat | fb_found |
|---|---|---|---|---|---|---|
| 300 | BASE | 3.0 | 36% | 85.2 | 89.2 | 28% |
| 300 | PNS | 6.2 | 70% | 43.0 | 45.6 | 31% |
| 300 | UB | 3.0 | 30% | 93.0 | 94.3 | 28% |
| 300 | LF2L-m1 | 5.6 | 100% | 25.8 | 28.4 | 34% |
| 300 | PH | 3.5 | 37% | 85.0 | 88.9 | 29% |
| 300 | LF2L-m1+PH | 6.0 | 100% | 25.8 | 28.4 | 34% |
| 300 | SCOPE+PH | 5.0 | 100% | 25.8 | 28.4 | 3% |
| 300 | REC | 7.1 | 100% | 25.8 | 28.4 | 32% |
| 300 | REC-S | 5.2 | 100% | 25.8 | 28.4 | 3% |
| 300 | PHp | 3.5 | 37% | 85.0 | 88.9 | 29% |
| 1000 | BASE | 3.0 | 33% | 91.7 | 94.5 | 11% |
| 1000 | PNS | 6.4 | 73% | 41.3 | 43.3 | 11% |
| 1000 | UB | 2.9 | 19% | 120 | 114 | 11% |
| 1000 | LF2L-m1 | 5.6 | 100% | 25.5 | 28.1 | 14% |
| 1000 | PH | 3.5 | 34% | 91.3 | 94.2 | 12% |
| 1000 | LF2L-m1+PH | 6.0 | 100% | 25.5 | 28.1 | 14% |
| 1000 | SCOPE+PH | 5.0 | 100% | 25.5 | 28.1 | 1% |
| 1000 | REC | 7.1 | 100% | 25.5 | 28.1 | 12% |
| 1000 | REC-S | 5.2 | 100% | 25.5 | 28.1 | 1% |
| 1000 | PHp | 3.6 | 34% | 91.3 | 94.2 | 12% |
| 3000 | BASE | 3.2 | 33% | 92.8 | 94.9 | 5% |
| 3000 | PNS | 6.4 | 76% | 42.6 | 44.3 | 5% |
| 3000 | UB | 3.2 | 20% | 136 | 127 | 5% |
| 3000 | LF2L-m1 | 5.6 | 100% | 27.1 | 29.8 | 6% |
| 3000 | PH | 3.8 | 33% | 92.4 | 94.5 | 5% |
| 3000 | LF2L-m1+PH | 6.1 | 100% | 27.1 | 29.8 | 6% |
| 3000 | SCOPE+PH | 5.1 | 100% | 27.1 | 29.8 | 0% |
| 3000 | REC | 7.3 | 100% | 27.1 | 29.8 | 6% |
| 3000 | REC-S | 5.1 | 100% | 27.1 | 29.8 | 0% |
| 3000 | PHp | 3.9 | 33% | 92.4 | 94.5 | 5% |

*Scenario H_late*

| S | strat | pub_stored | fi_found | fi_air | fi_lat | fb_found |
|---|---|---|---|---|---|---|
| 300 | BASE | 10.0 | 42% | 82.7 | 86.7 | 34% |
| 300 | PNS | 10.0 | 71% | 41.3 | 43.8 | 31% |
| 300 | UB | 10.0 | 36% | 90.1 | 91.5 | 34% |
| 300 | LF2L-m1 | 10.0 | 100% | 25.8 | 28.4 | 34% |
| 300 | PH | 10.0 | 42% | 82.7 | 86.7 | 34% |
| 300 | LF2L-m1+PH | 10.0 | 100% | 25.8 | 28.4 | 34% |
| 300 | SCOPE+PH | 9.0 | 100% | 25.8 | 28.4 | 3% |
| 300 | REC | 10.0 | 100% | 25.8 | 28.4 | 33% |
| 300 | REC-S | 9.0 | 100% | 25.8 | 28.4 | 3% |
| 300 | PHp | 10.0 | 42% | 82.7 | 86.7 | 34% |
| 1000 | BASE | 10.0 | 40% | 88.4 | 91.2 | 14% |
| 1000 | PNS | 10.0 | 75% | 39.1 | 41.1 | 11% |
| 1000 | UB | 10.0 | 24% | 116 | 110 | 14% |
| 1000 | LF2L-m1 | 10.0 | 100% | 25.5 | 28.1 | 14% |
| 1000 | PH | 10.0 | 40% | 88.4 | 91.2 | 14% |
| 1000 | LF2L-m1+PH | 10.0 | 100% | 25.5 | 28.1 | 14% |
| 1000 | SCOPE+PH | 9.1 | 100% | 25.5 | 28.1 | 1% |
| 1000 | REC | 10.0 | 100% | 25.5 | 28.1 | 13% |
| 1000 | REC-S | 9.1 | 100% | 25.5 | 28.1 | 1% |
| 1000 | PHp | 10.0 | 40% | 88.4 | 91.2 | 14% |
| 3000 | BASE | 10.0 | 40% | 88.4 | 90.3 | 6% |
| 3000 | PNS | 10.0 | 78% | 39.6 | 41.3 | 5% |
| 3000 | UB | 10.0 | 24% | 131 | 122 | 6% |
| 3000 | LF2L-m1 | 10.0 | 100% | 27.1 | 29.8 | 6% |
| 3000 | PH | 10.0 | 40% | 88.4 | 90.3 | 6% |
| 3000 | LF2L-m1+PH | 10.0 | 100% | 27.1 | 29.8 | 6% |
| 3000 | SCOPE+PH | 9.3 | 100% | 27.1 | 29.8 | 1% |
| 3000 | REC | 10.0 | 100% | 27.1 | 29.8 | 6% |
| 3000 | REC-S | 9.3 | 100% | 27.1 | 29.8 | 0% |
| 3000 | PHp | 10.0 | 40% | 88.4 | 90.3 | 6% |


## 4. Announce coverage on islands (S = 1000, connected)

f = fraction of off-island announces that reach island nodes (stable subset). With f small an island node knows
few backbone identities and may make only alpha = 1 path request per lookup, so its closest-nodes lookup stops at
the closest nodes it *knows*: records land on nodes that are not the true closest (pub_acc) and backbone
searchers miss them (fb_found).

| f | strat | pub_acc | pub_air | pub_preqs | fi_found | fi_air | fb_found | fb_lat | fib_found | fib_air |
|---|---|---|---|---|---|---|---|---|---|---|
| 1.0 | BASE | 9.4 | 350 | 0.3 | 100% | 43.8 | 100% | 1.7 | 100% | 43.7 |
| 1.0 | PNS | 9.3 | 343 | 0.3 | 100% | 34.9 | 100% | 0.8 | 100% | 35.6 |
| 1.0 | UB | 9.4 | 351 | 0.3 | 100% | 43.7 | 100% | 1.7 | 100% | 43.7 |
| 1.0 | PA1 | 9.2 | 348 | 0.1 | 100% | 44.3 | 100% | 1.8 | 100% | 43.3 |
| 1.0 | PROXY | 9.9 | 307 | 0.9 | 100% | 43.5 | 100% | 1.7 | 100% | 41.9 |
| 1.0 | REC | 9.4 | 458 | 0.3 | 100% | 25.5 | 100% | 1.1 | 100% | 43.5 |
| 1.0 | REC-S | 7.9 | 422 | 0.3 | 100% | 25.5 | 100% | 0.8 | 100% | 46.7 |
| 0.3 | BASE | 3.9 | 362 | 1.0 | 99% | 47.6 | 96% | 3.7 | 100% | 50.1 |
| 0.3 | PNS | 3.9 | 346 | 1.0 | 100% | 27.9 | 98% | 1.9 | 100% | 39.5 |
| 0.3 | UB | 5.2 | 385 | 2.2 | 100% | 46.2 | 100% | 1.9 | 100% | 50.3 |
| 0.3 | PA1 | 3.7 | 358 | 0.8 | 99% | 45.5 | 97% | 3.8 | 100% | 48.9 |
| 0.3 | PROXY | 9.9 | 392 | 7.0 | 100% | 42.0 | 100% | 1.4 | 100% | 43.3 |
| 0.3 | REC | 5.2 | 472 | 2.2 | 100% | 25.5 | 100% | 1.2 | 100% | 46.2 |
| 0.3 | REC-S | 4.8 | 449 | 2.3 | 100% | 25.5 | 100% | 0.8 | 100% | 49.1 |
| 0.1 | BASE | 2.2 | 351 | 1.0 | 97% | 40.1 | 88% | 5.9 | 97% | 54.7 |
| 0.1 | PNS | 2.2 | 341 | 1.0 | 100% | 23.8 | 90% | 4.0 | 97% | 48.5 |
| 0.1 | UB | 4.0 | 395 | 2.8 | 100% | 42.0 | 99% | 2.9 | 99% | 55.2 |
| 0.1 | PA1 | 2.2 | 350 | 0.9 | 97% | 39.1 | 90% | 5.7 | 99% | 54.4 |
| 0.1 | PROXY | 9.8 | 415 | 8.7 | 100% | 41.6 | 100% | 1.9 | 100% | 43.0 |
| 0.1 | REC | 4.0 | 461 | 2.8 | 100% | 25.5 | 98% | 2.1 | 99% | 55.3 |
| 0.1 | REC-S | 4.0 | 456 | 2.9 | 100% | 25.5 | 100% | 0.9 | 100% | 58.9 |
| 0.03 | BASE | 1.4 | 341 | 0.9 | 98% | 30.3 | 69% | 8.1 | 87% | 61.5 |
| 0.03 | PNS | 1.4 | 336 | 0.9 | 100% | 23.0 | 73% | 6.2 | 87% | 59.7 |
| 0.03 | UB | 3.2 | 397 | 2.8 | 99% | 33.3 | 89% | 4.4 | 94% | 63.7 |
| 0.03 | PA1 | 1.4 | 340 | 0.8 | 98% | 30.7 | 71% | 8.1 | 89% | 61.1 |
| 0.03 | PROXY | 9.8 | 425 | 9.4 | 100% | 41.2 | 100% | 1.9 | 100% | 43.0 |
| 0.03 | REC | 3.1 | 427 | 2.7 | 100% | 25.5 | 88% | 3.7 | 94% | 68.3 |
| 0.03 | REC-S | 3.6 | 455 | 3.2 | 100% | 25.3 | 99% | 0.9 | 98% | 72.7 |

## 5. Local-first / two-level parameters (S = 1000)

LF2L-m1 = exact island membership, 5 local replicas, ask 1 local server. LF-hN = "local" means within N hops over
a slow interface (no configuration; misclassifies backbone nodes near the gateway as local at N >= 4 and drops far
island nodes at small N). m3 = ask 3 local servers. k1/k3/k10 = local replicas. fi_local = provider found in the
local phase.

*C*

| strat | pub_air | fi_found | fi_local | fi_air | fi_lat | fib_found | fib_air |
|---|---|---|---|---|---|---|---|
| LF2L-m1 | 443 | 100% | 100% | 25.5 | 28.1 | 98% | 50.4 |
| LF-h4 | 413 | 100% | 91% | 24.5 | 26.7 | 99% | 42.5 |
| LF-h3 | 429 | 99% | 91% | 23.7 | 25.8 | 99% | 47.1 |
| LF-h2 | 424 | 100% | 79% | 24.9 | 27.1 | 98% | 47.5 |
| LF-h3-m3 | 429 | 100% | 98% | 22.9 | 25.2 | 99% | 74.8 |
| LF-k1 | 368 | 100% | 98% | 26.3 | 28.9 | 98% | 50.4 |
| LF-k3 | 400 | 100% | 100% | 25.5 | 28.1 | 98% | 50.4 |
| LF-k10 | 561 | 100% | 100% | 25.5 | 28.1 | 98% | 50.4 |

*PB_early*

| strat | pub_air | fi_found | fi_local | fi_air | fi_lat | fib_found | fib_air |
|---|---|---|---|---|---|---|---|
| LF2L-m1 | 443 | 100% | 100% | 25.5 | 28.1 | - | - |
| LF-h4 | 413 | 81% | 55% | 40.7 | 152 | - | - |
| LF-h3 | 429 | 95% | 80% | 29.1 | 72.5 | - | - |
| LF-h2 | 424 | 92% | 79% | 33.0 | 90.5 | - | - |
| LF-h3-m3 | 429 | 99% | 97% | 25.2 | 40.4 | - | - |
| LF-k1 | 368 | 98% | 98% | 26.7 | 36.8 | - | - |
| LF-k3 | 400 | 100% | 100% | 25.5 | 28.1 | - | - |
| LF-k10 | 561 | 100% | 100% | 25.5 | 28.1 | - | - |

*H_early*

| strat | pub_air | fi_found | fi_local | fi_air | fi_lat | fib_found | fib_air |
|---|---|---|---|---|---|---|---|
| LF2L-m1 | 263 | 100% | 100% | 25.5 | 28.1 | - | - |
| LF-h4 | 239 | 63% | 58% | 45.0 | 46.5 | - | - |
| LF-h3 | 254 | 83% | 82% | 30.9 | 32.8 | - | - |
| LF-h2 | 257 | 89% | 87% | 29.1 | 31.0 | - | - |
| LF-h3-m3 | 254 | 98% | 97% | 26.2 | 28.8 | - | - |
| LF-k1 | 208 | 99% | 99% | 26.4 | 29.0 | - | - |
| LF-k3 | 230 | 100% | 100% | 25.5 | 28.1 | - | - |
| LF-k10 | 374 | 100% | 100% | 25.5 | 28.1 | - | - |


## 6. Mixed populations (S = 1000, fraction u of all nodes upgraded, random)

*C*

| strat | u | fi_found | fi_air | fi_lat | fb_found | fbb_air | bpub_air | pub_air |
|---|---|---|---|---|---|---|---|---|
| REC | 0.0 | 97% | 44.9 | 47.5 | 91% | 27.1 | 69.8 | 358 |
| REC | 0.1 | 98% | 43.1 | 45.7 | 92% | 26.8 | 69.2 | 370 |
| REC | 0.25 | 98% | 42.0 | 44.4 | 93% | 26.0 | 68.8 | 385 |
| REC | 0.5 | 99% | 37.9 | 40.4 | 95% | 24.7 | 67.9 | 413 |
| REC | 0.75 | 99% | 33.4 | 35.9 | 96% | 23.6 | 66.5 | 436 |
| REC | 1.0 | 100% | 25.5 | 28.1 | 99% | 22.0 | 65.1 | 466 |
| REC-S | 0.0 | 97% | 44.9 | 47.5 | 91% | 27.1 | 69.8 | 358 |
| REC-S | 0.1 | 98% | 42.4 | 44.9 | 93% | 24.7 | 62.4 | 367 |
| REC-S | 0.25 | 98% | 41.1 | 43.5 | 94% | 21.2 | 55.4 | 374 |
| REC-S | 0.5 | 98% | 37.5 | 39.7 | 96% | 12.8 | 36.3 | 396 |
| REC-S | 0.75 | 99% | 35.0 | 37.3 | 98% | 7.5 | 19.8 | 420 |
| REC-S | 1.0 | 100% | 25.5 | 28.1 | 100% | 0.0 | 0.0 | 455 |
| LF2L-m1+PH | 0.0 | 97% | 44.9 | 47.5 | 91% | 27.1 | 69.8 | 358 |
| LF2L-m1+PH | 0.1 | 98% | 43.4 | 46.1 | 91% | 27.1 | 69.8 | 367 |
| LF2L-m1+PH | 0.25 | 98% | 41.9 | 44.5 | 91% | 27.1 | 69.8 | 379 |
| LF2L-m1+PH | 0.5 | 98% | 38.3 | 40.9 | 91% | 27.1 | 69.8 | 402 |
| LF2L-m1+PH | 0.75 | 99% | 33.9 | 36.6 | 91% | 27.1 | 69.8 | 420 |
| LF2L-m1+PH | 1.0 | 100% | 25.5 | 28.1 | 92% | 27.1 | 69.8 | 443 |
| UB | 0.0 | 97% | 44.9 | 47.5 | 91% | 27.1 | 69.8 | 358 |
| UB | 0.1 | 97% | 45.1 | 47.4 | 93% | 27.1 | 69.8 | 363 |
| UB | 0.25 | 97% | 45.3 | 47.4 | 94% | 27.1 | 69.8 | 369 |
| UB | 0.5 | 98% | 45.5 | 47.3 | 95% | 27.1 | 69.8 | 377 |
| UB | 0.75 | 99% | 45.5 | 47.1 | 96% | 27.1 | 69.8 | 385 |
| UB | 1.0 | 100% | 44.7 | 46.1 | 99% | 27.1 | 69.8 | 397 |
| PNS | 0.0 | 97% | 44.9 | 47.5 | 91% | 27.1 | 69.8 | 358 |
| PNS | 0.1 | 97% | 42.6 | 45.2 | 92% | 26.1 | 68.6 | 356 |
| PNS | 0.25 | 98% | 40.5 | 43.0 | 92% | 24.0 | 66.7 | 355 |
| PNS | 0.5 | 99% | 36.9 | 39.3 | 93% | 20.8 | 63.8 | 351 |
| PNS | 0.75 | 99% | 32.4 | 34.8 | 93% | 17.4 | 60.1 | 347 |
| PNS | 1.0 | 100% | 25.6 | 27.9 | 94% | 13.3 | 56.6 | 346 |

*PB_early*

| strat | u | fi_found | fi_air | fi_lat | fb_found | fbb_air | bpub_air | pub_air |
|---|---|---|---|---|---|---|---|---|
| REC | 0.0 | 55% | 74.9 | 310 | - | - | - | 358 |
| REC | 0.1 | 59% | 69.8 | 280 | - | - | - | 370 |
| REC | 0.25 | 67% | 60.6 | 223 | - | - | - | 385 |
| REC | 0.5 | 78% | 47.7 | 145 | - | - | - | 413 |
| REC | 0.75 | 86% | 38.0 | 93.4 | - | - | - | 436 |
| REC | 1.0 | 100% | 25.5 | 28.1 | - | - | - | 466 |
| REC-S | 0.0 | 55% | 74.9 | 310 | - | - | - | 358 |
| REC-S | 0.1 | 57% | 69.1 | 283 | - | - | - | 367 |
| REC-S | 0.25 | 60% | 62.4 | 242 | - | - | - | 374 |
| REC-S | 0.5 | 65% | 51.6 | 178 | - | - | - | 396 |
| REC-S | 0.75 | 73% | 42.3 | 127 | - | - | - | 420 |
| REC-S | 1.0 | 100% | 25.5 | 28.1 | - | - | - | 455 |
| LF2L-m1+PH | 0.0 | 55% | 74.9 | 310 | - | - | - | 358 |
| LF2L-m1+PH | 0.1 | 58% | 69.8 | 282 | - | - | - | 367 |
| LF2L-m1+PH | 0.25 | 66% | 61.1 | 227 | - | - | - | 379 |
| LF2L-m1+PH | 0.5 | 77% | 47.6 | 148 | - | - | - | 402 |
| LF2L-m1+PH | 0.75 | 85% | 37.6 | 94.8 | - | - | - | 420 |
| LF2L-m1+PH | 1.0 | 100% | 25.5 | 28.1 | - | - | - | 443 |
| UB | 0.0 | 55% | 74.9 | 310 | - | - | - | 358 |
| UB | 0.1 | 54% | 75.3 | 311 | - | - | - | 363 |
| UB | 0.25 | 54% | 75.1 | 311 | - | - | - | 369 |
| UB | 0.5 | 53% | 76.5 | 315 | - | - | - | 377 |
| UB | 0.75 | 53% | 76.8 | 316 | - | - | - | 385 |
| UB | 1.0 | 53% | 77.6 | 318 | - | - | - | 397 |
| PNS | 0.0 | 55% | 74.9 | 310 | - | - | - | 358 |
| PNS | 0.1 | 57% | 73.5 | 298 | - | - | - | 356 |
| PNS | 0.25 | 65% | 66.5 | 260 | - | - | - | 355 |
| PNS | 0.5 | 73% | 57.9 | 207 | - | - | - | 351 |
| PNS | 0.75 | 80% | 49.6 | 164 | - | - | - | 347 |
| PNS | 1.0 | 92% | 35.6 | 92.4 | - | - | - | 346 |

*PD_early*

| strat | u | fi_found | fi_air | fi_lat | fb_found | fbb_air | bpub_air | pub_air |
|---|---|---|---|---|---|---|---|---|
| REC | 0.0 | 63% | 66.0 | 278 | - | - | - | 202 |
| REC | 0.1 | 68% | 60.1 | 243 | - | - | - | 207 |
| REC | 0.25 | 74% | 53.6 | 199 | - | - | - | 214 |
| REC | 0.5 | 83% | 42.6 | 129 | - | - | - | 224 |
| REC | 0.75 | 89% | 34.5 | 82.5 | - | - | - | 232 |
| REC | 1.0 | 100% | 25.5 | 28.1 | - | - | - | 243 |
| REC-S | 0.0 | 63% | 66.0 | 278 | - | - | - | 202 |
| REC-S | 0.1 | 65% | 61.2 | 253 | - | - | - | 198 |
| REC-S | 0.25 | 66% | 57.6 | 225 | - | - | - | 195 |
| REC-S | 0.5 | 69% | 48.5 | 167 | - | - | - | 193 |
| REC-S | 0.75 | 75% | 41.0 | 123 | - | - | - | 180 |
| REC-S | 1.0 | 100% | 25.5 | 28.1 | - | - | - | 144 |
| LF2L-m1+PH | 0.0 | 63% | 66.0 | 278 | - | - | - | 202 |
| LF2L-m1+PH | 0.1 | 66% | 61.1 | 250 | - | - | - | 204 |
| LF2L-m1+PH | 0.25 | 72% | 54.2 | 205 | - | - | - | 208 |
| LF2L-m1+PH | 0.5 | 81% | 43.8 | 135 | - | - | - | 213 |
| LF2L-m1+PH | 0.75 | 87% | 35.1 | 85.9 | - | - | - | 218 |
| LF2L-m1+PH | 1.0 | 100% | 25.5 | 28.1 | - | - | - | 226 |
| UB | 0.0 | 63% | 66.0 | 278 | - | - | - | 202 |
| UB | 0.1 | 63% | 66.2 | 279 | - | - | - | 203 |
| UB | 0.25 | 63% | 66.2 | 279 | - | - | - | 203 |
| UB | 0.5 | 62% | 66.7 | 281 | - | - | - | 206 |
| UB | 0.75 | 62% | 66.9 | 281 | - | - | - | 207 |
| UB | 1.0 | 62% | 67.3 | 282 | - | - | - | 208 |
| PNS | 0.0 | 63% | 66.0 | 278 | - | - | - | 202 |
| PNS | 0.1 | 68% | 61.7 | 253 | - | - | - | 210 |
| PNS | 0.25 | 74% | 55.6 | 218 | - | - | - | 221 |
| PNS | 0.5 | 82% | 45.9 | 161 | - | - | - | 236 |
| PNS | 0.75 | 87% | 37.7 | 120 | - | - | - | 249 |
| PNS | 1.0 | 98% | 26.9 | 59.1 | - | - | - | 264 |

*H_early*

| strat | u | fi_found | fi_air | fi_lat | fb_found | fbb_air | bpub_air | pub_air |
|---|---|---|---|---|---|---|---|---|
| REC | 0.0 | 33% | 91.7 | 94.5 | 11% | - | - | 202 |
| REC | 0.1 | 38% | 87.1 | 89.7 | 12% | - | - | 207 |
| REC | 0.25 | 47% | 79.9 | 82.6 | 12% | - | - | 214 |
| REC | 0.5 | 64% | 63.8 | 66.1 | 11% | - | - | 224 |
| REC | 0.75 | 79% | 47.0 | 49.2 | 11% | - | - | 232 |
| REC | 1.0 | 100% | 25.5 | 28.1 | 12% | - | - | 243 |
| REC-S | 0.0 | 33% | 91.7 | 94.5 | 11% | - | - | 202 |
| REC-S | 0.1 | 34% | 88.4 | 90.7 | 10% | - | - | 198 |
| REC-S | 0.25 | 38% | 85.3 | 87.0 | 10% | - | - | 195 |
| REC-S | 0.5 | 46% | 73.6 | 74.1 | 7% | - | - | 193 |
| REC-S | 0.75 | 61% | 61.5 | 62.0 | 2% | - | - | 180 |
| REC-S | 1.0 | 100% | 25.5 | 28.1 | 1% | - | - | 144 |
| LF2L-m1+PH | 0.0 | 33% | 91.7 | 94.5 | 11% | - | - | 202 |
| LF2L-m1+PH | 0.1 | 38% | 86.9 | 89.7 | 12% | - | - | 204 |
| LF2L-m1+PH | 0.25 | 47% | 79.3 | 82.1 | 12% | - | - | 208 |
| LF2L-m1+PH | 0.5 | 63% | 62.8 | 65.6 | 12% | - | - | 213 |
| LF2L-m1+PH | 0.75 | 78% | 46.4 | 49.0 | 13% | - | - | 218 |
| LF2L-m1+PH | 1.0 | 100% | 25.5 | 28.1 | 14% | - | - | 226 |
| UB | 0.0 | 33% | 91.7 | 94.5 | 11% | - | - | 202 |
| UB | 0.1 | 32% | 94.2 | 96.4 | 11% | - | - | 203 |
| UB | 0.25 | 31% | 98.3 | 99.1 | 11% | - | - | 203 |
| UB | 0.5 | 26% | 106 | 105 | 11% | - | - | 206 |
| UB | 0.75 | 23% | 113 | 109 | 11% | - | - | 207 |
| UB | 1.0 | 19% | 120 | 114 | 11% | - | - | 208 |
| PNS | 0.0 | 33% | 91.7 | 94.5 | 11% | - | - | 202 |
| PNS | 0.1 | 36% | 87.5 | 90.2 | 12% | - | - | 210 |
| PNS | 0.25 | 41% | 81.7 | 84.4 | 12% | - | - | 221 |
| PNS | 0.5 | 52% | 68.6 | 70.9 | 11% | - | - | 236 |
| PNS | 0.75 | 59% | 55.8 | 57.9 | 10% | - | - | 249 |
| PNS | 1.0 | 73% | 41.3 | 43.3 | 11% | - | - | 264 |


**PRS with UB/REC, and mixed (S = 1000 / 3000)**

| S | strat | u | scen | pub_acc | pub_air | fi_found | fi_air | fb_found | fbb_air | bpub_air | fib_found | fib_air |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | REC+PRS | 0.25 | C | 3.1 | 385 | 98% | 42.0 | 92% | 20.6 | 52.9 | 98% | 54.4 |
| 1000 | REC+PRS | 0.25 | PB_early | 3.1 | 385 | 67% | 60.6 | - | - | - | - | - |
| 1000 | REC+PRS | 0.5 | C | 3.6 | 413 | 99% | 37.9 | 94% | 14.4 | 38.3 | 98% | 56.6 |
| 1000 | REC+PRS | 0.5 | PB_early | 3.6 | 413 | 78% | 47.7 | - | - | - | - | - |
| 1000 | REC+PRS | 0.75 | C | 3.9 | 436 | 99% | 33.4 | 95% | 7.0 | 20.2 | 97% | 61.1 |
| 1000 | REC+PRS | 0.75 | PB_early | 3.9 | 436 | 86% | 38.0 | - | - | - | - | - |
| 1000 | REC+PRS | 1.0 | C | 4.4 | 466 | 100% | 25.5 | 98% | 0.6 | 2.4 | 98% | 65.5 |
| 1000 | UB+PRS | 1.0 | C | 4.4 | 397 | 100% | 44.7 | 99% | 0.8 | 2.6 | 98% | 62.8 |
| 3000 | REC+PRS | 1.0 | C | 3.6 | 535 | 100% | 27.1 | 98% | 0.2 | 0.7 | 97% | 92.9 |
| 3000 | UB+PRS | 1.0 | C | 3.7 | 457 | 98% | 65.1 | 98% | 0.2 | 0.7 | 96% | 86.6 |

## 7. Early partition over time (S = 1000, shared node state, 40 lookups by island nodes, 2 min apart)

Failure memory (unreliable marking) is per node, so 0.1.x islands barely learn: each node does few lookups.

published before partition
| strat | ops 1-5 found | lat | to | ops 6-15 found | lat | to | ops 16-40 found | lat | to |
|---|---|---|---|---|---|---|---|---|---|
| BASE | 54% | 302 | 7.3 | 60% | 293 | 7.0 | 62% | 287 | 6.9 |
| PH | 57% | 126 | 2.2 | 62% | 104 | 1.6 | 61% | 82 | 1.1 |
| PHp | 57% | 44 | 0.0 | 61% | 42 | 0.0 | 59% | 42 | 0.0 |
| PNS | 92% | 91 | 1.9 | 92% | 89 | 1.8 | 92% | 88 | 1.8 |
| LF2L-m1+PH | 100% | 28 | 0.0 | 100% | 26 | 0.0 | 100% | 27 | 0.0 |
| REC | 100% | 28 | 0.0 | 100% | 26 | 0.0 | 100% | 27 | 0.0 |
published during partition
| strat | ops 1-5 found | lat | to | ops 6-15 found | lat | to | ops 16-40 found | lat | to |
|---|---|---|---|---|---|---|---|---|---|
| BASE | 62% | 270 | 6.6 | 70% | 250 | 5.9 | 73% | 235 | 5.5 |
| PH | 65% | 113 | 2.1 | 73% | 95 | 1.5 | 72% | 67 | 0.8 |
| PHp | 64% | 36 | 0.0 | 71% | 37 | 0.0 | 69% | 35 | 0.0 |
| PNS | 98% | 62 | 1.2 | 98% | 53 | 0.9 | 98% | 53 | 0.9 |
| LF2L-m1+PH | 100% | 26 | 0.0 | 100% | 27 | 0.0 | 100% | 27 | 0.0 |
| REC | 100% | 26 | 0.0 | 100% | 27 | 0.0 | 100% | 27 | 0.0 |

## 8. Load that the backbone puts on islands

Island servers are ordinary DHT servers, so backbone lookups route through them and backbone publishers store on
them whenever they are among the K closest (about 1 in 5 of the keyspace here). Each such contact crosses the
gateway and the LoRa hops. Per-op LoRa cost from section 1, scaled to one island of 30 nodes (share of all island
servers) and a per-backbone-node activity level, as a fraction of a 1 kbit/s channel (86 400 s/day):

| S | strategy | LoRa s per backbone publish | per backbone lookup | island(30 nodes) channel use from backbone, light (10 CIDs, 10 lookups /node/day) | heavy (100 CIDs, 50 lookups) | own island publishing (10 CIDs/node/3 d) |
|---|---|---|---|---|---|---|
| 1000 | BASE | 70 | 27.1 | 135% | 1158% | 41% |
| 1000 | PNS | 57 | 13.3 | 97% | 879% | 40% |
| 1000 | PNS50 | 65 | 22.0 | 121% | 1057% | 40% |
| 1000 | PRS | 3 | 0.8 | 5% | 41% | 41% |
| 1000 | REC | 65 | 22.0 | 121% | 1057% | 54% |
| 1000 | REC-S | 0 | 0.0 | 0% | 0% | 53% |
| 3000 | BASE | 74 | 33.2 | 141% | 1188% | 44% |
| 3000 | PNS | 58 | 13.9 | 94% | 848% | 43% |
| 3000 | PNS50 | 67 | 26.2 | 122% | 1044% | 43% |
| 3000 | PRS | 1 | 0.2 | 1% | 10% | 44% |
| 3000 | REC | 67 | 26.2 | 122% | 1044% | 62% |
| 3000 | REC-S | 0 | 0.0 | 0% | 0% | 59% |

PRS (fast nodes rank nodes > 3 hops away last) and SCOPE (island servers invisible to the backbone) remove
almost all of it. PNS only halves the lookup part (stores to island nodes among the K closest remain).
Own-island publishing alone (10 CIDs per node, republished every 3 days, ~360-530 s of airtime per key) already
uses ~40-60% of the channel: publish cost from a LoRa node is dominated by the 10 /provide stores, each on its own
identified link (link + identify + request + close, ~10 s per LoRa hop).

## 9. Recovery after a partition heals

Records published (or republished) during a partition are stored only on island nodes (section 3: a backbone
searcher finds 1-14% of them). 0.1.x recovers only at the publisher's next republish (3 days on slow nodes).
A heal-triggered republish (PH: when an off-island announce/answer is seen again after a detected partition,
republish the keys whose last round reached no off-island node) is bounded by the island channel:

| partition length D | baseline: mean wait for next republish (slow node, 3 d interval) | heal-republish (PH): 1 key/node, 30-node island | 5 keys/node | 20 keys/node |
|---|---|---|---|---|
| 1 h | 3.0 d | mean 3.9 h, last 7.8 h | mean 19.4 h, last 38.9 h | mean 3.2 d, last 6.5 d |
| 12 h | 2.8 d | mean 3.9 h, last 7.8 h | mean 19.4 h, last 38.9 h | mean 3.2 d, last 6.5 d |
| 48 h | 2.0 d | mean 3.9 h, last 7.8 h | mean 19.4 h, last 38.9 h | mean 3.2 d, last 6.5 d |
| 192 h | 36.0 h | mean 3.9 h, last 7.8 h | mean 19.4 h, last 38.9 h | mean 3.2 d, last 6.5 d |

(per-key island publish (REC, S=1000): 466 s airtime, 431 s latency; channel share U=0.5; detection 120 s)

So heal-republish helps a lot when few keys are affected (hours instead of ~3 days), and stops helping when
an island has more than ~10-20 affected keys per node; it must stay paced (the existing slow-node pacing) and only
cover keys whose last round failed globally. Records published *before* a partition are unaffected unless the
partition outlasts their TTL (7 days after their last successful store), after which the same applies.

## 10. PNS and eclipse exposure

Attacker with ground IDs one hop from the victim (on its LoRa segment, or a direct TCP connection to it), backbone
victim in S = 1000, buckets' honest candidates from the world, 5% of honest entries going stale per day. Share of
the victim's routing-table slots held by the attacker (low buckets with free slots go to the attacker under any
policy, which is why BASE is not 0):

| attacker IDs per targeted bucket | days of churn (5%/day) | BASE (first heard) | PNS50 (half the slots by hops, only open slots) | PNS (all open slots by hops) | PNS displacing incumbents |
|---|---|---|---|---|---|
| 2 | 7 | 10% | 23% | 23% | 23% |
| 2 | 30 | 12% | 23% | 23% | 23% |
| 2 | 90 | 12% | 23% | 23% | 23% |
| 5 | 7 | 21% | 39% | 39% | 54% |
| 5 | 30 | 27% | 54% | 54% | 54% |
| 5 | 90 | 28% | 54% | 54% | 54% |
| 10 | 7 | 29% | 45% | 45% | 100% |
| 10 | 30 | 43% | 68% | 83% | 100% |
| 10 | 90 | 48% | 74% | 99% | 100% |
| 20 | 7 | 31% | 45% | 45% | 100% |
| 20 | 30 | 47% | 70% | 83% | 100% |
| 20 | 90 | 54% | 77% | 99% | 100% |

Displacing healthy incumbents for proximity (textbook PNS) lets ~10 ground IDs per bucket eclipse a node at
once; it must not be done. Choosing by proximity only when a slot opens keeps rnfs's incumbency rule but still
lets a nearby attacker win every opened slot, so over months its share approaches that of displacement. Capping
proximity-chosen slots at half a bucket (PNS50) bounds that; provider ranking already puts seniority before hops.
