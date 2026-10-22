# Topology-aware lookups for rnfs: simulation study

Status: complete (all experiments below were run; numbers are means over 900 trials = 300 x 3 seeds unless noted).
Section 11 (added for 0.2.0) re-runs the configurations with RNS 1.5.6's ingress limit for path requests modelled,
for rnfs as built and with the planned pacing.

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

Code: `sim.py` (world, lookup engine mirroring rnfs/dht/lookup.py and node.py start nodes), `experiments.py` (scenarios; `python experiments.py connected|partition|mixed|fsweep|local|prs 300`), `learning.py`, `analysis.py` (load, recovery, PNS exposure), `tables.py`; section 11: `prlimit.py`, `prsim.py`, `exp_prlimit.py`, `validate_prlimit.py`.
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

## 11. Path requests under RNS's ingress limit (0.2.0 as built, and with the planned pacing)

Since RNS 1.2.5 every interface limits the path requests arriving on it. This section re-runs the topology
configurations with that limit modelled exactly, for rnfs 0.2.0 as built (sweep providing, disjoint paths, slow nodes'
budgets of 10 per publish lookup and 3 per search, two-level records, partition mode, slow-path demotion), and with
the pacing `roadmap.md` plans ("Pacing path requests"). Code: `prlimit.py` (RNS's limit and queue, and the planned
pacer), `prsim.py` (an engine running rnfs's operations as concurrent processes on one clock, on `sim.py`'s world),
`exp_prlimit.py` (the experiments and tables), `validate_prlimit.py` (the port checked against RNS's own code).

### 11.1 What is modelled, and how it matches RNS 1.5.6

**The limit** (`prlimit.PRIngress`, a line-for-line port with time passed in):

- `Interface.received_path_request` appends each arrival time to the receiving interface's `ip_freq_deque` (at most
  `IA_FREQ_SAMPLES` = 48), and to its parent's (a hub's server interface). `Transport.inbound` counts a path request only
  after dropping duplicate tags, so a request flooded over an island is counted once by each node.
- `Interface.incoming_pr_frequency`: 0 while the deque holds 2 times or fewer, else n / (now − oldest), with the oldest
  popped (one per call, after n is counted) when it is over `PR_FREQ_DECAY` (10 s) old.
- `Interface.should_ingress_limit_pr`: threshold `IC_PR_BURST_FREQ_NEW` = 3 while the interface is under
  `IC_NEW_TIME` (2 h) old, else `IC_PR_BURST_FREQ` = 8; activates above it; releases only after a call that sees the
  estimate below it more than `IC_BURST_HOLD` (15 s) after activation and after the last call at or above it, and then
  three more such calls (the cooldown); any other call resets the cooldown.
- It is evaluated where RNS evaluates it, since each evaluation pops: in `Transport.inbound` for every path request, again
  in `Transport.path_request` when that request wasn't limited, and for every interface in `Transport.jobs` each
  `interface_jobs_interval` (5 s).
- A limited request is queued as `TC_INGRESS_LIMITED` in `InboundQueues` (`prlimit.InboundQueue`): strict priority
  behind data, announces and other path requests, one queue of `INBOUND_IL_QUEUE_LENGTH` = 8 shared by every interface
  of the instance, dropped when full. The drainer (`Transport.inbound_job`) takes 2 ms a packet at a hub (each answer
  reads the cached announce from disk and scans the interface list in `Transport.get_cached_packet`), with 20% of its
  time taken by data and announces; 0.5 and 10 ms are tried too.
- `Transport.path_request` answers a limited request from the path table as any other (the cached announce, after
  `PATH_REQUEST_GRACE` = 0.4 s on the next 0.25 s job run), but doesn't forward it to discover an unknown path.
- `validate_prlimit.py` feeds 2,000 random arrival sequences (bursts, paced, Poisson at 0.2–20/s; new and old
  interfaces) through RNS 1.5.6's own `Interface` methods with `time.time` mocked and through the port: all 657,231
  evaluations agree, and the deques match after every arrival.

**Where it applies.** The gateway's LoRa interface counts every path request flooded over its island (the island's
first copy to reach it, at least one PR airtime apart: 0.6 s at ~1 kbit/s); the gateway answers from its path table
(it hears every announce). A backbone node's path requests go over its TCP link to its hub, counted on the hub's
spawned interface for that client (new for 2 h after the client connects) and on the hub's server interface (the
parent, which shows in `get_interface_stats` but limits nothing: `Transport.inbound` checks the receiving interface).
A request dropped from the queue, or not answered, fails after rnfs's `path_timeout` (30 s).

**RNS's own path requests.** After a link that never established, a node that isn't a transport node expires the path
and, if it last asked more than `PATH_REQUEST_MI` (20 s) ago, requests it again, at most one every 0.5 s
(`Transport.jobs`, RNS 1.5.6 `Transport.py` ~697–720, `handle_disovery_path_requests`). Half of each island's nodes
are transport nodes; backbone nodes aren't.

**rnfs as built** (`prsim.Engine`): lookups as `sim.Sim.lookup`, run as disjoint paths sharing no node (2 on slow
nodes, 3 elsewhere; start nodes dealt in turn), with `rnfs.dht.lookups`' budgets of path requests (`split_budget`:
a slow node's publish lookup 10 over its 2 paths, its search 3 and its fallback 3; elsewhere alpha = 3 per path). A
publish round is `rnfs.dht.sweep.Sweep` as built: a lookup takes the next due key not covered and not within
estimate − 1 leading bits of a key being looked up (the first lookup of a round on a node that knows no region yet
runs alone), and covers every due key sharing the bits its key shares with the 10th closest server it found; 4 lookups
at once on fast nodes, one at a time and 10 a round on slow nodes. Each key's store set: its 10 closest from what its
region learned, each path's 10 closest, the 10 closest senior servers of the announce first-seen log and the 3 local
replicas. Stores need no path request (every target answered or has a known path), so records are placed when the
lookups end. A join: bootstrap nodes one at a time until 2 answer, then a lookup for the node's own ID (alpha
unknowns). A node starting for the first time knows its island (if on one) and nothing else; its routing table holds
the servers that answered it.

**The planned pacing** (`PACE1`): one node-wide budget, rnfs's path requests at least 1 s apart (0.5 and 2 s tried); a
lookup waiting for it asks candidates with a known path meanwhile (`PACE1-idle`: it waits instead; `PACE1-k`: only
among its 10 closest candidates); a failed link charges the budget (RNS will re-request); a query failed for want of a
path doesn't count towards partition mode. `PACE1-ceilact`: island nodes also hold back while their own LoRa interface
limits path requests (`pr_burst_active`; `-b`: with a random backoff of up to 20 s).

**Configurations.** Connected networks of 1,000 and 3,000 servers (5 world seeds × 100 trials); an island-wide load
on every island of the 1,000-server worlds (24 h steady: every island server publishes a round every 12 h and searches
10 times a day; cold start: every island server starts for the first time within 10 minutes, joins and publishes its
first round), at ~1 kbit/s and at 5.6× that (SF7 at 125 kHz), gateway interface new (3/s) or older than 2 h (8/s); a
public hub (hub 0 of the 1,000-server worlds, ~100 clients) that fast nodes join for the first time at 2–30 an hour
for 2 h; the gateway down (section 2's early scenarios). `RNS-full`, `PACE1-full`: island transport nodes in RNS's
default interface mode (`full`), which doesn't forward a request for an unknown path, so an island node more than one
hop from its gateway gets a path only from a neighbour that has it. Cells give the mean over all trials and, in
brackets, the lowest and highest of the 5 seeds' means.

### 11.2 Results

Modes: OFF = no limit (the study so far; what the limit would have done is still counted), RNS = RNS 1.5.6's limit with rnfs as built, PACE* = with the planned pacing (11.1). Mean (lowest–highest of the 5 seed means).

**An island server publishes: one sweep round of 100 due keys** (connected)

| S | mode | region lookups | keys covered | path requests (rnfs) | per region lookup | RNS re-requests | limited | dropped | path failures | placement (of 10) | round s | LoRa s | partition mode entered |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | OFF | 10.0 | 57.7 (55.3–60.1) | 54.3 (53.9–55.0) | 5.4 (5.4–5.5) | 0.0 | 0.0 | 0.0 | 0.0 | 3.7 (3.5–3.9) | 1473 (1358–1613) | 2795 (2528–3053) | 0% |
| 1000 | PACE0.5 | 10.0 | 56.0 (54.1–58.4) | 53.9 (53.5–54.4) | 5.4 (5.3–5.4) | 0.0 | 0.0 | 0.0 | 0.0 | 3.7 (3.5–3.9) | 1530 (1412–1668) | 2864 (2590–3116) | 0% |
| 1000 | PACE1 | 10.0 | 56.0 (54.1–58.2) | 53.9 (53.5–54.4) | 5.4 (5.3–5.4) | 0.0 | 0.0 | 0.0 | 0.0 | 3.7 (3.5–3.9) | 1534 (1415–1671) | 2869 (2595–3120) | 0% |
| 1000 | PACE1-full | 9.6 (9.3–9.8) | 78.8 (75.1–81.4) | 75.6 (72.7–78.0) | 8.0 (7.7–8.1) | 0.0 | 0.0 | 0.0 | 55.4 (50.3–60.5) | 2.4 (2.3–2.6) | 2072 (1853–2274) | 2664 (2297–2951) | 0% |
| 1000 | PACE1-idle | 10.0 | 57.7 (55.3–60.1) | 54.3 (53.9–55.0) | 5.4 (5.4–5.5) | 0.0 | 0.0 | 0.0 | 0.0 | 3.7 (3.5–3.9) | 1474 (1360–1614) | 2795 (2528–3053) | 0% |
| 1000 | PACE1-k | 10.0 | 57.0 (54.8–59.4) | 53.9 (53.5–54.5) | 5.4 (5.4–5.5) | 0.0 | 0.0 | 0.0 | 0.0 | 3.7 (3.5–3.9) | 1517 (1399–1650) | 2846 (2575–3092) | 0% |
| 1000 | PACE2 | 10.0 | 56.0 (54.1–58.2) | 53.9 (53.4–54.4) | 5.4 (5.3–5.4) | 0.0 | 0.0 | 0.0 | 0.0 | 3.7 (3.5–3.9) | 1537 (1417–1675) | 2873 (2598–3126) | 0% |
| 1000 | RNS | 10.0 | 57.7 (55.3–60.1) | 54.3 (53.9–55.0) | 5.4 (5.4–5.5) | 0.0 | 0.0 | 0.0 | 0.0 | 3.7 (3.5–3.9) | 1473 (1358–1613) | 2795 (2528–3053) | 0% |
| 1000 | RNS-full | 6.7 (6.3–7.2) | 85.5 (82.7–87.5) | 22.6 (19.9–26.0) | 2.7 (2.5–3.0) | 0.0 | 0.0 | 0.0 | 3.4 (3.0–3.8) | 2.4 (2.3–2.6) | 611 (566–655) | 989 (899–1072) | 65% (58%–70%) |
| 3000 | OFF | 10.0 | 54.9 (51.2–59.7) | 67.1 (66.5–67.5) | 6.7 (6.7–6.8) | 0.0 | 0.0 | 0.0 | 0.0 | 2.2 (2.1–2.3) | 1873 (1716–2151) | 3567 (3250–4087) | 0% |
| 3000 | PACE0.5 | 10.0 | 53.1 (49.4–56.6) | 66.4 (66.0–66.9) | 6.6 (6.6–6.7) | 0.0 | 0.0 | 0.0 | 0.0 | 2.2 (2.1–2.4) | 1947 (1784–2229) | 3658 (3336–4192) | 0% |
| 3000 | PACE1 | 10.0 | 53.0 (49.4–56.7) | 66.3 (65.9–66.9) | 6.6 (6.6–6.7) | 0.0 | 0.0 | 0.0 | 0.0 | 2.2 (2.1–2.4) | 1952 (1788–2233) | 3666 (3343–4200) | 0% |
| 3000 | PACE1-full | 9.2 (8.8–9.4) | 82.4 (78.7–85.0) | 81.6 (77.7–84.4) | 9.0 (8.7–9.2) | 0.0 | 0.0 | 0.0 | 62.4 (54.3–70.4) | 1.0 (0.9–1.2) | 2236 (2101–2501) | 2911 (2582–3262) | 0% |
| 3000 | PACE1-idle | 10.0 | 54.9 (51.2–59.7) | 67.1 (66.5–67.5) | 6.7 (6.7–6.8) | 0.0 | 0.0 | 0.0 | 0.0 | 2.2 (2.1–2.3) | 1874 (1718–2152) | 3567 (3251–4087) | 0% |
| 3000 | PACE1-k | 10.0 | 54.5 (50.9–59.5) | 66.7 (66.3–67.1) | 6.7 (6.6–6.7) | 0.0 | 0.0 | 0.0 | 0.0 | 2.2 (2.1–2.3) | 1902 (1738–2184) | 3601 (3275–4129) | 0% |
| 3000 | PACE2 | 10.0 | 53.0 (49.3–56.7) | 66.3 (66.0–66.9) | 6.6 (6.6–6.7) | 0.0 | 0.0 | 0.0 | 0.0 | 2.2 (2.1–2.4) | 1957 (1791–2241) | 3672 (3346–4208) | 0% |
| 3000 | RNS | 10.0 | 54.9 (51.2–59.7) | 67.1 (66.5–67.5) | 6.7 (6.7–6.8) | 0.0 | 0.0 | 0.0 | 0.0 | 2.2 (2.1–2.3) | 1873 (1716–2151) | 3567 (3250–4087) | 0% |
| 3000 | RNS-full | 6.1 (5.7–6.9) | 87.7 (84.1–90.7) | 22.1 (16.6–29.2) | 2.7 (2.2–3.3) | 0.0 | 0.0 | 0.0 | 3.3 (2.8–3.7) | 1.0 (0.9–1.1) | 607 (516–688) | 978 (831–1177) | 72% (61%–81%) |

**Searches and joins** (the island searcher is a neighbour of the publisher, searching for one of its keys)

| S | mode | island searcher: found / s / path requests | backbone searcher: found / s / path requests | island fetches backbone content: found / s | backbone publisher's path requests | island node's first join: path requests / path failures | fast node's first join: path requests |
|---|---|---|---|---|---|---|---|
| 1000 | OFF | 100% / 27.4 / 0.0 | 95% / 1.5 / 0.0 | 100% / 49.0 | 0.0 | 3.0 / 0.0 | 5.0 |
| 1000 | PACE0.5 | 100% / 28.0 / 0.0 | 95% / 1.5 / 0.0 | 100% / 48.8 | 0.0 | 3.0 / 0.0 | 5.0 |
| 1000 | PACE1 | 100% / 27.9 / 0.0 | 95% / 1.5 / 0.0 | 100% / 48.8 | 0.0 | 3.0 / 0.0 | 5.0 |
| 1000 | PACE1-full | 100% / 27.8 / 0.0 | 91% / 1.8 / 0.0 | 98% / 57.0 | 0.0 | 4.2 / 2.3 | 5.0 |
| 1000 | PACE1-idle | 100% / 27.4 / 0.0 | 95% / 1.5 / 0.0 | 100% / 49.0 | 0.0 | 3.0 / 0.0 | 5.0 |
| 1000 | PACE1-k | 100% / 27.8 / 0.0 | 95% / 1.4 / 0.0 | 100% / 48.8 | 0.0 | 3.0 / 0.0 | 5.0 |
| 1000 | PACE2 | 100% / 27.9 / 0.0 | 95% / 1.4 / 0.0 | 100% / 48.8 | 0.0 | 3.0 / 0.0 | 5.0 |
| 1000 | RNS | 100% / 27.4 / 0.0 | 95% / 1.5 / 0.0 | 100% / 49.0 | 0.0 | 3.0 / 0.0 | 5.0 |
| 1000 | RNS-full | 100% / 28.0 / 0.0 | 91% / 1.7 / 0.0 | 96% / 53.5 | 0.0 | 3.9 / 2.3 | 5.0 |
| 3000 | OFF | 100% / 29.6 / 0.0 | 75% / 2.4 / 0.0 | 100% / 69.1 | 0.0 | 3.0 / 0.0 | 5.0 |
| 3000 | PACE0.5 | 100% / 29.1 / 0.0 | 75% / 2.5 / 0.0 | 100% / 67.8 | 0.0 | 3.0 / 0.0 | 5.0 |
| 3000 | PACE1 | 100% / 29.2 / 0.0 | 75% / 2.5 / 0.0 | 100% / 67.8 | 0.0 | 3.0 / 0.0 | 5.0 |
| 3000 | PACE1-full | 100% / 29.2 / 0.0 | 61% / 3.0 / 0.0 | 83% / 122 | 0.0 | 4.4 / 2.7 | 5.0 |
| 3000 | PACE1-idle | 100% / 29.6 / 0.0 | 75% / 2.4 / 0.0 | 100% / 69.1 | 0.0 | 3.0 / 0.0 | 5.0 |
| 3000 | PACE1-k | 100% / 29.6 / 0.0 | 76% / 2.5 / 0.0 | 100% / 69.2 | 0.0 | 3.0 / 0.0 | 5.0 |
| 3000 | PACE2 | 100% / 29.3 / 0.0 | 75% / 2.5 / 0.0 | 100% / 67.6 | 0.0 | 3.0 / 0.0 | 5.0 |
| 3000 | RNS | 100% / 29.6 / 0.0 | 75% / 2.4 / 0.0 | 100% / 69.1 | 0.0 | 3.0 / 0.0 | 5.0 |
| 3000 | RNS-full | 100% / 29.3 / 0.0 | 58% / 3.0 / 0.0 | 79% / 104 | 0.0 | 3.8 / 2.7 | 5.0 |

**A fast node starts for the first time: its first publish round after joining its hub (100 due keys)**

| S | mode | region lookups | path requests | limited | dropped | path failures | trips of its hub interface | peak estimate /s | placement (of 10) | round s | 3 failures in a row |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | OFF | 4.2 (3.5–4.8) | 37.4 (31.8–43.5) | 35.4 (29.8–41.5) | 0.0 | 0.0 | 1.0 (1.0–1.0) | 2226 | 0.5 (0.4–0.5) | 8.2 (7.2–10.0) | 0% |
| 1000 | PACE0.5 | 4.0 (3.4–4.7) | 36.2 (30.3–42.0) | 0.0 | 0.0 | 0.0 | 0.0 | 3.0 | 0.4 (0.4–0.5) | 23.1 (19.7–26.5) | 0% |
| 1000 | PACE1 | 4.3 (3.7–4.8) | 38.4 (33.3–42.8) | 0.0 | 0.0 | 0.0 | 0.0 | 1.5 | 0.6 (0.5–0.6) | 41.3 (36.1–45.4) | 0% |
| 1000 | PACE1-full | 4.3 (3.8–4.8) | 38.4 (33.7–42.9) | 0.0 | 0.0 | 0.0 | 0.0 | 1.5 | 0.6 (0.5–0.6) | 41.2 (36.4–45.6) | 0% |
| 1000 | PACE1-idle | 4.3 (3.8–4.8) | 38.6 (33.8–43.5) | 0.0 | 0.0 | 0.0 | 0.0 | 1.5 | 0.6 (0.5–0.6) | 41.5 (35.8–46.6) | 0% |
| 1000 | PACE1-k | 4.3 (3.7–4.8) | 38.3 (33.5–43.2) | 0.0 | 0.0 | 0.0 | 0.0 | 1.5 | 0.5 (0.5–0.6) | 41.2 (35.7–46.1) | 0% |
| 1000 | PACE2 | 4.4 (3.8–4.9) | 39.2 (34.4–44.1) | 0.0 | 0.0 | 0.0 | 0.0 | 0.8 | 0.6 (0.5–0.7) | 78.9 (69.4–88.5) | 0% |
| 1000 | RNS | 3.8 (3.5–4.4) | 34.4 (31.0–39.4) | 32.4 (29.0–37.3) | 6.1 (5.1–7.0) | 6.1 (5.1–7.0) | 1.0 (1.0–1.0) | 2222 | 0.4 (0.4–0.4) | 23.6 (20.0–27.1) | 59% (50%–69%) |
| 1000 | RNS-full | 3.8 (3.4–4.4) | 34.4 (30.9–39.6) | 32.4 (28.9–37.5) | 6.1 (5.1–7.0) | 6.1 (5.1–7.0) | 1.0 (1.0–1.0) | 2222 | 0.4 (0.4–0.4) | 23.6 (20.0–27.0) | 59% (50%–69%) |
| 3000 | OFF | 4.0 (3.5–5.1) | 35.6 (31.2–45.5) | 33.6 (29.2–43.5) | 0.0 | 0.0 | 1.0 | 2565 | 0.2 (0.1–0.2) | 5.1 (4.4–6.4) | 0% |
| 3000 | PACE0.5 | 3.8 (3.3–4.9) | 34.4 (29.4–43.7) | 0.0 | 0.0 | 0.0 | 0.0 | 3.0 | 0.2 (0.1–0.2) | 20.0 (17.4–25.4) | 0% |
| 3000 | PACE1 | 4.0 (3.4–4.9) | 35.6 (30.8–43.9) | 0.0 | 0.0 | 0.0 | 0.0 | 1.5 | 0.2 (0.2–0.3) | 37.0 (32.1–45.7) | 0% |
| 3000 | PACE1-full | 4.0 (3.4–4.9) | 35.8 (30.7–44.1) | 0.0 | 0.0 | 0.0 | 0.0 | 1.5 | 0.2 (0.2–0.3) | 37.2 (32.0–45.9) | 0% |
| 3000 | PACE1-idle | 4.1 (3.5–5.0) | 36.5 (31.8–45.0) | 0.0 | 0.0 | 0.0 | 0.0 | 1.5 | 0.3 (0.2–0.3) | 38.0 (33.4–46.8) | 0% |
| 3000 | PACE1-k | 4.0 (3.5–5.0) | 36.2 (31.8–44.6) | 0.0 | 0.0 | 0.0 | 0.0 | 1.5 | 0.2 (0.2–0.3) | 37.8 (33.3–46.4) | 0% |
| 3000 | PACE2 | 4.1 (3.6–5.1) | 36.7 (32.3–45.5) | 0.0 | 0.0 | 0.0 | 0.0 | 0.8 | 0.3 (0.3–0.4) | 73.4 (64.5–90.9) | 0% |
| 3000 | RNS | 3.9 (3.5–5.0) | 34.8 (31.1–44.9) | 32.8 (29.1–42.9) | 7.5 (6.3–9.9) | 7.5 (6.3–9.9) | 1.0 | 2565 | 0.1 (0.1–0.2) | 20.5 (17.5–25.4) | 57% (48%–71%) |
| 3000 | RNS-full | 3.9 (3.5–5.0) | 34.8 (31.1–44.9) | 32.8 (29.1–42.9) | 7.4 (6.2–9.9) | 7.4 (6.2–9.9) | 1.0 | 2565 | 0.1 (0.1–0.2) | 20.5 (17.5–25.4) | 57% (48%–71%) |

**A gateway's LoRa interface under island-wide load** (1,000 servers; every island of 5 worlds)

| S | mode | load | LoRa | gateway interface | path requests an hour at the gateway | trips an hour (mean, max island) | limited seconds an hour | limited share | peak estimate /s |
|---|---|---|---|---|---|---|---|---|---|
| 1000 | OFF | cold | 5.6x faster | new (3/s) | 584 | 0.84 (3.3) | 99 | 39.5% | 8.30 |
| 1000 | OFF | cold | 1 kbit/s | new (3/s) | 588 | 0.00 (0.0) | 0 | 0.0% | 2.50 |
| 1000 | OFF | steady | 5.6x faster | over 2 h (8/s) | 129 | 0.05 (0.2) | 1 | 0.2% | 13.89 |
| 1000 | OFF | steady | 5.6x faster | new (3/s) | 129 | 0.06 (0.3) | 1 | 0.3% | 13.89 |
| 1000 | OFF | steady | 1 kbit/s | over 2 h (8/s) | 128 | 0.00 (0.0) | 0 | 0.0% | 2.50 |
| 1000 | OFF | steady | 1 kbit/s | new (3/s) | 128 | 0.00 (0.0) | 0 | 0.0% | 2.50 |
| 1000 | PACE1 | cold | 5.6x faster | new (3/s) | 577 | 0.87 (4.0) | 94 | 36.9% | 7.45 |
| 1000 | PACE1 | cold | 1 kbit/s | new (3/s) | 582 | 0.00 (0.0) | 0 | 0.0% | 2.50 |
| 1000 | PACE1 | steady | 5.6x faster | over 2 h (8/s) | 128 | 0.00 (0.0) | 0 | 0.0% | 3.75 |
| 1000 | PACE1 | steady | 5.6x faster | new (3/s) | 128 | 0.00 (0.0) | 0 | 0.0% | 3.75 |
| 1000 | PACE1 | steady | 1 kbit/s | over 2 h (8/s) | 128 | 0.00 (0.0) | 0 | 0.0% | 2.50 |
| 1000 | PACE1 | steady | 1 kbit/s | new (3/s) | 128 | 0.00 (0.0) | 0 | 0.0% | 2.50 |
| 1000 | PACE1-ceilact | cold | 5.6x faster | new (3/s) | 540 | 9.76 (19.0) | 413 | 31.2% | 13.89 |
| 1000 | PACE1-ceilact | cold | 1 kbit/s | new (3/s) | 582 | 0.00 (0.0) | 0 | 0.0% | 2.50 |
| 1000 | PACE1-ceilact-b | cold | 5.6x faster | new (3/s) | 385 | 19.15 (49.0) | 654 | 19.5% | 13.89 |
| 1000 | PACE1-ceilact-b | cold | 1 kbit/s | new (3/s) | 582 | 0.00 (0.0) | 0 | 0.0% | 2.50 |
| 1000 | RNS | cold | 5.6x faster | new (3/s) | 582 | 0.90 (4.7) | 99 | 39.1% | 7.98 |
| 1000 | RNS | cold | 1 kbit/s | new (3/s) | 587 | 0.00 (0.0) | 0 | 0.0% | 2.50 |
| 1000 | RNS | steady | 5.6x faster | over 2 h (8/s) | 129 | 0.05 (0.2) | 1 | 0.2% | 13.89 |
| 1000 | RNS | steady | 5.6x faster | new (3/s) | 129 | 0.07 (0.3) | 2 | 0.3% | 13.89 |
| 1000 | RNS | steady | 1 kbit/s | over 2 h (8/s) | 128 | 0.00 (0.0) | 0 | 0.0% | 2.50 |
| 1000 | RNS | steady | 1 kbit/s | new (3/s) | 128 | 0.00 (0.0) | 0 | 0.0% | 2.50 |

**A public hub (~100 clients) that fast nodes join for the first time** (1,000 servers, 5 worlds, 2 h; per joining node unless named)

| S | mode | joins an hour | keys | hub ms a packet | path requests (join + first round) | limited | dropped | trips of its interface in its first hour | limited seconds in its first hour | peak estimate /s | placement | round s | 3 failures in a row | hub's ingress-limited queue drops | parent interface trips an hour |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | OFF | 2 | 100 | 2 | 46 | 35 | 0.0 | 1.0 | 43 | 2500.0 | 0.50 | 7 | 0% | 0 | 0.7 |
| 1000 | OFF | 10 | 100 | 0.5 | 44 | 33 | 0.0 | 1.0 | 44 | 2500.0 | 0.48 | 7 | 0% | 0 | 3.2 |
| 1000 | OFF | 10 | 100 | 2 | 44 | 33 | 0.0 | 1.0 | 44 | 2500.0 | 0.48 | 7 | 0% | 0 | 3.2 |
| 1000 | OFF | 10 | 100 | 10 | 44 | 33 | 0.0 | 1.0 | 44 | 2500.0 | 0.48 | 7 | 0% | 0 | 3.2 |
| 1000 | OFF | 10 | 1000 | 2 | 46 | 36 | 0.0 | 1.0 | 44 | 2500.0 | 0.43 | 7 | 0% | 0 | 3.7 |
| 1000 | OFF | 30 | 100 | 2 | 41 | 31 | 0.0 | 1.0 | 43 | 2500.0 | 0.46 | 7 | 0% | 0 | 7.7 |
| 1000 | PACE1 | 2 | 100 | 2 | 45 | 0 | 0.0 | 0.0 | 0 | 1.5 | 0.57 | 41 | 0% | 0 | 0.0 |
| 1000 | PACE1 | 10 | 100 | 0.5 | 47 | 0 | 0.0 | 0.0 | 0 | 1.5 | 0.61 | 44 | 0% | 0 | 0.0 |
| 1000 | PACE1 | 10 | 100 | 2 | 47 | 0 | 0.0 | 0.0 | 0 | 1.5 | 0.62 | 44 | 0% | 0 | 0.0 |
| 1000 | PACE1 | 10 | 100 | 10 | 47 | 0 | 0.0 | 0.0 | 0 | 1.5 | 0.61 | 44 | 0% | 0 | 0.0 |
| 1000 | PACE1 | 10 | 1000 | 2 | 48 | 0 | 0.0 | 0.0 | 0 | 1.5 | 0.43 | 45 | 0% | 0 | 0.0 |
| 1000 | PACE1 | 30 | 100 | 2 | 44 | 0 | 0.0 | 0.0 | 0 | 1.5 | 0.57 | 41 | 0% | 0 | 0.0 |
| 1000 | RNS | 2 | 100 | 2 | 47 | 36 | 8.0 | 1.0 | 42 | 2500.0 | 0.51 | 23 | 62% | 39 | 0.6 |
| 1000 | RNS | 10 | 100 | 0.5 | 44 | 33 | 0.0 | 1.0 | 44 | 2666.7 | 0.48 | 7 | 0% | 0 | 3.2 |
| 1000 | RNS | 10 | 100 | 2 | 43 | 32 | 7.1 | 1.0 | 43 | 2500.0 | 0.43 | 22 | 58% | 160 | 2.8 |
| 1000 | RNS | 10 | 100 | 10 | 41 | 30 | 10.3 | 1.0 | 42 | 2500.0 | 0.36 | 23 | 62% | 231 | 2.2 |
| 1000 | RNS | 10 | 1000 | 2 | 44 | 33 | 7.5 | 1.0 | 43 | 2500.0 | 0.36 | 25 | 68% | 168 | 2.7 |
| 1000 | RNS | 30 | 100 | 2 | 40 | 29 | 7.0 | 1.0 | 42 | 2500.0 | 0.39 | 22 | 59% | 421 | 7.0 |

**The gateway is down** (PD_early: an island server publishes a round; OFF_search: a neighbour searches for backbone content)

| S | scenario | mode | path requests (rnfs + RNS re-requests) | path failures | link failures | entered partition mode | s until it did (or the operation ended) | operation s |
|---|---|---|---|---|---|---|---|---|
| 1000 | OFF_search | OFF | 0.2 + 1.0 | 0.2 | 1.9 | 72% (66%–77%) | 106 (101–109) | 162 (146–170) |
| 1000 | OFF_search | PACE1 | 0.2 + 1.1 | 0.2 | 2.2 | 72% (66%–77%) | 113 (109–117) | 170 (157–178) |
| 1000 | OFF_search | RNS | 0.2 + 1.0 | 0.2 | 1.9 | 72% (66%–77%) | 106 (101–109) | 162 (146–170) |
| 1000 | PD_early | OFF | 0.2 + 1.8 | 0.2 | 3.7 | 100% | 75.2 (71.9–76.4) | 328 (259–409) |
| 1000 | PD_early | PACE1 | 0.2 + 1.8 | 0.2 | 3.8 | 100% | 77.6 (73.1–80.6) | 330 (260–406) |
| 1000 | PD_early | RNS | 0.2 + 1.8 | 0.2 | 3.7 | 100% | 75.2 (71.9–76.4) | 328 (259–409) |
| 3000 | OFF_search | OFF | 0.3 + 1.2 | 0.3 | 2.3 | 88% (85%–90%) | 131 (126–140) | 215 (201–225) |
| 3000 | OFF_search | PACE1 | 0.3 + 1.4 | 0.3 | 2.7 | 88% (85%–90%) | 145 (139–153) | 228 (215–239) |
| 3000 | OFF_search | RNS | 0.3 + 1.2 | 0.3 | 2.3 | 88% (85%–90%) | 131 (126–140) | 215 (201–225) |
| 3000 | PD_early | OFF | 0.1 + 1.9 | 0.1 | 3.7 | 100% | 80.1 (76.3–86.5) | 359 (273–414) |
| 3000 | PD_early | PACE1 | 0.2 + 2.0 | 0.2 | 3.8 | 100% | 82.5 (76.7–90.7) | 361 (273–419) |
| 3000 | PD_early | RNS | 0.1 + 1.9 | 0.1 | 3.7 | 100% | 80.1 (76.3–86.5) | 359 (273–414) |

**An island server publishes: sweep regions against one lookup per key** (OFF, 5 seeds × 40 trials)

| S | keys a round of 10 region lookups covers | their placement (of 10) | one lookup per key: placement |
|---|---|---|---|
| 1000 | 57.1 (55.2–61.9) | 3.6 (3.4–3.7) | 7.0 (7.0–7.1) |
| 3000 | 55.9 (53.3–60.2) | 2.1 (2.0–2.2) | 6.6 (6.5–6.6) |

### 11.3 Conclusions

**Path requests an operation sends** (0.2.0 as built, connected network):

| operation | 1,000 servers | 3,000 servers |
|---|---|---|
| island node, one publish round (10 region lookups, budget 10 each over 2 paths) | 54 (5.4 a region lookup) | 67 (6.7) |
| island node's search (local first, then budget 3) | 0 (the local server has it) | 0 |
| fast node that has heard every announce: publish round, search | 0 | 0 |
| island node's first join (2 bootstrap nodes + own-ID lookup, alpha 1) | 3 | 3 |
| fast node's first join (2 + alpha 3) | 5 | 5 |
| fast node's first publish round after its first join (about 4 region lookups, 9 each at once) | 36–38 | 35–36 |
| restart with saved routing table and RNS paths (not simulated: both are kept, so few are unknown) | about 0 | about 0 |
| gateway down: RNS's own re-requests after failed links, a publish round / a search | 1.8 / 1.0 | 1.9 / 1.2 |

**How often the limit trips.**

- *A gateway's LoRa interface at ~1 kbit/s: never.* Arrivals on one channel are at least one path-request airtime apart
  (0.6 s), and RNS's estimate n / (now − oldest) is then at most 3 / 1.2 s = 2.5 a second, below even a new
  interface's 3. Measured peak: 2.50 under every load, cold start included.
- *At 5.6× that (SF7, 125 kHz)*: steady load, 0.05–0.07 trips an hour per gateway (one a day at most; peak estimate
  13.9 from three requests back to back), 0 with pacing (peak 3.75). An island-wide cold start with the gateway's
  interface new: 0.8–0.9 trips an hour, about 95–100 limited seconds an hour, **with or without pacing**: each node
  paces itself, but the gateway counts the island's sum (580 requests an hour). A limited request on a gateway is still
  answered (the gateway has every path), so rnfs loses nothing; other applications' requests for paths the gateway
  lacks aren't forwarded while it lasts.
- *A public hub's spawned interface for an established client: never* (such a node has every path from announces).
  **Every fast node that starts for the first time trips its own interface once** (its join already sends 5 requests in
  about 1.5 s, and its first region lookup 9 at once, against a threshold of 3 while the interface is new); the limit
  holds about 43 s each time. The hub's server (parent) interface trips about 0.3 times per join, but limits nothing.
- *What a trip costs at a hub* depends on how fast the hub drains its queue: a limited request waits in the shared
  ingress-limited queue (8). At 0.5 ms a packet nothing is dropped; at 2 ms (with 20% of the drainer busy elsewhere) a
  fresh node loses 7–8 of its ~45 requests, at 10 ms 10. Each loss is a query failing after `path_timeout` (30 s),
  counted against the node, and **58–68% of such joins have 3 failures in a row**: a node with local servers (a
  gateway starting for the first time, say) would enter partition mode on a connected network. Its first round then
  takes 22–25 s instead of 7 s.

**With the planned pacing (1 s, node-wide):** no hub interface trips (peak estimate 1.5), nothing is dropped, and no
node has 3 failures in a row. It costs time where requests are many and fast: a fresh fast node's first round takes
41–45 s instead of 7 s. On islands it costs 60–80 s on a 25–31 minute round (4%), since a slow node's requests are
already seconds apart; searches are unchanged (they need none). Placement and findability are unchanged within the
spread (island 3.7 / 2.2 of 10, backbone finds island content 95% / 75%).

**Is 1 s the right spacing?** Yes. RNS's estimate of requests spaced s apart peaks at 1.5 / s (three within 2s), so 1 s
gives 1.5 a second, half a new interface's threshold, leaving room for jitter, RNS's own re-requests and other
applications. 0.5 s peaks at exactly 3.0, which doesn't trip (the test is "above") but has no margin at all: two
requests bunched by the network, or one from another application, trip it. 2 s halves the rate and doubles the
waiting for nothing measurable (a fresh node's round 79 s instead of 41 s). A shorter spacing once the node's own
interface is older than 2 h (threshold 8: 0.19 s would do) would gain little, since established nodes send almost
none.

**What the results say about the planned design.**

1. *"Requests spaced at least 1 s apart, so RNS's estimate stays under 1.5 a second"* holds for a hub's spawned
   interface (only this node's requests reach it) but not for a shared radio channel: the gateway counts every node's.
   At 1 kbit/s the channel itself keeps the estimate under 2.5; on faster LoRa an island-wide cold start trips it with
   pacing as often as without. That is harmless for rnfs (the gateway answers known paths).
2. *The ceiling* ("the busiest interface's `outgoing_pr_frequency` and `pr_burst_active`"): `outgoing_pr_frequency`
   is right (it counts this instance's requests, other applications' included, which is what the next hop's interface
   counts). **Holding back on `pr_burst_active` is harmful on a shared channel**: that state comes from other nodes'
   requests, so every node waits on the same signal and they all resume together when it clears. In the cold start it
   raised trips from 0.9 to 9.8 an hour (19 with a random backoff of up to 20 s, which spreads the releases into more
   short bursts), and limited seconds from 94 to 413–654 an hour. Use the outgoing estimate only.
3. *"While a lookup waits for the budget, it queries the next candidates with a known path"*: neither helps nor hurts
   here (`PACE1-idle`, `PACE1-k` within the spread). Slow nodes' requests are seconds apart anyway, and a fresh fast
   node's lookups hit their budget of 3 a path whatever they ask meanwhile. It can stay, or be dropped as unneeded.
4. *"During a real partition paths to off-island servers stay in RNS's table and fail as links"* is true only on
   transport nodes. A node that isn't one expires the path after the first link that fails to establish and requests it
   again (`Transport.jobs`, ~697–720); with the gateway down nobody answers, so later queries to that server fail for
   want of a path. Detection still works, because each server's first failure is a link failure: not counting path
   failures delays partition mode by 2–14 s (75 → 78 s, 131 → 145 s), and every partitioned publisher still enters it.
5. *Island relays in RNS's default interface mode (`full`)* answer a path request only for paths they have and never
   forward one, so an island node more than one hop from its gateway gets paths only for servers a neighbour heard.
   The study so far (and every row above but the `-full` ones) assumed requests flood the island and reach the
   gateway, which holds only if island transport nodes run `gateway` (or `access_point`, `roaming`) mode or
   `recursive_prs`. With `full` relays and **today's rule (path failures count towards partition mode), 65–72% of island
   publish rounds enter partition mode on a connected network**, and the round stops reaching off-island servers. With
   the planned rule none does, but each of the 55–62 failed requests costs 30 s (round 35–37 min instead of 25–31),
   placement falls to 2.4 / 1.0 of 10 and backbone searchers find island content 91% / 61%. The planned rule is
   needed whatever the limit does, and the slow nodes' budget of 10 buys little beyond one hop of the gateway unless the
   island's relays forward requests; the documentation should say so (gateway mode on island transport nodes).
6. Not about the limit, but measured on the way, with `rnfs.dht.sweep.find_region`'s sizing (a region is the keys
   sharing the bits a key shares with the 10th closest server its lookup found, the closest known, not only answered):
   - *Islands*: an island lookup's result is padded with island servers far from the key, so each region lookup covers
     about 5.5 due keys and their records land on 3.7 / 2.2 of their 10 closest. One lookup per key in the same engine
     gives 7.0 / 6.6 (the roadmap's measured 6.9; the earlier sections' UB, without senior servers, 4.4 / 3.7). Sweep
     thus trades about half the placement accuracy on islands for 5.5 times fewer lookups; backbone searchers find
     island content 95% / 75% (UB in section 1: 99% / 99%). The extra `/find` to a region's far half (`explore`) isn't
     modelled and may narrow the gap a little (an island node can store only to servers whose identity it knows).
   - *A node's first round after its first start*: the first lookup runs alone, its result is padded with whatever few
     servers the node knows, and its region covers most due keys: about 4 region lookups for 100 or 1,000 keys,
     records on 0.5 / 0.2 of their 10 closest, until the next round. Worth a guard (a floor on the region prefix from
     the network size estimate, or a first round that waits until the node has heard announces for a while).

### 11.4 Caveats

The island channel is modelled only as far as path requests need: each request's first copy reaches the gateway at
least one airtime after the previous one, and other traffic, collisions and the announce cap are left out (all of
which spread requests further apart, so trip counts at the gateway are an upper bound). A node's own LoRa interface is
taken to hear what its gateway hears (`PACE1-ceilact`). Hub queue speeds (0.5–10 ms a packet, 20% busy) are assumed,
not measured. Other applications' path requests aren't modelled. The rest of sections 1–10's caveats apply.

### 11.5 Re-running

From this folder (Python 3.11 or later, standard library only; `PROCS` sets the processes, default 8), with world
seeds 1–5:

    python3 exp_prlimit.py ops 100 5          # 11.2 ops tables (both sizes, every mode; ~5 min on 6 processes)
    python3 exp_prlimit.py partition 100 5
    python3 exp_prlimit.py hub 5
    python3 exp_prlimit.py gateway 5          # ~3 min
    python3 exp_prlimit.py perkey 40 5
    python3 exp_prlimit.py tables             # print every table from out/prlimit_*.json

Each trial's seed is world seed × 100003 + trial. `MODES=OFF,RNS,PACE1 python3 exp_prlimit.py ops ...` runs a subset.
The port's check needs RNS 1.5.6: `../../../.venv/bin/python validate_prlimit.py` (in a checkout whose `.venv` has
it).
