# Eclipse / key-hiding simulation for the rnfs DHT

Written incrementally; sections appear as runs finish. Code: `sim.py` (model + lookup engine), `validate.py`,
`run.py` (experiments), raw JSON in `results/`.

## 1. Validation of the fast model against rnfs's real classes

* Routing tables: the fast model's per-node table (bucket i = a random K-subset of the servers at distance
  [2^i, 2^(i+1)), as hearing every announce in random order gives; Sybils added where buckets have room
  ("fresh") or competing for slots at random ("pre-aged")) was fed into a real `rnfs.dht.routing.RoutingTable`:
  every entry accepted and `closest(key, K, for_reply=True)` identical to the model's /find reply in
  45 nodes x 20 keys x 8 attack variants: **0 mismatches**.
* Lookups: rnfs's real `Lookup` (threads, `find_closest` / `find_providers`) run over the same model network vs
  the fast re-implementation (400 searches each):

With replies in send order (what threads over an in-memory network give) the two are **identical** in every
case (same mean queries, same p95, same findability), e.g. S=2000 tight m=10: 8.70 queries, find 0.25% for both;
S=2000 ring m=20: 8.26 queries, 90.75% for both; alpha=1 cases identical too (`validate_fifo.log`).
With random reply latency (real Lookup given Exp(4 ms) sleeps per query; model given Exp latencies) they agree
within sampling noise (`validate_exp.log`):

| S | Sybils | alpha | real queries | model queries | real find | model find |
|---|---|---|---|---|---|---|
| 2000 | 0 | 3 | 6.78 | 6.72 | 100% | 100% |
| 2000 | 10 tight | 3 | 8.07 | 8.04 | 0% | 0% |
| 2000 | 20 ring | 3 | 7.67 | 7.62 | 89.3% | 88.3% |
| 1000 | 10 evasive | 3 | 7.09 | 7.05 | 89.3% | 85.8% |
| 1000 | 0 | 1 | 3.94 | 3.94 | 100% | 100% |
| 1000 | 10 ring | 1 | 4.19 | 4.19 | 91.5% | 91.5% |

All experiments below use random reply latency (Exp), the more realistic ordering.


## 2. Model and simplifications

* Honest servers S in {100, 300, 1000, 10000}, random 128-bit IDs, static. Every server heard every other
  server's announce in random order (h = 1; h = 0.5 in a sensitivity run), so bucket i is a random K-subset
  of the servers at distance [2^i, 2^(i+1)) (all of them if <= K). No churn, no failures, no message loss:
  every honest node answers; the K closest honest nodes are always reachable.
* Lookup = rnfs `Lookup` semantics (validated above): K start nodes = the searcher's K closest table entries,
  alpha in flight among the max(alpha, beta) closest known not yet queried, complete when the beta closest
  known answered, 80-query cap per run, 9-answers stall rule. Searches run to completion (one provider in the
  network, so `max_parallel_providers` never stops them early); success = some honest node that answered
  holds a record for the key (or the publisher itself answered: it records itself).
* Publish = one lookup for the K closest, then /provide to each (Sybils discard). One publish round, no
  republish history (records from earlier rounds at other nodes would only help).
* Sybils: m per attacked key, answer /find with no providers and the K Sybils closest to the asked key.
  Placement `tight`: uniformly closer than every honest node (expected grinding ~S key generations each);
  `ring`: uniformly within the K-th closest honest node's distance (~S/K each); `evasive`: Poisson spacing at
  f_s x the honest density (f_s = 0.75 in the main grid, so the combined density 1.75x stays under the T=2
  density check; ~S/i for the i-th). Timing `fresh`: enter honest buckets only where there's room;
  `pre-aged`: compete for bucket slots at random with honest nodes (present from the start), and count as
  long-known for every seniority-based defence.
* Attacks hit keys 0, 1, 2 (the CID's key and two derived keys) with m each unless stated ("split" runs give
  m/r per key).
* CIs: 95%, clustered by instance (one network + key + publisher; 6 searchers each).

### Defences as implemented (all local; nothing on the wire changes)

* `dens` (density check): size estimate N_est (see sweep); fires when some rank j >= K among known nodes has
  d_j < j*2^128/(T*N_est). Then widen to two rings: the nodes after the dense core's last rank, and the nodes
  beyond rho = K*2^128/(T*N_est) (independent of how many Sybils each node happened to learn, so publisher and
  searcher agree). Publisher stores to the usual K closest plus K per ring; searcher keeps going until 5 nodes
  of each ring have answered. A rank-only ring, as first tried, failed when publisher and searcher knew
  different numbers of Sybils (S=1000, m=50 pre-aged: rank-only 53-73%, radius 78-94%, both 93-96%).
* `est_tab`: also store to / query the 3 closest routing-table entries known for long (senior).
* `est_hrd`: an announce first-seen log (local; every server announce heard, with first-heard time): also
  store to the K closest nodes found that were first heard long ago, and search until the beta closest such
  have answered.
* `disjN`: S/Kademlia: N lookups with disjoint node sets (start nodes split round-robin, a node queried by one
  path is never used by another); publisher stores to each path's K closest; success if any path meets a
  holder.
* `k20`: publisher stores to the 20 closest. `s2k`: searcher continues until the 20 closest known answered.
* `multiR`: also provide under H(i||cid), i < R; searcher tries key 0, then 1, ... until found (cost counted
  until success).
* `srvX` (extra, server side): X% of honest servers leave nodes first heard recently out of their /find
  replies (local reply policy; same message).

## Density-check threshold sweep (exp_fp.py)

Check: after/while a lookup runs, with N_est the size estimate, it fires if some rank j >= K among the known nodes has d_j < j * 2^128 / (T * N_est). Estimator `own`: from the node's own K closest table entries, (K-1)*2^128/d_K. `pool`: median d_K of 16 earlier lookups (random keys). Firing widens (ring 'both', 5 ring nodes must answer, K extra stores per ring).

**False positives without attack** (% of publisher lookups / searcher lookups that fire), and extra /find per search vs baseline:

| S | T=2.0 own | T=2.0 pool | T=2.5 own | T=2.5 pool | T=3.0 own | T=3.0 pool | T=4.0 own | T=4.0 pool | T=5.0 own | T=5.0 pool |
|---|---|---|---|---|---|---|---|---|---|---|
| 100 | 12.3/11.2 (+0.84) | 4.5/4.3 (+0.27) | 5.0/4.6 (+0.34) | 1.4/1.3 (+0.08) | 2.1/2.0 (+0.15) | 0.3/0.3 (+0.02) | 0.5/0.5 (+0.03) | 0.0/0.0 (+0.00) | 0.0/0.1 (+0.01) | 0.0/0.0 (+-0.00) |
| 300 | 12.1/12.5 (+1.15) | 4.3/4.6 (+0.34) | 5.0/5.2 (+0.44) | 1.1/1.1 (+0.07) | 2.3/2.1 (+0.16) | 0.4/0.2 (+0.01) | 0.8/0.4 (+0.02) | 0.0/0.0 (+-0.00) | 0.0/0.1 (+-0.00) | 0.0/0.0 (+-0.01) |
| 1000 | 13.0/13.1 (+1.22) | 5.3/4.8 (+0.37) | 5.2/5.9 (+0.51) | 1.3/1.2 (+0.09) | 2.3/2.7 (+0.22) | 0.5/0.4 (+0.03) | 0.5/0.5 (+0.05) | 0.0/0.1 (+0.01) | 0.1/0.2 (+0.01) | 0.0/0.0 (+0.00) |
| 10000 | 11.6/11.5 (+1.13) | 3.6/3.5 (+0.28) | 4.7/4.3 (+0.36) | 1.0/0.9 (+0.07) | 2.0/1.9 (+0.16) | 0.3/0.3 (+0.01) | 0.6/0.5 (+0.03) | 0.1/0.1 (+0.00) | 0.1/0.1 (+0.01) | 0.1/0.0 (+-0.01) |

**tight m=10 (pre-aged)**: P(find) % (searcher-lookup detection %); baseline first

| S | base | T=2.0 own | T=2.0 pool | T=2.5 own | T=2.5 pool | T=3.0 own | T=3.0 pool | T=4.0 own | T=4.0 pool | T=5.0 own | T=5.0 pool |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | 1.9 | 73.9 (96) | 81.2 (100) | 77.5 (94) | 84.0 (99) | 77.7 (92) | 84.1 (98) | 75.2 (88) | 83.9 (95) | 71.8 (84) | 80.5 (91) |
| 300 | 0.9 | 88.2 (97) | 93.2 (99) | 88.9 (96) | 92.7 (98) | 88.6 (94) | 91.5 (97) | 84.1 (91) | 88.9 (92) | 80.4 (87) | 84.3 (88) |
| 1000 | 0.7 | 96.2 (99) | 98.2 (100) | 96.0 (98) | 97.9 (99) | 95.2 (97) | 96.9 (98) | 91.6 (94) | 93.9 (96) | 87.2 (90) | 88.9 (91) |
| 10000 | 0.4 | 98.0 (99) | 99.0 (99) | 96.9 (98) | 98.2 (99) | 95.1 (97) | 96.9 (97) | 91.3 (93) | 93.0 (93) | 86.4 (89) | 88.3 (89) |

**ring m=20 (pre-aged)**: P(find) % (searcher-lookup detection %); baseline first

| S | base | T=2.0 own | T=2.0 pool | T=2.5 own | T=2.5 pool | T=3.0 own | T=3.0 pool | T=4.0 own | T=4.0 pool | T=5.0 own | T=5.0 pool |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | 33.6 | 61.6 (65) | 72.6 (87) | 56.8 (55) | 64.9 (74) | 51.1 (46) | 57.3 (61) | 44.4 (31) | 45.4 (37) | 40.6 (20) | 40.2 (21) |
| 300 | 34.6 | 74.0 (76) | 80.5 (88) | 67.9 (66) | 68.9 (73) | 59.0 (54) | 57.0 (58) | 48.9 (35) | 44.9 (32) | 41.8 (23) | 39.4 (16) |
| 1000 | 32.4 | 78.1 (82) | 82.0 (88) | 68.1 (71) | 68.5 (72) | 60.9 (59) | 58.4 (55) | 48.6 (39) | 44.9 (31) | 41.2 (25) | 39.3 (17) |
| 10000 | 28.0 | 75.8 (80) | 77.9 (83) | 64.6 (67) | 64.8 (67) | 57.5 (55) | 50.9 (50) | 42.2 (34) | 36.5 (25) | 35.7 (21) | 32.4 (13) |

**evasive m=20, Sybil density 1.5x honest (pre-aged)**: P(find) % (searcher-lookup detection %); baseline first

| S | base | T=2.0 own | T=2.0 pool | T=2.5 own | T=2.5 pool | T=3.0 own | T=3.0 pool | T=4.0 own | T=4.0 pool | T=5.0 own | T=5.0 pool |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 100 | 44.8 | 63.4 (53) | 69.6 (79) | 57.8 (40) | 61.4 (59) | 52.4 (30) | 54.8 (41) | 49.4 (17) | 48.6 (19) | 46.3 (9) | 45.4 (7) |
| 300 | 45.2 | 69.0 (66) | 70.4 (76) | 61.1 (50) | 58.6 (53) | 55.7 (37) | 54.7 (33) | 50.6 (19) | 48.8 (12) | 48.6 (10) | 46.8 (4) |
| 1000 | 45.9 | 73.4 (71) | 73.8 (76) | 64.1 (56) | 62.0 (54) | 56.3 (41) | 54.5 (33) | 50.5 (20) | 46.6 (11) | 47.7 (9) | 46.7 (4) |
| 10000 | 38.3 | 66.2 (71) | 63.6 (72) | 56.5 (54) | 50.5 (49) | 48.6 (39) | 44.4 (29) | 39.8 (19) | 37.6 (9) | 38.8 (9) | 36.2 (3) |


Reading: with the pooled size estimate (median of the d_K seen in 16 earlier lookups) false positives are about
4% at T=2, 1% at T=2.5, 0.3% at T=3, and a false positive costs little (on average +0.3 /find per search at
T=2). The node's own neighbourhood alone is 2-4x noisier. Detection of tight Sybils is ~100% at any T <= 3.
Attackers who keep the local density under T x natural ("ring" m=20 is ~3x, "evasive" 2.5x here) evade it;
lower T forces them to dilute more, which helps baseline findability. **Chosen: T = 2, pooled estimator.**
Main-grid evasive Sybils then sit at 0.75x honest density (combined 1.75x).


## 3. Conclusions

### The attack today (baseline 0.1.x)
* **Grinding is free.** Tight Sybils (closer than every honest node) cost ~S key generations each (mean is heavy-tailed:
  ~70k at S=10000, i.e. ~4 s per Sybil on one core at the measured 19k RNS identities/s); ring Sybils (inside the
  K closest) ~S/K (0.06 s at S=10000). An identity-cost defence is not available anyway (wire compatibility).
* **m >= K tight Sybils hide content completely** at every network size: P(find) 0-4% for m = 10, 20, 50, fresh or
  pre-aged, alpha 3 or 1. Ring Sybils hurt mostly when pre-aged (S=10000: 59% / 27% / 5% for m = 10/20/50).
  Evasive Sybils (diluted to 1.75x natural density) still take baseline down to 49-76% when pre-aged, and to
  25% at alpha=1. Fresh ring/evasive Sybils are nearly harmless (67-98%): full buckets keep them out.
* Slow nodes (alpha=1) are more exposed: mean baseline P(find) 34.6% vs 41.8% at alpha=3 over the same attacks
  (m in {20, 50}, six variants), and pre-aged evasive Sybils take them to 25% (vs 48%).

### Ranking (alpha=3, S in {1000, 10000}, m in {10, 20, 50}, all six attack variants; full table in "Summary over attacks" below)

| rank | defence | mean P(find) | worst | +/find per search (no attack) | +msgs per publish (no attack) | note |
|---|---|---|---|---|---|---|
| 1 | density check (T=2) | 90.2% | 52% | +0.33 | +0.85 | ~105 pp gained per extra message; only weak spot is a diluted pre-aged attacker |
| 2 | announce first-seen log (est_hrd) | 66.0% | 0% | 0 | 0 | free; total vs fresh Sybils, nothing vs pre-aged |
| 3 | server reply seniority (srv100) | 65.8% | 0% | 0 | 0 | as est_hrd but needs ~all servers upgraded (50% of servers: +2 pp) |
| 4 | K_store=20 (k20) | 62.7% | 3% | 0 | +9.8 | publisher-only, cheap-ish, weak vs pre-aged |
| 5 | disjoint paths d=3 / d=4 | 79.0% / 89.5% | 4% / 16% | +15 / +21 | +24 / +38 | the only thing that beats diluted pre-aged Sybils; useless alone vs tight m >= 20 |
| 6 | k20 + searcher to 2K | 73.9% | 3% | +10.6 | +9.8 | searcher half alone (s2k) is the worst value: +8.8 pp for +10.6 /find |
| 7 | several keys r=3 / r=2 | 59.0% / 55.4% | 0% | 0 | +34 / +17 | alone useless if the attacker covers each key; strong combined with density |
| 8 | established set from the routing table (est_tab) | 49.8% | 1% | 0 | +2.7 | publisher's and searcher's long-known nodes near a key rarely coincide beyond S~300 |

Combinations (same set): density + disjoint d=3 98.2% (worst 88%) but +15 /find per search always; density + r=3
keys 99.0% (worst 89%) for +0.35 /find per search and +37 msgs per publish; density + est_hrd 90.6%.

### Recommended defaults

**Escalation keeps search cost flat.** The searcher runs the density-checked lookup and switches to disjoint
paths only when it found no provider; the publisher (once per republish) pays for disjoint paths up front. From
`fb_a3`/`fb_a1` (S in {1000, 10000}, m in {20, 50}, six variants):

| node | publisher | searcher | mean | worst | +/find per search (no attack) | +msgs per publish (no attack) |
|---|---|---|---|---|---|---|
| fast (alpha=3) | dens + disjoint d=3 | dens; on a miss dens + disjoint d=3 (d=4: +0.3 pp) | 98.9% | 93.0% | +0.36 | +24.7 |
| fast, cheaper | dens + disjoint d=2 | dens; on a miss dens + d=2 | 97.3% | 88.5% | +0.34 | +11.6 |
| slow (alpha=1) | dens + disjoint d=2 | dens; on a miss dens + d=2 | 90.8% | 59.2% | +0.35 | +8.4 |
| slow, if publish airtime allows | dens + disjoint d=3 | dens; on a miss dens + d=3 | 94.7% | 76.5% | +0.35 | +18.6 |
| reference: baseline alpha=3 / alpha=1 | | | 41.8% / 34.6% | 0% / 0% | | |

Add the announce first-seen log (est_hrd) if it is cheap to keep: it costs nothing without an attack and
makes fresh Sybils irrelevant even to *old* searchers when the publisher is new (S=10000 m=20 tight/fresh: new
publisher + old searcher 88% with est_hrd vs 61% with dens alone), and it still works at h=0.5 (88-90%). The
escalation hurts only searches for content nobody provides: they pay the fallback, about 2.5-3x a normal
lookup (S=10000: 8.1 + 24.3 /find at alpha=3; 5.0 + 14.4 at alpha=1). On slow nodes, cap or pace the
fallback like publish lookups.

**Parameters**
* Density threshold **T = 2** with the **pooled size estimate** (median d_K over the last ~16 lookups for other
  keys): 4% false positives, which cost +0.3 /find per search and +0.9 msgs per publish on average. T=3 gives
  0.3% false positives but loses 6 pp under attack (79.1% vs 84.9% in the sweep), because attackers can then
  pack ~2.5x density undetected. Fire only on ranks j >= K. Estimating from the node's own neighbourhood alone
  is 2-4x noisier (12% false positives at T=2).
* Widening: two rings, the nodes after the dense core's last rank and the nodes beyond rho = K*2^128/(T*N_est);
  the publisher stores to K per ring, the searcher needs **5** ring nodes per ring to answer (3: -3 pp, 8:
  +0.8 pp for +0.15 /find). Rank-only ring: -11 pp; radius-only: -6 pp.
* Disjoint paths **d = 3** (fast) / **d = 2** (slow) for publishing and for the fallback; d=4 adds <1 pp.
* Records under several keys: **r = 1** by default. r=2 with density (`Pdm2|Qdm2>d3`: 98.6%, +18.8 per publish)
  is about as good as publishing with d=3 disjoint paths; it is a reasonable alternative but doubles stored
  records network-wide. Against an attacker who splits a fixed budget, r=2-3 plus density reaches 89-100%.
* K_store: keep **10**. 20 or 30 adds 10-20 provides per publish for less than density gives at +0.9; s2k/s30
  (searcher to 2K/3K) are the least efficient of all.
* alpha/beta unchanged.

### Partial upgrade
Local defences need the **publisher** upgraded most. With a new publisher and an old searcher, fresh tight Sybils
are beaten 60-80% of the time (fast, escalation defaults; 88-98% with est_hrd), pre-aged ones only 14-25%,
since old searchers only stumble into ring nodes on the way; an old publisher with a new searcher gets ~0% vs tight
Sybils (its records sit on Sybils only), but 75% vs evasive ones. With independent 50% upgrade of publishers and
searchers: 28-72% (vs 0-48% baseline) at S=10000, m=50. Popular content has many publishers (`roots` strategy),
so a key is safe once *any* of its publishers is upgraded and the searcher is; old searchers stay exposed until
they upgrade. Server-side reply seniority is the one defence that protects old searchers without them
upgrading, but only against fresh Sybils and only once nearly all servers run it (50% of servers: +2-8 pp).

### Caveats
* Static, churn-free, loss-free network; every honest server answers; one publish round (republishing to
  shifting closest sets would leave extra honest copies, helping all defences). Sybils discard stores, answer
  with only Sybils and never with fake providers; a smarter attacker could answer honestly about other keys,
  return far honest nodes, or attack the size estimator (it would need Sybils near many random keys).
* Attackers can still: dilute pre-aged Sybils below T and keep the escalated search the only thing protecting
  content (worst fast-default cell 93%, slow 77%); spread fewer than K tight Sybils per key, which the
  rank >= K check misses about half the time (split r=3, m=20: dens 50-57%, disjoint paths cover it); or be
  patient (pre-aged) against seniority defences.
* Tables are random K-subsets per bucket (every announce heard in random order); with h=0.5 lookups cost
  ~7% more and est_hrd's extra stores grow (+5 provides), conclusions unchanged.
* The "50% upgraded" figure assumes independent upgrade of one publisher and one searcher.
* Cost counts /find and /provide requests only, not path requests, link setups or bytes (a widened /provide is
  one more link each).


## Summary over attacks, alpha=3: S in {1000, 10000}, m in {10, 20, 50}, every attack variant run at this alpha (alpha=1 grid: tight fresh, tight pre-aged, evasive pre-aged)

Cost of a defence = extra messages vs baseline. 'no attack' is what every lookup pays always; the efficiency column weights publish messages by 1/10 (a key is looked up roughly as often as it is republished; change the weight to taste).

| defence | mean P(find) % | worst % | gain pp | +/find per search, no attack | +msgs per publish, no attack | +/find per search, attacked | +msgs per publish, attacked | gain pp per extra msg (search + publish/10, no attack) |
|---|---|---|---|---|---|---|---|---|
| dens+multi3 | 99.0 | 89.3 | +52.5 | +0.35 | +36.71 | +11.0 | +102.1 | 13.1 |
| dens+est_hrd+disj3 | 98.3 | 87.8 | +51.7 | +15.02 | +24.72 | +27.7 | +55.2 | 3.0 |
| dens+disj3 | 98.2 | 88.2 | +51.7 | +15.02 | +24.72 | +26.9 | +50.4 | 3.0 |
| dens+est_hrd | 90.6 | 52.0 | +44.1 | +0.34 | +0.94 | +9.7 | +24.6 | 100.5 |
| srv100+dens | 90.4 | 51.7 | +43.8 | +0.35 | +0.92 | +4.5 | +11.0 | 99.9 |
| dens | 90.2 | 52.2 | +43.7 | +0.33 | +0.85 | +9.2 | +22.3 | 104.7 |
| disj4 | 89.5 | 16.2 | +42.9 | +21.40 | +38.19 | +23.5 | +39.1 | 1.7 |
| est_hrd+disj3 | 85.3 | 3.6 | +38.7 | +14.92 | +24.46 | +18.3 | +35.6 | 2.2 |
| dens+k20+s2k | 81.8 | 21.4 | +35.2 | +10.55 | +10.25 | +11.2 | +29.5 | 3.0 |
| disj3 | 79.0 | 3.7 | +32.4 | +14.93 | +24.45 | +16.3 | +25.4 | 1.9 |
| k20+s2k | 73.9 | 3.2 | +27.3 | +10.56 | +9.76 | +10.2 | +9.9 | 2.4 |
| disj2 | 66.0 | 0.3 | +19.5 | +7.41 | +11.22 | +8.0 | +12.1 | 2.3 |
| srv100+est_hrd | 66.0 | 0.0 | +19.5 | +0.00 | -0.01 | -0.5 | -0.5 | free |
| est_hrd | 66.0 | 0.0 | +19.4 | -0.00 | -0.02 | +0.7 | +4.2 | free |
| srv100 | 65.8 | 0.0 | +19.2 | -0.00 | -0.00 | -0.5 | -0.5 | free |
| k20 | 62.7 | 2.6 | +16.1 | +0.00 | +9.76 | +0.0 | +9.9 | 16.5 |
| multi3 | 59.0 | 0.0 | +12.4 | -0.00 | +34.02 | +7.5 | +35.1 | 3.6 |
| s2k | 55.4 | 0.0 | +8.8 | +10.56 | +0.00 | +10.2 | +0.0 | 0.8 |
| multi2 | 55.4 | 0.0 | +8.8 | -0.00 | +17.03 | +4.1 | +17.5 | 5.2 |
| est_tab | 49.8 | 0.7 | +3.2 | +0.00 | +2.67 | +0.0 | +2.8 | 12.0 |
| srv50 | 49.0 | 0.0 | +2.4 | -0.00 | -0.01 | +0.2 | +0.2 | free |
| base | 46.6 | 0.0 | +0.0 | +0.00 | +0.00 | +0.0 | +0.0 | free |

Small networks (S=100/300):

| defence | mean P(find) % | worst % | gain pp | +/find per search, no attack | +msgs per publish, no attack | +/find per search, attacked | +msgs per publish, attacked | gain pp per extra msg (search + publish/10, no attack) |
|---|---|---|---|---|---|---|---|---|
| dens+multi3 | 97.5 | 72.5 | +51.0 | +0.31 | +32.30 | +10.5 | +87.8 | 14.4 |
| dens+est_hrd+disj3 | 92.9 | 39.5 | +46.3 | +10.70 | +18.04 | +19.8 | +40.6 | 3.7 |
| dens+disj3 | 92.2 | 39.3 | +45.6 | +10.69 | +18.05 | +19.1 | +37.0 | 3.7 |
| disj4 | 86.8 | 9.6 | +40.3 | +14.28 | +25.50 | +17.6 | +31.5 | 2.4 |
| dens+est_hrd | 86.1 | 37.9 | +39.5 | +0.32 | +0.78 | +8.2 | +20.7 | 99.7 |
| srv100+dens | 84.4 | 38.2 | +37.8 | +0.31 | +0.78 | +3.6 | +8.9 | 98.0 |
| dens | 84.2 | 38.1 | +37.6 | +0.31 | +0.75 | +7.7 | +18.6 | 97.3 |
| est_hrd+disj3 | 83.7 | 3.5 | +37.2 | +10.65 | +18.02 | +14.3 | +29.1 | 3.0 |
| disj3 | 77.0 | 3.6 | +30.5 | +10.65 | +18.01 | +12.7 | +21.3 | 2.4 |
| dens+k20+s2k | 76.8 | 2.0 | +30.2 | +8.93 | +7.96 | +9.8 | +25.4 | 3.1 |
| k20+s2k | 69.7 | 1.0 | +23.2 | +8.95 | +7.48 | +9.4 | +9.4 | 2.4 |
| srv100+est_hrd | 66.3 | 0.1 | +19.8 | -0.00 | +0.00 | -0.4 | -0.1 | free |
| est_hrd | 66.3 | 0.1 | +19.7 | -0.00 | +0.00 | +0.7 | +4.1 | free |
| disj2 | 65.3 | 0.8 | +18.8 | +5.29 | +9.01 | +6.3 | +10.3 | 3.0 |
| srv100 | 63.7 | 0.1 | +17.2 | -0.00 | -0.00 | -0.5 | -0.5 | free |
| k20 | 61.2 | 0.9 | +14.6 | +0.00 | +7.48 | +0.0 | +9.4 | 19.5 |
| multi3 | 59.9 | 0.4 | +13.3 | -0.00 | +30.19 | +5.9 | +31.9 | 4.4 |
| est_tab | 58.2 | 0.3 | +11.6 | -0.00 | +1.14 | +0.1 | +2.1 | 103.0 |
| multi2 | 56.0 | 0.2 | +9.5 | -0.00 | +15.09 | +3.2 | +15.9 | 6.3 |
| s2k | 54.0 | 0.1 | +7.4 | +8.95 | +0.00 | +9.4 | +0.0 | 0.8 |
| srv50 | 48.7 | 0.1 | +2.1 | -0.00 | +0.01 | +0.2 | +0.2 | free |
| base | 46.6 | 0.1 | +0.0 | +0.00 | +0.00 | +0.0 | +0.0 | free |

Pre-aged attacks only (the hard case), S in {1000, 10000}:

| defence | mean P(find) % | worst % | gain pp | +/find per search, no attack | +msgs per publish, no attack | +/find per search, attacked | +msgs per publish, attacked | gain pp per extra msg (search + publish/10, no attack) |
|---|---|---|---|---|---|---|---|---|
| dens+multi3 | 98.0 | 89.3 | +65.9 | +0.35 | +36.71 | +12.9 | +100.5 | 16.4 |
| dens+est_hrd+disj3 | 96.6 | 87.8 | +64.5 | +15.02 | +24.72 | +25.9 | +49.0 | 3.7 |
| dens+disj3 | 96.6 | 88.2 | +64.5 | +15.02 | +24.72 | +25.9 | +49.0 | 3.7 |
| disj4 | 82.9 | 16.2 | +50.8 | +21.40 | +38.19 | +21.7 | +38.2 | 2.0 |
| dens+est_hrd | 81.3 | 52.0 | +49.2 | +0.34 | +0.94 | +9.5 | +22.0 | 112.1 |
| dens | 81.3 | 52.2 | +49.2 | +0.33 | +0.85 | +9.5 | +22.0 | 117.9 |
| srv100+dens | 81.1 | 51.7 | +49.0 | +0.35 | +0.92 | +9.5 | +22.0 | 111.7 |
| est_hrd+disj3 | 70.6 | 3.6 | +38.5 | +14.92 | +24.46 | +15.1 | +24.9 | 2.2 |
| disj3 | 70.5 | 3.7 | +38.4 | +14.93 | +24.45 | +15.1 | +25.0 | 2.2 |
| dens+k20+s2k | 69.5 | 21.4 | +37.4 | +10.55 | +10.25 | +11.9 | +29.5 | 3.2 |
| k20+s2k | 58.7 | 3.2 | +26.5 | +10.56 | +9.76 | +10.8 | +9.9 | 2.3 |
| disj2 | 55.4 | 0.3 | +23.3 | +7.41 | +11.22 | +7.5 | +11.9 | 2.7 |
| multi3 | 51.1 | 0.0 | +19.0 | -0.00 | +34.02 | +8.7 | +34.3 | 5.6 |
| s2k | 46.2 | 0.0 | +14.0 | +10.56 | +0.00 | +10.8 | +0.0 | 1.3 |
| multi2 | 45.0 | 0.0 | +12.9 | -0.00 | +17.03 | +4.8 | +17.1 | 7.6 |
| k20 | 38.9 | 2.6 | +6.7 | +0.00 | +9.76 | +0.0 | +9.9 | 6.9 |
| est_tab | 35.0 | 0.7 | +2.9 | +0.00 | +2.67 | +0.0 | +2.7 | 10.9 |
| srv50 | 32.2 | 0.0 | +0.1 | -0.00 | -0.01 | -0.0 | -0.0 | free |
| base | 32.1 | 0.0 | +0.0 | +0.00 | +0.00 | +0.0 | +0.0 | free |
| srv100+est_hrd | 32.1 | 0.0 | -0.1 | +0.00 | -0.01 | -0.0 | -0.0 | free |
| srv100 | 32.0 | 0.0 | -0.1 | -0.00 | -0.00 | -0.0 | -0.0 | free |
| est_hrd | 32.0 | 0.0 | -0.1 | -0.00 | -0.02 | -0.0 | -0.0 | free |


## Partial upgrade (old = 0.1.x behaviour)

Local defences only change what the upgraded publisher/searcher does, so the mix is exact: with a fraction u of nodes upgraded, P = (1-u)^2 P(old,old) + u(1-u)(P(old,new) + P(new,old)) + u^2 P(new,new). The 50% column is that with u = 0.5.

**S=10000, m=20**

| defence | variant | old pub, old srch (=base) | old pub, new srch | new pub, old srch | new pub, new srch | 50% upgraded |
|---|---|---|---|---|---|---|
| dens | tight/fresh | 0.1 | 0.3 | 60.9 | 99.8 | 40.3 |
| dens | tight/aged | 0.0 | 0.2 | 25.6 | 99.8 | 31.4 |
| dens | evas/aged | 59.5 | 61.1 | 61.6 | 63.4 | 61.4 |
| est_hrd | tight/fresh | 0.1 | 0.1 | 88.4 | 100.0 | 47.2 |
| est_hrd | tight/aged | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| est_hrd | evas/aged | 59.5 | 59.5 | 60.1 | 60.1 | 59.8 |
| disj3 | tight/fresh | 0.1 | 0.2 | 37.0 | 66.3 | 25.9 |
| disj3 | tight/aged | 0.0 | 0.1 | 8.0 | 24.0 | 8.0 |
| disj3 | evas/aged | 59.5 | 81.9 | 75.4 | 98.8 | 78.9 |
| k20+s2k | tight/fresh | 0.1 | 0.1 | 71.7 | 75.4 | 36.8 |
| k20+s2k | tight/aged | 0.0 | 0.0 | 10.4 | 22.1 | 8.1 |
| k20+s2k | evas/aged | 59.5 | 79.9 | 62.6 | 82.6 | 71.2 |
| multi3 | tight/fresh | 0.1 | 0.1 | 0.1 | 0.1 | 0.1 |
| multi3 | tight/aged | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| multi3 | evas/aged | 59.5 | 59.2 | 60.4 | 93.3 | 68.1 |
| dens+est_hrd | tight/fresh | 0.1 | 0.3 | 97.8 | 100.0 | 49.5 |
| dens+est_hrd | tight/aged | 0.0 | 0.2 | 25.5 | 99.8 | 31.4 |
| dens+est_hrd | evas/aged | 59.5 | 61.4 | 61.2 | 63.4 | 61.4 |
| dens+disj3 | tight/fresh | 0.1 | 0.5 | 82.1 | 99.9 | 45.6 |
| dens+disj3 | tight/aged | 0.0 | 0.3 | 34.6 | 97.8 | 33.2 |
| dens+disj3 | evas/aged | 59.5 | 82.2 | 76.0 | 99.2 | 79.2 |
| dens+multi3 | tight/fresh | 0.1 | 0.3 | 61.3 | 100.0 | 40.4 |
| dens+multi3 | tight/aged | 0.0 | 0.2 | 25.9 | 100.0 | 31.5 |
| dens+multi3 | evas/aged | 59.5 | 61.1 | 61.4 | 95.1 | 69.3 |
| dens+k20+s2k | tight/fresh | 0.1 | 0.1 | 78.3 | 82.7 | 40.3 |
| dens+k20+s2k | tight/aged | 0.0 | 0.1 | 25.9 | 44.0 | 17.5 |
| dens+k20+s2k | evas/aged | 59.5 | 79.8 | 63.1 | 82.3 | 71.2 |
| dens+est_hrd+disj3 | tight/fresh | 0.1 | 0.5 | 99.7 | 100.0 | 50.1 |
| dens+est_hrd+disj3 | tight/aged | 0.0 | 0.4 | 34.8 | 97.9 | 33.2 |
| dens+est_hrd+disj3 | evas/aged | 59.5 | 82.2 | 75.9 | 99.2 | 79.2 |

**S=1000, m=50**

| defence | variant | old pub, old srch (=base) | old pub, new srch | new pub, old srch | new pub, new srch | 50% upgraded |
|---|---|---|---|---|---|---|
| dens | tight/fresh | 0.4 | 1.8 | 61.2 | 98.8 | 40.6 |
| dens | tight/aged | 0.1 | 1.8 | 17.9 | 94.4 | 28.5 |
| dens | evas/aged | 55.4 | 57.3 | 56.8 | 60.1 | 57.4 |
| est_hrd | tight/fresh | 0.4 | 0.4 | 91.6 | 99.9 | 48.0 |
| est_hrd | tight/aged | 0.1 | 0.1 | 0.1 | 0.1 | 0.1 |
| est_hrd | evas/aged | 55.4 | 55.1 | 54.8 | 54.4 | 54.9 |
| disj3 | tight/fresh | 0.4 | 1.4 | 12.4 | 23.8 | 9.5 |
| disj3 | tight/aged | 0.1 | 0.7 | 1.7 | 3.7 | 1.5 |
| disj3 | evas/aged | 55.4 | 78.4 | 71.4 | 96.6 | 75.4 |
| k20+s2k | tight/fresh | 0.4 | 0.3 | 31.2 | 31.7 | 15.9 |
| k20+s2k | tight/aged | 0.1 | 0.2 | 2.7 | 3.2 | 1.5 |
| k20+s2k | evas/aged | 55.4 | 74.0 | 57.1 | 74.6 | 65.3 |
| multi3 | tight/fresh | 0.4 | 0.3 | 0.4 | 1.1 | 0.5 |
| multi3 | tight/aged | 0.1 | 0.1 | 0.1 | 0.4 | 0.2 |
| multi3 | evas/aged | 55.4 | 54.9 | 55.6 | 89.3 | 63.8 |
| dens+est_hrd | tight/fresh | 0.4 | 1.9 | 97.2 | 99.9 | 49.8 |
| dens+est_hrd | tight/aged | 0.1 | 1.6 | 18.1 | 94.2 | 28.5 |
| dens+est_hrd | evas/aged | 55.4 | 57.3 | 57.1 | 60.1 | 57.5 |
| dens+disj3 | tight/fresh | 0.4 | 3.9 | 79.3 | 99.1 | 45.7 |
| dens+disj3 | tight/aged | 0.1 | 3.2 | 30.8 | 95.6 | 32.4 |
| dens+disj3 | evas/aged | 55.4 | 78.7 | 71.5 | 96.7 | 75.6 |
| dens+multi3 | tight/fresh | 0.4 | 1.8 | 61.3 | 100.0 | 40.9 |
| dens+multi3 | tight/aged | 0.1 | 1.7 | 18.0 | 99.9 | 29.9 |
| dens+multi3 | evas/aged | 55.4 | 57.5 | 56.8 | 92.9 | 65.7 |
| dens+k20+s2k | tight/fresh | 0.4 | 0.4 | 62.5 | 65.4 | 32.2 |
| dens+k20+s2k | tight/aged | 0.1 | 0.2 | 17.8 | 21.4 | 9.9 |
| dens+k20+s2k | evas/aged | 55.4 | 73.8 | 58.2 | 75.5 | 65.8 |
| dens+est_hrd+disj3 | tight/fresh | 0.4 | 4.1 | 98.4 | 99.9 | 50.7 |
| dens+est_hrd+disj3 | tight/aged | 0.1 | 3.3 | 31.3 | 95.6 | 32.6 |
| dens+est_hrd+disj3 | evas/aged | 55.4 | 78.7 | 71.8 | 96.9 | 75.7 |

Server-side reply seniority (srvX: X% of servers leave recently heard nodes out of /find replies; helps old clients too):

| S | m | variant | 0% | 50% | 100% |
|---|---|---|---|---|---|
| 1000 | 20 | tight/fresh | 0.5 | 8.5 | 97.8 |
| 1000 | 20 | tight/aged | 0.2 | 0.2 | 0.2 |
| 1000 | 20 | ring/fresh | 89.8 | 94.7 | 99.7 |
| 1000 | 20 | ring/aged | 34.1 | 34.6 | 34.0 |
| 1000 | 20 | evas/fresh | 97.7 | 98.8 | 99.9 |
| 1000 | 20 | evas/aged | 68.5 | 68.7 | 68.1 |
| 1000 | 50 | tight/fresh | 0.4 | 8.0 | 97.3 |
| 1000 | 50 | tight/aged | 0.1 | 0.1 | 0.1 |
| 1000 | 50 | ring/fresh | 67.4 | 76.1 | 99.5 |
| 1000 | 50 | ring/aged | 6.2 | 6.2 | 6.4 |
| 1000 | 50 | evas/fresh | 97.4 | 98.6 | 99.9 |
| 1000 | 50 | evas/aged | 55.4 | 54.3 | 54.7 |
| 10000 | 20 | tight/fresh | 0.1 | 7.5 | 99.7 |
| 10000 | 20 | tight/aged | 0.0 | 0.0 | 0.0 |
| 10000 | 20 | ring/fresh | 89.3 | 94.5 | 100.0 |
| 10000 | 20 | ring/aged | 27.4 | 27.9 | 27.7 |
| 10000 | 20 | evas/fresh | 97.9 | 99.0 | 100.0 |
| 10000 | 20 | evas/aged | 59.5 | 60.7 | 59.9 |
| 10000 | 50 | tight/fresh | 0.1 | 8.6 | 99.8 |
| 10000 | 50 | tight/aged | 0.0 | 0.0 | 0.0 |
| 10000 | 50 | ring/fresh | 67.1 | 76.2 | 100.0 |
| 10000 | 50 | ring/aged | 4.8 | 4.9 | 5.1 |
| 10000 | 50 | evas/fresh | 97.8 | 98.8 | 100.0 |
| 10000 | 50 | evas/aged | 48.9 | 49.3 | 48.1 |


## Main grid, alpha=3 (main_a3.jsonl)

### Grinding cost

| S | placement | expected key generations per Sybil | seconds per Sybil at 19k/s/core (measured) |
|---|---|---|---|
| 100 | tight | 949 | 0.0495 |
| 100 | ring | 11 | 0.00058 |
| 100 | evasive | 91 | 0.00475 |
| 300 | tight | 6,607 | 0.345 |
| 300 | ring | 33 | 0.00171 |
| 300 | evasive | 247 | 0.0129 |
| 1000 | tight | 6,757 | 0.353 |
| 1000 | ring | 113 | 0.00591 |
| 1000 | evasive | 1,190 | 0.0621 |
| 10000 | tight | 70,232 | 3.67 |
| 10000 | ring | 1,114 | 0.0582 |
| 10000 | evasive | 12,141 | 0.634 |

### P(find) %, both publisher and searcher upgraded (±95% CI)

**S=100, no attack:** base 100.0, dens 100.0, est_tab 100.0, est_hrd 100.0, disj2 100.0, disj3 100.0, disj4 100.0, k20 100.0, s2k 100.0, k20+s2k 100.0, multi2 100.0, multi3 100.0, dens+est_hrd 100.0, dens+disj3 100.0, dens+multi3 100.0, dens+k20+s2k 100.0, est_hrd+disj3 100.0, dens+est_hrd+disj3 100.0, srv50 100.0, srv100 100.0, srv100+dens 100.0, srv100+est_hrd 100.0

**S=100, m=10**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 3.7 ±0.5 | 1.9 ±0.3 | 94.9 ±0.6 | 65.5 ±1.4 | 96.3 ±0.5 | 74.0 ±1.3 |
| dens | 87.6 ±0.7 | 80.7 ±1.0 | 97.7 ±0.3 | 77.5 ±1.1 | 98.1 ±0.3 | 80.1 ±1.1 |
| est_tab | 61.1 ±1.1 | 5.9 ±0.4 | 98.2 ±0.3 | 71.7 ±1.3 | 98.8 ±0.2 | 78.8 ±1.2 |
| est_hrd | 98.5 ±0.2 | 1.9 ±0.2 | 100.0 ±0.0 | 65.7 ±1.4 | 100.0 ±0.0 | 73.5 ±1.3 |
| disj2 | 81.0 ±0.8 | 42.7 ±0.9 | 99.6 ±0.1 | 93.5 ±0.7 | 99.6 ±0.1 | 95.3 ±0.6 |
| disj3 | 95.2 ±0.4 | 73.3 ±0.9 | 100.0 ±0.0 | 99.0 ±0.3 | 99.9 ±0.0 | 99.4 ±0.2 |
| disj4 | 98.1 ±0.2 | 95.4 ±0.4 | 100.0 ±0.0 | 99.9 ±0.1 | 100.0 ±0.0 | 99.8 ±0.1 |
| k20 | 80.4 ±0.7 | 21.4 ±0.8 | 97.6 ±0.3 | 67.5 ±1.3 | 98.2 ±0.3 | 75.1 ±1.3 |
| s2k | 5.7 ±0.7 | 12.0 ±0.9 | 99.8 ±0.1 | 82.4 ±1.3 | 99.7 ±0.2 | 85.7 ±1.2 |
| k20+s2k | 91.8 ±0.5 | 60.7 ±1.2 | 99.9 ±0.0 | 83.3 ±1.2 | 99.9 ±0.1 | 86.4 ±1.1 |
| multi2 | 6.9 ±0.6 | 3.9 ±0.3 | 99.5 ±0.2 | 86.7 ±0.9 | 99.7 ±0.1 | 91.7 ±0.8 |
| multi3 | 10.1 ±0.7 | 5.5 ±0.4 | 100.0 ±0.0 | 94.0 ±0.6 | 100.0 ±0.0 | 97.2 ±0.5 |
| dens+est_hrd | 98.5 ±0.2 | 80.4 ±1.0 | 100.0 ±0.0 | 77.2 ±1.2 | 100.0 ±0.0 | 80.0 ±1.2 |
| dens+disj3 | 96.1 ±0.3 | 84.8 ±0.8 | 100.0 ±0.0 | 99.3 ±0.2 | 100.0 ±0.0 | 99.5 ±0.2 |
| dens+multi3 | 99.8 ±0.1 | 99.1 ±0.2 | 100.0 ±0.0 | 98.3 ±0.3 | 100.0 ±0.0 | 98.9 ±0.2 |
| dens+k20+s2k | 94.1 ±0.4 | 78.4 ±0.9 | 100.0 ±0.0 | 86.8 ±1.0 | 100.0 ±0.0 | 88.0 ±1.0 |
| est_hrd+disj3 | 98.6 ±0.2 | 72.8 ±0.9 | 100.0 ±0.0 | 98.8 ±0.3 | 100.0 ±0.0 | 99.3 ±0.2 |
| dens+est_hrd+disj3 | 98.6 ±0.2 | 85.2 ±0.7 | 100.0 ±0.0 | 99.2 ±0.2 | 100.0 ±0.0 | 99.5 ±0.2 |
| srv50 | 10.2 ±1.0 | 1.9 ±0.2 | 96.6 ±0.4 | 65.9 ±1.4 | 97.4 ±0.4 | 73.7 ±1.3 |
| srv100 | 81.2 ±1.0 | 2.1 ±0.3 | 98.4 ±0.2 | 65.6 ±1.4 | 98.8 ±0.2 | 73.7 ±1.3 |
| srv100+dens | 88.1 ±0.7 | 80.4 ±1.0 | 98.6 ±0.2 | 77.6 ±1.2 | 99.0 ±0.2 | 80.1 ±1.1 |
| srv100+est_hrd | 98.5 ±0.2 | 1.9 ±0.2 | 100.0 ±0.0 | 65.2 ±1.4 | 100.0 ±0.0 | 73.4 ±1.3 |

**S=100, m=20**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 2.5 ±0.3 | 0.3 ±0.1 | 85.2 ±1.0 | 32.9 ±1.3 | 94.8 ±0.6 | 63.3 ±1.5 |
| dens | 87.2 ±0.8 | 71.7 ±1.1 | 96.4 ±0.4 | 73.8 ±1.2 | 97.9 ±0.3 | 73.4 ±1.3 |
| est_tab | 60.8 ±1.1 | 1.9 ±0.2 | 95.1 ±0.5 | 38.7 ±1.4 | 98.2 ±0.3 | 69.0 ±1.4 |
| est_hrd | 98.6 ±0.2 | 0.3 ±0.1 | 99.8 ±0.1 | 33.1 ±1.3 | 100.0 ±0.0 | 63.2 ±1.5 |
| disj2 | 14.8 ±1.1 | 5.3 ±0.5 | 97.3 ±0.4 | 66.5 ±1.4 | 99.5 ±0.2 | 88.1 ±1.0 |
| disj3 | 60.2 ±1.5 | 24.8 ±0.9 | 99.5 ±0.2 | 86.7 ±1.0 | 99.9 ±0.0 | 96.2 ±0.5 |
| disj4 | 94.4 ±0.5 | 53.9 ±1.1 | 99.8 ±0.1 | 93.6 ±0.7 | 100.0 ±0.0 | 98.3 ±0.3 |
| k20 | 64.1 ±1.0 | 6.8 ±0.4 | 94.7 ±0.5 | 36.3 ±1.3 | 97.6 ±0.3 | 65.3 ±1.4 |
| s2k | 2.9 ±0.4 | 0.5 ±0.1 | 96.4 ±0.7 | 48.2 ±1.5 | 99.4 ±0.3 | 75.7 ±1.4 |
| k20+s2k | 68.5 ±1.0 | 9.7 ±0.5 | 99.3 ±0.2 | 51.0 ±1.4 | 99.9 ±0.1 | 76.3 ±1.3 |
| multi2 | 4.9 ±0.5 | 0.7 ±0.1 | 97.3 ±0.4 | 54.8 ±1.4 | 99.7 ±0.1 | 84.8 ±1.0 |
| multi3 | 6.6 ±0.5 | 1.0 ±0.2 | 99.6 ±0.1 | 69.2 ±1.2 | 100.0 ±0.0 | 93.6 ±0.7 |
| dens+est_hrd | 98.6 ±0.2 | 71.7 ±1.1 | 99.8 ±0.1 | 72.9 ±1.2 | 100.0 ±0.0 | 72.9 ±1.3 |
| dens+disj3 | 92.9 ±0.4 | 71.7 ±1.0 | 99.7 ±0.1 | 91.7 ±0.7 | 100.0 ±0.0 | 96.5 ±0.5 |
| dens+multi3 | 99.7 ±0.1 | 96.6 ±0.4 | 100.0 ±0.0 | 97.3 ±0.4 | 100.0 ±0.0 | 97.2 ±0.4 |
| dens+k20+s2k | 76.8 ±0.8 | 15.3 ±0.6 | 99.5 ±0.2 | 65.3 ±1.1 | 99.9 ±0.1 | 80.1 ±1.2 |
| est_hrd+disj3 | 98.6 ±0.2 | 25.0 ±0.9 | 99.8 ±0.1 | 86.5 ±1.0 | 100.0 ±0.0 | 96.1 ±0.5 |
| dens+est_hrd+disj3 | 98.6 ±0.2 | 72.1 ±1.0 | 99.8 ±0.1 | 91.7 ±0.8 | 100.0 ±0.0 | 96.5 ±0.5 |
| srv50 | 8.0 ±0.9 | 0.3 ±0.1 | 89.6 ±0.8 | 33.4 ±1.3 | 96.7 ±0.4 | 63.5 ±1.5 |
| srv100 | 80.2 ±1.1 | 0.3 ±0.1 | 96.9 ±0.3 | 33.1 ±1.3 | 98.5 ±0.2 | 63.4 ±1.5 |
| srv100+dens | 86.8 ±0.8 | 71.7 ±1.1 | 97.3 ±0.3 | 73.5 ±1.2 | 98.7 ±0.2 | 73.5 ±1.3 |
| srv100+est_hrd | 98.6 ±0.2 | 0.4 ±0.1 | 99.8 ±0.1 | 33.5 ±1.3 | 100.0 ±0.0 | 63.2 ±1.5 |

**S=100, m=50**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 2.6 ±0.4 | 0.1 ±0.1 | 63.8 ±1.5 | 6.8 ±0.6 | 89.8 ±0.8 | 41.9 ±1.4 |
| dens | 87.8 ±0.7 | 41.7 ±1.3 | 94.4 ±0.5 | 38.1 ±1.3 | 97.4 ±0.3 | 63.7 ±1.4 |
| est_tab | 61.0 ±1.1 | 0.3 ±0.1 | 86.5 ±0.8 | 8.7 ±0.7 | 96.4 ±0.4 | 47.1 ±1.5 |
| est_hrd | 98.5 ±0.2 | 0.1 ±0.1 | 99.1 ±0.2 | 6.7 ±0.6 | 100.0 ±0.0 | 41.8 ±1.5 |
| disj2 | 7.6 ±0.7 | 1.0 ±0.2 | 84.9 ±1.2 | 17.5 ±1.0 | 98.4 ±0.3 | 68.1 ±1.4 |
| disj3 | 24.3 ±1.4 | 3.6 ±0.3 | 93.8 ±0.8 | 32.1 ±1.3 | 99.7 ±0.1 | 83.4 ±1.1 |
| disj4 | 60.1 ±1.6 | 9.6 ±0.6 | 97.9 ±0.4 | 45.7 ±1.4 | 99.9 ±0.1 | 90.0 ±0.9 |
| k20 | 18.3 ±1.2 | 0.9 ±0.1 | 87.8 ±0.8 | 8.7 ±0.7 | 95.9 ±0.4 | 44.0 ±1.4 |
| s2k | 2.8 ±0.4 | 0.1 ±0.1 | 76.3 ±1.6 | 11.1 ±0.8 | 98.1 ±0.4 | 56.2 ±1.5 |
| k20+s2k | 19.0 ±1.2 | 1.0 ±0.2 | 93.7 ±0.6 | 12.9 ±0.8 | 99.5 ±0.1 | 57.6 ±1.5 |
| multi2 | 4.9 ±0.5 | 0.2 ±0.1 | 86.6 ±1.0 | 12.8 ±0.8 | 99.2 ±0.2 | 66.4 ±1.3 |
| multi3 | 7.2 ±0.6 | 0.4 ±0.1 | 94.9 ±0.6 | 18.9 ±1.0 | 99.9 ±0.1 | 78.8 ±1.1 |
| dens+est_hrd | 98.5 ±0.2 | 41.5 ±1.3 | 99.1 ±0.2 | 37.9 ±1.2 | 100.0 ±0.0 | 64.6 ±1.3 |
| dens+disj3 | 90.6 ±0.5 | 39.3 ±1.2 | 98.5 ±0.2 | 50.9 ±1.4 | 99.9 ±0.1 | 85.7 ±1.0 |
| dens+multi3 | 99.8 ±0.1 | 77.8 ±1.1 | 100.0 ±0.0 | 72.5 ±1.1 | 100.0 ±0.0 | 93.2 ±0.6 |
| dens+k20+s2k | 58.6 ±1.0 | 2.0 ±0.2 | 96.7 ±0.4 | 25.0 ±1.0 | 99.8 ±0.1 | 65.6 ±1.3 |
| est_hrd+disj3 | 98.5 ±0.2 | 3.6 ±0.3 | 99.1 ±0.1 | 32.4 ±1.3 | 100.0 ±0.0 | 83.4 ±1.1 |
| dens+est_hrd+disj3 | 98.5 ±0.2 | 39.5 ±1.2 | 99.1 ±0.1 | 50.9 ±1.3 | 100.0 ±0.0 | 86.1 ±1.0 |
| srv50 | 8.0 ±0.9 | 0.1 ±0.1 | 70.4 ±1.5 | 6.6 ±0.6 | 93.4 ±0.6 | 41.6 ±1.5 |
| srv100 | 81.0 ±1.0 | 0.1 ±0.1 | 94.1 ±0.6 | 6.5 ±0.6 | 97.4 ±0.3 | 41.8 ±1.5 |
| srv100+dens | 87.7 ±0.8 | 41.3 ±1.3 | 95.7 ±0.4 | 38.2 ±1.2 | 97.9 ±0.3 | 63.3 ±1.4 |
| srv100+est_hrd | 98.5 ±0.2 | 0.1 ±0.1 | 99.1 ±0.2 | 6.6 ±0.6 | 100.0 ±0.0 | 41.4 ±1.5 |

**S=300, no attack:** base 100.0, dens 100.0, est_tab 100.0, est_hrd 100.0, disj2 100.0, disj3 100.0, disj4 100.0, k20 100.0, s2k 100.0, k20+s2k 100.0, multi2 100.0, multi3 100.0, dens+est_hrd 100.0, dens+disj3 100.0, dens+multi3 100.0, dens+k20+s2k 100.0, est_hrd+disj3 100.0, dens+est_hrd+disj3 100.0, srv50 100.0, srv100 100.0, srv100+dens 100.0, srv100+est_hrd 100.0

**S=300, m=10**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 2.2 ±0.3 | 1.4 ±0.2 | 96.7 ±0.4 | 69.8 ±1.3 | 97.7 ±0.4 | 77.7 ±1.2 |
| dens | 94.7 ±0.5 | 93.9 ±0.6 | 98.8 ±0.2 | 80.0 ±1.1 | 98.8 ±0.3 | 81.8 ±1.1 |
| est_tab | 33.5 ±1.1 | 7.6 ±0.5 | 97.9 ±0.3 | 76.7 ±1.2 | 98.5 ±0.3 | 83.1 ±1.1 |
| est_hrd | 99.4 ±0.1 | 1.3 ±0.2 | 100.0 ±0.0 | 69.0 ±1.3 | 100.0 ±0.0 | 77.0 ±1.3 |
| disj2 | 86.6 ±0.6 | 36.2 ±0.8 | 99.9 ±0.1 | 96.8 ±0.5 | 99.9 ±0.1 | 97.1 ±0.5 |
| disj3 | 97.9 ±0.3 | 74.1 ±0.8 | 100.0 ±0.0 | 99.7 ±0.1 | 99.9 ±0.1 | 99.8 ±0.1 |
| disj4 | 99.2 ±0.1 | 98.8 ±0.2 | 100.0 ±0.0 | 99.9 ±0.1 | 100.0 ±0.0 | 100.0 ±0.0 |
| k20 | 86.9 ±0.6 | 31.6 ±0.8 | 98.7 ±0.2 | 73.8 ±1.2 | 99.1 ±0.2 | 79.5 ±1.1 |
| s2k | 3.5 ±0.5 | 8.1 ±0.8 | 99.9 ±0.0 | 89.1 ±1.1 | 99.9 ±0.1 | 92.3 ±0.9 |
| k20+s2k | 95.3 ±0.4 | 73.6 ±1.0 | 100.0 ±0.0 | 91.0 ±0.9 | 100.0 ±0.0 | 92.2 ±0.9 |
| multi2 | 4.6 ±0.5 | 2.8 ±0.3 | 99.9 ±0.1 | 90.1 ±0.8 | 99.9 ±0.1 | 94.3 ±0.6 |
| multi3 | 6.5 ±0.6 | 4.0 ±0.4 | 100.0 ±0.0 | 96.4 ±0.5 | 100.0 ±0.0 | 98.3 ±0.3 |
| dens+est_hrd | 99.3 ±0.1 | 94.0 ±0.6 | 100.0 ±0.0 | 80.1 ±1.1 | 100.0 ±0.0 | 81.6 ±1.1 |
| dens+disj3 | 98.5 ±0.2 | 89.9 ±0.6 | 100.0 ±0.0 | 99.8 ±0.1 | 100.0 ±0.0 | 99.8 ±0.1 |
| dens+multi3 | 100.0 ±0.0 | 99.9 ±0.0 | 100.0 ±0.0 | 98.8 ±0.3 | 100.0 ±0.0 | 99.1 ±0.2 |
| dens+k20+s2k | 97.7 ±0.3 | 89.3 ±0.6 | 100.0 ±0.0 | 91.4 ±0.9 | 100.0 ±0.0 | 92.5 ±0.9 |
| est_hrd+disj3 | 99.4 ±0.1 | 74.2 ±0.8 | 100.0 ±0.0 | 99.6 ±0.2 | 100.0 ±0.0 | 99.7 ±0.1 |
| dens+est_hrd+disj3 | 99.4 ±0.1 | 90.0 ±0.6 | 100.0 ±0.0 | 99.8 ±0.1 | 100.0 ±0.0 | 99.8 ±0.1 |
| srv50 | 10.8 ±1.1 | 1.3 ±0.2 | 98.0 ±0.3 | 69.5 ±1.3 | 98.8 ±0.2 | 78.0 ±1.2 |
| srv100 | 92.7 ±0.7 | 1.4 ±0.2 | 99.5 ±0.1 | 69.6 ±1.3 | 99.8 ±0.1 | 77.3 ±1.2 |
| srv100+dens | 94.5 ±0.6 | 94.2 ±0.6 | 99.6 ±0.1 | 80.0 ±1.1 | 99.7 ±0.1 | 82.3 ±1.1 |
| srv100+est_hrd | 99.4 ±0.1 | 1.4 ±0.2 | 100.0 ±0.0 | 70.0 ±1.3 | 100.0 ±0.0 | 77.2 ±1.2 |

**S=300, m=20**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 1.1 ±0.2 | 0.4 ±0.1 | 87.8 ±0.9 | 35.4 ±1.3 | 97.3 ±0.4 | 66.4 ±1.4 |
| dens | 95.4 ±0.5 | 91.8 ±0.7 | 98.1 ±0.3 | 80.5 ±1.2 | 98.8 ±0.3 | 73.5 ±1.3 |
| est_tab | 33.3 ±1.1 | 3.9 ±0.3 | 92.3 ±0.7 | 41.8 ±1.4 | 98.4 ±0.3 | 73.0 ±1.3 |
| est_hrd | 99.4 ±0.1 | 0.3 ±0.1 | 100.0 ±0.0 | 35.3 ±1.3 | 100.0 ±0.0 | 66.9 ±1.4 |
| disj2 | 11.3 ±1.0 | 3.7 ±0.4 | 98.6 ±0.3 | 74.7 ±1.2 | 99.8 ±0.1 | 92.8 ±0.8 |
| disj3 | 62.3 ±1.6 | 22.8 ±0.8 | 99.8 ±0.1 | 92.1 ±0.7 | 100.0 ±0.0 | 98.6 ±0.3 |
| disj4 | 97.3 ±0.3 | 60.6 ±1.0 | 100.0 ±0.0 | 97.9 ±0.3 | 100.0 ±0.0 | 99.6 ±0.2 |
| k20 | 72.0 ±0.9 | 12.4 ±0.6 | 96.6 ±0.4 | 41.1 ±1.2 | 99.0 ±0.2 | 68.8 ±1.3 |
| s2k | 1.2 ±0.2 | 0.5 ±0.1 | 97.7 ±0.5 | 58.2 ±1.5 | 99.9 ±0.1 | 81.0 ±1.3 |
| k20+s2k | 76.2 ±0.9 | 16.9 ±0.6 | 99.7 ±0.1 | 62.4 ±1.4 | 100.0 ±0.0 | 82.2 ±1.2 |
| multi2 | 2.0 ±0.3 | 0.7 ±0.1 | 98.3 ±0.3 | 57.8 ±1.3 | 99.9 ±0.1 | 87.5 ±1.0 |
| multi3 | 2.9 ±0.3 | 1.0 ±0.2 | 99.7 ±0.1 | 72.1 ±1.2 | 100.0 ±0.0 | 94.6 ±0.6 |
| dens+est_hrd | 99.4 ±0.1 | 91.7 ±0.7 | 100.0 ±0.0 | 79.8 ±1.2 | 100.0 ±0.0 | 73.4 ±1.3 |
| dens+disj3 | 97.4 ±0.3 | 90.8 ±0.6 | 99.9 ±0.0 | 96.7 ±0.4 | 100.0 ±0.0 | 98.9 ±0.3 |
| dens+multi3 | 100.0 ±0.0 | 99.8 ±0.1 | 100.0 ±0.0 | 99.2 ±0.2 | 100.0 ±0.0 | 97.1 ±0.4 |
| dens+k20+s2k | 84.9 ±0.7 | 34.6 ±0.8 | 99.8 ±0.1 | 74.9 ±1.0 | 100.0 ±0.0 | 83.7 ±1.1 |
| est_hrd+disj3 | 99.4 ±0.1 | 23.2 ±0.8 | 100.0 ±0.0 | 92.1 ±0.7 | 100.0 ±0.0 | 98.5 ±0.3 |
| dens+est_hrd+disj3 | 99.4 ±0.1 | 90.9 ±0.6 | 100.0 ±0.0 | 96.6 ±0.4 | 100.0 ±0.0 | 98.8 ±0.3 |
| srv50 | 8.1 ±1.0 | 0.4 ±0.1 | 92.7 ±0.7 | 34.7 ±1.3 | 98.2 ±0.3 | 66.5 ±1.4 |
| srv100 | 92.8 ±0.7 | 0.4 ±0.1 | 99.0 ±0.2 | 35.7 ±1.3 | 99.6 ±0.1 | 66.5 ±1.4 |
| srv100+dens | 95.0 ±0.5 | 91.7 ±0.7 | 99.1 ±0.2 | 80.2 ±1.2 | 99.7 ±0.1 | 73.2 ±1.3 |
| srv100+est_hrd | 99.4 ±0.1 | 0.4 ±0.1 | 100.0 ±0.0 | 35.5 ±1.3 | 100.0 ±0.0 | 66.6 ±1.4 |

**S=300, m=50**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 1.2 ±0.2 | 0.1 ±0.1 | 66.8 ±1.5 | 6.4 ±0.6 | 95.6 ±0.5 | 51.8 ±1.5 |
| dens | 95.8 ±0.4 | 80.3 ±1.1 | 98.2 ±0.3 | 65.3 ±1.2 | 98.8 ±0.2 | 61.2 ±1.4 |
| est_tab | 33.1 ±1.1 | 0.8 ±0.2 | 77.9 ±1.2 | 8.6 ±0.7 | 97.1 ±0.4 | 57.6 ±1.5 |
| est_hrd | 99.4 ±0.1 | 0.1 ±0.1 | 99.7 ±0.1 | 6.2 ±0.6 | 100.0 ±0.0 | 51.5 ±1.5 |
| disj2 | 4.6 ±0.6 | 0.8 ±0.2 | 88.2 ±1.1 | 21.3 ±1.1 | 99.5 ±0.2 | 78.8 ±1.3 |
| disj3 | 22.1 ±1.4 | 3.6 ±0.3 | 96.3 ±0.6 | 41.9 ±1.3 | 100.0 ±0.0 | 91.5 ±0.8 |
| disj4 | 64.3 ±1.6 | 14.7 ±0.7 | 99.0 ±0.3 | 61.0 ±1.3 | 100.0 ±0.0 | 96.7 ±0.5 |
| k20 | 25.8 ±1.4 | 2.2 ±0.2 | 92.0 ±0.6 | 10.0 ±0.6 | 98.6 ±0.2 | 54.0 ±1.4 |
| s2k | 1.3 ±0.2 | 0.1 ±0.1 | 78.8 ±1.5 | 11.8 ±0.9 | 99.6 ±0.2 | 67.7 ±1.5 |
| k20+s2k | 27.0 ±1.4 | 2.6 ±0.3 | 96.1 ±0.5 | 15.9 ±0.9 | 99.9 ±0.0 | 68.9 ±1.4 |
| multi2 | 2.4 ±0.3 | 0.3 ±0.1 | 89.0 ±0.9 | 12.3 ±0.8 | 99.8 ±0.1 | 75.4 ±1.3 |
| multi3 | 3.1 ±0.4 | 0.4 ±0.1 | 96.2 ±0.5 | 17.1 ±0.9 | 100.0 ±0.0 | 87.1 ±0.9 |
| dens+est_hrd | 99.4 ±0.1 | 80.6 ±1.0 | 99.6 ±0.1 | 65.1 ±1.2 | 100.0 ±0.0 | 61.3 ±1.4 |
| dens+disj3 | 96.6 ±0.3 | 82.4 ±0.9 | 99.4 ±0.1 | 78.2 ±1.1 | 100.0 ±0.0 | 92.8 ±0.7 |
| dens+multi3 | 100.0 ±0.0 | 98.3 ±0.3 | 100.0 ±0.0 | 94.2 ±0.5 | 100.0 ±0.0 | 93.9 ±0.6 |
| dens+k20+s2k | 68.6 ±0.9 | 11.6 ±0.5 | 98.4 ±0.3 | 34.2 ±1.0 | 100.0 ±0.0 | 71.0 ±1.4 |
| est_hrd+disj3 | 99.5 ±0.1 | 3.5 ±0.3 | 99.7 ±0.1 | 42.1 ±1.3 | 100.0 ±0.0 | 91.1 ±0.8 |
| dens+est_hrd+disj3 | 99.5 ±0.1 | 82.4 ±0.9 | 99.7 ±0.1 | 78.1 ±1.1 | 100.0 ±0.0 | 93.2 ±0.7 |
| srv50 | 8.0 ±1.0 | 0.1 ±0.1 | 75.1 ±1.5 | 6.5 ±0.6 | 97.3 ±0.4 | 51.8 ±1.5 |
| srv100 | 92.1 ±0.8 | 0.1 ±0.1 | 98.0 ±0.3 | 6.4 ±0.6 | 99.5 ±0.1 | 51.3 ±1.5 |
| srv100+dens | 94.4 ±0.6 | 80.2 ±1.1 | 98.4 ±0.2 | 64.8 ±1.2 | 99.6 ±0.1 | 61.6 ±1.4 |
| srv100+est_hrd | 99.4 ±0.1 | 0.1 ±0.1 | 99.7 ±0.1 | 6.3 ±0.6 | 100.0 ±0.0 | 52.1 ±1.5 |

**S=1000, no attack:** base 100.0, dens 100.0, est_tab 100.0, est_hrd 100.0, disj2 100.0, disj3 100.0, disj4 100.0, k20 100.0, s2k 100.0, k20+s2k 100.0, multi2 100.0, multi3 100.0, dens+est_hrd 100.0, dens+disj3 100.0, dens+multi3 100.0, dens+k20+s2k 100.0, est_hrd+disj3 100.0, dens+est_hrd+disj3 100.0, srv50 100.0, srv100 100.0, srv100+dens 100.0, srv100+est_hrd 100.0

**S=1000, m=10**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 1.4 ±0.3 | 0.7 ±0.1 | 97.0 ±0.4 | 67.8 ±1.4 | 98.1 ±0.3 | 76.3 ±1.3 |
| dens | 98.3 ±0.4 | 98.0 ±0.4 | 98.9 ±0.2 | 77.4 ±1.2 | 99.0 ±0.2 | 80.1 ±1.1 |
| est_tab | 16.5 ±0.9 | 5.6 ±0.4 | 97.7 ±0.3 | 72.6 ±1.3 | 98.5 ±0.3 | 81.0 ±1.1 |
| est_hrd | 99.8 ±0.1 | 0.7 ±0.1 | 100.0 ±0.0 | 66.6 ±1.4 | 100.0 ±0.0 | 75.8 ±1.3 |
| disj2 | 87.8 ±0.6 | 34.1 ±0.8 | 99.9 ±0.0 | 95.7 ±0.5 | 99.9 ±0.1 | 97.5 ±0.4 |
| disj3 | 99.3 ±0.1 | 78.0 ±0.7 | 100.0 ±0.0 | 99.7 ±0.1 | 100.0 ±0.0 | 99.8 ±0.1 |
| disj4 | 99.8 ±0.1 | 99.6 ±0.1 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 |
| k20 | 89.0 ±0.6 | 29.2 ±0.8 | 99.0 ±0.2 | 71.6 ±1.2 | 99.3 ±0.2 | 79.1 ±1.1 |
| s2k | 2.3 ±0.5 | 6.0 ±0.8 | 100.0 ±0.0 | 91.3 ±1.0 | 100.0 ±0.1 | 93.9 ±0.8 |
| k20+s2k | 96.3 ±0.3 | 78.5 ±0.9 | 100.0 ±0.0 | 93.0 ±0.8 | 100.0 ±0.0 | 94.6 ±0.7 |
| multi2 | 2.5 ±0.4 | 1.4 ±0.2 | 99.9 ±0.1 | 89.0 ±0.8 | 100.0 ±0.0 | 93.9 ±0.6 |
| multi3 | 3.8 ±0.5 | 2.3 ±0.3 | 100.0 ±0.0 | 96.0 ±0.5 | 100.0 ±0.0 | 98.5 ±0.3 |
| dens+est_hrd | 99.8 ±0.1 | 98.0 ±0.4 | 100.0 ±0.0 | 77.2 ±1.2 | 100.0 ±0.0 | 80.5 ±1.1 |
| dens+disj3 | 99.6 ±0.1 | 92.3 ±0.5 | 100.0 ±0.0 | 99.8 ±0.1 | 100.0 ±0.0 | 99.8 ±0.1 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 99.0 ±0.2 | 100.0 ±0.0 | 99.3 ±0.2 |
| dens+k20+s2k | 99.1 ±0.2 | 92.4 ±0.4 | 100.0 ±0.0 | 94.0 ±0.7 | 100.0 ±0.0 | 95.0 ±0.7 |
| est_hrd+disj3 | 99.8 ±0.1 | 78.2 ±0.7 | 100.0 ±0.0 | 99.6 ±0.1 | 100.0 ±0.0 | 99.9 ±0.1 |
| dens+est_hrd+disj3 | 99.8 ±0.1 | 92.5 ±0.5 | 100.0 ±0.0 | 99.8 ±0.1 | 100.0 ±0.0 | 99.8 ±0.1 |
| srv50 | 9.2 ±1.1 | 0.7 ±0.1 | 98.4 ±0.3 | 67.0 ±1.4 | 99.1 ±0.2 | 76.5 ±1.3 |
| srv100 | 98.1 ±0.4 | 0.6 ±0.1 | 99.8 ±0.1 | 67.7 ±1.4 | 99.9 ±0.0 | 76.0 ±1.3 |
| srv100+dens | 98.5 ±0.3 | 97.8 ±0.4 | 99.8 ±0.1 | 77.4 ±1.2 | 99.9 ±0.0 | 80.2 ±1.1 |
| srv100+est_hrd | 99.8 ±0.1 | 0.8 ±0.2 | 100.0 ±0.0 | 66.4 ±1.4 | 100.0 ±0.0 | 76.0 ±1.3 |

**S=1000, m=20**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 0.5 ±0.1 | 0.2 ±0.1 | 89.8 ±0.8 | 34.1 ±1.3 | 97.7 ±0.4 | 68.5 ±1.4 |
| dens | 98.8 ±0.3 | 97.8 ±0.4 | 99.0 ±0.3 | 83.4 ±1.2 | 99.1 ±0.2 | 72.8 ±1.3 |
| est_tab | 15.8 ±0.9 | 3.5 ±0.3 | 91.3 ±0.7 | 39.5 ±1.4 | 98.1 ±0.3 | 73.7 ±1.3 |
| est_hrd | 99.8 ±0.1 | 0.2 ±0.1 | 100.0 ±0.0 | 34.3 ±1.3 | 100.0 ±0.0 | 67.6 ±1.4 |
| disj2 | 11.9 ±1.1 | 2.9 ±0.3 | 99.1 ±0.2 | 72.8 ±1.3 | 99.9 ±0.1 | 94.3 ±0.6 |
| disj3 | 66.0 ±1.6 | 22.4 ±0.8 | 100.0 ±0.0 | 92.1 ±0.7 | 100.0 ±0.0 | 99.0 ±0.3 |
| disj4 | 98.2 ±0.3 | 65.0 ±0.9 | 100.0 ±0.0 | 98.9 ±0.2 | 100.0 ±0.0 | 100.0 ±0.0 |
| k20 | 74.2 ±0.9 | 11.7 ±0.5 | 97.7 ±0.3 | 39.9 ±1.2 | 99.2 ±0.2 | 71.0 ±1.2 |
| s2k | 0.6 ±0.2 | 0.2 ±0.1 | 98.7 ±0.4 | 63.2 ±1.5 | 99.8 ±0.2 | 86.5 ±1.1 |
| k20+s2k | 77.9 ±0.8 | 20.2 ±0.7 | 99.8 ±0.1 | 68.1 ±1.3 | 100.0 ±0.0 | 88.5 ±1.0 |
| multi2 | 0.9 ±0.2 | 0.3 ±0.1 | 98.8 ±0.3 | 55.9 ±1.3 | 100.0 ±0.0 | 89.8 ±0.8 |
| multi3 | 1.2 ±0.2 | 0.5 ±0.1 | 99.9 ±0.1 | 71.5 ±1.2 | 100.0 ±0.0 | 96.3 ±0.5 |
| dens+est_hrd | 99.9 ±0.1 | 97.7 ±0.4 | 100.0 ±0.0 | 83.1 ±1.2 | 100.0 ±0.0 | 73.2 ±1.3 |
| dens+disj3 | 99.3 ±0.1 | 95.9 ±0.4 | 100.0 ±0.0 | 97.2 ±0.4 | 100.0 ±0.0 | 99.3 ±0.2 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 99.3 ±0.2 | 100.0 ±0.0 | 97.9 ±0.3 |
| dens+k20+s2k | 84.8 ±0.7 | 41.6 ±0.8 | 99.9 ±0.1 | 81.6 ±0.9 | 100.0 ±0.0 | 88.5 ±1.0 |
| est_hrd+disj3 | 99.9 ±0.1 | 23.0 ±0.8 | 100.0 ±0.0 | 92.5 ±0.7 | 100.0 ±0.0 | 99.2 ±0.2 |
| dens+est_hrd+disj3 | 99.9 ±0.1 | 96.0 ±0.4 | 100.0 ±0.0 | 97.1 ±0.4 | 100.0 ±0.0 | 99.3 ±0.2 |
| srv50 | 8.5 ±1.1 | 0.2 ±0.1 | 94.7 ±0.6 | 34.6 ±1.3 | 98.8 ±0.3 | 68.7 ±1.4 |
| srv100 | 97.8 ±0.4 | 0.2 ±0.1 | 99.7 ±0.1 | 34.0 ±1.3 | 99.9 ±0.1 | 68.1 ±1.4 |
| srv100+dens | 98.5 ±0.3 | 97.7 ±0.4 | 99.7 ±0.1 | 82.7 ±1.2 | 99.9 ±0.0 | 72.8 ±1.3 |
| srv100+est_hrd | 99.9 ±0.1 | 0.2 ±0.1 | 100.0 ±0.0 | 34.3 ±1.3 | 100.0 ±0.0 | 68.3 ±1.4 |

**S=1000, m=50**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 0.4 ±0.1 | 0.1 ±0.1 | 67.4 ±1.5 | 6.2 ±0.6 | 97.4 ±0.4 | 55.4 ±1.5 |
| dens | 98.8 ±0.2 | 94.4 ±0.6 | 99.5 ±0.1 | 78.9 ±1.0 | 98.9 ±0.3 | 60.1 ±1.4 |
| est_tab | 14.7 ±0.8 | 1.5 ±0.2 | 72.4 ±1.3 | 8.5 ±0.6 | 97.8 ±0.4 | 60.6 ±1.5 |
| est_hrd | 99.9 ±0.1 | 0.1 ±0.1 | 99.9 ±0.0 | 6.2 ±0.6 | 100.0 ±0.0 | 54.4 ±1.5 |
| disj2 | 3.1 ±0.5 | 0.6 ±0.1 | 88.9 ±1.0 | 22.1 ±1.1 | 99.9 ±0.1 | 86.8 ±1.0 |
| disj3 | 23.8 ±1.5 | 3.7 ±0.3 | 97.5 ±0.5 | 43.6 ±1.3 | 100.0 ±0.0 | 96.6 ±0.5 |
| disj4 | 64.2 ±1.7 | 16.2 ±0.7 | 99.5 ±0.2 | 66.1 ±1.2 | 100.0 ±0.0 | 99.1 ±0.3 |
| k20 | 31.2 ±1.4 | 2.7 ±0.3 | 93.0 ±0.5 | 10.5 ±0.6 | 99.1 ±0.2 | 57.1 ±1.4 |
| s2k | 0.3 ±0.1 | 0.2 ±0.1 | 80.9 ±1.5 | 14.2 ±1.0 | 99.8 ±0.2 | 74.0 ±1.4 |
| k20+s2k | 31.7 ±1.5 | 3.2 ±0.3 | 97.1 ±0.4 | 19.8 ±1.0 | 100.0 ±0.0 | 74.6 ±1.4 |
| multi2 | 0.7 ±0.2 | 0.3 ±0.1 | 89.2 ±0.9 | 12.3 ±0.8 | 99.9 ±0.1 | 78.9 ±1.1 |
| multi3 | 1.1 ±0.2 | 0.4 ±0.1 | 96.4 ±0.5 | 18.2 ±0.9 | 100.0 ±0.0 | 89.3 ±0.8 |
| dens+est_hrd | 99.9 ±0.1 | 94.2 ±0.6 | 99.9 ±0.0 | 79.0 ±1.0 | 100.0 ±0.0 | 60.1 ±1.4 |
| dens+disj3 | 99.1 ±0.2 | 95.6 ±0.5 | 99.9 ±0.1 | 88.2 ±0.8 | 100.0 ±0.0 | 96.7 ±0.5 |
| dens+multi3 | 100.0 ±0.0 | 99.9 ±0.1 | 100.0 ±0.0 | 98.7 ±0.2 | 100.0 ±0.0 | 92.9 ±0.6 |
| dens+k20+s2k | 65.4 ±0.9 | 21.4 ±0.7 | 99.0 ±0.2 | 43.4 ±1.0 | 100.0 ±0.0 | 75.5 ±1.3 |
| est_hrd+disj3 | 99.9 ±0.1 | 3.6 ±0.3 | 99.9 ±0.0 | 44.0 ±1.4 | 100.0 ±0.0 | 96.5 ±0.5 |
| dens+est_hrd+disj3 | 99.9 ±0.1 | 95.6 ±0.5 | 99.9 ±0.0 | 87.8 ±0.9 | 100.0 ±0.0 | 96.9 ±0.5 |
| srv50 | 8.0 ±1.0 | 0.1 ±0.1 | 76.1 ±1.4 | 6.2 ±0.6 | 98.6 ±0.3 | 54.3 ±1.5 |
| srv100 | 97.3 ±0.5 | 0.1 ±0.1 | 99.5 ±0.1 | 6.4 ±0.6 | 99.9 ±0.1 | 54.7 ±1.5 |
| srv100+dens | 98.1 ±0.4 | 94.1 ±0.6 | 99.6 ±0.1 | 78.9 ±1.0 | 99.9 ±0.0 | 59.7 ±1.4 |
| srv100+est_hrd | 99.8 ±0.1 | 0.1 ±0.1 | 99.9 ±0.0 | 6.1 ±0.6 | 100.0 ±0.0 | 54.7 ±1.5 |

**S=10000, no attack:** base 100.0, dens 100.0, est_tab 100.0, est_hrd 100.0, disj2 100.0, disj3 100.0, disj4 100.0, k20 100.0, s2k 100.0, k20+s2k 100.0, multi2 100.0, multi3 100.0, dens+est_hrd 100.0, dens+disj3 100.0, dens+multi3 100.0, dens+k20+s2k 100.0, est_hrd+disj3 100.0, dens+est_hrd+disj3 100.0, srv50 100.0, srv100 100.0, srv100+dens 100.0, srv100+est_hrd 100.0

**S=10000, m=10**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 1.0 ±0.3 | 0.5 ±0.1 | 96.9 ±0.4 | 59.2 ±1.4 | 98.0 ±0.4 | 68.6 ±1.4 |
| dens | 99.6 ±0.2 | 99.5 ±0.2 | 98.9 ±0.3 | 69.6 ±1.3 | 99.0 ±0.3 | 72.0 ±1.3 |
| est_tab | 3.9 ±0.5 | 1.5 ±0.2 | 97.0 ±0.4 | 62.2 ±1.4 | 98.1 ±0.4 | 70.2 ±1.3 |
| est_hrd | 100.0 ±0.0 | 0.4 ±0.1 | 100.0 ±0.0 | 59.7 ±1.4 | 100.0 ±0.0 | 68.0 ±1.4 |
| disj2 | 86.3 ±0.6 | 32.0 ±0.8 | 100.0 ±0.0 | 94.7 ±0.6 | 100.0 ±0.0 | 96.6 ±0.5 |
| disj3 | 99.6 ±0.1 | 79.0 ±0.7 | 100.0 ±0.0 | 99.8 ±0.1 | 100.0 ±0.0 | 99.8 ±0.1 |
| disj4 | 100.0 ±0.0 | 99.9 ±0.1 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 |
| k20 | 84.7 ±0.7 | 23.7 ±0.7 | 98.8 ±0.2 | 63.3 ±1.2 | 99.3 ±0.2 | 71.3 ±1.2 |
| s2k | 1.8 ±0.4 | 4.9 ±0.7 | 100.0 ±0.0 | 87.6 ±1.2 | 99.9 ±0.1 | 90.4 ±1.0 |
| k20+s2k | 94.3 ±0.4 | 73.6 ±1.0 | 100.0 ±0.0 | 89.6 ±1.0 | 100.0 ±0.0 | 91.1 ±0.9 |
| multi2 | 2.4 ±0.4 | 0.9 ±0.2 | 99.9 ±0.1 | 83.4 ±1.0 | 100.0 ±0.0 | 90.5 ±0.8 |
| multi3 | 2.8 ±0.4 | 1.3 ±0.2 | 100.0 ±0.0 | 93.3 ±0.6 | 100.0 ±0.0 | 97.0 ±0.4 |
| dens+est_hrd | 100.0 ±0.0 | 99.6 ±0.2 | 100.0 ±0.0 | 69.5 ±1.3 | 100.0 ±0.0 | 72.4 ±1.2 |
| dens+disj3 | 100.0 ±0.0 | 94.6 ±0.4 | 100.0 ±0.0 | 99.8 ±0.1 | 100.0 ±0.0 | 99.9 ±0.0 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 96.7 ±0.4 | 100.0 ±0.0 | 97.8 ±0.3 |
| dens+k20+s2k | 99.2 ±0.1 | 90.5 ±0.5 | 100.0 ±0.0 | 91.1 ±0.9 | 100.0 ±0.1 | 92.3 ±0.9 |
| est_hrd+disj3 | 100.0 ±0.0 | 78.7 ±0.7 | 100.0 ±0.0 | 99.7 ±0.1 | 100.0 ±0.0 | 99.8 ±0.1 |
| dens+est_hrd+disj3 | 100.0 ±0.0 | 94.3 ±0.4 | 100.0 ±0.0 | 99.9 ±0.1 | 100.0 ±0.0 | 99.9 ±0.1 |
| srv50 | 8.8 ±1.1 | 0.4 ±0.1 | 98.4 ±0.3 | 59.8 ±1.4 | 99.0 ±0.2 | 69.2 ±1.3 |
| srv100 | 99.8 ±0.1 | 0.4 ±0.1 | 100.0 ±0.0 | 59.4 ±1.4 | 100.0 ±0.0 | 68.1 ±1.3 |
| srv100+dens | 99.8 ±0.1 | 99.5 ±0.2 | 100.0 ±0.0 | 68.6 ±1.3 | 100.0 ±0.0 | 72.8 ±1.2 |
| srv100+est_hrd | 100.0 ±0.0 | 0.4 ±0.1 | 100.0 ±0.0 | 59.6 ±1.4 | 100.0 ±0.0 | 68.7 ±1.4 |

**S=10000, m=20**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 0.1 ±0.1 | 0.0 ±0.0 | 89.3 ±0.8 | 27.4 ±1.2 | 97.9 ±0.4 | 59.5 ±1.4 |
| dens | 99.8 ±0.1 | 99.8 ±0.1 | 99.3 ±0.2 | 79.1 ±1.3 | 99.1 ±0.2 | 63.4 ±1.3 |
| est_tab | 3.1 ±0.4 | 1.0 ±0.2 | 89.6 ±0.8 | 29.2 ±1.2 | 97.9 ±0.4 | 62.3 ±1.4 |
| est_hrd | 100.0 ±0.0 | 0.0 ±0.0 | 100.0 ±0.0 | 27.9 ±1.2 | 100.0 ±0.0 | 60.1 ±1.4 |
| disj2 | 11.8 ±1.1 | 2.9 ±0.3 | 99.0 ±0.2 | 67.1 ±1.3 | 99.9 ±0.1 | 93.0 ±0.7 |
| disj3 | 66.3 ±1.5 | 24.0 ±0.8 | 99.9 ±0.0 | 91.2 ±0.7 | 100.0 ±0.0 | 98.8 ±0.3 |
| disj4 | 98.1 ±0.3 | 64.4 ±0.8 | 100.0 ±0.0 | 99.4 ±0.1 | 100.0 ±0.0 | 100.0 ±0.0 |
| k20 | 71.7 ±0.9 | 10.4 ±0.5 | 96.8 ±0.4 | 33.2 ±1.1 | 99.2 ±0.2 | 62.6 ±1.3 |
| s2k | 0.1 ±0.1 | 0.0 ±0.0 | 98.7 ±0.4 | 55.2 ±1.6 | 100.0 ±0.0 | 79.9 ±1.4 |
| k20+s2k | 75.4 ±0.8 | 22.1 ±0.7 | 99.8 ±0.1 | 62.0 ±1.3 | 100.0 ±0.0 | 82.6 ±1.2 |
| multi2 | 0.1 ±0.1 | 0.0 ±0.0 | 98.7 ±0.3 | 46.7 ±1.3 | 99.9 ±0.1 | 84.2 ±1.0 |
| multi3 | 0.1 ±0.1 | 0.0 ±0.0 | 99.9 ±0.0 | 62.1 ±1.2 | 100.0 ±0.0 | 93.3 ±0.6 |
| dens+est_hrd | 100.0 ±0.0 | 99.8 ±0.1 | 100.0 ±0.0 | 78.9 ±1.3 | 100.0 ±0.0 | 63.4 ±1.4 |
| dens+disj3 | 99.9 ±0.0 | 97.8 ±0.2 | 100.0 ±0.0 | 96.6 ±0.4 | 100.0 ±0.0 | 99.2 ±0.2 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 99.0 ±0.3 | 100.0 ±0.0 | 95.1 ±0.5 |
| dens+k20+s2k | 82.7 ±0.7 | 44.0 ±0.8 | 99.9 ±0.1 | 76.7 ±1.0 | 100.0 ±0.0 | 82.3 ±1.2 |
| est_hrd+disj3 | 100.0 ±0.0 | 23.9 ±0.8 | 100.0 ±0.0 | 91.6 ±0.7 | 100.0 ±0.0 | 99.1 ±0.2 |
| dens+est_hrd+disj3 | 100.0 ±0.0 | 97.9 ±0.2 | 100.0 ±0.0 | 96.6 ±0.4 | 100.0 ±0.0 | 99.2 ±0.2 |
| srv50 | 7.5 ±1.0 | 0.0 ±0.0 | 94.5 ±0.6 | 27.9 ±1.2 | 99.0 ±0.2 | 60.7 ±1.4 |
| srv100 | 99.7 ±0.2 | 0.0 ±0.0 | 100.0 ±0.0 | 27.7 ±1.2 | 100.0 ±0.0 | 59.9 ±1.4 |
| srv100+dens | 99.9 ±0.1 | 99.8 ±0.1 | 100.0 ±0.0 | 79.1 ±1.3 | 100.0 ±0.0 | 62.9 ±1.4 |
| srv100+est_hrd | 100.0 ±0.0 | 0.0 ±0.0 | 100.0 ±0.0 | 27.3 ±1.2 | 100.0 ±0.0 | 60.4 ±1.4 |

**S=10000, m=50**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 0.1 ±0.1 | 0.0 ±0.0 | 67.1 ±1.5 | 4.8 ±0.5 | 97.8 ±0.4 | 48.9 ±1.4 |
| dens | 99.9 ±0.1 | 99.5 ±0.2 | 99.8 ±0.1 | 85.3 ±0.8 | 98.8 ±0.3 | 52.2 ±1.4 |
| est_tab | 2.7 ±0.4 | 0.7 ±0.1 | 68.0 ±1.4 | 6.1 ±0.6 | 97.8 ±0.4 | 50.9 ±1.4 |
| est_hrd | 100.0 ±0.0 | 0.0 ±0.0 | 100.0 ±0.0 | 5.2 ±0.5 | 100.0 ±0.0 | 48.8 ±1.4 |
| disj2 | 2.9 ±0.6 | 0.3 ±0.1 | 89.1 ±1.0 | 18.7 ±1.0 | 99.9 ±0.1 | 85.5 ±1.0 |
| disj3 | 24.4 ±1.5 | 4.5 ±0.4 | 97.3 ±0.5 | 40.6 ±1.3 | 100.0 ±0.0 | 96.8 ±0.5 |
| disj4 | 68.4 ±1.6 | 16.6 ±0.7 | 99.6 ±0.2 | 67.9 ±1.2 | 100.0 ±0.0 | 99.7 ±0.1 |
| k20 | 32.7 ±1.4 | 2.6 ±0.3 | 92.1 ±0.6 | 8.6 ±0.6 | 99.0 ±0.2 | 51.0 ±1.4 |
| s2k | 0.1 ±0.1 | 0.0 ±0.0 | 80.9 ±1.5 | 12.4 ±0.9 | 99.9 ±0.1 | 71.0 ±1.5 |
| k20+s2k | 34.1 ±1.4 | 3.8 ±0.3 | 96.7 ±0.4 | 18.6 ±0.9 | 100.0 ±0.0 | 72.2 ±1.4 |
| multi2 | 0.1 ±0.1 | 0.0 ±0.0 | 89.7 ±0.9 | 10.1 ±0.7 | 99.9 ±0.1 | 73.1 ±1.2 |
| multi3 | 0.1 ±0.1 | 0.1 ±0.0 | 96.6 ±0.5 | 13.8 ±0.8 | 100.0 ±0.0 | 86.5 ±0.8 |
| dens+est_hrd | 100.0 ±0.0 | 99.6 ±0.2 | 100.0 ±0.0 | 85.8 ±0.8 | 100.0 ±0.0 | 52.0 ±1.4 |
| dens+disj3 | 99.9 ±0.0 | 99.1 ±0.2 | 100.0 ±0.1 | 89.6 ±0.7 | 100.0 ±0.0 | 97.1 ±0.5 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 99.6 ±0.1 | 100.0 ±0.0 | 89.3 ±0.7 |
| dens+k20+s2k | 64.6 ±0.9 | 22.8 ±0.7 | 98.8 ±0.2 | 44.3 ±0.9 | 100.0 ±0.0 | 73.2 ±1.4 |
| est_hrd+disj3 | 100.0 ±0.0 | 4.2 ±0.4 | 100.0 ±0.0 | 40.8 ±1.3 | 100.0 ±0.0 | 97.0 ±0.5 |
| dens+est_hrd+disj3 | 100.0 ±0.0 | 99.1 ±0.2 | 100.0 ±0.0 | 89.9 ±0.7 | 100.0 ±0.0 | 97.2 ±0.4 |
| srv50 | 8.6 ±1.1 | 0.0 ±0.0 | 76.2 ±1.4 | 4.9 ±0.5 | 98.8 ±0.2 | 49.3 ±1.4 |
| srv100 | 99.8 ±0.1 | 0.0 ±0.0 | 100.0 ±0.0 | 5.1 ±0.5 | 100.0 ±0.0 | 48.1 ±1.4 |
| srv100+dens | 99.8 ±0.1 | 99.6 ±0.2 | 100.0 ±0.0 | 85.3 ±0.8 | 100.0 ±0.0 | 51.7 ±1.4 |
| srv100+est_hrd | 100.0 ±0.0 | 0.0 ±0.0 | 100.0 ±0.0 | 4.9 ±0.5 | 100.0 ±0.0 | 49.1 ±1.4 |

### Message cost (mean /find per search; per publish /find + /provide)

| defence | S=1000 no attack search | S=1000 no attack publish (find+provide) | S=1000 m=20 tight/aged search | S=1000 m=20 tight/aged publish (find+provide) | S=10000 no attack search | S=10000 no attack publish (find+provide) | S=10000 m=20 tight/aged search | S=10000 m=20 tight/aged publish (find+provide) |
|---|---|---|---|---|---|---|---|---|
| base | 6.3 | 6.3+10.0 | 6.5 | 6.5+10.0 | 7.7 | 7.8+10.0 | 7.8 | 7.8+10.0 |
| dens | 6.6 | 6.7+10.6 | 19.1 | 19.1+27.5 | 8.1 | 8.1+10.5 | 21.3 | 21.1+28.2 |
| est_tab | 6.3 | 6.3+12.4 | 6.5 | 6.4+12.5 | 7.7 | 7.7+12.9 | 7.8 | 7.8+12.9 |
| est_hrd | 6.3 | 6.3+10.0 | 6.5 | 6.4+10.0 | 7.7 | 7.7+10.0 | 7.8 | 7.8+10.0 |
| disj2 | 13.0 | 13.0+13.8 | 13.3 | 13.2+14.0 | 15.9 | 15.9+13.8 | 16.1 | 16.0+13.9 |
| disj3 | 19.7 | 19.7+19.4 | 20.1 | 20.1+20.1 | 24.2 | 24.1+19.8 | 24.4 | 24.3+20.2 |
| disj4 | 25.3 | 25.4+26.4 | 25.9 | 25.7+27.5 | 31.5 | 31.6+27.1 | 31.8 | 31.7+27.7 |
| k20 | 6.3 | 6.4+19.6 | 6.5 | 6.4+19.7 | 7.7 | 7.7+20.0 | 7.8 | 7.8+20.0 |
| s2k | 16.6 | 6.3+10.0 | 16.1 | 6.5+10.0 | 18.6 | 7.8+10.0 | 18.1 | 7.8+10.0 |
| k20+s2k | 16.6 | 6.4+19.6 | 16.1 | 6.4+19.7 | 18.6 | 7.7+20.0 | 18.1 | 7.8+20.0 |
| multi2 | 6.3 | 12.7+20.0 | 12.9 | 12.9+20.0 | 7.7 | 15.5+20.0 | 15.6 | 15.6+20.0 |
| multi3 | 6.3 | 19.0+30.0 | 19.4 | 19.4+30.0 | 7.7 | 23.2+30.0 | 23.4 | 23.5+30.0 |
| dens+est_hrd | 6.6 | 6.7+10.6 | 19.1 | 19.1+27.5 | 8.1 | 8.1+10.6 | 21.2 | 21.2+28.2 |
| dens+disj3 | 19.8 | 19.9+19.5 | 36.4 | 36.3+41.8 | 24.3 | 24.2+19.9 | 42.2 | 42.1+43.4 |
| dens+multi3 | 6.7 | 20.0+31.7 | 19.6 | 57.2+82.5 | 8.1 | 24.2+31.6 | 21.3 | 63.8+84.6 |
| dens+k20+s2k | 16.5 | 6.7+19.7 | 17.6 | 19.1+37.4 | 18.6 | 8.1+20.1 | 20.7 | 21.3+38.2 |
| est_hrd+disj3 | 19.7 | 19.7+19.4 | 20.1 | 20.0+20.1 | 24.2 | 24.1+19.7 | 24.4 | 24.4+20.2 |
| dens+est_hrd+disj3 | 19.8 | 19.9+19.6 | 36.4 | 36.2+41.9 | 24.3 | 24.2+19.9 | 42.1 | 42.0+43.5 |
| srv50 | 6.3 | 6.3+10.0 | 6.5 | 6.4+10.0 | 7.7 | 7.7+10.0 | 7.8 | 7.8+10.0 |
| srv100 | 6.3 | 6.3+10.0 | 6.5 | 6.5+10.0 | 7.7 | 7.7+10.0 | 7.8 | 7.8+10.0 |
| srv100+dens | 6.7 | 6.7+10.6 | 19.1 | 19.1+27.5 | 8.1 | 8.1+10.5 | 21.2 | 21.2+28.2 |
| srv100+est_hrd | 6.3 | 6.3+10.0 | 6.5 | 6.4+10.0 | 7.7 | 7.7+10.0 | 7.8 | 7.8+10.0 |

| defence | S=100 no attack search | S=100 no attack publish (find+provide) | S=100 m=20 tight/aged search | S=100 m=20 tight/aged publish (find+provide) | S=300 no attack search | S=300 no attack publish (find+provide) | S=300 m=20 tight/aged search | S=300 m=20 tight/aged publish (find+provide) |
|---|---|---|---|---|---|---|---|---|
| base | 4.7 | 4.7+10.0 | 5.5 | 5.5+10.0 | 5.5 | 5.5+10.0 | 5.9 | 5.9+10.0 |
| dens | 5.0 | 5.0+10.4 | 13.6 | 13.6+19.4 | 5.9 | 5.9+10.5 | 17.3 | 17.4+25.1 |
| est_tab | 4.7 | 4.7+10.6 | 5.5 | 5.5+11.3 | 5.5 | 5.5+11.7 | 5.9 | 5.9+12.0 |
| est_hrd | 4.7 | 4.7+10.0 | 5.5 | 5.5+10.0 | 5.5 | 5.5+10.0 | 5.9 | 5.9+10.0 |
| disj2 | 9.5 | 9.6+13.6 | 11.1 | 11.1+14.2 | 11.3 | 11.3+13.7 | 12.0 | 11.9+14.0 |
| disj3 | 14.4 | 14.5+16.3 | 16.8 | 16.8+19.1 | 17.1 | 17.2+18.3 | 18.1 | 18.1+19.6 |
| disj4 | 17.3 | 17.4+18.5 | 20.5 | 20.5+24.1 | 21.4 | 21.6+23.8 | 22.8 | 22.8+26.4 |
| k20 | 4.7 | 4.7+16.3 | 5.5 | 5.5+18.5 | 5.5 | 5.5+18.6 | 5.9 | 5.9+19.3 |
| s2k | 13.3 | 4.7+10.0 | 13.3 | 5.5+10.0 | 14.8 | 5.5+10.0 | 14.6 | 5.9+10.0 |
| k20+s2k | 13.3 | 4.7+16.3 | 13.3 | 5.5+18.5 | 14.8 | 5.5+18.6 | 14.6 | 5.9+19.3 |
| multi2 | 4.7 | 9.4+20.0 | 10.9 | 10.9+20.0 | 5.5 | 11.0+20.0 | 11.7 | 11.8+20.0 |
| multi3 | 4.7 | 14.1+30.0 | 16.3 | 16.4+30.0 | 5.5 | 16.6+30.0 | 17.5 | 17.6+30.0 |
| dens+est_hrd | 5.0 | 5.0+10.4 | 13.6 | 13.6+19.4 | 5.9 | 5.9+10.6 | 17.3 | 17.4+25.2 |
| dens+disj3 | 14.4 | 14.5+16.3 | 22.7 | 22.8+27.8 | 17.2 | 17.2+18.4 | 31.7 | 31.8+38.3 |
| dens+multi3 | 4.9 | 14.8+31.0 | 18.6 | 40.7+58.2 | 5.9 | 17.5+31.5 | 19.0 | 51.9+75.2 |
| dens+k20+s2k | 13.3 | 5.0+16.5 | 13.8 | 13.7+28.8 | 14.8 | 5.9+18.8 | 15.5 | 17.4+35.0 |
| est_hrd+disj3 | 14.4 | 14.5+16.3 | 16.8 | 16.8+19.1 | 17.1 | 17.2+18.3 | 18.1 | 18.1+19.6 |
| dens+est_hrd+disj3 | 14.4 | 14.5+16.3 | 22.7 | 22.8+27.6 | 17.2 | 17.2+18.3 | 31.7 | 31.8+38.3 |
| srv50 | 4.7 | 4.7+10.0 | 5.5 | 5.5+10.0 | 5.5 | 5.5+10.0 | 5.9 | 5.9+10.0 |
| srv100 | 4.7 | 4.7+10.0 | 5.5 | 5.5+10.0 | 5.5 | 5.5+10.0 | 5.9 | 5.9+10.0 |
| srv100+dens | 4.9 | 5.0+10.4 | 13.6 | 13.6+19.5 | 5.9 | 5.9+10.6 | 17.3 | 17.4+25.2 |
| srv100+est_hrd | 4.7 | 4.7+10.0 | 5.5 | 5.5+10.0 | 5.5 | 5.5+10.0 | 5.9 | 5.9+10.0 |


## Summary over attacks, alpha=1: S in {1000, 10000}, m in {10, 20, 50}, every attack variant run at this alpha (alpha=1 grid: tight fresh, tight pre-aged, evasive pre-aged)

Cost of a defence = extra messages vs baseline. 'no attack' is what every lookup pays always; the efficiency column weights publish messages by 1/10 (a key is looked up roughly as often as it is republished; change the weight to taste).

| defence | mean P(find) % | worst % | gain pp | +/find per search, no attack | +msgs per publish, no attack | +/find per search, attacked | +msgs per publish, attacked | gain pp per extra msg (search + publish/10, no attack) |
|---|---|---|---|---|---|---|---|---|
| dens+multi3 | 91.2 | 62.4 | +79.8 | +0.36 | +31.45 | +11.5 | +94.3 | 22.8 |
| dens+est_hrd+disj3 | 89.9 | 66.0 | +78.5 | +9.00 | +18.58 | +20.7 | +46.3 | 7.2 |
| dens+disj3 | 89.8 | 66.1 | +78.4 | +8.99 | +18.65 | +19.7 | +43.2 | 7.2 |
| disj4 | 80.5 | 10.0 | +69.1 | +13.51 | +30.82 | +14.4 | +31.1 | 4.2 |
| dens+est_hrd | 78.3 | 28.6 | +66.9 | +0.36 | +0.99 | +9.7 | +24.1 | 146.4 |
| dens | 78.1 | 28.8 | +66.7 | +0.35 | +0.91 | +9.0 | +21.7 | 151.8 |
| srv100+dens | 78.1 | 28.8 | +66.7 | +0.36 | +0.92 | +5.0 | +12.0 | 146.5 |
| est_hrd+disj3 | 70.0 | 3.1 | +58.6 | +8.91 | +18.40 | +11.4 | +27.8 | 5.5 |
| disj3 | 59.6 | 3.0 | +48.2 | +8.91 | +18.44 | +9.6 | +19.4 | 4.5 |
| srv100+est_hrd | 44.7 | 0.0 | +33.3 | +0.00 | +0.00 | -0.3 | -0.3 | free |
| est_hrd | 44.7 | 0.0 | +33.3 | +0.00 | +0.00 | +0.9 | +4.2 | free |
| srv100 | 44.3 | 0.0 | +32.9 | +0.00 | +0.00 | -0.3 | -0.3 | free |
| dens+k20+s2k | 43.6 | 5.1 | +32.2 | +7.41 | +10.32 | +7.5 | +29.8 | 3.8 |
| k20+s2k | 37.4 | 0.9 | +26.0 | +7.40 | +9.78 | +7.4 | +9.9 | 3.1 |
| disj2 | 36.7 | 0.5 | +25.3 | +4.43 | +7.96 | +4.8 | +9.0 | 4.8 |
| k20 | 33.2 | 0.9 | +21.8 | +0.00 | +9.78 | +0.0 | +9.9 | 22.3 |
| multi3 | 23.4 | 0.0 | +12.0 | +0.00 | +28.65 | +7.9 | +29.3 | 4.2 |
| est_tab | 21.8 | 0.7 | +10.4 | +1.96 | +4.64 | +2.1 | +4.9 | 4.3 |
| multi2 | 18.6 | 0.0 | +7.2 | +0.00 | +14.33 | +4.1 | +14.6 | 5.0 |
| s2k | 15.8 | 0.0 | +4.4 | +7.40 | +0.00 | +7.4 | +0.0 | 0.6 |
| srv50 | 14.5 | 0.0 | +3.1 | +0.00 | +0.00 | +0.1 | +0.1 | free |
| base | 11.4 | 0.0 | +0.0 | +0.00 | +0.00 | +0.0 | +0.0 | free |

Small networks (S=100/300):

| defence | mean P(find) % | worst % | gain pp | +/find per search, no attack | +msgs per publish, no attack | +/find per search, attacked | +msgs per publish, attacked | gain pp per extra msg (search + publish/10, no attack) |
|---|---|---|---|---|---|---|---|---|
| dens+multi3 | 92.5 | 73.5 | +80.5 | +0.28 | +28.95 | +11.3 | +80.5 | 25.3 |
| dens+est_hrd | 73.9 | 37.4 | +61.8 | +0.27 | +0.73 | +8.2 | +19.9 | 178.0 |
| dens+est_hrd+disj3 | 73.6 | 15.8 | +61.6 | +6.96 | +14.12 | +15.3 | +33.4 | 7.4 |
| disj4 | 73.6 | 3.3 | +61.5 | +10.38 | +20.65 | +11.7 | +25.1 | 4.9 |
| dens+disj3 | 71.4 | 15.9 | +59.3 | +6.96 | +14.11 | +14.2 | +30.7 | 7.1 |
| dens | 71.1 | 37.3 | +59.0 | +0.27 | +0.69 | +7.5 | +17.6 | 172.2 |
| srv100+dens | 70.6 | 37.5 | +58.6 | +0.27 | +0.70 | +4.3 | +10.0 | 170.9 |
| est_hrd+disj3 | 64.0 | 0.9 | +51.9 | +6.93 | +14.01 | +9.7 | +23.2 | 6.2 |
| disj3 | 51.9 | 0.8 | +39.9 | +6.93 | +14.02 | +7.9 | +16.8 | 4.8 |
| srv100+est_hrd | 44.8 | 0.0 | +32.8 | +0.00 | +0.00 | -0.2 | +0.1 | free |
| est_hrd | 44.8 | 0.0 | +32.8 | +0.00 | +0.00 | +0.9 | +4.0 | free |
| dens+k20+s2k | 40.1 | 0.1 | +28.1 | +7.55 | +7.98 | +7.5 | +25.3 | 3.4 |
| srv100 | 39.9 | 0.0 | +27.8 | +0.00 | +0.00 | -0.3 | -0.3 | free |
| est_tab | 38.3 | 0.3 | +26.2 | +1.60 | +2.72 | +2.0 | +4.1 | 14.0 |
| k20+s2k | 37.1 | 0.1 | +25.0 | +7.55 | +7.60 | +7.5 | +9.3 | 3.0 |
| disj2 | 32.4 | 0.2 | +20.4 | +3.46 | +6.88 | +3.9 | +8.1 | 4.9 |
| k20 | 31.2 | 0.1 | +19.1 | +0.00 | +7.60 | +0.0 | +9.3 | 25.1 |
| multi3 | 24.0 | 0.0 | +12.0 | +0.00 | +26.92 | +6.6 | +27.7 | 4.4 |
| multi2 | 19.2 | 0.0 | +7.2 | +0.00 | +13.46 | +3.4 | +13.8 | 5.3 |
| s2k | 18.0 | 0.0 | +5.9 | +7.55 | +0.00 | +7.5 | +0.0 | 0.8 |
| srv50 | 14.9 | 0.0 | +2.8 | +0.00 | +0.00 | +0.1 | +0.1 | free |
| base | 12.1 | 0.0 | +0.0 | +0.00 | +0.00 | +0.0 | +0.0 | free |

Pre-aged attacks only (the hard case), S in {1000, 10000}:

| defence | mean P(find) % | worst % | gain pp | +/find per search, no attack | +msgs per publish, no attack | +/find per search, attacked | +msgs per publish, attacked | gain pp per extra msg (search + publish/10, no attack) |
|---|---|---|---|---|---|---|---|---|
| dens+multi3 | 86.8 | 62.4 | +69.7 | +0.36 | +31.45 | +11.4 | +82.8 | 19.9 |
| dens+disj3 | 85.0 | 66.1 | +67.9 | +8.99 | +18.65 | +15.8 | +34.3 | 6.3 |
| dens+est_hrd+disj3 | 84.9 | 66.0 | +67.9 | +9.00 | +18.58 | +15.8 | +34.3 | 6.2 |
| disj4 | 73.8 | 10.0 | +56.7 | +13.51 | +30.82 | +13.6 | +30.5 | 3.4 |
| dens+est_hrd | 67.5 | 28.6 | +50.5 | +0.36 | +0.99 | +7.7 | +18.1 | 110.4 |
| dens | 67.5 | 28.8 | +50.4 | +0.35 | +0.91 | +7.7 | +18.1 | 114.8 |
| srv100+dens | 67.5 | 28.8 | +50.4 | +0.36 | +0.92 | +7.7 | +18.0 | 110.8 |
| disj3 | 55.2 | 3.0 | +38.1 | +8.91 | +18.44 | +9.0 | +19.2 | 3.5 |
| est_hrd+disj3 | 55.1 | 3.1 | +38.0 | +8.91 | +18.40 | +9.0 | +19.2 | 3.5 |
| disj2 | 39.0 | 0.5 | +22.0 | +4.43 | +7.96 | +4.5 | +9.1 | 4.2 |
| multi3 | 35.0 | 0.0 | +17.9 | +0.00 | +28.65 | +6.7 | +28.7 | 6.3 |
| dens+k20+s2k | 32.1 | 5.1 | +15.0 | +7.41 | +10.32 | +7.9 | +26.1 | 1.8 |
| est_tab | 29.4 | 0.7 | +12.3 | +1.96 | +4.64 | +2.1 | +4.9 | 5.1 |
| multi2 | 27.9 | 0.0 | +10.8 | +0.00 | +14.33 | +3.6 | +14.3 | 7.5 |
| k20+s2k | 26.4 | 0.9 | +9.4 | +7.40 | +9.78 | +7.8 | +9.9 | 1.1 |
| s2k | 23.7 | 0.0 | +6.6 | +7.40 | +0.00 | +7.8 | +0.0 | 0.9 |
| k20 | 20.0 | 0.9 | +3.0 | +0.00 | +9.78 | +0.0 | +9.9 | 3.0 |
| base | 17.1 | 0.0 | +0.0 | +0.00 | +0.00 | +0.0 | +0.0 | free |
| est_hrd | 17.1 | 0.0 | +0.0 | +0.00 | +0.00 | +0.0 | +0.0 | free |
| srv50 | 17.1 | 0.0 | +0.0 | +0.00 | +0.00 | +0.0 | +0.0 | free |
| srv100 | 17.1 | 0.0 | +0.0 | +0.00 | +0.00 | +0.0 | +0.0 | free |
| srv100+est_hrd | 17.1 | 0.0 | +0.0 | +0.00 | +0.00 | +0.0 | +0.0 | free |


## Main grid, alpha=1 (main_a1.jsonl)

### P(find) %, both publisher and searcher upgraded (±95% CI)

**S=100, no attack:** base 100.0, dens 100.0, est_tab 100.0, est_hrd 100.0, disj2 100.0, disj3 100.0, disj4 100.0, k20 100.0, s2k 100.0, k20+s2k 100.0, multi2 100.0, multi3 100.0, dens+est_hrd 100.0, dens+disj3 100.0, dens+multi3 100.0, dens+k20+s2k 100.0, est_hrd+disj3 100.0, dens+est_hrd+disj3 100.0, srv50 100.0, srv100 100.0, srv100+dens 100.0, srv100+est_hrd 100.0

**S=100, m=10**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 1.0 ±0.3 | 0.1 ±0.1 | - | - | - | 46.5 ±1.9 |
| dens | 86.1 ±1.1 | 79.4 ±1.3 | - | - | - | 55.9 ±1.8 |
| est_tab | 58.9 ±1.3 | 5.9 ±0.5 | - | - | - | 80.3 ±1.5 |
| est_hrd | 98.4 ±0.3 | 0.1 ±0.1 | - | - | - | 46.5 ±1.9 |
| disj2 | 63.2 ±1.2 | 9.0 ±0.6 | - | - | - | 73.7 ±1.7 |
| disj3 | 83.9 ±0.8 | 25.3 ±1.1 | - | - | - | 87.9 ±1.3 |
| disj4 | 96.7 ±0.4 | 86.8 ±1.1 | - | - | - | 97.8 ±0.5 |
| k20 | 59.9 ±1.3 | 2.3 ±0.3 | - | - | - | 46.8 ±1.9 |
| s2k | 1.0 ±0.3 | 0.1 ±0.1 | - | - | - | 72.7 ±1.8 |
| k20+s2k | 59.9 ±1.3 | 2.3 ±0.3 | - | - | - | 73.4 ±1.8 |
| multi2 | 1.8 ±0.4 | 0.1 ±0.1 | - | - | - | 70.2 ±1.7 |
| multi3 | 2.7 ±0.4 | 0.3 ±0.1 | - | - | - | 83.1 ±1.4 |
| dens+est_hrd | 98.4 ±0.3 | 79.5 ±1.3 | - | - | - | 55.3 ±1.9 |
| dens+disj3 | 87.8 ±0.7 | 34.1 ±1.1 | - | - | - | 88.5 ±1.2 |
| dens+multi3 | 99.9 ±0.1 | 98.8 ±0.3 | - | - | - | 89.9 ±1.0 |
| dens+k20+s2k | 79.9 ±1.0 | 7.2 ±0.6 | - | - | - | 75.3 ±1.7 |
| est_hrd+disj3 | 98.5 ±0.2 | 25.4 ±1.1 | - | - | - | 88.0 ±1.2 |
| dens+est_hrd+disj3 | 98.5 ±0.2 | 33.6 ±1.1 | - | - | - | 88.4 ±1.2 |
| srv50 | 9.3 ±1.3 | 0.1 ±0.1 | - | - | - | 46.5 ±1.9 |
| srv100 | 75.7 ±1.6 | 0.1 ±0.1 | - | - | - | 46.5 ±1.9 |
| srv100+dens | 83.2 ±1.4 | 79.1 ±1.3 | - | - | - | 55.5 ±1.9 |
| srv100+est_hrd | 98.4 ±0.3 | 0.1 ±0.1 | - | - | - | 46.5 ±1.9 |

**S=100, m=20**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 1.1 ±0.3 | 0.0 ±0.0 | - | - | - | 37.6 ±1.8 |
| dens | 86.6 ±1.1 | 72.4 ±1.5 | - | - | - | 51.0 ±1.8 |
| est_tab | 57.1 ±1.4 | 2.1 ±0.3 | - | - | - | 69.1 ±1.8 |
| est_hrd | 98.5 ±0.3 | 0.0 ±0.0 | - | - | - | 37.6 ±1.8 |
| disj2 | 16.0 ±1.3 | 1.3 ±0.2 | - | - | - | 62.5 ±1.9 |
| disj3 | 64.6 ±1.4 | 6.1 ±0.5 | - | - | - | 77.5 ±1.6 |
| disj4 | 89.6 ±0.7 | 20.7 ±1.0 | - | - | - | 89.8 ±1.2 |
| k20 | 58.7 ±1.3 | 0.6 ±0.2 | - | - | - | 38.0 ±1.8 |
| s2k | 1.1 ±0.3 | 0.0 ±0.0 | - | - | - | 58.9 ±2.0 |
| k20+s2k | 58.7 ±1.3 | 0.6 ±0.2 | - | - | - | 59.4 ±2.0 |
| multi2 | 2.2 ±0.4 | 0.1 ±0.1 | - | - | - | 59.9 ±1.8 |
| multi3 | 3.0 ±0.5 | 0.1 ±0.1 | - | - | - | 74.8 ±1.5 |
| dens+est_hrd | 98.5 ±0.3 | 72.5 ±1.5 | - | - | - | 50.7 ±1.8 |
| dens+disj3 | 88.5 ±0.7 | 31.8 ±1.1 | - | - | - | 78.8 ±1.5 |
| dens+multi3 | 99.7 ±0.1 | 96.7 ±0.6 | - | - | - | 87.1 ±1.1 |
| dens+k20+s2k | 53.2 ±1.4 | 1.1 ±0.2 | - | - | - | 63.2 ±1.9 |
| est_hrd+disj3 | 98.6 ±0.2 | 6.0 ±0.6 | - | - | - | 77.5 ±1.6 |
| dens+est_hrd+disj3 | 98.6 ±0.2 | 32.2 ±1.1 | - | - | - | 79.1 ±1.5 |
| srv50 | 10.1 ±1.4 | 0.0 ±0.0 | - | - | - | 37.6 ±1.8 |
| srv100 | 77.9 ±1.5 | 0.0 ±0.0 | - | - | - | 37.6 ±1.8 |
| srv100+dens | 85.4 ±1.2 | 72.5 ±1.5 | - | - | - | 51.1 ±1.8 |
| srv100+est_hrd | 98.5 ±0.3 | 0.0 ±0.0 | - | - | - | 37.6 ±1.8 |

**S=100, m=50**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 1.0 ±0.3 | 0.0 ±0.0 | - | - | - | 24.0 ±1.6 |
| dens | 85.8 ±1.1 | 41.1 ±1.7 | - | - | - | 48.3 ±1.8 |
| est_tab | 58.3 ±1.4 | 0.3 ±0.1 | - | - | - | 48.2 ±1.9 |
| est_hrd | 98.5 ±0.3 | 0.0 ±0.0 | - | - | - | 24.0 ±1.6 |
| disj2 | 5.7 ±0.8 | 0.2 ±0.1 | - | - | - | 43.9 ±1.9 |
| disj3 | 32.1 ±1.8 | 0.8 ±0.2 | - | - | - | 59.1 ±1.9 |
| disj4 | 74.9 ±1.3 | 3.3 ±0.4 | - | - | - | 75.0 ±1.6 |
| k20 | 47.4 ±1.4 | 0.1 ±0.1 | - | - | - | 24.5 ±1.6 |
| s2k | 1.0 ±0.3 | 0.0 ±0.0 | - | - | - | 39.3 ±1.9 |
| k20+s2k | 47.4 ±1.4 | 0.1 ±0.1 | - | - | - | 40.0 ±1.9 |
| multi2 | 1.8 ±0.4 | 0.0 ±0.0 | - | - | - | 39.7 ±1.8 |
| multi3 | 2.5 ±0.4 | 0.0 ±0.0 | - | - | - | 52.7 ±1.8 |
| dens+est_hrd | 98.5 ±0.3 | 40.5 ±1.7 | - | - | - | 49.1 ±1.8 |
| dens+disj3 | 88.3 ±0.7 | 15.9 ±0.9 | - | - | - | 62.1 ±1.8 |
| dens+multi3 | 99.7 ±0.2 | 77.7 ±1.4 | - | - | - | 84.0 ±1.2 |
| dens+k20+s2k | 24.0 ±1.2 | 0.1 ±0.1 | - | - | - | 48.6 ±1.7 |
| est_hrd+disj3 | 98.6 ±0.2 | 0.9 ±0.2 | - | - | - | 58.8 ±1.9 |
| dens+est_hrd+disj3 | 98.6 ±0.2 | 15.8 ±0.9 | - | - | - | 62.1 ±1.8 |
| srv50 | 9.1 ±1.3 | 0.0 ±0.0 | - | - | - | 24.0 ±1.6 |
| srv100 | 77.1 ±1.5 | 0.0 ±0.0 | - | - | - | 24.0 ±1.6 |
| srv100+dens | 84.0 ±1.3 | 40.8 ±1.7 | - | - | - | 49.3 ±1.8 |
| srv100+est_hrd | 98.5 ±0.3 | 0.0 ±0.0 | - | - | - | 24.0 ±1.6 |

**S=300, no attack:** base 100.0, dens 100.0, est_tab 100.0, est_hrd 100.0, disj2 100.0, disj3 100.0, disj4 100.0, k20 100.0, s2k 100.0, k20+s2k 100.0, multi2 100.0, multi3 100.0, dens+est_hrd 100.0, dens+disj3 100.0, dens+multi3 100.0, dens+k20+s2k 100.0, est_hrd+disj3 100.0, dens+est_hrd+disj3 100.0, srv50 100.0, srv100 100.0, srv100+dens 100.0, srv100+est_hrd 100.0

**S=300, m=10**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 0.4 ±0.1 | 0.1 ±0.1 | - | - | - | 42.0 ±1.8 |
| dens | 94.6 ±0.8 | 92.5 ±0.9 | - | - | - | 46.1 ±1.8 |
| est_tab | 28.3 ±1.2 | 7.7 ±0.6 | - | - | - | 82.1 ±1.5 |
| est_hrd | 99.5 ±0.1 | 0.1 ±0.1 | - | - | - | 42.0 ±1.8 |
| disj2 | 71.5 ±1.1 | 14.2 ±0.8 | - | - | - | 78.0 ±1.5 |
| disj3 | 93.4 ±0.6 | 37.2 ±1.1 | - | - | - | 93.1 ±0.9 |
| disj4 | 99.1 ±0.2 | 95.8 ±0.6 | - | - | - | 99.4 ±0.3 |
| k20 | 61.8 ±1.3 | 6.0 ±0.5 | - | - | - | 42.9 ±1.8 |
| s2k | 0.4 ±0.1 | 0.1 ±0.1 | - | - | - | 60.3 ±1.9 |
| k20+s2k | 61.8 ±1.3 | 6.0 ±0.5 | - | - | - | 61.1 ±1.9 |
| multi2 | 0.6 ±0.2 | 0.2 ±0.1 | - | - | - | 65.4 ±1.7 |
| multi3 | 0.9 ±0.2 | 0.3 ±0.1 | - | - | - | 78.2 ±1.4 |
| dens+est_hrd | 99.5 ±0.1 | 92.5 ±0.9 | - | - | - | 46.3 ±1.8 |
| dens+disj3 | 95.8 ±0.4 | 56.7 ±1.2 | - | - | - | 93.4 ±0.9 |
| dens+multi3 | 100.0 ±0.0 | 99.9 ±0.1 | - | - | - | 82.4 ±1.3 |
| dens+k20+s2k | 88.2 ±0.8 | 21.5 ±0.9 | - | - | - | 61.9 ±1.9 |
| est_hrd+disj3 | 99.5 ±0.1 | 37.6 ±1.1 | - | - | - | 93.0 ±0.9 |
| dens+est_hrd+disj3 | 99.5 ±0.1 | 56.1 ±1.2 | - | - | - | 93.1 ±0.9 |
| srv50 | 9.3 ±1.4 | 0.1 ±0.1 | - | - | - | 42.0 ±1.8 |
| srv100 | 91.9 ±1.0 | 0.1 ±0.1 | - | - | - | 42.0 ±1.8 |
| srv100+dens | 94.3 ±0.8 | 92.7 ±0.9 | - | - | - | 46.2 ±1.8 |
| srv100+est_hrd | 99.5 ±0.1 | 0.1 ±0.1 | - | - | - | 42.0 ±1.8 |

**S=300, m=20**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 0.4 ±0.1 | 0.1 ±0.1 | - | - | - | 34.8 ±1.7 |
| dens | 95.1 ±0.7 | 92.0 ±0.9 | - | - | - | 39.6 ±1.8 |
| est_tab | 27.7 ±1.2 | 3.9 ±0.4 | - | - | - | 72.8 ±1.7 |
| est_hrd | 99.5 ±0.1 | 0.1 ±0.1 | - | - | - | 34.8 ±1.7 |
| disj2 | 17.7 ±1.4 | 2.4 ±0.3 | - | - | - | 64.3 ±1.8 |
| disj3 | 71.6 ±1.5 | 11.6 ±0.7 | - | - | - | 85.5 ±1.3 |
| disj4 | 95.4 ±0.5 | 33.0 ±1.1 | - | - | - | 96.3 ±0.7 |
| k20 | 59.2 ±1.3 | 2.2 ±0.3 | - | - | - | 35.4 ±1.7 |
| s2k | 0.4 ±0.1 | 0.1 ±0.1 | - | - | - | 48.9 ±1.9 |
| k20+s2k | 59.2 ±1.3 | 2.2 ±0.3 | - | - | - | 49.5 ±1.9 |
| multi2 | 0.8 ±0.2 | 0.1 ±0.1 | - | - | - | 55.8 ±1.8 |
| multi3 | 1.2 ±0.3 | 0.2 ±0.1 | - | - | - | 71.0 ±1.5 |
| dens+est_hrd | 99.5 ±0.1 | 91.9 ±0.9 | - | - | - | 39.6 ±1.7 |
| dens+disj3 | 96.1 ±0.4 | 63.5 ±1.2 | - | - | - | 85.9 ±1.3 |
| dens+multi3 | 100.0 ±0.0 | 99.9 ±0.1 | - | - | - | 77.8 ±1.4 |
| dens+k20+s2k | 61.8 ±1.2 | 6.0 ±0.5 | - | - | - | 50.6 ±1.9 |
| est_hrd+disj3 | 99.5 ±0.1 | 11.4 ±0.7 | - | - | - | 85.3 ±1.3 |
| dens+est_hrd+disj3 | 99.5 ±0.1 | 63.4 ±1.2 | - | - | - | 85.8 ±1.3 |
| srv50 | 9.4 ±1.4 | 0.1 ±0.1 | - | - | - | 34.8 ±1.7 |
| srv100 | 90.9 ±1.1 | 0.1 ±0.1 | - | - | - | 34.8 ±1.7 |
| srv100+dens | 93.7 ±0.9 | 91.8 ±0.9 | - | - | - | 39.6 ±1.8 |
| srv100+est_hrd | 99.5 ±0.1 | 0.1 ±0.1 | - | - | - | 34.8 ±1.7 |

**S=300, m=50**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 0.2 ±0.1 | 0.0 ±0.0 | - | - | - | 27.8 ±1.6 |
| dens | 95.3 ±0.6 | 80.8 ±1.4 | - | - | - | 37.3 ±1.7 |
| est_tab | 27.1 ±1.2 | 1.0 ±0.2 | - | - | - | 58.3 ±1.9 |
| est_hrd | 99.5 ±0.1 | 0.0 ±0.0 | - | - | - | 27.8 ±1.6 |
| disj2 | 4.4 ±0.7 | 0.3 ±0.1 | - | - | - | 55.7 ±1.9 |
| disj3 | 31.0 ±1.9 | 2.0 ±0.3 | - | - | - | 72.0 ±1.7 |
| disj4 | 79.9 ±1.4 | 6.4 ±0.5 | - | - | - | 84.3 ±1.4 |
| k20 | 46.5 ±1.4 | 0.2 ±0.1 | - | - | - | 28.5 ±1.6 |
| s2k | 0.2 ±0.1 | 0.0 ±0.0 | - | - | - | 39.1 ±1.9 |
| k20+s2k | 46.5 ±1.4 | 0.2 ±0.1 | - | - | - | 39.6 ±1.9 |
| multi2 | 0.6 ±0.2 | 0.0 ±0.0 | - | - | - | 47.0 ±1.8 |
| multi3 | 1.0 ±0.2 | 0.0 ±0.0 | - | - | - | 60.5 ±1.7 |
| dens+est_hrd | 99.5 ±0.1 | 80.9 ±1.3 | - | - | - | 37.4 ±1.7 |
| dens+disj3 | 96.1 ±0.4 | 48.2 ±1.4 | - | - | - | 73.5 ±1.7 |
| dens+multi3 | 100.0 ±0.0 | 98.2 ±0.4 | - | - | - | 73.5 ±1.4 |
| dens+k20+s2k | 36.5 ±1.2 | 1.1 ±0.2 | - | - | - | 42.3 ±1.8 |
| est_hrd+disj3 | 99.5 ±0.1 | 1.9 ±0.3 | - | - | - | 71.7 ±1.7 |
| dens+est_hrd+disj3 | 99.5 ±0.1 | 48.6 ±1.4 | - | - | - | 73.1 ±1.7 |
| srv50 | 7.9 ±1.3 | 0.0 ±0.0 | - | - | - | 27.8 ±1.6 |
| srv100 | 91.7 ±1.0 | 0.0 ±0.0 | - | - | - | 27.8 ±1.6 |
| srv100+dens | 94.5 ±0.8 | 80.9 ±1.4 | - | - | - | 37.5 ±1.8 |
| srv100+est_hrd | 99.5 ±0.1 | 0.0 ±0.0 | - | - | - | 27.8 ±1.6 |

**S=1000, no attack:** base 100.0, dens 100.0, est_tab 100.0, est_hrd 100.0, disj2 100.0, disj3 100.0, disj4 100.0, k20 100.0, s2k 100.0, k20+s2k 100.0, multi2 100.0, multi3 100.0, dens+est_hrd 100.0, dens+disj3 100.0, dens+multi3 100.0, dens+k20+s2k 100.0, est_hrd+disj3 100.0, dens+est_hrd+disj3 100.0, srv50 100.0, srv100 100.0, srv100+dens 100.0, srv100+est_hrd 100.0

**S=1000, m=10**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 0.1 ±0.1 | 0.1 ±0.1 | - | - | - | 41.7 ±1.8 |
| dens | 98.4 ±0.5 | 97.6 ±0.5 | - | - | - | 44.9 ±1.8 |
| est_tab | 11.0 ±0.8 | 4.8 ±0.5 | - | - | - | 76.5 ±1.6 |
| est_hrd | 99.9 ±0.1 | 0.1 ±0.1 | - | - | - | 41.7 ±1.8 |
| disj2 | 77.3 ±1.0 | 14.6 ±0.8 | - | - | - | 83.1 ±1.4 |
| disj3 | 97.1 ±0.4 | 40.8 ±1.1 | - | - | - | 96.8 ±0.6 |
| disj4 | 99.7 ±0.1 | 98.8 ±0.3 | - | - | - | 99.9 ±0.1 |
| k20 | 66.7 ±1.3 | 8.7 ±0.6 | - | - | - | 43.2 ±1.8 |
| s2k | 0.1 ±0.1 | 0.1 ±0.1 | - | - | - | 58.9 ±1.9 |
| k20+s2k | 66.7 ±1.3 | 8.7 ±0.6 | - | - | - | 59.9 ±1.9 |
| multi2 | 0.3 ±0.1 | 0.2 ±0.1 | - | - | - | 65.0 ±1.7 |
| multi3 | 0.4 ±0.2 | 0.2 ±0.1 | - | - | - | 78.8 ±1.4 |
| dens+est_hrd | 99.9 ±0.1 | 97.6 ±0.5 | - | - | - | 45.0 ±1.8 |
| dens+disj3 | 98.9 ±0.2 | 66.1 ±1.1 | - | - | - | 97.0 ±0.5 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | - | - | - | 82.3 ±1.2 |
| dens+k20+s2k | 93.6 ±0.6 | 27.4 ±1.0 | - | - | - | 60.4 ±1.9 |
| est_hrd+disj3 | 99.9 ±0.1 | 41.7 ±1.1 | - | - | - | 96.6 ±0.6 |
| dens+est_hrd+disj3 | 99.9 ±0.1 | 66.0 ±1.1 | - | - | - | 97.0 ±0.5 |
| srv50 | 9.3 ±1.4 | 0.1 ±0.1 | - | - | - | 41.7 ±1.8 |
| srv100 | 98.2 ±0.4 | 0.1 ±0.1 | - | - | - | 41.7 ±1.8 |
| srv100+dens | 98.9 ±0.3 | 97.6 ±0.5 | - | - | - | 45.0 ±1.8 |
| srv100+est_hrd | 99.9 ±0.1 | 0.1 ±0.1 | - | - | - | 41.7 ±1.8 |

**S=1000, m=20**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 0.1 ±0.1 | 0.0 ±0.0 | - | - | - | 33.9 ±1.7 |
| dens | 98.9 ±0.3 | 98.0 ±0.4 | - | - | - | 36.6 ±1.7 |
| est_tab | 12.3 ±0.9 | 3.3 ±0.4 | - | - | - | 69.9 ±1.7 |
| est_hrd | 99.8 ±0.1 | 0.0 ±0.0 | - | - | - | 33.9 ±1.7 |
| disj2 | 15.0 ±1.4 | 2.0 ±0.3 | - | - | - | 72.8 ±1.6 |
| disj3 | 76.5 ±1.4 | 12.9 ±0.7 | - | - | - | 91.3 ±1.0 |
| disj4 | 97.4 ±0.4 | 38.1 ±1.1 | - | - | - | 98.4 ±0.4 |
| k20 | 62.0 ±1.3 | 3.3 ±0.4 | - | - | - | 35.2 ±1.6 |
| s2k | 0.1 ±0.1 | 0.0 ±0.0 | - | - | - | 46.8 ±1.9 |
| k20+s2k | 62.0 ±1.3 | 3.3 ±0.4 | - | - | - | 47.8 ±1.9 |
| multi2 | 0.3 ±0.1 | 0.1 ±0.1 | - | - | - | 56.1 ±1.7 |
| multi3 | 0.4 ±0.2 | 0.1 ±0.1 | - | - | - | 70.2 ±1.5 |
| dens+est_hrd | 99.9 ±0.1 | 98.0 ±0.4 | - | - | - | 36.9 ±1.7 |
| dens+disj3 | 99.0 ±0.2 | 80.6 ±1.0 | - | - | - | 91.5 ±1.0 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | - | - | - | 74.3 ±1.4 |
| dens+k20+s2k | 66.0 ±1.3 | 12.2 ±0.7 | - | - | - | 48.3 ±1.8 |
| est_hrd+disj3 | 99.9 ±0.1 | 13.0 ±0.7 | - | - | - | 91.3 ±1.0 |
| dens+est_hrd+disj3 | 99.9 ±0.1 | 79.8 ±1.0 | - | - | - | 91.5 ±1.0 |
| srv50 | 9.0 ±1.4 | 0.0 ±0.0 | - | - | - | 33.9 ±1.7 |
| srv100 | 97.9 ±0.5 | 0.0 ±0.0 | - | - | - | 33.9 ±1.7 |
| srv100+dens | 98.7 ±0.4 | 98.0 ±0.4 | - | - | - | 36.6 ±1.7 |
| srv100+est_hrd | 99.9 ±0.1 | 0.0 ±0.0 | - | - | - | 33.9 ±1.7 |

**S=1000, m=50**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 0.1 ±0.1 | 0.0 ±0.0 | - | - | - | 26.4 ±1.6 |
| dens | 98.9 ±0.3 | 94.1 ±0.8 | - | - | - | 30.3 ±1.6 |
| est_tab | 10.7 ±0.8 | 1.9 ±0.3 | - | - | - | 55.4 ±1.9 |
| est_hrd | 99.9 ±0.1 | 0.0 ±0.0 | - | - | - | 26.4 ±1.6 |
| disj2 | 2.9 ±0.6 | 0.5 ±0.1 | - | - | - | 58.2 ±1.9 |
| disj3 | 32.1 ±2.0 | 3.0 ±0.4 | - | - | - | 79.7 ±1.5 |
| disj4 | 83.1 ±1.3 | 10.0 ±0.7 | - | - | - | 92.3 ±1.0 |
| k20 | 50.5 ±1.4 | 0.9 ±0.2 | - | - | - | 27.4 ±1.6 |
| s2k | 0.1 ±0.1 | 0.0 ±0.0 | - | - | - | 35.2 ±1.8 |
| k20+s2k | 50.5 ±1.4 | 0.9 ±0.2 | - | - | - | 35.7 ±1.8 |
| multi2 | 0.3 ±0.1 | 0.0 ±0.0 | - | - | - | 45.1 ±1.7 |
| multi3 | 0.4 ±0.2 | 0.1 ±0.1 | - | - | - | 58.9 ±1.6 |
| dens+est_hrd | 99.9 ±0.1 | 94.0 ±0.8 | - | - | - | 30.6 ±1.6 |
| dens+disj3 | 99.0 ±0.2 | 76.2 ±1.2 | - | - | - | 80.1 ±1.5 |
| dens+multi3 | 100.0 ±0.0 | 99.8 ±0.2 | - | - | - | 64.9 ±1.6 |
| dens+k20+s2k | 39.6 ±1.1 | 5.1 ±0.5 | - | - | - | 36.7 ±1.7 |
| est_hrd+disj3 | 99.9 ±0.1 | 3.1 ±0.4 | - | - | - | 79.4 ±1.5 |
| dens+est_hrd+disj3 | 99.9 ±0.1 | 76.7 ±1.2 | - | - | - | 80.3 ±1.5 |
| srv50 | 9.8 ±1.5 | 0.0 ±0.0 | - | - | - | 26.4 ±1.6 |
| srv100 | 97.5 ±0.6 | 0.0 ±0.0 | - | - | - | 26.4 ±1.6 |
| srv100+dens | 98.4 ±0.5 | 94.1 ±0.8 | - | - | - | 30.4 ±1.6 |
| srv100+est_hrd | 99.9 ±0.1 | 0.0 ±0.0 | - | - | - | 26.4 ±1.6 |

**S=10000, no attack:** base 100.0, dens 100.0, est_tab 100.0, est_hrd 100.0, disj2 100.0, disj3 100.0, disj4 100.0, k20 100.0, s2k 100.0, k20+s2k 100.0, multi2 100.0, multi3 100.0, dens+est_hrd 100.0, dens+disj3 100.0, dens+multi3 100.0, dens+k20+s2k 100.0, est_hrd+disj3 100.0, dens+est_hrd+disj3 100.0, srv50 100.0, srv100 100.0, srv100+dens 100.0, srv100+est_hrd 100.0

**S=10000, m=10**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 0.0 ±0.0 | 0.0 ±0.0 | - | - | - | 42.4 ±1.8 |
| dens | 99.5 ±0.3 | 99.3 ±0.4 | - | - | - | 45.7 ±1.8 |
| est_tab | 1.9 ±0.4 | 1.0 ±0.2 | - | - | - | 53.7 ±1.9 |
| est_hrd | 99.9 ±0.1 | 0.0 ±0.0 | - | - | - | 42.4 ±1.8 |
| disj2 | 77.0 ±1.1 | 14.4 ±0.8 | - | - | - | 84.0 ±1.3 |
| disj3 | 98.0 ±0.3 | 45.8 ±1.1 | - | - | - | 97.6 ±0.5 |
| disj4 | 100.0 ±0.0 | 99.8 ±0.1 | - | - | - | 100.0 ±0.0 |
| k20 | 66.2 ±1.3 | 9.1 ±0.6 | - | - | - | 44.3 ±1.7 |
| s2k | 0.0 ±0.0 | 0.0 ±0.0 | - | - | - | 60.6 ±1.9 |
| k20+s2k | 66.2 ±1.3 | 9.1 ±0.6 | - | - | - | 61.8 ±1.8 |
| multi2 | 0.0 ±0.0 | 0.1 ±0.1 | - | - | - | 66.5 ±1.6 |
| multi3 | 0.0 ±0.0 | 0.1 ±0.1 | - | - | - | 80.3 ±1.3 |
| dens+est_hrd | 100.0 ±0.0 | 99.1 ±0.4 | - | - | - | 46.0 ±1.8 |
| dens+disj3 | 99.8 ±0.1 | 74.9 ±1.0 | - | - | - | 97.8 ±0.5 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | - | - | - | 83.3 ±1.2 |
| dens+k20+s2k | 93.8 ±0.6 | 27.8 ±0.9 | - | - | - | 61.9 ±1.8 |
| est_hrd+disj3 | 100.0 ±0.0 | 45.0 ±1.1 | - | - | - | 97.7 ±0.5 |
| dens+est_hrd+disj3 | 100.0 ±0.0 | 75.5 ±1.0 | - | - | - | 97.8 ±0.5 |
| srv50 | 9.4 ±1.4 | 0.0 ±0.0 | - | - | - | 42.4 ±1.8 |
| srv100 | 99.6 ±0.2 | 0.0 ±0.0 | - | - | - | 42.4 ±1.8 |
| srv100+dens | 99.7 ±0.2 | 99.2 ±0.4 | - | - | - | 45.7 ±1.8 |
| srv100+est_hrd | 100.0 ±0.0 | 0.0 ±0.0 | - | - | - | 42.4 ±1.8 |

**S=10000, m=20**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 0.0 ±0.0 | 0.0 ±0.0 | - | - | - | 33.3 ±1.7 |
| dens | 99.9 ±0.1 | 99.7 ±0.2 | - | - | - | 35.8 ±1.7 |
| est_tab | 1.8 ±0.3 | 0.8 ±0.2 | - | - | - | 46.3 ±1.9 |
| est_hrd | 100.0 ±0.0 | 0.0 ±0.0 | - | - | - | 33.3 ±1.7 |
| disj2 | 16.3 ±1.4 | 1.8 ±0.3 | - | - | - | 75.0 ±1.6 |
| disj3 | 74.1 ±1.5 | 14.5 ±0.8 | - | - | - | 92.7 ±0.9 |
| disj4 | 98.1 ±0.3 | 41.8 ±1.1 | - | - | - | 98.9 ±0.4 |
| k20 | 61.8 ±1.3 | 4.3 ±0.4 | - | - | - | 34.8 ±1.7 |
| s2k | 0.0 ±0.0 | 0.0 ±0.0 | - | - | - | 46.6 ±1.9 |
| k20+s2k | 61.8 ±1.3 | 4.3 ±0.4 | - | - | - | 47.7 ±1.9 |
| multi2 | 0.0 ±0.0 | 0.0 ±0.0 | - | - | - | 55.9 ±1.7 |
| multi3 | 0.0 ±0.1 | 0.0 ±0.0 | - | - | - | 71.4 ±1.5 |
| dens+est_hrd | 100.0 ±0.0 | 99.7 ±0.2 | - | - | - | 35.8 ±1.7 |
| dens+disj3 | 99.9 ±0.1 | 88.2 ±0.7 | - | - | - | 93.3 ±0.9 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | - | - | - | 74.1 ±1.4 |
| dens+k20+s2k | 65.6 ±1.2 | 13.1 ±0.7 | - | - | - | 48.0 ±1.9 |
| est_hrd+disj3 | 100.0 ±0.0 | 14.0 ±0.8 | - | - | - | 92.8 ±0.9 |
| dens+est_hrd+disj3 | 100.0 ±0.0 | 87.9 ±0.7 | - | - | - | 93.0 ±0.9 |
| srv50 | 9.8 ±1.5 | 0.0 ±0.0 | - | - | - | 33.3 ±1.7 |
| srv100 | 99.8 ±0.1 | 0.0 ±0.0 | - | - | - | 33.3 ±1.7 |
| srv100+dens | 99.9 ±0.1 | 99.7 ±0.2 | - | - | - | 35.8 ±1.7 |
| srv100+est_hrd | 100.0 ±0.0 | 0.0 ±0.0 | - | - | - | 33.3 ±1.7 |

**S=10000, m=50**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged |
|---|---|---|---|---|---|---|
| base | 0.1 ±0.1 | 0.0 ±0.0 | - | - | - | 26.9 ±1.6 |
| dens | 99.9 ±0.1 | 99.4 ±0.3 | - | - | - | 28.8 ±1.6 |
| est_tab | 2.1 ±0.4 | 0.7 ±0.2 | - | - | - | 38.0 ±1.8 |
| est_hrd | 100.0 ±0.0 | 0.0 ±0.0 | - | - | - | 26.9 ±1.6 |
| disj2 | 3.2 ±0.7 | 0.5 ±0.1 | - | - | - | 61.6 ±1.8 |
| disj3 | 32.4 ±2.0 | 3.5 ±0.4 | - | - | - | 83.5 ±1.4 |
| disj4 | 85.0 ±1.3 | 11.5 ±0.7 | - | - | - | 96.0 ±0.7 |
| k20 | 49.6 ±1.4 | 1.3 ±0.2 | - | - | - | 27.9 ±1.5 |
| s2k | 0.1 ±0.1 | 0.0 ±0.0 | - | - | - | 36.0 ±1.7 |
| k20+s2k | 49.6 ±1.4 | 1.3 ±0.2 | - | - | - | 36.7 ±1.7 |
| multi2 | 0.1 ±0.1 | 0.0 ±0.0 | - | - | - | 45.4 ±1.7 |
| multi3 | 0.1 ±0.1 | 0.0 ±0.0 | - | - | - | 59.7 ±1.6 |
| dens+est_hrd | 100.0 ±0.0 | 99.3 ±0.3 | - | - | - | 28.6 ±1.6 |
| dens+disj3 | 99.9 ±0.1 | 89.9 ±0.8 | - | - | - | 84.4 ±1.3 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | - | - | - | 62.4 ±1.6 |
| dens+k20+s2k | 41.5 ±1.1 | 6.6 ±0.5 | - | - | - | 37.1 ±1.7 |
| est_hrd+disj3 | 100.0 ±0.0 | 3.6 ±0.4 | - | - | - | 82.9 ±1.4 |
| dens+est_hrd+disj3 | 100.0 ±0.0 | 89.6 ±0.8 | - | - | - | 84.0 ±1.3 |
| srv50 | 9.3 ±1.4 | 0.0 ±0.0 | - | - | - | 26.9 ±1.6 |
| srv100 | 99.9 ±0.1 | 0.0 ±0.0 | - | - | - | 26.9 ±1.6 |
| srv100+dens | 99.9 ±0.1 | 99.3 ±0.3 | - | - | - | 28.8 ±1.6 |
| srv100+est_hrd | 100.0 ±0.0 | 0.0 ±0.0 | - | - | - | 26.9 ±1.6 |

### Message cost (mean /find per search; per publish /find + /provide)

| defence | S=1000 no attack search | S=1000 no attack publish (find+provide) | S=1000 m=20 tight/aged search | S=1000 m=20 tight/aged publish (find+provide) | S=10000 no attack search | S=10000 no attack publish (find+provide) | S=10000 m=20 tight/aged search | S=10000 m=20 tight/aged publish (find+provide) |
|---|---|---|---|---|---|---|---|---|
| base | 4.0 | 4.0+10.0 | 4.1 | 4.0+10.0 | 4.7 | 4.7+10.0 | 4.7 | 4.7+10.0 |
| dens | 4.4 | 4.3+10.5 | 17.0 | 16.8+27.3 | 5.0 | 5.1+10.5 | 17.9 | 17.9+28.0 |
| est_tab | 5.9 | 5.9+12.4 | 6.1 | 6.0+12.5 | 6.7 | 6.7+12.9 | 6.7 | 6.7+12.9 |
| est_hrd | 4.0 | 4.0+10.0 | 4.1 | 4.0+10.0 | 4.7 | 4.7+10.0 | 4.7 | 4.7+10.0 |
| disj2 | 8.1 | 8.0+13.5 | 8.2 | 8.2+13.7 | 9.5 | 9.5+13.5 | 9.6 | 9.5+13.6 |
| disj3 | 12.2 | 12.1+19.3 | 12.3 | 12.3+19.6 | 14.4 | 14.3+19.8 | 14.4 | 14.4+20.1 |
| disj4 | 16.4 | 16.3+26.4 | 16.4 | 16.4+27.2 | 19.3 | 19.3+28.3 | 19.3 | 19.3+28.9 |
| k20 | 4.0 | 4.0+19.6 | 4.1 | 4.0+19.8 | 4.7 | 4.7+20.0 | 4.7 | 4.7+20.0 |
| s2k | 11.4 | 4.0+10.0 | 10.7 | 4.0+10.0 | 12.1 | 4.7+10.0 | 11.3 | 4.7+10.0 |
| k20+s2k | 11.4 | 4.0+19.6 | 10.7 | 4.0+19.8 | 12.1 | 4.7+20.0 | 11.3 | 4.7+20.0 |
| multi2 | 4.0 | 8.0+20.0 | 8.1 | 8.1+20.0 | 4.7 | 9.4+20.0 | 9.5 | 9.5+20.0 |
| multi3 | 4.0 | 11.9+30.0 | 12.2 | 12.2+30.0 | 4.7 | 14.0+30.0 | 14.2 | 14.2+30.0 |
| dens+est_hrd | 4.4 | 4.4+10.6 | 17.0 | 17.0+27.4 | 5.0 | 5.1+10.6 | 17.9 | 17.9+28.0 |
| dens+disj3 | 12.2 | 12.2+19.4 | 24.8 | 24.5+34.8 | 14.4 | 14.5+19.9 | 28.1 | 27.9+37.6 |
| dens+multi3 | 4.4 | 13.0+31.6 | 17.3 | 50.6+81.7 | 5.0 | 15.2+31.8 | 18.0 | 53.5+83.7 |
| dens+k20+s2k | 11.4 | 4.4+19.8 | 10.7 | 16.9+37.2 | 12.1 | 5.1+20.1 | 11.3 | 17.9+37.9 |
| est_hrd+disj3 | 12.2 | 12.1+19.3 | 12.3 | 12.3+19.6 | 14.3 | 14.4+19.7 | 14.4 | 14.4+20.1 |
| dens+est_hrd+disj3 | 12.2 | 12.2+19.3 | 24.8 | 24.3+34.5 | 14.4 | 14.4+19.9 | 28.0 | 28.3+38.1 |
| srv50 | 4.0 | 4.0+10.0 | 4.1 | 4.0+10.0 | 4.7 | 4.7+10.0 | 4.7 | 4.7+10.0 |
| srv100 | 4.0 | 4.0+10.0 | 4.1 | 4.0+10.0 | 4.7 | 4.7+10.0 | 4.7 | 4.7+10.0 |
| srv100+dens | 4.4 | 4.4+10.6 | 17.0 | 16.9+27.3 | 5.0 | 5.0+10.5 | 17.9 | 18.0+28.0 |
| srv100+est_hrd | 4.0 | 4.0+10.0 | 4.1 | 4.0+10.0 | 4.7 | 4.7+10.0 | 4.7 | 4.7+10.0 |

| defence | S=100 no attack search | S=100 no attack publish (find+provide) | S=100 m=20 tight/aged search | S=100 m=20 tight/aged publish (find+provide) | S=300 no attack search | S=300 no attack publish (find+provide) | S=300 m=20 tight/aged search | S=300 m=20 tight/aged publish (find+provide) |
|---|---|---|---|---|---|---|---|---|
| base | 3.3 | 3.3+10.0 | 3.5 | 3.5+10.0 | 3.6 | 3.6+10.0 | 3.8 | 3.8+10.0 |
| dens | 3.6 | 3.6+10.4 | 11.7 | 11.8+19.5 | 3.9 | 3.9+10.4 | 15.3 | 15.3+25.0 |
| est_tab | 4.7 | 4.7+10.6 | 5.4 | 5.4+11.4 | 5.4 | 5.5+11.7 | 5.7 | 5.7+12.0 |
| est_hrd | 3.3 | 3.3+10.0 | 3.5 | 3.5+10.0 | 3.6 | 3.6+10.0 | 3.8 | 3.8+10.0 |
| disj2 | 6.6 | 6.6+13.2 | 7.1 | 7.1+14.0 | 7.3 | 7.3+13.6 | 7.5 | 7.6+13.8 |
| disj3 | 9.8 | 9.8+16.0 | 10.7 | 10.7+18.0 | 11.0 | 11.0+18.1 | 11.4 | 11.4+18.9 |
| disj4 | 13.0 | 13.0+17.8 | 14.1 | 14.1+21.5 | 14.7 | 14.8+22.7 | 15.1 | 15.1+24.7 |
| k20 | 3.3 | 3.3+16.4 | 3.5 | 3.5+18.8 | 3.6 | 3.6+18.8 | 3.8 | 3.8+19.4 |
| s2k | 10.7 | 3.3+10.0 | 10.1 | 3.5+10.0 | 11.3 | 3.6+10.0 | 10.4 | 3.8+10.0 |
| k20+s2k | 10.7 | 3.3+16.4 | 10.1 | 3.5+18.8 | 11.3 | 3.6+18.8 | 10.4 | 3.8+19.4 |
| multi2 | 3.3 | 6.6+20.0 | 7.1 | 7.1+20.0 | 3.6 | 7.3+20.0 | 7.5 | 7.5+20.0 |
| multi3 | 3.3 | 9.9+30.0 | 10.6 | 10.7+30.0 | 3.6 | 10.9+30.0 | 11.3 | 11.3+30.0 |
| dens+est_hrd | 3.6 | 3.6+10.4 | 11.7 | 11.8+19.5 | 3.9 | 4.0+10.5 | 15.3 | 15.4+25.0 |
| dens+disj3 | 9.8 | 9.9+16.0 | 14.3 | 14.4+22.0 | 11.0 | 11.1+18.2 | 20.9 | 21.0+30.4 |
| dens+multi3 | 3.6 | 10.5+30.9 | 16.0 | 35.0+58.2 | 3.9 | 11.9+31.5 | 16.7 | 45.7+74.7 |
| dens+k20+s2k | 10.7 | 3.6+16.5 | 10.1 | 11.8+28.9 | 11.3 | 3.9+18.9 | 10.4 | 15.3+34.9 |
| est_hrd+disj3 | 9.8 | 9.8+16.0 | 10.7 | 10.7+18.1 | 11.0 | 11.0+18.1 | 11.4 | 11.4+18.8 |
| dens+est_hrd+disj3 | 9.8 | 9.8+16.0 | 14.3 | 14.5+22.0 | 11.0 | 11.1+18.2 | 20.9 | 21.0+30.2 |
| srv50 | 3.3 | 3.3+10.0 | 3.5 | 3.5+10.0 | 3.6 | 3.6+10.0 | 3.8 | 3.8+10.0 |
| srv100 | 3.3 | 3.3+10.0 | 3.5 | 3.5+10.0 | 3.6 | 3.6+10.0 | 3.8 | 3.8+10.0 |
| srv100+dens | 3.6 | 3.6+10.4 | 11.7 | 11.8+19.5 | 3.9 | 3.9+10.4 | 15.3 | 15.3+25.0 |
| srv100+est_hrd | 3.3 | 3.3+10.0 | 3.5 | 3.5+10.0 | 3.6 | 3.6+10.0 | 3.8 | 3.8+10.0 |


## Parameter sweep, alpha=3 (sweep.jsonl)



**S=1000, m=20**

| defence | tight/fresh | tight/aged | ring/aged | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|
| base | 0.4 ±0.1 | 0.2 ±0.1 | 33.0 ±1.7 | 68.6 ±1.8 | 6.9 | 16.9 |
| dens | 98.8 ±0.3 | 97.5 ±0.5 | 81.3 ±1.6 | 72.1 ±1.6 | 16.8 | 41.4 |
| dens_T3 | 99.0 ±0.3 | 97.7 ±0.5 | 58.4 ±1.9 | 69.2 ±1.7 | 13.7 | 34.0 |
| dens_rank | 89.4 ±0.9 | 83.0 ±1.1 | 77.3 ±1.7 | 71.7 ±1.7 | 13.2 | 31.1 |
| dens_radius | 98.7 ±0.4 | 96.2 ±0.7 | 74.7 ±1.7 | 72.2 ±1.6 | 11.4 | 29.4 |
| dens_w5 | 98.7 ±0.3 | 97.5 ±0.5 | 80.5 ±1.6 | 72.9 ±1.6 | 16.8 | 34.6 |
| dens_w20 | 98.8 ±0.4 | 97.7 ±0.5 | 82.4 ±1.5 | 72.0 ±1.7 | 16.8 | 49.7 |
| dens_b3 | 98.5 ±0.4 | 96.4 ±0.6 | 77.7 ±1.7 | 72.6 ±1.6 | 13.1 | 37.0 |
| dens_b8 | 98.8 ±0.3 | 98.1 ±0.4 | 81.2 ±1.6 | 72.2 ±1.6 | 21.2 | 45.8 |
| k15 | 20.8 ±1.5 | 2.9 ±0.4 | 35.7 ±1.7 | 69.6 ±1.7 | 6.9 | 21.9 |
| k20 | 75.1 ±1.1 | 11.8 ±0.7 | 39.3 ±1.6 | 70.7 ±1.6 | 6.9 | 26.8 |
| k30 | 91.3 ±0.7 | 21.0 ±0.9 | 44.6 ±1.6 | 72.3 ±1.6 | 6.9 | 35.2 |
| s15 | 0.4 ±0.2 | 0.2 ±0.1 | 59.0 ±2.0 | 86.9 ±1.4 | 15.8 | 16.9 |
| s2k | 0.4 ±0.2 | 0.2 ±0.1 | 60.1 ±2.0 | 87.5 ±1.4 | 17.0 | 16.9 |
| s30 | 0.5 ±0.2 | 0.2 ±0.1 | 60.2 ±2.1 | 87.4 ±1.4 | 18.4 | 16.9 |
| k15+s15 | 21.6 ±1.6 | 4.1 ±0.4 | 62.4 ±1.9 | 87.0 ±1.4 | 15.8 | 21.9 |
| k20+s2k | 78.5 ±1.0 | 19.8 ±0.9 | 66.1 ±1.7 | 88.0 ±1.3 | 17.0 | 26.8 |
| k30+s30 | 93.6 ±0.6 | 32.1 ±1.1 | 70.6 ±1.6 | 88.7 ±1.3 | 18.4 | 35.2 |
| disj2 | 11.1 ±1.3 | 2.9 ±0.4 | 71.9 ±1.6 | 94.7 ±0.8 | 14.1 | 28.4 |
| disj3 | 66.7 ±2.0 | 23.4 ±1.0 | 91.7 ±1.0 | 99.2 ±0.3 | 21.5 | 41.0 |
| disj4 | 98.1 ±0.4 | 64.5 ±1.1 | 99.0 ±0.3 | 99.9 ±0.1 | 27.8 | 53.6 |
| est_tab | 14.8 ±1.1 | 3.5 ±0.4 | 38.4 ±1.8 | 74.1 ±1.7 | 6.9 | 19.6 |
| est_tab5 | 20.1 ±1.2 | 8.0 ±0.6 | 51.7 ±1.9 | 82.6 ±1.4 | 9.0 | 23.6 |
| dens+disj2 | 99.0 ±0.3 | 97.1 ±0.4 | 90.2 ±1.0 | 95.2 ±0.8 | 26.5 | 58.5 |
| dens+disj3 | 99.5 ±0.2 | 95.8 ±0.5 | 96.5 ±0.5 | 99.3 ±0.3 | 33.0 | 69.0 |
| dens+est_hrd | 99.9 ±0.1 | 97.6 ±0.5 | 80.5 ±1.6 | 73.2 ±1.6 | 17.2 | 43.1 |
| dens+est_hrd+disj2 | 99.9 ±0.1 | 97.1 ±0.5 | 90.7 ±1.0 | 95.3 ±0.7 | 27.0 | 60.1 |
| dens+multi2 | 100.0 ±0.0 | 99.9 ±0.1 | 96.7 ±0.6 | 92.2 ±0.9 | 18.5 | 82.9 |

**S=1000, m=50**

| defence | tight/fresh | tight/aged | ring/aged | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|
| base | 0.3 ±0.1 | 0.2 ±0.1 | 6.4 ±0.8 | 51.8 ±1.9 | 7.0 | 16.9 |
| dens | 98.8 ±0.3 | 93.7 ±0.8 | 78.9 ±1.3 | 57.6 ±1.8 | 18.8 | 43.7 |
| dens_T3 | 99.1 ±0.3 | 93.6 ±0.8 | 65.9 ±1.6 | 53.0 ±1.9 | 16.9 | 40.2 |
| dens_rank | 83.5 ±1.2 | 54.8 ±1.4 | 63.3 ±1.5 | 56.4 ±1.9 | 14.1 | 31.6 |
| dens_radius | 98.7 ±0.4 | 87.6 ±1.1 | 44.7 ±1.6 | 56.2 ±1.9 | 11.9 | 30.0 |
| dens_w5 | 98.6 ±0.4 | 93.2 ±0.8 | 70.9 ±1.5 | 57.3 ±1.8 | 18.8 | 36.6 |
| dens_w20 | 98.8 ±0.3 | 94.0 ±0.8 | 83.3 ±1.2 | 58.1 ±1.8 | 18.8 | 53.2 |
| dens_b3 | 98.6 ±0.4 | 88.5 ±1.1 | 62.0 ±1.5 | 56.5 ±1.8 | 14.2 | 38.5 |
| dens_b8 | 98.8 ±0.3 | 95.4 ±0.6 | 83.9 ±1.2 | 58.1 ±1.8 | 24.4 | 49.3 |
| k15 | 3.8 ±0.7 | 0.6 ±0.2 | 7.8 ±0.8 | 52.8 ±1.9 | 7.0 | 21.9 |
| k20 | 29.2 ±1.8 | 2.8 ±0.4 | 10.2 ±0.8 | 53.5 ±1.8 | 7.0 | 26.8 |
| k30 | 83.9 ±1.0 | 7.9 ±0.6 | 13.4 ±0.9 | 55.5 ±1.8 | 7.0 | 34.4 |
| s15 | 0.3 ±0.1 | 0.2 ±0.1 | 13.2 ±1.2 | 71.5 ±1.9 | 15.5 | 16.9 |
| s2k | 0.3 ±0.1 | 0.2 ±0.1 | 13.5 ±1.2 | 71.5 ±1.9 | 16.3 | 16.9 |
| s30 | 0.3 ±0.1 | 0.2 ±0.1 | 13.9 ±1.2 | 72.0 ±1.9 | 17.1 | 16.9 |
| k15+s15 | 3.9 ±0.7 | 0.7 ±0.2 | 15.6 ±1.2 | 72.0 ±1.9 | 15.5 | 21.9 |
| k20+s2k | 30.8 ±1.9 | 3.4 ±0.4 | 19.1 ±1.2 | 73.0 ±1.8 | 16.3 | 26.8 |
| k30+s30 | 86.5 ±1.0 | 9.4 ±0.7 | 22.5 ±1.2 | 74.2 ±1.7 | 17.1 | 34.4 |
| disj2 | 2.8 ±0.6 | 0.7 ±0.2 | 22.2 ±1.4 | 86.0 ±1.3 | 14.3 | 28.6 |
| disj3 | 24.6 ±1.9 | 3.6 ±0.4 | 42.6 ±1.7 | 96.1 ±0.7 | 21.8 | 42.0 |
| disj4 | 66.9 ±2.1 | 16.0 ±0.9 | 66.4 ±1.6 | 99.2 ±0.3 | 28.0 | 55.7 |
| est_tab | 15.3 ±1.1 | 1.9 ±0.3 | 8.4 ±0.9 | 57.8 ±1.9 | 7.0 | 19.6 |
| est_tab5 | 20.7 ±1.2 | 5.0 ±0.5 | 15.1 ±1.1 | 70.0 ±1.8 | 9.0 | 23.7 |
| dens+disj2 | 99.0 ±0.3 | 95.6 ±0.6 | 88.1 ±1.1 | 87.6 ±1.2 | 31.8 | 68.2 |
| dens+disj3 | 99.1 ±0.2 | 95.2 ±0.6 | 87.2 ±1.1 | 97.0 ±0.6 | 39.8 | 83.4 |
| dens+est_hrd | 99.8 ±0.1 | 93.6 ±0.8 | 78.9 ±1.3 | 57.7 ±1.8 | 19.3 | 45.5 |
| dens+est_hrd+disj2 | 99.8 ±0.1 | 95.5 ±0.6 | 87.8 ±1.1 | 87.4 ±1.2 | 32.5 | 70.2 |
| dens+multi2 | 100.0 ±0.0 | 99.5 ±0.2 | 94.0 ±0.7 | 81.4 ±1.3 | 21.5 | 87.7 |

S=1000, no attack:

| defence | P(find) | /find per search | publish find+provide |
|---|---|---|---|
| base | 100.0 | 6.30 | 6.3+10.0 |
| dens | 100.0 | 6.66 | 6.6+10.5 |
| dens_T3 | 100.0 | 6.32 | 6.3+10.1 |
| dens_rank | 100.0 | 6.58 | 6.6+10.4 |
| dens_radius | 100.0 | 6.56 | 6.5+10.4 |
| dens_w5 | 100.0 | 6.67 | 6.7+10.3 |
| dens_w20 | 100.0 | 6.68 | 6.7+10.9 |
| dens_b3 | 100.0 | 6.55 | 6.5+10.6 |
| dens_b8 | 100.0 | 6.83 | 6.8+10.6 |
| k15 | 100.0 | 6.30 | 6.3+14.9 |
| k20 | 100.0 | 6.30 | 6.3+19.5 |
| k30 | 100.0 | 6.30 | 6.3+26.8 |
| s15 | 100.0 | 15.15 | 6.3+10.0 |
| s2k | 100.0 | 16.51 | 6.3+10.0 |
| s30 | 100.0 | 18.15 | 6.3+10.0 |
| k15+s15 | 100.0 | 15.15 | 6.3+14.9 |
| k20+s2k | 100.0 | 16.51 | 6.3+19.5 |
| k30+s30 | 100.0 | 18.15 | 6.3+26.8 |
| disj2 | 100.0 | 12.97 | 12.9+13.9 |
| disj3 | 100.0 | 19.70 | 19.6+19.4 |
| disj4 | 100.0 | 25.27 | 25.1+26.3 |
| est_tab | 100.0 | 6.30 | 6.3+12.4 |
| est_tab5 | 100.0 | 8.30 | 8.3+14.3 |
| dens+disj2 | 100.0 | 13.10 | 13.1+14.1 |
| dens+disj3 | 100.0 | 19.74 | 19.7+19.5 |
| dens+est_hrd | 100.0 | 6.66 | 6.7+10.6 |
| dens+est_hrd+disj2 | 100.0 | 13.09 | 13.1+14.1 |
| dens+multi2 | 100.0 | 6.67 | 13.2+21.0 |

**S=10000, m=20**

| defence | tight/fresh | tight/aged | ring/aged | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|
| base | 0.1 ±0.0 | 0.0 ±0.0 | 28.0 ±1.6 | 60.8 ±1.9 | 8.2 | 18.2 |
| dens | 99.9 ±0.1 | 99.6 ±0.2 | 79.2 ±1.7 | 64.1 ±1.7 | 18.6 | 43.0 |
| dens_T3 | 99.8 ±0.1 | 99.6 ±0.3 | 51.0 ±2.0 | 60.6 ±1.8 | 15.3 | 35.6 |
| dens_rank | 89.6 ±0.9 | 88.1 ±0.9 | 75.7 ±1.7 | 61.7 ±1.8 | 14.9 | 32.9 |
| dens_radius | 99.9 ±0.1 | 99.2 ±0.3 | 72.0 ±1.7 | 62.6 ±1.8 | 13.0 | 30.9 |
| dens_w5 | 99.9 ±0.1 | 99.7 ±0.1 | 78.8 ±1.7 | 63.5 ±1.7 | 18.5 | 36.2 |
| dens_w20 | 99.9 ±0.1 | 99.7 ±0.2 | 79.6 ±1.7 | 63.2 ±1.8 | 18.6 | 53.3 |
| dens_b3 | 99.8 ±0.1 | 99.4 ±0.2 | 76.2 ±1.7 | 62.7 ±1.8 | 14.7 | 38.7 |
| dens_b8 | 99.9 ±0.1 | 99.8 ±0.1 | 79.6 ±1.7 | 63.6 ±1.7 | 23.2 | 48.1 |
| k15 | 21.5 ±1.5 | 2.7 ±0.3 | 29.9 ±1.5 | 62.5 ±1.8 | 8.2 | 23.2 |
| k20 | 72.8 ±1.1 | 10.1 ±0.6 | 33.3 ±1.5 | 63.2 ±1.7 | 8.2 | 28.2 |
| k30 | 86.5 ±0.9 | 16.6 ±0.8 | 37.2 ±1.5 | 64.6 ±1.7 | 8.2 | 37.9 |
| s15 | 0.1 ±0.0 | 0.1 ±0.1 | 53.0 ±2.0 | 79.5 ±1.8 | 17.2 | 18.2 |
| s2k | 0.1 ±0.0 | 0.1 ±0.0 | 55.3 ±2.0 | 80.5 ±1.7 | 18.7 | 18.2 |
| s30 | 0.1 ±0.1 | 0.1 ±0.1 | 56.0 ±2.0 | 81.3 ±1.7 | 20.9 | 18.2 |
| k15+s15 | 22.6 ±1.6 | 4.4 ±0.5 | 55.4 ±1.9 | 81.3 ±1.6 | 17.2 | 23.2 |
| k20+s2k | 75.6 ±1.1 | 22.3 ±0.9 | 62.2 ±1.7 | 82.8 ±1.5 | 18.7 | 28.2 |
| k30+s30 | 90.6 ±0.7 | 35.1 ±1.1 | 66.1 ±1.6 | 84.9 ±1.4 | 20.9 | 37.9 |
| disj2 | 10.2 ±1.3 | 2.9 ±0.4 | 68.3 ±1.7 | 93.1 ±0.9 | 16.9 | 31.1 |
| disj3 | 67.6 ±2.0 | 24.0 ±1.0 | 92.2 ±0.8 | 99.0 ±0.3 | 25.7 | 45.2 |
| disj4 | 98.1 ±0.4 | 64.4 ±1.1 | 99.5 ±0.2 | 99.9 ±0.1 | 33.6 | 59.5 |
| est_tab | 2.9 ±0.5 | 1.0 ±0.2 | 29.2 ±1.6 | 62.6 ±1.8 | 8.2 | 21.1 |
| est_tab5 | 4.0 ±0.6 | 1.6 ±0.3 | 33.3 ±1.7 | 67.9 ±1.7 | 10.2 | 25.1 |
| dens+disj2 | 99.9 ±0.1 | 98.7 ±0.3 | 88.9 ±1.1 | 93.5 ±0.9 | 29.7 | 62.2 |
| dens+disj3 | 99.9 ±0.1 | 97.9 ±0.3 | 96.4 ±0.5 | 99.3 ±0.3 | 38.1 | 75.2 |
| dens+est_hrd | 100.0 ±0.0 | 99.7 ±0.1 | 79.1 ±1.7 | 64.5 ±1.7 | 18.9 | 44.8 |
| dens+est_hrd+disj2 | 100.0 ±0.0 | 98.6 ±0.3 | 89.3 ±1.1 | 92.8 ±0.9 | 30.4 | 63.9 |
| dens+multi2 | 100.0 ±0.0 | 100.0 ±0.0 | 95.8 ±0.8 | 86.8 ±1.1 | 20.7 | 86.4 |

**S=10000, m=50**

| defence | tight/fresh | tight/aged | ring/aged | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|
| base | 0.1 ±0.0 | 0.0 ±0.0 | 5.1 ±0.7 | 48.3 ±1.8 | 8.2 | 18.2 |
| dens | 99.9 ±0.1 | 99.6 ±0.2 | 85.9 ±1.1 | 52.0 ±1.8 | 21.0 | 46.9 |
| dens_T3 | 99.9 ±0.1 | 99.2 ±0.3 | 69.8 ±1.6 | 49.3 ±1.8 | 18.8 | 42.9 |
| dens_rank | 87.8 ±0.9 | 70.8 ±1.3 | 76.1 ±1.3 | 50.6 ±1.8 | 16.1 | 34.3 |
| dens_radius | 99.8 ±0.1 | 96.2 ±0.6 | 46.8 ±1.6 | 51.8 ±1.8 | 13.5 | 31.8 |
| dens_w5 | 99.8 ±0.1 | 99.4 ±0.2 | 79.0 ±1.3 | 52.0 ±1.8 | 21.0 | 39.1 |
| dens_w20 | 99.9 ±0.1 | 99.7 ±0.2 | 89.8 ±0.9 | 53.4 ±1.8 | 21.0 | 60.3 |
| dens_b3 | 99.7 ±0.1 | 98.3 ±0.4 | 68.6 ±1.4 | 51.8 ±1.8 | 16.1 | 41.5 |
| dens_b8 | 99.9 ±0.1 | 99.8 ±0.1 | 90.7 ±0.9 | 51.9 ±1.8 | 27.3 | 53.4 |
| k15 | 4.3 ±0.8 | 0.4 ±0.1 | 6.3 ±0.7 | 49.6 ±1.8 | 8.2 | 23.2 |
| k20 | 32.8 ±1.8 | 2.4 ±0.3 | 8.6 ±0.7 | 50.2 ±1.8 | 8.2 | 28.2 |
| k30 | 80.9 ±1.1 | 6.9 ±0.5 | 12.6 ±0.8 | 51.7 ±1.7 | 8.2 | 37.9 |
| s15 | 0.1 ±0.0 | 0.0 ±0.0 | 10.8 ±1.1 | 69.5 ±2.0 | 17.0 | 18.2 |
| s2k | 0.1 ±0.0 | 0.0 ±0.0 | 11.4 ±1.2 | 70.5 ±1.9 | 18.1 | 18.2 |
| s30 | 0.0 ±0.0 | 0.0 ±0.0 | 12.2 ±1.2 | 71.4 ±1.9 | 19.5 | 18.2 |
| k15+s15 | 4.8 ±0.8 | 0.5 ±0.2 | 13.4 ±1.1 | 71.2 ±1.9 | 17.0 | 23.2 |
| k20+s2k | 34.2 ±1.9 | 3.6 ±0.4 | 18.1 ±1.1 | 71.9 ±1.8 | 18.1 | 28.2 |
| k30+s30 | 84.3 ±1.0 | 10.3 ±0.6 | 24.5 ±1.2 | 74.7 ±1.7 | 19.5 | 37.9 |
| disj2 | 2.8 ±0.7 | 0.4 ±0.1 | 19.6 ±1.3 | 86.0 ±1.3 | 17.0 | 31.2 |
| disj3 | 24.2 ±2.0 | 4.3 ±0.5 | 40.7 ±1.7 | 96.5 ±0.6 | 25.9 | 46.0 |
| disj4 | 67.5 ±2.1 | 16.5 ±0.9 | 68.0 ±1.5 | 99.8 ±0.1 | 33.6 | 61.7 |
| est_tab | 3.0 ±0.5 | 0.8 ±0.2 | 5.6 ±0.7 | 51.8 ±1.8 | 8.2 | 21.2 |
| est_tab5 | 3.7 ±0.6 | 1.4 ±0.2 | 7.5 ±0.8 | 56.9 ±1.8 | 10.3 | 25.2 |
| dens+disj2 | 99.9 ±0.1 | 99.6 ±0.2 | 90.7 ±1.0 | 86.6 ±1.2 | 36.0 | 74.4 |
| dens+disj3 | 99.9 ±0.1 | 99.1 ±0.2 | 90.0 ±0.9 | 97.2 ±0.6 | 45.7 | 91.4 |
| dens+est_hrd | 100.0 ±0.0 | 99.6 ±0.2 | 85.7 ±1.0 | 52.3 ±1.8 | 21.5 | 48.7 |
| dens+est_hrd+disj2 | 100.0 ±0.0 | 99.6 ±0.2 | 90.1 ±1.1 | 86.6 ±1.2 | 36.9 | 76.9 |
| dens+multi2 | 100.0 ±0.0 | 100.0 ±0.0 | 98.1 ±0.3 | 76.4 ±1.5 | 23.3 | 93.8 |

S=10000, no attack:

| defence | P(find) | /find per search | publish find+provide |
|---|---|---|---|
| base | 100.0 | 7.70 | 7.7+10.0 |
| dens | 100.0 | 8.01 | 8.1+10.5 |
| dens_T3 | 100.0 | 7.74 | 7.7+10.0 |
| dens_rank | 100.0 | 7.94 | 7.9+10.4 |
| dens_radius | 100.0 | 7.94 | 7.9+10.3 |
| dens_w5 | 100.0 | 8.02 | 8.0+10.2 |
| dens_w20 | 100.0 | 8.03 | 8.0+10.8 |
| dens_b3 | 100.0 | 7.91 | 8.0+10.4 |
| dens_b8 | 100.0 | 8.14 | 8.2+10.5 |
| k15 | 100.0 | 7.70 | 7.7+15.0 |
| k20 | 100.0 | 7.70 | 7.8+20.0 |
| k30 | 100.0 | 7.70 | 7.7+29.7 |
| s15 | 100.0 | 16.68 | 7.7+10.0 |
| s2k | 100.0 | 18.54 | 7.7+10.0 |
| s30 | 100.0 | 21.88 | 7.7+10.0 |
| k15+s15 | 100.0 | 16.68 | 7.7+15.0 |
| k20+s2k | 100.0 | 18.54 | 7.8+20.0 |
| k30+s30 | 100.0 | 21.88 | 7.7+29.7 |
| disj2 | 100.0 | 15.87 | 15.9+13.8 |
| disj3 | 100.0 | 24.12 | 24.2+19.7 |
| disj4 | 100.0 | 31.51 | 31.6+27.1 |
| est_tab | 100.0 | 7.71 | 7.7+12.9 |
| est_tab5 | 100.0 | 9.71 | 9.7+14.9 |
| dens+disj2 | 100.0 | 16.01 | 16.0+14.0 |
| dens+disj3 | 100.0 | 24.24 | 24.2+19.8 |
| dens+est_hrd | 100.0 | 8.01 | 8.0+10.4 |
| dens+est_hrd+disj2 | 100.0 | 16.02 | 16.0+14.0 |
| dens+multi2 | 100.0 | 8.01 | 16.1+21.0 |


## Parameter sweep, alpha=1 (slow nodes) (sweep_a1.jsonl)



**S=1000, m=20**

| defence | tight/fresh | tight/aged | ring/aged | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|
| base | 0.1 ±0.1 | 0.1 ±0.0 | 11.0 ±1.0 | 33.6 ±1.7 | 4.2 | 14.2 |
| dens | 98.8 ±0.3 | 97.5 ±0.5 | 69.5 ±2.0 | 36.3 ±1.7 | 14.0 | 37.6 |
| dens_b3 | 98.7 ±0.4 | 95.4 ±0.7 | 64.8 ±2.0 | 35.6 ±1.7 | 10.4 | 33.2 |
| dens_b8 | 98.8 ±0.3 | 98.0 ±0.4 | 70.3 ±2.0 | 36.8 ±1.7 | 18.5 | 42.4 |
| k20 | 62.1 ±1.3 | 3.7 ±0.4 | 13.4 ±1.0 | 34.8 ±1.7 | 4.2 | 24.1 |
| k20+s2k | 62.1 ±1.3 | 3.7 ±0.4 | 23.8 ±1.4 | 46.9 ±1.9 | 11.8 | 24.1 |
| disj2 | 15.4 ±1.4 | 2.0 ±0.3 | 39.4 ±1.7 | 71.2 ±1.7 | 8.5 | 22.7 |
| disj3 | 75.4 ±1.5 | 12.6 ±0.7 | 67.3 ±1.6 | 91.1 ±1.1 | 12.9 | 32.2 |
| dens+disj2 | 98.9 ±0.3 | 88.7 ±0.9 | 68.3 ±1.6 | 72.7 ±1.6 | 19.9 | 49.5 |
| dens+disj3 | 98.9 ±0.2 | 79.5 ±1.0 | 80.3 ±1.2 | 91.2 ±1.1 | 23.2 | 56.4 |
| dens+est_hrd | 99.9 ±0.1 | 97.5 ±0.5 | 68.6 ±2.0 | 36.5 ±1.7 | 14.5 | 39.3 |
| dens+est_hrd+disj2 | 99.9 ±0.1 | 88.4 ±1.0 | 69.6 ±1.6 | 72.9 ±1.6 | 20.8 | 51.7 |
| dens+multi2 | 100.0 ±0.0 | 99.9 ±0.1 | 90.1 ±1.3 | 58.9 ±1.7 | 16.4 | 74.8 |

**S=1000, m=50**

| defence | tight/fresh | tight/aged | ring/aged | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|
| base | 0.1 ±0.1 | 0.1 ±0.0 | 1.5 ±0.3 | 25.3 ±1.5 | 4.3 | 14.3 |
| dens | 98.6 ±0.4 | 93.7 ±0.9 | 76.3 ±1.4 | 29.7 ±1.6 | 16.1 | 40.6 |
| dens_b3 | 98.4 ±0.4 | 88.0 ±1.2 | 56.2 ±1.6 | 28.6 ±1.6 | 11.4 | 35.2 |
| dens_b8 | 98.6 ±0.3 | 95.7 ±0.7 | 87.5 ±1.1 | 30.3 ±1.6 | 22.0 | 46.9 |
| k20 | 50.7 ±1.4 | 0.8 ±0.2 | 2.6 ±0.4 | 26.2 ±1.5 | 4.3 | 24.2 |
| k20+s2k | 50.7 ±1.4 | 0.8 ±0.2 | 3.6 ±0.5 | 34.7 ±1.7 | 11.5 | 24.2 |
| disj2 | 3.3 ±0.7 | 0.4 ±0.1 | 6.7 ±0.7 | 55.7 ±1.9 | 8.7 | 22.8 |
| disj3 | 29.1 ±1.9 | 2.8 ±0.4 | 16.3 ±1.1 | 78.3 ±1.6 | 13.2 | 32.9 |
| dens+disj2 | 98.7 ±0.3 | 88.4 ±1.1 | 67.1 ±1.6 | 57.2 ±1.8 | 23.4 | 55.9 |
| dens+disj3 | 98.7 ±0.3 | 76.6 ±1.2 | 57.8 ±1.5 | 79.0 ±1.5 | 27.2 | 64.6 |
| dens+est_hrd | 99.8 ±0.1 | 93.6 ±0.9 | 76.6 ±1.4 | 29.6 ±1.6 | 16.7 | 42.4 |
| dens+est_hrd+disj2 | 99.8 ±0.1 | 88.5 ±1.1 | 67.2 ±1.6 | 57.4 ±1.8 | 24.6 | 58.6 |
| dens+multi2 | 100.0 ±0.0 | 98.8 ±0.4 | 94.3 ±0.7 | 49.7 ±1.7 | 18.8 | 80.9 |

S=1000, no attack:

| defence | P(find) | /find per search | publish find+provide |
|---|---|---|---|
| base | 100.0 | 3.99 | 4.0+10.0 |
| dens | 100.0 | 4.30 | 4.3+10.5 |
| dens_b3 | 100.0 | 4.18 | 4.2+10.4 |
| dens_b8 | 100.0 | 4.43 | 4.4+10.5 |
| k20 | 100.0 | 3.99 | 4.0+19.6 |
| k20+s2k | 100.0 | 11.39 | 4.0+19.6 |
| disj2 | 100.0 | 8.03 | 8.0+13.6 |
| disj3 | 100.0 | 12.12 | 12.1+19.2 |
| dens+disj2 | 100.0 | 8.15 | 8.2+13.7 |
| dens+disj3 | 100.0 | 12.19 | 12.2+19.3 |
| dens+est_hrd | 100.0 | 4.29 | 4.3+10.5 |
| dens+est_hrd+disj2 | 100.0 | 8.16 | 8.2+13.8 |
| dens+multi2 | 100.0 | 4.29 | 8.6+20.9 |

**S=10000, m=20**

| defence | tight/fresh | tight/aged | ring/aged | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|
| base | 0.0 ±0.0 | 0.0 ±0.0 | 11.3 ±1.0 | 32.1 ±1.7 | 4.9 | 14.9 |
| dens | 99.9 ±0.1 | 99.7 ±0.1 | 69.0 ±2.1 | 34.6 ±1.7 | 14.9 | 38.5 |
| dens_b3 | 99.8 ±0.1 | 98.8 ±0.3 | 65.9 ±2.0 | 34.1 ±1.7 | 11.2 | 34.2 |
| dens_b8 | 99.9 ±0.1 | 99.8 ±0.1 | 68.9 ±2.1 | 35.2 ±1.7 | 19.7 | 43.8 |
| k20 | 62.3 ±1.3 | 4.3 ±0.4 | 14.8 ±1.0 | 33.5 ±1.6 | 4.9 | 24.9 |
| k20+s2k | 62.3 ±1.3 | 4.3 ±0.4 | 25.3 ±1.4 | 47.2 ±1.8 | 12.5 | 24.9 |
| disj2 | 16.9 ±1.5 | 1.8 ±0.3 | 39.3 ±1.7 | 74.2 ±1.6 | 9.9 | 24.1 |
| disj3 | 76.3 ±1.5 | 13.5 ±0.8 | 70.0 ±1.5 | 92.8 ±0.9 | 15.0 | 34.6 |
| dens+disj2 | 99.9 ±0.1 | 94.1 ±0.6 | 72.1 ±1.6 | 75.6 ±1.6 | 21.8 | 52.9 |
| dens+disj3 | 99.8 ±0.1 | 89.2 ±0.7 | 84.8 ±1.1 | 92.7 ±0.9 | 26.2 | 61.4 |
| dens+est_hrd | 100.0 ±0.0 | 99.6 ±0.2 | 68.9 ±2.1 | 34.6 ±1.7 | 15.5 | 40.4 |
| dens+est_hrd+disj2 | 100.0 ±0.0 | 94.0 ±0.6 | 71.4 ±1.6 | 75.6 ±1.6 | 22.7 | 54.9 |
| dens+multi2 | 100.0 ±0.0 | 100.0 ±0.0 | 89.8 ±1.3 | 58.0 ±1.7 | 17.4 | 77.1 |

**S=10000, m=50**

| defence | tight/fresh | tight/aged | ring/aged | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|
| base | 0.0 ±0.0 | 0.0 ±0.0 | 1.8 ±0.4 | 25.0 ±1.5 | 4.9 | 14.9 |
| dens | 99.9 ±0.1 | 99.4 ±0.3 | 84.2 ±1.2 | 26.5 ±1.5 | 17.3 | 42.7 |
| dens_b3 | 99.6 ±0.2 | 96.5 ±0.6 | 62.6 ±1.5 | 26.3 ±1.5 | 12.4 | 37.1 |
| dens_b8 | 99.9 ±0.1 | 99.6 ±0.2 | 93.0 ±0.8 | 27.2 ±1.5 | 23.9 | 49.7 |
| k20 | 49.6 ±1.4 | 1.4 ±0.3 | 3.6 ±0.5 | 26.0 ±1.5 | 4.9 | 24.9 |
| k20+s2k | 49.6 ±1.4 | 1.4 ±0.3 | 4.8 ±0.6 | 35.0 ±1.7 | 12.2 | 24.9 |
| disj2 | 3.6 ±0.7 | 0.6 ±0.2 | 8.1 ±0.8 | 62.0 ±1.8 | 10.0 | 24.2 |
| disj3 | 30.2 ±2.0 | 3.4 ±0.4 | 20.2 ±1.3 | 85.0 ±1.3 | 15.2 | 35.4 |
| dens+disj2 | 99.9 ±0.1 | 96.8 ±0.5 | 76.2 ±1.5 | 63.0 ±1.8 | 26.6 | 61.3 |
| dens+disj3 | 99.9 ±0.1 | 90.7 ±0.8 | 71.0 ±1.4 | 85.7 ±1.3 | 31.1 | 71.7 |
| dens+est_hrd | 100.0 ±0.0 | 99.4 ±0.3 | 84.8 ±1.1 | 26.6 ±1.5 | 17.9 | 44.6 |
| dens+est_hrd+disj2 | 100.0 ±0.0 | 96.7 ±0.5 | 78.4 ±1.4 | 63.0 ±1.8 | 27.8 | 64.7 |
| dens+multi2 | 100.0 ±0.0 | 100.0 ±0.0 | 97.7 ±0.4 | 46.2 ±1.7 | 19.5 | 85.4 |

S=10000, no attack:

| defence | P(find) | /find per search | publish find+provide |
|---|---|---|---|
| base | 100.0 | 4.68 | 4.7+10.0 |
| dens | 100.0 | 5.06 | 5.2+10.7 |
| dens_b3 | 100.0 | 4.94 | 5.0+10.6 |
| dens_b8 | 100.0 | 5.27 | 5.3+10.6 |
| k20 | 100.0 | 4.68 | 4.7+20.0 |
| k20+s2k | 100.0 | 12.20 | 4.7+20.0 |
| disj2 | 100.0 | 9.48 | 9.5+13.6 |
| disj3 | 100.0 | 14.33 | 14.4+19.7 |
| dens+disj2 | 100.0 | 9.67 | 9.7+13.8 |
| dens+disj3 | 100.0 | 14.46 | 14.5+19.8 |
| dens+est_hrd | 100.0 | 5.08 | 5.1+10.6 |
| dens+est_hrd+disj2 | 100.0 | 9.68 | 9.7+13.8 |
| dens+multi2 | 100.0 | 5.09 | 10.1+21.1 |


## Escalation: disjoint paths only when needed, alpha=3 (fb_a3.jsonl)

Publisher pays once per republish; searcher runs the cheap density-checked lookup and escalates only when it found no provider (so searches for content nobody provides also pay the escalation: see 'no-attack miss cost').

* `Pd|Qd>d3`: pub dens; search dens, on failure dens+disj3
* `Pd3|Qd`: pub dens+disj3; search dens
* `Pd3|Qd>d3`: pub dens+disj3; search dens, on failure dens+disj3
* `Pd2|Qd>d2`: pub dens+disj2; search dens, on failure dens+disj2
* `Pd3|Qd>d4`: pub dens+disj3; search dens, on failure dens+disj4
* `Pd4|Qd>d4`: pub dens+disj4; search dens, on failure dens+disj4
* `Pde3|Qde>d3`: pub dens+est_hrd+disj3; search dens+est_hrd, on failure +disj3
* `Pdm2|Qdm2>d3`: pub dens+multi2; search dens on key 0, key 1, then dens+disj3
* `Pd3|Qd2`: pub dens+disj3; search dens+disj2

**Summary, S in {1000, 10000}, m in {20, 50}, 6 variants:**

| defence | mean P(find) % | worst % | gain pp | +/find per search, no attack | +msgs per publish, no attack | +/find per search, attacked | +msgs per publish, attacked | gain pp per extra msg (search + publish/10, no attack) |
|---|---|---|---|---|---|---|---|---|
| Pd4|Qd>d4 | 99.4 | 95.3 | +57.6 | +0.36 | +38.23 | +12.4 | +67.8 | 13.8 |
| Pd3|Qd>d4 | 99.2 | 93.7 | +57.3 | +0.36 | +24.72 | +12.6 | +57.6 | 20.3 |
| Pde3|Qde>d3 | 98.9 | 92.5 | +57.1 | +0.34 | +24.70 | +12.8 | +62.1 | 20.3 |
| Pd3|Qd>d3 | 98.9 | 93.0 | +57.1 | +0.36 | +24.72 | +12.2 | +57.6 | 20.1 |
| dens+multi3 | 98.8 | 89.0 | +57.0 | +0.35 | +36.80 | +12.3 | +109.3 | 14.1 |
| Pdm2|Qdm2>d3 | 98.6 | 89.5 | +56.8 | +0.37 | +18.81 | +12.7 | +67.2 | 25.3 |
| dens+disj3 | 97.9 | 88.0 | +56.1 | +15.00 | +24.72 | +29.9 | +57.6 | 3.2 |
| Pd2|Qd>d2 | 97.3 | 88.5 | +55.5 | +0.34 | +11.55 | +11.9 | +44.6 | 37.2 |
| Pd3|Qd2 | 97.0 | 88.3 | +55.2 | +7.57 | +24.72 | +21.8 | +57.6 | 5.5 |
| Pd|Qd>d3 | 95.4 | 76.9 | +53.6 | +0.36 | +0.88 | +13.2 | +24.7 | 118.9 |
| Pd3|Qd | 93.1 | 68.1 | +51.3 | +0.35 | +24.72 | +10.4 | +57.6 | 18.2 |
| dens | 89.7 | 51.7 | +47.9 | +0.35 | +0.88 | +10.4 | +24.7 | 109.7 |
| base | 41.8 | 0.0 | +0.0 | +0.00 | +0.00 | +0.0 | +0.0 | free |

**Summary, S=300, m in {20, 50}, 6 variants:**

| defence | mean P(find) % | worst % | gain pp | +/find per search, no attack | +msgs per publish, no attack | +/find per search, attacked | +msgs per publish, attacked | gain pp per extra msg (search + publish/10, no attack) |
|---|---|---|---|---|---|---|---|---|
| dens+multi3 | 98.5 | 93.7 | +56.0 | +0.31 | +33.34 | +12.0 | +99.8 | 15.4 |
| Pdm2|Qdm2>d3 | 97.8 | 91.7 | +55.2 | +0.30 | +16.99 | +12.3 | +61.1 | 27.7 |
| Pd4|Qd>d4 | 96.6 | 85.1 | +54.0 | +0.31 | +29.84 | +12.0 | +54.0 | 16.4 |
| Pd3|Qd>d4 | 96.1 | 84.9 | +53.5 | +0.31 | +20.10 | +12.1 | +47.4 | 23.1 |
| Pde3|Qde>d3 | 96.1 | 84.2 | +53.5 | +0.32 | +20.09 | +12.1 | +51.1 | 23.0 |
| Pd3|Qd>d3 | 95.6 | 83.4 | +53.0 | +0.27 | +20.10 | +11.8 | +47.4 | 23.3 |
| dens+disj3 | 94.5 | 78.4 | +51.9 | +11.70 | +20.10 | +23.8 | +47.4 | 3.8 |
| Pd2|Qd>d2 | 93.8 | 80.7 | +51.2 | +0.31 | +9.72 | +11.4 | +37.7 | 39.9 |
| Pd3|Qd2 | 93.3 | 76.5 | +50.7 | +5.87 | +20.10 | +18.0 | +47.4 | 6.4 |
| Pd|Qd>d3 | 92.3 | 79.9 | +49.8 | +0.27 | +0.73 | +12.6 | +22.4 | 145.2 |
| Pd3|Qd | 89.7 | 71.8 | +47.2 | +0.31 | +20.10 | +9.4 | +47.4 | 20.4 |
| dens | 86.4 | 62.6 | +43.8 | +0.31 | +0.73 | +9.4 | +22.4 | 115.9 |
| base | 42.6 | 0.2 | +0.0 | +0.00 | +0.00 | +0.0 | +0.0 | free |

Search for content nobody provides (no attack) costs the first lookup plus the fallback: about the dens column plus the dens+disj3 column below (/find):

| S | dens | dens+disj3 |
|---|---|---|
| 300 | 5.8 | 17.2 |
| 1000 | 6.7 | 19.7 |
| 10000 | 8.1 | 24.3 |

## Escalation detail alpha=3 (fb_a3.jsonl)



**S=1000, m=20**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|---|---|
| base | 0.4 ±0.2 | 0.1 ±0.1 | 88.8 ±1.1 | 33.9 ±1.7 | 97.8 ±0.5 | 66.3 ±1.8 | 6.9 | 16.9 |
| dens | 99.0 ±0.3 | 97.8 ±0.5 | 99.0 ±0.3 | 81.7 ±1.6 | 98.8 ±0.4 | 72.1 ±1.6 | 16.2 | 40.0 |
| dens+disj3 | 99.4 ±0.2 | 95.6 ±0.5 | 100.0 ±0.0 | 96.7 ±0.5 | 100.0 ±0.0 | 99.3 ±0.3 | 31.7 | 65.1 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 99.6 ±0.2 | 100.0 ±0.0 | 97.5 ±0.5 | 17.6 | 119.4 |
| Pd|Qd>d3 | 99.2 ±0.2 | 98.1 ±0.4 | 99.9 ±0.1 | 91.3 ±1.3 | 99.9 ±0.1 | 90.2 ±1.4 | 18.1 | 40.0 |
| Pd3|Qd | 99.1 ±0.2 | 98.0 ±0.4 | 99.4 ±0.2 | 87.8 ±1.0 | 99.6 ±0.2 | 82.4 ±1.0 | 16.2 | 65.1 |
| Pd3|Qd>d3 | 99.4 ±0.2 | 98.4 ±0.4 | 100.0 ±0.0 | 98.7 ±0.3 | 100.0 ±0.0 | 99.5 ±0.2 | 17.4 | 65.1 |
| Pd2|Qd>d2 | 99.2 ±0.2 | 98.1 ±0.4 | 99.9 ±0.1 | 94.3 ±0.8 | 99.9 ±0.1 | 96.5 ±0.6 | 17.2 | 55.1 |
| Pd3|Qd>d4 | 99.5 ±0.2 | 98.5 ±0.3 | 100.0 ±0.0 | 99.5 ±0.2 | 100.0 ±0.0 | 99.8 ±0.2 | 17.7 | 65.1 |
| Pd4|Qd>d4 | 99.6 ±0.1 | 98.9 ±0.3 | 100.0 ±0.0 | 99.8 ±0.1 | 100.0 ±0.0 | 99.9 ±0.1 | 17.6 | 72.7 |
| Pde3|Qde>d3 | 99.9 ±0.1 | 98.5 ±0.3 | 100.0 ±0.0 | 98.4 ±0.4 | 100.0 ±0.0 | 99.6 ±0.2 | 17.9 | 69.4 |
| Pdm2|Qdm2>d3 | 100.0 ±0.0 | 99.9 ±0.1 | 100.0 ±0.0 | 98.4 ±0.5 | 100.0 ±0.0 | 97.1 ±0.7 | 17.8 | 79.9 |
| Pd3|Qd2 | 99.2 ±0.2 | 97.2 ±0.4 | 99.9 ±0.1 | 93.0 ±0.7 | 100.0 ±0.0 | 96.8 ±0.5 | 25.2 | 65.1 |

**S=1000, m=50**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|---|---|
| base | 0.3 ±0.1 | 0.1 ±0.1 | 66.9 ±2.0 | 7.2 ±0.8 | 97.6 ±0.5 | 52.3 ±1.9 | 7.0 | 17.0 |
| dens | 98.9 ±0.3 | 94.3 ±0.8 | 99.5 ±0.2 | 79.5 ±1.3 | 99.3 ±0.3 | 57.3 ±1.9 | 18.0 | 42.6 |
| dens+disj3 | 99.2 ±0.2 | 95.4 ±0.6 | 99.9 ±0.1 | 88.0 ±1.1 | 100.0 ±0.0 | 97.0 ±0.5 | 37.9 | 78.6 |
| dens+multi3 | 100.0 ±0.0 | 99.9 ±0.1 | 100.0 ±0.0 | 98.7 ±0.3 | 100.0 ±0.0 | 92.4 ±0.8 | 20.2 | 127.9 |
| Pd|Qd>d3 | 99.0 ±0.2 | 95.9 ±0.6 | 99.8 ±0.1 | 91.5 ±0.9 | 100.0 ±0.0 | 79.1 ±1.9 | 21.3 | 42.6 |
| Pd3|Qd | 99.0 ±0.2 | 94.9 ±0.7 | 99.5 ±0.2 | 84.7 ±1.2 | 99.7 ±0.1 | 73.4 ±1.2 | 18.0 | 78.6 |
| Pd3|Qd>d3 | 99.1 ±0.2 | 96.6 ±0.5 | 99.9 ±0.1 | 93.0 ±0.9 | 100.0 ±0.0 | 97.8 ±0.5 | 20.2 | 78.6 |
| Pd2|Qd>d2 | 99.0 ±0.2 | 96.5 ±0.6 | 99.7 ±0.1 | 92.4 ±1.0 | 100.0 ±0.0 | 88.5 ±1.2 | 19.8 | 64.9 |
| Pd3|Qd>d4 | 99.2 ±0.2 | 96.7 ±0.5 | 99.9 ±0.1 | 93.7 ±0.9 | 100.0 ±0.0 | 99.2 ±0.3 | 20.5 | 78.6 |
| Pd4|Qd>d4 | 99.3 ±0.2 | 96.7 ±0.5 | 99.9 ±0.1 | 95.3 ±0.6 | 100.0 ±0.0 | 99.6 ±0.2 | 20.4 | 88.4 |
| Pde3|Qde>d3 | 99.9 ±0.1 | 96.6 ±0.5 | 99.9 ±0.1 | 92.5 ±1.0 | 100.0 ±0.0 | 97.8 ±0.5 | 20.8 | 83.1 |
| Pdm2|Qdm2>d3 | 100.0 ±0.0 | 99.5 ±0.2 | 100.0 ±0.0 | 98.1 ±0.4 | 100.0 ±0.0 | 92.0 ±1.0 | 20.7 | 85.4 |
| Pd3|Qd2 | 99.1 ±0.2 | 95.9 ±0.6 | 99.7 ±0.1 | 88.3 ±1.1 | 100.0 ±0.0 | 91.5 ±0.8 | 30.1 | 78.6 |

S=1000, no attack:

| defence | P(find) | /find per search | publish find+provide |
|---|---|---|---|
| base | 100.0 | 6.29 | 6.3+10.0 |
| dens | 100.0 | 6.65 | 6.7+10.6 |
| dens+disj3 | 100.0 | 19.74 | 19.8+19.5 |
| dens+multi3 | 100.0 | 6.67 | 20.2+31.8 |
| Pd|Qd>d3 | 100.0 | 6.68 | 6.7+10.6 |
| Pd3|Qd | 100.0 | 6.65 | 19.8+19.5 |
| Pd3|Qd>d3 | 100.0 | 6.68 | 19.8+19.5 |
| Pd2|Qd>d2 | 100.0 | 6.66 | 13.1+14.1 |
| Pd3|Qd>d4 | 100.0 | 6.69 | 19.8+19.5 |
| Pd4|Qd>d4 | 100.0 | 6.69 | 25.5+26.4 |
| Pde3|Qde>d3 | 100.0 | 6.67 | 19.9+19.6 |
| Pdm2|Qdm2>d3 | 100.0 | 6.68 | 13.5+21.2 |
| Pd3|Qd2 | 100.0 | 13.09 | 19.8+19.5 |

**S=10000, m=20**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|---|---|
| base | 0.0 ±0.0 | 0.0 ±0.0 | 87.4 ±1.2 | 28.0 ±1.6 | 97.4 ±0.5 | 60.3 ±1.8 | 8.2 | 18.2 |
| dens | 99.9 ±0.1 | 99.9 ±0.1 | 99.1 ±0.3 | 79.1 ±1.7 | 98.8 ±0.4 | 63.5 ±1.7 | 17.8 | 41.5 |
| dens+disj3 | 99.9 ±0.1 | 97.7 ±0.3 | 100.0 ±0.0 | 96.3 ±0.5 | 100.0 ±0.0 | 99.1 ±0.3 | 36.8 | 71.2 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 99.2 ±0.3 | 100.0 ±0.0 | 94.9 ±0.6 | 19.7 | 124.8 |
| Pd|Qd>d3 | 99.9 ±0.1 | 99.9 ±0.1 | 99.8 ±0.2 | 89.0 ±1.5 | 100.0 ±0.1 | 85.7 ±1.7 | 20.5 | 41.5 |
| Pd3|Qd | 99.9 ±0.1 | 99.8 ±0.1 | 99.6 ±0.2 | 85.3 ±1.2 | 99.5 ±0.2 | 76.7 ±1.0 | 17.8 | 71.2 |
| Pd3|Qd>d3 | 99.9 ±0.1 | 99.9 ±0.1 | 100.0 ±0.0 | 98.5 ±0.4 | 100.0 ±0.0 | 99.4 ±0.3 | 19.5 | 71.2 |
| Pd2|Qd>d2 | 99.9 ±0.1 | 99.8 ±0.1 | 100.0 ±0.0 | 92.2 ±1.0 | 100.0 ±0.0 | 95.8 ±0.7 | 19.2 | 58.7 |
| Pd3|Qd>d4 | 99.9 ±0.1 | 99.9 ±0.1 | 100.0 ±0.0 | 99.3 ±0.3 | 100.0 ±0.0 | 99.8 ±0.2 | 20.0 | 71.2 |
| Pd4|Qd>d4 | 99.9 ±0.1 | 100.0 ±0.0 | 100.0 ±0.0 | 99.9 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 19.8 | 81.4 |
| Pde3|Qde>d3 | 100.0 ±0.0 | 99.9 ±0.1 | 100.0 ±0.0 | 98.5 ±0.3 | 100.0 ±0.0 | 99.4 ±0.3 | 20.0 | 75.9 |
| Pdm2|Qdm2>d3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 98.5 ±0.5 | 100.0 ±0.0 | 94.9 ±0.8 | 20.1 | 83.3 |
| Pd3|Qd2 | 99.9 ±0.1 | 98.9 ±0.2 | 100.0 ±0.0 | 91.2 ±0.8 | 100.0 ±0.0 | 95.6 ±0.5 | 28.4 | 71.2 |

**S=10000, m=50**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|---|---|
| base | 0.0 ±0.0 | 0.1 ±0.1 | 67.6 ±1.9 | 5.3 ±0.7 | 97.9 ±0.5 | 47.8 ±1.9 | 8.3 | 18.3 |
| dens | 99.9 ±0.1 | 99.3 ±0.3 | 99.8 ±0.1 | 86.1 ±1.0 | 99.0 ±0.3 | 51.7 ±1.8 | 20.1 | 45.2 |
| dens+disj3 | 100.0 ±0.0 | 98.8 ±0.3 | 100.0 ±0.0 | 90.5 ±0.9 | 100.0 ±0.0 | 97.2 ±0.5 | 43.6 | 86.0 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 99.7 ±0.2 | 100.0 ±0.0 | 89.0 ±0.9 | 22.1 | 135.7 |
| Pd|Qd>d3 | 99.9 ±0.1 | 99.6 ±0.2 | 100.0 ±0.0 | 95.8 ±0.6 | 100.0 ±0.0 | 76.9 ±2.0 | 23.2 | 45.2 |
| Pd3|Qd | 99.9 ±0.1 | 99.3 ±0.3 | 99.9 ±0.1 | 90.3 ±1.0 | 99.6 ±0.2 | 68.1 ±1.2 | 20.1 | 86.0 |
| Pd3|Qd>d3 | 100.0 ±0.0 | 99.6 ±0.2 | 100.0 ±0.0 | 96.1 ±0.7 | 100.0 ±0.0 | 97.8 ±0.5 | 22.1 | 86.0 |
| Pd2|Qd>d2 | 99.9 ±0.1 | 99.7 ±0.2 | 99.9 ±0.0 | 95.1 ±0.8 | 100.0 ±0.0 | 89.5 ±1.2 | 21.7 | 70.2 |
| Pd3|Qd>d4 | 100.0 ±0.0 | 99.7 ±0.2 | 100.0 ±0.0 | 96.4 ±0.7 | 100.0 ±0.0 | 99.0 ±0.4 | 22.6 | 86.0 |
| Pd4|Qd>d4 | 100.0 ±0.0 | 99.7 ±0.2 | 100.0 ±0.0 | 98.3 ±0.4 | 100.0 ±0.0 | 99.8 ±0.1 | 22.4 | 99.4 |
| Pde3|Qde>d3 | 100.0 ±0.0 | 99.7 ±0.2 | 100.0 ±0.0 | 95.8 ±0.7 | 100.0 ±0.0 | 97.9 ±0.5 | 22.8 | 90.7 |
| Pdm2|Qdm2>d3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 99.3 ±0.3 | 100.0 ±0.0 | 89.5 ±1.2 | 22.7 | 90.5 |
| Pd3|Qd2 | 99.9 ±0.1 | 99.4 ±0.3 | 99.9 ±0.1 | 91.5 ±0.9 | 100.0 ±0.0 | 91.3 ±0.8 | 34.1 | 86.0 |

S=10000, no attack:

| defence | P(find) | /find per search | publish find+provide |
|---|---|---|---|
| base | 100.0 | 7.72 | 7.7+10.0 |
| dens | 100.0 | 8.06 | 8.1+10.5 |
| dens+disj3 | 100.0 | 24.28 | 24.3+19.9 |
| dens+multi3 | 100.0 | 8.04 | 24.2+31.5 |
| Pd|Qd>d3 | 100.0 | 8.06 | 8.1+10.5 |
| Pd3|Qd | 100.0 | 8.06 | 24.3+19.9 |
| Pd3|Qd>d3 | 100.0 | 8.06 | 24.3+19.9 |
| Pd2|Qd>d2 | 100.0 | 8.03 | 16.0+14.0 |
| Pd3|Qd>d4 | 100.0 | 8.04 | 24.3+19.9 |
| Pd4|Qd>d4 | 100.0 | 8.04 | 31.6+27.1 |
| Pde3|Qde>d3 | 100.0 | 8.03 | 24.2+19.8 |
| Pdm2|Qdm2>d3 | 100.0 | 8.06 | 16.1+21.0 |
| Pd3|Qd2 | 100.0 | 16.07 | 24.3+19.9 |


## Escalation: disjoint paths only when needed, alpha=1 (fb_a1.jsonl)

Publisher pays once per republish; searcher runs the cheap density-checked lookup and escalates only when it found no provider (so searches for content nobody provides also pay the escalation: see 'no-attack miss cost').

* `Pd|Qd>d3`: pub dens; search dens, on failure dens+disj3
* `Pd3|Qd`: pub dens+disj3; search dens
* `Pd3|Qd>d3`: pub dens+disj3; search dens, on failure dens+disj3
* `Pd2|Qd>d2`: pub dens+disj2; search dens, on failure dens+disj2
* `Pd3|Qd>d4`: pub dens+disj3; search dens, on failure dens+disj4
* `Pd4|Qd>d4`: pub dens+disj4; search dens, on failure dens+disj4
* `Pde3|Qde>d3`: pub dens+est_hrd+disj3; search dens+est_hrd, on failure +disj3
* `Pdm2|Qdm2>d3`: pub dens+multi2; search dens on key 0, key 1, then dens+disj3
* `Pd3|Qd2`: pub dens+disj3; search dens+disj2

**Summary, S in {1000, 10000}, m in {20, 50}, 6 variants:**

| defence | mean P(find) % | worst % | gain pp | +/find per search, no attack | +msgs per publish, no attack | +/find per search, attacked | +msgs per publish, attacked | gain pp per extra msg (search + publish/10, no attack) |
|---|---|---|---|---|---|---|---|---|
| Pd4|Qd>d4 | 97.5 | 82.7 | +62.9 | +0.36 | +30.81 | +12.7 | +53.0 | 18.3 |
| Pd3|Qd>d4 | 95.9 | 77.4 | +61.3 | +0.36 | +18.57 | +13.0 | +46.0 | 27.7 |
| Pde3|Qde>d3 | 94.8 | 77.5 | +60.2 | +0.35 | +18.62 | +13.2 | +51.3 | 27.2 |
| Pd3|Qd>d3 | 94.7 | 76.5 | +60.1 | +0.35 | +18.57 | +12.5 | +46.0 | 27.3 |
| dens+multi3 | 94.4 | 61.1 | +59.8 | +0.37 | +31.18 | +12.6 | +101.1 | 17.2 |
| Pdm2|Qdm2>d3 | 94.1 | 62.8 | +59.5 | +0.35 | +15.99 | +13.2 | +62.5 | 30.5 |
| Pd2|Qd>d2 | 90.8 | 59.2 | +56.2 | +0.35 | +8.44 | +12.1 | +38.0 | 47.1 |
| dens+disj3 | 90.5 | 58.9 | +55.9 | +8.99 | +18.57 | +21.3 | +46.0 | 5.2 |
| Pd3|Qd2 | 89.9 | 64.0 | +55.3 | +4.59 | +18.57 | +17.3 | +46.0 | 8.6 |
| Pd|Qd>d3 | 89.3 | 49.1 | +54.8 | +0.35 | +0.87 | +12.8 | +24.0 | 126.2 |
| Pd3|Qd | 86.3 | 45.2 | +51.7 | +0.34 | +18.57 | +10.2 | +46.0 | 23.5 |
| dens | 83.6 | 26.9 | +49.0 | +0.34 | +0.87 | +10.2 | +24.0 | 114.2 |
| base | 34.6 | 0.0 | +0.0 | +0.00 | +0.00 | +0.0 | +0.0 | free |

**Summary, S=300, m in {20, 50}, 6 variants:**

| defence | mean P(find) % | worst % | gain pp | +/find per search, no attack | +msgs per publish, no attack | +/find per search, attacked | +msgs per publish, attacked | gain pp per extra msg (search + publish/10, no attack) |
|---|---|---|---|---|---|---|---|---|
| dens+multi3 | 95.0 | 73.5 | +60.8 | +0.34 | +29.65 | +12.6 | +93.3 | 18.4 |
| Pdm2|Qdm2>d3 | 93.6 | 70.4 | +59.3 | +0.36 | +15.22 | +12.9 | +57.6 | 31.5 |
| Pd4|Qd>d4 | 91.4 | 66.0 | +57.1 | +0.33 | +23.70 | +12.6 | +42.3 | 21.2 |
| Pd3|Qd>d4 | 89.7 | 63.9 | +55.4 | +0.33 | +15.54 | +12.8 | +37.5 | 29.4 |
| Pde3|Qde>d3 | 88.8 | 62.8 | +54.6 | +0.35 | +15.44 | +12.9 | +42.2 | 28.8 |
| Pd3|Qd>d3 | 88.3 | 63.1 | +54.0 | +0.33 | +15.54 | +12.3 | +37.5 | 28.7 |
| Pd|Qd>d3 | 85.8 | 54.5 | +51.5 | +0.33 | +0.90 | +12.2 | +21.9 | 122.3 |
| Pd2|Qd>d2 | 85.6 | 60.4 | +51.3 | +0.34 | +7.50 | +11.8 | +31.9 | 47.1 |
| Pd3|Qd2 | 82.5 | 50.0 | +48.3 | +3.77 | +15.54 | +14.6 | +37.5 | 9.1 |
| dens+disj3 | 81.2 | 41.2 | +46.9 | +7.37 | +15.54 | +17.4 | +37.5 | 5.3 |
| Pd3|Qd | 80.5 | 49.2 | +46.3 | +0.34 | +15.54 | +9.4 | +37.5 | 24.4 |
| dens | 79.9 | 36.0 | +45.7 | +0.34 | +0.90 | +9.4 | +21.9 | 105.5 |
| base | 34.2 | 0.0 | +0.0 | +0.00 | +0.00 | +0.0 | +0.0 | free |

Search for content nobody provides (no attack) costs the first lookup plus the fallback: about the dens column plus the dens+disj3 column below (/find):

| S | dens | dens+disj3 |
|---|---|---|
| 300 | 4.0 | 11.0 |
| 1000 | 4.4 | 12.2 |
| 10000 | 5.0 | 14.4 |

## Escalation detail alpha=1 (fb_a1.jsonl)



**S=1000, m=20**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|---|---|
| base | 0.2 ±0.1 | 0.0 ±0.0 | 85.9 ±1.2 | 10.9 ±1.0 | 97.3 ±0.6 | 32.6 ±1.7 | 4.2 | 14.2 |
| dens | 98.8 ±0.3 | 97.5 ±0.5 | 98.5 ±0.4 | 68.5 ±2.0 | 98.7 ±0.4 | 35.3 ±1.7 | 13.4 | 36.3 |
| dens+disj3 | 98.9 ±0.3 | 79.1 ±1.1 | 99.9 ±0.1 | 79.9 ±1.3 | 100.0 ±0.0 | 90.8 ±1.1 | 22.3 | 53.7 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 96.7 ±0.7 | 100.0 ±0.0 | 72.8 ±1.5 | 15.8 | 108.8 |
| Pd|Qd>d3 | 98.9 ±0.3 | 97.8 ±0.5 | 99.9 ±0.0 | 77.7 ±1.9 | 100.0 ±0.0 | 58.2 ±2.3 | 15.8 | 36.3 |
| Pd3|Qd | 98.9 ±0.3 | 94.0 ±0.9 | 99.2 ±0.2 | 73.6 ±1.6 | 99.2 ±0.2 | 55.4 ±1.3 | 13.4 | 53.7 |
| Pd3|Qd>d3 | 99.1 ±0.2 | 95.1 ±0.8 | 99.9 ±0.1 | 88.9 ±1.1 | 100.0 ±0.0 | 91.2 ±1.0 | 15.3 | 53.7 |
| Pd2|Qd>d2 | 99.0 ±0.3 | 95.4 ±0.8 | 99.7 ±0.2 | 77.2 ±1.7 | 100.0 ±0.0 | 73.4 ±1.6 | 15.1 | 47.2 |
| Pd3|Qd>d4 | 99.4 ±0.2 | 95.1 ±0.8 | 100.0 ±0.0 | 93.0 ±1.0 | 100.0 ±0.0 | 95.1 ±0.9 | 15.8 | 53.7 |
| Pd4|Qd>d4 | 99.4 ±0.2 | 96.8 ±0.6 | 100.0 ±0.0 | 96.8 ±0.6 | 100.0 ±0.0 | 98.7 ±0.4 | 15.5 | 58.9 |
| Pde3|Qde>d3 | 99.9 ±0.1 | 95.8 ±0.7 | 100.0 ±0.0 | 89.2 ±1.1 | 100.0 ±0.0 | 91.2 ±1.0 | 15.9 | 58.5 |
| Pdm2|Qdm2>d3 | 100.0 ±0.0 | 99.8 ±0.1 | 100.0 ±0.0 | 93.3 ±1.0 | 100.0 ±0.0 | 72.6 ±1.8 | 16.3 | 72.7 |
| Pd3|Qd2 | 99.0 ±0.3 | 88.1 ±0.9 | 99.8 ±0.1 | 73.5 ±1.3 | 100.0 ±0.0 | 80.4 ±1.2 | 18.9 | 53.7 |

**S=1000, m=50**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|---|---|
| base | 0.1 ±0.1 | 0.0 ±0.0 | 63.5 ±1.9 | 1.8 ±0.4 | 96.6 ±0.6 | 25.0 ±1.5 | 4.3 | 14.3 |
| dens | 98.7 ±0.3 | 94.0 ±0.8 | 99.4 ±0.2 | 75.9 ±1.4 | 98.6 ±0.4 | 29.0 ±1.6 | 15.2 | 39.3 |
| dens+disj3 | 98.8 ±0.2 | 76.4 ±1.2 | 99.8 ±0.1 | 58.9 ±1.5 | 100.0 ±0.0 | 78.2 ±1.6 | 26.5 | 62.5 |
| dens+multi3 | 100.0 ±0.0 | 99.9 ±0.1 | 100.0 ±0.0 | 98.2 ±0.4 | 100.0 ±0.0 | 65.3 ±1.6 | 17.8 | 118.5 |
| Pd|Qd>d3 | 98.9 ±0.3 | 95.3 ±0.7 | 99.8 ±0.1 | 84.4 ±1.2 | 99.9 ±0.1 | 50.7 ±2.3 | 18.0 | 39.3 |
| Pd3|Qd | 98.8 ±0.3 | 87.6 ±1.3 | 99.5 ±0.2 | 68.8 ±1.7 | 99.4 ±0.2 | 45.2 ±1.4 | 15.2 | 62.5 |
| Pd3|Qd>d3 | 99.0 ±0.2 | 90.6 ±1.1 | 99.8 ±0.1 | 76.5 ±1.6 | 100.0 ±0.0 | 79.4 ±1.5 | 18.3 | 62.5 |
| Pd2|Qd>d2 | 98.9 ±0.3 | 93.4 ±1.0 | 99.7 ±0.1 | 78.1 ±1.6 | 100.0 ±0.1 | 59.2 ±1.8 | 17.6 | 54.4 |
| Pd3|Qd>d4 | 99.0 ±0.2 | 90.4 ±1.1 | 99.9 ±0.1 | 77.4 ±1.6 | 100.0 ±0.0 | 85.6 ±1.5 | 18.7 | 62.5 |
| Pd4|Qd>d4 | 99.1 ±0.2 | 91.7 ±0.9 | 99.9 ±0.1 | 82.7 ±1.3 | 100.0 ±0.0 | 91.9 ±1.0 | 18.4 | 69.1 |
| Pde3|Qde>d3 | 99.9 ±0.1 | 90.1 ±1.1 | 99.9 ±0.0 | 77.5 ±1.6 | 100.0 ±0.0 | 79.7 ±1.5 | 19.0 | 68.0 |
| Pdm2|Qdm2>d3 | 100.0 ±0.0 | 99.1 ±0.3 | 100.0 ±0.0 | 95.8 ±0.6 | 100.0 ±0.0 | 65.4 ±2.0 | 18.5 | 78.8 |
| Pd3|Qd2 | 98.9 ±0.2 | 85.9 ±1.2 | 99.8 ±0.1 | 64.0 ±1.6 | 100.0 ±0.1 | 67.1 ±1.5 | 22.8 | 62.5 |

S=1000, no attack:

| defence | P(find) | /find per search | publish find+provide |
|---|---|---|---|
| base | 100.0 | 3.98 | 4.0+10.0 |
| dens | 100.0 | 4.36 | 4.4+10.6 |
| dens+disj3 | 100.0 | 12.20 | 12.2+19.3 |
| dens+multi3 | 100.0 | 4.37 | 13.0+31.5 |
| Pd|Qd>d3 | 100.0 | 4.36 | 4.4+10.6 |
| Pd3|Qd | 100.0 | 4.36 | 12.2+19.3 |
| Pd3|Qd>d3 | 100.0 | 4.36 | 12.2+19.3 |
| Pd2|Qd>d2 | 100.0 | 4.36 | 8.2+13.8 |
| Pd3|Qd>d4 | 100.0 | 4.38 | 12.2+19.3 |
| Pd4|Qd>d4 | 100.0 | 4.38 | 16.4+26.1 |
| Pde3|Qde>d3 | 100.0 | 4.37 | 12.2+19.3 |
| Pdm2|Qdm2>d3 | 100.0 | 4.37 | 8.7+21.1 |
| Pd3|Qd2 | 100.0 | 8.20 | 12.2+19.3 |

**S=10000, m=20**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|---|---|
| base | 0.0 ±0.0 | 0.0 ±0.0 | 85.0 ±1.3 | 11.5 ±1.0 | 97.3 ±0.6 | 35.0 ±1.7 | 4.9 | 14.9 |
| dens | 99.9 ±0.1 | 99.6 ±0.3 | 98.8 ±0.4 | 70.3 ±2.0 | 98.6 ±0.4 | 37.2 ±1.7 | 14.2 | 37.6 |
| dens+disj3 | 99.9 ±0.1 | 87.6 ±0.8 | 100.0 ±0.0 | 84.1 ±1.1 | 100.0 ±0.0 | 92.7 ±1.0 | 25.0 | 58.1 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 97.2 ±0.6 | 100.0 ±0.0 | 73.7 ±1.4 | 16.6 | 112.5 |
| Pd|Qd>d3 | 99.9 ±0.1 | 99.6 ±0.2 | 100.0 ±0.0 | 79.0 ±1.9 | 100.0 ±0.0 | 62.8 ±2.3 | 16.8 | 37.6 |
| Pd3|Qd | 99.9 ±0.1 | 98.2 ±0.5 | 99.3 ±0.2 | 76.6 ±1.5 | 99.2 ±0.3 | 56.3 ±1.3 | 14.2 | 58.1 |
| Pd3|Qd>d3 | 99.9 ±0.1 | 98.9 ±0.4 | 100.0 ±0.0 | 91.6 ±0.9 | 100.0 ±0.0 | 93.4 ±0.9 | 16.1 | 58.1 |
| Pd2|Qd>d2 | 99.9 ±0.1 | 98.6 ±0.5 | 99.9 ±0.1 | 81.1 ±1.6 | 99.9 ±0.1 | 76.5 ±1.6 | 15.9 | 49.9 |
| Pd3|Qd>d4 | 99.9 ±0.1 | 98.7 ±0.4 | 100.0 ±0.0 | 95.4 ±0.8 | 100.0 ±0.0 | 96.6 ±0.8 | 16.7 | 58.1 |
| Pd4|Qd>d4 | 99.9 ±0.1 | 99.4 ±0.3 | 100.0 ±0.0 | 98.1 ±0.4 | 100.0 ±0.0 | 99.4 ±0.3 | 16.4 | 65.3 |
| Pde3|Qde>d3 | 100.0 ±0.0 | 98.7 ±0.4 | 100.0 ±0.0 | 91.1 ±1.0 | 100.0 ±0.0 | 93.2 ±0.9 | 16.8 | 63.2 |
| Pdm2|Qdm2>d3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 94.0 ±1.0 | 100.0 ±0.0 | 75.8 ±1.8 | 17.2 | 74.9 |
| Pd3|Qd2 | 99.9 ±0.1 | 94.1 ±0.6 | 99.9 ±0.1 | 78.5 ±1.2 | 99.9 ±0.1 | 83.1 ±1.1 | 20.7 | 58.1 |

**S=10000, m=50**

| defence | tight/fresh | tight/aged | ring/fresh | ring/aged | evas/fresh | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|---|---|---|
| base | 0.0 ±0.0 | 0.0 ±0.0 | 63.2 ±1.9 | 1.7 ±0.3 | 97.4 ±0.6 | 25.1 ±1.5 | 5.0 | 15.0 |
| dens | 99.8 ±0.1 | 99.5 ±0.2 | 99.8 ±0.2 | 84.7 ±1.1 | 98.3 ±0.4 | 26.9 ±1.6 | 16.3 | 41.1 |
| dens+disj3 | 99.8 ±0.1 | 90.6 ±0.7 | 100.0 ±0.0 | 72.0 ±1.4 | 100.0 ±0.0 | 83.7 ±1.4 | 29.8 | 68.1 |
| dens+multi3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 99.7 ±0.1 | 100.0 ±0.0 | 61.1 ±1.6 | 18.5 | 123.2 |
| Pd|Qd>d3 | 99.8 ±0.1 | 99.7 ±0.1 | 99.9 ±0.1 | 92.6 ±0.8 | 100.0 ±0.0 | 49.1 ±2.3 | 19.0 | 41.1 |
| Pd3|Qd | 99.8 ±0.1 | 96.7 ±0.7 | 99.9 ±0.1 | 80.4 ±1.5 | 98.9 ±0.3 | 45.7 ±1.3 | 16.3 | 68.1 |
| Pd3|Qd>d3 | 99.8 ±0.1 | 98.2 ±0.5 | 100.0 ±0.0 | 87.4 ±1.2 | 100.0 ±0.0 | 84.1 ±1.3 | 18.8 | 68.1 |
| Pd2|Qd>d2 | 99.8 ±0.1 | 99.1 ±0.4 | 100.0 ±0.0 | 87.9 ±1.3 | 99.9 ±0.1 | 62.6 ±1.8 | 18.3 | 58.9 |
| Pd3|Qd>d4 | 99.9 ±0.1 | 98.1 ±0.5 | 100.0 ±0.0 | 87.6 ±1.2 | 100.0 ±0.0 | 89.9 ±1.3 | 19.3 | 68.1 |
| Pd4|Qd>d4 | 99.9 ±0.1 | 98.8 ±0.3 | 100.0 ±0.0 | 91.5 ±0.9 | 100.0 ±0.0 | 95.6 ±0.7 | 18.9 | 77.2 |
| Pde3|Qde>d3 | 99.9 ±0.0 | 98.1 ±0.5 | 100.0 ±0.0 | 87.0 ±1.2 | 100.0 ±0.0 | 84.4 ±1.4 | 19.7 | 74.0 |
| Pdm2|Qdm2>d3 | 100.0 ±0.0 | 100.0 ±0.0 | 100.0 ±0.0 | 98.6 ±0.3 | 100.0 ±0.0 | 62.8 ±2.0 | 19.2 | 82.2 |
| Pd3|Qd2 | 99.8 ±0.1 | 96.3 ±0.6 | 99.9 ±0.1 | 76.8 ±1.4 | 99.9 ±0.1 | 71.7 ±1.4 | 25.2 | 68.1 |

S=10000, no attack:

| defence | P(find) | /find per search | publish find+provide |
|---|---|---|---|
| base | 100.0 | 4.68 | 4.7+10.0 |
| dens | 100.0 | 4.98 | 5.0+10.5 |
| dens+disj3 | 100.0 | 14.44 | 14.4+19.9 |
| dens+multi3 | 100.0 | 5.02 | 15.0+31.5 |
| Pd|Qd>d3 | 100.0 | 4.99 | 5.0+10.5 |
| Pd3|Qd | 100.0 | 4.98 | 14.4+19.9 |
| Pd3|Qd>d3 | 100.0 | 4.99 | 14.4+19.9 |
| Pd2|Qd>d2 | 100.0 | 5.00 | 9.7+13.9 |
| Pd3|Qd>d4 | 100.0 | 4.98 | 14.4+19.9 |
| Pd4|Qd>d4 | 100.0 | 4.98 | 19.3+28.4 |
| Pde3|Qde>d3 | 100.0 | 4.99 | 14.4+19.9 |
| Pdm2|Qdm2>d3 | 100.0 | 4.99 | 9.9+20.9 |
| Pd3|Qd2 | 100.0 | 9.64 | 14.4+19.9 |


## Records under several keys vs an attacker who splits the budget (split.jsonl)

split=r: the attacker spreads m over keys 0..r-1 (m/r each), as an attacker who knows the scheme but has a fixed budget would. (The main grid gives the attacker m on every key.) multiR searches key 0, then 1, ... until found.

| S | m | variant | split r | base | dens | multi r | dens+multi r | k20+s2k | dens+disj3 | multi r /find per search | multi r publish msgs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1000 | 20 | ring/aged | 2 | 68.7 | 78.6 | 89.0 | 94.5 | 94.5 | 99.8 | 8.6 | 33.0 |
| 1000 | 20 | ring/aged | 3 | 84.8 | 87.7 | 99.6 | 99.8 | 97.7 | 100.0 | 7.7 | 49.6 |
| 1000 | 20 | tight/aged | 2 | 0.6 | 98.0 | 1.5 | 99.9 | 77.9 | 92.6 | 12.8 | 32.9 |
| 1000 | 20 | tight/aged | 3 | 24.4 | 57.0 | 56.2 | 91.5 | 97.8 | 100.0 | 14.9 | 49.1 |
| 1000 | 20 | tight/fresh | 2 | 1.6 | 98.2 | 3.1 | 100.0 | 96.0 | 99.6 | 15.0 | 35.1 |
| 1000 | 20 | tight/fresh | 3 | 75.5 | 93.1 | 98.3 | 100.0 | 100.0 | 100.0 | 9.7 | 52.2 |
| 1000 | 50 | ring/aged | 2 | 26.9 | 87.3 | 45.8 | 98.7 | 59.7 | 95.6 | 11.1 | 32.9 |
| 1000 | 50 | ring/aged | 3 | 46.3 | 77.6 | 83.1 | 99.1 | 80.0 | 98.3 | 11.9 | 49.5 |
| 1000 | 50 | tight/aged | 2 | 0.3 | 97.2 | 0.4 | 99.8 | 11.7 | 96.6 | 12.9 | 33.0 |
| 1000 | 50 | tight/aged | 3 | 0.1 | 98.0 | 0.4 | 100.0 | 31.4 | 95.3 | 19.3 | 49.5 |
| 1000 | 50 | tight/fresh | 2 | 0.3 | 99.0 | 0.9 | 100.0 | 59.8 | 99.3 | 16.3 | 36.3 |
| 1000 | 50 | tight/fresh | 3 | 0.3 | 98.7 | 1.2 | 100.0 | 86.3 | 99.4 | 23.3 | 53.5 |
| 10000 | 20 | ring/aged | 2 | 57.2 | 69.3 | 82.5 | 91.1 | 88.5 | 99.8 | 11.1 | 35.6 |
| 10000 | 20 | ring/aged | 3 | 79.1 | 82.2 | 98.9 | 99.4 | 95.8 | 100.0 | 9.8 | 53.5 |
| 10000 | 20 | tight/aged | 2 | 0.5 | 99.3 | 0.9 | 100.0 | 74.0 | 94.3 | 15.6 | 35.5 |
| 10000 | 20 | tight/aged | 3 | 22.4 | 50.2 | 52.8 | 88.6 | 96.2 | 100.0 | 18.5 | 53.3 |
| 10000 | 20 | tight/fresh | 2 | 0.6 | 99.6 | 2.2 | 100.0 | 94.4 | 99.9 | 17.7 | 37.7 |
| 10000 | 20 | tight/fresh | 3 | 71.6 | 92.5 | 97.7 | 99.9 | 100.0 | 100.0 | 11.9 | 56.3 |
| 10000 | 50 | ring/aged | 2 | 20.9 | 87.1 | 36.9 | 98.1 | 51.9 | 95.7 | 13.9 | 35.5 |
| 10000 | 50 | ring/aged | 3 | 38.3 | 71.2 | 74.5 | 97.6 | 71.9 | 98.1 | 15.8 | 53.5 |
| 10000 | 50 | tight/aged | 2 | 0.0 | 99.8 | 0.0 | 100.0 | 12.5 | 98.2 | 15.6 | 35.6 |
| 10000 | 50 | tight/aged | 3 | 0.0 | 99.8 | 0.1 | 100.0 | 32.9 | 96.6 | 23.4 | 53.4 |
| 10000 | 50 | tight/fresh | 2 | 0.1 | 99.9 | 0.1 | 100.0 | 61.8 | 100.0 | 19.0 | 39.1 |
| 10000 | 50 | tight/fresh | 3 | 0.1 | 99.7 | 0.2 | 100.0 | 82.8 | 100.0 | 27.4 | 57.6 |

## Announce hearing fraction h = 0.5 (hear.jsonl)

Each server heard only half of the others' announces (tables and the first-seen log are built from what it heard).

**S=1000, m=20**

| defence | tight/fresh | tight/aged | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|
| base | 0.3 ±0.2 | 0.2 ±0.1 | 63.1 ±2.2 | 7.0 | 17.0 |
| dens | 98.7 ±0.4 | 98.0 ±0.6 | 70.2 ±2.0 | 16.5 | 40.8 |
| est_tab | 13.5 ±1.1 | 3.5 ±0.5 | 68.9 ±2.1 | 7.0 | 19.7 |
| est_hrd | 89.5 ±1.0 | 6.5 ±0.7 | 74.8 ±1.9 | 8.7 | 25.3 |
| disj3 | 47.0 ±2.4 | 24.9 ±1.3 | 99.2 ±0.3 | 21.6 | 41.2 |
| disj4 | 92.7 ±1.0 | 65.7 ±1.4 | 99.9 ±0.2 | 27.9 | 53.3 |
| k20+s2k | 57.8 ±1.5 | 22.0 ±1.1 | 89.0 ±1.5 | 17.8 | 26.8 |
| dens+est_hrd | 99.5 ±0.2 | 98.0 ±0.5 | 80.0 ±1.7 | 18.0 | 46.3 |
| dens+disj3 | 98.9 ±0.3 | 96.5 ±0.6 | 99.5 ±0.3 | 33.7 | 70.5 |
| dens+est_hrd+disj3 | 99.6 ±0.2 | 98.5 ±0.4 | 100.0 ±0.0 | 37.3 | 84.7 |
| srv100 | 95.4 ±1.0 | 0.2 ±0.1 | 63.1 ±2.2 | 6.7 | 16.6 |
| srv100+est_hrd | 99.4 ±0.3 | 6.4 ±0.7 | 75.4 ±1.9 | 7.9 | 23.0 |

**S=1000, m=50**

| defence | tight/fresh | tight/aged | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|
| base | 0.4 ±0.2 | 0.1 ±0.1 | 47.2 ±2.3 | 7.1 | 17.1 |
| dens | 98.7 ±0.4 | 95.3 ±0.9 | 54.9 ±2.3 | 18.0 | 42.3 |
| est_tab | 12.6 ±1.1 | 1.4 ±0.3 | 52.9 ±2.3 | 7.1 | 19.8 |
| est_hrd | 89.8 ±0.9 | 1.4 ±0.3 | 60.7 ±2.2 | 8.9 | 25.5 |
| disj3 | 15.3 ±1.8 | 3.3 ±0.5 | 96.0 ±0.8 | 22.2 | 42.5 |
| disj4 | 48.7 ±2.6 | 15.5 ±1.1 | 98.9 ±0.4 | 28.3 | 55.8 |
| k20+s2k | 19.7 ±1.8 | 2.9 ±0.5 | 74.9 ±2.1 | 17.3 | 27.0 |
| dens+est_hrd | 99.4 ±0.3 | 94.6 ±1.0 | 65.6 ±2.1 | 19.4 | 48.5 |
| dens+disj3 | 98.8 ±0.3 | 96.0 ±0.7 | 97.3 ±0.6 | 39.7 | 83.9 |
| dens+est_hrd+disj3 | 99.5 ±0.2 | 96.4 ±0.7 | 99.1 ±0.3 | 43.5 | 98.4 |
| srv100 | 96.0 ±0.9 | 0.1 ±0.1 | 47.6 ±2.3 | 6.6 | 16.6 |
| srv100+est_hrd | 99.7 ±0.2 | 1.4 ±0.3 | 59.8 ±2.2 | 8.0 | 22.9 |

S=1000, no attack:

| defence | P(find) | /find per search | publish find+provide |
|---|---|---|---|
| base | 100.0 | 6.76 | 6.8+10.0 |
| dens | 100.0 | 7.04 | 7.0+10.5 |
| est_tab | 100.0 | 6.78 | 6.8+12.4 |
| est_hrd | 100.0 | 7.95 | 8.0+15.0 |
| disj3 | 100.0 | 20.45 | 20.5+18.0 |
| disj4 | 100.0 | 26.24 | 26.3+22.2 |
| k20+s2k | 100.0 | 18.36 | 6.8+20.0 |
| dens+est_hrd | 100.0 | 8.21 | 8.2+15.3 |
| dens+disj3 | 100.0 | 20.48 | 20.4+18.0 |
| dens+est_hrd+disj3 | 100.0 | 24.39 | 24.4+30.1 |
| srv100 | 100.0 | 6.77 | 6.8+10.0 |
| srv100+est_hrd | 100.0 | 7.96 | 8.0+15.0 |

**S=10000, m=20**

| defence | tight/fresh | tight/aged | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|
| base | 0.0 ±0.0 | 0.0 ±0.0 | 53.3 ±2.3 | 8.3 | 18.3 |
| dens | 99.8 ±0.2 | 99.9 ±0.1 | 59.3 ±2.2 | 18.3 | 42.8 |
| est_tab | 2.5 ±0.5 | 1.1 ±0.3 | 57.8 ±2.3 | 8.3 | 21.2 |
| est_hrd | 89.8 ±1.0 | 4.9 ±0.6 | 64.7 ±2.2 | 10.3 | 26.8 |
| disj3 | 49.6 ±2.3 | 24.6 ±1.3 | 99.2 ±0.3 | 25.9 | 45.6 |
| disj4 | 94.0 ±0.8 | 64.3 ±1.3 | 100.0 ±0.0 | 33.7 | 59.2 |
| k20+s2k | 54.0 ±1.5 | 22.3 ±1.2 | 83.9 ±1.9 | 19.4 | 28.3 |
| dens+est_hrd | 100.0 ±0.0 | 99.8 ±0.2 | 70.0 ±1.9 | 19.9 | 48.7 |
| dens+disj3 | 99.8 ±0.2 | 97.7 ±0.4 | 98.9 ±0.4 | 38.8 | 77.0 |
| dens+est_hrd+disj3 | 100.0 ±0.0 | 99.4 ±0.2 | 100.0 ±0.0 | 42.9 | 92.6 |
| srv100 | 99.7 ±0.2 | 0.0 ±0.0 | 53.3 ±2.3 | 8.0 | 18.0 |
| srv100+est_hrd | 100.0 ±0.0 | 4.9 ±0.6 | 67.4 ±2.1 | 9.4 | 24.3 |

**S=10000, m=50**

| defence | tight/fresh | tight/aged | evas/aged | /find per search (attacked avg) | publish find+provide (attacked avg) |
|---|---|---|---|---|---|
| base | 0.1 ±0.1 | 0.0 ±0.0 | 45.6 ±2.2 | 8.3 | 18.4 |
| dens | 99.6 ±0.3 | 99.6 ±0.3 | 50.3 ±2.2 | 20.0 | 44.9 |
| est_tab | 2.7 ±0.5 | 0.7 ±0.2 | 49.0 ±2.2 | 8.4 | 21.3 |
| est_hrd | 88.4 ±1.0 | 1.4 ±0.3 | 58.0 ±2.2 | 10.4 | 27.0 |
| disj3 | 17.7 ±2.0 | 4.5 ±0.6 | 96.2 ±0.8 | 26.2 | 46.5 |
| disj4 | 55.4 ±2.6 | 17.4 ±1.1 | 99.6 ±0.2 | 33.9 | 61.6 |
| k20+s2k | 22.1 ±1.7 | 4.1 ±0.5 | 76.1 ±2.1 | 18.9 | 28.4 |
| dens+est_hrd | 99.8 ±0.1 | 99.6 ±0.3 | 61.5 ±2.1 | 21.7 | 51.3 |
| dens+disj3 | 99.7 ±0.2 | 98.9 ±0.3 | 97.4 ±0.6 | 45.5 | 91.4 |
| dens+est_hrd+disj3 | 99.8 ±0.1 | 99.4 ±0.2 | 99.5 ±0.3 | 50.0 | 108.0 |
| srv100 | 99.4 ±0.4 | 0.0 ±0.0 | 45.8 ±2.2 | 7.9 | 17.9 |
| srv100+est_hrd | 99.9 ±0.1 | 1.6 ±0.3 | 57.7 ±2.1 | 9.3 | 24.4 |

S=10000, no attack:

| defence | P(find) | /find per search | publish find+provide |
|---|---|---|---|
| base | 100.0 | 8.23 | 8.2+10.0 |
| dens | 100.0 | 8.58 | 8.7+10.7 |
| est_tab | 100.0 | 8.25 | 8.3+12.9 |
| est_hrd | 100.0 | 9.58 | 9.6+15.0 |
| disj3 | 100.0 | 24.91 | 24.9+18.0 |
| disj4 | 100.0 | 32.39 | 32.5+22.1 |
| k20+s2k | 100.0 | 20.12 | 8.2+20.0 |
| dens+est_hrd | 100.0 | 9.96 | 10.0+15.4 |
| dens+disj3 | 100.0 | 24.99 | 25.0+18.1 |
| dens+est_hrd+disj3 | 100.0 | 29.37 | 29.4+30.8 |
| srv100 | 100.0 | 8.24 | 8.2+10.0 |
| srv100+est_hrd | 100.0 | 9.59 | 9.6+15.0 |
