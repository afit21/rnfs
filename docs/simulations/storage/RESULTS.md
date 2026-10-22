# Provider-record store policy: simulation results

Code: `sim.py` (store policies), `run.py` (workload, attacker, metrics), `experiments.py` (batteries; raw rows in
`raw/*.jsonl`). Python stdlib only. This file is appended to as each battery finishes.

## Model and assumptions

**One DHT server**, capacity `C` records (the budget). Records are modelled per (publisher, republish group): a
publisher republishes a group's keys together, so a group is a count plus one last-stored time. Every policy studied
picks victims per publisher (largest holder, newest publisher, least recently stored group, or a random record), so
this loses nothing that matters and lets `C` be realistic (20k–3M) at any attacker size. Time step 1 h; honest
republishes happen at their exact (fractional) times; expiry is checked before each event.

**Honest publishers** (default 1000 established on this server, seed-varied):
- record counts heavy-tailed: 85% "a handful" (1 + Exp(mean 4)), 14% log-uniform 10–300, 1% mirrors log-uniform
  1,000–10,000 (≈55k records per 1000 publishers; mirrors are ~70% of records);
- 30% slow nodes: content republished every 72 h + U(0, 6 h) lookup-pacing delay; fast nodes every 24 h + U(0, 1 h);
- 20% also keep 1–3 names, each its own group republished every 72 h × (1 ± 10%, fixed per key) + the node's delay;
  so the longest honest gap is 72 × 1.1 + 6 = 85.2 h = 3.55 d;
- 3% Known (contacts / seeded bootstrap), chosen at random (can include a mirror);
- established publishers were first seen 10–365 days ago (uniform) and start Earned; churn: 1%/day leave (stop
  republishing), 1%/day new publishers join (same size distribution), including during attacks;
- `C` = initial honest records / load; load 2% (large network, disk-bounded store), 50%, 90% (small mirror-heavy
  network), 110% in peacetime (honest demand exceeds the budget).

**Attacker**: N ∈ {10, 100, 1000, 10000} identities, starts on day 7. Each hour every identity refreshes all its
records (so they never expire) and tries to store `C/N` new ones (max rate: the whole budget every hour; the real
wire limit is far higher, so this is a lower bound on pressure; a 'lora' rate of 11k records/h is used where
noted). Fresh identities are first seen at the attack's start; pre-aged identities (7 days by default) already
qualify as Earned / senior when it starts. Attacks last 1 day, 7 days or continuously (21 days). After it ends the
attacker stops refreshing and its records expire.

**Policies**
- **A today** (rnfs 0.1.x, `providers.py`): main area 90% + newcomer pool 10%; per-publisher cap `C/100` once
  first seen ≥ 1 day, `C/1000` before (min 10); when main is full a publisher first seen before the newest one in
  main evicts that newest publisher's least recently stored record, others go to the pool, a full pool evicts from
  the biggest pooled holder and refuses a publisher already holding as many. First-seen = first successful store.
- **B IPFS-like**: global capacity, no per-publisher caps, evict LRU (least recently stored) or a random record.
- **C tiers** (roadmap): Known / Earned / Newcomer with floors; anyone stores while there is room; when full,
  reclaim from the lowest tier above its floor first, within a tier from the publisher furthest over its fair share
  (max-min: the largest holder; a storer already holding as many as the largest is refused); a lower tier never
  evicts a higher one. Earned = publisher whose records were held and refreshed on ≥ 3 distinct (UTC) days.
  Newcomer records expire after 4 days, others after PROVIDER_TTL (7 days).
  - **S1** = exactly that: a lower tier never evicts a higher one, so a floor only protects what a tier already
    holds.
  - **S2** = floors are guaranteed: a tier *below* its floor may also reclaim from a tier *above* its floor
    (otherwise a pool emptied by Earned growth can never be refilled; see E1).
- **D variants**: listed in each battery.

**Metrics**: presence = records present / records wanted, record-weighted, for: Known; established (non-Known,
present at start); "new" (joined in the 7 days before the attack); "joined during attack". Small = ≤ 20 records.
Recovery = hours after the attack ends until all-honest presence is back within 1 point of its pre-attack
baseline. Writes/s = record inserts + evictions + expiries (refresh updates reported separately: they can be
coalesced). Lapse = a record of an active honest publisher that expired (must be 0).

## E0. Peacetime (no attacker)

Honest presence = records present / records honest publishers want stored (record-weighted), averaged over days 1–28; 3 seeds. "new" = publishers that joined during the run (1%/day). Refused/evicted = honest records per day refused at store time / evicted to make room. Lapses = records of active honest publishers that expired (must be 0).

**Established honest presence %** (columns: honest load / capacity)

| policy | 2% | 50% | 90% | 110% |
|---|---|---|---|---|
| A today | 100 | 52 | 40 | 37 |
| B ipfs-lru | 100 | 100 | 99 | 89 |
| C S1 (roadmap) | 100 | 100 | 99 | 89 |
| C S2 | 100 | 100 | 99 | 86 |
| D w90 | 100 | 100 | 100 | 89 |
| D sen | 100 | 100 | 100 | 93 |
| D promo+w90 | 100 | 100 | 100 | 89 |
| D days5 | 100 | 100 | 99 | 87 |
| D ncap | 100 | 100 | 99 | 88 |
| D nrate100 | 100 | 100 | 99 | 88 |
| D ticket24 | 100 | 100 | 99 | 87 |

**Newly joined honest presence %** (columns: honest load / capacity)

| policy | 2% | 50% | 90% | 110% |
|---|---|---|---|---|
| A today | 99 | 74 | 67 | 65 |
| B ipfs-lru | 100 | 100 | 100 | 95 |
| C S1 (roadmap) | 100 | 100 | 98 | 58 |
| C S2 | 100 | 100 | 100 | 96 |
| D w90 | 100 | 100 | 95 | 78 |
| D sen | 100 | 100 | 93 | 45 |
| D promo+w90 | 100 | 100 | 95 | 78 |
| D days5 | 100 | 100 | 100 | 95 |
| D ncap | 98 | 85 | 82 | 78 |
| D nrate100 | 87 | 87 | 87 | 84 |
| D ticket24 | 100 | 100 | 100 | 84 |

**Honest records refused per day** (columns: honest load / capacity)

| policy | 2% | 50% | 90% | 110% |
|---|---|---|---|---|
| A today | 82 | 19217 | 24764 | 26066 |
| B ipfs-lru | 0 | 0 | 0 | 0 |
| C S1 (roadmap) | 0 | 0 | 738 | 5861 |
| C S2 | 0 | 0 | 696 | 5525 |
| D w90 | 0 | 0 | 581 | 6141 |
| D sen | 0 | 0 | 603 | 6304 |
| D promo+w90 | 0 | 0 | 579 | 6149 |
| D days5 | 0 | 0 | 685 | 5509 |
| D ncap | 164 | 772 | 1285 | 5830 |
| D nrate100 | 757 | 757 | 1225 | 5805 |
| D ticket24 | 0 | 0 | 697 | 5550 |

**Honest records evicted per day** (columns: honest load / capacity)

| policy | 2% | 50% | 90% | 110% |
|---|---|---|---|---|
| A today | 0 | 0 | 0 | 0 |
| B ipfs-lru | 0 | 0 | 990 | 13892 |
| C S1 (roadmap) | 0 | 0 | 144 | 420 |
| C S2 | 0 | 0 | 173 | 601 |
| D w90 | 0 | 0 | 287 | 956 |
| D sen | 0 | 0 | 342 | 1268 |
| D promo+w90 | 0 | 0 | 290 | 953 |
| D days5 | 0 | 0 | 186 | 614 |
| D ncap | 0 | 0 | 116 | 410 |
| D nrate100 | 0 | 0 | 119 | 421 |
| D ticket24 | 0 | 0 | 165 | 566 |

**Honest lapses (expiry of an active publisher's record)** (columns: honest load / capacity)

| policy | 2% | 50% | 90% | 110% |
|---|---|---|---|---|
| A today | 0 | 0 | 0 | 0 |
| B ipfs-lru | 0 | 0 | 0 | 0 |
| C S1 (roadmap) | 0 | 0 | 0 | 0 |
| C S2 | 0 | 0 | 0 | 0 |
| D w90 | 0 | 0 | 0 | 0 |
| D sen | 0 | 0 | 0 | 0 |
| D promo+w90 | 0 | 0 | 0 | 0 |
| D days5 | 0 | 0 | 0 | 0 |
| D ncap | 0 | 0 | 0 | 0 |
| D nrate100 | 0 | 0 | 0 | 0 |
| D ticket24 | 0 | 0 | 0 | 0 |

**Store writes/s (inserts+evictions+expiries; refreshes excluded)** (columns: honest load / capacity)

| policy | 2% | 50% | 90% | 110% |
|---|---|---|---|---|
| A today | 0.01 | 0.01 | 0.00 | 0.00 |
| B ipfs-lru | 0.01 | 0.01 | 0.03 | 0.32 |
| C S1 (roadmap) | 0.01 | 0.01 | 0.01 | 0.02 |
| C S2 | 0.01 | 0.01 | 0.01 | 0.02 |
| D w90 | 0.01 | 0.01 | 0.01 | 0.03 |
| D sen | 0.01 | 0.01 | 0.02 | 0.03 |
| D promo+w90 | 0.01 | 0.01 | 0.01 | 0.03 |
| D days5 | 0.01 | 0.01 | 0.01 | 0.02 |
| D ncap | 0.01 | 0.01 | 0.01 | 0.02 |
| D nrate100 | 0.01 | 0.01 | 0.01 | 0.02 |
| D ticket24 | 0.01 | 0.01 | 0.01 | 0.02 |

**Refresh updates/s (coalescable)** (columns: honest load / capacity)

| policy | 2% | 50% | 90% | 110% |
|---|---|---|---|---|
| A today | 0.50 | 0.28 | 0.22 | 0.21 |
| B ipfs-lru | 0.50 | 0.50 | 0.49 | 0.35 |
| C S1 (roadmap) | 0.50 | 0.50 | 0.50 | 0.43 |
| C S2 | 0.50 | 0.50 | 0.50 | 0.44 |
| D w90 | 0.50 | 0.50 | 0.50 | 0.43 |
| D sen | 0.50 | 0.50 | 0.49 | 0.42 |
| D promo+w90 | 0.50 | 0.50 | 0.50 | 0.43 |
| D days5 | 0.50 | 0.50 | 0.50 | 0.44 |
| D ncap | 0.50 | 0.50 | 0.49 | 0.44 |
| D nrate100 | 0.50 | 0.50 | 0.49 | 0.44 |
| D ticket24 | 0.50 | 0.50 | 0.50 | 0.44 |

**A big mirror joins a small network** (200 publishers, store 70% full, mirror = 30% of capacity, joins on day 7)

| policy | mirror stored after 1 day % | mirror stored at end % | others (est) after % | honest evictions caused |
|---|---|---|---|---|
| A today | 1 | 3 | 32 | 0 |
| B ipfs-lru | 100 | 100 | 97 | 10676 |
| C S1 (roadmap) | 98 | 94 | 99 | 718 |
| C S2 | 98 | 94 | 99 | 782 |
| D w90 | 98 | 88 | 100 | 1054 |
| D sen | 98 | 100 | 100 | 1687 |
| D promo+w90 | 98 | 88 | 100 | 1054 |
| D days5 | 98 | 94 | 99 | 810 |
| D ncap | 0 | 94 | 99 | 663 |
| D nrate100 | 3 | 94 | 99 | 730 |
| D ticket24 | 98 | 94 | 99 | 709 |

## E4. Newcomer expiry vs honest lapses

Peacetime, store 50% full (no contention), 500 established publishers, 10%/day new publishers (so many are Newcomers), 50% keep names, 30% slow. Slow-node republish delay U(0, d) on top of 72 h (names: 72 h × (1 ± 10%) + delay). Cell: honest records that lapsed (expired while their publisher was still active) over 35 days, summed over 5 seeds. Longest honest gap = 79.2 h + d.

| newcomer expiry (d) | delay ≤6 h (max gap 85.2 h = 3.55 d) | delay ≤12 h (max gap 91.2 h = 3.80 d) | delay ≤24 h (max gap 103.2 h = 4.30 d) |
|---|---|---|---|
| 3.3 | 442 | 34375 | 64249 |
| 3.5 | 14 | 302 | 44149 |
| 3.6 | 0 | 133 | 34241 |
| 3.75 | 0 | 6 | 20194 |
| 4.0 | 0 | 0 | 113 |
| 4.5 | 0 | 0 | 0 |
| 5.0 | 0 | 0 | 0 |

**E4 reading.** The lapse threshold sits exactly at the longest honest gap: 3.5 d already lapses records of slow
nodes keeping names (72 h + 10% jitter + a few hours of pacing = 3.55 d). 4 d is safe while slow-node republish
delay stays under ~16.8 h (names) / 24 h (content); 4.5 d tolerates ~28.8 h. Pacing alone (≤10 lookups per round,
30·n s between rounds) keeps per-key gaps near the 72 h round for slow nodes, so the delay comes from restarts and
long offline spells; a node offline > expiry − 72 h loses its Newcomer records anyway and re-stores at its next
round. Only Newcomer records are affected (Earned/Known keep 7 d).

## E1. Baselines (A, B) and the proposed tier model (C, 30/60/10, newcomer expiry 4 d, Earned = 3 distinct days, max-min fair share), S1 = "a lower tier never evicts a higher one" literally, S2 = a tier below its floor may also reclaim from tiers above theirs

### Load 2% (honest records / capacity)

Established honest publishers, mean presence % during a **continuous** attack (21 days); pre-attack baseline in brackets

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| B ipfs-lru | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] |
| B ipfs-rand | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] |
| C S1 (roadmap) | 100 [100] | 100 [100] | 84 [100] | 33 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] |
| C S2 | 100 [100] | 100 [100] | 84 [100] | 33 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] |

Established, last day of the continuous attack % (small publishers ≤20 records / large)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) |
| B ipfs-lru | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) |
| B ipfs-rand | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) |
| C S1 (roadmap) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) |
| C S2 | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) |

Known (contacts/bootstrap) min % · publishers that joined before the attack (<7 d old) mean % · joined during the attack mean %  (continuous)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 100·100·99 | 100·100·96 | 100·100·52 | 100·100·24 | 100·100·99 | 100·98·97 | 100·29·53 | 100·9·25 |
| B ipfs-lru | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 |
| B ipfs-rand | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 |
| C S1 (roadmap) | 100·100·81 | 100·100·81 | 100·93·80 | 100·37·47 | 100·100·79 | 100·100·79 | 100·92·78 | 100·29·45 |
| C S2 | 100·100·100 | 100·100·100 | 100·93·99 | 100·37·67 | 100·100·100 | 100·100·100 | 100·92·99 | 100·29·67 |

Time until established honest presence < 90% / < 50% (continuous attack)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| B ipfs-lru | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h |
| B ipfs-rand | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h |
| C S1 (roadmap) | never / never | never / never | 2.0d / never | 2.0d / 2.0d | never / never | never / never | 0h / never | 0h / 0h |
| C S2 | never / never | never / never | 2.0d / never | 2.0d / 2.0d | never / never | never / never | 0h / never | 0h / 0h |

1-day attack: min established % · min all honest % · recovery to baseline−1pt after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·93·0h | 100·93·7.7d | 100·93·7.7d |
| B ipfs-lru | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d |
| B ipfs-rand | 0·0·2.8d | 0·0·2.9d | 0·0·2.9d | 0·0·2.9d | 0·0·2.8d | 0·0·2.9d | 0·0·2.9d | 0·0·2.9d |
| C S1 (roadmap) | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 83·84·8.7d | 28·29·9.9d |
| C S2 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 83·84·8.7d | 28·29·9.9d |

7-day attack: mean established % · mean all honest % · recovery after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 100·100·0h | 100·100·0h | 100·99·7.5d | 100·98·7.5d | 100·100·0h | 100·99·0h | 100·94·7.7d | 100·92·7.7d |
| B ipfs-lru | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d |
| B ipfs-rand | 3·3·2.6d | 3·3·2.8d | 3·3·2.9d | 3·3·2.9d | 3·3·2.6d | 3·3·2.8d | 3·3·2.9d | 3·3·2.9d |
| C S1 (roadmap) | 100·99·0h | 100·99·0h | 88·88·8.9d | 48·49·8.9d | 100·99·0h | 100·99·0h | 83·83·8.9d | 27·29·8.9d |
| C S2 | 100·100·0h | 100·100·0h | 88·89·8.9d | 48·50·8.9d | 100·100·0h | 100·100·0h | 83·84·8.9d | 27·30·8.9d |

Store writes/s during the continuous attack: all (inserts+evictions+expiries) / honest-only; attacker refreshes not counted

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 0.2/0.01 | 2.3/0.01 | 3.8/0.01 | 4.6/0.01 | 0.2/0.01 | 2.8/0.03 | 12.7/0.01 | 25.9/0.01 |
| B ipfs-lru | 2105.9/1.65 | 2105.9/1.65 | 2106.2/1.65 | 2110.7/1.65 | 2105.9/1.65 | 2105.9/1.65 | 2106.2/1.65 | 2110.7/1.65 |
| B ipfs-rand | 2105.9/1.64 | 2105.9/1.63 | 2105.9/1.63 | 2105.9/1.62 | 2105.9/1.64 | 2105.9/1.63 | 2105.9/1.63 | 2105.9/1.62 |
| C S1 (roadmap) | 2.2/0.01 | 2.2/0.01 | 2.6/0.02 | 4.5/0.04 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.1/0.04 |
| C S2 | 2.2/0.01 | 2.2/0.01 | 2.6/0.02 | 5.2/0.05 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.8/0.05 |

### Load 50% (honest records / capacity)

Established honest publishers, mean presence % during a **continuous** attack (21 days); pre-attack baseline in brackets

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 51 [52] | 51 [52] | 51 [52] | 51 [52] | 51 [52] | 51 [52] | 51 [52] | 51 [52] |
| B ipfs-lru | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] |
| B ipfs-rand | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] | 3 [100] |
| C S1 (roadmap) | 98 [100] | 48 [100] | 26 [100] | 16 [100] | 98 [100] | 43 [100] | 19 [100] | 7 [100] |
| C S2 | 98 [100] | 48 [100] | 26 [100] | 16 [100] | 98 [100] | 43 [100] | 19 [100] | 7 [100] |

Established, last day of the continuous attack % (small publishers ≤20 records / large)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 51 (100/48 min) | 51 (100/48 min) | 51 (100/48 min) | 51 (100/48 min) | 51 (100/48 min) | 51 (100/48 min) | 51 (100/48 min) | 51 (100/48 min) |
| B ipfs-lru | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) |
| B ipfs-rand | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) |
| C S1 (roadmap) | 98 (100/98 min) | 43 (100/40 min) | 19 (100/14 min) | 7 (98/2 min) | 98 (100/98 min) | 43 (100/40 min) | 19 (100/14 min) | 7 (98/2 min) |
| C S2 | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) |

Known (contacts/bootstrap) min % · publishers that joined before the attack (<7 d old) mean % · joined during the attack mean %  (continuous)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 100·51·76 | 100·51·49 | 100·51·17 | 100·51·3 | 100·51·76 | 100·27·50 | 100·6·17 | 100·1·3 |
| B ipfs-lru | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 |
| B ipfs-rand | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 |
| C S1 (roadmap) | 100·100·81 | 100·50·60 | 100·28·35 | 100·15·13 | 100·100·79 | 100·43·58 | 100·20·37 | 100·7·21 |
| C S2 | 100·100·100 | 100·49·80 | 100·28·56 | 100·15·36 | 100·100·100 | 100·43·80 | 100·20·59 | 100·7·43 |

Time until established honest presence < 90% / < 50% (continuous attack)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never |
| B ipfs-lru | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h |
| B ipfs-rand | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h |
| C S1 (roadmap) | never / never | 2.0d / 2.0d | 2.0d / 2.0d | 2.0d / 2.0d | never / never | 0h / 0h | 0h / 0h | 0h / 0h |
| C S2 | never / never | 2.0d / 2.0d | 2.0d / 2.0d | 2.0d / 2.0d | never / never | 0h / 0h | 0h / 0h | 0h / 0h |

1-day attack: min established % · min all honest % · recovery to baseline−1pt after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 52·53·0h | 52·53·0h | 52·53·0h | 52·53·0h | 52·53·0h | 52·49·3h | 52·49·7.7d | 52·49·7.7d |
| B ipfs-lru | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d |
| B ipfs-rand | 0·0·7.2d | 0·0·7.2d | 0·0·7.2d | 0·0·7.3d | 0·0·7.2d | 0·0·7.2d | 0·0·7.2d | 0·0·7.3d |
| C S1 (roadmap) | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 99·99·14h | 44·45·8.7d | 20·22·9.9d | 8·10·9.9d |
| C S2 | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 99·99·14h | 44·45·8.7d | 20·22·9.9d | 8·10·9.9d |

7-day attack: mean established % · mean all honest % · recovery after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 52·53·0h | 52·52·0h | 52·52·7.5d | 52·52·7.5d | 52·53·0h | 52·51·7.5d | 52·48·7.7d | 52·48·7.7d |
| B ipfs-lru | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d |
| B ipfs-rand | 3·3·7.0d | 3·3·7.0d | 3·3·7.0d | 3·3·7.0d | 3·3·7.0d | 3·3·7.0d | 3·3·7.0d | 3·3·7.0d |
| C S1 (roadmap) | 99·98·15h | 60·61·8.9d | 42·43·8.9d | 34·34·8.9d | 99·98·15h | 44·45·8.9d | 20·21·8.9d | 7·9·8.9d |
| C S2 | 99·99·15h | 60·61·8.9d | 42·44·8.9d | 34·36·8.9d | 99·99·15h | 44·46·8.9d | 20·22·8.9d | 7·10·8.9d |

Store writes/s during the continuous attack: all (inserts+evictions+expiries) / honest-only; attacker refreshes not counted

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 0.0/0.01 | 0.1/0.01 | 0.4/0.01 | 3.3/0.01 | 0.0/0.01 | 0.2/0.01 | 0.7/0.01 | 3.8/0.01 |
| B ipfs-lru | 85.8/1.65 | 85.8/1.65 | 86.0/1.65 | 90.5/1.65 | 85.8/1.65 | 85.8/1.65 | 86.0/1.65 | 90.5/1.65 |
| B ipfs-rand | 85.8/1.64 | 85.8/1.63 | 85.8/1.63 | 85.8/1.62 | 85.8/1.64 | 85.8/1.63 | 85.8/1.63 | 85.8/1.62 |
| C S1 (roadmap) | 0.1/0.01 | 0.3/0.03 | 0.5/0.04 | 3.6/0.05 | 0.1/0.01 | 0.2/0.03 | 0.4/0.04 | 3.7/0.05 |
| C S2 | 0.1/0.01 | 0.3/0.04 | 0.6/0.06 | 3.9/0.06 | 0.1/0.01 | 0.2/0.04 | 0.5/0.06 | 4.0/0.06 |

### Load 90% (honest records / capacity)

Established honest publishers, mean presence % during a **continuous** attack (21 days); pre-attack baseline in brackets

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 37 [38] | 37 [38] | 37 [38] | 37 [38] | 37 [38] | 37 [38] | 37 [38] | 37 [38] |
| B ipfs-lru | 3 [99] | 3 [99] | 3 [99] | 3 [99] | 3 [99] | 3 [99] | 3 [99] | 3 [99] |
| B ipfs-rand | 3 [98] | 3 [98] | 3 [98] | 3 [98] | 3 [98] | 3 [98] | 3 [98] | 3 [98] |
| C S1 (roadmap) | 71 [98] | 37 [98] | 22 [98] | 19 [98] | 68 [98] | 31 [98] | 14 [98] | 5 [98] |
| C S2 | 69 [98] | 36 [98] | 21 [98] | 13 [98] | 67 [98] | 30 [98] | 14 [98] | 5 [98] |

Established, last day of the continuous attack % (small publishers ≤20 records / large)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 37 (100/33 min) | 37 (100/33 min) | 37 (100/33 min) | 37 (100/33 min) | 37 (100/33 min) | 37 (100/33 min) | 37 (100/33 min) | 37 (100/33 min) |
| B ipfs-lru | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) |
| B ipfs-rand | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) | 3 (1/0 min) |
| C S1 (roadmap) | 69 (100/65 min) | 31 (100/26 min) | 14 (100/9 min) | 10 (100/6 min) | 69 (100/65 min) | 31 (100/26 min) | 14 (100/9 min) | 5 (82/1 min) |
| C S2 | 64 (100/62 min) | 29 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) | 64 (100/62 min) | 29 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) |

Known (contacts/bootstrap) min % · publishers that joined before the attack (<7 d old) mean % · joined during the attack mean %  (continuous)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 100·38·61 | 100·38·40 | 100·38·13 | 100·38·0 | 100·38·61 | 100·18·41 | 100·4·13 | 100·0·1 |
| B ipfs-lru | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 |
| B ipfs-rand | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 | 0·4·3 |
| C S1 (roadmap) | 100·72·77 | 100·39·41 | 100·22·19 | 100·16·2 | 100·69·77 | 100·33·48 | 100·15·31 | 100·5·18 |
| C S2 | 100·71·98 | 100·39·67 | 100·22·46 | 100·13·29 | 100·68·98 | 100·33·69 | 100·15·53 | 100·5·41 |

Time until established honest presence < 90% / < 50% (continuous attack)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h |
| B ipfs-lru | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h |
| B ipfs-rand | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h |
| C S1 (roadmap) | 2.0d / never | 2.0d / 2.0d | 2.0d / 2.0d | 2.0d / 2.1d | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |
| C S2 | 0h / never | 0h / 2.0d | 0h / 2.0d | 0h / 2.0d | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |

1-day attack: min established % · min all honest % · recovery to baseline−1pt after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 38·39·0h | 38·39·0h | 38·39·0h | 38·39·0h | 38·39·0h | 38·36·0h | 38·36·7.7d | 38·36·7.7d |
| B ipfs-lru | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d | 0·0·2.7d |
| B ipfs-rand | 0·0·8.7d | 0·0·8.7d | 0·0·8.7d | 0·0·8.7d | 0·0·8.7d | 0·0·8.7d | 0·0·8.7d | 0·0·8.7d |
| C S1 (roadmap) | 96·96·4.7d | 95·94·4.7d | 95·94·5.5d | 100·94·5.5d | 69·70·8.7d | 31·33·9.9d | 15·17·9.9d | 6·8·9.9d |
| C S2 | 94·94·5.5d | 94·94·5.5d | 94·93·5.5d | 94·93·5.5d | 69·70·8.7d | 31·33·9.9d | 15·17·9.9d | 6·8·9.9d |

7-day attack: mean established % · mean all honest % · recovery after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 38·39·0h | 38·39·0h | 38·38·7.0d | 38·38·7.6d | 38·39·0h | 38·37·7.5d | 38·35·7.7d | 38·35·7.7d |
| B ipfs-lru | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d | 3·3·2.6d |
| B ipfs-rand | 3·3·8.9d | 3·3·8.9d | 3·3·8.9d | 3·3·8.9d | 3·3·8.9d | 3·3·8.9d | 3·3·8.9d | 3·3·8.9d |
| C S1 (roadmap) | 78·78·8.9d | 51·52·8.9d | 39·39·8.9d | 34·35·8.9d | 70·70·8.9d | 31·33·8.9d | 15·16·8.9d | 5·7·8.9d |
| C S2 | 76·77·8.9d | 49·51·8.9d | 37·39·8.9d | 31·32·8.9d | 69·71·8.9d | 31·34·8.9d | 15·17·8.9d | 5·8·8.9d |

Store writes/s during the continuous attack: all (inserts+evictions+expiries) / honest-only; attacker refreshes not counted

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| A today | 0.0/0.01 | 0.1/0.01 | 0.4/0.01 | 1.0/0.01 | 0.0/0.01 | 0.1/0.01 | 0.5/0.01 | 1.3/0.01 |
| B ipfs-lru | 48.4/1.65 | 48.4/1.65 | 48.8/1.65 | 51.6/1.65 | 48.4/1.65 | 48.4/1.65 | 48.8/1.65 | 51.6/1.65 |
| B ipfs-rand | 48.4/1.64 | 48.4/1.63 | 48.4/1.63 | 48.4/1.62 | 48.4/1.64 | 48.4/1.63 | 48.4/1.63 | 48.4/1.62 |
| C S1 (roadmap) | 0.1/0.02 | 0.1/0.04 | 0.4/0.05 | 1.2/0.05 | 0.1/0.02 | 0.1/0.04 | 0.5/0.05 | 1.1/0.05 |
| C S2 | 0.1/0.04 | 0.2/0.05 | 0.5/0.06 | 1.9/0.07 | 0.1/0.03 | 0.2/0.05 | 0.5/0.06 | 2.0/0.07 |

**E1 reading.**
- **B (IPFS-like) collapses** under any max-rate flood: with no notion of who stored what, an attacker that rewrites
  the store every hour leaves ~3% of honest records (whatever was republished in the last hour). It also costs the
  most writes (the whole budget per hour).
- **A (today) protects established publishers completely** whenever they were first seen before the attacker's
  identities: "older evicts newest" means neither fresh nor 7-day pre-aged identities can touch them (2% load:
  100% in every cell). Its costs are elsewhere: (1) the per-publisher cap `C/100` caps mirrors even in peacetime
  (baseline 52% at 50% load, 38% at 90%: large publishers are refused, see E0); (2) publishers younger than the
  attacker (joined before the attack but after the pre-aged identities) are the first evicted (pre-aged N=10000:
  9% / 1% / 0% at 2/50/90% load); (3) newcomers during an attack share the 10% pool with every identity.
- **C as written (max-min fair share among Earned) is weaker than today for established publishers.** A *fresh*
  flood is confined to Newcomer only until its identities have held a record on 3 distinct days (~2 days);
  after that they are Earned and take a per-identity fair share, so large established publishers drop below 50%
  at day 2 for N ≥ 100 (50% load) and N ≥ 10000 (2% load). Pre-aged identities do it from hour 0. Small publishers
  (≤ 20 records) and Known keep ~100%: max-min protects whoever is below the water level. Recovery after the
  attack ~9 days (the attacker's Earned records keep the 7-day TTL).
- **S1 vs S2.** S1 ("a lower tier never evicts a higher one", literally) lets Earned growth take the Newcomer
  floor permanently: newcomers joining during an attack get 2–21% (S1) vs 29–100% (S2). S2's cost: under a fresh
  flood at 90% load, Newcomers reclaim their floor from Earned records above Earned's floor (est. 94–96% during a
  1-day attack instead of 100%). S2 is used from here on.

## E3. Variants of the tier model (all S2, 30/60/10, 4 d newcomer expiry unless named): within-Earned eviction order (w = weighted by age capped at 30/90/365 d, sen = newest publisher first, rand-rec = random record), Earned criterion (days2/5/7 distinct days, age1 = first seen ≥1 d, noEarned), promotion rate limit (promo = at most max(10, 2% of Earned) promotions/day, FIFO; promo5 = max(5, 1%)), admission (ticket24 = a never-stored newcomer must wait 24 h when the store is full; ncap = newcomer holds ≤ C/1000; nrate100 = newcomer inserts ≤100/day)

### Load 2% (honest records / capacity)

Established honest publishers, mean presence % during a **continuous** attack (21 days); pre-attack baseline in brackets

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 100 [100] | 100 [100] | 84 [100] | 33 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] |
| D w30 | 100 [100] | 100 [100] | 99 [100] | 53 [100] | 100 [100] | 100 [100] | 96 [100] | 35 [100] |
| D w90 | 100 [100] | 100 [100] | 100 [100] | 78 [100] | 100 [100] | 100 [100] | 100 [100] | 60 [100] |
| D w365 | 100 [100] | 100 [100] | 100 [100] | 94 [100] | 100 [100] | 100 [100] | 100 [100] | 87 [100] |
| D sen | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| D rand-rec | 14 [100] | 14 [100] | – [–] | – [–] | 5 [100] | 6 [100] | – [–] | – [–] |
| D days2 | 100 [100] | 100 [100] | 83 [100] | 30 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] |
| D days5 | 100 [100] | 100 [100] | 86 [100] | 40 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] |
| D days7 | 100 [100] | 100 [100] | 87 [100] | 47 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] |
| D age1 | 100 [100] | 100 [100] | 83 [100] | 30 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] |
| D noEarned | 100 [100] | 100 [100] | 83 [100] | 26 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] |
| D promo | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 98 [100] | 98 [100] |
| D promo+w90 | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| D promo+w365 | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| D promo+sen | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| D promo5+w90 | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| D nopre | 100 [100] | 100 [100] | 95 [100] | 94 [100] | 100 [100] | 100 [100] | 94 [100] | 94 [100] |
| D promo+nopre | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| D ticket24 | 100 [100] | 100 [100] | 84 [100] | 33 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] |
| D ncap | 100 [100] | 100 [100] | 84 [100] | 33 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] |
| D nrate100 | 100 [100] | 100 [100] | 84 [100] | 33 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] |

Established, last day of the continuous attack % (small publishers ≤20 records / large)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) |
| D w30 | 100 (100/100 min) | 100 (100/100 min) | 95 (100/94 min) | 30 (100/27 min) | 100 (100/100 min) | 100 (100/100 min) | 86 (100/85 min) | 27 (100/24 min) |
| D w90 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 53 (100/50 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 44 (100/41 min) |
| D w365 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 85 (100/84 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 76 (100/75 min) |
| D sen | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) |
| D rand-rec | 5 (2/1 min) | 5 (3/1 min) | – (–/– min) | – (–/– min) | 5 (3/1 min) | 5 (3/1 min) | – (–/– min) | – (–/– min) |
| D days2 | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) |
| D days5 | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) |
| D days7 | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) |
| D age1 | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) |
| D noEarned | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) |
| D promo | 100 (100/100 min) | 100 (100/100 min) | 98 (100/98 min) | 99 (100/98 min) | 100 (100/100 min) | 100 (100/100 min) | 89 (100/88 min) | 89 (100/88 min) |
| D promo+w90 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) |
| D promo+w365 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) |
| D promo+sen | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) |
| D promo5+w90 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) |
| D nopre | 100 (100/100 min) | 100 (100/100 min) | 85 (100/84 min) | 81 (100/80 min) | 100 (100/100 min) | 100 (100/100 min) | 85 (100/84 min) | 80 (100/79 min) |
| D promo+nopre | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) |
| D ticket24 | 100 (100/100 min) | 100 (100/100 min) | 82 (100/79 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) |
| D ncap | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) |
| D nrate100 | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) |

Known (contacts/bootstrap) min % · publishers that joined before the attack (<7 d old) mean % · joined during the attack mean %  (continuous)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 100·100·100 | 100·100·100 | 100·93·99 | 100·37·67 | 100·100·100 | 100·100·100 | 100·92·99 | 100·29·67 |
| D w30 | 100·100·100 | 100·100·99 | 100·100·97 | 100·40·63 | 100·100·100 | 100·100·99 | 100·75·87 | 100·27·59 |
| D w90 | 100·100·100 | 100·100·99 | 100·100·97 | 100·40·63 | 100·100·100 | 100·100·99 | 100·75·87 | 100·27·59 |
| D w365 | 100·100·100 | 100·100·99 | 100·100·97 | 100·40·63 | 100·100·100 | 100·100·99 | 100·75·87 | 100·27·59 |
| D sen | 100·100·34 | 100·100·34 | 100·100·34 | 100·100·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 |
| D rand-rec | 100·15·38 | 100·16·38 | –·–·– | –·–·– | 100·7·38 | 100·8·38 | –·–·– | –·–·– |
| D days2 | 100·100·100 | 100·100·100 | 100·93·98 | 100·33·61 | 100·100·100 | 100·100·100 | 100·92·98 | 100·29·61 |
| D days5 | 100·100·100 | 100·100·100 | 100·94·100 | 100·42·70 | 100·100·100 | 100·100·100 | 100·92·100 | 100·32·73 |
| D days7 | 100·100·100 | 100·100·100 | 100·94·100 | 100·44·69 | 100·100·100 | 100·100·100 | 100·93·100 | 100·37·78 |
| D age1 | 100·100·100 | 100·100·100 | 100·93·98 | 100·33·61 | 100·100·100 | 100·100·100 | 100·92·98 | 100·29·61 |
| D noEarned | 100·100·100 | 100·100·100 | 100·92·97 | 100·29·54 | 100·100·100 | 100·100·100 | 100·92·97 | 100·29·54 |
| D promo | 100·100·100 | 100·100·100 | 100·100·57 | 100·96·36 | 100·100·100 | 100·100·100 | 100·100·62 | 100·89·30 |
| D promo+w90 | 100·100·100 | 100·100·99 | 100·100·57 | 100·96·36 | 100·100·100 | 100·100·99 | 100·100·62 | 100·89·30 |
| D promo+w365 | 100·100·100 | 100·100·99 | 100·100·57 | 100·96·36 | 100·100·100 | 100·100·99 | 100·100·62 | 100·89·30 |
| D promo+sen | 100·100·34 | 100·100·45 | 100·100·57 | 100·96·36 | 100·1·34 | 100·1·34 | 100·18·62 | 100·15·30 |
| D promo5+w90 | 100·100·100 | 100·100·100 | 100·100·59 | 100·97·40 | 100·100·100 | 100·100·99 | 100·100·56 | 100·90·33 |
| D nopre | 100·100·100 | 100·100·100 | 100·100·100 | 100·99·100 | 100·100·100 | 100·100·100 | 100·100·100 | 100·99·100 |
| D promo+nopre | 100·100·100 | 100·100·100 | 100·100·57 | 100·96·36 | 100·100·100 | 100·100·100 | 100·100·62 | 100·89·30 |
| D ticket24 | 100·100·89 | 100·100·89 | 100·93·89 | 100·37·57 | 100·100·89 | 100·100·89 | 100·92·89 | 100·29·57 |
| D ncap | 100·100·98 | 100·100·98 | 100·93·97 | 100·37·64 | 100·100·98 | 100·100·98 | 100·92·97 | 100·29·64 |
| D nrate100 | 100·100·86 | 100·100·86 | 100·93·85 | 100·37·52 | 100·100·86 | 100·100·86 | 100·92·85 | 100·29·52 |

Time until established honest presence < 90% / < 50% (continuous attack)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | never / never | never / never | 2.0d / never | 2.0d / 2.0d | never / never | never / never | 0h / never | 0h / 0h |
| D w30 | never / never | never / never | never / never | 3.0d / 8.0d | never / never | never / never | 18.0d / never | 0h / 24h |
| D w90 | never / never | never / never | never / never | 7.0d / never | never / never | never / never | never / never | 0h / 15.0d |
| D w365 | never / never | never / never | never / never | 16.0d / never | never / never | never / never | never / never | 8.0d / never |
| D sen | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| D rand-rec | 2.0d / 2.0d | 2.0d / 2.0d | never / never | never / never | 1h / 1h | 1h / 1h | never / never | never / never |
| D days2 | never / never | never / never | 24h / never | 24h / 24h | never / never | never / never | 0h / never | 0h / 0h |
| D days5 | never / never | never / never | 4.0d / never | 4.0d / 4.0d | never / never | never / never | 0h / never | 0h / 0h |
| D days7 | never / never | never / never | 6.0d / never | 6.0d / 6.0d | never / never | never / never | 0h / never | 0h / 0h |
| D age1 | never / never | never / never | 24h / never | 24h / 24h | never / never | never / never | 0h / never | 0h / 0h |
| D noEarned | never / never | never / never | 0h / never | 0h / 0h | never / never | never / never | 0h / never | 0h / 0h |
| D promo | never / never | never / never | never / never | never / never | never / never | never / never | 19.8d / never | 20.0d / never |
| D promo+w90 | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| D promo+w365 | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| D promo+sen | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| D promo5+w90 | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| D nopre | never / never | never / never | 18.1d / never | 18.1d / never | never / never | never / never | 18.1d / never | 18.1d / never |
| D promo+nopre | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| D ticket24 | never / never | never / never | 2.0d / never | 2.0d / 2.0d | never / never | never / never | 0h / never | 0h / 0h |
| D ncap | never / never | never / never | 2.0d / never | 2.0d / 2.0d | never / never | never / never | 0h / never | 0h / 0h |
| D nrate100 | never / never | never / never | 2.0d / never | 2.0d / 2.0d | never / never | never / never | 0h / never | 0h / 0h |

1-day attack: min established % · min all honest % · recovery to baseline−1pt after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 83·84·8.7d | 28·29·9.9d |
| D w30 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 100·96·7.7d | 54·53·8.7d |
| D w90 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 100·96·7.7d | 88·84·8.7d |
| D w365 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 100·96·7.7d | 96·92·8.7d |
| D sen | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d |
| D rand-rec | 100·100·0h | 100·100·0h | –·–·never | 100·95·0h | 1·4·2.7d | 1·4·2.7d | –·–·never | –·–·never |
| D days2 | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 83·84·8.7d | 28·29·9.9d |
| D days5 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·31h | 100·100·0h | 100·100·0h | 83·84·8.7d | 28·34·9.9d |
| D days7 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·3.4d | 100·100·0h | 100·100·0h | 83·84·8.7d | 28·34·9.9d |
| D age1 | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 83·84·8.7d | 28·29·9.9d |
| D noEarned | 100·100·0h | 100·100·0h | 83·84·5.5d | 28·29·6.9d | 100·100·0h | 100·100·0h | 83·84·5.5d | 28·29·6.9d |
| D promo | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·7h | 100·100·0h | 100·100·0h | 100·96·7h | 100·95·4.7d |
| D promo+w90 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·7h | 100·100·0h | 100·100·0h | 100·96·7h | 100·95·4.7d |
| D promo+w365 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·7h | 100·100·0h | 100·100·0h | 100·96·7h | 100·95·4.7d |
| D promo+sen | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·7h | 100·94·7.7d | 100·94·7.7d | 100·95·7h | 100·95·4.7d |
| D promo5+w90 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·7h | 100·100·0h | 100·100·0h | 100·95·31h | 100·95·4.7d |
| D nopre | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| D promo+nopre | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·7h | 100·100·0h | 100·100·0h | 100·96·7h | 100·95·4.7d |
| D ticket24 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 83·84·8.7d | 28·29·9.9d |
| D ncap | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 83·84·8.7d | 28·29·9.9d |
| D nrate100 | 100·95·0h | 100·95·0h | 100·95·0h | 100·95·0h | 100·95·0h | 100·95·0h | 83·79·7.7d | 28·29·8.7d |

7-day attack: mean established % · mean all honest % · recovery after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 100·100·0h | 100·100·0h | 88·89·8.9d | 48·50·8.9d | 100·100·0h | 100·100·0h | 83·84·8.9d | 27·30·8.9d |
| D w30 | 100·100·0h | 100·100·0h | 100·100·0h | 82·81·8.9d | 100·100·0h | 100·100·0h | 100·97·7.7d | 45·45·8.9d |
| D w90 | 100·100·0h | 100·100·0h | 100·100·0h | 99·95·7.7d | 100·100·0h | 100·100·0h | 100·97·7.7d | 76·73·8.9d |
| D w365 | 100·100·0h | 100·100·0h | 100·100·0h | 100·96·7.7d | 100·100·0h | 100·100·0h | 100·97·7.7d | 94·89·8.9d |
| D sen | 100·99·7.5d | 100·99·7.5d | 100·99·7.5d | 100·99·7.5d | 100·92·7.7d | 100·92·7.7d | 100·92·7.7d | 100·92·7.7d |
| D rand-rec | 32·35·2.6d | 33·35·2.6d | –·–·never | –·–·never | 6·9·2.6d | 6·9·2.6d | –·–·never | –·–·never |
| D days2 | 100·100·0h | 100·100·0h | 85·87·8.9d | 38·40·8.9d | 100·100·0h | 100·100·0h | 83·84·8.9d | 27·30·8.9d |
| D days5 | 100·100·0h | 100·100·0h | 93·93·8.9d | 69·69·8.9d | 100·100·0h | 100·100·0h | 83·84·8.9d | 27·31·8.9d |
| D days7 | 100·100·0h | 100·100·0h | 98·98·8.9d | 90·87·8.9d | 100·100·0h | 100·100·0h | 83·84·8.9d | 27·33·8.9d |
| D age1 | 100·100·0h | 100·100·0h | 85·87·8.9d | 38·40·8.9d | 100·100·0h | 100·100·0h | 83·84·8.9d | 27·30·8.9d |
| D noEarned | 100·100·0h | 100·100·0h | 83·84·5.7d | 27·29·5.7d | 100·100·0h | 100·100·0h | 83·84·5.7d | 27·29·5.7d |
| D promo | 100·100·0h | 100·100·0h | 100·99·7h | 100·99·3.4d | 100·100·0h | 100·100·0h | 100·99·7h | 100·98·3.4d |
| D promo+w90 | 100·100·0h | 100·100·0h | 100·99·7h | 100·99·3.4d | 100·100·0h | 100·100·0h | 100·99·7h | 100·98·3.4d |
| D promo+w365 | 100·100·0h | 100·100·0h | 100·99·7h | 100·99·3.4d | 100·100·0h | 100·100·0h | 100·99·7h | 100·98·3.4d |
| D promo+sen | 100·99·7.5d | 100·100·0h | 100·99·7h | 100·99·3.4d | 100·92·7.7d | 100·92·7.7d | 100·93·4.7d | 100·93·4.7d |
| D promo5+w90 | 100·100·0h | 100·100·0h | 100·99·7h | 100·99·4.4d | 100·100·0h | 100·100·0h | 100·99·7h | 100·98·4.4d |
| D nopre | 100·100·0h | 100·100·0h | 98·98·7.7d | 98·98·7.7d | 100·100·0h | 100·100·0h | 98·98·7.7d | 98·98·7.7d |
| D promo+nopre | 100·100·0h | 100·100·0h | 100·99·7h | 100·99·3.4d | 100·100·0h | 100·100·0h | 100·99·7h | 100·98·3.4d |
| D ticket24 | 100·99·0h | 100·99·0h | 87·88·8.9d | 48·50·8.9d | 100·99·0h | 100·99·0h | 83·84·8.9d | 27·30·8.9d |
| D ncap | 100·100·0h | 100·100·0h | 88·89·8.9d | 48·50·8.9d | 100·100·0h | 100·100·0h | 83·84·8.9d | 27·30·8.9d |
| D nrate100 | 100·99·0h | 100·99·0h | 88·88·8.9d | 48·49·8.9d | 100·99·0h | 100·99·0h | 83·83·8.9d | 27·29·8.9d |

Store writes/s during the continuous attack: all (inserts+evictions+expiries) / honest-only; attacker refreshes not counted

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 2.2/0.01 | 2.2/0.01 | 2.6/0.02 | 5.2/0.05 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.8/0.05 |
| D w30 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 6.0/0.05 | 2.2/0.01 | 2.2/0.01 | 2.3/0.03 | 2.4/0.05 |
| D w90 | 2.2/0.01 | 2.2/0.01 | 2.6/0.02 | 6.0/0.04 | 2.2/0.01 | 2.2/0.01 | 2.3/0.02 | 2.5/0.04 |
| D w365 | 2.2/0.01 | 2.2/0.01 | 2.6/0.02 | 5.7/0.03 | 2.2/0.01 | 2.2/0.01 | 2.3/0.02 | 2.4/0.03 |
| D sen | 2.2/0.02 | 2.2/0.02 | 2.2/0.02 | 2.4/0.02 | 10.2/0.03 | 19.1/0.03 | 26.7/0.03 | 27.6/0.03 |
| D rand-rec | 1909.3/1.44 | 1909.3/1.43 | 0.0/0.00 | 0.0/0.00 | 2105.9/1.59 | 2105.9/1.58 | 0.0/0.00 | 0.0/0.00 |
| D days2 | 2.2/0.01 | 2.2/0.01 | 2.6/0.02 | 5.1/0.05 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.8/0.05 |
| D days5 | 2.2/0.01 | 2.2/0.01 | 2.6/0.02 | 5.0/0.05 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.9/0.05 |
| D days7 | 2.2/0.01 | 2.2/0.01 | 2.6/0.02 | 5.0/0.05 | 2.2/0.01 | 2.2/0.01 | 2.6/0.02 | 5.1/0.05 |
| D age1 | 2.2/0.01 | 2.2/0.01 | 2.6/0.02 | 5.1/0.05 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.8/0.05 |
| D noEarned | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.1/0.04 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.1/0.04 |
| D promo | 2.2/0.01 | 15.5/0.01 | 17.1/0.01 | 14.5/0.01 | 2.2/0.01 | 2.2/0.01 | 14.4/0.01 | 13.9/0.01 |
| D promo+w90 | 2.2/0.01 | 16.8/0.01 | 17.7/0.01 | 14.5/0.01 | 2.2/0.01 | 2.2/0.01 | 14.7/0.01 | 13.8/0.01 |
| D promo+w365 | 2.2/0.01 | 16.8/0.01 | 17.7/0.01 | 14.5/0.01 | 2.2/0.01 | 2.2/0.01 | 14.7/0.01 | 13.8/0.01 |
| D promo+sen | 2.2/0.02 | 9.2/0.02 | 7.8/0.01 | 8.8/0.01 | 10.2/0.02 | 19.2/0.02 | 24.8/0.01 | 17.3/0.01 |
| D promo5+w90 | 11.2/0.01 | 22.4/0.01 | 15.1/0.01 | 13.1/0.01 | 2.2/0.01 | 9.0/0.01 | 12.2/0.01 | 12.5/0.01 |
| D nopre | 2.2/0.01 | 2.2/0.01 | 2.2/0.02 | 2.4/0.02 | 2.1/0.01 | 2.1/0.01 | 2.1/0.02 | 2.1/0.02 |
| D promo+nopre | 2.2/0.01 | 9.2/0.01 | 7.8/0.01 | 8.8/0.01 | 2.1/0.01 | 2.1/0.01 | 8.8/0.01 | 8.8/0.01 |
| D ticket24 | 2.2/0.01 | 3.7/0.01 | 4.0/0.02 | 6.2/0.05 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.4/0.05 |
| D ncap | 2.2/0.01 | 2.6/0.01 | 2.5/0.02 | 5.2/0.05 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.8/0.05 |
| D nrate100 | 2.2/0.01 | 2.2/0.01 | 2.7/0.02 | 5.6/0.04 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.3/0.04 |

### Load 50% (honest records / capacity)

Established honest publishers, mean presence % during a **continuous** attack (21 days); pre-attack baseline in brackets

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 98 [100] | 48 [100] | 26 [100] | 16 [100] | 98 [100] | 43 [100] | 19 [100] | 7 [100] |
| D w30 | 100 [100] | 75 [100] | 35 [100] | 21 [100] | 100 [100] | 59 [100] | 23 [100] | 10 [100] |
| D w90 | 100 [100] | 94 [100] | 49 [100] | 26 [100] | 100 [100] | 86 [100] | 33 [100] | 15 [100] |
| D w365 | 100 [100] | 99 [100] | 68 [100] | 33 [100] | 100 [100] | 96 [100] | 52 [100] | 21 [100] |
| D sen | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| D rand-rec | 14 [100] | 14 [100] | – [–] | – [–] | 5 [100] | 5 [100] | – [–] | – [–] |
| D days2 | 98 [100] | 46 [100] | 23 [100] | 11 [100] | 98 [100] | 43 [100] | 19 [100] | 7 [100] |
| D days5 | 98 [100] | 54 [100] | 34 [100] | 25 [100] | 98 [100] | 43 [100] | 19 [100] | 7 [100] |
| D days7 | 99 [100] | 59 [100] | 42 [100] | 33 [100] | 98 [100] | 43 [100] | 19 [100] | 7 [100] |
| D age1 | 98 [100] | 46 [100] | 23 [100] | 11 [100] | 98 [100] | 43 [100] | 19 [100] | 7 [100] |
| D noEarned | 98 [100] | 43 [100] | 19 [100] | 7 [100] | 98 [100] | 43 [100] | 19 [100] | 7 [100] |
| D promo | 98 [100] | 50 [100] | 41 [100] | 48 [100] | 98 [100] | 43 [100] | 25 [100] | 29 [100] |
| D promo+w90 | 100 [100] | 94 [100] | 74 [100] | 75 [100] | 100 [100] | 86 [100] | 50 [100] | 51 [100] |
| D promo+w365 | 100 [100] | 99 [100] | 88 [100] | 88 [100] | 100 [100] | 96 [100] | 70 [100] | 70 [100] |
| D promo+sen | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| D promo5+w90 | 100 [100] | 94 [100] | 88 [100] | 88 [100] | 100 [100] | 86 [100] | 67 [100] | 68 [100] |
| D nopre | 98 [100] | 94 [100] | 94 [100] | 94 [100] | 100 [100] | 94 [100] | 94 [100] | 94 [100] |
| D promo+nopre | 98 [100] | 70 [100] | 95 [100] | 100 [100] | 100 [100] | 94 [100] | 92 [100] | 99 [100] |
| D ticket24 | 98 [100] | 49 [100] | 27 [100] | 16 [100] | 98 [100] | 43 [100] | 19 [100] | 7 [100] |
| D ncap | 98 [100] | 48 [100] | 26 [100] | 16 [100] | 98 [100] | 43 [100] | 19 [100] | 7 [100] |
| D nrate100 | 98 [100] | 48 [100] | 26 [100] | 16 [100] | 98 [100] | 43 [100] | 19 [100] | 7 [100] |

Established, last day of the continuous attack % (small publishers ≤20 records / large)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) |
| D w30 | 100 (100/100 min) | 51 (100/48 min) | 21 (100/17 min) | 8 (100/4 min) | 98 (100/98 min) | 44 (100/41 min) | 19 (100/15 min) | 7 (99/3 min) |
| D w90 | 100 (100/100 min) | 80 (100/78 min) | 29 (100/26 min) | 13 (100/9 min) | 100 (100/100 min) | 71 (100/69 min) | 26 (100/23 min) | 11 (100/7 min) |
| D w365 | 100 (100/100 min) | 95 (100/95 min) | 46 (100/42 min) | 19 (100/15 min) | 100 (100/100 min) | 92 (100/91 min) | 39 (100/36 min) | 17 (100/13 min) |
| D sen | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) |
| D rand-rec | 4 (2/1 min) | 5 (3/1 min) | – (–/– min) | – (–/– min) | 4 (3/1 min) | 5 (3/1 min) | – (–/– min) | – (–/– min) |
| D days2 | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) |
| D days5 | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) |
| D days7 | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) |
| D age1 | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) |
| D noEarned | 97 (100/97 min) | 43 (100/40 min) | 19 (100/14 min) | 7 (98/2 min) | 97 (100/97 min) | 43 (100/40 min) | 19 (100/14 min) | 7 (98/2 min) |
| D promo | 97 (100/97 min) | 42 (100/39 min) | 23 (100/19 min) | 23 (100/19 min) | 97 (100/97 min) | 42 (100/39 min) | 20 (100/16 min) | 20 (100/16 min) |
| D promo+w90 | 100 (100/100 min) | 80 (100/78 min) | 39 (100/35 min) | 39 (100/35 min) | 100 (100/100 min) | 71 (100/69 min) | 29 (100/25 min) | 29 (100/25 min) |
| D promo+w365 | 100 (100/100 min) | 95 (100/95 min) | 61 (100/59 min) | 62 (100/59 min) | 100 (100/100 min) | 92 (100/91 min) | 44 (100/41 min) | 45 (100/41 min) |
| D promo+sen | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) |
| D promo5+w90 | 100 (100/100 min) | 79 (100/78 min) | 58 (100/56 min) | 59 (100/56 min) | 100 (100/100 min) | 71 (100/69 min) | 41 (100/38 min) | 42 (100/38 min) |
| D nopre | 97 (100/97 min) | 81 (100/80 min) | 81 (100/80 min) | 80 (100/79 min) | 100 (100/100 min) | 80 (100/79 min) | 80 (100/79 min) | 80 (100/79 min) |
| D promo+nopre | 97 (100/97 min) | 62 (100/60 min) | 88 (100/86 min) | 99 (100/99 min) | 100 (100/100 min) | 80 (100/79 min) | 79 (100/77 min) | 99 (100/99 min) |
| D ticket24 | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) |
| D ncap | 98 (100/98 min) | 43 (100/40 min) | 18 (100/14 min) | 7 (98/2 min) | 98 (100/98 min) | 43 (100/40 min) | 18 (100/14 min) | 7 (98/2 min) |
| D nrate100 | 98 (100/98 min) | 43 (100/40 min) | 18 (100/14 min) | 7 (98/2 min) | 98 (100/98 min) | 43 (100/40 min) | 18 (100/14 min) | 7 (98/2 min) |

Known (contacts/bootstrap) min % · publishers that joined before the attack (<7 d old) mean % · joined during the attack mean %  (continuous)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 100·100·100 | 100·49·80 | 100·28·56 | 100·15·36 | 100·100·100 | 100·43·80 | 100·20·59 | 100·7·43 |
| D w30 | 100·100·98 | 100·52·71 | 100·29·52 | 100·16·33 | 100·98·95 | 100·37·67 | 100·17·50 | 100·6·39 |
| D w90 | 100·100·98 | 100·49·69 | 100·29·51 | 100·16·33 | 100·98·95 | 100·34·65 | 100·17·49 | 100·6·39 |
| D w365 | 100·100·98 | 100·49·68 | 100·27·50 | 100·16·33 | 100·98·95 | 100·33·64 | 100·16·49 | 100·6·39 |
| D sen | 100·100·34 | 100·100·34 | 100·100·32 | 100·99·27 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 |
| D rand-rec | 100·15·38 | 100·15·38 | –·–·– | –·–·– | 100·7·38 | 100·7·38 | –·–·– | –·–·– |
| D days2 | 100·100·100 | 100·46·76 | 100·24·52 | 100·11·29 | 100·100·100 | 100·43·76 | 100·20·53 | 100·6·33 |
| D days5 | 100·100·100 | 100·53·81 | 100·33·60 | 100·21·40 | 100·100·100 | 100·45·84 | 100·23·67 | 100·11·55 |
| D days7 | 100·100·100 | 100·54·80 | 100·35·58 | 100·25·40 | 100·100·100 | 100·50·87 | 100·30·73 | 100·21·64 |
| D age1 | 100·100·100 | 100·46·76 | 100·24·52 | 100·11·29 | 100·100·100 | 100·43·76 | 100·20·53 | 100·6·33 |
| D noEarned | 100·100·100 | 100·44·71 | 100·20·45 | 100·6·18 | 100·100·100 | 100·44·71 | 100·20·45 | 100·6·18 |
| D promo | 100·100·100 | 100·52·74 | 100·38·23 | 100·44·6 | 100·100·100 | 100·43·80 | 100·17·22 | 100·16·4 |
| D promo+w90 | 100·100·98 | 100·52·66 | 100·39·23 | 100·42·6 | 100·98·95 | 100·34·65 | 100·14·22 | 100·13·4 |
| D promo+w365 | 100·100·98 | 100·51·65 | 100·38·23 | 100·41·6 | 100·98·95 | 100·33·64 | 100·13·22 | 100·12·4 |
| D promo+sen | 100·100·34 | 100·100·37 | 100·94·23 | 100·92·6 | 100·1·34 | 100·1·34 | 100·5·22 | 100·4·4 |
| D promo5+w90 | 100·100·98 | 100·59·69 | 100·51·23 | 100·55·7 | 100·98·95 | 100·34·73 | 100·17·19 | 100·19·4 |
| D nopre | 100·100·100 | 100·99·100 | 100·95·97 | 100·93·91 | 100·100·100 | 100·99·100 | 100·99·100 | 100·99·100 |
| D promo+nopre | 100·100·100 | 100·72·92 | 100·94·23 | 100·92·6 | 100·100·100 | 100·99·100 | 100·86·22 | 100·83·4 |
| D ticket24 | 100·100·89 | 100·50·70 | 100·28·44 | 100·15·26 | 100·100·89 | 100·43·70 | 100·20·47 | 100·7·32 |
| D ncap | 100·100·86 | 100·49·66 | 100·28·43 | 100·15·22 | 100·100·86 | 100·43·66 | 100·20·45 | 100·7·30 |
| D nrate100 | 100·100·86 | 100·49·65 | 100·28·42 | 100·15·22 | 100·100·86 | 100·43·65 | 100·20·45 | 100·7·29 |

Time until established honest presence < 90% / < 50% (continuous attack)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | never / never | 2.0d / 2.0d | 2.0d / 2.0d | 2.0d / 2.0d | never / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D w30 | never / never | 6.0d / never | 2.0d / 3.0d | 2.0d / 2.0d | never / never | 0h / 16.0d | 0h / 0h | 0h / 0h |
| D w90 | never / never | 16.0d / never | 2.0d / 7.0d | 2.0d / 2.0d | never / never | 8.0d / never | 0h / 0h | 0h / 0h |
| D w365 | never / never | never / never | 3.6d / 17.0d | 2.0d / 2.0d | never / never | never / never | 0h / 10.0d | 0h / 0h |
| D sen | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| D rand-rec | 2.0d / 2.0d | 2.0d / 2.0d | never / never | never / never | 0h / 1h | 0h / 1h | never / never | never / never |
| D days2 | never / never | 24h / 24h | 24h / 24h | 24h / 24h | never / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D days5 | never / never | 4.0d / 4.0d | 4.0d / 4.0d | 4.0d / 4.0d | never / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D days7 | never / never | 6.0d / 6.0d | 6.0d / 6.0d | 6.0d / 6.0d | never / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D age1 | never / never | 24h / 24h | 24h / 24h | 24h / 24h | never / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D noEarned | never / never | 0h / 0h | 0h / 0h | 0h / 0h | never / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D promo | never / never | 2.1d / 4.2d | 2.6d / 4.3d | 5.0d / 6.0d | never / never | 0h / 0h | 2h / 4h | 22h / 34h |
| D promo+w90 | never / never | 16.0d / never | 8.1d / 16.0d | 8.6d / 16.0d | never / never | 8.0d / never | 14h / 8.4d | 25h / 8.6d |
| D promo+w365 | never / never | never / never | 12.0d / never | 12.0d / never | never / never | never / never | 4.0d / 17.0d | 4.0d / 17.1d |
| D promo+sen | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| D promo5+w90 | never / never | 16.0d / never | 12.0d / never | 12.6d / never | never / never | 8.0d / never | 4.0d / 15.5d | 4.2d / 16.0d |
| D nopre | never / never | 18.1d / never | 18.1d / never | 18.1d / never | never / never | 18.1d / never | 18.1d / never | 18.1d / never |
| D promo+nopre | never / never | 3.6d / never | 18.2d / never | never / never | never / never | 18.1d / never | 13.2d / never | never / never |
| D ticket24 | never / never | 2.0d / 3.0d | 2.0d / 2.0d | 2.0d / 2.0d | never / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D ncap | never / never | 2.0d / 2.0d | 2.0d / 2.0d | 2.0d / 2.0d | never / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D nrate100 | never / never | 2.0d / 2.0d | 2.0d / 2.0d | 2.0d / 2.0d | never / never | 0h / 0h | 0h / 0h | 0h / 0h |

1-day attack: min established % · min all honest % · recovery to baseline−1pt after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 99·99·14h | 44·45·8.7d | 20·22·9.9d | 8·10·9.9d |
| D w30 | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 100·98·2.3d | 82·79·8.7d | 31·32·9.9d | 14·16·9.9d |
| D w90 | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 100·98·2.3d | 100·95·7.7d | 47·46·8.7d | 21·22·9.9d |
| D w365 | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 100·98·2.3d | 100·95·7.7d | 71·68·8.7d | 28·28·9.9d |
| D sen | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d |
| D rand-rec | 100·100·0h | 100·96·0h | –·–·never | 100·94·12h | 1·4·7.0d | 1·4·7.1d | –·–·never | –·–·never |
| D days2 | 100·100·0h | 100·100·0h | 100·99·0h | 100·99·0h | 99·99·14h | 44·45·8.7d | 20·21·9.9d | 8·9·9.9d |
| D days5 | 100·100·0h | 100·96·31h | 100·94·31h | 100·94·2.9d | 99·99·14h | 44·49·8.7d | 20·26·9.9d | 7·15·9.9d |
| D days7 | 100·100·0h | 100·96·3.4d | 100·94·3.4d | 100·93·4.7d | 99·99·14h | 44·49·8.7d | 20·26·9.9d | 7·15·9.9d |
| D age1 | 100·100·0h | 100·100·0h | 100·99·0h | 100·99·0h | 99·99·14h | 44·45·8.7d | 20·21·9.9d | 8·9·9.9d |
| D noEarned | 99·99·0h | 44·46·5.5d | 20·21·6.9d | 8·9·6.9d | 99·99·0h | 44·46·5.5d | 20·21·6.9d | 8·9·6.9d |
| D promo | 100·100·0h | 100·96·7h | 100·94·7h | 100·94·36h | 99·99·14h | 44·45·8.7d | 33·33·8.7d | 95·89·5.5d |
| D promo+w90 | 100·100·0h | 100·96·7h | 100·94·7h | 100·94·36h | 100·98·2.3d | 100·95·7.7d | 89·84·7.5d | 95·89·4.9d |
| D promo+w365 | 100·100·0h | 100·96·7h | 100·94·7h | 100·94·36h | 100·98·2.3d | 100·95·7.7d | 96·90·4.9d | 96·90·4.9d |
| D promo+sen | 100·100·0h | 100·96·7h | 100·94·7h | 100·94·36h | 100·94·7.7d | 100·94·7.7d | 100·93·4.7d | 100·93·4.8d |
| D promo5+w90 | 100·100·0h | 100·96·7h | 100·94·7h | 100·94·36h | 100·98·2.3d | 100·95·7h | 100·94·4.7d | 100·94·4.7d |
| D nopre | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| D promo+nopre | 100·100·0h | 100·96·7h | 100·94·7h | 100·94·36h | 100·100·0h | 100·100·0h | 100·94·4.7d | 100·94·4.7d |
| D ticket24 | 100·100·0h | 100·97·0h | 100·95·0h | 100·94·12h | 99·99·14h | 44·45·8.7d | 20·22·9.9d | 8·10·9.9d |
| D ncap | 100·95·0h | 100·95·0h | 100·94·0h | 100·94·0h | 99·94·0h | 44·44·8.7d | 20·21·8.7d | 8·10·8.7d |
| D nrate100 | 100·95·0h | 100·95·0h | 100·94·0h | 100·94·0h | 99·94·0h | 44·44·8.7d | 20·21·8.7d | 8·9·8.7d |

7-day attack: mean established % · mean all honest % · recovery after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 99·99·15h | 60·61·8.9d | 42·44·8.9d | 34·36·8.9d | 99·99·15h | 44·46·8.9d | 20·22·8.9d | 7·10·8.9d |
| D w30 | 100·100·0h | 96·94·8.9d | 58·58·8.9d | 42·42·8.9d | 100·99·0h | 73·71·8.9d | 28·29·8.9d | 12·14·8.9d |
| D w90 | 100·100·0h | 100·97·7.7d | 74·73·8.9d | 47·47·8.9d | 100·99·0h | 97·92·8.9d | 41·41·8.9d | 19·20·8.9d |
| D w365 | 100·100·0h | 100·97·7.7d | 90·87·8.9d | 54·54·8.9d | 100·99·0h | 99·94·7.7d | 63·60·8.9d | 25·26·8.9d |
| D sen | 100·99·7.5d | 100·99·7.5d | 100·99·7.5d | 100·99·7.5d | 100·92·7.7d | 100·92·7.7d | 100·92·7.7d | 100·92·7.7d |
| D rand-rec | 32·35·6.7d | 33·35·6.8d | –·–·never | –·–·never | 5·8·6.7d | 6·9·6.7d | –·–·never | –·–·never |
| D days2 | 99·99·15h | 52·53·8.9d | 31·33·8.9d | 21·22·8.9d | 99·99·15h | 44·46·8.9d | 20·22·8.9d | 7·9·8.9d |
| D days5 | 99·99·15h | 76·76·8.9d | 65·66·8.9d | 60·60·8.9d | 99·99·15h | 44·47·8.9d | 19·24·8.9d | 7·12·8.9d |
| D days7 | 100·100·15h | 92·90·8.9d | 88·85·8.9d | 87·82·8.9d | 99·99·15h | 43·48·8.9d | 19·25·8.9d | 7·14·8.9d |
| D age1 | 99·99·15h | 52·53·8.9d | 31·33·8.9d | 21·22·8.9d | 99·99·15h | 44·46·8.9d | 20·22·8.9d | 7·9·8.9d |
| D noEarned | 99·99·15h | 44·46·5.7d | 20·21·5.7d | 7·9·5.7d | 99·99·15h | 44·46·5.7d | 20·21·5.7d | 7·9·5.7d |
| D promo | 99·99·15h | 66·67·8.9d | 69·68·8.9d | 88·86·8.9d | 99·99·15h | 44·46·8.9d | 31·31·8.9d | 42·41·8.9d |
| D promo+w90 | 100·100·0h | 100·97·7.7d | 100·96·4.7d | 100·96·4.7d | 100·99·0h | 97·92·8.9d | 73·68·8.9d | 75·70·8.9d |
| D promo+w365 | 100·100·0h | 100·97·7.7d | 100·96·4.7d | 100·96·4.7d | 100·99·0h | 99·94·7.7d | 90·84·7.7d | 91·85·7.7d |
| D promo+sen | 100·99·7.5d | 100·99·0h | 100·98·4.4d | 100·97·4.4d | 100·92·7.7d | 100·92·7.7d | 100·91·4.7d | 100·92·4.9d |
| D promo5+w90 | 100·100·0h | 100·98·7.7d | 100·97·4.7d | 100·97·4.7d | 100·99·0h | 96·92·7.7d | 90·84·8.9d | 92·86·7.7d |
| D nopre | 99·99·15h | 98·98·7.7d | 98·98·7.7d | 98·97·7.7d | 100·100·0h | 98·98·7.7d | 98·98·7.7d | 98·98·7.7d |
| D promo+nopre | 99·99·15h | 86·86·8.9d | 99·97·4.7d | 100·97·4.4d | 100·100·0h | 98·98·7.7d | 98·95·5.7d | 100·96·4.7d |
| D ticket24 | 99·99·15h | 62·63·8.9d | 43·45·8.9d | 34·36·8.9d | 99·98·15h | 44·45·8.9d | 20·22·8.9d | 7·10·8.9d |
| D ncap | 99·98·0h | 60·61·8.9d | 42·44·8.9d | 34·35·8.9d | 99·98·0h | 44·45·8.9d | 20·21·8.9d | 7·9·8.9d |
| D nrate100 | 99·98·0h | 60·61·8.9d | 42·44·8.9d | 34·35·8.9d | 99·98·0h | 44·45·8.9d | 20·21·8.9d | 7·9·8.9d |

Store writes/s during the continuous attack: all (inserts+evictions+expiries) / honest-only; attacker refreshes not counted

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 0.1/0.01 | 0.3/0.04 | 0.6/0.06 | 3.9/0.06 | 0.1/0.01 | 0.2/0.04 | 0.5/0.06 | 4.0/0.06 |
| D w30 | 0.2/0.02 | 0.3/0.04 | 0.7/0.06 | 4.1/0.06 | 0.2/0.02 | 0.3/0.05 | 0.3/0.06 | 0.3/0.06 |
| D w90 | 0.2/0.02 | 0.3/0.03 | 0.6/0.05 | 2.8/0.06 | 0.2/0.02 | 0.2/0.03 | 0.3/0.05 | 0.3/0.06 |
| D w365 | 0.2/0.02 | 0.2/0.02 | 0.6/0.05 | 3.6/0.06 | 0.2/0.02 | 0.2/0.02 | 0.3/0.05 | 0.3/0.06 |
| D sen | 0.1/0.02 | 0.1/0.02 | 0.2/0.02 | 0.3/0.02 | 0.3/0.03 | 0.5/0.03 | 0.7/0.03 | 0.7/0.03 |
| D rand-rec | 77.7/1.44 | 77.7/1.43 | 0.0/0.00 | 0.0/0.00 | 85.8/1.59 | 85.8/1.59 | 0.0/0.00 | 0.0/0.00 |
| D days2 | 0.1/0.01 | 0.3/0.04 | 0.6/0.06 | 4.0/0.07 | 0.1/0.01 | 0.2/0.04 | 0.5/0.06 | 3.9/0.07 |
| D days5 | 0.1/0.01 | 0.3/0.04 | 0.6/0.06 | 3.9/0.07 | 0.1/0.01 | 0.2/0.04 | 0.5/0.05 | 4.0/0.06 |
| D days7 | 0.1/0.01 | 0.3/0.05 | 0.6/0.06 | 4.0/0.07 | 0.1/0.01 | 0.2/0.04 | 0.5/0.05 | 4.0/0.06 |
| D age1 | 0.1/0.01 | 0.3/0.04 | 0.6/0.06 | 4.0/0.07 | 0.1/0.01 | 0.2/0.04 | 0.5/0.06 | 3.9/0.07 |
| D noEarned | 0.2/0.02 | 0.2/0.03 | 0.4/0.04 | 3.7/0.05 | 0.2/0.02 | 0.2/0.03 | 0.4/0.04 | 3.7/0.05 |
| D promo | 0.1/0.01 | 0.6/0.04 | 0.9/0.04 | 3.3/0.04 | 0.1/0.01 | 0.2/0.04 | 0.8/0.04 | 3.2/0.04 |
| D promo+w90 | 0.2/0.02 | 0.6/0.03 | 0.8/0.03 | 3.2/0.04 | 0.2/0.02 | 0.2/0.03 | 0.6/0.04 | 3.0/0.04 |
| D promo+w365 | 0.2/0.02 | 0.6/0.02 | 0.7/0.03 | 3.2/0.03 | 0.2/0.02 | 0.2/0.02 | 0.6/0.03 | 3.0/0.03 |
| D promo+sen | 0.1/0.02 | 0.3/0.02 | 0.5/0.01 | 3.0/0.01 | 0.3/0.02 | 0.5/0.02 | 0.8/0.01 | 3.1/0.01 |
| D promo5+w90 | 0.4/0.02 | 0.7/0.03 | 0.7/0.03 | 3.2/0.03 | 0.2/0.02 | 0.4/0.04 | 0.6/0.03 | 3.3/0.03 |
| D nopre | 0.1/0.01 | 0.1/0.02 | 0.2/0.02 | 0.3/0.02 | 0.1/0.01 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 |
| D promo+nopre | 0.1/0.01 | 0.3/0.03 | 0.5/0.01 | 3.0/0.01 | 0.1/0.01 | 0.1/0.02 | 0.4/0.02 | 2.8/0.01 |
| D ticket24 | 0.2/0.01 | 0.3/0.04 | 0.6/0.05 | 3.8/0.06 | 0.1/0.01 | 0.2/0.04 | 0.5/0.05 | 3.8/0.06 |
| D ncap | 0.1/0.01 | 0.2/0.03 | 0.6/0.04 | 3.8/0.05 | 0.1/0.01 | 0.2/0.03 | 0.5/0.04 | 3.9/0.05 |
| D nrate100 | 0.1/0.01 | 0.2/0.03 | 0.5/0.04 | 3.8/0.05 | 0.1/0.01 | 0.2/0.03 | 0.5/0.04 | 3.9/0.05 |

### Load 90% (honest records / capacity)

Established honest publishers, mean presence % during a **continuous** attack (21 days); pre-attack baseline in brackets

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 69 [98] | 36 [98] | 21 [98] | 13 [98] | 67 [98] | 30 [98] | 14 [98] | 5 [98] |
| D w30 | 86 [100] | 53 [100] | 29 [100] | 18 [100] | 79 [100] | 39 [100] | 18 [100] | 7 [100] |
| D w90 | 96 [100] | 71 [100] | 37 [100] | 22 [100] | 93 [100] | 58 [100] | 26 [100] | 11 [100] |
| D w365 | 98 [100] | 84 [100] | 49 [100] | 28 [100] | 96 [100] | 76 [100] | 36 [100] | 17 [100] |
| D sen | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| D rand-rec | 13 [98] | 13 [98] | – [–] | – [–] | 5 [98] | 5 [98] | – [–] | – [–] |
| D days2 | 68 [98] | 33 [98] | 17 [98] | 9 [98] | 67 [98] | 30 [98] | 14 [98] | 5 [98] |
| D days5 | 71 [98] | 41 [98] | 28 [98] | 22 [98] | 67 [98] | 30 [98] | 14 [98] | 5 [98] |
| D days7 | 74 [100] | 48 [100] | 36 [100] | 30 [100] | 67 [100] | 30 [100] | 14 [100] | 5 [100] |
| D age1 | 68 [98] | 33 [98] | 17 [98] | 9 [98] | 67 [98] | 30 [98] | 14 [98] | 5 [98] |
| D noEarned | 68 [99] | 31 [99] | 14 [99] | 5 [99] | 68 [99] | 31 [99] | 14 [99] | 5 [99] |
| D promo | 69 [98] | 37 [98] | 32 [98] | 37 [98] | 67 [98] | 30 [98] | 20 [98] | 22 [98] |
| D promo+w90 | 96 [100] | 70 [100] | 56 [100] | 57 [100] | 93 [100] | 58 [100] | 35 [100] | 36 [100] |
| D promo+w365 | 98 [100] | 84 [100] | 69 [100] | 70 [100] | 96 [100] | 76 [100] | 48 [100] | 49 [100] |
| D promo+sen | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| D promo5+w90 | 96 [100] | 72 [100] | 68 [100] | 68 [100] | 93 [100] | 58 [100] | 46 [100] | 47 [100] |
| D nopre | 84 [98] | 84 [98] | 84 [98] | 69 [98] | 92 [98] | 92 [98] | 92 [98] | 92 [98] |
| D promo+nopre | 84 [98] | 65 [98] | 87 [98] | 88 [98] | 92 [98] | 92 [98] | 84 [98] | 88 [98] |
| D ticket24 | 69 [98] | 37 [98] | 24 [98] | 17 [98] | 67 [98] | 30 [98] | 14 [98] | 5 [98] |
| D ncap | 70 [99] | 36 [99] | 21 [99] | 13 [99] | 68 [99] | 31 [99] | 14 [99] | 5 [99] |
| D nrate100 | 70 [99] | 36 [99] | 21 [99] | 13 [99] | 68 [99] | 30 [99] | 14 [99] | 5 [99] |

Established, last day of the continuous attack % (small publishers ≤20 records / large)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 64 (100/62 min) | 29 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) | 64 (100/62 min) | 29 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) |
| D w30 | 72 (100/69 min) | 34 (100/30 min) | 16 (100/12 min) | 6 (92/2 min) | 68 (100/65 min) | 31 (100/27 min) | 14 (100/10 min) | 5 (86/1 min) |
| D w90 | 85 (100/84 min) | 52 (100/48 min) | 23 (100/19 min) | 10 (100/5 min) | 83 (100/81 min) | 46 (100/43 min) | 21 (100/17 min) | 8 (100/4 min) |
| D w365 | 92 (100/91 min) | 70 (100/67 min) | 32 (100/28 min) | 14 (100/10 min) | 90 (100/89 min) | 64 (100/61 min) | 28 (100/25 min) | 13 (100/8 min) |
| D sen | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) |
| D rand-rec | 4 (2/1 min) | 5 (3/1 min) | – (–/– min) | – (–/– min) | 4 (2/1 min) | 5 (2/1 min) | – (–/– min) | – (–/– min) |
| D days2 | 65 (100/63 min) | 30 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) | 65 (100/63 min) | 30 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) |
| D days5 | 64 (100/62 min) | 29 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) | 64 (100/62 min) | 29 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) |
| D days7 | 64 (100/62 min) | 29 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) | 64 (100/62 min) | 29 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) |
| D age1 | 65 (100/63 min) | 30 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) | 65 (100/63 min) | 30 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) |
| D noEarned | 66 (100/64 min) | 30 (100/26 min) | 14 (100/9 min) | 5 (82/1 min) | 66 (100/64 min) | 30 (100/26 min) | 14 (100/9 min) | 5 (82/1 min) |
| D promo | 64 (100/62 min) | 29 (100/26 min) | 18 (100/14 min) | 18 (100/14 min) | 64 (100/62 min) | 29 (100/26 min) | 15 (100/11 min) | 15 (100/11 min) |
| D promo+w90 | 85 (100/83 min) | 52 (100/48 min) | 28 (100/25 min) | 29 (100/25 min) | 83 (100/80 min) | 46 (100/43 min) | 23 (100/19 min) | 23 (100/19 min) |
| D promo+w365 | 92 (100/91 min) | 70 (100/67 min) | 42 (100/38 min) | 42 (100/39 min) | 90 (100/89 min) | 64 (100/61 min) | 31 (100/28 min) | 31 (100/28 min) |
| D promo+sen | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) |
| D promo5+w90 | 85 (100/83 min) | 51 (100/48 min) | 39 (100/36 min) | 39 (100/36 min) | 83 (100/80 min) | 46 (100/43 min) | 30 (100/26 min) | 30 (100/26 min) |
| D nopre | 76 (100/75 min) | 74 (100/73 min) | 74 (100/73 min) | 58 (100/56 min) | 82 (100/81 min) | 81 (100/80 min) | 81 (100/80 min) | 81 (100/80 min) |
| D promo+nopre | 76 (100/74 min) | 52 (100/50 min) | 83 (100/82 min) | 88 (100/87 min) | 82 (100/81 min) | 80 (100/79 min) | 78 (100/77 min) | 87 (100/86 min) |
| D ticket24 | 64 (100/62 min) | 29 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) | 64 (100/62 min) | 29 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) |
| D ncap | 67 (100/65 min) | 30 (100/26 min) | 14 (100/9 min) | 5 (82/1 min) | 67 (100/65 min) | 30 (100/26 min) | 14 (100/9 min) | 5 (82/1 min) |
| D nrate100 | 67 (100/65 min) | 30 (100/26 min) | 14 (100/9 min) | 5 (82/1 min) | 67 (100/65 min) | 30 (100/26 min) | 14 (100/9 min) | 5 (82/1 min) |

Known (contacts/bootstrap) min % · publishers that joined before the attack (<7 d old) mean % · joined during the attack mean %  (continuous)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 100·71·98 | 100·39·67 | 100·22·46 | 100·13·29 | 100·68·98 | 100·33·69 | 100·15·53 | 100·5·41 |
| D w30 | 100·54·77 | 100·33·62 | 100·17·41 | 100·7·27 | 100·48·74 | 100·29·61 | 100·13·45 | 100·5·36 |
| D w90 | 100·40·69 | 100·29·59 | 100·16·41 | 100·6·27 | 100·36·67 | 100·26·58 | 100·12·45 | 100·5·36 |
| D w365 | 100·37·67 | 100·25·55 | 100·14·39 | 100·6·27 | 100·33·65 | 100·22·55 | 100·12·44 | 100·5·36 |
| D sen | 100·83·34 | 100·83·32 | 100·83·27 | 100·83·23 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 |
| D rand-rec | 100·14·37 | 100·14·36 | –·–·– | –·–·– | 100·7·37 | 100·7·38 | –·–·– | –·–·– |
| D days2 | 100·70·97 | 100·36·64 | 100·18·42 | 100·9·24 | 100·68·97 | 100·33·64 | 100·14·45 | 100·5·29 |
| D days5 | 100·73·96 | 100·43·68 | 100·28·47 | 100·17·33 | 100·69·99 | 100·35·75 | 100·18·61 | 100·10·52 |
| D days7 | 100·71·91 | 100·44·64 | 100·30·45 | 100·21·32 | 100·71·99 | 100·39·79 | 100·25·68 | 100·19·61 |
| D age1 | 100·70·97 | 100·36·64 | 100·18·42 | 100·9·24 | 100·68·97 | 100·33·64 | 100·14·45 | 100·5·29 |
| D noEarned | 100·69·95 | 100·33·58 | 100·14·35 | 100·5·14 | 100·69·95 | 100·33·58 | 100·14·35 | 100·5·14 |
| D promo | 100·71·98 | 100·40·62 | 100·30·15 | 100·34·0 | 100·68·98 | 100·33·69 | 100·12·17 | 100·11·0 |
| D promo+w90 | 100·40·69 | 100·29·56 | 100·19·15 | 100·16·0 | 100·36·67 | 100·26·58 | 100·10·17 | 100·7·0 |
| D promo+w365 | 100·37·67 | 100·25·52 | 100·16·15 | 100·13·1 | 100·33·65 | 100·22·55 | 100·10·17 | 100·6·0 |
| D promo+sen | 100·83·34 | 100·69·33 | 100·31·15 | 100·30·0 | 100·1·34 | 100·1·34 | 100·4·17 | 100·0·0 |
| D promo5+w90 | 100·39·71 | 100·29·60 | 100·22·14 | 100·19·0 | 100·36·67 | 100·27·67 | 100·11·15 | 100·8·1 |
| D nopre | 100·95·99 | 100·88·97 | 100·84·91 | 100·61·88 | 100·99·99 | 100·99·99 | 100·99·99 | 100·99·99 |
| D promo+nopre | 100·95·99 | 100·64·84 | 100·87·15 | 100·86·0 | 100·99·99 | 100·99·99 | 100·81·17 | 100·82·0 |
| D ticket24 | 100·72·88 | 100·41·57 | 100·25·35 | 100·17·23 | 100·68·88 | 100·33·59 | 100·15·41 | 100·5·29 |
| D ncap | 100·72·82 | 100·39·52 | 100·22·31 | 100·13·15 | 100·69·82 | 100·33·53 | 100·14·36 | 100·5·24 |
| D nrate100 | 100·72·85 | 100·39·54 | 100·22·32 | 100·13·16 | 100·69·85 | 100·33·55 | 100·15·39 | 100·5·27 |

Time until established honest presence < 90% / < 50% (continuous attack)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 0h / never | 0h / 2.0d | 0h / 2.0d | 0h / 2.0d | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D w30 | 7.1d / never | 2.0d / 9.0d | 2.0d / 2.0d | 2.0d / 2.0d | 2.0d / never | 0h / 3.0d | 0h / 0h | 0h / 0h |
| D w90 | 18.1d / never | 3.0d / never | 2.0d / 3.0d | 2.0d / 2.0d | 17.5d / never | 0h / 18.1d | 0h / 0h | 0h / 0h |
| D w365 | never / never | 5.0d / never | 2.0d / 7.0d | 2.0d / 2.0d | 19.5d / never | 0h / never | 0h / 0h | 0h / 0h |
| D sen | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| D rand-rec | 0h / 2.0d | 0h / 2.0d | never / never | never / never | 0h / 0h | 0h / 0h | never / never | never / never |
| D days2 | 0h / never | 0h / 24h | 0h / 24h | 0h / 24h | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D days5 | 28h / never | 28h / 4.0d | 28h / 4.0d | 2.2d / 4.0d | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D days7 | 3.2d / never | 3.2d / 6.0d | 3.2d / 6.0d | 4.2d / 6.0d | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D age1 | 0h / never | 0h / 24h | 0h / 24h | 0h / 24h | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D noEarned | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D promo | 0h / never | 0h / 2.2d | 0h / 2.6d | 0h / 5.0d | 0h / never | 0h / 0h | 0h / 2h | 0h / 22h |
| D promo+w90 | 18.1d / never | 3.0d / never | 3.0d / 10.2d | 3.3d / 10.6d | 17.5d / never | 0h / 18.1d | 0h / 3.0d | 4h / 3.0d |
| D promo+w365 | never / never | 4.2d / never | 4.3d / 16.0d | 4.5d / 16.3d | 19.5d / never | 0h / never | 0h / 8.4d | 4h / 8.6d |
| D promo+sen | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| D promo5+w90 | 18.1d / never | 4.8d / never | 4.8d / 15.0d | 5.0d / 15.1d | 17.5d / never | 0h / 18.1d | 1h / 7.0d | 10h / 7.6d |
| D nopre | 0h / never | 0h / never | 0h / never | 0h / never | 18.1d / never | 18.1d / never | 18.1d / never | 18.1d / never |
| D promo+nopre | 0h / never | 0h / never | 0h / never | 0h / never | 18.1d / never | 18.1d / never | 0h / never | 0h / never |
| D ticket24 | 0h / never | 4h / 3.0d | 15h / 3.0d | 24h / 3.0d | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D ncap | 2.0d / never | 0h / 2.0d | 0h / 2.0d | 0h / 2.0d | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D nrate100 | 2.0d / never | 0h / 2.0d | 0h / 2.0d | 0h / 2.0d | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |

1-day attack: min established % · min all honest % · recovery to baseline−1pt after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 94·94·5.5d | 94·94·5.5d | 94·93·5.5d | 94·93·5.5d | 69·70·8.7d | 31·33·9.9d | 15·17·9.9d | 6·8·9.9d |
| D w30 | 99·94·4.7d | 99·94·4.7d | 99·93·4.7d | 99·93·4.7d | 90·87·8.7d | 53·53·8.7d | 25·26·9.9d | 10·12·9.9d |
| D w90 | 99·94·4.7d | 99·94·4.7d | 99·93·4.7d | 99·93·4.9d | 97·92·7.7d | 72·70·8.7d | 33·33·9.9d | 16·18·9.9d |
| D w365 | 99·94·4.9d | 99·94·4.9d | 99·93·4.9d | 99·93·4.9d | 97·93·7.7d | 85·81·8.7d | 47·46·8.7d | 23·24·9.9d |
| D sen | 100·94·4.7d | 100·94·4.7d | 100·93·4.7d | 100·93·4.7d | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d |
| D rand-rec | 94·95·5.5d | 94·94·5.5d | –·–·never | 94·93·5.5d | 1·4·8.7d | 1·4·8.7d | –·–·never | –·–·never |
| D days2 | 93·93·5.5d | 93·93·5.5d | 93·93·5.5d | 93·93·5.5d | 70·71·8.7d | 32·33·9.9d | 15·16·9.9d | 6·7·9.9d |
| D days5 | 99·95·5.5d | 99·94·5.5d | 99·93·5.5d | 99·93·5.5d | 69·72·8.7d | 31·37·9.9d | 15·22·9.9d | 6·13·9.9d |
| D days7 | 100·96·4.7d | 100·94·5.5d | 100·93·5.5d | 100·93·5.5d | 69·72·8.7d | 31·37·9.9d | 15·22·9.9d | 6·13·9.9d |
| D age1 | 93·93·5.5d | 93·93·5.5d | 93·93·5.5d | 93·93·5.5d | 70·71·8.7d | 32·33·9.9d | 15·16·9.9d | 6·7·9.9d |
| D noEarned | 70·71·5.5d | 32·33·6.9d | 15·16·6.9d | 6·7·6.9d | 70·71·5.5d | 32·33·6.9d | 15·16·6.9d | 6·7·6.9d |
| D promo | 98·94·5.5d | 99·94·5.5d | 99·93·5.5d | 99·93·5.5d | 69·70·8.7d | 31·33·9.9d | 26·26·9.9d | 51·48·8.7d |
| D promo+w90 | 99·94·4.7d | 99·94·4.7d | 99·93·4.7d | 99·93·4.9d | 97·92·7.7d | 72·70·8.7d | 59·56·8.7d | 60·57·8.7d |
| D promo+w365 | 99·94·4.9d | 99·94·4.9d | 99·93·4.9d | 99·93·4.9d | 97·93·7.7d | 85·81·8.7d | 75·71·8.7d | 76·71·8.7d |
| D promo+sen | 100·94·4.7d | 100·94·4.7d | 100·93·4.7d | 100·93·4.7d | 100·94·7.7d | 100·94·7.7d | 100·93·4.7d | 100·93·4.8d |
| D promo5+w90 | 99·94·4.7d | 99·94·4.7d | 99·93·4.7d | 99·93·4.9d | 97·92·7.7d | 72·70·8.7d | 72·68·8.7d | 77·72·8.7d |
| D nopre | 98·94·4.7d | 99·94·4.7d | 99·93·4.7d | 99·93·4.7d | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| D promo+nopre | 98·94·4.7d | 99·94·4.7d | 99·93·4.7d | 99·93·4.7d | 100·100·0h | 100·100·0h | 98·92·7.7d | 99·93·4.7d |
| D ticket24 | 94·94·5.5d | 94·94·5.5d | 94·94·5.5d | 94·93·5.5d | 69·70·8.7d | 31·33·9.9d | 15·17·9.9d | 6·8·9.9d |
| D ncap | 100·94·0h | 94·94·0h | 94·93·8h | 99·93·7h | 70·69·8.5d | 32·32·8.7d | 15·16·8.7d | 6·7·8.7d |
| D nrate100 | 100·95·0h | 94·94·3h | 94·93·8h | 99·93·4.7d | 70·69·8.5d | 32·33·8.7d | 15·17·8.7d | 6·7·8.7d |

7-day attack: mean established % · mean all honest % · recovery after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 76·77·8.9d | 49·51·8.9d | 37·39·8.9d | 31·32·8.9d | 69·71·8.9d | 31·34·8.9d | 15·17·8.9d | 5·8·8.9d |
| D w30 | 96·93·8.9d | 76·73·8.9d | 49·49·8.9d | 38·38·8.9d | 88·85·8.9d | 48·48·8.9d | 23·24·8.9d | 9·12·8.9d |
| D w90 | 100·95·7.7d | 88·84·8.9d | 59·57·8.9d | 43·42·8.9d | 98·93·7.7d | 68·65·8.9d | 30·31·8.9d | 14·16·8.9d |
| D w365 | 100·95·7.7d | 94·89·8.9d | 72·68·8.9d | 48·46·8.9d | 98·93·7.7d | 84·79·8.9d | 43·42·8.9d | 20·21·8.9d |
| D sen | 100·97·7.5d | 100·97·7.5d | 100·97·7.5d | 100·97·7.5d | 100·92·7.7d | 100·92·7.7d | 100·92·7.7d | 100·92·7.7d |
| D rand-rec | 31·33·8.9d | 31·33·8.9d | –·–·never | –·–·never | 5·8·8.9d | 5·8·8.9d | –·–·never | –·–·never |
| D days2 | 72·74·8.9d | 40·42·8.9d | 26·28·8.9d | 18·20·8.9d | 69·71·8.9d | 31·33·8.9d | 15·17·8.9d | 5·7·8.9d |
| D days5 | 84·84·8.9d | 68·68·8.9d | 61·61·8.9d | 57·57·8.9d | 69·71·8.9d | 31·35·8.9d | 14·19·8.9d | 5·10·8.9d |
| D days7 | 93·91·8.9d | 88·85·8.9d | 86·82·8.9d | 85·80·8.9d | 69·71·8.9d | 31·36·8.9d | 14·21·8.9d | 5·12·8.9d |
| D age1 | 72·74·8.9d | 40·42·8.9d | 26·28·8.9d | 18·20·8.9d | 69·71·8.9d | 31·33·8.9d | 15·17·8.9d | 5·7·8.9d |
| D noEarned | 70·71·5.7d | 31·33·5.7d | 15·16·5.7d | 5·7·5.7d | 70·71·5.7d | 31·33·5.7d | 15·16·5.7d | 5·7·5.7d |
| D promo | 76·77·8.9d | 53·54·8.9d | 55·55·8.9d | 70·69·8.9d | 69·71·8.9d | 31·34·8.9d | 25·25·8.9d | 31·31·8.9d |
| D promo+w90 | 100·95·7.7d | 88·83·8.9d | 87·81·8.9d | 88·82·8.9d | 98·93·7.7d | 68·65·8.9d | 49·46·8.9d | 51·48·8.9d |
| D promo+w365 | 100·95·7.7d | 93·87·8.9d | 92·86·8.9d | 93·86·8.9d | 98·93·7.7d | 84·79·8.9d | 65·61·8.9d | 66·61·8.9d |
| D promo+sen | 100·97·7.5d | 100·94·0h | 100·92·4.7d | 100·92·4.9d | 100·92·7.7d | 100·92·7.7d | 100·91·7.0d | 100·91·4.9d |
| D promo5+w90 | 99·95·7.7d | 92·87·8.9d | 92·86·8.9d | 93·86·8.9d | 98·93·7.7d | 66·65·8.9d | 61·58·8.9d | 64·60·8.9d |
| D nopre | 92·93·8.9d | 92·92·8.9d | 92·92·8.9d | 79·79·8.9d | 98·98·7.7d | 98·98·7.7d | 98·98·7.7d | 98·98·7.7d |
| D promo+nopre | 92·93·8.9d | 83·82·8.9d | 94·91·8.9d | 94·92·5.7d | 98·98·7.7d | 98·98·7.7d | 92·90·8.9d | 93·91·5.7d |
| D ticket24 | 77·78·8.9d | 54·56·8.9d | 43·44·8.9d | 38·40·8.9d | 69·70·8.9d | 31·33·8.9d | 15·17·8.9d | 5·8·8.9d |
| D ncap | 78·78·8.9d | 49·50·8.9d | 37·38·8.9d | 31·32·8.9d | 70·70·8.9d | 31·33·8.9d | 15·16·8.9d | 5·7·8.9d |
| D nrate100 | 78·78·8.9d | 49·50·8.9d | 37·38·8.9d | 31·32·8.9d | 70·70·8.9d | 31·33·8.9d | 15·16·8.9d | 5·7·8.9d |

Store writes/s during the continuous attack: all (inserts+evictions+expiries) / honest-only; attacker refreshes not counted

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre N=10 | pre N=100 | pre N=1000 | pre N=10000 |
|---|---|---|---|---|---|---|---|---|
| C S2 | 0.1/0.04 | 0.2/0.05 | 0.5/0.06 | 1.9/0.07 | 0.1/0.03 | 0.2/0.05 | 0.5/0.06 | 2.0/0.07 |
| D w30 | 0.1/0.04 | 0.2/0.05 | 0.5/0.06 | 2.7/0.07 | 0.1/0.04 | 0.2/0.05 | 0.2/0.06 | 0.2/0.06 |
| D w90 | 0.1/0.04 | 0.2/0.05 | 0.5/0.06 | 3.6/0.06 | 0.1/0.03 | 0.2/0.05 | 0.2/0.06 | 0.2/0.06 |
| D w365 | 0.1/0.03 | 0.2/0.04 | 0.6/0.05 | 2.6/0.06 | 0.1/0.03 | 0.1/0.04 | 0.2/0.05 | 0.2/0.06 |
| D sen | 0.1/0.04 | 0.1/0.04 | 0.1/0.04 | 0.2/0.04 | 0.1/0.02 | 0.1/0.02 | 0.2/0.02 | 0.2/0.02 |
| D rand-rec | 43.8/1.46 | 43.8/1.46 | 0.0/0.00 | 0.0/0.00 | 48.4/1.59 | 48.4/1.59 | 0.0/0.00 | 0.0/0.00 |
| D days2 | 0.1/0.04 | 0.2/0.05 | 0.5/0.06 | 1.7/0.07 | 0.1/0.03 | 0.2/0.05 | 0.5/0.06 | 1.6/0.07 |
| D days5 | 0.1/0.04 | 0.2/0.05 | 0.5/0.06 | 2.2/0.07 | 0.1/0.03 | 0.2/0.05 | 0.4/0.06 | 2.5/0.07 |
| D days7 | 0.1/0.04 | 0.2/0.06 | 0.5/0.07 | 2.2/0.07 | 0.1/0.03 | 0.2/0.05 | 0.5/0.06 | 3.1/0.07 |
| D age1 | 0.1/0.04 | 0.2/0.05 | 0.5/0.06 | 1.7/0.07 | 0.1/0.03 | 0.2/0.05 | 0.5/0.06 | 1.6/0.07 |
| D noEarned | 0.1/0.03 | 0.1/0.04 | 0.4/0.05 | 1.0/0.05 | 0.1/0.03 | 0.1/0.04 | 0.4/0.05 | 1.0/0.05 |
| D promo | 0.1/0.04 | 0.3/0.05 | 0.6/0.04 | 1.1/0.04 | 0.1/0.03 | 0.2/0.05 | 0.6/0.04 | 1.0/0.04 |
| D promo+w90 | 0.1/0.04 | 0.3/0.05 | 0.5/0.04 | 1.0/0.04 | 0.1/0.03 | 0.2/0.05 | 0.4/0.04 | 0.9/0.04 |
| D promo+w365 | 0.1/0.03 | 0.3/0.04 | 0.4/0.03 | 1.0/0.03 | 0.1/0.03 | 0.1/0.04 | 0.4/0.04 | 0.8/0.04 |
| D promo+sen | 0.1/0.04 | 0.2/0.03 | 0.3/0.01 | 0.9/0.02 | 0.1/0.02 | 0.1/0.02 | 0.3/0.01 | 0.7/0.01 |
| D promo5+w90 | 0.2/0.04 | 0.3/0.04 | 0.5/0.03 | 1.0/0.04 | 0.1/0.03 | 0.3/0.05 | 0.4/0.04 | 1.0/0.04 |
| D nopre | 0.1/0.02 | 0.1/0.03 | 0.1/0.03 | 0.2/0.03 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 |
| D promo+nopre | 0.1/0.02 | 0.2/0.04 | 0.3/0.01 | 0.9/0.01 | 0.0/0.02 | 0.0/0.02 | 0.3/0.02 | 0.8/0.01 |
| D ticket24 | 0.1/0.03 | 0.2/0.05 | 0.5/0.06 | 1.7/0.06 | 0.1/0.03 | 0.2/0.05 | 0.4/0.06 | 1.8/0.06 |
| D ncap | 0.1/0.03 | 0.1/0.04 | 0.5/0.05 | 1.3/0.06 | 0.1/0.03 | 0.1/0.04 | 0.5/0.05 | 1.3/0.05 |
| D nrate100 | 0.1/0.03 | 0.1/0.04 | 0.5/0.05 | 1.3/0.06 | 0.1/0.03 | 0.1/0.04 | 0.4/0.05 | 1.3/0.06 |

**E3 reading** (continuous attack; cells read as established % / joined-during-attack %):
- **The within-Earned rule decides almost everything.** Max-min (fair share by publisher) gives every identity an
  equal share, so N identities in Earned dilute honest large publishers in proportion to N. Age weighting helps
  only when the store is mostly empty (2% load: w365 94/87% est. at N=10000 vs 33/26%), not at 50–90% load.
- **"By records" (rand-rec = evict a random record of the tier) is as bad as IPFS** (5–14%): a max-rate
  attacker owns most records, so it also owns most evictions' winnings.
- **Seniority (newest publisher evicted first, today's rule) keeps established at 100%** but publishers younger
  than the attacker (joined in the week before a 7-day pre-aged attack) drop to 1%.
- **No pre-emption inside Earned ("nopre": a stored, refreshed Earned record is never evicted by another Earned
  publisher; Earned reclaims only from Newcomer above its floor) keeps established at 84–100% and newcomers at
  88–100%**, independent of N and of pre-ageing: incumbents keep what they hold; promoted attackers can only
  take free space. Its residual loss at 90% load (84%, 69% at fresh N=10000) is a ratchet: fresh identities
  reclaim the Newcomer floor from Earned (S2), then get promoted carrying those records into Earned, freeing the
  floor to be reclaimed again. Fixed below by reclaiming newest-promoted first (E6).
- **Earned criterion.** More distinct days (5, 7) only delays the fresh-flood promotion (2→4→6 days); pre-aged
  identities are unaffected. "First seen ≥ 1 day" (today's seniority test) = 2 days of distinct refreshes.
- **Promotion rate limit** (≤ max(10, 2% of Earned)/day) protects Earned under max-min (est. 98–100% at 2%,
  48–88% at 50%) but keeps fresh identities in the Newcomer tier, where they crowd out honest newcomers
  (joined-during-attack 0–23% at N ≥ 1000). Moving attackers out of Newcomer into a no-pre-emption Earned is
  better for newcomers than holding them back.
- **Admission rules don't help against free identities**: ticket24 (wait 24 h before first admission when full),
  a newcomer cap (C/1000) and a newcomer insert rate (100/day) leave established numbers unchanged and only cost
  honest newcomers (E0: 82–87% newly-joined presence in peacetime for ncap/nrate100; mirror join takes days).

## E2. Tier floor splits (Known/Earned/Newcomer) × newcomer expiry

Each cell: established mean % during a continuous attack · joined-during-attack mean % · all-honest recovery after a 1-day attack. Max-min fair share, Earned = 3 distinct days.

### Load 50%

| policy | fresh N=100 | fresh N=10000 | pre N=100 | pre N=10000 |
|---|---|---|---|---|
| C 30/60/10 3.5d S1 | 48·61·0h | 16·13·12h | 43·58·8.7d | 7·21·9.9d |
| C 30/60/10 3.5d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 30/60/10 4d S1 | 48·60·0h | 16·13·12h | 43·58·8.7d | 7·21·9.9d |
| C 30/60/10 4d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 30/60/10 5d S1 | 48·61·0h | 16·13·12h | 43·59·8.7d | 7·21·9.9d |
| C 30/60/10 5d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 30/60/10 7d S1 | 48·60·0h | 16·13·12h | 43·59·8.7d | 7·21·9.9d |
| C 30/60/10 7d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 20/60/20 3.5d S1 | 48·61·0h | 16·13·12h | 43·58·8.7d | 7·21·9.9d |
| C 20/60/20 3.5d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 20/60/20 4d S1 | 48·60·0h | 16·13·12h | 43·58·8.7d | 7·21·9.9d |
| C 20/60/20 4d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 20/60/20 5d S1 | 48·61·0h | 16·13·12h | 43·59·8.7d | 7·21·9.9d |
| C 20/60/20 5d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 20/60/20 7d S1 | 48·60·0h | 16·13·12h | 43·59·8.7d | 7·21·9.9d |
| C 20/60/20 7d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 10/70/20 3.5d S1 | 48·61·0h | 16·13·12h | 43·58·8.7d | 7·21·9.9d |
| C 10/70/20 3.5d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 10/70/20 4d S1 | 48·60·0h | 16·13·12h | 43·58·8.7d | 7·21·9.9d |
| C 10/70/20 4d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 10/70/20 5d S1 | 48·61·0h | 16·13·12h | 43·59·8.7d | 7·21·9.9d |
| C 10/70/20 5d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 10/70/20 7d S1 | 48·60·0h | 16·13·12h | 43·59·8.7d | 7·21·9.9d |
| C 10/70/20 7d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 30/50/20 3.5d S1 | 48·61·0h | 16·13·12h | 43·58·8.7d | 7·21·9.9d |
| C 30/50/20 3.5d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 30/50/20 4d S1 | 48·60·0h | 16·13·12h | 43·58·8.7d | 7·21·9.9d |
| C 30/50/20 4d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 30/50/20 5d S1 | 48·61·0h | 16·13·12h | 43·59·8.7d | 7·21·9.9d |
| C 30/50/20 5d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |
| C 30/50/20 7d S1 | 48·60·0h | 16·13·12h | 43·59·8.7d | 7·21·9.9d |
| C 30/50/20 7d S2 | 48·80·0h | 16·36·12h | 43·80·8.7d | 7·43·9.9d |

### Load 90%

| policy | fresh N=100 | fresh N=10000 | pre N=100 | pre N=10000 |
|---|---|---|---|---|
| C 30/60/10 3.5d S1 | 37·41·4.4d | 19·2·5.5d | 31·48·9.9d | 5·18·9.9d |
| C 30/60/10 3.5d S2 | 36·67·5.5d | 13·30·5.5d | 30·69·9.9d | 5·41·9.9d |
| C 30/60/10 4d S1 | 37·41·4.7d | 19·2·5.5d | 31·48·9.9d | 5·18·9.9d |
| C 30/60/10 4d S2 | 36·67·5.5d | 13·29·5.5d | 30·69·9.9d | 5·41·9.9d |
| C 30/60/10 5d S1 | 37·41·5.7d | 19·2·5.7d | 31·48·9.9d | 5·18·9.9d |
| C 30/60/10 5d S2 | 36·67·5.7d | 13·30·5.7d | 30·69·9.9d | 5·41·9.9d |
| C 30/60/10 7d S1 | 37·41·8.7d | 19·2·8.7d | 31·48·9.9d | 5·18·9.9d |
| C 30/60/10 7d S2 | 36·67·8.7d | 13·30·8.7d | 30·69·9.9d | 5·41·9.9d |
| C 20/60/20 3.5d S1 | 37·41·4.4d | 19·2·5.5d | 31·48·9.9d | 5·18·9.9d |
| C 20/60/20 3.5d S2 | 35·70·5.5d | 12·31·5.5d | 30·70·9.9d | 5·41·9.9d |
| C 20/60/20 4d S1 | 37·41·4.7d | 19·2·5.5d | 31·48·9.9d | 5·18·9.9d |
| C 20/60/20 4d S2 | 35·70·5.5d | 12·31·5.5d | 30·70·9.9d | 5·41·9.9d |
| C 20/60/20 5d S1 | 37·41·5.7d | 19·2·5.7d | 31·48·9.9d | 5·18·9.9d |
| C 20/60/20 5d S2 | 35·70·5.9d | 12·31·5.9d | 30·70·9.9d | 5·41·9.9d |
| C 20/60/20 7d S1 | 37·41·8.7d | 19·2·8.7d | 31·48·9.9d | 5·18·9.9d |
| C 20/60/20 7d S2 | 35·70·8.7d | 12·31·8.7d | 30·70·9.9d | 5·41·9.9d |
| C 10/70/20 3.5d S1 | 37·41·4.4d | 19·2·5.5d | 31·48·9.9d | 5·18·9.9d |
| C 10/70/20 3.5d S2 | 35·70·5.5d | 12·31·5.5d | 30·70·9.9d | 5·41·9.9d |
| C 10/70/20 4d S1 | 37·41·4.7d | 19·2·5.5d | 31·48·9.9d | 5·18·9.9d |
| C 10/70/20 4d S2 | 35·70·5.5d | 12·31·5.5d | 30·70·9.9d | 5·41·9.9d |
| C 10/70/20 5d S1 | 37·41·5.7d | 19·2·5.7d | 31·48·9.9d | 5·18·9.9d |
| C 10/70/20 5d S2 | 35·70·5.9d | 12·31·5.9d | 30·70·9.9d | 5·41·9.9d |
| C 10/70/20 7d S1 | 37·41·8.7d | 19·2·8.7d | 31·48·9.9d | 5·18·9.9d |
| C 10/70/20 7d S2 | 35·70·8.7d | 12·31·8.7d | 30·70·9.9d | 5·41·9.9d |
| C 30/50/20 3.5d S1 | 37·41·4.4d | 19·2·5.5d | 31·48·9.9d | 5·18·9.9d |
| C 30/50/20 3.5d S2 | 35·70·5.5d | 12·31·5.5d | 30·70·9.9d | 5·41·9.9d |
| C 30/50/20 4d S1 | 37·41·4.7d | 19·2·5.5d | 31·48·9.9d | 5·18·9.9d |
| C 30/50/20 4d S2 | 35·70·5.5d | 12·31·5.5d | 30·70·9.9d | 5·41·9.9d |
| C 30/50/20 5d S1 | 37·41·5.7d | 19·2·5.7d | 31·48·9.9d | 5·18·9.9d |
| C 30/50/20 5d S2 | 35·70·5.9d | 12·31·5.9d | 30·70·9.9d | 5·41·9.9d |
| C 30/50/20 7d S1 | 37·41·8.7d | 19·2·8.7d | 31·48·9.9d | 5·18·9.9d |
| C 30/50/20 7d S2 | 35·70·8.7d | 12·31·8.7d | 30·70·9.9d | 5·41·9.9d |

**E2 reading.** Under max-min fair share the floor split barely matters (±1–4 points): the damage comes from
promoted identities taking fair shares *inside* Earned, which no floor addresses; a larger Newcomer floor (20%)
helps newcomers by ~3 points at 90% load and costs established ~1 point. S2 vs S1 is the real switch (newcomers
during an attack 13–48% → 29–80%). Newcomer expiry only shows in recovery from a short fresh flood on a nearly
full store (90%: 3.5 d → 4.4–5.5 d, 4 d → 4.7–5.5 d, 5 d → 5.7 d, 7 d → 8.7 d): fresh-attacker records that
nobody needs to evict sit until they expire. Pre-aged (Earned, 7-day TTL) floods take ~9–10 days whatever the
newcomer expiry. Floors and expiry are re-tested on the recommended design in E7.

## E6. Candidate designs. N = three tiers (30/60/10, S2, 4 d) with no pre-emption inside Earned ("nopre": Earned publishers never evict each other) or seniority ("sen"), and a Newcomer tier below its floor reclaiming from the most recently promoted Earned publisher first ("newest"). R = four tiers Known / Senior / Earned / Newcomer (floors in that order). Senior = in Earned for ≥ s days (s14/s30/s60); promo = Earned promotions limited to max(10, 2% of Earned)/day (promo5: max(5, 1%)), FIFO; S2 floors; max-min fair share inside every tier; Earned = held on 3 distinct days; Newcomer expiry 4 d (4.5 d where named). Compared with A, C S2 and the best 3-tier variant

### Load 2% (honest records / capacity)

Established honest publishers, mean presence % during a **continuous** attack (21 days); pre-attack baseline in brackets

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 98 [100] | 97 [100] | 100 [100] | 99 [100] | 75 [100] | 70 [100] |
| C S2 | 100 [100] | 100 [100] | 84 [100] | 33 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] |
| D sen | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 97 [100] | 97 [100] | 97 [100] | 97 [100] | 66 [100] | 66 [100] | 66 [100] | 66 [100] |
| D promo+w365 | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 99 [100] | 100 [100] | 100 [100] | 85 [100] | 37 [100] |
| D nopre | 100 [100] | 100 [100] | 95 [100] | 94 [100] | 100 [100] | 100 [100] | 94 [100] | 94 [100] | 100 [100] | 100 [100] | 94 [100] | 94 [100] | 100 [100] | 100 [100] | 94 [100] | 94 [100] |
| N nopre+newest | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| N nopre+newest promo | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| N sen+newest | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 97 [100] | 97 [100] | 97 [100] | 97 [100] | 66 [100] | 66 [100] | 66 [100] | 66 [100] |
| N nopre+newest S1 | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| N nopre+newest days5 | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 99 [100] | 99 [100] | 99 [100] | 99 [100] |
| N nopre+newest 20/60/20 | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| N nopre+newest 4.5d | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| R s30 S-nopre+newest | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 97 [100] | 97 [100] | 97 [100] | 97 [100] | 94 [100] | 94 [100] | 94 [100] | 94 [100] |
| R 20/50/20/10 s30 | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 84 [100] | 33 [100] | 100 [100] | 100 [100] | 83 [100] | 26 [100] |
| R 20/50/20/10 s30 promo | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 83 [100] | 37 [100] |

Established, last day of the continuous attack % (small publishers ≤20 records / large)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (98/97 min) | 97 (98/97 min) | 97 (98/97 min) | 100 (100/100 min) | 100 (53/67 min) | 74 (53/67 min) | 68 (53/67 min) |
| C S2 | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) |
| D sen | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 64 (51/64 min) | 64 (51/64 min) | 64 (51/64 min) | 64 (51/64 min) |
| D promo+w365 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 98 (100/98 min) | 100 (100/100 min) | 100 (100/100 min) | 85 (100/84 min) | 33 (100/29 min) |
| D nopre | 100 (100/100 min) | 100 (100/100 min) | 85 (100/84 min) | 81 (100/80 min) | 100 (100/100 min) | 100 (100/100 min) | 85 (100/84 min) | 80 (100/79 min) | 100 (100/100 min) | 100 (100/100 min) | 85 (100/84 min) | 80 (100/79 min) | 100 (100/100 min) | 100 (100/100 min) | 85 (100/84 min) | 80 (100/79 min) |
| N nopre+newest | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) |
| N nopre+newest promo | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (99/97 min) | 100 (100/100 min) | 100 (100/100 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) | 100 (100/100 min) |
| N sen+newest | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 64 (50/64 min) | 64 (50/64 min) | 64 (50/64 min) | 64 (50/64 min) |
| N nopre+newest S1 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) |
| N nopre+newest days5 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) |
| N nopre+newest 20/60/20 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) |
| N nopre+newest 4.5d | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) |
| R s30 S-nopre+newest | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 80 (62/81 min) | 80 (62/81 min) | 80 (62/81 min) | 80 (62/81 min) |
| R 20/50/20/10 s30 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 26 (100/22 min) |
| R 20/50/20/10 s30 promo | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/99 min) | 98 (100/98 min) | 100 (100/100 min) | 100 (100/100 min) | 82 (100/81 min) | 36 (100/32 min) |

Known (contacts/bootstrap) min % · publishers that joined before the attack (<7 d old) mean % · joined during the attack mean %  (continuous)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 100·100·99 | 100·100·96 | 100·100·52 | 100·100·24 | 100·100·99 | 100·98·97 | 100·29·53 | 100·9·25 | 100·100·99 | 100·98·98 | 100·29·54 | 100·9·25 | 100·100·99 | 74·98·99 | 74·29·55 | 74·10·26 |
| C S2 | 100·100·100 | 100·100·100 | 100·93·99 | 100·37·67 | 100·100·100 | 100·100·100 | 100·92·99 | 100·29·67 | 100·100·100 | 100·100·100 | 100·92·99 | 100·29·67 | 100·100·100 | 100·100·100 | 100·92·99 | 100·29·67 |
| D sen | 100·100·34 | 100·100·34 | 100·100·34 | 100·100·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 |
| D promo+w365 | 100·100·100 | 100·100·99 | 100·100·57 | 100·96·36 | 100·100·100 | 100·100·99 | 100·100·62 | 100·89·30 | 100·100·100 | 100·100·99 | 100·45·85 | 100·33·28 | 100·100·99 | 100·68·85 | 100·25·58 | 100·14·34 |
| D nopre | 100·100·100 | 100·100·100 | 100·100·100 | 100·99·100 | 100·100·100 | 100·100·100 | 100·100·100 | 100·99·100 | 100·100·100 | 100·100·100 | 100·100·100 | 100·99·100 | 100·100·100 | 100·100·100 | 100·100·100 | 100·99·100 |
| N nopre+newest | 100·94·81 | 100·94·81 | 100·94·81 | 100·94·81 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 |
| N nopre+newest promo | 100·94·81 | 100·99·91 | 100·100·57 | 100·96·36 | 100·59·81 | 100·60·81 | 100·100·62 | 100·89·30 | 100·59·81 | 100·59·81 | 100·97·90 | 100·89·28 | 100·59·81 | 100·59·81 | 100·59·81 | 100·91·34 |
| N sen+newest | 100·97·34 | 100·97·34 | 100·97·34 | 100·97·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 |
| N nopre+newest S1 | 100·100·29 | 100·100·29 | 100·100·29 | 100·100·29 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 |
| N nopre+newest days5 | 100·97·90 | 100·97·90 | 100·97·90 | 100·95·87 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 |
| N nopre+newest 20/60/20 | 100·94·81 | 100·94·81 | 100·94·81 | 100·94·81 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 |
| N nopre+newest 4.5d | 100·94·81 | 100·94·81 | 100·94·81 | 100·94·81 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 |
| R s30 S-nopre+newest | 100·99·96 | 100·99·96 | 100·92·95 | 100·35·63 | 100·97·96 | 100·97·96 | 100·89·95 | 100·26·63 | 100·100·100 | 100·100·100 | 100·99·100 | 100·92·100 | 100·100·100 | 100·100·100 | 100·100·100 | 100·100·100 |
| R 20/50/20/10 s30 | 100·100·100 | 100·100·100 | 100·93·99 | 100·37·66 | 100·100·100 | 100·100·100 | 100·92·99 | 100·29·66 | 100·100·100 | 100·100·100 | 100·99·100 | 100·92·100 | 100·100·100 | 100·100·100 | 100·100·100 | 100·100·100 |
| R 20/50/20/10 s30 promo | 100·100·100 | 100·100·100 | 100·100·57 | 100·96·36 | 100·100·100 | 100·100·100 | 100·100·62 | 100·89·30 | 100·100·100 | 100·100·100 | 100·51·85 | 100·45·28 | 100·100·100 | 100·100·100 | 100·100·100 | 100·25·34 |

Time until established honest presence < 90% / < 50% (continuous attack)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | 0h / never | 0h / never | 0h / never |
| C S2 | never / never | never / never | 2.0d / never | 2.0d / 2.0d | never / never | never / never | 0h / never | 0h / 0h | never / never | never / never | 0h / never | 0h / 0h | never / never | never / never | 0h / never | 0h / 0h |
| D sen | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | 0h / never | 0h / never | 0h / never | 0h / never |
| D promo+w365 | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | 0h / never | 2h / 2h |
| D nopre | never / never | never / never | 18.1d / never | 18.1d / never | never / never | never / never | 18.1d / never | 18.1d / never | never / never | never / never | 18.1d / never | 18.1d / never | never / never | never / never | 18.1d / never | 18.1d / never |
| N nopre+newest | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N nopre+newest promo | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N sen+newest | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | 0h / never | 0h / never | 0h / never | 0h / never |
| N nopre+newest S1 | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N nopre+newest days5 | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N nopre+newest 20/60/20 | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N nopre+newest 4.5d | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| R s30 S-nopre+newest | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | 18.1d / never | 18.1d / never | 18.1d / never | 18.1d / never |
| R 20/50/20/10 s30 | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | 2.0d / never | 2.0d / 2.0d | never / never | never / never | 0h / never | 0h / 0h |
| R 20/50/20/10 s30 promo | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | 0h / never | 2h / 2h |

1-day attack: min established % · min all honest % · recovery to baseline−1pt after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·93·0h | 100·93·7.7d | 100·93·7.7d | 100·100·0h | 97·90·0h | 97·90·7.7d | 97·90·7.7d | 100·100·0h | 66·61·29h | 66·61·8.7d | 66·61·8.7d |
| C S2 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 83·84·8.7d | 28·29·9.9d | 100·100·0h | 100·100·0h | 83·84·8.7d | 28·29·9.9d | 100·100·0h | 100·100·0h | 83·84·8.7d | 28·29·9.9d |
| D sen | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 97·91·7.7d | 97·91·7.7d | 97·91·7.7d | 97·91·7.7d | 66·63·9.2d | 66·63·9.2d | 66·63·9.2d | 66·63·9.2d |
| D promo+w365 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·7h | 100·100·0h | 100·100·0h | 100·96·7h | 100·95·4.7d | 100·100·0h | 100·98·7h | 100·96·7h | 100·94·3.4d | 100·100·0h | 100·95·7.7d | 86·82·8.7d | 43·42·5.5d |
| D nopre | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest promo | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·7h | 100·100·0h | 100·100·0h | 100·96·7h | 100·95·4.7d | 100·100·0h | 100·100·0h | 100·96·7h | 100·94·3.4d | 100·100·0h | 100·100·0h | 100·100·0h | 100·94·31h |
| N sen+newest | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 97·91·7.7d | 97·91·7.7d | 97·91·7.7d | 97·91·7.7d | 66·63·9.2d | 66·63·9.2d | 66·63·9.2d | 66·63·9.2d |
| N nopre+newest S1 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest days5 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·31h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest 20/60/20 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest 4.5d | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| R s30 S-nopre+newest | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·7.7d | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·7.5d | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| R 20/50/20/10 s30 | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·7.5d | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·8.7d | 100·100·0h | 100·100·0h | 83·84·8.7d | 28·34·9.9d |
| R 20/50/20/10 s30 promo | 100·100·0h | 100·100·0h | 100·100·0h | 100·95·7h | 100·100·0h | 100·100·0h | 100·96·7h | 100·95·4.7d | 100·100·0h | 100·100·0h | 100·96·7h | 100·94·3.4d | 100·100·0h | 100·100·0h | 83·84·8.7d | 39·39·6.9d |

Store writes/s during the continuous attack: all (inserts+evictions+expiries) / honest-only; attacker refreshes not counted

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 0.2/0.01 | 2.3/0.01 | 3.8/0.01 | 4.6/0.01 | 0.2/0.01 | 2.8/0.03 | 12.7/0.01 | 25.9/0.01 | 0.2/0.01 | 2.8/0.03 | 12.7/0.02 | 25.8/0.01 | 0.2/0.01 | 2.8/0.05 | 12.8/0.03 | 23.0/0.03 |
| C S2 | 2.2/0.01 | 2.2/0.01 | 2.6/0.02 | 5.2/0.05 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.8/0.05 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.8/0.05 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.8/0.05 |
| D sen | 2.2/0.02 | 2.2/0.02 | 2.2/0.02 | 2.4/0.02 | 10.2/0.03 | 19.1/0.03 | 26.7/0.03 | 27.6/0.03 | 10.2/0.03 | 19.2/0.03 | 26.7/0.03 | 27.7/0.03 | 10.4/0.04 | 19.3/0.04 | 26.8/0.04 | 27.8/0.04 |
| D promo+w365 | 2.2/0.01 | 16.8/0.01 | 17.7/0.01 | 14.5/0.01 | 2.2/0.01 | 2.2/0.01 | 14.7/0.01 | 13.8/0.01 | 2.2/0.01 | 2.2/0.02 | 14.9/0.02 | 11.7/0.01 | 2.2/0.01 | 2.2/0.02 | 2.3/0.03 | 9.9/0.04 |
| D nopre | 2.2/0.01 | 2.2/0.01 | 2.2/0.02 | 2.4/0.02 | 2.1/0.01 | 2.1/0.01 | 2.1/0.02 | 2.1/0.02 | 2.1/0.01 | 2.1/0.01 | 2.1/0.02 | 2.1/0.02 | 2.1/0.01 | 2.1/0.01 | 2.1/0.02 | 2.1/0.02 |
| N nopre+newest | 2.2/0.02 | 2.2/0.02 | 2.2/0.02 | 2.4/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 |
| N nopre+newest promo | 2.2/0.02 | 9.2/0.02 | 7.8/0.01 | 8.8/0.01 | 2.1/0.02 | 2.1/0.02 | 8.8/0.01 | 8.7/0.01 | 2.1/0.02 | 2.1/0.02 | 9.1/0.02 | 8.5/0.01 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 7.6/0.01 |
| N sen+newest | 2.2/0.02 | 2.2/0.02 | 2.2/0.02 | 2.4/0.02 | 10.2/0.03 | 19.1/0.03 | 26.7/0.03 | 27.6/0.03 | 10.7/0.03 | 19.0/0.03 | 26.6/0.03 | 27.6/0.03 | 10.4/0.04 | 19.3/0.04 | 26.8/0.04 | 27.8/0.04 |
| N nopre+newest S1 | 2.1/0.01 | 2.2/0.01 | 2.2/0.01 | 2.4/0.01 | 2.1/0.01 | 2.1/0.01 | 2.1/0.01 | 2.1/0.01 | 2.1/0.01 | 2.1/0.01 | 2.1/0.01 | 2.1/0.01 | 2.1/0.01 | 2.1/0.01 | 2.1/0.01 | 2.1/0.01 |
| N nopre+newest days5 | 2.2/0.02 | 2.2/0.02 | 2.2/0.02 | 2.6/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 |
| N nopre+newest 20/60/20 | 2.2/0.02 | 2.2/0.02 | 2.2/0.02 | 2.4/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 |
| N nopre+newest 4.5d | 2.2/0.02 | 2.2/0.02 | 2.2/0.02 | 2.4/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 |
| R s30 S-nopre+newest | 2.2/0.02 | 2.2/0.02 | 2.6/0.03 | 5.9/0.03 | 2.2/0.03 | 2.2/0.03 | 2.6/0.03 | 6.0/0.03 | 2.6/0.01 | 2.2/0.01 | 2.2/0.02 | 2.6/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 | 2.1/0.02 |
| R 20/50/20/10 s30 | 2.2/0.01 | 2.2/0.01 | 2.5/0.01 | 6.0/0.02 | 2.2/0.01 | 2.2/0.01 | 2.5/0.01 | 6.1/0.02 | 3.5/0.01 | 2.4/0.01 | 2.5/0.02 | 5.5/0.05 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 4.9/0.04 |
| R 20/50/20/10 s30 promo | 2.2/0.01 | 15.6/0.01 | 17.1/0.01 | 14.5/0.01 | 2.2/0.01 | 2.2/0.01 | 14.3/0.01 | 13.9/0.01 | 10.3/0.01 | 24.2/0.01 | 27.8/0.02 | 18.1/0.01 | 2.2/0.01 | 2.2/0.01 | 2.5/0.02 | 11.4/0.04 |

### Load 50% (honest records / capacity)

Established honest publishers, mean presence % during a **continuous** attack (21 days); pre-attack baseline in brackets

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 51 [52] | 51 [52] | 51 [52] | 51 [52] | 51 [52] | 51 [52] | 51 [52] | 51 [52] | 51 [52] | 49 [52] | 49 [52] | 49 [52] | 51 [52] | 40 [52] | 35 [52] | 33 [52] |
| C S2 | 98 [100] | 48 [100] | 26 [100] | 16 [100] | 98 [100] | 43 [100] | 19 [100] | 7 [100] | 98 [100] | 43 [100] | 19 [100] | 7 [100] | 98 [100] | 43 [100] | 19 [100] | 7 [100] |
| D sen | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 97 [100] | 97 [100] | 97 [100] | 97 [100] | 66 [100] | 66 [100] | 66 [100] | 66 [100] |
| D promo+w365 | 100 [100] | 99 [100] | 88 [100] | 88 [100] | 100 [100] | 96 [100] | 70 [100] | 70 [100] | 100 [100] | 87 [100] | 34 [100] | 34 [100] | 96 [100] | 47 [100] | 20 [100] | 10 [100] |
| D nopre | 98 [100] | 94 [100] | 94 [100] | 94 [100] | 100 [100] | 94 [100] | 94 [100] | 94 [100] | 100 [100] | 94 [100] | 94 [100] | 94 [100] | 100 [100] | 94 [100] | 94 [100] | 94 [100] |
| N nopre+newest | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| N nopre+newest promo | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| N sen+newest | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 97 [100] | 97 [100] | 97 [100] | 97 [100] | 66 [100] | 66 [100] | 66 [100] | 66 [100] |
| N nopre+newest S1 | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| N nopre+newest days5 | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 99 [100] | 99 [100] | 99 [100] | 99 [100] |
| N nopre+newest 20/60/20 | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| N nopre+newest 4.5d | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| R s30 S-nopre+newest | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 97 [100] | 97 [100] | 97 [100] | 97 [100] | 94 [100] | 94 [100] | 94 [100] | 94 [100] |
| R 20/50/20/10 s30 | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 98 [100] | 47 [100] | 26 [100] | 16 [100] | 98 [100] | 42 [100] | 18 [100] | 7 [100] |
| R 20/50/20/10 s30 promo | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 98 [100] | 51 [100] | 41 [100] | 47 [100] | 98 [100] | 42 [100] | 18 [100] | 10 [100] |

Established, last day of the continuous attack % (small publishers ≤20 records / large)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 51 (100/48 min) | 51 (100/48 min) | 51 (100/48 min) | 51 (100/48 min) | 51 (100/48 min) | 51 (100/48 min) | 51 (100/48 min) | 51 (100/48 min) | 51 (100/48 min) | 49 (98/46 min) | 49 (98/46 min) | 49 (98/46 min) | 51 (100/48 min) | 40 (53/32 min) | 35 (53/32 min) | 32 (53/31 min) |
| C S2 | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) | 97 (100/97 min) | 42 (100/39 min) | 18 (100/14 min) | 7 (97/2 min) |
| D sen | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 64 (51/64 min) | 64 (51/64 min) | 64 (51/64 min) | 64 (51/64 min) |
| D promo+w365 | 100 (100/100 min) | 95 (100/95 min) | 61 (100/59 min) | 62 (100/59 min) | 100 (100/100 min) | 92 (100/91 min) | 44 (100/41 min) | 45 (100/41 min) | 100 (100/100 min) | 81 (100/80 min) | 30 (100/27 min) | 26 (100/22 min) | 95 (100/95 min) | 46 (100/42 min) | 19 (100/15 min) | 8 (96/4 min) |
| D nopre | 97 (100/97 min) | 81 (100/80 min) | 81 (100/80 min) | 80 (100/79 min) | 100 (100/100 min) | 80 (100/79 min) | 80 (100/79 min) | 80 (100/79 min) | 100 (100/100 min) | 80 (100/79 min) | 80 (100/79 min) | 80 (100/79 min) | 100 (100/100 min) | 80 (100/79 min) | 80 (100/79 min) | 80 (100/79 min) |
| N nopre+newest | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) |
| N nopre+newest promo | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (99/97 min) | 100 (100/100 min) | 100 (100/100 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) | 100 (100/100 min) |
| N sen+newest | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 64 (50/64 min) | 64 (50/64 min) | 64 (50/64 min) | 64 (50/64 min) |
| N nopre+newest S1 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) |
| N nopre+newest days5 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) |
| N nopre+newest 20/60/20 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) |
| N nopre+newest 4.5d | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) |
| R s30 S-nopre+newest | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 80 (62/81 min) | 80 (62/81 min) | 80 (62/81 min) | 80 (62/81 min) |
| R 20/50/20/10 s30 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (100/97 min) | 41 (100/38 min) | 18 (100/13 min) | 6 (95/2 min) | 97 (100/97 min) | 41 (100/38 min) | 18 (100/13 min) | 6 (95/2 min) |
| R 20/50/20/10 s30 promo | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (100/94 min) | 41 (100/36 min) | 23 (100/19 min) | 23 (100/19 min) | 97 (100/97 min) | 41 (100/38 min) | 18 (100/13 min) | 9 (100/5 min) |

Known (contacts/bootstrap) min % · publishers that joined before the attack (<7 d old) mean % · joined during the attack mean %  (continuous)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 100·51·76 | 100·51·49 | 100·51·17 | 100·51·3 | 100·51·76 | 100·27·50 | 100·6·17 | 100·1·3 | 100·51·76 | 100·27·51 | 100·6·18 | 100·1·3 | 100·51·76 | 74·23·48 | 74·6·17 | 74·1·3 |
| C S2 | 100·100·100 | 100·49·80 | 100·28·56 | 100·15·36 | 100·100·100 | 100·43·80 | 100·20·59 | 100·7·43 | 100·100·100 | 100·43·80 | 100·20·59 | 100·7·43 | 100·100·100 | 100·43·80 | 100·20·59 | 100·7·43 |
| D sen | 100·100·34 | 100·100·34 | 100·100·32 | 100·99·27 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 |
| D promo+w365 | 100·100·98 | 100·51·65 | 100·38·23 | 100·41·6 | 100·98·95 | 100·33·64 | 100·13·22 | 100·12·4 | 100·62·83 | 100·26·58 | 100·13·41 | 100·6·3 | 100·31·64 | 100·16·49 | 100·6·39 | 100·2·6 |
| D nopre | 100·100·100 | 100·99·100 | 100·95·97 | 100·93·91 | 100·100·100 | 100·99·100 | 100·99·100 | 100·99·100 | 100·100·100 | 100·99·100 | 100·99·100 | 100·99·100 | 100·100·100 | 100·99·100 | 100·99·100 | 100·99·100 |
| N nopre+newest | 100·94·81 | 100·94·81 | 100·93·77 | 100·93·72 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 |
| N nopre+newest promo | 100·94·81 | 100·98·83 | 100·94·23 | 100·92·6 | 100·59·81 | 100·60·81 | 100·88·22 | 100·83·4 | 100·59·81 | 100·59·81 | 100·90·61 | 100·83·3 | 100·59·81 | 100·59·81 | 100·59·81 | 100·84·6 |
| N sen+newest | 100·97·34 | 100·97·34 | 100·97·32 | 100·96·27 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 |
| N nopre+newest S1 | 100·100·29 | 100·100·29 | 100·96·23 | 100·93·13 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 | 100·96·26 |
| N nopre+newest days5 | 100·97·90 | 100·96·88 | 100·94·81 | 100·93·73 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 | 100·55·90 |
| N nopre+newest 20/60/20 | 100·94·81 | 100·94·81 | 100·93·77 | 100·93·72 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 |
| N nopre+newest 4.5d | 100·94·81 | 100·94·81 | 100·93·77 | 100·93·72 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 | 100·59·80 |
| R s30 S-nopre+newest | 100·99·96 | 100·41·69 | 100·22·48 | 100·13·31 | 100·97·96 | 100·33·69 | 100·13·51 | 100·4·39 | 100·100·100 | 100·92·100 | 100·91·100 | 100·90·100 | 100·100·100 | 100·100·100 | 100·100·100 | 100·100·100 |
| R 20/50/20/10 s30 | 100·100·100 | 100·43·73 | 100·23·51 | 100·13·33 | 100·100·100 | 100·36·73 | 100·15·54 | 100·5·41 | 100·100·100 | 100·93·100 | 100·91·100 | 100·90·100 | 100·100·100 | 100·100·100 | 100·100·100 | 100·100·100 |
| R 20/50/20/10 s30 promo | 100·100·100 | 100·44·68 | 100·33·23 | 100·38·6 | 100·100·100 | 100·36·73 | 100·14·22 | 100·12·4 | 100·99·100 | 100·76·97 | 100·12·42 | 100·6·3 | 100·100·100 | 100·100·100 | 100·100·100 | 100·4·6 |

Time until established honest presence < 90% / < 50% (continuous attack)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |
| C S2 | never / never | 2.0d / 2.0d | 2.0d / 2.0d | 2.0d / 2.0d | never / never | 0h / 0h | 0h / 0h | 0h / 0h | never / never | 0h / 0h | 0h / 0h | 0h / 0h | never / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D sen | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | 0h / never | 0h / never | 0h / never | 0h / never |
| D promo+w365 | never / never | never / never | 12.0d / never | 12.0d / never | never / never | never / never | 4.0d / 17.0d | 4.0d / 17.1d | never / never | 0h / never | 0h / 1h | 6h / 9h | never / never | 0h / 0h | 0h / 0h | 1h / 1h |
| D nopre | never / never | 18.1d / never | 18.1d / never | 18.1d / never | never / never | 18.1d / never | 18.1d / never | 18.1d / never | never / never | 18.1d / never | 18.1d / never | 18.1d / never | never / never | 18.1d / never | 18.1d / never | 18.1d / never |
| N nopre+newest | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N nopre+newest promo | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N sen+newest | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | 0h / never | 0h / never | 0h / never | 0h / never |
| N nopre+newest S1 | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N nopre+newest days5 | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N nopre+newest 20/60/20 | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N nopre+newest 4.5d | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| R s30 S-nopre+newest | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | 18.1d / never | 18.1d / never | 18.1d / never | 18.1d / never |
| R 20/50/20/10 s30 | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | 2.0d / 2.0d | 2.0d / 2.0d | 2.0d / 2.0d | never / never | 0h / 0h | 0h / 0h | 0h / 0h |
| R 20/50/20/10 s30 promo | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | 2.6d / 4.7d | 3.0d / 4.4d | 4.9d / 6.0d | never / never | 0h / 0h | 0h / 0h | 1h / 1h |

1-day attack: min established % · min all honest % · recovery to baseline−1pt after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 52·53·0h | 52·53·0h | 52·53·0h | 52·53·0h | 52·53·0h | 52·49·3h | 52·49·7.7d | 52·49·7.7d | 52·53·0h | 50·47·7.5d | 50·47·7.7d | 50·47·7.7d | 52·53·0h | 33·31·7.7d | 33·31·8.7d | 33·31·8.7d |
| C S2 | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 99·99·14h | 44·45·8.7d | 20·22·9.9d | 8·10·9.9d | 99·99·14h | 44·45·8.7d | 20·22·9.9d | 8·10·9.9d | 99·99·14h | 44·45·8.7d | 20·22·9.9d | 8·10·9.9d |
| D sen | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 97·91·7.7d | 97·91·7.7d | 97·91·7.7d | 97·91·7.7d | 66·63·9.2d | 66·63·9.2d | 66·63·9.2d | 66·63·9.2d |
| D promo+w365 | 100·100·0h | 100·96·7h | 100·94·7h | 100·94·36h | 100·98·2.3d | 100·95·7.7d | 96·90·4.9d | 96·90·4.9d | 100·96·7.7d | 89·85·8.7d | 44·42·8.7d | 44·42·8.7d | 95·90·7.7d | 48·47·8.7d | 21·22·9.9d | 11·12·9.9d |
| D nopre | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest promo | 100·100·0h | 100·96·7h | 100·94·7h | 100·94·36h | 100·100·0h | 100·100·0h | 100·94·4.7d | 100·94·4.7d | 100·100·0h | 100·100·0h | 100·94·3.4d | 100·94·4.7d | 100·100·0h | 100·100·0h | 100·100·0h | 100·94·4.7d |
| N sen+newest | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 97·91·7.7d | 97·91·7.7d | 97·91·7.7d | 97·91·7.7d | 66·63·9.2d | 66·63·9.2d | 66·63·9.2d | 66·63·9.2d |
| N nopre+newest S1 | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest days5 | 100·100·0h | 100·96·31h | 100·94·31h | 100·94·2.9d | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest 20/60/20 | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest 4.5d | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| R s30 S-nopre+newest | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 100·100·0h | 100·96·7.7d | 100·95·7.7d | 100·94·7.7d | 100·100·0h | 100·96·7.5d | 100·95·7.5d | 100·94·7.5d | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| R 20/50/20/10 s30 | 100·100·0h | 100·96·0h | 100·94·0h | 100·94·12h | 100·100·0h | 100·96·7.5d | 100·95·7.7d | 100·94·7.7d | 100·100·0h | 100·96·8.7d | 100·95·8.7d | 100·94·8.7d | 99·99·14h | 44·49·8.7d | 20·27·9.9d | 7·15·9.9d |
| R 20/50/20/10 s30 promo | 100·100·0h | 100·96·7h | 100·94·7h | 100·94·36h | 100·100·0h | 100·96·7.5d | 100·94·4.7d | 100·94·4.7d | 100·100·0h | 100·96·7.7d | 100·94·3.4d | 100·93·4.7d | 99·99·14h | 44·49·8.7d | 20·27·9.9d | 11·12·9.9d |

Store writes/s during the continuous attack: all (inserts+evictions+expiries) / honest-only; attacker refreshes not counted

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 0.0/0.01 | 0.1/0.01 | 0.4/0.01 | 3.3/0.01 | 0.0/0.01 | 0.2/0.01 | 0.7/0.01 | 3.8/0.01 | 0.0/0.01 | 0.2/0.01 | 0.7/0.01 | 3.8/0.01 | 0.0/0.01 | 0.2/0.03 | 0.7/0.02 | 4.1/0.02 |
| C S2 | 0.1/0.01 | 0.3/0.04 | 0.6/0.06 | 3.9/0.06 | 0.1/0.01 | 0.2/0.04 | 0.5/0.06 | 4.0/0.06 | 0.1/0.01 | 0.2/0.04 | 0.5/0.06 | 4.0/0.06 | 0.1/0.01 | 0.2/0.04 | 0.5/0.06 | 4.0/0.06 |
| D sen | 0.1/0.02 | 0.1/0.02 | 0.2/0.02 | 0.3/0.02 | 0.3/0.03 | 0.5/0.03 | 0.7/0.03 | 0.7/0.03 | 0.3/0.03 | 0.5/0.03 | 0.7/0.03 | 0.8/0.03 | 0.3/0.04 | 0.6/0.04 | 0.9/0.04 | 0.9/0.04 |
| D promo+w365 | 0.2/0.02 | 0.6/0.02 | 0.7/0.03 | 3.2/0.03 | 0.2/0.02 | 0.2/0.02 | 0.6/0.03 | 3.0/0.03 | 0.2/0.02 | 0.2/0.03 | 0.6/0.05 | 2.3/0.04 | 0.2/0.03 | 0.2/0.05 | 0.2/0.06 | 1.9/0.05 |
| D nopre | 0.1/0.01 | 0.1/0.02 | 0.2/0.02 | 0.3/0.02 | 0.1/0.01 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.01 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.01 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 |
| N nopre+newest | 0.1/0.02 | 0.1/0.02 | 0.2/0.02 | 0.3/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 |
| N nopre+newest promo | 0.1/0.02 | 0.3/0.02 | 0.5/0.01 | 3.0/0.01 | 0.1/0.02 | 0.1/0.02 | 0.4/0.01 | 2.8/0.01 | 0.1/0.02 | 0.1/0.02 | 0.3/0.02 | 2.1/0.01 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 1.7/0.01 |
| N sen+newest | 0.1/0.02 | 0.1/0.02 | 0.2/0.02 | 0.3/0.02 | 0.3/0.03 | 0.5/0.03 | 0.7/0.03 | 0.7/0.03 | 0.3/0.03 | 0.5/0.03 | 0.7/0.03 | 0.8/0.03 | 0.4/0.04 | 0.6/0.04 | 0.9/0.04 | 0.9/0.04 |
| N nopre+newest S1 | 0.1/0.01 | 0.1/0.01 | 0.1/0.01 | 0.3/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 |
| N nopre+newest days5 | 0.1/0.02 | 0.1/0.02 | 0.2/0.02 | 0.7/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 |
| N nopre+newest 20/60/20 | 0.1/0.02 | 0.1/0.02 | 0.2/0.02 | 0.3/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 |
| N nopre+newest 4.5d | 0.1/0.02 | 0.1/0.02 | 0.2/0.02 | 0.3/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 |
| R s30 S-nopre+newest | 0.2/0.02 | 0.2/0.03 | 0.5/0.03 | 2.5/0.03 | 0.2/0.03 | 0.2/0.03 | 0.5/0.03 | 2.6/0.03 | 0.1/0.01 | 0.1/0.02 | 0.2/0.02 | 0.4/0.03 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 |
| R 20/50/20/10 s30 | 0.1/0.01 | 0.2/0.02 | 0.5/0.02 | 2.6/0.02 | 0.1/0.01 | 0.2/0.02 | 0.5/0.02 | 2.7/0.02 | 0.2/0.01 | 0.3/0.05 | 0.6/0.06 | 4.1/0.06 | 0.1/0.01 | 0.2/0.04 | 0.5/0.05 | 4.0/0.05 |
| R 20/50/20/10 s30 promo | 0.1/0.01 | 0.6/0.02 | 0.7/0.01 | 3.2/0.01 | 0.1/0.01 | 0.2/0.02 | 0.7/0.01 | 3.1/0.01 | 0.3/0.02 | 0.9/0.05 | 1.2/0.05 | 2.7/0.04 | 0.1/0.01 | 0.2/0.04 | 0.5/0.05 | 3.3/0.05 |

### Load 90% (honest records / capacity)

Established honest publishers, mean presence % during a **continuous** attack (21 days); pre-attack baseline in brackets

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 37 [38] | 37 [38] | 37 [38] | 37 [38] | 37 [38] | 37 [38] | 37 [38] | 37 [38] | 37 [38] | 36 [38] | 36 [38] | 35 [38] | 37 [38] | 28 [38] | 25 [38] | 23 [38] |
| C S2 | 69 [98] | 36 [98] | 21 [98] | 13 [98] | 67 [98] | 30 [98] | 14 [98] | 5 [98] | 67 [98] | 30 [98] | 14 [98] | 5 [98] | 67 [98] | 30 [98] | 14 [98] | 5 [98] |
| D sen | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 97 [100] | 97 [100] | 97 [100] | 97 [100] | 66 [100] | 66 [100] | 66 [100] | 66 [100] |
| D promo+w365 | 98 [100] | 84 [100] | 69 [100] | 70 [100] | 96 [100] | 76 [100] | 48 [100] | 49 [100] | 92 [100] | 59 [100] | 26 [100] | 26 [100] | 73 [100] | 32 [100] | 15 [100] | 7 [100] |
| D nopre | 84 [98] | 84 [98] | 84 [98] | 69 [98] | 92 [98] | 92 [98] | 92 [98] | 92 [98] | 92 [98] | 92 [98] | 92 [98] | 92 [98] | 92 [98] | 92 [98] | 92 [98] | 92 [98] |
| N nopre+newest | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| N nopre+newest promo | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| N sen+newest | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 97 [100] | 97 [100] | 97 [100] | 97 [100] | 66 [100] | 66 [100] | 66 [100] | 66 [100] |
| N nopre+newest S1 | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| N nopre+newest days5 | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| N nopre+newest 20/60/20 | 90 [100] | 90 [100] | 90 [100] | 90 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 99 [100] | 99 [100] | 99 [100] | 99 [100] |
| N nopre+newest 4.5d | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] | 100 [100] |
| R s30 S-nopre+newest | 89 [98] | 89 [98] | 89 [98] | 89 [98] | 89 [98] | 89 [98] | 89 [98] | 89 [98] | 88 [98] | 88 [98] | 88 [98] | 88 [98] | 93 [98] | 93 [98] | 93 [98] | 93 [98] |
| R 20/50/20/10 s30 | 89 [98] | 89 [98] | 89 [98] | 89 [98] | 89 [98] | 89 [98] | 89 [98] | 89 [98] | 67 [98] | 34 [98] | 20 [98] | 13 [98] | 65 [98] | 29 [98] | 13 [98] | 5 [98] |
| R 20/50/20/10 s30 promo | 89 [98] | 87 [98] | 81 [98] | 81 [98] | 89 [98] | 89 [98] | 80 [98] | 80 [98] | 67 [98] | 37 [98] | 31 [98] | 34 [98] | 65 [98] | 29 [98] | 13 [98] | 8 [98] |

Established, last day of the continuous attack % (small publishers ≤20 records / large)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 37 (100/33 min) | 37 (100/33 min) | 37 (100/33 min) | 37 (100/33 min) | 37 (100/33 min) | 37 (100/33 min) | 37 (100/33 min) | 37 (100/33 min) | 37 (100/33 min) | 36 (98/32 min) | 35 (98/32 min) | 35 (97/32 min) | 37 (100/33 min) | 28 (53/22 min) | 25 (53/22 min) | 23 (52/21 min) |
| C S2 | 64 (100/62 min) | 29 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) | 64 (100/62 min) | 29 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) | 64 (100/62 min) | 29 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) | 64 (100/62 min) | 29 (100/26 min) | 13 (100/9 min) | 5 (82/1 min) |
| D sen | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 64 (51/64 min) | 64 (51/64 min) | 64 (51/64 min) | 64 (51/64 min) |
| D promo+w365 | 92 (100/91 min) | 70 (100/67 min) | 42 (100/38 min) | 42 (100/39 min) | 90 (100/89 min) | 64 (100/61 min) | 31 (100/28 min) | 31 (100/28 min) | 86 (100/84 min) | 53 (100/50 min) | 24 (100/20 min) | 21 (100/17 min) | 69 (100/67 min) | 31 (100/28 min) | 14 (100/10 min) | 6 (87/2 min) |
| D nopre | 76 (100/75 min) | 74 (100/73 min) | 74 (100/73 min) | 58 (100/56 min) | 82 (100/81 min) | 81 (100/80 min) | 81 (100/80 min) | 81 (100/80 min) | 82 (100/81 min) | 81 (100/80 min) | 81 (100/80 min) | 81 (100/80 min) | 82 (100/81 min) | 81 (100/80 min) | 81 (100/80 min) | 81 (100/80 min) |
| N nopre+newest | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) |
| N nopre+newest promo | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (99/97 min) | 97 (99/97 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 100 (100/100 min) |
| N sen+newest | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 64 (50/64 min) | 64 (50/64 min) | 64 (50/64 min) | 64 (50/64 min) |
| N nopre+newest S1 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) |
| N nopre+newest days5 | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) |
| N nopre+newest 20/60/20 | 89 (92/89 min) | 89 (92/89 min) | 89 (92/89 min) | 89 (92/89 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 97 (97/97 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) | 94 (93/94 min) |
| N nopre+newest 4.5d | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 100 (100/100 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) | 97 (99/97 min) |
| R s30 S-nopre+newest | 84 (62/81 min) | 84 (62/81 min) | 84 (62/81 min) | 84 (62/81 min) | 84 (62/81 min) | 84 (62/81 min) | 84 (62/81 min) | 84 (62/81 min) | 88 (91/87 min) | 88 (91/87 min) | 88 (91/87 min) | 88 (91/87 min) | 78 (62/79 min) | 78 (62/79 min) | 78 (62/79 min) | 78 (62/79 min) |
| R 20/50/20/10 s30 | 84 (100/80 min) | 84 (100/80 min) | 84 (100/80 min) | 84 (100/80 min) | 84 (100/80 min) | 84 (100/80 min) | 84 (100/80 min) | 84 (100/80 min) | 61 (100/59 min) | 28 (100/24 min) | 12 (100/8 min) | 4 (75/1 min) | 61 (100/59 min) | 28 (100/24 min) | 12 (100/8 min) | 4 (76/1 min) |
| R 20/50/20/10 s30 promo | 84 (100/80 min) | 84 (100/77 min) | 81 (100/77 min) | 81 (100/77 min) | 84 (100/80 min) | 84 (100/80 min) | 81 (100/76 min) | 81 (100/76 min) | 62 (100/60 min) | 28 (100/24 min) | 18 (100/14 min) | 18 (100/13 min) | 62 (100/61 min) | 28 (100/24 min) | 12 (100/8 min) | 7 (99/3 min) |

Known (contacts/bootstrap) min % · publishers that joined before the attack (<7 d old) mean % · joined during the attack mean %  (continuous)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 100·38·61 | 100·38·40 | 100·38·13 | 100·38·0 | 100·38·61 | 100·18·41 | 100·4·13 | 100·0·1 | 100·38·61 | 100·18·41 | 100·4·13 | 100·0·1 | 100·38·61 | 74·13·34 | 74·4·12 | 74·0·1 |
| C S2 | 100·71·98 | 100·39·67 | 100·22·46 | 100·13·29 | 100·68·98 | 100·33·69 | 100·15·53 | 100·5·41 | 100·68·98 | 100·33·69 | 100·15·53 | 100·5·41 | 100·68·98 | 100·33·69 | 100·15·53 | 100·5·41 |
| D sen | 100·83·34 | 100·83·32 | 100·83·27 | 100·83·23 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 |
| D promo+w365 | 100·37·67 | 100·25·52 | 100·16·15 | 100·13·1 | 100·33·65 | 100·22·55 | 100·10·17 | 100·6·0 | 100·28·61 | 100·19·51 | 100·9·35 | 100·3·1 | 100·22·54 | 100·11·44 | 100·4·34 | 100·1·3 |
| D nopre | 100·95·99 | 100·88·97 | 100·84·91 | 100·61·88 | 100·99·99 | 100·99·99 | 100·99·99 | 100·99·99 | 100·99·99 | 100·99·99 | 100·99·99 | 100·99·99 | 100·99·99 | 100·99·99 | 100·99·99 | 100·99·99 |
| N nopre+newest | 100·14·79 | 100·13·77 | 100·13·71 | 100·11·68 | 100·52·79 | 100·52·79 | 100·52·79 | 100·52·79 | 100·52·79 | 100·52·79 | 100·52·79 | 100·52·79 | 100·52·79 | 100·52·79 | 100·52·79 | 100·52·79 |
| N nopre+newest promo | 100·14·80 | 100·17·79 | 100·13·15 | 100·11·0 | 100·52·80 | 100·53·80 | 100·7·17 | 100·3·0 | 100·52·80 | 100·52·80 | 100·8·56 | 100·3·1 | 100·52·80 | 100·52·80 | 100·52·80 | 100·4·3 |
| N sen+newest | 100·82·34 | 100·81·32 | 100·81·27 | 100·81·23 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 | 100·1·34 |
| N nopre+newest S1 | 100·68·26 | 100·65·16 | 100·63·9 | 100·63·2 | 100·72·25 | 100·72·25 | 100·72·25 | 100·72·25 | 100·72·25 | 100·72·25 | 100·72·25 | 100·72·25 | 100·72·25 | 100·72·25 | 100·72·25 | 100·72·25 |
| N nopre+newest days5 | 100·20·86 | 100·17·80 | 100·15·73 | 100·15·68 | 100·47·89 | 100·47·89 | 100·47·89 | 100·47·89 | 100·47·89 | 100·47·89 | 100·47·89 | 100·47·89 | 100·47·89 | 100·47·89 | 100·47·89 | 100·47·89 |
| N nopre+newest 20/60/20 | 100·6·80 | 100·6·80 | 100·6·73 | 100·5·69 | 100·52·80 | 100·52·80 | 100·52·80 | 100·52·80 | 100·52·80 | 100·52·80 | 100·52·80 | 100·52·80 | 100·52·80 | 100·52·80 | 100·52·80 | 100·52·80 |
| N nopre+newest 4.5d | 100·14·79 | 100·13·77 | 100·13·71 | 100·11·68 | 100·53·79 | 100·53·79 | 100·53·79 | 100·53·79 | 100·53·79 | 100·53·79 | 100·53·79 | 100·53·79 | 100·53·79 | 100·53·79 | 100·53·79 | 100·53·79 |
| R s30 S-nopre+newest | 100·49·78 | 100·28·56 | 100·15·36 | 100·10·24 | 100·44·78 | 100·20·58 | 100·7·43 | 100·2·35 | 100·94·100 | 100·91·100 | 100·91·100 | 100·90·100 | 100·100·100 | 100·100·100 | 100·100·100 | 100·100·100 |
| R 20/50/20/10 s30 | 100·50·79 | 100·28·57 | 100·15·36 | 100·10·25 | 100·44·79 | 100·21·59 | 100·7·44 | 100·2·35 | 100·94·100 | 100·91·100 | 100·91·100 | 100·90·100 | 100·100·100 | 100·100·100 | 100·100·100 | 100·100·100 |
| R 20/50/20/10 s30 promo | 100·50·79 | 100·29·53 | 100·21·15 | 100·24·0 | 100·44·79 | 100·21·59 | 100·8·17 | 100·6·0 | 100·92·99 | 100·73·95 | 100·9·36 | 100·3·1 | 100·100·99 | 100·100·99 | 100·100·99 | 100·3·3 |

Time until established honest presence < 90% / < 50% (continuous attack)

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h | 0h / 0h |
| C S2 | 0h / never | 0h / 2.0d | 0h / 2.0d | 0h / 2.0d | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |
| D sen | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | 0h / never | 0h / never | 0h / never | 0h / never |
| D promo+w365 | never / never | 4.2d / never | 4.3d / 16.0d | 4.5d / 16.3d | 19.5d / never | 0h / never | 0h / 8.4d | 4h / 8.6d | 18.1d / never | 0h / never | 0h / 0h | 1h / 6h | 0h / never | 0h / 0h | 0h / 0h | 0h / 1h |
| D nopre | 0h / never | 0h / never | 0h / never | 0h / never | 18.1d / never | 18.1d / never | 18.1d / never | 18.1d / never | 18.1d / never | 18.1d / never | 18.1d / never | 18.1d / never | 18.1d / never | 18.1d / never | 18.1d / never | 18.1d / never |
| N nopre+newest | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N nopre+newest promo | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N sen+newest | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | 0h / never | 0h / never | 0h / never | 0h / never |
| N nopre+newest S1 | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N nopre+newest days5 | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N nopre+newest 20/60/20 | 0h / never | 0h / never | 0h / never | 0h / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| N nopre+newest 4.5d | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never | never / never |
| R s30 S-nopre+newest | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 18.1d / never | 18.1d / never | 18.1d / never | 18.1d / never |
| R 20/50/20/10 s30 | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / 2.0d | 0h / 2.0d | 0h / 2.0d | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |
| R 20/50/20/10 s30 promo | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / never | 0h / 3.0d | 0h / 3.0d | 0h / 4.9d | 0h / never | 0h / 0h | 0h / 0h | 0h / 0h |

1-day attack: min established % · min all honest % · recovery to baseline−1pt after it ends

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 38·39·0h | 38·39·0h | 38·39·0h | 38·39·0h | 38·39·0h | 38·36·0h | 38·36·7.7d | 38·36·7.7d | 38·39·0h | 37·35·7.5d | 37·35·7.7d | 37·35·7.7d | 38·39·0h | 24·23·8.7d | 24·23·8.7d | 24·23·8.7d |
| C S2 | 94·94·5.5d | 94·94·5.5d | 94·93·5.5d | 94·93·5.5d | 69·70·8.7d | 31·33·9.9d | 15·17·9.9d | 6·8·9.9d | 69·70·8.7d | 31·33·9.9d | 15·17·9.9d | 6·8·9.9d | 69·70·8.7d | 31·33·9.9d | 15·17·9.9d | 6·8·9.9d |
| D sen | 100·94·4.7d | 100·94·4.7d | 100·93·4.7d | 100·93·4.7d | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 97·91·7.7d | 97·91·7.7d | 97·91·7.7d | 97·91·7.7d | 66·63·9.2d | 66·63·9.2d | 66·63·9.2d | 66·63·9.2d |
| D promo+w365 | 99·94·4.9d | 99·94·4.9d | 99·93·4.9d | 99·93·4.9d | 97·93·7.7d | 85·81·8.7d | 75·71·8.7d | 76·71·8.7d | 91·87·8.7d | 62·61·8.7d | 32·31·9.9d | 32·31·9.5d | 71·69·8.7d | 33·33·9.9d | 16·17·9.9d | 9·9·9.9d |
| D nopre | 98·94·4.7d | 99·94·4.7d | 99·93·4.7d | 99·93·4.7d | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest | 100·94·4.8d | 100·94·4.8d | 100·93·4.8d | 100·93·4.8d | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest promo | 100·94·4.8d | 100·94·4.8d | 100·93·4.8d | 100·93·4.8d | 100·100·0h | 100·100·0h | 100·93·4.7d | 100·93·4.7d | 100·100·0h | 100·100·0h | 100·93·4.7d | 100·93·4.8d | 100·100·0h | 100·100·0h | 100·100·0h | 100·92·4.8d |
| N sen+newest | 100·94·4.7d | 100·94·4.7d | 100·93·4.7d | 100·93·4.7d | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 100·94·7.7d | 97·91·7.7d | 97·91·7.7d | 97·91·7.7d | 97·91·7.7d | 66·63·9.2d | 66·63·9.2d | 66·63·9.2d | 66·63·9.2d |
| N nopre+newest S1 | 100·96·4.7d | 100·94·4.7d | 100·94·4.7d | 100·94·4.7d | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest days5 | 100·95·4.7d | 100·94·4.7d | 100·93·4.7d | 100·93·4.7d | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest 20/60/20 | 89·84·4.9d | 89·84·4.9d | 89·83·4.9d | 89·83·4.9d | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| N nopre+newest 4.5d | 100·94·5.4d | 100·94·5.4d | 100·93·5.4d | 100·93·5.4d | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| R s30 S-nopre+newest | 94·95·4.9d | 94·94·4.9d | 94·94·4.9d | 94·93·4.9d | 82·81·7.7d | 82·79·8.2d | 82·78·8.5d | 82·78·8.6d | 82·81·7.6d | 82·79·7.6d | 82·78·7.6d | 82·78·7.9d | 100·100·0h | 100·100·0h | 100·100·0h | 100·100·0h |
| R 20/50/20/10 s30 | 94·94·5.5d | 94·94·5.5d | 94·93·5.5d | 94·93·5.5d | 82·83·8.7d | 82·81·8.7d | 82·81·8.7d | 82·81·8.7d | 82·83·8.7d | 82·81·8.7d | 82·81·8.7d | 82·81·8.7d | 69·72·8.7d | 31·37·9.9d | 15·22·9.9d | 6·14·9.9d |
| R 20/50/20/10 s30 promo | 98·94·5.5d | 99·94·5.5d | 99·93·5.5d | 99·93·5.5d | 82·83·8.7d | 82·81·8.7d | 77·73·8.7d | 77·73·8.7d | 82·83·8.7d | 82·81·8.7d | 77·73·8.7d | 77·72·8.7d | 69·72·8.7d | 31·37·9.9d | 15·22·9.9d | 8·9·9.9d |

Store writes/s during the continuous attack: all (inserts+evictions+expiries) / honest-only; attacker refreshes not counted

| policy | fresh N=10 | fresh N=100 | fresh N=1000 | fresh N=10000 | pre7 N=10 | pre7 N=100 | pre7 N=1000 | pre7 N=10000 | pre30 N=10 | pre30 N=100 | pre30 N=1000 | pre30 N=10000 | pre180 N=10 | pre180 N=100 | pre180 N=1000 | pre180 N=10000 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A today | 0.0/0.01 | 0.1/0.01 | 0.4/0.01 | 1.0/0.01 | 0.0/0.01 | 0.1/0.01 | 0.5/0.01 | 1.3/0.01 | 0.0/0.01 | 0.1/0.01 | 0.5/0.01 | 1.3/0.01 | 0.0/0.01 | 0.1/0.02 | 0.5/0.02 | 1.4/0.02 |
| C S2 | 0.1/0.04 | 0.2/0.05 | 0.5/0.06 | 1.9/0.07 | 0.1/0.03 | 0.2/0.05 | 0.5/0.06 | 2.0/0.07 | 0.1/0.03 | 0.2/0.05 | 0.5/0.06 | 2.0/0.07 | 0.1/0.03 | 0.2/0.05 | 0.5/0.06 | 2.0/0.07 |
| D sen | 0.1/0.04 | 0.1/0.04 | 0.1/0.04 | 0.2/0.04 | 0.1/0.02 | 0.1/0.02 | 0.2/0.02 | 0.2/0.02 | 0.1/0.03 | 0.2/0.03 | 0.2/0.03 | 0.2/0.03 | 0.2/0.04 | 0.3/0.04 | 0.4/0.04 | 0.4/0.04 |
| D promo+w365 | 0.1/0.03 | 0.3/0.04 | 0.4/0.03 | 1.0/0.03 | 0.1/0.03 | 0.1/0.04 | 0.4/0.04 | 0.8/0.04 | 0.1/0.03 | 0.2/0.04 | 0.4/0.05 | 0.5/0.04 | 0.1/0.04 | 0.2/0.05 | 0.2/0.06 | 1.7/0.05 |
| D nopre | 0.1/0.02 | 0.1/0.03 | 0.1/0.03 | 0.2/0.03 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 |
| N nopre+newest | 0.0/0.02 | 0.1/0.02 | 0.1/0.02 | 0.2/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 |
| N nopre+newest promo | 0.0/0.02 | 0.1/0.02 | 0.3/0.01 | 0.8/0.01 | 0.0/0.02 | 0.0/0.02 | 0.3/0.01 | 0.7/0.01 | 0.0/0.02 | 0.0/0.02 | 0.2/0.02 | 0.4/0.01 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 1.5/0.01 |
| N sen+newest | 0.1/0.04 | 0.1/0.04 | 0.1/0.04 | 0.2/0.04 | 0.1/0.02 | 0.1/0.02 | 0.2/0.02 | 0.2/0.02 | 0.1/0.03 | 0.2/0.03 | 0.2/0.03 | 0.2/0.03 | 0.1/0.04 | 0.3/0.04 | 0.4/0.04 | 0.4/0.04 |
| N nopre+newest S1 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.5/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 | 0.0/0.01 |
| N nopre+newest days5 | 0.0/0.02 | 0.1/0.02 | 0.1/0.02 | 0.3/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 |
| N nopre+newest 20/60/20 | 0.1/0.02 | 0.1/0.02 | 0.1/0.02 | 0.3/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 |
| N nopre+newest 4.5d | 0.0/0.02 | 0.1/0.02 | 0.1/0.02 | 0.2/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 |
| R s30 S-nopre+newest | 0.1/0.04 | 0.2/0.04 | 0.4/0.04 | 2.3/0.04 | 0.1/0.04 | 0.1/0.04 | 0.4/0.04 | 2.3/0.04 | 0.1/0.03 | 0.1/0.03 | 0.1/0.03 | 0.3/0.03 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 | 0.0/0.02 |
| R 20/50/20/10 s30 | 0.1/0.04 | 0.2/0.04 | 0.4/0.05 | 2.3/0.05 | 0.1/0.04 | 0.2/0.04 | 0.4/0.05 | 2.3/0.05 | 0.1/0.04 | 0.2/0.06 | 0.5/0.06 | 2.6/0.07 | 0.1/0.03 | 0.2/0.04 | 0.4/0.05 | 2.4/0.06 |
| R 20/50/20/10 s30 promo | 0.1/0.04 | 0.3/0.05 | 0.5/0.02 | 1.0/0.02 | 0.1/0.04 | 0.2/0.04 | 0.5/0.02 | 0.9/0.03 | 0.2/0.05 | 0.5/0.06 | 0.8/0.06 | 0.9/0.04 | 0.1/0.03 | 0.2/0.04 | 0.4/0.05 | 3.1/0.05 |

### E6 peacetime (3 seeds): established % · newly joined % · honest refused/day · honest evicted/day

| policy | 2% | 50% | 90% | 110% |
|---|---|---|---|---|
| A today | 100·99·82·0 | 52·74·19217·0 | 40·67·24764·0 | 37·65·26066·0 |
| C S2 | 100·100·0·0 | 100·100·0·0 | 99·100·696·173 | 86·96·5525·601 |
| D sen | 100·100·0·0 | 100·100·0·0 | 100·93·603·342 | 93·45·6304·1268 |
| D promo+w365 | 100·100·0·0 | 100·100·0·0 | 100·95·584·270 | 90·75·6403·965 |
| D nopre | 100·100·0·0 | 100·100·0·0 | 99·100·693·160 | 86·98·6205·480 |
| N nopre+newest | 100·100·0·0 | 100·100·0·0 | 100·94·604·164 | 90·65·7068·506 |
| N nopre+newest promo | 100·100·0·0 | 100·100·0·0 | 100·94·604·166 | 90·65·7069·508 |
| N sen+newest | 100·100·0·0 | 100·100·0·0 | 100·93·569·342 | 93·45·6300·1266 |
| N nopre+newest S1 | 100·100·0·0 | 100·100·0·0 | 100·93·803·29 | 94·22·7074·34 |
| N nopre+newest days5 | 100·100·0·0 | 100·100·0·0 | 100·94·642·168 | 89·74·7158·508 |
| N nopre+newest 20/60/20 | 100·100·0·0 | 100·100·0·0 | 100·94·628·164 | 90·65·7056·516 |
| N nopre+newest 4.5d | 100·100·0·0 | 100·100·0·0 | 100·94·604·164 | 90·65·7069·506 |
| R s30 S-nopre+newest | 100·100·0·0 | 100·100·0·0 | 99·100·650·192 | 86·99·6430·519 |
| R 20/50/20/10 s30 | 100·100·0·0 | 100·100·0·0 | 99·100·683·194 | 86·99·5510·619 |
| R 20/50/20/10 s30 promo | 100·100·0·0 | 100·100·0·0 | 99·100·697·172 | 86·99·5519·601 |

Big mirror joining a small network (as E0): mirror stored after 1 d % · at end % · others % · honest evictions

| policy | mirror |
|---|---|
| A today | 1·3·32·0 |
| C S2 | 98·94·99·782 |
| D sen | 98·100·100·1687 |
| D promo+w365 | 98·88·100·1054 |
| D nopre | 98·94·99·757 |
| N nopre+newest | 98·92·100·770 |
| N nopre+newest promo | 98·92·100·770 |
| N sen+newest | 98·100·100·1683 |
| N nopre+newest S1 | 98·100·100·89 |
| N nopre+newest days5 | 98·91·100·767 |
| N nopre+newest 20/60/20 | 98·92·100·770 |
| N nopre+newest 4.5d | 98·92·100·773 |
| R s30 S-nopre+newest | 98·88·100·1073 |
| R 20/50/20/10 s30 | 98·88·100·757 |
| R 20/50/20/10 s30 promo | 98·88·100·757 |

**E6 reading** (pre-aged 30/180 d columns included; continuous attack, est / new-before-attack / joined-during):
- **No pre-emption inside Earned ("D nopre") is the only design that holds established publishers at ≥ 92%
  for every N and every pre-age (7, 30, 180 days) — except fresh floods on a store already ≥ 90% full by honest
  records (69–84%, next bullet) — while keeping newcomers at 88–100%** and leaving peacetime
  unchanged (same numbers as C S2 in E0/E6 peacetime; a big mirror joining still gets 98% within a day). It
  needs no promotion limit, no age weights and no admission rule. 1-day attacks: established ≥ 98%, recovery 0 h
  except a fresh flood on a 90%-full store (4.7 d, the Newcomer expiry).
- Its residual loss is the Newcomer floor: identities that fill the Newcomer floor (taken from the largest Earned
  holders under S2) keep those records when they are promoted, so each promotion wave moves up to one floor's worth
  (10% of C) into Earned. On a store whose honest load is under ~85% this costs nothing (the floor comes out of
  free space); at 90% load it costs 8–16% of established records (fresh N ≤ 1000) and 31% at N = 10000.
- **Seniority ("sen", today's rule)** keeps established at 100% against identities younger than them but costs
  everyone younger than the attacker: new-before-attack 1%, and with 180-day pre-ageing established fall to 66%
  (the half of publishers younger than 180 days). Max-min with age weights + promotion limit (D promo+w365)
  collapses against 30- and 180-day pre-ageing (10–47% at 50% load).
- **Reclaiming from the most recently promoted ("newest") instead of the largest holder** gets established to
  100% everywhere but moves the cost onto honest publishers promoted just before or during the attack (new-
  before-attack 11–59%). Weighting the reclaim by time since promotion (count / days^1 or ^2, tested separately,
  `t2.py`/`t2.out`) does the same (new 8–72%). Picking the largest holder is the better trade.
- **Four tiers with a Senior tier** (R) don't help: Senior is a seniority test the attacker passes by waiting,
  and max-min inside Senior is diluted by identity count exactly like Earned (180-day pre-aged N=10000: 5–26%).
- **Promotion rate limits** keep fresh identities in Newcomer, where they crowd honest newcomers to 0–23%; with no
  pre-emption in Earned there's nothing to protect by holding them back.

## E7. Floors (Known/Earned/Newcomer) × newcomer expiry × Earned days on the recommended design

Design ("P"): S2 floors, no pre-emption inside Earned, a Newcomer tier below its floor reclaims from the largest Earned holder, max-min inside Newcomer and Known. Attack cells: established mean % (continuous) · new-before-attack mean % · joined-during-attack mean % · all-honest recovery after a 1-day attack.

### Load 2%

| policy | fresh N=100 | fresh N=10000 | pre N=100 | pre N=10000 |
|---|---|---|---|---|
| P 30/65/5 4d days3 | 100·100·100·0h | 94·99·100·0h | 100·100·100·0h | 94·99·100·0h |
| P 30/65/5 4d days5 | 100·100·100·0h | 95·97·97·31h | 100·100·100·0h | 94·99·100·0h |
| P 30/65/5 4.5d days3 | 100·100·100·0h | 94·99·100·0h | 100·100·100·0h | 94·99·100·0h |
| P 30/65/5 4.5d days5 | 100·100·100·0h | 95·97·97·31h | 100·100·100·0h | 94·99·100·0h |
| P 30/60/10 4d days3 | 100·100·100·0h | 94·99·100·0h | 100·100·100·0h | 94·99·100·0h |
| P 30/60/10 4d days5 | 100·100·100·0h | 95·97·97·31h | 100·100·100·0h | 94·99·100·0h |
| P 30/60/10 4.5d days3 | 100·100·100·0h | 94·99·100·0h | 100·100·100·0h | 94·99·100·0h |
| P 30/60/10 4.5d days5 | 100·100·100·0h | 95·97·97·31h | 100·100·100·0h | 94·99·100·0h |
| P 20/60/20 4d days3 | 100·100·100·0h | 94·99·100·0h | 100·100·100·0h | 94·99·100·0h |
| P 20/60/20 4d days5 | 100·100·100·0h | 95·97·97·31h | 100·100·100·0h | 94·99·100·0h |
| P 20/60/20 4.5d days3 | 100·100·100·0h | 94·99·100·0h | 100·100·100·0h | 94·99·100·0h |
| P 20/60/20 4.5d days5 | 100·100·100·0h | 95·97·97·31h | 100·100·100·0h | 94·99·100·0h |

### Load 50%

| policy | fresh N=100 | fresh N=10000 | pre N=100 | pre N=10000 |
|---|---|---|---|---|
| P 30/65/5 4d days3 | 95·100·99·0h | 94·93·90·12h | 94·100·99·0h | 94·100·99·0h |
| P 30/65/5 4d days5 | 96·98·96·31h | 94·95·83·2.9d | 94·100·98·0h | 94·100·98·0h |
| P 30/65/5 4.5d days3 | 94·100·99·0h | 94·93·90·12h | 94·100·99·0h | 94·100·99·0h |
| P 30/65/5 4.5d days5 | 96·98·96·31h | 94·95·83·2.9d | 94·100·98·0h | 94·100·98·0h |
| P 30/60/10 4d days3 | 94·99·100·0h | 94·93·91·12h | 94·99·100·0h | 94·99·100·0h |
| P 30/60/10 4d days5 | 95·98·97·31h | 94·95·85·2.9d | 94·99·100·0h | 94·99·100·0h |
| P 30/60/10 4.5d days3 | 94·99·100·0h | 94·93·91·12h | 94·99·100·0h | 94·99·100·0h |
| P 30/60/10 4.5d days5 | 95·98·97·31h | 94·95·85·2.9d | 94·99·100·0h | 94·99·100·0h |
| P 20/60/20 4d days3 | 94·99·100·0h | 94·93·91·12h | 94·99·100·0h | 94·99·100·0h |
| P 20/60/20 4d days5 | 95·98·97·31h | 94·95·85·2.9d | 94·99·100·0h | 94·99·100·0h |
| P 20/60/20 4.5d days3 | 94·99·100·0h | 94·93·91·12h | 94·99·100·0h | 94·99·100·0h |
| P 20/60/20 4.5d days5 | 95·98·97·31h | 94·95·85·2.9d | 94·99·100·0h | 94·99·100·0h |

### Load 90%

| policy | fresh N=100 | fresh N=10000 | pre N=100 | pre N=10000 |
|---|---|---|---|---|
| P 30/65/5 4d days3 | 90·79·90·7h | 34·31·72·4.7d | 94·85·95·0h | 94·85·95·0h |
| P 30/65/5 4d days5 | 95·38·84·31h | 41·29·68·4.7d | 95·78·95·7.7d | 95·78·95·7.7d |
| P 30/65/5 4.5d days3 | 90·79·90·7h | 34·29·72·4.9d | 94·85·95·0h | 94·85·95·0h |
| P 30/65/5 4.5d days5 | 95·38·84·31h | 41·29·68·4.9d | 95·77·95·7.7d | 95·77·95·7.7d |
| P 30/60/10 4d days3 | 84·88·97·4.7d | 69·61·88·4.7d | 92·99·99·0h | 92·99·99·0h |
| P 30/60/10 4d days5 | 89·34·92·4.7d | 74·28·79·4.7d | 93·96·99·0h | 93·96·99·0h |
| P 30/60/10 4.5d days3 | 84·88·97·5.4d | 69·64·88·5.4d | 92·99·99·0h | 92·99·99·0h |
| P 30/60/10 4.5d days5 | 89·35·92·5.4d | 74·28·79·5.4d | 93·96·99·0h | 93·96·99·0h |
| P 20/60/20 4d days3 | 74·75·100·5.5d | 74·67·88·5.5d | 92·99·100·0h | 92·99·100·0h |
| P 20/60/20 4d days5 | 78·37·97·5.5d | 78·30·81·5.5d | 92·99·100·0h | 92·99·100·0h |
| P 20/60/20 4.5d days3 | 74·75·100·5.5d | 74·67·88·5.5d | 92·99·100·0h | 92·99·100·0h |
| P 20/60/20 4.5d days5 | 78·37·97·5.5d | 78·30·81·5.5d | 92·99·100·0h | 92·99·100·0h |

### Peacetime (seed 1): established % · newly joined % at 90% and 110% load; mirror join: mirror after 1 d % · at end % · others %

| policy | 90% | 110% | mirror |
|---|---|---|---|
| P 30/65/5 4d days3 | 98·94 | 81·77 | 99·100·100 |
| P 30/65/5 4d days5 | 98·91 | 84·64 | 99·100·100 |
| P 30/65/5 4.5d days3 | 98·93 | 81·77 | 99·100·100 |
| P 30/65/5 4.5d days5 | 98·91 | 84·63 | 99·100·100 |
| P 30/60/10 4d days3 | 97·99 | 78·94 | 99·100·100 |
| P 30/60/10 4d days5 | 97·97 | 79·90 | 99·100·100 |
| P 30/60/10 4.5d days3 | 97·99 | 78·94 | 99·100·100 |
| P 30/60/10 4.5d days5 | 97·97 | 79·90 | 99·100·100 |
| P 20/60/20 4d days3 | 96·100 | 78·95 | 99·100·100 |
| P 20/60/20 4d days5 | 96·100 | 78·96 | 99·100·100 |
| P 20/60/20 4.5d days3 | 96·100 | 78·95 | 99·100·100 |
| P 20/60/20 4.5d days5 | 96·100 | 78·96 | 99·100·100 |

**E7 reading.** On the no-pre-emption design:
- **Newcomer floor 10%** is the balance point. 5% is *worse* for established under a fresh N=10000 flood on a
  90%-full store (34% vs 69%: newcomer identities are promoted with fewer records but the floor refills sooner,
  more waves) and squeezes honest newcomers in an overloaded peacetime store (77% vs 94% newly joined at 110%);
  20% costs established 10 points at 90% load under a fresh N=100 flood (74% vs 84%) and gains 5 at N=10000. The Known/Earned split (30/60 vs 20/60) makes
  no measurable difference: Known never reaches its floor in these workloads.
- **Earned after 3 distinct days**, not 5: 5 days keeps honest publishers that joined in the week before an attack
  in Newcomer, where they compete with the flood (90% load: 28–38% vs 61–88%), and buys little for established.
- **Newcomer expiry 4 d vs 4.5 d**: identical except recovery after a fresh 1-day flood on a 90%-full store
  (4.7 d vs 5.4 d). 4.5 d adds a day of margin against slow-node republish delays (E4).
- At 2–50% load established sit at 94% under the largest floods, not 100%: honest churn. Records of departing
  publishers expire and the attacker (refreshing at max rate) takes the freed Earned space; publishers joining
  during the attack then reclaim their Newcomer floor from the largest Earned holders (mirrors). The drain rate
  is the honest churn rate (1%/day here), not the attacker's size.

## E5. Attacker identities × time to push established honest records below 90% / 50%

Continuous attack, 28 days. Cell: time from attack start until established honest presence (record-weighted) < 90% / < 50%, "never" = not within 28 days; then (mean % over the attack). Pre-aged d = identities first seen d days before the attack and already qualifying as Earned/senior (with a promotion limit, only as many as the limit let through in d days start Earned; the rest queue).

### Load 2%, fresh

| policy | N=10 | N=100 | N=1000 | N=10000 |
|---|---|---|---|---|
| A today | never/never (100) | never/never (100) | never/never (100) | never/never (100) |
| C S2 | never/never (100) | never/never (100) | 2.0d/never (84) | 2.0d/2.0d (31) |
| D sen | never/never (100) | never/never (100) | never/never (100) | never/never (100) |
| D promo+w365 | never/never (100) | never/never (100) | never/never (100) | never/never (100) |
| D nopre | never/never (100) | never/never (100) | 16.8d/never (93) | 16.8d/never (91) |
| N nopre+newest | never/never (100) | never/never (100) | never/never (100) | never/never (100) |

### Load 2%, pre-aged 7 d

| policy | N=10 | N=100 | N=1000 | N=10000 |
|---|---|---|---|---|
| A today | never/never (100) | never/never (100) | never/never (100) | never/never (100) |
| C S2 | never/never (100) | never/never (100) | 0h/never (82) | 0h/0h (26) |
| D sen | never/never (100) | never/never (100) | never/never (100) | never/never (100) |
| D promo+w365 | never/never (100) | never/never (100) | never/never (100) | never/never (100) |
| D nopre | never/never (100) | never/never (100) | 16.8d/never (91) | 16.8d/never (90) |
| N nopre+newest | never/never (100) | never/never (100) | never/never (100) | never/never (100) |

### Load 2%, pre-aged 30 d

| policy | N=10 | N=100 | N=1000 | N=10000 |
|---|---|---|---|---|
| A today | never/never (100) | never/never (100) | never/never (98) | never/never (97) |
| C S2 | never/never (100) | never/never (100) | 0h/never (82) | 0h/0h (26) |
| D sen | never/never (97) | never/never (97) | never/never (97) | never/never (97) |
| D promo+w365 | never/never (100) | never/never (100) | never/never (100) | never/never (99) |
| D nopre | never/never (100) | never/never (100) | 16.8d/never (91) | 16.8d/never (90) |
| N nopre+newest | never/never (99) | never/never (99) | never/never (99) | never/never (99) |

### Load 2%, pre-aged 180 d

| policy | N=10 | N=100 | N=1000 | N=10000 |
|---|---|---|---|---|
| A today | never/never (100) | 0h/never (99) | 0h/never (75) | 0h/never (70) |
| C S2 | never/never (100) | never/never (100) | 0h/never (82) | 0h/0h (26) |
| D sen | 0h/never (65) | 0h/never (65) | 0h/never (65) | 0h/never (65) |
| D promo+w365 | never/never (100) | never/never (100) | 0h/never (85) | 2h/2h (36) |
| D nopre | never/never (100) | never/never (100) | 16.8d/never (91) | 16.8d/never (90) |
| N nopre+newest | never/never (98) | never/never (98) | never/never (98) | never/never (98) |

### Load 90%, fresh

| policy | N=10 | N=100 | N=1000 | N=10000 |
|---|---|---|---|---|
| A today | 0h/0h (37) | 0h/0h (37) | 0h/0h (37) | 0h/0h (37) |
| C S2 | 2.0d/never (69) | 2.0d/2.0d (35) | 2.0d/2.0d (19) | 2.0d/2.0d (11) |
| D sen | never/never (100) | never/never (100) | never/never (100) | never/never (100) |
| D promo+w365 | never/never (98) | 4.3d/never (81) | 4.4d/16.0d (61) | 4.6d/16.3d (61) |
| D nopre | 12.4d/never (86) | 7.5d/never (86) | 8.0d/never (86) | 2.0d/never (69) |
| N nopre+newest | never/never (100) | never/never (100) | never/never (100) | never/never (100) |

### Load 90%, pre-aged 7 d

| policy | N=10 | N=100 | N=1000 | N=10000 |
|---|---|---|---|---|
| A today | 0h/0h (37) | 0h/0h (37) | 0h/0h (37) | 0h/0h (37) |
| C S2 | 0h/never (67) | 0h/0h (30) | 0h/0h (14) | 0h/0h (5) |
| D sen | never/never (100) | never/never (100) | never/never (100) | never/never (100) |
| D promo+w365 | never/never (96) | 0h/never (74) | 0h/8.4d (43) | 4h/8.6d (44) |
| D nopre | 16.8d/never (90) | 16.8d/never (90) | 16.8d/never (90) | 16.8d/never (90) |
| N nopre+newest | never/never (100) | never/never (100) | never/never (100) | never/never (100) |

### Load 90%, pre-aged 30 d

| policy | N=10 | N=100 | N=1000 | N=10000 |
|---|---|---|---|---|
| A today | 0h/0h (37) | 0h/0h (36) | 0h/0h (36) | 0h/0h (35) |
| C S2 | 0h/never (67) | 0h/0h (30) | 0h/0h (14) | 0h/0h (5) |
| D sen | never/never (97) | never/never (97) | never/never (97) | never/never (97) |
| D promo+w365 | 16.8d/never (92) | 0h/never (58) | 0h/0h (26) | 1h/6h (24) |
| D nopre | 16.8d/never (90) | 16.8d/never (90) | 16.8d/never (90) | 16.8d/never (90) |
| N nopre+newest | never/never (99) | never/never (99) | never/never (99) | never/never (99) |

### Load 90%, pre-aged 180 d

| policy | N=10 | N=100 | N=1000 | N=10000 |
|---|---|---|---|---|
| A today | 0h/0h (37) | 0h/0h (28) | 0h/0h (25) | 0h/0h (23) |
| C S2 | 0h/never (67) | 0h/0h (30) | 0h/0h (14) | 0h/0h (5) |
| D sen | 0h/never (65) | 0h/never (65) | 0h/never (65) | 0h/never (65) |
| D promo+w365 | 0h/never (74) | 0h/0h (33) | 0h/0h (15) | 0h/1h (7) |
| D nopre | 16.8d/never (90) | 16.8d/never (90) | 16.8d/never (90) | 16.8d/never (90) |
| N nopre+newest | never/never (99) | never/never (99) | never/never (99) | never/never (99) |

**E5 reading.** (A today at 90% load shows "0h" because its per-publisher cap already holds established at 37%
before any attack.) With no pre-emption inside Earned (D nopre), no attacker size or pre-age pushes established
honest records below 50% within 28 days; they cross 90% only through the churn drain (day 16.8 at 1%/day
churn) or, on a 90%-full store, through the Newcomer-floor waves of a fresh flood (day 2–12). Seniority (D sen)
and the newest-first reclaim stay at 100% against identities younger than the honest publishers but drop to 65%
against 180-day pre-ageing (sen) or cost recently joined publishers (newest, E6). Max-min Earned (C S2) falls
below 50% at day 0–2 for N ≥ 100–10000.

## Store writes

Writes = record inserts + evictions + expiries per second; refreshes (last-stored updates) separately because they
can be coalesced. Honest-only peacetime: ~0.02 writes/s and ~0.74 refreshes/s per 1000 publishers (≈ records/day
÷ 86,400). Under a max-rate flood (continuous, D nopre): 2.1 writes/s at 2% load (attacker filling a 3.8M-record
budget once), 0.03–0.33 at 50–90%; C S2 and today's policy churn more (up to 60–100 writes/s at 50–90% load with
N=1000–10000, because fair-share and pool evictions trade records back and forth), IPFS-like 50–2,100 writes/s
(the whole budget rewritten every hour). The attacker's own refreshes are what dominate: C records per refresh
interval (here 1,029/s at 2% load with hourly refreshes). An attacker can always force writes at its request rate
(withdraw + re-store), so SD-card safety needs local write controls independent of the eviction policy: keep
last-stored times in memory and persist a record's refresh only when it moved by more than ~1 h (TTL is 7 days),
batch inserts/deletes in one transaction every ~30 s (roadmap), and refuse rather than evict when a refused store
would cost a write (no-pre-emption refusals write nothing).

## Recommendation

**Tiers, applied only when the budget is full** (anyone stores freely while there is room):

| Tier | Who | Floor | Records expire | Eviction inside the tier |
|---|---|---|---|---|
| Known | contacts, private-mode members, seeded bootstrap nodes | 30% | 7 d (PROVIDER_TTL) | max-min (largest holder first) |
| Earned | held a record and refreshed it on ≥ 3 distinct UTC days | 60% | 7 d | **none: Earned publishers never evict each other** |
| Newcomer | everyone else | 10% | 4.5 d (4 d minimum) | max-min; a publisher already holding as many as the largest is refused |

- A store by tier t into a full budget reclaims, in order: (1) the lowest tier below t that is above its floor
  (its largest holder); (2) if t is below its own floor, a tier above t that is above its floor (its largest
  holder) — floors are guaranteed both ways ("S2"); (3) otherwise only within t by that tier's rule. A lower tier
  never takes a higher tier's records below that tier's floor.
- Earned: count a day only when the publisher holds ≥ 1 record and refreshes it; keep the count with the
  persisted first-seen time. On promotion its records switch to the 7-day TTL.
- No admission rule, promotion rate limit, newcomer cap or newcomer rate limit: none improved any honest class
  and each cost honest newcomers or joining mirrors (E0, E3, E6).
- Keep the per-key limit of 1000 publishers, giving way lowest tier first (not simulated here).

**What a pre-aged attacker can still do**: with no pre-emption inside Earned, any number of identities of any
age can only (a) take free space, (b) take the Newcomer floor (once per promotion wave on a nearly full store),
and (c) absorb space freed by honest churn while refreshing at max rate — new honest publishers then reclaim their
Newcomer floor from the largest Earned holders, so large established publishers lose roughly churn × attack
length (~6–10% over 3 weeks at 1%/day). It blocks *growth*: an established publisher adding new keys while the
attacker holds Earned can only displace Newcomer records above the floor. Newcomers arriving during a flood share
the Newcomer tier with every fresh identity (68–100% here, worse for large newcomers). Recovery after the flood
ends: immediate for pre-aged floods on stores below ~85% honest load; up to the Newcomer expiry (4.5–5.5 d) for
fresh floods on a 90%-full store; ~7–9 d for anything the attacker got into Earned.

## Caveats

- One server, publisher-granular model; per-key limits, lookup/eclipse effects and network-wide placement are
  not simulated. Attack identities are symmetric and refresh hourly at "max rate" (the whole budget per hour);
  a slower attacker only does less.
- Honest workload is synthetic (heavy-tailed sizes, 30% slow nodes, 1%/day churn, established publishers 10–365 d
  old); the churn drain scales with the real churn rate.
- "Earned" here is purely a refresh-days test. The roadmap's local score (verified blocks, useful /find answers)
  would be a stronger promotion criterion but was not modelled.
- Honest publishers that go offline longer than (Newcomer expiry − 72 h) lose their Newcomer records and
  re-store at their next round; Earned/Known publishers keep the 7-day TTL.
- Single seed for most attack cells (3 seeds in peacetime); differences under ~3 points are noise.
