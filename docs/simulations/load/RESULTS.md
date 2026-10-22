# DHT publishing load and scaling: summary

The full tables and code of this study were lost (they lived in a temporary directory cleared by a reboot). This is
the study's own summary, kept so the roadmap's figures have a source. The lookup model was validated against
rnfs's `Lookup` on the test suite's fake network: messages per lookup within about 6% (200, 1000 and 2000
servers), and both find at least 99.9% of the true 10 closest. Idealised: complete routing tables, no churn,
failures or Sybils; airtime has no CSMA backoff, collisions or retransmissions (expect 1.2–2× more).

## Facts checked in the code

- A `/provide` holds 22 keys at the 500-byte MTU (a 499-byte packet) and 55 at most.
- Slow-node rounds are 10 consecutive keys in key order, batched only within the round; every server gets its own
  identified link per round.
- `republish_interval` must be below `PROVIDER_TTL / 2`.

## Sweep providing

Design ("C3"): sort the due keys; run a normal lookup for the first uncovered key x; the region is every key
sharing x's first p bits, p being the common prefix of x and the 10th-closest result (it always holds at least 10
servers); if the region has 2 or more due keys, send one `/find` to the known node nearest the centre of the
region's far half, and if that reply is full (10 nodes inside), ask about each half too; give every key in the
region its 10 closest from the nodes learned, then batch `/provide` per server. Accuracy: 100% (about 1.6 extra
`/find` per region); 84–94% with no extra query, 96–99.9% with one.

Lookups per round, per-key → sweep (`/find` at alpha 3 in brackets):

| Servers | n = 1k | n = 10k | n = 1M |
|---|---|---|---|
| 100 | 1k → 8 | 10k → 9 | 1M → 8 |
| 1,000 | 1k (6.6k) → 81 (659) | 10k → 82 | 1M (6.6M) → 81 (660) |
| 10,000 | 1k (8.5k) → 544 (5.1k) | 10k (84k) → 782 (7.9k) | 1M (8.4M) → 801 (8.0k) |

Sweep lookups level off at about servers ÷ 12.5. `/find` per lookup at alpha 3: 3.0, 4.8, 6.6, 8.4 for 10,
100, 1k, 10k servers (alpha 1: 3.0, 3.35, 4.0, 4.7). On fast nodes one round for 1M keys takes about 83 h today
(one node at a time) and about 1.1 h with sweep and 16 servers in parallel.

## Airtime on LoRa

At 1 kbit/s: one identified link 6.4 s of airtime, one `/find` exchange 4.0 s, a perfectly batched key 2.63 s.
Largest number of keys a slow node can keep published, sweep (today's design in brackets):

| 1 kbit/s, servers | 24 h at 5 % | 24 h at 20 % | 72 h at 5 % | 72 h at 20 % |
|---|---|---|---|---|
| 100 | 1,216 (125) | 6,143 (616) | 4,498 (448) | 19,305 (1,971) |
| 1,000 | 41 (40) | 2,312 (275) | 590 (175) | 15,360 (1,324) |
| 10,000 | 35 (35) | 152 (150) | 111 (111) | 556 (501) |

At 300 bit/s: under 20 keys at 24 h / 5 % once there are 100 or more servers. EU868's 1 % duty limit is stricter.

## Server load

Busiest server relative to the mean: 1.6 (1k servers), 1.95 (10k), 2.44 (100k), 2.96 (1M). K = 20 cuts this by
only 10–25 % while doubling every per-key cost: keep K = 10 and give servers about 2× headroom (2.5× at 100k+).
Provider store memory today: about 450–510 B of RAM per record, 117 B in SQLite.

## Providing files inside folders

Keys per node grow 3–5× (folders of ~7 files), ~120× (~250) or ~1,300× (~2,460). Off on slow nodes is confirmed;
fast nodes need a cap on entry keys (e.g. 100k).

## Other bottlenecks

- Announces every 6 h: 0.46/s and 0.68 kbit/s on every backbone link at 10k nodes, 4.6/s and 6.8 kbit/s at
  100k, approaching RNS's ingress-control threshold. LoRa's 2 % announce cap carries about 1,150 announces a day at
  1 kbit/s: enough for about 290 rnfs nodes.
- Path requests cost 2.6 s each at 1 kbit/s; withdrawals are not paced on slow nodes (unpinning 1,000 keys: about
  2.9 h of airtime at 1k servers).
