"""Derived analyses from the simulation outputs: foreign LoRa load, heal recovery time, PNS eclipse exposure."""
import json
import os
import random

from sim import K, World

OUT = "out"  # relative: run from this folder (docs/simulations/README.md)
DAY = 86400.0


def get(data, S, strat, scen="C"):
    return next(r for r in data if r["S"] == S and r["strat"] == strat and r["scen"] == scen)


def foreign_load():
    con = json.load(open(os.path.join(OUT, "connected.json")))
    lines = ["| S | strategy | LoRa s per backbone publish | per backbone lookup | island(30 nodes) channel use from backbone, light (10 CIDs, 10 lookups /node/day) | heavy (100 CIDs, 50 lookups) | own island publishing (10 CIDs/node/3 d) |",
             "|---|---|---|---|---|---|---|"]
    for S in (1000, 3000):
        W = World(S, 1)
        n_bb = W.n_bb
        n_isl = sum(len(m) for m in W.island_nodes)
        share = 30 / n_isl  # one island of 30 nodes
        for st in ("BASE", "PNS", "PNS50", "PRS", "REC", "REC-S"):
            r = get(con, S, st)
            bp, bl = r["bpub_air"], r["fbb_air"]
            light = n_bb * (10 * bp + 10 * bl) * share / DAY
            heavy = n_bb * (100 * bp + 50 * bl) * share / DAY
            own = 30 * 10 / 3 * r["pub_air"] / DAY
            lines.append(f"| {S} | {st} | {bp:.0f} | {bl:.1f} | {100 * light:.0f}% | {100 * heavy:.0f}% | {100 * own:.0f}% |")
    return "\n".join(lines)


def recovery():
    """Time after a heal until records published during the partition are findable from the backbone."""
    con = json.load(open(os.path.join(OUT, "connected.json")))
    lines = ["| partition length D | baseline: mean wait for next republish (slow node, 3 d interval) | heal-republish (PH): 1 key/node, 30-node island | 5 keys/node | 20 keys/node |",
             "|---|---|---|---|---|"]
    r = get(con, 1000, "REC")
    pub_air, pub_lat = r["pub_air"], r["pub_lat"]
    U = 0.5  # share of the 1 kbit/s channel the republish may use (collisions, other traffic)
    detect = 120.0  # first off-island announce through the gateway's queue (~1 per 68 s) or an answered retry

    def ph(keys_per_node, n=30):
        # paced: a node runs at most 10 lookups per round, then waits 30 s x n; channel bound over the island
        per_node = keys_per_node * pub_lat + (keys_per_node // 10) * 300
        chan = n * keys_per_node * pub_air / U
        return detect + max(per_node, chan)

    def fmt(s):
        return f"{s / 3600:.1f} h" if s < 2 * DAY else f"{s / DAY:.1f} d"

    for D_h in (1, 12, 48, 24 * 8):
        D = D_h * 3600
        R = 3 * DAY
        if D < R:
            base = R - D / 2  # published uniformly during the partition, next republish R after
        else:
            base = R / 2
        lines.append(f"| {D_h} h | {fmt(base)} | mean {fmt(ph(1) / 2)}, last {fmt(ph(1))} | mean {fmt(ph(5) / 2)}, last {fmt(ph(5))}"
                     f" | mean {fmt(ph(20) / 2)}, last {fmt(ph(20))} |")
    note = (f"per-key island publish (REC, S=1000): {pub_air:.0f} s airtime, {pub_lat:.0f} s latency; channel share U={U}; "
            f"detection {detect:.0f} s")
    return "\n".join(lines), note


def pns_exposure():
    """Attacker share of a victim's routing table: A Sybil identities 1 hop from the victim, ground into the
    victim's top buckets. Honest candidates per bucket from an S=1000 world. Churn c per day."""
    W = World(1000, 1)
    v = 5  # a backbone victim
    me = W.ids[v]
    cand = {}
    for j in range(W.S):
        if j != v:
            cand.setdefault((me ^ W.ids[j]).bit_length(), []).append(j)
    buckets = sorted(cand, reverse=True)
    lines = ["| attacker IDs per targeted bucket | days of churn (5%/day) | BASE (first heard) | PNS50 (half the slots by hops, only open slots) | PNS (all open slots by hops) | PNS displacing incumbents |",
             "|---|---|---|---|---|---|"]
    c = 0.05
    for A in (2, 5, 10, 20):
        for days in (7, 30, 90):
            tot = 0
            res = {"base": 0.0, "pns50": 0.0, "pns": 0.0, "disp": 0.0}
            for b in buckets:
                H = len(cand[b])
                slots = min(K, H + A)
                tot += slots
                hon_slots = min(K, H)
                repl = 1 - (1 - c) ** days  # fraction of initial honest incumbents replaced
                opened = hon_slots * repl
                # baseline: each opened slot goes to a random candidate not in the bucket
                pa = A / (A + max(0, H - hon_slots)) if (A + max(0, H - hon_slots)) > 0 else 0
                free = max(0, K - H)  # empty slots: attacker fills them at once in every policy
                res["base"] += min(A, free + opened * pa)
                res["pns"] += min(A, free + opened)
                res["pns50"] += min(A, free + min(opened, K / 2) + max(0, opened - K / 2) * pa)
                res["disp"] += min(A, K)
            lines.append(f"| {A} | {days} | " + " | ".join(f"{100 * res[k] / tot:.0f}%" for k in ("base", "pns50", "pns", "disp")) + " |")
    return "\n".join(lines)


if __name__ == "__main__":
    print("## foreign load\n" + foreign_load())
    t, note = recovery()
    print("\n## recovery\n" + t + "\n" + note)
    print("\n## pns exposure\n" + pns_exposure())
