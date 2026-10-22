"""Early-partition learning curve: island nodes keep working through a fresh partition with shared state
(failure memory, unreliable marking, partition detection). Ops alternate: each op = a random island node
fetches a random key published (before the partition, or during it) by another island node."""
import json, os, random, sys
import multiprocessing as mp
from sim import Sim, Stats, World
from experiments import STRATS

OUT = "out"  # relative: run from this folder (docs/simulations/README.md)
os.makedirs(OUT, exist_ok=True)
J = 40      # ops per partition
GAP = 120.0 # seconds between ops on the island


def run(args):
    S, seed, sname, before = args
    W = World(S, seed)
    rng = random.Random(seed * 31 + (1 if before else 0))
    res = [[] for _ in range(J)]
    for rep in range(40):
        sizes = [len(m) for m in W.island_nodes]
        ii = rng.choices(range(W.n_islands), weights=sizes)[0]
        members = [x for x in W.island_nodes[ii] if x != W.gw[ii]]
        sim = Sim(W, set(range(W.S)), STRATS[sname], seed)
        sim.token = (sname, 1.0)
        W.part = set(); W.late = False; W.late_tables = False
        keys = []
        if before:
            for k in range(10):
                pub = rng.choice(members); key = rng.getrandbits(128)
                sim.publish(pub, key, 0, Stats()); keys.append((key, pub))
        W.part = {ii}
        t = 3600.0
        for j in range(J):
            if not before and j % 4 == 0:
                pub = rng.choice(members); key = rng.getrandbits(128)
                st = Stats(); lat, _, _ = sim.publish(pub, key, t, st); keys.append((key, pub))
            key, pub = rng.choice(keys)
            who = rng.choice([m for m in members if m != pub])
            st = Stats()
            found, lat, why = sim.fetch_lookup(who, key, t, st)
            res[j].append((1 if found else 0, lat, st.air, st.timeouts))
            t += GAP + lat
        W.part = set()
    return (S, sname, before), res


if __name__ == "__main__":
    jobs = [(1000, sd, sn, b) for sd in (1, 2, 3) for sn in ("BASE", "PH", "PHp", "PNS", "LF2L-m1+PH", "REC") for b in (True, False)]
    with mp.Pool(12) as p:
        out = p.map(run, jobs)
    agg = {}
    for k, res in out:
        a = agg.setdefault(k, [[] for _ in range(J)])
        for j in range(J):
            a[j].extend(res[j])
    data = []
    for (S, sn, b), a in agg.items():
        for j in range(J):
            r = a[j]
            data.append({"S": S, "strat": sn, "before": b, "op": j + 1, "found": sum(x[0] for x in r) / len(r),
                         "lat": sum(x[1] for x in r) / len(r), "air": sum(x[2] for x in r) / len(r),
                         "to": sum(x[3] for x in r) / len(r)})
    json.dump(data, open(os.path.join(OUT, "learning.json"), "w"), indent=1)
    for b in (True, False):
        print("published", "before" if b else "during", "partition")
        print("| strat | ops 1-5 found | lat | to | ops 6-15 found | lat | to | ops 16-40 found | lat | to |")
        print("|---|---|---|---|---|---|---|---|---|---|")
        for sn in ("BASE", "PH", "PHp", "PNS", "LF2L-m1+PH", "REC"):
            cells = []
            for lo, hi in ((1, 5), (6, 15), (16, 40)):
                rows = [d for d in data if d["strat"] == sn and d["before"] == b and lo <= d["op"] <= hi]
                cells += [f"{100*sum(d['found'] for d in rows)/len(rows):.0f}%", f"{sum(d['lat'] for d in rows)/len(rows):.0f}",
                          f"{sum(d['to'] for d in rows)/len(rows):.1f}"]
            print(f"| {sn} | " + " | ".join(cells) + " |")
