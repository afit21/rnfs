"""Experiments for the topology study. Usage: python experiments.py <experiment> [trials]"""
import json
import multiprocessing as mp
import os
import random
import sys
import time
from collections import defaultdict

from sim import K, Sim, Stats, World, true_closest

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")
os.makedirs(OUT, exist_ok=True)

STRATS = {
    "BASE": {},
    "PNS": {"pns": 1.0},
    "PNS50": {"pns": 0.5},
    "PA1": {"pa": 1},
    "PA2": {"pa": 2},
    "UB": {"ub": (10, 3)},
    "LF2L": {"lf": 3, "kloc": 5},
    "LF2L-h3": {"lf": 3, "kloc": 5, "nloc": 3},
    "LF2L-h2": {"lf": 3, "kloc": 5, "nloc": 2},
    "LF2L-k3": {"lf": 2, "kloc": 3},
    "LF2L-k10": {"lf": 3, "kloc": 10},
    "PH": {"ph": True},
    "PHp": {"ph": True, "ph_passive": True},
    "LF2L+PH": {"lf": 3, "kloc": 5, "ph": True},
    "PROXY": {"proxy": True},
    "PRS": {"prs": 3},
    "PNS+PRS": {"pns": 1.0, "prs": 3},
    "LF2L-m1": {"lf": 1, "kloc": 5},
    "SCOPE": {"scope": True, "lf": 1, "kloc": 5},
    "SCOPE+PH": {"scope": True, "lf": 1, "kloc": 5, "ph": True},
    "LF2L-m1+PH": {"lf": 1, "kloc": 5, "ph": True},
    "LF-h4": {"lf": 1, "kloc": 5, "nloc": 4},
    "LF-h3": {"lf": 1, "kloc": 5, "nloc": 3},
    "LF-h2": {"lf": 1, "kloc": 5, "nloc": 2},
    "LF-h3-m3": {"lf": 3, "kloc": 5, "nloc": 3},
    "LF-k1": {"lf": 1, "kloc": 1},
    "LF-k3": {"lf": 1, "kloc": 3},
    "LF-k10": {"lf": 1, "kloc": 10},
    "REC": {"pns": 0.5, "ub": (10, 3), "lf": 1, "kloc": 5, "ph": True},
    "REC-S": {"pns": 0.5, "ub": (10, 3), "lf": 1, "kloc": 5, "ph": True, "scope": True},
    "UB+PRS": {"ub": (10, 3), "prs": 3},
    "REC+PRS": {"pns": 0.5, "ub": (10, 3), "lf": 1, "kloc": 5, "ph": True, "prs": 3},
    "REC+PROXY": {"pns": 0.5, "ub": (10, 3), "lf": 1, "kloc": 5, "ph": True, "proxy": True},
}

_WORLDS = {}


def world(S, seed, **kw):
    key = (S, seed, tuple(sorted(kw.items())))
    w = _WORLDS.get(key)
    if w is None:
        if len(_WORLDS) > 3:
            _WORLDS.clear()
        w = _WORLDS[key] = World(S, seed, **kw)
    return w


def pick(W, rng):
    sizes = [len(m) for m in W.island_nodes]
    ii = rng.choices(range(W.n_islands), weights=sizes)[0]
    members = [x for x in W.island_nodes[ii] if x != W.gw[ii]]
    pub, srch = rng.sample(members, 2)
    bb = rng.randrange(W.n_bb)
    bb2 = rng.randrange(W.n_bb)
    key = rng.getrandbits(128)
    return ii, pub, srch, bb, bb2, key


def reset(W):
    W.part = set()
    W.late = False
    W.late_tables = False


def run_fetch(sim, who, key, t, prefix, out):
    st = Stats()
    found, lat, why = sim.fetch_lookup(who, key, t, st)
    out[prefix + "found"] = 1 if found else 0
    out[prefix + "air"] = st.air
    out[prefix + "lat"] = lat
    out[prefix + "finds"] = st.finds
    out[prefix + "preqs"] = st.preqs
    out[prefix + "links"] = st.links
    out[prefix + "to"] = st.timeouts
    out[prefix + "local"] = 1 if why == "local" else 0
    return found


def run_publish(sim, who, key, t, prefix, out, among=None):
    st = Stats()
    lat, stored, targets = sim.publish(who, key, t, st)
    W = sim.W
    tc = set(true_closest(W, key, among))
    out[prefix + "air"] = st.air
    out[prefix + "lat"] = lat
    out[prefix + "acc"] = len(set(stored) & tc)
    out[prefix + "stored"] = len(stored)
    out[prefix + "preqs"] = st.preqs
    out[prefix + "finds"] = st.finds
    out[prefix + "to"] = st.timeouts
    return lat


def trial(W, strat, upg, scen, seed, token=None):
    rng = random.Random(seed)
    ii, pub, srch, bb, bb2, key = pick(W, rng)
    reset(W)
    sim = Sim(W, upg, strat, seed)
    sim.token = token
    out = {}
    T = 0.0
    island = W.island_nodes[ii]
    reach_island = set(island)
    if scen == "C":
        T += run_publish(sim, pub, key, T, "pub_", out)
        T += 600
        run_fetch(sim, srch, key, T, "fi_", out)
        run_fetch(sim, bb, key, T, "fb_", out)
        # backbone publish, backbone fetch: LoRa airtime caused by backbone-only content
        key2 = rng.getrandbits(128)
        run_publish(sim, bb2, key2, T, "bpub_", out)
        run_fetch(sim, bb, key2, T + 600, "fbb_", out)
        # backbone publish, island fetch
        run_fetch(sim, srch, key2, T + 600, "fib_", out)
    elif scen in ("PB_early", "PB_late"):
        T += run_publish(sim, pub, key, T, "pub_", out)
        W.part = {ii}
        if scen == "PB_late":
            W.late = True
            W.late_tables = True
            T += 18 * 3600
        T += 600
        run_fetch(sim, srch, key, T, "fi_", out)
    elif scen in ("PD_early", "PD_late"):
        W.part = {ii}
        if scen == "PD_late":
            W.late = True
            W.late_tables = True
            T += 18 * 3600
        T += run_publish(sim, pub, key, T, "pub_", out, among=island)
        T += 600
        run_fetch(sim, srch, key, T, "fi_", out)
    elif scen in ("H_early", "H_late"):
        W.part = {ii}
        if scen == "H_late":
            W.late = True
            W.late_tables = True
        T += run_publish(sim, pub, key, T, "pub_", out, among=island)
        reset(W)  # heal
        T += 3600
        run_fetch(sim, srch, key, T, "fi_", out)
        run_fetch(sim, bb, key, T, "fb_", out)
    reset(W)
    return out


def job(args):
    S, wseed, sname, u, scen, ntr, wkw = args
    W = world(S, wseed, **wkw)
    strat = STRATS[sname]
    urng = random.Random(wseed * 7919 + int(u * 1000))
    upg = set(i for i in range(W.S) if urng.random() < u)
    res = []
    for t in range(ntr):
        res.append(trial(W, strat, upg, scen, wseed * 100003 + t, (sname, u)))
    return (S, wseed, sname, u, scen, tuple(sorted(wkw.items()))), res


def run(jobs, procs=12):
    with mp.Pool(procs) as pool:
        out = pool.map(job, jobs, chunksize=1)
    agg = defaultdict(list)
    for key, res in out:
        S, wseed, sname, u, scen, wkw = key
        agg[(S, sname, u, scen, wkw)].extend(res)
    return agg


def summarize(rows):
    keys = sorted(set(k for r in rows for k in r))
    s = {}
    for k in keys:
        vals = [r[k] for r in rows if k in r]
        s[k] = sum(vals) / len(vals) if vals else None
    s["n"] = len(rows)
    return s


def save(name, agg):
    data = []
    for (S, sname, u, scen, wkw), rows in agg.items():
        data.append({"S": S, "strat": sname, "u": u, "scen": scen, "world": dict(wkw), **summarize(rows)})
    with open(os.path.join(OUT, name + ".json"), "w") as f:
        json.dump(data, f, indent=1)
    return data


if __name__ == "__main__":
    exp = sys.argv[1]
    ntr = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    seeds = [1, 2, 3]
    t0 = time.time()
    jobs = []
    if exp == "connected":
        for S in (300, 1000, 3000):
            for sname in ("BASE", "PNS", "PNS50", "PA1", "PA2", "UB", "PRS", "PNS+PRS", "LF2L", "LF2L-m1", "SCOPE",
                          "PROXY", "REC", "REC-S", "REC+PROXY"):
                for sd in seeds:
                    jobs.append((S, sd, sname, 1.0, "C", ntr, {}))
    elif exp == "partition":
        for S in (300, 1000, 3000):
            for scen in ("PB_early", "PD_early", "PB_late", "PD_late", "H_early", "H_late"):
                for sname in ("BASE", "PNS", "LF2L-m1", "PH", "PHp", "LF2L-m1+PH", "SCOPE+PH", "UB", "REC", "REC-S"):
                    for sd in seeds:
                        jobs.append((S, sd, sname, 1.0, scen, ntr, {}))
    elif exp == "mixed":
        for S in (1000,):
            for scen in ("C", "PB_early", "PD_early", "H_early"):
                for sname in ("REC", "REC-S", "LF2L-m1+PH", "UB", "PNS"):
                    for u in (0.0, 0.1, 0.25, 0.5, 0.75, 1.0):
                        for sd in seeds:
                            jobs.append((S, sd, sname, u, scen, ntr, {}))
    elif exp == "prs":
        for S in (1000, 3000):
            for sname in ("UB+PRS", "REC+PRS"):
                for sd in seeds:
                    jobs.append((S, sd, sname, 1.0, "C", ntr, {}))
        for scen in ("C", "PB_early"):
            for sname in ("REC+PRS",):
                for u in (0.25, 0.5, 0.75):
                    for sd in seeds:
                        jobs.append((1000, sd, sname, u, scen, ntr, {}))
    elif exp == "fsweep":
        for S in (1000,):
            for f in (1.0, 0.3, 0.1, 0.03):
                for sname in ("BASE", "PNS", "UB", "PA1", "PROXY", "REC", "REC-S"):
                    for sd in seeds:
                        jobs.append((S, sd, sname, 1.0, "C", ntr, {"f_heard": f}))
    elif exp == "local":
        for S in (1000,):
            for scen in ("C", "PB_early", "PB_late", "H_early"):
                for sname in ("LF2L-m1", "LF-h4", "LF-h3", "LF-h2", "LF-h3-m3", "LF-k1", "LF-k3", "LF-k10"):
                    for sd in seeds:
                        jobs.append((S, sd, sname, 1.0, scen, ntr, {}))
    data = save(exp, run(jobs))
    print(f"{exp}: {len(jobs)} jobs, {time.time() - t0:.0f}s")
