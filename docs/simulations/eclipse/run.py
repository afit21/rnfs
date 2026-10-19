"""Experiments: per config, many (network, key) instances; one honest publisher per instance and several honest
searchers. Every publisher behaviour and every searcher behaviour runs on the same instance, so pairs
(old/new publisher x old/new searcher) are compared on identical networks."""
import json
import math
import multiprocessing as mp
import os
import random
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import sim  # noqa: E402

RESULTS = os.path.join(HERE, "results")


def behaviours(T=3.0, dens_mode="pool", ring="both", dbeta=5):
    d = {"dens": T, "dens_mode": dens_mode, "ring": ring, "dbeta": dbeta}
    return {
        "base": {},
        "dens": dict(d),
        "est_tab": {"est_table": 3},
        "est_hrd": {"est": "heard"},
        "disj2": {"disj": 2},
        "disj3": {"disj": 3},
        "disj4": {"disj": 4},
        "k20": {"kstore": 20},
        "s2k": {"sgoal": 20},
        "k20+s2k": {"kstore": 20, "sgoal": 20},
        "multi2": {"multi": 2},
        "multi3": {"multi": 3},
        "dens+est_hrd": dict(d, est="heard"),
        "dens+disj3": dict(d, disj=3),
        "dens+multi3": dict(d, multi=3),
        "dens+k20": dict(d, kstore=20),
        "dens+s2k": dict(d, sgoal=20),
        "est_hrd+disj3": {"est": "heard", "disj": 3},
        "dens+est_hrd+disj3": dict(d, est="heard", disj=3),
        "srv50": {"srv": 0.5},
        "srv100": {"srv": 1.0},
        "srv100+dens": dict(d, srv=1.0),
        "srv100+est_hrd": {"srv": 1.0, "est": "heard"},
    }


# defence label -> (publisher behaviour, searcher behaviour, local: has an old/new matrix)
DEFENCES = {
    "base": ("base", "base", False),
    "dens": ("dens", "dens", True),
    "est_tab": ("est_tab", "est_tab", True),
    "est_hrd": ("est_hrd", "est_hrd", True),
    "disj2": ("disj2", "disj2", True),
    "disj3": ("disj3", "disj3", True),
    "disj4": ("disj4", "disj4", True),
    "k20": ("k20", "base", True),
    "s2k": ("base", "s2k", True),
    "k20+s2k": ("k20", "s2k", True),
    "multi2": ("multi2", "multi2", True),
    "multi3": ("multi3", "multi3", True),
    "dens+est_hrd": ("dens+est_hrd", "dens+est_hrd", True),
    "dens+disj3": ("dens+disj3", "dens+disj3", True),
    "dens+multi3": ("dens+multi3", "dens+multi3", True),
    "dens+k20+s2k": ("dens+k20", "dens+s2k", True),
    "est_hrd+disj3": ("est_hrd+disj3", "est_hrd+disj3", True),
    "dens+est_hrd+disj3": ("dens+est_hrd+disj3", "dens+est_hrd+disj3", True),
    "srv50": ("srv50", "srv50", False),
    "srv100": ("srv100", "srv100", False),
    "srv100+dens": ("srv100+dens", "srv100+dens", False),
    "srv100+est_hrd": ("srv100+est_hrd", "srv100+est_hrd", False),
}


def pairs_for(defences, extra=None):
    """(label, combo, P, Q) for every reported pair: combo 'nn' (both upgraded), 'on' (old publisher, new
    searcher), 'no' (new publisher, old searcher)."""
    out = []
    table = dict(DEFENCES, **(extra or {}))
    for lab in defences:
        P, Q, local = table[lab]
        out.append((lab, "nn", P, Q))
        if local:
            out.append((lab, "on", "base", Q))
            out.append((lab, "no", P, "base"))
    return out


def run_task(task):
    cfg, net_seed, inst_seeds = task
    sim.LATENCY = cfg.get("latency", "exp")
    B = behaviours(cfg.get("T", 3.0), cfg.get("dens_mode", "pool"), cfg.get("ring", "both"), cfg.get("dbeta", 5))
    B.update(cfg.get("extra_beh", {}))
    defences = cfg["defences"]
    pairs = pairs_for(defences, cfg.get("extra_def"))
    Pn = sorted({p for _, _, p, _ in pairs})
    Qn = sorted({q for _, _, _, q in pairs})
    def mk(n):
        kw = dict(B[n])
        fb = kw.pop("fallback", None)
        return sim.Params(alpha=cfg["alpha"], fallback=mk(fb) if fb else None, **kw)

    params = {n: mk(n) for n in set(Pn) | set(Qn)}
    net = sim.Network(cfg["S"], net_seed, h=cfg.get("h", 1.0))
    agg = {"pairs": {}, "pub": {}, "srch": {}, "grind": [0.0, 0], "n_inst": 0}
    for seed in inst_seeds:
        m = cfg["m"]
        akeys = 3
        if cfg.get("split"):
            akeys = cfg["split"]
            m = m // akeys
        inst = sim.Instance(net, (net_seed, seed), m, cfg["placement"], cfg["timing"], akeys=akeys,
                            f_s=cfg.get("f_s", 1.5))
        rng = random.Random(repr((net_seed, seed, "run")))
        if inst.grind_cost:
            k0 = [c for s, c in zip(inst.sybils, inst.grind_cost) if inst.sybil_key[s] == 0]
            agg["grind"][0] += sum(k0) / max(1, len(k0))
            agg["grind"][1] += 1
        pub = rng.choice(net.ids)
        others = [x for x in rng.sample(net.ids, min(len(net.ids), cfg["searchers"] + 1)) if x != pub]
        searchers = others[:cfg["searchers"]]
        P = {}
        for n in Pn:
            holders, finds, provides, trig = sim.publish(inst, pub, params[n], random.Random(rng.random()))
            P[n] = holders
            a = agg["pub"].setdefault(n, [0, 0, 0, 0])
            a[0] += 1
            a[1] += finds
            a[2] += provides
            a[3] += trig
        Sres = {n: [] for n in Qn}
        for s in searchers:
            for n in Qn:
                r = sim.search(inst, s, params[n], random.Random(rng.random()))
                Sres[n].append(r)
                a = agg["srch"].setdefault(n, [0, 0])
                a[0] += 1
                a[1] += r[0][2]
        for lab, combo, pn, qn in pairs:
            succ, cost = 0, 0
            for r in Sres[qn]:
                ok, q = sim.evaluate(P[pn], r)
                succ += ok
                cost += q
            ns = len(Sres[qn])
            pi = succ / ns
            a = agg["pairs"].setdefault(f"{lab}|{combo}", [0, 0, 0, 0.0, 0.0, 0])
            a[0] += 1
            a[1] += succ
            a[2] += ns
            a[3] += pi
            a[4] += pi * pi
            a[5] += cost
        agg["n_inst"] += 1
    return agg


def merge(a, b):
    if a is None:
        return b
    for k in ("pairs", "pub", "srch"):
        for key, v in b[k].items():
            if key in a[k]:
                a[k][key] = [x + y for x, y in zip(a[k][key], v)]
            else:
                a[k][key] = v
    a["grind"] = [a["grind"][0] + b["grind"][0], a["grind"][1] + b["grind"][1]]
    a["n_inst"] += b["n_inst"]
    return a


def cfg_name(cfg):
    keys = ["exp", "S", "m", "placement", "timing", "alpha", "T", "dens_mode", "split", "h", "f_s"]
    return "_".join(f"{cfg[k]}" for k in keys if k in cfg)


def tasks_for(cfg, n_inst):
    per_net = cfg.get("per_net") or max(5, min(50, cfg["S"] // 100))
    tasks = []
    seed = 0
    i = 0
    while i < n_inst:
        n = min(per_net, n_inst - i)
        tasks.append((cfg, (cfg_name(cfg), seed), list(range(i, i + n))))
        seed += 1
        i += n
    return tasks


def run_task_wrap(task):
    return task[0], run_task(task)


def run_all(cfgs, n_inst, out_file, procs=12, on_config=None):
    """Run every config not yet in out_file (one JSON line per finished config)."""
    os.makedirs(RESULTS, exist_ok=True)
    done = set()
    if os.path.exists(out_file):
        with open(out_file) as f:
            for line in f:
                done.add(json.loads(line)["name"])
    todo = [c for c in cfgs if cfg_name(c) not in done]
    tasks, remaining = [], {}
    for c in todo:
        ts = tasks_for(c, c.get("n_inst", n_inst))
        tasks.extend(ts)
        remaining[cfg_name(c)] = len(ts)
    acc = {}
    t0 = time.time()
    with mp.Pool(procs) as pool:
        for cfg, res in pool.imap_unordered(run_task_wrap, tasks):
            name = cfg_name(cfg)
            acc[name] = merge(acc.get(name), res)
            remaining[name] -= 1
            if remaining[name] == 0:
                rec = {"name": name, "cfg": {k: v for k, v in cfg.items() if k not in ("defences", "extra_beh", "extra_def")},
                       "agg": acc.pop(name)}
                with open(out_file, "a") as f:
                    f.write(json.dumps(rec) + "\n")
                print(f"[{time.time() - t0:.0f}s] done {name}", flush=True)
                if on_config:
                    on_config()


def summarize(agg, label, combo="nn"):
    a = agg["pairs"].get(f"{label}|{combo}")
    if not a:
        return None
    n_inst, succ, ns, spi, spi2, cost = a
    p = succ / ns
    mean_pi = spi / n_inst
    var = max(0.0, spi2 / n_inst - mean_pi ** 2)
    se = math.sqrt(var / n_inst) if n_inst > 1 else 0.0
    return {"p": p, "ci": 1.96 * se, "q": cost / ns}


if __name__ == "__main__":
    pass
