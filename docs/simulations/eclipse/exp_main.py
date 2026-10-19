"""Exp B/C: main grid. Density check T=2 (pool estimator, both rings, 5 ring nodes); evasive Sybils at 0.75x the
honest density (combined 1.75x, under T=2)."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run

def grid(alpha, n_inst, variants, ms=(10, 20, 50), Ss=(100, 300, 1000, 10000)):
    cfgs = []
    for S in Ss:
        cfgs.append(dict(exp="main", S=S, m=0, placement="tight", timing="fresh", alpha=alpha, T=2.0, searchers=6,
                         f_s=0.75, defences=list(run.DEFENCES), n_inst=n_inst))
        for m in ms:
            for pl, tm in variants:
                cfgs.append(dict(exp="main", S=S, m=m, placement=pl, timing=tm, alpha=alpha, T=2.0, searchers=6,
                                 f_s=0.75, defences=list(run.DEFENCES), n_inst=n_inst))
    return cfgs

V = [("tight", "fresh"), ("tight", "preaged"), ("ring", "fresh"), ("ring", "preaged"),
     ("evasive", "fresh"), ("evasive", "preaged")]
if __name__ == "__main__":
    which = sys.argv[1]
    if which == "a3":
        run.run_all(grid(3, 2500, V), 2500, os.path.join(run.RESULTS, "main_a3.jsonl"))
    elif which == "a1":
        run.run_all(grid(1, 1500, [("tight", "fresh"), ("tight", "preaged"), ("evasive", "preaged")]), 1500,
                    os.path.join(run.RESULTS, "main_a1.jsonl"))
