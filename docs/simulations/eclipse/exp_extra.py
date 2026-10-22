"""Exp D (multi-key vs a split budget), E (parameter sweeps), F (hearing fraction h=0.5)."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run

D = {"dens": 2.0, "dens_mode": "pool", "ring": "both", "dbeta": 5}
SWEEP_BEH = {
    "k15": {"kstore": 15}, "k30": {"kstore": 30}, "s15": {"sgoal": 15}, "s30": {"sgoal": 30},
    "k15+s15": {"kstore": 15, "sgoal": 15}, "k30+s30": {"kstore": 30, "sgoal": 30},
    "dens_w5": dict(D, wide=5), "dens_w20": dict(D, wide=20), "dens_b3": dict(D, dbeta=3),
    "dens_b8": dict(D, dbeta=8), "dens_T3": dict(D, dens=3.0), "dens_rank": dict(D, ring="rank"),
    "dens_radius": dict(D, ring="radius"),
    "est_tab5": {"est_table": 5}, "dens+disj2": dict(D, disj=2), "dens+est_hrd+disj2": dict(D, est="heard", disj=2),
    "dens+multi2": dict(D, multi=2),
}
SWEEP_DEF = {
    "k15": ("k15", "base", True), "k30": ("k30", "base", True), "s15": ("base", "s15", True),
    "s30": ("base", "s30", True), "k15+s15": ("k15", "s15", True), "k30+s30": ("k30", "s30", True),
    "dens_w5": ("dens_w5", "dens_w5", True), "dens_w20": ("dens_w20", "dens_w20", True),
    "dens_b3": ("dens_b3", "dens_b3", True), "dens_b8": ("dens_b8", "dens_b8", True),
    "dens_T3": ("dens_T3", "dens_T3", True), "dens_rank": ("dens_rank", "dens_rank", True),
    "dens_radius": ("dens_radius", "dens_radius", True),
    "est_tab5": ("est_tab5", "est_tab5", True), "dens+disj2": ("dens+disj2", "dens+disj2", True),
    "dens+est_hrd+disj2": ("dens+est_hrd+disj2", "dens+est_hrd+disj2", True),
    "dens+multi2": ("dens+multi2", "dens+multi2", True),
}
BASIC = ["base", "dens", "k20", "s2k", "k20+s2k", "disj2", "disj3", "disj4", "est_tab", "dens+est_hrd", "dens+disj3"]


def sweep(alpha=3):
    cfgs = []
    for S in (1000, 10000):
        cfgs.append(dict(exp="sweep", S=S, m=0, placement="tight", timing="fresh", alpha=alpha, T=2.0,
                         searchers=6, f_s=0.75, defences=BASIC + list(SWEEP_DEF), extra_beh=SWEEP_BEH,
                         extra_def=SWEEP_DEF, n_inst=1500))
        for m in (20, 50):
            for pl, tm in (("tight", "preaged"), ("ring", "preaged"), ("evasive", "preaged"), ("tight", "fresh")):
                cfgs.append(dict(exp="sweep", S=S, m=m, placement=pl, timing=tm, alpha=alpha, T=2.0, searchers=6,
                                 f_s=0.75, defences=BASIC + list(SWEEP_DEF), extra_beh=SWEEP_BEH,
                                 extra_def=SWEEP_DEF, n_inst=1500))
    return cfgs


def split():
    cfgs = []
    for S in (1000, 10000):
        for m in (20, 50):
            for pl, tm in (("tight", "preaged"), ("tight", "fresh"), ("ring", "preaged")):
                for r in (2, 3):
                    defs = ["base", "dens", f"multi{r}", "k20+s2k", "dens+disj3"] + (["dens+multi3"] if r == 3 else ["dens+multi2"])
                    cfgs.append(dict(exp="split", S=S, m=m, placement=pl, timing=tm, alpha=3, T=2.0, searchers=6,
                                     f_s=0.75, split=r, defences=defs, extra_beh=SWEEP_BEH, extra_def=SWEEP_DEF,
                                     n_inst=1500))
    return cfgs


def hearing():
    cfgs = []
    defs = ["base", "dens", "est_tab", "est_hrd", "disj3", "disj4", "k20+s2k", "dens+est_hrd", "dens+disj3",
            "dens+est_hrd+disj3", "srv100", "srv100+est_hrd"]
    for S in (1000, 10000):
        cfgs.append(dict(exp="hear", S=S, m=0, placement="tight", timing="fresh", alpha=3, T=2.0, searchers=6,
                         f_s=0.75, h=0.5, defences=defs, n_inst=1000))
        for m in (20, 50):
            for pl, tm in (("tight", "preaged"), ("tight", "fresh"), ("evasive", "preaged")):
                cfgs.append(dict(exp="hear", S=S, m=m, placement=pl, timing=tm, alpha=3, T=2.0, searchers=6,
                                 f_s=0.75, h=0.5, defences=defs, n_inst=1000))
    return cfgs


if __name__ == "__main__":
    which = sys.argv[1]
    if which == "sweep":
        run.run_all(sweep(), 1500, os.path.join(run.RESULTS, "sweep.jsonl"))
    elif which == "sweep1":
        run.run_all(sweep(1), 1000, os.path.join(run.RESULTS, "sweep_a1.jsonl"))
    elif which == "split":
        run.run_all(split(), 1500, os.path.join(run.RESULTS, "split.jsonl"))
    elif which == "hear":
        run.run_all(hearing(), 1000, os.path.join(run.RESULTS, "hear.jsonl"))
