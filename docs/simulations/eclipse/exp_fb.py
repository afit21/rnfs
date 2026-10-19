"""Exp G: escalation. Searchers run the cheap density-checked lookup and fall back to disjoint paths (or other keys)
only when it finds nothing; publishers choose how much to spend once per republish."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run
from exp_main import V

D = {"dens": 2.0, "dens_mode": "pool", "ring": "both", "dbeta": 5}
BEH = {
    "dens+disj2": dict(D, disj=2), "dens+multi2": dict(D, multi=2),
    "dens+est_hrd+disj3": dict(D, est="heard", disj=3),
    "dens>disj2": dict(D, fallback="dens+disj2"),
    "dens>disj3": dict(D, fallback="dens+disj3"),
    "dens>disj4": dict(D, fallback="dens+disj4"),
    "dens+disj4": dict(D, disj=4),
    "dens+est_hrd>disj3": dict(D, est="heard", fallback="dens+est_hrd+disj3"),
    "dens+multi2>disj3": dict(D, multi=2, fallback="dens+disj3"),
}
DEF = {
    "Pd|Qd>d3": ("dens", "dens>disj3", True),
    "Pd3|Qd": ("dens+disj3", "dens", True),
    "Pd3|Qd>d3": ("dens+disj3", "dens>disj3", True),
    "Pd2|Qd>d2": ("dens+disj2", "dens>disj2", True),
    "Pd3|Qd>d4": ("dens+disj3", "dens>disj4", True),
    "Pd4|Qd>d4": ("dens+disj4", "dens>disj4", True),
    "Pde3|Qde>d3": ("dens+est_hrd+disj3", "dens+est_hrd>disj3", True),
    "Pdm2|Qdm2>d3": ("dens+multi2", "dens+multi2>disj3", True),
    "Pd3|Qd2": ("dens+disj3", "dens+disj2", True),
}
DEFS = ["base", "dens", "dens+disj3", "dens+multi3"] + list(DEF)


def cfgs(alpha, n_inst):
    out = []
    for S in (300, 1000, 10000):
        out.append(dict(exp="fb", S=S, m=0, placement="tight", timing="fresh", alpha=alpha, T=2.0, searchers=6,
                        f_s=0.75, defences=DEFS, extra_beh=BEH, extra_def=DEF, n_inst=n_inst))
        for m in (20, 50):
            for pl, tm in V:
                out.append(dict(exp="fb", S=S, m=m, placement=pl, timing=tm, alpha=alpha, T=2.0, searchers=6,
                                f_s=0.75, defences=DEFS, extra_beh=BEH, extra_def=DEF, n_inst=n_inst))
    return out


if __name__ == "__main__":
    a = int(sys.argv[1])
    run.run_all(cfgs(a, 1500), 1500, os.path.join(run.RESULTS, f"fb_a{a}.jsonl"))
