"""Exp A: density threshold sweep: false positives without attack, detection and findability under attack."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run

TS = (2.0, 2.5, 3.0, 4.0, 5.0)
extra_beh, extra_def = {}, {}
for T in TS:
    for mode in ("own", "pool"):
        n = f"dens_T{T}_{mode}"
        extra_beh[n] = dict(dens=T, dens_mode=mode, ring="both", dbeta=5)
        extra_def[n] = (n, n, False)
cfgs = []
for S in (100, 300, 1000, 10000):
    for m, pl in ((0, "tight"), (10, "tight"), (20, "ring"), (20, "evasive")):
        cfgs.append(dict(exp="fp", S=S, m=m, placement=pl, timing="preaged", alpha=3, searchers=6,
                         f_s=1.5, defences=["base"] + list(extra_def), extra_beh=extra_beh, extra_def=extra_def,
                         n_inst=1500 if m == 0 else 600))
if __name__ == "__main__":
    run.run_all(cfgs, 600, os.path.join(run.RESULTS, "fp.jsonl"))
