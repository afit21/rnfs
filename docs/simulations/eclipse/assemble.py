"""RESULTS.md = parts/head.md + generated sections + parts/conclusions.md (if any)."""
import os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import report as R

def section(f):
    try:
        return f()
    except Exception as e:  # partial data
        return f"(section not ready: {e!r})"

parts = [open(os.path.join(HERE, "parts/head.md")).read()]
c = os.path.join(HERE, "parts/conclusions.md")
if os.path.exists(c):
    parts.append(open(c).read())
parts += [section(R.summary_section), section(R.upgrade_section), section(R.main_section),
          section(lambda: R.summary_section("main_a1.jsonl", 1)), section(lambda: R.main_section("main_a1.jsonl", 1))]
from exp_extra import SWEEP_DEF
parts.append(section(lambda: R.generic_section("sweep.jsonl", "Parameter sweep, alpha=3",
    ["base", "dens", "dens_T3", "dens_rank", "dens_radius", "dens_w5", "dens_w20", "dens_b3", "dens_b8",
     "k15", "k20", "k30", "s15", "s2k", "s30", "k15+s15", "k20+s2k", "k30+s30", "disj2", "disj3", "disj4",
     "est_tab", "est_tab5", "dens+disj2", "dens+disj3", "dens+est_hrd", "dens+est_hrd+disj2", "dens+multi2"])))
parts.append(section(lambda: R.generic_section("sweep_a1.jsonl", "Parameter sweep, alpha=1 (slow nodes)",
    ["base", "dens", "dens_b3", "dens_b8", "k20", "k20+s2k", "disj2", "disj3", "dens+disj2", "dens+disj3",
     "dens+est_hrd", "dens+est_hrd+disj2", "dens+multi2"])))
parts.append(section(lambda: R.fb_section(3)))
parts.append(section(lambda: R.fb_section(1)))
parts.append(section(lambda: R.split_section()))
parts.append(section(lambda: R.generic_section("hear.jsonl", "Announce hearing fraction h = 0.5",
    ["base", "dens", "est_tab", "est_hrd", "disj3", "disj4", "k20+s2k", "dens+est_hrd", "dens+disj3",
     "dens+est_hrd+disj3", "srv100", "srv100+est_hrd"],
    extra_note="Each server heard only half of the others' announces (tables and the first-seen log are "
               "built from what it heard).")))
open(os.path.join(HERE, "RESULTS.md"), "w").write("\n\n".join(p for p in parts if p))
print("assembled")
