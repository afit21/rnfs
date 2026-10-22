"""Build tables from results/*.jsonl."""
import json
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
R = os.path.join(HERE, "results")


def load(name):
    path = os.path.join(R, name)
    out = []
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                out.append(json.loads(line))
    return out


def pair(rec, label, combo="nn"):
    a = rec["agg"]["pairs"].get(f"{label}|{combo}")
    if not a:
        return None
    n_inst, succ, ns, spi, spi2, cost = a
    p = succ / ns
    mean_pi = spi / n_inst
    var = max(0.0, spi2 / n_inst - mean_pi ** 2)
    se = math.sqrt(var / n_inst) if n_inst > 1 else 0.0
    return {"p": p, "ci": 1.96 * se, "q": cost / ns, "n": ns, "ni": n_inst}


def pub(rec, beh):
    a = rec["agg"]["pub"].get(beh)
    if not a:
        return None
    n, finds, provides, trig = a
    return {"finds": finds / n, "provides": provides / n, "trig": trig / n}


def srch_trig(rec, beh):
    a = rec["agg"]["srch"].get(beh)
    return None if not a else a[1] / a[0]


def pct(x, ci=None):
    if x is None:
        return "-"
    s = f"{100 * x:.1f}"
    if ci is not None:
        s += f" ±{100 * ci:.1f}"
    return s


def find(recs, **kw):
    for r in recs:
        c = r["cfg"]
        if all(c.get(k) == v for k, v in kw.items()):
            return r
    return None


def md_table(header, rows):
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for r in rows:
        out.append("| " + " | ".join(str(x) for x in r) + " |")
    return "\n".join(out)


def fp_section():
    recs = load("fp.jsonl")
    if not recs:
        return ""
    Ts = (2.0, 2.5, 3.0, 4.0, 5.0)
    lines = ["## Density-check threshold sweep (exp_fp.py)", "",
             "Check: after/while a lookup runs, with N_est the size estimate, it fires if some rank j >= K among "
             "the known nodes has d_j < j * 2^128 / (T * N_est). Estimator `own`: from the node's own K closest "
             "table entries, (K-1)*2^128/d_K. `pool`: median d_K of 16 earlier lookups (random keys). "
             "Firing widens (ring 'both', 5 ring nodes must answer, K extra stores per ring).", "",
             "**False positives without attack** (% of publisher lookups / searcher lookups that fire), and extra "
             "/find per search vs baseline:", ""]
    header = ["S"] + [f"T={T} {m}" for T in Ts for m in ("own", "pool")]
    rows = []
    for S in (100, 300, 1000, 10000):
        r = find(recs, S=S, m=0)
        if not r:
            continue
        base_q = pair(r, "base")["q"]
        row = [S]
        for T in Ts:
            for m in ("own", "pool"):
                n = f"dens_T{T}_{m}"
                pb = pub(r, n)
                st = srch_trig(r, n)
                dq = pair(r, n)["q"] - base_q
                row.append(f"{100 * pb['trig']:.1f}/{100 * st:.1f} (+{dq:.2f})")
        rows.append(row)
    lines.append(md_table(header, rows))
    lines.append("")
    for (m, pl, title) in ((10, "tight", "tight m=10 (pre-aged)"), (20, "ring", "ring m=20 (pre-aged)"),
                           (20, "evasive", "evasive m=20, Sybil density 1.5x honest (pre-aged)")):
        lines.append(f"**{title}**: P(find) % (searcher-lookup detection %); baseline first")
        lines.append("")
        header = ["S", "base"] + [f"T={T} {mm}" for T in Ts for mm in ("own", "pool")]
        rows = []
        for S in (100, 300, 1000, 10000):
            r = find(recs, S=S, m=m, placement=pl)
            if not r:
                continue
            row = [S, pct(pair(r, "base")["p"])]
            for T in Ts:
                for mm in ("own", "pool"):
                    n = f"dens_T{T}_{mm}"
                    row.append(f"{pct(pair(r, n)['p'])} ({100 * srch_trig(r, n):.0f})")
            rows.append(row)
        lines.append(md_table(header, rows))
        lines.append("")
    return "\n".join(lines)




VARIANTS = [("tight", "fresh"), ("tight", "preaged"), ("ring", "fresh"), ("ring", "preaged"),
            ("evasive", "fresh"), ("evasive", "preaged")]
VSHORT = {("tight", "fresh"): "tight/fresh", ("tight", "preaged"): "tight/aged", ("ring", "fresh"): "ring/fresh",
          ("ring", "preaged"): "ring/aged", ("evasive", "fresh"): "evas/fresh", ("evasive", "preaged"): "evas/aged"}
DEF_ORDER = ["base", "dens", "est_tab", "est_hrd", "disj2", "disj3", "disj4", "k20", "s2k", "k20+s2k", "multi2",
             "multi3", "dens+est_hrd", "dens+disj3", "dens+multi3", "dens+k20+s2k", "est_hrd+disj3",
             "dens+est_hrd+disj3", "srv50", "srv100", "srv100+dens", "srv100+est_hrd"]
# publisher / searcher behaviour of each defence (mirrors run.DEFENCES)
PQ = {"k20": ("k20", "base"), "s2k": ("base", "s2k"), "k20+s2k": ("k20", "s2k"), "dens+k20+s2k": ("dens+k20", "dens+s2k")}


def pq(d):
    return PQ.get(d, (d, d))


def findability_tables(recs, S, m, with_ci=True, variants=VARIANTS):
    header = ["defence"] + [VSHORT[v] for v in variants]
    rows = []
    for d in DEF_ORDER:
        row = [d]
        any_ = False
        for pl, tm in variants:
            r = find(recs, S=S, m=m, placement=pl, timing=tm)
            x = pair(r, d) if r else None
            any_ = any_ or x is not None
            row.append("-" if x is None else pct(x["p"], x["ci"] if with_ci else None))
        if any_:
            rows.append(row)
    return md_table(header, rows)


def cost_table(recs, Ss, attacks):
    """Mean /find per search; per publish: /find + /provide."""
    header = ["defence"]
    cols = []
    for S in Ss:
        for (m, pl, tm) in attacks:
            lab = f"S={S} " + ("no attack" if m == 0 else f"m={m} {VSHORT[(pl, tm)]}")
            header.append(lab + " search")
            header.append(lab + " publish (find+provide)")
            cols.append((S, m, pl, tm))
    rows = []
    for d in DEF_ORDER:
        row = [d]
        ok = False
        for S, m, pl, tm in cols:
            r = find(recs, S=S, m=m, placement=pl, timing=tm) if m else find(recs, S=S, m=0)
            x = pair(r, d) if r else None
            P, _ = pq(d)
            pb = pub(r, P) if r else None
            if x is None or pb is None:
                row += ["-", "-"]
                continue
            ok = True
            row.append(f"{x['q']:.1f}")
            row.append(f"{pb['finds']:.1f}+{pb['provides']:.1f}")
        if ok:
            rows.append(row)
    return md_table(header, rows)


def upgrade_table(recs, S, m, defs, variants=VARIANTS):
    """P(find) for old/new publisher x old/new searcher, and the 50% mix."""
    header = ["defence", "variant", "old pub, old srch (=base)", "old pub, new srch", "new pub, old srch",
              "new pub, new srch", "50% upgraded"]
    rows = []
    for d in defs:
        for v in variants:
            r = find(recs, S=S, m=m, placement=v[0], timing=v[1])
            if not r:
                continue
            oo = pair(r, "base")["p"]
            on = pair(r, d, "on")
            no = pair(r, d, "no")
            nn = pair(r, d, "nn")
            if on is None:
                continue
            mix = (oo + on["p"] + no["p"] + nn["p"]) / 4
            rows.append([d, VSHORT[v], pct(oo), pct(on["p"]), pct(no["p"]), pct(nn["p"]), pct(mix)])
    return md_table(header, rows)


def grind_table(recs):
    header = ["S", "placement", "expected key generations per Sybil", "seconds per Sybil at 19k/s/core (measured)"]
    rows = []
    for S in (100, 300, 1000, 10000):
        for pl in ("tight", "ring", "evasive"):
            r = find(recs, S=S, m=20, placement=pl, timing="preaged")
            if not r or not r["agg"]["grind"][1]:
                continue
            g = r["agg"]["grind"][0] / r["agg"]["grind"][1]
            rows.append([S, pl, f"{g:,.0f}", f"{g / 19145:.3g}"])
    return md_table(header, rows)


def main_section(fname="main_a3.jsonl", alpha=3):
    recs = load(fname)
    if not recs:
        return ""
    out = [f"## Main grid, alpha={alpha} ({fname})", ""]
    if alpha == 3:
        out += ["### Grinding cost", "", grind_table(recs), ""]
    out += ["### P(find) %, both publisher and searcher upgraded (±95% CI)", ""]
    for S in (100, 300, 1000, 10000):
        for m in (0, 10, 20, 50):
            if m == 0:
                r = find(recs, S=S, m=0)
                if not r:
                    continue
                vals = [f"{d} {pct(pair(r, d)['p'])}" for d in DEF_ORDER if pair(r, d)]
                out += [f"**S={S}, no attack:** " + ", ".join(vals), ""]
                continue
            if not find(recs, S=S, m=m):
                continue
            out += [f"**S={S}, m={m}**", "", findability_tables(recs, S, m), ""]
    out += ["### Message cost (mean /find per search; per publish /find + /provide)", ""]
    out += [cost_table(recs, (1000, 10000), [(0, "tight", "fresh"), (20, "tight", "preaged")]), ""]
    out += [cost_table(recs, (100, 300), [(0, "tight", "fresh"), (20, "tight", "preaged")]), ""]
    return "\n".join(out)


def summary_rows(recs, defs, Ss=(1000, 10000), ms=(10, 20, 50), variants=VARIANTS, pq_map=None):
    """Per defence: mean / worst P(find) over the attack configs, gain vs base, extra messages (no attack and under
    attack) per publish+search."""
    pq_map = pq_map or {}
    rows = []
    base_stats = None
    for d in defs:
        P, Q = pq_map.get(d, pq(d))
        ps, bases, xs_q, xs_pub = [], [], [], []
        for S in Ss:
            for m in ms:
                for pl, tm in variants:
                    r = find(recs, S=S, m=m, placement=pl, timing=tm)
                    if not r or not pair(r, d):
                        continue
                    x = pair(r, d)
                    b = pair(r, "base")
                    ps.append(x["p"])
                    bases.append(b["p"])
                    pb, bb = pub(r, P), pub(r, "base")
                    xs_q.append(x["q"] - b["q"])
                    xs_pub.append(pb["finds"] + pb["provides"] - bb["finds"] - bb["provides"])
        if not ps:
            continue
        q0, p0 = [], []
        for S in Ss:
            r = find(recs, S=S, m=0)
            if r and pair(r, d):
                q0.append(pair(r, d)["q"] - pair(r, "base")["q"])
                pb, bb = pub(r, P), pub(r, "base")
                p0.append(pb["finds"] + pb["provides"] - bb["finds"] - bb["provides"])
        mean = sum(ps) / len(ps)
        gain = mean - sum(bases) / len(bases)
        rows.append(dict(d=d, mean=mean, worst=min(ps), gain=gain,
                         q0=sum(q0) / max(1, len(q0)), p0=sum(p0) / max(1, len(p0)),
                         qa=sum(xs_q) / len(xs_q), pa=sum(xs_pub) / len(xs_pub)))
    return rows


def summary_table(rows):
    header = ["defence", "mean P(find) %", "worst %", "gain pp", "+/find per search, no attack",
              "+msgs per publish, no attack", "+/find per search, attacked", "+msgs per publish, attacked",
              "gain pp per extra msg (search + publish/10, no attack)"]
    out = []
    for r in sorted(rows, key=lambda r: -r["mean"]):
        cost = r["q0"] + r["p0"] / 10
        eff = "free" if cost < 0.05 else f"{100 * r['gain'] / cost:.1f}"
        out.append([r["d"], f"{100 * r['mean']:.1f}", f"{100 * r['worst']:.1f}", f"{100 * r['gain']:+.1f}",
                    f"{r['q0']:+.2f}", f"{r['p0']:+.2f}", f"{r['qa']:+.1f}", f"{r['pa']:+.1f}", eff])
    return md_table(header, out)


def summary_section(fname="main_a3.jsonl", alpha=3):
    recs = load(fname)
    if not recs:
        return ""
    out = [f"## Summary over attacks, alpha={alpha}: S in {{1000, 10000}}, m in {{10, 20, 50}}, every attack variant run at this alpha (alpha=1 grid: tight fresh, tight pre-aged, evasive pre-aged)", "",
           "Cost of a defence = extra messages vs baseline. 'no attack' is what every lookup pays always; "
           "the efficiency column weights publish messages by 1/10 (a key is looked up roughly as often as it is "
           "republished; change the weight to taste).", ""]
    out.append(summary_table(summary_rows(recs, DEF_ORDER)))
    out.append("")
    for name, Ss in (("S=100/300", (100, 300)),):
        out += [f"Small networks ({name}):", "", summary_table(summary_rows(recs, DEF_ORDER, Ss=Ss)), ""]
    out += ["Pre-aged attacks only (the hard case), S in {1000, 10000}:", "",
            summary_table(summary_rows(recs, DEF_ORDER, variants=[v for v in VARIANTS if v[1] == "preaged"])), ""]
    return "\n".join(out)


def upgrade_section(fname="main_a3.jsonl"):
    recs = load(fname)
    if not recs:
        return ""
    defs = ["dens", "est_hrd", "disj3", "k20+s2k", "multi3", "dens+est_hrd", "dens+disj3", "dens+multi3",
            "dens+k20+s2k", "dens+est_hrd+disj3"]
    out = ["## Partial upgrade (old = 0.1.x behaviour)", "",
           "Local defences only change what the upgraded publisher/searcher does, so the mix is exact: with a "
           "fraction u of nodes upgraded, P = (1-u)^2 P(old,old) + u(1-u)(P(old,new) + P(new,old)) + u^2 P(new,new). "
           "The 50% column is that with u = 0.5.", ""]
    for S, m in ((10000, 20), (1000, 50)):
        out += [f"**S={S}, m={m}**", "", upgrade_table(recs, S, m, defs,
                                                         [("tight", "fresh"), ("tight", "preaged"), ("evasive", "preaged")]), ""]
    out += ["Server-side reply seniority (srvX: X% of servers leave recently heard nodes out of /find replies; "
            "helps old clients too):", ""]
    rows = []
    for S in (1000, 10000):
        for m in (20, 50):
            for v in VARIANTS:
                r = find(recs, S=S, m=m, placement=v[0], timing=v[1])
                if not r:
                    continue
                rows.append([S, m, VSHORT[v]] + [pct(pair(r, d)["p"]) for d in ("base", "srv50", "srv100")])
    out += [md_table(["S", "m", "variant", "0%", "50%", "100%"], rows), ""]
    return "\n".join(out)


def generic_section(fname, title, defs, Ss=(1000, 10000), ms=(20, 50), variants=None, extra_note=""):
    recs = load(fname)
    if not recs:
        return ""
    variants = variants or sorted({(r["cfg"]["placement"], r["cfg"]["timing"]) for r in recs if r["cfg"]["m"]},
                                  key=lambda v: VARIANTS.index(v))
    out = [f"## {title} ({fname})", "", extra_note, ""]
    for S in Ss:
        for m in ms:
            header = ["defence"] + [VSHORT[v] for v in variants] + ["/find per search (attacked avg)",
                                                                     "publish find+provide (attacked avg)"]
            rows = []
            for d in defs:
                row = [d]
                qs, pbs = [], []
                for v in variants:
                    r = find(recs, S=S, m=m, placement=v[0], timing=v[1])
                    x = pair(r, d) if r else None
                    row.append("-" if x is None else pct(x["p"], x["ci"]))
                    if x:
                        qs.append(x["q"])
                        P = PQX.get(d, pq(d))[0]
                        pb = pub(r, P)
                        if pb:
                            pbs.append(pb["finds"] + pb["provides"])
                row.append(f"{sum(qs) / len(qs):.1f}" if qs else "-")
                row.append(f"{sum(pbs) / len(pbs):.1f}" if pbs else "-")
                rows.append(row)
            out += [f"**S={S}, m={m}**", "", md_table(header, rows), ""]
        r0 = find(recs, S=S, m=0)
        if r0:
            rows = []
            for d in defs:
                x = pair(r0, d)
                if not x:
                    continue
                pb = pub(r0, PQX.get(d, pq(d))[0])
                rows.append([d, pct(x["p"]), f"{x['q']:.2f}", f"{pb['finds']:.1f}+{pb['provides']:.1f}" if pb else "-"])
            out += [f"S={S}, no attack:", "", md_table(["defence", "P(find)", "/find per search", "publish find+provide"], rows), ""]
    return "\n".join(out)


PQX = {"k15": ("k15", "base"), "k30": ("k30", "base"), "s15": ("base", "s15"), "s30": ("base", "s30"),
       "k15+s15": ("k15", "s15"), "k30+s30": ("k30", "s30")}


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "fp"
    if which == "fp":
        print(fp_section())
    elif which == "main":
        print(main_section())
    elif which == "main1":
        print(main_section("main_a1.jsonl", 1))
    elif which == "summary":
        print(summary_section())
    elif which == "summary1":
        print(summary_section("main_a1.jsonl", 1))
    elif which == "upgrade":
        print(upgrade_section())


def split_section():
    recs = load("split.jsonl")
    if not recs:
        return ""
    out = ["## Records under several keys vs an attacker who splits the budget (split.jsonl)", "",
           "split=r: the attacker spreads m over keys 0..r-1 (m/r each), as an attacker who knows the scheme "
           "but has a fixed budget would. (The main grid gives the attacker m on every key.) "
           "multiR searches key 0, then 1, ... until found.", ""]
    header = ["S", "m", "variant", "split r", "base", "dens", "multi r", "dens+multi r", "k20+s2k", "dens+disj3",
              "multi r /find per search", "multi r publish msgs"]
    rows = []
    for r in recs:
        c = r["cfg"]
        k = c["split"]
        mr = f"multi{k}"
        dm = "dens+multi3" if k == 3 else "dens+multi2"
        pb = pub(r, mr)
        rows.append([c["S"], c["m"], VSHORT[(c["placement"], c["timing"])], k] +
                    [pct(pair(r, d)["p"]) if pair(r, d) else "-" for d in ("base", "dens", mr, dm, "k20+s2k", "dens+disj3")] +
                    [f"{pair(r, mr)['q']:.1f}", f"{pb['finds'] + pb['provides']:.1f}"])
    rows.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
    out.append(md_table(header, rows))
    return "\n".join(out)


def fb_section(alpha):
    from exp_fb import DEF, DEFS
    fname = f"fb_a{alpha}.jsonl"
    recs = load(fname)
    if not recs:
        return ""
    pqm = {k: (v[0], v[1]) for k, v in DEF.items()}
    PQX.update(pqm)
    names = {"Pd|Qd>d3": "pub dens; search dens, on failure dens+disj3",
             "Pd3|Qd": "pub dens+disj3; search dens",
             "Pd3|Qd>d3": "pub dens+disj3; search dens, on failure dens+disj3",
             "Pd2|Qd>d2": "pub dens+disj2; search dens, on failure dens+disj2",
             "Pd3|Qd>d4": "pub dens+disj3; search dens, on failure dens+disj4",
             "Pd4|Qd>d4": "pub dens+disj4; search dens, on failure dens+disj4",
             "Pde3|Qde>d3": "pub dens+est_hrd+disj3; search dens+est_hrd, on failure +disj3",
             "Pdm2|Qdm2>d3": "pub dens+multi2; search dens on key 0, key 1, then dens+disj3",
             "Pd3|Qd2": "pub dens+disj3; search dens+disj2"}
    out = [f"## Escalation: disjoint paths only when needed, alpha={alpha} ({fname})", "",
           "Publisher pays once per republish; searcher runs the cheap density-checked lookup and escalates only "
           "when it found no provider (so searches for content nobody provides also pay the escalation: "
           "see 'no-attack miss cost').", ""]
    for d, n in names.items():
        out.append(f"* `{d}`: {n}")
    out.append("")
    for Ss, lab in (((1000, 10000), "S in {1000, 10000}"), ((300,), "S=300")):
        rows = summary_rows(recs, DEFS, Ss=Ss, ms=(20, 50), pq_map=pqm)
        out += [f"**Summary, {lab}, m in {{20, 50}}, 6 variants:**", "", summary_table(rows), ""]
    # cost of a search for absent content: first lookup + fallback, no attack
    rows = []
    for S in (300, 1000, 10000):
        r = find(recs, S=S, m=0)
        if not r:
            continue
        rows.append([S, f"{pair(r, 'dens')['q']:.1f}", f"{pair(r, 'dens+disj3')['q']:.1f}"])
    out += ["Search for content nobody provides (no attack) costs the first lookup plus the fallback: about the "
            "dens column plus the dens+disj3 column below (/find):", "",
            md_table(["S", "dens", "dens+disj3"], rows), ""]
    out += [generic_section(fname, f"Escalation detail alpha={alpha}", DEFS, Ss=(1000, 10000), ms=(20, 50))]
    return "\n".join(out)
