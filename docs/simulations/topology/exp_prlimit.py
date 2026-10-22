"""Path requests under RNS 1.5.6's ingress limit, with and without rnfs's planned pacing (RESULTS.md section 11).

Usage, from this folder:
  python3 exp_prlimit.py ops [trials] [seeds]        per-operation path requests, limit trips and outcomes
  python3 exp_prlimit.py gateway [seeds]             a gateway's LoRa interface under island-wide load
  python3 exp_prlimit.py hub [seeds]                 a public hub's interfaces while fast nodes join it
  python3 exp_prlimit.py partition [trials] [seeds]  failed links, RNS's re-requests and partition mode
  python3 exp_prlimit.py perkey [trials] [seeds]     an island publisher's sweep round against one lookup per key
  python3 exp_prlimit.py tables                      print the tables from out/prlimit_*.json
Seeds are world seeds 1..seeds (default 5); trials per world seed (default 60). Raw output: out/prlimit_<exp>.json.
"""
import json
import multiprocessing as mp
import os
import random
import statistics
import sys
import time
from collections import defaultdict

from prsim import Engine, OpStats
from sim import Sim, World, true_closest  # noqa: F401

OUT = "out"

BUILT = {"ub": (10, 3), "lf": 1, "kloc": 3, "ph": True, "prs": 3}  # 0.2.0 as built (no PNS, no SCOPE)
PACED = {"limit": True, "pacing": 1.0, "path_fail_partition": False}
MODES = {
    "OFF": {"limit": False},                      # the study so far: no limit (its trips are still counted)
    "RNS": {"limit": True},                       # RNS 1.5.6's limit, rnfs as built
    "PACE1": PACED,                               # + rnfs's planned pacing, 1 s
    "PACE0.5": dict(PACED, pacing=0.5),
    "PACE2": dict(PACED, pacing=2.0),
    "PACE1-ceilact": dict(PACED, ceiling=1e9),     # + wait while the own LoRa interface limits (pr_burst_active)
    "PACE1-ceilact-b": dict(PACED, ceiling=1e9, ceiling_backoff=20.0),
    "PACE1-idle": dict(PACED, known_first=False),  # pacing, but a waiting lookup idles
    "PACE1-k": dict(PACED, known_first_k=10),     # pacing; a waiting lookup asks known-path nodes among its 10 closest
    "RNS-full": {"limit": True, "relay": "full"},  # island transport nodes in RNS's default interface mode
    "PACE1-full": dict(PACED, relay="full"),
}
SIZES = (1000, 3000)
KEYS = 100           # due keys of a publish round
BOOT = 4             # bootstrap nodes (on the backbone)


_WORLDS = {}


def world(S, wseed):
    """The world of size S and seed wseed, cached per process (its routing tables are built lazily and kept)."""
    W = _WORLDS.get((S, wseed))
    if W is None:
        if len(_WORLDS) > 2:
            _WORLDS.clear()
        W = _WORLDS[(S, wseed)] = World(S, wseed)
    W.part = set()
    W.late = W.late_tables = False
    return W


def bootstrap(W):
    """The world's bootstrap nodes: BOOT backbone servers, the same for every trial."""
    return random.Random(W.order_seed).sample(range(W.n_bb), BOOT)


def acc_of(W, covered):
    """Mean over covered keys of how many of their true K closest got the record."""
    if not covered:
        return 0.0
    return sum(len(set(t) & set(true_closest(W, k))) for k, t in covered.items()) / len(covered)


def run_op(eng, gen):
    """Run one operation to its end: (value, duration)."""
    ev = eng.spawn(eng.timed(gen))
    eng.run()
    return ev.value


def fresh(eng, a, table=None):
    """a has just started for the first time: no off-island announce heard (an island node still knows its island),
    routing table `table` (default empty), and its hub's spawned interface for it is new."""
    eng.fresh[a] = set()
    eng.table_of[a] = set(table or ())
    eng.client_since[a] = eng.now


def ops_trial(args):
    """One trial of every operation (section 11.1), in one mode."""
    S, wseed, mode, t = args
    W = world(S, wseed)
    rng = random.Random(wseed * 100003 + t)
    sizes = [len(m) for m in W.island_nodes]
    ii = rng.choices(range(W.n_islands), weights=sizes)[0]
    members = [x for x in W.island_nodes[ii] if x != W.gw[ii]]
    pub, srch, jn = rng.sample(members, 3)
    boot = bootstrap(W)
    bb, bb2, fz = rng.sample([x for x in range(W.n_bb) if x not in boot], 3)
    keys = [rng.getrandbits(128) for _ in range(KEYS)]
    keys2 = [rng.getrandbits(128) for _ in range(20)]
    keys3 = [rng.getrandbits(128) for _ in range(KEYS)]
    seed = wseed * 100003 + t
    sim = Sim(W, set(range(W.S)), BUILT, seed)
    eng = Engine(sim, MODES[mode], seed)
    o = {}
    # island publisher: one sweep round (slow: at most 10 region lookups)
    st = OpStats()
    info = {}
    cov, dur = run_op(eng, eng.publish_round(pub, keys, st, out=info))
    o.update(pub_preqs=st.preqs, pub_rns=st.rns_preqs, pub_lookups=info["lookups"], pub_keys=len(cov),
             pub_acc=acc_of(W, cov), pub_dur=dur, pub_air=st.air, pub_lim=st.limited, pub_drop=st.dropped,
             pub_pfail=st.path_fails, pub_part=1 if eng.partition_hits[pub] else 0,
             pub_prl=st.preqs / max(1, info["lookups"]))
    k = rng.choice(sorted(cov))
    for who, pre in ((srch, "fi_"), (bb, "fb_")):
        st = OpStats()
        (found, lat), _ = run_op(eng, eng.search(who, k, st))
        o.update({pre + "found": 1 if found else 0, pre + "lat": lat, pre + "preqs": st.preqs, pre + "air": st.air,
                  pre + "pfail": st.path_fails})
    # an established backbone node publishes; the island searcher fetches it
    st = OpStats()
    cov2, dur = run_op(eng, eng.publish_round(bb2, keys2, st))
    o.update(bpub_preqs=st.preqs, bpub_dur=dur)
    st = OpStats()
    (found, lat), _ = run_op(eng, eng.search(srch, rng.choice(sorted(cov2)), st))
    o.update(fib_found=1 if found else 0, fib_lat=lat, fib_preqs=st.preqs)
    # an island node starting for the first time joins
    fresh(eng, jn, [x for x in W.island_nodes[ii] if x != jn])
    st = OpStats()
    _, dur = run_op(eng, eng.join(jn, st, boot))
    o.update(sjoin_preqs=st.preqs, sjoin_dur=dur, sjoin_pfail=st.path_fails)
    # a fast node starting for the first time joins its hub, then runs its first publish round
    fresh(eng, fz)
    st = OpStats()
    _, dur = run_op(eng, eng.join(fz, st, boot))
    o.update(fjoin_preqs=st.preqs, fjoin_dur=dur, fjoin_drop=st.dropped)
    st = OpStats()
    info = {}
    cov3, dur = run_op(eng, eng.publish_round(fz, keys3, st, out=info))
    f = eng.ifaces[("hubcli", W.hub[fz], fz)]
    o.update(fpub_preqs=st.preqs, fpub_lookups=info["lookups"], fpub_acc=acc_of(W, cov3), fpub_dur=dur,
             fpub_lim=st.limited, fpub_drop=st.dropped, fpub_pfail=st.path_fails, fpub_trips=len(f.trips),
             fpub_maxf=f.max_freq, fpub_run3=1 if eng.run3[fz] else 0)
    g = eng.ifaces.get(("gwlora", ii))
    o.update(gw_trips=len(g.trips) if g else 0, gw_maxf=g.max_freq if g else 0.0)
    return (S, mode), wseed, o


def gateway_job(args):
    """An hour-scale island workload on every island of a world (section 11.2). scen "steady": every island server
    runs a publish round every 12 h and 10 searches a day; "cold": every island server starts for the first time within
    10 minutes (joins, then publishes its first round)."""
    S, wseed, mode, scen, ls, gw_new = args
    W = world(S, wseed)
    cfg = dict(MODES[mode], lora_scale=ls, gw_new=gw_new)
    sim = Sim(W, set(range(W.S)), BUILT, wseed)
    eng = Engine(sim, cfg, wseed)
    rng = random.Random(wseed * 31 + int(ls * 100))
    boot = bootstrap(W)
    T = 24 * 3600.0 if scen == "steady" else 3 * 3600.0
    published = []
    counts = defaultdict(int)

    def node(x, ii):
        if scen == "cold":
            fresh(eng, x, [y for y in W.island_nodes[ii] if y != x])
            yield rng.uniform(0, 600)
            yield from eng.join(x, OpStats(), boot)
            st = OpStats()
            cov = yield from eng.publish_round(x, [rng.getrandbits(128) for _ in range(KEYS)], st)
            published.extend(cov)
            counts["rounds"] += 1
            return
        t_round = rng.uniform(0, 12 * 3600)
        t_search = rng.expovariate(10 / 86400)
        while min(t_round, t_search) < T:
            if t_round <= t_search:
                yield t_round - eng.now
                st = OpStats()
                cov = yield from eng.publish_round(x, [rng.getrandbits(128) for _ in range(KEYS)], st)
                published.extend(cov)
                counts["rounds"] += 1
                t_round = max(eng.now, t_round + 12 * 3600)
            else:
                yield max(0.0, t_search - eng.now)
                k = rng.choice(published) if published else rng.getrandbits(128)
                yield from eng.search(x, k, OpStats())
                counts["searches"] += 1
                t_search = eng.now + rng.expovariate(10 / 86400)

    for ii, members in enumerate(W.island_nodes):
        for x in members:
            if x != W.gw[ii]:
                eng.spawn(node(x, ii))
    eng.run(until=T)
    hours = T / 3600
    rows = []
    for ii in range(W.n_islands):
        f = eng.ifaces.get(("gwlora", ii))
        if f is None:
            continue
        f.jobs_until(T)
        rows.append({"trips_h": len(f.trips) / hours, "active_s_h": f.active_seconds(0, T) / hours,
                     "prs_h": f.received / hours, "limited": f.limited / max(1, f.received), "maxf": f.max_freq,
                     "size": len(W.island_nodes[ii])})
    hub_rows = []
    for key, f in eng.ifaces.items():
        if key[0] == "hubcli" and key[2] in W.gw:
            hub_rows.append(len(f.trips) / hours)
    return (S, mode, scen, ls, gw_new), wseed, {"islands": rows, "gw_hub_trips_h": hub_rows, **counts}


def hub_job(args):
    """Fast nodes starting for the first time join one public hub (hub 0) at `rate` an hour for 2 h; each joins and
    runs its first publish round of KEYS keys (section 11.3)."""
    S, wseed, mode, rate, nkeys, service = args
    W = world(S, wseed)
    sim = Sim(W, set(range(W.S)), BUILT, wseed)
    eng = Engine(sim, dict(MODES[mode], hub_service=service), wseed)
    rng = random.Random(wseed * 17 + rate)
    boot = bootstrap(W)
    clients = [x for x in range(W.n_bb) if W.hub[x] == 0 and x not in boot]
    rng.shuffle(clients)
    T = 2 * 3600.0
    t = rng.expovariate(rate / 3600)
    joins = []
    while t < T and clients:
        joins.append((t, clients.pop()))
        t += rng.expovariate(rate / 3600)
    res = []

    def node(t0, x):
        yield t0
        fresh(eng, x)
        st = OpStats()
        r = {"t0": t0}
        yield from eng.join(x, st, boot)
        cov, dur = yield from eng.timed(eng.publish_round(x, [rng.getrandbits(128) for _ in range(nkeys)], st))
        r.update(preqs=st.preqs, limited=st.limited, dropped=st.dropped, pfail=st.path_fails, acc=acc_of(W, cov),
                 dur=dur, run3=1 if eng.run3[x] else 0)
        res.append((x, r))

    for t0, x in joins:
        eng.spawn(node(t0, x))
    eng.run(until=T + 3600)
    out = []
    for x, r in res:
        f = eng.ifaces.get(("hubcli", 0, x))
        if f:
            f.jobs_until(r["t0"] + 3600)  # the job passes after its last request, which release the limit
        r["trips"] = len(f.trips) if f else 0
        r["active_s"] = f.active_seconds(r["t0"], r["t0"] + 3600) if f else 0.0
        r["maxf"] = f.max_freq if f else 0.0
        out.append(r)
    parent = eng.ifaces.get(("hub", 0))
    if parent:
        parent.jobs_until(T + 3600)
    q = eng.queues.get(("hub", 0))
    return (S, mode, rate, nkeys, service), wseed, {"joins": out, "parent_trips_h": (len(parent.trips) if parent else 0) / 2,
                                           "il_dropped": q.dropped[1] if q else 0, "n_clients": len(clients) + len(out)}


def part_trial(args):
    """The island's gateway goes down (section 2's *early* scenarios) with RNS's path expiry and re-requests after
    failed links (section 11.4). PD: an island node publishes a round during the partition. OFF: a neighbour then
    searches for backbone content (published before the partition, so not on the island): the search can only fail,
    and how soon the node enters partition mode depends on which failures count."""
    S, wseed, mode, scen, t = args
    W = world(S, wseed)
    rng = random.Random(wseed * 100003 + t)
    sizes = [len(m) for m in W.island_nodes]
    ii = rng.choices(range(W.n_islands), weights=sizes)[0]
    members = [x for x in W.island_nodes[ii] if x != W.gw[ii]]
    pub, srch = rng.sample(members, 2)
    bb = rng.randrange(W.n_bb)
    seed = wseed * 100003 + t
    sim = Sim(W, set(range(W.S)), BUILT, seed)
    eng = Engine(sim, MODES[mode], seed)
    o = {}
    if scen == "PD_early":
        W.part = {ii}
        st = OpStats()
        t0 = eng.now
        cov, dur = run_op(eng, eng.publish_round(pub, [rng.getrandbits(128) for _ in range(KEYS)], st))
        o.update(preqs=st.preqs, rns=st.rns_preqs, dur=dur, pfail=st.path_fails, lfail=st.link_fails,
                 part=1 if pub in eng.part_at else 0, part_s=eng.part_at.get(pub, t0 + dur) - t0,
                 keys=len(cov))
    else:
        cov, _ = run_op(eng, eng.publish_round(bb, [rng.getrandbits(128) for _ in range(20)], OpStats()))
        W.part = {ii}
        eng.now += 600
        t0 = eng.now
        st = OpStats()
        (found, lat), dur = run_op(eng, eng.search(srch, rng.choice(sorted(cov)), st))
        o.update(found=1 if found else 0, dur=dur, preqs=st.preqs, rns=st.rns_preqs, pfail=st.path_fails,
                 lfail=st.link_fails, part=1 if srch in eng.part_at else 0,
                 part_s=eng.part_at.get(srch, t0 + dur) - t0)
    W.part = set()
    return (S, mode, scen), wseed, o


def perkey_trial(args):
    """An island publisher's sweep round (KEYS due keys, 10 region lookups) against looking up 10 of the keys one at a
    time, each its own round, in the same engine without the limit (section 11.3, point 6)."""
    S, wseed, t = args
    W = world(S, wseed)
    rng = random.Random(wseed * 100003 + t)
    sizes = [len(m) for m in W.island_nodes]
    ii = rng.choices(range(W.n_islands), weights=sizes)[0]
    pub = rng.choice([x for x in W.island_nodes[ii] if x != W.gw[ii]])
    keys = [rng.getrandbits(128) for _ in range(KEYS)]
    seed = wseed * 100003 + t
    eng = Engine(Sim(W, set(range(W.S)), BUILT, seed), MODES["OFF"], seed)
    cov, _ = run_op(eng, eng.publish_round(pub, keys, OpStats()))
    eng2 = Engine(Sim(W, set(range(W.S)), BUILT, seed), MODES["OFF"], seed)
    single = {}
    for k in keys[:10]:
        c, _ = run_op(eng2, eng2.publish_round(pub, [k], OpStats()))
        single.update(c)
    return (S,), wseed, {"sweep_keys": len(cov), "sweep_acc": acc_of(W, cov), "perkey_acc": acc_of(W, single)}


# Aggregation

def aggregate(results):
    """{key: {metric: (mean, min seed mean, max seed mean, p10, p90)}} from [(key, seed, row)]."""
    by = defaultdict(lambda: defaultdict(list))
    for key, wseed, row in results:
        by[key][wseed].append(row)
    agg = {}
    for key, seeds in by.items():
        metrics = sorted(set(m for rows in seeds.values() for r in rows for m in r))
        a = {}
        for m in metrics:
            allv = [r[m] for rows in seeds.values() for r in rows if m in r]
            means = [statistics.fmean([r[m] for r in rows if m in r]) for rows in seeds.values()
                     if any(m in r for r in rows)]
            qs = statistics.quantiles(allv, n=10) if len(allv) > 1 else [allv[0]] * 9
            a[m] = (statistics.fmean(allv), min(means), max(means), qs[0], qs[-1])
        a["n"] = sum(len(rows) for rows in seeds.values())
        agg[key] = a
    return agg


def save(name, data):
    """Write out/prlimit_<name>.json."""
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, f"prlimit_{name}.json"), "w") as f:
        json.dump(data, f, indent=1)


def load(name):
    """Read out/prlimit_<name>.json."""
    with open(os.path.join(OUT, f"prlimit_{name}.json")) as f:
        return json.load(f)


def fmt(v, pct=False, digits=None):
    """A number for a table cell."""
    if pct:
        return f"{100 * v:.0f}%"
    if digits is not None:
        return f"{v:.{digits}f}"
    return f"{v:.0f}" if abs(v) >= 100 else f"{v:.1f}"


def cell(a, m, pct=False, spread=True, digits=None):
    """mean (min-max of seed means)."""
    if m not in a:
        return "-"
    mean, lo, hi, _, _ = a[m]
    s = fmt(mean, pct, digits)
    if spread and abs(hi - lo) > 1e-9:
        s += f" ({fmt(lo, pct, digits)}–{fmt(hi, pct, digits)})"
    return s


def tables():
    """Print every table of section 11 from out/."""
    if os.path.exists(os.path.join(OUT, "prlimit_ops.json")):
        ops = {tuple(json.loads(k)): v for k, v in load("ops").items()}
        print("\n### ops: island publisher, one sweep round (100 due keys)\n")
        print("| S | mode | region lookups | keys covered | path requests (rnfs) | per region lookup | RNS re-requests | limited | "
              "dropped | path failures | placement (of 10) | round s | LoRa s | partition mode entered |")
        print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        for (S, mode), a in sorted(ops.items()):
            print(f"| {S} | {mode} | {cell(a, 'pub_lookups')} | {cell(a, 'pub_keys')} | {cell(a, 'pub_preqs')} | {cell(a, 'pub_prl')} | "
                  f"{cell(a, 'pub_rns')} | {cell(a, 'pub_lim')} | {cell(a, 'pub_drop')} | {cell(a, 'pub_pfail')} | "
                  f"{cell(a, 'pub_acc')} | {cell(a, 'pub_dur')} | {cell(a, 'pub_air')} | {cell(a, 'pub_part', True)} |")
        print("\n### ops: searches and joins\n")
        print("| S | mode | island searcher: found / s / path requests | backbone searcher: found / s / path requests | "
              "island fetches backbone content: found / s | backbone publisher's path requests | island node's first "
              "join: path requests / path failures | fast node's first join: path requests |")
        print("|---|---|---|---|---|---|---|---|")
        for (S, mode), a in sorted(ops.items()):
            print(f"| {S} | {mode} | {cell(a, 'fi_found', True, False)} / {cell(a, 'fi_lat', spread=False)} / "
                  f"{cell(a, 'fi_preqs', spread=False)} | {cell(a, 'fb_found', True, False)} / "
                  f"{cell(a, 'fb_lat', spread=False)} / {cell(a, 'fb_preqs', spread=False)} | "
                  f"{cell(a, 'fib_found', True, False)} / {cell(a, 'fib_lat', spread=False)} | "
                  f"{cell(a, 'bpub_preqs', spread=False)} | {cell(a, 'sjoin_preqs', spread=False)} / "
                  f"{cell(a, 'sjoin_pfail', spread=False)} | {cell(a, 'fjoin_preqs', spread=False)} |")
        print("\n### ops: a fast node's first publish round after joining a hub (100 due keys)\n")
        print("| S | mode | region lookups | path requests | limited | dropped | path failures | trips of its hub "
              "interface | peak estimate /s | placement (of 10) | round s | 3 failures in a row |")
        print("|---|---|---|---|---|---|---|---|---|---|---|---|")
        for (S, mode), a in sorted(ops.items()):
            print(f"| {S} | {mode} | {cell(a, 'fpub_lookups')} | {cell(a, 'fpub_preqs')} | {cell(a, 'fpub_lim')} | "
                  f"{cell(a, 'fpub_drop')} | {cell(a, 'fpub_pfail')} | {cell(a, 'fpub_trips')} | "
                  f"{cell(a, 'fpub_maxf', spread=False)} | {cell(a, 'fpub_acc')} | {cell(a, 'fpub_dur')} | "
                  f"{cell(a, 'fpub_run3', True)} |")
    if os.path.exists(os.path.join(OUT, "prlimit_gateway.json")):
        gw = load("gateway")
        print("\n### gateway: the gateway's LoRa interface under island-wide load\n")
        print("| S | mode | load | LoRa | gateway interface | path requests an hour at the gateway | trips an hour "
              "(mean, max island) | limited seconds an hour | limited share | peak estimate /s |")
        print("|---|---|---|---|---|---|---|---|---|---|")
        for r in gw:
            isl = r["islands"]
            if not isl:
                continue
            S, mode, scen, ls, gw_new = r["key"]
            tr = [x["trips_h"] for x in isl]
            print(f"| {S} | {mode} | {scen} | {'1 kbit/s' if ls == 1.0 else f'{1 / ls:.1f}x faster'} | "
                  f"{'new (3/s)' if gw_new else 'over 2 h (8/s)'} | "
                  f"{statistics.fmean(x['prs_h'] for x in isl):.0f} | {statistics.fmean(tr):.2f} ({max(tr):.1f}) | "
                  f"{statistics.fmean(x['active_s_h'] for x in isl):.0f} | "
                  f"{100 * statistics.fmean(x['limited'] for x in isl):.1f}% | {max(x['maxf'] for x in isl):.2f} |")
    if os.path.exists(os.path.join(OUT, "prlimit_hub.json")):
        hub = load("hub")
        print("\n### hub: fast nodes joining a public hub for the first time\n")
        print("| S | mode | joins an hour | keys | hub ms a packet | path requests (join + first round) | limited | dropped | trips of "
              "its interface in its first hour | limited seconds in its first hour | peak estimate /s | placement | "
              "round s | 3 failures in a row | hub's ingress-limited queue drops | parent interface trips an hour |")
        print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        for r in hub:
            S, mode, rate, nkeys, service = r["key"]
            js = [j for s in r["seeds"] for j in s["joins"]]
            if not js:
                continue

            def m(f):
                return statistics.fmean(j[f] for j in js)
            print(f"| {S} | {mode} | {rate} | {nkeys} | {1000 * service:g} | {m('preqs'):.0f} | {m('limited'):.0f} | {m('dropped'):.1f} | "
                  f"{m('trips'):.1f} | {m('active_s'):.0f} | {max(j['maxf'] for j in js):.1f} | {m('acc'):.2f} | "
                  f"{m('dur'):.0f} | {100 * m('run3'):.0f}% | "
                  f"{statistics.fmean(s['il_dropped'] for s in r['seeds']):.0f} | "
                  f"{statistics.fmean(s['parent_trips_h'] for s in r['seeds']):.1f} |")
    if os.path.exists(os.path.join(OUT, "prlimit_partition.json")):
        part = {tuple(json.loads(k)): v for k, v in load("partition").items()}
        print("\n### partition: the gateway is down\n")
        print("| S | scenario | mode | path requests (rnfs + RNS re-requests) | path failures | link failures | "
              "entered partition mode | s until it did (or the operation ended) | operation s |")
        print("|---|---|---|---|---|---|---|---|---|")
        for (S, mode, scen), a in sorted(part.items(), key=lambda kv: (kv[0][0], kv[0][2], kv[0][1])):
            print(f"| {S} | {scen} | {mode} | {cell(a, 'preqs', spread=False)} + {cell(a, 'rns', spread=False)} | "
                  f"{cell(a, 'pfail', spread=False)} | {cell(a, 'lfail', spread=False)} | {cell(a, 'part', True)} | "
                  f"{cell(a, 'part_s')} | {cell(a, 'dur')} |")


def perkey_table():
    """Print the perkey table, if it was run."""
    if os.path.exists(os.path.join(OUT, "prlimit_perkey.json")):
        pk = {tuple(json.loads(k)): v for k, v in load("perkey").items()}
        print("\n### perkey: an island publisher, sweep regions against one lookup per key\n")
        print("| S | keys a round of 10 region lookups covers | their placement (of 10) | one lookup per key: placement |")
        print("|---|---|---|---|")
        for (S,), a in sorted(pk.items()):
            print(f"| {S} | {cell(a, 'sweep_keys')} | {cell(a, 'sweep_acc')} | {cell(a, 'perkey_acc')} |")


def pool_map(fn, jobs, procs):
    """Run jobs over processes."""
    with mp.Pool(procs) as pool:
        return pool.map(fn, jobs, chunksize=1)


if __name__ == "__main__":
    exp = sys.argv[1]
    procs = int(os.environ.get("PROCS", "8"))
    t0 = time.time()
    if exp == "ops":
        trials = int(sys.argv[2]) if len(sys.argv) > 2 else 60
        seeds = int(sys.argv[3]) if len(sys.argv) > 3 else 5
        modes = os.environ.get("MODES", "OFF,RNS,PACE1,PACE0.5,PACE2,PACE1-idle,PACE1-k,RNS-full,PACE1-full").split(",")
        jobs = [(S, sd, m, t) for S in SIZES for m in modes for sd in range(1, seeds + 1) for t in range(trials)]
        agg = aggregate(pool_map(ops_trial, jobs, procs))
        save("ops", {json.dumps(list(k)): v for k, v in agg.items()})
    elif exp == "gateway":
        seeds = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        jobs = []
        for sd in range(1, seeds + 1):
            for m in os.environ.get("MODES", "OFF,RNS,PACE1,PACE1-ceilact,PACE1-ceilact-b").split(","):
                for ls in (1.0, 0.18):
                    jobs.append((1000, sd, m, "cold", ls, True))
                    jobs.append((1000, sd, m, "steady", ls, False))
                    jobs.append((1000, sd, m, "steady", ls, True))
        res = pool_map(gateway_job, jobs, procs)
        by = defaultdict(lambda: {"islands": [], "gw_hub_trips_h": []})
        for key, sd, r in res:
            by[key]["islands"] += r["islands"]
            by[key]["gw_hub_trips_h"] += r["gw_hub_trips_h"]
        save("gateway", [{"key": list(k), **v} for k, v in sorted(by.items())])
    elif exp == "hub":
        seeds = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        jobs = [(1000, sd, m, rate, nk, sv) for sd in range(1, seeds + 1) for m in ("OFF", "RNS", "PACE1")
                for rate, nk, sv in ((2, 100, 0.002), (10, 100, 0.002), (10, 1000, 0.002), (30, 100, 0.002),
                                     (10, 100, 0.0005), (10, 100, 0.01))]
        res = pool_map(hub_job, jobs, procs)
        by = defaultdict(list)
        for key, sd, r in res:
            by[key].append(r)
        save("hub", [{"key": list(k), "seeds": v} for k, v in sorted(by.items())])
    elif exp == "partition":
        trials = int(sys.argv[2]) if len(sys.argv) > 2 else 60
        seeds = int(sys.argv[3]) if len(sys.argv) > 3 else 5
        jobs = [(S, sd, m, scen, t) for S in SIZES for m in ("OFF", "RNS", "PACE1")
                for scen in ("PD_early", "OFF_search") for sd in range(1, seeds + 1) for t in range(trials)]
        agg = aggregate(pool_map(part_trial, jobs, procs))
        save("partition", {json.dumps(list(k)): v for k, v in agg.items()})
    elif exp == "perkey":
        trials = int(sys.argv[2]) if len(sys.argv) > 2 else 40
        seeds = int(sys.argv[3]) if len(sys.argv) > 3 else 5
        jobs = [(S, sd, t) for S in SIZES for sd in range(1, seeds + 1) for t in range(trials)]
        agg = aggregate(pool_map(perkey_trial, jobs, procs))
        save("perkey", {json.dumps(list(k)): v for k, v in agg.items()})
    if exp != "tables":
        print(f"{exp}: {time.time() - t0:.0f} s")
    tables()
    perkey_table()
