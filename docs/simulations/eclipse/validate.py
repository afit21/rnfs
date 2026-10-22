"""Validate the fast model against rnfs's real RoutingTable and Lookup classes."""
import os
import random
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", ".."))  # the rnfs tree this lives in
sys.path.insert(0, HERE)  # sim.py

from rnfs.config import Config  # noqa: E402
from rnfs.dht.lookup import Lookup  # noqa: E402
from rnfs.dht.routing import RoutingTable  # noqa: E402

import sim  # noqa: E402


def b(x: int) -> bytes:
    return x.to_bytes(16, "big")


def i(x: bytes) -> int:
    return int.from_bytes(x, "big")


def check_tables(inst, n=40, keys=20):
    """Real RoutingTable fed the same entries gives the same /find node lists."""
    rng = random.Random(5)
    bad = 0
    nodes = rng.sample(inst.net.ids, n) + [s for s in inst.sybils][:5]
    for x in nodes:
        rt = RoutingTable(b(x), Config(), clock=lambda: 1000.0)
        for e in inst.table(x):
            assert rt.heard(b(e)), "entry refused: bucket model wrong"
        for _ in range(keys):
            key = rng.choice(inst.keys) if rng.random() < .5 else rng.getrandbits(128)
            real = [i(z) for z in rt.closest(b(key), sim.K, for_reply=True)]
            mine = inst.closest_in_table(x, key, sim.K)
            bad += real != mine
    return bad


def real_lookup(inst, key, observer, alpha, holders=None, publish=False):
    """Run rnfs's Lookup over the model network. Returns (queries, result)."""
    count = [0]

    def query(nb, kb):
        count[0] += 1
        if sim.LATENCY == "exp":
            time.sleep(random.expovariate(1 / 0.004))
        n = i(nb)
        nodes = inst.reply(n, i(kb), 0.0)
        provs = [b(holders[0])] if (holders and n in holders[1] and not inst.is_sybil(n)) else []
        return provs, [b(z) for z in nodes]

    start = [b(z) for z in inst.closest_in_table(observer, key, sim.K)]
    lk = Lookup(b(key), start, query, b(observer), alpha=alpha, beta=3, timeout=30, poll_interval=0.01)
    if publish:
        res = [i(z) for z in lk.find_closest()]
    else:
        res = lk.find_providers(lambda p: None)
    return count[0], res


def compare(S, m, placement, alpha, n_inst=40, n_search=10, seed=0):
    net = sim.Network(S, seed)
    rq, fq, rfound, ffound, rpub, fpub = [], [], 0, 0, [], []
    tot = 0
    for k in range(n_inst):
        inst = sim.Instance(net, (seed, k), m, placement, "fresh")
        rng = random.Random(k)
        pub = rng.choice(net.ids)
        # real publish
        q, closest = real_lookup(inst, inst.key0, pub, alpha, publish=True)
        rpub.append(q)
        holders_real = {n for n in closest if not inst.is_sybil(n)} | {pub}
        # fast publish
        fh, ff, fp, _ = sim.publish(inst, pub, sim.Params(alpha=alpha), rng)
        fpub.append(ff)
        searchers = rng.sample([x for x in net.ids if x != pub], n_search)
        for s in searchers:
            q, provs = real_lookup(inst, inst.key0, s, alpha, holders=(pub, holders_real))
            rq.append(q)
            rfound += bool(provs)
            sres = sim.search(inst, s, sim.Params(alpha=alpha), rng)
            ok, qq = sim.evaluate(fh, sres)
            fq.append(qq)
            ffound += ok
            tot += 1
    return dict(S=S, m=m, placement=placement, alpha=alpha,
                real_q=statistics.mean(rq), fast_q=statistics.mean(fq),
                real_q_p95=sorted(rq)[int(.95 * len(rq))], fast_q_p95=sorted(fq)[int(.95 * len(fq))],
                real_pub_q=statistics.mean(rpub), fast_pub_q=statistics.mean(fpub),
                real_find=rfound / tot, fast_find=ffound / tot, n=tot)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        sim.LATENCY = sys.argv[1]
    t0 = time.time()
    net = sim.Network(1000, 1)
    for placement, m in (("tight", 0), ("tight", 20), ("ring", 20), ("evasive", 20)):
        for timing in ("fresh", "preaged"):
            inst = sim.Instance(net, ("v", placement, timing), m, placement, timing)
            print("tables", placement, m, timing, "mismatches:", check_tables(inst), flush=True)
    rows = []
    for S, m, placement, alpha in ((2000, 0, "tight", 3), (2000, 10, "tight", 3), (2000, 20, "ring", 3),
                                   (1000, 10, "evasive", 3), (1000, 0, "tight", 1), (1000, 10, "ring", 1)):
        r = compare(S, m, placement, alpha)
        rows.append(r)
        print(r, f"{time.time() - t0:.0f}s", flush=True)
