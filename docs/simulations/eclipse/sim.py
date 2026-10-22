"""Eclipse / key-hiding simulation for the rnfs DHT (spec section 6).

Fast model of rnfs's Kademlia: routing tables built lazily per node (bucket i = a random K-subset of the
servers in its distance range, as hearing every announce in random order gives), /find replies = the K closest
table entries, and a re-implementation of rnfs.dht.lookup.Lookup's iteration (alpha in flight within the
max(alpha, beta) closest, complete when the beta closest known answered, 8K query cap, 3*window stall rule),
extended with the local defences under study. validate.py checks it against the real Lookup/RoutingTable.

IDs are 128-bit ints; distance is XOR.
"""
import bisect
import hashlib
import heapq
import math
import random

K = 10
SPACE = 1 << 128
MAX_Q = 8 * K
STALL_WINDOWS = 3
FAR = 129
LATENCY = "exp"  # 'exp': random reply order; 'fifo': replies in send order (as threads over an in-memory net)


def bl(x: int) -> int:
    return x.bit_length()


def derived_key(key0: int, i: int) -> int:
    """H(i || cid) for i >= 1 (key 0 is the cid's usual key)."""
    if i == 0:
        return key0
    return int.from_bytes(hashlib.sha256(bytes([i]) + key0.to_bytes(16, "big")).digest()[:16], "big")


def heard(x: int, y: int, h: float, salt: int) -> bool:
    """Whether x heard y's announce (deterministic pseudo-random), for hearing fraction h."""
    if h >= 1.0:
        return True
    return (hash((x, y, salt)) & 0xFFFFFFFF) < h * 4294967296.0


class Network:
    """S honest servers with random IDs; tables built lazily."""

    def __init__(self, S: int, seed: int, h: float = 1.0):
        self.S = S
        self.seed = seed
        self.h = h
        rng = random.Random(("net", S, seed).__repr__())
        ids = set()
        while len(ids) < S:
            ids.add(rng.getrandbits(128))
        self.ids = sorted(ids)
        self.idset = ids
        self._tables: dict[int, dict[int, list[int]]] = {}
        self._dk_pool: list[int] | None = None

    def region_count(self, x: int, j: int) -> int:
        lo = ((x >> j) ^ 1) << j
        return bisect.bisect_left(self.ids, lo + (1 << j)) - bisect.bisect_left(self.ids, lo)

    def table(self, x: int) -> dict[int, list[int]]:
        """x's honest buckets {j: entries}."""
        t = self._tables.get(x)
        if t is not None:
            return t
        ids = self.ids
        rng = random.Random(hash((self.seed, x, "tbl")))
        t = {}
        for j in range(127, -1, -1):
            blk = (x >> (j + 1)) << (j + 1)
            a = bisect.bisect_left(ids, blk)
            b = bisect.bisect_left(ids, blk + (1 << (j + 1)))
            if b - a <= 1:
                break
            lo = ((x >> j) ^ 1) << j
            a = bisect.bisect_left(ids, lo)
            b = bisect.bisect_left(ids, lo + (1 << j))
            n = b - a
            if n == 0:
                continue
            if self.h >= 1.0:
                if n <= K:
                    t[j] = ids[a:b]
                else:
                    t[j] = [ids[a + i] for i in rng.sample(range(n), K)]
            else:
                chosen = []
                if n <= 4 * K:
                    order = list(range(n))
                    rng.shuffle(order)
                    for i in order:
                        if heard(x, ids[a + i], self.h, self.seed):
                            chosen.append(ids[a + i])
                            if len(chosen) == K:
                                break
                else:
                    tried = set()
                    while len(chosen) < K and len(tried) < n:
                        i = rng.randrange(n)
                        if i in tried:
                            continue
                        tried.add(i)
                        if heard(x, ids[a + i], self.h, self.seed):
                            chosen.append(ids[a + i])
                if chosen:
                    t[j] = chosen
        self._tables[x] = t
        return t

    def kth_closest(self, p: int, k: int = K) -> int:
        """Distance from p to its k-th closest honest server (p itself excluded)."""
        ids = self.ids

        def count(b):
            blk = (p >> b) << b
            return bisect.bisect_left(ids, blk + (1 << b)) - bisect.bisect_left(ids, blk)

        lo, hi = 0, 128
        while lo < hi:
            mid = (lo + hi) // 2
            if count(mid) >= k + 1:
                hi = mid
            else:
                lo = mid + 1
        blk = (p >> lo) << lo
        a = bisect.bisect_left(ids, blk)
        e = bisect.bisect_left(ids, blk + (1 << lo))
        ds = sorted(y ^ p for y in ids[a:e] if y != p)
        return ds[k - 1]

    def dk_pool(self) -> list[int]:
        """d_K at many random points: what a node learns from its past lookups (for size estimates)."""
        if self._dk_pool is None:
            rng = random.Random(hash((self.seed, "pool")))
            self._dk_pool = [self.kth_closest(rng.getrandbits(128)) for _ in range(512)]
        return self._dk_pool


class Instance:
    """One target key (and its derived keys) under attack in a network.

    placement: 'tight' (closer than every honest node), 'ring' (within the K-th closest honest distance),
    'evasive' (Poisson spacing at f_s times the honest density). timing: 'fresh' or 'preaged'.
    m: Sybils per attacked key; akeys: how many of keys 0..akeys-1 are attacked.
    """

    def __init__(self, net: Network, seed, m: int, placement: str = "tight", timing: str = "fresh",
                 akeys: int = 3, f_s: float = 1.5, nkeys: int = 3):
        self.net = net
        rng = random.Random(repr(("inst", net.seed, net.S, seed)))
        self.rng = rng
        self.key0 = rng.getrandbits(128)
        self.keys = [derived_key(self.key0, i) for i in range(nkeys)]
        self.m = m
        self.placement = placement
        self.timing = timing
        self.sybils: list[int] = []
        self.sybil_key: dict[int, int] = {}
        self.grind_cost: list[float] = []  # expected key generations per Sybil
        for i in range(min(akeys, nkeys)):
            key = self.keys[i]
            if m <= 0:
                continue
            if placement == "tight":
                D = net.kth_closest(key, 1)
                ds = [rng.randrange(1, max(2, D)) for _ in range(m)]
                costs = [SPACE / D] * m
            elif placement == "ring":
                D = net.kth_closest(key, K)
                ds = [rng.randrange(1, D) for _ in range(m)]
                costs = [SPACE / D] * m
            elif placement == "evasive":
                rate = f_s * net.S / SPACE
                ds, acc = [], 0.0
                for _ in range(m):
                    acc += rng.expovariate(1.0) / rate
                    ds.append(max(1, int(acc)))
                # grinding for "distance about d_i": ~2^128/d_i attempts (the attacker accepts any of [d_i/2, d_i])
                costs = [2 * SPACE / d for d in ds]
            else:
                raise ValueError(placement)
            for d, c in zip(ds, costs):
                s = key ^ d
                if s in net.idset or s in self.sybil_key:
                    continue
                self.sybils.append(s)
                self.sybil_key[s] = i
                self.grind_cost.append(c)
        self.sybil_set = set(self.sybils)
        self.preaged = timing == "preaged"
        self._full: dict[int, list[int]] = {}
        self._reply_cache: dict = {}

    # tables and replies

    def is_sybil(self, x: int) -> bool:
        return x in self.sybil_set

    def senior(self, x: int) -> bool:
        """Known for a long time (by anyone who heard it): honest servers, pre-aged Sybils."""
        return self.preaged or x not in self.sybil_set

    def established(self, observer: int, x: int) -> bool:
        """observer heard x's announce long ago (announce first-seen log)."""
        if x in self.sybil_set:
            return self.preaged and heard(observer, x, self.net.h, self.net.seed)
        return heard(observer, x, self.net.h, self.net.seed)

    def table(self, x: int) -> list[int]:
        """x's full routing table entries: honest buckets with the Sybils that got in."""
        t = self._full.get(x)
        if t is not None:
            return t
        base = self.net.table(x)
        if not self.sybils:
            t = [e for b in base.values() for e in b]
            self._full[x] = t
            return t
        groups: dict[int, list[int]] = {}
        for s in self.sybils:
            if s == x:
                continue
            groups.setdefault(bl(x ^ s) - 1, []).append(s)
        rng = random.Random(hash((self.net.seed, x, self.key0, "syb")))
        buckets = {j: list(v) for j, v in base.items()}
        h = self.net.h
        for j, grp in groups.items():
            if h < 1.0:
                grp = [s for s in grp if heard(x, s, h, self.net.seed)]
                if not grp:
                    continue
            rng.shuffle(grp)
            cur = buckets.get(j, [])
            if not self.preaged:
                room = K - len(cur)
                if room > 0:
                    buckets[j] = cur + grp[:room]
            else:
                nh = self.net.region_count(x, j)
                if h < 1.0:
                    nh = max(len(cur), int(round(nh * h)))
                total = nh + len(grp)
                if total <= K:
                    buckets[j] = cur + grp
                else:
                    # a random K-subset of the region's honest servers and Sybils (random hearing order)
                    pick = rng.sample(range(total), K)
                    ns = sum(1 for p in pick if p >= nh)
                    keep = rng.sample(cur, min(len(cur), K - ns))
                    buckets[j] = keep + grp[:ns]
        t = [e for b in buckets.values() for e in b]
        self._full[x] = t
        return t

    def closest_in_table(self, x: int, key: int, count: int = K, senior_only: bool = False) -> list[int]:
        t = self.table(x)
        if senior_only:
            t = [e for e in t if self.senior(e)]
        return heapq.nsmallest(count, t, key=lambda e: e ^ key)

    def reply(self, x: int, key: int, srv: float) -> list[int]:
        """Nodes in x's /find reply for key. srv: fraction of honest servers that leave fresh (non-senior)
        nodes out of replies (a local server-side defence)."""
        ck = (x, key, srv)
        r = self._reply_cache.get(ck)
        if r is not None:
            return r
        if x in self.sybil_set:
            r = heapq.nsmallest(K, (s for s in self.sybils if s != x), key=lambda e: e ^ key)
        else:
            upgraded = srv > 0 and (hash((x, self.net.seed, "srv")) & 0xFFFF) < srv * 65536
            r = self.closest_in_table(x, key, K, senior_only=upgraded and not self.preaged)
        self._reply_cache[ck] = r
        return r

    def size_estimate(self, observer: int, mode: str, rng: random.Random) -> float:
        """Network size estimate. 'own': from observer's own K closest; 'pool': median d_K of 16 past lookups."""
        if mode == "own":
            ds = sorted(e ^ observer for e in self.table(observer))
            if len(ds) < K:
                return max(1.0, float(len(ds)))
            return (K - 1) * SPACE / ds[K - 1]
        pool = self.net.dk_pool()
        sample = sorted(rng.choice(pool) for _ in range(16))
        med = (sample[7] + sample[8]) / 2
        return (K - 1 / 3) * SPACE / med


class Params:
    """Local lookup/publish behaviour of one node."""

    def __init__(self, alpha=3, beta=3, dens=None, dens_mode="pool", est=None, est_table=0, disj=1,
                 kstore=K, sgoal=0, multi=1, srv=0.0, wide=K, ring="both", dbeta=0, fallback=None):
        self.alpha = alpha
        self.beta = beta
        self.dens = dens          # density threshold T, or None
        self.dens_mode = dens_mode
        self.est = est            # 'heard': also need/store to the closest established (announce log)
        self.est_table = est_table  # also query/store to this many closest senior routing-table entries
        self.disj = disj          # disjoint paths
        self.kstore = kstore      # publisher stores to this many closest
        self.sgoal = sgoal        # searcher: this many closest must answer (0: beta)
        self.multi = multi        # keys provided/searched
        self.srv = srv            # fraction of servers with the reply-seniority defence
        self.wide = wide          # stores to the ring beyond a dense core (density check)
        self.ring = ring          # 'rank': ring after the dense core's last rank; 'radius': nodes beyond
        #                           rho = K * 2^128 / (T * N_est); 'both': both rings
        self.dbeta = dbeta or beta  # ring nodes a searcher needs answered
        self.fallback = fallback  # searcher: Params of a second search run (key 0) if this one finds nothing

    def extras(self) -> bool:
        return bool(self.dens or self.est or self.est_table or self.sgoal)


class _Path:
    __slots__ = ("known", "ranked", "queried", "answered", "inflight", "sent", "stalled", "reach",
                 "req_sig", "done", "why", "forced", "c")

    def __init__(self):
        self.known = {}
        self.ranked = []  # sorted list of (distance, id)
        self.queried = set()
        self.answered = set()
        self.inflight = 0
        self.sent = 0
        self.stalled = 0
        self.reach = FAR
        self.req_sig = None
        self.done = False
        self.why = ""
        self.forced = []
        self.c = 0

    def add(self, n, d):
        self.known[n] = d
        bisect.insort(self.ranked, (d, n))

    def remove(self, n):
        d = self.known.pop(n)
        i = bisect.bisect_left(self.ranked, (d, n))
        del self.ranked[i]


def core_rank(ranked, n_est: float, T: float) -> int:
    """Density check: the largest j >= K with d_j < j * 2^128 / (T * N_est) (0 if none): the anomalously dense
    core. Nodes ranked after it look natural."""
    c = 0
    unit = SPACE / (T * n_est)
    for j in range(K, len(ranked) + 1):
        if ranked[j - 1][0] < j * unit:
            c = j
        elif ranked[j - 1][0] > 4 * j * unit and j > 2 * K:
            break
    return c


def ring_starts(ranked, c, rho, ring):
    """Indices in ranked where the widened ring(s) start."""
    out = []
    if ring in ("rank", "both"):
        out.append(c)
    if ring in ("radius", "both"):
        out.append(bisect.bisect_left(ranked, (int(rho), -1)))
    return out


def lookup(inst: Instance, key: int, observer: int, p: Params, rng: random.Random, publish=False):
    """One (possibly multi-path) lookup. Returns dict: queries, answered (set), paths (list of _Path),
    store (list) if publish, triggered (bool)."""
    alpha, beta = p.alpha, p.beta
    window = max(alpha, beta)
    n_est = inst.size_estimate(observer, p.dens_mode, rng) if p.dens else None
    rho = K * SPACE / (p.dens * n_est) if p.dens else None
    start = inst.closest_in_table(observer, key, K)
    forced = []
    if p.est_table:
        forced = [n for n in inst.closest_in_table(observer, key, p.est_table, senior_only=True)]
    paths = [_Path() for _ in range(p.disj)]
    claimed: dict[int, int] = {}
    for i, n in enumerate(start):
        if n == observer:
            continue
        paths[i % p.disj].add(n, n ^ key)
    for i, n in enumerate(forced):
        pa = paths[i % p.disj]
        if n not in pa.known and all(n not in q.known for q in paths):
            pa.add(n, n ^ key)
        owner = next(q for q in paths if n in q.known)
        owner.forced.append(n)
    sgoal = max(beta, p.sgoal)
    events = []
    seq = 0
    t = 0.0
    total_sent = 0
    is_est = (lambda n: inst.established(observer, n)) if p.est == "heard" else None

    def required(pa: _Path):
        ranked = pa.ranked
        req = [n for _, n in ranked[:sgoal]]
        extra = []
        if is_est is not None:
            cnt = 0
            for _, n in ranked:
                if is_est(n):
                    extra.append(n)
                    cnt += 1
                    if cnt == beta:
                        break
        if n_est is not None:
            c = core_rank(ranked, n_est, p.dens)
            pa.c = c
            if c > 0:
                for i0 in ring_starts(ranked, c, rho, p.ring):
                    extra.extend(n for _, n in ranked[i0:i0 + p.dbeta])
        for n in pa.forced:
            if n in pa.known:
                extra.append(n)
        return req, extra

    def reach_of(pa):
        if len(pa.ranked) < beta:
            return FAR
        return bl(pa.ranked[beta - 1][0])

    for pa in paths:
        pa.reach = reach_of(pa)

    while True:
        active = False
        for idx, pa in enumerate(paths):
            if pa.done:
                continue
            req, extra = required(pa)
            if pa.ranked and all(n in pa.answered for n in req[:beta]) and \
                    all(n in pa.answered for n in req) and all(n in pa.answered for n in extra):
                pa.done, pa.why = True, "complete"
                continue
            if pa.stalled >= STALL_WINDOWS * window:
                pa.done, pa.why = True, "stall"
                continue
            while pa.inflight < alpha and pa.sent < MAX_Q:
                cand = [n for _, n in pa.ranked[:window]] + req + extra
                best = None
                for n in cand:
                    if n not in pa.queried and n in pa.known:
                        if best is None or pa.known[n] < pa.known[best]:
                            best = n
                if best is None:
                    break
                if claimed.get(best, idx) != idx:
                    pa.remove(best)
                    continue
                claimed[best] = idx
                pa.queried.add(best)
                pa.sent += 1
                pa.inflight += 1
                total_sent += 1
                seq += 1
                heapq.heappush(events, (t + (rng.expovariate(1.0) if LATENCY == "exp" else 1.0), seq, idx, best))
            if pa.inflight == 0:
                pa.done, pa.why = True, ("cap" if pa.sent >= MAX_Q else "exhausted")
                continue
            active = True
        if not active or not events:
            break
        t, _, idx, n = heapq.heappop(events)
        pa = paths[idx]
        pa.inflight -= 1
        if pa.done:
            continue
        pa.answered.add(n)
        before = pa.req_sig
        for m in inst.reply(n, key, p.srv):
            if m == observer or m in pa.known or m in pa.queried:
                continue
            if claimed.get(m, idx) != idx:
                continue
            pa.add(m, m ^ key)
        nearer = reach_of(pa)
        progress = nearer < pa.reach
        if progress:
            pa.reach = nearer
        if p.extras():
            req, extra = required(pa)
            sig = (tuple(req), tuple(extra))
            if sig != before:
                progress = True
            pa.req_sig = sig
        pa.stalled = 0 if progress else pa.stalled + 1
    answered = set()
    for pa in paths:
        answered |= pa.answered
    out = {"queries": total_sent, "answered": answered, "paths": paths,
           "triggered": any(pa.c > 0 for pa in paths)}
    if publish:
        store = []
        seen = set()
        inflight_unanswered = set()
        for pa in paths:
            inflight_unanswered |= pa.queried - pa.answered

        def cands(pa):
            return [n for _, n in pa.ranked if n in pa.answered or n not in pa.queried]

        for pa in paths:
            cs = cands(pa)
            chosen = cs[:p.kstore]
            if n_est is not None:
                c = core_rank(pa.ranked, n_est, p.dens)
                if c > 0:
                    for i0 in ring_starts(pa.ranked, c, rho, p.ring):
                        beyond = [n for _, n in pa.ranked[i0:] if n in pa.answered or n not in pa.queried]
                        chosen += beyond[:p.wide]
            if is_est is not None:
                chosen += [n for n in cs if is_est(n)][:K]
            chosen += [n for n in pa.forced if n in pa.known]
            for n in chosen:
                if n not in seen:
                    seen.add(n)
                    store.append(n)
        out["store"] = store
    return out


def publish(inst: Instance, publisher: int, p: Params, rng: random.Random):
    """Returns (holders per key (sets of honest nodes holding the record, publisher included), finds, provides,
    triggered)."""
    holders, finds, provides, trig = [], 0, 0, False
    for i in range(p.multi):
        r = lookup(inst, inst.keys[i], publisher, p, rng, publish=True)
        finds += r["queries"]
        provides += len(r["store"])
        trig = trig or r["triggered"]
        hs = {n for n in r["store"] if not inst.is_sybil(n)}
        hs.add(publisher)
        holders.append(hs)
    return holders, finds, provides, trig


def search(inst: Instance, searcher: int, p: Params, rng: random.Random):
    """Search steps in order: (answered honest set, queries, triggered, key index); one per key i < p.multi, then
    the fallback's steps (run only if the earlier ones found nothing: evaluate() stops at the first success)."""
    out = []
    for i in range(p.multi):
        r = lookup(inst, inst.keys[i], searcher, p, rng)
        ans = {n for n in r["answered"] if not inst.is_sybil(n)}
        out.append((ans, r["queries"], r["triggered"], i))
    if p.fallback is not None:
        out += search(inst, searcher, p.fallback, rng)
    return out


def evaluate(holders, sres):
    """(found, queries spent): keys searched in order, stopping at the first that finds a provider."""
    q = 0
    for ans, nq, _, i in sres:
        q += nq
        if i < len(holders) and ans & holders[i]:
            return True, q
    return False, q
