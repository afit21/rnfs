"""Topology simulation for rnfs DHT lookups over Reticulum backbones + LoRa islands.

Standard library only. The lookup engine mirrors rnfs/dht/lookup.py (alpha/beta, window, 80-query cap,
stall windows, unknown-ID budget of alpha path requests, unreliable nodes ranked last and retried on top),
and node.py's start-node choice; routing tables follow rnfs/dht/routing.py (K per bucket, keep incumbents).

Time is simulated (seconds); costs are LoRa airtime (seconds of channel use, summed over every hop's
transmission) and counts of messages.
"""
import heapq
import math
import random
from collections import defaultdict

K = 10
BETA = 3
MAX_QUERIES = 8 * K
STALL_WINDOWS = 3
LOOKUP_TIMEOUT = 600.0
PATH_TIMEOUT = 30.0
EST_PER_HOP = 6.0
EST_GRACE = 5.0
MAX_FAILURES = 3
AVOID_AFTER = 300.0
AVOID_MAX = 3600.0
LINK_IDLE = 300.0

# LoRa airtime per hop, seconds, at ~1 kbit/s
A_PREQ = 0.6      # one path-request transmission (flooded: every island transport node repeats it)
A_ANN = 1.4       # announce / path response, per hop
A_LINK = 6.4      # link request + proof + RTT packet, per hop
A_LINKREQ = 2.4   # link request alone (sent toward an unreachable node; nothing comes back)
A_IDENT = 1.5
A_PROVIDE = 2.0   # /provide request + True
A_CLOSE = 0.5
PER_TX = 0.2      # medium access / processing delay per LoRa transmission (latency only)
BB_HOP = 0.03     # backbone one-way per-hop latency
PROC = 0.05


def a_find(n_ids):
    """Airtime per LoRa hop of one /find exchange returning n_ids IDs (4.0 s at 10)."""
    return 1.6 + 0.24 * n_ids


def bitlen(x):
    return x.bit_length()


class World:
    """Topology, identities, announce coverage and routing tables.

    kind[i] = -1 for backbone nodes, else the island index. Island nodes are slow (alpha 1).
    """

    def __init__(self, S, seed, island_frac=0.2, f_heard=None, island_sizes=(10, 50),
                 per_node_loss=0.1, rnfs_cap_share=0.5, hubs=None):
        rng = random.Random(seed)
        self.rng = rng
        self.S = S
        sizes = []
        tot = 0
        while tot < island_frac * S:
            n = rng.randint(*island_sizes)
            sizes.append(n)
            tot += n
        n_bb = S - tot
        self.n_bb = n_bb
        self.n_islands = len(sizes)
        self.hubs = hubs or max(3, n_bb // 100)
        self.ids = [rng.getrandbits(128) for _ in range(S)]
        self.kind = [-1] * S
        self.hub = [0] * S
        self.L = [0] * S       # LoRa hops to the island's gateway
        self.lidx = [0] * S    # index within island
        for i in range(n_bb):
            self.hub[i] = rng.randrange(self.hubs)
        self.island_nodes = []
        self.mesh = []
        self.gw = []
        self.ntransport = []
        start = n_bb
        for ii, n in enumerate(sizes):
            members = list(range(start, start + n))
            start += n
            self.island_nodes.append(members)
            hopm, gwl = self._geo_mesh(n, rng)
            self.mesh.append(hopm)
            g = members[gwl]
            self.gw.append(g)
            ghub = rng.randrange(self.hubs)
            for li, x in enumerate(members):
                self.kind[x] = ii
                self.lidx[x] = li
                self.L[x] = hopm[li][gwl]
                self.hub[x] = ghub
            self.ntransport.append(max(1, (n + 1) // 2))
        # Announce coverage: the gateway forwards a capped share of off-island announces onto LoRa.
        # RNS caps announce traffic at 2% of an interface's bitrate: 20 bit/s at 1 kbit/s, one ~1.4 kbit
        # announce per ~68 s, ~318 per 6 h announce_interval, of which rnfs gets rnfs_cap_share.
        off = S - min(sizes) if sizes else S
        if f_heard is None:
            f_heard = min(1.0, rnfs_cap_share * 318 / max(1, off))
        self.f_heard = f_heard
        self.island_heard = []   # per island: set of off-island nodes whose announces reach it
        for ii, members in enumerate(self.island_nodes):
            ms = set(members)
            others = [j for j in range(S) if j not in ms]
            self.island_heard.append(set(j for j in others if rng.random() < f_heard))
        self.node_heard = {}  # island node -> its own heard off-island set (per-node loss)
        for ii, members in enumerate(self.island_nodes):
            base = sorted(self.island_heard[ii])
            for x in members:
                if x == self.gw[ii]:
                    self.node_heard[x] = None  # gateway is on the backbone: hears everything
                else:
                    self.node_heard[x] = set(j for j in base if rng.random() >= per_node_loss)
        self.order_seed = rng.getrandbits(32)
        self._tables = {}
        self.part = set()        # partitioned islands
        self.late = False        # partition older than 2 x announce_interval: cross-cut entries stale
        self.late_tables = False # tables refilled after a long partition (used during late phase)

    @staticmethod
    def _geo_mesh(n, rng):
        """Random geometric graph in a unit square, radius raised until connected; hop matrix and gateway."""
        pts = [(rng.random(), rng.random()) for _ in range(n)]
        r = 1.6 / math.sqrt(n) + 0.05
        while True:
            adj = [[j for j in range(n) if j != i and (pts[i][0] - pts[j][0]) ** 2 + (pts[i][1] - pts[j][1]) ** 2 <= r * r]
                   for i in range(n)]
            hop = []
            ok = True
            for s in range(n):
                d = [-1] * n
                d[s] = 0
                q = [s]
                for u in q:
                    for v in adj[u]:
                        if d[v] < 0:
                            d[v] = d[u] + 1
                            q.append(v)
                if min(d) < 0:
                    ok = False
                    break
                hop.append(d)
            if ok:
                break
            r *= 1.1
        # gateway: the most central of a few random nodes
        cand = rng.sample(range(n), min(n, 4))
        g = min(cand, key=lambda c: max(hop[c]))
        return hop, g

    # Topology queries
    def is_island(self, i):
        return self.kind[i] >= 0

    def slow(self, i):
        return self.kind[i] >= 0 and i != self.gw[self.kind[i]]

    def _up(self, i):
        """Hops from i to its hub (island: LoRa hops to gateway + 1)."""
        return (self.L[i] + 1) if self.kind[i] >= 0 else 1

    def hops(self, i, j):
        ki, kj = self.kind[i], self.kind[j]
        if ki >= 0 and ki == kj:
            return self.mesh[ki][self.lidx[i]][self.lidx[j]]
        return self._up(i) + self._up(j) + (0 if self.hub[i] == self.hub[j] else 1)

    def lora(self, i, j):
        """LoRa transmissions one way on the path i->j."""
        ki, kj = self.kind[i], self.kind[j]
        if ki >= 0 and ki == kj:
            return self.mesh[ki][self.lidx[i]][self.lidx[j]]
        return (self.L[i] if ki >= 0 else 0) + (self.L[j] if kj >= 0 else 0)

    def bb_hops(self, i, j):
        ki, kj = self.kind[i], self.kind[j]
        if ki >= 0 and ki == kj:
            return 0
        return self.hops(i, j) - self.lora(i, j)

    def slow_next(self, i, j):
        ki = self.kind[i]
        if ki < 0:
            return False
        if i == self.gw[ki]:
            return self.kind[j] == ki
        return True

    def reachable(self, i, j):
        ki, kj = self.kind[i], self.kind[j]
        if ki >= 0 and ki == kj:
            return True
        return not (ki in self.part or kj in self.part)

    def cut(self, i, j):
        """True if i and j are on different sides of a partition (for staleness)."""
        return not self.reachable(i, j)

    def lora_to_break(self, i, j):
        """LoRa transmissions a link request from i makes before the path breaks (unreachable j)."""
        ki = self.kind[i]
        if ki >= 0 and ki != self.kind[j]:
            return self.L[i]
        return 0

    def heard(self, i, j):
        """True if i heard j's announce (identity + path known without a path request)."""
        if i == j:
            return True
        ki = self.kind[i]
        if ki < 0:
            return True
        if self.kind[j] == ki:
            return True
        h = self.node_heard[i]
        return True if h is None else (j in h)

    def candidates(self, i):
        S = self.S
        ki = self.kind[i]
        if ki < 0 or self.node_heard[i] is None:
            return [j for j in range(S) if j != i]
        return [j for j in self.island_nodes[ki] if j != i] + sorted(self.node_heard[i])

    # Routing tables
    def table(self, i, policy, phase="steady", hidden=None, token=None):
        """Bucket -> list of node indices. policy: 'base' (heard order, incumbents kept) or ('pns', p):
        a fraction p of each bucket's slots chosen by proximity (hops, slow next hop), the rest heard order.
        phase 'refill': after a long partition, cross-cut entries went stale and were replaced by reachable
        newcomers (heard order, or proximity for pns)."""
        key = (i, policy, phase, frozenset(self.part) if phase == "refill" else None, token)
        t = self._tables.get(key)
        if t is not None:
            return t
        rng = random.Random(self.order_seed * 1000003 + i)
        cand = self.candidates(i)
        rng.shuffle(cand)
        if hidden:
            cand = [j for j in cand if j not in hidden]
        me = self.ids[i]
        ids = self.ids
        buckets = defaultdict(list)
        if policy == "base":
            for j in cand:
                b = buckets[(me ^ ids[j]).bit_length()]
                if len(b) < K:
                    b.append(j)
        else:
            p = policy[1]
            nprox = int(round(p * K))
            byb = defaultdict(list)
            for j in cand:
                byb[(me ^ ids[j]).bit_length()].append(j)
            for bi, lst in byb.items():
                prox = sorted(lst, key=lambda j: (self.hops(i, j), self.slow_next(i, j)))
                chosen = prox[:nprox]
                cs = set(chosen)
                for j in lst:
                    if len(chosen) >= K:
                        break
                    if j not in cs:
                        chosen.append(j)
                        cs.add(j)
                buckets[bi] = chosen
        if phase == "refill":
            # stale cross-cut entries replaced by reachable candidates not in the bucket, keeping the rest
            byb = defaultdict(list)
            for j in cand:
                if self.reachable(i, j):  # cand already excludes hidden nodes
                    byb[(me ^ ids[j]).bit_length()].append(j)
            nb = {}
            allb = set(buckets) | set(byb)
            for bi in allb:
                cur = buckets.get(bi, [])
                live = [j for j in cur if self.reachable(i, j)]
                stale = [j for j in cur if not self.reachable(i, j)]
                fresh = [j for j in byb.get(bi, []) if j not in set(cur)]
                if policy != "base":
                    fresh.sort(key=lambda j: (self.hops(i, j), self.slow_next(i, j)))
                room = K - len(live)
                add = fresh[:room]
                keep_stale = stale[:max(0, room - len(add))]
                nb[bi] = live + add + keep_stale
            buckets = defaultdict(list, nb)
        t = [j for b in buckets.values() for j in b]
        self._tables[key] = t
        return t


class NodeState:
    __slots__ = ("fails", "learned", "links", "part_detected", "offfail", "records")

    def __init__(self):
        self.fails = {}          # node -> [consecutive failures, retry_at]
        self.learned = set()     # identities learned by path requests
        self.links = {}          # node -> link expiry time (open outgoing link)
        self.part_detected = False
        self.offfail = 0
        self.records = defaultdict(set)  # key -> providers


class Stats:
    __slots__ = ("air", "finds", "preqs", "links", "timeouts", "lat", "stores", "store_ok", "air_island")

    def __init__(self):
        self.air = 0.0
        self.finds = 0
        self.preqs = 0
        self.links = 0
        self.timeouts = 0
        self.lat = 0.0
        self.stores = 0
        self.store_ok = 0
        self.air_island = 0.0

    def add(self, o):
        for f in self.__slots__:
            setattr(self, f, getattr(self, f) + getattr(o, f))


class Sim:
    """Mutable network state on top of a World: per-node failure memory, learned paths, records.

    `upg` is the set of upgraded nodes; `strat` a dict of strategy switches applying to upgraded nodes:
      pns: p (fraction of slots by proximity) or 0
      pa: delta bits (path-aware: skip an unknown ID if a known unqueried one is within delta bits) or None
      lf: m (local-first: query m closest local servers before the global lookup) or 0
      kloc: extra local replicas on publish (two-level) or 0
      nloc: hop radius defining 'local' (None: exactly the island)
      ph: partition handling (detect, then stay island-local)
      ph_passive: partition already known from announce silence (warm)
      proxy: delegate lookups to an upgraded gateway server (additive request)
    """

    def __init__(self, world, upg=None, strat=None, seed=0):
        self.W = world
        self.upg = upg if upg is not None else set()
        self.strat = strat or {}
        self.st = {}
        self.rng = random.Random(seed)
        self.token = None
        # local-scope servers: upgraded island nodes (not gateways) announcing client mode, serving only
        # their island; old nodes and the backbone never add them, /find replies never list them
        self.scoped = set()
        if self.strat.get("scope"):
            W = world
            self.scoped = set(i for i in self.upg if W.kind[i] >= 0 and i != W.gw[W.kind[i]])

    def state(self, i):
        s = self.st.get(i)
        if s is None:
            s = self.st[i] = NodeState()
        return s

    def S(self, i, name, default=None):
        if i in self.upg:
            return self.strat.get(name, default)
        return default

    def policy(self, i):
        p = self.S(i, "pns", 0)
        return ("pns", p) if p else "base"

    def phase(self):
        return "refill" if (self.W.part and self.W.late_tables) else "steady"

    def table(self, i):
        if not self.scoped:
            return self.W.table(i, self.policy(i), self.phase())
        W = self.W
        ki = W.kind[i]
        if i in self.upg and ki >= 0:
            hidden = self.scoped - set(W.island_nodes[ki])
            tok = (self.token, "in", ki)
        else:
            hidden = self.scoped
            tok = (self.token, "out")
        return W.table(i, self.policy(i), self.phase(), hidden, tok)

    def known(self, i, j):
        return self.W.heard(i, j) or j in self.state(i).learned

    def is_local(self, i, j):
        W = self.W
        if W.kind[i] < 0:
            return False
        nloc = self.S(i, "nloc", None)
        if nloc is None:
            return W.kind[j] == W.kind[i]
        return W.slow_next(i, j) and W.hops(i, j) <= nloc

    def local_set(self, i):
        W = self.W
        if W.kind[i] < 0:
            return []
        return [j for j in range(W.S) if j != i and self.is_local(i, j) and self.known(i, j)]

    # failure memory (routing.py: failed / answered / unreliable / may_retry)
    def failed_count(self, i, j):
        e = self.state(i).fails.get(j)
        return e[0] if e else 0

    def unreliable(self, i, j):
        return self.failed_count(i, j) >= MAX_FAILURES

    def on_fail(self, i, j, now):
        s = self.state(i)
        e = s.fails.get(j) or [0, now]
        e[0] += 1
        if e[0] >= MAX_FAILURES:
            e[1] = now + min(AVOID_MAX, AVOID_AFTER * 2 ** (e[0] - MAX_FAILURES))
        s.fails[j] = e
        if self.S(i, "ph") and not self.is_local(i, j) and self.W.kind[i] >= 0:
            s.offfail += 1
            if s.offfail >= 3:
                s.part_detected = True

    def on_answer(self, i, j):
        s = self.state(i)
        s.fails.pop(j, None)
        if not self.is_local(i, j):
            s.offfail = 0
            s.part_detected = False

    def may_retry(self, i, j, now):
        e = self.state(i).fails.get(j)
        if not e or e[0] < MAX_FAILURES or now < e[1]:
            return False
        e[1] = now + min(AVOID_MAX, AVOID_AFTER * 2 ** (e[0] + 1 - MAX_FAILURES))
        return True

    def stale(self, i, j):
        """Entry j in i's table that /find replies leave out: failed since heard, or silent (late partition)."""
        if self.failed_count(i, j) > 0:
            return True
        return self.W.late and self.W.cut(i, j)

    def closest(self, i, key, count, for_reply=False):
        ids = self.W.ids
        t = [j for j in self.table(i) if self.failed_count(i, j) < MAX_FAILURES]  # dropped after max_failures
        if for_reply:
            t = [j for j in t if not self.stale(i, j) and j not in self.scoped]
        t.sort(key=lambda j: ids[j] ^ key)
        return t[:count]

    def start_nodes(self, i, key):
        fresh = self.closest(i, key, 2 * K, True)
        fs = set(fresh)
        nearest = fresh + [n for n in self.closest(i, key, 2 * K) if n not in fs]
        reliable = [n for n in nearest if not self.unreliable(i, n)]
        rs = set(reliable)
        return (reliable + [n for n in nearest if n not in rs])[:K]

    def find_reply(self, b, key):
        provs = list(self.state(b).records.get(key, ()))
        return provs[:K], self.closest(b, key, K, True)

    def partition_mode(self, i):
        """True if i treats itself as partitioned (only local nodes are usable)."""
        if not self.S(i, "ph"):
            return False
        if self.S(i, "ph_passive") and self.W.kind[i] in self.W.part:
            return True
        return self.state(i).part_detected

    # one /find (or a store) from a to b
    def contact(self, a, b, now, stats, kind="find", nids=0):
        """Cost and outcome of a request a->b. Returns (latency, ok)."""
        W = self.W
        st = self.state(a)
        L = W.lora(a, b)
        bbh = W.bb_hops(a, b)
        bb_rtt = 2 * bbh * BB_HOP + PROC
        reach = W.reachable(a, b)
        lat = 0.0
        air = 0.0
        island_a = W.kind[a]
        if not self.known(a, b):
            stats.preqs += 1
            if island_a >= 0 and a != W.gw[island_a]:
                n_tx = 1 + W.ntransport[island_a]
                air += A_PREQ * n_tx
            if reach:
                air += A_ANN * L
                lat += (A_PREQ + A_ANN + 2 * PER_TX) * L + bb_rtt
                st.learned.add(b)
            else:
                lat += PATH_TIMEOUT
                stats.timeouts += 1
                stats.air += air
                stats.lat += lat
                return lat, False
        link_open = kind == "find" and st.links.get(b, -1) >= now
        if not link_open:
            stats.links += 1
            if reach:
                air += (A_LINK + (A_IDENT + A_CLOSE if kind == "store" else 0)) * L
                lat += (A_LINK + 3 * PER_TX) * L + 1.5 * bb_rtt
            else:
                air += A_LINKREQ * W.lora_to_break(a, b)
                lat += EST_PER_HOP * W.hops(a, b) + EST_GRACE
                stats.timeouts += 1
                stats.air += air
                stats.lat += lat
                return lat, False
        if not reach:  # open link to a now unreachable node: the request times out
            air += A_LINKREQ * W.lora_to_break(a, b)
            lat += EST_PER_HOP * W.hops(a, b) + EST_GRACE
            stats.timeouts += 1
            st.links.pop(b, None)
            stats.air += air
            stats.lat += lat
            return lat, False
        if kind == "find":
            stats.finds += 1
            air += a_find(nids) * L
            lat += (a_find(nids) + 2 * PER_TX) * L + bb_rtt
            st.links[b] = now + lat + LINK_IDLE
        else:
            stats.stores += 1
            air += A_PROVIDE * L
            lat += (A_PROVIDE + 2 * PER_TX) * L + bb_rtt
        stats.air += air
        stats.lat += lat
        return lat, True

    # The lookup (lookup.py _iterate)
    def lookup(self, a, key, mode, now, stats, pre=None, restrict_local=False, stop_first=None):
        """mode 'closest' or 'providers'. Returns dict with providers found, time to first provider,
        result list (closest), latency. pre: {node: reply} already answered (local-first phase)."""
        W = self.W
        ids = W.ids
        alpha = 1 if W.slow(a) else 3
        window = max(alpha, BETA)
        stop_at_first = W.slow(a) if stop_first is None else stop_first  # max_parallel_providers 1 on slow nodes
        prs = self.S(a, "prs", None) if not W.slow(a) else None
        far = set()
        pa = self.S(a, "pa", None)
        budget = alpha
        ub = self.S(a, "ub", None)  # (closest budget, providers budget) for path requests per lookup
        if ub is not None and W.slow(a):
            budget = max(alpha, ub[0] if mode == "closest" else ub[1])
        t0 = now
        deadline = now + LOOKUP_TIMEOUT
        failed = set()
        refused = set()
        allowed_unknown = set()
        known = {}
        doubtful = set()
        queried = set()
        answered = set()
        found = []
        found_set = set()
        t_first = None
        local_only = restrict_local

        def usable(n):
            if n == a or n in failed or n in refused:
                return False
            if local_only or self.partition_mode(a):
                return self.is_local(a, n)
            return True

        def dist(n):
            return ids[n] ^ key

        def merge(reply_nodes):
            for n in reply_nodes:
                if n not in known and n not in queried and usable(n):
                    known[n] = dist(n)
                    if prs is not None and W.hops(a, n) > prs:
                        far.add(n)
                    if self.unreliable(a, n):
                        doubtful.add(n)

        for n in self.start_nodes(a, key):
            if usable(n):
                known[n] = dist(n)
                if prs is not None and W.hops(a, n) > prs:
                    far.add(n)
                if self.unreliable(a, n):
                    doubtful.add(n)
        if pre:
            for n, (provs, nodes) in pre.items():
                if n not in known and usable(n):
                    known[n] = dist(n)
                queried.add(n)
                answered.add(n)
                doubtful.discard(n)
                merge(nodes)
                for p in provs:
                    if p not in found_set and p != a:
                        found_set.add(p)
                        found.append(p)
                        if t_first is None:
                            t_first = now

        def rank(n):
            return (n in doubtful or n in far, known[n])

        def reach_of():
            if len(known) < BETA:
                return 1 << 200
            return bitlen(heapq.nsmallest(BETA, known.values())[-1])

        heap = []  # (time, seq, node, reply, is_retry)
        seq = 0
        in_flight = 0
        sent = 0
        retrying = set()
        reach = reach_of()
        stalled = 0
        why = "?"
        while True:
            if mode == "providers" and found and (stop_at_first or len(found) >= 3):
                why = "enough providers"
                break
            if now >= deadline:
                why = "timeout"
                break
            # drop nodes that became unusable (partition detected mid-lookup)
            if self.partition_mode(a) and not local_only:
                for n in [n for n in known if not self.is_local(a, n)]:
                    if n not in answered:
                        del known[n]
            ranked = sorted(known, key=rank)
            if ranked and all(n in answered for n in ranked[:BETA]):
                why = "complete"
                break
            if stalled >= STALL_WINDOWS * window:
                why = "no progress"
                break
            for n in ranked[:window]:
                if n in retrying:
                    retrying.discard(n)
                    in_flight += 1
            while in_flight < alpha and sent < MAX_QUERIES:
                ranked = sorted(known, key=rank)[:window]
                n = next((x for x in ranked if x not in queried), None)
                if n is None:
                    break
                queried.add(n)
                if not self.known(a, n):
                    if pa is not None:
                        bl = bitlen(known[n])
                        alt = any((m not in queried and self.known(a, m) and m not in doubtful
                                   and bitlen(known[m]) <= bl + pa) for m in known)
                        if alt:
                            del known[n]
                            continue
                    if n not in allowed_unknown:
                        if len(allowed_unknown) >= budget:
                            refused.add(n)
                            del known[n]
                            continue
                        allowed_unknown.add(n)
                reply = self.find_reply(n, key) if W.reachable(a, n) else None
                nids = (len(reply[0]) + len(reply[1])) if reply else 0
                lat, ok = self.contact(a, n, now, stats, "find", nids)
                seq += 1
                heapq.heappush(heap, (now + lat, seq, n, reply if ok else None, False))
                sent += 1
                in_flight += 1
            for n in sorted(known, key=known.__getitem__)[:window]:
                if n not in doubtful or n in queried or sent >= MAX_QUERIES:
                    continue
                if self.known(a, n) and self.may_retry(a, n, now):
                    queried.add(n)
                    retrying.add(n)
                    sent += 1
                    reply = self.find_reply(n, key) if W.reachable(a, n) else None
                    nids = (len(reply[0]) + len(reply[1])) if reply else 0
                    sub = Stats()
                    lat, ok = self.contact(a, n, now, sub, "find", nids)
                    sub.lat = 0.0
                    stats.add(sub)
                    seq += 1
                    heapq.heappush(heap, (now + lat, seq, n, reply if ok else None, True))
            if in_flight == 0:
                why = "query cap" if sent >= MAX_QUERIES else "exhausted"
                break
            # next completion that is waited for (retries not waited for, but processed if they come first)
            t, _, n, reply, is_retry = heapq.heappop(heap)
            if t > deadline:
                now = deadline
                why = "timeout"
                heapq.heappush(heap, (t, _, n, reply, is_retry))
                break
            now = t
            if n in retrying:
                retrying.discard(n)  # a retry nothing waits for
            else:
                in_flight -= 1
            if reply is None:
                failed.add(n)
                known.pop(n, None)
                self.on_fail(a, n, now)
                continue
            self.on_answer(a, n)
            answered.add(n)
            doubtful.discard(n)
            provs, nodes = reply
            merge(nodes)
            nr = reach_of()
            if nr < reach:
                reach, stalled = nr, 0
            else:
                stalled += 1
            if mode == "providers":
                for p in provs:
                    if p not in found_set and p != a:
                        found_set.add(p)
                        found.append(p)
                        if t_first is None:
                            t_first = now
        # outstanding queries past the end: their failures still count against the nodes
        for t, _, n, reply, is_retry in heap:
            if reply is None:
                self.on_fail(a, n, t)
            else:
                self.on_answer(a, n)
        result = None
        if mode == "closest":
            cand = [n for n in known if n in answered or (n not in queried and self.known(a, n))]
            result = sorted(cand, key=lambda n: (n in doubtful or n in far, known[n]))[:K]
        return {"found": found, "t_first": None if t_first is None else t_first - t0, "lat": now - t0,
                "result": result, "why": why, "queried": len(queried)}

    # Publishing
    def store(self, a, targets, key, now, stats):
        t = now
        stored = []
        for n in targets:
            if n == a:
                continue
            lat, ok = self.contact(a, n, t, stats, "store")
            t += lat
            if ok:
                self.state(n).records[key].add(a)
                stats.store_ok += 1
                stored.append(n)
            else:
                self.on_fail(a, n, t)
        return t - now, stored

    def publish(self, a, key, now, stats):
        """Publish key from a: global closest (or delegated), plus local replicas if two-level."""
        self.state(a).records[key].add(a)
        W = self.W
        t = now
        targets = None
        if self.S(a, "proxy") and W.kind[a] >= 0 and not self.partition_mode(a):
            g = W.gw[W.kind[a]]
            if g in self.upg and W.reachable(a, g) and g != a:
                lat, targets = self._proxy(a, g, key, "closest", t, stats)
                t += lat
        if targets is None:
            r = self.lookup(a, key, "closest", t, stats)
            t += r["lat"]
            targets = r["result"]
        lat, stored = self.store(a, targets, key, t, stats)
        t += lat
        kloc = self.S(a, "kloc", 0)
        if kloc and W.kind[a] >= 0:
            loc = sorted(self.local_set(a), key=lambda j: W.ids[j] ^ key)[:kloc]
            extra = [j for j in loc if j not in set(targets)]
            lat2, st2 = self.store(a, extra, key, t, stats)
            t += lat2
            stored += st2
        return t - now, stored, targets

    def _proxy(self, a, g, key, mode, now, stats):
        """Delegated lookup: a asks gateway g (one exchange over a's LoRa hops), g runs it as a fast node."""
        sub = Stats()
        lat_ex, ok = self.contact(a, g, now, sub, "find", K)
        stats.add(sub)
        if not ok:
            return lat_ex, None
        sub2 = Stats()
        r = self.lookup(g, key, mode, now, sub2, stop_first=True)
        sub2.lat = 0
        stats.air += sub2.air
        stats.finds += sub2.finds
        stats.preqs += sub2.preqs
        stats.links += sub2.links
        stats.timeouts += sub2.timeouts
        if mode == "closest":
            return lat_ex + r["lat"], r["result"]
        return lat_ex + (r["t_first"] if r["t_first"] is not None else r["lat"]), r["found"]

    def fetch_lookup(self, a, key, now, stats):
        """Find providers of key from a, with the strategy's local-first phase and proxy. Returns
        (found providers, latency to first provider or end)."""
        W = self.W
        t = now
        m = self.S(a, "lf", 0)
        pre = {}
        found = []
        if m and W.kind[a] >= 0:
            loc = sorted(self.local_set(a), key=lambda j: W.ids[j] ^ key)[:m]
            for n in loc:
                reply = self.find_reply(n, key)
                lat, ok = self.contact(a, n, t, stats, "find", len(reply[0]) + len(reply[1]))
                t += lat
                if not ok:
                    self.on_fail(a, n, t)
                    continue
                self.on_answer(a, n)
                pre[n] = reply
                if reply[0]:
                    found = [p for p in reply[0] if p != a]
                    if found:
                        return found, t - now, "local"
        if self.S(a, "proxy") and W.kind[a] >= 0 and not self.partition_mode(a):
            g = W.gw[W.kind[a]]
            if g in self.upg and W.reachable(a, g) and g != a:
                lat, provs = self._proxy(a, g, key, "providers", t, stats)
                if provs:
                    return provs, t - now + lat, "proxy"
                t += lat
        r = self.lookup(a, key, "providers", t, stats, pre=pre)
        lat = r["t_first"] if r["t_first"] is not None else r["lat"]
        return r["found"], t - now + lat, r["why"]


def true_closest(W, key, among=None, count=K):
    nodes = range(W.S) if among is None else among
    return sorted(nodes, key=lambda j: W.ids[j] ^ key)[:count]
