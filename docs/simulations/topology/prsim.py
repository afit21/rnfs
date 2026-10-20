"""Concurrent engine for the path-request study: rnfs operations as processes on one simulated clock, so the path
requests of concurrent lookups, disjoint paths and nodes reach each transport node's interfaces in time order and go
through RNS's ingress limit (prlimit.PRIngress) and inbound queue (prlimit.InboundQueue).

It reuses sim.py's world, routing tables, failure memory, records and cost constants; what it adds:
- Lookups (sim.Sim.lookup ported to a process) run as disjoint paths that share no node (rnfs.dht.disjoint: start
  nodes dealt in turn, a node claimed by the first path to query it), with rnfs.dht.lookups' budgets of path requests
  (split_budget: slow publish 10 over 2 paths, slow search 3, fallback 3 over 2; elsewhere alpha per path).
- A publish round is sweep providing as built (rnfs.dht.sweep.Sweep, publisher.py): a lookup takes the next due key
  that is neither covered nor within estimate - 1 leading bits of a key being looked up (before any region is known
  the first lookup runs alone); its region is the keys sharing the bits its key shares with the 10th closest server it
  found. PUBLISH_LOOKUPS (4) run at once on fast nodes, one at a time and at most SLOW_ROUND_LOOKUPS (10) a round on
  slow ones. Each key's store set: its K closest of what its region learned, each path's K closest, the K closest
  senior servers of the announce first-seen log (every server the node heard; none on a node just started) and the
  local replicas. Stores need no path request (every store target answered, or has a known path), so their airtime
  and time aren't simulated: records are placed when the round's lookups end. The extra /find to a region's far half
  (sweep.explore) isn't modelled.
- A join (node.py join): bootstrap nodes one at a time until BOOTSTRAP_ANSWERS (2) answer, then a lookup for the
  node's own ID with the default budget (alpha).
- Path requests: an island node's is flooded over its island and answered by its gateway (or, with relay "full", RNS's
  default interface mode, only by a transport node within one hop that has the path: a transport node in mode full
  doesn't forward a request for an unknown path); a backbone node's (and a gateway's) goes to its hub over its TCP
  link. The transport node counts it on the receiving interface (the gateway's LoRa interface; the hub's spawned
  interface for that client), limits and queues it as RNS does, and answers it after PATH_REQUEST_GRACE on the next
  job run if it has the path. A request dropped from the queue, or not answered, fails after path_timeout (30 s).
- RNS's own path requests: after a link that never established, a node that isn't a transport node expires the path
  and requests it again if it last asked more than PATH_REQUEST_MI ago, at most one every DISCOVERY_PR_TX_THROTTLE
  (Transport.jobs, RNS 1.5.6 Transport.py ~697-720).
- rnfs's planned pacing (cfg pacing = spacing in seconds): a node-wide budget spaces rnfs's path requests; a lookup
  waiting for it asks candidates with a known path meanwhile (cfg known_first, known_first_k); a query failed for want
  of a path doesn't count towards partition mode (cfg path_fail_partition False); a failed link charges the budget.
  Optionally (cfg ceiling) an island node also holds back while its own LoRa interface limits path requests.
- Nodes just started (Engine.fresh, table_of, client_since): no off-island announce heard, a routing table of the
  servers that answered it, and a new spawned interface at its hub.
"""
import heapq
import random
from collections import defaultdict, deque

from prlimit import (DISCOVERY_PR_TX_THROTTLE, JOB_INTERVAL, PATH_REQUEST_GRACE, PATH_REQUEST_MI, InboundQueue,
                     Pacer, PRIngress)
from sim import (A_ANN, A_CLOSE, A_IDENT, A_LINK, A_LINKREQ, A_PREQ, A_PROVIDE, AVOID_AFTER, AVOID_MAX, BB_HOP, BETA,
                 EST_GRACE, EST_PER_HOP, K, LINK_IDLE, LOOKUP_TIMEOUT, MAX_FAILURES, MAX_QUERIES, PATH_TIMEOUT, PER_TX,
                 PROC, STALL_WINDOWS, Stats, a_find, bitlen)

INF = float("inf")
SLOW_PATHS, FAST_PATHS = 2, 3          # rnfs.dht.lookups SLOW_DISJOINT_PATHS, DISJOINT_PATHS
PUBLISH_LOOKUPS = 4                    # publisher.py: region lookups at once on fast nodes
SLOW_ROUND_LOOKUPS = 10                # publisher.py: region lookups per round on slow nodes
BOOTSTRAP_ANSWERS = 2                  # node.py

DEFAULT_CFG = {
    "limit": True,             # apply RNS's limit (False: count what it would do, change nothing: the study's model)
    "pacing": None,            # rnfs's planned node-wide spacing of path requests, seconds (None: no pacing)
    "known_first": True,       # with pacing: a lookup waiting for the budget asks known-path candidates meanwhile
    "known_first_k": None,     # ... only among its this many closest candidates (None: any it knows)
    "seniors": True,           # store sets include the K closest senior servers of the announce first-seen log
    # with pacing, on an island node: also wait while its own LoRa interface, which hears the same flood as the
    # gateway's, limits path requests (pr_burst_active), or estimates them at or above this rate (None: not used)
    "ceiling": None,
    "ceiling_backoff": 0.0,    # a node the ceiling holds back waits 1 s plus a random share of this before trying again
    "path_fail_partition": True,  # a query failed for want of a path counts towards partition mode (rnfs today)
    "relay": "flood",          # island path requests: "flood" (the study's model) or "full" (RNS's default mode)
    "lora_scale": 1.0,         # LoRa airtime multiplier (1.0: ~1 kbit/s; 0.18: ~5.5 kbit/s, SF7 at 125 kHz)
    "hub_service": 0.002,      # s per packet of a hub's inbound drainer (reads the cached announce from disk)
    "hub_busy": 0.2,           # share of the hub's drainer taken by data and announces (served first)
    "gw_service": 0.002,
    "burst_gap": 0.0005,       # s between path requests one node sends at the same moment (threads, TCP writes)
    "rerequest": True,         # RNS re-requests a path after a link that never established
    "gw_new": False,           # the gateway's LoRa interface is under 2 h old (threshold 3 instead of 8)
}


def split_budget(total, paths):
    """rnfs.dht.lookups.split_budget: total path requests dealt to paths (None: alpha each)."""
    if total is None:
        return [None] * paths
    return [total // paths + (1 if i < total % paths else 0) for i in range(paths)]


class Ev:
    """Something processes wait for (yield it); triggered once with a value."""
    __slots__ = ("done", "value", "waiters")

    def __init__(self):
        """Not triggered."""
        self.done = False
        self.value = None
        self.waiters = []


class Box:
    """Completions of a lookup's queries, which the lookup takes one at a time."""

    def __init__(self):
        """Empty."""
        self.items = deque()
        self.ev = None

    def put(self, eng, item):
        """A query completed."""
        self.items.append(item)
        if self.ev is not None:
            eng.trigger(self.ev)

    def get(self, eng, until):
        """Process step: the next completion, or None at `until`."""
        if not self.items:
            ev = Ev()
            self.ev = ev
            if until < INF:
                eng.timer(ev, until - eng.now)
            yield ev
            self.ev = None
        return self.items.popleft() if self.items else None


class OpStats(Stats):
    """sim.Stats plus path requests RNS sent by itself, dropped and limited ones, and path failures."""
    __slots__ = ("rns_preqs", "dropped", "limited", "path_fails", "link_fails", "wait")

    def __init__(self):
        """All zero."""
        super().__init__()
        self.rns_preqs = 0
        self.dropped = 0
        self.limited = 0
        self.path_fails = 0
        self.link_fails = 0
        self.wait = 0.0


class Engine:
    """Processes on one clock over a sim.Sim (world, tables, failure memory, records)."""

    def __init__(self, sim, cfg=None, seed=0):
        """An engine at time 0 with no process."""
        self.sim = sim
        self.W = sim.W
        self.cfg = dict(DEFAULT_CFG, **(cfg or {}))
        self.rng = random.Random(seed)
        self.now = 0.0
        self.heap = []
        self.seq = 0
        self.ifaces = {}
        self.queues = {}
        self.pacers = {}
        self.last_send = {}
        self.last_pr = {}
        self.rns_next = {}
        self.chan_free = {}
        self.expired = defaultdict(set)
        self.fresh = {}            # node -> set of off-island servers it has heard (None/absent: the world's)
        self.table_of = {}         # node -> its own routing table (fresh nodes), else the world's snapshot
        self.client_since = {}     # backbone node -> when its hub's spawned interface for it came up
        self.prlog = []            # (time, node, origin, outcome)
        self._heard_cache = {}
        self.partition_hits = defaultdict(int)  # node -> entries into partition mode (PH) seen
        self.part_at = {}                       # node -> when it first entered partition mode
        # Any node: failed queries in a row (path failures counted as cfg path_fail_partition says), and how often
        # that reached 3: what would put a node with local servers (a gateway, say) into partition mode
        self.fail_run = defaultdict(int)
        self.run3 = defaultdict(int)
        W = self.W
        rng = random.Random(seed * 7 + 1)
        self.transport = set(W.gw)
        for ii, members in enumerate(W.island_nodes):
            others = [x for x in members if x != W.gw[ii]]
            self.transport.update(rng.sample(others, min(len(others), max(0, W.ntransport[ii] - 1))))

    # Process scheduling

    def _at(self, t, gen, done, val):
        heapq.heappush(self.heap, (t, self.seq, gen, done, val))
        self.seq += 1

    def spawn(self, gen):
        """Start a process now; returns the Ev it triggers with its return value."""
        done = Ev()
        self._at(self.now, gen, done, None)
        return done

    def trigger(self, ev, value=None):
        """Trigger ev (once), resuming its waiters now."""
        if ev.done:
            return
        ev.done = True
        ev.value = value
        for gen, done in ev.waiters:
            self._at(self.now, gen, done, value)
        ev.waiters = []

    def timer(self, ev, delay, value=None):
        """Trigger ev after delay (unless already triggered)."""
        def t():
            yield max(0.0, delay)
            self.trigger(ev, value)
        self.spawn(t())

    def run(self, until=INF):
        """Run processes until none is left or the clock passes until."""
        while self.heap:
            t, _, gen, done, val = self.heap[0]
            if t > until:
                break
            heapq.heappop(self.heap)
            self.now = t
            try:
                y = gen.send(val)
            except StopIteration as e:
                self.trigger(done, e.value)
                continue
            if isinstance(y, Ev):
                if y.done:
                    self._at(self.now, gen, done, y.value)
                else:
                    y.waiters.append((gen, done))
            else:
                self._at(self.now + max(0.0, y or 0.0), gen, done, None)

    # Knowledge

    def known(self, a, b):
        """True if a has a path (and identity) for b: heard its announce or learned it by a path request, and RNS
        hasn't expired it."""
        if b in self.expired.get(a, ()):
            return False
        f = self.fresh.get(a)
        if f is not None:
            W = self.W
            if b in self.sim.state(a).learned or b in f:
                return True
            return W.kind[a] >= 0 and W.kind[a] == W.kind[b]
        return self.sim.known(a, b)

    def learn(self, a, b):
        """a got a path to b."""
        self.sim.state(a).learned.add(b)
        self.expired[a].discard(b)

    def start_nodes(self, a, key):
        """node.py start nodes, from a's own table if it has one (a fresh node), else sim's."""
        t = self.table_of.get(a)
        if t is None:
            return self.sim.start_nodes(a, key)
        sim = self.sim
        live = [n for n in t if sim.failed_count(a, n) < MAX_FAILURES]
        live.sort(key=lambda n: self.W.ids[n] ^ key)
        near = live[:2 * K]
        rel = [n for n in near if not sim.unreliable(a, n)]
        return (rel + [n for n in near if n not in set(rel)])[:K]

    def note_table(self, a, nodes):
        """A fresh node adds servers that answered to its routing table (node.py _answered: routing.answered)."""
        t = self.table_of.get(a)
        if t is not None:
            t.update(n for n in nodes if n != a)

    # Failure memory (sim.Sim.on_fail with the cause)

    def on_fail(self, a, n, cause):
        """A query a->n failed: cause "path" (no path came) or "link"."""
        sim = self.sim
        s = sim.state(a)
        e = s.fails.get(n) or [0, self.now]
        e[0] += 1
        if e[0] >= MAX_FAILURES:
            e[1] = self.now + min(AVOID_MAX, AVOID_AFTER * 2 ** (e[0] - MAX_FAILURES))
        s.fails[n] = e
        if cause == "path" and not self.cfg["path_fail_partition"]:
            return
        if not sim.is_local(a, n):
            self.fail_run[a] += 1
            if self.fail_run[a] == 3:
                self.run3[a] += 1
        if sim.S(a, "ph") and self.W.kind[a] >= 0 and not sim.is_local(a, n):
            s.offfail += 1
            if s.offfail >= 3 and not s.part_detected:
                s.part_detected = True
                self.partition_hits[a] += 1
                self.part_at.setdefault(a, self.now)

    def on_answer(self, a, n):
        """sim.Sim.on_answer."""
        self.sim.on_answer(a, n)
        if not self.sim.is_local(a, n):
            self.fail_run[a] = 0

    def timed(self, gen):
        """Process: run gen; returns (its value, the time it took)."""
        t0 = self.now
        v = yield from gen
        return v, self.now - t0

    # Path requests

    def iface(self, key, created, parent=None):
        """The PRIngress for key, made on first use."""
        f = self.ifaces.get(key)
        if f is None:
            f = self.ifaces[key] = PRIngress(key, created, parent, jobs_phase=self.rng.uniform(0, 5))
        return f

    def queue(self, key, service, busy):
        """The inbound queue of transport node key, with its drainer process."""
        q = self.queues.get(key)
        if q is None:
            q = self.queues[key] = InboundQueue(service, busy)
            self.spawn(self._drainer(q))
        return q

    def _drainer(self, q):
        """Transport.inbound_job: serve the queue one packet at a time."""
        while True:
            item = q.get()
            if item is None:
                q.idle = True
                q.wake = Ev()
                yield q.wake
                q.idle = False
                continue
            yield q.service
            self.trigger(item)

    def pacer(self, a):
        """a's node-wide path-request budget (planned)."""
        p = self.pacers.get(a)
        if p is None:
            p = self.pacers[a] = Pacer(self.cfg["pacing"] or 0.0)
        return p

    def neighbours(self, a):
        """Island nodes within one LoRa hop of a."""
        W = self.W
        ki = W.kind[a]
        m = W.mesh[ki]
        li = W.lidx[a]
        return [x for x in W.island_nodes[ki] if m[li][W.lidx[x]] == 1]

    def path_request(self, a, b, st, origin="rnfs"):
        """Process step: a asks the network for a path to b. Returns True once it has one, False after path_timeout.
        origin "rns": RNS's own re-request."""
        W, cfg = self.W, self.cfg
        t_send = max(self.now, self.last_send.get(a, -INF) + cfg["burst_gap"])
        self.last_send[a] = t_send
        if t_send > self.now:
            yield t_send - self.now
        t0 = self.now
        self.last_pr[(a, b)] = t0
        if origin == "rns":
            st.rns_preqs += 1
        else:
            st.preqs += 1
        reach = W.reachable(a, b)
        ki = W.kind[a]
        ls = cfg["lora_scale"]
        if ki >= 0 and a != W.gw[ki]:
            st.air += A_PREQ * ls * (1 + W.ntransport[ki])
            L = W.L[a]
            if cfg["relay"] == "full" and L > 1:
                # RNS default mode: only a transport node within one hop that has the path answers
                ok = reach and any(n in self.transport and n != a and self.known(n, b) for n in self.neighbours(a))
                if ok:
                    yield (A_PREQ + A_ANN) * ls + 2 * PER_TX + PATH_REQUEST_GRACE + self.rng.uniform(0, JOB_INTERVAL)
                    st.air += A_ANN * ls
                    self.learn(a, b)
                    self.prlog.append((t0, a, origin, "answered"))
                    return True
                yield PATH_TIMEOUT
                st.timeouts += 1
                st.path_fails += origin == "rnfs"
                self.prlog.append((t0, a, origin, "unanswered"))
                return False
            if ki in W.part:
                yield PATH_TIMEOUT
                st.timeouts += 1
                st.path_fails += origin == "rnfs"
                self.prlog.append((t0, a, origin, "unanswered"))
                return False
            g = A_PREQ * ls
            arr = max(self.now + L * (g + PER_TX), self.chan_free.get(ki, -INF) + g)
            self.chan_free[ki] = arr
            yield arr - self.now
            created = 0.0 if cfg["gw_new"] else -1e9
            f = self.iface(("gwlora", ki), created)
            q = self.queue(("gw", ki), cfg["gw_service"], 0.0)
            down = (A_ANN * ls + PER_TX) * L
            air_down = A_ANN * ls * L
        else:
            h = W.hub[a]
            yield BB_HOP
            parent = self.iface(("hub", h), -1e9)
            f = self.iface(("hubcli", h, a), self.client_since.get(a, -1e9), parent)
            q = self.queue(("hub", h), cfg["hub_service"], cfg["hub_busy"])
            down = BB_HOP
            air_down = 0.0
        limited = f.received_pr(self.now)
        if limited:
            st.limited += 1
        outcome = "answered"
        if cfg["limit"]:
            ev = Ev()
            if not q.put(ev, limited):
                st.dropped += 1
                outcome = "dropped"
            else:
                if q.idle and q.wake is not None:
                    self.trigger(q.wake)
                yield ev
                if not limited:  # Transport.path_request evaluates the limit again
                    f.jobs_until(self.now)
                    f.check(self.now)
        if outcome == "answered" and not reach:
            outcome = "unanswered"
        self.prlog.append((t0, a, origin, outcome + ("/limited" if limited else "")))
        if outcome != "answered":
            yield max(0.0, PATH_TIMEOUT - (self.now - t0))
            st.timeouts += 1
            st.path_fails += origin == "rnfs"
            return False
        yield PATH_REQUEST_GRACE + self.rng.uniform(0, JOB_INTERVAL) + down
        st.air += air_down
        self.learn(a, b)
        return True

    def rns_rerequest(self, a, b, st):
        """Transport.jobs after a link a->b that never established: expire the path and, unless a is a transport
        node or asked within PATH_REQUEST_MI, request it again (throttled)."""
        if not self.cfg["rerequest"] or a in self.transport:
            return
        self.expired[a].add(b)
        if self.cfg["pacing"]:
            self.pacer(a).charge(self.now)
        t = self.now + self.rng.uniform(0, 1.0)  # links_check_interval
        if t - self.last_pr.get((a, b), -INF) <= PATH_REQUEST_MI:
            return
        t = max(t, self.rns_next.get(a, -INF)) + DISCOVERY_PR_TX_THROTTLE
        self.rns_next[a] = t
        self.last_pr[(a, b)] = t

        def go():
            yield t - self.now
            yield from self.path_request(a, b, st, "rns")
        self.spawn(go())

    def ceiling_ok(self, a):
        """The ceiling (cfg ceiling): False while a's own LoRa interface limits path requests or estimates them at
        or above the ceiling. Its interface hears the same flood as its gateway's, so the gateway's stands in."""
        c = self.cfg["ceiling"]
        W = self.W
        if c is None or W.kind[a] < 0 or a == W.gw[W.kind[a]]:
            return True
        f = self.ifaces.get(("gwlora", W.kind[a]))
        if f is None:
            return True
        f.jobs_until(self.now)
        return not f.active and f.peek(self.now) < c

    def backoff(self):
        """How long a node the ceiling holds back waits before looking again."""
        return 1.0 + self.rng.uniform(0, self.cfg["ceiling_backoff"])

    def acquire(self, a, st):
        """Process step: wait for a's budget (planned pacing) and take it."""
        if not self.cfg["pacing"]:
            return
        p = self.pacer(a)
        while not p.free(self.now) or not self.ceiling_ok(a):
            w = max(p.free_at - self.now, 0.0 if self.ceiling_ok(a) else self.backoff())
            st.wait += w
            yield w
        p.take(self.now)

    # One request

    def link_cost(self, a, b, kind, nids, st):
        """sim.Sim.contact after the path is known: (latency, ok, never_established)."""
        W, sim = self.W, self.sim
        ls = self.cfg["lora_scale"]
        s = sim.state(a)
        L = W.lora(a, b)
        bb_rtt = 2 * W.bb_hops(a, b) * BB_HOP + PROC
        reach = W.reachable(a, b)
        lat = air = 0.0
        if not (kind == "find" and s.links.get(b, -1) >= self.now):
            st.links += 1
            if reach:
                air += (A_LINK + (A_IDENT + A_CLOSE if kind == "store" else 0)) * ls * L
                lat += (A_LINK * ls + 3 * PER_TX) * L + 1.5 * bb_rtt
            else:
                air += A_LINKREQ * ls * W.lora_to_break(a, b)
                lat += EST_PER_HOP * W.hops(a, b) + EST_GRACE
                st.timeouts += 1
                st.link_fails += 1
                st.air += air
                return lat, False, True
        if not reach:
            air += A_LINKREQ * ls * W.lora_to_break(a, b)
            lat += EST_PER_HOP * W.hops(a, b) + EST_GRACE
            st.timeouts += 1
            st.link_fails += 1
            s.links.pop(b, None)
            st.air += air
            return lat, False, False
        if kind == "find":
            st.finds += 1
            air += a_find(nids) * ls * L
            lat += (a_find(nids) * ls + 2 * PER_TX) * L + bb_rtt
            s.links[b] = self.now + lat + LINK_IDLE
        else:
            st.stores += 1
            air += A_PROVIDE * ls * L
            lat += (A_PROVIDE * ls + 2 * PER_TX) * L + bb_rtt
        st.air += air
        return lat, True, False

    def query(self, a, n, key, box, is_retry, st):
        """Process: one /find a->n (a path request first if needed); puts (n, reply or None, is_retry) in box."""
        if not self.known(a, n):
            ok = yield from self.path_request(a, n, st)
            if not ok:
                self.on_fail(a, n, "path")
                box.put(self, (n, None, is_retry))
                return
        W = self.W
        reply = self.sim.find_reply(n, key) if W.reachable(a, n) else None
        nids = (len(reply[0]) + len(reply[1])) if reply else 0
        lat, ok, never = self.link_cost(a, n, "find", nids, st)
        yield lat
        if never:
            self.rns_rerequest(a, n, st)
        if ok:
            self.on_answer(a, n)
            self.note_table(a, [n])
        else:
            self.on_fail(a, n, "link")
        box.put(self, (n, reply if ok else None, is_retry))

    # The lookup (sim.Sim.lookup as a process, one path of a disjoint lookup when claims is given)

    def lookup(self, a, key, mode, st, budget=None, start=None, claims=None, pidx=0, stop_first=None,
               pre=None, deadline=None, max_queries=MAX_QUERIES):
        """Process: returns {found, t_first, lat, result, why, queried, sent}."""
        sim, W = self.sim, self.W
        ids = W.ids
        cfg = self.cfg
        alpha = 1 if W.slow(a) else 3
        window = max(alpha, BETA)
        stop_at_first = W.slow(a) if stop_first is None else stop_first
        prs = sim.S(a, "prs", None) if not W.slow(a) else None
        budget = alpha if budget is None else max(alpha, budget)
        pacing = bool(cfg["pacing"])
        pacer = self.pacer(a) if pacing else None
        t0 = self.now
        deadline = t0 + LOOKUP_TIMEOUT if deadline is None else deadline
        failed, refused, allowed_unknown, doubtful, far = set(), set(), set(), set(), set()
        queried, answered = set(), set()
        known = {}
        found, found_set = [], set()
        t_first = None

        def usable(n):
            if n == a or n in failed or n in refused:
                return False
            if claims is not None and claims.get(n, pidx) != pidx:
                return False
            if sim.partition_mode(a):
                return sim.is_local(a, n)
            return True

        def add(n):
            known[n] = ids[n] ^ key
            if prs is not None and W.hops(a, n) > prs:
                far.add(n)
            if sim.unreliable(a, n):
                doubtful.add(n)

        for n in (self.start_nodes(a, key) if start is None else start):
            if usable(n):
                add(n)
        if pre:
            for n, (provs, nodes) in pre.items():
                if n not in known and usable(n):
                    known[n] = ids[n] ^ key
                queried.add(n)
                answered.add(n)
                doubtful.discard(n)
                for m in nodes:
                    if m not in known and m not in queried and usable(m):
                        add(m)
                for p in provs:
                    if p not in found_set and p != a:
                        found_set.add(p)
                        found.append(p)
                        t_first = t_first if t_first is not None else self.now

        def rank(n):
            return (n in doubtful or n in far, known[n])

        def reach_of():
            if len(known) < BETA:
                return 1 << 200
            return bitlen(heapq.nsmallest(BETA, known.values())[-1])

        box = Box()
        in_flight = sent = stalled = 0
        retrying = set()
        reach = reach_of()
        why = "?"
        while True:
            if mode == "providers" and found and (stop_at_first or len(found) >= 3):
                why = "enough providers"
                break
            if self.now >= deadline:
                why = "timeout"
                break
            if sim.partition_mode(a):
                for n in [n for n in known if not sim.is_local(a, n) and n not in answered]:
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
            waiting = False
            while in_flight < alpha and sent < max_queries:
                ranked = sorted(known, key=rank)[:window]
                n = next((x for x in ranked if x not in queried), None)
                if n is None:
                    break
                if claims is not None and claims.setdefault(n, pidx) != pidx:
                    del known[n]
                    continue
                if not self.known(a, n) and n not in allowed_unknown:
                    if len(allowed_unknown) >= budget:
                        refused.add(n)
                        del known[n]
                        continue
                    if pacing and not (pacer.free(self.now) and self.ceiling_ok(a)):
                        alt = None
                        if cfg["known_first"]:
                            for x in sorted(known, key=rank)[:cfg["known_first_k"] or len(known)]:
                                if x in queried or not self.known(a, x):
                                    continue
                                if claims is not None and claims.setdefault(x, pidx) != pidx:
                                    continue
                                alt = x
                                break
                        if alt is None:
                            waiting = True
                            break
                        n = alt
                    else:
                        if pacing:
                            pacer.take(self.now)
                        allowed_unknown.add(n)
                queried.add(n)
                self.spawn(self.query(a, n, key, box, False, st))
                sent += 1
                in_flight += 1
            for n in sorted(known, key=known.__getitem__)[:window]:
                if n not in doubtful or n in queried or sent >= max_queries:
                    continue
                if self.known(a, n) and sim.may_retry(a, n, self.now):
                    if claims is not None and claims.setdefault(n, pidx) != pidx:
                        continue
                    queried.add(n)
                    retrying.add(n)
                    sent += 1
                    self.spawn(self.query(a, n, key, box, True, st))
            if in_flight == 0 and not waiting:
                why = "query cap" if sent >= max_queries else "exhausted"
                break
            until = deadline
            if waiting:
                until = min(until, max(pacer.free_at, self.now + (0.0 if self.ceiling_ok(a) else self.backoff())))
            item = yield from box.get(self, until)
            if item is None:
                continue
            n, reply, is_retry = item
            if n in retrying:
                retrying.discard(n)
            else:
                in_flight -= 1
            if reply is None:
                failed.add(n)
                known.pop(n, None)
                continue
            answered.add(n)
            doubtful.discard(n)
            provs, nodes = reply
            for m in nodes:
                if m not in known and m not in queried and usable(m):
                    add(m)
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
                            t_first = self.now
        result = None
        if mode == "closest":
            cand = [n for n in known if n in answered or (n not in queried and self.known(a, n))]
            result = sorted(cand, key=lambda n: (n in doubtful or n in far, known[n]))[:K]
        pool = [n for n in known if n in answered or (n not in queried and self.known(a, n))]
        return {"found": found, "t_first": None if t_first is None else t_first - t0, "lat": self.now - t0,
                "result": result, "why": why, "queried": len(queried), "sent": sent, "pool": pool,
                "answered": list(answered)}

    def disjoint(self, a, key, mode, st, paths, total_budget, deadline=None, stop_first=None, max_queries=MAX_QUERIES):
        """Process: one lookup over `paths` disjoint paths (rnfs.dht.disjoint.DisjointLookup)."""
        start = self.start_nodes(a, key)
        claims = {}
        budgets = split_budget(total_budget, paths)
        evs = [self.spawn(self.lookup(a, key, mode, st, budget=budgets[p], start=start[p::paths], claims=claims,
                                      pidx=p, deadline=deadline, stop_first=stop_first,
                                      max_queries=max(1, max_queries // paths)))
               for p in range(paths)]
        res = []
        for ev in evs:
            r = yield ev
            res.append(r)
        ids = self.W.ids
        pool = list(dict.fromkeys(n for r in res for n in r["pool"]))
        merged = sorted((n for r in res for n in (r["result"] or [])), key=lambda n: ids[n] ^ key)
        merged = list(dict.fromkeys(merged))[:K]
        found = list(dict.fromkeys(p for r in res for p in r["found"]))
        firsts = [r["t_first"] for r in res if r["t_first"] is not None]
        answered = list(dict.fromkeys(n for r in res for n in r["answered"]))
        return {"found": found, "t_first": min(firsts) if firsts else None, "lat": max(r["lat"] for r in res),
                "result": merged, "paths": [r["result"] or [] for r in res], "pool": pool, "answered": answered,
                "sent": sum(r["sent"] for r in res), "queried": sum(r["queried"] for r in res)}

    # Operations

    def search(self, a, key, st):
        """Process: a provider search (rnfs.dht.lookups.Search): local first, the first lookup, then the disjoint
        fallback only if nothing was found. Returns (found, latency to first provider or end)."""
        sim, W = self.sim, self.W
        t0 = self.now
        slow = W.slow(a)
        m = sim.S(a, "lf", 0)
        pre = {}
        if m and W.kind[a] >= 0:
            loc = sorted(sim.local_set(a), key=lambda j: W.ids[j] ^ key)[:m]
            for n in loc:
                reply = sim.find_reply(n, key)
                lat, ok, _ = self.link_cost(a, n, "find", len(reply[0]) + len(reply[1]), st)
                yield lat
                if not ok:
                    self.on_fail(a, n, "link")
                    continue
                self.on_answer(a, n)
                pre[n] = reply
                fl = [p for p in reply[0] if p != a]
                if fl:
                    return fl, self.now - t0
        ub = sim.S(a, "ub", None)
        budget = ub[1] if (ub and slow) else None
        r = yield from self.lookup(a, key, "providers", st, budget=budget, pre=pre)
        if r["found"]:
            return r["found"], (self.now - t0) - r["lat"] + r["t_first"]
        left_q = MAX_QUERIES - r["sent"]
        left_t = t0 + LOOKUP_TIMEOUT - self.now
        if left_q <= 0 or left_t <= 0:
            return [], self.now - t0
        t1 = self.now
        r2 = yield from self.disjoint(a, key, "providers", st, SLOW_PATHS if slow else FAST_PATHS,
                                      budget if slow else None, deadline=t0 + LOOKUP_TIMEOUT, max_queries=left_q)
        if r2["found"]:
            return r2["found"], (t1 - t0) + r2["t_first"]
        return [], self.now - t0

    def heard_servers(self, a):
        """The servers in a's announce first-seen log heard at least provider_seniority ago: every server it heard
        (an island node: its island and the announces that reach it), none on a node just started."""
        hs = self._heard_cache.get(a)
        if hs is None:
            W = self.W
            if a in self.fresh:
                hs = []
            elif W.kind[a] < 0 or W.node_heard[a] is None:
                hs = [j for j in range(W.S) if j != a]
            else:
                hs = [j for j in W.island_nodes[W.kind[a]] if j != a] + sorted(W.node_heard[a])
            self._heard_cache[a] = hs
        return hs

    def region_store_set(self, a, key, r):
        """A key's store set from its region's lookup (publisher.py _store_set, add_store_part): its K closest of what
        the region learned, each path's K closest (for the region's own key), the K closest senior servers of the
        announce first-seen log, and the local replicas."""
        sim, W = self.sim, self.W
        ids = W.ids
        dist = lambda n: ids[n] ^ key  # noqa: E731
        s = sorted(r["pool"], key=dist)[:K]
        for p in r.get("paths", []):
            s += sorted(p, key=dist)[:K]
        if self.cfg["seniors"]:
            s += heapq.nsmallest(K, self.heard_servers(a), key=dist)
        kloc = sim.S(a, "kloc", 0)
        if kloc and W.kind[a] >= 0:
            s += sorted(sim.local_set(a), key=dist)[:kloc]
        return list(dict.fromkeys(n for n in s if n != a))

    def publish_round(self, a, keys, st, max_lookups=None, out=None, estimate=None):
        """Process: one sweep publish round of the due keys (in the order given) from a, as rnfs.dht.sweep.Sweep runs
        it: a lookup takes the next key not covered and not sharing estimate - 1 leading bits with a key being looked
        up (before any region is known, estimate None, the first lookup runs alone); its region (the keys sharing
        the bits its key shares with its 10th closest server found) is covered at once. Returns {key: store set}
        for the keys covered (records are placed on the reachable ones); out gets the round's region lookups."""
        sim, W = self.sim, self.W
        ids = W.ids
        slow = W.slow(a)
        paths = SLOW_PATHS if slow else FAST_PATHS
        ub = sim.S(a, "ub", None)
        total = ub[0] if (ub and slow) else None
        parallel = 1 if (slow or len(keys) == 1) else PUBLISH_LOOKUPS
        if max_lookups is None:
            max_lookups = SLOW_ROUND_LOOKUPS if slow else 10 ** 9
        order = list(dict.fromkeys(keys))
        covered = {}
        claims = {}           # key being looked up -> bits its region probably holds
        state = {"estimate": estimate, "lookups": 0, "waiters": []}
        done_ev = Ev()
        active = [0]

        def shares(v, prefix, bits):
            return bits == 0 or (v ^ prefix) >> (128 - bits) == 0

        def take():
            # Sweep.take: None once every key is covered or the round's lookups are spent; "wait" while every key
            # left is claimed
            if state["lookups"] >= max_lookups:
                return None
            left = [k for k in order if k not in covered]
            if not left:
                return None
            for k in left:
                if k in claims:
                    continue
                if any(shares(k, c, bits) for c, bits in claims.items()):
                    continue
                claims[k] = 0 if state["estimate"] is None else max(0, state["estimate"] - 1)
                state["lookups"] += 1
                return k
            return "wait"

        def cover(x, r):
            claims.pop(x, None)
            res = r["result"]
            bits = (128 - bitlen(x ^ ids[res[K - 1]])) if len(res) >= K else (
                128 - bitlen(x ^ ids[res[-1]]) if res else 0)
            for k in order:
                if k not in covered and shares(k, x, bits):
                    covered[k] = self.region_store_set(a, k, r)
            if res:
                state["estimate"] = bits
            for ev in state["waiters"]:
                self.trigger(ev)
            state["waiters"] = []

        def worker():
            while True:
                x = take()
                if x is None:
                    break
                if x == "wait":
                    ev = Ev()
                    state["waiters"].append(ev)
                    yield ev
                    continue
                r = yield from self.disjoint(a, x, "closest", st, paths, total)
                cover(x, r)
            active[0] -= 1
            if active[0] == 0:
                self.trigger(done_ev)

        for _ in range(parallel):
            active[0] += 1
            self.spawn(worker())
        yield done_ev
        for k, targets in covered.items():
            sim.state(a).records[k].add(a)
            for n in targets:
                if W.reachable(a, n):
                    sim.state(n).records[k].add(a)
        if out is not None:
            out["lookups"] = state["lookups"]
            out["estimate"] = state["estimate"]
        return covered

    def join(self, a, st, bootstrap):
        """Process: node.py join: bootstrap nodes one at a time until BOOTSTRAP_ANSWERS answer, then a lookup for the
        node's own ID. Returns the number of servers the node then knows of."""
        sim, W = self.sim, self.W
        key = W.ids[a]
        found = []
        answered = 0
        for b in bootstrap:
            if answered >= BOOTSTRAP_ANSWERS:
                break
            if b == a:
                continue
            if not self.known(a, b):
                yield from self.acquire(a, st)
                ok = yield from self.path_request(a, b, st)
                if not ok:
                    self.on_fail(a, b, "path")
                    continue
            reply = sim.find_reply(b, key) if W.reachable(a, b) else None
            lat, ok, never = self.link_cost(a, b, "find", (len(reply[1]) if reply else 0), st)
            yield lat
            if never:
                self.rns_rerequest(a, b, st)
            if not ok:
                self.on_fail(a, b, "link")
                continue
            self.on_answer(a, b)
            answered += 1
            self.note_table(a, [b])
            found += reply[1]
        start = list(dict.fromkeys(found))
        if a in self.table_of:
            start = sorted(set(start) | self.table_of[a], key=lambda n: W.ids[n] ^ key)[:K]
        else:
            start = self.start_nodes(a, key)
        yield from self.lookup(a, key, "closest", st, start=start)
        t = self.table_of.get(a)
        return len(t) if t is not None else None
