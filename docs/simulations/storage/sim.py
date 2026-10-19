"""Provider-record store policy simulator (rnfs, one DHT server).

Records are modelled per (publisher, group): all records of one group share their last-stored time (a publisher
republishes a group's keys together), so a group is a count plus a timestamp. Policies choose victims at
publisher granularity, which is how every policy studied here decides anyway. Time is in hours.
"""
import heapq
import math
import random

DAY = 24.0
TTL = 7 * DAY

N_T, E_T, K_T = 0, 1, 2  # tiers: Newcomer, Earned, Known


class Pub:
    __slots__ = ('pid', 'honest', 'known', 'first_seen', 'ndays', 'lastday', 'tier', 'count', 'groups', 'hv',
                 'w', 'main', 'pool', 'active', 'cls', 'join_t', 'queued', 'ins_day', 'ins_cnt', 'slow',
                 'ticket', 'desired', 'size_cls', 'held_since', 'leave_t', 'promo_t')

    def __init__(self, pid, honest):
        self.pid = pid
        self.honest = honest
        self.known = False
        self.first_seen = None
        self.ndays = 0
        self.lastday = -10 ** 9
        self.tier = N_T
        self.count = 0
        self.groups = []  # [desired, present, last_store, expiry, inheap, kind, jitter]
        self.hv = 0
        self.w = 1.0
        self.main = 0
        self.pool = 0
        self.active = True
        self.cls = 'est'
        self.join_t = 0.0
        self.queued = False
        self.ins_day = -1
        self.ins_cnt = 0
        self.slow = False
        self.ticket = None
        self.desired = 0
        self.size_cls = 'small'
        self.held_since = None
        self.promo_t = None


class Stats(dict):
    def add(self, k, v=1):
        self[k] = self.get(k, 0) + v


class Base:
    name = 'base'

    def __init__(self, C, rng):
        self.C = C
        self.rng = rng
        self.total = 0
        self.exph = []
        self.seq = 0
        self.pubs = {}
        self.st = Stats()

    # ---- hooks
    def register(self, p):
        self.pubs[p.pid] = p

    def ttl_for(self, p):
        return TTL

    def _region_add(self, p, k):
        pass

    def _region_drop(self, p, k):
        pass

    def tick(self, t):
        pass

    def day(self, t):
        pass

    # ---- core accounting
    def _schedule(self, p, gi):
        g = p.groups[gi]
        if not g[4]:
            self.seq += 1
            heapq.heappush(self.exph, (g[3], self.seq, p.pid, gi))
            g[4] = True

    def _touch(self, p, gi, t):
        g = p.groups[gi]
        g[2] = t
        g[3] = t + self.ttl_for(p)
        self._schedule(p, gi)

    def _add(self, p, gi, k, t):
        g = p.groups[gi]
        g[1] += k
        p.count += k
        self.total += k
        self._region_add(p, k)
        self._touch(p, gi, t)
        self.st.add('ins_h' if p.honest else 'ins_a', k)

    def _drop(self, p, k, why):
        """Remove k records of p, least recently stored group first."""
        left = k
        if len(p.groups) == 1:
            p.groups[0][1] -= k
        else:
            for g in sorted((g for g in p.groups if g[1] > 0), key=lambda g: g[2]):
                d = min(left, g[1])
                g[1] -= d
                left -= d
                if not left:
                    break
        p.count -= k
        self.total -= k
        self._region_drop(p, k)
        self.st.add(why + ('_h' if p.honest else '_a'), k)
        if why == 'exp' and p.honest and p.active:
            self.st.add('lapse_h', k)  # an active honest publisher's record expired: a lapse

    def _drop_group(self, p, gi, k, why):
        g = p.groups[gi]
        g[1] -= k
        p.count -= k
        self.total -= k
        self._region_drop(p, k)
        self.st.add(why + ('_h' if p.honest else '_a'), k)
        if why == 'exp' and p.honest and p.active:
            self.st.add('lapse_h', k)

    def expire(self, t):
        h = self.exph
        while h and h[0][0] <= t:
            _, _, pid, gi = heapq.heappop(h)
            p = self.pubs[pid]
            g = p.groups[gi]
            g[4] = False
            if g[1] <= 0:
                continue
            if g[3] > t:
                self._schedule(p, gi)
                continue
            self._drop_group(p, gi, g[1], 'exp')

    def seen(self, p, t):
        if p.first_seen is None:
            p.first_seen = t

    def refresh(self, p, gi, t):
        g = p.groups[gi]
        if g[1] > 0:
            self.seen(p, t)
            self._touch(p, gi, t)
            self.st.add('ref_h' if p.honest else 'ref_a', g[1])
            self.after_store(p, t)

    def after_store(self, p, t):
        pass

    def store(self, p, gi, n, t):
        raise NotImplementedError

    def attacker_tick(self, ids, n_per, t, rng):
        rng.shuffle(ids)
        for p in ids:
            self.refresh(p, 0, t)
        for p in ids:
            self.store(p, 0, n_per, t)


# ------------------------------------------------------------------------------------------------ IPFS-like
class IPFSStore(Base):
    """Global capacity, no per-publisher caps; when full evict LRU (oldest last-stored group) or a random record."""

    def __init__(self, C, rng, evict='lru'):
        super().__init__(C, rng)
        self.evict = evict
        self.name = 'B-ipfs-' + evict
        self.lru = []  # (last_store, seq, pid, gi)
        self.live = []  # pubs with count>0 (lazy list, for random)

    def _touch(self, p, gi, t):
        super()._touch(p, gi, t)
        if self.evict == 'lru':
            self.seq += 1
            heapq.heappush(self.lru, (t, self.seq, p.pid, gi))
            if len(self.lru) > 8 * len(self.pubs) + 1000:
                self.lru = [(q.groups[j][2], 0, q.pid, j) for q in self.pubs.values()
                            for j, g in enumerate(q.groups) if g[1] > 0]
                heapq.heapify(self.lru)

    def _evict_lru(self, k):
        done = 0
        while done < k and self.lru:
            ts, _, pid, gi = self.lru[0]
            p = self.pubs[pid]
            g = p.groups[gi]
            if g[1] <= 0 or g[2] != ts:
                heapq.heappop(self.lru)
                continue
            d = min(k - done, g[1])
            self._drop_group(p, gi, d, 'ev')
            done += d
            if g[1] <= 0:
                heapq.heappop(self.lru)
        return done

    def _evict_random(self, k):
        pubs = [q for q in self.pubs.values() if q.count > 0]
        tot = self.total
        if tot <= 0:
            return 0
        done = 0
        for q in pubs:
            x = q.count * k / tot
            d = int(x)
            if self.rng.random() < x - d:
                d += 1
            d = min(d, q.count)
            if d:
                self._drop(q, d, 'ev')
                done += d
        return done

    def store(self, p, gi, n, t):
        if n <= 0:
            return 0
        self.seen(p, t)
        room = self.C - self.total
        if room < n:
            if self.evict == 'lru':
                self._evict_lru(n - room)
            else:
                self._evict_random(n - room)
        k = min(n, self.C - self.total)
        if k > 0:
            self._add(p, gi, k, t)
        if p.honest and n - k > 0:
            self.st.add('refused_h', n - k)
        return k

    def attacker_tick(self, ids, n_per, t, rng):
        for p in ids:
            self.refresh(p, 0, t)
        if self.evict == 'random':
            # bulk: all new attacker records evict random records at once
            n = n_per * len(ids)
            room = self.C - self.total
            if room < n:
                self._evict_random(min(n - room, self.total))
            room = self.C - self.total
            per = room // len(ids) if ids else 0
            extra = room - per * len(ids)
            for i, p in enumerate(ids):
                k = per + (1 if i < extra else 0)
                if k:
                    self._add(p, 0, k, t)
        else:
            for p in ids:
                self.store(p, 0, n_per, t)


# ------------------------------------------------------------------------------------------------ today (A)
class TodayStore(Base):
    """rnfs 0.1.x: main area + newcomer pool (10%), per-publisher cap C/100 once seen 1 day (C/1000 before)."""
    name = 'A-today'

    def __init__(self, C, rng, seniority=DAY):
        super().__init__(C, rng)
        self.pool_size = C // 10
        self.main_size = C - self.pool_size
        self.main_n = 0
        self.pool_n = 0
        self.cap_sen = max(10, C // 100)
        self.cap_new = max(10, self.cap_sen // 10)
        self.seniority = seniority
        self.newest = []  # (-first_seen, pid) of pubs with main>0
        self.poolh = []  # (-pool, r, pid, pool)

    def _push_newest(self, p):
        heapq.heappush(self.newest, (-p.first_seen, p.pid))
        if len(self.newest) > 3 * len(self.pubs) + 1000:
            self.newest = [(-q.first_seen, q.pid) for q in self.pubs.values() if q.main > 0]
            heapq.heapify(self.newest)

    def _push_pool(self, p):
        if p.pool > 0:
            heapq.heappush(self.poolh, (-p.pool, self.rng.random(), p.pid, p.pool))
            if len(self.poolh) > 3 * len(self.pubs) + 1000:
                self.poolh = [(-q.pool, self.rng.random(), q.pid, q.pool) for q in self.pubs.values() if q.pool > 0]
                heapq.heapify(self.poolh)

    def _region_drop(self, p, k):
        # take from pool first
        d = min(k, p.pool)
        if d:
            p.pool -= d
            self.pool_n -= d
            self._push_pool(p)
        r = k - d
        if r:
            p.main -= r
            self.main_n -= r

    def _newest_main(self, excl):
        aside = []
        found = None
        h = self.newest
        while h:
            nf, pid = h[0]
            q = self.pubs[pid]
            if q.main <= 0:
                heapq.heappop(h)
                continue
            if pid == excl:
                aside.append(heapq.heappop(h))
                continue
            found = q
            break
        for e in aside:
            heapq.heappush(h, e)
        return found

    def _most_pooled(self, excl):
        aside = []
        found = None
        h = self.poolh
        while h:
            np_, r, pid, cnt = h[0]
            q = self.pubs[pid]
            if q.pool != cnt or cnt <= 0:
                heapq.heappop(h)
                continue
            if pid == excl:
                aside.append(heapq.heappop(h))
                continue
            found = q
            break
        for e in aside:
            heapq.heappush(h, e)
        return found

    def refresh(self, p, gi, t):
        g = p.groups[gi]
        if g[1] > 0:
            super().refresh(p, gi, t)
            if p.pool > 0 and self.main_n < self.main_size:
                # pooled records republished while the rest has room leave the pool (approx: those of this group)
                m = min(p.pool, g[1], self.main_size - self.main_n)
                p.pool -= m
                self.pool_n -= m
                p.main += m
                self.main_n += m
                self._push_pool(p)
                if p.main == m:
                    self._push_newest(p)

    def store(self, p, gi, n, t):
        if n <= 0:
            return 0
        cap = self.cap_sen if (p.first_seen is not None and t - p.first_seen >= self.seniority) else self.cap_new
        want = n
        n = min(n, cap - p.count)
        acc = 0
        while acc < n:
            need = n - acc
            if self.main_n < self.main_size:
                k = min(need, self.main_size - self.main_n)
                self._put(p, gi, k, t, main=True)
                acc += k
                continue
            if p.first_seen is not None:
                v = self._newest_main(p.pid)
                if v is not None and p.first_seen < v.first_seen:
                    k = min(need, v.main)
                    self._drop_main(v, k)
                    self._put(p, gi, k, t, main=True)
                    acc += k
                    continue
            if self.pool_n < self.pool_size:
                k = min(need, self.pool_size - self.pool_n)
                self._put(p, gi, k, t, main=False)
                acc += k
                continue
            v = self._most_pooled(p.pid)
            if v is None or p.pool >= v.pool:
                break
            k = max(1, min(need, (v.pool - p.pool) // 2))
            self._drop_pool(v, k)
            self._put(p, gi, k, t, main=False)
            acc += k
        if p.honest and want - acc > 0:
            self.st.add('refused_h', want - acc)
        return acc

    def _put(self, p, gi, k, t, main):
        if p.first_seen is None:
            p.first_seen = t
        if main:
            p.main += k
            self.main_n += k
            if p.main == k:
                self._push_newest(p)
        else:
            p.pool += k
            self.pool_n += k
            self._push_pool(p)
        self._add(p, gi, k, t)

    def _drop_main(self, v, k):
        # evict k main records of v (least recently stored first), bypassing pool-first rule
        v.main -= k
        self.main_n -= k
        left = k
        for g in sorted((g for g in v.groups if g[1] > 0), key=lambda g: g[2]):
            d = min(left, g[1])
            g[1] -= d
            left -= d
            if not left:
                break
        v.count -= k
        self.total -= k
        self.st.add('ev_h' if v.honest else 'ev_a', k)

    def _drop_pool(self, v, k):
        v.pool -= k
        self.pool_n -= k
        self._push_pool(v)
        left = k
        for g in sorted((g for g in v.groups if g[1] > 0), key=lambda g: g[2]):
            d = min(left, g[1])
            g[1] -= d
            left -= d
            if not left:
                break
        v.count -= k
        self.total -= k
        self.st.add('ev_h' if v.honest else 'ev_a', k)

    def seen(self, p, t):
        pass  # first-seen is set at the first successful store (as in providers.py)


# ------------------------------------------------------------------------------------------------ tiers (C, D)
class TierStore(Base):
    """Known / Earned / Newcomer tiers with floors; see module docstring of run.py for options."""

    def __init__(self, C, rng, floors=(0.10, 0.60, 0.30), newcomer_ttl=4 * DAY, earned=('days', 3),
                 within_e='maxmin', within_n='maxmin', weight_cap=60.0, s2=False, promo=None, newcomer_cap=None,
                 newcomer_rate=None, ticket=None, name='C', senior=None, within_s='maxmin', weight_exp=1.0,
                 reclaim='fair', recent_days=30.0):
        super().__init__(C, rng)
        self.name = name
        # senior: None (3 tiers N/E/K) or days in Earned after which a publisher becomes Senior (4 tiers N/E/S/K)
        self.senior = senior
        self.nt = 4 if senior is not None else 3
        assert len(floors) == self.nt
        self.KT = self.nt - 1
        self.ST = 2 if senior is not None else None
        self.floor = [f * C for f in floors]
        self.occ = [0] * self.nt
        self.newttl = newcomer_ttl
        self.earned = earned
        self.within = [within_n, within_e] + ([within_s] if senior is not None else []) + ['maxmin']
        self.wexp = weight_exp
        self.wcap = weight_cap
        self.s2 = s2
        self.promo = promo  # None or (min_per_day, frac_of_earned_per_day)
        self.tokens = 0.0
        self.queue = []  # heap (eligible_t, pid)
        self.n_earned = 0
        self.ncap = newcomer_cap
        self.nrate = newcomer_rate
        self.ticket = ticket  # None or wait hours for a never-held newcomer when the store is full
        self.hp = [[] for _ in range(self.nt)]
        self.members = [set() for _ in range(self.nt)]  # pids with count>0 per tier
        # reclaim: how a tier below its floor picks victims in a tier above its floor (S2): 'fair' = that tier's
        # own rule, 'newest' = the publisher that entered that tier most recently first
        self.reclaim = reclaim
        self.nh = [[] for _ in range(self.nt)]
        # 'recentfair': largest holder among publishers promoted within recent_days, else largest holder overall
        self.recent = recent_days * DAY
        self.rh = [[] for _ in range(self.nt)]
        self.now = 0.0

    def register(self, p):
        super().register(p)
        if p.known:
            p.tier = self.KT

    def ttl_for(self, p):
        return self.newttl if p.tier == N_T else TTL

    def _key(self, p):
        r = self.within[p.tier]
        if r == 'seniority':
            return -p.first_seen
        return -(p.count / p.w)

    def _push(self, p):
        if self.reclaim == 'recentfair' and p.count > 0 and p.promo_t is not None and \
                self.now - p.promo_t < self.recent and p.tier != N_T:
            h = self.rh[p.tier]
            heapq.heappush(h, (-p.count, self.rng.random(), p.pid, p.count))
            if len(h) > 4 * len(self.members[p.tier]) + 2000:
                h = [(-q.count, self.rng.random(), q.pid, q.count) for q in map(self.pubs.get, self.members[p.tier])
                     if q.promo_t is not None and self.now - q.promo_t < self.recent]
                heapq.heapify(h)
                self.rh[p.tier] = h
        if p.count > 0:
            p.hv += 1
            h = self.hp[p.tier]
            heapq.heappush(h, (self._key(p), self.rng.random(), p.pid, p.hv))
            if len(h) > 4 * len(self.members[p.tier]) + 2000:
                self._rebuild(p.tier)

    def _rebuild(self, t):
        h = []
        for pid in self.members[t]:
            q = self.pubs[pid]
            q.hv += 1
            h.append((self._key(q), self.rng.random(), pid, q.hv))
        heapq.heapify(h)
        self.hp[t] = h

    def _nkey(self, p):
        return p.promo_t if p.promo_t is not None else (p.first_seen if p.first_seen is not None else 0.0)

    def _npush(self, p):
        if self.reclaim == 'newest':
            h = self.nh[p.tier]
            heapq.heappush(h, (-self._nkey(p), p.pid))
            if len(h) > 4 * len(self.members[p.tier]) + 2000:
                h = [(-self._nkey(self.pubs[q]), q) for q in self.members[p.tier]]
                heapq.heapify(h)
                self.nh[p.tier] = h

    def _evict_recentfair(self, u, kmax):
        h = self.rh[u]
        while h:
            _, _, pid, cnt = h[0]
            q = self.pubs[pid]
            if q.tier != u or q.count != cnt or cnt <= 0 or self.now - q.promo_t >= self.recent:
                heapq.heappop(h)
                continue
            # second largest recent holder bounds how much to take at once
            k = min(kmax, max(1, (q.count + 1) // 2))
            self._drop(q, k, 'ev')
            return k
        return self._evict_lower(u, kmax)

    def _evict_newest(self, u, kmax):
        h = self.nh[u]
        while h:
            nk, pid = h[0]
            q = self.pubs[pid]
            if q.tier != u or q.count <= 0 or -nk != self._nkey(q):
                heapq.heappop(h)
                continue
            k = min(kmax, q.count)
            self._drop(q, k, 'ev')
            return k
        return 0

    def _region_add(self, p, k):
        self.occ[p.tier] += k
        if p.count == k:
            self.members[p.tier].add(p.pid)
            self._npush(p)
        if self.within[p.tier] != 'seniority' or p.count == k:
            self._push(p)

    def _region_drop(self, p, k):
        self.occ[p.tier] -= k
        if p.count <= 0:
            self.members[p.tier].discard(p.pid)
            p.hv += 1
        elif self.within[p.tier] != 'seniority':
            self._push(p)

    def _valid(self, e, t):
        q = self.pubs[e[2]]
        return q.hv == e[3] and q.tier == t and q.count > 0

    def _top2(self, t, excl):
        h = self.hp[t]
        held = []
        res = []
        while h and len(res) < 2:
            e = h[0]
            if not self._valid(e, t):
                heapq.heappop(h)
                continue
            e = heapq.heappop(h)
            held.append(e)
            if e[2] != excl:
                res.append(e)
        for e in held:
            heapq.heappush(h, e)
        return res

    def _evict_prop(self, t, k, excl):
        pids = [pid for pid in self.members[t] if pid != excl]
        tot = sum(self.pubs[pid].count for pid in pids)
        if tot <= 0:
            return 0
        k = min(k, tot)
        done = 0
        for pid in pids:
            q = self.pubs[pid]
            x = q.count * k / tot
            d = int(x)
            if self.rng.random() < x - d:
                d += 1
            d = min(d, q.count, k - done)
            if d:
                self._drop(q, d, 'ev')
                done += d
        return done

    def _evict_lower(self, u, kmax):
        """Evict up to kmax from tier u (no acceptance test). Returns evicted count."""
        r = self.within[u]
        if r == 'random':
            return self._evict_prop(u, kmax, None)
        res = self._top2(u, None)
        if not res:
            return 0
        v = self.pubs[res[0][2]]
        if r == 'seniority':
            k = min(kmax, v.count)
        else:
            x2 = -res[1][0] if len(res) > 1 else 0.0
            k = min(kmax, max(1, v.count - int(math.ceil(x2 * v.w))))
        self._drop(v, k, 'ev')
        return k

    def _evict_same(self, p, need):
        t = p.tier
        r = self.within[t]
        if r == 'none':  # no pre-emption inside the tier: first come, first kept
            return 0
        if r == 'random':
            return self._evict_prop(t, need, p.pid)
        res = self._top2(t, p.pid)
        if not res:
            return 0
        v = self.pubs[res[0][2]]
        if r == 'seniority':
            if p.first_seen < v.first_seen:
                k = min(need, v.count)
                self._drop(v, k, 'ev')
                return k
            return 0
        xv = v.count / v.w
        if (p.count + 1) / p.w > xv + 1e-9:
            return 0
        keq = int((v.count * p.w - p.count * v.w) / (v.w + p.w))
        k = max(1, keq)
        if len(res) > 1:
            x2 = -res[1][0]
            k = min(k, max(1, v.count - int(math.ceil(x2 * v.w))))
        k = min(k, need)
        self._drop(v, k, 'ev')
        return k

    def seen(self, p, t):
        if p.first_seen is None:
            p.first_seen = t

    def after_store(self, p, t):
        if p.count <= 0:
            return
        if p.held_since is None:
            p.held_since = t
        d = int(t // DAY)
        if d != p.lastday:
            p.lastday = d
            p.ndays += 1
        if p.tier == E_T and self.senior is not None:
            self._check_senior(p, t)
        if p.tier == N_T and not p.queued and self._eligible(p, t):
            if self.promo is None:
                self._promote(p, t)
            else:
                p.queued = True
                heapq.heappush(self.queue, (t, p.pid))

    def _eligible(self, p, t):
        kind, x = self.earned
        if kind == 'none':  # no Earned tier: everyone not Known is a newcomer
            return False
        if kind == 'days':
            return p.ndays >= x
        if kind == 'age':  # first seen at least x hours ago (today's seniority test)
            return t - p.first_seen >= x
        if kind == 'held':  # has held records for x hours and refreshed on at least 2 days
            return t - p.held_since >= x and p.ndays >= 2
        raise ValueError(kind)

    def _move(self, p, nt, t):
        c = p.count
        self.occ[p.tier] -= c
        self.members[p.tier].discard(p.pid)
        p.tier = nt
        p.w = self._weight(p, t)
        self.occ[nt] += c
        if c > 0:
            self.members[nt].add(p.pid)
        p.hv += 1
        self._push(p)
        if c > 0:
            self._npush(p)

    def _promote(self, p, t):
        self.now = t
        p.queued = False
        p.promo_t = t
        self.n_earned += 1
        self._move(p, E_T, t)
        for gi, g in enumerate(p.groups):
            if g[1] > 0:
                g[3] = g[2] + TTL

    def _check_senior(self, p, t):
        if self.senior is not None and p.tier == E_T and p.promo_t is not None and t - p.promo_t >= self.senior * DAY:
            self._move(p, self.ST, t)

    def _weight(self, p, t):
        if self.within[E_T] == 'weighted' and p.tier == E_T:
            return max(1.0, min(self.wcap, (t - p.first_seen) / DAY)) ** self.wexp
        if self.reclaim == 'aged' and p.tier == E_T:
            # reclaim order only (no pre-emption inside Earned): count / (days since promotion)^wexp
            pt = p.promo_t if p.promo_t is not None else p.first_seen
            return max(1.0, min(self.wcap, (t - pt) / DAY)) ** self.wexp
        return 1.0

    def tick(self, t):
        self.now = t
        if self.promo is not None:
            mn, fr = self.promo
            self.tokens = min(self.tokens + max(mn, fr * self.n_earned) / DAY, max(mn, fr * self.n_earned))
            while self.queue and self.tokens >= 1:
                _, pid = heapq.heappop(self.queue)
                p = self.pubs[pid]
                if p.tier != N_T:
                    continue
                if p.count <= 0:  # lost its records while waiting: back to counting
                    p.queued = False
                    continue
                self.tokens -= 1
                self._promote(p, t)

    def day(self, t):
        if self.senior is not None:
            for pid in list(self.members[E_T]):
                self._check_senior(self.pubs[pid], t)
        if self.within[E_T] == 'weighted' or self.reclaim == 'aged':
            for pid in self.members[E_T]:
                q = self.pubs[pid]
                q.w = self._weight(q, t)
            self._rebuild(E_T)

    def store(self, p, gi, n, t):
        self.now = t
        if n <= 0:
            return 0
        self.seen(p, t)
        want = n
        tt = p.tier
        if tt == N_T:
            if self.ncap is not None:
                n = min(n, self.ncap - p.count)
            if self.nrate is not None:
                d = int(t // DAY)
                if p.ins_day != d:
                    p.ins_day, p.ins_cnt = d, 0
                n = min(n, self.nrate - p.ins_cnt)
            if self.ticket is not None and p.count == 0 and self.total >= self.C:
                if p.ticket is None:
                    p.ticket = t + self.ticket
                    n = 0
                elif t < p.ticket:
                    n = 0
        acc = 0
        while acc < n:
            need = n - acc
            room = self.C - self.total
            if room > 0:
                k = min(need, room)
                self._add(p, gi, k, t)
                acc += k
                continue
            got = 0
            for u in range(0, tt):
                over = self.occ[u] - self.floor[u]
                if over >= 1:
                    got = self._evict_lower(u, min(need, int(over)))
                    if got:
                        break
            if not got and self.s2 and self.occ[tt] < self.floor[tt]:
                for u in range(tt + 1, self.nt):
                    over = self.occ[u] - self.floor[u]
                    if over >= 1:
                        kmax = min(need, int(over), int(self.floor[tt] - self.occ[tt]) + 1)
                        if self.reclaim == 'newest':
                            got = self._evict_newest(u, kmax)
                        elif self.reclaim == 'recentfair':
                            got = self._evict_recentfair(u, kmax)
                        else:
                            got = self._evict_lower(u, kmax)
                        if got:
                            break
            if not got:
                got = self._evict_same(p, need)
            if not got:
                break
            self._add(p, gi, got, t)
            acc += got
        if tt == N_T and self.nrate is not None:
            p.ins_cnt += acc
        if acc:
            self.after_store(p, t)
        if p.honest and want - acc > 0:
            self.st.add('refused_h', want - acc)
        return acc
