"""Driver: honest population, attacker, metrics. run_one(cfg) -> summary dict."""
import heapq
import math
import random

from sim import DAY, TTL, N_T, E_T, K_T, Pub, IPFSStore, TodayStore, TierStore

DEFAULT = dict(
    seed=1,
    n_pub=1000,            # established honest publishers on this server
    load=0.5,              # honest records / capacity
    known_frac=0.03,
    slow_frac=0.3,
    name_frac=0.2,         # publishers keeping 1-3 names (separate 72h +-10% schedule)
    join_rate=0.01,        # new honest publishers per day, as a fraction of n_pub
    leave_rate=0.01,       # per publisher per day
    mirror_frac=0.01,
    fast_delay=1.0,        # republish delay (h) on top of the interval: fast nodes
    slow_delay=6.0,        # slow nodes (lookup pacing)
    warm=7,                # days before the attack
    attack_days=7,         # None: no attack; 'cont': until the end
    after=10,              # days simulated after the attack ends
    cont_days=21,          # length of a continuous attack
    n_att=1000,
    att_rate='max',        # 'max' (C records/h) or a number of records/h
    preaged=None,          # None (fresh) or pre-age in days
    policy=('tier', {}),
    mirror_join=None,      # (day, records, slow) a big honest mirror joining
    series=False,
)


def gen_count(rng, mirror_frac):
    r = rng.random()
    if r < 0.85:
        return 1 + int(rng.expovariate(1 / 4.0))
    if r < 1 - mirror_frac:
        return int(10 * 30 ** rng.random())
    return int(1000 * 10 ** rng.random())


def make_engine(cfg, C, rng):
    kind, opts = cfg['policy']
    opts = dict(opts)
    if 'newcomer_cap_frac' in opts:
        opts['newcomer_cap'] = max(10, int(C * opts.pop('newcomer_cap_frac')))
    if kind == 'today':
        return TodayStore(C, rng, **opts)
    if kind == 'ipfs':
        return IPFSStore(C, rng, **opts)
    if kind == 'tier':
        return TierStore(C, rng, **opts)
    raise ValueError(kind)


def interval(p, g, rng, cfg):
    delay = rng.uniform(0, cfg['slow_delay'] if p.slow else cfg['fast_delay'])
    if g[5] == 'n':
        return 72 * (1 + g[6]) + delay
    return (72.0 if p.slow else 24.0) + delay


def new_pub(pid, rng, cfg, count, cls, t):
    p = Pub(pid, True)
    p.cls = cls
    p.join_t = t
    p.slow = rng.random() < cfg['slow_frac']
    p.groups.append([count, 0, 0.0, 0.0, False, 'c', 0.0])
    if rng.random() < cfg['name_frac']:
        for _ in range(rng.randint(1, 3)):
            p.groups.append([1, 0, 0.0, 0.0, False, 'n', rng.uniform(-0.1, 0.1)])
    p.desired = sum(g[0] for g in p.groups)
    p.size_cls = 'small' if p.desired <= 20 else 'large'
    return p


def run_one(cfg0):
    cfg = dict(DEFAULT)
    cfg.update(cfg0)
    rng = random.Random(cfg['seed'])
    erng = random.Random(cfg['seed'] * 7919 + 1)
    # ---- population
    pubs = []
    pid = 0
    for i in range(cfg['n_pub']):
        p = new_pub(pid, rng, cfg, gen_count(rng, cfg['mirror_frac']), 'est', 0.0)
        pid += 1
        pubs.append(p)
    honest0 = sum(p.desired for p in pubs)
    C = max(100, int(round(honest0 / cfg['load'])))
    eng = make_engine(cfg, C, erng)
    nk = max(1, int(round(cfg['known_frac'] * cfg['n_pub'])))
    for p in rng.sample(pubs, nk):
        p.known = True
        p.cls = 'known'
    warm = cfg['warm'] * DAY
    ad = cfg['attack_days']
    if ad is None:
        t_a = t_ae = None
        horizon = warm + cfg['after'] * DAY
    else:
        t_a = warm
        t_ae = warm + (cfg['cont_days'] if ad == 'cont' else ad) * DAY
        horizon = t_ae + (0 if ad == 'cont' else cfg['after'] * DAY)
    ev = []  # (t, seq, kind, pid, gi)
    seq = 0
    allp = {}

    def sched(t, kind, p, gi=0):
        nonlocal seq
        seq += 1
        heapq.heappush(ev, (t, seq, kind, p.pid, gi))

    # established: aged, already Earned, records in place with steady-state phases
    for p in pubs:
        p.first_seen = -rng.uniform(10, 365) * DAY
        p.ndays = 99
        p.lastday = -1
        if p.known:
            p.tier = eng.KT if isinstance(eng, TierStore) else K_T
        elif isinstance(eng, TierStore) and eng.earned[0] != 'none':
            p.tier = eng.ST if eng.senior is not None else E_T
            p.promo_t = p.first_seen + 3 * DAY
            eng.n_earned += 1
        allp[p.pid] = p
        eng.register(p)
        if isinstance(eng, TierStore):
            p.w = eng._weight(p, 0.0)
        p.leave_t = rng.expovariate(cfg['leave_rate']) * DAY if cfg['leave_rate'] > 0 else 1e18
    # initial fill, oldest publishers first (as they would have arrived)
    for p in sorted(pubs, key=lambda q: q.first_seen):
        for gi, g in enumerate(p.groups):
            I = interval(p, g, rng, cfg)
            last = -rng.uniform(0, I)
            eng.store(p, gi, g[0], last)
            sched(last + I, 'R', p, gi)
    eng.expire(0.0)
    for k in list(eng.st):
        eng.st[k] = 0
    # joins
    t = 0.0
    jr = cfg['join_rate'] * cfg['n_pub'] / DAY
    joins = []
    while jr > 0:
        t += rng.expovariate(jr)
        if t >= horizon:
            break
        joins.append(t)
    for tj in joins:
        p = new_pub(pid, rng, cfg, gen_count(rng, cfg['mirror_frac']), 'new', tj)
        pid += 1
        if t_a is not None and tj >= t_a:
            p.cls = 'newatk'
        p.leave_t = tj + (rng.expovariate(cfg['leave_rate']) * DAY if cfg['leave_rate'] > 0 else 1e18)
        allp[p.pid] = p
        eng.register(p)
        for gi in range(len(p.groups)):
            sched(tj, 'R', p, gi)
    if cfg['mirror_join']:
        dj, size, slow = cfg['mirror_join']
        if isinstance(size, str):
            size = int(C * float(size.split(':')[1]))
        p = Pub(pid, True)
        pid += 1
        p.cls = 'mirror'
        p.slow = slow
        p.join_t = dj * DAY
        p.groups.append([size, 0, 0.0, 0.0, False, 'c', 0.0])
        p.desired = size
        p.size_cls = 'large'
        p.leave_t = 1e18
        allp[p.pid] = p
        eng.register(p)
        sched(dj * DAY, 'R', p, 0)
    # attacker identities
    att = []
    if t_a is not None and cfg['n_att'] > 0:
        pre_slots = 0
        if isinstance(eng, TierStore) and eng.promo is not None and cfg['preaged'] is not None:
            # promotions are rate-limited: only this many identities could have been promoted while pre-aging
            mn, fr = eng.promo
            pre_slots = int(max(mn, fr * eng.n_earned) * cfg['preaged'])
        for i in range(cfg['n_att']):
            q = Pub(pid, False)
            pid += 1
            q.cls = 'att'
            q.groups.append([0, 0, 0.0, 0.0, False, 'a', 0.0])
            q.active = False
            if cfg['preaged'] is not None:
                q.first_seen = t_a - cfg['preaged'] * DAY + i * 1e-6
                q.ndays = 99
                q.held_since = q.first_seen
                if isinstance(eng, TierStore) and eng.earned[0] != 'none':
                    if eng.promo is None or i < pre_slots:
                        q.tier = E_T
                        # promoted after 3 distinct days; with a promotion limit, spread over the pre-age period
                        q.promo_t = q.first_seen + 2 * DAY
                        if eng.promo is not None and pre_slots:
                            q.promo_t = q.first_seen + 2 * DAY + (i / pre_slots) * max(0.0, cfg['preaged'] - 2) * DAY
                        if eng.senior is not None and t_a - q.promo_t >= eng.senior * DAY:
                            q.tier = eng.ST
                        eng.n_earned += 1
            allp[q.pid] = q
            eng.register(q)
            if isinstance(eng, TierStore):
                q.w = eng._weight(q, t_a)
            att.append(q)
    rate = C if cfg['att_rate'] == 'max' else (11000 if cfg['att_rate'] == 'lora' else cfg['att_rate'])
    n_per = max(1, int(math.ceil(rate / max(1, len(att))))) if att else 0

    honest = [p for p in allp.values() if p.honest]
    classes = ['known', 'est', 'new', 'newatk', 'mirror']
    series = []
    st = eng.st
    prev = dict(st)
    nh = int(horizon)
    for h in range(0, nh):
        th = float(h)
        eng.expire(th)
        if h % 24 == 0:
            eng.day(th)
        eng.tick(th)
        if att and t_a <= th < t_ae:
            eng.attacker_tick(att, n_per, th, erng)
        while ev and ev[0][0] < th + 1:
            te, _, kind, ppid, gi = heapq.heappop(ev)
            p = allp[ppid]
            if te < 0:
                continue
            if te >= p.leave_t:
                p.active = False
                continue
            eng.expire(te)
            g = p.groups[gi]
            eng.refresh(p, gi, te)
            miss = g[0] - g[1]
            if miss > 0:
                eng.store(p, gi, miss, te)
            sched(te + interval(p, g, rng, cfg), 'R', p, gi)
        # metrics at end of hour
        row = {'h': h}
        for c in classes:
            row[c] = [0, 0]
        row['est_small'] = [0, 0]
        row['est_large'] = [0, 0]
        pw = [0.0, 0]
        for p in honest:
            if not p.active or p.join_t > th:
                continue
            if p.leave_t <= th:
                p.active = False
                continue
            r = row[p.cls]
            r[0] += p.count
            r[1] += p.desired
            if p.cls == 'est':
                r2 = row['est_' + p.size_cls]
                r2[0] += p.count
                r2[1] += p.desired
                pw[0] += p.count / p.desired
                pw[1] += 1
        row['est_pw'] = pw[0] / pw[1] if pw[1] else float('nan')
        row['att'] = sum(q.count for q in att)
        row['total'] = eng.total
        d = {k: st.get(k, 0) - prev.get(k, 0) for k in st}
        prev = dict(st)
        row['d'] = d
        series.append(row)
    return summarize(cfg, C, series, t_a, t_ae, eng), (series if cfg['series'] else None)


def frac(r):
    return r[0] / r[1] if r[1] else float('nan')


def summarize(cfg, C, series, t_a, t_ae, eng):
    out = {'C': C}

    def avg(lo, hi, key, f=frac):
        xs = [f(r[key]) if f else r[key] for r in series if lo <= r['h'] < hi]
        xs = [x for x in xs if x == x]
        return sum(xs) / len(xs) if xs else float('nan')

    def mn(lo, hi, key, f=frac):
        xs = [f(r[key]) if f else r[key] for r in series if lo <= r['h'] < hi]
        xs = [x for x in xs if x == x]
        return min(xs) if xs else float('nan')

    def allh(r):
        a = sum(r[c][0] for c in ('known', 'est', 'new', 'newatk', 'mirror'))
        b = sum(r[c][1] for c in ('known', 'est', 'new', 'newatk', 'mirror'))
        return a / b if b else float('nan')

    for r in series:
        r['all'] = allh(r)
    H = len(series)

    def wsum(lo, hi, keys):
        return sum(sum(r['d'].get(k, 0) for k in keys) for r in series if lo <= r['h'] < hi)

    wkeys = ['ins_h', 'ins_a', 'ev_h', 'ev_a', 'exp_h', 'exp_a']
    rkeys = ['ref_h', 'ref_a']
    if t_a is None:
        lo = 24
        out['peace_all'] = avg(lo, H, 'all', None)
        for c in ('known', 'est', 'new', 'mirror'):
            out['peace_' + c] = avg(lo, H, c)
        out['peace_min_new'] = mn(lo, H, 'new')
        out['refused_h_per_day'] = wsum(lo, H, ['refused_h']) / ((H - lo) / DAY)
        out['evicted_h_per_day'] = wsum(lo, H, ['ev_h']) / ((H - lo) / DAY)
        out['lapse_h'] = wsum(lo, H, ['lapse_h'])
        out['writes_s'] = wsum(lo, H, wkeys) / ((H - lo) * 3600)
        out['refresh_s'] = wsum(lo, H, rkeys) / ((H - lo) * 3600)
        if cfg['mirror_join']:
            dj = cfg['mirror_join'][0] * DAY
            out['mirror_1d'] = avg(dj + 20, dj + 26, 'mirror')
            out['mirror_end'] = avg(H - 24, H, 'mirror')
            out['est_after_mirror'] = avg(dj + 24, H, 'est')
            out['evicted_h_after_mirror'] = wsum(dj, H, ['ev_h'])
        return out
    a, e = int(t_a), int(t_ae)
    out['base_all'] = avg(a - 24, a, 'all', None)
    for c in ('known', 'est', 'new', 'est_small', 'est_large'):
        out['base_' + c] = avg(a - 24, a, c)
    out['base_est_pw'] = avg(a - 24, a, 'est_pw', None)
    out['pre_refused_h_day'] = wsum(24, a, ['refused_h']) / ((a - 24) / DAY)
    for c in ('all',):
        out['att_mean_' + c] = avg(a, e, c, None)
        out['att_min_' + c] = mn(a, e, c, None)
    for c in ('known', 'est', 'est_small', 'est_large', 'new', 'newatk'):
        out['att_mean_' + c] = avg(a, e, c)
        out['att_min_' + c] = mn(a, e, c)
    out['att_mean_est_pw'] = avg(a, e, 'est_pw', None)
    out['att_min_est_pw'] = mn(a, e, 'est_pw', None)
    out['att_last_est'] = avg(e - 24, e, 'est')
    out['att_last_newatk'] = avg(e - 24, e, 'newatk')
    out['att_last_new'] = avg(e - 24, e, 'new')
    out['att_share_end'] = series[e - 1]['att'] / C
    t90 = t50 = None
    for r in series:
        if a <= r['h'] < e:
            f = frac(r['est'])
            if t90 is None and f < 0.9:
                t90 = r['h'] - a
            if t50 is None and f < 0.5:
                t50 = r['h'] - a
    out['t90_est_h'] = t90
    out['t50_est_h'] = t50
    rec = None
    if e < H:
        target = out['base_all'] - 0.01
        for r in series:
            if r['h'] >= e and r['all'] >= target:
                rec = r['h'] - e
                break
        out['post_all_end'] = avg(H - 24, H, 'all', None)
        out['post_att_left'] = series[-1]['att'] / C
    out['recover_h'] = rec
    out['writes_s_att'] = wsum(a, e, wkeys) / ((e - a) * 3600)
    out['writes_s_att_h'] = wsum(a, e, ['ins_h', 'ev_h', 'exp_h']) / ((e - a) * 3600)
    out['refresh_s_att'] = wsum(a, e, rkeys) / ((e - a) * 3600)
    out['writes_s_peace'] = wsum(24, a, wkeys) / ((a - 24) * 3600)
    out['refresh_s_peace'] = wsum(24, a, rkeys) / ((a - 24) * 3600)
    out['lapse_h_att'] = wsum(a, H, ['lapse_h'])
    return out


if __name__ == '__main__':
    import sys, time, json
    t0 = time.time()
    s, _ = run_one(dict(policy=('tier', {}), n_att=1000, attack_days=7))
    print(json.dumps(s, indent=1), time.time() - t0)
