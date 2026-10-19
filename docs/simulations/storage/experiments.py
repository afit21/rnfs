"""Experiment batteries. Usage: python experiments.py E1 [E2 ...]. Appends tables to RESULTS.md, raw rows to raw/."""
import json
import os
import sys
import time
from multiprocessing import Pool

import run

D = 24.0
HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, 'RESULTS.md')
RAW = os.path.join(HERE, 'raw')
os.makedirs(RAW, exist_ok=True)


def tier(floors_kEn='30/60/10', exp=4.0, s2=True, **kw):
    k, e, n = (int(x) / 100 for x in floors_kEn.split('/'))
    opts = dict(floors=(n, e, k), newcomer_ttl=exp * D, s2=s2)
    opts.update(kw)
    return ('tier', opts)


POL = {
    'A today': ('today', {}),
    'B ipfs-lru': ('ipfs', dict(evict='lru')),
    'B ipfs-rand': ('ipfs', dict(evict='random')),
    'C S1 (roadmap)': tier(s2=False),
    'C S2': tier(),
    # Earned-tier variants (all S2, 30/60/10, 4 d)
    'D w30': tier(within_e='weighted', weight_cap=30.0),
    'D w90': tier(within_e='weighted', weight_cap=90.0),
    'D w365': tier(within_e='weighted', weight_cap=365.0),
    'D sen': tier(within_e='seniority'),
    'D rand-rec': tier(within_e='random'),
    'D days2': tier(earned=('days', 2)),
    'D days5': tier(earned=('days', 5)),
    'D days7': tier(earned=('days', 7)),
    'D age1': tier(earned=('age', 24.0)),
    'D noEarned': tier(earned=('none', 0)),
    'D promo': tier(promo=(10, 0.02)),
    'D promo+w90': tier(promo=(10, 0.02), within_e='weighted', weight_cap=90.0),
    'D promo+w365': tier(promo=(10, 0.02), within_e='weighted', weight_cap=365.0),
    'D promo+sen': tier(promo=(10, 0.02), within_e='seniority'),
    'D promo5+w90': tier(promo=(5, 0.01), within_e='weighted', weight_cap=90.0),
    'D nopre': tier(within_e='none'),
    'D promo+nopre': tier(promo=(10, 0.02), within_e='none'),
    'D ticket24': tier(ticket=24.0),
    'D ncap': tier(newcomer_cap_frac=0.001),
    'D nrate100': tier(newcomer_rate=100),
}


def tier4(fl='20/50/20/10', senior=30, exp=4.0, promo=(10, 0.02), **kw):
    k, s, e, n = (int(x) / 100 for x in fl.split('/'))
    opts = dict(floors=(n, e, s, k), senior=senior, newcomer_ttl=exp * D, s2=True, promo=promo)
    opts.update(kw)
    return ('tier', opts)


POL.update({
    'N nopre+newest': tier(within_e='none', reclaim='newest'),
    'N nopre+newest promo': tier(within_e='none', reclaim='newest', promo=(10, 0.02)),
    'N sen+newest': tier(within_e='seniority', reclaim='newest'),
    'N nopre+newest S1': tier(within_e='none', reclaim='newest', s2=False),
    'N nopre+newest days5': tier(within_e='none', reclaim='newest', earned=('days', 5)),
    'N nopre+newest 20/60/20': tier('20/60/20', within_e='none', reclaim='newest'),
    'N nopre+newest 4.5d': tier(exp=4.5, within_e='none', reclaim='newest'),
    'R s30 S-nopre+newest': tier4(promo=None, within_s='none', reclaim='newest'),
    'R 20/50/20/10 s30': tier4(promo=None),
    'R 20/50/20/10 s30 promo': tier4(),
    'R 20/50/20/10 s14 promo': tier4(senior=14),
    'R 20/50/20/10 s60 promo': tier4(senior=60),
    'R 30/40/20/10 s30 promo': tier4('30/40/20/10'),
    'R 20/40/20/20 s30 promo': tier4('20/40/20/20'),
    'R 20/50/20/10 s30 promo 4.5d': tier4(exp=4.5),
    'R 20/50/20/10 s30 promo5': tier4(promo=(5, 0.01)),
})


def make_cfg(pol, **kw):
    kind, opts = POL[pol] if isinstance(pol, str) else pol
    cfg = dict(policy=(kind, dict(opts)))
    cfg.update(kw)
    return cfg


def _job(args):
    exp, key, cfg = args
    t0 = time.time()
    try:
        s, _ = run.run_one(cfg)
    except Exception as e:  # keep the battery going
        import traceback
        traceback.print_exc()
        s = {'error': repr(e)}
    s['_secs'] = round(time.time() - t0, 1)
    return exp, key, s


def run_jobs(exp, jobs, procs=12):
    """jobs: list of (key tuple, cfg). Returns {key: summary}. Caches in raw/<exp>.jsonl."""
    path = os.path.join(RAW, exp + '.jsonl')
    done = {}
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                r = json.loads(line)
                done[tuple(r['key'])] = r['s']
    todo = [(exp, k, c) for k, c in jobs if tuple(k) not in done]
    t0 = time.time()
    # longest first: N=10000 and IPFS random are slow
    todo.sort(key=lambda j: -(j[2].get('n_att', 0) * (5 if j[2]['policy'][0] == 'ipfs' else 1)))
    with Pool(procs) as pool, open(path, 'a') as f:
        for i, (e, k, s) in enumerate(pool.imap_unordered(_job, todo)):
            done[tuple(k)] = s
            f.write(json.dumps({'key': list(k), 's': s}) + '\n')
            f.flush()
            if (i + 1) % 25 == 0:
                print(f'{exp}: {i + 1}/{len(todo)} {time.time() - t0:.0f}s', flush=True)
    return done


def pct(x):
    if x is None:
        return '–'
    if x != x:
        return 'n/a'
    return f'{100 * x:.0f}'


def hrs(x):
    if x is None:
        return 'never'
    return f'{x / 24:.1f}d' if x >= 48 else f'{x:.0f}h'


def table(rows, cols, cell, rowhdr='policy'):
    out = ['| ' + rowhdr + ' | ' + ' | '.join(str(c) for c in cols) + ' |',
           '|' + '---|' * (len(cols) + 1)]
    for r in rows:
        out.append('| ' + str(r) + ' | ' + ' | '.join(cell(r, c) for c in cols) + ' |')
    return '\n'.join(out)


def append(text):
    with open(RES, 'a') as f:
        f.write(text.rstrip() + '\n\n')


# ------------------------------------------------------------------------------------------------ experiments
LOADS = {'2%': 0.02, '50%': 0.5, '90%': 0.9}
NS = [10, 100, 1000, 10000]


def attack_grid(pols, loads=('2%', '50%', '90%'), ns=NS, ages=(None, 7), durs=(1, 7, 'cont'), seeds=(1,), extra=None):
    jobs = []
    for pol in pols:
        for L in loads:
            for n in ns:
                for age in ages:
                    for dur in durs:
                        for sd in seeds:
                            kw = dict(load=LOADS[L], n_att=n, preaged=age, attack_days=dur, seed=sd)
                            if extra:
                                kw.update(extra)
                            jobs.append(((pol, L, n, age, str(dur), sd), make_cfg(pol, **kw)))
    return jobs


def mean_over_seeds(res, key, field):
    vals = []
    for k, s in res.items():
        if tuple(k[:-1]) == tuple(key) and field in s and s[field] is not None:
            vals.append(s[field])
    if not vals:
        return None
    return sum(vals) / len(vals)


def E0():
    """Peacetime: no attacker."""
    pols = ['A today', 'B ipfs-lru', 'C S1 (roadmap)', 'C S2', 'D w90', 'D sen', 'D promo+w90', 'D days5',
            'D ncap', 'D nrate100', 'D ticket24']
    loads = {'2%': 0.02, '50%': 0.5, '90%': 0.9, '110%': 1.1}
    jobs = []
    for pol in pols:
        for L, lv in loads.items():
            for sd in (1, 2, 3):
                jobs.append(((pol, L, 'none', sd), make_cfg(pol, load=lv, attack_days=None, after=21, seed=sd)))
            # a big mirror (30% of capacity) joins a small network that is 70% full
        for sd in (1, 2, 3):
            jobs.append(((pol, 'mirror', 'mirror', sd), make_cfg(pol, load=0.7, attack_days=None, after=21, seed=sd,
                                                               n_pub=200, mirror_join=(7, 'frac:0.3', False))))
    res = run_jobs('E0', jobs)

    def m(pol, L, field):
        return mean_over_seeds(res, (pol, L, 'none' if L != 'mirror' else 'mirror'), field)

    txt = ['## E0. Peacetime (no attacker)', '',
           'Honest presence = records present / records honest publishers want stored (record-weighted), averaged '
           'over days 1–28; 3 seeds. "new" = publishers that joined during the run (1%/day). Refused/evicted = honest '
           'records per day refused at store time / evicted to make room. Lapses = records of active honest '
           'publishers that expired (must be 0).', '']
    cols = ['2%', '50%', '90%', '110%']
    for field, label, f in [('peace_est', 'Established honest presence %', pct),
                            ('peace_new', 'Newly joined honest presence %', pct),
                            ('refused_h_per_day', 'Honest records refused per day', lambda x: '–' if x is None else f'{x:.0f}'),
                            ('evicted_h_per_day', 'Honest records evicted per day', lambda x: '–' if x is None else f'{x:.0f}'),
                            ('lapse_h', 'Honest lapses (expiry of an active publisher\'s record)', lambda x: '–' if x is None else f'{x:.0f}'),
                            ('writes_s', 'Store writes/s (inserts+evictions+expiries; refreshes excluded)', lambda x: '–' if x is None else f'{x:.2f}'),
                            ('refresh_s', 'Refresh updates/s (coalescable)', lambda x: '–' if x is None else f'{x:.2f}')]:
        txt.append(f'**{label}** (columns: honest load / capacity)')
        txt.append('')
        txt.append(table(pols, cols, lambda p, c: f(m(p, c, field))))
        txt.append('')
    txt.append('**A big mirror joins a small network** (200 publishers, store 70% full, mirror = 30% of capacity, '
               'joins on day 7)')
    txt.append('')
    txt.append(table(pols, ['mirror stored after 1 day %', 'mirror stored at end %', 'others (est) after %',
                            'honest evictions caused'],
                     lambda p, c: {'mirror stored after 1 day %': pct(m(p, 'mirror', 'mirror_1d')),
                                   'mirror stored at end %': pct(m(p, 'mirror', 'mirror_end')),
                                   'others (est) after %': pct(m(p, 'mirror', 'est_after_mirror')),
                                   'honest evictions caused': f"{m(p, 'mirror', 'evicted_h_after_mirror') or 0:.0f}"}[c]))
    append('\n'.join(txt))


def E1():
    """Baselines and the roadmap tier model under the full attack grid."""
    pols = ['A today', 'B ipfs-lru', 'B ipfs-rand', 'C S1 (roadmap)', 'C S2']
    res = run_jobs('E1', attack_grid(pols))
    report_grid('E1', 'Baselines (A, B) and the proposed tier model (C, 30/60/10, newcomer expiry 4 d, Earned = 3 '
                      'distinct days, max-min fair share), S1 = "a lower tier never evicts a higher one" literally, '
                      'S2 = a tier below its floor may also reclaim from tiers above theirs', pols, res)


def report_grid(exp, title, pols, res, loads=('2%', '50%', '90%'), ns=NS, ages=(None, 7)):
    def g(pol, L, n, age, dur, field):
        return mean_over_seeds(res, (pol, L, n, age, dur), field)
    txt = [f'## {exp}. {title}', '']
    cols = [f'{"pre" + str(a) if a else "fresh"} N={n}' for a in ages for n in ns]

    def colkey(c):
        a, n = c.split(' N=')
        return (int(a[3:]) if a.startswith('pre') else None), int(n)
    for L in loads:
        txt.append(f'### Load {L} (honest records / capacity)')
        txt.append('')
        txt.append('Established honest publishers, mean presence % during a **continuous** attack (21 days); '
                   'pre-attack baseline in brackets')
        txt.append('')
        txt.append(table(pols, cols, lambda p, c: (lambda age, n: f"{pct(g(p, L, n, age, 'cont', 'att_mean_est'))} "
                                                   f"[{pct(g(p, L, n, age, 'cont', 'base_est'))}]")(*colkey(c))))
        txt.append('')
        txt.append('Established, last day of the continuous attack % (small publishers ≤20 records / large)')
        txt.append('')
        txt.append(table(pols, cols, lambda p, c: (lambda age, n: f"{pct(g(p, L, n, age, 'cont', 'att_last_est'))} "
                                                   f"({pct(g(p, L, n, age, 'cont', 'att_min_est_small'))}/"
                                                   f"{pct(g(p, L, n, age, 'cont', 'att_min_est_large'))} min)")(*colkey(c))))
        txt.append('')
        txt.append('Known (contacts/bootstrap) min % · publishers that joined before the attack (<7 d old) mean % · '
                   'joined during the attack mean %  (continuous)')
        txt.append('')
        txt.append(table(pols, cols, lambda p, c: (lambda age, n: f"{pct(g(p, L, n, age, 'cont', 'att_min_known'))}·"
                                                   f"{pct(g(p, L, n, age, 'cont', 'att_mean_new'))}·"
                                                   f"{pct(g(p, L, n, age, 'cont', 'att_mean_newatk'))}")(*colkey(c))))
        txt.append('')
        txt.append('Time until established honest presence < 90% / < 50% (continuous attack)')
        txt.append('')
        txt.append(table(pols, cols, lambda p, c: (lambda age, n: f"{hrs(g(p, L, n, age, 'cont', 't90_est_h'))} / "
                                                   f"{hrs(g(p, L, n, age, 'cont', 't50_est_h'))}")(*colkey(c))))
        txt.append('')
        txt.append('1-day attack: min established % · min all honest % · recovery to baseline−1pt after it ends')
        txt.append('')
        txt.append(table(pols, cols, lambda p, c: (lambda age, n: f"{pct(g(p, L, n, age, '1', 'att_min_est'))}·"
                                                   f"{pct(g(p, L, n, age, '1', 'att_min_all'))}·"
                                                   f"{hrs(g(p, L, n, age, '1', 'recover_h'))}")(*colkey(c))))
        txt.append('')
        if any(k[4] == '7' for k in res):
          txt.append('7-day attack: mean established % · mean all honest % · recovery after it ends')
          txt.append('')
          txt.append(table(pols, cols, lambda p, c: (lambda age, n: f"{pct(g(p, L, n, age, '7', 'att_mean_est'))}·"
                                                   f"{pct(g(p, L, n, age, '7', 'att_mean_all'))}·"
                                                   f"{hrs(g(p, L, n, age, '7', 'recover_h'))}")(*colkey(c))))
          txt.append('')
        txt.append('Store writes/s during the continuous attack: all (inserts+evictions+expiries) / honest-only; '
                   'attacker refreshes not counted')
        txt.append('')
        txt.append(table(pols, cols, lambda p, c: (lambda age, n: f"{g(p, L, n, age, 'cont', 'writes_s_att') or 0:.1f}/"
                                                   f"{g(p, L, n, age, 'cont', 'writes_s_att_h') or 0:.2f}")(*colkey(c))))
        txt.append('')
    append('\n'.join(txt))


def E2():
    """Floor splits x newcomer expiry."""
    pols = []
    for fl in ('30/60/10', '20/60/20', '10/70/20', '30/50/20'):
        for ex in (3.5, 4.0, 5.0, 7.0):
            for s2 in (False, True):
                name = f'C {fl} {ex:g}d {"S2" if s2 else "S1"}'
                POL[name] = tier(fl, ex, s2)
                pols.append(name)
    jobs = attack_grid(pols, loads=('50%', '90%'), ns=(100, 10000), ages=(None, 7), durs=(1, 'cont'))
    res = run_jobs('E2', jobs)

    def g(pol, L, n, age, dur, field):
        return mean_over_seeds(res, (pol, L, n, age, dur), field)
    txt = ['## E2. Tier floor splits (Known/Earned/Newcomer) × newcomer expiry', '',
           'Each cell: established mean % during a continuous attack · joined-during-attack mean % · '
           'all-honest recovery after a 1-day attack. Max-min fair share, Earned = 3 distinct days.', '']
    for L in ('50%', '90%'):
        cols = [f'{a} N={n}' for a in ('fresh', 'pre') for n in (100, 10000)]
        txt.append(f'### Load {L}')
        txt.append('')
        txt.append(table(pols, cols, lambda p, c: (lambda age, n: f"{pct(g(p, L, n, age, 'cont', 'att_mean_est'))}·"
                                                   f"{pct(g(p, L, n, age, 'cont', 'att_mean_newatk'))}·"
                                                   f"{hrs(g(p, L, n, age, '1', 'recover_h'))}")(
            7 if c.startswith('pre') else None, int(c.split('=')[1]))))
        txt.append('')
    append('\n'.join(txt))


def E3():
    """Variants."""
    pols = ['C S2', 'D w30', 'D w90', 'D w365', 'D sen', 'D rand-rec', 'D days2', 'D days5', 'D days7', 'D age1',
            'D noEarned', 'D promo', 'D promo+w90', 'D promo+w365', 'D promo+sen', 'D promo5+w90', 'D nopre', 'D promo+nopre', 'D ticket24',
            'D ncap', 'D nrate100']
    # random-record eviction is O(publishers) per store in this simulator: too slow with 1000+ identities
    jobs = [j for j in attack_grid(pols) if not (j[0][0] == 'D rand-rec' and j[0][2] >= 1000)]
    res = run_jobs('E3', jobs)
    report_grid('E3', 'Variants of the tier model (all S2, 30/60/10, 4 d newcomer expiry unless named): within-Earned '
                      'eviction order (w = weighted by age capped at 30/90/365 d, sen = newest publisher first, '
                      'rand-rec = random record), Earned criterion (days2/5/7 distinct days, age1 = first seen ≥1 d, '
                      'noEarned), promotion rate limit (promo = at most max(10, 2% of Earned) promotions/day, FIFO; '
                      'promo5 = max(5, 1%)), admission (ticket24 = a never-stored newcomer must wait 24 h when the '
                      'store is full; ncap = newcomer holds ≤ C/1000; nrate100 = newcomer inserts ≤100/day)',
                pols, res)


def E4():
    """Newcomer expiry vs honest lapses (hard constraint check)."""
    exps = (3.3, 3.5, 3.6, 3.75, 4.0, 4.5, 5.0)
    delays = (6.0, 12.0, 24.0)
    jobs = []
    for ex in exps:
        for dl in delays:
            for sd in (1, 2, 3, 4, 5):
                pol = tier('30/60/10', ex, True, earned=('days', 3))
                jobs.append(((ex, dl, sd), make_cfg(pol, load=0.5, attack_days=None, after=28, seed=sd, n_pub=500,
                                                    join_rate=0.10, leave_rate=0.0, slow_delay=dl, name_frac=0.5)))
    res = run_jobs('E4', jobs)
    txt = ['## E4. Newcomer expiry vs honest lapses', '',
           'Peacetime, store 50% full (no contention), 500 established publishers, 10%/day new publishers (so many are '
           'Newcomers), 50% keep names, 30% slow. Slow-node republish delay U(0, d) on top of 72 h (names: 72 h × '
           '(1 ± 10%) + delay). Cell: honest records that lapsed (expired while their publisher was still active) '
           'over 35 days, summed over 5 seeds. Longest honest gap = 79.2 h + d.', '']
    txt.append(table(exps, [f'delay ≤{d:g} h (max gap {79.2 + d:.1f} h = {(79.2 + d) / 24:.2f} d)' for d in delays],
                     lambda ex, c: str(sum(s.get('lapse_h', 0) for k, s in res.items()
                                           if k[0] == ex and k[1] == float(c.split('≤')[1].split(' ')[0]))),
                     rowhdr='newcomer expiry (d)'))
    append('\n'.join(txt))


def E5():
    """Attacker identities x time to push established honest records below 90% / 50%."""
    pols = ['A today', 'C S2', 'D sen', 'D promo+w365', 'D nopre', 'N nopre+newest']
    ns = (10, 100, 1000, 10000)
    jobs = []
    for pol in pols:
        for L in ('2%', '90%'):
            for n in ns:
                for age in (None, 7, 30, 180):
                    kw = dict(load=LOADS[L], n_att=n, preaged=age, attack_days='cont', cont_days=28, seed=1)
                    jobs.append(((pol, L, n, age), make_cfg(pol, **kw)))
    res = run_jobs('E5', jobs)
    txt = ['## E5. Attacker identities × time to push established honest records below 90% / 50%', '',
           'Continuous attack, 28 days. Cell: time from attack start until established honest presence (record-'
           'weighted) < 90% / < 50%, "never" = not within 28 days; then (mean % over the attack). Pre-aged d = identities '
           'first seen d days before the attack and already qualifying as Earned/senior (with a promotion limit, only '
           'as many as the limit let through in d days start Earned; the rest queue).', '']
    for L in ('2%', '90%'):
        for age in (None, 7, 30, 180):
            txt.append(f'### Load {L}, {"fresh" if age is None else f"pre-aged {age} d"}')
            txt.append('')
            txt.append(table(pols, [f'N={n}' for n in ns],
                             lambda p, c: (lambda s: f"{hrs(s.get('t90_est_h'))}/{hrs(s.get('t50_est_h'))} "
                                                     f"({pct(s.get('att_mean_est'))})")(
                                 res.get((p, L, int(c[2:]), age), {}))))
            txt.append('')
    append('\n'.join(txt))


R_POLS = ['R 20/50/20/10 s30', 'R 20/50/20/10 s30 promo', 'R 20/50/20/10 s14 promo', 'R 20/50/20/10 s60 promo',
          'R 30/40/20/10 s30 promo', 'R 20/40/20/20 s30 promo', 'R 20/50/20/10 s30 promo 4.5d',
          'R 20/50/20/10 s30 promo5']


def E6():
    """Candidate 4-tier designs (Known / Senior / Earned / Newcomer)."""
    pols = ['A today', 'C S2', 'D sen', 'D promo+w365', 'D nopre', 'N nopre+newest', 'N nopre+newest promo',
            'N sen+newest', 'N nopre+newest S1', 'N nopre+newest days5', 'N nopre+newest 20/60/20',
            'N nopre+newest 4.5d', 'R s30 S-nopre+newest', 'R 20/50/20/10 s30', 'R 20/50/20/10 s30 promo']
    jobs = attack_grid(pols, ages=(None, 7, 30, 180), durs=(1, 'cont'))
    res = run_jobs('E6', jobs)
    report_grid('E6', 'Candidate designs. N = three tiers (30/60/10, S2, 4 d) with no pre-emption inside Earned '
                      '("nopre": Earned publishers never evict each other) or seniority ("sen"), and a Newcomer tier '
                      'below its floor reclaiming from the most recently promoted Earned publisher first ("newest"). '
                      'R = four tiers Known / Senior / Earned / Newcomer (floors in that order). '
                      'Senior = in Earned for ≥ s days (s14/s30/s60); promo = Earned promotions limited to max(10, 2% '
                      'of Earned)/day (promo5: max(5, 1%)), FIFO; S2 floors; max-min fair share inside every tier; '
                      'Earned = held on 3 distinct days; Newcomer expiry 4 d (4.5 d where named). Compared with A, C '
                      'S2 and the best 3-tier variant', pols, res, ages=(None, 7, 30, 180))
    # peacetime for the candidates
    loads = {'2%': 0.02, '50%': 0.5, '90%': 0.9, '110%': 1.1}
    jobs = []
    for pol in pols:
        for L, lv in loads.items():
            for sd in (1, 2, 3):
                jobs.append(((pol, L, 'none', sd), make_cfg(pol, load=lv, attack_days=None, after=21, seed=sd)))
        for sd in (1, 2, 3):
            jobs.append(((pol, 'mirror', 'mirror', sd), make_cfg(pol, load=0.7, attack_days=None, after=21, seed=sd,
                                                               n_pub=200, mirror_join=(7, 'frac:0.3', False))))
    res = run_jobs('E6peace', jobs)

    def m(pol, L, field):
        return mean_over_seeds(res, (pol, L, 'none' if L != 'mirror' else 'mirror'), field)
    cols = ['2%', '50%', '90%', '110%']
    txt = ['### E6 peacetime (3 seeds): established % · newly joined % · honest refused/day · honest evicted/day', '']
    txt.append(table(pols, cols, lambda p, c: f"{pct(m(p, c, 'peace_est'))}·{pct(m(p, c, 'peace_new'))}·"
                                              f"{m(p, c, 'refused_h_per_day') or 0:.0f}·{m(p, c, 'evicted_h_per_day') or 0:.0f}"))
    txt.append('')
    txt.append('Big mirror joining a small network (as E0): mirror stored after 1 d % · at end % · others % · honest evictions')
    txt.append('')
    txt.append(table(pols, ['mirror'], lambda p, c: f"{pct(m(p, 'mirror', 'mirror_1d'))}·{pct(m(p, 'mirror', 'mirror_end'))}·"
                                                   f"{pct(m(p, 'mirror', 'est_after_mirror'))}·{m(p, 'mirror', 'evicted_h_after_mirror') or 0:.0f}"))
    append('\n'.join(txt))


def E7():
    """Floors, newcomer expiry and Earned criterion on the recommended design (no pre-emption inside Earned,
    max-holder reclaim)."""
    pols = []
    for fl in ('30/65/5', '30/60/10', '20/60/20'):
        for ex in (4.0, 4.5):
            for days in (3, 5):
                name = f'P {fl} {ex:g}d days{days}'
                POL[name] = tier(fl, ex, True, within_e='none', earned=('days', days))
                pols.append(name)
    jobs = attack_grid(pols, loads=('2%', '50%', '90%'), ns=(100, 10000), ages=(None, 7), durs=(1, 'cont'))
    res = run_jobs('E7', jobs)
    peace = []
    for pol in pols:
        for L, lv in (('90%', 0.9), ('110%', 1.1)):
            peace.append(((pol, L, 'none', 1), make_cfg(pol, load=lv, attack_days=None, after=21, seed=1)))
        peace.append(((pol, 'mirror', 'mirror', 1), make_cfg(pol, load=0.7, attack_days=None, after=21, seed=1,
                                                             n_pub=200, mirror_join=(7, 'frac:0.3', False))))
    pres = run_jobs('E7peace', peace)

    def g(pol, L, n, age, dur, field):
        return mean_over_seeds(res, (pol, L, n, age, dur), field)
    txt = ['## E7. Floors (Known/Earned/Newcomer) × newcomer expiry × Earned days on the recommended design', '',
           'Design ("P"): S2 floors, no pre-emption inside Earned, a Newcomer tier below its floor reclaims from the '
           'largest Earned holder, max-min inside Newcomer and Known. Attack cells: established mean % (continuous) · '
           'new-before-attack mean % · joined-during-attack mean % · all-honest recovery after a 1-day attack.', '']
    cols = [f'{a} N={n}' for a in ('fresh', 'pre') for n in (100, 10000)]
    for L in ('2%', '50%', '90%'):
        txt.append(f'### Load {L}')
        txt.append('')
        txt.append(table(pols, cols, lambda p, c: (lambda age, n: f"{pct(g(p, L, n, age, 'cont', 'att_mean_est'))}·"
                                                   f"{pct(g(p, L, n, age, 'cont', 'att_mean_new'))}·"
                                                   f"{pct(g(p, L, n, age, 'cont', 'att_mean_newatk'))}·"
                                                   f"{hrs(g(p, L, n, age, '1', 'recover_h'))}")(
            7 if c.startswith('pre') else None, int(c.split('=')[1]))))
        txt.append('')
    txt.append('### Peacetime (seed 1): established % · newly joined % at 90% and 110% load; mirror join: '
               'mirror after 1 d % · at end % · others %')
    txt.append('')

    def pc(p, c):
        if c == 'mirror':
            return (f"{pct(mean_over_seeds(pres, (p, 'mirror', 'mirror'), 'mirror_1d'))}·"
                    f"{pct(mean_over_seeds(pres, (p, 'mirror', 'mirror'), 'mirror_end'))}·"
                    f"{pct(mean_over_seeds(pres, (p, 'mirror', 'mirror'), 'est_after_mirror'))}")
        return (f"{pct(mean_over_seeds(pres, (p, c, 'none'), 'peace_est'))}·"
                f"{pct(mean_over_seeds(pres, (p, c, 'none'), 'peace_new'))}")
    txt.append(table(pols, ['90%', '110%', 'mirror'], pc))
    append('\n'.join(txt))


if __name__ == '__main__':
    for name in sys.argv[1:]:
        t0 = time.time()
        globals()[name]()
        print(name, 'done', round(time.time() - t0), 's', flush=True)
