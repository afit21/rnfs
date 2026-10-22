import run, sys
from multiprocessing import Pool
def job(a):
    L,pre,n,name,opts=a
    s,_=run.run_one(dict(policy=('tier',opts),n_att=n,attack_days='cont',load=L,preaged=pre))
    return (L,pre,n,name, round(s['att_mean_est'],2), round(s['att_mean_new'],2), round(s['att_mean_newatk'],2))
base=dict(within_e='none',s2=True,floors=(0.1,0.6,0.3),newcomer_ttl=96, weight_cap=365.0)
V={'fair':dict(base,reclaim='fair'),'aged1':dict(base,reclaim='aged',weight_exp=1.0),'aged2':dict(base,reclaim='aged',weight_exp=2.0)}
jobs=[(L,pre,n,k,v) for L in (0.02,0.9) for pre in (None,7,180) for n in (10,1000,10000) for k,v in V.items()]
with Pool(12) as p:
    for r in p.imap(job, jobs): print(*r, flush=True)
