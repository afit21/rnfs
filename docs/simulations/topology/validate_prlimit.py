"""Check prlimit.PRIngress against RNS's own code: the same arrival times (and Transport.jobs passes) go through
RNS.Interfaces.Interface.Interface's received_path_request / should_ingress_limit_pr with time.time mocked, and
through the port; every call must give the same answer. Needs RNS (1.5.6) importable: run with the venv's python,
e.g. `../../../.venv/bin/python validate_prlimit.py` from this folder in a checkout whose .venv has RNS."""
import random
import sys
import time
from unittest import mock

import RNS
from RNS.Interfaces.Interface import Interface

from prlimit import INTERFACE_JOBS_INTERVAL, PRIngress


class Bare(Interface):
    """An Interface without Reticulum: just the attributes the path-request limit reads, as Interface.__init__ sets
    them with Reticulum's defaults."""

    def __init__(self, created):
        """Set what should_ingress_limit_pr and received_path_request use."""
        from collections import deque
        self.prxc = self.prxb = 0
        self.created = created
        self.ingress_control = True
        self.parent_interface = None
        self.ip_freq_deque = deque(maxlen=Interface.IA_FREQ_SAMPLES)
        self.ic_pr_burst_active = False
        self.ic_pr_burst_activated = 0
        self.ic_pr_burst_sustained = 0
        self.ic_pr_burst_cooldown = 0
        self.ic_burst_hold = Interface.IC_BURST_HOLD
        self.ic_pr_burst_freq_new = Interface.IC_PR_BURST_FREQ_NEW
        self.ic_pr_burst_freq = Interface.IC_PR_BURST_FREQ
        self.ic_new_time = Interface.IC_NEW_TIME


def run(arrivals, created, jobs_phase, lazy=False):
    """Feed arrivals (sorted times) and the 5 s job passes to both; returns the number of calls compared. lazy: the
    port runs its job passes itself (PRIngress.jobs_until, as the engine uses it), compared at arrivals only."""
    clock = [0.0]
    real = Bare(created)
    port = PRIngress("x", created, jobs_phase=jobs_phase)
    calls = 0
    with mock.patch("time.time", lambda: clock[0]):
        next_job = jobs_phase
        for t in arrivals + [arrivals[-1] + 200.0]:
            while next_job <= t:
                clock[0] = next_job
                a = real.should_ingress_limit_pr()
                if not lazy:
                    b = port.check(next_job)
                    assert a == b, (next_job, a, b)
                    calls += 1
                next_job += INTERFACE_JOBS_INTERVAL
            if not lazy:
                port.next_job = next_job
            if t == arrivals[-1] + 200.0:
                break
            clock[0] = t
            real.received_path_request()
            a = real.should_ingress_limit_pr()
            b = port.received_pr(t)
            assert a == b, (t, a, b)
            calls += 1
            if not a:  # Transport.path_request's second evaluation
                assert real.should_ingress_limit_pr() == port.check(t)
                calls += 1
            assert list(real.ip_freq_deque) == list(port.dq)
    return calls


if __name__ == "__main__":
    rng = random.Random(1)
    total = 0
    for trial in range(2000):
        n = rng.randint(3, 120)
        kind = rng.random()
        t = rng.uniform(0, 5)
        arrivals = []
        for _ in range(n):
            if kind < 0.3:      # bursts of near-simultaneous requests with pauses
                t += rng.choice([0.0005, 0.001, 0.3, 2.0, 20.0, 60.0])
            elif kind < 0.6:    # paced
                t += rng.choice([0.5, 1.0, 1.0, 1.5]) + rng.uniform(-0.05, 0.05)
            else:               # Poisson at a random rate
                t += rng.expovariate(rng.choice([0.2, 1, 3, 8, 20]))
            arrivals.append(t)
        created = rng.choice([0.0, -1e9, -7000.0])
        phase = rng.uniform(0, 5)
        total += run(arrivals, created, phase) + run(arrivals, created, phase, lazy=True)
    print(f"RNS {RNS.__version__}: {total} evaluations over 2000 arrival sequences (each with and without lazy job passes), port identical")
    sys.exit(0)
