"""RNS 1.5.6's ingress limit for path requests, ported line for line with simulated time, and rnfs's planned pacer.

What it ports (RNS/Interfaces/Interface.py and RNS/Transport.py of RNS 1.5.6):

- `Interface.received_path_request`: every path request that reaches `Transport.inbound` with a fresh tag (duplicates
  are dropped before it is counted) appends its arrival time to the receiving interface's `ip_freq_deque`, a deque of
  at most `IA_FREQ_SAMPLES` (48) times, and to its parent's (a hub's server interface) as well.
- `Interface.incoming_pr_frequency`: 0 while the deque holds 2 times or fewer (`IC_DEQUE_MIN_SAMPLE`); else
  n / (now - oldest), n counted before the oldest is popped when it is more than `PR_FREQ_DECAY` (10 s) old. Only
  one is popped per call, so a burst decays one sample per evaluation.
- `Interface.should_ingress_limit_pr`: threshold `IC_PR_BURST_FREQ_NEW` (3) while the interface is younger than
  `IC_NEW_TIME` (2 h), else `IC_PR_BURST_FREQ` (8). Inactive: activates when the estimate is above the threshold.
  Active: a call with the estimate below the threshold, more than `IC_BURST_HOLD` (15 s) after activation and after
  the last call that saw the estimate at or above it, counts the cooldown down from 3 and releases at 0 (that call
  still answers True); any other call resets the cooldown to 3 (and, at or above the threshold, the sustained time).
- Where it is evaluated: `Transport.inbound` for every path request (after counting it); `Transport.path_request` again
  when the request was not already limited; and `Transport.jobs` for every interface each `interface_jobs_interval`
  (5 s). The estimate's popping is a side effect of each evaluation, so all three are modelled.
- `InboundQueues`: one per transport instance, strict priority (data, announces, path requests, ingress-limited), sizes
  1024 / 128 / 128 / 8 (`INBOUND_*_QUEUE_LENGTH`), one drainer (`Transport.inbound_job`). A limited request goes to the
  ingress-limited queue (8, shared by every interface of the instance) and is dropped when that is full.
- `Transport.path_request` with a limited request: answered from the path table if the path is known (the cached
  announce is sent back after `PATH_REQUEST_GRACE`, 0.4 s, on the next job run), but never forwarded to discover an
  unknown path ("Not sending recursive path request ... due to active ingress limiting").

The parent interface of a hub (`TCPServerInterface`, `BackboneInterface`) gets every spawned client's samples, and
`Transport.jobs` evaluates it too, but `Transport.inbound` checks only the receiving (spawned) interface, so the
parent's state shows in `get_interface_stats` (`pr_burst_active`) without limiting anything.
"""
from collections import deque

# RNS 1.5.6 constants (Interface.py, Transport.py)
IP_FREQ_SAMPLES = 48          # ip_freq_deque is made with maxlen IA_FREQ_SAMPLES (48)
IC_DEQUE_MIN_SAMPLE = 2
PR_FREQ_DECAY = 10.0          # 1 / PR_MINFREQ_HZ
IC_NEW_TIME = 2 * 60 * 60
IC_PR_BURST_FREQ_NEW = 3
IC_PR_BURST_FREQ = 8
IC_BURST_HOLD = 15
PR_COOLDOWN = 3               # ic_pr_burst_cooldown set on activation and on every non-qualifying call
INTERFACE_JOBS_INTERVAL = 5.0
INBOUND_PR_QUEUE_LENGTH = 128
INBOUND_IL_QUEUE_LENGTH = 8
PATH_REQUEST_GRACE = 0.4      # before a transport node sends the cached announce answering a path request
JOB_INTERVAL = 0.25           # Transport.job_interval: the announce table (and so the answer) is served on job runs
PATH_REQUEST_MI = 20.0        # no automated re-request for a destination within this of the last request for it
DISCOVERY_PR_TX_THROTTLE = 0.5  # Transport.handle_disovery_path_requests sleeps this before each re-request


class PRIngress:
    """One interface's path-request ingress control (Interface.ip_freq_deque, incoming_pr_frequency,
    should_ingress_limit_pr), with time passed in. created: when the interface came up (a hub's spawned interface: when
    the client connected). parent: the hub's server interface, which gets every sample too (stats only)."""

    def __init__(self, name, created, parent=None, jobs_phase=0.0):
        """An interface created at `created` with no path request heard yet."""
        self.name = name
        self.created = created
        self.parent = parent
        self.dq = deque(maxlen=IP_FREQ_SAMPLES)
        self.active = False
        self.activated = 0.0
        self.sustained = 0.0
        self.cooldown = 0
        self.next_job = jobs_phase  # the next Transport.jobs interface pass (every INTERFACE_JOBS_INTERVAL)
        # what the study measures
        self.received = 0
        self.limited = 0
        self.trips = []        # activation times
        self.released = []     # release times
        self.max_freq = 0.0

    def age(self, now):
        """Interface.age()."""
        return now - self.created

    def freq(self, now):
        """Interface.incoming_pr_frequency()."""
        n = len(self.dq)
        if not n > IC_DEQUE_MIN_SAMPLE:
            return 0
        oldest = self.dq[0]
        span = now - oldest
        if span > PR_FREQ_DECAY:
            self.dq.popleft()
        if span <= 0:
            return 0
        return n / span

    def peek(self, now):
        """incoming_pr_frequency() without its side effect (what get_interface_stats would show, give or take a pop)."""
        n = len(self.dq)
        if not n > IC_DEQUE_MIN_SAMPLE:
            return 0
        span = now - self.dq[0]
        return 0 if span <= 0 else n / span

    def check(self, now):
        """Interface.should_ingress_limit_pr(): True while limiting."""
        threshold = IC_PR_BURST_FREQ_NEW if self.age(now) < IC_NEW_TIME else IC_PR_BURST_FREQ
        f = self.freq(now)
        if f > self.max_freq:
            self.max_freq = f
        if self.active:
            if f < threshold and now > self.activated + IC_BURST_HOLD and now > self.sustained + IC_BURST_HOLD:
                if self.cooldown <= 0:
                    self.active = False
                    self.released.append(now)
                else:
                    self.cooldown -= 1
            else:
                self.cooldown = PR_COOLDOWN
                if f >= threshold:
                    self.sustained = now
            return True
        if f > threshold:
            self.active = True
            self.activated = now
            self.sustained = now
            self.cooldown = PR_COOLDOWN
            self.trips.append(now)
            return True
        return False

    def jobs_until(self, now):
        """The Transport.jobs interface passes (should_ingress_limit_pr every INTERFACE_JOBS_INTERVAL) due up to now.
        Skips ahead once a pass can no longer change anything (inactive with 2 samples or fewer)."""
        while self.next_job <= now:
            if not self.active and len(self.dq) <= IC_DEQUE_MIN_SAMPLE:
                k = int((now - self.next_job) // INTERFACE_JOBS_INTERVAL) + 1
                self.next_job += k * INTERFACE_JOBS_INTERVAL
                break
            self.check(self.next_job)
            self.next_job += INTERFACE_JOBS_INTERVAL

    def received_pr(self, now):
        """Interface.received_path_request() then Transport.inbound's should_ingress_limit_pr(): True if the request is
        ingress-limited (queued as TC_INGRESS_LIMITED)."""
        self.jobs_until(now)
        self.received += 1
        self.dq.append(now)
        if self.parent is not None:
            self.parent.jobs_until(now)
            self.parent.received += 1
            self.parent.dq.append(now)
        limited = self.check(now)
        if limited:
            self.limited += 1
        return limited

    def active_seconds(self, t0, t1):
        """Seconds of [t0, t1) during which the limit was active."""
        tot = 0.0
        rel = list(self.released)
        for i, a in enumerate(self.trips):
            r = rel[i] if i < len(rel) else t1
            lo, hi = max(a, t0), min(r, t1)
            if hi > lo:
                tot += hi - lo
        return tot


class InboundQueue:
    """A transport instance's InboundQueues for the classes path requests use: path requests (128) ahead of
    ingress-limited ones (8), drained by one thread (Transport.inbound_job) taking `service` seconds a packet. Higher
    classes (data, announces) are modelled as the drainer being busy a fraction `busy` of the time, so each packet
    takes service / (1 - busy). The drainer itself runs in the engine (prsim.Engine._drainer)."""

    def __init__(self, service=0.002, busy=0.0):
        """An empty queue."""
        self.service = service / max(1e-9, 1.0 - busy)
        self.q = (deque(), deque())  # TC_PATH_REQUEST, TC_INGRESS_LIMITED
        self.sizes = (INBOUND_PR_QUEUE_LENGTH, INBOUND_IL_QUEUE_LENGTH)
        self.dropped = [0, 0]
        self.served = [0, 0]
        self.idle = True       # the drainer is waiting for a packet
        self.wake = None       # its wake-up event while idle (set by the engine)

    def put(self, item, limited):
        """InboundQueues.put: False (dropped) if the class's queue is full."""
        c = 1 if limited else 0
        if len(self.q[c]) >= self.sizes[c]:
            self.dropped[c] += 1
            return False
        self.q[c].append(item)
        return True

    def get(self):
        """InboundQueues.get without blocking: the next packet, highest class first, or None."""
        for c, q in enumerate(self.q):
            if q:
                self.served[c] += 1
                return q.popleft()
        return None


class Pacer:
    """rnfs's planned node-wide path-request budget: requests at least `spacing` seconds apart. A request waiting for
    it is not a failure and its wait doesn't count towards path_timeout."""

    def __init__(self, spacing=1.0):
        """No request sent yet."""
        self.spacing = spacing
        self.free_at = float("-inf")
        self.waited = 0.0
        self.taken = 0

    def free(self, now):
        """True if a request may go now."""
        return now >= self.free_at

    def take(self, now):
        """A request goes now (callers check free first, or wait until free_at)."""
        self.free_at = max(now, self.free_at) + self.spacing
        self.taken += 1

    def charge(self, now):
        """A request rnfs didn't send counts against the budget (RNS re-requesting a path after a failed link)."""
        self.free_at = max(now, self.free_at) + self.spacing
