"""The density check (specification section 6, "Identities placed next to a key"): a node estimates how many DHT
servers there are, notices when the servers found near a key are packed much more densely than that allows, and
then looks further out. All local: nothing about it is sent, and nothing on the wire changes.

- **Size estimate.** The distance from a key to its K-th closest server, d_K, is about K × 2^128 / N among N servers
  with random IDs. SizeEstimate keeps d_K from the node's last SIZE_SAMPLES complete lookups (one per key, the latest)
  and estimates N as (K − 1/3) × 2^128 over their median (the median of a Gamma(K) is about K − 1/3), leaving out the
  key being checked, so identities placed next to it can't shrink its own yardstick. Pooled over many keys it is 2 to
  4 times less noisy than a node's own neighbourhood (eclipse simulation: 4% false positives at T = 2 against 12%).
  Only lookups for keys the node chose itself are sampled (rnfs.dht.lookups: its publish lookups and lookups for
  random keys, never a search, whose key anyone may choose through the gateway), and a key the check flags loses its
  sample (forget): searches for keys inside one tight cluster of identities used to collapse the median, inflating
  the estimate a million times and turning the check off (AUD-M1).
  A lookup's sample counts only nodes it verified (verified_sample: the closest that answered or whose identity is
  known, up to the first that isn't, its d_j scaled to rank K), so made-up IDs in a reply can't inflate the estimate.
  It needs MIN_SIZE_SAMPLES lookups before the check runs, and is kept in the state database's meta table, written
  with the node's batched saves, so a restart has it at once.
- **The check.** With N the estimate, rank j (1-based) of the servers known near a key should lie at about
  j × 2^128 / N. A rank j >= K closer than j × 2^128 / (T × N) is anomalously dense (T = DENSITY_THRESHOLD, 2); the
  *dense core* is everything up to the last such rank (dense_core). Ranks below K never fire: a few close servers
  happen by chance, and fewer than K placed identities can't hide a key's K closest anyway.
- **Rings.** When it fires, two rings beyond the core count: the servers ranked just after the core's last rank,
  and the servers beyond the radius rho = K × 2^128 / (T × N), where K honest servers would be expected whatever
  identities were placed (rings). The radius doesn't depend on how many placed identities each node happened to
  learn of, so a publisher and a searcher agree on it; the ring after the core reaches past a core the radius
  underestimates. Publishers store to K servers of each ring; searchers continue until RING_ANSWERS of each ring
  have answered.
- **How often it fired** (CheckCounts, for the node's status): the searches and the published keys it checked, and
  those it flagged, over the last hour and day. A rate well above the few percent of false positives points at
  identities placed next to keys, or at a size estimate gone wrong.
"""
import bisect
import math
import os
import statistics
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Sequence

from rnfs.constants import ID_LENGTH, K
from rnfs.dht.recent import RecentCounts
from rnfs.dht.routing import is_id

SPACE = 1 << (ID_LENGTH * 8)  # the ID space: distances are below it
DENSITY_THRESHOLD = 2.0  # T: denser than this many times what the size estimate allows fires the check
SIZE_SAMPLES = 16  # lookups (distinct keys) the size estimate pools, the latest kept
MIN_SIZE_SAMPLES = 8  # samples needed before the check runs (it doesn't before)
MIN_VERIFIED = 3  # a lookup's sample needs at least this many verified nodes closest to its key (beta, by default)
RING_ANSWERS = 5  # servers of each ring a searcher waits for (eclipse simulation: 3 lost 3 points, 8 gained 0.8)
META_STORE = "size_estimate"  # the meta table's store name for the samples
_SAMPLE = 2 * ID_LENGTH  # bytes per saved sample: the key, then d_K


def _never(node_id: bytes) -> int:
    """Default later: every node ranks alike."""
    return 0


class SizeEstimate:
    """The network's size, estimated from recent lookups. Thread-safe; a node.Store (save, close) given a state_dir."""

    def __init__(self, state_dir: str | None = None, samples: int = SIZE_SAMPLES, minimum: int = MIN_SIZE_SAMPLES,
                 clock: Callable[[], float] = time.monotonic):
        """An estimate saved in state_dir (only in memory without one), pooling samples lookups and needing minimum;
        clock (monotonic seconds) times the latest sample."""
        self.samples = max(1, samples)
        self.minimum = max(1, min(minimum, self.samples))
        self.clock = clock
        self.updated = clock()  # when the latest sample came (or the estimate was made: a loaded one counts as fresh)
        self._lock = threading.Lock()
        self._distances: OrderedDict[bytes, int] = OrderedDict()  # key -> d_K, least recent first
        self._dirty = False
        self._db = None
        if state_dir:
            # Imported here: lookups import this module, and the state database loads RNS (through rnfs.log)
            from rnfs import statedb
            self._db = statedb.open_store(os.path.expanduser(state_dir), self._load)

    def _load(self, db):
        """Read the saved samples; a malformed value is ignored (the estimate warms up again)."""
        blob = db.meta(META_STORE, "samples")
        if not isinstance(blob, bytes) or len(blob) % _SAMPLE:
            self._dirty = blob is not None
            return
        for offset in range(0, len(blob), _SAMPLE):
            key, value = blob[offset:offset + ID_LENGTH], int.from_bytes(blob[offset + ID_LENGTH:offset + _SAMPLE],
                                                                           "big")
            if value > 0:
                self._distances[key] = value
        while len(self._distances) > self.samples:
            self._distances.popitem(last=False)

    def save(self):
        """Write the samples if they changed since the last save. Raises OSError (StateDbError) if the database fails;
        they are written by the next save."""
        if self._db is None:
            return
        with self._lock:
            if not self._dirty:
                return
            blob = b"".join(key + value.to_bytes(ID_LENGTH, "big") for key, value in self._distances.items())
            self._dirty = False
        from rnfs import statedb
        try:
            with self._db.transaction() as tx:
                statedb.set_meta(tx, META_STORE, "samples", blob)
        except OSError:
            with self._lock:
                self._dirty = True
            raise

    def close(self):
        """Release the state database (idempotent)."""
        if self._db is not None:
            self._db.close()

    def note(self, key: bytes, kth: int):
        """A complete lookup for key found its K-th closest server at distance kth: the latest sample for key."""
        if not is_id(key) or not 0 < kth < SPACE:
            return
        with self._lock:
            self._distances.pop(key, None)
            self._distances[key] = kth
            while len(self._distances) > self.samples:
                self._distances.popitem(last=False)
            self._dirty = True
            self.updated = self.clock()

    def forget(self, key: bytes):
        """Drop key's sample, if any: the density check flagged key, so its d_K says nothing of the network's size."""
        with self._lock:
            if self._distances.pop(key, None) is not None:
                self._dirty = True

    def estimate(self, key: bytes | None = None) -> float | None:
        """The estimated number of DHT servers, from the samples of keys other than key; None until there are
        minimum such samples."""
        with self._lock:
            distances = [value for sample, value in self._distances.items() if sample != key]
        if len(distances) < self.minimum:
            return None
        return (K - 1 / 3) * SPACE / statistics.median(distances)

    def __len__(self) -> int:
        """Samples held."""
        with self._lock:
            return len(self._distances)


class CheckCounts:
    """How often the density check ran and fired recently: once per search it checked (its first lookup had a size
    estimate) and once per published key it checked over a region's servers, flagged when it fired (in any of the
    search's lookups; for a key, when it got a widened lookup). In memory only, as the status reports it."""

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        """No checks yet; clock is monotonic seconds."""
        self.checked = RecentCounts(clock)
        self.flagged = RecentCounts(clock)

    def note(self, flagged: bool):
        """One check, flagged or not."""
        self.checked.add()
        if flagged:
            self.flagged.add()

    def status(self) -> dict:
        """{"density_checked_hour", "density_flagged_hour", "density_checked_day", "density_flagged_day"}."""
        checked_hour, checked_day = self.checked.hour_and_day()
        flagged_hour, flagged_day = self.flagged.hour_and_day()
        return {"density_checked_hour": checked_hour, "density_flagged_hour": flagged_hour,
                "density_checked_day": checked_day, "density_flagged_day": flagged_day}


def verified_sample(distances: dict[bytes, int], verified: Callable[[bytes], bool]) -> int | None:
    """A complete lookup's size-estimate sample, from distances (node -> distance to the key: every node it knew of
    and hadn't seen fail) and verified(node) (it answered, or its identity is known): over the longest run of
    verified nodes from the closest, at most K long, d_j of its last scaled to rank K, d_j × (K − 1/3) / (j − 1/3)
    (the medians of Gamma(j) and Gamma(K)); None if the run is shorter than MIN_VERIFIED.

    Not d_K over every node known: a /find reply can name made-up IDs just beyond the closest that answered, which
    the lookup completes without querying, and they would shrink d_K, inflating the estimate about 3 times. Not d_K
    over the verified nodes either: a node that knows few identities (on a LoRa island, say) would skip most real
    nodes too, and underestimate about 4 times. Ending the run at the first unverified node keeps the sample's rank
    honest either way; it is noisier only when unverified nodes come early."""
    run: list[int] = []
    for node_id in sorted(distances, key=distances.__getitem__):
        if len(run) == K or not verified(node_id):
            break
        run.append(distances[node_id])
    if len(run) < MIN_VERIFIED:
        return None
    return max(1, round(run[-1] * (K - 1 / 3) / (len(run) - 1 / 3)))


def dense_core(distances: Sequence[int], size: float, threshold: float = DENSITY_THRESHOLD) -> int:
    """The density check over distances (ascending) of the servers known near a key, for an estimate of size servers:
    the last rank j >= K (1-based) whose distance is below j × 2^128 / (threshold × size), or 0 if none is. Scanning
    stops past 2K once a rank lies 4 times further out than that, as far nodes say nothing about the key."""
    unit = SPACE / (threshold * size)
    core = 0
    for rank in range(K, len(distances) + 1):
        value = distances[rank - 1]
        if value < rank * unit:
            core = rank
        elif value > 4 * rank * unit and rank > 2 * K:
            break
    return core


def radius(size: float, threshold: float = DENSITY_THRESHOLD) -> int:
    """rho: the distance within which K servers would be expected even at threshold times the estimated density."""
    return math.ceil(K * SPACE / (threshold * size))


def rings(nodes: Sequence[bytes], distances: Sequence[int], size: float | None, count: int,
          later: Callable[[bytes], int] = _never) -> list[bytes]:
    """The servers to reach beyond a dense core: count from each ring, the ring after the core's last rank and the ring
    beyond the radius, each closest first with the nodes later() says last (unreliable ones); none when the check
    doesn't fire or there is no estimate. nodes are ranked by distance to the key, distances theirs (ascending)."""
    if size is None or size <= 0:
        return []
    core = dense_core(distances, size)
    if not core:
        return []
    chosen: dict[bytes, None] = {}
    for start in (core, bisect.bisect_left(distances, radius(size))):
        ring = sorted(nodes[start:], key=later)  # stable: by distance within each group
        chosen.update(dict.fromkeys(ring[:count]))
    return list(chosen)
