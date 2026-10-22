"""Local scores (specification section 6, "Local scores"): a private score per peer, raised by useful work this
node checked itself and lowered by failures, bad blocks and silence. Nothing about them is sent or published.

- Raised: a /blocks response that delivered blocks verified against their CIDs (SERVED), and a /find answer
  that proved useful (USEFUL_ANSWER): it listed a provider that then served verified blocks, or named a server
  closer to the key than the answering node that then answered itself. Each counts once per fetch or lookup.
- Lowered: a query or request that failed (FAILED: a /find unanswered, a provider failing in a fetch) and a
  block not matching its CID (BAD_BLOCK, on top of the ban).
- Silence: every score halves each SCORE_HALF_LIFE without new work, towards 0 from either side, so standing
  must keep being earned and a bad score is forgiven in time. Scores stay within ±SCORE_LIMIT.
- Bootstrap nodes (bootstrap_nodes, and DEFAULT_BOOTSTRAP_NODES unless default_bootstrap is off or in private
  mode) start with BOOTSTRAP_SEED when they have no row, so a fresh node has someone to lean on. The seed decays
  like the rest, so they keep standing only by earning it; a node no longer a bootstrap node loses what is left
  of its seed at the next start (the seed's share of a score is exact: every contribution decays alike).
- band() turns a score into a coarse rank, a power of two wide, so two peers of about the same standing are
  ordered by the next key (hops, seniority) rather than by noise. Scores order providers within each seniority
  group (seniority stays first: a score can be earned quickly by serving popular blocks) and the servers a join
  tries first; nothing else uses them.
- At most MAX_SCORES peers have a row; the least recently useful is forgotten first (a row only ever lowered
  goes before every row once raised). A peer without a row scores 0, so forgetting is always safe: rows are
  made only by this node's own dealings, so state grows with its activity, not with the network.

Kept in the state database's peer_scores table (rnfs.statedb), the rows changed written with the node's batched
saves and at stop, not durably. Times are wall clock (a score's decay spans restarts); one in the future when loaded
(a clock since corrected backwards) counts as now.
"""
import math
import os
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable

from rnfs import statedb
from rnfs.constants import DAY
from rnfs.dht.routing import is_id

SCORE_HALF_LIFE = 7 * DAY  # a score halves in this time without new work
SCORE_LIMIT = 64.0  # scores stay within ±this
SERVED = 1.0  # a /blocks response delivering blocks verified against their CIDs
USEFUL_ANSWER = 0.5  # a /find answer that led to a provider serving verified blocks, or to a closer live server
FAILED = -1.0  # a query or request that failed
BAD_BLOCK = -32.0  # a block not matching its CID
BOOTSTRAP_SEED = 6.0  # a bootstrap node's score when it has none (mid-band, so a moment's decay keeps its band)
MAX_SCORES = 4096  # peers with a score (about 0.5 MB); the least recently useful is forgotten first
TABLE = "peer_scores"


class _Row:
    """A peer's score as of updated (decaying since), when it was last raised (None: never) and when it was seeded
    as a bootstrap node (None: not, or no longer)."""

    __slots__ = ("score", "updated", "useful", "seeded")

    def __init__(self, score: float, updated: float, useful: float | None = None, seeded: float | None = None):
        """A row as given."""
        self.score = score
        self.updated = updated
        self.useful = useful
        self.seeded = seeded


def decayed(value: float, since: float, now: float) -> float:
    """value, set at since, as it has decayed by now (a since in the future counts as now)."""
    return value * 0.5 ** (max(0.0, now - since) / SCORE_HALF_LIFE)


def band(score: float) -> int:
    """A score's rank band: 0 within ±1, else ±(1 + floor(log2 |score|)), so bands are a power of two wide."""
    if abs(score) < 1:
        return 0
    return int(math.copysign(1 + math.floor(math.log2(abs(score))), score))


class Scores:
    """Every peer's local score. Thread-safe; a node.Store (save, close) when given a state_dir."""

    def __init__(self, state_dir: str | None = None, seeded: Iterable[bytes] = (),
                 clock: Callable[[], float] = time.time, max_rows: int = MAX_SCORES):
        """Scores saved in state_dir (only in memory without one), with the bootstrap nodes seeded given, on clock
        (Unix time)."""
        self.clock = clock
        self.max_rows = max_rows
        self._lock = threading.Lock()
        self._rows: OrderedDict[bytes, _Row] = OrderedDict()  # least recently useful first
        self._store: statedb.RowStore | None = None
        self._db = statedb.open_store(os.path.expanduser(state_dir), self._load) if state_dir else None
        self._seed(set(seeded))

    # Persistence

    def _load(self, db: statedb.StateDb):
        """Read the saved scores, least recently useful first (never raised before any raised)."""
        self._store = statedb.RowStore(db, TABLE, ("node",), durable=False, mode=statedb.SNAPSHOT, lock=self._lock,
                                       row=self._row)
        now = self.clock()
        changed = []
        for node, score, updated, useful, seeded in db.query(
                f"SELECT node, score, updated, useful, seeded FROM {TABLE} ORDER BY useful IS NOT NULL, useful"):
            times = [_time(value) for value in (updated, useful, seeded)]
            if not is_id(node) or _time(score) is None or times[0] is None or \
                    any(value is not None and t is None for value, t in zip((useful, seeded), times[1:])):
                changed.append(node)
                continue
            row = _Row(max(-SCORE_LIMIT, min(SCORE_LIMIT, score)), *(None if t is None else min(t, now)
                                                                     for t in times))
            if (row.score, row.updated, row.useful, row.seeded) != (score, updated, useful, seeded):
                changed.append(node)
            self._rows[bytes(node)] = row
        with self._lock:
            changed += self._evict()
            self._store.changed(*changed)

    def _row(self, node_id) -> tuple | None:
        """node_id's saved row, or None once it has none (lock held)."""
        row = self._rows.get(node_id)
        if row is None:
            return None
        return node_id, row.score, row.updated, row.useful, row.seeded

    def save(self):
        """Write the scores changed since the last save. Raises OSError (StateDbError) if the database fails; they
        are written by the next save."""
        if self._store is not None:
            self._store.save()

    def close(self):
        """Release the state database (idempotent)."""
        if self._db is not None:
            self._db.close()

    def _changed(self, node_id: bytes):
        """node_id's row changed or went (lock held)."""
        if self._store is not None:
            self._store.changed(node_id)

    def _evict(self) -> list[bytes]:
        """Forget the least recently useful rows past max_rows (lock held); the nodes forgotten."""
        gone = []
        while len(self._rows) > self.max_rows:
            node_id, _ = self._rows.popitem(last=False)
            gone.append(node_id)
            self._changed(node_id)
        return gone

    def _seed(self, bootstrap: set[bytes]):
        """Give each bootstrap node without a row BOOTSTRAP_SEED, and take what is left of the seed from nodes seeded
        before that aren't bootstrap nodes any more."""
        with self._lock:
            now = self.clock()
            for node_id, row in self._rows.items():
                if row.seeded is not None and node_id not in bootstrap:
                    # Within the limit: a score clamped at -SCORE_LIMIT has already lost the seed's share
                    row.score = max(-SCORE_LIMIT, decayed(row.score, row.updated, now)
                                    - decayed(BOOTSTRAP_SEED, row.seeded, now))
                    row.updated, row.seeded = now, None
                    self._changed(node_id)
            for node_id in sorted(bootstrap - self._rows.keys()):
                if is_id(node_id):
                    self._rows[node_id] = _Row(BOOTSTRAP_SEED, now, now, now)
                    self._changed(node_id)
            self._evict()

    # Scores

    def score(self, node_id: bytes) -> float:
        """node_id's score now (0 without a row)."""
        with self._lock:
            row = self._rows.get(node_id)
            return 0.0 if row is None else decayed(row.score, row.updated, self.clock())

    def band(self, node_id: bytes) -> int:
        """node_id's score's rank band (band())."""
        return band(self.score(node_id))

    def bands(self) -> dict[bytes, int]:
        """Every scored peer's band now, for ranking many at once."""
        with self._lock:
            now = self.clock()
            return {node_id: band(decayed(row.score, row.updated, now)) for node_id, row in self._rows.items()}

    def served(self, node_id: bytes):
        """node_id delivered blocks verified against their CIDs, in one response."""
        self._add(node_id, SERVED)

    def useful_answer(self, node_id: bytes):
        """A /find answer of node_id's proved useful (a provider it listed served, or a closer server it named
        answered)."""
        self._add(node_id, USEFUL_ANSWER)

    def failed(self, node_id: bytes):
        """A query or request to node_id failed."""
        self._add(node_id, FAILED)

    def bad_block(self, node_id: bytes):
        """node_id sent a block not matching its CID."""
        self._add(node_id, BAD_BLOCK)

    def _add(self, node_id: bytes, amount: float):
        """Add amount to node_id's score: raising it makes it the most recently useful; a row made by lowering goes
        first in line to be forgotten (when the table is full, at once: it would score as no row does, about 0)."""
        if not is_id(node_id):
            return
        with self._lock:
            now = self.clock()
            row = self._rows.get(node_id)
            if row is None:
                row = self._rows[node_id] = _Row(0.0, now)
                self._rows.move_to_end(node_id, last=False)
            row.score = max(-SCORE_LIMIT, min(SCORE_LIMIT, decayed(row.score, row.updated, now) + amount))
            row.updated = now
            if amount > 0:
                row.useful = now
                self._rows.move_to_end(node_id)
            self._changed(node_id)
            self._evict()


def _time(value) -> float | None:
    """A saved number as a float, or None if it isn't a finite number."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value)
