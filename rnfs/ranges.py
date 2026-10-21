"""/range on the requesting side (specification section 7, `/range`; rnfs 0.2.0): which nodes answer it, the partial
blocks received from each, and one range fetched into a partial block.

RangeSupport. A node before 0.2.0 has no handler for /range, and RNS leaves a request for a path without a handler
unanswered, so whether a node answers it is learnt. A /range that RNS times out with the link up and nothing of an
answer started, to a node not known to answer it, is a silence: the fetcher asks that provider with /blocks for the rest
of the fetch, at once and without counting it against it (UNSUPPORTED), and SILENCES of them in a row, in one fetch or
several, record that the node doesn't answer /range; any answer records that it does and forgets the silences. One
silence alone is what a lost packet on a lossy path looks like, which is where ranges pay most, so it marks nothing: an
older node costs SILENCES request timeouts per RECHECK. Kept per node ID in the state database (range_support), at most
MAX_NODES, the least recently checked dropped first; a "doesn't" and its silences are forgotten RECHECK after the last
silence, and the node asked afresh. Written only when a record changes (or is a day old), not on every range.

Partials. The bytes of a block received from one provider, in order, until the block is complete and verified whole
against its CID (PeerClient.block_range). One per (block, provider), at most MAX_PARTIALS, the least recently written
dropped first; in files under <state dir>/partial (in memory without a state directory), all deleted at start, and a
block's dropped once it is stored. Nothing here is a block: Storage never sees these bytes, so they are never served or
read, and a request uses a partial only while it holds it (acquire), so two requests never write one.
"""
import hashlib
import os
import threading
import time
from collections import OrderedDict
from typing import Callable

from rnfs.constants import DAY, MAX_BLOCK_SIZE
from rnfs.content.cid import Cid
from rnfs.log import LOG_VERBOSE, LOG_WARNING, log, node_name, size
from rnfs.network import client as peer_client

SILENCES = 2  # /range requests left unanswered in a row (link up, nothing of an answer started) that mark a node
RECHECK = 7 * DAY  # a node recorded as not answering /range is asked afresh this long after its last silence
REFRESH = DAY  # a record whose answer hasn't changed is written again (its time) at most this often
MAX_NODES = 10_000  # nodes whose answer is kept
MAX_PARTIALS = 64  # partial blocks kept (at most MAX_BLOCK_SIZE each: 8 MiB)
PARTIAL_DIR = "partial"  # under the state directory
PARTIAL_SUFFIX = ".part"
MAX_RATIO = 4.0  # the most a range asked for compressed is scaled up by the compression its provider achieved


class RangeSupport:
    """Whether each node answers /range: True, False (SILENCES silences in a row, within RECHECK of the last), or None
    (not known, perhaps with one silence counted). Thread-safe."""

    def __init__(self, db=None, clock: Callable[[], float] = time.time):
        """The records in db's range_support table (a statedb.StateDb; None: in memory only). clock is Unix time."""
        self.db = db
        self.clock = clock
        self.lock = threading.Lock()
        # node -> (answered /range last, silences in a row since, when last found), least recently checked first
        self.records: OrderedDict[bytes, tuple[bool, int, float]] = OrderedDict()
        if db is not None:
            for node, answers, silences, at in db.query(
                    "SELECT node, answers, silences, at FROM range_support ORDER BY at"):
                if isinstance(node, bytes) and isinstance(silences, int) and isinstance(at, (int, float)):
                    self.records[node] = (bool(answers), max(0, silences), float(at))

    def _silences(self, record, now: float) -> int:
        """The silences in a row a record counts now: none once RECHECK has passed since the last (asked afresh)."""
        answers, silences, at = record
        return 0 if answers or now - at >= RECHECK else silences

    def answers(self, node_id: bytes) -> bool | None:
        """True if node_id answers /range, False if it doesn't (SILENCES silences in a row, the last within RECHECK),
        None if not known."""
        with self.lock:
            record = self.records.get(node_id)
        if record is None:
            return None
        if record[0]:
            return True
        return False if self._silences(record, self.clock()) >= SILENCES else None

    def answered(self, node_id: bytes):
        """node_id answered a /range (False and [] included): it answers, and any silence counted is forgotten."""
        self._set(node_id, True, 0)

    def silent(self, node_id: bytes) -> bool:
        """node_id, not known to answer /range, left one unanswered with the link up: one more silence in a row.
        True if it now counts as not answering (SILENCES of them)."""
        with self.lock:
            record = self.records.get(node_id)
            silences = 1 + (self._silences(record, self.clock()) if record is not None else 0)
        self._set(node_id, False, silences)
        return silences >= SILENCES

    def record(self, node_id: bytes, answers: bool):
        """Record outright that node_id answers /range (True) or doesn't (False, as after SILENCES silences)."""
        self._set(node_id, answers, 0 if answers else SILENCES)

    def _set(self, node_id: bytes, answers: bool, silences: int):
        """Keep node_id's record, writing it if it changed or is REFRESH old."""
        now = self.clock()
        with self.lock:
            previous = self.records.pop(node_id, None)
            self.records[node_id] = (answers, silences, now)
            dropped = []
            while len(self.records) > MAX_NODES:
                dropped.append(self.records.popitem(last=False)[0])
            changed = previous is None or previous[:2] != (answers, silences) or now - previous[2] >= REFRESH
        if self.db is None or not (changed or dropped):
            return
        try:
            with self.db.transaction() as tx:
                tx.execute("INSERT OR REPLACE INTO range_support (node, answers, silences, at) VALUES (?, ?, ?, ?)",
                           (node_id, int(answers), silences, now))
                tx.executemany("DELETE FROM range_support WHERE node = ?", [(node,) for node in dropped])
        except OSError as e:
            log(f"could not save whether {node_name(node_id)} answers /range: {e}", LOG_WARNING)


class Partial:
    """The start of one block received from one provider (Partials.acquire)."""

    __slots__ = ("key", "cid", "node_id", "size", "length", "compress", "path", "data", "busy")

    def __init__(self, key: bytes, cid: Cid, node_id: bytes, path: str | None):
        """Nothing received yet."""
        self.key = key
        self.cid = cid
        self.node_id = node_id
        self.size: int | None = None  # the block's length, as the provider's first answer gave it
        self.length = 0  # bytes held
        self.compress = True  # False once a range of it came back uncompressed: it doesn't compress
        self.path = path  # its file, or None in memory
        self.data = bytearray() if path is None else None
        self.busy = False  # a request holds it


class Partials:
    """Partial blocks, per (block, provider): the module docstring. Thread-safe."""

    def __init__(self, directory: str | None, limit: int = MAX_PARTIALS):
        """Partials in files in directory (created if missing; any left by an earlier run deleted), or in memory if
        directory is None; at most limit of them."""
        self.directory = directory
        self.limit = limit
        self.lock = threading.Lock()
        self.partials: OrderedDict[bytes, Partial] = OrderedDict()  # least recently written first
        if directory is not None:
            os.makedirs(directory, mode=0o700, exist_ok=True)
            for name in os.listdir(directory):
                if name.endswith(PARTIAL_SUFFIX):
                    self._remove(os.path.join(directory, name))

    @staticmethod
    def _key(cid: Cid, node_id: bytes) -> bytes:
        """The partial's key, and its file's name."""
        return hashlib.sha256(cid.binary + node_id).digest()[:16]

    def length(self, cid: Cid, node_id: bytes) -> int:
        """Bytes held of cid's block from node_id (0 if none)."""
        with self.lock:
            partial = self.partials.get(self._key(cid, node_id))
            return partial.length if partial is not None else 0

    def acquire(self, cid: Cid, node_id: bytes) -> Partial | None:
        """cid's partial block from node_id (a new, empty one if there is none), held until release(); None if
        another request holds it, or every partial is held and there is no room for another."""
        key = self._key(cid, node_id)
        with self.lock:
            partial = self.partials.get(key)
            if partial is None:
                if len(self.partials) >= self.limit and not self._evict():
                    return None
                path = None if self.directory is None else os.path.join(self.directory, key.hex() + PARTIAL_SUFFIX)
                partial = self.partials[key] = Partial(key, cid, node_id, path)
            elif partial.busy:
                return None
            partial.busy = True
            return partial

    def _evict(self) -> bool:
        """Drop the least recently written partial no request holds. False if every one is held. Caller holds the
        lock."""
        for key, partial in self.partials.items():
            if not partial.busy:
                del self.partials[key]
                self._delete(partial)
                return True
        return False

    def read(self, partial: Partial) -> bytes:
        """The bytes held (the partial is dropped and b"" returned if its file can't be read whole)."""
        if partial.path is None:
            return bytes(partial.data)
        if partial.length == 0:
            return b""
        try:
            with open(partial.path, "rb") as f:
                data = f.read(MAX_BLOCK_SIZE + 1)
        except OSError as e:
            log(f"could not read the partial block of {partial.cid}: {e}", LOG_WARNING)
            data = b""
        if len(data) != partial.length:
            self.reset(partial)
            return b""
        return data

    def append(self, partial: Partial, block_size: int, data: bytes) -> bool:
        """Add the next range's bytes (the block being block_size bytes). False if they couldn't be written: the
        partial is then empty again."""
        partial.size = block_size
        if partial.path is None:
            partial.data += data
        else:
            try:
                with open(partial.path, "ab") as f:
                    f.write(data)
            except OSError as e:
                log(f"could not write the partial block of {partial.cid}: {e}", LOG_WARNING)
                self.reset(partial)
                return False
        with self.lock:
            partial.length += len(data)
            if partial.key in self.partials:
                self.partials.move_to_end(partial.key)
        return True

    def reset(self, partial: Partial):
        """Empty partial (a malformed or inconsistent answer, or an unreadable file): asked again from the start."""
        self._delete(partial)
        partial.size = None
        partial.length = 0
        partial.compress = True
        if partial.data is not None:
            partial.data = bytearray()

    def release(self, partial: Partial):
        """The request holding partial is done with it."""
        with self.lock:
            partial.busy = False

    def drop(self, cid: Cid):
        """Forget every provider's partial of cid's block (it is stored), except one a request holds (it drops its
        own once the block it completes is verified)."""
        with self.lock:
            gone = [key for key, partial in self.partials.items() if partial.cid == cid and not partial.busy]
            partials = [self.partials.pop(key) for key in gone]
        for partial in partials:
            self._delete(partial)

    def forget(self, partial: Partial):
        """Forget partial, held by the caller (its block completed, or it can't be continued)."""
        with self.lock:
            if self.partials.get(partial.key) is partial:
                del self.partials[partial.key]
        self._delete(partial)

    def count(self) -> int:
        """Partials kept."""
        with self.lock:
            return len(self.partials)

    def _delete(self, partial: Partial):
        """Remove partial's file, if it has one."""
        if partial.path is not None:
            self._remove(partial.path)

    @staticmethod
    def _remove(path: str):
        """Remove a partial's file, if it is there."""
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError as e:
            log(f"could not remove the partial block {path}: {e}", LOG_WARNING)


class Ranges:
    """The fetcher's /range state: which nodes answer it, and the partial blocks (the module docstring)."""

    def __init__(self, support: RangeSupport, partials: Partials):
        """Ranges with support records and partial blocks."""
        self.support = support
        self.partials = partials

    def usable(self, node_id: bytes) -> bool:
        """Whether node_id may be asked with /range: it answers it, or isn't known not to."""
        return self.support.answers(node_id) is not False

    def fetch(self, client, node_id: bytes, cid: Cid, target: int, ratio: float, compress: bool,
              block_size: int | None, on_progress=None) -> peer_client.BlocksResult | None:
        """Ask node_id for the next range of cid's block after what is held from it (a request thread's work), and
        return what came of it as a /blocks request's result: the block (verified whole), progress (.partial), a
        status, or UNSUPPORTED if node_id left the request unanswered and isn't known to answer /range (a silence,
        RangeSupport.silent). None if the
        partial is in use by another request: ask with /blocks instead.

        target is the range's size on the wire; with compress, the bytes asked for are target times ratio (the
        compression node_id's last range achieved, MAX_RATIO at most), unless a range of this block came back
        uncompressed. block_size is the block's length if its parent declares it.
        """
        partial = self.partials.acquire(cid, node_id)
        if partial is None:
            return None
        try:
            return self._fetch(client, node_id, cid, partial, target, ratio, compress, block_size, on_progress)
        finally:
            self.partials.release(partial)

    def _fetch(self, client, node_id: bytes, cid: Cid, partial: Partial, target: int, ratio: float, compress: bool,
               block_size: int | None, on_progress) -> peer_client.BlocksResult:
        """fetch(), with partial held."""
        if block_size is not None and partial.size is not None and partial.size != block_size:
            self.partials.reset(partial)  # not the block its parent declares: start again (the answer is checked)
        held = self.partials.read(partial)
        known = block_size if block_size is not None else partial.size
        if known is not None and len(held) >= known:
            self.partials.reset(partial)
            held = b""
        compress = compress and partial.compress
        wanted = int(target * min(max(ratio, 1.0), MAX_RATIO)) if compress else int(target)
        max_bytes = max(1, min(wanted, MAX_BLOCK_SIZE))
        if known is not None:
            max_bytes = max(1, min(max_bytes, known - len(held)))
        answers = self.support.answers(node_id)
        result = client.block_range(node_id, cid, held, max_bytes, compress, known, on_progress, answers=answers)
        if result.silent and answers is not True:
            # As an older node leaves it (no handler for /range, section 3), or a lost packet on a lossy path: asked
            # with /blocks for the rest of this fetch; twice in a row and it is taken for an older node
            if self.support.silent(node_id):
                log(f"{node_name(node_id)} doesn't answer /range (rnfs before 0.2.0, most likely): asking it with "
                    "/blocks", LOG_VERBOSE)
            else:
                log(f"{node_name(node_id)} left /range unanswered once (a second time in a row would mark it as not "
                    "answering): asking it with /blocks for now", LOG_VERBOSE)
            return peer_client.BlocksResult(peer_client.UNSUPPORTED, unsent=[cid], reason="no answer to /range")
        if result.silent:
            # Known to answer, yet nothing at all came back: a lost packet, or a record made in error (an older node,
            # REL-5). Not known again, so the next silence is counted as an older node's; this one is retried as lost
            self.support.silent(node_id)
        # What only a /range handler gives: an answer, a refusal, a response started, a malformed answer. Not a link
        # closed by the other end (before the request was sent, or as a busy older node evicts a link it sees idle)
        if result.status == peer_client.OK or result.reason in ("busy", "cancelled", "malformed response"):
            self.support.answered(node_id)
        if result.status != peer_client.OK:
            if result.reason == "malformed response":
                self.partials.forget(partial)
            return peer_client.BlocksResult(result.status, unsent=[cid], reason=result.reason)
        if result.missing:
            self.partials.forget(partial)
            return peer_client.BlocksResult(peer_client.OK, missing=[cid], elapsed=result.elapsed)
        ratio = len(result.data) / result.wire if result.compressed and result.wire else None
        if result.bad or result.block is not None:
            self.partials.forget(partial)
            if result.bad:
                return peer_client.BlocksResult(peer_client.OK, bad=[cid], elapsed=result.elapsed)
            return peer_client.BlocksResult(peer_client.OK, blocks={cid: result.block}, elapsed=result.elapsed,
                                            ratio=ratio)
        if compress and not result.compressed:
            partial.compress = False  # it doesn't compress (or the provider's budget is spent): ask the rest raw
        if not self.partials.append(partial, result.size, result.data):
            return peer_client.BlocksResult(peer_client.OK, partial={cid: 0}, elapsed=result.elapsed, ratio=ratio)
        log(f"{node_name(node_id)} sent {size(len(result.data))} of {cid}"
            + (f" in {size(result.wire)}" if result.compressed else "")
            + f": {size(partial.length)} of {size(result.size)} held", LOG_VERBOSE)
        return peer_client.BlocksResult(peer_client.OK, partial={cid: partial.length}, elapsed=result.elapsed,
                                        ratio=ratio)
