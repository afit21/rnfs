"""The node's side of pinning (rnfs/node.py's RnfsNode inherits NodePins; logic kept out of the shared file):

- Local pin details: pin add, rm, ls and label, and what pin ls reads from a pin's stored blocks (its type, the name
  inside, sealed status, entries and size), remembered for complete pins (PIN_MEMO).
- Remote pins (rnfs.remote_pins): asking another node to pin or release a CID and checking on it, and the pins other
  nodes ask of this one: accepted or refused, fetched by a worker per source, retried while not found, saved, and
  resumed at the next start.

The state these methods use is RnfsNode's, set up in its __init__.
"""
import math
import threading
import time
from typing import NamedTuple

import RNS

from rnfs import events, node_content, remote_pins
from rnfs.constants import APP_NAME, DHT_ASPECT
from rnfs.content import dag, seal
from rnfs.content.cid import Cid
from rnfs.content.storage import FOLDER, REMOTE_PREFIX, USER, app_of, remote_source
from rnfs.errors import Cancelled, Denied, InvalidDag, NotFound, RnfsError, StorageFull, TooLarge
from rnfs.fetcher import Observer, observing
from rnfs.log import LOG_ERROR, LOG_NOTICE, LOG_VERBOSE, LOG_WARNING, log, plural, size
from rnfs.remote_pins import DROPPED, FETCHING, PINNED, REFUSED, RELEASED, WAITING
from rnfs.retry import RETRY_FOR, counted_since, next_retry

REMOTE_PIN_QUEUE = 64  # remote pins one source may have waiting or being fetched; more are refused
# Complete pins whose pin_ls type, name from inside and sealed status are remembered (about 3 MB when full); pins
# past it are read each time
PIN_MEMO = 16384


def _caller_source(source) -> str:
    """source if a facade call may pin or release with it for its caller: "user" (the CLI, an embedded node), or
    an app's app:<name>[:<key>] (an app's token, rnfs.api); ValueError otherwise, so no caller takes another
    feature's pins (folder, name:, remote:, ...)."""
    if source != USER and app_of(source) is None:
        raise ValueError(f"invalid pin source {source!r} for a caller: user or app:<name>[:<key>]")
    return source


# Why a remote pin's fetch failed, by the error's class (rnfs.remote_pins.REASONS); any other is "failed"
_FAILURE_REASONS = {NotFound: "not_found", Denied: "denied", InvalidDag: "invalid_dag", TooLarge: "too_large",
                    StorageFull: "storage_full"}


def _finite(value) -> bool:
    """True if value is an int or float, finite (a time read back from the state database)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _remote_identity(source: str) -> bytes | None:
    """The identity hash in a remote:<identity> pin source, or None for any other source."""
    if not source.startswith(REMOTE_PREFIX):
        return None
    try:
        identity = bytes.fromhex(source[len(REMOTE_PREFIX):])
    except ValueError:
        return None
    return identity if len(identity) == 16 else None


class _PinDetails(NamedTuple):
    """What pin_ls reads from a pin's stored blocks (RnfsNode._pin_details): None for what they can't tell."""

    kind: str | None  # "file" or "directory"
    inside: str | None  # a directory's one entry's name, or "(directory: N entries)"
    sealed: bool | None  # a sealed file
    entries: int | None  # a directory's entry count
    size: int | None  # content bytes, as the root block states them


class NodePins:
    """The pin facade and remote-pin machinery of RnfsNode, which inherits it."""

    # Facade: pins

    def pin_add(self, target: str, source: str = USER) -> dict:
        """Fetch the target completely and pin it with source ("user", or the caller's app's)."""
        _caller_source(source)
        root, names = self._parse_target(target)
        with self.pinned_fetch(root, names) as fetch:
            expect = fetch.resolve()
            # At most max_output_entries entries, as get takes: a tree of empty files declares 0 bytes, and its
            # directory nodes pay for themselves under the DAG byte bound (AUD-M6)
            fetch.fetch_dag(expect, self.config.max_output_entries)
            self.storage.pin(expect.cid, source)
        self._app_still_there(source, [expect.cid])
        if target != str(expect.cid):  # a path or a contact nick says more than the CID does
            self.pin_names.record_if_unknown(expect.cid, target)
        log(f"pinned {expect.cid}", LOG_NOTICE)
        self.publisher.refresh()
        return {"cid": str(expect.cid)}

    def pin_rm(self, cid: str, source: str = USER):
        """Remove source's hold on a pin ("user", or the caller's app's: an app releases only its own); withdraws
        the CID at once if no longer provided."""
        _caller_source(source)
        parsed = Cid.parse(cid)
        if not self.storage.unpin(parsed, source):
            raise NotFound(f"{cid} is not pinned by the user" if source == USER else f"{cid} is not pinned by {source}")
        self.publisher.refresh()

    def _app_still_there(self, source: str, cids: list[Cid]):
        """After pinning cids with source: if it is an app's and the app was removed meanwhile (app_rm took its
        token, then released its pins, maybe before these were made), release them and raise NotFound, so a
        request already let in never leaves a pin nobody may release. app_rm forgets the app before it looks for
        its pins, so a pin made before that is released there, and one made after it here."""
        name = app_of(source)
        if name is None or name in self.apps:
            return
        for cid in cids:
            self.storage.unpin(cid, source)
        raise NotFound(f"app {name} was removed meanwhile, so its pin was released")

    def pin_ls(self, cid: str | None = None) -> list[dict]:
        """Pins with their sources, whether they're complete, their type, whether they're sealed files, a local
        name when one is known, and (rnfs 0.2.0) their label, origin, size, entry count and when added.

        origin is where the pin was added from on this machine (pin_names) or the shared-folder entry it came
        from; label the name its user gave it (pin_label), kept apart from origin. name, as before 0.2.0, is the
        label, else the origin, else for a stored directory its single entry's name (add --wrap) or its entry
        count as text. For a sealed file origin is still the local path or the name given to add (this node's own
        record, for its user): `sealed` tells callers that show pins to others not to show the name or the content.
        `sealed` is None when it can't be told without fetching (the root or the file's first chunk isn't stored).
        size is the content size the root block states (None until it is stored), entries a directory's entry
        count (None for a file, or while a large directory's index isn't all stored), added when it was first
        pinned (None for a pin made before rnfs 0.2.0).
        """
        pins = self.storage.pins()
        self.pin_names.prune(set(pins))
        with self._pin_memo_lock:
            for gone in [c for c in self._pin_memo if c not in pins]:
                del self._pin_memo[gone]
        if cid is not None:
            wanted = Cid.parse(cid)
            pins = {wanted: pins[wanted]} if wanted in pins else {}
        folder_names = self.shared_folder.names()
        result = []
        for c, sources in sorted(pins.items(), key=lambda item: str(item[0])):
            complete = self.storage.is_complete(c)
            details = self._pin_details(c, complete)
            origin = self.pin_names.get(c) or (folder_names.get(c) if FOLDER in sources else None)
            label = self.pin_names.label(c)
            result.append({"cid": str(c), "sources": sorted(sources), "complete": complete,
                           "type": details.kind, "sealed": details.sealed,
                           "name": label or origin or details.inside, "label": label, "origin": origin,
                           "size": details.size, "entries": details.entries, "added": self.storage.pin_added(c)})
        return result

    def pin_label(self, cid: str, label: str | None) -> dict:
        """Give a pin a label (None or "" clears it), kept apart from where it was added from: its pin_ls entry.
        NotFound if cid isn't pinned; ValueError for a label pin_names.check_label refuses. A label lasts while
        the CID stays pinned (by any source)."""
        parsed = Cid.parse(cid)
        if not self.storage.is_pinned(parsed):
            raise NotFound(f"{cid} is not pinned")
        if self.pin_names.set_label(parsed, label):
            events.publish(self, "pin", change="label", source=None, label=self.pin_names.label(parsed),
                           **events.cid_fields([parsed]))
        listed = self.pin_ls(str(parsed))
        if not listed:  # released meanwhile
            raise NotFound(f"{cid} is not pinned")
        return listed[0]

    def _pin_details(self, cid: Cid, complete: bool) -> "_PinDetails":
        """A pin's _PinDetails (_pin_shape, _pin_sealed), remembered while it is complete.

        They are read from stored blocks, which a complete pin has all of and whose content never changes, so
        they hold for as long as it stays complete; an incomplete pin's are read each time (a block may arrive,
        or have gone). pin_ls forgets the pins released; at most PIN_MEMO are remembered, the first met kept:
        pin_ls goes through them in the same order each time, so replacing older ones would remember none.
        """
        if complete:
            with self._pin_memo_lock:
                known = self._pin_memo.get(cid)
            if known is not None:
                return known
        shape = self._pin_shape(cid)
        details = shape._replace(sealed=self._pin_sealed(cid, shape.kind))
        # A complete DAG's blocks always tell them all; None means a block went while they were read
        if complete and details.kind is not None and details.sealed is not None \
                and (details.kind == "file" or details.entries is not None):
            with self._pin_memo_lock:
                if len(self._pin_memo) < PIN_MEMO:
                    self._pin_memo[cid] = details
        return details

    def _pin_sealed(self, cid: Cid, kind: str | None) -> bool | None:
        """True if a pinned file is sealed (its first bytes carry the sealed-file magic), False if it isn't or
        is a directory, None if that isn't known from stored blocks. Never fetches."""
        if kind == "directory":
            return False
        if kind is None:
            return None
        # The user's own list of what is stored: whether a denied pin is sealed is no secret, and isn't worth a hash
        head = node_content.stored_head(self, str(cid), len(seal.MAGIC), deny=False)
        return None if head is None else seal.is_sealed(head)

    def _pin_shape(self, cid: Cid, count: bool = True) -> "_PinDetails":
        """The _PinDetails of a pin whose root block is stored, but sealed (None): its type ("file" or
        "directory"), for a directory its one entry's name or "(directory: N entries)" (inside; a large
        directory's index root, "(directory)" as before 0.2.0) and, if count, its entry count, and its content
        size. All None if the root isn't stored (or isn't a valid DAG node)."""
        unknown = _PinDetails(None, None, None, None, None)
        if cid.is_raw:  # a raw block is its own content: its size is known without reading it
            size = self.storage.block_size(cid)
            return unknown if size is None else _PinDetails("file", None, None, None, size)
        block = self.storage.get(cid)
        if block is None:
            return unknown
        try:
            node = dag.decode(block)
        except InvalidDag:
            return unknown
        if isinstance(node, dag.FileNode):
            return _PinDetails("file", None, None, None, node.size)
        if isinstance(node, dag.DirectoryNode):
            inside = node.entries[0].name if len(node.entries) == 1 else f"(directory: {len(node.entries)} entries)"
            return _PinDetails("directory", inside, None, len(node.entries), node.size)
        return _PinDetails("directory", "(directory)", None, self._index_entries(node) if count else None, node.size)

    def _index_entries(self, index: dag.IndexNode) -> int | None:
        """The entries below a large directory's index node, counted from its stored groups; None if one of its
        index or group nodes isn't stored (or isn't valid), or if the count would pass max_output_entries. Never
        fetches. A node the index lists twice is invalid (a valid index never repeats a child), which also stops
        a few crafted blocks from making the walk grow exponentially; at most max_output_entries nodes are read."""
        count = 0
        seen: set[Cid] = set()
        stack = list(index.child_cids)
        limit = self.config.max_output_entries
        while stack:
            cid = stack.pop()
            if cid in seen or len(seen) >= limit:
                return None
            seen.add(cid)
            block = self.storage.get(cid)
            if block is None:
                return None
            try:
                node = dag.decode(block)
            except InvalidDag:
                return None
            if isinstance(node, dag.DirectoryNode):
                count += len(node.entries)
                if count > limit:
                    return None
            elif isinstance(node, dag.IndexNode):
                stack.extend(node.child_cids)
            else:
                return None
        return count

    def pin_remote(self, node_id: bytes, cid: str, pin: bool) -> bool:
        """Ask node_id to pin (or release our pin of) cid over an identified link; True if accepted. The ask and its
        answer are recorded (pin_remote_ls), and saved at once."""
        parsed = Cid.parse(cid)
        accepted = self.client.pin(node_id, parsed, pin)
        self.asked_pins.record(node_id, parsed, pin, bool(accepted))
        try:
            self.asked_pins.save()
        except OSError as e:  # saved by the next save of the batched stores
            log(f"saving the remote pins asked failed: {e!r}", LOG_ERROR)
        return accepted

    def pin_remote_ls(self, node_id: bytes | None = None, cid: str | None = None, check: bool = False) -> list[dict]:
        """The remote pins this node asked for (node_id's, cid's, or every one), most recently asked first
        (rnfs.remote_pins.AskedPins.entries); check: check the accepted ones first, on demand (check_now: a provider
        lookup each, paced on slow nodes)."""
        parsed = Cid.parse(cid) if cid is not None else None
        if check:
            self.asked_pins.check_now(node_id, parsed)
        return self.asked_pins.entries(node_id, parsed)

    def _providing(self, node_id: bytes, cid: Cid) -> bool | None:
        """Whether node_id provides cid, for the remote pins asked (rnfs.remote_pins): its record held here, or a
        provider lookup finding it, which stops once it does. False if the lookup ends without it; None if no node
        answered the lookup, or this node is stopping (inconclusive)."""
        if node_id == self.node_id:
            return cid in self.publisher.provided()
        if node_id in self.records.providers(cid.key):
            return True
        found = threading.Event()
        lookup = self._lookup(cid.key)
        lookup.find_providers(lambda provider: found.set() if provider == node_id else None,
                              should_stop=lambda: found.is_set() or self.stop_event.is_set())
        if found.is_set():
            return True
        if self.stop_event.is_set() or not lookup.closest_answered():
            return None
        return False

    def pin_remote_served(self) -> dict:
        """The remote pins other identities asked of this node: {"pins": [{"identity", "cid", "state", "reason",
        "at", "detail", "retry_at"}], most recent first, "usage": [{"identity", "used", "quota", "pinned", "waiting",
        "allowed"}], "quota"}. Current pins (storage's remote:<identity> sources) are "pinned" and those queued
        "waiting" or "fetching", whatever their recorded outcome says; the rest are the outcomes kept
        (rnfs.remote_pins.ServedPins). An outcome saying a pin is current that no longer is was lost to a crash
        ("dropped", reason "lost"). retry_at is when a pin waiting because its content wasn't found is tried again
        (None for the rest)."""
        outcomes = self.served_pins.outcomes()
        current: dict[tuple[bytes, Cid], str] = {}
        counts: dict[bytes, list[int]] = {}  # identity -> [pinned, waiting]
        for cid, sources in self.storage.pins().items():
            for source in sources:
                identity = _remote_identity(source)
                if identity is not None:
                    current[(identity, cid)] = PINNED
                    counts.setdefault(identity, [0, 0])[0] += 1
        with self._lock:
            queued = [(source, cid) for source, cids in self._remote_reserved.items() for cid in cids
                      if (source, cid) not in self._remote_released]
            retry_at = {(_remote_identity(source), cid): retry[0] for (source, cid), retry in self._remote_retry.items()
                        if self._remote_current.get(source) != cid}
        for source, cid in queued:
            identity = _remote_identity(source)
            if identity is None or (identity, cid) in current:
                continue
            outcome = outcomes.get((identity, cid))
            current[(identity, cid)] = FETCHING if outcome is not None and outcome.state == FETCHING else WAITING
            counts.setdefault(identity, [0, 0])[1] += 1
        pins = []
        for key in {**dict.fromkeys(outcomes), **dict.fromkeys(current)}:
            outcome = outcomes.get(key)
            state = current.get(key)
            if state is None:  # not current: as recorded, unless that says it is
                state = outcome.state
                if state in (WAITING, FETCHING, PINNED):
                    outcome, state = remote_pins.Outcome(DROPPED, "lost", outcome.at), DROPPED
            recorded = outcome if outcome is not None and outcome.state == state else None
            at = recorded.at if recorded is not None else None
            if at is None and state == PINNED:
                at = self.storage.pin_added(key[1])  # pinned before 0.2.0, or its outcome forgotten
            pins.append({"identity": key[0].hex(), "cid": str(key[1]), "state": state,
                         "reason": recorded.reason if recorded is not None else None, "at": at,
                         "detail": recorded.detail if recorded is not None else None,
                         "retry_at": retry_at.get(key) if state == WAITING else None})
        pins.sort(key=lambda pin: pin["at"] if pin["at"] is not None else float("-inf"), reverse=True)
        allowed = {bytes.fromhex(text) for text in self.config.remote_pin_set}
        quota = self.config.remote_pin_quota
        usage = [{"identity": identity.hex(), "used": self.storage.source_usage(remote_source(identity)),
                  "quota": quota, "pinned": counts.get(identity, [0, 0])[0],
                  "waiting": counts.get(identity, [0, 0])[1], "allowed": identity in allowed}
                 for identity in sorted(allowed | set(counts))]
        return {"pins": pins, "usage": usage, "quota": quota}

    def _served(self, source: str, cid: Cid, state: str, reason: str | None = None, detail: str | None = None):
        """Record what became of source's remote pin of cid (rnfs.remote_pins.ServedPins); a source that isn't a
        remote one is left out (a damaged row of the remote pins waiting)."""
        identity = _remote_identity(source)
        if identity is not None:
            self.served_pins.record(identity, cid, state, reason, detail)

    def remote_pin(self, identity_hash: bytes, cid: Cid, pin: bool) -> bool:
        """A /pin from an allowed identity: release at once, or fetch then pin with source remote:<identity>.

        A source's pins are fetched one at a time, so each is checked against its quota with the earlier ones
        counted; one with REMOTE_PIN_QUEUE pins waiting is refused (False), and so is one whose fetch thread
        can't be started. Accepted pins are saved (the remote_pins table) until fetched, and resumed at the next
        start if the node stops first.
        """
        source = remote_source(identity_hash)
        if not pin:
            released = False
            with self._lock:
                if (source, cid) in self._remote_retry and self._remote_current.get(source) != cid:
                    # Waiting to be tried again, which may be hours away: given up at once
                    self._remote_retry.pop((source, cid))
                    self._remote_reserved[source].pop(cid, None)
                    self._remote_pins_dirty = True
                    released = True
                    log(f"released remote pin of {cid} for {source}, which was waiting to be tried again", LOG_NOTICE)
                elif cid in self._remote_reserved.get(source, {}):
                    # Still being fetched: it mustn't be pinned when the fetch ends
                    self._remote_released.add((source, cid))
                    self._remote_pins_dirty = True
                    released = True
                    log(f"released remote pin of {cid} for {source} while fetching it", LOG_NOTICE)
            if self.storage.unpin(cid, source):
                released = True
                log(f"released remote pin of {cid} for {source}", LOG_NOTICE)
                self.publisher.refresh()
            if released:
                self._served(source, cid, RELEASED)
            return True
        if self.deny.denies(cid):
            log(f"refused remote pin of {cid} for {source}: it is on this node's deny list", LOG_WARNING)
            self._served(source, cid, REFUSED, "denied")
            return False
        with self._lock:
            reserved = self._remote_reserved.setdefault(source, {})
            if cid in reserved:
                if (source, cid) in self._remote_released:
                    self._remote_released.discard((source, cid))  # pinned again before the fetch ended
                    self._remote_pins_dirty = True
                    self._served(source, cid, WAITING)
                return True
            if source in self.storage.pins().get(cid, set()):
                return True
            if len(reserved) >= REMOTE_PIN_QUEUE:
                log(f"refused remote pin of {cid} for {source}: {len(reserved)} of its pins are waiting", LOG_WARNING)
                self._served(source, cid, REFUSED, "queue_full", f"{len(reserved)} of its pins are waiting")
                return False
            reserved[cid] = None
            self._remote_pins_dirty = True
            # Recorded before its worker can take it (that needs the lock held here), so "fetching" comes after
            if not self._ensure_remote_worker(source, cid):
                self._served(source, cid, REFUSED, "failed", "its fetch couldn't be started")
                return False
            self._served(source, cid, WAITING)
            return True

    def _ensure_remote_worker(self, source: str, cid: Cid) -> bool:
        """Start source's fetch thread unless it has one; False if it can't be started, with cid (the pin just
        reserved) given back. Caller holds the lock.

        A worker leaves _remote_workers in the same critical section in which it finds nothing waiting, so a
        pin reserved after that starts a new one, and one reserved before it is fetched by the old one.
        """
        try:
            self._start_remote_worker(source)
        except RuntimeError as e:  # can't start new thread: out of threads or memory
            reserved = self._remote_reserved.get(source, {})
            reserved.pop(cid, None)
            self._remote_retry.pop((source, cid), None)
            if not reserved:
                self._remote_reserved.pop(source, None)
            self._remote_pins_dirty = True
            log(f"refused remote pin of {cid} for {source}: couldn't start its fetch: {e}", LOG_ERROR)
            return False
        return True

    def _start_remote_worker(self, source: str):
        """Start source's fetch thread unless it has one. Raises RuntimeError if it can't be started. Caller holds
        the lock."""
        if source in self._remote_workers:
            return
        thread = threading.Thread(target=self._remote_pin_worker, args=(source,), name="rnfs-remote-pin",
                                  daemon=True)
        thread.start()
        # Registered after start() succeeds, still under the lock, so the worker can't look before it is
        self._remote_workers[source] = thread

    def _next_remote_pin(self, source: str, now: float) -> Cid | None:
        """source's oldest pin waiting that is due: not waiting to be tried again, or due to be by now (Unix time).
        Caller holds the lock."""
        for cid in self._remote_reserved.get(source, {}):
            retry = self._remote_retry.get((source, cid))
            if retry is None or retry[0] <= now:
                return cid
        return None

    def _retry_remote_pins(self):
        """Start the fetch thread of each source with a pin due to be tried again and no thread (maintenance, about
        every second); one that can't be started is tried again at the next pass."""
        now = time.time()
        with self._lock:
            if self.stop_event.is_set() or not self._remote_retry:
                return
            due = {source for (source, _), (retry_at, _) in self._remote_retry.items()
                   if retry_at <= now and source not in self._remote_workers}
            for source in due:
                try:
                    self._start_remote_worker(source)
                except RuntimeError as e:  # out of threads or memory: the next pass tries again
                    log(f"couldn't start the fetch of the remote pins of {source}: {e}", LOG_ERROR)

    def _remote_pin_worker(self, source: str):
        """Fetch source's remote pins one at a time, oldest first, until none is waiting or the node stops.

        Concurrent fetches would each be checked against the quota without the others' blocks, so together
        they could store several times it. Pins waiting when the node stops stay saved for the next start. Pins
        waiting to be tried again are skipped until they are due; once none is due the worker ends, and maintenance
        starts one again when the next is (_retry_remote_pins).
        """
        # Resumed and early pins wait for the join: a lookup before it would find no provider and drop them
        while not self.joined.is_set():
            if self.stop_event.wait(0.5):
                break
        while True:
            with self._lock:
                cid = None if self.stop_event.is_set() else self._next_remote_pin(source, time.time())
                if cid is None:
                    # Deregistered in the same critical section that saw nothing due: a /pin arriving after it
                    # starts a new worker instead of relying on this one, which is about to return
                    self._remote_workers.pop(source, None)
                    return
                self._remote_current[source] = cid
            try:
                self._remote_pin_fetch(cid, source)  # removes cid from _remote_reserved when done
            except BaseException:
                with self._lock:
                    self._remote_workers.pop(source, None)
                raise

    def _save_remote_pins(self):
        """Save the remote pins waiting or being fetched, in the order accepted (released ones left out), if
        they changed, so a restart resumes them. Run by maintenance (about every second) and at stop, not on
        every /pin: a crash loses at most the last second's (/pin is best effort, section 8)."""
        with self._remote_pins_save_lock:
            with self._lock:
                if not self._remote_pins_dirty:
                    return
                waiting = [(source, cid) for source, cids in self._remote_reserved.items() for cid in cids
                           if (source, cid) not in self._remote_released]
                retries = [(source, cid.binary, retry_at, since)
                           for (source, cid), (retry_at, since) in self._remote_retry.items()
                           if (source, cid) not in self._remote_released]
                self._remote_pins_dirty = False
            try:
                with self._db.transaction() as tx:  # a few rows (at most REMOTE_PIN_QUEUE per source): rewritten
                    tx.execute("DELETE FROM remote_pins")
                    tx.executemany("INSERT INTO remote_pins (position, source, cid) VALUES (?, ?, ?)",
                                   [(position, source, cid.binary) for position, (source, cid) in enumerate(waiting)])
                    tx.execute("DELETE FROM remote_pin_retries")
                    tx.executemany("INSERT INTO remote_pin_retries (source, cid, retry_at, since) VALUES (?, ?, ?, ?)",
                                   retries)
            except OSError as e:
                with self._lock:
                    self._remote_pins_dirty = True  # try again next time
                log(f"saving the remote pins waiting failed: {e!r}", LOG_ERROR)

    def _load_remote_pins(self) -> list[tuple[str, Cid]]:
        """Remote pins saved as waiting, oldest first, from identities still on remote_pin_identities (at most
        REMOTE_PIN_QUEUE each); others are dropped. Caller holds no lock (it reads the database)."""
        allowed = {remote_source(h) for h in self.config.remote_pin_set}
        try:
            saved = self._db.query("SELECT source, cid FROM remote_pins ORDER BY position")
        except OSError as e:
            log(f"reading the remote pins waiting failed: {e!r}", LOG_ERROR)
            return []
        pins: dict[tuple[str, Cid], None] = {}
        counts: dict[str, int] = {}
        malformed = 0
        for source, binary in saved:
            try:
                if not isinstance(source, str):
                    raise TypeError("not a string")
                cid = Cid.from_bytes(binary)
            except (ValueError, TypeError):
                malformed += 1
                continue
            if source not in allowed:
                self._served(source, cid, DROPPED, "identity_removed")
                continue
            if (source, cid) in pins or counts.get(source, 0) >= REMOTE_PIN_QUEUE:
                continue
            counts[source] = counts.get(source, 0) + 1
            pins[(source, cid)] = None
        if malformed:
            log(f"skipped {malformed} malformed remote pin{'' if malformed == 1 else 's'} saved as waiting",
                LOG_WARNING)
        return list(pins)

    def _load_remote_retries(self) -> dict[tuple[str, Cid], tuple[float, float]]:
        """When each remote pin saved as waiting for its content to be found is tried again and was first not found
        ((retry_at, since), Unix times), by (source, CID); a malformed row is left out (its pin is tried at once).
        Caller holds no lock (it reads the database)."""
        try:
            saved = self._db.query("SELECT source, cid, retry_at, since FROM remote_pin_retries")
        except OSError as e:
            log(f"reading the remote pins waiting to be tried again failed: {e!r}", LOG_ERROR)
            return {}
        retries = {}
        for source, binary, retry_at, since in saved:
            try:
                cid = Cid.from_bytes(binary)
            except (ValueError, TypeError):
                continue
            if isinstance(source, str) and _finite(retry_at) and _finite(since):
                retries[(source, cid)] = (float(retry_at), float(since))
        return retries

    def _resume_remote_pins(self):
        """Queue the remote pins saved as waiting when the node last stopped (called once at start)."""
        pins = self._load_remote_pins()
        retries = self._load_remote_retries()
        resumed = 0
        with self._lock:
            pinned = self.storage.pins()
            for source, cid in pins:
                if source not in pinned.get(cid, set()):
                    self._remote_reserved.setdefault(source, {})[cid] = None
                    if (source, cid) in retries:
                        self._remote_retry[(source, cid)] = retries[(source, cid)]
                    resumed += 1
            self._remote_pins_dirty = True
            for source, cids in list(self._remote_reserved.items()):
                # A thread that can't be started gives its pin back, so this ends. One whose pins all wait to be tried
                # again is started by maintenance once the first is due
                while cids and self._next_remote_pin(source, time.time()) is not None:
                    first = next(iter(cids))
                    if self._ensure_remote_worker(source, first):
                        break
                    self._served(source, first, DROPPED, "failed", "its fetch couldn't be started")
        if resumed:
            log(f"resuming {plural(resumed, 'remote pin')} accepted before the last stop", LOG_NOTICE)

    def _remote_pin_fetch(self, cid: Cid, source: str):
        """Fetch cid for a remote pin, dropping it as soon as its verified size, or the bytes of its DAG's
        blocks met so far, would exceed the quota. The fetch ends at its next step once the node stops.

        The node that asked is asked first (the fetch's hint: its node ID follows from its identity), as it serves
        what it provides before its provider records are out. A pin whose content isn't found (NotFound) waits and
        is tried again later (rnfs.retry: 10 minutes, doubling up to 6 hours, for up to 3 days), keeping what its
        fetch got as a cached fetch (pinned_fetch); then it is dropped as not found.
        """
        quota = self.config.remote_pin_quota
        identity = _remote_identity(source)
        hint = RNS.Destination.hash(identity, APP_NAME, DHT_ASPECT) if identity is not None else None
        waits = False
        observer = Observer()
        observer.cancelled = self.stop_event.is_set
        try:
            with self._lock:
                if (source, cid) in self._remote_released:
                    return  # released while it waited
            self._served(source, cid, FETCHING)
            with observing(observer), self.pinned_fetch(cid, hint=hint) as fetch:
                expect = fetch.resolve()
                content = fetch.size_of(expect)
                # Nothing else is being fetched for source (_remote_pin_worker), so its pins are all it uses
                used = self.storage.source_usage(source)
                if used + content > quota:
                    why = f"{size(content)} would exceed its quota ({size(used)} of {size(quota)} used)"
                    log(f"dropping remote pin of {cid} for {source}: {why}", LOG_WARNING)
                    self._served(source, cid, DROPPED, "quota", why)
                    return
                # A DAG of empty files has size 0 yet can hold gigabytes of directory nodes: count its blocks
                # (fetch_dag also bounds them by what the size can hold, whichever is lower)
                fetch.max_dag_bytes = quota - used
                try:
                    fetch.fetch_dag(expect, self.config.max_output_entries)  # as pin_add (AUD-M6)
                except TooLarge as e:
                    if getattr(e, "entries", False):
                        raise  # too many entries, not its quota: dropped as too_large (below)
                    why = (f"its blocks would exceed its quota ({size(used)} of {size(quota)} used) or what its size "
                           f"can hold: {e}")
                    log(f"dropping remote pin of {cid} for {source}: {why}", LOG_WARNING)
                    self._served(source, cid, DROPPED, "quota", why)
                    return
                with self._lock:
                    # Checked and pinned under the lock, so a release arriving now is either seen or undoes it
                    if (source, cid) in self._remote_released:
                        log(f"not pinning {cid} for {source}: released while it was fetched", LOG_NOTICE)
                        return
                    self.storage.pin(cid, source)
                    self._served(source, cid, PINNED)
            log(f"pinned {cid} for {source}", LOG_NOTICE)
            self.publisher.refresh()
        except Cancelled:
            log(f"remote pin of {cid} for {source} stopped with the node: it is fetched after the next start",
                LOG_VERBOSE)
            self._served(source, cid, WAITING)
        except NotFound as e:
            waits = self._not_found(source, cid, e)
        except RnfsError as e:
            log(f"remote pin of {cid} for {source} failed: {e}", LOG_WARNING)
            self._fetch_failed(source, cid, _FAILURE_REASONS.get(type(e), "failed"), str(e))
        except Exception as e:  # noqa: BLE001 - the pin waits for the next try; logged
            log(f"remote pin of {cid} for {source} failed: {e!r}", LOG_ERROR)
            self._fetch_failed(source, cid, "failed", repr(e))
        finally:
            with self._lock:
                self._remote_current.pop(source, None)
                # Once the node is stopping, a fetch cut short stays waiting, so it is saved for the next start
                # (a pin that completed meanwhile is skipped then); so does one waiting to be tried again
                if not self.stop_event.is_set() and not waits:
                    self._remote_reserved.get(source, {}).pop(cid, None)
                    self._remote_retry.pop((source, cid), None)
                    self._remote_released.discard((source, cid))
                self._remote_pins_dirty = True

    def _not_found(self, source: str, cid: Cid, error: NotFound) -> bool:
        """A remote pin's content wasn't found: wait to try again (True), or drop it once rnfs.retry.RETRY_FOR has
        passed since it was first not found (False), or if it was released while it was fetched (False, its release
        recorded already). Waiting when the node is stopping too, as any pin cut short."""
        now = time.time()
        with self._lock:
            if (source, cid) in self._remote_released:
                return False
            due, since = self._remote_retry.get((source, cid), (None, now))
            since = counted_since(since, now, due)
            retry_at = next_retry(since, now)
            if retry_at is not None:
                self._remote_retry[(source, cid)] = (retry_at, since)
        if retry_at is None:
            log(f"remote pin of {cid} for {source} failed: {error}; not found for "
                f"{RETRY_FOR / 86400:g} days", LOG_WARNING)
            self._fetch_failed(source, cid, "not_found", f"{error}; not found for {RETRY_FOR / 86400:g} days")
            return False
        log(f"remote pin of {cid} for {source}: {error}; trying again in {(retry_at - now) / 60:.0f} minutes",
            LOG_NOTICE)
        self._served(source, cid, WAITING, "not_found", str(error))
        return True

    def _fetch_failed(self, source: str, cid: Cid, reason: str, detail: str):
        """Record a remote pin's fetch that failed: dropped, unless the node is stopping, which keeps it waiting
        for the next start (_remote_pin_fetch's finally)."""
        if self.stop_event.is_set():
            self._served(source, cid, WAITING)
        else:
            self._served(source, cid, DROPPED, reason, detail)
