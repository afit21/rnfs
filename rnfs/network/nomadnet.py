"""Fetching a page or file from a NomadNet node, for `rnfs add <destination>:/page/<path>` and `:/file/<path>`
(specification section 11, "Adding from NomadNet"; rnfs/nomadnet_links.py has the links' syntax).

The fetch opens a link of its own to the node's `nomadnetwork.node` destination (a path request and an identity
recall first, as rnfs does for nodes, each within path_timeout), sends one request for the path and waits for the
answer: a page as bytes, a file as a Resource carrying `{"name": ...}` metadata, copied into a file in `directory`
which the caller adds and removes. The link doesn't identify unless the caller passes an identity (`--identify`:
the dedicated one in storage/nomadnet_identity, never the node's), and is closed when the fetch ends.

NomadNet compresses what it sends (files up to 32 MB) and sends anything over one Resource segment
(RNS.Resource.MAX_EFFICIENT_SIZE, 1 MiB) as several, while the daemon sets RNS's decompression limit to 0 for the
whole process (network/guards.py). So the fetch bounds its own link instead, and nothing else changes: rnfs.dht
links keep every rule they have, and an embedded node leaves its host's settings as they are.
- The request carries max_response_size: the caller's bound (min(max_download_size, the free space above the disk
  reserve)), which RNS checks against every segment's advertised total size. Its advertisement is read here too
  (Transfer.receive), only to report a refusal for size as one (TooLarge).
- A resource_started callback (Transfer.resource_started, run by RNS before any data arrives) gives each segment
  a decompression limit of one segment (MAX_EFFICIENT_SIZE + 1: what an honest segment holds, at most), and requires
  every segment to carry the first one's original hash and total size, the next segment index (the previous one
  complete), and the segment count an honest sender computes from that total (segment_refusal). RNS checks neither
  the sequence nor the decompressed size against the total, so a node could otherwise send segments without end.
  An answer without metadata (a page) is refused at its first segment if its total is over MAX_PAGE_SIZE: RNS reads
  such an answer whole into memory and decodes it (Link.response_resource_concluded), while a file is handed over
  as an open file. Anything else cancels the transfer and closes the link.
- After each segment (Transfer.resource_concluded, which RNS calls as a segment completes, before it decodes a
  file's metadata): what RNS has assembled must be no larger than the advertised total, and a file's metadata must
  be a small flat map (protocol.metadata_refusal), else it is removed unread and the link closed.
- A page answer is checked from its msgpack headers before RNS decodes it (guards: protocol.page_response_refusal):
  one flat byte string. A file answer is copied, no larger than the advertised total, inside the response callback,
  since RNS deletes its file once the callback returns.
- An answer sent as a packet (a page that fits one, such as NomadNet's "Request Not Allowed") that arrives before
  RNS has registered its request is kept and handed over once it is (links.EarlyResponses, as on rnfs.dht links):
  RNS (1.5.6) would drop it, and the fetch fail after RNS's request timeout. An answer sent as a Resource can arrive
  early too: RNS ignores an advertisement no pending request matches inside Link.receive, which EarlyResponses (a
  handle_response hook) doesn't reach. That costs a moment, not the fetch: the node sends the advertisement again
  once its Resource's timeout passes (the round trip × traffic_timeout_factor, plus 1 s), and by then the request
  is registered. Replaying the advertisement here would save that second at the price of feeding a packet back
  into RNS's Link.receive, so the fetch leaves it to RNS.
- Path requests take their turn in the node's budget (network/paths.py: not part of path_timeout), and a link that
  fails to establish costs a turn, as RNS then requests the path again.
- Timeouts: the path and the identity (path_timeout each), the link's establishment (RNS's timeout plus
  links.ESTABLISH_GRACE), and the answer: RNS's own request timeout until an answer starts, then no progress for
  stall_timeout, or an overall deadline (the answer's size at FLOOR_RATE) fails the fetch.

Everything here blocks its caller, so never call it from an RNS callback thread.
"""
import os
import secrets
import threading
import time
import types
from typing import Callable, NamedTuple

import RNS

from rnfs import nomadnet_links
from rnfs.errors import Cancelled, NotFound, TooLarge
from rnfs.log import LOG_VERBOSE, LOG_WARNING, log, node_name, size
from rnfs.network import guards, links, protocol
from rnfs.network.paths import PathRequests

SEGMENT_SIZE = RNS.Resource.MAX_EFFICIENT_SIZE  # the most an honest Resource segment holds, metadata included
SEGMENT_LIMIT = SEGMENT_SIZE + 1  # each segment's max_decompressed_size: RNS refuses one that reaches it unfinished
# The largest answer without metadata (a page): RNS reads one whole and decodes it into a second copy before rnfs
# sees it, so this, not the free disk, bounds its memory. NomadNet pages are micron text, a few KiB
MAX_PAGE_SIZE = 16 * 1024 * 1024
FLOOR_RATE = 32  # bytes/s: an answer slower than this overall fails (about 256 bit/s, below the slowest LoRa)
COPY_CHUNK = 1024 * 1024  # bytes copied at a time from RNS's file
PARTIAL_PREFIX, PARTIAL_SUFFIX = ".rnfs-nomadnet-", ".partial"  # the copy's name in the caller's directory


class Snapshot(NamedTuple):
    """What a NomadNet node answered, copied to path (the caller adds it and removes the file)."""

    path: str
    size: int
    sent_name: str | bytes | None  # the name a file's metadata gave, as sent: unchecked (nomadnet_links.snapshot_name)
    file: bool  # sent as a file (a Resource with metadata) rather than a page


def expected_segments(total: int) -> int:
    """How many segments an honest sender splits total bytes (data and metadata) into (RNS.Resource.__init__)."""
    return 1 if total <= SEGMENT_SIZE else (total - 1) // SEGMENT_SIZE + 1


def segment_refusal(resource, first, previous) -> str | None:
    """Why resource, starting on a NomadNet link, isn't the segment an honest sender sends next; None if it is.

    first and previous are the answer's first and latest segments so far (None for the first). The first must be
    segment 1 whose original hash is its own hash (as RNS makes it); every one must carry the first's original hash,
    total size, metadata flag and the segment count expected_segments gives for that total, with the index after the
    previous one's, which must be complete.
    """
    index, count, total = (getattr(resource, name, None) for name in ("segment_index", "total_segments", "total_size"))
    if not all(type(value) is int for value in (index, count, total)) or total < 0:
        return "malformed segment fields"
    if count != expected_segments(total):
        return f"{count} segments for {total} bytes (an honest sender sends {expected_segments(total)})"
    if first is None:
        if index != 1:
            return f"an answer starting at segment {index}"
        if resource.original_hash != resource.hash:
            return "a first segment whose original hash isn't its own"
        return None
    if (resource.original_hash, total, bool(resource.has_metadata)) != \
            (first.original_hash, first.total_size, bool(first.has_metadata)):
        return f"segment {index} of another answer (its original hash, total size or metadata differ)"
    if index != previous.segment_index + 1:
        return f"segment {index} after segment {previous.segment_index}"
    if getattr(previous, "status", None) != RNS.Resource.COMPLETE:
        return f"segment {index} while segment {previous.segment_index} is still arriving"
    return None


class Transfer:
    """One request on a NomadNet link and its answer: RNS callbacks record into it (only quick work, besides the
    copy of a file answer, which must happen inside the callback) while the fetching thread waits on it."""

    def __init__(self, link, partial: str, max_size: int | None, clock: Callable[[], float] = time.monotonic):
        """A request about to be sent on link, its answer to be copied to partial (created exclusively)."""
        self.link = link
        self.partial = partial
        self.max_size = max_size
        self.clock = clock
        self.lock = threading.Lock()
        self.done = threading.Event()
        self.first = self.previous = None  # the answer's first and latest Resource segments
        self.refused: str | None = None  # why the answer was refused, if it was
        self.too_large: int | None = None  # an answer's advertised size over max_size, which RNS rejected
        self.page_too_large: int | None = None  # a page answer's advertised size over MAX_PAGE_SIZE, refused here
        self.answered = False
        self.empty = False  # answered None or False: nothing, or an answer the guards refused
        self.size = 0  # bytes of the answer copied
        self.sent_name = None
        self.file = False
        self.progress = 0.0
        self.total: int | None = None  # the answer's advertised size, once a Resource starts
        self.started: float | None = None  # when the answer's first segment started
        self.last_progress = clock()

    # Hooks on the link (instance attributes: RNS calls link.receive and link.resource_concluded through them)

    def receive(self, packet):
        """The link's receive: note an answer advertised larger than max_size, which RNS rejects without saying
        why, then RNS's (guarded) Link.receive."""
        if getattr(packet, "context", None) == RNS.Packet.RESOURCE_ADV and self.max_size is not None:
            try:
                advertised = RNS.ResourceAdvertisement.unpack(self.link.decrypt(packet.data)).d
            except Exception:  # noqa: BLE001 - unreadable advertisements are left to RNS
                advertised = None
            if type(advertised) is int and advertised > self.max_size:
                self.too_large = advertised
        return RNS.Link.receive(self.link, packet)

    def resource_started(self, resource):
        """RNS's resource_started callback (before any of the segment arrives): bound its decompression and check
        its place in the sequence (segment_refusal) and, for a page, its total (MAX_PAGE_SIZE: later segments must
        carry the first's total and metadata flag); refuse it otherwise."""
        resource.max_decompressed_size = SEGMENT_LIMIT
        with self.lock:
            reason = segment_refusal(resource, self.first, self.previous)
            if reason is None and self.first is None and not resource.has_metadata \
                    and resource.total_size > MAX_PAGE_SIZE:
                self.page_too_large = resource.total_size
                reason = f"a page of {resource.total_size} bytes, over the {MAX_PAGE_SIZE} a page may have"
            if reason is None:
                if self.first is None:
                    self.first, self.total, self.started = resource, resource.total_size, self.clock()
                self.previous = resource
                self.last_progress = self.clock()
        if reason is not None:
            self.refuse(reason, resource)

    def resource_concluded(self, resource):
        """The link's resource_concluded, which RNS calls as a segment ends (before a file's metadata is decoded):
        RNS's own, then the checks on what it assembled so far."""
        RNS.Link.resource_concluded(self.link, resource)
        if getattr(resource, "status", None) != RNS.Resource.COMPLETE:
            return
        reason = None
        try:
            assembled = os.path.getsize(resource.storagepath)
            if self.total is not None and assembled > self.total:
                reason = f"{assembled} bytes assembled, over the {self.total} advertised"
        except OSError:
            pass  # nothing assembled yet, or RNS moved on
        if reason is None and resource.has_metadata and resource.segment_index == 1:
            reason = self._metadata_refusal(resource.meta_storagepath)
        if reason is not None:
            self.refuse(reason)

    @staticmethod
    def _metadata_refusal(path: str) -> str | None:
        """Why the metadata RNS stored at path isn't a NomadNet file's (protocol.metadata_refusal), read before RNS
        decodes it; removed if so, so RNS never decodes it."""
        try:
            with open(path, "rb") as f:
                packed = f.read(protocol.MAX_METADATA_SIZE + 1)
        except OSError:
            return "metadata missing"
        reason = protocol.metadata_refusal(packed)
        if reason is not None:
            _remove(path)
        return reason

    def refuse(self, reason: str, resource=None):
        """Refuse the answer: cancel resource (if given, still transferring) and close the link."""
        with self.lock:
            if self.refused is None:
                self.refused = reason
        log(f"refused an answer from NomadNet node {node_name(self._destination())}: {reason}", LOG_WARNING)
        if resource is not None:
            try:
                resource.cancel()
            except Exception:  # noqa: BLE001 - RNS internals: the link's teardown ends it anyway
                pass
        self.link.teardown()
        self.done.set()

    def _destination(self) -> bytes:
        """The NomadNet node's destination hash, for messages."""
        return bytes(getattr(getattr(self.link, "destination", None), "hash", b"") or b"")

    # Request callbacks (RNS threads)

    def on_response(self, receipt):
        """The answer: a page's bytes (or None or False) or a file's open file and metadata, copied to partial here,
        since RNS deletes its file once this returns."""
        try:
            if self.refused is None:
                self._copy(receipt.response, receipt.metadata)
        except Exception as e:  # noqa: BLE001 - on an RNS thread: recorded as a refusal, never raised into RNS
            with self.lock:
                self.refused = self.refused or f"its answer couldn't be stored ({e})"
        finally:
            self.done.set()

    def _copy(self, response, metadata):
        """Copy response (bytes or str: a page; a file: a file answer) to partial, at most the advertised total."""
        if response is None or response is False:
            self.empty = True
            self.answered = True
            return
        if isinstance(response, str):
            response = response.encode("utf-8")
        if isinstance(response, (bytes, bytearray)):
            if self.max_size is not None and len(response) > self.max_size:
                raise TooLarge(f"{len(response)} bytes")
            with open(self.partial, "xb") as out:
                out.write(response)
            self.size = len(response)
        elif hasattr(response, "read"):
            limit = self.total if self.total is not None else 0
            copied = 0
            with open(self.partial, "xb") as out:
                while copied <= limit:
                    data = response.read(min(COPY_CHUNK, limit + 1 - copied))
                    if not data:
                        break
                    out.write(data)
                    copied += len(data)
            if copied > limit:
                raise ValueError(f"the file is larger than the {limit} bytes advertised")
            self.size, self.file = copied, True
            self.sent_name = _sent_name(metadata)
        else:
            raise ValueError(f"an answer of type {type(response).__name__}")
        self.answered = True

    def on_failed(self, receipt):
        """The request timed out, its answer was rejected for its size, or the answer's transfer failed."""
        self.done.set()

    def on_progress(self, receipt):
        """Note progress receiving the answer, for the stall check and the caller's progress."""
        progress = receipt.get_progress()
        if isinstance(progress, (int, float)) and progress > self.progress:
            self.progress = float(progress)
            self.last_progress = self.clock()


def _sent_name(metadata):
    """The name in a file's metadata ({"name": ...}, as NomadNet sends it), or None."""
    if not isinstance(metadata, dict):
        return None
    name = metadata.get("name", metadata.get(b"name"))
    return name if isinstance(name, (str, bytes)) else None


def _remove(path: str):
    """Remove a file if it is there."""
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    except OSError as e:
        log(f"couldn't remove {path}: {e}", LOG_WARNING)


def remove_leftovers(directory: str):
    """Remove the copies an interrupted fetch left in directory (the node does this as it starts)."""
    try:
        names = os.listdir(directory)
    except OSError:
        return
    for name in names:
        if name.startswith(PARTIAL_PREFIX) and name.endswith(PARTIAL_SUFFIX):
            _remove(os.path.join(directory, name))


def _recall(destination_hash: bytes, timeout: float, paths: PathRequests):
    """The identity RNS knows for destination_hash, after a path request in its turn in paths (whose answer carries
    it) if needed."""
    identity = RNS.Identity.recall(destination_hash)
    if identity is not None:
        return identity
    if paths.await_known(destination_hash, lambda: RNS.Identity.recall(destination_hash, _no_use=True) is not None,
                         timeout):
        return RNS.Identity.recall(destination_hash) or RNS.Identity.recall(destination_hash, _no_use=True)
    return None


def open_link(target: nomadnet_links.NomadnetLink, path_timeout: float, paths: PathRequests | None = None):
    """An established link to target's NomadNet node, marked for the guards; its path requests take their turns in
    paths (the node's budget; None: a budget of this fetch's own). Raises NotFound if there is no path, no identity,
    or the link can't be established in time."""
    destination_hash, name = target.destination, node_name(target.destination)
    paths = paths if paths is not None else PathRequests()
    if not RNS.Transport.has_path(destination_hash):
        log(f"requesting a path to NomadNet node {name}", LOG_VERBOSE)
    if not paths.await_path(destination_hash, path_timeout):
        raise NotFound(f"no path to NomadNet node {name} after {path_timeout:g}s")
    identity = _recall(destination_hash, path_timeout, paths)
    if identity is None:
        raise NotFound(f"have a path to NomadNet node {name} but not its identity (none after {path_timeout:g}s)")
    destination = RNS.Destination(identity, RNS.Destination.OUT, RNS.Destination.SINGLE, nomadnet_links.APP_NAME,
                                  nomadnet_links.ASPECT)
    if destination.hash != destination_hash:
        # Another destination of that identity (an LXMF address, say): only its nomadnetwork.node serves pages
        raise NotFound(f"{name} isn't a NomadNet node (its identity's nomadnetwork.node is "
                       f"{node_name(destination.hash)})")
    settled = threading.Event()
    link = RNS.Link(destination, established_callback=lambda _: settled.set(),
                    closed_callback=lambda _: settled.set())
    guards.mark_nomadnet(link)  # before anything can arrive on it: no request is sent until it is established
    timeout = getattr(link, "establishment_timeout", None) or path_timeout
    settled.wait(timeout + links.ESTABLISH_GRACE)
    if link.status != RNS.Link.ACTIVE:
        link.teardown()
        paths.charge(destination_hash)  # RNS now requests the path again by itself
        raise NotFound(f"couldn't connect to NomadNet node {name}: " + (
            links.close_reason(link) if link.status == RNS.Link.CLOSED else "not established in time"))
    return link


def fetch(target: nomadnet_links.NomadnetLink, config, directory: str, identity=None, max_size: int | None = None,
          progress: Callable[[int, int | None], None] | None = None,
          stop_event: threading.Event | None = None, paths: PathRequests | None = None) -> Snapshot:
    """Fetch target from its NomadNet node into a file in directory (see the module): its Snapshot.

    identity (an RNS.Identity) identifies the link first; a "Request Not Allowed" page on an identified link is
    retried once after identifying again (Link.identify is unacknowledged). max_size bounds the answer.
    progress(received, total) is called from this thread about every links.POLL_INTERVAL while the answer arrives
    (total None until it is known), and may raise to stop the fetch (a job's Cancelled); stop_event, once set, stops
    it too. Raises NotFound (unreachable, no answer, an answer refused, or NomadNet's "Request Not Allowed" page),
    TooLarge (the answer is over max_size, or a page over MAX_PAGE_SIZE), Cancelled, OSError. paths is the node's
    path-request budget (open_link).
    """
    link = open_link(target, config.path_timeout, paths)
    try:
        early = links.EarlyResponses.install(link)  # before any request: no answer can come sooner
        if identity is not None:
            link.identify(identity)
        snapshot = _request(link, target, config, directory, max_size, progress, stop_event, early)
        if identity is not None and _not_allowed(snapshot):
            log(f"NomadNet node {node_name(target.destination)} didn't allow {target.path}; identifying again",
                LOG_VERBOSE)
            _remove(snapshot.path)
            link.identify(identity)
            snapshot = _request(link, target, config, directory, max_size, progress, stop_event, early)
        if _not_allowed(snapshot):
            _remove(snapshot.path)
            how = (f"its identity {identity.hash.hex()} (storage/nomadnet_identity) isn't on the list"
                   if identity is not None else "with --identify, if it lists rnfs's NomadNet identity")
            raise NotFound(f"NomadNet node {node_name(target.destination)} answered {target.path} with \"Request "
                           f"Not Allowed\": it serves it only to the identities it lists ({how})")
        return snapshot
    finally:
        link.teardown()


def _not_allowed(snapshot: Snapshot) -> bool:
    """True if snapshot is NomadNet's "Request Not Allowed" page."""
    if snapshot.size > 4 * len(nomadnet_links.NOT_ALLOWED):
        return False
    with open(snapshot.path, "rb") as f:
        return nomadnet_links.is_not_allowed(f.read())


def _request(link, target: nomadnet_links.NomadnetLink, config, directory: str, max_size: int | None,
             progress, stop_event, early: links.EarlyResponses | None = None) -> Snapshot:
    """Send target's request on link and wait for its answer (fetch). early holds the link's answers that came
    before RNS registered their request (None: a link without RNS's pending requests, a test's)."""
    partial = os.path.join(directory, f"{PARTIAL_PREFIX}{secrets.token_hex(8)}{PARTIAL_SUFFIX}")
    transfer = Transfer(link, partial, max_size)
    link.set_resource_started_callback(transfer.resource_started)
    link.resource_concluded = transfer.resource_concluded
    link.receive = transfer.receive
    ok = False
    try:
        receipt = link.request(target.path, None, response_callback=transfer.on_response,
                               failed_callback=transfer.on_failed, progress_callback=transfer.on_progress,
                               timeout=None, max_response_size=max_size)
        if not receipt:
            raise NotFound(f"couldn't send the request for {target.path} to NomadNet node "
                           f"{node_name(target.destination)}")
        if early is not None and early.deliver(receipt):  # its callbacks have run: transfer has the answer
            log(f"NomadNet node {node_name(target.destination)} answered {target.path} before RNS had registered "
                f"the request; handed the answer over", LOG_VERBOSE)
        _wait(link, target, transfer, config, progress, stop_event)
        snapshot = _outcome(target, transfer, max_size)
        ok = True
        return snapshot
    finally:
        if not ok:
            _remove(partial)
            _remove_rns_files(transfer)


def _wait(link, target: nomadnet_links.NomadnetLink, transfer: Transfer, config, progress, stop_event):
    """Wait for transfer's answer to target's request, within the timeouts (module docstring)."""
    what = f"NomadNet node {node_name(target.destination)}'s answer to {target.path}"
    clock = transfer.clock
    sent = clock()
    # As rnfs.dht requests other than /blocks: twice RNS's own RTT-based request timeout
    request_deadline = links.LinkPool.request_deadline(types.SimpleNamespace(link=link))
    while not transfer.done.wait(links.POLL_INTERVAL):
        if stop_event is not None and stop_event.is_set():
            raise Cancelled("the node is stopping")
        if link.status != RNS.Link.ACTIVE:
            return
        now = clock()
        total = transfer.total
        if progress is not None:
            progress(int(transfer.progress * total) if total else 0, total)  # every poll: it may raise to stop
        if transfer.started is None:
            if now - sent > request_deadline:
                raise NotFound(f"no {what} within {request_deadline:.0f}s")
            continue
        if now - transfer.last_progress > config.stall_timeout:
            raise NotFound(f"{what} made no progress for {config.stall_timeout:g}s")
        overall = max(request_deadline, (total or 0) / FLOOR_RATE)
        if now - transfer.started > overall:
            raise NotFound(f"{what} took longer than {overall:.0f}s")


def _outcome(target: nomadnet_links.NomadnetLink, transfer: Transfer, max_size: int | None) -> Snapshot:
    """The Snapshot of a transfer that ended, or the error saying why there is none."""
    name, path = node_name(target.destination), target.path
    if transfer.too_large is not None and not transfer.answered:
        raise TooLarge(f"NomadNet node {name}'s answer to {path} is {size(transfer.too_large)}, over the "
                       f"{size(max_size)} allowed (max_download_size, or the free space above the disk reserve)")
    if transfer.page_too_large is not None:
        raise TooLarge(f"NomadNet node {name}'s answer to {path} is a page (an answer without file "
                       f"metadata) of {size(transfer.page_too_large)}, over the {size(MAX_PAGE_SIZE)} a page may "
                       f"have: RNS reads a page into memory whole")
    if transfer.refused is not None:
        raise NotFound(f"refused NomadNet node {name}'s answer to {path}: {transfer.refused}")
    if not transfer.answered:
        status = getattr(transfer.previous, "status", None)
        why = ("a segment decompressed past its bound" if status == RNS.Resource.CORRUPT
               else "the link closed" if transfer.link.status != RNS.Link.ACTIVE
               else "no answer (it may not serve that path)")
        raise NotFound(f"NomadNet node {name} didn't answer {path}: {why}")
    if transfer.empty:
        raise NotFound(f"NomadNet node {name} sent nothing for {path} (an error on its side, or an answer that "
                       f"wasn't a page: see the log)")
    return Snapshot(transfer.partial, transfer.size, transfer.sent_name, transfer.file)


def _remove_rns_files(transfer: Transfer):
    """Remove what RNS kept of a refused or unfinished answer: it appends segments to a file named by the answer's
    original hash in its resource directory, and deletes it only once the last one is handed over."""
    first = transfer.first
    for path in (getattr(first, "storagepath", None), getattr(first, "meta_storagepath", None)):
        if isinstance(path, str):
            _remove(path)

