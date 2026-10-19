"""Sweep providing (specification section 6, "Publishing"): the K closest servers of many keys from one lookup per
region rather than one per key. No RNS dependency: the lookups are made by the caller (rnfs.publisher).

- **Regions.** The due keys are taken in turn (Sweep.take); for the first not yet covered, x, the closest-nodes
  lookup runs as for one key. p is the length of the prefix x shares with the farthest of its K closest, and the
  region is every due key sharing x's first p bits. Every server closer to x than that one shares those bits, so
  the region holds at least K servers and each of its keys' K closest are inside it (with fewer than K found, the
  farthest found sets p). Lookups per round level off at about servers / 12.5, however many keys are due.
- **The far half.** The half of the region holding x is known in full: its servers are all closer to x than the
  K-th. If the region holds 2 or more due keys, one /find goes to the known server nearest the centre of the other
  half, about that centre; a reply naming K servers inside it means there may be more, so each half of it is asked
  about in turn, at most MAX_REGION_FINDS in all. Every key of the region then takes its K closest from every
  server learned (Region.closest_to), ranked as find_closest ranks them: unreliable servers that haven't answered
  last. In the load study this placed 100% of records on their true K closest for about 1.6 extra /find a region.
- **Running lookups at once.** Each lookup running claims the keys it will probably cover (those sharing one bit
  fewer than the last region's p), so the next takes a key outside every claim, and the regions of lookups run
  at once seldom overlap; a key two regions cover keeps the first. Until a region has been found there is no
  estimate, and the first lookup runs alone.
"""
import bisect
import heapq
import threading
from collections.abc import Callable, Iterable

from rnfs.constants import ID_LENGTH, K
from rnfs.dht.routing import distance

ID_BITS = ID_LENGTH * 8
# Extra /find a region sends at most: the far half, then each half of a part whose reply was full, breadth first
# (three levels). The load study needed 1.6 on average
MAX_REGION_FINDS = 7
REGION_FIND_TRIES = 3  # servers asked about one part, nearest its centre first, before giving up on it


def prefix_length(a: bytes, b: bytes) -> int:
    """How many leading bits a and b share (ID_BITS if equal)."""
    return ID_BITS - distance(a, b).bit_length()


def _shares(value: int, prefix: int, bits: int) -> bool:
    """True if value's first bits bits are prefix's."""
    shift = ID_BITS - bits
    return value >> shift == prefix >> shift


class Region:
    """What one region's lookup learned: the region is the keys sharing the first `bits` bits of `key`, the key looked
    up; `closest` is that lookup's result for it; `nodes` every server learned, each with whether it ranks last
    (unreliable and not answered); `finds` the extra /find sent."""

    __slots__ = ("key", "bits", "closest", "nodes", "finds")

    def __init__(self, key: bytes, bits: int, closest: list[bytes], nodes: dict[bytes, bool], finds: int = 0):
        """A region found by a lookup for key."""
        self.key = key
        self.bits = bits
        self.closest = closest
        self.nodes = nodes
        self.finds = finds

    def __contains__(self, key: bytes) -> bool:
        """True if key is in the region (shares its first bits with the key looked up)."""
        return prefix_length(key, self.key) >= self.bits

    def closest_to(self, key: bytes, count: int = K) -> list[bytes]:
        """The count servers learned closest to key, ranked as a lookup ranks them (unreliable ones that haven't
        answered after every other). For the key looked up, never fewer than its lookup found: the far half's
        replies can name a server closer to it than one the lookup chose, never take one away."""
        if key == self.key:
            count = max(count, len(self.closest))
        nodes = self.nodes
        return heapq.nsmallest(count, nodes, key=lambda n: (nodes[n], distance(n, key)))


def find_region(lookup, key: bytes, own_id: bytes, members: Callable[[int], int]) -> Region:
    """Run lookup (a dht.lookup.Lookup for key) and find key's region; members(bits) is how many due keys share key's
    first bits bits, so the far half is asked about only for a region of 2 or more."""
    closest = [n for n in lookup.find_closest() if n != own_id]
    ranked = sorted(closest, key=lambda n: distance(n, key))[:K]
    # Without any server found there is nothing to learn: every due key shares the outcome (retried soon)
    bits = prefix_length(key, ranked[-1]) if ranked else 0
    finds = explore(lookup, key, bits, own_id) if ranked and members(bits) >= 2 else 0
    nodes = {n: doubtful for n, doubtful in lookup.candidates().items() if n != own_id}
    for n in closest:
        nodes.setdefault(n, False)
    return Region(key, bits, closest, nodes, finds)


def explore(lookup, key: bytes, bits: int, own_id: bytes) -> int:
    """Ask about the far half of the region of key sharing its first bits bits (the half not holding key), and about
    each half of a part again while replies come back full, until MAX_REGION_FINDS have been sent. The servers
    named join lookup's candidates. Returns the /find sent."""
    if bits >= ID_BITS:
        return 0
    value = int.from_bytes(key, "big")
    # The far half: key's first bits bits, then the other value of the next
    parts = [(value ^ (1 << (ID_BITS - 1 - bits)), bits + 1)]
    sent = 0
    while parts and sent < MAX_REGION_FINDS:
        prefix, length = parts.pop(0)
        base = prefix >> (ID_BITS - length) << (ID_BITS - length)
        centre = base | (1 << (ID_BITS - 1 - length)) if length < ID_BITS else base
        centre_id = centre.to_bytes(ID_LENGTH, "big")
        asked: set[bytes] = {own_id}
        failed = 0
        while sent < MAX_REGION_FINDS and failed < REGION_FIND_TRIES:
            candidates = lookup.candidates()
            node_id = min((n for n in candidates if n not in asked), default=None,
                          key=lambda n: (candidates[n], distance(n, centre_id)))
            if node_id is None:
                break
            asked.add(node_id)
            sent += 1
            named = lookup.ask(node_id, centre_id)
            if named is None:
                failed += 1
                continue
            if sum(1 for n in named if _shares(int.from_bytes(n, "big"), base, length)) >= K:
                # Full: the part may hold more than one reply can name, so each half of it is asked about
                if length < ID_BITS:
                    half = 1 << (ID_BITS - 1 - length)
                    parts += [(base, length + 1), (base | half, length + 1)]
                break
            # A server inside the part knows all of it (it holds under K + 1, so none of the server's buckets inside
            # it is full). One outside knows only some of it when its bucket holding the part is full: it hands over
            # to the server it named nearest the centre inside the part, if any
            if _shares(int.from_bytes(node_id, "big"), base, length):
                break
            if not any(_shares(int.from_bytes(n, "big"), base, length) for n in named if n not in asked):
                break
    return sent


class Sweep:
    """Which due key is looked up next, and which region covers each key, for one publish round. Thread-safe: the
    publisher's lookup threads take keys and cover regions at once.

    keys are taken in the order given (the publisher's: never published first, then the longest unpublished), each
    only while it is neither covered nor claimed by a lookup running. estimate is the p of a recent region (the
    publisher keeps it across rounds), or None.
    """

    def __init__(self, keys: Iterable[bytes], estimate: int | None = None):
        """A round over keys (distinct), none covered yet."""
        self.order = list(keys)
        self.sorted = sorted(self.order)
        self.values = [int.from_bytes(key, "big") for key in self.sorted]
        self.estimate = estimate
        self.covered: dict[bytes, Region] = {}
        self.regions: list[Region] = []
        self._claims: dict[bytes, int] = {}  # key being looked up -> the bits of the keys its region will probably hold
        self._cursor = 0  # keys before it in order are covered
        self._stopped = False
        self._cond = threading.Condition()

    def take(self) -> bytes | None:
        """The next key to look up, claimed until cover() or release(); None once every key is covered or stop() was
        called. While every key left is claimed, waits for a lookup to finish."""
        with self._cond:
            while True:
                order = self.order
                while self._cursor < len(order) and order[self._cursor] in self.covered:
                    self._cursor += 1
                if self._stopped or self._cursor == len(order):
                    return None
                claims = [(int.from_bytes(key, "big"), bits) for key, bits in self._claims.items()]
                for key in order[self._cursor:]:
                    if key in self.covered or key in self._claims:
                        continue
                    value = int.from_bytes(key, "big")
                    if any(_shares(value, prefix, bits) for prefix, bits in claims):
                        continue
                    # Before any region is known, a lookup claims everything: the first runs alone
                    self._claims[key] = 0 if self.estimate is None else max(0, self.estimate - 1)
                    return key
                self._cond.wait()

    def members(self, key: bytes, bits: int) -> int:
        """How many keys not yet covered share key's first bits bits."""
        with self._cond:
            return sum(1 for k in self._in_prefix(key, bits) if k not in self.covered)

    def cover(self, region: Region) -> list[bytes]:
        """region was found (for a key take() gave): its keys not yet covered are covered by it, and returned in
        sorted order. Releases the key's claim."""
        with self._cond:
            self._claims.pop(region.key, None)
            keys = [k for k in self._in_prefix(region.key, region.bits) if k not in self.covered]
            for k in keys:
                self.covered[k] = region
            self.regions.append(region)
            if region.closest:
                self.estimate = region.bits
            self._cond.notify_all()
            return keys

    def release(self, key: bytes):
        """The lookup for key ended without a region (it raised, or the round stopped)."""
        with self._cond:
            self._claims.pop(key, None)
            self._cond.notify_all()

    def stop(self):
        """Take no more keys: take() returns None from now on."""
        with self._cond:
            self._stopped = True
            self._cond.notify_all()

    def _in_prefix(self, key: bytes, bits: int) -> list[bytes]:
        """The keys sharing key's first bits bits, in sorted order (lock held)."""
        shift = ID_BITS - bits
        low = (int.from_bytes(key, "big") >> shift) << shift
        high = low + (1 << shift)
        return self.sorted[bisect.bisect_left(self.values, low):bisect.bisect_left(self.values, high)]
