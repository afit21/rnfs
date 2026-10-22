"""Sweep providing (specification section 6, "Publishing"): the K closest servers of many keys from one lookup per
region rather than one per key. No RNS dependency: the lookups are made by the caller (rnfs.publisher).

- **Regions.** The due keys are taken in turn (Sweep.take); for the first not yet covered, x, the closest-nodes
  lookup runs as for one key. p is the length of the prefix x shares with the farthest of its K closest, and the
  region is every due key sharing x's first p bits. Every server closer to x than that one shares those bits, so
  the region holds at least K servers and each of its keys' K closest are inside it (with fewer than K found, the
  farthest found sets p). Lookups per round level off at about servers / 12.5, however many keys are due.
- **Sized by the network size estimate too** (find_region's size, rnfs.dht.density's estimate). A node that knows few
  identities (on a LoRa island, or just started) finds x's K closest among them, padded with servers it knows far
  from x: p comes out short and the region holds many due keys the lookup knows little about (topology study section
  11.3: 3.7 of their 10 closest on an island against 7.0 with a lookup per key). So the region's prefix is never
  shorter than size_bits of the estimate (log2(N / K) less SIZE_MARGIN), and the far half is still the one p as found
  gives. While there is no estimate (a node's first lookups after its first start), a region is the key looked up
  alone.
- **Keys the region can store to.** Such a node also hears of servers near the region's keys (named in answers)
  whose identity it doesn't know, so storing to them would cost a path request each, which its lookups budget: a key
  of the region is covered only if at most UNSTORABLE of its K closest servers heard of are outside what the region
  learned (Region.knows); the others are looked up in turn like any key not yet covered. A node that knows every
  identity (one on the backbone that has heard every announce) covers every key of its regions, whatever the
  distances: the rule compares what was heard with what can be stored to, not with the size estimate, so it costs
  no lookups where nothing is missing (a rule from the estimate, the K-th closest within 1.5 times its median
  distance, looked up about 8% of keys on their own there, as K-th distances spread that far by chance).
- **The far half.** The half of the region holding x is known in full: its servers are all closer to x than the
  K-th. If the region holds 2 or more due keys, one /find goes to the known server nearest the centre of the other
  half, about that centre; a reply naming K servers inside it means there may be more, so each half of it is asked
  about in turn, at most MAX_REGION_FINDS in all. Every key of the region then takes its K closest from every
  server learned (Region.closest_to), ranked as find_closest ranks them: unreliable servers that haven't answered
  last. In the load study this placed 100% of records on their true K closest for about 1.6 extra /find a region.
- **Disjoint paths and the density check.** A region's lookup may run as disjoint paths (rnfs.dht.disjoint): its
  servers are all the paths learned, and each key is also stored on each path's K closest (Region.path_closest).
  The publisher then checks each key of the region for a dense core (rnfs.dht.density) over those servers; a key
  flagged gets a widened lookup of its own, counted in the round's lookups (Sweep.spend), whose ring servers it is
  stored on too (Region.rings). A key whose widened lookup the round can't afford is left for a later round
  (Region.deferred).
- **Running lookups at once.** Each lookup running claims the keys it will probably cover (those sharing one bit
  fewer than the last region's p), so the next takes a key outside every claim, and the regions of lookups run
  at once seldom overlap; a key two regions cover keeps the first. Until a region has been found there is no
  estimate, and the first lookup runs alone.
"""
import bisect
import heapq
import math
import threading
from collections.abc import Callable, Iterable

from rnfs.constants import ID_LENGTH, K
from rnfs.dht.lookup import FIRST
from rnfs.dht.routing import distance

ID_BITS = ID_LENGTH * 8
# Extra /find a region sends at most: the far half, then each half of a part whose reply was full, breadth first
# (three levels). The load study needed 1.6 on average
MAX_REGION_FINDS = 7
REGION_FIND_TRIES = 3  # servers asked about one part, nearest its centre first, before giving up on it
# A region's prefix is at least log2(N / K) less this (size_bits), N the network size estimate. A key's K-th closest
# shares about log2(N / K) - 0.5 bits with it, so on a network that is known well the bound seldom binds (1 bit: at
# 1,000 servers no more lookups than without it; 0 bits: 4-11% more, for nothing measurable)
SIZE_MARGIN = 1.0
# A region covers a key only if at most this many of its K closest servers heard of are ones it can't store to
# (Region.knows). Measured in test_topology's world, island node at 1,000 servers: 7.0 of 10 placed against 7.8 a lookup
# per key, 1.5 keys a region lookup (1: 7.4 in 1.3; 3: 6.6 in 2.0); at 3,000 about a lookup per key's either way
UNSTORABLE = 2


def prefix_length(a: bytes, b: bytes) -> int:
    """How many leading bits a and b share (ID_BITS if equal)."""
    return ID_BITS - distance(a, b).bit_length()


def _shares(value: int, prefix: int, bits: int) -> bool:
    """True if value's first bits bits are prefix's."""
    shift = ID_BITS - bits
    return value >> shift == prefix >> shift


class Region:
    """What one region's lookup learned: the region is the keys sharing the first `bits` bits of `key`, the key looked
    up (only `key` itself if `single`: there was no network size estimate to size it by); `closest` is that lookup's
    result for it; `nodes` every server learned, each with its rank tier (dht.lookup: UNRELIABLE if unreliable and not
    answered); `paths` each disjoint path's own (none for a single lookup); `finds` the extra /find sent; `heard` every
    server its lookup heard of, also those `nodes` leaves out as their identity isn't known (None: not judged). The
    publisher adds `rings` (key -> the ring servers its widened lookup found) and `deferred` (keys flagged whose widened
    lookup the round couldn't afford)."""

    __slots__ = ("key", "bits", "closest", "nodes", "finds", "paths", "single", "heard", "unstorable", "rings",
                 "deferred")

    def __init__(self, key: bytes, bits: int, closest: list[bytes], nodes: dict[bytes, int], finds: int = 0,
                 paths: list[dict[bytes, int]] | None = None, single: bool = False, heard: set[bytes] | None = None):
        """A region found by a lookup for key."""
        self.key = key
        self.bits = bits
        self.closest = closest
        self.nodes = nodes
        self.finds = finds
        self.paths = paths or []
        self.single = single
        self.heard = heard
        self.unstorable = frozenset() if heard is None else frozenset(heard - nodes.keys())
        self.rings: dict[bytes, list[bytes]] = {}
        self.deferred: set[bytes] = set()

    def __contains__(self, key: bytes) -> bool:
        """True if key is in the region (shares its first bits with the key looked up; is it, for a single one)."""
        if self.single:
            return key == self.key
        return prefix_length(key, self.key) >= self.bits

    def knows(self, key: bytes) -> bool:
        """True if the region can store key, a key in it, on its K closest servers heard of, but for at most UNSTORABLE
        it heard of without learning their identity. Always for the key looked up, or one region that heard of no such
        server; for a single region, only its key."""
        if self.single:
            return key == self.key
        if key == self.key or len(self.unstorable) <= UNSTORABLE:
            return True
        nearest = heapq.nsmallest(K, self.nodes.keys() | self.unstorable, key=lambda n: distance(n, key))
        return sum(1 for n in nearest if n in self.unstorable) <= UNSTORABLE

    def closest_to(self, key: bytes, count: int = K) -> list[bytes]:
        """The count servers learned closest to key, ranked as a lookup ranks them (by tier: unreliable ones that
        haven't answered after every other). For the key looked up, never fewer than its lookup found: the far half's
        replies can name a server closer to it than one the lookup chose, never take one away."""
        if key == self.key:
            count = max(count, len(self.closest))
        nodes = self.nodes
        return heapq.nsmallest(count, nodes, key=lambda n: (nodes[n], distance(n, key)))

    def path_closest(self, key: bytes, count: int = K) -> list[bytes]:
        """The count servers each disjoint path learned closest to key (ranked as closest_to ranks them), together and
        without duplicates, each path's in turn; none for a single lookup."""
        chosen: dict[bytes, None] = {}
        for nodes in self.paths:
            chosen.update(dict.fromkeys(heapq.nsmallest(count, nodes, key=lambda n: (nodes[n], distance(n, key)))))
        return list(chosen)


def size_bits(size: float) -> int:
    """The shortest prefix a region may have among size servers (the network size estimate): log2(size / K) less
    SIZE_MARGIN, rounded down, at least 0."""
    return max(0, math.floor(math.log2(max(size, 1.0) / K) - SIZE_MARGIN))


def find_region(lookup, key: bytes, own_id: bytes, members: Callable[[int], int],
                size: Callable[[], float | None] | None = None) -> Region:
    """Run lookup (a dht.lookup.Lookup for key) and find key's region; members(bits) is how many due keys share key's
    first bits bits, so the far half is asked about only for a region of 2 or more. size() is the network size
    estimate once the lookup has run: the region's prefix is never shorter than size_bits of it, it covers only the
    keys it can store to (Region.knows, from what lookup.heard() names), and while there is no estimate (None) it is
    key alone. Without size, the lookup alone sizes the region."""
    closest = [n for n in lookup.find_closest() if n != own_id]
    ranked = sorted(closest, key=lambda n: distance(n, key))[:K]
    # Without any server found there is nothing to learn: every due key shares the outcome (retried soon)
    found = prefix_length(key, ranked[-1]) if ranked else 0
    bits, single, estimate = found, False, None
    if ranked and size is not None:
        estimate = size()
        if estimate is None:
            single = True
        else:
            bits = max(found, size_bits(estimate))
    # The far half of the region as found, even when the estimate narrowed it: servers sharing found + 1 bits with key
    # are known (all closer than the K-th), and a narrower region's keys may have some of their K closest beyond it
    finds = explore(lookup, key, found, own_id) if ranked and not single and members(bits) >= 2 else 0
    nodes = {n: tier for n, tier in lookup.candidates().items() if n != own_id}
    for n in closest:
        nodes.setdefault(n, FIRST)
    # A disjoint lookup's paths (rnfs.dht.disjoint), each storing to its own K closest too
    paths = [{n: tier for n, tier in path.items() if n != own_id}
             for path in getattr(lookup, "path_candidates", list)()]
    # What it heard of beyond what it can store to, judged only when sizing by the estimate (as the node does)
    heard = lookup.heard() - {own_id} if size is not None and hasattr(lookup, "heard") else None
    return Region(key, bits, closest, nodes, finds, paths, single, heard)


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
            if lookup.remaining() <= 0:
                return sent  # the lookup's time budget is spent: nothing more is sent
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
        self.widened = 0  # widened lookups run for keys the density check flagged (spend)
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
                for index in range(self._cursor, len(order)):
                    key = order[index]
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
        """region was found (for a key take() gave): its keys not yet covered that it can store to (Region.knows)
        are covered by it, and returned in sorted order. Releases the key's claim; a single region's prefix doesn't
        become the estimate."""
        with self._cond:
            self._claims.pop(region.key, None)
            keys = [k for k in self._in_prefix(region.key, region.bits) if k not in self.covered and region.knows(k)]
            for k in keys:
                self.covered[k] = region
            self.regions.append(region)
            if region.closest and not region.single:
                self.estimate = region.bits
            self._cond.notify_all()
            return keys

    @property
    def lookups(self) -> int:
        """Lookups the round has run: one per region, and the widened ones."""
        with self._cond:
            return len(self.regions) + self.widened

    def spend(self, limit: int | None) -> bool:
        """Count a widened lookup if the round may run one more (limit: its lookups at most, None for no limit); False
        if it may not."""
        with self._cond:
            if limit is not None and len(self.regions) + self.widened >= limit:
                return False
            self.widened += 1
            return True

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
