#!/usr/bin/env python3
"""Benchmark of the publisher's state: what each provided key costs in memory and on disk, and how long a wake takes.

Not a unittest module (discovery runs test_*.py only) and not run by the suite: a script the maintainer runs by
hand. It provides --keys raw CIDs (each a complete pin of one block, through a stand-in for the storage) from a
publisher on a state database in --dir, publishes them over a fake network of --servers servers (each lookup finds
the K closest, a region's extra /find is answered; every /provide is recorded), and reports:

- the rounds that publish them all, the region lookups they took and the bytes written (Linux: the process's write()
  total, which counts SQLite's WAL and its checkpoints), then the same for a republish of every key;
- a wake: next_due() and publish_due() with nothing due (the median of 5), after the rounds and after a restart;
- the database's size per key, and its pages in use per table and index (SQLite's dbstat, if compiled in);
- the regions are sized by the network size estimate as a node's are (Publisher.set_network_size, given the
  true server count; --no-size leaves the publisher without one);
- with --trace, the memory the publisher holds per key (tracemalloc, which slows everything 2-3x): with the keys
  provided and never published, after the rounds, after a restart, and a round's peak.

Usage (the Python that runs rnfs, with its dependencies installed; from a copy of the repository):

    python3 tests/bench_publisher.py --keys 100000
    python3 tests/bench_publisher.py --keys 1000000 --trace --dir /mnt/sd/bench

--dir defaults to a new temporary directory under the current directory (not /tmp, which may be tmpfs); it is
removed afterwards. Disk space: about 400 bytes per key, and as much again for the WAL while it runs.
"""
import argparse
import bisect
import gc
import os
import random
import shutil
import sqlite3
import statistics
import sys
import tempfile
import time
import tracemalloc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # run from a plain checkout

import RNS

from rnfs import statedb
from rnfs.config import Config
from rnfs.constants import HOUR, K
from rnfs.content.cid import RAW, Cid
from rnfs.publisher import Publisher

INTERVAL = 22 * HOUR  # the republish interval used


class _Deny:
    """An empty deny list."""

    cids = frozenset()


class _Storage:
    """The storage calls the publisher makes: each CID a complete pin of one block."""

    def __init__(self, cids):
        """Pins of cids."""
        self.cids = frozenset(cids)
        self.deny = _Deny()
        self.dags = {cid: frozenset((cid,)) for cid in self.cids}

    def pinned_cids(self):
        """Every pin."""
        return self.cids

    def cached_targets(self):
        """No cached downloads."""
        return set()

    def dag_blocks(self, cid):
        """The DAG of a pin: its one block."""
        return self.dags[cid]

    def pin_order(self, cids):
        """Pins oldest first: here, by CID."""
        return sorted(cids)


class _Records:
    """The provider store: the publisher's own records are dropped."""

    def store(self, key, publisher):
        """Record nothing."""
        return True

    def withdraw(self, key, publisher):
        """Withdraw nothing."""
        return True


class _Client:
    """The network's /provide: every key recorded."""

    def __init__(self):
        """No request yet."""
        self.requests = 0

    def provide(self, node_id, keys, withdrawn):
        """Count the request; every key is recorded."""
        self.requests += 1
        return set(keys)


def _closest(servers, values, key: bytes, count: int = K) -> list[bytes]:
    """The count of servers (sorted, values their integers) closest to key."""
    value = int.from_bytes(key, "big")
    at = bisect.bisect_left(values, value)
    near = servers[max(0, at - 2 * count):at + 2 * count]
    return sorted(near, key=lambda node: int.from_bytes(node, "big") ^ value)[:count]


class _Lookup:
    """A lookup finding the K servers closest to key, having learned the 3K closest on its way (a real lookup's
    last hops answer with their K closest each); a region's extra /find is answered with the K closest to what it
    asks about, as a server knowing that part of the network would."""

    def __init__(self, servers, values, key):
        """A lookup for key over servers (sorted, values their integers)."""
        self.servers, self.values = servers, values
        self.nodes = _closest(servers, values, key)
        self.known = dict.fromkeys(_closest(servers, values, key, 3 * K), 0)

    def find_closest(self):
        """The K closest."""
        return list(self.nodes)

    def candidates(self):
        """Every server learned, all answered."""
        return dict(self.known)

    def heard(self):
        """Every server it heard of: those learned, every identity known (as a node that hears every announce)."""
        return set(self.known)

    def remaining(self):
        """Time left: plenty."""
        return 600.0

    def ask(self, node_id, key):
        """A region's extra /find: the K closest to key, learned too."""
        named = _closest(self.servers, self.values, key)
        self.known.update(dict.fromkeys(named, 0))
        return named


class _Clock:
    """A wall clock the benchmark moves."""

    def __init__(self, now):
        """At now."""
        self.now = now

    def __call__(self):
        """The time."""
        return self.now


def written() -> int:
    """Bytes this process has passed to write() (Linux; 0 elsewhere)."""
    try:
        with open("/proc/self/io", encoding="ascii") as f:
            return int(next(line for line in f if line.startswith("wchar:")).split()[1])
    except OSError:
        return 0


def median_time(work, runs: int = 5) -> float:
    """The median of runs timings of work(), in seconds."""
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        work()
        times.append(time.perf_counter() - start)
    return statistics.median(times)


def pages(path: str) -> tuple[int, dict[str, int]]:
    """The bytes of the database at path in use, and per table and index (empty without dbstat)."""
    conn = sqlite3.connect(path)
    try:
        count, free, size = (conn.execute(f"PRAGMA {name}").fetchone()[0]
                             for name in ("page_count", "freelist_count", "page_size"))
        try:
            tables = dict(conn.execute("SELECT name, SUM(pgsize) FROM dbstat GROUP BY name").fetchall())
        except sqlite3.OperationalError:
            tables = {}
    finally:
        conn.close()
    return (count - free) * size, tables


def main() -> int:
    """Run the benchmark."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--keys", type=int, default=100_000, help="CIDs provided (default 100,000)")
    parser.add_argument("--servers", type=int, default=2000, help="servers of the fake network (default 2,000)")
    parser.add_argument("--trace", action="store_true", help="measure memory (tracemalloc; much slower)")
    parser.add_argument("--no-size", action="store_true", help="give the publisher no network size estimate")
    parser.add_argument("--dir", help="where to put the state database (default: a new directory here)")
    args = parser.parse_args()
    RNS.loglevel = RNS.LOG_CRITICAL
    keys = args.keys
    rng = random.Random(1)
    servers = sorted(rng.randbytes(16) for _ in range(args.servers))
    values = [int.from_bytes(node, "big") for node in servers]
    directory = tempfile.mkdtemp(prefix="bench-publisher-", dir=args.dir or os.getcwd())
    state = os.path.join(directory, "state")
    try:
        storage = _Storage(Cid.for_block(RAW, n.to_bytes(8, "big")) for n in range(keys))
        clock = _Clock(1_800_000_000.0)
        config = Config(rnfs_dir=directory, provide_entries=False, republish_interval=INTERVAL)
        client = _Client()
        gc.collect()
        if args.trace:
            tracemalloc.start()
        base = tracemalloc.get_traced_memory()[0] if args.trace else 0

        def memory(label: str):
            """Report the memory held now, beyond what was before the publisher."""
            if args.trace:
                gc.collect()
                held = tracemalloc.get_traced_memory()[0] - base
                print(f"memory {label}: {held / 1e6:.1f} MB, {held / keys:.0f} B a key", flush=True)

        def make() -> Publisher:
            """A publisher on the state database, told the network's size unless --no-size."""
            made = Publisher(bytes(16), config, storage, _Records(), client,
                             lambda key: _Lookup(servers, values, key), False, state, clock=clock)
            if not args.no_size:
                made.set_network_size(lambda key: float(args.servers))
            return made

        def publish_all(label: str):
            """Run rounds until nothing is due; report them."""
            start, wrote, rounds, lookups, peak = time.perf_counter(), written(), 0, 0, 0
            while publisher.next_due() <= 0:
                if args.trace:
                    tracemalloc.reset_peak()
                lookups += publisher.publish_due()["lookups"]
                if args.trace:
                    peak = max(peak, tracemalloc.get_traced_memory()[1] - base)
                rounds += 1
            wrote = written() - wrote
            print(f"{label}: {rounds} rounds, {time.perf_counter() - start:.1f} s, {lookups} region lookups, "
                  f"{wrote / 1e6:.1f} MB written ({wrote / keys:.0f} B a key)"
                  + (f", peak {peak / 1e6:.1f} MB" if args.trace else ""), flush=True)

        def wake(label: str):
            """Report the time of a wake with nothing due."""
            clock.now += 1
            if not args.trace:
                print(f"wake {label}: next_due {median_time(publisher.next_due) * 1000:.1f} ms, publish_due "
                      f"{median_time(publisher.publish_due) * 1000:.1f} ms", flush=True)

        start = time.perf_counter()
        publisher = make()
        publisher.refresh()
        print(f"{keys} keys provided in {time.perf_counter() - start:.1f} s", flush=True)
        memory("with the keys provided, never published")
        publish_all("first publish")
        memory("after the rounds")
        wake("after the rounds")
        clock.now += INTERVAL + HOUR
        publish_all("republish")
        publisher.save()
        publisher.close()
        del publisher
        path = os.path.join(state, statedb.DB_FILE)
        used, tables = pages(path)
        print(f"database: {used / keys:.0f} B a key in use; "
              + ", ".join(f"{name} {size / keys:.0f}" for name, size in sorted(tables.items()) if size > 8192),
              flush=True)
        start = time.perf_counter()
        publisher = make()
        publisher.refresh()
        print(f"restart (load and first refresh): {time.perf_counter() - start:.1f} s", flush=True)
        memory("after a restart")
        wake("after a restart")
        publisher.close()
    finally:
        shutil.rmtree(directory)
    return 0


if __name__ == "__main__":
    sys.exit(main())
