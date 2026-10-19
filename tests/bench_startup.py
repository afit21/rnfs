#!/usr/bin/env python3
"""Benchmark of opening the block store at startup: what the API waits for before it answers.

Not a unittest module (discovery runs test_*.py only) and not run by the suite: a script the maintainer runs by
hand on the disks that matter. It writes --blocks small blocks in rnfs 0.1.6's flat layout (blocks/<cid>), then
times, through the real Storage:

- 0.1.6 listing  a listing of blocks/ as 0.1.6's start made it (each name parsed, each file lstat()ed), for the
                 "before" number; nothing is changed.
- first start    the first start of 0.2.0: every block moved into its subdirectory, then listed, and the sizes
                 written to the state database.
- clean start    a start after a clean stop: the sizes read from the database (each run --runs times).
- unclean start  a start after a crash: every subdirectory listed and the sizes written again (the clean mark
                 cleared by hand before each run).

Usage (the Python that runs rnfs, with its dependencies installed; from a copy of the repository):

    python3 tests/bench_startup.py --dir /path/on/the/disk/to/measure
    python3 tests/bench_startup.py --dir /mnt/sd/bench --blocks 1000000 --cold   # as root, for a cold cache

On a Raspberry Pi SD card, run it with --dir on the SD card's filesystem (`df -T DIR`: ext4, not tmpfs), the Pi
otherwise idle. --cold drops the page cache before each timed run (writing /proc/sys/vm/drop_caches, Linux, as
root): what a start after a reboot sees. Without it the directories and the database are in memory and the numbers
are what a restart sees. Disk space: about 4 KiB per block (one filesystem block each). --dir defaults to a new
temporary directory under the current directory (not /tmp, which may be tmpfs); it is removed afterwards.
"""
import argparse
import os
import shutil
import sqlite3
import statistics
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # run from a plain checkout

from rnfs import statedb
from rnfs.content.cid import RAW, Cid
from rnfs.content.storage import SIZES_STORE, Storage


def drop_caches(cold: bool):
    """Write back dirty pages, and with cold drop the page cache too (root only)."""
    if hasattr(os, "sync"):
        os.sync()
    if cold:
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")


def write_flat(blocks: str, count: int):
    """count small raw blocks in 0.1.6's flat layout."""
    os.makedirs(blocks)
    for n in range(count):
        data = b"block %d" % n
        with open(os.path.join(blocks, str(Cid.for_block(RAW, data))), "wb") as f:
            f.write(data)


def list_as_016(blocks: str) -> int:
    """List blocks/ as rnfs 0.1.6's BlockStore.scan did; how many blocks."""
    count = 0
    with os.scandir(blocks) as listing:
        for entry in listing:
            try:
                Cid.from_block_name(entry.name)
                if entry.is_file(follow_symlinks=False):
                    entry.stat(follow_symlinks=False)
                    count += 1
            except (ValueError, OSError):
                continue
    return count


def open_storage(blocks: str, state: str) -> tuple[float, int]:
    """Open (and close) storage; (seconds the open took, blocks indexed)."""
    start = time.perf_counter()
    storage = Storage(blocks, state, 1 << 60)
    seconds = time.perf_counter() - start
    count = storage.block_count()
    storage.close()
    return seconds, count


def mark_unclean(state: str):
    """Clear the clean mark, as a crash leaves it."""
    conn = sqlite3.connect(os.path.join(state, statedb.DB_FILE))
    try:
        with conn:
            conn.execute("UPDATE meta SET value = 0 WHERE store = ? AND key = 'clean'", (SIZES_STORE,))
    finally:
        conn.close()


def main():
    """Run the benchmark and print a table."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--dir", help="directory on the disk to measure (default: a new one under the current one)")
    parser.add_argument("--blocks", type=int, default=100_000, help="blocks stored (default 100000)")
    parser.add_argument("--runs", type=int, default=3, help="runs of each start but the first (default 3)")
    parser.add_argument("--cold", action="store_true", help="drop the page cache before each run (Linux, root)")
    args = parser.parse_args()
    work = tempfile.mkdtemp(prefix="rnfs-bench-", dir=args.dir or os.getcwd())
    try:
        blocks, state = os.path.join(work, "blocks"), os.path.join(work, "state")
        print(f"writing {args.blocks} blocks in {work} ...", file=sys.stderr, flush=True)
        write_flat(blocks, args.blocks)
        results = {}
        listings = []
        for _ in range(args.runs):
            drop_caches(args.cold)
            start = time.perf_counter()
            assert list_as_016(blocks) == args.blocks
            listings.append(time.perf_counter() - start)
        results["0.1.6 listing"] = listings
        drop_caches(args.cold)
        seconds, count = open_storage(blocks, state)
        assert count == args.blocks
        results["first start"] = [seconds]
        for name, before in (("clean start", None), ("unclean start", mark_unclean)):
            runs = []
            for _ in range(args.runs):
                if before is not None:
                    before(state)
                drop_caches(args.cold)
                seconds, count = open_storage(blocks, state)
                assert count == args.blocks
                runs.append(seconds)
            results[name] = runs
        print(f"{args.blocks} blocks, {'cold' if args.cold else 'warm'} cache, {work}")
        print(f"{'':16}{'median s':>10}{'min s':>10}")
        for name, runs in results.items():
            print(f"{name:16}{statistics.median(runs):10.3f}{min(runs):10.3f}")
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
