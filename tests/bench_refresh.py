#!/usr/bin/env python3
"""Benchmark of a start's first Publisher.refresh: walking every pin's DAG for what the node serves and provides.

Not a unittest module (discovery runs test_*.py only) and not run by the suite: a script the maintainer runs by
hand. It writes a store of --pins pins straight into blocks/ (no fsync, as a store that has been running a while
is laid out): one large folder laid out like a Project Gutenberg mirror (--files files in nested numbered folders,
a few per book, some books with an HTML subfolder; a third of the files one chunk, the rest a file node over 2 to
10 chunks), the rest folders of 1,000 files and large files of 8,000 chunks (two levels of file nodes). Chunks are
a few bytes each: the walk reads DAG nodes and only looks up a raw chunk's size, so their length changes nothing
but the disk space taken. It then pins them all ("user", so their entries are provided too) and closes storage
cleanly, and times, each in a new process (no memo carried over), --runs times:

- open       Storage(): the block sizes read from the state database, pins loaded
- refresh    the first Publisher.refresh(): dag_blocks of every pin, then the entries of each (provide_entries)

and the peak memory the refresh allocated (tracemalloc, in a run of its own, as it slows Python down) and, with
--profile, a cProfile of one refresh.

Usage (the Python that runs rnfs, with its dependencies installed; from a copy of the repository):

    python3 tests/bench_refresh.py --dir /path/on/the/disk/to/measure
    python3 tests/bench_refresh.py --files 20000 --pins 10 --profile

--dir defaults to a new temporary directory under the current directory (not /tmp, which may be tmpfs); it is
removed afterwards (--keep keeps it, and --store DIR measures one kept before without writing it again). Disk
space: about 4 KiB per block (one filesystem block each); the defaults write about 565,000 blocks.
"""
import argparse
import cProfile
import io
import json
import os
import pstats
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # run from a plain checkout

from rnfs import constants
from rnfs.config import Config
from rnfs.content import blockstore, builder, dag
from rnfs.content.cid import DAG, RAW, Cid
from rnfs.content.dag import Child, Entry, FileNode
from rnfs.content.storage import USER, Storage
from rnfs.publisher import Publisher


class _Writer:
    """Writes blocks straight into a block store's subdirectories, counting them."""

    def __init__(self, blocks: str):
        """Into blocks/."""
        self.blocks = blocks
        self.count = 0
        self.serial = 0
        os.makedirs(blocks, mode=0o700)
        for a in blockstore.SHARD_DIGITS:
            for b in blockstore.SHARD_DIGITS:
                os.mkdir(os.path.join(blocks, a + b), 0o700)

    def put(self, cid: Cid, block: bytes):
        """Write block as cid's file."""
        name = str(cid)
        path = os.path.join(self.blocks, blockstore.shard_of(name), name)
        if not os.path.exists(path):
            with open(path, "wb") as f:
                f.write(block)
            self.count += 1

    def chunk(self) -> Child:
        """A new raw chunk of a few bytes, unlike every other."""
        self.serial += 1
        data = b"chunk %d" % self.serial
        cid = Cid.for_block(RAW, data)
        self.put(cid, data)
        return Child(cid, len(data))

    def file(self, chunks: int) -> tuple[Cid, int]:
        """A file of chunks chunks: (root CID, size); a raw block for one, file nodes (levels of
        MAX_CHILDREN) for more."""
        level = [self.chunk() for _ in range(chunks)]
        if len(level) == 1:
            return level[0].cid, level[0].size
        while len(level) > 1:
            groups = [level[i:i + constants.MAX_CHILDREN] for i in range(0, len(level), constants.MAX_CHILDREN)]
            level = []
            for group in groups:
                node = FileNode(group)
                block = dag.encode(node)
                cid = Cid.for_block(DAG, block)
                self.put(cid, block)
                level.append(Child(cid, node.size))
        return level[0].cid, level[0].size

    def file_entry(self, name: str, chunks: int) -> Entry:
        """A directory entry for a new file."""
        cid, size = self.file(chunks)
        return Entry(name, cid, size, dag.KIND_FILE)

    def directory(self, name: str, entries: list[Entry]) -> Entry:
        """A directory entry for a new directory of entries."""
        built = builder.build_directory(entries, self.put)
        return Entry(name, built.cid, built.size, dag.KIND_DIRECTORY)


def _book_files(writer: _Writer, rng: random.Random, number: int) -> list[Entry]:
    """A book's files: a third of them one chunk, the rest 2 to 10."""

    def chunks() -> int:
        return 1 if rng.random() < 1 / 3 else rng.randint(2, 10)

    entries = [writer.file_entry(f"{number}{suffix}", chunks())
               for suffix in rng.sample([".txt", "-0.txt", "-8.txt", ".zip", "-0.zip", "-8.zip"], rng.randint(2, 6))]
    if rng.random() < 0.1:
        html = [writer.file_entry(f"{number}-h.htm", chunks())]
        html += [writer.file_entry(f"image{i:03}.jpg", chunks()) for i in range(rng.randint(1, 4))]
        entries.append(writer.directory(f"{number}-h", html))
    return entries


def gutenberg(writer: _Writer, rng: random.Random, files: int) -> Cid:
    """A folder of about files files, laid out as a Gutenberg mirror: book 12345 in 1/2/3/4/12345/."""
    tree: dict = {}
    number = 0
    while files > 0:
        number += 1
        book = _book_files(writer, rng, number)
        files -= sum(1 if entry.kind != dag.KIND_DIRECTORY else 2 for entry in book)
        digits = str(number)[:-1] or "0"
        place = tree
        for digit in digits:
            place = place.setdefault(digit, {})
        place[str(number)] = book

    def build(name: str, place) -> Entry:
        if isinstance(place, list):
            return writer.directory(name, place)
        return writer.directory(name, [build(child, below) for child, below in place.items()])

    return build("gutenberg", tree).cid


def build_store(work: str, files: int, pins: int, seed: int) -> dict:
    """Write the store and pin it; what it holds."""
    rng = random.Random(seed)
    writer = _Writer(os.path.join(work, "blocks"))
    roots = [gutenberg(writer, rng, files)]
    for n in range(1, pins):
        if n % 3 == 0:
            roots.append(writer.directory("folder", [writer.file_entry(f"file{i:04}", rng.randint(1, 6))
                                                     for i in range(1000)]).cid)
        else:
            roots.append(writer.file(8000)[0])
    storage = Storage(writer.blocks, os.path.join(work, "state"), 1 << 60)
    storage.pin_many(roots, USER)
    count = storage.block_count()
    storage.close()
    return {"blocks": count, "pins": len(roots)}


class _Records:
    """The provider store: the publisher's own records are dropped."""

    def store(self, key, publisher):
        """Record nothing."""
        return True

    def withdraw(self, key, publisher):
        """Withdraw nothing."""
        return True


def measure(work: str, mode: str) -> dict:
    """One start: open storage, make a publisher and refresh it (mode "time", "memory" or "profile")."""
    state = os.path.join(work, "state")
    start = time.perf_counter()
    storage = Storage(os.path.join(work, "blocks"), state, 1 << 60)
    opened = time.perf_counter() - start
    config = Config(rnfs_dir=work)
    publisher = Publisher(bytes(16), config, storage, _Records(), None, lambda key: None, False, state)
    result = {"open": opened}
    try:
        if mode == "memory":
            tracemalloc.start()
        profile = cProfile.Profile() if mode == "profile" else None
        start = time.perf_counter()
        if profile is not None:
            profile.enable()
        publisher.refresh()
        if profile is not None:
            profile.disable()
        result["refresh"] = time.perf_counter() - start
        if mode == "memory":
            result["peak"] = tracemalloc.get_traced_memory()[1]
            tracemalloc.stop()
        if profile is not None:
            out = io.StringIO()
            pstats.Stats(profile, stream=out).sort_stats("cumulative").print_stats(40)
            result["profile"] = out.getvalue()
        result["provided"] = len(publisher.provided())
        result["entries"] = len(publisher.provided_entries())
        result["served"] = len(publisher._served)
    finally:
        publisher.close()
        storage.close()
    return result


def run(work: str, mode: str) -> dict:
    """measure() in a new process, so no memo is carried over from an earlier run."""
    # The same hash seed in every run, so pins are walked in the same order (a set of CIDs) and peaks compare
    environment = dict(os.environ, PYTHONHASHSEED="0")
    output = subprocess.run([sys.executable, os.path.abspath(__file__), "--measure", work, "--mode", mode],
                            check=True, stdout=subprocess.PIPE, text=True, env=environment).stdout
    return json.loads(output.splitlines()[-1])  # after anything rnfs logged


def main():
    """Build the store, run the benchmark and print a table."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--dir", help="directory on the disk to measure (default: a new one under the current one)")
    parser.add_argument("--files", type=int, default=70_000, help="files in the large folder (default 70000)")
    parser.add_argument("--pins", type=int, default=28, help="pins, the large folder one of them (default 28)")
    parser.add_argument("--runs", type=int, default=3, help="timed starts (default 3)")
    parser.add_argument("--seed", type=int, default=1, help="random seed of the layout (default 1)")
    parser.add_argument("--profile", action="store_true", help="print a cProfile of one refresh")
    parser.add_argument("--keep", action="store_true", help="keep the store written (and say where)")
    parser.add_argument("--store", help="measure a store a run with --keep wrote, without writing one")
    parser.add_argument("--measure", help=argparse.SUPPRESS)
    parser.add_argument("--mode", default="time", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.measure:
        print("\n" + json.dumps(measure(args.measure, args.mode)))
        return
    work = args.store or tempfile.mkdtemp(prefix="rnfs-bench-", dir=args.dir or os.getcwd())
    try:
        if not args.store:
            print(f"writing the store in {work} ...", file=sys.stderr, flush=True)
            start = time.perf_counter()
            held = build_store(work, args.files, args.pins, args.seed)
            print(f"wrote {held['blocks']} blocks, {held['pins']} pins in {time.perf_counter() - start:.0f} s",
                  file=sys.stderr, flush=True)
        run(work, "time")  # untimed: the first writes the publisher's rows, as a node's first start did once
        runs = [run(work, "time") for _ in range(args.runs)]
        memory = run(work, "memory")
        last = runs[-1]
        print(f"{last['served']} blocks served, {last['provided']} keys provided ({last['entries']} entries), {work}")
        print(f"{'':10}{'median s':>10}{'min s':>10}")
        for name in ("open", "refresh"):
            times = [result[name] for result in runs]
            print(f"{name:10}{statistics.median(times):10.3f}{min(times):10.3f}")
        print(f"refresh peak memory (tracemalloc): {memory['peak'] / 1e6:.1f} MB")
        if args.profile:
            print(run(work, "profile")["profile"])
    finally:
        if args.keep or args.store:
            print(f"store kept in {work}", file=sys.stderr)
        else:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
